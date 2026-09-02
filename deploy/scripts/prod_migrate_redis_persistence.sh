#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

STATE_DIR="${ROLLING_STATE_DIR:-$PROD_ROOT/.deploy-state}"
STATE_FILE="$STATE_DIR/production.redis-migration.inprogress"
COMPLETE_FILE="$STATE_DIR/production.redis-migration.complete"
COPY_DIR="$STATE_DIR/production.redis-migration-copy"
mkdir -p "$STATE_DIR"

state_get() {
  local key="$1" default="${2:-}" line
  [[ -f "$STATE_FILE" ]] || { printf '%s\n' "$default"; return 0; }
  line="$(grep -E "^${key}=" "$STATE_FILE" | tail -n1 || true)"
  [[ -n "$line" ]] && printf '%s\n' "${line#*=}" || printf '%s\n' "$default"
}

state_set() {
  local key="$1" value="$2" tmp="${STATE_FILE}.tmp.$$"
  if [[ -f "$STATE_FILE" ]]; then
    grep -Ev "^${key}=" "$STATE_FILE" > "$tmp" || true
  else
    : > "$tmp"
  fi
  printf '%s=%s\n' "$key" "$value" >> "$tmp"
  mv "$tmp" "$STATE_FILE"
}

set_phase() {
  state_set phase "$1"
  phase="$1"
}

assert_stopped() {
  local service="$1"
  if service_is_running "$service"; then
    echo "Service '$service' is still running after a required stop; refusing Redis migration." >&2
    return 1
  fi
}

assert_release_label() {
  local service="$1" cid actual found=0
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    found=1
    actual="$(docker inspect -f '{{ index .Config.Labels "io.mediacms.release" }}' "$cid" 2>/dev/null || true)"
    if [[ "$actual" != "$MEDIACMS_RELEASE_SHA" ]]; then
      echo "Service '$service' restarted with release label '$actual', expected '$MEDIACMS_RELEASE_SHA'." >&2
      return 1
    fi
  done < <(service_container_ids "$service")
  [[ "$found" == 1 ]] || {
    echo "Service '$service' has no running container to verify release label." >&2
    return 1
  }
}

validate_persistent_redis() {
  wait_healthy db 300 1
  wait_healthy redis 300 1
  redis_is_persistent || {
    echo "Redis is not mounted on expected durable volume '$REDIS_VOLUME'." >&2
    return 1
  }

  local appendonly appendfsync aof_enabled
  appendonly="$(compose exec -T redis redis-cli --raw CONFIG GET appendonly | tail -n1 | tr -d '\r')"
  appendfsync="$(compose exec -T redis redis-cli --raw CONFIG GET appendfsync | tail -n1 | tr -d '\r')"
  aof_enabled="$(compose exec -T redis redis-cli --raw INFO persistence | tr -d '\r' | awk -F: '$1 == "aof_enabled" {print $2}')"
  if [[ "$appendonly" != yes || "$appendfsync" != everysec || "$aof_enabled" != 1 ]]; then
    echo "Redis is missing expected AOF durability settings." >&2
    return 1
  fi
}

ensure_release_service() {
  local service="$1" timeout="$2" use_crypto="$3"
  if [[ "$use_crypto" == 1 ]]; then
    compose_crypto up -d --no-deps --force-recreate "$service"
  else
    compose up -d --no-deps --force-recreate "$service"
  fi
  wait_healthy "$service" "$timeout" 1
  assert_release_label "$service"
}

migration_failure_notice() {
  local rc=$?
  if (( rc != 0 )); then
    echo "Redis persistence migration FAILED at phase $(state_get phase 0)." >&2
    echo "State was preserved in $STATE_FILE; rerun the same command after fixing the cause." >&2
    [[ -d "$COPY_DIR" ]] && echo "Copied Redis artifacts are preserved in $COPY_DIR." >&2
  fi
  return "$rc"
}
trap migration_failure_notice EXIT

if redis_is_persistent && [[ ! -f "$STATE_FILE" ]]; then
  echo "Redis already uses durable volume '$REDIS_VOLUME'."
  trap - EXIT
  exit 0
fi

cat >&2 <<'__NOTICE__'
This is the ONE-TIME Redis persistence migration.
It deliberately creates a short ingress outage. After publishers are frozen,
AOF is enabled on the still-running Redis and its initial rewrite is allowed to
finish before /data is copied to the durable volume. The migration is resumable:
rerunning the same command continues from the last completed phase.
__NOTICE__

[[ "${CONFIRM_REDIS_MIGRATION:-}" == "$PROJECT" ]] || {
  echo "Set CONFIRM_REDIS_MIGRATION=$PROJECT to execute." >&2
  exit 2
}

command -v git >/dev/null || {
  echo "git is required for the image-isolated Redis bootstrap" >&2
  exit 2
}
if [[ -n "$(git_repo status --porcelain --untracked-files=all)" ]]; then
  echo "Working-tree changes or untracked files are present; commit/stash/remove them before Redis migration." >&2
  exit 2
fi
export MEDIACMS_RELEASE_SHA="$(git_repo rev-parse HEAD)"

phase="$(state_get phase 0)"
[[ "$phase" =~ ^[0-9]+$ ]] || {
  echo "Invalid Redis migration phase '$phase' in $STATE_FILE" >&2
  exit 2
}

if [[ -f "$STATE_FILE" ]]; then
  target_sha="$(state_get target_sha '')"
  [[ "$target_sha" == "$MEDIACMS_RELEASE_SHA" ]] || {
    echo "Incomplete Redis migration targets $target_sha but checkout is $MEDIACMS_RELEASE_SHA; restore the original target checkout before resuming." >&2
    exit 2
  }
  legacy_app_mounts="$(state_get legacy_app_mounts 0)"
  deposit_running="$(state_get deposit_was_running 0)"
  sweeper_running="$(state_get sweeper_was_running 0)"
  signer_running="$(state_get signer_was_running 0)"
  celery_worker_running="$(state_get celery_worker_was_running 0)"
  celery_beat_running="$(state_get celery_beat_was_running 0)"
  cloudflared_running="$(state_get cloudflared_was_running 0)"
  echo "Resuming Redis migration at phase $phase for $MEDIACMS_RELEASE_SHA"
else
  legacy_app_mounts=0
  if legacy_app_mounts_present; then legacy_app_mounts=1; fi

  if [[ "$legacy_app_mounts" == 1 ]]; then
    legacy_web_healthy=0
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      state="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true)"
      health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || true)"
      if [[ "$state" == running && ( "$health" == healthy || "$health" == none ) ]]; then
        legacy_web_healthy=1
        break
      fi
    done < <(service_container_ids web)
    [[ "$legacy_web_healthy" == 1 ]] || {
      echo "Legacy Redis bootstrap requires at least one healthy web container." >&2
      exit 1
    }
    echo "Legacy bind-mounted application containers detected; using Docker-only preflight."
  else
    prod_preflight
  fi

  # Signer and production ingress are mandatory runtime dependencies. Refuse to
  # start a destructive bootstrap from an already-broken dependency state.
  if service_exists dfx_signer_service; then wait_healthy dfx_signer_service 60 1; fi
  if service_exists cloudflared; then wait_healthy cloudflared 60 1; fi

  deposit_running=0
  sweeper_running=0
  signer_running=0
  celery_worker_running=0
  celery_beat_running=0
  cloudflared_running=0
  service_is_running deposit_service && deposit_running=1
  service_is_running sweeper_service && sweeper_running=1
  service_is_running dfx_signer_service && signer_running=1
  service_is_running celery_worker && celery_worker_running=1
  service_is_running celery_beat && celery_beat_running=1
  service_is_running cloudflared && cloudflared_running=1

  state_set target_sha "$MEDIACMS_RELEASE_SHA"
  state_set legacy_app_mounts "$legacy_app_mounts"
  state_set deposit_was_running "$deposit_running"
  state_set sweeper_was_running "$sweeper_running"
  state_set signer_was_running "$signer_running"
  state_set celery_worker_was_running "$celery_worker_running"
  state_set celery_beat_was_running "$celery_beat_running"
  state_set cloudflared_was_running "$cloudflared_running"
  set_phase 1
fi

# Build everything we may recreate before the first required outage. A rerun
# before phase 10 simply rebuilds, which is safe and deterministic.
if (( phase < 10 )); then
  echo "Building target release images before Redis migration outage: $MEDIACMS_RELEASE_SHA"
  compose build web
  [[ "$deposit_running" == 1 ]] && compose_crypto build deposit_service
  if [[ "$signer_running" == 1 || "$sweeper_running" == 1 ]]; then
    compose_crypto build dfx_signer_service
  fi
  [[ "$sweeper_running" == 1 ]] && compose_crypto build sweeper_service
  set_phase 10
fi

# Freeze every producer before the persistence boundary. Required stops are
# fail-closed; a Docker stop failure aborts instead of continuing with writers.
if (( phase < 20 )); then
  if [[ "$legacy_app_mounts" == 1 ]]; then
    [[ "$celery_beat_running" == 1 ]] && stop_service_if_running celery_beat
    [[ "$celery_worker_running" == 1 ]] && stop_service_if_running celery_worker
  else
    drain_celery
  fi

  [[ "$deposit_running" == 1 ]] && stop_crypto_service_if_running deposit_service
  [[ "$sweeper_running" == 1 ]] && stop_crypto_service_if_running sweeper_service
  [[ "$cloudflared_running" == 1 ]] && stop_service_if_running cloudflared
  stop_service_if_running web

  [[ "$celery_worker_running" == 1 ]] && assert_stopped celery_worker
  [[ "$celery_beat_running" == 1 ]] && assert_stopped celery_beat
  [[ "$deposit_running" == 1 ]] && assert_stopped deposit_service
  [[ "$sweeper_running" == 1 ]] && assert_stopped sweeper_service
  [[ "$cloudflared_running" == 1 ]] && assert_stopped cloudflared
  assert_stopped web
  set_phase 20
fi

redis_id="$(state_get redis_container_id '')"
if (( phase < 30 )); then
  redis_id="$(service_container_ids redis | head -n1)"
  [[ -n "$redis_id" ]] || {
    echo "Existing running Redis container not found; refusing to invent an empty queue." >&2
    exit 1
  }

  docker exec "$redis_id" redis-cli SAVE >/dev/null
  docker exec "$redis_id" redis-cli CONFIG SET appendfsync everysec >/dev/null
  docker exec "$redis_id" redis-cli CONFIG SET appendonly yes >/dev/null

  wait_aof_ready() {
    local deadline=$((SECONDS + 900)) info enabled in_progress scheduled last_status
    while (( SECONDS < deadline )); do
      info="$(docker exec "$redis_id" redis-cli --raw INFO persistence | tr -d '\r')"
      enabled="$(awk -F: '$1 == "aof_enabled" {print $2}' <<<"$info")"
      in_progress="$(awk -F: '$1 == "aof_rewrite_in_progress" {print $2}' <<<"$info")"
      scheduled="$(awk -F: '$1 == "aof_rewrite_scheduled" {print $2}' <<<"$info")"
      last_status="$(awk -F: '$1 == "aof_last_bgrewrite_status" {print $2}' <<<"$info")"
      if [[ "$enabled" == 1 && "$in_progress" == 0 && "$scheduled" == 0 && "$last_status" == ok ]]; then
        return 0
      fi
      sleep 1
    done
    echo "Redis AOF conversion did not reach a safe completed state within 900s" >&2
    docker exec "$redis_id" redis-cli INFO persistence >&2 || true
    return 1
  }

  wait_aof_ready
  state_set redis_container_id "$redis_id"
  set_phase 30
fi

if (( phase < 40 )); then
  stop_service_if_running redis
  assert_stopped redis
  set_phase 40
fi

if (( phase < 50 )); then
  redis_id="$(state_get redis_container_id '')"
  [[ -n "$redis_id" ]] && docker inspect "$redis_id" >/dev/null 2>&1 || {
    echo "Stopped source Redis container '$redis_id' is unavailable; cannot resume safely." >&2
    exit 1
  }
  rm -rf "$COPY_DIR"
  mkdir -p "$COPY_DIR"
  docker cp "$redis_id:/data/." "$COPY_DIR/"

  [[ -e "$COPY_DIR/dump.rdb" ]] || {
    echo "Expected RDB backup is missing from copied Redis /data" >&2
    exit 1
  }
  if ! find "$COPY_DIR" -maxdepth 4 -type f \( -name 'appendonly.aof' -o -name 'appendonly.aof.manifest' -o -name '*.base.rdb' -o -name '*.base.aof' -o -name '*.incr.aof' \) -print -quit | grep -q .; then
    echo "No AOF artifact was found after Redis reported a successful AOF rewrite" >&2
    exit 1
  fi
  set_phase 50
fi

if (( phase < 60 )); then
  docker volume create "$REDIS_VOLUME" >/dev/null
  docker run --rm \
    -v "$REDIS_VOLUME:/target" \
    -v "$COPY_DIR:/source:ro" \
    redis:alpine \
    sh -ec 'rm -rf /target/* /target/.[!.]* /target/..?* 2>/dev/null || true; cp -a /source/. /target/; chown -R redis:redis /target'
  set_phase 60
fi

# If Redis was already restarted before an interruption, validate it and advance
# rather than trying to copy the source container again.
if (( phase < 70 )); then
  compose up -d --no-deps redis
  validate_persistent_redis
  set_phase 70
else
  if ! redis_is_persistent; then compose up -d --no-deps redis; fi
  validate_persistent_redis
fi

if (( phase < 80 )); then
  compose run --rm --no-deps migrations
  set_phase 80
fi

if [[ "$signer_running" == 1 ]]; then
  if (( phase < 90 )); then ensure_release_service dfx_signer_service 180 1; set_phase 90
  else wait_healthy dfx_signer_service 180 1 && assert_release_label dfx_signer_service
  fi
else
  (( phase < 90 )) && set_phase 90
fi

if (( phase < 100 )); then
  compose up -d --no-deps --force-recreate web
  if [[ "$COMPOSE_FILE" == *cloudflare* ]]; then wait_healthy web 300 2; else wait_healthy web 300 1; fi
  assert_release_label web
  set_phase 100
else
  if [[ "$COMPOSE_FILE" == *cloudflare* ]]; then wait_healthy web 300 2; else wait_healthy web 300 1; fi
  assert_release_label web
fi

if [[ "$cloudflared_running" == 1 ]]; then
  if (( phase < 110 )); then
    compose up -d --no-deps cloudflared
    wait_healthy cloudflared 60 1
    set_phase 110
  else
    wait_healthy cloudflared 60 1
  fi
else
  (( phase < 110 )) && set_phase 110
fi

if (( phase < 120 )); then
  if [[ "$celery_worker_running" == 1 ]]; then ensure_release_service celery_worker 120 0; fi
  if [[ "$celery_beat_running" == 1 ]]; then ensure_release_service celery_beat 120 0; fi
  set_phase 120
else
  if [[ "$celery_worker_running" == 1 ]]; then wait_healthy celery_worker 120 1 && assert_release_label celery_worker; fi
  if [[ "$celery_beat_running" == 1 ]]; then wait_healthy celery_beat 120 1 && assert_release_label celery_beat; fi
fi

if [[ "$deposit_running" == 1 ]]; then
  if (( phase < 130 )); then ensure_release_service deposit_service 120 1; set_phase 130
  else wait_healthy deposit_service 120 1 && assert_release_label deposit_service
  fi
else
  (( phase < 130 )) && set_phase 130
fi

if [[ "$sweeper_running" == 1 ]]; then
  [[ "$signer_running" == 1 ]] || {
    echo "Sweeper was running but signer was not; refusing to restart an inconsistent financial stack." >&2
    exit 1
  }
  if (( phase < 140 )); then ensure_release_service sweeper_service 120 1; set_phase 140
  else wait_healthy sweeper_service 120 1 && assert_release_label sweeper_service
  fi
else
  (( phase < 140 )) && set_phase 140
fi

# Final production gate. Nothing is marked complete until ingress and every
# service that was active before the migration are healthy again.
prod_preflight
wait_healthy db 60 1
validate_persistent_redis
if [[ "$signer_running" == 1 ]]; then wait_healthy dfx_signer_service 60 1; fi
if [[ "$cloudflared_running" == 1 ]]; then wait_healthy cloudflared 60 1; fi
if [[ "$celery_worker_running" == 1 ]]; then wait_healthy celery_worker 60 1; fi
if [[ "$celery_beat_running" == 1 ]]; then wait_healthy celery_beat 60 1; fi
if [[ "$deposit_running" == 1 ]]; then wait_healthy deposit_service 60 1; fi
if [[ "$sweeper_running" == 1 ]]; then wait_healthy sweeper_service 60 1; fi

printf '%s\n' "$MEDIACMS_RELEASE_SHA" > "${COMPLETE_FILE}.tmp.$$"
mv "${COMPLETE_FILE}.tmp.$$" "$COMPLETE_FILE"
rm -f "$STATE_FILE"
rm -rf "$COPY_DIR"
trap - EXIT

echo "Redis persistence migration complete: volume=$REDIS_VOLUME release=$MEDIACMS_RELEASE_SHA"
