#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

if redis_is_persistent; then
  echo "Redis already uses durable volume '$REDIS_VOLUME'."
  exit 0
fi

cat >&2 <<'__NOTICE__'
This is the ONE-TIME Redis persistence migration.
It deliberately creates a short ingress outage. After publishers are frozen,
AOF is enabled on the still-running Redis and its initial rewrite is allowed to
finish before /data is copied to the durable volume. Normal future deploys do
not stop ingress.
__NOTICE__

[[ "${CONFIRM_REDIS_MIGRATION:-}" == "$PROJECT" ]] || {
  echo "Set CONFIRM_REDIS_MIGRATION=$PROJECT to execute." >&2
  exit 2
}

command -v git >/dev/null || {
  echo "git is required for the image-isolated Redis bootstrap" >&2
  exit 2
}
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "Tracked working-tree changes are present; commit/stash them before Redis migration." >&2
  exit 2
fi
export MEDIACMS_RELEASE_SHA="$(git rev-parse HEAD)"

container_has_repo_root_mount() {
  local cid="$1"
  docker inspect -f '{{range .Mounts}}{{if eq .Destination "/home/mediacms.io/mediacms"}}yes{{end}}{{end}}' "$cid" 2>/dev/null | grep -q yes
}

legacy_app_mounts=0
for service in web celery_beat celery_worker; do
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    if container_has_repo_root_mount "$cid"; then
      legacy_app_mounts=1
      break 2
    fi
  done < <(service_container_ids_all "$service")
done

if [[ "$legacy_app_mounts" == 1 ]]; then
  # After git pull, legacy containers already expose the new checkout. Do not
  # launch fresh Django/Celery inspection processes inside them before schema
  # migration. Require at least one Docker-healthy web process instead.
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

deposit_running=0
sweeper_running=0
signer_running=0
[[ -n "$(service_container_ids deposit_service)" ]] && deposit_running=1
[[ -n "$(service_container_ids sweeper_service)" ]] && sweeper_running=1
[[ -n "$(service_container_ids dfx_signer_service)" ]] && signer_running=1

# Build every image that this one-time procedure may recreate while ingress is
# still live. Once web/Celery stop mounting the checkout, restarting from an old
# local image would otherwise silently roll the application backwards.
echo "Building target release images before Redis migration outage: $MEDIACMS_RELEASE_SHA"
compose build web
[[ "$deposit_running" == 1 ]] && compose_crypto build deposit_service
if [[ "$signer_running" == 1 || "$sweeper_running" == 1 ]]; then
  compose_crypto build dfx_signer_service
fi
[[ "$sweeper_running" == 1 ]] && compose_crypto build sweeper_service

if [[ "$legacy_app_mounts" == 1 ]]; then
  # Warm TERM shutdown finishes active tasks; queued tasks remain in Redis and
  # are copied into the durable volume. Avoid executing new checkout code in
  # the legacy containers.
  service_exists celery_beat && compose stop celery_beat >/dev/null || true
  [[ -n "$(service_container_ids celery_worker | head -n1)" ]] && compose stop celery_worker >/dev/null || true
else
  drain_celery
fi

# Financial loops also publish state through the internal web API. Quiesce them
# before taking web down, preserving exactly the subset that was running.
[[ "$deposit_running" == 1 ]] && compose_crypto stop deposit_service >/dev/null
[[ "$sweeper_running" == 1 ]] && compose_crypto stop sweeper_service >/dev/null

# Freeze request publishers before creating the final persistence boundary.
service_exists cloudflared && compose stop cloudflared >/dev/null || true
compose stop web >/dev/null || true

redis_id="$(service_container_ids redis | head -n1)"
if [[ -z "$redis_id" ]]; then
  echo "Existing Redis container not found; refusing to invent an empty queue." >&2
  exit 1
fi

# Keep an RDB backup as an independent rollback artifact, then follow Redis'
# documented live RDB -> AOF conversion procedure. CONFIG REWRITE is not used
# because the container is intentionally restarted from the Compose command
# that already declares appendonly=yes and appendfsync=everysec.
docker exec "$redis_id" redis-cli SAVE >/dev/null
docker exec "$redis_id" redis-cli CONFIG SET appendfsync everysec >/dev/null
docker exec "$redis_id" redis-cli CONFIG SET appendonly yes >/dev/null

wait_aof_ready() {
  local deadline=$((SECONDS + 900))
  local info enabled in_progress scheduled last_status

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

# Stop only after Redis confirms the initial AOF rewrite is complete.
compose stop redis >/dev/null

tmp="$(mktemp -d)"
migration_complete=0
cleanup() {
  local rc=$?
  if [[ "$migration_complete" == 1 ]]; then
    rm -rf "$tmp"
  else
    echo "Redis migration did not complete; preserving copied Redis data at: $tmp" >&2
  fi
  return "$rc"
}
trap cleanup EXIT

docker cp "$redis_id:/data/." "$tmp/"
if [[ ! -e "$tmp/dump.rdb" ]]; then
  echo "Expected RDB backup is missing from copied Redis /data" >&2
  exit 1
fi
if ! find "$tmp" -maxdepth 4 -type f \( -name 'appendonly.aof' -o -name 'appendonly.aof.manifest' -o -name '*.base.rdb' -o -name '*.base.aof' -o -name '*.incr.aof' \) -print -quit | grep -q .; then
  echo "No AOF artifact was found after Redis reported a successful AOF rewrite" >&2
  exit 1
fi

docker volume create "$REDIS_VOLUME" >/dev/null

docker run --rm \
  -v "$REDIS_VOLUME:/target" \
  -v "$tmp:/source:ro" \
  redis:alpine \
  sh -ec 'rm -rf /target/* /target/.[!.]* /target/..?* 2>/dev/null || true; cp -a /source/. /target/; chown -R redis:redis /target'

compose up -d db redis
wait_healthy db 300 1
wait_healthy redis 300 1

redis_is_persistent || {
  echo "Redis restarted without the expected durable volume '$REDIS_VOLUME'" >&2
  exit 1
}

appendonly="$(compose exec -T redis redis-cli --raw CONFIG GET appendonly | tail -n1 | tr -d '\r')"
appendfsync="$(compose exec -T redis redis-cli --raw CONFIG GET appendfsync | tail -n1 | tr -d '\r')"
aof_enabled="$(compose exec -T redis redis-cli --raw INFO persistence | tr -d '\r' | awk -F: '$1 == "aof_enabled" {print $2}')"
if [[ "$appendonly" != yes || "$appendfsync" != everysec || "$aof_enabled" != 1 ]]; then
  echo "Redis restarted without the expected AOF durability settings" >&2
  exit 1
fi

# The migration service is a one-shot gate and no longer retries forever.
compose run --rm migrations

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

# Recreate the signer first when it was part of the live stack. `--no-deps`
# prevents a web start from implicitly recreating an unbuilt dependency.
if [[ "$signer_running" == 1 ]]; then
  compose_crypto up -d --no-deps --force-recreate dfx_signer_service
  wait_healthy dfx_signer_service 180 1
  assert_release_label dfx_signer_service
fi

compose up -d --no-deps --force-recreate web
if [[ "$COMPOSE_FILE" == *cloudflare* ]]; then
  wait_healthy web 300 2
else
  wait_healthy web 300 1
fi
assert_release_label web

service_exists cloudflared && compose up -d cloudflared || true

compose up -d --no-deps --force-recreate celery_worker celery_beat
wait_healthy celery_worker 120 1
wait_healthy celery_beat 120 1
assert_release_label celery_worker
assert_release_label celery_beat

if [[ "$deposit_running" == 1 ]]; then
  compose_crypto up -d --no-deps --force-recreate deposit_service
  wait_healthy deposit_service 120 1
  assert_release_label deposit_service
fi
if [[ "$sweeper_running" == 1 ]]; then
  [[ "$signer_running" == 1 ]] || {
    echo "Sweeper was running but signer was not; refusing to restart an inconsistent financial stack." >&2
    exit 1
  }
  compose_crypto up -d --no-deps --force-recreate sweeper_service
  wait_healthy sweeper_service 120 1
  assert_release_label sweeper_service
fi

migration_complete=1
echo "Redis persistence migration complete: volume=$REDIS_VOLUME"
