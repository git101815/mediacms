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

prod_preflight

deposit_running=0
sweeper_running=0
[[ -n "$(service_container_ids deposit_service)" ]] && deposit_running=1
[[ -n "$(service_container_ids sweeper_service)" ]] && sweeper_running=1

drain_celery

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

compose up -d web
if [[ "$COMPOSE_FILE" == *cloudflare* ]]; then
  wait_healthy web 300 2
else
  wait_healthy web 300 1
fi
service_exists cloudflared && compose up -d cloudflared || true
compose up -d celery_worker celery_beat
wait_healthy celery_worker 120 1
wait_healthy celery_beat 120 1

if [[ "$deposit_running" == 1 ]]; then
  compose_crypto up -d --no-deps deposit_service
  wait_healthy deposit_service 120 1
fi
if [[ "$sweeper_running" == 1 ]]; then
  wait_healthy dfx_signer_service 180 1
  compose_crypto up -d --no-deps sweeper_service
  wait_healthy sweeper_service 120 1
fi

migration_complete=1
echo "Redis persistence migration complete: volume=$REDIS_VOLUME"
