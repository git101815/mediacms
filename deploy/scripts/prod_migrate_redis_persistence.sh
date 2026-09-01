#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

if redis_is_persistent; then
  echo "Redis already uses durable volume '$REDIS_VOLUME'."
  exit 0
fi

cat >&2 <<'__NOTICE__'
This is the ONE-TIME Redis persistence migration.
It deliberately creates a short ingress outage so no Celery/session/cache write
can occur between Redis SAVE and copying the old /data directory. Normal future
deploys do not stop ingress.
__NOTICE__

[[ "${CONFIRM_REDIS_MIGRATION:-}" == "$PROJECT" ]] || {
  echo "Set CONFIRM_REDIS_MIGRATION=$PROJECT to execute." >&2
  exit 2
}

prod_preflight

drain_celery

# Freeze request publishers for the SAVE/copy boundary. This is intentionally
# limited to the first persistence migration; future deployments stay online.
service_exists cloudflared && compose stop cloudflared >/dev/null || true
compose stop web >/dev/null || true

redis_id="$(service_container_ids redis | head -n1)"
if [[ -z "$redis_id" ]]; then
  echo "Existing Redis container not found; refusing to invent an empty queue." >&2
  exit 1
fi

docker exec "$redis_id" redis-cli SAVE >/dev/null
compose stop redis >/dev/null

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
docker cp "$redis_id:/data/." "$tmp/"
docker volume create "$REDIS_VOLUME" >/dev/null

docker run --rm \
  -v "$REDIS_VOLUME:/target" \
  -v "$tmp:/source:ro" \
  redis:alpine \
  sh -ec 'rm -rf /target/* /target/.[!.]* /target/..?* 2>/dev/null || true; cp -a /source/. /target/; chown -R redis:redis /target'

compose up -d db redis
wait_healthy db
wait_healthy redis

# The migration service is a one-shot gate and no longer retries forever.
compose run --rm migrations

compose up -d web
wait_healthy web
service_exists cloudflared && compose up -d cloudflared || true
compose up -d celery_worker celery_beat

echo "Redis persistence migration complete: volume=$REDIS_VOLUME"
