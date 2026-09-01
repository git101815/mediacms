#!/usr/bin/env bash
set -euo pipefail

PROJECT="${COMPOSE_PROJECT_NAME:-mediacms-prod}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose-cloudflare.yaml}"
REDIS_VOLUME="${REDIS_VOLUME_NAME:-mediacms-prod-redis-data}"
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-7500}"

if [[ "$PROJECT" != "mediacms-prod" && "${ALLOW_NON_PROD_PROJECT:-0}" != "1" ]]; then
  echo "Refusing project '$PROJECT'; expected mediacms-prod" >&2
  exit 2
fi
if [[ ! -f manage.py || ! -f "$COMPOSE_FILE" ]]; then
  echo "Run from the MediaCMS repository root; compose file '$COMPOSE_FILE' is missing" >&2
  exit 2
fi

export COMPOSE_PROJECT_NAME="$PROJECT"
export REDIS_VOLUME_NAME="$REDIS_VOLUME"
COMPOSE=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")

compose() { "${COMPOSE[@]}" "$@"; }
compose_crypto() { "${COMPOSE[@]}" --profile crypto-workers "$@"; }
service_exists() { compose --profile crypto-workers config --services | grep -Fxq "$1"; }
service_container_ids() { compose ps -q "$1" 2>/dev/null || true; }

wait_healthy() {
  local service="$1" timeout="${2:-300}" deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    mapfile -t ids < <(service_container_ids "$service")
    if (( ${#ids[@]} > 0 )); then
      local all_ok=1 cid state health
      for cid in "${ids[@]}"; do
        state="$(docker inspect -f '{{.State.Status}}' "$cid")"
        health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid")"
        if [[ "$state" != running || ( "$health" != healthy && "$health" != none ) ]]; then
          all_ok=0
          break
        fi
      done
      (( all_ok )) && return 0
    fi
    sleep 2
  done
  echo "Service '$service' did not become healthy" >&2
  compose ps "$service" >&2 || true
  return 1
}

prod_preflight() {
  local web_id
  web_id="$(service_container_ids web | head -n1)"
  if [[ -n "$web_id" ]]; then
    compose exec -T web python manage.py prod_preflight || true
  fi
}

celery_work_count() {
  local worker_id
  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ -z "$worker_id" ]]; then
    echo 0
    return
  fi
  compose exec -T celery_worker python - <<'__CELERY_COUNT_PY__'
from cms.celery import app
from django.conf import settings
from redis import Redis

count = 0
inspect = app.control.inspect(timeout=2.0)
for method_name in ("active", "reserved"):
    try:
        rows = getattr(inspect, method_name)() or {}
    except Exception:
        rows = {}
    count += sum(len(items or []) for items in rows.values())

client = Redis.from_url(settings.BROKER_URL)
try:
    count += int(client.llen("short_tasks"))
    count += int(client.llen("long_tasks"))
finally:
    client.close()
print(count)
__CELERY_COUNT_PY__
}

drain_celery() {
  service_exists celery_beat && compose stop celery_beat >/dev/null || true
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS)) count
  while (( SECONDS < deadline )); do
    count="$(celery_work_count | tail -n1 | tr -dc '0-9')"
    count="${count:-0}"
    if (( count == 0 )); then
      compose stop celery_worker >/dev/null || true
      return 0
    fi
    echo "Waiting for Celery drain: $count active/reserved/queued"
    sleep 5
  done
  echo "Celery did not drain within ${DRAIN_TIMEOUT_SECONDS}s; refusing destructive restart" >&2
  return 1
}

redis_is_persistent() {
  local cid source
  cid="$(service_container_ids redis | head -n1)"
  [[ -n "$cid" ]] || return 1
  source="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}' "$cid")"
  [[ "$source" == "$REDIS_VOLUME" ]]
}
