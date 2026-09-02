#!/usr/bin/env bash
set -euo pipefail

PROD_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROD_ROOT"

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

git_repo() { git -c "safe.directory=$PROD_ROOT" "$@"; }
compose() { "${COMPOSE[@]}" "$@"; }
compose_crypto() { "${COMPOSE[@]}" --profile crypto-workers "$@"; }
service_exists() { compose --profile crypto-workers config --services | grep -Fxq "$1"; }
service_container_ids() { compose ps -q "$1" 2>/dev/null || true; }
service_container_ids_all() { compose ps -a -q "$1" 2>/dev/null || true; }
service_is_running() { [[ -n "$(service_container_ids "$1" | head -n1)" ]]; }
stop_service_if_running() {
  local service="$1"
  if service_is_running "$service"; then
    compose stop "$service" >/dev/null
  fi
}
stop_crypto_service_if_running() {
  local service="$1"
  if service_is_running "$service"; then
    compose_crypto stop "$service" >/dev/null
  fi
}
container_has_repo_root_mount() {
  local cid="$1"
  docker inspect -f '{{range .Mounts}}{{if eq .Destination "/home/mediacms.io/mediacms"}}yes{{end}}{{end}}' "$cid" 2>/dev/null | grep -q yes
}
legacy_app_mounts_present() {
  local service cid
  for service in web celery_beat celery_worker; do
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      if container_has_repo_root_mount "$cid"; then return 0; fi
    done < <(service_container_ids_all "$service")
  done
  return 1
}

wait_healthy() {
  local service="$1"
  local timeout="${2:-300}"
  local expected="${3:-1}"
  local deadline=$((SECONDS + timeout))

  [[ "$expected" =~ ^[1-9][0-9]*$ ]] || {
    echo "Invalid expected replica count '$expected' for service '$service'" >&2
    return 2
  }

  while (( SECONDS < deadline )); do
    mapfile -t ids < <(service_container_ids_all "$service")
    if (( ${#ids[@]} == expected )); then
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

  echo "Service '$service' did not reach exactly $expected running/healthy replica(s)" >&2
  compose ps -a "$service" >&2 || true
  return 1
}

prod_preflight() {
  local web_id
  web_id="$(service_container_ids web | head -n1)"
  if [[ -z "$web_id" ]]; then
    echo "Production preflight requires at least one running web container" >&2
    return 1
  fi
  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python manage.py prod_preflight
}

celery_queue_count() {
  local web_id
  web_id="$(service_container_ids web | head -n1)"
  if [[ -z "$web_id" ]]; then
    echo "Cannot inspect Celery queues without a running web container" >&2
    return 1
  fi

  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python - <<'__CELERY_QUEUE_COUNT_PY__'
from django.conf import settings
from redis import Redis

client = Redis.from_url(settings.BROKER_URL)
try:
    count = int(client.llen("short_tasks")) + int(client.llen("long_tasks"))
finally:
    client.close()
print(count)
__CELERY_QUEUE_COUNT_PY__
}

celery_active_reserved_count() {
  local worker_id web_id
  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ -z "$worker_id" ]]; then
    echo 0
    return 0
  fi

  web_id="$(service_container_ids web | head -n1)"
  if [[ -z "$web_id" ]]; then
    echo "Cannot inspect active Celery work without a running web container" >&2
    return 1
  fi

  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python - <<'__CELERY_ACTIVE_COUNT_PY__'
from cms.celery import app

inspect = app.control.inspect(timeout=3.0)
active = inspect.active()
reserved = inspect.reserved()
if active is None or reserved is None or (not active and not reserved):
    raise SystemExit("No Celery worker inspection response")
print(
    sum(len(items or []) for items in active.values())
    + sum(len(items or []) for items in reserved.values())
)
__CELERY_ACTIVE_COUNT_PY__
}

celery_work_count() {
  local queued active_reserved worker_id

  if ! queued="$(celery_queue_count)"; then
    return 1
  fi
  [[ "$queued" =~ ^[0-9]+$ ]] || {
    echo "Unexpected Celery queue count: '$queued'" >&2
    return 1
  }

  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ -z "$worker_id" ]]; then
    if (( queued > 0 )); then
      echo "Celery worker is not running while $queued queued task(s) remain; refusing drain" >&2
      return 1
    fi
    echo 0
    return 0
  fi

  if ! active_reserved="$(celery_active_reserved_count)"; then
    echo "Could not reliably inspect active/reserved Celery work; refusing drain" >&2
    return 1
  fi
  [[ "$active_reserved" =~ ^[0-9]+$ ]] || {
    echo "Unexpected Celery active/reserved count: '$active_reserved'" >&2
    return 1
  }

  echo $((queued + active_reserved))
}

drain_celery() {
  if service_exists celery_beat; then stop_service_if_running celery_beat || return 1; fi
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS)) count

  while (( SECONDS < deadline )); do
    if ! count="$(celery_work_count)"; then
      return 1
    fi
    if (( count == 0 )); then
      stop_service_if_running celery_worker || return 1
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
