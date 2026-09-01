#!/usr/bin/env bash
set -euo pipefail

ROLLING_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROLLING_ROOT"

DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-7500}"
FRONTEND_BUILD_PROJECT="${FRONTEND_BUILD_PROJECT:-mediacms-frontend-build}"
ROLLING_STATE_DIR="${ROLLING_STATE_DIR:-$ROLLING_ROOT/.deploy-state}"

PROJECT=""
COMPOSE_FILE=""
REDIS_VOLUME=""
RELEASE_STATE_NAME=""
EXPECTED_WEB_REPLICAS=1
WEB_UPDATE_MODE="single"
REQUIRE_PERSISTENT_REDIS=0
ENVIRONMENT_NAME=""

CURRENT_SHA=""
BASE_SHA=""
STATE_FILE=""
CHANGED_FILES=()
FRONTEND_CHANGED=0
MAIN_IMAGE_CHANGED=0
DEPOSIT_IMAGE_CHANGED=0
SWEEPER_IMAGE_CHANGED=0
COMPOSE_CHANGED=0
RUNPOD_CHANGED=0
CELERY_DRAINED=0
FRONTEND_BUILT=0

configure_rolling_update() {
  PROJECT="$1"
  COMPOSE_FILE="$2"
  REDIS_VOLUME="$3"
  RELEASE_STATE_NAME="$4"
  EXPECTED_WEB_REPLICAS="$5"
  WEB_UPDATE_MODE="$6"
  REQUIRE_PERSISTENT_REDIS="$7"
  ENVIRONMENT_NAME="$8"

  export COMPOSE_PROJECT_NAME="$PROJECT"
  export REDIS_VOLUME_NAME="$REDIS_VOLUME"
  COMPOSE=(docker compose -p "$PROJECT" -f "$COMPOSE_FILE")
  FRONTEND_COMPOSE=(docker compose -p "$FRONTEND_BUILD_PROJECT" -f docker-compose-dev.yaml)
  STATE_FILE="$ROLLING_STATE_DIR/${RELEASE_STATE_NAME}.release"
}

compose() { "${COMPOSE[@]}" "$@"; }
compose_crypto() { "${COMPOSE[@]}" --profile crypto-workers "$@"; }
service_exists() { compose --profile crypto-workers config --services | grep -Fxq "$1"; }
service_container_ids() { compose ps -q "$1" 2>/dev/null || true; }
service_container_ids_all() { compose ps -a -q "$1" 2>/dev/null || true; }

die() {
  echo "rolling-update[$ENVIRONMENT_NAME]: $*" >&2
  exit 2
}

require_environment() {
  [[ -f manage.py && -f "$COMPOSE_FILE" && -f docker-compose-dev.yaml ]] || \
    die "run from the MediaCMS repository root"
  command -v git >/dev/null || die "git is required"
  command -v docker >/dev/null || die "docker is required"
  docker compose version >/dev/null || die "Docker Compose v2 is required"
  compose config >/dev/null

  if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    die "tracked working-tree changes are present; commit/stash them before a rolling update"
  fi
  CURRENT_SHA="$(git rev-parse HEAD)"
}

wait_healthy() {
  local service="$1"
  local timeout="${2:-300}"
  local expected="${3:-1}"
  local deadline=$((SECONDS + timeout))

  [[ "$expected" =~ ^[1-9][0-9]*$ ]] || die "invalid expected replica count '$expected'"

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

  echo "rolling-update[$ENVIRONMENT_NAME]: '$service' did not reach exactly $expected running/healthy replica(s)" >&2
  compose ps -a "$service" >&2 || true
  return 1
}

app_preflight() {
  local web_id
  web_id="$(service_container_ids web | head -n1)"
  [[ -n "$web_id" ]] || die "$ENVIRONMENT_NAME rolling update requires an already-running web service"
  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python manage.py prod_preflight
}

redis_is_persistent() {
  local cid source
  cid="$(service_container_ids redis | head -n1)"
  [[ -n "$cid" ]] || return 1
  source="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}' "$cid")"
  [[ "$source" == "$REDIS_VOLUME" ]]
}

load_release_delta() {
  BASE_SHA=""
  if [[ -f "$STATE_FILE" ]]; then
    BASE_SHA="$(tr -d '[:space:]' < "$STATE_FILE")"
    git cat-file -e "$BASE_SHA^{commit}" 2>/dev/null || die "release state contains unknown commit $BASE_SHA"
    git merge-base --is-ancestor "$BASE_SHA" "$CURRENT_SHA" || \
      die "deployed release $BASE_SHA is not an ancestor of $CURRENT_SHA; rolling rollback/branch switch is refused"
  fi

  if [[ -n "$BASE_SHA" && "$BASE_SHA" == "$CURRENT_SHA" ]]; then
    CHANGED_FILES=()
    return 0
  fi

  if [[ -z "$BASE_SHA" ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: no release state yet; using conservative first-run plan"
    mapfile -t CHANGED_FILES < <(git ls-files)
  else
    mapfile -t CHANGED_FILES < <(git diff --name-only "$BASE_SHA" "$CURRENT_SHA")
  fi
}

changed_matches() {
  local pattern="$1" path
  for path in "${CHANGED_FILES[@]}"; do
    [[ "$path" =~ $pattern ]] && return 0
  done
  return 1
}

main_image_inputs_changed() {
  local path
  for path in "${CHANGED_FILES[@]}"; do
    case "$path" in
      frontend/*|deposit_service/*|sweeper_service/*|runpod_worker/*|docs/*|tests/*|.github/*|maintenance/*|static/*|docker-compose*.yaml|deploy/scripts/*|*.md|.env.example|.gitignore)
        continue
        ;;
      *)
        return 0
        ;;
    esac
  done
  return 1
}

classify_release() {
  local first_run=0
  [[ -z "$BASE_SHA" ]] && first_run=1

  if (( first_run )) || changed_matches '^frontend/'; then FRONTEND_CHANGED=1; fi
  if (( first_run )) || main_image_inputs_changed; then MAIN_IMAGE_CHANGED=1; fi
  if (( first_run )) || changed_matches '^deposit_service/(Dockerfile|requirements\.txt|app/)'; then DEPOSIT_IMAGE_CHANGED=1; fi
  if (( first_run )) || changed_matches '^sweeper_service/(Dockerfile|requirements\.txt|app/)'; then SWEEPER_IMAGE_CHANGED=1; fi
  if changed_matches "^${COMPOSE_FILE//./\\.}$"; then COMPOSE_CHANGED=1; fi
  if changed_matches '^runpod_worker/'; then RUNPOD_CHANGED=1; fi
}

print_plan() {
  echo "rolling-update[$ENVIRONMENT_NAME]: release ${BASE_SHA:-<unknown>} -> $CURRENT_SHA"
  echo "  changed tracked files: ${#CHANGED_FILES[@]}"
  echo "  frontend build:        $FRONTEND_CHANGED"
  echo "  main image rebuild:    $MAIN_IMAGE_CHANGED"
  echo "  deposit rebuild:       $DEPOSIT_IMAGE_CHANGED"
  echo "  sweeper/signer build:  $SWEEPER_IMAGE_CHANGED"
  if (( COMPOSE_CHANGED )); then
    echo "  NOTE: $COMPOSE_FILE changed. This updater intentionally never recreates PostgreSQL/Redis."
  fi
  if (( RUNPOD_CHANGED )); then
    echo "  NOTE: runpod_worker changed; its external deployment is not performed by Docker Compose."
  fi
}

build_required_images() {
  if (( MAIN_IMAGE_CHANGED )); then
    compose build web
  fi
  if (( DEPOSIT_IMAGE_CHANGED )); then
    compose_crypto build deposit_service
  fi
  if (( SWEEPER_IMAGE_CHANGED )); then
    compose_crypto build dfx_signer_service sweeper_service
  fi
}

check_pending_migrations() {
  local rc=0
  set +e
  compose run --rm --no-deps web python deploy/scripts/check_rolling_migrations.py
  rc=$?
  set -e
  if (( rc == 0 )); then
    return 0
  fi
  if (( rc == 3 )) && [[ "${ALLOW_REVIEWED_ROLLING_MIGRATIONS:-0}" == 1 ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: proceeding with explicitly reviewed migration override" >&2
    return 0
  fi
  return "$rc"
}

build_frontend_dist() {
  (( FRONTEND_CHANGED )) || return 0
  echo "rolling-update[$ENVIRONMENT_NAME]: building frontend in an isolated one-shot dev container"
  "${FRONTEND_COMPOSE[@]}" run --rm --no-deps frontend bash -lc 'npm install && npm run dist'
  [[ -d frontend/dist/static ]] || die "frontend build completed without frontend/dist/static"
  FRONTEND_BUILT=1
}

publish_frontend_dist() {
  (( FRONTEND_BUILT )) || return 0
  # Overlay instead of deleting static/: old hashed assets remain available
  # while old and new web replicas overlap during the production rotation.
  cp -a frontend/dist/static/. static/
}

celery_queue_count() {
  local web_id
  web_id="$(service_container_ids web | head -n1)"
  [[ -n "$web_id" ]] || return 1
  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python - <<'__CELERY_QUEUE_COUNT_PY__'
from django.conf import settings
from redis import Redis

client = Redis.from_url(settings.BROKER_URL)
try:
    print(int(client.llen("short_tasks")) + int(client.llen("long_tasks")))
finally:
    client.close()
__CELERY_QUEUE_COUNT_PY__
}

celery_active_reserved_count() {
  local worker_id web_id
  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ -z "$worker_id" ]]; then echo 0; return 0; fi
  web_id="$(service_container_ids web | head -n1)"
  [[ -n "$web_id" ]] || return 1
  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python - <<'__CELERY_ACTIVE_COUNT_PY__'
from cms.celery import app

inspect = app.control.inspect(timeout=3.0)
active = inspect.active()
reserved = inspect.reserved()
if active is None or reserved is None or (not active and not reserved):
    raise SystemExit("No Celery worker inspection response")
print(sum(len(items or []) for items in active.values()) + sum(len(items or []) for items in reserved.values()))
__CELERY_ACTIVE_COUNT_PY__
}

celery_work_count() {
  local queued active_reserved worker_id
  queued="$(celery_queue_count)" || return 1
  [[ "$queued" =~ ^[0-9]+$ ]] || return 1
  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ -z "$worker_id" ]]; then
    (( queued == 0 )) || return 1
    echo 0
    return 0
  fi
  active_reserved="$(celery_active_reserved_count)" || return 1
  [[ "$active_reserved" =~ ^[0-9]+$ ]] || return 1
  echo $((queued + active_reserved))
}

drain_celery() {
  service_exists celery_beat && compose stop celery_beat >/dev/null || true
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS)) count
  while (( SECONDS < deadline )); do
    if ! count="$(celery_work_count)"; then
      echo "rolling-update[$ENVIRONMENT_NAME]: cannot reliably inspect Celery; refusing update" >&2
      return 1
    fi
    if (( count == 0 )); then
      compose stop celery_worker >/dev/null || true
      CELERY_DRAINED=1
      return 0
    fi
    echo "rolling-update[$ENVIRONMENT_NAME]: waiting for Celery drain: $count active/reserved/queued"
    sleep 5
  done
  echo "rolling-update[$ENVIRONMENT_NAME]: Celery drain timed out" >&2
  return 1
}

run_migrations() {
  # check_rolling_migrations already proved the pending plan is additive or
  # the operator explicitly reviewed it. The unique PostgreSQL instance stays live.
  compose run --rm migrations
}

stop_crypto_loops_if_needed() {
  DEPOSIT_WAS_RUNNING=0
  SWEEPER_WAS_RUNNING=0
  [[ -n "$(service_container_ids deposit_service)" ]] && DEPOSIT_WAS_RUNNING=1
  [[ -n "$(service_container_ids sweeper_service)" ]] && SWEEPER_WAS_RUNNING=1

  if (( DEPOSIT_IMAGE_CHANGED || SWEEPER_IMAGE_CHANGED )); then
    if (( DEPOSIT_WAS_RUNNING )); then compose_crypto stop deposit_service >/dev/null; fi
    if (( SWEEPER_WAS_RUNNING )); then compose_crypto stop sweeper_service >/dev/null; fi
  fi
}

rolling_replace_unbound_service() {
  local service="$1" timeout="$2"
  mapfile -t old_ids < <(service_container_ids "$service")
  if (( ${#old_ids[@]} == 0 )); then
    compose up -d --no-deps "$service"
    wait_healthy "$service" "$timeout" 1
    return 0
  fi
  compose up -d --no-deps --no-recreate --scale "$service=2" "$service"
  wait_healthy "$service" "$timeout" 2
  local old_id
  for old_id in "${old_ids[@]}"; do
    [[ -n "$old_id" ]] || continue
    docker stop --time 30 "$old_id" >/dev/null
    docker rm "$old_id" >/dev/null
  done
  compose up -d --no-deps --no-recreate --scale "$service=1" "$service"
  wait_healthy "$service" "$timeout" 1
}

update_signer_if_needed() {
  (( SWEEPER_IMAGE_CHANGED )) || return 0
  rolling_replace_unbound_service dfx_signer_service 180
}

update_web_scaled() {
  mapfile -t old_ids < <(service_container_ids web)
  (( ${#old_ids[@]} == EXPECTED_WEB_REPLICAS )) || \
    die "expected $EXPECTED_WEB_REPLICAS running web replicas before production rotation, found ${#old_ids[@]}"

  local temporary=$((EXPECTED_WEB_REPLICAS + 1))
  compose up -d --no-deps --no-recreate --scale "web=$temporary" web
  wait_healthy web 300 "$temporary"

  local old_id
  for old_id in "${old_ids[@]}"; do
    [[ -n "$old_id" ]] || continue
    docker stop --time 90 "$old_id" >/dev/null
    docker rm "$old_id" >/dev/null
    compose up -d --no-deps --no-recreate --scale "web=$temporary" web
    wait_healthy web 300 "$temporary"
  done

  compose up -d --no-deps --no-recreate --scale "web=$EXPECTED_WEB_REPLICAS" web
  wait_healthy web 300 "$EXPECTED_WEB_REPLICAS"
}

update_web_single() {
  # Staging binds host :80, so two web containers cannot coexist without a
  # proxy/port architecture change. DB/Redis remain online; only staging web
  # is recreated and there is deliberately no DNS/maintenance manipulation.
  compose up -d --no-deps --force-recreate web
  wait_healthy web 300 1
}

update_web() {
  case "$WEB_UPDATE_MODE" in
    scaled) update_web_scaled ;;
    single) update_web_single ;;
    *) die "unknown WEB_UPDATE_MODE=$WEB_UPDATE_MODE" ;;
  esac
}

restart_celery() {
  compose up -d --no-deps --force-recreate celery_worker celery_beat
  wait_healthy celery_worker 120 1
  wait_healthy celery_beat 120 1
  CELERY_DRAINED=0
}

restart_crypto_loops_if_needed() {
  if (( DEPOSIT_IMAGE_CHANGED )); then
    if (( DEPOSIT_WAS_RUNNING )) || [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
      compose_crypto up -d --no-deps --force-recreate deposit_service
      wait_healthy deposit_service 120 1
    fi
  fi
  if (( SWEEPER_IMAGE_CHANGED )); then
    if (( SWEEPER_WAS_RUNNING )) || [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
      compose_crypto up -d --no-deps --force-recreate sweeper_service
      wait_healthy sweeper_service 120 1
    fi
  fi
}

record_release() {
  mkdir -p "$ROLLING_STATE_DIR"
  printf '%s\n' "$CURRENT_SHA" > "$STATE_FILE"
}

rolling_failure_notice() {
  local rc=$?
  if (( rc != 0 )); then
    echo "rolling-update[$ENVIRONMENT_NAME]: FAILED; release state was not advanced." >&2
    if (( CELERY_DRAINED )); then
      echo "rolling-update[$ENVIRONMENT_NAME]: Celery remains intentionally stopped (fail-closed). Fix the error and rerun the same command." >&2
    fi
  fi
  return "$rc"
}

rolling_update_main() {
  trap rolling_failure_notice EXIT
  require_environment
  app_preflight

  if (( REQUIRE_PERSISTENT_REDIS )) && ! redis_is_persistent; then
    cat >&2 <<EOF
rolling-update[$ENVIRONMENT_NAME]: Redis is not mounted on '$REDIS_VOLUME'.
Run the one-time persistence migration before the production rolling updater:
  CONFIRM_REDIS_MIGRATION=$PROJECT deploy/scripts/prod_migrate_redis_persistence.sh
EOF
    exit 2
  fi

  load_release_delta
  if [[ -n "$BASE_SHA" && "$BASE_SHA" == "$CURRENT_SHA" ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: $CURRENT_SHA is already recorded as deployed; health/preflight OK"
    trap - EXIT
    return 0
  fi

  classify_release
  print_plan

  # Everything below this point is arranged so build/review failures happen
  # before any live process is stopped.
  build_required_images
  check_pending_migrations
  build_frontend_dist

  drain_celery
  run_migrations
  publish_frontend_dist
  stop_crypto_loops_if_needed
  update_signer_if_needed
  update_web
  restart_celery
  restart_crypto_loops_if_needed
  app_preflight
  record_release

  trap - EXIT
  echo "rolling-update[$ENVIRONMENT_NAME]: complete at $CURRENT_SHA; PostgreSQL and Redis were never recreated"
}
