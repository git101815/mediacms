#!/usr/bin/env bash
set -euo pipefail

ROLLING_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROLLING_ROOT"

DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-7500}"
FRONTEND_BUILD_PROJECT="${FRONTEND_BUILD_PROJECT:-mediacms-frontend-build}"
ROLLING_STATE_DIR="${ROLLING_STATE_DIR:-$ROLLING_ROOT/.deploy-state}"
RELEASE_LABEL="io.mediacms.release"

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
INPROGRESS_FILE=""
STATIC_STATE_FILE=""
LOCK_FILE=""
ROLLING_LOCK_FD=""
CHANGED_FILES=()
FRONTEND_CHANGED=0
MAIN_IMAGE_CHANGED=0
APP_CONFIG_CHANGED=0
DEPOSIT_IMAGE_CHANGED=0
DEPOSIT_CONFIG_CHANGED=0
SWEEPER_IMAGE_CHANGED=0
SWEEPER_CONFIG_CHANGED=0
SIGNER_IMAGE_CHANGED=0
SIGNER_CONFIG_CHANGED=0
LEGACY_BOOTSTRAP=0
CELERY_DRAINED=0
FRONTEND_BUILT=0
DEPOSIT_WAS_RUNNING=""
SWEEPER_WAS_RUNNING=""
SIGNER_WAS_RUNNING=""
STATIC_RELEASE_DIR=""
STATIC_CHANGED=0
STAGING_INGRESS_CHANGED=0
STAGING_INGRESS_VALIDATED=0
# Keep a few completed snapshots for rollback in addition to every snapshot
# protected by a release/in-progress state file.
STATIC_RELEASE_KEEP_COUNT="${STATIC_RELEASE_KEEP_COUNT:-3}"
STATIC_TMP_MAX_AGE_MINUTES="${STATIC_TMP_MAX_AGE_MINUTES:-1440}"

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
  INPROGRESS_FILE="$ROLLING_STATE_DIR/${RELEASE_STATE_NAME}.inprogress"
  STATIC_STATE_FILE="$ROLLING_STATE_DIR/${RELEASE_STATE_NAME}.static-release"
  if [[ "$ENVIRONMENT_NAME" == "production" ]]; then
    LOCK_FILE="$ROLLING_STATE_DIR/production.mutation.lock"
  else
    LOCK_FILE="$ROLLING_STATE_DIR/${RELEASE_STATE_NAME}.lock"
  fi
}

git_repo() { git -c "safe.directory=$ROLLING_ROOT" "$@"; }
compose() { "${COMPOSE[@]}" "$@"; }
compose_crypto() { "${COMPOSE[@]}" --profile crypto-workers "$@"; }
service_exists() { compose --profile crypto-workers config --services | grep -Fxq "$1"; }
service_container_ids() { compose ps -q "$1" 2>/dev/null || true; }
service_container_ids_all() { compose ps -a -q "$1" 2>/dev/null || true; }
service_is_running() { [[ -n "$(service_container_ids "$1" | head -n1)" ]]; }
stop_service_if_running() {
  local service="$1"
  if service_is_running "$service"; then compose stop "$service" >/dev/null; fi
}
stop_crypto_service_if_running() {
  local service="$1"
  if service_is_running "$service"; then compose_crypto stop "$service" >/dev/null; fi
}

die() {
  echo "rolling-update[$ENVIRONMENT_NAME]: $*" >&2
  exit 2
}

container_state() { docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true; }
container_health() { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null || true; }
container_release() { docker inspect -f "{{ index .Config.Labels \"$RELEASE_LABEL\" }}" "$1" 2>/dev/null || true; }

container_is_healthy() {
  local cid="$1" state health
  state="$(container_state "$cid")"
  health="$(container_health "$cid")"
  [[ "$state" == running && ( "$health" == healthy || "$health" == none ) ]]
}

container_has_repo_root_mount() {
  local cid="$1"
  docker inspect -f '{{range .Mounts}}{{if eq .Destination "/home/mediacms.io/mediacms"}}yes{{end}}{{end}}' "$cid" 2>/dev/null | grep -q yes
}

require_environment() {
  [[ -f manage.py && -f "$COMPOSE_FILE" && -f docker-compose-dev.yaml ]] || \
    die "run from the MediaCMS repository root"
  command -v git >/dev/null || die "git is required"
  command -v docker >/dev/null || die "docker is required"
  command -v python3 >/dev/null || die "python3 is required"
  command -v flock >/dev/null || die "flock is required"
  docker compose version >/dev/null || die "Docker Compose v2 is required"
  compose config >/dev/null

  if [[ -n "$(git_repo status --porcelain --untracked-files=all)" ]]; then
    die "working-tree changes or untracked files are present; commit/stash/remove them before a rolling update"
  fi
  CURRENT_SHA="$(git_repo rev-parse HEAD)"
  export MEDIACMS_RELEASE_SHA="$CURRENT_SHA"
  STATIC_RELEASE_DIR="$ROLLING_STATE_DIR/static/$CURRENT_SHA"
  export MEDIACMS_STATIC_DIR="$STATIC_RELEASE_DIR"
}

acquire_update_lock() {
  mkdir -p "$ROLLING_STATE_DIR"
  exec {ROLLING_LOCK_FD}>"$LOCK_FILE"
  flock -n "$ROLLING_LOCK_FD" || die "another $ENVIRONMENT_NAME rolling update is already running"
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
      local all_ok=1 cid
      for cid in "${ids[@]}"; do
        if ! container_is_healthy "$cid"; then
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

healthy_service_count() {
  local service="$1" count=0 cid
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    if container_is_healthy "$cid"; then count=$((count + 1)); fi
  done < <(service_container_ids "$service")
  echo "$count"
}

current_release_healthy_ids() {
  local service="$1" cid
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    if [[ "$(container_release "$cid")" == "$CURRENT_SHA" ]] && container_is_healthy "$cid"; then
      echo "$cid"
    fi
  done < <(service_container_ids "$service")
}

current_release_healthy_count() {
  local service="$1"
  current_release_healthy_ids "$service" | grep -c . || true
}

wait_current_release_healthy_count() {
  local service="$1" expected="$2" timeout="${3:-300}" deadline=$((SECONDS + timeout)) count
  while (( SECONDS < deadline )); do
    count="$(current_release_healthy_count "$service")"
    if (( count >= expected )); then return 0; fi
    sleep 2
  done
  echo "rolling-update[$ENVIRONMENT_NAME]: '$service' did not reach $expected healthy replica(s) for release $CURRENT_SHA" >&2
  compose ps -a "$service" >&2 || true
  return 1
}

find_healthy_web_id() {
  local cid
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    if container_is_healthy "$cid"; then echo "$cid"; return 0; fi
  done < <(service_container_ids web)
  return 1
}

app_preflight() {
  local web_id
  web_id="$(find_healthy_web_id || true)"
  [[ -n "$web_id" ]] || die "$ENVIRONMENT_NAME rolling update requires an already-running healthy web service"
  docker exec -i -w /home/mediacms.io/mediacms "$web_id" python manage.py prod_preflight
}

verify_runtime_dependencies() {
  local signer_count
  signer_count="$(service_container_ids_all dfx_signer_service | grep -c . || true)"

  if [[ "$ENVIRONMENT_NAME" == "production" ]]; then
    if service_exists dfx_signer_service; then
      (( signer_count >= 1 )) || die "production signer is not deployed"
      (( $(healthy_service_count dfx_signer_service) >= 1 )) || die "dfx_signer_service has no healthy replica"
    fi
    if service_exists cloudflared; then
      wait_healthy cloudflared 60 1 || die "cloudflared ingress is not healthy"
    fi
  elif (( signer_count > 0 )); then
    # Staging may legitimately run without the optional crypto profile. If a
    # signer is deployed, require at least one healthy replica, but do not make
    # it a prerequisite for an otherwise web-only staging deployment.
    (( $(healthy_service_count dfx_signer_service) >= 1 )) || die "deployed dfx_signer_service has no healthy replica"
  fi

  # During the one-time topology migration staging_ingress is not running yet.
  # After it exists, its healthcheck is end-to-end through the web service.
  if [[ "$ENVIRONMENT_NAME" == "staging" ]] && service_exists staging_ingress && service_is_running staging_ingress; then
    wait_healthy staging_ingress 90 1 || die "staging ingress is not serving the web application"
  fi
}
legacy_preflight() {
  local count
  count="$(healthy_service_count web)"
  (( count >= 1 )) || die "legacy bootstrap requires at least one healthy web container"
  echo "rolling-update[$ENVIRONMENT_NAME]: legacy bind-mounted application containers detected; entering one-time bootstrap mode"
}

detect_legacy_bootstrap() {
  LEGACY_BOOTSTRAP=0
  local service cid
  for service in web celery_beat celery_worker; do
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      if container_has_repo_root_mount "$cid"; then
        LEGACY_BOOTSTRAP=1
        return 0
      fi
    done < <(service_container_ids_all "$service")
  done
}

redis_is_persistent() {
  local cid source
  cid="$(service_container_ids redis | head -n1)"
  [[ -n "$cid" ]] || return 1
  source="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{if .Name}}{{.Name}}{{else}}{{.Source}}{{end}}{{end}}{{end}}' "$cid")"
  [[ "$source" == "$REDIS_VOLUME" ]]
}

progress_get() {
  local key="$1" default="${2:-}" line
  [[ -f "$INPROGRESS_FILE" ]] || { printf '%s\n' "$default"; return 0; }
  line="$(grep -E "^${key}=" "$INPROGRESS_FILE" | tail -n1 || true)"
  [[ -n "$line" ]] && printf '%s\n' "${line#*=}" || printf '%s\n' "$default"
}

progress_set() {
  local key="$1" value="$2" tmp
  mkdir -p "$ROLLING_STATE_DIR"
  tmp="${INPROGRESS_FILE}.tmp.$$"
  if [[ -f "$INPROGRESS_FILE" ]]; then
    grep -Ev "^${key}=" "$INPROGRESS_FILE" > "$tmp" || true
  else
    : > "$tmp"
  fi
  printf '%s=%s\n' "$key" "$value" >> "$tmp"
  mv "$tmp" "$INPROGRESS_FILE"
}

bind_progress_to_target() {
  local saved
  if [[ -f "$INPROGRESS_FILE" ]]; then
    saved="$(progress_get target_sha '')"
    [[ -n "$saved" ]] || die "unfinished deployment state has no target_sha; refusing ambiguous resume"
    [[ "$saved" == "$CURRENT_SHA" ]] || \
      die "unfinished deployment targets $saved but checkout is $CURRENT_SHA; restore that checkout or remove the state only after manual recovery"
  else
    progress_set target_sha "$CURRENT_SHA"
  fi
}

load_release_delta() {
  BASE_SHA=""
  if [[ -f "$STATE_FILE" ]]; then
    BASE_SHA="$(tr -d '[:space:]' < "$STATE_FILE")"
    git_repo cat-file -e "$BASE_SHA^{commit}" 2>/dev/null || die "release state contains unknown commit $BASE_SHA"
    git_repo merge-base --is-ancestor "$BASE_SHA" "$CURRENT_SHA" || \
      die "deployed release $BASE_SHA is not an ancestor of $CURRENT_SHA; rolling rollback/branch switch is refused"
  fi

  if [[ -n "$BASE_SHA" && "$BASE_SHA" == "$CURRENT_SHA" ]]; then
    CHANGED_FILES=()
    return 0
  fi

  if [[ -z "$BASE_SHA" ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: no release state yet; using conservative first-run plan"
    mapfile -t CHANGED_FILES < <(git_repo ls-files)
  else
    mapfile -t CHANGED_FILES < <(git_repo diff --name-only "$BASE_SHA" "$CURRENT_SHA")
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
      deploy/scripts/check_rolling_migrations.py)
        return 0
        ;;
      deploy/docker/staging_ingress.conf)
        # Stable staging infrastructure, not an input of the MediaCMS image.
        continue
        ;;
      frontend/*|deposit_service/*|sweeper_service/*|runpod_worker/*|docs/*|tests/*|.github/*|maintenance/*|docker-compose*.yaml|deploy/scripts/*|*.md|.env.example|.gitignore)
        continue
        ;;
      *)
        return 0
        ;;
    esac
  done
  return 1
}

classify_compose_delta() {
  [[ -n "$BASE_SHA" ]] || return 0
  changed_matches "^${COMPOSE_FILE//./\\.}$" || return 0

  # Process substitution does not propagate the producer's exit status to the
  # while loop. Capture the classifier first so any parser/git failure is
  # fail-closed instead of being mistaken for "no Compose changes".
  local part service compose_delta
  if ! compose_delta="$(python3 deploy/scripts/classify_compose_changes.py "$BASE_SHA" "$COMPOSE_FILE")"; then
    die "failed to classify $COMPOSE_FILE changes; refusing unattended rolling update"
  fi

  while IFS= read -r part; do
    [[ -n "$part" ]] || continue
    if [[ "$part" == top-level ]]; then
      die "$COMPOSE_FILE changed outside individual service blocks; review as an infrastructure change instead of rolling it unattended"
    fi
    [[ "$part" == service:* ]] || die "unexpected compose classifier output: $part"
    service="${part#service:}"
    case "$service" in
      web|celery_worker|celery_beat|migrations)
        APP_CONFIG_CHANGED=1
        ;;
      deposit_service)
        DEPOSIT_CONFIG_CHANGED=1
        ;;
      sweeper_service)
        SWEEPER_CONFIG_CHANGED=1
        ;;
      dfx_signer_service)
        SIGNER_CONFIG_CHANGED=1
        ;;
      staging_ingress)
        [[ "$ENVIRONMENT_NAME" == "staging" ]] || \
          die "$COMPOSE_FILE unexpectedly defines staging_ingress outside staging"
        STAGING_INGRESS_CHANGED=1
        ;;
      db|redis)
        die "$COMPOSE_FILE changes live $service; PostgreSQL/Redis are intentionally outside rolling application updates"
        ;;
      cloudflared)
        die "$COMPOSE_FILE changes cloudflared; tunnel infrastructure is intentionally outside rolling application updates"
        ;;
      *)
        die "$COMPOSE_FILE changes unsupported service '$service'; refusing to mark it deployed"
        ;;
    esac
  done <<<"$compose_delta"
}

classify_release() {
  local first_run=0
  [[ -z "$BASE_SHA" ]] && first_run=1

  if (( first_run )) || changed_matches '^frontend/'; then FRONTEND_CHANGED=1; fi
  if (( first_run )) || changed_matches '^static/'; then STATIC_CHANGED=1; fi
  if (( first_run )) || main_image_inputs_changed; then MAIN_IMAGE_CHANGED=1; fi
  if [[ "$ENVIRONMENT_NAME" == "staging" ]]; then
    if (( first_run )) || changed_matches '^deploy/docker/staging_ingress\.conf$'; then
      STAGING_INGRESS_CHANGED=1
    fi
  fi
  if (( first_run )) || changed_matches '^deposit_service/(Dockerfile|requirements\.txt|app/)'; then DEPOSIT_IMAGE_CHANGED=1; fi
  if changed_matches '^deposit_service/config/'; then DEPOSIT_CONFIG_CHANGED=1; fi
  if (( first_run )) || changed_matches '^sweeper_service/(Dockerfile|requirements\.txt|app/)'; then
    SWEEPER_IMAGE_CHANGED=1
    SIGNER_IMAGE_CHANGED=1
  fi
  if changed_matches '^sweeper_service/config/'; then SWEEPER_CONFIG_CHANGED=1; fi
  if (( first_run )); then
    APP_CONFIG_CHANGED=1
    DEPOSIT_CONFIG_CHANGED=1
    SWEEPER_CONFIG_CHANGED=1
    SIGNER_CONFIG_CHANGED=1
  fi
  if [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
    DEPOSIT_CONFIG_CHANGED=1
    SWEEPER_CONFIG_CHANGED=1
    SIGNER_CONFIG_CHANGED=1
  fi

  # runpod_worker is deliberately outside this Docker stack. It neither blocks
  # nor participates in the rolling deployment state.
  classify_compose_delta
}

app_update_needed() {
  (( MAIN_IMAGE_CHANGED || APP_CONFIG_CHANGED || FRONTEND_CHANGED || STATIC_CHANGED ))
}

# Every application release creates a fresh static snapshot. The frontend
# bundle is not a Django static source, so every application release must
# rebuild/copy it even when only backend code or app config changed.
frontend_build_needed() { app_update_needed; }

deposit_update_needed() { (( DEPOSIT_IMAGE_CHANGED || DEPOSIT_CONFIG_CHANGED )); }
sweeper_update_needed() { (( SWEEPER_IMAGE_CHANGED || SWEEPER_CONFIG_CHANGED )); }
signer_update_needed() { (( SIGNER_IMAGE_CHANGED || SIGNER_CONFIG_CHANGED )); }
staging_ingress_update_needed() { (( STAGING_INGRESS_CHANGED )); }
crypto_update_needed() { deposit_update_needed || sweeper_update_needed || signer_update_needed; }
stack_update_needed() { app_update_needed || crypto_update_needed || staging_ingress_update_needed; }

print_plan() {
  local app_release=0
  app_update_needed && app_release=1
  echo "rolling-update[$ENVIRONMENT_NAME]: release ${BASE_SHA:-<unknown>} -> $CURRENT_SHA"
  echo "  changed tracked files: ${#CHANGED_FILES[@]}"
  echo "  frontend source change: $FRONTEND_CHANGED"
  echo "  frontend build:         $app_release"
  echo "  static source change:   $STATIC_CHANGED"
  echo "  static snapshot:        $app_release"
  echo "  main image rebuild:    $MAIN_IMAGE_CHANGED"
  echo "  app config recreate:   $APP_CONFIG_CHANGED"
  echo "  deposit image/config:  $DEPOSIT_IMAGE_CHANGED/$DEPOSIT_CONFIG_CHANGED"
  echo "  sweeper image/config:  $SWEEPER_IMAGE_CHANGED/$SWEEPER_CONFIG_CHANGED"
  echo "  signer image/config:   $SIGNER_IMAGE_CHANGED/$SIGNER_CONFIG_CHANGED"
  echo "  staging ingress:       $STAGING_INGRESS_CHANGED"
  echo "  legacy bootstrap:      $LEGACY_BOOTSTRAP"
}

build_required_images() {
  if (( MAIN_IMAGE_CHANGED )); then compose build web; fi
  if (( DEPOSIT_IMAGE_CHANGED )); then compose_crypto build deposit_service; fi
  if (( SWEEPER_IMAGE_CHANGED || SIGNER_IMAGE_CHANGED )); then
    compose_crypto build dfx_signer_service sweeper_service
  fi
  if staging_ingress_update_needed; then
    # Pull before touching live application processes.
    compose pull staging_ingress
  fi
}

staging_ingress_configured() {
  [[ "$ENVIRONMENT_NAME" == "staging" ]] && service_exists staging_ingress
}

prepare_staging_ingress() {
  staging_ingress_configured || return 0
  if service_is_running staging_ingress && ! staging_ingress_update_needed; then return 0; fi

  echo "rolling-update[$ENVIRONMENT_NAME]: validating stable staging ingress config without binding :80"
  # docker compose run does not publish service ports unless explicitly requested.
  compose run --rm --no-deps --entrypoint nginx staging_ingress \
    -t -c /etc/nginx/nginx.conf
  STAGING_INGRESS_VALIDATED=1
}

ensure_staging_ingress() {
  staging_ingress_configured || return 0

  if service_is_running staging_ingress; then
    if staging_ingress_update_needed; then
      (( STAGING_INGRESS_VALIDATED )) || die "staging ingress config was not validated before recreate"
      echo "rolling-update[$ENVIRONMENT_NAME]: applying staging ingress config"
      compose up -d --no-deps --force-recreate staging_ingress
    fi
  else
    (( STAGING_INGRESS_VALIDATED )) || prepare_staging_ingress
    echo "rolling-update[$ENVIRONMENT_NAME]: starting stable staging ingress on :80"
    compose up -d --no-deps staging_ingress
  fi

  wait_healthy staging_ingress 90 1 || die "staging ingress is not serving the web application"
}

require_staging_ingress_healthy() {
  [[ "$ENVIRONMENT_NAME" == "staging" ]] || return 0
  service_exists staging_ingress || die "staging compose is missing the stable staging_ingress service"
  service_is_running staging_ingress || die "staging ingress is not running"
  wait_healthy staging_ingress 90 1 || die "staging ingress is not serving the web application"
}

check_pending_migrations() {
  local rc=0
  set +e
  compose run --rm --no-deps migrations python deploy/scripts/check_rolling_migrations.py
  rc=$?
  set -e
  if (( rc == 0 )); then return 0; fi
  if (( rc == 3 )) && [[ "${ALLOW_REVIEWED_ROLLING_MIGRATIONS:-0}" == 1 ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: proceeding with explicitly reviewed migration override" >&2
    return 0
  fi
  return "$rc"
}

build_frontend_dist() {
  frontend_build_needed || return 0
  echo "rolling-update[$ENVIRONMENT_NAME]: building reproducible frontend image and dist"
  [[ -f frontend/package-lock.json ]] || die "frontend/package-lock.json is required for reproducible frontend builds"
  "${FRONTEND_COMPOSE[@]}" build frontend
  "${FRONTEND_COMPOSE[@]}" run --rm --no-deps frontend npm run dist
  [[ -d frontend/dist/static ]] || die "frontend build completed without frontend/dist/static"
  [[ ! -d frontend/dist/static/static ]] || die "frontend build produced invalid nested frontend/dist/static/static"
  FRONTEND_BUILT=1
}

prepare_static_release() {
  local prepared tmp
  prepared="$(progress_get static_prepared 0)"
  if [[ "$prepared" == 1 ]]; then
    [[ -d "$STATIC_RELEASE_DIR" ]] || die "recorded static snapshot for $CURRENT_SHA is missing; refusing unsafe reconstruction after a partial deployment"
    return 0
  fi

  # STATIC_RELEASE_DIR is generated output, never a copy of the mutable
  # checkout. Django collectstatic fills this clean directory from static/
  # and installed-app finders during the migrations phase.
  tmp="${STATIC_RELEASE_DIR}.tmp.$$"
  rm -rf "$tmp"
  mkdir -p "$tmp"
  mkdir -p "$(dirname "$STATIC_RELEASE_DIR")"
  rm -rf "$STATIC_RELEASE_DIR"
  mv "$tmp" "$STATIC_RELEASE_DIR"
  progress_set static_prepared 1
}

finalize_static_release() {
  local finalized
  finalized="$(progress_get static_finalized 0)"
  [[ -d "$STATIC_RELEASE_DIR" ]] || die "release static snapshot is missing"
  if [[ "$finalized" == 1 ]]; then return 0; fi
  if (( FRONTEND_BUILT )); then
    cp -a frontend/dist/static/. "$STATIC_RELEASE_DIR/"
  fi
  progress_set static_finalized 1
}

run_migrations_once() {
  if [[ "$(progress_get migrations_done 0)" == 1 ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: migrations already completed for $CURRENT_SHA"
    return 0
  fi
  run_migrations
  progress_set migrations_done 1
}
celery_queue_count() {
  local web_id
  web_id="$(find_healthy_web_id || true)"
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
  web_id="$(find_healthy_web_id || true)"
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

drain_celery_legacy() {
  # Legacy containers mount the just-updated checkout. Do not launch a fresh
  # Django/Celery inspection process inside them before migrations. Stop Beat,
  # then rely on Celery's warm TERM shutdown to finish active work; queued work
  # remains durable in Redis for the new worker.
  if service_exists celery_beat; then stop_service_if_running celery_beat || return 1; fi
  if [[ -n "$(service_container_ids celery_worker | head -n1)" ]]; then
    stop_service_if_running celery_worker || return 1
  fi
  CELERY_DRAINED=1
  progress_set celery_drained 1
}

drain_celery_fresh() {
  if service_exists celery_beat; then stop_service_if_running celery_beat || return 1; fi
  local deadline=$((SECONDS + DRAIN_TIMEOUT_SECONDS)) count
  while (( SECONDS < deadline )); do
    if ! count="$(celery_work_count)"; then
      echo "rolling-update[$ENVIRONMENT_NAME]: cannot reliably inspect Celery; refusing update" >&2
      return 1
    fi
    if (( count == 0 )); then
      stop_service_if_running celery_worker || return 1
      CELERY_DRAINED=1
      progress_set celery_drained 1
      return 0
    fi
    echo "rolling-update[$ENVIRONMENT_NAME]: waiting for Celery drain: $count active/reserved/queued"
    sleep 5
  done
  echo "rolling-update[$ENVIRONMENT_NAME]: Celery drain timed out" >&2
  return 1
}

ensure_celery_drained() {
  local remembered worker_id
  remembered="$(progress_get celery_drained 0)"
  worker_id="$(service_container_ids celery_worker | head -n1)"
  if [[ "$remembered" == 1 && -z "$worker_id" ]]; then
    if service_exists celery_beat; then stop_service_if_running celery_beat || return 1; fi
    CELERY_DRAINED=1
    echo "rolling-update[$ENVIRONMENT_NAME]: resuming with Celery already drained from a previous attempt"
    return 0
  fi
  if (( LEGACY_BOOTSTRAP )); then
    drain_celery_legacy
  else
    drain_celery_fresh
  fi
}

run_migrations() {
  # DB/Redis are deliberately outside application rolling updates.
  # They are already healthy/running; never let `compose run` converge them.
  compose run --rm --no-deps migrations
}

capture_crypto_initial_state() {
  if [[ -n "$DEPOSIT_WAS_RUNNING" ]]; then return 0; fi

  local saved
  saved="$(progress_get deposit_was_running '')"
  if [[ -n "$saved" ]]; then
    DEPOSIT_WAS_RUNNING="$saved"
    SWEEPER_WAS_RUNNING="$(progress_get sweeper_was_running 0)"
    SIGNER_WAS_RUNNING="$(progress_get signer_was_running 0)"
    echo "rolling-update[$ENVIRONMENT_NAME]: restoring original crypto service state from unfinished update"
    return 0
  fi

  DEPOSIT_WAS_RUNNING=0
  SWEEPER_WAS_RUNNING=0
  SIGNER_WAS_RUNNING=0
  [[ -n "$(service_container_ids deposit_service)" ]] && DEPOSIT_WAS_RUNNING=1
  [[ -n "$(service_container_ids sweeper_service)" ]] && SWEEPER_WAS_RUNNING=1
  [[ -n "$(service_container_ids dfx_signer_service)" ]] && SIGNER_WAS_RUNNING=1
  progress_set deposit_was_running "$DEPOSIT_WAS_RUNNING"
  progress_set sweeper_was_running "$SWEEPER_WAS_RUNNING"
  progress_set signer_was_running "$SIGNER_WAS_RUNNING"
}

stop_crypto_for_update() {
  crypto_update_needed || return 0
  capture_crypto_initial_state

  if signer_update_needed; then
    stop_crypto_service_if_running deposit_service || return 1
    stop_crypto_service_if_running sweeper_service || return 1
    return 0
  fi
  if deposit_update_needed; then stop_crypto_service_if_running deposit_service || return 1; fi
  if sweeper_update_needed; then stop_crypto_service_if_running sweeper_service || return 1; fi
}

remove_nonrunning_service_containers() {
  local service="$1" cid state
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    state="$(container_state "$cid")"
    if [[ "$state" != running ]]; then docker rm -f "$cid" >/dev/null 2>&1 || true; fi
  done < <(service_container_ids_all "$service")
}

converge_unbound_service_release() {
  local service="$1" timeout="$2" stop_time="$3"
  local current total target cid release health count

  remove_nonrunning_service_containers "$service"
  current="$(current_release_healthy_count "$service")"
  while (( current < 1 )); do
    total="$(service_container_ids "$service" | grep -c . || true)"
    target=$((total + 1))
    compose up -d --no-deps --no-recreate --scale "$service=$target" "$service"
    wait_current_release_healthy_count "$service" 1 "$timeout"
    current="$(current_release_healthy_count "$service")"
  done

  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    release="$(container_release "$cid")"
    health="$(container_health "$cid")"
    if [[ "$release" != "$CURRENT_SHA" || "$health" != healthy ]]; then
      docker stop --time "$stop_time" "$cid" >/dev/null 2>&1 || true
      docker rm -f "$cid" >/dev/null 2>&1 || true
    fi
  done < <(service_container_ids "$service")

  mapfile -t current_ids < <(current_release_healthy_ids "$service")
  if (( ${#current_ids[@]} > 1 )); then
    for cid in "${current_ids[@]:1}"; do
      docker stop --time "$stop_time" "$cid" >/dev/null 2>&1 || true
      docker rm -f "$cid" >/dev/null 2>&1 || true
    done
  fi
  wait_healthy "$service" "$timeout" 1
  [[ "$(container_release "$(service_container_ids "$service" | head -n1)")" == "$CURRENT_SHA" ]] || \
    die "$service converged without the expected release label"
}

update_signer_if_needed() {
  signer_update_needed || return 0
  capture_crypto_initial_state
  if (( SIGNER_WAS_RUNNING || DEPOSIT_WAS_RUNNING || SWEEPER_WAS_RUNNING )) || [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
    converge_unbound_service_release dfx_signer_service 180 30
  fi
}

converge_web_scaled() {
  remove_nonrunning_service_containers web

  local current total target cid release health temporary healthy_count retired
  temporary=$((EXPECTED_WEB_REPLICAS + 1))
  current="$(current_release_healthy_count web)"

  while (( current < EXPECTED_WEB_REPLICAS )); do
    # Failed/unhealthy replicas do not protect availability and only consume a
    # scale slot, so remove them before deciding whether another replica fits.
    while IFS= read -r cid; do
      [[ -n "$cid" ]] || continue
      if ! container_is_healthy "$cid"; then
        docker stop --time 15 "$cid" >/dev/null 2>&1 || true
        docker rm -f "$cid" >/dev/null 2>&1 || true
      fi
    done < <(service_container_ids web)

    total="$(service_container_ids web | grep -c . || true)"
    (( total >= 1 )) || die "refusing to roll web with no running web container"

    # Never exceed steady-state+1. When resuming from 2 old + 1 new, retire one
    # old healthy replica first, then create the next current-release replica.
    if (( total >= temporary )); then
      healthy_count="$(healthy_service_count web)"
      (( healthy_count > EXPECTED_WEB_REPLICAS )) || \
        die "cannot free a web scale slot without dropping below healthy steady-state capacity"
      retired=0
      while IFS= read -r cid; do
        [[ -n "$cid" ]] || continue
        if [[ "$(container_release "$cid")" != "$CURRENT_SHA" ]] && container_is_healthy "$cid"; then
          docker stop --time 90 "$cid" >/dev/null
          docker rm -f "$cid" >/dev/null
          retired=1
          break
        fi
      done < <(service_container_ids web)
      (( retired )) || die "web is at temporary capacity but has no old replica that can be safely retired"
      total="$(service_container_ids web | grep -c . || true)"
    fi

    target=$((total + 1))
    (( target <= temporary )) || die "internal web convergence error: target $target exceeds $temporary"
    compose up -d --no-deps --no-recreate --scale "web=$target" web
    wait_current_release_healthy_count web $((current + 1)) 300
    current="$(current_release_healthy_count web)"
  done

  # Full target-release capacity exists. Old, unknown and unhealthy replicas can
  # now be retired without affecting availability.
  while IFS= read -r cid; do
    [[ -n "$cid" ]] || continue
    release="$(container_release "$cid")"
    health="$(container_health "$cid")"
    if [[ "$release" != "$CURRENT_SHA" || "$health" != healthy ]]; then
      docker stop --time 90 "$cid" >/dev/null 2>&1 || true
      docker rm -f "$cid" >/dev/null 2>&1 || true
    fi
  done < <(service_container_ids web)

  mapfile -t current_ids < <(current_release_healthy_ids web)
  if (( ${#current_ids[@]} > EXPECTED_WEB_REPLICAS )); then
    for cid in "${current_ids[@]:EXPECTED_WEB_REPLICAS}"; do
      docker stop --time 90 "$cid" >/dev/null 2>&1 || true
      docker rm -f "$cid" >/dev/null 2>&1 || true
    done
  fi

  wait_healthy web 300 "$EXPECTED_WEB_REPLICAS"
  while IFS= read -r cid; do
    [[ "$(container_release "$cid")" == "$CURRENT_SHA" ]] || die "web convergence left a non-current release replica"
  done < <(service_container_ids web)
}
update_web_single() {
  compose up -d --no-deps --force-recreate web
  wait_healthy web 300 1
  local cid
  cid="$(service_container_ids web | head -n1)"
  [[ "$(container_release "$cid")" == "$CURRENT_SHA" ]] || die "staging web did not start with current release label"
}

update_web() {
  case "$WEB_UPDATE_MODE" in
    scaled) converge_web_scaled ;;
    single) update_web_single ;;
    *) die "unknown WEB_UPDATE_MODE=$WEB_UPDATE_MODE" ;;
  esac
}

assert_current_release_service() {
  local service="$1" expected="${2:-1}" ids cid
  mapfile -t ids < <(service_container_ids "$service")
  (( ${#ids[@]} == expected )) || die "$service has ${#ids[@]} running replica(s), expected $expected"
  for cid in "${ids[@]}"; do
    container_is_healthy "$cid" || die "$service has an unhealthy replica"
    [[ "$(container_release "$cid")" == "$CURRENT_SHA" ]] || die "$service is not running release $CURRENT_SHA"
  done
}

restart_celery() {
  compose up -d --no-deps --force-recreate celery_worker celery_beat
  wait_healthy celery_worker 120 1
  wait_healthy celery_beat 120 1
  assert_current_release_service celery_worker 1
  assert_current_release_service celery_beat 1
  CELERY_DRAINED=0
  progress_set celery_drained 0
}

restart_crypto_after_update() {
  crypto_update_needed || return 0
  capture_crypto_initial_state

  if (( DEPOSIT_WAS_RUNNING )) || [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
    # A signer rotation deliberately quiesces the financial loops. Recreate
    # those loops instead of merely starting their old containers so they get
    # the current release identity and a fresh signer dependency context.
    if deposit_update_needed || signer_update_needed; then
      compose_crypto up -d --no-deps --force-recreate deposit_service
    elif [[ -z "$(service_container_ids deposit_service)" ]]; then
      compose_crypto start deposit_service >/dev/null || compose_crypto up -d --no-deps deposit_service
    fi
    wait_healthy deposit_service 120 1
    if deposit_update_needed || signer_update_needed; then
      assert_current_release_service deposit_service 1
    fi
  fi

  if (( SWEEPER_WAS_RUNNING )) || [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
    if sweeper_update_needed || signer_update_needed; then
      compose_crypto up -d --no-deps --force-recreate sweeper_service
    elif [[ -z "$(service_container_ids sweeper_service)" ]]; then
      compose_crypto start sweeper_service >/dev/null || compose_crypto up -d --no-deps sweeper_service
    fi
    wait_healthy sweeper_service 120 1
    if sweeper_update_needed || signer_update_needed; then
      assert_current_release_service sweeper_service 1
    fi
  fi
}

static_release_sha_valid() {
  [[ "$1" =~ ^[0-9a-f]{40}$ ]]
}

write_static_release_state() {
  local sha="$1" tmp
  static_release_sha_valid "$sha" || die "refusing to record invalid static release SHA '$sha'"
  mkdir -p "$ROLLING_STATE_DIR"
  tmp="${STATIC_STATE_FILE}.tmp.$$"
  printf '%s\n' "$sha" > "$tmp"
  mv "$tmp" "$STATIC_STATE_FILE"
}

record_static_release() {
  write_static_release_state "$CURRENT_SHA"
}

static_snapshot_sha_from_container() {
  local cid="$1" source source_abs root_abs sha
  source="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/home/mediacms.io/mediacms/static_collected"}}{{.Source}}{{end}}{{end}}' "$cid" 2>/dev/null || true)"
  [[ -n "$source" && -d "$source" && -d "$ROLLING_STATE_DIR/static" ]] || return 1
  source_abs="$(cd "$source" 2>/dev/null && pwd -P)" || return 1
  root_abs="$(cd "$ROLLING_STATE_DIR/static" 2>/dev/null && pwd -P)" || return 1
  [[ "${source_abs%/*}" == "$root_abs" ]] || return 1
  sha="${source_abs##*/}"
  static_release_sha_valid "$sha" || return 1
  printf '%s\n' "$sha"
}

bootstrap_static_release_state_from_web() {
  local cid sha observed=""
  local -a ids=()
  [[ -f "$STATIC_STATE_FILE" ]] && return 0

  mapfile -t ids < <(service_container_ids_all web)
  (( ${#ids[@]} > 0 )) || return 0

  for cid in "${ids[@]}"; do
    if ! sha="$(static_snapshot_sha_from_container "$cid")"; then
      echo "rolling-update[$ENVIRONMENT_NAME]: WARNING cannot infer the active static snapshot from web container $cid; static GC will stay disabled until a successful application release records it" >&2
      return 0
    fi
    if [[ -n "$observed" && "$observed" != "$sha" ]]; then
      echo "rolling-update[$ENVIRONMENT_NAME]: WARNING web replicas mount different static snapshots; static GC will stay disabled until convergence" >&2
      return 0
    fi
    observed="$sha"
  done

  [[ -n "$observed" ]] || return 0
  write_static_release_state "$observed"
  echo "rolling-update[$ENVIRONMENT_NAME]: initialized active static snapshot state at $observed"
}

static_release_state_coverage_complete() {
  local release static_state sha
  for release in "$ROLLING_STATE_DIR"/*.release; do
    [[ -f "$release" ]] || continue
    static_state="${release%.release}.static-release"
    if [[ ! -f "$static_state" ]]; then
      echo "rolling-update[$ENVIRONMENT_NAME]: WARNING ${static_state##*/} is missing; skipping static GC so an active snapshot from another environment cannot be pruned" >&2
      return 1
    fi
    sha="$(tr -d '[:space:]' < "$static_state" 2>/dev/null || true)"
    if ! static_release_sha_valid "$sha"; then
      echo "rolling-update[$ENVIRONMENT_NAME]: WARNING ${static_state##*/} does not contain a valid SHA; skipping static GC" >&2
      return 1
    fi
  done
  return 0
}

cleanup_static_releases() {
  local root="$ROLLING_STATE_DIR/static" state sha target dir name kept=0
  [[ -d "$root" ]] || return 0

  if ! [[ "$STATIC_RELEASE_KEEP_COUNT" =~ ^[0-9]+$ ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: WARNING invalid STATIC_RELEASE_KEEP_COUNT=$STATIC_RELEASE_KEEP_COUNT; skipping static GC" >&2
    return 0
  fi
  if ! [[ "$STATIC_TMP_MAX_AGE_MINUTES" =~ ^[0-9]+$ ]]; then
    echo "rolling-update[$ENVIRONMENT_NAME]: WARNING invalid STATIC_TMP_MAX_AGE_MINUTES=$STATIC_TMP_MAX_AGE_MINUTES; skipping static GC" >&2
    return 0
  fi
  if ! static_release_state_coverage_complete; then
    return 0
  fi

  # The state directory is shared by staging/production on a checkout. Never
  # prune another environment's current release, an interrupted target, or the
  # release adopted by the one-time Redis persistence migration.
  declare -A protected=()
  if static_release_sha_valid "$CURRENT_SHA"; then protected["$CURRENT_SHA"]=1; fi

  for state in "$ROLLING_STATE_DIR"/*.release "$ROLLING_STATE_DIR"/*.static-release "$ROLLING_STATE_DIR"/*.complete "$ROLLING_STATE_DIR"/*.inprogress; do
    [[ -f "$state" ]] || continue
    target="$(grep -E '^target_sha=' "$state" 2>/dev/null | tail -n1 | cut -d= -f2- || true)"
    if static_release_sha_valid "$target"; then
      protected["$target"]=1
      continue
    fi
    sha="$(tr -d '[:space:]' < "$state" 2>/dev/null || true)"
    if static_release_sha_valid "$sha"; then protected["$sha"]=1; fi
  done

  mapfile -t snapshot_dirs < <(
    find "$root" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null \
      | sort -nr \
      | cut -d' ' -f2-
  )

  for dir in "${snapshot_dirs[@]}"; do
    name="${dir##*/}"
    static_release_sha_valid "$name" || continue
    [[ -n "${protected[$name]:-}" ]] && continue
    if (( kept < STATIC_RELEASE_KEEP_COUNT )); then
      kept=$((kept + 1))
      continue
    fi
    if rm -rf -- "$dir"; then
      echo "rolling-update[$ENVIRONMENT_NAME]: pruned old static snapshot $name"
    else
      echo "rolling-update[$ENVIRONMENT_NAME]: WARNING failed to prune static snapshot $name" >&2
    fi
  done

  # A crash can leave <sha>.tmp.<pid> from prepare_static_release(). Remove only
  # old temp dirs whose target SHA is not protected by any live/in-progress state.
  for dir in "$root"/*.tmp.*; do
    [[ -d "$dir" ]] || continue
    name="${dir##*/}"
    sha="${name%%.tmp.*}"
    static_release_sha_valid "$sha" || continue
    [[ -n "${protected[$sha]:-}" ]] && continue
    if find "$dir" -maxdepth 0 -mmin "+$STATIC_TMP_MAX_AGE_MINUTES" -print -quit 2>/dev/null | grep -q .; then
      if rm -rf -- "$dir"; then
        echo "rolling-update[$ENVIRONMENT_NAME]: pruned stale static temp ${name}"
      else
        echo "rolling-update[$ENVIRONMENT_NAME]: WARNING failed to prune static temp ${name}" >&2
      fi
    fi
  done

  # Garbage collection is post-success housekeeping. A cleanup failure must not
  # turn an already-recorded healthy release into an ambiguous failed deploy.
  return 0
}

record_release() {
  mkdir -p "$ROLLING_STATE_DIR"
  local tmp="${STATE_FILE}.tmp.$$"
  printf '%s\n' "$CURRENT_SHA" > "$tmp"
  mv "$tmp" "$STATE_FILE"
  rm -f "$INPROGRESS_FILE"
}

rolling_failure_notice() {
  local rc=$?
  if (( rc != 0 )); then
    echo "rolling-update[$ENVIRONMENT_NAME]: FAILED; release state was not advanced." >&2
    if (( CELERY_DRAINED )) || [[ "$(progress_get celery_drained 0)" == 1 ]]; then
      echo "rolling-update[$ENVIRONMENT_NAME]: Celery remains intentionally stopped. Rerun the same updater; it will resume without requiring empty queues." >&2
    fi
  fi
  return "$rc"
}

rolling_update_main() {
  trap rolling_failure_notice EXIT
  require_environment
  acquire_update_lock
  detect_legacy_bootstrap
  if (( LEGACY_BOOTSTRAP )); then legacy_preflight; else app_preflight; fi
  verify_runtime_dependencies
  # Existing deployments predate the dedicated active-static marker. Infer it
  # once from the steady-state web mounts before any GC can run.
  bootstrap_static_release_state_from_web

  if (( REQUIRE_PERSISTENT_REDIS )) && ! redis_is_persistent; then
    cat >&2 <<EOF_REDIS
rolling-update[$ENVIRONMENT_NAME]: Redis is not mounted on '$REDIS_VOLUME'.
Run the one-time persistence migration before the production rolling updater:
  CONFIRM_REDIS_MIGRATION=$PROJECT deploy/scripts/prod_migrate_redis_persistence.sh
EOF_REDIS
    exit 2
  fi

  load_release_delta
  if [[ -n "$BASE_SHA" && "$BASE_SHA" == "$CURRENT_SHA" && "${CRYPTO_WORKERS:-0}" != 1 ]]; then
    rm -f "$INPROGRESS_FILE"
    if [[ "$ENVIRONMENT_NAME" == "staging" ]]; then
      prepare_staging_ingress
      ensure_staging_ingress
      require_staging_ingress_healthy
    fi
    cleanup_static_releases
    echo "rolling-update[$ENVIRONMENT_NAME]: $CURRENT_SHA is already recorded as deployed; health/preflight OK"
    trap - EXIT
    return 0
  fi

  bind_progress_to_target
  classify_release
  print_plan

  if ! stack_update_needed; then
    if [[ "$ENVIRONMENT_NAME" == "staging" ]]; then
      prepare_staging_ingress
      ensure_staging_ingress
      require_staging_ingress_healthy
    fi
    record_release
    cleanup_static_releases
    trap - EXIT
    echo "rolling-update[$ENVIRONMENT_NAME]: no Docker-stack component changed; release marker advanced only"
    return 0
  fi

  # Legacy containers still see the live checkout. Quiesce Celery first on this
  # one-time transition; normal isolated releases keep all build/review work
  # before the first live-process change.
  if (( LEGACY_BOOTSTRAP )) && app_update_needed; then ensure_celery_drained; fi

  build_frontend_dist
  build_required_images
  # On the first staging topology migration this validates the pinned ingress
  # config in a one-shot container without publishing :80. The old web can keep
  # serving until the new unbound web replica has passed its healthcheck.
  prepare_staging_ingress
  if app_update_needed; then
    prepare_static_release
    if [[ "$(progress_get migrations_done 0)" != 1 ]]; then
      check_pending_migrations
      if (( ! LEGACY_BOOTSTRAP )); then ensure_celery_drained; fi
      run_migrations_once
    fi
    finalize_static_release
  fi

  if crypto_update_needed; then
    stop_crypto_for_update
    update_signer_if_needed
  fi

  if app_update_needed; then
    update_web
    restart_celery
  fi

  # Staging ingress owns host :80 only after web convergence. On subsequent
  # releases it stays up while web performs the true 1 -> 2 -> health -> 1 roll.
  ensure_staging_ingress

  if crypto_update_needed; then restart_crypto_after_update; fi

  app_preflight
  verify_runtime_dependencies
  require_staging_ingress_healthy
  if app_update_needed; then
    # Only an application rollout changes the snapshot mounted by web. Crypto-,
    # ingress-, docs-, and other marker-only releases must leave this untouched.
    record_static_release
  fi
  record_release
  cleanup_static_releases

  trap - EXIT
  echo "rolling-update[$ENVIRONMENT_NAME]: complete at $CURRENT_SHA; PostgreSQL/Redis and DNS routing were untouched"
}
