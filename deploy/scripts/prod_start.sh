#!/usr/bin/env bash
set -euo pipefail

# Safe production cold start for MediaCMS.
#
# This is intentionally separate from prod_rolling_update.sh:
# rolling updates require an already-running healthy web service, while this
# command reconstructs a completely stopped production stack.
#
# It can be run outside the repository:
#   MEDIACMS_ROOT=/home/user/mediacms ./prod_start.sh
#
# Required:
#   CONFIRM_COLD_START=mediacms-prod
#
# Additionally required only when the durable Redis volume does not exist:
#   CONFIRM_EMPTY_REDIS=mediacms-prod
#
# The latter means the previous Redis state is known to be unavailable and a
# new empty durable Redis is intentionally being established.

MEDIACMS_ROOT="${MEDIACMS_ROOT:-$PWD}"
COMMON="$MEDIACMS_ROOT/deploy/scripts/rolling_update_common.sh"

[[ -f "$COMMON" ]] || {
  echo "prod-start: cannot find $COMMON" >&2
  exit 2
}

# rolling_update_common.sh sets/cds to the canonical repository root.
# shellcheck source=/dev/null
source "$COMMON"

configure_rolling_update \
  "${COMPOSE_PROJECT_NAME:-mediacms-prod}" \
  "${COMPOSE_FILE:-docker-compose-cloudflare.yaml}" \
  "${REDIS_VOLUME_NAME:-mediacms-prod-redis-data}" \
  "production" \
  2 \
  "scaled" \
  1 \
  "production"

[[ "$PROJECT" == "mediacms-prod" ]] || die "cold start requires COMPOSE_PROJECT_NAME=mediacms-prod"
[[ "$COMPOSE_FILE" == "docker-compose-cloudflare.yaml" ]] || die "cold start requires docker-compose-cloudflare.yaml"
[[ "${CONFIRM_COLD_START:-}" == "$PROJECT" ]] || \
  die "set CONFIRM_COLD_START=$PROJECT to execute a production cold start"

require_environment
acquire_update_lock

REDIS_COMPLETE_FILE="$ROLLING_STATE_DIR/production.redis-migration.complete"
POSTGRES_DATA_DIR="${PROD_POSTGRES_DATA_DIR:-$ROLLING_ROOT/../postgres_data}"

[[ -f "$POSTGRES_DATA_DIR/PG_VERSION" ]] || \
  die "refusing cold start: expected existing production PostgreSQL data at $POSTGRES_DATA_DIR/PG_VERSION"

cold_start_pre_mutation_resume_safe() {
  local saved="$1" key service

  [[ "$(progress_get cold_start 0)" == 1 ]] || return 1

  # Once any release mutation has started, the target SHA is immutable. These
  # markers are written only after the corresponding mutating phase.
  for key in static_prepared static_finalized migrations_done celery_drained; do
    [[ "$(progress_get "$key" 0)" == 0 ]] || return 1
  done

  # Defend against a crash in the tiny window after the static directory rename
  # but before static_prepared was persisted.
  [[ ! -d "$ROLLING_STATE_DIR/static/$saved" ]] || return 1

  # PostgreSQL and Redis may already have been started by the cold-start
  # preflight. No application/runtime service may have been started yet.
  for service in web celery_beat celery_worker dfx_signer_service deposit_service sweeper_service cloudflared; do
    service_is_running "$service" && return 1
  done

  return 0
}

retarget_pre_mutation_cold_start_if_needed() {
  local saved
  [[ -f "$INPROGRESS_FILE" ]] || return 0

  saved="$(progress_get target_sha '')"
  [[ -n "$saved" ]] || \
    die "unfinished cold-start state has no target_sha; refusing ambiguous resume"
  [[ "$saved" != "$CURRENT_SHA" ]] || return 0

  if ! cold_start_pre_mutation_resume_safe "$saved"; then
    die "unfinished cold start targets $saved and has crossed the safe retarget boundary; restore that checkout or recover manually"
  fi

  echo "prod-start: safely retargeting pre-mutation cold start $saved -> $CURRENT_SHA"
  progress_set previous_target_sha "$saved"
  progress_set target_sha "$CURRENT_SHA"
}

# Do not hijack an interrupted rolling deployment. A failed cold start, however,
# is resumable. It may be retargeted to a bug-fix commit only while still before
# the first application mutation.
if [[ -f "$INPROGRESS_FILE" ]] && [[ "$(progress_get cold_start 0)" != 1 ]]; then
  die "production.inprogress belongs to another deployment mode; resolve/resume that deployment before cold start"
fi

# If a healthy/live web exists without a cold-start state, this is not a cold
# start. Use the rolling updater instead.
if service_is_running web && [[ "$(progress_get cold_start 0)" != 1 ]]; then
  die "production web is already running; use deploy/scripts/prod_rolling_update.sh"
fi

retarget_pre_mutation_cold_start_if_needed
bind_progress_to_target
progress_set cold_start 1

cold_start_failure_notice() {
  local rc=$?
  if (( rc != 0 )); then
    echo "prod-start: FAILED for release $CURRENT_SHA." >&2
    echo "prod-start: state is preserved in $INPROGRESS_FILE; fix the cause and rerun the same command." >&2
  fi
  return "$rc"
}
trap cold_start_failure_notice EXIT

redis_volume_exists() {
  docker volume inspect "$REDIS_VOLUME" >/dev/null 2>&1
}

validate_durable_redis() {
  wait_healthy redis 180 1
  redis_is_persistent || die "Redis is not mounted on expected durable volume $REDIS_VOLUME"

  local appendonly appendfsync aof_enabled
  appendonly="$(compose exec -T redis redis-cli --raw CONFIG GET appendonly | tail -n1 | tr -d '\r')"
  appendfsync="$(compose exec -T redis redis-cli --raw CONFIG GET appendfsync | tail -n1 | tr -d '\r')"
  aof_enabled="$(compose exec -T redis redis-cli --raw INFO persistence | tr -d '\r' | awk -F: '$1 == "aof_enabled" {print $2}')"

  [[ "$appendonly" == yes ]] || die "Redis appendonly is not enabled"
  [[ "$appendfsync" == everysec ]] || die "Redis appendfsync is not everysec"
  [[ "$aof_enabled" == 1 ]] || die "Redis AOF persistence is not active"
}

write_atomic_sha() {
  local destination="$1" tmp="${destination}.tmp.$$"
  mkdir -p "$(dirname "$destination")"
  printf '%s\n' "$CURRENT_SHA" > "$tmp"
  mv "$tmp" "$destination"
}

# ---------------------------------------------------------------------------
# Redis bootstrap decision
# ---------------------------------------------------------------------------
#
# Before this architecture, production Redis had no volume. If the historical
# container has already been deleted, its state cannot be copied by the normal
# one-time migration. Creating an empty Redis must therefore be an explicit,
# separately confirmed action.
if ! redis_volume_exists; then
  if [[ "$(progress_get redis_empty_reset_confirmed 0)" != 1 ]]; then
    [[ "${CONFIRM_EMPTY_REDIS:-}" == "$PROJECT" ]] || {
      cat >&2 <<EOF
prod-start: durable Redis volume '$REDIS_VOLUME' does not exist.

The cold-start script will NOT invent an empty Redis silently.
If the previous Redis container is already gone and its state cannot be
recovered, rerun with:

  CONFIRM_EMPTY_REDIS=$PROJECT

This acknowledges loss of the previous Redis-backed queue/session/cache state.
EOF
      exit 2
    }
    progress_set redis_empty_reset_confirmed 1
  fi

  echo "prod-start: creating new EMPTY durable Redis volume $REDIS_VOLUME"
  docker volume create "$REDIS_VOLUME" >/dev/null
elif [[ ! -f "$REDIS_COMPLETE_FILE" ]] && [[ "$(progress_get redis_empty_reset_confirmed 0)" != 1 ]]; then
  die "Redis volume $REDIS_VOLUME exists but has no migration/cold-start completion marker; refusing ambiguous adoption"
fi

# A cold start rebuilds a complete application release irrespective of the diff
# from the previous marker. This guarantees image/static/frontend consistency.
MAIN_IMAGE_CHANGED=1
APP_CONFIG_CHANGED=1
FRONTEND_CHANGED=1
STATIC_CHANGED=1
DEPOSIT_IMAGE_CHANGED=1
DEPOSIT_CONFIG_CHANGED=1
SWEEPER_IMAGE_CHANGED=1
SWEEPER_CONFIG_CHANGED=1
SIGNER_IMAGE_CHANGED=1
SIGNER_CONFIG_CHANGED=1

echo "prod-start: building complete release $CURRENT_SHA before starting application services"
build_frontend_dist
build_required_images

echo "prod-start: starting existing PostgreSQL"
compose up -d --no-deps db
wait_healthy db 300 1

echo "prod-start: starting durable Redis"
compose up -d --no-deps redis
validate_durable_redis

# Even during a cold start, refuse migrations that the rolling classifier marks
# as ambiguous/destructive unless the operator explicitly reviewed them.
check_pending_migrations

echo "prod-start: preparing release static snapshot"
prepare_static_release
run_migrations_once
finalize_static_release

echo "prod-start: starting signer"
compose up -d --no-deps --force-recreate dfx_signer_service
wait_healthy dfx_signer_service 180 1
assert_current_release_service dfx_signer_service 1

echo "prod-start: starting web replicas"
compose up -d --no-deps --force-recreate --scale web="$EXPECTED_WEB_REPLICAS" web
wait_healthy web 300 "$EXPECTED_WEB_REPLICAS"
assert_current_release_service web "$EXPECTED_WEB_REPLICAS"

echo "prod-start: starting Celery"
restart_celery

echo "prod-start: starting deposit and sweeper workers"
compose_crypto up -d --no-deps --force-recreate deposit_service sweeper_service
wait_healthy deposit_service 180 1
wait_healthy sweeper_service 180 1
assert_current_release_service deposit_service 1
assert_current_release_service sweeper_service 1

echo "prod-start: starting Cloudflare ingress"
compose up -d --no-deps --force-recreate cloudflared
wait_healthy cloudflared 120 1

echo "prod-start: final production health checks"
app_preflight
verify_runtime_dependencies
validate_durable_redis

# From this point the Redis volume is the canonical durable production store,
# whether it was migrated previously or intentionally recreated empty here.
write_atomic_sha "$REDIS_COMPLETE_FILE"
record_static_release
record_release
cleanup_static_releases

trap - EXIT
echo "prod-start: production is healthy at $CURRENT_SHA"
echo "prod-start: future code deployments should use deploy/scripts/prod_rolling_update.sh"
