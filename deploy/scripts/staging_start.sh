#!/usr/bin/env bash
set -euo pipefail

# Safe staging cold start.
#
# This reconstructs a stopped staging application release and then exposes the
# already-healthy stable ingress through the dedicated Cloudflare Tunnel.
# Normal deployments to a running staging stack must use:
#   deploy/scripts/staging_rolling_update.sh
#
# Optional:
#   CRYPTO_WORKERS=1   also start signer/deposit/sweeper services.
#
# The Cloudflare named tunnel and its public hostname are operator-managed.
# docker-compose.yaml only runs the connector for STAGING_TUNNEL_TOKEN.

MEDIACMS_ROOT="${MEDIACMS_ROOT:-$PWD}"
COMMON="$MEDIACMS_ROOT/deploy/scripts/rolling_update_common.sh"

[[ -f "$COMMON" ]] || {
  echo "staging-start: cannot find $COMMON" >&2
  exit 2
}

# shellcheck source=/dev/null
source "$COMMON"

configure_rolling_update \
  "${COMPOSE_PROJECT_NAME:-mediacms-staging}" \
  "${COMPOSE_FILE:-docker-compose.yaml}" \
  "${REDIS_VOLUME_NAME:-mediacms-staging-redis-data}" \
  "staging" \
  1 \
  "scaled" \
  0 \
  "staging"

[[ "$PROJECT" == "mediacms-staging" ]] || die "staging start requires COMPOSE_PROJECT_NAME=mediacms-staging"
[[ "$COMPOSE_FILE" == "docker-compose.yaml" ]] || die "staging start requires docker-compose.yaml"

# Validate Compose first so a missing STAGING_TUNNEL_TOKEN is reported by
# Docker Compose instead of being misreported as a missing service.
require_environment
service_exists staging_ingress || die "staging compose is missing staging_ingress"
service_exists cloudflared || die "staging compose is missing cloudflared"
acquire_update_lock

if [[ -f "$INPROGRESS_FILE" ]] && [[ "$(progress_get staging_start 0)" != 1 ]]; then
  die "staging.inprogress belongs to another deployment mode; recover/resume it before cold start"
fi

if service_is_running web && [[ "$(progress_get staging_start 0)" != 1 ]]; then
  die "staging web is already running; use deploy/scripts/staging_rolling_update.sh"
fi

bind_progress_to_target
progress_set staging_start 1

staging_start_failure_notice() {
  local rc=$?
  if (( rc != 0 )); then
    echo "staging-start: FAILED for release $CURRENT_SHA." >&2
    echo "staging-start: state is preserved in $INPROGRESS_FILE; fix the cause and rerun the same command." >&2
  fi
  return "$rc"
}
trap staging_start_failure_notice EXIT

MAIN_IMAGE_CHANGED=1
APP_CONFIG_CHANGED=1
FRONTEND_CHANGED=1
STATIC_CHANGED=1
STAGING_INGRESS_CHANGED=1
STAGING_TUNNEL_CHANGED=1

if [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
  DEPOSIT_IMAGE_CHANGED=1
  DEPOSIT_CONFIG_CHANGED=1
  SWEEPER_IMAGE_CHANGED=1
  SWEEPER_CONFIG_CHANGED=1
  SIGNER_IMAGE_CHANGED=1
  SIGNER_CONFIG_CHANGED=1
fi

echo "staging-start: building complete release $CURRENT_SHA"
build_frontend_dist
build_required_images
prepare_staging_ingress

echo "staging-start: starting PostgreSQL"
compose up -d --no-deps db
wait_healthy db 300 1

echo "staging-start: starting Redis"
compose up -d --no-deps redis
wait_healthy redis 180 1

check_pending_migrations

echo "staging-start: preparing release static snapshot"
prepare_static_release
run_migrations_once
finalize_static_release

echo "staging-start: starting web"
compose up -d --no-deps --force-recreate --scale web="$EXPECTED_WEB_REPLICAS" web
wait_healthy web 300 "$EXPECTED_WEB_REPLICAS"
assert_current_release_service web "$EXPECTED_WEB_REPLICAS"

echo "staging-start: starting Celery"
restart_celery

if [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
  echo "staging-start: starting optional crypto workers"
  compose_crypto up -d --no-deps --force-recreate dfx_signer_service
  wait_healthy dfx_signer_service 180 1
  assert_current_release_service dfx_signer_service 1

  compose_crypto up -d --no-deps --force-recreate deposit_service sweeper_service
  wait_healthy deposit_service 180 1
  wait_healthy sweeper_service 180 1
  assert_current_release_service deposit_service 1
  assert_current_release_service sweeper_service 1
fi

echo "staging-start: starting stable staging ingress"
ensure_staging_ingress
require_staging_ingress_healthy

echo "staging-start: starting Cloudflare Tunnel"
ensure_staging_tunnel
require_staging_tunnel_healthy

echo "staging-start: final health checks"
app_preflight
verify_runtime_dependencies
require_staging_ingress_healthy
require_staging_tunnel_healthy

record_static_release
record_release
cleanup_static_releases

trap - EXIT
echo "staging-start: staging is healthy at $CURRENT_SHA"
echo "staging-start: future code deployments should use deploy/scripts/staging_rolling_update.sh"
