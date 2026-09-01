#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

prod_preflight

if ! redis_is_persistent; then
  cat >&2 <<EOF
Refusing normal deploy: Redis is not yet mounted on '$REDIS_VOLUME'.
Run the one-time migration first:
  CONFIRM_REDIS_MIGRATION=$PROJECT deploy/scripts/prod_migrate_redis_persistence.sh
EOF
  exit 2
fi

# Stop task production, finish already accepted work, then stop workers cleanly.
drain_celery

# Build before touching the live web containers. The bind-mounted checkout means
# this is not immutable-image blue/green, but the running uWSGI processes keep
# their already imported code until individually drained.
compose build migrations web celery_worker celery_beat

# Expand-compatible migrations run while the old web is still serving. A
# failing migration aborts here and no live web container is removed.
compose run --rm migrations

if [[ "$COMPOSE_FILE" == *cloudflare* ]]; then
  mapfile -t old_ids < <(service_container_ids web)

  # A crashed third container must not be hidden by `compose ps`: wait_healthy
  # includes exited containers and requires exactly three healthy replicas.
  compose up -d --no-deps --no-recreate --scale web=3 web
  wait_healthy web 300 3

  for old_id in "${old_ids[@]}"; do
    [[ -n "$old_id" ]] || continue
    if docker inspect "$old_id" >/dev/null 2>&1; then
      docker stop --time 90 "$old_id" >/dev/null
      docker rm "$old_id" >/dev/null
      compose up -d --no-deps --no-recreate --scale web=3 web
      wait_healthy web 300 3
    fi
  done

  # Production steady state keeps redundancy for webhooks and normal traffic.
  compose up -d --no-deps --no-recreate --scale web=2 web
  wait_healthy web 300 2
else
  [[ "${ALLOW_SINGLE_WEB_RECREATE:-0}" == 1 ]] || {
    echo "Non-Cloudflare compose binds host :80 and cannot be safely scaled. Set ALLOW_SINGLE_WEB_RECREATE=1 to accept a brief web restart." >&2
    exit 2
  }
  compose up -d --no-deps web
  wait_healthy web 300 1
fi

compose up -d --no-deps celery_worker celery_beat
wait_healthy celery_worker 120 1
wait_healthy celery_beat 120 1

# Preserve the operator's currently deployed crypto subset unless CRYPTO_WORKERS=1
# explicitly requests both loops. Build while they are still live, then stop the
# loops before recreating the signer they depend on.
deposit_running=0
sweeper_running=0
[[ -n "$(service_container_ids deposit_service)" ]] && deposit_running=1
[[ -n "$(service_container_ids sweeper_service)" ]] && sweeper_running=1

if [[ "${CRYPTO_WORKERS:-0}" == 1 || "$deposit_running" == 1 || "$sweeper_running" == 1 ]]; then
  deploy_deposit="$deposit_running"
  deploy_sweeper="$sweeper_running"
  if [[ "${CRYPTO_WORKERS:-0}" == 1 ]]; then
    deploy_deposit=1
    deploy_sweeper=1
  fi

  build_services=(dfx_signer_service)
  [[ "$deploy_deposit" == 1 ]] && build_services+=(deposit_service)
  [[ "$deploy_sweeper" == 1 ]] && build_services+=(sweeper_service)
  compose_crypto build "${build_services[@]}"

  [[ "$deposit_running" == 1 ]] && compose_crypto stop deposit_service >/dev/null
  [[ "$sweeper_running" == 1 ]] && compose_crypto stop sweeper_service >/dev/null

  compose_crypto up -d --no-deps dfx_signer_service
  wait_healthy dfx_signer_service 180 1

  if [[ "$deploy_deposit" == 1 ]]; then
    compose_crypto up -d --no-deps deposit_service
    wait_healthy deposit_service 120 1
  fi
  if [[ "$deploy_sweeper" == 1 ]]; then
    compose_crypto up -d --no-deps sweeper_service
    wait_healthy sweeper_service 120 1
  fi
fi

prod_preflight
echo "Production deploy complete; full-stack shutdown was not used."
