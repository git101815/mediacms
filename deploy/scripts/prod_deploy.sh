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

  # Create at least one freshly started web process before draining old ones.
  compose up -d --no-deps --no-recreate --scale web=3 web
  wait_healthy web 300

  for old_id in "${old_ids[@]}"; do
    [[ -n "$old_id" ]] || continue
    if docker inspect "$old_id" >/dev/null 2>&1; then
      docker stop --time 90 "$old_id" >/dev/null
      docker rm "$old_id" >/dev/null
      compose up -d --no-deps --no-recreate --scale web=3 web
      wait_healthy web 300
    fi
  done

  # Production steady state keeps redundancy for webhooks and normal traffic.
  compose up -d --no-deps --no-recreate --scale web=2 web
  wait_healthy web 300
else
  [[ "${ALLOW_SINGLE_WEB_RECREATE:-0}" == 1 ]] || {
    echo "Non-Cloudflare compose binds host :80 and cannot be safely scaled. Set ALLOW_SINGLE_WEB_RECREATE=1 to accept a brief web restart." >&2
    exit 2
  }
  compose up -d --no-deps web
  wait_healthy web 300
fi

compose up -d --no-deps celery_worker celery_beat

# Crypto workers are durable/idempotent already; recreate them only when they
# are currently deployed or the operator explicitly asks for them.
if [[ "${CRYPTO_WORKERS:-0}" == 1 || -n "$(service_container_ids deposit_service)$(service_container_ids sweeper_service)" ]]; then
  compose_crypto up -d --no-deps --build dfx_signer_service deposit_service sweeper_service
fi

prod_preflight
echo "Production deploy complete; full-stack shutdown was not used."
