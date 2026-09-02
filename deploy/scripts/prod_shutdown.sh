#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

[[ "${CONFIRM_SHUTDOWN:-}" == "$PROJECT" ]] || {
  echo "Set CONFIRM_SHUTDOWN=$PROJECT to perform a full production shutdown." >&2
  exit 2
}

if ! redis_is_persistent; then
  cat >&2 <<EOF_REDIS
Refusing full production shutdown: Redis is not yet mounted on '$REDIS_VOLUME'.
Run the one-time persistence migration first:
  CONFIRM_REDIS_MIGRATION=$PROJECT deploy/scripts/prod_migrate_redis_persistence.sh
EOF_REDIS
  exit 2
fi

if legacy_app_mounts_present; then
  # A post-pull legacy container sees the new checkout immediately. Do not run
  # fresh Django/Celery inspection code inside it before migrations. Beat is
  # stopped first; the worker gets its normal warm TERM shutdown and queued
  # tasks remain durable in the already-persistent Redis volume.
  echo "Legacy bind-mounted application containers detected; using warm-stop shutdown path."
  if service_exists celery_beat; then stop_service_if_running celery_beat; fi
  stop_service_if_running celery_worker
else
  prod_preflight
  drain_celery
fi

# Finish crypto worker iterations while the application/API is still reachable.
stop_crypto_service_if_running deposit_service
stop_crypto_service_if_running sweeper_service
stop_service_if_running dfx_signer_service

# Ingress is stopped only after background financial work is quiescent. Every
# stop here is fail-closed: if Docker cannot stop a running critical service,
# do not continue into `compose down` and pretend the stack was quiesced.
stop_service_if_running cloudflared
stop_service_if_running web
stop_service_if_running redis
stop_service_if_running db

# Containers/networks may now be removed. Orphans are intentionally NOT removed.
compose down

echo "Production stack shut down cleanly."
