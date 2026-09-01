#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

[[ "${CONFIRM_SHUTDOWN:-}" == "$PROJECT" ]] || {
  echo "Set CONFIRM_SHUTDOWN=$PROJECT to perform a full production shutdown." >&2
  exit 2
}

prod_preflight
drain_celery

# Finish crypto worker iterations while the application/API is still reachable.
compose_crypto stop deposit_service sweeper_service 2>/dev/null || true
service_exists dfx_signer_service && compose stop dfx_signer_service || true

# Ingress is stopped only after background financial work is quiescent.
service_exists cloudflared && compose stop cloudflared || true
compose stop web || true
compose stop redis || true
compose stop db || true

# Containers/networks may now be removed. Orphans are intentionally NOT removed.
compose down

echo "Production stack shut down cleanly."
