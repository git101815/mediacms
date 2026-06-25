#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${ENV_FILE:-.env.maintenance}"

if [ ! -f "$ENV_FILE" ]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

: "${N8N_MAINTENANCE_SWITCH_WEBHOOK:?Missing N8N_MAINTENANCE_SWITCH_WEBHOOK}"
: "${N8N_MAINTENANCE_SECRET:?Missing N8N_MAINTENANCE_SECRET}"

curl -fsS \
  -X POST "$N8N_MAINTENANCE_SWITCH_WEBHOOK" \
  -H 'Content-Type: application/json' \
  -d "{\"mode\":\"off\",\"secret\":\"${N8N_MAINTENANCE_SECRET}\"}"

echo
echo "Production route enabled."