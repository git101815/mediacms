#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/rolling_update_common.sh"

configure_rolling_update \
  "${COMPOSE_PROJECT_NAME:-mediacms-staging}" \
  "${COMPOSE_FILE:-docker-compose.yaml}" \
  "${REDIS_VOLUME_NAME:-mediacms-staging-redis-data}" \
  "staging" \
  1 \
  "scaled" \
  0 \
  "staging"

[[ "$PROJECT" == "mediacms-staging" ]] || die "staging updater requires COMPOSE_PROJECT_NAME=mediacms-staging"
[[ "$COMPOSE_FILE" == "docker-compose.yaml" ]] || die "staging updater requires docker-compose.yaml"

# Intentionally no maintenance-on/off, DNS, tunnel-token mutation or
# Cloudflare routing operation exists in this script or the shared updater.
rolling_update_main
