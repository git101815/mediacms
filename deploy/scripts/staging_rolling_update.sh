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

# The shared updater owns the staging connector lifecycle, but intentionally
# never creates/rotates tunnel credentials and never mutates Cloudflare DNS.
# A changed connector is recreated only after staging_ingress is healthy.
rolling_update_main
