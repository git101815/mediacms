#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/rolling_update_common.sh"

configure_rolling_update \
  "${COMPOSE_PROJECT_NAME:-mediacms-prod}" \
  "${COMPOSE_FILE:-docker-compose-cloudflare.yaml}" \
  "${REDIS_VOLUME_NAME:-mediacms-prod-redis-data}" \
  "production" \
  2 \
  "scaled" \
  1 \
  "production"

[[ "$PROJECT" == "mediacms-prod" ]] || die "production updater requires COMPOSE_PROJECT_NAME=mediacms-prod"
[[ "$COMPOSE_FILE" == "docker-compose-cloudflare.yaml" ]] || die "production updater requires docker-compose-cloudflare.yaml"

rolling_update_main
