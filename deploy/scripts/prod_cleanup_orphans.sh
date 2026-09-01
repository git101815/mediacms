#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/prod_common.sh"

mapfile -t expected < <(compose --profile crypto-workers config --services)
mapfile -t rows < <(docker ps -a \
  --filter "label=com.docker.compose.project=$PROJECT" \
  --format '{{.ID}}|{{.Label "com.docker.compose.service"}}|{{.Names}}')

orphans=()
for row in "${rows[@]}"; do
  IFS='|' read -r cid service name <<<"$row"
  found=0
  for valid in "${expected[@]}"; do
    [[ "$service" == "$valid" ]] && found=1 && break
  done
  if (( ! found )); then
    orphans+=("$cid|$service|$name")
  fi
done

if (( ${#orphans[@]} == 0 )); then
  echo "No Compose orphans for project '$PROJECT'."
  exit 0
fi

printf 'Orphans for project %s:\n' "$PROJECT"
printf '  %s\n' "${orphans[@]}"

if [[ "${1:-}" != --apply ]]; then
  echo "Dry-run only. Re-run with --apply and CONFIRM_PROJECT=$PROJECT to remove them."
  exit 0
fi

[[ "${CONFIRM_PROJECT:-}" == "$PROJECT" ]] || {
  echo "Refusing removal: CONFIRM_PROJECT must equal '$PROJECT'." >&2
  exit 2
}

for row in "${orphans[@]}"; do
  IFS='|' read -r cid _service _name <<<"$row"
  docker rm -f "$cid"
done
