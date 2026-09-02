#!/bin/sh
set -eu

STAMP="node_modules/.mediacms-dependencies.sha256"

dependency_hash="$(
    {
        printf '%s\0' package.json package-lock.json
        find packages/scripts packages/player -type f \
            ! -path '*/node_modules/*' \
            ! -path '*/dist/*' -print0
    } | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}'
)"

installed_hash=""
if [ -f "$STAMP" ]; then
    installed_hash="$(cat "$STAMP")"
fi

if [ "$installed_hash" != "$dependency_hash" ]; then
    echo "Frontend dependencies changed; running npm ci"
    npm ci --prefer-offline --no-audit --no-fund
    printf '%s\n' "$dependency_hash" > "$STAMP"
else
    echo "Frontend dependencies unchanged; reusing node_modules"
fi

exec npm run start
