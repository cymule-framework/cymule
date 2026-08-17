#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"
mkdir -p "$ROOT/.cache"

pnpm --dir sdk/typescript install --frozen-lockfile
pnpm --dir sdk/typescript run build
node sdk/typescript/scripts/prepare-package.mjs cymule "$ROOT/.cache/npm-cymule"
node sdk/typescript/scripts/prepare-package.mjs '@cymule/sdk' "$ROOT/.cache/npm-cymule-sdk"
npm pack --dry-run --json "$ROOT/.cache/npm-cymule" >/dev/null
npm pack --dry-run --json "$ROOT/.cache/npm-cymule-sdk" >/dev/null
