#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

exec python3 scripts/crates_release.py verify --allow-dirty "$@"
