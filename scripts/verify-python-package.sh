#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DIST=$(mktemp -d "${TMPDIR:-/tmp}/cymule-python-package.XXXXXX")
trap 'rm -rf "$DIST"' EXIT HUP INT TERM

uv build --project "$ROOT/sdk/python" --out-dir "$DIST/dist"
python3 -m venv "$DIST/venv"
"$DIST/venv/bin/python" -m pip install --no-deps "$DIST"/dist/*.whl
"$DIST/venv/bin/python" -c 'from cymule import CliEngine, DurableEngine, FlowBuilder; assert CliEngine().executable == "cymule"; assert DurableEngine and FlowBuilder'
