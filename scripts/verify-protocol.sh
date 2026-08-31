#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

echo "== Build shared engine and test plugin =="
cargo build --profile conformance -p cymule-cli -p cymule-test-adapter
cargo test -p cymule-cli --bin cymule
CYMULE_BIN="$ROOT/target/conformance/cymule"

echo "== Frozen schemas and semantic rejection =="
uv run --project sdk/python --frozen python "$ROOT/scripts/validate_schemas.py" "$ROOT" "$CYMULE_BIN"
if "$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/invalid-plan.json" >/dev/null 2>&1; then
  echo "invalid semantic plan was unexpectedly accepted" >&2
  exit 1
fi

echo "shared Plan ID: $("$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/cross-language-plan.json" | python3 -c 'import json, sys; print(json.load(sys.stdin)["plan_id"])')"
echo "shared Resource ID: $("$CYMULE_BIN" resource seal --input "$ROOT/tests/fixtures/resource-candidate.json" | python3 -c 'import json, sys; print(json.load(sys.stdin)["resource_id"])')"
