#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

echo "== Rust formatting and static analysis =="
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- \
  -D warnings \
  -A clippy::missing-errors-doc \
  -A clippy::missing-panics-doc \
  -A clippy::too-many-lines
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

echo "== Build shared engine and plugin =="
cargo build -p cymule-cli -p cymule-test-adapter
CYMULE_BIN="$ROOT/target/debug/cymule"
CYMULE_TEST_PLUGIN="$ROOT/target/debug/cymule-test-adapter"
CYMULE_WAIT_ACTIVATION_FIXTURE="$ROOT/tests/fixtures/wait-activation.json"
CYMULE_VIRTUAL_OCCURRENCE_FIXTURE="$ROOT/tests/fixtures/virtual-work-occurrence.json"
CYMULE_VIRTUAL_CONTROL_FIXTURE="$ROOT/tests/fixtures/virtual-work-control.json"
CYMULE_VIRTUAL_MIGRATION_FIXTURE="$ROOT/tests/fixtures/virtual-region-migration-control.json"
export CYMULE_BIN CYMULE_TEST_PLUGIN CYMULE_WAIT_ACTIVATION_FIXTURE
export CYMULE_VIRTUAL_OCCURRENCE_FIXTURE CYMULE_VIRTUAL_CONTROL_FIXTURE
export CYMULE_VIRTUAL_MIGRATION_FIXTURE

echo "== Frozen schemas and semantic rejection =="
uv run --project sdk/python --frozen python "$ROOT/scripts/validate_schemas.py" "$ROOT" "$CYMULE_BIN"
if "$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/invalid-plan.json" >/dev/null 2>&1; then
  echo "invalid semantic plan was unexpectedly accepted" >&2
  exit 1
fi

CYMULE_EXPECTED_PLAN_ID=$(
  "$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/cross-language-plan.json" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["plan_id"])'
)
export CYMULE_EXPECTED_PLAN_ID
echo "shared Plan ID: $CYMULE_EXPECTED_PLAN_ID"

CYMULE_EXPECTED_RESOURCE_ID=$(
  "$CYMULE_BIN" resource seal --input "$ROOT/tests/fixtures/resource-candidate.json" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["resource_id"])'
)
export CYMULE_EXPECTED_RESOURCE_ID
echo "shared Resource ID: $CYMULE_EXPECTED_RESOURCE_ID"

echo "== Hello World user quick start =="
mkdir -p "$ROOT/.cache"
cargo run --quiet -p cymule-example-hello-world -- Ada \
  > "$ROOT/.cache/hello-world-result.json"
python3 -c 'import json, pathlib, sys; result = json.loads(pathlib.Path(sys.argv[1]).read_text()); assert result["run_id"] == "run:hello-world"; assert result["value"] == {"message": "Hello, Ada!"}; assert len(result["effects"]) == 1' \
  "$ROOT/.cache/hello-world-result.json"
cargo run --quiet -p cymule-example-hello-world -- Ada --unknown-once \
  > "$ROOT/.cache/hello-world-unknown-result.json"
python3 -c 'import json, pathlib, sys; result = json.loads(pathlib.Path(sys.argv[1]).read_text()); assert result["value"] == {"message": "Hello, Ada!"}; assert len(result["effects"]) == 1' \
  "$ROOT/.cache/hello-world-unknown-result.json"

echo "== Rust SDK end-to-end =="
cargo test -p cymule-sdk --test cross_language

echo "== TypeScript SDK end-to-end =="
pnpm --dir sdk/typescript install --frozen-lockfile
pnpm --dir sdk/typescript run build
pnpm --dir sdk/typescript test
node sdk/typescript/scripts/prepare-package.mjs \
  cymule "$ROOT/.cache/npm-cymule"
node sdk/typescript/scripts/prepare-package.mjs \
  '@cymule/sdk' "$ROOT/.cache/npm-cymule-sdk"
npm pack --dry-run --json "$ROOT/.cache/npm-cymule" >/dev/null
npm pack --dry-run --json "$ROOT/.cache/npm-cymule-sdk" >/dev/null

echo "== Python SDK end-to-end =="
uv run --project sdk/python --frozen python -m unittest discover -s "$ROOT/sdk/python/tests"

echo "== Go SDK end-to-end =="
(cd sdk/go && go test ./...)

echo "== Optional MLIR workbench =="
"$ROOT/compiler/mlir/verify.sh"

echo "All Cymule verification passed."
