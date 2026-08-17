#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
STATE_DIR=${CYMULE_EXAMPLE_DIR:-"$ROOT/.cymule/examples/hello-world"}
RUN_ID=${CYMULE_EXAMPLE_RUN_ID:-"run:hello-world"}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$ROOT/target"}

mkdir -p "$STATE_DIR"
cd "$ROOT"

echo "Building Cymule and the Hello World plugin..." >&2
cargo build --quiet -p cymule-cli -p cymule-example-hello-plugin

ENGINE="$CARGO_TARGET_DIR/debug/cymule"
PLUGIN="$CARGO_TARGET_DIR/debug/cymule-example-hello-plugin"
PLAN="$STATE_DIR/plan.json"
RESULT="$STATE_DIR/result.json"

echo "Sealing the Hello World Flow..." >&2
"$ENGINE" seal --input "$SCRIPT_DIR/flow.json" > "$PLAN"

echo "Executing $RUN_ID..." >&2
"$ENGINE" run \
  --plan "$PLAN" \
  --input "$SCRIPT_DIR/input.json" \
  --plugin "$PLUGIN" \
  --run-id "$RUN_ID" \
  > "$RESULT"

cat "$RESULT"
