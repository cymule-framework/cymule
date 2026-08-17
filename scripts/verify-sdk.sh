#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 rust|typescript|python|go" >&2
  exit 2
fi

LANGUAGE=$1
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

cargo build -p cymule-cli -p cymule-test-adapter
CYMULE_BIN="$ROOT/target/debug/cymule"
CYMULE_TEST_PLUGIN="$ROOT/target/debug/cymule-test-adapter"
CYMULE_WAIT_ACTIVATION_FIXTURE="$ROOT/tests/fixtures/wait-activation.json"
CYMULE_VIRTUAL_OCCURRENCE_FIXTURE="$ROOT/tests/fixtures/virtual-work-occurrence.json"
CYMULE_VIRTUAL_CONTROL_FIXTURE="$ROOT/tests/fixtures/virtual-work-control.json"
CYMULE_VIRTUAL_MIGRATION_FIXTURE="$ROOT/tests/fixtures/virtual-region-migration-control.json"
CYMULE_VIRTUAL_COMPACTION_FIXTURE="$ROOT/tests/fixtures/virtual-compaction-control.json"
CYMULE_VIRTUAL_REHYDRATION_FIXTURE="$ROOT/tests/fixtures/virtual-rehydration-control.json"
CYMULE_VIRTUAL_CLAIM_FIXTURE="$ROOT/tests/fixtures/virtual-claim-control.json"
CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE="$ROOT/tests/fixtures/virtual-lease-renewal-control.json"
CYMULE_VIRTUAL_RECOVERY_FIXTURE="$ROOT/tests/fixtures/virtual-recovery-control.json"
CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE="$ROOT/tests/fixtures/virtual-run-weight-control.json"
CYMULE_EVOLUTION_CONTROL_FIXTURE="$ROOT/tests/fixtures/evolution-control.json"
CYMULE_EXPECTED_PLAN_ID=$("$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/cross-language-plan.json" | python3 -c 'import json, sys; print(json.load(sys.stdin)["plan_id"])')
CYMULE_EXPECTED_RESOURCE_ID=$("$CYMULE_BIN" resource seal --input "$ROOT/tests/fixtures/resource-candidate.json" | python3 -c 'import json, sys; print(json.load(sys.stdin)["resource_id"])')
export CYMULE_BIN CYMULE_TEST_PLUGIN CYMULE_WAIT_ACTIVATION_FIXTURE
export CYMULE_VIRTUAL_OCCURRENCE_FIXTURE CYMULE_VIRTUAL_CONTROL_FIXTURE
export CYMULE_VIRTUAL_MIGRATION_FIXTURE CYMULE_EXPECTED_PLAN_ID
export CYMULE_VIRTUAL_COMPACTION_FIXTURE CYMULE_VIRTUAL_REHYDRATION_FIXTURE
export CYMULE_VIRTUAL_CLAIM_FIXTURE CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE
export CYMULE_VIRTUAL_RECOVERY_FIXTURE
export CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE
export CYMULE_EVOLUTION_CONTROL_FIXTURE
export CYMULE_EXPECTED_RESOURCE_ID

case "$LANGUAGE" in
  rust)
    cargo test -p cymule-sdk --test cross_language
    ;;
  typescript)
    pnpm --dir sdk/typescript install --frozen-lockfile
    pnpm --dir sdk/typescript run build
    pnpm --dir sdk/typescript test
    ;;
  python)
    uv run --project sdk/python --frozen python -m unittest discover -s "$ROOT/sdk/python/tests"
    ;;
  go)
    (cd sdk/go && go test ./...)
    ;;
  *)
    echo "unsupported SDK language: $LANGUAGE" >&2
    exit 2
    ;;
esac
