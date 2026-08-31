#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 rust|typescript|python|go" >&2
  exit 2
fi

LANGUAGE=$1
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

case ${CYMULE_SDK_PREBUILT:-0} in
  0)
    cargo build --locked --profile conformance -p cymule-cli -p cymule-test-adapter
    CYMULE_BIN="$ROOT/target/conformance/cymule"
    CYMULE_TEST_PLUGIN="$ROOT/target/conformance/cymule-test-adapter"
    ;;
  1)
    : "${CYMULE_BIN:?CYMULE_BIN is required when CYMULE_SDK_PREBUILT=1}"
    : "${CYMULE_TEST_PLUGIN:?CYMULE_TEST_PLUGIN is required when CYMULE_SDK_PREBUILT=1}"
    ;;
  *)
    echo "CYMULE_SDK_PREBUILT must be 0 or 1" >&2
    exit 2
    ;;
esac
test -x "$CYMULE_BIN"
test -x "$CYMULE_TEST_PLUGIN"
CYMULE_WAIT_ACTIVATION_FIXTURE="$ROOT/tests/fixtures/wait-activation.json"
CYMULE_DURABLE_CONTROL_FIXTURE="$ROOT/tests/fixtures/durable-control.json"
CYMULE_DURABLE_CANCEL_FIXTURE="$ROOT/tests/fixtures/durable-cancel-control.json"
CYMULE_DURABLE_TERMINAL_FIXTURE="$ROOT/tests/fixtures/durable-terminal-responses.json"
CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE="$ROOT/tests/fixtures/applied-effect-summary.json"
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
CYMULE_EVOLUTION_RESTART_FIXTURE="$ROOT/tests/fixtures/evolution-restart-control.json"
CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE="$ROOT/tests/fixtures/live-evolution-control.json"
CYMULE_ENGINE_FAILURE_FIXTURE="$ROOT/tests/fixtures/engine-failures.json"
CYMULE_MALICIOUS_ENGINE="$ROOT/tests/fixtures/malicious-engine"
CYMULE_MALICIOUS_EFFECT_ENGINE="$ROOT/tests/fixtures/malicious-effect-engine"
CYMULE_UNSUPPORTED_ENGINE="$ROOT/tests/fixtures/unsupported-engine-protocol"
CYMULE_SLOW_ENGINE="$ROOT/tests/fixtures/slow-engine"
CYMULE_RUST_SDK_CONFORMANCE_REQUIRED=1
for fixture in \
  "$CYMULE_WAIT_ACTIVATION_FIXTURE" \
  "$CYMULE_DURABLE_CONTROL_FIXTURE" \
  "$CYMULE_DURABLE_CANCEL_FIXTURE" \
  "$CYMULE_DURABLE_TERMINAL_FIXTURE" \
  "$CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE" \
  "$CYMULE_VIRTUAL_OCCURRENCE_FIXTURE" \
  "$CYMULE_VIRTUAL_CONTROL_FIXTURE" \
  "$CYMULE_VIRTUAL_MIGRATION_FIXTURE" \
  "$CYMULE_VIRTUAL_COMPACTION_FIXTURE" \
  "$CYMULE_VIRTUAL_REHYDRATION_FIXTURE" \
  "$CYMULE_VIRTUAL_CLAIM_FIXTURE" \
  "$CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE" \
  "$CYMULE_VIRTUAL_RECOVERY_FIXTURE" \
  "$CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE" \
  "$CYMULE_EVOLUTION_CONTROL_FIXTURE" \
  "$CYMULE_EVOLUTION_RESTART_FIXTURE" \
  "$CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE" \
  "$CYMULE_ENGINE_FAILURE_FIXTURE"
do
  test -r "$fixture"
done
for executable_fixture in \
  "$CYMULE_MALICIOUS_ENGINE" \
  "$CYMULE_MALICIOUS_EFFECT_ENGINE" \
  "$CYMULE_UNSUPPORTED_ENGINE" \
  "$CYMULE_SLOW_ENGINE"
do
  test -x "$executable_fixture"
done
CYMULE_EXPECTED_PLAN_ID=$("$CYMULE_BIN" seal --input "$ROOT/tests/fixtures/cross-language-plan.json" | sed -n 's/.*"plan_id"[[:space:]]*:[[:space:]]*"\(sha256:[0-9a-f]\{64\}\)".*/\1/p')
CYMULE_EXPECTED_RESOURCE_ID=$("$CYMULE_BIN" resource seal --input "$ROOT/tests/fixtures/resource-candidate.json" | sed -n 's/.*"resource_id"[[:space:]]*:[[:space:]]*"\(sha256:[0-9a-f]\{64\}\)".*/\1/p')
test -n "$CYMULE_EXPECTED_PLAN_ID"
test -n "$CYMULE_EXPECTED_RESOURCE_ID"
export CYMULE_BIN CYMULE_TEST_PLUGIN CYMULE_WAIT_ACTIVATION_FIXTURE
export CYMULE_DURABLE_CONTROL_FIXTURE CYMULE_DURABLE_CANCEL_FIXTURE
export CYMULE_DURABLE_TERMINAL_FIXTURE
export CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE
export CYMULE_VIRTUAL_OCCURRENCE_FIXTURE CYMULE_VIRTUAL_CONTROL_FIXTURE
export CYMULE_VIRTUAL_MIGRATION_FIXTURE CYMULE_EXPECTED_PLAN_ID
export CYMULE_VIRTUAL_COMPACTION_FIXTURE CYMULE_VIRTUAL_REHYDRATION_FIXTURE
export CYMULE_VIRTUAL_CLAIM_FIXTURE CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE
export CYMULE_VIRTUAL_RECOVERY_FIXTURE
export CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE
export CYMULE_EVOLUTION_CONTROL_FIXTURE
export CYMULE_EVOLUTION_RESTART_FIXTURE
export CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE
export CYMULE_ENGINE_FAILURE_FIXTURE
export CYMULE_MALICIOUS_ENGINE CYMULE_MALICIOUS_EFFECT_ENGINE CYMULE_SLOW_ENGINE
export CYMULE_UNSUPPORTED_ENGINE CYMULE_RUST_SDK_CONFORMANCE_REQUIRED
export CYMULE_EXPECTED_RESOURCE_ID

case "$LANGUAGE" in
  rust)
    cargo test --locked -p cymule --lib --test facade --test cross_language
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
