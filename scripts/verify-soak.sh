#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

PROPTEST_CASES=${PROPTEST_CASES:-4096}
CYMULE_SOAK_REPETITIONS=${CYMULE_SOAK_REPETITIONS:-3}
case "$PROPTEST_CASES:$CYMULE_SOAK_REPETITIONS" in
  *[!0-9:]* | :* | *: | 0:* | *:0)
    echo "PROPTEST_CASES and CYMULE_SOAK_REPETITIONS must be positive integers" >&2
    exit 2
    ;;
esac
export PROPTEST_CASES

echo "== Causal replay properties: $PROPTEST_CASES generated cases =="
cargo test -p cymule-core --test semantic_kernel \
  independent_causal_facts_replay_to_one_digest -- --exact

iteration=1
while [ "$iteration" -le "$CYMULE_SOAK_REPETITIONS" ]; do
  echo "== Deterministic fault sweep $iteration/$CYMULE_SOAK_REPETITIONS =="
  cargo test -p cymule-durable --test resume \
    every_run_cas_boundary_recovers_from_io_failure_or_lost_acknowledgement -- --exact
  cargo test -p cymule-durable --test resume \
    recovery_survives_lost_unknown_receipt_after_provider_crash -- --exact
  cargo test -p cymule-virtual --test scheduler \
    archive_fault_sweep_never_partially_mutates_scheduler -- --exact
  cargo test -p cymule-virtual --test scheduler \
    multi_worker_claim_renew_recover_and_late_output_matrix_is_fenced -- --exact
  iteration=$((iteration + 1))
done
