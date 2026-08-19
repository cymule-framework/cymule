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
  cargo test -p cymule-store-sqlite --test store \
    active_sqlite_writer_returns_immediately_as_conflict -- --exact
  cargo test -p cymule-resource-fs --test resources \
    chunk_retry_commit_and_reopen_preserve_exact_bytes -- --exact
  cargo test -p cymule-resource-object-store --test object_store \
    object_store_chunk_retry_commit_and_read_are_exact -- --exact
  cargo test -p cymule-activation-http --test http \
    acknowledged_identity_replays_and_conflicting_reuse_fails -- --exact
  cargo test -p cymule-activation-timer --test timer \
    due_timer_redelivers_until_acknowledged -- --exact
  cargo test -p cymule-clock-system --test clock \
    logical_time_advances_across_reopen_and_backward_wall_time -- --exact
  cargo test -p cymule-executor-process --test process \
    process_timeout_is_reported_as_ambiguous -- --exact
  cargo test -p cymule-agent-mcp --test mcp \
    incomplete_mcp_work_is_not_driven_as_an_agent_loop -- --exact
  iteration=$((iteration + 1))
done
