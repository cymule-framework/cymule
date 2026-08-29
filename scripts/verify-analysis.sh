#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

OUTPUT_DIR=.cache/test-analysis
mkdir -p "$OUTPUT_DIR"

require_mutants() {
  command -v cargo-mutants >/dev/null 2>&1 || {
    echo "cargo-mutants 27.1.0 is required" >&2
    exit 2
  }
  cargo mutants --version | grep -Fx 'cargo-mutants 27.1.0' >/dev/null || {
    echo "cargo-mutants must be exactly 27.1.0" >&2
    exit 2
  }
}

require_nonempty_artifact() {
  artifact_path=$1
  if [ ! -s "$artifact_path" ]; then
    echo "analysis artifact is missing or empty: $artifact_path" >&2
    exit 2
  fi
}

run_mutants() {
  mutation_package=$1
  mutation_output=$2
  mutation_filter=$3
  rm -rf "$mutation_output"
  set -- cargo mutants \
    --package "$mutation_package" \
    --copy-target true \
    --jobs "${CYMULE_MUTATION_JOBS:-2}" \
    --timeout-multiplier 3 \
    --output "$mutation_output"
  if [ -n "$mutation_filter" ]; then
    set -- "$@" --re "$mutation_filter"
  fi
  if [ -n "${CYMULE_MUTATION_SHARD:-}" ]; then
    if ! printf '%s' "$CYMULE_MUTATION_SHARD" | grep -Eq '^[0-9]+/[1-9][0-9]*$'; then
      echo "CYMULE_MUTATION_SHARD must use zero-based K/N form" >&2
      exit 2
    fi
    set -- "$@" --shard "$CYMULE_MUTATION_SHARD"
  fi
  "$@"
}

require_mutant_inventory() {
  mutation_package=$1
  mutation_filter=$2
  shift 2
  mutation_inventory=$(cargo mutants \
    --package "$mutation_package" \
    --re "$mutation_filter" \
    --list)
  for required_symbol in "$@"; do
    if ! printf '%s\n' "$mutation_inventory" | grep -F "$required_symbol" >/dev/null; then
      echo "$mutation_package mutation selector omitted $required_symbol" >&2
      exit 2
    fi
  done
}

case "${1:-}" in
  coverage)
    command -v cargo-llvm-cov >/dev/null 2>&1 || {
      echo "cargo-llvm-cov 0.9.0 is required" >&2
      exit 2
    }
    cargo llvm-cov --version | grep -Fx 'cargo-llvm-cov 0.9.0' >/dev/null || {
      echo "cargo-llvm-cov must be exactly 0.9.0" >&2
      exit 2
    }
    cargo llvm-cov clean --workspace
    cargo llvm-cov \
      --package cymule-core \
      --package cymule-durable \
      --package cymule-evolution \
      --package cymule-virtual \
      --tests \
      --json \
      --summary-only \
      --output-path "$OUTPUT_DIR/coverage.json" \
      --fail-under-lines 72 \
      --fail-under-regions 78
    require_nonempty_artifact "$OUTPUT_DIR/coverage.json"
    cargo llvm-cov \
      --package cymule-profile-protocol \
      --tests \
      --json \
      --summary-only \
      --ignore-filename-regex 'crates/cymule-profile-protocol/src/(agent|error|lib|resource|virtual_work)\.rs' \
      --output-path "$OUTPUT_DIR/coverage-evolution-m4.json" \
      --fail-under-lines 63 \
      --fail-under-regions 64
    require_nonempty_artifact "$OUTPUT_DIR/coverage-evolution-m4.json"
    ;;
  coverage-plugins)
    command -v cargo-llvm-cov >/dev/null 2>&1 || {
      echo "cargo-llvm-cov 0.9.0 is required" >&2
      exit 2
    }
    cargo llvm-cov --version | grep -Fx 'cargo-llvm-cov 0.9.0' >/dev/null || {
      echo "cargo-llvm-cov must be exactly 0.9.0" >&2
      exit 2
    }
    cargo llvm-cov clean --workspace
    cargo llvm-cov \
      --package cymule-store-sqlite \
      --package cymule-resource-fs \
      --package cymule-resource-object-store \
      --package cymule-activation-http \
      --package cymule-activation-timer \
      --package cymule-clock-system \
      --package cymule-executor-process \
      --package cymule-observability-otel \
      --package cymule-agent-mcp \
      --tests \
      --json \
      --summary-only \
      --output-path "$OUTPUT_DIR/coverage-plugins.json" \
      --fail-under-lines 72 \
      --fail-under-regions 72
    ;;
  mutation)
    require_mutants
    run_mutants cymule-core "$OUTPUT_DIR/mutation" ''
    ;;
  mutation-evolution-m4)
    require_mutants
    m4_mutation_filter='analyze_relink|validate_migration_no_widening|MigrationSafePoint::(new|verify|verify_source_continuation|derived_id)|prepare_definition_publication|reduce_dependent_template_relink|build_relink_edge|update_decision|provider_required_artifacts|prepare_evolution_migration_target|admit_evolution_target_binding|verify_evolution_target_binding_record|EvolutionReductionSource::retained_migration|prevalidate_migration_source|reduce_migration_command|reduce_new_migration|prepare_evolution_selection|reduce_evolution_selection|verify_migration_material_authority|EvolutionPostcondition::migration_sidecar|validate_restart_preflight|reduce_restart_command|verify_target_program_counters|derive_plan_edge_id|verify_plan_edge|verify_edge_mutation_authority|derive_rollout_evaluation_id|derive_rollout_transition_id|verify_rollout_transition'
    require_mutant_inventory \
      cymule-profile-protocol \
      "$m4_mutation_filter" \
      analyze_relink \
      validate_migration_no_widening \
      'MigrationSafePoint::verify ->' \
      MigrationSafePoint::derived_id \
      prepare_definition_publication \
      build_relink_edge \
      update_decision \
      provider_required_artifacts \
      prepare_evolution_migration_target \
      admit_evolution_target_binding \
      verify_evolution_target_binding_record \
      EvolutionReductionSource::retained_migration \
      prevalidate_migration_source \
      reduce_migration_command \
      reduce_new_migration \
      prepare_evolution_selection \
      reduce_evolution_selection \
      verify_migration_material_authority \
      EvolutionPostcondition::migration_sidecar \
      reduce_restart_command \
      derive_plan_edge_id \
      verify_plan_edge \
      verify_edge_mutation_authority \
      derive_rollout_evaluation_id \
      derive_rollout_transition_id \
      verify_rollout_transition
    run_mutants \
      cymule-profile-protocol \
      "$OUTPUT_DIR/mutation-evolution-m4" \
      "$m4_mutation_filter"
    ;;
  mutation-plugins)
    require_mutants
    mkdir -p "$OUTPUT_DIR/mutation-plugins"
    http_mutation_filter='SqliteHttpSignalDriver.*receive|SqliteHttpSignalDriver.*acknowledge|receive_durable_signal|register_waiter|unregister_waiter|notify_waiters|read_acknowledged|persist_request|decode_request|require_current_http_spool|initialize_empty_http_spool'
    timer_mutation_filter='schedule|SqliteTimerDriver.*receive|SqliteTimerDriver.*acknowledge|initialize_or_require_timer_store|initialize_empty_timer_store|require_current_timer_schema'
    require_mutant_inventory \
      cymule-activation-http \
      "$http_mutation_filter" \
      receive_durable_signal \
      register_waiter \
      unregister_waiter \
      notify_waiters \
      read_acknowledged_async \
      read_acknowledged \
      decode_request \
      initialize_empty_http_spool \
      require_current_http_spool \
      persist_request
    require_mutant_inventory \
      cymule-activation-timer \
      "$timer_mutation_filter" \
      schedule \
      initialize_or_require_timer_store \
      initialize_empty_timer_store \
      require_current_timer_schema
    run_mutants \
      cymule-store-sqlite \
      "$OUTPUT_DIR/mutation-plugins/store-sqlite" \
      'compare_and_swap|SqliteStore::load'
    run_mutants \
      cymule-activation-http \
      "$OUTPUT_DIR/mutation-plugins/activation-http" \
      "$http_mutation_filter"
    run_mutants \
      cymule-activation-timer \
      "$OUTPUT_DIR/mutation-plugins/activation-timer" \
      "$timer_mutation_filter"
    run_mutants \
      cymule-clock-system \
      "$OUTPUT_DIR/mutation-plugins/clock-system" \
      'SqliteClock.*observe|ClockObservation::verify'
    run_mutants \
      cymule-executor-process \
      "$OUTPUT_DIR/mutation-plugins/executor-process" \
      'ProcessExecutor::invoke|read_limited'
    run_mutants \
      cymule-agent-mcp \
      "$OUTPUT_DIR/mutation-plugins/agent-mcp" \
      'invoke_tool_async|map_content|validate_tool_request'
    ;;
  *)
    echo "usage: $0 coverage|coverage-plugins|mutation|mutation-evolution-m4|mutation-plugins" >&2
    exit 2
    ;;
esac
