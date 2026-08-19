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
    run_mutants \
      cymule-evolution \
      "$OUTPUT_DIR/mutation-evolution-m4" \
      'compatibility\.rs|MigrationSafePoint|restart_under_new_plan|link_registered'
    ;;
  mutation-plugins)
    require_mutants
    mkdir -p "$OUTPUT_DIR/mutation-plugins"
    run_mutants \
      cymule-store-sqlite \
      "$OUTPUT_DIR/mutation-plugins/store-sqlite" \
      'compare_and_swap|SqliteStore::load'
    run_mutants \
      cymule-activation-http \
      "$OUTPUT_DIR/mutation-plugins/activation-http" \
      'HttpSignalDriver::receive|HttpSignalDriver::acknowledge|receive_signal'
    run_mutants \
      cymule-activation-timer \
      "$OUTPUT_DIR/mutation-plugins/activation-timer" \
      'schedule|SqliteTimerDriver.*receive|SqliteTimerDriver.*acknowledge'
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
