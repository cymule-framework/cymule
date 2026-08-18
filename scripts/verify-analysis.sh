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
  *)
    echo "usage: $0 coverage|mutation|mutation-evolution-m4" >&2
    exit 2
    ;;
esac
