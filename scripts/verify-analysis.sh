#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

OUTPUT_DIR=.cache/test-analysis
mkdir -p "$OUTPUT_DIR"

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
    command -v cargo-mutants >/dev/null 2>&1 || {
      echo "cargo-mutants 27.1.0 is required" >&2
      exit 2
    }
    cargo mutants --version | grep -Fx 'cargo-mutants 27.1.0' >/dev/null || {
      echo "cargo-mutants must be exactly 27.1.0" >&2
      exit 2
    }
    rm -rf "$OUTPUT_DIR/mutation"
    run_mutants() {
      cargo mutants \
        --package cymule-core \
        --copy-target true \
        --jobs "${CYMULE_MUTATION_JOBS:-2}" \
        --timeout-multiplier 3 \
        --output "$OUTPUT_DIR/mutation" \
        "$@"
    }
    if [ -n "${CYMULE_MUTATION_SHARD:-}" ]; then
      if ! printf '%s' "$CYMULE_MUTATION_SHARD" | grep -Eq '^[1-9][0-9]*/[1-9][0-9]*$'; then
        echo "CYMULE_MUTATION_SHARD must use one-based N/M form" >&2
        exit 2
      fi
      run_mutants --shard "$CYMULE_MUTATION_SHARD"
    else
      run_mutants
    fi
    ;;
  *)
    echo "usage: $0 coverage|mutation" >&2
    exit 2
    ;;
esac
