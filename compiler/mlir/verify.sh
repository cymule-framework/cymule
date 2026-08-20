#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MLIR_OPT=${MLIR_OPT:-}
if [ -z "$MLIR_OPT" ]; then
  if command -v mlir-opt >/dev/null 2>&1; then
    MLIR_OPT=$(command -v mlir-opt)
  elif [ -x /opt/homebrew/opt/llvm/bin/mlir-opt ]; then
    MLIR_OPT=/opt/homebrew/opt/llvm/bin/mlir-opt
  else
    echo "MLIR smoke skipped: mlir-opt is unavailable"
    exit 77
  fi
fi

"$MLIR_OPT" --allow-unregistered-dialect "$SCRIPT_DIR/examples/cross-language.mlir" >/dev/null
echo "MLIR smoke passed with $($MLIR_OPT --version | head -n 1)"
