#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

echo "== Rust formatting =="
cargo fmt --all -- --check

echo "== Rust workspace static analysis =="
cargo clippy --workspace --all-targets -- \
  -D warnings \
  -A clippy::missing-errors-doc \
  -A clippy::missing-panics-doc \
  -A clippy::too-many-lines

echo "== Rust workspace documentation =="
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
