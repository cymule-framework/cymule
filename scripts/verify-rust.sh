#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

echo "== Rust formatting =="
cargo fmt --all -- --check

if [ "$#" -eq 0 ]; then
  echo "== Rust workspace static analysis =="
  cargo clippy --workspace --all-targets -- \
    -D warnings \
    -A clippy::missing-errors-doc \
    -A clippy::missing-panics-doc \
    -A clippy::too-many-lines
  echo "== Rust workspace tests =="
  cargo test --workspace
  echo "== Rust workspace documentation =="
  RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
  exit 0
fi

package_args=""
for package in "$@"; do
  case "$package" in
    *[!a-zA-Z0-9_-]*)
      echo "invalid Cargo package name: $package" >&2
      exit 2
      ;;
  esac
  package_args="$package_args -p $package"
done

echo "== Selected Rust static analysis:$package_args =="
# Package names are constrained above before intentional argument expansion.
# shellcheck disable=SC2086
cargo clippy $package_args --all-targets -- \
  -D warnings \
  -A clippy::missing-errors-doc \
  -A clippy::missing-panics-doc \
  -A clippy::too-many-lines

echo "== Selected Rust tests:$package_args =="
# shellcheck disable=SC2086
cargo test $package_args

echo "== Selected Rust documentation:$package_args =="
# shellcheck disable=SC2086
RUSTDOCFLAGS="-D warnings" cargo doc $package_args --no-deps
