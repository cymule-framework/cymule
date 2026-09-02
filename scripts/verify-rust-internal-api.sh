#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

# Temporary lexical smoke gate for ADR 0006. This does not replace the planned
# package split, cargo-metadata allowlist, rustdoc public-API snapshot, semver
# check, or facade compile-fail fixtures.
internal_consumers=$(git grep -l 'cymule_core::durable_internal' -- '*.rs' \
  | cut -d/ -f1-2 | sort -u)
expected_internal_consumers='crates/cymule-core
crates/cymule-durable
crates/cymule-runtime
plugins/directory-store
plugins/store-sqlite'
if [ "$internal_consumers" != "$expected_internal_consumers" ]; then
  echo "cymule_core::durable_internal lexical consumer set changed" >&2
  echo "$internal_consumers" >&2
  exit 1
fi

if git grep -n -E 'package[[:space:]]*=[[:space:]]*"cymule-core"' \
  -- ':(glob)**/Cargo.toml' Cargo.toml >/dev/null; then
  echo "renaming the cymule-core Cargo dependency is forbidden while the internal bridge exists" >&2
  exit 1
fi

observed_surface=$(sed -n '/pub mod durable_internal {/,/^}/p' \
  crates/cymule-core/src/lib.rs | sha256sum | cut -d' ' -f1)
expected_surface=491a94fcd2a9e661c1c558c3c3d2eef79a3e1cdc34ad651fa61f3671f0436403
if [ "$observed_surface" != "$expected_surface" ]; then
  echo "cymule_core::durable_internal re-export surface changed" >&2
  exit 1
fi
