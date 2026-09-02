# ADR 0006: Compiler-Enforced Rust API Boundaries

Status: accepted on 2026-09-02; physical crate migration is proposed and the
current `durable_internal` bridge remains an explicitly tracked exception.

## Problem

Rust has crate visibility, not workspace visibility. Consequently,
`#[doc(hidden)] pub` is still callable by every downstream crate and is not an
encapsulation boundary. The current `cymule_core::durable_internal` module is a
real public compatibility surface. Durable is the dominant consumer, but the
current production Runtime also consumes it and Core plus official Store
plugins contain direct test consumers. Those are tracked migration debt, not a
declared architecture boundary.

Three contracts are currently mixed:

1. stable semantic API used by applications and SDK facades;
2. provider SPI implemented by Store and execution adapters; and
3. sibling implementation protocol used to couple Core and Durable reducers.

## Decision

The target package graph has three compiler-visible layers:

- stable facade crates expose only application-level semantic types and typed
  controls;
- narrow SPI crates expose provider contracts whose compatibility is promised;
- exact-version internal implementation crates carry reducer deltas, StateRoot
  layouts and coordinator coupling. They are public in the Rust language sense
  only because Cargo packages need them, are not re-exported by the facade, and
  use Cargo requirements of the form `=x.y.z` in addition to exact versions in
  one release BOM. A lockfile or ordinary `"0.2.0"` caret requirement is not an
  exact downstream library constraint.

The physical migration proceeds by moving the pinned Machine reducer and its
DTOs from `cymule-core` into an internal implementation crate consumed by Core
and Durable. `cymule-core` then re-exports only stable semantic admission and
replay contracts. StateRoot and coordinator implementation DTOs similarly move
behind a Durable-internal crate; `DurableStore` is reduced to immutable object
read/write plus small-head compare-and-swap primitives, while typed persistent
map/log lowering stays framework-owned.

Until that move is complete, new consumers of `durable_internal` are forbidden.
The existing bridge is not described as private, stable or semver-safe.
Documentation hiding, naming conventions and sealed traits are not substitutes
for the package split.

## Required completion gates

Except for the linked lexical smoke gate, these gates are not implemented yet.
C-03 remains open until all of them are linked to mandatory CI evidence:

- stable facade and provider SPI public-API snapshots are reviewed on every
  change;
- `cargo-semver-checks` runs against the last released facade/SPI versions;
- the release BOM pins internal crates exactly and rejects mixed versions;
- a Cargo-metadata dependency-graph check permits internal crates only from an
  allowlist;
- compile-fail fixtures prove a consumer depending only on a stable facade
  cannot name reducer deltas, StateRoot mutation DTOs or raw coordinator
  transactions through that facade.

The current
[`verify-rust-internal-api.sh`](../../scripts/verify-rust-internal-api.sh) is
only a temporary lexical smoke gate. It is conservatively routed for every Rust
source or manifest change and freezes the known spelling of direct consumers
and the current re-export text. Crate aliases, glob re-exports and Cargo package
identity are not soundly modeled by it; it is neither the planned metadata
allowlist nor an API snapshot.

Cargo's dependency and publishing model makes a published internal package
technically discoverable; exact versioning and graph policy manage that
distribution fact, while facade non-re-export and separate packages provide the
compiler boundary. Rust visibility alone cannot provide workspace privacy.
An application can still explicitly depend on a published internal crate; the
compiler boundary promised here is that stable facade dependencies do not
expose it, while package metadata and support policy make such a direct
dependency unsupported rather than impossible.

References:

- <https://doc.rust-lang.org/reference/visibility-and-privacy.html>
- <https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html>
- <https://doc.rust-lang.org/cargo/reference/publishing.html>
- <https://doc.rust-lang.org/cargo/reference/semver.html>
- <https://rust-lang.github.io/api-guidelines/future-proofing.html>

## Consequences

- C-03 is not closed by this ADR alone. It is closed only after the physical
  split, dependency allowlist, API snapshots and semver gates exist.
- The migration may be incremental, but no intermediate state may claim that
  `#[doc(hidden)] pub` is private.
- Store plugins implement storage mechanics rather than semantic tree layout or
  reducer policy.
