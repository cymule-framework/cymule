# Durable Protocol Authority

## Ownership

- This crate is the sole owner of provider-neutral M1 values shared below
  Durable and all higher profiles: logical Clock observations, execution
  claims, Continuations and frames, wait ownership, identified wait
  activations, their receipts, identities, versions, limits, and pure
  verification.
- `cymule-profile-protocol`, `cymule-durable`, profile crates, plugins, SDKs,
  and hosts import these public contracts directly. They must not re-export a
  canonical alias, copy the DTO graph, or fork identity and verification code.
- The dependency direction is `core <- durable-protocol <-
  profile-protocol/durable`. This crate must never depend on Runtime, Durable,
  a profile crate, a Store, a provider, a plugin host, an async runtime, or I/O.

## Closed protocol rules

- Public DTOs use closed enums, explicit version fields, deterministic ordered
  collections, required-nullable members, and `deny_unknown_fields` wherever
  accepting an unknown field would reinterpret durable authority.
- Content identities bind their complete semantic preimage through Core's
  canonical identity helpers. Do not duplicate hash framing or accept a
  shape-valid digest without recomputing it.
- Pure verification receives every authority input explicitly. It performs no
  clock read, Store lookup, provider call, runtime composition, retry,
  fallback, mutation, or process-local observation.
- Core failures cross this boundary through a closed lossless category map.
  Preserve validation, not-found, illegal-transition, command-reuse conflict,
  causal and pinned-read integrity, identity mismatch, exact archived-command
  replay fields, and encoding separately. Never classify through `Display`,
  parse a message prefix, or collapse an unknown Core variant into validation.
- Preserve `PagedScopeRequired` with its exact Run, Scope and cardinality. It
  requests another admitted preparation path, not an unchanged-request retry.
- Preserve collection provider failures as the canonical collections
  `ProviderFailure` payload, including revision/history conflict evidence and
  provider integrity/substrate codes. This pure DTO dependency adds no resolver
  or I/O capability; never turn provider failure into proof corruption.
- Logical counters and all `usize` wire positions remain within the exact
  cross-language integer range. Fixed count and byte bounds are protocol
  authority, not adapter tuning knobs.
- Untrusted Continuation JSON first uses its fixed raw wire-byte bound and
  Core's strict duplicate-member/number-lexeme decoder. Constructed
  Continuations additionally fit the same compact JSON byte bound. This is a
  transport resource limit, not a second JCS identity or canonicalization
  algorithm.
- A protocol shape, identity preimage, version, or semantic validation change
  is a hard version-domain change. Update schemas, SDKs, fixtures, release
  catalog, and positive/negative tests in the same cut; do not retain aliases
  for the former owner path.

## Verification

- Test exact identity derivation, tampering, required-nullable presence,
  execution claim/Continuation correlation, wait source cardinality, receipt
  subset rules, counter boundaries, and unknown-field rejection.
- Run this crate's all-target tests and Clippy with warnings denied, then the
  direct Profile and Durable consumers and package verification.
