# ADR 0004: Use Mature Mechanisms Below Semantics

Status: accepted on 2026-08-17.

## Decision

Cymule implements only the semantic behavior that distinguishes the framework.
For general mechanisms, prefer maintained libraries behind explicit interfaces:

- Tokio for async tasks, channels, timers, cancellation, and process I/O;
- Serde and JSON Schema implementations for encoding and structural schemas;
- reviewed file-lock or atomic-write libraries inside concrete adapters;
- established protocol SDKs inside ACP, MCP, and A2A adapters;
- `git-filter-repo` for deterministic public-history rewriting;
- mature property/model-testing libraries for generated fault traces.

The dependency remains outside `cymule-core` unless the semantic kernel itself
cannot be correct without it. Runtime and plugin crates may adopt dependencies
according to their own trust and portability boundary.

## Build versus adopt test

Build a Cymule implementation only when at least one is true:

1. the behavior defines canonical identity or transition meaning;
2. existing libraries cannot preserve fencing, replay, or effect uncertainty;
3. the abstraction must stay provider-neutral across incompatible products;
4. a small deterministic reference implementation is required for conformance.

Otherwise adopt a maintained mechanism and wrap it with a Cymule interface.

## Lock policy

Locks are never semantic authority. Prefer immutable records, optimistic CAS,
idempotency, fencing epochs, partitions, and single-writer ownership. If a
concrete adapter needs local writer exclusion to implement CAS, acquisition must
be non-blocking and contention must surface to normal retry/backoff policy.
