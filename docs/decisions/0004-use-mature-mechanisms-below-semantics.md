# ADR 0004: Use Mature Mechanisms Below Semantics

Status: accepted on 2026-08-17.

## Decision

Cymule implements only the semantic behavior that distinguishes the framework.
For general mechanisms, prefer maintained libraries behind explicit interfaces:

- Tokio for async tasks, channels, timers, cancellation, and process I/O;
- Serde and JSON Schema implementations for encoding and structural schemas;
- maintained URL and base64 libraries for credential-safe Resource validation
  and inline byte decoding;
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

Plan admission applies this decision with the maintained Rust `jsonschema`
compiler inside `cymule-core`. This narrow dependency is required because
Machine insertion and snapshot restore must reject an invalid executable schema
under the same authority that computes Plan identity. Draft 2020-12 is selected
explicitly and external retrieval is denied, so compilation stays pure and
deterministic. Runtime compilation only materializes value validators; it is not
a second sealer.

The M2 input controller uses the same maintained compiler for its separately
owned typed-input contract. Default filesystem/HTTP resolution is disabled, so
schema enforcement remains deterministic and local to the submitted contract.
The current resolver library uses a per-registry read/write lock for its
internal reference cache. Cymule does not share that registry across turns and
does not use the lock for Session, wait, CAS, dispatch, or recovery authority;
accepted schemas are recompiled at completion instead of introducing a shared
framework cache. This bounded library mechanism is not a coordination primitive
in Cymule's execution model.

The Resource profile likewise uses `url` for WHATWG URL parsing and `base64`
for RFC 4648 decoding outside `cymule-core`. Concrete multi-provider storage
plugins should evaluate Apache OpenDAL before implementing service protocols
from scratch; OpenDAL configuration and credentials remain inside the plugin,
never in a Resource Handle or canonical state.

## Lock policy

Locks are never semantic authority. Prefer immutable records, optimistic CAS,
idempotency, fencing epochs, partitions, and single-writer ownership. If a
concrete adapter needs local writer exclusion to implement CAS, acquisition must
be non-blocking and contention must surface to normal retry/backoff policy.
