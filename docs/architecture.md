# Architecture

Status: implemented unless marked otherwise.

## Trust boundary

The trusted computing base is `cymule-core`. It contains only semantic data,
canonical identity, validation, admission, deterministic reduction, and replay.
It performs no network, filesystem, clock, random, model, tool, or provider I/O.

```text
Language SDKs / MLIR workbench
              |
              v
        Plan candidates
              |
              v
  Rust sealer + semantic kernel <---- commands
     |          |          |
   plans      events    artifacts
              |
              v
      rebuildable projection
              |
              v
     runtime plugin interfaces
```

`cymule-runtime` interprets the frozen IR and connects abstract operations to a
`PluginHost`. The provided process host is one realization. A durable runtime
may replace it without changing core semantics.

## Why the core is Rust

Rust gives the kernel explicit ownership, closed enums, exhaustive transitions,
and a small dependency surface without requiring a managed runtime. The kernel
is a normal library rather than a service, so it can be embedded in test tools,
single-process runtimes, or future durable control planes.

The language SDKs do not use FFI and do not duplicate the reducer. They exchange
versioned canonical JSON through an `Engine` interface. The supplied CLI engine
uses stdin/stdout, making the boundary usable in local tools and conformance
tests without choosing an RPC stack.

## IR and compiler workbench

The frozen IR is intentionally much smaller than the source language. Source
frontends may be TypeScript, Python, Rust, Go, a visual editor, or generated
code. They all emit a Plan Candidate.

MLIR is optional and remains outside the kernel. The partial workbench currently
syntax-checks an experimental generic-operation form and documents its mapping
to the Plan Candidate schema. A registered dialect, structural verifiers, and
deterministic lowering remain proposed. LLVM/MLIR libraries are never a runtime
dependency.

## Plugin model

Plugins advertise abstract component and effect implementations. A manifest
includes stable implementation revisions, reconciliation capability, and a
stable implementation ID. Plan contracts own schemas and effect properties.
Registration never grants authority. Admission independently checks the plan,
binding, policy, and authority before dispatch.

The reference process protocol is request/response JSON over stdin/stdout. It is
designed for testability, not as the only production transport. Future WIT and
network transports can implement the same `PluginHost` trait.

## Optional integration plugins

Cymule does not define a Session, Agent Loop, message or tool lifecycle,
transport stream, or Agent-host occurrence. Those are integration-domain
objects, not semantic-kernel concepts.

The separately owned [`plugins/agent-interaction`](../plugins/agent-interaction)
package is one optional integration. It lowers Agent-domain projections,
controllers, and occurrences onto generic M1 journals, waits, effects, scopes,
resources, bindings, and CAS checkpoints. Its types are not re-exported by the
framework CLI or language SDKs, and its schema and conformance suite evolve in
the plugin's own version domain.

ACP, MCP, A2A, editor, model-provider, and concrete Agent Loop support belongs
in additional adapters or plugins above that package. The same core interfaces
can support unrelated integration domains without acquiring Agent-specific
semantics.

## Storage contracts

M1 defines a provider-neutral `DurableStore` as compare-and-swap over one
complete `DurableState` revision. A successful write atomically covers the
semantic Machine snapshot, Continuations, waits, leases, effect outbox,
component occurrences, snapshot metadata, and typed higher-profile journals.
This keeps M2-M4 records under the same revision authority without placing
their domain types in `cymule-core`.

The repository provides a non-blocking shared-memory reference store and an
atomic local directory adapter. Both surface writer contention as a conflict;
neither a mutex nor a file lock is semantic authority. Production adapters use
their substrate's native CAS and remain plugins. No database, queue, or object
store name is part of the contract.

## Cross-Run resources

Status: implemented foundation.

Run state and outputs can use a versioned resource descriptor rather than
assuming every Artifact is a small inline blob. The descriptor separates
logical shape and replay evidence from realization: inline text/JSON/bytes,
immutable objects, directory or collection manifests, and sandbox/workspace
snapshots share one contract. External URLs and storage locators are resolved by
bounded `ArtifactResolver` reads/lists; chunked writes use `ArtifactStore`.
Concrete local, object-storage, remote-drive, WebDAV, sandbox, and HTTP
implementations remain plugins.

The design follows content-descriptor practice: media type, digest, and size
prove bytes independently of where they are found. A locator or expiring access
grant is never canonical identity, and credentials never enter durable state.
Mutable references remain usable but cannot support exact replay until pinned by
content digest or immutable version evidence.

`cymule-resource` owns this higher-profile contract; `cymule-core` remains
unchanged. Resource ID covers shape, media type, inline/content/version/live
evidence, and semantic annotations but deliberately excludes locations. The
trusted Rust resource sealer validates and hashes candidates. TypeScript,
Python, Rust, and Go builders call that sealer through the Engine protocol.

`ResourceHandoffController` appends a typed, self-validating transfer to the
target Run's M1 application journal. The caller supplies source Run, target Run,
stable transfer ID, and target slot. Handoffs survive reopen, retry
idempotently, and reject conflicting ID reuse without adding Resource semantics
to M1 storage.
