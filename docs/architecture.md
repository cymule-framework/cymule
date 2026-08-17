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

## Agent interaction

`cymule-agent` keeps protocol-neutral Session projections and replaceable host
interfaces outside the semantic kernel. Durable context, model, permission,
tool, elicitation, and workspace calls pin their implementation binding before
invocation and journal typed lifecycle snapshots under the M1 CAS revision.

Cymule does not own an Agent or script loop. The caller decides how context,
models, permissions, tools, user input, and workspace changes are ordered and
when its own loop continues or terminates. For each external interaction the
caller supplies a stable occurrence ID and typed request to
`AgentInteractionController`. The controller owns only binding pinning,
`prepared -> started -> completed | unknown` persistence, retained-response
replay, and fail-closed recovery. Reopening a completed occurrence returns the
recorded typed response without binding or dispatching again; conflicting reuse
of an ID is rejected.

Human or external input uses an M1 `WaitKind::Input`, not a blocked task or
process-local channel. `AgentInputController` atomically couples the Session
projection and Continuation wait, so a crash cannot expose `RequiresAction`
without a deliverable wait or ready the Continuation without the resolved
elicitation. It compiles self-contained Draft 2020-12 schemas before suspension
and validates accepted values from the persisted request before the completion
CAS. The maintained Rust JSON Schema implementation runs with filesystem and
HTTP resolvers disabled; schema validation does not introduce an I/O or provider
boundary. Concrete UI, transport, and identity integrations remain adapters.

An ambiguous host call is recovered by querying its original pinned binding.
The query may return the original typed response, prove `not_applied`, or remain
`unknown`; it never dispatches a replacement request. A call that never left
`prepared` may be cancelled only with explicit non-dispatch evidence. Once a
recovery records the original typed response, the caller can reopen the
interaction controller and consume that response without redispatch. A
`not_applied` result requires the caller to admit a separately identified
replacement or terminate its own loop.

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
