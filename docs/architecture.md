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

`cymule.engine/1` wraps every operation and every response. Success and failure
share stdout and a single closed envelope; a nonzero process status means the
transport could not carry that envelope. Failures retain category, processing
phase, stable code, optional contract issues, and only a recovery disposition
proved by the owning boundary. In particular, losing a process response does
not authorize replay of a Run that may already have dispatched an effect.

## IR and compiler workbench

The frozen IR is intentionally much smaller than the source language. Source
frontends may be TypeScript, Python, Rust, Go, a visual editor, or generated
code. They all emit a Plan Candidate.

`cymule.ir/2` distinguishes component `call` from reusable definition
`invoke`. An invocation resolves another definition already sealed into the
same Plan, creates a structural invocation identity, receives explicit input,
and returns a result binding without inheriting caller locals. The live
evolution registry operates before sealing: it resolves a logical reference,
resolves the complete acyclic reusable-module closure, injects every exact
revision, and creates a new parent Plan. Compatible transitive updates advance
only the future link. Runtime interpretation never follows a mutable registry
head.

Live evolution remains a control plane around the runtime rather than a second
executor. Checked migration and shadow traits are plugin seams; the controller
validates pinned descriptors, records immutable Artifact evidence, and applies
deterministic rollout gates. It returns one exact selected Plan for dispatch but
does not own a worker, Agent loop, metric backend, traffic router, or sandbox.
Automatic module relinking scans only the entry-reachable definition closure;
new component/effect/wait surfaces or changed provider-neutral requirements
retain the old future head. Durable migration revalidates a proof derived from
the current Continuation. Restart authorization returns an exact Plan for a new
Run but still leaves process or Agent-loop execution to the owning runtime.

`LiveEvolutionController` is the complete single-domain control authority. Its
one portable snapshot contains reusable definitions, reverse dependencies,
template-plus-Plan link history, each template's Plan DAG and rollout state,
and immutable occurrence pins. Compatible publication, transitive parent
relinking, DAG edges, and future decisions checkpoint through one application
journal CAS; applications do not sequence a registry write and a separate
rollout write.

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

The reference runtime accepts one explicit `ExecutionBinding`. It first checks
the live manifest against that immutable selection, then admits every Plan
requirement against the normalized provider graph. Canonical descriptor bytes
are stored as `cymule.execution-binding/1`; all execution records point to its
Artifact identity or to a deterministic operation binding derived from it.
Process-local construction and finalization remain ordinary adapter ownership,
not durable semantics.

`cymule-runtime` compiles those schemas with the maintained `jsonschema`
implementation under fixed Draft 2020-12 semantics and a resolver that cannot
load ambient files or network resources. `cymule-core` continues to own the
unchanged canonical schema bytes and Plan identity; it does not depend on the
schema compiler. Embedded execution, durable execution, CLI sealing, durable
control, and live-evolution linking all use the same runtime Plan-admission
entry point.

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

M1 defines a provider-neutral `DurableStore` as compare-and-swap over a small
head that authenticates one content-addressed checkpoint and a bounded suffix
of immutable content-addressed state deltas. A successful head transition
atomically covers the semantic Machine snapshot, Continuations, waits, leases, effect outbox,
identified signal/timer activation receipts, component occurrences, snapshot
metadata, and typed higher-profile journals. This keeps M2-M4 records under the
same revision authority without placing their domain types in `cymule-core`.

One durable domain hosts multiple Runs under that same revision authority. The
first Run creates the state; later Run creation is an append-only Machine delta
and initial Continuation committed by the same CAS. A Run ID is never reused to
reset state, and a lost creation acknowledgement is resolved by reopening the
domain rather than publishing another Run.

Clock and signal plugins do not wake processes directly. They submit a stable
`cymule.wait-activation/1` proposal naming the declared source and exact parked
waits. M1 admits the receipt, result Artifact, wait completions, and Continuation
readiness in one CAS. Stable redelivery is safe, a consume-once signal token has
at most one consuming winner, and a resumed Continuation receives a new fenced
Attempt epoch.

Every parked wait pins an owner containing definition, invocation, Region path,
site, step, and an optional result local. The owner itself is never optional.
The activation CAS writes the result Artifact into that local when present
before the Continuation becomes ready, so later expressions consume durable
frame state after reopen. Embedded execution exposes the typed boundary but
creates no Continuation.

Production HTTP and timer sources persist the exact selected targets before
delivery, so an acknowledgement lost after M1 admission cannot cause target
reselection on restart. `cymule-clock-system` separately converts OS wall-clock
observations into strictly increasing per-scope logical values for lease and
scheduling commands. The command CAS, not the clock database, remains semantic
authority.

`cymule.durable-control/1` is the common mutation/query transport for all four
SDKs. It exposes start, resume, wait admission, explicit effect release, and
read-only Run/domain queries. The Rust `DurableRuntimeControl` is the only
reducer; clients do not reconstruct Continuations or outbox transitions.

The suffix rotates into an authenticated complete projection at 32 segments,
so reopen reads at most 31 deltas. Older checkpoints and segments form a cold
archive until explicit reclamation records an immutable receipt. The repository
provides a non-blocking shared-memory reference store, an atomic local directory
adapter, and a SQLite adapter with immediate
transactions, WAL, synchronous-full persistence, and zero-timeout contention.
Adapter exclusion is never semantic authority. No database, queue, or object
store name is part of the contract.

Machine snapshot v5 can compact a causally closed canonical Event prefix into
an authenticated base projection while retaining ordered Event identities,
command identities and semantic hashes, complete command-record digests, and
every full suffix Event. Restore recomputes the prefix digest from that evidence
and the projection digest before replaying the suffix. The M1 coordinator
records cumulative compaction lineage in the same small-head CAS, so a stale
writer loses and a lost response can be recovered by reopen without recomputing
history.

## Cross-Run resources

Status: implemented.

Run state and outputs use a semantic-only `cymule.resource/2` descriptor rather than
assuming every Artifact is a small inline blob. The descriptor separates
logical shape and replay evidence from realization: inline text/JSON/bytes,
immutable objects, directory or collection manifests, and sandbox/workspace
snapshots share one contract. Separate `cymule.resource-locators/1` records route
external URLs and opaque storage references through bounded `ArtifactResolver`
reads/lists; chunked writes use `ArtifactStore`.
Concrete local, object-storage, remote-drive, WebDAV, sandbox, and HTTP
implementations remain plugins.

The design follows content-descriptor practice: media type, digest, and size
prove bytes independently of where they are found. A locator or expiring access
grant is never canonical identity, and credentials never enter durable state.
Mutable references remain usable but cannot support exact replay until pinned by
content digest or immutable version evidence.

`cymule-resource` owns this higher-profile contract; `cymule-core` remains
unchanged. Resource ID covers shape, media type, inline/content/version/live
evidence, optional content-manifest descriptor, and semantic annotations. It
deliberately excludes locator sets, signed URLs, grants, and credential
revisions. The
trusted Rust resource sealer validates and hashes candidates. TypeScript,
Python, Rust, and Go builders call that sealer through the Engine protocol.

`ResourceHandoffController` appends a typed, self-validating transfer to the
target Run's M1 application journal. The caller supplies an exact producer Run,
component occurrence and output Artifact, target Run, stable transfer ID, and
target slot. Handoffs survive reopen, retry
idempotently, and reject conflicting ID reuse without adding Resource semantics
to M1 storage.

When the consumer is already parked on a matching input wait, the controller
can activate it atomically: canonical Resource Handle bytes become an Artifact,
the transfer and activation records enter separate typed journals, the wait
completes, and its Continuation becomes ready in one M1 revision. This is a
generic input-delivery seam, not a queue or Agent message model.

Listable content uses canonical sorted JSON-lines manifests. The semantic
descriptor retains byte digest/size, entry count, and Merkle root; each bounded
page supplies contiguous per-entry inclusion paths. This borrows only the
content-descriptor principle used by OCI: media type, digest, size, immutable
content, and independent retrieval. Cymule does not import an OCI registry,
repository, tag, platform, distribution, or credential model.

`ResourceLifecycleLedger` is the provider-neutral pin/release/GC/delete receipt
authority. Store plugins own physical deletion and must verify exact absence.
Filesystem and object-store uploads likewise return verified cleanup receipts
after removing every owned staging/chunk object; a best-effort delete is not a
terminal state.

## Large virtual work

Status: implemented.

`cymule-virtual` materializes bounded pages from a provider-neutral
`RegionSource`; an opaque cursor, not an offset interpreted by Cymule, names the
next source position. `cymule.virtual-checkpoint/1` records commit that cursor
with the complete bounded scheduler frontier and an explicit checkpoint parent
through an M1 application journal. Stale CAS rolls the in-process scheduler back
to its previous snapshot, and reopen restores the last committed checkpoint.

Parked work maintains a rebuildable exact-reason index rather than scanning the
parked population. A work item blocked on an M1 wait uses the exact wait ID as
its index key. `DurableVirtualController` can therefore lower one identified
activation and the resulting M3 wake snapshot into the same M1 CAS revision.
Concrete databases, object listings, queues, clocks, and signal transports stay
behind `RegionSource` or activation plugins.

Claims are also checkpoints. Before dispatch, the scheduler pins a concrete
implementation binding and records a running `cymule.virtual-work-occurrence/1`
under the new claim epoch. Worker output enters through a closed disposition:
success, retry, park, terminal failure, or cancellation. `DurableVirtualController`
atomically checkpoints result/evidence Artifacts and the updated frontier; a
stale owner, epoch, or CAS changes neither side. Retry policy remains a caller or
policy-plugin decision and produces a new occurrence rather than rewriting the
failed attempt.
Control checkpoints retain the full resolution command and its occurrence ID,
so historical command replay reads the original receipt even after unrelated
later claims; it never restores the older scheduler snapshot.

Multi-worker capacity is represented by abstract slot leases. A claim command
supplies the worker, slot, capabilities, binding, and Clock-derived logical
lease window; `DurableVirtualController` previews the next M1 lease and commits
it with the selected work in one CAS. Different slots can claim independently,
while one slot cannot hold two active claims. An empty poll records an
idempotent receipt without acquiring a lease.

Renewal advances the slot lease epoch and the running occurrence fence in one
checkpoint. Normal result commands carry both the work epoch and current lease
epoch and must be observed before expiry. After expiry, a recovery controller
must explicitly retry, fail, or cancel under the exact durable lease; expiry
does not silently requeue work. Receipt-loss reopen tests cover claim, renewal,
and recovery, and a later worker receives a greater work epoch before execution.
The framework never models worker processes, heartbeats, queue endpoints, or an
Agent Loop.

Run selection uses integer weighted deficit accounting over exact item cost.
Within the chosen Run, priority aging derives only from persisted successful
dispatch count and ready-entry sequence. This makes fairness portable across
processes and avoids a clock or floating-point dependency. The guarantee applies
to materialized, capability-compatible backlogs. `RegionSource` visibility is a
separate deterministic round-robin layer because Cymule cannot know an item's
cost before the source returns it.
Run weight changes use the same idempotent control and M1 checkpoint path and
reset old deficit before future selection.

Region topology changes also stay behind the source boundary. A pinned
`RegionMigrator` receives exact active source snapshots and produces opaque
replacement cursors plus a coverage-evidence Artifact. The adapter verifies the
plan again at admission. One M1 checkpoint then retires sources, activates
targets, retains evidence and receipt, and leaves all already materialized work
on its historical region identity. No database partition, Kafka offset, object
prefix, or range syntax enters framework state.

Completed regions use a separate `VirtualArchive` byte seam. Cymule serializes
the exact occurrence manifest, derives its semantic Resource descriptor, and
asks the adapter to idempotently store those bytes. The adapter may realize any
immutable storage substrate, but it cannot author the
`VirtualCompactionCertificate`.
After manifest readback, one M1 journal CAS replaces hot occurrence payloads
with a bounded summary, certificate, semantic cold descriptor, and small
per-work terminal fence index. A failed CAS leaves at most an unreferenced
immutable manifest in the adapter. Cold bytes never enter the hot Machine
Artifact map.

Partial rehydration is explicit rather than an implicit cache miss. A typed
command fixes the certificate and exact occurrence IDs. The controller reads
the manifest through its pinned binding, verifies content identity and every
certificate digest, then checkpoints only those records. This keeps archive
latency and provider behavior outside scheduling authority while preserving old
control receipt and debugging paths on demand.
