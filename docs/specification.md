# Cymule Semantic Specification

Status: implemented for the explicitly bounded `cymule.semantic/1` Embedded M0
subset; terminal durable-runtime requirements are marked proposed below.

## 1. Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative terms. A realization may
claim only the profiles whose complete required conformance cases pass.

## 2. Public model

The default public model is:

```text
Flow -> Run -> Result

Inside a Flow:  call | wait | effect | scope
Outside a Run:  observe | decide | change
```

A long-lived Run may emit typed outputs and observations without reaching a
terminal Result. The Run is the default live handle. Graphs are views.

## 3. Version domains

The following domains evolve independently:

| Domain | Current version | Compatibility rule |
| --- | --- | --- |
| Semantic specification | `cymule.semantic/1` | transition meaning is frozen |
| Canonical IR | `cymule.ir/1` | unknown operations are rejected |
| Canonical encoding | `cymule.jcs/1` | RFC 8785 JSON, SHA-256 IDs |
| Event schema | `cymule.event/1` | readers reject unknown semantic events |
| Command protocol | `cymule.command/1` | typed envelope and stable error codes |
| Plugin protocol | `cymule.plugin/1` | capability negotiation is explicit |
| Resource descriptor | `cymule.resource/1` | identity excludes realization locations |
| Resource handoff | `cymule.resource-handoff/1` | transfer IDs are idempotent per target Run |
| Wait activation | `cymule.wait-activation/1` | external delivery ID fixes source, targets, and result |
| Virtual checkpoint | `cymule.virtual-checkpoint/1` | cursor and bounded frontier advance together |
| Virtual work occurrence | `cymule.virtual-work-occurrence/1` | one immutable binding per claim epoch |
| Virtual work control | `cymule.virtual-work-control/1` | stable command ID plus owner/work/lease/time precondition |
| Virtual region migration | `cymule.virtual-region-migration/1` | opaque cursor coverage and retirement lineage |
| Virtual migration control | `cymule.virtual-region-migration-control/1` | stable command ID plus verified plan |
| Virtual archive manifest | `cymule.virtual-archive-manifest/1` | exact content-addressed occurrence history |
| Virtual compaction certificate | `cymule.virtual-compaction-certificate/1` | causal cut, summary, retention, and rehydration evidence |
| Virtual compaction control | `cymule.virtual-compaction-control/1` | stable command ID plus pinned archive binding |
| Virtual rehydration control | `cymule.virtual-rehydration-control/1` | stable command ID plus exact occurrence selection |
| Virtual claim control | `cymule.virtual-claim-control/1` | stable command ID plus capacity-slot lease proposal |
| Virtual lease renewal control | `cymule.virtual-lease-renewal-control/1` | exact work and slot lease fences |
| Virtual recovery control | `cymule.virtual-recovery-control/1` | expired lease plus explicit retry/fail/cancel decision |
| Virtual Run weight control | `cymule.virtual-run-weight-control/1` | stable command ID plus positive future share |
| Conformance profile | `cymule.conformance/1` | complete profile cases are required |

Changing effect identity, scope isolation, authority, causal admission, replay,
or migration meaning requires a new semantic specification version.

## 4. Canonical stores

Cymule has exactly three canonical stores:

1. **Plan store**: immutable content-addressed semantic plans.
2. **Event store**: admitted causal transitions with explicit parents.
3. **Artifact store**: immutable typed bytes, receipts, state, context, evidence,
   checkpoints, and occurrence bindings.

Run state, ready work, graphs, attention, effect summaries, and indexes are
deterministic projections and MUST be rebuildable.

## 5. Canonical identity

Canonical objects are encoded with RFC 8785 JSON Canonicalization Scheme and
identified as `sha256:<lowercase hex>`. The object identifier is not part of the
hashed preimage. Duplicate JSON property names and non-finite numbers are
invalid. A writer MUST validate the semantic schema before hashing.

`PlanId` identifies a sealed plan. `EventId` identifies the event payload and
causal parents. `ArtifactId` identifies artifact type metadata and bytes.

### 5.1 Cross-Run resource values

M1 Artifact exchange includes a versioned provider-neutral Resource descriptor.
A Resource has a logical shape (`inline`, `object`, `collection`, `directory`,
or `snapshot`), media type, replay evidence, semantic annotations, and optional
realization locations. Inline text/JSON/bytes and external content with verified
SHA-256/size provide location-independent exact evidence; availability still
requires retained inline bytes or a usable location. An immutable provider version
requires the original resolver binding for exact retrieval. A `live` Resource
is useful state but is never exact replay evidence.

Resolver locators and access grants are realization data, not Plan semantics.
Credentials MUST NOT enter a descriptor, Artifact, Event, Continuation, or IR.
A public URL MUST be credential-free HTTP(S) without userinfo, query, or
fragment. Private object, drive, sandbox, and signed-URL access uses an opaque
resolver binding/reference whose reference is non-secret. Locations do not
participate in Resource ID; moving identical content MUST preserve identity.

Read operations MUST be bounded chunks and directory/collection/snapshot lists
MUST be bounded opaque-cursor pages. A resolver response that exceeds the
requested bound, repeats an unsafe entry, stalls with an empty non-terminal
chunk, or fails content verification MUST be rejected. Chunked stores use
idempotent write IDs, exact offsets, explicit commit, and explicit abort.

A Run-to-Run handoff carries one verified Resource Handle under a stable caller
transfer ID and target slot. The handoff MUST be recorded in the target Run's
M1 application journal by whole-state CAS. Repeating identical semantics is
idempotent; reusing a transfer ID with different semantics MUST fail. One target
slot has at most one handoff; multiple values use one collection Resource.

## 6. Frozen IR

`cymule.ir/1` contains:

- named component and effect contracts;
- structured definitions composed from `call`, `wait`, `effect`, and `scope`;
- literal, input, binding, object, and array expressions;
- explicit input/output JSON Schemas;
- provider-neutral effect and execution properties.

The IR MUST NOT contain provider endpoints, credentials, queue names, database
products, worker addresses, or deployment topology. Frontends are proposal
producers. The trusted Rust sealer validates and computes the Plan ID.

## 7. Versioned effectful continuation

A terminal durable-profile continuation exists only at a semantic safe point
and contains:

```text
plan ID | future binding context | frame | typed state | wait set | scope
effect obligations | authority leases | budget | causal cut | epoch
```

Process memory and host-language stacks are not canonical. An Attempt pins an
immutable occurrence binding and the continuation epoch. Output from a stale
attempt MUST be rejected.

The M0 kernel implements Run, Attempt, epoch, scope, effect obligation, and
binding projections. M1 now defines and persists the complete first-class
Continuation field set through a provider-neutral CAS store. Automatic capture
and resumable interpretation at every safe point remain partial and are not
claimed by the Embedded profile.

An integration plugin MAY atomically checkpoint its own typed projection with
a Continuation, wait, outbox entry, or effect transition through the M1
application-journal boundary. The plugin owns its domain schema and transition
rules. Framework conformance requires only that the shared CAS write is
all-or-nothing, stale writers fail closed, and plugin records cannot widen
authority or bypass Plan-declared effects.

A signal or timer wait MUST complete through an identified
`cymule.wait-activation/1` record. The activation fixes one external delivery
ID, its declared signal key or timer ID, exact selected wait IDs, and one
immutable result Artifact. The activation receipt, completed waits, result
Artifact, and affected Continuation readiness MUST enter one M1 CAS revision.
Activation admission MAY add its result Artifact but MUST reject a proposed
Machine snapshot containing any other change. Direct uncorrelated completion of
signal and timer waits is invalid.

Repeating an identical activation ID is idempotent. Reusing it with a different
source, target set, or result MUST fail. One signal activation MAY wake multiple
non-consuming waits but MUST consume at most one wait whose signal policy is
consume-once. One timer activation MUST target exactly one matching timer wait.
Selection and eventual delivery belong to scheduler, signal, and clock
substrates; those substrates propose activations and never mutate canonical
state directly.

When an activation or other wait completion makes a Continuation `Ready`, a
resume after any process boundary MUST advance its epoch and commit a new fenced
Attempt before interpretation. The yielded Attempt that parked the wait MUST
NOT be reused.

M3 virtual materialization MUST checkpoint each source-owned successor cursor
with the complete bounded ready, active, and parked frontier that it produced.
The checkpoint is a typed M1 application-journal record with a stable ID and an
explicit parent checkpoint. Reusing the ID with different state MUST fail. A
failed or stale CAS MUST leave the in-process scheduler at its prior snapshot;
after an unknown acknowledgement the caller reopens and reads the durable
checkpoint before retrying the same immutable source cursor.
For one cursor, a source MUST return a deterministic bounded page and successor.
An undeclared cursor-version change, non-terminal stalled cursor, empty or
repeated work identity, oversized page, or partial source failure MUST leave the
entire scheduler snapshot unchanged.

Parked work MUST have a rebuildable exact-reason index. Waking one reason MUST
not require scanning unrelated parked work. Work parked on an M1 wait uses the
exact wait ID as its reason key. When an identified wait activation wakes M3
work, the activation receipt, M1 wait and Continuation updates, and M3 scheduler
checkpoint MUST be admitted by one CAS or not at all.
An activation already committed without that M3 checkpoint MUST NOT later be
relabeled as an atomic cross-profile transition; a retry succeeds only when the
exact checkpoint record was committed with the activation.

Every M3 work claim MUST resolve an immutable occurrence binding and create one
`cymule.virtual-work-occurrence/1` record before worker execution. Occurrence
identity is derived from logical work ID and monotonically increasing claim
epoch. Owner, binding, region, Run, and current capacity-slot lease epoch MUST
be retained. A stale owner, work epoch, or lease epoch MUST NOT resolve the
occurrence.

Durable multi-worker admission uses `cymule.virtual-claim-control/1`. A command
fixes a stable worker identity, abstract capacity-slot ID, occurrence binding,
capability set, Clock-supplied logical time, and positive lease TTL. A slot is a
capacity/fencing token only; it MUST NOT encode a queue provider, network
address, process, container, cluster node, or Agent Loop. One slot may own at
most one active claim, while different slots may claim independently.

The exact next M1 `AuthorityLease` and the M3 claim receipt MUST enter one CAS
revision. A failed or stale CAS changes neither. If no work is eligible, the
command MUST checkpoint a replayable empty receipt and MUST NOT acquire the
slot lease. Repeating an admitted command returns its original claimed item or
empty receipt even after unrelated scheduler progress.

`cymule.virtual-lease-renewal-control/1` fixes work ID, owner, work epoch,
expected current lease epoch, logical time, and TTL. Renewal atomically advances
the M1 lease epoch and the active claim and occurrence lease fence. It does not
create a new work attempt or change the occurrence binding. Receipt loss is
resolved by reopening and replaying the same command.

One running occurrence may end exactly once as `succeeded`, `retry_scheduled`,
`parked`, `failed`, or `cancelled`. Success stores one result Artifact. Retry and
terminal failure store failure evidence; cancellation stores its reason.
Parking stores an exact indexed condition. Repeating the same disposition is
idempotent; a different disposition for that occurrence MUST fail.

Retry does not mutate or erase the failed occurrence. It requeues or parks the
same logical work, and the next claim creates a new epoch and may pin a new
binding. Cancellation removes the active claim, so later worker output is stale
and rejected. Retryability, limits, delay, and escalation are explicit policy
decisions and MUST NOT be inferred from provider strings or transport status.

Claim and disposition transitions MUST enter chained M1 virtual checkpoints.
Result, failure, and cancellation Artifacts MUST commit with the occurrence and
frontier in the same CAS; the Machine proposal may add only those exact
Artifacts. Public control uses `cymule.virtual-work-control/1` with a stable
command ID and exact work, owner, work epoch, lease epoch, and Clock-supplied
observation-time precondition. A normal worker result MUST be observed strictly
before the current lease expiry.
The checkpoint MUST retain the complete command and returned occurrence ID.
Repeating that command after any number of later checkpoints MUST return the
original occurrence receipt without reverting scheduler state. Reusing its ID
with different semantics MUST fail.

Lease expiry is not an automatic state mutation. After expiry,
`cymule.virtual-recovery-control/1` must name the exact work, owner, work epoch,
lease epoch, logical observation time, and an explicit `retry`, terminal
`failed`, or `cancelled` disposition. The durable M1 lease must still equal that
expired fence. Recovery evidence Artifact and scheduler disposition commit in
one CAS. A concurrent renewal, worker result, or recovery has one winner; stale
proposals change nothing. Retry returns the logical item to ready or parked
state, and its next claim creates a greater work epoch, so output from the
failed worker remains fenced.

For continuously backlogged, materialized, capability-compatible Runs, M3
weighted selection uses positive integer Run weights and exact positive
`WorkItem.cost`. A scheduling round grants `base_quantum * weight` deficit and a
claim debits its cost. Implementations MUST use integer, snapshot-persisted
accounting and deterministic Run order; floating point, wall time, worker
latency, and queue-provider order are not scheduling authority.
A Run weight change resets that Run's accumulated deficit before future
selection so credit earned under an older policy cannot leak into the new share.
Public weight changes use `cymule.virtual-run-weight-control/1`, retain previous
and current positive weights, and checkpoint an idempotent receipt. The command
affects future selection only and does not rewrite historical claims.

Priority is local to one Run. Effective priority is base priority plus
`floor((dispatch_sequence - ready_since) / aging_interval)`. Both sequences and
the positive aging interval MUST be durable. Stable ready order breaks equal
scores. Therefore a continuously eligible old item eventually outranks a
continuous stream with any fixed finite higher base priority.

Weighted dispatch does not claim knowledge of work that has not been
materialized. Bounded materialization MUST separately rotate across registered
non-exhausted regions so every source remains visible even with one frontier
slot. Once work is visible, weight/cost selection owns dispatch fairness.
Policy, Run weights, deficits, dispatch sequence, ready age, and last Run/region
selection MUST survive checkpoint restore and produce the same next claim.

M3 MUST treat every region cursor as opaque. Split and merge are planned and
verified by a replaceable `RegionMigrator` pinned through an immutable migration
binding. A split has exactly one active source and at least two targets; a merge
has at least two active sources and exactly one target. All sources and targets
MUST belong to one Run and abstract source operation.

A migration plan fixes its stable ID, kind, exact source cursor map, replacement
regions, migration binding, and immutable coverage-evidence Artifact. Before
admission, the pinned adapter MUST verify that evidence proves complete,
non-overlapping coverage of remaining source work. Framework shape validation
or possession of an evidence reference alone is not verification.

Admission MUST compare every source cursor with the current checkpoint, reject
retired sources and existing/duplicate target IDs, retain the coverage Artifact,
and atomically record source retirement, target activation, migration receipt,
and scheduler checkpoint. A stale cursor, failed adapter verification, evidence
failure, target conflict, or stale CAS MUST retire nothing.

Retirement is not deletion. Existing ready, active, parked, known, and
occurrence records keep their source region IDs and remain valid; retired regions
remain in lineage but cannot materialize new work. Replacement targets own only
future materialization. A stable migration command replay MUST return its
original receipt after later checkpoints, while semantic ID reuse fails.

A virtual region MAY move exact occurrence history to cold storage only after
it is exhausted or retired, has no ready, active, or parked work, and the
greatest occurrence for every represented logical work is `succeeded`, `failed`,
or `cancelled`. `VirtualArchive` is an immutable byte interface pinned by a
compactor binding. It MUST NOT choose certificate fields, interpret occurrence
meaning, or place a provider locator or credential in semantic state.

The Rust controller MUST canonically encode `cymule.virtual-archive-manifest/1`
and compute its ordinary Artifact ID before calling the archive. The manifest
contains the exact occurrence records, final work/epoch index, region/Run, and a
non-empty causal checkpoint cut. A returned success followed by a failed M1 CAS
MAY leave that immutable object unreferenced; scheduler state, Machine state,
certificate, and checkpoint MUST remain unchanged.
For the M1 linear virtual journal, a new durable compaction cut MUST include the
current checkpoint head; an old command replays from its retained receipt before
this future-head check.

`cymule.virtual-compaction-certificate/1` MUST authenticate the causal cut,
bounded completion summary, complete manifest digest and Artifact, terminal
work/debug index digest, retained occurrence bindings, unresolved obligations,
replay availability, and pinned compactor revision. The current M3 compactor
admits only a completed subtree with no unresolved obligations and `exact`
replay through the retained manifest. It removes occurrence payloads from the
hot snapshot but retains logical work identity, greatest epoch, terminal state,
region/Run, and certificate identity so duplicate work and fencing checks remain
available without loading cold bytes. The summary is a projection, never new
canonical truth.

`cymule.virtual-rehydration-control/1` selects a non-empty exact set of
occurrence IDs from one certificate. Before inserting any record, the framework
MUST reload and verify immutable bytes, Artifact identity, manifest/schema
version, manifest digest, causal cut, summary, final work index, region/Run,
retained bindings, and certificate binding. Missing, extra, corrupted, or
conflicting occurrence data restores nothing. Compaction and rehydration
commands are idempotent M1 checkpoints; semantic command-ID reuse and stale CAS
fail without partial scheduler or Machine mutation.

## 8. Causal events

A causal cut is a causally closed down-set of admitted events. Implementations
may represent it with maximal frontier IDs. An event carries logical read/write
footprints and an optional coordination key. Independent events MUST commute.

Non-monotone decisions, including consume-once delivery, scope decisions,
effect release, budget reservation, authority widening, and default version
advancement, MUST be coordinated within a declared semantic domain.

## 9. Commands

Canonical mutation enters through a typed command envelope:

```text
command ID | actor | target | expected precondition | semantic payload
```

The same command ID and semantic payload MUST return the original receipt. The
same ID with different semantics MUST fail. A stale precondition MUST return a
typed conflict and current precondition token. Public callers cannot append raw
events.

## 10. Scopes and obligations

Embedded M0 scope state transitions are:

```text
open -> closed-committed
open -> closed-aborted
```

Commit atomically accepts declared state/evidence and transfers outstanding
world-effect requirements into an `EffectObligationSet`. Scope closure does not
claim that a provider action is applied. Run completion policy decides whether
unresolved obligations block the Result.

A plugin-mediated resource or workspace mutation MUST still enter through a
Plan-declared Effect. A domain controller cannot treat an external commit as an
internal state update, close a scope around an ambiguous result, or bypass the
effect obligation and reconciliation rules below. Domain-specific overlays,
sessions, receipts, and controllers are outside this specification.

## 11. Effects

Effect identity is structural:

```text
run | invocation | stable site | scope epoch | occurrence key
normalized arguments | effect schema version
```

Phase, world outcome, and reconciliation are orthogonal:

```text
phase: admitted -> prepared -> release-authorized -> dispatch-started
outcome: unobserved | applied | not-applied | unknown
reconciliation: not-required | pending | resolved | governance-required
```

Dispatch policy is admission authority, not adapter preference. `eager` is
legal only for observational effects and may claim while the scope is open.
`on_scope_commit` remains pending until its owning scope is committed.
`explicit` remains prepared after commit until the caller releases that exact
intent; repeating release after receipt loss MUST converge on the recorded
claim, reconciliation, settlement, or completed Result.

After dispatch ambiguity, the original intent becomes `unknown`. It MUST keep
its original occurrence binding and reconciler. It MUST NOT become a fresh
intent. An `unknown` outbox entry remains eligible for reconciliation under its
original claim across any number of process reopens. `still_unknown` MUST NOT
redispatch it, and a later applied or not-applied observation MUST be admissible
without changing identity. Compensation is a separately admitted effect.

For the durable single-domain profile, each outbox stage MUST validate an exact
Machine delta. Enqueue admits only the matching `EffectProposed`, `Prepare`, and
optional same-scope `ScopeCommitted` Events plus the input Artifact; claim
admits only `AuthorizeRelease` and
`StartDispatch`; settlement admits one matching observation or reconciliation
Event plus its declared result Artifact. Plans, existing command receipts,
unrelated Artifacts, and unrelated Events MUST remain byte-identical.
`Unknown` observation and the outbox `unknown` state MUST share one CAS.

`PrepareEffect` response loss may repeat preparation only with the same
structural intent ID, immutable binding, and input. Adapters MUST make this
operation idempotent. A committed `DispatchStarted` with no authoritative
outcome enters reconciliation after reopen and MUST NOT redispatch.

The M1 resumable interpreter persists nested frames as index-only paths into the
sealed Plan plus a matching scope stack. A restart MUST resolve the path from
the immutable Plan, MUST NOT serialize a host-language call stack, and MUST NOT
dispatch a child commit-gated effect while its child scope remains open.

## 12. Binding evolution

A Plan changes semantic meaning. A Binding Context changes realization defaults
for future occurrences. Every persisted occurrence must pin an immutable
binding at admission. Embedded M0 persists this for Attempts and Effect Intents,
including reconciliation. M1 defines canonical component occurrence records.
Optional plugins MAY define additional domain occurrences, but they MUST
preserve the same immutable binding rule whenever replay or reconciliation
depends on a selected implementation. Session, stream, and domain-controller
semantics remain in the owning plugin rather than this framework specification.

Changing a default MUST NOT rewrite an admitted occurrence. If its original
binding is unavailable, the occurrence enters an explicit unavailable or
governance path.

## 13. Replay

- **Exact state replay** reduces recorded events and artifacts without external
  I/O.
- **Exact execution replay** additionally requires every nondeterministic call
  result and occurrence binding to have been recorded.
- **Resume** replays the prefix, then admits new work beyond the frontier.
- **Fork** creates a new lineage from a selected cut.
- **Regeneration** performs new nondeterministic work and is not replay.

Replay availability is `exact`, `projection-only`, or `unavailable` relative to
an explicit required-artifact set. A missing artifact, schema, interpreter,
binding, or required authority MUST downgrade the claim. The runtime MUST NOT
silently regenerate missing data. M0 verifies exact canonical state replay; its
one-shot component calls are not an exact execution-replay implementation.

## 14. Implemented profile boundary

The Embedded profile implements canonicalization, sealing, in-memory stores,
causal replay, command idempotency and stale-action rejection, attempt fencing,
scope/obligation semantics, effect lifecycle, process plugins, and all four SDK
chains. `wait` is an authoring and suspension boundary but has no durable resume
loop in M0. The profile does not claim complete VEC persistence, exact replay of
unrecorded component outputs, persistent crash recovery, multi-process
consensus, tenant isolation, distributed scheduling, or provider-level
exactly-once.
