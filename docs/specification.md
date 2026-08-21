# Cymule Semantic Specification

Status: implemented for the bounded semantic, embedded, durable single-domain,
large-virtual-work, and live-evolution profiles. Distributed ownership,
federation, and strong isolation remain separate proposed profiles.

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
| Semantic specification | `cymule.semantic/4` | exact invocation-path and lexical effect admission is frozen |
| Canonical IR | `cymule.ir/2` | unknown operations are rejected |
| Canonical encoding | `cymule.jcs/1` | RFC 8785 JSON, SHA-256 IDs |
| Artifact identity | `cymule.artifact/2` | closed kind and explicit length-prefixed bytes |
| Artifact type contract | `cymule.artifact-type-contract/1` | exact contract is pinned in typed references |
| Machine snapshot | `cymule.machine-snapshot/5` | authenticated compacted evidence, invocation paths, and lexical scope origins are required |
| Event schema | `cymule.event/4` | readers reject incomplete or unknown semantic events |
| Command protocol | `cymule.command/3` | execution-location proof is required for scope/effect admission |
| Engine protocol | `cymule.engine/2` | stateful durable execution plus versioned success-or-failure envelopes |
| Plugin protocol | `cymule.plugin/2` | capability negotiation and expected failure are explicit |
| Resource descriptor | `cymule.resource/2` | semantic identity excludes locator/access state |
| Resource locator set | `cymule.resource-locators/1` | replaceable resolver records target one exact descriptor |
| Resource manifest | `cymule.resource-manifest/1` | byte digest, size, count, and Merkle root are exact |
| Resource list proof | `cymule.resource-list-proof/2` | index path and opaque cursor chain prove contiguous manifest inclusion |
| Resource handoff | `cymule.resource-handoff/3` | transfer pins the producer's typed Resource Handle Artifact |
| Resource lifecycle receipts | `cymule.resource-*-receipt/1` | pin/release/GC/delete/cleanup operations replay exactly |
| Wait activation | `cymule.wait-activation/1` | external delivery ID fixes source, targets, and result |
| Durable control | `cymule.durable-control/1` | closed mutations and queries delegate all admission to Rust |
| Virtual checkpoint | `cymule.virtual-checkpoint/2` | content-addressed cursor/frontier delta advances one authenticated head |
| Virtual work occurrence | `cymule.virtual-work-occurrence/2` | separate immutable Plan and ExecutionBinding pins per claim epoch |
| Virtual work control | `cymule.virtual-work-control/1` | stable command ID plus owner/work/lease/time precondition |
| Virtual region migration | `cymule.virtual-region-migration/1` | opaque cursor coverage and retirement lineage |
| Virtual migration control | `cymule.virtual-region-migration-control/1` | stable command ID plus verified plan |
| Virtual archive manifest | `cymule.virtual-archive-manifest/1` | exact content-addressed occurrence history |
| Virtual compaction certificate | `cymule.virtual-compaction-certificate/3` | cold descriptor and occurrence-proof root replace hot bytes |
| Virtual compaction control | `cymule.virtual-compaction-control/1` | stable command ID plus pinned archive binding |
| Virtual rehydration control | `cymule.virtual-rehydration-control/1` | stable command ID plus exact occurrence selection |
| Virtual claim control | `cymule.virtual-claim-control/2` | stable command ID, exact Plan and ExecutionBinding pins, plus capacity-slot lease proposal |
| Virtual lease renewal control | `cymule.virtual-lease-renewal-control/1` | exact work and slot lease fences |
| Virtual recovery control | `cymule.virtual-recovery-control/1` | expired lease plus explicit retry/fail/cancel decision |
| Virtual Run weight control | `cymule.virtual-run-weight-control/1` | stable command ID plus positive future share |
| Conformance profile | `cymule.conformance/1` | complete profile cases are required |

Changing effect identity, scope isolation, authority, causal admission, replay,
or migration meaning requires a new semantic specification version.

### 3.1 Engine failures

Every CLI Engine request and response MUST use `cymule.engine/2`. A semantic
failure MUST be a successful transport response containing exactly one closed
failure object with category, phase, stable code, and display-only message.
Contract identity, side, JSON Pointer, bounded issues, and retry disposition are
present only when the Engine has authoritative evidence for them.

An Engine envelope contains exactly one of a success response or failure
object. Success response tags and their fields are closed. Nested discriminated
responses, including completed-or-suspended execution and returned evolution
commands, reject unknown variants, overlapping variant fields, and fields owned
by another operation before the SDK returns them to a caller.

Failure categories distinguish transport, validation, contract violation,
admission denial, conflict, absence, declared plugin failure, plugin defect,
substrate failure, cancellation, timeout, and unknown external-world outcome.
An adapter error is not a declared plugin failure unless the selected operation
contract declares it. Failure to receive an Engine envelope MUST NOT imply that
replaying a potentially mutating request is safe. `reconcile` is the only retry
disposition for an admitted external intent whose world outcome is unknown.

SDKs MUST preserve the complete failure object. Process status and stderr are
transport diagnostics only and MUST NOT become a parallel semantic error path.
The CLI transport's `execute_durable` request MUST invoke the Rust durable
runtime against separate provider-neutral store and optional executor targets.
Queries MUST omit the executor. Its `execute_live_evolution` request MUST
checkpoint one closed command in the same durable domain. Migration and shadow
processes MUST be sealed to the caller-pinned revision and speak
`cymule.evolution-plugin/1`. Validation-only requests are not execution receipts. Duplicate
JSON object keys, non-finite numbers, and integers outside the shared exact
range of `-9007199254740991..=9007199254740991` MUST be rejected before
semantic admission. If a deadline or cancellation loses the response to a
mutating request, SDKs MUST report `unknown_world_outcome` with `reconcile`.

### 3.2 Executable Plan contracts

Every definition, component, effect, and typed input-wait schema MUST compile
as JSON Schema Draft 2020-12 before a Plan is sealed or admitted. If `$schema`
is present, it MUST equal the canonical Draft 2020-12 dialect URI. External
schema resolution is forbidden; fragment-local references remain legal. The
compiler MUST interpret schema-bearing keywords without treating `$ref`-shaped
application data inside `const` or `enum` as a reference.

The submitted schema remains unchanged in the canonical Plan preimage. A
runtime MUST validate Run and invocation inputs, component and effect inputs,
typed wait completions, component and effect outputs, definition returns, and
the terminal Result at their exact boundary. Invalid input MUST be rejected
before plugin dispatch or boundary-specific durable mutation. Invalid output
MUST NOT be bound, stored as a result Artifact, recorded as a component
occurrence, or used to settle an outbox entry. A mutating Effect may already
have changed the world before its returned output is found invalid; its claimed
intent remains unresolved and follows the existing reconciliation path.

Contract failures MUST retain boundary identity, schema/input/output side,
instance and schema JSON Pointers, masked issues, and an explicit retry
disposition in the Engine failure envelope.

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
invalid. Every raw JSON ingress, including Plan, Engine, plugin, SDK, canonical
Artifact, and persisted-state bytes, MUST reject duplicate property names at
every nesting depth before typed decoding can collapse them. A permissive or
fallback decoder is not a compatible reader. A writer MUST validate the
semantic schema before hashing. This strengthens admission within the existing
`cymule.jcs/1`, IR, Engine, plugin, and snapshot versions; it does not widen any
wire shape.

`PlanId` identifies a sealed plan. `EventId` identifies the event payload and
causal parents. Every `ArtifactRef` MUST carry
`identity_version = "cymule.artifact/2"`. Its `ArtifactId` is SHA-256 over the fixed identity-version
bytes, a big-endian 32-bit Artifact-kind length and kind bytes, then a big-endian
64-bit content length and the immutable bytes. Artifact kinds are closed,
lowercase ASCII path segments and MUST NOT contain control characters or
ambiguous delimiters. Snapshot restore MUST recompute this exact identity.
There is no v1 reader or writer.

### 5.1 Typed Artifact contracts

An opaque Artifact needs only its versioned kind and immutable bytes. Files,
directories, snapshots, and provider payloads MUST NOT be forced through a
schema. A typed canonical JSON Artifact additionally pins its exact immutable
`cymule.artifact-type-contract/1` ID in its reference kind. The contract retains
the logical Artifact kind, `application/json` media type, complete document-local
JSON Schema Draft 2020-12 value, and schema digest. Different contracts MUST
produce different Artifact references even for identical bytes.

Type contracts are themselves retained as canonical content-addressed Artifacts
so a registry can be reconstructed after process loss. Encoding MUST validate
before emitting RFC 8785 bytes. Decoding MUST derive the contract ID from the
Artifact reference, verify Artifact identity and canonical bytes, then validate
the value. A caller-supplied registry alias MUST NOT reinterpret a retained
Artifact. Validation issues expose only contract ID and JSON Pointer paths; they
MUST NOT include rejected instance values. Contract code performs no I/O;
resource resolution, clocks, networks, and conversions that require I/O remain
plugin responsibilities.

### 5.2 Cross-Run resource values

M1 Artifact exchange includes a versioned provider-neutral Resource descriptor.
A Resource has a logical shape (`inline`, `object`, `collection`, `directory`,
or `snapshot`), media type, replay evidence, semantic annotations, and an
optional exact manifest descriptor. Inline text/JSON/bytes and external content with verified
SHA-256/size provide location-independent exact evidence; availability still
requires retained inline bytes or a usable location. An immutable provider version
requires the original resolver binding for exact retrieval. A `live` Resource
is useful state but is never exact replay evidence.

`cymule.resource-locators/1` is a separate replaceable realization record bound
to one exact Resource ID and resolver implementation. Locator sets contain only
credential-free public URLs or opaque non-secret references. Signed URLs,
access grants, sessions, and credential revisions are resolver call state, not
Resource, locator-set, Plan, or Artifact semantics.
Credentials MUST NOT enter a descriptor, Artifact, Event, Continuation, or IR.
A public URL MUST be credential-free HTTP(S) without userinfo, query, or
fragment. Private object, drive, sandbox, and signed-URL access uses an opaque
resolver binding/reference whose reference is non-secret. Locations do not
participate in Resource ID; moving identical content MUST preserve identity.

Read operations MUST be bounded chunks. An exact directory, collection, or
snapshot list MUST pin canonical JSON-lines bytes by digest and size, exact
entry count, and Merkle root. Every bounded opaque-cursor page MUST carry a
`cymule.resource-list-proof/2` index-bound inclusion path for every contiguous
entry and digests of both the request and next opaque cursors. A
resolver response that exceeds the
requested bound, repeats an unsafe entry, stalls with an empty non-terminal
chunk, fails page inclusion, or fails content verification MUST be rejected.
Chunked stores use idempotent write IDs, exact offsets, explicit commit, and
explicit abort. Commit convergence and abort MUST delete all owned staging and
chunk objects, verify absence, and return an exact cleanup receipt.

A Run-to-Run handoff carries the exact typed Resource Handle Artifact produced
by the named Run/component occurrence under a stable caller transfer ID and
target slot. It never copies or wraps the external Resource bytes. The handoff
MUST be recorded in the target Run's
M1 application journal by segmented small-head CAS. Repeating identical semantics is
idempotent; reusing a transfer ID with different semantics MUST fail. One target
slot has at most one handoff; multiple values use one collection Resource.
When a handoff activates an input wait, the canonical Resource Handle Artifact,
transfer record, activation record, wait result, and Continuation readiness MUST
share one M1 CAS. The target wait MUST be an input wait whose Run and correlation
match the handoff target and slot. Receipt loss redelivers the same transfer and
MUST NOT complete the wait twice.

The Resource Handle input Artifact MUST use the closed framework
`ArtifactTypeContract`; caller-selected schemas cannot reinterpret it. The same
closed framework registry owns manifest descriptors, list proofs, handoffs, and
lifecycle receipt types.

Retention authority is an M1 lifecycle journal of stable pin and release
receipts. GC records the exact active-pin count and is deletion-eligible only at
zero. A durable delete intent MUST fence later pins before provider I/O;
reconciliation invokes only its exact store binding and commits a terminal
receipt only after verified-absent readback.
Historical receipt replay returns the original decision; later state does not
rewrite it.

## 6. Frozen IR

`cymule.ir/2` contains:

- named component and effect contracts;
- structured definitions composed from `call`, `invoke`, `wait`, `effect`, and
  `scope`;
- reusable definition invocation inside the same immutable Plan with explicit
  input and result binding;
- acyclic definition invocation: sealing rejects self-recursion and every
  recursive SCC, including invokes nested in scopes;
- wait suspension with an optional result binding whose omission intentionally
  discards the admitted result;
- literal, input, binding, object, and array expressions;
- explicit input/output JSON Schemas;
- provider-neutral effect and execution properties.

The IR MUST NOT contain provider endpoints, credentials, queue names, database
products, worker addresses, or deployment topology. Frontends are proposal
producers. `cymule_core::seal_plan` is the only trusted sealer. It compiles every
schema as Draft 2020-12 with external retrieval disabled before computing the
Plan ID; Machine insertion and restore reverify the same admission.

## 7. Versioned effectful continuation

A terminal durable-profile continuation exists only at a semantic safe point
and contains:

```text
plan ID | future binding context | frame | typed state | wait set | scope
effect obligations | authority leases | budget | causal cut | epoch
```

M1 `cymule.durable-state/3` frames separate the resolved definition ID,
structural invocation ID, immutable input Artifact, nested Region path, next
step, and local Artifact bindings. An invocation pushes a frame without opening
a scope. A nested scope retains the same definition, invocation, and input.
Every durable wait pins its exact owning definition, invocation, Region path,
site, and step; only the local bind inside that owner is optional. Registration,
parking, completion, activation, and restore MUST verify this ownership.
Activation atomically stores the result Artifact, completes the wait, writes
the local when present, and readies the Continuation. Resume consumes that
durable local in later expressions or the terminal return.

Process memory and host-language stacks are not canonical. An Attempt pins an
immutable occurrence binding and the continuation epoch. Output from a stale
attempt MUST be rejected.

The M0 kernel implements Run, Attempt, epoch, scope, effect obligation, and
binding projections. M1 defines and persists the complete first-class
Continuation field set through a provider-neutral CAS store and resumes every
safe point expressible by the frozen sequential/nested IR. The Embedded profile
does not claim this persistence because it deliberately uses one-shot memory.

The M1 store MUST persist each non-empty transition as an immutable
content-addressed `cymule.durable-segment/2` containing closed typed operations and atomically move one
`cymule.durable-head/1`. The head MUST authenticate the semantic revision, one
`cymule.durable-checkpoint/1`, the latest suffix segment, its exact length, and
a monotonic physical sequence. A store MUST reconstruct and validate the
checkpoint-manifest chain plus bounded packs before exposing state, MUST rotate
before a pack can exceed `MAX_HOT_SEGMENTS`, MUST create a materialized base
before the manifest chain reaches `MAX_CHECKPOINT_PACKS`, and MUST NOT clone, diff, validate,
or hash the complete projection on a normal mutation. Providers MUST verify and
move only the small head under writer exclusion. Reclaiming cold objects MUST
materialize a new base before its CAS and emit a content-addressed
`cymule.durable-gc-receipt/1`. Physical storage migrations are explicit and
offline; runtime open MUST NOT mix or implicitly fall back to an older format.
Each semantic Machine transition uses `cymule.machine-delta/1`: only newly
admitted Plans, Artifacts, Events, command receipts, and an exact optional
compaction cut enter the segment. A normal commit MUST NOT serialize or replay
the complete Machine snapshot. Providers load only checkpoint and segment IDs
reachable from one consistent head observation; unrelated immutable residue is
not reopen authority.

A durable domain MAY contain multiple independent Runs. Creating the first Run
initializes the domain; creating any later Run MUST append only that Run's exact
Plan when new, immutable input Artifact, `RunStarted` and first
`AttemptStarted` Events and command receipts, plus its initial Continuation in
one CAS revision. It MUST preserve every existing Run and compacted Machine
base. Repeating the same Run ID with identical Plan and input returns the
retained current boundary without resetting progress; changing either fails.
Acknowledgement loss after the creation CAS is recovered by reopen and the same
start request.

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
state directly. M1 exposes a rebuildable parked-wait index rather than a second
durable queue. A source driver MUST return exact indexed targets within the
framework bound and acknowledge transport delivery only after the activation
CAS succeeds. Lost acknowledgement MUST redeliver the identical activation ID,
source, targets, and value; admission then returns the retained decision.

The public M1 control union MUST remain closed and provider-neutral. It MAY
start or resume a Run, admit one identified wait delivery, explicitly release
one prepared effect, or query one Run/domain. It MUST NOT expose raw Event,
Continuation, outbox, or journal mutation. TypeScript, Python, Rust, and Go
construct the same command shape; only the Rust durable runtime reduces it.

When an activation or other wait completion makes a Continuation `Ready`, a
resume after any process boundary MUST advance its epoch and commit a new fenced
Attempt before interpretation. The yielded Attempt that parked the wait MUST
NOT be reused.

M3 virtual materialization MUST checkpoint each source-owned successor cursor
with the complete bounded ready, active, and parked frontier mutation that it
produced. The typed `cymule.virtual-checkpoint/2` M1 application-journal record
contains only the incremental delta, a content digest, a hard 4 MiB canonical
delta bound, and authenticated parent/result transition heads; it MUST NOT
repeat a full `VirtualSnapshot`. The record has a stable ID and an explicit
parent checkpoint. Reusing the ID with different state MUST fail. A
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
`cymule.virtual-work-occurrence/2` record before worker execution. Occurrence
identity is derived from logical work ID and monotonically increasing claim
epoch. Owner, binding, region, Run, and current capacity-slot lease epoch MUST
be retained. A stale owner, work epoch, or lease epoch MUST NOT resolve the
occurrence.

Durable multi-worker admission uses `cymule.virtual-claim-control/2`. A command
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
and compute its semantic content Resource descriptor before calling the archive.
The manifest contains the exact occurrence records, final work/epoch index,
region/Run, and a non-empty causal checkpoint cut. A returned success followed
by a failed M1 CAS MAY leave that immutable object unreferenced; scheduler state,
Machine state, certificate, and checkpoint MUST remain unchanged.
For the M1 linear virtual journal, a new durable compaction cut MUST include the
current checkpoint head; an old command replays from its retained receipt before
this future-head check.

`cymule.virtual-compaction-certificate/3` MUST authenticate the causal cut,
bounded completion summary, complete manifest digest and Resource descriptor,
index-bound occurrence range-proof root,
terminal work/debug index digest, retained occurrence bindings, unresolved
obligations, replay availability, and pinned compactor revision. The current M3 compactor
admits only a completed subtree with no unresolved obligations and `exact`
replay through the retained manifest. It removes occurrence payloads from the
hot snapshot but retains logical work identity, greatest epoch, terminal state,
region/Run, and certificate identity so duplicate work and fencing checks remain
available without loading cold bytes. The summary is a projection, never new
canonical truth.

`cymule.virtual-rehydration-control/1` selects a non-empty exact set of
occurrence IDs from one certificate. Before inserting any record, the framework
MUST reload and verify immutable bytes, Resource identity, manifest/schema
version, manifest digest, causal cut, summary, final work index, region/Run,
retained bindings, and certificate binding. Missing, extra, corrupted, or
conflicting occurrence data restores nothing. Compaction and rehydration
commands are idempotent M1 checkpoints; semantic command-ID reuse and stale CAS
fail without partial scheduler or Machine mutation.

## 8. Causal events

A causal cut is a causally closed down-set of admitted events. Implementations
may represent it with maximal frontier IDs. An event carries logical read/write
footprints and an optional coordination key. Independent events MUST commute.

Every non-start Event MUST extend the current causal frontier of its Run. A
fact is one Machine-wide immutable authority keyed by its exact fact key, so a
fact Event reads and writes that shared key and coordinates conflicting first
writes across Runs. Run-local names MUST NOT disguise the shared fact
authority.

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

A parent scope MUST remain open while any direct child is open. Opening a child,
changing scope membership, and closing either generation coordinate through the
same Run scope-tree authority; commit or abort of a parent with an open child is
illegal. Recursive closure follows because a child cannot close while its own
child remains open.

Commit atomically accepts declared state/evidence and transfers outstanding
world-effect requirements into an `EffectObligationSet`. Scope closure does not
claim that a provider action is applied. Run completion policy decides whether
unresolved obligations block the Result.

A scope MUST NOT commit or abort while any descendant scope remains open. A
commit Event MUST declare exactly one blocking obligation for every mutating
intent owned by that scope, no obligation for an observational intent, and the
reducer-derived resolution bit. Missing, duplicate, extra, or caller-authored
obligation state is invalid.

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

Core admission resolves every entry-rooted invoke edge and requires the derived
dynamic invocation ID, definition, lexical Region path, active scope, stable
site, operation, and occurrence key to agree. A nested site cannot attach to the
root scope, and a site in an invoked definition cannot claim the entry
invocation.
The canonical Event and rebuildable projection retain the complete structural
preimage, immutable occurrence binding, and Plan-declared Effect profile. Replay
recomputes the intent identity and applies policy without consulting a provider
or a mutable Plan lookup.

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

An `unknown` queryable or externally-attested effect enters `pending`. An
`unknown` human or impossible effect enters `governance-required`. Queryable and
externally-attested modes may resolve or remain unknown but cannot enter a
terminal governance dead-end. Human and impossible modes settle only through an
explicit provider-neutral applied/not-applied resolution of the original intent;
the same exact Machine/outbox CAS closes its obligation without redispatch.

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

After `StartDispatch`, a missing response, wrong response variant, or missing or
schema-invalid required output MUST first record `Unknown`. A missing, invalid,
or schema-invalid reconciliation output leaves the original intent `Unknown`
and eligible for another reconciliation attempt; neither case is safe for
provider redispatch.

Embedded execution reports `completed`, `suspended`, `release_required`, and
`reconciliation_required` as closed success-side boundaries. Release carries
the exact sorted intent set; reconciliation carries the original intent. These
states are never flattened into a string failure.

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
for future occurrences. `cymule.execution-binding/2` is the closed executable
binding authority: normalized provider descriptors, each selected operation's
provider and implementation, and every advertised operation revision are serialized to
canonical bytes and stored as an immutable Machine Artifact. Plan requirements
MUST match the selected provider before Run creation. Run, Continuation, and
Attempt MUST pin that Artifact ID. Component and Effect occurrence bindings
MUST be content-derived from the descriptor Artifact ID, operation class,
abstract operation ID, and exact selected operation record; implementation-ID
string concatenation is not a binding. Embedded M0 persists this for Attempts
and Effect Intents, including reconciliation. M1 defines canonical component
occurrence records and commits the binding Artifact atomically with Run input.
Runtime dispatch MUST route each operation to the provider selected in that
Artifact. A provider manifest is capability evidence only: advertising an
unbound operation MUST NOT authorize or route it, and a bound operation whose
selected provider no longer advertises the exact revision MUST fail closed.
Optional plugins MAY define additional domain occurrences, but they MUST
preserve the same immutable binding rule whenever replay or reconciliation
depends on a selected implementation. Session, stream, and domain-controller
semantics remain in the owning plugin rather than this framework specification.

Changing a default MUST NOT rewrite an admitted occurrence. If its original
binding is unavailable, the occurrence enters an explicit unavailable or
governance path.

### 12.1 Semantic Plan evolution

A semantic update creates a new immutable Plan node and a content-addressed
edge from its reviewed base. Structural diff is deterministic evidence for that
edge; it does not mutate or alias the parent. Reusable definitions and subflows
MUST resolve to immutable semantic dependency edges when a Plan is sealed. An
updated dependency creates a new child and, when selected, newly linked
dependent Plan commits. A sealed Plan MUST NOT contain an ambient `latest`
pointer whose later resolution can change its meaning.

The default authoring strategy for a logical reusable-definition reference is
`latest_compatible`. M4 resolves the newest monotonically published revision
whose input and output contract satisfies the reference, injects that exact
definition into a new parent candidate, seals a new parent Plan, advances only
the future default link, and retains every historical linked Plan. The current
implemented compatibility profile requires exact input/output JSON Schema
equality. A reusable revision MAY itself declare logical references. Linking
MUST resolve the complete acyclic dependency closure, record every exact
revision, assign collision-resistant local definition identities, and seal the
entire module closure into the parent Plan. Publishing a compatible transitive
dependency relinks every affected future parent default. Dependency cycles and
conflicting revision choices MUST fail closed. Contract changes require an
explicit checked adapter and MUST NOT be inferred from a logical name.

Before advancing an existing future head, M4 computes the reachable semantic
surface from the Plan entry through local invocations and nested scopes. A
candidate MUST NOT automatically add a reachable component, effect, or wait, or
change a reachable component/effect contract, safety profile, capability, or
authority requirement. A violation leaves the current head unchanged while the
new immutable revision remains independently addressable. Removing an old
surface is compatible. `LatestCompatible` is the API and omitted-wire default;
there is no unchecked-latest strategy.

The complete live-evolution snapshot is portable semantic control state. It
MUST contain the reusable-definition registry and one template-scoped Plan
DAG, rollout history, evidence set, and occurrence-pin map for every registered
parent. Link history MUST be identified by both template and Plan because
different parents may seal to the same Plan ID. Compatible publication,
reverse-dependency relinking, resulting DAG edges, and future rollout decisions
MUST enter one durable checkpoint; a registry head and rollout authority MUST
NOT advance in separate CAS revisions.

The reusable-definition registry inside that snapshot remains independently
verifiable.
Restore MUST verify revision content identities and publication sequences,
rebuild reverse dependencies, deterministically reproduce current and
historical links, and reject missing or extraneous revision claims. M1 journal
checkpoints MUST retain explicit lineage and idempotent checkpoint identity;
stale writers roll back their in-memory transition, while acknowledgement loss
reopens to the committed registry result.

Future calls may select a compatible new child Plan under an admitted rollout.
An already materialized invocation remains pinned to its original Plan. A
yielded Continuation changes Plan only at a semantic safe point with state and
contract migration evidence plus an epoch advance. Parent and child may execute
different Plan commits only when a checked cross-version adapter is total over
reachable input/state, preserves output contracts, does not widen effects or
authority, and maps failure, cancellation, budget, and ownership semantics.

M4 durable controls checkpoint the complete live-evolution authority through an M1
application journal with explicit parent lineage. Plan-edge admission, future
rollout, occurrence selection, migration, shadow evidence, promotion, and
rollback MUST survive stale CAS and lost acknowledgement without changing an
existing occurrence pin or creating a second decision.

`cymule.live-evolution-control/3` is the complete cross-language envelope. It
adds reusable-definition publication, parent-template registration, atomic
publish/relink, and template scope around the closed Plan operations. Migration
and replacement-Run restart commands MUST carry the exact durable safe-point
proof; clients MUST NOT sequence registry, rollout, and occurrence mutations.
When virtual work is dispatched, template-scoped Plan selection, the capacity
slot lease, and the worker claim MUST enter one CAS revision. Replaying a claim
whose coupled selection record is absent or different MUST fail closed.
The claim MUST retain `plan_id` and `occurrence_binding` separately, and
`occurrence_binding` MUST be the Artifact identity of the exact admitted
`cymule.execution-binding/2`, never the Plan ID.

A reviewed patch carries the complete target Plan Candidate, an exact declared
operation list, and evidence. M4 MUST seal the target, recompute the structural
diff, and reject the patch unless the lists are identical. Direct edge admission
and snapshot restore MUST enforce the same exact non-empty diff; a
content-addressed but caller-invented operation list is not review evidence.
Impact analysis MUST
inspect generic Continuation sites and MAY accept stable active-site identities
from higher profiles; it MUST NOT import their domain models.

A migration adapter is called only with a content-addressed safe-point proof and
is pinned by identity and revision. The proof MUST be derived from a persisted
`Ready` Continuation at the root scope with at least one frame and no waits,
effect obligations, or authority leases. Durable admission MUST re-derive the
proof from current M1 authority before invoking the adapter. Its admitted
descriptor MUST claim totality over
reachable source state, preserve failure/cancellation and budget/ownership
meaning, and not widen authority or effects. A shadow driver MUST suppress or
simulate target mutating effects and pin both occurrence bindings. Migration
output and shadow comparisons are immutable evidence, never ambient authority.
Plugins MUST return complete content-addressed Artifact records for new output
or evidence; Rust MUST verify their bytes and commit them to the M1 Machine in
the same CAS as the evolution checkpoint. That owning CAS MUST also verify the
source binding and target Plan/binding compatibility, replace the Continuation
Plan/state/binding, advance the Machine and Continuation epoch, fence every old
Attempt, and create one active Attempt under the target binding. A durable
receipt MUST NOT reference an Artifact that existed only in plugin memory.
Every mapped target frame MUST resolve in the target Plan, and each adjacent
frame MUST be owned by the parent's exact current scope or invoke step; path
prefixes alone do not prove a resumable call stack.
After acknowledgement loss, retry MUST return this retained transition without
invoking the migration adapter again.

`restart_under_new_plan` is an explicit alternative to state migration. At the
same verified source safe point it authorizes a distinct replacement Run, exact
target Plan, explicit replacement input, and policy evidence. It MUST NOT mutate
the source Run, reuse its identity, or reinterpret old state implicitly. The
runtime initializes the replacement through normal Run admission; the evolution
controller records only the immutable authorization and target Plan.

Rollout observations MUST reference the decision and the occurrence's immutable
Plan pin. A gate counts exact retained observation and shadow identities. An
exceeded failure or inequivalence ceiling yields rollback immediately; only
satisfied minimum success/equivalence evidence yields promotion; otherwise the
gate is pending. Promotion and rollback create new future-only decisions and
auditable transition receipts. Previously admitted occurrences do not change.
A later candidate after rollback MUST use the last authoritative fallback; the
failed target cannot become fallback merely because it remains the latest
registry link.

`cymule.evolution-control/4` is the closed cross-language command boundary.
SDKs may construct and transport its patch, selection, migration, shadow,
restart, observation, and gate operations, but only the Rust M4 controller
resolves dependencies, invokes plugins, evaluates evidence, or mutates durable
state.

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

`cymule.machine-snapshot/5` MAY replace a causally closed Event prefix with an
authenticated base projection and cumulative ordered compacted evidence. Each
compacted entry retains the Event ID, admitting command ID, Event command hash,
and digest of the complete command record and receipt. The prefix digest is
recomputed over that complete evidence plus the verified projection digest; a
shape-valid caller digest is not authentication. Every remaining Event stays in
full and MUST have all parents in either the base or retained suffix. Restore
verifies the base projection, every v2 Artifact reference, and an exact
bidirectional Event/command-receipt closure before replaying the suffix. Every
retained or compacted Event identity has exactly one applied receipt; every
applied receipt names one such Event; retained Events match command IDs and
hashes directly; and compacted Events match their authenticated command and
receipt evidence. A conflict receipt names no Event. Older snapshot versions
are rejected rather than upgraded implicitly. M1 compaction is a CAS transition with explicit
lineage; stale writers lose and acknowledgement loss reopens to the committed
base. Compaction preserves current state replay but does not claim the removed
Event bodies remain available for historical inspection unless a higher-profile
archive retained them.

## 14. Implemented profile boundary

The Embedded profile implements canonicalization, sealing, in-memory stores,
causal replay, command idempotency and stale-action rejection, attempt fencing,
scope/obligation semantics, effect lifecycle, process plugins, and all four SDK
chains. `wait` returns a typed site/wait/optional-bind boundary but no fake
Continuation or resume token; durable resume belongs to M1. The profile does not claim complete VEC persistence, exact replay of
unrecorded component outputs, persistent crash recovery, multi-process
consensus, tenant isolation, distributed scheduling, or provider-level
exactly-once.
