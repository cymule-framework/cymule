# Architecture

Status: partial. The terminal source integration is present in the worktree;
complete repository verification and version-authority closure remain pending
the final source freeze. This document describes realization and ownership.
The [semantic specification](specification.md) owns normative behavior, and
[conformance](conformance.md) owns profile claims. Exact wire generations are
listed only in [Version domains](version-domains.md).

## Trust and ownership

The semantic kernel is a Rust library, not a service. It performs no filesystem,
network, wall-clock, randomness, model, tool, or provider I/O. Concrete
integrations sit outside that boundary.

| Owner | Responsibility |
| --- | --- |
| [cymule-core](../crates/cymule-core/src/lib.rs) | Plan sealing and schema admission, canonical identity, typed commands, deterministic reduction, complete batch replay and compaction proofs |
| [cymule-authenticated-collections](../crates/cymule-authenticated-collections/src/lib.rs) | Pure authenticated maps and ordered logs used by Core and Durable |
| [cymule-durable-protocol](../crates/cymule-durable-protocol/src/lib.rs) | Clock receipt/reference, Continuation/frame, execution-claim, wait-owner and activation DTOs and pure validation |
| [cymule-runtime](../crates/cymule-runtime/src/lib.rs) | Embedded interpretation, executable boundary validators, exact provider-binding admission and Engine/plugin transport contracts |
| [cymule-profile-protocol](../crates/cymule-profile-protocol/src/lib.rs) | Resource, Virtual, Evolution and Agent DTOs, bounded source views, identities and pure reducers |
| [cymule-durable](../crates/cymule-durable/src/lib.rs) | Store-pinned source resolution, resumable execution and typed profile coordination, atomic material/receipt/sidecar publication |
| Provider crates and language SDKs | Concrete I/O or authoring/transport; neither is a second semantic reducer |

Rust's closed enums and ownership make the kernel's transition authority
explicit. The language boundary remains versioned JSON: TypeScript, Python and
Go do not use FFI or reconstruct the reducer. The Rust SDK uses the same public
Engine contract rather than a separate language-specific runtime.

## Plans and executable contracts

[Core IR admission](../crates/cymule-core/src/ir.rs) owns the only Plan sealer.
It compiles every definition, component, Effect and typed input-wait schema as
Draft 2020-12 with external retrieval disabled, rejects recursive definition
invocation, and verifies the canonical Plan identity. Machine insertion and
restore repeat that admission.

[Runtime contracts](../crates/cymule-runtime/src/contract.rs) compile the
unchanged admitted schemas into boundary validators and produce bounded,
masked contract diagnostics. This is execution validation, not a second Plan
sealer. Core itself depends on the schema compiler; it does not delegate
structural or executable-schema admission to a provider.

A sealed invocation resolves a definition inside that same immutable Plan.
Frames record structural invocation and lexical Scope ownership, not a
host-language call stack. Live Evolution links reusable modules before sealing;
a transitive pinned dependency cannot change merely because a registry head
moves. See [Frozen IR](specification.md#6-frozen-ir) and
[Semantic Plan evolution](specification.md#121-semantic-plan-evolution).

A component Call is unclassified computation. Its declared output schema and
required Artifact kind govern the successful result. Durable records the
occurrence and provider Attempt before invocation, but a lost result before
checkpoint can still cause a later admitted Attempt to repeat provider cost.
Provider observations with ambiguity handling use observational eager Effects;
world changes use mutating Effects.

MLIR remains an optional, partial authoring workbench. The current smoke path
checks experimental generic-operation syntax. A registered dialect,
structural verifiers and deterministic lowering remain proposed; LLVM/MLIR is
not a runtime dependency.

## Engine and provider admission

The supplied CLI Engine uses stdin/stdout and the closed Engine envelope.
Success includes the complete strictly decoded request next to its response;
each SDK compares it with the exact request value serialized on its wire
before interpreting the response. Failure has no request echo. Stderr and
process status are transport diagnostics, not a second semantic failure
channel. Compact stdout has one 128 MiB plus 32-byte framing envelope limit capable of carrying a
64 MiB accepted request echo plus a separately bounded 64 MiB response;
diagnostic stderr retains an independent 1 MiB limit. Request/response pairing, member-presence preservation and ambiguous
mutating-response handling are specified in
[Engine failures](specification.md#31-engine-failures).

Execution uses an immutable `ExecutionBinding`. The selected operation record
and its transitive provider dependency closure must match the runtime owner's
admitted pin. A live manifest demonstrates capability but cannot select a
provider, widen authority or authorize an unbound operation. The framework
hands the exact provider invocation to a private one-shot admission token.
Historical dispatch and reconciliation use the retained origin Plan and
binding, never current defaults.

The process executor captures its executable closure and explicit
configuration. Fresh durable execution preflight performs Describe before
writable Store construction and retains that exact host/binding admission.
Opening the runtime consumes the admission without another Describe. Concrete
process deadlines, closure copying and termination remain adapter-owned; their
contract is documented in [Plugins](plugins.md), not in Plan semantics.

The narrowest public Durable controls are distinct:

| Control | Required authority and work |
| --- | --- |
| `DurableStoreControl` | Store-only Run queries, wait admission, cancellation, Resource control, Evolution with its fixed provider registry, and explicit history maintenance |
| `DurableProviderControl` | Store plus admitted historical executor for terminal resolution of an already Unknown Effect; no Run claim or Clock |
| `DurableRuntimeControl` | Store, admitted executor and issued current-head Clock authority for Run execution; also supplies the owning Virtual and Agent controls |

The CLI checks exact capability presence for each command before writable
Store I/O. Queries use provider-native read-only openers. Exact terminal
Effect-resolution and Evolution retries read the retained receipt before
constructing historical providers. Ordinary Evolution requires neither a
runtime PluginHost nor an ambient Clock.

## Durable storage and execution

[DurableStore](../crates/cymule-durable/src/store.rs) exposes a small head and
immutable authenticated objects.
[StateRoot](../crates/cymule-durable/src/state_root.rs) owns their typed layout;
the provider neither invents a transition nor performs semantic reduction.
The fixed manifest roots Core authority and closed profile families. Persistent
map updates copy changed trie paths; ordered-log operations copy bounded AVL
spines. Their physical roots authenticate representation history, not merely
the materialized sequence.

Ordinary coordinator open reads only the head and its exact manifest. Commands
resolve bounded typed neighborhoods on demand. They do not rebuild the complete
Machine, active projection, scheduler, Agent Session, application journal or
parked-wait index. Exact historical lookup and `load_full_audit` are different
operations; the latter explicitly traverses reachable authority.

Run Wait pages authenticate a same-CAS map of small `DurableWaitSummary` leaves.
They never load the complete Wait leaf or compile its Input schema. Exact-item,
activation, and full-audit paths retain the complete Wait, while full audit
checks bidirectional membership and exact summary equality.

The coordinator lowers a Core command or complete command batch and any typed
sidecars before publication. The Store writes immutable objects and compares
the exact physical token before moving the small head. An unsuccessful CAS may
leave unreachable immutable objects but publishes no new semantic authority.
A possible publication with no authoritative acknowledgement becomes
`CommitOutcomeUnknown`, not permission to retry new work.

Parameter-free initialization creates an empty domain. Each Start then admits
its Plan, input, binding, Run/first-Attempt Events, Continuation and execution
claim together. Existing Runs are preserved. A Start replay observes retained
progress; it does not reset a Run or bypass the normal Busy/Ready boundaries.

A Running Continuation has one driver claim. Ready resume and expiry-proven
takeover use issued Clock evidence, with a non-blocking current-head guard held
through the final Store CAS. Expiry alone changes nothing. A component result
commits its validated Artifact, occurrence outcome, Attempt completion and
post-call Continuation together under that claim. See
[Continuation ownership](specification.md#71-continuation-ownership).

Small Scope closures use complete bounded inline membership and order proofs.
Standalone larger closures use persisted typed page progress and one terminal
semantic decision. A multi-command composite closure that exceeds its inline
contract returns `PagedScopeRequired`; it does not silently widen the
operation's read budget. Intermediate page progress is not a completed command
or permission for provider I/O.

Wait activation has one stable delivery identity. The source driver durably
retains its selected targets before delivery and acknowledges only after the
activation CAS. The parked-wait view is lazy and revision-pinned; terminal
nonwinners do not block a valid broadcast peer. A resumed Ready Continuation
acquires a new Attempt rather than reusing the yielded one.

Cancellation, declared component failure and Effect settlement retain separate
typed decisions. Missing implementation before dispatch cancels that Effect
before release as NotApplied; missing implementation after dispatch preserves
Unknown and requires governance/reconciliation. A lost provider response is
never a new semantic Effect. Detailed lifecycle laws remain in
[Run execution](specification.md#32-run-execution-and-world-settlement) and
[Effects](specification.md#11-effects).

## Replay, compaction and reclamation

A complete Core batch retains ordered member commands and receipts, its
material proposal, frozen source and admission parent. A material-only batch
has no command members but nonempty admitted material; a conflict can have a
receipt without an Event. Neither can be discarded because an Event count is
zero.

Machine compaction is explicit offline maintenance through
`DurableStoreControl::compact_machine_history(HistoryCompactionRequest)`.
The request selects an exact source revision and Event-prefix or Event-free
admission cut; it contains no replacement Machine or archive bytes. Durable
derives the source and consumes Core-prepared authority. The maintenance
operation may process the complete source/base once, while ordinary execution
retains bounded reads. It is Rust-only: no Engine or SDK transport command is
implied.

The base, archive entries, complete batches, sparse command index and new head
share publication authority. Hot restoration uses the exact Store-pinned base
anchor and retained suffix. Offline replay additionally verifies the archive.
A cut preserves each retained batch's frozen-source dependency and the semantic
authority root. See [Replay](specification.md#13-replay).

Physical reclamation is separate from semantic compaction.
`reconcile_cold_reclamation` finishes only the deletion page pinned by the
current head. `advance_cold_reclamation` explicitly selects and publishes the
next page. Neither an implicit reopen cleanup nor an unsuccessful delete can
stand in for the pinned receipt.

The repository supplies shared-memory reference, directory and SQLite stores.
Directory publication uses non-blocking exclusion and atomic local publication;
SQLite uses immediate transactions, WAL, synchronous-full persistence and
zero-timeout contention. These are realizations, not semantic identities.
Unsupported or mixed physical generations fail closed without a legacy
importer. See [Migration runbooks](migrations/README.md).

## Resource realization

Resource semantic DTOs and lifecycle reduction are shared through
`cymule-profile-protocol::resource`. `cymule-resource` supplies typed Artifact
contracts, verified resolver/store helpers and handoff convenience controls.
The Resource descriptor identifies content and replay evidence; separate
locator records identify how an admitted resolver can find it. Credentials
remain call state.

Bounded reads and canonical manifest-list proofs verify resolver output.
Chunked writes use stable write identities and receipt-backed commit/abort
cleanup. Filesystem and Apache object-store adapters are current concrete
realizations; a drive, sandbox or other remote transport is not implied by the
provider-neutral descriptor.

Handoff authority is keyed by transfer, with target-owned slot uniqueness and
payload-free paged indexes. Activation retains the exact producer occurrence,
Resource Handle output and source-transfer receipt and couples wait completion
to Continuation readiness. Retention and deletion use the immutable physical
binding/content key so annotation-distinct descriptors sharing bytes cannot
collect each other. Generic Resource commands do not own Virtual archive or
Agent finalized-stream pins. The exact laws and bounds are in
[Cross-Run resource values](specification.md#52-cross-run-resource-values).

## Large virtual work

`cymule-profile-protocol::virtual_work` owns normalized scheduler state and
pure reduction. `cymule-virtual` re-exports those contracts and provides
`ResourceBackedVirtualArchive`. Durable invokes the exact
`VirtualRegionSourceProvider`, `VirtualRegionMigratorProvider` or
`VirtualArchiveProvider` only when the command requires it.

A bounded materialized frontier is separate from keyed region, work,
occurrence, parked-reason and receipt authority. A Virtual Run is a scheduling
namespace, not a synthetic Machine Run. The actual selected Run determines
its direct Plan or Evolution selector. The claim returns either a retained
NoWork receipt or a claim with its complete verified SealedPlan; binding
material, Evolution selection when needed, claim and capacity lease commit
together. Later results retain exact owner/work/lease fences.

Fairness uses durable integer weight, cost, deficit and dispatch-count aging.
Source visibility rotates independently because unmaterialized cost is unknown.
Retry and lease recovery are explicit typed transitions. A region migration
returns a complete non-serializable proposal with exact evidence and target
Artifact records; provider verification and the normalized CAS precede source
retirement.

Archive publication is verified immutable Resource content. Compaction retains
a certificate, bounded summary, exact execution-binding pins and cumulative
work/command proof roots. The archive pin is admitted with the certificate;
only the owning retirement command releases it.

Rehydration is selected and explicit: it restores only requested occurrences
after proof and identity verification. The current Resource-backed adapter
loads and verifies the complete manifest, bounded to 8 MiB, and then checks the
selected range against its retained proof catalog. It does not yet provide
range-only archive I/O. A provider implementation that avoids the complete read
is proposed, not an achieved latency or allocation guarantee. The semantic
contract is [Large virtual work](specification.md#74-large-virtual-work).

## Live Evolution and optional Agent integration

Evolution's pure authority lives in
`cymule-profile-protocol::evolution`; `cymule-evolution` re-exports it and owns
the closed process-provider wire. `DurableStoreControl::evolution` binds a fixed
provider registry and exposes exact reads plus one typed persistence command.
All-ever command alias lookup precedes source derivation and provider work.
A fresh command derives its exact keyed source and commits normalized current,
receipt, material and coupled M1/M3 state together. Historical publication
reuses immutable records rather than rewriting evidence.

Migration derives quiescence from one pinned Run authority, resolves its target
binding before provider work, and replaces the Continuation as a claim-free
Ready safe point. Ordinary resume separately acquires the next claim. Restart
records authorization for a distinct replacement Run; it does not automatically
create or execute it. Adapter preservation declarations are required contracts,
not a kernel proof that arbitrary transformation code is total.
See [Semantic Plan evolution](specification.md#121-semantic-plan-evolution).

Core and the public Engine do not define an Agent Loop, Session or streaming
transport. The optional Agent profile keeps its portable state/reducer in
`cymule-profile-protocol::agent` so Durable can atomically couple its keyed
state, receipts and Resource pins. The
[Agent interaction plugin](../plugins/agent-interaction/README.md) supplies
controllers and concrete host-facing interfaces. It is not a generic journal
writer, and its domain types are not exported as an Engine/SDK operation.
Concrete ACP, MCP, model or editor behavior remains integration-owned.

The Agent profile uses one occurrence lifecycle for the standalone controller
and reference driver. Only a freshly acknowledged Started CAS grants host
dispatch. Context requests and snapshots pin an immutable `(message_head,
message_count)` prefix; pages read that retained prefix from a newer StateRoot
without substituting the current Session head. The cumulative selection budget
counts complete `AgentMessageCurrent` bytes independently from the page-wire
limit. A recovery call no longer owns the original reader-delivery capability,
so an unresolved Context cannot accept a provider-authored Completed snapshot;
it remains Unknown unless an earlier exact completion already committed.

Stream Open computes the canonical size of its prospective final Agent update.
Staged chunks consume that exact capacity before admission. External delivery
prederives one semantic Object Resource Handle from media type, digest, and
size; the provider may add resolver locations but cannot change that Handle.
This makes wrapper overflow a pre-I/O admission failure instead of a
post-publication terminalization failure.

Target ownership is a separate fixed StateRoot family keyed by Session,
Message/Tool kind, and local identity. Ordinary writers and streams use the
same pure transition: absence or a Released tombstone may become Reserved or
Materialized; a Reserved generation may become Materialized or Released;
Materialized is terminal. External reservation writes that target claim,
Resource pin/family, stream current, and Finalize command in one CAS before
provider I/O. Finalize writes the target, Materialized claim, stream/Session,
catalog, Active pin, command, and receipt in one CAS. This exact-key design
closes double-stream and stream-versus-direct-write races without an open-stream
scan or another target authority.

## Verification boundary

[Test ownership and commands](testing.md) define focused and full verification.
Source code and named tests demonstrate intended paths, not a completed
conformance run for a moving worktree. A profile claim requires the complete
fault-oriented suite against the same frozen source and exact adapters.
Publication, deployment and production enablement are separate evidence.
