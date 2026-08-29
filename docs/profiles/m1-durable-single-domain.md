# M1 Durable Single-Domain Profile

Status: partial terminal candidate; source-implemented, validation pending. The
terminal implementation is integrated and has passed multiple focused gates,
but review is still changing the tree and the final frozen-tree full gate has
not passed. This document describes the terminal authority boundaries, not a
release, package publication, operator migration, deployment, production
rollout, or assertion that every conformance gate has passed. Promotion follows
the [conformance status ladder](../conformance.md#status-ladder).

The [semantic specification](../specification.md) is the single normative
source. This profile maps those laws to the current Durable control and Store
boundaries rather than defining a second state machine.

## One durable authority

The semantic kernel owns Plans, admitted materials, Runs, Attempts, Scopes,
Effects, Events, and complete command batches. It performs no provider I/O.
`cymule-durable-protocol` owns the lower Clock, Continuation, execution-claim,
wait-owner, and activation contracts. Higher profiles do not redefine them.

`DurableCoordinator` owns admission against one pinned StateRoot. A runtime
control binds the exact execution providers and Clock; store-only controls do
not acquire execution authority merely to read or mutate unrelated profile
state. The public controls accept closed commands, never caller-authored
Continuations, postconditions, arbitrary transactions, or raw Event appends.

The Store publishes immutable authenticated map/log objects and moves one small
CAS head over a fixed manifest. Opening a coordinator authenticates that head
and manifest. Commands resolve their bounded typed neighborhoods on demand;
ordinary reopen does not reconstruct a whole Machine, profile snapshot,
application journal, or complete parked-wait index. Exact historical reads and
full offline audit are distinct operations.

## Execution and recovery

Parameter-free initialization may publish an empty domain first; it admits no
Run or provider work. `StartRun` admits the exact sealed Plan, input, and
execution-binding material together with `RunStarted`, the first
`AttemptStarted`, the Running
Continuation, and its execution claim in one CAS. Later Runs do not replace
existing Runs or their material. Accepted Start replay preserves the exact
Plan/input/binding and observes the retained boundary; a Running Run remains
Busy, while an ordinary Ready Run requires Resume. Before consulting the Clock,
Start performs one exact hot/cold command lookup. A retained command validates
its complete singleton batch and exact Plan, binding, and input leaves; a truly
absent command and Run proceeds to the fresh Clock-guarded admission without
constructing and discarding a duplicate Machine stage. Exact replay therefore
does no Clock or provider work, while a fresh Start remains subject to both the
admitted execution capability and current-Clock guard.

A Running Continuation has exactly one issued claim. Resume from Ready advances
its epoch, starts a new Attempt, and acquires its fence in the same CAS.
Unexpired ownership returns Busy before business Call or Effect invocation.
CLI Describe preflight is separate from that admission. Recovery is an explicit
takeover of the exact old fence after current-scope Clock evidence proves
expiry. Expiry alone changes no state. A current-head Clock guard must enclose
the final Store CAS; historical receipts are replay evidence, not new authority.

A component Call persists a semantic occurrence and a provider Attempt before
I/O. The occurrence is Pending or Completed; the Attempt is Running,
Superseded, or Completed. Only a newly admitted Attempt permits provider
invocation. Re-reading an existing Running Attempt returns InFlight, including
after a lost CAS response. Takeover supersedes the old Attempt; later admission
may create another Attempt for the same occurrence. A legacy Call can therefore
repeat provider cost after recovery and is not an exactly-once world effect.

The occurrence retains its Attempt count and exact latest-Attempt identity;
each successor Attempt pins its predecessor. Admission resolves that bounded
frontier rather than replaying every earlier Attempt.

A component result, outcome, Attempt completion, occurrence completion, and
post-call Continuation share one CAS under the current claim. The result must
validate against the exact origin Plan's output schema before canonicalization
under the declared Artifact kind. Invalid output completes neither record:
the occurrence remains Pending and the Attempt remains Running under its
retained claim. Execution status and world settlement remain separate axes.
An admitted `ExpectedFailure` that crossed a paged Core reservation is recovered
from that retained transition, its exact staged detail Artifact, occurrence,
Attempt, claim, and Continuation. Recovery rederives the original command and
material and advances the same pages; it never calls the component or Clock
again and exposes no caller-authored failure finalizer.

## Effects, Scopes, and waits

Effect identity is derived from semantic control position, not a worker,
execution epoch, provider generation, or physical Store revision. Each outbox
entry separately retains the origin Plan, execution-binding Artifact, derived
operation binding, and claim fence.

The current outbox payload has one Run-local authority in
`RunQueryIndexes.effects`; the global intent map is only an immutable
`intent -> Run` locator for intent-only controls. Active-effect and active-lease
roots are Run-local derived memberships. No current dispatch payload is mirrored
in a global map or recovered through a fallback reader.

Enqueue, dispatch admission, observation, and reconciliation each couple their
exact Core batch to the matching outbox and required Continuation transition. An
ambiguous dispatch remains Unknown under the original intent. Reopen and
reconciliation never redispatch it. Applied output is a canonical Artifact;
an absent provider value becomes JSON null only if the pinned output contract
accepts null. Invalid output cannot become an Applied receipt.

Exact implementation loss before dispatch terminalizes the Effect as
CancelledBeforeRelease/NotApplied without a claim or result. After dispatch,
implementation loss retains Unknown and its original claim for governance.
Public ResolveEffect accepts only that claimed Unknown boundary and requires
the historical provider ledger; it is not a pre-dispatch settlement API or a
current-provider fallback. Its receipt distinguishes the requested resolution
from the provider's actual terminal decision.

Nested invocation frames inherit their actual enclosing Scope. Scope
descriptors use the Core-admitted body location. Complete small Scope
neighborhoods may use a bounded inline proof; a standalone larger closure uses
typed persisted page progress and one final semantic decision. Its progress
CASes are not completed commands. A multi-command composite that cannot satisfy
its inline contract fails with PagedScopeRequired before publication or provider
I/O, rather than silently expanding a read budget.

A parked wait pins its complete Plan-derived structural owner. Activation
commits its value Artifact, completed wait, optional local result, and Ready
Continuation together. The lazy revision-pinned parked-wait index supports
bounded source and target selection; stale cursors are explicit.

An activation receipt retains the complete selected target set, newly applied
subset, and original Ready-Run set. Already terminal targets are nonwinners and
cannot block valid broadcast peers. Exact receipt replay precedes pending-wait
lookup. Transport acknowledgement follows the activation CAS and a lost
acknowledgement redelivers that same identity and selection.

Cancellation fences execution, supersedes in-flight provider Attempts, cancels
pending waits and undispatched Effects, and retains dispatched ambiguity for
reconciliation. Cancellation and explicit Effect resolution have separate
typed receipt maps; neither is a reserved application-journal record.

Wide failure and cancellation use the same Core-owned paged transition as their
semantic authority. A Durable-private companion, identified by that transition
rather than by a second cursor, advances hidden Run-local outbox roots with each
Effect page. The final CAS publishes only that Run's completed Effect roots,
Continuation, Attempt and outer receipt, preserving unrelated Run commits made
between pages. While the Core Run is `Transitioning`, ordinary same-Run result,
Attempt, Wait and Effect sidecars are fenced; only the exact transition may
advance or finalize them.

## Coupled higher profiles

Resource, Virtual, Evolution, and optional Agent state use their own bounded,
normalized keyed families. Their pure profile reducers own profile semantics;
Durable resolves exact inputs and commits the profile mutation, material, and
any coupled M1 transition in one CAS. An embedded receipt chain, global
lifecycle journal, scheduler snapshot, or generic prepared-postcondition write
is not a second authority.

Resource transfer may publish a future slot for an Active Running target.
Activation additionally requires the exact pending input wait and Waiting
Continuation. Resource retention and deletion use the physical binding/content
family; the semantic Resource ID is provenance.

Virtual claims, wakeups, leases, terminal work, and archive pins remain exact
normalized transitions. Evolution selection or safe-point migration couples its
exact Plan and binding with the M1 transition. Profile-specific provider I/O
uses a retained occurrence or admitted command boundary, never a recovered
ambient provider.

## Complete batch replay and compaction

A command receipt contains an ordered Event vector. One persistent batch owns
its complete ordered members, member receipts, material proposal, admission
parent, frozen source, and resulting authority. Material-only admission is a
nonempty material proposal with zero command members, not an empty fake command.
Staged material is not committed authority.

A causally closed compaction cut preserves complete batches and every retained
batch's authenticated frozen-source dependency. Cut-time Plan, Artifact, and
batch commitments remain separate from the final hot material inventory.
Compaction does not change the semantic authority root.

The current public maintenance entry is the Rust-only
`DurableStoreControl::compact_machine_history(HistoryCompactionRequest)`.
Callers supply a stable ID, exact source revision, EventPrefix or
EventFreeAdmissions kind and requested suffix, not a replacement Machine.
Durable resolves the pinned source, checks that pending material and paged
transitions are absent, and consumes Core-prepared compaction authority. The
explicit offline operation may process the complete source/base once; ordinary
open and execution do not inherit that traversal. There is no Engine or SDK
transport for this maintenance operation.

The exact Store-pinned base anchor binds the independently stored command
archive and its cumulative counts. Hot restoration uses that anchor and the
retained suffix without scanning the cold archive; full replay and offline
audit verify the complete archive explicitly. Zero-Event material batches,
conflict receipts, sparse command-index proofs, and independent batch records
remain part of that closure. Physical cold reclamation must preserve every
reachable index, entry, batch, and profile object.

Cold reclamation has two operations: reconcile the bounded deletion page named
by the current head, or explicitly advance to the next page and head.
Reconciliation never selects new work or silently advances a generation.

## Conformance and versions

The current exact-reject generations and their ownership are registered in
[Version domains](../version-domains.md). Pre-StateRoot stores, old snapshot and
wire generations, fallback decoders, and dual-write compatibility are not
supported. Operator-owned one-time transitions are documented in
[Migration runbooks](../migrations/README.md).

Promotion from this partial terminal candidate to a validated source candidate
requires focused and whole-workspace tests for
ordinary execution, illegal transitions, exact replay, stale writers, claim
takeover, response loss, real process death, tampered reachable objects,
compaction, cold reclamation, and all four SDK chains against the current Rust
Engine. A passing mock or an older binary does not establish that claim.
Directory and SQLite adapter conformance includes full reachable-root audit;
ordinary reads must remain bounded independently of that offline operation.
