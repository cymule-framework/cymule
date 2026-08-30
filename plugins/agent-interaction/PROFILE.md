# Agent Interaction Plugin Profile

Status: implemented optional profile.

This profile belongs to `plugins/agent-interaction` and
`crates/cymule-profile-protocol::agent`. It does not change Cymule's semantic
kernel or define a universal Agent loop.

## Authority boundary

The profile has one mutation union: `AgentCommandAction`. It contains direct
Session updates, standalone host-occurrence transitions, streams, M1 input, and
M1 workspace commands. `AgentCommand` binds the complete action to the exact
source StateRoot revision and a content-derived command ID.

`AgentCommandReceipt` embeds only bounded exact before witnesses and the exact
reducer result. `verify_for` replays the profile reducer and compares the whole
postcondition; membership or same-owner checks are not sufficient. The receipt
has its own content identity and never embeds the result StateRoot revision,
which would create a content-addressed fixed point. `AgentCommit` is a
non-persisted envelope whose `observed_revision` means the head observed by that
call: first commit normally returns the resulting head, while a later replay
returns the then-current head with the original receipt and no write.
Its required-nullable `committed_revision` is that same observed revision only
when this invocation newly committed the command and received a verified
acknowledgement. Exact replay carries explicit null even at the original result
head. Neither receipt presence nor a source/result revision difference proves
fresh dispatch ownership. The physical envelope never enters receipt identity.

The public persistence capability exposes only:

- ordinary `commit_agent` for commands requiring no provider product;
- specialized stream finalization, observe-only publication reconciliation,
  and workspace commit methods whose provider registries are framework-owned
  and binding-keyed;
- exact Session, message, tool, elicitation, occurrence, and stream reads;
- bounded message and unresolved-occurrence pages; and
- an M1-derived workspace-admission read.

The production writer is the provider- and Clock-bound
`cymule_durable::DurableAgentControl` borrowed from its owning Runtime. Store
control exposes Agent reads only and does not implement `AgentPersistence`.
This plugin implements `AgentPersistence` for the typed Runtime writer and maps
every method directly to the matching closed Durable seam; Durable does not
depend back on the optional Agent plugin, and there is no generic transaction
fallback.
Profile `Conflict`, `Substrate`, `Persistence`, and `Integrity` failures map
one-for-one into the corresponding structured `AgentError`; identity mismatch
maps to Integrity with a stable code, Encoding remains distinct, and `NotFound`
plus `CommitOutcomeUnknown` remain typed. Durable Busy, Substrate, Persistence,
RuntimeDefect, Integrity, history conflict, reconciliation, archived replay,
Cancelled, TimedOut, and Encoding failures retain their closed category and
original fields rather than a display-rendered string.

There is no Agent journal trait, raw `JournalRecord`, `JournalBatch`,
`DurableTransaction`, arbitrary StateRoot delta, prefix replacement, or
Session-wide enumeration boundary.

Controller creation has two non-overlapping operations. `open` proves the
Session key is absent at one exact revision and admits the first command over
that explicit genesis; `resume` requires an existing bounded Session current.
Neither operation silently converts missing durable state into the other.
The first Prepare must acknowledge the original absence-pinned source before
that opening pin is consumed. An unknown initial acknowledgement keeps the pin;
neither controller may join a subsequently created same-named Session by
rereading its latest revision.

## Keyed bounded state

`AgentSessionCurrent` is metadata only: state, stop reason, bounded Plan and
usage, sequence counters, message head/count, pending-input count, a bounded
non-terminal Tool capacity directory,
unresolved-occurrence count/generation, open-stream count/generation, and one
typed last-transition witness. It never embeds message, tool, elicitation,
occurrence, stream, or chunk history.

The remaining authorities are keyed independently:

- immutable message payload plus ordinal/order-head entry;
- update-ID alias and complete update digest;
- exact tool and elicitation currents;
- exact occurrence current with a bounded append-only recovery-observation
  list, plus a deletable unresolved ordinal index;
- exact stream current plus immutable ordinal chunks; and
- exact command and receipt entries.

Message history is read backward through a fixed `(message_head,
message_count)` source descriptor and StateRoot revision. A newer current
revision can read an older retained immutable prefix only by exact terminal
ordinal/head membership; the Session's latest head is not substituted for the
requested prefix. Unresolved occurrences are read forward through a fixed
index generation and revision. Cursors cannot reset the context reader's
cumulative entry or message-current byte budget. Complete page wire bytes have
their own independent bound, so splitting the same source into one-entry or
256-entry pages does not change which messages fit the cumulative scan. No
ordinary turn path materializes all historical messages or occurrences.

The protocol enforces these hard bounds before Store serialization:

- 256 entries and 4 MiB canonical bytes per ordinary page;
- 256 KiB per ordinary Agent value;
- 512 KiB per keyed current/read wrapper;
- 2 MiB per command and 10 MiB per semantic receipt;
- 256 entries per bounded content or Plan vector;
- 64 concurrently non-terminal Tools and 4 MiB of exact Tool-close charge;
- 64 append-only recovery observations per occurrence, with the final slot
  reserved for NotApplied evidence; and
- 4,096 entries / 16 MiB per pinned context scan capability.

A staged stream contains at most 64 chunks and at most 256 KiB of cumulative
canonical chunk bytes. Page builders stop at an item boundary; a query budget
which cannot fit one otherwise valid item fails explicitly instead of returning
a non-advancing cursor.

## Direct Session updates

Update IDs have independent keyed authority containing the digest of the whole
typed update. Reusing an admitted ID, reusing an immutable message alias,
repeating a tool state, or submitting a new command that does not change State,
Plan, or usage is rejected. Exact retry uses the retained command receipt, not a
second transition.

A Tool current can enter the Session only as `Pending`. Every later transition
keeps the same `tool_call_id`, operation, and immutable input while following
the closed lifecycle through permission, execution, and one terminal state.
The Session directory carries only each non-terminal Tool identity, exact
current digest, and before/Cancelled byte charge; the independently keyed
current remains lifecycle authority. Closing is not a generic metadata update: one bounded source resolves
that complete directory, and the sole close reducer writes `Closed` plus every
deterministic `Cancelled` Tool successor in the same command/CAS. Open streams,
unresolved occurrences, pending input, partial or reordered Tool sources, and
unbounded Tool-family scans cannot participate in close.

Generic Session updates cannot mutate elicitations. Input suspend/complete is
the only authority which changes an elicitation current, pending count,
Session state, M1 Wait, result Artifact, and Continuation together.

## Host occurrences and recovery

Every replaceable context, model, permission, tool, elicitation, or standalone
workspace call records an immutable request digest and complete
`AgentHostBinding`. The lifecycle is:

`Prepared -> Started -> Completed | Unknown | NotApplied`

Provider dispatch occurs only after `Prepared` is durable and this invocation
receives a verified fresh `Started` acknowledgement. A competing same-command
replay cannot dispatch, even at the same observed head; it reads the retained
current and returns a completed response or requires recovery. Losing a
Prepared or Started acknowledgement also requires recovery, while losing a
Completed acknowledgement returns its retained response on exact occurrence
replay. Any
ordinary error after Started persists Unknown and returns only
`HostOutcomeUnknown { occurrence_id }`; timeout or cancellation cannot claim
that dispatch did not apply. A lost result blocks redispatch. Recovery
exact-matches the retained binding and may only complete the original
occurrence, prove it did not apply, or preserve ambiguity. Context is narrower:
an unresolved recovery-time Completed snapshot cannot be admitted because the
recovery call no longer owns the original pinned message-reader evidence. A
Started Context is durably marked Unknown with one stable reason; an already
Unknown Context returns `HostOutcomeUnknown` without another write. A retained
terminal Context completion still replays directly, and NotApplied remains a
valid terminal proof. No serialized proof DTO substitutes for the original
reader capability. Each non-empty
reconciliation observation has a content-derived identity over the occurrence,
closed disposition, and exact evidence. The list is append-only and bounded to
64 total entries, with one slot reserved for terminal NotApplied proof. An
exact duplicate is a zero-write replay; a new observation advances the current
and remains readable after reopen. Generic occurrence and recovery paths reject
M1-owned workspace requests; their full lifecycle belongs to the workspace
command union.

Permission requests offer only the closed `AllowOnce`/`Deny` decision enum,
and a retained response must select one decision present in that exact request.
Arbitrary option strings are not a second policy-result authority.

The reference turn driver checks the bounded unresolved index (limit one)
instead of scanning all occurrences. Context selection receives a
`PinnedAgentMessageReader`; the host cannot change its revision or source
head/count, reset its cursor, or renew its cumulative scan budget. One turn
admits `1..=64` model rounds; the builder rejects zero or a larger value before
execution.
All six driver host operations and the standalone controller delegate their
Prepared/Started admission, provider dispatch, and terminal persistence to one
internal execution path. The driver keeps no second occurrence writer or
state-only dispatch fast path. Its no-ID calls create the next occurrence;
after a lost result, callers correlate and recover the original occurrence
instead of treating a new call as a retry.
The retained context response must echo the exact source message head and count
pinned by its request, and every selected message id/index/digest must
exact-match an entry actually returned through that single pinned reader. A
response cannot relabel content selected from another source descriptor, cite
an unread entry, or fabricate a persisted message binding.

## Streams and Resource retention

Opening a stream fixes its Session, target, and delivery authority. A message
target must not already exist; a tool target must be the exact in-progress tool
current. Open increments the Session open-stream index, and abort/finalize
removes it in the same transition. A closed Session can neither open nor retain
an open stream.

Staged delivery admits contiguous immutable chunks and finalizes their exact
ordered content into one message or tool update. External delivery admits no
chunks. Open pins the resolver binding plus exact media type, digest, and byte
size. Media type is the same at-most-255-byte lowercase ASCII type/subtype token
wire admitted by `cymule.resource/4`; parameters, controls, whitespace,
uppercase, empty tokens, and additional slashes fail before Open. Its serialized
Finalize command carries only Session/stream identity;
framework preflight derives a closed serializable intent binding source
revision/digest, Session, stream, command, target, resolver, and content. The
provider product remains non-Serde. The provider accepts only that intent,
publishes idempotently, and may return only Published with exact readback,
NotApplied, or Unknown.

The retained reservation intent target MUST equal the immutable stream current
target exactly. A separately self-consistent intent/reservation for another
message or Tool target is a cross-edge integrity failure even when Session,
stream, resolver, content, and all content IDs verify. The historical
`source_digest` is not recomputed through a second authority to enforce this
edge.

Before provider I/O Durable derives the semantic Resource handle, physical
retention family, and exact `ResourceProfilePin` from the immutable Open
content. One StateRoot CAS persists the publication reservation on the stream,
the role-free generation-bearing `Reserved` target claim, the `Reserved` pin,
its family count, and the Finalize command. Every ordinary Message/Tool write
and both staged and external Finalize paths exact-read that claim family. That
CAS competes directly with `BeginDelete`; whichever head transition wins makes
the loser a typed conflict before its provider call.

Only a freshly acknowledged reservation or NotApplied rearm owns one publish
call. Reopen of `DispatchClaimed` performs no publish. Dedicated reconciliation
requires the restored intent and calls only provider observation. Exact
NotApplied observation is persisted before a later rearm may claim one new
attempt. Every provider result carries the complete DispatchClaimed reservation
it observed and can settle only that exact attempt. Published readback is
reconciled against the retained reservation. Reconciliation rejects an already
durable `NotApplied` phase before provider I/O. Only an ambiguous final Store
acknowledgement becomes
`PublicationOutcomeUnknown`, while known source/reducer/CAS conflicts remain
their typed errors.
`PublicationNotApplied` carries the exact durable NotApplied reservation
generation; an intent alone is not absence proof.

A public Abort can retire an external reservation only after the provider's
latest `NotApplied` observation is durable. That Agent command carries the
exact reservation Resource currents in its bounded source and one typed
reserved-pin release in its outcome. Its single CAS clears the reservation,
marks the stream Aborted, decrements the Session open-stream index, changes the
target claim from `Reserved` to `Released`, changes the Resource pin from
`Reserved` to `Released`, and decrements the family's current count.
`DispatchClaimed`—including a provider result still reported as Unknown—rejects
Abort and preserves reconciliation authority. Generic Resource release cannot
consume this profile-owned reservation.
Both Abort Resource members are required-nullable on the persisted source and
effect wires: ordinary/staged Abort carries explicit null, while NotApplied
reservation retirement carries the complete source and release receipt.

External terminal finalization is one later StateRoot CAS over:

- command and semantic receipt;
- stream and Session/update/message-or-tool currents;
- the exact target claim promoted from `Reserved` to `Materialized`;
- exact `ResourceCatalogRecord` publication;
- the already content-derived `ResourcePinKind::AgentStream`; and
- promotion of the exact `Reserved` Resource pin to `Active` without changing
  the family obligation count.

Promotion binds the exact reserved pin, status, reservation origin, physical
family, and current active count. The count captured when the reservation was
created is historical receipt evidence, not a lower bound: a sibling pin may be
released before Published reconciliation without blocking promotion.

The shared Resource reducer rejects deleted/fenced families, duplicate pins,
wrong physical families, and released pins. Resource GC resolves the retained
Agent command/receipt reference before treating the pin as authority. A
caller-authored publication or a split Agent/Resource commit cannot become
terminal output.

## Input waits

Suspend and Complete commands bind `session_id`, `wait_id`, exact `run_id`, and
the complete structural `WaitOwner`. Suspend rejects an existing request alias
and a closed Session. Complete requires the exact pending request, validates the
accepted value against its retained local Draft 2020-12 schema, derives the
closed accepted/declined result, and exact-matches its
`cymule.wait-result/1` Artifact.

The checkpoint carries typed suspension/completion receipt references, owner,
Run, and result. Profile verification validates their self-contained shape;
every authoritative Durable commit/read additionally resolves and exact-matches
the underlying M1 receipts, Wait, Continuation, and result. Accepted JSON null
is distinct from decline. Every schema-required nullable member must be present
as either a value or explicit JSON `null`; omission is rejected.

## Workspace effects

Workspace commands contain semantic intent and exact M1 structural bindings,
not provider responses. StartEffect alone requires a dispatch lease request
containing the framework-derived occurrence owner, exact Run-scoped
`ClockObservationRef`, and positive TTL; StartAbort and both settlement phases
require explicit null. Runtime resolves the current Clock while its guard still
encloses the final CAS. StartEffect atomically stages Propose, Prepare,
CommitScope, AuthorizeRelease, and StartDispatch, then commits the resulting
Continuation, obligation, claimed outbox, lease, and Started Agent occurrence.
StartAbort obtains the active host binding without creating an Effect. Settle
commands resolve the original binding and observe that original provider
occurrence; callers cannot submit a Completed response, NotApplied evidence, or
Unknown evidence as commit input.

Source-only preflight prepares the full bounded inline Core Scope proof before
any provider binding or dispatch. StartEffect must own the current frame's final
Effect site and the exact Plan-derived Effect-args Artifact. A bound Scope result
is evaluated by the existing pure interpreter and admitted in the same CAS.
Abort is supported only when an unbound child Scope with no pre-existing Effect
neighborhood can unwind to its real parent. Root abort, required abort result
bindings, non-final Effect sites, and oversized inline closure fail before any
business provider I/O or durable mutation.

Only a fresh acknowledged Start dispatches, after its Clock guard has returned
successfully. The closed Submitted and Unknown submission results both leave the
occurrence Started; neither supplies terminal workspace evidence. A provider
error after that admission preserves Started and returns an unknown-outcome
error identifying the occurrence. Replaying the exact Start returns its original
receipt without binding, dispatch, observation, or Clock I/O. Explicit Settle
alone asks the original occurrence's pinned observer for a resolution.

An observer returns a non-Serde `AgentWorkspaceObservation`: the closed
resolution plus the immutable Artifact records it newly produced. Only actual
typed Artifact references are accepted, not apparent references inside generic
JSON. Durable combines exact parent reuse with the complete supplied records,
verifies reference/byte equality, uniqueness, and the aggregate 4 MiB material
budget, and admits new evidence with its terminal or Unknown observation CAS.
There is no separate evidence registration or pre-existing-evidence requirement.
The complete retained typed Artifact closure, including prior observations,
overlay, binding, and Effect result, has one shared 64 MiB raw-byte budget on
admission and receipt reads. A non-terminal occurrence reserves the exported
4 MiB observation-material limit and, only for its frozen M1 Effect-result path,
Core's 8 MiB Artifact limit for a legal terminal successor. Terminal phases use
their actual deduplicated closure size. A proposal exceeding its phase budget is
rejected before CAS; previous evidence stays unchanged and reopenable, and an
accepted Unknown retains enough material budget to settle Applied.
The same rule applies independently to the occurrence's 256 KiB canonical body.
The pure Workspace reducer encodes a Completed capacity probe using the exact
frozen change ID, commit decision, host binding, and the largest legal
ArtifactRef under Core's exported kind bound. Non-terminal admission and source
or receipt verification reject a body that could not retain that successor.
The probe only measures capacity: it is never an observation, an admitted
Artifact, or a real receipt, and no prior evidence is removed to make room.

Every `WorkspaceScopeCheckpoint` includes a bounded `AgentWorkspaceM1Witness`
binding Run, scope, terminal phase, Continuation digest, Effect intent,
obligation, and the exact closed M1 receipt ID. The Durable façade resolves that
receipt on commit and read and exact-matches scope, Effect, outbox, obligation,
lease, and Continuation changes. Generic Agent occurrence mutation cannot
advance any M1 workspace lifecycle state.

The M1 witness points to the real closed Agent-workspace coupled receipt. Its
StartEffect form retains the actual issued Clock observation so later store-only
reads can verify the original scope, reference, and TTL/expiry equation without
asking a current Clock provider. Command-bearing phases resolve their complete
ordered Core batch through authenticated hot or cold lookup. New evidence without
a semantic Core command uses the real material-only batch, whose material source
is non-empty; a fabricated empty command batch is never accepted.

`AgentWorkspaceCommitOutcome::Committed` carries the exact retained Agent
receipt. A fresh settlement whose Unknown evidence identity already exists
returns `Unchanged` with the current revision and occurrence, performs no CAS,
and creates no synthetic command or M1 receipt. New evidence remains an atomic
Agent/M1 transition.

## Wire and compatibility

Current persisted command and receipt selectors are `cymule.agent-command/4`
and `cymule.agent-command-receipt/5`; their complete-body IDs use
`cymule.agent-command-id/2` and `cymule.agent-command-receipt-id/3`; bounded
Session metadata is `cymule.agent-session-current/2`, and the current closed
schema generation is `cymule.agent/9`. Publication intent/reservation use `/2`
and `/3`; target
claims use `cymule.agent-target-claim-current/2` with key `/1` and identity
`/2` domains.
Recovery observations use their own content-ID generation. All persisted unions
deny unknown fields.
There is no reader or writer for the removed aggregate Session, recursive
journal-base, or stream-record formats. This profile is still internal, so the
terminal keyed model is a deliberate historical incompatibility rather than a
compatibility layer.

`schemas/agent-protocol.schema.json` covers the public Agent content,
host-interaction, bounded current, query-facing, and stream-command shapes. Rust
Serde/reducer tests additionally cover the complete persistence command/source/
receipt hard cut, the non-persisted `AgentCommit` envelope, and provider-product
exclusion. `AgentCommit` is a Rust persistence-facade result, not an Engine wire
branch or a branch of that JSON Schema. Its required-nullable freshness field
does not introduce a persisted identity or a new protocol generation.

## Validation

The profile is gated by:

```sh
cargo check -p cymule-profile-protocol --all-targets --locked
cargo test -p cymule-profile-protocol --lib --locked
cargo test -p cymule-agent --all-targets --locked
```

Durable integration tests must additionally prove late replay performs no CAS,
input/workspace receipt references fail closed when missing or mismatched, and
external finalization survives reopen/GC with its Agent-stream pin while a
delete-fenced family rejects a later finalization. Reservation and terminal
replay authenticate claim, pin, retention, and catalog sidecars; full audit
closes claims in both directions against every Message, terminal/non-terminal
Tool, and stream. Publication Unknown must
reopen through observe-only reconciliation with zero additional publish calls;
repeated recovery evidence must perform zero writes while new evidence remains
append-only and reopenable. Tests also release a sibling pin between reservation
and promotion, and prove NotApplied Abort releases its reserved pin while
DispatchClaimed/Unknown Abort writes nothing.
The SQLite process-death matrix stops at the real Store object-staged/pre-head
barrier and after committed CAS before acknowledgement for NotApplied Abort;
reopen must converge the same command with no provider call, authenticate both
Resource and target-claim sidecars, and make a second replay with zero writes.
Removing or tampering with any claim, pin, retention, or terminal catalog value
makes retained replay Integrity.
