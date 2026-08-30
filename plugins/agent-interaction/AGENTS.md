# Agent Interaction Guidance

- This directory is an optional profile. Agent Sessions, host occurrences,
  input, workspace, streams, and the reference turn driver are not Cymule core
  semantics and must not be re-exported by the core CLI or language SDKs.
- Wire DTOs, identity helpers, and deterministic reducers live only in
  `cymule_profile_protocol::agent`. The plugin may re-export or call them; it
  must not maintain a second reducer.
- Preserve the closed `AgentCommand`/`AgentCommandReceipt` mutation boundary.
  Never expose raw journals, records, StateRoot deltas, generic Durable
  transactions, prefix replacement, opaque serialized products, or all-history
  reads as Agent persistence authority.
- `AgentSessionCurrent` is bounded metadata. Messages, update aliases, tools,
  elicitations, occurrences, streams, and chunks remain exact keyed entries.
  Ordinary execution uses exact reads or revision/head/generation-pinned pages.
- Every message page binds the immutable source `(message_head, message_count)`
  descriptor. A current revision may read an older retained prefix only when
  the exact terminal ordinal still carries that head. Page wire bytes and the
  summed canonical bytes of returned message currents are separate hard
  budgets. Context adapters use `PinnedAgentMessageReader`; they cannot reset
  the cursor or renew the cumulative entry/message-byte budget, and page
  partitioning must not change which messages fit that cumulative budget. Each
  older page must continue the exact message-order head exposed by the
  previously verified newer page; a locally valid but disconnected page is
  corruption, not selectable context.
- Exact command retry resolves the retained semantic receipt. A late replay
  returns the current observed StateRoot head without writing; never persist a
  result-root identity inside the receipt.
- The non-persisted `AgentCommit` has required-nullable `committed_revision`.
  Only this invocation's acknowledged new CAS returns the observed revision;
  exact replay returns null, including at the same head. Validate the complete
  envelope before granting standalone provider dispatch. Receipt presence or a
  source/result revision difference is not fresh dispatch authority.
- `AgentError` preserves closed Conflict, Busy, Substrate, Persistence,
  RuntimeDefect, Integrity, Encoding, CommitOutcomeUnknown,
  HostOutcomeUnknown, ReconciliationRequired, ArchivedCommandReplayRequired,
  Cancelled, and TimedOut categories. Profile identity mismatches become
  Integrity with a stable code. Preserve original fields or explicit stable
  code/message pairs; never derive a lower-layer contract from its `Display`
  text.
- Paged Scope requirements retain Run, Scope and cardinality. Collection
  provider failures reuse the Durable typed mapping; neither category is
  inferred from prose or relabeled as corrupt proof bytes.
- `EphemeralAgentPersistence` is explicitly process-lifetime and never a
  controller default. It must continue rejecting M1 input/workspace and
  external Resource authority rather than emulating durability.
- The sole production writer adapter is the plugin-owned `AgentPersistence`
  impl for the Clock-carrying, provider-bound Runtime
  `cymule_durable::DurableAgentControl`. Store control exposes Agent reads only
  and must not implement `AgentPersistence`. Durable must not depend back on
  this optional plugin, and no generic transaction or fallback adapter may sit
  beside that typed writer.
- Controller construction is explicit: `open` requires a revision-pinned
  absent Session key, while `resume` requires an existing Session current.
  Never turn a missing resume target into implicit genesis.
- Both standalone controllers and the reference driver keep the original
  absence revision until the first Prepare acknowledgement verifies. An
  unknown initial acknowledgement never consumes that pin or permits joining
  a later same-named Session through a fresh current-head read.
- Generic Session updates must reject Elicitation. Generic occurrence and
  recovery paths must reject M1 workspace requests. Only the corresponding
  composite typed command may mutate those coupled domains.
- Provider products are non-Serde and framework-owned. External stream
  publication and workspace binding/settlement are resolved from the exact
  retained registry binding through specialized Durable methods; never accept
  a caller-supplied publication, host response, reconciliation result, token,
  trait object, or opaque bytes at commit time.
- External stream finalization first commits one content-derived publication
  reservation, `Reserved` Agent-stream Resource pin, and physical retention
  current before provider I/O. Only that fresh CAS acknowledgement, or a fresh
  rearm after durable NotApplied evidence, authorizes one publish call. Reopen
  observes a claimed attempt without redispatch. Reservation intent and stream
  current must retain the same exact immutable target; an independently valid
  foreign-target intent is not a stream authority. Published reconciliation then
  commits Agent current/receipt, catalog record, and promotion of that exact pin
  to `Active` in one CAS; it never increments the obligation twice. Promotion
  uses the physical family's current active count and does not retain the
  reservation-time aggregate as a lower bound, so unrelated sibling pin
  releases cannot strand the exact reserved pin.
- Public stream Abort may consume an external publication reservation only
  after its latest provider observation is durably `NotApplied`. One typed
  Agent receipt atomically clears the reservation, closes stream and Session,
  releases the exact `Reserved` pin, and decrements the current family count.
  `DispatchClaimed` and Unknown publication outcomes remain reconcilable and
  reject Abort. Its persisted Abort source and effect each retain a
  required-nullable Resource member, so omission cannot erase the owning
  release edge. Generic Resource release cannot consume an Agent reservation.
- Stream Open, Append, and Abort return the exact Agent commit. Finalize and
  observe-only reconciliation return the closed finalization outcome, which
  may retain an Unknown publication intent instead of claiming a commit.
- External finalization derives the semantic Resource handle, physical family,
  and profile-pin selectors before provider I/O. Durable exact-reads those
  Resource currents at the command source, persists the reservation on the
  stream current, and uses the reserved currents as the final receipt source.
  Provider products remain non-Serde throughout.
- External delivery pins expected media type, content digest, and byte size at
  Open. Media type uses Resource `/4`'s sole 255-byte lowercase ASCII
  type/subtype token grammar; parameters are invalid. Finalize derives one
  closed intent binding source revision/digest,
  Session, stream, command, resolver, target, and content. The intent is the
  only serializable recovery authority; provider products remain non-Serde.
  Providers accept only that intent, publish idempotently, and return a closed
  exact-readback observation. Unknown or an unacknowledged post-I/O CAS returns
  the same intent; known CAS/reducer conflicts remain typed errors. Recovery
  requires the intent, exact-matches the durable reservation, and uses the
  observe-only finalization path without calling publish again.
- Input checkpoints bind exact Run, full `WaitOwner`, response-derived result
  Artifact, and typed M1 receipt references. Workspace checkpoints bind exact
  Run/scope/phase/Continuation/Effect/obligation plus a closed M1 receipt ref.
  Durable authoritative reads always resolve those refs; SHA shape alone is
  not semantic proof.
- Workspace `StartEffect` alone carries a required dispatch lease request with
  framework-derived owner, exact Run-scoped `ClockObservationRef`, and positive
  TTL; every other phase requires explicit null. Runtime resolves that current
  Clock under the final CAS guard and commits the five ordered Effect stages,
  scope/Continuation, obligation, claimed outbox, lease, Agent occurrence, and
  typed M1 receipt atomically. A duplicate Unknown observation returns typed
  `Unchanged` with the exact current and performs no CAS or fake receipt.
- Workspace source-only preflight must complete the bounded inline Core Scope
  proof before binding or dispatch. `StartEffect` owns the current frame's final
  Effect site and its exact Plan-derived Effect-args Artifact; any Scope result
  is purely derived and admitted with the same CAS. Abort supports only a child
  Scope with no required result binding or pre-existing Effect neighborhood.
  Root abort and unsupported structural exits fail before provider I/O.
- Fresh Start dispatch occurs exactly once after the complete CAS and Clock
  guard succeed. `Submitted` and `Unknown` are non-terminal submission results;
  both retain Started. A provider error after admission is an unknown-outcome
  error naming the retained occurrence, never a same-request retry. Exact Start
  replay invokes no binding, dispatcher, observer, or Clock provider.
- Workspace observers return the closed non-Serde `AgentWorkspaceObservation`
  containing the resolution and complete newly produced Artifact records.
  Durable resolves omitted records by exact parent membership, rejects missing,
  duplicate, unrelated, or mismatched bytes, and enforces the aggregate 4 MiB
  material bound. New evidence is admitted with the Agent/M1 transition; a
  caller or provider must never pre-register it through a raw mutation seam.
  The complete retained typed Artifact closure has the same 64 MiB raw-byte
  budget on admission and exact receipt reads. A non-terminal occurrence
  reserves `MAX_AGENT_WORKSPACE_ARTIFACT_BYTES` for its future terminal
  observation, plus Core `MAX_ARTIFACT_BYTES` only when the frozen occurrence
  has an M1 Effect-result path. Terminal phases use their actual closure size.
  Reject an overflowing proposal before CAS without truncating evidence or
  invalidating the prior receipt; an accepted Unknown must remain settleable.
  Workspace non-terminal occurrence bodies also preserve the existing 256 KiB
  limit for a Completed successor. Profile derives that exact typed capacity
  from the frozen change, commit flag, binding, and maximum legal ArtifactRef,
  using Core's `MAX_ARTIFACT_KIND_BYTES`. Admission, source preflight, receipt
  replay, and `Unchanged` reads share this check. The serialization-only probe
  is never provider evidence, material, or a persisted receipt.
- Workspace M1 witnesses resolve the real `CoupledCheckpoint::AgentWorkspace`
  receipt and its exact hot or cold Core batch. Start receipts retain the issued
  dispatch Clock observation for historical scope/reference/TTL verification.
  Material-only observations use a real non-empty material admission batch;
  an empty fabricated command batch or a receipt-shaped digest is not authority.
- Preserve explicit required-nullable Serde members and `deny_unknown_fields`
  on persisted unions. Add both explicit-null success and missing-member
  failure tests for every new nullable wire field.
- Apply protocol size/count limits before Store serialization. A Store leaf
  limit is not a substitute for an Agent-domain validation error.
- Validate host requests before binding or I/O. Persist Prepared and Started
  before standalone provider dispatch; ambiguous outcomes block redispatch and
  require exact-binding reconciliation.
- Only a freshly acknowledged Started command may dispatch. A concurrent
  replay reads the retained current and never invokes the provider. Losing a
  Prepared or Started acknowledgement leaves that occurrence for explicit
  recovery; losing a Completed acknowledgement returns its exact response on
  retry without dispatching again.
- The standalone controller and all six reference-driver host operations use
  the same internal identified-interaction execution path. Do not restore a
  driver-local occurrence writer, lifecycle reducer, or state-only dispatch
  shortcut. The driver's next no-ID call creates a new occurrence; after an
  ambiguous result, correlate and recover the original occurrence explicitly.
- After Started is durable, an ordinary host error never returns its original
  timeout, cancellation, or host category as a terminal result. Persist Unknown
  and return `HostOutcomeUnknown { occurrence_id }`; only a closed NotApplied
  provider observation proves pre-dispatch semantics.
- A new Context or Model occurrence must exact-match the Session message head
  and count at its Prepare source. Later lifecycle closure may proceed over a
  newer Session current because the immutable admitted request already owns
  that complete source descriptor.
- An unresolved Context occurrence cannot accept a recovery-time `Completed`
  response because recovery no longer owns the original
  `PinnedAgentMessageReader` evidence. Started becomes Unknown with the stable
  plugin reason; an already Unknown occurrence returns `HostOutcomeUnknown`
  without a write. Existing terminal Completed replay, closed NotApplied proof,
  and non-Context recovery keep their ordinary behavior. Do not add a proof DTO
  or treat a caller-supplied snapshot as reader evidence.
- Host-occurrence transition identity binds the complete validated snapshot,
  including terminal response or recovery evidence; never derive an index
  generation from lifecycle state alone.
- Reconciliation evidence is an append-only, occurrence-bound observation list
  with content-derived identities and a hard 64-entry total bound. Exact
  duplicate evidence is a zero-write replay, new evidence advances the current,
  and one slot remains reserved for terminal NotApplied proof. Reopen must
  retain every prior observation; no latest-evidence overwrite is legal.
- ACP, MCP, A2A, concrete model/tool providers, editors, and loop policy belong
  in separate adapters. A catalog entry never grants permission or effect
  authority.
- Permission requests and responses share the same closed decision enum. A
  response may select only a decision explicitly offered by its exact request.
- A Tool current enters a Session only as `Pending`; every later update must
  preserve the exact `tool_call_id`, operation, and input while following the
  closed lifecycle. Session close exact-reads the bounded non-terminal Tool
  capacity directory and cancels every retained Tool in the same command/CAS;
  no generic metadata-only close or Tool-family scan exists. A context response
  must echo the request's exact pinned
  message head and count and may cite only id/index/digest-exact messages
  actually returned through that one `PinnedAgentMessageReader`; unread or
  fabricated references fail before the response is admitted.
- The reference driver hard-limits one turn to 64 model rounds.
  `with_max_model_rounds` is fallible and accepts only `1..=64`; do not restore
  an unchecked builder or a practically unbounded provider loop.
- Driver permission requests and Tool-result messages use the profile's typed
  Session/Tool/purpose content-ID helper before the first Pending write. Never
  concatenate a caller Tool ID into another bounded identity; the full legal
  512-scalar input must remain usable without truncation.
- The real SQLite `/6` process-death suite derives its Agent-owned CAS count
  from one successful terminal run through a test-only `DurableStore` wrapper,
  then kills a managed child after immutable objects are staged but before the
  head CAS and again after the underlying CAS returns but before the framework
  receives its acknowledgement. It covers every Session/Tool, host-occurrence,
  and staged-stream CAS; the AgentPersistence façade only records the exact
  recovery command and is not the fault boundary.
  Recovery first replays the exact recorded command receipt, then uses the
  current occurrence recovery API; it must never redispatch a retained provider
  result. Its Session-Close matrix separately retains Pending and InProgress
  Tools, kills immediately before and after the one Close CAS, and requires the
  same recorded command to converge to the exact no-death Closed Session and
  complete Cancelled Tool set without another replay write. Every case
  checkpoints WAL and repeats SQLite integrity checks.
- Use targeted `rustfmt` for owned Rust files. Do not run workspace-wide
  formatting in a shared worktree.
- Run profile-protocol all-target checks and lib tests, then the complete
  `cymule-agent` all-target suite. Durable/provider changes also require real
  reopen, late-replay/no-CAS, M1 receipt-resolution, Resource pin/GC, and
  delete-fence integration tests before the profile is considered frozen.
