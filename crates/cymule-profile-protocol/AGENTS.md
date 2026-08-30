# Profile Protocol Authority

## Ownership and dependency direction

- This crate is the sole authority for Agent, Evolution, Resource, and Virtual
  persistence DTOs, content identities, bounded typed source views, and
  provider-independent pure reducers.
- Shared Clock, Continuation/frame, execution-claim, wait-owner, and identified
  wait-activation contracts belong exclusively to `cymule-durable-protocol`.
  Import them directly; do not recreate a profile `common` module or re-export
  an alias from this crate.
- This crate depends downward on `cymule-durable-protocol`. `cymule-durable`
  depends on both crates to load exact state and atomically
  lower verified postconditions. This crate must never depend on Durable, a
  Store implementation, a provider host, or a profile adapter crate.
- Profile crates may re-export these contracts and implement concrete adapters
  or closed process wires. They must not copy reducers, DTO graphs, receipt
  identities, or persistence state machines.

## Closed transition contract

- Public persistence commands contain only semantic intent and explicit scalar
  optimistic preconditions. They must not carry caller-authored read sets,
  provider products, current snapshots, manifests, physical revisions, CAS
  tokens, or Durable-derived runtime authority.
- A reducer consumes an exact bounded typed view from one pinned root plus any
  non-serializable source authority constructed by Durable. It performs no I/O,
  provider invocation, clock read, retry, rollback, or hidden lookup.
- Missing exact membership or non-membership is a typed read requirement. The
  caller may extend the same pinned view and retry preparation; it may not scan
  history or fall back to an unpinned head. Virtual uses
  `VirtualStateRead`/`VirtualPreparationError::ReadRequired`; a proven absent
  key and an unread key are never represented by the same value.
- Reducer output is a deterministic bounded typed postcondition. Durable alone
  resolves providers after deterministic preparation and commits the complete
  postcondition with one CAS. Failure before that CAS has zero durable writes.
- Current state is normalized into bounded exact-key leaves. All-ever command
  aliases and semantic receipts are exact keyed records; ordinary reads never
  replay history. Offline audit traversal is a separate explicit surface.
- Virtual claim persistence retains only the exact Plan identity and execution
  binding reference. The public `VirtualClaimOutcome` is a closed `NoWork` or
  `Claimed` projection; only `Claimed` carries a non-null claim and complete
  verified `SealedPlan` loaded by Durable from the same pinned root. Do not add
  a raw Plan reader, nullable public Plan, or Plan bytes to the persistent
  receipt.
- Virtual parked M1 Wait reasons use exact Wait content IDs. The bounded
  `VirtualCurrent` frontier owns their complete capacity directory: each entry
  retains the exact ParkedIndex/Parked/Work source-byte charge and the exact
  future wake-mutation byte charge. Parking must prove that waking every
  retained Wait reason together fits both aggregate item and byte bounds before
  CAS. Activation intersects its applied Wait IDs with that directory, loads no
  unrelated negative keys, recomputes every selected charge from exact leaves,
  and removes the selected directory entries in the same transition.

## Identities, receipts, and limits

- `cymule.resource/4` owns one public `validate_resource_media_type` authority:
  exactly two non-empty lowercase ASCII RFC-token subsets separated by one
  slash, with no parameter, whitespace, control, uppercase, or additional
  slash. Resource stores reuse that validator; they never copy or widen it.
  Resource `/3` and framework Resource Handle `/3` have no reader or migration
  fallback.
- Content-derived IDs bind the complete semantic body they name. Receipt IDs
  bind the exact parent source, semantic command, outcome, and ordered typed
  mutations, but never their own result current, physical manifest, revision,
  or CAS token.
- Agent command and receipt generation `/4` use identity domains `/2`; each ID
  preimage includes its current selector. Never reuse a predecessor command or
  receipt identity across a reducer-semantic hard cut.
- Every leaf, command, receipt, source view, postcondition, page, and fanout has
  a checked count and canonical-byte bound. Account keys, negative lookups, and
  non-serializable source authority in aggregate accounting before provider
  execution.
- Agent streams retain a required `final_update_bytes` counter for the exact
  prospective AgentUpdate wrapper. Open derives it with the same constructor
  used by Finalize and rejects an over-limit wrapper before any provider call.
  Staged Append adds canonical array contents and cross-chunk separators exactly
  and retains the separate required `staged_content_blocks` counter; more than
  256 terminal blocks or 256 KiB of final update bytes is rejected before an
  immutable chunk is stored. In-progress Tool identity, operation, input, and
  presentation fields cannot change without a terminal lifecycle transition,
  so Append needs no extra target read or history scan. External Resource Open
  uniquely seals one Object Resource Handle from the declared media type,
  content digest, and size, with no inline value, manifest, or annotations.
  Provider publication may add only resolver-bound locations; its semantic
  Resource must equal that prederived Handle exactly. Finalized current and
  receipt reads recompute the full update byte count for both delivery modes.
  A finalized current also proves that its Message or completed Tool update
  matches the immutable stream target exactly and recomputes `content_digest`
  from that update's exact content array. This digest uses the canonical raw
  64-character lowercase hexadecimal grammar, never the `sha256:` content-ID
  grammar.
  External streams keep staged bytes and content blocks zero, while their final
  update byte counter remains nonzero. The independent Agent target-claim
  current is keyed by Session plus role-free Message/Tool target and advances
  through `Reserved`, `Materialized`, or `Released` with a monotonic generation.
  Every non-genesis generation binds its immediate predecessor claim and
  admitting command IDs, so higher generation alone is never lineage proof.
  Generation is capped at 64, bounding any receipt-linked replay; exhausted
  reuse requires a new Message or Tool identity.
  Direct Message and terminal Tool writes materialize it; non-terminal Tool
  writes reject Reserved/Materialized; Session Close materializes every
  Cancelled Tool in claim-key order. Before provider I/O, an external Finalize
  persists its content-derived publication reservation, `Reserved` target claim,
  and `Reserved` profile pin in the same physical retention family. Only a
  fresh reservation or NotApplied rearm acknowledgement owns one publish call;
  reopen observes a claimed attempt. Every provider publication result retains
  the complete DispatchClaimed reservation it observed, including attempt; a
  late result cannot settle a rearmed attempt, and reconciliation rejects a
  durable NotApplied phase before provider I/O. A public NotApplied outcome
  carries that exact durable reservation generation, not an intent-only claim.
  The reservation intent's
  immutable target must equal the stream current target exactly, in addition to
  matching Session, stream, resolver, and content. Terminal finalization
  promotes that exact reservation without changing the family obligation count.
  Promotion binds the family's current
  count, not the reservation-time aggregate as a lower bound. A public Abort
  carries required-nullable Resource source/effect members: only durable
  NotApplied may atomically clear the reservation, advance the claim to
  `Released`, release the Resource pin from `Reserved` to `Released`, decrement
  the current family count, and close stream/Session.
  DispatchClaimed or Unknown rejects Abort. Generic Resource release cannot
  consume that profile pin. The staged-chunk byte limit is unchanged.
- Agent Session close is the sole reducer authority for terminal Session state.
  Session metadata retains one bounded non-terminal Tool capacity directory;
  close exact-reads that complete directory and atomically writes every Tool's
  deterministic `Cancelled` successor with the `Closed` Session. A generic
  metadata-only Closed update, a partial Tool set, an unbounded family scan, or
  a Closed Session retaining a non-terminal Tool is invalid.
- Context requests and snapshots pin one exact immutable message-history prefix
  with required `source_message_head` and `source_message_count`. Count is a safe
  integer and zero exactly when the head is null; responses echo both fields and
  initial Context or Model occurrence admission matches both against the Session
  current. Message page queries and pages retain that same prefix descriptor.
  `max_message_canonical_bytes` bounds the checked sum of returned
  AgentMessageCurrent bytes, while `max_canonical_bytes` independently bounds
  the complete page-read wire; neither budget substitutes for the other.
- Agent Unknown occurrences retain at most 63 recovery observations so the
  64th slot remains available for terminal NotApplied proof. The occurrence
  validator owns this state-dependent capacity for helpers, public snapshot
  commands, current reads, and receipt replay alike.
- These Agent bounds and the required counters belong to the current unfrozen
  internal `cymule.agent/9` hard cut. Omitted counters, omitted target-claim
  sources, omitted publication reservations, and historical aggregate stream
  shapes have no default, compatibility decoder, or migration fallback;
  freeze the final schema digest with this release generation.
- Required-nullable fields reject a missing JSON member and accept explicit
  `null`. Never use Serde defaults as a compatibility path for a required
  protocol member.
- Protocol constants change whenever the accepted JSON shape, identity input,
  state-family meaning, or reducer semantics changes. Update schemas, SDKs,
  fixtures, migration notes, and exact positive/negative tests in the same
  hard cut; do not retain dual-version shims unless a separately owned external
  compatibility boundary explicitly requires one.
- Resource `Conflict`, `Substrate`, `Persistence`, and `Integrity` errors carry
  separate required machine-readable `code` and human-readable `message`
  fields. Reducers and adapters assign stable explicit codes at the source;
  callers must not synthesize generic codes or parse codes from messages.
- Core and Durable Protocol failures cross profile boundaries through a closed
  lossless category map. Preserve not-found and illegal-transition semantics,
  command-reuse conflict codes, causal and pinned-read integrity codes, exact
  archived-command replay fields, identity mismatch, and encoding; never map
  an unmatched variant through `Display` or generic validation.
- Scope pagination requirements preserve their exact Run, Scope and source
  cardinality across protocol boundaries; they are not provider failures.
- Collection provider failures retain the canonical collections
  `ProviderFailure` payload unchanged across both lower error boundaries.
  Preserve its closed category, stable code, and revision/history evidence;
  this pure error DTO dependency introduces no resolver or provider I/O.

## Verification

- Test deterministic replay, same ID with different content, missing exact
  reads, capacity boundaries, cross-partition/revision rejection, Artifact
  closure, and mutation/receipt tampering in this crate.
- Durable tests own CAS conflict, storage failure, crash, reopen, lost-ack
  replay, provider non-invocation on replay, and zero-write failure proofs.
- Keep production and tests free of raw Durable transactions, journal records,
  generic state deltas, full-snapshot replay, and compensating rollback seams.
