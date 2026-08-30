# Durable Single-Domain Guidance

## Ownership and public authority

- This crate owns provider-neutral persistence and recovery. Concrete storage,
  execution, activation, clocks, and Resource providers remain adapters.
- `cymule-durable-protocol` owns Clock observations, Continuations and frames,
  execution claims, wait owners, activation identities, and shared bounds.
  Import those types directly; do not copy or canonically re-export them.
- Public mutation enters through the closed `DurableCommand` union or the
  typed Agent, Evolution, Virtual, and Resource control facades. Callers cannot
  submit a raw Machine command, target Continuation, delta, journal batch,
  StateRoot, prepared postcondition, or provider-authored authoritative event.
- `DurableStoreControl` owns provider-free M1 reads, cancellation, identified
  wait admission, Resource commands, and provider-registry-bound Evolution.
  It does not require a PluginHost or create an execution binding.
  Agent writes and Virtual claims belong to `DurableRuntimeControl`; their
  read-only facades do not gain writer or provider authority.
- `DurableRuntimeControl::virtual_work().claim(...)` is the sole fresh Virtual
  Claim entry point and returns the complete closed `VirtualClaimOutcome`.
  Generic Virtual `commit` rejects an absent Claim alias before Clock,
  provider, or Store mutation. It may return a receipt-only `VirtualCommit`
  only when the exact Claim command already has a retained all-ever alias;
  that replay does not load the executable Plan.
- `DurableRuntimeControl::open` consumes the one-shot
  `ExecutionBindingAdmission` produced before writable Store I/O. Do not
  repeat Describe or binding admission during open.
- `DurableCommand::is_read_only` is the sole wire classifier: Run index,
  current, wait/effect/occurrence/Attempt pages, and exact Run item queries.
  Additional Rust-only exact reads require semantic identities plus an exact
  expected revision and return that observed revision with a typed value.
  Never expose a generic family, arbitrary key, raw Plan reader, or history scan.

## Fixed-root persistence

- Ordinary open loads only the small Store head and its exact fixed
  `StateRootManifest`. The coordinator retains no complete Machine,
  DurableState, active-domain cache, or rebuilt global index.
- Every command loads one closed, bounded exact-key/read-page neighborhood from
  that pinned root. Core derives typed touched-key mutations; immutable maps
  copy changed trie paths and logs copy their authenticated append spines.
  Never defer a whole-domain traversal until the first command.
- A verified persistent-map range page retains each selected key's authenticated
  value-object identity. A query projection loads that exact typed value once,
  validates the complete leaf before projection, and then binds the authenticated
  map key to that leaf's primary identity. Never discard the range proof result
  and repeat an exact-key proof per item.
- The Run Wait page indexes a small `DurableWaitSummary`, derived in the same CAS
  as the complete global Wait. Paging loads only that summary leaf and never
  recompiles an Input schema; exact item, activation, and offline audit retain
  the complete Wait. Full audit closes both key sets and exact summary equality.
- Typed DurableOperation::Put* values are complete normalized postconditions.
  An unchanged projection is valid when Core events or sibling fields advance.
  StateRoot alone compares exact encoded values and lowers only real physical
  map differences; coordinators must not duplicate reads, clones, or filters
  for this purpose. Raw map puts still reject same-value mutations. Run query
  indexes propagate a parent descriptor only when its exact child MapRoot
  changes. Do not add per-type exceptions or a second global filter.
- `DurableState` is an explicit offline-audit/materialization DTO, not an
  ordinary runtime cache or mutation input. Complete projection traversal,
  cold-history audits, and GC are separately named offline operations.
  Full audit verifies reachability of every normalized profile root before
  materializing the audit DTO; an unmaterialized Evolution or Virtual family
  cannot hide a missing immutable child. Ordinary open remains fixed-size.
  With a base anchor, full audit also authenticates the complete cold segment
  chain, every independent Entry and Batch, and the cumulative command-index
  nodes and membership. Inline archive content cannot substitute for missing
  independently addressed objects. This traversal is not part of ordinary
  commands or fresh compaction's pinned Core-source preparation.
- A StoreBatch contains one exact immutable-object set and one small head CAS.
  Its transient delta is never a stored segment or a second revision
  authority. Every constructor remains crate-private.
- The physical token is an opaque monotonic CAS-lineage fence. Semantic
  integrity belongs to the pinned StateRoot and typed reachability checks.
  Do not pretend a reopened snapshot can reconstruct its discarded parent.
- Old state.json, cymule_state, checkpoint/segment generations, and older
  official provider generations fail with unsupported_store_generation.
  There is no current-type importer, mixed reader, or whole-state fallback.
- Raw immutable-object lookup is optional: a fresh-object existence probe may
  return None. A required reachable node/value missing from the authenticated
  graph is Integrity, not a semantic key miss or retryable NotFound.
- StateRoot leaves, objects, heads, GC receipts, and command-archive objects use
  their exported canonical-byte limits. Enforce limits before allocation.
  Large user payloads externalize through immutable Resources; a canonical
  Machine base uses the existing ordered, indexed bounded-chunk descriptor.
  A typed leaf carries exact canonical UTF-8 JSON text as a string, not an
  integer byte array. Base chunks retain raw bytes encoded as strict padded
  Base64 on wire. These codecs still enforce decoded byte bounds, canonical
  content, kind and identity; no legacy array-reader fallback is admitted.
- Verify the exact returned head, semantic revision, and physical token before
  advancing pinned authority or allowing provider I/O. Once publication may
  have committed, a missing, stale, foreign, or uncorrelated acknowledgement is
  CommitOutcomeUnknown. Reopen and resolve exact retained authority; never
  silently retry that error.
- Core commands, including singleton commands, use one complete immutable
  command-batch authority. StartRun uses its closed initial material admission.
  Paged begin/progress stages publish no terminal profile receipt; only the
  verified final aggregate batch publishes the derived profile sidecars.
- Failure and cancellation paging carries one Run-local terminal companion
  beside Core's sole persisted transition. It binds the exact source
  Continuation and query roots, then advances only the bounded outbox entries
  named by each Core Effect page. Finalization selects those completed shadow
  roots and publishes the small terminal receipt/Continuation/Attempt sidecar;
  it never rereads every Effect or overwrites another Run's intervening work.
  While that fence exists, ordinary same-Run M1 sidecars conflict. A retained
  declared failure resumes from its exact Core command and staged detail
  Artifact without invoking the provider or Clock again.
- Standalone Plan/Artifact material admission must retain its ordered Core
  replay authority and exact owning outer receipt. Do not preload all final
  materials when replaying historical command batches.
- Compaction preserves the exact Store-pinned Machine base anchor, cumulative
  command sparse index, archive head, and complete admitted batch membership.
  A hot command miss obtains authenticated current-root archive membership or
  absence; it never scans cold segments or admits identity reuse by default.
- `DurableStoreControl::compact_machine_history` is an explicit offline
  maintenance boundary. Its Rust-only request carries a semantic compaction ID,
  exact source revision, closed kind, and requested suffix, never target state.
  `EventPrefix` selects a causally closed Event cut; `EventFreeAdmissions`
  includes conflict admissions and material-only batches and requires suffix
  zero. The removed conflict-only name has no compatibility alias.
  Exact receipt replay precedes all current-source reads. Fresh preparation
  rejects nonempty pending-command or paged-transition roots before loading the
  complete Core source; only this operation may process the complete base.
  It never materializes `DurableState` or installs a whole-state runtime cache.
  Core's opaque prepared result owns the root delta and archive. The shared
  pinned publisher commits archive objects, base, head, and the one typed
  receipt together; it accepts no caller-authored archive or raw Machine delta.
- `history_compaction_head` is a required-nullable exact value-object pointer
  to the existing typed receipt, not another history map or authority. The
  primary receipt key, immediate parent archive, current base anchor, and this
  pointer close in the same CAS. Count material-only batches in cumulative
  archive validation and GC even when they contain no commands or Events.
  GC retains and exact-compares every independent batch in each retained archive
  segment; a command-index-only traversal omits zero-member material batches.
- A single-CAS receipt return is bound to its verified acknowledgement, not a
  subsequent current-head lookup. A legitimate later writer does not invalidate
  an acknowledged historical result. Execution and Effect claims likewise
  return only transition-derived acknowledged authority; before the next
  execution step or provider invocation, refresh the bounded current head and
  exact-verify that same claim instead of re-adjudicating its completed CAS.
  GC verifies its exact physical successor from the frozen head and receipt;
  uncorrelated acknowledgements remain Unknown.
- Explicit cold GC computes reachability outside writer exclusion and fences a
  physical-only head transition before deletion. Reconciliation completes only
  its exact current receipt; advancing starts the next bounded page. Preserve
  retained commands, batches, receipts, and all current roots.
- Reference-store synchronization is adapter-local and non-blocking. Surface
  contention as conflict instead of waiting on a lock.

## Run, frame, and component execution

- Empty-domain genesis is parameter-free Machine::new(). A new Run's Plan,
  creation ExecutionBinding Artifact, input, first Attempt, Running
  Continuation, and exact issued Clock receipt commit together.
- Start first derives only its command identity and performs an authenticated
  hot/cold command plus Run-key lookup. Command/Run double absence is fresh and
  proceeds to the Clock-owned admission without constructing a throwaway
  material stage. Replay validates the retained singleton batch and then reads
  the exact Plan, binding, and input before returning or continuing an already
  admitted terminal failure; it does not consult the Clock or provider.
- A Running Continuation has exactly one execution claim and matching active
  Core Attempt. Ready resume advances epoch and fence with a new Attempt.
  Ordinary resume of Running work is Busy before provider I/O.
- Expiry changes nothing by itself. Explicit takeover verifies the old fence
  and the exact current-scope Clock head, supersedes the old active Attempt,
  and installs the new claim atomically. There is no automatic takeover or
  renewal in the M1 baseline.
- The Clock's non-blocking current-head guard must still enclose the final
  claim CAS. Historical resolve is not commit authority. If the callback
  committed and the guard subsequently fails, return CommitOutcomeUnknown.
  Claims independently verify Run-derived scope and the exact TTL equation.
- Hot Clock receipts exist only while referenced by an active claim. Releasing
  or replacing its last claim removes that hot reference in the same CAS; the
  Clock provider remains historical receipt authority.
- Semantic invocation, scope, wait, Effect, and component occurrence identities
  derive from immutable Plan and structural control position, never worker,
  epoch, provider revision, or fence. Internal command IDs never include a
  physical StateRoot revision.
- Persist index-only Region paths, structural invocation paths, canonical
  Artifact references, lexical scope stack, and explicit next-step positions.
  Re-resolve the sealed Plan and every adjacent parent/child frame edge.
  Invoke frames may inherit the caller's Scope; use Core's shared pinned frame
  validator instead of requiring the Scope creator's invocation to equal the
  callee. Nested Scope frames still require their exact owned Scope.
- ExecutorCoreBoundary carries minimal intent only. The coordinator derives
  scope, input/result, next frame, wait, and terminal disposition from the
  sealed Plan and exact source. YieldReady requires the exact current Effect
  boundary or the complete explicit-release set.
- A component Call persists its semantic Pending occurrence and a new Running
  provider Attempt before I/O. Only the current invocation's successful new
  CAS returns Admitted. The same claim's retained Running Attempt returns
  InFlight and cannot call the provider again.
- Occurrence state is only Pending or Completed. Attempt state is separately
  Running, Superseded, or Completed. Takeover leaves the occurrence Pending;
  a later Attempt increments the ordinal and references its exact predecessor.
  A takeover acknowledgement lost before a new provider Attempt must remain
  recoverable when the latest retained Attempt is already Superseded.
- Successful results validate against the sealed component output schema and
  required output_artifact_kind. The same CAS retains output, completed
  occurrence/Attempt, and the unique derived successor Continuation.
  Expected failure instead commits declared detail, RunFailed, terminal epoch
  and fence, completed Attempt, and claim-free Failed Continuation together.
- Late results cannot pass another execution fence. A completed occurrence
  prevents reinvocation. A legacy Call interrupted before result checkpoint
  may repeat its cost only through an explicit later takeover; never describe
  this as provider exactly-once or route world effects through it.

## Waits and terminal work

- Wait identity and complete owner are re-derived from Plan, Run, invocation,
  Region, site, and step. Kind, source, schema, consume-once policy, and optional
  local bind are Plan authority, not caller-supplied semantics.
  Offline audit binds terminal Waits to the unique immutable origin selected by
  their Wait ID within that Run's authenticated Plan lineage. Migration must
  not reinterpret a completed or cancelled Wait under the new current Plan;
  pending Waits still require the current Plan and retained Waiting frame.
- Identified signal/timer admission is store-only. Its result Artifact, exact
  selected/applied/Ready receipt, completed waits, frame locals, and readiness
  commit together without provider execution, Clock access, or auto-resume.
- Source drivers use authenticated pinned pending-source pages. A target set
  selected in the current receive call obeys both the framework hard target
  bound and that caller's `max_targets`; an exact target set retained by an
  earlier receive obeys the framework bound but is not reinterpreted by a
  later caller's smaller limit.
  Check a retained activation receipt before consulting today's pending bucket.
  Exact redelivery retains the original targets, winners, and Ready Run set.
  Terminal selected targets are stable non-winners, never broadcast head-of-line
  blockers or Virtual wake authority.
- Input waits use the typed Agent input suspension/completion controls.
  Completion resolves the exact suspension receipt, validates the sole
  Plan-declared schema, and commits material, Wait, Continuation, Run current,
  and Agent receipts in one pinned transition. Generic signal/timer admission
  cannot resolve Input waits.
- Run execution and world settlement are separate axes. Completed requires
  settled Effects. Failure/cancellation fence execution, cancel only pending
  waits and unreleased Effects, and retain dispatch ambiguity as Unknown.
- Cancellation and external Effect resolution have independent typed exact-key
  StateRoot receipt families. They are not reserved generic journals.
  Receipt identity, requested command, actual terminal state, original claim,
  canonical Artifacts, exact Core command, and complete batch material close
  one authority, including after cold command compaction.
- Resource transfer and activation live in coordinator/resource_handoff.rs.
  Transfer validates exact source/target Runs, producer occurrence/latest
  Attempt, result Artifact, and target slot. Activation commits its Wait,
  Continuation, Run current, coupled receipt, and Resource index together.
  Historical replay authenticates the original receipt, not equality with a
  Continuation that may have legitimately advanced later.

## Effects and profiles

- Every outbox entry pins its origin Plan, exact ExecutionBinding Artifact,
  operation binding, and transitive provider closure. Dispatch and
  reconciliation never substitute current defaults for a lost historical
  implementation.
- The current mutable outbox payload lives only in the owning Run's
  authenticated query root. The global outbox map is an immutable
  intent-to-Run locator needed by intent-only controls; it never mirrors a
  dispatch payload or acts as fallback authority. Full audit closes locator
  membership in both directions and includes in-progress terminal shadow
  roots in reachability.
- Enqueue, authorize/claim with its exact lease, observation, reconciliation,
  and unavailability each pair the exact Core batch with the matching outbox
  mutation and Run current. Unknown never has a detached event or sidecar CAS.
- After root-frame dispatch advances the Store, re-read the exact pinned
  execution step and require the original claim before deriving CompleteRun.
  Re-evaluate its result from that fresh Plan/frame; never reuse the pre-dispatch
  revision, relax CAS, or redispatch the provider to finish the Run.
- Only eager observational Effects dispatch in an open Scope. Deferred effects
  wait for scope commit; explicit effects additionally require explicit release.
  A bound eager Effect retains its current frame until a settled result binds.
- Every Applied response is normalized to one canonical result Artifact.
  Missing JSON means JSON null, which must first satisfy the original pinned
  output schema. Invalid dispatch output commits Unknown; invalid reconciliation
  output leaves the original Unknown settlement unchanged.
- Every persisted Effect dispatch has a result exactly when its outbox state is
  `Applied`, and that result has kind `cymule.effect-result/1`. The StateRoot
  value decoder, exact lookup, page summary, receipt closure, and full audit use
  this one invariant; no terminal path accepts a missing or foreign-kind result.
- After dispatch begins, wrong variants, wrong attempt echoes, transport loss,
  cancellation, defects, and invalid outputs never grant redispatch authority.
  Preserve the original claim for reconciliation. Only framework-owned binding
  and manifest comparison may persist implementation unavailability.
- ResolveEffect uses only the original unknown intent and dispatch fence.
  The exact historical provider linearizes its terminal decision before the
  receipt CAS. It acquires no Run claim, reads no Clock, and never resumes a
  Run, including after failure/cancellation.
- Agent, Evolution, Virtual, and Resource mutations reduce typed exact source
  neighborhoods and commit their complete normalized postconditions through the
  owning facade. No profile accepts a caller-authored target Continuation,
  generic journal callback, or arbitrary Machine write.
- Agent external publication reserves its physical family and dispatch attempt
  before I/O. The reservation CAS also acquires the sole exact
  `(Session, target kind, local identity)` target claim. Every ordinary
  Message/Tool write and staged/external terminalization reads that same family;
  no open-stream scan or parallel target authority is permitted. The
  reservation CAS is the sole fresh publish authority; reopen
  observes `DispatchClaimed`, durable NotApplied may be rearmed by one later CAS,
  and published reconciliation promotes the reserved pin in the final Agent CAS.
  Promotion uses the exact current family count, so a sibling release after
  reservation cannot become a false lower-bound conflict. Public Abort loads
  required-nullable reservation Resource source, accepts only durable
  NotApplied, and commits stream/Session closure plus `Reserved -> Released`
  and family count decrement in that same Agent CAS. DispatchClaimed/Unknown
  remains reconciliation-only, and generic Resource release cannot bypass it.
  Retained Abort alias replay authenticates the terminal Released pin and
  physical-family current plus the Released target claim; missing or tampered
  sidecars are Integrity. Finalized replay likewise authenticates the
  Materialized claim, Active pin, current retention family, and exact catalog;
  a valid later sibling Resource-family receipt remains admissible. A
  reservation-phase stream read authenticates its original Open receipt plus
  exact retained unterminated Finalize command, not equality with the old Open
  projection alone. It also rejects a self-consistent reservation whose intent
  target differs from the immutable stream target; no replacement source hash
  or compatibility derivation substitutes for that direct edge.
  Known post-publication conflicts remain typed conflicts rather than world
  Unknown. Agent publication reconciliation retains its expected original intent and
  rechecks semantic touched keys. Unknown evidence is append-only; identical
  Unknown workspace observation is Unchanged with zero CAS.
- Agent context message pages pin an immutable append-only prefix by exact
  message count and head, independently of a later Session head. A missing
  cursor defaults to that source count. Each page obeys separate summed
  message-current and complete response-wire byte budgets; page size changes
  grouping only, never the selected prefix. Reads verify the source terminal
  entry and every reachable log value without a current-head fallback.
- The non-persisted Agent commit envelope carries required-nullable
  `committed_revision`: only a newly acknowledged CAS sets it to the observed
  revision; exact replay sets null. Provider dispatch must not infer fresh
  ownership from a revision difference or receipt presence. This flag never
  enters the immutable semantic receipt or its identity.
- Agent workspace Start requires its exact nullable dispatch lease authority
  and may dispatch only after a newly acknowledged Start CAS. Replay never
  dispatches again. Settle observes the retained occurrence; no-Core phases use
  a real coupled receipt with unchanged Core/neighborhood authority.
- Workspace coupling lives in coordinator/agent_workspace.rs and uses the
  existing typed coupled-receipt family. Its 19 required checkpoint fields
  bind the outer command and phase, source/result Core roots, paired nullable
  real batch IDs, issued dispatch Clock, existing Continuation content IDs,
  full target Continuation, and exact before/after Effect, outbox, and lease.
  It contains no physical result revision or self-referential Agent receipt.
  Material-only phases retain a real ordered Core batch; no-Core phases require
  unchanged roots and neighborhood, never a fabricated empty command batch.
  Its independent 12 MiB canonical bound permits the complete legal Continuation
  and both single-Effect neighbors without widening the generic receipt bound.
- Workspace retained evidence is bounded on fresh writes and historical reads
  by the exact Core read-set budget. Nonterminal occurrences reserve one full
  allowed final observation product and, for an Effect occurrence only, its
  canonical result Artifact. Exhausted evidence cannot prevent terminal
  reconciliation; large provider products externalize through Resources.
- Virtual claims use the Runtime's admitted complete ExecutionBinding; first
  binding material, capacity lease, occurrence, and any standard Evolution
  selection commit together. No separate binding registration is required.
  The public VirtualClaimOutcome returns a complete verified Plan only for
  Claimed, retained from its pre-CAS pinned source; NoWork carries only its
  receipt. Dedicated replay loads the exact receipt and original Plan in one
  pinned callback; generic receipt-only replay is available only for an exact
  retained Claim alias and never loads a Plan or creates a fresh Claim.
- Virtual providers are exact binding-pinned sources, migrators, and archives.
  Migration returns the full typed proposal with verified coverage and exact
  target-source Artifact bytes. Archive pin/release uses Resource's shared pure
  lifecycle reducer and the same outer Virtual receipt/CAS.
- Retry policy remains a pure content-addressed algebra. Restore verifies issued
  historical Clock evidence once; later reduction verifies only the new suffix.
  It cannot by itself claim a durable retry. Unknown-world Effects stop with
  their original intent retained for reconciliation.

## Verification

- Cover positive, illegal-transition, stale-writer, lost-acknowledgement, reopen,
  and exact replay paths. Fault sweeps inspect provider call counts as well as
  full offline state audit and Machine replay.
- Component tests include lost Attempt acknowledgement before I/O, lost result,
  lost result acknowledgement, successive takeover after lost takeover receipt,
  stale output rejection, and nested Invoke/Scope/Wait recovery.
- Keep deterministic in-process CAS sweeps separate from real SQLite
  child-process-death sweeps. Discover boundary counts from a successful run.
- Versioned public changes update schemas, fixtures, SDK contracts, the profile
  documentation, and the version registry. Do not claim complete conformance
  until the entire applicable suite and strict Clippy pass.
