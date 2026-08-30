# Cymule Project Handbook

## Authority

Use this precedence order when guidance conflicts:

1. Executable tests, frozen schemas, and live code.
2. The nearest `AGENTS.md` in the target path.
3. `docs/specification.md` for normative semantics.
4. `docs/architecture.md` for realization guidance.
5. Other documentation.

## Project invariants

- Keep the trusted semantic core in Rust and intentionally small. The core owns
  canonical identity, admission laws, deterministic reduction, and replay. It
  must not depend on a database, queue, network client, model SDK, object store,
  process supervisor, or UI framework.
- Canonical truth consists of admitted sealed Plans, immutable Artifacts,
  complete ordered command batches, receipts, admissions, and causal Events.
  Staged proposals are not admitted authority. Views, indexes, graphs,
  attention items, and schedulers are rebuildable projections.
- An Actor identifies command provenance, not authentication or capability
  authorization. Embeddings own those policy boundaries; neither an Actor
  string nor a coordination lease grants permission by itself.
- Every raw JSON ingress uses the shared duplicate-rejecting decode contract
  before typed deserialization. CLI, SDK, plugin, canonical Artifact, and
  persisted-state readers must never pass wire bytes through a permissive
  object parser or a fallback decoder.
- Every Artifact reference pins `cymule.artifact/2`; its sole identity authority
  is the core length-prefixed helper. Typed JSON references additionally pin the
  exact content-addressed Artifact type contract in their kind. Opaque Artifact
  bytes remain schema-free. Do not retain v1 identity or snapshot fallback paths.
- Cross-Run resources use provider-neutral `cymule.resource/4` semantic
  descriptors. Locator sets are separate replaceable records; signed URLs,
  access grants, and credential revisions never enter Resource identity. Exact
  list operations require a content-addressed manifest descriptor and verified
  per-page inclusion proof. Never persist credentials in an Artifact or claim
  exact replay for a mutable external locator without immutable version or
  content evidence. Concrete object stores, drives, sandboxes, and URL fetchers
  belong behind resolver/store plugins.
- Framework-owned typed Artifacts use the closed exact
  `ArtifactTypeContract` set. Resource handoffs pin the producer Run,
  occurrence, and exact result Artifact. Retention uses idempotent pin, release,
  GC, deletion, and staging/chunk cleanup receipts; deletion and cleanup require
  provider absence readback. Pin/GC/delete authority is the physical retention
  key derived from the immutable store binding plus content digest, not the
  annotation-sensitive semantic Resource ID, so two descriptors sharing bytes
  cannot collect each other. Lifecycle authority is the closed typed Resource
  command receipt plus keyed retention, pin, and deletion current maps; there
  is no global lifecycle journal or normal-operation history replay. Generic
  Resource commands may release only explicit pins. Virtual archive retirement
  owns its profile release in the same CAS as terminal profile state. External
  Agent streams first persist one physical-family publication reservation and
  reserved profile pin before provider I/O. That CAS also acquires the sole
  generation-bearing Agent target claim keyed by Session, target kind, and
  local identity. Ordinary Message writes, every Tool lifecycle write, Session
  Close, staged Finalize, and external Finalize all exact-read that same claim;
  no stream scan or second target authority exists. Only a freshly acknowledged
  reservation or rearm may publish. Finalization atomically promotes that exact
  claim from `Reserved` to terminal `Materialized` while promoting the exact
  reservation to the permanent pin, catalog, and terminal stream state. After
  a provider-proved durable `NotApplied`, stream Abort is the sole reservation
  abandonment authority and atomically advances the target claim to `Released`,
  closes the stream/Session, and moves that exact pin from `Reserved` to
  `Released`; an unresolved attempt cannot be aborted. Released claim tombstones
  advance generation on later reuse, preventing ABA.
- Every Run-to-Run Resource transfer has one exact keyed current authority, one
  target-Run slot map, and one payload-free entry in that target owner's
  persistent-log index. Exact lookup addresses the keyed authority directly;
  incoming enumeration resolves one bounded index page and never scans or
  mirrors a generic/global handoff history. Activation uses its own keyed
  current authority and per-target persistent-log index and exact-references
  the retained source-transfer receipt.
- Plans describe meaning and requirements. Runtime bindings describe concrete
  realization. Never place provider names, credentials, endpoints, or deployment
  topology in canonical plan semantics.
- Every `cymule.ir/3` component contract declares one required
  `output_artifact_kind`. The durable Call boundary validates the provider JSON
  against the declared output schema before canonicalization and stores the
  result only under that declared kind; there is no default kind or parallel
  legacy result Artifact.
- Every persisted occurrence pins an immutable occurrence binding. M0 persists
  Attempt and Effect bindings; new component or plugin-owned domain occurrence
  records must add the same protection before claiming exact execution replay.
  Updating future defaults must never reinterpret historical work.
- Semantic invocation, scope, wait, Effect, and component occurrence identities
  are derived from the admitted Plan and structural control position; execution
  epochs, claim fences, provider revisions, and worker identity never enter
  those identities. A different Plan always creates different semantic
  occurrences. The current acyclic IR has no caller-authored iteration key.
- Scope closure commits declared state and transfers effect obligations. It does
  not claim that the external world has settled.
- Only observational effects may dispatch eagerly while a scope is open.
  Commit-gated effects wait for their owning scope; explicit effects remain
  prepared until a caller issues the release control after commit.
- An ambiguous dispatch becomes `unknown` and follows reconciliation. Never turn
  it into a fresh semantic intent or silently redispatch it.
- Durable Effect stages validate an exact canonical Machine delta against their
  outbox mutation. Enqueue, dispatch-start claim, observation, and reconciliation
  may not carry unrelated Plans, Events, commands, or Artifacts; `Unknown` Event
  and outbox state commit together.
- External signal and timer delivery is an identified durable activation, not a
  direct worker wake-up. Match the Plan-declared source, commit the activation
  receipt with wait results and Continuation readiness, and advance the epoch
  before a reopened `Ready` Continuation resumes.
- Durable Run creation publishes the initial Machine and first Continuation in
  one CAS. Never create canonical work that cannot be resumed after a lost
  acknowledgement.
- `StartRun` admits its exact Plan, input and binding material, `RunStarted`,
  the initial `AttemptStarted`, and the Running Continuation in one CAS.
  Command receipts retain ordered `event_ids`; a command is not restricted to
  one Event. Composite commands share one persistent ordered batch record and
  exact material admission. Hot lookup, cold replay, compaction and GC must
  preserve that batch, never reconstruct singleton receipts from a later head.
- A Running Continuation always carries one durable execution claim. Initial
  claim or Ready resume, continuation Attempt start, Running state, logical
  Clock evidence that is current for the Run scope, and fence advance share one
  M1 CAS. An unexpired claim makes every other resume busy before provider I/O.
  Persisted Running recovery is an explicit expiry-proven takeover pinned to
  the exact old fence and a new current-scope Clock head; expiry alone changes
  nothing, and there is no automatic renewal. Exact older Clock receipts remain
  resolvable only for historical replay and retry verification.
- Legacy component Calls persist their semantic occurrence and provider Attempt
  before provider I/O. Result checkpoint validates the active execution fence.
  Takeover keeps the occurrence and supersedes the old Attempt. A subsequent
  admission creates the next Attempt; late output from the old fence cannot
  commit. Only a freshly admitted Attempt permits provider I/O. An existing
  Running Attempt returns `InFlight`, including after a lost CAS response, and
  must not invoke the provider again. Occurrence Pending/Completed semantics
  remain separate from Attempt Running/Superseded/Completed state.
- M1 storage moves one small CAS head over one fixed content-addressed StateRoot
  manifest. Coordinators lower closed typed operations into bounded persistent
  map/log path updates; the manifest roots the active projection while all-ever
  journal and receipt history remains behind typed exact-key lookup. Reopen
  authenticates only the small head and fixed manifest; ordinary operations
  resolve bounded exact-key neighborhoods on demand. Never restore per-mutation
  whole-state clone/diff/hash authority, a StateSegment/checkpoint/suffix chain,
  provider projection reads under CAS, arbitrary head/GC injection, or mixed
  legacy fallback. Pre-StateRoot `state.json`, `cymule_state`, and checkpoint/
  segment physical generations fail with `unsupported_store_generation`; no
  current-type legacy importer exists.
- A Run Wait page reads only the same-CAS `DurableWaitSummary` leaf retained in
  that Run's query index. It never loads or compiles the complete Wait or its
  Input schema. The complete Wait remains authoritative only in the global
  exact-item, activation, and explicit full-audit paths; full audit proves the
  summary and complete Wait projections agree in both directions.
- Machine history compaction replaces only a causally closed Event prefix with
  an authenticated base projection and exact identities. Retain the complete
  suffix and command receipts; CAS lineage and replay must survive stale writes
  and lost acknowledgements.
- Machine restore requires bidirectional closure between every retained or
  compacted Event identity and exactly one applied command receipt. A retained
  Event's command ID and command hash must match that receipt's command record;
  conflicts never claim an Event.
- Compacted-prefix authentication cumulatively binds ordered Event identities,
  command identities and semantic hashes, complete command-record digests, and
  the base projection digest. Restore recomputes this evidence; a shape-valid
  digest string is never authentication.
- Signal and timer transport belongs behind `WaitSourceDriver`. Drivers select
  only from the rebuildable parked-wait index, page indexed source identities
  fairly instead of scanning a fixed transport prefix, and acknowledge only
  after durable target selection and the activation CAS. A current-call
  selection obeys its caller bound; a previously retained selection obeys the
  framework bound without being reinterpreted by a later smaller caller bound.
  Lost acknowledgement redelivers the identical activation identity and
  targets. `cymule.wait-activation-receipt/3` retains that complete selection,
  the newly applied wait subset, and the original ready-Run set. Targets already
  completed or cancelled are terminal nonwinners, never a broadcast HOL block
  or an M3 wake authority.
- The official activation adapters admit only the exact physical generations
  `cymule.activation-http-spool/2` and `cymule.activation-timer-store/3`.
  Their selection-aware partial indexes are part of the fixed DDL authority;
  `/1` HTTP and `/1` or `/2` timer databases have no reader, importer, alias,
  or in-place repair path. Retained internal-test state must be drained and
  recreated through the current public APIs under the registered runbooks.
- Virtual-work cursors and bounded scheduler frontiers live in normalized
  keyed StateRoot families. Each typed transition has a hard read/write and
  encoded-size bound; never reconstruct a whole scheduler or repeat a complete
  `VirtualSnapshot` in every record. Activation and the corresponding indexed
  wake transition commit atomically.
- M3 exposes no generic post-initialization checkpoint. Initialization admits
  one previously absent scheduler; every later change enters through its
  closed typed command and receipt.
- Every virtual region pins the exact RegionSource operation, adapter binding,
  and revision. New payload ArtifactRecords, cursor, and frontier share one CAS;
  a wrong adapter generation or dangling payload advances nothing. Source
  generation changes only through verified region migration.
- Every virtual-work claim creates a binding-pinned, epoch-fenced occurrence
  before execution. Public durable claims carry a typed ExecutionBinding
  ArtifactRef; Rust derives the Machine Plan and admits that binding before the
  occurrence exists. The public result is the closed `VirtualClaimOutcome`:
  `NoWork` carries only the exact normalized receipt, while `Claimed` also
  carries the non-null claim and complete verified `SealedPlan` loaded from the
  same pinned StateRoot. The persisted receipt retains only Plan identity and
  binding reference; no raw Plan reader or nullable public Plan exists.
  Success, retry, park, failure, and cancellation are closed dispositions; a
  retry creates a later occurrence and never rewrites history.
- Durable component occurrences close as `Succeeded` or `ExpectedFailure` in
  the same CAS as their post-call Continuation disposition. Run execution
  (`Active/Completed/Failed/Cancelled`) and world settlement are separate axes,
  but `Completed` requires every Effect to be settled. Failure and cancellation
  fence execution and cancel only waits and Effects that have not begun
  dispatch, while dispatched ambiguity remains reconcilable only through
  `Reconcile`.
- A higher-profile Virtual claim receipt binds the complete normalized
  coupled transition. Replay compares its exact typed selection, claim and
  material identities; a callback-returned subset is never authority.
- Weighted fairness applies to materialized, capability-compatible backlogged
  Runs. Persist integer weights, deficits, dispatch sequence, and ready age;
  region materialization uses a separate round-robin visibility guarantee.
- Region split/merge treats cursors as opaque. A pinned migration adapter must
  verify coverage evidence before one CAS retires sources and activates targets;
  old regions and already materialized work remain historical authority.
- Completed virtual history may move cold only behind an immutable byte archive
  interface. Rust computes and verifies a semantic manifest Resource descriptor,
  causal-cut certificate, summary digests, retained terminal fences/bindings,
  and replay availability; selected rehydration verifies an index-bound Merkle
  proof. The current Resource-backed adapter reloads and verifies one complete
  immutable manifest capped at 8 MiB before returning the selected entries; it
  does not claim range-only physical I/O. Archive locator/proof
  catalogs and credentials are plugin concerns. Hot state retains the
  descriptor, proof root, and certificate, never cold manifest bytes.
- Multi-worker M3 execution uses abstract capacity-slot leases, not worker-pool
  topology. Claim and lease admission share one M1 CAS; renewal advances the
  slot fence; normal resolution carries work/lease epochs and an issued
  current-head Clock reference; expired recovery is an explicit
  retry/fail/cancel command with the same Clock authority. Lease expiry alone
  never mutates state, and old worker output loses after expiry or takeover. A
  coordination lease owns only its coordination resource and fence; it is not
  capability authorization.
- A current-head Clock check authorizes a mutation only while its non-blocking
  guard still encloses the final Store CAS. Historical resolution may stage a
  proposal but never authorizes commit. This applies to M1 execution claims and
  every M3 claim, renewal, recovery, and resolution path.
- Public mutation enters through typed commands with idempotent IDs and causal
  preconditions. Raw canonical event append is internal only.
- One public Run identity contract crosses Core, Engine, M1, M3, M4, schemas,
  and every SDK: 1..=512 Unicode scalar values with no control character.
  Internal command, Continuation, capacity-slot, archive-write, and other
  derived identities use their typed content-ID domains; never concatenate a
  caller Run ID into another bounded public identity.
- Prefer optimistic CAS, immutable records, idempotency, fencing epochs, and
  partitioned single-writer authority over locks. Core semantics must never
  depend on a blocking lock. A concrete adapter may use narrowly scoped,
  non-blocking writer exclusion only when required to implement its CAS contract
  and must surface contention instead of waiting indefinitely.
- Cross-language SDKs author the same frozen IR and use the same engine contract.
  They must not implement a second reducer or invent language-specific semantics.
- `cymule.engine/5` is the only CLI Engine transport. Every request and every
  success or failure uses its versioned envelope; v3 is rejected without shape
  fallback. Stderr and process status are transport diagnostics, never a second
  semantic error channel. A missing response never implies that retrying a
  potentially mutating request is safe.
- The official Engine Store selectors are the terminal physical generations
  `cymule.directory-store/5` and `cymule.sqlite-store/6`. They have no alias or
  reader for any predecessor generation, including `/4` and `/5`; custom
  providers use a distinct provider identity.
- Engine clients accept exactly one success response or one failure object.
  Every success requires the complete inner `EngineRequest` actually accepted by
  the strict decoder plus the closed response. SDKs compare that echo with the
  exact JSON value they serialized and sent before interpreting the response;
  a predecessor success with no request echo or any mismatch fails closed. A
  failure contains no request because transport or strict decoding may fail
  before one exists. Success tags, nested execution outcomes, and returned
  evolution commands remain closed unions.
- Exact echo equality preserves the complete normalized JSON structure:
  omitted and explicit `null` are different wires, arrays retain exact length
  and order, and scalars cannot change. Comparing only after an
  optional/defaulting or set-like typed decode is insufficient when that decode
  erases members, collapses duplicates, reorders elements, or synthesizes
  defaults. Every typed Engine ingress strict-decodes and integer-normalizes the
  raw value, then requires its typed reserialization to be structurally equal.
  Required nullable members remain valid because typed serialization retains
  them.
- Engine request echo is the single transport-correlation mechanism for Seal,
  Resource sealing, verification, Clock observation, durable control,
  cancellation, execution, and live evolution. SDKs must not recompute a Rust
  Plan ID, Resource ID, Clock scope, rollout decision, or other derived result to
  decide which request a success belongs to. The echoed request does not replace
  recursive response validation or an operation's durable receipt. A missing or
  mismatched echo after a mutating request begins is
  `unknown_world_outcome/reconcile`; for a non-mutating request it is an invalid
  response with no inferred replay permission.
- Public custom transports expose one complete exchange seam: the SDK supplies
  the strict normalized inner Engine request, and success returns that complete
  accepted request plus the closed response. Operation-specific bare payloads,
  selected identifiers, or command subsets are not a transport result.
- The compact Engine request envelope is bounded to 64 MiB, a success payload
  to 64 MiB, and the complete response envelope to 128 MiB plus its exact
  32-byte compact-framing delta. SDK stdout retains
  only that response bound plus one overflow byte; diagnostic-only stderr has
  its independent 1 MiB bound. A complete valid failure is admitted before
  process exit status, while success requires a complete request write and zero
  exit status.
- The Engine charges the actual compact normalized request echo before any
  dispatch against `64 MiB - 48 bytes`, then separately charges the actual
  response payload and final envelope after execution. Raw input length is not
  proof that normalized or escaped echo bytes fit.
- The public `DurableEngine` is a transport facade over stateful Rust
  `execute_durable` and `execute_live_evolution` requests. `start`, `get`,
  `resume`, `signal`, `release`, and `evolve` must never fall back to local SDK
  reduction or validation-only receipts.
- Every successful Engine `execute_live_evolution(target, evolution_id,
  command)` mutation returns one `EvolutionCommit`. After matching the outer
  Engine request echo, SDKs verify the commit's `observed_revision`, required
  nullable `committed_revision`, and stable semantic receipt against the exact
  `evolution_id` and command sent. The receipt contains no generic history
  identity, StateRoot manifest, CAS token, or physical result revision.
- Cross-language JSON accepts only unique object keys, finite numbers, and
  integers in the shared exact range `-9007199254740991..=9007199254740991`.
  A lost deadline or cancellation response after mutation begins is an
  `unknown_world_outcome` requiring reconciliation.
- Strict JSON parsing has one explicit 128-level nesting limit. Non-integral
  decimal/exponent tokens retain exact decimal evidence through request echo
  and typed admission; two distinct mathematical fractions must never compare
  equal merely because a host binary float rounds them to the same value.
  Every number token is at most 256 bytes and its exponent at most six digits;
  this bounded raw scan precedes host parsing or big-integer allocation. Every
  typed boundary that owns raw JSON bytes, including direct CLI, ordinary
  plugin, and Evolution-plugin ingress, compares that evidence with typed
  reserialization before admitting an identity or effect.
- `cymule.plugin/3` is the only process-plugin protocol. Every dispatch and
  reconciliation carries one exact `cymule.effect-provider-attempt/1` derived
  from the semantic intent plus retained claim owner/fence, and the provider
  echoes that same attempt before settlement. Expected component
  failures and defects are distinct closed response variants; an unclassified
  process error is never an expected application result. The official Unix
  process executor launches a fresh private copy of its captured closure; its
  execution-binding revision covers executable bytes, arguments, explicit
  environment, working tree, runtime closure, deadline, and limits. Plugin
  stderr never enters an Engine failure.
- An Engine process must close every internal provider/process authority before
  its direct Child exits. SDKs own only that Child handle and their local pipes;
  they never probe or signal a raw PID/PGID. Deadline handling kills the direct
  Child if still live and closes local transport descriptors. The official
  Engine/executor watchdog remains the sole descendant-closure authority.
- Rust, Python, and Go process clients use SDK-owned cancellation gates that
  linearize launch and admitted completion; Go exposes no caller Context as a
  second cancellation authority. Engine failures omit `issues` or carry 1..=100
  entries, and every SDK admits an Effect result if and only if the state is
  `Applied`, with exact kind `cymule.effect-result/1`.
- `cymule.ir/3` reusable definition calls resolve inside one immutable Plan.
  Logical latest-compatible references are linked by M4 into a new parent Plan;
  a sealed Plan never dereferences a mutable `latest` alias at runtime.
- `cymule.ir/3` scopes have one auto-commit meaning and no mode field. A nested
  scope commits only after its body completes; do not restore the removed
  `transactional`/`speculative` label or add a compatibility alias.
- An Effect result binding is legal only for an observational Effect with eager
  dispatch. Enforce this once during core Plan admission so sealing, identity
  verification, Embedded execution, and durable Run creation reject every
  deferred bound Effect before canonical mutation or business plugin I/O.
- Legacy component `call` is unclassified computation and may run again when a
  response is lost after its pre-I/O occurrence/Attempt checkpoint but before
  the atomic result/Continuation checkpoint. Do not present it as world-effect
  or provider exactly-once authority; use an observational
  Effect for a bound provider observation, a mutating Effect for world changes,
  or an integration-owned identified occurrence such as the Agent plugin.
- `cymule_core::seal_plan` is the only Plan sealing authority. It rejects every
  recursive definition SCC, including invokes nested under scopes, and compiles
  schemas as Draft 2020-12 with external retrieval disabled. Machine insertion
  and restore reverify the same admission.
- Every durable `WaitCondition` pins its exact definition, invocation, Region
  path, site, and step. Only its nested local bind is optional. Activation
  atomically commits the result Artifact, completed wait, optional frame local,
  and Continuation readiness. Embedded execution returns a typed boundary and
  never claims a Continuation.
- Reusable modules resolve their complete acyclic pinned dependency closure
  before sealing and retain every exact selected revision in the immutable link
  record. Reverse dependency indexes contain only top-level
  `LatestCompatible` template references. A new revision cannot tunnel through
  a pinned transitive edge; the directly referenced module must publish a new
  revision before its parent template can relink.
- `LatestCompatible` is explicit and legal only in an unsealed parent
  `PlanTemplate`. Published reusable-definition dependencies are strictly
  ordered, unique, exact-contract `Pinned` references. Before advancing a
  future template head, compare entry-reachable component, effect, wait,
  capability, and authority surfaces; any widening retains the old head.
- M4 rollout state is evidence-driven and future-only. Migration/shadow code is
  a pinned plugin, observations match immutable occurrence pins, and only Rust
  evaluates deterministic promotion/rollback gates. SDKs carry the closed
  control union without duplicating these decisions.
- `cymule-profile-protocol::evolution` is the sole M4 DTO, identity, bounded
  source-view, and pure-reducer authority. `cymule-evolution` contains only its
  re-export and the closed process-provider wire. The only durable mutation
  seam is the provider-registry-bound `DurableEvolutionControl` obtained from
  `DurableStoreControl`; ordinary M4 work does not require a PluginHost, runtime
  execution binding or Clock. It exposes exact current/receipt reads and one typed
  `commit(EvolutionPersistenceCommand)` operation, never a raw transaction,
  generic history append, delta, StateRoot mutation, or prepared postcondition
  input.
- M4 state is a scalar partition current plus 22 independently keyed bounded
  StateRoot families for definition heads/records, exact compatibility,
  top-level reverse dependencies, templates, links, Plans, edges, rollout
  current/evidence/decisions, occurrences/selections, migration/restart/shadow,
  observations/evidence, and transitions. No leaf contains the complete
  registry, template history, evidence history, or a replayable snapshot.
  Ordinary open and command handling use exact keys; history traversal is an
  explicit offline audit operation.
- Durable checks the all-ever command alias before current reads, source
  derivation, Describe, or provider execution. The same command ID and exact
  semantic content returns the original receipt with
  `committed_revision: null`; different content conflicts before I/O. A fresh
  command prepares against one pinned StateRoot, satisfies typed bounded read
  requirements, derives M1 quiescence and source binding internally, resolves
  any fresh migration target binding from the control's fixed exact-Plan
  registry, runs any exact provider into non-serializable authority, reduces
  purely, and writes current,
  alias, receipt, normalized leaves, Plans, Artifacts, and coupled M1 state in
  one CAS. Conflict, validation, provider, storage, or pre-CAS crash writes
  nothing and performs no reverse rollback or genesis replay.
- Historical M4 publication reuses exact immutable definition, link, Plan, and
  `cymule.plan-edge/2` structural-transition records. Edge identity binds the
  ordered from/to structural diff, not publication evidence. The first accepted
  `EdgeRecord` evidence remains immutable, while every later publication retains
  its own evidence Artifact through its complete semantic command receipt. Every
  generated future decision binds its source decision so a historical Plan
  cycle cannot reuse an evidence accumulator. `cymule.rollout-transition/2`
  binds exactly the retained source decision, target decision, and verified gate
  evaluation; its verifier always recomputes that identity.
- M4 occurrence admission stores one typed pin containing occurrence, template,
  retained rollout decision, semantic Plan, exact `cymule.execution-binding/2`
  Artifact, and selection identity. A Virtual Run persists either a direct Plan
  or an Evolution partition/template selector. After fairness selects the Run,
  Evolution selection, M3 claim, and the M1 execution authority share one CAS;
  callers do not supply an M4 selector at claim time. Late evidence remains
  attributed to the retained decision; only gate application requires that
  decision to remain current. Never store a Plan ID in `occurrence_binding`. A
  safe-point migration commits its receipt and output
  Artifacts together with the Machine Plan/binding transition, Continuation
  state replacement, and epoch advance as a claim-free `Ready` safe point while
  preserving the execution fence and Attempt history. The next ordinary resume
  separately acquires a claim and creates the next Attempt. A lost
  acknowledgement resolves through the exact command alias and receipt without
  reinvoking the adapter. The semantic migration wire carries intent and scalar
  optimistic preconditions only; Durable derives the Continuation, quiescence
  witness, and source binding from the pinned StateRoot. Direct reduction and
  stored receipt reads share one receipt self-consistency validator. For fresh
  migration, deterministic exact reads select the target Plan before the fixed
  provider registry resolves its complete `ExecutionBinding`; the profile
  verifies and admits that binding and materializes its canonical Artifact
  record before provider I/O. The binding, typed Core `MigrateRun`, target
  Continuation, and M4 postcondition share one CAS. Caller-authored, Ref-only,
  ambient, lazy, or fallback target binding authority is invalid. Exact retained
  migration-record replay loads that binding Artifact from the same root,
  invokes no binding registry or adapter, and emits no second M1 sidecar.
  A Restart receipt likewise retains its complete request and exact target Plan;
  authorization and restore share one complete receipt validator.
- Runtime composition dispatches each bound operation to the exact admitted
  provider. Historical execution first requires the complete selected
  OperationBinding and resolved transitive provider dependency closure to equal
  the runtime owner's current admitted pin. Every current and historical
  component/effect call repeats this admission. Live manifests advertise
  capability only; they do not prove code/configuration identity, select a
  provider, widen authority, or make an unbound operation callable.
  Post-claim invocation consumes the framework-owned one-shot admission token;
  its private construction and `FnOnce` provider closure prevent callers from
  fabricating, reusing, or bypassing the admitted operation.
- Never accept a caller boolean or isolated Continuation as migration-safe-point
  authority. The durable coordinator derives exact-domain quiescence from the
  Machine, Continuation, waits, outbox, obligations, Attempts, and effect claim
  leases at one head revision; migration and replacement authorization consume
  that same receipt before work and at their final CAS.
- Effect occurrences and outbox records pin the origin Plan, exact
  `cymule.execution-binding/2` Artifact, and derived operation binding.
  Dispatch admission compares that operation's complete binding and resolved
  provider closure, then checks its live capability; unrelated historical
  providers outside that closure are not dependencies. Output validation and
  reconciliation use only the origin pins. Missing historical handlers never
  fall back to the current binding. Before dispatch, unavailability closes the
  undispatched intent as CancelledBeforeRelease/NotApplied and resolves its
  obligation. After dispatch, it retains ambiguity for governance. If a
  real claim already committed and immutable binding equivalence already proves
  handler loss, one CAS records `Unknown` plus unavailable and retains the claim
  fence. Otherwise recovery records `Unknown` before live provider admission;
  a subsequent framework-owned manifest mismatch marks that retained claim
  unavailable for governance.
- An Effect dispatch has a result if and only if its state is `Applied`, and
  that reference has exact kind `cymule.effect-result/1`. StateRoot leaf reopen,
  exact reads, query summaries, and full audit all enforce the same invariant.
- New behavior is provider-neutral by default. Concrete persistence, activation,
  execution, model, tool, and effect integrations belong behind plugin or
  substrate interfaces.
- Day-one official plugins cover SQLite/directory durability, filesystem and
  Apache object-store Resources, HTTP/timer activation, restart-monotonic clock
  observation, process execution, OpenTelemetry observation, and RMCP tool
  mapping. They remain independent crates and may not move their
  runtime/provider semantics into core.
- Domain-specific Sessions, Agent Loops, transport streams, protocol objects,
  and their controllers belong in optional plugins. Core crates, CLI, and SDKs
  expose only technology-neutral semantic and substrate contracts.
- Keep all source code, comments, documentation, commit messages, schemas, and
  user-facing project metadata in English.
- User-facing README files describe observable capabilities and limits without
  M0-M6 milestone labels. Keep milestone sequencing in the roadmap and
  maintainer profile/specification documents.
- User quick starts lead with a concrete scenario, the failure or cost being
  avoided, and an observable outcome. Defer CAS, journal, occurrence, binding,
  and other implementation vocabulary to architecture or conformance material.
- GitHub Actions is the only publication authority for every public artifact,
  including packages, release assets, and future registry distributions. Local
  commands may build, test, stage, and inspect release bytes, but must never
  publish them or require a long-lived registry token.
- Every external Action is pinned to a full immutable commit. Verification,
  exact-SHA soak, build, test, and archive staging run without OIDC; only a
  protected terminal environment grants the minimal publisher `id-token`.
- Release bytes are independently rebuilt and closed by two no-OIDC jobs before
  publication. The terminal OIDC job uploads only those closed bytes through an
  exact-SHA controller; it never builds, packages, tests, or executes code
  carried by an artifact. Immediately before mutation, every terminal publisher
  re-reads the peeled annotated release tag and requires its commit to equal the
  frozen release SHA. The terminal controller resolves a failed write response
  through bounded exact readback in the same run; a later job repeats registry
  verification without OIDC.
- A non-cancelling release workflow linearizes controller admission exactly
  once by proving its workflow SHA is current public main before release work
  begins. Every later job executes that immutable controller SHA; a subsequent
  mirror advance is a new generation and does not revoke the admitted run.
  Terminal writes still re-read every immutable release/tag/receipt authority.
  The GitHub Release owns exactly its frozen BOM asset and rejects every
  additional asset; npm tag push response loss is resolved only by exact remote
  readback.
- GitHub Release finalization additionally binds the annotated tag object itself.
  Verification requires object type `tag`, records its exact 40-hex
  `release_tag_sha` separately from the peeled `release_sha`, and rejects the two
  identities being equal. Its finalization bundle uses exact
  `stage_version: cymule.release-finalization-stage/3`
  and also binds the authenticated private source SHA, raw immutable
  `cymule-mirror/<public-sha>` receipt tag, and shared source-snapshot digest.
  Freeze, terminal publish, and every Release mutation re-read the release and
  receipt tag authorities. Replacing either annotated tag while preserving its
  target commit is an authority change and fails before mutation.
  `scripts/release_contracts.py` is the single selector source for the
  finalization stage, mirror receipt, GitHub settings snapshot, and GitHub
  control-plane receipt; private mirror shell writes must be statically matched
  to that public reader and never become a second registered source.
- GitHub Release finalization first requires a protected contents-read live
  control-plane gate, with a repository-scoped Administration-read plus
  Actions-read App token, before `actions/attest` may create the terminal BOM
  attestation. After attestation, a separate protected contents-read job repeats
  the live gate and closes a fresh 15-minute
  `cymule.github-release-control-plane-receipt/2` from immutable Release, exact
  release/receipt tag rulesets, protected-environment, default-permission, and
  default-branch authority. The contents writer receives no administration
  credential and validates the same-run/attempt receipt before every mutation,
  including before `--draft=false`. Settings administrators must not change
  that control plane from finalization dispatch, including throughout the live
  reads, until the non-cancelling finalization completes or is cancelled;
  expiry requires a new preflight, never a writer
  credential or receipt extension.
- Stable release workflow input is one canonical ASCII `MAJOR.MINOR.PATCH`
  value. The current controller validates it before constructing any Git ref or
  writing `GITHUB_OUTPUT`; prereleases, leading zeroes, whitespace, newlines,
  Unicode digits, and additional output records are not admitted by the stable
  workflows.
- Every ordinary Required CI plan includes one dedicated deterministic
  version-domain source-closure leaf. It exact-matches all public candidate
  bytes and Git modes to the registered source snapshot and validates the
  registry closure without turning each narrow product change into `full`.
  Any plan selecting `rust-executor-plugin` additionally requires an exact
  candidate-SHA `macos-15` witness, and the `Required CI` aggregator closes that
  job and its reported SHA. `publish-crates.yml` consumes the equivalent
  credential-free exact-release-SHA macOS executor witness before its OIDC job
  can acquire a token or issue a PUT.
- npm trusts only the inert `publish-npm-release.yml` caller generation `/1`.
  That caller delegates to `publish-npm-controller.yml@main`; the called
  verify job requires GitHub's resolved workflow SHA to equal freshly fetched
  public main and freezes that immutable controller for the non-cancelling run.
  Every contents-write and OIDC job exact-matches and executes only that
  admitted controller while treating
  the exact calling tag as payload/provenance data. The retired
  `publish-npm.yml`, tags without caller `/1`, and direct controller dispatch
  have no publication authority.
- The inert npm caller's reusable-workflow job supplies the exact
  `contents: read` plus `id-token: write` ceiling that GitHub requires for its
  called jobs. Controller jobs narrow that authority independently, and the tag
  writer and registry publisher both require the protected `npm` environment.
  Only after that environment's approval may the tag job obtain its separate
  repository-scoped Contents-write GitHub App token; the built-in
  `GITHUB_TOKEN` remains read-only and must never create tags.
  npm trusted-publisher configuration names that caller; SLSA workflow/ref and
  its singleton Git dependency bind the caller ref plus release SHA; the Fulcio
  SAN separately binds `publish-npm-controller.yml@refs/heads/main`. Fulcio
  extensions 1.9 and 1.10 bind the actual publisher `signer_ref` and
  `signer_sha`; that signer may be a retained historical controller and is not
  interchangeable with a later finalizer controller SHA.
- The immutable GitHub Release asset is `cymule.release-bom/3`. It records the
  immutable source inventory with distinct authenticated private and rewritten
  public SHAs, plus a required
  `publication` member for every package: closed Cargo/npm registry content and
  provenance evidence, or explicit `null` for Python/Go packages that have no
  publication authority. A fresh credential-free freeze runner performs the
  terminal registry readback immediately before building the BOM; the protected
  Release job executes no tag or package code and only publishes the frozen
  three-file finalization bundle bound to both `release_tag_sha` and
  `release_sha`. The current finalizer SHA belongs to that run's stage,
  attestation and control-plane receipt, never the immutable BOM bytes. A new
  current-main controller can therefore revalidate and attest the same BOM
  before completing an interrupted draft without replacing its asset.
- Stable GitHub Release finalization is globally serialized in one
  non-cancelling group. Its once-admitted immutable controller reads every REST page and an
  exact `isLatest` projection around each mutation, orders only canonical
  published stable tags by numeric SemVer, and explicitly marks historical
  recovery non-Latest. Completion requires exactly one Latest equal to the
  highest published stable version; non-stable Latest ownership, duplicate
  identities, and multiple Latest authorities fail closed.
- The public GitHub repository never stores private-source credentials or a
  mirror controller. A credential-free job proposes and secret-scans one
  public-only candidate, but that artifact is not rewrite authority. Before any
  remote read, the credentialed terminal controller independently reruns the
  canonical deterministic rewriter from the frozen private tip and requires the
  candidate tip to equal that complete rewritten commit graph. The mirror stage
  waits for the entire verify barrier, rechecks current private main, publishes
  only against the observed public-tip lease, and reads back the exact public
  tip.
  Private/public commit SHAs belong only in the mirror receipt and release
  BOM; registry provenance uses the mirror-stable snapshot digest.
- Public Rust crates share the TypeScript release version. `cymule` is the Rust
  facade and `cymule-cli` is the binary package; profile/plugin crates publish
  in the dependency order owned by `scripts/crates-release.toml`. That graph
  includes every workspace-local normal, build, target-specific, and versioned
  dev dependency retained by Cargo's normalized package; path-only dev
  dependencies with no version are repository-test-only and are stripped.
  Validation uses one deterministic dependency-first sort and rejects every
  cycle before staging or terminal publication. Every PUT result receives
  bounded exact-checksum readback; only an exact new-name 429 plus confirmed
  absence may retry, after repeating the immutable tag fence under the same
  admitted controller.

## Change discipline

- Read the nearest nested `AGENTS.md` before editing a directory.
- Treat generated files and lockfiles as derived artifacts; update their source
  and regenerate them in the same change.
- Any semantic change requires a version-domain decision and updates to the
  normative specification, schemas, conformance tests, and SDK fixtures.
- `versioning/version-domains.json` is the only version-domain inventory.
  Public version constants, schema identities and canonical digests, schema
  dependency edges, package ownership, generated specification table, and
  release BOM must verify against it. One protocol string never selects among
  different shapes; generation selection precedes closed decoding.
  The validator inventories every current `cymule.*/*` production literal in
  Rust crates/plugins, all three SDKs, schemas, and release controllers;
  private Artifact-kind and content-ID domains are version authorities too.
  Registry and schema authority JSON is duplicate-free no-float I-JSON,
  canonicalized with RFC 8785 UTF-16 member order and safe integers. The first
  registry is an explicit genesis: all four predecessor fields and every
  domain's source provenance are null, and HEAD ancestry may contain only that
  same unfrozen release generation. Every successor binds its canonical
  predecessor digest to the predecessor public source-snapshot digest. Neither
  form depends on a private-history commit SHA or fabricates missing ancestry.
- Add a root rule only when it applies across multiple project areas. Put domain
  detail in the nearest nested handbook.
- Do not claim a conformance profile unless its complete fault-oriented suite
  passes. Mark planned and partial behavior explicitly.

## Required verification

Use `python3 scripts/test_harness.py plan --base <trusted-ref>` and run its
selected suites before an ordinary scoped commit. Run `./scripts/verify.sh` for
semantic version changes, profile claims, shared schemas, release/publication
changes, harness or CI changes, unknown routes, and before claiming complete
repository verification. The suite model and fault-test rules live in
`docs/testing.md`. At minimum, applicable changes must preserve:

- Rust formatting, Clippy, unit tests, and documentation build;
- deterministic canonical IDs and replay digests;
- JSON Schema validation fixtures;
- TypeScript, Python, Rust, and Go SDK end-to-end tests against the Rust engine;
- the MLIR workbench smoke test when the pinned host MLIR toolchain is available.

Run `python3 scripts/test_harness.py run rust-soak` only for scheduled, release,
or explicit anomaly-depth verification; keep it independent from focused local
feedback.
- Durable Engine requests carry separate store and optional executor targets;
  queries omit execution authority. Migration and shadow targets pin exact
  process bytes and use the closed `cymule.evolution-plugin/3` protocol.
