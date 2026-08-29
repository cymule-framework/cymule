# Live Evolution Terminal Guidance

## Authority boundary

- `cymule-profile-protocol::evolution` owns every portable M4 command,
  normalized state leaf, exact query/read DTO, semantic receipt, provider
  contract, and the provider-independent pure reducer. `cymule-evolution`
  re-exports that public contract and owns provider hosting only; it must not
  copy or fork the DTOs or reducer.
- `cymule-durable::DurableEvolutionControl` is the only persistence mutation
  authority. It is obtained from a provider-registry-bound
  `DurableRuntimeControl`, reads one pinned `StateRoot`, invokes the selected
  exact provider only after detached preparation is complete, and commits one
  typed postcondition with one CAS.
- Production Evolution code must not import or expose raw Durable transaction,
  generic delta/history-record, or StateRoot mutation APIs. Do not retain a
  compatibility bridge.
- Ordinary load and replay are exact keyed reads of scalar current,
  command alias, and receipt. Historical traversal is an explicit offline
  audit operation and is never needed to prepare or replay a command.
- A CAS conflict, storage failure, provider failure, validation failure, or
  process crash before the CAS produces zero durable writes. There is no
  reverse full-state rollback and no reload-from-genesis recovery path.

## Normalized state and receipts

- No M4 leaf may contain a complete live snapshot, registry history, template
  history, or accumulated evidence list. State is partitioned into scalar
  current plus exact keyed definition, compatibility, dependency, template,
  link, Plan, edge, rollout, occurrence, migration, restart, shadow,
  observation, evidence, and transition families.
- Respect the profile-owned fixed limits for command, leaf, receipt, source
  aggregate, postcondition aggregate, read/write count, publication fanout,
  reusable-definition references, and dependency depth. Checked aggregate
  accounting includes the scalar-current membership key even at genesis.
- Command IDs are exact all-ever idempotency aliases. Reusing a command ID with
  different semantic content conflicts before current reads or provider I/O;
  exact replay returns the original receipt without invoking a provider.
- A semantic receipt binds its complete command, exact parent current, optional
  Durable-derived source witness, closed outcome, and strictly ordered typed
  write descriptors. It never contains the result StateRoot revision,
  manifest, CAS token, or a child-current identity that creates a fixed-point
  cycle. Physical revisions exist only in read/commit envelopes.
- Immutable link records are keyed by the complete linked closure identity,
  not only by Plan ID. A template current retains the exact selected revision
  closure even when two closures seal to the same executable Plan.

## Wire and provider authority

- Cross-language execution is exactly
  `execute_live_evolution(target, evolution_id, command) -> EvolutionCommit`.
  The host constructs and verifies `EvolutionPersistenceCommand` and calls the
  single Durable façade; no second physical or history-shaped receipt surface
  exists.
- External commands carry semantic intent and scalar optimistic
  preconditions only. They never carry a safe point, Continuation, execution
  binding bytes, provider product, read set, StateRoot, manifest, or CAS token.
  An occurrence selection may name one already-admitted ExecutionBinding
  Artifact reference; Durable must load its exact record from the same pinned
  root, decode the canonical binding, and admit the selected Plan before it can
  reach the reducer.
- Durable derives quiescence, Continuation, source binding, admitted Plans, and
  Artifact membership from the same pinned root. A fresh migration target
  binding instead comes from the control's fixed exact-Plan registry after
  deterministic preparation, is admitted against that pinned target Plan, and
  enters the reducer as non-serializable authority; retained migration replay
  loads its complete binding record from the same pinned root without invoking
  the registry.
- Generic occurrence selection first prepares the exact selected Plan, then
  Durable resolves and verifies the requested pre-existing binding record from
  the same root before reduction. Migration instead carries the runtime's
  complete canonical target binding record as non-serializable authority,
  admits it against the prepared target Plan before provider I/O, and publishes
  it in the same CAS; a Ref-only target binding is invalid.
- A provider registry is fixed for the lifetime of the owning Durable control.
  Target execution bindings resolve by exact Plan, while migration adapters and
  shadow drivers resolve by exact semantic identity and immutable content
  revision. A retry cannot replace the registry, binding, or provider
  implementation; absence never falls back to ambient runtime state.
- Provider Describe and execution happen only after the exact command alias is
  absent and all deterministic validation/read requirements have completed.
  A provider result is opaque, non-serializable reduction authority; callers
  cannot author or submit a prepared target state.
- Process requests and responses use the same fixed raw-message bound and
  duplicate/unsafe-number/unknown-member rejecting decoder. Provider failures
  retain one closed category (`cancelled`, `timed_out`, `contract`,
  `integrity`, `plugin_defect`, or `substrate`) and preserve structured
  contract violations or exact code/message fields end to end; never infer a
  category from text or concatenate a stable code into a message.
- Migration and shadow products must equal their exact Artifact closure.
  Introduced records and required pre-existing references are verified and
  retained in the same CAS as normalized M4 mutations.

## Reusable definitions and linking

- Plans and reusable-definition revisions are immutable and content addressed.
  Publication creates a new revision; it never edits an existing Plan or
  revision.
- `PublishDefinition` and `LivePublicationCommand` always contain an explicit
  `references` array. Missing is invalid; an explicit empty array means no
  dependencies.
- A published module's references are strictly ordered by unique logical
  identity and pin one exact revision plus exact input/output contracts.
  Every parent reference carries one explicit closed strategy;
  `LatestCompatible` is allowed only in an unsealed `PlanTemplate` and is never
  supplied by a Serde or Rust default.
- Linking operates on a bounded exact transitive revision closure. It rejects
  missing revisions, contract mismatch, conflicting choices, cycles,
  extraneous revisions, oversized closures, and excessive depth before any
  provider call or durable write.
- The compatibility-current family is keyed by logical reference and exact
  contract. Never scan immutable revision history to implement
  `LatestCompatible`, and never fall back to the incompatible global head.
- Publication fanout is a bounded reverse index of parent templates whose
  future selection is dynamic. Pinned references do not become implicit
  mutable dependencies.

## Evolution semantics

- `cymule.plan-edge/2` binds the exact source Plan, target Plan, and
  deterministic non-empty structural diff. The first accepted edge record
  retains its exact evidence Artifact. A repeated same-direction transition
  reuses that record; each later publication retains its own evidence Artifact
  through its complete semantic receipt and cannot replace the first evidence.
  Caller-authored operations must equal the pure diff.
- Rollout decisions affect future selections only. Occurrences retain one
  immutable Plan, decision, selection ID, and ExecutionBinding Artifact.
- A Virtual Run owns one immutable execution selector: `Direct` pins an exact
  Plan and creates no M4 mutation, while `Evolution` names one M4 partition and
  template. After fairness selects the Run, the cross-profile selection ID is
  derived only from the Virtual persistence ID; Durable prepares the bounded
  Evolution read set, validates the claim's already-admitted binding record
  against the selected Plan, and commits the standard Evolution receipt plus
  occurrence/selection leaves in the same CAS as the Virtual claim. There is
  no claim-level optional M4 selector or parallel selection receipt.
- Rollout evidence is stored as exact observation/shadow records plus a bounded
  authenticated accumulator. Publication evidence is retained by its exact
  semantic command receipt. Never retain an ever-growing evidence vector in a
  current leaf.
- Completed decisions cannot become current again. Promotion and rollback
  create immutable transition records and a new decision.
- Migration consumes a Durable-derived quiescent source, exact reviewed edge,
  deterministic compatibility, no-widening proof, exact adapter revision, and
  complete target Continuation. Restart authorizes a distinct replacement Run;
  it does not mutate or resume the source Run.
- Keep Plan semantics separate from execution binding semantics. A Plan ID is
  never an implementation binding or provider selector.

## Verification

- Focused tests must cover exact replay, same command ID with different
  content, CAS conflict, provider failure, storage failure, crash before and
  after CAS, reopen, lost acknowledgement, zero writes on every failure,
  missing exact leaves, aggregate limits, publication fanout, reference
  ordering/size/depth, linkage/compatibility, and Artifact closure.
- Verify production code and tests contain no raw Durable transaction or
  untyped persistence imports or calls. Do not satisfy this check by hiding an
  obsolete path behind a wrapper or feature flag.
- Run profile reducer tests first, then Durable façade tests, Evolution host and
  process-protocol tests, SDK/schema conformance, and finally workspace
  all-target and clippy gates. Report unrelated concurrent failures separately;
  never describe a blocked gate as passing.
