# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- Every schema `$id` and RFC 8785 canonical digest is registered in
  `versioning/version-domains.json`. External `$ref` edges must match the
  registered domain dependency closure. A protocol string never authorizes a
  different schema generation or decode-failure fallback.
- A domain owns an authenticated schema set, not one privileged root file.
  Root and supporting schema paths each retain their `$id` and canonical digest
  and must all appear in the release BOM. Every supporting owner names its exact
  JSON Pointer or `$anchor`; dependency checks follow only the reachable
  fragment, and every tracked root or plugin schema has one document-root owner.
  Supporting records are sorted and unique by `(path, fragment)`, allowing one
  owner to authenticate several real contracts in a shared document without
  attributing a pure DTO to the document's unrelated runtime owner. BOM schema
  entries remain path-unique with exact ID/digest equality.
- Release BOM/3 contains only immutable release source, package, schema,
  registry, migration, and publication evidence. It rejects a finalizer
  `controller_sha` field; the finalization stage, attestation, and current-run
  control-plane receipt independently bind controller authority. Both
  `source_sha` and `public_source_sha` are required lowercase SHA-1 strings;
  semantic validation additionally requires them to differ.
- `engine-protocol.schema.json` owns both sides of `cymule.engine/5`: one
  versioned request envelope and one success-or-failure response envelope.
  Every success requires both the complete inner request and closed response;
  that request uses the same exact union as request ingress, and each request
  variant admits exactly its corresponding success-response variant rather
  than an independently selected response tag. Failure requires only the
  structured error and forbids a request because strict decoding may not have
  produced one. Every predecessor success shape is invalid.
  Failure categories, phases, contract sides, issue bounds, and retry
  dispositions are closed and must match Rust plus every SDK. A category admits
  only its exact recovery set; only transport failure and not-found omit the
  member, while unknown-world-outcome requires reconciliation. Contract issues
  preserve separate instance `path` and `schema_path` JSON Pointers.
- Engine v5 live evolution returns an `EvolutionCommit` whose persistence
  receipt retains the exact evolution identity, complete admitted semantic
  command, closed outcome, and mutation set. Nested migration receipts retain
  the complete admitted migration request and reject
  source, target, Continuation, and program-counter integers outside the shared
  JSON safe-integer range.
- Migration receipt target epochs have minimum one, and source/target
  migration Continuations have at least one frame and scope in schemas and all
  SDK validators. Every serialized Continuation carries the required exact
  `cymule.continuation-state/1` DTO generation; `cymule.continuation/1` remains
  only the distinct continuation-ID content domain.
- `plugin-protocol.schema.json` owns both request and response variants of
  `cymule.plugin/3`. There is no generic error response: a component may return
  a bounded `expected_failure`, while a protocol failure is an explicit
  `defect`. Effects return exact world outcomes.
- Every public `ArtifactRef` requires `identity_version = cymule.artifact/2`, a
  lowercase SHA-256 ID, and a closed lowercase path kind. The v2 identity and
  machine snapshot v11 replace their predecessors without fallback. The
  `artifact-type-contract.schema.json` file freezes recoverable canonical JSON
  contracts; opaque Artifacts do not use that schema.
- Keep this exact Artifact reference shape identical in every owning public
  schema and retain negative fixtures for missing/legacy versions, malformed or
  uppercase digests, and invalid kinds.
- `live-evolution-control.schema.json` publication evidence is a complete
  `ArtifactRecord`: `reference` and bounded canonical padded Base64 bytes are both required.
  Base64 admits at most eight decoded MiB, requires zero unused padding bits,
  and rejects every trailing byte, including a line break. Schema freezes the
  closed shape; Rust and SDK admission additionally
  recompute the artifact/2 preimage and reject mismatched bytes.
- `cymule.ir/3` is the sole IR generation and includes the closed `invoke`
  operation. The superseded `/2` generation is rejected without a reader.
  Future operation additions require a new IR version rather than widening this
  frozen schema in place.
- Every `cymule.ir/3` component contract requires a non-null
  `output_artifact_kind`; omission and explicit null are both invalid. The Plan
  owns that kind and neither schema nor SDK supplies a compatibility default.
- The `cymule.ir/3` scope has no mode field; the removed
  transactional/speculative labels are unknown members with no compatibility
  reader. Effect `bind` remains a wire field whose profile relationship is
  enforced by Rust Plan admission.
- The existing `wait` operation has an optional `bind`; omission intentionally
  ignores the result. Engine success distinguishes completion from typed
  Embedded suspension, explicit release, and reconciliation boundaries without
  publishing a fake Continuation or string failure.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.
- `execution-binding.schema.json` freezes `cymule.execution-binding/2`. Rust
  additionally enforces normalized provider order, exact service ownership,
  Plan requirements, manifest equality, and content identities.
- `resource.schema.json` owns `cymule.resource/4` candidates/handles, separate
  locator sets/publications, `cymule.resource-manifest/3` descriptors,
  `cymule.resource-list-proof/5` proofs with a required-nullable predecessor,
  `cymule.resource-list-cursor/3` cursors with a required last entry name, and
  the external `cymule.resource-handoff/5` plus
  `cymule.resource-handoff-activation/3` typed-Artifact records. Internal
  profile commands, lifecycle receipts/current projections, deletion targets,
  and index records belong to Durable storage schemas and never widen the
  three public language SDKs. Public shape or integrity changes require Rust
  semantic validation, all SDKs, fixtures, and cross-language Resource ID tests.
  Candidate and Handle `media_type` uses the exact shared lowercase ASCII
  type/subtype token pattern; parameters, whitespace, controls, uppercase, and
  additional slashes are shape failures. Resource `/3` is exact-rejected.
- Resource page proofs admit at most 1000 inclusions and 53 Merkle steps per
  inclusion, including the predecessor. Artifact-type schema admission has
  separate aggregate Rust budgets of one encoded MiB, 16384 JSON value nodes,
  and depth 64 with root depth one; shape validation never replaces those
  pre-clone, pre-hash, and pre-compilation checks. Resolved local schema
  references reject cycles and unresolved targets; expanded traversal is
  bounded to depth 64, 65536 visits, and sixteen cumulative canonical MiB,
  counting every repeated reference.
- `wait-activation.schema.json` owns the provider-neutral
  `cymule.wait-activation/2` delivery record; Engine responses retain it in
  `cymule.wait-activation-receipt/3`. Source, targets, and result must
  stay closed and pass Rust plus four-SDK fixture conformance; concrete clock,
  signal, queue, and transport fields never enter this schema.
- `wait-condition.schema.json` freezes the public M1 wait projection. Every
  wait owns an exact definition, invocation, Region path, site, and step;
  `bind` alone is nullable and remains nested inside that mandatory owner.
- `durable-control.schema.json` owns the closed
  `cymule.durable-control/4` mutation/query union. Start, resume, takeover, and
  effect release carry the exact driver, TTL, expected takeover fence where
  applicable, and opaque issued Clock reference. SDKs may construct those,
  cancellation, wait-activation, and seven bounded read-only query commands,
  but only the
  Rust M1 runtime may reduce them against a durable domain. Wait activation
  targets are exact lowercase SHA-256 wait content IDs at control ingress, not
  generic caller-assigned identities.
- Query schema members preserve required-null `expected_revision`, cursor, and
  exact item results. Page cursors bind query family, optional Run owner,
  source revision/root, and the authenticated final key/hash; no schema may
  restore the removed full-Run/domain response or `query_id` authority.
- The same union owns provider-linearized `resolve_effect`: exact original Run/intent,
  execution-binding Artifact, occurrence binding, retained claim owner/fence,
  one of `resolved_applied|resolved_not_applied`, and a required nullable value
  member. It carries no Run execution claim or Clock; the exact historical
  Effect provider must atomically close late first-dispatch admission before a
  terminal result can be persisted.
- Engine durable responses freeze `cymule.effect-resolution-receipt/1` and
  `cymule.run-cancellation-receipt/1`. Each receipt nests its complete normalized
  command and carries a Rust-derived `receipt_id`, never a Store revision.
  Effect receipts separately retain the historical provider's actual terminal
  resolution/value, which may differ from the requested decision; they never
  duplicate the Run's mutable aggregate `world_settlement`. Cancellation binds
  its Rust-derived reason Artifact only through the terminal boundary.
  SDK validators compare command echoes and closed references without deriving
  Artifact or receipt identities.
  An actual Applied receipt requires a result Artifact even for a JSON null
  value; actual NotApplied requires both value and result to be null.
  Effect query summaries preserve the same result-presence rule: Applied has
  an exact Artifact reference, every other state has null. The summary also
  enforces the Rust reconciliation/execution-availability state matrix.
- `DurableBoundary.effect_not_applied` carries the exact lowercase SHA-256
  Effect intent ID when a bound eager Effect settles NotApplied and releases
  the execution claim without advancing past the Effect site.
- Public Run identities, execution owners, and Clock source/scope strings use
  1..=512 Unicode scalar values and reject C0, DEL, and C1 controls exactly like
  Rust and all four SDKs. Every Engine request and M1 projection reuses that Run
  identity definition rather than narrowing or widening an embedded copy.
- JSON Schema `integer` is mathematical: safe `1`, `1.0`, and `1e0` values are
  equivalent. Raw protocol readers recursively normalize safe integral numbers
  before typed decoding and success echo construction, retain finite fractional
  numbers for fields that admit them, and reject unsafe integral values. Schema
  fixtures must differentially prove integral-float acceptance and fractional
  rejection at typed integer fields.
- Engine projections and typed storage delta operations freeze
  `ComponentOutcome`, semantic component occurrence, provider Attempt, retained
  Clock receipt, and Continuation claim lifecycles exactly. A claim's
  `continuation_id` is the lowercase SHA-256 content ID derived under
  `cymule.continuation/1`, never a descriptive prefix. Migration
  request/output/receipt Continuations are Ready with a null claim and do not
  carry a provider Attempt. Restart preserves an authorization and target Plan
  only; its distinct replacement Run enters normal Run admission afterward.
- Every Effect dispatch projection includes Core's closed reconciliation axis,
  and a Run view's aggregate world settlement must be recomputable from the
  complete ordered Effect set. The completed Run branch requires `settled` and
  permits only `applied`, `not_applied`, or `cancelled_before_release` Effects;
  failed and cancelled Runs may retain `unknown` for reconciliation.
- Engine completed results require an exact lowercase Plan content ID, a raw
  lowercase 64-character projection digest, unique lowercase effect content
  IDs, and a closed precondition token whose decimal epoch is canonical and no
  greater than `9007199254740991`.
- Current Agent selector set: `cymule.agent/10`, `cymule.agent-command/4`,
  `cymule.agent-command-id/2`, `cymule.agent-command-receipt/6`,
  `cymule.agent-command-receipt-id/4`,
  `cymule.agent-target-claim-current/3`,
  `cymule.agent-target-claim-generation-record/1`,
  `cymule.agent-target-claim-generation-key/1`,
  `cymule.agent-target-claim-key/1`, and
  `cymule.agent-target-claim-id/3`.
- `durable-storage.schema.json` freezes the exact provider-neutral physical
  union: `cymule.durable-head/2`, the
  `cymule.durable-state-root/6` immutable object graph and fixed manifest,
  `cymule.durable-gc-receipt/2`, and Core `MachineCommandArchiveObject`s.
  StateRoot is the sole persisted semantic projection authority. The closed
  `cymule.machine-delta/6`, `cymule.durable-state/7`, and
  `cymule.machine-snapshot/11` definitions exist only for admitted transition,
  materialization, open, and audit boundaries; no provider persists a recursive
  StateSegment, checkpoint envelope, or checkpoint-plus-suffix authority. Old
  physical generations have no reader or fallback.
  `StoreHead` pins the exact semantic revision, StateRoot manifest, required and
  nullable Machine base anchor, semantic and GC sequences,
  `cymule.durable-physical-token/2`, and required-nullable latest GC receipt.
  Every manifest has required-nullable parent manifest, parent revision, delta
  digest, and Machine base anchor. The parent manifest/revision/delta trio is
  all null only at genesis and all non-null on a successor; the independent
  Machine base anchor remains null until a verified compaction installs one.
  The root set requires a nullable `history_compaction_head` pointing directly
  to the typed latest history receipt value object. The same CAS updates its
  existing primary receipt map; head/base presence and a nonempty primary map
  agree exactly. There is no second history index or head map.
  Generation `/6` additionally requires `agent_target_claims`, immutable
  `agent_target_claim_generations`, and their closed current/generation-record
  leaf kinds. The physical schema admits neither a `/4` root/value nor an
  omitted claim root.
  History compaction `/2` admits only `event_prefix` and
  `event_free_admissions`; the retired conflict-only tag has no alias.
  Its fixed root set reaches only closed typed value objects and persistent
  map/log nodes. Rust derives and verifies every content identity, revision,
  ordered commitment, count relationship, canonical typed leaf, and encoded-
  size bound.
  Run query indexes use `cymule.run-query-indexes/3`; their Wait page root stores
  only closed `wait_summary` leaves. Complete `wait` leaves remain in the global
  exact/audit root, and Rust full audit proves the two projections agree.
  `StateRootValue::Leaf.canonical_json` is exact UTF-8 JSON text, not a numeric
  byte array. Rust enforces its twelve-MiB byte bound and typed canonical round
  trip; schema string length is only a necessary scalar ceiling. Machine-base
  chunk bytes use nonempty canonical padded standard Base64 with a four-MiB
  decoded bound and preallocation validation, including terminal unused bits.
  These chunks may split UTF-8. Old array codecs are rejected; ArtifactRecord
  and unrelated byte fields keep their existing contracts.
  GC `/2` binds the retained StateRoot and semantic sequence to exact parent and
  result physical tokens plus a monotonic GC sequence. It carries one bounded,
  sorted page of reclaimed content IDs and the exact remaining candidate count;
  an empty reclaimed page is valid only for a closed empty inventory.
  Machine compaction atomically inserts the exact Core command-archive object
  set—segment, independently addressed entry, and persistent sparse-map node—
  before the same head CAS. Segment headers close genesis/successor lineage and
  admission/Event/index counts and complete atomic batch manifests; applied
  entries carry their exact Event batch and conflicts carry an empty batch.
  Normal lookup follows at most 256 authenticated nodes and loads a
  member entry by its digest; it never scans segments or accepts caller proof
  bytes. Explicit raw audit follows archive segments, while explicit GC walks
  StateRoot plus the archive graph and verifies every retained object.
  Explicit application-journal compaction is the closed
  `replace_journal_prefix` operation. It authenticates a bounded index-zero
  prefix by record count, endpoints, and ordered record/content digest, replaces
  it with at most sixteen self-validating records bounded to four canonical MiB
  each. Normal Store rotation and GC may reclaim old payload bytes, while
  StateRoot maps retain cumulative record manifests and replacement-ID
  receipt/command digests so exact old replay and conflicting ID reuse remain
  decidable.
  Materialized `DurableState` retains only active journals and each journal's
  latest replacement. All-ever record manifests, replacement history, and
  coupled receipts remain reachable only through their StateRoot maps and exact
  typed point lookup; they are never rematerialized as duplicate aggregate
  maps.
  Run cancellation and Effect resolution have separate required StateRoot map
  roots and closed typed receipt insertion operations. Their optional
  materialized maps are omitted when empty; reserved generic application
  journals are not a receipt authority.
  Paged Machine transitions retain the original complete single-command batch
  manifest plus separately rooted staged material. The terminal batch receipt
  retains both the frozen source parent and the actual linear admission parent;
  unrelated Run progress between those parents is valid and never rewrites the
  original batch identity.
  Agent workspace coupling is a closed `agent_workspace { checkpoint }`
  variant with nineteen required members. It retains required-nullable batch
  identity/receipt and source/result Effect, outbox, and lease neighbors;
  only StartEffectDispatch carries a non-null full dispatch Clock. Its
  Continuation digests are content IDs under the existing Continuation state
  generation, not raw JSON digests. The entire Agent workspace coupled receipt
  has the twelve-MiB StateRoot-leaf limit; other coupled receipts retain their
  one-MiB limit. No Agent result receipt or physical result root is duplicated.
  Durable-owned operation payloads are closed typed definitions; generic
  `{ "type": "object" }` sidecars are forbidden. Clock removal is an explicit
  content-ID operation, and journal-plus-wait/effect coupling retains a closed
  `cymule.coupled-checkpoint-receipt/3` with complete typed checkpoint semantics
  and `machine_authority_root` wherever a Machine transition is coupled. Input
  completion retains its exact suspension receipt and exactly one journal whose
  identity equals the suspension journal; it has no standalone
  journal-wait-completion variant. Wait identities are lowercase SHA-256 content
  IDs. Resource input handoff is a separate closed variant binding its transfer,
  activation, source coupling/receipt, exact Run and Wait owner, result Artifact,
  and two distinct named activation manifests; the referenced source `/3`
  journal-set receipt owns the transfer manifests. `wait_activations` is omitted
  when empty and must have at least one member whenever present.
- `virtual-control.schema.json` freezes the current finite Virtual command and
  DTO set shared with the Rust profile: fenced work resolution, capacity-slot
  claim/renewal/recovery, Run weights, opaque-cursor region migration,
  compaction intent/certificate, exact rehydration, and Work occurrences. It
  defines no Engine transport, provider implementation, generic checkpoint,
  complete scheduler snapshot, or normalized runtime receipt.
  Ordinary virtual identities use 1..=512 non-control Unicode scalars; source
  and archive provider selectors keep their separate 256-scalar boundary.
  Migration Plans and requests retain the explicit immutable
  `migration_revision`, and every region retains `source_artifact`.
  Compaction commands retain complete bounded work, occurrence, and archived
  command selections plus `archive { binding, revision }`. Their command ID is
  derived by Rust; an SDK may copy the issued ID but never seal it locally.
  Certificates preserve both archived-work roots, ordered update digest,
  command count and required-nullable command root, plus the same exact archive
  generation. Runtime admission and archive/provider proof verification remain
  Rust authority.
  The retired checkpoint/journal-base schemas and old coupled-journal claim
  receipt have no current producer, reader, fallback, or version-domain entry.
  Their bytes live only in the explicit historical rejection fixture, which
  current Rust DTO decoders and the control schema must reject.
- `evolution-control.schema.json` owns the closed
  `cymule.evolution-control/5` command union shared by all SDKs. Occurrence
  selection requires a stable selection ID and exact ExecutionBinding Artifact;
  its response exposes the complete typed pin. Migration
  commands additionally pin source and target ExecutionBinding Artifacts; all
  commands carry only immutable Plan/Artifact identities, exact patches, pinned
  migration or shadow requests, observations, and deterministic gates. Provider
  endpoints, credentials, clocks, and Agent-loop state never enter this
  boundary.
- Every field that names an admitted Plan anywhere inside Evolution commands,
  provider descriptors, outcomes, receipts, and publication updates is an
  exact lowercase SHA-256 content ID. A non-empty legacy identity is never a
  valid Plan reference, including patch parents, rollout fallback/target
  Plans, observations, and migration descriptor endpoints.
- `live-evolution-control.schema.json` owns
  `cymule.live-evolution-control/6`. An `apply` carries only its exact nested
  semantic request; the retired outer `safe_point` member and public
  `MigrationSafePoint` shape are rejected. Migration and restart requests pin
  the source Run, Plan, and epoch while Durable derives the authenticated source
  witness at the same StateRoot. Generation `/5` is rejected without a
  compatibility reader.
- Engine successes contain no untyped durable or live-evolution object;
  `cymule.evolution-plugin/3` separately freezes migration/shadow process I/O.
  Migration egress carries the Durable-derived witness, complete source
  Continuation, input state, and source/target ExecutionBinding Artifacts; it
  is not the public semantic command. Failure is a closed tagged union:
  `contract` carries one structured `ContractViolation`, while all other
  categories carry an ASCII `^[a-z][a-z0-9_]{0,199}$` code and a 1..=2000
  Unicode-scalar message, exactly matching the sole Rust admission path.
- Engine v5 `clock_observed` carries exactly one `ClockObservationResult` with
  the requested `run_id` and nested reference. The predecessor response with a
  bare observation is invalid. Resource annotations have at least one member
  whenever present; the empty map has no wire distinct from omission.
