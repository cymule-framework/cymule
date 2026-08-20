# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- `engine-protocol.schema.json` owns both sides of `cymule.engine/1`: one
  versioned request envelope and one success-or-failure response envelope.
  Failure categories, phases, contract sides, issue bounds, and retry
  dispositions are closed and must match Rust plus every SDK. Contract issues
  preserve separate instance `path` and `schema_path` JSON Pointers.
- `plugin-protocol.schema.json` owns both request and response variants of
  `cymule.plugin/2`. There is no generic error response: a component may return
  a bounded `expected_failure`, while a protocol failure is an explicit
  `defect`. Effects return exact world outcomes.
- Every public `ArtifactRef` requires `identity_version = cymule.artifact/2`, a
  lowercase SHA-256 ID, and a closed lowercase path kind. The v2 identity and
  machine snapshot v5 replace their predecessors without fallback. The
  `artifact-type-contract.schema.json` file freezes recoverable canonical JSON
  contracts; opaque Artifacts do not use that schema.
- Keep this exact Artifact reference shape identical in every owning public
  schema and retain negative fixtures for missing/legacy versions, malformed or
  uppercase digests, and invalid kinds.
- `cymule.ir/2` adds the closed `invoke` operation. Future operation additions
  require a new IR version rather than widening this frozen schema in place.
- The existing `wait` operation has an optional `bind`; omission intentionally
  ignores the result. Engine success distinguishes completion from typed
  Embedded suspension, explicit release, and reconciliation boundaries without
  publishing a fake Continuation or string failure.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.
- `execution-binding.schema.json` freezes `cymule.execution-binding/1`. Rust
  additionally enforces normalized provider order, exact service ownership,
  Plan requirements, manifest equality, and content identities.
- `resource.schema.json` owns `cymule.resource/2` candidates/handles, separate
  locator sets/publications, content manifests/list proofs, exact lifecycle
  receipts, and `cymule.resource-handoff/2` producer provenance. Shape or
  integrity changes require Rust semantic validation, all SDKs, fixtures, and
  cross-language Resource ID tests.
- `wait-activation.schema.json` owns the provider-neutral
  `cymule.wait-activation/1` delivery record. Source, targets, and result must
  stay closed and pass Rust plus four-SDK fixture conformance; concrete clock,
  signal, queue, and transport fields never enter this schema.
- `wait-condition.schema.json` freezes the public M1 wait projection. Every
  wait owns an exact definition, invocation, Region path, site, and step;
  `bind` alone is nullable and remains nested inside that mandatory owner.
- `durable-control.schema.json` owns the closed
  `cymule.durable-control/1` mutation/query union. SDKs may construct start,
  resume, wait-activation, effect-release, and read-only query commands, but
  only the Rust M1 runtime may reduce them against a durable domain.
- `durable-storage.schema.json` freezes the provider-neutral M1 physical head,
  recursive delta, checkpoint envelope, and GC receipt. Rust additionally
  verifies every content identity, segment lineage, semantic revision, and the
  checkpoint-plus-suffix replay bound.
- `virtual-checkpoint.schema.json` owns `cymule.virtual-checkpoint/2`
  content-addressed cursor and bounded-frontier delta payloads. Each record
  authenticates its parent and resulting transition head and never repeats a
  full `VirtualSnapshot`. The derived parked-reason index is omitted from wire
  state and rebuilt from the closed parked-work map on restore.
  Its owned definitions also freeze `cymule.virtual-work-occurrence/2` and
  `cymule.virtual-work-control/1`; disposition variants are closed and preserve
  owner, work epoch, lease epoch, logical observation time, and binding
  preconditions.
  Scheduling policy, integer Run weights/deficits, dispatch sequence, ready age,
  and last selections are checkpoint authority; derived parked indexes remain
  omitted.
  Region migration definitions preserve opaque source cursors, split/merge
  cardinality, pinned adapter binding, coverage evidence, retirement lineage,
  and command receipts.
  Compaction definitions preserve a causal cut, bounded summary, content
  manifest, replay classification, retained binding/debug indexes, pinned
  compactor, and exact partial-rehydration selection. Concrete archive locators
  and credentials never enter this schema.
  Multi-worker scheduling definitions preserve capacity-slot identity, logical
  Clock values, work/lease fences, explicit recovery disposition, and future
  Run weight. Worker addresses, heartbeats, queue/provider fields, and topology
  remain outside semantic records.
- `evolution-control.schema.json` owns the closed
  `cymule.evolution-control/3` command union shared by all SDKs. Migration
  commands additionally pin source and target ExecutionBinding Artifacts; all commands carry
  only immutable Plan/Artifact identities, exact patches, pinned migration or
  shadow requests, observations, and deterministic gates. Provider endpoints,
  credentials, clocks, and Agent-loop state never enter this boundary.
