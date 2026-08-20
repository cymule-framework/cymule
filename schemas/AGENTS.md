# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- `engine-protocol.schema.json` owns both sides of `cymule.engine/1`: one
  versioned request envelope and one success-or-failure response envelope.
  Failure categories, phases, contract sides, issue bounds, and retry
  dispositions are closed and must match Rust plus every SDK.
- `cymule.ir/2` adds the closed `invoke` operation. Future operation additions
  require a new IR version rather than widening this frozen schema in place.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.
- `resource.schema.json` owns `cymule.resource/1` candidates/handles and
  `cymule.resource-handoff/1`. Shape or integrity changes require Rust semantic
  validation, all SDKs, fixtures, and cross-language Resource ID tests.
- `wait-activation.schema.json` owns the provider-neutral
  `cymule.wait-activation/1` delivery record. Source, targets, and result must
  stay closed and pass Rust plus four-SDK fixture conformance; concrete clock,
  signal, queue, and transport fields never enter this schema.
- `durable-control.schema.json` owns the closed
  `cymule.durable-control/1` mutation/query union. SDKs may construct start,
  resume, wait-activation, effect-release, and read-only query commands, but
  only the Rust M1 runtime may reduce them against a durable domain.
- `virtual-checkpoint.schema.json` owns `cymule.virtual-checkpoint/1` cursor and
  bounded-frontier journal payloads. The derived parked-reason index is omitted
  from wire state and rebuilt from the closed parked-work map on restore.
  Its owned definitions also freeze `cymule.virtual-work-occurrence/1` and
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
  `cymule.evolution-control/2` command union shared by all SDKs. Commands carry
  only immutable Plan/Artifact identities, exact patches, pinned migration or
  shadow requests, observations, and deterministic gates. Provider endpoints,
  credentials, clocks, and Agent-loop state never enter this boundary.
