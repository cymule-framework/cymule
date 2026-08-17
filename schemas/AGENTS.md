# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.
- `resource.schema.json` owns `cymule.resource/1` candidates/handles and
  `cymule.resource-handoff/1`. Shape or integrity changes require Rust semantic
  validation, all SDKs, fixtures, and cross-language Resource ID tests.
- `wait-activation.schema.json` owns the provider-neutral
  `cymule.wait-activation/1` delivery record. Source, targets, and result must
  stay closed and pass Rust plus four-SDK fixture conformance; concrete clock,
  signal, queue, and transport fields never enter this schema.
- `virtual-checkpoint.schema.json` owns `cymule.virtual-checkpoint/1` cursor and
  bounded-frontier journal payloads. The derived parked-reason index is omitted
  from wire state and rebuilt from the closed parked-work map on restore.
