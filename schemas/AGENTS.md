# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.
- `agent-protocol.schema.json` owns the frozen M2 AgentUpdate and host-occurrence
  and reconciliation wire shape. A binding or lifecycle change must update its
  fixture, Rust validation, profile documentation, and future SDK interaction
  clients.
- `cymule.agent-stream/1` records are an independent M2 version domain inside
  `agent-protocol.schema.json`. A stream change requires reducer tests, the
  shared fixture, all SDK wire types, and atomic M1 finalization evidence.
- `resource.schema.json` owns `cymule.resource/1` candidates/handles and
  `cymule.resource-handoff/1`. Shape or integrity changes require Rust semantic
  validation, all SDKs, fixtures, and cross-language Resource ID tests.
