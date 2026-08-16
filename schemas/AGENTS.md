# Schema Maintenance

- Schemas are frozen public contract artifacts, not informal examples.
- Use JSON Schema Draft 2020-12 and reject unknown fields at closed boundaries.
- A schema change requires a version-domain decision, fixtures, all SDK updates,
  and corresponding Rust deserialization and semantic-validation tests.
- Keep semantic validation in the Rust kernel. JSON Schema validates wire shape;
  it does not replace transition or authority rules.

