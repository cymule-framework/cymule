# Documentation Guidance

- Write all documentation in English.
- Label behavior as `implemented`, `partial`, `proposed`, or `historical`.
- `specification.md` is normative. Architecture and research notes must not
  silently create new semantics.
- Keep one canonical explanation for each concept and link to it instead of
  copying it across files.
- User-facing documentation names capabilities directly. Reserve M0-M6 labels
  for the roadmap and maintainer-facing profile/specification cross-references.
- Any semantic object or transition added to documentation must map to code,
  schema, and a conformance test, or be explicitly marked proposed.
- Prefer technology-neutral property names. Concrete products may appear in
  research comparisons and adapter documentation, never as semantic authority.
- M4 is implemented only for one provider-neutral durable domain. Keep metrics,
  traffic movement, deployments, shadow sandboxes, schema transformation code,
  and Agent/session controllers explicitly outside the profile.
- `releasing.md` is the publication runbook. Keep package names, dependency
  order, immutable retry checks, OIDC-only trusted publishing, new-name
  ownership gates, and credential-removal requirements aligned with live
  workflows and scripts.
- `plugins.md` is the current user-facing adapter catalog. Keep implementation
  status, limitations, mature foundations, and RocksDB/P1 guidance aligned with
  live crates and focused suites.
