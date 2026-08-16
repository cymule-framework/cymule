# Conformance Asset Guidance

- Fixtures are shared across language SDKs and must stay language-neutral.
- The expected Plan ID is always computed by the Rust kernel from the checked-in
  candidate; never duplicate canonicalization in a test script.
- Cross-language tests must seal and execute through the real engine and process
  plugin, not mocks.
- Add fault-oriented tests for semantic changes, especially stale commands,
  fencing, scope closure, ambiguous effects, reconciliation, and replay.

