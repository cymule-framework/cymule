# Conformance Asset Guidance

- Fixtures are shared across language SDKs and must stay language-neutral.
- The expected Plan ID is always computed by the Rust kernel from the checked-in
  candidate; never duplicate canonicalization in a test script.
- Cross-language tests must seal and execute through the real engine and process
  plugin, not mocks.
- Resource fixtures are sealed only by the Rust engine. Every SDK must submit
  the shared candidate and receive the same Resource ID; no fixture may contain
  credentials or a signed URL.
- Add fault-oriented tests for semantic changes, especially stale commands,
  fencing, scope closure, ambiguous effects, reconciliation, and replay.
- Wait activation fixtures contain only stable delivery/source/wait/Artifact
  identities. Stateful tests must cover redelivery, conflicting identity,
  source mismatch, consume-once competition, stale CAS, reopen, and epoch
  advance before resume.
