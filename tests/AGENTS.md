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
- Virtual checkpoint fixtures omit derived indexes and preserve opaque cursor,
  bounded frontier, claim fencing, and explicit parent lineage wire shapes.
- Virtual work occurrence fixtures preserve logical work identity separately
  from attempt epoch, owner, immutable binding, and exactly one disposition.
- Virtual work control fixtures carry a stable command ID and exact owner/epoch
  precondition; SDKs do not infer retry or cancellation policy from strings.
- Fairness tests distinguish materialization visibility from weighted dispatch,
  debit exact item cost, restore scheduler accounting, and use continuous
  high-priority arrivals to prove finite priority-aging progress.
- Region migration fixtures keep source cursors opaque, pin the migration
  binding, retain coverage evidence, and distinguish retirement from deletion.
  Stateful tests cover adapter verification, stale cursor/CAS, target conflict,
  split-then-merge lineage, existing-work preservation, reopen, and historical
  command replay.
