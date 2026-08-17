# Conformance Asset Guidance

- Follow `docs/testing.md`: every durable fault test names the injected
  operation boundary, sweeps deterministic failure positions where practical,
  reopens authority after the fault, and runs an integrity probe. Compound
  recovery faults and soak matrices remain separate suites from focused tests.
- A seeded property/fuzz failure must print its seed and be minimized into a
  permanent regression fixture. Do not rely on wall-clock races when a CAS
  revision, epoch, counter, or explicit barrier can identify the interleaving.
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
- Virtual work control fixtures carry a stable command ID and exact owner, work
  epoch, lease epoch, and logical observation-time precondition; SDKs do not
  infer retry or cancellation policy from strings.
- Fairness tests distinguish materialization visibility from weighted dispatch,
  debit exact item cost, restore scheduler accounting, and use continuous
  high-priority arrivals to prove finite priority-aging progress.
- Region migration fixtures keep source cursors opaque, pin the migration
  binding, retain coverage evidence, and distinguish retirement from deletion.
  Stateful tests cover adapter verification, stale cursor/CAS, target conflict,
  split-then-merge lineage, existing-work preservation, reopen, and historical
  command replay.
- Compaction fixtures preserve a non-empty causal cut, pinned archive binding
  and revision, certificate identity, and exact rehydration occurrence set.
  Stateful tests sweep archive put/get failures, tamper bytes, stale CAS, reopen,
  and receipt replay; an archive adapter never validates its own certificate.
- Multi-worker tests inject stale CAS and lost receipts at claim, renewal, and
  recovery; prove distinct slots can progress, one slot cannot overclaim,
  expiry rejects normal output, explicit takeover increments work epoch, and
  Run-weight commands replay without leaking old deficit.
