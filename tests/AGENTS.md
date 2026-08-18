# Conformance Asset Guidance

- Follow `docs/testing.md`: every durable fault test names the injected
  operation boundary, sweeps deterministic failure positions where practical,
  reopens authority after the fault, and runs an integrity probe. Compound
  recovery faults and soak matrices remain separate suites from focused tests.
- A seeded property/fuzz failure must print its seed and be minimized into a
  permanent regression fixture. Do not rely on wall-clock races when a CAS
  revision, epoch, counter, or explicit barrier can identify the interleaving.
- Property tests run a bounded default case count in focused suites and honor
  `PROPTEST_CASES` in `rust-soak`; do not make ordinary test latency depend on
  soak-scale generation.
- Fixtures are shared across language SDKs and must stay language-neutral.
- The expected Plan ID is always computed by the Rust kernel from the checked-in
  candidate; never duplicate canonicalization in a test script.
- Cross-language tests must seal and execute through the real engine and process
  plugin, not mocks.
- The shared Plan exercises a reusable definition invocation so every SDK
  proves `cymule.ir/2` declaration, invocation input/result binding, Rust
  sealing, and real embedded execution.
- The shared evolution control fixture exercises one deterministic gate command
  through all four SDKs and the Rust verifier. Rust stateful tests separately
  prove transitive relinking, checked adapters, promotion/rollback, mixed Plan
  execution, stale CAS, and acknowledgement-loss recovery.
- The shared restart fixture proves the `/2` safe-point and replacement-Run wire
  contract across all SDKs. Stateful Rust tests must reject stale durable proofs
  and preserve one restart receipt after acknowledgement loss.
- Resource fixtures are sealed only by the Rust engine. Every SDK must submit
  the shared candidate and receive the same Resource ID; no fixture may contain
  credentials or a signed URL.
- Add fault-oriented tests for semantic changes, especially stale commands,
  fencing, scope closure, ambiguous effects, reconciliation, and replay.
- M4 negative tests must isolate one admission axis and use distinct command
  identities so a later idempotency conflict cannot mask a broken earlier
  check. The scheduled M4 mutation witness is the regression probe for this.
- Rust packaging tests operate on normalized `.crate` contents. They must prove
  deterministic archives, no dependency-path leakage, compilation of every
  public library/binary, and a user facade consumer before publication.
- Plugin suites remain split by store, Resource, activation, executor,
  observability, and Agent-protocol ownership. A plugin change runs its leaf;
  manifest/catalog changes additionally run package verification.
- Effect fault matrices distinguish prepare-response loss, durable enqueue,
  scope commit, dispatch-start claim, provider application, Applied settlement,
  and Unknown observation. Assert exact provider call counts and reject
  unrelated Machine deltas at every outbox stage.
- Compound recovery tests stack a second durable or acknowledgement failure on
  an already ambiguous effect. Reopen between faults and prove the original
  intent is reconciled once without provider redispatch.
- Nested-scope restart tests must fault both before and after the child commit,
  prove no staged effect dispatches while its scope is open, and reopen from the
  persisted region path without repeating completed component occurrences.
- Eager-effect tests retain the frame until a durable result binding exists.
  Explicit-release tests prove resume alone performs no dispatch and retry the
  same release after claim or settlement receipt loss.
- Wait activation fixtures contain only stable delivery/source/wait/Artifact
  identities. Stateful tests must cover redelivery, conflicting identity,
  source mismatch, consume-once competition, stale CAS, reopen, and epoch
  advance before resume.
- Wait-source tests separate index selection from transport acknowledgement.
  Lose the acknowledgement after the activation CAS, rebuild the index on
  reopen, redeliver the same identity, and prove exactly one activation.
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
