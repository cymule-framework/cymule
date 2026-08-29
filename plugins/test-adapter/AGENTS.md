# Test Adapter Guidance

- This executable is conformance infrastructure, not a production provider.
- Keep behavior deterministic and input-driven.
- It must exercise normal application and unknown-then-reconcile paths without
  ambient network, filesystem, clock, or random dependencies.
- Its provider settlement ledger is an explicit persistent locator supplied by
  `CYMULE_TEST_EFFECT_LEDGER_PATH` through the process configuration. Fresh
  process occurrences share that provider-owned database; the process executor
  never creates or owns it. SQLite writer contention is non-blocking and
  surfaces as provider failure; the adapter never waits behind an implicit busy
  timeout.
- The ledger retains an Applied provider value exactly as returned. A
  result-less Applied decision stays absent here; only the owning Durable
  execution boundary may materialize its canonical null Result Artifact.
- It must expose one input-selected `ExpectedFailure` so every SDK proves that
  declared application failure remains distinct from plugin defects.
- The `protocol_defect` component input exits successfully after emitting
  strict JSON with a response variant that cannot satisfy the admitted Call;
  this proves protocol defects remain distinct from nonzero process failure.
- Evolution ingress uses the public closed `cymule-evolution` wire envelope and
  the shared strict decoder. The migration descriptor pins the exact test Plan
  edge and compatibility identity, and both migrate and shadow requests return
  complete deterministic typed products; hand-parsed JSON or describe-only
  stubs are not conformance evidence. Valid requests that the deterministic
  operation cannot honor return the bounded typed failure envelope; malformed
  ingress emits no semantic response authority.
- Read stdin once through the larger fixed Evolution limit plus one byte, then
  route plugin/3 through `decode_plugin_request` and its smaller Core-Artifact
  ceiling. Direct test execution must not bypass the production wire bounds.
