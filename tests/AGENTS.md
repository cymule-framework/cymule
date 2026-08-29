# Conformance Asset Guidance

- Follow `docs/testing.md`: every durable fault test names the injected
  operation boundary, sweeps deterministic failure positions where practical,
  reopens authority after the fault, and runs an integrity probe. Compound
  recovery faults and soak matrices remain separate suites from focused tests.
- A seeded property/fuzz failure must print its seed and be minimized into a
  permanent regression fixture. Do not rely on wall-clock races when a CAS
  revision, epoch, counter, or explicit barrier can identify the interleaving.
- Shared test composition lives in the workspace-private `tests/test-world`
  crate only while at least three independently routed suites consume it. Keep
  clocks, random state, fault counters, observations, temporary domains, and
  child lifecycles owned by one test case; never add global hooks to production.
- Generated stateful command traces are authored once in Rust. A failure prints
  its seed, retained command path, exact Cargo replay command, and minimized
  language-neutral JSON; SDKs consume the promoted fixture instead of owning
  another generator.
- Shrinking must preserve the exact failure fingerprint, not merely any error.
  The minimized fixture records that fingerprint with the seed and path.
- User-example crash tests should execute the built binary and use explicit
  logical clock values. Exit after an identified durable boundary, reopen from
  public adapters, and assert the retained occurrence and Resource identities.
- Property tests run a bounded default case count in focused suites and honor
  `PROPTEST_CASES` in `rust-soak`; do not make ordinary test latency depend on
  soak-scale generation.
- Fixtures are shared across language SDKs and must stay language-neutral.
- Engine negative fixtures assert category, phase, code, and only an explicitly
  justified retry disposition through the real Rust CLI in all four SDKs.
  Transport loss must not manufacture replay safety.
- The unsupported-Engine fixture intentionally carries a v3-only extension:
  clients select/reject the unsupported generation before applying the closed
  v4 shape, yielding read-only `contract_violation/never` and mutating
  `unknown_world_outcome/reconcile` with the same stable protocol code.
- Every success fixture echoes the complete inner request exactly as serialized
  and sent. The matrix covers every request variant and rejects a missing echo,
  one-field mismatch, unknown/duplicate echo members, or the former v4
  response-only success before accepting the payload. Failure fixtures carry no
  request and reject one as an unknown member. SDK tests must prove this common
  correlation path without recomputing Rust Plan, Resource, Clock, rollout, or
  other derived identities. Include omitted-versus-explicit-null cases so a
  typed optional/defaulting decoder cannot erase a real wire mismatch.
- The shared failure fixture includes every exercised typed plugin outcome;
  protocol categories and stable codes may not live only in one SDK test.
- The expected Plan ID is always computed by the Rust kernel from the checked-in
  candidate; never duplicate canonicalization in a test script.
- Cross-language tests must seal and execute through the real engine and process
  plugin, not mocks.
- The shared Plan carries one no-mode auto-commit scope and one bound
  observational eager Effect. Core/runtime/durable negatives reject every
  deferred bound Effect before canonical mutation or business plugin calls.
- Call recovery tests distinguish a committed component occurrence from the
  duplicate-possible response-before-checkpoint window.
- The shared Plan exercises a reusable definition invocation so every SDK
  proves `cymule.ir/3` declaration, invocation input/result binding, Rust
  sealing, and real embedded execution.
- The shared evolution control fixture exercises one deterministic gate command
  through all four SDKs and the Rust verifier. Rust stateful tests separately
  prove transitive relinking, checked adapters, promotion/rollback, mixed Plan
  execution, stale CAS, and acknowledgement-loss recovery.
- The shared live-evolution fixture wraps one occurrence selection in an exact
  parent-template scope and carries distinct occurrence/selection identities
  plus an exact ExecutionBinding Artifact. Stateful Rust tests separately prove
  decision-attributed late evidence and atomic transitive relink/rollout
  publication plus version-selection/worker-claim/lease CAS coupling.
- Every stateful Engine live-evolution control fixture returns one complete
  `EvolutionCommit` and compares the echoed request, exact semantic command and
  receipt, observed StateRoot revision, and required-nullable committed
  revision. Stateful tests prove exact-key receipt replay, normalized mutation
  tamper rejection, and that an earlier template, Plan, decision, rollout, or
  occurrence pin cannot be silently reinterpreted. Coupled Virtual claim
  remains its own typed authority, shares the final CAS, and never fabricates a
  second M4 receipt or history/checkpoint surface.
- The shared durable-control fixture is the exact four-language explicit
  takeover envelope, including its execution-claim request, issued Clock
  reference shape, TTL, and expected fence. Rust validates the envelope through
  Engine v4; stateful restart-level tests prove actual receipt resolution and
  every mutation variant. SDKs transport the union and never become a second
  state reducer.
- The shared terminal cancellation response is a complete
  `cymule.run-cancellation-receipt/1`: it retains the normalized cancellation
  command and binds the Rust-derived reason Artifact only through the cancelled
  boundary. Its `receipt_id` authenticates the complete receipt and is not a
  Store revision. SDK fixtures compare the command echo and boundary reference
  but never duplicate the Artifact or receipt hash.
- The shared restart fixture proves the `/2` safe-point and replacement-Run wire
  contract across all SDKs. The complete proof appears once at nested
  `request.safe_point`; a live restart must omit the migration-only outer
  `safe_point`. Stateful Rust tests must reject stale durable proofs and
  preserve one restart receipt after acknowledgement loss.
- Resource fixtures are sealed only by the Rust engine. Every SDK must submit
  the shared candidate and receive the same Resource ID; no fixture may contain
  credentials or a signed URL.
- Add fault-oriented tests for semantic changes, especially stale commands,
  fencing, scope closure, ambiguous effects, reconciliation, and replay.
- M1 execution-ownership tests use two independently opened coordinators and
  explicit Clock evidence. They prove Busy before provider I/O, expiry without
  mutation, exact-fence takeover, same-occurrence/new-Attempt behavior, and late
  old-fence result rejection; process-local guards are not evidence.
- M4 negative tests must isolate one admission axis and use distinct command
  identities so a later idempotency conflict cannot mask a broken earlier
  check. The scheduled M4 mutation witness targets the profile-protocol
  Evolution reducer and must prove a nonempty required symbol inventory before
  execution; the Evolution host leaf separately owns process-wire conformance.
- M4 coverage is an independent profile-protocol Evolution artifact with its
  own floor and explicit nonempty-file gate; no host or sibling-profile average
  may substitute for it.
- Rust packaging tests operate on normalized `.crate` contents. They must prove
  deterministic archives, no dependency-path leakage, compilation of every
  public library/binary, and a user facade consumer before publication.
- Release-script tests require crates.io rate-limit recovery to match both the
  exact new-crate 429 reason and a bounded server timestamp. Never retry an
  authentication, checksum, malformed response, or unrelated registry error.
- Crate publication-graph tests cover normal, build, target-specific, and
  versioned dev workspace edges, prove path-only unversioned dev dependencies
  are stripped, pin the current Clock consumers, and reject a versioned-dev
  cycle before any terminal PUT.
- Release-security tests authenticate staged npm and crate bytes, bind them to
  the verified Git commit, validate npm provenance identity, reject broad
  ruleset bypass, and run the static workflow verifier. Version authorities and
  release-controller scripts must route to their package and security leaves.
- Release artifact negatives require an exact stage file set, canonical
  manifest basenames, real non-symlink files, and resolved paths contained by
  the stage directory. Workflow negatives also preserve the inert npm caller's
  exact read-only/OIDC ceiling, narrow tag-App authority, per-job narrowing,
  and protected tag/publication environment boundary. Controller tests prove
  raw tag-object lost-response convergence, pre-mutation BOM attestation,
  complete package/registry/schema/domain/migration validation in the pinned
  read-only stages, rejection when projection validation precedes attestation,
  separation of attestation from the Action-free `contents: write` job,
  a separate Administration-read live-preflight job and short-lived same-run
  settings receipt, per-mutation receipt/ref fencing before draft publication,
  missing/stale/foreign/tampered receipt rejection, asset replacement-race
  rejection, exact REST asset identity, and immutable Release projection.
  Harness tests also require every Required CI plan to attach the dedicated
  deterministic source-closure leaf without widening narrow path evidence.
  Tag-authority tests distinguish the configured GitHub App ID from its live
  bot user ID and reject a tagger email derived from the App ID.
- Plugin suites remain split by store, Resource, activation, executor,
  observability, and Agent-protocol ownership. A plugin change runs its leaf;
  manifest/catalog changes additionally run package verification.
- Real process-death tests may wrap a production `DurableStore` to place a
  marker immediately before or after CAS, then let the parent send `SIGKILL`.
  The wrapper belongs only in integration tests; never add a pause or crash
  switch to a reducer or production adapter.
- HTTP and timer live-process leaves cover retained ingress/schedule, target
  selection, both M1 activation-CAS sides, and both acknowledgement sides.
  Clock covers both public observation sides. Every case reads the exact
  barrier identity, reopens public authority, runs SQLite integrity checks, and
  proves redelivery or terminal acknowledgement as appropriate.
- Keep process death, injected I/O error, and modeled power loss as separate
  evidence. `SIGKILL` at an API or CAS barrier does not prove SQLite VFS write,
  sync, WAL, checkpoint, torn-sector, or filesystem reorder behavior. An absent
  faithful test VFS is unsupported coverage, never a skipped-as-passed lane.
- Store reopen complexity fixtures must add malformed unreachable objects and
  prove they are neither read nor treated as authority. Directory GC fixtures
  cover the durable publish-before-delete phases; SQLite statement triggers
  prove transaction rollback only, while the existing process-death suite owns
  real CAS-level SIGKILL evidence.
- Use `ManagedChild` for shared process-death tests. Its Drop path must kill and
  wait even when the caller forgets explicit termination; keep an independent
  leak probe in the live-process suite.
- A crash barrier is the exact marker payload, not path existence. Use
  `wait_for_content`; `fs::write` may expose a zero-length or partial file
  before the named boundary is complete.
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
- Wait activation fixtures use `cymule.wait-activation/2` and contain only
  stable delivery/source/wait/Artifact
  identities. Stateful tests must cover redelivery, conflicting identity,
  source mismatch, consume-once competition, stale CAS, reopen, and epoch
  advance before resume.
- Wait activation receipts use `cymule.wait-activation-receipt/3` and every SDK
  must bind the receipt's activation ID, source, and selected wait set to the
  submitted command. Shared terminal fixtures include the closed
  `effect_not_applied` boundary with an exact lowercase SHA-256 intent ID.
- The durable wait-condition fixture always carries its exact owner even when
  `owner.bind` is null. Missing owner or missing bind presence must fail the
  frozen schema instead of restoring the old optional-owner shape.
- The durable wait-condition fixture is the exact projection of a real sealed
  Plan site: its content-addressed wait ID derives from Run, Plan, invocation,
  and site, and its kind/schema/consume-once/owner/bind fields match that Plan.
  Do not replace it with a syntactically valid arbitrary digest.
- Wait-source tests separate index selection from transport acknowledgement.
  Lose the acknowledgement after the activation CAS, rebuild the index on
  reopen, redeliver the same identity, and prove exactly one activation.
- Current Virtual command fixtures preserve opaque cursors, bounded selections,
  exact provider generations, and claim fencing without a generic checkpoint
  or full scheduler-snapshot wire.
- Current Virtual controls validate exact finite DTOs and shared identities.
  The removed checkpoint/journal-base and coupled-journal claim receipt occur
  only in `retired-virtual-contracts.json`; current Rust decoders and the
  control schema reject all three. Never add a positive Schema fixture for a
  model with no current Rust producer. Region migration fixtures retain source
  Artifacts and provider revision; compaction fixtures use a command ID emitted
  by the real Rust constructor and complete bounded selections.
- `agent-workspace-checkpoint.json` is serialized from the Rust model's
  locally verified StartAbort fixture. Schema tests retain all nineteen
  required fields, nullable pair closure, exact content-ID Continuation
  digests, and phase-specific Clock presence; StateRoot fault tests separately
  prove that the retained source/result neighborhood is actual durable state.
- M3 provenance tests must prove wrong RegionSource binding/revision, dangling
  or mismatched payload bytes, stale Plan/binding admission, and lost fill
  acknowledgement followed by a different adapter advance neither Machine nor
  cursor/frontier/claim state.
- Higher-profile Virtual claim tests read the complete Rust receipt and its
  standard Evolution selection link from the same CAS. Reject changed Plan,
  binding, claim, or selection lineage. The public Rust `VirtualClaimOutcome`
  supplies verified claim/Plan authority; three-language fake receipt mirrors
  and callback-selected journal subsets are not proof.
- Virtual work occurrence fixtures preserve logical work identity separately
  from attempt epoch, owner, immutable binding, and exactly one disposition.
- Virtual work control fixtures carry a stable command ID and exact owner, work
  epoch, lease epoch, and opaque issued current-head Clock reference. Rust
  resolves and retains the complete observation in the same typed persistence CAS; SDKs
  do not author logical time or infer retry/cancellation policy from strings.
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
  Hot-state assertions prove the certificate retains a semantic Resource
  descriptor while the archived manifest bytes remain absent from Machine
  Artifacts before and after reopen.
  They also prove the current projection retains one cumulative archived-work root:
  after compact and reopen, a second region cannot rematerialize the same work
  ID or reuse its epoch/occurrence, while a Store-owned verified
  non-membership proof admits genuinely new work without scanning certificates.
- Multi-worker tests inject stale CAS and lost receipts at claim, renewal, and
  recovery; prove distinct slots can progress, one slot cannot overclaim,
  expiry rejects normal output, explicit takeover increments work epoch, and
  Run-weight commands replay without leaking old deficit.
- Every SDK rejects response-shaped malicious Engines with forged nested
  durable or live-evolution fields.
- Release-security mirror tests pin every runtime image digest, reject package
  installation or executable downloads in mirror jobs, exercise the real
  scanner's clean and provider-secret canaries, and prove secrets retained only
  in an old blob, raw historical pathname, or commit metadata block publication.
  They also prove inline allow annotations, repository ignore state, high-line
  positions, and files above the byte-exact scan limit cannot bypass admission.
  ZIP, tar/container, common compressed-archive magic, and Git LFS pointers are
  terminal rejection canaries. The no-credential scanner must scan the actual
  candidate job artifact, not only a fixture. Registry-only, commit-message,
  author, parent/history, and coherent bundle/manifest replacements must fail
  the terminal controller's independently reconstructed full public-history tip
  before any remote read, and an artifact-code sentinel must prove candidate
  code is never executed.
  Hostile Git config, URL rewrite, CA, proxy, and no-op movement fixtures must
  prove credentials are not redirected and stale readback cannot create a
  receipt. Static tests pin all validation images plus exact Rust and pnpm tool
  acquisition.
