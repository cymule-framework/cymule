# Cymule Project Handbook

## Authority

Use this precedence order when guidance conflicts:

1. Executable tests, frozen schemas, and live code.
2. The nearest `AGENTS.md` in the target path.
3. `docs/specification.md` for normative semantics.
4. `docs/architecture.md` for realization guidance.
5. Other documentation.

## Project invariants

- Keep the trusted semantic core in Rust and intentionally small. The core owns
  canonical identity, admission laws, deterministic reduction, and replay. It
  must not depend on a database, queue, network client, model SDK, object store,
  process supervisor, or UI framework.
- Canonical truth consists of sealed plans, causal events, and immutable
  artifacts. Views, indexes, graphs, attention items, and schedulers are
  rebuildable projections.
- Every raw JSON ingress uses the shared duplicate-rejecting decode contract
  before typed deserialization. CLI, SDK, plugin, canonical Artifact, and
  persisted-state readers must never pass wire bytes through a permissive
  object parser or a fallback decoder.
- Every Artifact reference pins `cymule.artifact/2`; its sole identity authority
  is the core length-prefixed helper. Typed JSON references additionally pin the
  exact content-addressed Artifact type contract in their kind. Opaque Artifact
  bytes remain schema-free. Do not retain v1 identity or snapshot fallback paths.
- Cross-Run resources use provider-neutral `cymule.resource/2` semantic
  descriptors. Locator sets are separate replaceable records; signed URLs,
  access grants, and credential revisions never enter Resource identity. Exact
  list operations require a content-addressed manifest descriptor and verified
  per-page inclusion proof. Never persist credentials in an Artifact or claim
  exact replay for a mutable external locator without immutable version or
  content evidence. Concrete object stores, drives, sandboxes, and URL fetchers
  belong behind resolver/store plugins.
- Framework-owned typed Artifacts use the closed exact
  `ArtifactTypeContract` set. Resource handoffs pin the producer Run,
  occurrence, and exact result Artifact. Retention uses idempotent pin, release,
  GC, deletion, and staging/chunk cleanup receipts; deletion and cleanup require
  provider absence readback.
- Plans describe meaning and requirements. Runtime bindings describe concrete
  realization. Never place provider names, credentials, endpoints, or deployment
  topology in canonical plan semantics.
- Every persisted occurrence pins an immutable occurrence binding. M0 persists
  Attempt and Effect bindings; new component or plugin-owned domain occurrence
  records must add the same protection before claiming exact execution replay.
  Updating future defaults must never reinterpret historical work.
- Scope closure commits declared state and transfers effect obligations. It does
  not claim that the external world has settled.
- Only observational effects may dispatch eagerly while a scope is open.
  Commit-gated effects wait for their owning scope; explicit effects remain
  prepared until a caller issues the release control after commit.
- An ambiguous dispatch becomes `unknown` and follows reconciliation. Never turn
  it into a fresh semantic intent or silently redispatch it.
- Durable Effect stages validate an exact canonical Machine delta against their
  outbox mutation. Enqueue, dispatch-start claim, observation, and reconciliation
  may not carry unrelated Plans, Events, commands, or Artifacts; `Unknown` Event
  and outbox state commit together.
- External signal and timer delivery is an identified durable activation, not a
  direct worker wake-up. Match the Plan-declared source, commit the activation
  receipt with wait results and Continuation readiness, and advance the epoch
  before a reopened `Ready` Continuation resumes.
- Durable Run creation publishes the initial Machine and first Continuation in
  one CAS. Never create canonical work that cannot be resumed after a lost
  acknowledgement.
- M1 storage moves one small CAS head over immutable content-addressed deltas.
  Coordinators emit closed typed operations and revisions form an incremental
  hash chain. Rotation writes fixed-size parent/covered-segment manifests; only
  explicit GC materializes a new base outside the writer lock. Never restore
  per-mutation whole-state clone/diff/hash authority, provider projection reads
  under CAS, arbitrary head/GC injection, or mixed legacy fallback.
- Machine history compaction replaces only a causally closed Event prefix with
  an authenticated base projection and exact identities. Retain the complete
  suffix and command receipts; CAS lineage and replay must survive stale writes
  and lost acknowledgements.
- Machine restore requires bidirectional closure between every retained or
  compacted Event identity and exactly one applied command receipt. A retained
  Event's command ID and command hash must match that receipt's command record;
  conflicts never claim an Event.
- Compacted-prefix authentication cumulatively binds ordered Event identities,
  command identities and semantic hashes, complete command-record digests, and
  the base projection digest. Restore recomputes this evidence; a shape-valid
  digest string is never authentication.
- Signal and timer transport belongs behind `WaitSourceDriver`. Drivers select
  only from the rebuildable parked-wait index, page indexed source identities
  fairly instead of scanning a fixed transport prefix, obey the hard target
  bound, and acknowledge only after durable target selection and the activation
  CAS; lost acknowledgement redelivers the identical activation identity and
  targets.
- Virtual-work cursors and bounded scheduler frontiers checkpoint through the M1
  application-journal CAS as content-addressed incremental deltas. Each delta
  has a hard encoded-size bound and authenticates its parent and resulting
  transition head; never repeat a full `VirtualSnapshot` in every record. Exact
  parked-wait indexes are rebuildable projections; activation and the
  corresponding indexed wake checkpoint commit atomically.
- Every virtual-work claim creates a binding-pinned, epoch-fenced occurrence
  before execution. Success, retry, park, failure, and cancellation are closed
  dispositions; a retry creates a later occurrence and never rewrites history.
- Weighted fairness applies to materialized, capability-compatible backlogged
  Runs. Persist integer weights, deficits, dispatch sequence, and ready age;
  region materialization uses a separate round-robin visibility guarantee.
- Region split/merge treats cursors as opaque. A pinned migration adapter must
  verify coverage evidence before one CAS retires sources and activates targets;
  old regions and already materialized work remain historical authority.
- Completed virtual history may move cold only behind an immutable byte archive
  interface. Rust computes and verifies a semantic manifest Resource descriptor,
  causal-cut certificate, summary digests, retained terminal fences/bindings,
  and replay availability; partial rehydration restores only explicitly selected
  exact occurrences. Archive products, locators, and credentials are plugin
  concerns. Hot state retains the descriptor and certificate, never the cold
  manifest bytes.
- Multi-worker M3 execution uses abstract capacity-slot leases, not worker-pool
  topology. Claim and lease admission share one M1 CAS; renewal advances the
  slot fence; normal resolution carries work/lease epochs and logical time;
  expired recovery is an explicit retry/fail/cancel command. Lease expiry alone
  never mutates state, and old worker output loses after expiry or takeover.
- Public mutation enters through typed commands with idempotent IDs and causal
  preconditions. Raw canonical event append is internal only.
- Prefer optimistic CAS, immutable records, idempotency, fencing epochs, and
  partitioned single-writer authority over locks. Core semantics must never
  depend on a blocking lock. A concrete adapter may use narrowly scoped,
  non-blocking writer exclusion only when required to implement its CAS contract
  and must surface contention instead of waiting indefinitely.
- Cross-language SDKs author the same frozen IR and use the same engine contract.
  They must not implement a second reducer or invent language-specific semantics.
- `cymule.engine/2` is the only CLI Engine transport. Every request and every
  success or failure uses its versioned envelope; stderr and process status are
  transport diagnostics, never a second semantic error channel. A missing
  response never implies that retrying a potentially mutating request is safe.
- Engine clients accept exactly one success response or one failure object.
  Success tags, nested execution outcomes, and returned evolution commands are
  closed unions; unknown or overlapping variants and fields fail transport
  validation before reaching SDK callers.
- The public `DurableEngine` is a transport facade over stateful Rust
  `execute_durable` and `execute_live_evolution` requests. `start`, `get`,
  `resume`, `signal`, `release`, and `evolve` must never fall back to local SDK
  reduction or validation-only receipts.
- Cross-language JSON accepts only unique object keys, finite numbers, and
  integers in the shared exact range `-9007199254740991..=9007199254740991`.
  A lost deadline or cancellation response after mutation begins is an
  `unknown_world_outcome` requiring reconciliation.
- `cymule.plugin/2` is the only process-plugin protocol. Expected component
  failures and defects are distinct closed response variants; an unclassified
  process error is never an expected application result. The official Unix
  process executor launches a fresh private copy of its captured closure; its
  execution-binding revision covers executable bytes, arguments, explicit
  environment, working tree, runtime closure, deadline, and limits. Plugin
  stderr never enters an Engine failure.
- `cymule.ir/2` reusable definition calls resolve inside one immutable Plan.
  Logical latest-compatible references are linked by M4 into a new parent Plan;
  a sealed Plan never dereferences a mutable `latest` alias at runtime.
- `cymule_core::seal_plan` is the only Plan sealing authority. It rejects every
  recursive definition SCC, including invokes nested under scopes, and compiles
  schemas as Draft 2020-12 with external retrieval disabled. Machine insertion
  and restore reverify the same admission.
- Every durable `WaitCondition` pins its exact definition, invocation, Region
  path, site, and step. Only its nested local bind is optional. Activation
  atomically commits the result Artifact, completed wait, optional frame local,
  and Continuation readiness. Embedded execution returns a typed boundary and
  never claims a Continuation.
- Reusable modules resolve their complete acyclic dependency closure before
  sealing. Store every exact revision in the linked record, derive reverse
  indexes from registry state, and make transitive compatible updates create a
  new future parent Plan without rewriting any historical Plan or invocation.
- `LatestCompatible` is the actual API and Serde default. Before advancing an
  existing future head, compare entry-reachable component, effect, wait,
  capability, and authority surfaces; any widening retains the old head.
- M4 rollout state is evidence-driven and future-only. Migration/shadow code is
  a pinned plugin, observations match immutable occurrence pins, and only Rust
  evaluates deterministic promotion/rollback gates. SDKs carry the closed
  control union without duplicating these decisions.
- M4 occurrence admission pins semantic `plan_id` separately from an exact
  `cymule.execution-binding/1` Artifact. Never store a Plan ID in
  `occurrence_binding`. A safe-point migration commits its receipt and output
  Artifacts together with the Machine Plan/binding transition, Continuation
  state replacement, epoch advance, and new Attempt. A lost acknowledgement
  reopens that one committed transition without reinvoking the adapter.
- Runtime composition dispatches each bound operation to the exact admitted
  provider. Live manifests advertise capability only; they cannot select a
  provider, widen authority, or make an unbound operation callable.
- Never accept a caller boolean as migration-safe-point authority. Derive a
  content-addressed proof from a quiescent durable Continuation and revalidate it
  against M1 before adapter invocation or replacement-Run authorization.
- New behavior is provider-neutral by default. Concrete persistence, activation,
  execution, model, tool, and effect integrations belong behind plugin or
  substrate interfaces.
- Day-one official plugins cover SQLite/directory durability, filesystem and
  Apache object-store Resources, HTTP/timer activation, restart-monotonic clock
  observation, process execution, OpenTelemetry observation, and RMCP tool
  mapping. They remain independent crates and may not move their
  runtime/provider semantics into core.
- Domain-specific Sessions, Agent Loops, transport streams, protocol objects,
  and their controllers belong in optional plugins. Core crates, CLI, and SDKs
  expose only technology-neutral semantic and substrate contracts.
- Keep all source code, comments, documentation, commit messages, schemas, and
  user-facing project metadata in English.
- User-facing README files describe observable capabilities and limits without
  M0-M6 milestone labels. Keep milestone sequencing in the roadmap and
  maintainer profile/specification documents.
- User quick starts lead with a concrete scenario, the failure or cost being
  avoided, and an observable outcome. Defer CAS, journal, occurrence, binding,
  and other implementation vocabulary to architecture or conformance material.
- GitHub Actions is the only publication authority for every public artifact,
  including packages, release assets, and future registry distributions. Local
  commands may build, test, stage, and inspect release bytes, but must never
  publish them or require a long-lived registry token.
- Every external Action is pinned to a full immutable commit. Verification,
  exact-SHA soak, build, test, and archive staging run without OIDC; only a
  protected terminal environment grants the minimal publisher `id-token`.
- The public GitHub repository never stores private-source credentials or a
  mirror controller. Private CI rewrites a public-only snapshot, preserves
  commit metadata, pushes it, and reads back the exact public tip.
- Public Rust crates share the TypeScript release version. `cymule` is the Rust
  facade and `cymule-cli` is the binary package; profile/plugin crates publish
  in the dependency order owned by `scripts/crates-release.toml`. Retry only
  after comparing registry and exact-tag archive checksums.

## Change discipline

- Read the nearest nested `AGENTS.md` before editing a directory.
- Treat generated files and lockfiles as derived artifacts; update their source
  and regenerate them in the same change.
- Any semantic change requires a version-domain decision and updates to the
  normative specification, schemas, conformance tests, and SDK fixtures.
- Add a root rule only when it applies across multiple project areas. Put domain
  detail in the nearest nested handbook.
- Do not claim a conformance profile unless its complete fault-oriented suite
  passes. Mark planned and partial behavior explicitly.

## Required verification

Use `python3 scripts/test_harness.py plan --base <trusted-ref>` and run its
selected suites before an ordinary scoped commit. Run `./scripts/verify.sh` for
semantic version changes, profile claims, shared schemas, release/publication
changes, harness or CI changes, unknown routes, and before claiming complete
repository verification. The suite model and fault-test rules live in
`docs/testing.md`. At minimum, applicable changes must preserve:

- Rust formatting, Clippy, unit tests, and documentation build;
- deterministic canonical IDs and replay digests;
- JSON Schema validation fixtures;
- TypeScript, Python, Rust, and Go SDK end-to-end tests against the Rust engine;
- the MLIR workbench smoke test when the pinned host MLIR toolchain is available.

Run `python3 scripts/test_harness.py run rust-soak` only for scheduled, release,
or explicit anomaly-depth verification; keep it independent from focused local
feedback.
