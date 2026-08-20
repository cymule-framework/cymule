# Changelog

All notable changes to Cymule are documented here. The project follows Semantic
Versioning, with semantic compatibility additionally split into the version
domains described in `docs/specification.md`.

## [Unreleased]

- Replace `cymule.virtual-checkpoint/1` whole-snapshot journal records with
  `cymule.virtual-checkpoint/2` content-addressed incremental deltas, a 4 MiB
  canonical delta bound, authenticated lineage, linear history, and exact
  reopen.
- Make durable HTTP signal selection use fair parked-key cursor pages plus an
  indexed SQLite match, and reject timer acknowledgement before durable target
  selection.
- Complete M4 safe-point migration as one lost-acknowledgement-safe CAS that
  changes Machine Plan/binding, Continuation state, epoch, Attempt, Artifacts,
  and evolution receipt together.
- Separate virtual occurrence Plan pins from exact ExecutionBinding Artifact
  pins, retain the authoritative fallback across rollback/relink, and route
  composed operations to their exact admitted providers.
- Advance the affected command, Event, Machine, durable, evolution, live
  evolution, virtual occurrence, and virtual claim wire domains.

## [0.1.4] - 2026-08-18

- Add day-one SQLite and filesystem/object-store persistence plugins with
  immediate contention, idempotent chunking, conditional publication, and
  content verification.
- Add HTTP signal and durable timer activation sources that acknowledge only
  after M1 admission, plus a hardened bounded process executor.
- Add composable OpenTelemetry/OTLP observations and an official RMCP tool
  adapter without introducing Agent Loop semantics into the framework.
- Split every plugin family into independently routed Rust verification suites
  and extend the ordered crates.io release catalog.
- Make first-publication recovery honor crates.io's bounded new-crate rate-limit
  timestamp without retrying unrelated registry failures.
- Separate the current release controller from immutable tag payloads so an old
  partial release can use reviewed recovery logic without changing its bytes.

## [0.1.3] - 2026-08-18

- Publish the canonical Rust facade as `cymule` plus the CLI, semantic profile
  crates, and official reusable plugins through an ordered crates.io release.
- Add deterministic whole-workspace Cargo archives, normalized-manifest
  compilation, checksum-checked retries, and fresh registry consumer/install
  verification.
- Add an idempotent GitHub Actions crates.io workflow with temporary
  first-release bootstrap and OIDC trusted publishing for normal releases.

## [0.1.2] - 2026-08-18

- Make `latest_compatible` the actual reference default and block automatic
  future-head changes that widen reachable component, effect, wait,
  capability, or authority surfaces.
- Replace caller-asserted migration booleans with content-addressed safe-point
  proofs verified against durable Continuations, and add first-class
  `restart_under_new_plan` authorization through `cymule.evolution-control/2`
  across all four SDKs.
- Add a separately sharded M4 mutation witness for compatibility, safe-point,
  automatic relink, and replacement-Run admission laws.

## [0.1.1] - 2026-08-18

- Introduce frozen `cymule.ir/2` with reusable local definition invocation,
  durable invocation frames, and matching TypeScript, Python, Rust, and Go
  authoring/execution conformance.
- Add latest-compatible reusable-module registry linking with exact-schema
  compatibility, transitive dependent relinking, pinned revisions, historical
  parent Plan retention, and durable tamper-checked recovery.
- Add a self-contained Hello World Flow and example plugin as the stable user
  quick start.
- Publish GitHub-native repository metadata, CI, and a clean-history public
  mirror workflow.
- Add the first M1 durable profile foundation: portable Machine snapshots,
  whole-state CAS, full Continuations, durable waits, leases, effect outbox,
  component occurrences, snapshot records, and an atomic directory adapter.
- Add resumable sequential call/wait execution with process reopen, Attempt
  epoch advancement, and exact component-result replay.
- Add the M2 agent interaction foundation with typed Session updates,
  context/model/tool/permission/elicitation/workspace interfaces, ordered
  projections, and a tested model-tool-model reference turn driver.
- Add the M3 virtual-work foundation with opaque cursors, bounded frontiers,
  deterministic Run fairness, capability claims, fencing, parked indexes, and
  million-item snapshot/restore tests.
- Publish the TypeScript SDK as the public `cymule` npm package through GitHub
  Actions trusted publishing with provenance.
- Adopt a project-wide build-versus-adopt policy and non-blocking lock boundary;
  prefer Tokio and other maintained mechanisms below Cymule semantics.
- Add the M4 evolution foundation with immutable Plan DAG edges, impact cones,
  occurrence pins, deterministic canary and rollback, safe-point migration
  receipts, shadow evidence, and cycle/fault tests.
- Complete the bounded M4 profile with transitive reusable modules, durable
  registry recovery, exact reviewed patches, checked migration/shadow plugins,
  deterministic evidence gates, mixed-version dispatch, and four-SDK controls.
- Add resumable commit-gated effect execution with fenced outbox claims and a
  crash-after-provider-application test proving recovery reconciles without
  redispatch.
- Add authenticated canonical Event-prefix compaction with exact suffix
  rehydration and atomic Resource handoff-to-input activation, both fault-tested
  for stale writers and lost acknowledgements.

## [0.1.0] - 2026-08-16

- Initial Rust-first semantic kernel and embedded runtime.
- Frozen `cymule.ir/1` plan candidate and canonical encoding.
- Causal event replay, command idempotency, epoch fencing, scopes, effect
  obligations, occurrence binding, and replay availability.
- TypeScript, Python, Rust, and Go SDKs with cross-language end-to-end tests.
- Provider-neutral process plugin protocol and conformance adapter.
- Partial optional MLIR workbench with generic-operation syntax and host-tool
  validation; a registered dialect and lowering passes remain proposed.
