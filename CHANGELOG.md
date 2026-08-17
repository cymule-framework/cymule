# Changelog

All notable changes to Cymule are documented here. The project follows Semantic
Versioning, with semantic compatibility additionally split into the version
domains described in `docs/specification.md`.

## [Unreleased]

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
