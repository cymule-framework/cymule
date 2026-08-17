# Changelog

All notable changes to Cymule are documented here. The project follows Semantic
Versioning, with semantic compatibility additionally split into the version
domains described in `docs/specification.md`.

## [Unreleased]

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

## [0.1.0] - 2026-08-16

- Initial Rust-first semantic kernel and embedded runtime.
- Frozen `cymule.ir/1` plan candidate and canonical encoding.
- Causal event replay, command idempotency, epoch fencing, scopes, effect
  obligations, occurrence binding, and replay availability.
- TypeScript, Python, Rust, and Go SDKs with cross-language end-to-end tests.
- Provider-neutral process plugin protocol and conformance adapter.
- Partial optional MLIR workbench with generic-operation syntax and host-tool
  validation; a registered dialect and lowering passes remain proposed.
