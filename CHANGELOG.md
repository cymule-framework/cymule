# Changelog

All notable changes to Cymule are documented here. The project follows Semantic
Versioning, with semantic compatibility additionally split into the version
domains described in `docs/specification.md`.

## [Unreleased]

- Add a self-contained Hello World Flow and example plugin as the stable user
  quick start.

## [0.1.0] - 2026-08-16

- Initial Rust-first semantic kernel and embedded runtime.
- Frozen `cymule.ir/1` plan candidate and canonical encoding.
- Causal event replay, command idempotency, epoch fencing, scopes, effect
  obligations, occurrence binding, and replay availability.
- TypeScript, Python, Rust, and Go SDKs with cross-language end-to-end tests.
- Provider-neutral process plugin protocol and conformance adapter.
- Partial optional MLIR workbench with generic-operation syntax and host-tool
  validation; a registered dialect and lowering passes remain proposed.
