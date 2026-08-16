# Cymule

Cymule is a small, language-neutral execution semantics framework for durable,
effectful, and live-evolvable programs.

The trusted kernel is written in Rust. It owns a deliberately narrow contract:

- a frozen, canonical intermediate representation;
- content-addressed plans, causal events, and immutable artifacts;
- versioned effectful continuations and fenced attempts;
- transactional state scopes with transferred world-effect obligations;
- structurally identified effects with explicit `unknown` and reconciliation;
- idempotent, preconditioned commands;
- deterministic replay and explicit replay availability.

Everything else is an interface. Storage engines, activation transports,
executors, model providers, tool providers, secret systems, and effect handlers
are replaceable substrates or plugins. They are not named in canonical plans.

## Status

Version `0.1.0` implements the bounded Semantic Interpreter and Embedded M0
profiles. It is an executable framework and state-replay conformance reference,
not yet a persistent distributed runtime or exact execution-replay engine. See
[the conformance boundary](docs/conformance.md) and
[the roadmap](docs/roadmap.md).

## Repository map

```text
crates/cymule-core      small trusted Rust semantic kernel
crates/cymule-runtime   embedded runtime and plugin host
crates/cymule-sdk       native Rust authoring and client API
crates/cymule-cli       protocol-neutral command-line engine
sdk/                    TypeScript, Python, and Go SDKs
schemas/                frozen JSON Schema contracts
compiler/mlir           optional MLIR workbench
plugins/                plugin contracts and conformance fixtures
tests/                  cross-language and semantic conformance assets
docs/                   specification, architecture, and maintenance guidance
```

## Quick start

```sh
cargo build --workspace
./scripts/verify.sh
```

The language SDK tests build the same plan, ask the Rust engine to seal it, run
it through the external test adapter, and verify the same result and plan ID.

## Design principles

1. Strong kernel, small public model: `Flow -> Run -> Result`.
2. Four authoring boundaries: `call`, `wait`, `effect`, and `scope`.
3. Three canonical stores; graphs and current state are projections.
4. Semantic plans and concrete runtime bindings are separate.
5. Historical occurrences are immutable; only future defaults evolve.
6. External exactly-once is never fabricated without provider cooperation.
7. Conformance is behavioral and fault-oriented, not product-based.

## License

Licensed under either the Apache License, Version 2.0 or the MIT License, at your
option.
