# ADR 0003: Provider-Neutral Profile Crates

Status: accepted on 2026-08-17.

## Decision

Keep `cymule-core` limited to frozen semantics. Implement M1-M4 as separate,
dependency-directed crates:

- `cymule-durable` owns durable single-domain contracts;
- `cymule-agent` owns typed agent interaction contracts;
- `cymule-virtual` owns bounded virtual-work scheduling;
- `cymule-evolution` owns Plan DAG and migration policy.

Each crate defines interfaces and deterministic reference state machines.
Concrete databases, object stores, queues, model providers, tool providers,
workspaces, clocks, and transports remain separately reviewable plugins.

## Consequences

- The trusted kernel does not grow a provider or orchestration dependency.
- Applications may adopt only the profiles they need.
- Concrete adapters must pass behavior-based profile conformance rather than
  being selected by product name.
- Cross-profile behavior is validated by end-to-end examples and SDK protocol
  fixtures instead of circular crate dependencies.
