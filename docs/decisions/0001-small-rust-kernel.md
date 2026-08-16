# ADR 0001: A Small Rust Semantic Kernel

Status: accepted on 2026-08-16.

## Decision

Implement the only authoritative admission and reducer in Rust. Keep it as a
library with no provider, network, persistence, scheduler, model, or UI
dependencies. Cross-language SDKs author data and call an engine; they do not
reimplement semantics.

## Consequences

- One executable source of semantic truth is easier to audit and test.
- SDK behavior converges on one canonical ID and transition implementation.
- Language integrations require an engine boundary instead of an independent
  in-process reducer.
- New convenience APIs must lower to the frozen IR and command protocol.

