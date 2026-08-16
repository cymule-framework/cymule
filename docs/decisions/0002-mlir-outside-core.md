# ADR 0002: Keep MLIR Outside the Runtime Core

Status: accepted on 2026-08-16.

## Decision

Use MLIR as an optional compiler workbench for analysis and progressive lowering.
The runtime consumes the smaller frozen Cymule IR. LLVM and MLIR are not kernel
dependencies.

## Consequences

- Compiler experiments can evolve without changing runtime semantics.
- The trusted runtime build remains small and portable.
- The workbench must lower to and validate against the same canonical schemas as
  every other frontend.

