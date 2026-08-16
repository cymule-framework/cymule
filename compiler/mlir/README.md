# Experimental MLIR Workbench

Status: partial.

This directory reserves a small MLIR authoring surface for compiler tooling.
The checked-in generic-form example uses unregistered `cymule.*` operations and
is syntax-checked by MLIR 22. It demonstrates the intended progressive-lowering
boundary without adding LLVM or MLIR to the runtime dependency graph.

Implemented:

- generic MLIR syntax for `flow`, `input`, `call`, `effect`, and `result`;
- host-tool smoke validation with `mlir-opt`;
- an explicit mapping from experimental operations to frozen IR fields.

Proposed:

- a registered dialect with TableGen operation definitions;
- structural verification interfaces and canonicalization passes;
- deterministic lowering to `cymule.ir/1` Plan Candidates;
- source-location and diagnostic round trips for every SDK frontend.

The Rust sealer remains authoritative even after those pieces exist.

