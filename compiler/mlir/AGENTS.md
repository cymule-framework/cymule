# MLIR Workbench Guidance

- Pin verification to a known MLIR major version before claiming compatibility.
- Keep experimental operation names under the `cymule` namespace.
- Checked-in examples must parse with `mlir-opt --allow-unregistered-dialect`.
- Keep `cymule.invoke` definition/site attributes aligned with frozen
  `Operation::Invoke`; MLIR symbol lookup never becomes runtime registry lookup.
- Do not link LLVM or MLIR into `cymule-core` or the runtime.
- A future registered dialect must lower deterministically to
  `schemas/plan-candidate.schema.json` and pass the same Rust sealer.
