# MLIR Workbench Guidance

- Pin verification to a known MLIR major version before claiming compatibility.
- Keep experimental operation names under the `cymule` namespace.
- Checked-in examples must parse with `mlir-opt --allow-unregistered-dialect`.
- Missing optional MLIR tooling exits with code 77 so the test harness records
  an explicit skip rather than a false pass. A configured but broken tool is a
  normal failure.
- Keep `cymule.invoke` definition/site attributes aligned with frozen
  `Operation::Invoke`; MLIR symbol lookup never becomes runtime registry lookup.
- Do not link LLVM or MLIR into `cymule-core` or the runtime.
- A future registered dialect must lower deterministically to
  `schemas/plan-candidate.schema.json` and pass the same Rust sealer.
