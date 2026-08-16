# Compiler Workbench Guidance

- Compiler workbenches are optional authoring tools and must not become runtime
  or kernel dependencies.
- Every lowering target is the frozen Plan Candidate contract.
- Mark experimental syntax and incomplete lowering explicitly.
- Semantic validation remains authoritative in `cymule-core` after lowering.

