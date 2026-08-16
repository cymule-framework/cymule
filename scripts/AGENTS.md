# Verification Script Guidance

- Scripts must be non-interactive, fail closed, and run from any working
  directory.
- Do not hide skipped coverage. Optional-tool skips must print the exact reason.
- Cross-language tests must use freshly built Rust binaries and a Plan ID sealed
  from the checked-in shared fixture.
- Keep host-native verification reproducible and avoid container-only workflows.

