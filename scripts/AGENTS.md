# Verification Script Guidance

- Scripts must be non-interactive, fail closed, and run from any working
  directory.
- Do not hide skipped coverage. Optional-tool skips must print the exact reason.
- Cross-language tests must use freshly built Rust binaries and a Plan ID sealed
  from the checked-in shared fixture.
- Keep host-native verification reproducible and avoid container-only workflows.
- GitHub publication builds a snapshot on top of the prior public GitHub commit.
  Never push private source ancestry or remote configuration.
- Public export removes private CI metadata and fails closed if a private host
  or project path remains in the snapshot.
