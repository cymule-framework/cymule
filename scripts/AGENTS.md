# Verification Script Guidance

- Scripts must be non-interactive, fail closed, and run from any working
  directory.
- Do not hide skipped coverage. Optional-tool skips must print the exact reason.
- Cross-language tests must use freshly built Rust binaries and a Plan ID sealed
  from the checked-in shared fixture.
- Export the Resource ID sealed from the checked-in Resource Candidate so every
  SDK verifies the same Rust-owned identity.
- Every SDK submits the shared wait activation fixture to the Rust Engine. This
  proves the closed wire boundary only; stateful source and consume-once cases
  stay in the M1 fault suite.
- Every SDK parses the same virtual work occurrence and constructs the same
  owner/epoch-fenced control command. Stateful reduction remains in the Rust M3
  controller and its M1 checkpoint fault suite.
- Schema verification covers every `schemas/*.schema.json` file and must include
  positive and unknown-field rejection cases for each public protocol family.
- Keep host-native verification reproducible and avoid container-only workflows.
- GitHub publication builds a snapshot on top of the prior public GitHub commit.
  Never push private source ancestry or remote configuration.
- Public export removes private CI metadata and fails closed if a private host
  or project path remains in the snapshot.
