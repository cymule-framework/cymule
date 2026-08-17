# Verification Script Guidance

- `tests/harness/suites.toml` is the suite inventory. Keep leaf commands
  independently runnable and let `scripts/test_harness.py` own dependency
  expansion, risk routing, lane grouping, and machine-readable reports.
- The same manifest owns path routes. Do not reintroduce a hard-coded route
  table in Python or workflow YAML; catalog validation rejects unknown suites.
- A narrow path route must select the smallest sufficient evidence family. A
  shared semantic/wire change selects every affected SDK; an unknown path,
  validation-infrastructure change, or incomplete route escalates to `full`.
- Commands in the manifest are argument arrays, never interpolated shell
  fragments. Route tests must pin both narrow selection and fail-closed
  escalation.
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
  owner/work-epoch/lease-epoch/time-fenced control command. Stateful reduction
  remains in the Rust M3 controller and its M1 checkpoint fault suite.
- Every SDK parses the same opaque-cursor region migration command. Coverage
  validation and source retirement remain Rust M3/M1 CAS behavior.
- Every SDK constructs the same compaction and exact-occurrence rehydration
  commands. Archive content verification, certificate admission, and partial
  restore remain Rust M3/M1 CAS behavior.
- Every SDK constructs the same worker-slot claim, lease renewal, and expired
  claim recovery commands plus future Run-weight updates. M1 logical lease
  admission, deterministic work selection, and recovery fencing remain Rust
  controller behavior.
- Schema verification covers every `schemas/*.schema.json` file and must include
  positive and unknown-field rejection cases for each public protocol family.
- Keep host-native verification reproducible and avoid container-only workflows.
- GitHub publication builds a snapshot on top of the prior public GitHub commit.
  Never push private source ancestry or remote configuration.
- Public history containing workflow changes is pushed by `mirror.yml` with the
  encrypted `RETIRED_PRIVATE_PUSH_TOKEN`, whose GitHub authorization includes
  repository contents and workflow updates. The default Actions token cannot
  update workflow files and must not be used as a fallback.
- Public export removes private CI metadata and fails closed if a private host
  or project path remains in the snapshot.
- GitHub CI derives one exact change plan, groups selected suites into
  independent toolchain lanes, and uploads one JSON harness report per lane.
  A skipped lane means its risk was not selected, not that its test silently
  skipped.
- If a force-push event's prior SHA is absent or has no merge base, CI must
  select `full`. Never infer a narrow diff from an unreachable public history.
- Keep CI lanes as statically declared jobs selected by planner outputs. GitHub
  resolves `uses:` actions before step-level conditions, so a conditional
  matrix would download unrelated toolchain actions and defeat lane isolation.
- `verify-soak.sh` owns only repeatable high-risk Rust properties and anomaly
  sweeps. Keep it out of `full`; scheduled soak complements, rather than
  duplicates, change-routed verification.
- `verify-analysis.sh` owns scheduled/manual coverage and mutation witnesses.
  Keep their exact tool versions in `analysis.yml`, their measured floors in the
  script, and their artifacts separate from normal lane reports.
