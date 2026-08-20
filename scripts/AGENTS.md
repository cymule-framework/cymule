# Verification Script Guidance

- `tests/harness/suites.toml` is the suite inventory. Keep leaf commands
  independently runnable and let `scripts/test_harness.py` own dependency
  expansion, risk routing, lane grouping, and machine-readable reports.
- Every leaf suite belongs to exactly one execution class: `deterministic`,
  `live_process`, or `live_provider`. The harness validates full coverage,
  includes the class in list/matrix/report output, and rejects duplicates.
- The same manifest owns path routes. Do not reintroduce a hard-coded route
  table in Python or workflow YAML; catalog validation rejects unknown suites.
- A narrow path route must select the smallest sufficient evidence family. A
  shared semantic/wire change selects every affected SDK; an unknown path,
  validation-infrastructure change, or incomplete route escalates to `full`.
- Every Cargo package `src/` or manifest change selects the owner and complete
  transitive reverse-dependency closure from Cargo metadata. `package_suites`
  maps that closure to behavioral leaves; a workspace compile is not a
  substitute for consumer tests. Keep the table exhaustive and validated.
- Commands in the manifest are argument arrays, never interpolated shell
  fragments. Route tests must pin both narrow selection and fail-closed
  escalation.
- Scripts must be non-interactive, fail closed, and run from any working
  directory.
- Do not hide skipped coverage. Optional-tool skips exit with code 77 and are
  recorded as `skipped`; execution failures are `infrastructure_error`, never
  a report whose aggregate status says passed.
- Runner exceptions and cancellation must publish the active command as
  `infrastructure_error`, mark remaining leaves `not_run`, write the report,
  and only then propagate the exception.
- Cross-language tests must use freshly built Rust binaries and a Plan ID sealed
  from the checked-in shared fixture.
- Every SDK also runs the same structured Engine negative fixture through that
  binary. Keep missing-envelope transport failure separate from remote semantic
  failure and assert retry disposition only where the Rust boundary proves it.
- The example leaf owns both the minimal Hello World path and the durable
  evaluation campaign's black-box crash, Resource, lease, and M4 tests. Keep it
  independently runnable; do not scatter those user-path checks across SDK or
  plugin leaves.
- The SQLite plugin route also selects the example leaf because campaign status
  relies on its non-mutating read-only observation contract.
- HTTP activation, timer activation, and restart-monotonic clock adapters own
  separate live-process suites. Do not recombine them into one activation leaf;
  each package must expose its process-death result independently.
- Export the Resource ID sealed from the checked-in Resource Candidate so every
  SDK verifies the same Rust-owned identity.
- Every SDK submits the shared wait activation fixture to the Rust Engine. This
  proves the closed wire boundary only; stateful source and consume-once cases
  stay in the M1 fault suite.
- Schema verification also validates the shared durable wait-condition fixture:
  owner is mandatory and closed even when its nested bind is null.
- Shared Artifact fixtures always carry `identity_version = cymule.artifact/2`;
  schema validation must reject missing or legacy identity versions in every
  public protocol family.
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
- Every SDK constructs the same closed M4 gate command and submits it to the
  Rust verifier. Stateful linking, migration/shadow plugin calls, observation
  gates, and lost-receipt recovery remain in the Rust evolution fault suite.
- Every SDK also constructs the shared unified live-evolution command. The Rust
  verifier rejects unknown fields and safe-point proofs on operations that do
  not accept them.
- Every SDK also constructs the same `/2` replacement-Run restart command with
  exact safe-point proof, source epoch, distinct Run IDs, input, and evidence.
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
- Rust CI remains split into static consumer compilation, semantic profiles,
  durable/live-process profiles, provider plugins, and release-package bytes.
  Do not collapse these witnesses into one long Rust job or rerun workspace
  behavioral tests before every owner leaf.
- `verify-soak.sh` owns only repeatable high-risk Rust properties and anomaly
  sweeps. Keep it out of `full`; scheduled soak complements, rather than
  duplicates, change-routed verification.
- The soak sweep repeats day-one plugin authority boundaries: SQLite
  contention, Resource chunk replay, HTTP/timer acknowledgement, process
  ambiguity, and incomplete MCP work. Add a plugin case only when it is
  deterministic and independently runnable by exact test name.
- `verify-analysis.sh` owns scheduled/manual coverage and mutation witnesses.
  Keep their exact tool versions in `analysis.yml`, their measured floors in the
  script, and their artifacts separate from normal lane reports.
- Keep semantic and day-one plugin coverage in separate reports and floors. A
  green aggregate may not hide an uncovered provider boundary or reduce the
  semantic-core baseline.
- Keep core mutation and the bounded M4 evolution mutation as separate suites.
  The M4 filter owns compatibility, safe-point, relink-admission, and restart
  laws; expand it deliberately when a new M4 admission law becomes normative.
- Day-one plugin mutation is a third independent bounded witness. Keep its
  filters on authority, acknowledgement, ambiguity, and protocol mapping;
  resource streaming remains covered by fault/soak until separately sharded.
- `cymule-clock-system` precedes timer adapters in the release catalog because
  timer clock injection reuses its wall-clock boundary. Keep its focused
  restart/backward-time witness in plugin coverage, mutation, and soak lanes.
- `crates-release.toml` is the single public Rust package order. The release
  verifier must match Cargo metadata exactly, run Cargo's whole-workspace
  publication dry-run, package the unpublished workspace as one set, reject
  dependency-path leakage, compare two archive hashes, and compile normalized
  package bytes through a local patch registry.
- The whole-workspace Cargo dry-run uses an ephemeral `[patch.crates-io]`
  pointing at the exact candidate workspace so coordinated inter-crate API
  changes compile as one release set. The patch is control input only: archive
  inspection must still reject every normalized dependency path.
- The package witness uses Cargo `--allow-dirty` so pre-commit candidate changes
  are the bytes under test. The actual publish command separately requires an
  exact annotated tag and a clean checkout; never weaken that release gate.
- crates.io publication is ordered and resumable. Before skipping an existing
  version, compare its registry checksum with the archive built from the exact
  tag; after every upload, wait for the index and verify downloaded bytes.
- Retry publication automatically only for crates.io's exact new-crate 429
  response with a parseable server retry timestamp. Bound both delay and retry
  count; authentication, checksum, malformed-limit, and other failures remain
  immediate hard failures.
- `CYMULE_RELEASE_WORKSPACE` is the GitHub-only immutable-tag payload root used
  by a newer reviewed controller. Require an absolute path and resolve every
  catalog, manifest, package, Git check, report, and consumer operation under
  that root; control checkout files must never become release payload bytes.
