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
- Shared typed query fixtures also route to the owning Rust behavior suite.
  The Applied EffectSummary fixture selects `rust-durable` in addition to the
  protocol and four SDK lanes already owned by the generic fixture route.
- `verify-sdk.sh` uses the locked Cargo graph and runs the Rust client unit and
  public-field facade tests beside real cross-language transport conformance. Re-exported
  nested DTOs must remain nameable from the `cymule` package alone.
- `verify-sdk.sh`, `verify-protocol.sh`, and the self-hosting campaign use the
  named Cargo `conformance` profile for executable fixtures. That profile keeps
  dev/test semantics while removing platform-specific debug-symbol bulk, so
  the same artifacts must fit the fixed 64 MiB SDK and 128 MiB campaign closure
  budgets on every CI host. Never grow a closure budget to admit debug metadata.
- The process-heavy durable campaign always runs as its own Cargo invocation
  with `--test-threads=1`. Its cases intentionally create many sealed process
  occurrences; case-level parallelism measures host I/O contention instead of
  campaign semantics and can manufacture deadline failures.
- Every SDK also runs the same structured Engine negative fixture through that
  binary. Keep missing-envelope transport failure separate from remote semantic
  failure and assert retry disposition only where the Rust boundary proves it.
- Protocol and SDK gates cover the required Engine v5 success request echo for
  every inner request variant. Reject missing/mismatched echoes, duplicate or
  unknown echo fields, and every predecessor success shape; failure carries
  no request. Include omitted-versus-explicit-null mismatches. Correlation
  compares the actual serialized wire, including member presence, and never
  depends on an SDK reimplementation of Rust-derived identities.
- Schema/SDK conformance pins `cymule_runtime::MAX_ENGINE_REQUEST_BYTES` as the
  sole Rust 64 MiB envelope authority and locks the TypeScript, Python, and Go
  mirrors. Each SDK must prove exact-limit admission, max-plus-one pre-spawn
  rejection, valid early failure preservation, and rejection of an early-close
  forged success.
- Package witnesses cover all four SDKs: normalized Cargo archives, both npm
  names, a wheel installed into a clean virtual environment, and a fresh Go
  consumer module. Source-tree imports are not package evidence.
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
- Every SDK also constructs the shared unified live-evolution occurrence
  selection with distinct selection and occurrence identities plus an exact
  ExecutionBinding Artifact. The Rust verifier rejects unknown fields and
  safe-point proofs on operations that do not accept them.
- Every SDK also constructs the same `/2` replacement-Run restart command with
  exact safe-point proof, source epoch, distinct Run IDs, input, and evidence.
- Schema verification covers every `schemas/*.schema.json` file and must include
  positive and unknown-field rejection cases for each public protocol family.
- Keep host-native verification reproducible and avoid container-only workflows.
- Public history is rewritten and published only by the private-source
  `.gitlab/scripts/publish-public-mirror.sh` controller. Public Actions contain
  neither source credentials nor a mirror/force-push workflow. The export
  removes all private CI metadata, preserves commit metadata, and fails closed
  if a private host or project path remains anywhere in history.
  Candidate construction is credential-free and uses the exact digest-pinned
  Python/Git image with no installed dependency; its artifact is a proposal,
  not rewrite authority. A separate credential-free Gitleaks job binds and
  scans that actual bundle, including every reachable blob, commit metadata,
  and raw pathname, and runs the live provider-secret canaries. The
  credential-bearing job uses the same pinned Python/Git bytes as candidate
  construction, installs nothing, independently reruns the sole canonical
  rewriter from the frozen private tip, and exact-matches the complete expected
  public tip before any remote read. It never executes candidate code. The
  mirror-stage barrier must include the actual scanner and every other verify
  job.
  Static release verification also requires every Gitleaks entrypoint to use
  the single exact pinned-version normalizer, the pinned OCI job to prove its
  raw `v8.24.3` identity, and ZIP classification to validate an actual end
  record/central-directory/local-header chain rather than offset-zero magic.
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
- The `rust-profile-protocol` leaf owns pure M3/M4 semantics; `rust-durable`
  owns their single-CAS persistence. `rust-virtual` owns immutable archive
  provider bytes/proofs and public Durable scheduling/archive fault witnesses,
  while `rust-evolution` owns the process wire.
  Never keep retired scheduler/controller targets or relabel provider/DTO
  checks as durable fault-sweep evidence.
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
- Keep the profile-protocol M4 reducer in its own filtered, nonempty coverage
  artifact and floor. Only `src/evolution.rs` and `src/evolution/**` belong in
  that report; Evolution host-wire or another profile must not average over it.
- Keep core mutation and the bounded M4 evolution mutation as separate suites.
  The M4 filter targets the sole reducer authority in
  `cymule-profile-protocol::evolution`, owns compatibility, safe-point,
  relink/edge/evidence admission, migration target-binding/M1-sidecar closure,
  rollout-transition identity, and restart laws, and fails when its required
  mutant inventory is absent. Process-wire
  conformance remains in the independent `cymule-evolution` Rust leaf; expand
  the mutation filter deliberately when a new M4 admission law becomes
  normative.
- Day-one plugin mutation is a third independent bounded witness. Keep its
  filters on authority, acknowledgement, ambiguity, and protocol mapping;
  resource streaming remains covered by fault/soak until separately sharded.
- `cymule-clock-system` precedes every catalog package whose normalized Cargo
  manifest retains it, including directory, SQLite, HTTP, timer, and CLI
  consumers. Keep its focused restart/backward-time witness in plugin coverage,
  mutation, and soak lanes.
- `crates-release.toml` is the single public Rust package order. The release
  verifier must match Cargo's normalized publication graph exactly, including
  normal, build, target-specific, and versioned dev dependencies. Only a dev
  dependency declared with a path and no version is repository-test-only and
  stripped from that graph. Use a deterministic dependency-first sort, reject
  every cycle, run Cargo's whole-workspace publication dry-run, package the
  unpublished workspace as one set, reject dependency-path leakage, compare two
  archive hashes, and compile normalized package bytes through a local patch
  registry. The terminal controller statically revalidates the same source
  manifests and must never invoke Cargo in its OIDC job.
- `version_domains.py` validates the sole version-domain registry, generates
  the specification table and release BOM, and cross-checks public Rust
  constants, exact supporting-schema fragments, tracked plugin/root schema
  ownership, canonical schema digests, genesis-or-predecessor registry
  provenance, and the exact Cargo release catalog before BOM materialization.
  Its `verify-source-closure` command is the lightweight, ancestry-free Required
  CI gate: every plan runs it in its own deterministic lane so any public source
  byte or mode change must exact-match the registered candidate snapshot without
  selecting the full suite for unrelated narrow paths.
  Its production-literal inventory includes non-public Artifact-kind and
  content-ID strings across Rust, SDK, schema, and release sources. Release
  stages must call its canonical digest implementation and bind that digest to
  the exact tag.
- Version authority files use a duplicate-rejecting no-float I-JSON loader,
  safe integers, and RFC 8785 UTF-16 key ordering. Source anchors ignore Rust
  comments and bind one exact constant/literal token; Python anchors use the
  AST and require one exact string assignment, never a textual prefix or
  expression. Named production Rust constants passed directly to `content_id`
  are identity authorities and must use `content_id_domain` or
  `catalog_namespace` with the required `cymule.jcs/1` edge. JSON pointers
  resolve exact values or an explicitly requested bounded token. External schema refs
  must resolve the percent-decoded UTF-8 pointer or anchor before ownership is
  considered. A genesis registry has four null predecessor fields, explicit
  null provenance on every first-registered domain, and no ancestral registry
  from another release generation. A successor pairs the canonical predecessor
  registry digest with its mirror-stable public source-snapshot digest; partial
  or fabricated lineage fails closed.
- Every registry `conformance` value names a concrete leaf in
  `tests/harness/suites.toml`. Changes to the registry, its schema, or its
  validator must route every declared leaf or select `full`.
- Rust-only normalized receipt domains declare real typed containment even
  without a public transport schema. The source-mechanical field audit expands
  unversioned wrappers only to exact reviewed semantic/wire owners; it must not
  infer edges from type prefixes or count enum variant names as field types.
- The public source-snapshot preimage binds each ordered Git path, mode, object
  type, and exact blob bytes. Candidate, historical, and rewritten-public
  calculations use the same tuple and reject unsupported Git object types.
  Pure Rust text parsing may memoize a bounded set keyed by complete source
  strings, never path/mtime. Historical snapshot memoization pins the freshly
  resolved exact HEAD and returns an isolated value; neither file replacement
  nor ref movement may reuse stale authority.
- The production-literal inventory excludes only syntactically bounded Rust
  `#[cfg(test)]` module bodies. Keep that range parser fail-closed and covered
  by nested-brace, comment, raw-string, and unterminated-module tests; never add
  path exceptions or disguise a legacy test literal with string construction.
  Schema rejection probes load exact retired versions from the shared
  `tests/fixtures/legacy-protocol-versions.json` authority instead of generating
  hidden version strings inside a production-scanned script.
- Identity authorities use exact named constants with a declared source role;
  DTO/schema occurrences belong in `literal_locations`, not fake generator or
  type anchors. Every embedding edge must also be in the dependency closure.
  Cross-domain strongly connected components are valid for finite recursive
  values such as Resource Handles and manifest entries because every edge pins
  an exact generation; reject self-edges and unknown domains, while keeping the
  Cargo release catalog independently topologically ordered.
- Supporting schema ownership is keyed by the exact `(path, fragment)` pair.
  One domain may own several independently authenticated fragments in the same
  document; duplicate pairs, unordered pairs, incorrect owners, or fragment
  drift remain invalid. Provenance compares the same pair, while the BOM emits
  one exact path/ID/digest document and rejects conflicting digests for a path.
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
- Every crates.io PUT result, including transport loss, timeout, and non-429
  HTTP failure, enters one bounded exact-checksum readback. Exact bytes
  converge, different bytes fail, sustained reachable absence is a definite
  failed attempt, and an unavailable readback reports
  `crate_publish_outcome_ambiguous` without changing crate/version/archive
  identity. Retry publication automatically only for crates.io's exact
  new-crate 429 response after reachable absence and a parseable bounded server
  retry timestamp; the next PUT repeats the remote authority fence.
- npm and crates release jobs consume archives staged by predecessor jobs that
  have no OIDC identity. Manifests bind every archive digest to the exact
  verified release SHA. A separate no-OIDC Close job independently reproduces
  the complete bytes before terminal upload, and a later no-OIDC job reads
  registry bytes back. OIDC jobs do not invoke Cargo, package managers beyond
  the terminal upload itself, compilers, tests, or artifact-carried scripts.
  Stage manifests, independently reproduced close manifests, embedded package
  metadata, and registry JSON all use the shared duplicate-rejecting no-float
  I-JSON loader.
- Immediately before terminal npm, crates.io, or GitHub Release mutation, the
  privileged job reads the remote annotated tag's peeled commit and requires it
  to equal the frozen release SHA. A prior-job check or local checkout is not
  terminal tag authority. The workflow verify job separately linearizes
  controller admission once against current public main; every later job
  exact-matches the invoked controller and version-domain modules to that
  immutable admitted commit. A later mirror generation does not revoke the
  running non-cancelled controller.
- `finalize_release.py` additionally freezes the annotated tag object as the
  distinct 40-hex `release_tag_sha`. Its closed finalization manifest has exact
  `stage_version: cymule.release-finalization-stage/3` and additionally binds the private source SHA, raw
  immutable mirror-receipt tag, and shared source-snapshot digest. The freeze
  job, terminal job, and controller fence separately compare both tag
  authorities before each Release mutation. A replacement tag object targeting
  the same commit is not equivalent authority and must fail before any write.
- `version_domains.py` owns the complete BOM/3 semantic validator. It derives
  the exact public Cargo catalog from payload manifests, requires both npm
  publications plus explicit Go/Python records, and exact-matches registry
  generation/digest, schema, domain, compatibility, and migration projections.
  Finalization, staging, and tests reuse this validator; a shape-only BOM check
  is not release authority.
- Before any GitHub Release mutation, `finalize_release.py` verifies both the
  exact BOM's GitHub Artifact Attestation against the current-main finalizer
  workflow/controller SHA and the unexpired same-run
  `cymule.github-release-control-plane-receipt/2`. A separate protected
  Administration-read plus Actions-read App job creates that receipt from live immutable-Release,
  exact-tag-ruleset, environment, default-permissions, and default-branch
  settings; the contents writer receives no administration credential. Release
  REST readback binds the sole asset's database ID,
  name, byte size, SHA-256 digest, and uploaded state across publication, and a
  terminal Release must be immutable. GitHub Release remains a projection of
  the attested BOM, not a second mutable authority.
- Release BOM publication evidence binds the rewritten public SHA used by
  GitHub/npm, while BOM `source_sha` binds the private source SHA from the
  authenticated `cymule.public-mirror-receipt/2`. The two SHAs must differ and both
  sides must match the same registered public source-snapshot digest; never
  populate both fields from `RELEASE_SHA`.
- `release_contracts.py` is the sole public source for the finalization-stage,
  mirror-receipt, GitHub settings-snapshot, and GitHub control-plane-receipt
  selectors. Public controllers import those constants; the private mirror
  writer remains outside the public source inventory and is admitted only when
  the static release verifier exact-matches its wire literal to that source.
- Multi-write controllers repeat immutable release authority at each real
  external mutation: every missing-crate PUT and rate-limit retry re-read the
  peeled tag, and Release draft creation, BOM upload, and publication re-read
  the raw/peeled tag, mirror receipt, attestation, and control-plane receipt.
  Current-main admission occurs only once before the run. A failed npm tag push
  always proceeds to exact remote readback;
  both the raw annotated-tag object and peeled commit must equal the locally
  frozen values before response loss converges.
- `npm_release.py` requires npm tarball SHA-1/SHA-512 equality, a fully verified
  Sigstore bundle under exactly Node `v26.7.0` and npm `11.19.0`, with the
  current-main reusable controller as the Fulcio SAN, GitHub Actions as issuer,
  CT/Rekor evidence, and one closed SLSA v1 subject/dependency pair.
  The subject's purl and digest match together; the dependency names the exact
  `publish-npm-release.yml` caller Git dependency at either `refs/heads/main`
  for initial publication or that version's exact annotated tag for partial
  recovery. The resolved Git commit must equal the retained release SHA on both
  paths. npm trusted-publisher configuration itself names the inert caller,
  while the SLSA workflow/ref remains caller-owned; neither is the Fulcio
  controller SAN. The verifier also extracts Fulcio extensions
  `1.3.6.1.4.1.57264.1.9` and `.1.10` as the exact reusable-workflow
  `signer_ref` and `signer_sha`, requires the signer commit to be retained on
  current public main with that controller file, and carries both into release
  evidence. A historical signer SHA is publisher identity, not the later
  finalizer controller SHA. Its `publish` command alone owns the registry observation-to-
  mutation transition and repeats the remote immutable-tag fence after a missing
  result; workflow shell must never invoke `npm publish` directly.
- npm packages close `publishConfig` to the official registry, public access,
  provenance, and `latest`, with no additional key. The terminal repeats those
  flags explicitly and disables lifecycle scripts. All stable versions share
  one global non-cancelling controller concurrency group. It is an overlap
  lock rather than a durable queue; a superseded pending invocation must be
  dispatched again. A missing version rejects a higher published stable
  version; successful publication reads back exact
  bytes, provenance, and `dist-tags.latest`. Historical exact-version recovery
  never rolls `latest` back.
- `publication-admission` is the sole tag-preflight command. For both packages
  it applies the same exact-version, full-provenance, packument monotonicity, and
  historical/current `latest` rules as Publish before an annotated tag can be
  created. The resolved current-main script owns this check and the final
  registry verifier; the exact-tag workspace supplies only registry/version
  data and must never supply executable release code.
- `verify_github_release_settings.py` is the live operator check for repository
  rulesets, strict `Required CI`, exact mirror/tag Integration bypasses, a
  separate bypass-free tag update/deletion ruleset, default
  Actions permissions, owner-enforced immutable Releases, and protected release
  environments. Each environment admits exactly its configured reviewer Team,
  prevents self-review, and disables administrator bypass. The npm environment
  uses selected deployment refs with exactly branch `main` and tag `v*`; this
  is the only release environment that admits tags. Crates and Release
  finalization remain protected-branch-only. In workflow preflight it also
  closes the verified normalized snapshot into a self-digested 15-minute
  receipt bound to repository, run/attempt, controller, release commit, and raw
  annotated-tag object.
- The inert npm caller's reusable-workflow job grants the exact
  `contents: read` and `id-token: write` ceiling. The tag job obtains only a
  repository-scoped Contents-write GitHub App token from its Client ID after
  environment approval. Current-main `npm_release.py` exact-matches the minted
  App slug and configured App ID through GitHub's live API, then resolves the
  distinct bot user ID for the annotated tagger email;
  no built-in workflow token may create a tag. The verifier requires that App
  boundary and requires both tag mutation and registry publication to use the
  protected `npm` environment.
- `CYMULE_RELEASE_WORKSPACE` remains an absolute immutable-tag payload root only
  for a reviewed controller executing outside that checkout. It never changes
  which manifest, catalog, source, archive, or Git identity is release authority.
  The npm and crates controllers always import the current controller's
  `version_domains.py`; every payload read, including the canonical registry
  digest, is resolved against that absolute immutable-tag workspace.
- Release controllers accept one canonical ASCII stable version only. Validate
  `MAJOR.MINOR.PATCH` before the workflow constructs refs or writes outputs, and
  keep the Python entry points on the same closed grammar; do not normalize or
  recover malformed, prerelease, multiline, or leading-zero input.
- `finalize_release.py` runs its stage and publish commands only from the exact
  controller admitted against current main at workflow start. The read-only verify job emits authenticated package
  stages; a separate fresh read-only freeze job reruns npm and crates.io
  registry byte/provenance readback and creates `cymule.release-bom/3`. Every
  source package record has a required `publication`: closed registry evidence
  for Cargo/npm and explicit `null` for unpublished Python/Go. The BOM excludes
  mutable finalizer identity, while npm evidence preserves its immutable signed
  publisher `signer_ref`/`signer_sha`. Current-controller authority lives in the
  finalization stage, attestation, and same-run control-plane receipt, allowing
  exact draft recovery after a later controller advances. Freeze packages
  the exact-tag notes and BOM into a closed three-file bundle bound to repository,
  annotated-tag object, release, and controller SHAs, canonical names, sizes,
  and digests. A separate protected attestation job has `contents: read`, reruns
  the workspace-derived complete BOM/3 semantic validator, and attests the BOM
  bytes without Release mutation authority. A later protected contents-read job
  mints one repository-scoped Administration-read plus Actions-read App token, performs live
  settings readback, uploads only the same-run short-lived receipt, and then
  loses that token. The sole `contents: write` job has
  no third-party Action step; it fetches the exact controller and exact-tag
  workspace with Git, downloads the current run's immutable stage and
  attestation and control-plane receipt artifacts, authenticates the stage,
  verifies the exact attestation and unexpired receipt before every mutation,
  and only then validates the already-attested registry/package
  projection before converging the immutable GitHub Release. The full semantic
  validator already ran in freeze and the read-only attestor. The writer never
  executes tag or artifact-carried code, installs
  dependencies, or rebuilds release payload. The only admissible
  Release asset set is the one frozen
  versioned BOM; an extra or duplicate asset is not retained as unowned state.
  All stable finalizations use one literal non-cancelling concurrency group.
  The publish controller REST-paginates the complete Release set and exact-joins
  the explicit `isLatest` projection before and after every mutation. Only
  canonical published stable tags enter numeric ordering. The highest is
  explicitly Latest, historical recovery is explicitly non-Latest, and the
  final state must contain exactly one Latest equal to the highest stable
  version. Duplicate identities, multiple Latest flags, and any non-stable
  Latest owner are invalid authority, not states to normalize.
- Since the contents writer cannot read Administration settings, repository
  settings administrators must not change the receipt-bound control plane from
  finalization dispatch, including during sequential preflight reads, until the
  same non-cancelling finalization completes or is
  cancelled. Receipt expiry requires a new workflow run; never extend it or
  grant the writer an administration token.
- Protocol verification runs the CLI unit tests in addition to building its
  binary, so raw duplicate-key and shared-number ingress regressions cannot hide
  behind successful schema fixtures.
- The protocol leaf also runs the complete `cymule-test-adapter` Rust suite.
  Building the conformance executable without executing its process-wire and
  provider-ledger tests is not protocol evidence.
- Run that self-hosting adapter suite through `verify-rust.sh --conformance`.
  Cargo must bind `CARGO_BIN_EXE_cymule-test-adapter` to the same stripped,
  fixed-budget artifact under test; do not point tests at an artifact from an
  earlier command or enlarge the 64 MiB process-closure contract.
