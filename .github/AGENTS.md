# GitHub Public Repository Guidance

- GitHub is the public repository authority for clone URLs, package metadata,
  issues, pull requests, Actions, and security reporting.
- Public commits must have public-only ancestry. Never push or merge a private
  source commit directly into a GitHub branch.
- The public workflow tree never reads private source or owns mirror
  credentials. History rewriting and force publication are private-source CI
  responsibilities under `.gitlab/`, which is absent from every public export.
- Keep Actions aligned with `scripts/verify.sh`; a green private pipeline does
  not replace public-repository verification.
- `analysis.yml`, `compatibility.yml`, and `soak.yml` are independent
  scheduled/manual witnesses. Do not add them to ordinary push CI or make a
  leaf SDK lane wait for coverage, mutation, portability, or repetition.
- `compatibility.yml` keeps the Linux/macOS harness matrix and one independent
  native `windows-2025` PowerShell job. That Windows job installs the exact
  `1.97.1-x86_64-pc-windows-msvc` toolchain, runs the exact DirectoryStore
  non-Unix unit, and fails unless the Rust harness reports exactly one passing
  test. It remains scheduled/manual evidence and never becomes Required CI.
- Ordinary `Required CI` may expand only the harness `full` suite. The broader
  `catalog` aggregate is explicit operator evidence and must never be selected
  by a changed path, including changes to the harness or scheduled runners.
- The meta and protocol lanes check out complete history because their
  release/schema/BOM witnesses authenticate the registered baseline source
  snapshot in ancestry. Never weaken that validator or substitute a shallow
  clone success.
- Partition core and bounded M4 evolution mutation independently. Every matrix
  entry must select one named harness suite and upload only that suite's report
  and mutation output; do not merge their evidence directories.
- Day-one plugin mutation uses its own four shards and output tree. It must not
  share a cargo-mutants output directory with core or M4.
- Semantic and plugin coverage are separate Analysis jobs and artifacts. The
  semantic job also emits an independent profile-protocol M4 reducer artifact;
  do not average that floor with the Evolution host or another profile, and do
  not make ordinary push CI wait for it.
- GitHub Actions is the only publication authority for all public artifacts.
  Release workflows must use GitHub-hosted runners, frozen dependencies,
  repository verification, staged-byte inspection, and short-lived OIDC or the
  registry's equivalent trusted identity. Never document or add a local publish
  path or a long-lived registry token.
- Pin every third-party Action to a reviewed full commit SHA. The one same-repo
  npm reusable-workflow call names
  `cymule-framework/cymule/.github/workflows/publish-npm-controller.yml@main`;
  its jobs require `job.workflow_sha` to equal freshly fetched public `main`
  before either write. A release grants `id-token: write` only through the inert
  npm parent envelope and the terminal registry job; repository verification,
  soak, compilation, tests, and archive staging run in predecessor jobs without
  an OIDC publication identity.
- Every `setup-uv` step used by CI meta or release control pins uv `0.7.2`
  explicitly; the immutable Action revision does not pin the installed uv
  toolchain by itself.
- Stage and Close are independent no-OIDC builds. Publish consumes only the
  byte-identical closed artifact and an exact-SHA controller; it never compiles,
  packages, tests, or executes code delivered inside an artifact. Registry
  readback and fresh consumers run in a later no-OIDC job.
- Every terminal package mutation re-reads the remote annotated tag's peeled
  commit and exact-matches it to the frozen release SHA. GitHub Release
  finalization additionally freezes the raw annotated tag object as
  `release_tag_sha`; every freeze, publish, and Release mutation separately
  exact-matches the raw ref and peeled commit. Earlier job admission and a local
  tag checkout do not replace that terminal readback.
- `publish-npm-release.yml` is the only npm trusted-publisher caller for both
  `cymule` and `@cymule/sdk`. It contains no runner or step and passes only the
  version plus `cymule.npm-release-caller/1` to
  `publish-npm-controller.yml@main`. The called workflow is not directly
  dispatchable. The former `publish-npm.yml` filename is retired and must not
  remain trusted. A local npm command may run `pack --dry-run`, but never
  `publish`.
- The inert npm caller's sole reusable-workflow job grants `contents: read`
  and `id-token: write`. The protected tag job uses a repository-scoped GitHub
  App installation token with only Contents write; its built-in token stays
  read-only. Token minting uses the App Client ID. The current-main controller
  then exact-matches the minted App slug and configured App ID through GitHub's
  live App API, resolves the distinct bot user ID from that exact slug, and uses
  only that bot user ID in the annotated tagger email. The exact `v*` creation
  ruleset admits only that Integration, and
  tag lost-ack recovery requires the remote raw annotated-tag object to equal
  the locally constructed raw object as well as the peeled release commit.
- GitHub Release mutation has one workflow writer:
  `finalize-release.yml`. It must attest the completely revalidated BOM/3
  before any mutation. A separate protected contents-read job uses a
  repository-scoped Administration-read plus Actions-read App token to close a 15-minute live
  control-plane receipt bound to the same run/attempt, controller, release, raw
  tag object, and exact settings snapshot. The contents writer never receives
  that credential and revalidates the receipt before every mutation, including
  before draft publication. Preserve the exact REST asset
  ID/name/size/digest/state, and accept a published Release only when
  owner-enforced immutability reports `isImmutable=true`. The attested BOM is
  authority; Release is its immutable projection.
- `publish-crates.yml` is the only crates.io publication authority. It consumes
  the exact public release tag, follows `scripts/crates-release.toml` in
  dependency order, compares immutable checksums on retries, and verifies a
  fresh facade consumer plus `cargo install cymule-cli` from registry bytes.
  Every PUT outcome receives bounded exact-checksum readback; unavailable
  readback is an ambiguous outcome under the unchanged crate/version/archive
  identity, while only an exact 429 plus confirmed absence may retry.
  Before that job receives OIDC, a no-credential `macos-15` job must run the
  exact release SHA's executor suite and export that same SHA to the publisher.
- Crate release recovery uses separate checkouts: current public `main` owns the
  reviewed controller script while the exact immutable tag owns every manifest,
  catalog, source file, package archive, and checksum. Never move a tag or let
  controller files replace release payload files.
- `publish-crates.yml` is OIDC-only and may never accept a registry-token
  fallback. A future new crate name requires a separate reviewed, temporary
  Actions change to establish ownership; configure its trusted publisher and
  remove that path before the normal release workflow can publish it.
- A new manual release caller runs only when its ref and event SHA equal current
  public `main`. npm partial-publication recovery dispatches the inert
  `publish-npm-release.yml` caller from the exact annotated
  `refs/tags/v<version>` whose commit is the retained release SHA. Standard OIDC
  provenance therefore remains tag-owned, while `job.workflow_ref` and
  `job.workflow_sha` identify the reusable current-main controller. The
  verify job admits that controller once against current main. The tag-App and
  OIDC jobs check it out separately, exact-match the invoked controller files
  to the admitted commit, and treat the tag checkout as data only. Admission
  requires the
  tag's caller file to remain byte-identical to the resolved current-main `/1`
  caller. A tag without that caller generation is unsupported; the retired
  filename and direct controller never receive authority.
- Each stable workflow admits only canonical ASCII `MAJOR.MINOR.PATCH` input,
  before any tag/ref construction or `GITHUB_OUTPUT` write. Never loosen this to
  generic SemVer, substring matching, multiline values, or shell sanitization.
- Manual npm release dispatch verifies the complete repository before creating
  a missing public tag. For a missing tag, both packages must pass the stricter
  tag-creation admission that requires the target to remain at the complete
  packument's stable frontier; an existing exact tag uses historical recovery
  admission and never moves `latest` backward. The resolved current-main
  controller runs the applicable admission against the exact-tag payload and
  closed archives, then re-observes the remote tag and repeats both applicable
  closed-archive admissions inside the protected tag-App job after its
  environment wait and before any tag mutation.
  A historical payload therefore cannot authorize a missing lower version or
  an irreversible tag from stale registry evidence. Package matrix jobs are
  independently retryable, skip immutable versions already present in npm, and
  publish both package names before the separately authorized finalization
  workflow may create the GitHub Release. A missing-tag push keeps the
  once-admitted controller immutable, re-observes the remote tag immediately
  before mutation, and resolves push failure or lost response only when the
  remote raw annotated-tag object equals the locally constructed object and
  its peeled commit equals the release SHA; the push exit status alone is not
  authority.
- A registry retry retains a version only after rebuilding or downloading the
  exact staged bytes and comparing registry digests. npm additionally reads the
  fully verified Sigstore bundle and closed singleton SLSA statement back and
  requires its subject plus exact caller dependency to match the verified
  release. npm trusted-publisher configuration names
  `publish-npm-release.yml`; SLSA workflow/ref provenance remains caller-owned;
  the Fulcio SAN separately names the resolved
  `publish-npm-controller.yml@refs/heads/main` job. Every verifier path pins and
  reads back Node `v26.7.0` plus npm `11.19.0`. The stable npm controller
  serializes every running version in one global non-cancelling group and
  rejects a missing version below an already published stable version. GitHub
  may supersede a pending invocation before it starts; that invocation has no
  mutation and must be dispatched again. The controller owns exact
  `dist-tags.latest` readback without historical rollback.
- `finalize-release.yml` is an idempotent metadata recovery path only after both
  npm packages and every crate in `scripts/crates-release.toml` match the exact
  current tag. It reruns the exact-SHA soak, verifies npm provenance and the
  complete crates.io catalog, and never publishes package bytes or moves a tag.
  An existing GitHub Release must exactly match tag, title, changelog notes,
  draft state, and prerelease state; existence alone is not completion.
- Every stable GitHub Release finalization shares the literal non-cancelling
  `finalize-release-stable` concurrency group. The current controller reads all
  REST pages and exact-joins the CLI `isLatest` projection before and after each
  mutation. It orders only canonical public stable tags numerically, publishes
  the highest with explicit `--latest=true`, publishes historical recovery with
  explicit `--latest=false`, and finishes only when exactly one Latest exists
  and it is the highest published stable version. Duplicate identities,
  multiple Latest flags, or a non-stable Latest owner fail closed.
- Finalization verification and freeze have `contents: read` only. Verification
  emits only authenticated npm and Cargo stages. A fresh freeze runner executes
  the exact current-main registry verifiers against those stages, reads the
  registries back again, and materializes notes plus `cymule.release-bom/3` as a
  data-only bundle bound to the release SHA, exact current-main finalizer SHA,
  raw annotated-tag object SHA, canonical names, sizes, and digests. The closed
  finalization manifest has exact
  `stage_version: cymule.release-finalization-stage/3`; older bundles have no
  reader. It binds the private source SHA, distinct public release SHA, raw
  immutable mirror-receipt tag, and shared source-snapshot digest. A protected
  `contents: read` attestation job uses complete release ancestry and reruns the complete BOM/3
  workspace-derived validator and attests the exact BOM without Release write
  authority. The sole protected `contents: write` job runs no third-party
  Action: it fetches the exact controller and complete data-only release
  workspace with Git, downloads only this run's immutable stage and attestation
  artifacts, authenticates the stage bytes, verifies the exact attestation,
  and then validates only the attested registry/package projection before it
  re-reads the frozen raw/peeled tag identities and invokes the immutable
  controller admitted from current main at workflow start. The complete workspace-derived semantic validator
  runs in freeze and the read-only attestor. The writer never installs
  dependencies, builds, tests, or executes tag/artifact-carried code.
- Finalization is a recoverable draft transaction: read or create the exact
  draft, create a missing BOM asset without replacement, byte-compare it by
  download, re-read metadata, publish, and repeat the terminal metadata/asset
  readback. Current main is admitted once for the non-cancelling run; the raw
  tag object and peeled commit are re-read immediately before each create,
  upload, publish, or Latest mutation and once more after terminal readback. A same-commit retag with
  different annotation or signature is rejected even when the Release was
  already converged and no mutation was needed. The complete Release asset set is
  exactly the frozen BOM; its REST database ID, name, size, SHA-256, and
  `uploaded` state remain identical across publication, and terminal readback
  requires `isImmutable=true`. Any additional or replaced asset fails closed.
  The Artifact Attestation is the terminal content authority and Release is its
  immutable projection. Lost responses at
  create, upload, publish, or Latest correction are resolved only by the full
  exact inventory and terminal Latest readback. A package version needs one
  non-empty changelog section, and a non-empty `Unreleased` section blocks every
  package publisher and Release.
- GitHub does not expose repository Administration settings to the contents-only
  Release writer. From dispatch, including every sequential live-preflight read,
  until that non-cancelling `finalize-release-stable` invocation completes or is
  cancelled, settings
  administrators must not change immutable-Release enforcement, release tag
  rulesets, protected release environments, default Actions permissions, or
  default-branch authority. Expiry requires a new workflow preflight; never
  extend the receipt or give the writer an administration token.
- Finalization generates `cymule.release-bom/3` from the exact tag and fresh
  registry readback. Every source package record has a required `publication`
  member: Cargo and npm carry closed registry identity, version, content digest,
  and provenance evidence; the unpublished Python and Go entries carry explicit
  `null`. The current finalizer SHA belongs only to the run's authenticated
  stage, attestation and control-plane receipt, not the BOM. This keeps draft
  recovery byte-identical after main advances while still requiring a fresh
  current-controller attestation and settings preflight. npm
  evidence separately records the Fulcio-signed `signer_ref` and
  `signer_sha` from certificate extensions 1.9 and 1.10; a retained historical
  publisher commit is valid and must never be replaced with the finalizer SHA.
  An existing release is accepted only when the deterministic versioned asset is
  byte-identical; a missing or different BOM is not an update-in-place path.
- The `npm` environment admits exactly branch `main` and tag `v*` through typed
  selected-ref policies so exact-tag recovery can reach its protected OIDC job.
  `crates-io` and `release-finalize` remain protected-branch-only. All three
  require exactly their configured Team, non-self approval, and administrator
  bypass disabled. The active main ruleset prohibits deletion and
  non-fast-forward mutation and requires main status checks. One exact tag
  ruleset restricts creation to the release App Integration; an independent
  exact tag ruleset prohibits every update/deletion with no bypass. Repository
  mirror receipts use the same split protection at exact
  `refs/tags/cymule-mirror/*`: only the narrow mirror Integration may create,
  and nobody may update or delete. The tag carries
  `cymule.public-mirror-receipt/2`, targets the rewritten public
  commit and carries the distinct private/public SHA mapping plus shared source
  snapshot; public Actions never owns its credential.
  immutable Releases must be owner-enforced. Audit the live settings
  with `scripts/verify_github_release_settings.py`.
- `Required CI` is the sole stable required-status context. It closes the
  planner and every selected static lane. Every plan also carries the dedicated
  deterministic version-domain source-closure leaf; it authenticates the exact
  public candidate snapshot and registry without widening an otherwise narrow
  path route to `full`. The default-branch ruleset is strict
  and the live repository default branch must be exactly `main`; the normalized
  control-plane receipt binds and verifies that name before interpreting
  `~DEFAULT_BRANCH`. The ruleset grants exactly one always-on bypass to the narrow mirror GitHub App
  Integration; release tags grant no bypass.
  When the selected plan contains `rust-executor-plugin`, Required CI also
  closes one exact-`github.sha` `macos-15` executor witness; a skipped or
  different-SHA result fails the aggregate.
- Do not add private hosting URLs, internal project IDs, credentials, runner
  names, or private CI metadata under `.github/`.
