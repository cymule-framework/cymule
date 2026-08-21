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
- Partition core and bounded M4 evolution mutation independently. Every matrix
  entry must select one named harness suite and upload only that suite's report
  and mutation output; do not merge their evidence directories.
- Day-one plugin mutation uses its own four shards and output tree. It must not
  share a cargo-mutants output directory with core or M4.
- Semantic and plugin coverage are separate Analysis jobs and artifacts. Do not
  average them into one percentage or make ordinary push CI wait for them.
- GitHub Actions is the only publication authority for all public artifacts.
  Release workflows must use GitHub-hosted runners, frozen dependencies,
  repository verification, staged-byte inspection, and short-lived OIDC or the
  registry's equivalent trusted identity. Never document or add a local publish
  path or a long-lived registry token.
- Pin every third-party Action to a reviewed full commit SHA. A release grants
  `id-token: write` only to the terminal registry job; repository verification,
  soak, compilation, tests, and archive staging run in predecessor jobs without
  an OIDC publication identity.
- Stage and Close are independent no-OIDC builds. Publish consumes only the
  byte-identical closed artifact and an exact-SHA controller; it never compiles,
  packages, tests, or executes code delivered inside an artifact. Registry
  readback and fresh consumers run in a later no-OIDC job.
- `publish-npm.yml` is the only npm publication authority for both `cymule` and
  `@cymule/sdk`. A local npm command may run `pack --dry-run`, but never
  `publish`.
- `publish-crates.yml` is the only crates.io publication authority. It consumes
  the exact public release tag, follows `scripts/crates-release.toml` in
  dependency order, compares immutable checksums on retries, and verifies a
  fresh facade consumer plus `cargo install cymule-cli` from registry bytes.
- Crate release recovery uses separate checkouts: current public `main` owns the
  reviewed controller script while the exact immutable tag owns every manifest,
  catalog, source file, package archive, and checksum. Never move a tag or let
  controller files replace release payload files.
- `publish-crates.yml` is OIDC-only and may never accept a registry-token
  fallback. A future new crate name requires a separate reviewed, temporary
  Actions change to establish ownership; configure its trusted publisher and
  remove that path before the normal release workflow can publish it.
- Manual release controllers run only when the workflow ref and event SHA equal
  current public `main`. A release tag is annotated, immutable, and must select
  that same commit; arbitrary-ref dispatch and historical-tag publication are
  rejected.
- Manual npm release dispatch verifies the complete repository before creating
  a missing public tag. Package matrix jobs are independently retryable, skip
  immutable versions already present in npm, and create the GitHub Release only
  after both package names succeed.
- A registry retry retains a version only after rebuilding or downloading the
  exact staged bytes and comparing registry digests. npm additionally reads the
  SLSA statement back and requires its subject digest and resolved Git commit to
  match the verified release.
- `finalize-release.yml` is an idempotent metadata recovery path only after both
  npm packages and every crate in `scripts/crates-release.toml` match the exact
  current tag. It reruns the exact-SHA soak, verifies npm provenance and the
  complete crates.io catalog, and never publishes package bytes or moves a tag.
  An existing GitHub Release must exactly match tag, title, changelog notes,
  draft state, and prerelease state; existence alone is not completion.
- `npm`, `crates-io`, and `release-finalize` environments admit protected
  branches only and require non-self approval. Active main and release-tag
  rulesets prohibit deletion and non-fast-forward mutation, require main status
  checks, and grant no broad administrator or repository-role bypass. Audit the
  live settings with `scripts/verify_github_release_settings.py`.
- `Required CI` is the sole stable required-status context. It closes the
  planner and every selected static lane. The default-branch ruleset is strict
  and grants exactly one always-on bypass to the narrow mirror GitHub App
  Integration; release tags grant no bypass.
- Do not add private hosting URLs, internal project IDs, credentials, runner
  names, or private CI metadata under `.github/`.
