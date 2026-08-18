# GitHub Public Repository Guidance

- GitHub is the public repository authority for clone URLs, package metadata,
  issues, pull requests, Actions, and security reporting.
- Public commits must have public-only ancestry. Never push or merge a private
  source commit directly into a GitHub branch.
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
- crates.io requires an owner token for the first version of a new crate name.
  The bootstrap branch is temporary first-release machinery: use only a
  short-expiry token stored as `CRATES_IO_BOOTSTRAP_TOKEN`, configure every
  crate for the pinned trusted-publishing workflow and `crates-io` Environment
  immediately afterward, then remove both the secret and bootstrap branch.
  Normal releases use OIDC only.
- Scheduled mirror runs must no-op when the rewritten source tip already equals
  public `main`. A changed PAT-backed push emits the standard CI event; do not
  dispatch a duplicate CI run manually.
- Manual npm release dispatch verifies the complete repository before creating
  a missing public tag. Package matrix jobs are independently retryable, skip
  immutable versions already present in npm, and create the GitHub Release only
  after both package names succeed.
- A retry for an existing version checks out and verifies that immutable public
  tag even when `main` has advanced. Never move a published tag merely to make a
  workflow rerun select the current branch.
- `finalize-release.yml` is an idempotent recovery path for release metadata
  after both immutable npm versions already exist. It verifies the exact tag,
  package manifest, and both registry names before creating a missing GitHub
  Release; it never publishes package bytes or moves a tag.
- Do not add private hosting URLs, internal project IDs, credentials, runner
  names, or private CI metadata under `.github/`.
