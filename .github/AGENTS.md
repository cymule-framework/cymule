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
- GitHub Actions is the only publication authority for all public artifacts.
  Release workflows must use GitHub-hosted runners, frozen dependencies,
  repository verification, staged-byte inspection, and short-lived OIDC or the
  registry's equivalent trusted identity. Never document or add a local publish
  path or a long-lived registry token.
- `publish-npm.yml` is the only npm publication authority for both `cymule` and
  `@cymule/sdk`. A local npm command may run `pack --dry-run`, but never
  `publish`.
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
