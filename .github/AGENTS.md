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
- Do not add private hosting URLs, internal project IDs, credentials, runner
  names, or private CI metadata under `.github/`.
