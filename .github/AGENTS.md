# GitHub Public Repository Guidance

- GitHub is the public repository authority for clone URLs, package metadata,
  issues, pull requests, Actions, and security reporting.
- Public commits must have public-only ancestry. Never push or merge a private
  source commit directly into a GitHub branch.
- Keep Actions aligned with `scripts/verify.sh`; a green private pipeline does
  not replace public-repository verification.
- `publish-npm.yml` is the only npm publication authority. It uses OIDC trusted
  publishing, a GitHub-hosted runner, frozen dependencies, tests, package
  inspection, and provenance; never add `NPM_TOKEN`.
- Do not add private hosting URLs, internal project IDs, credentials, runner
  names, or private CI metadata under `.github/`.
