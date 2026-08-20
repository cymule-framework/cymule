# Releasing Cymule

Status: implemented for npm and crates.io public packages.

GitHub Actions is the only publication authority. Local commands build,
package, inspect, and test candidate bytes but never upload them. A release
uses one version across the TypeScript packages, Python wheel, Go module
release notes, and every public Rust crate.

## Public packages

The TypeScript workflow publishes:

- `cymule`
- `@cymule/sdk`

Python `cymule` currently has an installed-wheel dry-run but no PyPI trusted
publisher workflow, so `0.2.0` must not be described as published on PyPI. The
Go SDK is verified as a fresh external module consumer; public Go resolution
requires the repository tag `sdk/go/v0.2.0` in addition to the root release
tag. Creating that tag remains a publication gate until a reviewed GitHub
Actions release step owns it.

The ordered Rust catalog in `scripts/crates-release.toml` publishes:

1. `cymule-core`
2. `cymule-runtime`
3. `cymule-durable`
4. `cymule-resource`
5. `cymule-evolution`
6. `cymule-virtual`
7. durable/resource/activation/executor/observability adapter crates
8. `cymule-agent`
9. `cymule-agent-mcp`
10. `cymule`
11. `cymule-cli`

The exact expanded list and dependency order is executable authority; read it
from `scripts/crates-release.toml` rather than duplicating every adapter here.

`cymule` is the Rust user facade. `cymule-cli` owns the installable `cymule`
binary. The conformance adapter and repository examples are never registry
packages.

## Candidate verification

Before committing a release version:

```sh
./scripts/verify.sh
```

The `package-rust` leaf first runs Cargo's dependency-aware workspace
`publish --dry-run`, including Cargo's own package builds. An ephemeral
`[patch.crates-io]` points dependency resolution at the exact candidate
workspace so one coordinated release can introduce inter-crate APIs before
those versions exist in the registry. It then packages the complete workspace
twice and requires the archive hashes to match, verifies the catalog against
Cargo metadata, rejects dependency paths in normalized manifests, safely
extracts the exact archives, and compiles every public library and binary plus
a fresh `cymule` consumer through a separate local patch-registry simulation.

The release commit must update the workspace and TypeScript package to the same
version, update the Python package to that version, and add a dated changelog
entry. Mirror the reviewed private-source
commit through `mirror.yml`; never push it directly to the public repository.
Require public CI and the applicable independent analysis/compatibility gates
before publication.

## Publication order

Dispatch `publish-npm.yml` with the version first. That workflow verifies the
public commit, creates the missing immutable `v<version>` tag, publishes both
npm names with provenance, and creates the GitHub Release.

Then dispatch `publish-crates.yml` against the same version. It checks out the
exact annotated tag, repeats complete verification, requests a short-lived
crates.io token through GitHub OIDC, and publishes in catalog dependency order.
For each crate it:

1. runs Cargo's full package verification;
2. computes the candidate archive SHA-256;
3. retains an existing version only when crates.io reports the same checksum;
4. waits for a new version to enter the index;
5. downloads the registry archive and verifies its checksum.

The workflow keeps its reviewed release controller in a current-public-main
checkout and all release payload in a separate exact-tag checkout. This lets a
historical immutable release use a corrected resumability controller without
moving its tag or changing any manifest, catalog, source, archive, or checksum.

After the ordered upload, the workflow builds a clean consumer of exact
registry versions and installs `cymule-cli` from crates.io. A partial failure is
safe to retry because every completed version must match the exact tag bytes.
If crates.io returns its explicit new-crate-name 429 with a server retry time,
the publisher waits only until that bounded timestamp and retries. It does not
retry authentication, checksum, malformed-limit, or other registry failures.

## New crate names

The normal workflow is OIDC-only and can publish only names that already trust
repository `cymule-framework/cymule`, workflow `publish-crates.yml`, and
environment `crates-io`. crates.io ownership for a new name must be established
through a separate reviewed, time-bounded GitHub Actions change. Configure and
verify its trusted publisher, revoke the temporary credential, and remove the
temporary path before completing that release. Never add a registry-token
fallback to `publish-crates.yml`.
