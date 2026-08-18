# Releasing Cymule

Status: implemented for npm and crates.io public packages.

GitHub Actions is the only publication authority. Local commands build,
package, inspect, and test candidate bytes but never upload them. A release
uses one version across the TypeScript packages and every public Rust crate.

## Public packages

The TypeScript workflow publishes:

- `cymule`
- `@cymule/sdk`

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
`publish --dry-run`, including Cargo's own package builds. It then packages the
complete workspace twice and requires the archive hashes to match, verifies the
catalog against Cargo metadata, rejects dependency paths in normalized
manifests, safely extracts the exact archives, and compiles every public library
and binary plus a fresh `cymule` consumer through a local `[patch.crates-io]`
registry simulation.

The release commit must update the workspace and TypeScript package to the same
version and add a dated changelog entry. Mirror the reviewed private-source
commit through `mirror.yml`; never push it directly to the public repository.
Require public CI and the applicable independent analysis/compatibility gates
before publication.

## Publication order

Dispatch `publish-npm.yml` with the version first. That workflow verifies the
public commit, creates the missing immutable `v<version>` tag, publishes both
npm names with provenance, and creates the GitHub Release.

Then dispatch `publish-crates.yml` against the same version using `trusted`
authentication. It checks out the exact annotated tag, repeats complete
verification, requests a short-lived crates.io token through GitHub OIDC, and
publishes in catalog dependency order. For each crate it:

1. runs Cargo's full package verification;
2. computes the candidate archive SHA-256;
3. retains an existing version only when crates.io reports the same checksum;
4. waits for a new version to enter the index;
5. downloads the registry archive and verifies its checksum.

After the ordered upload, the workflow builds a clean consumer of exact
registry versions and installs `cymule-cli` from crates.io. A partial failure is
safe to retry because every completed version must match the exact tag bytes.
If crates.io returns its explicit new-crate-name 429 with a server retry time,
the publisher waits only until that bounded timestamp and retries. It does not
retry authentication, checksum, malformed-limit, or other registry failures.

## First publication bootstrap

crates.io requires an owner token before the first version of a new crate name;
trusted publishers can be attached only after ownership exists. For that one
release:

1. create a short-expiry crates.io token with only the authority required to
   create the new crate names;
2. store it temporarily as the GitHub Actions secret
   `CRATES_IO_BOOTSTRAP_TOKEN` in the `crates-io` GitHub Environment;
3. dispatch `publish-crates.yml` with `bootstrap` authentication;
4. configure every published crate to trust repository
   `cymule-framework/cymule`, workflow `publish-crates.yml`, and environment
   `crates-io`;
5. rerun the workflow with `trusted` authentication to verify OIDC recovery;
6. delete the bootstrap secret and remove the bootstrap workflow branch in the
   next reviewed commit.

Never retain the owner token as a normal publication fallback. A trusted
publishing failure is a release failure, not permission to silently use the
bootstrap credential.
