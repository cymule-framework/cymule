# Releasing Cymule

Status: implemented for npm and crates.io package publication. Live GitHub
control-plane gates must pass before a release workflow is authorized.

GitHub Actions is the only publication authority. Local commands build,
package, inspect, and test candidate bytes but never upload them. One version
is shared by the TypeScript packages, Python wheel, Go release notes, and every
public Rust crate.

## Public packages

The npm release publishes `cymule` and `@cymule/sdk` from the same source and
version. Python `cymule` has an installed-wheel witness but no PyPI trusted
publisher. The Go SDK is tested as an external module; public Go resolution
still requires a reviewed Actions-owned `sdk/go/v<version>` tag before it can be
claimed as published.

`scripts/crates-release.toml` is the executable ordered catalog for all public
Rust crates. It starts with the semantic and runtime foundations, orders every
durable, Resource, activation, executor, observability, and Agent adapter after
its dependencies, and ends with the `cymule` facade and `cymule-cli`. Examples
and the conformance adapter are never published.

## Public mirror boundary

The public repository contains no private-source URL, credential, reader, or
force-push workflow. After private verification, the source-side GitLab job
runs `.gitlab/scripts/publish-public-mirror.sh`. That controller:

1. rewrites a fresh complete source history;
2. removes `.gitlab-ci.yml`, `.gitlab/`, and the retired public
   `.github/workflows/mirror.yml` from every public commit;
3. preserves author, committer, dates, and messages;
4. rejects every remaining private host or project reference;
5. no-ops when the public tip already matches;
6. force-publishes with a protected private CI credential; and
7. reads the exact public tip back.

The source credential should be a narrowly installed GitHub App token. Token
creation, installation, ruleset bypass, and rotation are external control-plane
operations; they never enter public Git history or GitHub Actions secrets.

## Required GitHub settings

Before dispatching a release, run:

```sh
GITHUB_TOKEN=<administration-read-token> \
CYMULE_GITHUB_MIRROR_INTEGRATION_ID=<github-app-integration-id> \
  python3 scripts/verify_github_release_settings.py
```

The verifier requires:

- read-only default Actions permissions and no pull-request approval grant;
- an active exact-default-branch ruleset with deletion, non-fast-forward, and
  strict `Required CI` status protection;
- an active `v*` tag ruleset with deletion and non-fast-forward protections;
- the exact narrow mirror GitHub App Integration as the default branch's only
  bypass, and no release-tag bypass; and
- `npm`, `crates-io`, and `release-finalize` environments restricted to
  protected branches with a required reviewer who cannot self-approve.

The private mirror identity may receive the one explicit integration bypass
needed to update rewritten public history. Human administrator bypass is not a
substitute.

## Candidate verification

The release version commit updates Rust, TypeScript, and Python authorities
together and adds its changelog entry. Before it reaches public `main`, run:

```sh
./scripts/verify.sh
```

The Rust package witness runs Cargo's dependency-aware publication dry-run,
packages the complete catalog twice, rejects dependency-path leakage, compares
archive hashes, safely extracts normalized archives, and compiles every public
library, binary, and a fresh facade consumer.

Every release workflow is dispatched only from `refs/heads/main` and requires
the event SHA to equal freshly fetched public `origin/main`. The annotated
`v<version>` tag must select that same commit. Arbitrary-ref dispatch,
historical-tag payloads, lightweight tags, and moved tags fail before any
publication identity is granted.

Both npm and crates workflows rerun complete verification and the independent
`rust-soak` suite for that exact commit. A stale scheduled soak or a successful
run for another SHA is not release evidence.

## Least-privilege publication

Every external Action is pinned to a full reviewed commit SHA. Publication is
split into five authorities:

1. **Verify** has no OIDC permission. It authenticates public main and the tag,
   runs the repository and exact-SHA soak, and carries the verified commit as a
   job output.
2. **Stage** has no OIDC permission. It checks out that exact commit, builds and
   tests, and emits short-lived archives plus a manifest binding package name,
   version, digest, and release SHA.
3. **Close** has no OIDC permission. It independently rebuilds the exact tag,
   requires byte-for-byte equality with Stage, and emits the immutable closed
   artifact consumed by publication.
4. **Publish** runs in a protected environment with `id-token: write`. It
   checks out only the exact controller, downloads only the closed artifact,
   and uploads a missing immutable version. It does not build, test, package,
   or execute artifact-carried code.
5. **Verify published** has no OIDC permission. It reads registry bytes and
   provenance back and performs fresh-consumer compilation.

For npm, Close independently rebuilds both package names and compares SHA-1,
SHA-512, and archive bytes. The terminal runs `npm_release.py` only from the
exact checkout, never from the artifact. Verify published downloads the tarball
and reads the SLSA v1 attestation. One subject must carry both the expected purl
and digest; workflow path, `refs/heads/main`, repository, and resolved Git commit
must also match.

For crates.io, Stage contains every catalog archive and its Cargo Registry Web
API upload body. Close independently regenerates both and requires byte
equality. The terminal follows catalog dependency order and sends only those
closed bodies with the short-lived trusted-publisher token; it never invokes
Cargo. Verify published compares registry checksums, downloads exact bytes,
then compiles a fresh registry consumer and installs `cymule-cli`.

The only automatic crates.io retry is its exact new-crate-name 429 response with
a parseable, bounded server retry time. Authentication failures, malformed
limits, checksum mismatches, and other errors fail immediately. A new crate name
still requires a separate reviewed, temporary ownership bootstrap and trusted
publisher configuration; the normal workflow has no token fallback.

## Finalization

`publish-npm.yml` and `publish-crates.yml` never create the GitHub Release.
After both registries are complete, dispatch `finalize-release.yml` from current
public `main`. It reruns exact-SHA soak, rebuilds both npm tarballs and verifies
their registry bytes plus provenance, rebuilds every crate archive and compares
the complete catalog with crates.io, verifies fresh Rust consumers, and only
then creates the missing GitHub Release. An existing Release is accepted only
when its tag, title, notes, draft state, and prerelease state match exactly. The
workflow is idempotent and never publishes package bytes or moves a tag.
