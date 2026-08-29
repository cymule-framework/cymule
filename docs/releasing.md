# Releasing Cymule

Status: implemented in source for npm and crates.io package publication. npm
publication remains unauthorized until the two live trusted-publisher records
have completed
[`npm-trusted-publisher-caller-v1.md`](migrations/npm-trusted-publisher-caller-v1.md).
Git tag and GitHub Release mutation remain unauthorized until
[`github-release-authority-v1.md`](migrations/github-release-authority-v1.md)
has been executed and read back.
All live GitHub control-plane gates must also pass before a release workflow is
authorized.

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
Rust crates. It starts with `cymule-authenticated-collections`, `cymule-core`,
and `cymule-runtime`, then publishes the lower `cymule-durable-protocol`
authority before `cymule-profile-protocol` and `cymule-durable`. The catalog
contains every workspace-local normal, build, target-specific, and versioned
dev edge that Cargo retains in a normalized package. A path-only dev dependency
with no version is repository-test-only and Cargo removes it from the published
manifest. One deterministic dependency-first sort rejects cycles; in particular
`cymule-clock-system` precedes the directory, SQLite, HTTP, timer, and CLI
packages whose normalized manifests retain it. The catalog ends with the
`cymule` facade and `cymule-cli`. Examples and the conformance adapter are never
published.

## Public mirror boundary

The public repository contains no private-source URL, credential, reader, or
force-push workflow. After private verification, the source-side GitLab
pipeline constructs and publishes the mirror only for the protected private
default branch:

1. a credential-free job, running the digest-pinned Python `3.13.15-bookworm`
   Official Image, proposes a fresh complete source-history rewrite using only
   Git and repository-owned code; this artifact is not terminal authority;
2. removes every root first-component beginning with `.gitlab` and the retired
   public `.github/workflows/mirror.yml` from every public commit;
3. preserves author, committer, dates, and messages;
4. proves the ordered public source-snapshot digest and emits one digest-bound
   Git bundle;
5. a separate credential-free job in the digest-pinned Gitleaks `8.24.3` image
   binds and scans the actual candidate bundle, every unique reachable blob,
   every raw historical pathname and every commit metadata record, and runs the
   five live canaries;
6. after the complete verify-stage barrier, the credentialed publisher runs in
   the same digest-pinned Python/Git image as candidate construction, reruns the
   sole canonical rewriter from the frozen private tip, and requires the
   candidate tip to equal that complete expected commit graph before any remote
   read;
7. no mirror stage installs or downloads tools or dependencies at runtime;
8. re-reads the private default branch and fails if the pipeline is stale;
9. no-ops when the public tip already matches;
10. publishes with a protected private CI credential and an exact
   force-with-lease predecessor, so concurrent public movement is never
   replaced; and
11. always performs one bounded exact public-tip readback, even when the push
   reports failure. The exact source tip converges successfully, a different
   reachable tip fails, and an unavailable readback reports an ambiguous
   outcome without changing the retry identity.

Every Gitleaks invocation clears config environment variables, uses one empty
controller-created ignore file, and passes `--ignore-gitleaks-allow`. The
black-box suite proves that repository ignore data, inline allow annotations,
and secrets at lines 62189, 62190, and the final line of a one-million-line file
cannot suppress detection.
The credentialed controller uses an empty Git HOME, disables system/global
configuration, rejects dangerous local configuration and inherited Git/network
controls, scopes its authorization header to the exact GitHub URL, and disables
redirects. Even an apparent no-op re-reads the exact public tip after its
private-tip fence and before writing a receipt. `mirror-public` has no `needs`;
the mirror stage waits for every verification job, while
`dependencies: [mirror-candidate]` selects only the same immutable candidate
producer artifact already scanned by the no-credential gate.

The source credential should be a narrowly installed GitHub App token. Token
creation, installation, ruleset bypass, and rotation are external control-plane
operations; they never enter public Git history or GitHub Actions secrets.
Before enabling the new pipeline, delete the wildcard-scoped legacy mirror push
token variable and recreate it as protected, masked, and scoped only to the
GitLab `public-mirror` environment. Candidate, ShellCheck, scanner, and
controller-test jobs intentionally fail if that variable is visible; mirror
publication is not an applied authority until this environment-scope readback
passes.

## Required GitHub settings

The tag writer uses repository variable `CYMULE_RELEASE_TAG_APP_CLIENT_ID` to
mint its installation token and the separate numeric
`CYMULE_RELEASE_TAG_APP_ID` to bind that minted slug to the exact GitHub App.
The current-main controller then resolves the distinct bot user ID from
GitHub's live App and user APIs and uses that user ID, never the App ID, in the
annotated tagger email. Its private key exists only as
`CYMULE_RELEASE_TAG_APP_PRIVATE_KEY` in the protected `npm` environment.

Release finalization uses a different repository-installed App. Repository
variable `CYMULE_RELEASE_CONTROL_APP_ID` identifies it, and
`CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY` exists only in the protected
`release-finalize` environment. That App installation has repository
Administration read plus Actions read, and no write permission. Actions read is
required to inspect the protected environments; Administration read owns the
immutable-Release and repository settings endpoints. Its short-lived token
exists only in the read-only live-preflight job; the `contents: write`
projection job never receives the App token or private key.

The live workflow preflight reads the non-secret repository variables
`CYMULE_GITHUB_MIRROR_INTEGRATION_ID`,
`CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID`,
`CYMULE_GITHUB_NPM_REVIEWER_TEAM_ID`,
`CYMULE_GITHUB_CRATES_REVIEWER_TEAM_ID`, and
`CYMULE_GITHUB_RELEASE_REVIEWER_TEAM_ID`. The operator-side command below uses
the same exact values as environment variables.

Before dispatching a release, run:

```sh
GITHUB_TOKEN=<administration-read-token> \
CYMULE_GITHUB_MIRROR_INTEGRATION_ID=<github-app-integration-id> \
CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID=<release-tag-app-integration-id> \
CYMULE_GITHUB_NPM_REVIEWER_TEAM_ID=<team-id> \
CYMULE_GITHUB_CRATES_REVIEWER_TEAM_ID=<team-id> \
CYMULE_GITHUB_RELEASE_REVIEWER_TEAM_ID=<team-id> \
  python3 scripts/verify_github_release_settings.py
```

The verifier requires:

- read-only default Actions permissions and no pull-request approval grant;
- an active exact-default-branch ruleset with deletion, non-fast-forward, and
  strict `Required CI` status protection;
- one active exact `v*` creation-only ruleset whose sole bypass is the release
  App Integration, plus a separate active exact `v*` update/deletion ruleset
  with no bypass actor;
- the exact narrow mirror GitHub App Integration as the default branch's only
  bypass, and the exact release-tag GitHub App Integration as the creation
  ruleset's only bypass;
- an `npm` environment with typed selected-ref policies for exactly branch
  `main` and tag `v*`, exactly the configured reviewer Team, non-self approval,
  and administrator bypass disabled;
- `crates-io` and `release-finalize` environments restricted to protected
  branches with exactly their configured reviewer Teams, non-self approval,
  and administrator bypass disabled; and
- repository immutable Releases enabled and enforced by the repository owner.

The npm trusted publisher for both `cymule` and `@cymule/sdk` must name
`publish-npm-release.yml`, never the retired `publish-npm.yml` or the directly
non-dispatchable controller. Complete the linked migration runbook and its live
readback before treating this source boundary as enabled.

The private mirror identity may receive the one explicit integration bypass
needed to update rewritten public history. Human administrator bypass is not a
substitute.

## Candidate verification

The release version commit updates Rust, TypeScript, and Python authorities
together and adds its changelog entry. Stable release workflows accept only one
canonical ASCII `MAJOR.MINOR.PATCH`; they reject prereleases, leading zeroes,
whitespace, newlines, Unicode digits, and appended output records before using
the value in a Git ref or `GITHUB_OUTPUT`. Before the commit reaches public
`main`, run:

```sh
./scripts/verify.sh
```

The Rust package witness runs Cargo's dependency-aware publication dry-run,
packages the complete catalog twice, rejects dependency-path leakage, compares
archive hashes, safely extracts normalized archives, and compiles every public
library, binary, and a fresh facade consumer.

Every new release workflow is dispatched from `refs/heads/main` and requires
the event SHA to equal freshly fetched public `origin/main`. A new version
freezes that commit as its annotated `v<version>` tag. npm has a deliberately
inert trusted caller, `publish-npm-release.yml`; it has no runner or executable
step and calls only
`cymule-framework/cymule/.github/workflows/publish-npm-controller.yml@main`
with the version and `cymule.npm-release-caller/1`.

The reusable controller checks GitHub's resolved `job.workflow_ref`,
`job.workflow_sha`, repository, and file path, then exact-matches that SHA to a
fresh read of public main. Its write jobs check out only that controller at the
workspace root and keep the exact tag in an isolated payload checkout. If npm
publication is only partially complete after main advances, rerun the new
caller from `v<version>`: standard OIDC and npm provenance remain bound to the
calling tag while the called job identity and executed controller remain bound
to current main. A tag predating `cymule.npm-release-caller/1` has no such
caller and is unsupported; `/1` admission also requires its caller file to be
byte-identical to the resolved current-main `/1` caller. The retired
`publish-npm.yml` and a direct
`publish-npm-controller.yml` dispatch have no publication authority.

For a new version, run `publish-npm-release.yml` first: its reusable
controller's closed two-package registry preflight is the sole authority that
requests the annotated tag. The protected tag job downloads both
closed archives, re-observes the remote annotated tag, and repeats the strict
stable-frontier admission for both packages after the environment wait when
that tag is absent. An existing exact tag instead uses historical recovery
admission, which accepts exact retained bytes without moving `latest` backward.
Only after that branch is fixed does the writer recheck current main, mint a
repository-scoped GitHub App installation token with only `contents: write`,
construct the annotated tag locally, freeze its raw object SHA, and push that
exact object. A concurrent tag is accepted only when both its raw tag-object
SHA and peeled commit equal the local values. A lost push response converges
only after the same two exact identities are read back. The workflow's built-in
`GITHUB_TOKEN` remains `contents: read`; the
earlier credential-free preflight is not treated as a terminal mutation fence. Run
`publish-crates.yml` only after that tag exists. Crates recovery continues to
use tag-owned package payload, but its OIDC job executes the exact current-main
controller from a separate checkout.

For an npm-only partial recovery after main has advanced, dispatch the immutable
tag explicitly:

```sh
version=0.2.0
gh workflow run publish-npm-release.yml --ref "v$version" --field "version=$version"
```

Do not rerun that historical version from main; the controller rejects it
before staging or publication authority is reached. Do not attempt recovery for
a tag that lacks `publish-npm-release.yml` with caller generation `/1`.

`versioning/version-domains.json` is the only semantic and protocol version
inventory. Stage manifests bind its canonical digest to the package bytes and
exact release SHA. Finalization generates `cymule.release-bom/2` with that
registry digest, every registered schema digest, package-manifest digest, exact
public source SHA, and immutable publication evidence. The exact current-main
finalizer `controller_sha` belongs to the run's stage, signed attestation and
control-plane receipt, not the BOM. Every
source package has a required `publication` member: Cargo and npm records carry
closed registry identity, version, content digest, and provenance evidence;
Python and Go carry explicit `null` because this release flow has no publication
authority for them. Ecosystem-qualified canonical `package_id` values keep the
Cargo and npm packages both named `cymule` distinct and impose one unique sorted
inventory. A protocol string or shared SemVer never authorizes shape
probing or a second set of bytes.

Both npm and crates workflows rerun complete verification and the independent
`rust-soak` suite for that exact commit. A stale scheduled soak or a successful
run for another SHA is not release evidence.

## Least-privilege publication

Every external Action is pinned to a full reviewed commit SHA. Publication is
split into terminal authorities:

1. **Trusted npm caller and Verify.** The inert caller grants its sole reusable-
   workflow call `contents: read` and `id-token: write`: GitHub does not let a
   called workflow elevate beyond that caller-job ceiling. The caller itself
   owns no runner, step, environment, or mutation, and every called Verify job
   explicitly downgrades to `contents: read`. It authenticates either a new release at exact
   current public main or npm recovery at the exact caller tag, freezes both the
   release SHA and resolved current-main controller SHA, runs the repository and
   exact-SHA soak, and carries both identities as outputs. Crates publication
   and GitHub Release finalization use the same controller/payload separation.
2. **Stage** has no OIDC permission. It checks out that exact commit, builds and
   tests, and emits short-lived archives plus a manifest binding package name,
   version, digest, and release SHA.
3. **Close** has no OIDC permission. It independently rebuilds the exact tag,
   requires byte-for-byte equality with Stage, and emits the immutable closed
   artifact consumed by publication.
4. **npm registry preflight and tag** read both closed npm candidates before any
   tag mutation. The resolved current-main controller uses a separate exact-tag
   payload workspace. A missing tag selects strict tag-creation admission and
   is rejected when either package already has a higher stable version; an
   existing tag selects historical recovery admission. Existing versions must
   match exact bytes, provenance, and the applicable historical-or-current
   `latest` rule. The tag-App job, gated by the same protected `npm`
   environment as publication, downloads both closed archives, freshly reads
   the remote tag, and repeats the selected pair of admissions before it
   creates or verifies the immutable annotated tag. Only the single-purpose
   GitHub App can bypass the tag-creation rule. It re-reads current main
   immediately before a missing-tag push and resolves a failed or lost push
   response only through exact equality of the local raw tag-object SHA, the
   remote raw tag-object SHA, and the peeled release commit.
5. **Publish** runs in that protected environment with `id-token: write`. It
   re-reads the remote annotated tag's peeled commit against the frozen release
   SHA and current public main against the resolved controller SHA immediately
   inside the mutation step, exact-matches the invoked controller files to that
   commit, downloads only the closed artifact, and uploads a
   missing immutable version. npm and crates both execute current-main
   controller code while a separate exact-tag workspace supplies only catalog,
   version registry, and payload data. Neither terminal job builds, tests,
   packages, or executes tag- or artifact-carried code. Crates re-read current
   main and the peeled tag immediately before every actual crate PUT, including
   each bounded rate-limit retry.
6. **Verify published** has no OIDC permission. npm executes the resolved
   current-main verifier against the separate exact-tag payload; it reads
   registry bytes and provenance back. Crates additionally perform
   fresh-consumer compilation.

For npm, Close independently rebuilds both package names and compares SHA-1,
SHA-512, and archive bytes. The terminal runs the current-main
`npm_release.py publish` controller, which reads registry state and repeats the
main/tag fence after a missing result immediately before invoking npm. It points
its read-only payload root at the exact tag and never executes a script from
that payload or artifact. Every stable-version controller shares one global,
non-cancelling concurrency group because npm exposes no conditional dist-tag
write. This is an overlap lock, not a durable request queue: a pending
invocation superseded by GitHub before it starts has performed no mutation and
must be dispatched again. The closed archive requires exactly the official
registry, public access, provenance, and `latest` publish configuration; the terminal
also supplies those values plus `--ignore-scripts` explicitly. A missing
version is publishable only when no higher stable version exists, and the same
run must read back both exact immutable bytes and `dist-tags.latest` at that
version. An exact historical version remains recoverable after a higher stable
version exists, but recovery never moves `latest` backward.

Verify published uses exactly Node `v26.7.0` and npm `11.19.0`, downloads the
tarball, and verifies the complete Sigstore bundle before accepting its SLSA v1
statement. Three identities remain distinct and must all match:

- npm trusted-publisher configuration names the inert calling workflow
  `publish-npm-release.yml`;
- the Fulcio certificate SAN names the reusable
  `publish-npm-controller.yml@refs/heads/main` job, with the GitHub Actions OIDC
  issuer and required CT/Rekor evidence; its GitHub Build Signer URI and Build
  Signer Digest extensions (`1.3.6.1.4.1.57264.1.9` and `.1.10`) supply the
  exact `signer_ref` and `signer_sha`; and
- the SLSA external workflow/ref plus its singleton resolved Git dependency
  name the caller at either `refs/heads/main` for initial publication or the
  exact `refs/tags/v<version>` recovery ref, with the retained release SHA.

The statement also has one exact subject carrying the expected purl and digest
together. The publisher `signer_sha` must be a retained public-main ancestor
containing the signed controller file. It may therefore name the controller that
performed an earlier publication and is never replaced by the SHA of a later
finalization run.

For crates.io, Stage contains every catalog archive and its Cargo Registry Web
API upload body. Close independently regenerates both and requires byte
equality. Both Cargo metadata during the no-OIDC stage and a static source-
manifest read in the terminal controller exact-match the complete normalized
dependency graph and deterministic catalog order. The terminal then follows
that order and sends only those closed bodies with the short-lived trusted-
publisher token; it never invokes Cargo. Verify published compares registry
checksums, downloads exact bytes, then compiles a fresh registry consumer and
installs `cymule-cli`.

Every crates.io PUT result, including a transport loss, timeout, or non-429
HTTP failure, receives one bounded exact-checksum readback. Exact bytes
converge, different bytes fail, sustained reachable absence is a failed
attempt, and an unavailable readback reports an ambiguous outcome while
preserving the same crate/version/archive retry identity. The only automatic
PUT retry is an exact new-crate-name 429 response whose readback proves absence
and whose server retry time is parseable and bounded; the next PUT repeats the
main/tag fence. A new crate name still requires a separate reviewed, temporary
ownership bootstrap and trusted-publisher configuration; the normal workflow
has no token fallback.

## Finalization

`publish-npm-release.yml` and `publish-crates.yml` never create the GitHub Release.
After both registries are complete, dispatch `finalize-release.yml` from current
public `main`. Its credential-free verify job reruns exact-SHA soak, rebuilds
both npm tarballs and verifies registry bytes plus provenance, rebuilds every
crate archive, compares the complete catalog with crates.io, verifies fresh
Rust consumers, and uploads only the authenticated npm and Cargo stages.

A separate fresh credential-free freeze job checks out the exact current-main
controller and exact tag payload in separate roots, downloads those data stages,
requires the release ref to be an annotated tag object, records its exact
`release_tag_sha` separately from the peeled `release_sha`, re-reads both remote
identities, and reruns npm and crates.io byte/provenance readback. With exactly Node
`v26.7.0` and npm `11.19.0`, it preserves the Fulcio-signed publisher
`signer_ref`/`signer_sha`, then builds the closed BOM/2 and release notes into a
three-file finalization bundle whose manifest has exact `schema_version: 2` and
binds both Git identities plus the freeze controller SHA. The immutable BOM
binds the release SHA but intentionally excludes the mutable controller identity;
the current controller is bound outside those stable bytes by the finalization
stage, Artifact Attestation, and same-run control-plane receipt. Publication
evidence never substitutes one authority for another.

The protected attestation job has `contents: read`, rechecks that controller
SHA is still public `main`, re-reads the raw remote tag ref against
`release_tag_sha` and its peeled commit against `release_sha`, and revalidates
the complete BOM/2 projection:
release generation, registry digest, every schema/domain/migration edge, every
public Cargo/npm publication, and the explicit Go/Python manifest records.
`actions/attest` creates a GitHub Artifact Attestation for the exact BOM bytes
without ever receiving `contents: write` and emits its bundle as an immutable
workflow artifact.

After attestation, a separately protected `contents: read` control-plane job
mints one repository-scoped installation token with only Administration read
and Actions read. It reads
immutable-Release owner enforcement, both exact tag rulesets, the default
Actions permission ceiling, the default-branch rule authority, and all three
protected environments from GitHub. The current controller closes that
observation into `github-release-control-plane-receipt/1`: a self-digested,
15-minute receipt bound to repository, workflow run and attempt, controller,
release commit, raw annotated-tag object, observation/expiry times, and the
normalized settings snapshot. The App token is revoked with that job; only the
receipt artifact crosses the boundary.

The separately protected projection job is the only `contents: write` job and
runs no third-party Action. With built-in Git and GitHub CLI it fetches the
exact controller plus a complete data-only exact-release workspace and
downloads only the current run's frozen stage, attestation, and control-plane
receipt artifacts. It
authenticates the closed stage bytes, verifies the exact BOM attestation, and
only then checks the already-attested registry/package projection; the complete
workspace-derived semantic validator runs in both the credential-free freeze
and protected read-only attestor, never as an unpinned dependency install in
the mutation job. `finalize_release.py` re-verifies the bundle against the
current-main finalizer workflow and controller SHA before every Release
mutation. Immediately before draft creation, BOM upload, draft publication, or
Latest correction, after attestation and the current-main/raw-tag fence, its
final fail-before-write check revalidates the receipt digest, same-run/attempt
and Git identities, exact safe settings snapshot, and expiry. In particular,
owner-enforced immutable Releases are proven before `--draft=false`; a
post-publication `isImmutable` readback is convergence evidence, not the first
authority check. The receipt also binds the live repository default branch to
`main` before treating `~DEFAULT_BRANCH` rules as main protection. The writer
requires its stage, attestation and settings receipt to bind the same current
controller SHA, installs no toolchain, runs no
package lifecycle, and never executes tag payload. An existing
Release is accepted only when its tag, title, notes, draft state, prerelease
state, immutable state, exact one-BOM asset set, and BOM bytes match. REST asset
readback must preserve the exact asset database ID, name, byte size, server
SHA-256 digest, and `uploaded` state across publication. If main advances after
the first run uploads the BOM, the next current controller rebuilds the same
immutable BOM, creates a fresh stage and attestation, and completes the draft
without deleting or replacing its asset. The old run's stage is not accepted
under the new controller. The controller re-reads
the complete REST-paginated Release inventory and its exact `isLatest`
projection before and after every mutation, then re-reads current main, the raw
annotated tag object, and its peeled commit immediately before draft creation,
BOM upload, publication, or Latest correction and after the final metadata and
asset readback. Retagging the same commit with a different annotation or
signature is therefore a tag-authority change and fails
before mutation. All stable finalizations share the single non-cancelling
`finalize-release-stable` concurrency group. The controller compares only
canonical ASCII `vMAJOR.MINOR.PATCH` non-draft, non-prerelease Releases by
numeric SemVer: publishing the highest stable version passes explicit
`--latest=true`, while recovering a historical version passes explicit
`--latest=false`. Other Releases do not participate in that ordering, but a
non-stable, draft, or prerelease Latest owner, duplicate Release identity, or
multiple Latest flags fails closed. Terminal readback requires exactly one
Latest Release and requires it to be the highest published stable version, so
recovering v2 after v3 cannot roll the public pointer backward. The workflow is
idempotent, never publishes package bytes or moves a tag, and downloads the
published BOM for byte-for-byte terminal readback on both create and
already-existing paths.

The content-addressed Artifact Attestation is the terminal BOM authority;
GitHub Release is its owner-enforced immutable discovery projection. GitHub
does not expose a conditional draft-to-published CAS. Therefore the exact tag
creator, single `contents: write` workflow, protected environment, and disabled
administrator bypass are mandatory. If an out-of-band repository owner races a
draft asset in the final window, asset-ID/digest readback fails the run and the
attested BOM remains authoritative; the raced Release must not be treated as a
valid projection. After publication GitHub immutability prevents tag or asset
replacement and GitHub supplies its release attestation.

GitHub does not let the contents-only writer re-read repository Administration
settings, and no long-lived or write-capable administration credential is
admitted to that job. The remaining explicit operational trust boundary is:
from dispatch of the non-cancelling `finalize-release-stable` invocation,
including throughout its sequential live reads, until that same invocation
completes or is cancelled, settings
administrators must not change immutable-Release enforcement, release tag
rulesets, release environments, default Actions permissions, or the protected
default-branch authority. A receipt that expires is not extended or refreshed
inside the writer; the workflow must be rerun so a new protected preflight
observes live settings before any further mutation.

GitHub may supersede a pending invocation before it
starts; an invocation that never acquired the group has no mutation and must be
dispatched again.
