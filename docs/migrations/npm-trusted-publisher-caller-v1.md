# npm trusted-publisher caller generation 1

Status: source implemented; operator execution pending.

Owner: Cymule release maintainer with npm package-owner access and GitHub
environment-administration access.

This one-time control-plane migration moves both public npm packages from the
retired `publish-npm.yml` trust identity to the new inert
`publish-npm-release.yml` caller. The caller contains no runner or executable
step. It supplies `cymule.npm-release-caller/1` to the reusable
`publish-npm-controller.yml@main`; the called write jobs execute only the
freshly resolved current-main controller while the caller ref remains the
standard npm provenance source. The resulting identity contract has three
distinct exact layers: npm trusts the caller filename, the SLSA external
workflow/ref and singleton Git dependency name that caller ref and release
SHA, and the Fulcio certificate SAN names
`publish-npm-controller.yml@refs/heads/main`. Provenance verification uses
exactly Node `v26.7.0` and npm `11.19.0`. It also extracts the Fulcio GitHub
Build Signer URI and Build Signer Digest extensions
(`1.3.6.1.4.1.57264.1.9` and `.1.10`) as the exact `signer_ref` and
`signer_sha`; the signed publisher commit remains distinct from any later
release-finalization controller.

## Scope

- npm package `cymule`;
- npm package `@cymule/sdk`;
- GitHub repository `cymule-framework/cymule`;
- trusted caller filename `publish-npm-release.yml`;
- protected GitHub environment `npm`; and
- allowed trusted-publisher operation `npm publish` only.

No package bytes, Git tag, GitHub Release, package owner, token, or user-facing
runtime changes during this migration. Never record an npm session, recovery
code, OIDC token, or other credential in this runbook.

## Preflight

1. Freeze release dispatches. Confirm no npm or release-finalization workflow is
   queued, running, or awaiting approval.
2. Record the exact public `main` SHA and prove it contains
   `publish-npm-release.yml`, `publish-npm-controller.yml`, and the verifier
   that rejects the retired filename and direct controller dispatch.
3. Run `python3 scripts/verify_release_workflows.py` at that SHA.
4. Confirm `publish-npm-release.yml` calls the literal same-repository target
   `cymule-framework/cymule/.github/workflows/publish-npm-controller.yml@main`
   and passes caller generation `/1`. Its sole call job must grant exactly
   `contents: read` and `id-token: write`. The called tag job obtains its
   separate narrow App token under the companion
   [`github-release-authority-v1.md`](github-release-authority-v1.md)
   migration; the inert caller has no runner, step, or environment.
   Expressions, another repository, another ref, or a directly dispatchable
   controller are stop conditions.
5. Confirm tag admission, terminal publication, and final registry
   verification execute `npm_release.py` only from the resolved current-main
   controller. The exact-tag checkout may supply the version registry and
   closed archive data but never executable release code.
6. Read and record the GitHub `npm` environment deployment policy and reviewer
   configuration. Its terminal target is selected-ref mode with exactly branch
   `main` and tag `v*`, plus the exact reviewer Team, non-self approval, and
   administrator bypass disabled. “Protected branches only” is not
   sufficient because GitHub does not admit tag-triggered jobs through that
   mode.
7. Read both npm package settings. Record the existing trusted-publisher
   repository, filename, environment, and allowed operation without copying any
   credential. Both packages must still be owned by the intended npm
   organization and must not have an unexpected publisher record.

## Stop conditions

Stop without changing either package if:

- public main moves after preflight;
- either package, repository, environment, or trusted-publisher record differs
  from Scope;
- a release workflow is active;
- npm cannot configure the same new caller identity for both packages;
- the GitHub environment cannot configure typed branch `main` and tag `v*`
  rules without admitting another ref;
- the old publisher cannot be removed; or
- live readback is unavailable.

If public main moves, restart preflight at the new exact SHA. Never compensate
with a token-based publish path or by trusting `publish-npm-controller.yml`
directly.

## Execution

Keep release dispatch disabled for the whole two-package interval.

1. For `cymule`, replace the trusted GitHub Actions workflow filename with
   `publish-npm-release.yml`, retain repository `cymule-framework/cymule`, bind
   environment `npm`, and allow `npm publish` only.
2. Read the saved `cymule` publisher back and exact-match every field. Do not
   dispatch a release yet.
3. Apply and read back the identical publisher configuration for
   `@cymule/sdk`.
4. Confirm neither package retains a publisher for `publish-npm.yml` or
   `publish-npm-controller.yml`.
5. Configure the `npm` environment for selected branches and tags. Delete every
   deployment-ref rule except typed branch `main` and typed tag `v*`; set the
   exact reviewed Team, preserve non-self approval, and disable administrator
   bypass.
6. Read back the environment policy, both typed ref rules, and reviewer rule.
   Any duplicate or additional rule fails closure.
7. Re-read the exact public main SHA. It must still equal the preflight SHA.
8. Run `python3 scripts/verify_github_release_settings.py` with the required
   non-secret mirror/tag Integration IDs, exact reviewer Team IDs, and an
   administration-read token supplied only through the process environment.
9. Re-enable release dispatch only after both package readbacks and the GitHub
   environment readback are closed.

## Verification

For each package, retain an operator receipt containing only timestamp, actor,
package, repository, caller filename, environment, allowed operation, and the
exact public main SHA. Then verify:

- the npm settings readback names `publish-npm-release.yml` exactly;
- the retired filename and direct controller are absent;
- GitHub still exposes only the inert caller as manually dispatchable;
- the caller's sole call job supplies the exact read-only-contents/OIDC permission
  ceiling, while every called job narrows to its own authority;
- SLSA names the exact caller ref/release SHA while Fulcio separately names the
  current-main controller SAN and its signed controller ref/SHA extensions,
  verified by Node `v26.7.0` and npm `11.19.0`;
- `publish-npm-controller.yml` exposes `workflow_call` but no
  `workflow_dispatch`;
- the `npm` environment admits only typed branch `main` and tag `v*`, requires
  non-self approval, and has no duplicate or additional deployment-ref rule;
  and
- `python3 scripts/verify_release_workflows.py` still passes at the recorded
  main SHA.

A real package publication is not a migration probe. The next planned release
must independently verify exact registry bytes and provenance through the normal
release workflow.

## Rollback

Rollback is fail-closed: remove the newly added trusted-publisher records from
both packages, restore the exact preflight environment policy if it changed,
and keep release dispatch disabled. Do **not** restore trust in
`publish-npm.yml`, grant trust directly to the controller, or introduce an npm
token. Correct source or control-plane state, repeat Preflight, and execute the
migration again.

If source must move back temporarily, leaving the new publisher filename in npm
is also fail-closed because that workflow does not exist on old tags. Publication
remains disabled until current source and both live publisher records pass this
runbook again.

## Execution record

Not executed. The operator must append a dated record containing the exact
public main SHA, both non-secret npm settings readbacks, GitHub environment
readback, verifier result, and final enabled/disabled decision.
