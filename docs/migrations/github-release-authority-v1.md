# GitHub release authority generation 1

Status: source implemented; operator execution pending.

The independent
[`public-mirror-receipt-carrier-v1.md`](public-mirror-receipt-carrier-v1.md)
migration must also be executed before finalization can authenticate the
private/public source mapping in BOM/3.

Owner: Cymule release maintainer with GitHub organization-owner, repository
ruleset, environment, GitHub App, Actions-secret, and immutable-release
administration access.

## Scope

This migration makes one repository-installed GitHub App the only creator of
`refs/tags/v*`, makes the finalizer workflow the only `contents: write`
workflow, disables environment administrator bypass, and enables
owner-enforced immutable GitHub Releases. The exact BOM/3 Artifact Attestation
is the terminal content authority; a GitHub Release is its immutable projection.

Required non-secret configuration:

- `CYMULE_RELEASE_TAG_APP_CLIENT_ID` and numeric
  `CYMULE_RELEASE_TAG_APP_ID` repository variables; the current-main controller
  exact-matches that App ID to the minted App slug through GitHub's live API and
  separately resolves the slug's bot user ID for annotated-tag identity;
- `CYMULE_GITHUB_MIRROR_INTEGRATION_ID` and
  `CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID` repository variables;
- `CYMULE_GITHUB_NPM_REVIEWER_TEAM_ID`,
  `CYMULE_GITHUB_CRATES_REVIEWER_TEAM_ID`, and
  `CYMULE_GITHUB_RELEASE_REVIEWER_TEAM_ID` repository variables; and
- `CYMULE_RELEASE_CONTROL_APP_ID`, identifying the repository-installed App
  whose only repository permissions are Administration read and Actions read.

Required secret configuration:

- `CYMULE_RELEASE_TAG_APP_PRIVATE_KEY`, scoped to the `npm` environment; and
- `CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY`, scoped to the
  `release-finalize` environment.

Both App installations are limited to this repository. The tag App token
request grants only repository `contents: write`. The control-plane App token
request grants only repository Administration read plus Actions read; Actions
read is required for environment policy readback. The two authorities never
share a job, private key, or installation token.

## Preflight

1. Freeze npm, crates.io, tag, and GitHub Release dispatches.
2. Confirm there is no in-progress release workflow and no draft Release.
3. Record current default-branch and `refs/tags/v*` rulesets, all three
   environment policies/reviewers, immutable-release settings, repository
   Actions permissions, installed App ID, and current public `main` SHA.
4. At that SHA run:

   ```sh
   python3 scripts/verify_release_workflows.py
   python3 -m unittest tests.harness.test_release_security
   ```

5. Confirm the App private key is not present in repository files, logs, or a
   repository-wide secret; it belongs only to the protected `npm` environment.
6. Confirm the separate control-plane App private key is not repository-wide;
   it belongs only to `release-finalize`, and that App has Administration read
   with no write permission.

## Stop conditions

Stop without dispatching a release if any of the following holds:

- the organization cannot enforce immutable Releases;
- the creation ruleset cannot restrict creation to one exact Integration, or
  the independent update/deletion ruleset cannot remain bypass-free;
- any environment cannot require its exact Team, prevent self-review, or set
  `can_admins_bypass=false`;
- another workflow retains `contents: write`;
- the control-plane App has any write permission or cannot read the exact
  rulesets, environments, Actions defaults, and immutable-Release setting;
- an existing `v*` tag is lightweight, mutable, or has ambiguous raw/peeled
  identity; or
- the settings token cannot read every required control-plane value.
- the tag App token cannot exact-read its App slug/App ID and distinct bot user
  identity from GitHub without redirects.

## Procedure

1. Create or select the dedicated release-tag GitHub App. Grant only repository
   Contents read/write, install it only on `cymule-framework/cymule`, and record
   its App/Integration ID without recording the private key.
2. Set repository variables `CYMULE_RELEASE_TAG_APP_CLIENT_ID` and the numeric
   `CYMULE_RELEASE_TAG_APP_ID`. Store the private key as
   `CYMULE_RELEASE_TAG_APP_PRIVATE_KEY` only in the `npm` environment.
   After minting the repository-scoped installation token, the current-main
   controller must exact-match `/apps/{slug}` to that App ID and
   `/users/{slug}[bot]` to one positive Bot user ID. Use the bot user ID—not the
   App ID—in the annotated tagger email.
3. Create a separate repository-installed control-plane App with only
   Administration read and Actions read. Set `CYMULE_RELEASE_CONTROL_APP_ID` and store
   `CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY` only in `release-finalize`. Do not
   grant Contents write or expose this credential to the projection job.
   Set the five `CYMULE_GITHUB_*` Integration/Team ID repository variables to
   the exact actors verified by the rulesets and environments.
4. Create one active exact `refs/tags/v*` ruleset containing only `creation`.
   Set its sole bypass actor to the release App Integration with
   `bypass_mode=always`. Create a second active exact `refs/tags/v*` ruleset
   containing only `update` and `deletion`, with an empty bypass list. This
   separation lets the App create one tag but gives it no update/delete escape.
   Remove every user, team, repository-role, deploy-key, and administrator
   bypass from both mutation protections.
5. For each of `npm`, `crates-io`, and `release-finalize`, configure exactly one
   required reviewer Team, enable prevent-self-review, and disable administrator
   bypass. Keep `npm` selected refs exactly typed branch `main` and tag `v*`;
   keep the other two protected-branch-only.
6. Enable immutable Releases at organization owner scope and confirm the
   repository reports both `enabled=true` and `enforced_by_owner=true`.
7. Keep default Actions permissions read-only with pull-request approval
   disabled. Confirm only `finalize-release.yml` grants job-level
   `contents: write`. Within that workflow, require a separate protected
   `contents: read` attestation job and a sole `contents: write` projection job
   with no third-party Action steps.
8. Run the live readback:

   ```sh
   GITHUB_TOKEN=<administration-read-token> \
   CYMULE_GITHUB_MIRROR_INTEGRATION_ID=<mirror-integration-id> \
   CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID=<tag-integration-id> \
   CYMULE_GITHUB_NPM_REVIEWER_TEAM_ID=<team-id> \
   CYMULE_GITHUB_CRATES_REVIEWER_TEAM_ID=<team-id> \
   CYMULE_GITHUB_RELEASE_REVIEWER_TEAM_ID=<team-id> \
     python3 scripts/verify_github_release_settings.py
   ```

9. Re-enable dispatch only after the source verifier and live readback both
   pass at the recorded current public `main` SHA.
10. Before the first finalization, brief every repository settings administrator
    on the explicit non-cancelling window: from finalization dispatch, including
    every sequential live-preflight read, until completion or cancellation, do
    not change
    immutable-Release enforcement, tag rulesets, release environments, default
    Actions permissions, or default-branch protection until that invocation
    completes or is cancelled. If the receipt expires, rerun finalization; do
    not extend it or give the contents writer an administration token.

## Verification

The next planned release is the first real end-to-end witness. Record:

- the workflow run and exact controller/release/raw-tag SHAs;
- the tag App Integration ID observed by the ruleset;
- exact environment reviewer approvals;
- the same-run control-plane receipt digest, observation time, expiry, and
  normalized settings snapshot;
- Artifact Attestation ID and BOM SHA-256;
- immutable Release ID and `isImmutable=true`;
- exact Release asset ID, name, size, digest, and `uploaded` state; and
- terminal Latest/readback result.

A failed run, mutable Release, changed asset ID, missing attestation, or merely
successful `gh release edit` response is not verification.

## Rollback

If configuration cannot be closed, disable release dispatch, revoke both
release App installations/private keys, and restore the recorded non-release settings only
where doing so does not weaken unrelated protection. Do not remove creation or
bypass-free update/deletion protection, enable administrator bypass, grant a
human tag bypass, or fall back to `GITHUB_TOKEN` tag writes. Immutable Releases
cannot be converted back into mutable authority; retain their attestation and
investigate any invalid
projection before another release.

## Execution record

- Executed at (UTC): pending
- Operator: pending
- Public `main` SHA: pending
- Release tag App/Integration ID: pending
- Reviewer Team IDs: pending
- Settings verifier result and control-plane App permission readback: pending
- First finalization control-plane receipt digest/expiry: pending
- First release workflow run: pending
- BOM attestation and immutable Release readback: pending
