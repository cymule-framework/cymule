# Public mirror receipt carrier generation 1

Status: source implemented; operator execution pending. Release finalization is
unauthorized until this migration has executed and the live settings verifier
passes at the current public `main` SHA.

Owner: Cymule release maintainer with GitLab protected-environment authority,
GitHub organization-owner and repository-ruleset administration access, and
administration-read access to `cymule-framework/cymule`.

## Scope

This migration establishes one authenticated cross-system carrier for the
private-to-public source mapping. The private `mirror-public` controller writes
the registered `cymule.public-mirror-receipt/2` shape and creates
an annotated tag named `cymule-mirror/<public-source-sha>` whose exact target is
the rewritten public commit and whose canonical message closes:

- the exact private source SHA;
- the exact rewritten public source SHA; and
- the shared `cymule.public-source-snapshot/1` digest.

The main update and a new receipt tag are published atomically. A retry accepts
only the same raw receipt-tag object and the same public tip. Public GitHub
Actions holds no mirror credential: finalization reads the immutable tag,
recomputes the public source snapshot, matches the version registry generation,
and carries the private/public mapping through
`cymule.release-finalization-stage/3`, the BOM/3 attestation, and
`cymule.github-release-control-plane-receipt/2`, whose settings projection is
`cymule.github-release-settings-snapshot/2`.

Two exact GitHub tag rulesets make the carrier authentic:

- `refs/tags/cymule-mirror/*` creation has the private mirror GitHub App
  Integration as its sole always-on bypass; and
- `refs/tags/cymule-mirror/*` update and deletion have no bypass actor.

## Preflight

1. Freeze public mirror mutation and every package or GitHub Release dispatch.
2. Confirm no `mirror-public`, package publication, or finalization workflow is
   running.
3. Record the current private default-branch SHA, public `main` SHA, raw
   `refs/tags/cymule-mirror/*` inventory, mirror App Integration ID, and all
   current repository rulesets.
4. Confirm the private mirror publisher token is protected, masked, and scoped
   only to the private `public-mirror` environment. Do not expose its private
   variable name or print or copy its value into public source or evidence.
5. Confirm that the token belongs to the same narrowly installed GitHub App
   Integration identified by `CYMULE_GITHUB_MIRROR_INTEGRATION_ID` and has only
   the repository Contents permission required by the mirror controller.
6. At the recorded private source SHA run:

   ```sh
   python3 scripts/verify_release_workflows.py
   python3 -m unittest tests.harness.test_release_security
   CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=controller \
     ./.gitlab/scripts/test_public_mirror_controller.sh
   ```

7. Confirm every existing public stable release tag either has one exact
   receipt carrier from the corresponding authenticated private pipeline or is
   explicitly ineligible for finalization. Public history alone cannot recover
   or invent a missing private source SHA.

## Stop conditions

Stop without enabling mirror or release mutation if:

- GitHub cannot create the two exact receipt-tag rulesets;
- any user, Team, role, deploy key, administrator, or second Integration can
  create a receipt tag;
- any actor can update or delete a receipt tag;
- an existing receipt ref is lightweight, points at a different public commit,
  has a noncanonical message, or conflicts with the deterministic raw tag
  object reconstructed by the private controller;
- the mirror App identity or token scope cannot be exact-read;
- private and public source snapshots differ;
- public `main` or the private default branch moves during the migration; or
- a historical release lacks a receipt and the exact private source pipeline
  can no longer be authenticated and rerun.

## Procedure

1. Create one active tag ruleset scoped exactly to
   `refs/tags/cymule-mirror/*` with the sole rule `creation`. Configure the
   mirror GitHub App Integration as the only bypass actor with
   `bypass_mode=always`.
2. Create a second active tag ruleset scoped exactly to the same pattern with
   only `update` and `deletion`. Leave its bypass actor list empty.
3. Do not add a public workflow, repository secret, deploy key, personal token,
   or human bypass. The existing protected private mirror job remains the only
   receipt creator.
4. Run `scripts/verify_github_release_settings.py` with an
   administration-read token and the documented Integration/Team IDs. It must
   return success only after both receipt-tag rulesets are observed exactly.
5. Re-read the private and public branch SHAs from preflight. If either moved,
   stop and restart preflight.
6. Re-enable one current private-default-branch mirror pipeline. Require its
   terminal artifact to record the private SHA, public SHA, shared snapshot,
   receipt ref, and raw receipt-tag SHA. Require GitHub readback to show both
   public `main` and the exact receipt ref.
7. Fetch the receipt ref without credentials from the public repository and
   run the current controller against the exact public checkout:

   ```sh
   public_sha=<recorded-public-sha>
   receipt_ref="refs/tags/cymule-mirror/$public_sha"
   git fetch --force origin "$receipt_ref:$receipt_ref"
   python3 scripts/finalize_release.py verify-mirror-receipt \
     --public-source-sha "$public_sha"
   ```

8. Re-enable package and finalization dispatch only after the source verifier,
   live settings verifier, mirror artifact, public ref readback, and local
   receipt validation all agree.

## Verification

Record all of the following without credentials:

- private pipeline ID and exact private source SHA;
- exact rewritten public source SHA and public `main` readback;
- shared public source-snapshot digest;
- receipt ref and raw annotated-tag object SHA;
- receipt tag target, canonical message validation, and no-credential fetch;
- creation ruleset ID plus sole mirror Integration bypass;
- update/deletion ruleset ID plus empty bypass list;
- settings verifier result at the exact public `main` SHA; and
- the first finalization stage, attestation, and control-plane receipt that bind
  the same mapping.

A successful main push without the receipt tag, a matching tree without the
private SHA, an artifact that was not published as the protected immutable ref,
or a green finalization against different mapping values is not verification.

## Rollback

If the source or control plane cannot close, disable mirror and release
dispatch. Retain every already-created receipt tag and its update/deletion
protection; do not delete, move, or replace it. Revoke or rotate the private
mirror credential only through the protected GitLab environment procedure.
Restore the recorded prior non-release settings only when that does not weaken
main, release-tag, immutable-Release, or receipt-tag protection. There is no
fallback that sets both BOM SHA fields to the public commit or reconstructs a
private SHA in public Actions.

## Execution record

- Executed at (UTC): pending
- Operator: pending
- Private pipeline ID and source SHA: pending
- Public `main` SHA: pending
- Mirror App/Integration ID: pending
- Receipt creation ruleset ID/readback: pending
- Receipt immutability ruleset ID/readback: pending
- Receipt ref and raw tag-object SHA: pending
- Shared source-snapshot digest: pending
- Settings verifier result: pending
- First bound finalization run/stage/attestation/receipt: pending
