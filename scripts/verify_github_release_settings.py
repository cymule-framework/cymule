#!/usr/bin/env python3
"""Verify the GitHub control-plane gates required for public releases."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

from release_contracts import (
    CONTROL_PLANE_RECEIPT_VERSION,
    CONTROL_PLANE_SETTINGS_VERSION,
)


DEFAULT_REPOSITORY = "cymule-framework/cymule"
REQUIRED_STATUS_CONTEXTS = {"Required CI"}
GITHUB_ACTIONS_INTEGRATION_ID = 15368
CONTROL_PLANE_RECEIPT_TTL_SECONDS = 15 * 60
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
POSITIVE_DECIMAL_PATTERN = re.compile(r"[1-9][0-9]*")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"GitHub settings response repeats object member {key!r}")
        value[key] = item
    return value


def _reject_float(value: str) -> object:
    raise ValueError(f"GitHub settings response contains non-integral number {value}")


def load_github_json(payload: bytes) -> object:
    return json.loads(
        payload,
        object_pairs_hook=_unique_object,
        parse_float=_reject_float,
        parse_constant=_reject_float,
    )


def canonical_json_bytes(value: object) -> bytes:
    """Encode one closed receipt value without insignificant JSON variation."""

    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sha256_identity(value: object) -> str:
    return f"sha256:{hashlib.sha256(canonical_json_bytes(value)).hexdigest()}"


def projected_bypass_actors(ruleset: dict[str, object]) -> list[dict[str, object]]:
    observed = ruleset.get("bypass_actors", [])
    if not isinstance(observed, list):
        raise ValueError(f"ruleset {ruleset.get('name')} bypass actors are malformed")
    projected = [
        {
            "actor_id": actor.get("actor_id"),
            "actor_type": actor.get("actor_type"),
            "bypass_mode": actor.get("bypass_mode"),
        }
        for actor in observed
        if isinstance(actor, dict)
    ]
    if len(projected) != len(observed):
        raise ValueError(f"ruleset {ruleset.get('name')} bypass actors are malformed")
    return projected


def github_json(repository: str, path: str, token: str) -> object:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "cymule-release-settings/1",
            "X-GitHub-Api-Version": "2026-03-10",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return load_github_json(response.read())


def github_json_list(repository: str, path: str, token: str) -> list[object]:
    """Read every page of one GitHub list endpoint without truncation."""

    values: list[object] = []
    page = 1
    while True:
        separator = "&" if "?" in path else "?"
        result = github_json(
            repository,
            f"{path}{separator}per_page=100&page={page}",
            token,
        )
        if not isinstance(result, list):
            raise ValueError(f"GitHub returned a malformed paginated list for {path}")
        values.extend(result)
        if len(result) < 100:
            return values
        page += 1


def includes_ref(ruleset: dict[str, object], expected: str) -> bool:
    conditions = ruleset.get("conditions", {})
    ref_name = conditions.get("ref_name", {}) if isinstance(conditions, dict) else {}
    include = ref_name.get("include", []) if isinstance(ref_name, dict) else []
    return expected in include


def verify_exact_ref_scope(ruleset: dict[str, object], expected: str) -> None:
    conditions = ruleset.get("conditions", {})
    ref_name = conditions.get("ref_name", {}) if isinstance(conditions, dict) else {}
    include = ref_name.get("include", []) if isinstance(ref_name, dict) else []
    exclude = ref_name.get("exclude", []) if isinstance(ref_name, dict) else []
    if conditions != {
        "ref_name": {"include": [expected], "exclude": []}
    }:
        raise ValueError(
            f"ruleset {ruleset.get('name')} has non-exact ref scope "
            f"include={include} exclude={exclude}"
        )


def require_ruleset(
    rulesets: list[dict[str, object]],
    *,
    target: str,
    ref: str,
    rules: set[str],
) -> dict[str, object]:
    for ruleset in rulesets:
        if (
            ruleset.get("enforcement") == "active"
            and ruleset.get("target") == target
            and includes_ref(ruleset, ref)
        ):
            observed = {
                rule.get("type")
                for rule in ruleset.get("rules", [])
                if isinstance(rule, dict)
            }
            if rules <= observed:
                return ruleset
    raise ValueError(f"no active {target} ruleset protects {ref} with {sorted(rules)}")


def require_exact_ruleset(
    rulesets: list[dict[str, object]],
    *,
    target: str,
    ref: str,
    rules: set[str],
) -> dict[str, object]:
    """Select one active exact-scope ruleset with no unrelated rule authority."""

    matches = []
    for ruleset in rulesets:
        raw_rules = ruleset.get("rules")
        if not isinstance(raw_rules, list) or not all(
            isinstance(rule, dict) and isinstance(rule.get("type"), str)
            for rule in raw_rules
        ):
            continue
        observed = {rule["type"] for rule in raw_rules}
        if (
            ruleset.get("enforcement") == "active"
            and ruleset.get("target") == target
            and includes_ref(ruleset, ref)
            and observed == rules
            and len(observed) == len(raw_rules)
        ):
            matches.append(ruleset)
    if len(matches) != 1:
        raise ValueError(
            f"expected one exact active {target} ruleset for {ref} with "
            f"{sorted(rules)}, found {len(matches)}"
        )
    verify_exact_ref_scope(matches[0], ref)
    return matches[0]


def verify_main_bypass(ruleset: dict[str, object], integration_id: int) -> None:
    expected = [
        {
            "actor_id": integration_id,
            "actor_type": "Integration",
            "bypass_mode": "always",
        }
    ]
    projected = projected_bypass_actors(ruleset)
    if projected != expected:
        raise ValueError(
            f"ruleset {ruleset.get('name')} must grant only exact mirror Integration "
            f"{integration_id}, found {projected}"
        )


def verify_release_tag_bypass(
    ruleset: dict[str, object], integration_id: int
) -> None:
    expected = [
        {
            "actor_id": integration_id,
            "actor_type": "Integration",
            "bypass_mode": "always",
        }
    ]
    projected = projected_bypass_actors(ruleset)
    if projected != expected:
        raise ValueError(
            f"ruleset {ruleset.get('name')} must grant tag creation only to "
            f"Integration {integration_id}, found {projected}"
        )


def verify_no_bypass(ruleset: dict[str, object]) -> None:
    """Require a ruleset whose protections no actor can bypass."""

    if projected_bypass_actors(ruleset) != []:
        raise ValueError(f"ruleset {ruleset.get('name')} must have no bypass actor")


def verify_required_status_checks(ruleset: dict[str, object]) -> None:
    matches = [
        rule
        for rule in ruleset.get("rules", [])
        if isinstance(rule, dict) and rule.get("type") == "required_status_checks"
    ]
    if len(matches) != 1:
        raise ValueError("main ruleset must contain one required-status-check rule")
    parameters = matches[0].get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("main required-status-check parameters are malformed")
    if parameters.get("strict_required_status_checks_policy") is not True:
        raise ValueError("main status checks must require the current branch head")
    checks = parameters.get("required_status_checks", [])
    contexts = {check.get("context") for check in checks if isinstance(check, dict)}
    if contexts != REQUIRED_STATUS_CONTEXTS or len(checks) != len(contexts):
        raise ValueError(
            f"main status contexts must be exactly {sorted(REQUIRED_STATUS_CONTEXTS)}, "
            f"found {sorted(value for value in contexts if isinstance(value, str))}"
        )
    if any(
        check.get("integration_id") != GITHUB_ACTIONS_INTEGRATION_ID
        for check in checks
        if isinstance(check, dict)
    ):
        raise ValueError("Required CI must be bound to the GitHub Actions Integration")


def verify_immutable_releases(settings: object) -> dict[str, bool]:
    """Require immutable Release projection as an owner-enforced invariant."""

    if not isinstance(settings, dict) or (
        settings.get("enabled") is not True
        or settings.get("enforced_by_owner") is not True
    ):
        raise ValueError(
            "GitHub immutable Releases must be enabled and enforced by the owner"
        )
    return {"enabled": True, "enforced_by_owner": True}


def verify_environment(
    repository: str,
    name: str,
    token: str,
    *,
    expected_reviewers: set[tuple[str, int]],
    selected_refs: set[tuple[str, str]] | None = None,
) -> dict[str, object]:
    encoded_name = urllib.parse.quote(name, safe="")
    environment = github_json(repository, f"/environments/{encoded_name}", token)
    if not isinstance(environment, dict):
        raise ValueError(f"GitHub returned malformed environment {name}")
    if environment.get("can_admins_bypass") is not False:
        raise ValueError(f"environment {name} must disable administrator bypass")
    policy = environment.get("deployment_branch_policy")
    observed_refs: list[dict[str, str]] | None = None
    if selected_refs is None:
        if policy != {"protected_branches": True, "custom_branch_policies": False}:
            raise ValueError(
                f"environment {name} must admit only protected branches, found {policy}"
            )
    else:
        expected_policy = {
            "protected_branches": False,
            "custom_branch_policies": True,
        }
        if policy != expected_policy:
            raise ValueError(
                f"environment {name} must use selected branch/tag rules, found {policy}"
            )
        raw_policies = github_json(
            repository,
            f"/environments/{encoded_name}/deployment-branch-policies?per_page=100",
            token,
        )
        if not isinstance(raw_policies, dict):
            raise ValueError(f"GitHub returned malformed deployment policies for {name}")
        values = raw_policies.get("branch_policies")
        total = raw_policies.get("total_count")
        if (
            not isinstance(values, list)
            or type(total) is not int
            or total != len(values)
        ):
            raise ValueError(f"GitHub returned incomplete deployment policies for {name}")
        observed = {
            (value.get("type"), value.get("name"))
            for value in values
            if isinstance(value, dict)
            and isinstance(value.get("type"), str)
            and isinstance(value.get("name"), str)
        }
        if len(observed) != len(values) or observed != selected_refs:
            raise ValueError(
                f"environment {name} deployment refs must be exactly "
                f"{sorted(selected_refs)}, found {sorted(observed)}"
            )
        observed_refs = [
            {"type": ref_type, "name": ref_name}
            for ref_type, ref_name in sorted(observed)
        ]
    rules = environment.get("protection_rules")
    if not isinstance(rules, list):
        raise ValueError(f"environment {name} omitted protection rules")
    reviewer_rules = [
        rule
        for rule in rules
        if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
    ]
    if len(reviewer_rules) != 1:
        raise ValueError(f"environment {name} must have one reviewer rule")
    reviewers = reviewer_rules[0]
    if reviewers.get("prevent_self_review") is not True:
        raise ValueError(
            f"environment {name} requires non-self approval before OIDC publication"
        )
    reviewer_values = reviewers.get("reviewers")
    if not isinstance(reviewer_values, list) or not reviewer_values:
        raise ValueError(f"environment {name} has no required reviewer")
    reviewer_identities = {
        (reviewer.get("type"), identity.get("id"))
        for reviewer in reviewer_values
        if isinstance(reviewer, dict)
        and reviewer.get("type") in {"User", "Team"}
        and isinstance((identity := reviewer.get("reviewer")), dict)
        and type(identity.get("id")) is int
        and identity["id"] > 0
    }
    if len(reviewer_identities) != len(reviewer_values):
        raise ValueError(f"environment {name} has malformed or duplicate reviewers")
    if reviewer_identities != expected_reviewers:
        raise ValueError(
            f"environment {name} reviewers must be exactly "
            f"{sorted(expected_reviewers)}, found {sorted(reviewer_identities)}"
        )
    return {
        "can_admins_bypass": False,
        "deployment_branch_policy": policy,
        "required_reviewers": [
            {"type": reviewer_type, "id": reviewer_id}
            for reviewer_type, reviewer_id in sorted(reviewer_identities)
        ],
        "selected_refs": observed_refs,
    }


def verify(
    repository: str,
    token: str,
    mirror_integration_id: int,
    release_tag_integration_id: int,
    npm_reviewer_team_id: int,
    crates_reviewer_team_id: int,
    release_reviewer_team_id: int,
) -> dict[str, object]:
    metadata = github_json(repository, "", token)
    if not isinstance(metadata, dict) or metadata.get("default_branch") != "main":
        raise ValueError("release repository default branch must be exactly main")
    raw_rulesets = github_json_list(
        repository, "/rulesets?includes_parents=true", token
    )
    rulesets = []
    ruleset_ids: set[int] = set()
    for value in raw_rulesets:
        if (
            not isinstance(value, dict)
            or type(value.get("id")) is not int
            or value["id"] <= 0
        ):
            raise ValueError("GitHub returned a malformed ruleset summary")
        if value["id"] in ruleset_ids:
            raise ValueError("GitHub returned duplicate ruleset identities")
        ruleset_ids.add(value["id"])
        detail = github_json(repository, f"/rulesets/{value['id']}", token)
        if not isinstance(detail, dict) or detail.get("id") != value["id"]:
            raise ValueError(f"GitHub returned malformed ruleset {value['id']}")
        rulesets.append(detail)
    main = require_ruleset(
        rulesets,
        target="branch",
        ref="~DEFAULT_BRANCH",
        rules={"deletion", "non_fast_forward", "required_status_checks"},
    )
    release_tag_creation = require_exact_ruleset(
        rulesets,
        target="tag",
        ref="refs/tags/v*",
        rules={"creation"},
    )
    immutable_release_tags = require_exact_ruleset(
        rulesets,
        target="tag",
        ref="refs/tags/v*",
        rules={"deletion", "update"},
    )
    mirror_receipt_creation = require_exact_ruleset(
        rulesets,
        target="tag",
        ref="refs/tags/cymule-mirror/*",
        rules={"creation"},
    )
    immutable_mirror_receipts = require_exact_ruleset(
        rulesets,
        target="tag",
        ref="refs/tags/cymule-mirror/*",
        rules={"deletion", "update"},
    )
    verify_exact_ref_scope(main, "~DEFAULT_BRANCH")
    verify_required_status_checks(main)
    verify_main_bypass(main, mirror_integration_id)
    verify_release_tag_bypass(release_tag_creation, release_tag_integration_id)
    verify_no_bypass(immutable_release_tags)
    verify_release_tag_bypass(mirror_receipt_creation, mirror_integration_id)
    verify_no_bypass(immutable_mirror_receipts)
    permissions = github_json(repository, "/actions/permissions/workflow", token)
    expected_permissions = {
        "default_workflow_permissions": "read",
        "can_approve_pull_request_reviews": False,
    }
    if permissions != expected_permissions:
        raise ValueError(f"unsafe default Actions permissions: {permissions}")
    immutable_releases = verify_immutable_releases(
        github_json(repository, "/immutable-releases", token)
    )
    npm_environment = verify_environment(
        repository,
        "npm",
        token,
        expected_reviewers={("Team", npm_reviewer_team_id)},
        selected_refs={("branch", "main"), ("tag", "v*")},
    )
    crates_environment = verify_environment(
        repository,
        "crates-io",
        token,
        expected_reviewers={("Team", crates_reviewer_team_id)},
    )
    release_environment = verify_environment(
        repository,
        "release-finalize",
        token,
        expected_reviewers={("Team", release_reviewer_team_id)},
    )
    main_status_rule = next(
        rule
        for rule in main["rules"]
        if isinstance(rule, dict) and rule.get("type") == "required_status_checks"
    )
    main_status_parameters = main_status_rule["parameters"]
    if not isinstance(main_status_parameters, dict):
        raise ValueError("verified main status parameters changed type")
    main_checks = main_status_parameters["required_status_checks"]
    if not isinstance(main_checks, list):
        raise ValueError("verified main status checks changed type")
    snapshot = {
        "snapshot_version": CONTROL_PLANE_SETTINGS_VERSION,
        "default_branch": "main",
        "authorities": {
            "mirror_integration_id": mirror_integration_id,
            "release_tag_integration_id": release_tag_integration_id,
            "npm_reviewer_team_id": npm_reviewer_team_id,
            "crates_reviewer_team_id": crates_reviewer_team_id,
            "release_reviewer_team_id": release_reviewer_team_id,
        },
        "rulesets": {
            "main": {
                "enforcement": "active",
                "target": "branch",
                "ref": "~DEFAULT_BRANCH",
                "required_status_checks": sorted(
                    (
                        {
                            "context": check["context"],
                            "integration_id": check["integration_id"],
                        }
                        for check in main_checks
                        if isinstance(check, dict)
                    ),
                    key=lambda value: (value["context"], value["integration_id"]),
                ),
                "strict_required_status_checks_policy": True,
                "bypass_actors": projected_bypass_actors(main),
            },
            "release_tag_creation": {
                "enforcement": "active",
                "target": "tag",
                "ref": "refs/tags/v*",
                "rules": ["creation"],
                "bypass_actors": projected_bypass_actors(release_tag_creation),
            },
            "release_tag_immutable": {
                "enforcement": "active",
                "target": "tag",
                "ref": "refs/tags/v*",
                "rules": ["deletion", "update"],
                "bypass_actors": projected_bypass_actors(immutable_release_tags),
            },
            "mirror_receipt_creation": {
                "enforcement": "active",
                "target": "tag",
                "ref": "refs/tags/cymule-mirror/*",
                "rules": ["creation"],
                "bypass_actors": projected_bypass_actors(mirror_receipt_creation),
            },
            "mirror_receipt_immutable": {
                "enforcement": "active",
                "target": "tag",
                "ref": "refs/tags/cymule-mirror/*",
                "rules": ["deletion", "update"],
                "bypass_actors": projected_bypass_actors(
                    immutable_mirror_receipts
                ),
            },
        },
        "actions_permissions": expected_permissions,
        "immutable_releases": immutable_releases,
        "environments": {
            "npm": npm_environment,
            "crates-io": crates_environment,
            "release-finalize": release_environment,
        },
    }
    return snapshot


def create_control_plane_receipt(
    *,
    output: pathlib.Path,
    repository: str,
    run_id: str,
    run_attempt: str,
    controller_sha: str,
    release_sha: str,
    release_tag_sha: str,
    private_source_sha: str,
    mirror_receipt_tag_sha: str,
    public_source_snapshot_digest: str,
    settings_snapshot: dict[str, object],
    observed_at: dt.datetime | None = None,
) -> pathlib.Path:
    """Close one short-lived live-settings observation for the Release writer."""

    if not repository or repository.count("/") != 1:
        raise ValueError("release repository is invalid")
    for label, value in (("run ID", run_id), ("run attempt", run_attempt)):
        if POSITIVE_DECIMAL_PATTERN.fullmatch(value) is None:
            raise ValueError(f"{label} must be one positive decimal string")
    for label, value in (
        ("controller SHA", controller_sha),
        ("release SHA", release_sha),
        ("release tag SHA", release_tag_sha),
        ("private source SHA", private_source_sha),
        ("mirror receipt tag SHA", mirror_receipt_tag_sha),
    ):
        if GIT_SHA_PATTERN.fullmatch(value) is None:
            raise ValueError(f"{label} must be one exact lowercase Git identity")
    if release_tag_sha == release_sha:
        raise ValueError("release tag SHA must be a distinct annotated tag object")
    if private_source_sha == release_sha:
        raise ValueError("private and public source SHA must be distinct")
    if not isinstance(public_source_snapshot_digest, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", public_source_snapshot_digest
    ) is None:
        raise ValueError("public source snapshot digest is malformed")
    if (
        settings_snapshot.get("snapshot_version")
        != CONTROL_PLANE_SETTINGS_VERSION
    ):
        raise ValueError("GitHub release settings snapshot has the wrong generation")
    if settings_snapshot.get("default_branch") != "main":
        raise ValueError("GitHub release settings default branch must be exactly main")

    instant = observed_at or dt.datetime.now(dt.timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() != dt.timedelta(0):
        raise ValueError("control-plane receipt time must be UTC")
    instant = instant.replace(microsecond=0)
    expires = instant + dt.timedelta(seconds=CONTROL_PLANE_RECEIPT_TTL_SECONDS)
    receipt: dict[str, object] = {
        "receipt_version": CONTROL_PLANE_RECEIPT_VERSION,
        "repository": repository,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "controller_sha": controller_sha,
        "release_sha": release_sha,
        "release_tag_sha": release_tag_sha,
        "private_source_sha": private_source_sha,
        "mirror_receipt_tag_sha": mirror_receipt_tag_sha,
        "public_source_snapshot_digest": public_source_snapshot_digest,
        "observed_at": instant.isoformat().replace("+00:00", "Z"),
        "expires_at": expires.isoformat().replace("+00:00", "Z"),
        "settings_snapshot": settings_snapshot,
    }
    receipt["receipt_sha256"] = sha256_identity(receipt)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY)
    parser.add_argument(
        "--mirror-integration-id",
        type=int,
        default=os.environ.get("CYMULE_GITHUB_MIRROR_INTEGRATION_ID"),
        required=os.environ.get("CYMULE_GITHUB_MIRROR_INTEGRATION_ID") is None,
    )
    parser.add_argument(
        "--release-tag-integration-id",
        type=int,
        default=os.environ.get("CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID"),
        required=os.environ.get("CYMULE_GITHUB_RELEASE_TAG_INTEGRATION_ID") is None,
    )
    for environment in ("npm", "crates", "release"):
        variable = f"CYMULE_GITHUB_{environment.upper()}_REVIEWER_TEAM_ID"
        parser.add_argument(
            f"--{environment}-reviewer-team-id",
            type=int,
            default=os.environ.get(variable),
            required=os.environ.get(variable) is None,
        )
    parser.add_argument("--receipt-output", type=pathlib.Path)
    parser.add_argument("--run-id")
    parser.add_argument("--run-attempt")
    parser.add_argument("--controller-sha")
    parser.add_argument("--release-sha")
    parser.add_argument("--release-tag-sha")
    parser.add_argument("--private-source-sha")
    parser.add_argument("--mirror-receipt-tag-sha")
    parser.add_argument("--public-source-snapshot-digest")
    args = parser.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError(
            "GITHUB_TOKEN with repository Administration read and Actions read is required"
        )
    numeric_authorities = {
        "mirror Integration ID": args.mirror_integration_id,
        "release tag Integration ID": args.release_tag_integration_id,
        "npm reviewer Team ID": args.npm_reviewer_team_id,
        "crates reviewer Team ID": args.crates_reviewer_team_id,
        "release reviewer Team ID": args.release_reviewer_team_id,
    }
    for label, value in numeric_authorities.items():
        if value <= 0:
            raise ValueError(f"{label} must be positive")
    snapshot = verify(
        args.repository,
        token,
        args.mirror_integration_id,
        args.release_tag_integration_id,
        args.npm_reviewer_team_id,
        args.crates_reviewer_team_id,
        args.release_reviewer_team_id,
    )
    receipt_bindings = {
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "controller_sha": args.controller_sha,
        "release_sha": args.release_sha,
        "release_tag_sha": args.release_tag_sha,
        "private_source_sha": args.private_source_sha,
        "mirror_receipt_tag_sha": args.mirror_receipt_tag_sha,
        "public_source_snapshot_digest": args.public_source_snapshot_digest,
    }
    if args.receipt_output is None:
        if any(value is not None for value in receipt_bindings.values()):
            raise ValueError("receipt bindings require --receipt-output")
        print(f"verified GitHub release control plane for {args.repository}")
    else:
        missing = [name for name, value in receipt_bindings.items() if value is None]
        if missing:
            raise ValueError(f"control-plane receipt omits bindings {missing}")
        create_control_plane_receipt(
            output=args.receipt_output,
            repository=args.repository,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            controller_sha=args.controller_sha,
            release_sha=args.release_sha,
            release_tag_sha=args.release_tag_sha,
            private_source_sha=args.private_source_sha,
            mirror_receipt_tag_sha=args.mirror_receipt_tag_sha,
            public_source_snapshot_digest=args.public_source_snapshot_digest,
            settings_snapshot=snapshot,
        )
        print(args.receipt_output)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, urllib.error.URLError) as error:
        print(f"GitHub release settings verification failed: {error}", file=sys.stderr)
        sys.exit(1)
