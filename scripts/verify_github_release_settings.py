#!/usr/bin/env python3
"""Verify the GitHub control-plane gates required for public releases."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_REPOSITORY = "cymule-framework/cymule"
REQUIRED_STATUS_CONTEXTS = {"Required CI"}
GITHUB_ACTIONS_INTEGRATION_ID = 15368


def github_json(repository: str, path: str, token: str) -> object:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}{path}",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "cymule-release-settings/1",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


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
    if include != [expected] or exclude != []:
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


def verify_main_bypass(ruleset: dict[str, object], integration_id: int) -> None:
    expected = [
        {
            "actor_id": integration_id,
            "actor_type": "Integration",
            "bypass_mode": "always",
        }
    ]
    observed = ruleset.get("bypass_actors", [])
    projected = [
        {
            "actor_id": actor.get("actor_id"),
            "actor_type": actor.get("actor_type"),
            "bypass_mode": actor.get("bypass_mode"),
        }
        for actor in observed
        if isinstance(actor, dict)
    ]
    if projected != expected or len(projected) != len(observed):
        raise ValueError(
            f"ruleset {ruleset.get('name')} must grant only exact mirror Integration "
            f"{integration_id}, found {projected}"
        )


def verify_no_bypass(ruleset: dict[str, object]) -> None:
    if ruleset.get("bypass_actors", []):
        raise ValueError(f"ruleset {ruleset.get('name')} must not allow tag bypass")


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


def verify_environment(repository: str, name: str, token: str) -> None:
    encoded_name = urllib.parse.quote(name, safe="")
    environment = github_json(repository, f"/environments/{encoded_name}", token)
    if not isinstance(environment, dict):
        raise ValueError(f"GitHub returned malformed environment {name}")
    policy = environment.get("deployment_branch_policy")
    if policy != {"protected_branches": True, "custom_branch_policies": False}:
        raise ValueError(
            f"environment {name} must admit only protected branches, found {policy}"
        )
    rules = environment.get("protection_rules")
    if not isinstance(rules, list):
        raise ValueError(f"environment {name} omitted protection rules")
    reviewers = next(
        (
            rule
            for rule in rules
            if isinstance(rule, dict) and rule.get("type") == "required_reviewers"
        ),
        None,
    )
    if reviewers is None or not reviewers.get("prevent_self_review", False):
        raise ValueError(
            f"environment {name} requires non-self approval before OIDC publication"
        )
    if not reviewers.get("reviewers"):
        raise ValueError(f"environment {name} has no required reviewer")


def verify(repository: str, token: str, mirror_integration_id: int) -> None:
    raw_rulesets = github_json(repository, "/rulesets?includes_parents=true", token)
    if not isinstance(raw_rulesets, list):
        raise ValueError("GitHub returned malformed repository rulesets")
    rulesets = []
    for value in raw_rulesets:
        if not isinstance(value, dict) or not isinstance(value.get("id"), int):
            raise ValueError("GitHub returned a malformed ruleset summary")
        detail = github_json(repository, f"/rulesets/{value['id']}", token)
        if not isinstance(detail, dict):
            raise ValueError(f"GitHub returned malformed ruleset {value['id']}")
        rulesets.append(detail)
    main = require_ruleset(
        rulesets,
        target="branch",
        ref="~DEFAULT_BRANCH",
        rules={"deletion", "non_fast_forward", "required_status_checks"},
    )
    release_tags = require_ruleset(
        rulesets,
        target="tag",
        ref="refs/tags/v*",
        rules={"deletion", "non_fast_forward"},
    )
    verify_exact_ref_scope(main, "~DEFAULT_BRANCH")
    verify_exact_ref_scope(release_tags, "refs/tags/v*")
    verify_required_status_checks(main)
    verify_main_bypass(main, mirror_integration_id)
    verify_no_bypass(release_tags)
    permissions = github_json(repository, "/actions/permissions/workflow", token)
    if permissions != {
        "default_workflow_permissions": "read",
        "can_approve_pull_request_reviews": False,
    }:
        raise ValueError(f"unsafe default Actions permissions: {permissions}")
    verify_environment(repository, "npm", token)
    verify_environment(repository, "crates-io", token)
    verify_environment(repository, "release-finalize", token)
    print(f"verified GitHub release control plane for {repository}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY)
    parser.add_argument(
        "--mirror-integration-id",
        type=int,
        default=os.environ.get("CYMULE_GITHUB_MIRROR_INTEGRATION_ID"),
        required=os.environ.get("CYMULE_GITHUB_MIRROR_INTEGRATION_ID") is None,
    )
    args = parser.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError(
            "GITHUB_TOKEN with repository administration read access is required"
        )
    if args.mirror_integration_id <= 0:
        raise ValueError("mirror Integration ID must be positive")
    verify(args.repository, token, args.mirror_integration_id)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, urllib.error.URLError) as error:
        print(f"GitHub release settings verification failed: {error}", file=sys.stderr)
        sys.exit(1)
