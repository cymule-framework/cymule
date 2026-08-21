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
    raise ValueError(
        f"no active {target} ruleset protects {ref} with {sorted(rules)}"
    )


def reject_admin_bypass(ruleset: dict[str, object]) -> None:
    for actor in ruleset.get("bypass_actors", []):
        if not isinstance(actor, dict):
            continue
        actor_type = actor.get("actor_type")
        if actor_type in {"OrganizationAdmin", "RepositoryRole"}:
            raise ValueError(
                f"ruleset {ruleset.get('name')} grants broad {actor_type} bypass"
            )


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


def verify(repository: str, token: str) -> None:
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
    reject_admin_bypass(main)
    reject_admin_bypass(release_tags)
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
    args = parser.parse_args()
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError("GITHUB_TOKEN with repository administration read access is required")
    verify(args.repository, token)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, urllib.error.URLError) as error:
        print(f"GitHub release settings verification failed: {error}", file=sys.stderr)
        sys.exit(1)
