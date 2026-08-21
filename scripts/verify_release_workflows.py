#!/usr/bin/env python3
"""Fail closed on mutable or credential-expansive public release workflows."""

from __future__ import annotations

import pathlib
import re
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTION = re.compile(r"^\s*-?\s*uses:\s*([^\s@]+)@([^\s#]+)", re.MULTILINE)
FULL_SHA = re.compile(r"[0-9a-f]{40}")
RELEASE_WORKFLOWS = (
    "publish-npm.yml",
    "publish-crates.yml",
    "finalize-release.yml",
)
PRIVATE_CREDENTIAL_MARKERS = (
    "CYMULE_" + "SOURCE_",
    "CYMULE_" + "PUBLIC_PUSH_TOKEN",
)
JOB = re.compile(
    r"^  (?P<name>[a-zA-Z0-9_-]+):\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:|\Z)",
    re.MULTILINE | re.DOTALL,
)


def verify() -> None:
    mirror = WORKFLOWS / "mirror.yml"
    if mirror.exists():
        raise ValueError(
            "the public workflow tree must not contain a private-source mirror"
        )
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        for action, revision in ACTION.findall(text):
            if FULL_SHA.fullmatch(revision) is None:
                raise ValueError(f"{path.name} uses mutable action {action}@{revision}")
        if any(marker in text for marker in PRIVATE_CREDENTIAL_MARKERS):
            raise ValueError(f"{path.name} references private mirror credentials")
    for name in RELEASE_WORKFLOWS:
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        required = (
            'test "$GITHUB_REF" = "refs/heads/main"',
            'test "$GITHUB_SHA" = "$current_main"',
        )
        if any(fragment not in text for fragment in required):
            raise ValueError(
                f"{name} does not bind manual control to exact public main"
            )
    for name in ("publish-npm.yml", "publish-crates.yml"):
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        oidc_jobs = [
            (match.group("name"), match.group("body"))
            for match in JOB.finditer(text)
            if "id-token: write" in match.group("body")
        ]
        if len(oidc_jobs) != 1 or oidc_jobs[0][0] != "publish":
            raise ValueError(f"{name} must grant OIDC to exactly one publish job")
        oidc_body = oidc_jobs[0][1]
        for forbidden in (
            "./scripts/verify.sh",
            "verify-soak",
            "npm pack",
            "pnpm ",
            "rustup ",
            "cargo ",
            "verify-registry",
            "$RUNNER_TEMP/npm-stage/npm_release.py",
        ):
            if forbidden in oidc_body:
                raise ValueError(
                    f"{name} performs {forbidden} inside the OIDC publication job"
                )
        expected_environment = "npm" if name == "publish-npm.yml" else "crates-io"
        if f"environment: {expected_environment}" not in oidc_body:
            raise ValueError(f"{name} publisher omits protected terminal environment")


def main() -> int:
    try:
        verify()
    except ValueError as error:
        print(f"release workflow verification failed: {error}", file=sys.stderr)
        return 1
    print("verified immutable, least-privilege public release workflows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
