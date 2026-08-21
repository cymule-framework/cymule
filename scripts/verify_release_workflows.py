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


def verify() -> None:
    mirror = WORKFLOWS / "mirror.yml"
    if mirror.exists():
        raise ValueError("the public workflow tree must not contain a private-source mirror")
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        for action, revision in ACTION.findall(text):
            if FULL_SHA.fullmatch(revision) is None:
                raise ValueError(f"{path.name} uses mutable action {action}@{revision}")
        if "RETIRED_PRIVATE_SOURCE_" in text or "RETIRED_PRIVATE_PUSH_TOKEN" in text:
            raise ValueError(f"{path.name} references private mirror credentials")
    for name in RELEASE_WORKFLOWS:
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        required = (
            'test "$GITHUB_REF" = "refs/heads/main"',
            'test "$GITHUB_SHA" = "$current_main"',
        )
        if any(fragment not in text for fragment in required):
            raise ValueError(f"{name} does not bind manual control to exact public main")
    for name in ("publish-npm.yml", "publish-crates.yml"):
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        if "id-token: write" not in text:
            raise ValueError(f"{name} omits trusted-publisher OIDC")
        oidc_suffix = text.split("id-token: write", maxsplit=1)[1]
        for forbidden in ("./scripts/verify.sh", "verify-soak", "npm pack", "cargo test"):
            if forbidden in oidc_suffix:
                raise ValueError(
                    f"{name} performs {forbidden} after granting the OIDC publication token"
                )


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
