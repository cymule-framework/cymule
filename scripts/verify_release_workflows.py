#!/usr/bin/env python3
"""Fail closed on mutable or credential-expansive public release workflows."""

from __future__ import annotations

import pathlib
import re
import sys
import tomllib


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
CI_WORKFLOW = WORKFLOWS / "ci.yml"
USES_LINE = re.compile(r"^\s*-?\s*uses:\s*(?P<value>.*)$")
FULL_SHA = re.compile(r"[0-9a-f]{40}")
RELEASE_WORKFLOWS = (
    "publish-npm-controller.yml",
    "publish-crates.yml",
    "finalize-release.yml",
)
NPM_CALLER = "publish-npm-release.yml"
NPM_CONTROLLER = "publish-npm-controller.yml"
NPM_REUSABLE_TARGET = (
    "cymule-framework/cymule/.github/workflows/publish-npm-controller.yml@main"
)
PRIVATE_CREDENTIAL_MARKERS = (
    "CYMULE_" + "SOURCE_",
    "CYMULE_" + "PUBLIC_PUSH_TOKEN",
)
JOB = re.compile(
    r"^  (?P<name>[a-zA-Z0-9_-]+):\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:|\Z)",
    re.MULTILINE | re.DOTALL,
)
SETUP_UV = "astral-sh/setup-uv@"
PINNED_UV_VERSION = "0.7.2"
GITLAB_CI = ROOT / ".gitlab-ci.yml"
PUBLIC_MIRROR_CONTROLLER = ROOT / ".gitlab/scripts/publish-public-mirror.sh"
PUBLIC_MIRROR_SCANNER = ROOT / ".gitlab/scripts/verify_public_mirror_candidate.sh"
PUBLIC_MIRROR_ARTIFACT_SCANNER = (
    ROOT / ".gitlab/scripts/scan_public_mirror_artifact.sh"
)
PINNED_GITLEAKS_VERSION_VERIFIER = (
    ROOT / ".gitlab/scripts/verify_pinned_gitleaks_version.sh"
)
PUBLIC_MIRROR_BLACK_BOX = ROOT / ".gitlab/scripts/test_public_mirror_controller.sh"
PUBLIC_SOURCE_SNAPSHOT_HELPER = (
    ROOT / ".gitlab/scripts/compute_public_source_snapshot.sh"
)
PNPM_INSTALLER = ROOT / ".gitlab/scripts/install_pinned_pnpm.sh"
SDK_ENTRYPOINT = ROOT / "scripts/verify-sdk.sh"
RELEASE_CONTRACTS = ROOT / "scripts/release_contracts.py"
RUST_TOOLCHAIN = ROOT / "rust-toolchain.toml"
SDK_TESTS = (
    ROOT / "crates/cymule-sdk/tests/cross_language.rs",
    ROOT / "sdk/typescript/test/e2e.ts",
    ROOT / "sdk/python/tests/test_e2e.py",
    ROOT / "sdk/go/cymule_test.go",
)
SDK_ENVIRONMENT = re.compile(r"CYMULE_[A-Z0-9_]+")
STABLE_VERSION_ADMISSION = (
    'export LC_ALL=C\n'
    '          [[ "$RELEASE_VERSION" =~ '
    '^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$ ]]'
)


def workflow_paths(root: pathlib.Path = WORKFLOWS) -> list[pathlib.Path]:
    """Return every GitHub-executable workflow extension."""

    return sorted((*root.glob("*.yml"), *root.glob("*.yaml")))


def verify_release_contract_selectors(text: str) -> None:
    """Require one public source for every closed release-only selector."""

    assignments = dict(
        re.findall(
            r'^([A-Z][A-Z0-9_]*) = "(cymule\.[a-z0-9.-]+/[1-9][0-9]*)"$',
            text,
            flags=re.MULTILINE,
        )
    )
    expected = {
        "FINALIZATION_STAGE_VERSION": "cymule.release-finalization-stage/3",
        "CONTROL_PLANE_RECEIPT_VERSION": (
            "cymule.github-release-control-plane-receipt/2"
        ),
        "CONTROL_PLANE_SETTINGS_VERSION": (
            "cymule.github-release-settings-snapshot/2"
        ),
        "MIRROR_RECEIPT_VERSION": "cymule.public-mirror-receipt/2",
    }
    if assignments != expected or len(re.findall(r"^[A-Z][A-Z0-9_]* = ", text, re.MULTILINE)) != 4:
        raise ValueError("release_contracts.py does not close four exact selectors")


def job_bodies(text: str) -> dict[str, str]:
    """Return the fixed top-level job bodies from one workflow."""

    marker = "\njobs:\n"
    if text.count(marker) != 1:
        raise ValueError("release workflow must contain one top-level jobs map")
    jobs_text = text.split(marker, 1)[1]
    matches = list(JOB.finditer(jobs_text))
    raw_names = re.findall(
        r"^  ([^ \t:\n][^:\n]*):(?:\n|[ \t])", jobs_text, re.MULTILINE
    )
    parsed_names = [match.group("name") for match in matches]
    if raw_names != parsed_names:
        raise ValueError("release workflow has an unsupported top-level job key")
    jobs = {match.group("name"): match.group("body") for match in matches}
    if len(jobs) != len(matches):
        raise ValueError("release workflow repeats a top-level job name")
    return jobs


def workflow_events(text: str) -> set[str]:
    """Return one workflow's explicit, block-form event names."""

    marker = "\non:\n"
    boundary = "\npermissions:"
    if text.count(marker) != 1 or boundary not in text.split(marker, 1)[1]:
        raise ValueError("release workflow must declare one closed event map")
    event_text = text.split(marker, 1)[1].split(boundary, 1)[0]
    events = re.findall(r"^  ([a-zA-Z0-9_-]+):", event_text, re.MULTILINE)
    raw_events = re.findall(
        r"^  ([^ \t:\n][^:\n]*):(?:\n|[ \t])", event_text, re.MULTILINE
    )
    if raw_events != events or len(events) != len(set(events)):
        raise ValueError("release workflow has an unsupported or repeated event")
    return set(events)


def verify_stable_version_admission(text: str, name: str) -> None:
    """Reject multiline or non-stable manual input before ref/output use."""

    verify_job = job_bodies(text).get("verify", "")
    if verify_job.count(STABLE_VERSION_ADMISSION) != 1:
        raise ValueError(
            f"{name} must admit one strict ASCII stable SemVer before using its input"
        )
    admission = verify_job.index(STABLE_VERSION_ADMISSION)
    prefix = verify_job[:admission]
    input_binding = "RELEASE_VERSION: ${{ inputs.version }}"
    if (
        prefix.count(input_binding) != 1
        or "$RELEASE_VERSION" in prefix
        or "inputs.version" in prefix.replace(input_binding, "", 1)
        or "GITHUB_OUTPUT" in prefix
    ):
        raise ValueError(
            f"{name} must not use release input before strict ASCII stable SemVer admission"
        )
    output = verify_job.find('} >> "$GITHUB_OUTPUT"')
    if output < 0 or admission > output:
        raise ValueError(
            f"{name} must validate stable version input before writing GITHUB_OUTPUT"
        )
    ref_uses = [
        offset
        for marker in (
            'tag="v$RELEASE_VERSION"',
            'refs/tags/v$RELEASE_VERSION',
        )
        if (offset := verify_job.find(marker)) >= 0
    ]
    if not ref_uses or admission > min(ref_uses):
        raise ValueError(
            f"{name} must validate stable version input before constructing a Git ref"
        )


def job_permissions(body: str) -> dict[str, str]:
    """Parse one job's closed top-level permission map."""

    matches = re.findall(
        r"^    permissions:\n(?P<body>(?:^      [a-z-]+: [a-z]+\n)+)",
        body,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise ValueError("release job must declare one explicit permission map")
    permissions: dict[str, str] = {}
    for line in matches[0].splitlines():
        name, value = line.strip().split(": ", 1)
        if name in permissions:
            raise ValueError(f"release job repeats permission {name}")
        permissions[name] = value
    return permissions


def verify_remote_tag_readback(
    body: str, workflow: str, *, tag_object: bool = False
) -> None:
    """Require the terminal authority to re-read the annotated tag target."""

    if tag_object:
        block = (
            'remote_refs=$(git ls-remote origin "refs/tags/$RELEASE_TAG" '
            '"refs/tags/$RELEASE_TAG^{}")\n'
            "          test \"$(printf '%s\\n' \"$remote_refs\" | grep -c .)\" -eq 2\n"
            "          remote_tag_sha=$(printf '%s\\n' \"$remote_refs\" | awk -v "
            'ref="refs/tags/$RELEASE_TAG" \'$2 == ref {print $1}\')\n'
            "          remote_release_sha=$(printf '%s\\n' \"$remote_refs\" | awk -v "
            'ref="refs/tags/$RELEASE_TAG^{}" \'$2 == ref {print $1}\')\n'
            '          test "$remote_tag_sha" = "$RELEASE_TAG_SHA"\n'
            '          test "$remote_release_sha" = "$RELEASE_SHA"\n'
        )
        required_environment = "RELEASE_TAG_SHA:"
    else:
        block = (
            'remote_release_sha=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" '
            "| cut -f1)\n"
            '          test -n "$remote_release_sha"\n'
            '          test "$remote_release_sha" = "$RELEASE_SHA"\n'
        )
        required_environment = "RELEASE_TAG:"
    if (
        "RELEASE_TAG:" not in body
        or required_environment not in body
        or body.count(block) != 1
    ):
        raise ValueError(
            f"{workflow} terminal job does not re-read the exact annotated tag"
        )


def verify_setup_uv_pins(name: str, text: str) -> None:
    """Require every setup-uv step to install the repository's exact uv release."""

    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not line.lstrip().startswith(f"- uses: {SETUP_UV}"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        block: list[str] = []
        for following in lines[index + 1 :]:
            following_indent = len(following) - len(following.lstrip(" "))
            if following_indent == indent and following.lstrip().startswith("- "):
                break
            block.append(following)
        with_line = " " * (indent + 2) + "with:"
        version_line = " " * (indent + 4) + f"version: {PINNED_UV_VERSION}"
        if block.count(with_line) != 1 or block.count(version_line) != 1:
            raise ValueError(
                f"{name} setup-uv step must pin uv {PINNED_UV_VERSION} exactly"
            )


def uses_values(name: str, text: str) -> list[str]:
    """Parse every uses scalar in one canonical, single-line form."""

    values: list[str] = []
    for line in text.splitlines():
        match = USES_LINE.fullmatch(line)
        if match is None:
            continue
        value = re.sub(r"\s+#.*$", "", match.group("value")).strip()
        if not value or any(character.isspace() for character in value):
            raise ValueError(f"{name} has unsupported uses syntax")
        values.append(value)
    return values


def action_names(name: str, text: str) -> list[str]:
    """Return every external or local action identity without its revision."""

    return [value.rpartition("@")[0] or value for value in uses_values(name, text)]


def verify_action_pins(name: str, text: str) -> None:
    """Accept only canonical local uses or one bare external revision."""

    for value in uses_values(name, text):
        if value.startswith(("./", "$/")):
            continue
        action, separator, revision = value.rpartition("@")
        if (
            not separator
            or re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)+", action) is None
            or not revision
        ):
            raise ValueError(f"{name} has unsupported uses syntax")
        if name == NPM_CALLER and value == NPM_REUSABLE_TARGET:
            continue
        if FULL_SHA.fullmatch(revision) is None:
            raise ValueError(f"{name} uses mutable action {value}")


def verify_npm_release_ref_authority(text: str) -> None:
    """Require new npm publication on exact main and recovery on its exact tag."""

    required = (
        "workflow_call:",
        "caller_generation:",
        'test "$CALLER_GENERATION" = cymule.npm-release-caller/1',
        'test "$GITHUB_WORKFLOW_REF" = "$GITHUB_REPOSITORY/.github/workflows/publish-npm-release.yml@$GITHUB_REF"',
        'test "$GITHUB_WORKFLOW_SHA" = "$GITHUB_SHA"',
        'CONTROLLER_SHA: ${{ job.workflow_sha }}',
        'test "$CONTROLLER_SHA" = "$current_main"',
        'git diff --quiet "$CONTROLLER_SHA" -- .github/workflows/publish-npm-release.yml',
        'case "$GITHUB_REF" in',
        "refs/heads/main)",
        'test "$GITHUB_SHA" = "$current_main"',
        'if test "$tag_exists" = true && test "$release_sha" != "$current_main"; then',
        'refs/tags/"$tag")',
        'test "$tag_exists" = true',
        'test "$GITHUB_SHA" = "$release_sha"',
        'git merge-base --is-ancestor "$release_sha" "$current_main"',
        "historical $tag recovery must be dispatched from refs/tags/$tag",
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(
            f"{NPM_CONTROLLER} does not bind the new caller, current controller, and exact source ref: {missing}"
        )


def verify_npm_caller_boundary(caller: str, controller: str) -> None:
    """Keep the trusted npm caller inert and resolve only the current controller."""

    if workflow_events(caller) != {"workflow_dispatch"}:
        raise ValueError(f"{NPM_CALLER} must be only the manual trusted caller")
    if caller.count("permissions:\n  contents: read\n  id-token: write\n") != 1:
        raise ValueError(f"{NPM_CALLER} must grant the closed parent OIDC envelope")
    jobs = job_bodies(caller)
    if set(jobs) != {"release"}:
        raise ValueError(f"{NPM_CALLER} must contain one inert reusable-workflow call")
    release = jobs["release"]
    if job_permissions(release) != {"contents": "read", "id-token": "write"}:
        raise ValueError(
            f"{NPM_CALLER} call must grant the reusable workflow's exact "
            "read-only contents and OIDC ceiling"
        )
    required = (
        f"uses: {NPM_REUSABLE_TARGET}",
        "version: ${{ inputs.version }}",
        "caller_generation: cymule.npm-release-caller/1",
    )
    missing = [fragment for fragment in required if release.count(fragment) != 1]
    if missing:
        raise ValueError(f"{NPM_CALLER} does not call the one main controller: {missing}")
    if any(
        fragment in release
        for fragment in ("runs-on:", "steps:", "run:", "environment:", "secrets:")
    ):
        raise ValueError(f"{NPM_CALLER} may not execute or publish directly")
    expected_release = f"""    permissions:
      contents: read
      id-token: write
    uses: {NPM_REUSABLE_TARGET}
    with:
      version: ${{{{ inputs.version }}}}
      caller_generation: cymule.npm-release-caller/1
"""
    if release != expected_release:
        raise ValueError(f"{NPM_CALLER} must remain one closed inert call envelope")
    if workflow_events(controller) != {"workflow_call"}:
        raise ValueError(f"{NPM_CONTROLLER} must be reusable and not directly dispatchable")
    if "cymule.npm-release-caller/1" not in controller:
        raise ValueError(f"{NPM_CONTROLLER} does not reject unsupported caller generations")
    concurrency_blocks = re.findall(
        r"^concurrency:\n(?P<body>(?:^  [^\n]+\n)+)",
        controller,
        flags=re.MULTILINE,
    )
    if concurrency_blocks != [
        "  group: publish-npm-stable\n  cancel-in-progress: false\n"
    ]:
        raise ValueError(
            f"{NPM_CONTROLLER} must serialize every stable version with one closed "
            "non-cancelling concurrency group"
        )


def verify_npm_publish_boundary(text: str) -> None:
    """Keep tag mutation and npm OIDC publication as separate exact authorities."""

    jobs = job_bodies(text)
    expected = {
        "verify",
        "stage",
        "close",
        "preflight-registry",
        "tag",
        "publish",
        "verify-published",
    }
    if set(jobs) != expected:
        raise ValueError(
            f"{NPM_CONTROLLER} has an open or incomplete job set: "
            f"{sorted(set(jobs) ^ expected)}"
        )
    provenance_toolchain = (
        "node-version: 26.7.0",
        'test "$(node --version)" = v26.7.0',
        'test "$(npm --version)" = 11.19.0',
    )
    for name in ("preflight-registry", "tag", "publish", "verify-published"):
        missing = [
            fragment for fragment in provenance_toolchain if jobs[name].count(fragment) != 1
        ]
        if missing:
            raise ValueError(
                f"{NPM_CONTROLLER} {name} does not pin the provenance verifier toolchain: {missing}"
            )
    preflight = jobs["preflight-registry"]
    if (
        preflight.count("python3 scripts/npm_release.py publication-admission") != 1
        or preflight.count("python3 scripts/npm_release.py tag-creation-admission") != 1
        or "TAG_EXISTS: ${{ needs.verify.outputs.tag_exists }}" not in preflight
        or "python3 scripts/npm_release.py registry-status" in preflight
    ):
        raise ValueError(
            f"{NPM_CONTROLLER} must close package-wide monotonic state before tag creation"
        )
    for name in expected - {"tag", "publish"}:
        if job_permissions(jobs[name]) != {"contents": "read"}:
            raise ValueError(f"{NPM_CONTROLLER} {name} must remain credential-free")
        if "environment:" in jobs[name]:
            raise ValueError(
                f"{NPM_CONTROLLER} {name} may not use a protected environment"
            )
    if job_permissions(jobs["tag"]) != {"contents": "read"}:
        raise ValueError(
            f"{NPM_CONTROLLER} tag job must use only its narrow GitHub App writer"
        )
    if job_permissions(jobs["publish"]) != {
        "contents": "read",
        "id-token": "write",
    }:
        raise ValueError(f"{NPM_CONTROLLER} terminal publisher has broader authority")

    tag = jobs["tag"]
    publish = jobs["publish"]
    if tag.count("environment: npm") != 1:
        raise ValueError(f"{NPM_CONTROLLER} tag mutation must use the npm environment")
    if publish.count("environment: npm") != 1:
        raise ValueError(f"{NPM_CONTROLLER} publisher must use the npm environment")
    if action_names(NPM_CONTROLLER, tag) != [
        "actions/checkout",
        "actions/checkout",
        "actions/download-artifact",
        "actions/download-artifact",
        "actions/setup-node",
        "actions/create-github-app-token",
    ]:
        raise ValueError(f"{NPM_CONTROLLER} tag writer has an unexpected action")
    if action_names(NPM_CONTROLLER, publish) != [
        "actions/checkout",
        "actions/checkout",
        "actions/download-artifact",
        "actions/setup-node",
    ]:
        raise ValueError(f"{NPM_CONTROLLER} npm publisher has an unexpected action")
    for name in ("preflight-registry", "verify-published"):
        if action_names(NPM_CONTROLLER, jobs[name]) != [
            "actions/checkout",
            "actions/checkout",
            "actions/download-artifact",
            "actions/setup-node",
        ]:
            raise ValueError(
                f"{NPM_CONTROLLER} {name} verifier has an unexpected action"
            )
    controller_checkout = (
        "repository: ${{ job.workflow_repository }}\n"
        "          ref: ${{ job.workflow_sha }}"
    )
    payload_checkout = (
        "ref: ${{ needs.verify.outputs.release_sha }}\n"
        "          path: release-payload"
    )
    isolated_jobs = {
        name: jobs[name]
        for name in ("preflight-registry", "tag", "publish", "verify-published")
    }
    for name, body in isolated_jobs.items():
        if body.count(controller_checkout) != 1 or body.count(payload_checkout) != 1:
            raise ValueError(
                f"{NPM_CONTROLLER} {name} must isolate current controller and tag payload"
            )
        if any(
            forbidden in body
            for forbidden in (
                "python3 release-payload/",
                'python3 "$CYMULE_RELEASE_WORKSPACE/',
                "release-payload/scripts/",
                '$CYMULE_RELEASE_WORKSPACE/scripts/',
                'cd "$CYMULE_RELEASE_WORKSPACE"',
                "working-directory: release-payload",
            )
        ):
            raise ValueError(f"{NPM_CONTROLLER} {name} executes tag-carried code")
    controller_integrity = (
        'git diff --quiet "$CONTROLLER_SHA" -- '
        "scripts/npm_release.py scripts/version_domains.py"
    )
    for name, command in (
        ("preflight-registry", "python3 scripts/npm_release.py publication-admission"),
        ("verify-published", "python3 scripts/npm_release.py verify-registry"),
    ):
        body = jobs[name]
        required = (
            "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
            'test "$RESOLVED_CONTROLLER_SHA" = "$CONTROLLER_SHA"',
            'test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"',
            'test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"',
            controller_integrity,
            command,
        )
        missing = [fragment for fragment in required if body.count(fragment) != 1]
        if missing:
            raise ValueError(
                f"{NPM_CONTROLLER} {name} does not authenticate the current "
                f"verifier and tag payload: {missing}"
            )
    if (
        jobs["preflight-registry"].count(
            'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"'
        )
        != 1
    ):
        raise ValueError(f"{NPM_CONTROLLER} tag admission is not current-main bound")
    expected_tag_run = """        run: |
          test "$RESOLVED_CONTROLLER_SHA" = "$CONTROLLER_SHA"
          test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"
          git diff --quiet "$CONTROLLER_SHA" -- scripts/npm_release.py scripts/version_domains.py
          git fetch --no-tags origin main
          current_main=$(git rev-parse refs/remotes/origin/main)
          test "$current_main" = "$CONTROLLER_SHA"
          test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"
          test "$(node --version)" = v26.7.0
          test "$(npm --version)" = 11.19.0
          remote_refs=$(git ls-remote origin \\
            "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}")
          remote_tag_sha=$(printf '%s\\n' "$remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {print $1}')
          remote_release_sha=$(printf '%s\\n' "$remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG^{}" '$2 == ref {print $1}')
          if test -n "$remote_tag_sha" || test -n "$remote_release_sha"; then
            [[ "$remote_tag_sha" =~ ^[0-9a-f]{40}$ ]]
            test "$remote_release_sha" = "$RELEASE_SHA"
            expected_tag_sha=$remote_tag_sha
            python3 scripts/npm_release.py publication-admission \\
              --manifest "$RUNNER_TEMP/npm-stage-cymule/manifest.json" \\
              --release-sha "$RELEASE_SHA"
            python3 scripts/npm_release.py publication-admission \\
              --manifest "$RUNNER_TEMP/npm-stage-cymule-sdk/manifest.json" \\
              --release-sha "$RELEASE_SHA"
          else
            test "$current_main" = "$RELEASE_SHA"
            python3 scripts/npm_release.py tag-creation-admission \\
              --manifest "$RUNNER_TEMP/npm-stage-cymule/manifest.json" \\
              --release-sha "$RELEASE_SHA"
            python3 scripts/npm_release.py tag-creation-admission \\
              --manifest "$RUNNER_TEMP/npm-stage-cymule-sdk/manifest.json" \\
              --release-sha "$RELEASE_SHA"
            [[ "$TAG_APP_ID" =~ ^[1-9][0-9]*$ ]]
            test -n "$TAG_APP_SLUG"
            test -n "$TAG_PUSH_TOKEN"
            bot_user_id=$(GITHUB_APP_TOKEN="$TAG_PUSH_TOKEN" \\
              python3 scripts/npm_release.py github-app-bot-user-id \\
                --app-slug "$TAG_APP_SLUG" \\
                --app-id "$TAG_APP_ID")
            [[ "$bot_user_id" =~ ^[1-9][0-9]*$ ]]
            git -C "$CYMULE_RELEASE_WORKSPACE" config user.name "${TAG_APP_SLUG}[bot]"
            git -C "$CYMULE_RELEASE_WORKSPACE" config user.email "$bot_user_id+${TAG_APP_SLUG}[bot]@users.noreply.github.com"
            git -C "$CYMULE_RELEASE_WORKSPACE" tag -a "$RELEASE_TAG" -m "Cymule $RELEASE_VERSION"
            expected_tag_sha=$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse "refs/tags/$RELEASE_TAG")
            [[ "$expected_tag_sha" =~ ^[0-9a-f]{40}$ ]]
            test "$expected_tag_sha" != "$RELEASE_SHA"
            remote_before_refs=$(git ls-remote origin \\
              "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}")
            remote_before_tag_sha=$(printf '%s\\n' "$remote_before_refs" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {print $1}')
            remote_before_release_sha=$(printf '%s\\n' "$remote_before_refs" | awk -v ref="refs/tags/$RELEASE_TAG^{}" '$2 == ref {print $1}')
            if test -n "$remote_before_tag_sha" || test -n "$remote_before_release_sha"; then
              test "$remote_before_tag_sha" = "$expected_tag_sha"
              test "$remote_before_release_sha" = "$RELEASE_SHA"
            else
              git fetch --no-tags origin main
              current_main=$(git rev-parse refs/remotes/origin/main)
              test "$current_main" = "$CONTROLLER_SHA"
              test "$current_main" = "$RELEASE_SHA"
              authorization=$(printf 'x-access-token:%s' "$TAG_PUSH_TOKEN" | base64 | tr -d '\\n')
              if ! git -C "$CYMULE_RELEASE_WORKSPACE" \\
                -c "http.extraHeader=Authorization: Basic $authorization" \\
                push "https://github.com/$GITHUB_REPOSITORY.git" "refs/tags/$RELEASE_TAG"; then
                echo "tag push response unavailable; resolving by exact readback" >&2
              fi
            fi
          fi
          final_remote_refs=$(git ls-remote origin \\
            "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}")
          final_remote_tag_sha=$(printf '%s\\n' "$final_remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {print $1}')
          final_remote_release_sha=$(printf '%s\\n' "$final_remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG^{}" '$2 == ref {print $1}')
          test "$final_remote_tag_sha" = "$expected_tag_sha"
          test "$final_remote_release_sha" = "$RELEASE_SHA"

"""
    app_authority = (
        "if: needs.verify.outputs.tag_exists == 'false'",
        "uses: actions/create-github-app-token@"
        "bcd2ba49218906704ab6c1aa796996da409d3eb1",
        "client-id: ${{ vars.CYMULE_RELEASE_TAG_APP_CLIENT_ID }}",
        "private-key: ${{ secrets.CYMULE_RELEASE_TAG_APP_PRIVATE_KEY }}",
        "owner: ${{ github.repository_owner }}",
        "repositories: ${{ github.event.repository.name }}",
        "permission-contents: write",
        "TAG_PUSH_TOKEN: ${{ steps.tag-token.outputs.token }}",
        'python3 scripts/npm_release.py github-app-bot-user-id',
        'git -C "$CYMULE_RELEASE_WORKSPACE" config user.email '
        '"$bot_user_id+${TAG_APP_SLUG}[bot]@users.noreply.github.com"',
    )
    if any(tag.count(fragment) != 1 for fragment in app_authority) or any(
        forbidden in tag
        for forbidden in ("GH_TOKEN:", "${{ github.token }}", "app-id:")
    ):
        raise ValueError(
            f"{NPM_CONTROLLER} tag writer lacks one exact single-purpose App authority"
        )
    for fragment in (
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
        'test "$RESOLVED_CONTROLLER_SHA" = "$CONTROLLER_SHA"',
        'test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"',
        'test "$current_main" = "$CONTROLLER_SHA"',
        'test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"',
        controller_integrity,
        'test "$current_main" = "$RELEASE_SHA"',
    ):
        if fragment not in tag:
            raise ValueError(f"{NPM_CONTROLLER} tag writer is not current-main bound")
    tag_admission = "python3 scripts/npm_release.py publication-admission"
    tag_creation_admission = "python3 scripts/npm_release.py tag-creation-admission"
    if (
        tag.count(tag_admission) != 2
        or tag.count(tag_creation_admission) != 2
        or tag.count("name: npm-release-closed-cymule-${{ inputs.version }}") != 1
        or tag.count("name: npm-release-closed-cymule-sdk-${{ inputs.version }}") != 1
        or tag.count('$RUNNER_TEMP/npm-stage-cymule/manifest.json') != 2
        or tag.count('$RUNNER_TEMP/npm-stage-cymule-sdk/manifest.json') != 2
    ):
        raise ValueError(
            f"{NPM_CONTROLLER} tag writer lacks a fresh closed two-package registry admission"
        )
    push_marker = '              if ! git -C "$CYMULE_RELEASE_WORKSPACE" \\\n'
    final_tag_readback = (
        '          test "$final_remote_tag_sha" = "$expected_tag_sha"\n'
    )
    final_commit_readback = (
        '          test "$final_remote_release_sha" = "$RELEASE_SHA"\n'
    )
    pre_push_fence = (
        "              git fetch --no-tags origin main\n"
        "              current_main=$(git rev-parse refs/remotes/origin/main)\n"
        '              test "$current_main" = "$CONTROLLER_SHA"\n'
        '              test "$current_main" = "$RELEASE_SHA"\n'
    )
    if (
        tag.count(push_marker) != 1
        or tag.count(pre_push_fence) != 1
        or tag.count(final_tag_readback) != 1
        or tag.count(final_commit_readback) != 1
        or tag.count('            expected_tag_sha=$remote_tag_sha\n') != 1
        or tag.count(
            '            expected_tag_sha=$(git -C "$CYMULE_RELEASE_WORKSPACE" '
            'rev-parse "refs/tags/$RELEASE_TAG")\n'
        )
        != 1
        or tag.count(
            '              test "$remote_before_tag_sha" = "$expected_tag_sha"\n'
        )
        != 1
        or 'x-access-token:%s\' "$TAG_PUSH_TOKEN"' not in tag
        or tag.rindex(tag_creation_admission) >= tag.index(pre_push_fence)
        or tag.index(pre_push_fence) >= tag.index(push_marker)
        or tag.index(push_marker) >= tag.index(final_tag_readback)
        or tag.index(final_tag_readback) >= tag.index(final_commit_readback)
    ):
        raise ValueError(
            f"{NPM_CONTROLLER} tag push is not current-fenced and loss-resolved"
        )
    if tag.count(expected_tag_run) != 1 or re.search(
        r"^\s+run:", tag.replace(expected_tag_run, "", 1), flags=re.MULTILINE
    ):
        raise ValueError(
            f"{NPM_CONTROLLER} tag writer may execute only its closed tag controller"
        )

    if len(re.findall(r"^        run:", publish, flags=re.MULTILINE)) != 2:
        raise ValueError(f"{NPM_CONTROLLER} terminal publisher has extra executable steps")
    for fragment in (
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
        'test "$RESOLVED_CONTROLLER_SHA" = "$CONTROLLER_SHA"',
        'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"',
        'test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"',
        "python3 scripts/npm_release.py verify-staged",
        "python3 scripts/npm_release.py publish",
    ):
        if fragment not in publish:
            raise ValueError(f"{NPM_CONTROLLER} terminal publisher omits {fragment}")
    if re.search(r"^\s+npm publish\b", publish, flags=re.MULTILINE):
        raise ValueError(f"{NPM_CONTROLLER} bypasses the fenced npm controller")
    mutation_marker = "      - name: Publish missing version with trusted provenance\n"
    if publish.count(mutation_marker) != 1:
        raise ValueError(f"{NPM_CONTROLLER} must have one terminal mutation step")
    mutation = publish.split(mutation_marker, 1)[1]
    if publish.count(controller_integrity) != 2 or controller_integrity not in mutation:
        raise ValueError(
            f"{NPM_CONTROLLER} does not authenticate current controller bytes"
        )
    verify_remote_tag_readback(mutation, f"{NPM_CONTROLLER} mutation")


def verify_crates_controller_boundary(text: str) -> None:
    """Separate the current reviewed publisher from immutable tag payload."""

    jobs = job_bodies(text)
    expected_jobs = {
        "verify",
        "stage",
        "close",
        "executor-witness",
        "publish",
        "verify-published",
    }
    if set(jobs) != expected_jobs:
        raise ValueError(
            "publish-crates.yml has an open or incomplete job set: "
            f"{sorted(set(jobs) ^ expected_jobs)}"
        )
    for name in expected_jobs - {"publish"}:
        if job_permissions(jobs[name]) != {"contents": "read"}:
            raise ValueError(f"publish-crates.yml {name} must remain credential-free")
        if "environment:" in jobs[name]:
            raise ValueError(
                f"publish-crates.yml {name} may not use a protected environment"
            )
    verify = jobs.get("verify", "")
    executor_witness = jobs.get("executor-witness", "")
    publish = jobs.get("publish", "")
    required_verify = (
        "controller_sha: ${{ steps.identity.outputs.controller_sha }}",
        'echo "controller_sha=$current_main"\n'
        '            echo "release_sha=$release_sha"\n'
        '            echo "version=$RELEASE_VERSION"\n'
        '          } >> "$GITHUB_OUTPUT"',
    )
    required_publish = (
        "needs: [verify, close, executor-witness]",
        "ref: ${{ needs.verify.outputs.controller_sha }}",
        "ref: ${{ needs.verify.outputs.release_sha }}\n          path: release-payload",
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
        "EXECUTOR_WITNESS_SHA: ${{ needs.executor-witness.outputs.release_sha }}",
        'test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"',
        'test "$EXECUTOR_WITNESS_SHA" = "$RELEASE_SHA"',
        'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"',
        'test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"',
        "python3 scripts/crates_release.py publish",
    )
    missing = [fragment for fragment in required_verify if fragment not in verify]
    missing.extend(fragment for fragment in required_publish if fragment not in publish)
    if missing:
        raise ValueError(
            "publish-crates.yml does not separate current controller and tag payload: "
            f"{missing}"
        )
    required_executor_witness = (
        "needs: verify",
        "runs-on: macos-15",
        "permissions:\n      contents: read",
        "release_sha: ${{ steps.identity.outputs.release_sha }}",
        "ref: ${{ needs.verify.outputs.release_sha }}",
        "fetch-depth: 0",
        'test "$(git rev-parse HEAD)" = "$RELEASE_SHA"',
        'echo "release_sha=$RELEASE_SHA" >> "$GITHUB_OUTPUT"',
        "python3 scripts/test_harness.py run",
        "rust-executor-plugin",
        "--report .cache/test-harness/release-executor-macos.json",
    )
    if any(fragment not in executor_witness for fragment in required_executor_witness):
        raise ValueError(
            "publish-crates.yml lacks one exact-SHA credential-free macOS executor witness"
        )
    if action_names("publish-crates.yml executor-witness", executor_witness) != [
        "actions/checkout",
        "actions/upload-artifact",
    ]:
        raise ValueError(
            "publish-crates.yml macOS executor witness has an unexpected action"
        )
    if (
        publish.count("actions/checkout@") != 2
        or publish.count("ref: ${{ needs.verify.outputs.controller_sha }}") != 1
        or publish.count("ref: ${{ needs.verify.outputs.release_sha }}") != 1
    ):
        raise ValueError(
            "publish-crates.yml must have one current-controller checkout and one isolated tag payload checkout"
        )
    if job_permissions(publish) != {"contents": "read", "id-token": "write"}:
        raise ValueError(
            "publish-crates.yml terminal publisher has broader than registry authority"
        )
    expected_actions = [
        "actions/checkout",
        "actions/checkout",
        "actions/download-artifact",
        "rust-lang/crates-io-auth-action",
        "actions/upload-artifact",
    ]
    if action_names("publish-crates.yml", publish) != expected_actions:
        raise ValueError(
            "publish-crates.yml terminal publisher has an unexpected privileged action"
        )
    if publish.count("environment: crates-io") != 1:
        raise ValueError("publish-crates.yml publisher must use the crates-io environment")
    for forbidden in (
        "python3 release-payload/",
        'python3 "$CYMULE_RELEASE_WORKSPACE/',
        "release-payload/scripts/",
        '$CYMULE_RELEASE_WORKSPACE/scripts/',
        'cd "$CYMULE_RELEASE_WORKSPACE"',
        "working-directory: release-payload",
    ):
        if forbidden in publish:
            raise ValueError(
                "publish-crates.yml executes historical tag code inside the OIDC job"
            )
    expected_rebind = """        run: |
          test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"
          test "$EXECUTOR_WITNESS_SHA" = "$RELEASE_SHA"
          git diff --quiet "$CONTROLLER_SHA" -- scripts/crates_release.py scripts/version_domains.py
          git fetch --no-tags origin main
          test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"
          remote_release_sha=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | cut -f1)
          test -n "$remote_release_sha"
          test "$remote_release_sha" = "$RELEASE_SHA"
          test "$(git -C "$CYMULE_RELEASE_WORKSPACE" rev-parse HEAD)" = "$RELEASE_SHA"
          test "$(git -C "$CYMULE_RELEASE_WORKSPACE" describe --exact-match --tags)" = "v$RELEASE_VERSION"
          python3 scripts/crates_release.py list >/dev/null
"""
    expected_publish = """        run: |
          test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"
          git diff --quiet "$CONTROLLER_SHA" -- scripts/crates_release.py scripts/version_domains.py
          git fetch --no-tags origin main
          test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"
          remote_release_sha=$(git ls-remote origin "refs/tags/$RELEASE_TAG^{}" | cut -f1)
          test -n "$remote_release_sha"
          test "$remote_release_sha" = "$RELEASE_SHA"
          python3 scripts/crates_release.py publish --version "$RELEASE_VERSION"
"""
    remaining_runs = publish
    for exact in (expected_rebind, expected_publish):
        if remaining_runs.count(exact) != 1:
            raise ValueError(
                "publish-crates.yml terminal publisher must execute only the frozen current controller"
            )
        remaining_runs = remaining_runs.replace(exact, "", 1)
    if re.search(r"^\s+run:", remaining_runs, flags=re.MULTILINE):
        raise ValueError(
            "publish-crates.yml terminal publisher contains an additional executable step"
        )
    mutation_marker = "      - name: Publish or retain exact staged crates\n"
    if publish.count(mutation_marker) != 1:
        raise ValueError("publish-crates.yml must have one terminal mutation step")
    verify_remote_tag_readback(
        publish.split(mutation_marker, 1)[1], "publish-crates.yml mutation"
    )


def verify_finalization_controller_boundary(text: str) -> None:
    """Keep tag execution outside the sole contents-write metadata job."""

    concurrency_blocks = re.findall(
        r"^concurrency:\n(?P<body>(?:^  [^\n]+\n)+)",
        text,
        flags=re.MULTILINE,
    )
    if concurrency_blocks != [
        "  group: finalize-release-stable\n  cancel-in-progress: false\n"
    ]:
        raise ValueError(
            "finalize-release.yml must serialize every stable version with one "
            "closed non-cancelling concurrency group"
        )

    jobs = job_bodies(text)
    expected_jobs = {"verify", "freeze", "attest", "control-plane", "publish"}
    if set(jobs) != expected_jobs:
        raise ValueError(
            "finalize-release.yml has an open or incomplete job set: "
            f"{sorted(set(jobs) ^ expected_jobs)}"
        )
    privileged = [name for name, body in jobs.items() if "contents: write" in body]
    if privileged != ["publish"]:
        raise ValueError(
            "finalize-release.yml must grant contents write only to terminal publish"
        )
    verify = jobs.get("verify", "")
    freeze = jobs.get("freeze", "")
    attest = jobs.get("attest", "")
    control_plane = jobs.get("control-plane", "")
    publish = jobs.get("publish", "")
    required_verify = (
        "permissions:\n      contents: read",
        "controller_sha: ${{ steps.identity.outputs.controller_sha }}",
        "release_tag_sha: ${{ steps.identity.outputs.release_tag_sha }}",
        "private_source_sha: ${{ steps.mirror.outputs.private_source_sha }}",
        "mirror_receipt_tag_sha: ${{ steps.mirror.outputs.mirror_receipt_tag_sha }}",
        "public_source_snapshot_digest: ${{ steps.mirror.outputs.public_source_snapshot_digest }}",
        "ref: ${{ steps.identity.outputs.release_sha }}",
        "path: release-payload",
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
        'test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"',
        'test "$(git -C release-payload rev-parse HEAD)" = "$RELEASE_SHA"',
        'test "$(git cat-file -t "refs/tags/v$RELEASE_VERSION")" = tag',
        'release_tag_sha=$(git rev-parse "refs/tags/v$RELEASE_VERSION")',
        '[[ "$release_tag_sha" =~ ^[0-9a-f]{40}$ ]]',
        'test "$release_tag_sha" != "$release_sha"',
        'test "$(git -C release-payload rev-parse "refs/tags/v$RELEASE_VERSION")" = "$RELEASE_TAG_SHA"',
        'receipt_ref="refs/tags/cymule-mirror/$RELEASE_SHA"',
        "python3 scripts/finalize_release.py verify-mirror-receipt",
        '--github-output "$GITHUB_OUTPUT"',
        "scripts/version_domains.py verify",
        "scripts/version_domains.py verify-release",
        "python3 scripts/npm_release.py verify-registry",
        "python3 scripts/crates_release.py verify-registry",
        "node-version: 26.7.0",
        'test "$(node --version)" = v26.7.0',
        'test "$(npm --version)" = 11.19.0',
    )
    required_freeze = (
        "needs: verify",
        "permissions:\n      contents: read",
        "ref: ${{ needs.verify.outputs.controller_sha }}",
        "ref: ${{ needs.verify.outputs.release_sha }}",
        "path: release-payload",
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-payload",
        'test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"',
        'test "$(git -C release-payload rev-parse HEAD)" = "$RELEASE_SHA"',
        'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"',
        'RELEASE_TAG_SHA: ${{ needs.verify.outputs.release_tag_sha }}',
        'PRIVATE_SOURCE_SHA: ${{ needs.verify.outputs.private_source_sha }}',
        'MIRROR_RECEIPT_TAG_SHA: ${{ needs.verify.outputs.mirror_receipt_tag_sha }}',
        'PUBLIC_SOURCE_SNAPSHOT_DIGEST: ${{ needs.verify.outputs.public_source_snapshot_digest }}',
        'test "$remote_tag_sha" = "$RELEASE_TAG_SHA"',
        'test "$remote_release_sha" = "$RELEASE_SHA"',
        "scripts/version_domains.py verify",
        "scripts/version_domains.py verify-release",
        "scripts/version_domains.py release-notes",
        "python3 scripts/npm_release.py verify-registry",
        "--publication-output",
        "python3 scripts/crates_release.py registry-evidence",
        "scripts/version_domains.py bom",
        '--source-sha "$PRIVATE_SOURCE_SHA"',
        '--public-source-sha "$RELEASE_SHA"',
        '--controller-sha "$CONTROLLER_SHA"',
        '--publications "$evidence_dir/npm-cymule.json"',
        '--publications "$evidence_dir/npm-cymule-sdk.json"',
        '--publications "$evidence_dir/crates.json"',
        'test "$(node --version)" = v26.7.0',
        'test "$(npm --version)" = 11.19.0',
        "uv run --project sdk/python --frozen python3 scripts/finalize_release.py stage",
        '--release-tag-sha "$RELEASE_TAG_SHA"',
        '--private-source-sha "$PRIVATE_SOURCE_SHA"',
        '--mirror-receipt-tag-sha "$MIRROR_RECEIPT_TAG_SHA"',
        '--public-source-snapshot-digest "$PUBLIC_SOURCE_SNAPSHOT_DIGEST"',
        "name: release-evidence-${{ needs.verify.outputs.version }}",
        "path: ${{ runner.temp }}/release-evidence",
        "name: release-finalization-${{ needs.verify.outputs.version }}",
        "path: ${{ runner.temp }}/release-finalization",
        "actions/upload-artifact@",
    )
    required_attest = (
        "needs: [verify, freeze]",
        "environment: release-finalize",
        "attestations: write",
        "contents: read",
        "id-token: write",
        "ref: ${{ needs.verify.outputs.controller_sha }}",
        "ref: ${{ needs.verify.outputs.release_sha }}",
        "path: release-authority",
        "fetch-depth: 0",
        "actions/download-artifact@",
        "astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d",
        "version: 0.7.2",
        "actions/attest@a1948c3f048ba23858d222213b7c278aabede763",
        "uv run --project sdk/python --frozen python3 scripts/finalize_release.py verify-stage",
        "CYMULE_RELEASE_WORKSPACE: ${{ github.workspace }}/release-authority",
        'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"',
        'RELEASE_TAG_SHA: ${{ needs.verify.outputs.release_tag_sha }}',
        'PRIVATE_SOURCE_SHA: ${{ needs.verify.outputs.private_source_sha }}',
        'MIRROR_RECEIPT_TAG_SHA: ${{ needs.verify.outputs.mirror_receipt_tag_sha }}',
        'PUBLIC_SOURCE_SNAPSHOT_DIGEST: ${{ needs.verify.outputs.public_source_snapshot_digest }}',
        "python3 scripts/finalize_release.py verify-mirror-receipt",
        '--expected-tag-sha "$MIRROR_RECEIPT_TAG_SHA"',
        '--private-source-sha "$PRIVATE_SOURCE_SHA"',
        '--mirror-receipt-tag-sha "$MIRROR_RECEIPT_TAG_SHA"',
        '--public-source-snapshot-digest "$PUBLIC_SOURCE_SNAPSHOT_DIGEST"',
        "Close the attestation bundle for the projection writer",
        'ATTESTATION_BUNDLE: ${{ steps.bom-attestation.outputs.bundle-path }}',
        "name: release-attestation-${{ needs.verify.outputs.version }}",
        "path: ${{ runner.temp }}/release-attestation",
        "actions/upload-artifact@",
    )
    required_control_plane = (
        "needs: [verify, freeze, attest]",
        "environment: release-finalize",
        "permissions:\n      contents: read",
        "ref: ${{ needs.verify.outputs.controller_sha }}",
        "actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1",
        "app-id: ${{ vars.CYMULE_RELEASE_CONTROL_APP_ID }}",
        "private-key: ${{ secrets.CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY }}",
        "permission-administration: read",
        "permission-actions: read",
        "owner: ${{ github.repository_owner }}",
        "repositories: ${{ github.event.repository.name }}",
        'git diff --quiet "$CONTROLLER_SHA" -- scripts/verify_github_release_settings.py',
        'test "$(git rev-parse refs/remotes/origin/main)" = "$CONTROLLER_SHA"',
        'test "$remote_tag_sha" = "$RELEASE_TAG_SHA"',
        'test "$remote_release_sha" = "$RELEASE_SHA"',
        "python3 scripts/verify_github_release_settings.py",
        '--receipt-output "$RUNNER_TEMP/release-control-plane/receipt.json"',
        '--run-id "$GITHUB_RUN_ID"',
        '--run-attempt "$GITHUB_RUN_ATTEMPT"',
        '--controller-sha "$CONTROLLER_SHA"',
        '--release-sha "$RELEASE_SHA"',
        '--release-tag-sha "$RELEASE_TAG_SHA"',
        '--private-source-sha "$PRIVATE_SOURCE_SHA"',
        '--mirror-receipt-tag-sha "$MIRROR_RECEIPT_TAG_SHA"',
        '--public-source-snapshot-digest "$PUBLIC_SOURCE_SNAPSHOT_DIGEST"',
        "name: release-control-plane-${{ needs.verify.outputs.version }}",
        "path: ${{ runner.temp }}/release-control-plane",
    )
    required_publish = (
        "needs: [verify, freeze, attest, control-plane]",
        "environment: release-finalize",
        "actions: read",
        "contents: write",
        'controller_dir="$RUNNER_TEMP/release-controller"',
        'authority_dir="$RUNNER_TEMP/release-authority"',
        'git -C "$controller_dir" fetch --no-tags --depth=1 origin "$CONTROLLER_SHA"',
        'git -C "$authority_dir" fetch --no-tags origin "$RELEASE_SHA"',
        'git -C "$authority_dir" fetch --force origin "$receipt_ref:$receipt_ref"',
        'gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY"',
        'release-finalization-$RELEASE_VERSION',
        'release-attestation-$RELEASE_VERSION',
        'release-control-plane-$RELEASE_VERSION',
        'export CYMULE_RELEASE_WORKSPACE="$authority_dir"',
        'test "$remote_main" = "$CONTROLLER_SHA"',
        'test "$remote_tag_sha" = "$RELEASE_TAG_SHA"',
        'python3 "$controller_dir/scripts/finalize_release.py" publish',
        '--attestation-bundle "$attestation_dir/bundle.json"',
        '--control-plane-receipt "$control_plane_dir/receipt.json"',
        '--private-source-sha "$PRIVATE_SOURCE_SHA"',
        '--mirror-receipt-tag-sha "$MIRROR_RECEIPT_TAG_SHA"',
        '--public-source-snapshot-digest "$PUBLIC_SOURCE_SNAPSHOT_DIGEST"',
        '--run-id "$GITHUB_RUN_ID"',
        '--run-attempt "$GITHUB_RUN_ATTEMPT"',
    )
    missing = [fragment for fragment in required_verify if fragment not in verify]
    missing.extend(fragment for fragment in required_freeze if fragment not in freeze)
    missing.extend(fragment for fragment in required_attest if fragment not in attest)
    missing.extend(
        fragment for fragment in required_control_plane if fragment not in control_plane
    )
    missing.extend(fragment for fragment in required_publish if fragment not in publish)
    if missing:
        raise ValueError(
            "finalize-release.yml does not freeze data before its current-main controller: "
            f"{missing}"
        )
    if (
        verify.count("actions/checkout@") != 2
        or verify.count("ref: ${{ steps.identity.outputs.release_sha }}") != 1
        or len(
            re.findall(
                r"^\s+path: release-payload$", verify, flags=re.MULTILINE
            )
        )
        != 1
        or verify.count("python3 scripts/npm_release.py verify-registry") != 1
        or verify.count("python3 scripts/crates_release.py verify-registry") != 1
    ):
        raise ValueError(
            "finalize-release.yml verification must separate one current controller "
            "from one exact tag payload"
        )
    for forbidden in (
        "python3 release-payload/scripts/npm_release.py",
        "python3 release-payload/scripts/crates_release.py",
        "python3 release-payload/scripts/finalize_release.py",
        "release-payload/scripts/version_domains.py",
    ):
        if forbidden in verify:
            raise ValueError(
                "finalize-release.yml verification executes historical tag controller code"
            )
    if (
        freeze.count("actions/checkout@") != 2
        or freeze.count("ref: ${{ needs.verify.outputs.controller_sha }}") != 1
        or freeze.count("ref: ${{ needs.verify.outputs.release_sha }}") != 1
        or len(
            re.findall(
                r"^\s+path: release-payload$", freeze, flags=re.MULTILINE
            )
        )
        != 1
        or freeze.count(
            "uv run --project sdk/python --frozen python3 "
            "scripts/finalize_release.py stage"
        )
        != 1
        or freeze.count("python3 scripts/npm_release.py verify-registry") != 1
        or freeze.count("python3 scripts/crates_release.py registry-evidence") != 1
        or freeze.count("--publication-output") != 1
        or freeze.count("--publications") != 3
        or freeze.count('--controller-sha "$CONTROLLER_SHA"') != 1
        or freeze.count('--release-tag-sha "$RELEASE_TAG_SHA"') != 1
        or len(re.findall(r"^\s+run:", freeze, flags=re.MULTILINE)) != 6
        or freeze.count("actions/upload-artifact@") != 1
    ):
        raise ValueError(
            "finalize-release.yml freeze must use one current controller, one "
            "exact payload, and one closed data bundle"
        )
    verify_remote_tag_readback(freeze, "finalize-release.yml freeze", tag_object=True)
    freeze_order = tuple(
        freeze.find(marker)
        for marker in (
            "actions/download-artifact@",
            "python3 scripts/finalize_release.py verify-mirror-receipt",
            "python3 scripts/npm_release.py verify-registry",
            "python3 scripts/crates_release.py registry-evidence",
            "scripts/version_domains.py bom",
            "uv run --project sdk/python --frozen python3 "
            "scripts/finalize_release.py stage",
            "actions/upload-artifact@",
        )
    )
    if any(offset < 0 for offset in freeze_order) or freeze_order != tuple(
        sorted(freeze_order)
    ):
        raise ValueError(
            "finalize-release.yml must authenticate stages, read registries, "
            "freeze BOM/3, and upload the closed bundle in exact order"
        )
    if (
        attest.count("actions/checkout@") != 2
        or attest.count("ref: ${{ needs.verify.outputs.controller_sha }}") != 1
        or attest.count("ref: ${{ needs.verify.outputs.release_sha }}") != 1
        or attest.count("fetch-depth: 0") != 2
        or attest.count("actions/download-artifact@") != 1
        or attest.count("actions/attest@") != 1
        or attest.count("actions/upload-artifact@") != 1
        or attest.count(
            "uv run --project sdk/python --frozen python3 "
            "scripts/finalize_release.py verify-stage"
        )
        != 1
        or attest.count("python3 ") != 2
    ):
        raise ValueError(
            "finalize-release.yml attestation authority must use one current-main "
            "controller, one data-only source, and one immutable bundle"
        )
    attest_order = tuple(
        attest.find(marker)
        for marker in (
            "python3 scripts/finalize_release.py verify-mirror-receipt",
            "scripts/finalize_release.py verify-stage",
            "actions/attest@",
        )
    )
    if any(offset < 0 for offset in attest_order) or attest_order != tuple(
        sorted(attest_order)
    ):
        raise ValueError(
            "finalize-release.yml must authenticate the mirror receipt and stage "
            "before BOM attestation"
        )
    if "sparse-checkout" in attest:
        raise ValueError(
            "finalize-release.yml data-only source must contain the complete exact "
            "workspace publication authority"
        )
    if job_permissions(verify) != {"contents": "read"}:
        raise ValueError("finalize-release.yml verification must remain credential-free")
    if "environment:" in verify:
        raise ValueError("finalize-release.yml verification may not use an environment")
    if job_permissions(freeze) != {"contents": "read"}:
        raise ValueError("finalize-release.yml freeze must remain credential-free")
    if "environment:" in freeze:
        raise ValueError("finalize-release.yml freeze may not use an environment")
    if action_names("finalize-release.yml", freeze) != [
        "actions/checkout",
        "actions/checkout",
        "astral-sh/setup-uv",
        "actions/setup-node",
        "actions/download-artifact",
        "actions/upload-artifact",
    ]:
        raise ValueError(
            "finalize-release.yml freeze has an unexpected executable action"
        )
    for forbidden in (
        "working-directory: release-payload",
        "python3 release-payload/",
        "release-payload/scripts/",
        "node release-payload/",
        "pnpm ",
        "npm publish",
        "npm install",
        "npm pack",
        "npm run",
        "npm test",
        "cargo build",
        "cargo check",
        "cargo install",
        "cargo package",
        "cargo publish",
        "test_harness.py",
        "npm_release.py publish",
        "crates_release.py publish",
        "finalize_release.py publish",
    ):
        if forbidden in freeze:
            raise ValueError(
                "finalize-release.yml data-only freeze executes payload or package code"
            )
    if job_permissions(attest) != {
        "attestations": "write",
        "contents": "read",
        "id-token": "write",
    }:
        raise ValueError(
            "finalize-release.yml attestor must hold no Release mutation authority"
        )
    if action_names("finalize-release.yml", attest) != [
        "actions/checkout",
        "actions/checkout",
        "actions/download-artifact",
        "astral-sh/setup-uv",
        "actions/attest",
        "actions/upload-artifact",
    ]:
        raise ValueError(
            "finalize-release.yml attestation job has an unexpected executable action"
        )
    if attest.count("environment: release-finalize") != 1:
        raise ValueError(
            "finalize-release.yml attestor must use the release-finalize environment"
        )
    verify_remote_tag_readback(
        attest, "finalize-release.yml attestation", tag_object=True
    )
    if job_permissions(control_plane) != {"contents": "read"}:
        raise ValueError(
            "finalize-release.yml live control-plane preflight must have no write authority"
        )
    if control_plane.count("environment: release-finalize") != 1:
        raise ValueError(
            "finalize-release.yml live control-plane preflight must use the protected environment"
        )
    if action_names("finalize-release.yml", control_plane) != [
        "actions/checkout",
        "actions/create-github-app-token",
        "actions/upload-artifact",
    ]:
        raise ValueError(
            "finalize-release.yml live control-plane preflight has an unexpected action"
        )
    if any(
        forbidden in control_plane
        for forbidden in (
            "contents: write",
            "permission-administration: write",
            "permission-actions: write",
            "permission-contents: write",
            "GH_TOKEN: ${{ github.token }}",
            "${{ github.token }}",
            "finalize_release.py publish",
            "gh release create",
            "gh release upload",
            "gh release edit",
        )
    ):
        raise ValueError(
            "finalize-release.yml live control-plane preflight has mutation authority"
        )
    if (
        control_plane.count("actions/create-github-app-token@") != 1
        or control_plane.count("permission-administration: read") != 1
        or control_plane.count("permission-actions: read") != 1
        or control_plane.count("python3 scripts/verify_github_release_settings.py") != 1
        or control_plane.count("actions/upload-artifact@") != 1
        or control_plane.count("run: |") != 1
    ):
        raise ValueError(
            "finalize-release.yml live control-plane preflight is not one closed receipt producer"
        )
    control_order = tuple(
        control_plane.find(marker)
        for marker in (
            "actions/create-github-app-token@",
            'git diff --quiet "$CONTROLLER_SHA" -- scripts/verify_github_release_settings.py',
            "remote_mirror_receipt_tag_sha=",
            "python3 scripts/verify_github_release_settings.py",
            "actions/upload-artifact@",
        )
    )
    if any(offset < 0 for offset in control_order) or control_order != tuple(
        sorted(control_order)
    ):
        raise ValueError(
            "finalize-release.yml must authenticate the live-settings controller "
            "after token minting"
        )
    verify_remote_tag_readback(
        control_plane, "finalize-release.yml control plane", tag_object=True
    )
    if job_permissions(publish) != {"actions": "read", "contents": "write"}:
        raise ValueError(
            "finalize-release.yml publisher must hold only artifact read and Release authority"
        )
    if action_names("finalize-release.yml", publish):
        raise ValueError(
            "finalize-release.yml contents-write job may not execute any third-party Action"
        )
    if any(
        marker in publish
        for marker in (
            "CYMULE_RELEASE_CONTROL_APP_ID",
            "CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY",
            "permission-administration:",
            "CONTROL_PLANE_TOKEN",
        )
    ):
        raise ValueError(
            "finalize-release.yml contents-write job may not hold control-plane credentials"
        )
    if publish.count("environment: release-finalize") != 1:
        raise ValueError(
            "finalize-release.yml publisher must use the release-finalize environment"
        )
    for forbidden in (
        "rustup ",
        "pnpm ",
        "npm ",
        "cargo ",
        "setup-node@",
        "setup-uv@",
        "test_harness.py",
        "python3 scripts/version_domains.py",
        "npm_release.py",
        "crates_release.py",
        "verify-registry",
        "release-payload",
        "release-authority/scripts/",
        "python3 release-authority/",
        'git checkout --detach "$release_sha"',
    ):
        if forbidden in publish:
            raise ValueError(
                f"finalize-release.yml executes {forbidden} with contents-write authority"
            )
    expected_run = """        run: |
          controller_dir="$RUNNER_TEMP/release-controller"
          authority_dir="$RUNNER_TEMP/release-authority"
          stage_dir="$RUNNER_TEMP/release-finalization"
          attestation_dir="$RUNNER_TEMP/release-attestation"
          control_plane_dir="$RUNNER_TEMP/release-control-plane"
          repository_url="https://github.com/$GITHUB_REPOSITORY.git"

          git init "$controller_dir"
          git -C "$controller_dir" remote add origin "$repository_url"
          git -C "$controller_dir" fetch --no-tags --depth=1 origin "$CONTROLLER_SHA"
          test "$(git -C "$controller_dir" rev-parse FETCH_HEAD)" = "$CONTROLLER_SHA"
          git -C "$controller_dir" checkout --detach "$CONTROLLER_SHA"
          git init "$authority_dir"
          git -C "$authority_dir" remote add origin "$repository_url"
          git -C "$authority_dir" fetch --no-tags origin "$RELEASE_SHA"
          test "$(git -C "$authority_dir" rev-parse FETCH_HEAD)" = "$RELEASE_SHA"
          git -C "$authority_dir" checkout --detach "$RELEASE_SHA"
          receipt_ref="refs/tags/cymule-mirror/$RELEASE_SHA"
          git -C "$authority_dir" fetch --force origin "$receipt_ref:$receipt_ref"

          mkdir "$stage_dir" "$attestation_dir" "$control_plane_dir"
          gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" \\
            --name "release-finalization-$RELEASE_VERSION" --dir "$stage_dir"
          gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" \\
            --name "release-attestation-$RELEASE_VERSION" --dir "$attestation_dir"
          gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" \\
            --name "release-control-plane-$RELEASE_VERSION" --dir "$control_plane_dir"
          test -f "$attestation_dir/bundle.json"
          test -f "$control_plane_dir/receipt.json"

          export CYMULE_RELEASE_WORKSPACE="$authority_dir"
          cd "$controller_dir"
          test "$(git rev-parse HEAD)" = "$CONTROLLER_SHA"
          remote_main=$(git ls-remote origin refs/heads/main | cut -f1)
          test "$remote_main" = "$CONTROLLER_SHA"
          remote_refs=$(git ls-remote origin "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}")
          test "$(printf '%s\\n' "$remote_refs" | grep -c .)" -eq 2
          remote_tag_sha=$(printf '%s\\n' "$remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG" '$2 == ref {print $1}')
          remote_release_sha=$(printf '%s\\n' "$remote_refs" | awk -v ref="refs/tags/$RELEASE_TAG^{}" '$2 == ref {print $1}')
          test "$remote_tag_sha" = "$RELEASE_TAG_SHA"
          test "$remote_release_sha" = "$RELEASE_SHA"
          python3 "$controller_dir/scripts/finalize_release.py" publish \\
            --repository "$GITHUB_REPOSITORY" \\
            --version "$RELEASE_VERSION" \\
            --release-sha "$RELEASE_SHA" \\
            --release-tag-sha "$RELEASE_TAG_SHA" \\
            --private-source-sha "$PRIVATE_SOURCE_SHA" \\
            --mirror-receipt-tag-sha "$MIRROR_RECEIPT_TAG_SHA" \\
            --public-source-snapshot-digest "$PUBLIC_SOURCE_SNAPSHOT_DIGEST" \\
            --controller-sha "$CONTROLLER_SHA" \\
            --stage "$stage_dir" \\
            --attestation-bundle "$attestation_dir/bundle.json" \\
            --control-plane-receipt "$control_plane_dir/receipt.json" \\
            --run-id "$GITHUB_RUN_ID" \\
            --run-attempt "$GITHUB_RUN_ATTEMPT"
"""
    if publish.count(expected_run) != 1:
        raise ValueError(
            "finalize-release.yml contents-write job must execute only the frozen current controller"
        )
    if re.search(
        r"^\s+run:",
        publish.replace(expected_run, "", 1),
        flags=re.MULTILINE,
    ):
        raise ValueError(
            "finalize-release.yml contents-write job contains an additional executable step"
        )
    verify_remote_tag_readback(publish, "finalize-release.yml", tag_object=True)


def verify_private_mirror_ci(text: str) -> None:
    """Require immutable mirror construction, scanning, lint, and mutation jobs."""

    def job_body(name: str) -> str:
        matches = list(re.finditer(rf"^{re.escape(name)}:\n", text, re.MULTILINE))
        if len(matches) != 1:
            raise ValueError(f"private GitLab CI must contain one {name} job")
        start = matches[0].end()
        following = re.search(r"^[^ \t#][^:\n]*:\n", text[start:], re.MULTILINE)
        return text[start : start + following.start()] if following else text[start:]

    build_engine = job_body("build-engine")
    rust = job_body("rust")
    typescript = job_body("typescript-sdk")
    python_sdk = job_body("python-sdk-and-schemas")
    go_sdk = job_body("go-sdk")
    candidate = job_body("mirror-candidate")
    shellcheck = job_body("mirror-shellcheck")
    scanner = job_body("mirror-scanner")
    controller = job_body("mirror-controller")
    mirror = job_body("mirror-public")
    protected_rule = (
        '$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH && '
        '$CI_COMMIT_REF_PROTECTED == "true"'
    )
    python_image = (
        "python:3.13.15-bookworm@sha256:"
        "933b46a028fd786c9c3d426ebabc237e29a15912231ea8de576e95f0e4f41a4c"
    )
    scanner_image = (
        "ghcr.io/gitleaks/gitleaks:v8.24.3@sha256:"
        "e1b35e12a8c6fa8901f060459cfb6b2fc4c484d3afbe3b029733a3bbfab07055"
    )
    shellcheck_image = (
        "koalaman/shellcheck-alpine:v0.10.0@sha256:"
        "5921d946dac740cbeec2fb1c898747b6105e585130cc7f0602eec9a10f7ddb63"
    )
    rust_image = (
        "rust:1.97-bookworm@sha256:"
        "0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97"
    )
    node_image = (
        "node:26.7.0-bookworm@sha256:"
        "e929171d35b9df7773a3ec5b068e387fa109441dc90f91e6560af5d39b7e9bf1"
    )
    uv_image = (
        "ghcr.io/astral-sh/uv:0.7.2-python3.13-bookworm@sha256:"
        "56acf02763dbd3b76cb51f8b204979472de76beab67197681d0a754a0395ff91"
    )
    go_image = (
        "golang:1.26.5-bookworm@sha256:"
        "53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd"
    )
    mirror_token = "CYMULE_" + "PUBLIC_PUSH_TOKEN"
    candidate_required = (
        "  stage: verify",
        f"  image: {python_image}",
        'GIT_DEPTH: "0"',
        f'test -z "${{{mirror_token}:-}}"',
        "tests.harness.test_release_security.PublicHistoryTests",
        "./.gitlab/scripts/prepare_public_mirror_candidate.sh",
        ".cache/public-mirror/candidate.bundle",
        ".cache/public-mirror/candidate.manifest",
        protected_rule,
    )
    shellcheck_required = (
        "  stage: verify",
        f"name: {shellcheck_image}",
        'entrypoint: [""]',
        f'test -z "${{{mirror_token}:-}}"',
        'test "$(shellcheck --version | sed -n \'s/^version: //p\')" = 0.10.0',
        "shellcheck .gitlab/scripts/compute_public_source_snapshot.sh",
        "shellcheck .gitlab/scripts/publish-public-mirror.sh",
        "shellcheck .gitlab/scripts/scan_public_mirror_artifact.sh",
        "shellcheck .gitlab/scripts/verify_pinned_gitleaks_version.sh",
        "shellcheck .gitlab/scripts/install_pinned_pnpm.sh",
        "shellcheck .gitlab/scripts/prepare_public_mirror_candidate.sh",
        "shellcheck .gitlab/scripts/verify_public_mirror_candidate.sh",
        "shellcheck .gitlab/scripts/test_public_mirror_controller.sh",
        protected_rule,
    )
    scanner_required = (
        "  stage: verify",
        f"name: {scanner_image}",
        'entrypoint: [""]',
        "needs: [mirror-candidate]",
        f'test -z "${{{mirror_token}:-}}"',
        "verify_pinned_gitleaks_version.sh --oci-image /usr/bin/gitleaks",
        "./.gitlab/scripts/scan_public_mirror_artifact.sh",
        "CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=scanner "
        "./.gitlab/scripts/test_public_mirror_controller.sh",
        protected_rule,
    )
    controller_required = (
        "  stage: verify",
        f"  image: {python_image}",
        "needs: [mirror-candidate]",
        f'test -z "${{{mirror_token}:-}}"',
        'test "$(python3 --version)" = "Python 3.13.15"',
        "CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=controller "
        "./.gitlab/scripts/test_public_mirror_controller.sh",
        protected_rule,
    )
    mirror_required = (
        "  stage: mirror",
        f"  image: {python_image}",
        "dependencies: [mirror-candidate]",
        "interruptible: false",
        "resource_group: public-mirror",
        "  environment:\n    name: public-mirror",
        'test "$(python3 --version)" = "Python 3.13.15"',
        protected_rule,
        "./.gitlab/scripts/publish-public-mirror.sh",
        "when: always",
        ".cache/public-mirror/receipt.json",
    )
    missing = [fragment for fragment in candidate_required if fragment not in candidate]
    missing.extend(fragment for fragment in shellcheck_required if fragment not in shellcheck)
    missing.extend(fragment for fragment in scanner_required if fragment not in scanner)
    missing.extend(fragment for fragment in controller_required if fragment not in controller)
    missing.extend(fragment for fragment in mirror_required if fragment not in mirror)
    if missing or any(
        body.count("\n    - if:") != 1
        for body in (candidate, shellcheck, scanner, controller, mirror)
    ):
        raise ValueError(
            f"private GitLab CI omits immutable mirror closure: {missing}"
        )
    required = (
        "./scripts/verify-sdk.sh rust",
        "./scripts/verify-sdk.sh typescript",
        "./scripts/verify-sdk.sh python",
        "./scripts/verify-sdk.sh go",
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(
            f"private GitLab CI omits immutable mirror closure: {missing}"
        )
    mirror_jobs = candidate + shellcheck + scanner + controller + mirror
    for forbidden in (
        "apt-get",
        "apk add",
        "pip install",
        "curl ",
        "wget ",
        "public-mirror-requirements",
    ):
        if forbidden in mirror_jobs:
            raise ValueError(
                "private GitLab mirror jobs install or download executable bytes"
            )
    if scanner.count(scanner_image) != 1:
        raise ValueError(
            "private GitLab mirror scanner does not pin one scanner closure"
        )
    if controller.count(python_image) != 1 or mirror.count(python_image) != 1:
        raise ValueError(
            "private GitLab controller and publisher do not share the canonical rewrite toolchain"
        )
    if "  needs:" in mirror or mirror.count("  dependencies:") != 1:
        raise ValueError(
            "private GitLab mirror publisher bypasses the complete verify-stage barrier"
        )
    exact_scripts = {
        "candidate": (
            candidate,
            '  script:\n'
            '    - test -z "${' + mirror_token + ':-}"\n'
            '    - python3 -m unittest tests.harness.test_release_security.PublicHistoryTests\n'
            '    - ./.gitlab/scripts/prepare_public_mirror_candidate.sh\n',
        ),
        "shellcheck": (
            shellcheck,
            '  script:\n'
            '    - test -z "${' + mirror_token + ':-}"\n'
            '    - test "$(shellcheck --version | sed -n \'s/^version: //p\')" = 0.10.0\n'
            '    - shellcheck .gitlab/scripts/compute_public_source_snapshot.sh\n'
            '    - shellcheck .gitlab/scripts/publish-public-mirror.sh\n'
            '    - shellcheck .gitlab/scripts/scan_public_mirror_artifact.sh\n'
            '    - shellcheck .gitlab/scripts/verify_pinned_gitleaks_version.sh\n'
            '    - shellcheck .gitlab/scripts/install_pinned_pnpm.sh\n'
            '    - shellcheck .gitlab/scripts/prepare_public_mirror_candidate.sh\n'
            '    - shellcheck .gitlab/scripts/verify_public_mirror_candidate.sh\n'
            '    - shellcheck .gitlab/scripts/test_public_mirror_controller.sh\n',
        ),
        "scanner": (
            scanner,
            '  script:\n'
            '    - test -z "${' + mirror_token + ':-}"\n'
            '    - test "$(./.gitlab/scripts/verify_pinned_gitleaks_version.sh '
            '--oci-image /usr/bin/gitleaks)" = 8.24.3\n'
            '    - ./.gitlab/scripts/scan_public_mirror_artifact.sh\n'
            '    - CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=scanner '
            './.gitlab/scripts/test_public_mirror_controller.sh\n',
        ),
        "controller": (
            controller,
            '  script:\n'
            '    - test -z "${' + mirror_token + ':-}"\n'
            '    - test "$(python3 --version)" = "Python 3.13.15"\n'
            '    - CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=controller '
            './.gitlab/scripts/test_public_mirror_controller.sh\n',
        ),
        "publisher": (
            mirror,
            '  script:\n'
            '    - test "$(python3 --version)" = "Python 3.13.15"\n'
            '    - ./.gitlab/scripts/publish-public-mirror.sh\n',
        ),
    }
    for name, (body, exact_script) in exact_scripts.items():
        if body.count(exact_script) != 1 or body.count("  script:\n") != 1:
            raise ValueError(
                f"private GitLab mirror {name} job has executable commands outside its closure"
            )
        if any(
            key in body
            for key in ("  before_script:\n", "  after_script:\n", "  hooks:\n", "  services:\n")
        ):
            raise ValueError(
                f"private GitLab mirror {name} job has an executable side channel"
            )
    if text.count('CYMULE_SDK_PREBUILT: "1"') != 4:
        raise ValueError(
            "private GitLab CI must give all four SDK lanes one complete prebuilt conformance entry"
        )
    pinned_jobs = (
        ("build-engine", build_engine, rust_image),
        ("rust", rust, rust_image),
        ("typescript-sdk", typescript, node_image),
        ("python-sdk-and-schemas", python_sdk, uv_image),
        ("go-sdk", go_sdk, go_image),
    )
    for name, body, image in pinned_jobs:
        if body.count("  image:") != 1 or f"  image: {image}" not in body:
            raise ValueError(
                f"private GitLab {name} job does not pin one immutable validation image"
            )
    rust_required = (
        'RUSTUP_DIST_SERVER: "https://static.rust-lang.org"',
        'RUSTUP_UPDATE_ROOT: "https://static.rust-lang.org/rustup"',
        'test "$(rustc --version)" = "rustc 1.97.1 (8bab26f4f 2026-07-14)"',
        'test "$(cargo --version)" = "cargo 1.97.1 (c980f4866 2026-06-30)"',
        "rustup component add --toolchain 1.97.1 clippy rustfmt",
        "rustup component list --toolchain 1.97.1 --installed | grep -Eq '^clippy-'",
        "rustup component list --toolchain 1.97.1 --installed | grep -Eq '^rustfmt-'",
    )
    toolchain = tomllib.loads(RUST_TOOLCHAIN.read_text(encoding="utf-8"))["toolchain"]
    if toolchain.get("channel") != "1.97.1" or set(toolchain.get("components", [])) != {
        "clippy", "rustfmt"
    }:
        raise ValueError("private GitLab Rust toolchain differs from repository authority")
    missing_rust = [fragment for fragment in rust_required if fragment not in rust]
    if missing_rust:
        raise ValueError(
            f"private GitLab Rust validation has an unclosed component install: {missing_rust}"
        )
    if (
        'export PATH="$(./.gitlab/scripts/install_pinned_pnpm.sh):$PATH"'
        not in typescript
        or "npm install" in typescript
        or "corepack " in typescript
    ):
        raise ValueError(
            "private GitLab TypeScript validation does not use pinned pnpm bytes"
        )


def verify_private_mirror_controller(
    text: str, contracts: str | None = None
) -> None:
    """Require closed Git execution plus leased response-loss closure."""

    if contracts is None:
        contracts = RELEASE_CONTRACTS.read_text(encoding="utf-8")
    verify_release_contract_selectors(contracts)
    selectors = re.findall(
        r'^MIRROR_RECEIPT_VERSION = "(cymule\.public-mirror-receipt/[1-9][0-9]*)"$',
        contracts,
        flags=re.MULTILINE,
    )
    if len(selectors) != 1:
        raise ValueError(
            "public mirror receipt writer has no single registered public reader"
        )
    receipt_selector = selectors[0]
    if (
        text.count(f'"receipt_version":"{receipt_selector}"') != 1
        or text.count(f'"receipt_version": "{receipt_selector}"') != 1
    ):
        raise ValueError(
            "private mirror receipt selector differs from its public reader"
        )

    required = (
        "readonly PRODUCTION_PYTHON_BINARY=/usr/local/bin/python3",
        "CYMULE_PUBLIC_TEST_PYTHON_BINARY",
        "readonly GIT_BINARY=/usr/bin/git",
        "readonly ENV_BINARY=/usr/bin/env",
        "reject_hostile_git_environment",
        "done < <(compgen -e)",
        "GIT_* | HTTP_PROXY | HTTPS_PROXY | ALL_PROXY | NO_PROXY",
        'HOME="$git_home"',
        "PATH=/usr/bin:/bin",
        "GIT_CONFIG_NOSYSTEM=1",
        "GIT_CONFIG_SYSTEM=/dev/null",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_NO_REPLACE_OBJECTS=1",
        "GIT_TERMINAL_PROMPT=0",
        'authority_git config --file "$config_path" --no-includes --null --list',
        "include.* | includeif.* | url.* | http.* | credential.* | filter.*",
        'GIT_CONFIG_KEY_0="http.$url.extraHeader"',
        'GIT_CONFIG_KEY_1="http.$url.followRedirects"',
        "GIT_CONFIG_VALUE_1=false",
        "GIT_CONFIG_KEY_2=core.hooksPath",
        "GIT_CONFIG_VALUE_2=/dev/null",
        "printf '%s  %s\\n' \"$bundle_sha256\" \"$bundle_path\" | sha256sum -c -",
        'snapshot_helper="$CI_PROJECT_DIR/.gitlab/scripts/compute_public_source_snapshot.sh"',
        'history_rewriter="$CI_PROJECT_DIR/.gitlab/scripts/rewrite_public_history.py"',
        'expected_source_tip=$("$ENV_BINARY" -i',
        '"$python_binary" "$history_rewriter"',
        '--source "$source_checkout"',
        '--revision "$private_source_sha"',
        'if ! [[ "$expected_source_tip" =~ ^[0-9a-f]{40}$ ]]',
        'test "$source_tip" != "$expected_source_tip"',
        "candidate full history does not match the trusted rewrite",
        'private_source_snapshot=$("$bash_binary" "$snapshot_helper"',
        'candidate_source_snapshot=$("$bash_binary" "$snapshot_helper"',
        'if test "$manifest_source_snapshot" != "$private_source_snapshot"; then',
        'if test "$candidate_source_snapshot" != "$private_source_snapshot"; then',
        "source_snapshot=$candidate_source_snapshot",
        'receipt_tag_name="cymule-mirror/$source_tip"',
        'receipt_ref="refs/tags/$receipt_tag_name"',
        'hash-object -t tag -w "$receipt_tag_object"',
        'update-ref "$receipt_ref" "$receipt_tag_sha"',
        "push --atomic",
        '--force-with-lease="$receipt_ref:"',
        '--force-with-lease="refs/heads/main:$public_tip"',
        '"$timeout_binary" 30 "$ENV_BINARY" -i',
        "|| push_status=$?",
        'observed_tip=$(read_tip "$public_repository" refs/heads/main) || readback_status=$?',
        'observed_receipt_tag_sha=$(read_tip "$public_repository" "$receipt_ref")',
        'if test "$readback_status" -ne 0; then',
        'if test "$observed_tip" != "$source_tip"; then',
        'if test "$push_status" -eq 0; then',
        'confirmed_public_tip=$(read_tip "$public_repository" refs/heads/main)',
        'confirmed_receipt_tag_sha=$(read_tip "$public_repository" "$receipt_ref")',
        'if test "$confirmed_public_tip" != "$source_tip"; then',
        "public mirror tip moved during no-op closure",
        "exit 75",
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(
            f"private mirror controller omits closed Git or response-loss authority: {missing}"
        )
    if text.count("GIT_NO_REPLACE_OBJECTS=1") != 7:
        raise ValueError(
            "private mirror controller omits closed Git or response-loss authority: "
            "replacement refs are not disabled on every Git path"
        )
    ordered = (
        'expected_source_tip=$("$ENV_BINARY" -i',
        'if ! [[ "$expected_source_tip" =~ ^[0-9a-f]{40}$ ]]',
        'test "$source_tip" != "$expected_source_tip"',
        'private_source_snapshot=$("$bash_binary" "$snapshot_helper"',
        'candidate_source_snapshot=$("$bash_binary" "$snapshot_helper"',
        'if test "$manifest_source_snapshot" != "$private_source_snapshot"; then',
        'if test "$candidate_source_snapshot" != "$private_source_snapshot"; then',
        "source_snapshot=$candidate_source_snapshot",
        'receipt_tag_name="cymule-mirror/$source_tip"',
        'receipt_tag_sha=$(authority_git -C "$candidate_checkout"',
        'public_tip=$(read_tip "$public_repository" refs/heads/main)',
        'public_receipt_tag_sha=$(read_tip "$public_repository" "$receipt_ref")',
        "assert_current_private_tip\nif test -n \"$public_receipt_tag_sha\"",
        'confirmed_public_tip=$(read_tip "$public_repository" refs/heads/main)',
        'confirmed_receipt_tag_sha=$(read_tip "$public_repository" "$receipt_ref")',
        'if test "$confirmed_public_tip" != "$source_tip"; then',
        'if test "$confirmed_receipt_tag_sha" != "$receipt_tag_sha"; then',
        '\n  write_receipt\n  echo "public mirror and authenticated receipt already match',
        "assert_current_private_tip\nauthorization=",
        "push --atomic",
        '--force-with-lease="$receipt_ref:"',
        'observed_tip=$(read_tip "$public_repository" refs/heads/main) || readback_status=$?',
        'observed_receipt_tag_sha=$(read_tip "$public_repository" "$receipt_ref")',
        'if test "$observed_tip" != "$source_tip"; then',
        'if test "$observed_receipt_tag_sha" != "$receipt_tag_sha"; then',
    )
    positions = [text.index(fragment) for fragment in ordered]
    receipt_position = text.rindex("\nwrite_receipt\n")
    if positions != sorted(positions) or receipt_position <= positions[-1]:
        raise ValueError(
            "private mirror controller does not close no-op and write readbacks before receipt"
        )
    if re.search(r"(?m)^\s*git\s", text) or "http.extraHeader" in text:
        raise ValueError(
            "private mirror controller executes Git outside its closed authority"
        )
    for forbidden in ("apt-get", "apk add", "pip install", "curl ", "wget "):
        if forbidden in text:
            raise ValueError(
                "private mirror controller installs or downloads executable bytes"
            )
    for forbidden in (
        '"$candidate_checkout/scripts/',
        'source "$candidate_checkout',
        '. "$candidate_checkout',
        'CYMULE_RELEASE_WORKSPACE="$candidate_checkout"',
    ):
        if forbidden in text:
            raise ValueError("private mirror controller executes artifact code")


def verify_public_source_snapshot_helper(text: str) -> None:
    """Require one exact-tree, data-only implementation of snapshot generation 1."""

    required = (
        "readonly GIT_BINARY=/usr/bin/git",
        "readonly ENV_BINARY=/usr/bin/env",
        '"$ENV_BINARY" -i',
        'HOME="$git_home"',
        "GIT_CONFIG_NOSYSTEM=1",
        "GIT_CONFIG_SYSTEM=/dev/null",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_NO_REPLACE_OBJECTS=1",
        'rev-parse --verify "$revision^{commit}"',
        '.gitlab-ci.yml | .github/workflows/mirror.yml',
        "versioning/version-domains.json | .gitlab/*",
        'ls-tree -r -z --full-tree "$revision"',
        "100644 | 100755 | 120000",
        "for shift in 56 48 40 32 24 16 8 0",
        "append_field cymule.public-source-snapshot/1",
        'cat-file -s "$object_oid"',
        'cat-file blob "$object_oid"',
        'digest=$(sha256sum "$preimage_path" | awk \'{print $1}\')',
        "printf 'sha256:%s\\n' \"$digest\"",
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(
            f"public source snapshot helper omits exact-tree closure: {missing}"
        )
    for forbidden in (
        "python",
        "eval ",
        "curl ",
        "wget ",
        "git archive",
    ):
        if forbidden in text:
            raise ValueError("public source snapshot helper executes artifact code")
    if re.search(r"(?m)^\s*(?:source|\.)\s+", text):
        raise ValueError("public source snapshot helper executes artifact code")


def verify_pinned_gitleaks_version_contract(text: str) -> None:
    """Require one closed normalization contract for release and OCI binaries."""

    required = (
        "readonly PINNED_GITLEAKS_VERSION=8.24.3",
        "readonly PINNED_GITLEAKS_OCI_VERSION=v8.24.3",
        'if test "${1:-}" = --oci-image; then',
        'case "$gitleaks_binary" in',
        '"$gitleaks_binary" version > "$version_output"',
        'printf \'%s\\n\' "$PINNED_GITLEAKS_OCI_VERSION" | cmp -s - "$version_output"',
        'printf \'%s\\n\' "$PINNED_GITLEAKS_VERSION" | cmp -s - "$version_output"',
        'if test "$require_oci_image" = 1; then',
        'printf \'%s\\n\' "$PINNED_GITLEAKS_VERSION"',
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing or text.count("cmp -s -") != 2:
        raise ValueError(
            f"pinned Gitleaks version verifier is not one closed contract: {missing}"
        )
    for forbidden in ("sed ", "tr ", "grep ", "[0-9]*", "8.24.*"):
        if forbidden in text:
            raise ValueError("pinned Gitleaks version verifier accepts an open version")


def verify_public_mirror_artifact_scanner(text: str) -> None:
    """Require the no-credential scanner to bind and scan the actual artifact."""

    private_push_token = "CYMULE_PUBLIC_" + "PUSH_TOKEN"
    required = (
        f'test -z "${{{private_push_token}:-}}"',
        "readonly PRODUCTION_GITLEAKS_BINARY=/usr/bin/gitleaks",
        'gitleaks_version_options=(--oci-image)',
        "CYMULE_PUBLIC_TEST_GITLEAKS_BINARY",
        'gitleaks_version_options=()',
        'verify_pinned_gitleaks_version.sh',
        '"${gitleaks_version_options[@]}" "$gitleaks_binary"',
        "readonly GIT_BINARY=/usr/bin/git",
        '"$ENV_BINARY" -i',
        "GIT_CONFIG_NOSYSTEM=1",
        "GIT_CONFIG_SYSTEM=/dev/null",
        "GIT_CONFIG_GLOBAL=/dev/null",
        "GIT_NO_REPLACE_OBJECTS=1",
        'test "$CI_COMMIT_SHA" = "$(scan_git -C "$CI_PROJECT_DIR" rev-parse HEAD)"',
        'manifest_path="$candidate_root/candidate.manifest"',
        'bundle_path="$candidate_root/candidate.bundle"',
        'test "$private_source_sha" = "$CI_COMMIT_SHA"',
        "printf '%s  %s\\n' \"$bundle_sha256\" \"$bundle_path\" | sha256sum -c -",
        'mapfile -t bundle_heads < <(scan_git bundle list-heads "$bundle_path")',
        'test "${#bundle_heads[@]}" -eq 1',
        'test "$bundle_ref" = refs/heads/main',
        'scan_git clone --quiet --no-local --no-checkout',
        'refs/heads/main refs/remotes/origin/main',
        'private_snapshot=$(/bin/bash "$snapshot_helper"',
        'candidate_snapshot=$(/bin/bash "$snapshot_helper"',
        'test "$source_snapshot" = "$private_snapshot"',
        'test "$candidate_snapshot" = "$private_snapshot"',
        '/bin/bash "$candidate_verifier" \\\n  --repository "$candidate_repository"',
        '--revision "$public_source_sha"',
        'echo "scanned exact public mirror candidate $public_source_sha"',
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing or text.count("GIT_NO_REPLACE_OBJECTS=1") != 2:
        raise ValueError(
            f"public mirror artifact scanner omits exact candidate closure: {missing}"
        )
    ordered = (
        'test "$CI_COMMIT_SHA" = "$(scan_git -C "$CI_PROJECT_DIR" rev-parse HEAD)"',
        'mapfile -t manifest < "$manifest_path"',
        "printf '%s  %s\\n' \"$bundle_sha256\" \"$bundle_path\" | sha256sum -c -",
        'mapfile -t bundle_heads < <(scan_git bundle list-heads "$bundle_path")',
        'scan_git clone --quiet --no-local --no-checkout',
        'test "$candidate_snapshot" = "$private_snapshot"',
        '/bin/bash "$candidate_verifier" \\\n  --repository "$candidate_repository"',
    )
    if [text.index(fragment) for fragment in ordered] != sorted(
        text.index(fragment) for fragment in ordered
    ):
        raise ValueError(
            "public mirror artifact scanner does not bind the artifact before scanning"
        )
    for forbidden in (
        "apt-get",
        "apk add",
        "pip install",
        "curl ",
        "wget ",
        'source "$candidate_repository',
        '. "$candidate_repository',
    ):
        if forbidden in text:
            raise ValueError("public mirror artifact scanner executes untrusted bytes")
    if re.search(r"(?m)^\s*git\s", text):
        raise ValueError("public mirror artifact scanner bypasses its closed Git path")


def verify_private_mirror_scanner(text: str) -> None:
    """Require exact whole-history scanner coverage and live rule canaries."""

    required = (
        'verify_pinned_gitleaks_version.sh',
        'gitleaks_binary=$(command -v gitleaks)',
        "git -C \"$repository\" rev-list --reverse --topo-order \"$revision\"",
        "git -C \"$repository\" ls-tree -r -z \"$commit\"",
        ".gitlab* | .github/workflows/mirror.yml",
        "git -C \"$repository\" cat-file commit \"$commit\"",
        "readonly MAX_PUBLIC_MIRROR_BLOB_BYTES=$((8 * 1024 * 1024))",
        "readonly GITLEAKS_MAX_TARGET_MEGABYTES=9",
        'printf \'%s\' "$path" > "$pathname_root/$pathname_record"',
        "--batch-check='%(objectname) %(objecttype) %(objectsize)'",
        'test "$object_size" -gt "$MAX_PUBLIC_MIRROR_BLOB_BYTES"',
        'git -C "$repository" cat-file blob "$blob" > "$blob_record"',
        'reject_unsupported_blob_container "$blob" "$blob_record"',
        "readonly GIT_LFS_POINTER_HEADER_HEX=",
        "zip_container_present()",
        "readonly MAX_ZIP_EOCD_CANDIDATES=4096",
        "readonly MAX_ZIP_DIRECTORY_ENTRIES=4096",
        "tail -c \"$tail_size\" \"$path\"",
        "grep -aob $'PK\\005\\006'",
        'archive_base=$((eocd_offset - central_size - central_offset))',
        'test "$(read_hex_at "$path" "$cursor" 4)" != 504b0102',
        'test "$(read_hex_at "$path" "$local_start" 4)" != 504b0304',
        'if zip_container_present "$blob_record"; then',
        "28b52ffd* | 5[0-9a-f]2a4d18*",
        "213c617263683e0a* | 213c7468696e3e0a*",
        "tar_magic=$(od -An -v -tx1 -j 257 -N 5",
        "unsupported Git LFS pointer",
        "unsupported archive/container blob",
        'grep -aFrq -- "$marker" "$history_root"',
        "sort -u \"$blob_ids_path\"",
        "github-classic github-fine-grained npm aws private-key",
        "'ghp_'",
        "'github_pat_'",
        "'npm_'",
        "'AKIA'",
        "'-----BEGIN ' 'PRIVATE KEY-----'",
        "GITLEAKS_CONFIG_TOML",
        "gitleaks detect --no-git --no-banner --redact",
        "--ignore-gitleaks-allow",
        '--gitleaks-ignore-path "$empty_ignore_path"',
        ': > "$empty_ignore_path"',
        '--max-target-megabytes "$GITLEAKS_MAX_TARGET_MEGABYTES"',
        '--source "$history_root"',
        "if test \"$canary_status\" -ne 1; then",
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(
            f"private mirror scanner omits whole-history or live-canary closure: {missing}"
        )
    if (
        text.count("gitleaks detect --no-git --no-banner --redact") != 3
        or text.count("--ignore-gitleaks-allow") != 3
        or text.count('--gitleaks-ignore-path "$empty_ignore_path"') != 3
        or text.count("env -u GITLEAKS_CONFIG -u GITLEAKS_CONFIG_TOML") != 3
    ):
        raise ValueError(
            "private mirror scanner must have clean, rejection-canary, and history scans"
        )
    for forbidden in (
        "--config",
        "--baseline-path",
        ".gitleaksignore",
        "--exit-code",
        "apt-get",
        "apk add",
        "pip install",
        "curl ",
        "wget ",
        "blob-batch",
        'cat-file --batch < "$blob_ids_path"',
    ):
        if forbidden in text:
            raise ValueError("private mirror scanner weakens its immutable rule closure")


def verify_pinned_pnpm_installer(text: str) -> None:
    """Require exact downloaded pnpm bytes before any archive execution."""

    required = (
        "readonly PNPM_VERSION=11.17.0",
        "readonly PNPM_URL=https://registry.npmjs.org/pnpm/-/pnpm-11.17.0.tgz",
        "readonly PNPM_SHA512=cca3cea332ad254bb84145f966d19f4879615210346fc92c79a047f23a0d7b3cca3c3792f0076ba1f1831d277efbcf0a9119b31a9a60eca7fb3d6231f331ef72",
        "--proto '=https' --proto-redir '=https' --tlsv1.2",
        'printf \'%s  %s\\n\' "$PNPM_SHA512" "$archive" | sha512sum -c -',
        'tar --extract --gzip --file "$archive" --directory "$install_root"',
        'manifest.version !== "11.17.0"',
        'manifest.bin?.pnpm !== "bin/pnpm.mjs"',
        'test "$(node "$pnpm_entry" --version)" = "$PNPM_VERSION"',
    )
    missing = [fragment for fragment in required if fragment not in text]
    if missing:
        raise ValueError(f"pinned pnpm installer omits byte closure: {missing}")
    ordered = (
        'curl --fail --silent --show-error --location',
        'sha512sum -c -',
        'tar --extract --gzip',
        'manifest.version !== "11.17.0"',
        'node "$pnpm_entry" --version',
    )
    positions = [text.index(fragment) for fragment in ordered]
    if positions != sorted(positions) or "npm install" in text or "corepack " in text:
        raise ValueError("pinned pnpm installer executes bytes before checksum closure")


def verify_sdk_conformance_entrypoint(text: str) -> None:
    """Require the common SDK entrypoint to export every test-owned fixture."""

    required = {
        match.group(0)
        for path in SDK_TESTS
        for match in SDK_ENVIRONMENT.finditer(path.read_text(encoding="utf-8"))
    }
    required.discard("CYMULE_TEST_EFFECT_LEDGER_PATH")
    exported = {
        match.group(0)
        for line in text.splitlines()
        if line.startswith("export ")
        for match in SDK_ENVIRONMENT.finditer(line)
    }
    missing = sorted(required - exported)
    if missing:
        raise ValueError(
            f"scripts/verify-sdk.sh does not export the complete SDK conformance environment: {missing}"
        )
    for fragment in (
        "CYMULE_SDK_PREBUILT",
        'test -x "$CYMULE_BIN"',
        'test -x "$CYMULE_TEST_PLUGIN"',
        "CYMULE_RUST_SDK_CONFORMANCE_REQUIRED=1",
    ):
        if fragment not in text:
            raise ValueError(
                f"scripts/verify-sdk.sh omits prebuilt fail-closed admission {fragment}"
            )


def verify_required_ci_source_closure(text: str) -> None:
    """Require one always-planned, deterministic public-source closure lane."""

    jobs = job_bodies(text)
    plan = jobs.get("plan", "")
    source = jobs.get("version-domain", "")
    required = jobs.get("required", "")
    plan_required = (
        "version_domain: ${{ steps.plan.outputs.version_domain }}",
        '"version-domain", "workbench"',
    )
    source_required = (
        "needs: plan",
        "if: needs.plan.outputs.version_domain != ''",
        "runs-on: ubuntu-24.04",
        "version: 0.7.2",
        "python3 scripts/test_harness.py run",
        "${{ needs.plan.outputs.version_domain }}",
        "--report .cache/test-harness/version-domain.json",
        "name: test-harness-version-domain",
    )
    required_closure = (
        "      - version-domain\n",
        '"version-domain": "version_domain"',
    )
    if (
        not source
        or any(fragment not in plan for fragment in plan_required)
        or any(fragment not in source for fragment in source_required)
        or any(fragment not in required for fragment in required_closure)
        or action_names("ci.yml version-domain", source)
        != ["actions/checkout", "astral-sh/setup-uv", "actions/upload-artifact"]
        or any(
            forbidden in source
            for forbidden in (
                "permissions:",
                "environment:",
                "id-token:",
                "secrets.",
                "cargo ",
                "./scripts/verify.sh",
            )
        )
    ):
        raise ValueError(
            "ci.yml does not close every Required CI plan through one lightweight "
            "version-domain source lane"
        )


def verify_required_ci_executor_macos(text: str) -> None:
    """Require executor paths to close on the exact macOS candidate SHA."""

    jobs = job_bodies(text)
    witness = jobs.get("executor-macos", "")
    required = jobs.get("required", "")
    witness_required = (
        "needs: plan",
        "if: contains(needs.plan.outputs.rust_plugins, 'rust-executor-plugin')",
        "runs-on: macos-15",
        "permissions:\n      contents: read",
        "source_sha: ${{ steps.identity.outputs.source_sha }}",
        "ref: ${{ github.sha }}",
        "fetch-depth: 0",
        'test "$source_sha" = "$GITHUB_SHA"',
        'echo "source_sha=$source_sha" >> "$GITHUB_OUTPUT"',
        "python3 scripts/test_harness.py run",
        "rust-executor-plugin",
        "--report .cache/test-harness/executor-macos.json",
    )
    aggregator_required = (
        "      - executor-macos\n",
        "GITHUB_SOURCE_SHA: ${{ github.sha }}",
        'executor_result = needs["executor-macos"]["result"]',
        'needs["executor-macos"]["outputs"].get(',
        '!= os.environ["GITHUB_SOURCE_SHA"]',
    )
    if (
        any(fragment not in witness for fragment in witness_required)
        or any(fragment not in required for fragment in aggregator_required)
        or action_names("ci.yml executor-macos", witness)
        != ["actions/checkout", "actions/upload-artifact"]
    ):
        raise ValueError(
            "ci.yml does not close executor changes through one exact-SHA macos-15 witness"
        )


def verify_unique_release_writer(
    script_root: pathlib.Path = ROOT / "scripts",
) -> None:
    """Keep GitHub Release mutation behind one reviewed controller and job."""

    workflows = {
        path.name: path.read_text(encoding="utf-8") for path in workflow_paths()
    }
    write_all = sorted(
        name
        for name, text in workflows.items()
        if re.search(
            r"^\s*(?:permissions|'permissions'|\"permissions\")\s*:\s*"
            r"(?:write-all|'write-all'|\"write-all\")"
            r"\s*(?:#.*)?$",
            text,
            re.MULTILINE,
        )
    )
    if write_all:
        raise ValueError(
            f"GitHub Release mutation cannot coexist with write-all workflows: {write_all}"
        )
    contents_writers = sorted(
        name
        for name, text in workflows.items()
        if re.search(
            r"(?:^|[{,])\s*(?:contents|'contents'|\"contents\")\s*:\s*"
            r"(?:write|'write'|\"write\")(?=\s*(?:[,}#]|$))",
            text,
            re.MULTILINE,
        )
    )
    if contents_writers != ["finalize-release.yml"]:
        raise ValueError(
            "GitHub Release mutation must have one contents-write workflow, found "
            f"{contents_writers}"
        )
    release_tag_app_users = sorted(
        name
        for name, text in workflows.items()
        if any(
            marker in text
            for marker in (
                "CYMULE_RELEASE_TAG_APP_CLIENT_ID",
                "CYMULE_RELEASE_TAG_APP_PRIVATE_KEY",
                "CYMULE_RELEASE_TAG_APP_ID",
            )
        )
    )
    if release_tag_app_users != [NPM_CONTROLLER]:
        raise ValueError(
            "release tag App authority must have one audited workflow, found "
            f"{release_tag_app_users}"
        )
    release_control_app_users = sorted(
        name
        for name, text in workflows.items()
        if any(
            marker in text
            for marker in (
                "CYMULE_RELEASE_CONTROL_APP_ID",
                "CYMULE_RELEASE_CONTROL_APP_PRIVATE_KEY",
            )
        )
    )
    if release_control_app_users != ["finalize-release.yml"]:
        raise ValueError(
            "release control-plane App authority must have one audited workflow, found "
            f"{release_control_app_users}"
        )
    direct_mutation = re.compile(
        r"(?:\bgh\s+release\s+(?:create|edit|upload|delete)\b|"
        r"\bgh\s+api\b[^\n]*(?:POST|PATCH|DELETE)[^\n]*/releases)"
    )
    for name, text in workflows.items():
        if direct_mutation.search(text):
            raise ValueError(f"{name} bypasses the unique Release controller")
    scripts_with_release_mutation = []
    mutation_patterns = (
        re.compile(r"\bgh\s+release\s+(?:create|edit|upload|delete)\b"),
        re.compile(
            r'["\']release["\']\s*,.{0,200}?["\'](?:create|edit|upload|delete)["\']',
            re.DOTALL,
        ),
        re.compile(r"\b(?:createRelease|updateRelease|deleteRelease)\b"),
        re.compile(
            r"(?:--method\s+(?:POST|PATCH|DELETE)|"
            r"method\s*=\s*[\"\'](?:POST|PATCH|DELETE)[\"\']).{0,300}?/releases(?:/|\b)",
            re.DOTALL | re.IGNORECASE,
        ),
    )
    source_suffixes = {".js", ".mjs", ".py", ".sh", ".ts"}
    for path in sorted(script_root.rglob("*")):
        if (
            not path.is_file()
            or path.is_symlink()
            or path.suffix not in source_suffixes
        ):
            continue
        if path.name == "verify_release_workflows.py":
            continue
        text = path.read_text(encoding="utf-8")
        if any(pattern.search(text) for pattern in mutation_patterns):
            scripts_with_release_mutation.append(
                path.relative_to(script_root).as_posix()
            )
    if scripts_with_release_mutation != ["finalize_release.py"]:
        raise ValueError(
            "GitHub Release mutation must have one audited script, found "
            f"{scripts_with_release_mutation}"
        )


def verify() -> None:
    verify_release_contract_selectors(RELEASE_CONTRACTS.read_text(encoding="utf-8"))
    mirror = WORKFLOWS / "mirror.yml"
    if mirror.exists():
        raise ValueError(
            "the public workflow tree must not contain a private-source mirror"
        )
    retired_npm = WORKFLOWS / "publish-npm.yml"
    if retired_npm.exists():
        raise ValueError(
            "the old npm trusted-publisher filename must not remain dispatchable"
        )
    for path in workflow_paths():
        text = path.read_text(encoding="utf-8")
        verify_action_pins(path.name, text)
        if any(marker in text for marker in PRIVATE_CREDENTIAL_MARKERS):
            raise ValueError(f"{path.name} references private mirror credentials")
        verify_setup_uv_pins(path.name, text)
    ci_text = CI_WORKFLOW.read_text(encoding="utf-8")
    verify_required_ci_source_closure(ci_text)
    verify_required_ci_executor_macos(ci_text)
    if GITLAB_CI.exists():
        verify_private_mirror_ci(GITLAB_CI.read_text(encoding="utf-8"))
    if PUBLIC_MIRROR_CONTROLLER.exists():
        if (
            not PUBLIC_SOURCE_SNAPSHOT_HELPER.exists()
            or not PUBLIC_MIRROR_ARTIFACT_SCANNER.exists()
            or not PINNED_GITLEAKS_VERSION_VERIFIER.exists()
        ):
            raise ValueError(
                "private mirror controller omits its trusted snapshot or artifact scanner"
            )
        verify_private_mirror_controller(
            PUBLIC_MIRROR_CONTROLLER.read_text(encoding="utf-8")
        )
    if PUBLIC_SOURCE_SNAPSHOT_HELPER.exists():
        verify_public_source_snapshot_helper(
            PUBLIC_SOURCE_SNAPSHOT_HELPER.read_text(encoding="utf-8")
        )
    if PUBLIC_MIRROR_SCANNER.exists():
        verify_private_mirror_scanner(
            PUBLIC_MIRROR_SCANNER.read_text(encoding="utf-8")
        )
    if PINNED_GITLEAKS_VERSION_VERIFIER.exists():
        verify_pinned_gitleaks_version_contract(
            PINNED_GITLEAKS_VERSION_VERIFIER.read_text(encoding="utf-8")
        )
    if PUBLIC_MIRROR_ARTIFACT_SCANNER.exists():
        verify_public_mirror_artifact_scanner(
            PUBLIC_MIRROR_ARTIFACT_SCANNER.read_text(encoding="utf-8")
        )
    if PNPM_INSTALLER.exists():
        verify_pinned_pnpm_installer(PNPM_INSTALLER.read_text(encoding="utf-8"))
    verify_sdk_conformance_entrypoint(SDK_ENTRYPOINT.read_text(encoding="utf-8"))
    verify_unique_release_writer()
    npm_caller = WORKFLOWS.joinpath(NPM_CALLER).read_text(encoding="utf-8")
    npm_text = WORKFLOWS.joinpath(NPM_CONTROLLER).read_text(encoding="utf-8")
    verify_stable_version_admission(npm_text, NPM_CONTROLLER)
    verify_npm_caller_boundary(npm_caller, npm_text)
    verify_npm_release_ref_authority(npm_text)
    verify_npm_publish_boundary(npm_text)
    crates_text = WORKFLOWS.joinpath("publish-crates.yml").read_text(encoding="utf-8")
    verify_stable_version_admission(crates_text, "publish-crates.yml")
    verify_crates_controller_boundary(crates_text)
    finalize_text = WORKFLOWS.joinpath("finalize-release.yml").read_text(
        encoding="utf-8"
    )
    verify_stable_version_admission(finalize_text, "finalize-release.yml")
    verify_finalization_controller_boundary(finalize_text)
    if workflow_events(crates_text) != {"workflow_dispatch"}:
        raise ValueError("publish-crates.yml must be only manually dispatchable")
    if workflow_events(finalize_text) != {"workflow_dispatch"}:
        raise ValueError("finalize-release.yml must be only manually dispatchable")
    for name in ("publish-crates.yml", "finalize-release.yml"):
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        required = (
            'test "$GITHUB_REF" = "refs/heads/main"',
            'test "$GITHUB_SHA" = "$current_main"',
            'git merge-base --is-ancestor "$release_sha" "$current_main"',
        )
        if any(fragment not in text for fragment in required):
            raise ValueError(
                f"{name} does not bind manual control to exact public main"
            )
    for name in RELEASE_WORKFLOWS:
        text = WORKFLOWS.joinpath(name).read_text(encoding="utf-8")
        if "scripts/version_domains.py verify-release" not in text:
            raise ValueError(f"{name} does not require a closed release changelog")
    verify_finalize_bom_readback(
        finalize_text,
        ROOT.joinpath("scripts/finalize_release.py").read_text(encoding="utf-8"),
    )
    finalize = finalize_text
    release_notes_step = finalize.split("- name: Prepare release notes", 1)[-1].split(
        "\n      - ", 1
    )[0]
    if (
        "- name: Prepare release notes" not in finalize
        or "scripts/version_domains.py release-notes" not in release_notes_step
        or "awk " in release_notes_step
    ):
        raise ValueError("finalize-release.yml does not extract one exact changelog section")
    for name in (NPM_CONTROLLER, "publish-crates.yml"):
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
        expected_environment = "npm" if name == NPM_CONTROLLER else "crates-io"
        if f"environment: {expected_environment}" not in oidc_body:
            raise ValueError(f"{name} publisher omits protected terminal environment")
    npm = npm_text
    preflight = npm.find("Preflight immutable npm version before tag creation")
    tag = npm.find("Create or verify immutable release tag after registry preflight")
    if preflight < 0 or tag < 0 or preflight >= tag:
        raise ValueError(f"{NPM_CONTROLLER} must close registry state before tag mutation")
    if "needs: [verify, preflight-registry]" not in npm:
        raise ValueError(
            f"{NPM_CONTROLLER} tag authority is not gated by both registry preflights"
        )


def verify_finalize_bom_readback(text: str, controller: str | None = None) -> None:
    """Require closed draft/assets, full inventory, and monotonic Latest."""

    if "python3 scripts/finalize_release.py" not in text:
        raise ValueError("finalize-release.yml does not invoke the Release state machine")
    if controller is None:
        controller = ROOT.joinpath("scripts/finalize_release.py").read_text(
            encoding="utf-8"
        )
    required = (
        "from release_contracts import (",
        "FINALIZATION_STAGE_VERSION,",
        "MIRROR_RECEIPT_VERSION,",
        "load_mirror_receipt(",
        "version_domains.commit_source_snapshot_digest(",
        "assert_remote_release_fence(",
        '"release_tag_sha": release_tag_sha',
        "if refs[tag_ref] != release_tag_sha:",
        "release_tag_sha=frozen.release_tag_sha",
        "validate_release_inventory(",
        "release_inventory(",
        '"--paginate"',
        '"--slurp"',
        '"tagName,isDraft,isPrerelease,isLatest"',
        "highest_stable_release(",
        "validate_asset_scope(",
        "release_assets(",
        "exact_release_asset(",
        "def validate_bom_identity(",
        "version_domains.validate_release_bom(",
        "def validate_attested_bom_projection(",
        "version_domains.validate_release_bom_projection(",
        "def load_finalization_stage(",
        "frozen = _load_finalization_stage_files(",
        "validate_bom_identity(",
        "verify_bom_attestation(",
        "assert_control_plane_receipt(",
        'publish.add_argument("--control-plane-receipt", type=pathlib.Path, required=True)',
        'publish.add_argument("--private-source-sha", required=True)',
        'publish.add_argument("--mirror-receipt-tag-sha", required=True)',
        "assert_control_plane=control_plane_fence",
        '"--signer-digest"',
        '"--deny-self-hosted-runners"',
        '        mutate_release(\n            [\n                "release",\n                "create"',
        '        mutate_release(\n            [\n                "release",\n                "upload"',
        '        mutate_release(\n            [\n                "release",\n                "edit"',
        '"release",\n                "create"',
        '"--draft"',
        '"release",\n                "upload"',
        '"release",\n                "download"',
        '"release",\n                "edit"',
        '"--draft=false"',
        '"--latest=true"',
        '"--latest=false"',
        "terminal=True",
        'if not final["isImmutable"]:',
        "compare_remote_asset(",
        "release_view(",
        '    assert_attestation()\n    assert_fence()\n    assert_control_plane()\n\n\ndef main()',
    )
    missing = [fragment for fragment in required if fragment not in controller]
    if missing:
        raise ValueError(
            f"Release finalization controller omits transitions {missing}"
        )
    if (
        controller.count('"--paginate"') != 2
        or controller.count("def exact_release_asset(") != 1
        or controller.count("def verify_bom_attestation(") != 1
        or controller.count("load_mirror_receipt(") != 3
        or controller.count("version_domains.commit_source_snapshot_digest(") != 1
        or controller.count("FINALIZATION_STAGE_VERSION,") != 3
    ):
        raise ValueError(
            "Release finalization controller omits transitions: "
            "complete asset enumeration and attested exact-byte binding"
        )
    if controller.count('"release_tag_sha": release_tag_sha,') != 3:
        raise ValueError(
            "Release finalization controller omits transitions: "
            "closed tag-object binding"
        )
    main_offset = controller.rfind("\ndef main()")
    main_body = controller[main_offset:] if main_offset >= 0 else ""
    publish_order = tuple(
        main_body.find(marker)
        for marker in (
            "receipt = load_mirror_receipt(",
            "frozen = _load_finalization_stage_files(",
            "control_plane_fence()",
            "verify_bom_attestation(",
            "validate_attested_bom_projection(",
            "converge_release(",
        )
    )
    if any(offset < 0 for offset in publish_order) or publish_order != tuple(
        sorted(publish_order)
    ):
        raise ValueError(
            "Release finalization controller omits transitions: attestation must "
            "precede projection validation and every Release mutation"
        )
    if (
        controller.count("mutate_release(") != 5
        or controller.count("        assert_attestation()\n") != 1
        or controller.count("        assert_fence()\n") != 1
        or controller.count("        assert_control_plane()\n") != 1
        or controller.count("        invoke(arguments)\n") != 1
        or (
            "        assert_attestation()\n"
            "        assert_fence()\n"
            "        assert_control_plane()\n"
            "        invoke(arguments)\n"
        )
        not in controller
    ):
        raise ValueError(
            "Release finalization must validate control plane, attest, and fence "
            "every external mutation"
        )
    if '"--clobber"' in controller:
        raise ValueError("Release finalization may not replace an existing BOM")


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
