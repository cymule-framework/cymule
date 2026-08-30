#!/usr/bin/env python3
"""Converge one exact GitHub Release and immutable version-domain BOM."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from typing import Any

import version_domains
from release_contracts import (
    CONTROL_PLANE_RECEIPT_VERSION,
    CONTROL_PLANE_SETTINGS_VERSION,
    FINALIZATION_STAGE_VERSION,
    MIRROR_RECEIPT_VERSION,
)


Invoke = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
Fence = Callable[[], None]
FINALIZATION_MANIFEST_NAME = "release-finalization.json"
FINALIZATION_NOTES_NAME = "release-notes.md"
GIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
GH_RELEASE_NOT_FOUND = "release not found"
MIRROR_RECEIPT_TAG_PREFIX = "cymule-mirror/"
CONTROL_PLANE_RECEIPT_TTL_SECONDS = 15 * 60
GITHUB_ACTIONS_INTEGRATION_ID = 15368
POSITIVE_DECIMAL_PATTERN = re.compile(r"[1-9][0-9]*")
UTC_TIMESTAMP_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)


@dataclasses.dataclass(frozen=True)
class FinalizationStage:
    """Authenticated data consumed by the terminal Release controller."""

    tag: str
    release_tag_sha: str
    private_source_sha: str
    mirror_receipt_tag_sha: str
    public_source_snapshot_digest: str
    title: str
    notes_path: pathlib.Path
    asset_path: pathlib.Path
    asset_name: str


@dataclasses.dataclass(frozen=True)
class MirrorReceipt:
    """One immutable private-to-public source mapping carried by a Git tag."""

    private_source_sha: str
    public_source_sha: str
    public_source_snapshot_digest: str
    tag_name: str
    tag_sha: str


@dataclasses.dataclass(frozen=True)
class ReleaseInventoryRecord:
    """One Release identity from the complete REST inventory plus Latest state."""

    database_id: int
    tag: str
    draft: bool
    prerelease: bool
    is_latest: bool


@dataclasses.dataclass(frozen=True)
class ReleaseAssetRecord:
    """One immutable GitHub Release asset identity."""

    database_id: int
    name: str
    size: int
    digest: str
    state: str


def release_identity(version: str) -> tuple[str, str, str]:
    """Derive the closed public Release identity from one package version."""

    version_domains.validate_stable_release_version(version)
    return (
        f"v{version}",
        f"Cymule {version}",
        f"cymule-{version}-version-domain-bom.json",
    )


def file_identity(path: pathlib.Path) -> dict[str, object]:
    """Return one immutable file identity."""

    if not path.is_file() or path.is_symlink():
        raise ValueError(f"release input {path} is not one regular file")
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return {"size": size, "sha256": f"sha256:{digest.hexdigest()}"}


def mirror_receipt_tag_name(public_source_sha: str) -> str:
    """Return the sole public ref carrying one rewritten-source receipt."""

    if GIT_SHA_PATTERN.fullmatch(public_source_sha) is None:
        raise ValueError("public source SHA must be one exact lowercase Git commit")
    return f"{MIRROR_RECEIPT_TAG_PREFIX}{public_source_sha}"


def load_mirror_receipt(
    repository_root: pathlib.Path,
    *,
    public_source_sha: str,
    expected_tag_sha: str | None = None,
) -> MirrorReceipt:
    """Authenticate an immutable annotated-tag mapping and its source snapshot."""

    if repository_root.is_symlink() or not repository_root.is_dir():
        raise ValueError("mirror receipt repository must be one real directory")
    if GIT_SHA_PATTERN.fullmatch(public_source_sha) is None:
        raise ValueError("public source SHA must be one exact lowercase Git commit")
    if expected_tag_sha is not None and GIT_SHA_PATTERN.fullmatch(expected_tag_sha) is None:
        raise ValueError("mirror receipt tag SHA must be one exact Git object")
    resolved_public = subprocess.run(
        ["git", "rev-parse", "--verify", f"{public_source_sha}^{{commit}}"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if resolved_public != public_source_sha:
        raise ValueError("mirror receipt public source is not the exact commit")

    tag_name = mirror_receipt_tag_name(public_source_sha)
    tag_ref = f"refs/tags/{tag_name}"
    tag_sha = subprocess.run(
        ["git", "rev-parse", "--verify", tag_ref],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if GIT_SHA_PATTERN.fullmatch(tag_sha) is None or (
        expected_tag_sha is not None and tag_sha != expected_tag_sha
    ):
        raise ValueError("mirror receipt tag object belongs to another mapping")
    tag_type = subprocess.run(
        ["git", "cat-file", "-t", tag_sha],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if tag_type != "tag":
        raise ValueError("mirror receipt carrier must be one annotated Git tag")
    raw = subprocess.run(
        ["git", "cat-file", "tag", tag_sha],
        cwd=repository_root,
        check=True,
        capture_output=True,
    ).stdout
    header, separator, message = raw.partition(b"\n\n")
    headers = header.split(b"\n")
    if (
        separator != b"\n\n"
        or len(headers) != 4
        or headers[0] != f"object {public_source_sha}".encode("ascii")
        or headers[1] != b"type commit"
        or headers[2] != f"tag {tag_name}".encode("ascii")
        or re.fullmatch(
            rb"tagger Cymule Public Mirror <mirror@cymule\.dev> (?:0|[1-9][0-9]*) \+0000",
            headers[3],
        )
        is None
    ):
        raise ValueError("mirror receipt annotated-tag envelope is not exact")
    value = version_domains.load_json_bytes(
        message.removesuffix(b"\n"), label="public mirror receipt"
    )
    if not isinstance(value, dict) or set(value) != {
        "private_source_sha",
        "public_source_sha",
        "public_source_snapshot_digest",
        "receipt_version",
    }:
        raise ValueError("public mirror receipt has an open or incomplete shape")
    private_source_sha = value["private_source_sha"]
    public_source_snapshot_digest = value["public_source_snapshot_digest"]
    if (
        value["receipt_version"] != MIRROR_RECEIPT_VERSION
        or not isinstance(private_source_sha, str)
        or GIT_SHA_PATTERN.fullmatch(private_source_sha) is None
        or private_source_sha == public_source_sha
        or value["public_source_sha"] != public_source_sha
        or not isinstance(public_source_snapshot_digest, str)
        or re.fullmatch(
            r"sha256:[0-9a-f]{64}", public_source_snapshot_digest
        )
        is None
        or message != version_domains.canonical_bytes(value) + b"\n"
    ):
        raise ValueError("public mirror receipt source mapping is not exact")
    observed_snapshot = version_domains.commit_source_snapshot_digest(
        public_source_sha, root=repository_root
    )
    registry_snapshot = version_domains.load_registry(repository_root)[
        "source_generation"
    ]["source_snapshot_digest"]
    if (
        public_source_snapshot_digest != observed_snapshot
        or public_source_snapshot_digest != registry_snapshot
    ):
        raise ValueError(
            "public mirror receipt does not bind the exact source snapshot generation"
        )
    return MirrorReceipt(
        private_source_sha=private_source_sha,
        public_source_sha=public_source_sha,
        public_source_snapshot_digest=public_source_snapshot_digest,
        tag_name=tag_name,
        tag_sha=tag_sha,
    )


def _receipt_digest(value: dict[str, object]) -> str:
    digest = hashlib.sha256(version_domains.canonical_bytes(value)).hexdigest()
    return f"sha256:{digest}"


def _validate_control_plane_settings(snapshot: object) -> None:
    """Require the exact normalized settings authority emitted by live preflight."""

    if not isinstance(snapshot, dict) or set(snapshot) != {
        "snapshot_version",
        "default_branch",
        "authorities",
        "rulesets",
        "actions_permissions",
        "immutable_releases",
        "environments",
    }:
        raise ValueError("control-plane receipt settings snapshot is open or incomplete")
    if snapshot["snapshot_version"] != CONTROL_PLANE_SETTINGS_VERSION:
        raise ValueError("control-plane receipt settings snapshot has the wrong generation")
    if snapshot["default_branch"] != "main":
        raise ValueError("control-plane receipt default branch must be exactly main")
    authorities = snapshot["authorities"]
    authority_names = {
        "mirror_integration_id",
        "release_tag_integration_id",
        "npm_reviewer_team_id",
        "crates_reviewer_team_id",
        "release_reviewer_team_id",
    }
    if not isinstance(authorities, dict) or set(authorities) != authority_names:
        raise ValueError("control-plane receipt authority identities are malformed")
    if any(
        type(authorities[name]) is not int or authorities[name] <= 0
        for name in authority_names
    ):
        raise ValueError(
            "control-plane receipt authority identities must be positive integers"
        )

    expected_main = {
        "enforcement": "active",
        "target": "branch",
        "ref": "~DEFAULT_BRANCH",
        "required_status_checks": [
            {
                "context": "Required CI",
                "integration_id": GITHUB_ACTIONS_INTEGRATION_ID,
            }
        ],
        "strict_required_status_checks_policy": True,
        "bypass_actors": [
            {
                "actor_id": authorities["mirror_integration_id"],
                "actor_type": "Integration",
                "bypass_mode": "always",
            }
        ],
    }
    expected_tag_creation = {
        "enforcement": "active",
        "target": "tag",
        "ref": "refs/tags/v*",
        "rules": ["creation"],
        "bypass_actors": [
            {
                "actor_id": authorities["release_tag_integration_id"],
                "actor_type": "Integration",
                "bypass_mode": "always",
            }
        ],
    }
    expected_tag_immutable = {
        "enforcement": "active",
        "target": "tag",
        "ref": "refs/tags/v*",
        "rules": ["deletion", "update"],
        "bypass_actors": [],
    }
    expected_mirror_receipt_creation = {
        "enforcement": "active",
        "target": "tag",
        "ref": "refs/tags/cymule-mirror/*",
        "rules": ["creation"],
        "bypass_actors": [
            {
                "actor_id": authorities["mirror_integration_id"],
                "actor_type": "Integration",
                "bypass_mode": "always",
            }
        ],
    }
    expected_mirror_receipt_immutable = {
        "enforcement": "active",
        "target": "tag",
        "ref": "refs/tags/cymule-mirror/*",
        "rules": ["deletion", "update"],
        "bypass_actors": [],
    }
    expected_rulesets = {
        "main": expected_main,
        "release_tag_creation": expected_tag_creation,
        "release_tag_immutable": expected_tag_immutable,
        "mirror_receipt_creation": expected_mirror_receipt_creation,
        "mirror_receipt_immutable": expected_mirror_receipt_immutable,
    }
    if snapshot["rulesets"] != expected_rulesets:
        raise ValueError("control-plane receipt ruleset authority is not exact")
    if snapshot["actions_permissions"] != {
        "default_workflow_permissions": "read",
        "can_approve_pull_request_reviews": False,
    }:
        raise ValueError("control-plane receipt default Actions permissions are unsafe")
    if snapshot["immutable_releases"] != {
        "enabled": True,
        "enforced_by_owner": True,
    }:
        raise ValueError(
            "control-plane receipt does not prove owner-enforced immutable Releases"
        )

    def expected_environment(team_id: int, *, npm: bool) -> dict[str, object]:
        return {
            "can_admins_bypass": False,
            "deployment_branch_policy": (
                {"protected_branches": False, "custom_branch_policies": True}
                if npm
                else {"protected_branches": True, "custom_branch_policies": False}
            ),
            "required_reviewers": [{"type": "Team", "id": team_id}],
            "selected_refs": (
                [
                    {"type": "branch", "name": "main"},
                    {"type": "tag", "name": "v*"},
                ]
                if npm
                else None
            ),
        }

    expected_environments = {
        "npm": expected_environment(authorities["npm_reviewer_team_id"], npm=True),
        "crates-io": expected_environment(
            authorities["crates_reviewer_team_id"], npm=False
        ),
        "release-finalize": expected_environment(
            authorities["release_reviewer_team_id"], npm=False
        ),
    }
    if snapshot["environments"] != expected_environments:
        raise ValueError("control-plane receipt protected environments are not exact")


def _parse_receipt_time(value: object, *, field: str) -> dt.datetime:
    if not isinstance(value, str) or UTC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise ValueError(f"control-plane receipt {field} is not canonical UTC")
    try:
        return dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc
        )
    except ValueError as error:
        raise ValueError(f"control-plane receipt {field} is not a real UTC time") from error


def assert_control_plane_receipt(
    path: pathlib.Path,
    *,
    repository: str,
    run_id: str,
    run_attempt: str,
    controller_sha: str,
    release_sha: str,
    release_tag_sha: str,
    private_source_sha: str,
    mirror_receipt_tag_sha: str,
    public_source_snapshot_digest: str,
    now: dt.datetime | None = None,
) -> None:
    """Revalidate one short-lived, same-run live settings receipt before a write."""

    file_identity(path)
    value = version_domains.load_json_bytes(
        path.read_bytes(), label="GitHub Release control-plane receipt"
    )
    expected_fields = {
        "receipt_version",
        "repository",
        "run_id",
        "run_attempt",
        "controller_sha",
        "release_sha",
        "release_tag_sha",
        "private_source_sha",
        "mirror_receipt_tag_sha",
        "public_source_snapshot_digest",
        "observed_at",
        "expires_at",
        "settings_snapshot",
        "receipt_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise ValueError("control-plane receipt has an open or incomplete shape")
    expected_identity = {
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
    }
    for field, expected in expected_identity.items():
        if value[field] != expected:
            raise ValueError(
                f"control-plane receipt {field} belongs to another finalization"
            )
    if (
        POSITIVE_DECIMAL_PATTERN.fullmatch(run_id) is None
        or POSITIVE_DECIMAL_PATTERN.fullmatch(run_attempt) is None
    ):
        raise ValueError("control-plane receipt run identity is not canonical")
    for field in (
        "controller_sha",
        "release_sha",
        "release_tag_sha",
        "private_source_sha",
        "mirror_receipt_tag_sha",
    ):
        if GIT_SHA_PATTERN.fullmatch(value[field]) is None:
            raise ValueError(
                f"control-plane receipt {field} is not one exact Git identity"
            )
    if value["release_tag_sha"] == value["release_sha"]:
        raise ValueError(
            "control-plane receipt does not bind a distinct annotated tag object"
        )
    if value["private_source_sha"] == value["release_sha"]:
        raise ValueError(
            "control-plane receipt does not distinguish private and public source"
        )
    if not isinstance(value["public_source_snapshot_digest"], str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", value["public_source_snapshot_digest"]
    ) is None:
        raise ValueError("control-plane receipt source snapshot is malformed")
    claimed_digest = value["receipt_sha256"]
    if not isinstance(claimed_digest, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", claimed_digest
    ) is None:
        raise ValueError("control-plane receipt digest is malformed")
    digest_preimage = {
        key: item for key, item in value.items() if key != "receipt_sha256"
    }
    if _receipt_digest(digest_preimage) != claimed_digest:
        raise ValueError("control-plane receipt digest does not authenticate its contents")
    _validate_control_plane_settings(value["settings_snapshot"])

    observed_at = _parse_receipt_time(value["observed_at"], field="observed_at")
    expires_at = _parse_receipt_time(value["expires_at"], field="expires_at")
    if expires_at - observed_at != dt.timedelta(
        seconds=CONTROL_PLANE_RECEIPT_TTL_SECONDS
    ):
        raise ValueError("control-plane receipt does not use the fixed short lifetime")
    instant = now or dt.datetime.now(dt.timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() != dt.timedelta(0):
        raise ValueError("control-plane receipt verification time must be UTC")
    if instant < observed_at:
        raise ValueError("control-plane receipt observation is in the future")
    if instant >= expires_at:
        raise ValueError("control-plane receipt is stale")


def validate_bom_identity(
    path: pathlib.Path,
    *,
    version: str,
    release_sha: str,
    private_source_sha: str,
    public_source_snapshot_digest: str,
) -> None:
    """Bind the frozen BOM to distinct private and rewritten public sources."""

    value = version_domains.load_json_bytes(path.read_bytes(), label="release BOM")
    registry = version_domains.load_registry()
    if (
        registry["source_generation"]["source_snapshot_digest"]
        != public_source_snapshot_digest
    ):
        raise ValueError("release BOM source snapshot does not match the mirror receipt")
    version_domains.validate_release_bom(
        value,
        registry=registry,
        source_sha=private_source_sha,
        public_source_sha=release_sha,
    )
    if value["workspace_version"] != version:
        raise ValueError("release BOM workspace_version belongs to another generation")


def validate_attested_bom_projection(
    path: pathlib.Path,
    *,
    version: str,
    release_sha: str,
    private_source_sha: str,
    public_source_snapshot_digest: str,
) -> None:
    """Bind an attested BOM to the exact payload without re-opening its authority."""

    value = version_domains.load_json_bytes(path.read_bytes(), label="attested release BOM")
    registry = version_domains.load_registry()
    if (
        registry["source_generation"]["source_snapshot_digest"]
        != public_source_snapshot_digest
    ):
        raise ValueError("attested release BOM snapshot differs from the mirror receipt")
    version_domains.validate_release_bom_projection(
        value,
        registry=registry,
        source_sha=private_source_sha,
        public_source_sha=release_sha,
    )
    if value["workspace_version"] != version:
        raise ValueError("release BOM workspace_version belongs to another generation")


def create_finalization_stage(
    *,
    repository: str,
    version: str,
    release_sha: str,
    release_tag_sha: str,
    private_source_sha: str,
    mirror_receipt_tag_sha: str,
    public_source_snapshot_digest: str,
    controller_sha: str,
    notes_path: pathlib.Path,
    asset_path: pathlib.Path,
    output: pathlib.Path,
) -> pathlib.Path:
    """Freeze exact notes and BOM in one credential-free data bundle."""

    if not repository or repository.count("/") != 1:
        raise ValueError("release repository is invalid")
    if GIT_SHA_PATTERN.fullmatch(release_sha) is None:
        raise ValueError("release SHA must be one exact lowercase Git commit")
    if (
        GIT_SHA_PATTERN.fullmatch(release_tag_sha) is None
        or release_tag_sha == release_sha
    ):
        raise ValueError("release tag SHA must be one distinct annotated tag object")
    if GIT_SHA_PATTERN.fullmatch(controller_sha) is None:
        raise ValueError("controller SHA must be one exact lowercase Git commit")
    if (
        GIT_SHA_PATTERN.fullmatch(private_source_sha) is None
        or private_source_sha == release_sha
    ):
        raise ValueError("private source SHA must differ from the public release SHA")
    if GIT_SHA_PATTERN.fullmatch(mirror_receipt_tag_sha) is None:
        raise ValueError("mirror receipt tag SHA must be one exact Git object")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", public_source_snapshot_digest) is None:
        raise ValueError("public source snapshot digest is malformed")
    tag, title, asset_name = release_identity(version)
    file_identity(notes_path)
    file_identity(asset_path)
    notes = notes_path.read_text(encoding="utf-8")
    if not notes.strip():
        raise ValueError("release notes are empty")
    validate_bom_identity(
        asset_path,
        version=version,
        release_sha=release_sha,
        private_source_sha=private_source_sha,
        public_source_snapshot_digest=public_source_snapshot_digest,
    )

    output.mkdir(parents=True, exist_ok=False)
    frozen_notes = output / FINALIZATION_NOTES_NAME
    frozen_asset = output / asset_name
    shutil.copyfile(notes_path, frozen_notes)
    shutil.copyfile(asset_path, frozen_asset)
    manifest = {
        "stage_version": FINALIZATION_STAGE_VERSION,
        "repository": repository,
        "version": version,
        "tag": tag,
        "title": title,
        "release_sha": release_sha,
        "release_tag_sha": release_tag_sha,
        "private_source_sha": private_source_sha,
        "mirror_receipt_tag_sha": mirror_receipt_tag_sha,
        "public_source_snapshot_digest": public_source_snapshot_digest,
        "controller_sha": controller_sha,
        "notes": {"name": frozen_notes.name, **file_identity(frozen_notes)},
        "asset": {"name": frozen_asset.name, **file_identity(frozen_asset)},
    }
    manifest_path = output / FINALIZATION_MANIFEST_NAME
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest_path


def _load_finalization_stage_files(
    directory: pathlib.Path,
    *,
    repository: str,
    version: str,
    release_sha: str,
    release_tag_sha: str,
    private_source_sha: str,
    mirror_receipt_tag_sha: str,
    public_source_snapshot_digest: str,
    controller_sha: str,
) -> FinalizationStage:
    """Authenticate frozen file identities without executing tag payload."""

    tag, title, asset_name = release_identity(version)
    if (
        GIT_SHA_PATTERN.fullmatch(release_tag_sha) is None
        or release_tag_sha == release_sha
    ):
        raise ValueError("release tag SHA must be one distinct annotated tag object")
    if (
        GIT_SHA_PATTERN.fullmatch(private_source_sha) is None
        or private_source_sha == release_sha
        or GIT_SHA_PATTERN.fullmatch(mirror_receipt_tag_sha) is None
        or re.fullmatch(r"sha256:[0-9a-f]{64}", public_source_snapshot_digest)
        is None
    ):
        raise ValueError("release finalization source mapping identity is malformed")
    expected_names = {
        FINALIZATION_MANIFEST_NAME,
        FINALIZATION_NOTES_NAME,
        asset_name,
    }
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("release finalization stage is not one real directory")
    if {path.name for path in directory.iterdir()} != expected_names:
        raise ValueError("release finalization stage has unexpected files")
    manifest_path = directory / FINALIZATION_MANIFEST_NAME
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise ValueError("release finalization manifest is not one regular file")
    manifest = version_domains.load_json_bytes(
        manifest_path.read_bytes(), label="release finalization manifest"
    )
    if not isinstance(manifest, dict) or set(manifest) != {
        "stage_version",
        "repository",
        "version",
        "tag",
        "title",
        "release_sha",
        "release_tag_sha",
        "private_source_sha",
        "mirror_receipt_tag_sha",
        "public_source_snapshot_digest",
        "controller_sha",
        "notes",
        "asset",
    }:
        raise ValueError("release finalization manifest has an open or incomplete shape")
    expected = {
        "stage_version": FINALIZATION_STAGE_VERSION,
        "repository": repository,
        "version": version,
        "tag": tag,
        "title": title,
        "release_sha": release_sha,
        "release_tag_sha": release_tag_sha,
        "private_source_sha": private_source_sha,
        "mirror_receipt_tag_sha": mirror_receipt_tag_sha,
        "public_source_snapshot_digest": public_source_snapshot_digest,
        "controller_sha": controller_sha,
    }
    for field, wanted in expected.items():
        if manifest.get(field) != wanted:
            raise ValueError(f"release finalization {field} belongs to another generation")

    paths: dict[str, pathlib.Path] = {}
    for field, name in (("notes", FINALIZATION_NOTES_NAME), ("asset", asset_name)):
        record = manifest.get(field)
        if not isinstance(record, dict) or set(record) != {"name", "size", "sha256"}:
            raise ValueError(f"release finalization {field} identity is malformed")
        if (
            record["name"] != name
            or type(record["size"]) is not int
            or record["size"] < 0
            or not isinstance(record["sha256"], str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", record["sha256"]) is None
        ):
            raise ValueError(f"release finalization {field} identity is not canonical")
        path = directory / name
        if file_identity(path) != {"size": record["size"], "sha256": record["sha256"]}:
            raise ValueError(f"release finalization {field} bytes changed")
        paths[field] = path

    if not paths["notes"].read_text(encoding="utf-8").strip():
        raise ValueError("release notes are empty")
    return FinalizationStage(
        tag,
        release_tag_sha,
        private_source_sha,
        mirror_receipt_tag_sha,
        public_source_snapshot_digest,
        title,
        paths["notes"],
        paths["asset"],
        asset_name,
    )


def load_finalization_stage(
    directory: pathlib.Path,
    *,
    repository: str,
    version: str,
    release_sha: str,
    release_tag_sha: str,
    private_source_sha: str,
    mirror_receipt_tag_sha: str,
    public_source_snapshot_digest: str,
    controller_sha: str,
) -> FinalizationStage:
    """Authenticate one downloaded bundle and its complete BOM/3 semantics."""

    frozen = _load_finalization_stage_files(
        directory,
        repository=repository,
        version=version,
        release_sha=release_sha,
        release_tag_sha=release_tag_sha,
        private_source_sha=private_source_sha,
        mirror_receipt_tag_sha=mirror_receipt_tag_sha,
        public_source_snapshot_digest=public_source_snapshot_digest,
        controller_sha=controller_sha,
    )
    validate_bom_identity(
        frozen.asset_path,
        version=version,
        release_sha=release_sha,
        private_source_sha=private_source_sha,
        public_source_snapshot_digest=public_source_snapshot_digest,
    )
    return frozen


def invoke_gh(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["gh", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def invoke_git(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        check=False,
        capture_output=True,
        text=True,
    )


def assert_remote_release_fence(
    *,
    repository: str,
    tag: str,
    release_sha: str,
    release_tag_sha: str,
    mirror_receipt_tag_sha: str,
    invoke: Invoke = invoke_git,
) -> None:
    """Require immutable release and mirror refs before one mutation.

    The workflow admits one exact controller SHA from public main before any
    release work begins. That admission is the linearization point for the
    non-cancelled workflow run; a later main advance does not revoke the
    already-running immutable controller.
    """

    mirror_receipt_ref = (
        f"refs/tags/{mirror_receipt_tag_name(release_sha)}"
    )
    result = invoke(
        [
            "ls-remote",
            f"https://github.com/{repository}.git",
            f"refs/tags/{tag}",
            f"refs/tags/{tag}^{{}}",
            mirror_receipt_ref,
        ]
    )
    if result.returncode != 0:
        raise ValueError("remote release fence readback failed")
    refs: dict[str, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        if (
            len(fields) != 2
            or GIT_SHA_PATTERN.fullmatch(fields[0]) is None
            or fields[1] in refs
        ):
            raise ValueError("remote release fence readback is malformed")
        refs[fields[1]] = fields[0]
    tag_ref = f"refs/tags/{tag}"
    peeled_ref = f"{tag_ref}^{{}}"
    if set(refs) != {
        tag_ref,
        peeled_ref,
        mirror_receipt_ref,
    }:
        raise ValueError(
            "remote release fence omitted the release tag or mirror receipt"
        )
    if refs[tag_ref] != release_tag_sha:
        raise ValueError("remote annotated tag object moved from the frozen tag")
    if refs[peeled_ref] != release_sha:
        raise ValueError("remote annotated tag moved from the release payload")
    if refs[mirror_receipt_ref] != mirror_receipt_tag_sha:
        raise ValueError("remote mirror receipt moved from the frozen source mapping")


def stable_tag_version(tag: object) -> tuple[int, int, int] | None:
    """Return the numeric version of one canonical stable Release tag."""

    if not isinstance(tag, str) or not tag.startswith("v"):
        return None
    version = tag[1:]
    if version_domains.STABLE_VERSION_PATTERN.fullmatch(version) is None:
        return None
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


def validate_release_inventory(
    records: Sequence[ReleaseInventoryRecord], *, terminal: bool = False
) -> None:
    """Close duplicate identities and the single stable Latest authority."""

    database_ids = [record.database_id for record in records]
    tags = [record.tag for record in records]
    if len(database_ids) != len(set(database_ids)) or len(tags) != len(set(tags)):
        raise ValueError("GitHub Release inventory contains duplicate identities")

    stable: dict[tuple[int, int, int], ReleaseInventoryRecord] = {}
    latest = []
    for record in records:
        if (
            type(record.database_id) is not int
            or record.database_id <= 0
            or not isinstance(record.tag, str)
            or not record.tag
            or type(record.draft) is not bool
            or type(record.prerelease) is not bool
            or type(record.is_latest) is not bool
        ):
            raise ValueError("GitHub Release inventory identity is malformed")
        if record.is_latest:
            latest.append(record)
        version = stable_tag_version(record.tag)
        if record.draft or record.prerelease or version is None:
            continue
        if version in stable:
            raise ValueError("GitHub Release inventory repeats a stable version")
        stable[version] = record

    if len(latest) > 1:
        raise ValueError("GitHub Release inventory has multiple Latest authorities")
    if latest:
        owner = latest[0]
        if owner.draft or owner.prerelease or stable_tag_version(owner.tag) is None:
            raise ValueError("a non-stable GitHub Release owns Latest authority")
    if terminal:
        if not stable or len(latest) != 1:
            raise ValueError("published stable Releases do not have exactly one Latest")
        highest = stable[max(stable)]
        if latest[0] != highest:
            raise ValueError("GitHub Release Latest is not the highest stable version")


def release_inventory(
    invoke: Invoke, repository: str
) -> tuple[ReleaseInventoryRecord, ...]:
    """Read every REST page and bind it to the CLI's explicit Latest flags."""

    response = invoke(
        [
            "api",
            "--method",
            "GET",
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2022-11-28",
            f"repos/{repository}/releases?per_page=100",
        ]
    )
    if response.returncode != 0:
        raise ValueError("complete GitHub Release inventory readback failed")
    pages = version_domains.load_json_bytes(
        response.stdout.encode("utf-8"), label="GitHub Release REST inventory"
    )
    if not isinstance(pages, list) or not all(isinstance(page, list) for page in pages):
        raise ValueError("GitHub Release REST pagination is malformed")

    by_tag: dict[str, tuple[int, bool, bool]] = {}
    database_ids: set[int] = set()
    for page in pages:
        for value in page:
            if not isinstance(value, dict):
                raise ValueError("GitHub Release REST record is not an object")
            database_id = value.get("id")
            tag = value.get("tag_name")
            draft = value.get("draft")
            prerelease = value.get("prerelease")
            if (
                type(database_id) is not int
                or database_id <= 0
                or not isinstance(tag, str)
                or not tag
                or type(draft) is not bool
                or type(prerelease) is not bool
            ):
                raise ValueError("GitHub Release REST identity is malformed")
            if database_id in database_ids or tag in by_tag:
                raise ValueError("GitHub Release REST inventory contains duplicates")
            database_ids.add(database_id)
            by_tag[tag] = (database_id, draft, prerelease)

    # The REST list is the complete identity authority. Requesting one more CLI
    # row than that exact count proves the Latest projection was not truncated;
    # any concurrent create/delete or state change also fails the exact join.
    projection = invoke(
        [
            "release",
            "list",
            "--repo",
            repository,
            "--limit",
            str(len(by_tag) + 1),
            "--json",
            "tagName,isDraft,isPrerelease,isLatest",
        ]
    )
    if projection.returncode != 0:
        raise ValueError("GitHub Release Latest projection readback failed")
    values = version_domains.load_json_bytes(
        projection.stdout.encode("utf-8"), label="GitHub Release Latest projection"
    )
    if not isinstance(values, list) or len(values) != len(by_tag):
        raise ValueError("GitHub Release Latest projection is incomplete or raced")

    records = []
    observed_tags: set[str] = set()
    for value in values:
        if not isinstance(value, dict) or set(value) != {
            "tagName",
            "isDraft",
            "isPrerelease",
            "isLatest",
        }:
            raise ValueError("GitHub Release Latest projection has an open shape")
        tag = value["tagName"]
        if not isinstance(tag, str) or tag in observed_tags or tag not in by_tag:
            raise ValueError("GitHub Release Latest projection identity is not exact")
        database_id, draft, prerelease = by_tag[tag]
        if (
            type(value["isDraft"]) is not bool
            or type(value["isPrerelease"]) is not bool
            or type(value["isLatest"]) is not bool
            or value["isDraft"] != draft
            or value["isPrerelease"] != prerelease
        ):
            raise ValueError("GitHub Release Latest projection raced REST state")
        observed_tags.add(tag)
        records.append(
            ReleaseInventoryRecord(
                database_id=database_id,
                tag=tag,
                draft=draft,
                prerelease=prerelease,
                is_latest=value["isLatest"],
            )
        )
    if observed_tags != set(by_tag):
        raise ValueError("GitHub Release Latest projection omitted REST identities")
    result = tuple(records)
    validate_release_inventory(result)
    return result


def inventory_record(
    records: Sequence[ReleaseInventoryRecord], tag: str
) -> ReleaseInventoryRecord | None:
    matches = [record for record in records if record.tag == tag]
    if len(matches) > 1:
        raise ValueError("GitHub Release inventory repeats the target tag")
    return matches[0] if matches else None


def highest_stable_release(
    records: Sequence[ReleaseInventoryRecord], *, include_tag: str | None = None
) -> tuple[tuple[int, int, int], ReleaseInventoryRecord | None]:
    """Select the numeric stable maximum, optionally including a future target."""

    versions = [
        (version, record)
        for record in records
        if not record.draft
        and not record.prerelease
        and (version := stable_tag_version(record.tag)) is not None
    ]
    if include_tag is not None:
        version = stable_tag_version(include_tag)
        if version is None:
            raise ValueError("target GitHub Release tag is not canonical stable SemVer")
        if not any(record.tag == include_tag for _, record in versions):
            versions.append((version, None))
    if not versions:
        raise ValueError("GitHub Release inventory has no stable version")
    return max(versions, key=lambda item: item[0])


def release_view_for_inventory(
    invoke: Invoke,
    repository: str,
    tag: str,
    records: Sequence[ReleaseInventoryRecord],
) -> dict[str, Any] | None:
    """Exact-join a targeted metadata view to the complete inventory snapshot."""

    record = inventory_record(records, tag)
    view = release_view(invoke, repository, tag)
    if (record is None) != (view is None):
        raise ValueError("GitHub Release target view raced the complete inventory")
    if record is not None and view is not None and (
        view.get("tagName") != record.tag
        or view.get("isDraft") != record.draft
        or view.get("isPrerelease") != record.prerelease
    ):
        raise ValueError("GitHub Release target view disagrees with the inventory")
    return view


def release_view(
    invoke: Invoke, repository: str, tag: str
) -> dict[str, Any] | None:
    result = invoke(
        [
            "release",
            "view",
            tag,
            "--repo",
            repository,
            "--json",
            "tagName,name,isDraft,isPrerelease,isImmutable,body,assets",
        ]
    )
    if result.returncode != 0:
        if not result.stdout and result.stderr.strip() == GH_RELEASE_NOT_FOUND:
            return None
        raise ValueError("GitHub Release readback failed")
    value = version_domains.load_json_bytes(
        result.stdout.encode("utf-8"), label="GitHub Release readback"
    )
    if not isinstance(value, dict):
        raise ValueError("GitHub Release readback is not an object")
    return value


def validate_metadata(
    release: dict[str, Any], *, tag: str, title: str, notes: str
) -> None:
    expected = {"tagName": tag, "name": title}
    for field, value in expected.items():
        if release.get(field) != value:
            raise ValueError(
                f"GitHub Release metadata {field}={release.get(field)!r}, "
                f"expected {value!r}"
            )
    if type(release.get("isDraft")) is not bool:
        raise ValueError("GitHub Release draft state is missing or malformed")
    if type(release.get("isPrerelease")) is not bool:
        raise ValueError("GitHub Release prerelease state is missing or malformed")
    if type(release.get("isImmutable")) is not bool:
        raise ValueError("GitHub Release immutable state is missing or malformed")
    if release["isPrerelease"]:
        raise ValueError("GitHub Release unexpectedly targets a prerelease")
    if release["isImmutable"] == release["isDraft"]:
        raise ValueError(
            "GitHub Release must be mutable only while draft and immutable once published"
        )
    if not isinstance(release.get("body"), str) or release["body"] != notes:
        raise ValueError("GitHub Release notes differ from the exact changelog entry")
    assets = release.get("assets")
    if not isinstance(assets, list) or not all(isinstance(asset, dict) for asset in assets):
        raise ValueError("GitHub Release asset readback is malformed")


def validate_asset_scope(
    release: dict[str, Any], *, asset_name: str, allow_missing: bool
) -> None:
    """Reject every GitHub Release asset outside the one frozen BOM."""

    names = [asset.get("name") for asset in release["assets"]]
    if not all(isinstance(name, str) and name for name in names):
        raise ValueError("GitHub Release asset identity is malformed")
    if len(names) != len(set(names)):
        raise ValueError("GitHub Release contains duplicate asset names")
    unexpected = set(names) - {asset_name}
    if unexpected:
        raise ValueError(f"GitHub Release contains unexpected assets {sorted(unexpected)}")
    if not allow_missing and names != [asset_name]:
        raise ValueError("published GitHub Release is missing its immutable BOM")


def compare_remote_asset(
    invoke: Invoke,
    repository: str,
    tag: str,
    asset_name: str,
    expected: pathlib.Path,
) -> None:
    with tempfile.TemporaryDirectory(prefix="cymule-release-readback-") as temporary:
        observed = pathlib.Path(temporary) / asset_name
        result = invoke(
            [
                "release",
                "download",
                tag,
                "--repo",
                repository,
                "--pattern",
                asset_name,
                "--output",
                str(observed),
            ]
        )
        if result.returncode != 0 or not observed.is_file():
            raise ValueError("GitHub Release BOM readback failed")
        if observed.read_bytes() != expected.read_bytes():
            raise ValueError("GitHub Release BOM differs from the exact-tag reconstruction")


def release_assets(
    invoke: Invoke, repository: str, release_id: int
) -> tuple[ReleaseAssetRecord, ...]:
    """Read the complete REST asset inventory for one exact Release identity."""

    response = invoke(
        [
            "api",
            "--method",
            "GET",
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2026-03-10",
            f"repos/{repository}/releases/{release_id}/assets?per_page=100",
        ]
    )
    if response.returncode != 0:
        raise ValueError("GitHub Release asset inventory readback failed")
    pages = version_domains.load_json_bytes(
        response.stdout.encode("utf-8"), label="GitHub Release asset inventory"
    )
    if not isinstance(pages, list) or not all(isinstance(page, list) for page in pages):
        raise ValueError("GitHub Release asset pagination is malformed")
    records: list[ReleaseAssetRecord] = []
    identities: set[int] = set()
    names: set[str] = set()
    for page in pages:
        for value in page:
            if not isinstance(value, dict):
                raise ValueError("GitHub Release asset record is not an object")
            database_id = value.get("id")
            name = value.get("name")
            size = value.get("size")
            digest = value.get("digest")
            state = value.get("state")
            if (
                type(database_id) is not int
                or database_id <= 0
                or not isinstance(name, str)
                or not name
                or type(size) is not int
                or size < 0
                or not isinstance(digest, str)
                or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
                or state != "uploaded"
                or database_id in identities
                or name in names
            ):
                raise ValueError("GitHub Release asset identity is malformed")
            identities.add(database_id)
            names.add(name)
            records.append(
                ReleaseAssetRecord(database_id, name, size, digest, state)
            )
    return tuple(records)


def exact_release_asset(
    invoke: Invoke,
    repository: str,
    release: ReleaseInventoryRecord,
    *,
    asset_name: str,
    expected: pathlib.Path,
    allow_missing: bool,
) -> ReleaseAssetRecord | None:
    """Bind one Release projection to the exact local BOM identity."""

    records = release_assets(invoke, repository, release.database_id)
    if not records and allow_missing:
        return None
    identity = file_identity(expected)
    wanted = ReleaseAssetRecord(
        database_id=records[0].database_id if len(records) == 1 else 0,
        name=asset_name,
        size=identity["size"],
        digest=identity["sha256"],
        state="uploaded",
    )
    if len(records) != 1 or records[0] != wanted:
        raise ValueError("GitHub Release asset identity or bytes are not exact")
    return records[0]


def verify_bom_attestation(
    *,
    repository: str,
    controller_sha: str,
    asset_path: pathlib.Path,
    bundle_path: pathlib.Path,
    invoke: Invoke = invoke_gh,
) -> None:
    """Verify the content-addressed BOM authority before Release projection."""

    file_identity(asset_path)
    file_identity(bundle_path)
    result = invoke(
        [
            "attestation",
            "verify",
            str(asset_path),
            "--repo",
            repository,
            "--bundle",
            str(bundle_path),
            "--signer-workflow",
            f"{repository}/.github/workflows/finalize-release.yml",
            "--signer-digest",
            controller_sha,
            "--source-digest",
            controller_sha,
            "--source-ref",
            "refs/heads/main",
            "--deny-self-hosted-runners",
            "--format",
            "json",
        ]
    )
    if result.returncode != 0:
        raise ValueError("release BOM attestation verification failed")
    value = version_domains.load_json_bytes(
        result.stdout.encode("utf-8"), label="release BOM attestation verification"
    )
    if not isinstance(value, list) or not value:
        raise ValueError("release BOM attestation verification returned no authority")


def converge_release(
    *,
    repository: str,
    tag: str,
    title: str,
    notes_path: pathlib.Path,
    asset_path: pathlib.Path,
    asset_name: str,
    assert_control_plane: Fence,
    assert_fence: Fence,
    assert_attestation: Fence,
    invoke: Invoke = invoke_gh,
) -> None:
    notes = notes_path.read_text(encoding="utf-8")
    if not notes.strip():
        raise ValueError("release notes are empty")
    if not asset_path.is_file() or not asset_name or "/" in asset_name:
        raise ValueError("release BOM path or name is invalid")

    def mutate_release(arguments: Sequence[str]) -> None:
        assert_attestation()
        assert_fence()
        assert_control_plane()
        invoke(arguments)

    inventory = release_inventory(invoke, repository)
    release = release_view_for_inventory(invoke, repository, tag, inventory)
    if release is None:
        # The command may lose its response after creating the draft. Readback,
        # not the process result, decides whether the transition committed.
        mutate_release(
            [
                "release",
                "create",
                tag,
                "--repo",
                repository,
                "--verify-tag",
                "--draft",
                "--title",
                title,
                "--notes-file",
                str(notes_path),
            ]
        )
        inventory = release_inventory(invoke, repository)
        release = release_view_for_inventory(invoke, repository, tag, inventory)
        if release is None:
            raise ValueError("GitHub Release draft creation did not converge")
    validate_metadata(release, tag=tag, title=title, notes=notes)
    validate_asset_scope(
        release, asset_name=asset_name, allow_missing=release["isDraft"]
    )
    target = inventory_record(inventory, tag)
    if target is None:
        raise ValueError("GitHub Release target is absent from the complete inventory")
    asset_record = exact_release_asset(
        invoke,
        repository,
        target,
        asset_name=asset_name,
        expected=asset_path,
        allow_missing=release["isDraft"],
    )

    if asset_record is None:
        if not release["isDraft"]:
            raise ValueError("published GitHub Release is missing its immutable BOM")
        # No --clobber: this transition may create a missing asset but can never
        # replace bytes. A lost response is resolved by the following readback.
        inventory = release_inventory(invoke, repository)
        release = release_view_for_inventory(invoke, repository, tag, inventory)
        if release is None:
            raise ValueError("GitHub Release disappeared before BOM upload")
        validate_metadata(release, tag=tag, title=title, notes=notes)
        validate_asset_scope(release, asset_name=asset_name, allow_missing=True)
        target = inventory_record(inventory, tag)
        if target is None:
            raise ValueError("GitHub Release target vanished before BOM upload")
        if exact_release_asset(
            invoke,
            repository,
            target,
            asset_name=asset_name,
            expected=asset_path,
            allow_missing=True,
        ) is not None:
            raise ValueError("GitHub Release BOM appeared outside this controller")
        if not release["isDraft"]:
            raise ValueError("GitHub Release published before its immutable BOM")
        mutate_release(
            [
                "release",
                "upload",
                tag,
                f"{asset_path}#{asset_name}",
                "--repo",
                repository,
            ]
        )
        inventory = release_inventory(invoke, repository)
        release = release_view_for_inventory(invoke, repository, tag, inventory)
        if release is None:
            raise ValueError("GitHub Release disappeared after BOM upload")
        validate_metadata(release, tag=tag, title=title, notes=notes)
        validate_asset_scope(release, asset_name=asset_name, allow_missing=False)
        target = inventory_record(inventory, tag)
        if target is None:
            raise ValueError("GitHub Release target vanished after BOM upload")
        asset_record = exact_release_asset(
            invoke,
            repository,
            target,
            asset_name=asset_name,
            expected=asset_path,
            allow_missing=False,
        )
    if asset_record is None:
        raise ValueError("GitHub Release BOM identity did not converge")
    compare_remote_asset(invoke, repository, tag, asset_name, asset_path)

    # Re-read the complete stable set immediately before publication. GitHub's
    # default for a newly published Release is Latest=true, so both branches
    # pass an explicit boolean selected by numeric stable SemVer.
    inventory = release_inventory(invoke, repository)
    release = release_view_for_inventory(invoke, repository, tag, inventory)
    if release is None:
        raise ValueError("GitHub Release disappeared before publication")
    validate_metadata(release, tag=tag, title=title, notes=notes)
    validate_asset_scope(release, asset_name=asset_name, allow_missing=False)
    target = inventory_record(inventory, tag)
    if target is None:
        raise ValueError("GitHub Release target is absent from the complete inventory")
    highest_version, _ = highest_stable_release(inventory, include_tag=tag)
    target_version = stable_tag_version(tag)
    if target_version is None:
        raise ValueError("target GitHub Release tag is not canonical stable SemVer")
    target_should_be_latest = target_version == highest_version
    target_latest_is_wrong = target.is_latest != target_should_be_latest
    prepublication_asset = exact_release_asset(
        invoke,
        repository,
        target,
        asset_name=asset_name,
        expected=asset_path,
        allow_missing=False,
    )
    if prepublication_asset != asset_record:
        raise ValueError("GitHub Release BOM asset identity changed before publication")
    compare_remote_asset(invoke, repository, tag, asset_name, asset_path)
    if release["isDraft"] or target_latest_is_wrong:
        mutate_release(
            [
                "release",
                "edit",
                tag,
                "--repo",
                repository,
                "--draft=false",
                "--latest=true" if target_should_be_latest else "--latest=false",
            ]
        )
        inventory = release_inventory(invoke, repository)
        release = release_view_for_inventory(invoke, repository, tag, inventory)
        if release is None:
            raise ValueError("published GitHub Release readback failed")
        target = inventory_record(inventory, tag)
        if (
            release["isDraft"]
            or target is None
            or target.is_latest != target_should_be_latest
        ):
            raise ValueError("GitHub Release publication or explicit Latest state did not converge")
        if not release["isImmutable"]:
            raise ValueError("published GitHub Release is not immutable")
        if exact_release_asset(
            invoke,
            repository,
            target,
            asset_name=asset_name,
            expected=asset_path,
            allow_missing=False,
        ) != prepublication_asset:
            raise ValueError("GitHub Release BOM asset identity changed during publication")

    # A pre-existing repository may have one structurally valid but stale
    # Latest pointer. Move it only forward to the numeric maximum; historical
    # recovery can therefore never demote a newer stable Release.
    inventory = release_inventory(invoke, repository)
    _, highest = highest_stable_release(inventory)
    if highest is None:
        raise ValueError("highest published stable Release has no inventory record")
    latest = [record for record in inventory if record.is_latest]
    if len(latest) != 1 or latest[0] != highest:
        mutate_release(
            [
                "release",
                "edit",
                highest.tag,
                "--repo",
                repository,
                "--latest=true",
            ]
        )
        inventory = release_inventory(invoke, repository)

    validate_release_inventory(inventory, terminal=True)
    final = release_view_for_inventory(invoke, repository, tag, inventory)
    if final is None:
        raise ValueError("published GitHub Release readback failed")
    validate_metadata(final, tag=tag, title=title, notes=notes)
    validate_asset_scope(final, asset_name=asset_name, allow_missing=False)
    if final["isDraft"]:
        raise ValueError("GitHub Release remained a draft")
    if not final["isImmutable"]:
        raise ValueError("published GitHub Release is not immutable")
    final_target = inventory_record(inventory, tag)
    if final_target is None:
        raise ValueError("published GitHub Release target is absent")
    if exact_release_asset(
        invoke,
        repository,
        final_target,
        asset_name=asset_name,
        expected=asset_path,
        allow_missing=False,
    ) != prepublication_asset:
        raise ValueError("published GitHub Release BOM asset identity changed")
    compare_remote_asset(invoke, repository, tag, asset_name, asset_path)
    assert_attestation()
    assert_fence()
    assert_control_plane()


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    mirror = commands.add_parser("verify-mirror-receipt")
    mirror.add_argument("--public-source-sha", required=True)
    mirror.add_argument("--expected-tag-sha")
    mirror.add_argument("--expected-private-source-sha")
    mirror.add_argument("--expected-source-snapshot-digest")
    mirror.add_argument("--github-output", type=pathlib.Path)
    stage = commands.add_parser("stage")
    stage.add_argument("--repository", required=True)
    stage.add_argument("--version", required=True)
    stage.add_argument("--release-sha", required=True)
    stage.add_argument("--release-tag-sha", required=True)
    stage.add_argument("--private-source-sha", required=True)
    stage.add_argument("--mirror-receipt-tag-sha", required=True)
    stage.add_argument("--public-source-snapshot-digest", required=True)
    stage.add_argument("--controller-sha", required=True)
    stage.add_argument("--notes-file", type=pathlib.Path, required=True)
    stage.add_argument("--asset", type=pathlib.Path, required=True)
    stage.add_argument("--output", type=pathlib.Path, required=True)
    verify_stage = commands.add_parser("verify-stage")
    verify_stage.add_argument("--repository", required=True)
    verify_stage.add_argument("--version", required=True)
    verify_stage.add_argument("--release-sha", required=True)
    verify_stage.add_argument("--release-tag-sha", required=True)
    verify_stage.add_argument("--private-source-sha", required=True)
    verify_stage.add_argument("--mirror-receipt-tag-sha", required=True)
    verify_stage.add_argument("--public-source-snapshot-digest", required=True)
    verify_stage.add_argument("--controller-sha", required=True)
    verify_stage.add_argument("--stage", type=pathlib.Path, required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--repository", required=True)
    publish.add_argument("--version", required=True)
    publish.add_argument("--release-sha", required=True)
    publish.add_argument("--release-tag-sha", required=True)
    publish.add_argument("--private-source-sha", required=True)
    publish.add_argument("--mirror-receipt-tag-sha", required=True)
    publish.add_argument("--public-source-snapshot-digest", required=True)
    publish.add_argument("--controller-sha", required=True)
    publish.add_argument("--stage", type=pathlib.Path, required=True)
    publish.add_argument("--attestation-bundle", type=pathlib.Path, required=True)
    publish.add_argument("--control-plane-receipt", type=pathlib.Path, required=True)
    publish.add_argument("--run-id", required=True)
    publish.add_argument("--run-attempt", required=True)
    arguments = parser.parse_args()
    if arguments.command == "verify-mirror-receipt":
        receipt = load_mirror_receipt(
            version_domains.ROOT,
            public_source_sha=arguments.public_source_sha,
            expected_tag_sha=arguments.expected_tag_sha,
        )
        if (
            arguments.expected_private_source_sha is not None
            and receipt.private_source_sha
            != arguments.expected_private_source_sha
        ):
            raise ValueError("mirror receipt private source differs from expectation")
        if (
            arguments.expected_source_snapshot_digest is not None
            and receipt.public_source_snapshot_digest
            != arguments.expected_source_snapshot_digest
        ):
            raise ValueError("mirror receipt source snapshot differs from expectation")
        outputs = {
            "private_source_sha": receipt.private_source_sha,
            "public_source_sha": receipt.public_source_sha,
            "public_source_snapshot_digest": receipt.public_source_snapshot_digest,
            "mirror_receipt_tag_sha": receipt.tag_sha,
        }
        if arguments.github_output is None:
            print(json.dumps(outputs, sort_keys=True, separators=(",", ":")))
        else:
            if arguments.github_output.is_symlink() or not arguments.github_output.is_file():
                raise ValueError("GitHub output must be one existing regular file")
            with arguments.github_output.open("a", encoding="utf-8") as stream:
                for name, value in outputs.items():
                    stream.write(f"{name}={value}\n")
    elif arguments.command == "stage":
        manifest = create_finalization_stage(
            repository=arguments.repository,
            version=arguments.version,
            release_sha=arguments.release_sha,
            release_tag_sha=arguments.release_tag_sha,
            private_source_sha=arguments.private_source_sha,
            mirror_receipt_tag_sha=arguments.mirror_receipt_tag_sha,
            public_source_snapshot_digest=arguments.public_source_snapshot_digest,
            controller_sha=arguments.controller_sha,
            notes_path=arguments.notes_file,
            asset_path=arguments.asset,
            output=arguments.output,
        )
        print(manifest)
    elif arguments.command == "verify-stage":
        frozen = load_finalization_stage(
            arguments.stage,
            repository=arguments.repository,
            version=arguments.version,
            release_sha=arguments.release_sha,
            release_tag_sha=arguments.release_tag_sha,
            private_source_sha=arguments.private_source_sha,
            mirror_receipt_tag_sha=arguments.mirror_receipt_tag_sha,
            public_source_snapshot_digest=arguments.public_source_snapshot_digest,
            controller_sha=arguments.controller_sha,
        )
        print(frozen.asset_path)
    else:
        receipt = load_mirror_receipt(
            version_domains.ROOT,
            public_source_sha=arguments.release_sha,
            expected_tag_sha=arguments.mirror_receipt_tag_sha,
        )
        if (
            receipt.private_source_sha != arguments.private_source_sha
            or receipt.public_source_snapshot_digest
            != arguments.public_source_snapshot_digest
        ):
            raise ValueError("mirror receipt differs from finalization source authority")
        frozen = _load_finalization_stage_files(
            arguments.stage,
            repository=arguments.repository,
            version=arguments.version,
            release_sha=arguments.release_sha,
            release_tag_sha=arguments.release_tag_sha,
            private_source_sha=arguments.private_source_sha,
            mirror_receipt_tag_sha=arguments.mirror_receipt_tag_sha,
            public_source_snapshot_digest=arguments.public_source_snapshot_digest,
            controller_sha=arguments.controller_sha,
        )
        control_plane_fence = lambda: assert_control_plane_receipt(
            arguments.control_plane_receipt,
            repository=arguments.repository,
            run_id=arguments.run_id,
            run_attempt=arguments.run_attempt,
            controller_sha=arguments.controller_sha,
            release_sha=arguments.release_sha,
            release_tag_sha=arguments.release_tag_sha,
            private_source_sha=arguments.private_source_sha,
            mirror_receipt_tag_sha=arguments.mirror_receipt_tag_sha,
            public_source_snapshot_digest=arguments.public_source_snapshot_digest,
        )
        control_plane_fence()
        verify_bom_attestation(
            repository=arguments.repository,
            controller_sha=arguments.controller_sha,
            asset_path=frozen.asset_path,
            bundle_path=arguments.attestation_bundle,
        )
        validate_attested_bom_projection(
            frozen.asset_path,
            version=arguments.version,
            release_sha=arguments.release_sha,
            private_source_sha=arguments.private_source_sha,
            public_source_snapshot_digest=arguments.public_source_snapshot_digest,
        )
        converge_release(
            repository=arguments.repository,
            tag=frozen.tag,
            title=frozen.title,
            notes_path=frozen.notes_path,
            asset_path=frozen.asset_path,
            asset_name=frozen.asset_name,
            assert_control_plane=control_plane_fence,
            assert_attestation=lambda: verify_bom_attestation(
                repository=arguments.repository,
                controller_sha=arguments.controller_sha,
                asset_path=frozen.asset_path,
                bundle_path=arguments.attestation_bundle,
            ),
            assert_fence=lambda: assert_remote_release_fence(
                repository=arguments.repository,
                tag=frozen.tag,
                release_sha=arguments.release_sha,
                release_tag_sha=frozen.release_tag_sha,
                mirror_receipt_tag_sha=frozen.mirror_receipt_tag_sha,
            ),
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        raise SystemExit(f"release finalization failed: {error}") from error
