"""Tests for immutable public release and control-plane verification."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import urllib.parse
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]


def load_script(name: str):
    specification = importlib.util.spec_from_file_location(
        name, ROOT / "scripts" / f"{name}.py"
    )
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


npm_release = load_script("npm_release")
release_workflows = load_script("verify_release_workflows")
github_settings = load_script("verify_github_release_settings")
finalize_release = load_script("finalize_release")


def load_private_script(name: str):
    path = ROOT / ".gitlab" / "scripts" / f"{name}.py"
    if not path.is_file():
        return None
    specification = importlib.util.spec_from_file_location(name, path)
    assert specification is not None and specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


public_history = load_private_script("rewrite_public_history")


class FinalizeReleaseTests(unittest.TestCase):
    RELEASE_TAG_SHA = "f" * 40

    @staticmethod
    def settings_snapshot() -> dict[str, object]:
        return {
            "snapshot_version": github_settings.SETTINGS_SNAPSHOT_VERSION,
            "default_branch": "main",
            "authorities": {
                "mirror_integration_id": 41,
                "release_tag_integration_id": 42,
                "npm_reviewer_team_id": 51,
                "crates_reviewer_team_id": 52,
                "release_reviewer_team_id": 53,
            },
            "rulesets": {
                "main": {
                    "enforcement": "active",
                    "target": "branch",
                    "ref": "~DEFAULT_BRANCH",
                    "required_status_checks": [
                        {
                            "context": "Required CI",
                            "integration_id": github_settings.GITHUB_ACTIONS_INTEGRATION_ID,
                        }
                    ],
                    "strict_required_status_checks_policy": True,
                    "bypass_actors": [
                        {
                            "actor_id": 41,
                            "actor_type": "Integration",
                            "bypass_mode": "always",
                        }
                    ],
                },
                "release_tag_creation": {
                    "enforcement": "active",
                    "target": "tag",
                    "ref": "refs/tags/v*",
                    "rules": ["creation"],
                    "bypass_actors": [
                        {
                            "actor_id": 42,
                            "actor_type": "Integration",
                            "bypass_mode": "always",
                        }
                    ],
                },
                "release_tag_immutable": {
                    "enforcement": "active",
                    "target": "tag",
                    "ref": "refs/tags/v*",
                    "rules": ["deletion", "update"],
                    "bypass_actors": [],
                },
            },
            "actions_permissions": {
                "default_workflow_permissions": "read",
                "can_approve_pull_request_reviews": False,
            },
            "immutable_releases": {"enabled": True, "enforced_by_owner": True},
            "environments": {
                "npm": {
                    "can_admins_bypass": False,
                    "deployment_branch_policy": {
                        "protected_branches": False,
                        "custom_branch_policies": True,
                    },
                    "required_reviewers": [{"type": "Team", "id": 51}],
                    "selected_refs": [
                        {"type": "branch", "name": "main"},
                        {"type": "tag", "name": "v*"},
                    ],
                },
                "crates-io": {
                    "can_admins_bypass": False,
                    "deployment_branch_policy": {
                        "protected_branches": True,
                        "custom_branch_policies": False,
                    },
                    "required_reviewers": [{"type": "Team", "id": 52}],
                    "selected_refs": None,
                },
                "release-finalize": {
                    "can_admins_bypass": False,
                    "deployment_branch_policy": {
                        "protected_branches": True,
                        "custom_branch_policies": False,
                    },
                    "required_reviewers": [{"type": "Team", "id": 53}],
                    "selected_refs": None,
                },
            },
        }

    @staticmethod
    def bom(
        release_sha: str,
        *,
        source_sha: str | None = None,
    ) -> dict[str, object]:
        exact_source = release_sha if source_sha is None else source_sha
        version_domains = finalize_release.version_domains
        catalog = version_domains.release_catalog_entries()
        publications = []
        for entry in catalog:
            name = entry["name"]
            digest = "sha256:" + "a" * 64
            publications.append(
                {
                    "package_id": f"cargo:{name}",
                    "name": name,
                    "version": "0.2.0",
                    "publication": {
                        "kind": "cargo",
                        "registry": version_domains.CRATES_REGISTRY,
                        "registry_identity": f"https://crates.io/crates/{name}/0.2.0",
                        "content_digest": digest,
                        "provenance": {
                            "kind": "registry-checksum",
                            "checksum": digest,
                            "download_url": (
                                f"https://static.crates.io/crates/{name}/"
                                f"{name}-0.2.0.crate"
                            ),
                        },
                    },
                }
            )
        npm_digest = "d" * 128
        integrity = "sha512-" + base64.b64encode(bytes.fromhex(npm_digest)).decode()
        for name in ("cymule", "@cymule/sdk"):
            encoded = urllib.parse.quote(name, safe="")
            publications.append(
                {
                    "package_id": f"npm:{name}",
                    "name": name,
                    "version": "0.2.0",
                    "publication": {
                        "kind": "npm",
                        "registry": version_domains.NPM_REGISTRY,
                        "registry_identity": (
                            f"{version_domains.NPM_REGISTRY.rstrip('/')}/"
                            f"{encoded}/0.2.0"
                        ),
                        "content_digest": f"sha512:{npm_digest}",
                        "provenance": {
                            "kind": "sigstore",
                            "sha1": "sha1:" + "c" * 40,
                            "integrity": integrity,
                            "tarball_url": (
                                f"{version_domains.NPM_REGISTRY}{encoded}/-/package.tgz"
                            ),
                            "attestations_url": (
                                f"{version_domains.NPM_REGISTRY}-/attestations"
                            ),
                            "bundle_digest": "sha256:" + "1" * 64,
                            "statement_digest": "sha256:" + "2" * 64,
                            "certificate_identity": (
                                version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY
                            ),
                            "certificate_issuer": (
                                version_domains.NPM_SIGSTORE_CERTIFICATE_ISSUER
                            ),
                            "predicate_type": version_domains.NPM_SLSA_PROVENANCE,
                            "workflow_ref": "refs/heads/main",
                            "source_sha": exact_source,
                            "signer_ref": (
                                version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY
                            ),
                            "signer_sha": "e" * 40,
                        },
                    },
                }
            )
        return version_domains.build_bom(
            version_domains.load_registry(),
            exact_source,
            exact_source,
            publications,
            catalog=catalog,
        )

    class FakeGh:
        def __init__(self, *, lose_responses: bool = False) -> None:
            self.releases: dict[str, dict[str, object]] = {}
            self.assets: dict[str, bytes] = {}
            self.asset_ids: dict[str, int] = {}
            self.latest_tags: set[str] = set()
            self.extra_assets: list[dict[str, str]] = []
            self.lose_responses = lose_responses
            self.calls: list[str] = []
            self.fence_calls = 0
            self.attestation_calls = 0
            self.control_plane_calls = 0
            self.next_id = 1
            self.next_asset_id = 10_000
            self.asset_name = "bom.json"

        @property
        def release(self):
            return self.releases.get("v0.2.0")

        @release.setter
        def release(self, value) -> None:
            if value is None:
                self.releases.pop("v0.2.0", None)
                self.assets.pop("v0.2.0", None)
                self.asset_ids.pop("v0.2.0", None)
                self.latest_tags.discard("v0.2.0")
                return
            self.add_release("v0.2.0", value=value)

        @property
        def asset(self):
            return self.assets.get("v0.2.0")

        @asset.setter
        def asset(self, value) -> None:
            if value is None:
                self.assets.pop("v0.2.0", None)
            else:
                self.assets["v0.2.0"] = value
                self.asset_ids.setdefault("v0.2.0", self.next_asset_id)
                if self.asset_ids["v0.2.0"] == self.next_asset_id:
                    self.next_asset_id += 1

        def add_release(
            self,
            tag: str,
            *,
            value: dict[str, object] | None = None,
            latest: bool = False,
            asset: bytes | None = None,
        ) -> dict[str, object]:
            if value is None:
                version = tag.removeprefix("v")
                value = {
                    "tagName": tag,
                    "name": f"Cymule {version}",
                    "isDraft": False,
                    "isPrerelease": False,
                    "body": "exact notes\n",
                }
            current = dict(value)
            current.setdefault("tagName", tag)
            current.setdefault("isImmutable", not current.get("isDraft", False))
            current.setdefault("_id", self.next_id)
            if current["_id"] == self.next_id:
                self.next_id += 1
            self.releases[tag] = current
            if asset is not None:
                self.assets[tag] = asset
                self.asset_ids[tag] = self.next_asset_id
                self.next_asset_id += 1
            if latest:
                self.latest_tags = {tag}
            return current

        def fence(self) -> None:
            self.fence_calls += 1

        def attest(self) -> None:
            self.attestation_calls += 1

        def control_plane(self) -> None:
            self.control_plane_calls += 1

        def __call__(self, arguments):
            command = " ".join(arguments)
            self.calls.append(command)
            action = tuple(arguments[:2])
            if arguments[0] == "api":
                endpoint = arguments[-1]
                if "/assets?per_page=100" in endpoint:
                    release_id = int(endpoint.split("/releases/", 1)[1].split("/", 1)[0])
                    tag = next(
                        (
                            candidate
                            for candidate, release in self.releases.items()
                            if release["_id"] == release_id
                        ),
                        None,
                    )
                    if tag is None:
                        return subprocess.CompletedProcess(arguments, 1, "", "missing")
                    values = []
                    if tag in self.assets:
                        payload = self.assets[tag]
                        values.append(
                            {
                                "id": self.asset_ids[tag],
                                "name": self.asset_name,
                                "size": len(payload),
                                "digest": "sha256:" + hashlib.sha256(payload).hexdigest(),
                                "state": "uploaded",
                            }
                        )
                    if tag == "v0.2.0":
                        for index, extra in enumerate(self.extra_assets):
                            values.append(
                                {
                                    "id": 20_000 + index,
                                    "name": extra["name"],
                                    "size": 1,
                                    "digest": "sha256:" + "0" * 64,
                                    "state": "uploaded",
                                }
                            )
                    return subprocess.CompletedProcess(
                        arguments, 0, json.dumps([values]), ""
                    )
                values = [
                    {
                        "id": release["_id"],
                        "tag_name": tag,
                        "draft": release["isDraft"],
                        "prerelease": release["isPrerelease"],
                    }
                    for tag, release in self.releases.items()
                ]
                return subprocess.CompletedProcess(arguments, 0, json.dumps([values]), "")
            if action == ("release", "list"):
                values = [
                    {
                        "tagName": tag,
                        "isDraft": release["isDraft"],
                        "isPrerelease": release["isPrerelease"],
                        "isLatest": tag in self.latest_tags,
                    }
                    for tag, release in self.releases.items()
                ]
                limit = int(arguments[arguments.index("--limit") + 1])
                return subprocess.CompletedProcess(
                    arguments, 0, json.dumps(values[:limit]), ""
                )
            if action == ("release", "view"):
                tag = arguments[2]
                release = self.releases.get(tag)
                if release is None:
                    return subprocess.CompletedProcess(
                        arguments, 1, "", finalize_release.GH_RELEASE_NOT_FOUND
                    )
                assets = [] if tag not in self.assets else [{"name": self.asset_name}]
                extras = self.extra_assets if tag == "v0.2.0" else []
                value = {
                    key: item for key, item in release.items() if key != "_id"
                }
                value["assets"] = [*assets, *extras]
                return subprocess.CompletedProcess(arguments, 0, json.dumps(value), "")
            if action == ("release", "create"):
                tag = arguments[2]
                if tag not in self.releases:
                    notes = pathlib.Path(arguments[arguments.index("--notes-file") + 1]).read_text()
                    self.add_release(
                        tag,
                        value={
                            "tagName": tag,
                            "name": arguments[arguments.index("--title") + 1],
                            "isDraft": True,
                            "isPrerelease": False,
                            "body": notes,
                        },
                    )
                return subprocess.CompletedProcess(
                    arguments, 1 if self.lose_responses else 0, "", "lost"
                )
            if action == ("release", "upload"):
                tag = arguments[2]
                source = arguments[3].split("#", 1)[0]
                if tag not in self.assets:
                    self.assets[tag] = pathlib.Path(source).read_bytes()
                    self.asset_ids[tag] = self.next_asset_id
                    self.next_asset_id += 1
                return subprocess.CompletedProcess(
                    arguments, 1 if self.lose_responses else 0, "", "lost"
                )
            if action == ("release", "download"):
                tag = arguments[2]
                if tag not in self.assets:
                    return subprocess.CompletedProcess(arguments, 1, "", "missing")
                output = pathlib.Path(arguments[arguments.index("--output") + 1])
                output.write_bytes(self.assets[tag])
                return subprocess.CompletedProcess(arguments, 0, "", "")
            if action == ("release", "edit"):
                tag = arguments[2]
                release = self.releases.get(tag)
                if release is not None:
                    if "--draft=false" in arguments:
                        release["isDraft"] = False
                        release["isImmutable"] = True
                    if "--latest=true" in arguments:
                        self.latest_tags = {tag}
                    elif "--latest=false" in arguments:
                        self.latest_tags.discard(tag)
                return subprocess.CompletedProcess(
                    arguments, 1 if self.lose_responses else 0, "", "lost"
                )
            raise AssertionError(arguments)

    def finalize(self, fake, directory: pathlib.Path, version: str = "0.2.0") -> None:
        notes = directory / "notes.md"
        notes.write_text("exact notes\n")
        asset = directory / "bom.json"
        asset.write_bytes(b"exact bom\n")
        finalize_release.converge_release(
            repository="cymule-framework/cymule",
            tag=f"v{version}",
            title=f"Cymule {version}",
            notes_path=notes,
            asset_path=asset,
            asset_name="bom.json",
            assert_control_plane=fake.control_plane,
            assert_fence=fake.fence,
            assert_attestation=fake.attest,
            invoke=fake,
        )

    def create_control_plane_receipt(
        self, directory: pathlib.Path, *, observed_at: dt.datetime
    ) -> pathlib.Path:
        return github_settings.create_control_plane_receipt(
            output=directory / "control-plane-receipt.json",
            repository="cymule-framework/cymule",
            run_id="1001",
            run_attempt="2",
            controller_sha="a" * 40,
            release_sha="b" * 40,
            release_tag_sha=self.RELEASE_TAG_SHA,
            settings_snapshot=self.settings_snapshot(),
            observed_at=observed_at,
        )

    def assert_control_plane_receipt(
        self, path: pathlib.Path, *, now: dt.datetime
    ) -> None:
        finalize_release.assert_control_plane_receipt(
            path,
            repository="cymule-framework/cymule",
            run_id="1001",
            run_attempt="2",
            controller_sha="a" * 40,
            release_sha="b" * 40,
            release_tag_sha=self.RELEASE_TAG_SHA,
            now=now,
        )

    def test_draft_asset_and_publish_response_loss_converges(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake = self.FakeGh(lose_responses=True)
            self.finalize(fake, pathlib.Path(temporary))
            self.assertFalse(fake.release["isDraft"])
            self.assertEqual(fake.asset, b"exact bom\n")
            self.assertEqual(fake.latest_tags, {"v0.2.0"})
            self.assertEqual(fake.fence_calls, 4)
            self.assertEqual(fake.attestation_calls, 4)
            self.assertEqual(fake.control_plane_calls, 4)
            mutations = [call for call in fake.calls if any(word in call for word in (" create ", " upload ", " edit "))]
            self.assertTrue(any("--latest=true" in call for call in mutations))
            self.finalize(fake, pathlib.Path(temporary))
            self.assertEqual(fake.fence_calls, 5)
            self.assertEqual(fake.attestation_calls, 5)
            self.assertEqual(fake.control_plane_calls, 5)
            self.assertEqual(
                mutations,
                [call for call in fake.calls if any(word in call for word in (" create ", " upload ", " edit "))],
            )

    def test_draft_recovers_under_a_new_controller_without_replacing_the_bom(self) -> None:
        release_sha = "b" * 40
        old_controller = "a" * 40
        new_controller = "c" * 40
        current_main = old_controller
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            bom = directory / "bom.json"
            value = self.bom(release_sha)
            self.assertNotIn("controller_sha", value)
            bom.write_text(json.dumps(value) + "\n", encoding="utf-8")
            fake = self.FakeGh()
            stages = []
            attestors = []

            for controller in (old_controller, new_controller):
                stage = directory / controller
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller,
                    notes_path=notes,
                    asset_path=bom,
                    output=stage,
                )
                frozen = finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller,
                )
                stages.append(frozen)
                fake.asset_name = frozen.asset_name

                def fence() -> None:
                    nonlocal current_main
                    fake.fence()
                    if fake.fence_calls == 3:
                        current_main = new_controller
                    if controller != current_main:
                        raise ValueError("remote public main moved from the release controller")

                def attest() -> None:
                    attestors.append((controller, frozen.asset_path.read_bytes()))

                def converge() -> None:
                    finalize_release.converge_release(
                        repository="cymule-framework/cymule",
                        tag=frozen.tag,
                        title=frozen.title,
                        notes_path=frozen.notes_path,
                        asset_path=frozen.asset_path,
                        asset_name=frozen.asset_name,
                        assert_control_plane=fake.control_plane,
                        assert_fence=fence,
                        assert_attestation=attest,
                        invoke=fake,
                    )

                if controller == old_controller:
                    with self.assertRaisesRegex(ValueError, "public main moved"):
                        converge()
                    self.assertTrue(fake.release["isDraft"])
                    self.assertEqual(fake.asset, bom.read_bytes())
                    original_asset_id = fake.asset_ids[frozen.tag]
                else:
                    converge()
                    self.assertFalse(fake.release["isDraft"])
                    self.assertEqual(fake.asset_ids[frozen.tag], original_asset_id)
            self.assertEqual(stages[0].asset_path.read_bytes(), stages[1].asset_path.read_bytes())
            self.assertEqual(sum(" upload " in call for call in fake.calls), 1)
            self.assertIn((new_controller, bom.read_bytes()), attestors)
            with self.assertRaisesRegex(ValueError, "controller_sha"):
                finalize_release.load_finalization_stage(
                    directory / old_controller,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=new_controller,
                )

    def test_control_plane_receipt_is_closed_same_run_and_short_lived(self) -> None:
        observed_at = dt.datetime(2026, 8, 28, 1, 2, 3, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            receipt_path = self.create_control_plane_receipt(
                directory, observed_at=observed_at
            )
            self.assert_control_plane_receipt(
                receipt_path, now=observed_at + dt.timedelta(seconds=1)
            )

            with self.assertRaisesRegex(ValueError, "stale"):
                self.assert_control_plane_receipt(
                    receipt_path,
                    now=observed_at
                    + dt.timedelta(
                        seconds=finalize_release.CONTROL_PLANE_RECEIPT_TTL_SECONDS
                    ),
                )
            with self.assertRaisesRegex(ValueError, "not one regular file"):
                self.assert_control_plane_receipt(
                    directory / "missing.json", now=observed_at
                )

    def test_control_plane_receipt_rejects_foreign_or_tampered_authority(self) -> None:
        observed_at = dt.datetime(2026, 8, 28, 1, 2, 3, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            receipt_path = self.create_control_plane_receipt(
                directory, observed_at=observed_at
            )
            with self.assertRaisesRegex(ValueError, "run_id belongs to another"):
                finalize_release.assert_control_plane_receipt(
                    receipt_path,
                    repository="cymule-framework/cymule",
                    run_id="1002",
                    run_attempt="2",
                    controller_sha="a" * 40,
                    release_sha="b" * 40,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    now=observed_at + dt.timedelta(seconds=1),
                )

            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["settings_snapshot"]["immutable_releases"]["enabled"] = False
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "digest does not authenticate"):
                self.assert_control_plane_receipt(
                    receipt_path, now=observed_at + dt.timedelta(seconds=1)
                )

            receipt.pop("receipt_sha256")
            receipt["receipt_sha256"] = github_settings.sha256_identity(receipt)
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "owner-enforced immutable"):
                self.assert_control_plane_receipt(
                    receipt_path, now=observed_at + dt.timedelta(seconds=1)
                )

    def test_control_plane_receipt_rejects_another_or_missing_default_branch(self) -> None:
        observed_at = dt.datetime(2026, 8, 28, 1, 2, 3, tzinfo=dt.timezone.utc)
        for branch in (None, "not-main"):
            with self.subTest(branch=branch), tempfile.TemporaryDirectory() as temporary:
                receipt_path = self.create_control_plane_receipt(
                    pathlib.Path(temporary), observed_at=observed_at
                )
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                if branch is None:
                    receipt["settings_snapshot"].pop("default_branch")
                else:
                    receipt["settings_snapshot"]["default_branch"] = branch
                receipt.pop("receipt_sha256")
                receipt["receipt_sha256"] = github_settings.sha256_identity(receipt)
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "incomplete|default branch"):
                    self.assert_control_plane_receipt(receipt_path, now=observed_at)

    def test_control_plane_receipt_expiry_before_publish_fails_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            fake = self.FakeGh()
            observed_at = dt.datetime(2026, 8, 28, 1, 2, 3, tzinfo=dt.timezone.utc)
            receipt_path = self.create_control_plane_receipt(
                directory, observed_at=observed_at
            )
            verification_times = iter(
                (
                    observed_at + dt.timedelta(seconds=1),
                    observed_at + dt.timedelta(seconds=2),
                    observed_at
                    + dt.timedelta(
                        seconds=finalize_release.CONTROL_PLANE_RECEIPT_TTL_SECONDS
                    ),
                )
            )

            def control_plane() -> None:
                self.assert_control_plane_receipt(
                    receipt_path, now=next(verification_times)
                )

            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            asset = directory / "bom.json"
            asset.write_bytes(b"exact bom\n")
            with self.assertRaisesRegex(ValueError, "receipt is stale"):
                finalize_release.converge_release(
                    repository="cymule-framework/cymule",
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes_path=notes,
                    asset_path=asset,
                    asset_name="bom.json",
                    assert_control_plane=control_plane,
                    assert_fence=fake.fence,
                    assert_attestation=fake.attest,
                    invoke=fake,
                )
            self.assertTrue(fake.release["isDraft"])
            self.assertFalse(fake.release["isImmutable"])
            self.assertFalse(
                any("--draft=false" in call for call in fake.calls)
            )

    def test_converged_noop_still_requires_terminal_remote_fence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            fake = self.FakeGh()
            self.finalize(fake, directory)
            mutation_count = len(
                [
                    call
                    for call in fake.calls
                    if any(word in call for word in (" create ", " upload ", " edit "))
                ]
            )

            def retagged() -> None:
                raise ValueError("remote annotated tag object moved from the frozen tag")

            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            asset = directory / "bom.json"
            asset.write_bytes(b"exact bom\n")
            with self.assertRaisesRegex(ValueError, "tag object moved"):
                finalize_release.converge_release(
                    repository="cymule-framework/cymule",
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes_path=notes,
                    asset_path=asset,
                    asset_name="bom.json",
                    assert_control_plane=lambda: None,
                    assert_fence=retagged,
                    assert_attestation=lambda: None,
                    invoke=fake,
                )
            self.assertEqual(
                len(
                    [
                        call
                        for call in fake.calls
                        if any(
                            word in call
                            for word in (" create ", " upload ", " edit ")
                        )
                    ]
                ),
                mutation_count,
            )

    def test_v3_latest_then_v2_recovery_never_rolls_latest_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            fake = self.FakeGh()
            self.finalize(fake, directory, "0.2.0")
            self.finalize(fake, directory, "0.3.0")
            self.assertEqual(fake.latest_tags, {"v0.3.0"})

            v2_mutations_before = len(
                [call for call in fake.calls if "release edit v0.2.0" in call]
            )
            self.finalize(fake, directory, "0.2.0")
            self.assertEqual(fake.latest_tags, {"v0.3.0"})
            self.assertEqual(
                len([call for call in fake.calls if "release edit v0.2.0" in call]),
                v2_mutations_before,
            )
            self.assertIn(
                "release edit v0.3.0 --repo cymule-framework/cymule "
                "--draft=false --latest=true",
                fake.calls,
            )

    def test_historical_draft_is_published_with_explicit_non_latest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake = self.FakeGh(lose_responses=True)
            fake.add_release(
                "v0.3.0", latest=True, asset=b"newer bom\n"
            )
            self.finalize(fake, pathlib.Path(temporary), "0.2.0")
            self.assertEqual(fake.latest_tags, {"v0.3.0"})
            self.assertIn(
                "release edit v0.2.0 --repo cymule-framework/cymule "
                "--draft=false --latest=false",
                fake.calls,
            )

    def test_publish_failure_without_latest_commit_is_not_a_lost_response_success(self) -> None:
        class RejectedEdit(self.FakeGh):
            def __call__(self, arguments):
                if tuple(arguments[:2]) == ("release", "edit"):
                    self.calls.append(" ".join(arguments))
                    return subprocess.CompletedProcess(arguments, 1, "", "rejected")
                return super().__call__(arguments)

        with tempfile.TemporaryDirectory() as temporary:
            fake = RejectedEdit()
            with self.assertRaisesRegex(ValueError, "did not converge"):
                self.finalize(fake, pathlib.Path(temporary))
            self.assertTrue(fake.release["isDraft"])
            self.assertEqual(fake.latest_tags, set())

    def test_asset_replacement_race_cannot_become_successful_projection(self) -> None:
        class ReplacedBeforePublish(self.FakeGh):
            def __call__(self, arguments):
                if (
                    tuple(arguments[:2]) == ("release", "edit")
                    and "--draft=false" in arguments
                ):
                    tag = arguments[2]
                    self.assets[tag] = b"raced bytes\n"
                    self.asset_ids[tag] = self.next_asset_id
                    self.next_asset_id += 1
                return super().__call__(arguments)

        with tempfile.TemporaryDirectory() as temporary:
            fake = ReplacedBeforePublish()
            with self.assertRaisesRegex(ValueError, "asset identity or bytes"):
                self.finalize(fake, pathlib.Path(temporary))
            self.assertTrue(fake.release["isImmutable"])
            self.assertGreaterEqual(fake.attestation_calls, 3)

    def test_missing_attestation_blocks_the_first_release_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            asset = directory / "bom.json"
            asset.write_bytes(b"exact bom\n")
            fake = self.FakeGh()
            with self.assertRaisesRegex(ValueError, "missing attestation"):
                finalize_release.converge_release(
                    repository="cymule-framework/cymule",
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes_path=notes,
                    asset_path=asset,
                    asset_name="bom.json",
                    assert_control_plane=fake.control_plane,
                    assert_fence=fake.fence,
                    assert_attestation=lambda: (_ for _ in ()).throw(
                        ValueError("missing attestation")
                    ),
                    invoke=fake,
                )
            self.assertEqual(fake.fence_calls, 0)
            self.assertFalse(
                any(
                    word in call
                    for call in fake.calls
                    for word in (" create ", " upload ", " edit ")
                )
            )

    def test_attestation_verifier_binds_bom_workflow_ref_and_controller(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            asset = directory / "bom.json"
            bundle = directory / "bundle.jsonl"
            asset.write_bytes(b"exact bom\n")
            bundle.write_bytes(b"exact bundle\n")
            controller_sha = "a" * 40

            def accepted(arguments):
                self.assertEqual(arguments[:2], ["attestation", "verify"])
                self.assertIn(str(asset), arguments)
                self.assertIn(str(bundle), arguments)
                self.assertIn(
                    "cymule-framework/cymule/.github/workflows/finalize-release.yml",
                    arguments,
                )
                self.assertIn(controller_sha, arguments)
                self.assertIn("refs/heads/main", arguments)
                self.assertEqual(
                    arguments[arguments.index("--signer-digest") + 1], controller_sha
                )
                self.assertEqual(
                    arguments[arguments.index("--source-digest") + 1], controller_sha
                )
                self.assertIn("--deny-self-hosted-runners", arguments)
                return subprocess.CompletedProcess(arguments, 0, '[{"ok":true}]', "")

            finalize_release.verify_bom_attestation(
                repository="cymule-framework/cymule",
                controller_sha=controller_sha,
                asset_path=asset,
                bundle_path=bundle,
                invoke=accepted,
            )

            def empty(arguments):
                return subprocess.CompletedProcess(arguments, 0, "[]", "")

            with self.assertRaisesRegex(ValueError, "returned no authority"):
                finalize_release.verify_bom_attestation(
                    repository="cymule-framework/cymule",
                    controller_sha=controller_sha,
                    asset_path=asset,
                    bundle_path=bundle,
                    invoke=empty,
                )

    def test_release_inventory_rejects_duplicate_multi_latest_and_nonstable_latest(self) -> None:
        record = finalize_release.ReleaseInventoryRecord(
            1, "v0.2.0", False, False, True
        )
        with self.assertRaisesRegex(ValueError, "duplicate"):
            finalize_release.validate_release_inventory((record, record))

        with self.assertRaisesRegex(ValueError, "multiple Latest"):
            finalize_release.validate_release_inventory(
                (
                    record,
                    finalize_release.ReleaseInventoryRecord(
                        2, "v0.3.0", False, False, True
                    ),
                )
            )

        with self.assertRaisesRegex(ValueError, "non-stable"):
            finalize_release.validate_release_inventory(
                (
                    finalize_release.ReleaseInventoryRecord(
                        1, "nightly", False, False, True
                    ),
                    finalize_release.ReleaseInventoryRecord(
                        2, "v0.2.0", False, False, False
                    ),
                )
            )

    def test_complete_release_inventory_uses_rest_pagination_and_exact_projection(self) -> None:
        fake = self.FakeGh()
        fake.add_release("v0.2.0", latest=True)
        fake.add_release("nightly", value={
            "tagName": "nightly",
            "name": "Nightly",
            "isDraft": True,
            "isPrerelease": True,
            "body": "nightly\n",
        })
        records = finalize_release.release_inventory(
            fake, "cymule-framework/cymule"
        )
        self.assertEqual({record.tag for record in records}, {"v0.2.0", "nightly"})
        self.assertIn("--paginate", fake.calls[0])
        self.assertIn("--slurp", fake.calls[0])
        self.assertIn("--limit 3", fake.calls[1])

    def test_published_release_cannot_hide_a_missing_or_different_bom(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            for asset, message in (
                (None, "missing"),
                (b"other", "asset identity or bytes"),
            ):
                fake = self.FakeGh()
                fake.release = {
                    "tagName": "v0.2.0", "name": "Cymule 0.2.0", "isDraft": False,
                    "isPrerelease": False, "body": "exact notes\n",
                }
                fake.asset = asset
                with self.subTest(asset=asset):
                    with self.assertRaisesRegex(ValueError, message):
                        self.finalize(fake, directory)

    def test_release_rejects_every_unfrozen_asset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake = self.FakeGh()
            fake.release = {
                "tagName": "v0.2.0",
                "name": "Cymule 0.2.0",
                "isDraft": False,
                "isPrerelease": False,
                "body": "exact notes\n",
            }
            fake.asset = b"exact bom\n"
            fake.extra_assets = [{"name": "unfrozen-binary.zip"}]
            with self.assertRaisesRegex(ValueError, "unexpected assets"):
                self.finalize(fake, pathlib.Path(temporary))
            self.assertEqual(fake.fence_calls, 0)

    def test_ambiguous_release_read_never_authorizes_create(self) -> None:
        calls = []

        def unavailable(arguments):
            calls.append(arguments)
            return subprocess.CompletedProcess(arguments, 1, "", "network unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            asset = directory / "bom.json"
            asset.write_bytes(b"exact bom\n")
            with self.assertRaisesRegex(ValueError, "readback failed"):
                finalize_release.converge_release(
                    repository="cymule-framework/cymule",
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes_path=notes,
                    asset_path=asset,
                    asset_name="bom.json",
                    assert_control_plane=lambda: self.fail(
                        "read failure reached control-plane authority"
                    ),
                    assert_fence=lambda: self.fail("read failure reached mutation fence"),
                    assert_attestation=lambda: self.fail(
                        "read failure reached attestation authority"
                    ),
                    invoke=unavailable,
                )
        self.assertEqual(len(calls), 1)

    def test_remote_release_fence_matches_main_tag_object_and_peeled_commit(self) -> None:
        controller_sha = "a" * 40
        release_sha = "b" * 40
        tag_object = "c" * 40

        def invoke(arguments):
            self.assertEqual(arguments[0], "ls-remote")
            return subprocess.CompletedProcess(
                arguments,
                0,
                "".join(
                    (
                        f"{controller_sha}\trefs/heads/main\n",
                        f"{tag_object}\trefs/tags/v0.2.0\n",
                        f"{release_sha}\trefs/tags/v0.2.0^{{}}\n",
                    )
                ),
                "",
            )

        finalize_release.assert_remote_release_fence(
            repository="cymule-framework/cymule",
            tag="v0.2.0",
            controller_sha=controller_sha,
            release_sha=release_sha,
            release_tag_sha=tag_object,
            invoke=invoke,
        )

        def stale(arguments):
            result = invoke(arguments)
            return subprocess.CompletedProcess(
                arguments,
                0,
                result.stdout.replace(controller_sha, "d" * 40, 1),
                "",
            )

        with self.assertRaisesRegex(ValueError, "public main moved"):
            finalize_release.assert_remote_release_fence(
                repository="cymule-framework/cymule",
                tag="v0.2.0",
                controller_sha=controller_sha,
                release_sha=release_sha,
                release_tag_sha=tag_object,
                invoke=stale,
            )

        def retagged(arguments):
            result = invoke(arguments)
            return subprocess.CompletedProcess(
                arguments,
                0,
                result.stdout.replace(tag_object, "e" * 40, 1),
                "",
            )

        with self.assertRaisesRegex(ValueError, "tag object moved"):
            finalize_release.assert_remote_release_fence(
                repository="cymule-framework/cymule",
                tag="v0.2.0",
                controller_sha=controller_sha,
                release_sha=release_sha,
                release_tag_sha=tag_object,
                invoke=retagged,
            )

        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            asset = directory / "bom.json"
            asset.write_bytes(b"exact bom\n")
            fake = self.FakeGh()
            with self.assertRaisesRegex(ValueError, "tag object moved"):
                finalize_release.converge_release(
                    repository="cymule-framework/cymule",
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes_path=notes,
                    asset_path=asset,
                    asset_name="bom.json",
                    assert_control_plane=lambda: None,
                    assert_fence=lambda: finalize_release.assert_remote_release_fence(
                        repository="cymule-framework/cymule",
                        tag="v0.2.0",
                        controller_sha=controller_sha,
                        release_sha=release_sha,
                        release_tag_sha=tag_object,
                        invoke=retagged,
                    ),
                    assert_attestation=lambda: None,
                    invoke=fake,
                )
            self.assertFalse(
                any(
                    word in call
                    for call in fake.calls
                    for word in (" create ", " upload ", " edit ")
                )
            )

    def test_bom_readback_is_owned_by_the_convergence_controller(self) -> None:
        workflow = (ROOT / ".github/workflows/finalize-release.yml").read_text()
        release_workflows.verify_finalize_bom_readback(workflow)
        controller = (ROOT / "scripts/finalize_release.py").read_text()
        with self.assertRaisesRegex(ValueError, "omits transitions"):
            release_workflows.verify_finalize_bom_readback(
                workflow,
                controller.replace('"release",\n                "upload"', '"release", "missing"'),
            )
        with self.assertRaisesRegex(
            ValueError, "omits transitions|validate control plane, attest, and fence"
        ):
            release_workflows.verify_finalize_bom_readback(
                workflow, controller.replace("        assert_fence()\n", "", 1)
            )
        with self.assertRaisesRegex(
            ValueError, "validate control plane, attest, and fence"
        ):
            release_workflows.verify_finalize_bom_readback(
                workflow,
                controller.replace(
                    "        assert_attestation()\n"
                    "        assert_fence()\n"
                    "        assert_control_plane()\n",
                    "        assert_control_plane()\n"
                    "        assert_attestation()\n"
                    "        assert_fence()\n",
                    1,
                ),
            )
        for fragment in (
            "FINALIZATION_STAGE_SCHEMA = 2",
            '"release_tag_sha": release_tag_sha',
            "if refs[tag_ref] != release_tag_sha:",
            '"--paginate"',
            '"--latest=false"',
            "verify_bom_attestation(",
            "version_domains.validate_release_bom(",
            "version_domains.validate_release_bom_projection(",
            "exact_release_asset(",
            'if not final["isImmutable"]:',
            "terminal=True",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "omits transitions"
            ):
                release_workflows.verify_finalize_bom_readback(
                    workflow, controller.replace(fragment, "missing-terminal-closure", 1)
                )

        main_offset = controller.rfind("\ndef main()")
        self.assertGreater(main_offset, 0)
        reordered = controller[:main_offset] + controller[main_offset:].replace(
            "        verify_bom_attestation(",
            "        validate_attested_bom_projection(",
            1,
        )
        with self.assertRaisesRegex(ValueError, "attestation must precede"):
            release_workflows.verify_finalize_bom_readback(workflow, reordered)

    def test_release_readback_rejects_duplicate_json_members(self) -> None:
        def duplicate(arguments):
            return subprocess.CompletedProcess(
                arguments,
                0,
                '{"tagName":"v0.2.0","tagName":"v0.2.1"}',
                "",
            )

        with self.assertRaisesRegex(ValueError, "repeats object member"):
            finalize_release.release_view(
                duplicate, "cymule-framework/cymule", "v0.2.0"
            )

    def test_release_metadata_rejects_numeric_boolean_disguises(self) -> None:
        release = {
            "tagName": "v0.2.0",
            "name": "Cymule 0.2.0",
            "isDraft": False,
            "isPrerelease": False,
            "isImmutable": True,
            "body": "notes\n",
            "assets": [],
        }
        for field in ("isDraft", "isPrerelease", "isImmutable"):
            malformed = {**release, field: 0}
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, "state is missing or malformed"
            ):
                finalize_release.validate_metadata(
                    malformed,
                    tag="v0.2.0",
                    title="Cymule 0.2.0",
                    notes="notes\n",
                )

    def test_release_metadata_requires_exact_note_bytes(self) -> None:
        release = {
            "tagName": "v0.2.0",
            "name": "Cymule 0.2.0",
            "isDraft": True,
            "isPrerelease": False,
            "isImmutable": False,
            "body": "notes\n\n",
            "assets": [],
        }
        with self.assertRaisesRegex(ValueError, "notes differ"):
            finalize_release.validate_metadata(
                release,
                tag="v0.2.0",
                title="Cymule 0.2.0",
                notes="notes\n",
            )

    def test_terminal_stage_binds_identity_and_exact_data_bytes(self) -> None:
        release_sha = "a" * 40
        controller_sha = "b" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            bom = directory / "source-bom.json"
            bom.write_text(
                json.dumps(self.bom(release_sha)) + "\n",
                encoding="utf-8",
            )
            stage = directory / "stage"
            finalize_release.create_finalization_stage(
                repository="cymule-framework/cymule",
                version="0.2.0",
                release_sha=release_sha,
                release_tag_sha=self.RELEASE_TAG_SHA,
                controller_sha=controller_sha,
                notes_path=notes,
                asset_path=bom,
                output=stage,
            )
            frozen = finalize_release.load_finalization_stage(
                stage,
                repository="cymule-framework/cymule",
                version="0.2.0",
                release_sha=release_sha,
                release_tag_sha=self.RELEASE_TAG_SHA,
                controller_sha=controller_sha,
            )
            self.assertEqual(frozen.tag, "v0.2.0")
            self.assertEqual(frozen.release_tag_sha, self.RELEASE_TAG_SHA)
            self.assertEqual(frozen.notes_path.read_text(), "exact notes\n")

            manifest_path = stage / finalize_release.FINALIZATION_MANIFEST_NAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema_version"], 2)
            manifest["release_tag_sha"] = "e" * 40
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "release_tag_sha"):
                finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )
            manifest["release_tag_sha"] = self.RELEASE_TAG_SHA
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            stage_alias = directory / "stage-alias"
            stage_alias.symlink_to(stage, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "not one real directory"):
                finalize_release.load_finalization_stage(
                    stage_alias,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )

            unexpected = stage / "unexpected"
            unexpected.write_text("not closed\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unexpected files"):
                finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )
            unexpected.unlink()

            frozen_notes = frozen.notes_path.read_bytes()
            frozen.notes_path.unlink()
            outside_notes = directory / "outside-notes.md"
            outside_notes.write_bytes(frozen_notes)
            frozen.notes_path.symlink_to(outside_notes)
            with self.assertRaisesRegex(ValueError, "not one regular file"):
                finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )
            frozen.notes_path.unlink()
            frozen.notes_path.write_bytes(frozen_notes)

            frozen.notes_path.write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "notes bytes changed"):
                finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )

    def test_terminal_stage_rejects_foreign_controller_and_bom_generation(self) -> None:
        release_sha = "a" * 40
        controller_sha = "d" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            bom = directory / "bom.json"
            bom.write_text(
                json.dumps(
                    self.bom(
                        release_sha,
                        source_sha="b" * 40,
                    )
                ),
                encoding="utf-8",
            )
            notes = directory / "notes.md"
            notes.write_text("notes\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source_sha"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "stage",
                )

            obsolete_bom = self.bom(release_sha)
            obsolete_bom["controller_sha"] = "e" * 40
            bom.write_text(json.dumps(obsolete_bom), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "top-level shape"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "stage-controller",
                )

            open_package = self.bom(release_sha)
            open_package["packages"][0].pop("publication")
            bom.write_text(json.dumps(open_package), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "package record"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "stage-open-package",
                )

            unordered = self.bom(release_sha)
            unordered["packages"].reverse()
            bom.write_text(json.dumps(unordered), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "canonical package_id order"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "stage-unordered-packages",
                )

            wrong_null = self.bom(release_sha)
            next(
                package
                for package in wrong_null["packages"]
                if package["publication"] is None
            )["name"] = "other/module"
            bom.write_text(json.dumps(wrong_null), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "complete exact package catalog"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "stage-wrong-null",
                )

            for field in ("tarball_url", "attestations_url"):
                foreign_registry = self.bom(release_sha)
                npm_package = next(
                    package
                    for package in foreign_registry["packages"]
                    if package["package_id"] == "npm:cymule"
                )
                npm_package["publication"]["provenance"][field] = (
                    "https://registry.npmjs.org.attacker.invalid/-/forged"
                )
                bom.write_text(json.dumps(foreign_registry), encoding="utf-8")
                with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "npm publication evidence is not exact"
                ):
                    finalize_release.create_finalization_stage(
                        repository="cymule-framework/cymule",
                        version="0.2.0",
                        release_sha=release_sha,
                        release_tag_sha=self.RELEASE_TAG_SHA,
                        controller_sha=controller_sha,
                        notes_path=notes,
                        asset_path=bom,
                        output=directory / f"stage-foreign-{field}",
                    )

    def test_terminal_stage_schema_version_is_an_exact_integer(self) -> None:
        release_sha = "a" * 40
        controller_sha = "b" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            bom = directory / "bom.json"
            bom.write_text(
                json.dumps(self.bom(release_sha)),
                encoding="utf-8",
            )
            stage = directory / "stage"
            manifest_path = finalize_release.create_finalization_stage(
                repository="cymule-framework/cymule",
                version="0.2.0",
                release_sha=release_sha,
                release_tag_sha=self.RELEASE_TAG_SHA,
                controller_sha=controller_sha,
                notes_path=notes,
                asset_path=bom,
                output=stage,
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["schema_version"] = True
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "schema version is not an integer"):
                finalize_release.load_finalization_stage(
                    stage,
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                )

    def test_finalizer_rejects_noncanonical_stable_versions_and_symlink_inputs(self) -> None:
        for version in (
            "01.2.3",
            "1.2.3-rc.1",
            "1.2.3\nforged=accepted",
            "１.２.３",
        ):
            with self.subTest(version=version), self.assertRaisesRegex(
                ValueError, "exact stable SemVer"
            ):
                finalize_release.release_identity(version)

        release_sha = "a" * 40
        controller_sha = "b" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            notes = directory / "notes.md"
            notes.write_text("exact notes\n", encoding="utf-8")
            notes_alias = directory / "notes-alias.md"
            notes_alias.symlink_to(notes)
            bom = directory / "bom.json"
            bom.write_text(
                json.dumps(self.bom(release_sha)),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "distinct annotated tag object"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=release_sha,
                    controller_sha=controller_sha,
                    notes_path=notes,
                    asset_path=bom,
                    output=directory / "same-object-stage",
                )
            with self.assertRaisesRegex(ValueError, "not one regular file"):
                finalize_release.create_finalization_stage(
                    repository="cymule-framework/cymule",
                    version="0.2.0",
                    release_sha=release_sha,
                    release_tag_sha=self.RELEASE_TAG_SHA,
                    controller_sha=controller_sha,
                    notes_path=notes_alias,
                    asset_path=bom,
                    output=directory / "stage",
                )


class NpmReleaseTests(unittest.TestCase):
    def load_controller(self, workspace: pathlib.Path):
        specification = importlib.util.spec_from_file_location(
            "npm_release_workspace_test", ROOT / "scripts" / "npm_release.py"
        )
        assert specification is not None and specification.loader is not None
        module = importlib.util.module_from_spec(specification)
        sys.modules[specification.name] = module
        with mock.patch.dict(
            os.environ, {"CYMULE_RELEASE_WORKSPACE": str(workspace)}
        ):
            specification.loader.exec_module(module)
        return module

    def test_current_controller_reads_the_exact_payload_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = pathlib.Path(temporary).resolve()
            payload_version_domains = workspace / "scripts" / "version_domains.py"
            payload_version_domains.parent.mkdir()
            payload_version_domains.write_text(
                'raise RuntimeError("payload controller executed")\n',
                encoding="utf-8",
            )
            controller = self.load_controller(workspace)
            self.assertEqual(controller.CONTROL_ROOT, ROOT)
            self.assertEqual(controller.ROOT, workspace)
            self.assertEqual(
                pathlib.Path(controller.version_domains.__file__).resolve(),
                ROOT / "scripts" / "version_domains.py",
            )
            with mock.patch.object(
                controller.version_domains,
                "registry_digest",
                return_value="sha256:" + "3" * 64,
            ) as digest:
                self.assertEqual(
                    controller.version_registry_digest(), "sha256:" + "3" * 64
                )
                digest.assert_called_once_with(workspace)

    def test_release_workspace_must_be_absolute(self) -> None:
        specification = importlib.util.spec_from_file_location(
            "npm_release_relative_workspace_test",
            ROOT / "scripts" / "npm_release.py",
        )
        assert specification is not None and specification.loader is not None
        module = importlib.util.module_from_spec(specification)
        sys.modules[specification.name] = module
        with mock.patch.dict(
            os.environ, {"CYMULE_RELEASE_WORKSPACE": "relative/tag"}
        ):
            with self.assertRaisesRegex(
                ValueError, "CYMULE_RELEASE_WORKSPACE must be an absolute path"
            ):
                specification.loader.exec_module(module)

    def test_registry_digest_delegates_to_the_version_authority(self) -> None:
        with mock.patch.object(
            npm_release.version_domains,
            "registry_digest",
            return_value="sha256:" + "1" * 64,
        ) as digest:
            self.assertEqual(
                npm_release.version_registry_digest(), "sha256:" + "1" * 64
            )
            digest.assert_called_once_with(npm_release.ROOT)

    def make_stage(
        self,
        directory: pathlib.Path,
        release_sha: str,
        publish_config: dict[str, object] | None = None,
    ) -> pathlib.Path:
        archive = directory / "cymule-0.2.0.tgz"
        package_json = json.dumps(
            {
                "name": "cymule",
                "version": "0.2.0",
                "publishConfig": {
                    "access": "public",
                    "provenance": True,
                    "registry": npm_release.PUBLISH_REGISTRY,
                    "tag": npm_release.PUBLISH_DIST_TAG,
                }
                if publish_config is None
                else publish_config,
            }
        ).encode()
        with tarfile.open(archive, "w:gz") as bundle:
            info = tarfile.TarInfo("package/package.json")
            info.size = len(package_json)
            bundle.addfile(info, io.BytesIO(package_json))
        identity = npm_release.archive_identity(archive)
        manifest = directory / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": "cymule.npm-release-stage/3",
                    "package": "cymule",
                    "version": "0.2.0",
                    "dist_tag": npm_release.PUBLISH_DIST_TAG,
                    "release_sha": release_sha,
                    "version_domain_registry_digest": npm_release.version_registry_digest(),
                    "archive": archive.name,
                    **identity,
                }
            ),
            encoding="utf-8",
        )
        return manifest

    def make_provenance_payload(
        self,
        release_sha: str,
        sha512: str,
        *,
        workflow_ref: str = "refs/heads/main",
    ) -> dict[str, object]:
        statement = {
            "_type": npm_release.INTOTO_STATEMENT,
            "subject": [
                {
                    "name": "pkg:npm/cymule@0.2.0",
                    "digest": {"sha512": sha512},
                }
            ],
            "predicateType": npm_release.SLSA_PROVENANCE,
            "predicate": {
                "buildDefinition": {
                    "buildType": npm_release.SLSA_GITHUB_BUILD_TYPE,
                    "externalParameters": {
                        "workflow": {
                            "ref": workflow_ref,
                            "repository": npm_release.REPOSITORY,
                            "path": npm_release.WORKFLOW,
                        }
                    },
                    "internalParameters": {
                        "github": {
                            "event_name": "workflow_dispatch",
                            "repository_id": npm_release.REPOSITORY_ID,
                            "repository_owner_id": npm_release.REPOSITORY_OWNER_ID,
                        }
                    },
                    "resolvedDependencies": [
                        {
                            "uri": f"git+{npm_release.REPOSITORY}@{workflow_ref}",
                            "digest": {"gitCommit": release_sha},
                        }
                    ],
                },
                "runDetails": {
                    "builder": {
                        "id": npm_release.SLSA_GITHUB_BUILDER
                    },
                    "metadata": {
                        "invocationId": f"{npm_release.REPOSITORY}/actions/runs/1/attempts/1"
                    },
                },
            },
        }
        return {
            "attestations": [
                {
                    "predicateType": npm_release.SLSA_PROVENANCE,
                    "bundle": {
                        "mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
                        "verificationMaterial": {},
                        "dsseEnvelope": {
                            "payloadType": npm_release.INTOTO_PAYLOAD,
                            "payload": base64.b64encode(
                                json.dumps(statement).encode()
                            ).decode(),
                            "signatures": [{"sig": "fixture"}],
                        },
                    },
                }
            ]
        }

    def provenance_statement(self, payload: dict[str, object]) -> dict[str, object]:
        attestations = payload["attestations"]
        assert isinstance(attestations, list)
        attestation = attestations[0]
        assert isinstance(attestation, dict)
        bundle = attestation["bundle"]
        assert isinstance(bundle, dict)
        envelope = bundle["dsseEnvelope"]
        assert isinstance(envelope, dict)
        statement = json.loads(base64.b64decode(envelope["payload"]))
        assert isinstance(statement, dict)
        return statement

    def set_provenance_statement(
        self, payload: dict[str, object], statement: dict[str, object]
    ) -> None:
        attestations = payload["attestations"]
        assert isinstance(attestations, list)
        attestation = attestations[0]
        assert isinstance(attestation, dict)
        bundle = attestation["bundle"]
        assert isinstance(bundle, dict)
        envelope = bundle["dsseEnvelope"]
        assert isinstance(envelope, dict)
        envelope["payload"] = base64.b64encode(
            json.dumps(statement).encode()
        ).decode()

    def test_stage_is_bound_to_the_verified_commit_and_bytes(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            manifest = self.make_stage(directory, release_sha)
            npm_release.load_stage(manifest, release_sha)
            with self.assertRaisesRegex(ValueError, "another verified commit"):
                npm_release.load_stage(manifest, "b" * 40)
            staged = json.loads(manifest.read_text())
            staged["version_domain_registry_digest"] = "sha256:" + "0" * 64
            manifest.write_text(json.dumps(staged), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "version-domain generation"):
                npm_release.load_stage(manifest, release_sha)
            manifest = self.make_stage(directory, release_sha)
            staged = json.loads(manifest.read_text())
            staged["dist_tag"] = "next"
            manifest.write_text(json.dumps(staged), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "distribution tag"):
                npm_release.load_stage(manifest, release_sha)
            manifest = self.make_stage(directory, release_sha)
            staged = json.loads(manifest.read_text())
            staged["version"] = "0.2.0\ncontroller_sha=attacker"
            manifest.write_text(json.dumps(staged), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "exact semver"):
                npm_release.load_stage(manifest, release_sha)
            manifest = self.make_stage(directory, release_sha)
            staged = json.loads(manifest.read_text())
            staged["package"] = "attacker-package"
            manifest.write_text(json.dumps(staged), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unsupported package"):
                npm_release.load_stage(manifest, release_sha)
            manifest = self.make_stage(directory, release_sha)
            with directory.joinpath("cymule-0.2.0.tgz").open("ab") as archive:
                archive.write(b"tamper")
            with self.assertRaisesRegex(ValueError, "digest"):
                npm_release.load_stage(manifest, release_sha)

    def test_npm_registry_evidence_binds_bytes_and_historical_signer(self) -> None:
        release_sha = "a" * 40
        signer_sha = "b" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            manifest_path = self.make_stage(directory, release_sha)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            tarball = f"{npm_release.REGISTRY}/cymule/-/cymule-0.2.0.tgz"
            attestations = f"{npm_release.REGISTRY}/-/attestations/cymule@0.2.0"
            provenance = {
                "attestations_url": attestations,
                "bundle_digest": "sha256:" + "1" * 64,
                "statement_digest": "sha256:" + "2" * 64,
                "certificate_identity": npm_release.SIGSTORE_CERTIFICATE_IDENTITY,
                "certificate_issuer": npm_release.SIGSTORE_CERTIFICATE_ISSUER,
                "predicate_type": npm_release.SLSA_PROVENANCE,
                "workflow_ref": "refs/tags/v0.2.0",
                "source_sha": release_sha,
                "signer_ref": npm_release.SIGSTORE_CERTIFICATE_IDENTITY,
                "signer_sha": signer_sha,
            }
            metadata = {
                "dist": {
                    "shasum": manifest["sha1"],
                    "integrity": manifest["integrity"],
                    "tarball": tarball,
                    "attestations": {
                        "provenance": {
                            "predicateType": npm_release.SLSA_PROVENANCE
                        },
                        "url": attestations,
                    },
                }
            }
            archive = directory / manifest["archive"]
            with (
                mock.patch.object(
                    npm_release, "wait_for_registry", return_value=metadata
                ),
                mock.patch.object(
                    npm_release.urllib.request,
                    "urlopen",
                    return_value=io.BytesIO(archive.read_bytes()),
                ),
                mock.patch.object(
                    npm_release, "verify_provenance", return_value=provenance
                ),
                mock.patch.object(npm_release, "verify_existing_stable_state"),
            ):
                evidence = npm_release.verify_registry(
                    manifest_path, release_sha
                )
            self.assertEqual(evidence["package_id"], "npm:cymule")
            self.assertEqual(
                evidence["publication"]["content_digest"],
                f"sha512:{manifest['sha512']}",
            )
            self.assertEqual(
                evidence["publication"]["provenance"]["signer_sha"],
                signer_sha,
            )

    def test_npm_registry_urls_require_the_exact_https_origin(self) -> None:
        self.assertEqual(
            npm_release.require_registry_url(
                "https://registry.npmjs.org/cymule/-/cymule-0.2.0.tgz",
                path_prefix="/",
                label="npm tarball URL",
            ),
            "https://registry.npmjs.org/cymule/-/cymule-0.2.0.tgz",
        )
        for value in (
            "http://registry.npmjs.org/cymule/-/cymule-0.2.0.tgz",
            "https://registry.npmjs.org.attacker.invalid/-/forged",
            "https://attacker@registry.npmjs.org/-/forged",
            "https://registry.npmjs.org/-/forged?redirect=attacker",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "outside the registry authority"
            ):
                npm_release.require_registry_url(
                    value,
                    path_prefix="/",
                    label="npm tarball URL",
                )

    def test_release_tag_uses_the_apps_distinct_verified_bot_user_id(self) -> None:
        app_slug = "cymule-release"
        app_id = 12345
        bot_user_id = 987654
        responses = [
            {"id": app_id, "slug": app_slug},
            {
                "id": bot_user_id,
                "login": f"{app_slug}[bot]",
                "type": "Bot",
                "site_admin": False,
            },
        ]
        with mock.patch.object(
            npm_release, "github_api_json", side_effect=responses
        ) as request:
            self.assertEqual(
                npm_release.github_app_bot_user_id(app_slug, app_id, "token"),
                bot_user_id,
            )
        self.assertNotEqual(app_id, bot_user_id)
        self.assertEqual(
            request.call_args_list,
            [
                mock.call("/apps/cymule-release", "token"),
                mock.call("/users/cymule-release%5Bbot%5D", "token"),
            ],
        )

        malformed = (
            ([{"id": app_id + 1, "slug": app_slug}], "slug and App ID"),
            (
                [
                    {"id": app_id, "slug": app_slug},
                    {
                        "id": bot_user_id,
                        "login": f"{app_slug}[bot]",
                        "type": "User",
                        "site_admin": False,
                    },
                ],
                "bot user identity",
            ),
        )
        for responses, expected in malformed:
            with self.subTest(expected=expected), mock.patch.object(
                npm_release, "github_api_json", side_effect=responses
            ), self.assertRaisesRegex(ValueError, expected):
                npm_release.github_app_bot_user_id(app_slug, app_id, "token")
        for slug in ("", "Uppercase", "ends-"):
            with self.subTest(slug=slug), self.assertRaisesRegex(
                ValueError, "slug is not canonical"
            ):
                npm_release.github_app_bot_user_id(slug, app_id, "token")

    def test_stage_and_close_reject_duplicate_members_and_floats(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            manifest = self.make_stage(directory, release_sha)
            valid = manifest.read_text(encoding="utf-8")
            malformed_values = (
                valid.replace(
                    '"package": "cymule"',
                    '"package": "cymule", "package": "other"',
                    1,
                ),
                valid.replace('"package": "cymule"', '"float": 1.5, "package": "cymule"', 1),
            )
            for malformed in malformed_values:
                with self.subTest(malformed=malformed):
                    manifest.write_text(malformed, encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, "strict I-JSON"):
                        npm_release.load_stage(manifest, release_sha)
                    with self.assertRaisesRegex(ValueError, "strict I-JSON"):
                        npm_release.compare_stages(manifest, manifest)
            manifest.write_text(valid, encoding="utf-8")

            archive = directory / "cymule-0.2.0.tgz"
            package_json = json.dumps(
                {
                    "name": "cymule",
                    "version": "0.2.0",
                    "publishConfig": {
                        "access": "public",
                        "provenance": True,
                        "registry": npm_release.PUBLISH_REGISTRY,
                        "tag": npm_release.PUBLISH_DIST_TAG,
                    },
                }
            ).encode()
            with tarfile.open(archive, "w:gz") as bundle:
                for _ in range(2):
                    info = tarfile.TarInfo("package/package.json")
                    info.size = len(package_json)
                    bundle.addfile(info, io.BytesIO(package_json))
            staged = json.loads(manifest.read_text(encoding="utf-8"))
            staged.update(npm_release.archive_identity(archive))
            manifest.write_text(json.dumps(staged), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unsafe npm archive member"):
                npm_release.load_stage(manifest, release_sha)

    def test_npm_stage_rejects_traversal_symlinks_and_extra_files(self) -> None:
        release_sha = "a" * 40
        for case in ("traversal", "symlink", "extra"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                directory = root / "stage"
                directory.mkdir()
                manifest = self.make_stage(directory, release_sha)
                archive = directory / "cymule-0.2.0.tgz"
                if case == "traversal":
                    value = json.loads(manifest.read_text(encoding="utf-8"))
                    value["archive"] = "../cymule-0.2.0.tgz"
                    manifest.write_text(json.dumps(value), encoding="utf-8")
                    expected = "one basename"
                elif case == "symlink":
                    external = root / archive.name
                    archive.rename(external)
                    archive.symlink_to(external)
                    expected = "regular non-symlink"
                else:
                    directory.joinpath("unowned.txt").write_text(
                        "unowned", encoding="utf-8"
                    )
                    expected = "exact file set"
                with self.assertRaisesRegex(ValueError, expected):
                    npm_release.load_stage(manifest, release_sha)

    def test_npm_publish_fences_refs_after_missing_status_before_write(self) -> None:
        release_sha = "b" * 40
        controller_sha = "a" * 40
        release_tag = "v0.2.0"
        moved_refs = (
            ("c" * 40, release_sha),
            (controller_sha, "d" * 40),
        )
        for observed_main, observed_tag in moved_refs:
            with self.subTest(
                observed_main=observed_main, observed_tag=observed_tag
            ), tempfile.TemporaryDirectory() as temporary:
                directory = pathlib.Path(temporary)
                manifest = self.make_stage(directory, release_sha)
                events: list[str] = []

                def missing(*_args: object) -> str:
                    events.append("status")
                    return "missing"

                def remote(arguments: list[str], **_kwargs: object):
                    events.append("fence")
                    return subprocess.CompletedProcess(
                        arguments,
                        0,
                        (
                            f"{observed_main}\trefs/heads/main\n"
                            f"{observed_tag}\trefs/tags/{release_tag}^{{}}\n"
                        ),
                        "",
                    )

                with (
                    mock.patch.object(
                        npm_release, "registry_status", side_effect=missing
                    ),
                    mock.patch.object(
                        npm_release, "latest_publish_status", return_value="missing"
                    ),
                    mock.patch.object(
                        npm_release,
                        "required_publish_authority",
                        return_value=(controller_sha, release_tag),
                    ),
                    mock.patch.object(npm_release.subprocess, "run", side_effect=remote),
                    mock.patch.object(npm_release, "run_npm_publish") as publish,
                ):
                    with self.assertRaisesRegex(ValueError, "authority moved"):
                        npm_release.publish(manifest, release_sha)
                self.assertEqual(events, ["status", "fence"])
                publish.assert_not_called()

    def test_npm_publish_retains_an_exact_existing_version_without_write(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            with (
                mock.patch.object(
                    npm_release, "registry_status", return_value="exists"
                ),
                mock.patch.object(npm_release, "verify_registry") as verify,
                mock.patch.object(npm_release, "required_publish_authority") as authority,
                mock.patch.object(npm_release, "run_npm_publish") as publish,
            ):
                self.assertEqual(
                    npm_release.publish(manifest, release_sha), "retained"
                )
            verify.assert_called_once_with(manifest, release_sha)
            authority.assert_not_called()
            publish.assert_not_called()

    def test_npm_publish_rejects_a_higher_stable_before_authority_or_write(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            with (
                mock.patch.object(
                    npm_release, "registry_status", return_value="missing"
                ),
                mock.patch.object(
                    npm_release,
                    "latest_publish_status",
                    side_effect=ValueError(
                        "npm registry already contains higher stable version 0.3.0"
                    ),
                ),
                mock.patch.object(npm_release, "required_publish_authority") as authority,
                mock.patch.object(npm_release, "run_npm_publish") as publish,
            ):
                with self.assertRaisesRegex(ValueError, "higher stable"):
                    npm_release.publish(manifest, release_sha)
            authority.assert_not_called()
            publish.assert_not_called()

    def test_npm_packument_admission_is_monotonic_and_tag_exact(self) -> None:
        exact = {
            "name": "cymule",
            "versions": {"0.1.0": {}, "0.2.0": {}},
            "dist-tags": {"latest": "0.2.0"},
        }
        with mock.patch.object(npm_release, "request_json", return_value=exact):
            self.assertEqual(
                npm_release.latest_publish_status("cymule", "0.2.0"), "exists"
            )
            npm_release.wait_for_latest_tag("cymule", "0.2.0")
        for packument, expected in (
            (
                {
                    "name": "cymule",
                    "versions": {"0.2.0": {}, "0.3.0": {}},
                    "dist-tags": {"latest": "0.3.0"},
                },
                "higher stable",
            ),
            (
                {
                    "name": "cymule",
                    "versions": {"0.2.0": {}, "0.3.0": {}},
                    "dist-tags": {"latest": "0.2.0"},
                },
                "higher stable",
            ),
        ):
            with self.subTest(packument=packument), mock.patch.object(
                npm_release, "request_json", return_value=packument
            ):
                with self.assertRaisesRegex(ValueError, expected):
                    npm_release.latest_publish_status("cymule", "0.2.0")
        historical = {
            "name": "cymule",
            "versions": {"0.2.0": {}, "0.3.0": {}},
            "dist-tags": {"latest": "0.3.0"},
        }
        with mock.patch.object(
            npm_release, "request_json", return_value=historical
        ):
            npm_release.verify_existing_stable_state("cymule", "0.2.0")
        stale_latest = {
            "name": "cymule",
            "versions": {"0.1.0": {}, "0.2.0": {}, "0.3.0": {}},
            "dist-tags": {"latest": "0.1.0"},
        }
        with mock.patch.object(
            npm_release, "request_json", return_value=stale_latest
        ):
            with self.assertRaisesRegex(ValueError, "highest stable"):
                npm_release.verify_existing_stable_state("cymule", "0.2.0")

    def test_tag_creation_admission_blocks_higher_stable_before_tagging(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            with (
                mock.patch.object(
                    npm_release, "registry_status", return_value="missing"
                ),
                mock.patch.object(
                    npm_release,
                    "latest_publish_status",
                    side_effect=ValueError("higher stable version"),
                ),
                mock.patch.object(npm_release, "verify_registry") as verify,
            ):
                with self.assertRaisesRegex(ValueError, "higher stable"):
                    npm_release.tag_creation_admission(manifest, release_sha)
            verify.assert_not_called()

    def test_tag_creation_admission_rejects_exact_historical_version(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            with (
                mock.patch.object(
                    npm_release, "registry_status", return_value="exists"
                ),
                mock.patch.object(npm_release, "verify_registry") as verify,
                mock.patch.object(
                    npm_release,
                    "latest_publish_status",
                    side_effect=ValueError("higher stable version"),
                ),
            ):
                with self.assertRaisesRegex(ValueError, "higher stable"):
                    npm_release.tag_creation_admission(manifest, release_sha)
            verify.assert_called_once_with(manifest, release_sha)

    def test_npm_publish_always_readbacks_and_reconciles_lost_response(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            for publish_error in (
                None,
                subprocess.CalledProcessError(1, ["npm", "publish"]),
                subprocess.TimeoutExpired(["npm", "publish"], 600),
            ):
                with self.subTest(publish_error=publish_error):
                    with (
                        mock.patch.object(
                            npm_release, "registry_status", return_value="missing"
                        ),
                        mock.patch.object(
                            npm_release,
                            "latest_publish_status",
                            return_value="missing",
                        ),
                        mock.patch.object(
                            npm_release,
                            "required_publish_authority",
                            return_value=("b" * 40, "v0.2.0"),
                        ),
                        mock.patch.object(
                            npm_release, "verify_remote_release_authority"
                        ),
                        mock.patch.object(
                            npm_release,
                            "run_npm_publish",
                            side_effect=publish_error,
                        ),
                        mock.patch.object(
                            npm_release, "verify_registry"
                        ) as verify,
                    ):
                        self.assertEqual(
                            npm_release.publish(manifest, release_sha), "published"
                        )
                verify.assert_called_once_with(
                    manifest, release_sha, require_latest=True
                )

    def test_npm_lost_response_conflict_and_unreachable_are_not_success(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            for readback_error, expected in (
                (ValueError("immutable version has other bytes"), "other bytes"),
                (
                    npm_release.urllib.error.URLError("unreachable"),
                    "npm_publish_outcome_ambiguous",
                ),
            ):
                with self.subTest(readback_error=readback_error):
                    with (
                        mock.patch.object(
                            npm_release, "registry_status", return_value="missing"
                        ),
                        mock.patch.object(
                            npm_release,
                            "latest_publish_status",
                            return_value="missing",
                        ),
                        mock.patch.object(
                            npm_release,
                            "required_publish_authority",
                            return_value=("b" * 40, "v0.2.0"),
                        ),
                        mock.patch.object(
                            npm_release, "verify_remote_release_authority"
                        ),
                        mock.patch.object(
                            npm_release,
                            "run_npm_publish",
                            side_effect=subprocess.CalledProcessError(
                                1, ["npm", "publish"]
                            ),
                        ),
                        mock.patch.object(
                            npm_release, "verify_registry", side_effect=readback_error
                        ),
                    ):
                        with self.assertRaisesRegex(Exception, expected):
                            npm_release.publish(manifest, release_sha)

    def test_npm_success_without_exact_readback_is_ambiguous(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            manifest = self.make_stage(pathlib.Path(temporary), release_sha)
            with (
                mock.patch.object(npm_release, "registry_status", return_value="missing"),
                mock.patch.object(
                    npm_release, "latest_publish_status", return_value="missing"
                ),
                mock.patch.object(
                    npm_release,
                    "required_publish_authority",
                    return_value=("b" * 40, "v0.2.0"),
                ),
                mock.patch.object(npm_release, "verify_remote_release_authority"),
                mock.patch.object(npm_release, "run_npm_publish"),
                mock.patch.object(
                    npm_release,
                    "verify_registry",
                    side_effect=npm_release.urllib.error.URLError("unreachable"),
                ),
            ):
                with self.assertRaisesRegex(
                    ValueError, "npm_publish_outcome_ambiguous"
                ):
                    npm_release.publish(manifest, release_sha)

    def test_npm_cli_registers_each_subcommand_once(self) -> None:
        with mock.patch.object(sys, "argv", ["npm_release.py", "--help"]):
            with self.assertRaises(SystemExit) as exit_status:
                npm_release.parse_args()
        self.assertEqual(exit_status.exception.code, 0)

    def test_npm_publish_rejects_archive_controlled_targets_without_write(self) -> None:
        release_sha = "a" * 40
        valid = {
            "access": "public",
            "provenance": True,
            "registry": npm_release.PUBLISH_REGISTRY,
            "tag": npm_release.PUBLISH_DIST_TAG,
        }
        malicious = (
            {**valid, "registry": "https://registry.attacker.invalid/"},
            {**valid, "tag": "next"},
            {**valid, "script-shell": "/tmp/attacker"},
        )
        for publish_config in malicious:
            with self.subTest(
                publish_config=publish_config
            ), tempfile.TemporaryDirectory() as temporary:
                manifest = self.make_stage(
                    pathlib.Path(temporary), release_sha, publish_config
                )
                with mock.patch.object(npm_release, "run_npm_publish") as publish:
                    with self.assertRaisesRegex(ValueError, "publishConfig"):
                        npm_release.publish(manifest, release_sha)
                publish.assert_not_called()

    def test_npm_publish_command_reasserts_the_closed_target(self) -> None:
        archive = pathlib.Path("/stage/cymule-0.2.0.tgz")
        with mock.patch.object(npm_release.subprocess, "run") as invoke:
            npm_release.run_npm_publish(archive)
        invoke.assert_called_once_with(
            [
                "npm",
                "publish",
                str(archive),
                "--registry=https://registry.npmjs.org/",
                "--access=public",
                "--provenance",
                "--tag=latest",
                "--ignore-scripts",
            ],
            cwd=npm_release.CONTROL_ROOT,
            check=True,
            timeout=npm_release.NPM_PUBLISH_TIMEOUT_SECONDS,
        )

    def test_provenance_binds_main_or_exact_tag_and_release_sha(self) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        for workflow_ref in ("refs/heads/main", "refs/tags/v0.2.0"):
            payload = self.make_provenance_payload(
                release_sha, sha512, workflow_ref=workflow_ref
            )
            with self.subTest(workflow_ref=workflow_ref):
                with (
                    mock.patch.object(
                        npm_release, "request_json", return_value=payload
                    ),
                    mock.patch.object(
                        npm_release,
                        "verify_sigstore_bundle",
                        return_value=(
                            npm_release.SIGSTORE_CERTIFICATE_IDENTITY,
                            "f" * 40,
                        ),
                    ) as verify,
                ):
                    npm_release.verify_provenance(
                        f"{npm_release.REGISTRY}/-/attestations",
                        "cymule",
                        "0.2.0",
                        sha512,
                        release_sha,
                    )
            verify.assert_called_once()

    def test_provenance_rejects_unrelated_ref_source_and_extra_dependencies(
        self,
    ) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        mutations = []
        for workflow_ref in (
            "refs/tags/v0.2.1",
            "refs/tags/other",
            "refs/heads/release",
        ):
            mutations.append(
                (
                    f"workflow-{workflow_ref}",
                    lambda statement, value=workflow_ref: statement["predicate"][
                        "buildDefinition"
                    ]["externalParameters"]["workflow"].update(ref=value),
                )
            )
        mutations.extend(
            [
                (
                    "unrelated-uri",
                    lambda statement: statement["predicate"]["buildDefinition"][
                        "resolvedDependencies"
                    ][0].update(uri="git+https://github.com/other/repo@refs/heads/main"),
                ),
                (
                    "other-sha",
                    lambda statement: statement["predicate"]["buildDefinition"][
                        "resolvedDependencies"
                    ][0]["digest"].update(gitCommit="e" * 40),
                ),
                (
                    "extra-dependency",
                    lambda statement: statement["predicate"]["buildDefinition"][
                        "resolvedDependencies"
                    ].append(
                        {
                            "uri": "git+https://github.com/other/repo@refs/heads/main",
                            "digest": {"gitCommit": "e" * 40},
                        }
                    ),
                ),
            ]
        )
        for name, mutate in mutations:
            payload = self.make_provenance_payload(release_sha, sha512)
            statement = self.provenance_statement(payload)
            mutate(statement)
            self.set_provenance_statement(payload, statement)
            with self.subTest(name=name):
                with (
                    mock.patch.object(
                        npm_release, "request_json", return_value=payload
                    ),
                    mock.patch.object(
                        npm_release,
                        "verify_sigstore_bundle",
                        return_value=(
                            npm_release.SIGSTORE_CERTIFICATE_IDENTITY,
                            "f" * 40,
                        ),
                    ),
                ):
                    with self.assertRaisesRegex(ValueError, "workflow|release source"):
                        npm_release.verify_provenance(
                            f"{npm_release.REGISTRY}/-/attestations",
                            "cymule",
                            "0.2.0",
                            sha512,
                            release_sha,
                        )

    def test_provenance_rejects_extra_subjects_and_another_builder(self) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        mutations = (
            (
                "extra-subject",
                lambda statement: statement["subject"].append(
                    {
                        "name": "pkg:npm/other@0.2.0",
                        "digest": {"sha512": sha512},
                    }
                ),
            ),
            (
                "build-type",
                lambda statement: statement["predicate"]["buildDefinition"].update(
                    buildType="https://example.invalid/build"
                ),
            ),
            (
                "repository-id",
                lambda statement: statement["predicate"]["buildDefinition"][
                    "internalParameters"
                ]["github"].update(repository_id="1"),
            ),
            (
                "builder",
                lambda statement: statement["predicate"]["runDetails"][
                    "builder"
                ].update(id="https://example.invalid/runner"),
            ),
            (
                "invocation",
                lambda statement: statement["predicate"]["runDetails"][
                    "metadata"
                ].update(invocationId="https://example.invalid/run"),
            ),
        )
        for name, mutate in mutations:
            payload = self.make_provenance_payload(release_sha, sha512)
            statement = self.provenance_statement(payload)
            mutate(statement)
            self.set_provenance_statement(payload, statement)
            with self.subTest(name=name):
                with (
                    mock.patch.object(
                        npm_release, "request_json", return_value=payload
                    ),
                    mock.patch.object(
                        npm_release,
                        "verify_sigstore_bundle",
                        return_value=(
                            npm_release.SIGSTORE_CERTIFICATE_IDENTITY,
                            "f" * 40,
                        ),
                    ),
                ):
                    with self.assertRaisesRegex(
                        ValueError,
                        "package subject|build definition|source repository|invocation",
                    ):
                        npm_release.verify_provenance(
                            f"{npm_release.REGISTRY}/-/attestations",
                            "cymule",
                            "0.2.0",
                            sha512,
                            release_sha,
                        )

    def test_provenance_rejects_a_bundle_that_fails_sigstore_verification(self) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        payload = self.make_provenance_payload(release_sha, sha512)
        with (
            mock.patch.object(npm_release, "request_json", return_value=payload),
            mock.patch.object(
                npm_release,
                "verify_sigstore_bundle",
                side_effect=ValueError("signature verification failed"),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "signature verification"):
                npm_release.verify_provenance(
                    f"{npm_release.REGISTRY}/-/attestations",
                    "cymule",
                    "0.2.0",
                    sha512,
                    release_sha,
                )

    def test_real_sigstore_bundle_verifies_signature_identity_ct_and_rekor(
        self,
    ) -> None:
        fixture = json.loads(
            (ROOT / "tests/fixtures/npm-sigstore-provenance-bundle.json").read_text(
                encoding="utf-8"
            )
        )
        npm_root = pathlib.Path(
            subprocess.run(
                ["npm", "root", "--global"],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
        )
        identity = (
            "https://github.com/sigstore/sigstore-js/"
            ".github/workflows/release.yml@refs/heads/main"
        )
        issuer = npm_release.SIGSTORE_CERTIFICATE_ISSUER
        npm_release.run_sigstore_verifier(fixture, npm_root, identity, issuer)
        signer_ref, signer_sha = npm_release.extract_fulcio_signer(
            fixture, expected_signer_ref=identity
        )
        self.assertEqual(signer_ref, identity)
        self.assertEqual(signer_sha, "c4ad6141eb947a20690837888e5d90d9a30b5af3")

        missing_signer_uri = json.loads(json.dumps(fixture))
        certificate = base64.b64decode(
            missing_signer_uri["verificationMaterial"]["certificate"]["rawBytes"]
        )
        signer_uri_oid = bytes.fromhex("060a2b0601040183bf300109")
        unrelated_oid = bytes.fromhex("060a2b0601040183bf300163")
        self.assertEqual(certificate.count(signer_uri_oid), 1)
        missing_signer_uri["verificationMaterial"]["certificate"]["rawBytes"] = (
            base64.b64encode(
                certificate.replace(signer_uri_oid, unrelated_oid, 1)
            ).decode()
        )
        with self.assertRaisesRegex(ValueError, "omits required extension"):
            npm_release.extract_fulcio_signer(
                missing_signer_uri, expected_signer_ref=identity
            )

        tampered_signer_digest = json.loads(json.dumps(fixture))
        certificate = base64.b64decode(
            tampered_signer_digest["verificationMaterial"]["certificate"]["rawBytes"]
        )
        self.assertGreaterEqual(certificate.count(signer_sha.encode()), 1)
        tampered_signer_digest["verificationMaterial"]["certificate"]["rawBytes"] = (
            base64.b64encode(
                certificate.replace(
                    signer_sha.encode(), ("g" + signer_sha[1:]).encode()
                )
            ).decode()
        )
        with self.assertRaisesRegex(ValueError, "exact Git commit"):
            npm_release.extract_fulcio_signer(
                tampered_signer_digest, expected_signer_ref=identity
            )

        wrong_identity = identity.replace("release.yml", "other.yml")
        with self.assertRaisesRegex(ValueError, "Sigstore bundle verification"):
            npm_release.run_sigstore_verifier(
                fixture, npm_root, wrong_identity, issuer
            )
        with self.assertRaisesRegex(ValueError, "Sigstore bundle verification"):
            npm_release.run_sigstore_verifier(
                fixture, npm_root, identity, "https://issuer.invalid"
            )

        tampered_signature = json.loads(json.dumps(fixture))
        signature = tampered_signature["dsseEnvelope"]["signatures"][0]["sig"]
        tampered_signature["dsseEnvelope"]["signatures"][0]["sig"] = (
            ("A" if signature[0] != "A" else "B") + signature[1:]
        )
        missing_rekor = json.loads(json.dumps(fixture))
        missing_rekor["verificationMaterial"]["tlogEntries"] = []
        tampered_certificate = json.loads(json.dumps(fixture))
        certificate = base64.b64decode(
            tampered_certificate["verificationMaterial"]["certificate"]["rawBytes"]
        )
        certificate = certificate[:-1] + bytes([certificate[-1] ^ 1])
        tampered_certificate["verificationMaterial"]["certificate"]["rawBytes"] = (
            base64.b64encode(certificate).decode()
        )
        for name, bundle in (
            ("signature", tampered_signature),
            ("rekor", missing_rekor),
            ("certificate-sct", tampered_certificate),
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(
                    ValueError, "Sigstore bundle verification"
                ):
                    npm_release.run_sigstore_verifier(
                        bundle, npm_root, identity, issuer
                    )


class WorkflowSecurityTests(unittest.TestCase):
    def test_release_version_input_is_closed_before_ref_and_output_use(self) -> None:
        workflows = {
            "publish-npm-controller.yml": (
                ROOT / ".github/workflows/publish-npm-controller.yml"
            ).read_text(),
            "publish-crates.yml": (
                ROOT / ".github/workflows/publish-crates.yml"
            ).read_text(),
            "finalize-release.yml": (
                ROOT / ".github/workflows/finalize-release.yml"
            ).read_text(),
        }
        for name, workflow in workflows.items():
            release_workflows.verify_stable_version_admission(workflow, name)
            with self.subTest(name=name, mutation="weak"), self.assertRaisesRegex(
                ValueError, "strict ASCII stable SemVer"
            ):
                release_workflows.verify_stable_version_admission(
                    workflow.replace(
                        release_workflows.STABLE_VERSION_ADMISSION,
                        'export LC_ALL=C\n          [[ -n "$RELEASE_VERSION" ]]',
                        1,
                    ),
                    name,
                )
            without_admission = workflow.replace(
                release_workflows.STABLE_VERSION_ADMISSION, "", 1
            )
            after_output = without_admission.replace(
                '} >> "$GITHUB_OUTPUT"',
                '} >> "$GITHUB_OUTPUT"\n          '
                + release_workflows.STABLE_VERSION_ADMISSION,
                1,
            )
            with self.subTest(name=name, mutation="late"), self.assertRaisesRegex(
                ValueError,
                "before writing GITHUB_OUTPUT|before constructing a Git ref|"
                "must not use release input",
            ):
                release_workflows.verify_stable_version_admission(after_output, name)
            for premature in (
                'echo "$RELEASE_VERSION" >> "$GITHUB_OUTPUT"\n          ',
                'git fetch origin "refs/tags/v${{ inputs.version }}"\n          ',
            ):
                with self.subTest(
                    name=name, mutation="premature-input"
                ), self.assertRaisesRegex(ValueError, "must not use release input"):
                    release_workflows.verify_stable_version_admission(
                        workflow.replace(
                            "export LC_ALL=C",
                            premature + "export LC_ALL=C",
                            1,
                        ),
                        name,
                    )

        expression = (
            'export LC_ALL=C; [[ "$1" =~ '
            '^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$ ]]'
        )
        for value in ("0.0.0", "1.2.3", "10.20.30"):
            with self.subTest(value=value):
                self.assertEqual(
                    subprocess.run(
                        ["bash", "-c", expression, "cymule-version-test", value],
                        check=False,
                    ).returncode,
                    0,
                )
        for value in (
            "01.2.3",
            "1.2.3-rc.1",
            "1.2.3\nforged_output=accepted",
            "1.2.3 ",
            "１.２.３",
        ):
            with self.subTest(value=value):
                self.assertNotEqual(
                    subprocess.run(
                        ["bash", "-c", expression, "cymule-version-test", value],
                        check=False,
                    ).returncode,
                    0,
                )

    def test_public_workflows_are_pinned_and_credential_closed(self) -> None:
        release_workflows.verify()

    def test_required_ci_source_closure_is_mandatory_and_unprivileged(self) -> None:
        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        release_workflows.verify_required_ci_source_closure(workflow)
        for original, replacement in (
            ("      - version-domain\n", ""),
            (
                "if: needs.plan.outputs.version_domain != ''",
                "if: false",
            ),
            (
                "  version-domain:\n"
                "    name: Verify / version-domain source closure\n"
                "    needs: plan\n"
                "    if: needs.plan.outputs.version_domain != ''\n"
                "    runs-on: ubuntu-24.04\n",
                "  version-domain:\n"
                "    name: Verify / version-domain source closure\n"
                "    needs: plan\n"
                "    if: needs.plan.outputs.version_domain != ''\n"
                "    runs-on: ubuntu-24.04\n"
                "    permissions:\n      id-token: write\n",
            ),
        ):
            with self.subTest(original=original), self.assertRaisesRegex(
                ValueError, "lightweight version-domain source lane"
            ):
                release_workflows.verify_required_ci_source_closure(
                    workflow.replace(original, replacement, 1)
                )

    def test_release_controller_clis_are_executable(self) -> None:
        for name in (
            "finalize_release.py",
            "npm_release.py",
            "crates_release.py",
            "verify_github_release_settings.py",
            "verify_release_workflows.py",
            "version_domains.py",
        ):
            path = ROOT / "scripts" / name
            with self.subTest(name=name):
                self.assertTrue(os.access(path, os.X_OK), f"{name} is not executable")

    def test_github_release_mutation_has_one_workflow_writer(self) -> None:
        release_workflows.verify_unique_release_writer()
        with tempfile.TemporaryDirectory() as temporary:
            extra = pathlib.Path(temporary) / "other-release.yml"
            finalizer = ROOT / ".github/workflows/finalize-release.yml"
            for workflow in (
                "permissions:\n  contents: write\njobs: {}\n",
                "jobs:\n  mutate:\n    permissions:\n      contents: write\n",
                "permissions: {contents: write}\njobs: {}\n",
                'permissions:\n  contents: "write"\njobs: {}\n',
                "permissions: {'contents': 'write'}\njobs: {}\n",
            ):
                extra.write_text(workflow, encoding="utf-8")
                with self.subTest(workflow=workflow), mock.patch.object(
                    release_workflows,
                    "workflow_paths",
                    return_value=[finalizer, extra],
                ), self.assertRaisesRegex(ValueError, "one contents-write workflow"):
                    release_workflows.verify_unique_release_writer()
            for workflow in (
                "permissions: write-all\njobs: {}\n",
                'permissions: "write-all"\njobs: {}\n',
                "'permissions': write-all\njobs: {}\n",
                "\"permissions\": 'write-all'\njobs: {}\n",
            ):
                extra.write_text(workflow, encoding="utf-8")
                with self.subTest(workflow=workflow), mock.patch.object(
                    release_workflows,
                    "workflow_paths",
                    return_value=[finalizer, extra],
                ), self.assertRaisesRegex(ValueError, "write-all workflows"):
                    release_workflows.verify_unique_release_writer()
            npm_controller = ROOT / ".github/workflows/publish-npm-controller.yml"
            extra.write_text(
                "permissions: {}\njobs:\n  mint:\n    env:\n"
                "      KEY: ${{ secrets.CYMULE_RELEASE_TAG_APP_PRIVATE_KEY }}\n",
                encoding="utf-8",
            )
            with mock.patch.object(
                release_workflows,
                "workflow_paths",
                return_value=[finalizer, npm_controller, extra],
            ), self.assertRaisesRegex(ValueError, "one audited workflow"):
                release_workflows.verify_unique_release_writer()
            scripts = pathlib.Path(temporary) / "scripts"
            scripts.mkdir()
            shutil.copy2(ROOT / "scripts/finalize_release.py", scripts)
            (scripts / "bypass.sh").write_text(
                "#!/bin/sh\ngh release upload v0.2.0 forged\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "one audited script"):
                release_workflows.verify_unique_release_writer(scripts)

    def test_workflow_inventory_includes_yaml_and_yml(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "one.yml").write_text("name: one\n")
            (root / "two.yaml").write_text("name: two\n")
            (root / "ignored.txt").write_text("not a workflow\n")
            self.assertEqual(
                [path.name for path in release_workflows.workflow_paths(root)],
                ["one.yml", "two.yaml"],
            )

    def test_action_pins_reject_noncanonical_or_mutable_uses_scalars(self) -> None:
        sha = "0" * 40
        release_workflows.verify_action_pins(
            "fixture.yml", f"steps:\n  - uses: actions/checkout@{sha}\n"
        )
        release_workflows.verify_action_pins(
            "fixture.yml", "steps:\n  - uses: ./local-action\n"
        )
        for value in (
            '"actions/checkout@' + sha + '"',
            "actions/checkout@main",
            ">-",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "unsupported uses syntax|mutable action"
            ):
                release_workflows.verify_action_pins(
                    "fixture.yml", f"steps:\n  - uses: {value}\n"
                )

    def test_release_job_inventory_rejects_unparsed_top_level_keys(self) -> None:
        valid = """name: fixture

on:
  workflow_dispatch:

permissions: {}

jobs:
  valid:
    permissions:
      contents: read
"""
        self.assertEqual(set(release_workflows.job_bodies(valid)), {"valid"})
        with self.assertRaisesRegex(ValueError, "unsupported top-level job key"):
            release_workflows.job_bodies(
                valid + '  "hidden":\n    permissions:\n      contents: write\n'
            )

    def test_setup_uv_requires_the_exact_repository_version(self) -> None:
        valid = """steps:
      - uses: astral-sh/setup-uv@0000000000000000000000000000000000000000
        with:
          version: 0.7.2
      - name: Continue
        run: true
"""
        release_workflows.verify_setup_uv_pins("fixture.yml", valid)
        for malformed in (
            valid.replace("          version: 0.7.2\n", ""),
            valid.replace("version: 0.7.2", "version: latest"),
        ):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(ValueError, "pin uv 0.7.2 exactly"):
                    release_workflows.verify_setup_uv_pins("fixture.yml", malformed)

    def test_npm_release_ref_authority_requires_new_caller_and_current_controller(self) -> None:
        workflow = (
            ROOT / ".github/workflows/publish-npm-controller.yml"
        ).read_text()
        release_workflows.verify_npm_release_ref_authority(workflow)
        for fragment in (
            'test "$GITHUB_WORKFLOW_SHA" = "$GITHUB_SHA"',
            'test "$CONTROLLER_SHA" = "$current_main"',
            'git diff --quiet "$CONTROLLER_SHA" -- .github/workflows/publish-npm-release.yml',
            'test "$GITHUB_SHA" = "$current_main"',
            'test "$GITHUB_SHA" = "$release_sha"',
            'refs/tags/"$tag")',
            "historical $tag recovery must be dispatched from refs/tags/$tag",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "new caller, current controller, and exact source ref"
            ):
                release_workflows.verify_npm_release_ref_authority(
                    workflow.replace(fragment, "missing-authority", 1)
                )

    def test_npm_trusted_caller_is_inert_and_old_filename_is_retired(self) -> None:
        caller = (ROOT / ".github/workflows/publish-npm-release.yml").read_text()
        controller = (
            ROOT / ".github/workflows/publish-npm-controller.yml"
        ).read_text()
        self.assertFalse((ROOT / ".github/workflows/publish-npm.yml").exists())
        release_workflows.verify_npm_caller_boundary(caller, controller)
        with self.assertRaisesRegex(ValueError, "only the manual trusted caller"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace(
                    "  workflow_dispatch:\n",
                    "  push:\n  workflow_dispatch:\n",
                    1,
                ),
                controller,
            )
        with self.assertRaisesRegex(ValueError, "read-only contents and OIDC ceiling"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace(
                    "    permissions:\n      contents: read\n      id-token: write",
                    "    permissions:\n      contents: write\n      id-token: write",
                    1,
                ),
                controller,
            )
        with self.assertRaisesRegex(ValueError, "one main controller"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace("@main", "@${{ github.sha }}", 1), controller
            )
        with self.assertRaisesRegex(ValueError, "may not execute"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace(
                    "    uses: cymule-framework/",
                    "    runs-on: ubuntu-24.04\n    uses: cymule-framework/",
                    1,
                ),
                controller,
            )
        with self.assertRaisesRegex(ValueError, "closed inert call envelope"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace(
                    "    with:\n",
                    "    strategy:\n      matrix:\n        copy: [one, two]\n    with:\n",
                    1,
                ),
                controller,
            )
        with self.assertRaisesRegex(ValueError, "may not execute"):
            release_workflows.verify_npm_caller_boundary(
                caller.replace(
                    "    with:\n",
                    "    secrets: inherit\n    with:\n",
                    1,
                ),
                controller,
            )
        with self.assertRaisesRegex(ValueError, "not directly dispatchable"):
            release_workflows.verify_npm_caller_boundary(
                caller, controller.replace("workflow_call:", "workflow_dispatch:", 1)
            )
        with self.assertRaisesRegex(ValueError, "provenance verifier toolchain"):
            release_workflows.verify_npm_publish_boundary(
                controller.replace(
                    'test "$(npm --version)" = 11.19.0',
                    "missing-npm-toolchain-readback",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "package-wide monotonic state"):
            release_workflows.verify_npm_publish_boundary(
                controller.replace("publication-admission", "registry-status", 1)
            )
        for command in (
            "publication-admission",
            "tag-creation-admission",
            "verify-registry",
        ):
            expected = (
                "package-wide monotonic state"
                if command in {"publication-admission", "tag-creation-admission"}
                else "executes tag-carried code"
            )
            with self.subTest(command=command), self.assertRaisesRegex(
                ValueError, expected
            ):
                release_workflows.verify_npm_publish_boundary(
                    controller.replace(
                        f"python3 scripts/npm_release.py {command}",
                        f"python3 release-payload/scripts/npm_release.py {command}",
                        1,
                    )
                )
        for fragment in ("group: publish-npm-stable", "cancel-in-progress: false"):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "serialize every stable version"
            ):
                release_workflows.verify_npm_caller_boundary(
                    caller,
                    controller.replace(fragment, "missing-stable-concurrency", 1),
                )
        with self.assertRaisesRegex(ValueError, "serialize every stable version"):
            release_workflows.verify_npm_caller_boundary(
                caller,
                controller.replace(
                    "  cancel-in-progress: false\n",
                    "  cancel-in-progress: false\n  queue: max\n",
                    1,
                ),
            )

    def test_terminal_release_jobs_separate_controller_and_payload(self) -> None:
        crates = (ROOT / ".github/workflows/publish-crates.yml").read_text()
        finalize = (ROOT / ".github/workflows/finalize-release.yml").read_text()
        release_workflows.verify_crates_controller_boundary(crates)
        release_workflows.verify_finalization_controller_boundary(finalize)

        for fragment in (
            "group: finalize-release-stable",
            "cancel-in-progress: false",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "serialize every stable version"
            ):
                release_workflows.verify_finalization_controller_boundary(
                    finalize.replace(fragment, "missing-stable-concurrency", 1)
                )
        with self.assertRaisesRegex(ValueError, "serialize every stable version"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "  cancel-in-progress: false\n",
                    "  cancel-in-progress: false\n  queue: max\n",
                    1,
                )
            )

        with self.assertRaisesRegex(ValueError, "current controller and tag payload"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "ref: ${{ needs.verify.outputs.controller_sha }}",
                    "ref: ${{ needs.verify.outputs.release_sha }}",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "historical tag code|current controller"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "python3 scripts/crates_release.py publish",
                    "python3 release-payload/scripts/crates_release.py publish",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "historical tag code"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "python3 scripts/crates_release.py publish --version",
                    'bash "$CYMULE_RELEASE_WORKSPACE/scripts/publish.sh"\n'
                    "          python3 scripts/crates_release.py publish --version",
                    1,
                )
            )
        with self.assertRaisesRegex(
            ValueError, "current controller and tag payload|isolated tag payload checkout"
        ):
            release_workflows.verify_crates_controller_boundary(
                crates.replace("          path: release-payload\n", "", 1)
            )
        with self.assertRaisesRegex(ValueError, "contents-write authority"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    'python3 "$controller_dir/scripts/finalize_release.py" publish',
                    "pnpm --dir release-payload test\n"
                    '          python3 "$controller_dir/scripts/finalize_release.py" publish',
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "historical tag controller"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "scripts/version_domains.py verify",
                    "release-payload/scripts/version_domains.py verify",
                    1,
                )
            )
        for controller in (
            "npm_release.py verify-registry",
            "crates_release.py verify-registry",
            "crates_release.py registry-evidence",
            "finalize_release.py stage",
        ):
            with self.subTest(controller=controller), self.assertRaisesRegex(
                ValueError,
                "current-main controller|historical tag controller|data-only freeze",
            ):
                release_workflows.verify_finalization_controller_boundary(
                    finalize.replace(
                        f"scripts/{controller}",
                        f"release-payload/scripts/{controller}\n"
                        f"          python3 scripts/{controller}",
                        1,
                    )
                )
        for fragment in (
            '--publication-output "$evidence_dir/npm-$stage_name.json"',
            '--controller-sha "$CONTROLLER_SHA"',
            '--publications "$evidence_dir/crates.json"',
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError,
                "current controller.*exact payload.*closed data bundle|does not freeze data",
            ):
                release_workflows.verify_finalization_controller_boundary(
                    finalize.replace(fragment, "missing-final-evidence", 1)
                )
        for fragment in (
            "actions/attest@a1948c3f048ba23858d222213b7c278aabede763",
            "python3 scripts/finalize_release.py verify-stage",
            '--attestation-bundle "$attestation_dir/bundle.json"',
            "path: release-authority",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError,
                "does not freeze data|attested bundle|unexpected privileged action|"
                "frozen current controller",
            ):
                release_workflows.verify_finalization_controller_boundary(
                    finalize.replace(fragment, "missing-attestation-authority", 1)
                )
        with self.assertRaisesRegex(ValueError, "complete exact workspace"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "          path: release-authority\n",
                    "          path: release-authority\n"
                    "          sparse-checkout: /Cargo.toml\n",
                    1,
                )
            )
        out_of_order = finalize.replace(
            "python3 scripts/crates_release.py registry-evidence",
            "python3 scripts/version_domains.py bom",
            1,
        ).replace(
            "uv run --project sdk/python --frozen python scripts/version_domains.py bom",
            "python3 scripts/crates_release.py registry-evidence",
            1,
        )
        with self.assertRaisesRegex(ValueError, "exact order|does not freeze data"):
            release_workflows.verify_finalization_controller_boundary(out_of_order)
        with self.assertRaisesRegex(
            ValueError, "current controller.*exact tag payload|freeze data"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace("          path: release-payload\n", "", 1)
            )
        publish_prefix, publish_suffix = finalize.rsplit(
            "      - uses: actions/download-artifact@", 1
        )
        with self.assertRaisesRegex(ValueError, "one current-main controller"):
            release_workflows.verify_finalization_controller_boundary(
                publish_prefix
                + "      - uses: actions/checkout@"
                    "3d3c42e5aac5ba805825da76410c181273ba90b1\n"
                    "        with:\n"
                    "          ref: ${{ needs.verify.outputs.release_sha }}\n\n"
                + "      - uses: actions/download-artifact@"
                + publish_suffix
            )
        with self.assertRaisesRegex(ValueError, "current-main controller"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "ref: ${{ needs.verify.outputs.controller_sha }}",
                    "ref: ${{ needs.verify.outputs.release_sha }}",
                    1,
                )
            )

    def test_terminal_release_permissions_and_tag_readback_are_closed(self) -> None:
        npm = (
            ROOT / ".github/workflows/publish-npm-controller.yml"
        ).read_text()
        crates = (ROOT / ".github/workflows/publish-crates.yml").read_text()
        finalize = (ROOT / ".github/workflows/finalize-release.yml").read_text()
        release_workflows.verify_npm_publish_boundary(npm)
        release_workflows.verify_crates_controller_boundary(crates)
        release_workflows.verify_finalization_controller_boundary(finalize)

        with self.assertRaisesRegex(ValueError, "does not freeze data"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "      release_tag_sha: ${{ steps.identity.outputs.release_tag_sha }}\n",
                    "",
                    1,
                )
            )
        with self.assertRaisesRegex(
            ValueError, "does not freeze data|exact annotated tag"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    '          test "$remote_tag_sha" = "$RELEASE_TAG_SHA"',
                    "          test true",
                    1,
                )
            )
        finalizer_prefix, finalizer_publish = finalize.rsplit(
            '          test "$remote_tag_sha" = "$RELEASE_TAG_SHA"', 1
        )
        with self.assertRaisesRegex(
            ValueError,
            "does not freeze data|frozen current controller|exact annotated tag",
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalizer_prefix + "          test true" + finalizer_publish
            )
        finalizer_prefix, finalizer_publish = finalize.rsplit(
            '            --release-tag-sha "$RELEASE_TAG_SHA" \\\n', 1
        )
        with self.assertRaisesRegex(
            ValueError, "frozen current controller|contents-write job"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalizer_prefix + finalizer_publish
            )
        with self.assertRaisesRegex(
            ValueError, "does not freeze data|live control-plane preflight"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "          permission-administration: read",
                    "          permission-administration: write",
                    1,
                )
            )
        with self.assertRaisesRegex(
            ValueError, "does not freeze data|live control-plane preflight"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "          permission-actions: read",
                    "          permission-actions: write",
                    1,
                )
            )
        with self.assertRaisesRegex(
            ValueError, "does not freeze data|frozen current controller"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    '            --control-plane-receipt "$control_plane_dir/receipt.json" \\\n',
                    "",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "does not freeze data"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "    needs: [verify, freeze, attest, control-plane]",
                    "    needs: [verify, freeze, attest]",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "control-plane credentials"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "          GH_TOKEN: ${{ github.token }}",
                    "          GH_TOKEN: ${{ github.token }}\n"
                    "          CONTROL_PLANE_TOKEN: stolen",
                    1,
                )
            )

        with self.assertRaisesRegex(ValueError, "tag mutation must use"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace("    environment: npm\n", "", 1)
            )
        with self.assertRaisesRegex(ValueError, "may not use a protected environment"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "  verify:\n    runs-on: ubuntu-24.04",
                    "  verify:\n    runs-on: ubuntu-24.04\n    environment: npm",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "stage must remain credential-free"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "  stage:\n    needs: verify\n    runs-on: ubuntu-24.04\n"
                    "    permissions:\n      contents: read",
                    "  stage:\n    needs: verify\n    runs-on: ubuntu-24.04\n"
                    "    permissions:\n      contents: write",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "current-fenced and loss-resolved"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    '              if ! git -C "$CYMULE_RELEASE_WORKSPACE" \\\n',
                    '              git -C "$CYMULE_RELEASE_WORKSPACE" \\\n',
                    1,
                )
            )
        for fragment in (
            '            expected_tag_sha=$(git -C "$CYMULE_RELEASE_WORKSPACE" '
            'rev-parse "refs/tags/$RELEASE_TAG")\n',
            '              test "$remote_before_tag_sha" = "$expected_tag_sha"\n',
            '          test "$final_remote_tag_sha" = "$expected_tag_sha"\n',
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "current-fenced and loss-resolved"
            ):
                release_workflows.verify_npm_publish_boundary(
                    npm.replace(fragment, "missing-raw-tag-binding\n", 1)
                )
        with self.assertRaisesRegex(ValueError, "single-purpose App authority"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "permission-contents: write",
                    "permission-contents: read",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "single-purpose App authority"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "if: needs.verify.outputs.tag_exists == 'false'",
                    "if: always()",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "single-purpose App authority"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "client-id: ${{ vars.CYMULE_RELEASE_TAG_APP_CLIENT_ID }}",
                    "app-id: ${{ vars.CYMULE_RELEASE_TAG_APP_ID }}",
                    1,
                )
            )
        for original, replacement in (
            (
                "python3 scripts/npm_release.py github-app-bot-user-id",
                "printf '%s\\n' \"$TAG_APP_ID\"",
            ),
            (
                "$bot_user_id+${TAG_APP_SLUG}[bot]@users.noreply.github.com",
                "$TAG_APP_ID+${TAG_APP_SLUG}[bot]@users.noreply.github.com",
            ),
        ):
            with self.subTest(original=original), self.assertRaisesRegex(
                ValueError, "single-purpose App authority"
            ):
                release_workflows.verify_npm_publish_boundary(
                    npm.replace(original, replacement, 1)
                )
        with self.assertRaisesRegex(ValueError, "fresh closed two-package registry admission"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "$RUNNER_TEMP/npm-stage-cymule-sdk/manifest.json",
                    "$RUNNER_TEMP/unadmitted-sdk/manifest.json",
                    1,
                )
            )
        controller_integrity = (
            'git diff --quiet "$CONTROLLER_SHA" -- '
            "scripts/npm_release.py scripts/version_domains.py"
        )
        mutation_marker = "      - name: Publish missing version with trusted provenance\n"
        npm_prefix, npm_mutation = npm.split(mutation_marker, 1)
        with self.assertRaisesRegex(ValueError, "current controller bytes"):
            release_workflows.verify_npm_publish_boundary(
                npm_prefix
                + mutation_marker
                + npm_mutation.replace(
                    controller_integrity, "missing-controller-integrity", 1
                )
            )
        with self.assertRaisesRegex(
            ValueError, "fenced npm controller|terminal publisher omits"
        ):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "python3 scripts/npm_release.py publish",
                    "npm publish unclosed.tgz --provenance",
                    1,
                )
            )
        for workflow, verifier, fragment in (
            (
                crates,
                release_workflows.verify_crates_controller_boundary,
                'git diff --quiet "$CONTROLLER_SHA" -- '
                "scripts/crates_release.py scripts/version_domains.py",
            ),
            (
                finalize,
                release_workflows.verify_finalization_controller_boundary,
                'test "$remote_main" = "$CONTROLLER_SHA"',
            ),
        ):
            prefix, suffix = workflow.rsplit(fragment, 1)
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "frozen current controller|does not freeze data"
            ):
                verifier(prefix + "missing-controller-integrity" + suffix)

        with self.assertRaisesRegex(ValueError, "broader authority"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "    permissions:\n      contents: read\n      id-token: write\n    strategy:",
                    "    permissions:\n      contents: write\n      id-token: write\n    strategy:",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "broader than registry authority"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "    permissions:\n      contents: read\n      id-token: write\n    steps:",
                    "    permissions:\n      contents: write\n      id-token: write\n    steps:",
                    1,
                )
            )
        with self.assertRaisesRegex(
            ValueError, "attestor must hold no Release mutation authority|does not freeze data"
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "      attestations: write\n",
                    "      attestations: read\n",
                    1,
                )
            )

        tag_readback = (
            'test "$remote_release_sha" = "$RELEASE_SHA"',
            "exact annotated tag",
        )
        fragment, message = tag_readback
        npm_prefix, npm_suffix = npm.rsplit(fragment, 1)
        with self.assertRaisesRegex(ValueError, message):
            release_workflows.verify_npm_publish_boundary(
                npm_prefix + "missing-tag-readback" + npm_suffix
            )
        with self.assertRaisesRegex(ValueError, "frozen current controller|exact annotated tag"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(fragment, "missing-tag-readback", 1)
            )
        with self.assertRaisesRegex(
            ValueError,
            "frozen current controller|exact annotated tag|current-main controller",
        ):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(fragment, "missing-tag-readback", 1)
            )

    def test_privileged_release_jobs_reject_extra_actions_or_commands(self) -> None:
        npm = (
            ROOT / ".github/workflows/publish-npm-controller.yml"
        ).read_text()
        crates = (ROOT / ".github/workflows/publish-crates.yml").read_text()
        finalize = (ROOT / ".github/workflows/finalize-release.yml").read_text()
        with self.assertRaisesRegex(ValueError, "unexpected action"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "      - name: Authenticate exact staged bytes",
                    "      - uses: actions/cache@0000000000000000000000000000000000000000\n\n"
                    "      - name: Authenticate exact staged bytes",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "closed tag controller"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    '          test "$final_remote_release_sha" = "$RELEASE_SHA"',
                    '          test "$final_remote_release_sha" = "$RELEASE_SHA"\n'
                    '          curl -X POST "https://api.github.com/repos/'
                    '$GITHUB_REPOSITORY/releases"',
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "unexpected action"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "      - name: Authenticate exact staged bytes",
                    "      - uses: ./unreviewed-local-action\n\n"
                    "      - name: Authenticate exact staged bytes",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "extra executable steps"):
            release_workflows.verify_npm_publish_boundary(
                npm.replace(
                    "      - name: Authenticate exact staged bytes",
                    "      - name: Unreviewed command\n"
                    "        run: true\n\n"
                    "      - name: Authenticate exact staged bytes",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "unexpected privileged action"):
            release_workflows.verify_crates_controller_boundary(
                crates.replace(
                    "      - name: Rebind publisher",
                    "      - uses: actions/cache@0000000000000000000000000000000000000000\n\n"
                    "      - name: Rebind publisher",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "execute only the frozen current controller"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "        run: |\n"
                    '          controller_dir="$RUNNER_TEMP/release-controller"',
                    "        run: |\n"
                    "          bash -c true\n"
                    '          controller_dir="$RUNNER_TEMP/release-controller"',
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "may not execute any third-party Action"):
            release_workflows.verify_finalization_controller_boundary(
                finalize.replace(
                    "      - name: Converge the exact draft, BOM, and published Release",
                    "      - uses: actions/cache@0000000000000000000000000000000000000000\n\n"
                    "      - name: Converge the exact draft, BOM, and published Release",
                    1,
                )
            )

    def test_terminal_readback_cannot_be_hoisted_before_registry_mutation(self) -> None:
        fragment = 'test "$remote_release_sha" = "$RELEASE_SHA"'
        npm = (
            ROOT / ".github/workflows/publish-npm-controller.yml"
        ).read_text()
        npm_prefix, npm_suffix = npm.rsplit(fragment, 1)
        with self.assertRaisesRegex(ValueError, "exact annotated tag"):
            release_workflows.verify_npm_publish_boundary(
                npm_prefix + "test true" + npm_suffix
            )

        crates = (ROOT / ".github/workflows/publish-crates.yml").read_text()
        crates_prefix, crates_suffix = crates.rsplit(fragment, 1)
        with self.assertRaisesRegex(
            ValueError, "frozen current controller|exact annotated tag"
        ):
            release_workflows.verify_crates_controller_boundary(
                crates_prefix + "test true" + crates_suffix
            )

    def test_private_ci_matches_the_repository_rust_toolchain(self) -> None:
        gitlab_ci = ROOT / ".gitlab-ci.yml"
        if not gitlab_ci.exists():
            self.skipTest("private GitLab CI is absent from the public export")
        text = gitlab_ci.read_text(encoding="utf-8")
        for channel in ("1.97.0", "stable"):
            with self.subTest(channel=channel), tempfile.TemporaryDirectory() as temporary:
                toolchain = pathlib.Path(temporary) / "rust-toolchain.toml"
                toolchain.write_text(
                    f'[toolchain]\nchannel = "{channel}"\n'
                    'components = ["clippy", "rustfmt"]\n',
                    encoding="utf-8",
                )
                with mock.patch.object(release_workflows, "RUST_TOOLCHAIN", toolchain):
                    with self.assertRaisesRegex(ValueError, "repository authority"):
                        release_workflows.verify_private_mirror_ci(text)

    def test_private_ci_requires_real_mirror_lint_and_black_box(self) -> None:
        gitlab_ci = ROOT / ".gitlab-ci.yml"
        if not gitlab_ci.exists():
            self.skipTest("private GitLab CI is absent from the public export")
        text = gitlab_ci.read_text(encoding="utf-8")
        release_workflows.verify_private_mirror_ci(text)
        for fragment in (
            '$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH && $CI_COMMIT_REF_PROTECTED == "true"',
            "resource_group: public-mirror",
            "dependencies: [mirror-candidate]",
            "python:3.13.15-bookworm@sha256:933b46a028fd786c9c3d426ebabc237e29a15912231ea8de576e95f0e4f41a4c",
            "ghcr.io/gitleaks/gitleaks:v8.24.3@sha256:e1b35e12a8c6fa8901f060459cfb6b2fc4c484d3afbe3b029733a3bbfab07055",
            "shellcheck .gitlab/scripts/compute_public_source_snapshot.sh",
            "shellcheck .gitlab/scripts/publish-public-mirror.sh",
            "shellcheck .gitlab/scripts/scan_public_mirror_artifact.sh",
            "shellcheck .gitlab/scripts/install_pinned_pnpm.sh",
            "./.gitlab/scripts/scan_public_mirror_artifact.sh",
            "CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=scanner",
            "CYMULE_PUBLIC_MIRROR_TEST_COMPONENT=controller",
            "./.gitlab/scripts/test_public_mirror_controller.sh",
            "./.gitlab/scripts/prepare_public_mirror_candidate.sh",
            "./scripts/verify-sdk.sh go",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "immutable mirror closure|scanner closure"
            ):
                release_workflows.verify_private_mirror_ci(
                    text.replace(fragment, "missing-verification", 1)
                )
        with self.assertRaisesRegex(ValueError, "immutable mirror closure"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    "    - if: '$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH",
                    "    - if: '$CI_PIPELINE_SOURCE == \"web\"'\n"
                    "    - if: '$CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH",
                    1,
                )
            )
        validation_images = (
            "rust:1.97-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97",
            "node:26.7.0-bookworm@sha256:e929171d35b9df7773a3ec5b068e387fa109441dc90f91e6560af5d39b7e9bf1",
            "ghcr.io/astral-sh/uv:0.7.2-python3.13-bookworm@sha256:56acf02763dbd3b76cb51f8b204979472de76beab67197681d0a754a0395ff91",
            "golang:1.26.5-bookworm@sha256:53eeac89074db483fdf0ab3be1df32bf6e47562263d2d0d6baa7f26acb4957dd",
        )
        for image in validation_images:
            with self.subTest(image=image), self.assertRaisesRegex(
                ValueError, "immutable validation image"
            ):
                release_workflows.verify_private_mirror_ci(
                    text.replace(image, image.split("@", 1)[0], 1)
                )
        with self.assertRaisesRegex(ValueError, "unclosed component install"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    "rustup component add --toolchain 1.97.1 clippy rustfmt",
                    "rustup component add clippy rustfmt",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "pinned pnpm bytes"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    'export PATH="$(./.gitlab/scripts/install_pinned_pnpm.sh):$PATH"',
                    "npm install --global pnpm@11.17.0",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "verify-stage barrier"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    "  dependencies: [mirror-candidate]",
                    "  dependencies: [mirror-candidate]\n  needs: [mirror-candidate]",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "install or download executable bytes"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    "mirror-public:\n  stage: mirror",
                    "mirror-public:\n  stage: mirror\n  before_script:\n    - apt-get update",
                    1,
                )
            )
        with self.assertRaisesRegex(ValueError, "immutable mirror closure"):
            release_workflows.verify_private_mirror_ci(
                text.replace(
                    "python:3.13.15-bookworm@sha256:933b46a028fd786c9c3d426ebabc237e29a15912231ea8de576e95f0e4f41a4c",
                    "python:3.13-bookworm",
                    1,
                )
            )

        controller = (
            ROOT / ".gitlab/scripts/publish-public-mirror.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_private_mirror_controller(controller)
        for fragment in (
            "|| push_status=$?",
            'observed_tip=$(read_tip "$public_repository" refs/heads/main) || readback_status=$?',
            'if test "$observed_tip" != "$source_tip"; then',
            '"$timeout_binary" 30 "$ENV_BINARY" -i',
            'GIT_CONFIG_KEY_0="http.$url.extraHeader"',
            "GIT_NO_REPLACE_OBJECTS=1",
            'confirmed_public_tip=$(read_tip "$public_repository" refs/heads/main)',
            'private_source_snapshot=$("$bash_binary" "$snapshot_helper"',
            'candidate_source_snapshot=$("$bash_binary" "$snapshot_helper"',
            'if test "$candidate_source_snapshot" != "$private_source_snapshot"; then',
            'history_rewriter="$CI_PROJECT_DIR/.gitlab/scripts/rewrite_public_history.py"',
            'expected_source_tip=$("$ENV_BINARY" -i',
            '"$python_binary" "$history_rewriter"',
            'test "$source_tip" != "$expected_source_tip"',
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "closed Git or response-loss authority|no-op and write readbacks"
            ):
                release_workflows.verify_private_mirror_controller(
                    controller.replace(fragment, "missing-response-loss-closure")
                )

        artifact_scanner = (
            ROOT / ".gitlab/scripts/scan_public_mirror_artifact.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_public_mirror_artifact_scanner(artifact_scanner)
        for fragment in (
            'test "$CI_COMMIT_SHA" = "$(scan_git -C "$CI_PROJECT_DIR" rev-parse HEAD)"',
            'mapfile -t bundle_heads < <(scan_git bundle list-heads "$bundle_path")',
            'test "${#bundle_heads[@]}" -eq 1',
            'scan_git clone --quiet --no-local --no-checkout',
            'test "$candidate_snapshot" = "$private_snapshot"',
            '/bin/bash "$candidate_verifier" \\\n  --repository "$candidate_repository"',
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "exact candidate closure|bind the artifact"
            ):
                release_workflows.verify_public_mirror_artifact_scanner(
                    artifact_scanner.replace(fragment, "missing-artifact-closure", 1)
                )

        scanner = (
            ROOT / ".gitlab/scripts/verify_public_mirror_candidate.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_private_mirror_scanner(scanner)
        for fragment in (
            'git -C "$repository" rev-list --reverse --topo-order "$revision"',
            'git -C "$repository" ls-tree -r -z "$commit"',
            ".gitlab* | .github/workflows/mirror.yml",
            'git -C "$repository" cat-file commit "$commit"',
            "readonly MAX_PUBLIC_MIRROR_BLOB_BYTES=$((8 * 1024 * 1024))",
            "readonly GITLEAKS_MAX_TARGET_MEGABYTES=9",
            'printf \'%s\' "$path" > "$pathname_root/$pathname_record"',
            "--batch-check='%(objectname) %(objecttype) %(objectsize)'",
            'git -C "$repository" cat-file blob "$blob" > "$blob_record"',
            'reject_unsupported_blob_container "$blob" "$blob_record"',
            "readonly GIT_LFS_POINTER_HEADER_HEX=",
            "504b0304* | 504b0506* | 504b0708*",
            "28b52ffd* | 5[0-9a-f]2a4d18*",
            "213c617263683e0a* | 213c7468696e3e0a*",
            "tar_magic=$(od -An -v -tx1 -j 257 -N 5",
            'grep -aFrq -- "$marker" "$history_root"',
            "--ignore-gitleaks-allow",
            '--gitleaks-ignore-path "$empty_ignore_path"',
            '--max-target-megabytes "$GITLEAKS_MAX_TARGET_MEGABYTES"',
            "github-classic github-fine-grained npm aws private-key",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "whole-history or live-canary closure|rejection-canary"
            ):
                release_workflows.verify_private_mirror_scanner(
                    scanner.replace(fragment, "missing-scanner-closure", 1)
                )

        pnpm_installer = (
            ROOT / ".gitlab/scripts/install_pinned_pnpm.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_pinned_pnpm_installer(pnpm_installer)
        for fragment in (
            "cca3cea332ad254bb84145f966d19f4879615210346fc92c79a047f23a0d7b3cca3c3792f0076ba1f1831d277efbcf0a9119b31a9a60eca7fb3d6231f331ef72",
            "sha512sum -c -",
            "--proto '=https' --proto-redir '=https' --tlsv1.2",
        ):
            with self.subTest(fragment=fragment), self.assertRaisesRegex(
                ValueError, "pnpm installer"
            ):
                release_workflows.verify_pinned_pnpm_installer(
                    pnpm_installer.replace(fragment, "missing-byte-closure", 1)
                )

    def test_common_sdk_entrypoint_exports_every_conformance_fixture(self) -> None:
        entrypoint = (ROOT / "scripts/verify-sdk.sh").read_text(encoding="utf-8")
        release_workflows.verify_sdk_conformance_entrypoint(entrypoint)
        with self.assertRaisesRegex(ValueError, "complete SDK conformance"):
            release_workflows.verify_sdk_conformance_entrypoint(
                entrypoint.replace(
                    "export CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE",
                    "omitted CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE",
                    1,
                )
            )

    def test_main_ruleset_requires_one_exact_mirror_integration(self) -> None:
        valid = {
            "name": "main",
            "bypass_actors": [
                {
                    "actor_id": 42,
                    "actor_type": "Integration",
                    "bypass_mode": "always",
                }
            ],
        }
        github_settings.verify_main_bypass(valid, 42)
        for invalid in (
            {"name": "main", "bypass_actors": []},
            {
                "name": "main",
                "bypass_actors": [
                    {
                        "actor_id": 42,
                        "actor_type": "RepositoryRole",
                        "bypass_mode": "always",
                    }
                ],
            },
        ):
            with self.assertRaisesRegex(ValueError, "only exact mirror Integration"):
                github_settings.verify_main_bypass(invalid, 42)

    def test_main_ruleset_status_context_and_strictness_are_exact(self) -> None:
        valid = {
            "rules": [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [
                            {
                                "context": "Required CI",
                                "integration_id": (
                                    github_settings.GITHUB_ACTIONS_INTEGRATION_ID
                                ),
                            }
                        ],
                    },
                }
            ]
        }
        github_settings.verify_required_status_checks(valid)
        valid["rules"][0]["parameters"]["strict_required_status_checks_policy"] = False
        with self.assertRaisesRegex(ValueError, "current branch head"):
            github_settings.verify_required_status_checks(valid)

    def test_release_tag_ruleset_allows_only_the_exact_creator_integration(self) -> None:
        valid = {
            "name": "release-tags",
            "bypass_actors": [
                {
                    "actor_id": 91,
                    "actor_type": "Integration",
                    "bypass_mode": "always",
                }
            ],
        }
        github_settings.verify_release_tag_bypass(valid, 91)
        for invalid in (
            {"name": "release-tags", "bypass_actors": []},
            {
                "name": "release-tags",
                "bypass_actors": [
                    {
                        "actor_id": 92,
                        "actor_type": "Integration",
                        "bypass_mode": "always",
                    }
                ],
            },
            {
                "name": "release-tags",
                "bypass_actors": [
                    {
                        "actor_id": 91,
                        "actor_type": "Integration",
                        "bypass_mode": "pull_request",
                    }
                ],
            },
        ):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "tag creation only"
            ):
                github_settings.verify_release_tag_bypass(invalid, 91)

    def test_github_settings_ingress_rejects_duplicate_keys_and_floats(self) -> None:
        with self.assertRaisesRegex(ValueError, "repeats object member"):
            github_settings.load_github_json(b'{"enabled":true,"enabled":false}')
        with self.assertRaisesRegex(ValueError, "non-integral number"):
            github_settings.load_github_json(b'{"actor_id":1.0}')

    def test_release_tag_ruleset_requires_creation_and_owner_enforced_immutability(self) -> None:
        common = {
            "enforcement": "active",
            "target": "tag",
            "conditions": {
                "ref_name": {"include": ["refs/tags/v*"], "exclude": []}
            },
        }
        creation = {
            **common,
            "name": "release-tag-creation",
            "rules": [{"type": "creation"}],
        }
        immutable = {
            **common,
            "name": "immutable-release-tags",
            "rules": [{"type": "deletion"}, {"type": "update"}],
            "bypass_actors": [],
        }
        github_settings.require_exact_ruleset(
            [creation, immutable],
            target="tag",
            ref="refs/tags/v*",
            rules={"creation"},
        )
        github_settings.require_exact_ruleset(
            [creation, immutable],
            target="tag",
            ref="refs/tags/v*",
            rules={"deletion", "update"},
        )
        github_settings.verify_no_bypass(immutable)
        with self.assertRaisesRegex(ValueError, "one exact active tag ruleset"):
            github_settings.require_exact_ruleset(
                [{**creation, "rules": []}, immutable],
                target="tag",
                ref="refs/tags/v*",
                rules={"creation"},
            )
        with self.assertRaisesRegex(ValueError, "non-exact ref scope"):
            github_settings.require_exact_ruleset(
                [
                    {
                        **creation,
                        "conditions": {
                            **common["conditions"],
                            "repository_name": {
                                "include": ["cymule"],
                                "exclude": [],
                            },
                        },
                    },
                    immutable,
                ],
                target="tag",
                ref="refs/tags/v*",
                rules={"creation"},
            )
        with self.assertRaisesRegex(ValueError, "one exact active tag ruleset"):
            github_settings.require_exact_ruleset(
                [
                    {
                        **creation,
                        "rules": [{"type": "creation"}, {"type": "creation"}],
                    },
                    immutable,
                ],
                target="tag",
                ref="refs/tags/v*",
                rules={"creation"},
            )
        with self.assertRaisesRegex(ValueError, "no bypass actor"):
            github_settings.verify_no_bypass(
                {
                    **immutable,
                    "bypass_actors": [
                        {
                            "actor_id": 91,
                            "actor_type": "Integration",
                            "bypass_mode": "always",
                        }
                    ],
                }
            )
        github_settings.verify_immutable_releases(
            {"enabled": True, "enforced_by_owner": True}
        )
        for invalid in (
            {"enabled": False, "enforced_by_owner": True},
            {"enabled": True, "enforced_by_owner": False},
            {"enabled": True},
        ):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "enabled and enforced by the owner"
            ):
                github_settings.verify_immutable_releases(invalid)

    def test_ruleset_inventory_reads_every_page_before_exact_selection(self) -> None:
        first_page = [{"id": index} for index in range(1, 101)]
        second_page = [{"id": 101}]
        with mock.patch.object(
            github_settings,
            "github_json",
            side_effect=(first_page, second_page),
        ) as request:
            self.assertEqual(
                github_settings.github_json_list(
                    "cymule-framework/cymule",
                    "/rulesets?includes_parents=true",
                    "token",
                ),
                [*first_page, *second_page],
            )
        self.assertEqual(request.call_count, 2)
        self.assertIn("page=1", request.call_args_list[0].args[1])
        self.assertIn("page=2", request.call_args_list[1].args[1])

    def test_live_settings_rejects_another_default_before_reading_rulesets(self) -> None:
        for metadata in ({"default_branch": "not-main"}, {"default_branch": None}, {}, []):
            with self.subTest(metadata=metadata), mock.patch.object(
                github_settings, "github_json", return_value=metadata
            ) as request:
                with self.assertRaisesRegex(ValueError, "default branch must be exactly main"):
                    github_settings.verify(
                        "cymule-framework/cymule", "token", 41, 42, 51, 52, 53
                    )
                request.assert_called_once_with("cymule-framework/cymule", "", "token")

    def test_live_settings_verifier_materializes_the_closed_receipt_snapshot(self) -> None:
        main = {
            "id": 1,
            "name": "main",
            "enforcement": "active",
            "target": "branch",
            "conditions": {
                "ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}
            },
            "bypass_actors": [
                {
                    "actor_id": 41,
                    "actor_type": "Integration",
                    "bypass_mode": "always",
                }
            ],
            "rules": [
                {"type": "deletion"},
                {"type": "non_fast_forward"},
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [
                            {
                                "context": "Required CI",
                                "integration_id": (
                                    github_settings.GITHUB_ACTIONS_INTEGRATION_ID
                                ),
                            }
                        ],
                    },
                },
            ],
        }
        tag_creation = {
            "id": 2,
            "name": "release-tag-creation",
            "enforcement": "active",
            "target": "tag",
            "conditions": {
                "ref_name": {"include": ["refs/tags/v*"], "exclude": []}
            },
            "bypass_actors": [
                {
                    "actor_id": 42,
                    "actor_type": "Integration",
                    "bypass_mode": "always",
                }
            ],
            "rules": [{"type": "creation"}],
        }
        tag_immutable = {
            "id": 3,
            "name": "release-tag-immutable",
            "enforcement": "active",
            "target": "tag",
            "conditions": {
                "ref_name": {"include": ["refs/tags/v*"], "exclude": []}
            },
            "bypass_actors": [],
            "rules": [{"type": "deletion"}, {"type": "update"}],
        }

        def environment(team_id: int, *, npm: bool) -> dict[str, object]:
            return {
                "can_admins_bypass": False,
                "deployment_branch_policy": (
                    {
                        "protected_branches": False,
                        "custom_branch_policies": True,
                    }
                    if npm
                    else {
                        "protected_branches": True,
                        "custom_branch_policies": False,
                    }
                ),
                "protection_rules": [
                    {
                        "type": "required_reviewers",
                        "prevent_self_review": True,
                        "reviewers": [
                            {"type": "Team", "reviewer": {"id": team_id}}
                        ],
                    }
                ],
            }

        with mock.patch.object(
            github_settings,
            "github_json",
            side_effect=(
                {"default_branch": "main"},
                [{"id": 1}, {"id": 2}, {"id": 3}],
                main,
                tag_creation,
                tag_immutable,
                {
                    "default_workflow_permissions": "read",
                    "can_approve_pull_request_reviews": False,
                },
                {"enabled": True, "enforced_by_owner": True},
                environment(51, npm=True),
                {
                    "total_count": 2,
                    "branch_policies": [
                        {"id": 11, "name": "main", "type": "branch"},
                        {"id": 12, "name": "v*", "type": "tag"},
                    ],
                },
                environment(52, npm=False),
                environment(53, npm=False),
            ),
        ):
            self.assertEqual(
                github_settings.verify(
                    "cymule-framework/cymule",
                    "token",
                    41,
                    42,
                    51,
                    52,
                    53,
                ),
                FinalizeReleaseTests.settings_snapshot(),
            )

    def test_npm_environment_admits_only_main_and_release_tags(self) -> None:
        environment = {
            "can_admins_bypass": False,
            "deployment_branch_policy": {
                "protected_branches": False,
                "custom_branch_policies": True,
            },
            "protection_rules": [
                {
                    "type": "required_reviewers",
                    "prevent_self_review": True,
                    "reviewers": [{"type": "Team", "reviewer": {"id": 7}}],
                }
            ],
        }
        policies = {
            "total_count": 2,
            "branch_policies": [
                {"id": 1, "name": "main", "type": "branch"},
                {"id": 2, "name": "v*", "type": "tag"},
            ],
        }
        with mock.patch.object(
            github_settings, "github_json", side_effect=(environment, policies)
        ):
            self.assertEqual(
                github_settings.verify_environment(
                    "cymule-framework/cymule",
                    "npm",
                    "token",
                    expected_reviewers={("Team", 7)},
                    selected_refs={("branch", "main"), ("tag", "v*")},
                ),
                {
                    "can_admins_bypass": False,
                    "deployment_branch_policy": environment[
                        "deployment_branch_policy"
                    ],
                    "required_reviewers": [{"type": "Team", "id": 7}],
                    "selected_refs": [
                        {"type": "branch", "name": "main"},
                        {"type": "tag", "name": "v*"},
                    ],
                },
            )

        malformed = {
            **policies,
            "branch_policies": [{"id": 1, "name": "main", "type": "branch"}],
            "total_count": 1,
        }
        with mock.patch.object(
            github_settings, "github_json", side_effect=(environment, malformed)
        ), self.assertRaisesRegex(ValueError, "deployment refs must be exactly"):
            github_settings.verify_environment(
                "cymule-framework/cymule",
                "npm",
                "token",
                expected_reviewers={("Team", 7)},
                selected_refs={("branch", "main"), ("tag", "v*")},
            )

        malformed_reviewer = {
            **environment,
            "protection_rules": [
                {
                    "type": "required_reviewers",
                    "prevent_self_review": True,
                    "reviewers": [{}],
                }
            ],
        }
        with mock.patch.object(
            github_settings,
            "github_json",
            side_effect=(malformed_reviewer, policies),
        ), self.assertRaisesRegex(ValueError, "malformed or duplicate reviewers"):
            github_settings.verify_environment(
                "cymule-framework/cymule",
                "npm",
                "token",
                expected_reviewers={("Team", 7)},
                selected_refs={("branch", "main"), ("tag", "v*")},
            )

        for changed, message in (
            ({**environment, "can_admins_bypass": True}, "administrator bypass"),
            (
                {
                    key: value
                    for key, value in environment.items()
                    if key != "can_admins_bypass"
                },
                "administrator bypass",
            ),
            (
                {
                    **environment,
                    "protection_rules": [
                        {
                            "type": "required_reviewers",
                            "prevent_self_review": True,
                            "reviewers": [
                                {"type": "Team", "reviewer": {"id": 8}}
                            ],
                        }
                    ],
                },
                "reviewers must be exactly",
            ),
        ):
            with mock.patch.object(
                github_settings, "github_json", side_effect=(changed, policies)
            ), self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                github_settings.verify_environment(
                    "cymule-framework/cymule",
                    "npm",
                    "token",
                    expected_reviewers={("Team", 7)},
                    selected_refs={("branch", "main"), ("tag", "v*")},
                )

    def test_non_npm_environment_remains_protected_branch_only(self) -> None:
        environment = {
            "can_admins_bypass": False,
            "deployment_branch_policy": {
                "protected_branches": True,
                "custom_branch_policies": False,
            },
            "protection_rules": [
                {
                    "type": "required_reviewers",
                    "prevent_self_review": True,
                    "reviewers": [{"type": "Team", "reviewer": {"id": 7}}],
                }
            ],
        }
        with mock.patch.object(
            github_settings, "github_json", return_value=environment
        ):
            github_settings.verify_environment(
                "cymule-framework/cymule",
                "crates-io",
                "token",
                expected_reviewers={("Team", 7)},
            )


class PublicMirrorControllerTests(unittest.TestCase):
    def test_controller_pushes_reads_back_and_receipts_then_noops(self) -> None:
        controller = ROOT / ".gitlab/scripts/publish-public-mirror.sh"
        if not controller.is_file():
            self.skipTest("private mirror controller is absent from the public export")
        controller_text = controller.read_text(encoding="utf-8")
        release_workflows.verify_private_mirror_controller(controller_text)
        self.assertIn("readonly GIT_BINARY=/usr/bin/git", controller_text)
        self.assertIn(
            'history_rewriter="$CI_PROJECT_DIR/.gitlab/scripts/rewrite_public_history.py"',
            controller_text,
        )
        self.assertIn("candidate full history does not match", controller_text)
        self.assertIn("GIT_CONFIG_NOSYSTEM=1", controller_text)
        self.assertIn(
            '--force-with-lease="refs/heads/main:$public_tip"', controller_text
        )
        self.assertNotIn("push --force ", controller_text)
        self.assertNotIn("pip install", controller_text)
        self.assertNotIn("apt-get", controller_text)

        scanner_text = (
            ROOT / ".gitlab/scripts/verify_public_mirror_candidate.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_private_mirror_scanner(scanner_text)
        artifact_scanner_text = (
            ROOT / ".gitlab/scripts/scan_public_mirror_artifact.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_public_mirror_artifact_scanner(
            artifact_scanner_text
        )
        snapshot_helper = (
            ROOT / ".gitlab/scripts/compute_public_source_snapshot.sh"
        ).read_text(encoding="utf-8")
        release_workflows.verify_public_source_snapshot_helper(snapshot_helper)

        black_box = (
            ROOT / ".gitlab/scripts/test_public_mirror_controller.sh"
        ).read_text(encoding="utf-8")
        for fragment in (
            "published and read back public mirror",
            "public mirror already matches",
            "push response failed, but exact readback confirmed",
            "secret retained only in an old blob",
            "secret-bearing commit metadata",
            "front:0:128",
            "boundary:65536:128",
            "cross-boundary:65534:128",
            "end:128:0",
            "line-62189:62189",
            "line-62190:62190",
            "line-end:1000000",
            "allow-annotation",
            "repository-ignore-state",
            "mirror controller no-op race did not fail before receipt",
            "mirror controller accepted hostile $label environment",
            "mirror controller accepted hostile local Git config",
            "manifest-snapshot-tamper",
            "excluded-registry-only-coherent-tamper",
            "commit-message-coherent-tamper",
            "commit-author-coherent-tamper",
            "commit-parent-history-coherent-tamper",
            "remote-must-not-be-read",
            "artifact-code-sentinel",
            "artifact-code-noop-sentinel",
            "max_blob_bytes=$((8 * 1024 * 1024))",
            "oversized-blob 'exceeds public mirror blob limit'",
            "zip-archive 'unsupported archive/container blob (zip)'",
            "tar-archive 'unsupported archive/container blob (tar)'",
            "git-lfs-pointer 'unsupported Git LFS pointer'",
            "historical-pat-path",
            "historical-private-host-path",
            "root-gitlab-prefix",
        ):
            self.assertIn(fragment, black_box)


class PublicHistoryTests(unittest.TestCase):
    def test_rewrite_peak_memory_does_not_scale_with_total_history_volume(self) -> None:
        if public_history is None:
            self.skipTest("private mirror rewriter is absent from the public export")
        import gc
        import tracemalloc

        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "source"
            source.mkdir()
            subprocess.run(["git", "init", "-b", "main"], cwd=source, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Cymule Test"],
                cwd=source,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=source,
                check=True,
            )
            blob_size = 1024 * 1024
            blob_count = 16
            base = bytes(range(256)) * (blob_size // 256)
            for index in range(blob_count):
                value = bytearray(base)
                value[:8] = index.to_bytes(8, "big")
                source.joinpath(f"large-{index:02}.bin").write_bytes(value)
            subprocess.run(["git", "add", "."], cwd=source, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Add independent large blobs"],
                cwd=source,
                check=True,
            )
            metadata_size = 512 * 1024
            metadata_count = 16
            message_path = root / "commit-message"
            for index in range(metadata_count):
                message_path.write_text(
                    f"Large metadata {index}\n\n" + "m" * metadata_size,
                    encoding="utf-8",
                )
                subprocess.run(
                    ["git", "commit", "--allow-empty", "--file", str(message_path)],
                    cwd=source,
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=source,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            gc.collect()
            tracemalloc.start()
            try:
                public_history.rewrite(
                    source,
                    root / "public",
                    revision,
                    "private.example",
                    "group/cymule",
                )
                _current, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertLess(
                peak,
                blob_size * blob_count // 2,
                f"rewrite peak {peak} bytes scales with aggregate blob or metadata volume",
            )

    def test_public_source_snapshot_survives_real_history_rewrite(self) -> None:
        if public_history is None:
            self.skipTest("private mirror rewriter is absent from the public export")
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "source"
            source.mkdir()
            subprocess.run(["git", "init", "-b", "main"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.name", "Cymule Test"], cwd=source, check=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=source, check=True)
            source.joinpath("README.md").write_text("public source\n")
            executable = source / "tool.sh"
            executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            executable.chmod(0o755)
            source.joinpath("tool-link").symlink_to("tool.sh")
            source.joinpath(".gitlab").mkdir()
            source.joinpath(".gitlab/private.yml").write_text("private control\n")
            source.joinpath("versioning").mkdir()
            source.joinpath("versioning/version-domains.json").write_text("{}\n")
            subprocess.run(["git", "add", "."], cwd=source, check=True)
            subprocess.run(["git", "commit", "-m", "Add source"], cwd=source, check=True)
            private_sha = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=source, check=True,
                text=True, capture_output=True,
            ).stdout.strip()
            expected = npm_release.version_domains.current_source_snapshot_digest(source)
            helper = ROOT / ".gitlab/scripts/compute_public_source_snapshot.sh"
            helper_private = subprocess.run(
                [
                    str(helper),
                    "--repository",
                    str(source.resolve()),
                    "--revision",
                    private_sha,
                ],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            self.assertEqual(helper_private, expected)
            source.joinpath("README.md").write_text(
                "dirty working-tree value\n", encoding="utf-8"
            )
            source.joinpath("untracked.txt").write_text(
                "untracked value\n", encoding="utf-8"
            )
            helper_dirty = subprocess.run(
                [
                    str(helper),
                    "--repository",
                    str(source.resolve()),
                    "--revision",
                    private_sha,
                ],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            self.assertEqual(helper_dirty, expected)
            output = root / "public"
            rewritten_tip = public_history.rewrite(
                source, output, private_sha, "private.example", "group/cymule"
            )
            second_output = root / "public-second"
            self.assertEqual(
                public_history.rewrite(
                    source,
                    second_output,
                    private_sha,
                    "private.example",
                    "group/cymule",
                ),
                rewritten_tip,
            )
            subprocess.run(["git", "fsck", "--strict"], cwd=output, check=True)
            self.assertEqual(
                npm_release.version_domains.current_source_snapshot_digest(output),
                expected,
            )
            helper_public = subprocess.run(
                [
                    str(helper),
                    "--repository",
                    str(output.resolve()),
                    "--revision",
                    rewritten_tip,
                ],
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            self.assertEqual(helper_public, expected)
            exported_entries = subprocess.run(
                ["git", "ls-tree", "HEAD", "tool.sh", "tool-link"],
                cwd=output,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            self.assertIn("100755 blob", exported_entries)
            self.assertIn("120000 blob", exported_entries)
            self.assertNotEqual(
                subprocess.run(
                    ["git", "rev-parse", "HEAD"], cwd=output, check=True,
                    text=True, capture_output=True,
                ).stdout.strip(),
                private_sha,
            )

    def test_export_removes_the_mirror_controller_from_every_commit(self) -> None:
        if public_history is None:
            result = subprocess.run(
                [
                    "git",
                    "log",
                    "--all",
                    "--format=%H",
                    "--",
                    ".github/workflows/mirror.yml",
                ],
                cwd=ROOT,
                check=True,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.stdout, "")
            return
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "source"
            source.mkdir()
            subprocess.run(["git", "init", "-b", "main"], cwd=source, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Cymule Test"], cwd=source, check=True
            )
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=source,
                check=True,
            )
            mirror = source / ".github" / "workflows" / "mirror.yml"
            mirror.parent.mkdir(parents=True)
            internal = source / ".gitlab-internal.yml"
            private_secret = "CYMULE_" + "SOURCE_TOKEN"
            mirror.write_text(f"source: ${{{{ secrets.{private_secret} }}}}\n")
            internal.write_text("private control\n", encoding="utf-8")
            source.joinpath("README.md").write_text(
                "clone https://private.example/group/cymule\n", encoding="utf-8"
            )
            subprocess.run(["git", "add", "."], cwd=source, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Add source mirror"], cwd=source, check=True
            )
            mirror.unlink()
            subprocess.run(["git", "add", "-u"], cwd=source, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Retire source mirror"], cwd=source, check=True
            )
            revision = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=source,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            output = root / "public"
            public_history.rewrite(
                source,
                output,
                revision,
                "private.example",
                "group/cymule",
            )
            for private_path in (mirror, internal):
                history = subprocess.run(
                    [
                        "git",
                        "log",
                        "--all",
                        "--format=%H",
                        "--",
                        private_path.relative_to(source),
                    ],
                    cwd=output,
                    check=True,
                    text=True,
                    capture_output=True,
                ).stdout
                self.assertEqual(history, "")
                self.assertFalse(output.joinpath(private_path.relative_to(source)).exists())
            self.assertEqual(
                output.joinpath("README.md").read_text(encoding="utf-8"),
                "clone https://github.com/cymule-framework/cymule\n",
            )


if __name__ == "__main__":
    unittest.main()
