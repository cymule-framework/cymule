"""Tests for immutable public release and control-plane verification."""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import pathlib
import sys
import tarfile
import tempfile
import unittest
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


class NpmReleaseTests(unittest.TestCase):
    def make_stage(self, directory: pathlib.Path, release_sha: str) -> pathlib.Path:
        archive = directory / "cymule-0.2.0.tgz"
        package_json = json.dumps({"name": "cymule", "version": "0.2.0"}).encode()
        with tarfile.open(archive, "w:gz") as bundle:
            info = tarfile.TarInfo("package/package.json")
            info.size = len(package_json)
            bundle.addfile(info, io.BytesIO(package_json))
        identity = npm_release.archive_identity(archive)
        manifest = directory / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": "cymule.npm-release-stage/1",
                    "package": "cymule",
                    "version": "0.2.0",
                    "release_sha": release_sha,
                    "archive": archive.name,
                    **identity,
                }
            ),
            encoding="utf-8",
        )
        return manifest

    def test_stage_is_bound_to_the_verified_commit_and_bytes(self) -> None:
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            manifest = self.make_stage(directory, release_sha)
            npm_release.load_stage(manifest, release_sha)
            with self.assertRaisesRegex(ValueError, "another verified commit"):
                npm_release.load_stage(manifest, "b" * 40)
            with directory.joinpath("cymule-0.2.0.tgz").open("ab") as archive:
                archive.write(b"tamper")
            with self.assertRaisesRegex(ValueError, "digest"):
                npm_release.load_stage(manifest, release_sha)

    def test_provenance_must_bind_the_exact_release_sha(self) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        statement = {
            "subject": [
                {
                    "name": "pkg:npm/cymule@0.2.0",
                    "digest": {"sha512": sha512},
                }
            ],
            "predicate": {
                "buildDefinition": {
                    "externalParameters": {
                        "workflow": {
                            "ref": "refs/heads/main",
                            "repository": npm_release.REPOSITORY,
                            "path": npm_release.WORKFLOW,
                        }
                    },
                    "resolvedDependencies": [
                        {"digest": {"gitCommit": release_sha}}
                    ],
                }
            },
        }
        payload = {
            "attestations": [
                {
                    "predicateType": npm_release.SLSA_PROVENANCE,
                    "bundle": {
                        "dsseEnvelope": {
                            "payload": base64.b64encode(
                                json.dumps(statement).encode()
                            ).decode()
                        }
                    },
                }
            ]
        }
        with mock.patch.object(npm_release, "request_json", return_value=payload):
            npm_release.verify_provenance(
                f"{npm_release.REGISTRY}/-/attestations",
                "cymule",
                "0.2.0",
                sha512,
                release_sha,
            )
            with self.assertRaisesRegex(ValueError, "exact"):
                npm_release.verify_provenance(
                    f"{npm_release.REGISTRY}/-/attestations",
                    "cymule",
                    "0.2.0",
                    sha512,
                    "e" * 40,
                )


class WorkflowSecurityTests(unittest.TestCase):
    def test_public_workflows_are_pinned_and_credential_closed(self) -> None:
        release_workflows.verify()

    def test_broad_administrator_ruleset_bypass_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "broad"):
            github_settings.reject_admin_bypass(
                {
                    "name": "main",
                    "bypass_actors": [
                        {
                            "actor_type": "OrganizationAdmin",
                            "bypass_mode": "always",
                        }
                    ],
                }
            )


if __name__ == "__main__":
    unittest.main()
