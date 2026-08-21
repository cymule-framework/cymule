"""Tests for immutable public release and control-plane verification."""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import pathlib
import subprocess
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
                    "resolvedDependencies": [{"digest": {"gitCommit": release_sha}}],
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

    def test_provenance_name_and_digest_must_share_one_subject(self) -> None:
        release_sha = "c" * 40
        sha512 = "d" * 128
        statement = {
            "subject": [
                {"name": "pkg:npm/cymule@0.2.0", "digest": {"sha512": "e" * 128}},
                {"name": "pkg:npm/other@0.2.0", "digest": {"sha512": sha512}},
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
                    "resolvedDependencies": [{"digest": {"gitCommit": release_sha}}],
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
            with self.assertRaisesRegex(ValueError, "exact"):
                npm_release.verify_provenance(
                    f"{npm_release.REGISTRY}/-/attestations",
                    "cymule",
                    "0.2.0",
                    sha512,
                    release_sha,
                )


class WorkflowSecurityTests(unittest.TestCase):
    def test_public_workflows_are_pinned_and_credential_closed(self) -> None:
        release_workflows.verify()

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


class PublicHistoryTests(unittest.TestCase):
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
            private_secret = "CYMULE_" + "SOURCE_TOKEN"
            mirror.write_text(f"source: ${{{{ secrets.{private_secret} }}}}\n")
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
            history = subprocess.run(
                [
                    "git",
                    "log",
                    "--all",
                    "--format=%H",
                    "--",
                    mirror.relative_to(source),
                ],
                cwd=output,
                check=True,
                text=True,
                capture_output=True,
            ).stdout
            self.assertEqual(history, "")
            self.assertEqual(
                output.joinpath("README.md").read_text(encoding="utf-8"),
                "clone https://github.com/cymule-framework/cymule\n",
            )


if __name__ == "__main__":
    unittest.main()
