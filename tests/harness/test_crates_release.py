"""Unit tests for fail-closed crates.io publication recovery."""

from __future__ import annotations

import datetime
import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import tomllib
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "crates_release", ROOT / "scripts" / "crates_release.py"
)
assert SPEC is not None and SPEC.loader is not None
crates_release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = crates_release
SPEC.loader.exec_module(crates_release)


class NewCrateRateLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime.datetime(
            2026, 8, 18, 16, 0, 0, tzinfo=datetime.timezone.utc
        ).timestamp()

    def test_explicit_new_crate_limit_uses_server_time_plus_grace(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time. "
            "Please try again after Tue, 18 Aug 2026 16:02:13 GMT"
        )
        self.assertEqual(
            crates_release.new_crate_rate_limit_delay(output, now=self.now), 138
        )

    def test_unrelated_failure_is_never_retried(self) -> None:
        self.assertIsNone(
            crates_release.new_crate_rate_limit_delay(
                "status 403 Forbidden: token is invalid", now=self.now
            )
        )

    def test_rate_limit_without_server_timestamp_fails_closed(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time."
        )
        with self.assertRaisesRegex(ValueError, "omitted a retry timestamp"):
            crates_release.new_crate_rate_limit_delay(output, now=self.now)

    def test_excessive_server_delay_fails_closed(self) -> None:
        output = (
            "status 429 Too Many Requests: "
            "You have published too many new crates in a short period of time. "
            "Please try again after Tue, 18 Aug 2026 16:30:00 GMT"
        )
        with self.assertRaisesRegex(ValueError, "excessive"):
            crates_release.new_crate_rate_limit_delay(output, now=self.now)


class WorkspaceDryRunTests(unittest.TestCase):
    def test_candidate_workspace_patch_keeps_cargo_verification_enabled(self) -> None:
        crates = [
            crates_release.PublicCrate("cymule-core", ROOT / "crates/cymule-core", ()),
            crates_release.PublicCrate(
                "cymule-durable",
                ROOT / "crates/cymule-durable",
                ("cymule-core",),
            ),
        ]
        observed: dict[str, object] = {}

        def inspect(args: list[str], **_kwargs: object) -> None:
            observed["args"] = args
            config = pathlib.Path(args[args.index("--config") + 1])
            observed["config"] = tomllib.loads(config.read_text(encoding="utf-8"))

        with mock.patch.object(crates_release, "run", side_effect=inspect):
            crates_release.cargo_publish_dry_run(crates, allow_dirty=True)

        args = observed["args"]
        self.assertIsInstance(args, list)
        self.assertIn("--dry-run", args)
        self.assertIn("--allow-dirty", args)
        self.assertNotIn("--no-verify", args)
        config = observed["config"]
        self.assertEqual(
            config["patch"]["crates-io"]["cymule-durable"]["path"],
            str(ROOT / "crates/cymule-durable"),
        )


class ReleaseStageTests(unittest.TestCase):
    def test_stage_authenticates_catalog_order_commit_and_archives(self) -> None:
        crates = [
            crates_release.PublicCrate("cymule-core", ROOT / "crates/cymule-core", ()),
            crates_release.PublicCrate(
                "cymule-durable",
                ROOT / "crates/cymule-durable",
                ("cymule-core",),
            ),
        ]
        release_sha = "a" * 40
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            entries = []
            for crate in crates:
                archive = directory / f"{crate.name}-0.2.0.crate"
                archive.write_bytes(crate.name.encode())
                entries.append(
                    {
                        "name": crate.name,
                        "archive": archive.name,
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                    }
                )
            directory.joinpath("manifest.json").write_text(
                json.dumps(
                    {
                        "schema": "cymule.crates-release-stage/1",
                        "version": "0.2.0",
                        "release_sha": release_sha,
                        "crates": entries,
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                set(
                    crates_release.load_stage(
                        directory, crates, "0.2.0", release_sha
                    )
                ),
                {"cymule-core", "cymule-durable"},
            )
            with self.assertRaisesRegex(ValueError, "another version or commit"):
                crates_release.load_stage(directory, crates, "0.2.0", "b" * 40)

if __name__ == "__main__":
    unittest.main()
