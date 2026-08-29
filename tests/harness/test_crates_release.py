"""Unit tests for fail-closed crates.io publication recovery."""

from __future__ import annotations

import datetime
import hashlib
import importlib.util
import json
import pathlib
import tarfile
import io
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


class PublishGraphTests(unittest.TestCase):
    @staticmethod
    def package(*dependencies: dict[str, object]) -> dict[str, object]:
        return {"dependencies": list(dependencies)}

    @staticmethod
    def dependency(
        name: str,
        kind: str | None,
        requirement: str,
        *,
        target: str | None = None,
    ) -> dict[str, object]:
        return {
            "name": name,
            "kind": kind,
            "req": requirement,
            "target": target,
        }

    def test_publish_graph_keeps_every_cargo_normalized_edge_kind(self) -> None:
        prerequisites = {
            "cymule-normal",
            "cymule-build",
            "cymule-dev",
            "cymule-target-normal",
            "cymule-target-build",
            "cymule-target-dev",
            "cymule-path-only-dev",
        }
        packages = {name: self.package() for name in prerequisites}
        packages["cymule-owner"] = self.package(
            self.dependency("cymule-normal", None, "^0.2.0"),
            self.dependency("cymule-build", "build", "^0.2.0"),
            self.dependency("cymule-dev", "dev", "^0.2.0"),
            self.dependency("cymule-target-normal", None, "^0.2.0", target="cfg(unix)"),
            self.dependency(
                "cymule-target-build", "build", "^0.2.0", target="cfg(unix)"
            ),
            self.dependency("cymule-target-dev", "dev", "^0.2.0", target="cfg(unix)"),
            self.dependency("cymule-path-only-dev", "dev", "*"),
            self.dependency("serde", None, "^1.0"),
        )
        public = prerequisites | {"cymule-owner"}

        graph = crates_release.cargo_publish_graph(packages, public)

        self.assertEqual(
            graph["cymule-owner"],
            prerequisites - {"cymule-path-only-dev"},
        )

    def test_terminal_manifest_graph_matches_cargo_normalization(self) -> None:
        prerequisites = (
            "cymule-normal",
            "cymule-build",
            "cymule-dev",
            "cymule-target-normal",
            "cymule-target-build",
            "cymule-target-dev",
            "cymule-path-only-dev",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            catalog = []
            for name in (*prerequisites, "cymule-owner"):
                relative = f"packages/{name}"
                path = root / relative
                path.mkdir(parents=True)
                path.joinpath("Cargo.toml").write_text(
                    f'[package]\nname = "{name}"\n', encoding="utf-8"
                )
                catalog.append({"name": name, "path": relative, "dependencies": []})
            root.joinpath("packages/cymule-owner/Cargo.toml").write_text(
                '[package]\nname = "cymule-owner"\n\n'
                "[dependencies]\n"
                'cymule-normal = { version = "0.2.0", path = "../cymule-normal" }\n\n'
                "[build-dependencies]\n"
                'cymule-build = { version = "0.2.0", path = "../cymule-build" }\n\n'
                "[dev-dependencies]\n"
                'cymule-dev = { version = "0.2.0", path = "../cymule-dev" }\n'
                'cymule-path-only-dev = { path = "../cymule-path-only-dev" }\n\n'
                "[target.'cfg(unix)'.dependencies]\n"
                'cymule-target-normal = { version = "0.2.0", path = "../cymule-target-normal" }\n\n'
                "[target.'cfg(unix)'.build-dependencies]\n"
                'cymule-target-build = { version = "0.2.0", path = "../cymule-target-build" }\n\n'
                "[target.'cfg(unix)'.dev-dependencies]\n"
                'cymule-target-dev = { version = "0.2.0", path = "../cymule-target-dev" }\n',
                encoding="utf-8",
            )

            graph = crates_release.version_domains.manifest_publish_graph(
                root, catalog, {}, "0.2.0"
            )

        self.assertEqual(
            graph["cymule-owner"],
            set(prerequisites) - {"cymule-path-only-dev"},
        )

    def test_current_clock_consumers_follow_the_complete_publish_graph(self) -> None:
        crates = crates_release.load_catalog()
        by_name = {crate.name: crate for crate in crates}
        positions = {crate.name: index for index, crate in enumerate(crates)}
        clock_consumers = (
            "cymule-directory-store",
            "cymule-store-sqlite",
            "cymule-activation-http",
            "cymule-activation-timer",
            "cymule-cli",
        )
        for consumer in clock_consumers:
            with self.subTest(consumer=consumer):
                self.assertIn("cymule-clock-system", by_name[consumer].dependencies)
                self.assertLess(positions["cymule-clock-system"], positions[consumer])
        for dependency in ("cymule-runtime", "cymule-store-sqlite"):
            with self.subTest(agent_dev_dependency=dependency):
                self.assertIn(dependency, by_name["cymule-agent"].dependencies)
                self.assertLess(positions[dependency], positions["cymule-agent"])
        self.assertEqual(sum(len(crate.dependencies) for crate in crates), 87)

    def test_versioned_dev_cycle_is_rejected_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            scripts = root / "scripts"
            scripts.mkdir()
            packages = root / "packages"
            packages.mkdir()
            members = []
            for name, dependency in (
                ("cymule-a", "cymule-b"),
                ("cymule-b", "cymule-a"),
            ):
                path = packages / name
                path.mkdir()
                members.append(f'  "packages/{name}",')
                path.joinpath("Cargo.toml").write_text(
                    "[package]\n"
                    f'name = "{name}"\n'
                    "version.workspace = true\n"
                    "publish.workspace = true\n\n"
                    "[dev-dependencies]\n"
                    f'{dependency} = {{ version = "0.2.0", path = "../{dependency}" }}\n',
                    encoding="utf-8",
                )
            root.joinpath("Cargo.toml").write_text(
                "[workspace]\n"
                "members = [\n" + "\n".join(members) + "\n]\n\n"
                "[workspace.package]\n"
                'version = "0.2.0"\n'
                'publish = ["crates-io"]\n',
                encoding="utf-8",
            )
            catalog = scripts / "crates-release.toml"
            catalog.write_text(
                "schema = 1\n\n"
                "[[crate]]\n"
                'name = "cymule-a"\n'
                'path = "packages/cymule-a"\n'
                'dependencies = ["cymule-b"]\n\n'
                "[[crate]]\n"
                'name = "cymule-b"\n'
                'path = "packages/cymule-b"\n'
                'dependencies = ["cymule-a"]\n',
                encoding="utf-8",
            )
            synthetic_catalog = [
                {
                    "name": "cymule-a",
                    "path": "packages/cymule-a",
                    "dependencies": ["cymule-b"],
                },
                {
                    "name": "cymule-b",
                    "path": "packages/cymule-b",
                    "dependencies": ["cymule-a"],
                },
            ]
            manifest_graph = crates_release.version_domains.manifest_publish_graph(
                root, synthetic_catalog, {}, "0.2.0"
            )
            self.assertEqual(
                manifest_graph,
                {"cymule-a": {"cymule-b"}, "cymule-b": {"cymule-a"}},
            )
            with self.assertRaisesRegex(ValueError, "contains a cycle"):
                crates_release.version_domains.deterministic_release_catalog_order(
                    manifest_graph, ("cymule-a", "cymule-b")
                )
            with (
                mock.patch.object(crates_release, "ROOT", root),
                mock.patch.object(crates_release, "CATALOG_PATH", catalog),
            ):
                with self.assertRaisesRegex(ValueError, "contains a cycle"):
                    crates_release.load_catalog()


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


class RegistryMutationFenceTests(unittest.TestCase):
    def test_remote_main_or_tag_move_stops_before_the_next_crate_put(self) -> None:
        controller_sha = "a" * 40
        release_sha = "b" * 40
        release_tag = "v0.2.0"
        expected = (
            f"{controller_sha}\trefs/heads/main\n"
            f"{release_sha}\trefs/tags/{release_tag}^{{}}\n"
        )
        moved_refs = (
            (
                f"{'c' * 40}\trefs/heads/main\n"
                f"{release_sha}\trefs/tags/{release_tag}^{{}}\n"
            ),
            (
                f"{controller_sha}\trefs/heads/main\n"
                f"{'d' * 40}\trefs/tags/{release_tag}^{{}}\n"
            ),
        )
        for moved in moved_refs:
            with self.subTest(moved=moved), tempfile.TemporaryDirectory() as temporary:
                upload = pathlib.Path(temporary) / "crate.publish"
                upload.write_bytes(b"closed body")
                authority = lambda: crates_release.verify_remote_release_authority(
                    controller_sha, release_sha, release_tag
                )
                with (
                    mock.patch.object(
                        crates_release,
                        "run",
                        side_effect=[
                            mock.Mock(stdout=expected),
                            mock.Mock(stdout=moved),
                        ],
                    ),
                    mock.patch.object(
                        crates_release.urllib.request,
                        "urlopen",
                        return_value=io.BytesIO(b"{}"),
                    ) as put,
                    mock.patch.object(crates_release, "wait_for_checksum"),
                ):
                    crates_release.publish_crate(
                        "cymule-first",
                        "0.2.0",
                        "e" * 64,
                        upload,
                        "token",
                        authority,
                    )
                    with self.assertRaisesRegex(ValueError, "authority moved"):
                        crates_release.publish_crate(
                            "cymule-second",
                            "0.2.0",
                            "e" * 64,
                            upload,
                            "token",
                            authority,
                        )
                    self.assertEqual(put.call_count, 1)

    def test_put_transport_or_http_loss_reconciles_exact_checksum(self) -> None:
        errors = (
            crates_release.urllib.error.URLError("connection lost"),
            TimeoutError("response timed out"),
            crates_release.urllib.error.HTTPError(
                "https://crates.io/api/v1/crates/new",
                500,
                "Internal Server Error",
                {},
                io.BytesIO(b"response lost"),
            ),
        )
        for error in errors:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as temporary:
                upload = pathlib.Path(temporary) / "crate.publish"
                upload.write_bytes(b"closed body")
                with (
                    mock.patch.object(
                        crates_release.urllib.request,
                        "urlopen",
                        side_effect=error,
                    ),
                    mock.patch.object(
                        crates_release, "wait_for_checksum"
                    ) as readback,
                ):
                    crates_release.publish_crate(
                        "cymule-core",
                        "0.2.0",
                        "e" * 64,
                        upload,
                        "token",
                        lambda: None,
                    )
                readback.assert_called_once_with(
                    "cymule-core", "0.2.0", "e" * 64
                )

    def test_put_loss_missing_conflict_and_unavailable_never_succeed(self) -> None:
        outcomes = (
            (
                crates_release.RegistryChecksumMissing("still absent"),
                "did not publish",
            ),
            (ValueError("registry checksum mismatch"), "checksum mismatch"),
            (
                crates_release.CratePublishOutcomeAmbiguous(
                    "crate_publish_outcome_ambiguous"
                ),
                "crate_publish_outcome_ambiguous",
            ),
        )
        for readback_error, expected in outcomes:
            with self.subTest(readback_error=readback_error), tempfile.TemporaryDirectory() as temporary:
                upload = pathlib.Path(temporary) / "crate.publish"
                upload.write_bytes(b"closed body")
                with (
                    mock.patch.object(
                        crates_release.urllib.request,
                        "urlopen",
                        side_effect=crates_release.urllib.error.URLError(
                            "connection lost"
                        ),
                    ),
                    mock.patch.object(
                        crates_release,
                        "wait_for_checksum",
                        side_effect=readback_error,
                    ),
                ):
                    with self.assertRaisesRegex(Exception, expected):
                        crates_release.publish_crate(
                            "cymule-core",
                            "0.2.0",
                            "e" * 64,
                            upload,
                            "token",
                            lambda: None,
                        )

    def test_new_name_rate_limit_retries_only_after_absence_readback(self) -> None:
        error = crates_release.urllib.error.HTTPError(
            "https://crates.io/api/v1/crates/new",
            429,
            "Too Many Requests",
            {},
            io.BytesIO(
                b"You have published too many new crates in a short period of time. "
                b"Please try again after Tue, 18 Aug 2026 16:02:13 GMT"
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            upload = pathlib.Path(temporary) / "crate.publish"
            upload.write_bytes(b"closed body")
            with (
                mock.patch.object(
                    crates_release.urllib.request,
                    "urlopen",
                    side_effect=[error, io.BytesIO(b"{}")],
                ) as put,
                mock.patch.object(
                    crates_release,
                    "wait_for_checksum",
                    side_effect=[
                        crates_release.RegistryChecksumMissing("absent"),
                        None,
                    ],
                ) as readback,
                mock.patch.object(
                    crates_release, "new_crate_rate_limit_delay", return_value=1
                ),
                mock.patch.object(crates_release.time, "sleep"),
            ):
                crates_release.publish_crate(
                    "cymule-new",
                    "0.2.0",
                    "e" * 64,
                    upload,
                    "token",
                    lambda: None,
                )
            self.assertEqual(put.call_count, 2)
            self.assertEqual(readback.call_count, 2)

    def test_checksum_readback_distinguishes_absent_from_unavailable(self) -> None:
        cases = (
            (None, crates_release.RegistryChecksumMissing, "did not index"),
            (
                crates_release.urllib.error.URLError("offline"),
                crates_release.CratePublishOutcomeAmbiguous,
                "crate_publish_outcome_ambiguous",
            ),
        )
        for observation, exception, expected in cases:
            with self.subTest(observation=observation):
                with (
                    mock.patch.object(
                        crates_release,
                        "registry_checksum",
                        side_effect=observation
                        if isinstance(observation, BaseException)
                        else None,
                        return_value=observation
                        if not isinstance(observation, BaseException)
                        else None,
                    ),
                    mock.patch.object(
                        crates_release.time, "monotonic", side_effect=[0, 301]
                    ),
                ):
                    with self.assertRaisesRegex(exception, expected):
                        crates_release.wait_for_checksum(
                            "cymule-core", "0.2.0", "e" * 64
                        )


class WorkspaceDryRunTests(unittest.TestCase):
    def test_catalog_rejects_external_and_symlink_package_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = pathlib.Path(temporary)
            root = base / "payload"
            root.mkdir()
            scripts = root / "scripts"
            scripts.mkdir()
            external = base / "external"
            external.mkdir()
            external.joinpath("Cargo.toml").write_text("[package]\nname='x'\n")
            link = root / "linked"
            link.symlink_to(external, target_is_directory=True)
            catalog = scripts / "crates-release.toml"
            for path, expected in (
                ("../external", "invalid public crate path"),
                (str(external), "invalid public crate path"),
                ("linked", "symlink"),
            ):
                with self.subTest(path=path):
                    catalog.write_text(
                        "schema = 1\n\n[[crate]]\n"
                        'name = "cymule-core"\n'
                        f"path = {json.dumps(path)}\n"
                        "dependencies = []\n",
                        encoding="utf-8",
                    )
                    with (
                        mock.patch.object(crates_release, "ROOT", root),
                        mock.patch.object(crates_release, "CATALOG_PATH", catalog),
                    ):
                        with self.assertRaisesRegex(ValueError, expected):
                            crates_release.load_catalog()

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
    def make_stage(
        self, directory: pathlib.Path
    ) -> tuple[list[crates_release.PublicCrate], pathlib.Path, pathlib.Path]:
        crate = crates_release.PublicCrate(
            "cymule-core", ROOT / "crates/cymule-core", ()
        )
        archive = directory / "cymule-core-0.2.0.crate"
        cargo_manifest = (
            '[package]\nname = "cymule-core"\nversion = "0.2.0"\n'
            'edition = "2024"\nlicense = "MIT"\n'
        ).encode()
        with tarfile.open(archive, "w:gz") as bundle:
            info = tarfile.TarInfo("cymule-core-0.2.0/Cargo.toml")
            info.size = len(cargo_manifest)
            bundle.addfile(info, io.BytesIO(cargo_manifest))
        upload = directory / "cymule-core-0.2.0.publish"
        crates_release.write_upload_body(archive, crate.name, "0.2.0", upload)
        manifest = directory / "manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": "cymule.crates-release-stage/3",
                    "version": "0.2.0",
                    "release_sha": "a" * 40,
                    "version_domain_registry_digest": crates_release.version_registry_digest(),
                    "crates": [
                        {
                            "name": crate.name,
                            "archive": archive.name,
                            "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                            "upload": upload.name,
                            "upload_sha256": hashlib.sha256(
                                upload.read_bytes()
                            ).hexdigest(),
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return [crate], manifest, archive

    def test_registry_digest_delegates_to_the_version_authority(self) -> None:
        with mock.patch.object(
            crates_release.version_domains,
            "registry_digest",
            return_value="sha256:" + "2" * 64,
        ) as digest:
            self.assertEqual(
                crates_release.version_registry_digest(), "sha256:" + "2" * 64
            )
            digest.assert_called_once_with(crates_release.ROOT)

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
                manifest = (
                    f'[package]\nname = "{crate.name}"\nversion = "0.2.0"\n'
                    'edition = "2024"\nlicense = "MIT"\n'
                ).encode()
                with tarfile.open(archive, "w:gz") as bundle:
                    info = tarfile.TarInfo(f"{crate.name}-0.2.0/Cargo.toml")
                    info.size = len(manifest)
                    bundle.addfile(info, io.BytesIO(manifest))
                upload = directory / f"{crate.name}-0.2.0.publish"
                crates_release.write_upload_body(archive, crate.name, "0.2.0", upload)
                entries.append(
                    {
                        "name": crate.name,
                        "archive": archive.name,
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "upload": upload.name,
                        "upload_sha256": hashlib.sha256(
                            upload.read_bytes()
                        ).hexdigest(),
                    }
                )
            directory.joinpath("manifest.json").write_text(
                json.dumps(
                    {
                        "schema": "cymule.crates-release-stage/3",
                        "version": "0.2.0",
                        "release_sha": release_sha,
                        "version_domain_registry_digest": crates_release.version_registry_digest(),
                        "crates": entries,
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                set(crates_release.load_stage(directory, crates, "0.2.0", release_sha)),
                {"cymule-core", "cymule-durable"},
            )
            with self.assertRaisesRegex(ValueError, "stable SemVer"):
                crates_release.load_stage(
                    directory,
                    crates,
                    "0.2.0\ncontroller_sha=attacker",
                    release_sha,
                )
            with self.assertRaisesRegex(ValueError, "another version or commit"):
                crates_release.load_stage(directory, crates, "0.2.0", "b" * 40)
            manifest_path = directory / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["version_domain_registry_digest"] = "sha256:" + "0" * 64
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "version-domain generation"):
                crates_release.load_stage(directory, crates, "0.2.0", release_sha)

    def test_stage_and_close_reject_duplicate_members_and_floats(self) -> None:
        release_sha = "a" * 40
        malformed_values = (
            '{"schema":"cymule.crates-release-stage/3","schema":"other"}',
            '{"schema":"cymule.crates-release-stage/3","float":1.5}',
        )
        for malformed in malformed_values:
            with self.subTest(malformed=malformed), tempfile.TemporaryDirectory() as temporary:
                directory = pathlib.Path(temporary)
                directory.joinpath("manifest.json").write_text(
                    malformed, encoding="utf-8"
                )
                with self.assertRaisesRegex(ValueError, "strict I-JSON"):
                    crates_release.load_stage(directory, [], "0.2.0", release_sha)
                with self.assertRaisesRegex(ValueError, "strict I-JSON"):
                    crates_release.compare_stages(directory, directory)

        cargo_manifest = (
            '[package]\nname = "cymule-core"\nversion = "0.2.0"\n'
            'edition = "2024"\nlicense = "MIT"\n'
        ).encode()
        for member_kind in ("duplicate", "special"):
            with self.subTest(member_kind=member_kind), tempfile.TemporaryDirectory() as temporary:
                archive = pathlib.Path(temporary) / "cymule-core-0.2.0.crate"
                with tarfile.open(archive, "w:gz") as bundle:
                    info = tarfile.TarInfo("cymule-core-0.2.0/Cargo.toml")
                    info.size = len(cargo_manifest)
                    bundle.addfile(info, io.BytesIO(cargo_manifest))
                    if member_kind == "duplicate":
                        duplicate = tarfile.TarInfo(
                            "cymule-core-0.2.0/Cargo.toml"
                        )
                        duplicate.size = len(cargo_manifest)
                        bundle.addfile(duplicate, io.BytesIO(cargo_manifest))
                    else:
                        special = tarfile.TarInfo("cymule-core-0.2.0/device")
                        special.type = tarfile.CHRTYPE
                        bundle.addfile(special)
                with self.assertRaisesRegex(ValueError, "unsafe archive member"):
                    crates_release.publish_metadata(
                        archive, "cymule-core", "0.2.0"
                    )

    def test_stage_registry_evidence_binds_checksum_and_download(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            crates, _manifest, archive = self.make_stage(directory)
            expected = hashlib.sha256(archive.read_bytes()).hexdigest()
            with (
                mock.patch.object(crates_release, "load_catalog", return_value=crates),
                mock.patch.object(
                    crates_release, "registry_checksum", return_value=expected
                ),
                mock.patch.object(
                    crates_release, "verify_download", return_value=expected
                ),
            ):
                evidence = crates_release.stage_registry_evidence(
                    directory, "0.2.0", "a" * 40
                )
            self.assertEqual(
                evidence,
                [
                    {
                        "package_id": "cargo:cymule-core",
                        "name": "cymule-core",
                        "version": "0.2.0",
                        "publication": {
                            "kind": "cargo",
                            "registry": "https://crates.io/",
                            "registry_identity": (
                                "https://crates.io/crates/cymule-core/0.2.0"
                            ),
                            "content_digest": f"sha256:{expected}",
                            "provenance": {
                                "kind": "registry-checksum",
                                "checksum": f"sha256:{expected}",
                                "download_url": (
                                    "https://static.crates.io/crates/cymule-core/"
                                    "cymule-core-0.2.0.crate"
                                ),
                            },
                        },
                    }
                ],
            )

    def test_stage_rejects_traversal_symlinks_and_extra_files(self) -> None:
        cases = ("traversal", "symlink", "extra")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temporary:
                root = pathlib.Path(temporary)
                directory = root / "stage"
                directory.mkdir()
                crates, manifest, archive = self.make_stage(directory)
                if case == "traversal":
                    value = json.loads(manifest.read_text(encoding="utf-8"))
                    value["crates"][0]["archive"] = (
                        "../cymule-core-0.2.0.crate"
                    )
                    manifest.write_text(json.dumps(value), encoding="utf-8")
                    expected = "expected basename"
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
                    crates_release.load_stage(
                        directory, crates, "0.2.0", "a" * 40
                    )


if __name__ == "__main__":
    unittest.main()
