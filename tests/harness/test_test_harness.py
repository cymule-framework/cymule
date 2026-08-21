"""Tests for conservative Cymule change-to-suite routing."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("cymule_test_harness", ROOT / "scripts" / "test_harness.py")
assert SPEC is not None and SPEC.loader is not None
HARNESS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HARNESS)


class ChangeRoutingTests(unittest.TestCase):
    """Prove narrow routes stay narrow and unknown risk fails closed."""

    def test_docs_change_stays_in_meta_lane(self) -> None:
        suites, _ = HARNESS.select_suites(["docs/architecture.md"])
        self.assertEqual(suites, ["docs"])

    def test_virtual_semantics_select_every_language_contract(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-virtual/src/model.rs"])
        self.assertEqual(
            set(suites),
            {
                "example",
                "protocol",
                "rust-evolution",
                "rust-virtual",
                "sdk-go",
                "sdk-python",
                "sdk-rust",
                "sdk-typescript",
            },
        )

    def test_language_sdk_change_does_not_select_other_languages(self) -> None:
        suites, _ = HARNESS.select_suites(["sdk/go/cymule.go"])
        self.assertEqual(suites, ["sdk-go"])

    def test_core_property_test_change_stays_in_core_suite(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-core/tests/semantic_kernel.rs"])
        self.assertEqual(suites, ["rust-core"])

    def test_durable_executor_change_does_not_run_unrelated_profiles(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-durable/src/executor.rs"])
        self.assertIn("rust-durable", suites)
        self.assertIn("rust-agent-plugin", suites)
        self.assertIn("rust-store-plugins", suites)
        self.assertNotIn("sdk-go", suites)
        self.assertNotIn("sdk-python", suites)
        self.assertNotIn("sdk-typescript", suites)

    def test_durable_store_contract_change_selects_transitive_consumers(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-durable/src/store.rs"])
        for expected in (
            "rust-agent-plugin",
            "rust-agent-mcp-plugin",
            "rust-directory-plugin",
            "rust-durable",
            "rust-resource",
            "rust-store-plugins",
            "rust-virtual",
        ):
            self.assertIn(expected, suites)

    def test_test_world_change_selects_every_behavioral_consumer(self) -> None:
        suites, _ = HARNESS.select_suites(["tests/test-world/src/lib.rs"])
        for expected in (
            "test-world-deterministic",
            "test-world-live-process",
            "rust-durable",
            "rust-store-plugins",
            "rust-agent-plugin",
            "rust-resource-plugins",
            "rust-activation-http",
            "rust-activation-timer",
            "rust-clock-system",
        ):
            self.assertIn(expected, suites)

    def test_unknown_path_escalates_to_full(self) -> None:
        suites, evidence = HARNESS.select_suites(["future-domain/meaning.rs"])
        self.assertEqual(suites, ["full"])
        self.assertEqual(evidence, {"full": ["future-domain/meaning.rs"]})

    def test_any_unknown_path_escalates_the_complete_change(self) -> None:
        suites, _ = HARNESS.select_suites(["docs/architecture.md", "future-domain/meaning.rs"])
        self.assertEqual(suites, ["full"])

    def test_full_expands_to_independent_lanes(self) -> None:
        manifest = HARNESS.load_manifest()
        matrix = HARNESS.ci_matrix(["full"], manifest)
        lanes = {entry["lane"] for entry in matrix["include"]}
        self.assertIn("rust-static", lanes)
        self.assertIn("rust-semantic", lanes)
        self.assertIn("rust-durable", lanes)
        self.assertIn("rust-plugins", lanes)
        self.assertIn("rust-package", lanes)
        self.assertIn("sdk-typescript", lanes)
        self.assertIn("sdk-python", lanes)
        self.assertIn("sdk-go", lanes)
        self.assertIn("meta", lanes)
        rust_lane = next(
            entry for entry in matrix["include"] if entry["lane"] == "rust-durable"
        )
        self.assertEqual(
            set(rust_lane["execution_classes"]),
            {"deterministic", "live_process"},
        )

    def test_every_leaf_has_one_execution_class(self) -> None:
        manifest = HARNESS.load_manifest()
        classes = HARNESS.suite_execution_classes(manifest)
        leaves = {
            name
            for name, suite in manifest["suites"].items()
            if not suite.get("abstract", False)
        }
        self.assertEqual(set(classes), leaves)

    def test_duplicate_execution_class_is_rejected(self) -> None:
        manifest = HARNESS.load_manifest()
        manifest["execution_classes"]["live_provider"]["suites"].append("rust-core")
        with self.assertRaisesRegex(ValueError, "duplicate execution classes"):
            HARNESS.validate_manifest(manifest)

    def test_route_catalog_rejects_unknown_suite(self) -> None:
        manifest = HARNESS.load_manifest()
        manifest["routes"][0]["suites"] = ["missing-suite"]
        with self.assertRaisesRegex(ValueError, "unknown suites"):
            HARNESS.validate_manifest(manifest)

    def test_invalid_leaf_command_is_rejected_before_execution(self) -> None:
        manifest = self._run_manifest([])
        with self.assertRaisesRegex(ValueError, "invalid command"):
            HARNESS.validate_manifest(manifest)

    def test_cargo_source_change_reports_owner_and_transitive_consumers(self) -> None:
        affected = HARNESS.cargo_affected_packages(
            ["crates/cymule-evolution/src/control.rs"]
        )
        packages = affected["crates/cymule-evolution/src/control.rs"]
        self.assertIn("cymule-evolution", packages)
        self.assertIn("cymule", packages)
        self.assertIn("cymule-cli", packages)

    def test_agent_source_change_compiles_agent_mcp_consumer(self) -> None:
        affected = HARNESS.cargo_affected_packages(
            ["plugins/agent-interaction/src/lib.rs"]
        )
        self.assertIn(
            "cymule-agent-mcp",
            affected["plugins/agent-interaction/src/lib.rs"],
        )
        suites, _ = HARNESS.select_suites(["plugins/agent-interaction/src/lib.rs"])
        self.assertIn("rust-agent-plugin", suites)
        self.assertIn("rust-agent-mcp-plugin", suites)

    def test_harness_authority_changes_select_the_complete_catalog(self) -> None:
        for path in ("scripts/test_harness.py", "tests/harness/suites.toml"):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, ["catalog"])
            expanded = HARNESS.expand_suites(suites, HARNESS.load_manifest())
            self.assertIn("rust-soak", expanded)
            self.assertIn("rust-mutation", expanded)

    def test_soak_runner_change_selects_only_the_soak_leaf(self) -> None:
        suites, _ = HARNESS.select_suites(["scripts/verify-soak.sh"])
        self.assertEqual(suites, ["rust-soak"])

    def test_release_scripts_select_their_package_and_security_witnesses(self) -> None:
        npm_suites, _ = HARNESS.select_suites(["scripts/npm_release.py"])
        self.assertEqual(
            set(npm_suites), {"package-typescript", "release-workflows"}
        )
        crates_suites, _ = HARNESS.select_suites(["scripts/crates_release.py"])
        self.assertEqual(set(crates_suites), {"package-rust", "release-workflows"})

    def test_shared_version_sources_select_the_complete_release_lock(self) -> None:
        for path in (
            "Cargo.toml",
            "sdk/typescript/package.json",
            "sdk/python/pyproject.toml",
        ):
            suites, _ = HARNESS.select_suites([path])
            self.assertIn("package-rust", suites)
            self.assertIn("release-workflows", suites)

    def test_private_mirror_controller_does_not_select_product_suites(self) -> None:
        suites, _ = HARNESS.select_suites(
            [".gitlab/scripts/publish-public-mirror.sh"]
        )
        self.assertEqual(suites, ["release-workflows"])

    @staticmethod
    def _run_manifest(command: list[str], *, allow_skip: bool = False) -> dict:
        return {
            "schema_version": 2,
            "suites": {
                "leaf": {
                    "description": "synthetic leaf",
                    "lane": "meta",
                    "tools": [],
                    "commands": [command],
                    "allow_skip": allow_skip,
                },
                "full": {
                    "description": "synthetic full",
                    "abstract": True,
                    "requires": ["leaf"],
                },
            },
            "execution_classes": {
                "deterministic": {
                    "description": "deterministic",
                    "suites": ["leaf"],
                },
                "live_process": {"description": "live", "suites": []},
                "live_provider": {"description": "provider", "suites": []},
            },
            "routes": [{"patterns": ["**"], "suites": ["leaf"]}],
        }

    def test_executor_exception_is_reported_as_infrastructure_error(self) -> None:
        manifest = self._run_manifest(["missing-command"])
        manifest["suites"]["second"] = {
            "description": "second leaf",
            "lane": "meta",
            "tools": [],
            "commands": [["never-runs"]],
        }
        manifest["suites"]["full"]["requires"].append("second")
        manifest["execution_classes"]["deterministic"]["suites"].append("second")
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with mock.patch.object(HARNESS, "git", return_value="head"), mock.patch.object(
                HARNESS.subprocess,
                "run",
                side_effect=FileNotFoundError("missing-command"),
            ):
                result = HARNESS.run_suites(["full"], manifest, False, report)
            payload = json.loads(report.read_text())
        self.assertEqual(result, 1)
        self.assertEqual(payload["status"], "infrastructure_error")
        self.assertEqual(payload["results"][0]["status"], "infrastructure_error")
        self.assertEqual(
            payload["results"][0]["commands"][0]["status"],
            "infrastructure_error",
        )
        self.assertEqual(payload["results"][1]["suite"], "second")
        self.assertEqual(payload["results"][1]["status"], "not_run")

    def test_optional_skip_is_distinct_from_pass(self) -> None:
        manifest = self._run_manifest(["optional-tool"], allow_skip=True)
        completed = subprocess.CompletedProcess(["optional-tool"], HARNESS.SKIP_EXIT_CODE)
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with mock.patch.object(HARNESS, "git", return_value="head"), mock.patch.object(
                HARNESS.subprocess,
                "run",
                return_value=completed,
            ):
                result = HARNESS.run_suites(["leaf"], manifest, False, report)
            payload = json.loads(report.read_text())
        self.assertEqual(result, 0)
        self.assertEqual(payload["status"], "passed_with_skips")
        self.assertEqual(payload["results"][0]["status"], "skipped")

    def test_keyboard_interrupt_is_reported_before_it_is_propagated(self) -> None:
        manifest = self._run_manifest(["interrupt"])
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with mock.patch.object(HARNESS, "git", return_value="head"), mock.patch.object(
                HARNESS.subprocess,
                "run",
                side_effect=KeyboardInterrupt(),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    HARNESS.run_suites(["leaf"], manifest, False, report)
            payload = json.loads(report.read_text())
        self.assertEqual(payload["status"], "infrastructure_error")
        self.assertEqual(payload["results"][0]["status"], "infrastructure_error")
        self.assertEqual(payload["results"][0]["commands"][0]["status"], "infrastructure_error")

    def test_unknown_runner_exception_is_never_reported_as_passed(self) -> None:
        manifest = self._run_manifest(["explode"])
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.json"
            with mock.patch.object(HARNESS, "git", return_value="head"), mock.patch.object(
                HARNESS.subprocess,
                "run",
                side_effect=RuntimeError("runner exploded"),
            ):
                with self.assertRaisesRegex(RuntimeError, "runner exploded"):
                    HARNESS.run_suites(["leaf"], manifest, False, report)
            payload = json.loads(report.read_text())
        self.assertEqual(payload["status"], "infrastructure_error")


if __name__ == "__main__":
    unittest.main()
