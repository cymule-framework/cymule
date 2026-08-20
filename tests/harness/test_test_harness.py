"""Tests for conservative Cymule change-to-suite routing."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


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
                "protocol",
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
        self.assertEqual(suites, ["rust-durable"])

    def test_durable_store_contract_change_selects_direct_consumers(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-durable/src/store.rs"])
        self.assertEqual(
            set(suites),
            {
                "rust-agent-plugin",
                "rust-directory-plugin",
                "rust-durable",
                "rust-resource",
                "rust-virtual",
            },
        )

    def test_test_world_change_selects_its_three_existing_consumers(self) -> None:
        suites, _ = HARNESS.select_suites(["tests/test-world/src/lib.rs"])
        self.assertEqual(
            set(suites),
            {
                "test-world-deterministic",
                "test-world-live-process",
                "rust-durable",
                "rust-store-plugins",
                "rust-agent-plugin",
            },
        )

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
        self.assertIn("rust", lanes)
        self.assertIn("sdk-typescript", lanes)
        self.assertIn("sdk-python", lanes)
        self.assertIn("sdk-go", lanes)
        self.assertIn("meta", lanes)
        rust_lane = next(entry for entry in matrix["include"] if entry["lane"] == "rust")
        self.assertEqual(
            set(rust_lane["execution_classes"]),
            {"deterministic", "live_process", "live_provider"},
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


if __name__ == "__main__":
    unittest.main()
