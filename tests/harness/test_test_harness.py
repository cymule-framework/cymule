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


if __name__ == "__main__":
    unittest.main()
