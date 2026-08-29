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
        docs = HARNESS.load_manifest()["suites"]["docs"]
        self.assertEqual(docs["tools"], [])
        self.assertFalse(
            any(command[0] == "cargo" for command in docs["commands"])
        )

    def test_governance_schemas_select_full_registry_evidence(self) -> None:
        for path in (
            "schemas/version-domain-registry.schema.json",
            "schemas/release-bom.schema.json",
        ):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, ["full"], path)

    def test_registry_authority_changes_select_full_evidence(self) -> None:
        for path in (
            "versioning/version-domains.json",
            "scripts/version_domains.py",
            "tests/harness/test_version_domains.py",
        ):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, ["full"], path)

    def test_virtual_semantics_select_every_language_and_behavioral_consumer(self) -> None:
        suites, _ = HARNESS.select_suites(["crates/cymule-virtual/src/archive.rs"])
        self.assertEqual(
            set(suites),
            {
                "example",
                "protocol",
                "rust-resource-plugins",
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

    def test_every_ci_plan_adds_only_the_lightweight_source_closure(self) -> None:
        manifest = HARNESS.load_manifest()
        self.assertEqual(manifest["required_suites"], ["version-domain-source"])
        source_suite = manifest["suites"]["version-domain-source"]
        self.assertEqual(source_suite["lane"], "version-domain")
        self.assertEqual(source_suite["tools"], ["uv"])
        self.assertEqual(
            source_suite["commands"],
            [[
                "uv",
                "run",
                "--project",
                "sdk/python",
                "--frozen",
                "python",
                "scripts/version_domains.py",
                "verify-source-closure",
            ]],
        )
        for path, expected_path_suite in (
            ("docs/architecture.md", "docs"),
            ("sdk/go/cymule.go", "sdk-go"),
            ("crates/cymule-core/tests/semantic_kernel.rs", "rust-core"),
        ):
            with self.subTest(path=path):
                selected, evidence = HARNESS.select_suites([path], manifest)
                planned, planned_evidence = HARNESS.plan_required_suites(
                    selected, evidence, manifest
                )
                self.assertIn(expected_path_suite, planned)
                self.assertIn("version-domain-source", planned)
                self.assertNotIn("full", planned)
                self.assertEqual(
                    planned_evidence["version-domain-source"], ["<required>"]
                )
                expanded = HARNESS.expand_suites(planned, manifest)
                self.assertIn("version-domain-source", expanded)

        workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertIn("version_domain: ${{ steps.plan.outputs.version_domain }}", workflow)
        self.assertIn("  version-domain:\n", workflow)
        self.assertIn("      - version-domain\n", workflow)
        self.assertIn('"version-domain": "version_domain"', workflow)

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

    def test_name_status_z_retains_both_rename_and_copy_paths(self) -> None:
        value = (
            b"M\0docs/line\nbreak.md\0"
            b"R100\0crates/old.rs\0crates/new.rs\0"
            b"C075\0scripts/source.py\0scripts/copy.py\0"
            b"D\0removed.txt\0"
        )
        self.assertEqual(
            HARNESS.parse_name_status_z(value),
            [
                "docs/line\nbreak.md",
                "crates/old.rs",
                "crates/new.rs",
                "scripts/source.py",
                "scripts/copy.py",
                "removed.txt",
            ],
        )

    def test_name_status_z_rejects_a_truncated_rename(self) -> None:
        with self.assertRaisesRegex(ValueError, "truncated"):
            HARNESS.parse_name_status_z(b"R100\0old.rs\0")

    def test_name_status_copy_detection_retains_unchanged_source_and_destination(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary)
            subprocess.run(["git", "init", "-b", "main"], cwd=repository, check=True)
            subprocess.run(
                ["git", "config", "user.name", "Cymule Test"],
                cwd=repository,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=repository,
                check=True,
            )
            source = repository / "source.txt"
            source.write_text("copied authority\n", encoding="utf-8")
            subprocess.run(["git", "add", "source.txt"], cwd=repository, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Add source"], cwd=repository, check=True
            )
            base = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repository,
                check=True,
                text=True,
                capture_output=True,
            ).stdout.strip()
            (repository / "copy.txt").write_bytes(source.read_bytes())
            subprocess.run(["git", "add", "copy.txt"], cwd=repository, check=True)
            subprocess.run(
                ["git", "commit", "-m", "Copy source"], cwd=repository, check=True
            )
            output = subprocess.run(
                [
                    "git",
                    "diff",
                    "--name-status",
                    "-z",
                    "--find-copies",
                    "--find-copies-harder",
                    f"{base}..HEAD",
                ],
                cwd=repository,
                check=True,
                capture_output=True,
            ).stdout
        self.assertEqual(
            HARNESS.parse_name_status_z(output), ["source.txt", "copy.txt"]
        )

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
        self.assertIn("version-domain", lanes)
        rust_lane = next(
            entry for entry in matrix["include"] if entry["lane"] == "rust-durable"
        )
        self.assertEqual(
            set(rust_lane["execution_classes"]),
            {"deterministic", "live_process"},
        )
        expanded = HARNESS.expand_suites(["full"], manifest)
        self.assertIn("rust-directory-plugin", expanded)

    def test_every_leaf_has_one_execution_class(self) -> None:
        manifest = HARNESS.load_manifest()
        classes = HARNESS.suite_execution_classes(manifest)
        leaves = {
            name
            for name, suite in manifest["suites"].items()
            if not suite.get("abstract", False)
        }
        self.assertEqual(set(classes), leaves)

    def test_package_suite_catalog_covers_the_exact_workspace(self) -> None:
        manifest = HARNESS.load_manifest()
        roots, _ = HARNESS.workspace_package_graph()
        self.assertEqual(set(manifest["package_suites"]), set(roots.values()))
        self.assertEqual(
            manifest["package_suites"]["cymule-authenticated-collections"],
            ["rust-authenticated-collections"],
        )
        self.assertEqual(
            manifest["package_suites"]["cymule-durable-protocol"],
            ["rust-durable-protocol"],
        )
        self.assertEqual(
            manifest["package_suites"]["cymule-profile-protocol"],
            ["rust-profile-protocol"],
        )

    def test_public_protocol_crates_have_independent_behavioral_leaves(self) -> None:
        for path, expected in (
            (
                "crates/cymule-authenticated-collections/tests/map.rs",
                "rust-authenticated-collections",
            ),
            (
                "crates/cymule-durable-protocol/tests/model.rs",
                "rust-durable-protocol",
            ),
            (
                "crates/cymule-profile-protocol/tests/evolution.rs",
                "rust-profile-protocol",
            ),
        ):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, [expected], path)

    def test_protocol_leaf_executes_the_test_adapter_conformance_suite(self) -> None:
        protocol = HARNESS.load_manifest()["suites"]["protocol"]
        self.assertIn(
            ["./scripts/verify-rust.sh", "cymule-test-adapter"],
            protocol["commands"],
        )

    def test_duplicate_execution_class_is_rejected(self) -> None:
        manifest = HARNESS.load_manifest()
        manifest["execution_classes"]["live_provider"]["suites"].append("rust-core")
        with self.assertRaisesRegex(ValueError, "duplicate execution classes"):
            HARNESS.validate_manifest(manifest)

    def test_required_ci_suite_must_be_a_deterministic_concrete_leaf(self) -> None:
        manifest = HARNESS.load_manifest()
        manifest["required_suites"] = ["rust-observability-plugin"]
        with self.assertRaisesRegex(ValueError, "required suite.*deterministic"):
            HARNESS.validate_manifest(manifest)

    def test_route_catalog_rejects_unknown_suite(self) -> None:
        manifest = HARNESS.load_manifest()
        manifest["routes"][0]["suites"] = ["missing-suite"]
        with self.assertRaisesRegex(ValueError, "unknown suites"):
            HARNESS.validate_manifest(manifest)

    def test_changed_paths_cannot_route_scheduled_or_catalog_evidence(self) -> None:
        manifest = HARNESS.load_manifest()
        for selected in (["rust-soak"], ["catalog"]):
            candidate = {
                **manifest,
                "routes": [dict(route) for route in manifest["routes"]],
            }
            candidate["routes"][0] = {
                **candidate["routes"][0],
                "suites": selected,
            }
            with self.subTest(selected=selected), self.assertRaisesRegex(
                ValueError, "non-ordinary suites"
            ):
                HARNESS.validate_manifest(candidate)

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

    def test_harness_authority_changes_select_normal_full_only(self) -> None:
        for path in ("scripts/test_harness.py", "tests/harness/suites.toml"):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, ["full"])
            expanded = HARNESS.expand_suites(suites, HARNESS.load_manifest())
            self.assertIn("rust-directory-plugin", expanded)
            for scheduled in (
                "rust-soak",
                "rust-coverage",
                "rust-coverage-plugins",
                "rust-mutation",
                "rust-mutation-evolution-m4",
                "rust-mutation-plugins",
                "rust-portability",
            ):
                self.assertNotIn(scheduled, expanded)

    def test_release_contract_selector_source_selects_full(self) -> None:
        suites, _ = HARNESS.select_suites(["scripts/release_contracts.py"])
        self.assertEqual(suites, ["full"])

    def test_catalog_retains_explicit_scheduled_and_normal_evidence(self) -> None:
        expanded = HARNESS.expand_suites(["catalog"], HARNESS.load_manifest())
        self.assertIn("rust-directory-plugin", expanded)
        self.assertIn("rust-soak", expanded)
        self.assertIn("rust-mutation", expanded)
        self.assertIn("rust-portability", expanded)

    def test_compatibility_keeps_a_native_windows_directory_non_unix_witness(self) -> None:
        workflow = (ROOT / ".github/workflows/compatibility.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn("  schedule:\n", workflow)
        self.assertIn("  workflow_dispatch:\n", workflow)
        self.assertIn("os: [ubuntu-24.04, macos-15]", workflow)
        for ordinary_event in ("push", "pull_request", "merge_group"):
            self.assertNotIn(f"  {ordinary_event}:\n", workflow)

        marker = "\n  directory-non-unix:\n"
        self.assertEqual(workflow.count(marker), 1)
        witness = workflow.split(marker, 1)[1]
        for fragment in (
            "runs-on: windows-2025",
            "shell: pwsh",
            "rustup toolchain install 1.97.1-x86_64-pc-windows-msvc --profile minimal",
            "cargo +1.97.1-x86_64-pc-windows-msvc test --locked --package cymule-directory-store --lib non_unix_tests::writable_generation_fails_before_initialization -- --exact",
            "if (-not $IsWindows)",
            'test result: ok\\. 1 passed; 0 failed;',
        ):
            self.assertIn(fragment, witness)
        self.assertNotIn("continue-on-error", witness)
        self.assertNotIn("shell: bash", witness)
        self.assertNotIn("needs:", witness)

        required_ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        self.assertNotIn("directory-non-unix", required_ci)
        self.assertNotIn("compatibility.yml", required_ci)

        source = (ROOT / "plugins/directory-store/src/lib.rs").read_text(
            encoding="utf-8"
        )
        self.assertIn("#[cfg(not(unix))]\nmod non_unix_tests", source)
        self.assertEqual(
            source.count("fn writable_generation_fails_before_initialization()"),
            1,
        )

    def test_python_toolchain_authority_routes_every_uv_consumer(self) -> None:
        suites, _ = HARNESS.select_suites(["sdk/python/uv.lock"])
        self.assertEqual(
            set(suites),
            {"harness", "release-workflows", "sdk-python"},
        )

    def test_retired_virtual_fixture_selects_schema_and_rust_rejection(self) -> None:
        suites, _ = HARNESS.select_suites(
            ["tests/harness/fixtures/retired-virtual-contracts.json"]
        )
        self.assertEqual(
            set(suites), {"docs", "harness", "protocol", "rust-virtual"}
        )

    def test_workspace_checkpoint_fixture_selects_schema_and_durable_roundtrip(self) -> None:
        suites, _ = HARNESS.select_suites(
            ["tests/harness/fixtures/agent-workspace-checkpoint.json"]
        )
        self.assertEqual(
            set(suites), {"docs", "harness", "protocol", "rust-durable"}
        )

    def test_durable_state_root_fixture_selects_every_schema_reader(self) -> None:
        suites, _ = HARNESS.select_suites(
            ["tests/harness/fixtures/durable-storage-state-root.json"]
        )
        self.assertEqual(
            set(suites),
            {
                "docs",
                "harness",
                "protocol",
                "sdk-rust",
                "sdk-typescript",
                "sdk-python",
                "sdk-go",
            },
        )

    def test_applied_effect_summary_fixture_selects_durable_and_all_sdk_readers(self) -> None:
        suites, _ = HARNESS.select_suites(
            ["tests/fixtures/applied-effect-summary.json"]
        )
        self.assertEqual(
            set(suites),
            {
                "protocol",
                "rust-durable",
                "sdk-rust",
                "sdk-typescript",
                "sdk-python",
                "sdk-go",
            },
        )

    def test_scheduled_runner_changes_select_normal_full_without_running_scheduled_work(self) -> None:
        for path in ("scripts/verify-soak.sh", "scripts/verify-analysis.sh"):
            suites, _ = HARNESS.select_suites([path])
            self.assertEqual(suites, ["full"])
            expanded = HARNESS.expand_suites(suites, HARNESS.load_manifest())
            self.assertNotIn("rust-soak", expanded)
            self.assertNotIn("rust-coverage", expanded)
            self.assertNotIn("rust-mutation", expanded)
            self.assertNotIn("rust-portability", expanded)

    def test_m4_mutation_route_targets_the_real_profile_reducer_inventory(self) -> None:
        script = (ROOT / "scripts" / "verify-analysis.sh").read_text(encoding="utf-8")
        route = script.split("mutation-evolution-m4)", 1)[1].split(";;", 1)[0]
        self.assertIn("require_mutant_inventory", route)
        self.assertGreaterEqual(route.count("cymule-profile-protocol"), 2)
        self.assertNotIn("cymule-evolution", route)
        for symbol in (
            "analyze_relink",
            "validate_migration_no_widening",
            "MigrationSafePoint::verify ->",
            "MigrationSafePoint::derived_id",
            "prepare_definition_publication",
            "build_relink_edge",
            "update_decision",
            "provider_required_artifacts",
            "prepare_evolution_migration_target",
            "admit_evolution_target_binding",
            "verify_evolution_target_binding_record",
            "EvolutionReductionSource::retained_migration",
            "prevalidate_migration_source",
            "reduce_migration_command",
            "reduce_new_migration",
            "prepare_evolution_selection",
            "reduce_evolution_selection",
            "verify_migration_material_authority",
            "EvolutionPostcondition::migration_sidecar",
            "reduce_restart_command",
            "derive_plan_edge_id",
            "verify_plan_edge",
            "verify_edge_mutation_authority",
            "derive_rollout_evaluation_id",
            "derive_rollout_transition_id",
            "verify_rollout_transition",
        ):
            self.assertIn(symbol, route)
        for retired in (
            "link_registered",
            "restart_under_new_plan",
            "compatibility\\.rs",
        ):
            self.assertNotIn(retired, route)

    def test_m4_coverage_has_an_independent_nonempty_artifact(self) -> None:
        script = (ROOT / "scripts" / "verify-analysis.sh").read_text(encoding="utf-8")
        route = script.split("coverage)", 1)[1].split(";;", 1)[0]
        self.assertIn("--package cymule-profile-protocol", route)
        self.assertIn("coverage-evolution-m4.json", route)
        self.assertIn("src/(agent|error|lib|resource|virtual_work)", route)
        self.assertIn("--fail-under-lines 63", route)
        self.assertIn("--fail-under-regions 64", route)
        self.assertIn(
            'require_nonempty_artifact "$OUTPUT_DIR/coverage-evolution-m4.json"',
            route,
        )
        self.assertIn('if [ ! -s "$artifact_path" ]', script)
        workflow = (ROOT / ".github" / "workflows" / "analysis.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn(".cache/test-analysis/coverage-evolution-m4.json", workflow)

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
            "required_suites": ["leaf"],
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
