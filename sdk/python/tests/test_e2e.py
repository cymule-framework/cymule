"""Cross-language Python SDK conformance."""

from __future__ import annotations

import json
import os
import unittest

from cymule import (
    CliEngine,
    FlowBuilder,
    ResourceBuilder,
    VirtualSchedulingControlBuilder,
    VirtualWorkControlBuilder,
    WaitActivationBuilder,
)


class EndToEndTest(unittest.TestCase):
    def test_python_candidate_seals_and_executes(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        plugin_path = os.environ.get("CYMULE_TEST_PLUGIN")
        expected_plan_id = os.environ.get("CYMULE_EXPECTED_PLAN_ID")
        if engine_path is None or plugin_path is None or expected_plan_id is None:
            self.skipTest("cross-language binaries are not configured")
        candidate = (
            FlowBuilder("cross_language_echo", {}, {})
            .component("test.echo", {}, {})
            .effect_contract(
                "test.capture",
                {},
                {},
                {
                    "mutation": "mutating",
                    "dispatch": "on_scope_commit",
                    "reconciliation": "queryable",
                    "keyed_idempotency": True,
                    "irreversible": False,
                },
            )
            .definition(
                "echo_subflow",
                {},
                {},
                {
                    "steps": [
                        {
                            "id": "call.echo",
                            "op": "call",
                            "component": "test.echo",
                            "input": {"kind": "input"},
                            "bind": "echoed",
                        }
                    ],
                    "result": {"kind": "binding", "name": "echoed"},
                },
            )
            .invoke("invoke.echo-subflow", "echo_subflow", {"kind": "input"}, "echoed")
            .effect(
                "effect.capture",
                "test.capture",
                {"kind": "binding", "name": "echoed"},
                "primary",
            )
            .finish({"kind": "binding", "name": "echoed"})
        )
        engine = CliEngine(engine_path)
        plan = engine.seal(candidate)
        self.assertEqual(plan["plan_id"], expected_plan_id)
        input_value = {"message": "hello from Python"}
        result = engine.run(plan, input_value, plugin_path, "run:python-e2e")
        self.assertEqual(result["value"], input_value)
        self.assertEqual(len(result["effects"]), 1)

    def test_python_resource_seals_through_rust_engine(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        expected_resource_id = os.environ.get("CYMULE_EXPECTED_RESOURCE_ID")
        if engine_path is None or expected_resource_id is None:
            self.skipTest("resource engine conformance is not configured")
        resource = CliEngine(engine_path).seal_resource(
            ResourceBuilder.text(
                "shared cross-run resource",
                {"purpose": "cross-language-conformance"},
            )
        )
        self.assertEqual(resource["resource_id"], expected_resource_id)
        self.assertEqual(resource["integrity"], {"kind": "inline"})

    def test_python_wait_activation_validates_through_rust_engine(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        fixture_path = os.environ.get("CYMULE_WAIT_ACTIVATION_FIXTURE")
        if engine_path is None or fixture_path is None:
            self.skipTest("wait activation engine conformance is not configured")
        activation = WaitActivationBuilder.signal(
            "activation:shared:1",
            "signal:continue",
            ["wait:shared:1"],
            {
                "artifact_id": (
                    "sha256:0123456789abcdef0123456789abcdef"
                    "0123456789abcdef0123456789abcdef"
                ),
                "kind": "cymule.wait-activation-result/1",
            },
        )
        with open(fixture_path, encoding="utf-8") as source:
            self.assertEqual(activation, json.load(source))
        self.assertEqual(
            CliEngine(engine_path).verify_wait_activation(activation), activation
        )

    def test_python_virtual_work_query_and_control_fixtures_stay_exact(self) -> None:
        occurrence_path = os.environ.get("CYMULE_VIRTUAL_OCCURRENCE_FIXTURE")
        control_path = os.environ.get("CYMULE_VIRTUAL_CONTROL_FIXTURE")
        if occurrence_path is None or control_path is None:
            self.skipTest("virtual work SDK conformance is not configured")
        with open(occurrence_path, encoding="utf-8") as source:
            occurrence = json.load(source)
        self.assertEqual(occurrence["occurrence_binding"], "binding:worker/fixture@1")
        command = VirtualWorkControlBuilder.succeed(
            "command:virtual:fixture:success",
            "work:fixture",
            "worker:fixture",
            1,
            1,
            101,
            {
                "artifact_id": (
                    "sha256:abcdef0123456789abcdef0123456789"
                    "abcdef0123456789abcdef0123456789"
                ),
                "kind": "example/result",
            },
        )
        with open(control_path, encoding="utf-8") as source:
            self.assertEqual(command, json.load(source))
        migration_path = os.environ.get("CYMULE_VIRTUAL_MIGRATION_FIXTURE")
        if migration_path is None:
            self.skipTest("virtual region migration SDK conformance is not configured")
        with open(migration_path, encoding="utf-8") as source:
            migration_fixture = json.load(source)
        self.assertEqual(
            VirtualWorkControlBuilder.migration(
                "command:migration:fixture-split",
                migration_fixture["plan"],
            ),
            migration_fixture,
        )
        compaction_path = os.environ.get("CYMULE_VIRTUAL_COMPACTION_FIXTURE")
        rehydration_path = os.environ.get("CYMULE_VIRTUAL_REHYDRATION_FIXTURE")
        if compaction_path is None or rehydration_path is None:
            self.skipTest("virtual archive SDK conformance is not configured")
        with open(compaction_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualWorkControlBuilder.compaction(
                    "command:compaction:fixture",
                    "region:fixture",
                    ["virtual:fixture:terminal"],
                    "binding:archive/fixture@1",
                    "compactor:fixture/1",
                ),
                json.load(source),
            )
        with open(rehydration_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualWorkControlBuilder.rehydration(
                    "command:rehydration:fixture",
                    (
                        "sha256:0123456789abcdef0123456789abcdef"
                        "0123456789abcdef0123456789abcdef"
                    ),
                    [occurrence["occurrence_id"]],
                ),
                json.load(source),
            )
        claim_path = os.environ.get("CYMULE_VIRTUAL_CLAIM_FIXTURE")
        renewal_path = os.environ.get("CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE")
        recovery_path = os.environ.get("CYMULE_VIRTUAL_RECOVERY_FIXTURE")
        run_weight_path = os.environ.get("CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE")
        if (
            claim_path is None
            or renewal_path is None
            or recovery_path is None
            or run_weight_path is None
        ):
            self.skipTest("virtual scheduling SDK conformance is not configured")
        with open(claim_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualSchedulingControlBuilder.claim(
                    "command:claim:fixture",
                    "worker:fixture",
                    "slot:worker-fixture:0",
                    "binding:worker/fixture@1",
                    ["sandbox", "cpu", "cpu"],
                    100,
                    30,
                ),
                json.load(source),
            )
        with open(renewal_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualSchedulingControlBuilder.renew(
                    "command:renew:fixture",
                    "work:fixture",
                    "worker:fixture",
                    1,
                    1,
                    120,
                    30,
                ),
                json.load(source),
            )
        with open(recovery_path, encoding="utf-8") as source:
            recovery_fixture = json.load(source)
        self.assertEqual(
            VirtualSchedulingControlBuilder.recovery(
                "command:recovery:fixture",
                "work:fixture",
                "worker:fixture",
                1,
                2,
                150,
                recovery_fixture["resolution"],
            ),
            recovery_fixture,
        )
        with open(run_weight_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualSchedulingControlBuilder.run_weight(
                    "command:run-weight:fixture", "run:fixture", 3
                ),
                json.load(source),
            )


if __name__ == "__main__":
    unittest.main()
