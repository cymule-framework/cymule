"""Cross-language Python SDK conformance."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from collections.abc import Callable

from cymule import (
    CliEngine,
    DurableEngine,
    DurableControlBuilder,
    EngineError,
    EvolutionControlBuilder,
    FlowBuilder,
    LiveEvolutionControlBuilder,
    ResourceBuilder,
    VirtualSchedulingControlBuilder,
    VirtualWorkControlBuilder,
    WaitActivationBuilder,
    sqlite_store,
)
from cymule import _unique_json_object, _validate_engine_envelope


class EndToEndTest(unittest.TestCase):
    def test_engine_json_rejects_duplicate_object_members(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate JSON object member"):
            json.loads(
                '{"response":{"type":"verified","type":"executed"}}',
                object_pairs_hook=_unique_json_object,
            )

    def test_engine_success_and_nested_unions_are_closed(self) -> None:
        invalid_responses = [
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {"type": "sealed", "plan": None},
            },
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {"type": "unknown"},
            },
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {
                    "type": "execution_boundary",
                    "execution": {"status": "completed", "result": {}, "suspension": {}},
                },
            },
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {
                    "type": "verified_evolution_command",
                    "command": {
                        "control_version": "cymule.evolution-control/3",
                        "command_id": "command:test",
                        "operation": "future_operation",
                    },
                },
            },
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {
                    "type": "execution_boundary",
                    "execution": {
                        "status": "suspended",
                        "suspension": {
                            "run_id": "run:test",
                            "plan_id": "sha256:test",
                            "definition_id": "main",
                            "invocation_id": "main",
                            "site_id": "wait:test",
                            "wait": {"kind": "future", "unexpected": True},
                            "result_bind": None,
                        },
                    },
                },
            },
            {
                "engine_protocol": "cymule.engine/2",
                "outcome": "success",
                "response": {
                    "type": "verified_evolution_command",
                    "command": {
                        "control_version": "cymule.evolution-control/3",
                        "command_id": "command:test",
                        "operation": "migrate",
                        "request": {"unexpected": True},
                    },
                },
            },
        ]
        for response in invalid_responses:
            with self.subTest(response=response):
                with self.assertRaises(EngineError):
                    _validate_engine_envelope(response)

        for execution in (
            {
                "status": "release_required",
                "release": {
                    "run_id": "run:test",
                    "plan_id": "sha256:test",
                    "intent_ids": ["intent:test"],
                },
            },
            {
                "status": "reconciliation_required",
                "reconciliation": {
                    "run_id": "run:test",
                    "plan_id": "sha256:test",
                    "intent_id": "intent:test",
                },
            },
        ):
            _validate_engine_envelope(
                {
                    "engine_protocol": "cymule.engine/2",
                    "outcome": "success",
                    "response": {"type": "execution_boundary", "execution": execution},
                }
            )

    def test_python_rejects_unsafe_json_and_preserves_pre_cancellation(self) -> None:
        candidate = FlowBuilder("transport", {}, {}).finish({"kind": "input"})
        with self.assertRaises(EngineError) as cancelled:
            CliEngine(cancelled=lambda: True).seal(candidate)
        self.assertEqual(cancelled.exception.failure["category"], "cancelled")
        candidate["name"] = float("nan")
        with self.assertRaises(EngineError) as unsafe:
            CliEngine("missing-engine").seal(candidate)
        self.assertEqual(unsafe.exception.failure["code"], "request_encoding_failed")

    def test_python_preserves_structured_engine_failures(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        plugin_path = os.environ.get("CYMULE_TEST_PLUGIN")
        failure_path = os.environ.get("CYMULE_ENGINE_FAILURE_FIXTURE")
        if engine_path is None or plugin_path is None or failure_path is None:
            self.skipTest("Engine failure conformance is not configured")
        with open(failure_path, encoding="utf-8") as source:
            expected = json.load(source)["cases"]
        candidate_path = failure_path.replace(
            "engine-failures.json", "cross-language-plan.json"
        )
        with open(candidate_path, encoding="utf-8") as source:
            candidate = json.load(source)
        engine = CliEngine(engine_path)
        invalid = dict(candidate)
        invalid["ir_version"] = "cymule.ir/unsupported"
        self._assert_engine_failure(
            lambda: engine.seal(invalid), expected["invalid_plan_version"]
        )
        plan = engine.seal(candidate)
        self._assert_engine_failure(
            lambda: engine.run(
                plan,
                {"simulate": "expected_failure"},
                plugin_path,
                "run:python-expected",
            ),
            expected["expected_plugin_failure"],
        )
        self._assert_engine_failure(
            lambda: engine.run(
                plan, {"message": "defect"}, engine_path, "run:python-defect"
            ),
            expected["plugin_defect"],
        )
        self._assert_engine_failure(
            lambda: engine.run(
                plan,
                {"message": "substrate"},
                "/cymule-conformance/missing-plugin",
                "run:python-substrate",
            ),
            expected["substrate_failure"],
        )

    def _assert_engine_failure(
        self, operation: Callable[[], object], expected: dict[str, str]
    ) -> None:
        with self.assertRaises(EngineError) as raised:
            operation()
        failure = raised.exception.failure
        self.assertEqual(failure["category"], expected["category"])
        self.assertEqual(failure["phase"], expected["phase"])
        self.assertEqual(failure["code"], expected["code"])
        self.assertEqual(
            failure.get("retry_disposition"), expected.get("retry_disposition")
        )

    def test_python_durable_control_validates(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        fixture_path = os.environ.get("CYMULE_DURABLE_CONTROL_FIXTURE")
        if engine_path is None or fixture_path is None:
            self.skipTest("durable control conformance is not configured")
        command = DurableControlBuilder.query_domain(
            "query:cross-language-domain"
        )
        with open(fixture_path, encoding="utf-8") as source:
            self.assertEqual(command, json.load(source))
        self.assertEqual(
            CliEngine(engine_path).verify_durable_command(command), command
        )
        self.assertEqual(
            DurableControlBuilder.activate_signal(
                "activation:sdk",
                "signal:sdk",
                ["wait:z", "wait:a", "wait:z"],
                {"accepted": True},
            )["wait_ids"],
            ["wait:a", "wait:z"],
        )

    def test_python_unified_live_evolution_validates(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        fixture_path = os.environ.get("CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE")
        if engine_path is None or fixture_path is None:
            self.skipTest("live-evolution conformance is not configured")
        with open(fixture_path, encoding="utf-8") as source:
            expected = json.load(source)
        command = LiveEvolutionControlBuilder.apply(
            "command:live-evolution:fixture:select",
            "template:review-parent",
            expected["command"],
        )
        self.assertEqual(command, expected)
        self.assertEqual(
            CliEngine(engine_path).verify_live_evolution_command(command), command
        )

    def test_python_evolution_control_validates(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        fixture_path = os.environ.get("CYMULE_EVOLUTION_CONTROL_FIXTURE")
        if engine_path is None or fixture_path is None:
            self.skipTest("evolution control conformance is not configured")
        command = EvolutionControlBuilder.apply_gate(
            "command:evolution:fixture:promote",
            {
                "gate_id": "gate:fixture:promote",
                "decision_id": "rollout:fixture:canary",
                "min_target_observations": 3,
                "max_target_failures": 0,
                "min_equivalent_shadows": 2,
                "max_inequivalent_shadows": 0,
            },
            "rollout:fixture:active",
        )
        with open(fixture_path, encoding="utf-8") as source:
            self.assertEqual(command, json.load(source))
        self.assertEqual(CliEngine(engine_path).verify_evolution_command(command), command)
        restart_path = os.environ.get("CYMULE_EVOLUTION_RESTART_FIXTURE")
        if restart_path is None:
            self.skipTest("evolution restart conformance is not configured")
        with open(restart_path, encoding="utf-8") as source:
            restart_expected = json.load(source)
        restart = EvolutionControlBuilder.restart_under_new_plan(
            "command:evolution:fixture:restart",
            restart_expected["request"],
        )
        self.assertEqual(restart, restart_expected)
        self.assertEqual(
            CliEngine(engine_path).verify_evolution_command(restart), restart
        )

    def test_python_candidate_seals_and_executes(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        plugin_path = os.environ.get("CYMULE_TEST_PLUGIN")
        expected_plan_id = os.environ.get("CYMULE_EXPECTED_PLAN_ID")
        if engine_path is None or plugin_path is None or expected_plan_id is None:
            self.skipTest("cross-language binaries are not configured")
        candidate = (
            FlowBuilder("cross_language_echo", {}, {})
            .component("test.echo", {}, {}, {})
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
                {},
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
        execution = engine.run(plan, input_value, plugin_path, "run:python-e2e")
        self.assertEqual(execution["status"], "completed")
        if execution["status"] != "completed":
            self.fail("expected terminal execution")
        result = execution["result"]
        self.assertEqual(result["value"], input_value)
        self.assertEqual(len(result["effects"]), 1)
        with tempfile.TemporaryDirectory(prefix="cymule-python-durable-") as store:
            target = sqlite_store(os.path.join(store, "domain.sqlite"), "sdk-python")
            durable = DurableEngine(target, plugin_path, engine)
            self.assertEqual(
                durable.start("run:python-durable-e2e", candidate, input_value)["type"],
                "run_boundary",
            )
            self.assertIsNotNone(DurableEngine(target, None, engine).get("run:python-durable-e2e"))
            self.assertEqual(
                durable.evolve(
                    LiveEvolutionControlBuilder.publish_definition(
                        "evolve:python:publish",
                        "definition:python:echo",
                        candidate["definitions"][0],
                    )
                )["result"],
                "definition_published",
            )

    def test_python_rejects_malicious_engine_and_cancels_in_flight(self) -> None:
        malicious = os.environ.get("CYMULE_MALICIOUS_ENGINE")
        slow = os.environ.get("CYMULE_SLOW_ENGINE")
        if malicious is None or slow is None:
            self.skipTest("malicious Engine conformance is not configured")
        with self.assertRaises(EngineError) as forged:
            DurableEngine("unused", None, CliEngine(malicious)).get("run:fake")
        self.assertEqual(forged.exception.failure["code"], "invalid_engine_response")
        cancelled = threading.Event()
        timer = threading.Timer(0.05, cancelled.set)
        timer.start()
        try:
            with self.assertRaises(EngineError) as interrupted:
                CliEngine(slow, timeout_seconds=5, cancelled=cancelled.is_set).seal(
                    FlowBuilder("cancel-in-flight", {}, {}).finish({"kind": "input"})
                )
            self.assertEqual(interrupted.exception.failure["category"], "cancelled")
        finally:
            timer.cancel()

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
                "identity_version": "cymule.artifact/2",
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
                "identity_version": "cymule.artifact/2",
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
                    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
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
