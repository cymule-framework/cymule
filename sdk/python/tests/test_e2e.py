"""Cross-language Python SDK conformance."""

from __future__ import annotations

import os
import unittest

from cymule import CliEngine, FlowBuilder, ResourceBuilder


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
            .call("call.echo", "test.echo", {"kind": "input"}, "echoed")
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

    def test_python_agent_stream_is_reduced_by_rust_engine(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        if engine_path is None:
            self.skipTest("Agent stream engine conformance is not configured")
        records = [
            {
                "record": "opened",
                "stream_id": "stream:python:1",
                "session_id": "session:python",
                "target": {
                    "kind": "message",
                    "message_id": "message:python:1",
                    "role": "agent",
                },
            },
            {
                "record": "chunk",
                "stream_id": "stream:python:1",
                "session_id": "session:python",
                "chunk": {
                    "sequence": 0,
                    "content": [{"type": "text", "text": "hello"}],
                },
            },
            {
                "record": "finalized",
                "stream_id": "stream:python:1",
                "session_id": "session:python",
                "content_digest": (
                    "57e90e6cb7aff1276e78399ad62cee581"
                    "909f0d4944c24801d529c141c23a241"
                ),
                "update": {
                    "type": "message",
                    "update_id": "update:stream:python:1:finalized",
                    "message": {
                        "message_id": "message:python:1",
                        "role": "agent",
                        "content": [{"type": "text", "text": "hello"}],
                    },
                },
            },
        ]
        stream = CliEngine(engine_path).verify_agent_stream(records)
        self.assertEqual(stream["state"], "finalized")
        self.assertEqual(len(stream["chunks"]), 1)


if __name__ == "__main__":
    unittest.main()
