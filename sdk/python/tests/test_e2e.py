"""Cross-language Python SDK conformance."""

from __future__ import annotations

import copy
import base64
import hashlib
import json
import os
import tempfile
import threading
import time
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
    ResourceHandoffActivation,
    VirtualCompactionCertificate,
    VirtualSchedulingControlBuilder,
    VirtualWorkControlBuilder,
    WaitActivationBuilder,
    process_plugin,
    sqlite_store,
    sqlite_clock,
    directory_store,
)
from cymule import (
    _unique_json_object,
    _validate_durable_response,
    _validate_durable_command_response,
    _validate_continuation_execution_claim,
    _validate_core_identity,
    _validate_engine_envelope,
    _validate_engine_store_target,
    _validate_evolution_commit,
    _validate_evolution_command,
    _validate_execution_outcome,
    _validate_live_evolution_command,
    _validate_live_evolution_outcome,
    _validate_migration_continuation,
    _validate_migration_request,
    _validate_positive_epoch,
    _validate_plan_candidate,
    _validate_restart_request,
    _strict_json_loads,
)


def fixture_execution() -> dict[str, object]:
    return {
        "owner": "driver:cross-language",
        "clock": {
            "clock_version": "cymule.clock-observation/2",
            "observation_id": "sha256:" + "1" * 64,
            "source_id": "clock:cross-language",
            "source_generation": "sha256:" + "2" * 64,
            "scope": "sha256:7aa23baf73ce53a540a6f3eddaa0175e6be22d751e5d5090d5d77485f58fa74c",
        },
        "ttl": 30,
    }


def _content_id(digit: str) -> str:
    return "sha256:" + digit * 64


def _manifest_descriptor_id(root_digest: str, size: int, entry_count: int) -> str:
    identity = json.dumps(
        {
            "entry_count": entry_count,
            "media_type": "application/vnd.cymule.resource-manifest+jsonl",
            "root_digest": root_digest,
            "size": size,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(
        b"cymule.resource-manifest/3\0" + identity
    ).hexdigest()


def _artifact(digit: str, kind: str) -> dict[str, object]:
    return {
        "identity_version": "cymule.artifact/2",
        "artifact_id": _content_id(digit),
        "kind": kind,
    }


def _artifact_record(kind: str, payload: bytes) -> dict[str, object]:
    kind_bytes = kind.encode("ascii")
    preimage = (
        b"cymule.artifact/2"
        + len(kind_bytes).to_bytes(4, "big")
        + kind_bytes
        + len(payload).to_bytes(8, "big")
        + payload
    )
    return {
        "reference": {
            "identity_version": "cymule.artifact/2",
            "artifact_id": "sha256:" + hashlib.sha256(preimage).hexdigest(),
            "kind": kind,
        },
        "bytes": base64.b64encode(payload).decode("ascii"),
    }


def process_target(executable: str, revision: str | None = None) -> dict[str, object]:
    return process_plugin(
        {
            "executable": executable,
            "arguments": [],
            "environment": {
                "CYMULE_TEST_EFFECT_LEDGER_PATH": executable
                + ".effect-ledger.sqlite3"
            },
            "working_directory": None,
            "runtime_closure": {
                "component-runtime": "sha256:" + "a" * 64
            },
            "timeout_ms": 60_000,
            "message_limit": (
                8 * 1024 * 1024 if revision is None else 16 * 1024 * 1024
            ),
            "closure_limit": 64 * 1024 * 1024,
        },
        revision,
    )


def durable_target_for(
    command: dict[str, object], store: dict[str, object] | None = None
) -> dict[str, object]:
    target: dict[str, object] = {
        "store": store or directory_store("unused")
    }
    if command["type"] in {
        "start_run",
        "resume_run",
        "takeover_run",
        "release_effect",
        "resolve_effect",
    }:
        target["executor"] = process_target("/bin/true")
    if command["type"] in {
        "start_run",
        "resume_run",
        "takeover_run",
        "release_effect",
    }:
        target["clock"] = sqlite_clock(
            "/tmp/cymule-python-preflight-clock.sqlite",
            "clock:python-preflight",
            "sha256:" + "f" * 64,
        )
    return target


def evolution_target() -> dict[str, object]:
    return {
        "store": directory_store("unused"),
        "migration_adapter": None,
        "shadow_driver": None,
        "target_execution_bindings": {},
    }


def _fixed_live_evolution_outcomes() -> dict[str, dict[str, object]]:
    candidate = FlowBuilder("wire_test", {}, {}).finish({"kind": "input"})
    source_plan = _content_id("c")
    target_plan = _content_id("d")
    evidence = _artifact("4", "cymule.evolution-evidence/1")
    input_state = _artifact("1", "cymule.migration-state/1")
    output_state = _artifact("2", "cymule.migration-state/1")
    source_binding = _artifact("5", "cymule.execution-binding/2")
    target_binding = _artifact("6", "cymule.execution-binding/2")
    revision = {
        "revision_version": "cymule.subflow-revision/2",
        "revision_id": _content_id("b"),
        "logical_ref": "definition:test",
        "sequence": 1,
        "definition": candidate["definitions"][0],
        "references": [
            {
                "logical_ref": "definition:dependency",
                "local_definition": "dependency",
                "input_schema": {},
                "output_schema": {},
                "strategy": {
                    "strategy": "pinned",
                    "revision_id": _content_id("a"),
                },
            }
        ],
    }
    frame = {
        "definition_id": "main",
        "invocation_id": "invocation:test",
        "invocation_path": [],
        "scope_id": "scope:root",
        "input": input_state,
        "region_path": [],
        "next_step": 0,
        "locals": {},
    }
    source_continuation = {
        "continuation_version": "cymule.continuation-state/1",
        "run_id": "run:test",
        "plan_id": source_plan,
        "binding_context": source_binding["artifact_id"],
        "frames": [frame],
        "state": input_state,
        "wait_set": [],
        "scope_stack": ["scope:root"],
        "epoch": 0,
        "execution_fence": 3,
        "execution_claim": None,
        "status": "ready",
    }
    target_continuation = {
        **copy.deepcopy(source_continuation),
        "plan_id": target_plan,
        "binding_context": target_binding["artifact_id"],
        "state": output_state,
        "epoch": 1,
    }
    migration_request = {
        "migration_id": "migration:test",
        "run_id": "run:test",
        "from_plan": source_plan,
        "to_plan": target_plan,
        "plan_edge_id": _content_id("9"),
        "compatibility_id": _content_id("8"),
        "expected_source_epoch": 0,
        "adapter_id": "adapter:test",
        "adapter_revision": _content_id("a"),
    }
    plan = {"plan_id": target_plan, "candidate": candidate}
    gate = {
        "gate_id": "gate:test",
        "decision_id": "decision:source",
        "min_target_observations": 1,
        "max_target_failures": 0,
        "min_equivalent_shadows": 0,
        "max_inequivalent_shadows": 0,
    }
    return {
        "definition_published": {
            "result": "definition_published",
            "revision": revision,
        },
        "template_registered": {
            "result": "template_registered",
            "linked": {
                "template_id": "template:test",
                "plan": plan,
                "resolved_revisions": {"definition:test": revision["revision_id"]},
            },
        },
        "publication_applied": {
            "result": "publication_applied",
            "receipt": {
                "revision": revision,
                "updates": [
                    {
                        "template_id": "template:a",
                        "previous_plan_id": source_plan,
                        "current_plan_id": target_plan,
                        "decision_id": _content_id("e"),
                        "advanced": True,
                    },
                    {
                        "template_id": "template:b",
                        "previous_plan_id": _content_id("f"),
                        "current_plan_id": _content_id("f"),
                        "decision_id": None,
                        "advanced": False,
                    },
                ],
            },
        },
        "patch_applied": {
            "result": "patch_applied",
            "edge": {
                "edge_id": _content_id("9"),
                "from_plan": source_plan,
                "to_plan": target_plan,
                "operations": [
                    {
                        "kind": "add",
                        "target": "definition:a",
                        "before": None,
                        "after": "1" * 64,
                    },
                    {
                        "kind": "replace",
                        "target": "definition:b",
                        "before": "2" * 64,
                        "after": "3" * 64,
                    },
                ],
            },
        },
        "applied": {"result": "applied"},
        "occurrence_selected": {
            "result": "occurrence_selected",
            "pin": {
                "occurrence_id": "occurrence:test",
                "template_id": "template:test",
                "decision_id": "decision:test",
                "plan_id": target_plan,
                "execution_binding": source_binding,
                "selection_id": "selection:test",
            },
        },
        "migrated": {
            "result": "migrated",
            "receipt": {
                "request": migration_request,
                "source_witness_id": _content_id("7"),
                "source_binding": source_binding,
                "target_binding": target_binding,
                "source_execution_fence": 3,
                "target_epoch": 1,
                "adapter_id": "adapter:test",
                "adapter_revision": _content_id("a"),
                "from_schema": "schema:source",
                "to_schema": "schema:target",
                "output_state": output_state,
                "target_continuation": target_continuation,
                "evidence": evidence,
            },
        },
        "restart_authorized": {
            "result": "restart_authorized",
            "receipt": {
                "request": {
                    "restart_id": "restart:test",
                    "replacement_run": "run:target",
                    "run_id": "run:test",
                    "from_plan": source_plan,
                    "expected_source_epoch": 0,
                    "to_plan": target_plan,
                    "input": input_state,
                    "evidence": evidence,
                },
                "source_witness_id": _content_id("7"),
                "target_plan": plan,
            },
        },
        "shadow_recorded": {
            "result": "shadow_recorded",
            "comparison": {
                "comparison_id": "comparison:test",
                "subject": "run:test",
                "decision_id": "decision:test",
                "primary_plan": source_plan,
                "shadow_plan": target_plan,
                "driver_id": "driver:test",
                "driver_revision": _content_id("a"),
                "comparison_policy": "policy:test",
                "primary_digest": "a" * 64,
                "shadow_digest": "b" * 64,
                "equivalent": True,
                "evidence": evidence,
            },
        },
        "gate_applied": {
            "result": "gate_applied",
            "transition": {
                "transition_id": _content_id("9"),
                "from_decision": "decision:source",
                "to_decision": "decision:target",
                "evaluation": {
                    "evaluation_id": _content_id("8"),
                    "gate": gate,
                    "target_observations": 1,
                    "target_failures": 0,
                    "equivalent_shadows": 0,
                    "inequivalent_shadows": 0,
                    "outcome": "promote",
                    "evidence_ids": ["observation:test"],
                },
            },
        },
    }


def _evolution_commit(
    command: dict[str, object],
    outcome: dict[str, object],
    evolution_id: str = "evolution:test",
) -> dict[str, object]:
    consumes_source = command.get("operation") == "apply" and command.get(
        "command", {}
    ).get("operation") in {"migrate", "restart_under_new_plan"}
    return {
        "observed_revision": _content_id("8"),
        "committed_revision": _content_id("8"),
        "receipt": {
            "receipt_version": "cymule.evolution-persistence-receipt/4",
            "receipt_id": _content_id("7"),
            "command": {
                "persistence_version": "cymule.evolution-persistence-command/4",
                "persistence_id": _content_id("6"),
                "evolution_id": evolution_id,
                "command": copy.deepcopy(command),
            },
            "parent_current_id": None,
            "source_witness_id": _content_id("5") if consumes_source else None,
            "outcome": copy.deepcopy(outcome),
            "mutations": [],
            "mutation_id": _content_id("4"),
        },
    }


def _engine_with_success(directory: str, response: dict[str, object]) -> CliEngine:
    executable = os.path.join(directory, "engine")
    with open(executable + ".response", "w", encoding="utf-8") as output:
        json.dump(response, output)
    with open(executable, "w", encoding="utf-8") as script:
        script.write(
            """#!/usr/bin/env python3
import json
import sys

request = json.load(sys.stdin)["request"]
with open(__file__ + ".response", encoding="utf-8") as source:
    response = json.load(source)
json.dump(
    {
        "engine_protocol": "cymule.engine/5",
        "outcome": "success",
        "request": request,
        "response": response,
    },
    sys.stdout,
)
"""
        )
    os.chmod(executable, 0o700)
    return CliEngine(executable)


def _engine_with_envelope(
    directory: str, envelope: dict[str, object]
) -> CliEngine:
    executable = os.path.join(directory, "engine")
    with open(executable + ".envelope", "w", encoding="utf-8") as output:
        json.dump(envelope, output)
    with open(executable, "w", encoding="utf-8") as script:
        script.write(
            '#!/bin/sh\n/bin/cat >/dev/null\nexec /bin/cat "$0.envelope"\n'
        )
    os.chmod(executable, 0o700)
    return CliEngine(executable)


def _engine_with_patch_success(
    directory: str,
    sealed_plan: dict[str, object],
    commit: dict[str, object],
) -> tuple[CliEngine, str]:
    executable = os.path.join(directory, "engine")
    with open(executable + ".responses.json", "w", encoding="utf-8") as output:
        json.dump(
            {"sealed_plan": sealed_plan, "commit": commit}, output
        )
    with open(executable, "w", encoding="utf-8") as script:
        script.write(
            """#!/usr/bin/env python3
import json
import sys

with open(__file__ + ".responses.json", encoding="utf-8") as source:
    responses = json.load(source)
request = json.load(sys.stdin)["request"]
with open(__file__ + ".requests", "a", encoding="utf-8") as log:
    log.write(request["type"] + "\\n")
if request["type"] == "seal":
    response = {"type": "sealed", "plan": responses["sealed_plan"]}
elif request["type"] == "execute_live_evolution":
    response = {
        "type": "live_evolution_executed",
        "commit": responses["commit"],
    }
else:
    raise SystemExit(2)
json.dump(
    {
        "engine_protocol": "cymule.engine/5",
        "outcome": "success",
        "request": request,
        "response": response,
    },
    sys.stdout,
)
"""
        )
    os.chmod(executable, 0o700)
    return CliEngine(executable), executable + ".requests"


class EndToEndTest(unittest.TestCase):
    def test_python_virtual_builders_enforce_closed_wire_authority(self) -> None:
        identity = "🧪" * 512
        clock = {**fixture_execution()["clock"], "scope": identity}
        binding = _artifact("2", "cymule.execution-binding/2")
        evidence = _artifact("3", "example/evidence")
        resolution = {"resolution": "retry", "error": evidence, "next_reason": None}
        command = VirtualSchedulingControlBuilder.claim(
            identity, identity, identity, binding, [identity], clock, 30
        )
        self.assertEqual(command["owner"], identity)
        for invalid in ("", "🧪" * 513, "id:\u0085", "id:\ud800"):
            with self.subTest(identity=repr(invalid)):
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.claim(
                        invalid, identity, identity, binding, [], clock, 30
                    )
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.claim(
                        identity, invalid, identity, binding, [], clock, 30
                    )
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.claim(
                        identity, identity, identity, binding, [invalid], clock, 30
                    )
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.renew(
                        identity, invalid, identity, 1, 1, clock, 30
                    )
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.recovery(
                        identity, identity, invalid, 1, 1, clock, resolution
                    )
                with self.assertRaises(ValueError):
                    VirtualWorkControlBuilder.succeed(
                        identity, invalid, identity, 1, 1, clock, evidence
                    )
        for invalid in (
            {**binding, "artifact_id": "sha256:not-a-digest"},
            {**binding, "unexpected": True},
        ):
            with self.assertRaises(ValueError):
                VirtualSchedulingControlBuilder.claim(
                    identity, identity, identity, invalid, [], clock, 30
                )
        for invalid in (
            {"resolution": "succeeded", "result": evidence},
            {"resolution": "parked", "reason": {"kind": "wait", "key": "wait:fixture"}},
            {**resolution, "result": evidence},
            {"resolution": "retry", "error": evidence},
        ):
            with self.assertRaises(ValueError):
                VirtualSchedulingControlBuilder.recovery(
                    identity, identity, identity, 1, 1, clock, invalid
                )
        with self.assertRaises(ValueError):
            VirtualSchedulingControlBuilder.run_weight(identity, identity, 4_294_967_296)
        VirtualSchedulingControlBuilder.recovery(
            identity, identity, identity, 1, 1, clock, resolution
        )

    def test_python_live_publication_requires_self_verifying_artifact_record(
        self,
    ) -> None:
        candidate = FlowBuilder("publication", {}, {}).finish({"kind": "input"})
        evidence = _artifact_record(
            "cymule.evolution-evidence/1", b"publication evidence"
        )
        command = LiveEvolutionControlBuilder.publish_and_relink(
            "command:publication",
            {
                "logical_ref": "definition:publication",
                "definition": candidate["definitions"][0],
                "references": [],
                "evidence": evidence,
                "mode": {"mode": "active"},
            },
        )
        _validate_live_evolution_command(command)
        self.assertEqual(
            command["publication"]["evidence"]["bytes"],
            base64.b64encode(b"publication evidence").decode("ascii"),
        )

        malformed = copy.deepcopy(command)
        malformed["publication"]["evidence"]["reference"]["artifact_id"] = (
            _content_id("0")
        )
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed)

        malformed = copy.deepcopy(command)
        malformed["publication"]["evidence"]["bytes"] = [112]
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed)

        malformed = copy.deepcopy(command)
        malformed["publication"]["evidence"]["bytes"] = "YQ"
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed)

        malformed = copy.deepcopy(command)
        malformed["publication"]["evidence"]["bytes"] = "A" * 11_184_816
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed)

    def test_current_protocol_generations_reject_immediate_predecessors(self) -> None:
        candidate = FlowBuilder("version", {}, {}).finish({"kind": "input"})
        self.assertEqual(candidate["ir_version"], "cymule.ir/3")
        legacy_candidate = copy.deepcopy(candidate)
        legacy_candidate["ir_version"] = "cymule.ir/2"
        with self.assertRaises(EngineError):
            _validate_plan_candidate(legacy_candidate)

        durable = DurableControlBuilder.run_current("run:version", None)
        self.assertEqual(
            durable["control_version"], "cymule.durable-control/4"
        )
        durable["control_version"] = "cymule.durable-control/3"
        with self.assertRaises(EngineError):
            _validate_durable_command_response(durable)

        live = LiveEvolutionControlBuilder.publish_definition(
            "command:version", "definition:version", candidate["definitions"][0], []
        )
        self.assertEqual(
            live["control_version"], "cymule.live-evolution-control/6"
        )
        live["control_version"] = "cymule.live-evolution-control/5"
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(live)

        self.assertEqual(
            directory_store("store")["provider"], "cymule.directory-store/5"
        )
        self.assertEqual(
            sqlite_store("store.sqlite", "domain")["provider"],
            "cymule.sqlite-store/6",
        )
        for provider_neutral_store in (
            {"provider": "acme.store/1", "location": "provider-location"},
            {
                "provider": "acme.partitioned-store/7",
                "location": "provider-location",
                "domain": "tenant-a",
            },
        ):
            _validate_engine_store_target(provider_neutral_store)
        for invalid_store in (
            {"provider": "", "location": "store"},
            {"provider": "acme.store/1", "location": "", "domain": "tenant"},
            {"provider": "acme.store/1", "location": "store", "domain": ""},
        ):
            with self.assertRaises(EngineError):
                _validate_engine_store_target(invalid_store)
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-provider-neutral-store-"
        ) as directory:
            transport = _engine_with_success(
                directory,
                {
                    "type": "durable_executed",
                    "response": {
                        "type": "run_current",
                        "observed_revision": _content_id("a"),
                        "source_root": "b" * 64,
                        "current": None,
                    },
                },
            )
            durable = DurableEngine(
                {
                    "provider": "acme.partitioned-store/7",
                    "location": "provider-owned-location",
                    "domain": "tenant-a",
                },
                None,
                None,
                transport,
            )
            self.assertIsNone(
                durable.run_current("run:provider-neutral-store", None)["current"]
            )

    def test_python_live_evolution_outcome_variants_are_recursively_closed(self) -> None:
        valid = _fixed_live_evolution_outcomes()
        for result, response in valid.items():
            with self.subTest(result=result, case="valid"):
                _validate_live_evolution_outcome(response)

        malicious: list[
            tuple[str, str, Callable[[dict[str, object]], None]]
        ] = [
            (
                "definition_published",
                "wrong revision version",
                lambda response: response["revision"].__setitem__(
                    "revision_version", "cymule.subflow-revision/1"
                ),
            ),
            (
                "definition_published",
                "non-content revision identity",
                lambda response: response["revision"].__setitem__(
                    "revision_id", "revision:forged"
                ),
            ),
            (
                "definition_published",
                "zero revision sequence",
                lambda response: response["revision"].__setitem__("sequence", 0),
            ),
            (
                "definition_published",
                "oversized UTF-8 definition reference",
                lambda response: response["revision"].__setitem__(
                    "logical_ref", "é" * 81
                ),
            ),
            (
                "definition_published",
                "malformed nested Definition",
                lambda response: response["revision"]["definition"].__setitem__(
                    "unexpected", True
                ),
            ),
            (
                "definition_published",
                "unhashable reference strategy",
                lambda response: response["revision"]["references"][0][
                    "strategy"
                ].__setitem__("strategy", []),
            ),
            (
                "definition_published",
                "oversized UTF-8 local definition",
                lambda response: response["revision"]["references"][0].__setitem__(
                    "local_definition", "é" * 81
                ),
            ),
            (
                "template_registered",
                "malformed sealed Plan candidate",
                lambda response: response["linked"]["plan"]["candidate"].__setitem__(
                    "definitions", []
                ),
            ),
            (
                "publication_applied",
                "non-boolean advance",
                lambda response: response["receipt"]["updates"][0].__setitem__(
                    "advanced", "true"
                ),
            ),
            (
                "publication_applied",
                "non-canonical template order",
                lambda response: response["receipt"]["updates"].reverse(),
            ),
            (
                "publication_applied",
                "advance relation mismatch",
                lambda response: response["receipt"]["updates"][0].__setitem__(
                    "advanced", False
                ),
            ),
            (
                "patch_applied",
                "same source and target Plan",
                lambda response: response["edge"].__setitem__(
                    "to_plan", response["edge"]["from_plan"]
                ),
            ),
            (
                "patch_applied",
                "empty patch operation set",
                lambda response: response["edge"].__setitem__("operations", []),
            ),
            (
                "patch_applied",
                "unknown patch kind",
                lambda response: response["edge"]["operations"][0].__setitem__(
                    "kind", "future"
                ),
            ),
            (
                "patch_applied",
                "malformed patch digest",
                lambda response: response["edge"]["operations"][0].__setitem__(
                    "after", "A" * 64
                ),
            ),
            (
                "patch_applied",
                "non-canonical patch order",
                lambda response: response["edge"]["operations"].reverse(),
            ),
            (
                "patch_applied",
                "retired Plan edge evidence",
                lambda response: response["edge"].__setitem__(
                    "evidence", _artifact("4", "cymule.evolution-evidence/1")
                ),
            ),
            (
                "applied",
                "unexpected applied payload",
                lambda response: response.__setitem__("receipt", {}),
            ),
            (
                "occurrence_selected",
                "wrong occurrence binding kind",
                lambda response: response["pin"]["execution_binding"].__setitem__(
                    "kind", "test/binding"
                ),
            ),
            (
                "migrated",
                "source Run mismatch",
                lambda response: response["receipt"]["request"].__setitem__(
                    "run_id", "run:forged"
                ),
            ),
            (
                "migrated",
                "non-content adapter revision",
                lambda response: response["receipt"].__setitem__(
                    "adapter_revision", "revision:forged"
                ),
            ),
            (
                "migrated",
                "adapter does not match request",
                lambda response: response["receipt"].__setitem__(
                    "adapter_id", "adapter:forged"
                ),
            ),
            (
                "migrated",
                "malformed source witness",
                lambda response: response["receipt"].__setitem__(
                    "source_witness_id", "witness:forged"
                ),
            ),
            (
                "migrated",
                "wrong source binding kind",
                lambda response: response["receipt"]["source_binding"].__setitem__(
                    "kind", "test/binding"
                ),
            ),
            (
                "migrated",
                "target binding does not match Continuation",
                lambda response: response["receipt"]["target_binding"].__setitem__(
                    "artifact_id", _content_id("0")
                ),
            ),
            (
                "migrated",
                "Continuation generation missing",
                lambda response: response["receipt"]["target_continuation"].pop(
                    "continuation_version"
                ),
            ),
            (
                "migrated",
                "Continuation generation unsupported",
                lambda response: response["receipt"]["target_continuation"].__setitem__(
                    "continuation_version", "cymule.continuation-state/999"
                ),
            ),
            (
                "migrated",
                "target epoch is not successor",
                lambda response: (
                    response["receipt"].__setitem__("target_epoch", 2),
                    response["receipt"]["target_continuation"].__setitem__(
                        "epoch", 2
                    ),
                ),
            ),
            (
                "migrated",
                "target fence changed",
                lambda response: response["receipt"]["target_continuation"].__setitem__(
                    "execution_fence", 4
                ),
            ),
            (
                "migrated",
                "source fence changed",
                lambda response: response["receipt"].__setitem__(
                    "source_execution_fence", 4
                ),
            ),
            (
                "migrated",
                "target state mismatch",
                lambda response: response["receipt"]["target_continuation"].__setitem__(
                    "state", _artifact("0", "cymule.migration-state/1")
                ),
            ),
            (
                "restart_authorized",
                "same source and replacement Run",
                lambda response: response["receipt"]["request"].__setitem__(
                    "replacement_run",
                    response["receipt"]["request"]["run_id"],
                ),
            ),
            (
                "restart_authorized",
                "same source and target Plan",
                lambda response: response["receipt"]["request"].__setitem__(
                    "from_plan", response["receipt"]["request"]["to_plan"]
                ),
            ),
            (
                "restart_authorized",
                "malformed source witness",
                lambda response: response["receipt"].__setitem__(
                    "source_witness_id", "witness:forged"
                ),
            ),
            (
                "restart_authorized",
                "target Plan mismatch",
                lambda response: response["receipt"]["target_plan"].__setitem__(
                    "plan_id", _content_id("a")
                ),
            ),
            (
                "shadow_recorded",
                "non-boolean shadow outcome",
                lambda response: response["comparison"].__setitem__(
                    "equivalent", "true"
                ),
            ),
            (
                "shadow_recorded",
                "non-content driver revision",
                lambda response: response["comparison"].__setitem__(
                    "driver_revision", "revision:forged"
                ),
            ),
            (
                "shadow_recorded",
                "malformed primary Plan",
                lambda response: response["comparison"].__setitem__(
                    "primary_plan", "plan:forged"
                ),
            ),
            (
                "gate_applied",
                "same source and target decision",
                lambda response: response["transition"].__setitem__(
                    "to_decision", response["transition"]["from_decision"]
                ),
            ),
            (
                "gate_applied",
                "gate decision mismatch",
                lambda response: response["transition"]["evaluation"]["gate"].__setitem__(
                    "decision_id", "decision:forged"
                ),
            ),
            (
                "gate_applied",
                "failures exceed observations",
                lambda response: response["transition"]["evaluation"].__setitem__(
                    "target_failures", 2
                ),
            ),
            (
                "gate_applied",
                "evidence count mismatch",
                lambda response: response["transition"]["evaluation"].__setitem__(
                    "evidence_ids", []
                ),
            ),
            (
                "gate_applied",
                "duplicate evidence identity",
                lambda response: (
                    response["transition"]["evaluation"].__setitem__(
                        "target_observations", 2
                    ),
                    response["transition"]["evaluation"].__setitem__(
                        "evidence_ids", ["observation:test", "observation:test"]
                    ),
                ),
            ),
            (
                "gate_applied",
                "outcome does not match evidence",
                lambda response: response["transition"]["evaluation"].__setitem__(
                    "outcome", "rollback"
                ),
            ),
            (
                "gate_applied",
                "pending transition",
                lambda response: (
                    response["transition"]["evaluation"]["gate"].__setitem__(
                        "min_target_observations", 2
                    ),
                    response["transition"]["evaluation"].__setitem__(
                        "outcome", "pending"
                    ),
                ),
            ),
            (
                "gate_applied",
                "malformed evaluation identity",
                lambda response: response["transition"]["evaluation"].__setitem__(
                    "evaluation_id", "evaluation:forged"
                ),
            ),
        ]
        for result, case, mutate in malicious:
            response = copy.deepcopy(valid[result])
            mutate(response)
            with self.subTest(result=result, case=case):
                with self.assertRaises(EngineError):
                    _validate_live_evolution_outcome(response)

    def test_python_binds_evolution_commits_to_the_exact_wire_request(self) -> None:
        candidate = FlowBuilder(
            "response_match", {"const": True}, {}
        ).finish({"kind": "input"})
        command = LiveEvolutionControlBuilder.publish_definition(
            "command:response-match", "definition:response-match", candidate["definitions"][0], []
        )
        outcome = copy.deepcopy(
            _fixed_live_evolution_outcomes()["definition_published"]
        )
        outcome["revision"]["logical_ref"] = "definition:response-match"
        outcome["revision"]["definition"] = candidate["definitions"][0]
        outcome["revision"]["references"] = []
        commit = _evolution_commit(command, outcome)
        _validate_evolution_commit(commit)

        with tempfile.TemporaryDirectory(prefix="cymule-python-wrong-tag-") as directory:
            engine = _engine_with_success(directory, {"type": "verified"})
            with self.assertRaises(EngineError) as wrong_outer:
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                )
            self.assertEqual(
                wrong_outer.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                wrong_outer.exception.failure["code"], "invalid_engine_response"
            )
            self.assertEqual(
                wrong_outer.exception.failure["retry_disposition"], "reconcile"
            )
        with tempfile.TemporaryDirectory(prefix="cymule-python-read-tag-") as directory:
            engine = _engine_with_success(directory, {"type": "verified"})
            with self.assertRaises(EngineError) as read_only:
                engine.seal(candidate)
            self.assertEqual(read_only.exception.failure["category"], "transport_failure")
            self.assertEqual(read_only.exception.failure["code"], "invalid_engine_response")
            self.assertNotIn("retry_disposition", read_only.exception.failure)

        malicious_commits = []
        wrong_evolution = copy.deepcopy(commit)
        wrong_evolution["receipt"]["command"]["evolution_id"] = "evolution:forged"
        malicious_commits.append(("evolution identity", wrong_evolution))
        wrong_command = copy.deepcopy(commit)
        wrong_command["receipt"]["command"]["command"]["command_id"] = "command:forged"
        malicious_commits.append(("command", wrong_command))
        wrong_command_body = copy.deepcopy(commit)
        wrong_command_body["receipt"]["command"]["command"]["logical_ref"] = "definition:forged"
        malicious_commits.append(("command body", wrong_command_body))
        wrong_json_type = copy.deepcopy(commit)
        wrong_json_type["receipt"]["command"]["command"]["definition"]["input_schema"]["const"] = 1
        malicious_commits.append(("command JSON type", wrong_json_type))
        wrong_outcome = copy.deepcopy(commit)
        wrong_outcome["receipt"]["outcome"] = {"result": "applied"}
        malicious_commits.append(("outcome variant", wrong_outcome))
        missing_command = copy.deepcopy(commit)
        missing_command["receipt"].pop("command")
        malicious_commits.append(("missing command", missing_command))
        missing_outcome = copy.deepcopy(commit)
        missing_outcome["receipt"].pop("outcome")
        malicious_commits.append(("missing outcome", missing_outcome))
        for case, malicious in malicious_commits:
            with self.subTest(case=case), tempfile.TemporaryDirectory(
                prefix="cymule-python-wrong-receipt-"
            ) as directory:
                engine = _engine_with_success(
                    directory,
                    {"type": "live_evolution_executed", "commit": malicious},
                )
                with self.assertRaises(EngineError) as rejected:
                    engine.execute_live_evolution(
                        evolution_target(),
                        "evolution:test",
                        command,
                    )
                self.assertEqual(
                    rejected.exception.failure["category"], "unknown_world_outcome"
                )
                self.assertEqual(
                    rejected.exception.failure["code"], "invalid_engine_response"
                )
                self.assertEqual(
                    rejected.exception.failure["retry_disposition"], "reconcile"
                )

        with tempfile.TemporaryDirectory(prefix="cymule-python-match-result-") as directory:
            engine = _engine_with_success(
                directory,
                {"type": "live_evolution_executed", "commit": commit},
            )
            self.assertEqual(
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                ),
                commit,
            )

    def test_python_correlates_every_success_and_payload_with_request(self) -> None:
        candidate = FlowBuilder("echo_correlation", {}, {}).finish({"kind": "input"})
        sealed = {"plan_id": _content_id("1"), "candidate": candidate}

        with tempfile.TemporaryDirectory(prefix="cymule-python-seal-echo-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {"type": "seal", "candidate": {**candidate, "name": "forged"}},
                    "response": {"type": "sealed", "plan": sealed},
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.seal(candidate)
            self.assertEqual(rejected.exception.failure["category"], "transport_failure")
            self.assertEqual(rejected.exception.failure["code"], "invalid_engine_response")
            self.assertNotIn("retry_disposition", rejected.exception.failure)

        with tempfile.TemporaryDirectory(prefix="cymule-python-legacy-v4-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "response": {"type": "sealed", "plan": sealed},
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.seal(candidate)
            self.assertEqual(rejected.exception.failure["category"], "transport_failure")
            self.assertEqual(rejected.exception.failure["code"], "invalid_engine_response")

        clock_target = sqlite_clock(
            "clock.sqlite", "clock:echo", _content_id("2")
        )
        clock_echo = {
            "type": "observe_clock",
            "target": clock_target,
            "run_id": "run:forged",
        }
        observation = {
            "clock_version": "cymule.clock-observation/2",
            "observation_id": _content_id("3"),
            "source_id": "clock:echo",
            "source_generation": _content_id("2"),
            "scope": "scope:echo",
        }
        with tempfile.TemporaryDirectory(prefix="cymule-python-clock-echo-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": clock_echo,
                    "response": {
                        "type": "clock_observed",
                        "result": {
                            "run_id": "run:forged",
                            "observation": observation,
                        },
                    },
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.observe_clock(clock_target, "run:expected")
            self.assertEqual(
                rejected.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "reconcile"
            )

        candidate = FlowBuilder("binding", {}, {}).finish({"kind": "input"})
        plan = {"plan_id": _content_id("1"), "candidate": candidate}
        run_request = {
            "type": "run",
            "plan": plan,
            "input": None,
            "plugin": process_target("/bin/true"),
            "run_id": "run:expected",
        }
        forged_execution = {
            "status": "completed",
            "result": {
                "run_id": "run:forged",
                "plan_id": plan["plan_id"],
                "value": None,
                "projection_digest": "2" * 64,
                "precondition_token": f"pre:0:{_content_id('3')}",
                "effects": [],
            },
        }
        durable_command = DurableControlBuilder.resume_run(
            "run:expected", fixture_execution()
        )
        durable_target = durable_target_for(durable_command)
        durable_request = {
            "type": "execute_durable",
            "target": durable_target,
            "command": durable_command,
        }
        forged_boundary = {
            "type": "run_boundary",
            "boundary": {
                "status": "completed",
                "result": {
                    "run_id": "run:forged",
                    "plan_id": _content_id("6"),
                    "value": None,
                    "projection_digest": "7" * 64,
                    "precondition_token": f"pre:0:{_content_id('8')}",
                    "effects": [],
                },
            },
        }
        cases = (
            (
                run_request,
                {"type": "execution_boundary", "execution": forged_execution},
                lambda engine: engine.run(
                    plan, None, process_target("/bin/true"), "run:expected"
                ),
            ),
            (
                durable_request,
                {"type": "durable_executed", "response": forged_boundary},
                lambda engine: engine.execute_durable(
                    durable_target, durable_command
                ),
            ),
        )
        for request, response, invoke in cases:
            with self.subTest(request=request["type"]), tempfile.TemporaryDirectory(
                prefix="cymule-python-payload-binding-"
            ) as directory:
                engine = _engine_with_envelope(
                    directory,
                    {
                        "engine_protocol": "cymule.engine/5",
                        "outcome": "success",
                        "request": request,
                        "response": response,
                    },
                )
                with self.assertRaises(EngineError) as rejected:
                    invoke(engine)
                self.assertEqual(
                    rejected.exception.failure["category"],
                    "unknown_world_outcome",
                )
                self.assertEqual(
                    rejected.exception.failure["retry_disposition"],
                    "reconcile",
                )

        cancel = DurableControlBuilder.cancel_run(
            "cancel:echo", "run:echo", {"reason": "test"}
        )
        cancel_echo = copy.deepcopy(cancel)
        cancel_echo["cancellation_id"] = "cancel:forged"
        cancel_target = {"store": directory_store("unused")}
        with tempfile.TemporaryDirectory(prefix="cymule-python-cancel-echo-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {
                        "type": "execute_durable",
                        "target": cancel_target,
                        "command": cancel_echo,
                    },
                    "response": {"type": "verified"},
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.execute_durable(cancel_target, cancel)
            self.assertEqual(
                rejected.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "reconcile"
            )

        live_command = LiveEvolutionControlBuilder.publish_definition(
            "command:echo-live",
            "definition:echo-live",
            candidate["definitions"][0],
            [],
        )
        live_outcome = copy.deepcopy(
            _fixed_live_evolution_outcomes()["definition_published"]
        )
        live_outcome["revision"]["logical_ref"] = "definition:echo-live"
        live_outcome["revision"]["definition"] = candidate["definitions"][0]
        live_outcome["revision"]["references"] = []
        live_commit = _evolution_commit(live_command, live_outcome)
        live_echo = copy.deepcopy(live_command)
        live_echo["command_id"] = "command:forged"
        with tempfile.TemporaryDirectory(prefix="cymule-python-live-echo-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {
                        "type": "execute_live_evolution",
                        "target": evolution_target(),
                        "evolution_id": "evolution:test",
                        "command": live_echo,
                    },
                    "response": {
                        "type": "live_evolution_executed",
                        "commit": live_commit,
                    },
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.execute_live_evolution(
                    evolution_target(),
                    "evolution:test",
                    live_command,
                )
            self.assertEqual(
                rejected.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "reconcile"
            )

        selection = LiveEvolutionControlBuilder.apply(
            "command:echo-omitted",
            "template:echo",
            EvolutionControlBuilder.select_occurrence(
                "command:echo-select",
                "occurrence:echo",
                "selection:echo",
                _artifact("4", "cymule.execution-binding/2"),
            ),
        )
        null_echo = copy.deepcopy(selection)
        null_echo["safe_point"] = None
        with tempfile.TemporaryDirectory(prefix="cymule-python-null-echo-") as directory:
            engine = _engine_with_envelope(
                directory,
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {
                        "type": "verify_live_evolution_command",
                        "command": null_echo,
                    },
                    "response": {
                        "type": "verified_live_evolution_command",
                        "command": selection,
                    },
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.verify_live_evolution_command(selection)
            self.assertEqual(rejected.exception.failure["category"], "transport_failure")
            self.assertEqual(rejected.exception.failure["code"], "invalid_engine_response")
            self.assertNotIn("retry_disposition", rejected.exception.failure)

    def test_python_durable_clock_rejects_a_typed_result_for_another_run(self) -> None:
        clock = sqlite_clock(
            "clock.sqlite", "clock:fake-run", _content_id("1")
        )

        class WrongRunClockTransport:
            @staticmethod
            def observe_clock(
                _target: dict[str, object], _run_id: str
            ) -> dict[str, object]:
                return {
                    "run_id": "run:foreign",
                    "observation": {
                        "clock_version": "cymule.clock-observation/2",
                        "observation_id": _content_id("2"),
                        "source_id": clock["source_id"],
                        "source_generation": clock["source_generation"],
                        "scope": _content_id("3"),
                    },
                }

        durable = DurableEngine(
            directory_store("unused"),
            None,
            clock,
            WrongRunClockTransport(),  # type: ignore[arg-type]
        )
        with self.assertRaises(EngineError) as rejected:
            durable.observe_clock("run:expected")
        self.assertEqual(
            rejected.exception.failure["category"], "unknown_world_outcome"
        )
        self.assertEqual(
            rejected.exception.failure["retry_disposition"], "reconcile"
        )

    def test_python_binds_every_verify_result_to_the_exact_wire_value(self) -> None:
        activation = WaitActivationBuilder.signal(
            "activation:verify",
            "signal:verify",
            [_content_id("a")],
            _artifact("1", "cymule.wait-result/1"),
        )
        forged_activation = copy.deepcopy(activation)
        forged_activation["activation_id"] = "activation:forged"

        durable = DurableControlBuilder.run_current("run:verify", None)
        forged_durable = copy.deepcopy(durable)
        forged_durable["run_id"] = "run:forged"

        evolution = EvolutionControlBuilder.select_occurrence(
            "command:verify-evolution",
            "occurrence:verify",
            "selection:verify",
            _artifact("2", "cymule.execution-binding/2"),
        )
        forged_evolution = copy.deepcopy(evolution)
        forged_evolution["selection_id"] = "selection:forged"

        candidate = FlowBuilder("verify_result", {}, {}).finish({"kind": "input"})
        live = LiveEvolutionControlBuilder.publish_definition(
            "command:verify-live",
            "definition:verify-live",
            candidate["definitions"][0],
            [],
        )
        forged_live = copy.deepcopy(live)
        forged_live["logical_ref"] = "definition:forged"

        cases = [
            (
                "wait activation",
                {"type": "verified_wait_activation", "activation": forged_activation},
                lambda engine: engine.verify_wait_activation(activation),
            ),
            (
                "durable command",
                {"type": "verified_durable_command", "command": forged_durable},
                lambda engine: engine.verify_durable_command(durable),
            ),
            (
                "evolution command",
                {"type": "verified_evolution_command", "command": forged_evolution},
                lambda engine: engine.verify_evolution_command(evolution),
            ),
            (
                "live-evolution command",
                {"type": "verified_live_evolution_command", "command": forged_live},
                lambda engine: engine.verify_live_evolution_command(live),
            ),
        ]
        for label, response, operation in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory(
                prefix="cymule-python-verify-binding-"
            ) as directory:
                with self.assertRaises(EngineError) as rejected:
                    operation(_engine_with_success(directory, response))
                self.assertEqual(
                    rejected.exception.failure["category"], "transport_failure"
                )
                self.assertEqual(
                    rejected.exception.failure["code"], "invalid_engine_response"
                )
                self.assertNotIn(
                    "retry_disposition", rejected.exception.failure
                )

    def test_python_binds_typed_durable_receipts_to_commands(self) -> None:
        activation = DurableControlBuilder.activate_signal(
            "activation:receipt",
            "signal:receipt",
            [_content_id("a")],
            {"accepted": True},
        )
        activation_receipt = {
            "receipt_version": "cymule.wait-activation-receipt/3",
            "activation": {
                "activation_version": "cymule.wait-activation/2",
                "activation_id": activation["activation_id"],
                "source": activation["source"],
                "wait_ids": activation["wait_ids"],
                "result": _artifact("9", "cymule.wait-result/1"),
            },
            "applied_wait_ids": activation["wait_ids"],
            "ready_run_ids": ["run:ready"],
        }
        activation_response = {
            "type": "durable_executed",
            "response": {
                "type": "wait_activated",
                "receipt": activation_receipt,
            },
        }
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-wait-receipt-"
        ) as directory:
            self.assertEqual(
                _engine_with_success(directory, activation_response).execute_durable(
                    durable_target_for(activation), activation
                ),
                activation_response["response"],
            )
        forged_activation = copy.deepcopy(activation_response)
        forged_activation["response"]["receipt"]["applied_wait_ids"] = [
            _content_id("b")
        ]
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-forged-wait-receipt-"
        ) as directory, self.assertRaises(EngineError):
            _engine_with_success(directory, forged_activation).execute_durable(
                durable_target_for(activation), activation
            )

        reason = {"code": "operator_request", "detail": None}
        cancel = DurableControlBuilder.cancel_run(
            "cancel:receipt", "run:receipt", reason
        )
        reason_ref = _artifact("a", "cymule.cancellation-reason/1")
        cancellation_receipt = {
            "receipt_version": "cymule.run-cancellation-receipt/1",
            "command": {
                "cancellation_id": cancel["cancellation_id"],
                "run_id": cancel["run_id"],
                "reason": reason,
            },
            "boundary": {"status": "cancelled", "reason": reason_ref},
            "receipt_id": "a" * 64,
        }
        cancel_target = durable_target_for(cancel)
        cancellation_response = {
            "type": "durable_executed",
            "response": {"type": "run_cancelled", "receipt": cancellation_receipt},
        }
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-cancel-receipt-"
        ) as directory:
            self.assertEqual(
                _engine_with_success(directory, cancellation_response).execute_durable(
                    cancel_target, cancel
                )["type"],
                "run_cancelled",
            )
        forged_cancellation = copy.deepcopy(cancellation_response)
        forged_cancellation["response"]["receipt"]["command"]["reason"] = {
            "code": "forged"
        }
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-forged-cancel-receipt-"
        ) as directory, self.assertRaises(EngineError) as rejected:
            _engine_with_success(directory, forged_cancellation).execute_durable(
                cancel_target, cancel
            )
        self.assertEqual(
            rejected.exception.failure["category"], "unknown_world_outcome"
        )
        binding = _artifact("b", "cymule.execution-binding/2")
        resolution = DurableControlBuilder.resolve_effect(
            "resolution:receipt",
            "run:receipt",
            _content_id("c"),
            binding,
            _content_id("d"),
            "driver:receipt",
            7,
            "resolved_applied",
            {"requested": True},
        )
        with self.assertRaisesRegex(ValueError, "occurrence binding"):
            DurableControlBuilder.resolve_effect(
                "resolution:legacy-occurrence-binding",
                "run:receipt",
                _content_id("c"),
                binding,
                "binding:not-content",
                "driver:receipt",
                7,
                "resolved_applied",
                {"requested": True},
            )
        with self.assertRaises(ValueError):
            DurableControlBuilder.resolve_effect(
                "resolution:not-applied-with-value",
                "run:receipt",
                _content_id("c"),
                binding,
                _content_id("d"),
                "driver:receipt",
                7,
                "resolved_not_applied",
                {"forged": True},
            )
        resolution_receipt = {
            "receipt_version": "cymule.effect-resolution-receipt/1",
            "command": {
                "resolution_id": resolution["resolution_id"],
                "run_id": resolution["run_id"],
                "intent_id": resolution["intent_id"],
                "execution_binding": binding,
                "occurrence_binding": resolution["occurrence_binding"],
                "claim_owner": resolution["claim_owner"],
                "claim_epoch": resolution["claim_epoch"],
                "resolution": resolution["resolution"],
                "value": {"requested": True},
            },
            "actual_resolution": "resolved_not_applied",
            "actual_value": None,
            "result": None,
            "receipt_id": "b" * 64,
        }
        resolution_target = durable_target_for(resolution)
        resolution_response = {
            "type": "durable_executed",
            "response": {"type": "effect_resolved", "receipt": resolution_receipt},
        }
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-resolution-receipt-"
        ) as directory:
            self.assertEqual(
                _engine_with_success(directory, resolution_response).execute_durable(
                    resolution_target, resolution
                )["type"],
                "effect_resolved",
            )
        forged_resolution = copy.deepcopy(resolution_response)
        applied_null = copy.deepcopy(resolution_response)
        applied_null["response"]["receipt"]["actual_resolution"] = "resolved_applied"
        applied_null["response"]["receipt"]["result"] = _artifact("e", "cymule.effect-result/1")
        with tempfile.TemporaryDirectory(prefix="cymule-python-applied-null-") as directory:
            self.assertEqual(
                _engine_with_success(directory, applied_null).execute_durable(
                    resolution_target, resolution
                )["type"],
                "effect_resolved",
            )
        applied_null["response"]["receipt"]["result"] = None
        with tempfile.TemporaryDirectory(prefix="cymule-python-applied-null-missing-") as directory:
            with self.assertRaises(EngineError) as rejected:
                _engine_with_success(directory, applied_null).execute_durable(
                    resolution_target, resolution
                )
        self.assertEqual(rejected.exception.failure["category"], "unknown_world_outcome")
        forged_resolution["response"]["receipt"]["command"]["claim_epoch"] = 8
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-forged-resolution-receipt-"
        ) as directory, self.assertRaises(EngineError) as rejected:
            _engine_with_success(directory, forged_resolution).execute_durable(
                resolution_target, resolution
            )
        self.assertEqual(
            rejected.exception.failure["category"], "unknown_world_outcome"
        )
        forged_result = copy.deepcopy(resolution_response)
        forged_result["response"]["receipt"]["actual_value"] = {"forged": True}
        forged_result["response"]["receipt"]["result"] = _artifact(
            "e", "cymule.effect-result/1"
        )
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-forged-resolution-result-"
        ) as directory, self.assertRaises(EngineError) as rejected:
            _engine_with_success(directory, forged_result).execute_durable(
                resolution_target, resolution
            )
        self.assertEqual(
            rejected.exception.failure["category"], "unknown_world_outcome"
        )

    def test_python_resource_handoff_v5_requires_exact_provenance(self) -> None:
        resource = _artifact("a", "cymule.resource-handle/2")
        producer = {
            "run_id": "run:producer",
            "occurrence_id": _content_id("b"),
            "result": resource,
        }
        handoff = ResourceBuilder.handoff(
            "transfer:test",
            producer,
            "run:consumer",
            "input:resource",
            resource,
        )
        self.assertEqual(
            handoff["handoff_version"], "cymule.resource-handoff/5"
        )
        activation: ResourceHandoffActivation = {
            "activation_version": "cymule.resource-handoff-activation/3",
            "activation_id": _content_id("d"),
            "transfer_id": handoff["transfer_id"],
            "to_run": handoff["to_run"],
            "wait_id": "wait:resource",
            "result": resource,
        }
        self.assertEqual(
            set(activation),
            {
                "activation_version",
                "activation_id",
                "transfer_id",
                "to_run",
                "wait_id",
                "result",
            },
        )
        forged = _artifact("c", "cymule.resource-handle/2")
        with self.assertRaises(ValueError):
            ResourceBuilder.handoff(
                "transfer:test",
                producer,
                "run:consumer",
                "input:resource",
                forged,
            )
        with self.assertRaises(ValueError):
            ResourceBuilder.handoff(
                "transfer:test",
                producer,
                producer["run_id"],
                "input:resource",
                resource,
            )

    def test_python_validates_and_binds_complete_resource_handles(self) -> None:
        digest = _content_id("1")
        manifest_root = _content_id("2")
        manifest_digest = _manifest_descriptor_id(manifest_root, 10, 1)
        empty_root = (
            "sha256:6a754fadbb296b87040c37dab30caea6"
            "3de1bd1a85142bc82a03a7cf82e64dfc"
        )
        empty_digest = _manifest_descriptor_id(empty_root, 0, 0)
        self.assertNotIn("annotations", ResourceBuilder.text("no annotations"))
        valid_candidates = [
            ResourceBuilder.text("resource validation"),
            ResourceBuilder.json({"nested": [1, True, None]}),
            {
                "resource_version": "cymule.resource/3",
                "shape": "inline",
                "media_type": "application/octet-stream",
                "inline": {"encoding": "base64", "data": "YQ=="},
                "integrity": {"kind": "inline"},
            },
            ResourceBuilder.external(
                "object",
                "application/octet-stream",
                {"kind": "content", "digest": digest, "size": 1},
            ),
            ResourceBuilder.external(
                "object",
                "application/octet-stream",
                {"kind": "version", "authority": "store:test", "version": "v1"},
            ),
            ResourceBuilder.external(
                "snapshot",
                "application/octet-stream",
                {"kind": "live", "identity": "workspace:test"},
            ),
            ResourceBuilder.external(
                "collection",
                "application/octet-stream",
                {"kind": "content", "digest": manifest_digest, "size": 10},
                {
                    "manifest_version": "cymule.resource-manifest/3",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": manifest_digest,
                    "size": 10,
                    "entry_count": 1,
                    "root_digest": manifest_root,
                },
                {"purpose": "validation"},
            ),
            ResourceBuilder.external(
                "directory",
                "application/octet-stream",
                {
                    "kind": "content",
                    "digest": empty_digest,
                    "size": 0,
                },
                {
                    "manifest_version": "cymule.resource-manifest/3",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": empty_digest,
                    "size": 0,
                    "entry_count": 0,
                    "root_digest": empty_root,
                },
            ),
        ]
        for index, candidate in enumerate(valid_candidates):
            returned_candidate = copy.deepcopy(candidate)
            if returned_candidate.get("annotations") == {}:
                returned_candidate.pop("annotations")
            response = {
                "type": "sealed_resource",
                "resource": {
                    "resource_id": _content_id(str(index + 1)),
                    **returned_candidate,
                },
            }
            with self.subTest(index=index), tempfile.TemporaryDirectory(
                prefix="cymule-python-resource-valid-"
            ) as directory:
                self.assertEqual(
                    _engine_with_success(directory, response).seal_resource(candidate),
                    response["resource"],
                )

        expected = ResourceBuilder.text("expected")
        forged = ResourceBuilder.text("forged")
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-resource-binding-"
        ) as directory:
            engine = _engine_with_success(
                directory,
                {
                    "type": "sealed_resource",
                    "resource": {"resource_id": _content_id("8"), **forged},
                },
            )
            with self.assertRaises(EngineError) as rejected:
                engine.seal_resource(expected)
            self.assertEqual(
                rejected.exception.failure["category"], "transport_failure"
            )
            self.assertEqual(
                rejected.exception.failure["code"], "invalid_engine_response"
            )

    def test_python_rejects_malformed_resource_handle_relationships(self) -> None:
        digest = _content_id("1")
        manifest_digest = _manifest_descriptor_id(_content_id("2"), 10, 1)
        inline = ResourceBuilder.text("value")
        invalid_candidates = [
            {**inline, "annotations": {}},
            ResourceBuilder.external(
                "directory",
                "application/octet-stream",
                {
                    "kind": "content",
                    "digest": (
                        "sha256:e3b0c44298fc1c149afbf4c8996fb924"
                        "27ae41e4649b934ca495991b7852b855"
                    ),
                    "size": 0,
                },
                {
                    "manifest_version": "cymule.resource-manifest/2",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": (
                        "sha256:e3b0c44298fc1c149afbf4c8996fb924"
                        "27ae41e4649b934ca495991b7852b855"
                    ),
                    "size": 0,
                    "entry_count": 0,
                    "root_digest": (
                        "sha256:b6009c22e4a61a949312181d089c381"
                        "94269a3aa38098801fa38a6d8307050a3"
                    ),
                },
            ),
            {**inline, "inline": {"encoding": "utf8"}},
            {
                "resource_version": "cymule.resource/3",
                "shape": "object",
                "media_type": "application/octet-stream",
                "inline": {"encoding": "utf8", "text": "forged"},
                "integrity": {"kind": "content", "digest": digest, "size": 6},
            },
            {
                "resource_version": "cymule.resource/3",
                "shape": "collection",
                "media_type": "application/octet-stream",
                "integrity": {"kind": "content", "digest": digest, "size": 10},
                "manifest": {
                    "manifest_version": "cymule.resource-manifest/3",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": _content_id("2"),
                    "size": 10,
                    "entry_count": 1,
                    "root_digest": _content_id("3"),
                },
            },
            {**inline, "media_type": "Text/Plain"},
            ResourceBuilder.external(
                "object",
                "application/octet-stream",
                {"kind": "version", "authority": "", "version": "v1"},
            ),
            {**inline, "annotations": {"key": "x" * 4097}},
            {**inline, "inline": {"encoding": "base64", "data": "YQ"}},
            ResourceBuilder.external(
                "object",
                "application/octet-stream",
                {"kind": "content", "digest": "sha256:" + "g" * 64, "size": 1},
            ),
            {**inline, "integrity": {"kind": "live", "identity": "inline"}},
            {**inline, "annotations": None},
            ResourceBuilder.external(
                "collection",
                "application/octet-stream",
                {"kind": "content", "digest": digest, "size": 1},
                None,
            )
            | {"manifest": None},
            ResourceBuilder.external(
                "directory",
                "application/octet-stream",
                {"kind": "content", "digest": digest, "size": 0},
                {
                    "manifest_version": "cymule.resource-manifest/3",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": digest,
                    "size": 0,
                    "entry_count": 0,
                    "root_digest": _content_id("2"),
                },
            ),
            ResourceBuilder.external(
                "collection",
                "application/octet-stream",
                {"kind": "content", "digest": manifest_digest, "size": 10},
                {
                    "manifest_version": "cymule.resource-manifest/3",
                    "media_type": "application/vnd.cymule.resource-manifest+jsonl",
                    "digest": manifest_digest,
                    "size": 10,
                    "entry_count": 1,
                    "root_digest": _content_id("3"),
                },
            ),
        ]
        for index, candidate in enumerate(invalid_candidates):
            with self.subTest(index=index), tempfile.TemporaryDirectory(
                prefix="cymule-python-resource-invalid-"
            ) as directory:
                engine = _engine_with_success(
                    directory,
                    {
                        "type": "sealed_resource",
                        "resource": {
                            "resource_id": _content_id(str((index % 9) + 1)),
                            **candidate,
                        },
                    },
                )
                with self.assertRaises(EngineError) as rejected:
                    engine.seal_resource(candidate)
                self.assertEqual(
                    rejected.exception.failure["category"], "transport_failure"
                )
                self.assertEqual(
                    rejected.exception.failure["code"], "invalid_engine_response"
                )

        with tempfile.TemporaryDirectory(
            prefix="cymule-python-resource-id-"
        ) as directory:
            engine = _engine_with_success(
                directory,
                {
                    "type": "sealed_resource",
                    "resource": {"resource_id": "sha256:" + "G" * 64, **inline},
                },
            )
            with self.assertRaises(EngineError):
                engine.seal_resource(inline)

    def test_python_rejects_null_failure_members_and_non_ascii_codes(self) -> None:
        failure = {
            "category": "contract_violation",
            "phase": "validate_request",
            "code": "invalid_request",
            "message": "request is invalid",
            "contract": "cymule.engine/5",
            "contract_side": "input",
            "path": "/request",
            "issues": [
                {
                    "code": "invalid_member",
                    "message": "member is invalid",
                    "path": "/request/value",
                    "schema_path": "/properties/value",
                }
            ],
            "retry_disposition": "correct_and_retry",
        }
        malformed_failures = []
        for field in (
            "contract",
            "contract_side",
            "path",
            "issues",
            "retry_disposition",
        ):
            malformed = copy.deepcopy(failure)
            malformed[field] = None
            malformed_failures.append((f"null {field}", malformed))
        for field in ("path", "schema_path"):
            malformed = copy.deepcopy(failure)
            malformed["issues"][0][field] = None
            malformed_failures.append((f"null issue {field}", malformed))
        non_ascii_code = copy.deepcopy(failure)
        non_ascii_code["code"] = "échec"
        malformed_failures.append(("non-ASCII code", non_ascii_code))
        incompatible_retry = copy.deepcopy(failure)
        incompatible_retry["category"] = "unknown_world_outcome"
        incompatible_retry["retry_disposition"] = "retry_same_request"
        malformed_failures.append(("incompatible retry", incompatible_retry))

        for label, malformed in malformed_failures:
            with self.subTest(label=label), self.assertRaises(EngineError):
                _validate_engine_envelope(
                    {
                        "engine_protocol": "cymule.engine/5",
                        "outcome": "failure",
                        "error": malformed,
                    }
                )

        null_failure = copy.deepcopy(failure)
        null_failure["path"] = None
        envelope = {
            "engine_protocol": "cymule.engine/5",
            "outcome": "failure",
            "error": null_failure,
        }
        candidate = FlowBuilder("failure_null", {}, {}).finish({"kind": "input"})
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-failure-null-read-"
        ) as directory:
            with self.assertRaises(EngineError) as rejected:
                _engine_with_envelope(directory, envelope).seal(candidate)
            self.assertEqual(
                rejected.exception.failure["category"], "transport_failure"
            )
            self.assertNotIn("retry_disposition", rejected.exception.failure)
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-failure-null-mutation-"
        ) as directory:
            command = DurableControlBuilder.cancel_run(
                "cancel:null-failure", "run:null-failure", {"reason": "test"}
            )
            with self.assertRaises(EngineError) as rejected:
                _engine_with_envelope(directory, envelope).execute_durable(
                    {"store": directory_store("unused")}, command
                )
            self.assertEqual(
                rejected.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "reconcile"
            )

    def test_python_engine_failure_bounds_count_unicode_scalars(self) -> None:
        failure = {
            "category": "contract_violation",
            "phase": "validate_request",
            "code": "unicode_failure",
            "message": "🙂" * 8192,
            "contract": "界" * 500,
            "contract_side": "input",
            "path": "/" + "🙂" * 999,
            "issues": [
                {
                    "code": "界" * 200,
                    "message": "🙂" * 2000,
                    "path": "/" + "界" * 999,
                    "schema_path": "/" + "🙂" * 999,
                }
            ],
            "retry_disposition": "correct_and_retry",
        }

        def envelope(candidate: dict[str, object]) -> dict[str, object]:
            return {
                "engine_protocol": "cymule.engine/5",
                "outcome": "failure",
                "error": candidate,
            }

        _validate_engine_envelope(envelope(failure))
        invalid = [
            (
                "message",
                lambda candidate: candidate.__setitem__("message", "🙂" * 8193),
            ),
            (
                "contract",
                lambda candidate: candidate.__setitem__("contract", "界" * 501),
            ),
            (
                "control in message",
                lambda candidate: candidate.__setitem__("message", "invalid\nmessage"),
            ),
            (
                "control in contract",
                lambda candidate: candidate.__setitem__("contract", "invalid\0contract"),
            ),
            (
                "issue code",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "code", "界" * 201
                ),
            ),
            (
                "issue message",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "message", "🙂" * 2001
                ),
            ),
            (
                "control in issue code",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "code", "invalid\ncode"
                ),
            ),
            (
                "control in issue message",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "message", "invalid\0message"
                ),
            ),
            (
                "failure path",
                lambda candidate: candidate.__setitem__(
                    "path", "/" + "🙂" * 1000
                ),
            ),
            (
                "issue path",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "path", "/" + "界" * 1000
                ),
            ),
            (
                "issue schema path",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "schema_path", "/" + "🙂" * 1000
                ),
            ),
            (
                "control in path",
                lambda candidate: candidate.__setitem__("path", "/invalid\npath"),
            ),
            (
                "surrogate message",
                lambda candidate: candidate.__setitem__("message", "\ud800"),
            ),
            (
                "surrogate issue",
                lambda candidate: candidate["issues"][0].__setitem__(
                    "message", "\ud800"
                ),
            ),
            (
                "surrogate path",
                lambda candidate: candidate.__setitem__("path", "/\ud800"),
            ),
        ]
        for label, mutate in invalid:
            malformed = copy.deepcopy(failure)
            mutate(malformed)
            with self.subTest(label=label), self.assertRaises(EngineError):
                _validate_engine_envelope(envelope(malformed))

    def test_python_classifies_non_v4_and_malformed_success_after_request_start(self) -> None:
        candidate = FlowBuilder("protocol_response", {}, {}).finish({"kind": "input"})
        sealed = {"plan_id": _content_id("1"), "candidate": candidate}
        legacy = {
            "engine_protocol": "cymule.engine/4",
            "outcome": "success",
            "request": {"type": "seal", "candidate": candidate},
            "response": {"type": "sealed", "plan": sealed},
        }
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-legacy-success-read-"
        ) as directory:
            with self.assertRaises(EngineError) as rejected:
                _engine_with_envelope(directory, legacy).seal(candidate)
            self.assertEqual(
                rejected.exception.failure["category"], "contract_violation"
            )
            self.assertEqual(
                rejected.exception.failure["code"], "unsupported_engine_protocol"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "never"
            )

        command = DurableControlBuilder.cancel_run(
            "cancel:legacy", "run:legacy", {"reason": "test"}
        )
        malformed_successes = [
            legacy,
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "response": {"type": "verified"},
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": [],
                "error": {},
            },
        ]
        for index, envelope in enumerate(malformed_successes):
            with self.subTest(index=index), tempfile.TemporaryDirectory(
                prefix="cymule-python-malformed-success-mutation-"
            ) as directory:
                with self.assertRaises(EngineError) as rejected:
                    _engine_with_envelope(directory, envelope).execute_durable(
                        {"store": directory_store("unused")}, command
                    )
                self.assertEqual(
                    rejected.exception.failure["category"], "unknown_world_outcome"
                )
                self.assertEqual(
                    rejected.exception.failure["code"],
                    (
                        "unsupported_engine_protocol"
                        if index == 0
                        else "invalid_engine_response"
                    ),
                )
                self.assertEqual(
                    rejected.exception.failure["retry_disposition"], "reconcile"
                )

    def test_python_accepts_registry_typed_definition_without_plan_admission(self) -> None:
        definition = {
            "id": "registry_definition",
            "input_schema": [],
            "output_schema": None,
            "body": {
                "steps": [
                    {
                        "id": "",
                        "op": "call",
                        "component": "",
                        "input": {"kind": "binding", "name": ""},
                        "bind": "",
                    }
                ],
                "result": {"kind": "binding", "name": ""},
            },
        }
        command = LiveEvolutionControlBuilder.publish_definition(
            "command:registry-typed-definition",
            "definition:registry-typed",
            definition,
            [],
        )
        outcome = copy.deepcopy(
            _fixed_live_evolution_outcomes()["definition_published"]
        )
        outcome["revision"]["logical_ref"] = "definition:registry-typed"
        outcome["revision"]["definition"] = definition
        outcome["revision"]["references"] = []
        commit = _evolution_commit(command, outcome)
        _validate_live_evolution_command(command)
        _validate_evolution_commit(commit)

        with tempfile.TemporaryDirectory(prefix="cymule-python-registry-wire-") as directory:
            engine = _engine_with_success(
                directory,
                {"type": "live_evolution_executed", "commit": commit},
            )
            self.assertEqual(
                engine.execute_live_evolution(
                    evolution_target(),
                    "evolution:test",
                    command,
                ),
                commit,
            )

    def test_python_preflights_patch_target_with_rust_seal_authority(self) -> None:
        target = FlowBuilder("patch_target", {}, {}).finish({"kind": "input"})
        response = copy.deepcopy(_fixed_live_evolution_outcomes()["patch_applied"])
        edge = response["edge"]
        command = LiveEvolutionControlBuilder.apply(
            "command:live:patch",
            "template:test",
            EvolutionControlBuilder.apply_patch(
                "command:evolution:patch",
                {
                    "from_plan": edge["from_plan"],
                    "target": target,
                    "operations": copy.deepcopy(edge["operations"]),
                    "evidence": _artifact("4", "cymule.evolution-evidence/1"),
                },
            ),
        )
        sealed = {"plan_id": edge["to_plan"], "candidate": target}
        commit = _evolution_commit(command, response)

        with tempfile.TemporaryDirectory(prefix="cymule-python-patch-seal-") as directory:
            engine, requests = _engine_with_patch_success(directory, sealed, commit)
            self.assertEqual(
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                ),
                commit,
            )
            with open(requests, encoding="utf-8") as source:
                self.assertEqual(
                    source.read().splitlines(), ["seal", "execute_live_evolution"]
                )

        forged_edge = copy.deepcopy(commit)
        forged_edge["receipt"]["outcome"]["edge"]["to_plan"] = _content_id("e")
        with tempfile.TemporaryDirectory(prefix="cymule-python-patch-edge-") as directory:
            engine, requests = _engine_with_patch_success(
                directory, sealed, forged_edge
            )
            with self.assertRaises(EngineError) as forged:
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                )
            self.assertEqual(
                forged.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                forged.exception.failure["retry_disposition"], "reconcile"
            )
            with open(requests, encoding="utf-8") as source:
                self.assertEqual(
                    source.read().splitlines(), ["seal", "execute_live_evolution"]
                )

        other_target = FlowBuilder("other_target", {}, {}).finish({"kind": "input"})
        forged_seal = {"plan_id": edge["to_plan"], "candidate": other_target}
        with tempfile.TemporaryDirectory(prefix="cymule-python-patch-preflight-") as directory:
            engine, requests = _engine_with_patch_success(
                directory, forged_seal, commit
            )
            with self.assertRaises(EngineError) as forged:
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                )
            self.assertEqual(forged.exception.failure["category"], "transport_failure")
            self.assertNotIn("retry_disposition", forged.exception.failure)
            with open(requests, encoding="utf-8") as source:
                self.assertEqual(source.read().splitlines(), ["seal"])

    def test_python_registered_template_revision_keys_match_references(self) -> None:
        candidate = FlowBuilder("template_keys", {}, {}).finish({"kind": "input"})
        references = [
            {
                "logical_ref": "definition:b",
                "local_definition": "dependency_b",
                "input_schema": {},
                "output_schema": {},
                "strategy": {"strategy": "latest_compatible"},
            },
            {
                "logical_ref": "definition:a",
                "local_definition": "dependency_a",
                "input_schema": {},
                "output_schema": {},
                "strategy": {"strategy": "latest_compatible"},
            },
        ]
        command = LiveEvolutionControlBuilder.register_template(
            "command:register:keys",
            {
                "template_id": "template:keys",
                "candidate": candidate,
                "references": references,
            },
        )
        response = copy.deepcopy(
            _fixed_live_evolution_outcomes()["template_registered"]
        )
        response["linked"]["template_id"] = "template:keys"
        response["linked"]["resolved_revisions"] = {
            "definition:a": _content_id("a"),
            "definition:b": _content_id("b"),
        }
        commit = _evolution_commit(command, response)
        with tempfile.TemporaryDirectory(prefix="cymule-python-template-keys-") as directory:
            engine = _engine_with_success(
                directory,
                {"type": "live_evolution_executed", "commit": commit},
            )
            self.assertEqual(
                engine.execute_live_evolution(
                    evolution_target(), "evolution:test", command
                ),
                commit,
            )

        for case, mutate in (
            (
                "missing",
                lambda revisions: revisions.pop("definition:b"),
            ),
            (
                "extra",
                lambda revisions: revisions.__setitem__(
                    "definition:extra", _content_id("e")
                ),
            ),
        ):
            malicious = copy.deepcopy(commit)
            mutate(
                malicious["receipt"]["outcome"]["linked"]["resolved_revisions"]
            )
            with self.subTest(case=case), tempfile.TemporaryDirectory(
                prefix=f"cymule-python-template-{case}-"
            ) as directory:
                engine = _engine_with_success(
                    directory,
                    {"type": "live_evolution_executed", "commit": malicious},
                )
                with self.assertRaises(EngineError) as forged:
                    engine.execute_live_evolution(
                        evolution_target(),
                        "evolution:test",
                        command,
                    )
                self.assertEqual(
                    forged.exception.failure["category"], "unknown_world_outcome"
                )
                self.assertEqual(
                    forged.exception.failure["retry_disposition"], "reconcile"
                )

        duplicate = copy.deepcopy(command)
        duplicate["template"]["references"][1]["logical_ref"] = "definition:b"
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(duplicate)
        missing_strategy = copy.deepcopy(command)
        del missing_strategy["template"]["references"][0]["strategy"]
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(missing_strategy)

    def test_python_live_evolution_apply_uses_only_the_nested_command(self) -> None:
        responses = _fixed_live_evolution_outcomes()
        migration_request = copy.deepcopy(
            responses["migrated"]["receipt"]["request"]
        )
        migration = LiveEvolutionControlBuilder.apply(
            "command:live:migrate",
            "template:test",
            EvolutionControlBuilder.migrate(
                "command:evolution:migrate", migration_request
            ),
        )
        _validate_live_evolution_command(migration)

        restart_request = copy.deepcopy(
            responses["restart_authorized"]["receipt"]["request"]
        )
        restart = LiveEvolutionControlBuilder.apply(
            "command:live:restart",
            "template:test",
            EvolutionControlBuilder.restart_under_new_plan(
                "command:evolution:restart", restart_request
            ),
        )
        _validate_live_evolution_command(restart)

        for label, command in (("migration", migration), ("restart", restart)):
            retired_outer_authority = copy.deepcopy(command)
            retired_outer_authority["safe_point"] = {"retired": True}
            with self.subTest(command=label, authority="outer"):
                with self.assertRaises(EngineError):
                    _validate_live_evolution_command(retired_outer_authority)

            retired_nested_authority = copy.deepcopy(command)
            retired_nested_authority["command"]["request"]["safe_point"] = {
                "retired": True
            }
            with self.subTest(command=label, authority="nested"):
                with self.assertRaises(EngineError):
                    _validate_live_evolution_command(retired_nested_authority)

        malformed_migration = copy.deepcopy(migration)
        malformed_migration["command"]["request"]["adapter_revision"] = (
            "revision:forged"
        )
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed_migration)

        malformed_restart = copy.deepcopy(restart)
        malformed_restart["command"]["request"]["replacement_run"] = (
            malformed_restart["command"]["request"]["run_id"]
        )
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(malformed_restart)

        comparison = copy.deepcopy(responses["shadow_recorded"]["comparison"])
        shadow_request = {
            field: copy.deepcopy(comparison[field])
            for field in (
                "comparison_id", "decision_id", "subject", "primary_plan",
                "shadow_plan", "driver_id", "driver_revision",
                "comparison_policy",
            )
        }
        shadow_request["input"] = _artifact("4", "cymule.evolution-evidence/1")
        shadow = LiveEvolutionControlBuilder.apply(
            "command:live:shadow",
            "template:test",
            EvolutionControlBuilder.shadow(
                "command:evolution:shadow", shadow_request
            ),
        )
        shadow_commit = _evolution_commit(shadow, responses["shadow_recorded"])
        _validate_evolution_commit(shadow_commit)
        for field, forged in (
            ("driver_id", "driver:forged"),
            ("driver_revision", _content_id("0")),
        ):
            forged_commit = copy.deepcopy(shadow_commit)
            forged_commit["receipt"]["outcome"]["comparison"][field] = forged
            with self.subTest(command="shadow", mismatch=field):
                with self.assertRaises(EngineError):
                    _validate_evolution_commit(forged_commit)

        missing_driver_pin = copy.deepcopy(shadow)
        missing_driver_pin["command"]["request"].pop("driver_revision")
        with self.assertRaises(EngineError):
            _validate_live_evolution_command(missing_driver_pin)

    def test_python_durable_engine_selects_only_the_required_evolution_provider(self) -> None:
        outcomes = _fixed_live_evolution_outcomes()
        evolution_id = "cymule.sdk.live-evolution"
        store = directory_store("unused")

        revision = outcomes["definition_published"]["revision"]
        publish_outcome = copy.deepcopy(outcomes["definition_published"])
        publish_outcome["revision"]["references"] = []
        publish = LiveEvolutionControlBuilder.publish_definition(
            "command:provider:none",
            revision["logical_ref"],
            revision["definition"],
            [],
        )
        migration_request = copy.deepcopy(
            outcomes["migrated"]["receipt"]["request"]
        )
        migration = LiveEvolutionControlBuilder.apply(
            "command:provider:migration",
            "template:test",
            EvolutionControlBuilder.migrate(
                "command:provider:migration:child", migration_request
            ),
        )
        comparison = outcomes["shadow_recorded"]["comparison"]
        shadow = LiveEvolutionControlBuilder.apply(
            "command:provider:shadow",
            "template:test",
            EvolutionControlBuilder.shadow(
                "command:provider:shadow:child",
                {
                    "comparison_id": comparison["comparison_id"],
                    "decision_id": comparison["decision_id"],
                    "subject": comparison["subject"],
                    "primary_plan": comparison["primary_plan"],
                    "shadow_plan": comparison["shadow_plan"],
                    "driver_id": comparison["driver_id"],
                    "driver_revision": comparison["driver_revision"],
                    "input": _artifact("4", "cymule.evolution-evidence/1"),
                    "comparison_policy": comparison["comparison_policy"],
                },
            ),
        )
        migration_provider = {
            "adapter_id": migration_request["adapter_id"],
            "adapter_revision": migration_request["adapter_revision"],
            "process": process_target(
                "/bin/true", migration_request["adapter_revision"]
            ),
        }
        shadow_request = shadow["command"]["request"]
        shadow_provider = {
            "driver_id": shadow_request["driver_id"],
            "driver_revision": shadow_request["driver_revision"],
            "process": process_target(
                "/bin/true", shadow_request["driver_revision"]
            ),
        }
        target_execution = process_target("/bin/true")
        target_execution["revision"] = _content_id("9")
        exact_target = {
            "store": store,
            "migration_adapter": copy.deepcopy(migration_provider),
            "shadow_driver": None,
            "target_execution_bindings": {
                migration_request["to_plan"]: target_execution
            },
        }
        for limit in (16 * 1024 * 1024 - 1, 16 * 1024 * 1024 + 1):
            invalid_target = copy.deepcopy(exact_target)
            invalid_target["migration_adapter"]["process"]["process"][
                "message_limit"
            ] = limit
            with self.assertRaises(EngineError) as invalid_limit:
                CliEngine("missing-engine").execute_live_evolution(
                    invalid_target,
                    evolution_id,
                    migration,
                )
            self.assertEqual(
                invalid_limit.exception.failure["category"], "validation"
            )
        invalid_target_bindings = []
        missing_target_bindings = copy.deepcopy(exact_target)
        del missing_target_bindings["target_execution_bindings"]
        invalid_target_bindings.append(missing_target_bindings)
        too_many_target_bindings = copy.deepcopy(exact_target)
        too_many_target_bindings["target_execution_bindings"][
            _content_id("8")
        ] = target_execution
        invalid_target_bindings.append(too_many_target_bindings)
        unpinned_target_binding = copy.deepcopy(exact_target)
        del next(iter(unpinned_target_binding["target_execution_bindings"].values()))[
            "revision"
        ]
        invalid_target_bindings.append(unpinned_target_binding)
        for invalid_target in invalid_target_bindings:
            with self.assertRaises(EngineError) as invalid_binding:
                CliEngine("missing-engine").execute_live_evolution(
                    invalid_target, evolution_id, migration
                )
            self.assertEqual(
                invalid_binding.exception.failure["category"], "validation"
            )

        for label, command, outcome, target in (
            (
                "none",
                publish,
                publish_outcome,
                {
                    "store": store,
                    "migration_adapter": None,
                    "shadow_driver": None,
                    "target_execution_bindings": {},
                },
            ),
            (
                "migration",
                migration,
                outcomes["migrated"],
                {
                    "store": store,
                    "migration_adapter": migration_provider,
                    "shadow_driver": None,
                    "target_execution_bindings": {
                        migration_request["to_plan"]: target_execution
                    },
                },
            ),
            (
                "shadow",
                shadow,
                outcomes["shadow_recorded"],
                {
                    "store": store,
                    "migration_adapter": None,
                    "shadow_driver": shadow_provider,
                    "target_execution_bindings": {},
                },
            ),
        ):
            commit = _evolution_commit(command, outcome, evolution_id)
            request = {
                "type": "execute_live_evolution",
                "target": target,
                "evolution_id": evolution_id,
                "command": command,
            }
            envelope = {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": request,
                "response": {
                    "type": "live_evolution_executed",
                    "commit": commit,
                },
            }
            with self.subTest(label=label), tempfile.TemporaryDirectory(
                prefix="cymule-python-provider-matrix-"
            ) as directory:
                durable = DurableEngine(
                    store,
                    None,
                    None,
                    _engine_with_envelope(directory, envelope),
                    evolution_id,
                    migration_provider,
                    shadow_provider,
                    target["target_execution_bindings"],
                )
                self.assertEqual(durable.evolve(command), commit)

    def test_python_evolution_command_identities_fail_closed(self) -> None:
        artifact = _artifact("5", "cymule.execution-binding/2")
        commands = [
            {
                "control_version": "cymule.evolution-control/5",
                "command_id": "",
                "operation": "select_occurrence",
                "occurrence_id": "occurrence:test",
                "selection_id": "selection:test",
                "execution_binding": artifact,
            },
            {
                "control_version": "cymule.evolution-control/5",
                "command_id": "command:patch",
                "operation": "apply_patch",
                "patch": {
                    "from_plan": "",
                    "target": {},
                    "operations": [],
                    "evidence": {},
                },
            },
            {
                "control_version": "cymule.evolution-control/5",
                "command_id": "command:rollout",
                "operation": "set_rollout",
                "decision": {
                    "decision_id": "",
                    "fallback_plan": "plan:fallback",
                    "target_plan": "plan:target",
                    "mode": {"mode": "active"},
                },
            },
            {
                "control_version": "cymule.evolution-control/5",
                "command_id": "command:observe",
                "operation": "observe",
                "observation": {
                    "observation_id": "",
                    "decision_id": "decision:test",
                    "occurrence_id": "occurrence:test",
                    "plan_id": _content_id("1"),
                    "outcome": "succeeded",
                    "evidence": _artifact("1", "cymule.evolution-evidence/1"),
                },
            },
            {
                "control_version": "cymule.evolution-control/5",
                "command_id": "command:gate",
                "operation": "apply_gate",
                "gate": {
                    "gate_id": "",
                    "decision_id": "decision:test",
                    "min_target_observations": 1,
                    "max_target_failures": 0,
                    "min_equivalent_shadows": 0,
                    "max_inequivalent_shadows": 0,
                },
                "next_decision_id": "decision:next",
            },
        ]
        for command in commands:
            with self.subTest(operation=command["operation"]):
                with self.assertRaises(EngineError):
                    _validate_live_evolution_command(
                        {
                            "control_version": "cymule.live-evolution-control/6",
                            "command_id": "command:live:identity",
                            "operation": "apply",
                            "template_id": "template:test",
                            "command": command,
                        }
                    )

    def test_python_evolution_plan_fields_require_lowercase_content_ids(
        self,
    ) -> None:
        candidate = FlowBuilder(
            "evolution_plan_fields", {}, {}
        ).finish({"kind": "input"})
        patch = EvolutionControlBuilder.apply_patch(
            "command:plan-fields:patch",
            {
                "from_plan": _content_id("1"),
                "target": candidate,
                "operations": [
                    {
                        "kind": "add",
                        "target": "definition:added",
                        "before": None,
                        "after": "2" * 64,
                    }
                ],
                "evidence": _artifact("3", "cymule.evolution-evidence/1"),
            },
        )
        rollout = EvolutionControlBuilder.set_rollout(
            "command:plan-fields:rollout",
            {
                "decision_id": "decision:plan-fields",
                "fallback_plan": _content_id("4"),
                "target_plan": _content_id("5"),
                "mode": {"mode": "active"},
            },
        )
        observation = EvolutionControlBuilder.observe(
            "command:plan-fields:observation",
            {
                "observation_id": "observation:plan-fields",
                "decision_id": "decision:plan-fields",
                "occurrence_id": "occurrence:plan-fields",
                "plan_id": _content_id("5"),
                "outcome": "succeeded",
                "evidence": _artifact("6", "cymule.evolution-evidence/1"),
            },
        )
        for command in (patch, rollout, observation):
            _validate_evolution_command(command)

        cases = (
            ("patch.from_plan", patch, "patch", "from_plan"),
            (
                "rollout.fallback_plan",
                rollout,
                "decision",
                "fallback_plan",
            ),
            ("rollout.target_plan", rollout, "decision", "target_plan"),
            (
                "observation.plan_id",
                observation,
                "observation",
                "plan_id",
            ),
        )
        for label, command, container, field in cases:
            for invalid in ("plan:legacy", "sha256:" + "A" * 64):
                malformed = copy.deepcopy(command)
                malformed[container][field] = invalid
                with self.subTest(field=label, invalid=invalid), self.assertRaises(
                    EngineError
                ) as rejected:
                    _validate_evolution_command(malformed)
                self.assertEqual(
                    rejected.exception.failure["code"],
                    "invalid_engine_response",
                )

    def test_python_m4_identities_use_a_distinct_unicode_scalar_domain(self) -> None:
        maximum = "🙂" * 256
        binding = _artifact("5", "cymule.execution-binding/2")
        evolution = EvolutionControlBuilder.select_occurrence(
            maximum,
            maximum,
            maximum,
            binding,
        )
        _validate_evolution_command(evolution)
        live = LiveEvolutionControlBuilder.apply(
            maximum,
            maximum,
            evolution,
        )
        _validate_live_evolution_command(live)

        for invalid in ("🙂" * 257, "m4:\u0085forged", "\ud800"):
            with self.subTest(invalid=repr(invalid)):
                invalid_evolution = copy.deepcopy(evolution)
                invalid_evolution["command_id"] = invalid
                with self.assertRaises(EngineError):
                    _validate_evolution_command(invalid_evolution)
                invalid_live = copy.deepcopy(live)
                invalid_live["command_id"] = invalid
                with self.assertRaises(EngineError):
                    _validate_live_evolution_command(invalid_live)

    def test_python_applied_effect_summary_requires_canonical_result(self) -> None:
        fixture_path = os.environ.get("CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE")
        if fixture_path is None:
            self.skipTest("Applied Effect summary conformance is not configured")
        with open(fixture_path, encoding="utf-8") as source:
            fixture = json.load(source)
        command = DurableControlBuilder.run_effect_page(fixture["run_id"], {
            "expected_revision": None, "cursor": None,
            "limit": 1, "max_canonical_bytes": 1024 * 1024,
        })
        for label, state, result, accepted in (
            ("applied canonical null Artifact", "applied", fixture["result"], True),
            ("applied missing Artifact", "applied", None, False),
            ("not applied absence", "not_applied", None, True),
            ("not applied unexpected Artifact", "not_applied", fixture["result"], False),
        ):
            summary = {**fixture, "state": state, "result": result}
            response = {
                "type": "durable_executed",
                "response": {
                    "type": "run_effect_page", "run_id": fixture["run_id"],
                    "page": {
                        "observed_revision": _content_id("5"), "source_root": "6" * 64,
                        "items": [summary], "next_cursor": None,
                    },
                },
            }
            with self.subTest(label=label), tempfile.TemporaryDirectory(
                prefix="cymule-python-effect-summary-"
            ) as directory:
                engine = _engine_with_success(directory, response)
                if accepted:
                    self.assertEqual(
                        engine.execute_durable({"store": directory_store("unused")}, command),
                        response["response"],
                    )
                else:
                    with self.assertRaises(EngineError) as failure:
                        engine.execute_durable({"store": directory_store("unused")}, command)
                    self.assertEqual(failure.exception.failure["code"], "invalid_engine_response")

    def test_python_closes_every_durable_control_4_query_shape(self) -> None:
        revision = _content_id("5")
        source_root = "6" * 64
        run_id = "run:query-v4"
        options = {
            "expected_revision": None,
            "cursor": None,
            "limit": 256,
            "max_canonical_bytes": 1024 * 1024,
        }
        commands = [
            DurableControlBuilder.run_index_page(options),
            DurableControlBuilder.run_current(run_id, None),
            DurableControlBuilder.run_wait_page(run_id, options),
            DurableControlBuilder.run_effect_page(run_id, options),
            DurableControlBuilder.run_occurrence_page(run_id, options),
            DurableControlBuilder.run_attempt_page(run_id, options),
            DurableControlBuilder.run_item(
                {
                    "run_id": run_id,
                    "expected_revision": None,
                    "selector": {"kind": "wait", "wait_id": _content_id("7")},
                    "max_canonical_bytes": 13 * 1024 * 1024,
                }
            ),
        ]
        self.assertEqual(
            [command["type"] for command in commands],
            [
                "run_index_page",
                "run_current",
                "run_wait_page",
                "run_effect_page",
                "run_occurrence_page",
                "run_attempt_page",
                "run_item",
            ],
        )
        for command in commands:
            _validate_durable_command_response(command)
            self.assertEqual(
                command["control_version"], "cymule.durable-control/4"
            )

        current = {
            "run_id": run_id,
            "plan_id": _content_id("2"),
            "execution_binding": _artifact(
                "3", "cymule.execution-binding/2"
            ),
            "continuation_status": "ready",
            "epoch": 0,
            "execution_fence": 0,
            "result": None,
            "execution_status": {"status": "active"},
            "world_settlement": "settled",
        }
        current_response = {
            "type": "run_current",
            "observed_revision": revision,
            "source_root": source_root,
            "current": current,
        }
        _validate_durable_response(current_response)
        absent_run = copy.deepcopy(current_response)
        absent_run["current"] = None
        _validate_durable_response(absent_run)
        missing_current = copy.deepcopy(current_response)
        del missing_current["current"]
        with self.assertRaises(EngineError):
            _validate_durable_response(missing_current)
        missing_result = copy.deepcopy(current_response)
        del missing_result["current"]["result"]
        with self.assertRaises(EngineError):
            _validate_durable_response(missing_result)

        def key_position(canonical_key: str) -> tuple[str, str]:
            def frame(value: bytes) -> bytes:
                return len(value).to_bytes(8, "big") + value

            preimage = b"".join(
                (
                    frame(b"cymule.authenticated-collection-preimage/1"),
                    frame(b"cymule.authenticated-map-key/1"),
                    frame((1).to_bytes(8, "big")),
                    frame(canonical_key.encode("utf-8")),
                )
            )
            return hashlib.sha256(preimage).hexdigest(), canonical_key

        summaries = [
            {
                "run_id": "run:index-a",
                "continuation_status": "ready",
                "execution_status": {"status": "active"},
                "world_settlement": "settled",
            },
            {
                "run_id": "run:index-b",
                "continuation_status": "completed",
                "execution_status": {"status": "completed"},
                "world_settlement": "settled",
            },
        ]
        summaries.sort(key=lambda item: key_position(item["run_id"]))
        terminal_hash, terminal_key = key_position(summaries[-1]["run_id"])
        run_index_response = {
            "type": "run_index_page",
            "page": {
                "observed_revision": revision,
                "source_root": source_root,
                "items": summaries,
                "next_cursor": {
                    "query_kind": "run_index",
                    "run_id": None,
                    "source_revision": revision,
                    "source_root": source_root,
                    "position": {
                        "canonical_key": terminal_key,
                        "key_hash": terminal_hash,
                    },
                },
            },
        }
        _validate_durable_response(run_index_response)
        missing_next_cursor = copy.deepcopy(run_index_response)
        del missing_next_cursor["page"]["next_cursor"]
        with self.assertRaises(EngineError):
            _validate_durable_response(missing_next_cursor)
        forged_position = copy.deepcopy(run_index_response)
        forged_position["page"]["next_cursor"]["position"]["key_hash"] = "0" * 64
        with self.assertRaises(EngineError):
            _validate_durable_response(forged_position)
        reversed_items = copy.deepcopy(run_index_response)
        reversed_items["page"]["items"].reverse()
        with self.assertRaises(EngineError):
            _validate_durable_response(reversed_items)

        wait_page = {
            "type": "run_wait_page",
            "run_id": run_id,
            "page": {
                "observed_revision": revision,
                "source_root": source_root,
                "items": [
                    {
                        "wait_id": _content_id("7"),
                        "run_id": run_id,
                        "state": "pending",
                        "result": None,
                    }
                ],
                "next_cursor": None,
            },
        }
        _validate_durable_response(wait_page)
        missing_wait_result = copy.deepcopy(wait_page)
        del missing_wait_result["page"]["items"][0]["result"]
        with self.assertRaises(EngineError):
            _validate_durable_response(missing_wait_result)

        missing_item = {
            "type": "run_item",
            "run_id": run_id,
            "observed_revision": revision,
            "source_root": source_root,
            "item": None,
        }
        _validate_durable_response(missing_item)
        absent_item_member = copy.deepcopy(missing_item)
        del absent_item_member["item"]
        with self.assertRaises(EngineError):
            _validate_durable_response(absent_item_member)

        legacy_query = {
            "type": "query_run",
            "control_version": "cymule.durable-control/3",
            "query_id": "query:legacy",
            "run_id": run_id,
        }
        with self.assertRaises(EngineError):
            _validate_durable_command_response(legacy_query)

    def test_python_durable_controls_accept_maximum_exact_integer(self) -> None:
        execution = fixture_execution()
        execution["ttl"] = 9_007_199_254_740_991
        command = DurableControlBuilder.takeover_run(
            "run:max-safe-fence", 9_007_199_254_740_991, execution
        )
        self.assertEqual(command["expected_fence"], 9_007_199_254_740_991)
        accepted = fixture_execution()
        accepted["owner"] = "é" * 512
        DurableControlBuilder.resume_run("run:unicode-owner", accepted)
        for owner in ("driver:\u0085forged", "é" * 513):
            rejected = fixture_execution()
            rejected["owner"] = owner
            with self.assertRaises(ValueError):
                DurableControlBuilder.resume_run("run:invalid-owner", rejected)

    def test_python_uses_the_core_unicode_scalar_contract_for_run_ids(self) -> None:
        run_id = "界" * 512
        other_run_id = "🙂" * 512
        _validate_core_identity(run_id, "Run")
        self.assertEqual(
            DurableControlBuilder.run_current(run_id, None)["run_id"],
            run_id,
        )
        self.assertEqual(
            VirtualSchedulingControlBuilder.run_weight(
                "command:unicode-run", run_id, 1
            )["run_id"],
            run_id,
        )
        self.assertEqual(
            sqlite_clock("clock.sqlite", other_run_id, _content_id("1"))[
                "source_id"
            ],
            other_run_id,
        )

        completed = {
            "status": "completed",
            "result": {
                "run_id": run_id,
                "plan_id": _content_id("2"),
                "value": None,
                "projection_digest": "3" * 64,
                "precondition_token": f"pre:0:{_content_id('9')}",
                "effects": [],
            },
        }
        _validate_execution_outcome(completed)

        migration = copy.deepcopy(
            _fixed_live_evolution_outcomes()["migrated"]["receipt"]["request"]
        )
        migration["run_id"] = run_id
        _validate_migration_request(migration)

        restart = copy.deepcopy(
            _fixed_live_evolution_outcomes()["restart_authorized"]["receipt"][
                "request"
            ]
        )
        restart["run_id"] = run_id
        restart["replacement_run"] = other_run_id
        _validate_restart_request(restart)

        claim = {
            "claim_version": "cymule.continuation-execution-claim/1",
            "run_id": run_id,
            "continuation_id": _content_id("4"),
            "owner": other_run_id,
            "continuation_attempt_id": _content_id("7"),
            "fence": 1,
            "plan_id": _content_id("5"),
            "execution_binding_ref": _artifact(
                "6", "cymule.execution-binding/2"
            ),
            "clock_observation_ref": fixture_execution()["clock"],
            "logical_acquired_at": 10,
            "logical_expires_at": 40,
            "logical_ttl": 30,
        }
        _validate_continuation_execution_claim(claim)
        concatenated = copy.deepcopy(claim)
        concatenated["continuation_id"] = f"continuation:{run_id}"
        with self.assertRaises(EngineError):
            _validate_continuation_execution_claim(concatenated)
        for field in ("continuation_attempt_id", "plan_id"):
            malformed = copy.deepcopy(claim)
            malformed[field] = f"{field}:not-content-addressed"
            with self.assertRaises(EngineError):
                _validate_continuation_execution_claim(malformed)

        class CapturingTransport:
            command: dict[str, object] | None = None

            def execute_durable(
                self, target: dict[str, object], command: dict[str, object]
            ) -> dict[str, object]:
                del target
                self.command = copy.deepcopy(command)
                return {
                    "type": "run_current",
                    "observed_revision": _content_id("a"),
                    "source_root": "b" * 64,
                    "current": None,
                }

        transport = CapturingTransport()
        self.assertIsNone(
            DurableEngine(
                directory_store("unused"), None, None, transport  # type: ignore[arg-type]
            ).run_current(run_id, None)["current"]
        )
        self.assertEqual(
            transport.command,
            {
                "type": "run_current",
                "control_version": "cymule.durable-control/4",
                "run_id": run_id,
                "expected_revision": None,
            },
        )

        for invalid in ("界" * 513, "run:\u0085forged", "\ud800"):
            with self.subTest(invalid=repr(invalid)):
                with self.assertRaises(EngineError):
                    _validate_core_identity(invalid, "Run")
                with self.assertRaises(ValueError):
                    DurableControlBuilder.run_current(invalid, None)
                invalid_completed = copy.deepcopy(completed)
                invalid_completed["result"]["run_id"] = invalid
                with self.assertRaises(EngineError):
                    _validate_execution_outcome(invalid_completed)

    def test_python_high_level_durable_engine_closes_custom_transport(
        self,
    ) -> None:
        store = directory_store("unused")
        plugin = process_target("/bin/true")
        clock = sqlite_clock(
            "clock.sqlite",
            "clock:cross-language",
            _content_id("2"),
        )

        class NoCallTransport:
            def __init__(self) -> None:
                self.calls = 0

            def execute_durable(
                self,
                target: dict[str, object],
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, command
                self.calls += 1
                raise AssertionError("invalid durable request reached transport")

            def observe_clock(
                self, target: dict[str, object], run_id: str
            ) -> dict[str, object]:
                del target, run_id
                self.calls += 1
                raise AssertionError("invalid Clock request reached transport")

        def assert_local_validation(
            engine: DurableEngine,
            transport: NoCallTransport,
            invoke: Callable[[DurableEngine], object],
        ) -> None:
            with self.assertRaises(EngineError) as rejected:
                invoke(engine)
            self.assertEqual(rejected.exception.failure["category"], "validation")
            self.assertEqual(
                rejected.exception.failure["retry_disposition"],
                "correct_and_retry",
            )
            self.assertEqual(transport.calls, 0)

        validation_cases = (
            (
                "invalid command",
                plugin,
                clock,
                lambda engine: engine.run_current("", None),
            ),
            (
                "missing executor",
                None,
                clock,
                lambda engine: engine.resume(
                    "run:missing-executor", fixture_execution()
                ),
            ),
            (
                "missing Clock",
                plugin,
                None,
                lambda engine: engine.resume(
                    "run:missing-clock", fixture_execution()
                ),
            ),
            (
                "missing Clock observation target",
                plugin,
                None,
                lambda engine: engine.observe_clock("run:missing-clock"),
            ),
        )
        for label, selected_plugin, selected_clock, invoke in validation_cases:
            transport = NoCallTransport()
            engine = DurableEngine(
                store,
                selected_plugin,
                selected_clock,
                transport,  # type: ignore[arg-type]
            )
            with self.subTest(case=label):
                assert_local_validation(engine, transport, invoke)

        invalid_plugin = copy.deepcopy(plugin)
        invalid_plugin["process"]["executable"] = "relative-executable"
        invalid_clock = copy.deepcopy(clock)
        invalid_clock["source_generation"] = "sha256:" + "A" * 64
        invalid_store_transport = NoCallTransport()
        invalid_executor_transport = NoCallTransport()
        invalid_clock_transport = NoCallTransport()
        configured_target_cases = (
            (
                "invalid Store",
                invalid_store_transport,
                DurableEngine(
                    {"provider": "", "location": "unused"},
                    None,
                    None,
                    invalid_store_transport,  # type: ignore[arg-type]
                ),
                lambda engine: engine.run_current("run:invalid-store", None),
            ),
            (
                "invalid executor",
                invalid_executor_transport,
                DurableEngine(
                    store,
                    invalid_plugin,
                    clock,
                    invalid_executor_transport,  # type: ignore[arg-type]
                ),
                lambda engine: engine.resume(
                    "run:invalid-executor", fixture_execution()
                ),
            ),
            (
                "invalid Clock",
                invalid_clock_transport,
                DurableEngine(
                    store,
                    plugin,
                    invalid_clock,
                    invalid_clock_transport,  # type: ignore[arg-type]
                ),
                lambda engine: engine.observe_clock("run:invalid-clock"),
            ),
        )
        for label, transport, engine, invoke in configured_target_cases:
            with self.subTest(case=label):
                assert_local_validation(engine, transport, invoke)

        class ReturningTransport:
            def __init__(
                self,
                *,
                durable_response: dict[str, object] | None = None,
                evolution_commit: dict[str, object] | None = None,
            ) -> None:
                self.durable_response = durable_response
                self.evolution_commit = evolution_commit

            def execute_durable(
                self,
                target: dict[str, object],
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, command
                assert self.durable_response is not None
                return copy.deepcopy(self.durable_response)

            def execute_live_evolution(
                self,
                target: dict[str, object],
                evolution_id: str,
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, evolution_id, command
                assert self.evolution_commit is not None
                return copy.deepcopy(self.evolution_commit)

        forged_current = {
            "type": "run_current",
            "observed_revision": _content_id("7"),
            "source_root": "8" * 64,
            "current": {
                "run_id": "run:forged",
                "plan_id": _content_id("9"),
                "execution_binding": _artifact(
                    "a", "cymule.execution-binding/2"
                ),
                "continuation_status": "ready",
                "epoch": 0,
                "execution_fence": 0,
                "result": None,
                "execution_status": {"status": "active"},
                "world_settlement": "settled",
            },
        }
        read_transport = ReturningTransport(durable_response=forged_current)
        with self.assertRaises(EngineError) as forged_read:
            DurableEngine(
                store,
                None,
                None,
                read_transport,  # type: ignore[arg-type]
            ).run_current("run:expected", None)
        self.assertEqual(
            forged_read.exception.failure["category"], "transport_failure"
        )
        self.assertEqual(
            forged_read.exception.failure["code"], "invalid_engine_response"
        )
        self.assertNotIn("retry_disposition", forged_read.exception.failure)

        forged_boundary = {
            "type": "run_boundary",
            "boundary": {
                "status": "completed",
                "result": {
                    "run_id": "run:forged",
                    "plan_id": _content_id("b"),
                    "value": None,
                    "projection_digest": "c" * 64,
                    "precondition_token": f"pre:0:{_content_id('d')}",
                    "effects": [],
                },
            },
        }
        mutation_transport = ReturningTransport(
            durable_response=forged_boundary
        )
        with self.assertRaises(EngineError) as forged_mutation:
            DurableEngine(
                store,
                plugin,
                clock,
                mutation_transport,  # type: ignore[arg-type]
            ).resume("run:expected", fixture_execution())
        self.assertEqual(
            forged_mutation.exception.failure["category"],
            "unknown_world_outcome",
        )
        self.assertEqual(
            forged_mutation.exception.failure["retry_disposition"],
            "reconcile",
        )

        outcomes = _fixed_live_evolution_outcomes()
        published = copy.deepcopy(outcomes["definition_published"])
        revision = published["revision"]
        evolution_command = LiveEvolutionControlBuilder.publish_definition(
            "command:custom-transport-commit",
            revision["logical_ref"],
            revision["definition"],
            revision["references"],
        )
        forged_commit = _evolution_commit(evolution_command, published)
        forged_commit["receipt"]["command"]["command"]["command_id"] = (
            "command:forged"
        )
        commit_transport = ReturningTransport(
            evolution_commit=forged_commit
        )
        with self.assertRaises(EngineError) as forged_commit_failure:
            DurableEngine(
                store,
                None,
                None,
                commit_transport,  # type: ignore[arg-type]
            ).evolve(evolution_command)
        self.assertEqual(
            forged_commit_failure.exception.failure["category"],
            "unknown_world_outcome",
        )
        self.assertEqual(
            forged_commit_failure.exception.failure["retry_disposition"],
            "reconcile",
        )

        class RaisingTransport:
            def execute_durable(
                self,
                target: dict[str, object],
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, command
                raise RuntimeError("ordinary custom transport failure")

            def execute_live_evolution(
                self,
                target: dict[str, object],
                evolution_id: str,
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, evolution_id, command
                raise RuntimeError("ordinary custom transport failure")

        exception_cases = (
            (
                "read",
                DurableEngine(
                    store,
                    None,
                    None,
                    RaisingTransport(),  # type: ignore[arg-type]
                ),
                lambda engine: engine.run_current("run:transport-error", None),
                "transport_failure",
                None,
            ),
            (
                "mutation",
                DurableEngine(
                    store,
                    plugin,
                    clock,
                    RaisingTransport(),  # type: ignore[arg-type]
                ),
                lambda engine: engine.resume(
                    "run:transport-error", fixture_execution()
                ),
                "unknown_world_outcome",
                "reconcile",
            ),
            (
                "evolution commit",
                DurableEngine(
                    store,
                    None,
                    None,
                    RaisingTransport(),  # type: ignore[arg-type]
                ),
                lambda engine: engine.evolve(evolution_command),
                "unknown_world_outcome",
                "reconcile",
            ),
        )
        for label, engine, invoke, category, retry in exception_cases:
            with self.subTest(exception=label), self.assertRaises(
                EngineError
            ) as structured:
                invoke(engine)
            self.assertEqual(structured.exception.failure["category"], category)
            self.assertEqual(
                structured.exception.failure["code"], "engine_transport_failed"
            )
            if retry is None:
                self.assertNotIn(
                    "retry_disposition", structured.exception.failure
                )
            else:
                self.assertEqual(
                    structured.exception.failure["retry_disposition"], retry
                )

        class EngineFailureTransport:
            def __init__(self, failure: dict[str, object]) -> None:
                self.failure = failure

            def execute_durable(
                self,
                target: dict[str, object],
                command: dict[str, object],
            ) -> dict[str, object]:
                del target, command
                raise EngineError(copy.deepcopy(self.failure))

        malformed_failure = {
            "category": "unknown_world_outcome",
            "phase": "transport",
            "code": "malformed_recovery_matrix",
            "message": "unknown outcomes cannot be retried as the same request",
            "retry_disposition": "retry_same_request",
        }
        malformed_cases = (
            (
                "read",
                DurableEngine(
                    store,
                    None,
                    None,
                    EngineFailureTransport(
                        malformed_failure
                    ),  # type: ignore[arg-type]
                ),
                lambda engine: engine.run_current("run:malformed-failure", None),
                "transport_failure",
                None,
            ),
            (
                "mutation",
                DurableEngine(
                    store,
                    plugin,
                    clock,
                    EngineFailureTransport(
                        malformed_failure
                    ),  # type: ignore[arg-type]
                ),
                lambda engine: engine.resume(
                    "run:malformed-failure", fixture_execution()
                ),
                "unknown_world_outcome",
                "reconcile",
            ),
        )
        for label, engine, invoke, category, retry in malformed_cases:
            with self.subTest(malformed_failure=label), self.assertRaises(
                EngineError
            ) as rejected:
                invoke(engine)
            self.assertEqual(rejected.exception.failure["category"], category)
            self.assertEqual(
                rejected.exception.failure["code"], "invalid_engine_response"
            )
            if retry is None:
                self.assertNotIn(
                    "retry_disposition", rejected.exception.failure
                )
            else:
                self.assertEqual(
                    rejected.exception.failure["retry_disposition"], retry
                )

        valid_failure = {
            "category": "validation",
            "phase": "validate_request",
            "code": "custom_transport_validation",
            "message": "custom transport rejected the request",
            "retry_disposition": "correct_and_retry",
        }
        valid_failure_cases = (
            (
                DurableEngine(
                    store,
                    None,
                    None,
                    EngineFailureTransport(
                        valid_failure
                    ),  # type: ignore[arg-type]
                ),
                lambda engine: engine.run_current("run:valid-failure", None),
            ),
            (
                DurableEngine(
                    store,
                    plugin,
                    clock,
                    EngineFailureTransport(
                        valid_failure
                    ),  # type: ignore[arg-type]
                ),
                lambda engine: engine.resume(
                    "run:valid-failure", fixture_execution()
                ),
            ),
        )
        for engine, invoke in valid_failure_cases:
            with self.assertRaises(EngineError) as preserved:
                invoke(engine)
            self.assertEqual(preserved.exception.failure, valid_failure)

    def test_python_classifies_mutating_response_loss_as_unknown(self) -> None:
        engine = CliEngine(
            os.path.join(os.getcwd(), "tests", "fixtures", "response-loss-engine")
        )
        resume = DurableControlBuilder.resume_run(
            "run:response-loss", fixture_execution()
        )
        with self.assertRaises(EngineError) as failure:
            engine.execute_durable(
                durable_target_for(
                    resume, directory_store("/tmp/cymule-response-loss")
                ),
                resume,
            )
        self.assertEqual(failure.exception.failure["category"], "unknown_world_outcome")
        self.assertEqual(failure.exception.failure["retry_disposition"], "reconcile")
        with self.assertRaises(EngineError) as clock_failure:
            engine.observe_clock(
                sqlite_clock(
                    "/tmp/cymule-response-loss-clock",
                    "clock:response-loss",
                    "sha256:" + "4" * 64,
                ),
                "run:response-loss",
            )
        self.assertEqual(clock_failure.exception.failure["category"], "unknown_world_outcome")
        self.assertEqual(clock_failure.exception.failure["retry_disposition"], "reconcile")

    def test_python_cancellation_callback_failure_reaps_the_engine_group(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-cancellation-callback-"
        ) as directory:
            executable = os.path.join(directory, "callback-engine")
            started = os.path.join(directory, "started")
            late_effect = os.path.join(directory, "late-effect")
            with open(executable, "w", encoding="utf-8") as script:
                script.write(
                    "#!/bin/sh\n"
                    f"( sleep 0.4; printf leaked > {late_effect!r} ) &\n"
                    f"printf started > {started!r}\n"
                    "while :; do sleep 1; done\n"
                )
            os.chmod(executable, 0o700)

            def failed_callback() -> bool:
                if os.path.exists(started):
                    raise RuntimeError("private callback failure")
                return False

            engine = CliEngine(
                executable,
                timeout_seconds=5,
                cancelled=failed_callback,
            )
            with self.assertRaises(EngineError) as rejected:
                engine.observe_clock(
                    sqlite_clock(
                        os.path.join(directory, "clock.sqlite"),
                        "clock:callback-failure",
                        _content_id("4"),
                    ),
                    "run:callback-failure",
                )
            self.assertEqual(
                rejected.exception.failure["category"], "unknown_world_outcome"
            )
            self.assertEqual(
                rejected.exception.failure["code"],
                "engine_cancellation_callback_failed",
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"], "reconcile"
            )
            self.assertNotIn("private callback failure", str(rejected.exception))
            time.sleep(0.6)
            self.assertFalse(os.path.exists(late_effect))

    def test_engine_json_rejects_duplicate_object_members(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate JSON object member"):
            json.loads(
                '{"response":{"type":"verified","type":"executed"}}',
                object_pairs_hook=_unique_json_object,
            )

    def test_python_normalizes_mathematical_integer_tokens(self) -> None:
        for lexeme in ("1.0", "1e0"):
            decoded = _strict_json_loads(
                '{"gate":{"min_target_observations":' + lexeme + "}}"
            )
            self.assertEqual(decoded["gate"]["min_target_observations"], 1)
            self.assertIsInstance(decoded["gate"]["min_target_observations"], int)
        fractional = _strict_json_loads(
            '{"gate":{"min_target_observations":1.5},"value":1.5}'
        )
        self.assertEqual(fractional["value"], 1.5)
        self.assertIsInstance(fractional["value"], float)
        with self.assertRaisesRegex(ValueError, "not distinguishable from an integer"):
            _strict_json_loads('{"value":9007199254740991.1}')

        engine_path = os.environ.get("CYMULE_BIN")
        if engine_path is None:
            self.skipTest("CYMULE_BIN is required for request-snapshot normalization")
        with open(
            os.path.join("tests", "fixtures", "cross-language-plan.json"),
            encoding="utf-8",
        ) as fixture:
            candidate = json.load(fixture)
        candidate["definitions"][0]["body"]["steps"][2]["body"]["result"][
            "value"
        ] = 1.0
        sealed = CliEngine(engine_path).seal(candidate)
        normalized = sealed["candidate"]["definitions"][0]["body"]["steps"][2][
            "body"
        ]["result"]["value"]
        self.assertEqual(normalized, 1)
        self.assertIsInstance(normalized, int)

    def test_engine_success_and_nested_unions_are_closed(self) -> None:
        with self.assertRaises(EngineError) as legacy:
            _validate_engine_envelope(
                {
                    "engine_protocol": "cymule.engine/4",
                    "outcome": "success",
                    "request": {},
                    "response": {"type": "verified"},
                }
            )
        self.assertEqual(
            legacy.exception.failure["code"], "unsupported_engine_protocol"
        )
        with self.assertRaises(EngineError):
            _validate_engine_envelope(
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "failure",
                    "request": {},
                    "error": {},
                }
            )

        invalid_responses = [
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {"type": "sealed", "plan": None},
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {"type": "unknown"},
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {
                    "type": "execution_boundary",
                    "execution": {"status": "completed", "result": {}, "suspension": {}},
                },
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {
                    "type": "verified_evolution_command",
                    "command": {
                        "control_version": "cymule.evolution-control/5",
                        "command_id": "command:test",
                        "operation": "future_operation",
                    },
                },
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
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
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {
                    "type": "verified_evolution_command",
                    "command": {
                        "control_version": "cymule.evolution-control/5",
                        "command_id": "command:test",
                        "operation": "migrate",
                        "request": {"unexpected": True},
                    },
                },
            },
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "success",
                "request": {},
                "response": {
                    "type": "verified_live_evolution_command",
                    "command": {
                        "control_version": "cymule.live-evolution-control/6",
                        "command_id": "command:unsafe-safe-point",
                        "operation": "apply",
                        "template_id": "template:test",
                        "command": {
                            "control_version": "cymule.evolution-control/5",
                            "command_id": "command:select",
                            "operation": "select_occurrence",
                            "occurrence_id": "occurrence:test",
                        },
                        "safe_point": {"retired": True},
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
                    "plan_id": _content_id("1"),
                    "intent_ids": [_content_id("2")],
                },
            },
            {
                "status": "reconciliation_required",
                "reconciliation": {
                    "run_id": "run:test",
                    "plan_id": _content_id("1"),
                    "intent_id": _content_id("2"),
                },
            },
        ):
            _validate_engine_envelope(
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {},
                    "response": {"type": "execution_boundary", "execution": execution},
                }
            )

    def test_python_real_client_classifies_unsupported_engine_protocol(self) -> None:
        executable = os.environ.get("CYMULE_UNSUPPORTED_ENGINE")
        if executable is None:
            self.skipTest("unsupported Engine protocol fixture is not configured")
        engine = CliEngine(executable)
        candidate = FlowBuilder("unsupported_protocol", {}, {}).finish(
            {"kind": "input"}
        )
        with self.assertRaises(EngineError) as read:
            engine.seal(candidate)
        self.assertEqual(read.exception.failure["category"], "contract_violation")
        self.assertEqual(
            read.exception.failure["code"], "unsupported_engine_protocol"
        )
        self.assertEqual(read.exception.failure["retry_disposition"], "never")

        with self.assertRaises(EngineError) as mutation:
            engine.observe_clock(
                sqlite_clock(
                    "/tmp/cymule-python-unsupported-clock.sqlite",
                    "clock:python:unsupported",
                    _content_id("4"),
                ),
                "run:python:unsupported",
            )
        self.assertEqual(
            mutation.exception.failure["category"], "unknown_world_outcome"
        )
        self.assertEqual(
            mutation.exception.failure["code"], "unsupported_engine_protocol"
        )
        self.assertEqual(
            mutation.exception.failure["retry_disposition"], "reconcile"
        )

    def test_python_rejects_unsafe_json_and_preserves_pre_cancellation(self) -> None:
        candidate = FlowBuilder("transport", {}, {}).finish({"kind": "input"})
        with self.assertRaises(EngineError) as cancelled:
            CliEngine(cancelled=lambda: True).seal(candidate)
        self.assertEqual(cancelled.exception.failure["category"], "cancelled")
        self.assertEqual(
            cancelled.exception.failure["retry_disposition"], "never"
        )
        plan = {"plan_id": _content_id("1"), "candidate": candidate}
        with self.assertRaises(EngineError) as mutating_cancelled:
            CliEngine(cancelled=lambda: True).run(
                plan, None, process_target("/bin/true"), "run:pre-cancelled"
            )
        self.assertEqual(
            mutating_cancelled.exception.failure["category"], "cancelled"
        )
        self.assertEqual(
            mutating_cancelled.exception.failure["retry_disposition"], "never"
        )
        candidate["name"] = float("nan")
        with self.assertRaises(EngineError) as unsafe:
            CliEngine("missing-engine").seal(candidate)
        self.assertEqual(unsafe.exception.failure["category"], "validation")
        self.assertEqual(unsafe.exception.failure["code"], "invalid_engine_request")
        self.assertEqual(
            unsafe.exception.failure["retry_disposition"], "correct_and_retry"
        )
        cyclic: dict[str, object] = {}
        cyclic["self"] = cyclic
        cyclic_candidate = FlowBuilder("cyclic", {}, {}).finish({"kind": "input"})
        cyclic_candidate["metadata"] = cyclic
        with self.assertRaises(EngineError) as rejected_cycle:
            CliEngine("missing-engine").seal(cyclic_candidate)
        self.assertEqual(
            rejected_cycle.exception.failure["category"], "validation"
        )

        with tempfile.TemporaryDirectory(
            prefix="cymule-python-process-preflight-"
        ) as directory:
            preflight_candidate = FlowBuilder("process_preflight", {}, {}).finish(
                {"kind": "input"}
            )
            preflight_plan = {
                "plan_id": _content_id("a"),
                "candidate": preflight_candidate,
            }
            executable = os.path.join(directory, "must-not-start")
            started = executable + ".started"
            with open(executable, "w", encoding="utf-8") as script:
                script.write(f'#!/bin/sh\n/bin/touch "{started}"\nexit 1\n')
            os.chmod(executable, 0o700)
            valid_target = process_target("/bin/true")
            maximum_process = copy.deepcopy(valid_target["process"])
            maximum_process["arguments"] = [""] * 4096
            maximum_process["environment"] = {
                f"ENTRY_{index}": "" for index in range(4096)
            }
            maximum_process["runtime_closure"] = {
                f"runtime-{index}": "sha256:" + "a" * 64
                for index in range(4096)
            }
            process_plugin(maximum_process)
            for field, overflow in [
                ("arguments", [""] * 4097),
                ("environment", {f"ENTRY_{index}": "" for index in range(4097)}),
                (
                    "runtime_closure",
                    {
                        f"runtime-{index}": "sha256:" + "a" * 64
                        for index in range(4097)
                    },
                ),
            ]:
                overflow_process = copy.deepcopy(valid_target["process"])
                overflow_process[field] = overflow
                with self.assertRaises(ValueError):
                    process_plugin(overflow_process)
            missing_working_directory = copy.deepcopy(valid_target)
            missing_working_directory["process"].pop("working_directory")
            invalid_targets = [
                "/bin/true",
                {**valid_target, "location": "/bin/true"},
                missing_working_directory,
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "executable": "relative-plugin",
                    },
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "runtime_closure": {},
                    },
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "runtime_closure": {"host-abi": "unix:darwin:arm64"},
                    },
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "runtime_closure": {"runtime": "sha256:" + "A" * 64},
                    },
                },
                {
                    **valid_target,
                    "process": {**valid_target["process"], "timeout_ms": 0},
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "message_limit": 8 * 1024 * 1024 - 1,
                    },
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "message_limit": 8 * 1024 * 1024 + 1,
                    },
                },
                {
                    **valid_target,
                    "process": {
                        **valid_target["process"],
                        "message_limit": 64 * 1024 * 1024 + 1,
                    },
                },
            ]
            for invalid_target in invalid_targets:
                with self.subTest(target=invalid_target), self.assertRaises(
                    EngineError
                ) as invalid:
                    CliEngine(executable).run(
                        preflight_plan,
                        None,
                        invalid_target,
                        "run:process-preflight",
                    )
                self.assertEqual(
                    invalid.exception.failure["code"], "invalid_plugin_target"
                )
                self.assertFalse(os.path.exists(started))

            resume = DurableControlBuilder.resume_run(
                "run:target-preflight", fixture_execution()
            )
            with self.assertRaises(EngineError):
                CliEngine(executable).execute_durable(
                    {"store": directory_store("unused")}, resume
                )
            self.assertFalse(os.path.exists(started))

            artifact = _artifact("b", "test/input")
            shadow = LiveEvolutionControlBuilder.apply(
                "command:shadow-preflight",
                "template:shadow-preflight",
                EvolutionControlBuilder.shadow(
                    "command:shadow-child",
                    {
                        "comparison_id": "comparison:shadow-preflight",
                        "decision_id": "decision:shadow-preflight",
                        "subject": "run:shadow-preflight",
                        "primary_plan": _content_id("c"),
                        "shadow_plan": _content_id("d"),
                        "driver_id": "driver:shadow-preflight",
                        "driver_revision": _content_id("e"),
                        "input": artifact,
                        "comparison_policy": "policy:shadow-preflight",
                    },
                ),
            )
            for target in (
                {
                    "store": directory_store("unused"),
                    "migration_adapter": None,
                    "shadow_driver": {
                        "driver_id": "driver:shadow-preflight",
                        "driver_revision": _content_id("e"),
                        "process": valid_target,
                    },
                    "target_execution_bindings": {},
                },
            ):
                with self.assertRaises(EngineError):
                    CliEngine(executable).execute_live_evolution(
                        target, "journal:shadow-preflight", shadow
                    )
                self.assertFalse(os.path.exists(started))

    def test_python_classifies_interruptions_by_process_start_and_mutation(self) -> None:
        candidate = FlowBuilder("interruption", {}, {}).finish({"kind": "input"})
        plan = {"plan_id": _content_id("1"), "candidate": candidate}
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-interruption-"
        ) as directory:
            executable = os.path.join(directory, "engine")
            with open(executable, "w", encoding="utf-8") as script:
                script.write(
                    """#!/usr/bin/env python3
import sys
import time

sys.stdin.buffer.read()
time.sleep(10)
"""
                )
            os.chmod(executable, 0o700)

            cancelled = threading.Event()
            timer = threading.Timer(0.05, cancelled.set)
            timer.start()
            try:
                with self.assertRaises(EngineError) as post_start_cancelled:
                    CliEngine(
                        executable,
                        timeout_seconds=5,
                        cancelled=cancelled.is_set,
                    ).run(
                        plan,
                        None,
                        process_target("/bin/true"),
                        "run:post-start-cancel",
                    )
            finally:
                timer.cancel()
            self.assertEqual(
                post_start_cancelled.exception.failure["category"],
                "unknown_world_outcome",
            )
            self.assertEqual(
                post_start_cancelled.exception.failure["retry_disposition"],
                "reconcile",
            )

            with self.assertRaises(EngineError) as mutating_timeout:
                CliEngine(executable, timeout_seconds=0.05).run(
                    plan,
                    None,
                    process_target("/bin/true"),
                    "run:post-start-timeout",
                )
            self.assertEqual(
                mutating_timeout.exception.failure["category"],
                "unknown_world_outcome",
            )
            self.assertEqual(
                mutating_timeout.exception.failure["retry_disposition"],
                "reconcile",
            )

            with self.assertRaises(EngineError) as read_timeout:
                CliEngine(executable, timeout_seconds=0.05).seal(candidate)
            self.assertEqual(
                read_timeout.exception.failure["category"], "timed_out"
            )
            self.assertEqual(
                read_timeout.exception.failure["retry_disposition"],
                "retry_same_request",
            )

            remote_timeout = {
                "category": "timed_out",
                "phase": "execute_durable",
                "code": "durable_attempt_timed_out",
                "message": "the persisted Running Attempt timed out",
                "retry_disposition": "refresh_and_retry",
            }
            with self.assertRaises(EngineError) as authoritative_timeout:
                resume = DurableControlBuilder.resume_run(
                    "run:remote-timeout", fixture_execution()
                )
                _engine_with_envelope(
                    directory,
                    {
                        "engine_protocol": "cymule.engine/5",
                        "outcome": "failure",
                        "error": remote_timeout,
                    },
                ).execute_durable(
                    durable_target_for(resume),
                    resume,
                )
            self.assertEqual(
                authoritative_timeout.exception.failure, remote_timeout
            )

    def test_python_stream_limits_and_descendant_pipe_deadline(self) -> None:
        candidate = FlowBuilder("stream_limits", {}, {}).finish({"kind": "input"})
        with tempfile.TemporaryDirectory(
            prefix="cymule-python-output-limit-"
        ) as directory:
            for stream in ("stdout", "stderr"):
                executable = os.path.join(directory, f"engine-{stream}")
                with open(executable, "w", encoding="utf-8") as script:
                    script.write(
                        """#!/usr/bin/env python3
import os
import sys

sys.stdin.buffer.read()
fd = 1 if sys.argv[0].endswith("stdout") else 2
remaining = 16 * 1024 * 1024 + 1
chunk = b"x" * (64 * 1024)
while remaining:
    part = chunk[:remaining]
    os.write(fd, part)
    remaining -= len(part)
"""
                    )
                os.chmod(executable, 0o700)
                with self.subTest(stream=stream), self.assertRaises(
                    EngineError
                ) as rejected:
                    CliEngine(executable, timeout_seconds=5).seal(candidate)
                self.assertEqual(
                    rejected.exception.failure["category"], "transport_failure"
                )
                self.assertEqual(
                    rejected.exception.failure["code"],
                    "engine_output_limit_exceeded",
                )
                if stream == "stderr":
                    plan = {"plan_id": _content_id("1"), "candidate": candidate}
                    with self.assertRaises(EngineError) as mutating:
                        CliEngine(executable, timeout_seconds=5).run(
                            plan,
                            None,
                            process_target("/bin/true"),
                            "run:stderr-overflow",
                        )
                    self.assertEqual(
                        mutating.exception.failure["category"],
                        "unknown_world_outcome",
                    )
                    self.assertEqual(
                        mutating.exception.failure["retry_disposition"],
                        "reconcile",
                    )

            descendant = os.path.join(directory, "engine-descendant")
            with open(descendant, "w", encoding="utf-8") as script:
                script.write(
                    """#!/usr/bin/env python3
import os
import sys
import time

sys.stdin.buffer.read()
if os.fork() == 0:
    time.sleep(10)
    os._exit(0)
os._exit(0)
"""
                )
            os.chmod(descendant, 0o700)
            started = time.monotonic()
            with self.assertRaises(EngineError) as timed_out:
                CliEngine(descendant, timeout_seconds=0.1).seal(candidate)
            elapsed = time.monotonic() - started
            self.assertEqual(timed_out.exception.failure["category"], "timed_out")
            self.assertEqual(
                timed_out.exception.failure["retry_disposition"],
                "retry_same_request",
            )
            self.assertLess(elapsed, 2)

            escaping = os.path.join(directory, "engine-closed-pipe-descendant")
            ready = os.path.join(directory, "closed-pipe.ready")
            marker = os.path.join(directory, "closed-pipe.late-marker")
            with open(escaping, "w", encoding="utf-8") as script:
                script.write(
                    f'''#!/usr/bin/env python3
import os
import signal
import sys
import time

sys.stdin.buffer.read()
if os.fork() == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with open({ready!r}, "w", encoding="utf-8") as ready_file:
        ready_file.write("ready")
    for descriptor in (0, 1, 2):
        try:
            os.close(descriptor)
        except OSError:
            pass
    time.sleep(0.45)
    with open({marker!r}, "w", encoding="utf-8") as marker_file:
        marker_file.write("late")
    time.sleep(10)
    os._exit(0)
while not os.path.exists({ready!r}):
    time.sleep(0.005)
time.sleep(30)
'''
                )
            os.chmod(escaping, 0o700)
            with self.assertRaises(EngineError) as killed:
                CliEngine(escaping, timeout_seconds=0.2).seal(candidate)
            self.assertEqual(killed.exception.failure["category"], "timed_out")
            time.sleep(0.6)
            self.assertFalse(
                os.path.exists(marker),
                "a closed-pipe descendant executed after the SDK deadline",
            )

            invalid_timeout_engine = os.path.join(directory, "invalid-timeout-engine")
            started = invalid_timeout_engine + ".started"
            with open(invalid_timeout_engine, "w", encoding="utf-8") as script:
                script.write(f'#!/bin/sh\n/bin/touch "{started}"\nexit 1\n')
            os.chmod(invalid_timeout_engine, 0o700)
            for invalid_timeout in (float("nan"), float("inf"), 0, -1):
                with self.subTest(timeout=invalid_timeout), self.assertRaises(
                    EngineError
                ) as invalid:
                    CliEngine(
                        invalid_timeout_engine,
                        timeout_seconds=invalid_timeout,
                    ).seal(candidate)
                self.assertEqual(
                    invalid.exception.failure["code"], "invalid_engine_timeout"
                )
                self.assertFalse(os.path.exists(started))

    def test_python_engine_request_limit_counts_the_complete_utf8_envelope(
        self,
    ) -> None:
        limit = 64 * 1024 * 1024

        def encoded_size(padding: str) -> int:
            return len(
                json.dumps(
                    {
                        "engine_protocol": "cymule.engine/5",
                        "request": {
                            "type": "seal",
                            "candidate": {"padding": padding},
                        },
                    },
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )

        empty_size = encoded_size("")
        unicode_unit_size = encoded_size("🧪") - empty_size
        remaining = limit - empty_size
        padding = "🧪" * (remaining // unicode_unit_size) + "x" * (
            remaining % unicode_unit_size
        )
        self.assertGreater(unicode_unit_size, 1)
        self.assertEqual(encoded_size(padding), limit)

        failure = {
            "category": "validation",
            "phase": "validate_request",
            "code": "synthetic_failure",
            "message": "synthetic Engine failure",
            "retry_disposition": "correct_and_retry",
        }
        envelope = json.dumps(
            {
                "engine_protocol": "cymule.engine/5",
                "outcome": "failure",
                "error": failure,
            },
            separators=(",", ":"),
        )

        def write_nonreading_engine(executable: str, marker: str) -> None:
            with open(executable, "w", encoding="utf-8") as script:
                script.write(
                    f'''#!/usr/bin/env python3
from pathlib import Path
import sys

Path({marker!r}).write_text("started", encoding="utf-8")
sys.stdout.write({envelope!r})
'''
                )
            os.chmod(executable, 0o700)

        with tempfile.TemporaryDirectory(
            prefix="cymule-python-request-limit-"
        ) as directory:
            exact_engine = os.path.join(directory, "engine-exact")
            exact_marker = exact_engine + ".started"
            write_nonreading_engine(exact_engine, exact_marker)
            with self.assertRaises(EngineError) as remote_failure:
                CliEngine(exact_engine, timeout_seconds=10).seal(
                    {"padding": padding}
                )
            self.assertEqual(remote_failure.exception.failure, failure)
            self.assertTrue(os.path.exists(exact_marker))

            oversized_engine = os.path.join(directory, "engine-oversized")
            oversized_marker = oversized_engine + ".started"
            write_nonreading_engine(oversized_engine, oversized_marker)
            with self.assertRaises(EngineError) as oversized:
                CliEngine(oversized_engine, timeout_seconds=10).seal(
                    {"padding": padding + "x"}
                )
            self.assertEqual(
                oversized.exception.failure["category"], "validation"
            )
            self.assertEqual(
                oversized.exception.failure["code"],
                "engine_request_too_large",
            )
            self.assertEqual(
                oversized.exception.failure["retry_disposition"],
                "correct_and_retry",
            )
            self.assertFalse(os.path.exists(oversized_marker))

            early_candidate = FlowBuilder(
                "early_close_success", {}, {}
            ).finish({"kind": "input"})
            early_candidate["metadata"] = {
                "padding": "x" * (4 * 1024 * 1024)
            }
            plan = {
                "plan_id": _content_id("1"),
                "candidate": early_candidate,
            }

            def write_nonreading_success_engine(
                executable: str,
                request: dict[str, object],
                response: dict[str, object],
            ) -> None:
                success = json.dumps(
                    {
                        "engine_protocol": "cymule.engine/5",
                        "outcome": "success",
                        "request": request,
                        "response": response,
                    },
                    separators=(",", ":"),
                )
                with open(executable, "w", encoding="utf-8") as script:
                    script.write(
                        f'''#!/usr/bin/env python3
import sys

sys.stdout.write({success!r})
'''
                    )
                os.chmod(executable, 0o700)

            forged_read_request = {
                "type": "seal",
                "candidate": early_candidate,
            }
            forged_read_engine = os.path.join(
                directory, "engine-early-success-read"
            )
            write_nonreading_success_engine(
                forged_read_engine,
                forged_read_request,
                {"type": "sealed", "plan": plan},
            )
            with self.assertRaises(EngineError) as forged_read:
                CliEngine(forged_read_engine, timeout_seconds=10).seal(
                    early_candidate
                )
            self.assertEqual(
                forged_read.exception.failure["category"],
                "transport_failure",
            )
            self.assertEqual(
                forged_read.exception.failure["code"],
                "engine_request_incomplete",
            )
            self.assertNotIn(
                "retry_disposition", forged_read.exception.failure
            )

            forged_mutation_request = {
                "type": "run",
                "plan": plan,
                "input": None,
                "plugin": process_target("/bin/true"),
                "run_id": "run:early-success",
            }
            forged_mutation_engine = os.path.join(
                directory, "engine-early-success-mutation"
            )
            write_nonreading_success_engine(
                forged_mutation_engine,
                forged_mutation_request,
                {
                    "type": "execution_boundary",
                    "execution": {
                        "status": "completed",
                        "result": {
                            "run_id": "run:early-success",
                            "plan_id": plan["plan_id"],
                            "value": None,
                            "projection_digest": "2" * 64,
                            "precondition_token": f"pre:0:{_content_id('3')}",
                            "effects": [],
                        },
                    },
                },
            )
            with self.assertRaises(EngineError) as forged_mutation:
                CliEngine(
                    forged_mutation_engine, timeout_seconds=10
                ).run(
                    plan,
                    None,
                    forged_mutation_request["plugin"],
                    "run:early-success",
                )
            self.assertEqual(
                forged_mutation.exception.failure["category"],
                "unknown_world_outcome",
            )
            self.assertEqual(
                forged_mutation.exception.failure["code"],
                "engine_request_incomplete",
            )
            self.assertEqual(
                forged_mutation.exception.failure["retry_disposition"],
                "reconcile",
            )

    def test_python_rejects_invalid_live_command_before_starting_engine(self) -> None:
        candidate = FlowBuilder("invalid_live_local", {}, {}).finish({"kind": "input"})
        command = LiveEvolutionControlBuilder.publish_definition(
            "command:invalid-local",
            "definition:invalid-local",
            candidate["definitions"][0],
            [],
        )
        command["control_version"] = "cymule.live-evolution-control/5"
        with tempfile.TemporaryDirectory(prefix="cymule-python-no-process-") as directory:
            executable = os.path.join(directory, "engine")
            marker = executable + ".started"
            with open(executable, "w", encoding="utf-8") as script:
                script.write(
                    f'#!/bin/sh\n/bin/touch "{marker}"\nexit 1\n'
                )
            os.chmod(executable, 0o700)
            with self.assertRaises(EngineError) as rejected:
                CliEngine(executable).execute_live_evolution(
                    evolution_target(),
                    "journal:test",
                    command,
                )
            self.assertEqual(rejected.exception.failure["category"], "validation")
            self.assertEqual(
                rejected.exception.failure["code"], "invalid_engine_request"
            )
            self.assertEqual(
                rejected.exception.failure["retry_disposition"],
                "correct_and_retry",
            )
            self.assertFalse(os.path.exists(marker))

    def test_python_migration_epochs_and_continuations_are_closed(self) -> None:
        with self.assertRaises(EngineError):
            _validate_positive_epoch(0)
        artifact = {
            "identity_version": "cymule.artifact/2",
            "artifact_id": "sha256:" + "1" * 64,
            "kind": "test/value",
        }
        continuation = {
            "continuation_version": "cymule.continuation-state/1",
            "run_id": "run:test",
            "plan_id": "sha256:" + "2" * 64,
            "binding_context": "sha256:" + "3" * 64,
            "frames": [{
                "definition_id": "main",
                "invocation_id": "main",
                "invocation_path": [],
                "scope_id": "scope:root",
                "input": artifact,
                "region_path": [],
                "next_step": 0,
                "locals": {},
            }],
            "state": artifact,
            "wait_set": [],
            "scope_stack": [],
            "epoch": 0,
            "status": "ready",
        }
        with self.assertRaises(EngineError):
            _validate_migration_continuation(continuation)

        migration_request = copy.deepcopy(
            _fixed_live_evolution_outcomes()["migrated"]["receipt"]["request"]
        )
        migration_request["expected_source_epoch"] = 9_007_199_254_740_992
        with self.assertRaises(EngineError):
            _validate_migration_request(migration_request)

        migration_request = copy.deepcopy(
            _fixed_live_evolution_outcomes()["migrated"]["receipt"]["request"]
        )
        migration_request["to_plan"] = migration_request["from_plan"]
        with self.assertRaises(EngineError):
            _validate_migration_request(migration_request)

        restart = copy.deepcopy(
            _fixed_live_evolution_outcomes()["restart_authorized"]["receipt"][
                "request"
            ]
        )
        restart["input"] = {**artifact, "identity_version": "cymule.artifact/1"}
        with self.assertRaises(EngineError):
            _validate_restart_request(restart)

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
                process_target(plugin_path),
                "run:python-expected",
            ),
            expected["expected_plugin_failure"],
        )
        self._assert_engine_failure(
            lambda: engine.run(
                plan,
                {"message": "defect"},
                process_target(engine_path),
                "run:python-defect",
            ),
            expected["plugin_defect"],
        )
        self._assert_engine_failure(
            lambda: engine.run(
                plan,
                {"message": "substrate"},
                process_target("/cymule-conformance/missing-plugin"),
                "run:python-substrate",
            ),
            expected["substrate_failure"],
        )

    def test_python_rejects_missing_or_null_component_output_artifact_kind(
        self,
    ) -> None:
        candidate = (
            FlowBuilder("component-output-kind", {}, {})
            .component(
                "test.echo", {}, {}, "cymule.component-output/1", {}
            )
            .finish({"kind": "input"})
        )
        component = candidate["components"][0]
        without_output_artifact_kind = {
            key: value
            for key, value in component.items()
            if key != "output_artifact_kind"
        }
        for label, malformed_component in (
            ("missing", without_output_artifact_kind),
            ("null", {**component, "output_artifact_kind": None}),
        ):
            with self.subTest(label=label), tempfile.TemporaryDirectory(
                prefix="cymule-python-component-output-kind-"
            ) as directory:
                malformed = copy.deepcopy(candidate)
                malformed["components"][0] = malformed_component
                engine = _engine_with_success(
                    directory,
                    {
                        "type": "sealed",
                        "plan": {
                            "plan_id": _content_id("1"),
                            "candidate": malformed,
                        },
                    },
                )
                with self.assertRaises(EngineError) as rejected:
                    engine.seal(malformed)
                self.assertEqual(
                    rejected.exception.failure["category"], "transport_failure"
                )
                self.assertEqual(
                    rejected.exception.failure["code"], "invalid_engine_response"
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
        cancel_fixture_path = os.environ.get("CYMULE_DURABLE_CANCEL_FIXTURE")
        if engine_path is None or fixture_path is None or cancel_fixture_path is None:
            self.skipTest("durable control conformance is not configured")
        command = DurableControlBuilder.takeover_run(
            "run:cross-language", 7, fixture_execution()
        )
        with open(fixture_path, encoding="utf-8") as source:
            self.assertEqual(command, json.load(source))
        self.assertEqual(
            CliEngine(engine_path).verify_durable_command(command), command
        )
        cancel = DurableControlBuilder.cancel_run(
            "cancel:cross-language",
            "run:cross-language",
            {"code": "operator_request"},
        )
        with open(cancel_fixture_path, encoding="utf-8") as source:
            self.assertEqual(cancel, json.load(source))
        self.assertEqual(
            CliEngine(engine_path).verify_durable_command(cancel), cancel
        )
        self.assertEqual(
            DurableControlBuilder.activate_signal(
                "activation:sdk",
                "signal:sdk",
                [_content_id("b"), _content_id("a"), _content_id("b")],
                {"accepted": True},
            )["wait_ids"],
            [_content_id("a"), _content_id("b")],
        )

    def test_python_accepts_shared_terminal_durable_boundaries(self) -> None:
        fixture_path = os.environ.get("CYMULE_DURABLE_TERMINAL_FIXTURE")
        if fixture_path is None:
            self.skipTest("durable terminal fixture is not configured")
        with open(fixture_path, encoding="utf-8") as source:
            responses = json.load(source)
        for response in responses:
            _validate_engine_envelope(
                {
                    "engine_protocol": "cymule.engine/5",
                    "outcome": "success",
                    "request": {},
                    "response": {"type": "durable_executed", "response": response},
                }
            )
            if response["type"] == "run_boundary" and response["boundary"]["status"] == "failed":
                self.assertEqual(response["boundary"]["status"], "failed")
            elif response["type"] == "run_cancelled":
                self.assertEqual(response["type"], "run_cancelled")
                self.assertEqual(
                    response["receipt"]["boundary"]["status"], "cancelled"
                )
            elif response["boundary"]["status"] == "effect_not_applied":
                self.assertEqual(
                    response["boundary"],
                    {
                        "status": "effect_not_applied",
                        "intent_id": _content_id("2"),
                    },
                )
            else:
                self.assertEqual(
                    response["boundary"],
                    {
                        "status": "effect_unavailable",
                        "intent_id": (
                            "sha256:982a836f8dcb860b0eedabf0fd133bc2"
                            "f966992526e2703316cba497f929e03b"
                        ),
                    },
                )

    def test_python_unified_live_evolution_validates(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        fixture_path = os.environ.get("CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE")
        if engine_path is None or fixture_path is None:
            self.skipTest("live-evolution conformance is not configured")
        with open(fixture_path, encoding="utf-8") as source:
            expected = json.load(source)
        selection = EvolutionControlBuilder.select_occurrence(
            "command:evolution:fixture:select",
            "occurrence:fixture:1",
            "selection:fixture:1",
            expected["command"]["execution_binding"],
        )
        command = LiveEvolutionControlBuilder.apply(
            "command:live-evolution:fixture:select",
            "template:review-parent",
            selection,
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
        live_restart = LiveEvolutionControlBuilder.apply(
            "command:live-evolution:fixture:restart",
            "template:fixture:restart",
            restart,
        )
        self.assertEqual(
            CliEngine(engine_path).verify_live_evolution_command(live_restart),
            live_restart,
        )

    def test_python_candidate_seals_and_executes(self) -> None:
        engine_path = os.environ.get("CYMULE_BIN")
        plugin_path = os.environ.get("CYMULE_TEST_PLUGIN")
        expected_plan_id = os.environ.get("CYMULE_EXPECTED_PLAN_ID")
        if engine_path is None or plugin_path is None or expected_plan_id is None:
            self.skipTest("cross-language binaries are not configured")
        candidate = (
            FlowBuilder("cross_language_echo", {}, {})
            .component(
                "test.echo", {}, {}, "cymule.component-output/1", {}
            )
            .effect_contract(
                "test.capture",
                {},
                {},
                {
                    "mutation": "observational",
                    "dispatch": "eager",
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
                "observed",
            )
            .scope(
                "scope.finalize",
                {"steps": [], "result": {"kind": "literal", "value": None}},
                "scope_result",
            )
            .finish({"kind": "binding", "name": "echoed"})
        )
        engine = CliEngine(engine_path)
        plan = engine.seal(candidate)
        self.assertEqual(plan["plan_id"], expected_plan_id)
        input_value = {"message": "hello from Python"}
        plugin = process_target(plugin_path)
        execution = engine.run(plan, input_value, plugin, "run:python-e2e")
        self.assertEqual(execution["status"], "completed")
        if execution["status"] != "completed":
            self.fail("expected terminal execution")
        result = execution["result"]
        self.assertEqual(result["value"], input_value)
        self.assertEqual(len(result["effects"]), 1)
        with tempfile.TemporaryDirectory(prefix="cymule-python-durable-") as store:
            target = sqlite_store(os.path.join(store, "domain.sqlite"), "sdk-python")
            clock = sqlite_clock(
                os.path.join(store, "clock.sqlite"),
                "clock:sdk-python",
                "sha256:" + "3" * 64,
            )
            durable = DurableEngine(target, plugin, clock, engine)
            clock_ref = durable.observe_clock("run:python-durable-e2e")
            later_clock_ref = durable.observe_clock("run:python-durable-e2e")
            self.assertNotEqual(later_clock_ref["observation_id"], clock_ref["observation_id"])
            self.assertEqual(
                durable.start(
                    "run:python-durable-e2e",
                    candidate,
                    input_value,
                    {"owner": "driver:sdk-python", "clock": later_clock_ref, "ttl": 30},
                )["type"],
                "run_boundary",
            )
            self.assertIsNotNone(
                DurableEngine(target, None, None, engine).run_current(
                    "run:python-durable-e2e", None
                )["current"]
            )
            self.assertEqual(
                durable.evolve(
                    LiveEvolutionControlBuilder.publish_definition(
                        "evolve:python:publish",
                        "definition:python:echo",
                        candidate["definitions"][0],
                        [],
                    )
                )["receipt"]["outcome"]["result"],
                "definition_published",
            )

    def test_python_rejects_malicious_engine_and_cancels_in_flight(self) -> None:
        malicious = os.environ.get("CYMULE_MALICIOUS_ENGINE")
        slow = os.environ.get("CYMULE_SLOW_ENGINE")
        if malicious is None or slow is None:
            self.skipTest("malicious Engine conformance is not configured")
        with self.assertRaises(EngineError) as forged:
            DurableEngine("unused", None, None, CliEngine(malicious)).run_current(
                "run:fake", None
            )
        self.assertEqual(forged.exception.failure["code"], "invalid_engine_response")
        self.assertEqual(forged.exception.failure["category"], "transport_failure")
        cancelled = threading.Event()
        timer = threading.Timer(0.05, cancelled.set)
        timer.start()
        try:
            with self.assertRaises(EngineError) as interrupted:
                CliEngine(slow, timeout_seconds=5, cancelled=cancelled.is_set).seal(
                    FlowBuilder("cancel-in-flight", {}, {}).finish({"kind": "input"})
                )
            self.assertEqual(interrupted.exception.failure["category"], "cancelled")
            self.assertEqual(
                interrupted.exception.failure["retry_disposition"], "never"
            )
        finally:
            timer.cancel()

    def test_python_rejects_malicious_effect_boundary(self) -> None:
        malicious = os.environ.get("CYMULE_MALICIOUS_EFFECT_ENGINE")
        if malicious is None:
            self.skipTest("malicious Effect Engine conformance is not configured")
        candidate = FlowBuilder("malicious-effect", {}, {}).finish({"kind": "input"})
        with self.assertRaises(EngineError) as rejected:
            CliEngine(malicious).run(
                {"plan_id": _content_id("1"), "candidate": candidate},
                None,
                process_target("/bin/true"),
                "run:malicious-effect",
            )
        self.assertEqual(
            rejected.exception.failure["code"], "invalid_engine_response"
        )
        self.assertEqual(
            rejected.exception.failure["category"], "unknown_world_outcome"
        )
        self.assertEqual(
            rejected.exception.failure["retry_disposition"], "reconcile"
        )

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
            ["sha256:8d55f9d1981f4579ce12d106f25d85307ed27db86a4c106bbe17cb0ea8e9acc5"],
            {
                "identity_version": "cymule.artifact/2",
                "artifact_id": (
                    "sha256:0123456789abcdef0123456789abcdef"
                    "0123456789abcdef0123456789abcdef"
                ),
                "kind": "cymule.wait-result/1",
            },
        )
        with open(fixture_path, encoding="utf-8") as source:
            self.assertEqual(activation, json.load(source))
        self.assertEqual(
            CliEngine(engine_path).verify_wait_activation(activation), activation
        )

    def test_python_preserves_complete_virtual_compaction_certificate_wire(self) -> None:
        certificate: VirtualCompactionCertificate = {
            "certificate_version": "cymule.virtual-compaction-certificate/4",
            "certificate_id": _content_id("1"),
            "source_causal_cut": ["virtual:terminal"],
            "summary": {
                "region_id": "region:terminal",
                "run_id": "run:terminal",
                "occurrence_count": 1,
                "work_count": 1,
                "succeeded_count": 1,
                "failed_count": 0,
                "cancelled_count": 0,
                "output_digest": "2" * 64,
                "evidence_digest": "3" * 64,
                "retained_debug_index_digest": "4" * 64,
            },
            "summary_state_digest": "5" * 64,
            "occurrence_root_digest": _content_id("6"),
            "parent_work_index_root_digest": _content_id("7"),
            "work_index_updates_digest": "8" * 64,
            "work_index_root_digest": _content_id("9"),
            "command_root_digest": None,
            "command_count": 0,
            "unresolved_obligations": [],
            "retained_execution_bindings": [
                {
                    "identity_version": "cymule.artifact/2",
                    "artifact_id": _content_id("a"),
                    "kind": "cymule.execution-binding/2",
                }
            ],
            "replay_availability": {"status": "exact"},
            "rehydration_manifest": {
                "resource_version": "cymule.resource/3",
                "resource_id": _content_id("b"),
                "shape": "object",
                "media_type": "application/octet-stream",
                "integrity": {"kind": "content", "digest": "c" * 64, "size": 0},
            },
            "archive": {"binding": "compactor:terminal", "revision": "revision:terminal"},
        }
        wire = json.loads(json.dumps(certificate))
        self.assertIsNone(wire["command_root_digest"])
        required = {
            "parent_work_index_root_digest",
            "work_index_updates_digest",
            "work_index_root_digest",
            "command_root_digest",
            "command_count",
        }
        self.assertTrue(required.issubset(VirtualCompactionCertificate.__required_keys__))
        missing = copy.deepcopy(wire)
        del missing["command_root_digest"]
        self.assertNotEqual(missing, wire)

    def test_python_virtual_work_query_and_control_fixtures_stay_exact(self) -> None:
        occurrence_path = os.environ.get("CYMULE_VIRTUAL_OCCURRENCE_FIXTURE")
        control_path = os.environ.get("CYMULE_VIRTUAL_CONTROL_FIXTURE")
        if occurrence_path is None or control_path is None:
            self.skipTest("virtual work SDK conformance is not configured")
        with open(occurrence_path, encoding="utf-8") as source:
            occurrence = json.load(source)
        self.assertEqual(
            occurrence["execution_binding"]["kind"], "cymule.execution-binding/2"
        )
        with open(control_path, encoding="utf-8") as source:
            control_fixture = json.load(source)
        command = VirtualWorkControlBuilder.succeed(
            "command:virtual:fixture:success",
            "work:fixture",
            "worker:fixture",
            1,
            1,
            control_fixture["clock"],
            {
                "identity_version": "cymule.artifact/2",
                "artifact_id": (
                    "sha256:abcdef0123456789abcdef0123456789"
                    "abcdef0123456789abcdef0123456789"
                ),
                "kind": "example/result",
            },
        )
        self.assertEqual(command, control_fixture)
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
            compaction_fixture = json.load(source)
            self.assertEqual(
                VirtualWorkControlBuilder.compaction(
                    compaction_fixture["command_id"],
                    "region:fixture",
                    ["virtual:fixture:terminal"],
                    ["work:fixture"],
                    [occurrence["occurrence_id"]],
                    [],
                    {"binding": "binding:archive/fixture@1", "revision": "compactor:fixture/1"},
                ),
                compaction_fixture,
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
            claim_fixture = json.load(source)
        self.assertEqual(
            VirtualSchedulingControlBuilder.claim(
                    "command:claim:fixture",
                    "worker:fixture",
                    "slot:worker-fixture:0",
                    {
                        "identity_version": "cymule.artifact/2",
                        "artifact_id": "sha256:" + "2" * 64,
                        "kind": "cymule.execution-binding/2",
                    },
                    ["sandbox", "cpu", "cpu"],
                    claim_fixture["clock"],
                    30,
            ),
            claim_fixture,
        )
        with open(renewal_path, encoding="utf-8") as source:
            renewal_fixture = json.load(source)
        self.assertEqual(
            VirtualSchedulingControlBuilder.renew(
                    "command:renew:fixture",
                    "work:fixture",
                    "worker:fixture",
                    1,
                    1,
                    renewal_fixture["clock"],
                    30,
            ),
            renewal_fixture,
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
                recovery_fixture["clock"],
                recovery_fixture["resolution"],
            ),
            recovery_fixture,
        )
        for forged_resolution in (
            {**recovery_fixture["resolution"], "unknown": True},
            {
                "resolution": "retry",
                "error": recovery_fixture["resolution"]["error"],
                "reason": {"kind": "wait", "key": "wait:forged"},
            },
            {
                "resolution": "parked",
                "reason": {"kind": "wait", "key": "wait:forged"},
            },
        ):
            with self.subTest(forged_resolution=forged_resolution):
                with self.assertRaises(ValueError):
                    VirtualSchedulingControlBuilder.recovery(
                        "command:recovery:forged",
                        "work:fixture",
                        "worker:fixture",
                        1,
                        2,
                        recovery_fixture["clock"],
                        forged_resolution,
                    )
        with open(run_weight_path, encoding="utf-8") as source:
            self.assertEqual(
                VirtualSchedulingControlBuilder.run_weight(
                    "command:run-weight:fixture", "run:fixture", 3
                ),
                json.load(source),
            )
        with self.assertRaises(ValueError):
            VirtualSchedulingControlBuilder.claim(
                "command:unsafe",
                "worker:fixture",
                "slot:worker-fixture:0",
                claim_fixture["execution_binding"],
                [],
                claim_fixture["clock"],
                9_007_199_254_740_992,
            )


if __name__ == "__main__":
    unittest.main()
