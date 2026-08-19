#!/usr/bin/env python3
"""Validate frozen schemas and public fixtures with Draft 2020-12."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource


def load(path: Path) -> object:
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_schemas.py ROOT CYMULE_BIN")
    root = Path(sys.argv[1]).resolve()
    engine = Path(sys.argv[2]).resolve()
    schema_paths = sorted((root / "schemas").glob("*.schema.json"))
    schemas = [load(path) for path in schema_paths]
    for schema in schemas:
        Draft202012Validator.check_schema(schema)
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas
    )
    by_title = {schema["title"]: schema for schema in schemas}

    candidate_validator = Draft202012Validator(
        by_title["Cymule Plan Candidate cymule.ir/2"], registry=registry
    )
    candidate_paths = [
        root / "tests/fixtures/cross-language-plan.json",
        root / "examples/hello-world/flow.json",
    ]
    candidates = [load(path) for path in candidate_paths]
    for candidate in candidates:
        candidate_validator.validate(candidate)
    candidate = candidates[0]

    sealed = json.loads(
        subprocess.run(
            [str(engine), "seal", "--input", str(root / "tests/fixtures/cross-language-plan.json")],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    Draft202012Validator(
        by_title["Cymule Sealed Plan"], registry=registry
    ).validate(sealed)
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "seal", "candidate": candidate})

    plugin_validator = Draft202012Validator(
        by_title["Cymule Process Plugin Message"], registry=registry
    )
    plugin_validator.validate({"type": "describe"})
    plugin_validator.validate(
        {
            "type": "dispatch_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "input": {"message": "fixture"},
        }
    )

    resource_validator = Draft202012Validator(
        by_title["Cymule Resource Protocol cymule.resource/1"], registry=registry
    )
    resource_candidate = load(root / "tests/fixtures/resource-candidate.json")
    resource_validator.validate(resource_candidate)
    sealed_resource = json.loads(
        subprocess.run(
            [
                str(engine),
                "resource",
                "seal",
                "--input",
                str(root / "tests/fixtures/resource-candidate.json"),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    resource_validator.validate(sealed_resource)
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "seal_resource", "candidate": resource_candidate})
    resource_validator.validate(
        {
            "handoff_version": "cymule.resource-handoff/1",
            "transfer_id": "transfer:fixture",
            "from_run": "run:producer",
            "to_run": "run:consumer",
            "slot": "input.resource",
            "resource": sealed_resource,
        }
    )
    malformed_resource = dict(resource_candidate)
    malformed_resource["provider"] = "must-not-enter-resource-semantics"
    try:
        resource_validator.validate(malformed_resource)
    except ValidationError:
        pass
    else:
        raise AssertionError("Resource schema accepted an unknown provider field")

    wait_activation = load(root / "tests/fixtures/wait-activation.json")
    activation_validator = Draft202012Validator(
        by_title["Cymule Wait Activation cymule.wait-activation/1"],
        registry=registry,
    )
    activation_validator.validate(wait_activation)
    verified_activation = json.loads(
        subprocess.run(
            [
                str(engine),
                "wait-activation",
                "verify",
                "--input",
                str(root / "tests/fixtures/wait-activation.json"),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    if verified_activation != wait_activation:
        raise AssertionError("Rust Engine changed the wait activation fixture")
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "verify_wait_activation", "activation": wait_activation})
    malformed_activation = dict(wait_activation)
    malformed_activation["provider"] = "must-not-enter-wait-activation"
    try:
        activation_validator.validate(malformed_activation)
    except ValidationError:
        pass
    else:
        raise AssertionError("wait activation schema accepted a provider field")

    durable_control = load(root / "tests/fixtures/durable-control.json")
    durable_validator = Draft202012Validator(
        by_title["Cymule Durable Control cymule.durable-control/1"],
        registry=registry,
    )
    durable_variants = [
        {
            "type": "start_run",
            "control_version": "cymule.durable-control/1",
            "run_id": "run:fixture",
            "candidate": candidate,
            "input": {"message": "fixture"},
        },
        {
            "type": "resume_run",
            "control_version": "cymule.durable-control/1",
            "run_id": "run:fixture",
        },
        {
            "type": "activate_wait",
            "control_version": "cymule.durable-control/1",
            "activation_id": "activation:fixture",
            "source": {"kind": "signal", "key": "signal:fixture"},
            "wait_ids": ["wait:fixture"],
            "value": {"accepted": True},
        },
        {
            "type": "release_effect",
            "control_version": "cymule.durable-control/1",
            "intent_id": "effect:fixture",
        },
        {
            "type": "query_run",
            "control_version": "cymule.durable-control/1",
            "query_id": "query:run-fixture",
            "run_id": "run:fixture",
        },
        durable_control,
    ]
    for command in durable_variants:
        durable_validator.validate(command)
        verified = json.loads(
            subprocess.run(
                [str(engine), "durable-command", "verify", "--input", "-"],
                input=json.dumps(command),
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        if verified != command:
            raise AssertionError(
                f"Rust Engine changed durable command {command['type']}"
            )
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "verify_durable_command", "command": durable_control})
    malformed_durable = dict(durable_control)
    malformed_durable["provider"] = "must-not-enter-durable-control"
    try:
        durable_validator.validate(malformed_durable)
    except ValidationError:
        pass
    else:
        raise AssertionError("durable control schema accepted a provider field")

    evolution_control = load(root / "tests/fixtures/evolution-control.json")
    evolution_validator = Draft202012Validator(
        by_title["Cymule Evolution Control cymule.evolution-control/2"],
        registry=registry,
    )
    evolution_validator.validate(evolution_control)
    verified_evolution = json.loads(
        subprocess.run(
            [
                str(engine),
                "evolution-command",
                "verify",
                "--input",
                str(root / "tests/fixtures/evolution-control.json"),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    if verified_evolution != evolution_control:
        raise AssertionError("Rust Engine changed the evolution control fixture")
    artifact = {
        "artifact_id": "sha256:" + "a" * 64,
        "kind": "test/evidence",
    }
    evolution_variants = [
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:patch",
            "operation": "apply_patch",
            "patch": {
                "from_plan": "sha256:" + "1" * 64,
                "target": candidate,
                "operations": [
                    {
                        "kind": "replace",
                        "target": "definition:main",
                        "before": None,
                        "after": "sha256:" + "2" * 64,
                    }
                ],
                "evidence": artifact,
            },
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:rollout",
            "operation": "set_rollout",
            "decision": {
                "decision_id": "rollout:canary",
                "fallback_plan": "sha256:" + "1" * 64,
                "target_plan": "sha256:" + "2" * 64,
                "mode": {"mode": "canary", "basis_points": 500},
            },
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:select",
            "operation": "select_occurrence",
            "occurrence_id": "occurrence:1",
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:migrate",
            "operation": "migrate",
            "request": {
                "migration_id": "migration:1",
                "run_id": "run:1",
                "from_plan": "sha256:" + "1" * 64,
                "to_plan": "sha256:" + "2" * 64,
                "safe_point_id": "sha256:" + "3" * 64,
                "source_epoch": 7,
                "input_state": artifact,
            },
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:restart",
            "operation": "restart_under_new_plan",
            "request": {
                "restart_id": "restart:1",
                "source_run": "run:source",
                "replacement_run": "run:replacement",
                "from_plan": "sha256:" + "1" * 64,
                "to_plan": "sha256:" + "2" * 64,
                "safe_point_id": "sha256:" + "3" * 64,
                "source_epoch": 7,
                "input": artifact,
                "evidence": artifact,
            },
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:shadow",
            "operation": "shadow",
            "request": {
                "comparison_id": "shadow:1",
                "decision_id": "rollout:canary",
                "subject": "occurrence:1",
                "primary_plan": "sha256:" + "1" * 64,
                "shadow_plan": "sha256:" + "2" * 64,
                "input": artifact,
                "comparison_policy": "json-exact/1",
            },
        },
        {
            "control_version": "cymule.evolution-control/2",
            "command_id": "command:observe",
            "operation": "observe",
            "observation": {
                "observation_id": "observation:1",
                "decision_id": "rollout:canary",
                "occurrence_id": "occurrence:1",
                "plan_id": "sha256:" + "2" * 64,
                "outcome": "succeeded",
                "evidence": artifact,
            },
        },
        evolution_control,
    ]
    for command in evolution_variants:
        evolution_validator.validate(command)
        verified = json.loads(
            subprocess.run(
                [str(engine), "evolution-command", "verify", "--input", "-"],
                input=json.dumps(command),
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        if verified != command:
            raise AssertionError(
                f"Rust Engine changed evolution operation {command['operation']}"
            )
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "verify_evolution_command", "command": evolution_control})
    malformed_evolution = dict(evolution_control)
    malformed_evolution["provider"] = "must-not-enter-evolution-control"
    try:
        evolution_validator.validate(malformed_evolution)
    except ValidationError:
        pass
    else:
        raise AssertionError("evolution control schema accepted a provider field")

    live_evolution = load(root / "tests/fixtures/live-evolution-control.json")
    live_evolution_validator = Draft202012Validator(
        by_title[
            "Cymule Live Evolution Control cymule.live-evolution-control/1"
        ],
        registry=registry,
    )
    live_evolution_validator.validate(live_evolution)
    verified_live_evolution = json.loads(
        subprocess.run(
            [
                str(engine),
                "live-evolution-command",
                "verify",
                "--input",
                str(root / "tests/fixtures/live-evolution-control.json"),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    if verified_live_evolution != live_evolution:
        raise AssertionError("Rust Engine changed the live-evolution fixture")
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate(
        {"type": "verify_live_evolution_command", "command": live_evolution}
    )
    malformed_live_evolution = dict(live_evolution)
    malformed_live_evolution["provider"] = "must-not-enter-live-evolution"
    try:
        live_evolution_validator.validate(malformed_live_evolution)
    except ValidationError:
        pass
    else:
        raise AssertionError(
            "live-evolution control schema accepted a provider field"
        )

    virtual_checkpoint = load(root / "tests/fixtures/virtual-checkpoint.json")
    virtual_validator = Draft202012Validator(
        by_title["Cymule Virtual Checkpoint cymule.virtual-checkpoint/1"],
        registry=registry,
    )
    virtual_validator.validate(virtual_checkpoint)
    virtual_occurrence = load(root / "tests/fixtures/virtual-work-occurrence.json")
    occurrence_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                "#/$defs/occurrence"
            )
        },
        registry=registry,
    )
    occurrence_validator.validate(virtual_occurrence)
    virtual_control = load(root / "tests/fixtures/virtual-work-control.json")
    control_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                "#/$defs/controlCommand"
            )
        },
        registry=registry,
    )
    control_validator.validate(virtual_control)
    migration_control = load(
        root / "tests/fixtures/virtual-region-migration-control.json"
    )
    migration_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                "#/$defs/migrationControlCommand"
            )
        },
        registry=registry,
    )
    migration_validator.validate(migration_control)
    compaction_control = load(
        root / "tests/fixtures/virtual-compaction-control.json"
    )
    compaction_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                "#/$defs/compactionControlCommand"
            )
        },
        registry=registry,
    )
    compaction_validator.validate(compaction_control)
    rehydration_control = load(
        root / "tests/fixtures/virtual-rehydration-control.json"
    )
    rehydration_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                "#/$defs/rehydrationControlCommand"
            )
        },
        registry=registry,
    )
    rehydration_validator.validate(rehydration_control)
    scheduling_fixtures = [
        (
            "virtual-claim-control.json",
            "claimControlCommand",
            "virtual claim",
        ),
        (
            "virtual-lease-renewal-control.json",
            "leaseRenewalControlCommand",
            "virtual lease renewal",
        ),
        (
            "virtual-recovery-control.json",
            "recoveryControlCommand",
            "virtual recovery",
        ),
        (
            "virtual-run-weight-control.json",
            "runWeightControlCommand",
            "virtual Run weight",
        ),
    ]
    for fixture_name, definition, label in scheduling_fixtures:
        value = load(root / "tests/fixtures" / fixture_name)
        validator = Draft202012Validator(
            {
                "$ref": (
                    "https://cymule.dev/schemas/virtual-checkpoint.schema.json"
                    f"#/$defs/{definition}"
                )
            },
            registry=registry,
        )
        validator.validate(value)
        malformed_value = dict(value)
        malformed_value["provider"] = "must-not-enter-virtual-scheduling"
        try:
            validator.validate(malformed_value)
        except ValidationError:
            pass
        else:
            raise AssertionError(f"{label} accepted a provider field")
    malformed_virtual = dict(virtual_checkpoint)
    malformed_virtual["provider"] = "must-not-enter-virtual-checkpoint"
    try:
        virtual_validator.validate(malformed_virtual)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual checkpoint schema accepted a provider field")
    malformed_occurrence = dict(virtual_occurrence)
    malformed_occurrence["provider"] = "must-not-enter-work-occurrence"
    try:
        occurrence_validator.validate(malformed_occurrence)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual work occurrence accepted a provider field")
    malformed_control = dict(virtual_control)
    malformed_control["provider"] = "must-not-enter-work-control"
    try:
        control_validator.validate(malformed_control)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual work control accepted a provider field")
    malformed_migration = dict(migration_control)
    malformed_migration["provider"] = "must-not-enter-region-migration"
    try:
        migration_validator.validate(malformed_migration)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual region migration accepted a provider field")
    malformed_compaction = dict(compaction_control)
    malformed_compaction["provider"] = "must-not-enter-virtual-compaction"
    try:
        compaction_validator.validate(malformed_compaction)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual compaction accepted a provider field")
    malformed_rehydration = dict(rehydration_control)
    malformed_rehydration["provider"] = "must-not-enter-virtual-rehydration"
    try:
        rehydration_validator.validate(malformed_rehydration)
    except ValidationError:
        pass
    else:
        raise AssertionError("virtual rehydration accepted a provider field")

    credential_url = {
        "resource_version": "cymule.resource/1",
        "shape": "object",
        "media_type": "application/octet-stream",
        "integrity": {"kind": "live", "identity": "live:credential-check"},
        "locations": [
            {
                "kind": "public_url",
                "url": "https://example.com/object?access_token=secret",
            }
        ],
    }
    credential_result = subprocess.run(
        [str(engine), "resource", "seal", "--input", "-"],
        input=json.dumps(credential_url),
        capture_output=True,
        text=True,
        check=False,
    )
    if credential_result.returncode == 0:
        raise AssertionError("Rust resource sealer accepted a credential-bearing public URL")

    malformed = dict(candidate)
    malformed["provider"] = "must-not-enter-canonical-plan"
    try:
        candidate_validator.validate(malformed)
    except ValidationError:
        pass
    else:
        raise AssertionError("Plan schema accepted an unknown provider field")

    print(
        f"validated {len(schemas)} schemas, {len(candidates)} public Plans, "
        "and shared protocol fixtures"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
