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
        by_title["Cymule Plan Candidate cymule.ir/1"], registry=registry
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
