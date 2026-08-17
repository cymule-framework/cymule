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

    agent_validator = Draft202012Validator(
        by_title["Cymule Agent Protocol cymule.agent/1"], registry=registry
    )
    agent_occurrence = load(root / "tests/fixtures/agent-occurrence.json")
    agent_validator.validate(agent_occurrence)
    agent_validator.validate(
        {
            "type": "state",
            "update_id": "update:fixture:1",
            "state": "requires_action",
            "stop_reason": None,
        }
    )
    agent_validator.validate(
        {
            "type": "elicitation",
            "update_id": "update:agent-input:fixture:pending",
            "elicitation": {
                "wait_id": "wait:agent-input:fixture",
                "request": {
                    "request_id": "elicitation:fixture",
                    "schema": {"type": "string"},
                    "prompt": [{"type": "text", "text": "Continue?"}],
                },
                "response": None,
            },
        }
    )
    malformed_occurrence = dict(agent_occurrence)
    malformed_occurrence["provider"] = "must-not-enter-agent-occurrence"
    try:
        agent_validator.validate(malformed_occurrence)
    except ValidationError:
        pass
    else:
        raise AssertionError("Agent schema accepted an unknown provider field")

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
