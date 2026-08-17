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
    stream_records = load(root / "tests/fixtures/agent-stream-records.json")
    for stream_record in stream_records:
        agent_validator.validate(stream_record)
    Draft202012Validator(
        by_title["Cymule Engine Request"], registry=registry
    ).validate({"type": "verify_agent_stream", "records": stream_records})
    malformed_stream = dict(stream_records[0])
    malformed_stream["provider"] = "must-not-enter-agent-stream-semantics"
    try:
        agent_validator.validate(malformed_stream)
    except ValidationError:
        pass
    else:
        raise AssertionError("Agent schema accepted an unknown stream provider field")
    agent_validator.validate(
        {
            "resolution": "not_applied",
            "evidence": [
                {"type": "text", "text": "dispatch boundary was never entered"}
            ],
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
    agent_validator.validate(
        {
            "record": "chunk",
            "stream_id": "stream:resource-fixture",
            "session_id": "session:fixture",
            "chunk": {
                "sequence": 0,
                "content": [
                    {"type": "resource_handle", "resource": sealed_resource}
                ],
            },
        }
    )
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
