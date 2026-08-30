#!/usr/bin/env python3
"""Validate frozen schemas and public fixtures with Draft 2020-12."""

from __future__ import annotations

import base64
import json
import subprocess
import sys
import tempfile
import urllib.parse
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

import version_domains


def load(path: Path) -> object:
    return version_domains.load_json(path)


def decode_json_text(value: str, label: str) -> object:
    return version_domains.load_json_bytes(value.encode("utf-8"), label=label)


def assert_invalid(validator: Draft202012Validator, value: object, message: str) -> None:
    try:
        validator.validate(value)
    except ValidationError:
        return
    raise AssertionError(message)


def release_publication_fixture(version: str, source_sha: str) -> list[dict]:
    cargo = []
    for crate in version_domains.release_catalog():
        name = crate["name"]
        digest = "sha256:" + "a" * 64
        cargo.append(
            {
                "package_id": f"cargo:{name}",
                "name": name,
                "version": version,
                "publication": {
                    "kind": "cargo",
                    "registry": version_domains.CRATES_REGISTRY,
                    "registry_identity": f"https://crates.io/crates/{name}/{version}",
                    "content_digest": digest,
                    "provenance": {
                        "kind": "registry-checksum",
                        "checksum": digest,
                        "download_url": (
                            f"https://static.crates.io/crates/{name}/"
                            f"{name}-{version}.crate"
                        ),
                    },
                },
            }
        )
    npm = []
    content = "d" * 128
    integrity = "sha512-" + base64.b64encode(bytes.fromhex(content)).decode()
    for name in ("cymule", "@cymule/sdk"):
        encoded = urllib.parse.quote(name, safe="")
        npm.append(
            {
                "package_id": f"npm:{name}",
                "name": name,
                "version": version,
                "publication": {
                    "kind": "npm",
                    "registry": version_domains.NPM_REGISTRY,
                    "registry_identity": (
                        f"{version_domains.NPM_REGISTRY.rstrip('/')}/{encoded}/{version}"
                    ),
                    "content_digest": f"sha512:{content}",
                    "provenance": {
                        "kind": "sigstore",
                        "sha1": "sha1:" + "c" * 40,
                        "integrity": integrity,
                        "tarball_url": f"{version_domains.NPM_REGISTRY}{encoded}/-/package.tgz",
                        "attestations_url": f"{version_domains.NPM_REGISTRY}-/attestations",
                        "bundle_digest": "sha256:" + "1" * 64,
                        "statement_digest": "sha256:" + "2" * 64,
                        "certificate_identity": version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY,
                        "certificate_issuer": version_domains.NPM_SIGSTORE_CERTIFICATE_ISSUER,
                        "predicate_type": version_domains.NPM_SLSA_PROVENANCE,
                        "workflow_ref": "refs/heads/main",
                        "source_sha": source_sha,
                        "signer_ref": version_domains.NPM_SIGSTORE_CERTIFICATE_IDENTITY,
                        "signer_sha": "e" * 40,
                    },
                },
            }
        )
    return [*cargo, *npm]


def main() -> int:
    if len(sys.argv) != 3:
        raise SystemExit("usage: validate_schemas.py ROOT CYMULE_BIN")
    root = Path(sys.argv[1]).resolve()
    engine = Path(sys.argv[2]).resolve()
    engine_request_limit_sources = {
        "crates/cymule-runtime/src/protocol.rs": (
            "pub const MAX_ENGINE_REQUEST_BYTES: usize = 64 * 1024 * 1024;"
        ),
        "crates/cymule-cli/src/main.rs": "MAX_ENGINE_REQUEST_BYTES",
        "crates/cymule-sdk/src/client.rs": "MAX_ENGINE_REQUEST_BYTES",
        "sdk/typescript/src/index.ts": (
            "const ENGINE_REQUEST_LIMIT = 64 * 1024 * 1024;"
        ),
        "sdk/python/src/cymule/__init__.py": (
            "_ENGINE_REQUEST_LIMIT = 64 * 1024 * 1024"
        ),
        "sdk/go/cymule.go": "const maxEngineRequestBytes = 64 * 1024 * 1024",
    }
    for relative, authority in engine_request_limit_sources.items():
        if authority not in (root / relative).read_text(encoding="utf-8"):
            raise AssertionError(
                f"{relative} drifted from the shared 64 MiB Engine request bound"
            )
    schema_paths = sorted((root / "schemas").glob("*.schema.json"))
    schemas = [load(path) for path in schema_paths]
    for schema in schemas:
        Draft202012Validator.check_schema(schema)
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas
    )
    by_title = {schema["title"]: schema for schema in schemas}
    version_registry = load(root / "versioning/version-domains.json")
    version_registry_validator = Draft202012Validator(
        by_title[
            "Cymule Version Domain Registry cymule.version-domain-registry/3"
        ],
        registry=registry,
    )
    version_registry_validator.validate(version_registry)
    assert_invalid(
        version_registry_validator,
        {**version_registry, "fallback_generation": "forbidden"},
        "version-domain registry accepted an unknown fallback generation",
    )
    release_version = version_registry["source_generation"]["workspace_version"]
    with tempfile.TemporaryDirectory(prefix="cymule-release-bom-") as temporary:
        publications = Path(temporary) / "publications.json"
        publications.write_text(
            json.dumps(release_publication_fixture(release_version, "b" * 40)),
            encoding="utf-8",
        )
        release_bom = json.loads(
            subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts/version_domains.py"),
                    "bom",
                    "--source-sha",
                    "a" * 40,
                    "--public-source-sha",
                    "b" * 40,
                    "--publications",
                    str(publications),
                ],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
    release_bom_validator = Draft202012Validator(
        by_title["Cymule Release BOM cymule.release-bom/3"], registry=registry
    )
    release_bom_validator.validate(release_bom)
    assert_invalid(
        release_bom_validator,
        {**release_bom, "shape_probe": True},
        "release BOM accepted an unknown shape-probing field",
    )
    assert_invalid(
        release_bom_validator,
        {**release_bom, "controller_sha": "c" * 40},
        "release BOM accepted mutable finalizer identity as immutable payload",
    )
    assert_invalid(
        release_bom_validator,
        {**release_bom, "public_source_sha": None},
        "release BOM accepted a null rewritten public source SHA",
    )
    package = release_bom["packages"][0]
    assert_invalid(
        release_bom_validator,
        {
            **release_bom,
            "packages": [
                {key: value for key, value in package.items() if key != "publication"},
                *release_bom["packages"][1:],
            ],
        },
        "release BOM accepted an omitted required-nullable publication",
    )
    canonical_artifact = {
        "identity_version": "cymule.artifact/2",
        "artifact_id": "sha256:" + "a" * 64,
        "kind": "test/evidence",
    }
    for schema_name in [
        "evolution-control.schema.json",
        "live-evolution-control.schema.json",
    ]:
        identity_validator = Draft202012Validator(
            {
                "$ref": f"https://cymule.dev/schemas/{schema_name}#/$defs/id"
            },
            registry=registry,
        )
        identity_validator.validate("🧪" * 256)
        for malformed_identity in ["🧪" * 257, "m4:c1:\u0085"]:
            assert_invalid(
                identity_validator,
                malformed_identity,
                f"{schema_name} accepted a malformed M4 identity",
            )
        run_identity_validator = Draft202012Validator(
            {
                "$ref": (
                    f"https://cymule.dev/schemas/{schema_name}#/$defs/runIdentity"
                )
            },
            registry=registry,
        )
        run_identity_validator.validate("🧪" * 512)
        for malformed_run_id in ["🧪" * 513, "run:c1:\u0085"]:
            assert_invalid(
                run_identity_validator,
                malformed_run_id,
                f"{schema_name} accepted a malformed M1 Run identity",
            )
    legacy_artifact = load(root / "tests/fixtures/legacy-artifact-reference.json")
    for schema_name in [
        "evolution-control.schema.json",
        "live-evolution-control.schema.json",
        "virtual-control.schema.json",
    ]:
        artifact_validator = Draft202012Validator(
            {
                "$ref": (
                    f"https://cymule.dev/schemas/{schema_name}#/$defs/artifact"
                )
            },
            registry=registry,
        )
        artifact_validator.validate(canonical_artifact)
        for malformed_artifact in [
            legacy_artifact,
            {**canonical_artifact, "artifact_id": "sha256:not-a-digest"},
            {**canonical_artifact, "artifact_id": "sha256:" + "A" * 64},
            {**canonical_artifact, "kind": "Invalid Kind"},
        ]:
            assert_invalid(
                artifact_validator,
                malformed_artifact,
                f"{schema_name} accepted a malformed Artifact reference",
            )
    engine_schema = by_title["Cymule Engine Protocol cymule.engine/5"]
    engine_validator = Draft202012Validator(engine_schema, registry=registry)
    effect_summary_validator = Draft202012Validator(
        {"$ref": engine_schema["$id"] + "#/$defs/durableEffectSummary"}, registry=registry
    )
    wait_summary_validator = Draft202012Validator(
        {"$ref": engine_schema["$id"] + "#/$defs/durableWaitSummary"}, registry=registry
    )
    pending_wait_summary = {
        "wait_id": "sha256:" + "a" * 64,
        "run_id": "run:wait-summary",
        "state": "pending",
        "result": None,
    }
    wait_summary_validator.validate(pending_wait_summary)
    for malformed in (
        {**pending_wait_summary, "state": "completed"},
        {**pending_wait_summary, "result": canonical_artifact},
    ):
        assert_invalid(
            wait_summary_validator,
            malformed,
            "Wait summary accepted a state/result mismatch",
        )
    applied_effect_summary = load(root / "tests/fixtures/applied-effect-summary.json")
    effect_summary_validator.validate(applied_effect_summary)
    for malformed in (
        {**applied_effect_summary, "result": None},
        {**applied_effect_summary, "state": "not_applied"},
        {**applied_effect_summary, "result": canonical_artifact},
        {key: value for key, value in applied_effect_summary.items() if key != "result"},
    ):
        assert_invalid(
            effect_summary_validator, malformed,
            "Effect summary erased an Applied result or retained a non-Applied result",
        )
    durable_identity_ref = {"$ref": "#/$defs/durableIdentity"}

    def assert_engine_run_identity_refs(value: object, path: str = "$") -> None:
        if isinstance(value, dict):
            properties = value.get("properties")
            if isinstance(properties, dict):
                expected_run_identity = (
                    {"oneOf": [durable_identity_ref, {"type": "null"}]}
                    if path == "$/$defs/durablePageCursor"
                    else durable_identity_ref
                )
                if (
                    "run_id" in properties
                    and properties["run_id"] != expected_run_identity
                ):
                    raise AssertionError(
                        f"Engine {path}.run_id does not use durableIdentity"
                    )
                for field in ["run_ids", "ready_run_ids"]:
                    field_schema = properties.get(field)
                    if not isinstance(field_schema, dict) or "items" not in field_schema:
                        continue
                    if field_schema["items"] != durable_identity_ref:
                        raise AssertionError(
                            f"Engine {path}.{field} does not use durableIdentity"
                        )
            for key, nested in value.items():
                assert_engine_run_identity_refs(nested, f"{path}/{key}")
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                assert_engine_run_identity_refs(nested, f"{path}/{index}")

    assert_engine_run_identity_refs(engine_schema)
    durable_identity_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/durableIdentity"
            )
        },
        registry=registry,
    )
    durable_identity_validator.validate("界" * 512)
    for malformed_identity in ["", "界" * 513, "run:\u0000forged", "run:\u0085forged"]:
        assert_invalid(
            durable_identity_validator,
            malformed_identity,
            "Engine durable identity accepted an invalid Unicode scalar boundary",
        )
    execution_binding_validator = Draft202012Validator(
        by_title["Cymule Execution Binding cymule.execution-binding/2"],
        registry=registry,
    )
    token = "x" * 256
    service = {"namespace": token, "name": token, "api_revision": token}
    implementation = {"implementation_id": token, "revision": token}
    provider = {
        "version": "cymule.runtime-composition/1",
        "provider_id": token,
        "implementation": implementation,
        "provides": [service],
        "requires": [],
        "properties": {"property": "界" * 1024},
        "configuration_schema_digest": "sha256:" + "a" * 64,
        "configuration_fingerprint": "sha256:" + "b" * 64,
    }
    operation_binding = {
        "service": service,
        "provider_id": token,
        "implementation": implementation,
        "operation_revision": token,
    }
    execution_binding = {
        "version": "cymule.execution-binding/2",
        "context": {
            "version": "cymule.runtime-composition/1",
            "providers": [provider],
        },
        "components": {"component": operation_binding},
        "effects": {"effect": {**operation_binding, "can_reconcile": True}},
    }
    execution_binding_validator.validate(execution_binding)
    for field, maximum, overflow in [
        (
            "providers",
            [{**provider, "provider_id": f"provider-{index}"} for index in range(64)],
            [{**provider, "provider_id": f"provider-{index}"} for index in range(65)],
        ),
        (
            "provides",
            [{**service, "name": f"service-{index}"} for index in range(256)],
            [{**service, "name": f"service-{index}"} for index in range(257)],
        ),
        (
            "properties",
            {f"property-{index}": "value" for index in range(256)},
            {f"property-{index}": "value" for index in range(257)},
        ),
        (
            "components",
            {f"component-{index}": operation_binding for index in range(128)},
            {f"component-{index}": operation_binding for index in range(129)},
        ),
        (
            "effects",
            {
                f"effect-{index}": {**operation_binding, "can_reconcile": True}
                for index in range(128)
            },
            {
                f"effect-{index}": {**operation_binding, "can_reconcile": True}
                for index in range(129)
            },
        ),
    ]:
        maximum_binding = json.loads(json.dumps(execution_binding))
        overflow_binding = json.loads(json.dumps(execution_binding))
        if field in {"providers"}:
            maximum_binding["context"][field] = maximum
            overflow_binding["context"][field] = overflow
        elif field in {"provides", "properties"}:
            maximum_binding["context"]["providers"][0][field] = maximum
            overflow_binding["context"]["providers"][0][field] = overflow
        else:
            maximum_binding[field] = maximum
            overflow_binding[field] = overflow
        execution_binding_validator.validate(maximum_binding)
        assert_invalid(
            execution_binding_validator,
            overflow_binding,
            f"Execution Binding accepted {field} above its terminal bound",
        )
    for malformed_binding in [
        {
            **execution_binding,
            "context": {
                **execution_binding["context"],
                "providers": [{**provider, "provider_id": "invalid provider"}],
            },
        },
        {
            **execution_binding,
            "context": {
                **execution_binding["context"],
                "providers": [
                    {**provider, "properties": {"property": "界" * 1025}}
                ],
            },
        },
    ]:
        assert_invalid(
            execution_binding_validator,
            malformed_binding,
            "Execution Binding accepted an invalid token or property value",
        )
    execution_result_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/executionResult"
            )
        },
        registry=registry,
    )
    execution_result = {
        "run_id": "run:schema-completed",
        "plan_id": "sha256:" + "a" * 64,
        "value": None,
        "projection_digest": "b" * 64,
        "precondition_token": "pre:9007199254740991:sha256:" + "c" * 64,
        "effects": ["sha256:" + "d" * 64],
    }
    execution_result_validator.validate(execution_result)
    for malformed_result in [
        {**execution_result, "plan_id": "sha256:" + "A" * 64},
        {
            **execution_result,
            "projection_digest": "sha256:" + "b" * 64,
        },
        {
            **execution_result,
            "precondition_token": "pre:9007199254740992:sha256:" + "c" * 64,
        },
        {
            **execution_result,
            "precondition_token": "pre:01:sha256:" + "c" * 64,
        },
        {**execution_result, "effects": ["sha256:" + "D" * 64]},
    ]:
        assert_invalid(
            execution_result_validator,
            malformed_result,
            "Engine completed result accepted malformed authority evidence",
        )
    effect_release_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/effectReleaseBoundary"
            )
        },
        registry=registry,
    )
    effect_reconciliation_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/effectReconciliationBoundary"
            )
        },
        registry=registry,
    )
    release_boundary = {
        "run_id": "run:fixture",
        "plan_id": "sha256:" + "a" * 64,
        "intent_ids": ["sha256:" + "b" * 64],
    }
    reconciliation_boundary = {
        "run_id": "run:fixture",
        "plan_id": "sha256:" + "a" * 64,
        "intent_id": "sha256:" + "b" * 64,
    }
    effect_release_validator.validate(release_boundary)
    effect_reconciliation_validator.validate(reconciliation_boundary)
    for validator, malformed in [
        (effect_release_validator, {**release_boundary, "plan_id": "not-a-content-id"}),
        (effect_release_validator, {**release_boundary, "intent_ids": ["not-a-content-id"]}),
        (
            effect_reconciliation_validator,
            {**reconciliation_boundary, "plan_id": "not-a-content-id"},
        ),
        (
            effect_reconciliation_validator,
            {**reconciliation_boundary, "intent_id": "not-a-content-id"},
        ),
    ]:
        assert_invalid(
            validator,
            malformed,
            "Engine effect boundary accepted a non-content identity",
        )
    success_correlation_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/successCorrelation"
            )
        },
        registry=registry,
    )
    success_tag_pairs = {
        "seal": "sealed",
        "verify": "verified",
        "seal_resource": "sealed_resource",
        "verify_wait_activation": "verified_wait_activation",
        "verify_durable_command": "verified_durable_command",
        "observe_clock": "clock_observed",
        "verify_evolution_command": "verified_evolution_command",
        "verify_live_evolution_command": "verified_live_evolution_command",
        "execute_durable": "durable_executed",
        "execute_live_evolution": "live_evolution_executed",
        "run": "execution_boundary",
    }
    for request_type, response_type in success_tag_pairs.items():
        correlated_tags = {
            "request": {"type": request_type},
            "response": {"type": response_type},
        }
        success_correlation_validator.validate(correlated_tags)
        for mismatched_response_type in success_tag_pairs.values():
            if mismatched_response_type == response_type:
                continue
            assert_invalid(
                success_correlation_validator,
                {
                    **correlated_tags,
                    "response": {"type": mismatched_response_type},
                },
                (
                    "Engine success correlation accepted "
                    f"{request_type} -> {mismatched_response_type}"
                ),
            )

    def validate_engine_request(request: object) -> None:
        engine_validator.validate(
            {"engine_protocol": "cymule.engine/5", "request": request}
        )

    def engine_success(request: object, response: object) -> dict[str, object]:
        return {
            "outcome": "success",
            "engine_protocol": "cymule.engine/5",
            "request": request,
            "response": response,
        }

    candidate_validator = Draft202012Validator(
        by_title["Cymule Plan Candidate cymule.ir/3"], registry=registry
    )
    candidate_paths = [
        root / "tests/fixtures/cross-language-plan.json",
        root / "examples/hello-world/flow.json",
    ]
    candidates = [load(path) for path in candidate_paths]
    for candidate in candidates:
        candidate_validator.validate(candidate)
    candidate = candidates[0]
    legacy_versions = load(root / "tests/fixtures/legacy-protocol-versions.json")
    assert_invalid(
        candidate_validator,
        {**candidate, "ir_version": legacy_versions["ir"]},
        "Plan schema accepted the superseded IR generation",
    )
    component = candidate["components"][0]
    for label, malformed_component in [
        (
            "missing",
            {
                key: value
                for key, value in component.items()
                if key != "output_artifact_kind"
            },
        ),
        ("null", {**component, "output_artifact_kind": None}),
    ]:
        malformed_candidate = json.loads(json.dumps(candidate))
        malformed_candidate["components"][0] = malformed_component
        assert_invalid(
            candidate_validator,
            malformed_candidate,
            f"Plan schema accepted a component with {label} output_artifact_kind",
        )
    scope_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/plan-candidate.schema.json#/$defs/scope"
            )
        },
        registry=registry,
    )
    scope = next(
        step
        for definition in candidate["definitions"]
        for step in definition["body"]["steps"]
        if step["op"] == "scope"
    )
    scope_validator.validate(scope)
    for legacy_mode in ["transactional", "speculative"]:
        assert_invalid(
            scope_validator,
            {**scope, "mode": legacy_mode},
            f"Plan schema accepted removed scope mode {legacy_mode}",
        )

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
    direct_run_request = {
        "type": "run",
        "plan": sealed,
        "input": None,
        "plugin": {
            "provider": "schema.process-provider",
            "process": {
                "executable": "/opt/cymule-test-adapter",
                "arguments": ["__plugin"],
                "environment": {},
                "working_directory": None,
                "runtime_closure": {"runtime": "sha256:" + "a" * 64},
                "timeout_ms": 1000,
                "message_limit": 8 * 1024 * 1024,
                "closure_limit": 67108864,
            },
        },
        "run_id": "界" * 512,
    }
    validate_engine_request(direct_run_request)
    for invalid_limit in (8 * 1024 * 1024 - 1, 8 * 1024 * 1024 + 1):
        invalid_run_limit = json.loads(json.dumps(direct_run_request))
        invalid_run_limit["plugin"]["process"]["message_limit"] = invalid_limit
        assert_invalid(
            engine_validator,
            {"engine_protocol": "cymule.engine/5", "request": invalid_run_limit},
            "ordinary Engine plugin accepted a narrowed or widened message limit",
        )
    for field, maximum, overflow in [
        ("arguments", [""] * 4096, [""] * 4097),
        (
            "environment",
            {f"ENTRY_{index}": "" for index in range(4096)},
            {f"ENTRY_{index}": "" for index in range(4097)},
        ),
        (
            "runtime_closure",
            {f"runtime-{index}": "sha256:" + "a" * 64 for index in range(4096)},
            {f"runtime-{index}": "sha256:" + "a" * 64 for index in range(4097)},
        ),
    ]:
        maximum_request = json.loads(json.dumps(direct_run_request))
        maximum_request["plugin"]["process"][field] = maximum
        validate_engine_request(maximum_request)
        overflow_request = json.loads(json.dumps(direct_run_request))
        overflow_request["plugin"]["process"][field] = overflow
        assert_invalid(
            engine_validator,
            {"engine_protocol": "cymule.engine/5", "request": overflow_request},
            f"Engine process {field} accepted 4097 entries",
        )
    for invalid_runtime in ["unix:darwin:arm64", "sha256:" + "A" * 64]:
        invalid_runtime_request = json.loads(json.dumps(direct_run_request))
        invalid_runtime_request["plugin"]["process"]["runtime_closure"] = {
            "runtime": invalid_runtime
        }
        assert_invalid(
            engine_validator,
            {"engine_protocol": "cymule.engine/5", "request": invalid_runtime_request},
            "Engine process runtime closure accepted a non-content identity",
        )
    for malformed_run_id in ["界" * 513, "run:\u0000forged", "run:\u0085forged"]:
        assert_invalid(
            engine_validator,
            {
                "engine_protocol": "cymule.engine/5",
                "request": {**direct_run_request, "run_id": malformed_run_id},
            },
            "Engine direct Run request accepted an invalid Run identity",
        )
    seal_request = {"type": "seal", "candidate": candidate}
    validate_engine_request(seal_request)
    sealed_response = engine_success(
        seal_request,
        {"type": "sealed", "plan": sealed},
    )
    engine_validator.validate(sealed_response)
    correlated_rpc = json.loads(
        subprocess.run(
            [str(engine), "rpc"],
            input=json.dumps(
                {"engine_protocol": "cymule.engine/5", "request": seal_request}
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    engine_validator.validate(correlated_rpc)
    if (
        correlated_rpc["outcome"] != "success"
        or correlated_rpc["request"] != seal_request
        or correlated_rpc["response"] != sealed_response["response"]
    ):
        raise AssertionError("Rust Engine did not correlate the executed inner request")
    missing_success_request = json.loads(json.dumps(sealed_response))
    del missing_success_request["request"]
    assert_invalid(
        engine_validator,
        missing_success_request,
        "Engine v5 schema accepted a success without its complete request",
    )
    engine_retry_matrix = {
        "transport_failure": {None},
        "validation": {"correct_and_retry"},
        "contract_violation": {"correct_and_retry", "never"},
        "admission_denied": {"correct_and_retry", "never"},
        "conflict": {"refresh_and_retry", "never"},
        "not_found": {None},
        "expected_plugin_failure": {"never"},
        "plugin_defect": {"never"},
        "substrate_failure": {"retry_same_request"},
        "cancelled": {"never"},
        "timed_out": {"retry_same_request", "refresh_and_retry"},
        "unknown_world_outcome": {"reconcile"},
    }
    engine_retry_dispositions = {
        None,
        "never",
        "correct_and_retry",
        "refresh_and_retry",
        "retry_same_request",
        "reconcile",
    }
    for category, allowed_dispositions in engine_retry_matrix.items():
        for disposition in engine_retry_dispositions:
            failure = {
                "outcome": "failure",
                "engine_protocol": "cymule.engine/5",
                "error": {
                    "category": category,
                    "phase": "transport",
                    "code": "fixture_failure",
                    "message": "shared structured Engine failure fixture",
                },
            }
            if disposition is not None:
                failure["error"]["retry_disposition"] = disposition
            if disposition in allowed_dispositions:
                engine_validator.validate(failure)
            else:
                assert_invalid(
                    engine_validator,
                    failure,
                    f"Engine failure accepted {category} with {disposition}",
                )
    transport_failure_with_null_retry = {
        "outcome": "failure",
        "engine_protocol": "cymule.engine/5",
        "error": {
            "category": "transport_failure",
            "phase": "transport",
            "code": "fixture_failure",
            "message": "shared structured Engine failure fixture",
            "retry_disposition": None,
        },
    }
    assert_invalid(
        engine_validator,
        transport_failure_with_null_retry,
        "Engine failure treated an explicit null retry disposition as omission",
    )
    bounded_contract_failure = {
        "outcome": "failure",
        "engine_protocol": "cymule.engine/5",
        "error": {
            "category": "contract_violation",
            "phase": "validate_request",
            "code": "contract_rejected",
            "message": "界" * 8192,
            "contract": "界" * 500,
            "path": "/" + "界" * 999,
            "issues": [
                {
                    "code": "界" * 200,
                    "message": "界" * 2000,
                    "path": "/" + "界" * 999,
                    "schema_path": "/" + "界" * 999,
                }
            ],
            "retry_disposition": "correct_and_retry",
        },
    }
    engine_validator.validate(bounded_contract_failure)
    for field, value in [
        ("message", "invalid\nmessage"),
        ("contract", "invalid\0contract"),
        ("path", "/invalid\npath"),
    ]:
        malformed_failure = json.loads(json.dumps(bounded_contract_failure))
        malformed_failure["error"][field] = value
        assert_invalid(
            engine_validator,
            malformed_failure,
            f"Engine failure accepted control characters in {field}",
        )
    for field, value in [
        ("code", "invalid\ncode"),
        ("message", "invalid\0message"),
        ("path", "/invalid\npath"),
        ("schema_path", "/invalid\0path"),
    ]:
        malformed_failure = json.loads(json.dumps(bounded_contract_failure))
        malformed_failure["error"]["issues"][0][field] = value
        assert_invalid(
            engine_validator,
            malformed_failure,
            f"Engine issue accepted control characters in {field}",
        )
    assert_invalid(
        engine_validator,
        {
            "outcome": "failure",
            "engine_protocol": "cymule.engine/5",
            "request": seal_request,
            "error": {
                "category": "validation",
                "phase": "validate_request",
                "code": "fixture_failure",
                "message": "failure envelopes do not claim a decoded request",
            },
        },
        "Engine v5 schema accepted a request on a failure envelope",
    )
    malformed_rpc = json.loads(
        subprocess.run(
            [str(engine), "rpc"],
            input=json.dumps(
                {
                    "engine_protocol": "cymule.engine/5",
                    "request": {"type": "seal", "candidate": candidate, "extra": True},
                }
            ),
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    engine_validator.validate(malformed_rpc)
    if (
        malformed_rpc["outcome"] != "failure"
        or malformed_rpc["error"]["category"] != "validation"
        or malformed_rpc["error"]["phase"] != "decode_request"
    ):
        raise AssertionError("Rust Engine did not return a structured decode failure")
    duplicate_request = json.dumps(
        {"engine_protocol": "cymule.engine/5", "request": {"type": "seal", "candidate": candidate}}
    ).replace(
        '"engine_protocol": "cymule.engine/5"',
        '"engine_protocol": "cymule.engine/5", "engine_protocol": "cymule.engine/5"',
        1,
    )
    duplicate_rpc = json.loads(
        subprocess.run(
            [str(engine), "rpc"],
            input=duplicate_request,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    if duplicate_rpc["outcome"] != "failure" or duplicate_rpc["error"]["code"] != "invalid_engine_request":
        raise AssertionError("Rust Engine accepted a duplicate JSON object key")

    plugin_validator = Draft202012Validator(
        by_title["Cymule Process Plugin Protocol cymule.plugin/3"], registry=registry
    )
    plugin_validator.validate({"type": "describe"})
    provider_attempt = {
        "attempt_version": "cymule.effect-provider-attempt/1",
        "attempt_id": "sha256:" + "1" * 64,
        "claim_owner": "provider:fixture",
        "claim_epoch": 1,
    }
    plugin_validator.validate(
        {
            "type": "dispatch_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": provider_attempt,
            "input": {"message": "fixture"},
        }
    )
    mathematical_attempt = {**provider_attempt, "claim_epoch": 1.0}
    plugin_validator.validate(
        {
            "type": "dispatch_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": mathematical_attempt,
            "input": {"message": "mathematical integer"},
        }
    )
    assert_invalid(
        plugin_validator,
        {
            "type": "dispatch_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": {**provider_attempt, "claim_epoch": 1.5},
            "input": {"message": "fractional integer field"},
        },
        "plugin schema accepted a fractional value for an integer field",
    )
    manifest = {
        "type": "manifest",
        "manifest": {
            "plugin_version": "cymule.plugin/3",
            "implementation_id": "schema.test-adapter",
            "components": {},
            "effects": {},
        },
    }
    plugin_validator.validate(manifest)
    for field in ("components", "effects"):
        maximum_manifest = json.loads(json.dumps(manifest))
        overflow_manifest = json.loads(json.dumps(manifest))
        maximum_manifest["manifest"][field] = {
            f"operation-{index}": (
                {"implementation_revision": "revision"}
                if field == "components"
                else {"implementation_revision": "revision", "can_reconcile": True}
            )
            for index in range(128)
        }
        overflow_manifest["manifest"][field] = {
            f"operation-{index}": (
                {"implementation_revision": "revision"}
                if field == "components"
                else {"implementation_revision": "revision", "can_reconcile": True}
            )
            for index in range(129)
        }
        plugin_validator.validate(maximum_manifest)
        assert_invalid(
            plugin_validator,
            overflow_manifest,
            f"plugin manifest accepted 129 {field}",
        )
    for missing in ["components", "effects"]:
        malformed_manifest = json.loads(json.dumps(manifest))
        del malformed_manifest["manifest"][missing]
        assert_invalid(
            plugin_validator,
            malformed_manifest,
            f"plugin schema accepted manifest without required {missing}",
        )
    for response_type in ["expected_failure", "defect"]:
        valid_failure = (
            {
                "type": "expected_failure",
                "error": {"code": "evaluation_rejected", "message": "🧭" * 2000},
            }
            if response_type == "expected_failure"
            else {
                "type": "defect",
                "code": "provider_unavailable",
                "message": "🧭" * 2000,
            }
        )
        plugin_validator.validate(valid_failure)
        for invalid_message in ["", "🧭" * 2001]:
            malformed_failure = json.loads(json.dumps(valid_failure))
            if response_type == "expected_failure":
                malformed_failure["error"]["message"] = invalid_message
            else:
                malformed_failure["message"] = invalid_message
            assert_invalid(
                plugin_validator,
                malformed_failure,
                f"plugin schema accepted {response_type} outside the Unicode scalar bound",
            )
    plugin_validator.validate(
        {
            "type": "reconcile_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": provider_attempt,
            "decision": "resolve_not_applied",
            "resolution_value": None,
            "input": {"message": "fixture"},
        }
    )
    plugin_validator.validate(
        {
            "type": "effect_result",
            "attempt": provider_attempt,
            "outcome": "not_applied",
            "value": None,
        }
    )
    for malformed_plugin_value in [
        {
            "type": "reconcile_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": provider_attempt,
            "decision": "observe",
            "resolution_value": {"forbidden": True},
            "input": {"message": "fixture"},
        },
        {
            "type": "reconcile_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "attempt": provider_attempt,
            "decision": "resolve_not_applied",
            "resolution_value": {"forbidden": True},
            "input": {"message": "fixture"},
        },
        {
            "type": "effect_result",
            "attempt": provider_attempt,
            "outcome": "not_applied",
            "value": {"forbidden": True},
        },
        {
            "type": "effect_result",
            "attempt": provider_attempt,
            "outcome": "unknown",
            "value": {"forbidden": True},
        },
        {
            "type": "reconciliation_result",
            "attempt": provider_attempt,
            "resolution": "resolved_not_applied",
            "value": {"forbidden": True},
        },
        {
            "type": "reconciliation_result",
            "attempt": provider_attempt,
            "resolution": "governance_required",
            "value": None,
        },
    ]:
        assert_invalid(
            plugin_validator,
            malformed_plugin_value,
            "plugin protocol accepted an outcome/value authority mismatch",
        )
    plugin_validator.validate(
        {
            "type": "reconciliation_result",
            "attempt": provider_attempt,
            "resolution": "resolved_not_applied",
            "value": None,
        }
    )
    assert_invalid(
        plugin_validator,
        {
            "type": "dispatch_effect",
            "operation": "test.capture",
            "intent_id": "sha256:" + "0" * 64,
            "input": {"message": "fixture"},
        },
        "plugin dispatch accepted no provider attempt",
    )

    resource_validator = Draft202012Validator(
        by_title["Cymule Resource Protocol cymule.resource/4"], registry=registry
    )
    resource_candidate = load(root / "tests/fixtures/resource-candidate.json")
    resource_validator.validate(resource_candidate)
    resource_validator.validate(
        {
            **resource_candidate,
            "media_type": "application/vnd.cymule.resource+json",
        }
    )
    assert_invalid(
        resource_validator,
        {
            **resource_candidate,
            "resource_version": legacy_versions["resource"],
        },
        "Resource schema accepted predecessor generation /3",
    )
    for invalid_media_type in [
        "text/\0plain",
        "text/",
        "/plain",
        "a/b/c",
        "Text/plain",
        "text/Plain",
        "text/plain;charset=utf-8",
        "text/ plain",
        "text/\u007fplain",
    ]:
        assert_invalid(
            resource_validator,
            {**resource_candidate, "media_type": invalid_media_type},
            f"Resource schema accepted invalid media type {invalid_media_type!r}",
        )
    assert_invalid(
        resource_validator,
        {**resource_candidate, "annotations": {}},
        "Resource schema accepted an explicit empty annotation map",
    )
    maximum_annotations = {
        **resource_candidate,
        "annotations": {f"annotation-{index}": "value" for index in range(64)},
    }
    resource_validator.validate(maximum_annotations)
    assert_invalid(
        resource_validator,
        {
            **resource_candidate,
            "annotations": {f"annotation-{index}": "value" for index in range(65)},
        },
        "Resource schema accepted 65 annotations",
    )
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
    maximum_locator_set = {
        "locator_version": "cymule.resource-locators/2",
        "resource_id": sealed_resource["resource_id"],
        "resolver_binding": "resolver:fixture",
        "locations": [
            {"kind": "opaque", "reference": f"provider-reference-{index}"}
            for index in range(16)
        ],
    }
    resource_validator.validate(maximum_locator_set)
    assert_invalid(
        resource_validator,
        {
            **maximum_locator_set,
            "locator_version": legacy_versions["resource_locators"],
        },
        "Resource schema accepted superseded locator-set generation /1",
    )
    assert_invalid(
        resource_validator,
        {
            **maximum_locator_set,
            "locations": [
                {"kind": "opaque", "reference": f"provider-reference-{index}"}
                for index in range(17)
            ],
        },
        "Resource schema accepted 17 locator entries",
    )
    maximum_url = "https://example.com/" + "a" * (8192 - len("https://example.com/"))
    resource_validator.validate(
        {
            **maximum_locator_set,
            "locations": [{"kind": "public_url", "url": maximum_url}],
        }
    )
    assert_invalid(
        resource_validator,
        {
            **maximum_locator_set,
            "locations": [{"kind": "public_url", "url": maximum_url + "a"}],
        },
        "Resource schema accepted a public URL above 8192 scalars",
    )
    validate_engine_request({"type": "seal_resource", "candidate": resource_candidate})
    list_proof = {
        "proof_version": "cymule.resource-list-proof/5",
        "manifest_digest": "sha256:" + "1" * 64,
        "entry_count": 1,
        "start_index": 0,
        "request_cursor_digest": "sha256:" + "2" * 64,
        "next_cursor_digest": "sha256:" + "3" * 64,
        "predecessor": None,
        "inclusions": [{"index": 0, "path": []}],
    }
    resource_validator.validate(list_proof)
    predecessor_proof = json.loads(json.dumps(list_proof))
    predecessor_proof["start_index"] = 1
    predecessor_proof["predecessor"] = {
        "entry": {"name": "before", "resource": sealed_resource},
        "inclusion": {"index": 0, "path": []},
    }
    resource_validator.validate(predecessor_proof)
    missing_predecessor = {
        key: value for key, value in list_proof.items() if key != "predecessor"
    }
    assert_invalid(
        resource_validator,
        missing_predecessor,
        "Resource schema accepted a list proof without required-nullable predecessor",
    )
    assert_invalid(
        resource_validator,
        {**list_proof, "proof_version": legacy_versions["resource_list_proof"]},
        "Resource schema accepted superseded list proof generation /4",
    )
    list_cursor = {
        "cursor_version": "cymule.resource-list-cursor/3",
        "cursor_id": "sha256:" + "4" * 64,
        "resource_id": "sha256:" + "5" * 64,
        "manifest_digest": "sha256:" + "1" * 64,
        "resolver_binding": "resolver:fixture",
        "request_cursor_digest": "sha256:" + "2" * 64,
        "request_limit": 1,
        "start_index": 0,
        "next_index": 1,
        "last_name": "entry.txt",
        "progress_digest": "sha256:" + "6" * 64,
    }
    resource_validator.validate(list_cursor)
    assert_invalid(
        resource_validator,
        {key: value for key, value in list_cursor.items() if key != "last_name"},
        "Resource schema accepted a list cursor without last_name",
    )
    assert_invalid(
        resource_validator,
        {**list_cursor, "cursor_version": legacy_versions["resource_list_cursor"]},
        "Resource schema accepted superseded list cursor generation /2",
    )
    resource_handoff = {
        "handoff_version": "cymule.resource-handoff/5",
        "transfer_id": "transfer:fixture",
        "producer": {
            "run_id": "run:producer",
            "occurrence_id": "occurrence:producer:result",
            "result": {
                "identity_version": "cymule.artifact/2",
                "artifact_id": "sha256:" + "0" * 64,
                "kind": "cymule.result/1",
            },
        },
        "to_run": "run:consumer",
        "slot": "input.resource",
        "resource": {
            "identity_version": "cymule.artifact/2",
            "artifact_id": "sha256:" + "0" * 64,
            "kind": "cymule.typed-json/sha256-" + "1" * 64,
        },
    }
    resource_validator.validate(resource_handoff)
    boundary_handoff = json.loads(json.dumps(resource_handoff))
    boundary_handoff["transfer_id"] = "传" * 512
    boundary_handoff["producer"]["run_id"] = "源" * 512
    boundary_handoff["producer"]["occurrence_id"] = "次" * 512
    boundary_handoff["to_run"] = "目" * 512
    boundary_handoff["slot"] = "槽" * 512
    resource_validator.validate(boundary_handoff)
    for malformed_handoff in [
        {
            **resource_handoff,
            "handoff_version": "unsupported-resource-handoff-version",
        },
        {
            **resource_handoff,
            "producer": {**resource_handoff["producer"], "run_id": "界" * 513},
        },
        {**resource_handoff, "to_run": "run:c1:\u0085"},
        {**resource_handoff, "transfer_id": "界" * 513},
        {
            **resource_handoff,
            "producer": {
                **resource_handoff["producer"],
                "occurrence_id": "occurrence:c1:\u0085",
            },
        },
        {**resource_handoff, "slot": "界" * 513},
    ]:
        assert_invalid(
            resource_validator,
            malformed_handoff,
            "Resource schema accepted a non-current version or malformed Run identity",
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
        by_title["Cymule Wait Activation cymule.wait-activation/2"],
        registry=registry,
    )
    activation_validator.validate(wait_activation)
    assert_invalid(
        activation_validator,
        {
            **wait_activation,
            "result": {
                **wait_activation["result"],
                "kind": "cymule.component-output/1",
            },
        },
        "wait activation accepted a non-wait-result Artifact kind",
    )
    for field in ["activation_id", "source.key"]:
        boundary = json.loads(json.dumps(wait_activation))
        if field == "activation_id":
            boundary["activation_id"] = "界" * 512
        else:
            boundary["source"]["key"] = "界" * 512
        activation_validator.validate(boundary)
        for malformed_identity in [
            "界" * 513,
            "activation:\u0000forged",
            "activation:\u0085forged",
        ]:
            malformed = json.loads(json.dumps(wait_activation))
            if field == "activation_id":
                malformed["activation_id"] = malformed_identity
            else:
                malformed["source"]["key"] = malformed_identity
            assert_invalid(
                activation_validator,
                malformed,
                f"wait activation accepted malformed {field} identity",
            )
    content_wait = json.loads(json.dumps(wait_activation))
    content_wait["wait_ids"] = ["sha256:" + "f" * 64]
    activation_validator.validate(content_wait)
    for legacy_wait_id in [
        "界" * 512,
        "界" * 513,
        "wait:" + "legacy",
        "A" * 64,
        "wait:\u0000forged",
        "wait:\u0085forged",
    ]:
        malformed = json.loads(json.dumps(wait_activation))
        malformed["wait_ids"] = [legacy_wait_id]
        assert_invalid(
            activation_validator,
            malformed,
            "wait activation accepted a non-content-addressed wait target",
        )
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
    validate_engine_request(
        {"type": "verify_wait_activation", "activation": wait_activation}
    )
    malformed_activation = dict(wait_activation)
    malformed_activation["provider"] = "must-not-enter-wait-activation"
    try:
        activation_validator.validate(malformed_activation)
    except ValidationError:
        pass
    else:
        raise AssertionError("wait activation schema accepted a provider field")
    for invalid_result in [
        {
            "artifact_id": wait_activation["result"]["artifact_id"],
            "kind": wait_activation["result"]["kind"],
        },
        {
            **wait_activation["result"],
            "identity_version": legacy_artifact["identity_version"],
        },
        {**wait_activation["result"], "artifact_id": "sha256:not-a-digest"},
        {**wait_activation["result"], "kind": "Invalid Kind"},
    ]:
        invalid_activation = {**wait_activation, "result": invalid_result}
        try:
            activation_validator.validate(invalid_activation)
        except ValidationError:
            pass
        else:
            raise AssertionError("wait activation accepted a non-v2 Artifact reference")

    wait_condition = load(root / "tests/fixtures/wait-condition.json")
    wait_condition_validator = Draft202012Validator(
        by_title["Cymule Durable Wait Condition"],
        registry=registry,
    )
    wait_condition_validator.validate(wait_condition)
    for malformed_wait in [
        {key: value for key, value in wait_condition.items() if key != "owner"},
        {**wait_condition, "wait_id": "wait:not-content-addressed"},
        {
            **wait_condition,
            "owner": {
                key: value
                for key, value in wait_condition["owner"].items()
                if key != "bind"
            },
        },
        {
            **wait_condition,
            "owner": {
                **wait_condition["owner"],
                "region_path": [9_007_199_254_740_992],
            },
        },
        {
            **wait_condition,
            "owner": {
                **wait_condition["owner"],
                "step_index": 9_007_199_254_740_992,
            },
        },
        {
            **wait_condition,
            "kind": {**wait_condition["kind"], "transport": "ambient"},
        },
    ]:
        assert_invalid(
            wait_condition_validator,
            malformed_wait,
            "durable wait schema accepted missing ownership",
        )

    artifact_contract_validator = Draft202012Validator(
        by_title[
            "Cymule Artifact Type Contract cymule.artifact-type-contract/1"
        ],
        registry=registry,
    )
    artifact_contract_validator.validate(
        {
            "contract_version": "cymule.artifact-type-contract/1",
            "artifact_kind": "example.value/1",
            "media_type": "application/json",
            "schema": {"type": "object"},
        }
    )

    durable_control = load(root / "tests/fixtures/durable-control.json")
    durable_cancel = load(root / "tests/fixtures/durable-cancel-control.json")
    durable_validator = Draft202012Validator(
        by_title["Cymule Durable Control cymule.durable-control/4"],
        registry=registry,
    )
    execution = durable_control["execution"]
    resolve_effect_command = {
        "type": "resolve_effect",
        "control_version": "cymule.durable-control/4",
        "resolution_id": "resolution:schema",
        "run_id": "run:fixture",
        "intent_id": "sha256:" + "f" * 64,
        "execution_binding": {
            "identity_version": "cymule.artifact/2",
            "artifact_id": "sha256:" + "d" * 64,
            "kind": "cymule.execution-binding/2",
        },
        "occurrence_binding": "sha256:" + "b" * 64,
        "claim_owner": "driver:schema",
        "claim_epoch": 1,
        "resolution": "resolved_not_applied",
        "value": None,
    }
    durable_variants = [
        {
            "type": "start_run",
            "control_version": "cymule.durable-control/4",
            "run_id": "run:fixture",
            "candidate": candidate,
            "input": {"message": "fixture"},
            "execution": execution,
        },
        {
            "type": "resume_run",
            "control_version": "cymule.durable-control/4",
            "run_id": "run:fixture",
            "execution": execution,
        },
        {
            "type": "activate_wait",
            "control_version": "cymule.durable-control/4",
            "activation_id": "activation:fixture",
            "source": {"kind": "signal", "key": "signal:fixture"},
            "wait_ids": ["sha256:" + "7" * 64],
            "value": {"accepted": True},
        },
        {
            "type": "release_effect",
            "control_version": "cymule.durable-control/4",
            "intent_id": "sha256:" + "f" * 64,
            "execution": execution,
        },
        durable_cancel,
        {
            "type": "run_index_page",
            "control_version": "cymule.durable-control/4",
            "expected_revision": None,
            "cursor": None,
            "limit": 32,
            "max_canonical_bytes": 65536,
        },
        {
            "type": "run_current",
            "control_version": "cymule.durable-control/4",
            "run_id": "run:fixture",
            "expected_revision": None,
        },
        *[
            {
                "type": command_type,
                "control_version": "cymule.durable-control/4",
                "run_id": "run:fixture",
                "expected_revision": None,
                "cursor": None,
                "limit": 32,
                "max_canonical_bytes": 65536,
            }
            for command_type in (
                "run_wait_page",
                "run_effect_page",
                "run_occurrence_page",
                "run_attempt_page",
            )
        ],
        {
            "type": "run_item",
            "control_version": "cymule.durable-control/4",
            "run_id": "run:fixture",
            "expected_revision": None,
            "selector": {"kind": "wait", "wait_id": "sha256:" + "7" * 64},
            "max_canonical_bytes": 131072,
        },
        durable_control,
        resolve_effect_command,
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
    assert_invalid(
        durable_validator,
        {
            **next(
                command
                for command in durable_variants
                if command["type"] == "activate_wait"
            ),
            "wait_ids": ["wait:not-content-addressed"],
        },
        "durable wait activation accepted a non-content wait identity",
    )
    missing_expected_revision = dict(
        next(
            command
            for command in durable_variants
            if command["type"] == "run_current"
        )
    )
    del missing_expected_revision["expected_revision"]
    assert_invalid(
        durable_validator,
        missing_expected_revision,
        "durable Run-current query omitted required-null expected_revision",
    )
    legacy_query = {
        "type": "query_run",
        "control_version": legacy_versions["durable_control"],
        "query_id": "query:legacy",
        "run_id": "run:fixture",
    }
    assert_invalid(
        durable_validator,
        legacy_query,
        "durable-control/4 admitted the legacy unbounded Run query",
    )
    validate_engine_request(
        {"type": "verify_durable_command", "command": durable_control}
    )
    clock_target = {
        "provider": "cymule.clock-system/2",
        "location": "/tmp/cymule-schema-clock.sqlite",
        "source_id": execution["clock"]["source_id"],
        "source_generation": execution["clock"]["source_generation"],
    }
    clock_request = {
        "type": "observe_clock",
        "target": clock_target,
        "run_id": durable_control["run_id"],
    }
    validate_engine_request(clock_request)
    assert_invalid(
        engine_validator,
        engine_success(clock_request, {"type": "verified"}),
        "Engine schema accepted observe_clock paired with verified",
    )
    engine_validator.validate(
        engine_success(
            clock_request,
            {
                "type": "clock_observed",
                "result": {
                    "run_id": durable_control["run_id"],
                    "observation": execution["clock"],
                },
            },
        )
    )
    for malformed_clock_success in [
        {"type": "clock_observed", "observation": execution["clock"]},
        {
            "type": "clock_observed",
            "result": {"observation": execution["clock"]},
        },
        {
            "type": "clock_observed",
            "result": {
                "run_id": "run:\u0000forged",
                "observation": execution["clock"],
            },
        },
    ]:
        assert_invalid(
            engine_validator,
            engine_success(clock_request, malformed_clock_success),
            "Engine Clock success accepted an unbound Run observation result",
        )
    for malformed_execution in [
        {**execution, "clock": {**execution["clock"], "clock_version": "unsupported"}},
        {**execution, "clock": {**execution["clock"], "logical_time": 10}},
        {**execution, "owner": "driver:\u0000forged"},
        {**execution, "owner": "driver:\u0085forged"},
        {**execution, "owner": "é" * 513},
        {**execution, "ttl": 9_007_199_254_740_992},
    ]:
        assert_invalid(
            durable_validator,
            {**durable_control, "execution": malformed_execution},
            "durable control accepted malformed Clock evidence",
        )
    durable_validator.validate(
        {
            **durable_control,
            "expected_fence": 9_007_199_254_740_991,
            "execution": {**execution, "ttl": 9_007_199_254_740_991},
        }
    )
    durable_validator.validate(
        {**durable_control, "execution": {**execution, "owner": "é" * 512}}
    )
    assert_invalid(
        durable_validator,
        {**durable_control, "expected_fence": 9_007_199_254_740_992},
        "durable takeover accepted an unsafe fence",
    )
    assert_invalid(
        durable_validator,
        {**resolve_effect_command, "occurrence_binding": "binding:not-content-addressed"},
        "durable Effect resolution accepted a non-content occurrence binding",
    )
    durable_run_engine_request = {
        "type": "execute_durable",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-domain",
            }
        },
        "command": durable_control,
    }
    validate_engine_request(durable_run_engine_request)
    unicode_durable_run_request = json.loads(json.dumps(durable_run_engine_request))
    unicode_durable_run_request["command"]["run_id"] = "界" * 512
    validate_engine_request(unicode_durable_run_request)
    for malformed_run_id in ["界" * 513, "run:\u0000forged", "run:\u0085forged"]:
        malformed_durable_run_request = json.loads(
            json.dumps(durable_run_engine_request)
        )
        malformed_durable_run_request["command"]["run_id"] = malformed_run_id
        assert_invalid(
            engine_validator,
            {
                "engine_protocol": "cymule.engine/5",
                "request": malformed_durable_run_request,
            },
            "Engine execute_durable accepted an invalid Run identity",
        )
    validate_engine_request(
        {
            "type": "execute_durable",
            "target": {
                "store": {
                "provider": "cymule.directory-store/5",
                    "location": "/tmp/cymule-schema-domain",
                }
            },
            "command": durable_variants[2],
        }
    )
    durable_domain_request = {
        "type": "execute_durable",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-domain",
            }
        },
        "command": durable_variants[5],
    }
    validate_engine_request(durable_domain_request)
    null_executor_request = json.loads(json.dumps(durable_domain_request))
    null_executor_request["target"]["executor"] = None
    assert_invalid(
        engine_validator,
        {"engine_protocol": "cymule.engine/5", "request": null_executor_request},
        "Engine request schema treated an explicit null executor as omission",
    )
    engine_validator.validate(
        engine_success(
            durable_domain_request,
            {
                "type": "durable_executed",
                "response": {
                    "type": "run_index_page",
                    "page": {
                        "observed_revision": "sha256:" + "a" * 64,
                        "source_root": "b" * 64,
                        "items": [],
                        "next_cursor": None,
                    },
                },
            },
        )
    )
    assert_invalid(
        engine_validator,
        engine_success(
            durable_domain_request,
            {
                "type": "durable_executed",
                "response": {
                    "type": "domain",
                    "domain": {"revision": None, "run_ids": []},
                },
            },
        ),
        "Engine schema accepted the removed unbounded durable domain response",
    )
    malformed_durable = dict(durable_control)
    malformed_durable["provider"] = "must-not-enter-durable-control"
    try:
        durable_validator.validate(malformed_durable)
    except ValidationError:
        pass
    else:
        raise AssertionError("durable control schema accepted a provider field")

    component_outcome_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/componentOutcome"},
        registry=registry,
    )
    occurrence_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/componentOccurrence"},
        registry=registry,
    )
    attempt_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/operationAttempt"},
        registry=registry,
    )
    succeeded = {"outcome": "succeeded", "output": canonical_artifact}
    expected_failure = {
        "outcome": "expected_failure",
        "code": "fixture_rejected",
        "detail": canonical_artifact,
    }
    component_outcome_validator.validate(succeeded)
    component_outcome_validator.validate(expected_failure)
    pending_occurrence = {
        "occurrence_version": "cymule.component-occurrence/4",
        "occurrence_id": "sha256:" + "0" * 64,
        "run_id": "run:fixture",
        "plan_id": "sha256:" + "1" * 64,
        "binding_context": "sha256:" + "2" * 64,
        "invocation_id": "main",
        "invocation_path": [],
        "definition_id": "main",
        "region_path": [],
        "site_id": "call:fixture",
        "step_index": 0,
        "component": "test.echo",
        "input": canonical_artifact,
        "outcome": None,
        "occurrence_binding": "sha256:" + "b" * 64,
        "implementation_revision": "sha256:" + "3" * 64,
        "attempt_count": 1,
        "latest_attempt_id": "sha256:" + "5" * 64,
        "continuation_digest": None,
        "state": "pending",
    }
    completed_occurrence = {
        **pending_occurrence,
        "outcome": expected_failure,
        "continuation_digest": "4" * 64,
        "state": "completed",
    }
    occurrence_validator.validate(pending_occurrence)
    occurrence_validator.validate(completed_occurrence)
    running_attempt = {
        "attempt_version": "cymule.operation-attempt/2",
        "attempt_id": "sha256:" + "5" * 64,
        "occurrence_id": pending_occurrence["occurrence_id"],
        "run_id": pending_occurrence["run_id"],
        "attempt_ordinal": 1,
        "previous_attempt_id": None,
        "continuation_attempt_id": "sha256:" + "6" * 64,
        "execution_claim_owner": "executor:fixture",
        "execution_claim_fence": 1,
        "operation_occurrence_binding": "sha256:" + "b" * 64,
        "transport_request_id": "sha256:" + "7" * 64,
        "state": "running",
        "outcome": None,
    }
    completed_attempt = {**running_attempt, "state": "completed", "outcome": succeeded}
    attempt_validator.validate(running_attempt)
    attempt_validator.validate(completed_attempt)
    for validator, malformed, message in [
        (occurrence_validator, {**pending_occurrence, "outcome": succeeded}, "pending occurrence accepted an outcome"),
        (occurrence_validator, {**completed_occurrence, "outcome": None}, "completed occurrence accepted a null outcome"),
        (occurrence_validator, {**completed_occurrence, "output": canonical_artifact}, "component occurrence accepted the legacy output field"),
        (occurrence_validator, {**pending_occurrence, "occurrence_version": legacy_versions["component_occurrence"]}, "component occurrence accepted the retired generation"),
        (occurrence_validator, {key: value for key, value in pending_occurrence.items() if key != "attempt_count"}, "component occurrence accepted a missing Attempt count"),
        (occurrence_validator, {**pending_occurrence, "attempt_count": 0}, "component occurrence accepted a zero Attempt count"),
        (occurrence_validator, {**pending_occurrence, "latest_attempt_id": "attempt:not-content-addressed"}, "component occurrence accepted a malformed latest Attempt"),
        (attempt_validator, {**running_attempt, "outcome": succeeded}, "running Attempt accepted an outcome"),
        (attempt_validator, {**completed_attempt, "outcome": None}, "completed Attempt accepted a null outcome"),
        (attempt_validator, {**running_attempt, "attempt_version": legacy_versions["operation_attempt"]}, "operation Attempt accepted the retired generation"),
        (attempt_validator, {key: value for key, value in running_attempt.items() if key != "previous_attempt_id"}, "operation Attempt accepted a missing predecessor"),
        (attempt_validator, {**running_attempt, "previous_attempt_id": "sha256:" + "8" * 64}, "first operation Attempt accepted a predecessor"),
        (attempt_validator, {**running_attempt, "attempt_ordinal": 2, "previous_attempt_id": None}, "later operation Attempt accepted a missing predecessor"),
    ]:
        assert_invalid(validator, malformed, message)
    for field in (
        "attempt_id",
        "occurrence_id",
        "continuation_attempt_id",
        "transport_request_id",
    ):
        assert_invalid(
            attempt_validator,
            {**running_attempt, field: f"not-a-content-id:{field}"},
            f"operation Attempt accepted malformed {field}",
        )
    assert_invalid(
        occurrence_validator,
        {**pending_occurrence, "occurrence_id": "occurrence:not-content-addressed"},
        "component occurrence accepted a non-content identity",
    )

    storage_delta_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/durable-storage.schema.json#/$defs/durable_delta"},
        registry=registry,
    )
    full_clock_observation = {
        **execution["clock"],
        "logical_time": 10,
        "observed_unix_ms": 1_700_000_000_000,
    }
    storage_delta_validator.validate(
        {
            "operations": [
                {"op": "put_clock_observation", "value": full_clock_observation},
                {"op": "put_component_occurrence", "value": pending_occurrence},
                {"op": "put_operation_attempt", "value": running_attempt},
            ]
        }
    )
    assert_invalid(
        storage_delta_validator,
        {"operations": [{"op": "put_clock_observation", "value": execution["clock"]}]},
        "durable storage accepted a Clock reference as a complete receipt",
    )
    execution_binding = {
        "identity_version": "cymule.artifact/2",
        "artifact_id": "sha256:" + "8" * 64,
        "kind": "cymule.execution-binding/2",
    }
    claim = {
        "claim_version": "cymule.continuation-execution-claim/1",
        "run_id": "run:fixture",
        "continuation_id": "sha256:" + "a" * 64,
        "owner": "driver:fixture",
        "continuation_attempt_id": "sha256:" + "9" * 64,
        "fence": 1,
        "plan_id": "sha256:" + "1" * 64,
        "execution_binding_ref": execution_binding,
        "clock_observation_ref": execution["clock"],
        "logical_acquired_at": 10,
        "logical_ttl": 30,
        "logical_expires_at": 40,
    }
    continuation = {
        "continuation_version": "cymule.continuation-state/1",
        "run_id": "run:fixture",
        "plan_id": claim["plan_id"],
        "binding_context": execution_binding["artifact_id"],
        "frames": [{
            "definition_id": "main",
            "invocation_id": "sha256:" + "a" * 64,
            "invocation_path": [],
            "scope_id": "scope:root",
            "input": canonical_artifact,
            "region_path": [],
            "next_step": 0,
            "locals": {},
        }],
        "state": None,
        "wait_set": [],
        "scope_stack": ["scope:root"],
        "epoch": 0,
        "execution_fence": 1,
        "execution_claim": claim,
        "status": "running",
    }
    execution_claim_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/executionClaim"
            )
        },
        registry=registry,
    )
    for field, malformed_identity in [
        ("continuation_id", "continuation:run:fixture"),
        ("continuation_id", "sha256:" + "A" * 64),
        ("continuation_attempt_id", "attempt:fixture"),
        ("continuation_attempt_id", "sha256:" + "A" * 64),
        ("plan_id", "plan:fixture"),
        ("plan_id", "sha256:" + "A" * 64),
    ]:
        assert_invalid(
            execution_claim_validator,
            {**claim, field: malformed_identity},
            f"Engine execution claim accepted a malformed {field} content ID",
        )
    continuation_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/continuation"},
        registry=registry,
    )
    continuation_validator.validate(continuation)
    for invalid_version in (None, "cymule.continuation-state/0"):
        malformed_continuation = {**continuation}
        if invalid_version is None:
            del malformed_continuation["continuation_version"]
        else:
            malformed_continuation["continuation_version"] = invalid_version
        assert_invalid(
            continuation_validator,
            malformed_continuation,
            "Engine schema accepted an invalid Continuation generation",
        )
    ready_continuation = {**continuation, "execution_claim": None, "status": "ready"}
    continuation_validator.validate(ready_continuation)
    waiting_continuation = {
        **ready_continuation,
        "wait_set": ["wait:fixture"],
        "status": "waiting",
    }
    continuation_validator.validate(waiting_continuation)
    assert_invalid(
        continuation_validator,
        {**continuation, "execution_claim": None},
        "running Continuation accepted a null execution claim",
    )
    assert_invalid(
        continuation_validator,
        {**ready_continuation, "execution_claim": claim},
        "ready Continuation accepted an execution claim",
    )
    assert_invalid(
        continuation_validator,
        {**ready_continuation, "wait_set": ["wait:fixture"]},
        "ready Continuation accepted a non-empty wait set",
    )
    assert_invalid(
        continuation_validator,
        {**waiting_continuation, "wait_set": []},
        "waiting Continuation accepted an empty wait set",
    )
    durable_run_current_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/durableRunCurrent"},
        registry=registry,
    )
    durable_run_current = {
        "run_id": "run:fixture",
        "plan_id": "sha256:" + "b" * 64,
        "execution_binding": {
            **canonical_artifact,
            "artifact_id": "sha256:" + "d" * 64,
            "kind": "cymule.execution-binding/2",
        },
        "continuation_status": "ready",
        "epoch": 0,
        "execution_fence": 0,
        "result": None,
        "execution_status": {"status": "active"},
        "world_settlement": "settled",
    }
    durable_run_current_validator.validate(durable_run_current)
    assert_invalid(
        durable_run_current_validator,
        {**durable_run_current, "execution_status": {"status": "completed"}},
        "Run current accepted a Continuation/execution-status mismatch",
    )
    completed_effect = {
        "intent_id": "sha256:" + "e" * 64,
        "run_id": "run:fixture",
        "origin_plan_id": "sha256:" + "c" * 64,
        "operation": "effect:fixture",
        "input": canonical_artifact,
        "execution_binding": {
            **canonical_artifact,
            "artifact_id": "sha256:" + "d" * 64,
            "kind": "cymule.execution-binding/2",
        },
        "occurrence_binding": "sha256:" + "b" * 64,
        "execution_availability": "available",
        "reconciliation": "not_required",
        "state": "applied",
        "claim_epoch": 1,
        "claim_owner": "driver:fixture",
        "result": {**canonical_artifact, "kind": "cymule.effect-result/1"},
    }
    effect_dispatch_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/effectDispatch"
            )
        },
        registry=registry,
    )
    effect_dispatch_validator.validate(completed_effect)
    assert_invalid(
        effect_dispatch_validator,
        {**completed_effect, "result": None},
        "Applied Effect dispatch omitted its required result Artifact",
    )
    assert_invalid(
        effect_dispatch_validator,
        {**completed_effect, "result": canonical_artifact},
        "Applied Effect dispatch accepted a result with the wrong Artifact kind",
    )
    durable_boundary_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/durableBoundary"
            )
        },
        registry=registry,
    )
    effect_not_applied_boundary = {
        "status": "effect_not_applied",
        "intent_id": "sha256:" + "f" * 64,
    }
    durable_boundary_validator.validate(effect_not_applied_boundary)
    assert_invalid(
        durable_boundary_validator,
        {**effect_not_applied_boundary, "intent_id": "effect:not-content-addressed"},
        "effect-not-applied boundary accepted a non-content intent identity",
    )
    effect_resolution_receipt_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/engine-protocol.schema.json"
                "#/$defs/effectResolutionReceipt"
            )
        },
        registry=registry,
    )
    effect_resolution_receipt = {
        "receipt_version": "cymule.effect-resolution-receipt/1",
        "command": {
            "resolution_id": "resolution:schema",
            "run_id": "run:fixture",
            "intent_id": "sha256:" + "f" * 64,
            "execution_binding": {
                **canonical_artifact,
                "artifact_id": "sha256:" + "d" * 64,
                "kind": "cymule.execution-binding/2",
            },
            "occurrence_binding": "sha256:" + "b" * 64,
            "claim_owner": "driver:schema",
            "claim_epoch": 1,
            "resolution": "resolved_not_applied",
            "value": None,
        },
        "actual_resolution": "resolved_not_applied",
        "actual_value": None,
        "result": None,
        "receipt_id": "a" * 64,
    }
    effect_resolution_receipt_validator.validate(effect_resolution_receipt)
    applied_null_receipt = {
        **effect_resolution_receipt,
        "actual_resolution": "resolved_applied",
        "actual_value": None,
        "result": {**canonical_artifact, "kind": "cymule.effect-result/1"},
    }
    effect_resolution_receipt_validator.validate(applied_null_receipt)
    assert_invalid(
        effect_resolution_receipt_validator,
        {**applied_null_receipt, "result": None},
        "Applied JSON null omitted its canonical result Artifact",
    )
    assert_invalid(
        effect_resolution_receipt_validator,
        {**effect_resolution_receipt, "world_settlement": "settled"},
        "Effect resolution receipt accepted duplicated mutable Run settlement",
    )
    not_applied_effect = {
        **completed_effect,
        "state": "not_applied",
        "result": None,
    }
    effect_dispatch_validator.validate(not_applied_effect)
    assert_invalid(
        effect_dispatch_validator,
        {**not_applied_effect, "result": canonical_artifact},
        "not-applied Effect accepted a result Artifact",
    )
    completed_run_current = {
        **durable_run_current,
        "continuation_status": "completed",
        "result": canonical_artifact,
        "execution_status": {"status": "completed"},
        "world_settlement": "settled",
    }
    durable_run_current_validator.validate(completed_run_current)
    assert_invalid(
        durable_run_current_validator,
        {**completed_run_current, "world_settlement": "unknown"},
        "completed Run current accepted non-settled world state",
    )
    durable_run_current_validator.validate(
        {
            **completed_run_current,
            "continuation_status": "cancelled",
            "result": None,
            "execution_status": {
                "status": "cancelled",
                "reason": canonical_artifact,
            },
            "world_settlement": "unknown",
        }
    )
    storage_delta_validator.validate(
        {"operations": [{"op": "put_continuation", "value": continuation}]}
    )
    durable_state_validator = Draft202012Validator(
        {"$ref": "https://cymule.dev/schemas/durable-storage.schema.json#/$defs/durable_state"},
        registry=registry,
    )
    durable_state = {
        "durable_version": "cymule.durable-state/7",
        "machine": {
            "snapshot_version": "cymule.machine-snapshot/11",
            "plans": [],
            "artifacts": [],
            "batches": [],
            "events": [],
            "admissions": [],
            "commands": {},
            "command_index_proofs": {},
        },
        "continuations": {"run:fixture": ready_continuation},
        "waits": {},
        "leases": {},
        "outbox": {},
        "component_occurrences": {},
        "operation_attempts": {},
        "clock_observations": {full_clock_observation["observation_id"]: full_clock_observation},
        "snapshots": {},
    }
    durable_state_validator.validate(durable_state)
    assert_invalid(
        durable_state_validator,
        {
            **durable_state,
            "durable_version": legacy_versions["durable_state"],
        },
        "durable state accepted the superseded pre-StateRoot generation",
    )
    assert_invalid(
        durable_state_validator,
        {
            **durable_state,
            "machine": {
                **durable_state["machine"],
                "snapshot_version": legacy_versions["machine_snapshot"],
            },
        },
        "durable state accepted the superseded Machine snapshot generation",
    )
    assert_invalid(
        durable_state_validator,
        {**durable_state, "application_journal_compacted_records": {}},
        "durable state accepted the superseded compacted-record authority",
    )
    for removed_history_map in (
        "application_journal_record_manifests",
        "application_journal_prefix_replacement_history",
        "coupled_checkpoint_receipts",
    ):
        assert_invalid(
            durable_state_validator,
            {**durable_state, removed_history_map: {}},
            f"materialized durable state accepted removed {removed_history_map}",
        )
    assert_invalid(
        durable_state_validator,
        {**durable_state, "wait_activations": {}},
        "durable state accepted non-canonical explicit empty wait_activations",
    )
    assert_invalid(
        durable_state_validator,
        {**durable_state, "machine": {**durable_state["machine"], "unknown": True}},
        "durable state accepted an open Machine snapshot envelope",
    )
    assert_invalid(
        durable_state_validator,
        {**durable_state, "clock_observations": {execution["clock"]["observation_id"]: execution["clock"]}},
        "durable checkpoint state accepted a Clock reference as a full receipt",
    )
    coupled_record = {
        "record_id": "record:coupled:fixture",
        "schema": "test.coupled/1",
        "payload": {"terminal": True},
        "content_digest": "1" * 64,
    }
    coupled_batch = {
        "journal_id": "journal:coupled:fixture",
        "records": [
            {
                "record_id": coupled_record["record_id"],
                "schema": coupled_record["schema"],
                "content_digest": coupled_record["content_digest"],
                "record_digest": "6" * 64,
            }
        ],
    }
    coupled_checkpoint_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/coupled_checkpoint"
            )
        },
        registry=registry,
    )
    pending_coupled_effect = {
        **completed_effect,
        "state": "pending",
        "claim_epoch": 0,
        "claim_owner": None,
        "result": None,
    }
    coupled_effect_enqueue = {
        "kind": "journal_effect_enqueue",
        "machine_authority_root": "9" * 64,
        "continuation": continuation,
        "dispatch": pending_coupled_effect,
        "journal": coupled_batch,
    }
    coupled_checkpoint_validator.validate(coupled_effect_enqueue)
    coupled_input_wait = {
        "kind": "input_wait_journals",
        "machine_authority_root": "9" * 64,
        "suspension_receipt_id": "sha256:" + "8" * 64,
        "result": canonical_artifact,
        "wait_id": "sha256:" + "7" * 64,
        "journals": [coupled_batch],
    }
    coupled_checkpoint_validator.validate(coupled_input_wait)
    for malformed_coupled_input, message in [
        (
            {**coupled_input_wait, "wait_id": "wait:not-content-addressed"},
            "coupled checkpoint accepted a non-content wait identity",
        ),
        (
            {
                key: value
                for key, value in coupled_input_wait.items()
                if key != "suspension_receipt_id"
            },
            "coupled input completion omitted its suspension receipt",
        ),
        (
            {
                **coupled_input_wait,
                "suspension_receipt_id": "receipt:not-content-addressed",
            },
            "coupled input completion accepted a non-content suspension receipt",
        ),
        (
            {
                **coupled_input_wait,
                "journals": [
                    coupled_batch,
                    {**coupled_batch, "journal_id": "journal:second:fixture"},
                ],
            },
            "coupled input completion accepted more than one owning journal",
        ),
    ]:
        assert_invalid(
            coupled_checkpoint_validator,
            malformed_coupled_input,
            message,
        )
    assert_invalid(
        coupled_checkpoint_validator,
        {
            "kind": "journal_wait_completion",
            "wait_id": "sha256:" + "7" * 64,
            "result": canonical_artifact,
            "journal": coupled_batch,
        },
        "coupled checkpoint accepted the removed standalone wait completion",
    )
    resource_handoff_input = {
        "kind": "resource_handoff_input",
        "machine_authority_root": "9" * 64,
        "transfer_id": "transfer:fixture",
        "activation_id": "sha256:" + "a" * 64,
        "source_coupling_id": "sha256:" + "b" * 64,
        "source_receipt_id": "sha256:" + "c" * 64,
        "run_id": "run:fixture",
        "owner": {
            "invocation_id": "invocation:fixture",
            "definition_id": "definition:fixture",
            "site_id": "site:fixture",
            "region_path": [0],
            "step_index": 0,
            "bind": "resource_handle",
        },
        "wait_id": "sha256:" + "d" * 64,
        "result": canonical_artifact,
        "activation_authority": {
            **coupled_batch,
            "journal_id": "journal:resource-activation-authority",
        },
        "activation_index": {
            **coupled_batch,
            "journal_id": "journal:resource-activation-index",
        },
    }
    coupled_checkpoint_validator.validate(resource_handoff_input)
    for required_field in resource_handoff_input:
        malformed_handoff = dict(resource_handoff_input)
        del malformed_handoff[required_field]
        assert_invalid(
            coupled_checkpoint_validator,
            malformed_handoff,
            f"resource handoff checkpoint omitted required {required_field}",
        )
    for identity_field in (
        "activation_id",
        "source_coupling_id",
        "source_receipt_id",
        "wait_id",
    ):
        assert_invalid(
            coupled_checkpoint_validator,
            {**resource_handoff_input, identity_field: f"legacy:{identity_field}"},
            f"resource handoff checkpoint accepted malformed {identity_field}",
        )
    handoff_without_owner_bind = json.loads(json.dumps(resource_handoff_input))
    del handoff_without_owner_bind["owner"]["bind"]
    assert_invalid(
        coupled_checkpoint_validator,
        handoff_without_owner_bind,
        "resource handoff checkpoint omitted required-nullable owner bind",
    )
    assert_invalid(
        coupled_checkpoint_validator,
        {**resource_handoff_input, "journals": [coupled_batch]},
        "resource handoff checkpoint accepted a generic journal collection",
    )
    legacy_machine_digest = dict(coupled_effect_enqueue)
    legacy_machine_digest["machine_digest"] = legacy_machine_digest.pop(
        "machine_authority_root"
    )
    assert_invalid(
        coupled_checkpoint_validator,
        legacy_machine_digest,
        "coupled checkpoint accepted the superseded Machine digest field",
    )
    coupled_variants = by_title["Cymule Durable Storage Records"]["$defs"][
        "coupled_checkpoint"
    ]["oneOf"]
    for coupled_kind in (
        "journal_effect_enqueue",
        "journal_effect_settlement",
        "wait_activation_journals",
        "input_wait_journals",
        "resource_handoff_input",
    ):
        coupled_variant = next(
            variant
            for variant in coupled_variants
            if variant["properties"]["kind"].get("const") == coupled_kind
        )
        if (
            "machine_authority_root" not in coupled_variant["required"]
            or "machine_digest" in coupled_variant["properties"]
        ):
            raise AssertionError(
                f"coupled checkpoint {coupled_kind} did not hard-cut Machine authority"
            )
    coupled_receipt = {
        "receipt_version": "cymule.coupled-checkpoint-receipt/3",
        "coupling_id": "sha256:" + "2" * 64,
        "checkpoint": {
            "kind": "journal_set",
            "coupling_key": "sha256:" + "2" * 64,
            "source_revision": "sha256:" + "3" * 64,
            "result_revision": "sha256:" + "4" * 64,
            "manifest": [coupled_batch],
        },
        "receipt_id": "sha256:" + "5" * 64,
    }
    storage_delta_validator.validate(
        {
            "operations": [
                {
                    "op": "remove_clock_observation",
                    "observation_id": full_clock_observation["observation_id"],
                },
                {"op": "put_coupled_checkpoint_receipt", "value": coupled_receipt},
            ]
        }
    )
    assert_invalid(
        storage_delta_validator,
        {
            "operations": [
                {
                    "op": "put_coupled_checkpoint_receipt",
                    "value": {
                        **coupled_receipt,
                        "receipt_version": legacy_versions["coupled_checkpoint_receipt"],
                    },
                }
            ]
        },
        "durable storage accepted the superseded coupled receipt generation",
    )
    assert_invalid(
        storage_delta_validator,
        {
            "operations": [
                {
                    "op": "put_coupled_checkpoint_receipt",
                    "value": {**coupled_receipt, "unknown": True},
                }
            ]
        },
        "durable storage accepted an open coupled checkpoint receipt",
    )

    durable_storage_schema = by_title["Cymule Durable Storage Records"]
    durable_storage_validator = Draft202012Validator(
        durable_storage_schema,
        registry=registry,
    )
    storage_fixture = load(
        root / "tests/harness/fixtures/durable-storage-state-root.json"
    )
    head_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/head"
            )
        },
        registry=registry,
    )
    state_root_manifest_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/state_root_manifest"
            )
        },
        registry=registry,
    )
    machine_base_anchor_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/machine_base_anchor"
            )
        },
        registry=registry,
    )
    state_root_object_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/state_root_object"
            )
        },
        registry=registry,
    )
    gc_receipt_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/gc_receipt"
            )
        },
        registry=registry,
    )
    command_archive_object_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/command_archive_object"
            )
        },
        registry=registry,
    )
    command_envelope_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/command_envelope"
            )
        },
        registry=registry,
    )
    core_event_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/core_event"
            )
        },
        registry=registry,
    )
    scope_projection_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/scope_projection"
            )
        },
        registry=registry,
    )
    run_projection_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/run_projection"
            )
        },
        registry=registry,
    )
    history_compaction_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/history_compaction"
            )
        },
        registry=registry,
    )

    physical_head = storage_fixture["head"]
    head_validator.validate(physical_head)
    durable_storage_validator.validate(physical_head)
    for required_nullable in ("machine_base_anchor", "gc_receipt"):
        malformed_head = dict(physical_head)
        del malformed_head[required_nullable]
        assert_invalid(
            head_validator,
            malformed_head,
            f"durable head accepted missing required-nullable {required_nullable}",
        )
    assert_invalid(
        head_validator,
        {**physical_head, "gc_receipt": "sha256:" + "0" * 64},
        "zero-GC-sequence durable head accepted a GC receipt",
    )
    assert_invalid(
        head_validator,
        {**physical_head, "checkpoint_id": "sha256:" + "0" * 64},
        "StateRoot head accepted a legacy checkpoint pointer",
    )
    assert_invalid(
        head_validator,
        {
            **physical_head,
            "head_version": legacy_versions["durable_head"],
        },
        "durable head accepted the superseded physical generation",
    )
    machine_base_anchor = storage_fixture["machine_base_anchor"]
    machine_base_anchor_validator.validate(machine_base_anchor)
    head_validator.validate(
        {**physical_head, "machine_base_anchor": machine_base_anchor}
    )
    assert_invalid(
        machine_base_anchor_validator,
        {
            **machine_base_anchor,
            "anchor_version": legacy_versions["machine_base_anchor"],
        },
        "StateRoot head accepted the superseded Machine-base anchor",
    )
    missing_projection_root = dict(machine_base_anchor)
    del missing_projection_root["projection_root"]
    assert_invalid(
        machine_base_anchor_validator,
        missing_projection_root,
        "Machine-base anchor accepted a missing reducer root",
    )

    state_root_objects = storage_fixture["state_root_objects"]
    for state_root_object in state_root_objects:
        state_root_object_validator.validate(state_root_object)
        durable_storage_validator.validate(state_root_object)
    for value_kind, byte_field in (
        ("leaf", "canonical_json"),
        ("machine_base_chunk", "bytes"),
    ):
        current = next(
            value for value in state_root_objects
            if value.get("value", {}).get("value") == value_kind
        )
        retired = json.loads(json.dumps(current))
        retired["value"][byte_field] = [123, 125]
        assert_invalid(
            state_root_object_validator, retired,
            f"StateRoot {value_kind} accepted the retired numeric byte array codec",
        )
        if value_kind == "machine_base_chunk":
            noncanonical = json.loads(json.dumps(current))
            noncanonical["value"][byte_field] = "AB=="
            assert_invalid(
                state_root_object_validator, noncanonical,
                "Machine-base chunk accepted nonzero unused Base64 padding bits",
            )
    expected_leaf_kinds = {
        "machine_plan", "machine_artifact", "machine_event", "machine_admission",
        "machine_command_batch",
        "machine_effect", "machine_obligation", "machine_attempt", "machine_fact",
        "continuation", "run_current", "wait", "wait_summary", "wait_activation", "lease", "outbox", "outbox_owner",
        "component_occurrence", "operation_attempt", "clock_observation", "snapshot",
        "cancellation_receipt", "effect_resolution_receipt",
        "history_compaction", "journal_record", "journal_prefix_replacement",
        "journal_record_manifest", "journal_prefix_replacement_authority",
        "coupled_checkpoint_receipt", "resource_command_receipt",
        "resource_retention_current", "resource_pin_current", "resource_delete_current",
        "resource_handoff_current", "resource_handoff_activation_current",
        "resource_handoff_index", "resource_handoff_activation_index", "agent_command",
        "agent_command_receipt", "agent_input_suspension_receipt",
        "agent_input_completion_receipt", "agent_session_current", "agent_update_current",
        "agent_message_current", "agent_tool_current", "agent_target_claim_current", "agent_elicitation_current",
        "agent_occurrence_current", "agent_stream_current", "agent_stream_chunk_current",
        "evolution_current", "evolution_command_alias", "evolution_persistence_receipt",
        "evolution_mutation", "virtual_current", "virtual_persistence_receipt",
        "virtual_state_leaf", "resource_catalog_record",
    }
    if set(durable_storage_schema["$defs"]["state_root_leaf_kind"]["enum"]) != expected_leaf_kinds:
        raise AssertionError("StateRoot leaf-kind schema drifted from the closed Rust enum")
    retired_machine_leaf = json.loads(json.dumps(state_root_objects[1]))
    retired_machine_leaf["value"]["kind"] = "machine_command"
    assert_invalid(
        state_root_object_validator,
        retired_machine_leaf,
        "StateRoot accepted the retired independently stored Machine command leaf",
    )
    run_query_index_object = next(
        value
        for value in state_root_objects
        if value.get("value", {}).get("value") == "run_query_indexes"
    )
    for field in ("pending_waits", "active_effects", "active_leases", "terminal"):
        malformed_indexes = json.loads(json.dumps(run_query_index_object))
        del malformed_indexes["value"][field]
        assert_invalid(
            state_root_object_validator,
            malformed_indexes,
            f"Run query indexes omitted required current {field}",
        )
    legacy_run_indexes = json.loads(json.dumps(run_query_index_object))
    legacy_run_indexes["value"]["index_version"] = legacy_versions[
        "run_query_indexes"
    ]
    assert_invalid(
        state_root_object_validator,
        legacy_run_indexes,
        "Run query indexes accepted predecessor generation /1",
    )
    pending_wait_source_object = next(
        value
        for value in state_root_objects
        if value.get("value", {}).get("value") == "pending_wait_source"
    )
    for malformed_source, message in [
        (
            {
                **pending_wait_source_object,
                "value": {
                    **pending_wait_source_object["value"],
                    "source_version": "cymule.pending-wait-source/0",
                },
            },
            "pending Wait source accepted a retired generation",
        ),
        (
            {
                **pending_wait_source_object,
                "value": {
                    **pending_wait_source_object["value"],
                    "wait_ids": ["sha256:" + "0" * 64],
                },
            },
            "pending Wait source accepted the retired embedded full index",
        ),
    ]:
        assert_invalid(
            state_root_object_validator,
            malformed_source,
            message,
        )
    manifest_object = state_root_objects[0]
    state_root_manifest = {
        key: value for key, value in manifest_object.items() if key != "object"
    }
    state_root_manifest_validator.validate(state_root_manifest)
    missing_frontier = dict(state_root_manifest)
    del missing_frontier["machine_frontier"]
    assert_invalid(
        state_root_manifest_validator,
        missing_frontier,
        "StateRoot manifest omitted its required Machine frontier",
    )
    retired_authority_root = dict(state_root_manifest)
    retired_authority_root["machine_authority_root"] = retired_authority_root[
        "machine_frontier"
    ]["authority_root"]
    del retired_authority_root["machine_frontier"]
    assert_invalid(
        state_root_manifest_validator,
        retired_authority_root,
        "StateRoot manifest accepted the retired scalar Machine authority root",
    )
    retired_command_proof_root = json.loads(json.dumps(state_root_manifest))
    retired_command_proof_root["roots"]["machine_command_index_proofs"] = {
        "node": None,
        "entries": 0,
    }
    assert_invalid(
        state_root_manifest_validator,
        retired_command_proof_root,
        "StateRoot manifest accepted the retired Machine command-proof family",
    )
    for required_batch_root in (
        "machine_command_batches",
        "machine_command_batch_admissions",
    ):
        missing_batch_root = json.loads(json.dumps(state_root_manifest))
        del missing_batch_root["roots"][required_batch_root]
        assert_invalid(
            state_root_manifest_validator,
            missing_batch_root,
            f"StateRoot manifest omitted required {required_batch_root}",
        )
    for required_source_root in ("pending_signal_sources", "pending_timer_sources"):
        missing_source_root = json.loads(json.dumps(state_root_manifest))
        del missing_source_root["roots"][required_source_root]
        assert_invalid(
            state_root_manifest_validator,
            missing_source_root,
            f"StateRoot manifest omitted required {required_source_root}",
        )
    for required_nullable in (
        "parent_manifest",
        "parent_revision",
        "delta_digest",
        "machine_base_anchor",
    ):
        malformed_manifest = dict(state_root_manifest)
        del malformed_manifest[required_nullable]
        assert_invalid(
            state_root_manifest_validator,
            malformed_manifest,
            f"StateRoot manifest accepted missing required-nullable {required_nullable}",
        )
    assert_invalid(
        state_root_manifest_validator,
        {**state_root_manifest, "parent_manifest": "sha256:" + "0" * 64},
        "genesis StateRoot manifest accepted successor lineage",
    )
    successor_manifest = {
        **state_root_manifest,
        "sequence": 1,
        "parent_manifest": "sha256:" + "0" * 64,
        "parent_revision": "sha256:" + "1" * 64,
        "delta_digest": "2" * 64,
    }
    state_root_manifest_validator.validate(successor_manifest)
    assert_invalid(
        state_root_manifest_validator,
        {**successor_manifest, "delta_digest": None},
        "successor StateRoot manifest accepted incomplete parent lineage",
    )
    assert_invalid(
        state_root_object_validator,
        {**state_root_objects[1], "object": "opaque"},
        "StateRoot object accepted an open object union variant",
    )
    assert_invalid(
        state_root_object_validator,
        {**state_root_objects[1], "references": []},
        "StateRoot value accepted caller-authored reference authority",
    )
    open_value = json.loads(json.dumps(state_root_objects[1]))
    open_value["value"]["value"] = "opaque"
    assert_invalid(
        state_root_object_validator,
        open_value,
        "StateRoot value accepted an open typed-value variant",
    )
    missing_map_node = json.loads(json.dumps(state_root_manifest))
    del missing_map_node["roots"]["machine_plans"]["node"]
    assert_invalid(
        state_root_manifest_validator,
        missing_map_node,
        "StateRoot manifest accepted an omitted required-nullable map node",
    )
    missing_log_node = json.loads(json.dumps(state_root_manifest))
    del missing_log_node["roots"]["machine_events"]["node"]
    assert_invalid(
        state_root_manifest_validator,
        missing_log_node,
        "StateRoot manifest accepted an omitted required-nullable log node",
    )
    missing_machine_base = json.loads(json.dumps(state_root_manifest))
    del missing_machine_base["roots"]["machine_base"]
    assert_invalid(
        state_root_manifest_validator,
        missing_machine_base,
        "StateRoot manifest accepted an omitted required-nullable Machine base",
    )
    for malformed_root, message in [
        (
            {"node": None, "entries": 1},
            "StateRoot map accepted a non-empty map without a node",
        ),
        (
            {"node": "sha256:" + "3" * 64, "entries": 0},
            "StateRoot map accepted an empty map with a node",
        ),
    ]:
        malformed_manifest = json.loads(json.dumps(state_root_manifest))
        malformed_manifest["roots"]["machine_plans"] = malformed_root
        assert_invalid(state_root_manifest_validator, malformed_manifest, message)
    for malformed_root, message in [
        (
            {
                "node": None,
                "len": 1,
                "height": 1,
                "ordered_root": "sha256:" + "4" * 64,
            },
            "StateRoot log accepted a non-empty log without a node",
        ),
        (
            {
                "node": "sha256:" + "4" * 64,
                "len": 0,
                "height": 1,
                "ordered_root": "sha256:" + "4" * 64,
            },
            "StateRoot log accepted an empty log with live node authority",
        ),
        (
            {
                "node": None,
                "len": 0,
                "height": 0,
                "ordered_root": "sha256:" + "4" * 64,
            },
            "StateRoot log accepted a forged empty-log commitment",
        ),
    ]:
        malformed_manifest = json.loads(json.dumps(state_root_manifest))
        malformed_manifest["roots"]["machine_events"] = malformed_root
        assert_invalid(state_root_manifest_validator, malformed_manifest, message)

    gc_receipt = storage_fixture["gc_receipt"]
    gc_receipt_validator.validate(gc_receipt)
    durable_storage_validator.validate(gc_receipt)
    assert_invalid(
        gc_receipt_validator,
        {
            **gc_receipt,
            "receipt_version": legacy_versions["durable_gc_receipt"],
        },
        "GC receipt accepted the superseded physical generation",
    )
    missing_remaining = dict(gc_receipt)
    del missing_remaining["remaining_objects"]
    assert_invalid(
        gc_receipt_validator,
        missing_remaining,
        "GC receipt accepted a missing bounded remaining-object count",
    )
    for malformed_gc, message in [
        (
            {**gc_receipt, "gc_sequence": 0},
            "GC receipt accepted a zero physical sequence",
        ),
        (
            {
                **gc_receipt,
                "reclaimed_ids": [],
                "reclaimed_objects": 0,
                "remaining_objects": 1,
            },
            "empty GC page accepted a non-zero remaining inventory",
        ),
        (
            {**gc_receipt, "reclaimed_objects": 0},
            "non-empty GC page accepted a zero reclaimed count",
        ),
        (
            {**gc_receipt, "reclaimed_objects": 262_145},
            "GC receipt accepted a reclaimed count above its protocol bound",
        ),
    ]:
        assert_invalid(gc_receipt_validator, malformed_gc, message)
    assert_invalid(
        gc_receipt_validator,
        {**gc_receipt, "retained_checkpoint": "sha256:" + "0" * 64},
        "StateRoot GC receipt accepted a legacy checkpoint authority",
    )

    for archive_object in storage_fixture["command_archive_objects"]:
        command_archive_object_validator.validate(archive_object)
        durable_storage_validator.validate(archive_object)
    assert_invalid(
        command_archive_object_validator,
        {
            **storage_fixture["command_archive_objects"][3],
            "object_kind": "proof",
        },
        "command archive accepted an open persistence-object variant",
    )
    open_archive = json.loads(
        json.dumps(storage_fixture["command_archive_objects"][1])
    )
    open_archive["object"]["locator"] = "provider-owned"
    assert_invalid(
        command_archive_object_validator,
        open_archive,
        "command archive entry accepted provider-owned physical metadata",
    )
    noncanonical_conflict = json.loads(
        json.dumps(storage_fixture["command_archive_objects"][1])
    )
    noncanonical_conflict["object"]["command"]["receipt"]["error_code"] = (
        "precondition_conflict"
    )
    assert_invalid(
        command_archive_object_validator,
        noncanonical_conflict,
        "command archive accepted a noncanonical conflict receipt",
    )
    genesis_archive = storage_fixture["command_archive_objects"][0]
    retired_archive_generation = json.loads(json.dumps(genesis_archive))
    retired_archive_generation["object"]["header"]["segment_version"] = (
        legacy_versions["command_archive_segment"]
    )
    assert_invalid(
        command_archive_object_validator,
        retired_archive_generation,
        "command archive accepted the retired pre-batch segment generation",
    )
    missing_archive_batches = json.loads(json.dumps(genesis_archive))
    del missing_archive_batches["object"]["batches"]
    assert_invalid(
        command_archive_object_validator,
        missing_archive_batches,
        "command archive omitted its complete batch records",
    )
    for parent_field in ("parent_authority_root", "admission_parent_authority_root"):
        missing_batch_parent = json.loads(json.dumps(genesis_archive))
        del missing_batch_parent["object"]["batches"][0][parent_field]
        assert_invalid(
            command_archive_object_validator,
            missing_batch_parent,
            f"command archive omitted required batch parent {parent_field}",
        )
    forged_genesis_archive = json.loads(json.dumps(genesis_archive))
    forged_genesis_archive["object"]["header"]["parent_segment"] = (
        "sha256:" + "6" * 64
    )
    command_archive_object_validator.validate(forged_genesis_archive)
    forged_genesis_event_count = json.loads(json.dumps(genesis_archive))
    forged_genesis_event_count["object"]["header"]["parent_event_count"] = 1
    assert_invalid(
        command_archive_object_validator,
        forged_genesis_event_count,
        "genesis command archive accepted a parent Event count",
    )
    incomplete_successor_archive = json.loads(json.dumps(genesis_archive))
    incomplete_successor_archive["object"]["header"]["parent_count"] = 1
    assert_invalid(
        command_archive_object_validator,
        incomplete_successor_archive,
        "successor command archive omitted its parent identities",
    )
    malformed_membership_archive = json.loads(json.dumps(genesis_archive))
    malformed_membership_archive["object"]["command_index_updates"][0]["value"] = (
        storage_fixture["command_archive_objects"][3]["object"]["value"]
    )
    assert_invalid(
        command_archive_object_validator,
        malformed_membership_archive,
        "command archive accepted a membership proof with non-membership shape",
    )
    missing_proof_null = json.loads(json.dumps(genesis_archive))
    del missing_proof_null["object"]["command_index_updates"][0]["empty_depth"]
    assert_invalid(
        command_archive_object_validator,
        missing_proof_null,
        "command archive accepted an omitted required-nullable proof depth",
    )

    archive_entry = storage_fixture["command_archive_objects"][1]
    machine_command_current_object = {
        "object": "value",
        "value_version": "cymule.durable-state-value/5",
        "object_id": "sha256:" + "f" * 64,
        "value": {
            "value": "machine_command_current",
            "record": archive_entry["object"]["command"],
            "admission": archive_entry["object"]["admission"],
            "index_proof": storage_fixture["command_archive_objects"][0]["object"][
                "command_index_updates"
            ][0],
            "first_event_position": None,
        },
    }
    state_root_object_validator.validate(machine_command_current_object)
    retired_split_command = json.loads(json.dumps(machine_command_current_object))
    del retired_split_command["value"]["index_proof"]
    assert_invalid(
        state_root_object_validator,
        retired_split_command,
        "Machine command current omitted its atomic archive-index proof",
    )
    for required_receipt_field in (
        "event_ids",
        "error_code",
        "message",
        "observed_precondition",
        "current_precondition",
    ):
        missing_receipt_field = json.loads(json.dumps(archive_entry))
        del missing_receipt_field["object"]["command"]["receipt"][
            required_receipt_field
        ]
        assert_invalid(
            command_archive_object_validator,
            missing_receipt_field,
            f"command archive accepted omitted receipt field {required_receipt_field}",
        )
    applied_receipt = json.loads(
        json.dumps(archive_entry["object"]["command"]["receipt"])
    )
    applied_receipt.update(
        {
            "status": "applied",
            "event_ids": ["sha256:" + "a" * 64, "sha256:" + "b" * 64],
            "error_code": None,
            "message": None,
            "observed_precondition": None,
            "current_precondition": "pre:1:sha256:" + "c" * 64,
        }
    )
    command_receipt_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/command_receipt"
            )
        },
        registry=registry,
    )
    command_receipt_validator.validate(applied_receipt)
    assert_invalid(
        command_receipt_validator,
        {
            **applied_receipt,
            "event_ids": [
                "sha256:" + "a" * 64,
                "sha256:" + "b" * 64,
                "sha256:" + "c" * 64,
            ],
        },
        "Command receipt accepted more than two atomic Events",
    )
    assert_invalid(
        command_receipt_validator,
        {
            **archive_entry["object"]["command"]["receipt"],
            "event_ids": ["sha256:" + "a" * 64],
        },
        "conflicting Command receipt retained an Event",
    )
    missing_expected_precondition = json.loads(json.dumps(archive_entry))
    del missing_expected_precondition["object"]["command"]["envelope"][
        "expected_precondition"
    ]
    assert_invalid(
        command_archive_object_validator,
        missing_expected_precondition,
        "command archive accepted an omitted required-nullable command precondition",
    )
    run_input = {
        "identity_version": "cymule.artifact/2",
        "artifact_id": "sha256:" + "9" * 64,
        "kind": "cymule.input/1",
    }
    start_run_envelope = json.loads(
        json.dumps(archive_entry["object"]["command"]["envelope"])
    )
    start_run_envelope["expected_precondition"] = None
    start_run_envelope["command"] = {
        "type": "start_run",
        "plan_id": "sha256:" + "8" * 64,
        "binding_context": "sha256:" + "7" * 64,
        "input": run_input,
    }
    command_envelope_validator.validate(start_run_envelope)
    for malformed_start in [
        {
            **start_run_envelope,
            "command": {
                key: value
                for key, value in start_run_envelope["command"].items()
                if key != "input"
            },
        },
        {
            **start_run_envelope,
            "command": {
                **start_run_envelope["command"],
                "input": {**run_input, "kind": "cymule.component-output/1"},
            },
        },
    ]:
        assert_invalid(
            command_envelope_validator,
            malformed_start,
            "Core StartRun accepted a missing or wrong-kind input Artifact",
        )
    complete_run_envelope = json.loads(
        json.dumps(archive_entry["object"]["command"]["envelope"])
    )
    complete_run_envelope["command"] = {"type": "complete_run", "result": None}
    command_envelope_validator.validate(complete_run_envelope)
    del complete_run_envelope["command"]["result"]
    assert_invalid(
        command_envelope_validator,
        complete_run_envelope,
        "command archive accepted an omitted required-nullable completion result",
    )

    completed_event = {
        "event_id": "sha256:" + "a" * 64,
        "event_version": "cymule.event/8",
        "command_id": "command:completed",
        "command_hash": "b" * 64,
        "run_id": "run:completed",
        "parents": [],
        "reads": [],
        "writes": [],
        "coordination_key": None,
        "payload": {"type": "run_completed", "result": None},
    }
    core_event_validator.validate(completed_event)
    started_event = {
        **completed_event,
        "payload": {
            "type": "run_started",
            "plan_id": "sha256:" + "8" * 64,
            "entry_definition": "main",
            "binding_context": "sha256:" + "7" * 64,
            "input": run_input,
        },
    }
    core_event_validator.validate(started_event)
    for malformed_started in [
        {
            **started_event,
            "payload": {
                key: value
                for key, value in started_event["payload"].items()
                if key != "input"
            },
        },
        {
            **started_event,
            "payload": {
                **started_event["payload"],
                "input": {**run_input, "kind": "cymule.component-output/1"},
            },
        },
    ]:
        assert_invalid(
            core_event_validator,
            malformed_started,
            "Core RunStarted accepted a missing or wrong-kind input Artifact",
        )
    for field, message in (
        (
            "coordination_key",
            "Core Event accepted an omitted required-nullable coordination key",
        ),
        ("payload.result", "Core Event accepted an omitted completion result"),
    ):
        malformed_event = json.loads(json.dumps(completed_event))
        if field == "coordination_key":
            del malformed_event[field]
        else:
            del malformed_event["payload"]["result"]
        assert_invalid(core_event_validator, malformed_event, message)

    root_scope = {
        "scope_id": "scope:root",
        "parent_scope": None,
        "invocation_id": "sha256:" + "c" * 64,
        "invocation_path": [],
        "definition_id": "definition:root",
        "region_path": [],
        "site_id": None,
        "status": "open",
        "intents": [],
    }
    scope_projection_validator.validate(root_scope)
    for required_nullable in ("parent_scope", "site_id"):
        missing_scope_field = dict(root_scope)
        del missing_scope_field[required_nullable]
        assert_invalid(
            scope_projection_validator,
            missing_scope_field,
            f"Machine projection accepted omitted required-nullable scope {required_nullable}",
        )
    run_projection = {
        "run_id": "run:projection",
        "initial_plan": "sha256:" + "d" * 64,
        "current_plan": "sha256:" + "d" * 64,
        "plan_lineage": ["sha256:" + "d" * 64],
        "initial_binding_context": "sha256:" + "e" * 64,
        "current_binding_context": "sha256:" + "e" * 64,
        "binding_lineage": ["sha256:" + "e" * 64],
        "epoch": 0,
        "execution_status": {"status": "active"},
        "world_settlement": "settled",
        "scopes": {},
        "effects": {},
        "obligations": {},
        "attempts": {},
        "result": None,
        "last_event": "sha256:" + "f" * 64,
    }
    run_projection_validator.validate(run_projection)
    del run_projection["result"]
    assert_invalid(
        run_projection_validator,
        run_projection,
        "Machine projection accepted an omitted required-nullable Run result",
    )
    history_compaction = {
        "compaction_version": "cymule.history-compaction/2",
        "compaction_id": "compaction:schema",
        "parent_compaction": None,
        "kind": "event_free_admissions",
        "source_revision": "sha256:" + "1" * 64,
        "requested_suffix": 0,
        "result": {
            "base_id": "sha256:" + "2" * 64,
            "compacted_events": 0,
            "retained_events": 0,
            "causal_frontier": [],
            "projection_digest": "3" * 64,
            "archive_segment": genesis_archive["object"]["header"],
        },
    }
    history_compaction_validator.validate(history_compaction)
    assert_invalid(
        history_compaction_validator,
        {**history_compaction, "kind": "conflict_admissions"},
        "history compaction admitted the retired conflict-only kind",
    )
    del history_compaction["parent_compaction"]
    assert_invalid(
        history_compaction_validator,
        history_compaction,
        "history compaction accepted an omitted required-nullable parent",
    )

    if "segment" in durable_storage_schema["$defs"] or "checkpoint" in durable_storage_schema["$defs"]:
        raise AssertionError("durable storage still publishes legacy segment/checkpoint definitions")
    assert_invalid(
        durable_storage_validator,
        {
            "segment_version": legacy_versions["durable_segment"],
            "segment_id": "sha256:" + "0" * 64,
            "sequence": 1,
            "parent_segment": None,
            "base_revision": "sha256:" + "1" * 64,
            "revision": "sha256:" + "2" * 64,
            "delta": {"operations": []},
        },
        "StateRoot-only physical schema accepted a StateSegment",
    )
    assert_invalid(
        durable_storage_validator,
        {
            "checkpoint_version": legacy_versions["durable_checkpoint"],
            "checkpoint_id": "sha256:" + "0" * 64,
            "state": durable_state,
        },
        "StateRoot-only physical schema accepted a checkpoint envelope",
    )
    evolution_control = load(root / "tests/fixtures/evolution-control.json")
    evolution_validator = Draft202012Validator(
        by_title["Cymule Evolution Control cymule.evolution-control/5"],
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
    artifact = canonical_artifact
    artifact_record_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/evolution-control.schema.json"
                "#/$defs/artifactRecord"
            )
        },
        registry=registry,
    )
    canonical_artifact_record = {"reference": artifact, "bytes": "e30="}
    artifact_record_validator.validate(canonical_artifact_record)
    for invalid_bytes in ([123, 125], "YQ", "A" * 11_184_816):
        assert_invalid(
            artifact_record_validator,
            {**canonical_artifact_record, "bytes": invalid_bytes},
            "ArtifactRecord accepted a legacy, noncanonical, or oversized bytes wire",
        )
    execution_binding = {
        "identity_version": "cymule.artifact/2",
        "artifact_id": "sha256:" + "9" * 64,
        "kind": "cymule.execution-binding/2",
    }
    target_execution_binding = {
        "identity_version": "cymule.artifact/2",
        "artifact_id": "sha256:" + "8" * 64,
        "kind": "cymule.execution-binding/2",
    }
    restart_control = load(root / "tests/fixtures/evolution-restart-control.json")
    evolution_variants = [
        {
            "control_version": "cymule.evolution-control/5",
            "command_id": "command:patch",
            "operation": "apply_patch",
            "patch": {
                "from_plan": "sha256:" + "1" * 64,
                "target": candidate,
                "operations": [
                    {
                        "kind": "replace",
                        "target": "definition:main",
                        "before": "1" * 64,
                        "after": "2" * 64,
                    }
                ],
                "evidence": artifact,
            },
        },
        {
            "control_version": "cymule.evolution-control/5",
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
            "control_version": "cymule.evolution-control/5",
            "command_id": "command:select",
            "operation": "select_occurrence",
            "occurrence_id": "occurrence:1",
            "selection_id": "selection:1",
            "execution_binding": execution_binding,
        },
        {
            "control_version": "cymule.evolution-control/5",
            "command_id": "command:migrate",
            "operation": "migrate",
            "request": {
                "migration_id": "migration:1",
                "run_id": "run:1",
                "from_plan": "sha256:" + "1" * 64,
                "to_plan": "sha256:" + "2" * 64,
                "plan_edge_id": "sha256:" + "4" * 64,
                "compatibility_id": "sha256:" + "5" * 64,
                "expected_source_epoch": 7,
                "adapter_id": "adapter:fixture",
                "adapter_revision": "sha256:" + "c" * 64,
            },
        },
        restart_control,
        {
            "control_version": "cymule.evolution-control/5",
            "command_id": "command:shadow",
            "operation": "shadow",
            "request": {
                "comparison_id": "shadow:1",
                "decision_id": "rollout:canary",
                "subject": "occurrence:1",
                "primary_plan": "sha256:" + "1" * 64,
                "shadow_plan": "sha256:" + "2" * 64,
                "driver_id": "driver:fixture",
                "driver_revision": "sha256:" + "d" * 64,
                "input": artifact,
                "comparison_policy": "json-exact/1",
            },
        },
        {
            "control_version": "cymule.evolution-control/5",
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
    patch_command = next(
        command for command in evolution_variants if command["operation"] == "apply_patch"
    )
    rollout_command = next(
        command for command in evolution_variants if command["operation"] == "set_rollout"
    )
    observation_command = next(
        command for command in evolution_variants if command["operation"] == "observe"
    )
    for label, command, path in (
        ("patch.from_plan", patch_command, ("patch", "from_plan")),
        (
            "decision.fallback_plan",
            rollout_command,
            ("decision", "fallback_plan"),
        ),
        (
            "decision.target_plan",
            rollout_command,
            ("decision", "target_plan"),
        ),
        (
            "observation.plan_id",
            observation_command,
            ("observation", "plan_id"),
        ),
    ):
        for invalid_plan in ("plan:legacy", "sha256:" + "A" * 64):
            malformed = json.loads(json.dumps(command))
            malformed[path[0]][path[1]] = invalid_plan
            assert_invalid(
                evolution_validator,
                malformed,
                f"Evolution schema accepted invalid Plan identity at {label}",
            )
    migration_descriptor_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/evolution-control.schema.json"
                "#/$defs/migrationDescriptor"
            )
        },
        registry=registry,
    )
    migration_descriptor = {
        "adapter_id": "adapter:terminal",
        "adapter_revision": "sha256:" + "1" * 64,
        "from_plan": "sha256:" + "2" * 64,
        "to_plan": "sha256:" + "3" * 64,
        "plan_edge_id": "edge:terminal",
        "compatibility_id": "compatibility:terminal",
        "from_schema": "schema:source",
        "to_schema": "schema:target",
        "state_coverage": "total_reachable_state",
        "failure_and_cancellation": "preserved",
        "budget_and_ownership": "preserved",
        "authority_and_effects": "no_widening",
    }
    migration_descriptor_validator.validate(migration_descriptor)
    for invalid_plan in ("plan:legacy", "sha256:" + "A" * 64):
        malformed_descriptor = dict(migration_descriptor)
        malformed_descriptor["from_plan"] = invalid_plan
        assert_invalid(
            migration_descriptor_validator,
            malformed_descriptor,
            "Evolution schema accepted invalid migration descriptor source Plan",
        )
    for definition, valid_value, plan_fields in (
        (
            "planEdge",
            {
                "edge_id": "edge:terminal",
                "from_plan": "sha256:" + "1" * 64,
                "to_plan": "sha256:" + "2" * 64,
                "operations": [
                    {
                        "kind": "add",
                        "target": "definition:next",
                        "before": None,
                        "after": "3" * 64,
                    }
                ],
            },
            ("from_plan", "to_plan"),
        ),
        (
            "templateUpdate",
            {
                "template_id": "template:terminal",
                "previous_plan_id": "sha256:" + "1" * 64,
                "current_plan_id": "sha256:" + "2" * 64,
                "decision_id": "sha256:" + "3" * 64,
                "advanced": True,
            },
            ("previous_plan_id", "current_plan_id"),
        ),
    ):
        response_validator = Draft202012Validator(
            {
                "$ref": (
                    "https://cymule.dev/schemas/engine-protocol.schema.json"
                    f"#/$defs/{definition}"
                )
            },
            registry=registry,
        )
        response_validator.validate(valid_value)
        for plan_field in plan_fields:
            for invalid_plan in ("plan:legacy", "sha256:" + "A" * 64):
                malformed_response = dict(valid_value)
                malformed_response[plan_field] = invalid_plan
                assert_invalid(
                    response_validator,
                    malformed_response,
                    f"Engine schema accepted invalid Evolution {definition}.{plan_field}",
                )
    for malformed_operation in (
        {"kind": "replace", "before": None, "after": "2" * 64},
        {
            "kind": "replace",
            "before": "1" * 64,
            "after": "sha256:" + "2" * 64,
        },
        {"kind": "replace", "before": "1" * 64, "after": "1" * 64},
    ):
        malformed_patch = json.loads(json.dumps(patch_command))
        malformed_patch["patch"]["operations"][0].update(malformed_operation)
        rejected = subprocess.run(
            [str(engine), "evolution-command", "verify", "--input", "-"],
            input=json.dumps(malformed_patch),
            check=False,
            capture_output=True,
            text=True,
        )
        if rejected.returncode == 0:
            raise AssertionError("Rust Engine accepted a malformed patch operation")
    shadow_command = next(
        command for command in evolution_variants if command["operation"] == "shadow"
    )
    for required_pin in ("driver_id", "driver_revision"):
        missing_shadow_pin = json.loads(json.dumps(shadow_command))
        del missing_shadow_pin["request"][required_pin]
        assert_invalid(
            evolution_validator,
            missing_shadow_pin,
            f"Evolution schema accepted shadow request without {required_pin}",
        )
    malformed_shadow_revision = json.loads(json.dumps(shadow_command))
    malformed_shadow_revision["request"]["driver_revision"] = "revision:forged"
    assert_invalid(
        evolution_validator,
        malformed_shadow_revision,
        "Evolution schema accepted a non-content shadow driver revision",
    )
    validate_engine_request(
        {"type": "verify_evolution_command", "command": evolution_control}
    )
    mathematical_integer_command = json.loads(json.dumps(evolution_control))
    mathematical_integer_command["command_id"] = "command:mathematical-integer"
    mathematical_integer_command["gate"]["min_target_observations"] = 1.0
    evolution_validator.validate(mathematical_integer_command)
    validate_engine_request(
        {
            "type": "verify_evolution_command",
            "command": mathematical_integer_command,
        }
    )
    mathematical_envelope = {
        "engine_protocol": "cymule.engine/5",
        "request": {
            "type": "verify_evolution_command",
            "command": mathematical_integer_command,
        },
    }
    mathematical_wire = json.dumps(mathematical_envelope, separators=(",", ":"))
    for lexeme in ["1.0", "1e0"]:
        wire = mathematical_wire.replace(
            '"min_target_observations":1.0',
            f'"min_target_observations":{lexeme}',
        )
        result = json.loads(
            subprocess.run(
                [str(engine), "rpc"],
                input=wire,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        normalized = result["request"]["command"]["gate"][
            "min_target_observations"
        ]
        if result["outcome"] != "success" or not isinstance(normalized, int):
            raise AssertionError(
                f"Rust Engine did not normalize mathematical integer {lexeme}"
            )
    fractional_integer_command = json.loads(json.dumps(evolution_control))
    fractional_integer_command["command_id"] = "command:fractional-integer"
    fractional_integer_command["gate"]["min_target_observations"] = 1.5
    assert_invalid(
        evolution_validator,
        fractional_integer_command,
        "Evolution schema accepted a fractional value for an integer field",
    )
    assert_invalid(
        engine_validator,
        {
            "engine_protocol": "cymule.engine/5",
            "request": {
                "type": "verify_evolution_command",
                "command": fractional_integer_command,
            },
        },
        "Engine schema accepted a fractional value for an integer field",
    )
    malformed_evolution = dict(evolution_control)
    malformed_evolution["provider"] = "must-not-enter-evolution-control"
    try:
        evolution_validator.validate(malformed_evolution)
    except ValidationError:
        pass
    else:
        raise AssertionError("evolution control schema accepted a provider field")

    restart_request = next(
        command["request"]
        for command in evolution_variants
        if command["operation"] == "restart_under_new_plan"
    )
    unsafe_restart = json.loads(json.dumps(restart_control))
    unsafe_restart["command_id"] = "command:unsafe-restart"
    unsafe_restart["request"]["expected_source_epoch"] = 9_007_199_254_740_992
    assert_invalid(
        evolution_validator,
        unsafe_restart,
        "Evolution schema accepted an unsafe restart source epoch",
    )
    for retired_field in ("safe_point", "source_continuation", "source_epoch"):
        retired_restart = json.loads(json.dumps(restart_control))
        retired_restart["request"][retired_field] = {"retired": True}
        assert_invalid(
            evolution_validator,
            retired_restart,
            f"Evolution schema accepted retired restart field {retired_field}",
        )

    migration_command = next(
        command for command in evolution_variants if command["operation"] == "migrate"
    )
    migration_request = migration_command["request"]
    target_continuation = {
        "continuation_version": "cymule.continuation-state/1",
        "run_id": migration_request["run_id"],
        "plan_id": migration_request["to_plan"],
        "binding_context": target_execution_binding["artifact_id"],
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
        "scope_stack": ["scope:root"],
        "epoch": migration_request["expected_source_epoch"] + 1,
        "execution_fence": 3,
        "execution_claim": None,
        "status": "ready",
    }
    migration_descriptor = {
        "adapter_id": migration_request["adapter_id"],
        "adapter_revision": migration_request["adapter_revision"],
        "from_plan": migration_command["request"]["from_plan"],
        "to_plan": migration_command["request"]["to_plan"],
        "plan_edge_id": migration_command["request"]["plan_edge_id"],
        "compatibility_id": migration_command["request"]["compatibility_id"],
        "from_schema": "schema:source",
        "to_schema": "schema:target",
        "state_coverage": "total_reachable_state",
        "failure_and_cancellation": "preserved",
        "budget_and_ownership": "preserved",
        "authority_and_effects": "no_widening",
    }
    migration_output = {
        "continuation": target_continuation,
        "artifacts": [{"reference": artifact, "bytes": "e30="}],
        "evidence": {"reference": artifact, "bytes": "e30="},
    }
    evolution_plugin_validator = Draft202012Validator(
        by_title["Cymule Evolution Plugin Protocol cymule.evolution-plugin/3"],
        registry=registry,
    )
    plugin_source_continuation = {
        **target_continuation,
        "plan_id": migration_request["from_plan"],
        "binding_context": execution_binding["artifact_id"],
        "state": artifact,
        "epoch": migration_request["expected_source_epoch"],
    }
    plugin_migration_request = {
        "evolution_plugin_protocol": "cymule.evolution-plugin/3",
        "implementation_revision": migration_request["adapter_revision"],
        "request": {
            "type": "migrate",
            "request": {
                "intent": migration_request,
                "source_witness_id": "sha256:" + "3" * 64,
                "source_continuation": plugin_source_continuation,
                "input_state": artifact,
                "source_binding": execution_binding,
                "target_binding": target_execution_binding,
            },
        },
    }
    evolution_plugin_validator.validate(plugin_migration_request)
    for invalid_version in (None, "cymule.continuation-state/0"):
        malformed_plugin_request = json.loads(json.dumps(plugin_migration_request))
        source_continuation = malformed_plugin_request["request"]["request"][
            "source_continuation"
        ]
        if invalid_version is None:
            del source_continuation["continuation_version"]
        else:
            source_continuation["continuation_version"] = invalid_version
        assert_invalid(
            evolution_plugin_validator,
            malformed_plugin_request,
            "evolution plugin accepted a missing or predecessor Continuation state version",
        )
    evolution_plugin_validator.validate(
        {
            "outcome": "success",
            "evolution_plugin_protocol": "cymule.evolution-plugin/3",
            "response": {"type": "migration_descriptor", "descriptor": migration_descriptor},
        }
    )
    for missing in ["plan_edge_id", "compatibility_id"]:
        incomplete_descriptor = json.loads(json.dumps(migration_descriptor))
        del incomplete_descriptor[missing]
        assert_invalid(
            evolution_plugin_validator,
            {
                "outcome": "success",
                "evolution_plugin_protocol": "cymule.evolution-plugin/3",
                "response": {
                    "type": "migration_descriptor",
                    "descriptor": incomplete_descriptor,
                },
            },
            f"evolution plugin accepted descriptor without {missing}",
        )
    evolution_plugin_validator.validate(
        {
            "outcome": "success",
            "evolution_plugin_protocol": "cymule.evolution-plugin/3",
            "response": {"type": "migrated", "output": migration_output},
        }
    )
    evolution_plugin_validator.validate(
        {
            "outcome": "failure",
            "evolution_plugin_protocol": "cymule.evolution-plugin/3",
            "error": {
                "category": "substrate",
                "code": "provider_unavailable_2",
                "message": "🧭" * 2000,
            },
        }
    )
    evolution_plugin_validator.validate(
        {
            "outcome": "failure",
            "evolution_plugin_protocol": "cymule.evolution-plugin/3",
            "error": {
                "category": "contract",
                "violation": {
                    "phase": "execution",
                    "target": {
                        "boundary": "component",
                        "id": "component:migration",
                        "side": "output",
                    },
                    "issues": [
                        {
                            "kind": "validation",
                            "instance_path": "/state",
                            "schema_path": "/properties/state",
                            "message": "state is invalid",
                        }
                    ],
                },
            },
        }
    )
    maximum_contract_issues = [
        {
            "kind": "validation",
            "instance_path": f"/field-{index}",
            "schema_path": f"/properties/field-{index}",
            "message": "界" * 2000,
        }
        for index in range(99)
    ] + [
        {
            "kind": "omitted",
            "instance_path": "",
            "schema_path": "",
            "message": "additional contract issues were omitted after the fixed validation budget",
        }
    ]
    maximum_contract_failure = {
        "outcome": "failure",
        "evolution_plugin_protocol": "cymule.evolution-plugin/3",
        "error": {
            "category": "contract",
            "violation": {
                "phase": "execution",
                "target": {
                    "boundary": "component",
                    "id": "界" * 512,
                    "side": "output",
                },
                "issues": maximum_contract_issues,
            },
        },
    }
    evolution_plugin_validator.validate(maximum_contract_failure)
    overflow_contract_failure = json.loads(json.dumps(maximum_contract_failure))
    overflow_contract_failure["error"]["violation"]["issues"].insert(
        0,
        {
            "kind": "validation",
            "instance_path": "/overflow",
            "schema_path": "/overflow",
            "message": "overflow",
        },
    )
    assert_invalid(
        evolution_plugin_validator,
        overflow_contract_failure,
        "evolution plugin accepted more than 100 bounded contract issues",
    )
    for malformed_issue in [
        {
            "instance_path": "/state",
            "schema_path": "/state",
            "message": "missing kind",
        },
        {
            "kind": "future",
            "instance_path": "/state",
            "schema_path": "/state",
            "message": "unknown kind",
        },
        {
            "kind": "omitted",
            "instance_path": "/not-empty",
            "schema_path": "",
            "message": "additional contract issues were omitted after the fixed validation budget",
        },
        {
            "kind": "validation",
            "instance_path": "/" + "x" * 1000,
            "schema_path": "",
            "message": "long pointer",
        },
        {
            "kind": "validation",
            "instance_path": "",
            "schema_path": "",
            "message": "invalid\nmessage",
        },
    ]:
        malformed_contract = json.loads(json.dumps(maximum_contract_failure))
        malformed_contract["error"]["violation"]["issues"] = [malformed_issue]
        assert_invalid(
            evolution_plugin_validator,
            malformed_contract,
            "evolution plugin accepted a malformed ContractIssue",
        )
    for malformed_failure in [
        {"category": "substrate", "code": "2provider", "message": "failure"},
        {"category": "substrate", "code": "provider-Unavailable", "message": "failure"},
        {"category": "substrate", "code": "provider", "message": "🧭" * 2001},
        {"category": "unknown", "code": "provider", "message": "failure"},
        {"category": "contract", "code": "provider", "message": "failure"},
        {"category": "contract", "violation": {"phase": "execution", "target": {"boundary": "component", "id": "component:migration", "side": "output"}, "issues": []}},
    ]:
        assert_invalid(
            evolution_plugin_validator,
            {
                "outcome": "failure",
                "evolution_plugin_protocol": "cymule.evolution-plugin/3",
                "error": malformed_failure,
            },
            "evolution plugin accepted a failure outside Rust admission",
        )
    assert_invalid(
        evolution_plugin_validator,
        {
            "outcome": "success",
            "evolution_plugin_protocol": "cymule.evolution-plugin/3",
            "response": {
                "type": "migrated",
                "output": {"output_state": migration_output["artifacts"][0], "evidence": migration_output["evidence"]},
            },
        },
        "evolution plugin accepted the legacy migration output",
    )
    for retired_field, retired_value in (
        ("safe_point_id", "sha256:" + "3" * 64),
        ("source_epoch", 7),
        ("source_continuation", target_continuation),
        ("input_state", artifact),
        ("source_binding", execution_binding),
        ("target_binding", target_execution_binding),
    ):
        retired_migration = json.loads(json.dumps(migration_command))
        retired_migration["request"][retired_field] = retired_value
        assert_invalid(
            evolution_validator,
            retired_migration,
            f"Evolution schema accepted retired migration field {retired_field}",
        )
    unsafe_migration = json.loads(json.dumps(migration_command))
    unsafe_migration["request"]["expected_source_epoch"] = 9_007_199_254_740_992
    assert_invalid(
        evolution_validator,
        unsafe_migration,
        "Evolution schema accepted an unsafe migration source epoch",
    )
    migration_receipt = {
        "request": migration_request,
        "source_witness_id": "sha256:" + "3" * 64,
        "source_binding": execution_binding,
        "target_binding": target_execution_binding,
        "source_execution_fence": 3,
        "target_epoch": migration_request["expected_source_epoch"] + 1,
        "adapter_id": migration_request["adapter_id"],
        "adapter_revision": migration_request["adapter_revision"],
        "from_schema": migration_descriptor["from_schema"],
        "to_schema": migration_descriptor["to_schema"],
        "output_state": artifact,
        "target_continuation": target_continuation,
        "evidence": artifact,
    }
    if not (
        migration_receipt["target_epoch"]
        == migration_request["expected_source_epoch"] + 1
        and migration_receipt["adapter_id"] == migration_request["adapter_id"]
        and migration_receipt["adapter_revision"]
        == migration_request["adapter_revision"]
        and target_continuation["run_id"] == migration_request["run_id"]
        and target_continuation["plan_id"] == migration_request["to_plan"]
        and target_continuation["binding_context"]
        == migration_receipt["target_binding"]["artifact_id"]
        and target_continuation["epoch"] == migration_receipt["target_epoch"]
        and target_continuation["state"] == migration_receipt["output_state"]
        and target_continuation["execution_fence"]
        == migration_receipt["source_execution_fence"]
    ):
        raise AssertionError("migration receipt fixture lost terminal correlation")
    migration_live_command = {
        "control_version": "cymule.live-evolution-control/6",
        "command_id": "command:live-migration",
        "operation": "apply",
        "template_id": "template:schema-migration",
        "command": migration_command,
    }
    def evolution_commit(
        evolution_id: str,
        command: dict[str, object],
        outcome: dict[str, object],
        *,
        source_witness_id: str | None = None,
    ) -> dict[str, object]:
        observed_revision = "sha256:" + "8" * 64
        return {
            "observed_revision": observed_revision,
            "committed_revision": observed_revision,
            "receipt": {
                "receipt_version": "cymule.evolution-persistence-receipt/4",
                "receipt_id": "sha256:" + "7" * 64,
                "command": {
                    "persistence_version": "cymule.evolution-persistence-command/4",
                    "persistence_id": "sha256:" + "6" * 64,
                    "evolution_id": evolution_id,
                    "command": command,
                },
                "parent_current_id": None,
                "source_witness_id": source_witness_id,
                "outcome": outcome,
                "mutations": [],
                "mutation_id": "sha256:" + "4" * 64,
            },
        }

    migration_evolution_id = "evolution:schema-migration"
    migration_process = json.loads(json.dumps(direct_run_request["plugin"]))
    migration_process["provider"] = "cymule.executor-process/1"
    migration_process["revision"] = migration_request["adapter_revision"]
    migration_process["process"]["message_limit"] = 16 * 1024 * 1024
    migration_engine_request = {
        "type": "execute_live_evolution",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-migration-domain",
            },
            "migration_adapter": {
                "adapter_id": migration_request["adapter_id"],
                "adapter_revision": migration_request["adapter_revision"],
                "process": migration_process,
            },
            "shadow_driver": None,
            "target_execution_bindings": {},
        },
        "evolution_id": migration_evolution_id,
        "command": migration_live_command,
    }
    validate_engine_request(migration_engine_request)
    missing_target_bindings = json.loads(json.dumps(migration_engine_request))
    del missing_target_bindings["target"]["target_execution_bindings"]
    assert_invalid(
        engine_validator,
        {"engine_protocol": "cymule.engine/5", "request": missing_target_bindings},
        "Evolution Engine target omitted target_execution_bindings",
    )
    target_execution = json.loads(json.dumps(direct_run_request["plugin"]))
    target_execution["provider"] = "cymule.executor-process/1"
    target_execution["revision"] = "sha256:" + "d" * 64
    exact_target_binding_request = json.loads(json.dumps(migration_engine_request))
    exact_target_binding_request["target"]["target_execution_bindings"] = {
        migration_request["to_plan"]: target_execution
    }
    validate_engine_request(exact_target_binding_request)
    too_many_target_bindings = json.loads(json.dumps(exact_target_binding_request))
    too_many_target_bindings["target"]["target_execution_bindings"][
        "sha256:" + "e" * 64
    ] = target_execution
    assert_invalid(
        engine_validator,
        {
            "engine_protocol": "cymule.engine/5",
            "request": too_many_target_bindings,
        },
        "Evolution Engine target accepted more than one target execution binding",
    )
    unpinned_target_binding = json.loads(json.dumps(exact_target_binding_request))
    del next(
        iter(unpinned_target_binding["target"]["target_execution_bindings"].values())
    )["revision"]
    assert_invalid(
        engine_validator,
        {
            "engine_protocol": "cymule.engine/5",
            "request": unpinned_target_binding,
        },
        "Evolution Engine target accepted an unpinned target execution binding",
    )
    for invalid_limit in (16 * 1024 * 1024 - 1, 16 * 1024 * 1024 + 1):
        invalid_migration_limit = json.loads(json.dumps(migration_engine_request))
        invalid_migration_limit["target"]["migration_adapter"]["process"]["process"][
            "message_limit"
        ] = invalid_limit
        assert_invalid(
            engine_validator,
            {
                "engine_protocol": "cymule.engine/5",
                "request": invalid_migration_limit,
            },
            "Evolution Engine plugin accepted a narrowed or widened message limit",
        )
    migration_response = engine_success(
        migration_engine_request,
        {
            "type": "live_evolution_executed",
            "commit": evolution_commit(
                migration_evolution_id,
                migration_live_command,
                {
                    "result": "migrated",
                    "receipt": migration_receipt,
                },
                source_witness_id=migration_receipt["source_witness_id"],
            ),
        },
    )
    engine_validator.validate(migration_response)
    for required_field in migration_receipt:
        incomplete_receipt = json.loads(json.dumps(migration_response))
        del incomplete_receipt["response"]["commit"]["receipt"]["outcome"]["receipt"][
            required_field
        ]
        assert_invalid(
            engine_validator,
            incomplete_receipt,
            f"Engine schema accepted migration receipt without {required_field}",
        )
    for field, invalid in (
        ("source_witness_id", "witness:forged"),
        ("source_execution_fence", 9_007_199_254_740_992),
        ("adapter_id", ""),
        ("adapter_revision", "revision:forged"),
    ):
        malformed_receipt = json.loads(json.dumps(migration_response))
        malformed_receipt["response"]["commit"]["receipt"]["outcome"]["receipt"][field] = (
            invalid
        )
        assert_invalid(
            engine_validator,
            malformed_receipt,
            f"Engine schema accepted malformed migration receipt {field}",
        )
    for binding_field in ("source_binding", "target_binding"):
        malformed_binding = json.loads(json.dumps(migration_response))
        malformed_binding["response"]["commit"]["receipt"]["outcome"]["receipt"][
            binding_field
        ]["kind"] = "cymule.input/1"
        assert_invalid(
            engine_validator,
            malformed_binding,
            f"Engine schema accepted non-execution {binding_field}",
        )
    running_target = json.loads(json.dumps(migration_response))
    running_target_continuation = running_target["response"]["commit"]["receipt"][
        "outcome"
    ]["receipt"]["target_continuation"]
    running_target_continuation["execution_claim"] = claim
    running_target_continuation["status"] = "running"
    assert_invalid(
        engine_validator,
        running_target,
        "migration receipt accepted a running target Continuation",
    )
    empty_target_frames = json.loads(json.dumps(migration_response))
    empty_target_frames["response"]["commit"]["receipt"]["outcome"]["receipt"][
        "target_continuation"
    ]["frames"] = []
    assert_invalid(
        engine_validator,
        empty_target_frames,
        "migration receipt accepted an empty target frame stack",
    )
    unsafe_target_epoch = json.loads(json.dumps(migration_response))
    unsafe_target_epoch["response"]["commit"]["receipt"]["outcome"]["receipt"]["target_epoch"] = (
        9_007_199_254_740_992
    )
    assert_invalid(
        engine_validator,
        unsafe_target_epoch,
        "Engine schema accepted an unsafe migration target epoch",
    )
    zero_target_epoch = json.loads(json.dumps(migration_response))
    zero_target_epoch["response"]["commit"]["receipt"]["outcome"]["receipt"]["target_epoch"] = 0
    assert_invalid(
        engine_validator,
        zero_target_epoch,
        "Engine schema accepted a zero migration target epoch",
    )
    oversized_artifact_kind = json.loads(json.dumps(migration_response))
    oversized_artifact_kind["response"]["commit"]["receipt"]["outcome"]["receipt"][
        "output_state"
    ]["kind"] = "a/" + "b" * 254
    assert_invalid(
        engine_validator,
        oversized_artifact_kind,
        "Engine schema accepted an Artifact kind longer than 255 characters",
    )
    unsafe_source_epoch = json.loads(json.dumps(migration_response))
    unsafe_source_epoch["response"]["commit"]["receipt"]["outcome"]["receipt"]["request"][
        "expected_source_epoch"
    ] = 9_007_199_254_740_992
    assert_invalid(
        engine_validator,
        unsafe_source_epoch,
        "Engine schema accepted an unsafe migration source epoch",
    )
    for field in [
        "epoch",
        "execution_fence",
        "next_step",
        "region_path",
        "invocation_region_path",
    ]:
        unsafe_continuation = json.loads(json.dumps(migration_response))
        receipt = unsafe_continuation["response"]["commit"]["receipt"]["outcome"]["receipt"]
        continuation = receipt["target_continuation"]
        if field == "epoch":
            continuation["epoch"] = 9_007_199_254_740_992
        elif field == "execution_fence":
            continuation["execution_fence"] = 9_007_199_254_740_992
        elif field == "next_step":
            continuation["frames"][0]["next_step"] = 9_007_199_254_740_992
        elif field == "region_path":
            continuation["frames"][0]["region_path"] = [9_007_199_254_740_992]
        else:
            continuation["frames"][0]["invocation_path"] = [{
                "site_id": "site:schema",
                "region_path": [9_007_199_254_740_992],
                "scope_id": "scope:root",
            }]
        assert_invalid(
            engine_validator,
            unsafe_continuation,
            f"Engine schema accepted unsafe target Continuation {field}",
        )
    for field in ["frames", "scope_stack"]:
        empty_target = json.loads(json.dumps(migration_response))
        empty_target["response"]["commit"]["receipt"]["outcome"]["receipt"]["target_continuation"][
            field
        ] = []
        assert_invalid(
            engine_validator,
            empty_target,
            f"Engine schema accepted an empty migration target {field}",
        )
    for invalid_version in (None, "cymule.continuation-state/0"):
        invalid_target_version = json.loads(json.dumps(migration_response))
        continuation = invalid_target_version["response"]["commit"]["receipt"][
            "outcome"
        ]["receipt"]["target_continuation"]
        if invalid_version is None:
            del continuation["continuation_version"]
        else:
            continuation["continuation_version"] = invalid_version
        assert_invalid(
            engine_validator,
            invalid_target_version,
            "Engine schema accepted a missing or predecessor Continuation state version",
        )

    ghost_invocation_epoch = json.loads(json.dumps(migration_response))
    ghost_invocation_epoch["response"]["commit"]["receipt"]["outcome"]["receipt"][
        "target_continuation"
    ]["frames"][0]["invocation_path"] = [{
        "site_id": "site:schema",
        "region_path": [],
        "scope_id": "scope:root",
        "epoch": 0,
    }]
    assert_invalid(
        engine_validator,
        ghost_invocation_epoch,
        "Engine schema accepted a ghost invocation-path epoch",
    )

    legacy_migration_receipt = {
        **migration_request,
        "target_epoch": migration_receipt["target_epoch"],
        "adapter_id": migration_receipt["adapter_id"],
        "adapter_revision": migration_receipt["adapter_revision"],
        "from_schema": migration_receipt["from_schema"],
        "to_schema": migration_receipt["to_schema"],
        "output_state": artifact,
        "target_continuation": target_continuation,
        "evidence": artifact,
    }
    legacy_migration_response = json.loads(json.dumps(migration_response))
    legacy_migration_response["response"]["commit"]["receipt"]["outcome"]["receipt"] = (
        legacy_migration_receipt
    )
    assert_invalid(
        engine_validator,
        legacy_migration_response,
        "Engine schema accepted the flattened migration receipt",
    )

    restart_live_command = {
        "control_version": "cymule.live-evolution-control/6",
        "command_id": "command:live-restart",
        "operation": "apply",
        "template_id": "template:schema-restart",
        "command": next(
            command
            for command in evolution_variants
            if command["operation"] == "restart_under_new_plan"
        ),
    }
    restart_evolution_id = "evolution:schema-restart"
    restart_engine_request = {
        "type": "execute_live_evolution",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-restart-domain",
            },
            "migration_adapter": None,
            "shadow_driver": None,
            "target_execution_bindings": {},
        },
        "evolution_id": restart_evolution_id,
        "command": restart_live_command,
    }
    restart_response = engine_success(
        restart_engine_request,
        {
            "type": "live_evolution_executed",
            "commit": evolution_commit(
                restart_evolution_id,
                restart_live_command,
                {
                    "result": "restart_authorized",
                    "receipt": {
                        "request": restart_request,
                        "source_witness_id": "sha256:" + "6" * 64,
                        "target_plan": {
                            "plan_id": restart_request["to_plan"],
                            "candidate": candidate,
                        },
                    },
                },
                source_witness_id="sha256:" + "6" * 64,
            ),
        },
    )
    engine_validator.validate(restart_response)
    legacy_restart_request = json.loads(json.dumps(restart_engine_request))
    del legacy_restart_request["evolution_id"]
    legacy_restart_request["journal_id"] = "journal:schema-restart"
    assert_invalid(
        engine_validator,
        {
            "engine_protocol": "cymule.engine/5",
            "request": legacy_restart_request,
        },
        "Engine schema accepted a retired live-evolution journal identity",
    )
    restart_receipt = restart_response["response"]["commit"]["receipt"]["outcome"]["receipt"]
    if restart_receipt["target_plan"]["plan_id"] != restart_request["to_plan"]:
        raise AssertionError("restart receipt fixture lost target Plan correlation")
    for required_field in restart_receipt:
        incomplete_restart_receipt = json.loads(json.dumps(restart_response))
        del incomplete_restart_receipt["response"]["commit"]["receipt"]["outcome"]["receipt"][
            required_field
        ]
        assert_invalid(
            engine_validator,
            incomplete_restart_receipt,
            f"Engine schema accepted restart receipt without {required_field}",
        )
    malformed_restart_witness = json.loads(json.dumps(restart_response))
    malformed_restart_witness["response"]["commit"]["receipt"]["outcome"]["receipt"][
        "source_witness_id"
    ] = "witness:forged"
    assert_invalid(
        engine_validator,
        malformed_restart_witness,
        "Engine schema accepted a malformed restart source witness",
    )
    fn_restart_response = json.loads(json.dumps(restart_response))
    fn_step = fn_restart_response["response"]["commit"]["receipt"]["outcome"]["receipt"][
        "target_plan"
    ]["candidate"]["definitions"][0]["body"]["steps"][0]
    fn_step["op"] = "fn"
    assert_invalid(
        engine_validator,
        fn_restart_response,
        "Engine schema accepted an internal Fn operation token on the wire",
    )

    shadow_live_command = {
        "control_version": "cymule.live-evolution-control/6",
        "command_id": "command:live-shadow",
        "operation": "apply",
        "template_id": "template:schema-shadow",
        "command": shadow_command,
    }
    shadow_evolution_id = "evolution:schema-shadow"
    shadow_engine_request = {
        "type": "execute_live_evolution",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-shadow-domain",
            },
            "migration_adapter": None,
            "shadow_driver": None,
            "target_execution_bindings": {},
        },
        "evolution_id": shadow_evolution_id,
        "command": shadow_live_command,
    }
    shadow_request = shadow_command["request"]
    shadow_comparison = {
        "comparison_id": shadow_request["comparison_id"],
        "subject": shadow_request["subject"],
        "decision_id": shadow_request["decision_id"],
        "primary_plan": shadow_request["primary_plan"],
        "shadow_plan": shadow_request["shadow_plan"],
        "driver_id": shadow_request["driver_id"],
        "driver_revision": shadow_request["driver_revision"],
        "comparison_policy": shadow_request["comparison_policy"],
        "primary_digest": "a" * 64,
        "shadow_digest": "b" * 64,
        "equivalent": True,
        "evidence": artifact,
    }
    if any(
        shadow_comparison[field] != shadow_request[field]
        for field in (
            "comparison_id", "subject", "decision_id", "primary_plan",
            "shadow_plan", "driver_id", "driver_revision", "comparison_policy",
        )
    ):
        raise AssertionError("shadow comparison fixture lost request correlation")
    shadow_response = engine_success(
        shadow_engine_request,
        {
            "type": "live_evolution_executed",
            "commit": evolution_commit(
                shadow_evolution_id,
                shadow_live_command,
                {
                    "result": "shadow_recorded",
                    "comparison": shadow_comparison,
                },
            ),
        },
    )
    engine_validator.validate(shadow_response)
    for required_pin in ("driver_id", "driver_revision"):
        incomplete_shadow_response = json.loads(json.dumps(shadow_response))
        del incomplete_shadow_response["response"]["commit"]["receipt"]["outcome"][
            "comparison"
        ][required_pin]
        assert_invalid(
            engine_validator,
            incomplete_shadow_response,
            f"Engine schema accepted shadow comparison without {required_pin}",
        )

    live_evolution = load(root / "tests/fixtures/live-evolution-control.json")
    live_evolution_schema = by_title[
        "Cymule Live Evolution Control cymule.live-evolution-control/6"
    ]
    live_evolution_validator = Draft202012Validator(
        live_evolution_schema,
        registry=registry,
    )
    reference_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/live-evolution-control.schema.json"
                "#/$defs/reference"
            )
        },
        registry=registry,
    )
    explicit_reference = {
        "logical_ref": "definition:dependency",
        "local_definition": "dependency",
        "input_schema": {},
        "output_schema": {},
        "strategy": {"strategy": "latest_compatible"},
    }
    reference_validator.validate(explicit_reference)
    assert_invalid(
        reference_validator,
        {key: value for key, value in explicit_reference.items() if key != "strategy"},
        "live-evolution reference accepted an omitted strategy",
    )
    live_evolution_validator.validate(live_evolution)
    legacy_live_evolution = json.loads(json.dumps(live_evolution))
    legacy_live_evolution["control_version"] = legacy_versions[
        "live_evolution_control"
    ]
    assert_invalid(
        live_evolution_validator,
        legacy_live_evolution,
        "live-evolution schema accepted generation /4 under the /5 contract",
    )
    live_evolution_validator.validate(migration_live_command)
    live_evolution_validator.validate(restart_live_command)
    live_evolution_validator.validate(shadow_live_command)
    for label, command in (
        ("migration", migration_live_command),
        ("restart", restart_live_command),
        ("non-migration", live_evolution),
    ):
        for retired_authority in ({"retired": True}, None):
            unsafe_command = json.loads(json.dumps(command))
            unsafe_command["safe_point"] = retired_authority
            assert_invalid(
                live_evolution_validator,
                unsafe_command,
                f"live-evolution schema accepted an outer safe point on {label}",
            )
    evolution_control_schema = by_title[
        "Cymule Evolution Control cymule.evolution-control/5"
    ]
    if "safePoint" in evolution_control_schema["$defs"]:
        raise AssertionError("Evolution schema still publishes MigrationSafePoint")
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
    occurrence_pin = {
        "occurrence_id": "occurrence:fixture:1",
        "template_id": "template:review-parent",
        "decision_id": "rollout:fixture:canary",
        "plan_id": "sha256:" + "2" * 64,
        "execution_binding": execution_binding,
        "selection_id": "selection:fixture:1",
    }
    live_evolution_id = "evolution:schema-live"
    live_select_engine_request = {
        "type": "execute_live_evolution",
        "target": {
            "store": {
                "provider": "cymule.directory-store/5",
                "location": "/tmp/cymule-schema-domain",
            },
            "migration_adapter": None,
            "shadow_driver": None,
            "target_execution_bindings": {},
        },
        "evolution_id": live_evolution_id,
        "command": live_evolution,
    }
    valid_live_commit_response = engine_success(
        live_select_engine_request,
        {
            "type": "live_evolution_executed",
            "commit": evolution_commit(
                live_evolution_id,
                live_evolution,
                {"result": "occurrence_selected", "pin": occurrence_pin},
            ),
        },
    )
    engine_validator.validate(valid_live_commit_response)
    malformed_pin = json.loads(json.dumps(valid_live_commit_response))
    del malformed_pin["response"]["commit"]["receipt"]["outcome"]["pin"]["decision_id"]
    assert_invalid(
        engine_validator,
        malformed_pin,
        "Engine schema accepted an incomplete occurrence pin",
    )
    validate_engine_request(
        {"type": "verify_live_evolution_command", "command": live_evolution}
    )
    validate_engine_request(live_select_engine_request)
    applied_live_command = {
        "control_version": "cymule.live-evolution-control/6",
        "command_id": "command:live-rollout",
        "operation": "apply",
        "template_id": "template:schema-rollout",
        "command": next(
            command
            for command in evolution_variants
            if command["operation"] == "set_rollout"
        ),
    }
    applied_evolution_id = "evolution:schema-live-applied"
    applied_engine_request = {
        **live_select_engine_request,
        "evolution_id": applied_evolution_id,
        "command": applied_live_command,
    }
    engine_validator.validate(
        engine_success(
            applied_engine_request,
            {
                "type": "live_evolution_executed",
                "commit": evolution_commit(
                    applied_evolution_id, applied_live_command, {"result": "applied"}
                ),
            },
        )
    )
    for missing in ("observed_revision", "committed_revision", "receipt"):
        malformed = json.loads(json.dumps(valid_live_commit_response))
        del malformed["response"]["commit"][missing]
        assert_invalid(
            engine_validator, malformed,
            f"Engine schema accepted a live-evolution commit without {missing}",
        )
    for missing in valid_live_commit_response["response"]["commit"]["receipt"]:
        malformed = json.loads(json.dumps(valid_live_commit_response))
        del malformed["response"]["commit"]["receipt"][missing]
        assert_invalid(
            engine_validator, malformed,
            f"Engine schema accepted a live-evolution persistence receipt without {missing}",
        )
    for malformed_identity in ("", "e" * 257, "evolution:\u0085control"):
        malformed = json.loads(json.dumps(valid_live_commit_response))
        malformed["response"]["commit"]["receipt"]["command"]["evolution_id"] = malformed_identity
        assert_invalid(
            engine_validator, malformed,
            "Engine schema accepted a malformed persistence Evolution identity",
        )
    legacy_live_response = json.loads(json.dumps(valid_live_commit_response))
    legacy_live_response["response"] = {
        "type": "live_evolution_executed",
        "receipt": {
            "journal_id": "schema:live-evolution",
            "command": live_evolution,
            "outcome": {"result": "occurrence_selected", "pin": occurrence_pin},
        },
    }
    assert_invalid(
        engine_validator, legacy_live_response,
        "Engine schema accepted the retired journal-shaped live-evolution receipt",
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

    virtual_schema = by_title["Cymule Virtual Control Contracts"]
    virtual_validator = Draft202012Validator(virtual_schema, registry=registry)
    retired_virtual = load(root / "tests/harness/fixtures/retired-virtual-contracts.json")
    for retired in retired_virtual["cases"]:
        assert_invalid(
            virtual_validator,
            retired["value"],
            f"current Virtual controls accepted retired model {retired['name']}",
        )
    virtual_identity_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-control.schema.json"
                "#/$defs/virtualIdentity"
            )
        },
        registry=registry,
    )
    virtual_identity_validator.validate("界" * 512)
    for malformed_identity in ["", "界" * 513, "virtual:\u0000forged", "virtual:\u0085forged"]:
        assert_invalid(
            virtual_identity_validator,
            malformed_identity,
            "virtual identity accepted an invalid Unicode scalar boundary",
        )
    virtual_park_reason_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-control.schema.json"
                "#/$defs/parkReason"
            )
        },
        registry=registry,
    )
    virtual_park_reason_validator.validate(
        {"kind": "wait", "key": "sha256:" + "a" * 64}
    )
    assert_invalid(
        virtual_park_reason_validator,
        {"kind": "wait", "key": "wait:not-content-addressed"},
        "Virtual Wait park reason accepted a non-content M1 Wait identity",
    )
    wait_activation_receipt = {
        "receipt_version": "cymule.wait-activation-receipt/3",
        "activation": wait_activation,
        "applied_wait_ids": wait_activation["wait_ids"],
        "ready_run_ids": ["run:fixture"],
    }
    wait_activation_receipt_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/wait_activation_receipt"
            )
        },
        registry=registry,
    )
    wait_activation_receipt_validator.validate(
        {**wait_activation_receipt, "ready_run_ids": ["界" * 512]}
    )
    for malformed_run_id in ["界" * 513, "run:\u0000forged", "run:\u0085forged"]:
        assert_invalid(
            wait_activation_receipt_validator,
            {**wait_activation_receipt, "ready_run_ids": [malformed_run_id]},
            "wait activation receipt accepted an invalid ready Run identity",
        )
    wait_identity_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/durable-storage.schema.json"
                "#/$defs/wait_identity"
            )
        },
        registry=registry,
    )
    wait_identity_validator.validate("sha256:" + "f" * 64)
    for malformed_wait_id in [
        "wait:not-content-addressed",
        "sha256:" + "F" * 64,
        "sha256:" + "f" * 63,
    ]:
        assert_invalid(
            wait_identity_validator,
            malformed_wait_id,
            "wait activation receipt accepted a non-content wait identity",
        )
    assert_invalid(
        wait_activation_receipt_validator,
        {
            **wait_activation_receipt,
            "applied_wait_ids": ["wait:not-content-addressed"],
        },
        "wait activation receipt accepted a non-content applied wait identity",
    )
    assert_invalid(
        wait_activation_receipt_validator,
        {
            **wait_activation_receipt,
            "applied_wait_ids": [],
            "ready_run_ids": ["run:fixture"],
        },
        "terminal non-winner activation receipt claimed a Ready Run",
    )
    virtual_occurrence = load(root / "tests/fixtures/virtual-work-occurrence.json")
    occurrence_validator = Draft202012Validator(
        {
            "$ref": (
                "https://cymule.dev/schemas/virtual-control.schema.json"
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
                "https://cymule.dev/schemas/virtual-control.schema.json"
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
                "https://cymule.dev/schemas/virtual-control.schema.json"
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
                "https://cymule.dev/schemas/virtual-control.schema.json"
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
                "https://cymule.dev/schemas/virtual-control.schema.json"
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
    safe_integer_field = {
        "virtual-claim-control.json": "lease_ttl",
        "virtual-lease-renewal-control.json": "expected_lease_epoch",
        "virtual-recovery-control.json": "expected_epoch",
        "virtual-run-weight-control.json": "weight",
    }
    for fixture_name, definition, label in scheduling_fixtures:
        value = load(root / "tests/fixtures" / fixture_name)
        validator = Draft202012Validator(
            {
                "$ref": (
                    "https://cymule.dev/schemas/virtual-control.schema.json"
                    f"#/$defs/{definition}"
                )
            },
            registry=registry,
        )
        validator.validate(value)
        unsafe_value = dict(value)
        unsafe_value[safe_integer_field[fixture_name]] = 9_007_199_254_740_992
        assert_invalid(
            validator,
            unsafe_value,
            f"{label} accepted an integer outside the exact JSON range",
        )
        malformed_value = dict(value)
        malformed_value["provider"] = "must-not-enter-virtual-scheduling"
        try:
            validator.validate(malformed_value)
        except ValidationError:
            pass
        else:
            raise AssertionError(f"{label} accepted a provider field")
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
        "resource_version": "cymule.resource/4",
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
