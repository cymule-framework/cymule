"""Governance tests for current Virtual controls and typed storage schemas."""

from __future__ import annotations

import copy
import importlib.util
import pathlib
import re
import unittest

from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource


ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "virtual_schema_version_domains", ROOT / "scripts/version_domains.py"
)
assert SPEC is not None and SPEC.loader is not None
version_domains = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(version_domains)


class VirtualSchemaGovernanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schemas = [
            version_domains.load_json(path)
            for path in sorted((ROOT / "schemas").glob("*.schema.json"))
        ]
        cls.by_title = {schema["title"]: schema for schema in schemas}
        cls.by_id = {schema["$id"]: schema for schema in schemas}
        cls.registry = Registry().with_resources(
            (schema["$id"], Resource.from_contents(schema)) for schema in schemas
        )

    def test_every_frozen_schema_is_valid_draft_2020_12(self) -> None:
        for schema in self.by_id.values():
            with self.subTest(schema=schema["$id"]):
                Draft202012Validator.check_schema(schema)

    def test_current_storage_dto_fields_match_their_source_inventory(self) -> None:
        """Mechanical field parity complements, never replaces, Rust round trips."""
        pairs = (
            ("crates/cymule-core/src/machine.rs", "MachineCommandBatchRecord", "durable-storage.schema.json", "command_batch"),
            ("crates/cymule-core/src/machine.rs", "MachineCommandBatchMaterialSource", "durable-storage.schema.json", "command_batch_material_source"),
            ("crates/cymule-core/src/machine.rs", "CommandAdmission", "durable-storage.schema.json", "command_admission"),
            ("crates/cymule-core/src/machine.rs", "MachineCommandArchiveSegmentHeader", "durable-storage.schema.json", "command_archive_header"),
            ("crates/cymule-core/src/machine.rs", "MachineCommandArchiveSegment", "durable-storage.schema.json", "command_archive_segment"),
            ("crates/cymule-core/src/machine.rs", "MachineBaseSnapshot", "durable-storage.schema.json", "machine_base"),
            ("crates/cymule-core/src/machine.rs", "MachineBaseAnchor", "durable-storage.schema.json", "machine_base_anchor"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachineAuthorityFrontier", "durable-storage.schema.json", "machine_authority_frontier"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachineRunCurrent", "durable-storage.schema.json", "machine_run_current"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachineScopeCurrent", "durable-storage.schema.json", "machine_scope_current"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachinePagedTransitionCurrent", "durable-storage.schema.json", "machine_paged_transition_current"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachinePagedBatchManifest", "durable-storage.schema.json", "machine_paged_batch_manifest"),
            ("crates/cymule-core/src/machine/pinned.rs", "MachinePagedMaterialRoots", "durable-storage.schema.json", "machine_paged_material_roots"),
            ("crates/cymule-durable/src/state_root.rs", "StateRootManifest", "durable-storage.schema.json", "state_root_manifest"),
            ("crates/cymule-durable/src/state_root.rs", "StateRoots", "durable-storage.schema.json", "state_roots"),
            ("crates/cymule-durable/src/store.rs", "StoreHead", "durable-storage.schema.json", "head"),
            ("crates/cymule-durable/src/model.rs", "DurableState", "durable-storage.schema.json", "durable_state"),
            ("crates/cymule-durable/src/model.rs", "ComponentOccurrence", "engine-protocol.schema.json", "componentOccurrence"),
            ("crates/cymule-durable/src/model.rs", "OperationAttempt", "engine-protocol.schema.json", "operationAttempt"),
            ("crates/cymule-durable/src/model.rs", "AgentWorkspaceCheckpoint", "durable-storage.schema.json", "agent_workspace_checkpoint"),
        )

        def schema_fields(schema: dict, node: dict) -> set[str]:
            fields = set(node.get("properties", {}))
            if reference := node.get("$ref"):
                self.assertTrue(reference.startswith("#/$defs/"), reference)
                fields.update(schema_fields(schema, schema["$defs"][reference.removeprefix("#/$defs/")]))
            for branch in node.get("allOf", []):
                fields.update(schema_fields(schema, branch))
            return fields

        for source_path, name, schema_path, definition in pairs:
            with self.subTest(dto=name):
                masked = version_domains.rust_code_mask((ROOT / source_path).read_text())
                match = re.search(r"\bstruct\s+" + re.escape(name) + r"\s*\{", masked)
                self.assertIsNotNone(match, source_path)
                assert match is not None
                start, end, depth = match.end(), match.end(), 1
                while depth:
                    depth += (masked[end] == "{") - (masked[end] == "}")
                    end += 1
                fields = set(re.findall(
                    r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?([a-z][a-z0-9_]*)\s*:",
                    masked[start:end - 1],
                ))
                schema = self.by_id[f"https://cymule.dev/schemas/{schema_path}"]
                self.assertEqual(fields, schema_fields(schema, schema["$defs"][definition]))

    def test_artifact_record_bytes_are_exact_bounded_canonical_base64(self) -> None:
        schemas = []
        for path, definition in (
            ("durable-storage.schema.json", "artifact_record"),
            ("evolution-control.schema.json", "artifactRecord"),
            ("live-evolution-control.schema.json", "artifactRecord"),
        ):
            schemas.append(
                self.by_id[f"https://cymule.dev/schemas/{path}"]["$defs"]
                [definition]["properties"]["bytes"]
            )
        expected_maximum = ((8 * 1024 * 1024 + 2) // 3) * 4
        for schema in schemas:
            self.assertEqual(schema, schemas[0])
            self.assertEqual(schema["maxLength"], expected_maximum)
            self.assertEqual(
                schema["allOf"],
                [{"if": {"minLength": expected_maximum}, "then": {"pattern": "=$"}}],
            )
            validator = Draft202012Validator(schema)
            for canonical in ("", "AA==", "AAA=", "AAAA", "YQ==", "YWI="):
                validator.validate(canonical)
            for malformed in (
                None,
                [97],
                "YQ",
                "YQ=",
                "A===",
                "Zh==",
                "YWJ=",
                "AA==\n",
                "AA==\r\n",
                "AA== ",
                "AA==é",
            ):
                with self.subTest(malformed=malformed):
                    with self.assertRaises(ValidationError):
                        validator.validate(malformed)

        # The cap is two modulo three. At its maximum encoded length a missing
        # padding character would admit one extra decoded byte. Exercise that
        # rule on the same remainder with a small cap, independently of regex
        # engine allocation behavior on an eleven-MiB input.
        bounded = copy.deepcopy(schemas[0])
        bounded["maxLength"] = 12
        bounded["allOf"][0]["if"]["minLength"] = 12
        validator = Draft202012Validator(bounded)
        validator.validate("AAAAAAAAAA==")
        validator.validate("AAAAAAAAAAA=")
        with self.assertRaises(ValidationError):
            validator.validate("AAAAAAAAAAAA")

    def test_storage_leaf_text_and_binary_chunks_have_one_current_codec(self) -> None:
        schema = self.by_id["https://cymule.dev/schemas/durable-storage.schema.json"]
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/state_root_value"}, registry=self.registry
        )
        leaf = {"value": "leaf", "kind": "journal_record", "canonical_json": "{}"}
        validator.validate(leaf)
        for retired in ([123, 125], [], None):
            with self.subTest(retired_leaf=retired):
                with self.assertRaises(ValidationError):
                    validator.validate({**leaf, "canonical_json": retired})

        chunk = {"value": "machine_base_chunk", "index": 0, "bytes": "e30="}
        validator.validate(chunk)
        validator.validate({**chunk, "bytes": "gA=="})  # An isolated UTF-8 continuation byte.
        for invalid in ([123, 125], "", "e30", "AB==", "AAB=", "e30=\n", "e3_="):
            with self.subTest(chunk_bytes=invalid):
                with self.assertRaises(ValidationError):
                    validator.validate({**chunk, "bytes": invalid})
        chunk_schema = next(
            branch["properties"]["bytes"]
            for branch in schema["$defs"]["state_root_value"]["oneOf"]
            if branch["properties"]["value"].get("const") == "machine_base_chunk"
        )
        self.assertEqual(chunk_schema["maxLength"], ((4 * 1024 * 1024 + 2) // 3) * 4)
        self.assertEqual(
            chunk_schema["allOf"],
            [{"if": {"minLength": 5592408}, "then": {"pattern": "==$"}}],
        )
        bounded = copy.deepcopy(chunk_schema)
        bounded["maxLength"] = 8
        bounded["allOf"][0]["if"]["minLength"] = 8
        validator = Draft202012Validator(bounded)
        validator.validate("AAAAAA==")  # Four decoded bytes, the same modulo-three boundary.
        for oversized in ("AAAAAAA=", "AAAAAAAA"):
            with self.assertRaises(ValidationError):
                validator.validate(oversized)

    def test_run_query_terminal_companion_is_required_nullable_and_closed(self) -> None:
        schema = self.by_id["https://cymule.dev/schemas/durable-storage.schema.json"]
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/run_query_indexes"},
            registry=self.registry,
        )
        fixture = version_domains.load_json(
            ROOT / "tests/harness/fixtures/durable-storage-state-root.json"
        )
        indexes = next(
            value["value"]
            for value in fixture["state_root_objects"]
            if value.get("value", {}).get("value") == "run_query_indexes"
        )
        validator.validate(indexes)
        with self.assertRaises(ValidationError):
            validator.validate({key: value for key, value in indexes.items() if key != "terminal"})

        empty_root = {"node": None, "entries": 0}
        terminal = {
            "transition_id": "sha256:" + "1" * 64,
            "transition_digest": "2" * 64,
            "source_continuation_digest": "3" * 64,
            "source_query_digest": "4" * 64,
            "effects": empty_root,
            "active_effects": empty_root,
            "active_leases": empty_root,
        }
        validator.validate({**indexes, "terminal": terminal})
        for field in terminal:
            with self.subTest(missing_terminal_field=field):
                malformed = {key: value for key, value in terminal.items() if key != field}
                with self.assertRaises(ValidationError):
                    validator.validate({**indexes, "terminal": malformed})
        for field in (
            "transition_digest",
            "source_continuation_digest",
            "source_query_digest",
        ):
            with self.subTest(tampered_digest=field):
                with self.assertRaises(ValidationError):
                    validator.validate({
                        **indexes,
                        "terminal": {**terminal, field: "A" * 64},
                    })
        with self.assertRaises(ValidationError):
            validator.validate({
                **indexes,
                "terminal": {
                    **terminal,
                    "effects": {"node": "not-a-content-id", "entries": 1},
                },
            })

    def test_resource_page_proofs_enforce_page_and_path_bounds(self) -> None:
        schema = self.by_id["https://cymule.dev/schemas/resource.schema.json"]
        proof = {
            "proof_version": "cymule.resource-list-proof/5",
            "manifest_digest": "sha256:" + "a" * 64,
            "entry_count": 1000,
            "start_index": 0,
            "request_cursor_digest": "sha256:" + "b" * 64,
            "next_cursor_digest": "sha256:" + "c" * 64,
            "predecessor": None,
            "inclusions": [{"index": index, "path": []} for index in range(1000)],
        }
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/listProof"}, registry=self.registry
        )
        validator.validate(proof)
        proof["inclusions"].append({"index": 1000, "path": []})
        with self.assertRaises(ValidationError):
            validator.validate(proof)

        inclusion = {
            "index": 0,
            "path": [{"side": "left", "digest": "sha256:" + "d" * 64}] * 53,
        }
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/inclusionProof"}, registry=self.registry
        )
        validator.validate(inclusion)
        inclusion["path"].append({"side": "right", "digest": "sha256:" + "e" * 64})
        with self.assertRaises(ValidationError):
            validator.validate(inclusion)
        self.assertEqual(
            schema["$defs"]["manifestPredecessorProof"]["properties"]["inclusion"],
            {"$ref": "#/$defs/inclusionProof"},
        )

    def test_durable_storage_fixture_closes_batch_authority(self) -> None:
        schema = self.by_id["https://cymule.dev/schemas/durable-storage.schema.json"]
        fixture = version_domains.load_json(
            ROOT / "tests/harness/fixtures/durable-storage-state-root.json"
        )
        for field, definition in (
            ("head", "head"),
            ("machine_base_anchor", "machine_base_anchor"),
            ("gc_receipt", "gc_receipt"),
            ("state_root_objects", "state_root_object"),
            ("command_archive_objects", "command_archive_object"),
        ):
            validator = Draft202012Validator(
                {"$ref": schema["$id"] + f"#/$defs/{definition}"},
                registry=self.registry,
            )
            values = fixture[field] if isinstance(fixture[field], list) else [fixture[field]]
            for index, value in enumerate(values):
                with self.subTest(field=field, index=index):
                    validator.validate(value)

        batch = next(
            value["object"]
            for value in fixture["command_archive_objects"]
            if value["object_kind"] == "batch"
        )
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/command_batch"},
            registry=self.registry,
        )
        for parent_field in ("parent_authority_root", "admission_parent_authority_root"):
            with self.subTest(parent_field=parent_field):
                missing = {key: value for key, value in batch.items() if key != parent_field}
                with self.assertRaises(ValidationError):
                    validator.validate(missing)
                with self.assertRaises(ValidationError):
                    validator.validate({**batch, parent_field: None})
        validator.validate({**batch, "admission_parent_authority_root": "c" * 64})

        material_only = {
            **batch,
            "members": [],
            "receipts": [],
            "event_ids": [],
            "material_digest": "sha256:" + "d" * 64,
            "material_source": {
                "source_command_id": "material:fixture",
                "plan_ids": ["sha256:" + "e" * 64],
                "artifacts": [],
            },
            "plan_ids": ["sha256:" + "e" * 64],
        }
        validator.validate(material_only)
        for malformed in (
            {**batch, "material_source": material_only["material_source"]},
            {**material_only, "material_source": None},
            {**material_only, "receipts": batch["receipts"]},
            {**material_only, "event_ids": ["sha256:" + "f" * 64]},
            {**material_only, "material_source": {"source_command_id": "material:fixture", "plan_ids": [], "artifacts": []}},
            {key: value for key, value in batch.items() if key != "material_source"},
        ):
            with self.subTest(material=malformed):
                with self.assertRaises(ValidationError):
                    validator.validate(malformed)

        roots = fixture["state_root_objects"][0]["roots"]
        validator = Draft202012Validator(
            {"$ref": schema["$id"] + "#/$defs/state_roots"}, registry=self.registry
        )
        self.assertIsNone(roots["history_compaction_head"])
        validator.validate(roots)
        for malformed in (
            {key: value for key, value in roots.items() if key != "history_compaction_head"},
            {**roots, "history_compaction_head": "a" * 64},
            {**roots, "history_compaction_head": {"node": None, "entries": 0}},
            {**roots, "history_compaction_head": "sha256:" + "a" * 64},
            {**roots, "machine_base": "sha256:" + "a" * 64},
            {**roots, "history_compactions": {"node": "sha256:" + "a" * 64, "entries": 1}},
        ):
            with self.subTest(history_head=malformed.get("history_compaction_head")):
                with self.assertRaises(ValidationError):
                    validator.validate(malformed)

        kind = Draft202012Validator(schema["$defs"]["history_compaction"]["properties"]["kind"])
        kind.validate("event_prefix")
        kind.validate("event_free_admissions")
        with self.assertRaises(ValidationError):
            kind.validate("conflict_admissions")

    def test_current_virtual_control_fixtures_match_live_dtos(self) -> None:
        validator = Draft202012Validator(
            self.by_title["Cymule Virtual Control Contracts"], registry=self.registry
        )
        for filename in (
            "virtual-work-occurrence.json",
            "virtual-work-control.json",
            "virtual-region-migration-control.json",
            "virtual-compaction-control.json",
            "virtual-rehydration-control.json",
            "virtual-claim-control.json",
            "virtual-lease-renewal-control.json",
            "virtual-recovery-control.json",
            "virtual-run-weight-control.json",
        ):
            fixture = version_domains.load_json(ROOT / "tests/fixtures" / filename)
            with self.subTest(fixture=filename):
                validator.validate(fixture)
                with self.assertRaises(ValidationError):
                    validator.validate({**fixture, "provider": "not-a-wire-authority"})

        migration = version_domains.load_json(
            ROOT / "tests/fixtures/virtual-region-migration-control.json"
        )
        del migration["plan"]["migration_revision"]
        with self.assertRaises(ValidationError):
            validator.validate(migration)
        compaction = version_domains.load_json(
            ROOT / "tests/fixtures/virtual-compaction-control.json"
        )
        for field in ("work_ids", "occurrence_ids", "archived_command_ids", "archive"):
            with self.subTest(compaction=field):
                with self.assertRaises(ValidationError):
                    validator.validate({key: value for key, value in compaction.items() if key != field})
        with self.assertRaises(ValidationError):
            validator.validate({**compaction, "command_id": "command:caller-authored"})

    def test_rust_verified_agent_workspace_checkpoint_has_closed_required_wire(self) -> None:
        fixture = version_domains.load_json(
            ROOT / "tests/harness/fixtures/agent-workspace-checkpoint.json"
        )
        schema_id = "https://cymule.dev/schemas/durable-storage.schema.json"
        validator = Draft202012Validator(
            {"$ref": schema_id + "#/$defs/agent_workspace_checkpoint"},
            registry=self.registry,
        )
        validator.validate(fixture)
        self.assertEqual(len(fixture), 19)
        for field in fixture:
            with self.subTest(missing=field):
                with self.assertRaises(ValidationError):
                    validator.validate({key: value for key, value in fixture.items() if key != field})
        for field in ("core_batch_id", "core_batch_receipt_id"):
            with self.subTest(partial_batch=field):
                with self.assertRaises(ValidationError):
                    validator.validate({**fixture, field: "sha256:" + "a" * 64})
        for field in ("source_continuation_digest", "continuation_digest"):
            with self.subTest(raw_digest=field):
                with self.assertRaises(ValidationError):
                    validator.validate({**fixture, field: "a" * 64})
        with self.assertRaises(ValidationError):
            validator.validate({**fixture, "dispatch_clock": {
                "clock_version": "cymule.clock-observation/2",
                "observation_id": "sha256:" + "b" * 64,
                "source_id": "clock:checkpoint",
                "source_generation": "sha256:" + "c" * 64,
                "scope": "scope:checkpoint",
                "logical_time": 1,
                "observed_unix_ms": 1,
            }})
        wrapper = Draft202012Validator(
            {"$ref": schema_id + "#/$defs/coupled_checkpoint"}, registry=self.registry
        )
        wrapper.validate({"kind": "agent_workspace", "checkpoint": fixture})
        with self.assertRaises(ValidationError):
            wrapper.validate({"kind": "agent_workspace", "checkpoint": fixture, "agent_receipt": {}})

    def test_applied_effect_summary_requires_its_canonical_null_artifact(self) -> None:
        fixture = version_domains.load_json(ROOT / "tests/fixtures/applied-effect-summary.json")
        validator = Draft202012Validator(
            {"$ref": "https://cymule.dev/schemas/engine-protocol.schema.json#/$defs/durableEffectSummary"},
            registry=self.registry,
        )
        validator.validate(fixture)
        validator.validate({**fixture, "state": "not_applied", "result": None})
        for malformed in (
            {**fixture, "result": None},
            {**fixture, "state": "not_applied"},
            {key: value for key, value in fixture.items() if key != "result"},
            {**fixture, "reconciliation": "pending"},
        ):
            with self.subTest(summary=malformed):
                with self.assertRaises(ValidationError):
                    validator.validate(malformed)

    def test_every_virtual_object_map_uses_the_exact_identity(self) -> None:
        schema = self.by_title["Cymule Virtual Control Contracts"]
        expected = {"$ref": "#/$defs/virtualIdentity"}
        observed: list[str] = []

        def inspect(node: object, pointer: str = "#") -> None:
            if isinstance(node, dict):
                if node.get("type") == "object" and isinstance(
                    node.get("additionalProperties"), dict
                ):
                    observed.append(pointer)
                    self.assertEqual(node.get("propertyNames"), expected, pointer)
                for key, child in node.items():
                    inspect(
                        child,
                        pointer
                        + "/"
                        + key.replace("~", "~0").replace("/", "~1"),
                    )
            elif isinstance(node, list):
                for index, child in enumerate(node):
                    inspect(child, f"{pointer}/{index}")

        inspect(schema)
        self.assertGreaterEqual(len(observed), 1)

    def test_retired_virtual_models_have_no_current_schema(self) -> None:
        fixture = version_domains.load_json(
            ROOT / "tests/harness/fixtures/retired-virtual-contracts.json"
        )
        validator = Draft202012Validator(
            self.by_title["Cymule Virtual Control Contracts"],
            registry=self.registry,
        )
        self.assertEqual(fixture["status"], "historical")
        self.assertEqual(
            {case["name"] for case in fixture["cases"]},
            {"virtual_checkpoint_v4", "virtual_journal_base_v2", "coupled_claim_receipt"},
        )
        for case in fixture["cases"]:
            with self.subTest(retired=case["name"]):
                with self.assertRaises(ValidationError):
                    validator.validate(case["value"])
        definitions = validator.schema["$defs"]
        for removed in ("snapshot", "delta", "claimReceipt", "compactionReceipt", "mapDelta"):
            self.assertNotIn(removed, definitions)
        for name in ("virtual-checkpoint.schema.json", "virtual-journal-base.schema.json"):
            self.assertFalse((ROOT / "schemas" / name).exists())

    def test_durable_machine_authority_rejects_generic_object_payloads(self) -> None:
        schema = self.by_title["Cymule Durable Storage Records"]
        validator = Draft202012Validator(schema, registry=self.registry)
        snapshot = {
            "snapshot_version": "cymule.machine-snapshot/11",
            "plans": [],
            "artifacts": [],
            "batches": [],
            "events": [],
            "admissions": [],
            "commands": {},
            "command_index_proofs": {},
        }
        validator.evolve(schema=schema["$defs"]["machine_snapshot"]).validate(snapshot)

        mutations = []
        for member in ("batches", "events", "admissions"):
            malformed = copy.deepcopy(snapshot)
            malformed[member] = [{}]
            mutations.append(malformed)
        for member in ("commands", "command_index_proofs"):
            malformed = copy.deepcopy(snapshot)
            malformed[member] = {"command:test": {}}
            mutations.append(malformed)
        malformed = copy.deepcopy(snapshot)
        malformed["base"] = {"projection": {}}
        mutations.append(malformed)

        machine_schema = schema["$defs"]["machine_snapshot"]
        for malformed in mutations:
            with self.subTest(malformed=malformed):
                with self.assertRaises(ValidationError):
                    validator.evolve(schema=machine_schema).validate(malformed)

        for retired_version in (
            "cymule.machine-snapshot/9",
            "cymule.machine-snapshot/10",
        ):
            with self.subTest(retired_version=retired_version):
                malformed = {**snapshot, "snapshot_version": retired_version}
                with self.assertRaises(ValidationError):
                    validator.evolve(schema=machine_schema).validate(malformed)


if __name__ == "__main__":
    unittest.main()
