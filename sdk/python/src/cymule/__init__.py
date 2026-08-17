"""Python authoring and engine client SDK for Cymule."""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path
from typing import Any, Protocol, TypedDict

Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]
ArtifactRef = dict[str, str]
ParkReason = dict[str, str]
WorkResolution = dict[str, Any]


class WorkOccurrence(TypedDict):
    """Binding-pinned M3 work attempt returned by a control transport."""

    occurrence_version: str
    occurrence_id: str
    work_id: str
    region_id: str
    run_id: str
    owner: str
    epoch: int
    occurrence_binding: str
    state: str
    result: ArtifactRef | None
    error: ArtifactRef | None
    next_reason: ParkReason | None


class WorkResolutionCommand(TypedDict):
    """Idempotent owner/epoch-fenced M3 resolution command."""

    control_version: str
    command_id: str
    work_id: str
    owner: str
    epoch: int
    resolution: WorkResolution


class VirtualCursor(TypedDict):
    """Opaque provider-owned region cursor."""

    version: str
    position: str
    exhausted: bool


class VirtualRegion(TypedDict):
    """One active or retired virtual source region."""

    region_id: str
    run_id: str
    source: str
    cursor: VirtualCursor
    estimated_total: int | None


class RegionMigrationRequest(TypedDict):
    """Caller request for an opaque-cursor split or merge plan."""

    migration_id: str
    kind: str
    source_region_ids: list[str]
    target_count: int
    migration_binding: str


class RegionMigrationPlan(TypedDict):
    """Adapter-produced split/merge plan with coverage evidence."""

    migration_version: str
    migration_id: str
    kind: str
    expected_sources: dict[str, VirtualCursor]
    targets: list[VirtualRegion]
    migration_binding: str
    coverage_evidence: ArtifactRef


class RegionMigrationCommand(TypedDict):
    """Idempotent region migration control command."""

    control_version: str
    command_id: str
    plan: RegionMigrationPlan


class RegionMigrationReceipt(TypedDict):
    """Durable source retirement and target activation receipt."""

    plan: RegionMigrationPlan
    retired_regions: list[str]
    active_targets: list[str]


class VirtualCompletionSummary(TypedDict):
    """Bounded projection of one completed virtual region."""

    region_id: str
    run_id: str
    occurrence_count: int
    work_count: int
    succeeded_count: int
    failed_count: int
    cancelled_count: int
    output_digest: str
    evidence_digest: str
    retained_debug_index_digest: str


class VirtualCompactionCertificate(TypedDict):
    """Verified witness retaining exact archived-history interpretation data."""

    certificate_version: str
    certificate_id: str
    source_causal_cut: list[str]
    summary: VirtualCompletionSummary
    summary_state_digest: str
    unresolved_obligations: list[str]
    retained_occurrence_bindings: list[str]
    replay_availability: dict[str, Any]
    rehydration_manifest: ArtifactRef
    compactor_binding: str
    compactor_revision: str


class VirtualCompactionCommand(TypedDict):
    """Idempotent completed-region compaction request."""

    control_version: str
    command_id: str
    region_id: str
    source_causal_cut: list[str]
    compactor_binding: str
    compactor_revision: str


class VirtualCompactionReceipt(TypedDict):
    """Durable compaction command and verified certificate."""

    command: VirtualCompactionCommand
    certificate: VirtualCompactionCertificate


class VirtualRehydrationCommand(TypedDict):
    """Idempotent exact occurrence-selection request."""

    control_version: str
    command_id: str
    certificate_id: str
    occurrence_ids: list[str]


class VirtualRehydrationReceipt(TypedDict):
    """Exact occurrence identities restored into the hot projection."""

    command: VirtualRehydrationCommand
    restored_occurrence_ids: list[str]


class VirtualArchive(Protocol):
    """Replaceable immutable byte archive for completed virtual history."""

    def binding(self) -> str: ...

    def put(self, reference: ArtifactRef, data: bytes) -> None: ...

    def get(self, reference: ArtifactRef) -> bytes: ...


class RegionMigrator(Protocol):
    """Replaceable opaque-cursor split/merge adapter."""

    def binding(self) -> str: ...

    def plan(
        self,
        request: RegionMigrationRequest,
        sources: list[VirtualRegion],
    ) -> RegionMigrationPlan: ...

    def verify(self, plan: RegionMigrationPlan) -> None: ...


class VirtualWorkControl(Protocol):
    """Transport-neutral M3 occurrence query and control interface."""

    def occurrence(self, occurrence_id: str) -> WorkOccurrence | None: ...

    def resolve(self, command: WorkResolutionCommand) -> WorkOccurrence: ...

    def migrate(self, command: RegionMigrationCommand) -> RegionMigrationReceipt: ...

    def compact(self, command: VirtualCompactionCommand) -> VirtualCompactionReceipt: ...

    def rehydrate(self, command: VirtualRehydrationCommand) -> VirtualRehydrationReceipt: ...


class FlowBuilder:
    """Small code-first builder for the frozen language-neutral IR."""

    def __init__(self, name: str, input_schema: Json, output_schema: Json) -> None:
        self._candidate: dict[str, Any] = {
            "ir_version": "cymule.ir/1",
            "name": name,
            "entry": "main",
            "components": [],
            "effects": [],
            "definitions": [
                {
                    "id": "main",
                    "input_schema": input_schema,
                    "output_schema": output_schema,
                    "body": {"steps": [], "result": {"kind": "literal", "value": None}},
                }
            ],
            "metadata": {},
        }

    def component(self, operation_id: str, input_schema: Json, output_schema: Json) -> FlowBuilder:
        self._candidate["components"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "requirements": {},
            }
        )
        return self

    def effect_contract(
        self,
        operation_id: str,
        input_schema: Json,
        output_schema: Json,
        profile: dict[str, Json],
    ) -> FlowBuilder:
        self._candidate["effects"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "profile": profile,
                "requirements": {},
            }
        )
        return self

    def call(self, site: str, component: str, expression: dict[str, Json], bind: str) -> FlowBuilder:
        self._steps().append(
            {"id": site, "op": "call", "component": component, "input": expression, "bind": bind}
        )
        return self

    def effect(
        self,
        site: str,
        effect: str,
        expression: dict[str, Json],
        occurrence: str,
    ) -> FlowBuilder:
        self._steps().append(
            {
                "id": site,
                "op": "effect",
                "effect": effect,
                "input": expression,
                "occurrence": occurrence,
            }
        )
        return self

    def wait(self, site: str, wait: dict[str, Json]) -> FlowBuilder:
        """Append a durable suspension boundary."""
        self._steps().append({"id": site, "op": "wait", "wait": wait})
        return self

    def scope(
        self,
        site: str,
        mode: str,
        body: dict[str, Json],
        bind: str,
    ) -> FlowBuilder:
        """Append a structured transactional or speculative scope."""
        self._steps().append(
            {"id": site, "op": "scope", "mode": mode, "body": body, "bind": bind}
        )
        return self

    def finish(self, result: dict[str, Json]) -> dict[str, Any]:
        self._entry()["body"]["result"] = result
        return copy.deepcopy(self._candidate)

    def _entry(self) -> dict[str, Any]:
        return self._candidate["definitions"][0]

    def _steps(self) -> list[dict[str, Any]]:
        return self._entry()["body"]["steps"]


class ResourceBuilder:
    """Provider-neutral resource candidates sealed only by the Rust engine."""

    @staticmethod
    def text(text: str, annotations: dict[str, str] | None = None) -> dict[str, Any]:
        return {
            "resource_version": "cymule.resource/1",
            "shape": "inline",
            "media_type": "text/plain;charset=utf-8",
            "inline": {"encoding": "utf8", "text": text},
            "integrity": {"kind": "inline"},
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def json(value: Json, annotations: dict[str, str] | None = None) -> dict[str, Any]:
        return {
            "resource_version": "cymule.resource/1",
            "shape": "inline",
            "media_type": "application/json",
            "inline": {"encoding": "json", "value": value},
            "integrity": {"kind": "inline"},
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def external(
        shape: str,
        media_type: str,
        integrity: dict[str, Json],
        locations: list[dict[str, str]],
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if shape == "inline":
            raise ValueError("external resource shape cannot be inline")
        return {
            "resource_version": "cymule.resource/1",
            "shape": shape,
            "media_type": media_type,
            "integrity": copy.deepcopy(integrity),
            "locations": copy.deepcopy(locations),
            "annotations": dict(annotations or {}),
        }

    @staticmethod
    def handoff(
        transfer_id: str,
        from_run: str,
        to_run: str,
        slot: str,
        resource: dict[str, Any],
    ) -> dict[str, Any]:
        """Create one M1 Run-to-Run resource handoff record."""
        return {
            "handoff_version": "cymule.resource-handoff/1",
            "transfer_id": transfer_id,
            "from_run": from_run,
            "to_run": to_run,
            "slot": slot,
            "resource": copy.deepcopy(resource),
        }


class WaitActivationBuilder:
    """Build identified provider-neutral signal and timer deliveries."""

    @staticmethod
    def signal(
        activation_id: str,
        key: str,
        wait_ids: list[str],
        result: dict[str, str],
    ) -> dict[str, Any]:
        return WaitActivationBuilder._build(
            activation_id,
            {"kind": "signal", "key": key},
            wait_ids,
            result,
        )

    @staticmethod
    def timer(
        activation_id: str,
        timer_id: str,
        wait_id: str,
        result: dict[str, str],
    ) -> dict[str, Any]:
        return WaitActivationBuilder._build(
            activation_id,
            {"kind": "timer", "timer_id": timer_id},
            [wait_id],
            result,
        )

    @staticmethod
    def _build(
        activation_id: str,
        source: dict[str, str],
        wait_ids: list[str],
        result: dict[str, str],
    ) -> dict[str, Any]:
        targets = sorted(set(wait_ids))
        if not activation_id or not targets:
            raise ValueError("wait activation requires an identity and at least one target")
        return {
            "activation_version": "cymule.wait-activation/1",
            "activation_id": activation_id,
            "source": source,
            "wait_ids": targets,
            "result": dict(result),
        }


class VirtualWorkControlBuilder:
    """Build owner/epoch-fenced virtual work resolution commands."""

    @staticmethod
    def succeed(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        result: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            {"resolution": "succeeded", "result": dict(result)},
        )

    @staticmethod
    def retry(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        error: ArtifactRef,
        next_reason: ParkReason | None = None,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            {
                "resolution": "retry",
                "error": dict(error),
                "next_reason": dict(next_reason) if next_reason is not None else None,
            },
        )

    @staticmethod
    def fail(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        error: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            {"resolution": "failed", "error": dict(error)},
        )

    @staticmethod
    def park(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        reason: ParkReason,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            {"resolution": "parked", "reason": dict(reason)},
        )

    @staticmethod
    def cancel(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        reason: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            {"resolution": "cancelled", "reason": dict(reason)},
        )

    @staticmethod
    def migration(
        command_id: str,
        plan: RegionMigrationPlan,
    ) -> RegionMigrationCommand:
        if not command_id:
            raise ValueError("virtual region migration requires a command identity")
        return {
            "control_version": "cymule.virtual-region-migration-control/1",
            "command_id": command_id,
            "plan": copy.deepcopy(plan),
        }

    @staticmethod
    def compaction(
        command_id: str,
        region_id: str,
        source_causal_cut: list[str],
        compactor_binding: str,
        compactor_revision: str,
    ) -> VirtualCompactionCommand:
        """Build one completed-region compaction command."""
        causal_cut = sorted(set(source_causal_cut))
        if (
            not command_id
            or not region_id
            or not causal_cut
            or not compactor_binding
            or not compactor_revision
        ):
            raise ValueError(
                "virtual compaction requires identities, a causal cut, binding, and revision"
            )
        return {
            "control_version": "cymule.virtual-compaction-control/1",
            "command_id": command_id,
            "region_id": region_id,
            "source_causal_cut": causal_cut,
            "compactor_binding": compactor_binding,
            "compactor_revision": compactor_revision,
        }

    @staticmethod
    def rehydration(
        command_id: str,
        certificate_id: str,
        occurrence_ids: list[str],
    ) -> VirtualRehydrationCommand:
        """Build one exact occurrence-selection rehydration command."""
        occurrences = sorted(set(occurrence_ids))
        if not command_id or not certificate_id or not occurrences:
            raise ValueError(
                "virtual rehydration requires command, certificate, and occurrence identities"
            )
        return {
            "control_version": "cymule.virtual-rehydration-control/1",
            "command_id": command_id,
            "certificate_id": certificate_id,
            "occurrence_ids": occurrences,
        }

    @staticmethod
    def _build(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        resolution: WorkResolution,
    ) -> WorkResolutionCommand:
        if not command_id or not work_id or not owner or epoch < 1:
            raise ValueError(
                "virtual work control requires command, work, owner, and positive epoch"
            )
        return {
            "control_version": "cymule.virtual-work-control/1",
            "command_id": command_id,
            "work_id": work_id,
            "owner": owner,
            "epoch": epoch,
            "resolution": resolution,
        }


class CliEngine:
    """CLI-backed Engine transport."""

    def __init__(self, executable: str | Path) -> None:
        self.executable = str(executable)

    def seal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        response = self._request({"type": "seal", "candidate": candidate})
        if response.get("type") != "sealed":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["plan"]

    def seal_resource(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Validate and seal a Resource Candidate with the Rust engine."""
        response = self._request({"type": "seal_resource", "candidate": candidate})
        if response.get("type") != "sealed_resource":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["resource"]

    def verify_wait_activation(self, activation: dict[str, Any]) -> dict[str, Any]:
        """Validate an identified signal or timer delivery with the Rust engine."""
        response = self._request(
            {"type": "verify_wait_activation", "activation": activation}
        )
        if response.get("type") != "verified_wait_activation":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["activation"]

    def run(
        self,
        plan: dict[str, Any],
        input_value: Json,
        plugin: str | Path,
        run_id: str,
    ) -> dict[str, Any]:
        response = self._request(
            {
                "type": "run",
                "plan": plan,
                "input": input_value,
                "plugin": str(plugin),
                "run_id": run_id,
            }
        )
        if response.get("type") != "executed":
            raise RuntimeError(f"unexpected engine response: {response!r}")
        return response["result"]

    def _request(self, request: dict[str, Any]) -> dict[str, Any]:
        completed = subprocess.run(
            [self.executable, "rpc"],
            input=json.dumps(request, separators=(",", ":")),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or f"engine exited {completed.returncode}")
        return json.loads(completed.stdout)


__all__ = [
    "ArtifactRef",
    "CliEngine",
    "FlowBuilder",
    "Json",
    "ParkReason",
    "RegionMigrationCommand",
    "RegionMigrationPlan",
    "RegionMigrationReceipt",
    "RegionMigrationRequest",
    "RegionMigrator",
    "ResourceBuilder",
    "VirtualWorkControl",
    "VirtualWorkControlBuilder",
    "VirtualCursor",
    "VirtualRegion",
    "WorkOccurrence",
    "WorkResolution",
    "WorkResolutionCommand",
    "WaitActivationBuilder",
]
