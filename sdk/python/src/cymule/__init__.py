"""Python authoring and engine client SDK for Cymule."""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict

Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]
class ArtifactRef(TypedDict):
    """One immutable Artifact reference under the closed identity version."""

    identity_version: Literal["cymule.artifact/2"]
    artifact_id: str
    kind: str


ParkReason = dict[str, str]
WorkResolution = dict[str, Any]
EvolutionCommand = dict[str, Any]
LiveEvolutionCommand = dict[str, Any]
DurableCommand = dict[str, Any]
ENGINE_PROTOCOL_VERSION = "cymule.engine/1"


class EngineIssue(TypedDict, total=False):
    """One machine-readable validation or contract issue."""

    code: str
    message: str
    path: str
    schema_path: str


class _EngineFailureRequired(TypedDict):
    category: str
    phase: str
    code: str
    message: str


class EngineFailure(_EngineFailureRequired, total=False):
    """Closed structured failure returned by the Rust Engine."""

    contract: str
    contract_side: str
    path: str
    issues: list[EngineIssue]
    retry_disposition: str


class EngineError(RuntimeError):
    """Typed Engine transport or semantic failure."""

    def __init__(self, failure: EngineFailure) -> None:
        self.failure = failure
        super().__init__(f"{failure['code']}: {failure['message']}")


class ExecutionResult(TypedDict):
    """Terminal Embedded execution result."""

    run_id: str
    plan_id: str
    value: Json
    projection_digest: str
    precondition_token: str
    effects: list[str]


class SuspensionBoundary(TypedDict):
    """Typed wait boundary without a resumable Embedded continuation."""

    run_id: str
    plan_id: str
    definition_id: str
    invocation_id: str
    site_id: str
    wait: dict[str, Json]
    result_bind: str | None


class CompletedExecution(TypedDict):
    status: Literal["completed"]
    result: ExecutionResult


class SuspendedExecution(TypedDict):
    status: Literal["suspended"]
    suspension: SuspensionBoundary


ExecutionOutcome = CompletedExecution | SuspendedExecution


class RolloutDecision(TypedDict):
    """One immutable future-selection decision."""

    decision_id: str
    fallback_plan: str
    target_plan: str
    mode: dict[str, Any]


class RolloutObservation(TypedDict):
    """One terminal observation for a pinned rollout occurrence."""

    observation_id: str
    decision_id: str
    occurrence_id: str
    plan_id: str
    outcome: str
    evidence: ArtifactRef


class RolloutGate(TypedDict):
    """Deterministic promotion and rollback thresholds."""

    gate_id: str
    decision_id: str
    min_target_observations: int
    max_target_failures: int
    min_equivalent_shadows: int
    max_inequivalent_shadows: int


class MigrationRequest(TypedDict):
    """Checked safe-point migration request for a pinned adapter."""

    migration_id: str
    run_id: str
    from_plan: str
    to_plan: str
    safe_point_id: str
    source_epoch: int
    input_state: ArtifactRef


class RestartRequest(TypedDict):
    """Authorize a replacement Run under one exact new Plan."""

    restart_id: str
    source_run: str
    replacement_run: str
    from_plan: str
    to_plan: str
    safe_point_id: str
    source_epoch: int
    input: ArtifactRef
    evidence: ArtifactRef


class ShadowRequest(TypedDict):
    """Isolated, non-authoritative shadow execution request."""

    comparison_id: str
    decision_id: str
    subject: str
    primary_plan: str
    shadow_plan: str
    input: ArtifactRef
    comparison_policy: str


class WorkItem(TypedDict):
    """One materialized provider-neutral virtual work item."""

    work_id: str
    region_id: str
    run_id: str
    payload: ArtifactRef
    capability: str | None
    priority: int
    cost: int


class VirtualClaimLease(TypedDict):
    """Fenced worker capacity-slot lease."""

    resource: str
    owner: str
    epoch: int
    expires_at: int


class ClaimedWork(TypedDict):
    """One active work occurrence and its current capacity-slot lease."""

    item: WorkItem
    owner: str
    epoch: int
    occurrence_id: str
    occurrence_binding: str
    lease: VirtualClaimLease


class WorkOccurrence(TypedDict):
    """Binding-pinned M3 work attempt returned by a control transport."""

    occurrence_version: str
    occurrence_id: str
    work_id: str
    region_id: str
    run_id: str
    owner: str
    epoch: int
    lease_epoch: int
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
    expected_lease_epoch: int
    observed_at: int
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


class VirtualClaimCommand(TypedDict):
    """Idempotent worker capacity-slot claim request."""

    control_version: str
    command_id: str
    owner: str
    slot_id: str
    occurrence_binding: str
    capabilities: list[str]
    logical_now: int
    lease_ttl: int


class VirtualClaimReceipt(TypedDict):
    """Claimed work or a durable empty eligibility observation."""

    command: VirtualClaimCommand
    claim: ClaimedWork | None


class VirtualLeaseRenewalCommand(TypedDict):
    """Idempotent active-claim lease renewal request."""

    control_version: str
    command_id: str
    work_id: str
    owner: str
    epoch: int
    expected_lease_epoch: int
    logical_now: int
    lease_ttl: int


class VirtualLeaseRenewalReceipt(TypedDict):
    """New lease fence committed for one active claim."""

    command: VirtualLeaseRenewalCommand
    lease: VirtualClaimLease


class VirtualRecoveryCommand(TypedDict):
    """Explicit retry, fail, or cancel decision for an expired claim."""

    control_version: str
    command_id: str
    work_id: str
    expected_owner: str
    expected_epoch: int
    expected_lease_epoch: int
    observed_at: int
    resolution: WorkResolution


class VirtualRecoveryReceipt(TypedDict):
    """Expired occurrence after its admitted recovery disposition."""

    command: VirtualRecoveryCommand
    occurrence: WorkOccurrence


class VirtualRunWeightCommand(TypedDict):
    """Idempotent future Run scheduling-share update."""

    control_version: str
    command_id: str
    run_id: str
    weight: int


class VirtualRunWeightReceipt(TypedDict):
    """Previous and current Run scheduling weights."""

    command: VirtualRunWeightCommand
    previous_weight: int
    current_weight: int


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


class EvolutionControl(Protocol):
    """Transport-neutral M4 command interface; Rust remains authority."""

    def submit(self, command: EvolutionCommand) -> Any: ...


class LiveEvolutionControl(Protocol):
    """Unified registry, DAG, rollout, migration, and pin transport."""

    def submit(self, command: LiveEvolutionCommand) -> Any: ...


class DurableControl(Protocol):
    """Transport-neutral M1 mutation and query interface."""

    def submit(self, command: DurableCommand) -> Any: ...


class VirtualWorkControl(Protocol):
    """Transport-neutral M3 occurrence query and control interface."""

    def occurrence(self, occurrence_id: str) -> WorkOccurrence | None: ...

    def resolve(self, command: WorkResolutionCommand) -> WorkOccurrence: ...

    def migrate(self, command: RegionMigrationCommand) -> RegionMigrationReceipt: ...

    def compact(self, command: VirtualCompactionCommand) -> VirtualCompactionReceipt: ...

    def rehydrate(self, command: VirtualRehydrationCommand) -> VirtualRehydrationReceipt: ...


class VirtualSchedulingControl(Protocol):
    """Transport-neutral M3 worker-slot scheduling commands."""

    def claim(self, command: VirtualClaimCommand) -> VirtualClaimReceipt: ...

    def renew(self, command: VirtualLeaseRenewalCommand) -> VirtualLeaseRenewalReceipt: ...

    def recover(self, command: VirtualRecoveryCommand) -> VirtualRecoveryReceipt: ...

    def set_run_weight(self, command: VirtualRunWeightCommand) -> VirtualRunWeightReceipt: ...


class FlowBuilder:
    """Small code-first builder for the frozen language-neutral IR."""

    def __init__(self, name: str, input_schema: Json, output_schema: Json) -> None:
        self._candidate: dict[str, Any] = {
            "ir_version": "cymule.ir/2",
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

    def definition(
        self,
        definition_id: str,
        input_schema: Json,
        output_schema: Json,
        body: dict[str, Any],
    ) -> FlowBuilder:
        """Add one reusable definition to the same immutable Plan."""
        self._candidate["definitions"].append(
            {
                "id": definition_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "body": body,
            }
        )
        return self

    def invoke(
        self,
        site: str,
        definition: str,
        expression: dict[str, Json],
        bind: str,
    ) -> FlowBuilder:
        """Append one reusable definition invocation."""
        self._steps().append(
            {
                "id": site,
                "op": "invoke",
                "definition": definition,
                "input": expression,
                "bind": bind,
            }
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

    def wait(self, site: str, wait: dict[str, Json], bind: str | None = None) -> FlowBuilder:
        """Append a durable suspension boundary with its result binding."""
        step: dict[str, Any] = {"id": site, "op": "wait", "wait": wait}
        if bind is not None:
            step["bind"] = bind
        self._steps().append(step)
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


class DurableControlBuilder:
    """Build closed M1 controls without reducing durable state locally."""

    @staticmethod
    def start_run(run_id: str, candidate: dict[str, Any], input_value: Json) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        return {
            "type": "start_run",
            "control_version": "cymule.durable-control/1",
            "run_id": run_id,
            "candidate": copy.deepcopy(candidate),
            "input": copy.deepcopy(input_value),
        }

    @staticmethod
    def resume_run(run_id: str) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        return {
            "type": "resume_run",
            "control_version": "cymule.durable-control/1",
            "run_id": run_id,
        }

    @staticmethod
    def activate_signal(
        activation_id: str, key: str, wait_ids: list[str], value: Json
    ) -> DurableCommand:
        return DurableControlBuilder._activate(
            activation_id, {"kind": "signal", "key": key}, wait_ids, value
        )

    @staticmethod
    def activate_timer(
        activation_id: str, timer_id: str, wait_id: str, value: Json
    ) -> DurableCommand:
        return DurableControlBuilder._activate(
            activation_id,
            {"kind": "timer", "timer_id": timer_id},
            [wait_id],
            value,
        )

    @staticmethod
    def release_effect(intent_id: str) -> DurableCommand:
        DurableControlBuilder._identity("effect intent", intent_id)
        return {
            "type": "release_effect",
            "control_version": "cymule.durable-control/1",
            "intent_id": intent_id,
        }

    @staticmethod
    def query_run(query_id: str, run_id: str) -> DurableCommand:
        DurableControlBuilder._identity("query", query_id)
        DurableControlBuilder._identity("Run", run_id)
        return {
            "type": "query_run",
            "control_version": "cymule.durable-control/1",
            "query_id": query_id,
            "run_id": run_id,
        }

    @staticmethod
    def query_domain(query_id: str) -> DurableCommand:
        DurableControlBuilder._identity("query", query_id)
        return {
            "type": "query_domain",
            "control_version": "cymule.durable-control/1",
            "query_id": query_id,
        }

    @staticmethod
    def _activate(
        activation_id: str,
        source: dict[str, str],
        wait_ids: list[str],
        value: Json,
    ) -> DurableCommand:
        DurableControlBuilder._identity("activation", activation_id)
        targets = sorted(set(wait_ids))
        if not targets or any(not target for target in targets):
            raise ValueError("durable activation requires at least one wait identity")
        return {
            "type": "activate_wait",
            "control_version": "cymule.durable-control/1",
            "activation_id": activation_id,
            "source": dict(source),
            "wait_ids": targets,
            "value": copy.deepcopy(value),
        }

    @staticmethod
    def _identity(kind: str, value: str) -> None:
        if not value or len(value) > 512:
            raise ValueError(
                f"durable {kind} identity must contain 1..=512 characters"
            )


class EvolutionControlBuilder:
    """Build closed idempotent M4 commands without reducing state locally."""

    @staticmethod
    def apply_patch(command_id: str, patch: dict[str, Any]) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id, {"operation": "apply_patch", "patch": patch}
        )

    @staticmethod
    def set_rollout(command_id: str, decision: RolloutDecision) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id, {"operation": "set_rollout", "decision": decision}
        )

    @staticmethod
    def select_occurrence(command_id: str, occurrence_id: str) -> EvolutionCommand:
        if not occurrence_id:
            raise ValueError("evolution selection requires an occurrence identity")
        return EvolutionControlBuilder._build(
            command_id,
            {"operation": "select_occurrence", "occurrence_id": occurrence_id},
        )

    @staticmethod
    def migrate(command_id: str, request: MigrationRequest) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id, {"operation": "migrate", "request": request}
        )

    @staticmethod
    def restart_under_new_plan(
        command_id: str, request: RestartRequest
    ) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id,
            {"operation": "restart_under_new_plan", "request": request},
        )

    @staticmethod
    def shadow(command_id: str, request: ShadowRequest) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id, {"operation": "shadow", "request": request}
        )

    @staticmethod
    def observe(command_id: str, observation: RolloutObservation) -> EvolutionCommand:
        return EvolutionControlBuilder._build(
            command_id, {"operation": "observe", "observation": observation}
        )

    @staticmethod
    def apply_gate(
        command_id: str, gate: RolloutGate, next_decision_id: str
    ) -> EvolutionCommand:
        if not next_decision_id:
            raise ValueError("evolution gate requires a next decision identity")
        return EvolutionControlBuilder._build(
            command_id,
            {
                "operation": "apply_gate",
                "gate": gate,
                "next_decision_id": next_decision_id,
            },
        )

    @staticmethod
    def _build(command_id: str, operation: dict[str, Any]) -> EvolutionCommand:
        if not command_id:
            raise ValueError("evolution control requires a command identity")
        return {
            "control_version": "cymule.evolution-control/2",
            "command_id": command_id,
            **copy.deepcopy(operation),
        }


class LiveEvolutionControlBuilder:
    """Build commands for the complete durable live-evolution authority."""

    @staticmethod
    def publish_definition(
        command_id: str,
        logical_ref: str,
        definition: dict[str, Any],
    ) -> LiveEvolutionCommand:
        return LiveEvolutionControlBuilder._build(
            command_id,
            {
                "operation": "publish_definition",
                "logical_ref": logical_ref,
                "definition": definition,
            },
        )

    @staticmethod
    def register_template(
        command_id: str, template: dict[str, Any]
    ) -> LiveEvolutionCommand:
        return LiveEvolutionControlBuilder._build(
            command_id, {"operation": "register_template", "template": template}
        )

    @staticmethod
    def publish_and_relink(
        command_id: str, publication: dict[str, Any]
    ) -> LiveEvolutionCommand:
        return LiveEvolutionControlBuilder._build(
            command_id,
            {"operation": "publish_and_relink", "publication": publication},
        )

    @staticmethod
    def apply(
        command_id: str,
        template_id: str,
        command: EvolutionCommand,
        safe_point: dict[str, Any] | None = None,
    ) -> LiveEvolutionCommand:
        if not template_id:
            raise ValueError("live evolution requires a template identity")
        operation: dict[str, Any] = {
            "operation": "apply",
            "template_id": template_id,
            "command": command,
        }
        if safe_point is not None:
            operation["safe_point"] = safe_point
        return LiveEvolutionControlBuilder._build(command_id, operation)

    @staticmethod
    def _build(
        command_id: str, operation: dict[str, Any]
    ) -> LiveEvolutionCommand:
        if not command_id:
            raise ValueError("live-evolution control requires a command identity")
        return {
            "control_version": "cymule.live-evolution-control/1",
            "command_id": command_id,
            **copy.deepcopy(operation),
        }


class VirtualWorkControlBuilder:
    """Build owner/epoch-fenced virtual work resolution commands."""

    @staticmethod
    def succeed(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        observed_at: int,
        result: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            observed_at,
            {"resolution": "succeeded", "result": dict(result)},
        )

    @staticmethod
    def retry(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        observed_at: int,
        error: ArtifactRef,
        next_reason: ParkReason | None = None,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            observed_at,
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
        expected_lease_epoch: int,
        observed_at: int,
        error: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            observed_at,
            {"resolution": "failed", "error": dict(error)},
        )

    @staticmethod
    def park(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        observed_at: int,
        reason: ParkReason,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            observed_at,
            {"resolution": "parked", "reason": dict(reason)},
        )

    @staticmethod
    def cancel(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        observed_at: int,
        reason: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            observed_at,
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
        expected_lease_epoch: int,
        observed_at: int,
        resolution: WorkResolution,
    ) -> WorkResolutionCommand:
        if (
            not command_id
            or not work_id
            or not owner
            or epoch < 1
            or expected_lease_epoch < 1
            or observed_at < 0
        ):
            raise ValueError(
                "virtual work control requires identities, work and lease fences, and logical time"
            )
        return {
            "control_version": "cymule.virtual-work-control/1",
            "command_id": command_id,
            "work_id": work_id,
            "owner": owner,
            "epoch": epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "observed_at": observed_at,
            "resolution": resolution,
        }


class VirtualSchedulingControlBuilder:
    """Build fenced worker-slot claim, renewal, and recovery commands."""

    @staticmethod
    def claim(
        command_id: str,
        owner: str,
        slot_id: str,
        occurrence_binding: str,
        capabilities: list[str],
        logical_now: int,
        lease_ttl: int,
    ) -> VirtualClaimCommand:
        """Build one idempotent capacity-slot claim command."""
        normalized = sorted(set(capabilities))
        if (
            not command_id
            or not owner
            or not slot_id
            or not occurrence_binding
            or any(not capability for capability in normalized)
            or logical_now < 0
            or lease_ttl < 1
        ):
            raise ValueError(
                "virtual claim requires identities, binding, logical time, and positive TTL"
            )
        return {
            "control_version": "cymule.virtual-claim-control/1",
            "command_id": command_id,
            "owner": owner,
            "slot_id": slot_id,
            "occurrence_binding": occurrence_binding,
            "capabilities": normalized,
            "logical_now": logical_now,
            "lease_ttl": lease_ttl,
        }

    @staticmethod
    def renew(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        logical_now: int,
        lease_ttl: int,
    ) -> VirtualLeaseRenewalCommand:
        """Build one active-claim lease renewal command."""
        if (
            not command_id
            or not work_id
            or not owner
            or epoch < 1
            or expected_lease_epoch < 1
            or logical_now < 0
            or lease_ttl < 1
        ):
            raise ValueError(
                "virtual renewal requires identities, fences, logical time, and positive TTL"
            )
        return {
            "control_version": "cymule.virtual-lease-renewal-control/1",
            "command_id": command_id,
            "work_id": work_id,
            "owner": owner,
            "epoch": epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "logical_now": logical_now,
            "lease_ttl": lease_ttl,
        }

    @staticmethod
    def recovery(
        command_id: str,
        work_id: str,
        expected_owner: str,
        expected_epoch: int,
        expected_lease_epoch: int,
        observed_at: int,
        resolution: WorkResolution,
    ) -> VirtualRecoveryCommand:
        """Build one explicit expired-claim recovery command."""
        if (
            not command_id
            or not work_id
            or not expected_owner
            or expected_epoch < 1
            or expected_lease_epoch < 1
            or observed_at < 0
            or resolution.get("resolution") not in {"retry", "failed", "cancelled"}
        ):
            raise ValueError(
                "virtual recovery requires identities, fences, time, and retry/fail/cancel"
            )
        return {
            "control_version": "cymule.virtual-recovery-control/1",
            "command_id": command_id,
            "work_id": work_id,
            "expected_owner": expected_owner,
            "expected_epoch": expected_epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "observed_at": observed_at,
            "resolution": copy.deepcopy(resolution),
        }

    @staticmethod
    def run_weight(command_id: str, run_id: str, weight: int) -> VirtualRunWeightCommand:
        """Build one future Run scheduling-share update."""
        if not command_id or not run_id or weight < 1:
            raise ValueError(
                "virtual Run weight requires command, Run, and positive weight"
            )
        return {
            "control_version": "cymule.virtual-run-weight-control/1",
            "command_id": command_id,
            "run_id": run_id,
            "weight": weight,
        }


class CliEngine:
    """CLI-backed Engine transport."""

    def __init__(self, executable: str | Path) -> None:
        self.executable = str(executable)

    def seal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        response = self._request({"type": "seal", "candidate": candidate})
        if response.get("type") != "sealed":
            raise _unexpected_response("sealed", response)
        return response["plan"]

    def seal_resource(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Validate and seal a Resource Candidate with the Rust engine."""
        response = self._request({"type": "seal_resource", "candidate": candidate})
        if response.get("type") != "sealed_resource":
            raise _unexpected_response("sealed_resource", response)
        return response["resource"]

    def verify_wait_activation(self, activation: dict[str, Any]) -> dict[str, Any]:
        """Validate an identified signal or timer delivery with the Rust engine."""
        response = self._request(
            {"type": "verify_wait_activation", "activation": activation}
        )
        if response.get("type") != "verified_wait_activation":
            raise _unexpected_response("verified_wait_activation", response)
        return response["activation"]

    def verify_durable_command(self, command: DurableCommand) -> DurableCommand:
        """Validate one closed M1 control envelope with the Rust engine."""
        response = self._request(
            {"type": "verify_durable_command", "command": command}
        )
        if response.get("type") != "verified_durable_command":
            raise _unexpected_response("verified_durable_command", response)
        return response["command"]

    def verify_evolution_command(self, command: EvolutionCommand) -> EvolutionCommand:
        """Validate one closed M4 control envelope with the Rust engine."""
        response = self._request(
            {"type": "verify_evolution_command", "command": command}
        )
        if response.get("type") != "verified_evolution_command":
            raise _unexpected_response("verified_evolution_command", response)
        return response["command"]

    def verify_live_evolution_command(
        self, command: LiveEvolutionCommand
    ) -> LiveEvolutionCommand:
        """Validate one unified live-evolution envelope with the Rust engine."""
        response = self._request(
            {"type": "verify_live_evolution_command", "command": command}
        )
        if response.get("type") != "verified_live_evolution_command":
            raise _unexpected_response("verified_live_evolution_command", response)
        return response["command"]

    def run(
        self,
        plan: dict[str, Any],
        input_value: Json,
        plugin: str | Path,
        run_id: str,
    ) -> ExecutionOutcome:
        response = self._request(
            {
                "type": "run",
                "plan": plan,
                "input": input_value,
                "plugin": str(plugin),
                "run_id": run_id,
            }
        )
        if response.get("type") != "execution_boundary":
            raise _unexpected_response("execution_boundary", response)
        return response["execution"]

    def _request(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            completed = subprocess.run(
                [self.executable, "rpc"],
                input=json.dumps(
                    {"engine_protocol": ENGINE_PROTOCOL_VERSION, "request": request},
                    separators=(",", ":"),
                ),
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except OSError as error:
            raise _transport_error("engine_start_failed", str(error)) from error
        except subprocess.TimeoutExpired as error:
            if request.get("type") == "run":
                raise EngineError(
                    {
                        "category": "unknown_world_outcome",
                        "phase": "transport",
                        "code": "engine_response_timed_out",
                        "message": "the Engine response deadline elapsed after execution began",
                        "retry_disposition": "reconcile",
                    }
                ) from error
            raise _transport_error("engine_response_timed_out", str(error)) from error
        if completed.returncode != 0:
            raise _transport_error(
                "engine_process_failed",
                f"engine exited without a protocol response (status {completed.returncode})",
            )
        try:
            envelope = json.loads(
                completed.stdout, object_pairs_hook=_unique_json_object
            )
        except (json.JSONDecodeError, ValueError) as error:
            raise _transport_error("invalid_engine_response", str(error)) from error
        _validate_engine_envelope(envelope)
        if envelope.get("engine_protocol") != ENGINE_PROTOCOL_VERSION:
            raise EngineError(
                {
                    "category": "contract_violation",
                    "phase": "transport",
                    "code": "unsupported_engine_protocol",
                    "message": (
                        f"expected {ENGINE_PROTOCOL_VERSION}, received "
                        f"{envelope.get('engine_protocol')!r}"
                    ),
                    "contract": ENGINE_PROTOCOL_VERSION,
                    "contract_side": "schema",
                    "retry_disposition": "never",
                }
            )
        if envelope.get("outcome") == "failure":
            raise EngineError(envelope["error"])
        if envelope.get("outcome") != "success":
            raise _transport_error(
                "invalid_engine_response", "response outcome is not closed"
            )
        return envelope["response"]


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, member in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object member {key!r}")
        value[key] = member
    return value


def _transport_error(code: str, message: str) -> EngineError:
    return EngineError(
        {
            "category": "transport_failure",
            "phase": "transport",
            "code": code,
            "message": message,
        }
    )


def _unexpected_response(expected: str, response: dict[str, Any]) -> EngineError:
    return EngineError(
        {
            "category": "contract_violation",
            "phase": "transport",
            "code": "unexpected_engine_response",
            "message": f"expected {expected}, received {response.get('type')!r}",
            "retry_disposition": "never",
        }
    )


def _validate_engine_envelope(envelope: object) -> None:
    if not isinstance(envelope, dict):
        raise _transport_error("invalid_engine_response", "response envelope is not an object")
    outcome = envelope.get("outcome")
    expected_keys = (
        {"outcome", "engine_protocol", "response"}
        if outcome == "success"
        else {"outcome", "engine_protocol", "error"}
    )
    if outcome not in {"success", "failure"} or set(envelope) != expected_keys:
        raise _transport_error("invalid_engine_response", "response envelope is not closed")
    if outcome == "failure":
        _validate_engine_failure(envelope["error"])


def _validate_engine_failure(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "Engine failure is not an object")
    required = {"category", "phase", "code", "message"}
    optional = {
        "contract",
        "contract_side",
        "path",
        "issues",
        "retry_disposition",
    }
    if not required <= set(value) or not set(value) <= required | optional:
        raise _transport_error("invalid_engine_response", "Engine failure fields are not closed")
    categories = {
        "transport_failure", "validation", "contract_violation", "admission_denied",
        "conflict", "not_found", "expected_plugin_failure", "plugin_defect",
        "substrate_failure", "cancelled", "timed_out", "unknown_world_outcome",
    }
    phases = {
        "transport", "decode_request", "validate_request", "seal_plan", "verify_plan",
        "seal_resource", "verify_wait_activation", "verify_durable_command",
        "verify_evolution_command", "verify_live_evolution_command", "execute_plan",
        "plugin_describe", "plugin_call", "effect_prepare", "effect_dispatch",
        "effect_reconcile", "encode_response",
    }
    retries = {
        "never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile"
    }
    if value["category"] not in categories or value["phase"] not in phases:
        raise _transport_error("invalid_engine_response", "Engine failure enum is unknown")
    code = value["code"]
    message = value["message"]
    if (
        not isinstance(code, str)
        or not 1 <= len(code) <= 200
        or not code[0].islower()
        or not all(character.islower() or character.isdigit() or character == "_" for character in code)
        or not isinstance(message, str)
        or not 1 <= len(message.encode()) <= 8192
    ):
        raise _transport_error("invalid_engine_response", "Engine failure bounds are invalid")
    if "retry_disposition" in value and value["retry_disposition"] not in retries:
        raise _transport_error("invalid_engine_response", "retry disposition is unknown")
    contract = value.get("contract")
    if contract is not None and (
        not isinstance(contract, str) or not 1 <= len(contract.encode()) <= 500
    ):
        raise _transport_error("invalid_engine_response", "contract identity is invalid")
    if value.get("contract_side") not in {None, "schema", "input", "output"}:
        raise _transport_error("invalid_engine_response", "contract side is unknown")
    _validate_engine_path(value.get("path"), "failure path")
    issues = value.get("issues", [])
    if not isinstance(issues, list) or len(issues) > 100:
        raise _transport_error("invalid_engine_response", "Engine issue set is invalid")
    for issue in issues:
        if not isinstance(issue, dict) or not {"code", "message"} <= set(issue) or not set(issue) <= {"code", "message", "path", "schema_path"}:
            raise _transport_error("invalid_engine_response", "Engine issue fields are not closed")
        if (
            not isinstance(issue["code"], str)
            or not 1 <= len(issue["code"].encode()) <= 200
            or not isinstance(issue["message"], str)
            or not 1 <= len(issue["message"].encode()) <= 2000
        ):
            raise _transport_error("invalid_engine_response", "Engine issue bounds are invalid")
        _validate_engine_path(issue.get("path"), "issue path")
        _validate_engine_path(issue.get("schema_path"), "issue schema path")


def _validate_engine_path(value: object, label: str) -> None:
    if value is not None and (
        not isinstance(value, str)
        or len(value.encode()) > 1000
        or bool(value) and not value.startswith("/")
    ):
        raise _transport_error("invalid_engine_response", f"{label} is invalid")


__all__ = [
    "ArtifactRef",
    "CliEngine",
    "ENGINE_PROTOCOL_VERSION",
    "DurableCommand",
    "DurableControl",
    "DurableControlBuilder",
    "EvolutionCommand",
    "EvolutionControl",
    "EvolutionControlBuilder",
    "EngineError",
    "EngineFailure",
    "EngineIssue",
    "LiveEvolutionCommand",
    "LiveEvolutionControl",
    "LiveEvolutionControlBuilder",
    "FlowBuilder",
    "Json",
    "MigrationRequest",
    "ParkReason",
    "RegionMigrationCommand",
    "RegionMigrationPlan",
    "RegionMigrationReceipt",
    "RegionMigrationRequest",
    "RegionMigrator",
    "RolloutDecision",
    "RolloutGate",
    "RolloutObservation",
    "ResourceBuilder",
    "RestartRequest",
    "ShadowRequest",
    "VirtualWorkControl",
    "VirtualWorkControlBuilder",
    "VirtualCursor",
    "VirtualRegion",
    "WorkOccurrence",
    "WorkResolution",
    "WorkResolutionCommand",
    "WaitActivationBuilder",
]
