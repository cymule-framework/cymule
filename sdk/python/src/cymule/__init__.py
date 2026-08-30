"""Python authoring and engine client SDK for Cymule."""

from __future__ import annotations

import base64
import binascii
import copy
from decimal import Decimal, InvalidOperation
import errno
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import selectors
import threading
import time
from pathlib import Path
from typing import Any, Callable, Literal, Protocol, TypedDict, cast

Json = None | bool | int | float | str | list["Json"] | dict[str, "Json"]


class ArtifactRef(TypedDict):
    """One immutable Artifact reference under the closed identity version."""

    identity_version: Literal["cymule.artifact/2"]
    artifact_id: str
    kind: str


class ArtifactRecord(TypedDict):
    """One immutable Artifact reference and its exact self-verifying bytes."""

    reference: ArtifactRef
    bytes: str


class ResourceProducerProvenance(TypedDict):
    """Exact producer occurrence and result provenance for one transfer."""

    run_id: str
    occurrence_id: str
    result: ArtifactRef


class ResourceHandoff(TypedDict):
    """One external Run-to-Run Resource transfer authority."""

    handoff_version: Literal["cymule.resource-handoff/5"]
    transfer_id: str
    producer: ResourceProducerProvenance
    to_run: str
    slot: str
    resource: ArtifactRef


class ResourceHandoffActivation(TypedDict):
    """One external activation of a transfer into an exact target Wait."""

    activation_version: Literal["cymule.resource-handoff-activation/3"]
    activation_id: str
    transfer_id: str
    to_run: str
    wait_id: str
    result: ArtifactRef


ParkReason = dict[str, str]
WorkResolution = dict[str, Any]
EvolutionCommand = dict[str, Any]
LiveEvolutionCommand = dict[str, Any]
LiveEvolutionOutcome = dict[str, Any]
DurableCommand = dict[str, Any]
ENGINE_PROTOCOL_VERSION = "cymule.engine/5"
_ENGINE_REQUEST_LIMIT = 64 * 1024 * 1024
_ENGINE_REQUEST_FRAMING_BYTES = 48
_ENGINE_RESPONSE_PAYLOAD_LIMIT = _ENGINE_REQUEST_LIMIT
_ENGINE_SUCCESS_FRAMING_BYTES = 80
_ENGINE_RESPONSE_LIMIT = (
    _ENGINE_REQUEST_LIMIT
    - _ENGINE_REQUEST_FRAMING_BYTES
    + _ENGINE_RESPONSE_PAYLOAD_LIMIT
    + _ENGINE_SUCCESS_FRAMING_BYTES
)
_ENGINE_DIAGNOSTIC_LIMIT = 1024 * 1024
_MAX_JSON_DEPTH = 128
_MAX_JSON_NUMBER_TOKEN_BYTES = 256
_MAX_JSON_EXPONENT_DIGITS = 6


EvolutionStateFamily = Literal[
    "definition_current",
    "definition_compatibility_current",
    "definition_record",
    "dependency_current",
    "template_current",
    "link_record",
    "plan_record",
    "edge_record",
    "rollout_current",
    "rollout_evidence_current",
    "rollout_decision",
    "occurrence_current",
    "selection_current",
    "migration_record",
    "restart_record",
    "shadow_record",
    "shadow_subject_current",
    "observation_record",
    "observation_occurrence_current",
    "evidence_current",
    "decision_transition_current",
    "transition_record",
]


class EvolutionMutationWrite(TypedDict):
    family: EvolutionStateFamily
    storage_key: str
    value_id: str


class EvolutionPersistenceCommand(TypedDict):
    persistence_version: Literal["cymule.evolution-persistence-command/4"]
    persistence_id: str
    evolution_id: str
    command: LiveEvolutionCommand


class EvolutionPersistenceReceipt(TypedDict):
    receipt_version: Literal["cymule.evolution-persistence-receipt/4"]
    receipt_id: str
    command: EvolutionPersistenceCommand
    parent_current_id: str | None
    source_witness_id: str | None
    outcome: LiveEvolutionOutcome
    mutations: list[EvolutionMutationWrite]
    mutation_id: str


class EvolutionCommit(TypedDict):
    observed_revision: str
    committed_revision: str | None
    receipt: EvolutionPersistenceReceipt


DurablePageQueryKind = Literal[
    "run_index",
    "run_waits",
    "run_effects",
    "run_occurrences",
    "run_attempts",
]


class DurablePagePosition(TypedDict):
    canonical_key: str
    key_hash: str


class DurablePageCursor(TypedDict):
    query_kind: DurablePageQueryKind
    run_id: str | None
    source_revision: str
    source_root: str
    position: DurablePagePosition


class DurablePageQueryOptions(TypedDict):
    expected_revision: str | None
    cursor: DurablePageCursor | None
    limit: int
    max_canonical_bytes: int


class DurableRunItemQuery(TypedDict):
    run_id: str
    expected_revision: str | None
    selector: dict[str, str]
    max_canonical_bytes: int


class DurableQueryPage(TypedDict):
    observed_revision: str
    source_root: str
    items: list[dict[str, Any]]
    next_cursor: DurablePageCursor | None


class DurableRunCurrentResponse(TypedDict):
    type: Literal["run_current"]
    observed_revision: str
    source_root: str
    current: dict[str, Any] | None


class _EngineStoreTargetRequired(TypedDict):
    provider: str
    location: str


class EngineStoreTarget(_EngineStoreTargetRequired, total=False):
    domain: str


class EngineProcessConfig(TypedDict):
    executable: str
    arguments: list[str]
    environment: dict[str, str]
    working_directory: str | None
    runtime_closure: dict[str, str]
    timeout_ms: int
    message_limit: int
    closure_limit: int


class _EnginePluginTargetRequired(TypedDict):
    provider: Literal["cymule.executor-process/1"]
    process: EngineProcessConfig


class EnginePluginTarget(_EnginePluginTargetRequired, total=False):
    revision: str


class EngineClockTarget(TypedDict):
    provider: Literal["cymule.clock-system/2"]
    location: str
    source_id: str
    source_generation: str


class ClockObservationRef(TypedDict):
    """Opaque reference to one receipt retained by the selected Clock."""

    clock_version: Literal["cymule.clock-observation/2"]
    observation_id: str
    source_id: str
    source_generation: str
    scope: str


class ClockObservationResult(TypedDict):
    """Engine correlation authority for one Run-scoped Clock issuance."""

    run_id: str
    observation: ClockObservationRef


class ClockObservation(ClockObservationRef):
    """Complete receipt retained by a persistence-backed Clock authority."""

    logical_time: int
    observed_unix_ms: int


class ExecutionClaimRequest(TypedDict):
    """Exact authority input for one durable Run driver."""

    owner: str
    clock: ClockObservationRef
    ttl: int


class WaitActivation(TypedDict):
    activation_version: Literal["cymule.wait-activation/2"]
    activation_id: str
    source: dict[str, str]
    wait_ids: list[str]
    result: ArtifactRef


class WaitActivationReceipt(TypedDict):
    receipt_version: Literal["cymule.wait-activation-receipt/3"]
    activation: WaitActivation
    applied_wait_ids: list[str]
    ready_run_ids: list[str]


class EffectResolutionCommand(TypedDict):
    resolution_id: str
    run_id: str
    intent_id: str
    execution_binding: ArtifactRef
    occurrence_binding: str
    claim_owner: str
    claim_epoch: int
    resolution: Literal["resolved_applied", "resolved_not_applied"]
    value: Json


class EffectResolutionReceipt(TypedDict):
    receipt_version: Literal["cymule.effect-resolution-receipt/1"]
    command: EffectResolutionCommand
    actual_resolution: Literal["resolved_applied", "resolved_not_applied"]
    actual_value: Json
    result: ArtifactRef | None
    receipt_id: str


class CancellationCommand(TypedDict):
    cancellation_id: str
    run_id: str
    reason: Json


class CancelledRunBoundary(TypedDict):
    status: Literal["cancelled"]
    reason: ArtifactRef


class RunCancellationReceipt(TypedDict):
    receipt_version: Literal["cymule.run-cancellation-receipt/1"]
    command: CancellationCommand
    boundary: CancelledRunBoundary
    receipt_id: str


class _EngineDurableTargetRequired(TypedDict):
    store: EngineStoreTarget


class EngineDurableTarget(_EngineDurableTargetRequired, total=False):
    executor: EnginePluginTarget
    clock: EngineClockTarget


class EngineMigrationProviderTarget(TypedDict):
    adapter_id: str
    adapter_revision: str
    process: EnginePluginTarget


class EngineShadowProviderTarget(TypedDict):
    driver_id: str
    driver_revision: str
    process: EnginePluginTarget


class EngineEvolutionTarget(TypedDict):
    store: EngineStoreTarget
    migration_adapter: EngineMigrationProviderTarget | None
    shadow_driver: EngineShadowProviderTarget | None
    target_execution_bindings: dict[str, EnginePluginTarget]


def directory_store(location: str | Path) -> EngineStoreTarget:
    return {"provider": "cymule.directory-store/5", "location": str(location)}


def sqlite_store(location: str | Path, domain: str) -> EngineStoreTarget:
    return {
        "provider": "cymule.sqlite-store/6",
        "location": str(location),
        "domain": domain,
    }


def process_plugin(
    process: EngineProcessConfig, revision: str | None = None
) -> EnginePluginTarget:
    try:
        _validate_json_value(process)
        target: EnginePluginTarget = {
            "provider": "cymule.executor-process/1",
            "process": copy.deepcopy(process),
        }
        if revision is not None:
            target["revision"] = revision
        _validate_engine_plugin_target(target, require_revision=False)
    except EngineError as error:
        raise ValueError("process plugin target is invalid") from error
    return target


def sqlite_clock(
    location: str | Path, source_id: str, source_generation: str
) -> EngineClockTarget:
    if not _is_core_identity(source_id):
        raise ValueError(
            "Clock source identity must contain 1..=512 printable Unicode scalar values"
        )
    return {
        "provider": "cymule.clock-system/2",
        "location": str(location),
        "source_id": source_id,
        "source_generation": source_generation,
    }


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


class EngineTransportSuccess(TypedDict):
    """Complete accepted request echo and closed Engine response."""

    request: dict[str, Any]
    response: dict[str, Any]


class EngineTransport(Protocol):
    """Single custom transport seam shared by all Engine operations."""

    def exchange(self, request: dict[str, Any]) -> EngineTransportSuccess:
        """Exchange one complete strict request snapshot."""
        ...


class EngineCancellation:
    """One-way cancellation token with launch and completion gates.

    ``cancel()`` and the CLI Engine's process launch share one lock.  If
    cancellation acquires that lock first, the subprocess factory is never
    called.  Once launch acquires it first, later cancellation is necessarily
    classified against an already-started Engine process.  Each invocation
    independently races its admitted result against cancellation, so completing
    one call never completes a shared token for another call.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cancelled = False

    def cancel(self) -> None:
        """Permanently cancel this token."""
        with self._lock:
            self._cancelled = True

    def is_cancelled(self) -> bool:
        """Return whether cancellation has been linearized."""
        with self._lock:
            return self._cancelled

    def _launch(
        self, factory: Callable[[], subprocess.Popen[bytes]]
    ) -> subprocess.Popen[bytes] | None:
        with self._lock:
            if self._cancelled:
                return None
            return factory()

    def _complete_call(self) -> bool:
        """Elect this invocation's completed result against cancellation."""
        with self._lock:
            return not self._cancelled


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


class EffectReleaseBoundary(TypedDict):
    """Exact explicit Effect intents requiring caller release."""

    run_id: str
    plan_id: str
    intent_ids: list[str]


class ReleaseRequiredExecution(TypedDict):
    status: Literal["release_required"]
    release: EffectReleaseBoundary


class EffectReconciliationBoundary(TypedDict):
    """Original ambiguous Effect intent requiring reconciliation."""

    run_id: str
    plan_id: str
    intent_id: str


class ReconciliationRequiredExecution(TypedDict):
    status: Literal["reconciliation_required"]
    reconciliation: EffectReconciliationBoundary


ExecutionOutcome = (
    CompletedExecution
    | SuspendedExecution
    | ReleaseRequiredExecution
    | ReconciliationRequiredExecution
)


class RolloutDecision(TypedDict):
    """One immutable future-selection decision."""

    decision_id: str
    fallback_plan: str
    target_plan: str
    mode: dict[str, Any]


class OccurrencePin(TypedDict):
    """Complete immutable rollout and execution lineage for one occurrence."""

    occurrence_id: str
    template_id: str
    decision_id: str
    plan_id: str
    execution_binding: ArtifactRef
    selection_id: str


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


class MigrationInvocationPathSegment(TypedDict):
    """One exact dynamic invocation edge in a mapped Continuation."""

    site_id: str
    region_path: list[int]
    scope_id: str


class MigrationFrame(TypedDict):
    """One mapped interpreter frame and exact target program counter."""

    definition_id: str
    invocation_id: str
    invocation_path: list[MigrationInvocationPathSegment]
    scope_id: str
    input: ArtifactRef
    region_path: list[int]
    next_step: int
    locals: dict[str, ArtifactRef]


class ContinuationExecutionClaim(TypedDict):
    """Retained single-driver authority for one running Continuation."""

    claim_version: Literal["cymule.continuation-execution-claim/1"]
    run_id: str
    continuation_id: str
    owner: str
    continuation_attempt_id: str
    fence: int
    plan_id: str
    execution_binding_ref: ArtifactRef
    clock_observation_ref: ClockObservationRef
    logical_acquired_at: int
    logical_ttl: int
    logical_expires_at: int


class MigrationContinuation(TypedDict):
    """Complete source or target interpreter state at a migration boundary."""

    continuation_version: Literal["cymule.continuation-state/1"]
    run_id: str
    plan_id: str
    binding_context: str
    frames: list[MigrationFrame]
    state: ArtifactRef | None
    wait_set: list[str]
    scope_stack: list[str]
    epoch: int
    execution_fence: int
    execution_claim: None
    status: Literal["ready"]


class MigrationRequest(TypedDict):
    """Semantic migration intent for one exact source epoch and adapter."""

    migration_id: str
    run_id: str
    from_plan: str
    to_plan: str
    plan_edge_id: str
    compatibility_id: str
    expected_source_epoch: int
    adapter_id: str
    adapter_revision: str


class RestartRequest(TypedDict):
    """Authorize a replacement Run under one exact new Plan."""

    restart_id: str
    replacement_run: str
    run_id: str
    from_plan: str
    expected_source_epoch: int
    to_plan: str
    input: ArtifactRef
    evidence: ArtifactRef


class ShadowRequest(TypedDict):
    """Isolated, non-authoritative shadow execution request."""

    comparison_id: str
    decision_id: str
    subject: str
    primary_plan: str
    shadow_plan: str
    driver_id: str
    driver_revision: str
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
    plan_id: str
    execution_binding: ArtifactRef
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
    plan_id: str
    execution_binding: ArtifactRef
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
    clock: ClockObservationRef
    resolution: WorkResolution


class VirtualCursor(TypedDict):
    """Opaque provider-owned region cursor."""

    version: str
    position: str
    exhausted: bool


class RegionSourceBinding(TypedDict):
    """Exact operation, adapter binding, and implementation revision."""

    operation: str
    binding: str
    revision: str


class RegionSourceCheckpoint(TypedDict):
    """Exact source generation and cursor observed by a migration."""

    source: RegionSourceBinding
    cursor: VirtualCursor


class VirtualRegion(TypedDict):
    """One active or retired virtual source region."""

    region_id: str
    run_id: str
    source: RegionSourceBinding
    source_artifact: ArtifactRef
    cursor: VirtualCursor
    estimated_total: int | None


class RegionMigrationRequest(TypedDict):
    """Caller request for an opaque-cursor split or merge plan."""

    migration_id: str
    kind: str
    source_region_ids: list[str]
    target_count: int
    migration_binding: str
    migration_revision: str


class RegionMigrationPlan(TypedDict):
    """Adapter-produced split/merge plan with coverage evidence."""

    migration_version: str
    migration_id: str
    kind: str
    expected_sources: dict[str, RegionSourceCheckpoint]
    targets: list[VirtualRegion]
    migration_binding: str
    migration_revision: str
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


class VirtualArchiveBinding(TypedDict):
    """One immutable Rust archive provider generation."""

    binding: str
    revision: str


class VirtualCompactionCertificate(TypedDict):
    """Verified witness retaining exact archived-history interpretation data."""

    certificate_version: str
    certificate_id: str
    source_causal_cut: list[str]
    summary: VirtualCompletionSummary
    summary_state_digest: str
    occurrence_root_digest: str
    parent_work_index_root_digest: str
    work_index_updates_digest: str
    work_index_root_digest: str
    command_root_digest: str | None
    command_count: int
    unresolved_obligations: list[str]
    retained_execution_bindings: list[ArtifactRef]
    replay_availability: dict[str, Any]
    rehydration_manifest: dict[str, Any]
    archive: VirtualArchiveBinding


class VirtualCompactionCommand(TypedDict):
    """Idempotent completed-region compaction request."""

    control_version: str
    command_id: str
    region_id: str
    source_causal_cut: list[str]
    work_ids: list[str]
    occurrence_ids: list[str]
    archived_command_ids: list[str]
    archive: VirtualArchiveBinding


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
    execution_binding: ArtifactRef
    capabilities: list[str]
    clock: ClockObservationRef
    lease_ttl: int


class VirtualLeaseRenewalCommand(TypedDict):
    """Idempotent active-claim lease renewal request."""

    control_version: str
    command_id: str
    work_id: str
    owner: str
    epoch: int
    expected_lease_epoch: int
    clock: ClockObservationRef
    lease_ttl: int


class VirtualLeaseRenewalReceipt(TypedDict):
    """New lease fence committed for one active claim."""

    command: VirtualLeaseRenewalCommand
    clock_observation: ClockObservation
    lease: VirtualClaimLease


class VirtualRecoveryCommand(TypedDict):
    """Explicit retry, fail, or cancel decision for an expired claim."""

    control_version: str
    command_id: str
    work_id: str
    expected_owner: str
    expected_epoch: int
    expected_lease_epoch: int
    clock: ClockObservationRef
    resolution: WorkResolution


class VirtualRecoveryReceipt(TypedDict):
    """Expired occurrence after its admitted recovery disposition."""

    command: VirtualRecoveryCommand
    clock_observation: ClockObservation
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


class FlowBuilder:
    """Small code-first builder for the frozen language-neutral IR."""

    def __init__(self, name: str, input_schema: Json, output_schema: Json) -> None:
        self._candidate: dict[str, Any] = {
            "ir_version": "cymule.ir/3",
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

    def component(
        self,
        operation_id: str,
        input_schema: Json,
        output_schema: Json,
        output_artifact_kind: str,
        requirements: dict[str, str],
    ) -> FlowBuilder:
        self._candidate["components"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "output_artifact_kind": output_artifact_kind,
                "requirements": copy.deepcopy(requirements),
            }
        )
        return self

    def effect_contract(
        self,
        operation_id: str,
        input_schema: Json,
        output_schema: Json,
        profile: dict[str, Json],
        requirements: dict[str, str],
    ) -> FlowBuilder:
        self._candidate["effects"].append(
            {
                "id": operation_id,
                "input_schema": input_schema,
                "output_schema": output_schema,
                "profile": profile,
                "requirements": copy.deepcopy(requirements),
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
        bind: str | None = None,
    ) -> FlowBuilder:
        step: dict[str, Any] = {
            "id": site,
            "op": "effect",
            "effect": effect,
            "input": expression,
            "occurrence": occurrence,
        }
        if bind is not None:
            step["bind"] = bind
        self._steps().append(step)
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
        body: dict[str, Json],
        bind: str,
    ) -> FlowBuilder:
        """Append a structured auto-commit scope."""
        self._steps().append({"id": site, "op": "scope", "body": body, "bind": bind})
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
        retained_annotations = dict(annotations or {})
        return {
            "resource_version": "cymule.resource/4",
            "shape": "inline",
            "media_type": "text/plain",
            "inline": {"encoding": "utf8", "text": text},
            "integrity": {"kind": "inline"},
            **({} if not retained_annotations else {"annotations": retained_annotations}),
        }

    @staticmethod
    def json(value: Json, annotations: dict[str, str] | None = None) -> dict[str, Any]:
        retained_annotations = dict(annotations or {})
        return {
            "resource_version": "cymule.resource/4",
            "shape": "inline",
            "media_type": "application/json",
            "inline": {"encoding": "json", "value": value},
            "integrity": {"kind": "inline"},
            **({} if not retained_annotations else {"annotations": retained_annotations}),
        }

    @staticmethod
    def external(
        shape: str,
        media_type: str,
        integrity: dict[str, Json],
        manifest: dict[str, Json] | None = None,
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        if shape == "inline":
            raise ValueError("external resource shape cannot be inline")
        if not _is_resource_media_type(media_type):
            raise ValueError("Resource media type is invalid")
        retained_annotations = dict(annotations or {})
        return {
            "resource_version": "cymule.resource/4",
            "shape": shape,
            "media_type": media_type,
            "integrity": copy.deepcopy(integrity),
            **({} if manifest is None else {"manifest": copy.deepcopy(manifest)}),
            **({} if not retained_annotations else {"annotations": retained_annotations}),
        }

    @staticmethod
    def handoff(
        transfer_id: str,
        producer: ResourceProducerProvenance,
        to_run: str,
        slot: str,
        resource: ArtifactRef,
    ) -> ResourceHandoff:
        """Create one M1 Run-to-Run resource handoff record."""
        if (
            not _is_core_identity(transfer_id)
            or not _is_core_identity(to_run)
            or not _is_core_identity(slot)
        ):
            raise ValueError(
                "resource handoff identities must contain 1..=512 printable Unicode scalar values"
            )
        if (
            not isinstance(producer, dict)
            or set(producer) != {"run_id", "occurrence_id", "result"}
            or not _is_core_identity(producer.get("run_id"))
            or not _is_core_identity(producer.get("occurrence_id"))
            or producer["run_id"] == to_run
        ):
            raise ValueError(
                "resource producer provenance is invalid"
            )
        try:
            _validate_artifact_ref(producer["result"])
            _validate_artifact_ref(resource)
        except EngineError as error:
            raise ValueError("resource handoff Artifact is invalid") from error
        if not _wire_json_equal(producer["result"], resource):
            raise ValueError(
                "resource handoff must transfer the producer's exact result Artifact"
            )
        return {
            "handoff_version": "cymule.resource-handoff/5",
            "transfer_id": transfer_id,
            "producer": copy.deepcopy(producer),
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
        activation = {
            "activation_version": "cymule.wait-activation/2",
            "activation_id": activation_id,
            "source": source,
            "wait_ids": targets,
            "result": dict(result),
        }
        try:
            _validate_wait_activation_response(activation)
        except EngineError as error:
            raise ValueError("wait activation is outside the closed contract") from error
        return activation


class DurableControlBuilder:
    """Build closed M1 controls without reducing durable state locally."""

    @staticmethod
    def start_run(
        run_id: str,
        candidate: dict[str, Any],
        input_value: Json,
        execution: ExecutionClaimRequest,
    ) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        DurableControlBuilder._execution(execution)
        return {
            "type": "start_run",
            "control_version": "cymule.durable-control/4",
            "run_id": run_id,
            "candidate": copy.deepcopy(candidate),
            "input": copy.deepcopy(input_value),
            "execution": copy.deepcopy(execution),
        }

    @staticmethod
    def resume_run(run_id: str, execution: ExecutionClaimRequest) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        DurableControlBuilder._execution(execution)
        return {
            "type": "resume_run",
            "control_version": "cymule.durable-control/4",
            "run_id": run_id,
            "execution": copy.deepcopy(execution),
        }

    @staticmethod
    def takeover_run(
        run_id: str, expected_fence: int, execution: ExecutionClaimRequest
    ) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        DurableControlBuilder._execution(execution)
        if (
            not isinstance(expected_fence, int)
            or isinstance(expected_fence, bool)
            or expected_fence < 1
            or expected_fence > 9_007_199_254_740_991
        ):
            raise ValueError("takeover expected fence must be positive")
        return {
            "type": "takeover_run",
            "control_version": "cymule.durable-control/4",
            "run_id": run_id,
            "expected_fence": expected_fence,
            "execution": copy.deepcopy(execution),
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
    def release_effect(intent_id: str, execution: ExecutionClaimRequest) -> DurableCommand:
        if not _is_sha256_id(intent_id):
            raise ValueError("effect intent must be a lowercase SHA-256 content ID")
        DurableControlBuilder._execution(execution)
        return {
            "type": "release_effect",
            "control_version": "cymule.durable-control/4",
            "intent_id": intent_id,
            "execution": copy.deepcopy(execution),
        }

    @staticmethod
    def resolve_effect(
        resolution_id: str,
        run_id: str,
        intent_id: str,
        execution_binding: ArtifactRef,
        occurrence_binding: str,
        claim_owner: str,
        claim_epoch: int,
        resolution: Literal["resolved_applied", "resolved_not_applied"],
        value: Json,
    ) -> DurableCommand:
        for label, identity in (
            ("resolution", resolution_id),
            ("Run", run_id),
            ("occurrence binding", occurrence_binding),
            ("claim owner", claim_owner),
        ):
            DurableControlBuilder._identity(label, identity)
        if not _is_sha256_id(intent_id):
            raise ValueError("effect intent must be a lowercase SHA-256 content ID")
        if not _is_sha256_id(occurrence_binding):
            raise ValueError(
                "effect occurrence binding must be a lowercase SHA-256 content ID"
            )
        try:
            _validate_artifact_ref(execution_binding)
        except EngineError as error:
            raise ValueError("effect resolution binding is invalid") from error
        if execution_binding["kind"] != "cymule.execution-binding/2":
            raise ValueError("effect resolution requires an ExecutionBinding Artifact")
        if not _is_positive_safe_integer(claim_epoch):
            raise ValueError("effect resolution claim epoch must be positive")
        if resolution not in {"resolved_applied", "resolved_not_applied"}:
            raise ValueError("effect resolution disposition is invalid")
        if resolution == "resolved_not_applied" and value is not None:
            raise ValueError("NotApplied effect resolution cannot carry a value")
        return {
            "type": "resolve_effect",
            "control_version": "cymule.durable-control/4",
            "resolution_id": resolution_id,
            "run_id": run_id,
            "intent_id": intent_id,
            "execution_binding": copy.deepcopy(execution_binding),
            "occurrence_binding": occurrence_binding,
            "claim_owner": claim_owner,
            "claim_epoch": claim_epoch,
            "resolution": resolution,
            "value": copy.deepcopy(value),
        }

    @staticmethod
    def cancel_run(cancellation_id: str, run_id: str, reason: Json) -> DurableCommand:
        DurableControlBuilder._identity("cancellation", cancellation_id)
        DurableControlBuilder._identity("Run", run_id)
        return {
            "type": "cancel_run",
            "control_version": "cymule.durable-control/4",
            "cancellation_id": cancellation_id,
            "run_id": run_id,
            "reason": copy.deepcopy(reason),
        }

    @staticmethod
    def run_index_page(options: DurablePageQueryOptions) -> DurableCommand:
        DurableControlBuilder._page_query("run_index", None, options)
        return {
            "type": "run_index_page",
            "control_version": "cymule.durable-control/4",
            **copy.deepcopy(options),
        }

    @staticmethod
    def run_current(run_id: str, expected_revision: str | None) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        DurableControlBuilder._expected_revision(expected_revision)
        return {
            "type": "run_current",
            "control_version": "cymule.durable-control/4",
            "run_id": run_id,
            "expected_revision": expected_revision,
        }

    @staticmethod
    def run_wait_page(
        run_id: str, options: DurablePageQueryOptions
    ) -> DurableCommand:
        return DurableControlBuilder._run_page(
            "run_wait_page", "run_waits", run_id, options
        )

    @staticmethod
    def run_effect_page(
        run_id: str, options: DurablePageQueryOptions
    ) -> DurableCommand:
        return DurableControlBuilder._run_page(
            "run_effect_page", "run_effects", run_id, options
        )

    @staticmethod
    def run_occurrence_page(
        run_id: str, options: DurablePageQueryOptions
    ) -> DurableCommand:
        return DurableControlBuilder._run_page(
            "run_occurrence_page", "run_occurrences", run_id, options
        )

    @staticmethod
    def run_attempt_page(
        run_id: str, options: DurablePageQueryOptions
    ) -> DurableCommand:
        return DurableControlBuilder._run_page(
            "run_attempt_page", "run_attempts", run_id, options
        )

    @staticmethod
    def run_item(query: DurableRunItemQuery) -> DurableCommand:
        if set(query) != {
            "run_id", "expected_revision", "selector", "max_canonical_bytes"
        }:
            raise ValueError("exact Run-item query is not closed")
        DurableControlBuilder._identity("Run", query["run_id"])
        DurableControlBuilder._expected_revision(query["expected_revision"])
        try:
            _validate_durable_run_item_selector(query["selector"])
        except EngineError as error:
            raise ValueError("exact Run-item selector is invalid") from error
        if not _is_positive_safe_integer_at_most(
            query["max_canonical_bytes"], 13 * 1024 * 1024
        ):
            raise ValueError("exact Run-item byte budget must be within 1..=13631488")
        return {
            "type": "run_item",
            "control_version": "cymule.durable-control/4",
            **copy.deepcopy(query),
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
        if not targets or any(not _is_sha256_id(target) for target in targets):
            raise ValueError("durable activation requires at least one wait identity")
        command = {
            "type": "activate_wait",
            "control_version": "cymule.durable-control/4",
            "activation_id": activation_id,
            "source": dict(source),
            "wait_ids": targets,
            "value": copy.deepcopy(value),
        }
        try:
            _validate_durable_command_response(command)
        except EngineError as error:
            raise ValueError("durable activation is outside the closed contract") from error
        return command

    @staticmethod
    def _identity(kind: str, value: str) -> None:
        if not _is_core_identity(value):
            raise ValueError(
                f"durable {kind} identity must contain 1..=512 printable Unicode scalar values"
            )

    @staticmethod
    def _expected_revision(value: str | None) -> None:
        if value is not None and not _is_sha256_id(value):
            raise ValueError("durable query expected revision must be a content ID or null")

    @staticmethod
    def _page_query(
        query_kind: DurablePageQueryKind,
        run_id: str | None,
        options: DurablePageQueryOptions,
    ) -> None:
        if set(options) != {
            "expected_revision", "cursor", "limit", "max_canonical_bytes"
        }:
            raise ValueError("durable page query options are not closed")
        DurableControlBuilder._expected_revision(options["expected_revision"])
        if not _is_positive_safe_integer_at_most(options["limit"], 256):
            raise ValueError("durable query page limit must be within 1..=256")
        if not _is_positive_safe_integer_at_most(
            options["max_canonical_bytes"], 1024 * 1024
        ):
            raise ValueError("durable query page byte budget must be within 1..=1048576")
        cursor = options["cursor"]
        if cursor is not None:
            try:
                _validate_durable_page_cursor(cursor)
            except EngineError as error:
                raise ValueError("durable page cursor is invalid") from error
            if (
                cursor["query_kind"] != query_kind
                or cursor["run_id"] != run_id
                or options["expected_revision"] != cursor["source_revision"]
            ):
                raise ValueError("durable query cursor belongs to another authority")

    @staticmethod
    def _run_page(
        command_type: Literal[
            "run_wait_page",
            "run_effect_page",
            "run_occurrence_page",
            "run_attempt_page",
        ],
        query_kind: DurablePageQueryKind,
        run_id: str,
        options: DurablePageQueryOptions,
    ) -> DurableCommand:
        DurableControlBuilder._identity("Run", run_id)
        DurableControlBuilder._page_query(query_kind, run_id, options)
        return {
            "type": command_type,
            "control_version": "cymule.durable-control/4",
            "run_id": run_id,
            **copy.deepcopy(options),
        }

    @staticmethod
    def _execution(value: ExecutionClaimRequest) -> None:
        if set(value) != {"owner", "clock", "ttl"}:
            raise ValueError("execution claim request is not closed")
        DurableControlBuilder._identity("execution owner", value["owner"])
        clock = value["clock"]
        if not isinstance(clock, dict) or set(clock) != {
            "clock_version",
            "observation_id",
            "source_id",
            "source_generation",
            "scope",
        }:
            raise ValueError("execution Clock observation reference is not closed")
        DurableControlBuilder._identity("Clock source", clock["source_id"])
        DurableControlBuilder._identity("Clock scope", clock["scope"])
        if (
            clock["clock_version"] != "cymule.clock-observation/2"
            or re.fullmatch(r"sha256:[0-9a-f]{64}", clock["observation_id"])
            is None
            or re.fullmatch(r"sha256:[0-9a-f]{64}", clock["source_generation"])
            is None
            or not isinstance(value["ttl"], int)
            or isinstance(value["ttl"], bool)
            or value["ttl"] < 1
            or value["ttl"] > 9_007_199_254_740_991
        ):
            raise ValueError("execution Clock observation reference or TTL is invalid")


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
    def select_occurrence(
        command_id: str,
        occurrence_id: str,
        selection_id: str,
        execution_binding: ArtifactRef,
    ) -> EvolutionCommand:
        if not occurrence_id or not selection_id:
            raise ValueError(
                "evolution selection requires occurrence and selection identities"
            )
        return EvolutionControlBuilder._build(
            command_id,
            {
                "operation": "select_occurrence",
                "occurrence_id": occurrence_id,
                "selection_id": selection_id,
                "execution_binding": execution_binding,
            },
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
            "control_version": "cymule.evolution-control/5",
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
        references: list[dict[str, Any]],
    ) -> LiveEvolutionCommand:
        return LiveEvolutionControlBuilder._build(
            command_id,
            {
                "operation": "publish_definition",
                "logical_ref": logical_ref,
                "definition": definition,
                "references": references,
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
    ) -> LiveEvolutionCommand:
        if not template_id:
            raise ValueError("live evolution requires a template identity")
        return LiveEvolutionControlBuilder._build(
            command_id,
            {
                "operation": "apply",
                "template_id": template_id,
                "command": command,
            },
        )

    @staticmethod
    def _build(
        command_id: str, operation: dict[str, Any]
    ) -> LiveEvolutionCommand:
        if not command_id:
            raise ValueError("live-evolution control requires a command identity")
        return {
            "control_version": "cymule.live-evolution-control/6",
            "command_id": command_id,
            **copy.deepcopy(operation),
        }


def _is_positive_safe_integer(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1 <= value <= 9_007_199_254_740_991
    )


def _validate_builder_clock(clock: ClockObservationRef) -> None:
    try:
        _validate_clock_observation_ref(clock)
    except EngineError as error:
        raise ValueError("virtual control requires an issued Clock observation reference") from error


def _validate_builder_park_reason(value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError("virtual park reason is not an object")
    expected = {
        "wait": {"kind", "key"},
        "dependency": {"kind", "work_id"},
        "budget": {"kind", "account"},
        "capability": {"kind", "capability"},
        "backpressure": {"kind", "domain"},
    }.get(value.get("kind"))
    if expected is None or set(value) != expected:
        raise ValueError("virtual park reason is not closed")
    identity = next(member for member in expected if member != "kind")
    if not _is_core_identity(value[identity]):
        raise ValueError("virtual park reason identity is invalid")


def _validate_builder_work_resolution(
    value: object, *, recovery: bool = False
) -> None:
    if not isinstance(value, dict):
        raise ValueError("virtual work resolution is not an object")
    kind = value.get("resolution")
    expected = {
        "succeeded": {"resolution", "result"},
        "retry": {"resolution", "error", "next_reason"},
        "parked": {"resolution", "reason"},
        "failed": {"resolution", "error"},
        "cancelled": {"resolution", "reason"},
    }.get(kind)
    if expected is None or set(value) != expected:
        raise ValueError("virtual work resolution is not closed")
    if recovery and kind not in {"retry", "failed", "cancelled"}:
        raise ValueError("virtual recovery accepts only retry, failure, or cancellation")
    if kind == "retry":
        try:
            _validate_artifact_ref(value["error"])
        except EngineError as error:
            raise ValueError("virtual retry evidence is invalid") from error
        if value["next_reason"] is not None:
            _validate_builder_park_reason(value["next_reason"])
    elif kind == "parked":
        _validate_builder_park_reason(value["reason"])
    else:
        artifact_field = "result" if kind == "succeeded" else (
            "error" if kind == "failed" else "reason"
        )
        try:
            _validate_artifact_ref(value[artifact_field])
        except EngineError as error:
            raise ValueError("virtual resolution Artifact is invalid") from error


class VirtualWorkControlBuilder:
    """Build owner/epoch-fenced virtual work resolution commands."""

    @staticmethod
    def succeed(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        result: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            clock,
            {"resolution": "succeeded", "result": dict(result)},
        )

    @staticmethod
    def retry(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        error: ArtifactRef,
        next_reason: ParkReason | None = None,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            clock,
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
        clock: ClockObservationRef,
        error: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            clock,
            {"resolution": "failed", "error": dict(error)},
        )

    @staticmethod
    def park(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        reason: ParkReason,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            clock,
            {"resolution": "parked", "reason": dict(reason)},
        )

    @staticmethod
    def cancel(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        reason: ArtifactRef,
    ) -> WorkResolutionCommand:
        return VirtualWorkControlBuilder._build(
            command_id,
            work_id,
            owner,
            epoch,
            expected_lease_epoch,
            clock,
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
            "control_version": "cymule.virtual-region-migration-control/3",
            "command_id": command_id,
            "plan": copy.deepcopy(plan),
        }

    @staticmethod
    def compaction(
        command_id: str,
        region_id: str,
        source_causal_cut: list[str],
        work_ids: list[str],
        occurrence_ids: list[str],
        archived_command_ids: list[str],
        archive: VirtualArchiveBinding,
    ) -> VirtualCompactionCommand:
        """Copy a complete compaction intent with its Rust-issued command ID."""
        causal_cut = sorted(set(source_causal_cut))
        works = sorted(set(work_ids))
        occurrences = sorted(set(occurrence_ids))
        commands = sorted(set(archived_command_ids))
        if (
            not _is_sha256_id(command_id)
            or not _is_core_identity(region_id)
            or not causal_cut
            or not 1 <= len(works) <= 1024
            or not 1 <= len(occurrences) <= 1024
            or len(commands) > 1024
            or any(not _is_core_identity(value) for value in [*causal_cut, *works, *commands])
            or any(not _is_sha256_id(value) for value in occurrences)
            or not isinstance(archive, dict)
            or set(archive) != {"binding", "revision"}
            or any(not _is_bounded_printable_scalar_identity(value, 256) for value in archive.values())
        ):
            raise ValueError(
                "virtual compaction requires a Rust-issued identity, bounded exact selections, and archive generation"
            )
        return {
            "control_version": "cymule.virtual-compaction-control/1",
            "command_id": command_id,
            "region_id": region_id,
            "source_causal_cut": causal_cut,
            "work_ids": works,
            "occurrence_ids": occurrences,
            "archived_command_ids": commands,
            "archive": dict(archive),
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
        clock: ClockObservationRef,
        resolution: WorkResolution,
    ) -> WorkResolutionCommand:
        if (
            not _is_core_identity(command_id)
            or not _is_core_identity(work_id)
            or not _is_core_identity(owner)
            or not _is_positive_safe_integer(epoch)
            or not _is_positive_safe_integer(expected_lease_epoch)
        ):
            raise ValueError(
                "virtual work control requires identities, work and lease fences, and logical time"
            )
        _validate_builder_clock(clock)
        _validate_builder_work_resolution(resolution)
        return {
            "control_version": "cymule.virtual-work-control/2",
            "command_id": command_id,
            "work_id": work_id,
            "owner": owner,
            "epoch": epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "clock": dict(clock),
            "resolution": copy.deepcopy(resolution),
        }


class VirtualSchedulingControlBuilder:
    """Build fenced worker-slot claim, renewal, and recovery commands."""

    @staticmethod
    def claim(
        command_id: str,
        owner: str,
        slot_id: str,
        execution_binding: ArtifactRef,
        capabilities: list[str],
        clock: ClockObservationRef,
        lease_ttl: int,
    ) -> VirtualClaimCommand:
        """Build one idempotent capacity-slot claim command."""
        if (
            not _is_core_identity(command_id)
            or not _is_core_identity(owner)
            or not _is_core_identity(slot_id)
            or not isinstance(capabilities, list)
            or any(not _is_core_identity(capability) for capability in capabilities)
            or not _is_positive_safe_integer(lease_ttl)
        ):
            raise ValueError(
                "virtual claim requires identities, binding, a Clock observation, and positive TTL"
            )
        _validate_builder_clock(clock)
        if clock["scope"] != slot_id:
            raise ValueError("virtual claim Clock scope must equal its capacity slot")
        try:
            _validate_artifact_ref(execution_binding)
        except EngineError as error:
            raise ValueError("virtual claim execution binding Artifact is invalid") from error
        if execution_binding["kind"] != "cymule.execution-binding/2":
            raise ValueError("virtual claim requires an ExecutionBinding Artifact")
        normalized = sorted(set(capabilities))
        return {
            "control_version": "cymule.virtual-claim-control/4",
            "command_id": command_id,
            "owner": owner,
            "slot_id": slot_id,
            "execution_binding": dict(execution_binding),
            "capabilities": normalized,
            "clock": dict(clock),
            "lease_ttl": lease_ttl,
        }

    @staticmethod
    def renew(
        command_id: str,
        work_id: str,
        owner: str,
        epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        lease_ttl: int,
    ) -> VirtualLeaseRenewalCommand:
        """Build one active-claim lease renewal command."""
        if (
            not _is_core_identity(command_id)
            or not _is_core_identity(work_id)
            or not _is_core_identity(owner)
            or not _is_positive_safe_integer(epoch)
            or not _is_positive_safe_integer(expected_lease_epoch)
            or not _is_positive_safe_integer(lease_ttl)
        ):
            raise ValueError(
                "virtual renewal requires identities, fences, logical time, and positive TTL"
            )
        _validate_builder_clock(clock)
        return {
            "control_version": "cymule.virtual-lease-renewal-control/2",
            "command_id": command_id,
            "work_id": work_id,
            "owner": owner,
            "epoch": epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "clock": dict(clock),
            "lease_ttl": lease_ttl,
        }

    @staticmethod
    def recovery(
        command_id: str,
        work_id: str,
        expected_owner: str,
        expected_epoch: int,
        expected_lease_epoch: int,
        clock: ClockObservationRef,
        resolution: WorkResolution,
    ) -> VirtualRecoveryCommand:
        """Build one explicit expired-claim recovery command."""
        if (
            not _is_core_identity(command_id)
            or not _is_core_identity(work_id)
            or not _is_core_identity(expected_owner)
            or not _is_positive_safe_integer(expected_epoch)
            or not _is_positive_safe_integer(expected_lease_epoch)
        ):
            raise ValueError(
                "virtual recovery requires identities, fences, time, and retry/fail/cancel"
            )
        _validate_builder_clock(clock)
        _validate_builder_work_resolution(resolution, recovery=True)
        return {
            "control_version": "cymule.virtual-recovery-control/2",
            "command_id": command_id,
            "work_id": work_id,
            "expected_owner": expected_owner,
            "expected_epoch": expected_epoch,
            "expected_lease_epoch": expected_lease_epoch,
            "clock": dict(clock),
            "resolution": copy.deepcopy(resolution),
        }

    @staticmethod
    def run_weight(command_id: str, run_id: str, weight: int) -> VirtualRunWeightCommand:
        """Build one future Run scheduling-share update."""
        if (
            not _is_core_identity(command_id)
            or not _is_core_identity(run_id)
            or not _is_positive_safe_integer(weight)
            or weight > 4_294_967_295
        ):
            raise ValueError(
                "virtual Run weight requires command, Run, and positive weight"
            )
        return {
            "control_version": "cymule.virtual-run-weight-control/1",
            "command_id": command_id,
            "run_id": run_id,
            "weight": weight,
        }


def _snapshot_engine_request(
    request: dict[str, Any],
) -> tuple[bytes, dict[str, Any]]:
    envelope = {
        "engine_protocol": ENGINE_PROTOCOL_VERSION,
        "request": request,
    }
    _validate_json_value(envelope)
    try:
        encoded = json.dumps(
            envelope, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError, UnicodeEncodeError, RecursionError) as error:
        raise _validation_error(
            "invalid_engine_request", "Engine request could not be encoded"
        ) from error
    if len(encoded) > _ENGINE_REQUEST_LIMIT:
        raise _validation_error(
            "engine_request_too_large",
            f"complete Engine request exceeds {_ENGINE_REQUEST_LIMIT} UTF-8 bytes",
        )
    wire_envelope = _strict_json_loads(encoded.decode("utf-8"))
    if (
        not isinstance(wire_envelope, dict)
        or set(wire_envelope) != {"engine_protocol", "request"}
        or wire_envelope.get("engine_protocol") != ENGINE_PROTOCOL_VERSION
        or not isinstance(wire_envelope.get("request"), dict)
    ):
        raise _validation_error(
            "invalid_engine_request", "Engine request envelope must be closed"
        )
    return encoded, wire_envelope["request"]


def _exchange_engine(
    transport: EngineTransport,
    request: dict[str, Any],
    *,
    expected_start_plan_id: str | None = None,
    expected_patch_plan_id: str | None = None,
) -> dict[str, Any]:
    _, wire_request = _snapshot_engine_request(request)
    custom_request = copy.deepcopy(request)
    transport_request = copy.deepcopy(custom_request)
    correlation_request = custom_request
    try:
        success = transport.exchange(transport_request)
    except Exception as error:
        raise _transport_invocation_error(wire_request, error) from error
    try:
        if (
            not isinstance(success, dict)
            or set(success) != {"request", "response"}
            or not isinstance(success.get("request"), dict)
            or not isinstance(success.get("response"), dict)
            or not _wire_json_equal(success["request"], correlation_request)
        ):
            raise _transport_error(
                "invalid_engine_response",
                "custom Engine success did not echo the complete accepted request",
            )
        response = success["response"]
        _validate_engine_response_payload_bound(correlation_request, response)
        _validate_success_response(response)
        expected_type = {
            "seal": "sealed",
            "seal_resource": "sealed_resource",
            "verify_wait_activation": "verified_wait_activation",
            "verify_durable_command": "verified_durable_command",
            "observe_clock": "clock_observed",
            "verify_evolution_command": "verified_evolution_command",
            "verify_live_evolution_command": "verified_live_evolution_command",
            "run": "execution_boundary",
            "execute_durable": "durable_executed",
            "execute_live_evolution": "live_evolution_executed",
        }.get(correlation_request.get("type"))
        if response.get("type") != expected_type:
            raise _transport_error(
                "invalid_engine_response", "Engine success type does not match its request"
            )
        _validate_success_response_for_request(
            correlation_request,
            response,
            expected_start_plan_id=expected_start_plan_id,
        )
        if correlation_request.get("type") == "execute_live_evolution":
            _validate_evolution_commit_for_request(
                correlation_request["evolution_id"],
                correlation_request["command"],
                response["commit"],
                expected_patch_plan_id=expected_patch_plan_id,
            )
        validated_response = cast(
            dict[str, Any], _unbox_exact_fractions(response)
        )
    except Exception as error:
        raise _response_loss_error(wire_request, "invalid_engine_response") from error
    return validated_response


class CliEngine:
    """CLI-backed Engine transport."""

    def __init__(
        self,
        executable: str | Path = "cymule",
        *,
        timeout_seconds: float = 30,
        cancellation: EngineCancellation | None = None,
    ) -> None:
        if cancellation is not None and not isinstance(
            cancellation, EngineCancellation
        ):
            raise TypeError("cancellation must be an EngineCancellation")
        self.executable = str(executable)
        self.timeout_seconds = timeout_seconds
        self.cancellation = cancellation

    def seal(self, candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            candidate_snapshot = _strict_json_loads(
                json.dumps(candidate, separators=(",", ":"), allow_nan=False)
            )
        except (TypeError, ValueError, OverflowError) as error:
            raise _validation_error(
                "invalid_engine_request",
                "Plan Candidate is outside the shared JSON domain",
            ) from error
        if not isinstance(candidate_snapshot, dict):
            raise _validation_error(
                "invalid_engine_request", "Plan Candidate must be an object"
            )
        response = _exchange_engine(self, {"type": "seal", "candidate": candidate_snapshot})
        if response.get("type") != "sealed":
            raise _unexpected_response("sealed", response)
        plan = response["plan"]
        if not _wire_json_equal(plan["candidate"], candidate_snapshot):
            raise _transport_error(
                "invalid_engine_response",
                "sealed Plan does not match the requested candidate",
            )
        return plan

    def seal_resource(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Validate and seal a Resource Candidate with the Rust engine."""
        response = _exchange_engine(self, {"type": "seal_resource", "candidate": candidate})
        if response.get("type") != "sealed_resource":
            raise _unexpected_response("sealed_resource", response)
        return response["resource"]

    def verify_wait_activation(self, activation: dict[str, Any]) -> dict[str, Any]:
        """Validate an identified signal or timer delivery with the Rust engine."""
        response = _exchange_engine(self,
            {"type": "verify_wait_activation", "activation": activation}
        )
        if response.get("type") != "verified_wait_activation":
            raise _unexpected_response("verified_wait_activation", response)
        return response["activation"]

    def verify_durable_command(self, command: DurableCommand) -> DurableCommand:
        """Validate one closed M1 control envelope with the Rust engine."""
        response = _exchange_engine(self,
            {"type": "verify_durable_command", "command": command}
        )
        if response.get("type") != "verified_durable_command":
            raise _unexpected_response("verified_durable_command", response)
        return response["command"]

    def observe_clock(
        self, target: EngineClockTarget, run_id: str
    ) -> ClockObservationResult:
        """Issue one receipt-backed logical Clock reference for a Run."""
        try:
            DurableControlBuilder._identity("Run", run_id)
        except ValueError as error:
            raise _validation_error(
                "invalid_engine_request", "Clock observation request is invalid"
            ) from error
        try:
            _validate_json_value(target)
            _validate_engine_clock_target(target)
            target_snapshot = copy.deepcopy(target)
        except EngineError as error:
            raise _validation_error(
                "invalid_clock_target", "Clock target failed local validation"
            ) from error
        response = _exchange_engine(self,
            {"type": "observe_clock", "target": target_snapshot, "run_id": run_id}
        )
        if response.get("type") != "clock_observed":
            raise _unexpected_response("clock_observed", response)
        return response["result"]

    def verify_evolution_command(self, command: EvolutionCommand) -> EvolutionCommand:
        """Validate one closed M4 control envelope with the Rust engine."""
        response = _exchange_engine(self,
            {"type": "verify_evolution_command", "command": command}
        )
        if response.get("type") != "verified_evolution_command":
            raise _unexpected_response("verified_evolution_command", response)
        return response["command"]

    def verify_live_evolution_command(
        self, command: LiveEvolutionCommand
    ) -> LiveEvolutionCommand:
        """Validate one unified live-evolution envelope with the Rust engine."""
        response = _exchange_engine(self,
            {"type": "verify_live_evolution_command", "command": command}
        )
        if response.get("type") != "verified_live_evolution_command":
            raise _unexpected_response("verified_live_evolution_command", response)
        return response["command"]

    def execute_durable(
        self,
        target: EngineDurableTarget,
        command: DurableCommand,
    ) -> dict[str, Any]:
        """Execute one closed stateful command against a durable Rust domain."""
        try:
            _validate_json_value(target)
            _validate_json_value(command)
            _validate_durable_command_response(command)
            _validate_engine_durable_target(target, command)
            target_snapshot = copy.deepcopy(target)
            command_snapshot = copy.deepcopy(command)
        except EngineError as error:
            raise _validation_error(
                "invalid_engine_request", "durable request failed local validation"
            ) from error
        expected_start_plan_id = None
        if command_snapshot["type"] == "start_run":
            expected_start_plan_id = self.seal(command_snapshot["candidate"])["plan_id"]
        response = _exchange_engine(self,
            {
                "type": "execute_durable",
                "target": target_snapshot,
                "command": command_snapshot,
            },
            expected_start_plan_id=expected_start_plan_id,
        )
        if response.get("type") != "durable_executed":
            raise _unexpected_response("durable_executed", response)
        return response["response"]

    def execute_live_evolution(
        self,
        target: EngineEvolutionTarget,
        evolution_id: str,
        command: LiveEvolutionCommand,
    ) -> EvolutionCommit:
        """Execute one atomic live-evolution command against durable authority."""
        try:
            _validate_json_value(target)
            _validate_json_value(command)
            _validate_live_identity(evolution_id, "evolution")
            _validate_live_evolution_command(command)
            _validate_engine_evolution_target(target, command)
            target_snapshot = copy.deepcopy(target)
            command_snapshot = copy.deepcopy(command)
        except EngineError as error:
            raise _validation_error(
                "invalid_engine_request",
                "live-evolution command failed local validation",
            ) from error
        expected_patch_plan_id: str | None = None
        if (
            command_snapshot["operation"] == "apply"
            and command_snapshot["command"]["operation"] == "apply_patch"
        ):
            expected_patch_plan_id = self.seal(
                command_snapshot["command"]["patch"]["target"]
            )["plan_id"]
        request = {
            "type": "execute_live_evolution",
            "target": target_snapshot,
            "evolution_id": evolution_id,
            "command": command_snapshot,
        }
        response = _exchange_engine(self, request, expected_patch_plan_id=expected_patch_plan_id)
        if response.get("type") != "live_evolution_executed":
            raise _unexpected_response("live_evolution_executed", response)
        commit = response["commit"]
        if (
            expected_patch_plan_id is not None
            and commit["receipt"]["outcome"]["edge"]["to_plan"]
            != expected_patch_plan_id
        ):
            raise _response_loss_error(request, "invalid_engine_response")
        return commit

    def run(
        self,
        plan: dict[str, Any],
        input_value: Json,
        plugin: EnginePluginTarget,
        run_id: str,
    ) -> ExecutionOutcome:
        try:
            DurableControlBuilder._identity("Run", run_id)
        except ValueError as error:
            raise _validation_error(
                "invalid_engine_request", "execution request Run identity is invalid"
            ) from error
        try:
            _validate_json_value(plan)
            _validate_json_value(input_value)
            _validate_sealed_plan(plan)
            plan_snapshot = copy.deepcopy(plan)
            input_snapshot = copy.deepcopy(input_value)
        except EngineError as error:
            raise _validation_error(
                "invalid_engine_request", "execution request failed local validation"
            ) from error
        try:
            _validate_json_value(plugin)
            _validate_engine_plugin_target(
                plugin,
                require_revision=False,
                expected_message_limit=8 * 1024 * 1024,
            )
            plugin_snapshot = copy.deepcopy(plugin)
        except EngineError as error:
            raise _validation_error(
                "invalid_plugin_target", "execution target failed local validation"
            ) from error
        response = _exchange_engine(self,
            {
                "type": "run",
                "plan": plan_snapshot,
                "input": input_snapshot,
                "plugin": plugin_snapshot,
                "run_id": run_id,
            }
        )
        if response.get("type") != "execution_boundary":
            raise _unexpected_response("execution_boundary", response)
        return response["execution"]

    def _exchange_wire(self, request: dict[str, Any]) -> EngineTransportSuccess:
        if (
            not isinstance(self.timeout_seconds, (int, float))
            or isinstance(self.timeout_seconds, bool)
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise _validation_error(
                "invalid_engine_timeout",
                "Engine timeout must be a finite positive number of seconds",
            )
        encoded, wire_request = _snapshot_engine_request(request)
        if os.name != "posix":
            raise _validation_error(
                "engine_rpc_platform_unsupported",
                "CLI Engine process-tree containment requires Unix",
            )

        def launch_process() -> subprocess.Popen[bytes]:
            return subprocess.Popen(
                [self.executable, "rpc"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        try:
            process = (
                launch_process()
                if self.cancellation is None
                else self.cancellation._launch(launch_process)
            )
        except OSError as error:
            raise _transport_error(
                "engine_start_failed", "the Engine process could not be started"
            ) from error
        if process is None:
            raise _interrupted_error(
                wire_request, "cancelled", request_began=False
            )

        def signal_process(selected_signal: int) -> None:
            try:
                process.send_signal(selected_signal)
            except ProcessLookupError:
                return

        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        for stream in (process.stdin, process.stdout, process.stderr):
            os.set_blocking(stream.fileno(), False)
        selector = selectors.DefaultSelector()
        selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
        selector.register(process.stdout, selectors.EVENT_READ, "stdout")
        selector.register(process.stderr, selectors.EVENT_READ, "stderr")
        open_streams = {"stdin", "stdout", "stderr"}
        stdout = bytearray()
        stderr = bytearray()
        input_offset = 0
        input_failed = False
        overflow_code: str | None = None
        deadline = time.monotonic() + self.timeout_seconds
        interruption: Literal["cancelled", "timed_out"] | None = None
        transport_completed = False
        child_exited = False

        def close_stream(name: str) -> None:
            if name not in open_streams:
                return
            stream = {
                "stdin": process.stdin,
                "stdout": process.stdout,
                "stderr": process.stderr,
            }[name]
            try:
                selector.unregister(stream)
            except (KeyError, ValueError):
                pass
            stream.close()
            open_streams.remove(name)

        try:
            while True:
                if (
                    self.cancellation is not None
                    and self.cancellation.is_cancelled()
                ):
                    interruption = "cancelled"
                    break
                if time.monotonic() >= deadline:
                    interruption = "timed_out"
                    break
                if not child_exited:
                    child_exited = _process_exited_without_reaping(process.pid)
                if child_exited and not open_streams:
                    transport_completed = True
                    break
                remaining = max(0.0, deadline - time.monotonic())
                for key, _ in selector.select(min(0.01, remaining)):
                    name = cast(str, key.data)
                    stream = key.fileobj
                    if name == "stdin":
                        try:
                            written = os.write(stream.fileno(), encoded[input_offset:])
                            if written == 0:
                                input_failed = True
                                close_stream("stdin")
                            else:
                                input_offset += written
                                if input_offset == len(encoded):
                                    close_stream("stdin")
                        except BlockingIOError:
                            pass
                        except OSError as error:
                            if error.errno not in {errno.EAGAIN, errno.EINTR}:
                                input_failed = True
                                close_stream("stdin")
                    else:
                        try:
                            chunk = os.read(stream.fileno(), 64 * 1024)
                        except BlockingIOError:
                            continue
                        except OSError as error:
                            if error.errno in {errno.EAGAIN, errno.EINTR}:
                                continue
                            overflow_code = "engine_io_failed"
                            break
                        if not chunk:
                            close_stream(name)
                            continue
                        retained = stdout if name == "stdout" else stderr
                        limit = (
                            _ENGINE_RESPONSE_LIMIT
                            if name == "stdout"
                            else _ENGINE_DIAGNOSTIC_LIMIT
                        )
                        if len(retained) <= limit:
                            retained.extend(chunk[: limit + 1 - len(retained)])
                        if len(retained) > limit:
                            overflow_code = (
                                "engine_output_limit_exceeded"
                                if name == "stdout"
                                else "engine_diagnostic_limit_exceeded"
                            )
                            break
                if overflow_code is not None:
                    break
        finally:
            if (
                not transport_completed
                or interruption is not None
                or overflow_code is not None
            ):
                signal_process(signal.SIGKILL)
            for name in tuple(open_streams):
                close_stream(name)
            selector.close()
            try:
                process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if overflow_code is not None:
            raise _response_loss_error(wire_request, overflow_code)
        if interruption is not None:
            raise _interrupted_error(
                wire_request, interruption, request_began=True
            )
        try:
            envelope = _strict_json_loads(stdout.decode())
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            if process.returncode not in {0, None}:
                raise _response_loss_error(
                    wire_request, "engine_process_failed"
                ) from error
            raise _response_loss_error(wire_request, "invalid_engine_response") from error
        if not isinstance(envelope, dict) or envelope.get("engine_protocol") != ENGINE_PROTOCOL_VERSION:
            if isinstance(envelope, dict) and isinstance(envelope.get("engine_protocol"), str):
                raise _unsupported_engine_protocol_error(
                    wire_request, envelope["engine_protocol"]
                )
            raise _response_loss_error(wire_request, "invalid_engine_response")
        try:
            _validate_engine_envelope_shape(envelope)
        except EngineError as error:
            if process.returncode not in {0, None}:
                raise _response_loss_error(
                    wire_request, "engine_process_failed"
                ) from error
            raise _response_loss_error(
                wire_request, "invalid_engine_response"
            ) from error
        if envelope.get("outcome") == "failure":
            try:
                _validate_engine_failure(envelope["error"])
            except EngineError as error:
                raise _response_loss_error(
                    wire_request, "invalid_engine_response"
                ) from error
            failure = cast(
                EngineFailure, _unbox_exact_fractions(envelope["error"])
            )
            if (
                self.cancellation is not None
                and not self.cancellation._complete_call()
            ):
                raise _interrupted_error(
                    wire_request, "cancelled", request_began=True
                )
            raise EngineError(failure)
        if input_failed:
            raise _response_loss_error(wire_request, "engine_request_incomplete")
        if process.returncode not in {0, None}:
            raise _response_loss_error(wire_request, "engine_process_failed")
        if not _wire_json_equal(envelope["request"], wire_request):
            raise _response_loss_error(wire_request, "invalid_engine_response")
        response = envelope["response"]
        try:
            _validate_engine_response_payload_bound(wire_request, response)
            _validate_success_response(response)
        except EngineError as error:
            raise _response_loss_error(
                wire_request, "invalid_engine_response"
            ) from error
        if (
            self.cancellation is not None
            and not self.cancellation._complete_call()
        ):
            raise _interrupted_error(
                wire_request, "cancelled", request_began=True
            )
        return {
            "request": envelope["request"],
            "response": response,
        }

    def exchange(self, request: dict[str, Any]) -> EngineTransportSuccess:
        return cast(
            EngineTransportSuccess,
            _unbox_exact_fractions(self._exchange_wire(request)),
        )


def _process_exited_without_reaping(process_id: int) -> bool:
    while True:
        try:
            result = os.waitid(
                os.P_PID,
                process_id,
                os.WEXITED | os.WNOHANG | os.WNOWAIT,
            )
            return result is not None and result.si_code in {
                os.CLD_EXITED,
                os.CLD_KILLED,
                os.CLD_DUMPED,
            }
        except InterruptedError:
            continue


def _validate_engine_response_payload_bound(
    request: dict[str, Any], response: object
) -> None:
    if _json_size(_unbox_exact_fractions(response)) > _ENGINE_RESPONSE_PAYLOAD_LIMIT:
        raise _response_loss_error(request, "engine_response_payload_too_large")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, member in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object member {key!r}")
        value[key] = member
    return value


class DurableEngine:
    """High-level durable Run client that keeps all reduction in Rust."""

    def __init__(
        self,
        store: EngineStoreTarget | str | Path,
        plugin: EnginePluginTarget | None,
        clock: EngineClockTarget | None,
        transport: EngineTransport | None = None,
        evolution_id: str = "cymule.sdk.live-evolution",
        migration_adapter: EngineMigrationProviderTarget | None = None,
        shadow_driver: EngineShadowProviderTarget | None = None,
        target_execution_bindings: dict[str, EnginePluginTarget] | None = None,
    ) -> None:
        self.store = directory_store(store) if isinstance(store, (str, Path)) else store
        self.plugin = copy.deepcopy(plugin) if plugin is not None else None
        self.clock = clock
        self.transport = transport or CliEngine()
        self.evolution_id = evolution_id
        self.migration_adapter = migration_adapter
        self.shadow_driver = shadow_driver
        self.target_execution_bindings = copy.deepcopy(
            target_execution_bindings or {}
        )

    def start(
        self,
        run_id: str,
        candidate: dict[str, Any],
        input_value: Json,
        execution: ExecutionClaimRequest,
    ) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.start_run(
                run_id, candidate, input_value, execution
            )
        ))

    def observe_clock(self, run_id: str) -> ClockObservationRef:
        if self.clock is None:
            raise _validation_error(
                "missing_clock_provider", "durable Clock target is missing"
            )
        try:
            DurableControlBuilder._identity("Run", run_id)
            _validate_json_value(self.clock)
            _validate_engine_clock_target(self.clock)
            request = {
                "type": "observe_clock",
                "target": copy.deepcopy(self.clock),
                "run_id": run_id,
            }
            _snapshot_engine_request(request)
        except (EngineError, ValueError, TypeError) as error:
            if (
                isinstance(error, EngineError)
                and error.failure["category"] == "validation"
            ):
                raise
            raise _validation_error(
                "clock_request_validation_failed",
                "Clock observation request failed local validation",
            ) from error
        response = _exchange_engine(self.transport, request)
        if response.get("type") != "clock_observed":
            raise _response_loss_error(request, "invalid_engine_response")
        return response["result"]["observation"]

    def run_index_page(self, options: DurablePageQueryOptions) -> DurableQueryPage:
        response = self._submit(self._build_command(
            lambda: DurableControlBuilder.run_index_page(options)
        ))
        if response.get("type") != "run_index_page":
            raise _unexpected_response("run_index_page", response)
        return response["page"]

    def run_current(
        self, run_id: str, expected_revision: str | None
    ) -> DurableRunCurrentResponse:
        response = self._submit(self._build_command(
            lambda: DurableControlBuilder.run_current(run_id, expected_revision)
        ))
        if response.get("type") != "run_current":
            raise _unexpected_response("run_current", response)
        return cast(DurableRunCurrentResponse, response)

    def run_wait_page(
        self, run_id: str, options: DurablePageQueryOptions
    ) -> DurableQueryPage:
        return self._run_page(
            "run_wait_page",
            self._build_command(
                lambda: DurableControlBuilder.run_wait_page(run_id, options)
            ),
        )

    def run_effect_page(
        self, run_id: str, options: DurablePageQueryOptions
    ) -> DurableQueryPage:
        return self._run_page(
            "run_effect_page",
            self._build_command(
                lambda: DurableControlBuilder.run_effect_page(run_id, options)
            ),
        )

    def run_occurrence_page(
        self, run_id: str, options: DurablePageQueryOptions
    ) -> DurableQueryPage:
        return self._run_page(
            "run_occurrence_page",
            self._build_command(
                lambda: DurableControlBuilder.run_occurrence_page(run_id, options)
            ),
        )

    def run_attempt_page(
        self, run_id: str, options: DurablePageQueryOptions
    ) -> DurableQueryPage:
        return self._run_page(
            "run_attempt_page",
            self._build_command(
                lambda: DurableControlBuilder.run_attempt_page(run_id, options)
            ),
        )

    def run_item(self, query: DurableRunItemQuery) -> dict[str, Any]:
        response = self._submit(self._build_command(
            lambda: DurableControlBuilder.run_item(query)
        ))
        if response.get("type") != "run_item":
            raise _unexpected_response("run_item", response)
        return response

    def resume(self, run_id: str, execution: ExecutionClaimRequest) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.resume_run(run_id, execution)
        ))

    def takeover(
        self, run_id: str, expected_fence: int, execution: ExecutionClaimRequest
    ) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.takeover_run(
                run_id, expected_fence, execution
            )
        ))

    def signal(
        self,
        activation_id: str,
        key: str,
        wait_ids: list[str],
        value: Json,
    ) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.activate_signal(
                activation_id, key, wait_ids, value
            )
        ))

    def release(self, intent_id: str, execution: ExecutionClaimRequest) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.release_effect(intent_id, execution)
        ))

    def resolve_effect(
        self,
        resolution_id: str,
        run_id: str,
        intent_id: str,
        execution_binding: ArtifactRef,
        occurrence_binding: str,
        claim_owner: str,
        claim_epoch: int,
        resolution: Literal["resolved_applied", "resolved_not_applied"],
        value: Json,
    ) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.resolve_effect(
                resolution_id,
                run_id,
                intent_id,
                execution_binding,
                occurrence_binding,
                claim_owner,
                claim_epoch,
                resolution,
                value,
            )
        ))

    def cancel(self, cancellation_id: str, run_id: str, reason: Json) -> dict[str, Any]:
        return self._submit(self._build_command(
            lambda: DurableControlBuilder.cancel_run(
                cancellation_id, run_id, reason
            )
        ))

    def evolve(self, command: LiveEvolutionCommand) -> EvolutionCommit:
        try:
            _validate_json_value(command)
            _validate_live_identity(self.evolution_id, "evolution")
            _validate_live_evolution_command(command)
            command_snapshot = copy.deepcopy(command)
            operation = (
                command_snapshot["command"]["operation"]
                if command_snapshot["operation"] == "apply"
                else None
            )
            target_plan = (
                command_snapshot["command"]["request"]["to_plan"]
                if operation == "migrate"
                else None
            )
            target_execution = self.target_execution_bindings.get(target_plan)
            selected_target_executions = (
                {target_plan: copy.deepcopy(target_execution)}
                if target_plan is not None and target_execution is not None
                else {}
            )
            target: EngineEvolutionTarget = {
                "store": self.store,
                "migration_adapter": (
                    self.migration_adapter if operation == "migrate" else None
                ),
                "shadow_driver": (
                    self.shadow_driver if operation == "shadow" else None
                ),
                "target_execution_bindings": selected_target_executions,
            }
            _validate_json_value(target)
            _validate_engine_evolution_target(target, command_snapshot)
            request = {
                "type": "execute_live_evolution",
                "target": copy.deepcopy(target),
                "evolution_id": self.evolution_id,
                "command": copy.deepcopy(command_snapshot),
            }
            _snapshot_engine_request(request)
        except (EngineError, KeyError, TypeError, ValueError) as error:
            if (
                isinstance(error, EngineError)
                and error.failure["category"] == "validation"
            ):
                raise
            raise _validation_error(
                "evolution_request_validation_failed",
                "live-evolution request failed local validation",
            ) from error
        expected_patch_plan_id: str | None = None
        if (
            request["command"]["operation"] == "apply"
            and request["command"]["command"]["operation"] == "apply_patch"
        ):
            seal_request = {
                "type": "seal",
                "candidate": copy.deepcopy(
                    request["command"]["command"]["patch"]["target"]
                ),
            }
            _snapshot_engine_request(seal_request)
            sealed_response = _exchange_engine(self.transport, seal_request)
            if sealed_response.get("type") != "sealed":
                raise _response_loss_error(seal_request, "invalid_engine_response")
            expected_patch_plan_id = sealed_response["plan"]["plan_id"]
        response = _exchange_engine(
            self.transport,
            request,
            expected_patch_plan_id=expected_patch_plan_id,
        )
        if response.get("type") != "live_evolution_executed":
            raise _response_loss_error(request, "invalid_engine_response")
        return cast(EvolutionCommit, response["commit"])

    def _submit(self, command: DurableCommand) -> dict[str, Any]:
        store_only = command["type"] in {
            "activate_wait",
            "cancel_run",
            "run_index_page",
            "run_current",
            "run_wait_page",
            "run_effect_page",
            "run_occurrence_page",
            "run_attempt_page",
            "run_item",
        }
        provider_only = command["type"] == "resolve_effect"
        try:
            _validate_json_value(command)
            _validate_durable_command_response(command)
            target: EngineDurableTarget = {"store": self.store}
            if not store_only and self.plugin is not None:
                target["executor"] = self.plugin
            if not store_only and not provider_only and self.clock is not None:
                target["clock"] = self.clock
            _validate_json_value(target)
            _validate_engine_durable_target(target, command)
            request = {
                "type": "execute_durable",
                "target": copy.deepcopy(target),
                "command": copy.deepcopy(command),
            }
            _snapshot_engine_request(request)
        except (EngineError, KeyError, TypeError, ValueError) as error:
            if (
                isinstance(error, EngineError)
                and error.failure["category"] == "validation"
            ):
                raise
            raise _validation_error(
                "durable_request_validation_failed",
                "durable request failed local validation",
            ) from error
        expected_start_plan_id: str | None = None
        if request["command"]["type"] == "start_run":
            seal_request = {
                "type": "seal",
                "candidate": copy.deepcopy(request["command"]["candidate"]),
            }
            _snapshot_engine_request(seal_request)
            sealed_response = _exchange_engine(self.transport, seal_request)
            if sealed_response.get("type") != "sealed":
                raise _response_loss_error(seal_request, "invalid_engine_response")
            expected_start_plan_id = sealed_response["plan"]["plan_id"]
        engine_response = _exchange_engine(
            self.transport,
            request,
            expected_start_plan_id=expected_start_plan_id,
        )
        if engine_response.get("type") != "durable_executed":
            raise _response_loss_error(request, "invalid_engine_response")
        return engine_response["response"]

    def _run_page(
        self,
        response_type: Literal[
            "run_wait_page",
            "run_effect_page",
            "run_occurrence_page",
            "run_attempt_page",
        ],
        command: DurableCommand,
    ) -> DurableQueryPage:
        response = self._submit(command)
        if response.get("type") != response_type:
            raise _unexpected_response(response_type, response)
        return response["page"]

    @staticmethod
    def _build_command(factory: Callable[[], DurableCommand]) -> DurableCommand:
        try:
            return factory()
        except ValueError as error:
            raise _validation_error(
                "invalid_engine_request", "durable command failed local validation"
            ) from error


def _request_can_mutate(request: dict[str, Any]) -> bool:
    read_only_durable_commands = {
        "run_index_page",
        "run_current",
        "run_wait_page",
        "run_effect_page",
        "run_occurrence_page",
        "run_attempt_page",
        "run_item",
    }
    return request.get("type") in {"run", "observe_clock", "execute_live_evolution"} or (
        request.get("type") == "execute_durable"
        and request.get("command", {}).get("type") not in read_only_durable_commands
    )


def _interrupted_error(
    request: dict[str, Any],
    kind: Literal["cancelled", "timed_out"],
    *,
    request_began: bool,
) -> EngineError:
    if request_began and _request_can_mutate(request):
        return EngineError(
            {
                "category": "unknown_world_outcome",
                "phase": "transport",
                "code": f"engine_response_{kind}",
                "message": f"the Engine response was {kind} after a mutating request began",
                "retry_disposition": "reconcile",
            }
        )
    return EngineError(
        {
            "category": kind,
            "phase": "transport",
            "code": f"engine_response_{kind}",
            "message": f"the Engine response was {kind}",
            "retry_disposition": (
                "never" if kind == "cancelled" else "retry_same_request"
            ),
        }
    )


def _response_loss_error(request: dict[str, Any], code: str) -> EngineError:
    if _request_can_mutate(request):
        return EngineError(
            {
                "category": "unknown_world_outcome",
                "phase": "transport",
                "code": code,
                "message": "the Engine response was unavailable after a mutating request began",
                "retry_disposition": "reconcile",
            }
        )
    return _transport_error(code, "the Engine response was unavailable")


def _transport_invocation_error(
    request: dict[str, Any], error: Exception
) -> EngineError:
    if isinstance(error, EngineError):
        try:
            _validate_engine_failure(error.failure)
        except EngineError:
            return _response_loss_error(request, "invalid_engine_response")
        return error
    return _response_loss_error(request, "engine_transport_failed")


def _unsupported_engine_protocol_error(
    request: dict[str, Any], received: str
) -> EngineError:
    if _request_can_mutate(request):
        return EngineError(
            {
                "category": "unknown_world_outcome",
                "phase": "transport",
                "code": "unsupported_engine_protocol",
                "message": (
                    f"expected {ENGINE_PROTOCOL_VERSION}, received {received!r} "
                    "after a mutating request began"
                ),
                "retry_disposition": "reconcile",
            }
        )
    return EngineError(
        {
            "category": "contract_violation",
            "phase": "transport",
            "code": "unsupported_engine_protocol",
            "message": f"expected {ENGINE_PROTOCOL_VERSION}, received {received!r}",
            "contract": ENGINE_PROTOCOL_VERSION,
            "contract_side": "schema",
            "retry_disposition": "never",
        }
    )


_MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
_EMPTY_SHA256_ID = (
    "sha256:e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855"
)
_EMPTY_RESOURCE_MANIFEST_ROOT = (
    "sha256:6a754fadbb296b87040c37dab30caea6"
    "3de1bd1a85142bc82a03a7cf82e64dfc"
)


class _ExactFraction(float):
    """Internal exact decimal evidence for one non-integral JSON token."""

    exact: Decimal

    def __new__(cls, exact: Decimal) -> _ExactFraction:
        instance = super().__new__(cls, float(exact))
        instance.exact = exact
        return instance


def _wire_json_equal(left: object, right: object, depth: int = 0) -> bool:
    if depth > _MAX_JSON_DEPTH:
        return False
    if isinstance(left, _ExactFraction) or isinstance(right, _ExactFraction):
        return (
            isinstance(left, _ExactFraction)
            and isinstance(right, _ExactFraction)
            and left.exact == right.exact
        )
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _wire_json_equal(member, right[index], depth + 1)
            for index, member in enumerate(left)
        )
    if isinstance(left, dict):
        return set(left) == set(right) and all(
            _wire_json_equal(member, right[key], depth + 1)
            for key, member in left.items()
        )
    return left == right


def _validate_json_value(
    value: object,
    active: set[int] | None = None,
    depth: int = 0,
) -> None:
    if depth > _MAX_JSON_DEPTH:
        raise _validation_error(
            "invalid_engine_request", "JSON nesting exceeds the fixed depth limit"
        )
    if active is None:
        active = set()
    if isinstance(value, str) and any(
        0xD800 <= ord(character) <= 0xDFFF for character in value
    ):
        raise _validation_error(
            "invalid_engine_request", "string contains a non-scalar code point"
        )
    if isinstance(value, float) and (
        value != value
        or value in {float("inf"), float("-inf")}
        or value.is_integer() and abs(value) > _MAX_SAFE_JSON_INTEGER
    ):
        raise _validation_error(
            "invalid_engine_request", "number is outside the shared JSON domain"
        )
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > _MAX_SAFE_JSON_INTEGER:
        raise _validation_error(
            "invalid_engine_request", "integer is outside the shared JSON domain"
        )
    if isinstance(value, list):
        identity = id(value)
        if identity in active:
            raise _validation_error(
                "invalid_engine_request", "JSON value contains a cycle"
            )
        active.add(identity)
        try:
            for child in value:
                _validate_json_value(child, active, depth + 1)
        finally:
            active.remove(identity)
    elif isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise _validation_error(
                "invalid_engine_request", "JSON object keys must be strings"
            )
        identity = id(value)
        if identity in active:
            raise _validation_error(
                "invalid_engine_request", "JSON value contains a cycle"
            )
        active.add(identity)
        try:
            for key in value:
                _validate_json_value(key, active, depth + 1)
            for child in value.values():
                _validate_json_value(child, active, depth + 1)
        finally:
            active.remove(identity)
    elif value is not None and not isinstance(value, (bool, str, int, float)):
        raise _validation_error(
            "invalid_engine_request", "value is outside the JSON data model"
        )


def _strict_json_loads(value: str) -> object:
    def integer(value: str) -> int:
        if len(value) > _MAX_JSON_NUMBER_TOKEN_BYTES:
            raise ValueError("JSON number token exceeds the fixed byte limit")
        result = int(value)
        if abs(result) > _MAX_SAFE_JSON_INTEGER:
            raise ValueError("integer is outside the shared JSON domain")
        return result

    def floating(value: str) -> int | _ExactFraction:
        if len(value) > _MAX_JSON_NUMBER_TOKEN_BYTES:
            raise ValueError("JSON number token exceeds the fixed byte limit")
        exponent = re.search(r"[eE]([+-]?)(\d+)$", value)
        if exponent is not None and len(exponent.group(2)) > _MAX_JSON_EXPONENT_DIGITS:
            raise ValueError("JSON number exponent exceeds the fixed digit limit")
        try:
            exact = Decimal(value)
        except InvalidOperation as error:
            raise ValueError("invalid JSON number") from error
        if not exact.is_finite():
            raise ValueError("number is outside the shared JSON domain")
        if exact == exact.to_integral_value():
            if abs(exact) > Decimal(_MAX_SAFE_JSON_INTEGER):
                raise ValueError("number is outside the shared JSON domain")
            return int(exact)
        result = float(exact)
        if not math.isfinite(result):
            raise ValueError("number is outside the shared JSON domain")
        if result.is_integer():
            raise ValueError(
                "fractional number is not distinguishable from an integer"
            )
        return _ExactFraction(exact)

    try:
        decoded = json.loads(
            value,
            object_pairs_hook=_unique_json_object,
            parse_int=integer,
            parse_float=floating,
            parse_constant=lambda item: (_ for _ in ()).throw(ValueError(f"invalid number {item}")),
        )
    except RecursionError as error:
        raise ValueError("JSON nesting exceeds the fixed depth limit") from error
    _validate_decoded_json_depth(decoded)
    if _json_contains_surrogate(decoded):
        raise ValueError("JSON string contains a non-scalar code point")
    return decoded


def _validate_decoded_json_depth(value: object) -> None:
    pending = [(value, 0)]
    while pending:
        current, depth = pending.pop()
        if depth > _MAX_JSON_DEPTH:
            raise ValueError("JSON nesting exceeds the fixed depth limit")
        if isinstance(current, list):
            pending.extend((member, depth + 1) for member in current)
        elif isinstance(current, dict):
            pending.extend((key, depth + 1) for key in current)
            pending.extend((member, depth + 1) for member in current.values())


def _json_contains_surrogate(value: object) -> bool:
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str) and any(
            0xD800 <= ord(character) <= 0xDFFF for character in current
        ):
            return True
        if isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, dict):
            pending.extend(current)
            pending.extend(current.values())
    return False


def _transport_error(code: str, message: str) -> EngineError:
    return EngineError(
        {
            "category": "transport_failure",
            "phase": "transport",
            "code": code,
            "message": message,
        }
    )


def _validation_error(code: str, message: str) -> EngineError:
    return EngineError(
        {
            "category": "validation",
            "phase": "validate_request",
            "code": code,
            "message": message,
            "retry_disposition": "correct_and_retry",
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
    _validate_engine_envelope_shape(envelope)
    if envelope["outcome"] == "failure":
        _validate_engine_failure(envelope["error"])
    else:
        if not isinstance(envelope["request"], dict):
            raise _transport_error(
                "invalid_engine_response", "success request echo is not an object"
            )
        _validate_success_response(envelope["response"])


def _validate_engine_envelope_shape(envelope: object) -> None:
    if not isinstance(envelope, dict):
        raise _transport_error("invalid_engine_response", "response envelope is not an object")
    outcome = envelope.get("outcome")
    expected_keys = (
        {"outcome", "engine_protocol", "request", "response"}
        if outcome == "success"
        else {"outcome", "engine_protocol", "error"}
    )
    if (
        not isinstance(outcome, str)
        or outcome not in {"success", "failure"}
        or set(envelope) != expected_keys
    ):
        raise _transport_error("invalid_engine_response", "response envelope is not closed")
    if envelope["engine_protocol"] != ENGINE_PROTOCOL_VERSION:
        raise EngineError(
            {
                "category": "contract_violation",
                "phase": "transport",
                "code": "unsupported_engine_protocol",
                "message": (
                    f"expected {ENGINE_PROTOCOL_VERSION}, received "
                    f"{envelope['engine_protocol']!r}"
                ),
                "contract": ENGINE_PROTOCOL_VERSION,
                "contract_side": "schema",
                "retry_disposition": "never",
            }
        )


def _validate_success_response(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise _transport_error("invalid_engine_response", "success response is not tagged")
    fields = {
        "sealed": {"type", "plan"},
        "sealed_resource": {"type", "resource"},
        "verified_wait_activation": {"type", "activation"},
        "verified_durable_command": {"type", "command"},
        "clock_observed": {"type", "result"},
        "verified_evolution_command": {"type", "command"},
        "verified_live_evolution_command": {"type", "command"},
        "execution_boundary": {"type", "execution"},
        "durable_executed": {"type", "response"},
        "live_evolution_executed": {"type", "commit"},
        "verified": {"type"},
    }.get(value["type"])
    if fields is None or set(value) != fields:
        raise _transport_error("invalid_engine_response", "success response fields are not closed")
    if value["type"] == "sealed":
        _validate_sealed_plan(value["plan"])
    elif value["type"] == "sealed_resource":
        _validate_resource_handle(value["resource"])
    elif value["type"] == "verified_wait_activation":
        _validate_wait_activation_response(value["activation"])
    elif value["type"] == "verified_durable_command":
        _validate_durable_command_response(value["command"])
    elif value["type"] == "clock_observed":
        _validate_clock_observation_result(value["result"])
    if value["type"] == "durable_executed":
        _validate_durable_response(value["response"])
    if value["type"] == "live_evolution_executed":
        _validate_evolution_commit(value["commit"])

    if value["type"] == "execution_boundary":
        _validate_execution_outcome(value["execution"])
    elif value["type"] == "verified_evolution_command":
        _validate_evolution_command(value["command"])
    elif value["type"] == "verified_live_evolution_command":
        _validate_live_evolution_command(value["command"])


def _validate_success_response_for_request(
    request: dict[str, Any],
    response: dict[str, Any],
    *,
    expected_start_plan_id: str | None = None,
) -> None:
    request_type = request.get("type")
    if request_type == "seal":
        if not _wire_json_equal(response["plan"]["candidate"], request.get("candidate")):
            raise _transport_error(
                "invalid_engine_response",
                "sealed Plan does not match the exact candidate",
            )
        return
    returned_member = {
        "verify_wait_activation": ("activation", "activation"),
        "verify_durable_command": ("command", "command"),
        "verify_evolution_command": ("command", "command"),
        "verify_live_evolution_command": ("command", "command"),
    }.get(request_type)
    if returned_member is not None:
        request_member, response_member = returned_member
        if request_member not in request or not _wire_json_equal(
            response[response_member], request[request_member]
        ):
            raise _transport_error(
                "invalid_engine_response",
                "verified value does not match the exact request",
            )
        return
    if request_type == "seal_resource":
        if "candidate" not in request:
            raise _transport_error(
                "invalid_engine_response", "Resource seal request has no candidate"
            )
        returned_candidate = {
            key: copy.deepcopy(value)
            for key, value in response["resource"].items()
            if key != "resource_id"
        }
        requested_candidate = copy.deepcopy(request["candidate"])
        if not isinstance(requested_candidate, dict):
            raise _transport_error(
                "invalid_engine_response", "Resource seal request candidate is invalid"
            )
        if not _wire_json_equal(returned_candidate, requested_candidate):
            raise _transport_error(
                "invalid_engine_response",
                "sealed Resource does not match the exact candidate",
            )
        return
    if request_type == "observe_clock":
        target = request.get("target")
        result = response["result"]
        observation = result["observation"]
        if (
            not isinstance(target, dict)
            or result["run_id"] != request.get("run_id")
            or observation["source_id"] != target.get("source_id")
            or observation["source_generation"] != target.get("source_generation")
        ):
            raise _transport_error(
                "invalid_engine_response",
                "Clock observation does not match its complete request",
            )
        return
    if request_type == "run":
        if not _execution_outcome_matches_request(
            response["execution"], request.get("plan"), request.get("run_id")
        ):
            raise _transport_error(
                "invalid_engine_response",
                "execution outcome does not match its complete request",
            )
        return
    if request_type == "execute_durable":
        if not _durable_response_matches_command(
            request.get("command"),
            response["response"],
            expected_start_plan_id,
        ):
            raise _transport_error(
                "invalid_engine_response",
                "durable response does not match its complete command",
            )


def _execution_outcome_matches_request(
    outcome: object, plan: object, run_id: object
) -> bool:
    if not isinstance(outcome, dict) or not isinstance(plan, dict):
        return False
    status = outcome.get("status")
    payload_name = {
        "completed": "result",
        "suspended": "suspension",
        "release_required": "release",
        "reconciliation_required": "reconciliation",
    }.get(status)
    if payload_name is None:
        return False
    payload = outcome.get(payload_name)
    if (
        not isinstance(payload, dict)
        or payload.get("run_id") != run_id
        or payload.get("plan_id") != plan.get("plan_id")
    ):
        return False
    if status != "suspended":
        return True
    step = _find_plan_step(
        plan.get("candidate"), payload.get("definition_id"), payload.get("site_id")
    )
    return (
        isinstance(step, dict)
        and step.get("op") == "wait"
        and _wire_json_equal(step.get("wait"), payload.get("wait"))
        and step.get("bind") == payload.get("result_bind")
    )


def _find_plan_step(
    candidate: object, definition_id: object, site_id: object
) -> dict[str, Any] | None:
    if not isinstance(candidate, dict) or not isinstance(
        candidate.get("definitions"), list
    ):
        return None
    definition = next(
        (
            item
            for item in candidate["definitions"]
            if isinstance(item, dict) and item.get("id") == definition_id
        ),
        None,
    )
    if not isinstance(definition, dict):
        return None

    def find(region: object) -> dict[str, Any] | None:
        if not isinstance(region, dict) or not isinstance(region.get("steps"), list):
            return None
        for step in region["steps"]:
            if not isinstance(step, dict):
                continue
            if step.get("id") == site_id:
                return step
            if step.get("op") == "scope":
                nested = find(step.get("body"))
                if nested is not None:
                    return nested
        return None

    return find(definition.get("body"))


def _durable_response_matches_command(
    command: object,
    response: object,
    expected_start_plan_id: str | None,
) -> bool:
    if not isinstance(command, dict) or not isinstance(response, dict):
        return False
    command_type = command.get("type")
    expected_response = {
        "start_run": "run_boundary",
        "resume_run": "run_boundary",
        "takeover_run": "run_boundary",
        "release_effect": "run_boundary",
        "resolve_effect": "effect_resolved",
        "cancel_run": "run_cancelled",
        "activate_wait": "wait_activated",
        "run_index_page": "run_index_page",
        "run_current": "run_current",
        "run_wait_page": "run_wait_page",
        "run_effect_page": "run_effect_page",
        "run_occurrence_page": "run_occurrence_page",
        "run_attempt_page": "run_attempt_page",
        "run_item": "run_item",
    }.get(command_type)
    if response.get("type") != expected_response:
        return False
    if command_type in {"start_run", "resume_run", "takeover_run"}:
        boundary = response.get("boundary")
        if not isinstance(boundary, dict):
            return False
        if boundary.get("status") != "completed":
            return True
        result = boundary.get("result")
        return (
            isinstance(result, dict)
            and result.get("run_id") == command.get("run_id")
            and (
                command_type != "start_run"
                or expected_start_plan_id is not None
                and result.get("plan_id") == expected_start_plan_id
            )
        )
    if command_type == "release_effect":
        boundary = response.get("boundary")
        if not isinstance(boundary, dict):
            return False
        if boundary.get("status") in {
            "reconciliation_required",
            "effect_unavailable",
            "effect_not_applied",
        }:
            return boundary.get("intent_id") == command.get("intent_id")
        if boundary.get("status") == "release_required":
            return command.get("intent_id") in boundary.get("intent_ids", [])
        return True
    if command_type == "cancel_run":
        receipt = response.get("receipt")
        return (
            isinstance(receipt, dict)
            and _wire_json_equal(
                receipt.get("command"), _cancellation_command_from(command)
            )
        )
    if command_type == "resolve_effect":
        receipt = response.get("receipt")
        if not isinstance(receipt, dict):
            return False
        return _wire_json_equal(
            receipt.get("command"), _effect_resolution_command_from(command)
        )
    if command_type == "activate_wait":
        receipt = response.get("receipt")
        activation = receipt.get("activation") if isinstance(receipt, dict) else None
        return (
            isinstance(activation, dict)
            and activation.get("activation_id") == command.get("activation_id")
            and _wire_json_equal(activation.get("source"), command.get("source"))
            and _wire_json_equal(activation.get("wait_ids"), command.get("wait_ids"))
        )
    if command_type == "run_current":
        current = response.get("current")
        return (
            command.get("expected_revision") is None
            or command.get("expected_revision") == response.get("observed_revision")
        ) and (
            current is None
            or isinstance(current, dict)
            and current.get("run_id") == command.get("run_id")
        ) and _json_size(response) <= 1024 * 1024
    if command_type in {
        "run_index_page",
        "run_wait_page",
        "run_effect_page",
        "run_occurrence_page",
        "run_attempt_page",
    }:
        if command_type != "run_index_page" and response.get("run_id") != command.get("run_id"):
            return False
        return _durable_query_page_matches_command(command, response.get("page")) and (
            _json_size(response) <= command.get("max_canonical_bytes", 0)
        )
    if command_type == "run_item":
        item = response.get("item")
        return (
            response.get("run_id") == command.get("run_id")
            and (
                command.get("expected_revision") is None
                or command.get("expected_revision") == response.get("observed_revision")
            )
            and (
                item is None
                or _durable_run_item_matches_selector(item, command.get("selector"))
            )
            and _json_size(response) <= command.get("max_canonical_bytes", 0)
        )
    return True


def _durable_query_page_matches_command(
    command: dict[str, Any], page: object
) -> bool:
    if not isinstance(page, dict) or not isinstance(page.get("items"), list):
        return False
    cursor = command.get("cursor")
    if command.get("expected_revision") is not None and (
        command["expected_revision"] != page.get("observed_revision")
    ):
        return False
    if isinstance(cursor, dict) and (
        cursor.get("source_revision") != page.get("observed_revision")
        or cursor.get("source_root") != page.get("source_root")
    ):
        return False
    if len(page["items"]) > command.get("limit", 0):
        return False
    if isinstance(cursor, dict) and page["items"]:
        first_key = _durable_summary_key(cursor.get("query_kind"), page["items"][0])
        if first_key is None:
            return False
        first_position = (_durable_page_key_hash(first_key), first_key)
        cursor_position = cursor.get("position")
        if not isinstance(cursor_position, dict) or first_position <= (
            cursor_position.get("key_hash"), cursor_position.get("canonical_key")
        ):
            return False
    return True


def _durable_run_item_matches_selector(item: object, selector: object) -> bool:
    if not isinstance(item, dict) or not isinstance(selector, dict):
        return False
    field = {
        "wait": ("wait", "wait_id"),
        "effect": ("effect", "intent_id"),
        "occurrence": ("occurrence", "occurrence_id"),
        "attempt": ("attempt", "attempt_id"),
    }.get(selector.get("kind"))
    if field is None or item.get("kind") != selector.get("kind"):
        return False
    member = item.get(field[0])
    return isinstance(member, dict) and member.get(field[1]) == selector.get(field[1])


def _effect_resolution_command_from(command: dict[str, Any]) -> dict[str, Any]:
    return {
        field: copy.deepcopy(command[field])
        for field in (
            "resolution_id",
            "run_id",
            "intent_id",
            "execution_binding",
            "occurrence_binding",
            "claim_owner",
            "claim_epoch",
            "resolution",
            "value",
        )
    }


def _cancellation_command_from(command: dict[str, Any]) -> dict[str, Any]:
    return {
        field: copy.deepcopy(command[field])
        for field in ("cancellation_id", "run_id", "reason")
    }


def _validate_resource_handle(value: object) -> None:
    resource = _require_closed_subset(
        value,
        {"resource_id", "resource_version", "shape", "media_type", "integrity"},
        {"inline", "manifest", "annotations"},
        "Resource Handle",
    )
    shape = resource["shape"]
    if (
        resource["resource_version"] != "cymule.resource/4"
        or not _is_sha256_id(resource["resource_id"])
        or not isinstance(shape, str)
        or shape not in {"inline", "object", "collection", "directory", "snapshot"}
        or not _is_resource_media_type(resource["media_type"])
        or not isinstance(resource["integrity"], dict)
    ):
        raise _transport_error("invalid_engine_response", "Resource Handle fields are invalid")
    integrity = resource["integrity"]
    integrity_kind = integrity.get("kind")
    expected = {
        "inline": {"kind"}, "content": {"kind", "digest", "size"},
        "version": {"kind", "authority", "version"}, "live": {"kind", "identity"},
    }.get(integrity_kind if isinstance(integrity_kind, str) else "")
    if expected is None or set(integrity) != expected:
        raise _transport_error("invalid_engine_response", "Resource integrity is not closed")
    if integrity_kind == "content" and (
        not _is_sha256_id(integrity["digest"])
        or not _is_nonnegative_safe_integer(integrity["size"])
    ):
        raise _transport_error("invalid_engine_response", "content integrity is invalid")
    if integrity_kind == "version" and (
        not _is_resource_token(integrity["authority"])
        or not _is_resource_token(integrity["version"])
    ):
        raise _transport_error("invalid_engine_response", "version integrity is invalid")
    if integrity_kind == "live" and not _is_resource_token(integrity["identity"]):
        raise _transport_error("invalid_engine_response", "live integrity is invalid")

    if shape == "inline":
        if (
            not isinstance(resource.get("inline"), dict)
            or integrity_kind != "inline"
            or "manifest" in resource
        ):
            raise _transport_error(
                "invalid_engine_response", "inline Resource evidence is invalid"
            )
        _validate_inline_resource(resource["inline"])
    elif "inline" in resource or integrity_kind == "inline":
        raise _transport_error(
            "invalid_engine_response", "external Resource retained inline data"
        )

    if "manifest" in resource:
        _validate_resource_manifest(resource["manifest"])
        manifest = resource["manifest"]
        if (
            shape not in {"collection", "directory", "snapshot"}
            or integrity_kind != "content"
            or integrity["digest"] != manifest["digest"]
            or integrity["size"] != manifest["size"]
        ):
            raise _transport_error(
                "invalid_engine_response",
                "Resource manifest does not match content integrity",
            )

    if "annotations" in resource:
        annotations = resource["annotations"]
        if not isinstance(annotations, dict) or not annotations:
            raise _transport_error(
                "invalid_engine_response", "Resource annotations are invalid"
            )
        for key, annotation in annotations.items():
            annotation_size = _utf8_size(annotation)
            if (
                not _is_resource_token(key)
                or annotation_size is None
                or annotation_size > 4096
            ):
                raise _transport_error(
                    "invalid_engine_response", "Resource annotation is invalid"
                )


def _validate_inline_resource(value: dict[str, Any]) -> None:
    encoding = value.get("encoding")
    fields = {
        "utf8": {"encoding", "text"},
        "json": {"encoding", "value"},
        "base64": {"encoding", "data"},
    }.get(encoding if isinstance(encoding, str) else "")
    if fields is None or set(value) != fields:
        raise _transport_error(
            "invalid_engine_response", "inline Resource data is not closed"
        )
    if encoding == "utf8":
        size = _utf8_size(value["text"])
        if size is None or size > 1024 * 1024:
            raise _transport_error(
                "invalid_engine_response", "inline UTF-8 Resource is invalid"
            )
    elif encoding == "json":
        try:
            encoded = json.dumps(
                value["value"],
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        except (TypeError, ValueError, UnicodeEncodeError) as error:
            raise _transport_error(
                "invalid_engine_response", "inline JSON Resource is invalid"
            ) from error
        if len(encoded) > 1024 * 1024:
            raise _transport_error(
                "invalid_engine_response", "inline JSON Resource is invalid"
            )
    else:
        data = value["data"]
        if not isinstance(data, str) or not data.isascii():
            raise _transport_error(
                "invalid_engine_response", "inline base64 Resource is invalid"
            )
        try:
            decoded = base64.b64decode(data, validate=True)
        except (ValueError, binascii.Error) as error:
            raise _transport_error(
                "invalid_engine_response", "inline base64 Resource is invalid"
            ) from error
        if len(decoded) > 1024 * 1024 or base64.b64encode(decoded).decode() != data:
            raise _transport_error(
                "invalid_engine_response", "inline base64 Resource is not canonical"
            )


def _validate_resource_manifest(value: object) -> None:
    manifest = _require_closed_record(
        value,
        {
            "manifest_version",
            "media_type",
            "digest",
            "size",
            "entry_count",
            "root_digest",
        },
        "Resource manifest",
    )
    if (
        manifest["manifest_version"] != "cymule.resource-manifest/3"
        or manifest["media_type"]
        != "application/vnd.cymule.resource-manifest+jsonl"
        or not _is_sha256_id(manifest["digest"])
        or not _is_sha256_id(manifest["root_digest"])
        or not _is_nonnegative_safe_integer(manifest["size"])
        or not _is_nonnegative_safe_integer(manifest["entry_count"])
        or manifest["digest"] != _resource_manifest_descriptor_id(manifest)
        or (manifest["entry_count"] == 0 and manifest["size"] != 0)
        or (manifest["entry_count"] > 0 and manifest["size"] == 0)
        or (
            manifest["entry_count"] == 0
            and (
                manifest["root_digest"] != _EMPTY_RESOURCE_MANIFEST_ROOT
            )
        )
    ):
        raise _transport_error(
            "invalid_engine_response", "Resource manifest is invalid"
        )


def _resource_manifest_descriptor_id(manifest: dict[str, Any]) -> str:
    identity = json.dumps(
        {
            "entry_count": manifest["entry_count"],
            "media_type": manifest["media_type"],
            "root_digest": manifest["root_digest"],
            "size": manifest["size"],
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    digest = hashlib.sha256(b"cymule.resource-manifest/3\0" + identity).hexdigest()
    return f"sha256:{digest}"


def _is_resource_media_type(value: object) -> bool:
    size = _utf8_size(value)
    return (
        isinstance(value, str)
        and size is not None
        and 3 <= size <= 255
        and re.fullmatch(r"[a-z0-9!#$%&'*+.^_`|~-]+/[a-z0-9!#$%&'*+.^_`|~-]+", value)
        is not None
    )


def _is_resource_token(value: object) -> bool:
    size = _utf8_size(value)
    return (
        isinstance(value, str)
        and size is not None
        and 1 <= size <= 2048
        and not any(
            ord(character) < 32 or 127 <= ord(character) <= 159
            for character in value
        )
    )


def _is_nonnegative_safe_integer(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= _MAX_SAFE_JSON_INTEGER
    )


def _validate_wait_activation_response(value: object) -> None:
    activation = _require_closed_record(value, {"activation_version", "activation_id", "source", "wait_ids", "result"}, "wait activation")
    if activation["activation_version"] != "cymule.wait-activation/2":
        raise _transport_error("invalid_engine_response", "wait activation version is invalid")
    _validate_core_identity(activation["activation_id"], "wait activation")
    _require_sorted_unique_string_list(activation["wait_ids"], "wait targets")
    if not activation["wait_ids"] or len(activation["wait_ids"]) > 4096:
        raise _transport_error(
            "invalid_engine_response", "wait activation target count is invalid"
        )
    for wait_id in activation["wait_ids"]:
        _validate_content_id(wait_id, "wait activation target")
    _validate_artifact_ref(activation["result"])
    if activation["result"]["kind"] != "cymule.wait-result/1":
        raise _transport_error(
            "invalid_engine_response", "wait activation result kind is invalid"
        )
    source = activation["source"]
    expected = {"signal": {"kind", "key"}, "timer": {"kind", "timer_id"}}.get(source.get("kind") if isinstance(source, dict) else None)
    if expected is None or set(source) != expected:
        raise _transport_error("invalid_engine_response", "wait activation source is not closed")
    source_identity = source["key"] if source["kind"] == "signal" else source["timer_id"]
    _validate_core_identity(source_identity, "wait activation source")


def _validate_durable_command_response(value: object) -> None:
    if not isinstance(value, dict) or value.get("control_version") != "cymule.durable-control/4":
        raise _transport_error("invalid_engine_response", "durable command is invalid")
    expected = {
        "start_run": {"type", "control_version", "run_id", "candidate", "input", "execution"},
        "resume_run": {"type", "control_version", "run_id", "execution"},
        "takeover_run": {"type", "control_version", "run_id", "expected_fence", "execution"},
        "activate_wait": {"type", "control_version", "activation_id", "source", "wait_ids", "value"},
        "release_effect": {"type", "control_version", "intent_id", "execution"},
        "resolve_effect": {
            "type", "control_version", "resolution_id", "run_id", "intent_id",
            "execution_binding", "occurrence_binding", "claim_owner", "claim_epoch",
            "resolution", "value",
        },
        "cancel_run": {"type", "control_version", "cancellation_id", "run_id", "reason"},
        "run_index_page": {
            "type", "control_version", "expected_revision", "cursor", "limit",
            "max_canonical_bytes",
        },
        "run_current": {
            "type", "control_version", "run_id", "expected_revision",
        },
        "run_wait_page": {
            "type", "control_version", "run_id", "expected_revision", "cursor",
            "limit", "max_canonical_bytes",
        },
        "run_effect_page": {
            "type", "control_version", "run_id", "expected_revision", "cursor",
            "limit", "max_canonical_bytes",
        },
        "run_occurrence_page": {
            "type", "control_version", "run_id", "expected_revision", "cursor",
            "limit", "max_canonical_bytes",
        },
        "run_attempt_page": {
            "type", "control_version", "run_id", "expected_revision", "cursor",
            "limit", "max_canonical_bytes",
        },
        "run_item": {
            "type", "control_version", "run_id", "expected_revision", "selector",
            "max_canonical_bytes",
        },
    }.get(value.get("type"))
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "durable command is not closed")
    if "run_id" in value:
        _validate_core_identity(value["run_id"], "durable Run")
    if value["type"] == "activate_wait":
        _validate_core_identity(value["activation_id"], "durable activation")
        _require_sorted_unique_string_list(value["wait_ids"], "wait targets")
        if not value["wait_ids"] or len(value["wait_ids"]) > 4096:
            raise _transport_error(
                "invalid_engine_response", "durable activation target count is invalid"
            )
        for wait_id in value["wait_ids"]:
            _validate_content_id(wait_id, "durable activation target")
        source = value["source"]
        expected_source = {
            "signal": {"kind", "key"},
            "timer": {"kind", "timer_id"},
        }.get(source.get("kind") if isinstance(source, dict) else None)
        if expected_source is None or set(source) != expected_source:
            raise _transport_error(
                "invalid_engine_response", "durable activation source is not closed"
            )
        source_identity = source["key"] if source["kind"] == "signal" else source["timer_id"]
        _validate_core_identity(source_identity, "durable activation source")
    if value["type"] in {"start_run", "resume_run", "takeover_run", "release_effect"}:
        execution = _require_closed_record(value["execution"], {"owner", "clock", "ttl"}, "execution claim request")
        _validate_core_identity(execution["owner"], "execution owner")
        if not isinstance(execution["ttl"], int) or isinstance(execution["ttl"], bool) or execution["ttl"] < 1:
            raise _transport_error("invalid_engine_response", "execution claim owner or TTL is invalid")
        _validate_clock_observation_ref(execution["clock"])
    if value["type"] == "release_effect" and not _is_sha256_id(value["intent_id"]):
        raise _transport_error(
            "invalid_engine_response", "released effect intent is invalid"
        )
    if value["type"] == "resolve_effect":
        _require_strings(
            value,
            {
                "resolution_id", "run_id", "intent_id", "occurrence_binding",
                "claim_owner", "resolution",
            },
        )
        _validate_artifact_ref(value["execution_binding"])
        if (
            value["execution_binding"]["kind"]
            != "cymule.execution-binding/2"
            or not _is_sha256_id(value["intent_id"])
            or not _is_sha256_id(value["occurrence_binding"])
            or not _is_positive_safe_integer(value["claim_epoch"])
            or value["resolution"]
            not in {"resolved_applied", "resolved_not_applied"}
            or value["resolution"] == "resolved_not_applied"
            and value["value"] is not None
        ):
            raise _transport_error(
                "invalid_engine_response", "effect resolution authority is invalid"
            )
    if value["type"] == "takeover_run" and (not isinstance(value["expected_fence"], int) or isinstance(value["expected_fence"], bool) or value["expected_fence"] < 1 or value["expected_fence"] > 9_007_199_254_740_991):
        raise _transport_error("invalid_engine_response", "takeover fence is invalid")
    if value["type"] in {
        "run_index_page", "run_current", "run_wait_page", "run_effect_page",
        "run_occurrence_page", "run_attempt_page", "run_item",
    }:
        _validate_expected_revision(value["expected_revision"])
    page_kinds = {
        "run_index_page": "run_index",
        "run_wait_page": "run_waits",
        "run_effect_page": "run_effects",
        "run_occurrence_page": "run_occurrences",
        "run_attempt_page": "run_attempts",
    }
    if value["type"] in page_kinds:
        if not _is_positive_safe_integer_at_most(value["limit"], 256) or not (
            _is_positive_safe_integer_at_most(
                value["max_canonical_bytes"], 1024 * 1024
            )
        ):
            raise _transport_error(
                "invalid_engine_response", "durable page query bounds are invalid"
            )
        run_id = None if value["type"] == "run_index_page" else value["run_id"]
        if value["cursor"] is not None:
            _validate_durable_page_cursor(value["cursor"])
            if (
                value["cursor"]["query_kind"] != page_kinds[value["type"]]
                or value["cursor"]["run_id"] != run_id
                or value["expected_revision"]
                != value["cursor"]["source_revision"]
            ):
                raise _transport_error(
                    "invalid_engine_response", "durable page cursor scope is invalid"
                )
    if value["type"] == "run_item":
        _validate_durable_run_item_selector(value["selector"])
        if not _is_positive_safe_integer_at_most(
            value["max_canonical_bytes"], 13 * 1024 * 1024
        ):
            raise _transport_error(
                "invalid_engine_response", "exact Run-item byte budget is invalid"
            )


def _validate_expected_revision(value: object) -> None:
    if value is not None:
        _validate_content_id(value, "durable query expected revision")


def _is_positive_safe_integer_at_most(value: object, maximum: int) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 1 <= value <= maximum
        and value <= 9_007_199_254_740_991
    )


def _validate_clock_observation_ref(value: object) -> None:
    reference = _require_closed_record(value, {"clock_version", "observation_id", "source_id", "source_generation", "scope"}, "Clock observation reference")
    if reference["clock_version"] != "cymule.clock-observation/2" or re.fullmatch(r"sha256:[0-9a-f]{64}", str(reference["observation_id"])) is None or re.fullmatch(r"sha256:[0-9a-f]{64}", str(reference["source_generation"])) is None:
        raise _transport_error("invalid_engine_response", "Clock observation reference is invalid")
    for field in ("source_id", "scope"):
        _validate_core_identity(reference[field], f"Clock observation {field}")


def _validate_clock_observation_result(value: object) -> None:
    result = _require_closed_record(
        value, {"run_id", "observation"}, "Clock observation result"
    )
    _validate_core_identity(result["run_id"], "Clock observation Run")
    _validate_clock_observation_ref(result["observation"])


def _validate_engine_store_target(value: object) -> None:
    if not isinstance(value, dict) or set(value) not in (
        {"provider", "location"},
        {"provider", "location", "domain"},
    ):
        raise _transport_error(
            "invalid_engine_response", "Engine Store target is not closed"
        )
    provider = value["provider"]
    location = value["location"]
    if (
        _unicode_scalar_count(provider) is None
        or not 1 <= len(provider) <= 256
        or _unicode_scalar_count(location) is None
        or not 1 <= len(location) <= 4096
    ):
        raise _transport_error(
            "invalid_engine_response", "Engine Store target fields are invalid"
        )
    if "domain" in value:
        domain = value["domain"]
        if _unicode_scalar_count(domain) is None or not 1 <= len(domain) <= 512:
            raise _transport_error(
                "invalid_engine_response", "Engine Store target domain is invalid"
            )


def _validate_process_map(
    value: object,
    label: str,
    *,
    require_nonempty_values: bool,
    require_nonempty_map: bool,
    require_content_ids: bool,
) -> None:
    if (
        not isinstance(value, dict)
        or len(value) > 4096
        or require_nonempty_map
        and not value
    ):
        raise _transport_error("invalid_engine_response", f"{label} is invalid")
    for key, member in value.items():
        if (
            not isinstance(key, str)
            or not key
            or "=" in key
            or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in key)
            or not isinstance(member, str)
            or "\0" in member
            or require_nonempty_values
            and not member
            or require_content_ids
            and not _is_sha256_id(member)
        ):
            raise _transport_error(
                "invalid_engine_response", f"{label} is outside its closed contract"
            )


def _validate_engine_process_config(
    value: object, *, expected_message_limit: int | None = None
) -> None:
    process = _require_closed_record(
        value,
        {
            "executable",
            "arguments",
            "environment",
            "working_directory",
            "runtime_closure",
            "timeout_ms",
            "message_limit",
            "closure_limit",
        },
        "Engine process configuration",
    )
    executable = process["executable"]
    working_directory = process["working_directory"]
    if (
        _unicode_scalar_count(executable) is None
        or not 1 <= len(executable) <= 4096
        or not os.path.isabs(executable)
        or "\0" in executable
        or not isinstance(process["arguments"], list)
        or len(process["arguments"]) > 4096
        or any(
            not isinstance(argument, str) or "\0" in argument
            for argument in process["arguments"]
        )
        or working_directory is not None
        and (
            _unicode_scalar_count(working_directory) is None
            or not 1 <= len(working_directory) <= 4096
            or not os.path.isabs(working_directory)
            or "\0" in working_directory
        )
        or not _is_positive_safe_integer(process["timeout_ms"])
        or not isinstance(process["message_limit"], int)
        or isinstance(process["message_limit"], bool)
        or not 1 <= process["message_limit"] <= 64 * 1024 * 1024
        or not isinstance(process["closure_limit"], int)
        or isinstance(process["closure_limit"], bool)
        or not 1 <= process["closure_limit"] <= 1024 * 1024 * 1024
    ):
        raise _transport_error(
            "invalid_engine_response",
            "Engine process configuration is outside the bounded contract",
        )
    _validate_process_map(
        process["environment"],
        "Engine process environment",
        require_nonempty_values=False,
        require_nonempty_map=False,
        require_content_ids=False,
    )
    _validate_process_map(
        process["runtime_closure"],
        "Engine runtime closure",
        require_nonempty_values=True,
        require_nonempty_map=True,
        require_content_ids=True,
    )
    if (
        process["message_limit"] not in {8 * 1024 * 1024, 16 * 1024 * 1024}
        if expected_message_limit is None
        else process["message_limit"] != expected_message_limit
    ):
        raise _transport_error(
            "invalid_engine_response",
            "Engine process message limit does not match its protocol context",
        )


def _validate_engine_plugin_target(
    value: object,
    *,
    require_revision: bool,
    expected_message_limit: int | None = None,
) -> None:
    if not isinstance(value, dict) or set(value) not in (
        {"provider", "process"},
        {"provider", "process", "revision"},
    ):
        raise _transport_error(
            "invalid_engine_response", "Engine plugin target is not closed"
        )
    if value["provider"] != "cymule.executor-process/1":
        raise _transport_error(
            "invalid_engine_response", "Engine plugin provider is unsupported"
        )
    _validate_engine_process_config(
        value["process"], expected_message_limit=expected_message_limit
    )
    if "revision" in value and not _is_sha256_id(value["revision"]):
        raise _transport_error(
            "invalid_engine_response", "Engine plugin revision is invalid"
        )
    if require_revision and "revision" not in value:
        raise _transport_error(
            "invalid_engine_response", "evolution plugin target is not revision-pinned"
        )


def _validate_engine_clock_target(value: object) -> None:
    target = _require_closed_record(
        value,
        {"provider", "location", "source_id", "source_generation"},
        "Engine Clock target",
    )
    if (
        target["provider"] != "cymule.clock-system/2"
        or _unicode_scalar_count(target["location"]) is None
        or not 1 <= len(target["location"]) <= 4096
        or not _is_core_identity(target["source_id"])
        or not _is_sha256_id(target["source_generation"])
    ):
        raise _transport_error(
            "invalid_engine_response", "Engine Clock target is invalid"
        )


def _validate_engine_durable_target(
    value: object, command: DurableCommand
) -> None:
    if (
        not isinstance(value, dict)
        or "store" not in value
        or not set(value) <= {"store", "executor", "clock"}
    ):
        raise _transport_error(
            "invalid_engine_response", "durable Engine target is not closed"
        )
    _validate_engine_store_target(value["store"])
    requires_executor = command["type"] in {
        "start_run",
        "resume_run",
        "takeover_run",
        "release_effect",
        "resolve_effect",
    }
    requires_clock = command["type"] in {
        "start_run",
        "resume_run",
        "takeover_run",
        "release_effect",
    }
    if requires_executor != ("executor" in value) or requires_clock != ("clock" in value):
        raise _transport_error(
            "invalid_engine_response",
            "durable Engine target does not match the command capability",
        )
    if "executor" in value:
        _validate_engine_plugin_target(
            value["executor"],
            require_revision=False,
            expected_message_limit=8 * 1024 * 1024,
        )
    if "clock" in value:
        _validate_engine_clock_target(value["clock"])


def _validate_engine_evolution_target(
    value: object, command: LiveEvolutionCommand
) -> None:
    target = _require_closed_record(
        value,
        {
            "store",
            "migration_adapter",
            "shadow_driver",
            "target_execution_bindings",
        },
        "evolution Engine target",
    )
    _validate_engine_store_target(target["store"])
    operation = (
        command.get("command", {}).get("operation")
        if command.get("operation") == "apply"
        else None
    )
    target_plan = (
        command["command"]["request"]["to_plan"]
        if operation == "migrate"
        else None
    )
    target_executions = target["target_execution_bindings"]
    if not isinstance(target_executions, dict) or len(target_executions) > 1:
        raise _transport_error(
            "invalid_engine_response", "target execution bindings are outside bounds"
        )
    for plan_id, execution_target in target_executions.items():
        if not _is_sha256_id(plan_id) or target_plan is None or plan_id != target_plan:
            raise _transport_error(
                "invalid_engine_response", "target execution binding Plan is invalid"
            )
        _validate_engine_plugin_target(
            execution_target,
            require_revision=True,
            expected_message_limit=8 * 1024 * 1024,
        )
    binding_count = len(target_executions)
    if operation == "migrate":
        valid_provider_shape = target["shadow_driver"] is None and (
            target["migration_adapter"] is None
            and binding_count == 0
            or target["migration_adapter"] is not None
            and binding_count == 1
        )
    elif operation == "shadow":
        valid_provider_shape = (
            target["migration_adapter"] is None and binding_count == 0
        )
    else:
        valid_provider_shape = (
            target["migration_adapter"] is None
            and target["shadow_driver"] is None
            and binding_count == 0
        )
    if not valid_provider_shape:
        raise _transport_error(
            "invalid_engine_response",
            "evolution Engine plugin presence does not match the command",
        )
    if target["migration_adapter"] is not None:
        adapter = _require_closed_record(
            target["migration_adapter"],
            {"adapter_id", "adapter_revision", "process"},
            "migration Engine target",
        )
        request = command.get("command", {}).get("request", {})
        if (
            operation != "migrate"
            or adapter["adapter_id"] != request.get("adapter_id")
            or adapter["adapter_revision"] != request.get("adapter_revision")
        ):
            raise _transport_error(
                "invalid_engine_response",
                "migration Engine target does not match the semantic command",
            )
        _validate_engine_plugin_target(
            adapter["process"],
            require_revision=True,
            expected_message_limit=16 * 1024 * 1024,
        )
        if adapter["process"].get("revision") != adapter["adapter_revision"]:
            raise _transport_error(
                "invalid_engine_response",
                "migration Engine process revision does not match the adapter revision",
            )
    if target["shadow_driver"] is not None:
        driver = _require_closed_record(
            target["shadow_driver"],
            {"driver_id", "driver_revision", "process"},
            "shadow Engine target",
        )
        request = command.get("command", {}).get("request", {})
        if (
            operation != "shadow"
            or driver["driver_id"] != request.get("driver_id")
            or driver["driver_revision"] != request.get("driver_revision")
        ):
            raise _transport_error(
                "invalid_engine_response",
                "shadow Engine target does not match the semantic command",
            )
        _validate_engine_plugin_target(
            driver["process"],
            require_revision=True,
            expected_message_limit=16 * 1024 * 1024,
        )
        if driver["process"].get("revision") != driver["driver_revision"]:
            raise _transport_error(
                "invalid_engine_response",
                "shadow Engine process revision does not match the driver revision",
            )


def _validate_continuation_execution_claim(value: object) -> None:
    claim = _require_closed_record(value, {"claim_version", "run_id", "continuation_id", "owner", "continuation_attempt_id", "fence", "plan_id", "execution_binding_ref", "clock_observation_ref", "logical_acquired_at", "logical_expires_at", "logical_ttl"}, "Continuation execution claim")
    if claim["claim_version"] != "cymule.continuation-execution-claim/1":
        raise _transport_error("invalid_engine_response", "execution claim version is invalid")
    _require_strings(claim, {"run_id", "continuation_id", "owner", "continuation_attempt_id", "plan_id"})
    _validate_core_identity(claim["run_id"], "execution claim Run")
    _validate_core_identity(claim["owner"], "execution claim owner")
    if not _is_sha256_id(claim["continuation_id"]):
        raise _transport_error(
            "invalid_engine_response", "execution claim Continuation identity is invalid"
        )
    _validate_content_id(
        claim["continuation_attempt_id"], "execution claim Continuation Attempt"
    )
    _validate_content_id(claim["plan_id"], "execution claim Plan")
    _validate_artifact_ref(claim["execution_binding_ref"])
    if claim["execution_binding_ref"]["kind"] != "cymule.execution-binding/2":
        raise _transport_error(
            "invalid_engine_response", "execution claim binding kind is invalid"
        )
    _validate_clock_observation_ref(claim["clock_observation_ref"])
    for field in ("fence", "logical_acquired_at", "logical_expires_at", "logical_ttl"):
        _validate_epoch(claim[field])
    if (
        claim["fence"] < 1
        or claim["logical_ttl"] < 1
        or claim["logical_expires_at"]
        != claim["logical_acquired_at"] + claim["logical_ttl"]
        or claim["logical_expires_at"] > _MAX_SAFE_JSON_INTEGER
    ):
        raise _transport_error(
            "invalid_engine_response", "execution claim timing or fence is invalid"
        )


def _require_closed_subset(value: object, required: set[str], optional: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not required.issubset(value) or not set(value).issubset(required | optional):
        raise _transport_error("invalid_engine_response", f"{label} fields are not closed")
    return value


def _validate_execution_outcome(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "execution outcome is not an object")
    expected = {
        "completed": {"status", "result"},
        "suspended": {"status", "suspension"},
        "release_required": {"status", "release"},
        "reconciliation_required": {"status", "reconciliation"},
    }.get(value.get("status"))
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "execution outcome is not closed")
    status = value["status"]
    nested_key, nested_fields = {
        "completed": (
            "result",
            {"run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"},
        ),
        "suspended": (
            "suspension",
            {"run_id", "plan_id", "definition_id", "invocation_id", "site_id", "wait", "result_bind"},
        ),
        "release_required": ("release", {"run_id", "plan_id", "intent_ids"}),
        "reconciliation_required": (
            "reconciliation",
            {"run_id", "plan_id", "intent_id"},
        ),
    }[status]
    nested = value[nested_key]
    if not isinstance(nested, dict) or set(nested) != nested_fields:
        raise _transport_error("invalid_engine_response", "execution payload fields are not closed")
    _require_strings(nested, {"run_id", "plan_id"})
    _validate_core_identity(nested["run_id"], "execution Run")
    _validate_content_id(nested["plan_id"], "execution Plan")
    if status == "completed":
        _require_strings(nested, {"run_id", "plan_id", "projection_digest", "precondition_token"})
        _validate_content_id(nested["plan_id"], "execution Plan")
        if not _is_lower_hex_digest(nested["projection_digest"]):
            raise _transport_error(
                "invalid_engine_response", "execution projection digest is invalid"
            )
        if not _is_precondition_token(nested["precondition_token"]):
            raise _transport_error(
                "invalid_engine_response", "execution precondition token is invalid"
            )
        _require_sorted_unique_string_list(
            nested["effects"], "execution effects", content_ids=True
        )
    elif status == "suspended":
        _require_strings(
            nested, {"run_id", "plan_id", "definition_id", "invocation_id", "site_id"}
        )
        if nested["result_bind"] is not None and not _is_nonempty_string(nested["result_bind"]):
            raise _transport_error("invalid_engine_response", "wait result binding is invalid")
        _validate_wait_spec(nested["wait"])
    elif status == "release_required":
        _require_strings(nested, {"run_id", "plan_id"})
        if (
            not isinstance(nested["intent_ids"], list)
            or not nested["intent_ids"]
        ):
            raise _transport_error("invalid_engine_response", "effect release intents are invalid")
        _require_sorted_unique_string_list(
            nested["intent_ids"], "effect release intents", content_ids=True
        )
    else:
        _require_strings(nested, {"run_id", "plan_id", "intent_id"})
        _validate_content_id(nested["intent_id"], "effect reconciliation intent")


def _validate_durable_response(value: object) -> None:
    response = _validate_tagged_result(value, "type", {
        "run_boundary": {"type", "boundary"}, "wait_activated": {"type", "receipt"},
        "run_cancelled": {"type", "receipt"}, "effect_resolved": {"type", "receipt"},
        "run_index_page": {"type", "page"},
        "run_current": {
            "type", "observed_revision", "source_root", "current",
        },
        "run_wait_page": {"type", "run_id", "page"},
        "run_effect_page": {"type", "run_id", "page"},
        "run_occurrence_page": {"type", "run_id", "page"},
        "run_attempt_page": {"type", "run_id", "page"},
        "run_item": {
            "type", "run_id", "observed_revision", "source_root", "item",
        },
    })
    kind = response["type"]
    if kind == "run_boundary":
        boundary = _validate_tagged_result(response["boundary"], "status", {
            "suspended": {"status", "wait_id"},
            "reconciliation_required": {"status", "intent_id"},
            "effect_unavailable": {"status", "intent_id"},
            "effect_not_applied": {"status", "intent_id"},
            "release_required": {"status", "intent_ids"},
            "completed": {"status", "result"},
            "failed": {"status", "failure"},
            "cancelled": {"status", "reason"},
        })
        if boundary["status"] == "completed":
            _validate_execution_result(boundary["result"])
        elif boundary["status"] == "failed":
            _validate_run_failure(boundary["failure"])
        elif boundary["status"] == "cancelled":
            _validate_artifact_ref(boundary["reason"])
        elif boundary["status"] == "release_required":
            if not boundary["intent_ids"]:
                raise _transport_error(
                    "invalid_engine_response", "effect intent set is empty"
                )
            _require_sorted_unique_string_list(
                boundary["intent_ids"], "effect intents", content_ids=True
            )
        else:
            _require_strings(boundary, {"wait_id"} if boundary["status"] == "suspended" else {"intent_id"})
            if boundary["status"] != "suspended":
                _validate_content_id(boundary["intent_id"], "effect intent")
    elif kind == "run_cancelled":
        _validate_run_cancellation_receipt(response["receipt"])
    elif kind == "effect_resolved":
        _validate_effect_resolution_receipt(response["receipt"])
    elif kind == "wait_activated":
        _validate_wait_activation_receipt(response["receipt"])
    elif kind == "run_index_page":
        _validate_durable_query_page(
            response["page"], "run_index", None, _validate_run_index_summary
        )
    elif kind == "run_current":
        _validate_durable_query_source(
            response["observed_revision"], response["source_root"]
        )
        if response["current"] is not None:
            _validate_durable_run_current(response["current"])
    elif kind in {
        "run_wait_page", "run_effect_page", "run_occurrence_page",
        "run_attempt_page",
    }:
        query_kind, validator = {
            "run_wait_page": ("run_waits", _validate_wait_summary),
            "run_effect_page": ("run_effects", _validate_effect_summary),
            "run_occurrence_page": (
                "run_occurrences", _validate_occurrence_summary
            ),
            "run_attempt_page": ("run_attempts", _validate_attempt_summary),
        }[kind]
        _validate_core_identity(response["run_id"], "durable Run page owner")
        _validate_durable_query_page(
            response["page"], query_kind, response["run_id"], validator
        )
    elif kind == "run_item":
        _validate_core_identity(response["run_id"], "durable Run-item owner")
        _validate_durable_query_source(
            response["observed_revision"], response["source_root"]
        )
        if response["item"] is not None:
            _validate_durable_run_item(response["item"])
            if _durable_run_item_owner(response["item"]) != response["run_id"]:
                raise _transport_error(
                    "invalid_engine_response", "exact Run item escaped its owner"
                )
    query_limit = (
        13 * 1024 * 1024
        if kind == "run_item"
        else 1024 * 1024
        if kind in {
            "run_index_page", "run_current", "run_wait_page", "run_effect_page",
            "run_occurrence_page", "run_attempt_page",
        }
        else None
    )
    if query_limit is not None and _json_size(response) > query_limit:
        raise _transport_error(
            "invalid_engine_response", "durable query response is oversized"
        )


def _validate_wait_activation_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value,
        {"receipt_version", "activation", "applied_wait_ids", "ready_run_ids"},
        "wait activation receipt",
    )
    if receipt["receipt_version"] != "cymule.wait-activation-receipt/3":
        raise _transport_error(
            "invalid_engine_response", "wait activation receipt version is invalid"
        )
    _validate_wait_activation_response(receipt["activation"])
    _require_sorted_unique_string_list(
        receipt["applied_wait_ids"], "applied wait identities"
    )
    for wait_id in receipt["applied_wait_ids"]:
        _validate_content_id(wait_id, "applied wait")
    _require_sorted_unique_string_list(receipt["ready_run_ids"], "ready Run identities")
    for run_id in receipt["ready_run_ids"]:
        _validate_core_identity(run_id, "ready Run")
    if not set(receipt["applied_wait_ids"]).issubset(
        receipt["activation"]["wait_ids"]
    ):
        raise _transport_error(
            "invalid_engine_response",
            "wait activation receipt applied targets escape the selected set",
        )
    if not receipt["applied_wait_ids"] and receipt["ready_run_ids"]:
        raise _transport_error(
            "invalid_engine_response",
            "terminal non-winner activation returned ready Runs",
        )


def _validate_run_cancellation_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value,
        {"receipt_version", "command", "boundary", "receipt_id"},
        "Run cancellation receipt",
    )
    if receipt["receipt_version"] != "cymule.run-cancellation-receipt/1":
        raise _transport_error(
            "invalid_engine_response", "Run cancellation receipt version is invalid"
        )
    _validate_cancellation_command(receipt["command"])
    if not _is_lower_hex_digest(receipt["receipt_id"]):
        raise _transport_error(
            "invalid_engine_response", "Run cancellation receipt identity is invalid"
        )
    boundary = _require_closed_record(
        receipt["boundary"], {"status", "reason"}, "cancelled Run boundary"
    )
    if boundary["status"] != "cancelled":
        raise _transport_error(
            "invalid_engine_response", "Run cancellation boundary is invalid"
        )
    _validate_artifact_ref(boundary["reason"])
    if boundary["reason"]["kind"] != "cymule.cancellation-reason/1":
        raise _transport_error(
            "invalid_engine_response", "Run cancellation reason kind is invalid"
        )


def _validate_cancellation_command(value: object) -> None:
    command = _require_closed_record(
        value, {"cancellation_id", "run_id", "reason"}, "Run cancellation command"
    )
    _validate_core_identity(command["cancellation_id"], "cancellation")
    _validate_core_identity(command["run_id"], "cancelled Run")


def _validate_effect_result(
    value: object, *, applied: bool, label: str
) -> None:
    """Validate the one terminal Effect-result relationship shared by all DTOs."""
    if not applied:
        if value is not None:
            raise _transport_error(
                "invalid_engine_response", f"non-applied {label} has a result"
            )
        return
    if value is None:
        raise _transport_error(
            "invalid_engine_response", f"applied {label} has no result"
        )
    _validate_artifact_ref(value)
    if not isinstance(value, dict) or value.get("kind") != "cymule.effect-result/1":
        raise _transport_error(
            "invalid_engine_response", f"{label} result kind is invalid"
        )


def _validate_effect_resolution_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value,
        {
            "receipt_version", "command", "actual_resolution", "actual_value",
            "result", "receipt_id",
        },
        "effect resolution receipt",
    )
    if receipt["receipt_version"] != "cymule.effect-resolution-receipt/1":
        raise _transport_error(
            "invalid_engine_response", "effect resolution receipt version is invalid"
        )
    _validate_effect_resolution_command(receipt["command"])
    if (
        receipt["actual_resolution"]
        not in {"resolved_applied", "resolved_not_applied"}
        or not _is_lower_hex_digest(receipt["receipt_id"])
    ):
        raise _transport_error(
            "invalid_engine_response", "effect resolution receipt is invalid"
        )
    _validate_effect_result(
        receipt["result"],
        applied=receipt["actual_resolution"] == "resolved_applied",
        label="Effect resolution",
    )
    if (
        receipt["actual_resolution"] == "resolved_not_applied"
        and receipt["actual_value"] is not None
    ):
        raise _transport_error(
            "invalid_engine_response",
            "effect resolution value and result presence disagree",
        )


def _validate_effect_resolution_command(value: object) -> None:
    command = _require_closed_record(
        value,
        {
            "resolution_id", "run_id", "intent_id", "execution_binding",
            "occurrence_binding", "claim_owner", "claim_epoch", "resolution", "value",
        },
        "effect resolution command",
    )
    for field in (
        "resolution_id", "run_id", "occurrence_binding", "claim_owner"
    ):
        _validate_core_identity(command[field], f"effect resolution {field}")
    _validate_content_id(command["intent_id"], "effect resolution intent")
    _validate_content_id(
        command["occurrence_binding"], "effect resolution occurrence binding"
    )
    _validate_artifact_ref(command["execution_binding"])
    if (
        command["execution_binding"]["kind"] != "cymule.execution-binding/2"
        or not _is_positive_safe_integer(command["claim_epoch"])
        or command["resolution"] not in {"resolved_applied", "resolved_not_applied"}
        or command["resolution"] == "resolved_not_applied"
        and command["value"] is not None
    ):
        raise _transport_error(
            "invalid_engine_response", "effect resolution command is invalid"
        )


def _validate_execution_result(value: object) -> None:
    result = _require_closed_record(value, {"run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"}, "execution result")
    _require_strings(result, {"run_id", "plan_id", "projection_digest", "precondition_token"})
    _validate_core_identity(result["run_id"], "execution Run")
    _validate_content_id(result["plan_id"], "execution Plan")
    if not _is_lower_hex_digest(result["projection_digest"]):
        raise _transport_error(
            "invalid_engine_response", "execution projection digest is invalid"
        )
    if not _is_precondition_token(result["precondition_token"]):
        raise _transport_error(
            "invalid_engine_response", "execution precondition token is invalid"
        )
    _require_sorted_unique_string_list(
        result["effects"], "execution effects", content_ids=True
    )


def _validate_run_failure(value: object) -> None:
    failure = _require_closed_record(value, {"class", "code", "detail"}, "Run failure")
    if failure["class"] not in {"declared_failure", "runtime_defect", "substrate"}:
        raise _transport_error("invalid_engine_response", "Run failure class is invalid")
    code = failure["code"]
    if not isinstance(code, str) or not code or not code.isascii() or len(code) > 200 or not code[0].islower() or not all(
        character.islower() or character.isdigit() or character == "_" for character in code
    ):
        raise _transport_error("invalid_engine_response", "Run failure code is invalid")
    _validate_artifact_ref(failure["detail"])


def _json_size(value: object) -> int:
    return len(_encode_exact_json(value).encode("utf-8"))


def _encode_exact_json(value: object, depth: int = 0) -> str:
    if depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON nesting exceeds the fixed depth limit")
    if isinstance(value, _ExactFraction):
        return str(value.exact)
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return json.dumps(value, allow_nan=False)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(
            _encode_exact_json(member, depth + 1) for member in value
        ) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(
            json.dumps(key, ensure_ascii=False)
            + ":"
            + _encode_exact_json(value[key], depth + 1)
            for key in sorted(value)
        ) + "}"
    raise TypeError("value is outside the JSON data model")


def _unbox_exact_fractions(value: object) -> object:
    if isinstance(value, _ExactFraction):
        return float(value)
    if isinstance(value, list):
        return [_unbox_exact_fractions(member) for member in value]
    if isinstance(value, dict):
        return {
            key: _unbox_exact_fractions(member) for key, member in value.items()
        }
    return value


def _durable_page_key_hash(canonical_key: str) -> str:
    def frame(value: bytes) -> bytes:
        return len(value).to_bytes(8, "big") + value

    field_count = (1).to_bytes(8, "big")
    preimage = b"".join(
        (
            frame(b"cymule.authenticated-collection-preimage/1"),
            frame(b"cymule.authenticated-map-key/1"),
            frame(field_count),
            frame(canonical_key.encode("utf-8")),
        )
    )
    return hashlib.sha256(preimage).hexdigest()


def _validate_durable_query_source(
    observed_revision: object, source_root: object
) -> None:
    _validate_content_id(observed_revision, "durable query observed revision")
    if not _is_lower_hex_digest(source_root):
        raise _transport_error(
            "invalid_engine_response", "durable query source root is invalid"
        )


def _validate_durable_page_cursor(value: object) -> None:
    cursor = _require_closed_record(
        value,
        {"query_kind", "run_id", "source_revision", "source_root", "position"},
        "durable page cursor",
    )
    query_kind = cursor["query_kind"]
    if query_kind not in {
        "run_index", "run_waits", "run_effects", "run_occurrences", "run_attempts"
    }:
        raise _transport_error(
            "invalid_engine_response", "durable page cursor query kind is invalid"
        )
    if query_kind == "run_index":
        if cursor["run_id"] is not None:
            raise _transport_error(
                "invalid_engine_response", "Run-index cursor has a Run owner"
            )
    else:
        _validate_core_identity(cursor["run_id"], "durable page cursor Run")
    _validate_durable_query_source(cursor["source_revision"], cursor["source_root"])
    position = _require_closed_record(
        cursor["position"], {"canonical_key", "key_hash"}, "durable page position"
    )
    canonical_key = position["canonical_key"]
    if query_kind == "run_index":
        _validate_core_identity(canonical_key, "durable Run-index key")
    else:
        _validate_content_id(canonical_key, "durable page key")
    if (
        not _is_lower_hex_digest(position["key_hash"])
        or position["key_hash"] != _durable_page_key_hash(canonical_key)
    ):
        raise _transport_error(
            "invalid_engine_response", "durable page position is invalid"
        )


def _durable_summary_key(query_kind: object, item: object) -> str | None:
    if not isinstance(item, dict):
        return None
    field = {
        "run_index": "run_id",
        "run_waits": "wait_id",
        "run_effects": "intent_id",
        "run_occurrences": "occurrence_id",
        "run_attempts": "attempt_id",
    }.get(query_kind)
    key = item.get(field) if field is not None else None
    return key if isinstance(key, str) else None


def _validate_durable_query_page(
    value: object,
    query_kind: DurablePageQueryKind,
    run_id: str | None,
    validate_item: Callable[[object], None],
) -> None:
    page = _require_closed_record(
        value,
        {"observed_revision", "source_root", "items", "next_cursor"},
        "durable query page",
    )
    _validate_durable_query_source(page["observed_revision"], page["source_root"])
    if not isinstance(page["items"], list) or len(page["items"]) > 256:
        raise _transport_error(
            "invalid_engine_response", "durable query page item count is invalid"
        )
    previous: tuple[str, str] | None = None
    for item in page["items"]:
        validate_item(item)
        if run_id is not None and (
            not isinstance(item, dict) or item.get("run_id") != run_id
        ):
            raise _transport_error(
                "invalid_engine_response", "durable query item escaped its Run"
            )
        if _json_size(item) > 32 * 1024:
            raise _transport_error(
                "invalid_engine_response", "durable query summary is oversized"
            )
        key = _durable_summary_key(query_kind, item)
        if key is None:
            raise _transport_error(
                "invalid_engine_response", "durable query summary key is invalid"
            )
        position = (_durable_page_key_hash(key), key)
        if previous is not None and previous >= position:
            raise _transport_error(
                "invalid_engine_response",
                "durable query items are not in authenticated key order",
            )
        previous = position
    if page["next_cursor"] is not None:
        _validate_durable_page_cursor(page["next_cursor"])
        cursor = page["next_cursor"]
        if (
            cursor["query_kind"] != query_kind
            or cursor["run_id"] != run_id
            or cursor["source_revision"] != page["observed_revision"]
            or cursor["source_root"] != page["source_root"]
            or previous is None
            or (
                cursor["position"]["key_hash"],
                cursor["position"]["canonical_key"],
            )
            != previous
        ):
            raise _transport_error(
                "invalid_engine_response",
                "durable next cursor does not bind the terminal item and source",
            )


def _validate_continuation_execution_axes(
    continuation_status: object, execution_status: object
) -> None:
    expected = {
        "ready": "active",
        "waiting": "active",
        "running": "active",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
    }.get(continuation_status)
    _validate_run_execution_status(execution_status)
    if expected is None or execution_status["status"] != expected:
        raise _transport_error(
            "invalid_engine_response",
            "Continuation and execution summary axes disagree",
        )


def _validate_world_settlement(value: object, execution_status: dict[str, Any]) -> None:
    if value not in {"settled", "pending", "unknown", "governance_required"}:
        raise _transport_error(
            "invalid_engine_response", "world settlement is invalid"
        )
    if execution_status["status"] == "completed" and value != "settled":
        raise _transport_error(
            "invalid_engine_response", "completed Run retains unsettled Effects"
        )


def _validate_run_index_summary(value: object) -> None:
    summary = _require_closed_record(
        value,
        {"run_id", "continuation_status", "execution_status", "world_settlement"},
        "Run-index summary",
    )
    _validate_core_identity(summary["run_id"], "Run-index summary")
    _validate_continuation_execution_axes(
        summary["continuation_status"], summary["execution_status"]
    )
    _validate_world_settlement(
        summary["world_settlement"], summary["execution_status"]
    )


def _validate_durable_run_current(value: object) -> None:
    current = _require_closed_record(
        value,
        {
            "run_id", "plan_id", "execution_binding", "continuation_status",
            "epoch", "execution_fence", "result", "execution_status",
            "world_settlement",
        },
        "Run-current projection",
    )
    _validate_core_identity(current["run_id"], "Run-current owner")
    _validate_content_id(current["plan_id"], "Run-current Plan")
    _validate_artifact_ref(current["execution_binding"])
    if current["execution_binding"]["kind"] != "cymule.execution-binding/2":
        raise _transport_error(
            "invalid_engine_response", "Run-current binding is invalid"
        )
    _validate_epoch(current["epoch"])
    _validate_epoch(current["execution_fence"])
    _validate_continuation_execution_axes(
        current["continuation_status"], current["execution_status"]
    )
    _validate_world_settlement(
        current["world_settlement"], current["execution_status"]
    )
    if current["execution_status"]["status"] == "completed":
        _validate_artifact_ref(current["result"])
    elif current["result"] is not None:
        raise _transport_error(
            "invalid_engine_response", "non-completed Run carries a terminal result"
        )
    if _json_size(current) > 32 * 1024:
        raise _transport_error(
            "invalid_engine_response", "Run-current projection is oversized"
        )


def _validate_wait_summary(value: object) -> None:
    summary = _require_closed_record(
        value, {"wait_id", "run_id", "state", "result"}, "wait summary"
    )
    _validate_content_id(summary["wait_id"], "wait summary identity")
    _validate_core_identity(summary["run_id"], "wait summary Run")
    if summary["state"] not in {"pending", "completed", "cancelled"}:
        raise _transport_error(
            "invalid_engine_response", "wait summary state is invalid"
        )
    if summary["state"] == "completed":
        _validate_artifact_ref(summary["result"])
    elif summary["result"] is not None:
        raise _transport_error(
            "invalid_engine_response", "non-completed wait summary has a result"
        )


def _validate_effect_summary(value: object) -> None:
    summary = _require_closed_record(
        value,
        {
            "intent_id", "run_id", "state", "execution_availability",
            "reconciliation", "result",
        },
        "Effect summary",
    )
    _validate_content_id(summary["intent_id"], "Effect summary intent")
    _validate_core_identity(summary["run_id"], "Effect summary Run")
    if summary["state"] not in {
        "pending", "claimed", "applied", "not_applied", "unknown",
        "cancelled_before_release",
    } or summary["execution_availability"] not in {"available", "unavailable"}:
        raise _transport_error(
            "invalid_engine_response", "Effect summary state is invalid"
        )
    reconciliation_matches = {
        "pending": {"not_required"},
        "claimed": {"not_required"},
        "applied": {"not_required", "resolved"},
        "not_applied": {"not_required", "resolved"},
        "unknown": {"pending", "governance_required"},
        "cancelled_before_release": {"resolved"},
    }[summary["state"]]
    if (
        summary["reconciliation"] not in reconciliation_matches
        or summary["state"] in {"pending", "claimed"}
        and summary["execution_availability"] != "available"
    ):
        raise _transport_error(
            "invalid_engine_response", "Effect summary lifecycle is inconsistent"
        )
    _validate_effect_result(
        summary["result"],
        applied=summary["state"] == "applied",
        label="Effect summary",
    )


def _validate_occurrence_summary(value: object) -> None:
    summary = _require_closed_record(
        value,
        {"occurrence_id", "run_id", "state", "outcome"},
        "component occurrence summary",
    )
    _validate_content_id(summary["occurrence_id"], "component occurrence summary")
    _validate_core_identity(summary["run_id"], "component occurrence summary Run")
    if summary["state"] not in {"pending", "completed"}:
        raise _transport_error(
            "invalid_engine_response", "component occurrence summary state is invalid"
        )
    if summary["state"] == "completed":
        _validate_component_outcome(summary["outcome"])
    elif summary["outcome"] is not None:
        raise _transport_error(
            "invalid_engine_response", "pending occurrence summary has an outcome"
        )


def _validate_attempt_summary(value: object) -> None:
    summary = _require_closed_record(
        value,
        {
            "attempt_id", "occurrence_id", "run_id", "attempt_ordinal", "state",
            "outcome",
        },
        "operation Attempt summary",
    )
    _validate_content_id(summary["attempt_id"], "operation Attempt summary")
    _validate_content_id(
        summary["occurrence_id"], "operation Attempt summary occurrence"
    )
    _validate_core_identity(summary["run_id"], "operation Attempt summary Run")
    if (
        not _is_positive_safe_integer_at_most(
            summary["attempt_ordinal"], 9_007_199_254_740_991
        )
        or summary["state"] not in {"running", "completed", "superseded"}
    ):
        raise _transport_error(
            "invalid_engine_response", "operation Attempt summary is invalid"
        )
    if summary["state"] == "completed":
        _validate_component_outcome(summary["outcome"])
    elif summary["outcome"] is not None:
        raise _transport_error(
            "invalid_engine_response", "non-completed Attempt summary has an outcome"
        )


def _validate_durable_run_item_selector(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error(
            "invalid_engine_response", "exact Run-item selector is invalid"
        )
    field = {
        "wait": "wait_id",
        "effect": "intent_id",
        "occurrence": "occurrence_id",
        "attempt": "attempt_id",
    }.get(value.get("kind"))
    if field is None or set(value) != {"kind", field}:
        raise _transport_error(
            "invalid_engine_response", "exact Run-item selector is not closed"
        )
    _validate_content_id(value[field], "exact Run-item selector identity")


def _validate_durable_run_item(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error(
            "invalid_engine_response", "exact durable Run item is invalid"
        )
    field = {
        "wait": "wait",
        "effect": "effect",
        "occurrence": "occurrence",
        "attempt": "attempt",
    }.get(value.get("kind"))
    if field is None or set(value) != {"kind", field}:
        raise _transport_error(
            "invalid_engine_response", "exact durable Run item is not closed"
        )
    validator = {
        "wait": _validate_wait_condition,
        "effect": _validate_effect_dispatch,
        "occurrence": _validate_component_occurrence,
        "attempt": _validate_operation_attempt,
    }[value["kind"]]
    validator(value[field])
    if _json_size(value[field]) > 12 * 1024 * 1024:
        raise _transport_error(
            "invalid_engine_response", "exact durable Run item is oversized"
        )


def _durable_run_item_owner(value: dict[str, Any]) -> str:
    field = {
        "wait": "wait",
        "effect": "effect",
        "occurrence": "occurrence",
        "attempt": "attempt",
    }[value["kind"]]
    return value[field]["run_id"]


def _validate_wait_condition(value: object) -> None:
    wait = _require_closed_record(
        value,
        {"wait_id", "run_id", "kind", "consume_once", "owner", "state", "result"},
        "wait condition",
    )
    _validate_content_id(wait["wait_id"], "wait condition identity")
    _validate_core_identity(wait["run_id"], "wait condition Run")
    if not isinstance(wait["consume_once"], bool) or wait["state"] not in {
        "pending", "completed", "cancelled"
    }:
        raise _transport_error(
            "invalid_engine_response", "wait condition state is invalid"
        )
    _validate_durable_wait_kind(wait["kind"])
    owner = _require_closed_record(
        wait["owner"],
        {
            "invocation_id", "definition_id", "site_id", "region_path",
            "step_index", "bind",
        },
        "wait owner",
    )
    _require_strings(owner, {"invocation_id", "definition_id", "site_id"})
    _validate_index_list(owner["region_path"], "wait owner Region path")
    _validate_epoch(owner["step_index"])
    if owner["bind"] is not None and not _is_nonempty_string(owner["bind"]):
        raise _transport_error(
            "invalid_engine_response", "wait owner bind is invalid"
        )
    if wait["state"] == "completed":
        _validate_artifact_ref(wait["result"])
    elif wait["result"] is not None:
        raise _transport_error(
            "invalid_engine_response", "non-completed wait has a result"
        )


def _validate_effect_dispatch(value: object) -> None:
    effect = _require_closed_record(
        value,
        {
            "intent_id", "run_id", "origin_plan_id", "operation", "input",
            "execution_binding", "occurrence_binding", "execution_availability",
            "state", "reconciliation", "claim_epoch", "claim_owner", "result",
        },
        "effect dispatch",
    )
    _validate_content_id(effect["intent_id"], "Effect intent")
    _validate_core_identity(effect["run_id"], "Effect Run")
    _validate_content_id(effect["origin_plan_id"], "Effect origin Plan")
    _validate_content_id(effect["occurrence_binding"], "Effect occurrence binding")
    _require_strings(effect, {"operation"})
    _validate_artifact_ref(effect["input"])
    _validate_artifact_ref(effect["execution_binding"])
    if effect["execution_binding"]["kind"] != "cymule.execution-binding/2":
        raise _transport_error(
            "invalid_engine_response", "Effect execution binding is invalid"
        )
    _validate_epoch(effect["claim_epoch"])
    if effect["claim_owner"] is not None and not _is_nonempty_string(
        effect["claim_owner"]
    ):
        raise _transport_error(
            "invalid_engine_response", "Effect claim owner is invalid"
        )
    if effect["execution_availability"] not in {"available", "unavailable"} or (
        effect["state"] not in {
            "pending", "claimed", "applied", "not_applied", "unknown",
            "cancelled_before_release",
        }
    ):
        raise _transport_error(
            "invalid_engine_response", "Effect lifecycle is invalid"
        )
    if effect["reconciliation"] not in {
        "pending": {"not_required"},
        "claimed": {"not_required"},
        "applied": {"not_required", "resolved"},
        "not_applied": {"not_required", "resolved"},
        "unknown": {"pending", "governance_required"},
        "cancelled_before_release": {"resolved"},
    }[effect["state"]]:
        raise _transport_error(
            "invalid_engine_response", "Effect reconciliation is invalid"
        )
    claimed = (
        _is_positive_safe_integer_at_most(
            effect["claim_epoch"], 9_007_199_254_740_991
        )
        and _is_nonempty_string(effect["claim_owner"])
    )
    lifecycle_matches = (
        effect["state"] == "pending"
        and effect["execution_availability"] == "available"
        and effect["claim_epoch"] == 0
        and effect["claim_owner"] is None
        or effect["state"] == "claimed"
        and effect["execution_availability"] == "available"
        and claimed
        or effect["state"] == "unknown"
        and claimed
        or effect["state"] == "applied"
        and claimed
        or effect["state"] == "not_applied"
        and claimed
        or effect["state"] == "cancelled_before_release"
        and effect["claim_epoch"] == 0
        and effect["claim_owner"] is None
    )
    if not lifecycle_matches:
        raise _transport_error(
            "invalid_engine_response", "Effect dispatch lifecycle is invalid"
        )
    _validate_effect_result(
        effect["result"],
        applied=effect["state"] == "applied",
        label="Effect dispatch",
    )


def _validate_component_occurrence(value: object) -> None:
    occurrence = _require_closed_record(
        value,
        {
            "occurrence_version", "occurrence_id", "run_id", "plan_id",
            "binding_context", "invocation_id", "invocation_path", "definition_id",
            "region_path", "site_id", "step_index", "component", "input", "outcome",
            "occurrence_binding", "implementation_revision", "attempt_count",
            "latest_attempt_id", "continuation_digest",
            "state",
        },
        "component occurrence",
    )
    if occurrence["occurrence_version"] != "cymule.component-occurrence/4":
        raise _transport_error(
            "invalid_engine_response", "component occurrence version is invalid"
        )
    _validate_content_id(occurrence["occurrence_id"], "component occurrence")
    _validate_content_id(
        occurrence["occurrence_binding"], "component occurrence binding"
    )
    _validate_content_id(
        occurrence["latest_attempt_id"], "component occurrence latest Attempt"
    )
    _validate_core_identity(occurrence["run_id"], "component occurrence Run")
    _require_strings(
        occurrence,
        {
            "plan_id", "binding_context", "invocation_id", "definition_id",
            "site_id", "component", "implementation_revision",
        },
    )
    _validate_artifact_ref(occurrence["input"])
    _validate_index_list(occurrence["region_path"], "occurrence Region path")
    _validate_epoch(occurrence["step_index"])
    if not _is_positive_safe_integer_at_most(
        occurrence["attempt_count"], 9_007_199_254_740_991
    ):
        raise _transport_error(
            "invalid_engine_response", "component occurrence Attempt count is invalid"
        )
    if not isinstance(occurrence["invocation_path"], list):
        raise _transport_error(
            "invalid_engine_response", "occurrence invocation path is invalid"
        )
    for segment in occurrence["invocation_path"]:
        edge = _require_closed_record(
            segment, {"site_id", "region_path", "scope_id"}, "invocation edge"
        )
        _require_strings(edge, {"site_id", "scope_id"})
        _validate_index_list(edge["region_path"], "invocation edge Region path")
    if occurrence["state"] == "pending":
        if (
            occurrence["outcome"] is not None
            or occurrence["continuation_digest"] is not None
        ):
            raise _transport_error(
                "invalid_engine_response", "pending occurrence has terminal data"
            )
    elif occurrence["state"] == "completed":
        _validate_component_outcome(occurrence["outcome"])
        if not _is_lower_hex_digest(occurrence["continuation_digest"]):
            raise _transport_error(
                "invalid_engine_response", "completed occurrence digest is invalid"
            )
    else:
        raise _transport_error(
            "invalid_engine_response", "component occurrence state is invalid"
        )


def _validate_operation_attempt(value: object) -> None:
    attempt = _require_closed_record(
        value,
        {
            "attempt_version", "attempt_id", "occurrence_id", "run_id",
            "attempt_ordinal", "previous_attempt_id", "continuation_attempt_id",
            "execution_claim_owner", "execution_claim_fence",
            "operation_occurrence_binding", "transport_request_id", "state", "outcome",
        },
        "operation Attempt",
    )
    if attempt["attempt_version"] != "cymule.operation-attempt/2":
        raise _transport_error(
            "invalid_engine_response", "operation Attempt version is invalid"
        )
    for field, label in (
        ("attempt_id", "operation Attempt"),
        ("occurrence_id", "operation occurrence"),
        ("continuation_attempt_id", "Continuation Attempt"),
        ("operation_occurrence_binding", "operation occurrence binding"),
        ("transport_request_id", "transport request"),
    ):
        _validate_content_id(attempt[field], label)
    _validate_core_identity(attempt["run_id"], "operation Attempt Run")
    _validate_core_identity(
        attempt["execution_claim_owner"], "operation Attempt execution owner"
    )
    if not _is_positive_safe_integer_at_most(
        attempt["attempt_ordinal"], 9_007_199_254_740_991
    ) or not _is_positive_safe_integer_at_most(
        attempt["execution_claim_fence"], 9_007_199_254_740_991
    ):
        raise _transport_error(
            "invalid_engine_response", "operation Attempt fence is invalid"
        )
    previous_attempt_id = attempt["previous_attempt_id"]
    if previous_attempt_id is not None:
        _validate_content_id(previous_attempt_id, "previous operation Attempt")
    if (attempt["attempt_ordinal"] == 1) != (previous_attempt_id is None):
        raise _transport_error(
            "invalid_engine_response", "operation Attempt predecessor is invalid"
        )
    if attempt["state"] == "completed":
        _validate_component_outcome(attempt["outcome"])
    elif attempt["state"] in {"running", "superseded"}:
        if attempt["outcome"] is not None:
            raise _transport_error(
                "invalid_engine_response", "non-completed Attempt has an outcome"
            )
    else:
        raise _transport_error(
            "invalid_engine_response", "operation Attempt state is invalid"
        )


def _validate_component_outcome(value: object) -> None:
    outcome = _validate_tagged_result(
        value,
        "outcome",
        {
            "succeeded": {"outcome", "output"},
            "expected_failure": {"outcome", "code", "detail"},
        },
    )
    if outcome["outcome"] == "succeeded":
        _validate_artifact_ref(outcome["output"])
        return
    code = outcome["code"]
    if (
        not isinstance(code, str)
        or re.fullmatch(r"[a-z][a-z0-9_]{0,199}", code) is None
    ):
        raise _transport_error(
            "invalid_engine_response", "expected component failure code is invalid"
        )
    _validate_artifact_ref(outcome["detail"])


def _validate_durable_wait_kind(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "durable wait kind is invalid")
    expected = {
        "signal": {"kind", "key"},
        "timer": {"kind", "timer_id"},
        "input": {"kind", "correlation", "schema"},
    }.get(value.get("kind"))
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "durable wait kind is not closed")
    identity = value.get({"signal": "key", "timer": "timer_id", "input": "correlation"}[value["kind"]])
    if not isinstance(identity, str) or not identity:
        raise _transport_error("invalid_engine_response", "durable wait kind identity is invalid")
    if value["kind"] == "input" and not isinstance(value["schema"], (bool, dict)):
        raise _transport_error("invalid_engine_response", "durable input wait schema is invalid")


def _validate_run_execution_status(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "Run execution status is invalid")
    expected = {
        "active": {"status"},
        "completed": {"status"},
        "failed": {"status", "failure"},
        "cancelled": {"status", "reason"},
    }.get(value.get("status"))
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "Run execution status is not closed")
    if value["status"] == "failed": _validate_run_failure(value["failure"])
    if value["status"] == "cancelled": _validate_artifact_ref(value["reason"])


def _validate_live_evolution_outcome(value: object) -> None:
    response = _validate_tagged_result(value, "result", {
        "definition_published": {"result", "revision"}, "template_registered": {"result", "linked"},
        "publication_applied": {"result", "receipt"}, "patch_applied": {"result", "edge"},
        "applied": {"result"}, "occurrence_selected": {"result", "pin"},
        "migrated": {"result", "receipt"}, "restart_authorized": {"result", "receipt"},
        "shadow_recorded": {"result", "comparison"}, "gate_applied": {"result", "transition"},
    })
    kind = response["result"]
    if kind == "definition_published":
        _validate_subflow_revision(response["revision"])
    elif kind == "template_registered":
        _validate_linked_plan(response["linked"])
    elif kind == "publication_applied":
        _validate_publication_receipt(response["receipt"])
    elif kind == "patch_applied":
        _validate_plan_edge(response["edge"])
    elif kind == "occurrence_selected":
        _validate_occurrence_pin(response["pin"])
    elif kind == "migrated":
        _validate_migration_receipt(response["receipt"])
    elif kind == "restart_authorized":
        _validate_restart_receipt(response["receipt"])
    elif kind == "shadow_recorded":
        _validate_shadow_comparison(response["comparison"])
    elif kind == "gate_applied":
        _validate_rollout_transition(response["transition"])


_EVOLUTION_STATE_FAMILIES = (
    "definition_current",
    "definition_compatibility_current",
    "definition_record",
    "dependency_current",
    "template_current",
    "link_record",
    "plan_record",
    "edge_record",
    "rollout_current",
    "rollout_evidence_current",
    "rollout_decision",
    "occurrence_current",
    "selection_current",
    "migration_record",
    "restart_record",
    "shadow_record",
    "shadow_subject_current",
    "observation_record",
    "observation_occurrence_current",
    "evidence_current",
    "decision_transition_current",
    "transition_record",
)


def _validate_evolution_commit(value: object) -> None:
    commit = _require_closed_record(
        value,
        {"observed_revision", "committed_revision", "receipt"},
        "evolution commit",
    )
    observed = commit["observed_revision"]
    committed = commit["committed_revision"]
    if (
        not _is_sha256_id(observed)
        or (committed is not None and not _is_sha256_id(committed))
        or (committed is not None and committed != observed)
    ):
        raise _transport_error(
            "invalid_engine_response", "evolution commit revision is invalid"
        )
    _validate_evolution_persistence_receipt(commit["receipt"])


def _validate_evolution_persistence_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value,
        {
            "receipt_version",
            "receipt_id",
            "command",
            "parent_current_id",
            "source_witness_id",
            "outcome",
            "mutations",
            "mutation_id",
        },
        "evolution persistence receipt",
    )
    parent = receipt["parent_current_id"]
    source_witness = receipt["source_witness_id"]
    if (
        receipt["receipt_version"] != "cymule.evolution-persistence-receipt/4"
        or not _is_sha256_id(receipt["receipt_id"])
        or (parent is not None and not _is_sha256_id(parent))
        or (source_witness is not None and not _is_sha256_id(source_witness))
        or not _is_sha256_id(receipt["mutation_id"])
    ):
        raise _transport_error(
            "invalid_engine_response",
            "evolution persistence receipt identity is invalid",
        )
    command = _validate_evolution_persistence_command(receipt["command"])
    _validate_live_evolution_outcome(receipt["outcome"])
    semantic = command["command"]
    consumes_source = semantic["operation"] == "apply" and semantic["command"][
        "operation"
    ] in {"migrate", "restart_under_new_plan"}
    if consumes_source != (source_witness is not None):
        raise _transport_error(
            "invalid_engine_response",
            "evolution source witness does not match its semantic command",
        )
    if not _live_evolution_command_matches_outcome(semantic, receipt["outcome"]):
        raise _transport_error(
            "invalid_engine_response",
            "evolution persistence receipt outcome does not match its complete command",
        )
    mutations = receipt["mutations"]
    if not isinstance(mutations, list) or len(mutations) > 8192:
        raise _transport_error(
            "invalid_engine_response", "evolution mutation set is invalid"
        )
    previous: tuple[int, bytes] | None = None
    for mutation_value in mutations:
        mutation = _require_closed_record(
            mutation_value,
            {"family", "storage_key", "value_id"},
            "evolution mutation write",
        )
        family = mutation["family"]
        if (
            family not in _EVOLUTION_STATE_FAMILIES
            or not _is_sha256_id(mutation["storage_key"])
            or not _is_sha256_id(mutation["value_id"])
        ):
            raise _transport_error(
                "invalid_engine_response", "evolution mutation write is invalid"
            )
        current = (
            _EVOLUTION_STATE_FAMILIES.index(family),
            mutation["storage_key"].encode("utf-8"),
        )
        if previous is not None and previous >= current:
            raise _transport_error(
                "invalid_engine_response",
                "evolution mutation writes are not strictly key ordered",
            )
        previous = current


def _validate_evolution_persistence_command(value: object) -> dict[str, Any]:
    command = _require_closed_record(
        value,
        {"persistence_version", "persistence_id", "evolution_id", "command"},
        "evolution persistence command",
    )
    if (
        command["persistence_version"] != "cymule.evolution-persistence-command/4"
        or not _is_sha256_id(command["persistence_id"])
    ):
        raise _transport_error(
            "invalid_engine_response", "evolution persistence command is invalid"
        )
    _validate_live_identity(command["evolution_id"], "evolution")
    _validate_live_evolution_command(command["command"])
    return command


def _live_evolution_command_matches_outcome(
    command: dict[str, Any], outcome: dict[str, Any]
) -> bool:
    operation = command["operation"]
    result = outcome["result"]
    if operation == "publish_definition" and result == "definition_published":
        revision = outcome["revision"]
        return (
            revision["logical_ref"] == command["logical_ref"]
            and _wire_json_equal(revision["definition"], command["definition"])
            and _wire_json_equal(
                revision["references"], command["references"]
            )
        )
    if operation == "register_template" and result == "template_registered":
        expected_revisions = sorted(
            reference["logical_ref"]
            for reference in command["template"]["references"]
        )
        linked = outcome["linked"]
        return (
            linked["template_id"] == command["template"]["template_id"]
            and sorted(linked["resolved_revisions"]) == expected_revisions
        )
    if operation == "publish_and_relink" and result == "publication_applied":
        revision = outcome["receipt"]["revision"]
        publication = command["publication"]
        return (
            revision["logical_ref"] == publication["logical_ref"]
            and _wire_json_equal(revision["definition"], publication["definition"])
            and _wire_json_equal(
                revision["references"], publication["references"]
            )
        )
    if operation != "apply":
        return False

    applied = command["command"]
    applied_operation = applied["operation"]
    if applied_operation == "apply_patch" and result == "patch_applied":
        edge = outcome["edge"]
        patch = applied["patch"]
        return (
            edge["from_plan"] == patch["from_plan"]
            and _wire_json_equal(edge["operations"], patch["operations"])
        )
    if applied_operation in {"set_rollout", "observe"}:
        return result == "applied"
    if applied_operation == "select_occurrence" and result == "occurrence_selected":
        pin = outcome["pin"]
        return (
            pin["template_id"] == command["template_id"]
            and pin["occurrence_id"] == applied["occurrence_id"]
            and pin["selection_id"] == applied["selection_id"]
            and _wire_json_equal(
                pin["execution_binding"], applied["execution_binding"]
            )
        )
    if applied_operation == "migrate" and result == "migrated":
        return _wire_json_equal(outcome["receipt"]["request"], applied["request"])
    if (
        applied_operation == "restart_under_new_plan"
        and result == "restart_authorized"
    ):
        return _wire_json_equal(outcome["receipt"]["request"], applied["request"])
    if applied_operation == "shadow" and result == "shadow_recorded":
        comparison = outcome["comparison"]
        request = applied["request"]
        return all(
            comparison[field] == request[field]
            for field in (
                "comparison_id",
                "decision_id",
                "subject",
                "primary_plan",
                "shadow_plan",
                "driver_id",
                "driver_revision",
                "comparison_policy",
            )
        )
    if applied_operation == "apply_gate" and result == "gate_applied":
        transition = outcome["transition"]
        return (
            _wire_json_equal(transition["evaluation"]["gate"], applied["gate"])
            and transition["to_decision"] == applied["next_decision_id"]
        )
    return False


def _validate_evolution_commit_for_request(
    evolution_id: object,
    command_value: object,
    commit_value: object,
    *,
    expected_patch_plan_id: str | None = None,
) -> None:
    _validate_evolution_commit(commit_value)
    receipt_value = commit_value["receipt"]
    persisted_command = receipt_value["command"]
    if (
        persisted_command["evolution_id"] != evolution_id
        or not _wire_json_equal(persisted_command["command"], command_value)
    ):
        raise _transport_error(
            "invalid_engine_response",
            "evolution commit does not match its request",
        )
    if expected_patch_plan_id is not None:
        outcome = receipt_value["outcome"]
        if (
            not isinstance(outcome, dict)
            or outcome.get("result") != "patch_applied"
            or not isinstance(outcome.get("edge"), dict)
            or outcome["edge"].get("to_plan") != expected_patch_plan_id
        ):
            raise _transport_error(
                "invalid_engine_response",
                "Plan patch commit does not match the Rust-sealed target Plan",
            )


def _validate_live_identity(value: object, label: str) -> None:
    if not _is_bounded_printable_scalar_identity(value, 256):
        raise _transport_error(
            "invalid_engine_response",
            f"{label} identity must contain 1..=256 printable Unicode scalar values",
        )


def _is_bounded_printable_scalar_identity(
    value: object, maximum_scalars: int
) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= maximum_scalars
        and all(
            not (0xD800 <= ord(character) <= 0xDFFF)
            and not (ord(character) < 32 or 127 <= ord(character) <= 159)
            for character in value
        )
    )


def _is_core_identity(value: object) -> bool:
    return _is_bounded_printable_scalar_identity(value, 512)


def _validate_core_identity(value: object, label: str) -> None:
    if not _is_core_identity(value):
        raise _transport_error(
            "invalid_engine_response",
            f"{label} identity must contain 1..=512 printable Unicode scalar values",
        )


def _validate_content_id(value: object, label: str) -> None:
    if not _is_sha256_id(value):
        raise _transport_error(
            "invalid_engine_response", f"{label} identity is not a sha256 content ID"
        )


def _validate_subflow_name(value: object, label: str) -> None:
    if not _is_bounded_printable_identity(value, 160):
        raise _transport_error(
            "invalid_engine_response",
            f"{label} must contain 1..=160 printable UTF-8 bytes",
        )


def _is_bounded_printable_identity(value: object, maximum_bytes: int) -> bool:
    if (
        not isinstance(value, str)
        or not value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        return False
    try:
        return len(value.encode("utf-8")) <= maximum_bytes
    except UnicodeEncodeError:
        return False


def _validate_plan_definition(value: object) -> dict[str, Any]:
    definition = _require_closed_record(
        value,
        {"id", "input_schema", "output_schema", "body"},
        "Plan definition",
    )
    if not _is_plan_id(definition["id"]):
        raise _transport_error(
            "invalid_engine_response", "Plan definition identity is invalid"
        )
    _validate_plan_schema(definition["input_schema"])
    _validate_plan_schema(definition["output_schema"])
    _validate_plan_region(definition["body"])
    return definition


def _validate_typed_wire_definition(value: object) -> dict[str, Any]:
    definition = _require_closed_record(
        value,
        {"id", "input_schema", "output_schema", "body"},
        "typed Definition",
    )
    if not isinstance(definition["id"], str):
        raise _transport_error(
            "invalid_engine_response", "typed Definition identity is invalid"
        )
    _validate_json_value(definition["input_schema"])
    _validate_json_value(definition["output_schema"])
    _validate_typed_wire_region(definition["body"])
    return definition


def _validate_typed_wire_region(value: object) -> None:
    region = _require_closed_record(
        value, {"steps", "result"}, "typed Definition Region"
    )
    if not isinstance(region["steps"], list):
        raise _transport_error(
            "invalid_engine_response", "typed Definition steps are invalid"
        )
    for step in region["steps"]:
        _validate_typed_wire_step(step)
    _validate_typed_wire_expression(region["result"])


def _validate_typed_wire_step(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("op"), str):
        raise _transport_error(
            "invalid_engine_response", "typed Definition operation is invalid"
        )
    op = value["op"]
    fields = {
        "call": {"id", "op", "component", "input"},
        "invoke": {"id", "op", "definition", "input"},
        "wait": {"id", "op", "wait"},
        "effect": {"id", "op", "effect", "input", "occurrence"},
        "scope": {"id", "op", "body"},
    }.get(op)
    if fields is None or frozenset(value) not in {
        frozenset(fields),
        frozenset(fields | {"bind"}),
    }:
        raise _transport_error(
            "invalid_engine_response", "typed Definition operation is not closed"
        )
    string_fields = {
        "call": {"id", "component"},
        "invoke": {"id", "definition"},
        "wait": {"id"},
        "effect": {"id", "effect", "occurrence"},
        "scope": {"id"},
    }[op]
    if any(not isinstance(value[field], str) for field in string_fields) or (
        "bind" in value and not isinstance(value["bind"], str)
    ):
        raise _transport_error(
            "invalid_engine_response", "typed Definition operation strings are invalid"
        )
    if op in {"call", "invoke", "effect"}:
        _validate_typed_wire_expression(value["input"])
    elif op == "wait":
        _validate_typed_wire_wait(value["wait"])
    elif op == "scope":
        _validate_typed_wire_region(value["body"])


def _validate_typed_wire_expression(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise _transport_error(
            "invalid_engine_response", "typed Definition expression is invalid"
        )
    kind = value["kind"]
    fields = {
        "input": {"kind"},
        "literal": {"kind", "value"},
        "binding": {"kind", "name"},
        "object": {"kind", "fields"},
        "array": {"kind", "items"},
    }.get(kind)
    if fields is None or set(value) != fields:
        raise _transport_error(
            "invalid_engine_response", "typed Definition expression is not closed"
        )
    if kind == "literal":
        _validate_json_value(value["value"])
    elif kind == "binding":
        if not isinstance(value["name"], str):
            raise _transport_error(
                "invalid_engine_response", "typed binding expression is invalid"
            )
    elif kind == "object":
        if not isinstance(value["fields"], dict) or not all(
            isinstance(name, str) for name in value["fields"]
        ):
            raise _transport_error(
                "invalid_engine_response", "typed object expression is invalid"
            )
        for expression in value["fields"].values():
            _validate_typed_wire_expression(expression)
    elif kind == "array":
        if not isinstance(value["items"], list):
            raise _transport_error(
                "invalid_engine_response", "typed array expression is invalid"
            )
        for expression in value["items"]:
            _validate_typed_wire_expression(expression)


def _validate_typed_wire_wait(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise _transport_error(
            "invalid_engine_response", "typed Definition wait is invalid"
        )
    expected = {
        "signal": {"kind", "key", "consume_once"},
        "timer": {"kind", "timer_id"},
        "input": {"kind", "correlation", "schema"},
    }.get(value["kind"])
    if expected is None or set(value) != expected:
        raise _transport_error(
            "invalid_engine_response", "typed Definition wait is not closed"
        )
    if value["kind"] == "signal":
        if not isinstance(value["key"], str) or not isinstance(
            value["consume_once"], bool
        ):
            raise _transport_error(
                "invalid_engine_response", "typed signal wait is invalid"
            )
    elif value["kind"] == "timer":
        if not isinstance(value["timer_id"], str):
            raise _transport_error(
                "invalid_engine_response", "typed timer wait is invalid"
            )
    else:
        if not isinstance(value["correlation"], str):
            raise _transport_error(
                "invalid_engine_response", "typed input wait is invalid"
            )
        _validate_json_value(value["schema"])


def _validate_subflow_references(
    value: object, local_definitions: set[str]
) -> list[str]:
    if not isinstance(value, list):
        raise _transport_error(
            "invalid_engine_response", "subflow references are invalid"
        )
    logical_refs: set[str] = set()
    occupied_definitions = set(local_definitions)
    for reference_value in value:
        reference = _require_closed_record(
            reference_value,
            {
                "logical_ref", "local_definition", "input_schema",
                "output_schema", "strategy",
            },
            "subflow reference",
        )
        _validate_subflow_name(reference["logical_ref"], "definition reference")
        _validate_subflow_name(reference["local_definition"], "local definition")
        _validate_json_value(reference["input_schema"])
        _validate_json_value(reference["output_schema"])
        if (
            reference["logical_ref"] in logical_refs
            or reference["local_definition"] in occupied_definitions
        ):
            raise _transport_error(
                "invalid_engine_response", "subflow references are not unique"
            )
        logical_refs.add(reference["logical_ref"])
        occupied_definitions.add(reference["local_definition"])
        strategy = reference["strategy"]
        if not isinstance(strategy, dict):
            raise _transport_error(
                "invalid_engine_response", "subflow reference strategy is invalid"
            )
        strategy_kind = strategy.get("strategy")
        if not isinstance(strategy_kind, str):
            raise _transport_error(
                "invalid_engine_response", "subflow reference strategy is invalid"
            )
        expected_fields = {
            "latest_compatible": {"strategy"},
            "pinned": {"strategy", "revision_id"},
        }.get(strategy_kind)
        if expected_fields is None or set(strategy) != expected_fields:
            raise _transport_error(
                "invalid_engine_response", "subflow reference strategy is not closed"
            )
        if strategy_kind == "pinned":
            _validate_content_id(strategy["revision_id"], "pinned subflow revision")
    return sorted(logical_refs)


def _validate_published_subflow_references(
    value: object, local_definitions: set[str]
) -> None:
    logical_refs = _validate_subflow_references(value, local_definitions)
    if not isinstance(value, list) or len(value) > 1024:
        raise _transport_error(
            "invalid_engine_response", "publication references are outside bounds"
        )
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    if len(encoded) > 1024 * 1024:
        raise _transport_error(
            "invalid_engine_response", "publication references are outside bounds"
        )
    actual_refs = [reference["logical_ref"] for reference in value]
    if actual_refs != logical_refs or any(
        reference["strategy"]["strategy"] != "pinned" for reference in value
    ):
        raise _transport_error(
            "invalid_engine_response",
            "publication references must be ordered, unique, and pinned",
        )


def _validate_subflow_revision(value: object) -> None:
    revision = _require_closed_record(
        value,
        {
            "revision_version", "revision_id", "logical_ref", "sequence",
            "definition", "references",
        },
        "subflow revision",
    )
    if revision["revision_version"] != "cymule.subflow-revision/2":
        raise _transport_error(
            "invalid_engine_response", "subflow revision version is invalid"
        )
    _validate_content_id(revision["revision_id"], "subflow revision")
    _validate_subflow_name(revision["logical_ref"], "definition reference")
    _validate_positive_epoch(revision["sequence"])
    definition = _validate_typed_wire_definition(revision["definition"])
    _validate_subflow_name(definition["id"], "definition")
    _validate_published_subflow_references(
        revision["references"], {definition["id"]}
    )


def _validate_publication_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value, {"revision", "updates"}, "publication receipt"
    )
    _validate_subflow_revision(receipt["revision"])
    updates = receipt["updates"]
    if not isinstance(updates, list):
        raise _transport_error(
            "invalid_engine_response", "publication updates are invalid"
        )
    previous_template: str | None = None
    for update_value in updates:
        update = _require_closed_record(
            update_value,
            {
                "template_id", "previous_plan_id", "current_plan_id",
                "decision_id", "advanced",
            },
            "template update",
        )
        _validate_live_identity(update["template_id"], "template")
        _validate_content_id(update["previous_plan_id"], "previous Plan")
        _validate_content_id(update["current_plan_id"], "current Plan")
        if previous_template is not None and previous_template >= update["template_id"]:
            raise _transport_error(
                "invalid_engine_response",
                "publication updates are not strictly template-ordered",
            )
        previous_template = update["template_id"]
        if not isinstance(update["advanced"], bool):
            raise _transport_error(
                "invalid_engine_response", "publication update disposition is invalid"
            )
        decision_id = update["decision_id"]
        if update["advanced"]:
            _validate_content_id(decision_id, "rollout decision")
            valid_advance = update["previous_plan_id"] != update["current_plan_id"]
        else:
            valid_advance = (
                decision_id is None
                and update["previous_plan_id"] == update["current_plan_id"]
            )
        if not valid_advance:
            raise _transport_error(
                "invalid_engine_response",
                "publication update does not match its Plan advance",
            )


def _validate_plan_edge(value: object) -> None:
    edge = _require_closed_record(
        value,
        {"edge_id", "from_plan", "to_plan", "operations"},
        "Plan edge",
    )
    _validate_content_id(edge["edge_id"], "Plan edge")
    _validate_content_id(edge["from_plan"], "source Plan")
    _validate_content_id(edge["to_plan"], "target Plan")
    operations = edge["operations"]
    if edge["from_plan"] == edge["to_plan"] or not isinstance(operations, list) or not operations:
        raise _transport_error(
            "invalid_engine_response",
            "Plan edge must contain a non-empty transition between distinct Plans",
        )
    previous_operation: tuple[str, str] | None = None
    for operation_value in operations:
        operation = _require_closed_record(
            operation_value,
            {"kind", "target", "before", "after"},
            "patch operation",
        )
        _validate_live_identity(operation["kind"], "patch operation kind")
        _validate_live_identity(operation["target"], "patch target")
        before = operation["before"]
        after = operation["after"]
        valid_shape = {
            "add": before is None and _is_lower_hex_digest(after),
            "remove": _is_lower_hex_digest(before) and after is None,
            "replace": (
                _is_lower_hex_digest(before)
                and _is_lower_hex_digest(after)
                and before != after
            ),
        }.get(operation["kind"], False)
        if not valid_shape:
            raise _transport_error(
                "invalid_engine_response",
                "Plan edge contains a malformed patch operation",
            )
        current_operation = (operation["target"], operation["kind"])
        if previous_operation is not None and previous_operation >= current_operation:
            raise _transport_error(
                "invalid_engine_response",
                "Plan edge operations are not in canonical order",
            )
        previous_operation = current_operation


def _validate_migration_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value,
        {
            "request", "source_witness_id", "source_binding", "target_binding",
            "source_execution_fence", "target_epoch", "adapter_id",
            "adapter_revision", "from_schema", "to_schema", "output_state",
            "target_continuation", "evidence",
        },
        "migration receipt",
    )
    request = _validate_migration_request(receipt["request"])
    _validate_content_id(receipt["source_witness_id"], "migration source witness")
    for field in ("source_binding", "target_binding"):
        _validate_artifact_ref(receipt[field])
        if receipt[field]["kind"] != "cymule.execution-binding/2":
            raise _transport_error(
                "invalid_engine_response",
                "migration receipt binding is not an ExecutionBinding Artifact",
            )
    _validate_epoch(receipt["source_execution_fence"])
    for field in ("adapter_id", "from_schema", "to_schema"):
        _validate_live_identity(receipt[field], f"migration {field}")
    _validate_content_id(receipt["adapter_revision"], "migration adapter revision")
    _validate_positive_epoch(receipt["target_epoch"])
    _validate_artifact_ref(receipt["output_state"])
    _validate_artifact_ref(receipt["evidence"])
    _validate_migration_continuation(receipt["target_continuation"])
    target = receipt["target_continuation"]
    expected_target_epoch = request["expected_source_epoch"] + 1
    if (
        expected_target_epoch > _MAX_SAFE_JSON_INTEGER
        or receipt["target_epoch"] != expected_target_epoch
        or receipt["adapter_id"] != request["adapter_id"]
        or receipt["adapter_revision"] != request["adapter_revision"]
        or target["run_id"] != request["run_id"]
        or target["plan_id"] != request["to_plan"]
        or target["binding_context"] != receipt["target_binding"]["artifact_id"]
        or target["epoch"] != receipt["target_epoch"]
        or not _wire_json_equal(target["state"], receipt["output_state"])
        or target["execution_fence"] != receipt["source_execution_fence"]
        or not _is_root_migration_continuation(target)
    ):
        raise _transport_error(
            "invalid_engine_response",
            "migration receipt has an inconsistent target Continuation",
        )


def _validate_restart_receipt(value: object) -> None:
    receipt = _require_closed_record(
        value, {"request", "source_witness_id", "target_plan"}, "restart receipt"
    )
    request = _validate_restart_request(receipt["request"])
    _validate_content_id(receipt["source_witness_id"], "restart source witness")
    _validate_sealed_plan(receipt["target_plan"])
    if receipt["target_plan"]["plan_id"] != request["to_plan"]:
        raise _transport_error(
            "invalid_engine_response",
            "restart receipt target Plan does not match its request",
        )


def _validate_shadow_comparison(value: object) -> None:
    comparison = _require_closed_record(
        value,
        {
            "comparison_id", "subject", "decision_id", "primary_plan",
            "shadow_plan", "driver_id", "driver_revision",
            "comparison_policy", "primary_digest", "shadow_digest",
            "equivalent", "evidence",
        },
        "shadow comparison",
    )
    for field in (
        "comparison_id", "subject", "decision_id", "driver_id",
        "comparison_policy",
    ):
        _validate_live_identity(comparison[field], f"shadow {field}")
    _validate_content_id(comparison["driver_revision"], "shadow driver revision")
    if not _is_lower_hex_digest(comparison["primary_digest"]) or not _is_lower_hex_digest(
        comparison["shadow_digest"]
    ):
        raise _transport_error(
            "invalid_engine_response", "shadow comparison digest is invalid"
        )
    _validate_content_id(comparison["primary_plan"], "primary Plan")
    _validate_content_id(comparison["shadow_plan"], "shadow Plan")
    if (
        comparison["primary_plan"] == comparison["shadow_plan"]
        or not isinstance(comparison["equivalent"], bool)
    ):
        raise _transport_error(
            "invalid_engine_response", "shadow comparison lineage or result is invalid"
        )
    _validate_artifact_ref(comparison["evidence"])


def _validate_rollout_transition(value: object) -> None:
    transition = _require_closed_record(
        value,
        {"transition_id", "from_decision", "to_decision", "evaluation"},
        "rollout transition",
    )
    _validate_content_id(transition["transition_id"], "rollout transition")
    _validate_live_identity(transition["from_decision"], "source rollout decision")
    _validate_live_identity(transition["to_decision"], "target rollout decision")
    if transition["from_decision"] == transition["to_decision"]:
        raise _transport_error(
            "invalid_engine_response",
            "rollout transition must create a distinct decision",
        )
    evaluation = _require_closed_record(
        transition["evaluation"],
        {
            "evaluation_id", "gate", "target_observations", "target_failures",
            "equivalent_shadows", "inequivalent_shadows", "outcome",
            "evidence_ids",
        },
        "rollout evaluation",
    )
    _validate_content_id(evaluation["evaluation_id"], "rollout evaluation")
    gate = _require_closed_record(
        evaluation["gate"],
        {
            "gate_id", "decision_id", "min_target_observations",
            "max_target_failures", "min_equivalent_shadows",
            "max_inequivalent_shadows",
        },
        "rollout gate",
    )
    _validate_live_identity(gate["gate_id"], "rollout gate")
    _validate_live_identity(gate["decision_id"], "gate rollout decision")
    if gate["decision_id"] != transition["from_decision"]:
        raise _transport_error(
            "invalid_engine_response",
            "rollout transition does not match its gate decision",
        )
    count_fields = (
        "min_target_observations", "max_target_failures",
        "min_equivalent_shadows", "max_inequivalent_shadows",
    )
    for field in count_fields:
        _validate_epoch(gate[field])
    for field in (
        "target_observations", "target_failures",
        "equivalent_shadows", "inequivalent_shadows",
    ):
        _validate_epoch(evaluation[field])
    if evaluation["target_failures"] > evaluation["target_observations"]:
        raise _transport_error(
            "invalid_engine_response",
            "rollout failures exceed target observations",
        )
    evidence_ids = evaluation["evidence_ids"]
    if not isinstance(evidence_ids, list):
        raise _transport_error(
            "invalid_engine_response", "rollout evidence identities are invalid"
        )
    seen_evidence: set[str] = set()
    for evidence_id in evidence_ids:
        _validate_live_identity(evidence_id, "rollout evidence")
        if evidence_id in seen_evidence:
            raise _transport_error(
                "invalid_engine_response", "rollout evidence identities are not unique"
            )
        seen_evidence.add(evidence_id)
    expected_evidence_count = (
        evaluation["target_observations"]
        + evaluation["equivalent_shadows"]
        + evaluation["inequivalent_shadows"]
    )
    if len(evidence_ids) != expected_evidence_count:
        raise _transport_error(
            "invalid_engine_response",
            "rollout evidence counts do not match the frozen identity set",
        )
    if (
        evaluation["target_failures"] > gate["max_target_failures"]
        or evaluation["inequivalent_shadows"] > gate["max_inequivalent_shadows"]
    ):
        expected_outcome = "rollback"
    elif (
        evaluation["target_observations"] >= gate["min_target_observations"]
        and evaluation["equivalent_shadows"] >= gate["min_equivalent_shadows"]
    ):
        expected_outcome = "promote"
    else:
        expected_outcome = "pending"
    if evaluation["outcome"] != expected_outcome or expected_outcome == "pending":
        raise _transport_error(
            "invalid_engine_response",
            "rollout transition outcome does not match its exact evidence",
        )


def _validate_occurrence_pin(value: object) -> None:
    pin = _require_closed_record(
        value,
        {
            "occurrence_id", "template_id", "decision_id", "plan_id",
            "execution_binding", "selection_id",
        },
        "occurrence pin",
    )
    for field in ("occurrence_id", "template_id", "decision_id", "selection_id"):
        _validate_live_identity(pin[field], field)
    _validate_content_id(pin["plan_id"], "selected Plan")
    _validate_artifact_ref(pin["execution_binding"])
    if pin["execution_binding"]["kind"] != "cymule.execution-binding/2":
        raise _transport_error(
            "invalid_engine_response",
            "occurrence pin binding is not an ExecutionBinding Artifact",
        )


def _validate_linked_plan(value: object) -> None:
    linked = _require_closed_record(
        value, {"template_id", "plan", "resolved_revisions"}, "linked Plan"
    )
    _validate_live_identity(linked["template_id"], "template")
    _validate_sealed_plan(linked["plan"])
    resolved_revisions = linked["resolved_revisions"]
    if not isinstance(resolved_revisions, dict):
        raise _transport_error(
            "invalid_engine_response", "resolved revisions are invalid"
        )
    for logical_ref, revision_id in resolved_revisions.items():
        _validate_live_identity(logical_ref, "definition reference")
        _validate_content_id(revision_id, "subflow revision")


def _validate_migration_continuation(value: object) -> None:
    fields = {
        "continuation_version", "run_id", "plan_id", "binding_context", "frames", "state", "wait_set", "scope_stack",
        "epoch", "execution_fence", "execution_claim", "status",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise _transport_error("invalid_engine_response", "migration Continuation is not closed")
    if value["continuation_version"] != "cymule.continuation-state/1":
        raise _transport_error("invalid_engine_response", "migration Continuation generation is unsupported")
    _require_strings(value, {"run_id", "plan_id", "binding_context"})
    _validate_core_identity(value["run_id"], "migration Continuation Run")
    _validate_content_id(value["plan_id"], "migration Continuation Plan")
    _validate_epoch(value["epoch"])
    _validate_epoch(value["execution_fence"])
    if value["execution_claim"] is not None or value["status"] != "ready":
        raise _transport_error("invalid_engine_response", "migration Continuation must be ready without an execution claim")
    for field in {"wait_set", "scope_stack"}:
        items = value[field]
        if (
            not isinstance(items, list)
            or (field == "scope_stack" and not items)
            or any(not isinstance(item, str) or not item for item in items)
        ):
            raise _transport_error("invalid_engine_response", f"migration Continuation {field} is invalid")
    if value["state"] is not None:
        _validate_artifact_ref(value["state"])
    frames = value["frames"]
    frame_fields = {"definition_id", "invocation_id", "invocation_path", "scope_id", "input", "region_path", "next_step", "locals"}
    if not isinstance(frames, list) or not frames:
        raise _transport_error("invalid_engine_response", "migration Continuation frames are invalid")
    for frame in frames:
        if not isinstance(frame, dict) or set(frame) != frame_fields:
            raise _transport_error("invalid_engine_response", "migration frame is not closed")
        _require_strings(frame, {"definition_id", "invocation_id", "scope_id"})
        _validate_epoch(frame["next_step"])
        _validate_artifact_ref(frame["input"])
        if not isinstance(frame["region_path"], list) or any(not isinstance(index, int) or isinstance(index, bool) or index < 0 or index > _MAX_SAFE_JSON_INTEGER for index in frame["region_path"]):
            raise _transport_error("invalid_engine_response", "migration frame Region path is invalid")
        if not isinstance(frame["locals"], dict):
            raise _transport_error("invalid_engine_response", "migration frame locals are invalid")
        for reference in frame["locals"].values():
            _validate_artifact_ref(reference)
        if not isinstance(frame["invocation_path"], list):
            raise _transport_error("invalid_engine_response", "migration invocation path is invalid")
        for segment in frame["invocation_path"]:
            if not isinstance(segment, dict) or set(segment) != {"site_id", "region_path", "scope_id"}:
                raise _transport_error("invalid_engine_response", "migration invocation segment is not closed")
            _require_strings(segment, {"site_id", "scope_id"})
            if not isinstance(segment["region_path"], list) or any(not isinstance(index, int) or isinstance(index, bool) or index < 0 or index > _MAX_SAFE_JSON_INTEGER for index in segment["region_path"]):
                raise _transport_error("invalid_engine_response", "migration invocation Region path is invalid")


def _is_root_migration_continuation(value: dict[str, Any]) -> bool:
    return (
        value["status"] == "ready"
        and value["execution_claim"] is None
        and bool(value["frames"])
        and value["wait_set"] == []
        and value["scope_stack"] == ["scope:root"]
    )


def _validate_evolution_command(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "evolution command is not an object")
    common = {"control_version", "command_id", "operation"}
    operation = value.get("operation")
    if not isinstance(operation, str):
        raise _transport_error(
            "invalid_engine_response", "evolution command operation is invalid"
        )
    variant = {
        "apply_patch": {"patch"},
        "set_rollout": {"decision"},
        "select_occurrence": {"occurrence_id", "selection_id", "execution_binding"},
        "migrate": {"request"},
        "restart_under_new_plan": {"request"},
        "shadow": {"request"},
        "observe": {"observation"},
        "apply_gate": {"gate", "next_decision_id"},
    }.get(operation)
    if (
        value.get("control_version") != "cymule.evolution-control/5"
        or variant is None
        or set(value) != common | variant
    ):
        raise _transport_error("invalid_engine_response", "evolution command is not closed")
    _validate_live_identity(value["command_id"], "evolution command")
    if operation == "apply_patch":
        patch = _require_closed_record(
            value["patch"],
            {"from_plan", "target", "operations", "evidence"},
            "Plan patch",
        )
        _validate_content_id(patch["from_plan"], "parent Plan")
        _validate_plan_candidate(patch["target"])
        _validate_patch_operations(patch["operations"])
        _validate_artifact_ref(patch["evidence"])
    elif operation == "set_rollout":
        decision = _require_closed_record(
            value["decision"],
            {"decision_id", "fallback_plan", "target_plan", "mode"},
            "rollout decision",
        )
        _validate_live_identity(decision["decision_id"], "rollout decision")
        _validate_content_id(decision["fallback_plan"], "rollout fallback Plan")
        _validate_content_id(decision["target_plan"], "rollout target Plan")
        _validate_rollout_mode(decision["mode"])
    elif operation == "select_occurrence":
        _validate_live_identity(value["occurrence_id"], "occurrence")
        _validate_live_identity(value["selection_id"], "occurrence selection")
        _validate_artifact_ref(value["execution_binding"])
        if value["execution_binding"]["kind"] != "cymule.execution-binding/2":
            raise _transport_error(
                "invalid_engine_response",
                "occurrence binding is not an ExecutionBinding Artifact",
            )
    elif operation == "migrate":
        _validate_migration_request(value["request"])
    elif operation == "restart_under_new_plan":
        _validate_restart_request(value["request"])
    elif operation == "shadow":
        request = _validate_evolution_request(
            value["request"],
            {
                "comparison_id", "decision_id", "subject", "primary_plan", "shadow_plan",
                "driver_id", "driver_revision", "input", "comparison_policy",
            },
            {
                "comparison_id", "decision_id", "subject", "primary_plan",
                "shadow_plan", "driver_id", "comparison_policy",
            },
        )
        for field in (
            "comparison_id", "decision_id", "subject", "driver_id",
            "comparison_policy",
        ):
            _validate_live_identity(request[field], f"shadow {field}")
        _validate_content_id(request["primary_plan"], "primary shadow Plan")
        _validate_content_id(request["shadow_plan"], "secondary shadow Plan")
        _validate_content_id(request["driver_revision"], "shadow driver revision")
        if request["primary_plan"] == request["shadow_plan"]:
            raise _transport_error(
                "invalid_engine_response", "shadow request Plans must be distinct"
            )
        _validate_artifact_ref(request["input"])
    elif operation == "observe":
        observation = _require_closed_record(
            value["observation"],
            {
                "observation_id", "decision_id", "occurrence_id", "plan_id",
                "outcome", "evidence",
            },
            "rollout observation",
        )
        _validate_live_identity(observation["observation_id"], "rollout observation")
        _validate_live_identity(observation["decision_id"], "rollout decision")
        _validate_live_identity(observation["occurrence_id"], "rollout occurrence")
        _validate_content_id(observation["plan_id"], "observed Plan")
        if observation["outcome"] not in {"succeeded", "failed"}:
            raise _transport_error(
                "invalid_engine_response", "rollout observation outcome is invalid"
            )
        _validate_artifact_ref(observation["evidence"])
    elif operation == "apply_gate":
        gate = _require_closed_record(
            value["gate"],
            {
                "gate_id", "decision_id", "min_target_observations",
                "max_target_failures", "min_equivalent_shadows",
                "max_inequivalent_shadows",
            },
            "rollout gate",
        )
        _validate_live_identity(gate["gate_id"], "rollout gate")
        _validate_live_identity(value["next_decision_id"], "rollout decision")


def _validate_patch_operations(value: object) -> None:
    if not isinstance(value, list) or not value:
        raise _transport_error(
            "invalid_engine_response", "Plan patch operations are invalid"
        )
    previous: tuple[str, str] | None = None
    for operation_value in value:
        operation = _require_closed_record(
            operation_value,
            {"kind", "target", "before", "after"},
            "patch operation",
        )
        _validate_live_identity(operation["kind"], "patch operation kind")
        _validate_live_identity(operation["target"], "patch operation target")
        valid_shape = {
            "add": operation["before"] is None
            and _is_lower_hex_digest(operation["after"]),
            "remove": _is_lower_hex_digest(operation["before"])
            and operation["after"] is None,
            "replace": _is_lower_hex_digest(operation["before"])
            and _is_lower_hex_digest(operation["after"])
            and operation["before"] != operation["after"],
        }.get(operation["kind"], False)
        if not valid_shape:
            raise _transport_error(
                "invalid_engine_response", "Plan patch operation is malformed"
            )
        current = (operation["target"], operation["kind"])
        if previous is not None and previous >= current:
            raise _transport_error(
                "invalid_engine_response",
                "Plan patch operations are not in canonical order",
            )
        previous = current


def _validate_rollout_mode(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("mode"), str):
        raise _transport_error(
            "invalid_engine_response", "rollout mode is invalid"
        )
    if value["mode"] == "canary":
        _require_closed_record(value, {"mode", "basis_points"}, "rollout mode")
        basis_points = value["basis_points"]
        if (
            not isinstance(basis_points, int)
            or isinstance(basis_points, bool)
            or not 0 <= basis_points <= 10_000
        ):
            raise _transport_error(
                "invalid_engine_response", "canary rollout basis points are invalid"
            )
        return
    _require_closed_record(value, {"mode"}, "rollout mode")
    if value["mode"] not in {"shadow", "active", "rolled_back"}:
        raise _transport_error(
            "invalid_engine_response", "rollout mode is invalid"
        )


def _validate_live_evolution_command(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "live evolution command is not an object")
    common = {"control_version", "command_id", "operation"}
    operation = value.get("operation")
    if not isinstance(operation, str):
        raise _transport_error(
            "invalid_engine_response", "live evolution command operation is invalid"
        )
    variants = {
        "publish_definition": [common | {"logical_ref", "definition", "references"}],
        "register_template": [common | {"template"}],
        "publish_and_relink": [common | {"publication"}],
        "apply": [common | {"template_id", "command"}],
    }.get(operation)
    if (
        value.get("control_version") != "cymule.live-evolution-control/6"
        or variants is None
        or set(value) not in variants
    ):
        raise _transport_error("invalid_engine_response", "live evolution command is not closed")
    _validate_live_identity(value["command_id"], "live-evolution command")
    if operation == "publish_definition":
        _validate_subflow_name(value["logical_ref"], "definition reference")
        definition = _validate_typed_wire_definition(value["definition"])
        _validate_subflow_name(definition["id"], "definition")
        _validate_published_subflow_references(
            value["references"], {definition["id"]}
        )
    elif operation == "register_template":
        template = _require_closed_record(
            value["template"],
            {"template_id", "candidate", "references"},
            "Plan template",
        )
        _validate_live_identity(template["template_id"], "template")
        _validate_plan_candidate(template["candidate"])
        _validate_subflow_references(
            template["references"],
            {
                definition["id"]
                for definition in template["candidate"]["definitions"]
            },
        )
    elif operation == "publish_and_relink":
        publication = _require_closed_record(
            value["publication"],
            {"logical_ref", "definition", "references", "evidence", "mode"},
            "live publication",
        )
        _validate_subflow_name(publication["logical_ref"], "definition reference")
        definition = _validate_typed_wire_definition(publication["definition"])
        _validate_subflow_name(definition["id"], "definition")
        _validate_published_subflow_references(
            publication["references"], {definition["id"]}
        )
        _validate_artifact_record(publication["evidence"])
    elif operation == "apply":
        _validate_live_identity(value["template_id"], "template")
        _validate_evolution_command(value["command"])


def _validate_migration_request(value: object) -> dict[str, Any]:
    request = _validate_evolution_request(
        value,
        {
            "migration_id", "run_id", "from_plan", "to_plan", "plan_edge_id",
            "compatibility_id", "expected_source_epoch", "adapter_id",
            "adapter_revision",
        },
        {
            "migration_id", "run_id", "from_plan", "to_plan", "plan_edge_id",
            "compatibility_id", "adapter_id", "adapter_revision",
        },
    )
    _validate_live_identity(request["migration_id"], "migration")
    _validate_live_identity(request["adapter_id"], "migration adapter")
    _validate_core_identity(request["run_id"], "migration Run")
    _validate_content_id(request["from_plan"], "source Plan")
    _validate_content_id(request["to_plan"], "target Plan")
    _validate_content_id(request["plan_edge_id"], "migration Plan edge")
    _validate_content_id(request["compatibility_id"], "migration compatibility")
    _validate_content_id(request["adapter_revision"], "migration adapter revision")
    _validate_epoch(request["expected_source_epoch"])
    if request["from_plan"] == request["to_plan"]:
        raise _transport_error(
            "invalid_engine_response", "migration request Plans must be distinct"
        )
    return request


def _validate_restart_request(value: object) -> dict[str, Any]:
    request = _validate_evolution_request(
        value,
        {
            "restart_id", "replacement_run", "run_id", "from_plan",
            "expected_source_epoch", "to_plan", "input", "evidence",
        },
        {"restart_id", "replacement_run", "run_id", "from_plan", "to_plan"},
    )
    _validate_live_identity(request["restart_id"], "restart")
    _validate_core_identity(request["replacement_run"], "replacement Run")
    _validate_core_identity(request["run_id"], "source Run")
    _validate_content_id(request["from_plan"], "source Plan")
    _validate_content_id(request["to_plan"], "target Plan")
    _validate_epoch(request["expected_source_epoch"])
    _validate_artifact_ref(request["input"])
    _validate_artifact_ref(request["evidence"])
    if (
        request["replacement_run"] == request["run_id"]
        or request["from_plan"] == request["to_plan"]
    ):
        raise _transport_error(
            "invalid_engine_response",
            "restart request has invalid source or replacement lineage",
        )
    return request


def _validate_tagged_result(
    value: object, tag: str, variants: dict[str, set[str]]
) -> dict[str, Any]:
    if not isinstance(value, dict) or not isinstance(value.get(tag), str):
        raise _transport_error("invalid_engine_response", "response union is not tagged")
    expected = variants.get(value[tag])
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "response union fields are not closed")
    return value


def _require_string_list(value: object, label: str) -> None:
    if not isinstance(value, list) or not all(_is_nonempty_string(item) for item in value):
        raise _transport_error("invalid_engine_response", f"{label} are invalid")


def _require_sorted_unique_string_list(
    value: object, label: str, *, content_ids: bool = False
) -> None:
    _require_string_list(value, label)
    if value != sorted(set(value)) or (
        content_ids and any(not _is_sha256_id(item) for item in value)
    ):
        raise _transport_error(
            "invalid_engine_response", f"{label} are not a sorted unique set"
        )


def _validate_index_list(value: object, label: str) -> None:
    if not isinstance(value, list) or any(
        not isinstance(index, int)
        or isinstance(index, bool)
        or index < 0
        or index > 9_007_199_254_740_991
        for index in value
    ):
        raise _transport_error("invalid_engine_response", f"{label} is invalid")


def _validate_sealed_plan(value: object) -> None:
    plan = _require_closed_record(value, {"plan_id", "candidate"}, "sealed Plan")
    if not _is_sha256_id(plan["plan_id"]):
        raise _transport_error("invalid_engine_response", "sealed Plan identity is invalid")
    _validate_plan_candidate(plan["candidate"])


def _validate_plan_candidate(value: object) -> None:
    candidate = _require_closed_record(
        value,
        {"ir_version", "name", "entry", "components", "effects", "definitions", "metadata"},
        "Plan candidate",
    )
    if (
        candidate["ir_version"] != "cymule.ir/3"
        or not _is_plan_id(candidate["name"])
        or not _is_plan_id(candidate["entry"])
    ):
        raise _transport_error("invalid_engine_response", "Plan candidate identity is invalid")
    for field in ("components", "effects", "definitions"):
        if not isinstance(candidate[field], list):
            raise _transport_error("invalid_engine_response", f"Plan candidate {field} are invalid")
    if not candidate["definitions"]:
        raise _transport_error("invalid_engine_response", "Plan candidate has no definitions")
    if not isinstance(candidate["metadata"], dict) or not all(
        isinstance(item, str) for item in candidate["metadata"].values()
    ):
        raise _transport_error("invalid_engine_response", "Plan candidate metadata is invalid")
    for contract in candidate["components"]:
        _validate_plan_contract(contract, effect=False)
    for contract in candidate["effects"]:
        _validate_plan_contract(contract, effect=True)
    for value_definition in candidate["definitions"]:
        _validate_plan_definition(value_definition)


def _validate_plan_contract(value: object, *, effect: bool) -> None:
    fields = {"id", "input_schema", "output_schema", "requirements"}
    if effect:
        fields.add("profile")
    else:
        fields.add("output_artifact_kind")
    contract = _require_closed_record(value, fields, "Plan contract")
    if not _is_plan_id(contract["id"]):
        raise _transport_error("invalid_engine_response", "Plan contract identity is invalid")
    _validate_plan_schema(contract["input_schema"])
    _validate_plan_schema(contract["output_schema"])
    if not isinstance(contract["requirements"], dict) or not all(
        isinstance(item, str) for item in contract["requirements"].values()
    ):
        raise _transport_error("invalid_engine_response", "Plan contract requirements are invalid")
    if not effect:
        output_artifact_kind = contract["output_artifact_kind"]
        if (
            not isinstance(output_artifact_kind, str)
            or not output_artifact_kind.isascii()
            or len(output_artifact_kind) > 255
            or re.fullmatch(
                r"[a-z0-9._+\-]+(?:/[a-z0-9._+\-]+)+",
                output_artifact_kind,
            )
            is None
        ):
            raise _transport_error(
                "invalid_engine_response",
                "Component output Artifact kind is invalid",
            )
    if effect:
        profile = _require_closed_record(
            contract["profile"],
            {"mutation", "dispatch", "reconciliation", "keyed_idempotency", "irreversible"},
            "Effect profile",
        )
        if (
            profile["mutation"] not in ("observational", "mutating")
            or profile["dispatch"] not in ("eager", "on_scope_commit", "explicit")
            or profile["reconciliation"] not in ("queryable", "externally_attested", "human", "impossible")
            or not isinstance(profile["keyed_idempotency"], bool)
            or not isinstance(profile["irreversible"], bool)
        ):
            raise _transport_error("invalid_engine_response", "Effect profile is invalid")


def _validate_plan_region(value: object) -> None:
    region = _require_closed_record(value, {"steps", "result"}, "Plan Region")
    if not isinstance(region["steps"], list):
        raise _transport_error("invalid_engine_response", "Plan Region steps are invalid")
    for step in region["steps"]:
        _validate_plan_step(step)
    _validate_plan_expression(region["result"])


def _validate_plan_step(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "Plan operation is invalid")
    op = value.get("op")
    if not isinstance(op, str):
        raise _transport_error("invalid_engine_response", "Plan operation tag is invalid")
    fields = {
        "call": {"id", "op", "component", "input"},
        "invoke": {"id", "op", "definition", "input"},
        "wait": {"id", "op", "wait"},
        "effect": {"id", "op", "effect", "input", "occurrence"},
        "scope": {"id", "op", "body"},
    }.get(op)
    if fields is None or frozenset(value) not in {frozenset(fields), frozenset(fields | {"bind"})}:
        raise _transport_error("invalid_engine_response", "Plan operation is not a wire operation")
    if not _is_plan_id(value["id"]) or ("bind" in value and not _is_plan_id(value["bind"])):
        raise _transport_error("invalid_engine_response", "Plan operation identity is invalid")
    if op == "call":
        if not _is_plan_id(value["component"]):
            raise _transport_error("invalid_engine_response", "Plan component call is invalid")
        _validate_plan_expression(value["input"])
    elif op == "invoke":
        if not _is_plan_id(value["definition"]):
            raise _transport_error("invalid_engine_response", "Plan invocation is invalid")
        _validate_plan_expression(value["input"])
    elif op == "wait":
        _validate_wait_spec(value["wait"])
    elif op == "effect":
        if not _is_plan_id(value["effect"]) or not _is_plan_id(value["occurrence"]):
            raise _transport_error("invalid_engine_response", "Plan Effect is invalid")
        _validate_plan_expression(value["input"])
    else:
        _validate_plan_region(value["body"])


def _validate_plan_expression(value: object) -> None:
    if not isinstance(value, dict):
        raise _transport_error("invalid_engine_response", "Plan expression is invalid")
    kind = value.get("kind")
    if not isinstance(kind, str):
        raise _transport_error("invalid_engine_response", "Plan expression tag is invalid")
    fields = {
        "input": {"kind"},
        "literal": {"kind", "value"},
        "binding": {"kind", "name"},
        "object": {"kind", "fields"},
        "array": {"kind", "items"},
    }.get(kind)
    if fields is None or set(value) != fields:
        raise _transport_error("invalid_engine_response", "Plan expression is not closed")
    if kind == "binding" and not _is_plan_id(value["name"]):
        raise _transport_error("invalid_engine_response", "Plan binding expression is invalid")
    if kind == "object":
        if not isinstance(value["fields"], dict):
            raise _transport_error("invalid_engine_response", "Plan object expression is invalid")
        for expression in value["fields"].values():
            _validate_plan_expression(expression)
    if kind == "array":
        if not isinstance(value["items"], list):
            raise _transport_error("invalid_engine_response", "Plan array expression is invalid")
        for expression in value["items"]:
            _validate_plan_expression(expression)


def _validate_plan_schema(value: object) -> None:
    if not isinstance(value, (bool, dict)):
        raise _transport_error("invalid_engine_response", "Plan schema is invalid")


def _is_plan_id(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 200


def _validate_wait_spec(value: object) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise _transport_error("invalid_engine_response", "wait contract is not tagged")
    expected = {
        "signal": {"kind", "key", "consume_once"},
        "timer": {"kind", "timer_id"},
        "input": {"kind", "correlation", "schema"},
    }.get(value["kind"])
    if expected is None or set(value) != expected:
        raise _transport_error("invalid_engine_response", "wait contract fields are not closed")
    if value["kind"] == "signal":
        _require_strings(value, {"key"})
        if not isinstance(value["consume_once"], bool):
            raise _transport_error("invalid_engine_response", "signal wait consume_once is invalid")
    elif value["kind"] == "timer":
        _require_strings(value, {"timer_id"})
    else:
        _require_strings(value, {"correlation"})
        if not isinstance(value["schema"], (dict, bool)):
            raise _transport_error("invalid_engine_response", "input wait schema is invalid")


def _validate_artifact_ref(value: object) -> None:
    reference = _require_closed_record(
        value, {"artifact_id", "identity_version", "kind"}, "Artifact reference"
    )
    _require_strings(reference, {"artifact_id", "identity_version", "kind"})
    artifact_id = reference["artifact_id"]
    kind = reference["kind"]
    if (
        reference["identity_version"] != "cymule.artifact/2"
        or not isinstance(artifact_id, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", artifact_id) is None
        or not isinstance(kind, str)
        or not kind.isascii()
        or len(kind) > 255
        or re.fullmatch(r"[a-z0-9._+\-]+(?:/[a-z0-9._+\-]+)+", kind) is None
    ):
        raise _transport_error("invalid_engine_response", "Artifact reference identity is invalid")


def _validate_artifact_record(value: object) -> None:
    record = _require_closed_record(value, {"reference", "bytes"}, "Artifact record")
    _validate_artifact_ref(record["reference"])
    payload = record["bytes"]
    if not isinstance(payload, str) or len(payload) > 11_184_812:
        raise _transport_error("invalid_engine_response", "Artifact record bytes are invalid")
    try:
        raw_bytes = base64.b64decode(payload, validate=True)
    except (ValueError, binascii.Error) as error:
        raise _transport_error(
            "invalid_engine_response", "Artifact record bytes are invalid Base64"
        ) from error
    if (
        len(raw_bytes) > 8 * 1024 * 1024
        or base64.b64encode(raw_bytes).decode("ascii") != payload
    ):
        raise _transport_error(
            "invalid_engine_response", "Artifact record bytes are not canonical Base64"
        )
    reference = record["reference"]
    kind = reference["kind"]
    kind_bytes = kind.encode("ascii")
    preimage = (
        b"cymule.artifact/2"
        + len(kind_bytes).to_bytes(4, "big")
        + kind_bytes
        + len(raw_bytes).to_bytes(8, "big")
        + raw_bytes
    )
    expected = "sha256:" + hashlib.sha256(preimage).hexdigest()
    if reference["artifact_id"] != expected:
        raise _transport_error(
            "invalid_engine_response",
            "Artifact record identity does not match its bytes",
        )


def _validate_evolution_request(
    value: object, fields: set[str], string_fields: set[str]
) -> dict[str, Any]:
    request = _require_closed_record(value, fields, "evolution request")
    _require_strings(request, string_fields)
    return request


def _require_closed_record(
    value: object, fields: set[str], label: str
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise _transport_error("invalid_engine_response", f"{label} fields are not closed")
    return value


def _require_strings(value: dict[str, Any], fields: set[str]) -> None:
    if not all(_is_nonempty_string(value.get(field)) for field in fields):
        raise _transport_error("invalid_engine_response", "required string field is invalid")


def _validate_epoch(value: object) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
        or value > _MAX_SAFE_JSON_INTEGER
    ):
        raise _transport_error("invalid_engine_response", "evolution epoch is invalid")


def _validate_positive_epoch(value: object) -> None:
    _validate_epoch(value)
    if value == 0:
        raise _transport_error("invalid_engine_response", "evolution epoch must be positive")


def _is_nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _is_sha256_id(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None


def _is_lower_hex_digest(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _is_precondition_token(value: object) -> bool:
    if not isinstance(value, str):
        return False
    match = re.fullmatch(r"pre:(0|[1-9][0-9]*):(sha256:[0-9a-f]{64})", value)
    return match is not None and int(match.group(1)) <= _MAX_SAFE_JSON_INTEGER


def _utf8_size(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return len(value.encode())
    except UnicodeEncodeError:
        return None


def _unicode_scalar_count(value: object) -> int | None:
    if not isinstance(value, str) or any(
        0xD800 <= ord(character) <= 0xDFFF for character in value
    ):
        return None
    return len(value)


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
    if any(value[field] is None for field in optional if field in value):
        raise _transport_error(
            "invalid_engine_response",
            "Engine failure optional fields are omission-only",
        )
    categories = {
        "transport_failure", "validation", "contract_violation", "admission_denied",
        "conflict", "not_found", "expected_plugin_failure", "plugin_defect",
        "substrate_failure", "cancelled", "timed_out", "unknown_world_outcome",
    }
    phases = {
        "transport", "decode_request", "validate_request", "seal_plan", "verify_plan",
        "seal_resource", "verify_wait_activation", "verify_durable_command", "observe_clock",
        "verify_evolution_command", "verify_live_evolution_command", "execute_plan",
        "execute_durable", "execute_live_evolution",
        "plugin_describe", "plugin_call", "effect_prepare", "effect_dispatch",
        "effect_reconcile", "encode_response",
    }
    retries = {
        "never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile"
    }
    category = value["category"]
    phase = value["phase"]
    if (
        not isinstance(category, str)
        or category not in categories
        or not isinstance(phase, str)
        or phase not in phases
    ):
        raise _transport_error("invalid_engine_response", "Engine failure enum is unknown")
    code = value["code"]
    message = value["message"]
    message_size = _unicode_scalar_count(message)
    if (
        not isinstance(code, str)
        or re.fullmatch(r"[a-z][a-z0-9_]{0,199}", code) is None
        or message_size is None
        or not 1 <= message_size <= 8192
        or _has_control(message)
    ):
        raise _transport_error("invalid_engine_response", "Engine failure bounds are invalid")
    if "retry_disposition" in value:
        retry_disposition = value["retry_disposition"]
        if (
            not isinstance(retry_disposition, str)
            or retry_disposition not in retries
        ):
            raise _transport_error(
                "invalid_engine_response", "retry disposition is unknown"
            )
    else:
        retry_disposition = None
    allowed_retries = {
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
    if retry_disposition not in allowed_retries[category]:
        raise _transport_error(
            "invalid_engine_response",
            "Engine failure category and retry disposition are incompatible",
        )
    contract = value.get("contract")
    contract_size = _unicode_scalar_count(contract)
    if contract is not None and (
        contract_size is None or not 1 <= contract_size <= 500 or _has_control(contract)
    ):
        raise _transport_error("invalid_engine_response", "contract identity is invalid")
    contract_side = value.get("contract_side")
    if contract_side is not None and (
        not isinstance(contract_side, str)
        or contract_side not in {"schema", "input", "output"}
    ):
        raise _transport_error("invalid_engine_response", "contract side is unknown")
    _validate_engine_path(value.get("path"), "failure path")
    issues = value.get("issues", [])
    if (
        not isinstance(issues, list)
        or "issues" in value and not 1 <= len(issues) <= 100
    ):
        raise _transport_error("invalid_engine_response", "Engine issue set is invalid")
    for issue in issues:
        if not isinstance(issue, dict) or not {"code", "message"} <= set(issue) or not set(issue) <= {"code", "message", "path", "schema_path"}:
            raise _transport_error("invalid_engine_response", "Engine issue fields are not closed")
        if any(
            issue[field] is None
            for field in ("path", "schema_path")
            if field in issue
        ):
            raise _transport_error(
                "invalid_engine_response",
                "Engine issue optional fields are omission-only",
            )
        issue_code_size = _unicode_scalar_count(issue["code"])
        issue_message_size = _unicode_scalar_count(issue["message"])
        if (
            issue_code_size is None
            or not 1 <= issue_code_size <= 200
            or issue_message_size is None
            or not 1 <= issue_message_size <= 2000
            or _has_control(issue["code"])
            or _has_control(issue["message"])
        ):
            raise _transport_error("invalid_engine_response", "Engine issue bounds are invalid")
        _validate_engine_path(issue.get("path"), "issue path")
        _validate_engine_path(issue.get("schema_path"), "issue schema path")


def _validate_engine_path(value: object, label: str) -> None:
    if value is not None:
        size = _unicode_scalar_count(value)
        if (
            size is None
            or size > 1000
            or _has_control(value)
            or bool(value) and not value.startswith("/")
        ):
            raise _transport_error("invalid_engine_response", f"{label} is invalid")


def _has_control(value: str) -> bool:
    return any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)


__all__ = [
    "ArtifactRecord",
    "ArtifactRef",
    "CliEngine",
    "CancelledRunBoundary",
    "DurableEngine",
    "ENGINE_PROTOCOL_VERSION",
    "DurableCommand",
    "DurableControlBuilder",
    "EngineCancellation",
    "EngineClockTarget",
    "EngineDurableTarget",
    "EvolutionCommand",
    "EvolutionCommit",
    "EvolutionControlBuilder",
    "EvolutionMutationWrite",
    "EvolutionPersistenceCommand",
    "EvolutionPersistenceReceipt",
    "EvolutionStateFamily",
    "EngineError",
    "EngineFailure",
    "EngineIssue",
    "EngineEvolutionTarget",
    "EngineMigrationProviderTarget",
    "EnginePluginTarget",
    "EngineShadowProviderTarget",
    "EngineProcessConfig",
    "EngineStoreTarget",
    "EngineTransport",
    "EngineTransportSuccess",
    "EffectResolutionCommand",
    "EffectResolutionReceipt",
    "EffectReconciliationBoundary",
    "EffectReleaseBoundary",
    "ExecutionOutcome",
    "LiveEvolutionCommand",
    "LiveEvolutionControlBuilder",
    "LiveEvolutionOutcome",
    "FlowBuilder",
    "Json",
    "MigrationRequest",
    "OccurrencePin",
    "ParkReason",
    "RegionMigrationCommand",
    "RegionMigrationPlan",
    "RegionMigrationReceipt",
    "RegionMigrationRequest",
    "RolloutDecision",
    "RolloutGate",
    "RolloutObservation",
    "ResourceBuilder",
    "ResourceHandoff",
    "ResourceHandoffActivation",
    "ResourceProducerProvenance",
    "RunCancellationReceipt",
    "CancellationCommand",
    "RestartRequest",
    "ShadowRequest",
    "VirtualArchiveBinding",
    "VirtualCompactionCertificate",
    "VirtualWorkControlBuilder",
    "VirtualCursor",
    "VirtualRegion",
    "WorkOccurrence",
    "WorkResolution",
    "WorkResolutionCommand",
    "WaitActivationBuilder",
    "WaitActivation",
    "WaitActivationReceipt",
    "directory_store",
    "process_plugin",
    "sqlite_clock",
    "sqlite_store",
]
