import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import { isAbsolute } from "node:path";
import { types as nodeTypes } from "node:util";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Schema = boolean | { [key: string]: Json };

export type ResourceShape = "inline" | "object" | "collection" | "directory" | "snapshot";
export type InlineData =
  | { encoding: "utf8"; text: string }
  | { encoding: "json"; value: Json }
  | { encoding: "base64"; data: string };
export type ResourceIntegrity =
  | { kind: "inline" }
  | { kind: "content"; digest: string; size: number }
  | { kind: "version"; authority: string; version: string }
  | { kind: "live"; identity: string };
export type ResourceLocation =
  | { kind: "public_url"; url: string }
  | { kind: "opaque"; reference: string };

export interface ResourceManifestDescriptor {
  manifest_version: "cymule.resource-manifest/3";
  media_type: "application/vnd.cymule.resource-manifest+jsonl";
  /** Content ID of media_type, size, entry_count, and root_digest. */
  digest: string;
  size: number;
  entry_count: number;
  root_digest: string;
}

export interface ResourceCandidate {
  resource_version: "cymule.resource/3";
  shape: ResourceShape;
  media_type: string;
  inline?: InlineData;
  integrity: ResourceIntegrity;
  manifest?: ResourceManifestDescriptor;
  annotations?: Record<string, string>;
}

export interface ResourceHandle extends ResourceCandidate {
  resource_id: string;
}

export interface ResourceHandoff {
  handoff_version: "cymule.resource-handoff/5";
  transfer_id: string;
  producer: { run_id: string; occurrence_id: string; result: ArtifactRef };
  to_run: string;
  slot: string;
  resource: ArtifactRef;
}

export interface ResourceHandoffActivation {
  activation_version: "cymule.resource-handoff-activation/3";
  activation_id: string;
  transfer_id: string;
  to_run: string;
  wait_id: string;
  result: ArtifactRef;
}

export interface ArtifactRef {
  identity_version: "cymule.artifact/2";
  artifact_id: string;
  kind: string;
}

export interface ArtifactRecord {
  reference: ArtifactRef;
  bytes: string;
}

export type RolloutMode =
  | { mode: "shadow" }
  | { mode: "canary"; basis_points: number }
  | { mode: "active" }
  | { mode: "rolled_back" };

export interface RolloutDecision {
  decision_id: string;
  fallback_plan: string;
  target_plan: string;
  mode: RolloutMode;
}

export interface OccurrencePin {
  occurrence_id: string;
  template_id: string;
  decision_id: string;
  plan_id: string;
  execution_binding: ArtifactRef;
  selection_id: string;
}

export interface RolloutObservation {
  observation_id: string;
  decision_id: string;
  occurrence_id: string;
  plan_id: string;
  outcome: "succeeded" | "failed";
  evidence: ArtifactRef;
}

export interface RolloutGate {
  gate_id: string;
  decision_id: string;
  min_target_observations: number;
  max_target_failures: number;
  min_equivalent_shadows: number;
  max_inequivalent_shadows: number;
}

export interface MigrationRequest {
  migration_id: string;
  run_id: string;
  from_plan: string;
  to_plan: string;
  plan_edge_id: string;
  compatibility_id: string;
  expected_source_epoch: number;
  adapter_id: string;
  adapter_revision: string;
}

export interface MigrationInvocationPathSegment {
  site_id: string;
  region_path: number[];
  scope_id: string;
}

export interface MigrationFrame {
  definition_id: string;
  invocation_id: string;
  invocation_path: MigrationInvocationPathSegment[];
  scope_id: string;
  input: ArtifactRef;
  region_path: number[];
  next_step: number;
  locals: Record<string, ArtifactRef>;
}

export interface MigrationContinuation {
  continuation_version: "cymule.continuation-state/1";
  run_id: string;
  plan_id: string;
  binding_context: string;
  frames: MigrationFrame[];
  state: ArtifactRef | null;
  wait_set: string[];
  scope_stack: string[];
  epoch: number;
  execution_fence: number;
  execution_claim: null;
  status: "ready";
}

export interface RestartRequest {
  restart_id: string;
  replacement_run: string;
  run_id: string;
  from_plan: string;
  expected_source_epoch: number;
  to_plan: string;
  input: ArtifactRef;
  evidence: ArtifactRef;
}

export interface ShadowRequest {
  comparison_id: string;
  decision_id: string;
  subject: string;
  primary_plan: string;
  shadow_plan: string;
  driver_id: string;
  driver_revision: string;
  input: ArtifactRef;
  comparison_policy: string;
}

export interface PlanPatch {
  from_plan: string;
  target: PlanCandidate;
  operations: Array<{
    kind: string;
    target: string;
    before: string | null;
    after: string | null;
  }>;
  evidence: ArtifactRef;
}

type EvolutionOperation =
  | { operation: "apply_patch"; patch: PlanPatch }
  | { operation: "set_rollout"; decision: RolloutDecision }
  | {
      operation: "select_occurrence";
      occurrence_id: string;
      selection_id: string;
      execution_binding: ArtifactRef;
    }
  | { operation: "migrate"; request: MigrationRequest }
  | { operation: "restart_under_new_plan"; request: RestartRequest }
  | { operation: "shadow"; request: ShadowRequest }
  | { operation: "observe"; observation: RolloutObservation }
  | { operation: "apply_gate"; gate: RolloutGate; next_decision_id: string };

export type EvolutionCommand = {
  control_version: "cymule.evolution-control/5";
  command_id: string;
} & EvolutionOperation;

type EvolutionCommandFor<Operation extends EvolutionOperation["operation"]> = {
  control_version: "cymule.evolution-control/5";
  command_id: string;
} & Extract<EvolutionOperation, { operation: Operation }>;

export type ReferenceStrategy =
  | { strategy: "latest_compatible" }
  | { strategy: "pinned"; revision_id: string };

export interface SubflowReference {
  logical_ref: string;
  local_definition: string;
  input_schema: Schema;
  output_schema: Schema;
  strategy: ReferenceStrategy;
}

export interface PlanTemplate {
  template_id: string;
  candidate: PlanCandidate;
  references: SubflowReference[];
}

export interface LivePublicationCommand {
  logical_ref: string;
  definition: PlanCandidate["definitions"][number];
  references: SubflowReference[];
  evidence: ArtifactRecord;
  mode: RolloutMode;
}

type LiveEvolutionOperation =
  | {
      operation: "publish_definition";
      logical_ref: string;
      definition: PlanCandidate["definitions"][number];
      references: SubflowReference[];
    }
  | { operation: "register_template"; template: PlanTemplate }
  | { operation: "publish_and_relink"; publication: LivePublicationCommand }
  | {
      operation: "apply";
      template_id: string;
      command: EvolutionCommand;
    };

type LiveEvolutionCommandEnvelope<T extends LiveEvolutionOperation> = T extends unknown ? {
  control_version: "cymule.live-evolution-control/6";
  command_id: string;
} & T : never;

export type LiveEvolutionCommand = LiveEvolutionCommandEnvelope<LiveEvolutionOperation>;

export type WaitActivationSource =
  | { kind: "signal"; key: string }
  | { kind: "timer"; timer_id: string };

export interface WaitActivation {
  activation_version: "cymule.wait-activation/2";
  activation_id: string;
  source: WaitActivationSource;
  wait_ids: string[];
  result: ArtifactRef;
}

export interface ClockObservationRef {
  clock_version: "cymule.clock-observation/2";
  observation_id: string;
  source_id: string;
  source_generation: string;
  scope: string;
}

/** Engine correlation authority for one Run-scoped Clock issuance. */
export interface ClockObservationResult {
  run_id: string;
  observation: ClockObservationRef;
}

export interface ClockObservation extends ClockObservationRef {
  logical_time: number;
  observed_unix_ms: number;
}

export interface ExecutionClaimRequest {
  owner: string;
  clock: ClockObservationRef;
  ttl: number;
}

export interface ContinuationExecutionClaim {
  claim_version: "cymule.continuation-execution-claim/1";
  run_id: string;
  continuation_id: string;
  owner: string;
  continuation_attempt_id: string;
  fence: number;
  plan_id: string;
  execution_binding_ref: ArtifactRef;
  clock_observation_ref: ClockObservationRef;
  logical_acquired_at: number;
  logical_ttl: number;
  logical_expires_at: number;
}

export type EffectResolution = "resolved_applied" | "resolved_not_applied";

export type DurableCommand =
  | {
      type: "start_run";
      control_version: "cymule.durable-control/4";
      run_id: string;
      candidate: PlanCandidate;
      input: Json;
      execution: ExecutionClaimRequest;
    }
  | {
      type: "resume_run";
      control_version: "cymule.durable-control/4";
      run_id: string;
      execution: ExecutionClaimRequest;
    }
  | {
      type: "takeover_run";
      control_version: "cymule.durable-control/4";
      run_id: string;
      expected_fence: number;
      execution: ExecutionClaimRequest;
    }
  | {
      type: "activate_wait";
      control_version: "cymule.durable-control/4";
      activation_id: string;
      source: WaitActivationSource;
      wait_ids: string[];
      value: Json;
    }
  | {
      type: "release_effect";
      control_version: "cymule.durable-control/4";
      intent_id: string;
      execution: ExecutionClaimRequest;
    }
  | {
      type: "resolve_effect";
      control_version: "cymule.durable-control/4";
      resolution_id: string;
      run_id: string;
      intent_id: string;
      execution_binding: ArtifactRef;
      occurrence_binding: string;
      claim_owner: string;
      claim_epoch: number;
      resolution: EffectResolution;
      value: Json;
    }
  | {
      type: "cancel_run";
      control_version: "cymule.durable-control/4";
      cancellation_id: string;
      run_id: string;
      reason: Json;
    }
  | {
      type: "run_index_page";
      control_version: "cymule.durable-control/4";
      expected_revision: string | null;
      cursor: DurablePageCursor | null;
      limit: number;
      max_canonical_bytes: number;
    }
  | {
      type: "run_current";
      control_version: "cymule.durable-control/4";
      run_id: string;
      expected_revision: string | null;
    }
  | {
      type: "run_wait_page" | "run_effect_page" | "run_occurrence_page" | "run_attempt_page";
      control_version: "cymule.durable-control/4";
      run_id: string;
      expected_revision: string | null;
      cursor: DurablePageCursor | null;
      limit: number;
      max_canonical_bytes: number;
    }
  | {
      type: "run_item";
      control_version: "cymule.durable-control/4";
      run_id: string;
      expected_revision: string | null;
      selector: DurableRunItemSelector;
      max_canonical_bytes: number;
    };

export interface EngineStoreTarget {
  provider: string;
  location: string;
  domain?: string;
}

export interface EngineProcessConfig {
  executable: string;
  arguments: string[];
  environment: Record<string, string>;
  working_directory: string | null;
  runtime_closure: Record<string, string>;
  timeout_ms: number;
  message_limit: number;
  closure_limit: number;
}

export interface EnginePluginTarget {
  provider: "cymule.executor-process/1";
  process: EngineProcessConfig;
  revision?: string;
}

export interface EngineClockTarget {
  provider: "cymule.clock-system/2";
  location: string;
  source_id: string;
  source_generation: string;
}

export interface EngineDurableTarget {
  store: EngineStoreTarget;
  executor?: EnginePluginTarget;
  clock?: EngineClockTarget;
}

export interface EngineMigrationProviderTarget {
  adapter_id: string;
  adapter_revision: string;
  process: EnginePluginTarget;
}

export interface EngineShadowProviderTarget {
  driver_id: string;
  driver_revision: string;
  process: EnginePluginTarget;
}

export interface EngineEvolutionTarget {
  store: EngineStoreTarget;
  migration_adapter: EngineMigrationProviderTarget | null;
  shadow_driver: EngineShadowProviderTarget | null;
  target_execution_bindings: Record<string, EnginePluginTarget>;
}

export const directoryStore = (location: string): EngineStoreTarget => ({
  provider: "cymule.directory-store/5",
  location,
});

export const sqliteStore = (location: string, domain: string): EngineStoreTarget => ({
  provider: "cymule.sqlite-store/6",
  location,
  domain,
});

export const processPlugin = (
  process: EngineProcessConfig,
  revision?: string,
): EnginePluginTarget => {
  try {
    assertStrictJson(process);
    const target: EnginePluginTarget = {
      provider: "cymule.executor-process/1",
      process: structuredClone(process),
      ...(revision === undefined ? {} : { revision }),
    };
    validateEnginePluginTarget(target, false);
    return target;
  } catch (error) {
    throw new Error(`process plugin target is invalid: ${errorMessage(error)}`);
  }
};

export const sqliteClock = (
  location: string,
  sourceId: string,
  sourceGeneration: string,
): EngineClockTarget => ({
  provider: "cymule.clock-system/2",
  location,
  source_id: sourceId,
  source_generation: sourceGeneration,
});

export interface DurableFrameState {
  definition_id: string;
  invocation_id: string;
  invocation_path: Array<{ site_id: string; region_path: number[]; scope_id: string }>;
  scope_id: string;
  input: ArtifactRef;
  region_path: number[];
  next_step: number;
  locals: Record<string, ArtifactRef>;
}

export interface DurableContinuation {
  continuation_version: "cymule.continuation-state/1";
  run_id: string;
  plan_id: string;
  binding_context: string;
  frames: DurableFrameState[];
  state: ArtifactRef | null;
  wait_set: string[];
  scope_stack: string[];
  epoch: number;
  execution_fence: number;
  execution_claim: ContinuationExecutionClaim | null;
  status: "ready" | "waiting" | "running" | "completed" | "failed" | "cancelled";
}

export interface ComponentOccurrence {
  occurrence_version: "cymule.component-occurrence/4";
  occurrence_id: string; run_id: string; plan_id: string; binding_context: string;
  invocation_id: string; invocation_path: MigrationInvocationPathSegment[];
  definition_id: string; region_path: number[]; site_id: string; step_index: number;
  component: string; input: ArtifactRef; outcome: ComponentOutcome | null;
  occurrence_binding: string; implementation_revision: string;
  attempt_count: number; latest_attempt_id: string;
  continuation_digest: string | null; state: "pending" | "completed";
}

export interface OperationAttempt {
  attempt_version: "cymule.operation-attempt/2";
  attempt_id: string; occurrence_id: string; run_id: string; attempt_ordinal: number;
  previous_attempt_id: string | null; continuation_attempt_id: string;
  execution_claim_owner: string; execution_claim_fence: number;
  operation_occurrence_binding: string;
  transport_request_id: string; state: "running" | "completed" | "superseded";
  outcome: ComponentOutcome | null;
}

export type ComponentOutcome =
  | { outcome: "succeeded"; output: ArtifactRef }
  | { outcome: "expected_failure"; code: string; detail: ArtifactRef };

export interface DurableWaitCondition {
  wait_id: string;
  run_id: string;
  kind: { kind: "signal"; key: string } | { kind: "timer"; timer_id: string } |
    { kind: "input"; correlation: string; schema: Schema };
  consume_once: boolean;
  owner: {
    invocation_id: string; definition_id: string; site_id: string;
    region_path: number[]; step_index: number; bind: string | null;
  };
  state: "pending" | "completed" | "cancelled";
  result: ArtifactRef | null;
}

export interface DurableEffectDispatch {
  intent_id: string;
  run_id: string;
  origin_plan_id: string;
  operation: string;
  input: ArtifactRef;
  execution_binding: ArtifactRef;
  occurrence_binding: string;
  execution_availability: "available" | "unavailable";
  state: "pending" | "claimed" | "applied" | "not_applied" | "unknown" |
    "cancelled_before_release";
  reconciliation: "not_required" | "pending" | "resolved" | "governance_required";
  claim_epoch: number;
  claim_owner: string | null;
  result: ArtifactRef | null;
}

export type DurablePageQueryKind =
  | "run_index"
  | "run_waits"
  | "run_effects"
  | "run_occurrences"
  | "run_attempts";

export interface DurablePagePosition {
  canonical_key: string;
  key_hash: string;
}

export interface DurablePageCursor {
  query_kind: DurablePageQueryKind;
  run_id: string | null;
  source_revision: string;
  source_root: string;
  position: DurablePagePosition;
}

export interface DurableQueryPage<Item> {
  observed_revision: string;
  source_root: string;
  items: Item[];
  next_cursor: DurablePageCursor | null;
}

export type DurableContinuationStatus = DurableContinuation["status"];
export type WorldSettlementStatus = "settled" | "pending" | "unknown" | "governance_required";

export interface DurableRunIndexSummary {
  run_id: string;
  continuation_status: DurableContinuationStatus;
  execution_status: RunExecutionStatus;
  world_settlement: WorldSettlementStatus;
}

export interface DurableRunCurrent {
  run_id: string;
  plan_id: string;
  execution_binding: ArtifactRef;
  continuation_status: DurableContinuationStatus;
  epoch: number;
  execution_fence: number;
  result: ArtifactRef | null;
  execution_status: RunExecutionStatus;
  world_settlement: WorldSettlementStatus;
}

export interface DurableWaitSummary {
  wait_id: string;
  run_id: string;
  state: DurableWaitCondition["state"];
  result: ArtifactRef | null;
}

export interface DurableEffectSummary {
  intent_id: string;
  run_id: string;
  state: DurableEffectDispatch["state"];
  execution_availability: DurableEffectDispatch["execution_availability"];
  reconciliation: DurableEffectDispatch["reconciliation"];
  result: ArtifactRef | null;
}

export interface DurableOccurrenceSummary {
  occurrence_id: string;
  run_id: string;
  state: ComponentOccurrence["state"];
  outcome: ComponentOutcome | null;
}

export interface DurableAttemptSummary {
  attempt_id: string;
  occurrence_id: string;
  run_id: string;
  attempt_ordinal: number;
  state: OperationAttempt["state"];
  outcome: ComponentOutcome | null;
}

export type DurableRunItemSelector =
  | { kind: "wait"; wait_id: string }
  | { kind: "effect"; intent_id: string }
  | { kind: "occurrence"; occurrence_id: string }
  | { kind: "attempt"; attempt_id: string };

export type DurableRunItem =
  | { kind: "wait"; wait: DurableWaitCondition }
  | { kind: "effect"; effect: DurableEffectDispatch }
  | { kind: "occurrence"; occurrence: ComponentOccurrence }
  | { kind: "attempt"; attempt: OperationAttempt };

export interface RunFailure {
  class: "declared_failure" | "runtime_defect" | "substrate";
  code: string;
  detail: ArtifactRef;
}

export type RunExecutionStatus =
  | { status: "active" }
  | { status: "completed" }
  | { status: "failed"; failure: RunFailure }
  | { status: "cancelled"; reason: ArtifactRef };

export type DurableBoundary =
  | { status: "suspended"; wait_id: string }
  | { status: "reconciliation_required"; intent_id: string }
  | { status: "effect_unavailable"; intent_id: string }
  | { status: "effect_not_applied"; intent_id: string }
  | { status: "release_required"; intent_ids: string[] }
  | { status: "completed"; result: ExecutionResult }
  | { status: "failed"; failure: RunFailure }
  | { status: "cancelled"; reason: ArtifactRef };

export type DurableResponse =
  | { type: "run_boundary"; boundary: DurableBoundary }
  | { type: "wait_activated"; receipt: WaitActivationReceipt }
  | { type: "effect_resolved"; receipt: EffectResolutionReceipt }
  | { type: "run_cancelled"; receipt: RunCancellationReceipt }
  | { type: "run_index_page"; page: DurableQueryPage<DurableRunIndexSummary> }
  | {
      type: "run_current";
      observed_revision: string;
      source_root: string;
      current: DurableRunCurrent | null;
    }
  | { type: "run_wait_page"; run_id: string; page: DurableQueryPage<DurableWaitSummary> }
  | { type: "run_effect_page"; run_id: string; page: DurableQueryPage<DurableEffectSummary> }
  | {
      type: "run_occurrence_page";
      run_id: string;
      page: DurableQueryPage<DurableOccurrenceSummary>;
    }
  | { type: "run_attempt_page"; run_id: string; page: DurableQueryPage<DurableAttemptSummary> }
  | {
      type: "run_item";
      run_id: string;
      observed_revision: string;
      source_root: string;
      item: DurableRunItem | null;
    };

export interface WaitActivationReceipt {
  receipt_version: "cymule.wait-activation-receipt/3";
  activation: WaitActivation;
  applied_wait_ids: string[];
  ready_run_ids: string[];
}

export interface EffectResolutionCommand {
  resolution_id: string;
  run_id: string;
  intent_id: string;
  execution_binding: ArtifactRef;
  occurrence_binding: string;
  claim_owner: string;
  claim_epoch: number;
  resolution: EffectResolution;
  value: Json;
}

export interface EffectResolutionReceipt {
  receipt_version: "cymule.effect-resolution-receipt/1";
  command: EffectResolutionCommand;
  actual_resolution: EffectResolution;
  actual_value: Json;
  result: ArtifactRef | null;
  receipt_id: string;
}

export interface CancellationCommand {
  cancellation_id: string;
  run_id: string;
  reason: Json;
}

export interface RunCancellationReceipt {
  receipt_version: "cymule.run-cancellation-receipt/1";
  command: CancellationCommand;
  boundary: Extract<DurableBoundary, { status: "cancelled" }>;
  receipt_id: string;
}

export type LiveEvolutionOutcome =
  | { result: "definition_published"; revision: Record<string, unknown> }
  | { result: "template_registered"; linked: Record<string, unknown> }
  | { result: "publication_applied"; receipt: Record<string, unknown> }
  | { result: "patch_applied"; edge: Record<string, unknown> }
  | { result: "applied" }
  | { result: "occurrence_selected"; pin: OccurrencePin }
  | { result: "migrated"; receipt: Record<string, unknown> }
  | { result: "restart_authorized"; receipt: Record<string, unknown> }
  | { result: "shadow_recorded"; comparison: Record<string, unknown> }
  | { result: "gate_applied"; transition: Record<string, unknown> };

export type EvolutionStateFamily =
  | "definition_current"
  | "definition_compatibility_current"
  | "definition_record"
  | "dependency_current"
  | "template_current"
  | "link_record"
  | "plan_record"
  | "edge_record"
  | "rollout_current"
  | "rollout_evidence_current"
  | "rollout_decision"
  | "occurrence_current"
  | "selection_current"
  | "migration_record"
  | "restart_record"
  | "shadow_record"
  | "shadow_subject_current"
  | "observation_record"
  | "observation_occurrence_current"
  | "evidence_current"
  | "decision_transition_current"
  | "transition_record";

export interface EvolutionMutationWrite {
  family: EvolutionStateFamily;
  storage_key: string;
  value_id: string;
}

export interface EvolutionPersistenceCommand {
  persistence_version: "cymule.evolution-persistence-command/4";
  persistence_id: string;
  evolution_id: string;
  command: LiveEvolutionCommand;
}

export interface EvolutionPersistenceReceipt {
  receipt_version: "cymule.evolution-persistence-receipt/4";
  receipt_id: string;
  command: EvolutionPersistenceCommand;
  parent_current_id: string | null;
  source_witness_id: string | null;
  outcome: LiveEvolutionOutcome;
  mutations: EvolutionMutationWrite[];
  mutation_id: string;
}

export interface EvolutionCommit {
  observed_revision: string;
  committed_revision: string | null;
  receipt: EvolutionPersistenceReceipt;
}

export type ParkReason =
  | { kind: "wait"; key: string }
  | { kind: "dependency"; work_id: string }
  | { kind: "budget"; account: string }
  | { kind: "capability"; capability: string }
  | { kind: "backpressure"; domain: string };

export type WorkOccurrenceState =
  | "running"
  | "succeeded"
  | "retry_scheduled"
  | "parked"
  | "failed"
  | "cancelled";

export interface WorkItem {
  work_id: string;
  region_id: string;
  run_id: string;
  payload: ArtifactRef;
  capability: string | null;
  priority: number;
  cost: number;
}

export interface VirtualClaimLease {
  resource: string;
  owner: string;
  epoch: number;
  expires_at: number;
}

export interface ClaimedWork {
  item: WorkItem;
  owner: string;
  epoch: number;
  occurrence_id: string;
  plan_id: string;
  execution_binding: ArtifactRef;
  lease: VirtualClaimLease;
}

export interface WorkOccurrence {
  occurrence_version: "cymule.virtual-work-occurrence/3";
  occurrence_id: string;
  work_id: string;
  region_id: string;
  run_id: string;
  owner: string;
  epoch: number;
  lease_epoch: number;
  plan_id: string;
  execution_binding: ArtifactRef;
  state: WorkOccurrenceState;
  result: ArtifactRef | null;
  error: ArtifactRef | null;
  next_reason: ParkReason | null;
}

export type WorkResolution =
  | { resolution: "succeeded"; result: ArtifactRef }
  | { resolution: "retry"; error: ArtifactRef; next_reason: ParkReason | null }
  | { resolution: "parked"; reason: ParkReason }
  | { resolution: "failed"; error: ArtifactRef }
  | { resolution: "cancelled"; reason: ArtifactRef };

export interface WorkResolutionCommand {
  control_version: "cymule.virtual-work-control/2";
  command_id: string;
  work_id: string;
  owner: string;
  epoch: number;
  expected_lease_epoch: number;
  clock: ClockObservationRef;
  resolution: WorkResolution;
}

export interface VirtualCursor {
  version: string;
  position: string;
  exhausted: boolean;
}

export interface RegionSourceBinding {
  operation: string;
  binding: string;
  revision: string;
}

export interface RegionSourceCheckpoint {
  source: RegionSourceBinding;
  cursor: VirtualCursor;
}

export interface VirtualRegion {
  region_id: string;
  run_id: string;
  source: RegionSourceBinding;
  source_artifact: ArtifactRef;
  cursor: VirtualCursor;
  estimated_total: number | null;
}

export type RegionMigrationKind = "split" | "merge";

export interface RegionMigrationRequest {
  migration_id: string;
  kind: RegionMigrationKind;
  source_region_ids: string[];
  target_count: number;
  migration_binding: string;
  migration_revision: string;
}

export interface RegionMigrationPlan {
  migration_version: "cymule.virtual-region-migration/3";
  migration_id: string;
  kind: RegionMigrationKind;
  expected_sources: Record<string, RegionSourceCheckpoint>;
  targets: VirtualRegion[];
  migration_binding: string;
  migration_revision: string;
  coverage_evidence: ArtifactRef;
}

export interface RegionMigrationReceipt {
  plan: RegionMigrationPlan;
  retired_regions: string[];
  active_targets: string[];
}

export interface RegionMigrationCommand {
  control_version: "cymule.virtual-region-migration-control/3";
  command_id: string;
  plan: RegionMigrationPlan;
}

export type ReplayAvailability =
  | { status: "exact" }
  | { status: "projection_only"; missing: string[] }
  | { status: "unavailable"; reason: string };

export interface VirtualCompletionSummary {
  region_id: string;
  run_id: string;
  occurrence_count: number;
  work_count: number;
  succeeded_count: number;
  failed_count: number;
  cancelled_count: number;
  output_digest: string;
  evidence_digest: string;
  retained_debug_index_digest: string;
}

export interface VirtualArchiveBinding {
  binding: string;
  revision: string;
}

export interface VirtualCompactionCertificate {
  certificate_version: "cymule.virtual-compaction-certificate/4";
  certificate_id: string;
  source_causal_cut: string[];
  summary: VirtualCompletionSummary;
  summary_state_digest: string;
  occurrence_root_digest: string;
  parent_work_index_root_digest: string;
  work_index_updates_digest: string;
  work_index_root_digest: string;
  command_root_digest: string | null;
  command_count: number;
  unresolved_obligations: string[];
  retained_execution_bindings: ArtifactRef[];
  replay_availability: ReplayAvailability;
  rehydration_manifest: ResourceHandle;
  archive: VirtualArchiveBinding;
}

export interface VirtualCompactionCommand {
  control_version: "cymule.virtual-compaction-control/1";
  command_id: string;
  region_id: string;
  source_causal_cut: string[];
  work_ids: string[];
  occurrence_ids: string[];
  archived_command_ids: string[];
  archive: VirtualArchiveBinding;
}

export interface VirtualRehydrationCommand {
  control_version: "cymule.virtual-rehydration-control/1";
  command_id: string;
  certificate_id: string;
  occurrence_ids: string[];
}

export interface VirtualRehydrationReceipt {
  command: VirtualRehydrationCommand;
  restored_occurrence_ids: string[];
}

export interface VirtualClaimCommand {
  control_version: "cymule.virtual-claim-control/4";
  command_id: string;
  owner: string;
  slot_id: string;
  execution_binding: ArtifactRef;
  capabilities: string[];
  clock: ClockObservationRef;
  lease_ttl: number;
}

export interface VirtualLeaseRenewalCommand {
  control_version: "cymule.virtual-lease-renewal-control/2";
  command_id: string;
  work_id: string;
  owner: string;
  epoch: number;
  expected_lease_epoch: number;
  clock: ClockObservationRef;
  lease_ttl: number;
}

export interface VirtualLeaseRenewalReceipt {
  command: VirtualLeaseRenewalCommand;
  clock_observation: ClockObservation;
  lease: VirtualClaimLease;
}

export interface VirtualRecoveryCommand {
  control_version: "cymule.virtual-recovery-control/2";
  command_id: string;
  work_id: string;
  expected_owner: string;
  expected_epoch: number;
  expected_lease_epoch: number;
  clock: ClockObservationRef;
  resolution: Extract<WorkResolution, { resolution: "retry" | "failed" | "cancelled" }>;
}

export interface VirtualRecoveryReceipt {
  command: VirtualRecoveryCommand;
  clock_observation: ClockObservation;
  occurrence: WorkOccurrence;
}

export interface VirtualRunWeightCommand {
  control_version: "cymule.virtual-run-weight-control/1";
  command_id: string;
  run_id: string;
  weight: number;
}

export interface VirtualRunWeightReceipt {
  command: VirtualRunWeightCommand;
  previous_weight: number;
  current_weight: number;
}

export type Expression =
  | { kind: "input" }
  | { kind: "literal"; value: Json }
  | { kind: "binding"; name: string }
  | { kind: "object"; fields: Record<string, Expression> }
  | { kind: "array"; items: Expression[] };

export type WaitSpec =
  | { kind: "signal"; key: string; consume_once: boolean }
  | { kind: "timer"; timer_id: string }
  | { kind: "input"; correlation: string; schema: Schema };

export interface Region {
  steps: Step[];
  result: Expression;
}

export interface EffectProfile {
  mutation: "observational" | "mutating";
  dispatch: "eager" | "on_scope_commit" | "explicit";
  reconciliation: "queryable" | "externally_attested" | "human" | "impossible";
  keyed_idempotency: boolean;
  irreversible: boolean;
}

export interface PlanCandidate {
  ir_version: "cymule.ir/3";
  name: string;
  entry: string;
  components: Array<{
    id: string;
    input_schema: Schema;
    output_schema: Schema;
    output_artifact_kind: string;
    requirements: Record<string, string>;
  }>;
  effects: Array<{
    id: string;
    input_schema: Schema;
    output_schema: Schema;
    profile: EffectProfile;
    requirements: Record<string, string>;
  }>;
  definitions: Array<{
    id: string;
    input_schema: Schema;
    output_schema: Schema;
    body: { steps: Step[]; result: Expression };
  }>;
  metadata: Record<string, string>;
}

export type Step =
  | { id: string; op: "call"; component: string; input: Expression; bind: string }
  | { id: string; op: "invoke"; definition: string; input: Expression; bind: string }
  | { id: string; op: "wait"; wait: WaitSpec; bind?: string }
  | { id: string; op: "effect"; effect: string; input: Expression; occurrence: string; bind?: string }
  | {
      id: string;
      op: "scope";
      body: Region;
      bind: string;
    };

export interface SealedPlan {
  plan_id: string;
  candidate: PlanCandidate;
}

export interface ExecutionResult {
  run_id: string;
  plan_id: string;
  value: Json;
  projection_digest: string;
  precondition_token: string;
  effects: string[];
}

export interface SuspensionBoundary {
  run_id: string;
  plan_id: string;
  definition_id: string;
  invocation_id: string;
  site_id: string;
  wait: WaitSpec;
  result_bind: string | null;
}

export interface EffectReleaseBoundary {
  run_id: string;
  plan_id: string;
  intent_ids: string[];
}

export interface EffectReconciliationBoundary {
  run_id: string;
  plan_id: string;
  intent_id: string;
}

export type ExecutionOutcome =
  | { status: "completed"; result: ExecutionResult }
  | { status: "suspended"; suspension: SuspensionBoundary }
  | { status: "release_required"; release: EffectReleaseBoundary }
  | {
      status: "reconciliation_required";
      reconciliation: EffectReconciliationBoundary;
    };

export class FlowBuilder {
  readonly #candidate: PlanCandidate;

  constructor(name: string, inputSchema: Schema, outputSchema: Schema) {
    this.#candidate = {
      ir_version: "cymule.ir/3",
      name,
      entry: "main",
      components: [],
      effects: [],
      definitions: [{
        id: "main",
        input_schema: inputSchema,
        output_schema: outputSchema,
        body: { steps: [], result: { kind: "literal", value: null } },
      }],
      metadata: {},
    };
  }

  component(
    id: string,
    inputSchema: Schema,
    outputSchema: Schema,
    outputArtifactKind: string,
    requirements: Record<string, string>,
  ): this {
    this.#candidate.components.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
      output_artifact_kind: outputArtifactKind,
      requirements: structuredClone(requirements),
    });
    return this;
  }

  effectContract(
    id: string,
    inputSchema: Schema,
    outputSchema: Schema,
    profile: EffectProfile,
    requirements: Record<string, string>,
  ): this {
    this.#candidate.effects.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
      profile,
      requirements: structuredClone(requirements),
    });
    return this;
  }

  call(site: string, component: string, input: Expression, bind: string): this {
    this.entry().body.steps.push({ id: site, op: "call", component, input, bind });
    return this;
  }

  definition(
    id: string,
    inputSchema: Schema,
    outputSchema: Schema,
    body: Region,
  ): this {
    this.#candidate.definitions.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
      body,
    });
    return this;
  }

  invoke(site: string, definition: string, input: Expression, bind: string): this {
    this.entry().body.steps.push({ id: site, op: "invoke", definition, input, bind });
    return this;
  }

  effect(site: string, effect: string, input: Expression, occurrence: string, bind?: string): this {
    this.entry().body.steps.push({
      id: site,
      op: "effect",
      effect,
      input,
      occurrence,
      ...(bind === undefined ? {} : { bind }),
    });
    return this;
  }

  wait(site: string, wait: WaitSpec, bind?: string): this {
    this.entry().body.steps.push({ id: site, op: "wait", wait, ...(bind === undefined ? {} : { bind }) });
    return this;
  }

  scope(
    site: string,
    body: Region,
    bind: string,
  ): this {
    this.entry().body.steps.push({ id: site, op: "scope", body, bind });
    return this;
  }

  finish(result: Expression): PlanCandidate {
    this.entry().body.result = result;
    return structuredClone(this.#candidate);
  }

  private entry(): PlanCandidate["definitions"][number] {
    const definition = this.#candidate.definitions[0];
    if (definition === undefined) throw new Error("entry definition is missing");
    return definition;
  }
}

export class WaitActivationBuilder {
  static signal(
    activationId: string,
    key: string,
    waitIds: string[],
    result: ArtifactRef,
  ): WaitActivation {
    return WaitActivationBuilder.build(activationId, { kind: "signal", key }, waitIds, result);
  }

  static timer(
    activationId: string,
    timerId: string,
    waitId: string,
    result: ArtifactRef,
  ): WaitActivation {
    return WaitActivationBuilder.build(
      activationId,
      { kind: "timer", timer_id: timerId },
      [waitId],
      result,
    );
  }

  private static build(
    activationId: string,
    source: WaitActivationSource,
    waitIds: string[],
    result: ArtifactRef,
  ): WaitActivation {
    const targets = [...new Set(waitIds)].sort(compareWireStrings);
    if (activationId.length === 0 || targets.length === 0) {
      throw new Error("wait activation requires an identity and at least one target");
    }
    const activation: WaitActivation = {
      activation_version: "cymule.wait-activation/2",
      activation_id: activationId,
      source,
      wait_ids: targets,
      result,
    };
    validateWaitActivation(activation);
    return activation;
  }
}

export interface DurablePageQueryOptions {
  expected_revision: string | null;
  cursor: DurablePageCursor | null;
  limit: number;
  max_canonical_bytes: number;
}

export interface DurableRunItemQuery {
  run_id: string;
  expected_revision: string | null;
  selector: DurableRunItemSelector;
  max_canonical_bytes: number;
}

export class DurableControlBuilder {
  static startRun(
    runId: string,
    candidate: PlanCandidate,
    input: Json,
    execution: ExecutionClaimRequest,
  ): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    DurableControlBuilder.execution(execution);
    return {
      type: "start_run",
      control_version: "cymule.durable-control/4",
      run_id: runId,
      candidate: structuredClone(candidate),
      input: structuredClone(input),
      execution: structuredClone(execution),
    };
  }

  static resumeRun(runId: string, execution: ExecutionClaimRequest): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    DurableControlBuilder.execution(execution);
    return {
      type: "resume_run",
      control_version: "cymule.durable-control/4",
      run_id: runId,
      execution: structuredClone(execution),
    };
  }

  static takeoverRun(
    runId: string,
    expectedFence: number,
    execution: ExecutionClaimRequest,
  ): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    DurableControlBuilder.execution(execution);
    if (!Number.isSafeInteger(expectedFence) || expectedFence < 1) {
      throw new Error("takeover expected fence must be a positive safe integer");
    }
    return {
      type: "takeover_run",
      control_version: "cymule.durable-control/4",
      run_id: runId,
      expected_fence: expectedFence,
      execution: structuredClone(execution),
    };
  }

  static activateSignal(
    activationId: string,
    key: string,
    waitIds: string[],
    value: Json,
  ): DurableCommand {
    return DurableControlBuilder.activate(
      activationId,
      { kind: "signal", key },
      waitIds,
      value,
    );
  }

  static activateTimer(
    activationId: string,
    timerId: string,
    waitId: string,
    value: Json,
  ): DurableCommand {
    return DurableControlBuilder.activate(
      activationId,
      { kind: "timer", timer_id: timerId },
      [waitId],
      value,
    );
  }

  static releaseEffect(intentId: string, execution: ExecutionClaimRequest): DurableCommand {
    if (!isContentId(intentId)) {
      throw new Error("effect intent must be a lowercase SHA-256 content ID");
    }
    DurableControlBuilder.execution(execution);
    return {
      type: "release_effect",
      control_version: "cymule.durable-control/4",
      intent_id: intentId,
      execution: structuredClone(execution),
    };
  }

  static resolveEffect(
    resolutionId: string,
    runId: string,
    intentId: string,
    executionBinding: ArtifactRef,
    occurrenceBinding: string,
    claimOwner: string,
    claimEpoch: number,
    resolution: EffectResolution,
    value: Json,
  ): DurableCommand {
    for (const [kind, identity] of [
      ["effect resolution", resolutionId],
      ["Run", runId],
      ["effect intent", intentId],
      ["effect occurrence binding", occurrenceBinding],
      ["effect claim owner", claimOwner],
    ] as const) {
      DurableControlBuilder.identity(kind, identity);
    }
    if (executionBinding.identity_version !== "cymule.artifact/2"
      || !isContentId(executionBinding.artifact_id)
      || executionBinding.kind !== "cymule.execution-binding/2"
      || !isContentId(intentId)
      || !isContentId(occurrenceBinding)
      || !Number.isSafeInteger(claimEpoch) || claimEpoch < 1
      || !new Set<EffectResolution>([
        "resolved_applied",
        "resolved_not_applied",
      ]).has(resolution)) {
      throw new Error("Effect resolution authority is invalid");
    }
    return {
      type: "resolve_effect",
      control_version: "cymule.durable-control/4",
      resolution_id: resolutionId,
      run_id: runId,
      intent_id: intentId,
      execution_binding: structuredClone(executionBinding),
      occurrence_binding: occurrenceBinding,
      claim_owner: claimOwner,
      claim_epoch: claimEpoch,
      resolution,
      value: structuredClone(value),
    };
  }

  static cancelRun(cancellationId: string, runId: string, reason: Json): DurableCommand {
    DurableControlBuilder.identity("cancellation", cancellationId);
    DurableControlBuilder.identity("Run", runId);
    return {
      type: "cancel_run",
      control_version: "cymule.durable-control/4",
      cancellation_id: cancellationId,
      run_id: runId,
      reason: structuredClone(reason),
    };
  }

  static runIndexPage(options: DurablePageQueryOptions): DurableCommand {
    DurableControlBuilder.pageQuery("run_index", null, options);
    return {
      type: "run_index_page",
      control_version: "cymule.durable-control/4",
      ...structuredClone(options),
    };
  }

  static runCurrent(runId: string, expectedRevision: string | null): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    DurableControlBuilder.expectedRevision(expectedRevision);
    return {
      type: "run_current",
      control_version: "cymule.durable-control/4",
      run_id: runId,
      expected_revision: expectedRevision,
    };
  }

  static runWaitPage(runId: string, options: DurablePageQueryOptions): DurableCommand {
    return DurableControlBuilder.runPage("run_wait_page", "run_waits", runId, options);
  }

  static runEffectPage(runId: string, options: DurablePageQueryOptions): DurableCommand {
    return DurableControlBuilder.runPage("run_effect_page", "run_effects", runId, options);
  }

  static runOccurrencePage(runId: string, options: DurablePageQueryOptions): DurableCommand {
    return DurableControlBuilder.runPage(
      "run_occurrence_page",
      "run_occurrences",
      runId,
      options,
    );
  }

  static runAttemptPage(runId: string, options: DurablePageQueryOptions): DurableCommand {
    return DurableControlBuilder.runPage("run_attempt_page", "run_attempts", runId, options);
  }

  static runItem(query: DurableRunItemQuery): DurableCommand {
    DurableControlBuilder.identity("Run", query.run_id);
    DurableControlBuilder.expectedRevision(query.expected_revision);
    validateDurableRunItemSelector(query.selector);
    if (!Number.isSafeInteger(query.max_canonical_bytes)
      || query.max_canonical_bytes < 1
      || query.max_canonical_bytes > 13 * 1024 * 1024) {
      throw new Error("exact Run item byte budget must be within 1..=13631488");
    }
    return {
      type: "run_item",
      control_version: "cymule.durable-control/4",
      ...structuredClone(query),
    };
  }

  private static activate(
    activationId: string,
    source: WaitActivationSource,
    waitIds: string[],
    value: Json,
  ): DurableCommand {
    DurableControlBuilder.identity("activation", activationId);
    const targets = [...new Set(waitIds)].sort(compareWireStrings);
    if (targets.length === 0 || targets.some((target) => !isContentId(target))) {
      throw new Error("durable activation requires at least one wait identity");
    }
    const command: DurableCommand = {
      type: "activate_wait",
      control_version: "cymule.durable-control/4",
      activation_id: activationId,
      source,
      wait_ids: targets,
      value: structuredClone(value),
    };
    validateDurableCommand(command);
    return command;
  }

  private static identity(kind: string, value: string): void {
    if (!isDurableIdentity(value)) {
      throw new Error(`durable ${kind} identity must contain 1..=512 Unicode scalars without controls`);
    }
  }

  private static expectedRevision(value: string | null): void {
    if (value !== null && !isContentId(value)) {
      throw new Error("durable query expected revision must be a content ID or null");
    }
  }

  private static pageQuery(
    queryKind: DurablePageQueryKind,
    runId: string | null,
    options: DurablePageQueryOptions,
  ): void {
    DurableControlBuilder.expectedRevision(options.expected_revision);
    if (!Number.isSafeInteger(options.limit) || options.limit < 1 || options.limit > 256) {
      throw new Error("durable query page limit must be within 1..=256");
    }
    if (!Number.isSafeInteger(options.max_canonical_bytes)
      || options.max_canonical_bytes < 1
      || options.max_canonical_bytes > 1024 * 1024) {
      throw new Error("durable query page byte budget must be within 1..=1048576");
    }
    if (options.cursor !== null) {
      validateDurablePageCursor(options.cursor);
      if (options.cursor.query_kind !== queryKind
        || options.cursor.run_id !== runId
        || options.expected_revision !== options.cursor.source_revision) {
        throw new Error("durable query cursor belongs to a different query authority");
      }
    }
  }

  private static runPage(
    type: "run_wait_page" | "run_effect_page" | "run_occurrence_page" | "run_attempt_page",
    queryKind: DurablePageQueryKind,
    runId: string,
    options: DurablePageQueryOptions,
  ): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    DurableControlBuilder.pageQuery(queryKind, runId, options);
    return {
      type,
      control_version: "cymule.durable-control/4",
      run_id: runId,
      ...structuredClone(options),
    };
  }

  private static execution(value: ExecutionClaimRequest): void {
    if (Object.keys(value).sort().join(",") !== "clock,owner,ttl") {
      throw new Error("execution claim request is not closed");
    }
    DurableControlBuilder.identity("execution owner", value.owner);
    validateClockObservationRef(value.clock);
    if (
      !Number.isSafeInteger(value.ttl) ||
      value.ttl < 1
    ) {
      throw new Error("execution claim Clock observation or TTL is invalid");
    }
  }
}

export class EvolutionControlBuilder {
  static applyPatch(commandId: string, patch: PlanPatch): EvolutionCommandFor<"apply_patch"> {
    return EvolutionControlBuilder.build(commandId, { operation: "apply_patch", patch });
  }

  static setRollout(commandId: string, decision: RolloutDecision): EvolutionCommandFor<"set_rollout"> {
    return EvolutionControlBuilder.build(commandId, { operation: "set_rollout", decision });
  }

  static selectOccurrence(
    commandId: string,
    occurrenceId: string,
    selectionId: string,
    executionBinding: ArtifactRef,
  ): EvolutionCommandFor<"select_occurrence"> {
    return EvolutionControlBuilder.build(commandId, {
      operation: "select_occurrence",
      occurrence_id: occurrenceId,
      selection_id: selectionId,
      execution_binding: structuredClone(executionBinding),
    });
  }

  static migrate(commandId: string, request: MigrationRequest): EvolutionCommandFor<"migrate"> {
    return EvolutionControlBuilder.build(commandId, { operation: "migrate", request });
  }

  static restartUnderNewPlan(
    commandId: string,
    request: RestartRequest,
  ): EvolutionCommandFor<"restart_under_new_plan"> {
    return EvolutionControlBuilder.build(commandId, {
      operation: "restart_under_new_plan",
      request,
    });
  }

  static shadow(commandId: string, request: ShadowRequest): EvolutionCommandFor<"shadow"> {
    return EvolutionControlBuilder.build(commandId, { operation: "shadow", request });
  }

  static observe(commandId: string, observation: RolloutObservation): EvolutionCommandFor<"observe"> {
    return EvolutionControlBuilder.build(commandId, { operation: "observe", observation });
  }

  static applyGate(
    commandId: string,
    gate: RolloutGate,
    nextDecisionId: string,
  ): EvolutionCommandFor<"apply_gate"> {
    if (nextDecisionId.length === 0) throw new Error("evolution gate requires a next decision ID");
    return EvolutionControlBuilder.build(commandId, {
      operation: "apply_gate",
      gate,
      next_decision_id: nextDecisionId,
    });
  }

  private static build<Operation extends EvolutionOperation>(
    commandId: string,
    operation: Operation,
  ): { control_version: "cymule.evolution-control/5"; command_id: string } & Operation {
    if (commandId.length === 0) throw new Error("evolution control requires a command identity");
    return {
      control_version: "cymule.evolution-control/5",
      command_id: commandId,
      ...structuredClone(operation),
    };
  }
}

export class LiveEvolutionControlBuilder {
  static publishDefinition(
    commandId: string,
    logicalRef: string,
    definition: PlanCandidate["definitions"][number],
    references: SubflowReference[],
  ): LiveEvolutionCommand {
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "publish_definition",
      logical_ref: logicalRef,
      definition: structuredClone(definition),
      references: structuredClone(references),
    });
  }

  static registerTemplate(commandId: string, template: PlanTemplate): LiveEvolutionCommand {
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "register_template",
      template: structuredClone(template),
    });
  }

  static publishAndRelink(
    commandId: string,
    publication: LivePublicationCommand,
  ): LiveEvolutionCommand {
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "publish_and_relink",
      publication: structuredClone(publication),
    });
  }

  static apply(
    commandId: string,
    templateId: string,
    command: EvolutionCommand,
  ): LiveEvolutionCommand {
    if (templateId.length === 0) throw new Error("live evolution requires a template identity");
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "apply",
      template_id: templateId,
      command: structuredClone(command),
    });
  }

  private static build(
    commandId: string,
    operation: LiveEvolutionOperation,
  ): LiveEvolutionCommand {
    if (commandId.length === 0) {
      throw new Error("live-evolution control requires a command identity");
    }
    return {
      control_version: "cymule.live-evolution-control/6",
      command_id: commandId,
      ...operation,
    };
  }
}

function validateVirtualParkReason(value: unknown): void {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new Error("virtual park reason is not an object with a closed kind");
  }
  const fields: Record<string, string> = {
    wait: "key", dependency: "work_id", budget: "account",
    capability: "capability", backpressure: "domain",
  };
  if (!Object.hasOwn(fields, value.kind)) {
    throw new Error("virtual park reason kind is unsupported");
  }
  const field = fields[value.kind]!;
  requireClosedRecord(value, ["kind", field], "virtual park reason");
  if (!isDurableIdentity(value[field])) {
    throw new Error("virtual park reason identity is invalid");
  }
}

function validateVirtualWorkResolution(value: unknown, recovery = false): void {
  if (!isRecord(value) || typeof value.resolution !== "string") {
    throw new Error("virtual work resolution is not an object with a closed kind");
  }
  if (recovery && !["retry", "failed", "cancelled"].includes(value.resolution)) {
    throw new Error("virtual recovery accepts only retry, failure, or cancellation");
  }
  switch (value.resolution) {
    case "succeeded":
      requireClosedRecord(value, ["resolution", "result"], "virtual work success");
      validateArtifactRef(value.result);
      break;
    case "retry":
      requireClosedRecord(value, ["resolution", "error", "next_reason"], "virtual work retry");
      validateArtifactRef(value.error);
      if (value.next_reason !== null) validateVirtualParkReason(value.next_reason);
      break;
    case "parked":
      requireClosedRecord(value, ["resolution", "reason"], "virtual work park");
      validateVirtualParkReason(value.reason);
      break;
    case "failed":
      requireClosedRecord(value, ["resolution", "error"], "virtual work failure");
      validateArtifactRef(value.error);
      break;
    case "cancelled":
      requireClosedRecord(value, ["resolution", "reason"], "virtual work cancellation");
      validateArtifactRef(value.reason);
      break;
    default:
      throw new Error("virtual work resolution kind is unsupported");
  }
}

export class VirtualWorkControlBuilder {
  static succeed(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    result: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, clock, {
      resolution: "succeeded",
      result,
    });
  }

  static retry(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    error: ArtifactRef,
    nextReason: ParkReason | null = null,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, clock, {
      resolution: "retry",
      error,
      next_reason: nextReason,
    });
  }

  static park(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    reason: ParkReason,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, clock, {
      resolution: "parked",
      reason,
    });
  }

  static fail(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    error: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, clock, {
      resolution: "failed",
      error,
    });
  }

  static cancel(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    reason: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, clock, {
      resolution: "cancelled",
      reason,
    });
  }

  static migration(commandId: string, plan: RegionMigrationPlan): RegionMigrationCommand {
    if (commandId.length === 0) {
      throw new Error("virtual region migration requires a command identity");
    }
    if (plan.targets.some((target) => !isRunIdentity(target.run_id))) {
      throw new Error("virtual region migration target Run identity is invalid");
    }
    return {
      control_version: "cymule.virtual-region-migration-control/3",
      command_id: commandId,
      plan,
    };
  }

  static compaction(
    commandId: string,
    regionId: string,
    sourceCausalCut: string[],
    workIds: string[],
    occurrenceIds: string[],
    archivedCommandIds: string[],
    archive: VirtualArchiveBinding,
  ): VirtualCompactionCommand {
    const causalCut = [...new Set(sourceCausalCut)].sort(compareWireStrings);
    const works = [...new Set(workIds)].sort(compareWireStrings);
    const occurrences = [...new Set(occurrenceIds)].sort(compareWireStrings);
    const commands = [...new Set(archivedCommandIds)].sort(compareWireStrings);
    requireClosedRecord(archive, ["binding", "revision"], "Virtual archive binding");
    if (
      !isContentId(commandId) ||
      !isDurableIdentity(regionId) ||
      causalCut.length === 0 ||
      works.length === 0 || works.length > 1024 ||
      occurrences.length === 0 || occurrences.length > 1024 ||
      commands.length > 1024 ||
      [...causalCut, ...works, ...commands].some((value) => !isDurableIdentity(value)) ||
      occurrences.some((value) => !isContentId(value)) ||
      !isWireIdentity(archive.binding) || !isWireIdentity(archive.revision)
    ) {
      throw new Error("virtual compaction requires a Rust-issued identity, bounded exact selections, and archive generation");
    }
    return {
      control_version: "cymule.virtual-compaction-control/1",
      command_id: commandId,
      region_id: regionId,
      source_causal_cut: causalCut,
      work_ids: works,
      occurrence_ids: occurrences,
      archived_command_ids: commands,
      archive: { ...archive },
    };
  }

  static rehydration(
    commandId: string,
    certificateId: string,
    occurrenceIds: string[],
  ): VirtualRehydrationCommand {
    const occurrences = [...new Set(occurrenceIds)].sort(compareWireStrings);
    if (commandId.length === 0 || certificateId.length === 0 || occurrences.length === 0) {
      throw new Error("virtual rehydration requires command, certificate, and occurrence identities");
    }
    return {
      control_version: "cymule.virtual-rehydration-control/1",
      command_id: commandId,
      certificate_id: certificateId,
      occurrence_ids: occurrences,
    };
  }

  private static build(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    resolution: WorkResolution,
  ): WorkResolutionCommand {
    if (
      !isDurableIdentity(commandId) || !isDurableIdentity(workId) || !isDurableIdentity(owner) ||
      !Number.isSafeInteger(epoch) || epoch < 1 ||
      !Number.isSafeInteger(expectedLeaseEpoch) || expectedLeaseEpoch < 1
    ) {
      throw new Error("virtual work control requires identities, work and lease fences, and a Clock observation");
    }
    validateClockObservationRef(clock);
    validateVirtualWorkResolution(resolution);
    return {
      control_version: "cymule.virtual-work-control/2",
      command_id: commandId,
      work_id: workId,
      owner,
      epoch,
      expected_lease_epoch: expectedLeaseEpoch,
      clock: { ...clock },
      resolution: structuredClone(resolution),
    };
  }
}

export class VirtualSchedulingControlBuilder {
  static claim(
    commandId: string,
    owner: string,
    slotId: string,
    executionBinding: ArtifactRef,
    capabilities: string[],
    clock: ClockObservationRef,
    leaseTtl: number,
  ): VirtualClaimCommand {
    if (
      !isDurableIdentity(commandId) ||
      !isDurableIdentity(owner) ||
      !isDurableIdentity(slotId) ||
      !Array.isArray(capabilities) ||
      capabilities.some((capability) => !isDurableIdentity(capability)) ||
      !Number.isSafeInteger(leaseTtl) || leaseTtl < 1
    ) {
      throw new Error("virtual claim requires identities, binding, a Clock observation, and positive TTL");
    }
    validateClockObservationRef(clock);
    validateArtifactRef(executionBinding);
    if (clock.scope !== slotId || executionBinding.kind !== "cymule.execution-binding/2") {
      throw new Error("virtual claim requires its slot Clock and an ExecutionBinding Artifact");
    }
    const sortedCapabilities = [...new Set(capabilities)].sort(compareWireStrings);
    return {
      control_version: "cymule.virtual-claim-control/4",
      command_id: commandId,
      owner,
      slot_id: slotId,
      execution_binding: { ...executionBinding },
      capabilities: sortedCapabilities,
      clock: { ...clock },
      lease_ttl: leaseTtl,
    };
  }

  static renew(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    leaseTtl: number,
  ): VirtualLeaseRenewalCommand {
    if (
      !isDurableIdentity(commandId) ||
      !isDurableIdentity(workId) ||
      !isDurableIdentity(owner) ||
      !Number.isSafeInteger(epoch) || epoch < 1 ||
      !Number.isSafeInteger(expectedLeaseEpoch) || expectedLeaseEpoch < 1 ||
      !Number.isSafeInteger(leaseTtl) || leaseTtl < 1
    ) {
      throw new Error("virtual renewal requires identities, fences, a Clock observation, and positive TTL");
    }
    validateClockObservationRef(clock);
    return {
      control_version: "cymule.virtual-lease-renewal-control/2",
      command_id: commandId,
      work_id: workId,
      owner,
      epoch,
      expected_lease_epoch: expectedLeaseEpoch,
      clock: { ...clock },
      lease_ttl: leaseTtl,
    };
  }

  static recovery(
    commandId: string,
    workId: string,
    expectedOwner: string,
    expectedEpoch: number,
    expectedLeaseEpoch: number,
    clock: ClockObservationRef,
    resolution: VirtualRecoveryCommand["resolution"],
  ): VirtualRecoveryCommand {
    if (
      !isDurableIdentity(commandId) ||
      !isDurableIdentity(workId) ||
      !isDurableIdentity(expectedOwner) ||
      !Number.isSafeInteger(expectedEpoch) || expectedEpoch < 1 ||
      !Number.isSafeInteger(expectedLeaseEpoch) || expectedLeaseEpoch < 1
    ) {
      throw new Error("virtual recovery requires identities, fences, and a Clock observation");
    }
    validateClockObservationRef(clock);
    validateVirtualWorkResolution(resolution, true);
    return {
      control_version: "cymule.virtual-recovery-control/2",
      command_id: commandId,
      work_id: workId,
      expected_owner: expectedOwner,
      expected_epoch: expectedEpoch,
      expected_lease_epoch: expectedLeaseEpoch,
      clock: { ...clock },
      resolution: structuredClone(resolution),
    };
  }

  static runWeight(commandId: string, runId: string, weight: number): VirtualRunWeightCommand {
    if (!isDurableIdentity(commandId) || !isRunIdentity(runId)
      || !Number.isSafeInteger(weight) || weight < 1 || weight > 4_294_967_295) {
      throw new Error("virtual Run weight requires command, Run, and positive weight");
    }
    return {
      control_version: "cymule.virtual-run-weight-control/1",
      command_id: commandId,
      run_id: runId,
      weight,
    };
  }
}

export class ResourceBuilder {
  static text(text: string, annotations: Record<string, string> = {}): ResourceCandidate {
    return {
      resource_version: "cymule.resource/3",
      shape: "inline",
      media_type: "text/plain;charset=utf-8",
      inline: { encoding: "utf8", text },
      integrity: { kind: "inline" },
      ...(Object.keys(annotations).length === 0 ? {} : { annotations: { ...annotations } }),
    };
  }

  static json(value: Json, annotations: Record<string, string> = {}): ResourceCandidate {
    return {
      resource_version: "cymule.resource/3",
      shape: "inline",
      media_type: "application/json",
      inline: { encoding: "json", value },
      integrity: { kind: "inline" },
      ...(Object.keys(annotations).length === 0 ? {} : { annotations: { ...annotations } }),
    };
  }

  static external(
    shape: Exclude<ResourceShape, "inline">,
    mediaType: string,
    integrity: Exclude<ResourceIntegrity, { kind: "inline" }>,
    manifest: ResourceManifestDescriptor | undefined = undefined,
    annotations: Record<string, string> = {},
  ): ResourceCandidate {
    return {
      resource_version: "cymule.resource/3",
      shape,
      media_type: mediaType,
      integrity,
      ...(manifest === undefined ? {} : { manifest }),
      ...(Object.keys(annotations).length === 0 ? {} : { annotations: { ...annotations } }),
    };
  }

  static handoff(
    transferId: string,
    producer: { run_id: string; occurrence_id: string; result: ArtifactRef },
    toRun: string,
    slot: string,
    resource: ArtifactRef,
  ): ResourceHandoff {
    if (!isRunIdentity(producer.run_id) || !isRunIdentity(toRun)
      || producer.run_id === toRun
      || ![transferId, producer.occurrence_id, slot].every(isDurableIdentity)
      || !isArtifactRefWire(producer.result)
      || !isArtifactRefWire(resource)
      || !wireValuesEqual(producer.result, resource)) {
      throw new Error("Resource handoff authority is invalid");
    }
    return {
      handoff_version: "cymule.resource-handoff/5",
      transfer_id: transferId,
      producer: structuredClone(producer),
      to_run: toRun,
      slot,
      resource: structuredClone(resource),
    };
  }
}

export const ENGINE_PROTOCOL_VERSION = "cymule.engine/5" as const;

export type EngineFailureCategory =
  | "transport_failure"
  | "validation"
  | "contract_violation"
  | "admission_denied"
  | "conflict"
  | "not_found"
  | "expected_plugin_failure"
  | "plugin_defect"
  | "substrate_failure"
  | "cancelled"
  | "timed_out"
  | "unknown_world_outcome";

export type EnginePhase =
  | "transport"
  | "decode_request"
  | "validate_request"
  | "seal_plan"
  | "verify_plan"
  | "seal_resource"
  | "verify_wait_activation"
  | "verify_durable_command"
  | "observe_clock"
  | "verify_evolution_command"
  | "verify_live_evolution_command"
  | "execute_plan"
  | "execute_durable"
  | "execute_live_evolution"
  | "plugin_describe"
  | "plugin_call"
  | "effect_prepare"
  | "effect_dispatch"
  | "effect_reconcile"
  | "encode_response";

export type EngineContractSide = "schema" | "input" | "output";
export type EngineRetryDisposition =
  | "never"
  | "correct_and_retry"
  | "refresh_and_retry"
  | "retry_same_request"
  | "reconcile";

export interface EngineIssue {
  code: string;
  message: string;
  path?: string;
  schema_path?: string;
}

export interface EngineFailure {
  category: EngineFailureCategory;
  phase: EnginePhase;
  code: string;
  message: string;
  contract?: string;
  contract_side?: EngineContractSide;
  path?: string;
  issues?: EngineIssue[];
  retry_disposition?: EngineRetryDisposition;
}

export class EngineError extends Error {
  constructor(readonly failure: EngineFailure) {
    super(`${failure.code}: ${failure.message}`);
    this.name = "EngineError";
  }
}

export interface CliEngineOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

/** Provider-neutral Engine transport consumed by the high-level durable facade. */
export interface EngineTransport {
  seal(candidate: PlanCandidate): Promise<SealedPlan>;
  observeClock(target: EngineClockTarget, runId: string): Promise<ClockObservationResult>;
  executeDurable(target: EngineDurableTarget, command: DurableCommand): Promise<DurableResponse>;
  executeLiveEvolution(
    target: EngineEvolutionTarget,
    evolutionId: string,
    command: LiveEvolutionCommand,
  ): Promise<EvolutionCommit>;
}

const DEFAULT_ENGINE_TIMEOUT_MS = 30_000;
const ENGINE_STREAM_LIMIT = 16 * 1024 * 1024;

export class CliEngine {
  constructor(
    readonly executable = "cymule",
    readonly options: CliEngineOptions = {},
  ) {}

  async seal(candidate: PlanCandidate): Promise<SealedPlan> {
    const response = await this.request({ type: "seal", candidate });
    if (response.type !== "sealed") throw unexpectedResponse("sealed", response.type);
    return response.plan;
  }

  async sealResource(candidate: ResourceCandidate): Promise<ResourceHandle> {
    const response = await this.request({ type: "seal_resource", candidate });
    if (response.type !== "sealed_resource") {
      throw unexpectedResponse("sealed_resource", response.type);
    }
    return response.resource;
  }

  async verifyWaitActivation(activation: WaitActivation): Promise<WaitActivation> {
    const response = await this.request({ type: "verify_wait_activation", activation });
    if (response.type !== "verified_wait_activation") {
      throw unexpectedResponse("verified_wait_activation", response.type);
    }
    return response.activation;
  }

  async verifyDurableCommand(command: DurableCommand): Promise<DurableCommand> {
    const response = await this.request({ type: "verify_durable_command", command });
    if (response.type !== "verified_durable_command") {
      throw unexpectedResponse("verified_durable_command", response.type);
    }
    return response.command;
  }

  async observeClock(target: EngineClockTarget, runId: string): Promise<ClockObservationResult> {
    requireRequestRunIdentity(runId);
    let targetSnapshot: EngineClockTarget;
    try {
      assertStrictJson(target);
      validateEngineClockTarget(target);
      targetSnapshot = structuredClone(target);
    } catch (error) {
      throw localValidationError("validate_request", "invalid_clock_target", error);
    }
    const response = await this.request({
      type: "observe_clock",
      target: targetSnapshot,
      run_id: runId,
    });
    if (response.type !== "clock_observed") {
      throw unexpectedResponse("clock_observed", response.type);
    }
    return response.result;
  }

  async verifyEvolutionCommand(command: EvolutionCommand): Promise<EvolutionCommand> {
    const response = await this.request({ type: "verify_evolution_command", command });
    if (response.type !== "verified_evolution_command") {
      throw unexpectedResponse("verified_evolution_command", response.type);
    }
    return response.command;
  }

  async verifyLiveEvolutionCommand(command: LiveEvolutionCommand): Promise<LiveEvolutionCommand> {
    const response = await this.request({ type: "verify_live_evolution_command", command });
    if (response.type !== "verified_live_evolution_command") {
      throw unexpectedResponse("verified_live_evolution_command", response.type);
    }
    return response.command;
  }

  async executeDurable(
    target: EngineDurableTarget,
    command: DurableCommand,
  ): Promise<DurableResponse> {
    let targetSnapshot: EngineDurableTarget;
    let commandSnapshot: DurableCommand;
    try {
      assertStrictJson(target);
      assertStrictJson(command);
      validateDurableCommand(command);
      validateEngineDurableTarget(target, command);
      targetSnapshot = structuredClone(target);
      commandSnapshot = structuredClone(command);
    } catch (error) {
      throw localValidationError(
        "execute_durable",
        "durable_request_validation_failed",
        error,
      );
    }
    const expectedStartPlan = commandSnapshot.type === "start_run"
      ? (await this.seal(commandSnapshot.candidate)).plan_id
      : undefined;
    const response = await this.request(
      { type: "execute_durable", target: targetSnapshot, command: commandSnapshot },
      expectedStartPlan === undefined ? {} : { expectedStartPlan },
    );
    if (response.type !== "durable_executed") {
      throw unexpectedResponse("durable_executed", response.type);
    }
    return response.response;
  }

  async executeLiveEvolution(
    target: EngineEvolutionTarget,
    evolutionId: string,
    command: LiveEvolutionCommand,
  ): Promise<EvolutionCommit> {
    let targetSnapshot: EngineEvolutionTarget;
    let commandSnapshot: LiveEvolutionCommand;
    try {
      assertStrictJson(target);
      assertStrictJson(command);
      if (!isWireIdentity(evolutionId)) {
        throw new Error("evolution identity must contain 1..=256 Unicode scalars without controls");
      }
      validateLiveEvolutionCommand(command);
      validateEngineEvolutionTarget(target, command);
      targetSnapshot = structuredClone(target);
      commandSnapshot = structuredClone(command);
    } catch (error) {
      throw localValidationError(
        "execute_live_evolution",
        "evolution_request_validation_failed",
        error,
      );
    }
    const expectedPatchTargetPlan = commandSnapshot.operation === "apply"
      && commandSnapshot.command.operation === "apply_patch"
      ? (await this.seal(commandSnapshot.command.patch.target)).plan_id
      : undefined;
    const response = await this.request(
      {
        type: "execute_live_evolution",
        target: targetSnapshot,
        evolution_id: evolutionId,
        command: commandSnapshot,
      },
      expectedPatchTargetPlan === undefined ? {} : { expectedPatchTargetPlan },
    );
    if (response.type !== "live_evolution_executed") {
      throw unexpectedResponse("live_evolution_executed", response.type);
    }
    return response.commit;
  }

  async run(
    plan: SealedPlan,
    input: Json,
    plugin: EnginePluginTarget,
    runId: string,
  ): Promise<ExecutionOutcome> {
    requireRequestRunIdentity(runId);
    let planSnapshot: SealedPlan;
    let inputSnapshot: Json;
    let pluginSnapshot: EnginePluginTarget;
    try {
      assertStrictJson(plan);
      assertStrictJson(input);
      validateSealedPlan(plan);
      planSnapshot = structuredClone(plan);
      inputSnapshot = structuredClone(input);
    } catch (error) {
      throw localValidationError(
        "validate_request",
        error instanceof EngineError && error.failure.code === "request_encoding_failed"
          ? "request_encoding_failed"
          : "invalid_engine_request",
        error,
      );
    }
    try {
      assertStrictJson(plugin);
      validateEnginePluginTarget(plugin, false, 8 * 1024 * 1024);
      pluginSnapshot = structuredClone(plugin);
    } catch (error) {
      throw localValidationError("validate_request", "invalid_plugin_target", error);
    }
    const response = await this.request({
      type: "run",
      plan: planSnapshot,
      input: inputSnapshot,
      plugin: pluginSnapshot,
      run_id: runId,
    });
    if (response.type !== "execution_boundary") {
      throw unexpectedResponse("execution_boundary", response.type);
    }
    return response.execution;
  }

  private async request(
    request: EngineRequest,
    preflight: EngineRequestPreflight = {},
  ): Promise<EngineResponse> {
    if (this.options.signal?.aborted === true) {
      throw interruptedError(request, "cancelled", false);
    }
    const timeoutMs = this.options.timeoutMs ?? DEFAULT_ENGINE_TIMEOUT_MS;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
      throw localValidationError(
        "validate_request",
        "invalid_engine_timeout",
        new Error("Engine timeout must be a positive safe integer in milliseconds"),
      );
    }
    const envelopeRequest = { engine_protocol: ENGINE_PROTOCOL_VERSION, request };
    let encodedRequest: string;
    let wireEnvelope: unknown;
    try {
      encodedRequest = encodeStrictJson(envelopeRequest);
      wireEnvelope = parseStrictJson(encodedRequest);
    } catch (error) {
      throw localValidationError(
        "validate_request",
        "request_encoding_failed",
        error,
      );
    }
    if (!isRecord(wireEnvelope) || !isRecord(wireEnvelope.request)) {
      throw transportError("request_encoding_failed", "encoded Engine request is invalid");
    }
    const wireRequest = wireEnvelope.request as EngineRequest;
    const stdout = await runEngineProcess(
      this.executable,
      encodedRequest,
      wireRequest,
      timeoutMs,
      this.options.signal,
    );
    let rawEnvelope: unknown;
    try {
      rawEnvelope = parseStrictJson(stdout);
    } catch (error) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        error instanceof Error ? error.message : String(error),
      );
    }
    if (isRecord(rawEnvelope) && typeof rawEnvelope.engine_protocol === "string"
      && rawEnvelope.engine_protocol !== ENGINE_PROTOCOL_VERSION) {
      if (requestCanMutate(wireRequest)) {
        throw responseLossError(
          wireRequest,
          "unsupported_engine_protocol",
          `expected ${ENGINE_PROTOCOL_VERSION}, received ${JSON.stringify(rawEnvelope.engine_protocol)}`,
        );
      }
      throw new EngineError({
        category: "contract_violation",
        phase: "transport",
        code: "unsupported_engine_protocol",
        message: `expected ${ENGINE_PROTOCOL_VERSION}, received ${JSON.stringify(rawEnvelope.engine_protocol)}`,
        contract: ENGINE_PROTOCOL_VERSION,
        contract_side: "schema",
        retry_disposition: "never",
      });
    }
    let envelope: EngineResponseEnvelope;
    try {
      envelope = parseEngineEnvelope(rawEnvelope);
    } catch (error) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        error instanceof Error ? error.message : String(error),
      );
    }
    if (envelope.outcome === "failure") {
      const failure = unboxFloatIntegerTokens(envelope.error) as EngineFailure;
      try {
        validateEngineFailure(failure);
      } catch (error) {
        throw responseLossError(
          wireRequest,
          "invalid_engine_response",
          error instanceof Error ? error.message : String(error),
        );
      }
      throw new EngineError(failure);
    }
    if (envelope.outcome !== "success") {
      throw transportError("invalid_engine_response", "response outcome is not closed");
    }
    if (!wireValuesEqual(envelope.request, wireRequest)) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        "Engine success does not echo the complete request",
      );
    }
    try {
      validateSuccessResponse(envelope.response);
    } catch (error) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        error instanceof Error ? error.message : String(error),
      );
    }
    const expectedType = expectedSuccessResponseType(wireRequest);
    if (envelope.response.type !== expectedType) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        `expected ${expectedType}, received ${envelope.response.type}`,
      );
    }
    let matches = false;
    try {
      matches = successResponseMatchesRequest(wireRequest, envelope.response, preflight);
    } catch {
      matches = false;
    }
    if (!matches) {
      throw responseLossError(
        wireRequest,
        "invalid_engine_response",
        "Engine success does not match its complete request",
      );
    }
    return unboxFloatIntegerTokens(envelope.response) as EngineResponse;
  }
}

export class DurableEngine {
  readonly #transport: EngineTransport;

  constructor(
    readonly store: EngineStoreTarget | string,
    readonly executor: EnginePluginTarget | undefined,
    readonly clock: EngineClockTarget | undefined,
    transport: EngineTransport = new CliEngine(),
    readonly evolutionId = "cymule.sdk.live-evolution",
    readonly migrationAdapter?: EngineMigrationProviderTarget,
    readonly shadowDriver?: EngineShadowProviderTarget,
    readonly targetExecutionBindings: Readonly<Record<string, EnginePluginTarget>> = {},
  ) {
    this.#transport = transport;
  }

  async start(
    runId: string,
    candidate: PlanCandidate,
    input: Json,
    execution: ExecutionClaimRequest,
  ): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.startRun(runId, candidate, input, execution));
  }

  async observeClock(runId: string): Promise<ClockObservationRef> {
    if (this.clock === undefined) throw new Error("durable Clock target is missing");
    const request: EngineRequest = {
      type: "observe_clock",
      target: this.clock,
      run_id: runId,
    };
    const result = await this.#transport.observeClock(this.clock, runId);
    try {
      validateClockObservationResult(result);
      if (result.run_id !== runId
        || result.observation.source_id !== this.clock.source_id
        || result.observation.source_generation !== this.clock.source_generation) {
        throw new Error("Clock observation result does not match its request");
      }
    } catch (error) {
      throw responseLossError(request, "invalid_engine_response", errorMessage(error));
    }
    return result.observation;
  }

  async runIndexPage(
    options: DurablePageQueryOptions,
  ): Promise<DurableQueryPage<DurableRunIndexSummary>> {
    const response = await this.submit(DurableControlBuilder.runIndexPage(options));
    if (response.type !== "run_index_page") {
      throw unexpectedResponse("run_index_page", response.type);
    }
    return response.page;
  }

  async runCurrent(
    runId: string,
    expectedRevision: string | null,
  ): Promise<Extract<DurableResponse, { type: "run_current" }>> {
    const response = await this.submit(
      DurableControlBuilder.runCurrent(runId, expectedRevision),
    );
    if (response.type !== "run_current") throw unexpectedResponse("run_current", response.type);
    return response;
  }

  async runWaitPage(
    runId: string,
    options: DurablePageQueryOptions,
  ): Promise<DurableQueryPage<DurableWaitSummary>> {
    const response = await this.submit(DurableControlBuilder.runWaitPage(runId, options));
    if (response.type !== "run_wait_page") {
      throw unexpectedResponse("run_wait_page", response.type);
    }
    return response.page;
  }

  async runEffectPage(
    runId: string,
    options: DurablePageQueryOptions,
  ): Promise<DurableQueryPage<DurableEffectSummary>> {
    const response = await this.submit(DurableControlBuilder.runEffectPage(runId, options));
    if (response.type !== "run_effect_page") {
      throw unexpectedResponse("run_effect_page", response.type);
    }
    return response.page;
  }

  async runOccurrencePage(
    runId: string,
    options: DurablePageQueryOptions,
  ): Promise<DurableQueryPage<DurableOccurrenceSummary>> {
    const response = await this.submit(DurableControlBuilder.runOccurrencePage(runId, options));
    if (response.type !== "run_occurrence_page") {
      throw unexpectedResponse("run_occurrence_page", response.type);
    }
    return response.page;
  }

  async runAttemptPage(
    runId: string,
    options: DurablePageQueryOptions,
  ): Promise<DurableQueryPage<DurableAttemptSummary>> {
    const response = await this.submit(DurableControlBuilder.runAttemptPage(runId, options));
    if (response.type !== "run_attempt_page") {
      throw unexpectedResponse("run_attempt_page", response.type);
    }
    return response.page;
  }

  async runItem(
    query: DurableRunItemQuery,
  ): Promise<Extract<DurableResponse, { type: "run_item" }>> {
    const response = await this.submit(DurableControlBuilder.runItem(query));
    if (response.type !== "run_item") throw unexpectedResponse("run_item", response.type);
    return response;
  }

  async resume(runId: string, execution: ExecutionClaimRequest): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.resumeRun(runId, execution));
  }

  async takeover(
    runId: string,
    expectedFence: number,
    execution: ExecutionClaimRequest,
  ): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.takeoverRun(runId, expectedFence, execution));
  }

  async signal(
    activationId: string,
    key: string,
    waitIds: string[],
    value: Json,
  ): Promise<DurableResponse> {
    return await this.submit(
      DurableControlBuilder.activateSignal(activationId, key, waitIds, value),
    );
  }

  async release(
    intentId: string,
    execution: ExecutionClaimRequest,
  ): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.releaseEffect(intentId, execution));
  }

  async resolveEffect(
    resolutionId: string,
    runId: string,
    intentId: string,
    executionBinding: ArtifactRef,
    occurrenceBinding: string,
    claimOwner: string,
    claimEpoch: number,
    resolution: EffectResolution,
    value: Json,
  ): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.resolveEffect(
      resolutionId,
      runId,
      intentId,
      executionBinding,
      occurrenceBinding,
      claimOwner,
      claimEpoch,
      resolution,
      value,
    ));
  }

  async cancel(
    cancellationId: string,
    runId: string,
    reason: Json,
  ): Promise<DurableResponse> {
    return await this.submit(DurableControlBuilder.cancelRun(cancellationId, runId, reason));
  }

  async evolve(command: LiveEvolutionCommand): Promise<EvolutionCommit> {
    let operation: EvolutionCommand["operation"] | undefined;
    let targetPlan: string | undefined;
    if (command.operation === "apply") {
      operation = command.command.operation;
      if (command.command.operation === "migrate") {
        targetPlan = command.command.request.to_plan;
      }
    }
    const targetExecution = targetPlan === undefined
      ? undefined
      : this.targetExecutionBindings[targetPlan];
    return await this.#transport.executeLiveEvolution(
      {
        store: typeof this.store === "string" ? directoryStore(this.store) : this.store,
        migration_adapter: operation === "migrate" ? this.migrationAdapter ?? null : null,
        shadow_driver: operation === "shadow" ? this.shadowDriver ?? null : null,
        target_execution_bindings: targetPlan !== undefined && targetExecution !== undefined
          ? { [targetPlan]: structuredClone(targetExecution) }
          : {},
      },
      this.evolutionId,
      command,
    );
  }

  private async submit(command: DurableCommand): Promise<DurableResponse> {
    const storeOnly = command.type === "activate_wait" || command.type === "cancel_run" ||
      command.type === "run_index_page" || command.type === "run_current" ||
      command.type === "run_wait_page" || command.type === "run_effect_page" ||
      command.type === "run_occurrence_page" || command.type === "run_attempt_page" ||
      command.type === "run_item";
    const providerOnly = command.type === "resolve_effect";
    const store = typeof this.store === "string" ? directoryStore(this.store) : this.store;
    const executor = this.executor;
    return await this.#transport.executeDurable(
      {
        store,
        ...(!storeOnly && executor !== undefined ? { executor } : {}),
        ...(!storeOnly && !providerOnly && this.clock !== undefined ? { clock: this.clock } : {}),
      },
      command,
    );
  }
}

async function runEngineProcess(
  executable: string,
  encodedRequest: string,
  request: EngineRequest,
  timeoutMs: number,
  signal: AbortSignal | undefined,
): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    let child: ChildProcessWithoutNullStreams;
    try {
      child = spawn(executable, ["rpc"], {
        detached: process.platform !== "win32",
        stdio: ["pipe", "pipe", "pipe"],
      });
    } catch (error) {
      reject(transportError("engine_start_failed", errorMessage(error)));
      return;
    }

    const stdoutChunks: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let requestBegan = child.pid !== undefined;
    let terminalError: EngineError | undefined;
    let state: "starting" | "running" | "terminating" | "closed" | "settled" =
      "starting";

    const clear = (): void => {
      clearTimeout(deadline);
      signal?.removeEventListener("abort", abort);
    };
    const settleTermination = (): void => {
      if (state !== "closed" || terminalError === undefined) return;
      state = "settled";
      clear();
      reject(terminalError);
    };
    const terminate = (error: EngineError): void => {
      if (terminalError !== undefined || state === "closed" || state === "settled") return;
      terminalError = error;
      state = "terminating";
      terminateEngineProcessGroup(child);
    };
    const abort = (): void => {
      terminate(interruptedError(request, "cancelled", requestBegan));
    };
    const deadline = setTimeout(() => {
      terminate(interruptedError(request, "timed_out", requestBegan));
    }, timeoutMs);

    child.once("spawn", () => {
      requestBegan = true;
      if (state === "starting") state = "running";
    });
    child.once("error", (error) => {
      if (state === "settled") return;
      if (!requestBegan) {
        state = "settled";
        clear();
        reject(transportError("engine_start_failed", error.message));
        return;
      }
      terminate(responseLossError(request, "engine_io_failed"));
    });
    child.stdout.on("data", (chunk: Buffer) => {
      if (terminalError !== undefined) return;
      stdoutBytes += chunk.length;
      if (stdoutBytes > ENGINE_STREAM_LIMIT) {
        terminate(responseLossError(request, "engine_response_too_large"));
        return;
      }
      stdoutChunks.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      if (terminalError !== undefined) return;
      stderrBytes += chunk.length;
      if (stderrBytes > ENGINE_STREAM_LIMIT) {
        terminate(responseLossError(request, "engine_diagnostic_too_large"));
      }
    });
    child.stdin.once("error", () => {
      terminate(responseLossError(request, "engine_io_failed"));
    });
    child.once("close", (code) => {
      if (state === "settled") return;
      state = "closed";
      if (terminalError !== undefined) {
        settleTermination();
        return;
      }
      state = "settled";
      clear();
      if (code !== 0) {
        reject(responseLossError(request, "engine_process_failed"));
        return;
      }
      try {
        resolve(
          new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(stdoutChunks)),
        );
      } catch (error) {
        reject(responseLossError(request, "invalid_engine_response", errorMessage(error)));
      }
    });

    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted === true) abort();
    if (terminalError === undefined) child.stdin.end(encodedRequest, "utf8");
  });
}

function terminateEngineProcessGroup(child: ChildProcessWithoutNullStreams): void {
  const pid = child.pid;
  if (pid === undefined) return;
  try {
    if (process.platform === "win32") {
      child.kill("SIGKILL");
    } else {
      process.kill(-pid, "SIGKILL");
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") child.kill("SIGKILL");
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function localValidationError(
  phase: EnginePhase,
  code: string,
  error: unknown,
): EngineError {
  return new EngineError({
    category: "validation",
    phase,
    code,
    message: errorMessage(error),
    retry_disposition: "correct_and_retry",
  });
}

function transportError(code: string, message: string): EngineError {
  return new EngineError({
    category: "transport_failure",
    phase: "transport",
    code,
    message,
  });
}

function unexpectedResponse(expected: string, received: string): EngineError {
  return new EngineError({
    category: "contract_violation",
    phase: "transport",
    code: "unexpected_engine_response",
    message: `expected ${expected}, received ${received}`,
    retry_disposition: "never",
  });
}

type EngineRequest =
  | { type: "seal"; candidate: PlanCandidate }
  | { type: "seal_resource"; candidate: ResourceCandidate }
  | { type: "verify_wait_activation"; activation: WaitActivation }
  | { type: "verify_durable_command"; command: DurableCommand }
  | { type: "observe_clock"; target: EngineClockTarget; run_id: string }
  | { type: "verify_evolution_command"; command: EvolutionCommand }
  | { type: "verify_live_evolution_command"; command: LiveEvolutionCommand }
  | { type: "execute_durable"; target: EngineDurableTarget; command: DurableCommand }
  | {
      type: "execute_live_evolution";
      target: EngineEvolutionTarget;
      evolution_id: string;
      command: LiveEvolutionCommand;
    }
  | {
      type: "run";
      plan: SealedPlan;
      input: Json;
      plugin: EnginePluginTarget;
      run_id: string;
    };

interface EngineRequestPreflight {
  expectedPatchTargetPlan?: string;
  expectedStartPlan?: string;
}

type EngineResponse =
  | { type: "sealed"; plan: SealedPlan }
  | { type: "sealed_resource"; resource: ResourceHandle }
  | { type: "verified_wait_activation"; activation: WaitActivation }
  | { type: "verified_durable_command"; command: DurableCommand }
  | { type: "clock_observed"; result: ClockObservationResult }
  | { type: "verified_evolution_command"; command: EvolutionCommand }
  | { type: "verified_live_evolution_command"; command: LiveEvolutionCommand }
  | { type: "execution_boundary"; execution: ExecutionOutcome }
  | { type: "durable_executed"; response: DurableResponse }
  | { type: "live_evolution_executed"; commit: EvolutionCommit }
  | { type: "verified" };

type EngineResponseEnvelope =
  | {
      outcome: "success";
      engine_protocol: typeof ENGINE_PROTOCOL_VERSION;
      request: EngineRequest;
      response: EngineResponse;
    }
  | {
      outcome: "failure";
      engine_protocol: typeof ENGINE_PROTOCOL_VERSION;
      error: EngineFailure;
    };

function expectedSuccessResponseType(request: EngineRequest): EngineResponse["type"] {
  switch (request.type) {
    case "seal": return "sealed";
    case "seal_resource": return "sealed_resource";
    case "verify_wait_activation": return "verified_wait_activation";
    case "verify_durable_command": return "verified_durable_command";
    case "observe_clock": return "clock_observed";
    case "verify_evolution_command": return "verified_evolution_command";
    case "verify_live_evolution_command": return "verified_live_evolution_command";
    case "execute_durable": return "durable_executed";
    case "execute_live_evolution": return "live_evolution_executed";
    case "run": return "execution_boundary";
  }
}

function successResponseMatchesRequest(
  request: EngineRequest,
  response: EngineResponse,
  preflight: EngineRequestPreflight,
): boolean {
  switch (request.type) {
    case "seal":
      return response.type === "sealed"
        && wireValuesEqual(response.plan.candidate, request.candidate);
    case "seal_resource": {
      if (response.type !== "sealed_resource") return false;
      const { resource_id: _resourceId, ...candidate } = response.resource;
      return wireValuesEqual(candidate, request.candidate);
    }
    case "verify_wait_activation":
      return response.type === "verified_wait_activation"
        && wireValuesEqual(response.activation, request.activation);
    case "verify_durable_command":
      return response.type === "verified_durable_command"
        && wireValuesEqual(response.command, request.command);
    case "observe_clock":
      return response.type === "clock_observed"
        && response.result.run_id === request.run_id
        && response.result.observation.source_id === request.target.source_id
        && response.result.observation.source_generation === request.target.source_generation;
    case "verify_evolution_command":
      return response.type === "verified_evolution_command"
        && wireValuesEqual(response.command, request.command);
    case "verify_live_evolution_command":
      return response.type === "verified_live_evolution_command"
        && wireValuesEqual(response.command, request.command);
    case "execute_durable":
      return response.type === "durable_executed"
        && durableResponseMatchesCommand(
          request.command,
          response.response,
          preflight.expectedStartPlan,
        );
    case "execute_live_evolution":
      return response.type === "live_evolution_executed"
        && evolutionCommitMatchesRequest(
          request.evolution_id,
          request.command,
          response.commit,
          preflight.expectedPatchTargetPlan,
        );
    case "run":
      return response.type === "execution_boundary"
        && executionOutcomeMatchesRequest(response.execution, request.plan, request.run_id);
  }
}

function durableResponseMatchesCommand(
  command: DurableCommand,
  response: DurableResponse,
  expectedStartPlan?: string,
): boolean {
  switch (command.type) {
    case "start_run":
      return response.type === "run_boundary"
        && durableBoundaryMatchesRun(
          response.boundary,
          command.run_id,
          expectedStartPlan,
        );
    case "resume_run":
    case "takeover_run":
      return response.type === "run_boundary"
        && durableBoundaryMatchesRun(response.boundary, command.run_id);
    case "cancel_run":
      return response.type === "run_cancelled"
        && wireValuesEqual(response.receipt.command, cancellationCommandFrom(command));
    case "release_effect":
      return response.type === "run_boundary"
        && (response.boundary.status === "reconciliation_required"
            || response.boundary.status === "effect_unavailable"
            || response.boundary.status === "effect_not_applied"
          ? response.boundary.intent_id === command.intent_id
          : response.boundary.status === "release_required"
          ? response.boundary.intent_ids.includes(command.intent_id)
          : true);
    case "resolve_effect":
      return response.type === "effect_resolved"
        && wireValuesEqual(response.receipt.command, effectResolutionCommandFrom(command));
    case "activate_wait":
      return response.type === "wait_activated"
        && response.receipt.activation.activation_id === command.activation_id
        && wireValuesEqual(response.receipt.activation.source, command.source)
        && wireValuesEqual(response.receipt.activation.wait_ids, command.wait_ids);
    case "run_index_page":
      return response.type === "run_index_page"
        && durableQueryPageMatchesCommand(
          response.page,
          command.expected_revision,
          command.cursor,
          command.limit,
          command.max_canonical_bytes,
        )
        && Buffer.byteLength(JSON.stringify(response), "utf8")
          <= command.max_canonical_bytes;
    case "run_current":
      return response.type === "run_current"
        && (command.expected_revision === null
          || command.expected_revision === response.observed_revision)
        && (response.current === null || response.current.run_id === command.run_id)
        && Buffer.byteLength(JSON.stringify(response), "utf8")
          <= DURABLE_QUERY_PAGE_BYTES;
    case "run_wait_page":
    case "run_effect_page":
    case "run_occurrence_page":
    case "run_attempt_page": {
      if (response.type !== command.type || response.run_id !== command.run_id) return false;
      return durableQueryPageMatchesCommand(
        response.page,
        command.expected_revision,
        command.cursor,
        command.limit,
        command.max_canonical_bytes,
      ) && Buffer.byteLength(JSON.stringify(response), "utf8")
        <= command.max_canonical_bytes;
    }
    case "run_item":
      return response.type === "run_item"
        && response.run_id === command.run_id
        && (command.expected_revision === null
          || command.expected_revision === response.observed_revision)
        && (response.item === null
          || durableRunItemMatchesSelector(response.item, command.selector))
        && Buffer.byteLength(JSON.stringify(response), "utf8") <= command.max_canonical_bytes;
  }
}

function durableQueryPageMatchesCommand(
  page: DurableQueryPage<unknown>,
  expectedRevision: string | null,
  cursor: DurablePageCursor | null,
  limit: number,
  maxCanonicalBytes: number,
): boolean {
  const advanced = cursor === null || page.items.length === 0
    || compareDurablePositions(
      cursor.position,
      durablePositionForKey(durableSummaryKey(cursor.query_kind, page.items[0])),
    ) < 0;
  return (expectedRevision === null || page.observed_revision === expectedRevision)
    && (cursor === null
      || cursor.source_revision === page.observed_revision
        && cursor.source_root === page.source_root)
    && advanced
    && page.items.length <= limit
    && maxCanonicalBytes <= DURABLE_QUERY_PAGE_BYTES;
}

function durableRunItemMatchesSelector(
  item: DurableRunItem,
  selector: DurableRunItemSelector,
): boolean {
  switch (selector.kind) {
    case "wait": return item.kind === "wait" && item.wait.wait_id === selector.wait_id;
    case "effect": return item.kind === "effect" && item.effect.intent_id === selector.intent_id;
    case "occurrence":
      return item.kind === "occurrence"
        && item.occurrence.occurrence_id === selector.occurrence_id;
    case "attempt": return item.kind === "attempt" && item.attempt.attempt_id === selector.attempt_id;
  }
}

function effectResolutionCommandFrom(
  command: Extract<DurableCommand, { type: "resolve_effect" }>,
): EffectResolutionCommand {
  return {
    resolution_id: command.resolution_id,
    run_id: command.run_id,
    intent_id: command.intent_id,
    execution_binding: command.execution_binding,
    occurrence_binding: command.occurrence_binding,
    claim_owner: command.claim_owner,
    claim_epoch: command.claim_epoch,
    resolution: command.resolution,
    value: command.value,
  };
}

function cancellationCommandFrom(
  command: Extract<DurableCommand, { type: "cancel_run" }>,
): CancellationCommand {
  return {
    cancellation_id: command.cancellation_id,
    run_id: command.run_id,
    reason: command.reason,
  };
}

function durableBoundaryMatchesRun(
  boundary: DurableBoundary,
  runId: string,
  expectedPlan?: string,
): boolean {
  return boundary.status !== "completed"
    || boundary.result.run_id === runId
      && (expectedPlan === undefined || boundary.result.plan_id === expectedPlan);
}

function executionOutcomeMatchesRequest(
  outcome: ExecutionOutcome,
  plan: SealedPlan,
  runId: string,
): boolean {
  const boundary = outcome.status === "completed"
    ? outcome.result
    : outcome.status === "suspended"
    ? outcome.suspension
    : outcome.status === "release_required"
    ? outcome.release
    : outcome.reconciliation;
  if (boundary.run_id !== runId || boundary.plan_id !== plan.plan_id) return false;
  if (outcome.status !== "suspended") return true;
  const step = findPlanStep(plan.candidate, outcome.suspension.definition_id, outcome.suspension.site_id);
  return step?.op === "wait"
    && wireValuesEqual(step.wait, outcome.suspension.wait)
    && (step.bind ?? null) === outcome.suspension.result_bind;
}

function findPlanStep(
  candidate: PlanCandidate,
  definitionId: string,
  siteId: string,
): Step | undefined {
  const definition = candidate.definitions.find(({ id }) => id === definitionId);
  if (definition === undefined) return undefined;
  const find = (region: Region): Step | undefined => {
    for (const step of region.steps) {
      if (step.id === siteId) return step;
      if (step.op === "scope") {
        const nested = find(step.body);
        if (nested !== undefined) return nested;
      }
    }
    return undefined;
  };
  return find(definition.body);
}

function evolutionCommitMatchesRequest(
  evolutionId: string,
  command: LiveEvolutionCommand,
  commit: EvolutionCommit,
  expectedPatchTargetPlan?: string,
): boolean {
  return commit.receipt.command.evolution_id === evolutionId
    && wireValuesEqual(commit.receipt.command.command, command)
    && liveEvolutionOutcomeMatchesCommand(
      commit.receipt.command.command,
      commit.receipt.outcome,
      expectedPatchTargetPlan,
    );
}

function liveEvolutionOutcomeMatchesCommand(
  command: LiveEvolutionCommand,
  response: LiveEvolutionOutcome,
  expectedPatchTargetPlan?: string,
): boolean {
  if (command.operation === "publish_definition") {
    return response.result === "definition_published"
      && response.revision.logical_ref === command.logical_ref
      && wireValuesEqual(response.revision.definition, command.definition)
      && wireValuesEqual(response.revision.references, command.references);
  }
  if (command.operation === "register_template") {
    const expectedReferences = command.template.references
      .map((reference) => reference.logical_ref)
      .sort(compareWireStrings);
    const resolvedReferences = response.result === "template_registered"
      && isRecord(response.linked.resolved_revisions)
      ? Object.keys(response.linked.resolved_revisions).sort(compareWireStrings)
      : [];
    return response.result === "template_registered"
      && response.linked.template_id === command.template.template_id
      && expectedReferences.length === new Set(expectedReferences).size
      && expectedReferences.length === resolvedReferences.length
      && expectedReferences.every((reference, index) => reference === resolvedReferences[index]);
  }
  if (command.operation === "publish_and_relink") {
    return response.result === "publication_applied"
      && isRecord(response.receipt.revision)
      && response.receipt.revision.logical_ref === command.publication.logical_ref
      && wireValuesEqual(response.receipt.revision.definition, command.publication.definition)
      && wireValuesEqual(
        response.receipt.revision.references,
        command.publication.references,
      );
  }
  switch (command.command.operation) {
    case "apply_patch":
      return response.result === "patch_applied"
        && response.edge.from_plan === command.command.patch.from_plan
        && (expectedPatchTargetPlan === undefined
          || response.edge.to_plan === expectedPatchTargetPlan)
        && wireValuesEqual(response.edge.operations, command.command.patch.operations);
    case "set_rollout":
    case "observe":
      return response.result === "applied";
    case "select_occurrence":
      return response.result === "occurrence_selected"
        && response.pin.occurrence_id === command.command.occurrence_id
        && response.pin.template_id === command.template_id
        && response.pin.selection_id === command.command.selection_id
        && wireValuesEqual(response.pin.execution_binding, command.command.execution_binding);
    case "migrate":
      return response.result === "migrated"
        && wireValuesEqual(response.receipt.request, command.command.request);
    case "restart_under_new_plan":
      return response.result === "restart_authorized"
        && wireValuesEqual(response.receipt.request, command.command.request);
    case "shadow":
      return response.result === "shadow_recorded"
        && response.comparison.comparison_id === command.command.request.comparison_id
        && response.comparison.subject === command.command.request.subject
        && response.comparison.decision_id === command.command.request.decision_id
        && response.comparison.primary_plan === command.command.request.primary_plan
        && response.comparison.shadow_plan === command.command.request.shadow_plan
        && response.comparison.driver_id === command.command.request.driver_id
        && response.comparison.driver_revision === command.command.request.driver_revision
        && response.comparison.comparison_policy === command.command.request.comparison_policy;
    case "apply_gate":
      return response.result === "gate_applied"
        && response.transition.from_decision === command.command.gate.decision_id
        && response.transition.to_decision === command.command.next_decision_id
        && isRecord(response.transition.evaluation)
        && wireValuesEqual(response.transition.evaluation.gate, command.command.gate);
  }
}

function wireValuesEqual(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (isFloatIntegerToken(left) || isFloatIntegerToken(right)) {
    const leftValue = isFloatIntegerToken(left) ? left.value : left;
    const rightValue = isFloatIntegerToken(right) ? right.value : right;
    return typeof leftValue === "number"
      && typeof rightValue === "number"
      && Object.is(leftValue, rightValue);
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return Array.isArray(left) && Array.isArray(right)
      && left.length === right.length
      && left.every((value, index) => wireValuesEqual(value, right[index]));
  }
  if (!isRecord(left) || !isRecord(right)) return false;
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  return leftKeys.length === rightKeys.length
    && leftKeys.every((key, index) => key === rightKeys[index]
      && wireValuesEqual(left[key], right[key]));
}

const ENGINE_FAILURE_CATEGORIES = new Set<EngineFailureCategory>([
  "transport_failure", "validation", "contract_violation", "admission_denied", "conflict",
  "not_found", "expected_plugin_failure", "plugin_defect", "substrate_failure", "cancelled",
  "timed_out", "unknown_world_outcome",
]);
const ENGINE_PHASES = new Set<EnginePhase>([
  "transport", "decode_request", "validate_request", "seal_plan", "verify_plan",
  "seal_resource", "verify_wait_activation", "verify_durable_command", "observe_clock",
  "verify_evolution_command", "verify_live_evolution_command", "execute_plan",
  "execute_durable", "execute_live_evolution",
  "plugin_describe", "plugin_call", "effect_prepare", "effect_dispatch", "effect_reconcile",
  "encode_response",
]);
const ENGINE_RETRIES = new Set<EngineRetryDisposition>([
  "never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile",
]);
const ENGINE_RETRY_MATRIX: Record<
  EngineFailureCategory,
  ReadonlySet<EngineRetryDisposition | undefined>
> = {
  transport_failure: new Set([undefined]),
  validation: new Set(["correct_and_retry"]),
  contract_violation: new Set(["correct_and_retry", "never"]),
  admission_denied: new Set(["correct_and_retry", "never"]),
  conflict: new Set(["refresh_and_retry", "never"]),
  not_found: new Set([undefined]),
  expected_plugin_failure: new Set(["never"]),
  plugin_defect: new Set(["never"]),
  substrate_failure: new Set(["retry_same_request"]),
  cancelled: new Set(["never"]),
  timed_out: new Set(["retry_same_request", "refresh_and_retry"]),
  unknown_world_outcome: new Set(["reconcile"]),
};

function parseEngineEnvelope(value: unknown): EngineResponseEnvelope {
  if (!isRecord(value) || (value.outcome !== "success" && value.outcome !== "failure")) {
    throw transportError("invalid_engine_response", "response envelope is not closed");
  }
  const keys = Object.keys(value).sort().join(",");
  const expected = value.outcome === "success"
    ? "engine_protocol,outcome,request,response"
    : "engine_protocol,error,outcome";
  if (keys !== expected) {
    throw transportError("invalid_engine_response", "response envelope fields are not closed");
  }
  if (value.outcome === "success" && (!isRecord(value.request) || !isRecord(value.response))) {
    throw transportError("invalid_engine_response", "success request or response is invalid");
  }
  return value as EngineResponseEnvelope;
}

function validateSuccessResponse(value: unknown): void {
  if (!isRecord(value) || typeof value.type !== "string") {
    throw transportError("invalid_engine_response", "success response is not tagged");
  }
  const payload = new Map<string, string>([
    ["sealed", "plan,type"], ["sealed_resource", "resource,type"],
    ["verified_wait_activation", "activation,type"],
    ["verified_durable_command", "command,type"],
    ["clock_observed", "result,type"],
    ["verified_evolution_command", "command,type"],
    ["verified_live_evolution_command", "command,type"],
    ["execution_boundary", "execution,type"], ["verified", "type"],
    ["durable_executed", "response,type"],
    ["live_evolution_executed", "commit,type"],
  ]).get(value.type);
  if (payload === undefined || Object.keys(value).sort().join(",") !== payload) {
    throw transportError("invalid_engine_response", "success response fields are not closed");
  }
  if (value.type === "sealed") validateSealedPlan(value.plan);
  if (value.type === "sealed_resource") validateResourceHandle(value.resource);
  if (value.type === "verified_wait_activation") validateWaitActivation(value.activation);
  if (value.type === "verified_durable_command") validateDurableCommand(value.command);
  if (value.type === "clock_observed") validateClockObservationResult(value.result);
  if (value.type === "execution_boundary") validateExecutionOutcome(value.execution);
  if (value.type === "verified_evolution_command") validateEvolutionCommand(value.command);
  if (value.type === "verified_live_evolution_command") validateLiveEvolutionCommand(value.command);
  if (value.type === "durable_executed") validateDurableResponse(value.response);
  if (value.type === "live_evolution_executed") validateEvolutionCommit(value.commit);
}

function validateResourceHandle(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "Resource Handle is invalid");
  const allowed = new Set(["resource_id", "resource_version", "shape", "media_type", "inline", "integrity", "manifest", "annotations"]);
  if (!Object.keys(value).every((key) => allowed.has(key)) ||
    !["resource_id", "resource_version", "shape", "media_type", "integrity"].every((key) => key in value) ||
    value.resource_version !== "cymule.resource/3" || !/^sha256:[0-9a-f]{64}$/.test(String(value.resource_id)) ||
    !new Set(["inline", "object", "collection", "directory", "snapshot"]).has(String(value.shape)) ||
    !isResourceMediaType(value.media_type) || !isRecord(value.integrity)) {
    throw transportError("invalid_engine_response", "Resource Handle fields are invalid");
  }
  const integrityFields = new Map<string, string>([["inline", "kind"], ["content", "digest,kind,size"], ["version", "authority,kind,version"], ["live", "identity,kind"]]).get(String(value.integrity.kind));
  if (integrityFields === undefined || Object.keys(value.integrity).sort().join(",") !== integrityFields) throw transportError("invalid_engine_response", "Resource integrity is not closed");
  if (value.integrity.kind === "content") {
    if (!isContentId(value.integrity.digest) || !isNonNegativeInteger(value.integrity.size)) {
      throw transportError("invalid_engine_response", "content integrity is invalid");
    }
  } else if (value.integrity.kind === "version") {
    if (!isResourceToken(value.integrity.authority) || !isResourceToken(value.integrity.version)) {
      throw transportError("invalid_engine_response", "version integrity is invalid");
    }
  } else if (value.integrity.kind === "live" && !isResourceToken(value.integrity.identity)) {
    throw transportError("invalid_engine_response", "live integrity is invalid");
  }
  if (value.shape === "inline") {
    if (!isRecord(value.inline) || value.integrity.kind !== "inline" || value.manifest !== undefined) {
      throw transportError("invalid_engine_response", "inline Resource evidence is invalid");
    }
    validateInlineResource(value.inline);
  } else if (value.inline !== undefined || value.integrity.kind === "inline") {
    throw transportError("invalid_engine_response", "external Resource retained inline data");
  }
  if (value.manifest !== undefined) {
    validateResourceManifest(value.manifest);
    const manifest = value.manifest as Record<string, unknown>;
    if (!new Set(["collection", "directory", "snapshot"]).has(String(value.shape))
      || value.integrity.kind !== "content"
      || value.integrity.digest !== manifest.digest
      || value.integrity.size !== manifest.size) {
      throw transportError("invalid_engine_response", "Resource manifest does not match content integrity");
    }
  }
  if (value.annotations !== undefined) {
    if (!isRecord(value.annotations) || Object.keys(value.annotations).length === 0) {
      throw transportError("invalid_engine_response", "Resource annotations are invalid");
    }
    for (const [key, annotation] of Object.entries(value.annotations)) {
      if (!isResourceToken(key) || typeof annotation !== "string"
        || Buffer.byteLength(annotation) > 4_096) {
        throw transportError("invalid_engine_response", "Resource annotation is invalid");
      }
    }
  }
}

function validateInlineResource(value: Record<string, unknown>): void {
  const fields = value.encoding === "utf8" ? "encoding,text"
    : value.encoding === "json" ? "encoding,value"
    : value.encoding === "base64" ? "data,encoding" : undefined;
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) {
    throw transportError("invalid_engine_response", "inline Resource data is not closed");
  }
  if (value.encoding === "utf8") {
    if (typeof value.text !== "string" || Buffer.byteLength(value.text) > 1_048_576) {
      throw transportError("invalid_engine_response", "inline UTF-8 Resource is invalid");
    }
  } else if (value.encoding === "json") {
    const encoded = JSON.stringify(unboxFloatIntegerTokens(value.value));
    if (encoded === undefined || Buffer.byteLength(encoded) > 1_048_576) {
      throw transportError("invalid_engine_response", "inline JSON Resource is invalid");
    }
  } else if (value.encoding === "base64") {
    if (typeof value.data !== "string") {
      throw transportError("invalid_engine_response", "inline base64 Resource is invalid");
    }
    const decoded = Buffer.from(value.data, "base64");
    if (decoded.length > 1_048_576 || decoded.toString("base64") !== value.data) {
      throw transportError("invalid_engine_response", "inline base64 Resource is not canonical");
    }
  }
}

const EMPTY_RESOURCE_MANIFEST_ROOT =
  "sha256:6a754fadbb296b87040c37dab30caea63de1bd1a85142bc82a03a7cf82e64dfc";

function validateResourceManifest(value: unknown): void {
  requireClosedRecord(value, [
    "manifest_version", "media_type", "digest", "size", "entry_count", "root_digest",
  ], "Resource manifest");
  if (value.manifest_version !== "cymule.resource-manifest/3"
    || value.media_type !== "application/vnd.cymule.resource-manifest+jsonl"
    || !isContentId(value.digest) || !isContentId(value.root_digest)
    || !isNonNegativeInteger(value.size) || !isNonNegativeInteger(value.entry_count)
    || value.digest !== resourceManifestDescriptorId(value)
    || (Number(value.entry_count) === 0 && Number(value.size) !== 0)
    || (Number(value.entry_count) === 0
      && value.root_digest !== EMPTY_RESOURCE_MANIFEST_ROOT)
    || (Number(value.entry_count) > 0 && Number(value.size) === 0)) {
    throw transportError("invalid_engine_response", "Resource manifest is invalid");
  }
}

function resourceManifestDescriptorId(value: Record<string, unknown>): string {
  const identity = JSON.stringify({
    entry_count: value.entry_count,
    media_type: value.media_type,
    root_digest: value.root_digest,
    size: value.size,
  });
  return `sha256:${createHash("sha256")
    .update("cymule.resource-manifest/3\0", "utf8")
    .update(identity, "utf8")
    .digest("hex")}`;
}

function isResourceMediaType(value: unknown): value is string {
  return typeof value === "string"
    && Buffer.byteLength(value) >= 1
    && Buffer.byteLength(value) <= 255
    && value.includes("/")
    && /^[\x00-\x7f]+$/.test(value)
    && value === value.toLowerCase()
    && !/\s/.test(value);
}

function isResourceToken(value: unknown): value is string {
  return typeof value === "string"
    && Buffer.byteLength(value) >= 1
    && Buffer.byteLength(value) <= 2_048
    && !/[\u0000-\u001f\u007f-\u009f]/.test(value)
    && !hasUnpairedSurrogate(value);
}

function validateWaitActivation(value: unknown): void {
  requireClosedRecord(value, ["activation_id", "activation_version", "result", "source", "wait_ids"], "wait activation");
  if (value.activation_version !== "cymule.wait-activation/2"
    || !isDurableIdentity(value.activation_id)) {
    throw transportError("invalid_engine_response", "wait activation version or identity is invalid");
  }
  requireStrictlyOrderedIdentities(value.wait_ids, "wait targets", isContentId);
  if (value.wait_ids.length === 0 || value.wait_ids.length > 4_096) {
    throw transportError("invalid_engine_response", "wait activation has no targets");
  }
  validateArtifactRef(value.result);
  if ((value.result as ArtifactRef).kind !== "cymule.wait-result/1") {
    throw transportError("invalid_engine_response", "wait activation result kind is invalid");
  }
  validateWaitActivationSource(value.source);
}

function validateWaitActivationSource(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "wait activation source is invalid");
  const identity = value.kind === "signal" ? value.key
    : value.kind === "timer" ? value.timer_id : undefined;
  const fields = value.kind === "signal" ? "key,kind"
    : value.kind === "timer" ? "kind,timer_id" : undefined;
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields
    || !isDurableIdentity(identity)) {
    throw transportError("invalid_engine_response", "wait activation source is not closed");
  }
}

function validateDurableCommand(value: unknown): void {
  if (!isRecord(value) || value.control_version !== "cymule.durable-control/4") throw transportError("invalid_engine_response", "durable command is invalid");
  const fields = new Map<string, string>([
    ["start_run", "candidate,control_version,execution,input,run_id,type"],
    ["resume_run", "control_version,execution,run_id,type"],
    ["takeover_run", "control_version,execution,expected_fence,run_id,type"],
    ["activate_wait", "activation_id,control_version,source,type,value,wait_ids"],
    ["release_effect", "control_version,execution,intent_id,type"],
    ["resolve_effect", "claim_epoch,claim_owner,control_version,execution_binding,intent_id,occurrence_binding,resolution,resolution_id,run_id,type,value"],
    ["cancel_run", "cancellation_id,control_version,reason,run_id,type"],
    ["run_index_page", "control_version,cursor,expected_revision,limit,max_canonical_bytes,type"],
    ["run_current", "control_version,expected_revision,run_id,type"],
    ["run_wait_page", "control_version,cursor,expected_revision,limit,max_canonical_bytes,run_id,type"],
    ["run_effect_page", "control_version,cursor,expected_revision,limit,max_canonical_bytes,run_id,type"],
    ["run_occurrence_page", "control_version,cursor,expected_revision,limit,max_canonical_bytes,run_id,type"],
    ["run_attempt_page", "control_version,cursor,expected_revision,limit,max_canonical_bytes,run_id,type"],
    ["run_item", "control_version,expected_revision,max_canonical_bytes,run_id,selector,type"],
  ]).get(String(value.type));
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) throw transportError("invalid_engine_response", "durable command is not closed");
  if (value.type === "start_run") {
    if (!isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "start Run identity is invalid");
    }
    validatePlanCandidate(value.candidate);
    validateExecutionClaimRequest(value.execution);
  } else if (value.type === "resume_run" || value.type === "takeover_run") {
    if (!isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "resume Run identity is invalid");
    }
    validateExecutionClaimRequest(value.execution);
    if (value.type === "takeover_run"
      && (!Number.isSafeInteger(value.expected_fence) || Number(value.expected_fence) < 1)) {
      throw transportError("invalid_engine_response", "takeover fence is invalid");
    }
  } else if (value.type === "activate_wait") {
    if (!isClockIdentity(value.activation_id)) {
      throw transportError("invalid_engine_response", "durable activation identity is invalid");
    }
    validateWaitActivationSource(value.source);
    requireStrictlyOrderedIdentities(value.wait_ids, "durable activation targets", isContentId);
    if (value.wait_ids.length === 0 || value.wait_ids.length > 4_096) {
      throw transportError("invalid_engine_response", "durable activation target count is invalid");
    }
  } else if (value.type === "release_effect") {
    if (!isContentId(value.intent_id)) {
      throw transportError("invalid_engine_response", "effect intent identity is invalid");
    }
    validateExecutionClaimRequest(value.execution);
  } else if (value.type === "resolve_effect") {
    for (const field of [
      "resolution_id", "run_id", "intent_id", "occurrence_binding", "claim_owner",
    ]) {
      if (!isDurableIdentity(value[field])) {
        throw transportError("invalid_engine_response", "Effect resolution identity is invalid");
      }
    }
    validateArtifactRef(value.execution_binding);
    if (!isRecord(value.execution_binding)
      || value.execution_binding.kind !== "cymule.execution-binding/2"
      || !isContentId(value.intent_id)
      || !Number.isSafeInteger(value.claim_epoch) || Number(value.claim_epoch) < 1
      || !new Set(["resolved_applied", "resolved_not_applied"]).has(String(value.resolution))
      || value.resolution === "resolved_not_applied" && value.value !== null) {
      throw transportError("invalid_engine_response", "Effect resolution authority is invalid");
    }
  } else if (value.type === "cancel_run") {
    if (!isClockIdentity(value.cancellation_id) || !isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "cancellation identity is invalid");
    }
  } else if (value.type === "run_current") {
    if (!isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "Run-current query identity is invalid");
    }
    validateExpectedRevision(value.expected_revision);
  } else if (value.type === "run_item") {
    if (!isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "Run-item query identity is invalid");
    }
    validateExpectedRevision(value.expected_revision);
    validateDurableRunItemSelector(value.selector);
    if (!isPositiveIntegerAtMost(value.max_canonical_bytes, 13 * 1024 * 1024)) {
      throw transportError("invalid_engine_response", "Run-item query byte budget is invalid");
    }
  } else if (new Set([
    "run_index_page", "run_wait_page", "run_effect_page",
    "run_occurrence_page", "run_attempt_page",
  ]).has(String(value.type))) {
    const queryKind = new Map<string, DurablePageQueryKind>([
      ["run_index_page", "run_index"],
      ["run_wait_page", "run_waits"],
      ["run_effect_page", "run_effects"],
      ["run_occurrence_page", "run_occurrences"],
      ["run_attempt_page", "run_attempts"],
    ]).get(String(value.type))!;
    const runId = value.type === "run_index_page" ? null : value.run_id;
    if (runId !== null && !isRunIdentity(runId)) {
      throw transportError("invalid_engine_response", "Run page query identity is invalid");
    }
    validateExpectedRevision(value.expected_revision);
    if (!isPositiveIntegerAtMost(value.limit, 256)
      || !isPositiveIntegerAtMost(value.max_canonical_bytes, 1024 * 1024)) {
      throw transportError("invalid_engine_response", "durable page query bounds are invalid");
    }
    if (value.cursor !== null) {
      validateDurablePageCursor(value.cursor);
      if (value.cursor.query_kind !== queryKind
        || value.cursor.run_id !== runId
        || value.expected_revision !== value.cursor.source_revision) {
        throw transportError("invalid_engine_response", "durable page cursor scope is invalid");
      }
    }
  }
}

function validateExpectedRevision(value: unknown): void {
  if (value !== null && !isContentId(value)) {
    throw transportError("invalid_engine_response", "durable query revision is invalid");
  }
}

function isPositiveIntegerAtMost(value: unknown, maximum: number): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 1 && Number(value) <= maximum;
}

function validateExecutionClaimRequest(value: unknown): void {
  requireClosedRecord(value, ["clock", "owner", "ttl"], "execution claim request");
  if (!isClockIdentity(value.owner) || !Number.isSafeInteger(value.ttl) || Number(value.ttl) < 1) {
    throw transportError("invalid_engine_response", "execution claim owner or TTL is invalid");
  }
  validateClockObservationRef(value.clock);
}

function validateClockObservationRef(value: unknown): void {
  requireClosedRecord(value, ["clock_version", "observation_id", "scope", "source_generation", "source_id"], "Clock observation reference");
  if (value.clock_version !== "cymule.clock-observation/2" ||
    !/^sha256:[0-9a-f]{64}$/.test(String(value.observation_id)) ||
    !/^sha256:[0-9a-f]{64}$/.test(String(value.source_generation)) ||
    !isClockIdentity(value.source_id) || !isClockIdentity(value.scope)) {
    throw transportError("invalid_engine_response", "Clock observation reference is invalid");
  }
}

function validateClockObservationResult(value: unknown): void {
  requireClosedRecord(value, ["run_id", "observation"], "Clock observation result");
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "Clock observation Run is invalid");
  }
  validateClockObservationRef(value.observation);
}

function validateEngineStoreTarget(value: unknown): asserts value is EngineStoreTarget {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Engine Store target is invalid");
  }
  const keys = Object.keys(value).sort().join(",");
  if (keys !== "location,provider" && keys !== "domain,location,provider") {
    throw transportError("invalid_engine_response", "Engine Store target is not closed");
  }
  if (!hasUnicodeScalarLength(value.provider, 1, 256)
    || !hasUnicodeScalarLength(value.location, 1, 4_096)) {
    throw transportError("invalid_engine_response", "Engine Store target fields are invalid");
  }
  if ("domain" in value && !hasUnicodeScalarLength(value.domain, 1, 512)) {
    throw transportError("invalid_engine_response", "Engine Store target domain is invalid");
  }
}

function validateProcessMap(
  value: unknown,
  label: string,
  requireNonEmptyValues: boolean,
  requireNonEmptyMap: boolean,
  requireContentIds: boolean,
): asserts value is Record<string, string> {
  if (!isRecord(value)
    || Object.keys(value).length > 4_096
    || requireNonEmptyMap && Object.keys(value).length === 0) {
    throw transportError("invalid_engine_response", `${label} is invalid`);
  }
  for (const [key, member] of Object.entries(value)) {
    if (key.length === 0 || key.includes("=")
      || /[\u0000-\u001f\u007f-\u009f]/.test(key)
      || typeof member !== "string" || member.includes("\0")
      || requireNonEmptyValues && member.length === 0
      || requireContentIds && !isContentId(member)) {
      throw transportError("invalid_engine_response", `${label} is outside its closed contract`);
    }
  }
}

function validateEngineProcessConfig(
  value: unknown,
  expectedMessageLimit?: number,
): asserts value is EngineProcessConfig {
  requireClosedRecord(value, [
    "executable", "arguments", "environment", "working_directory", "runtime_closure",
    "timeout_ms", "message_limit", "closure_limit",
  ], "Engine process configuration");
  if (!hasUnicodeScalarLength(value.executable, 1, 4_096)
    || !isAbsolute(value.executable as string)
    || (value.executable as string).includes("\0")
    || !Array.isArray(value.arguments)
    || value.arguments.length > 4_096
    || !value.arguments.every((argument) => typeof argument === "string" && !argument.includes("\0"))
    || value.working_directory !== null
      && (!hasUnicodeScalarLength(value.working_directory, 1, 4_096)
        || !isAbsolute(value.working_directory as string)
        || (value.working_directory as string).includes("\0"))
    || !Number.isSafeInteger(value.timeout_ms) || Number(value.timeout_ms) < 1
    || !Number.isSafeInteger(value.message_limit) || Number(value.message_limit) < 1
      || Number(value.message_limit) > 64 * 1024 * 1024
    || !Number.isSafeInteger(value.closure_limit) || Number(value.closure_limit) < 1
      || Number(value.closure_limit) > 1024 * 1024 * 1024) {
    throw transportError(
      "invalid_engine_response",
      "Engine process configuration is outside the bounded contract",
    );
  }
  validateProcessMap(value.environment, "Engine process environment", false, false, false);
  validateProcessMap(value.runtime_closure, "Engine runtime closure", true, true, true);
  const messageLimit = Number(value.message_limit);
  if (expectedMessageLimit === undefined
    ? ![8 * 1024 * 1024, 16 * 1024 * 1024].includes(messageLimit)
    : messageLimit !== expectedMessageLimit) {
    throw transportError(
      "invalid_engine_response",
      "Engine process message limit does not match its protocol context",
    );
  }
}

function validateEnginePluginTarget(
  value: unknown,
  requireRevision: boolean,
  expectedMessageLimit?: number,
): asserts value is EnginePluginTarget {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Engine plugin target is invalid");
  }
  const keys = Object.keys(value).sort().join(",");
  if (keys !== "process,provider" && keys !== "process,provider,revision") {
    throw transportError("invalid_engine_response", "Engine plugin target is not closed");
  }
  if (value.provider !== "cymule.executor-process/1") {
    throw transportError("invalid_engine_response", "Engine plugin provider is unsupported");
  }
  validateEngineProcessConfig(value.process, expectedMessageLimit);
  if ("revision" in value && !/^sha256:[0-9a-f]{64}$/.test(String(value.revision))) {
    throw transportError("invalid_engine_response", "Engine plugin revision is invalid");
  }
  if (requireRevision && !("revision" in value)) {
    throw transportError("invalid_engine_response", "evolution plugin target is not revision-pinned");
  }
}

function validateEngineClockTarget(value: unknown): asserts value is EngineClockTarget {
  requireClosedRecord(
    value,
    ["provider", "location", "source_id", "source_generation"],
    "Engine Clock target",
  );
  if (value.provider !== "cymule.clock-system/2"
    || !hasUnicodeScalarLength(value.location, 1, 4_096)
    || !isClockIdentity(value.source_id)
    || !/^sha256:[0-9a-f]{64}$/.test(String(value.source_generation))) {
    throw transportError("invalid_engine_response", "Engine Clock target is invalid");
  }
}

function validateEngineDurableTarget(
  value: unknown,
  command: DurableCommand,
): asserts value is EngineDurableTarget {
  if (!isRecord(value) || !("store" in value)
    || !Object.keys(value).every((key) => new Set(["store", "executor", "clock"]).has(key))) {
    throw transportError("invalid_engine_response", "durable Engine target is not closed");
  }
  validateEngineStoreTarget(value.store);
  const requiresExecutor = new Set([
    "start_run", "resume_run", "takeover_run", "release_effect", "resolve_effect",
  ]).has(command.type);
  const requiresClock = new Set([
    "start_run", "resume_run", "takeover_run", "release_effect",
  ]).has(command.type);
  if (requiresExecutor !== ("executor" in value)
    || requiresClock !== ("clock" in value)) {
    throw transportError(
      "invalid_engine_response",
      "durable Engine target does not match the command capability",
    );
  }
  if ("executor" in value) {
    validateEnginePluginTarget(value.executor, false, 8 * 1024 * 1024);
  }
  if ("clock" in value) validateEngineClockTarget(value.clock);
}

function validateEngineEvolutionTarget(
  value: unknown,
  command: LiveEvolutionCommand,
): asserts value is EngineEvolutionTarget {
  if (!isRecord(value)
    || !hasExactKeys(value, [
      "store", "migration_adapter", "shadow_driver", "target_execution_bindings",
    ])) {
    throw transportError("invalid_engine_response", "evolution Engine target is not closed");
  }
  validateEngineStoreTarget(value.store);
  let operation: EvolutionCommand["operation"] | undefined;
  let targetPlan: string | undefined;
  if (command.operation === "apply") {
    operation = command.command.operation;
    if (command.command.operation === "migrate") {
      targetPlan = command.command.request.to_plan;
    }
  }
  if (!isRecord(value.target_execution_bindings)
    || Object.keys(value.target_execution_bindings).length > 1) {
    throw transportError("invalid_engine_response", "target execution bindings are outside bounds");
  }
  for (const [planId, target] of Object.entries(value.target_execution_bindings)) {
    if (!isContentId(planId) || targetPlan === undefined || planId !== targetPlan) {
      throw transportError("invalid_engine_response", "target execution binding Plan is invalid");
    }
    validateEnginePluginTarget(target, true, 8 * 1024 * 1024);
  }
  if ((operation === "migrate") !== (value.migration_adapter !== null)
    || (operation === "shadow") !== (value.shadow_driver !== null)) {
    throw transportError(
      "invalid_engine_response",
      "evolution Engine plugin presence does not match the command",
    );
  }
  if (value.migration_adapter !== null) {
    const request = command.operation === "apply" && command.command.operation === "migrate"
      ? command.command.request
      : undefined;
    if (request === undefined
      || !hasExactKeys(value.migration_adapter, ["adapter_id", "adapter_revision", "process"])
      || value.migration_adapter.adapter_id !== request.adapter_id
      || value.migration_adapter.adapter_revision !== request.adapter_revision) {
      throw transportError(
        "invalid_engine_response",
        "migration Engine target does not match the semantic command",
      );
    }
    validateEnginePluginTarget(value.migration_adapter.process, true, 16 * 1024 * 1024);
    if (value.migration_adapter.process.revision !== value.migration_adapter.adapter_revision) {
      throw transportError(
        "invalid_engine_response",
        "migration Engine process revision does not match the adapter revision",
      );
    }
  }
  if (value.shadow_driver !== null) {
    const request = command.operation === "apply" && command.command.operation === "shadow"
      ? command.command.request
      : undefined;
    if (request === undefined
      || !hasExactKeys(value.shadow_driver, ["driver_id", "driver_revision", "process"])
      || value.shadow_driver.driver_id !== request.driver_id
      || value.shadow_driver.driver_revision !== request.driver_revision) {
      throw transportError(
        "invalid_engine_response",
        "shadow Engine target does not match the semantic command",
      );
    }
    validateEnginePluginTarget(value.shadow_driver.process, true, 16 * 1024 * 1024);
    if (value.shadow_driver.process.revision !== value.shadow_driver.driver_revision) {
      throw transportError(
        "invalid_engine_response",
        "shadow Engine process revision does not match the driver revision",
      );
    }
  }
}

function isClockIdentity(value: unknown): value is string {
  return isDurableIdentity(value);
}

function isRunIdentity(value: unknown): value is string {
  return isDurableIdentity(value);
}

function isDurableIdentity(value: unknown): value is string {
  return hasUnicodeScalarLength(value, 1, 512)
    && !/[\u0000-\u001f\u007f-\u009f]/.test(value);
}

function requireRequestRunIdentity(value: unknown): asserts value is string {
  if (!isRunIdentity(value)) {
    throw localValidationError(
      "validate_request",
      "invalid_run_identity",
      new Error("Run identity must contain 1..=512 Unicode scalars without controls"),
    );
  }
}

function isEngineWireIdentity(value: unknown): value is string {
  return typeof value === "string"
    && Buffer.byteLength(value) >= 1
    && Buffer.byteLength(value) <= 512
    && !/[\u0000-\u001f\u007f-\u009f]/.test(value)
    && !hasUnpairedSurrogate(value);
}

function validateExecutionOutcome(value: unknown): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "execution outcome is not an object");
  }
  const expected = value.status === "completed"
    ? "result,status"
    : value.status === "suspended"
    ? "status,suspension"
    : value.status === "release_required"
    ? "release,status"
    : value.status === "reconciliation_required"
    ? "reconciliation,status"
    : undefined;
  if (expected === undefined || Object.keys(value).sort().join(",") !== expected) {
    throw transportError("invalid_engine_response", "execution outcome is not closed");
  }
  const nested = value.status === "completed"
    ? value.result
    : value.status === "suspended"
    ? value.suspension
    : value.status === "release_required"
    ? value.release
    : value.reconciliation;
  const nestedFields = value.status === "completed"
    ? "effects,plan_id,precondition_token,projection_digest,run_id,value"
    : value.status === "suspended"
    ? "definition_id,invocation_id,plan_id,result_bind,run_id,site_id,wait"
    : value.status === "release_required"
    ? "intent_ids,plan_id,run_id"
    : "intent_id,plan_id,run_id";
  if (!isRecord(nested) || Object.keys(nested).sort().join(",") !== nestedFields) {
    throw transportError("invalid_engine_response", "execution payload fields are not closed");
  }
  if (!isRunIdentity(nested.run_id) || !isContentId(nested.plan_id)) {
    throw transportError("invalid_engine_response", "execution Run or Plan identity is invalid");
  }
  if (value.status === "completed") {
    validateExecutionResult(nested);
  } else if (value.status === "suspended") {
    requireStrings(nested, ["run_id", "plan_id", "definition_id", "invocation_id", "site_id"]);
    if (!isRunIdentity(nested.run_id)
      || !isContentId(nested.invocation_id)
      || !isEngineWireIdentity(nested.definition_id)
      || !isEngineWireIdentity(nested.site_id)
      || (nested.result_bind !== null && !isEngineWireIdentity(nested.result_bind))) {
      throw transportError("invalid_engine_response", "wait result binding is invalid");
    }
    validateWaitSpec(nested.wait);
    const wait = nested.wait as Record<string, unknown>;
    const waitIdentity = wait.kind === "signal" ? wait.key
      : wait.kind === "timer" ? wait.timer_id : wait.correlation;
    if (!isEngineWireIdentity(waitIdentity)) {
      throw transportError("invalid_engine_response", "execution wait identity is invalid");
    }
  } else if (value.status === "release_required") {
    requireStrings(nested, ["run_id", "plan_id"]);
    if (!isRunIdentity(nested.run_id) || !isContentId(nested.plan_id)) {
      throw transportError("invalid_engine_response", "effect release Run identity is invalid");
    }
    requireStrictlyOrderedIdentities(nested.intent_ids, "effect release intents", isContentId);
    if (nested.intent_ids.length === 0) {
      throw transportError("invalid_engine_response", "effect release intents are invalid");
    }
  } else {
    requireStrings(nested, ["run_id", "plan_id", "intent_id"]);
    if (!isRunIdentity(nested.run_id) || !isContentId(nested.plan_id) || !isContentId(nested.intent_id)) {
      throw transportError("invalid_engine_response", "effect reconciliation intent is invalid");
    }
  }
}

function validateMigrationContinuation(value: unknown): void {
  if (!isRecord(value) || Object.keys(value).sort().join(",") !== [
    "binding_context", "continuation_version", "epoch", "execution_claim", "execution_fence", "frames", "plan_id",
    "run_id", "scope_stack", "state", "status", "wait_set",
  ].join(",")) {
    throw transportError("invalid_engine_response", "migration Continuation is not closed");
  }
  if (value.continuation_version !== "cymule.continuation-state/1") {
    throw transportError("invalid_engine_response", "migration Continuation generation is unsupported");
  }
  requireStrings(value, ["run_id", "plan_id", "binding_context"]);
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "migration Continuation Run identity is invalid");
  }
  requireEpoch(value.epoch);
  requireEpoch(value.execution_fence);
  if (value.execution_claim !== null || value.status !== "ready") {
    throw transportError("invalid_engine_response", "migration Continuation must be ready without an execution claim");
  }
  for (const field of ["wait_set", "scope_stack"] as const) {
    if (!Array.isArray(value[field])
      || (field === "scope_stack" && value[field].length === 0)
      || value[field].some((item) => typeof item !== "string" || item.length === 0)) {
      throw transportError("invalid_engine_response", `migration Continuation ${field} is invalid`);
    }
  }
  if (value.state !== null) validateArtifactRef(value.state);
  if (!Array.isArray(value.frames) || value.frames.length === 0) {
    throw transportError("invalid_engine_response", "migration Continuation frames are invalid");
  }
  for (const frame of value.frames) {
    if (!isRecord(frame) || Object.keys(frame).sort().join(",") !== [
      "definition_id", "input", "invocation_id", "invocation_path", "locals", "next_step",
      "region_path", "scope_id",
    ].join(",")) {
      throw transportError("invalid_engine_response", "migration frame is not closed");
    }
    requireStrings(frame, ["definition_id", "invocation_id", "scope_id"]);
    requireEpoch(frame.next_step);
    validateArtifactRef(frame.input);
    if (!Array.isArray(frame.region_path) || frame.region_path.some((index) => !Number.isSafeInteger(index) || index < 0)) {
      throw transportError("invalid_engine_response", "migration frame Region path is invalid");
    }
    if (!isRecord(frame.locals)) throw transportError("invalid_engine_response", "migration frame locals are invalid");
    for (const reference of Object.values(frame.locals)) validateArtifactRef(reference);
    if (!Array.isArray(frame.invocation_path)) throw transportError("invalid_engine_response", "migration invocation path is invalid");
    for (const segment of frame.invocation_path) {
      if (!isRecord(segment) || Object.keys(segment).sort().join(",") !== "region_path,scope_id,site_id") {
        throw transportError("invalid_engine_response", "migration invocation segment is not closed");
      }
      requireStrings(segment, ["site_id", "scope_id"]);
      if (!Array.isArray(segment.region_path) || segment.region_path.some((index) => !Number.isSafeInteger(index) || index < 0)) {
        throw transportError("invalid_engine_response", "migration invocation Region path is invalid");
      }
    }
  }
}

function validateEvolutionCommand(value: unknown): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "evolution command is not an object");
  }
  const fields = new Map<string, string>([
    ["apply_patch", "command_id,control_version,operation,patch"],
    ["set_rollout", "command_id,control_version,decision,operation"],
    ["select_occurrence", "command_id,control_version,execution_binding,occurrence_id,operation,selection_id"],
    ["migrate", "command_id,control_version,operation,request"],
    ["restart_under_new_plan", "command_id,control_version,operation,request"],
    ["shadow", "command_id,control_version,operation,request"],
    ["observe", "command_id,control_version,observation,operation"],
    ["apply_gate", "command_id,control_version,gate,next_decision_id,operation"],
  ]).get(String(value.operation));
  if (
    value.control_version !== "cymule.evolution-control/5"
    || fields === undefined
    || Object.keys(value).sort().join(",") !== fields
  ) {
    throw transportError("invalid_engine_response", "evolution command is not closed");
  }
  requireWireIdentities(value, ["command_id"], "evolution command");
  if (value.operation === "apply_patch") {
    if (!isRecord(value.patch) || !isWireIdentity(value.patch.from_plan)) {
      throw transportError("invalid_engine_response", "Plan patch parent identity is invalid");
    }
  } else if (value.operation === "set_rollout") {
    if (!isRecord(value.decision) || !isWireIdentity(value.decision.decision_id)) {
      throw transportError("invalid_engine_response", "rollout decision identity is invalid");
    }
  } else if (value.operation === "select_occurrence") {
    requireWireIdentities(value, ["occurrence_id", "selection_id"], "occurrence selection");
    validateArtifactRef(value.execution_binding);
    if (value.execution_binding.kind !== "cymule.execution-binding/2") {
      throw transportError("invalid_engine_response", "occurrence binding is not an ExecutionBinding Artifact");
    }
  } else if (value.operation === "migrate") {
    validateMigrationRequest(value.request);
  } else if (value.operation === "restart_under_new_plan") {
    validateRestartRequest(value.request);
  } else if (value.operation === "shadow") {
    validateEvolutionRequest(value.request, [
      "comparison_id", "comparison_policy", "decision_id", "driver_id", "driver_revision",
      "input", "primary_plan", "shadow_plan", "subject",
    ], [
      "comparison_id", "decision_id", "subject", "primary_plan", "shadow_plan",
      "driver_id", "comparison_policy",
    ]);
    requireWireIdentities(
      value.request,
      ["comparison_id", "driver_id"],
      "shadow request",
    );
    if (!isContentId(value.request.primary_plan)
      || !isContentId(value.request.shadow_plan)
      || value.request.primary_plan === value.request.shadow_plan
      || !isContentId(value.request.driver_revision)) {
      throw transportError("invalid_engine_response", "shadow request lineage is invalid");
    }
    validateArtifactRef((value.request as Record<string, unknown>).input);
  } else if (value.operation === "observe") {
    if (!isRecord(value.observation) || !isWireIdentity(value.observation.observation_id)) {
      throw transportError("invalid_engine_response", "rollout observation identity is invalid");
    }
  } else if (value.operation === "apply_gate") {
    validateRolloutGate(value.gate);
    if (!isWireIdentity(value.next_decision_id)) {
      throw transportError("invalid_engine_response", "next rollout decision identity is invalid");
    }
  }
}

function validateLiveEvolutionCommand(value: unknown): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "live evolution command is not an object");
  }
  const fields = new Map<string, string[]>([
    ["publish_definition", ["command_id,control_version,definition,logical_ref,operation,references"]],
    ["register_template", ["command_id,control_version,operation,template"]],
    ["publish_and_relink", ["command_id,control_version,operation,publication"]],
    ["apply", ["command,command_id,control_version,operation,template_id"]],
  ]).get(String(value.operation));
  if (
    value.control_version !== "cymule.live-evolution-control/6"
    || fields === undefined
    || !fields.includes(Object.keys(value).sort().join(","))
  ) {
    throw transportError("invalid_engine_response", "live evolution command is not closed");
  }
  requireWireIdentities(value, ["command_id"], "live evolution command");
  if (value.operation === "publish_definition") {
    if (!isWireIdentity(value.logical_ref) || !isRecord(value.definition)
      || !isWireIdentity(value.definition.id)) {
      throw transportError("invalid_engine_response", "definition publication identity is invalid");
    }
    validateRegistryDefinition(value.definition);
    validatePublishedSubflowReferences(value.references, new Set([value.definition.id]));
  } else if (value.operation === "register_template") {
    validatePlanTemplate(value.template);
  } else if (value.operation === "publish_and_relink") {
    requireClosedRecord(
      value.publication,
      ["logical_ref", "definition", "references", "evidence", "mode"],
      "live publication",
    );
    if (!isWireIdentity(value.publication.logical_ref)
      || !isRecord(value.publication.definition)
      || !isWireIdentity(value.publication.definition.id)) {
      throw transportError("invalid_engine_response", "live publication identity is invalid");
    }
    validateRegistryDefinition(value.publication.definition);
    validatePublishedSubflowReferences(
      value.publication.references,
      new Set([value.publication.definition.id]),
    );
    validateArtifactRecord(value.publication.evidence);
    validateRolloutMode(value.publication.mode);
  } else if (value.operation === "apply") {
    if (!isWireIdentity(value.template_id)) {
      throw transportError("invalid_engine_response", "live evolution template identity is invalid");
    }
    validateEvolutionCommand(value.command);
  }
}

function validateRolloutMode(value: unknown): void {
  if (!isRecord(value) || typeof value.mode !== "string") {
    throw transportError("invalid_engine_response", "rollout mode is invalid");
  }
  if (value.mode === "canary") {
    requireClosedRecord(value, ["mode", "basis_points"], "canary rollout mode");
    if (!Number.isSafeInteger(value.basis_points)
      || Number(value.basis_points) < 0
      || Number(value.basis_points) > 10_000) {
      throw transportError("invalid_engine_response", "canary basis points are invalid");
    }
    return;
  }
  if (!new Set(["shadow", "active", "rolled_back"]).has(value.mode)) {
    throw transportError("invalid_engine_response", "rollout mode is invalid");
  }
  requireClosedRecord(value, ["mode"], "rollout mode");
}

function validatePlanTemplate(value: unknown): asserts value is Record<string, unknown> & {
  template_id: string;
  candidate: PlanCandidate;
  references: SubflowReference[];
} {
  requireClosedRecord(value, ["template_id", "candidate", "references"], "Plan template");
  if (!isWireIdentity(value.template_id) || !Array.isArray(value.references)) {
    throw transportError("invalid_engine_response", "Plan template identity or references are invalid");
  }
  validatePlanCandidate(value.candidate, false);
  for (const reference of value.references) {
    validateSubflowReference(reference, false);
  }
}

function validateMigrationRequest(value: unknown): void {
  validateEvolutionRequest(value, [
    "adapter_id", "adapter_revision", "compatibility_id", "expected_source_epoch",
    "from_plan", "migration_id", "plan_edge_id", "run_id", "to_plan",
  ], [
    "migration_id", "run_id", "from_plan", "to_plan", "plan_edge_id",
    "compatibility_id", "adapter_id",
  ]);
  requireWireIdentities(value, ["migration_id", "adapter_id"], "migration request");
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "migration request Run identity is invalid");
  }
  for (const field of [
    "from_plan", "to_plan", "plan_edge_id", "compatibility_id", "adapter_revision",
  ] as const) {
    if (!isContentId(value[field])) {
      throw transportError("invalid_engine_response", `migration ${field} identity is invalid`);
    }
  }
  if (value.from_plan === value.to_plan) {
    throw transportError("invalid_engine_response", "migration request Plans must be distinct");
  }
  requireEpoch(value.expected_source_epoch);
}

function validateRestartRequest(value: unknown): void {
  validateEvolutionRequest(value, [
    "evidence", "expected_source_epoch", "from_plan", "input", "replacement_run",
    "restart_id", "run_id", "to_plan",
  ], ["restart_id", "replacement_run", "run_id", "from_plan", "to_plan"]);
  requireWireIdentities(value, ["restart_id"], "restart request");
  if (!isRunIdentity(value.replacement_run)
    || !isRunIdentity(value.run_id)
    || value.replacement_run === value.run_id
    || !isContentId(value.from_plan)
    || !isContentId(value.to_plan)
    || value.from_plan === value.to_plan) {
    throw transportError("invalid_engine_response", "restart target identity is invalid");
  }
  requireEpoch(value.expected_source_epoch);
  validateArtifactRef(value.input);
  validateArtifactRef(value.evidence);
}

function validateDurableResponse(value: unknown): void {
  if (!isRecord(value) || typeof value.type !== "string") {
    throw transportError("invalid_engine_response", "durable response is not tagged");
  }
  const keys = new Map<string, string>([
    ["run_boundary", "boundary,type"], ["wait_activated", "receipt,type"],
    ["effect_resolved", "receipt,type"], ["run_cancelled", "receipt,type"],
    ["run_index_page", "page,type"],
    ["run_current", "current,observed_revision,source_root,type"],
    ["run_wait_page", "page,run_id,type"],
    ["run_effect_page", "page,run_id,type"],
    ["run_occurrence_page", "page,run_id,type"],
    ["run_attempt_page", "page,run_id,type"],
    ["run_item", "item,observed_revision,run_id,source_root,type"],
  ]).get(value.type);
  if (keys === undefined || Object.keys(value).sort().join(",") !== keys) {
    throw transportError("invalid_engine_response", "durable response fields are not closed");
  }
  if (value.type === "run_boundary") {
    validateDurableBoundary(value.boundary);
  } else if (value.type === "wait_activated") {
    validateWaitActivationReceipt(value.receipt);
  } else if (value.type === "effect_resolved") {
    validateEffectResolutionReceipt(value.receipt);
  } else if (value.type === "run_cancelled") {
    validateRunCancellationReceipt(value.receipt);
  } else if (value.type === "run_index_page") {
    validateDurableQueryPage(value.page, "run_index", null, validateDurableRunIndexSummary);
  } else if (value.type === "run_current") {
    validateDurableQuerySource(value.observed_revision, value.source_root);
    if (value.current !== null) validateDurableRunCurrent(value.current);
  } else if (value.type === "run_wait_page") {
    validateDurableRunPage(value, "run_waits", validateDurableWaitSummary);
  } else if (value.type === "run_effect_page") {
    validateDurableRunPage(value, "run_effects", validateDurableEffectSummary);
  } else if (value.type === "run_occurrence_page") {
    validateDurableRunPage(value, "run_occurrences", validateDurableOccurrenceSummary);
  } else if (value.type === "run_attempt_page") {
    validateDurableRunPage(value, "run_attempts", validateDurableAttemptSummary);
  } else if (value.type === "run_item") {
    if (!isRunIdentity(value.run_id)) {
      throw transportError("invalid_engine_response", "Run-item owner is invalid");
    }
    validateDurableQuerySource(value.observed_revision, value.source_root);
    if (value.item !== null) {
      validateDurableRunItem(value.item);
      if (durableRunItemOwner(value.item as DurableRunItem) !== value.run_id) {
        throw transportError("invalid_engine_response", "Run item escaped its owner");
      }
    }
  }
  const queryLimit = value.type === "run_item"
    ? DURABLE_QUERY_EXACT_RESPONSE_BYTES
    : new Set<string>([
      "run_index_page", "run_current", "run_wait_page", "run_effect_page",
      "run_occurrence_page", "run_attempt_page",
    ]).has(value.type)
    ? DURABLE_QUERY_PAGE_BYTES
    : undefined;
  if (queryLimit !== undefined
    && Buffer.byteLength(JSON.stringify(value), "utf8") > queryLimit) {
    throw transportError("invalid_engine_response", "durable query response is oversized");
  }
}

function validateWaitActivationReceipt(value: unknown): asserts value is WaitActivationReceipt {
  requireClosedRecord(
    value,
    ["receipt_version", "activation", "applied_wait_ids", "ready_run_ids"],
    "wait activation receipt",
  );
  if (value.receipt_version !== "cymule.wait-activation-receipt/3") {
    throw transportError("invalid_engine_response", "wait activation receipt version is invalid");
  }
  validateWaitActivation(value.activation);
  requireStrictlyOrderedIdentities(value.applied_wait_ids, "applied wait identities", isContentId);
  requireStrictlyOrderedIdentities(value.ready_run_ids, "ready Run identities", isRunIdentity);
  const selected = new Set((value.activation as WaitActivation).wait_ids);
  if (!(value.applied_wait_ids as string[]).every((waitId) => selected.has(waitId))) {
    throw transportError(
      "invalid_engine_response",
      "wait activation receipt applied targets escape the selected set",
    );
  }
  if ((value.applied_wait_ids as string[]).length === 0
    && (value.ready_run_ids as string[]).length !== 0) {
    throw transportError(
      "invalid_engine_response",
      "terminal non-winner activation readied a Run",
    );
  }
}

function validateEffectResolutionReceipt(value: unknown): void {
  requireClosedRecord(value, [
    "receipt_version", "command", "actual_resolution", "actual_value", "result",
    "receipt_id",
  ], "Effect resolution receipt");
  if (value.receipt_version !== "cymule.effect-resolution-receipt/1"
    || !new Set(["resolved_applied", "resolved_not_applied"]).has(
      String(value.actual_resolution),
    )
    || typeof value.receipt_id !== "string" || !/^[0-9a-f]{64}$/.test(value.receipt_id)) {
    throw transportError("invalid_engine_response", "Effect resolution receipt is invalid");
  }
  validateEffectResolutionCommand(value.command);
  if (value.result !== null) {
    validateArtifactRef(value.result);
    if (!isRecord(value.result) || value.result.kind !== "cymule.effect-result/1") {
      throw transportError(
        "invalid_engine_response",
        "Effect resolution result kind is invalid",
      );
    }
  }
  if (value.actual_resolution === "resolved_applied" && value.result === null
    || value.actual_resolution === "resolved_not_applied"
      && (value.actual_value !== null || value.result !== null)) {
    throw transportError(
      "invalid_engine_response",
      "Effect resolution value and result presence disagree",
    );
  }
}

function validateEffectResolutionCommand(value: unknown): asserts value is EffectResolutionCommand {
  requireClosedRecord(value, [
    "resolution_id", "run_id", "intent_id", "execution_binding", "occurrence_binding",
    "claim_owner", "claim_epoch", "resolution", "value",
  ], "Effect resolution command");
  if (![value.resolution_id, value.run_id, value.intent_id, value.occurrence_binding,
    value.claim_owner].every(isDurableIdentity)
    || !isContentId(value.intent_id)
    || !isContentId(value.occurrence_binding)
    || !Number.isSafeInteger(value.claim_epoch) || Number(value.claim_epoch) < 1
    || !new Set(["resolved_applied", "resolved_not_applied"]).has(String(value.resolution))
    || value.resolution === "resolved_not_applied" && value.value !== null) {
    throw transportError("invalid_engine_response", "Effect resolution command is invalid");
  }
  validateArtifactRef(value.execution_binding);
  if ((value.execution_binding as ArtifactRef).kind !== "cymule.execution-binding/2") {
    throw transportError("invalid_engine_response", "Effect resolution binding is invalid");
  }
}

function validateRunCancellationReceipt(value: unknown): void {
  requireClosedRecord(value, [
    "receipt_version", "command", "boundary", "receipt_id",
  ], "Run cancellation receipt");
  if (value.receipt_version !== "cymule.run-cancellation-receipt/1"
    || typeof value.receipt_id !== "string" || !/^[0-9a-f]{64}$/.test(value.receipt_id)) {
    throw transportError("invalid_engine_response", "Run cancellation receipt is invalid");
  }
  validateCancellationCommand(value.command);
  validateDurableBoundary(value.boundary);
  if (!isRecord(value.boundary) || value.boundary.status !== "cancelled") {
    throw transportError(
      "invalid_engine_response",
      "Run cancellation receipt boundary is invalid",
    );
  }
  if (!isRecord(value.boundary.reason)
    || value.boundary.reason.kind !== "cymule.cancellation-reason/1") {
    throw transportError("invalid_engine_response", "Run cancellation reason kind is invalid");
  }
}

function validateCancellationCommand(value: unknown): asserts value is CancellationCommand {
  requireClosedRecord(
    value,
    ["cancellation_id", "run_id", "reason"],
    "Run cancellation command",
  );
  if (!isDurableIdentity(value.cancellation_id) || !isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "Run cancellation command is invalid");
  }
}

function validateDurableBoundary(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "durable boundary is invalid");
  const fields = new Map<string, string>([
    ["suspended", "status,wait_id"],
    ["reconciliation_required", "intent_id,status"],
    ["effect_unavailable", "intent_id,status"],
    ["effect_not_applied", "intent_id,status"],
    ["release_required", "intent_ids,status"],
    ["completed", "result,status"],
    ["failed", "failure,status"],
    ["cancelled", "reason,status"],
  ]).get(String(value.status));
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) {
    throw transportError("invalid_engine_response", "durable boundary is not closed");
  }
  if (value.status === "suspended" && !isClockIdentity(value.wait_id)) {
    throw transportError("invalid_engine_response", "suspended wait identity is invalid");
  }
  if (value.status === "reconciliation_required" || value.status === "effect_unavailable"
    || value.status === "effect_not_applied") {
    if (!isContentId(value.intent_id)) {
      throw transportError("invalid_engine_response", "effect intent identity is invalid");
    }
  }
  if (value.status === "release_required") {
    requireStrictlyOrderedIdentities(value.intent_ids, "effect intents", isContentId);
    if (value.intent_ids.length === 0) {
      throw transportError("invalid_engine_response", "release-required boundary has no effect intents");
    }
  }
  if (value.status === "completed") validateExecutionResult(value.result);
  if (value.status === "failed") validateRunFailure(value.failure);
  if (value.status === "cancelled") validateArtifactRef(value.reason);
}

function validateRunFailure(value: unknown): void {
  requireClosedRecord(value, ["class", "code", "detail"], "Run failure");
  if (!new Set(["declared_failure", "runtime_defect", "substrate"]).has(String(value.class)) ||
    typeof value.code !== "string" || !/^[a-z][a-z0-9_]{0,199}$/.test(value.code)) {
    throw transportError("invalid_engine_response", "Run failure classification is invalid");
  }
  validateArtifactRef(value.detail);
}

function validateExecutionResult(value: unknown): void {
  requireClosedRecord(value, ["run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"], "execution result");
  requireStrings(value, ["run_id", "plan_id", "projection_digest", "precondition_token"]);
  if (!isRunIdentity(value.run_id)
    || !isContentId(value.plan_id)
    || !isDigest(value.projection_digest)
    || !isPreconditionToken(value.precondition_token)) {
    throw transportError("invalid_engine_response", "execution result identity is invalid");
  }
  requireStrictlyOrderedIdentities(value.effects, "execution effects", isContentId);
}

const DURABLE_QUERY_PAGE_BYTES = 1024 * 1024;
const DURABLE_QUERY_SUMMARY_BYTES = 32 * 1024;
const DURABLE_QUERY_EXACT_RESPONSE_BYTES = 13 * 1024 * 1024;
const DURABLE_STATE_ROOT_LEAF_BYTES = 12 * 1024 * 1024;

function validateDurableQuerySource(observedRevision: unknown, sourceRoot: unknown): void {
  if (!isContentId(observedRevision) || !isDigest(sourceRoot)) {
    throw transportError(
      "invalid_engine_response",
      "durable query revision or authenticated source root is invalid",
    );
  }
}

function durablePageKeyHash(canonicalKey: string): string {
  const hasher = createHash("sha256");
  const frame = (bytes: Buffer): void => {
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(bytes.length));
    hasher.update(length);
    hasher.update(bytes);
  };
  frame(Buffer.from("cymule.authenticated-collection-preimage/1", "utf8"));
  frame(Buffer.from("cymule.authenticated-map-key/1", "utf8"));
  const fieldCount = Buffer.alloc(8);
  fieldCount.writeBigUInt64BE(1n);
  frame(fieldCount);
  frame(Buffer.from(canonicalKey, "utf8"));
  return hasher.digest("hex");
}

function validateDurablePagePosition(
  value: unknown,
  queryKind: DurablePageQueryKind,
): asserts value is DurablePagePosition {
  requireClosedRecord(value, ["canonical_key", "key_hash"], "durable page position");
  const validKey = queryKind === "run_index"
    ? isRunIdentity(value.canonical_key)
    : isContentId(value.canonical_key);
  if (!validKey || !isDigest(value.key_hash)
    || value.key_hash !== durablePageKeyHash(value.canonical_key as string)) {
    throw transportError("invalid_engine_response", "durable page position is invalid");
  }
}

function validateDurablePageCursor(value: unknown): asserts value is DurablePageCursor {
  requireClosedRecord(
    value,
    ["query_kind", "run_id", "source_revision", "source_root", "position"],
    "durable page cursor",
  );
  if (!new Set<DurablePageQueryKind>([
    "run_index", "run_waits", "run_effects", "run_occurrences", "run_attempts",
  ]).has(value.query_kind as DurablePageQueryKind)) {
    throw transportError("invalid_engine_response", "durable page cursor query kind is invalid");
  }
  const queryKind = value.query_kind as DurablePageQueryKind;
  if ((queryKind === "run_index" && value.run_id !== null)
    || (queryKind !== "run_index" && !isRunIdentity(value.run_id))) {
    throw transportError("invalid_engine_response", "durable page cursor owner is invalid");
  }
  validateDurableQuerySource(value.source_revision, value.source_root);
  validateDurablePagePosition(value.position, queryKind);
}

function durableSummaryKey(queryKind: DurablePageQueryKind, value: unknown): string {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "durable query summary is invalid");
  }
  const key = queryKind === "run_index" ? value.run_id
    : queryKind === "run_waits" ? value.wait_id
    : queryKind === "run_effects" ? value.intent_id
    : queryKind === "run_occurrences" ? value.occurrence_id
    : value.attempt_id;
  if (typeof key !== "string") {
    throw transportError("invalid_engine_response", "durable query summary key is invalid");
  }
  return key;
}

function durablePositionForKey(canonicalKey: string): DurablePagePosition {
  return { canonical_key: canonicalKey, key_hash: durablePageKeyHash(canonicalKey) };
}

function compareDurablePositions(left: DurablePagePosition, right: DurablePagePosition): number {
  const hashOrder = compareWireStrings(left.key_hash, right.key_hash);
  return hashOrder === 0
    ? compareWireStrings(left.canonical_key, right.canonical_key)
    : hashOrder;
}

function validateDurableQueryPage(
  value: unknown,
  queryKind: DurablePageQueryKind,
  runId: string | null,
  validateItem: (item: unknown) => void,
): void {
  requireClosedRecord(
    value,
    ["observed_revision", "source_root", "items", "next_cursor"],
    "durable query page",
  );
  validateDurableQuerySource(value.observed_revision, value.source_root);
  if (!Array.isArray(value.items) || value.items.length > 256) {
    throw transportError("invalid_engine_response", "durable query page item count is invalid");
  }
  let previous: DurablePagePosition | undefined;
  for (const item of value.items) {
    validateItem(item);
    if (runId !== null && (!isRecord(item) || item.run_id !== runId)) {
      throw transportError("invalid_engine_response", "durable query item escaped its Run");
    }
    if (Buffer.byteLength(JSON.stringify(item), "utf8") > DURABLE_QUERY_SUMMARY_BYTES) {
      throw transportError("invalid_engine_response", "durable query summary is oversized");
    }
    const position = durablePositionForKey(durableSummaryKey(queryKind, item));
    if (previous !== undefined && compareDurablePositions(previous, position) >= 0) {
      throw transportError(
        "invalid_engine_response",
        "durable query items are not in strict authenticated key order",
      );
    }
    previous = position;
  }
  if (value.next_cursor !== null) {
    validateDurablePageCursor(value.next_cursor);
    const cursor = value.next_cursor as DurablePageCursor;
    if (cursor.query_kind !== queryKind || cursor.run_id !== runId
      || cursor.source_revision !== value.observed_revision
      || cursor.source_root !== value.source_root
      || previous === undefined
      || !wireValuesEqual(cursor.position, previous)) {
      throw transportError(
        "invalid_engine_response",
        "durable next cursor does not bind the terminal item and source",
      );
    }
  }
}

function validateDurableRunPage(
  value: Record<string, unknown>,
  queryKind: DurablePageQueryKind,
  validateItem: (item: unknown) => void,
): void {
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "durable Run page owner is invalid");
  }
  validateDurableQueryPage(value.page, queryKind, value.run_id, validateItem);
}

function validateContinuationExecutionAxes(
  continuationStatus: unknown,
  executionStatus: unknown,
): void {
  if (!new Set(["ready", "waiting", "running", "completed", "failed", "cancelled"])
    .has(String(continuationStatus))) {
    throw transportError("invalid_engine_response", "Continuation summary status is invalid");
  }
  validateRunExecutionStatus(executionStatus);
  const expected = new Map<string, string>([
    ["ready", "active"], ["waiting", "active"], ["running", "active"],
    ["completed", "completed"], ["failed", "failed"], ["cancelled", "cancelled"],
  ]).get(String(continuationStatus));
  if ((executionStatus as { status: unknown }).status !== expected) {
    throw transportError(
      "invalid_engine_response",
      "Continuation and execution summary axes disagree",
    );
  }
}

function validateWorldSettlement(value: unknown, executionStatus: unknown): void {
  if (!new Set(["settled", "pending", "unknown", "governance_required"])
    .has(String(value))) {
    throw transportError("invalid_engine_response", "world settlement is invalid");
  }
  if (isRecord(executionStatus)
    && executionStatus.status === "completed"
    && value !== "settled") {
    throw transportError("invalid_engine_response", "completed Run retains unsettled Effects");
  }
}

function validateDurableRunIndexSummary(value: unknown): void {
  requireClosedRecord(
    value,
    ["run_id", "continuation_status", "execution_status", "world_settlement"],
    "Run-index summary",
  );
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "Run-index summary identity is invalid");
  }
  validateContinuationExecutionAxes(value.continuation_status, value.execution_status);
  validateWorldSettlement(value.world_settlement, value.execution_status);
}

function validateDurableRunCurrent(value: unknown): void {
  requireClosedRecord(value, [
    "run_id", "plan_id", "execution_binding", "continuation_status", "epoch",
    "execution_fence", "result", "execution_status", "world_settlement",
  ], "Run-current projection");
  if (!isRunIdentity(value.run_id) || !isContentId(value.plan_id)) {
    throw transportError("invalid_engine_response", "Run-current identity is invalid");
  }
  validateArtifactRef(value.execution_binding);
  if (!isRecord(value.execution_binding)
    || value.execution_binding.kind !== "cymule.execution-binding/2") {
    throw transportError("invalid_engine_response", "Run-current binding is invalid");
  }
  requireEpoch(value.epoch);
  requireEpoch(value.execution_fence);
  validateContinuationExecutionAxes(value.continuation_status, value.execution_status);
  validateWorldSettlement(value.world_settlement, value.execution_status);
  const completed = isRecord(value.execution_status)
    && value.execution_status.status === "completed";
  if (completed) validateArtifactRef(value.result);
  else if (value.result !== null) {
    throw transportError("invalid_engine_response", "non-completed Run carries a result");
  }
  if (Buffer.byteLength(JSON.stringify(value), "utf8") > DURABLE_QUERY_SUMMARY_BYTES) {
    throw transportError("invalid_engine_response", "Run-current projection is oversized");
  }
}

function validateDurableWaitSummary(value: unknown): void {
  requireClosedRecord(value, ["wait_id", "run_id", "state", "result"], "wait summary");
  if (!isContentId(value.wait_id) || !isRunIdentity(value.run_id)
    || !new Set(["pending", "completed", "cancelled"]).has(String(value.state))) {
    throw transportError("invalid_engine_response", "wait summary is invalid");
  }
  if (value.state === "completed") validateArtifactRef(value.result);
  else if (value.result !== null) {
    throw transportError("invalid_engine_response", "non-completed wait summary has a result");
  }
}

function validateDurableEffectSummary(value: unknown): void {
  requireClosedRecord(value, [
    "intent_id", "run_id", "state", "execution_availability", "reconciliation", "result",
  ], "Effect summary");
  if (!isContentId(value.intent_id) || !isRunIdentity(value.run_id)
    || !new Set(["pending", "claimed", "applied", "not_applied", "unknown",
      "cancelled_before_release"]).has(String(value.state))
    || !new Set(["available", "unavailable"]).has(String(value.execution_availability))) {
    throw transportError("invalid_engine_response", "Effect summary is invalid");
  }
  const reconciliationMatches = value.state === "pending" || value.state === "claimed"
    ? value.reconciliation === "not_required"
    : value.state === "applied" || value.state === "not_applied"
    ? new Set(["not_required", "resolved"]).has(String(value.reconciliation))
    : value.state === "unknown"
    ? new Set(["pending", "governance_required"]).has(String(value.reconciliation))
    : value.reconciliation === "resolved";
  if (!reconciliationMatches
    || ((value.state === "pending" || value.state === "claimed")
      && value.execution_availability !== "available")) {
    throw transportError("invalid_engine_response", "Effect summary lifecycle is inconsistent");
  }
  if (value.state === "applied") validateArtifactRef(value.result);
  else if (value.result !== null) {
    throw transportError("invalid_engine_response", "non-applied Effect summary has a result");
  }
}

function validateDurableOccurrenceSummary(value: unknown): void {
  requireClosedRecord(
    value,
    ["occurrence_id", "run_id", "state", "outcome"],
    "component occurrence summary",
  );
  if (!isContentId(value.occurrence_id) || !isRunIdentity(value.run_id)
    || !new Set(["pending", "completed"]).has(String(value.state))) {
    throw transportError("invalid_engine_response", "component occurrence summary is invalid");
  }
  if (value.state === "completed") validateComponentOutcome(value.outcome);
  else if (value.outcome !== null) {
    throw transportError("invalid_engine_response", "pending occurrence summary has an outcome");
  }
}

function validateDurableAttemptSummary(value: unknown): void {
  requireClosedRecord(value, [
    "attempt_id", "occurrence_id", "run_id", "attempt_ordinal", "state", "outcome",
  ], "operation Attempt summary");
  if (!isContentId(value.attempt_id) || !isContentId(value.occurrence_id)
    || !isRunIdentity(value.run_id)
    || !Number.isSafeInteger(value.attempt_ordinal) || Number(value.attempt_ordinal) < 1
    || !new Set(["running", "completed", "superseded"]).has(String(value.state))) {
    throw transportError("invalid_engine_response", "operation Attempt summary is invalid");
  }
  if (value.state === "completed") validateComponentOutcome(value.outcome);
  else if (value.outcome !== null) {
    throw transportError("invalid_engine_response", "non-completed Attempt summary has an outcome");
  }
}

function validateDurableRunItemSelector(
  value: unknown,
): asserts value is DurableRunItemSelector {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Run-item selector is invalid");
  }
  const field = value.kind === "wait" ? "wait_id"
    : value.kind === "effect" ? "intent_id"
    : value.kind === "occurrence" ? "occurrence_id"
    : value.kind === "attempt" ? "attempt_id" : undefined;
  if (field === undefined
    || Object.keys(value).sort().join(",") !== [field, "kind"].sort().join(",")
    || !isContentId(value[field])) {
    throw transportError("invalid_engine_response", "Run-item selector is not closed");
  }
}

function validateDurableRunItem(value: unknown): asserts value is DurableRunItem {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "exact durable Run item is invalid");
  }
  const field = value.kind === "wait" ? "wait"
    : value.kind === "effect" ? "effect"
    : value.kind === "occurrence" ? "occurrence"
    : value.kind === "attempt" ? "attempt" : undefined;
  if (field === undefined
    || Object.keys(value).sort().join(",") !== [field, "kind"].sort().join(",")) {
    throw transportError("invalid_engine_response", "exact durable Run item is not closed");
  }
  if (field === "wait") validateWaitCondition(value.wait);
  else if (field === "effect") validateEffectDispatch(value.effect);
  else if (field === "occurrence") validateComponentOccurrence(value.occurrence);
  else validateOperationAttempt(value.attempt);
  if (Buffer.byteLength(JSON.stringify(value[field]), "utf8") > DURABLE_STATE_ROOT_LEAF_BYTES) {
    throw transportError("invalid_engine_response", "exact durable Run item is oversized");
  }
}

function durableRunItemOwner(value: DurableRunItem): string {
  if (value.kind === "wait") return value.wait.run_id;
  if (value.kind === "effect") return value.effect.run_id;
  if (value.kind === "occurrence") return value.occurrence.run_id;
  return value.attempt.run_id;
}

function validateRunExecutionStatus(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "Run execution status is invalid");
  const fields = value.status === "active" || value.status === "completed" ? "status" :
    value.status === "failed" ? "failure,status" :
    value.status === "cancelled" ? "reason,status" : undefined;
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) {
    throw transportError("invalid_engine_response", "Run execution status is not closed");
  }
  if (value.status === "failed") validateRunFailure(value.failure);
  if (value.status === "cancelled") validateArtifactRef(value.reason);
}

function validateContinuation(value: unknown): void {
  requireClosedRecord(value, [
    "run_id", "plan_id", "binding_context", "frames", "state", "wait_set", "scope_stack",
    "epoch", "execution_fence", "execution_claim", "status",
  ], "Continuation");
  requireStrings(value, ["run_id", "plan_id", "binding_context"]);
  if (!isRunIdentity(value.run_id) || !isContentId(value.plan_id)) {
    throw transportError("invalid_engine_response", "Continuation Run or Plan identity is invalid");
  }
  if (!new Set(["ready", "waiting", "running", "completed", "failed", "cancelled"]).has(String(value.status))) {
    throw transportError("invalid_engine_response", "Continuation status is invalid");
  }
  requireEpoch(value.epoch);
  requireEpoch(value.execution_fence);
  if (value.execution_claim !== null) {
    validateContinuationExecutionClaim(value.execution_claim);
    const claim = value.execution_claim as ContinuationExecutionClaim;
    if (claim.run_id !== value.run_id ||
      claim.fence !== value.execution_fence || claim.plan_id !== value.plan_id ||
      claim.execution_binding_ref.artifact_id !== value.binding_context) {
      throw transportError("invalid_engine_response", "Continuation execution claim does not match its owner");
    }
  }
  if ((value.status === "running") !== (value.execution_claim !== null)) throw transportError("invalid_engine_response", "Continuation execution claim does not match status");
  for (const field of ["wait_set", "scope_stack"]) {
    requireStringArray(value[field], `Continuation ${field}`);
  }
  requireStrictlyOrderedIdentities(value.wait_set, "Continuation wait set");
  if ((value.status === "waiting") !== ((value.wait_set as unknown[]).length > 0)) {
    throw transportError("invalid_engine_response", "Continuation waiting status does not match its wait set");
  }
  if (value.state !== null) validateArtifactRef(value.state);
  if (!Array.isArray(value.frames)) throw transportError("invalid_engine_response", "Continuation frames are invalid");
  if (new Set(["ready", "waiting", "running"]).has(String(value.status)) &&
    (value.frames.length === 0 || !Array.isArray(value.scope_stack) || value.scope_stack.length === 0)) {
    throw transportError("invalid_engine_response", "active Continuation has no frame or scope");
  }
  for (const frame of value.frames) {
    requireClosedRecord(frame, ["definition_id", "invocation_id", "invocation_path", "scope_id", "input", "region_path", "next_step", "locals"], "Continuation frame");
    requireStrings(frame, ["definition_id", "invocation_id", "scope_id"]);
    validateArtifactRef(frame.input);
    requireIndexArray(frame.region_path, "frame Region path");
    if (!isNonNegativeInteger(frame.next_step) || !isRecord(frame.locals)) throw transportError("invalid_engine_response", "Continuation frame is invalid");
    Object.values(frame.locals).forEach(validateArtifactRef);
    if (!Array.isArray(frame.invocation_path)) throw transportError("invalid_engine_response", "invocation path is invalid");
    for (const segment of frame.invocation_path) {
      requireClosedRecord(segment, ["site_id", "region_path", "scope_id"], "invocation segment");
      requireStrings(segment, ["site_id", "scope_id"]);
      requireIndexArray(segment.region_path, "invocation Region path");
    }
  }
}

function validateContinuationExecutionClaim(value: unknown): void {
  requireClosedRecord(value, ["claim_version", "run_id", "continuation_id", "owner", "continuation_attempt_id", "fence", "plan_id", "execution_binding_ref", "clock_observation_ref", "logical_acquired_at", "logical_expires_at", "logical_ttl"], "Continuation execution claim");
  if (value.claim_version !== "cymule.continuation-execution-claim/1") throw transportError("invalid_engine_response", "execution claim version is invalid");
  requireStrings(value, ["run_id", "continuation_id", "owner", "continuation_attempt_id", "plan_id"]);
  if (!isRunIdentity(value.run_id)
    || !isContentId(value.continuation_id)
    || !isContentId(value.continuation_attempt_id)
    || !isContentId(value.plan_id)
    || !isClockIdentity(value.owner)) {
    throw transportError(
      "invalid_engine_response",
      "execution claim Run, Continuation, or owner identity is invalid",
    );
  }
  validateArtifactRef(value.execution_binding_ref);
  if ((value.execution_binding_ref as ArtifactRef).kind !== "cymule.execution-binding/2") {
    throw transportError("invalid_engine_response", "execution claim binding is invalid");
  }
  validateClockObservationRef(value.clock_observation_ref);
  for (const field of ["fence", "logical_acquired_at", "logical_expires_at", "logical_ttl"]) requireEpoch(value[field]);
  const fence = Number(value.fence);
  const acquiredAt = Number(value.logical_acquired_at);
  const ttl = Number(value.logical_ttl);
  const expiresAt = Number(value.logical_expires_at);
  if (fence < 1 || ttl < 1 || acquiredAt + ttl !== expiresAt ||
    !Number.isSafeInteger(expiresAt)) {
    throw transportError("invalid_engine_response", "execution claim logical expiry is invalid");
  }
}

function validateComponentOccurrence(value: unknown): void {
  requireClosedRecord(value, ["occurrence_version", "occurrence_id", "run_id", "plan_id", "binding_context", "invocation_id", "invocation_path", "definition_id", "region_path", "site_id", "step_index", "component", "input", "outcome", "occurrence_binding", "implementation_revision", "attempt_count", "latest_attempt_id", "continuation_digest", "state"], "component occurrence");
  if (value.occurrence_version !== "cymule.component-occurrence/4"
    || !isContentId(value.occurrence_id)
    || !isContentId(value.occurrence_binding)
    || !isContentId(value.latest_attempt_id)
    || !new Set(["pending", "completed"]).has(String(value.state))) {
    throw transportError("invalid_engine_response", "component occurrence is invalid");
  }
  requireStrings(value, ["occurrence_id", "run_id", "plan_id", "binding_context", "invocation_id", "definition_id", "site_id", "component", "occurrence_binding", "implementation_revision", "latest_attempt_id"]);
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "component occurrence Run identity is invalid");
  }
  validateArtifactRef(value.input);
  requireIndexArray(value.region_path, "component occurrence Region path");
  if (!Array.isArray(value.invocation_path) || !isNonNegativeInteger(value.step_index)
    || !Number.isSafeInteger(value.attempt_count) || Number(value.attempt_count) < 1) {
    throw transportError("invalid_engine_response", "component occurrence position is invalid");
  }
  for (const segment of value.invocation_path) {
    requireClosedRecord(segment, ["site_id", "region_path", "scope_id"], "component occurrence invocation edge");
    requireStrings(segment, ["site_id", "scope_id"]);
    requireIndexArray(segment.region_path, "component occurrence invocation Region path");
  }
  if (value.state === "pending") {
    if (value.outcome !== null || value.continuation_digest !== null) {
      throw transportError("invalid_engine_response", "pending component occurrence has a terminal result");
    }
  } else {
    validateComponentOutcome(value.outcome);
    if (typeof value.continuation_digest !== "string" || !/^[0-9a-f]{64}$/.test(value.continuation_digest)) {
      throw transportError("invalid_engine_response", "completed component occurrence has no Continuation digest");
    }
  }
}

function validateOperationAttempt(value: unknown): void {
  requireClosedRecord(value, ["attempt_version", "attempt_id", "occurrence_id", "run_id", "attempt_ordinal", "previous_attempt_id", "continuation_attempt_id", "execution_claim_owner", "execution_claim_fence", "operation_occurrence_binding", "transport_request_id", "state", "outcome"], "operation Attempt");
  if (value.attempt_version !== "cymule.operation-attempt/2"
    || !isContentId(value.attempt_id)
    || !isContentId(value.occurrence_id)
    || !isContentId(value.continuation_attempt_id)
    || !isDurableIdentity(value.execution_claim_owner)
    || !isContentId(value.operation_occurrence_binding)
    || !isContentId(value.transport_request_id)
    || !new Set(["running", "completed", "superseded"]).has(String(value.state))) {
    throw transportError("invalid_engine_response", "operation Attempt is invalid");
  }
  requireStrings(value, ["attempt_id", "occurrence_id", "run_id", "continuation_attempt_id", "execution_claim_owner", "operation_occurrence_binding", "transport_request_id"]);
  if (!isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "operation Attempt Run identity is invalid");
  }
  requireEpoch(value.attempt_ordinal); requireEpoch(value.execution_claim_fence);
  if (Number(value.attempt_ordinal) < 1 || Number(value.execution_claim_fence) < 1) {
    throw transportError("invalid_engine_response", "operation Attempt fence is invalid");
  }
  if (value.previous_attempt_id !== null && !isContentId(value.previous_attempt_id)
    || (Number(value.attempt_ordinal) === 1) !== (value.previous_attempt_id === null)) {
    throw transportError("invalid_engine_response", "operation Attempt predecessor is invalid");
  }
  if (value.state === "completed") validateComponentOutcome(value.outcome);
  else if (value.outcome !== null) throw transportError("invalid_engine_response", "non-terminal operation Attempt has an outcome");
}

function validateComponentOutcome(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "component outcome is invalid");
  if (value.outcome === "succeeded") {
    requireClosedRecord(value, ["outcome", "output"], "succeeded component outcome");
    validateArtifactRef(value.output);
    return;
  }
  if (value.outcome === "expected_failure") {
    requireClosedRecord(value, ["outcome", "code", "detail"], "expected component failure");
    if (typeof value.code !== "string" || !/^[a-z][a-z0-9_]{0,199}$/.test(value.code)) throw transportError("invalid_engine_response", "expected component failure code is invalid");
    validateArtifactRef(value.detail);
    return;
  }
  throw transportError("invalid_engine_response", "component outcome variant is unknown");
}

function validateWaitCondition(value: unknown): void {
  requireClosedRecord(value, ["wait_id", "run_id", "kind", "consume_once", "owner", "state", "result"], "wait condition");
  requireStrings(value, ["wait_id", "run_id"]);
  if (!isContentId(value.wait_id) || !isRunIdentity(value.run_id)) {
    throw transportError("invalid_engine_response", "wait condition Run identity is invalid");
  }
  if (typeof value.consume_once !== "boolean" || !new Set(["pending", "completed", "cancelled"]).has(String(value.state))) throw transportError("invalid_engine_response", "wait condition state is invalid");
  validateDurableWaitKind(value.kind);
  requireClosedRecord(value.owner, ["invocation_id", "definition_id", "site_id", "region_path", "step_index", "bind"], "wait owner");
  requireStrings(value.owner, ["invocation_id", "definition_id", "site_id"]);
  requireIndexArray(value.owner.region_path, "wait owner Region path");
  requireEpoch(value.owner.step_index);
  if (value.owner.bind !== null && !isNonEmptyString(value.owner.bind)) throw transportError("invalid_engine_response", "wait bind is invalid");
  if (value.state === "completed") validateArtifactRef(value.result);
  else if (value.result !== null) throw transportError("invalid_engine_response", "non-completed wait has a result");
}

function validateDurableWaitKind(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "durable wait kind is invalid");
  const fields = value.kind === "signal" ? "key,kind" : value.kind === "timer" ? "kind,timer_id" : value.kind === "input" ? "correlation,kind,schema" : undefined;
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) throw transportError("invalid_engine_response", "durable wait kind is not closed");
  if (value.kind === "signal") requireStrings(value, ["key"]);
  if (value.kind === "timer") requireStrings(value, ["timer_id"]);
  if (value.kind === "input") {
    requireStrings(value, ["correlation"]);
    if (typeof value.schema !== "boolean" && !isRecord(value.schema)) {
      throw transportError("invalid_engine_response", "durable input wait schema is invalid");
    }
  }
}

function validateEffectDispatch(value: unknown): void {
  requireClosedRecord(value, ["intent_id", "run_id", "origin_plan_id", "operation", "input", "execution_binding", "occurrence_binding", "execution_availability", "state", "reconciliation", "claim_epoch", "claim_owner", "result"], "effect dispatch");
  requireStrings(value, ["intent_id", "run_id", "origin_plan_id", "operation", "occurrence_binding"]);
  if (!isRunIdentity(value.run_id) || !isContentId(value.intent_id)
    || !isContentId(value.origin_plan_id) || !isContentId(value.occurrence_binding)) {
    throw transportError("invalid_engine_response", "effect dispatch Run identity is invalid");
  }
  validateArtifactRef(value.input);
  if (!isRecord(value.execution_binding)) throw transportError("invalid_engine_response", "effect execution binding is invalid");
  validateArtifactRef(value.execution_binding);
  if (value.execution_binding.kind !== "cymule.execution-binding/2") throw transportError("invalid_engine_response", "effect execution binding kind is invalid");
  requireEpoch(value.claim_epoch);
  if (!new Set(["available", "unavailable"]).has(String(value.execution_availability))) throw transportError("invalid_engine_response", "effect execution availability is invalid");
  if (!new Set(["pending", "claimed", "applied", "not_applied", "unknown", "cancelled_before_release"]).has(String(value.state))) throw transportError("invalid_engine_response", "effect state is invalid");
  const reconciliation = String(value.reconciliation);
  const reconciliationMatches = value.state === "pending"
    ? reconciliation === "not_required"
    : value.state === "claimed"
    ? reconciliation === "not_required"
    : value.state === "applied" || value.state === "not_applied"
    ? new Set(["not_required", "resolved"]).has(reconciliation)
    : value.state === "unknown"
    ? new Set(["pending", "governance_required"]).has(reconciliation)
    : reconciliation === "resolved";
  if (!reconciliationMatches) {
    throw transportError("invalid_engine_response", "effect reconciliation state is invalid");
  }
  if (value.claim_owner !== null && !isNonEmptyString(value.claim_owner)) throw transportError("invalid_engine_response", "effect claim owner is invalid");
  if (value.result !== null) validateArtifactRef(value.result);
  const hasClaim = Number(value.claim_epoch) >= 1 && isNonEmptyString(value.claim_owner);
  const lifecycleMatches = value.state === "pending"
    ? value.execution_availability === "available"
      && value.claim_epoch === 0 && value.claim_owner === null && value.result === null
    : value.state === "claimed"
    ? value.execution_availability === "available" && hasClaim && value.result === null
    : value.state === "unknown"
    ? hasClaim && value.result === null
    : value.state === "applied" || value.state === "not_applied"
    ? hasClaim && (value.state !== "not_applied" || value.result === null)
    : value.claim_epoch === 0 && value.claim_owner === null && value.result === null;
  if (!lifecycleMatches) {
    throw transportError("invalid_engine_response", "effect dispatch lifecycle is invalid");
  }
}

const EVOLUTION_STATE_FAMILIES: EvolutionStateFamily[] = [
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
];

function validateEvolutionCommit(value: unknown): asserts value is EvolutionCommit {
  requireClosedRecord(
    value,
    ["observed_revision", "committed_revision", "receipt"],
    "evolution commit",
  );
  if (!isContentId(value.observed_revision)
    || (value.committed_revision !== null && !isContentId(value.committed_revision))
    || (value.committed_revision !== null
      && value.committed_revision !== value.observed_revision)) {
    throw transportError("invalid_engine_response", "evolution commit revision is invalid");
  }
  validateEvolutionPersistenceReceipt(value.receipt);
}

function validateEvolutionPersistenceReceipt(
  value: unknown,
): asserts value is EvolutionPersistenceReceipt {
  requireClosedRecord(value, [
    "receipt_version",
    "receipt_id",
    "command",
    "parent_current_id",
    "source_witness_id",
    "outcome",
    "mutations",
    "mutation_id",
  ], "evolution persistence receipt");
  if (value.receipt_version !== "cymule.evolution-persistence-receipt/4"
    || !isContentId(value.receipt_id)
    || (value.parent_current_id !== null && !isContentId(value.parent_current_id))
    || (value.source_witness_id !== null && !isContentId(value.source_witness_id))
    || !isContentId(value.mutation_id)) {
    throw transportError("invalid_engine_response", "evolution persistence receipt identity is invalid");
  }
  validateEvolutionPersistenceCommand(value.command);
  validateLiveEvolutionOutcome(value.outcome);
  const command = value.command as EvolutionPersistenceCommand;
  const consumesSource = command.command.operation === "apply"
    && (command.command.command.operation === "migrate"
      || command.command.command.operation === "restart_under_new_plan");
  if (consumesSource !== (value.source_witness_id !== null)) {
    throw transportError(
      "invalid_engine_response",
      "evolution source witness does not match its semantic command",
    );
  }
  if (!liveEvolutionOutcomeMatchesCommand(
    command.command,
    value.outcome as LiveEvolutionOutcome,
  )) {
    throw transportError(
      "invalid_engine_response",
      "evolution persistence receipt outcome does not match its command",
    );
  }
  if (!Array.isArray(value.mutations) || value.mutations.length > 8192) {
    throw transportError("invalid_engine_response", "evolution mutation set is invalid");
  }
  let previous: [number, string] | undefined;
  for (const item of value.mutations) {
    requireClosedRecord(item, ["family", "storage_key", "value_id"], "evolution mutation write");
    const family = EVOLUTION_STATE_FAMILIES.indexOf(item.family as EvolutionStateFamily);
    if (family < 0 || !isContentId(item.storage_key) || !isContentId(item.value_id)) {
      throw transportError("invalid_engine_response", "evolution mutation write is invalid");
    }
    const current: [number, string] = [family, item.storage_key];
    if (previous !== undefined
      && (previous[0] > current[0]
        || (previous[0] === current[0]
          && compareWireStrings(previous[1], current[1]) >= 0))) {
      throw transportError(
        "invalid_engine_response",
        "evolution mutation writes are not strictly key ordered",
      );
    }
    previous = current;
  }
}

function validateEvolutionPersistenceCommand(
  value: unknown,
): asserts value is EvolutionPersistenceCommand {
  requireClosedRecord(
    value,
    ["persistence_version", "persistence_id", "evolution_id", "command"],
    "evolution persistence command",
  );
  if (value.persistence_version !== "cymule.evolution-persistence-command/4"
    || !isContentId(value.persistence_id)
    || !isWireIdentity(value.evolution_id)) {
    throw transportError("invalid_engine_response", "evolution persistence command is invalid");
  }
  validateLiveEvolutionCommand(value.command);
}

function validateLiveEvolutionOutcome(value: unknown): asserts value is LiveEvolutionOutcome {
  if (!isRecord(value) || typeof value.result !== "string") {
    throw transportError("invalid_engine_response", "live-evolution response is not tagged");
  }
  const keys = new Map<string, string>([
    ["definition_published", "result,revision"], ["template_registered", "linked,result"],
    ["publication_applied", "receipt,result"], ["patch_applied", "edge,result"],
    ["applied", "result"], ["occurrence_selected", "pin,result"],
    ["migrated", "receipt,result"], ["restart_authorized", "receipt,result"],
    ["shadow_recorded", "comparison,result"], ["gate_applied", "result,transition"],
  ]).get(value.result);
  if (keys === undefined || Object.keys(value).sort().join(",") !== keys) {
    throw transportError("invalid_engine_response", "live-evolution response fields are not closed");
  }
  switch (value.result) {
    case "definition_published": validateSubflowRevision(value.revision); break;
    case "template_registered": validateLinkedPlan(value.linked); break;
    case "publication_applied": validatePublicationReceipt(value.receipt); break;
    case "patch_applied": validatePlanEdge(value.edge); break;
    case "occurrence_selected": validateOccurrencePin(value.pin); break;
    case "migrated": validateMigrationReceipt(value.receipt); break;
    case "restart_authorized": validateRestartReceipt(value.receipt); break;
    case "shadow_recorded": validateShadowComparison(value.comparison); break;
    case "gate_applied": validateRolloutTransition(value.transition); break;
  }
}

function validateOccurrencePin(value: unknown): void {
  requireClosedRecord(value, ["occurrence_id", "template_id", "decision_id", "plan_id", "execution_binding", "selection_id"], "occurrence pin");
  requireWireIdentities(value, ["occurrence_id", "template_id", "decision_id", "selection_id"], "occurrence pin");
  if (!isContentId(value.plan_id)) {
    throw transportError("invalid_engine_response", "occurrence pin Plan identity is invalid");
  }
  validateArtifactRef(value.execution_binding);
  if (value.execution_binding.kind !== "cymule.execution-binding/2") {
    throw transportError("invalid_engine_response", "occurrence pin binding is not an ExecutionBinding Artifact");
  }
}

function validateSubflowRevision(value: unknown): void {
  requireClosedRecord(value, ["revision_version", "revision_id", "logical_ref", "sequence", "definition", "references"], "subflow revision");
  if (value.revision_version !== "cymule.subflow-revision/2"
    || !isContentId(value.revision_id)
    || !isWireIdentity(value.logical_ref)) {
    throw transportError("invalid_engine_response", "subflow revision identity or version is invalid");
  }
  requirePositiveEpoch(value.sequence);
  validateRegistryDefinition(value.definition);
  const definition = value.definition as Record<string, unknown>;
  if (!isCoreDefinitionName(definition.id)) {
    throw transportError("invalid_engine_response", "subflow definition identity is invalid");
  }
  validatePublishedSubflowReferences(value.references, new Set([String(definition.id)]));
}

function validatePublishedSubflowReferences(
  value: unknown,
  localDefinitions: Set<string>,
): void {
  if (!Array.isArray(value) || value.length > 1024
    || new TextEncoder().encode(JSON.stringify(value)).length > 1024 * 1024) {
    throw transportError("invalid_engine_response", "publication references are outside bounds");
  }
  let previousLogicalRef: string | undefined;
  for (const reference of value) {
    validateSubflowReference(reference);
    if ((reference.strategy as ReferenceStrategy).strategy !== "pinned") {
      throw transportError(
        "invalid_engine_response",
        "publication reference strategy must be pinned",
      );
    }
    if (previousLogicalRef !== undefined
      && compareWireStrings(previousLogicalRef, reference.logical_ref) >= 0) {
      throw transportError(
        "invalid_engine_response",
        "publication references are not strictly logical-reference ordered",
      );
    }
    if (localDefinitions.has(reference.local_definition)) {
      throw transportError(
        "invalid_engine_response",
        "publication references repeat a local definition",
      );
    }
    previousLogicalRef = reference.logical_ref;
    localDefinitions.add(reference.local_definition);
  }
}

function validateRegistryDefinition(value: unknown): void {
  validatePlanDefinition(value, false);
}

function validateSubflowReference(value: unknown, admitted = true): asserts value is Record<string, unknown> & {
  logical_ref: string;
  local_definition: string;
} {
  requireClosedRecord(value, ["logical_ref", "local_definition", "input_schema", "output_schema", "strategy"], "subflow reference");
  if (!(admitted
    ? isWireIdentity(value.logical_ref) && isCoreDefinitionName(value.local_definition)
    : typeof value.logical_ref === "string" && typeof value.local_definition === "string")) {
    throw transportError("invalid_engine_response", "subflow reference identity is invalid");
  }
  validatePlanSchema(value.input_schema, admitted);
  validatePlanSchema(value.output_schema, admitted);
  if (!isRecord(value.strategy) || typeof value.strategy.strategy !== "string") {
    throw transportError("invalid_engine_response", "subflow strategy is invalid");
  }
  if (value.strategy.strategy === "latest_compatible") {
    requireClosedRecord(value.strategy, ["strategy"], "latest-compatible subflow strategy");
  } else if (value.strategy.strategy === "pinned") {
    requireClosedRecord(value.strategy, ["strategy", "revision_id"], "pinned subflow strategy");
    if (!(admitted
      ? isContentId(value.strategy.revision_id)
      : typeof value.strategy.revision_id === "string")) {
      throw transportError("invalid_engine_response", "pinned subflow revision identity is invalid");
    }
  } else {
    throw transportError("invalid_engine_response", "subflow strategy is invalid");
  }
}

function validateLinkedPlan(value: unknown): void {
  requireClosedRecord(value, ["template_id", "plan", "resolved_revisions"], "linked Plan");
  requireWireIdentities(value, ["template_id"], "linked Plan");
  validateSealedPlan(value.plan);
  if (!isRecord(value.resolved_revisions)) {
    throw transportError("invalid_engine_response", "resolved revisions are invalid");
  }
  for (const [logicalRef, revisionId] of Object.entries(value.resolved_revisions)) {
    if (!isWireIdentity(logicalRef) || !isContentId(revisionId)) {
      throw transportError("invalid_engine_response", "resolved revision identity is invalid");
    }
  }
}

function validatePublicationReceipt(value: unknown): void {
  requireClosedRecord(value, ["revision", "updates"], "publication receipt");
  validateSubflowRevision(value.revision);
  if (!Array.isArray(value.updates)) {
    throw transportError("invalid_engine_response", "publication updates are invalid");
  }
  let previousTemplate: string | undefined;
  for (const update of value.updates) {
    requireClosedRecord(update, ["template_id", "previous_plan_id", "current_plan_id", "decision_id", "advanced"], "template update");
    if (!isWireIdentity(update.template_id)
      || !isContentId(update.previous_plan_id) || !isContentId(update.current_plan_id)
      || typeof update.advanced !== "boolean") {
      throw transportError("invalid_engine_response", "template update is invalid");
    }
    if (previousTemplate !== undefined && compareWireStrings(previousTemplate, update.template_id) >= 0) {
      throw transportError("invalid_engine_response", "publication updates are not strictly template-ordered");
    }
    previousTemplate = update.template_id;
    if (update.advanced) {
      if (!isContentId(update.decision_id) || update.previous_plan_id === update.current_plan_id) {
        throw transportError("invalid_engine_response", "advanced publication update is inconsistent");
      }
    } else if (update.decision_id !== null || update.previous_plan_id !== update.current_plan_id) {
      throw transportError("invalid_engine_response", "retained publication update is inconsistent");
    }
  }
}

function validatePlanEdge(value: unknown): void {
  requireClosedRecord(value, ["edge_id", "from_plan", "to_plan", "operations"], "Plan edge");
  if (!isContentId(value.edge_id) || !isContentId(value.from_plan) || !isContentId(value.to_plan)
    || value.from_plan === value.to_plan) {
    throw transportError("invalid_engine_response", "Plan edge lineage is invalid");
  }
  if (!Array.isArray(value.operations) || value.operations.length === 0) {
    throw transportError("invalid_engine_response", "Plan edge operations are invalid");
  }
  let previous: [string, string] | undefined;
  for (const operation of value.operations) {
    requireClosedRecord(operation, ["kind", "target", "before", "after"], "patch operation");
    if (!isWireIdentity(operation.target) || typeof operation.kind !== "string") {
      throw transportError("invalid_engine_response", "Plan edge operation identity is invalid");
    }
    const before = operation.before;
    const after = operation.after;
    const validShape = operation.kind === "add"
      ? before === null && isDigest(after)
      : operation.kind === "remove"
      ? isDigest(before) && after === null
      : operation.kind === "replace"
      ? isDigest(before) && isDigest(after) && before !== after
      : false;
    if (!validShape) {
      throw transportError("invalid_engine_response", "Plan edge operation is malformed");
    }
    const current: [string, string] = [operation.target, operation.kind];
    if (previous !== undefined && compareWireTuples(previous, current) >= 0) {
      throw transportError("invalid_engine_response", "Plan edge operations are not in canonical order");
    }
    previous = current;
  }
}

function validateMigrationReceipt(value: unknown): void {
  requireClosedRecord(value, [
    "request", "source_witness_id", "source_binding", "target_binding",
    "source_execution_fence", "target_epoch", "adapter_id", "adapter_revision",
    "from_schema", "to_schema", "output_state", "target_continuation", "evidence",
  ], "migration receipt");
  requireWireIdentities(value, ["adapter_id", "from_schema", "to_schema"], "migration receipt");
  validateMigrationRequest(value.request);
  if (!isContentId(value.source_witness_id) || !isContentId(value.adapter_revision)) {
    throw transportError("invalid_engine_response", "migration receipt identity is invalid");
  }
  for (const field of ["source_binding", "target_binding"] as const) {
    validateArtifactRef(value[field]);
    if (!isRecord(value[field]) || value[field].kind !== "cymule.execution-binding/2") {
      throw transportError(
        "invalid_engine_response",
        "migration receipt binding is not an ExecutionBinding Artifact",
      );
    }
  }
  requireEpoch(value.source_execution_fence);
  requirePositiveEpoch(value.target_epoch);
  for (const field of ["output_state", "evidence"]) validateArtifactRef(value[field]);
  validateMigrationContinuation(value.target_continuation);
  const request = value.request as Record<string, unknown>;
  const target = value.target_continuation as Record<string, unknown>;
  const targetBinding = value.target_binding as Record<string, unknown>;
  const expectedTargetEpoch = Number(request.expected_source_epoch) + 1;
  if (!Number.isSafeInteger(expectedTargetEpoch)
    || value.target_epoch !== expectedTargetEpoch
    || value.adapter_id !== request.adapter_id
    || value.adapter_revision !== request.adapter_revision
    || target.run_id !== request.run_id
    || target.plan_id !== request.to_plan
    || target.binding_context !== targetBinding.artifact_id
    || target.epoch !== value.target_epoch
    || !artifactRefsEqual(target.state, value.output_state)
    || target.execution_fence !== value.source_execution_fence
    || !isRootMigrationContinuation(target)) {
    throw transportError("invalid_engine_response", "migration receipt target Continuation is inconsistent");
  }
}

function isRootMigrationContinuation(value: Record<string, unknown>): boolean {
  return value.status === "ready"
    && value.execution_claim === null
    && Array.isArray(value.frames) && value.frames.length > 0
    && Array.isArray(value.wait_set) && value.wait_set.length === 0
    && Array.isArray(value.scope_stack) && value.scope_stack.length === 1
    && value.scope_stack[0] === "scope:root";
}

function validateRestartReceipt(value: unknown): void {
  requireClosedRecord(value, ["request", "source_witness_id", "target_plan"], "restart receipt");
  validateRestartRequest(value.request);
  if (!isContentId(value.source_witness_id)) {
    throw transportError("invalid_engine_response", "restart source witness identity is invalid");
  }
  validateSealedPlan(value.target_plan);
  const request = value.request as Record<string, unknown>;
  const targetPlan = value.target_plan as Record<string, unknown>;
  if (targetPlan.plan_id !== request.to_plan) {
    throw transportError("invalid_engine_response", "restart receipt lineage is inconsistent");
  }
}

function validateShadowComparison(value: unknown): void {
  requireClosedRecord(value, ["comparison_id", "subject", "decision_id", "primary_plan", "shadow_plan", "driver_id", "driver_revision", "comparison_policy", "primary_digest", "shadow_digest", "equivalent", "evidence"], "shadow comparison");
  requireWireIdentities(value, ["comparison_id", "subject", "decision_id", "driver_id", "comparison_policy", "primary_digest", "shadow_digest"], "shadow comparison");
  if (!isContentId(value.primary_plan)
    || !isContentId(value.shadow_plan)
    || value.primary_plan === value.shadow_plan
    || !isContentId(value.driver_revision)) {
    throw transportError("invalid_engine_response", "shadow comparison Plan lineage is invalid");
  }
  if (typeof value.equivalent !== "boolean") throw transportError("invalid_engine_response", "shadow comparison result is invalid");
  validateArtifactRef(value.evidence);
}

function validateRolloutTransition(value: unknown): void {
  requireClosedRecord(value, ["transition_id", "from_decision", "to_decision", "evaluation"], "rollout transition");
  if (!isContentId(value.transition_id)) {
    throw transportError("invalid_engine_response", "rollout transition identity is invalid");
  }
  requireWireIdentities(value, ["from_decision", "to_decision"], "rollout transition");
  if (value.from_decision === value.to_decision) {
    throw transportError("invalid_engine_response", "rollout transition decisions must differ");
  }
  requireClosedRecord(value.evaluation, ["evaluation_id", "gate", "target_observations", "target_failures", "equivalent_shadows", "inequivalent_shadows", "outcome", "evidence_ids"], "rollout evaluation");
  if (!isContentId(value.evaluation.evaluation_id)) {
    throw transportError("invalid_engine_response", "rollout evaluation identity is invalid");
  }
  for (const field of ["target_observations", "target_failures", "equivalent_shadows", "inequivalent_shadows"]) requireEpoch(value.evaluation[field]);
  validateRolloutGate(value.evaluation.gate);
  const gate = value.evaluation.gate as Record<string, unknown>;
  if (gate.decision_id !== value.from_decision) {
    throw transportError("invalid_engine_response", "rollout gate decision does not match its transition");
  }
  const targetObservations = Number(value.evaluation.target_observations);
  const targetFailures = Number(value.evaluation.target_failures);
  const equivalentShadows = Number(value.evaluation.equivalent_shadows);
  const inequivalentShadows = Number(value.evaluation.inequivalent_shadows);
  if (targetFailures > targetObservations) {
    throw transportError("invalid_engine_response", "rollout failures exceed target observations");
  }
  requireStringArray(value.evaluation.evidence_ids, "rollout evidence");
  const evidenceIds = value.evaluation.evidence_ids as string[];
  for (let index = 0; index < evidenceIds.length; index += 1) {
    if (!isWireIdentity(evidenceIds[index])
      || (index > 0 && compareWireStrings(evidenceIds[index - 1]!, evidenceIds[index]!) >= 0)) {
      throw transportError("invalid_engine_response", "rollout evidence identities are not a canonical set");
    }
  }
  const evidenceCount = targetObservations + equivalentShadows + inequivalentShadows;
  if (!Number.isSafeInteger(evidenceCount) || evidenceCount !== evidenceIds.length) {
    throw transportError("invalid_engine_response", "rollout evidence counts do not match its identities");
  }
  const expectedOutcome = targetFailures > Number(gate.max_target_failures)
      || inequivalentShadows > Number(gate.max_inequivalent_shadows)
    ? "rollback"
    : targetObservations >= Number(gate.min_target_observations)
        && equivalentShadows >= Number(gate.min_equivalent_shadows)
    ? "promote"
    : "pending";
  if (value.evaluation.outcome !== expectedOutcome || expectedOutcome === "pending") {
    throw transportError("invalid_engine_response", "rollout outcome does not match its exact evidence");
  }
}

function validateRolloutGate(value: unknown): void {
  requireClosedRecord(value, ["gate_id", "decision_id", "min_target_observations", "max_target_failures", "min_equivalent_shadows", "max_inequivalent_shadows"], "rollout gate");
  requireWireIdentities(value, ["gate_id", "decision_id"], "rollout gate");
  for (const field of ["min_target_observations", "max_target_failures", "min_equivalent_shadows", "max_inequivalent_shadows"] as const) {
    requireEpoch(value[field]);
  }
}

function validateSealedPlan(value: unknown): void {
  requireClosedRecord(value, ["candidate", "plan_id"], "sealed Plan");
  if (!/^sha256:[0-9a-f]{64}$/.test(String(value.plan_id))) {
    throw transportError("invalid_engine_response", "sealed Plan identity is invalid");
  }
  validatePlanCandidate(value.candidate);
}

function validatePlanCandidate(value: unknown, admitted = true): void {
  requireClosedRecord(value, [
    "components", "definitions", "effects", "entry", "ir_version", "metadata", "name",
  ], "Plan candidate");
  if (admitted
    ? value.ir_version !== "cymule.ir/3" || !isPlanId(value.name) || !isPlanId(value.entry)
    : typeof value.ir_version !== "string" || typeof value.name !== "string"
      || typeof value.entry !== "string") {
    throw transportError("invalid_engine_response", "Plan candidate identity is invalid");
  }
  if (!Array.isArray(value.components) || !Array.isArray(value.effects)
    || !Array.isArray(value.definitions)
    || (admitted && value.definitions.length === 0)) {
    throw transportError("invalid_engine_response", "Plan candidate collections are invalid");
  }
  if (!isRecord(value.metadata) || !Object.values(value.metadata).every((item) => typeof item === "string")) {
    throw transportError("invalid_engine_response", "Plan candidate metadata is invalid");
  }
  for (const contract of value.components) validatePlanContract(contract, false, admitted);
  for (const contract of value.effects) validatePlanContract(contract, true, admitted);
  for (const definitionValue of value.definitions) validatePlanDefinition(definitionValue, admitted);
}

function validatePlanDefinition(value: unknown, admitted = true): void {
  requireClosedRecord(value, ["body", "id", "input_schema", "output_schema"], "Plan definition");
  if (!(admitted ? isPlanId(value.id) : typeof value.id === "string")) {
    throw transportError("invalid_engine_response", "Plan definition identity is invalid");
  }
  validatePlanSchema(value.input_schema, admitted);
  validatePlanSchema(value.output_schema, admitted);
  validatePlanRegion(value.body, admitted);
}

function validatePlanContract(value: unknown, effect: boolean, admitted = true): void {
  const fields = ["id", "input_schema", "output_schema", "requirements"];
  if (effect) fields.push("profile");
  else fields.push("output_artifact_kind");
  requireClosedRecord(value, fields, "Plan contract");
  if (!isPlanWireString(value.id, admitted)) {
    throw transportError("invalid_engine_response", "Plan contract identity is invalid");
  }
  validatePlanSchema(value.input_schema, admitted);
  validatePlanSchema(value.output_schema, admitted);
  if (!isRecord(value.requirements)
    || !Object.values(value.requirements).every((item) => typeof item === "string")) {
    throw transportError("invalid_engine_response", "Plan contract requirements are invalid");
  }
  if (!effect && (typeof value.output_artifact_kind !== "string"
    || Buffer.byteLength(value.output_artifact_kind) > 255
    || !/^[a-z0-9._+\-]+(?:\/[a-z0-9._+\-]+)+$/.test(value.output_artifact_kind))) {
    throw transportError("invalid_engine_response", "Component output Artifact kind is invalid");
  }
  if (effect) {
    requireClosedRecord(value.profile, [
      "dispatch", "irreversible", "keyed_idempotency", "mutation", "reconciliation",
    ], "Effect profile");
    if (!new Set(["observational", "mutating"]).has(String(value.profile.mutation))
      || !new Set(["eager", "on_scope_commit", "explicit"]).has(String(value.profile.dispatch))
      || !new Set(["queryable", "externally_attested", "human", "impossible"]).has(String(value.profile.reconciliation))
      || typeof value.profile.keyed_idempotency !== "boolean"
      || typeof value.profile.irreversible !== "boolean") {
      throw transportError("invalid_engine_response", "Effect profile is invalid");
    }
  }
}

function validatePlanRegion(value: unknown, admitted = true): void {
  requireClosedRecord(value, ["result", "steps"], "Plan Region");
  if (!Array.isArray(value.steps)) {
    throw transportError("invalid_engine_response", "Plan Region steps are invalid");
  }
  for (const step of value.steps) validatePlanStep(step, admitted);
  validatePlanExpression(value.result, admitted);
}

function validatePlanStep(value: unknown, admitted = true): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Plan operation is invalid");
  }
  const fields = new Map<string, string[]>([
    ["call", ["component", "id", "input", "op"]],
    ["invoke", ["definition", "id", "input", "op"]],
    ["wait", ["id", "op", "wait"]],
    ["effect", ["effect", "id", "input", "occurrence", "op"]],
    ["scope", ["body", "id", "op"]],
  ]).get(String(value.op));
  if (fields === undefined) {
    throw transportError("invalid_engine_response", "Plan operation is not a wire operation");
  }
  const expected = "bind" in value ? [...fields, "bind"] : fields;
  if (Object.keys(value).sort().join(",") !== expected.sort().join(",")) {
    throw transportError("invalid_engine_response", "Plan operation is not closed");
  }
  if (!isPlanWireString(value.id, admitted)
    || ("bind" in value && !isPlanWireString(value.bind, admitted))) {
    throw transportError("invalid_engine_response", "Plan operation identity is invalid");
  }
  if (value.op === "call") {
    if (!isPlanWireString(value.component, admitted)) throw transportError("invalid_engine_response", "Plan component call is invalid");
    validatePlanExpression(value.input, admitted);
  } else if (value.op === "invoke") {
    if (!isPlanWireString(value.definition, admitted)) throw transportError("invalid_engine_response", "Plan invocation is invalid");
    validatePlanExpression(value.input, admitted);
  } else if (value.op === "wait") {
    validateWaitSpec(value.wait, admitted);
  } else if (value.op === "effect") {
    if (!isPlanWireString(value.effect, admitted)
      || !isPlanWireString(value.occurrence, admitted)) {
      throw transportError("invalid_engine_response", "Plan Effect is invalid");
    }
    validatePlanExpression(value.input, admitted);
  } else {
    validatePlanRegion(value.body, admitted);
  }
}

function validatePlanExpression(value: unknown, admitted = true): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Plan expression is invalid");
  }
  const fields = new Map<string, string[]>([
    ["input", ["kind"]],
    ["literal", ["kind", "value"]],
    ["binding", ["kind", "name"]],
    ["object", ["fields", "kind"]],
    ["array", ["items", "kind"]],
  ]).get(String(value.kind));
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields.join(",")) {
    throw transportError("invalid_engine_response", "Plan expression is not closed");
  }
  if (value.kind === "binding" && !isPlanWireString(value.name, admitted)) {
    throw transportError("invalid_engine_response", "Plan binding expression is invalid");
  }
  if (value.kind === "object") {
    if (!isRecord(value.fields)) throw transportError("invalid_engine_response", "Plan object expression is invalid");
    for (const expression of Object.values(value.fields)) validatePlanExpression(expression, admitted);
  }
  if (value.kind === "array") {
    if (!Array.isArray(value.items)) throw transportError("invalid_engine_response", "Plan array expression is invalid");
    for (const expression of value.items) validatePlanExpression(expression, admitted);
  }
}

function validatePlanSchema(value: unknown, admitted = true): void {
  if (admitted && !isRecord(value) && typeof value !== "boolean") {
    throw transportError("invalid_engine_response", "Plan schema is invalid");
  }
}

function isPlanWireString(value: unknown, admitted: boolean): value is string {
  return admitted ? isPlanId(value) : typeof value === "string";
}

function isPlanId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && Array.from(value).length <= 200;
}

function isContentId(value: unknown): value is string {
  return typeof value === "string" && /^sha256:[0-9a-f]{64}$/.test(value);
}

function isCoreDefinitionName(value: unknown): value is string {
  return isWireIdentity(value) && Buffer.byteLength(value) <= 160;
}

function isDigest(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function isPreconditionToken(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match = /^pre:(0|[1-9][0-9]{0,15}):(sha256:[0-9a-f]{64})$/.exec(value);
  return match !== null
    && BigInt(match[1]!) <= BigInt(MAX_SAFE_JSON_INTEGER)
    && isContentId(match[2]);
}

function isWireIdentity(value: unknown): value is string {
  return hasUnicodeScalarLength(value, 1, 256)
    && !/[\u0000-\u001f\u007f-\u009f]/.test(value);
}

function hasUnpairedSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!Number.isInteger(next) || next < 0xdc00 || next > 0xdfff) return true;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function requireWireIdentities(
  value: Record<string, unknown>,
  fields: string[],
  label: string,
): void {
  if (!fields.every((field) => isWireIdentity(value[field]))) {
    throw transportError("invalid_engine_response", `${label} identity is invalid`);
  }
}

function compareWireStrings(left: string, right: string): number {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

function compareWireTuples(left: [string, string], right: [string, string]): number {
  const first = compareWireStrings(left[0], right[0]);
  return first === 0 ? compareWireStrings(left[1], right[1]) : first;
}

function artifactRefsEqual(left: unknown, right: unknown): boolean {
  return isRecord(left) && isRecord(right)
    && left.identity_version === right.identity_version
    && left.artifact_id === right.artifact_id
    && left.kind === right.kind;
}

function validateWaitSpec(value: unknown, admitted = true): void {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw transportError("invalid_engine_response", "wait contract is not tagged");
  }
  const fields = new Map<string, string>([
    ["signal", "consume_once,key,kind"],
    ["timer", "kind,timer_id"],
    ["input", "correlation,kind,schema"],
  ]).get(value.kind);
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) {
    throw transportError("invalid_engine_response", "wait contract fields are not closed");
  }
  const validString = (candidate: unknown) => admitted
    ? isNonEmptyString(candidate)
    : typeof candidate === "string";
  if (value.kind === "signal") {
    if (!validString(value.key) || typeof value.consume_once !== "boolean") {
      throw transportError("invalid_engine_response", "signal wait consume_once is invalid");
    }
  } else if (value.kind === "timer") {
    if (!validString(value.timer_id)) {
      throw transportError("invalid_engine_response", "timer wait identity is invalid");
    }
  } else {
    if (!validString(value.correlation)
      || (admitted && !isRecord(value.schema) && typeof value.schema !== "boolean")) {
      throw transportError("invalid_engine_response", "input wait schema is invalid");
    }
  }
}

function validateArtifactRef(value: unknown): asserts value is ArtifactRef {
  requireClosedRecord(value, ["artifact_id", "identity_version", "kind"], "Artifact reference");
  requireStrings(value, ["artifact_id", "identity_version", "kind"]);
  if (value.identity_version !== "cymule.artifact/2" ||
    !/^sha256:[0-9a-f]{64}$/.test(String(value.artifact_id)) ||
    Buffer.byteLength(String(value.kind)) > 255 ||
    !/^[a-z0-9._+\-]+(?:\/[a-z0-9._+\-]+)+$/.test(String(value.kind))) {
    throw transportError("invalid_engine_response", "Artifact reference identity is invalid");
  }
}

function validateArtifactRecord(value: unknown): asserts value is ArtifactRecord {
  requireClosedRecord(value, ["bytes", "reference"], "Artifact record");
  validateArtifactRef(value.reference);
  if (typeof value.bytes !== "string"
    || value.bytes.length > 11_184_812
    || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value.bytes)) {
    throw transportError("invalid_engine_response", "Artifact record bytes are invalid");
  }
  const kind = Buffer.from(value.reference.kind, "utf8");
  const bytes = Buffer.from(value.bytes, "base64");
  if (bytes.length > 8 * 1024 * 1024 || bytes.toString("base64") !== value.bytes) {
    throw transportError("invalid_engine_response", "Artifact record bytes are not canonical Base64");
  }
  const kindLength = Buffer.alloc(4);
  kindLength.writeUInt32BE(kind.length);
  const bytesLength = Buffer.alloc(8);
  bytesLength.writeBigUInt64BE(BigInt(bytes.length));
  const artifactId = `sha256:${createHash("sha256")
    .update(Buffer.from("cymule.artifact/2", "ascii"))
    .update(kindLength)
    .update(kind)
    .update(bytesLength)
    .update(bytes)
    .digest("hex")}`;
  if (value.reference.artifact_id !== artifactId) {
    throw transportError("invalid_engine_response", "Artifact record identity does not match its bytes");
  }
}

function isArtifactRefWire(value: unknown): value is ArtifactRef {
  return isRecord(value)
    && Object.keys(value).sort().join(",") === "artifact_id,identity_version,kind"
    && value.identity_version === "cymule.artifact/2"
    && isContentId(value.artifact_id)
    && typeof value.kind === "string"
    && Buffer.byteLength(value.kind) <= 255
    && /^[a-z0-9._+\-]+(?:\/[a-z0-9._+\-]+)+$/.test(value.kind);
}

function validateEvolutionRequest(
  value: unknown,
  fields: string[],
  stringFields: string[],
): asserts value is Record<string, unknown> {
  requireClosedRecord(value, fields, "evolution request");
  requireStrings(value, stringFields);
}

function requireClosedRecord(
  value: unknown,
  fields: string[],
  label: string,
): asserts value is Record<string, unknown> {
  if (!isRecord(value) || Object.keys(value).sort().join(",") !== [...fields].sort().join(",")) {
    throw transportError("invalid_engine_response", `${label} fields are not closed`);
  }
}

function requireStrings(value: Record<string, unknown>, fields: string[]): void {
  if (!fields.every((field) => isNonEmptyString(value[field]))) {
    throw transportError("invalid_engine_response", "required string field is invalid");
  }
}

function requireEpoch(value: unknown): void {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw transportError("invalid_engine_response", "evolution epoch is invalid");
  }
}

function requirePositiveEpoch(value: unknown): void {
  requireEpoch(value);
  if (Number(value) === 0) {
    throw transportError("invalid_engine_response", "evolution epoch must be positive");
  }
}

function isNonNegativeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function requireIndexArray(value: unknown, label: string): void {
  if (!Array.isArray(value) || !value.every(isNonNegativeInteger)) {
    throw transportError("invalid_engine_response", `${label} is invalid`);
  }
}

function requireStringArray(value: unknown, label: string): void {
  if (!Array.isArray(value) || !value.every(isNonEmptyString)) {
    throw transportError("invalid_engine_response", `${label} is invalid`);
  }
}

function requireStrictlyOrderedIdentities(
  value: unknown,
  label: string,
  validate: (identity: unknown) => identity is string = isClockIdentity,
): asserts value is string[] {
  if (!Array.isArray(value)
    || !value.every(validate)
    || value.some((identity, index) => index > 0
      && compareWireStrings(value[index - 1] as string, identity) >= 0)) {
    throw transportError("invalid_engine_response", `${label} is not strictly identity-ordered`);
  }
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function validateEngineFailure(value: unknown): asserts value is EngineFailure {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "Engine failure is not an object");
  }
  const allowed = new Set([
    "category", "phase", "code", "message", "contract", "contract_side", "path", "issues",
    "retry_disposition",
  ]);
  if (
    !Object.keys(value).every((key) => allowed.has(key)) ||
    typeof value.category !== "string" || !ENGINE_FAILURE_CATEGORIES.has(value.category as EngineFailureCategory) ||
    typeof value.phase !== "string" || !ENGINE_PHASES.has(value.phase as EnginePhase) ||
    typeof value.code !== "string" || !/^[a-z][a-z0-9_]{0,199}$/.test(value.code) ||
    !hasUnicodeScalarLength(value.message, 1, 8192) || hasControlCharacter(value.message)
  ) {
    throw transportError("invalid_engine_response", "Engine failure fields are invalid");
  }
  if (
    value.retry_disposition !== undefined &&
    (typeof value.retry_disposition !== "string" || !ENGINE_RETRIES.has(value.retry_disposition as EngineRetryDisposition))
  ) {
    throw transportError("invalid_engine_response", "retry disposition is unknown");
  }
  const category = value.category as EngineFailureCategory;
  const retryDisposition = value.retry_disposition as EngineRetryDisposition | undefined;
  if (!ENGINE_RETRY_MATRIX[category].has(retryDisposition)) {
    throw transportError("invalid_engine_response", "Engine failure retry disposition is invalid for its category");
  }
  if (value.contract !== undefined
    && (!hasUnicodeScalarLength(value.contract, 1, 500) || hasControlCharacter(value.contract))) {
    throw transportError("invalid_engine_response", "contract identity is invalid");
  }
  if (value.contract_side !== undefined && !["schema", "input", "output"].includes(String(value.contract_side))) {
    throw transportError("invalid_engine_response", "contract side is unknown");
  }
  validateEnginePath(value.path);
  if (value.issues !== undefined) {
    if (!Array.isArray(value.issues) || value.issues.length > 100) {
      throw transportError("invalid_engine_response", "Engine issue set is invalid");
    }
    for (const issue of value.issues) {
      if (!isRecord(issue) || !Object.keys(issue).every((key) => ["code", "message", "path", "schema_path"].includes(key)) ||
        !hasUnicodeScalarLength(issue.code, 1, 200) ||
        !hasUnicodeScalarLength(issue.message, 1, 2000) ||
        hasControlCharacter(issue.code) || hasControlCharacter(issue.message)) {
        throw transportError("invalid_engine_response", "Engine issue is invalid");
      }
      validateEnginePath(issue.path);
      validateEnginePath(issue.schema_path);
    }
  }
}

function validateEnginePath(value: unknown): void {
  if (value !== undefined && (!hasUnicodeScalarLength(value, 0, 1000)
    || hasControlCharacter(value)
    || (value !== "" && !value.startsWith("/")))) {
    throw transportError("invalid_engine_response", "Engine failure path is invalid");
  }
}

function hasControlCharacter(value: unknown): boolean {
  return typeof value === "string" && /[\u0000-\u001f\u007f-\u009f]/u.test(value);
}

function hasUnicodeScalarLength(
  value: unknown,
  minimum: number,
  maximum: number,
): value is string {
  if (typeof value !== "string" || hasUnpairedSurrogate(value)) return false;
  const length = Array.from(value).length;
  return length >= minimum && length <= maximum;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object"
    && value !== null
    && !Array.isArray(value)
    && !isFloatIntegerToken(value);
}

function hasExactKeys(
  value: unknown,
  expected: readonly string[],
): value is Record<string, unknown> {
  if (!isRecord(value)) return false;
  const keys = Object.keys(value);
  return keys.length === expected.length && expected.every((key) => key in value);
}

function interruptedError(
  request: EngineRequest,
  kind: "cancelled" | "timed_out",
  requestBegan = true,
): EngineError {
  if (requestCanMutate(request) && requestBegan) {
    return new EngineError({
      category: "unknown_world_outcome",
      phase: "transport",
      code: `engine_response_${kind}`,
      message: `the Engine response was ${kind} after a mutating request began`,
      retry_disposition: "reconcile",
    });
  }
  return new EngineError({
    category: kind,
    phase: "transport",
    code: `engine_response_${kind}`,
    message: `the Engine response was ${kind}`,
    retry_disposition: kind === "cancelled" ? "never" : "retry_same_request",
  });
}

function responseLossError(request: EngineRequest, code: string, detail = "the Engine response was unavailable"): EngineError {
  if (requestCanMutate(request)) {
    return new EngineError({
      category: "unknown_world_outcome",
      phase: "transport",
      code,
      message: "the Engine response was unavailable after a mutating request began",
      retry_disposition: "reconcile",
    });
  }
  return transportError(code, detail);
}

function requestCanMutate(request: EngineRequest): boolean {
  const readOnlyDurableCommands = new Set<DurableCommand["type"]>([
    "run_index_page",
    "run_current",
    "run_wait_page",
    "run_effect_page",
    "run_occurrence_page",
    "run_attempt_page",
    "run_item",
  ]);
  return request.type === "run"
    || request.type === "observe_clock"
    || request.type === "execute_live_evolution"
    || (request.type === "execute_durable"
      && !readOnlyDurableCommands.has(request.command.type));
}

const MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991;
const FLOAT_INTEGER_TOKEN = Symbol("cymule.json-float-integer-token");

interface FloatIntegerToken {
  readonly [FLOAT_INTEGER_TOKEN]: true;
  readonly value: number;
}

function isFloatIntegerToken(value: unknown): value is FloatIntegerToken {
  return typeof value === "object"
    && value !== null
    && (value as Partial<FloatIntegerToken>)[FLOAT_INTEGER_TOKEN] === true;
}

function mathematicalIntegerToken(token: string): bigint | undefined {
  const negative = token.startsWith("-");
  const unsigned = negative ? token.slice(1) : token;
  const exponentIndex = unsigned.search(/[eE]/);
  const mantissa = exponentIndex < 0 ? unsigned : unsigned.slice(0, exponentIndex);
  const exponent = BigInt(exponentIndex < 0 ? "0" : unsigned.slice(exponentIndex + 1));
  const point = mantissa.indexOf(".");
  const fractionalDigits = point < 0 ? 0 : mantissa.length - point - 1;
  const digits = point < 0 ? mantissa : mantissa.slice(0, point) + mantissa.slice(point + 1);
  let coefficient = BigInt(digits);
  if (negative) coefficient = -coefficient;
  if (coefficient === 0n) return 0n;
  const scale = exponent - BigInt(fractionalDigits);
  if (scale >= 0n) {
    if (scale > 16n) {
      return coefficient < 0n
        ? -BigInt(MAX_SAFE_JSON_INTEGER) - 1n
        : BigInt(MAX_SAFE_JSON_INTEGER) + 1n;
    }
    return coefficient * 10n ** scale;
  }
  const denominatorExponent = -scale;
  if (denominatorExponent > BigInt(digits.length)) return undefined;
  const denominator = 10n ** denominatorExponent;
  if (coefficient % denominator !== 0n) return undefined;
  return coefficient / denominator;
}

function assertJsonString(value: string): void {
  if (hasUnpairedSurrogate(value)) {
    throw transportError("request_encoding_failed", "unpaired UTF-16 surrogate is not JSON");
  }
}

function assertStrictJson(value: unknown, seen = new Set<object>()): void {
  if (value === undefined || typeof value === "function" || typeof value === "symbol") {
    throw transportError("request_encoding_failed", "value is not in the JSON data model");
  }
  if (typeof value === "bigint") throw transportError("request_encoding_failed", "bigint is not JSON");
  if (typeof value === "string") {
    assertJsonString(value);
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)
      || (Number.isInteger(value) && Math.abs(value) > MAX_SAFE_JSON_INTEGER)) {
      throw transportError("request_encoding_failed", "number is outside the shared JSON domain");
    }
  }
  if (typeof value !== "object" || value === null) return;
  if (nodeTypes.isProxy(value)) {
    throw transportError("request_encoding_failed", "Proxy values are not plain JSON data");
  }
  if (seen.has(value)) throw transportError("request_encoding_failed", "cyclic value is not JSON");
  seen.add(value);
  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype) {
      throw transportError("request_encoding_failed", "array has a non-plain prototype");
    }
    if (Object.getOwnPropertyDescriptor(Array.prototype, "toJSON") !== undefined
      || Object.getOwnPropertyDescriptor(Object.prototype, "toJSON") !== undefined) {
      throw transportError("request_encoding_failed", "inherited toJSON is not plain JSON data");
    }
    const keys = Reflect.ownKeys(value);
    if (keys.some((key) => typeof key === "symbol")) {
      throw transportError("request_encoding_failed", "symbol properties are not JSON");
    }
    const expectedKeys = Array.from({ length: value.length }, (_, index) => String(index));
    expectedKeys.push("length");
    if (keys.length !== expectedKeys.length
      || expectedKeys.some((key) => !keys.includes(key))) {
      throw transportError("request_encoding_failed", "array is sparse or has extra properties");
    }
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable) {
        throw transportError("request_encoding_failed", "array element is not plain JSON data");
      }
      assertStrictJson(descriptor.value, seen);
    }
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw transportError("request_encoding_failed", "object has a non-plain prototype");
    }
    if (prototype === Object.prototype
      && Object.getOwnPropertyDescriptor(Object.prototype, "toJSON") !== undefined) {
      throw transportError("request_encoding_failed", "inherited toJSON is not plain JSON data");
    }
    const keys = Reflect.ownKeys(value);
    if (keys.some((key) => typeof key === "symbol")) {
      throw transportError("request_encoding_failed", "symbol properties are not JSON");
    }
    for (const key of keys as string[]) {
      assertJsonString(key);
      if (key === "toJSON") {
        throw transportError("request_encoding_failed", "toJSON is not plain JSON data");
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || !("value" in descriptor) || !descriptor.enumerable) {
        throw transportError("request_encoding_failed", "object property is not plain JSON data");
      }
      assertStrictJson(descriptor.value, seen);
    }
  }
  seen.delete(value);
}

function encodeStrictJson(value: unknown): string {
  assertStrictJson(value);
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw transportError("request_encoding_failed", "value is not in the JSON data model");
  }
  return encoded;
}

function parseStrictJson(text: string): unknown {
  let index = 0;
  const whitespace = () => { while (/[ \t\r\n]/.test(text[index] ?? "")) index += 1; };
  const stringToken = (): string => {
    const start = index++;
    while (index < text.length) {
      if (text[index] === "\\") { index += 2; continue; }
      if (text[index++] === '"') {
        const parsed = JSON.parse(text.slice(start, index)) as string;
        if (hasUnpairedSurrogate(parsed)) throw new Error("unpaired UTF-16 surrogate in JSON string");
        return parsed;
      }
    }
    throw new Error("unterminated JSON string");
  };
  const value = (): unknown => {
    whitespace();
    if (text[index] === "{") {
      index += 1; whitespace();
      const keys = new Set<string>();
      const object = Object.create(null) as Record<string, unknown>;
      if (text[index] === "}") { index += 1; return object; }
      while (true) {
        if (text[index] !== '"') throw new Error("object key is not a string");
        const key = stringToken();
        if (keys.has(key)) throw new Error(`duplicate JSON object key ${JSON.stringify(key)}`);
        keys.add(key); whitespace();
        if (text[index++] !== ":") throw new Error("missing object colon");
        object[key] = value(); whitespace();
        if (text[index] === "}") { index += 1; return object; }
        if (text[index++] !== ",") throw new Error("missing object comma");
        whitespace();
      }
    }
    if (text[index] === "[") {
      index += 1; whitespace();
      const array: unknown[] = [];
      if (text[index] === "]") { index += 1; return array; }
      while (true) {
        array.push(value()); whitespace();
        if (text[index] === "]") { index += 1; return array; }
        if (text[index++] !== ",") throw new Error("missing array comma");
      }
    }
    if (text[index] === '"') return stringToken();
    const match = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(index));
    if (match === null) throw new Error("invalid JSON value");
    const token = match[0];
    index += token.length;
    if (token === "true") return true;
    if (token === "false") return false;
    if (token === "null") return null;
    if (/^-?\d+$/.test(token)) {
      const digits = token.startsWith("-") ? token.slice(1) : token;
      if (digits.length > 16) {
        throw new Error("integer is outside the shared JSON domain");
      }
      const integer = BigInt(token);
      if (integer < -BigInt(MAX_SAFE_JSON_INTEGER) || integer > BigInt(MAX_SAFE_JSON_INTEGER)) {
        throw new Error("integer is outside the shared JSON domain");
      }
      return token === "-0" ? -0 : Number(integer);
    }
    const mathematicalInteger = mathematicalIntegerToken(token);
    if (mathematicalInteger !== undefined) {
      if (mathematicalInteger < -BigInt(MAX_SAFE_JSON_INTEGER)
        || mathematicalInteger > BigInt(MAX_SAFE_JSON_INTEGER)) {
        throw new Error("integer is outside the shared JSON domain");
      }
      return Number(mathematicalInteger);
    }
    const number = Number(token);
    if (!Number.isFinite(number)) throw new Error("non-finite JSON number");
    if (Number.isInteger(number)) {
      throw new Error("non-integral JSON number is not distinguishable from an integer");
    }
    return number;
  };
  const parsed = value(); whitespace();
  if (index !== text.length) throw new Error("trailing JSON content");
  return parsed;
}

function unboxFloatIntegerTokens(value: unknown): unknown {
  if (isFloatIntegerToken(value)) return value.value;
  if (Array.isArray(value)) return value.map(unboxFloatIntegerTokens);
  if (!isRecord(value)) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, child]) => [key, unboxFloatIntegerTokens(child)]),
  );
}
