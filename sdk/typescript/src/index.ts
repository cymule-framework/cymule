import { spawnSync } from "node:child_process";

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
  manifest_version: "cymule.resource-manifest/1";
  media_type: "application/vnd.cymule.resource-manifest+jsonl";
  digest: string;
  size: number;
  entry_count: number;
  root_digest: string;
}

export interface ResourceCandidate {
  resource_version: "cymule.resource/2";
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
  handoff_version: "cymule.resource-handoff/3";
  transfer_id: string;
  producer: { run_id: string; occurrence_id: string; result: ArtifactRef };
  to_run: string;
  slot: string;
  resource: ArtifactRef;
}

export interface ArtifactRef {
  identity_version: "cymule.artifact/2";
  artifact_id: string;
  kind: string;
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
  safe_point_id: string;
  source_epoch: number;
  input_state: ArtifactRef;
  source_binding: ArtifactRef;
  target_binding: ArtifactRef;
}

export interface RestartRequest {
  restart_id: string;
  source_run: string;
  replacement_run: string;
  from_plan: string;
  to_plan: string;
  safe_point_id: string;
  source_epoch: number;
  input: ArtifactRef;
  evidence: ArtifactRef;
}

export interface ShadowRequest {
  comparison_id: string;
  decision_id: string;
  subject: string;
  primary_plan: string;
  shadow_plan: string;
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
  | { operation: "select_occurrence"; occurrence_id: string }
  | { operation: "migrate"; request: MigrationRequest }
  | { operation: "restart_under_new_plan"; request: RestartRequest }
  | { operation: "shadow"; request: ShadowRequest }
  | { operation: "observe"; observation: RolloutObservation }
  | { operation: "apply_gate"; gate: RolloutGate; next_decision_id: string };

export type EvolutionCommand = {
  control_version: "cymule.evolution-control/3";
  command_id: string;
} & EvolutionOperation;

export interface EvolutionControl<Response = Json> {
  submit(command: EvolutionCommand): Promise<Response>;
}

export interface MigrationSafePoint {
  safe_point_version: "cymule.migration-safe-point/1";
  safe_point_id: string;
  run_id: string;
  plan_id: string;
  epoch: number;
  state: ArtifactRef | null;
  continuation_digest: string;
}

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
  evidence: ArtifactRef;
  mode: RolloutMode;
}

type LiveEvolutionOperation =
  | {
      operation: "publish_definition";
      logical_ref: string;
      definition: PlanCandidate["definitions"][number];
    }
  | { operation: "register_template"; template: PlanTemplate }
  | { operation: "publish_and_relink"; publication: LivePublicationCommand }
  | {
      operation: "apply";
      template_id: string;
      command: EvolutionCommand;
      safe_point?: MigrationSafePoint;
    };

export type LiveEvolutionCommand = {
  control_version: "cymule.live-evolution-control/2";
  command_id: string;
} & LiveEvolutionOperation;

export interface LiveEvolutionControl<Response = Json> {
  submit(command: LiveEvolutionCommand): Promise<Response>;
}

export type WaitActivationSource =
  | { kind: "signal"; key: string }
  | { kind: "timer"; timer_id: string };

export interface WaitActivation {
  activation_version: "cymule.wait-activation/1";
  activation_id: string;
  source: WaitActivationSource;
  wait_ids: string[];
  result: ArtifactRef;
}

export type DurableCommand =
  | {
      type: "start_run";
      control_version: "cymule.durable-control/1";
      run_id: string;
      candidate: PlanCandidate;
      input: Json;
    }
  | {
      type: "resume_run";
      control_version: "cymule.durable-control/1";
      run_id: string;
    }
  | {
      type: "activate_wait";
      control_version: "cymule.durable-control/1";
      activation_id: string;
      source: WaitActivationSource;
      wait_ids: string[];
      value: Json;
    }
  | {
      type: "release_effect";
      control_version: "cymule.durable-control/1";
      intent_id: string;
    }
  | {
      type: "query_run";
      control_version: "cymule.durable-control/1";
      query_id: string;
      run_id: string;
    }
  | {
      type: "query_domain";
      control_version: "cymule.durable-control/1";
      query_id: string;
    };

export interface DurableControl<Response = Json> {
  submit(command: DurableCommand): Promise<Response>;
}

export interface EngineStoreTarget {
  provider: string;
  location: string;
  domain?: string;
}

export interface EnginePluginTarget {
  provider: string;
  location: string;
  revision?: string;
}

export interface EngineDurableTarget {
  store: EngineStoreTarget;
  executor?: EnginePluginTarget;
}

export interface EngineEvolutionTarget {
  store: EngineStoreTarget;
  migration?: EnginePluginTarget;
  shadow?: EnginePluginTarget;
}

export const directoryStore = (location: string): EngineStoreTarget => ({
  provider: "cymule.directory-store/2",
  location,
});

export const sqliteStore = (location: string, domain: string): EngineStoreTarget => ({
  provider: "cymule.sqlite-store/2",
  location,
  domain,
});

export const processPlugin = (location: string, revision?: string): EnginePluginTarget => ({
  provider: "cymule.executor-process/1",
  location,
  ...(revision === undefined ? {} : { revision }),
});

export interface DurableFrameState {
  definition_id: string;
  invocation_id: string;
  invocation_path: Array<{ site_id: string; region_path: number[]; scope_id: string; epoch: number }>;
  scope_id: string;
  input: ArtifactRef;
  region_path: number[];
  next_step: number;
  locals: Record<string, ArtifactRef>;
}

export interface DurableContinuation {
  run_id: string;
  plan_id: string;
  binding_context: string;
  frames: DurableFrameState[];
  state: ArtifactRef | null;
  wait_set: string[];
  scope_stack: string[];
  effect_obligations: string[];
  authority_leases: string[];
  budget: Record<string, number>;
  causal_frontier: string[];
  epoch: number;
  status: "ready" | "waiting" | "running" | "completed";
}

export interface DurableWaitCondition {
  wait_id: string;
  run_id: string;
  kind: { kind: "signal"; key: string } | { kind: "timer"; timer_id: string } |
    { kind: "input"; correlation: string; schema: Json };
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
  operation: string;
  input: ArtifactRef;
  occurrence_binding: string;
  state: "pending" | "claimed" | "applied" | "not_applied" | "unknown";
  claim_epoch: number;
  claim_owner: string | null;
  result: ArtifactRef | null;
}

export interface DurableRunView {
  revision: string;
  continuation: DurableContinuation;
  waits: DurableWaitCondition[];
  effects: DurableEffectDispatch[];
  result: ArtifactRef | null;
}

export type DurableBoundary =
  | { status: "suspended"; wait_id: string }
  | { status: "reconciliation_required"; intent_id: string }
  | { status: "release_required"; intent_ids: string[] }
  | { status: "completed"; result: ExecutionResult };

export type DurableResponse =
  | { type: "run_boundary"; boundary: DurableBoundary }
  | { type: "wait_activated"; ready_run_ids: string[] }
  | { type: "run"; run: DurableRunView | null }
  | { type: "domain"; domain: { revision: string | null; run_ids: string[] } };

export type LiveEvolutionResponse =
  | { result: "definition_published"; revision: Record<string, unknown> }
  | { result: "template_registered"; linked: Record<string, unknown> }
  | { result: "publication_applied"; receipt: Record<string, unknown> }
  | { result: "patch_applied"; edge: Record<string, unknown> }
  | { result: "applied" }
  | { result: "occurrence_selected"; plan_id: string }
  | { result: "migrated"; receipt: Record<string, unknown> }
  | { result: "restart_authorized"; receipt: Record<string, unknown> }
  | { result: "shadow_recorded"; comparison: Record<string, unknown> }
  | { result: "gate_applied"; transition: Record<string, unknown> };

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
  occurrence_binding: string;
  lease: VirtualClaimLease;
}

export interface WorkOccurrence {
  occurrence_version: "cymule.virtual-work-occurrence/2";
  occurrence_id: string;
  work_id: string;
  region_id: string;
  run_id: string;
  owner: string;
  epoch: number;
  lease_epoch: number;
  plan_id: string;
  occurrence_binding: string;
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
  control_version: "cymule.virtual-work-control/1";
  command_id: string;
  work_id: string;
  owner: string;
  epoch: number;
  expected_lease_epoch: number;
  observed_at: number;
  resolution: WorkResolution;
}

export interface VirtualCursor {
  version: string;
  position: string;
  exhausted: boolean;
}

export interface VirtualRegion {
  region_id: string;
  run_id: string;
  source: string;
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
}

export interface RegionMigrationPlan {
  migration_version: "cymule.virtual-region-migration/1";
  migration_id: string;
  kind: RegionMigrationKind;
  expected_sources: Record<string, VirtualCursor>;
  targets: VirtualRegion[];
  migration_binding: string;
  coverage_evidence: ArtifactRef;
}

export interface RegionMigrationReceipt {
  plan: RegionMigrationPlan;
  retired_regions: string[];
  active_targets: string[];
}

export interface RegionMigrationCommand {
  control_version: "cymule.virtual-region-migration-control/1";
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

export interface VirtualCompactionCertificate {
  certificate_version: "cymule.virtual-compaction-certificate/3";
  certificate_id: string;
  source_causal_cut: string[];
  summary: VirtualCompletionSummary;
  summary_state_digest: string;
  occurrence_root_digest: string;
  unresolved_obligations: string[];
  retained_occurrence_bindings: string[];
  replay_availability: ReplayAvailability;
  rehydration_manifest: ResourceHandle;
  compactor_binding: string;
  compactor_revision: string;
}

export interface VirtualCompactionCommand {
  control_version: "cymule.virtual-compaction-control/1";
  command_id: string;
  region_id: string;
  source_causal_cut: string[];
  compactor_binding: string;
  compactor_revision: string;
}

export interface VirtualCompactionReceipt {
  command: VirtualCompactionCommand;
  certificate: VirtualCompactionCertificate;
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
  control_version: "cymule.virtual-claim-control/2";
  command_id: string;
  owner: string;
  slot_id: string;
  plan_id: string;
  occurrence_binding: string;
  capabilities: string[];
  logical_now: number;
  lease_ttl: number;
}

export interface VirtualClaimReceipt {
  command: VirtualClaimCommand;
  claim: ClaimedWork | null;
}

export interface VirtualLeaseRenewalCommand {
  control_version: "cymule.virtual-lease-renewal-control/1";
  command_id: string;
  work_id: string;
  owner: string;
  epoch: number;
  expected_lease_epoch: number;
  logical_now: number;
  lease_ttl: number;
}

export interface VirtualLeaseRenewalReceipt {
  command: VirtualLeaseRenewalCommand;
  lease: VirtualClaimLease;
}

export interface VirtualRecoveryCommand {
  control_version: "cymule.virtual-recovery-control/1";
  command_id: string;
  work_id: string;
  expected_owner: string;
  expected_epoch: number;
  expected_lease_epoch: number;
  observed_at: number;
  resolution: Extract<WorkResolution, { resolution: "retry" | "failed" | "cancelled" }>;
}

export interface VirtualRecoveryReceipt {
  command: VirtualRecoveryCommand;
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

export interface VirtualArchive {
  readonly binding: string;
  put(reference: ArtifactRef, bytes: Uint8Array): Promise<void>;
  get(reference: ArtifactRef): Promise<Uint8Array>;
}

export interface RegionMigrator {
  readonly binding: string;
  plan(request: RegionMigrationRequest, sources: VirtualRegion[]): Promise<RegionMigrationPlan>;
  verify(plan: RegionMigrationPlan): Promise<void>;
}

export interface VirtualWorkControl {
  occurrence(occurrenceId: string): Promise<WorkOccurrence | null>;
  resolve(command: WorkResolutionCommand): Promise<WorkOccurrence>;
  migrate(command: RegionMigrationCommand): Promise<RegionMigrationReceipt>;
  compact(command: VirtualCompactionCommand): Promise<VirtualCompactionReceipt>;
  rehydrate(command: VirtualRehydrationCommand): Promise<VirtualRehydrationReceipt>;
}

export interface VirtualSchedulingControl {
  claim(command: VirtualClaimCommand): Promise<VirtualClaimReceipt>;
  renew(command: VirtualLeaseRenewalCommand): Promise<VirtualLeaseRenewalReceipt>;
  recover(command: VirtualRecoveryCommand): Promise<VirtualRecoveryReceipt>;
  setRunWeight(command: VirtualRunWeightCommand): Promise<VirtualRunWeightReceipt>;
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
  ir_version: "cymule.ir/2";
  name: string;
  entry: string;
  components: Array<{
    id: string;
    input_schema: Schema;
    output_schema: Schema;
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
  | { id: string; op: "effect"; effect: string; input: Expression; occurrence: string }
  | {
      id: string;
      op: "scope";
      mode: "transactional" | "speculative";
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
      ir_version: "cymule.ir/2",
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
    requirements: Record<string, string>,
  ): this {
    this.#candidate.components.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
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

  effect(site: string, effect: string, input: Expression, occurrence: string): this {
    this.entry().body.steps.push({ id: site, op: "effect", effect, input, occurrence });
    return this;
  }

  wait(site: string, wait: WaitSpec, bind?: string): this {
    this.entry().body.steps.push({ id: site, op: "wait", wait, ...(bind === undefined ? {} : { bind }) });
    return this;
  }

  scope(
    site: string,
    mode: "transactional" | "speculative",
    body: Region,
    bind: string,
  ): this {
    this.entry().body.steps.push({ id: site, op: "scope", mode, body, bind });
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
    const targets = [...new Set(waitIds)].sort();
    if (activationId.length === 0 || targets.length === 0) {
      throw new Error("wait activation requires an identity and at least one target");
    }
    return {
      activation_version: "cymule.wait-activation/1",
      activation_id: activationId,
      source,
      wait_ids: targets,
      result,
    };
  }
}

export class DurableControlBuilder {
  static startRun(runId: string, candidate: PlanCandidate, input: Json): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    return {
      type: "start_run",
      control_version: "cymule.durable-control/1",
      run_id: runId,
      candidate: structuredClone(candidate),
      input: structuredClone(input),
    };
  }

  static resumeRun(runId: string): DurableCommand {
    DurableControlBuilder.identity("Run", runId);
    return { type: "resume_run", control_version: "cymule.durable-control/1", run_id: runId };
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

  static releaseEffect(intentId: string): DurableCommand {
    DurableControlBuilder.identity("effect intent", intentId);
    return {
      type: "release_effect",
      control_version: "cymule.durable-control/1",
      intent_id: intentId,
    };
  }

  static queryRun(queryId: string, runId: string): DurableCommand {
    DurableControlBuilder.identity("query", queryId);
    DurableControlBuilder.identity("Run", runId);
    return {
      type: "query_run",
      control_version: "cymule.durable-control/1",
      query_id: queryId,
      run_id: runId,
    };
  }

  static queryDomain(queryId: string): DurableCommand {
    DurableControlBuilder.identity("query", queryId);
    return {
      type: "query_domain",
      control_version: "cymule.durable-control/1",
      query_id: queryId,
    };
  }

  private static activate(
    activationId: string,
    source: WaitActivationSource,
    waitIds: string[],
    value: Json,
  ): DurableCommand {
    DurableControlBuilder.identity("activation", activationId);
    const targets = [...new Set(waitIds)].sort();
    if (targets.length === 0 || targets.some((target) => target.length === 0)) {
      throw new Error("durable activation requires at least one wait identity");
    }
    return {
      type: "activate_wait",
      control_version: "cymule.durable-control/1",
      activation_id: activationId,
      source,
      wait_ids: targets,
      value: structuredClone(value),
    };
  }

  private static identity(kind: string, value: string): void {
    if (value.length === 0 || value.length > 512) {
      throw new Error(`durable ${kind} identity must contain 1..=512 characters`);
    }
  }
}

export class EvolutionControlBuilder {
  static applyPatch(commandId: string, patch: PlanPatch): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, { operation: "apply_patch", patch });
  }

  static setRollout(commandId: string, decision: RolloutDecision): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, { operation: "set_rollout", decision });
  }

  static selectOccurrence(commandId: string, occurrenceId: string): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, {
      operation: "select_occurrence",
      occurrence_id: occurrenceId,
    });
  }

  static migrate(commandId: string, request: MigrationRequest): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, { operation: "migrate", request });
  }

  static restartUnderNewPlan(commandId: string, request: RestartRequest): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, {
      operation: "restart_under_new_plan",
      request,
    });
  }

  static shadow(commandId: string, request: ShadowRequest): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, { operation: "shadow", request });
  }

  static observe(commandId: string, observation: RolloutObservation): EvolutionCommand {
    return EvolutionControlBuilder.build(commandId, { operation: "observe", observation });
  }

  static applyGate(
    commandId: string,
    gate: RolloutGate,
    nextDecisionId: string,
  ): EvolutionCommand {
    if (nextDecisionId.length === 0) throw new Error("evolution gate requires a next decision ID");
    return EvolutionControlBuilder.build(commandId, {
      operation: "apply_gate",
      gate,
      next_decision_id: nextDecisionId,
    });
  }

  private static build(commandId: string, operation: EvolutionOperation): EvolutionCommand {
    if (commandId.length === 0) throw new Error("evolution control requires a command identity");
    return {
      control_version: "cymule.evolution-control/3",
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
  ): LiveEvolutionCommand {
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "publish_definition",
      logical_ref: logicalRef,
      definition: structuredClone(definition),
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
    safePoint?: MigrationSafePoint,
  ): LiveEvolutionCommand {
    if (templateId.length === 0) throw new Error("live evolution requires a template identity");
    return LiveEvolutionControlBuilder.build(commandId, {
      operation: "apply",
      template_id: templateId,
      command: structuredClone(command),
      ...(safePoint === undefined ? {} : { safe_point: structuredClone(safePoint) }),
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
      control_version: "cymule.live-evolution-control/2",
      command_id: commandId,
      ...operation,
    };
  }
}

export class VirtualWorkControlBuilder {
  static succeed(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    observedAt: number,
    result: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, observedAt, {
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
    observedAt: number,
    error: ArtifactRef,
    nextReason: ParkReason | null = null,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, observedAt, {
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
    observedAt: number,
    reason: ParkReason,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, observedAt, {
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
    observedAt: number,
    error: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, observedAt, {
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
    observedAt: number,
    reason: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, expectedLeaseEpoch, observedAt, {
      resolution: "cancelled",
      reason,
    });
  }

  static migration(commandId: string, plan: RegionMigrationPlan): RegionMigrationCommand {
    if (commandId.length === 0) {
      throw new Error("virtual region migration requires a command identity");
    }
    return {
      control_version: "cymule.virtual-region-migration-control/1",
      command_id: commandId,
      plan,
    };
  }

  static compaction(
    commandId: string,
    regionId: string,
    sourceCausalCut: string[],
    compactorBinding: string,
    compactorRevision: string,
  ): VirtualCompactionCommand {
    const causalCut = [...new Set(sourceCausalCut)].sort();
    if (
      commandId.length === 0 ||
      regionId.length === 0 ||
      causalCut.length === 0 ||
      compactorBinding.length === 0 ||
      compactorRevision.length === 0
    ) {
      throw new Error("virtual compaction requires identities, a causal cut, binding, and revision");
    }
    return {
      control_version: "cymule.virtual-compaction-control/1",
      command_id: commandId,
      region_id: regionId,
      source_causal_cut: causalCut,
      compactor_binding: compactorBinding,
      compactor_revision: compactorRevision,
    };
  }

  static rehydration(
    commandId: string,
    certificateId: string,
    occurrenceIds: string[],
  ): VirtualRehydrationCommand {
    const occurrences = [...new Set(occurrenceIds)].sort();
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
    observedAt: number,
    resolution: WorkResolution,
  ): WorkResolutionCommand {
    if (
      commandId.length === 0 || workId.length === 0 || owner.length === 0 || epoch < 1 ||
      expectedLeaseEpoch < 1 || observedAt < 0
    ) {
      throw new Error("virtual work control requires identities, work and lease fences, and logical time");
    }
    return {
      control_version: "cymule.virtual-work-control/1",
      command_id: commandId,
      work_id: workId,
      owner,
      epoch,
      expected_lease_epoch: expectedLeaseEpoch,
      observed_at: observedAt,
      resolution,
    };
  }
}

export class VirtualSchedulingControlBuilder {
  static claim(
    commandId: string,
    owner: string,
    slotId: string,
    planId: string,
    occurrenceBinding: string,
    capabilities: string[],
    logicalNow: number,
    leaseTtl: number,
  ): VirtualClaimCommand {
    const sortedCapabilities = [...new Set(capabilities)].sort();
    if (
      commandId.length === 0 ||
      owner.length === 0 ||
      slotId.length === 0 ||
      planId.length === 0 ||
      occurrenceBinding.length === 0 ||
      sortedCapabilities.some((capability) => capability.length === 0) ||
      logicalNow < 0 ||
      leaseTtl < 1
    ) {
      throw new Error("virtual claim requires identities, binding, logical time, and positive TTL");
    }
    return {
      control_version: "cymule.virtual-claim-control/2",
      command_id: commandId,
      owner,
      slot_id: slotId,
      plan_id: planId,
      occurrence_binding: occurrenceBinding,
      capabilities: sortedCapabilities,
      logical_now: logicalNow,
      lease_ttl: leaseTtl,
    };
  }

  static renew(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    expectedLeaseEpoch: number,
    logicalNow: number,
    leaseTtl: number,
  ): VirtualLeaseRenewalCommand {
    if (
      commandId.length === 0 ||
      workId.length === 0 ||
      owner.length === 0 ||
      epoch < 1 ||
      expectedLeaseEpoch < 1 ||
      logicalNow < 0 ||
      leaseTtl < 1
    ) {
      throw new Error("virtual renewal requires identities, fences, logical time, and positive TTL");
    }
    return {
      control_version: "cymule.virtual-lease-renewal-control/1",
      command_id: commandId,
      work_id: workId,
      owner,
      epoch,
      expected_lease_epoch: expectedLeaseEpoch,
      logical_now: logicalNow,
      lease_ttl: leaseTtl,
    };
  }

  static recovery(
    commandId: string,
    workId: string,
    expectedOwner: string,
    expectedEpoch: number,
    expectedLeaseEpoch: number,
    observedAt: number,
    resolution: VirtualRecoveryCommand["resolution"],
  ): VirtualRecoveryCommand {
    if (
      commandId.length === 0 ||
      workId.length === 0 ||
      expectedOwner.length === 0 ||
      expectedEpoch < 1 ||
      expectedLeaseEpoch < 1 ||
      observedAt < 0
    ) {
      throw new Error("virtual recovery requires identities, fences, and logical observation time");
    }
    return {
      control_version: "cymule.virtual-recovery-control/1",
      command_id: commandId,
      work_id: workId,
      expected_owner: expectedOwner,
      expected_epoch: expectedEpoch,
      expected_lease_epoch: expectedLeaseEpoch,
      observed_at: observedAt,
      resolution,
    };
  }

  static runWeight(commandId: string, runId: string, weight: number): VirtualRunWeightCommand {
    if (commandId.length === 0 || runId.length === 0 || weight < 1) {
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
      resource_version: "cymule.resource/2",
      shape: "inline",
      media_type: "text/plain;charset=utf-8",
      inline: { encoding: "utf8", text },
      integrity: { kind: "inline" },
      annotations,
    };
  }

  static json(value: Json, annotations: Record<string, string> = {}): ResourceCandidate {
    return {
      resource_version: "cymule.resource/2",
      shape: "inline",
      media_type: "application/json",
      inline: { encoding: "json", value },
      integrity: { kind: "inline" },
      annotations,
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
      resource_version: "cymule.resource/2",
      shape,
      media_type: mediaType,
      integrity,
      ...(manifest === undefined ? {} : { manifest }),
      annotations,
    };
  }

  static handoff(
    transferId: string,
    producer: { run_id: string; occurrence_id: string; result: ArtifactRef },
    toRun: string,
    slot: string,
    resource: ArtifactRef,
  ): ResourceHandoff {
    return {
      handoff_version: "cymule.resource-handoff/3",
      transfer_id: transferId,
      producer: structuredClone(producer),
      to_run: toRun,
      slot,
      resource,
    };
  }
}

export const ENGINE_PROTOCOL_VERSION = "cymule.engine/2" as const;

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

export class CliEngine {
  constructor(
    readonly executable = "cymule",
    readonly options: CliEngineOptions = {},
  ) {}

  seal(candidate: PlanCandidate): SealedPlan {
    const response = this.request({ type: "seal", candidate });
    if (response.type !== "sealed") throw unexpectedResponse("sealed", response.type);
    return response.plan;
  }

  sealResource(candidate: ResourceCandidate): ResourceHandle {
    const response = this.request({ type: "seal_resource", candidate });
    if (response.type !== "sealed_resource") {
      throw unexpectedResponse("sealed_resource", response.type);
    }
    return response.resource;
  }

  verifyWaitActivation(activation: WaitActivation): WaitActivation {
    const response = this.request({ type: "verify_wait_activation", activation });
    if (response.type !== "verified_wait_activation") {
      throw unexpectedResponse("verified_wait_activation", response.type);
    }
    return response.activation;
  }

  verifyDurableCommand(command: DurableCommand): DurableCommand {
    const response = this.request({ type: "verify_durable_command", command });
    if (response.type !== "verified_durable_command") {
      throw unexpectedResponse("verified_durable_command", response.type);
    }
    return response.command;
  }

  verifyEvolutionCommand(command: EvolutionCommand): EvolutionCommand {
    const response = this.request({ type: "verify_evolution_command", command });
    if (response.type !== "verified_evolution_command") {
      throw unexpectedResponse("verified_evolution_command", response.type);
    }
    return response.command;
  }

  verifyLiveEvolutionCommand(command: LiveEvolutionCommand): LiveEvolutionCommand {
    const response = this.request({ type: "verify_live_evolution_command", command });
    if (response.type !== "verified_live_evolution_command") {
      throw unexpectedResponse("verified_live_evolution_command", response.type);
    }
    return response.command;
  }

  executeDurable(target: EngineDurableTarget, command: DurableCommand): DurableResponse {
    const response = this.request({ type: "execute_durable", target, command });
    if (response.type !== "durable_executed") {
      throw unexpectedResponse("durable_executed", response.type);
    }
    return response.response;
  }

  executeLiveEvolution(
    target: EngineEvolutionTarget,
    journalId: string,
    command: LiveEvolutionCommand,
  ): LiveEvolutionResponse {
    const response = this.request({
      type: "execute_live_evolution",
      target,
      journal_id: journalId,
      command,
    });
    if (response.type !== "live_evolution_executed") {
      throw unexpectedResponse("live_evolution_executed", response.type);
    }
    return response.response;
  }

  run(plan: SealedPlan, input: Json, plugin: string, runId: string): ExecutionOutcome {
    const response = this.request({ type: "run", plan, input, plugin, run_id: runId });
    if (response.type !== "execution_boundary") {
      throw unexpectedResponse("execution_boundary", response.type);
    }
    return response.execution;
  }

  private request(request: EngineRequest): EngineResponse {
    if (this.options.signal?.aborted === true) {
      throw interruptedError(request, "cancelled");
    }
    const envelopeRequest = { engine_protocol: ENGINE_PROTOCOL_VERSION, request };
    assertStrictJson(envelopeRequest);
    const child = spawnSync(this.executable, ["rpc"], {
      input: JSON.stringify(envelopeRequest),
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
      timeout: this.options.timeoutMs,
      signal: this.options.signal,
    });
    if (child.error !== undefined) {
      const code = (child.error as NodeJS.ErrnoException).code;
      if (code === "ETIMEDOUT") throw interruptedError(request, "timed_out");
      if (code === "ABORT_ERR") throw interruptedError(request, "cancelled");
      throw transportError("engine_start_failed", child.error.message);
    }
    if (child.status !== 0) {
      throw transportError(
        "engine_process_failed",
        `engine exited without a protocol response (status ${child.status})`,
      );
    }
    let envelope: EngineResponseEnvelope;
    try {
      envelope = parseEngineEnvelope(parseStrictJson(child.stdout));
    } catch (error) {
      throw transportError(
        "invalid_engine_response",
        error instanceof Error ? error.message : String(error),
      );
    }
    if (envelope.engine_protocol !== ENGINE_PROTOCOL_VERSION) {
      throw new EngineError({
        category: "contract_violation",
        phase: "transport",
        code: "unsupported_engine_protocol",
        message: `expected ${ENGINE_PROTOCOL_VERSION}, received ${JSON.stringify(envelope.engine_protocol)}`,
        contract: ENGINE_PROTOCOL_VERSION,
        contract_side: "schema",
        retry_disposition: "never",
      });
    }
    if (envelope.outcome === "failure") throw new EngineError(envelope.error);
    if (envelope.outcome !== "success") {
      throw transportError("invalid_engine_response", "response outcome is not closed");
    }
    return envelope.response;
  }
}

export class DurableEngine {
  readonly #transport: CliEngine;

  constructor(
    readonly store: EngineStoreTarget | string,
    readonly executor: EnginePluginTarget | string | undefined,
    transport = new CliEngine(),
    readonly evolutionJournal = "cymule.sdk.live-evolution",
    readonly migration?: EnginePluginTarget,
    readonly shadow?: EnginePluginTarget,
  ) {
    this.#transport = transport;
  }

  start(runId: string, candidate: PlanCandidate, input: Json): DurableResponse {
    return this.submit(DurableControlBuilder.startRun(runId, candidate, input));
  }

  get(runId: string): DurableRunView | null {
    const response = this.submit(DurableControlBuilder.queryRun(`sdk:get:${runId}`, runId));
    if (response.type !== "run") throw unexpectedResponse("run", response.type);
    return response.run;
  }

  resume(runId: string): DurableResponse {
    return this.submit(DurableControlBuilder.resumeRun(runId));
  }

  signal(
    activationId: string,
    key: string,
    waitIds: string[],
    value: Json,
  ): DurableResponse {
    return this.submit(DurableControlBuilder.activateSignal(activationId, key, waitIds, value));
  }

  release(intentId: string): DurableResponse {
    return this.submit(DurableControlBuilder.releaseEffect(intentId));
  }

  evolve(command: LiveEvolutionCommand): LiveEvolutionResponse {
    return this.#transport.executeLiveEvolution(
      {
        store: typeof this.store === "string" ? directoryStore(this.store) : this.store,
        ...(this.migration === undefined ? {} : { migration: this.migration }),
        ...(this.shadow === undefined ? {} : { shadow: this.shadow }),
      },
      this.evolutionJournal,
      command,
    );
  }

  private submit(command: DurableCommand): DurableResponse {
    const query = command.type === "query_run" || command.type === "query_domain";
    const store = typeof this.store === "string" ? directoryStore(this.store) : this.store;
    const executor = typeof this.executor === "string"
      ? processPlugin(this.executor)
      : this.executor;
    return this.#transport.executeDurable(
      { store, ...(!query && executor !== undefined ? { executor } : {}) },
      command,
    );
  }
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
  | { type: "verify_evolution_command"; command: EvolutionCommand }
  | { type: "verify_live_evolution_command"; command: LiveEvolutionCommand }
  | { type: "execute_durable"; target: EngineDurableTarget; command: DurableCommand }
  | {
      type: "execute_live_evolution";
      target: EngineEvolutionTarget;
      journal_id: string;
      command: LiveEvolutionCommand;
    }
  | { type: "run"; plan: SealedPlan; input: Json; plugin: string; run_id: string };

type EngineResponse =
  | { type: "sealed"; plan: SealedPlan }
  | { type: "sealed_resource"; resource: ResourceHandle }
  | { type: "verified_wait_activation"; activation: WaitActivation }
  | { type: "verified_durable_command"; command: DurableCommand }
  | { type: "verified_evolution_command"; command: EvolutionCommand }
  | { type: "verified_live_evolution_command"; command: LiveEvolutionCommand }
  | { type: "execution_boundary"; execution: ExecutionOutcome }
  | { type: "durable_executed"; response: DurableResponse }
  | { type: "live_evolution_executed"; response: LiveEvolutionResponse }
  | { type: "verified" };

type EngineResponseEnvelope =
  | {
      outcome: "success";
      engine_protocol: typeof ENGINE_PROTOCOL_VERSION;
      response: EngineResponse;
    }
  | {
      outcome: "failure";
      engine_protocol: typeof ENGINE_PROTOCOL_VERSION;
      error: EngineFailure;
    };

const ENGINE_FAILURE_CATEGORIES = new Set<EngineFailureCategory>([
  "transport_failure", "validation", "contract_violation", "admission_denied", "conflict",
  "not_found", "expected_plugin_failure", "plugin_defect", "substrate_failure", "cancelled",
  "timed_out", "unknown_world_outcome",
]);
const ENGINE_PHASES = new Set<EnginePhase>([
  "transport", "decode_request", "validate_request", "seal_plan", "verify_plan",
  "seal_resource", "verify_wait_activation", "verify_durable_command",
  "verify_evolution_command", "verify_live_evolution_command", "execute_plan",
  "execute_durable", "execute_live_evolution",
  "plugin_describe", "plugin_call", "effect_prepare", "effect_dispatch", "effect_reconcile",
  "encode_response",
]);
const ENGINE_RETRIES = new Set<EngineRetryDisposition>([
  "never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile",
]);

function parseEngineEnvelope(value: unknown): EngineResponseEnvelope {
  if (!isRecord(value) || (value.outcome !== "success" && value.outcome !== "failure")) {
    throw transportError("invalid_engine_response", "response envelope is not closed");
  }
  const keys = Object.keys(value).sort().join(",");
  const expected = value.outcome === "success"
    ? "engine_protocol,outcome,response"
    : "engine_protocol,error,outcome";
  if (keys !== expected) {
    throw transportError("invalid_engine_response", "response envelope fields are not closed");
  }
  if (value.outcome === "failure") {
    validateEngineFailure(value.error);
  } else {
    validateSuccessResponse(value.response);
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
    ["verified_evolution_command", "command,type"],
    ["verified_live_evolution_command", "command,type"],
    ["execution_boundary", "execution,type"], ["verified", "type"],
    ["durable_executed", "response,type"],
    ["live_evolution_executed", "response,type"],
  ]).get(value.type);
  if (payload === undefined || Object.keys(value).sort().join(",") !== payload) {
    throw transportError("invalid_engine_response", "success response fields are not closed");
  }
  if (value.type === "sealed") validateSealedPlan(value.plan);
  if (value.type === "execution_boundary") validateExecutionOutcome(value.execution);
  if (value.type === "verified_evolution_command") validateEvolutionCommand(value.command);
  if (value.type === "verified_live_evolution_command") validateLiveEvolutionCommand(value.command);
  if (value.type === "durable_executed") validateDurableResponse(value.response);
  if (value.type === "live_evolution_executed") validateLiveEvolutionResponse(value.response);
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
  if (value.status === "completed") {
    requireStrings(nested, ["run_id", "plan_id", "projection_digest", "precondition_token"]);
    if (!Array.isArray(nested.effects) || !nested.effects.every(isNonEmptyString)) {
      throw transportError("invalid_engine_response", "execution effects are invalid");
    }
  } else if (value.status === "suspended") {
    requireStrings(nested, ["run_id", "plan_id", "definition_id", "invocation_id", "site_id"]);
    if (nested.result_bind !== null && !isNonEmptyString(nested.result_bind)) {
      throw transportError("invalid_engine_response", "wait result binding is invalid");
    }
    validateWaitSpec(nested.wait);
  } else if (value.status === "release_required") {
    requireStrings(nested, ["run_id", "plan_id"]);
    if (!Array.isArray(nested.intent_ids) || nested.intent_ids.length === 0 ||
      !nested.intent_ids.every(isNonEmptyString)) {
      throw transportError("invalid_engine_response", "effect release intents are invalid");
    }
  } else {
    requireStrings(nested, ["run_id", "plan_id", "intent_id"]);
  }
}

function validateEvolutionCommand(value: unknown): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "evolution command is not an object");
  }
  const fields = new Map<string, string>([
    ["apply_patch", "command_id,control_version,operation,patch"],
    ["set_rollout", "command_id,control_version,decision,operation"],
    ["select_occurrence", "command_id,control_version,occurrence_id,operation"],
    ["migrate", "command_id,control_version,operation,request"],
    ["restart_under_new_plan", "command_id,control_version,operation,request"],
    ["shadow", "command_id,control_version,operation,request"],
    ["observe", "command_id,control_version,observation,operation"],
    ["apply_gate", "command_id,control_version,gate,next_decision_id,operation"],
  ]).get(String(value.operation));
  if (
    value.control_version !== "cymule.evolution-control/3"
    || fields === undefined
    || Object.keys(value).sort().join(",") !== fields
  ) {
    throw transportError("invalid_engine_response", "evolution command is not closed");
  }
  if (value.operation === "migrate") {
    validateEvolutionRequest(value.request, [
      "from_plan", "input_state", "migration_id", "run_id", "safe_point_id",
      "source_binding", "source_epoch", "target_binding", "to_plan",
    ], ["migration_id", "run_id", "from_plan", "to_plan", "safe_point_id"]);
    const request = value.request as Record<string, unknown>;
    validateArtifactRef(request.input_state);
    validateArtifactRef(request.source_binding);
    validateArtifactRef(request.target_binding);
    requireEpoch(request.source_epoch);
  } else if (value.operation === "restart_under_new_plan") {
    validateEvolutionRequest(value.request, [
      "evidence", "from_plan", "input", "replacement_run", "restart_id", "safe_point_id",
      "source_epoch", "source_run", "to_plan",
    ], ["restart_id", "source_run", "replacement_run", "from_plan", "to_plan", "safe_point_id"]);
    const request = value.request as Record<string, unknown>;
    validateArtifactRef(request.input);
    validateArtifactRef(request.evidence);
    requireEpoch(request.source_epoch);
  } else if (value.operation === "shadow") {
    validateEvolutionRequest(value.request, [
      "comparison_id", "comparison_policy", "decision_id", "input", "primary_plan",
      "shadow_plan", "subject",
    ], ["comparison_id", "decision_id", "subject", "primary_plan", "shadow_plan", "comparison_policy"]);
    validateArtifactRef((value.request as Record<string, unknown>).input);
  }
}

function validateLiveEvolutionCommand(value: unknown): void {
  if (!isRecord(value)) {
    throw transportError("invalid_engine_response", "live evolution command is not an object");
  }
  const fields = new Map<string, string[]>([
    ["publish_definition", ["command_id,control_version,definition,logical_ref,operation"]],
    ["register_template", ["command_id,control_version,operation,template"]],
    ["publish_and_relink", ["command_id,control_version,operation,publication"]],
    ["apply", [
      "command,command_id,control_version,operation,template_id",
      "command,command_id,control_version,operation,safe_point,template_id",
    ]],
  ]).get(String(value.operation));
  if (
    value.control_version !== "cymule.live-evolution-control/2"
    || fields === undefined
    || !fields.includes(Object.keys(value).sort().join(","))
  ) {
    throw transportError("invalid_engine_response", "live evolution command is not closed");
  }
  if (value.operation === "apply") validateEvolutionCommand(value.command);
}

function validateDurableResponse(value: unknown): void {
  if (!isRecord(value) || typeof value.type !== "string") {
    throw transportError("invalid_engine_response", "durable response is not tagged");
  }
  const keys = new Map<string, string>([
    ["run_boundary", "boundary,type"], ["wait_activated", "ready_run_ids,type"],
    ["run", "run,type"], ["domain", "domain,type"],
  ]).get(value.type);
  if (keys === undefined || Object.keys(value).sort().join(",") !== keys) {
    throw transportError("invalid_engine_response", "durable response fields are not closed");
  }
  if (value.type === "run_boundary") {
    validateDurableBoundary(value.boundary);
  } else if (value.type === "wait_activated") {
    requireStringArray(value.ready_run_ids, "ready Run identities");
  } else if (value.type === "run") {
    if (value.run !== null) validateDurableRunView(value.run);
  } else {
    requireClosedRecord(value.domain, ["revision", "run_ids"], "durable domain view");
    if (value.domain.revision !== null && !isNonEmptyString(value.domain.revision)) {
      throw transportError("invalid_engine_response", "durable revision is invalid");
    }
    requireStringArray(value.domain.run_ids, "durable Run index");
  }
}

function validateDurableBoundary(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "durable boundary is invalid");
  const fields = new Map<string, string>([
    ["suspended", "status,wait_id"],
    ["reconciliation_required", "intent_id,status"],
    ["release_required", "intent_ids,status"],
    ["completed", "result,status"],
  ]).get(String(value.status));
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) {
    throw transportError("invalid_engine_response", "durable boundary is not closed");
  }
  if (value.status === "suspended") requireStrings(value, ["wait_id"]);
  if (value.status === "reconciliation_required") requireStrings(value, ["intent_id"]);
  if (value.status === "release_required") requireStringArray(value.intent_ids, "effect intents");
  if (value.status === "completed") validateExecutionResult(value.result);
}

function validateExecutionResult(value: unknown): void {
  requireClosedRecord(value, ["run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"], "execution result");
  requireStrings(value, ["run_id", "plan_id", "projection_digest", "precondition_token"]);
  requireStringArray(value.effects, "execution effects");
}

function validateDurableRunView(value: unknown): void {
  requireClosedRecord(value, ["revision", "continuation", "waits", "effects", "result"], "durable Run view");
  requireStrings(value, ["revision"]);
  validateContinuation(value.continuation);
  if (!Array.isArray(value.waits)) throw transportError("invalid_engine_response", "durable waits are invalid");
  value.waits.forEach(validateWaitCondition);
  if (!Array.isArray(value.effects)) throw transportError("invalid_engine_response", "durable effects are invalid");
  value.effects.forEach(validateEffectDispatch);
  if (value.result !== null) validateArtifactRef(value.result);
}

function validateContinuation(value: unknown): void {
  requireClosedRecord(value, [
    "run_id", "plan_id", "binding_context", "frames", "state", "wait_set", "scope_stack",
    "effect_obligations", "authority_leases", "budget", "causal_frontier", "epoch", "status",
  ], "Continuation");
  requireStrings(value, ["run_id", "plan_id", "binding_context"]);
  if (!new Set(["ready", "waiting", "running", "completed"]).has(String(value.status))) {
    throw transportError("invalid_engine_response", "Continuation status is invalid");
  }
  requireEpoch(value.epoch);
  for (const field of ["wait_set", "scope_stack", "effect_obligations", "authority_leases", "causal_frontier"]) {
    requireStringArray(value[field], `Continuation ${field}`);
  }
  if (value.state !== null) validateArtifactRef(value.state);
  if (!isRecord(value.budget) || !Object.values(value.budget).every(isNonNegativeInteger)) {
    throw transportError("invalid_engine_response", "Continuation budget is invalid");
  }
  if (!Array.isArray(value.frames)) throw transportError("invalid_engine_response", "Continuation frames are invalid");
  for (const frame of value.frames) {
    requireClosedRecord(frame, ["definition_id", "invocation_id", "invocation_path", "scope_id", "input", "region_path", "next_step", "locals"], "Continuation frame");
    requireStrings(frame, ["definition_id", "invocation_id", "scope_id"]);
    validateArtifactRef(frame.input);
    requireIndexArray(frame.region_path, "frame Region path");
    if (!isNonNegativeInteger(frame.next_step) || !isRecord(frame.locals)) throw transportError("invalid_engine_response", "Continuation frame is invalid");
    Object.values(frame.locals).forEach(validateArtifactRef);
    if (!Array.isArray(frame.invocation_path)) throw transportError("invalid_engine_response", "invocation path is invalid");
    for (const segment of frame.invocation_path) {
      requireClosedRecord(segment, ["site_id", "region_path", "scope_id", "epoch"], "invocation segment");
      requireStrings(segment, ["site_id", "scope_id"]);
      requireIndexArray(segment.region_path, "invocation Region path");
      requireEpoch(segment.epoch);
    }
  }
}

function validateWaitCondition(value: unknown): void {
  requireClosedRecord(value, ["wait_id", "run_id", "kind", "consume_once", "owner", "state", "result"], "wait condition");
  requireStrings(value, ["wait_id", "run_id"]);
  if (typeof value.consume_once !== "boolean" || !new Set(["pending", "completed", "cancelled"]).has(String(value.state))) throw transportError("invalid_engine_response", "wait condition state is invalid");
  validateDurableWaitKind(value.kind);
  requireClosedRecord(value.owner, ["invocation_id", "definition_id", "site_id", "region_path", "step_index", "bind"], "wait owner");
  requireStrings(value.owner, ["invocation_id", "definition_id", "site_id"]);
  requireIndexArray(value.owner.region_path, "wait owner Region path");
  requireEpoch(value.owner.step_index);
  if (value.owner.bind !== null && !isNonEmptyString(value.owner.bind)) throw transportError("invalid_engine_response", "wait bind is invalid");
  if (value.result !== null) validateArtifactRef(value.result);
}

function validateDurableWaitKind(value: unknown): void {
  if (!isRecord(value)) throw transportError("invalid_engine_response", "durable wait kind is invalid");
  const fields = value.kind === "signal" ? "key,kind" : value.kind === "timer" ? "kind,timer_id" : value.kind === "input" ? "correlation,kind,schema" : undefined;
  if (fields === undefined || Object.keys(value).sort().join(",") !== fields) throw transportError("invalid_engine_response", "durable wait kind is not closed");
  if (value.kind === "signal") requireStrings(value, ["key"]);
  if (value.kind === "timer") requireStrings(value, ["timer_id"]);
  if (value.kind === "input") requireStrings(value, ["correlation"]);
}

function validateEffectDispatch(value: unknown): void {
  requireClosedRecord(value, ["intent_id", "run_id", "operation", "input", "occurrence_binding", "state", "claim_epoch", "claim_owner", "result"], "effect dispatch");
  requireStrings(value, ["intent_id", "run_id", "operation", "occurrence_binding"]);
  validateArtifactRef(value.input);
  requireEpoch(value.claim_epoch);
  if (!new Set(["pending", "claimed", "applied", "not_applied", "unknown"]).has(String(value.state))) throw transportError("invalid_engine_response", "effect state is invalid");
  if (value.claim_owner !== null && !isNonEmptyString(value.claim_owner)) throw transportError("invalid_engine_response", "effect claim owner is invalid");
  if (value.result !== null) validateArtifactRef(value.result);
}

function validateLiveEvolutionResponse(value: unknown): void {
  if (!isRecord(value) || typeof value.result !== "string") {
    throw transportError("invalid_engine_response", "live-evolution response is not tagged");
  }
  const keys = new Map<string, string>([
    ["definition_published", "result,revision"], ["template_registered", "linked,result"],
    ["publication_applied", "receipt,result"], ["patch_applied", "edge,result"],
    ["applied", "result"], ["occurrence_selected", "plan_id,result"],
    ["migrated", "receipt,result"], ["restart_authorized", "receipt,result"],
    ["shadow_recorded", "comparison,result"], ["gate_applied", "result,transition"],
  ]).get(value.result);
  if (keys === undefined || Object.keys(value).sort().join(",") !== keys) {
    throw transportError("invalid_engine_response", "live-evolution response fields are not closed");
  }
  switch (value.result) {
    case "definition_published": validateSubflowRevision(value.revision); break;
    case "template_registered": validateLinkedPlan(value.linked); break;
    case "publication_applied":
      requireClosedRecord(value.receipt, ["revision", "updates"], "publication receipt");
      validateSubflowRevision(value.receipt.revision);
      if (!Array.isArray(value.receipt.updates)) throw transportError("invalid_engine_response", "publication updates are invalid");
      for (const update of value.receipt.updates) {
        requireClosedRecord(update, ["template_id", "previous_plan_id", "current_plan_id", "decision_id", "advanced"], "template update");
        requireStrings(update, ["template_id", "previous_plan_id", "current_plan_id"]);
        if ((update.decision_id !== null && !isNonEmptyString(update.decision_id)) || typeof update.advanced !== "boolean") throw transportError("invalid_engine_response", "template update is invalid");
      }
      break;
    case "patch_applied": validatePlanEdge(value.edge); break;
    case "occurrence_selected": if (!isNonEmptyString(value.plan_id)) throw transportError("invalid_engine_response", "selected Plan is invalid"); break;
    case "migrated": validateMigrationReceipt(value.receipt); break;
    case "restart_authorized":
      requireClosedRecord(value.receipt, ["request", "target_plan"], "restart receipt");
      validateEvolutionRequest(value.receipt.request, ["evidence", "from_plan", "input", "replacement_run", "restart_id", "safe_point_id", "source_epoch", "source_run", "to_plan"], ["restart_id", "source_run", "replacement_run", "from_plan", "to_plan", "safe_point_id"]);
      validateSealedPlan(value.receipt.target_plan);
      break;
    case "shadow_recorded": validateShadowComparison(value.comparison); break;
    case "gate_applied": validateRolloutTransition(value.transition); break;
  }
}

function validateSubflowRevision(value: unknown): void {
  requireClosedRecord(value, ["revision_version", "revision_id", "logical_ref", "sequence", "definition", "references"], "subflow revision");
  if (value.revision_version !== "cymule.subflow-revision/2") throw transportError("invalid_engine_response", "subflow revision version is invalid");
  requireStrings(value, ["revision_id", "logical_ref"]);
  requireEpoch(value.sequence);
  if (!isRecord(value.definition) || !Array.isArray(value.references)) throw transportError("invalid_engine_response", "subflow revision payload is invalid");
  for (const reference of value.references) {
    requireClosedRecord(reference, ["logical_ref", "local_definition", "input_schema", "output_schema", "strategy"], "subflow reference");
    requireStrings(reference, ["logical_ref", "local_definition"]);
    if (!isRecord(reference.strategy) || !new Set(["latest_compatible", "pinned"]).has(String(reference.strategy.strategy))) throw transportError("invalid_engine_response", "subflow strategy is invalid");
  }
}

function validateLinkedPlan(value: unknown): void {
  requireClosedRecord(value, ["template_id", "plan", "resolved_revisions"], "linked Plan");
  requireStrings(value, ["template_id"]);
  validateSealedPlan(value.plan);
  if (!isRecord(value.resolved_revisions) || !Object.values(value.resolved_revisions).every(isNonEmptyString)) throw transportError("invalid_engine_response", "resolved revisions are invalid");
}

function validatePlanEdge(value: unknown): void {
  requireClosedRecord(value, ["edge_id", "from_plan", "to_plan", "operations", "evidence"], "Plan edge");
  requireStrings(value, ["edge_id", "from_plan", "to_plan"]);
  validateArtifactRef(value.evidence);
  if (!Array.isArray(value.operations)) throw transportError("invalid_engine_response", "Plan edge operations are invalid");
  for (const operation of value.operations) requireClosedRecord(operation, ["kind", "target", "before", "after"], "patch operation");
}

function validateMigrationReceipt(value: unknown): void {
  requireClosedRecord(value, ["migration_id", "run_id", "from_plan", "to_plan", "safe_point_id", "source_epoch", "target_epoch", "source_binding", "target_binding", "adapter_id", "adapter_revision", "from_schema", "to_schema", "input_state", "output_state", "evidence"], "migration receipt");
  requireStrings(value, ["migration_id", "run_id", "from_plan", "to_plan", "safe_point_id", "adapter_id", "adapter_revision", "from_schema", "to_schema"]);
  requireEpoch(value.source_epoch); requireEpoch(value.target_epoch);
  for (const field of ["source_binding", "target_binding", "input_state", "output_state", "evidence"]) validateArtifactRef(value[field]);
}

function validateShadowComparison(value: unknown): void {
  requireClosedRecord(value, ["comparison_id", "subject", "decision_id", "primary_plan", "shadow_plan", "driver_id", "driver_revision", "comparison_policy", "primary_digest", "shadow_digest", "equivalent", "evidence"], "shadow comparison");
  requireStrings(value, ["comparison_id", "subject", "decision_id", "primary_plan", "shadow_plan", "driver_id", "driver_revision", "comparison_policy", "primary_digest", "shadow_digest"]);
  if (typeof value.equivalent !== "boolean") throw transportError("invalid_engine_response", "shadow comparison result is invalid");
  validateArtifactRef(value.evidence);
}

function validateRolloutTransition(value: unknown): void {
  requireClosedRecord(value, ["transition_id", "from_decision", "to_decision", "evaluation"], "rollout transition");
  requireStrings(value, ["transition_id", "from_decision", "to_decision"]);
  requireClosedRecord(value.evaluation, ["evaluation_id", "gate", "target_observations", "target_failures", "equivalent_shadows", "inequivalent_shadows", "outcome", "evidence_ids"], "rollout evaluation");
  requireStrings(value.evaluation, ["evaluation_id"]);
  if (!new Set(["pending", "promote", "rollback"]).has(String(value.evaluation.outcome))) throw transportError("invalid_engine_response", "rollout outcome is invalid");
  for (const field of ["target_observations", "target_failures", "equivalent_shadows", "inequivalent_shadows"]) requireEpoch(value.evaluation[field]);
  requireStringArray(value.evaluation.evidence_ids, "rollout evidence");
  if (!isRecord(value.evaluation.gate)) throw transportError("invalid_engine_response", "rollout gate is invalid");
}

function validateSealedPlan(value: unknown): void {
  requireClosedRecord(value, ["candidate", "plan_id"], "sealed Plan");
  requireStrings(value, ["plan_id"]);
  if (!isRecord(value.candidate)) {
    throw transportError("invalid_engine_response", "sealed Plan candidate is invalid");
  }
}

function validateWaitSpec(value: unknown): void {
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
  if (value.kind === "signal") {
    requireStrings(value, ["key"]);
    if (typeof value.consume_once !== "boolean") {
      throw transportError("invalid_engine_response", "signal wait consume_once is invalid");
    }
  } else if (value.kind === "timer") {
    requireStrings(value, ["timer_id"]);
  } else {
    requireStrings(value, ["correlation"]);
    if (!isRecord(value.schema) && typeof value.schema !== "boolean") {
      throw transportError("invalid_engine_response", "input wait schema is invalid");
    }
  }
}

function validateArtifactRef(value: unknown): void {
  requireClosedRecord(value, ["artifact_id", "identity_version", "kind"], "Artifact reference");
  requireStrings(value, ["artifact_id", "identity_version", "kind"]);
  if (value.identity_version !== "cymule.artifact/2" ||
    !/^sha256:[0-9a-f]{64}$/.test(String(value.artifact_id))) {
    throw transportError("invalid_engine_response", "Artifact reference identity is invalid");
  }
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
    typeof value.message !== "string" || value.message.length < 1 || Buffer.byteLength(value.message) > 8192
  ) {
    throw transportError("invalid_engine_response", "Engine failure fields are invalid");
  }
  if (
    value.retry_disposition !== undefined &&
    (typeof value.retry_disposition !== "string" || !ENGINE_RETRIES.has(value.retry_disposition as EngineRetryDisposition))
  ) {
    throw transportError("invalid_engine_response", "retry disposition is unknown");
  }
  if (value.contract !== undefined && (typeof value.contract !== "string" || Buffer.byteLength(value.contract) < 1 || Buffer.byteLength(value.contract) > 500)) {
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
        typeof issue.code !== "string" || Buffer.byteLength(issue.code) < 1 || Buffer.byteLength(issue.code) > 200 ||
        typeof issue.message !== "string" || Buffer.byteLength(issue.message) < 1 || Buffer.byteLength(issue.message) > 2000) {
        throw transportError("invalid_engine_response", "Engine issue is invalid");
      }
      validateEnginePath(issue.path);
      validateEnginePath(issue.schema_path);
    }
  }
}

function validateEnginePath(value: unknown): void {
  if (value !== undefined && (typeof value !== "string" || Buffer.byteLength(value) > 1000 || (value !== "" && !value.startsWith("/")))) {
    throw transportError("invalid_engine_response", "Engine failure path is invalid");
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function interruptedError(request: EngineRequest, kind: "cancelled" | "timed_out"): EngineError {
  const mutating = request.type === "run" || request.type === "execute_live_evolution" ||
    (request.type === "execute_durable" && !request.command.type.startsWith("query_"));
  if (mutating) {
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
  });
}

const MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991;

function assertStrictJson(value: unknown, seen = new Set<object>()): void {
  if (typeof value === "bigint") throw transportError("request_encoding_failed", "bigint is not JSON");
  if (typeof value === "number" && (!Number.isFinite(value) ||
    (Number.isInteger(value) && Math.abs(value) > MAX_SAFE_JSON_INTEGER))) {
    throw transportError("request_encoding_failed", "number is outside the shared JSON domain");
  }
  if (typeof value !== "object" || value === null) return;
  if (seen.has(value)) throw transportError("request_encoding_failed", "cyclic value is not JSON");
  seen.add(value);
  for (const child of Array.isArray(value) ? value : Object.values(value)) assertStrictJson(child, seen);
  seen.delete(value);
}

function parseStrictJson(text: string): unknown {
  let index = 0;
  const whitespace = () => { while (/\s/.test(text[index] ?? "")) index += 1; };
  const stringToken = (): string => {
    const start = index++;
    while (index < text.length) {
      if (text[index] === "\\") { index += 2; continue; }
      if (text[index++] === '"') return JSON.parse(text.slice(start, index)) as string;
    }
    throw new Error("unterminated JSON string");
  };
  const value = (): void => {
    whitespace();
    if (text[index] === "{") {
      index += 1; whitespace();
      const keys = new Set<string>();
      if (text[index] === "}") { index += 1; return; }
      while (true) {
        if (text[index] !== '"') throw new Error("object key is not a string");
        const key = stringToken();
        if (keys.has(key)) throw new Error(`duplicate JSON object key ${JSON.stringify(key)}`);
        keys.add(key); whitespace();
        if (text[index++] !== ":") throw new Error("missing object colon");
        value(); whitespace();
        if (text[index] === "}") { index += 1; return; }
        if (text[index++] !== ",") throw new Error("missing object comma");
        whitespace();
      }
    }
    if (text[index] === "[") {
      index += 1; whitespace();
      if (text[index] === "]") { index += 1; return; }
      while (true) {
        value(); whitespace();
        if (text[index] === "]") { index += 1; return; }
        if (text[index++] !== ",") throw new Error("missing array comma");
      }
    }
    if (text[index] === '"') { stringToken(); return; }
    const match = /^(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)/.exec(text.slice(index));
    if (match === null) throw new Error("invalid JSON value");
    if (/^-?\d+$/.test(match[0]) && Math.abs(Number(match[0])) > MAX_SAFE_JSON_INTEGER) {
      throw new Error("integer is outside the shared JSON domain");
    }
    index += match[0].length;
  };
  value(); whitespace();
  if (index !== text.length) throw new Error("trailing JSON content");
  const parsed = JSON.parse(text) as unknown;
  assertStrictJson(parsed);
  return parsed;
}
