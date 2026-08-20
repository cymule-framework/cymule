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
  | { kind: "resolver"; binding: string; reference: string };

export interface ResourceCandidate {
  resource_version: "cymule.resource/1";
  shape: ResourceShape;
  media_type: string;
  inline?: InlineData;
  integrity: ResourceIntegrity;
  locations?: ResourceLocation[];
  annotations?: Record<string, string>;
}

export interface ResourceHandle extends ResourceCandidate {
  resource_id: string;
}

export interface ResourceHandoff {
  handoff_version: "cymule.resource-handoff/1";
  transfer_id: string;
  from_run: string;
  to_run: string;
  slot: string;
  resource: ResourceHandle;
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
  control_version: "cymule.evolution-control/2";
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
  control_version: "cymule.live-evolution-control/1";
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
  occurrence_binding: string;
  lease: VirtualClaimLease;
}

export interface WorkOccurrence {
  occurrence_version: "cymule.virtual-work-occurrence/1";
  occurrence_id: string;
  work_id: string;
  region_id: string;
  run_id: string;
  owner: string;
  epoch: number;
  lease_epoch: number;
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
  certificate_version: "cymule.virtual-compaction-certificate/1";
  certificate_id: string;
  source_causal_cut: string[];
  summary: VirtualCompletionSummary;
  summary_state_digest: string;
  unresolved_obligations: string[];
  retained_occurrence_bindings: string[];
  replay_availability: ReplayAvailability;
  rehydration_manifest: ArtifactRef;
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
  control_version: "cymule.virtual-claim-control/1";
  command_id: string;
  owner: string;
  slot_id: string;
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

export type ExecutionOutcome =
  | { status: "completed"; result: ExecutionResult }
  | { status: "suspended"; suspension: SuspensionBoundary };

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

  component(id: string, inputSchema: Schema, outputSchema: Schema): this {
    this.#candidate.components.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
      requirements: {},
    });
    return this;
  }

  effectContract(
    id: string,
    inputSchema: Schema,
    outputSchema: Schema,
    profile: EffectProfile,
  ): this {
    this.#candidate.effects.push({
      id,
      input_schema: inputSchema,
      output_schema: outputSchema,
      profile,
      requirements: {},
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
      control_version: "cymule.evolution-control/2",
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
      control_version: "cymule.live-evolution-control/1",
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
      occurrenceBinding.length === 0 ||
      sortedCapabilities.some((capability) => capability.length === 0) ||
      logicalNow < 0 ||
      leaseTtl < 1
    ) {
      throw new Error("virtual claim requires identities, binding, logical time, and positive TTL");
    }
    return {
      control_version: "cymule.virtual-claim-control/1",
      command_id: commandId,
      owner,
      slot_id: slotId,
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
      resource_version: "cymule.resource/1",
      shape: "inline",
      media_type: "text/plain;charset=utf-8",
      inline: { encoding: "utf8", text },
      integrity: { kind: "inline" },
      annotations,
    };
  }

  static json(value: Json, annotations: Record<string, string> = {}): ResourceCandidate {
    return {
      resource_version: "cymule.resource/1",
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
    locations: ResourceLocation[],
    annotations: Record<string, string> = {},
  ): ResourceCandidate {
    return {
      resource_version: "cymule.resource/1",
      shape,
      media_type: mediaType,
      integrity,
      locations,
      annotations,
    };
  }

  static handoff(
    transferId: string,
    fromRun: string,
    toRun: string,
    slot: string,
    resource: ResourceHandle,
  ): ResourceHandoff {
    return {
      handoff_version: "cymule.resource-handoff/1",
      transfer_id: transferId,
      from_run: fromRun,
      to_run: toRun,
      slot,
      resource,
    };
  }
}

export const ENGINE_PROTOCOL_VERSION = "cymule.engine/1" as const;

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

export class CliEngine {
  constructor(readonly executable: string) {}

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

  run(plan: SealedPlan, input: Json, plugin: string, runId: string): ExecutionOutcome {
    const response = this.request({ type: "run", plan, input, plugin, run_id: runId });
    if (response.type !== "execution_boundary") {
      throw unexpectedResponse("execution_boundary", response.type);
    }
    return response.execution;
  }

  private request(request: EngineRequest): EngineResponse {
    const child = spawnSync(this.executable, ["rpc"], {
      input: JSON.stringify({ engine_protocol: ENGINE_PROTOCOL_VERSION, request }),
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
    });
    if (child.error !== undefined) {
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
      envelope = parseEngineEnvelope(parseUniqueJson(child.stdout));
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

function parseUniqueJson(input: string): unknown {
  let offset = 0;
  const whitespace = () => {
    while ([" ", "\t", "\r", "\n"].includes(input[offset] ?? "")) offset += 1;
  };
  const string = (): string => {
    const start = offset;
    if (input[offset] !== '"') throw new SyntaxError("expected a JSON object member name");
    offset += 1;
    while (offset < input.length) {
      if (input[offset] === "\\") {
        offset += 2;
      } else if (input[offset] === '"') {
        offset += 1;
        return JSON.parse(input.slice(start, offset)) as string;
      } else {
        offset += 1;
      }
    }
    throw new SyntaxError("unterminated JSON string");
  };
  const value = (): unknown => {
    whitespace();
    if (input[offset] === "{") {
      offset += 1;
      whitespace();
      const members = new Map<string, unknown>();
      if (input[offset] === "}") {
        offset += 1;
        return {};
      }
      while (offset < input.length) {
        const key = string();
        if (members.has(key)) throw new SyntaxError(`duplicate JSON object member ${JSON.stringify(key)}`);
        whitespace();
        if (input[offset] !== ":") throw new SyntaxError("expected ':' after JSON object member");
        offset += 1;
        members.set(key, value());
        whitespace();
        if (input[offset] === "}") {
          offset += 1;
          return Object.fromEntries(members);
        }
        if (input[offset] !== ",") throw new SyntaxError("expected ',' between JSON object members");
        offset += 1;
        whitespace();
      }
      throw new SyntaxError("unterminated JSON object");
    }
    if (input[offset] === "[") {
      offset += 1;
      whitespace();
      const values: unknown[] = [];
      if (input[offset] === "]") {
        offset += 1;
        return values;
      }
      while (offset < input.length) {
        values.push(value());
        whitespace();
        if (input[offset] === "]") {
          offset += 1;
          return values;
        }
        if (input[offset] !== ",") throw new SyntaxError("expected ',' between JSON array values");
        offset += 1;
      }
      throw new SyntaxError("unterminated JSON array");
    }
    if (input[offset] === '"') {
      return string();
    }
    const start = offset;
    while (
      offset < input.length
      && ![" ", "\t", "\r", "\n", ",", "]", "}"].includes(input[offset] ?? "")
    ) offset += 1;
    if (offset === start) throw new SyntaxError("expected a JSON value");
    return JSON.parse(input.slice(start, offset)) as unknown;
  };
  const parsed = value();
  whitespace();
  if (offset !== input.length) throw new SyntaxError("unexpected trailing JSON value");
  return parsed;
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
  | { type: "run"; plan: SealedPlan; input: Json; plugin: string; run_id: string };

type EngineResponse =
  | { type: "sealed"; plan: SealedPlan }
  | { type: "sealed_resource"; resource: ResourceHandle }
  | { type: "verified_wait_activation"; activation: WaitActivation }
  | { type: "verified_durable_command"; command: DurableCommand }
  | { type: "verified_evolution_command"; command: EvolutionCommand }
  | { type: "verified_live_evolution_command"; command: LiveEvolutionCommand }
  | { type: "execution_boundary"; execution: ExecutionOutcome }
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
  ]).get(value.type);
  if (payload === undefined || Object.keys(value).sort().join(",") !== payload) {
    throw transportError("invalid_engine_response", "success response fields are not closed");
  }
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
