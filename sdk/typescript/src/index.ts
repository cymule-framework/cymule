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
  artifact_id: string;
  kind: string;
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

export interface WorkOccurrence {
  occurrence_version: "cymule.virtual-work-occurrence/1";
  occurrence_id: string;
  work_id: string;
  region_id: string;
  run_id: string;
  owner: string;
  epoch: number;
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
  resolution: WorkResolution;
}

export interface VirtualWorkControl {
  occurrence(occurrenceId: string): Promise<WorkOccurrence | null>;
  resolve(command: WorkResolutionCommand): Promise<WorkOccurrence>;
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
  ir_version: "cymule.ir/1";
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
  | { id: string; op: "wait"; wait: WaitSpec }
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

export class FlowBuilder {
  readonly #candidate: PlanCandidate;

  constructor(name: string, inputSchema: Schema, outputSchema: Schema) {
    this.#candidate = {
      ir_version: "cymule.ir/1",
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

  effect(site: string, effect: string, input: Expression, occurrence: string): this {
    this.entry().body.steps.push({ id: site, op: "effect", effect, input, occurrence });
    return this;
  }

  wait(site: string, wait: WaitSpec): this {
    this.entry().body.steps.push({ id: site, op: "wait", wait });
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

export class VirtualWorkControlBuilder {
  static succeed(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    result: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, {
      resolution: "succeeded",
      result,
    });
  }

  static retry(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    error: ArtifactRef,
    nextReason: ParkReason | null = null,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, {
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
    reason: ParkReason,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, {
      resolution: "parked",
      reason,
    });
  }

  static fail(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    error: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, {
      resolution: "failed",
      error,
    });
  }

  static cancel(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    reason: ArtifactRef,
  ): WorkResolutionCommand {
    return VirtualWorkControlBuilder.build(commandId, workId, owner, epoch, {
      resolution: "cancelled",
      reason,
    });
  }

  private static build(
    commandId: string,
    workId: string,
    owner: string,
    epoch: number,
    resolution: WorkResolution,
  ): WorkResolutionCommand {
    if (commandId.length === 0 || workId.length === 0 || owner.length === 0 || epoch < 1) {
      throw new Error("virtual work control requires command, work, owner, and positive epoch");
    }
    return {
      control_version: "cymule.virtual-work-control/1",
      command_id: commandId,
      work_id: workId,
      owner,
      epoch,
      resolution,
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

export class CliEngine {
  constructor(readonly executable: string) {}

  seal(candidate: PlanCandidate): SealedPlan {
    const response = this.request({ type: "seal", candidate });
    if (response.type !== "sealed") throw new Error(`unexpected response ${response.type}`);
    return response.plan;
  }

  sealResource(candidate: ResourceCandidate): ResourceHandle {
    const response = this.request({ type: "seal_resource", candidate });
    if (response.type !== "sealed_resource") {
      throw new Error(`unexpected response ${response.type}`);
    }
    return response.resource;
  }

  verifyWaitActivation(activation: WaitActivation): WaitActivation {
    const response = this.request({ type: "verify_wait_activation", activation });
    if (response.type !== "verified_wait_activation") {
      throw new Error(`unexpected response ${response.type}`);
    }
    return response.activation;
  }

  run(plan: SealedPlan, input: Json, plugin: string, runId: string): ExecutionResult {
    const response = this.request({ type: "run", plan, input, plugin, run_id: runId });
    if (response.type !== "executed") throw new Error(`unexpected response ${response.type}`);
    return response.result;
  }

  private request(request: EngineRequest): EngineResponse {
    const child = spawnSync(this.executable, ["rpc"], {
      input: JSON.stringify(request),
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
    });
    if (child.error !== undefined) throw child.error;
    if (child.status !== 0) throw new Error(child.stderr.trim() || `engine exited ${child.status}`);
    return JSON.parse(child.stdout) as EngineResponse;
  }
}

type EngineRequest =
  | { type: "seal"; candidate: PlanCandidate }
  | { type: "seal_resource"; candidate: ResourceCandidate }
  | { type: "verify_wait_activation"; activation: WaitActivation }
  | { type: "run"; plan: SealedPlan; input: Json; plugin: string; run_id: string };

type EngineResponse =
  | { type: "sealed"; plan: SealedPlan }
  | { type: "sealed_resource"; resource: ResourceHandle }
  | { type: "verified_wait_activation"; activation: WaitActivation }
  | { type: "executed"; result: ExecutionResult }
  | { type: "verified" };
