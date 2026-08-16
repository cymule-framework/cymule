import { spawnSync } from "node:child_process";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Schema = boolean | { [key: string]: Json };

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

export class CliEngine {
  constructor(readonly executable: string) {}

  seal(candidate: PlanCandidate): SealedPlan {
    const response = this.request({ type: "seal", candidate });
    if (response.type !== "sealed") throw new Error(`unexpected response ${response.type}`);
    return response.plan;
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
  | { type: "run"; plan: SealedPlan; input: Json; plugin: string; run_id: string };

type EngineResponse =
  | { type: "sealed"; plan: SealedPlan }
  | { type: "executed"; result: ExecutionResult }
  | { type: "verified" };
