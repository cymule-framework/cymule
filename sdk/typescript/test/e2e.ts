import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  CliEngine,
  DurableControlBuilder,
  EngineError,
  EvolutionControlBuilder,
  FlowBuilder,
  LiveEvolutionControlBuilder,
  ResourceBuilder,
  VirtualSchedulingControlBuilder,
  VirtualWorkControlBuilder,
  WaitActivationBuilder,
  type EffectProfile,
  type PlanCandidate,
} from "../src/index.js";

const profile: EffectProfile = {
  mutation: "mutating",
  dispatch: "on_scope_commit",
  reconciliation: "queryable",
  keyed_idempotency: true,
  irreversible: false,
};

test("TypeScript Engine ingress rejects duplicate JSON object members", () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "duplicate-engine");
  writeFileSync(
    executable,
    `#!/bin/sh
cat >/dev/null
printf '%s' '{"engine_protocol":"cymule.engine/1","outcome":"success","response":{"type":"sealed","type":"verified"}}'
`,
  );
  chmodSync(executable, 0o700);
  try {
    assert.throws(
      () => new CliEngine(executable).seal({} as PlanCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "invalid_engine_response"
        && error.failure.message.includes("duplicate JSON object member"),
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript Engine success and nested unions are closed", () => {
  const cases = [
    {
      response: { type: "verified_evolution_command", command: {
        control_version: "cymule.evolution-control/2",
        command_id: "command:test",
        operation: "future_operation",
      } },
      invoke: (engine: CliEngine) => engine.verifyEvolutionCommand({} as never),
    },
    {
      response: { type: "execution_boundary", execution: {
        status: "completed", result: {}, suspension: {},
      } },
      invoke: (engine: CliEngine) => engine.run({} as never, null, "plugin", "run:test"),
    },
  ];
  for (const [index, entry] of cases.entries()) {
    const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
    const executable = join(directory, `closed-engine-${index}`);
    writeFileSync(
      executable,
      `#!/bin/sh
cat >/dev/null
printf '%s' '${JSON.stringify({
        engine_protocol: "cymule.engine/1",
        outcome: "success",
        response: entry.response,
      })}'
`,
    );
    chmodSync(executable, 0o700);
    try {
      assert.throws(
        () => entry.invoke(new CliEngine(executable)),
        (error: unknown) => error instanceof EngineError
          && error.failure.code === "invalid_engine_response",
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
});

test("TypeScript accepts typed Effect execution boundaries", () => {
  const executions = [
    {
      status: "release_required",
      release: {
        run_id: "run:test",
        plan_id: "sha256:test",
        intent_ids: ["intent:test"],
      },
    },
    {
      status: "reconciliation_required",
      reconciliation: {
        run_id: "run:test",
        plan_id: "sha256:test",
        intent_id: "intent:test",
      },
    },
  ];
  for (const [index, execution] of executions.entries()) {
    const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
    const executable = join(directory, `effect-boundary-${index}`);
    writeFileSync(
      executable,
      `#!/bin/sh
cat >/dev/null
printf '%s' '${JSON.stringify({
        engine_protocol: "cymule.engine/1",
        outcome: "success",
        response: { type: "execution_boundary", execution },
      })}'
`,
    );
    chmodSync(executable, 0o700);
    try {
      assert.equal(
        new CliEngine(executable).run({} as never, null, "plugin", "run:test").status,
        execution.status,
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
});

test("TypeScript candidate seals and executes through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const pluginPath = process.env.CYMULE_TEST_PLUGIN;
  const expectedPlanId = process.env.CYMULE_EXPECTED_PLAN_ID;
  if (enginePath === undefined || pluginPath === undefined || expectedPlanId === undefined) {
    return;
  }

  const candidate = new FlowBuilder("cross_language_echo", {}, {})
    .component("test.echo", {}, {})
    .effectContract("test.capture", {}, {}, profile)
    .definition("echo_subflow", {}, {}, {
      steps: [{
        id: "call.echo",
        op: "call",
        component: "test.echo",
        input: { kind: "input" },
        bind: "echoed",
      }],
      result: { kind: "binding", name: "echoed" },
    })
    .invoke("invoke.echo-subflow", "echo_subflow", { kind: "input" }, "echoed")
    .effect(
      "effect.capture",
      "test.capture",
      { kind: "binding", name: "echoed" },
      "primary",
    )
    .finish({ kind: "binding", name: "echoed" });

  const engine = new CliEngine(enginePath);
  const plan = engine.seal(candidate);
  assert.equal(plan.plan_id, expectedPlanId);
  const input = { message: "hello from TypeScript" };
  const execution = engine.run(plan, input, pluginPath, "run:typescript-e2e");
  assert.equal(execution.status, "completed");
  if (execution.status !== "completed") throw new Error("expected terminal execution");
  const result = execution.result;
  assert.deepEqual(result.value, input);
  assert.equal(result.effects.length, 1);
});

test("TypeScript resource seals through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const expectedResourceId = process.env.CYMULE_EXPECTED_RESOURCE_ID;
  if (enginePath === undefined || expectedResourceId === undefined) return;

  const resource = new CliEngine(enginePath).sealResource(
    ResourceBuilder.text("shared cross-run resource", {
      purpose: "cross-language-conformance",
    }),
  );
  assert.equal(resource.resource_id, expectedResourceId);
  assert.equal(resource.integrity.kind, "inline");
});

test("TypeScript wait activation validates through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_WAIT_ACTIVATION_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) return;
  const activation = WaitActivationBuilder.signal(
    "activation:shared:1",
    "signal:continue",
    ["wait:shared:1"],
    {
      identity_version: "cymule.artifact/2",
      artifact_id: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      kind: "cymule.wait-activation-result/1",
    },
  );
  assert.deepEqual(activation, JSON.parse(readFileSync(fixturePath, "utf8")));
  assert.deepEqual(new CliEngine(enginePath).verifyWaitActivation(activation), activation);
});

test("TypeScript durable control fixture validates through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_DURABLE_CONTROL_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) return;
  const command = DurableControlBuilder.queryDomain("query:cross-language-domain");
  assert.deepEqual(command, JSON.parse(readFileSync(fixturePath, "utf8")));
  assert.deepEqual(new CliEngine(enginePath).verifyDurableCommand(command), command);

  const activation = DurableControlBuilder.activateSignal(
    "activation:sdk",
    "signal:sdk",
    ["wait:z", "wait:a", "wait:z"],
    { accepted: true },
  );
  assert.equal(activation.type, "activate_wait");
  if (activation.type !== "activate_wait") throw new Error("activation builder returned wrong type");
  assert.deepEqual(activation.wait_ids, ["wait:a", "wait:z"]);
});

test("TypeScript virtual work query and control fixtures stay exact", () => {
  const occurrencePath = process.env.CYMULE_VIRTUAL_OCCURRENCE_FIXTURE;
  const controlPath = process.env.CYMULE_VIRTUAL_CONTROL_FIXTURE;
  if (occurrencePath === undefined || controlPath === undefined) return;
  const occurrence = JSON.parse(readFileSync(occurrencePath, "utf8"));
  assert.equal(occurrence.occurrence_binding, "binding:worker/fixture@1");
  const command = VirtualWorkControlBuilder.succeed(
    "command:virtual:fixture:success",
    "work:fixture",
    "worker:fixture",
    1,
    1,
    101,
    {
      identity_version: "cymule.artifact/2",
      artifact_id: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
      kind: "example/result",
    },
  );
  assert.deepEqual(command, JSON.parse(readFileSync(controlPath, "utf8")));
  const migrationPath = process.env.CYMULE_VIRTUAL_MIGRATION_FIXTURE;
  if (migrationPath === undefined) return;
  const migrationFixture = JSON.parse(readFileSync(migrationPath, "utf8"));
  assert.deepEqual(
    VirtualWorkControlBuilder.migration(
      "command:migration:fixture-split",
      migrationFixture.plan,
    ),
    migrationFixture,
  );
  const compactionPath = process.env.CYMULE_VIRTUAL_COMPACTION_FIXTURE;
  const rehydrationPath = process.env.CYMULE_VIRTUAL_REHYDRATION_FIXTURE;
  if (compactionPath === undefined || rehydrationPath === undefined) return;
  assert.deepEqual(
    VirtualWorkControlBuilder.compaction(
      "command:compaction:fixture",
      "region:fixture",
      ["virtual:fixture:terminal"],
      "binding:archive/fixture@1",
      "compactor:fixture/1",
    ),
    JSON.parse(readFileSync(compactionPath, "utf8")),
  );
  assert.deepEqual(
    VirtualWorkControlBuilder.rehydration(
      "command:rehydration:fixture",
      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      [occurrence.occurrence_id],
    ),
    JSON.parse(readFileSync(rehydrationPath, "utf8")),
  );
  const claimPath = process.env.CYMULE_VIRTUAL_CLAIM_FIXTURE;
  const renewalPath = process.env.CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE;
  const recoveryPath = process.env.CYMULE_VIRTUAL_RECOVERY_FIXTURE;
  const runWeightPath = process.env.CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE;
  if (
    claimPath === undefined || renewalPath === undefined || recoveryPath === undefined ||
    runWeightPath === undefined
  ) return;
  assert.deepEqual(
    VirtualSchedulingControlBuilder.claim(
      "command:claim:fixture",
      "worker:fixture",
      "slot:worker-fixture:0",
      "binding:worker/fixture@1",
      ["sandbox", "cpu", "cpu"],
      100,
      30,
    ),
    JSON.parse(readFileSync(claimPath, "utf8")),
  );
  assert.deepEqual(
    VirtualSchedulingControlBuilder.renew(
      "command:renew:fixture",
      "work:fixture",
      "worker:fixture",
      1,
      1,
      120,
      30,
    ),
    JSON.parse(readFileSync(renewalPath, "utf8")),
  );
  const recoveryFixture = JSON.parse(readFileSync(recoveryPath, "utf8"));
  assert.deepEqual(
    VirtualSchedulingControlBuilder.recovery(
      "command:recovery:fixture",
      "work:fixture",
      "worker:fixture",
      1,
      2,
      150,
      recoveryFixture.resolution,
    ),
    recoveryFixture,
  );
  assert.deepEqual(
    VirtualSchedulingControlBuilder.runWeight("command:run-weight:fixture", "run:fixture", 3),
    JSON.parse(readFileSync(runWeightPath, "utf8")),
  );
});

test("TypeScript evolution control validates through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_EVOLUTION_CONTROL_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) return;
  const expected = JSON.parse(readFileSync(fixturePath, "utf8"));
  const command = EvolutionControlBuilder.applyGate(
    "command:evolution:fixture:promote",
    {
      gate_id: "gate:fixture:promote",
      decision_id: "rollout:fixture:canary",
      min_target_observations: 3,
      max_target_failures: 0,
      min_equivalent_shadows: 2,
      max_inequivalent_shadows: 0,
    },
    "rollout:fixture:active",
  );
  assert.deepEqual(command, expected);
  assert.deepEqual(new CliEngine(enginePath).verifyEvolutionCommand(command), command);
  const restartPath = process.env.CYMULE_EVOLUTION_RESTART_FIXTURE;
  if (restartPath === undefined) return;
  const restartExpected = JSON.parse(readFileSync(restartPath, "utf8"));
  const restart = EvolutionControlBuilder.restartUnderNewPlan(
    "command:evolution:fixture:restart",
    restartExpected.request,
  );
  assert.deepEqual(restart, restartExpected);
  assert.deepEqual(new CliEngine(enginePath).verifyEvolutionCommand(restart), restart);
});

test("TypeScript unified live evolution validates through the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) return;
  const expected = JSON.parse(readFileSync(fixturePath, "utf8"));
  const command = LiveEvolutionControlBuilder.apply(
    "command:live-evolution:fixture:select",
    "template:review-parent",
    expected.command,
  );
  assert.deepEqual(command, expected);
  assert.deepEqual(new CliEngine(enginePath).verifyLiveEvolutionCommand(command), command);
});

test("TypeScript preserves structured Rust Engine failures", () => {
  const enginePath = process.env.CYMULE_BIN;
  const pluginPath = process.env.CYMULE_TEST_PLUGIN;
  const failurePath = process.env.CYMULE_ENGINE_FAILURE_FIXTURE;
  if (enginePath === undefined || pluginPath === undefined || failurePath === undefined) return;
  const expected = JSON.parse(readFileSync(failurePath, "utf8")).cases as Record<
    string,
    Record<string, string>
  >;
  const candidate = JSON.parse(
    readFileSync(failurePath.replace("engine-failures.json", "cross-language-plan.json"), "utf8"),
  ) as PlanCandidate;
  const engine = new CliEngine(enginePath);
  const invalid = {
    ...candidate,
    ir_version: "cymule.ir/unsupported",
  } as unknown as PlanCandidate;
  assertEngineFailure(() => engine.seal(invalid), expected.invalid_plan_version!);
  const plan = engine.seal(candidate);
  assertEngineFailure(
    () => engine.run(plan, { simulate: "expected_failure" }, pluginPath, "run:ts-expected"),
    expected.expected_plugin_failure!,
  );
  assertEngineFailure(
    () => engine.run(plan, { message: "defect" }, enginePath, "run:ts-defect"),
    expected.plugin_defect!,
  );
  assertEngineFailure(
    () =>
      engine.run(
        plan,
        { message: "substrate" },
        "/cymule-conformance/missing-plugin",
        "run:ts-substrate",
      ),
    expected.substrate_failure!,
  );
});

function assertEngineFailure(operation: () => unknown, expected: Record<string, string>): void {
  try {
    operation();
    assert.fail("operation unexpectedly succeeded");
  } catch (error) {
    assert.ok(error instanceof EngineError);
    assert.equal(error.failure.category, expected.category);
    assert.equal(error.failure.phase, expected.phase);
    assert.equal(error.failure.code, expected.code);
    assert.equal(error.failure.retry_disposition, expected.retry_disposition);
  }
}
