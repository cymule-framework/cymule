import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CliEngine,
  FlowBuilder,
  ResourceBuilder,
  VirtualSchedulingControlBuilder,
  VirtualWorkControlBuilder,
  WaitActivationBuilder,
  type EffectProfile,
} from "../src/index.js";

const profile: EffectProfile = {
  mutation: "mutating",
  dispatch: "on_scope_commit",
  reconciliation: "queryable",
  keyed_idempotency: true,
  irreversible: false,
};

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
  const result = engine.run(plan, input, pluginPath, "run:typescript-e2e");
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
      artifact_id: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      kind: "cymule.wait-activation-result/1",
    },
  );
  assert.deepEqual(activation, JSON.parse(readFileSync(fixturePath, "utf8")));
  assert.deepEqual(new CliEngine(enginePath).verifyWaitActivation(activation), activation);
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
