import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  CliEngine,
  FlowBuilder,
  ResourceBuilder,
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
    .call("call.echo", "test.echo", { kind: "input" }, "echoed")
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
});
