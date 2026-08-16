import assert from "node:assert/strict";
import test from "node:test";

import { CliEngine, FlowBuilder, type EffectProfile } from "../src/index.js";

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
