import assert from "node:assert/strict";
import test from "node:test";

import {
  CliEngine,
  FlowBuilder,
  ResourceBuilder,
  type AgentStreamRecord,
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

test("TypeScript Agent stream is reduced by the Rust engine", () => {
  const enginePath = process.env.CYMULE_BIN;
  if (enginePath === undefined) return;
  const records: AgentStreamRecord[] = [
    {
      record: "opened",
      stream_id: "stream:typescript:1",
      session_id: "session:typescript",
      target: { kind: "message", message_id: "message:typescript:1", role: "agent" },
    },
    {
      record: "chunk",
      stream_id: "stream:typescript:1",
      session_id: "session:typescript",
      chunk: { sequence: 0, content: [{ type: "text", text: "hello" }] },
    },
    {
      record: "finalized",
      stream_id: "stream:typescript:1",
      session_id: "session:typescript",
      content_digest: "57e90e6cb7aff1276e78399ad62cee581909f0d4944c24801d529c141c23a241",
      update: {
        type: "message",
        update_id: "update:stream:typescript:1:finalized",
        message: {
          message_id: "message:typescript:1",
          role: "agent",
          content: [{ type: "text", text: "hello" }],
        },
      },
    },
  ];
  const stream = new CliEngine(enginePath).verifyAgentStream(records);
  assert.equal(stream.state, "finalized");
  assert.equal(stream.chunks.length, 1);
});
