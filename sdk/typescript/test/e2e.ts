import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

import {
  CliEngine,
  DurableEngine,
  DurableControlBuilder,
  EngineError,
  EvolutionControlBuilder,
  FlowBuilder,
  LiveEvolutionControlBuilder,
  processPlugin,
  ResourceBuilder,
  sqliteStore,
  sqliteClock,
  VirtualSchedulingControlBuilder,
  VirtualWorkControlBuilder,
  WaitActivationBuilder,
  directoryStore,
  type EffectProfile,
  type EngineDurableTarget,
  type EngineEvolutionTarget,
  type EngineTransport,
  type EngineMigrationProviderTarget,
  type EnginePluginTarget,
  type EngineShadowProviderTarget,
  type EngineStoreTarget,
  type DurableCommand,
  type EvolutionCommand,
  type EvolutionCommit,
  type ArtifactRecord,
  type ArtifactRef,
  type LiveEvolutionCommand,
  type LiveEvolutionOutcome,
  type PlanCandidate,
  type ResourceHandoffActivation,
  type VirtualCompactionCertificate,
  type VirtualRecoveryCommand,
} from "../src/index.js";

const processTarget = (executable: string, revision?: string): EnginePluginTarget =>
  processPlugin({
    executable,
    arguments: [],
    environment: {
      CYMULE_TEST_EFFECT_LEDGER_PATH: `${executable}.effect-ledger.sqlite3`,
    },
    working_directory: null,
    runtime_closure: {
      "component-runtime": `sha256:${"a".repeat(64)}`,
    },
    timeout_ms: 60_000,
    message_limit: revision === undefined ? 8 * 1024 * 1024 : 16 * 1024 * 1024,
    closure_limit: 64 * 1024 * 1024,
  }, revision);

const durableTargetFor = (
  command: DurableCommand,
  store = directoryStore("unused"),
): EngineDurableTarget => {
  const requiresExecutor = new Set([
    "start_run", "resume_run", "takeover_run", "release_effect", "resolve_effect",
  ]).has(command.type);
  const requiresClock = new Set([
    "start_run", "resume_run", "takeover_run", "release_effect",
  ]).has(command.type);
  return {
    store,
    ...(requiresExecutor ? { executor: processTarget(process.execPath) } : {}),
    ...(requiresClock
      ? {
          clock: sqliteClock(
            join(tmpdir(), "cymule-sdk-preflight-clock.sqlite"),
            "clock:sdk-preflight",
            `sha256:${"f".repeat(64)}`,
          ),
        }
      : {}),
  };
};

const evolutionTargetFor = (
  command: LiveEvolutionCommand,
  store = directoryStore("unused"),
): EngineEvolutionTarget => {
  const operation = command.operation === "apply" ? command.command.operation : undefined;
  const migrationRequest = command.operation === "apply" && command.command.operation === "migrate"
    ? command.command.request
    : undefined;
  const shadowRequest = command.operation === "apply" && command.command.operation === "shadow"
    ? command.command.request
    : undefined;
  return {
    store,
    migration_adapter: operation === "migrate" && migrationRequest !== undefined
      ? {
        adapter_id: migrationRequest.adapter_id,
        adapter_revision: migrationRequest.adapter_revision,
        process: processTarget(process.execPath, migrationRequest.adapter_revision),
      }
      : null,
    shadow_driver: operation === "shadow" && shadowRequest !== undefined
      ? {
        driver_id: shadowRequest.driver_id,
        driver_revision: shadowRequest.driver_revision,
        process: processTarget(process.execPath, shadowRequest.driver_revision),
      }
      : null,
    target_execution_bindings: migrationRequest === undefined
      ? {}
      : {
          [migrationRequest.to_plan]: {
            ...processTarget(process.execPath),
            revision: migrationRequest.adapter_revision,
          },
        },
  };
};

const fixtureExecution = () => ({
  owner: "driver:cross-language",
  clock: {
    clock_version: "cymule.clock-observation/2" as const,
    observation_id: `sha256:${"1".repeat(64)}`,
    source_id: "clock:cross-language",
    source_generation: `sha256:${"2".repeat(64)}`,
    scope: "sha256:7aa23baf73ce53a540a6f3eddaa0175e6be22d751e5d5090d5d77485f58fa74c",
  },
  ttl: 30,
});

const testContentId = (digit: string) => `sha256:${digit.repeat(64)}`;

const evolutionCommit = (
  evolutionId: string,
  command: LiveEvolutionCommand,
  outcome: LiveEvolutionOutcome,
): EvolutionCommit => {
  const consumesSource = command.operation === "apply"
    && (command.command.operation === "migrate"
      || command.command.operation === "restart_under_new_plan");
  return {
    observed_revision: testContentId("8"),
    committed_revision: testContentId("8"),
    receipt: {
      receipt_version: "cymule.evolution-persistence-receipt/4",
      receipt_id: testContentId("7"),
      command: {
        persistence_version: "cymule.evolution-persistence-command/4",
        persistence_id: testContentId("6"),
        evolution_id: evolutionId,
        command,
      },
      parent_current_id: null,
      source_witness_id: consumesSource ? testContentId("5") : null,
      outcome,
      mutations: [],
      mutation_id: testContentId("4"),
    },
  };
};

const artifactRecord = (kind: string, data: Uint8Array): ArtifactRecord => {
  const kindBytes = Buffer.from(kind, "utf8");
  const bytes = Buffer.from(data);
  const kindLength = Buffer.alloc(4);
  kindLength.writeUInt32BE(kindBytes.length);
  const bytesLength = Buffer.alloc(8);
  bytesLength.writeBigUInt64BE(BigInt(bytes.length));
  return {
    reference: {
      identity_version: "cymule.artifact/2",
      artifact_id: `sha256:${createHash("sha256")
        .update(Buffer.from("cymule.artifact/2", "ascii"))
        .update(kindLength)
        .update(kindBytes)
        .update(bytesLength)
        .update(bytes)
        .digest("hex")}`,
      kind,
    },
    bytes: bytes.toString("base64"),
  };
};

const profile: EffectProfile = {
  mutation: "observational",
  dispatch: "eager",
  reconciliation: "queryable",
  keyed_idempotency: true,
  irreversible: false,
};

const delay = (milliseconds: number) => new Promise<void>((resolve) => {
  setTimeout(resolve, milliseconds);
});

async function waitForText(path: string, timeoutMs = 2_000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      const value = readFileSync(path, "utf8").trim();
      if (value.length > 0) return value;
    } catch {
      // The producer may not have created the file yet.
    }
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${path}`);
    await delay(10);
  }
}

async function readPid(path: string): Promise<number> {
  const value = await waitForText(path);
  if (!/^[1-9][0-9]*$/.test(value)) throw new Error(`invalid PID in ${path}: ${value}`);
  const pid = Number(value);
  if (!Number.isSafeInteger(pid)) throw new Error(`invalid PID in ${path}: ${value}`);
  return pid;
}

function assertProcessCannotExecute(pid: number): void {
  const result = spawnSync("ps", ["-o", "state=", "-p", String(pid)], {
    encoding: "utf8",
  });
  if (result.error !== undefined) throw result.error;
  const state = result.stdout.trim().at(0);
  if (result.status !== 0 && state === undefined) return;
  if (result.status !== 0) {
    throw new Error(`failed to inspect Engine descendant ${pid}: ${result.stderr.trim()}`);
  }
  assert.equal(state, "Z", `Engine descendant ${pid} remains executable in state ${state}`);
}

function assertProcessGroupCannotExecute(processGroupId: number): void {
  const result = spawnSync("ps", ["-axo", "pgid=,state="], { encoding: "utf8" });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    throw new Error(`failed to inspect Engine process group ${processGroupId}: ${result.stderr.trim()}`);
  }
  const states = result.stdout
    .split("\n")
    .flatMap((line) => {
      const fields = line.trim().split(/\s+/);
      if (fields.length < 2 || Number(fields[0]) !== processGroupId) return [];
      const state = fields[1]?.at(0);
      return state === undefined ? [] : [state];
    });
  assert.ok(
    states.every((state) => state === "Z"),
    `Engine process group ${processGroupId} remains executable in states ${states.join(",")}`,
  );
}

async function withSuccessEngine<T>(
  response: unknown,
  invoke: (engine: CliEngine) => T | Promise<T>,
  sealedPlanId?: string,
  engineProtocol = "cymule.engine/5",
  echoRequest?: unknown,
): Promise<T> {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "success-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  const configured = JSON.parse(${JSON.stringify(JSON.stringify(response))});
  const sealedPlanId = ${JSON.stringify(sealedPlanId)};
  const configuredRequest = ${echoRequest === undefined
    ? "undefined"
    : `JSON.parse(${JSON.stringify(JSON.stringify(echoRequest))})`};
  const selected = request.type === "seal" && sealedPlanId !== undefined
    ? { type: "sealed", plan: { plan_id: sealedPlanId, candidate: request.candidate } }
    : configured;
  process.stdout.write(JSON.stringify({
    engine_protocol: ${JSON.stringify(engineProtocol)},
    outcome: "success",
    request: configuredRequest === undefined ? request : configuredRequest,
    response: selected,
  }));
});
`,
  );
  chmodSync(executable, 0o700);
  try {
    return await invoke(new CliEngine(executable));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

async function withRawSuccessEngine<T>(
  rawResponse: string,
  invoke: (engine: CliEngine) => T | Promise<T>,
): Promise<T> {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "raw-success-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  process.stdout.write(
    '{"engine_protocol":"cymule.engine/5","outcome":"success","request":'
      + JSON.stringify(request)
      + ',"response":'
      + ${JSON.stringify(rawResponse)}
      + '}',
  );
});
`,
  );
  chmodSync(executable, 0o700);
  try {
    return await invoke(new CliEngine(executable));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

async function withFailureEngine<T>(
  failure: unknown,
  invoke: (engine: CliEngine) => T | Promise<T>,
  engineProtocol = "cymule.engine/5",
): Promise<T> {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "failure-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
process.stdin.resume();
process.stdin.on("end", () => {
  process.stdout.write(${JSON.stringify(JSON.stringify({
      engine_protocol: engineProtocol,
      outcome: "failure",
      error: failure,
    }))});
});
`,
  );
  chmodSync(executable, 0o700);
  try {
    return await invoke(new CliEngine(executable));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("TypeScript classifies mutating Engine response loss as unknown", async () => {
  const engine = new CliEngine(
    join(process.cwd(), "..", "..", "tests", "fixtures", "response-loss-engine"),
  );
  const resume = DurableControlBuilder.resumeRun("run:response-loss", fixtureExecution());
  await assert.rejects(
    () => engine.executeDurable(
      durableTargetFor(resume, directoryStore("/tmp/cymule-response-loss")),
      resume,
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => engine.observeClock(
      sqliteClock("/tmp/cymule-response-loss-clock", "clock:response-loss", `sha256:${"4".repeat(64)}`),
      "run:response-loss",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript classifies post-spawn output overflow as response loss", async () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "oversized-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
process.stdin.resume();
process.stdin.on("end", () => {
  process.stdout.write("x".repeat(17 * 1024 * 1024));
});
`,
  );
  chmodSync(executable, 0o700);
  try {
    const resume = DurableControlBuilder.resumeRun(
      "run:oversized-response",
      fixtureExecution(),
    );
    await assert.rejects(
      () => new CliEngine(executable).executeDurable(
        durableTargetFor(resume),
        resume,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "engine_response_too_large"
        && error.failure.retry_disposition === "reconcile",
    );

    const candidate = new FlowBuilder("missing-engine", {}, {}).finish({ kind: "input" });
    await assert.rejects(
      () => new CliEngine(join(directory, "missing-engine")).seal(candidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "engine_start_failed",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript admits an exact 64 MiB Engine envelope and rejects the next UTF-8 byte", async () => {
  const requestLimit = 64 * 1024 * 1024;
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-request-limit-"));
  const executable = join(directory, "early-failure-engine");
  const started = join(directory, "started");
  const failure = {
    engine_protocol: "cymule.engine/5",
    outcome: "failure",
    error: {
      category: "validation",
      phase: "validate_request",
      code: "synthetic_engine_rejection",
      message: "the synthetic Engine rejected the request without reading stdin",
      retry_disposition: "correct_and_retry",
    },
  };
  writeFileSync(
    executable,
    `#!/bin/sh
printf '%s' started > ${JSON.stringify(started)}
exec 0<&-
printf '%s' '${JSON.stringify(failure)}'
`,
  );
  chmodSync(executable, 0o700);

  const baseCandidate = new FlowBuilder("request-limit", {}, {})
    .finish({ kind: "input" });
  const utf8Prefix = "🧪";
  const envelopeBytes = (candidate: PlanCandidate): number => Buffer.byteLength(
    JSON.stringify({
      engine_protocol: "cymule.engine/5",
      request: { type: "seal", candidate },
    }),
    "utf8",
  );
  const prefixCandidate: PlanCandidate = {
    ...baseCandidate,
    metadata: { padding: utf8Prefix },
  };
  const remainingBytes = requestLimit - envelopeBytes(prefixCandidate);
  assert.ok(remainingBytes > 0);
  const exactPadding = `${utf8Prefix}${"x".repeat(remainingBytes)}`;
  const exactCandidate: PlanCandidate = {
    ...prefixCandidate,
    metadata: { padding: exactPadding },
  };
  const oversizedCandidate: PlanCandidate = {
    ...prefixCandidate,
    metadata: { padding: `${exactPadding}x` },
  };
  assert.equal(envelopeBytes(exactCandidate), requestLimit);
  assert.equal(envelopeBytes(oversizedCandidate), requestLimit + 1);

  try {
    await assert.rejects(
      () => new CliEngine(executable).seal(exactCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "synthetic_engine_rejection"
        && error.failure.retry_disposition === "correct_and_retry",
    );
    assert.equal(existsSync(started), true, "the exact-limit request did not spawn the Engine");

    rmSync(started, { force: true });
    await assert.rejects(
      () => new CliEngine(executable).seal(oversizedCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.phase === "validate_request"
        && error.failure.code === "engine_request_too_large"
        && error.failure.retry_disposition === "correct_and_retry",
    );
    assert.equal(existsSync(started), false, "the oversized request spawned the Engine");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript rejects an early-close Engine success by request mutation authority", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const requestBytes = 4 * 1024 * 1024;
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-early-success-"));
  const readExecutable = join(directory, "read-success-engine");
  const mutationExecutable = join(directory, "mutation-success-engine");
  const writeEarlyCloseEngine = (path: string, body: string): void => {
    const encodedBody = Buffer.from(body, "utf8").toString("base64");
    writeFileSync(
      path,
      `#!/bin/sh
exec 0<&-
exec ${JSON.stringify(process.execPath)} -e 'eval(Buffer.from("${encodedBody}", "base64").toString("utf8"))'
`,
    );
    chmodSync(path, 0o700);
  };

  const readBase = new FlowBuilder("early-read-success", {}, {})
    .finish({ kind: "input" });
  const readPrefix: PlanCandidate = { ...readBase, metadata: { padding: "" } };
  const readEnvelopeBytes = (candidate: PlanCandidate): number => Buffer.byteLength(
    JSON.stringify({
      engine_protocol: "cymule.engine/5",
      request: { type: "seal", candidate },
    }),
    "utf8",
  );
  const readPaddingLength = requestBytes - readEnvelopeBytes(readPrefix);
  assert.ok(readPaddingLength > 0);
  const readCandidate: PlanCandidate = {
    ...readPrefix,
    metadata: { padding: "x".repeat(readPaddingLength) },
  };
  assert.equal(readEnvelopeBytes(readCandidate), requestBytes);
  writeEarlyCloseEngine(
    readExecutable,
    `
const candidate = JSON.parse(${JSON.stringify(JSON.stringify(readPrefix))});
candidate.metadata.padding = "x".repeat(${readPaddingLength});
const request = { type: "seal", candidate };
process.stdout.write(JSON.stringify({
  engine_protocol: "cymule.engine/5",
  outcome: "success",
  request,
  response: {
    type: "sealed",
    plan: { plan_id: ${JSON.stringify(contentId("1"))}, candidate },
  },
}));
`,
  );

  const cancellationId = "cancel:early-success";
  const cancelRunId = "run:early-success";
  const cancelPrefix = DurableControlBuilder.cancelRun(
    cancellationId,
    cancelRunId,
    { padding: "" },
  );
  const cancelTarget = durableTargetFor(cancelPrefix);
  const mutationEnvelopeBytes = (command: DurableCommand): number => Buffer.byteLength(
    JSON.stringify({
      engine_protocol: "cymule.engine/5",
      request: { type: "execute_durable", target: cancelTarget, command },
    }),
    "utf8",
  );
  const mutationPaddingLength = requestBytes - mutationEnvelopeBytes(cancelPrefix);
  assert.ok(mutationPaddingLength > 0);
  const cancel = DurableControlBuilder.cancelRun(
    cancellationId,
    cancelRunId,
    { padding: "x".repeat(mutationPaddingLength) },
  );
  assert.equal(mutationEnvelopeBytes(cancel), requestBytes);
  writeEarlyCloseEngine(
    mutationExecutable,
    `
const target = JSON.parse(${JSON.stringify(JSON.stringify(cancelTarget))});
const command = JSON.parse(${JSON.stringify(JSON.stringify(cancelPrefix))});
command.reason.padding = "x".repeat(${mutationPaddingLength});
const request = { type: "execute_durable", target, command };
process.stdout.write(JSON.stringify({
  engine_protocol: "cymule.engine/5",
  outcome: "success",
  request,
  response: {
    type: "durable_executed",
    response: {
      type: "run_cancelled",
      receipt: {
        receipt_version: "cymule.run-cancellation-receipt/1",
        command: {
          cancellation_id: command.cancellation_id,
          run_id: command.run_id,
          reason: command.reason,
        },
        boundary: {
          status: "cancelled",
          reason: {
            identity_version: "cymule.artifact/2",
            artifact_id: ${JSON.stringify(contentId("2"))},
            kind: "cymule.cancellation-reason/1",
          },
        },
        receipt_id: ${JSON.stringify("3".repeat(64))},
      },
    },
  },
}));
`,
  );

  try {
    await assert.rejects(
      () => new CliEngine(readExecutable).seal(readCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "engine_request_incomplete",
    );
    await assert.rejects(
      () => new CliEngine(mutationExecutable).executeDurable(cancelTarget, cancel),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "engine_request_incomplete"
        && error.failure.retry_disposition === "reconcile",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript classifies local timeout recovery by mutation authority", async () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "timeout-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
process.stdin.resume();
process.stdin.on("end", () => {
  setTimeout(() => process.stdout.write("late"), 1_000);
});
`,
  );
  chmodSync(executable, 0o700);
  const engine = new CliEngine(executable, { timeoutMs: 20 });
  try {
    const candidate = new FlowBuilder("timeout-read", {}, {}).finish({ kind: "input" });
    await assert.rejects(
      () => engine.seal(candidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "timed_out"
        && error.failure.retry_disposition === "retry_same_request",
    );
    await assert.rejects(
      () => {
        const resume = DurableControlBuilder.resumeRun(
          "run:timeout-mutation",
          fixtureExecution(),
        );
        return engine.executeDurable(durableTargetFor(resume), resume);
      },
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.retry_disposition === "reconcile",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript cancellation terminates descendants after the direct child exits", async (context) => {
  if (process.platform === "win32") {
    context.skip("POSIX process-group conformance is unavailable on Windows");
    return;
  }
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "descendant-pipe-engine");
  const pidPath = join(directory, "descendant.pid");
  const readyPath = join(directory, "descendant.ready");
  const markerPath = join(directory, "late-marker");
  writeFileSync(
    executable,
    `#!/bin/sh
printf '%s\n' "$$" >${JSON.stringify(pidPath)}
parent_pid="$$"
(
  trap '' TERM
  while kill -0 "$parent_pid" 2>/dev/null; do /bin/sleep 0.01; done
  printf 'ready' >${JSON.stringify(readyPath)}
  /bin/sleep 5
  printf 'late' >${JSON.stringify(markerPath)}
  /bin/sleep 30
) &
exit 0
`,
  );
  chmodSync(executable, 0o700);
  const cancelled = new AbortController();
  try {
    const request = new CliEngine(executable, {
      timeoutMs: 5_000,
      signal: cancelled.signal,
    }).seal(
      new FlowBuilder("descendant-pipe", {}, {}).finish({ kind: "input" }),
    );
    const rejection = assert.rejects(
      request,
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "cancelled"
        && error.failure.retry_disposition === "never",
    );
    const processGroupId = await readPid(pidPath);
    await waitForText(readyPath);
    cancelled.abort();
    await rejection;
    assertProcessGroupCannotExecute(processGroupId);
    assert.equal(existsSync(markerPath), false, "terminated descendant wrote a late marker");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript cancellation kills the in-flight Engine process group", async (context) => {
  if (process.platform === "win32") {
    context.skip("POSIX process-group conformance is unavailable on Windows");
    return;
  }
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "cancellable-engine");
  const descendantScript = join(directory, "descendant.js");
  const pidPath = join(directory, "descendant.pid");
  const markerPath = join(directory, "late-marker");
  writeFileSync(
    descendantScript,
    `const { writeFileSync } = require("node:fs");
process.on("SIGTERM", () => {});
writeFileSync(${JSON.stringify(pidPath)}, String(process.pid));
setTimeout(() => writeFileSync(${JSON.stringify(markerPath)}, "late"), 5_000);
setInterval(() => {}, 30_000);
`,
  );
  writeFileSync(
    executable,
    `#!/usr/bin/env node
const { spawn } = require("node:child_process");
spawn(process.execPath, [${JSON.stringify(descendantScript)}], {
  stdio: ["ignore", "inherit", "inherit"],
});
setInterval(() => {}, 30_000);
`,
  );
  chmodSync(executable, 0o700);
  const cancelled = new AbortController();
  try {
    const request = new CliEngine(executable, {
      timeoutMs: 5_000,
      signal: cancelled.signal,
    }).seal(new FlowBuilder("cancel-process-group", {}, {}).finish({ kind: "input" }));
    const rejection = assert.rejects(
      request,
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "cancelled"
        && error.failure.retry_disposition === "never",
    );
    const descendant = await readPid(pidPath);
    cancelled.abort();
    await rejection;
    assertProcessCannotExecute(descendant);
    assert.equal(existsSync(markerPath), false, "terminated descendant wrote a late marker");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript cancellation prevents detached-pipe descendant side effects", async (context) => {
  if (process.platform === "win32") {
    context.skip("POSIX process-group conformance is unavailable on Windows");
    return;
  }
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "cancellable-engine");
  const descendantScript = join(directory, "detached-pipe-descendant.js");
  const pidPath = join(directory, "descendant.pid");
  const markerPath = join(directory, "late-marker");
  writeFileSync(
    descendantScript,
    `const { writeFileSync } = require("node:fs");
process.on("SIGTERM", () => {});
writeFileSync(${JSON.stringify(pidPath)}, String(process.pid));
setTimeout(() => writeFileSync(${JSON.stringify(markerPath)}, "late"), 100);
setInterval(() => {}, 30_000);
`,
  );
  writeFileSync(
    executable,
    `#!/usr/bin/env node
const { spawn } = require("node:child_process");
spawn(process.execPath, [${JSON.stringify(descendantScript)}], {
  stdio: "ignore",
});
setInterval(() => {}, 30_000);
`,
  );
  chmodSync(executable, 0o700);
  const cancelled = new AbortController();
  try {
    const request = new CliEngine(executable, {
      timeoutMs: 5_000,
      signal: cancelled.signal,
    }).seal(new FlowBuilder("cancel-detached-pipe", {}, {}).finish({ kind: "input" }));
    const rejection = assert.rejects(
      request,
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "cancelled"
        && error.failure.retry_disposition === "never",
    );
    const descendant = await readPid(pidPath);
    cancelled.abort();
    await rejection;
    assertProcessCannotExecute(descendant);
    assert.equal(existsSync(markerPath), false, "terminated descendant wrote a late marker");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript natural close wins a simultaneous cancellation cleanup", async () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "successful-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  process.stdout.write(JSON.stringify({
    engine_protocol: "cymule.engine/5",
    outcome: "success",
    request,
    response: {
      type: "sealed",
      plan: { plan_id: "sha256:${"7".repeat(64)}", candidate: request.candidate },
    },
  }));
});
`,
  );
  chmodSync(executable, 0o700);
  let listener: EventListener | undefined;
  const racingSignal = {
    aborted: false,
    addEventListener(type: string, candidate: EventListener): void {
      if (type === "abort") listener = candidate;
    },
    removeEventListener(type: string, candidate: EventListener): void {
      if (type !== "abort" || listener !== candidate) return;
      listener(new Event("abort"));
      listener = undefined;
    },
  } as unknown as AbortSignal;
  try {
    const candidate = new FlowBuilder("natural-close-race", {}, {}).finish({ kind: "input" });
    const plan = await new CliEngine(executable, { signal: racingSignal }).seal(candidate);
    assert.equal(plan.plan_id, `sha256:${"7".repeat(64)}`);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript rejects non-UTF-8 Engine stdout", async () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "invalid-utf8-engine");
  writeFileSync(
    executable,
    `#!/usr/bin/env node
process.stdin.resume();
process.stdin.on("end", () => process.stdout.write(Buffer.from([0xff])));
`,
  );
  chmodSync(executable, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(executable).seal(
        new FlowBuilder("invalid-utf8", {}, {}).finish({ kind: "input" }),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript durable controls accept the maximum exact integer", async () => {
  const command = DurableControlBuilder.takeoverRun(
    "run:max-safe-fence",
    Number.MAX_SAFE_INTEGER,
    { ...fixtureExecution(), ttl: Number.MAX_SAFE_INTEGER },
  );
  assert.equal(command.type, "takeover_run");
  if (command.type !== "takeover_run") throw new Error("takeover builder returned wrong type");
  assert.equal(command.expected_fence, Number.MAX_SAFE_INTEGER);
  assert.doesNotThrow(() => DurableControlBuilder.resumeRun(
    "run:unicode-owner",
    { ...fixtureExecution(), owner: "é".repeat(512) },
  ));
  assert.throws(() => DurableControlBuilder.resumeRun(
    "run:c1-owner",
    { ...fixtureExecution(), owner: "driver:\u0085forged" },
  ));
  assert.throws(() => DurableControlBuilder.resumeRun(
    "run:long-owner",
    { ...fixtureExecution(), owner: "é".repeat(513) },
  ));
});

test("official Store constructors pin terminal physical generations", () => {
  assert.deepEqual(directoryStore("store"), {
    provider: "cymule.directory-store/5",
    location: "store",
  });
  assert.deepEqual(sqliteStore("store.sqlite", "domain"), {
    provider: "cymule.sqlite-store/6",
    location: "store.sqlite",
    domain: "domain",
  });
});

test("TypeScript uses the Unicode-scalar Run identity contract end to end", async () => {
  const runId = "🧪".repeat(512);
  const tooLong = `${runId}🧪`;
  const planId = `sha256:${"1".repeat(64)}`;
  const binding = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: `sha256:${"2".repeat(64)}`,
    kind: "cymule.execution-binding/2",
  };
  const artifact = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: `sha256:${"3".repeat(64)}`,
    kind: "test/value",
  };
  const candidate = new FlowBuilder("unicode-run", {}, {}).finish({ kind: "input" });
  assert.equal(candidate.ir_version, "cymule.ir/3");
  const plan = { plan_id: planId, candidate };

  const resume = DurableControlBuilder.resumeRun(runId, fixtureExecution());
  assert.equal(resume.type, "resume_run");
  if (resume.type !== "resume_run") throw new Error("resume builder returned wrong type");
  assert.equal(resume.run_id, runId);
  assert.equal(
    VirtualSchedulingControlBuilder.runWeight("command:unicode-run", runId, 1).run_id,
    runId,
  );
  assert.equal(
    ResourceBuilder.handoff(
      "transfer:unicode-run",
      { run_id: runId, occurrence_id: "occurrence:unicode-run", result: artifact },
      "run:unicode-consumer",
      "slot:unicode-run",
      artifact,
    ).to_run,
    "run:unicode-consumer",
  );
  assert.equal(
    ResourceBuilder.handoff(
      "transfer:version",
      { run_id: runId, occurrence_id: "occurrence:version", result: artifact },
      "run:unicode-consumer",
      "slot:version",
      artifact,
    ).handoff_version,
    "cymule.resource-handoff/5",
  );
  const handoffActivation: ResourceHandoffActivation = {
    activation_version: "cymule.resource-handoff-activation/3",
    activation_id: `sha256:${"4".repeat(64)}`,
    transfer_id: "transfer:version",
    to_run: "run:unicode-consumer",
    wait_id: "wait:resource",
    result: artifact,
  };
  assert.deepEqual(Object.keys(handoffActivation).sort(), [
    "activation_id",
    "activation_version",
    "result",
    "to_run",
    "transfer_id",
    "wait_id",
  ]);
  assert.throws(() => ResourceBuilder.handoff(
    "transfer:self",
    { run_id: runId, occurrence_id: "occurrence:self", result: artifact },
    runId,
    "slot:self",
    artifact,
  ));
  assert.throws(() => ResourceBuilder.handoff(
    "transfer:mismatch",
    { run_id: runId, occurrence_id: "occurrence:mismatch", result: artifact },
    "run:unicode-consumer",
    "slot:mismatch",
    { ...artifact, artifact_id: `sha256:${"9".repeat(64)}` },
  ));
  const migrationPlan = {
    targets: [{ run_id: runId }],
  } as unknown as Parameters<typeof VirtualWorkControlBuilder.migration>[1];
  assert.equal(
    VirtualWorkControlBuilder.migration("command:unicode-migration", migrationPlan)
      .plan.targets[0]?.run_id,
    runId,
  );

  const execution = await withSuccessEngine(
    {
      type: "execution_boundary",
      execution: {
        status: "completed",
        result: {
          run_id: runId,
          plan_id: planId,
          value: null,
          projection_digest: "4".repeat(64),
          precondition_token: `pre:1:sha256:${"7".repeat(64)}`,
          effects: [],
        },
      },
    },
    (engine) => engine.run(plan, null, processTarget(process.execPath), runId),
  );
  assert.equal(execution.status, "completed");

  const runningCurrent = {
    run_id: runId,
    plan_id: planId,
    execution_binding: binding,
    continuation_status: "running",
    epoch: 1,
    execution_fence: 1,
    result: null,
    execution_status: { status: "active" },
    world_settlement: "settled",
  };
  const readCurrent = (current: unknown) => withSuccessEngine(
    {
      type: "durable_executed",
      response: {
        type: "run_current",
        observed_revision: `sha256:${"5".repeat(64)}`,
        source_root: "6".repeat(64),
        current,
      },
    },
    (engine) => new DurableEngine(
      directoryStore("unused"),
      undefined,
      undefined,
      engine,
    ).runCurrent(runId, null),
  );
  assert.deepEqual((await readCurrent(runningCurrent)).current, runningCurrent);
  for (const forgedBinding of [
    { ...binding, artifact_id: `sha256:${"A".repeat(64)}` },
    { ...binding, kind: "cymule.component-output/1" },
  ]) {
    await assert.rejects(
      () => readCurrent({ ...runningCurrent, execution_binding: forgedBinding }),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }

  for (const invalid of [tooLong, "run:\u0085forged", "\ud800"]) {
    assert.throws(() => DurableControlBuilder.resumeRun(invalid, fixtureExecution()));
    assert.throws(() => VirtualSchedulingControlBuilder.runWeight(
      "command:invalid-run",
      invalid,
      1,
    ));
    assert.throws(() => ResourceBuilder.handoff(
      "transfer:invalid-run",
      { run_id: invalid, occurrence_id: "occurrence:invalid-run", result: artifact },
      runId,
      "slot:invalid-run",
      artifact,
    ));
    await assert.rejects(
      () => new CliEngine("missing-engine").run(
        plan,
        null,
        processTarget(process.execPath),
        invalid,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "invalid_run_identity"
        && error.failure.retry_disposition === "correct_and_retry",
    );
  }
  const invalidMigration = {
    targets: [{ run_id: tooLong }],
  } as unknown as Parameters<typeof VirtualWorkControlBuilder.migration>[1];
  assert.throws(() => VirtualWorkControlBuilder.migration(
    "command:invalid-migration",
    invalidMigration,
  ));
});

test("TypeScript uses the 256-scalar M4 identity contract", async () => {
  const identity = "🧭".repeat(256);
  const definition: PlanCandidate["definitions"][number] = {
    id: "main",
    input_schema: {},
    output_schema: {},
    body: { steps: [], result: { kind: "input" } },
  };
  const command = LiveEvolutionControlBuilder.publishDefinition(
    identity,
    identity,
    definition,
    [],
  );
  const evolutionId = "evolution:m4-unicode";
  const commit = evolutionCommit(evolutionId, command, {
      result: "definition_published",
      revision: {
        revision_version: "cymule.subflow-revision/2",
        revision_id: `sha256:${"7".repeat(64)}`,
        logical_ref: identity,
        sequence: 1,
        definition,
        references: [],
      },
    });
  assert.deepEqual(
    await withSuccessEngine(
      { type: "live_evolution_executed", commit },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        command,
      ),
    ),
    commit,
  );

  const invalid = [
    LiveEvolutionControlBuilder.publishDefinition(`${identity}🧭`, identity, definition, []),
    LiveEvolutionControlBuilder.publishDefinition("command:m4-long-ref", `${identity}🧭`, definition, []),
    LiveEvolutionControlBuilder.publishDefinition("command:m4-control", "module:\u0085forged", definition, []),
  ];
  for (const rejected of invalid) {
    await assert.rejects(
      () => new CliEngine("missing-engine").executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "evolution:m4-invalid",
        rejected,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "evolution_request_validation_failed"
        && error.failure.retry_disposition === "correct_and_retry",
    );
  }
});

test("TypeScript rejects malformed wait activation and durable command echoes", async () => {
  const artifact = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: `sha256:${"1".repeat(64)}`,
    kind: "cymule.wait-result/1",
  };
  const activation = {
    activation_version: "cymule.wait-activation/2",
    activation_id: "activation:wire",
    source: { kind: "signal", key: "signal:wire" },
    wait_ids: [`sha256:${"a".repeat(64)}`],
    result: artifact,
  };
  const invalidActivations = [
    { ...activation, wait_ids: [] },
    { ...activation, wait_ids: [`sha256:${"a".repeat(64)}`, `sha256:${"a".repeat(64)}`] },
    { ...activation, wait_ids: [`sha256:${"b".repeat(64)}`, `sha256:${"a".repeat(64)}`] },
    { ...activation, source: { kind: "signal", key: "" } },
    { ...activation, result: { ...artifact, kind: "cymule.component-output/1" } },
  ];
  for (const invalid of invalidActivations) {
    await assert.rejects(
      () => withSuccessEngine(
        { type: "verified_wait_activation", activation: invalid },
        (engine) => engine.verifyWaitActivation(invalid as never),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }

  const oversizedTargets = Array.from(
    { length: 4_097 },
    (_, index) => `sha256:${index.toString(16).padStart(64, "0")}`,
  );
  const runIndexQuery = DurableControlBuilder.runIndexPage({
    expected_revision: null,
    cursor: null,
    limit: 16,
    max_canonical_bytes: 1024 * 1024,
  });
  assert.throws(
    () => DurableControlBuilder.releaseEffect("effect:not-content-addressed", fixtureExecution()),
    /content ID/,
  );
  const invalidCommands = [
    {
      ...runIndexQuery,
      control_version: "cymule.durable-control/3",
    },
    { ...DurableControlBuilder.resumeRun("run:wire", fixtureExecution()), run_id: "" },
    { ...runIndexQuery, limit: 0 },
    {
      ...DurableControlBuilder.releaseEffect(
        `sha256:${"2".repeat(64)}`,
        fixtureExecution(),
      ),
      intent_id: "",
    },
    {
      ...DurableControlBuilder.releaseEffect(
        `sha256:${"2".repeat(64)}`,
        fixtureExecution(),
      ),
      intent_id: "effect:not-content-addressed",
    },
    {
      ...DurableControlBuilder.cancelRun("cancel:wire", "run:wire", null),
      cancellation_id: "",
    },
    {
      type: "activate_wait",
      control_version: "cymule.durable-control/4",
      activation_id: "activation:wire",
      source: { kind: "signal", key: "signal:wire" },
      wait_ids: [],
      value: null,
    },
    {
      type: "activate_wait",
      control_version: "cymule.durable-control/4",
      activation_id: "activation:wire",
      source: { kind: "timer", timer_id: "" },
      wait_ids: ["wait:a"],
      value: null,
    },
    {
      type: "activate_wait",
      control_version: "cymule.durable-control/4",
      activation_id: "activation:wire",
      source: { kind: "signal", key: "signal:wire" },
      wait_ids: oversizedTargets,
      value: null,
    },
  ];
  for (const invalid of invalidCommands) {
    await assert.rejects(
      () => withSuccessEngine(
        { type: "verified_durable_command", command: invalid },
        (engine) => engine.verifyDurableCommand(invalid as never),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }
});

test("TypeScript Engine ingress rejects duplicate JSON object members", async () => {
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "duplicate-engine");
  writeFileSync(
    executable,
    `#!/bin/sh
cat >/dev/null
printf '%s' '{"engine_protocol":"cymule.engine/5","outcome":"success","response":{"type":"sealed","type":"verified"}}'
`,
  );
  chmodSync(executable, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(executable).seal({} as PlanCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "invalid_engine_response"
        && error.failure.message.includes("duplicate JSON object"),
    );
    await assert.rejects(
      () => {
        const resume = DurableControlBuilder.resumeRun(
          "run:duplicate-response",
          fixtureExecution(),
        );
        return new CliEngine(executable).executeDurable(durableTargetFor(resume), resume);
      },
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript Engine success and nested unions are closed", async () => {
  const runCandidate = new FlowBuilder("nested-union-run", {}, {})
    .finish({ kind: "input" });
  const runPlan = { plan_id: `sha256:${"0".repeat(64)}`, candidate: runCandidate };
  const artifact = {
    identity_version: "cymule.artifact/2",
    artifact_id: `sha256:${"1".repeat(64)}`,
    kind: "test/value",
  };
  const binding = {
    identity_version: "cymule.artifact/2",
    artifact_id: `sha256:${"2".repeat(64)}`,
    kind: "cymule.execution-binding/2",
  };
  const sourceContinuation = {
    continuation_version: "cymule.continuation-state/1" as const,
    run_id: "run:test",
    plan_id: `sha256:${"3".repeat(64)}`,
    binding_context: binding.artifact_id,
    frames: [{
      definition_id: "main",
      invocation_id: "main",
      invocation_path: [],
      scope_id: "scope:root",
      input: artifact,
      region_path: [],
      next_step: 0,
      locals: {},
    }],
    state: artifact,
    wait_set: [],
    scope_stack: ["scope:root"],
    epoch: 0,
    execution_fence: 0,
    execution_claim: null,
    status: "ready",
  };
  const migrationRequest = {
    migration_id: "migration:test",
    run_id: "run:test",
    from_plan: sourceContinuation.plan_id,
    to_plan: `sha256:${"4".repeat(64)}`,
    plan_edge_id: `sha256:${"5".repeat(64)}`,
    compatibility_id: `sha256:${"6".repeat(64)}`,
    expected_source_epoch: 0,
    adapter_id: "adapter:test",
    adapter_revision: `sha256:${"8".repeat(64)}`,
  };
  const migrationReceipt = {
    request: migrationRequest,
    source_witness_id: `sha256:${"7".repeat(64)}`,
    source_binding: binding,
    target_binding: binding,
    source_execution_fence: 0,
    target_epoch: 1,
    adapter_id: migrationRequest.adapter_id,
    adapter_revision: migrationRequest.adapter_revision,
    from_schema: "schema:source",
    to_schema: "schema:target",
    output_state: artifact,
    target_continuation: {
      ...sourceContinuation,
      plan_id: migrationRequest.to_plan,
      epoch: 1,
      status: "ready",
    },
    evidence: artifact,
  };
  const durableCurrent = {
    run_id: "run:test",
    plan_id: migrationRequest.from_plan,
    execution_binding: binding,
    continuation_status: "ready",
    epoch: 0,
    execution_fence: 0,
    result: null,
    execution_status: { status: "active" },
    world_settlement: "settled",
  };
  const cases = [
    {
      response: { type: "sealed", plan: null },
      invoke: (engine: CliEngine) => engine.seal({} as never),
    },
    {
      response: { type: "verified_evolution_command", command: {
        control_version: "cymule.evolution-control/5",
        command_id: "command:test",
        operation: "future_operation",
      } },
      invoke: (engine: CliEngine) => engine.verifyEvolutionCommand({} as never),
    },
    {
      response: { type: "durable_executed", response: {
        type: "run_current",
        observed_revision: `sha256:${"9".repeat(64)}`,
        source_root: "8".repeat(64),
        current: { ...durableCurrent, continuation_status: "future" },
      } },
      invoke: (engine: CliEngine) => engine.executeDurable(
        { store: directoryStore("unused") },
        DurableControlBuilder.runCurrent("run:test", null),
      ),
    },
    {
      response: { type: "durable_executed", response: {
        type: "run_current",
        observed_revision: `sha256:${"9".repeat(64)}`,
        source_root: "8".repeat(64),
        current: { ...durableCurrent, execution_status: { status: "completed" } },
      } },
      invoke: (engine: CliEngine) => engine.executeDurable(
        { store: directoryStore("unused") },
        DurableControlBuilder.runCurrent("run:test", null),
      ),
    },
    {
      response: { type: "execution_boundary", execution: {
        status: "completed", result: {}, suspension: {},
      } },
      invoke: (engine: CliEngine) => engine.run(
        runPlan,
        null,
        processTarget(process.execPath),
        "run:test",
      ),
    },
    {
      response: { type: "execution_boundary", execution: {
        status: "suspended",
        suspension: {
          run_id: "run:test",
          plan_id: "sha256:test",
          definition_id: "main",
          invocation_id: "main",
          site_id: "wait:test",
          wait: { kind: "future", unexpected: true },
          result_bind: null,
        },
      } },
      invoke: (engine: CliEngine) => engine.run(
        runPlan,
        null,
        processTarget(process.execPath),
        "run:test",
      ),
    },
    {
      response: { type: "verified_evolution_command", command: {
        control_version: "cymule.evolution-control/5",
        command_id: "command:test",
        operation: "migrate",
        request: { unexpected: true },
      } },
      invoke: (engine: CliEngine) => engine.verifyEvolutionCommand({} as never),
    },
    {
      response: { type: "verified_live_evolution_command", command: {
        control_version: "cymule.live-evolution-control/6",
        command_id: "command:unsafe-safe-point",
        operation: "apply",
        template_id: "template:test",
        command: {
          control_version: "cymule.evolution-control/5",
          command_id: "command:select",
          operation: "select_occurrence",
          occurrence_id: "occurrence:test",
        },
        safe_point: {
          safe_point_version: "cymule.migration-safe-point/2",
          safe_point_id: `sha256:${"1".repeat(64)}`,
          domain_revision: "2".repeat(64),
          run_id: "run:test",
          plan_id: `sha256:${"3".repeat(64)}`,
          binding_context: `sha256:${"4".repeat(64)}`,
          epoch: 9_007_199_254_740_992,
          state: null,
          continuation_digest: "5".repeat(64),
        },
      } },
      invoke: (engine: CliEngine) => engine.verifyLiveEvolutionCommand({} as never),
    },
  ];
  for (const [index, entry] of cases.entries()) {
    const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
    const executable = join(directory, `closed-engine-${index}`);
    writeFileSync(
      executable,
      `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  const response = JSON.parse(${JSON.stringify(JSON.stringify(entry.response))});
  process.stdout.write(JSON.stringify({
    engine_protocol: "cymule.engine/5",
    outcome: "success",
    request,
    response,
  }));
});
`,
    );
    chmodSync(executable, 0o700);
    try {
      await assert.rejects(
        () => entry.invoke(new CliEngine(executable)),
        (error: unknown) => error instanceof EngineError
          && error.failure.code === "invalid_engine_response",
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
});

test("TypeScript Artifact records use bounded canonical Base64", async () => {
  const definition = new FlowBuilder("artifact-base64", {}, {})
    .finish({ kind: "input" }).definitions[0]!;
  const evidence = artifactRecord(
    "cymule.evolution-evidence/1",
    Buffer.from("publication evidence", "utf8"),
  );
  assert.equal(evidence.bytes, Buffer.from("publication evidence", "utf8").toString("base64"));
  LiveEvolutionControlBuilder.publishAndRelink("command:artifact-base64", {
    logical_ref: "definition:artifact-base64",
    definition,
    references: [],
    evidence,
    mode: { mode: "active" },
  });
  for (const bytes of [[112], "YQ", "A".repeat(11_184_816)]) {
    const invalid = LiveEvolutionControlBuilder.publishAndRelink("command:artifact-base64-invalid", {
        logical_ref: "definition:artifact-base64",
        definition,
        references: [],
        evidence: { ...evidence, bytes } as unknown as ArtifactRecord,
        mode: { mode: "active" },
      });
    await assert.rejects(
      () => new CliEngine("missing-engine").executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "evolution:artifact-base64",
        invalid,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "evolution_request_validation_failed",
    );
  }
});

test("TypeScript live-evolution successes are recursively closed and self-consistent", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const digest = (digit: string) => digit.repeat(64);
  const migrationRunId = "🦀".repeat(512);
  const artifact = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("1"),
    kind: "cymule.evolution-evidence/1",
  };
  const publicationEvidence = artifactRecord(
    "cymule.evolution-evidence/1",
    Buffer.from("publication evidence", "utf8"),
  );
  const outputState = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("2"),
    kind: "cymule.migrated-state/1",
  };
  const sourceBinding = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("3"),
    kind: "cymule.execution-binding/2",
  };
  const targetBinding = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("4"),
    kind: "cymule.execution-binding/2",
  };
  const definition: PlanCandidate["definitions"][number] = {
    id: "main",
    input_schema: { type: "object" },
    output_schema: true,
    body: { steps: [], result: { kind: "input" } },
  };
  const candidate: PlanCandidate = {
    ir_version: "cymule.ir/3",
    name: "live-evolution-fixture",
    entry: "main",
    components: [],
    effects: [],
    definitions: [definition],
    metadata: {},
  };
  const sourcePlan = contentId("5");
  const targetPlan = contentId("6");
  const sealedPlan = { plan_id: targetPlan, candidate };
  const revision = {
    revision_version: "cymule.subflow-revision/2",
    revision_id: contentId("7"),
    logical_ref: "module:fixture",
    sequence: 1,
    definition,
    references: [],
  };
  const templateReference = {
    logical_ref: "module:dependency",
    local_definition: "dependency",
    input_schema: {},
    output_schema: {},
    strategy: { strategy: "latest_compatible" as const },
  };
  const publicationUpdates = [
    {
      template_id: "template:a",
      previous_plan_id: sourcePlan,
      current_plan_id: targetPlan,
      decision_id: contentId("8"),
      advanced: true,
    },
    {
      template_id: "template:b",
      previous_plan_id: targetPlan,
      current_plan_id: targetPlan,
      decision_id: null,
      advanced: false,
    },
  ];
  const patchOperations = [
    { kind: "add", target: "component:a", before: null, after: digest("1") },
    { kind: "replace", target: "definition:b", before: digest("2"), after: digest("3") },
    { kind: "remove", target: "effect:c", before: digest("4"), after: null },
  ];
  const sourceEpoch = 4;
  const sourceContinuation = {
    continuation_version: "cymule.continuation-state/1" as const,
    run_id: migrationRunId,
    plan_id: sourcePlan,
    binding_context: sourceBinding.artifact_id,
    frames: [{
      definition_id: "main",
      invocation_id: "invocation:main",
      invocation_path: [],
      scope_id: "scope:root",
      input: artifact,
      region_path: [],
      next_step: 0,
      locals: {},
    }],
    state: artifact,
    wait_set: [],
    scope_stack: ["scope:root"],
    epoch: sourceEpoch,
    execution_fence: 9,
    execution_claim: null,
    status: "ready" as const,
  };
  const migrationRequest = {
    migration_id: "migration:fixture",
    run_id: sourceContinuation.run_id,
    from_plan: sourcePlan,
    to_plan: targetPlan,
    plan_edge_id: contentId("9"),
    compatibility_id: contentId("a"),
    expected_source_epoch: sourceEpoch,
    adapter_id: "adapter:fixture",
    adapter_revision: contentId("b"),
  };
  const targetContinuation = {
    ...sourceContinuation,
    plan_id: targetPlan,
    binding_context: targetBinding.artifact_id,
    state: outputState,
    epoch: sourceEpoch + 1,
  };
  const restartRequest = {
    restart_id: "restart:fixture",
    replacement_run: "🚀".repeat(512),
    run_id: sourceContinuation.run_id,
    from_plan: sourceContinuation.plan_id,
    expected_source_epoch: sourceContinuation.epoch,
    to_plan: targetPlan,
    input: artifact,
    evidence: artifact,
  };
  const shadowRequest = {
    comparison_id: "comparison:fixture",
    decision_id: "decision:source",
    subject: "run:subject",
    primary_plan: sourcePlan,
    shadow_plan: targetPlan,
    driver_id: "driver:fixture",
    driver_revision: contentId("c"),
    input: artifact,
    comparison_policy: "policy:exact",
  };
  const gate = {
    gate_id: "gate:fixture",
    decision_id: "decision:source",
    min_target_observations: 2,
    max_target_failures: 0,
    min_equivalent_shadows: 1,
    max_inequivalent_shadows: 0,
  };
  const validResponses: Record<string, LiveEvolutionOutcome> = {
    definition_published: { result: "definition_published", revision },
    template_registered: {
      result: "template_registered",
      linked: {
        template_id: "template:fixture",
        plan: sealedPlan,
        resolved_revisions: { [templateReference.logical_ref]: contentId("0") },
      },
    },
    publication_applied: {
      result: "publication_applied",
      receipt: { revision, updates: publicationUpdates },
    },
    patch_applied: {
      result: "patch_applied",
      edge: {
        edge_id: contentId("d"),
        from_plan: sourcePlan,
        to_plan: targetPlan,
        operations: patchOperations,
      },
    },
    applied: { result: "applied" },
    occurrence_selected: {
      result: "occurrence_selected",
      pin: {
        occurrence_id: "occurrence:fixture",
        template_id: "template:fixture",
        decision_id: "decision:source",
        plan_id: targetPlan,
        execution_binding: targetBinding,
        selection_id: "selection:fixture",
      },
    },
    migrated: {
      result: "migrated",
      receipt: {
        request: migrationRequest,
        source_witness_id: contentId("7"),
        source_binding: sourceBinding,
        target_binding: targetBinding,
        source_execution_fence: sourceContinuation.execution_fence,
        target_epoch: sourceEpoch + 1,
        adapter_id: migrationRequest.adapter_id,
        adapter_revision: migrationRequest.adapter_revision,
        from_schema: "schema:source",
        to_schema: "schema:target",
        output_state: outputState,
        target_continuation: targetContinuation,
        evidence: artifact,
      },
    },
    restart_authorized: {
      result: "restart_authorized",
      receipt: {
        request: restartRequest,
        source_witness_id: contentId("8"),
        target_plan: sealedPlan,
      },
    },
    shadow_recorded: {
      result: "shadow_recorded",
      comparison: {
        comparison_id: shadowRequest.comparison_id,
        subject: shadowRequest.subject,
        decision_id: shadowRequest.decision_id,
        primary_plan: shadowRequest.primary_plan,
        shadow_plan: shadowRequest.shadow_plan,
        driver_id: shadowRequest.driver_id,
        driver_revision: shadowRequest.driver_revision,
        comparison_policy: shadowRequest.comparison_policy,
        primary_digest: digest("5"),
        shadow_digest: digest("6"),
        equivalent: true,
        evidence: artifact,
      },
    },
    gate_applied: {
      result: "gate_applied",
      transition: {
        transition_id: contentId("e"),
        from_decision: gate.decision_id,
        to_decision: "decision:target",
        evaluation: {
          evaluation_id: contentId("f"),
          gate,
          target_observations: 2,
          target_failures: 0,
          equivalent_shadows: 1,
          inequivalent_shadows: 0,
          outcome: "promote",
          evidence_ids: ["evidence:1", "evidence:2", "evidence:3"],
        },
      },
    },
  };
  const commands: Record<string, LiveEvolutionCommand> = {
    definition_published: LiveEvolutionControlBuilder.publishDefinition(
      "command:definition",
      revision.logical_ref,
      definition,
      [],
    ),
    template_registered: LiveEvolutionControlBuilder.registerTemplate(
      "command:template",
      { template_id: "template:fixture", candidate, references: [templateReference] },
    ),
    publication_applied: LiveEvolutionControlBuilder.publishAndRelink(
      "command:publication",
      {
        logical_ref: revision.logical_ref,
        definition,
        references: [],
        evidence: publicationEvidence,
        mode: { mode: "active" },
      },
    ),
    patch_applied: LiveEvolutionControlBuilder.apply(
      "command:patch",
      "template:fixture",
      EvolutionControlBuilder.applyPatch("command:edge", {
        from_plan: sourcePlan,
        target: candidate,
        operations: patchOperations,
        evidence: artifact,
      }),
    ),
    applied: LiveEvolutionControlBuilder.apply(
      "command:rollout",
      "template:fixture",
      EvolutionControlBuilder.setRollout("command:set-rollout", {
        decision_id: "decision:source",
        fallback_plan: sourcePlan,
        target_plan: targetPlan,
        mode: { mode: "active" },
      }),
    ),
    occurrence_selected: LiveEvolutionControlBuilder.apply(
      "command:selection",
      "template:fixture",
      EvolutionControlBuilder.selectOccurrence(
        "command:select",
        "occurrence:fixture",
        "selection:fixture",
        targetBinding,
      ),
    ),
    migrated: LiveEvolutionControlBuilder.apply(
      "command:migration",
      "template:fixture",
      EvolutionControlBuilder.migrate("command:migrate", migrationRequest),
    ),
    restart_authorized: LiveEvolutionControlBuilder.apply(
      "command:restart",
      "template:fixture",
      EvolutionControlBuilder.restartUnderNewPlan("command:restart-child", restartRequest),
    ),
    shadow_recorded: LiveEvolutionControlBuilder.apply(
      "command:shadow",
      "template:fixture",
      EvolutionControlBuilder.shadow("command:shadow-child", shadowRequest),
    ),
    gate_applied: LiveEvolutionControlBuilder.apply(
      "command:gate",
      "template:fixture",
      EvolutionControlBuilder.applyGate("command:gate-child", gate, "decision:target"),
    ),
  };
  const evolutionId = "evolution:fixture";
  const execute = (engine: CliEngine, command: LiveEvolutionCommand) =>
    engine.executeLiveEvolution(
      evolutionTargetFor(command),
      evolutionId,
      command,
    );
  for (const [result, outcome] of Object.entries(validResponses)) {
    const commit = evolutionCommit(evolutionId, commands[result]!, outcome);
    let actual: EvolutionCommit;
    try {
      actual = await withSuccessEngine(
        { type: "live_evolution_executed", commit },
        (engine) => execute(engine, commands[result]!),
        result === "patch_applied" ? targetPlan : undefined,
      );
    } catch (error) {
      throw new Error(`valid ${result} response was rejected`, { cause: error });
    }
    assert.deepEqual(
      actual,
      commit,
      `valid ${result} response was rejected`,
    );
  }
  const configuredStore = directoryStore("unused");
  const migrationProvider = evolutionTargetFor(commands.migrated!, configuredStore)
    .migration_adapter as EngineMigrationProviderTarget;
  const targetExecutionBindings = evolutionTargetFor(commands.migrated!, configuredStore)
    .target_execution_bindings;
  const shadowProvider = evolutionTargetFor(commands.shadow_recorded!, configuredStore)
    .shadow_driver as EngineShadowProviderTarget;
  for (const limit of [16 * 1024 * 1024 - 1, 16 * 1024 * 1024 + 1]) {
    const invalidTarget = structuredClone(
      evolutionTargetFor(commands.migrated!, configuredStore),
    );
    (invalidTarget.migration_adapter as EngineMigrationProviderTarget)
      .process.process.message_limit = limit;
    await assert.rejects(
      () => new CliEngine("missing-engine").executeLiveEvolution(
        invalidTarget,
        evolutionId,
        commands.migrated!,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "evolution_request_validation_failed",
    );
  }
  const targetMigrationCommand = commands.migrated!;
  if (targetMigrationCommand.operation !== "apply"
    || targetMigrationCommand.command.operation !== "migrate") {
    throw new Error("migration fixture changed variants");
  }
  const targetMigrationPlan = targetMigrationCommand.command.request.to_plan;
  const targetExecution = {
    ...processTarget(process.execPath),
    revision: `sha256:${"9".repeat(64)}`,
  };
  const exactTarget: EngineEvolutionTarget = {
    ...evolutionTargetFor(commands.migrated!, configuredStore),
    target_execution_bindings: { [targetMigrationPlan]: targetExecution },
  };
  const { target_execution_bindings: _bindings, ...missingBindings } = exactTarget;
  const invalidTargetBindings: unknown[] = [
    missingBindings,
    {
      ...exactTarget,
      target_execution_bindings: {
        ...exactTarget.target_execution_bindings,
        [`sha256:${"8".repeat(64)}`]: targetExecution,
      },
    },
    {
      ...exactTarget,
      target_execution_bindings: {
        [targetPlan]: processTarget(process.execPath),
      },
    },
  ];
  for (const invalidTarget of invalidTargetBindings) {
    await assert.rejects(
      () => new CliEngine("missing-engine").executeLiveEvolution(
        invalidTarget as EngineEvolutionTarget,
        evolutionId,
        commands.migrated!,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation",
    );
  }
  for (const [result, expectedTarget] of [
    ["definition_published", evolutionTargetFor(commands.definition_published!, configuredStore)],
    ["migrated", evolutionTargetFor(commands.migrated!, configuredStore)],
    ["shadow_recorded", evolutionTargetFor(commands.shadow_recorded!, configuredStore)],
  ] as const) {
    const command = commands[result]!;
    const commit = evolutionCommit(evolutionId, command, validResponses[result]!);
    assert.deepEqual(
      await withSuccessEngine(
        { type: "live_evolution_executed", commit },
        (engine) => new DurableEngine(
          configuredStore,
          undefined,
          undefined,
          engine,
          evolutionId,
          migrationProvider,
          shadowProvider,
          targetExecutionBindings,
        ).evolve(command),
        undefined,
        "cymule.engine/5",
        {
          type: "execute_live_evolution",
          target: expectedTarget,
          evolution_id: evolutionId,
          command,
        },
      ),
      commit,
      `dual-configured DurableEngine selected the wrong provider for ${result}`,
    );
  }
  const historicalMigrationCommit = evolutionCommit(
    evolutionId,
    commands.migrated!,
    validResponses.migrated!,
  );
  assert.deepEqual(
    await withSuccessEngine(
      { type: "live_evolution_executed", commit: historicalMigrationCommit },
      (engine) => engine.executeLiveEvolution(
        {
          store: directoryStore("unused"),
          migration_adapter: migrationProvider,
          shadow_driver: null,
          target_execution_bindings: targetExecutionBindings,
        },
        evolutionId,
        commands.migrated!,
      ),
    ),
    historicalMigrationCommit,
    "migration requests must retain their exact provider target",
  );

  const object = (value: unknown) => value as Record<string, unknown>;
  const list = (value: unknown) => value as unknown[];
  const commitFor = (result: string) => structuredClone(evolutionCommit(
    evolutionId,
    commands[result]!,
    validResponses[result]!,
  )) as unknown as Record<string, unknown>;
  const persisted = (commit: Record<string, unknown>) => object(commit.receipt);
  const semantic = (commit: Record<string, unknown>) => object(object(persisted(commit).command).command);
  const wrongEvolution = commitFor("applied");
  object(persisted(wrongEvolution).command).evolution_id = "evolution:other";
  const wrongOuterCommand = commitFor("applied");
  semantic(wrongOuterCommand).command_id = "command:other";
  const wrongInnerCommand = commitFor("applied");
  object(semantic(wrongInnerCommand).command).command_id = "command:inner-other";
  const wrongTemplate = commitFor("template_registered");
  object(object(semantic(wrongTemplate).template).candidate).metadata = {
    generation: "other",
  };
  const wrongMigrationRequest = commitFor("migrated");
  object(object(semantic(wrongMigrationRequest).command).request).adapter_revision =
    contentId("0");
  const wrongPublicationEvidence = commitFor("publication_applied");
  object(object(object(semantic(wrongPublicationEvidence).publication).evidence).reference)
    .artifact_id = contentId("0");
  const wrongPublicationMode = commitFor("publication_applied");
  object(semantic(wrongPublicationMode).publication).mode = { mode: "shadow" };
  const wrongRolloutDecision = commitFor("applied");
  object(object(semantic(wrongRolloutDecision).command).decision).mode = { mode: "shadow" };
  const observeCommand = LiveEvolutionControlBuilder.apply(
    "command:observation",
    "template:fixture",
    EvolutionControlBuilder.observe("command:observe", {
      observation_id: "observation:fixture",
      decision_id: "decision:source",
      occurrence_id: "occurrence:fixture",
      plan_id: targetPlan,
      outcome: "succeeded",
      evidence: artifact,
    }),
  );
  const receiptTampering: Array<{
    name: string;
    sent: LiveEvolutionCommand;
    commit: Record<string, unknown>;
  }> = [
    { name: "receipt evolution identity", sent: commands.applied!, commit: wrongEvolution },
    { name: "receipt outer command", sent: commands.applied!, commit: wrongOuterCommand },
    { name: "receipt inner command", sent: commands.applied!, commit: wrongInnerCommand },
    { name: "receipt template", sent: commands.template_registered!, commit: wrongTemplate },
    {
      name: "receipt migration request",
      sent: commands.migrated!,
      commit: wrongMigrationRequest,
    },
    {
      name: "receipt publication evidence",
      sent: commands.publication_applied!,
      commit: wrongPublicationEvidence,
    },
    {
      name: "receipt publication mode",
      sent: commands.publication_applied!,
      commit: wrongPublicationMode,
    },
    { name: "receipt rollout decision", sent: commands.applied!, commit: wrongRolloutDecision },
    { name: "receipt observation swap", sent: observeCommand, commit: commitFor("applied") },
  ];
  for (const testCase of receiptTampering) {
    await assert.rejects(
      () => withSuccessEngine(
        { type: "live_evolution_executed", commit: testCase.commit },
        (engine) => execute(engine, testCase.sent),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
      testCase.name,
    );
  }
  const malicious: Array<{
    name: string;
    result: string;
    mutate: (response: Record<string, unknown>) => void;
  }> = [
    {
      name: "definition revision version",
      result: "definition_published",
      mutate: (response) => { object(response.revision).revision_version = "cymule.subflow-revision/1"; },
    },
    {
      name: "definition zero sequence",
      result: "definition_published",
      mutate: (response) => { object(response.revision).sequence = 0; },
    },
    {
      name: "definition nested body",
      result: "definition_published",
      mutate: (response) => {
        object(object(response.revision).definition).body = { steps: [], result: { kind: "future" } };
      },
    },
    {
      name: "definition reference strategy",
      result: "definition_published",
      mutate: (response) => {
        object(response.revision).references = [{
          logical_ref: "module:dependency",
          local_definition: "dependency",
          input_schema: {},
          output_schema: {},
          strategy: { strategy: "latest_compatible", revision_id: contentId("1") },
        }];
      },
    },
    {
      name: "linked sealed Plan",
      result: "template_registered",
      mutate: (response) => { delete object(object(response.linked).plan).candidate; },
    },
    {
      name: "linked resolved revision key set",
      result: "template_registered",
      mutate: (response) => {
        object(object(response.linked).resolved_revisions)["module:extra"] = contentId("1");
      },
    },
    {
      name: "publication update type",
      result: "publication_applied",
      mutate: (response) => {
        object(list(object(response.receipt).updates)[0]).advanced = "true";
      },
    },
    {
      name: "publication update order",
      result: "publication_applied",
      mutate: (response) => { list(object(response.receipt).updates).reverse(); },
    },
    {
      name: "publication advance relation",
      result: "publication_applied",
      mutate: (response) => {
        const update = object(list(object(response.receipt).updates)[0]);
        update.current_plan_id = update.previous_plan_id;
      },
    },
    {
      name: "Plan edge lineage",
      result: "patch_applied",
      mutate: (response) => {
        const edge = object(response.edge);
        edge.to_plan = edge.from_plan;
      },
    },
    {
      name: "Plan edge sealed target",
      result: "patch_applied",
      mutate: (response) => { object(response.edge).to_plan = contentId("0"); },
    },
    {
      name: "Plan edge empty operations",
      result: "patch_applied",
      mutate: (response) => { object(response.edge).operations = []; },
    },
    {
      name: "Plan edge operation kind",
      result: "patch_applied",
      mutate: (response) => {
        object(list(object(response.edge).operations)[0]).kind = "future";
      },
    },
    {
      name: "Plan edge digest shape",
      result: "patch_applied",
      mutate: (response) => {
        object(list(object(response.edge).operations)[0]).after = "ABC";
      },
    },
    {
      name: "Plan edge operation order",
      result: "patch_applied",
      mutate: (response) => { list(object(response.edge).operations).reverse(); },
    },
    {
      name: "retired Plan edge evidence",
      result: "patch_applied",
      mutate: (response) => { object(response.edge).evidence = artifact; },
    },
    {
      name: "applied payload",
      result: "applied",
      mutate: (response) => { response.receipt = {}; },
    },
    {
      name: "occurrence Plan identity",
      result: "occurrence_selected",
      mutate: (response) => { object(response.pin).plan_id = "plan:forged"; },
    },
    {
      name: "occurrence execution binding",
      result: "occurrence_selected",
      mutate: (response) => { object(object(response.pin).execution_binding).kind = "test/value"; },
    },
    {
      name: "migration adapter pin",
      result: "migrated",
      mutate: (response) => {
        object(object(response.receipt).request).adapter_revision = contentId("0");
      },
    },
    {
      name: "migration Run identity scalar bound",
      result: "migrated",
      mutate: (response) => {
        object(object(response.receipt).request).run_id = `${migrationRunId}🦀`;
      },
    },
    {
      name: "migration source witness",
      result: "migrated",
      mutate: (response) => {
        object(response.receipt).source_witness_id = "witness:forged";
      },
    },
    {
      name: "migration Plan lineage",
      result: "migrated",
      mutate: (response) => {
        const request = object(object(response.receipt).request);
        request.to_plan = request.from_plan;
      },
    },
    {
      name: "migration target epoch",
      result: "migrated",
      mutate: (response) => { object(response.receipt).target_epoch = sourceEpoch + 2; },
    },
    {
      name: "migration target binding",
      result: "migrated",
      mutate: (response) => {
        object(object(response.receipt).target_continuation).binding_context = sourceBinding.artifact_id;
      },
    },
    {
      name: "migration Continuation generation missing",
      result: "migrated",
      mutate: (response) => {
        delete object(object(response.receipt).target_continuation).continuation_version;
      },
    },
    {
      name: "migration Continuation generation unsupported",
      result: "migrated",
      mutate: (response) => {
        object(object(response.receipt).target_continuation).continuation_version = "cymule.continuation-state/999";
      },
    },
    {
      name: "migration target state",
      result: "migrated",
      mutate: (response) => { object(object(response.receipt).target_continuation).state = artifact; },
    },
    {
      name: "migration target fence",
      result: "migrated",
      mutate: (response) => { object(object(response.receipt).target_continuation).execution_fence = 10; },
    },
    {
      name: "migration Artifact kind grammar",
      result: "migrated",
      mutate: (response) => { object(object(response.receipt).evidence).kind = "Test/Value"; },
    },
    {
      name: "restart Run lineage",
      result: "restart_authorized",
      mutate: (response) => {
        const request = object(object(response.receipt).request);
        request.replacement_run = request.run_id;
      },
    },
    {
      name: "restart Run identity control",
      result: "restart_authorized",
      mutate: (response) => {
        object(object(response.receipt).request).replacement_run = "run:\u0085forged";
      },
    },
    {
      name: "restart Plan lineage",
      result: "restart_authorized",
      mutate: (response) => {
        const request = object(object(response.receipt).request);
        request.to_plan = request.from_plan;
      },
    },
    {
      name: "restart expected source epoch",
      result: "restart_authorized",
      mutate: (response) => {
        const request = object(object(response.receipt).request);
        request.expected_source_epoch = 9_007_199_254_740_992;
      },
    },
    {
      name: "restart source witness",
      result: "restart_authorized",
      mutate: (response) => {
        object(response.receipt).source_witness_id = "witness:forged";
      },
    },
    {
      name: "restart target Plan",
      result: "restart_authorized",
      mutate: (response) => { object(object(response.receipt).target_plan).plan_id = contentId("0"); },
    },
    {
      name: "shadow Plan identity",
      result: "shadow_recorded",
      mutate: (response) => { object(response.comparison).shadow_plan = "plan:forged"; },
    },
    {
      name: "shadow result type",
      result: "shadow_recorded",
      mutate: (response) => { object(response.comparison).equivalent = "true"; },
    },
    {
      name: "shadow driver pin",
      result: "shadow_recorded",
      mutate: (response) => { object(response.comparison).driver_revision = contentId("0"); },
    },
    {
      name: "shadow unpaired surrogate identity",
      result: "shadow_recorded",
      mutate: (response) => { object(response.comparison).comparison_id = "\ud800"; },
    },
    {
      name: "rollout gate fields",
      result: "gate_applied",
      mutate: (response) => {
        object(object(object(response.transition).evaluation).gate).unexpected = true;
      },
    },
    {
      name: "rollout decision relation",
      result: "gate_applied",
      mutate: (response) => {
        object(object(object(response.transition).evaluation).gate).decision_id = "decision:other";
      },
    },
    {
      name: "rollout transition lineage",
      result: "gate_applied",
      mutate: (response) => {
        const transition = object(response.transition);
        transition.to_decision = transition.from_decision;
      },
    },
    {
      name: "rollout failures",
      result: "gate_applied",
      mutate: (response) => {
        object(object(response.transition).evaluation).target_failures = 3;
      },
    },
    {
      name: "rollout evidence count",
      result: "gate_applied",
      mutate: (response) => {
        object(object(response.transition).evaluation).evidence_ids = ["evidence:1"];
      },
    },
    {
      name: "rollout evidence IDs",
      result: "gate_applied",
      mutate: (response) => {
        object(object(response.transition).evaluation).evidence_ids = [
          "evidence:1", "evidence:1", "evidence:3",
        ];
      },
    },
    {
      name: "rollout outcome",
      result: "gate_applied",
      mutate: (response) => {
        object(object(response.transition).evaluation).outcome = "rollback";
      },
    },
    {
      name: "rollout pending transition",
      result: "gate_applied",
      mutate: (response) => {
        const evaluation = object(object(response.transition).evaluation);
        evaluation.target_observations = 1;
        evaluation.equivalent_shadows = 0;
        evaluation.outcome = "pending";
        evaluation.evidence_ids = ["evidence:1"];
      },
    },
    {
      name: "rollout content identity",
      result: "gate_applied",
      mutate: (response) => { object(response.transition).transition_id = "transition:forged"; },
    },
  ];
  for (const testCase of malicious) {
    const outcome = structuredClone(validResponses[testCase.result]!);
    testCase.mutate(outcome as unknown as Record<string, unknown>);
    const commit = evolutionCommit(
      evolutionId,
      commands[testCase.result]!,
      outcome,
    );
    await assert.rejects(
      () => withSuccessEngine(
        { type: "live_evolution_executed", commit },
        (engine) => execute(engine, commands[testCase.result]!),
        testCase.result === "patch_applied" ? targetPlan : undefined,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
      testCase.name,
    );
  }
  const retiredShapeCommands: Array<{
    name: string;
    result: string;
    command: LiveEvolutionCommand;
  }> = [
    {
      name: "migration retired safe-point field",
      result: "migrated",
      command: {
        control_version: "cymule.live-evolution-control/6",
        command_id: "command:migration:retired-request",
        operation: "apply",
        template_id: "template:fixture",
        command: {
          ...EvolutionControlBuilder.migrate("command:migrate:retired-request", migrationRequest),
          request: { ...migrationRequest, safe_point_id: contentId("0") },
        },
      } as unknown as LiveEvolutionCommand,
    },
    {
      name: "restart retired nested safe-point field",
      result: "restart_authorized",
      command: {
        control_version: "cymule.live-evolution-control/6",
        command_id: "command:restart:retired-request",
        operation: "apply",
        template_id: "template:fixture",
        command: {
          ...EvolutionControlBuilder.restartUnderNewPlan(
            "command:restart-child:retired-request",
            restartRequest,
          ),
          request: { ...restartRequest, safe_point: { retired: true } },
        },
      } as unknown as LiveEvolutionCommand,
    },
    {
      name: "apply retired outer safe-point field",
      result: "occurrence_selected",
      command: {
        control_version: "cymule.live-evolution-control/6",
        command_id: "command:selection:retired-proof",
        operation: "apply",
        template_id: "template:fixture",
        command: EvolutionControlBuilder.selectOccurrence(
          "command:select:retired-proof",
          "occurrence:fixture",
          "selection:fixture",
          targetBinding,
        ),
        safe_point: { safe_point_version: "cymule.migration-safe-point/2" },
      } as unknown as LiveEvolutionCommand,
    },
    {
      name: "shadow missing driver pin",
      result: "shadow_recorded",
      command: {
        control_version: "cymule.live-evolution-control/6",
        command_id: "command:shadow:missing-driver",
        operation: "apply",
        template_id: "template:fixture",
        command: {
          control_version: "cymule.evolution-control/5",
          command_id: "command:shadow-child:missing-driver",
          operation: "shadow",
          request: {
            comparison_id: shadowRequest.comparison_id,
            decision_id: shadowRequest.decision_id,
            subject: shadowRequest.subject,
            primary_plan: shadowRequest.primary_plan,
            shadow_plan: shadowRequest.shadow_plan,
            input: shadowRequest.input,
            comparison_policy: shadowRequest.comparison_policy,
          },
        },
      } as unknown as LiveEvolutionCommand,
    },
  ];
  for (const testCase of retiredShapeCommands) {
    await assert.rejects(
      () => withSuccessEngine(
        {
          type: "live_evolution_executed",
          commit: evolutionCommit(
            evolutionId,
            testCase.command,
            validResponses[testCase.result]!,
          ),
        },
        (engine) => execute(engine, testCase.command),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "evolution_request_validation_failed"
        && error.failure.retry_disposition === "correct_and_retry",
      testCase.name,
    );
  }
});

test("TypeScript keeps registry definition wire validation distinct from sealed Plan admission", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const registryDefinition = {
    id: "main",
    input_schema: 0,
    output_schema: null,
    body: {
      steps: [{
        id: "",
        op: "call",
        component: "",
        input: { kind: "literal", value: 1 },
      }],
      result: { kind: "binding", name: "" },
    },
  } as unknown as PlanCandidate["definitions"][number];
  const command = LiveEvolutionControlBuilder.publishDefinition(
    "command:registry-wire",
    "module:registry-wire",
    registryDefinition,
    [],
  );
  const evolutionId = "evolution:registry-wire";
  const commit = evolutionCommit(evolutionId, command, {
      result: "definition_published",
      revision: {
        revision_version: "cymule.subflow-revision/2",
        revision_id: contentId("1"),
        logical_ref: "module:registry-wire",
        sequence: 1,
        definition: registryDefinition,
        references: [],
      },
    });
  assert.deepEqual(
    await withSuccessEngine(
      { type: "live_evolution_executed", commit },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        command,
      ),
    ),
    commit,
  );
});

test("TypeScript preserves and closes published module references", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const definition = new FlowBuilder("published-references", {}, {})
    .finish({ kind: "input" }).definitions[0]!;
  const reference = {
    logical_ref: "module:child",
    local_definition: "child",
    input_schema: {},
    output_schema: {},
    strategy: { strategy: "pinned" as const, revision_id: contentId("1") },
  };
  const command = LiveEvolutionControlBuilder.publishDefinition(
    "command:published-references",
    "module:parent",
    definition,
    [reference],
  );
  const evolutionId = "evolution:published-references";
  const commit = evolutionCommit(evolutionId, command, {
      result: "definition_published",
      revision: {
        revision_version: "cymule.subflow-revision/2",
        revision_id: contentId("2"),
        logical_ref: "module:parent",
        sequence: 1,
        definition,
        references: [reference],
      },
    });
  assert.deepEqual(
    await withSuccessEngine(
      { type: "live_evolution_executed", commit },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        command,
      ),
    ),
    commit,
  );

  const invalidCommands = [
    Object.fromEntries(Object.entries(command).filter(([field]) => field !== "references")),
    {
      ...command,
      references: [Object.fromEntries(Object.entries(reference).filter(([field]) => field !== "strategy"))],
    },
    { ...command, references: [{ ...reference, strategy: { strategy: "latest_compatible" } }] },
    {
      ...command,
      references: [
        { ...reference, logical_ref: "module:z", local_definition: "z" },
        { ...reference, logical_ref: "module:a", local_definition: "a" },
      ],
    },
    {
      ...command,
      references: [{
        ...reference,
        input_schema: { description: "x".repeat(1024 * 1024) },
      }],
    },
  ];
  for (const invalid of invalidCommands) {
    await assert.rejects(
      () => new CliEngine("missing-engine").executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        invalid as LiveEvolutionCommand,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.retry_disposition === "correct_and_retry",
    );
  }

  const mismatchedCommit = structuredClone(commit);
  if (mismatchedCommit.receipt.outcome.result !== "definition_published") {
    throw new Error("published-reference fixture has the wrong outcome");
  }
  mismatchedCommit.receipt.outcome.revision.references = [];
  await assert.rejects(
    () => withSuccessEngine(
      { type: "live_evolution_executed", commit: mismatchedCommit },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        command,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript seals an apply-patch target before mutation and binds the returned Plan", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const digest = (digit: string) => digit.repeat(64);
  const candidate = new FlowBuilder("patch-preflight", {}, {}).finish({ kind: "input" });
  const sourcePlan = contentId("1");
  const sealedTargetPlan = contentId("2");
  const forgedTargetPlan = contentId("3");
  const evidence = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("4"),
    kind: "cymule.evolution-evidence/1",
  };
  const operations = [{
    kind: "add",
    target: "definition:next",
    before: null,
    after: digest("5"),
  }];
  const command = LiveEvolutionControlBuilder.apply(
    "command:patch-preflight",
    "template:patch-preflight",
    EvolutionControlBuilder.applyPatch("command:patch", {
      from_plan: sourcePlan,
      target: candidate,
      operations,
      evidence,
    }),
  );
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "patch-preflight-engine");
  const log = join(directory, "requests.log");
  const evolutionId = "evolution:patch-preflight";
  const mutationResponse = {
    type: "live_evolution_executed",
    commit: evolutionCommit(evolutionId, command, {
        result: "patch_applied",
        edge: {
          edge_id: contentId("6"),
          from_plan: sourcePlan,
          to_plan: forgedTargetPlan,
          operations,
        },
      }),
  };
  writeFileSync(
    executable,
    `#!/usr/bin/env node
const { appendFileSync } = require("node:fs");
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  appendFileSync(${JSON.stringify(log)}, request.type + "\\n");
  const response = request.type === "seal"
    ? { type: "sealed", plan: {
        plan_id: ${JSON.stringify(sealedTargetPlan)},
        candidate: request.candidate,
      } }
    : JSON.parse(${JSON.stringify(JSON.stringify(mutationResponse))});
  process.stdout.write(JSON.stringify({
    engine_protocol: "cymule.engine/5",
    outcome: "success",
    request,
    response,
  }));
});
`,
  );
  chmodSync(executable, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(executable).executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        evolutionId,
        command,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
    );
    assert.deepEqual(readFileSync(log, "utf8").trim().split("\n"), [
      "seal",
      "execute_live_evolution",
    ]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript rejects an invalid live command before starting the Engine", async () => {
  const definition = new FlowBuilder("live-preflight", {}, {})
    .finish({ kind: "input" }).definitions[0]!;
  const command = {
    ...LiveEvolutionControlBuilder.publishDefinition(
      "command:live-preflight",
      "module:live-preflight",
      definition,
      [],
    ),
    control_version: "cymule.live-evolution-control/5",
  } as unknown as LiveEvolutionCommand;
  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const executable = join(directory, "must-not-start");
  const started = join(directory, "started");
  writeFileSync(
    executable,
    `#!/bin/sh
printf started >${JSON.stringify(started)}
exit 1
`,
  );
  chmodSync(executable, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(executable).executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "journal:live-preflight",
        command,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "evolution_request_validation_failed"
        && error.failure.retry_disposition === "correct_and_retry",
    );
    assert.throws(() => readFileSync(started));
    const invalidDurable = {
      ...DurableControlBuilder.resumeRun("run:live-preflight", fixtureExecution()),
      run_id: "",
    } as DurableCommand;
    await assert.rejects(
      () => new CliEngine(executable).executeDurable(
        { store: directoryStore("unused") },
        invalidDurable,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "validation"
        && error.failure.code === "durable_request_validation_failed"
        && error.failure.retry_disposition === "correct_and_retry",
    );
    assert.throws(() => readFileSync(started));
    const cancelled = new AbortController();
    cancelled.abort();
    const validCommand = LiveEvolutionControlBuilder.publishDefinition(
      "command:live-pre-cancelled",
      "module:live-pre-cancelled",
      definition,
      [],
    );
    await assert.rejects(
      () => new CliEngine(executable, { signal: cancelled.signal }).executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "journal:live-pre-cancelled",
        validCommand,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "cancelled"
        && error.failure.retry_disposition === "never",
    );
    assert.throws(() => readFileSync(started));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript classifies wrong success tags by request mutation authority", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const definition: PlanCandidate["definitions"][number] = {
    id: "main",
    input_schema: {},
    output_schema: {},
    body: { steps: [], result: { kind: "input" } },
  };
  const candidate: PlanCandidate = {
    ir_version: "cymule.ir/3",
    name: "wrong-success-tag",
    entry: "main",
    components: [],
    effects: [],
    definitions: [definition],
    metadata: {},
  };
  const command = LiveEvolutionControlBuilder.publishDefinition(
    "command:wrong-tag",
    "module:wrong-tag",
    definition,
    [],
  );
  await assert.rejects(
    () => withSuccessEngine(
      { type: "verified" },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "journal:wrong-tag",
        command,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withSuccessEngine({ type: "verified" }, (engine) => engine.seal(candidate)),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
  await assert.rejects(
    () => withSuccessEngine(
      { type: "sealed", plan: { plan_id: contentId("b"), candidate } },
      (engine) => engine.seal(candidate),
      undefined,
      "cymule.engine/4",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "contract_violation"
      && error.failure.code === "unsupported_engine_protocol",
  );
  await assert.rejects(
    () => withSuccessEngine(
      { type: "verified" },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "journal:legacy-success",
        command,
      ),
      undefined,
      "cymule.engine/4",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "unsupported_engine_protocol"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withFailureEngine(
      {
        category: "transport_failure",
        phase: "transport",
        code: "legacy_failure",
        message: "legacy failure envelope",
      },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "journal:legacy-failure",
        command,
      ),
      "cymule.engine/4",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "unsupported_engine_protocol"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "live_evolution_executed",
        commit: evolutionCommit("evolution:wrong-result", command, { result: "applied" }),
      },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "evolution:wrong-result",
        command,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "live_evolution_executed",
        commit: evolutionCommit("evolution:mismatched-result", command, {
            result: "definition_published",
            revision: {
              revision_version: "cymule.subflow-revision/2",
              revision_id: contentId("a"),
              logical_ref: "module:different",
              sequence: 1,
              definition,
              references: [],
            },
          }),
      },
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        "evolution:mismatched-result",
        command,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript real client classifies an unsupported Engine by mutation authority", async (context) => {
  const executable = process.env.CYMULE_UNSUPPORTED_ENGINE;
  if (executable === undefined) {
    context.skip("unsupported Engine protocol fixture is not configured");
    return;
  }
  const engine = new CliEngine(executable);
  const candidate = new FlowBuilder("unsupported_protocol", {}, {}).finish({ kind: "input" });
  await assert.rejects(
    () => engine.seal(candidate),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "contract_violation"
      && error.failure.code === "unsupported_engine_protocol"
      && error.failure.retry_disposition === "never",
  );
  await assert.rejects(
    () => engine.observeClock(
      sqliteClock(
        "/tmp/cymule-typescript-unsupported-clock.sqlite",
        "clock:typescript:unsupported",
        `sha256:${"4".repeat(64)}`,
      ),
      "run:typescript:unsupported",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "unsupported_engine_protocol"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript binds every success envelope to the exact sent request", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const candidate = new FlowBuilder("request-echo", {}, {}).finish({ kind: "input" });
  const sealedResponse = {
    type: "sealed",
    plan: { plan_id: contentId("1"), candidate },
  };
  await assert.rejects(
    () => withSuccessEngine(
      sealedResponse,
      (engine) => engine.seal(candidate),
      undefined,
      "cymule.engine/5",
      { type: "seal", candidate: { ...candidate, name: "other" } },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );

  const clockTarget = sqliteClock(
    "/tmp/cymule-clock-echo",
    "clock:echo",
    contentId("2"),
  );
  const clockResponse = {
    type: "clock_observed",
    result: {
      run_id: "run:echo",
      observation: {
        clock_version: "cymule.clock-observation/2",
        observation_id: contentId("3"),
        source_id: clockTarget.source_id,
        source_generation: clockTarget.source_generation,
        scope: "scope:echo",
      },
    },
  };
  await assert.rejects(
    () => withSuccessEngine(
      clockResponse,
      (engine) => engine.observeClock(clockTarget, "run:echo"),
      undefined,
      "cymule.engine/5",
      { type: "observe_clock", target: clockTarget, run_id: "run:other" },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const cancel = DurableControlBuilder.cancelRun(
    "cancel:echo",
    "run:echo",
    { reason: "expected" },
  );
  const cancelTarget = durableTargetFor(cancel);
  const cancelledResponse = {
    type: "durable_executed",
    response: {
      type: "run_cancelled",
      receipt: {
        receipt_version: "cymule.run-cancellation-receipt/1",
        command: {
          cancellation_id: "cancel:echo",
          run_id: "run:echo",
          reason: { reason: "expected" },
        },
        boundary: {
          status: "cancelled",
          reason: {
            identity_version: "cymule.artifact/2",
            artifact_id: contentId("4"),
            kind: "cymule.cancellation-reason/1",
          },
        },
        receipt_id: "a".repeat(64),
      },
    },
  };
  assert.deepEqual(
    await withSuccessEngine(
      cancelledResponse,
      (engine) => engine.executeDurable(cancelTarget, cancel),
    ),
    cancelledResponse.response,
  );
  const wrongCancellationKind = structuredClone(cancelledResponse);
  wrongCancellationKind.response.receipt.boundary.reason.kind = "test/reason";
  await assert.rejects(
    () => withSuccessEngine(
      wrongCancellationKind,
      (engine) => engine.executeDurable(cancelTarget, cancel),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withSuccessEngine(
      cancelledResponse,
      (engine) => engine.executeDurable(cancelTarget, cancel),
      undefined,
      "cymule.engine/5",
      {
        type: "execute_durable",
        target: cancelTarget,
        command: { ...cancel, reason: { reason: "other" } },
      },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withSuccessEngine(
      cancelledResponse,
      (engine) => engine.executeDurable(cancelTarget, cancel),
      undefined,
      "cymule.engine/5",
      {
        type: "execute_durable",
        target: {
          store: { ...cancelTarget.store, domain: null },
        },
        command: cancel,
      },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const resolutionBinding = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("b"),
    kind: "cymule.execution-binding/2",
  };
  const resolution = DurableControlBuilder.resolveEffect(
    "resolution:echo",
    "run:echo",
    contentId("a"),
    resolutionBinding,
    contentId("e"),
    "driver:echo",
    3,
    "resolved_applied",
    { resolved: true },
  );
  assert.throws(
    () => DurableControlBuilder.resolveEffect(
      "resolution:legacy-binding",
      "run:echo",
      contentId("a"),
      resolutionBinding,
      "binding:not-content",
      "driver:echo",
      3,
      "resolved_applied",
      { resolved: true },
    ),
    /authority is invalid/,
  );
  const resolutionTarget = durableTargetFor(resolution);
  const resolutionResponse = {
    type: "durable_executed",
    response: {
      type: "effect_resolved",
      receipt: {
        receipt_version: "cymule.effect-resolution-receipt/1",
        command: {
          resolution_id: "resolution:echo",
          run_id: "run:echo",
          intent_id: contentId("a"),
          execution_binding: resolutionBinding,
          occurrence_binding: `sha256:${"e".repeat(64)}`,
          claim_owner: "driver:echo",
          claim_epoch: 3,
          resolution: "resolved_applied",
          value: { resolved: true },
        },
        actual_resolution: "resolved_applied",
        actual_value: { resolved: true },
        result: {
          identity_version: "cymule.artifact/2",
          artifact_id: contentId("c"),
          kind: "cymule.effect-result/1",
        },
        receipt_id: "d".repeat(64),
      },
    },
  };
  assert.deepEqual(
    await withSuccessEngine(
      resolutionResponse,
      (engine) => engine.executeDurable(resolutionTarget, resolution),
    ),
    resolutionResponse.response,
  );
  const providerTruth = structuredClone(resolutionResponse);
  const appliedNull = structuredClone(resolutionResponse);
  (appliedNull.response.receipt as { actual_value: unknown }).actual_value = null;
  assert.deepEqual(
    await withSuccessEngine(
      appliedNull,
      (engine) => engine.executeDurable(resolutionTarget, resolution),
    ),
    appliedNull.response,
  );
  (appliedNull.response.receipt as { result: unknown }).result = null;
  await assert.rejects(
    () => withSuccessEngine(appliedNull, (engine) => engine.executeDurable(resolutionTarget, resolution)),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.retry_disposition === "reconcile",
  );
  const providerReceipt = providerTruth.response.receipt as {
    actual_resolution: string;
    actual_value: unknown;
    result: unknown;
  };
  providerReceipt.actual_resolution = "resolved_not_applied";
  providerReceipt.actual_value = null;
  providerReceipt.result = null;
  assert.deepEqual(
    await withSuccessEngine(
      providerTruth,
      (engine) => engine.executeDurable(resolutionTarget, resolution),
    ),
    providerTruth.response,
  );
  const wrongResultKind = structuredClone(resolutionResponse);
  if (wrongResultKind.response.receipt.result === null) {
    throw new Error("Effect resolution fixture result is missing");
  }
  wrongResultKind.response.receipt.result.kind = "test/result";
  await assert.rejects(
    () => withSuccessEngine(
      wrongResultKind,
      (engine) => engine.executeDurable(resolutionTarget, resolution),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  const missingResult = structuredClone(resolutionResponse);
  (missingResult.response.receipt as unknown as Record<string, unknown>).result = null;
  await assert.rejects(
    () => withSuccessEngine(
      missingResult,
      (engine) => engine.executeDurable(resolutionTarget, resolution),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const definition = candidate.definitions[0]!;
  const liveCommand = LiveEvolutionControlBuilder.publishDefinition(
    "command:echo",
    "module:echo",
    definition,
    [],
  );
  const evolutionId = "evolution:echo";
  const liveCommit = evolutionCommit(evolutionId, liveCommand, {
      result: "definition_published",
      revision: {
        revision_version: "cymule.subflow-revision/2",
        revision_id: contentId("5"),
        logical_ref: "module:echo",
        sequence: 1,
        definition,
        references: [],
      },
    });
  const evolutionTarget: EngineEvolutionTarget = {
    store: directoryStore("unused"),
    migration_adapter: null,
    shadow_driver: null,
    target_execution_bindings: {},
  };
  await assert.rejects(
    () => withSuccessEngine(
      { type: "live_evolution_executed", commit: liveCommit },
      (engine) => engine.executeLiveEvolution(
        evolutionTarget,
        evolutionId,
        liveCommand,
      ),
      undefined,
      "cymule.engine/5",
      {
        type: "execute_live_evolution",
        target: evolutionTarget,
        evolution_id: evolutionId,
        command: { ...liveCommand, command_id: "command:other" },
      },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  await assert.rejects(
    () => withSuccessEngine(
      {
        ...clockResponse,
        result: {
          ...clockResponse.result,
          observation: {
            ...clockResponse.result.observation,
            source_id: "clock:other",
          },
        },
      },
      (engine) => engine.observeClock(clockTarget, "run:echo"),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "durable_executed",
        response: {
          type: "run_index_page",
          page: {
            observed_revision: contentId("a"),
            source_root: "b".repeat(64),
            items: [],
            next_cursor: null,
          },
        },
      },
      (engine) => engine.executeDurable(cancelTarget, cancel),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const resourceCandidate = ResourceBuilder.text("expected");
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "sealed_resource",
        resource: {
          resource_id: contentId("6"),
          ...ResourceBuilder.text("other"),
        },
      },
      (engine) => engine.sealResource(resourceCandidate),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );

  const runPlan = { plan_id: contentId("7"), candidate };
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "execution_boundary",
        execution: {
          status: "release_required",
          release: {
            run_id: "run:echo",
            plan_id: contentId("8"),
            intent_ids: [contentId("9")],
          },
        },
      },
      (engine) => engine.run(
        runPlan,
        null,
        processTarget(process.execPath),
        "run:echo",
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
  const legacyExecutable = join(directory, "missing-request-echo");
  writeFileSync(
    legacyExecutable,
    `#!/bin/sh
cat >/dev/null
printf '%s' '${JSON.stringify({
      engine_protocol: "cymule.engine/5",
      outcome: "success",
      response: sealedResponse,
    })}'
`,
  );
  chmodSync(legacyExecutable, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(legacyExecutable).seal(candidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript durable Clock rejects a typed result bound to another Run", async () => {
  const clock = sqliteClock(
    "/tmp/cymule-clock-fake-run",
    "clock:fake-run",
    `sha256:${"1".repeat(64)}`,
  );
  const transport: EngineTransport = {
    async seal(): Promise<never> { throw new Error("unused"); },
    async observeClock() {
      return {
        run_id: "run:foreign",
        observation: {
          clock_version: "cymule.clock-observation/2",
          observation_id: `sha256:${"2".repeat(64)}`,
          source_id: clock.source_id,
          source_generation: clock.source_generation,
          scope: `sha256:${"3".repeat(64)}`,
        },
      };
    },
    async executeDurable(): Promise<never> { throw new Error("unused"); },
    async executeLiveEvolution(): Promise<never> { throw new Error("unused"); },
  };
  const durable = new DurableEngine(directoryStore("unused"), undefined, clock, transport);
  await assert.rejects(
    () => durable.observeClock("run:expected"),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript DurableEngine closes custom transport validation and response-loss boundaries", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const clock = sqliteClock(
    "/tmp/cymule-custom-transport-clock",
    "clock:custom-transport",
    contentId("1"),
  );
  const calls = {
    seal: 0,
    observeClock: 0,
    executeDurable: 0,
    executeLiveEvolution: 0,
  };
  let durableResult: unknown = {
    type: "run_index_page",
    page: {
      observed_revision: contentId("2"),
      source_root: "3".repeat(64),
      items: [],
      next_cursor: null,
    },
  };
  let durableRejection: Error | undefined;
  let evolutionResult: unknown;
  let evolutionRejection: Error | undefined;
  const transport: EngineTransport = {
    async seal(): Promise<never> {
      calls.seal += 1;
      throw new Error("unexpected custom seal call");
    },
    async observeClock(): Promise<never> {
      calls.observeClock += 1;
      throw new Error("unexpected custom Clock call");
    },
    async executeDurable() {
      calls.executeDurable += 1;
      if (durableRejection !== undefined) throw durableRejection;
      return structuredClone(durableResult) as never;
    },
    async executeLiveEvolution() {
      calls.executeLiveEvolution += 1;
      if (evolutionRejection !== undefined) throw evolutionRejection;
      return structuredClone(evolutionResult) as never;
    },
  };
  const store = directoryStore("unused");
  const executor = processTarget(process.execPath);

  await assert.rejects(
    () => new DurableEngine(store, executor, clock, transport)
      .resume("", fixtureExecution()),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.phase === "verify_durable_command"
      && error.failure.code === "durable_command_validation_failed"
      && error.failure.retry_disposition === "correct_and_retry",
  );
  await assert.rejects(
    () => new DurableEngine(store, undefined, clock, transport)
      .resume("run:missing-executor", fixtureExecution()),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.code === "durable_request_validation_failed"
      && error.failure.retry_disposition === "correct_and_retry",
  );
  await assert.rejects(
    () => new DurableEngine(store, executor, undefined, transport)
      .resume("run:missing-clock", fixtureExecution()),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.code === "durable_request_validation_failed"
      && error.failure.retry_disposition === "correct_and_retry",
  );
  await assert.rejects(
    () => new DurableEngine(store, executor, undefined, transport)
      .observeClock("run:missing-clock"),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.phase === "observe_clock"
      && error.failure.code === "missing_clock_provider"
      && error.failure.retry_disposition === "correct_and_retry",
  );
  await assert.rejects(
    () => new DurableEngine(
      store,
      executor,
      { ...clock, source_generation: `sha256:${"A".repeat(64)}` },
      transport,
    ).observeClock("run:invalid-clock"),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.phase === "observe_clock"
      && error.failure.code === "clock_request_validation_failed"
      && error.failure.retry_disposition === "correct_and_retry",
  );
  assert.deepEqual(calls, {
    seal: 0,
    observeClock: 0,
    executeDurable: 0,
    executeLiveEvolution: 0,
  });

  const durable = new DurableEngine(
    store,
    executor,
    clock,
    transport,
    "evolution:custom-transport",
  );
  await assert.rejects(
    () => durable.runCurrent("run:forged-read", null),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
  await assert.rejects(
    () => durable.cancel("cancel:forged-mutation", "run:forged-mutation", null),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  const definition = new FlowBuilder("custom-transport-evolution", {}, {})
    .finish({ kind: "input" }).definitions[0]!;
  const liveCommand = LiveEvolutionControlBuilder.publishDefinition(
    "command:custom-transport-evolution",
    "module:custom-transport-evolution",
    definition,
    [],
  );
  evolutionResult = evolutionCommit(
    "evolution:forged-other",
    liveCommand,
    {
      result: "definition_published",
      revision: {
        revision_version: "cymule.subflow-revision/2",
        revision_id: contentId("4"),
        logical_ref: "module:custom-transport-evolution",
        sequence: 1,
        definition,
        references: [],
      },
    },
  );
  await assert.rejects(
    () => durable.evolve(liveCommand),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );

  durableRejection = new Error("custom read transport rejected");
  await assert.rejects(
    () => durable.runCurrent("run:rejected-read", null),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.phase === "transport"
      && error.failure.code === "engine_transport_failed"
      && error.failure.message === "custom read transport rejected",
  );
  durableRejection = new Error("custom mutation transport rejected");
  await assert.rejects(
    () => durable.cancel("cancel:rejected-mutation", "run:rejected-mutation", null),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.phase === "transport"
      && error.failure.code === "engine_transport_failed"
      && error.failure.retry_disposition === "reconcile",
  );
  durableRejection = new EngineError({
    category: "unknown_world_outcome",
    phase: "transport",
    code: "forged_retry_matrix",
    message: "the custom transport forged an unsafe retry disposition",
    retry_disposition: "retry_same_request",
  });
  await assert.rejects(
    () => durable.runCurrent("run:forged-engine-error", null),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === undefined,
  );
  await assert.rejects(
    () => durable.cancel(
      "cancel:forged-engine-error",
      "run:forged-engine-error",
      null,
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  const validFailure = {
    category: "validation" as const,
    phase: "validate_request" as const,
    code: "custom_validation",
    message: "the custom transport returned a valid structured failure",
    retry_disposition: "correct_and_retry" as const,
  };
  durableRejection = new EngineError(validFailure);
  await assert.rejects(
    () => durable.runCurrent("run:valid-engine-error", null),
    (error: unknown) => {
      if (!(error instanceof EngineError)) return false;
      assert.deepEqual(error.failure, validFailure);
      return true;
    },
  );
  evolutionRejection = new Error("custom evolution transport rejected");
  await assert.rejects(
    () => durable.evolve(liveCommand),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "engine_transport_failed"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript rejects every legacy or uppercase Evolution Plan identity before transport", async () => {
  let calls = 0;
  const transport: EngineTransport = {
    async seal(): Promise<never> {
      calls += 1;
      throw new Error("invalid Evolution command reached seal");
    },
    async observeClock(): Promise<never> {
      calls += 1;
      throw new Error("invalid Evolution command reached Clock");
    },
    async executeDurable(): Promise<never> {
      calls += 1;
      throw new Error("invalid Evolution command reached durable transport");
    },
    async executeLiveEvolution(): Promise<never> {
      calls += 1;
      throw new Error("invalid Evolution command reached live transport");
    },
  };
  const durable = new DurableEngine(
    directoryStore("unused"),
    undefined,
    undefined,
    transport,
    "evolution:plan-identity-preflight",
  );
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const candidate = new FlowBuilder("evolution-plan-identity", {}, {})
    .finish({ kind: "input" });
  const evidence = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("4"),
    kind: "cymule.evolution-evidence/1",
  };
  const operations = [{
    kind: "add",
    target: "definition:next",
    before: null,
    after: "5".repeat(64),
  }];
  const commandFor = new Map<string, (invalidPlan: string) => LiveEvolutionCommand>([
    ["patch.from_plan", (invalidPlan) => LiveEvolutionControlBuilder.apply(
      "command:invalid-patch-parent",
      "template:plan-identity",
      EvolutionControlBuilder.applyPatch("command:invalid-patch-parent:inner", {
        from_plan: invalidPlan,
        target: candidate,
        operations,
        evidence,
      }),
    )],
    ["rollout.fallback_plan", (invalidPlan) => LiveEvolutionControlBuilder.apply(
      "command:invalid-rollout-fallback",
      "template:plan-identity",
      EvolutionControlBuilder.setRollout("command:invalid-rollout-fallback:inner", {
        decision_id: "decision:plan-identity",
        fallback_plan: invalidPlan,
        target_plan: contentId("2"),
        mode: { mode: "active" },
      }),
    )],
    ["rollout.target_plan", (invalidPlan) => LiveEvolutionControlBuilder.apply(
      "command:invalid-rollout-target",
      "template:plan-identity",
      EvolutionControlBuilder.setRollout("command:invalid-rollout-target:inner", {
        decision_id: "decision:plan-identity",
        fallback_plan: contentId("1"),
        target_plan: invalidPlan,
        mode: { mode: "active" },
      }),
    )],
    ["observation.plan_id", (invalidPlan) => LiveEvolutionControlBuilder.apply(
      "command:invalid-observation-plan",
      "template:plan-identity",
      EvolutionControlBuilder.observe("command:invalid-observation-plan:inner", {
        observation_id: "observation:plan-identity",
        decision_id: "decision:plan-identity",
        occurrence_id: "occurrence:plan-identity",
        plan_id: invalidPlan,
        outcome: "succeeded",
        evidence,
      }),
    )],
  ]);
  for (const [field, build] of commandFor) {
    for (const [variant, invalidPlan] of [
      ["legacy", "plan:legacy"],
      ["uppercase", `sha256:${"A".repeat(64)}`],
    ] as const) {
      await assert.rejects(
        () => durable.evolve(build(invalidPlan)),
        (error: unknown) => error instanceof EngineError
          && error.failure.category === "validation"
          && error.failure.phase === "execute_live_evolution"
          && error.failure.code === "evolution_request_validation_failed"
          && error.failure.retry_disposition === "correct_and_retry",
        `${field} accepted a ${variant} Plan identity`,
      );
    }
  }
  assert.equal(calls, 0, "invalid Evolution Plan identities reached the custom transport");
});

test("TypeScript rejects malformed Resource integrity relationships", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const manifestId = (rootDigest: string, size: number, entryCount: number) => {
    const identity = JSON.stringify({
      entry_count: entryCount,
      media_type: "application/vnd.cymule.resource-manifest+jsonl",
      root_digest: rootDigest,
      size,
    });
    return `sha256:${createHash("sha256")
      .update("cymule.resource-manifest/3\0", "utf8")
      .update(identity, "utf8")
      .digest("hex")}`;
  };
  const inline = ResourceBuilder.text("value");
  assert.equal("annotations" in inline, false);
  const emptyDigest = "sha256:936574b81c099aaf0318e91b80912c958a0527ee271eabd62da35fed55607928";
  const emptyRoot = "sha256:6a754fadbb296b87040c37dab30caea63de1bd1a85142bc82a03a7cf82e64dfc";
  const emptyManifest = {
    manifest_version: "cymule.resource-manifest/3" as const,
    media_type: "application/vnd.cymule.resource-manifest+jsonl" as const,
    digest: emptyDigest,
    size: 0,
    entry_count: 0,
    root_digest: emptyRoot,
  };
  const emptyCandidate = ResourceBuilder.external(
    "collection",
    "application/octet-stream",
    { kind: "content", digest: emptyDigest, size: 0 },
    emptyManifest,
  );
  assert.deepEqual(
    await withSuccessEngine(
      {
        type: "sealed_resource",
        resource: { resource_id: contentId("4"), ...emptyCandidate },
      },
      (engine) => engine.sealResource(emptyCandidate),
    ),
    { resource_id: contentId("4"), ...emptyCandidate },
  );
  const manifestRoot = contentId("2");
  const manifestDigest = manifestId(manifestRoot, 10, 1);
  const nonEmptyManifest = {
    manifest_version: "cymule.resource-manifest/3" as const,
    media_type: "application/vnd.cymule.resource-manifest+jsonl" as const,
    digest: manifestDigest,
    size: 10,
    entry_count: 1,
    root_digest: manifestRoot,
  };
  const nonEmptyCandidate = ResourceBuilder.external(
    "collection",
    "application/octet-stream",
    { kind: "content", digest: manifestDigest, size: 10 },
    nonEmptyManifest,
  );
  assert.deepEqual(
    await withSuccessEngine(
      {
        type: "sealed_resource",
        resource: { resource_id: contentId("5"), ...nonEmptyCandidate },
      },
      (engine) => engine.sealResource(nonEmptyCandidate),
    ),
    { resource_id: contentId("5"), ...nonEmptyCandidate },
  );
  const invalid = [
    { ...inline, annotations: {} },
    {
      ...emptyCandidate,
      manifest: {
        ...emptyManifest,
        manifest_version: "cymule.resource-manifest/2",
        root_digest: "sha256:b6009c22e4a61a949312181d089c38194269a3aa38098801fa38a6d8307050a3",
      },
    },
    { ...inline, inline: { encoding: "utf8" } },
    {
      resource_version: "cymule.resource/3",
      shape: "object",
      media_type: "application/octet-stream",
      inline: { encoding: "utf8", text: "forged" },
      integrity: { kind: "content", digest: contentId("1"), size: 6 },
      annotations: {},
    },
    {
      resource_version: "cymule.resource/3",
      shape: "collection",
      media_type: "application/octet-stream",
      integrity: { kind: "content", digest: contentId("1"), size: 10 },
      manifest: {
        manifest_version: "cymule.resource-manifest/3",
        media_type: "application/vnd.cymule.resource-manifest+jsonl",
        digest: contentId("2"),
        size: 10,
        entry_count: 1,
        root_digest: contentId("3"),
      },
      annotations: {},
    },
    {
      ...emptyCandidate,
      integrity: { kind: "content", digest: contentId("7"), size: 0 },
      manifest: { ...emptyManifest, digest: contentId("7") },
    },
    {
      ...emptyCandidate,
      manifest: { ...emptyManifest, root_digest: contentId("8") },
    },
    { ...inline, media_type: "Text/Plain" },
    {
      resource_version: "cymule.resource/3",
      shape: "object",
      media_type: "application/octet-stream",
      integrity: { kind: "version", authority: "", version: "v1" },
      annotations: {},
    },
    { ...inline, annotations: { key: "x".repeat(4_097) } },
    { ...inline, inline: { encoding: "base64", data: "YQ" } },
  ];
  for (const [index, candidate] of invalid.entries()) {
    await assert.rejects(
      () => withSuccessEngine(
        {
          type: "sealed_resource",
          resource: { resource_id: contentId(String((index % 9) + 1)), ...candidate },
        },
        (engine) => engine.sealResource(candidate as never),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }
});

test("TypeScript accepts typed Effect execution boundaries", async () => {
  const planId = `sha256:${"1".repeat(64)}`;
  const intentId = `sha256:${"2".repeat(64)}`;
  const plan = {
    plan_id: planId,
    candidate: new FlowBuilder("effect-boundary", {}, {}).finish({ kind: "input" }),
  };
  const executions = [
    {
      status: "release_required",
      release: {
        run_id: "run:test",
        plan_id: planId,
        intent_ids: [intentId],
      },
    },
    {
      status: "reconciliation_required",
      reconciliation: {
        run_id: "run:test",
        plan_id: planId,
        intent_id: intentId,
      },
    },
  ];
  for (const [index, execution] of executions.entries()) {
    const directory = mkdtempSync(join(tmpdir(), "cymule-sdk-"));
    const executable = join(directory, `effect-boundary-${index}`);
    writeFileSync(
      executable,
      `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  const response = JSON.parse(${JSON.stringify(JSON.stringify({
        type: "execution_boundary",
        execution,
      }))});
  process.stdout.write(JSON.stringify({
    engine_protocol: "cymule.engine/5",
    outcome: "success",
    request,
    response,
  }));
});
`,
    );
    chmodSync(executable, 0o700);
    try {
      assert.equal(
        (await new CliEngine(executable).run(
          plan,
          null,
          processTarget(process.execPath),
          "run:test",
        )).status,
        execution.status,
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
});

test("TypeScript candidate seals and executes through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const pluginPath = process.env.CYMULE_TEST_PLUGIN;
  const expectedPlanId = process.env.CYMULE_EXPECTED_PLAN_ID;
  if (enginePath === undefined || pluginPath === undefined || expectedPlanId === undefined) {
    context.skip("cross-language binaries are not configured");
    return;
  }

  const candidate = new FlowBuilder("cross_language_echo", {}, {})
    .component("test.echo", {}, {}, "cymule.component-output/1", {})
    .effectContract("test.capture", {}, {}, profile, {})
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
      "observed",
    )
    .scope(
      "scope.finalize",
      { steps: [], result: { kind: "literal", value: null } },
      "scope_result",
    )
    .finish({ kind: "binding", name: "echoed" });

  const engine = new CliEngine(enginePath);
  const plan = await engine.seal(candidate);
  assert.equal(plan.plan_id, expectedPlanId);
  const input = { message: "hello from TypeScript" };
  const plugin = processTarget(pluginPath);
  const execution = await engine.run(plan, input, plugin, "run:typescript-e2e");
  assert.equal(execution.status, "completed");
  if (execution.status !== "completed") throw new Error("expected terminal execution");
  const result = execution.result;
  assert.deepEqual(result.value, input);
  assert.equal(result.effects.length, 1);

  const store = mkdtempSync(join(tmpdir(), "cymule-ts-durable-"));
  try {
    const target = sqliteStore(join(store, "domain.sqlite"), "sdk-typescript");
    const clock = sqliteClock(
      join(store, "clock.sqlite"),
      "clock:sdk-typescript",
      `sha256:${"3".repeat(64)}`,
    );
    const durable = new DurableEngine(target, plugin, clock, engine);
    const clockRef = await durable.observeClock("run:typescript-durable-e2e");
    const laterClockRef = await durable.observeClock("run:typescript-durable-e2e");
    assert.notEqual(laterClockRef.observation_id, clockRef.observation_id);
    assert.equal((await durable.start("run:typescript-durable-e2e", candidate, input, {
      owner: "driver:sdk-typescript",
      clock: laterClockRef,
      ttl: 30,
    })).type, "run_boundary");
    assert.notEqual(
      (await new DurableEngine(target, undefined, undefined, engine).runCurrent(
        "run:typescript-durable-e2e",
        null,
      )).current,
      null,
    );
    assert.equal(
      (await durable.evolve(
        LiveEvolutionControlBuilder.publishDefinition(
          "evolve:typescript:publish",
          "definition:typescript:echo",
          candidate.definitions[0]!,
          [],
        ),
      )).receipt.outcome.result,
      "definition_published",
    );
  } finally {
    rmSync(store, { recursive: true, force: true });
  }
});

test("root README current-source API remains executable", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const pluginPath = process.env.CYMULE_TEST_PLUGIN;
  if (enginePath === undefined || pluginPath === undefined) {
    context.skip("README source-checkout binaries are not configured");
    return;
  }
  const quickstartLedgerPath = resolve("quickstart-effect-ledger.sqlite3");
  context.after(() => rmSync(quickstartLedgerPath, { force: true }));
  const captureProfile: EffectProfile = {
    mutation: "mutating",
    dispatch: "on_scope_commit",
    reconciliation: "queryable",
    keyed_idempotency: true,
    irreversible: false,
  };
  const candidate = new FlowBuilder("echo_and_capture", {}, {})
    .component("test.echo", {}, {}, "cymule.component-output/1", {})
    .effectContract("test.capture", {}, {}, captureProfile, {})
    .call("call.echo", "test.echo", { kind: "input" }, "echoed")
    .effect(
      "effect.capture",
      "test.capture",
      { kind: "binding", name: "echoed" },
      "primary",
    )
    .finish({ kind: "binding", name: "echoed" });
  const engine = new CliEngine(enginePath);
  const plan = await engine.seal(candidate);
  const execution = await engine.run(
    plan,
    { message: "README" },
    processPlugin({
      executable: pluginPath,
      arguments: [],
      environment: {
        CYMULE_TEST_EFFECT_LEDGER_PATH: quickstartLedgerPath,
      },
      working_directory: null,
      runtime_closure: { "component-runtime": `sha256:${"a".repeat(64)}` },
      timeout_ms: 60_000,
      message_limit: 8 * 1024 * 1024,
      closure_limit: 64 * 1024 * 1024,
    }),
    "run:readme-source",
  );
  assert.equal(execution.status, "completed");
  if (execution.status !== "completed") {
    throw new Error("README Effect fixture did not complete after the root scope committed");
  }
  assert.deepEqual(execution.result.value, { message: "README" });
  assert.equal(execution.result.effects.length, 1);

  const resource = await engine.sealResource(ResourceBuilder.text("README resource"));
  assert.match(resource.resource_id, /^sha256:[0-9a-f]{64}$/);
  const artifact = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: `sha256:${"0".repeat(64)}`,
    kind: `cymule.typed-json/sha256-${"1".repeat(64)}`,
  };
  const handoff = ResourceBuilder.handoff(
    "transfer:readme",
    {
      run_id: "run:readme-producer",
      occurrence_id: `sha256:${"5".repeat(64)}`,
      result: artifact,
    },
    "run:readme-consumer",
    "input.dataset",
    artifact,
  );
  assert.deepEqual(handoff.resource, artifact);
});

test("TypeScript rejects a malicious nested Engine success", async (context) => {
  const malicious = process.env.CYMULE_MALICIOUS_ENGINE;
  if (malicious === undefined) {
    context.skip("malicious Engine conformance is not configured");
    return;
  }
  await assert.rejects(
    () => new DurableEngine(
      "unused",
      undefined,
      undefined,
      new CliEngine(malicious),
    ).runCurrent("run:fake", null),
    (error: unknown) => error instanceof EngineError &&
      error.failure.code === "invalid_engine_response" &&
      error.failure.category === "transport_failure",
  );
});

test("TypeScript reconciles a malicious Effect release success", async (context) => {
  const malicious = process.env.CYMULE_MALICIOUS_EFFECT_ENGINE;
  if (malicious === undefined) {
    context.skip("malicious Effect Engine conformance is not configured");
    return;
  }
  const plan = {
    plan_id: `sha256:${"a".repeat(64)}`,
    candidate: new FlowBuilder("malicious-effect", {}, {}).finish({ kind: "input" }),
  };
  await assert.rejects(
    () => new CliEngine(malicious).run(
      plan,
      null,
      processTarget(process.execPath),
      "run:malicious-effect",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "invalid_engine_response"
      && error.failure.category === "unknown_world_outcome"
      && error.failure.retry_disposition === "reconcile",
  );
});

test("TypeScript resource seals through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const expectedResourceId = process.env.CYMULE_EXPECTED_RESOURCE_ID;
  if (enginePath === undefined || expectedResourceId === undefined) {
    context.skip("Resource Engine conformance is not configured");
    return;
  }

  const resource = await new CliEngine(enginePath).sealResource(
    ResourceBuilder.text("shared cross-run resource", {
      purpose: "cross-language-conformance",
    }),
  );
  assert.equal(resource.resource_id, expectedResourceId);
  assert.equal(resource.integrity.kind, "inline");
  assert.equal(
    (await new CliEngine(enginePath).sealResource(ResourceBuilder.text("empty annotations"))).shape,
    "inline",
  );
});

test("TypeScript wait activation validates through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_WAIT_ACTIVATION_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) {
    context.skip("wait activation Engine conformance is not configured");
    return;
  }
  const activation = WaitActivationBuilder.signal(
    "activation:shared:1",
    "signal:continue",
    ["sha256:8d55f9d1981f4579ce12d106f25d85307ed27db86a4c106bbe17cb0ea8e9acc5"],
    {
      identity_version: "cymule.artifact/2",
      artifact_id: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      kind: "cymule.wait-result/1",
    },
  );
  assert.deepEqual(activation, JSON.parse(readFileSync(fixturePath, "utf8")));
  assert.deepEqual(await new CliEngine(enginePath).verifyWaitActivation(activation), activation);
});

test("TypeScript durable control fixture validates through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_DURABLE_CONTROL_FIXTURE;
  const cancelFixturePath = process.env.CYMULE_DURABLE_CANCEL_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined || cancelFixturePath === undefined) {
    context.skip("durable control Engine conformance is not configured");
    return;
  }
  const command = DurableControlBuilder.takeoverRun(
    "run:cross-language",
    7,
    fixtureExecution(),
  );
  assert.deepEqual(command, JSON.parse(readFileSync(fixturePath, "utf8")));
  assert.deepEqual(await new CliEngine(enginePath).verifyDurableCommand(command), command);
  const cancel = DurableControlBuilder.cancelRun(
    "cancel:cross-language",
    "run:cross-language",
    { code: "operator_request" },
  );
  assert.deepEqual(cancel, JSON.parse(readFileSync(cancelFixturePath, "utf8")));
  assert.deepEqual(await new CliEngine(enginePath).verifyDurableCommand(cancel), cancel);

  const activation = DurableControlBuilder.activateSignal(
    "activation:sdk",
    "signal:sdk",
    [`sha256:${"b".repeat(64)}`, `sha256:${"a".repeat(64)}`, `sha256:${"b".repeat(64)}`],
    { accepted: true },
  );
  assert.equal(activation.type, "activate_wait");
  if (activation.type !== "activate_wait") throw new Error("activation builder returned wrong type");
  assert.deepEqual(activation.wait_ids, [`sha256:${"a".repeat(64)}`, `sha256:${"b".repeat(64)}`]);
});

test("TypeScript accepts every shared terminal durable boundary", async (context) => {
  const fixturePath = process.env.CYMULE_DURABLE_TERMINAL_FIXTURE;
  if (fixturePath === undefined) {
    context.skip("durable terminal fixture is not configured");
    return;
  }
  const responses = JSON.parse(readFileSync(fixturePath, "utf8")) as unknown[];
  for (const [index, response] of responses.entries()) {
    const directory = mkdtempSync(join(tmpdir(), "cymule-terminal-"));
    const executable = join(directory, "engine");
    writeFileSync(
      executable,
      `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  const response = JSON.parse(${JSON.stringify(JSON.stringify({
        type: "durable_executed",
        response,
      }))});
  process.stdout.write(JSON.stringify({
    engine_protocol: "cymule.engine/5",
    outcome: "success",
    request,
    response,
  }));
});
`,
    );
    chmodSync(executable, 0o700);
    try {
      const command = index === 0
        ? DurableControlBuilder.resumeRun("run:terminal", fixtureExecution())
        : index === 1
        ? DurableControlBuilder.cancelRun(
            "cancel:fixture",
            "run:fixture:cancelled",
            { code: "fixture_cancelled" },
          )
        : DurableControlBuilder.releaseEffect(
            index === 2
              ? `sha256:${"2".repeat(64)}`
              : String((response as { boundary: { intent_id: string } }).boundary.intent_id),
            fixtureExecution(),
          );
      assert.deepEqual(
        await new CliEngine(executable).executeDurable(
          durableTargetFor(command),
          command,
        ),
        response,
      );
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
});

test("TypeScript Applied Effect summaries require canonical result Artifacts", async (t) => {
  const fixturePath = process.env.CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE;
  if (!fixturePath) {
    t.skip("Applied Effect summary conformance is not configured");
    return;
  }
  const fixture = JSON.parse(readFileSync(fixturePath, "utf8"));
  const command = DurableControlBuilder.runEffectPage(fixture.run_id, {
    expected_revision: null, cursor: null, limit: 1, max_canonical_bytes: 1024 * 1024,
  });
  for (const [label, state, result, accepted] of [
    ["applied canonical null Artifact", "applied", fixture.result, true],
    ["applied missing Artifact", "applied", null, false],
    ["not applied absence", "not_applied", null, true],
    ["not applied unexpected Artifact", "not_applied", fixture.result, false],
  ] as const) {
    const response = {
      type: "run_effect_page", run_id: fixture.run_id,
      page: {
        observed_revision: `sha256:${"5".repeat(64)}`, source_root: "6".repeat(64),
        items: [{ ...fixture, state, result }], next_cursor: null,
      },
    };
    const invoke = () => withSuccessEngine(
      { type: "durable_executed", response },
      (engine) => engine.executeDurable({ store: directoryStore("unused") }, command),
    );
    if (accepted) assert.deepEqual(await invoke(), response, label);
    else await assert.rejects(
      invoke,
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "invalid_engine_response",
      label,
    );
  }
});

test("TypeScript closes every durable-control/4 query shape", async () => {
  const contentId = (digit: string) => "sha256:" + digit.repeat(64);
  const pageKeyHash = (key: string) => {
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
    frame(Buffer.from(key, "utf8"));
    return hasher.digest("hex");
  };
  const revision = contentId("9");
  const sourceRoot = "8".repeat(64);
  const runId = "run:query";
  const binding = {
    identity_version: "cymule.artifact/2" as const,
    artifact_id: contentId("7"),
    kind: "cymule.execution-binding/2",
  };
  const current = {
    run_id: runId,
    plan_id: contentId("6"),
    execution_binding: binding,
    continuation_status: "ready",
    epoch: 0,
    execution_fence: 0,
    result: null,
    execution_status: { status: "active" },
    world_settlement: "settled",
  };
  const currentQuery = DurableControlBuilder.runCurrent(runId, null);
  const executeCurrent = (response: unknown) => withSuccessEngine(
    { type: "durable_executed", response },
    (engine) => engine.executeDurable({ store: directoryStore("unused") }, currentQuery),
  );
  assert.deepEqual(
    await executeCurrent({
      type: "run_current",
      observed_revision: revision,
      source_root: sourceRoot,
      current,
    }),
    {
      type: "run_current",
      observed_revision: revision,
      source_root: sourceRoot,
      current,
    },
  );
  assert.equal((await executeCurrent({
    type: "run_current",
    observed_revision: revision,
    source_root: sourceRoot,
    current: null,
  })).type, "run_current");
  await assert.rejects(
    () => executeCurrent({
      type: "run_current",
      observed_revision: revision,
      source_root: sourceRoot,
    }),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "invalid_engine_response",
    "missing required-null current was admitted",
  );

  const summaries = ["run:a", "run:b"].map((summaryRunId) => ({
    run_id: summaryRunId,
    continuation_status: "ready",
    execution_status: { status: "active" },
    world_settlement: "settled",
  })).sort((left, right) => {
    const leftHash = pageKeyHash(left.run_id);
    const rightHash = pageKeyHash(right.run_id);
    return leftHash === rightHash
      ? left.run_id.localeCompare(right.run_id)
      : leftHash.localeCompare(rightHash);
  });
  const terminal = summaries.at(-1)!;
  const cursor = {
    query_kind: "run_index" as const,
    run_id: null,
    source_revision: revision,
    source_root: sourceRoot,
    position: {
      canonical_key: terminal.run_id,
      key_hash: pageKeyHash(terminal.run_id),
    },
  };
  const indexQuery = DurableControlBuilder.runIndexPage({
    expected_revision: null,
    cursor: null,
    limit: 2,
    max_canonical_bytes: 1024 * 1024,
  });
  const executeIndex = (page: unknown) => withSuccessEngine(
    { type: "durable_executed", response: { type: "run_index_page", page } },
    (engine) => engine.executeDurable({ store: directoryStore("unused") }, indexQuery),
  );
  const validPage = {
    observed_revision: revision,
    source_root: sourceRoot,
    items: summaries,
    next_cursor: cursor,
  };
  assert.deepEqual(
    await executeIndex(validPage),
    { type: "run_index_page", page: validPage },
  );
  for (const [label, page] of [
    ["missing required-null next cursor", {
      observed_revision: revision,
      source_root: sourceRoot,
      items: summaries,
    }],
    ["wrong terminal cursor hash", {
      ...validPage,
      next_cursor: {
        ...cursor,
        position: { ...cursor.position, key_hash: "0".repeat(64) },
      },
    }],
    ["non-authenticated item order", {
      ...validPage,
      items: [...summaries].reverse(),
      next_cursor: null,
    }],
  ] as const) {
    await assert.rejects(
      () => executeIndex(page),
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "invalid_engine_response",
      label,
    );
  }

  const continuedQuery = DurableControlBuilder.runIndexPage({
    expected_revision: revision,
    cursor,
    limit: 2,
    max_canonical_bytes: 1024 * 1024,
  });
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "durable_executed",
        response: {
          type: "run_index_page",
          page: {
            observed_revision: revision,
            source_root: sourceRoot,
            items: [terminal],
            next_cursor: null,
          },
        },
      },
      (engine) => engine.executeDurable(
        { store: directoryStore("unused") },
        continuedQuery,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "invalid_engine_response",
    "continued page did not advance past its cursor",
  );

  const waitId = contentId("5");
  const waitQuery = DurableControlBuilder.runWaitPage(runId, {
    expected_revision: null,
    cursor: null,
    limit: 1,
    max_canonical_bytes: 1024 * 1024,
  });
  const waitSummary = { wait_id: waitId, run_id: runId, state: "pending", result: null };
  const executeWait = (item: unknown) => withSuccessEngine(
    {
      type: "durable_executed",
      response: {
        type: "run_wait_page",
        run_id: runId,
        page: {
          observed_revision: revision,
          source_root: sourceRoot,
          items: [item],
          next_cursor: null,
        },
      },
    },
    (engine) => engine.executeDurable({ store: directoryStore("unused") }, waitQuery),
  );
  assert.equal((await executeWait(waitSummary)).type, "run_wait_page");
  const { result: _result, ...missingResult } = waitSummary;
  await assert.rejects(
    () => executeWait(missingResult),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "invalid_engine_response",
    "missing required-null wait result was admitted",
  );

  const itemQuery = DurableControlBuilder.runItem({
    run_id: runId,
    expected_revision: null,
    selector: { kind: "wait", wait_id: waitId },
    max_canonical_bytes: 13 * 1024 * 1024,
  });
  assert.equal((await withSuccessEngine(
    {
      type: "durable_executed",
      response: {
        type: "run_item",
        run_id: runId,
        observed_revision: revision,
        source_root: sourceRoot,
        item: null,
      },
    },
    (engine) => engine.executeDurable({ store: directoryStore("unused") }, itemQuery),
  )).type, "run_item");
  assert.throws(() => DurableControlBuilder.runIndexPage({
    expected_revision: null,
    cursor,
    limit: 1,
    max_canonical_bytes: 1024 * 1024,
  }));
  await assert.rejects(
    () => withSuccessEngine(
      { type: "run_boundary", boundary: { status: "completed", result: null } },
      (engine) => engine.executeDurable(
        { store: directoryStore("unused") },
        {
          type: "query_run",
          control_version: "cymule.durable-control/3",
          query_id: "query:legacy",
          run_id: runId,
        } as unknown as DurableCommand,
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation",
    "legacy full-Run query was admitted",
  );
});

test("TypeScript rejects inconsistent durable sets and dispositions", async () => {
  const effectIntent = `sha256:${"d".repeat(64)}`;
  const otherEffectIntent = `sha256:${"e".repeat(64)}`;
  const activation = DurableControlBuilder.activateSignal(
    "activation:invalid-set",
    "signal:invalid-set",
    [`sha256:${"a".repeat(64)}`],
    null,
  );
  if (activation.type !== "activate_wait") {
    throw new Error("activation builder returned the wrong command variant");
  }
  const activationReceipt = {
    receipt_version: "cymule.wait-activation-receipt/3",
    activation: {
      activation_version: "cymule.wait-activation/2",
      activation_id: activation.activation_id,
      source: activation.source,
      wait_ids: activation.wait_ids,
      result: {
        identity_version: "cymule.artifact/2",
        artifact_id: `sha256:${"c".repeat(64)}`,
        kind: "cymule.wait-result/1",
      },
    },
    applied_wait_ids: activation.wait_ids,
    ready_run_ids: ["run:ready"],
  };
  assert.deepEqual(
    await withSuccessEngine(
      {
        type: "durable_executed",
        response: { type: "wait_activated", receipt: activationReceipt },
      },
      (engine) => engine.executeDurable(durableTargetFor(activation), activation),
    ),
    { type: "wait_activated", receipt: activationReceipt },
  );
  const mutationFailure = async (
    response: unknown,
    command: Parameters<CliEngine["executeDurable"]>[1],
  ) =>
    await assert.rejects(
      () => withSuccessEngine(
        { type: "durable_executed", response },
        (engine) => engine.executeDurable(durableTargetFor(command), command),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
    );
  await mutationFailure({
    type: "wait_activated",
    receipt: {
      receipt_version: "cymule.wait-activation-receipt/3",
      activation: {
        activation_version: "cymule.wait-activation/2",
        activation_id: activation.activation_id,
        source: activation.source,
        wait_ids: activation.wait_ids,
        result: {
          identity_version: "cymule.artifact/2",
          artifact_id: `sha256:${"a".repeat(64)}`,
          kind: "cymule.wait-result/1",
        },
      },
      applied_wait_ids: [],
      ready_run_ids: ["run:a"],
    },
  }, activation);
  await mutationFailure({
    type: "wait_activated",
    receipt: {
      receipt_version: "cymule.wait-activation-receipt/3",
      activation: {
        activation_version: "cymule.wait-activation/2",
        activation_id: activation.activation_id,
        source: activation.source,
        wait_ids: activation.wait_ids,
        result: {
          identity_version: "cymule.artifact/2",
          artifact_id: `sha256:${"b".repeat(64)}`,
          kind: "cymule.wait-result/1",
        },
      },
      applied_wait_ids: activation.wait_ids,
      ready_run_ids: ["run:b", "run:a"],
    },
  }, activation);
  await mutationFailure({
    type: "run_boundary",
    boundary: { status: "release_required", intent_ids: [effectIntent, effectIntent] },
  }, DurableControlBuilder.releaseEffect(effectIntent, fixtureExecution()));
  await mutationFailure({
    type: "run_boundary",
    boundary: { status: "reconciliation_required", intent_id: otherEffectIntent },
  }, DurableControlBuilder.releaseEffect(effectIntent, fixtureExecution()));
  await mutationFailure({
    type: "run_boundary",
    boundary: { status: "release_required", intent_ids: [otherEffectIntent] },
  }, DurableControlBuilder.releaseEffect(effectIntent, fixtureExecution()));
  const effectNotApplied = {
    type: "run_boundary" as const,
    boundary: { status: "effect_not_applied" as const, intent_id: effectIntent },
  };
  const release = DurableControlBuilder.releaseEffect(effectIntent, fixtureExecution());
  assert.deepEqual(
    await withSuccessEngine(
      { type: "durable_executed", response: effectNotApplied },
      (engine) => engine.executeDurable(durableTargetFor(release), release),
    ),
    effectNotApplied,
  );
  await mutationFailure({
    type: "run_boundary",
    boundary: { status: "effect_not_applied", intent_id: "effect:not-content-addressed" },
  }, release);
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "durable_executed",
        response: {
          type: "run_index_page",
          page: {
            observed_revision: `sha256:${"a".repeat(64)}`,
            source_root: "b".repeat(64),
            items: ["run:a", "run:a"].map((run_id) => ({
              run_id,
              continuation_status: "ready",
              execution_status: { status: "active" },
              world_settlement: "settled",
            })),
            next_cursor: null,
          },
        },
      },
      (engine) => engine.executeDurable(
        { store: directoryStore("unused") },
        DurableControlBuilder.runIndexPage({
          expected_revision: null,
          cursor: null,
          limit: 2,
          max_canonical_bytes: 1024 * 1024,
        }),
      ),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
});

test("TypeScript rejects malformed post-plugin execution evidence", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const builder = new FlowBuilder("execution-evidence", {}, {});
  builder.wait(
    "wait:site",
    { kind: "signal", key: "signal:wait", consume_once: true },
    "answer",
  );
  const plan = {
    plan_id: contentId("1"),
    candidate: builder.finish({ kind: "binding", name: "answer" }),
  };
  const completed = {
    status: "completed",
    result: {
      run_id: "run:evidence",
      plan_id: plan.plan_id,
      value: null,
      projection_digest: "2".repeat(64),
      precondition_token: `pre:1:sha256:${"4".repeat(64)}`,
      effects: [contentId("3")],
    },
  };
  const suspended = {
    status: "suspended",
    suspension: {
      run_id: "run:evidence",
      plan_id: plan.plan_id,
      definition_id: "main",
      invocation_id: contentId("4"),
      site_id: "wait:site",
      wait: { kind: "signal", key: "signal:wait", consume_once: true },
      result_bind: "answer",
    },
  };
  const invalid = [
    { ...completed, result: { ...completed.result, plan_id: "A".repeat(64) } },
    { ...completed, result: { ...completed.result, projection_digest: "not-a-digest" } },
    { ...completed, result: { ...completed.result, projection_digest: "A".repeat(64) } },
    { ...completed, result: { ...completed.result, precondition_token: "é".repeat(300) } },
    {
      ...completed,
      result: {
        ...completed.result,
        precondition_token: `pre:9007199254740992:sha256:${"4".repeat(64)}`,
      },
    },
    {
      ...completed,
      result: {
        ...completed.result,
        precondition_token: `pre:01:sha256:${"4".repeat(64)}`,
      },
    },
    { ...completed, result: { ...completed.result, effects: [contentId("3"), contentId("3")] } },
    { ...completed, result: { ...completed.result, effects: [`sha256:${"A".repeat(64)}`] } },
    {
      ...suspended,
      suspension: { ...suspended.suspension, invocation_id: "invocation:not-content" },
    },
    {
      status: "release_required",
      release: {
        run_id: "run:evidence",
        plan_id: plan.plan_id,
        intent_ids: [contentId("5"), contentId("5")],
      },
    },
    {
      status: "reconciliation_required",
      reconciliation: {
        run_id: "run:evidence",
        plan_id: plan.plan_id,
        intent_id: "intent:not-content",
      },
    },
  ];
  for (const execution of invalid) {
    await assert.rejects(
      () => withSuccessEngine(
        { type: "execution_boundary", execution },
        (engine) => engine.run(
          plan,
          null,
          processTarget(process.execPath),
          "run:evidence",
        ),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
    );
  }

  for (const result of [
    { ...completed.result, plan_id: "plan:forged" },
    { ...completed.result, projection_digest: `sha256:${"2".repeat(64)}` },
    {
      ...completed.result,
      precondition_token: `pre:9007199254740992:sha256:${"4".repeat(64)}`,
    },
    { ...completed.result, effects: [`sha256:${"A".repeat(64)}`] },
  ]) {
    await assert.rejects(
      () => withSuccessEngine(
        {
          type: "durable_executed",
          response: {
            type: "run_boundary",
            boundary: { status: "completed", result },
          },
        },
        (engine) => {
          const resume = DurableControlBuilder.resumeRun("run:evidence", fixtureExecution());
          return engine.executeDurable(durableTargetFor(resume), resume);
        },
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
    );
  }
});

test("TypeScript preserves the complete virtual compaction certificate wire", () => {
  const certificate: VirtualCompactionCertificate = {
    certificate_version: "cymule.virtual-compaction-certificate/4",
    certificate_id: `sha256:${"1".repeat(64)}`,
    source_causal_cut: ["virtual:terminal"],
    summary: {
      region_id: "region:terminal",
      run_id: "run:terminal",
      occurrence_count: 1,
      work_count: 1,
      succeeded_count: 1,
      failed_count: 0,
      cancelled_count: 0,
      output_digest: "2".repeat(64),
      evidence_digest: "3".repeat(64),
      retained_debug_index_digest: "4".repeat(64),
    },
    summary_state_digest: "5".repeat(64),
    occurrence_root_digest: `sha256:${"6".repeat(64)}`,
    parent_work_index_root_digest: `sha256:${"7".repeat(64)}`,
    work_index_updates_digest: "8".repeat(64),
    work_index_root_digest: `sha256:${"9".repeat(64)}`,
    command_root_digest: null,
    command_count: 0,
    unresolved_obligations: [],
    retained_execution_bindings: [{
      identity_version: "cymule.artifact/2",
      artifact_id: `sha256:${"a".repeat(64)}`,
      kind: "cymule.execution-binding/2",
    }],
    replay_availability: { status: "exact" },
    rehydration_manifest: {
      resource_version: "cymule.resource/3",
      resource_id: `sha256:${"b".repeat(64)}`,
      shape: "object",
      media_type: "application/octet-stream",
      integrity: { kind: "content", digest: "c".repeat(64), size: 0 },
    },
    archive: { binding: "compactor:terminal", revision: "revision:terminal" },
  };
  const wire = JSON.parse(JSON.stringify(certificate)) as Record<string, unknown>;
  assert.equal(wire.command_root_digest, null);
  assert.equal(Object.hasOwn(wire, "command_root_digest"), true);
  for (const field of [
    "parent_work_index_root_digest",
    "work_index_updates_digest",
    "work_index_root_digest",
    "command_root_digest",
    "command_count",
  ]) {
    assert.equal(Object.hasOwn(wire, field), true);
  }
  const { command_root_digest: _requiredNullable, ...missing } = certificate;
  assert.equal(_requiredNullable, null);
  // @ts-expect-error command_root_digest is required even when its value is null.
  const invalid: VirtualCompactionCertificate = missing;
  assert.notDeepEqual(JSON.parse(JSON.stringify(invalid)), wire);
});

test("TypeScript virtual builders enforce closed wire authority", () => {
  const identity = "🧪".repeat(512);
  const clock = { ...fixtureExecution().clock, scope: identity };
  const binding: ArtifactRef = {
    identity_version: "cymule.artifact/2",
    artifact_id: testContentId("2"),
    kind: "cymule.execution-binding/2",
  };
  const evidence = { ...binding, kind: "example/evidence" };
  const resolution = { resolution: "retry" as const, error: evidence, next_reason: null };
  assert.equal(VirtualSchedulingControlBuilder.claim(
    identity, identity, identity, binding, [identity], clock, 30,
  ).owner, identity);
  for (const invalid of ["", "🧪".repeat(513), "id:\u0085", "id:\ud800"]) {
    assert.throws(() => VirtualSchedulingControlBuilder.claim(
      invalid, identity, identity, binding, [], clock, 30,
    ));
    assert.throws(() => VirtualSchedulingControlBuilder.claim(
      identity, invalid, identity, binding, [], clock, 30,
    ));
    assert.throws(() => VirtualSchedulingControlBuilder.claim(
      identity, identity, identity, binding, [invalid], clock, 30,
    ));
    assert.throws(() => VirtualSchedulingControlBuilder.renew(
      identity, invalid, identity, 1, 1, clock, 30,
    ));
    assert.throws(() => VirtualSchedulingControlBuilder.recovery(
      identity, identity, invalid, 1, 1, clock, resolution,
    ));
    assert.throws(() => VirtualWorkControlBuilder.succeed(
      identity, invalid, identity, 1, 1, clock, evidence,
    ));
  }
  for (const invalid of [
    { ...binding, artifact_id: "sha256:not-a-digest" },
    { ...binding, unexpected: true },
  ]) {
    assert.throws(() => VirtualSchedulingControlBuilder.claim(
      identity, identity, identity, invalid, [], clock, 30,
    ));
  }
  for (const invalid of [
    { resolution: "succeeded", result: evidence },
    { resolution: "parked", reason: { kind: "wait", key: "wait:fixture" } },
    { ...resolution, result: evidence },
    { resolution: "retry", error: evidence },
  ]) {
    assert.throws(() => VirtualSchedulingControlBuilder.recovery(
      identity, identity, identity, 1, 1, clock,
      invalid as VirtualRecoveryCommand["resolution"],
    ));
  }
  assert.throws(() => VirtualSchedulingControlBuilder.runWeight(identity, identity, 4_294_967_296));
  VirtualSchedulingControlBuilder.recovery(identity, identity, identity, 1, 1, clock, resolution);
});

test("TypeScript virtual work query and control fixtures stay exact", async (context) => {
  const occurrencePath = process.env.CYMULE_VIRTUAL_OCCURRENCE_FIXTURE;
  const controlPath = process.env.CYMULE_VIRTUAL_CONTROL_FIXTURE;
  const migrationPath = process.env.CYMULE_VIRTUAL_MIGRATION_FIXTURE;
  const compactionPath = process.env.CYMULE_VIRTUAL_COMPACTION_FIXTURE;
  const rehydrationPath = process.env.CYMULE_VIRTUAL_REHYDRATION_FIXTURE;
  const claimPath = process.env.CYMULE_VIRTUAL_CLAIM_FIXTURE;
  const renewalPath = process.env.CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE;
  const recoveryPath = process.env.CYMULE_VIRTUAL_RECOVERY_FIXTURE;
  const runWeightPath = process.env.CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE;
  if (occurrencePath === undefined || controlPath === undefined
    || migrationPath === undefined || compactionPath === undefined
    || rehydrationPath === undefined || claimPath === undefined
    || renewalPath === undefined || recoveryPath === undefined
    || runWeightPath === undefined) {
    context.skip("virtual work fixtures are not configured");
    return;
  }
  const occurrence = JSON.parse(readFileSync(occurrencePath, "utf8"));
  const controlFixture = JSON.parse(readFileSync(controlPath, "utf8"));
  assert.equal(occurrence.execution_binding.kind, "cymule.execution-binding/2");
  const command = VirtualWorkControlBuilder.succeed(
    "command:virtual:fixture:success",
    "work:fixture",
    "worker:fixture",
    1,
    1,
    controlFixture.clock,
    {
      identity_version: "cymule.artifact/2",
      artifact_id: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
      kind: "example/result",
    },
  );
  assert.deepEqual(command, controlFixture);
  const migrationFixture = JSON.parse(readFileSync(migrationPath, "utf8"));
  assert.deepEqual(
    VirtualWorkControlBuilder.migration(
      "command:migration:fixture-split",
      migrationFixture.plan,
    ),
    migrationFixture,
  );
  const compactionFixture = JSON.parse(readFileSync(compactionPath, "utf8"));
  assert.deepEqual(
    VirtualWorkControlBuilder.compaction(
      compactionFixture.command_id,
      "region:fixture",
      ["virtual:fixture:terminal"],
      ["work:fixture"],
      [occurrence.occurrence_id],
      [],
      { binding: "binding:archive/fixture@1", revision: "compactor:fixture/1" },
    ),
    compactionFixture,
  );
  assert.deepEqual(
    VirtualWorkControlBuilder.rehydration(
      "command:rehydration:fixture",
      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      [occurrence.occurrence_id],
    ),
    JSON.parse(readFileSync(rehydrationPath, "utf8")),
  );
  const claimFixture = JSON.parse(readFileSync(claimPath, "utf8"));
  assert.deepEqual(
    VirtualSchedulingControlBuilder.claim(
      "command:claim:fixture",
      "worker:fixture",
      "slot:worker-fixture:0",
      {
        identity_version: "cymule.artifact/2",
        artifact_id: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        kind: "cymule.execution-binding/2",
      },
      ["sandbox", "cpu", "cpu"],
      claimFixture.clock,
      30,
    ),
    claimFixture,
  );
  const renewalFixture = JSON.parse(readFileSync(renewalPath, "utf8"));
  assert.deepEqual(
    VirtualSchedulingControlBuilder.renew(
      "command:renew:fixture",
      "work:fixture",
      "worker:fixture",
      1,
      1,
      renewalFixture.clock,
      30,
    ),
    renewalFixture,
  );
  const recoveryFixture = JSON.parse(readFileSync(recoveryPath, "utf8"));
  assert.deepEqual(
    VirtualSchedulingControlBuilder.recovery(
      "command:recovery:fixture",
      "work:fixture",
      "worker:fixture",
      1,
      2,
      recoveryFixture.clock,
      recoveryFixture.resolution,
    ),
    recoveryFixture,
  );
  assert.deepEqual(
    VirtualSchedulingControlBuilder.runWeight("command:run-weight:fixture", "run:fixture", 3),
    JSON.parse(readFileSync(runWeightPath, "utf8")),
  );
  assert.throws(() => VirtualSchedulingControlBuilder.claim(
    "command:unsafe", "worker:fixture", "slot:worker-fixture:0",
    claimFixture.execution_binding, [], claimFixture.clock, Number.MAX_SAFE_INTEGER + 1,
  ));
  assert.throws(() => VirtualWorkControlBuilder.succeed(
    "command:unsafe", "work:fixture", "worker:fixture",
    Number.MAX_SAFE_INTEGER + 1, 1, controlFixture.clock,
    controlFixture.resolution.result,
  ));
});

test("TypeScript evolution control validates through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_EVOLUTION_CONTROL_FIXTURE;
  const restartPath = process.env.CYMULE_EVOLUTION_RESTART_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined || restartPath === undefined) {
    context.skip("evolution Engine conformance is not configured");
    return;
  }
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
  assert.deepEqual(await new CliEngine(enginePath).verifyEvolutionCommand(command), command);
  const restartExpected = JSON.parse(readFileSync(restartPath, "utf8"));
  const restart = EvolutionControlBuilder.restartUnderNewPlan(
    "command:evolution:fixture:restart",
    restartExpected.request,
  );
  assert.deepEqual(restart, restartExpected);
  assert.deepEqual(await new CliEngine(enginePath).verifyEvolutionCommand(restart), restart);
});

test("TypeScript unified live evolution validates through the Rust engine", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const fixturePath = process.env.CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE;
  if (enginePath === undefined || fixturePath === undefined) {
    context.skip("live-evolution Engine conformance is not configured");
    return;
  }
  const expected = JSON.parse(readFileSync(fixturePath, "utf8"));
  const selection = EvolutionControlBuilder.selectOccurrence(
    "command:evolution:fixture:select",
    "occurrence:fixture:1",
    "selection:fixture:1",
    expected.command.execution_binding,
  );
  const command = LiveEvolutionControlBuilder.apply(
    "command:live-evolution:fixture:select",
    "template:review-parent",
    selection,
  );
  assert.deepEqual(command, expected);
  assert.deepEqual(await new CliEngine(enginePath).verifyLiveEvolutionCommand(command), command);
});

test("TypeScript applies JSON Schema scalar lengths to Engine failures", async () => {
  const candidate = new FlowBuilder("failure-scalar-length", {}, {}).finish({ kind: "input" });
  const issue = {
    code: "🧪".repeat(200),
    message: "🧭".repeat(2_000),
    path: `/${"🦀".repeat(999)}`,
    schema_path: `/${"🚀".repeat(999)}`,
  };
  const boundaryFailure = {
    category: "validation",
    phase: "validate_request",
    code: "unicode_boundary",
    message: "🧪".repeat(8_192),
    contract: "🧭".repeat(500),
    contract_side: "schema",
    path: `/${"🦀".repeat(999)}`,
    issues: [issue],
    retry_disposition: "correct_and_retry",
  };
  await assert.rejects(
    () => withFailureEngine(boundaryFailure, (engine) => engine.seal(candidate)),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "validation"
      && error.failure.code === "unicode_boundary"
      && error.failure.message === boundaryFailure.message
      && error.failure.retry_disposition === "correct_and_retry",
  );

  const invalidFailures = [
    { ...boundaryFailure, message: "🧪".repeat(8_193) },
    { ...boundaryFailure, message: "invalid\nmessage" },
    { ...boundaryFailure, contract: "🧭".repeat(501) },
    { ...boundaryFailure, contract: "invalid\0contract" },
    { ...boundaryFailure, path: `/${"🦀".repeat(1_000)}` },
    { ...boundaryFailure, path: "/invalid\npath" },
    { ...boundaryFailure, issues: [{ ...issue, code: "🧪".repeat(201) }] },
    { ...boundaryFailure, issues: [{ ...issue, code: "invalid\ncode" }] },
    { ...boundaryFailure, issues: [{ ...issue, message: "🧭".repeat(2_001) }] },
    { ...boundaryFailure, issues: [{ ...issue, message: "invalid\0message" }] },
    { ...boundaryFailure, issues: [{ ...issue, path: `/${"🦀".repeat(1_000)}` }] },
    { ...boundaryFailure, issues: [{ ...issue, schema_path: `/${"🚀".repeat(1_000)}` }] },
  ];
  for (const failure of invalidFailures) {
    await assert.rejects(
      () => withFailureEngine(failure, (engine) => engine.seal(candidate)),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }
  const wrongRetryFailure = {
    category: "unknown_world_outcome",
    phase: "transport",
    code: "wrong_retry_matrix",
    message: "mutation outcome is unknown",
    retry_disposition: "retry_same_request",
  };
  await assert.rejects(
    () => withFailureEngine(wrongRetryFailure, (engine) => engine.seal(candidate)),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
  await assert.rejects(
    () => withFailureEngine(
      wrongRetryFailure,
      (engine) => {
        const resume = DurableControlBuilder.resumeRun("run:wrong-retry", fixtureExecution());
        return engine.executeDurable(durableTargetFor(resume), resume);
      },
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "unknown_world_outcome"
      && error.failure.code === "invalid_engine_response"
      && error.failure.retry_disposition === "reconcile",
  );
  await assert.rejects(
    () => withFailureEngine(
      {
        category: "timed_out",
        phase: "execute_durable",
        code: "persisted_attempt_timed_out",
        message: "the persisted Attempt timed out",
        retry_disposition: "refresh_and_retry",
      },
      (engine) => engine.seal(candidate),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "timed_out"
      && error.failure.retry_disposition === "refresh_and_retry",
  );
  await assert.rejects(
    () => withFailureEngine(
      {
        category: "timed_out",
        phase: "execute_durable",
        code: "forged_timeout_retry",
        message: "the timeout recovery was forged",
        retry_disposition: "reconcile",
      },
      (engine) => engine.seal(candidate),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
});

test("TypeScript preserves structured Rust Engine failures", async (context) => {
  const enginePath = process.env.CYMULE_BIN;
  const pluginPath = process.env.CYMULE_TEST_PLUGIN;
  const failurePath = process.env.CYMULE_ENGINE_FAILURE_FIXTURE;
  if (enginePath === undefined || pluginPath === undefined || failurePath === undefined) {
    context.skip("Engine failure conformance is not configured");
    return;
  }
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
  await assertEngineFailure(() => engine.seal(invalid), expected.invalid_plan_version!);
  const plan = await engine.seal(candidate);
  await assertEngineFailure(
    () => engine.run(
      plan,
      { simulate: "expected_failure" },
      processTarget(pluginPath),
      "run:ts-expected",
    ),
    expected.expected_plugin_failure!,
  );
  await assertEngineFailure(
    () => engine.run(
      plan,
      { message: "defect" },
      processTarget(enginePath),
      "run:ts-defect",
    ),
    expected.plugin_defect!,
  );
  await assertEngineFailure(
    () =>
      engine.run(
        plan,
        { message: "substrate" },
        processTarget("/cymule-conformance/missing-plugin"),
        "run:ts-substrate",
      ),
    expected.substrate_failure!,
  );
});

test("TypeScript normalizes mathematical integers before typed response validation", async () => {
  const contentId = (digit: string) => `sha256:${digit.repeat(64)}`;
  const gate = {
    gate_id: "gate:numeric-token",
    decision_id: "decision:numeric-token",
    min_target_observations: 1,
    max_target_failures: 0,
    min_equivalent_shadows: 0,
    max_inequivalent_shadows: 0,
  };
  const command = EvolutionControlBuilder.applyGate(
    "command:numeric-token",
    gate,
    "decision:next",
  );
  const typedResponse = {
    type: "verified_evolution_command",
    command,
  };
  for (const lexeme of ["1.0", "1e0"]) {
    const raw = JSON.stringify(typedResponse).replace(
      '"min_target_observations":1',
      `"min_target_observations":${lexeme}`,
    );
    assert.deepEqual(
      await withRawSuccessEngine(raw, (engine) => engine.verifyEvolutionCommand(command)),
      command,
      `mathematical integer token ${lexeme} was not normalized`,
    );
  }
  for (const lexeme of ["1.5", "9007199254740991.1"]) {
    const fractionalTyped = JSON.stringify(typedResponse).replace(
      '"min_target_observations":1',
      `"min_target_observations":${lexeme}`,
    );
    await assert.rejects(
      () => withRawSuccessEngine(
        fractionalTyped,
        (engine) => engine.verifyEvolutionCommand(command),
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
      `non-integral token populated an integer field: ${lexeme}`,
    );
  }

  const literalDefinition: PlanCandidate["definitions"][number] = {
    id: "main",
    input_schema: {},
    output_schema: {},
    body: { steps: [], result: { kind: "literal", value: 1 } },
  };
  const literalCommand = LiveEvolutionControlBuilder.publishDefinition(
    "command:literal-token",
    "module:literal-token",
    literalDefinition,
    [],
  );
  const literalEvolutionId = "evolution:literal-token";
  const literalResponse = {
    type: "live_evolution_executed",
    commit: evolutionCommit(literalEvolutionId, literalCommand, {
        result: "definition_published",
        revision: {
          revision_version: "cymule.subflow-revision/2",
          revision_id: contentId("2"),
          logical_ref: "module:literal-token",
          sequence: 1,
          definition: literalDefinition,
          references: [],
        },
      }),
  };
  const floatEcho = JSON.stringify(literalResponse).replaceAll('"value":1', '"value":1.0');
  assert.deepEqual(
    await withRawSuccessEngine(
      floatEcho,
      (engine) => engine.executeLiveEvolution(
        { store: directoryStore("unused"), migration_adapter: null, shadow_driver: null, target_execution_bindings: {} },
        literalEvolutionId,
        literalCommand,
      ),
    ),
    literalResponse.commit,
    "integer-valued arbitrary JSON was not normalized",
  );

  const outputPlan = {
    plan_id: contentId("3"),
    candidate: new FlowBuilder("float-output", {}, {}).finish({ kind: "input" }),
  };
  await assert.rejects(
    () => new CliEngine("missing-engine").run(
      outputPlan,
      1e21,
      processTarget(process.execPath),
      "run:unsafe-input",
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "request_encoding_failed",
  );
  for (const [lexeme, expected] of [["1.0", 1], ["1e0", 1], ["1.5", 1.5]] as const) {
    const response = {
      type: "execution_boundary",
      execution: {
        status: "completed",
        result: {
          run_id: "run:float-output",
          plan_id: outputPlan.plan_id,
          value: expected,
          projection_digest: "4".repeat(64),
          precondition_token: `pre:1:sha256:${"7".repeat(64)}`,
          effects: [],
        },
      },
    };
    const raw = JSON.stringify(response).replace(
      `"value":${JSON.stringify(expected)}`,
      `"value":${lexeme}`,
    );
    const outcome = await withRawSuccessEngine(raw, (engine) => engine.run(
      outputPlan,
      null,
      processTarget(process.execPath),
      "run:float-output",
    ));
    assert.equal(outcome.status, "completed");
    if (outcome.status !== "completed") throw new Error("execution is not completed");
    assert.equal(outcome.result.value, expected);
    assert.equal(typeof outcome.result.value, "number");
  }

  for (const [lexeme, expected] of [
    ["9007199254740991.0", Number.MAX_SAFE_INTEGER],
    ["-9007199254740991e0", Number.MIN_SAFE_INTEGER],
  ] as const) {
    const raw = JSON.stringify({
      type: "execution_boundary",
      execution: {
        status: "completed",
        result: {
          run_id: "run:safe-integer-output",
          plan_id: outputPlan.plan_id,
          value: 0,
          projection_digest: "5".repeat(64),
          precondition_token: `pre:1:sha256:${"8".repeat(64)}`,
          effects: [],
        },
      },
    }).replace('"value":0', `"value":${lexeme}`);
    const outcome = await withRawSuccessEngine(raw, (engine) => engine.run(
      outputPlan,
      null,
      processTarget(process.execPath),
      "run:safe-integer-output",
    ));
    assert.equal(outcome.status, "completed");
    if (outcome.status !== "completed") throw new Error("execution is not completed");
    assert.equal(outcome.result.value, expected);
  }

  for (const lexeme of [
    "9007199254740992.0",
    "9007199254740993e0",
    "-9007199254740992.0",
    "1e-10000",
    "-1e-10000",
    "1.0000000000000001",
    "0.99999999999999999",
  ]) {
    const raw = JSON.stringify({
      type: "execution_boundary",
      execution: {
        status: "completed",
        result: {
          run_id: "run:unsafe-integer-output",
          plan_id: outputPlan.plan_id,
          value: 0,
          projection_digest: "6".repeat(64),
          precondition_token: `pre:1:sha256:${"9".repeat(64)}`,
          effects: [],
        },
      },
    }).replace('"value":0', `"value":${lexeme}`);
    await assert.rejects(
      () => withRawSuccessEngine(raw, (engine) => engine.run(
        outputPlan,
        null,
        processTarget(process.execPath),
        "run:unsafe-integer-output",
      )),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
      `unsafe integer accepted ${lexeme}`,
    );
  }

  const echoDirectory = mkdtempSync(join(tmpdir(), "cymule-rounded-echo-"));
  const echoEngine = join(echoDirectory, "rounded-echo-engine");
  const echoResponse = {
    type: "execution_boundary",
    execution: {
      status: "completed",
      result: {
        run_id: "run:rounded-echo",
        plan_id: outputPlan.plan_id,
        value: 0,
        projection_digest: "7".repeat(64),
        precondition_token: `pre:1:sha256:${"a".repeat(64)}`,
        effects: [],
      },
    },
  };
  writeFileSync(echoEngine, `#!/usr/bin/env node
let input = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => { input += chunk; });
process.stdin.on("end", () => {
  const request = JSON.parse(input).request;
  const rawRequest = JSON.stringify(request).replace('"input":0', '"input":1e-10000');
  process.stdout.write(
    '{"engine_protocol":"cymule.engine/5","outcome":"success","request":'
      + rawRequest
      + ',"response":'
      + ${JSON.stringify(JSON.stringify(echoResponse))}
      + '}',
  );
});
`);
  chmodSync(echoEngine, 0o700);
  try {
    await assert.rejects(
      () => new CliEngine(echoEngine).run(
        outputPlan,
        0,
        processTarget(process.execPath),
        "run:rounded-echo",
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "unknown_world_outcome"
        && error.failure.code === "invalid_engine_response"
        && error.failure.retry_disposition === "reconcile",
      "a mathematically fractional request echo compared equal to integer zero",
    );
  } finally {
    rmSync(echoDirectory, { recursive: true, force: true });
  }
});

test("TypeScript rejects unsafe JSON and preserves pre-cancellation", async () => {
  const candidate = new FlowBuilder("transport", {}, {}).finish({ kind: "input" });
  const legacyCandidate = {
    ...candidate,
    ir_version: "cymule.ir/2",
  } as unknown as PlanCandidate;
  await assert.rejects(
    () => withSuccessEngine(
      {
        type: "sealed",
        plan: { plan_id: `sha256:${"1".repeat(64)}`, candidate: legacyCandidate },
      },
      (engine) => engine.seal(legacyCandidate),
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.category === "transport_failure"
      && error.failure.code === "invalid_engine_response",
  );
  const cancelled = new AbortController();
  cancelled.abort();
  try {
    await new CliEngine("cymule", { signal: cancelled.signal }).seal(candidate);
    assert.fail("cancelled request unexpectedly started");
  } catch (error) {
    assert.ok(error instanceof EngineError);
    assert.equal(error.failure.category, "cancelled");
    assert.equal(error.failure.retry_disposition, "never");
  }
  try {
    await new CliEngine("missing-engine").seal(
      { ...candidate, name: Number.NaN } as unknown as PlanCandidate,
    );
    assert.fail("non-finite request unexpectedly encoded");
  } catch (error) {
    assert.ok(error instanceof EngineError);
    assert.equal(error.failure.code, "request_encoding_failed");
  }
  await assert.rejects(
    () => new CliEngine("missing-engine").seal(
      { ...candidate, name: undefined } as unknown as PlanCandidate,
    ),
    (error: unknown) => error instanceof EngineError
      && error.failure.code === "request_encoding_failed",
  );

  let accessorReads = 0;
  const accessorCandidate = { ...candidate } as Record<string, unknown>;
  Object.defineProperty(accessorCandidate, "name", {
    enumerable: true,
    get: () => {
      accessorReads += 1;
      return "accessor";
    },
  });
  const symbolCandidate = { ...candidate } as Record<PropertyKey, unknown>;
  symbolCandidate[Symbol("hidden")] = "value";
  const extraArray: unknown[] & { extra?: boolean } = [];
  extraArray.extra = true;
  const nonEnumerableCandidate = { ...candidate } as Record<string, unknown>;
  Object.defineProperty(nonEnumerableCandidate, "hidden", {
    enumerable: false,
    value: true,
  });
  const rejected: Array<[string, PlanCandidate]> = [
    ["custom prototype", Object.assign(Object.create({}), candidate) as PlanCandidate],
    ["toJSON", { ...candidate, toJSON: () => candidate } as unknown as PlanCandidate],
    ["accessor", accessorCandidate as unknown as PlanCandidate],
    ["symbol property", symbolCandidate as unknown as PlanCandidate],
    ["sparse array", { ...candidate, components: new Array(1) } as PlanCandidate],
    ["extra array key", { ...candidate, components: extraArray } as PlanCandidate],
    ["non-enumerable key", nonEnumerableCandidate as unknown as PlanCandidate],
    ["unpaired surrogate value", { ...candidate, name: "\ud800" }],
    ["unpaired surrogate key", {
      ...candidate,
      metadata: { ["\udc00"]: "value" },
    }],
  ];
  for (const [label, rejectedCandidate] of rejected) {
    await assert.rejects(
      () => new CliEngine("missing-engine").seal(rejectedCandidate),
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "request_encoding_failed",
      label,
    );
  }
  assert.equal(accessorReads, 0);

  const directory = mkdtempSync(join(tmpdir(), "cymule-process-preflight-"));
  const executable = join(directory, "must-not-start");
  const started = join(directory, "started");
  writeFileSync(executable, `#!/bin/sh\nprintf started >${JSON.stringify(started)}\nexit 1\n`);
  chmodSync(executable, 0o700);
  const plan = { plan_id: `sha256:${"a".repeat(64)}`, candidate };
  const validTarget = processTarget(process.execPath);
  processPlugin({
    ...validTarget.process,
    arguments: Array.from({ length: 4_096 }, () => ""),
    environment: Object.fromEntries(Array.from({ length: 4_096 }, (_, index) => [`ENTRY_${index}`, ""])),
    runtime_closure: Object.fromEntries(Array.from(
      { length: 4_096 },
      (_, index) => [`runtime-${index}`, `sha256:${"a".repeat(64)}`],
    )),
  });
  for (const [field, overflow] of [
    ["arguments", Array.from({ length: 4_097 }, () => "")],
    ["environment", Object.fromEntries(Array.from({ length: 4_097 }, (_, index) => [`ENTRY_${index}`, ""]))],
    ["runtime_closure", Object.fromEntries(Array.from(
      { length: 4_097 },
      (_, index) => [`runtime-${index}`, `sha256:${"a".repeat(64)}`],
    ))],
  ] as const) {
    assert.throws(
      () => processPlugin({ ...validTarget.process, [field]: overflow }),
      /process plugin target is invalid/,
      `${field} accepted 4097 entries`,
    );
  }
  const { working_directory: _workingDirectory, ...missingWorkingDirectory } =
    validTarget.process;
  const invalidTargets: Array<[string, EnginePluginTarget]> = [
    ["legacy string", process.execPath as unknown as EnginePluginTarget],
    ["legacy location", {
      ...validTarget,
      location: process.execPath,
    } as unknown as EnginePluginTarget],
    ["missing required nullable working directory", {
      ...validTarget,
      process: missingWorkingDirectory,
    } as unknown as EnginePluginTarget],
    ["relative executable", {
      ...validTarget,
      process: { ...validTarget.process, executable: "relative-plugin" },
    }],
    ["empty runtime closure", {
      ...validTarget,
      process: { ...validTarget.process, runtime_closure: {} },
    }],
    ["host label runtime closure", {
      ...validTarget,
      process: { ...validTarget.process, runtime_closure: { "host-abi": "unix:darwin:arm64" } },
    }],
    ["tampered runtime closure digest", {
      ...validTarget,
      process: { ...validTarget.process, runtime_closure: { runtime: `sha256:${"A".repeat(64)}` } },
    }],
    ["zero timeout", {
      ...validTarget,
      process: { ...validTarget.process, timeout_ms: 0 },
    }],
    ["narrowed plugin message limit", {
      ...validTarget,
      process: { ...validTarget.process, message_limit: 8 * 1024 * 1024 - 1 },
    }],
    ["widened plugin message limit", {
      ...validTarget,
      process: { ...validTarget.process, message_limit: 8 * 1024 * 1024 + 1 },
    }],
    ["oversized message limit", {
      ...validTarget,
      process: { ...validTarget.process, message_limit: 64 * 1024 * 1024 + 1 },
    }],
  ];
  try {
    for (const [label, invalidTarget] of invalidTargets) {
      await assert.rejects(
        () => new CliEngine(executable).run(plan, null, invalidTarget, "run:preflight"),
        (error: unknown) => error instanceof EngineError
          && error.failure.category === "validation"
          && error.failure.code === "invalid_plugin_target",
        label,
      );
      assert.equal(existsSync(started), false, `${label} started the Engine`);
    }

    const query = DurableControlBuilder.runCurrent("run:custom-store", null);
    const customStore: EngineStoreTarget = {
      provider: "acme.store/1",
      location: "provider-owned-location",
      domain: "tenant-a",
    };
    await assert.rejects(
      () => new CliEngine(executable).executeDurable({ store: customStore }, query),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "engine_process_failed",
      "provider-neutral Store target did not reach the Engine",
    );
    assert.equal(existsSync(started), true, "custom Store provider was rejected by the SDK");
    rmSync(started, { force: true });

    const resume = DurableControlBuilder.resumeRun("run:target-preflight", fixtureExecution());
    await assert.rejects(
      () => new CliEngine(executable).executeDurable(
        { store: directoryStore("unused") },
        resume,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.code === "durable_request_validation_failed",
    );
    assert.equal(existsSync(started), false);

    const artifact = {
      identity_version: "cymule.artifact/2" as const,
      artifact_id: `sha256:${"b".repeat(64)}`,
      kind: "test/input",
    };
    const shadow = LiveEvolutionControlBuilder.apply(
      "command:shadow-preflight",
      "template:shadow-preflight",
      EvolutionControlBuilder.shadow("command:shadow-child", {
        comparison_id: "comparison:shadow-preflight",
        decision_id: "decision:shadow-preflight",
        subject: "run:shadow-preflight",
        primary_plan: `sha256:${"c".repeat(64)}`,
        shadow_plan: `sha256:${"d".repeat(64)}`,
        driver_id: "driver:shadow-preflight",
        driver_revision: `sha256:${"e".repeat(64)}`,
        input: artifact,
        comparison_policy: "policy:shadow-preflight",
      }),
    );
    for (const target of [
      {
        store: directoryStore("unused"),
        migration_adapter: null,
        shadow_driver: {
          driver_id: "driver:shadow-preflight",
          driver_revision: `sha256:${"e".repeat(64)}`,
          process: validTarget,
        },
        target_execution_bindings: {},
      },
    ]) {
      await assert.rejects(
        () => new CliEngine(executable).executeLiveEvolution(
          target,
          "journal:shadow-preflight",
          shadow,
        ),
        (error: unknown) => error instanceof EngineError
          && error.failure.code === "evolution_request_validation_failed",
      );
      assert.equal(existsSync(started), false);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("TypeScript rejects missing or null component output Artifact kinds", async () => {
  const candidate = new FlowBuilder("component-output-kind", {}, {})
    .component("test.echo", {}, {}, "cymule.component-output/1", {})
    .finish({ kind: "input" });
  const component = candidate.components[0]!;
  const { output_artifact_kind: _omitted, ...withoutOutputArtifactKind } = component;
  for (const malformedComponent of [
    withoutOutputArtifactKind,
    { ...component, output_artifact_kind: null },
  ]) {
    const malformed = {
      ...candidate,
      components: [malformedComponent],
    } as unknown as PlanCandidate;
    await assert.rejects(
      () => withSuccessEngine(
        {},
        (engine) => engine.seal(malformed),
        `sha256:${"1".repeat(64)}`,
      ),
      (error: unknown) => error instanceof EngineError
        && error.failure.category === "transport_failure"
        && error.failure.code === "invalid_engine_response",
    );
  }
});

async function assertEngineFailure(
  operation: () => Promise<unknown>,
  expected: Record<string, string>,
): Promise<void> {
  try {
    await operation();
    assert.fail("operation unexpectedly succeeded");
  } catch (error) {
    assert.ok(error instanceof EngineError);
    assert.equal(error.failure.category, expected.category);
    assert.equal(error.failure.phase, expected.phase);
    assert.equal(error.failure.code, expected.code);
    assert.equal(error.failure.retry_disposition, expected.retry_disposition);
  }
}
