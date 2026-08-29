# Cymule TypeScript SDK

This package authors `cymule.ir/3` Plan Candidates and calls a trusted Cymule
Engine. It does not implement canonical sealing or runtime semantics.

`FlowBuilder.definition()` adds a reusable definition to the same immutable
Plan and `invoke()` calls it with explicit input and result binding. Logical
latest-compatible registry resolution is performed by the Rust live-evolution
linker before sealing, never by the SDK.

`FlowBuilder.component()` requires the Plan-owned output Artifact kind as an
explicit argument; it never defaults one. Ordinary components use
`cymule.component-output/1`. Resource-producing components use the exact
`cymule.typed-json/sha256-...` kind derived from the sealed Resource Handle
contract, never the logical framework type key. The Engine validates the
returned value against the declared output schema before canonical JSON is
stored under that kind.

The current breaking contract is source-checkout authority. Build the Rust
Engine and TypeScript package from the same reviewed revision:

```sh
cargo build -p cymule-cli
pnpm --dir sdk/typescript install --frozen-lockfile
pnpm --dir sdk/typescript run build
```

Published `0.2.0` Rust/npm artifacts predate this contract and must not be
presented or combined as its runtime. Consume `sdk/typescript` from the reviewed
checkout until a release is finalized from that exact source.

```ts
import {
  CliEngine,
  DurableEngine,
  FlowBuilder,
  ResourceBuilder,
  processPlugin,
  sqliteClock,
  sqliteStore,
} from "cymule";
```

Resource Candidates use the same Engine boundary:

```ts
const resource = await new CliEngine().sealResource(
  ResourceBuilder.text("input for another Run"),
);
```

`DurableEngine(store, plugin, clock)` uses `CliEngine` by default and accepts
any structurally complete `EngineTransport`; the high-level facade therefore
does not make the CLI or either official Store provider part of domain
semantics. It exposes real
`start`, `runIndexPage`, `runCurrent`, bounded Run child pages, exact `runItem`,
`resume`, `signal`, `release`, `resolveEffect`, `cancel`, and
`evolve` operations. There is no separate generic control-submit interface.
Cancellation and claimed-Effect resolution return complete
Rust-issued receipts; the client checks exact request identity/fence evidence
and accepts the provider's actual terminal outcome without hashing reason or
result values. The
store, process plugin, and persistence-backed Clock are transport
configuration; the Plan and durable command remain provider-neutral. `evolve`
returns the Engine `/4` receipt containing the exact journal, full submitted
command, and closed outcome. Obtain an opaque issued Clock reference before
every new driver claim:

```ts
const clock = sqliteClock(
  "./clock.sqlite",
  "clock:local",
  `sha256:${"0".repeat(64)}`,
);
const durable = new DurableEngine(
  sqliteStore("./runs.sqlite", "local"),
  processPlugin({
    executable: "/opt/cymule/bin/component-plugin",
    arguments: [],
    environment: {},
    working_directory: null,
    runtime_closure: { "component-runtime": `sha256:${"a".repeat(64)}` },
    timeout_ms: 60_000,
    message_limit: 8 * 1024 * 1024,
    closure_limit: 64 * 1024 * 1024,
  }),
  clock,
);
const runId = "run:example";
const clockRef = await durable.observeClock(runId);
await durable.start(runId, candidate, input, {
  owner: "driver:example",
  clock: clockRef,
  ttl: 30,
});
```

CLI methods are asynchronous because the client owns the live Engine PID and
its isolated process group. The transport applies a finite 30-second default
deadline, drains bounded byte streams, decodes stdout as fatal UTF-8, and kills
the whole group once on timeout or `AbortSignal` cancellation. The interrupted
request rejects only after the direct child is reaped and inherited response
pipes close. A descendant that has reached POSIX zombie state is already unable
to execute a late side effect and is left for its owning system reaper; the SDK
does not mistake `kill(pid, 0)` success for executable liveness or risk a second
signal after PID/PGID reuse. A natural close that wins the lifecycle race keeps
its response authority.

The SDK never seals or hashes a future Clock receipt locally. A deadline or
cancellation after a mutating request begins returns a structured
`unknown_world_outcome` that callers must reconcile.
Every Engine `/4` success echoes the complete inner request; the client matches
it to the exact serialized wire before exposing the typed payload.
Process-backed targets always carry the complete closed `EngineProcessConfig`;
there is no path-string overload, ambient environment, implicit working
directory, or default process deadline/limit. An exact nested migration carries
only its pinned migration target, an exact nested shadow carries only its pinned
shadow target, and every other evolution command carries neither, even when the
client is configured with both providers.
Durable wait activation, Effect reconciliation, and cancellation return their
current nested receipts. Each receipt retains the complete admitted activation
or normalized command; requested Effect resolution remains separate from the
provider's actual terminal resolution and does not duplicate the Run's world
settlement. A provider `NotApplied` result is exposed as the closed
`effect_not_applied` boundary with its exact content-addressed intent.
Effect dispatches, component occurrences, and reconciliation commands all bind
the provider occurrence with an exact lowercase SHA-256 content ID.
Migration and restart authorization carry exact source Run, Plan, and epoch
intent. The Durable reducer derives the authenticated source witness from the
same pinned StateRoot; the retired public safe-point and caller-authored source
Continuation shapes are rejected.
`EngineStoreTarget` keeps provider, location, and optional domain as an open
transport boundary. `directoryStore` and `sqliteStore` select the current
official generations, while Engine ingress decides whether any provider is
supported. Queries omit the executor; migration and shadow commands accept
exact-revision process targets.

Use `ResourceBuilder.external` for content-addressed/version-pinned objects,
directories, collections, snapshots, and live references. Its optional manifest
pins exact list content. Concrete locator, grant, signed-URL, and credential
revision state stays behind resolver plugins and outside Resource Candidates.
`ResourceBuilder.handoff` emits only `cymule.resource-handoff/5`, requires
distinct producer/consumer Runs, and transfers the producer's exact typed result
Artifact without deriving another identity.

`WaitActivationBuilder` creates provider-neutral signal or timer delivery
records. `CliEngine.verifyWaitActivation` validates the closed wire contract;
the durable runtime remains responsible for matching pending waits and admitting
the activation through CAS.

`VirtualWorkControlBuilder` creates success, retry, failure, and cancellation
commands and copies exact revision-pinned region migration Plans. Compaction
authoring requires the command ID already issued by Rust, complete bounded
work/occurrence/archived-command sets, and an exact archive binding/revision.
The SDK never derives that identity or interprets opaque source cursors.
Finite Work occurrence and certificate DTOs describe the same Rust contracts.
Scheduler execution, archive and migration providers, complete persistence
receipts, and the verified claim outcome are Rust-only; this package has no
Virtual runtime or provider transport.

`VirtualSchedulingControlBuilder` creates capacity-slot claim, lease-renewal,
explicit expired-claim recovery, and future Run-weight commands. It requires
work and lease fences plus opaque Engine-issued Clock observation references;
it never accepts caller time, runs a worker loop, or infers expiry from
JavaScript time. These authoring helpers do not add a Virtual operation to
`CliEngine` or `DurableEngine`.

The release workflow uses GitHub Actions npm trusted publishing with provenance.
That configured workflow does not make an unreleased source checkout a
published package. The Rust Engine remains the semantic authority.
