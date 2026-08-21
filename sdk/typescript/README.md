# Cymule TypeScript SDK

This package authors `cymule.ir/2` Plan Candidates and calls a trusted Cymule
Engine. It does not implement canonical sealing or runtime semantics.

`FlowBuilder.definition()` adds a reusable definition to the same immutable
Plan and `invoke()` calls it with explicit input and result binding. Logical
latest-compatible registry resolution is performed by the Rust live-evolution
linker before sealing, never by the SDK.

```sh
cargo install cymule-cli --version 0.2.0
npm install cymule@0.2.0
# Equivalent scoped export: npm install @cymule/sdk@0.2.0
```

```ts
import { CliEngine, DurableEngine, FlowBuilder, ResourceBuilder } from "cymule";
```

Resource Candidates use the same Engine boundary:

```ts
const resource = new CliEngine().sealResource(
  ResourceBuilder.text("input for another Run"),
);
```

`DurableEngine(store, plugin)` calls the installed CLI and exposes real
`start`, `get`, `resume`, `signal`, `release`, and `evolve` operations. The
store and process plugin are transport configuration; the Plan and durable
command remain provider-neutral. A deadline or cancellation after a mutating
request begins returns a structured `unknown_world_outcome` that callers must
reconcile.
Use `sqliteStore` or a custom Engine for other stores; queries omit the
executor. Migration and shadow commands accept exact-revision process targets.

Use `ResourceBuilder.external` for content-addressed/version-pinned objects,
directories, collections, snapshots, and live references. Its optional manifest
pins exact list content. Concrete locator, grant, signed-URL, and credential
revision state stays behind resolver plugins and outside Resource Candidates.

`WaitActivationBuilder` creates provider-neutral signal or timer delivery
records. `CliEngine.verifyWaitActivation` validates the closed wire contract;
the durable runtime remains responsible for matching pending waits and admitting
the activation through CAS.

`VirtualWorkControl` is a transport-neutral interface for querying identified
virtual-work attempt occurrences and submitting owner/work/lease/time-fenced
resolution commands.
`VirtualWorkControlBuilder` creates success, retry, failure, and cancellation
commands without choosing a scheduler or worker transport.
The same interface accepts adapter-produced region split/merge plans with
opaque cursor preconditions and coverage evidence; SDK code never partitions
cursor strings itself.
It also carries completed-region compaction and exact-occurrence rehydration
commands. `VirtualArchive` is only an immutable byte seam; the Rust controller
computes and verifies manifest and certificate identity before durable
admission.

`VirtualSchedulingControl` carries capacity-slot claims, lease renewals,
explicit expired-claim recovery, and future Run-weight updates. Builders require
work and lease fences plus logical Clock values; they never run a worker loop or
infer expiry from JavaScript time.

The package is published from GitHub Actions with npm trusted publishing and
provenance. The Rust Engine remains the semantic authority.
