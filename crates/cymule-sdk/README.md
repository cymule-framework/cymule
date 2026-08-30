# Cymule for Rust

`cymule` is the Rust authoring and engine-client facade for the Cymule semantic
execution framework. It emits the same frozen IR and control records as the
TypeScript, Python, and Go SDKs; semantic identity, admission, reduction, and
replay remain owned by the Rust engine.

Until the next public release is finalized, consume the SDK from the same
reviewed source checkout as the Engine:

```toml
[dependencies]
cymule = { path = "../cymule/crates/cymule-sdk" }
```

The previously published `0.2.0` packages predate the current breaking Engine,
durable receipt, process-target, and archive contracts and are not compatible
with this source generation.

Build a provider-neutral Flow:

```rust
use cymule::{Expression, FlowBuilder, PlanCandidate};

fn hello_flow() -> PlanCandidate {
    FlowBuilder::new(
        "hello",
        serde_json::json!({"type": "object"}),
        serde_json::json!({"type": "object"}),
    )
    .finish(Expression::Input)
}
```

Add `serde_json` when using the `json!` convenience macro. A Flow declares
abstract components, effects, waits, and scopes; concrete models, tools,
storage, queues, sandboxes, and Agent Loops remain replaceable plugins or
application code. Every component declaration supplies its required Plan-owned
output Artifact kind. Ordinary components pass
`cymule::COMPONENT_OUTPUT_ARTIFACT_KIND`; Resource-producing components
pass the exact `cymule.typed-json/sha256-...` kind derived by
`framework_artifact_contract(ResourceHandle).typed_artifact_kind()`. The
logical Resource framework type key is not a persisted output kind, and there
is no default. The Engine validates each returned value against that
component's output schema before canonical JSON is stored under the declared
kind.

Build the matching CLI from that checkout:

```sh
cargo build -p cymule-cli
```

Build an `EnginePluginTarget` with an explicit `EngineProcessConfig`, then pass
that complete target to `DurableEngine::new("cymule", store, executor, clock)`.
The target fixes arguments, an ambient-cleared environment, optional working
directory, runtime closure, timeout, and byte limits; provider-owned ledger or
sidecar locators belong in that explicit environment and therefore in the
execution binding. The durable client also binds a persistence-backed
`EngineClockTarget`. Call `observe_clock(run_id)` first,
then pass the returned opaque reference with a driver owner and positive TTL to
`start`, `resume`, `takeover`, or `release`. The SDK neither reads local time
nor seals Clock receipts. The engine also provides the seven bounded
`run_index_page`/`run_current`/child-page/`run_item` reads, `signal`, and
`resolve_effect`, `cancel`, and `evolve`; Effect resolution and cancellation
return complete Rust-issued receipts with the exact accepted command nested
beside the authoritative provider outcome and content receipt. The SDK compares
that command to the echoed request but does not re-hash Rust-owned result or
reason Artifacts. Effect-resolution receipts do not duplicate the Run's world
settlement; a provider `NotApplied` result is the closed `effect_not_applied`
boundary with its exact content-addressed intent. `evolve` returns a
`EvolutionCommit` that retains the observed revision, required-nullable
committed revision, and exact persistence receipt containing the admitted
authority, semantic command, typed outcome, and mutation set. The Rust durable
runtime remains the only reducer.
Each query passes the caller's required-null revision/cursor and explicit item
or canonical-byte budget unchanged. Responses bind the exact revision and
StateRoot; the SDK exposes no full Run/domain mirror or query identity. Each
request carries exactly its command's authority: queries, signals, and
cancellation carry Store only; Effect resolution carries Store plus executor;
Run execution carries Store, executor, and Clock. Live evolution similarly
includes only the migration adapter or shadow driver selected by that command,
while both provider members remain explicit nullable wire fields. Each non-null
provider binds its semantic identity and revision to an exact process target
with the same pinned revision, even when the client has both providers
configured.
Use `EngineStoreTarget::sqlite` or a custom `Engine` transport for other stores.
Custom transports implement one `exchange` over `EngineRequestSnapshot` and
return `EngineTransportSuccess` with the complete accepted request echo plus
raw response; operation-specific bare payload methods are not transport
authority. Queries need no executor. Migration and shadow variants accept exact-revision
process targets.

`CliEngine` owns one direct Child handle and keeps one absolute deadline across
process exit plus bounded stdin/stdout/stderr completion. Timeout or
cancellation closes local pipes and kills the direct child if it is still
unreaped. An explicit timeout override must be positive; zero fails as local
validation before process spawn. The official Engine/executor watchdog owns descendant closure. Stdout uses the
Runtime-owned 128 MiB plus 32-byte framing response-envelope bound and stderr its independent 1 MiB
diagnostic bound. `EngineCancellation` linearizes preflight, launch,
cancellation and admitted completion; there is no public atomic-flag launch
race. Completed
boundaries are admitted only with an exact Plan content ID, lowercase
projection digest, ordered effect content IDs, and the closed
`pre:<safe-epoch>:<event-content-id>` token.

Every v5 success is correlated only by the exact request echoed by the Engine.
The SDK still validates the returned object itself, but it does not reseal a
Plan or Resource candidate or compare a locally derived identity as a second
correlation channel. Valid structured failures are preserved exactly; a
malformed failure after a mutating request is an unknown world outcome that
requires reconciliation.

Embedded `CliEngine::run` accepts the same complete `EnginePluginTarget`; there
is no path-only overload or ambient process configuration authority.

The facade also exposes the complete provider-neutral virtual archive wire:
occurrence and historical-command proofs, the typed archived command,
cumulative work/command index proofs, and work-resolution receipts. Machine
reducer and Store archive internals remain in the advanced `cymule-core` and
`cymule-durable` crates.

See the [repository README](https://github.com/cymule-framework/cymule) for the
complete quick start, execution model, profile boundaries, and plugin APIs.
