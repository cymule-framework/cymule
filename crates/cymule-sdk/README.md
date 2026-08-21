# Cymule for Rust

`cymule` is the Rust authoring and engine-client facade for the Cymule semantic
execution framework. It emits the same frozen IR and control records as the
TypeScript, Python, and Go SDKs; semantic identity, admission, reduction, and
replay remain owned by the Rust engine.

Add the SDK:

```sh
cargo add cymule@0.2.0
```

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
application code.

Install the CLI separately:

```sh
cargo install cymule-cli --version 0.2.0
```

`DurableEngine::new("cymule", store, plugin)` provides stateful `start`, `get`,
`resume`, `signal`, `release`, and `evolve` calls over the closed Engine
transport. The Rust durable runtime remains the only reducer.
Use `EngineStoreTarget::sqlite` or a custom `Engine` transport for other stores;
queries need no executor. Migration and shadow variants accept exact-revision
process targets.

See the [repository README](https://github.com/cymule-framework/cymule) for the
complete quick start, execution model, profile boundaries, and plugin APIs.
