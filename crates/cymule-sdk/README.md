# Cymule for Rust

`cymule` is the Rust authoring and engine-client facade for the Cymule semantic
execution framework. It emits the same frozen IR and control records as the
TypeScript, Python, and Go SDKs; semantic identity, admission, reduction, and
replay remain owned by the Rust engine.

Add the SDK:

```sh
cargo add cymule
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
cargo install cymule-cli
```

See the [repository README](https://github.com/cymule-framework/cymule) for the
complete quick start, execution model, profile boundaries, and plugin APIs.
