# Hello World

This example is a complete code-first Cymule application. It shows how to:

1. author a typed Flow with the Rust SDK;
2. implement abstract component and effect operations;
3. seal the Flow into a content-addressed Plan;
4. execute it with the Embedded runtime;
5. observe an external effect and its Run result;
6. simulate an ambiguous dispatch and reconcile the original intent.

## Run it

From the repository root, greet anyone by passing their name:

```sh
cargo run -p cymule-example-hello-world -- Ada
```

The component produces `Hello, Ada!`, the effect captures that greeting, and
the application prints the complete Run result.

## What to read

- [`src/flow.rs`](src/flow.rs) builds the Flow with `FlowBuilder`. Start here to
  add a component, wait, effect, scope, or different Result.
- [`src/plugin.rs`](src/plugin.rs) realizes the abstract `example.greet` and
  `example.capture` operations. Replace these handlers with your own services.
- [`src/main.rs`](src/main.rs) seals and executes the Plan with
  `EmbeddedRuntime`.
- [`flow.json`](flow.json) is the published language-neutral IR emitted by the
  code-first Flow. A unit test prevents the two forms from drifting.

## Try the failure path

Run the same semantic effect while simulating a lost provider response:

```sh
cargo run -p cymule-example-hello-world -- Ada --unknown-once
```

The plugin first returns `unknown`. Cymule keeps the same structural effect
identity and calls reconciliation instead of creating a new effect. The plugin
then resolves the original intent as `applied`, allowing the Run to complete.

## Make it yours

Useful first changes:

1. Change the greeting in `src/plugin.rs` and run the command again. The Plan ID
   stays stable because implementation behavior is a binding concern.
2. Add a new step or change a stable site in `src/flow.rs`. The Plan ID changes
   because program meaning changed.
3. Replace `example.capture` with a real effect adapter. Preserve `intent_id` as
   the provider idempotency and reconciliation key.
4. Change `mutation`, `dispatch`, or `reconciliation` in the effect profile and
   observe which combinations the Rust sealer accepts.

This example uses an in-process `PluginHost` for clarity. The process plugin
protocol provides the same semantic boundary when integrations need separate
packaging or runtime isolation.
