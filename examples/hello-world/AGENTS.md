# Hello World Example

- Keep `flow.json`, `src/flow.rs`, `src/plugin.rs`, and the README narrative
  aligned.
- The example plugin implements only `example.greet` and `example.capture`.
- Preserve the mutating, commit-gated, queryable effect profile so the example
  crosses both component and effect paths.
- `cargo run -p cymule-example-hello-world -- <name>` is the stable quick-start
  entrypoint.
