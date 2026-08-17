# Hello World

This example runs a complete Cymule Flow through a standalone example plugin.
It is the stable user quick start and does not depend on conformance fixtures.

## Run it

From the repository root:

```sh
./examples/hello-world/run.sh
```

The script:

1. builds the Cymule engine and Hello World plugin;
2. validates and seals `flow.json` into an immutable Plan;
3. executes the Plan with `input.json`;
4. prints the Result;
5. writes the sealed Plan and Result under
   `.cymule/examples/hello-world/`.

Set `CYMULE_EXAMPLE_DIR` to choose a different state directory.

## Flow

The Flow declares two abstract operations:

- `example.echo` is a component that returns its input;
- `example.capture` is a mutating effect released on scope commit and capable
  of authoritative reconciliation.

The Flow calls `example.echo`, stages `example.capture` with the echoed value,
and returns that value as its Result. The plugin is a concrete realization of
those abstract contracts; its executable path and implementation identity are
not part of the Plan.

## Run the steps manually

The wrapper is equivalent to:

```sh
cargo build -p cymule-cli -p cymule-example-hello-plugin

mkdir -p .cymule/examples/hello-world

./target/debug/cymule seal \
  --input examples/hello-world/flow.json \
  > .cymule/examples/hello-world/plan.json

./target/debug/cymule run \
  --plan .cymule/examples/hello-world/plan.json \
  --input examples/hello-world/input.json \
  --plugin ./target/debug/cymule-example-hello-plugin \
  --run-id run:hello-world \
  > .cymule/examples/hello-world/result.json

cat .cymule/examples/hello-world/result.json
```
