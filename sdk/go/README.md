# Cymule Go SDK

The Go module authors the same `cymule.ir/2` candidates as the other SDKs and
uses the installed Rust CLI as its only Engine authority.

```sh
cargo install cymule-cli --version 0.2.0
go get github.com/cymule-framework/cymule/sdk/go@v0.2.0
```

```go
engine := cymule.CliEngine{}
candidate := cymule.NewFlow("hello", map[string]any{}, map[string]any{}).
    Component("example.echo", map[string]any{}, map[string]any{}, map[string]string{
        "capability": "echo",
    }).
    Call("call.echo", "example.echo", cymule.Expression{"kind": "input"}, "message").
    Finish(cymule.Expression{"kind": "binding", "name": "message"})

plan, err := engine.Seal(candidate)
```

`DurableEngine` exposes real `Start`, `Get`, `Resume`, `Signal`, `Release`, and
`Evolve` operations over a configured durable store and immutable process
plugin. `Finish` returns a deep-frozen candidate: later builder changes cannot
mutate it. Context cancellation and deadlines preserve structured Engine
failures, including `unknown_world_outcome` for a lost mutating response.
Use `SQLiteStore` or a custom Engine for other stores; queries omit the
executor. Migration and shadow commands accept exact-revision process targets.
