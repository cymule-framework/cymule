# Cymule Go SDK

The Go module authors the same `cymule.ir/3` candidates as the other SDKs and
uses the installed Rust CLI as its only Engine authority.

Until this terminal generation is published, build the CLI and use the Go
module from the same reviewed source checkout. Do not pair this source API with
the older `0.2.0` packages.

```sh
cargo build -p cymule-cli
cd sdk/go && go test ./...
```

```go
engine := cymule.CliEngine{}
candidate := cymule.NewFlow("hello", map[string]any{}, map[string]any{}).
    Component(
        "example.echo",
        map[string]any{},
        map[string]any{},
        "cymule.component-output/1",
        map[string]string{"capability": "echo"},
    ).
    Call("call.echo", "example.echo", cymule.Expression{"kind": "input"}, "message").
    Finish(cymule.Expression{"kind": "binding", "name": "message"})

plan, err := engine.Seal(candidate)

plugin := cymule.ProcessPlugin(cymule.EngineProcessConfig{
    Executable:       "/absolute/path/to/plugin",
    Arguments:        []string{},
    Environment:      map[string]string{},
    WorkingDirectory: nil,
    RuntimeClosure:   map[string]string{"component-runtime": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
    TimeoutMS:        60_000,
    MessageLimit:     8 * 1024 * 1024,
    ClosureLimit:     64 * 1024 * 1024,
})
outcome, err := engine.Run(plan, map[string]any{"message": "hello"}, plugin, "run:hello")
```

The fourth `Component` argument is the required Plan-owned output Artifact
kind; the SDK has no default. A Resource-producing component instead declares
the exact `cymule.typed-json/sha256-...` kind derived from the sealed Resource
Handle contract, never the logical framework type key. The Engine validates the
returned value against the declared output schema before canonical JSON is
stored under that kind.

`DurableEngine` exposes real `Start`, `RunIndexPage`, `RunCurrent`, bounded Run
child pages, exact `RunItem`, `Resume`, `Takeover`, `Signal`, `Release`,
`ResolveEffect`, `Cancel`, and `Evolve` operations over a configured
durable store and immutable process plugin. No separate generic control-submit
interface is provided. `Finish` returns a deep-frozen
candidate: later builder changes cannot mutate it. Context cancellation and
deadlines preserve structured Engine
failures, including `unknown_world_outcome` for a lost mutating response.
Every query carries explicit revision/cursor and item/byte bounds and returns
one revision/StateRoot-pinned response; there is no query ID or full
Run/domain mirror.
Interruption immediately kills the official direct Child, closes all three
parent pipe endpoints, and reaps that child. The Engine/executor watchdog owns
descendant closure; the SDK never signals a raw PID or PGID after reaping.
Natural Engine exit is observed with `waitid(WNOWAIT)` and `Cmd.Wait` is the
single reaper after local stdout/stderr EOF.
Compact Engine stdout is bounded to the 128 MiB plus 32-byte framing response envelope while
diagnostic stderr has its independent 1 MiB bound. A complete valid failure is
decoded before nonzero exit status; success still requires a complete request
write and zero status.
Cancellation and effect reconciliation return request-bound typed receipts;
the SDK validates Rust-issued Artifact references without recomputing them.
Effect-resolution receipts do not duplicate Run world settlement, and a
provider `NotApplied` result is exposed as the closed `effect_not_applied`
boundary with its exact content-addressed intent. A dual-configured evolution
client sends only the pinned migration or shadow provider required by the exact
nested command; all other commands send neither.
Strict JSON rejects invalid UTF-8 and unpaired surrogate escapes before a
request can start. Caller-defined JSON and text marshalers are rejected rather
than invoked across this boundary, preventing silent UTF-8 replacement and
stateful double encoding.
Store targets keep provider, location, and optional domain as an open transport
boundary. `DirectoryStore` and `SQLiteStore` select the current official
generations, while Engine ingress decides provider support. Queries omit the
executor. Migration and shadow commands accept exact-revision process targets.
The zero-value CLI transport installs a 30-second deadline; a positive Timeout
overrides it and an earlier caller Context deadline remains authoritative.
Every process target carries the complete ambient-cleared realization shown
above; there is no path-only `Run` or location-only plugin target. Use
`PinnedProcessPlugin` for migration and shadow providers.

Wait activation, cancellation, and Effect reconciliation return complete nested
receipts. Cancellation and Effect receipts retain the exact requested command;
Effect dispatches, component occurrences, and reconciliation commands all bind
the provider occurrence with an exact lowercase SHA-256 content ID.
Effect receipts separately retain the provider's actual resolution and value.
The SDK validates receipt IDs and Artifact references without deriving them.
`DurableEngine.Evolve` sends no process target for registry/rollout operations
and selects exactly one pinned migration or shadow target only when required.

Virtual scheduling helpers author typed claim, renewal, recovery, and Run-weight
commands. Compaction authoring requires the Rust-issued command ID, complete
bounded selections, and exact archive binding/revision. Region migration Plans
retain the immutable provider revision. The Go SDK exposes finite DTOs, not a
Virtual runtime, archive/migration provider, or complete persistence-receipt
transport. Rust alone selects work, resolves the pinned Plan, verifies archive
evidence, and admits claims; these helpers do not add Virtual operations to
`CliEngine` or `DurableEngine`.
