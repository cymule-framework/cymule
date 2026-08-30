# Cymule Python SDK

This package authors `cymule.ir/3` Plan Candidates and calls a trusted Cymule
Engine. It has no runtime dependencies and does not implement semantic replay.

`FlowBuilder.definition()` adds a reusable definition to the same immutable
Plan and `invoke()` calls it with explicit input and result binding. The Python
SDK never resolves logical latest-compatible registry heads.

`FlowBuilder.component()` requires the Plan-owned output Artifact kind as an
explicit argument; it never defaults one. Ordinary components use
`cymule.component-output/1`. Resource-producing components use the exact
`cymule.typed-json/sha256-...` kind derived from the sealed Resource Handle
contract, never the logical framework type key. The Engine validates the
returned value against the declared output schema before canonical JSON is
stored under that kind.

Until this terminal generation is published, build the Engine and run the SDK
from the same reviewed source checkout. The older `0.2.0` packages do not carry
the current breaking wire contract.

```sh
cargo build -p cymule-cli
uv run --project sdk/python --frozen python your_program.py
```

```python
from cymule import (
    CliEngine,
    DurableEngine,
    EngineCancellation,
    ResourceBuilder,
    process_plugin,
    sqlite_clock,
    sqlite_store,
)

cancellation = EngineCancellation()
engine = CliEngine(
    "./target/debug/cymule",
    cancellation=cancellation,
)
resource = engine.seal_resource(
    ResourceBuilder.text("input for another Run")
)
```

`ResourceBuilder.external` describes semantic objects, directories,
collections, snapshots, and live references without choosing a provider. Its
optional manifest argument pins exact list content; locator/access state stays
outside the candidate. Resource IDs are always validated and sealed by Rust.

`DurableEngine(store, plugin, clock)` uses the selected CLI Engine for real
durable `start`, `run_index_page`, `run_current`, bounded Run child pages,
exact `run_item`, `resume`, `takeover`, `signal`, `release`,
`resolve_effect`, `cancel`, and `evolve` operations. There is no separate generic
control-submit protocol. Python
does not replay Continuations or reduce state. Non-finite numbers, duplicate
response keys, and integers outside the shared safe JSON range fail closed.
The client speaks only `cymule.engine/5`. `evolve` returns the complete durable
receipt—journal, exact echoed command, and recursively validated outcome—and
classifies any post-mutation receipt mismatch as requiring reconciliation.
Every successful Engine call also echoes the complete raw inner request; the
client verifies that echo against the exact JSON snapshot sent before exposing
the payload. Verification calls additionally require the returned activation or
command to equal the submitted wire value. Resource sealing validates the full
Handle integrity and manifest relationships and binds the returned descriptor
to the submitted Candidate. Optional structured-failure members are
omission-only, and malformed post-mutation responses require reconciliation.
Cancellation and claimed-effect resolution return complete request-bound
receipts while Rust remains the sole Artifact identity authority. Stdout and
stderr are drained by one nonblocking selector with independent 128 MiB plus 32-byte framing
response-envelope and 1 MiB diagnostic limits. A custom transport implements
only `exchange(request)` and returns the complete accepted request plus
response. Every ordinary exception raised while validating that returned
success becomes request-aware response loss; process-control `BaseException`
signals are not swallowed. Strict JSON retains exact fractional decimal
evidence through echo admission and rejects nesting beyond 128 levels. A finite
positive deadline or SDK-owned `EngineCancellation` kills only the official
direct Child when still live, closes local descriptors, and reaps it. Arbitrary
cancellation callbacks are unsupported: `cancel()` and the sole subprocess
launch share one lock, so cancellation that wins the launch gate never calls
`Popen`, while cancellation after launch uses the post-start outcome rules.
Admitted success and valid remote failure use the same lock for a per-call
completion election: completion that wins is returned unchanged, while
cancellation that wins uses the started-request classification. One completed
call does not complete a token shared by concurrent calls. The Engine/executor
watchdog owns descendant closure; the SDK never signals a raw PID or PGID after
reaping.
Run IDs, execution owners, and Clock source/scope identities accept 1..512
printable Unicode scalar values. Queries carry required-null revision/cursor
members plus explicit item and canonical-byte budgets; there is no query ID or
full Run/domain mirror.
The persistence-backed Clock issues the opaque reference used by one exact
driver claim:

```python
clock = sqlite_clock("./clock.sqlite", "clock:local", "sha256:" + "0" * 64)
durable = DurableEngine(
    sqlite_store("./runs.sqlite", "local"),
    process_plugin({
        "executable": "/opt/cymule/bin/component-plugin",
        "arguments": [],
        "environment": {},
        "working_directory": None,
        "runtime_closure": {"component-runtime": "sha256:" + "a" * 64},
        "timeout_ms": 60_000,
        "message_limit": 8 * 1024 * 1024,
        "closure_limit": 64 * 1024 * 1024,
    }),
    clock,
    engine,
)
run_id = "run:example"
clock_ref = durable.observe_clock(run_id)
durable.start(
    run_id,
    candidate,
    input_value,
    {"owner": "driver:example", "clock": clock_ref, "ttl": 30},
)
```

The SDK never seals or hashes a future Clock receipt locally.
Process-backed targets always carry the complete closed `EngineProcessConfig`;
there is no path-string overload, ambient environment, implicit working
directory, or default process deadline/limit. An exact nested migration carries
only its pinned migration target, an exact nested shadow carries only its pinned
shadow target, and every other evolution command carries neither, even when the
client is configured with both providers. Durable wait activation, Effect
reconciliation, and cancellation return their current nested receipts. Each
receipt retains the complete admitted activation or normalized command, while
requested Effect resolution remains distinct from the provider's actual
terminal resolution and does not duplicate Run world settlement. A provider
`NotApplied` result is the closed `effect_not_applied` boundary with its exact
content-addressed intent. Applied Effect summaries, exact Effect records, and
resolution receipts all require a non-null result Artifact of exact kind
`cymule.effect-result/1`; every non-applied state carries null. An explicitly
present Engine failure `issues` list has 1..100 entries, while no issues is
represented by omitting the member.
Effect dispatches, component occurrences, and reconciliation commands all bind
the provider occurrence with an exact lowercase SHA-256 content ID.
Store targets keep provider, location, and optional domain as an open transport
boundary. `directory_store` and `sqlite_store` select the current official
generations, while Engine ingress decides provider support. Queries omit the
executor; migration and shadow commands accept exact-revision process targets.


`WaitActivationBuilder` creates provider-neutral signal and timer delivery
records. The Rust Engine verifies their closed wire shape; a durable runtime
still performs source matching and consume-once admission through CAS.

`VirtualWorkControlBuilder` provides success, retry, failure, and cancellation
command helpers plus exact revision-pinned migration Plan authoring. Compaction
authoring requires its Rust-issued command ID, complete bounded
work/occurrence/archived-command sets, and exact archive binding/revision.
Finite Work occurrence and certificate DTOs mirror Rust's contracts without
interpreting cursors or deriving identities. Scheduler execution, archive and
migration providers, complete persistence receipts, and verified claim outcomes
remain Rust-only; Python exposes no Virtual runtime or provider transport.
`VirtualSchedulingControlBuilder` creates capacity-slot claim, renewal,
explicit expired recovery, and future Run-weight commands while keeping Clock
and worker-loop behavior outside the Python SDK. These authoring helpers do not
add Virtual operations to `CliEngine` or `DurableEngine`.
