# Cymule Python SDK

This package authors `cymule.ir/2` Plan Candidates and calls a trusted Cymule
Engine. It has no runtime dependencies and does not implement semantic replay.

`FlowBuilder.definition()` adds a reusable definition to the same immutable
Plan and `invoke()` calls it with explicit input and result binding. The Python
SDK never resolves logical latest-compatible registry heads.

```sh
cargo install cymule-cli --version 0.2.0
python -m pip install cymule==0.2.0
```

```python
from cymule import CliEngine, DurableEngine, ResourceBuilder

resource = CliEngine().seal_resource(
    ResourceBuilder.text("input for another Run")
)
```

`ResourceBuilder.external` describes semantic objects, directories,
collections, snapshots, and live references without choosing a provider. Its
optional manifest argument pins exact list content; locator/access state stays
outside the candidate. Resource IDs are always validated and sealed by Rust.

`DurableEngine(store, plugin)` uses the installed CLI for real durable
`start`, `get`, `resume`, `signal`, `release`, and `evolve` operations. Python
does not replay Continuations or reduce state. Non-finite numbers, duplicate
response keys, and integers outside the shared safe JSON range fail closed.
Migration and shadow evolution commands require a transport with their pinned
adapter or driver binding; the local CLI rejects them when unbound.


`WaitActivationBuilder` creates provider-neutral signal and timer delivery
records. The Rust Engine verifies their closed wire shape; a durable runtime
still performs source matching and consume-once admission through CAS.

`VirtualWorkControl` describes occurrence queries and fenced resolution
commands independently of transport. `VirtualWorkControlBuilder` provides
success, retry, failure, and cancellation command helpers.
Region migration commands wrap adapter-produced split/merge plans; cursor
partitioning and coverage proof stay outside the SDK.
Virtual work also exposes certified compaction and exact partial-rehydration
controls. A
`VirtualArchive` adapter stores immutable bytes only; Rust owns manifest
identity, certificate verification, and durable admission.
`VirtualSchedulingControl` adds capacity-slot claim, renewal, explicit expired
recovery, and future Run-weight commands while keeping Clock and worker-loop
behavior outside the Python SDK.
