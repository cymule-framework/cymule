# Cymule Python SDK

This package authors `cymule.ir/2` Plan Candidates and calls a trusted Cymule
Engine. It has no runtime dependencies and does not implement semantic replay.

`FlowBuilder.definition()` adds a reusable definition to the same immutable
Plan and `invoke()` calls it with explicit input and result binding. The Python
SDK never resolves logical latest-compatible registry heads.

```python
from cymule import CliEngine, ResourceBuilder

resource = CliEngine("./target/debug/cymule").seal_resource(
    ResourceBuilder.text("input for another Run")
)
```

`ResourceBuilder.external` describes semantic objects, directories,
collections, snapshots, and live references without choosing a provider. Its
optional manifest argument pins exact list content; locator/access state stays
outside the candidate. Resource IDs are always validated and sealed by Rust.

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
