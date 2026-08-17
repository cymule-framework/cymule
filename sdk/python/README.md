# Cymule Python SDK

This package authors `cymule.ir/1` Plan Candidates and calls a trusted Cymule
Engine. It has no runtime dependencies and does not implement semantic replay.

```python
from cymule import CliEngine, ResourceBuilder

resource = CliEngine("./target/debug/cymule").seal_resource(
    ResourceBuilder.text("input for another Run")
)
```

`ResourceBuilder.external` describes objects, directories, collections,
snapshots, and live references without choosing a provider. Resource IDs are
always validated and sealed by the Rust Engine.

`WaitActivationBuilder` creates provider-neutral signal and timer delivery
records. The Rust Engine verifies their closed wire shape; a durable runtime
still performs source matching and consume-once admission through CAS.

`VirtualWorkControl` describes occurrence queries and fenced resolution
commands independently of transport. `VirtualWorkControlBuilder` provides
success, retry, failure, and cancellation command helpers.
Region migration commands wrap adapter-produced split/merge plans; cursor
partitioning and coverage proof stay outside the SDK.
M3 also exposes certified compaction and exact partial-rehydration controls. A
`VirtualArchive` adapter stores immutable bytes only; Rust owns manifest
identity, certificate verification, and durable admission.
`VirtualSchedulingControl` adds capacity-slot claim, renewal, explicit expired
recovery, and future Run-weight commands while keeping Clock and worker-loop
behavior outside the Python SDK.
