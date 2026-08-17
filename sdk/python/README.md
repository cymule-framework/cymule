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
