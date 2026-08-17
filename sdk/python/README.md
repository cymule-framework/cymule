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

`CliEngine.verify_agent_stream(records)` sends `cymule.agent-stream/1` records
to the Rust reducer. Chunks are staging; only an explicit validated final record
represents durable message or tool output.
