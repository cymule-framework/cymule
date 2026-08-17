# SDK Guidance

- Every SDK emits the same frozen `cymule.ir/1` JSON shape and calls an Engine.
- SDKs must not compute authoritative Plan/Event IDs or implement a reducer.
- SDKs also author the same `cymule.resource/1` candidates and Run handoff wire
  records. They delegate Resource ID validation and sealing to the Rust Engine.
- Keep APIs idiomatic in each language while preserving explicit site IDs,
  effect occurrence keys, scopes, risk profiles, and version information.
- Cross-language fixtures must produce the same Plan ID, Resource ID, and
  execution result.
- Wait activation clients must preserve `cymule.wait-activation/1` delivery,
  source, exact targets, and Artifact identity. All SDKs submit the shared
  fixture to the Rust Engine; only a durable runtime admits it against state.
- Avoid runtime dependencies unless they materially improve correctness.
