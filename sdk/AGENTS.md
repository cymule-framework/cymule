# SDK Guidance

- Every SDK emits the same frozen `cymule.ir/1` JSON shape and calls an Engine.
- SDKs must not compute authoritative Plan/Event IDs or implement a reducer.
- Keep APIs idiomatic in each language while preserving explicit site IDs,
  effect occurrence keys, scopes, risk profiles, and version information.
- Cross-language fixtures must produce the same Plan ID and execution result.
- Avoid runtime dependencies unless they materially improve correctness.

