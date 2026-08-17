# SDK Guidance

- Every SDK emits the same frozen `cymule.ir/2` JSON shape and calls an Engine.
- Every SDK exposes reusable definition declaration and `invoke` authoring with
  the same explicit local definition ID, input expression, site ID, and result
  binding. Linking logical latest-compatible references remains Rust authority.
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
- Virtual work SDK contracts preserve stable control command and occurrence
  identities, owner/work/lease fencing, logical observation time, immutable
  binding, and closed disposition variants. SDKs expose transport interfaces
  but no local state reducer.
- Region migration contracts preserve exact source cursors, target descriptors,
  migration binding, and evidence across languages. No SDK interprets cursor
  positions or certifies coverage.
- Archive contracts expose immutable byte storage and typed compaction/
  rehydration controls only. SDKs preserve causal cuts, certificate identity,
  replay availability, and exact occurrence selections; Rust verifies content
  identity and performs M1/M3 admission.
- Scheduling clients preserve capacity-slot, work epoch, lease epoch, logical
  time, capabilities, binding, and explicit recovery disposition. They do not
  discover workers, read clocks, infer expiry, classify failures, or reduce
  state locally. Run-weight updates are typed future scheduling commands.
- Avoid runtime dependencies unless they materially improve correctness.
