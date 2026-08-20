# SDK Guidance

- Every SDK emits the same frozen `cymule.ir/2` JSON shape and calls an Engine.
- Every CLI client sends and receives only `cymule.engine/2`. Surface a typed
  Engine error that preserves the Rust failure object; never parse stderr or a
  human message into semantic categories, and never recommend replay merely
  because the transport ended without a response.
- Engine response JSON must reject duplicate object members recursively before
  shape validation. SDKs must not rely on the host parser's last-key-wins
  behavior or retry with a permissive decoder.
- Engine envelopes require response/error exclusivity. Validate the exact
  success tag and nested discriminated unions, including execution and returned
  evolution commands, before returning any payload to application code.
- Every language exposes one high-level `DurableEngine` with stateful `start`,
  `get`, `resume`, `signal`, `release`, and `evolve` methods backed by the Rust
  CLI authority. Validation-only transport is never a durable operation.
- Reject duplicate JSON object keys, non-finite numbers, and integers outside
  the shared exact range before accepting a response or sending a request.
  Deadlines and cancellation preserve structured failure parity; response loss
  after mutation begins requires reconciliation.
- Contract issue decoding preserves both the failing value `path` and the
  failing `schema_path`; neither SDK may flatten them into display text.
- Every SDK exposes reusable definition declaration and `invoke` authoring with
  the same explicit local definition ID, input expression, site ID, and result
  binding. Linking logical latest-compatible references remains Rust authority.
- Every SDK preserves optional `wait.bind` and the closed Embedded
  completed-or-suspended outcome. Suspension has no client-side Continuation.
- SDKs must not compute authoritative Plan/Event IDs or implement a reducer.
- Every SDK exposes the same closed `cymule.evolution-control/3` command union
  and transport interface. SDKs construct commands only; Rust resolves module
  revisions, invokes pinned migration/shadow plugins, counts observations, and
  admits promotion or rollback.
- Every SDK also exposes `cymule.live-evolution-control/2`, which scopes the
  existing Plan operations to one parent template and adds definition
  publication, template registration, atomic publish/relink, and required
  migration/restart safe-point proofs. SDKs never sequence these writes.
- Migration and restart commands carry exact safe-point IDs and source epochs.
  SDKs never derive safe points, reinterpret old state, or initialize the
  replacement Run locally.
- SDKs also author the same semantic-only `cymule.resource/2` candidates and
  producer-provenance `cymule.resource-handoff/2` wire records. Locators, grants,
  signed URLs, and credential revisions never enter candidates. SDKs delegate
  Resource ID validation and sealing to the Rust Engine.
- Keep APIs idiomatic in each language while preserving explicit site IDs,
  effect occurrence keys, scopes, risk profiles, and version information.
- Cross-language fixtures must produce the same Plan ID, Resource ID, and
  execution result.
- Every SDK preserves the required `cymule.artifact/2` identity version on every
  Artifact reference. SDKs neither derive Artifact IDs nor substitute a local
  typed-contract registry alias for the exact contract pinned by Rust.
- Wait activation clients must preserve `cymule.wait-activation/1` delivery,
  source, exact targets, and Artifact identity. All SDKs submit the shared
  fixture to the Rust Engine; only a durable runtime admits it against state.
- Every SDK exposes the same closed `cymule.durable-control/1` mutations and
  queries. Builders normalize only set-like target ordering; Rust alone seals
  Plans/Artifacts and admits Continuation, wait, or effect transitions.
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
