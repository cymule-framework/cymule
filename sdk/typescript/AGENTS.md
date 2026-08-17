# TypeScript SDK Guidance

- Support maintained Node.js LTS lines and use strict TypeScript.
- Keep the package dependency-free at runtime.
- Do not depend on object insertion order for identity; the Rust engine performs
  canonicalization and sealing.
- Keep Resource unions closed and dependency-free. Never normalize URLs or hash
  Resource Candidates in TypeScript; `CliEngine.sealResource` is authoritative.
- Wait activation builders sort and deduplicate exact wait targets while
  preserving delivery, source, and Artifact identities. Engine verification is
  not stateful admission; consume-once remains a durable runtime CAS decision.
- Virtual work query/control types keep logical work, attempt occurrence,
  binding, owner, epoch, and disposition separate. Builders require stable
  command IDs; transports never apply retry policy locally.
- Region migrator/control types preserve opaque cursors and coverage evidence.
  Never parse cursor positions or synthesize split/merge coverage in the SDK.
- Compaction and rehydration builders sort/deduplicate causal cuts and occurrence
  selections. Archive adapters store exact bytes under a framework reference;
  they never create or validate certificates in TypeScript.
- Scheduling builders sort/deduplicate capabilities and require explicit slot,
  work/lease fences, logical times, TTL, and recovery disposition. They never
  read `Date.now()`, manage workers, or infer retryability.
- Use discriminated unions for IR and Engine protocol types.
- Keep `invoke` as a closed discriminated variant and `definition()` as a pure
  candidate authoring operation; neither may resolve logical latest heads.
- Keep M4 operations as the closed `EvolutionCommand` discriminated union.
  `EvolutionControlBuilder` copies caller data but never executes adapters,
  counts evidence, or chooses promotion/rollback.
- The public npm package name is `cymule`. Changes to exports, files, engine
  requirements, or minimum Node versions require a package dry-run and release
  workflow review.
- npm publication uses GitHub Actions trusted publishing with provenance. Never
  add a long-lived npm token to repository or organization secrets.
