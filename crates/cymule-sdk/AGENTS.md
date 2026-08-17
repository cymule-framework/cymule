# Rust SDK Guidance

- The SDK is an authoring and client facade. It does not own semantic reduction.
- Builders must emit the same `cymule.ir/2` objects as other language SDKs.
- Rust builder definitions and invocations must match the TypeScript, Python,
  and Go wire shape; they never resolve logical registry heads locally.
- Keep convenient APIs lossless: effect risk, occurrence identity, scopes, and
  version information must remain explicit in the emitted plan.
- CLI transport is one Engine implementation, not the semantic definition.
- Resource builders emit `cymule.resource/1` candidates. Only the Rust Engine
  seals Resource IDs; the SDK must not duplicate the resource canonicalizer.
- Wait activation DTOs preserve stable delivery, source, target, and Artifact
  identities. CLI verification covers the closed record; only a durable runtime
  CAS can admit it against pending waits and enforce consume-once semantics.
- `VirtualWorkControl` is transport-neutral. Preserve stable command and
  occurrence IDs, immutable binding, owner, work epoch, lease epoch, and
  logical observation time; SDK transports never implement retry/failure
  reduction locally.
- Region migration clients preserve opaque cursors, exact source preconditions,
  pinned migration binding, and coverage evidence. SDKs never split cursor
  strings or infer partition coverage.
- Re-export the provider-neutral archive and typed compaction/rehydration
  controls without adding a second validator. `VirtualWorkControl` transports
  commands; the M1-backed Rust controller remains admission authority.
- `VirtualSchedulingControl` transports claim, renewal, expired recovery, and
  future Run-weight commands. Preserve both work and lease fences plus logical
  observation time; never turn the SDK into a worker loop or scheduler.
- `EvolutionControl` transports closed `cymule.evolution-control/1` commands.
  Re-export Rust M4 DTOs without adding client-side latest resolution,
  migration/shadow execution, evidence counting, or rollout decisions.
