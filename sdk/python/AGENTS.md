# Python SDK Guidance

- Support maintained CPython versions with no runtime dependencies.
- Use typed dictionaries or dataclasses for public contracts and preserve exact
  wire names.
- Subprocess errors must include bounded stderr and never expose environment
  variables or credentials.
- Raise `EngineError` with the exact structured `failure` object for all Engine
  responses. A timed-out mutating Run is an unknown-world outcome, not a safe
  generic retry.
- The Rust engine remains the only authoritative sealer and reducer.
- `definition()` and `invoke()` emit exact `cymule.ir/2` wire records. Python
  must not resolve or cache logical subflow heads.
- Resource builders preserve exact wire names and send candidates to the Rust
  engine. Do not add a Python Resource ID implementation or accept credentials
  in public URL helpers.
- Wait activation builders sort and deduplicate exact targets while preserving
  delivery, source, and Artifact identities. Rust Engine verification is not a
  substitute for durable CAS admission against pending waits.
- `DurableControlBuilder` emits the complete M1 command/query union and copies
  caller JSON defensively; no Python code reduces durable state.
- Virtual work query/control types preserve command, occurrence, binding,
  owner, epoch, and disposition fields. SDK transports do not classify errors
  or decide retry policy.
- Region migration types preserve opaque cursor maps, pinned adapter binding,
  and coverage evidence. Python clients never split cursor strings locally.
- Archive protocols expose immutable bytes without provider locators in semantic
  commands. Compaction/rehydration builders preserve the pinned binding, causal
  cut, certificate, and exact occurrence selection; Rust validates them.
- Scheduling protocols keep capacity slots, logical time, work/lease fences,
  Run weight, and recovery disposition explicit. Do not read a local clock or
  add a Python worker registry/reducer.
- Evolution builders preserve the closed M4 operation tag, exact Plan and
  Artifact identities, adapter requests, observations, and gates. Python never
  resolves latest revisions or evaluates a gate locally.
- Unified live-evolution builders preserve the parent template, nested command,
  publication evidence, and exact safe-point proof. They never implement
  reverse-dependency relinking or split the durable command.
- Migration/restart builders preserve safe-point proof identity, source epoch,
  distinct Run IDs, explicit input, and evidence without local interpretation.
