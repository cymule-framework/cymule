# Durable Single-Domain Guidance

- This crate owns provider-neutral persistence and recovery contracts. It must
  not name a database, queue, object store, cloud, or transport product.
- A durable write is compare-and-swap over one complete `DurableState` revision.
  Adapters must not acknowledge a partial state transition.
- Continuations contain only typed canonical references and explicit logical
  positions. Never persist process memory, closures, host-language stacks, or
  ambient time.
- Wait completion, lease acquisition, outbox claims, occurrence recording, and
  snapshot publication must be idempotent and fenced.
- Concrete storage belongs under `plugins/` and must pass this crate's shared
  conformance suite, including reopen and stale-writer tests.
- M1 changes require updates to the profile document, fault matrix, schemas,
  SDK control surfaces, and restart-level tests.
