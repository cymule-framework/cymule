# SQLite Durable Store Guidance

- This plugin implements the complete-state `DurableStore` CAS contract; it
  does not reinterpret or partition canonical M1 state.
- Keep `busy_timeout` at zero. SQLite writer contention must return a Cymule
  conflict immediately instead of waiting behind a database lock.
- Observation-only callers use `open_read_only`, which performs no schema or
  journal-mode writes and rejects CAS. Never make a status path initialize or
  reconfigure the database.
- Use an immediate transaction to compare the current revision and replace the
  state atomically. Serialized bytes and the canonical next revision are
  computed before acquiring the writer transaction.
- Reopen, stale writer, busy writer, committed-receipt loss, and corrupted-row
  tests are required. SQLite WAL and synchronous-full durability are adapter
  configuration, not framework semantics.
- Real process-death sweeps use the workspace-private `TestWorld` temporary
  domain and `ManagedChild` barrier/reap lifecycle. Do not reintroduce local
  child guards or wall-clock race polling.
- Process-kill workers construct the same explicit content-addressed execution
  binding before every open and reopen; the store never invents provider
  identity from a live manifest.
