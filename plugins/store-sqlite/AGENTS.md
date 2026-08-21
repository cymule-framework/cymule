# SQLite Durable Store Guidance

- This plugin implements the segmented `DurableStore` contract: immutable
  delta/checkpoint rows plus one small transactional head. It does not
  reinterpret or partition canonical M1 state.
- Keep `busy_timeout` at zero. SQLite writer contention must return a Cymule
  conflict immediately instead of waiting behind a database lock.
- Observation-only callers use `open_read_only`, which performs no schema or
  journal-mode writes and rejects CAS. Never make a status path initialize or
  reconfigure the database.
- Use an immediate transaction to compare the exact current head, insert
  immutable segment/checkpoint bytes, and move the head atomically. Validate
  all content identities and the resulting semantic revision before commit.
- `cymule.sqlite-store/1` migration is offline and explicit. Normal open must
  reject the legacy table and any mixed v1/v2 database; never add runtime
  fallback or automatic conversion.
- Reopen, stale writer, busy writer, committed-receipt loss, and corrupted-row
  tests are required. SQLite WAL and synchronous-full durability are adapter
  configuration, not framework semantics.
- One deferred read transaction owns head observation plus every reachable
  checkpoint, segment, and GC receipt read. Reopen never enumerates unrelated
  rows. SQL-trigger rollback tests prove statement/transaction boundaries;
  they do not claim SQLite VFS, torn-sector, or physical power-loss coverage.
- Real process-death sweeps use the workspace-private `TestWorld` temporary
  domain and `ManagedChild` barrier/reap lifecycle. Do not reintroduce local
  child guards or wall-clock race polling.
- After every killed boundary, run the complete `PRAGMA integrity_check`,
  checkpoint the WAL, repeat the check, and then reopen through `SqliteStore`.
  This proves process-death recovery at the CAS boundary, not failures inside a
  SQLite VFS operation or physical power loss.
- Process-kill workers construct the same explicit content-addressed execution
  binding before every open and reopen; the store never invents provider
  identity from a live manifest.
