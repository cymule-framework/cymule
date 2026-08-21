# Cymule SQLite Store

`cymule-store-sqlite` is the day-one embedded realization of Cymule's
provider-neutral `DurableStore`. A small transactional head points to an
authenticated checkpoint and at most 31 immutable content-addressed delta
segments. A mutation writes its segment and moves that head atomically; it does
not rewrite the complete `DurableState`.

```sh
cargo add cymule-store-sqlite
```

The adapter enables WAL and full synchronous durability for file databases and
uses a zero busy timeout. Writer contention is returned immediately as a
Cymule conflict; the application owns retry and backoff policy.

Status and inspection paths can use `SqliteStore::open_read_only`. That
constructor performs no WAL or schema configuration and rejects every CAS, so
an observer cannot contend as a writer or accidentally acquire mutation
authority. Each reopen observes the head and reads only its reachable
checkpoint/segment lineage inside one deferred read transaction; unrelated
rows are neither scanned nor treated as authority.

The old `cymule.sqlite-store/1` whole-state table is rejected during normal
open. Migrate it only while offline with:

```sh
cargo run -p cymule-store-sqlite --example migrate-sqlite-v1 -- /absolute/path/domain.sqlite
```

See [the migration runbook](../../docs/migrations/sqlite-store-v1-to-v2.md).

SQLite is appropriate for local development, desktop applications, and
single-node services. It does not provide distributed ownership or failover.
Conformance covers SQL statement rollback, transaction/CAS process death, WAL
integrity checks, and reopen. It does not claim a custom SQLite VFS, torn-sector,
or physical power-loss fault model.
