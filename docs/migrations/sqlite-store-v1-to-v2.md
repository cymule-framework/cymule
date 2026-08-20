# SQLite Store v1 to v2 Offline Migration

Status: implemented.

## Purpose

`cymule.sqlite-store/2` replaces the per-mutation `cymule_state.state_json`
rewrite with a small CAS head, immutable content-addressed delta segments, and
periodic authenticated checkpoints. Runtime open is terminal: it accepts v2 or
an empty database and rejects v1. It never reads both formats or migrates on
startup.

## Preconditions

- Stop every writer and read-only observer that uses the database.
- Copy the database and its `-wal` and `-shm` companions as one recoverable
  backup, or checkpoint the WAL before copying.
- Confirm the input contains `cymule_state` and does not contain
  `cymule_heads`. A database containing both formats is rejected.

## Execute

From a checkout containing the target Cymule release:

```sh
cargo run -p cymule-store-sqlite --example migrate-v1 -- /absolute/path/domain.sqlite
```

The command obtains an exclusive SQLite transaction with zero busy timeout,
validates every legacy revision against its canonical `DurableState`, writes
one sequence-zero v2 checkpoint and head per domain, persists a content-addressed
migration receipt, drops `cymule_state`, and commits once. Any validation,
contention, or write failure rolls back the complete migration.

## Verification

- Preserve the printed receipt JSON with the change record.
- Open every expected domain through `SqliteStore::open_read_only` and compare
  its semantic revision with `legacy_revision` in the receipt.
- Run `PRAGMA integrity_check` against the migrated database.
- Confirm `cymule_state` is absent and `cymule_store_meta.schema_version` is
  `cymule.sqlite-store/2`.

## Stop and rollback

Do not start a new writer when the command fails, a domain receipt is missing,
or a readback revision differs. Restore the complete pre-migration database
backup. There is no online downgrade and no mixed-format fallback.
