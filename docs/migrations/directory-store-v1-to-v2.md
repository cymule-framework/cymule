# Directory Store v1 to v2 Offline Migration

Status: implemented.

The segmented directory store rejects a legacy `state.json` during normal open
and also rejects a directory containing both `state.json` and `head.json`.
Stop every reader and writer, preserve a copy of the complete directory, then
run:

```sh
cargo run -p cymule-directory-store --example migrate-directory-v1 -- /absolute/path/store
```

The command validates the legacy revision, publishes an authenticated
sequence-zero checkpoint and head, writes a migration receipt, and removes the
legacy state file. It is idempotently recoverable if a process dies after the
matching new head is durable but before `state.json` is removed. A mismatched
mixed head fails closed.

Afterward, open the store and compare its revision with `legacy_revision` in
`migration-v1-receipt.json`. If either differs, restore the preserved directory
copy. There is no online downgrade or runtime mixed-format fallback.
