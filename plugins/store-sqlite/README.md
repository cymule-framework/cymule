# Cymule SQLite Store

`cymule-store-sqlite` is the day-one embedded realization of Cymule's
provider-neutral `DurableStore`. It stores one complete single-domain
`DurableState` behind a transactional revision compare-and-swap.

```sh
cargo add cymule-store-sqlite
```

The adapter enables WAL and full synchronous durability for file databases and
uses a zero busy timeout. Writer contention is returned immediately as a
Cymule conflict; the application owns retry and backoff policy.

SQLite is appropriate for local development, desktop applications, and
single-node services. It does not provide distributed ownership or failover.
