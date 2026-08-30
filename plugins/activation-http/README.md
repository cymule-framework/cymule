# Cymule HTTP Activation

`cymule-activation-http` exposes an Axum router for durable signal ingress and a
matching `WaitSourceDriver`.

```sh
cargo add cymule-activation-http
```

`durable_signal_router` persists each exact request and its first selected wait
targets in a SQLite spool. `POST /v1/signals` returns success only after the
application admits and acknowledges the M1 activation CAS. If the process dies,
the client sees transport failure and an identical retry joins or observes the
retained delivery; conflicting reuse returns 409. Channel saturation or SQLite
contention returns 503 without changing activation semantics. The durable
router applies the same capacity bound to in-flight acknowledgement waiters.
After an initially pending acknowledgement read, each request waits for at most
one fixed window and then performs exactly one durable readback. A local
notification can end that window early; an independent process can rely on the
window expiry. If SQLite still reports pending, the handler returns 503 and the
producer retries the same activation ID rather than the server polling in the
background.

Request bodies have an explicit 2 MiB limit and are recursively decoded before
authorization so duplicate JSON members are rejected at every depth. A legal
raw body whose value expands beyond 2 MiB under canonical JSON is rejected with
413 before persistence. Other persistence and substrate failures remain 503;
they are not relabeled as producer size errors. SQLite is the acknowledgement
authority: in-process waiter notifications reduce latency, but every success is
confirmed by durable acknowledgement readback and cannot be lost if
acknowledgement races waiter registration or is committed by an independently
opened driver.

Every SQLite read that can replay ingress, select targets, acknowledge, or
classify an identical producer retry loads the complete retained row. The
driver requires strict canonical bytes for the value and target set, rebuilds
the original `HttpSignalRequest`, and recomputes its `request_digest` before it
can call parked-wait selection or return a delivery. Direct row corruption is
therefore an `Integrity` failure rather than a new signal interpretation or an
M1 activation candidate.

SQLite length metadata gates every variable column before Rust allocation.
Activation and signal keys use the 512-scalar/2,048-byte identity ceiling,
request digests use exactly 64 lowercase hexadecimal bytes, values use the
2 MiB ingress contract, and selected-target JSON uses the exact framework
target-count formula. Queries return capped BLOB projections for TEXT and
decode UTF-8 explicitly; oversized or invalid UTF-8 corruption remains
`Integrity` across hot, replay, durable-readback, and acknowledgement paths.

The durable driver redelivers retained selections first. A new target set is
checked against the bound of the exact call that selected it before SQLite can
retain it. After that point the complete retained set is the selection
authority: reopening with a smaller caller bound neither rejects, truncates,
nor reselects it, while the framework-wide target maximum still applies. New
selection pages the provider-neutral parked signal-key index with a fair cursor
and uses the SQLite `(acknowledged, signal_key, activation_id)` index, so an
arbitrary prefix of unrelated pending requests cannot starve a later matching
activation.

Hot fresh and retained reads use the exact `acknowledged = 0` predicate and
separate partial indexes for `selected_wait_ids IS NULL` and `IS NOT NULL`.
An arbitrarily large unselected prefix therefore cannot delay retained replay,
and retained rows never pollute fresh matching. Duplicate-ingress and
acknowledgement point reads use the activation primary index; none of these
paths permits a full table scan or temporary B-tree.

Every selected wait identity is an exact lowercase SHA-256 content ID. A
forged new identity fails before SQLite retains the target set; a malformed
retained identity is row corruption and returns `Integrity` before an M1
delivery.

The spool has one physical generation,
`cymule.activation-http-spool/2`. A completely empty UTF-8 database is
initialized atomically; every router, ingress, and acknowledgement connection
otherwise requires UTF-8 plus the exact singleton generation and fixed
table/index DDL before any configuration or data access. Metadata and
`sqlite_master` validation reads only bounded prefixes and at most the expected
object count plus one. Generation 1, UTF-16, partial, foreign, modified, or
oversized-metadata databases fail with `unsupported_store_generation` and are
not altered. This crate has no in-place upgrade or importer.

The SQLite spool is the only ingress, selection, and acknowledgement
authority. The crate exposes no process-local alternate router or driver.

Applications must supply an `HttpActivationAuthorizer`; `AllowAll` is provided
only for explicit local/test use. Typed user input remains owned by its
higher-profile input controller rather than being reinterpreted as a signal.
