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
authorization so duplicate JSON members are rejected at every depth. SQLite is
the acknowledgement authority: in-process waiter notifications reduce latency,
but every success is confirmed by durable acknowledgement readback and cannot
be lost if acknowledgement races waiter registration or is committed by an
independently opened driver.

Every SQLite read that can replay ingress, select targets, acknowledge, or
classify an identical producer retry loads the complete retained row. The
driver requires strict canonical bytes for the value and target set, rebuilds
the original `HttpSignalRequest`, and recomputes its `request_digest` before it
can call parked-wait selection or return a delivery. Direct row corruption is
therefore an `Integrity` failure rather than a new signal interpretation or an
M1 activation candidate.

The durable driver redelivers retained selections first. A new target set is
checked against the bound of the exact call that selected it before SQLite can
retain it. After that point the complete retained set is the selection
authority: reopening with a smaller caller bound neither rejects, truncates,
nor reselects it, while the framework-wide target maximum still applies. New
selection pages the provider-neutral parked signal-key index with a fair cursor
and uses the SQLite `(acknowledged, signal_key, activation_id)` index, so an
arbitrary prefix of unrelated pending requests cannot starve a later matching
activation.

The spool has one physical generation,
`cymule.activation-http-spool/1`. A completely empty database is initialized
atomically; every router, ingress, and acknowledgement connection otherwise
requires the exact singleton generation and fixed table/index DDL before any
configuration or data access. Older, partial, foreign, or modified databases
fail with `unsupported_store_generation` and are not altered. This crate has no
in-place upgrade or importer.

The SQLite spool is the only ingress, selection, and acknowledgement
authority. The crate exposes no process-local alternate router or driver.

Applications must supply an `HttpActivationAuthorizer`; `AllowAll` is provided
only for explicit local/test use. Typed user input remains owned by its
higher-profile input controller rather than being reinterpreted as a signal.
