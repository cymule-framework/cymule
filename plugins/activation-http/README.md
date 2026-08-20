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
contention returns 503 without changing activation semantics.

The durable driver redelivers retained selections first. New selection pages
the provider-neutral parked signal-key index with a fair cursor and uses the
SQLite `(acknowledged, signal_key, activation_id)` index, so an arbitrary prefix
of unrelated pending requests cannot starve a later matching activation.

`signal_router` remains an explicitly process-local embedding option. It has
the same acknowledgement contract but is not the production restart boundary.

Applications must supply an `HttpActivationAuthorizer`; `AllowAll` is provided
only for explicit local/test use. Typed user input remains owned by its
higher-profile input controller rather than being reinterpreted as a signal.
