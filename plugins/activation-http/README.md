# Cymule HTTP Activation

`cymule-activation-http` exposes an Axum router for durable signal ingress and a
matching `WaitSourceDriver`.

```sh
cargo add cymule-activation-http
```

`POST /v1/signals` accepts an activation ID, signal key, and JSON value. The
request does not return success when it enters an in-memory queue: it waits
until the application drives the source and acknowledges the committed M1 CAS.
Channel saturation returns 503 so the producer can retry the same identity.

Applications must supply an `HttpActivationAuthorizer`; `AllowAll` is provided
only for explicit local/test use. Typed user input remains owned by its
higher-profile input controller rather than being reinterpreted as a signal.
