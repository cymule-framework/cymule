# Cymule Process Executor

`cymule-executor-process` is a hardened one-request-per-process implementation
of Cymule's `PluginHost` contract.

```sh
cargo add cymule-executor-process
```

It clears the ambient environment, accepts only explicit arguments/environment,
bounds request and output bytes, drains both output pipes concurrently, and
kills/reaps a child at the configured timeout. Timeout is reported as an
ambiguous plugin failure; effect retry and reconciliation remain owned by the
Cymule durable outbox.

This crate is a transport, not a sandbox. Use an OS/container/Wasm executor
plugin when untrusted-code isolation is required.
