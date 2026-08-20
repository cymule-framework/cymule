# Cymule Process Executor

`cymule-executor-process` is a hardened one-request-per-process implementation
of Cymule's `PluginHost` contract.

```sh
cargo add cymule-executor-process
```

It copies the selected executable into a private location and exposes the digest
of those exact launch bytes for `cymule.execution-binding/1`. It clears the
ambient environment, accepts only explicit arguments/environment, bounds request
and output bytes, drains both output pipes concurrently, and kills/reaps a child
at the configured timeout. Stderr remains diagnostic-only. A response lost after
Effect dispatch starts is an unknown-world outcome; reconciliation remains owned
by the Cymule durable outbox.

This crate is a transport, not a sandbox. Use an OS/container/Wasm executor
plugin when untrusted-code isolation is required.
