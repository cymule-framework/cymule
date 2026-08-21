# Cymule Process Executor

`cymule-executor-process` is a hardened one-request-per-process implementation
of Cymule's `PluginHost` contract.

```sh
cargo add cymule-executor-process
```

At construction, the executor captures the selected executable and optional
working-directory tree. Every request runs from a fresh private materialization,
so a plugin that changes its own file or working data cannot change a later
occurrence. Its canonical implementation revision covers the executable,
arguments, explicit environment, working tree, declared runtime closure,
deadline, and byte limits; use that revision in `cymule.execution-binding/1`.
`ProcessExecutorConfig::runtime_closure` starts with the host OS/architecture
ABI. Deployments that rely on a mutable interpreter, loader, or sidecar must
replace or extend that map with the immutable admitted revision of each such
facility.

Each request runs in a dedicated Unix process group with no inherited
environment. Stdin, stdout, and stderr use bounded nonblocking I/O under one
absolute deadline. On exit, timeout, or I/O failure, the executor terminates the
whole group, reaps its leader, and drains available pipe data. Forked children
cannot keep a request open after the leader returns. Stderr remains diagnostic
only. A response lost after Effect dispatch starts is an unknown-world outcome;
reconciliation remains owned by the Cymule durable outbox.

This crate supports Unix process-group and permission semantics; construction
fails on other platforms. It is a transport, not a sandbox: a hostile process
can deliberately leave a process group or exercise the caller's same-UID OS
authority. Use an OS/container/Wasm executor plugin for untrusted code.
