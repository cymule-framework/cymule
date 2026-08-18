# Cymule Agent Interaction Plugin

This optional Rust plugin maps Agent-domain state onto Cymule's generic durable
execution interfaces. It is useful when an application needs durable Sessions,
identified Agent-host calls, external input, workspace effects, or finalized
message/tool streams without placing those concepts in framework core.

It provides:

- protocol-neutral Session and content types;
- caller-driven Agent-host occurrence and recovery controllers;
- M1-backed input, workspace, and stream checkpoints;
- plugin-owned schemas, fixtures, and fault-oriented tests.

It deliberately does not implement an Agent Loop or a transport protocol. ACP,
MCP, A2A, editor, provider, and loop integrations should be separate plugins or
adapters that depend on this package when its domain model is useful.

The Rust package is named `cymule-agent`. Add it from crates.io:

```sh
cargo add cymule-agent
```

Use the controllers directly from `cymule_agent`; the application remains
responsible for interaction ordering and loop progress. See
[`PROFILE.md`](PROFILE.md) for the exact implemented behavior and remaining
gates.

Run the plugin suite from the repository root:

```sh
cargo test -p cymule-agent
```
