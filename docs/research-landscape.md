# Research and Implementation Landscape

Status: informative snapshot, reviewed 2026-08-17.

Cymule borrows mechanisms, not semantic ownership, from maintained systems and
standards.

## Durable execution

- [Temporal](https://docs.temporal.io/) demonstrates mature durable workflow
  histories, deterministic replay, worker versioning, and operational tooling.
  Its host-language workflow replay is intentionally not Cymule's canonical IR.
- [Restate](https://docs.restate.dev/foundations/services) demonstrates a small
  service model, journaled context operations, immutable deployments, durable
  promises, and TypeScript/Java/Python/Go/Rust SDK coverage. Cymule adopts the
  clear handler/runtime boundary while keeping program meaning in its own plan.
- [DBOS](https://docs.dbos.dev/) demonstrates a lightweight library experience,
  durable steps, queues, and recovery centered on a transactional database.
  Cymule keeps persistence as a contract rather than a required product class.
- [Dapr components](https://docs.dapr.io/concepts/components-concept/) show how
  stable building-block interfaces can support built-in and independently
  deployed implementations. Cymule similarly separates semantic operations from
  plugins, but plugin availability is not authority.
- [OCI content descriptors](https://github.com/opencontainers/image-spec/blob/main/descriptor.md)
  separate media type, digest, size, and optional retrieval URLs. Cymule's
  cross-Run Resource Handle adopts that content-proof separation without
  adopting the container-image domain model.
- [Apache OpenDAL](https://opendal.apache.org/) provides a maintained Rust data
  access layer across object stores, filesystems, WebDAV, and other services.
  Cymule can reuse it inside optional Artifact resolver/store plugins while the
  framework contract remains provider-neutral and credentials stay outside
  canonical state.

## Compiler and component boundaries

- [MLIR dialects](https://mlir.llvm.org/docs/DefiningDialects/) and
  [interfaces](https://mlir.llvm.org/docs/Interfaces/) support progressive
  lowering and generic analyses without hard-coding every operation. Cymule uses
  MLIR only in an optional workbench; the frozen runtime IR remains smaller.
- [MLIR bytecode](https://mlir.llvm.org/docs/BytecodeFormat/) explicitly ties
  compatibility to dialect immutability or dialect-owned upgrades. Cymule keeps
  separate IR and canonical-encoding version domains for the same reason.
- [WebAssembly Component Model WIT](https://component-model.bytecodealliance.org/design/wit.html)
  provides language-neutral, single-focus interfaces and explicit imports. It is
  the preferred direction for a future sandboxed plugin ABI after current WASI
  async/toolchain support is sufficiently uniform.

## Composition and agent interaction

- [Cordis](https://github.com/cordiverse/cordis) demonstrates a small Context,
  Service registry, dependency injection, scoped isolation/interception, Fiber
  lifecycle, and disposable effects. Cymule adopts the interface/lifecycle
  separation, while durable identity and replay remain owned by its Rust
  semantic profiles rather than an ambient runtime context. It does not adopt
  an Agent loop: caller-owned orchestration supplies individually identified
  interactions to the durable boundary.
- [Agent Client Protocol](https://agentclientprotocol.com/protocol/overview)
  separates accepted prompts from ordered session updates, typed content, tool
  status, permission requests, elicitation, plans, usage, and terminal state.
  An optional Agent integration plugin can map these to durable occurrences and
  projections instead of making a transport session canonical.
- [ACP Message ID](https://agentclientprotocol.com/rfds/message-id) identifies
  chunks by stable message identity instead of guessing boundaries from update
  type or timing. The Agent interaction plugin requires a stream and target
  identity, then adds durable sequence and explicit finalization semantics
  below the adapter.
- [ACP additional workspace roots](https://agentclientprotocol.com/rfds/additional-directories)
  keeps client-mediated filesystem capability and boundary enforcement outside
  the Agent. An Agent plugin can likewise keep concrete path and sandbox policy
  in adapters while lowering the mutation to framework effects and evidence.
- [Model Context Protocol](https://modelcontextprotocol.io/specification/) keeps
  resources, prompts, tools, user input, and asynchronous Tasks as capability
  surfaces. Optional adapters can translate these objects and pin their selected
  domain occurrence without making MCP part of Cymule core.
- [MCP progress and Tasks](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)
  separate optional progress notifications from durable terminal task results.
  The Agent interaction plugin likewise treats progress/chunks as non-final
  staging and does not rely on notification delivery as Session authority.
- [A2A](https://a2a-protocol.org/dev/specification/) distinguishes Messages,
  Tasks, status updates, and Artifacts. An A2A adapter can use the same
  communication versus durable-output distinction without adding A2A bindings
  to core.
- [A2A streamed artifact updates](https://a2a-protocol.org/v0.2.0/specification/)
  expose append and `lastChunk` explicitly. An Agent plugin adapter can map
  those fields to contiguous chunks and finalization while M1 CAS remains
  durable truth.
- [Kubernetes finalizers](https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/)
  separate an accepted lifecycle decision from external cleanup obligations.
  Cymule scope commit similarly closes internal state while unresolved Effect
  obligations remain explicit and blocking rather than pretending the world
  settled atomically.
- [Temporal](https://docs.temporal.io/) demonstrates durable resumption around
  external activities. Cymule uses a smaller provider-neutral occurrence and
  outbox boundary, with explicit `unknown` reconciliation instead of assuming a
  retry proves whether the original workspace mutation happened.

## State scale and live evolution

- [Apache Flink checkpoints and savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpoints_vs_savepoints/)
  distinguish fast runtime recovery from portable, user-owned upgrade state.
  Cymule similarly separates automatic checkpoints from portable canonical
  savepoints and requires serializer/schema evidence for M4 migration.
- [Flink state schema evolution](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/)
  demonstrates why key migrations and serializer compatibility must fail closed.
- [Restate versioning](https://docs.restate.dev/services/versioning) keeps
  deployments immutable and pins in-flight invocations while new work advances.
  This informs Cymule's occurrence binding and Plan/Binding separation.

## Encoding and schemas

- [RFC 8785 JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785.html)
  defines invariant JSON bytes suitable for content addressing.
- [JSON Schema 2020-12](https://json-schema.org/specification) is the schema
  dialect used for Plan inputs, outputs, plugin operations, and public fixtures.
- [`jsonschema`](https://github.com/Stranger6667/jsonschema) provides the
  maintained Rust Draft 2020-12 compiler used by the optional Agent interaction
  plugin. The plugin disables default HTTP and filesystem resolvers so a
  validation boundary cannot become ambient I/O; internal references remain
  supported.

## Deliberate differences

Cymule does not re-execute arbitrary host-language orchestration as canonical
replay, does not make a database transaction the universal world-effect model,
does not treat a plugin catalog as authority, and does not claim external
exactly-once when a provider cannot prove it. Its contribution is the closure of
causal replay, scopes, obligations, binding evolution, and effect uncertainty on
one small semantic kernel.

General async execution, timers, channels, process supervision, encoding,
protocol clients, file coordination, and history rewriting are not Cymule
inventions. Runtime/profile crates adopt maintained libraries such as Tokio,
Serde, protocol SDKs, and `git-filter-repo` behind the semantic interfaces.
