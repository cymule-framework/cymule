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
  semantic profiles rather than an ambient runtime context.
- [Agent Client Protocol](https://agentclientprotocol.com/protocol/overview)
  separates accepted prompts from ordered session updates, typed content, tool
  status, permission requests, elicitation, plans, usage, and terminal state.
  Cymule M2 maps these to durable occurrences and projections instead of making
  a transport session canonical.
- [Model Context Protocol](https://modelcontextprotocol.io/specification/) keeps
  resources, prompts, tools, user input, and asynchronous Tasks as capability
  surfaces. Cymule treats external protocol objects as adapter inputs and pins
  the selected context/tool occurrence before execution.
- [A2A](https://a2a-protocol.org/dev/specification/) distinguishes Messages,
  Tasks, status updates, and Artifacts. Cymule uses the same communication versus
  durable-output distinction without adopting A2A transport bindings in core.

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

## Deliberate differences

Cymule does not re-execute arbitrary host-language orchestration as canonical
replay, does not make a database transaction the universal world-effect model,
does not treat a plugin catalog as authority, and does not claim external
exactly-once when a provider cannot prove it. Its contribution is the closure of
causal replay, scopes, obligations, binding evolution, and effect uncertainty on
one small semantic kernel.
