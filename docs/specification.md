# Cymule Semantic Specification

Status: implemented for the explicitly bounded `cymule.semantic/1` Embedded M0
subset; terminal durable-runtime requirements are marked proposed below.

## 1. Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative terms. A realization may
claim only the profiles whose complete required conformance cases pass.

## 2. Public model

The default public model is:

```text
Flow -> Run -> Result

Inside a Flow:  call | wait | effect | scope
Outside a Run:  observe | decide | change
```

A long-lived Run may emit typed outputs and observations without reaching a
terminal Result. The Run is the default live handle. Graphs are views.

## 3. Version domains

The following domains evolve independently:

| Domain | Current version | Compatibility rule |
| --- | --- | --- |
| Semantic specification | `cymule.semantic/1` | transition meaning is frozen |
| Canonical IR | `cymule.ir/1` | unknown operations are rejected |
| Canonical encoding | `cymule.jcs/1` | RFC 8785 JSON, SHA-256 IDs |
| Event schema | `cymule.event/1` | readers reject unknown semantic events |
| Command protocol | `cymule.command/1` | typed envelope and stable error codes |
| Plugin protocol | `cymule.plugin/1` | capability negotiation is explicit |
| Conformance profile | `cymule.conformance/1` | complete profile cases are required |

Changing effect identity, scope isolation, authority, causal admission, replay,
or migration meaning requires a new semantic specification version.

## 4. Canonical stores

Cymule has exactly three canonical stores:

1. **Plan store**: immutable content-addressed semantic plans.
2. **Event store**: admitted causal transitions with explicit parents.
3. **Artifact store**: immutable typed bytes, receipts, state, context, evidence,
   checkpoints, and occurrence bindings.

Run state, ready work, graphs, attention, effect summaries, and indexes are
deterministic projections and MUST be rebuildable.

## 5. Canonical identity

Canonical objects are encoded with RFC 8785 JSON Canonicalization Scheme and
identified as `sha256:<lowercase hex>`. The object identifier is not part of the
hashed preimage. Duplicate JSON property names and non-finite numbers are
invalid. A writer MUST validate the semantic schema before hashing.

`PlanId` identifies a sealed plan. `EventId` identifies the event payload and
causal parents. `ArtifactId` identifies artifact type metadata and bytes.

### 5.1 Cross-Run resource values (proposed)

M1 will extend Artifact exchange with a versioned provider-neutral resource
descriptor. A resource has a logical shape (`inline`, `object`, `collection`,
`directory`, or `snapshot`), media type, optional byte size, and replay
evidence. Inline text/JSON/bytes are content-addressed directly. External
objects, directory manifests, sandbox snapshots, remote-drive items, and URL
content MUST carry either a content digest or an immutable provider version to
qualify for exact replay.

Resolver locators and access grants are realization data, not Plan semantics.
Credentials MUST NOT enter a descriptor, Artifact, Event, Continuation, or IR.
A mutable locator without immutable evidence MAY be passed between Runs as a
live reference, but replay availability MUST be downgraded until a resolver
materializes and verifies stable content. Collections and directories use
immutable manifests of child descriptors rather than assuming one provider's
listing or path semantics.

## 6. Frozen IR

`cymule.ir/1` contains:

- named component and effect contracts;
- structured definitions composed from `call`, `wait`, `effect`, and `scope`;
- literal, input, binding, object, and array expressions;
- explicit input/output JSON Schemas;
- provider-neutral effect and execution properties.

The IR MUST NOT contain provider endpoints, credentials, queue names, database
products, worker addresses, or deployment topology. Frontends are proposal
producers. The trusted Rust sealer validates and computes the Plan ID.

## 7. Versioned effectful continuation

A terminal durable-profile continuation exists only at a semantic safe point
and contains:

```text
plan ID | future binding context | frame | typed state | wait set | scope
effect obligations | authority leases | budget | causal cut | epoch
```

Process memory and host-language stacks are not canonical. An Attempt pins an
immutable occurrence binding and the continuation epoch. Output from a stale
attempt MUST be rejected.

The M0 kernel implements Run, Attempt, epoch, scope, effect obligation, and
binding projections. M1 now defines and persists the complete first-class
Continuation field set through a provider-neutral CAS store. Automatic capture
and resumable interpretation at every safe point remain partial and are not
claimed by the Embedded profile.

M2 durable input suspension is one M1 safe-point transition: the pending
elicitation projection, Session `RequiresAction` state, typed input wait, and
Continuation `Waiting` state MUST enter the same CAS revision. Completion MUST
atomically store the wait result and resolved elicitation; the Session remains
`RequiresAction` while any elicitation is unresolved and returns to `Running`
only when the final input wait completes.

An M2 elicitation schema MUST be a self-contained JSON Schema Draft 2020-12
document that compiles without external resource retrieval. The schema MUST be
accepted before suspension. An accepted response MUST carry a value that
satisfies the persisted request schema before any completion record is written;
a declined response MUST NOT carry a value. Schema compilation or value
validation failure leaves the wait, Session projection, Continuation, and CAS
revision unchanged. Draft 2020-12 `format` remains an annotation rather than an
additional validation assertion.

## 8. Causal events

A causal cut is a causally closed down-set of admitted events. Implementations
may represent it with maximal frontier IDs. An event carries logical read/write
footprints and an optional coordination key. Independent events MUST commute.

Non-monotone decisions, including consume-once delivery, scope decisions,
effect release, budget reservation, authority widening, and default version
advancement, MUST be coordinated within a declared semantic domain.

## 9. Commands

Canonical mutation enters through a typed command envelope:

```text
command ID | actor | target | expected precondition | semantic payload
```

The same command ID and semantic payload MUST return the original receipt. The
same ID with different semantics MUST fail. A stale precondition MUST return a
typed conflict and current precondition token. Public callers cannot append raw
events.

## 10. Scopes and obligations

Embedded M0 scope state transitions are:

```text
open -> closed-committed
open -> closed-aborted
```

Commit atomically accepts declared state/evidence and transfers outstanding
world-effect requirements into an `EffectObligationSet`. Scope closure does not
claim that a provider action is applied. Run completion policy decides whether
unresolved obligations block the Result.

An M2 workspace commit MUST reference an immutable overlay Artifact and a
Plan-declared Effect whose profile is mutating, `on_scope_commit`, queryable,
and keyed-idempotent. The binding-pinned workspace occurrence, committed scope,
transferred obligation, pending outbox entry, Machine snapshot, and
Continuation MUST enter the same M1 CAS authority before dispatch. The outbox
claim, `DispatchStarted` transition, and typed `started` occurrence MUST also be
atomic. Provider settlement MUST atomically update the Effect outcome,
obligation, outbox, retained occurrence, Machine, and Continuation.

An M2 workspace abort MUST leave the scope open until the original binding
returns or reconciliation recovers a typed receipt proving the overlay was not
committed. An ambiguous or explicitly not-applied abort MUST NOT close the
scope. A replacement cleanup attempt requires a new occurrence identity.

## 11. Effects

Effect identity is structural:

```text
run | invocation | stable site | scope epoch | occurrence key
normalized arguments | effect schema version
```

Phase, world outcome, and reconciliation are orthogonal:

```text
phase: admitted -> prepared -> release-authorized -> dispatch-started
outcome: unobserved | applied | not-applied | unknown
reconciliation: not-required | pending | resolved | governance-required
```

After dispatch ambiguity, the original intent becomes `unknown`. It MUST keep
its original occurrence binding and reconciler. It MUST NOT become a fresh
intent. An `unknown` outbox entry remains eligible for reconciliation under its
original claim across any number of process reopens. `still_unknown` MUST NOT
redispatch it, and a later applied or not-applied observation MUST be admissible
without changing identity. Compensation is a separately admitted effect.

## 12. Binding evolution

A Plan changes semantic meaning. A Binding Context changes realization defaults
for future occurrences. Every persisted occurrence must pin an immutable
binding at admission. Embedded M0 persists this for Attempts and Effect Intents,
including reconciliation. M1 defines canonical component occurrence records.
M2 records context, model, permission, tool, elicitation, and workspace calls as
request-digested, binding-pinned occurrences.

The caller or adapter owns its Agent/script loop, including ordering, strategy,
program counter, and continuation decisions. M2 MUST NOT interpret a fixed
model/tool loop or persist loop phases as framework semantics. For an individual
interaction, the caller supplies a stable occurrence identity and typed request.
Repeating a completed occurrence with the same request MUST return its retained
typed response without binding or dispatching again. Reusing the identity with
a different request MUST fail. A `prepared`, `started`, `unknown`, or
`not_applied` occurrence MUST require explicit recovery, a separately admitted
replacement identity, or caller-owned termination before execution can proceed.

M2 host reconciliation MUST query the occurrence's pinned binding and MUST NOT
redispatch its request. It may settle the occurrence as `completed` with a
matching typed response or `not-applied` with explicit evidence. A `prepared`
occurrence may enter `not-applied` only when dispatch is proven not to have
started. A still-unknown result continues to block that occurrence. After
reconciliation retains a matching typed response, repeating the same occurrence
MUST return that response without redispatch.

Changing a default MUST NOT rewrite an admitted occurrence. If its original
binding is unavailable, the occurrence enters an explicit unavailable or
governance path.

## 13. Replay

- **Exact state replay** reduces recorded events and artifacts without external
  I/O.
- **Exact execution replay** additionally requires every nondeterministic call
  result and occurrence binding to have been recorded.
- **Resume** replays the prefix, then admits new work beyond the frontier.
- **Fork** creates a new lineage from a selected cut.
- **Regeneration** performs new nondeterministic work and is not replay.

Replay availability is `exact`, `projection-only`, or `unavailable` relative to
an explicit required-artifact set. A missing artifact, schema, interpreter,
binding, or required authority MUST downgrade the claim. The runtime MUST NOT
silently regenerate missing data. M0 verifies exact canonical state replay; its
one-shot component calls are not an exact execution-replay implementation.

## 14. Implemented profile boundary

The Embedded profile implements canonicalization, sealing, in-memory stores,
causal replay, command idempotency and stale-action rejection, attempt fencing,
scope/obligation semantics, effect lifecycle, process plugins, and all four SDK
chains. `wait` is an authoring and suspension boundary but has no durable resume
loop in M0. The profile does not claim complete VEC persistence, exact replay of
unrecorded component outputs, persistent crash recovery, multi-process
consensus, tenant isolation, distributed scheduling, or provider-level
exactly-once.
