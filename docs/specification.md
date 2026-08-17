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
| Resource descriptor | `cymule.resource/1` | identity excludes realization locations |
| Resource handoff | `cymule.resource-handoff/1` | transfer IDs are idempotent per target Run |
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

### 5.1 Cross-Run resource values

M1 Artifact exchange includes a versioned provider-neutral Resource descriptor.
A Resource has a logical shape (`inline`, `object`, `collection`, `directory`,
or `snapshot`), media type, replay evidence, semantic annotations, and optional
realization locations. Inline text/JSON/bytes and external content with verified
SHA-256/size provide location-independent exact evidence; availability still
requires retained inline bytes or a usable location. An immutable provider version
requires the original resolver binding for exact retrieval. A `live` Resource
is useful state but is never exact replay evidence.

Resolver locators and access grants are realization data, not Plan semantics.
Credentials MUST NOT enter a descriptor, Artifact, Event, Continuation, or IR.
A public URL MUST be credential-free HTTP(S) without userinfo, query, or
fragment. Private object, drive, sandbox, and signed-URL access uses an opaque
resolver binding/reference whose reference is non-secret. Locations do not
participate in Resource ID; moving identical content MUST preserve identity.

Read operations MUST be bounded chunks and directory/collection/snapshot lists
MUST be bounded opaque-cursor pages. A resolver response that exceeds the
requested bound, repeats an unsafe entry, stalls with an empty non-terminal
chunk, or fails content verification MUST be rejected. Chunked stores use
idempotent write IDs, exact offsets, explicit commit, and explicit abort.

A Run-to-Run handoff carries one verified Resource Handle under a stable caller
transfer ID and target slot. The handoff MUST be recorded in the target Run's
M1 application journal by whole-state CAS. Repeating identical semantics is
idempotent; reusing a transfer ID with different semantics MUST fail. One target
slot has at most one handoff; multiple values use one collection Resource.

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

An integration plugin MAY atomically checkpoint its own typed projection with
a Continuation, wait, outbox entry, or effect transition through the M1
application-journal boundary. The plugin owns its domain schema and transition
rules. Framework conformance requires only that the shared CAS write is
all-or-nothing, stale writers fail closed, and plugin records cannot widen
authority or bypass Plan-declared effects.

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

A plugin-mediated resource or workspace mutation MUST still enter through a
Plan-declared Effect. A domain controller cannot treat an external commit as an
internal state update, close a scope around an ambiguous result, or bypass the
effect obligation and reconciliation rules below. Domain-specific overlays,
sessions, receipts, and controllers are outside this specification.

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
Optional plugins MAY define additional domain occurrences, but they MUST
preserve the same immutable binding rule whenever replay or reconciliation
depends on a selected implementation. Session, stream, and domain-controller
semantics remain in the owning plugin rather than this framework specification.

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
