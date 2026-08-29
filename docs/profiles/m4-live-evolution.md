# M4 Live Evolution Profile

Implementation status: implemented in source for the provider-neutral,
single-domain profile.

Conformance status: focused reducer and process-wire suites are required for
every change. A complete profile claim additionally requires the final frozen
tree's Durable, Engine, schema, SDK, documentation, and branch-wide gates; an
earlier partial run is not current evidence.

## Terminal authority

`cymule-profile-protocol::evolution` is the only portable authority for M4
commands, identities, normalized leaves, exact queries, semantic receipts,
bounded source views, provider products, and pure reduction.
`cymule-evolution` re-exports that contract and owns the closed process-provider
wire. It does not define a second reducer or a persistence controller.

The sole durable mutation path is a provider-registry-bound
`DurableEvolutionControl` obtained from `DurableStoreControl`. Its public
surface is:

- exact current reads with `EvolutionCurrentQuery`;
- exact command-receipt reads with `EvolutionReceiptQuery`; and
- `commit(EvolutionPersistenceCommand) -> EvolutionCommit`.

Production M4 code exposes no raw `DurableTransaction`, generic delta,
untyped history append, StateRoot mutation, caller-authored source view, or
caller-authored postcondition. Pre-normalized persistence generations have no
reader, writer, or compatibility bridge.

## Normalized StateRoot model

One M4 partition has a scalar `cymule.evolution-current/2` plus 22 independently
keyed persistent-map families using `cymule.evolution-state-leaf/3`:

- definition current, exact-contract compatibility current, and immutable
  definition records;
- top-level reverse dependencies, template current, immutable link records,
  Plans, and Plan edges;
- rollout current, bounded evidence current, and immutable decisions;
- occurrence pins and deterministic-selection aliases;
- migration, restart, shadow, and shadow-subject records;
- observation, occurrence-observation, and cross-family evidence records; and
- completed-decision aliases and immutable transition records.

No current leaf contains a complete registry, registry history, template
history, evidence vector, or replayable snapshot. Ordinary open, retry, and
command preparation use authenticated exact-key reads from one pinned
StateRoot. Historical traversal is an explicit offline audit operation.

The profile enforces fixed bounds before provider execution and before CAS:

- 4 MiB canonical command bytes;
- 12 MiB per normalized leaf;
- 8 MiB per semantic receipt;
- 8,192 exact source or mutation leaves;
- 1,024 templates per atomic publication; and
- 64 MiB each for the accounted typed source and complete postcondition.

Membership keys, negative lookups, scalar current, and non-serializable
Durable-derived source authority are included in aggregate accounting.

## Commands, commits, and replay

`cymule.live-evolution-control/6` is the complete public command union. It owns
definition publication, template registration, atomic publish-and-relink, and
template-scoped `cymule.evolution-control/5` operations. The persistence wrapper
and receipt are `cymule.evolution-persistence-command/4` and
`cymule.evolution-persistence-receipt/4`.

External commands contain semantic intent plus explicit scalar optimistic
preconditions only. They never carry a safe point, Continuation, provider
product, read set, StateRoot, manifest, or CAS token. Durable derives the typed
source view and Run authority from one pinned root; fixed providers may produce
only non-serializable authority after deterministic preparation.

The all-ever command alias is checked immediately after strict command
validation and before current reads, Run-source derivation, provider Describe,
or provider execution. The same command ID and exact semantic command returns
the original receipt without provider I/O and with
`EvolutionCommit.committed_revision = null`. Reusing the ID with different
semantic content conflicts before I/O.

A fresh command is reduced against one pinned root. One CAS publishes the new
scalar current, command alias, semantic receipt, normalized leaves, introduced
Plans and Artifacts, required Artifact retention, and any coupled M1 or M3
state. Validation, provider, Store, CAS-conflict, or pre-CAS process failure
publishes nothing. Recovery uses exact keyed receipt lookup; it never replays
history from genesis and never compensates by restoring a prior full state.

The semantic receipt binds the complete command, exact parent current,
optional Durable source witness, closed outcome, and strictly ordered typed
write descriptors. It deliberately excludes physical result revision,
StateRoot manifest, and CAS token, avoiding a fixed-point identity cycle.

## Reusable definitions and historical relinking

Published definition revisions and linked Plans are immutable and content
addressed. A reusable-definition revision carries strictly ordered, unique,
exact-contract dependencies, and every dependency is pinned to one immutable
revision. Every parent reference carries an explicit closed strategy;
`LatestCompatible` is legal only on an unsealed `PlanTemplate` and is never an
omitted default.

Linking consumes the exact bounded transitive revision closure and rejects
missing or extraneous revisions, conflicting choices, contract mismatch,
cycles, oversized closures, and excessive depth. Exact-contract compatibility
current is the only `LatestCompatible` lookup authority; immutable history is
not scanned and an incompatible global head is never a fallback.

Publishing new content allocates the next lineage sequence. Republishing
historical content reuses the original immutable revision and moves only the
current and compatibility heads. Historical relinking reuses exact retained
link and Plan records. `cymule.plan-edge/2` identifies the ordered structural
transition independently of publication evidence: an already retained directed
edge is verified and reused, while a genuinely new reverse transition creates
one immutable edge. The first accepted `EdgeRecord` evidence cannot be replaced;
every later publication retains its own evidence Artifact in the complete
semantic command receipt.

Every generated future rollout decision binds its source decision as well as
template, fallback, target, and mode. Consequently an A1 -> A2 -> A1 -> A2
cycle creates distinct decision identities and distinct evidence accumulators;
returning to an old Plan cannot overwrite evidence from an earlier decision.

Before a dynamic parent head advances, compatibility compares the complete
entry-reachable component, Effect, wait, capability, and authority surface.
Widening retains the old future head while preserving the new immutable
definition revision for explicit use. Pinned transitive dependencies do not
advance implicitly.

## Rollout and occurrence semantics

Rollout modes affect future selections only: shadow, deterministic canary,
active, and rolled back. Each occurrence retains one immutable pin containing
its occurrence, template, decision, semantic Plan, exact
`cymule.execution-binding/2` Artifact, and selection identity. Later rollout
changes never reinterpret that pin.

Observations and shadow comparisons are exact immutable records. A bounded
authenticated accumulator retains their counts and root instead of embedding
an ever-growing evidence vector in current state. Late evidence remains
attributed to the occurrence's retained decision. Only applying a gate requires
that decision to remain current.

Promotion or rollback creates a new immutable transition and a new future
decision. A completed source decision cannot become current again. Rollback
does not delete a Plan, edge, occurrence, migration, shadow result, observation,
or released Effect. `cymule.rollout-transition/2` derives from exactly the
retained source decision, target decision, and complete verified evaluation;
the verifier recomputes it and rejects the superseded identity generation.

A Virtual Run owns exactly one execution selector: `Direct` pins a Plan and
creates no M4 mutation; `Evolution` names one partition and template. Once
fairness selects the Run, Durable derives the selection identity from the
Virtual persistence identity, loads and admits the exact ExecutionBinding
Artifact, and commits the Evolution receipt and occurrence pin in the same CAS
as the M3 claim and M1 execution authority. There is no claim-time optional M4
selector or parallel selection receipt.

## Migration, restart, and shadow providers

Migration and shadow implementations are pinned by semantic provider identity
and immutable implementation revision. A provider registry is fixed for the
lifetime of its owning Durable control, so retry cannot substitute another
implementation.

After exact alias lookup, a fresh migration admits exactly one adapter and one
target-binding entry keyed by `to_plan`; a fresh shadow admits exactly one
driver. Selection resolves its existing M1 binding, restart defers binding to
normal new-Run admission, and all other commands admit no Evolution provider
capability. Missing or extra authority fails before writable Store open.

For migration, Durable derives exact-domain quiescence, the complete source
Continuation, source binding, admitted Plans, and Artifact membership from one
root. A fresh target binding comes from the fixed exact-Plan registry after
that deterministic preparation; retained replay loads its complete target
binding from the root. The source must be a claim-free `Ready`
Continuation with frames and no pending wait, pending/claimed/unknown outbox
entry, nonterminal Effect, unresolved blocking obligation, active Attempt,
active Effect claim lease, or open nested scope.

The fixed provider registry resolves a complete target `ExecutionBinding` by
the exact Plan selected during deterministic M4 preparation; absence has no
ambient fallback. The profile verifies and admits that binding, materializes
its canonical Artifact record, and only then permits provider I/O. The binding
Artifact, Evolution postcondition, Core `MigrateRun`, and target Continuation
then share the one final CAS. A caller-authored or Ref-only assertion is invalid.
An exact retained migration-record replay emits no M1 sidecar and never applies
`MigrateRun` again; it loads the retained target binding Artifact from the same
root and does not invoke the target-binding registry or adapter.

The migration adapter must prove total reachable-state coverage, exact reviewed
edge and compatibility, no capability/effect widening, and preservation of
failure, cancellation, budget, and ownership meaning. Its output publishes a
complete target Continuation plus exact Artifact closure. The one CAS changes
the Machine Plan and binding, replaces state and Continuation, advances the
epoch, preserves the execution fence and Attempt history, and leaves a
claim-free `Ready` target for a later ordinary resume.

Migration Artifact products admit at most 1,024 records and 4 MiB of canonical
bytes. Shadow evidence is bounded to 4 MiB raw and canonical bytes. Missing,
duplicate, extraneous, forged, or oversized products are closed plugin defects,
not truncation candidates.

Restart authorizes a distinct replacement Run under one exact target Plan and
explicit input. It never mutates or resumes the source Run and never
reinterprets old state under new semantics.

## Fixed process wire

Process-hosted migration and shadow providers use exactly
`cymule.evolution-plugin/3`. Request and response each have the same fixed
16 MiB raw-message limit. The Engine target and process executor must declare
that exact limit; a smaller generic limit or a larger target limit is not a
second protocol mode.

Both directions pass through the shared duplicate-rejecting strict JSON parser,
safe-number validation, unknown-member rejection, and typed member-presence
comparison. The CLI uses the executor's dedicated raw Evolution entry and the
single `decode_evolution_plugin_response` path; it never falls back to generic
plugin decoding.

Failures preserve one closed category: `cancelled`, `timed_out`, `contract`,
`integrity`, `plugin_defect`, or `substrate`. Structured contract violations
and stable code/message fields survive end to end. Stderr and error-message
prefixes are never semantic classification authority.

## Engine surface and verification

The cross-language mutation is exactly:

```text
execute_live_evolution(target, evolution_id, command) -> EvolutionCommit
```

The Engine v4 success first echoes the complete strictly decoded request. An
SDK compares that echo with the exact JSON value it sent before validating the
commit. It then verifies `observed_revision`, the required-nullable
`committed_revision`, and the semantic receipt against the exact `evolution_id`
and command. A malformed or mismatched success after mutation begins is an
unknown world outcome requiring reconciliation, never an uncorrelated success.

Focused conformance covers:

- exact replay and same-ID semantic conflict before provider I/O;
- missing exact reads, count and byte boundaries, and wrong-root rejection;
- A1 -> A2 -> A1 -> A2 immutable-history reuse and evidence isolation;
- dependency order, size, depth, closure, and compatibility;
- occurrence and Virtual selection binding;
- migration, restart, shadow, observation, and gate product tampering;
- exact 16 MiB process-wire max/+1, duplicate keys, unsafe numbers, unknown
  fields, failure-category retention, and 4 MiB semantic product max/+1; and
- Durable CAS conflict, Store failure, crash on both CAS sides, reopen, lost
  acknowledgement, zero writes on failure, and provider non-invocation on
  replay.

## Deliberately external

Concrete metrics backends, traffic routers, deployments, state stores,
schema-specific transformation code, shadow sandboxes, and Agent/Session
controllers remain plugins or operator policy. Distributed ownership and
cross-domain federation are outside the implemented single-domain M4 profile.
