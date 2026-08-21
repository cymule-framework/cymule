# M4 Live Evolution Profile

Status: implemented for the provider-neutral single-domain profile.

## Implemented profile

- immutable sealed Plan nodes and content-addressed parent/child patch edges;
- deterministic structural Plan diff over IR version, entry, component,
  effect, and definition contracts, lowered into reviewed patch operations;
- exact reviewed patch admission that seals the complete target candidate and
  rejects any declared operation list that differs from the deterministic diff,
  including direct edge admission and restored snapshots;
- `cymule.ir/2` reusable local definition invocation with explicit input/result
  binding in Embedded and durable runtimes plus four SDK authoring surfaces;
- provider-neutral `DefinitionRegistry` with default `LatestCompatible`
  resolution, exact-schema compatibility, reusable modules, acyclic transitive
  dependency resolution and relinking into new immutable parent Plans, pinned
  references, and retained historical links;
- template-plus-Plan historical identities, allowing multiple parent templates
  to seal to the same Plan without overwriting one another's link history;
- strict reachable-surface admission that retains the previous future head when
  a candidate adds a component, effect, wait, capability/authority requirement,
  or changes an already reachable contract;
- portable registry snapshots that verify revision identities, sequences,
  exact current/history links, and rebuild derived dependency indexes;
- M1-journal-backed registry publication and template linking with explicit
  checkpoint lineage, stale-CAS rollback, and lost-receipt reopen;
- one complete `cymule.live-evolution/2` authority that snapshots the registry
  together with every template-scoped Plan DAG, rollout decision, evidence,
  and occurrence pin; compatible transitive relinks and their future decisions
  enter one `cymule.live-evolution-checkpoint/2` journal CAS;
- exact publication command/receipt replay after acknowledgement loss, including
  the original set of advanced and blocked parent templates;
- declared patch operations with review/compiler evidence artifacts;
- cycle rejection and portable Plan DAG snapshots;
- conservative impact cones over changed stable targets, active Continuation
  definition/invocation frames, waits, scopes, obligations, state schemas,
  already released effects, and caller-supplied higher-profile semantic sites;
- future-only rollout decisions for shadow, deterministic canary, active, and
  rolled-back modes;
- separate immutable Plan and exact ExecutionBinding Artifact assignment per
  admitted occurrence across later decisions; a Plan ID is never an occurrence binding;
- M1-backed evolution checkpoints with explicit parent lineage and idempotent
  replay for Plan edges, rollout decisions, mixed-version occurrence pins,
  migrations, and shadow evidence;
- safe-point-only state migration receipts with source/target ExecutionBinding,
  input/output/evidence Artifacts, and source/target Attempt epochs;
- one owning migration CAS that revalidates the safe point and target
  compatibility, changes the Machine Plan/binding, replaces Continuation
  Plan/state/binding, advances the epoch, closes old Attempt authority, creates
  the new Attempt, and appends the retained receipt atomically;
- migration output and shadow evidence bytes are content-verified and committed
  to the M1 Machine in the same CAS as their unified evolution checkpoint;
- pinned migration-adapter contracts that require total reachable-state
  coverage, failure/cancellation and budget/ownership preservation, and no
  authority/effect widening before plugin invocation;
- mapped Continuations whose complete frame stack proves each child's exact
  parent scope or invoke step before durable replacement and resume;
- content-addressed migration safe-point proofs derived from and revalidated
  against ready root-scoped durable Continuations without waits, obligations,
  or authority leases;
- first-class `restart_under_new_plan` authorization for a distinct replacement
  Run and exact target Plan, without implicit old-state reinterpretation;
- pinned shadow-driver contracts that require target mutation suppression and
  immutable occurrence bindings, with idempotent comparison evidence;
- immutable rollout observations tied to occurrence Plan pins, deterministic
  evidence gates, and auditable future-only promotion/rollback transitions;
- exact selected-Plan return for mixed-version runtime dispatch;
- atomic live-version selection plus virtual-work capacity-slot claim, so the
  immutable Plan pin and fenced worker occurrence enter one lease CAS and a
  lost acknowledgement replays only when both journal records are retained;
- frozen `cymule.evolution-control/4` command union, schema, Rust verifier, and
  TypeScript, Python, Rust, and Go transport interfaces/builders;
- frozen `cymule.live-evolution-control/3` unified command union and shared
  four-language fixture for definition publication, template registration,
  atomic relinking, and template-scoped operations with required safe-point
  proofs;
- tests for DAG and reusable-module cycles, deterministic diff, active impact,
  transitive executable relinking, deterministic pins, rollback without
  history rewrite, safe-point migration, tamper-resistant snapshot restore,
  stale CAS rollback, and lost-checkpoint receipt reopen.

The profile's crash suite covers real process death on both sides of a unified
publication CAS, plus stale writers and lost acknowledgements for registry
relinking, mixed-version pins, migration output, shadow evidence, and
promotion. Retrying after reopen returns the committed record without invoking
the migration or shadow plugin again.

Multi-parent exact history and unified authority advance the registry snapshot
to `cymule.definition-registry/3` and its standalone durable checkpoint to
`cymule.definition-registry-checkpoint/2`. Complete applications use
`cymule.live-evolution/2`; the standalone registry/evolution controllers are
lower-level reducers and do not independently constitute this profile.

## Deliberately external

Concrete metrics backends, traffic routers, state stores, code deployment,
schema-specific transformation logic, shadow sandboxes, and Agent/Session
controllers are plugins or operator policy. Distributed ownership and
cross-domain federation remain M5 work, not unfinished M4 semantics.

Rollback is a new future-selection decision. It never removes a child Plan,
migration receipt, shadow result, released effect, or historical occurrence pin.
After rollback, a later candidate inherits the last authoritative Plan as its
fallback; the failed target remains historical evidence and cannot re-enter the
fallback chain merely because it is the registry's latest linked revision.
