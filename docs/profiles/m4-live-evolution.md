# M4 Live Evolution Profile

Status: implemented for the provider-neutral single-domain profile.

## Implemented foundation

- immutable sealed Plan nodes and content-addressed parent/child patch edges;
- deterministic structural Plan diff over IR version, entry, component,
  effect, and definition contracts, lowered into reviewed patch operations;
- exact reviewed patch admission that seals the complete target candidate and
  rejects any declared operation list that differs from the deterministic diff;
- `cymule.ir/2` reusable local definition invocation with explicit input/result
  binding in Embedded and durable runtimes plus four SDK authoring surfaces;
- provider-neutral `DefinitionRegistry` with default `LatestCompatible`
  resolution, exact-schema compatibility, reusable modules, acyclic transitive
  dependency resolution and relinking into new immutable parent Plans, pinned
  references, and retained historical links;
- portable registry snapshots that verify revision identities, sequences,
  exact current/history links, and rebuild derived dependency indexes;
- M1-journal-backed registry publication and template linking with explicit
  checkpoint lineage, stale-CAS rollback, and lost-receipt reopen;
- declared patch operations with review/compiler evidence artifacts;
- cycle rejection and portable Plan DAG snapshots;
- conservative impact cones over changed stable targets, active Continuation
  definition/invocation frames, waits, scopes, obligations, state schemas,
  already released effects, and caller-supplied higher-profile semantic sites;
- future-only rollout decisions for shadow, deterministic canary, active, and
  rolled-back modes;
- immutable Plan assignment per admitted occurrence across later decisions;
- M1-backed evolution checkpoints with explicit parent lineage and idempotent
  replay for Plan edges, rollout decisions, mixed-version occurrence pins,
  migrations, and shadow evidence;
- safe-point-only state migration receipts with input/output/evidence artifacts;
- pinned migration-adapter contracts that require total reachable-state
  coverage, failure/cancellation and budget/ownership preservation, and no
  authority/effect widening before plugin invocation;
- pinned shadow-driver contracts that require target mutation suppression and
  immutable occurrence bindings, with idempotent comparison evidence;
- immutable rollout observations tied to occurrence Plan pins, deterministic
  evidence gates, and auditable future-only promotion/rollback transitions;
- exact selected-Plan return for mixed-version runtime dispatch;
- frozen `cymule.evolution-control/1` command union, schema, Rust verifier, and
  TypeScript, Python, Rust, and Go transport interfaces/builders;
- tests for DAG and reusable-module cycles, deterministic diff, active impact,
  transitive executable relinking, deterministic pins, rollback without
  history rewrite, safe-point migration, tamper-resistant snapshot restore,
  stale CAS rollback, and lost-checkpoint receipt reopen.

The profile's crash suite covers stale writers plus lost acknowledgements for
registry relinking, mixed-version pins, migration output, shadow evidence, and
promotion. Retrying after reopen returns the committed record without invoking
the migration or shadow plugin again.

## Deliberately external

Concrete metrics backends, traffic routers, state stores, code deployment,
schema-specific transformation logic, shadow sandboxes, and Agent/Session
controllers are plugins or operator policy. Distributed ownership and
cross-domain federation remain M5 work, not unfinished M4 semantics.

Rollback is a new future-selection decision. It never removes a child Plan,
migration receipt, shadow result, released effect, or historical occurrence pin.
