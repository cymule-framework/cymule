# M4 Live Evolution Profile

Status: partial.

## Implemented foundation

- immutable sealed Plan nodes and content-addressed parent/child patch edges;
- deterministic structural Plan diff over IR version, entry, component,
  effect, and definition contracts, lowered into reviewed patch operations;
- `cymule.ir/2` reusable local definition invocation with explicit input/result
  binding in Embedded and durable runtimes plus four SDK authoring surfaces;
- provider-neutral `DefinitionRegistry` with default `LatestCompatible`
  resolution, exact-schema compatibility, reverse dependency indexing, direct
  dependent relinking into new immutable parent Plans, pinned references, and
  retained historical links;
- declared patch operations with review/compiler evidence artifacts;
- cycle rejection and portable Plan DAG snapshots;
- conservative impact cones over changed stable targets, active Continuation
  frames, state schemas, and already released effects;
- future-only rollout decisions for shadow, deterministic canary, active, and
  rolled-back modes;
- immutable Plan assignment per admitted occurrence across later decisions;
- M1-backed evolution checkpoints with explicit parent lineage and idempotent
  replay for Plan edges, rollout decisions, mixed-version occurrence pins,
  migrations, and shadow evidence;
- safe-point-only state migration receipts with input/output/evidence artifacts;
- idempotent shadow comparison evidence;
- tests for DAG cycles, deterministic diff, active impact, deterministic pins,
  rollback without history rewrite, safe-point migration, snapshot restore,
  stale CAS rollback, and lost-checkpoint receipt reopen.

## Remaining completion gates

- multi-definition reusable modules, transitive dependent relinking, and impact
  propagation beyond direct registry dependents;
- patch application/lowering from reviewed operations into a new sealed Plan;
- impact over nested wait/scope/tool/model state and virtual regions;
- migration adapter registry with schema compatibility and transformed state;
- shadow execution driver and comparison policies;
- rollout observation gates, canary promotion, automatic rollback decisions,
  and mixed-version runtime dispatch;
- cross-language evolution control clients;
- restart/crash tests during migration, promotion, and rollback beyond the
  implemented occurrence-pin receipt-loss boundary.

Rollback is a new future-selection decision. It never removes a child Plan,
migration receipt, shadow result, released effect, or historical occurrence pin.
