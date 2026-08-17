# M4 Live Evolution Profile

Status: partial.

## Implemented foundation

- immutable sealed Plan nodes and content-addressed parent/child patch edges;
- declared patch operations with review/compiler evidence artifacts;
- cycle rejection and portable Plan DAG snapshots;
- conservative impact cones over changed stable targets, active Continuation
  frames, state schemas, and already released effects;
- future-only rollout decisions for shadow, deterministic canary, active, and
  rolled-back modes;
- immutable Plan assignment per admitted occurrence across later decisions;
- safe-point-only state migration receipts with input/output/evidence artifacts;
- idempotent shadow comparison evidence;
- tests for DAG cycles, active impact, deterministic pins, rollback without
  history rewrite, safe-point migration, and snapshot restore.

## Remaining completion gates

- automatic structural Plan diff and patch application/lowering;
- impact over nested wait/scope/tool/model state and virtual regions;
- migration adapter registry with schema compatibility and transformed state;
- shadow execution driver and comparison policies;
- rollout observation gates, canary promotion, automatic rollback decisions,
  and mixed-version runtime dispatch;
- durable persistence through M1 and cross-language evolution control clients;
- restart/crash tests during migration, rollout, and rollback.

Rollback is a new future-selection decision. It never removes a child Plan,
migration receipt, shadow result, released effect, or historical occurrence pin.
