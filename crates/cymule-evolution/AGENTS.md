# Live Evolution Guidance

- Plans are immutable content-addressed nodes. Evolution adds DAG edges and
  decisions; it never edits or aliases an existing Plan.
- Reusable definitions and subflows are semantic dependencies, not mutable
  pointers. Sealing resolves them into an immutable Plan dependency graph; an
  update creates new child and dependent Plan commits for future calls.
- Never implement a dynamic `latest` reference inside a sealed Plan. Existing
  invocations remain pinned; yielded work changes Plan only through safe-point
  migration, and cross-version calls require checked contract adapters.
- `DefinitionRegistry` defaults to `LatestCompatible`, resolves by monotonic
  publication order, materializes a complete acyclic reusable-module closure,
  and retains every historical linked Plan. A compatible leaf update relinks
  all transitive future callers. Exact schema equality is the current
  compatibility profile; adapters remain explicit future work.
- Registry snapshots are portable authority. Restore must recompute revision
  identities, validate sequences and exact links, rebuild reverse indexes, and
  reject tampering or extraneous resolution claims. Durable publication uses
  the generic M1 journal and rolls back local state on stale CAS.
- Keep semantic Plan changes separate from Binding Context changes. Rollout and
  rollback affect future selection only; admitted occurrences remain pinned.
- State migration is legal only at an explicit semantic safe point and must
  record source/target schema, input/output artifacts, and evidence.
- Impact analysis must include active frames, stable sites, waits, scopes, and
  released effects. Missing evidence fails closed.
- Shadow output is evidence, not user-visible authority. Canary selection is
  deterministic from stable identities and never ambient randomness.
- Migration and shadow implementations are pinned plugins. Validate their
  safety descriptors before invocation; retries after a committed checkpoint
  must return retained evidence without calling the plugin again.
- Rollout observations must match an immutable occurrence Plan pin. Gates count
  exact retained identities and create a new future-only decision; never mutate
  an existing decision or reinterpret an admitted occurrence.
- Tests must prove mixed-version execution, deterministic canaries, safe-point
  migration, rollback without history rewrite, and DAG cycle rejection.
- Durable evolution records use the generic M1 journal with explicit checkpoint
  lineage. Receipt loss must reopen to the committed occurrence pin or control
  decision without creating a second edge or selection.
