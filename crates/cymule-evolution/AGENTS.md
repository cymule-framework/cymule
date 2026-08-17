# Live Evolution Guidance

- Plans are immutable content-addressed nodes. Evolution adds DAG edges and
  decisions; it never edits or aliases an existing Plan.
- Keep semantic Plan changes separate from Binding Context changes. Rollout and
  rollback affect future selection only; admitted occurrences remain pinned.
- State migration is legal only at an explicit semantic safe point and must
  record source/target schema, input/output artifacts, and evidence.
- Impact analysis must include active frames, stable sites, waits, scopes, and
  released effects. Missing evidence fails closed.
- Shadow output is evidence, not user-visible authority. Canary selection is
  deterministic from stable identities and never ambient randomness.
- Tests must prove mixed-version execution, deterministic canaries, safe-point
  migration, rollback without history rewrite, and DAG cycle rejection.
