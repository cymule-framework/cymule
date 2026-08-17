# Live Evolution Guidance

- Plans are immutable content-addressed nodes. Evolution adds DAG edges and
  decisions; it never edits or aliases an existing Plan.
- Reusable definitions and subflows are semantic dependencies, not mutable
  pointers. Sealing resolves them into an immutable Plan dependency graph; an
  update creates new child and dependent Plan commits for future calls.
- Never implement a dynamic `latest` reference inside a sealed Plan. Existing
  invocations remain pinned; yielded work changes Plan only through safe-point
  migration, and cross-version calls require checked contract adapters.
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
- Durable evolution records use the generic M1 journal with explicit checkpoint
  lineage. Receipt loss must reopen to the committed occurrence pin or control
  decision without creating a second edge or selection.
