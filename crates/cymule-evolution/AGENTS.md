# Live Evolution Guidance

- Plans are immutable content-addressed nodes. Evolution adds DAG edges and
  decisions; it never edits or aliases an existing Plan.
- Reusable definitions and subflows are semantic dependencies, not mutable
  pointers. Sealing resolves them into an immutable Plan dependency graph; an
  update creates new child and dependent Plan commits for future calls.
- Patch and linker sealing use the single executable Plan admission path from
  `cymule-runtime`; malformed JSON Schema may not enter the evolution DAG or a
  future default head.
- Never implement a dynamic `latest` reference inside a sealed Plan. Existing
  invocations remain pinned; yielded work changes Plan only through safe-point
  migration, and cross-version calls require checked contract adapters.
- `DefinitionRegistry` defaults to `LatestCompatible`, resolves by monotonic
  publication order, materializes a complete acyclic reusable-module closure,
  and retains every historical linked Plan. A compatible leaf update relinks
  all transitive future callers. Exact schema equality is the current
  compatibility profile; adapters remain explicit future work.
- Automatic relinking compares the entry-reachable semantic surface. Adding a
  component, effect, wait, capability/authority requirement, or changing a
  reachable contract blocks the future-head update without deleting the new
  revision. There is no unchecked-latest escape hatch.
- Registry snapshots are portable authority. Restore must recompute revision
  identities, validate sequences and exact links, rebuild reverse indexes, and
  reject tampering or extraneous resolution claims. Durable publication uses
  the generic M1 journal and rolls back local state on stale CAS.
- `LiveEvolutionController` is the complete application-facing authority. It
  checkpoints the registry and every template-scoped DAG, rollout decision,
  and occurrence pin in one journal snapshot. Applications must not advance a
  registry head and a separate rollout controller in sequential CAS writes.
- Historical links are keyed by template-plus-Plan identity. Distinct parent
  templates may legitimately seal to the same Plan ID and must not overwrite
  one another's dependency or history records.
- Keep semantic Plan changes separate from Binding Context changes. Rollout and
  rollback affect future selection only; admitted occurrences remain pinned.
- A new candidate after rollback uses the last authoritative fallback, never
  the failed target that merely remains the registry's historical link head.
- Virtual occurrence admission stores the selected Plan and exact
  ExecutionBinding Artifact as separate immutable fields; Plan identity is
  never an implementation binding.
- State migration is legal only at an explicit semantic safe point and must
  record source/target schema, input/output artifacts, and evidence.
- Durable migration is one owning M1 CAS: revalidate the safe point and source
  binding, admit the target Plan against the target ExecutionBinding Artifact,
  commit output/evidence, replace Continuation Plan/state/binding, advance the
  epoch, close old Attempt authority, create the new Attempt, and append the
  evolution receipt together.
- A migration request pins one exact reviewed `PlanEdge` and the deterministic
  compatibility-report identity for that source/target pair. The adapter
  receives the complete authenticated source Continuation and returns the
  complete target Continuation, including every mapped frame and program
  counter. Resume uses that returned mapping; copying source frames is invalid.
- Registry and unified live-controller mutations are staged values. Publish,
  relink, template registration, controller admission, and durable checkpoint
  errors leave the previously visible snapshot byte-for-byte unchanged.
- Safe points are derived proofs over current durable Continuations, never
  caller booleans. Restart-under-new-plan authorizes a distinct replacement Run
  and explicit input; it does not mutate the source or execute a loop.
- Impact analysis must include active frames, stable sites, waits, scopes, and
  released effects. Missing evidence fails closed.
- Shadow output is evidence, not user-visible authority. Canary selection is
  deterministic from stable identities and never ambient randomness.
- Migration and shadow implementations are pinned plugins. Validate their
  safety descriptors before invocation; retries after a committed checkpoint
  must return retained evidence without calling the plugin again.
- Migration and shadow plugins return complete `ArtifactRecord` products, not
  bare references. Verify content identity and commit those bytes to the M1
  Machine in the same CAS as the evolution checkpoint; an evolution journal
  must never reference evidence that only existed in plugin memory.
- Rollout observations must match an immutable occurrence Plan pin. Gates count
  exact retained identities and create a new future-only decision; never mutate
  an existing decision or reinterpret an admitted occurrence.
- Tests must prove mixed-version execution, deterministic canaries, safe-point
  migration, rollback without history rewrite, and DAG cycle rejection.
- Executable evolution tests must construct an explicit binding whose provider
  properties satisfy the selected Plan; a manifest alone is never admission.
- Durable safe-point fixtures retain their frame and state Artifacts in the
  same Machine before publishing a Continuation; synthetic dangling hashes are
  not valid migration evidence.
- Durable evolution records use the generic M1 journal with explicit checkpoint
  lineage. Receipt loss must reopen to the committed occurrence pin or control
  decision without creating a second edge or selection.
- Process-hosted migration and shadow implementations use
  `cymule.evolution-plugin/1`; descriptor revision must equal sealed bytes.
