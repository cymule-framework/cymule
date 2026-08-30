# Durable StateRoot Generation 5

Status: source implemented; operator execution pending.

Owner: each embedding that owns a retained Cymule directory `/5` or SQLite
`/6` domain.

## Scope and terminal boundary

`cymule.durable-state-root/5` and `cymule.durable-state-value/5` add the
independent `agent_target_claims` family. The family is the sole authority for
exclusive `(Session, target kind, local identity)` ownership across ordinary
Agent Message/Tool writes and Agent stream publication. Agent receipts are
`cymule.agent-command-receipt/4`; external publication intent and reservation
records are `/2`; the Agent schema is `cymule.agent/8`.

Generation `/4` cannot represent that claim family or prove that a provider
publication reservation won the same CAS as every competing target writer.
Current code therefore rejects `/4` manifests and values. There is no runtime
importer, mixed reader, inferred claim, dual writer, or decode-failure fallback.

## Preflight

For every retained domain, record without secrets:

- environment and domain owner;
- exact directory or SQLite store identity;
- current process/release identity;
- observed StateRoot manifest and value generations;
- active Run, Agent Session, open Agent stream, unresolved publication,
  unknown Effect, and pending reconciliation counts;
- the source-system identifiers required to recreate accepted work; and
- the exact immutable export or upstream queue frontier used for requeue.

The domain must be removed from new admission before inspection. Existing
workers may drain only work they already own under the old generation.

## Stop conditions

Stop without deleting or replacing the old store when any of these is true:

- new admission still reaches the old domain;
- an Agent stream is open, reserved, Unknown, or awaiting reconciliation;
- a Run or external Effect is not terminally settled;
- the source queue/export frontier is incomplete or cannot be read back;
- another writer still holds the domain; or
- the replacement store cannot be created under one exclusive owner.

## Procedure

1. Fence new admission to the old domain and record the routing readback.
2. Drain every accepted Run, Agent Session, publication, and reconciliation to
   a durable terminal state.
3. Export only immutable business inputs and upstream queue identities needed
   to recreate future work. Do not copy `/4` StateRoot objects or synthesize
   target claims from their projections.
4. Preserve the complete old store as a read-only rollback artifact with its
   exact generation, digest, and owner record.
5. Create a new empty directory `/5` or SQLite `/6` store with a generation `/5`
   StateRoot. Never reuse the old physical location in place.
6. Start exactly one current-generation writer and verify its empty manifest,
   value objects, and `agent_target_claims` root.
7. Requeue the recorded immutable business inputs under new Run/Session/command
   identities through current admission. Do not import old receipts, revisions,
   reservations, or claim generations.
8. Move admission authority to the new store and read back the exact route.

## Rollback

Before step 7, rollback means restoring routing to the preserved old runtime and
old store as one unchanged generation. After any work is admitted to the new
store, the two generations must not merge. Fence and drain the new domain, then
requeue its immutable inputs into exactly one selected generation. Never copy
mutable StateRoot or Agent claim records between them.

## Verification

Completion requires all of the following:

- old admission remains fenced and the preserved old bytes are unchanged;
- the new head resolves only `/5` manifest and value objects;
- full audit and reachable-object traversal pass;
- every Message and terminal Tool has one `Materialized` claim;
- every non-terminal Tool has no `Reserved` or `Materialized` claim;
- every external publication reservation has one exact `Reserved` claim,
  Reserved Resource pin, and active retention family before provider I/O;
- `NotApplied` Abort releases the same claim and Resource pin in one CAS;
- exact command replay performs no provider call or second write; and
- requeued source inputs and terminal outcomes match the execution record.

## Execution record

Append one non-secret record per domain containing owner, start/end UTC,
old-store identity and generation, preserved-store digest, new-store identity,
new genesis manifest, source frontier, requeued counts, verification result,
and rollback disposition. Source implementation alone is not an execution
record.
