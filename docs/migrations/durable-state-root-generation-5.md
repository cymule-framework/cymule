# Durable StateRoot Generation 5

Status: source implemented; operator execution pending.

Owner: each embedding that owns an internal, disposable Cymule directory `/5`
or SQLite `/6` domain with no retained compatibility or replay promise.

## Scope and terminal boundary

`cymule.durable-state-root/5` and `cymule.durable-state-value/5` add the
independent `agent_target_claims` family. The family is the sole authority for
exclusive `(Session, target kind, local identity)` ownership across ordinary
Agent Message/Tool writes and Agent stream publication. Agent commands and
receipts are `cymule.agent-command/4` and `cymule.agent-command-receipt/4`;
external publication intent and reservation records are `/2`; the Agent schema
is `cymule.agent/8`.

Generation `/4` cannot represent that claim family or prove that a provider
publication reservation won the same CAS as every competing target writer.
Current code therefore rejects `/4` manifests and values. There is no runtime
importer, mixed reader, inferred claim, dual writer, or decode-failure fallback.
This reset runbook is valid only when every persisted family may be retired as
internal test state. A domain that must preserve any historical query, receipt,
replay, Resource, Evolution, Virtual, journal, or business outcome MUST stop and
wait for an explicit migrator or an owner-routed historical reader.

## Preflight

For every retained domain, record without secrets:

- environment and domain owner;
- exact directory or SQLite store identity;
- current process/release identity;
- observed StateRoot manifest and value generations;
- an inventory and retention decision for every nonempty StateRoot family,
  including Run/Machine history, command receipts and cold archives, closed and
  active Agent Sessions, Resource pins/catalog/handoffs/deletions, Evolution,
  Virtual work, application journals, and coupled receipts;
- active Run, open Agent stream, unresolved publication, unknown Effect, and
  pending reconciliation counts;
- every public or internal historical query and exact-replay promise; and
- the exact immutable upstream queue frontier for items independently proven
  never admitted and still required after the cut.

The domain must be removed from new admission before inspection. Existing
workers may drain only work they already own under the old generation.

## Stop conditions

Stop without deleting or replacing the old store when any of these is true:

- new admission still reaches the old domain;
- an Agent stream is open, reserved, Unknown, or awaiting reconciliation;
- a Run or external Effect is not terminally settled;
- any persisted family, terminal outcome, receipt, closed Session, Resource,
  Evolution/Virtual state, journal, or history must remain queryable;
- any compatibility or replay promise exists;
- the never-admitted source queue frontier is incomplete or cannot be read back;
- another writer still holds the domain; or
- the replacement store cannot be created under one exclusive owner.

## Procedure

1. Fence new admission to the old domain and record the routing readback.
2. Settle or explicitly abandon every accepted Run, Agent Session,
   publication, and reconciliation under the internal-test owner. Record their
   terminal receipts as retired evidence; never enqueue their inputs again.
3. Export only upstream items independently proven never admitted and still
   required, with exact absence correlation against the old domain. Do not copy
   `/4` StateRoot objects, infer admission from missing UI output, or synthesize
   target claims from projections.
4. Preserve the complete old store as a read-only rollback artifact with its
   exact generation, digest, and owner record.
5. Create a new empty directory `/5` or SQLite `/6` store with a generation `/5`
   StateRoot. Never reuse the old physical location in place.
6. Start exactly one current-generation writer and verify its empty manifest,
   value objects, and `agent_target_claims` root.
7. Requeue only the step-3 never-admitted items under new
   Run/Session/command identities through current admission. Do not requeue an
   accepted terminal input or import old receipts, revisions, reservations, or
   claim generations.
8. Move admission authority to the new store and read back the exact route.

## Rollback

Before step 7, rollback means restoring routing to the preserved old runtime and
old store as one unchanged generation. After any work is admitted to the new
store, the two generations must not merge. Fence and drain the new domain, then
classify its outcomes and requeue only independently proven never-admitted items
into exactly one selected generation. Never replay a terminal input or copy
mutable StateRoot or Agent claim records between generations.

## Verification

Completion requires all of the following:

- old admission remains fenced and the preserved old bytes are unchanged;
- the new head resolves only `/5` manifest and value objects;
- full audit and reachable-object traversal pass;
- every Message and terminal Tool has one `Materialized` claim;
- every non-terminal Tool has no `Materialized` claim; `Reserved` is legal only
  with its exact open external stream and Resource reservation sidecars;
- every external publication reservation has one exact `Reserved` claim,
  Reserved Resource pin, and active retention family before provider I/O;
- `NotApplied` Abort releases the same claim and Resource pin in one CAS;
- exact command replay performs no provider call or second write; and
- requeued items were all proved never admitted, and retired terminal outcomes
  match the execution record without a second dispatch.

## Execution record

Append one non-secret record per domain containing owner, start/end UTC,
old-store identity and generation, preserved-store digest, new-store identity,
new genesis manifest, source frontier, requeued counts, verification result,
and rollback disposition. Source implementation alone is not an execution
record.
