# Timer Activation Store Generation 2

Status: exact `/1` rejection is implemented; compatibility migration is
unsupported. No environment migration has been executed by this source change.

## Authority and decision

`cymule.activation-timer-store/2` is the sole current timer-store generation.
It adds the mandatory `schedule_digest`, derived from the complete activation
ID, timer ID, due observation, and typed value. Every schedule replay, fresh
selection, retained delivery, and acknowledgement revalidates the complete row
and digest before a delivery can reach M1.

The `/1` shape cannot authenticate those four fields as one schedule. The
current runtime therefore has no `/1` reader, importer, dual writer, or
decode-failure fallback. Copying `/1` rows into `/2` or synthesizing their
missing digest would manufacture authority and is forbidden. This hard cut is
permitted because the retained stores are internal-test state with no public
replay promise.

## Owner and preflight

The embedding operator owns the configured timer database; the workload owner
owns reconstruction of any timer that must remain pending. Before resetting a
retained `/1` database, record and verify:

1. the exact environment, host, absolute database path, old runtime commit, and
   current runtime commit;
2. the exact `/1` singleton and fixed `/1` table/index shape, with no partial,
   mixed, or foreign authority;
3. that every consumer is internal-test only and no external compatibility or
   replay promise depends on the database;
4. a complete inventory of unacknowledged schedules and selected deliveries,
   correlated with M1 activation receipts and in-flight producers; and
5. an independently authoritative source command or workload state from which
   every required pending timer can be scheduled again under `/2`.

## Stop conditions

Stop without modifying the database if ownership, generation, environment, or
the reconstruction source is uncertain; if a schedule or activation is in
flight or has an ambiguous acknowledgement; if M1 state cannot distinguish an
already admitted activation from an unadmitted timer; or if any public replay
promise exists. Do not add a compatibility decoder or derive a digest from the
old row to bypass a stop condition.

## Drain, reset, and reseed

1. Fence new timer scheduling and wait-source polling in the owning embedding.
2. Drain or classify every in-flight schedule, retained selection, activation
   CAS, and acknowledgement against its durable M1 receipt.
3. Preserve a byte-for-byte backup of the `/1` database and the exact old
   runtime that can inspect it; verify the backup before proceeding.
4. Detach the `/1` database as one unit. Do not edit or upgrade it in place.
5. Let the current runtime initialize one new empty database and read back the
   exact `cymule.activation-timer-store/2` singleton, table, and index set.
6. Recreate only timers proven pending and still required, using the normal
   current `schedule` API and the independently authoritative reconstruction
   source. Do not copy target selections or acknowledgements.
7. Resume polling and scheduling only after all verification passes.

## Rollback

Before a `/2` schedule or M1 activation is admitted, rollback may detach the
new empty database and restore the matched `/1` backup together with the exact
old runtime. The current runtime must never open that backup.

After `/2` scheduling or activation begins, swapping `/1` back is not a valid
rollback because the authorities have diverged. Fence the embedding again,
discard the owned internal-test `/2` state and its matched M1 test state as one
reset, then restore only a mutually consistent preflight snapshot. If that
snapshot is unavailable, leave the workload fenced.

## Verification and execution record

Verify exact `/2` DDL and singleton readback, WAL plus FULL durability, strict
row/digest closure after reopen, exact redelivery of a retained `/2` selection,
selection-required acknowledgement, SQLite `integrity_check`, the focused
timer process-death suite, and version-domain/source-closure checks on the exact
runtime commit.

For every real reset, append a non-secret execution record with UTC start and
finish, operator and workload owner, environment/path, old and new commits,
`/1` and `/2` generation evidence, drain/M1 correlation, backup identity,
reseed inventory, verification results, rollback use, and final disposition.

| Field | Current source record |
| --- | --- |
| UTC start and finish | Not executed |
| Operator and workload owner | Pending a real internal-test reset |
| Environment and database path | None |
| Old runtime and `/1` evidence | None retained by this source change |
| Current runtime and `/2` evidence | Source and focused tests only; deployment pending |
| Drain and M1 correlation | Not executed |
| Backup and reseed inventory | Not executed |
| Verification | Focused source validation; environment verification pending |
| Rollback used | No |
| Final disposition | Source implemented; operator execution pending if `/1` state exists |
