# Timer Activation Store Generation 3

Status: the source hard cut is implemented. No operator migration has been
executed by this source change. Retained internal-test `/1` or `/2` state
requires the drain and recreation procedure below before a `/3` runtime can
serve it.

## Authority and scope

The embedding operator owns the configured timer database. The workload owner
owns the durable M1 state and the independently authoritative command or
workload state from which a pending timer can be scheduled again. This runbook
applies only to an exact `cymule.activation-timer-store/1` or
`cymule.activation-timer-store/2` database whose consumers remain internal or
pre-release.

`cymule.activation-timer-store/3` is the sole current physical generation. Its
fixed DDL adds selection-aware fresh and retained partial indexes, and its read
paths bound and validate every variable row field before typed decoding. The
complete schedule digest introduced by historical `/2` remains required, but
the changed DDL means an existing `/2` database cannot truthfully retain the
same physical marker. The `/3` runtime has no `/1` or `/2` reader, importer,
alias, dual writer, or decode-failure fallback. A migration never edits marker
bytes, copies timer rows, or synthesizes schedule digests, selections, or
acknowledgements.

## Preflight

Record and verify all of the following before changing a retained store:

1. the exact environment, host, absolute database path, store binding, old
   runtime commit, and current runtime commit;
2. the exact predecessor singleton, table, and index shape, with no partial,
   mixed, extended, or foreign SQLite authority;
3. that every consumer is internal-test only and no public compatibility or
   replay promise depends on the database;
4. a complete inventory of unacknowledged fresh timers, retained target
   selections, active wait-source calls, and every producer that can schedule
   new timers;
5. correlation of each selected timer with its exact M1 activation receipt so
   an already admitted activation cannot be scheduled again; and
6. for each timer still required after the cut, an independently authoritative
   source command or workload state containing its intended activation ID,
   timer ID, due observation, and typed value. A predecessor-row export alone
   is not that source.

## Stop conditions

Stop without modifying the database if ownership, environment, or exact
predecessor shape is uncertain; if scheduling or polling cannot be fenced; if a
schedule, activation CAS, or acknowledgement is active or has an unknown
outcome; if M1 cannot prove whether a selected activation was admitted; if a
required timer lacks an independent reconstruction source; or if any public
replay promise exists. Do not add a predecessor decoder or synthesize missing
authority to bypass a stop condition.

## Drain, export, requeue, and recreate

1. Fence new timer scheduling and wait-source polling at the owning embedding.
2. Drain each in-flight timer through terminal acknowledgement, or classify its
   exact schedule and M1 activation outcome. Keep the embedding fenced while
   any result remains unknown.
3. Export a non-secret workload manifest that identifies each timer still
   required and links it to its independent reconstruction source plus M1
   correlation. The manifest is operational evidence only; do not export
   SQLite rows as `/3` import material.
4. Preserve and verify a byte-for-byte backup of the complete predecessor
   database and the exact old runtime capable of inspecting it. Detach the old
   database as one unit; never edit it in place.
5. Let the current runtime initialize a new empty database. Read back the exact
   `cymule.activation-timer-store/3` singleton, fixed tables, selection-aware
   partial indexes, WAL mode, and FULL synchronous durability.
6. Recreate only timers proven not admitted and still required. Schedule each
   through the normal current API from its independently authoritative source.
   Never copy predecessor digests, target selections, or acknowledgement state.
7. Recreate any explicitly owned internal-test producer state needed for those
   schedules, then reconcile each new activation with M1 before accepting the
   next one.
8. Resume polling and scheduling only after every verification below passes.

## Rollback

Before the new store accepts a schedule, rollback may detach the empty `/3`
database and restore the complete predecessor backup together with its exact
old runtime and routing authority. The current runtime must never open that
backup.

After `/3` scheduling or an M1 activation begins, restoring `/1` or `/2` is not
a valid rollback because the authorities have diverged. Fence scheduling again,
classify all `/3` outcomes, and restore only a matched preflight snapshot of the
old runtime, predecessor database, producer state, and M1 test state. If no
mutually consistent snapshot exists, leave the workload fenced and preserve all
databases for investigation.

## Verification

Verify on the exact current commit:

- exact `/3` singleton and DDL readback, including both partial-index predicates;
- SQLite `integrity_check`, WAL journal mode, and FULL synchronous durability;
- rejection of `/1`, `/2`, partial, extended, and same-marker shape-drift
  databases;
- bounded invalid-row classification before typed decoding;
- strict schedule-digest closure for fresh, retained, point-read, replay, and
  acknowledgement paths;
- exact retained-target redelivery and selection-required acknowledgement after
  process restart;
- the focused timer activation and process-death suites; and
- version-domain, generated-document, and source-closure verification.

## Non-secret execution record

Append one record for every real cut. Never include timer values, credentials,
or customer data.

| Field | Current source record |
| --- | --- |
| UTC start and finish | Not executed |
| Operator and workload owner | Pending a real internal-test cut |
| Environment and database path | None |
| Old runtime and predecessor evidence | None retained by this source change |
| Current runtime and `/3` evidence | Source implementation only; operator readback pending |
| Drain and M1 correlation | Not executed |
| Backup identity and verification | Not executed |
| Exported workload manifest | Not created |
| Recreated timer count and identities | Not executed |
| Verification results | Focused source validation pending final exact-commit verification |
| Rollback used | No |
| Final disposition | Source implemented; operator execution pending if predecessor state exists |
