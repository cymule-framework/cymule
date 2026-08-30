# HTTP Activation Spool Generation 2

Status: the source hard cut is implemented. No operator migration has been
executed by this source change. Retained internal-test `/1` state requires the
drain and recreation procedure below before a `/2` runtime can serve it.

## Authority and scope

The embedding operator owns the configured HTTP spool database. The workload
owner owns the durable M1 state and the independently authoritative source for
any request that must be submitted again. This runbook applies only to an exact
`cymule.activation-http-spool/1` database whose entire consumer set remains
internal or pre-release.

`cymule.activation-http-spool/2` is the sole current physical generation. Its
fixed DDL includes selection-aware fresh and retained partial indexes, and its
read paths bound and validate every variable row field before typed decoding.
Those changes alter physical authority, so an existing `/1` database cannot be
accepted under the old marker. The `/2` runtime has no `/1` reader, importer,
alias, dual writer, or decode-failure fallback. A migration never edits marker
bytes, copies spool rows, or synthesizes selections or acknowledgements.

## Preflight

Record and verify all of the following before changing a retained spool:

1. the exact environment, host, absolute database path, store binding, old
   runtime commit, and current runtime commit;
2. the exact `/1` singleton, table, and index shape, with no partial, mixed,
   extended, or foreign SQLite authority;
3. that every consumer is internal-test only and no public compatibility or
   replay promise depends on the database;
4. a complete inventory of unacknowledged fresh rows, retained target
   selections, ingress responses still waiting for acknowledgement, and every
   producer that can submit new requests;
5. correlation of each selected row with its exact M1 activation receipt so an
   already admitted activation cannot be submitted again; and
6. for each request still required after the cut, an independently
   authoritative request source containing the complete activation ID, signal
   key, typed value, and authorization context. A spool-row export alone is not
   that source.

## Stop conditions

Stop without modifying the database if ownership, environment, or exact `/1`
shape is uncertain; if ingress cannot be fenced; if a request, activation CAS,
or acknowledgement is active or has an unknown outcome; if M1 cannot prove
whether a selected activation was admitted; if a required request lacks an
independent reconstruction source; or if any public replay promise exists. Do
not introduce a compatibility decoder or reinterpret `/1` bytes to bypass a
stop condition.

## Drain, export, requeue, and recreate

1. Fence new HTTP ingress and wait-source polling at the owning embedding.
2. Drain each in-flight request through terminal ingress acknowledgement, or
   classify its exact request and M1 activation outcome. Keep the embedding
   fenced while any result remains unknown.
3. Export a non-secret workload manifest that identifies each request still
   required and links it to its independent source plus M1 correlation. This
   manifest is operational evidence only; do not export SQLite rows as `/2`
   import material.
4. Preserve and verify a byte-for-byte backup of the complete `/1` database and
   the exact old runtime capable of inspecting it. Detach the original database
   as one unit; never edit it in place.
5. Let the current runtime initialize a new empty database. Read back the exact
   `cymule.activation-http-spool/2` singleton, fixed tables, selection-aware
   partial indexes, WAL mode, and FULL synchronous durability.
6. Requeue only requests proven not admitted and still required. Submit each
   through the normal current HTTP ingress from its independently authoritative
   source. Never copy selected targets or acknowledgement state.
7. Recreate any explicitly owned internal-test producer state needed for those
   submissions, then reconcile each new activation with M1 before accepting the
   next one.
8. Resume polling and ingress only after every verification below passes.

## Rollback

Before the new spool accepts a request, rollback may detach the empty `/2`
database and restore the complete `/1` backup together with the exact old
runtime and routing authority. The current runtime must never open that backup.

After `/2` ingress or an M1 activation begins, restoring `/1` is not a valid
rollback because the two authorities have diverged. Fence ingress again,
classify all `/2` outcomes, and restore only a matched preflight snapshot of the
old runtime, `/1` database, producer state, and M1 test state. If no mutually
consistent snapshot exists, leave the workload fenced and preserve both
databases for investigation.

## Verification

Verify on the exact current commit:

- exact `/2` singleton and DDL readback, including both partial-index predicates;
- SQLite `integrity_check`, WAL journal mode, and FULL synchronous durability;
- rejection of `/1`, partial, extended, and same-marker shape-drift databases;
- bounded invalid-row classification before typed decoding;
- strict request-digest closure for fresh, retained, point-read, replay, and
  acknowledgement paths;
- exact retained-target redelivery and acknowledgement after process restart;
- the focused HTTP activation and process-death suites; and
- version-domain, generated-document, and source-closure verification.

## Non-secret execution record

Append one record for every real cut. Never include request values,
authorization material, credentials, or customer data.

| Field | Current source record |
| --- | --- |
| UTC start and finish | Not executed |
| Operator and workload owner | Pending a real internal-test cut |
| Environment and database path | None |
| Old runtime and `/1` evidence | None retained by this source change |
| Current runtime and `/2` evidence | Source implementation only; operator readback pending |
| Drain and M1 correlation | Not executed |
| Backup identity and verification | Not executed |
| Exported workload manifest | Not created |
| Requeued request count and identities | Not executed |
| Verification results | Focused source validation pending final exact-commit verification |
| Rollback used | No |
| Final disposition | Source implemented; operator execution pending if `/1` state exists |
