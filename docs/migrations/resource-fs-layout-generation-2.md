# Filesystem Resource Layout Generation 2

Status: exact `/1` rejection is implemented; compatibility migration is
unsupported. Any retained internal-test `/1` root requires an operator-owned
drain, reset, and reseed before the current runtime may serve it.

## Owner and scope

The owner is the operator of the embedding that configured the filesystem
Resource root, together with the workload owner who can prove the authoritative
source for every Resource that must be reseeded. This runbook applies only to
roots whose exact marker is `cymule.resource-fs-layout/1` and whose consumers
are still internal or pre-release.

The current `cymule.resource-fs-layout/2` store has no `/1` reader, importer,
dual writer, or decode-failure fallback. Reset means replacing the complete
physical root with a newly initialized empty `/2` root. Reseed means publishing
Resources again through the current `FsResourceStore` APIs from independently
authoritative source bytes or manifests. Copying `/1` objects, indexes,
catalogs, upload records, locks, staging entries, or marker bytes into `/2` is
not a migration and is forbidden.

## Preflight

Record and verify all of the following before changing a root:

1. the exact environment, host, absolute root, configured store binding, old
   runtime version, and current runtime commit;
2. an exact `/1` marker with no mixed, missing, partial, or unknown layout
   generation;
3. internal-test ownership of every consumer and confirmation that no public
   compatibility or replay promise exists for the retained root;
4. the complete workload-level inventory of active uploads, readers, pins,
   deletion/reconciliation work, and durable Resource references;
5. an independently authoritative, immutable or otherwise reproducible source
   for every Resource that must exist after reseed; and
6. a recoverable byte-for-byte backup of the complete `/1` root, its ownership,
   permissions, and the exact old runtime capable of reading it.

## Stop conditions

Stop without modifying the root when any preflight item cannot be proved, when
new Resource admission cannot be fenced, when an upload or lifecycle mutation
remains active or ambiguous, when an unresolved durable reference still depends
on `/1`, when source bytes needed for reseed are unavailable, or when the root
contains mixed or unrecognized physical entries. Do not introduce a
compatibility reader or copy selected files to bypass a stop condition.

## Drain, reset, and reseed

1. Fence new Resource admission at the owning embedding and drain every active
   reader, upload, pin, release, deletion, and reconciliation operation.
2. Read back the workload and durable authorities until no operation can still
   write the `/1` root. Preserve the exact preflight inventory in the execution
   record.
3. Take and verify the complete recoverable `/1` backup. Detach the old root as
   one unit; do not edit it in place.
4. Initialize a new empty root with the exact current runtime and verify that it
   durably contains `cymule.resource-fs-layout/2` plus only the current fixed
   namespaces.
5. Reseed each required Resource through normal current publication APIs from
   its recorded authoritative source. Rebuild all locator, manifest-index, and
   catalog projections through those APIs. Never synthesize retained receipts
   or assume an old semantic identity without current verification.
6. Rebind or recreate only the internal-test durable state explicitly owned by
   this reset, then verify it resolves the newly published Resources.
7. Resume admission only after every verification below passes.

## Rollback

Before any `/2` write or durable reference is admitted, rollback consists of
detaching the new empty root, restoring the complete `/1` backup, and restoring
the exact old runtime and routing authority together. The current runtime must
never be pointed at that backup.

After `/2` publication or durable rebinding begins, swapping the old root back
is not a valid rollback because the two authorities have diverged. Fence
admission again, discard the internal-test `/2` state as one owned reset, restore
the workload and `/1` root from the matched preflight backups, and restart only
the exact old runtime. If those matched backups are unavailable, stop and leave
the workload fenced.

## Verification

Verify and record all of the following before resuming:

- writable and read-only open both read back exactly
  `cymule.resource-fs-layout/2`;
- the new root contains no `/1` marker or copied `/1` physical namespace entry;
- each reseeded Resource passes current stat/read or exact manifest-list proof
  verification under its configured binding;
- required locator, manifest-index, and catalog projections were rebuilt by
  current publication and survive reopen;
- no active durable reference resolves through the detached `/1` root; and
- the focused filesystem Resource suite and version-domain verification pass on
  the exact deployed source commit.

## Execution record

No reset has been executed by this source change. For each real execution,
append a non-secret record containing:

| Field | Value |
| --- | --- |
| UTC start and finish | Pending |
| Operator and workload owner | Pending |
| Environment, host, root, binding | Pending |
| Old runtime and `/1` marker evidence | Pending |
| Current source commit and `/2` marker evidence | Pending |
| Drain and active-operation readback | Pending |
| Backup identity and restore test | Pending |
| Reseed source inventory | Pending |
| Verification evidence | Pending |
| Rollback used | Pending |
| Final disposition | Pending |
