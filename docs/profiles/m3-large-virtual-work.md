# M3 Large Virtual Work Profile

Status: partial terminal candidate. The normalized reducers and bounded provider
contracts are implemented; complete public Durable integration and the final
fault, packaging, schema and cross-language gates are not yet closed.

## Authority

`cymule-profile-protocol::virtual_work` owns the closed DTOs, identities,
fairness policy and pure reducer. `cymule-durable` resolves authenticated exact
reads, invokes the selected provider and commits the normalized postcondition.
`cymule-virtual` owns archive/provider realization, not a second scheduler.

The public writer is `DurableVirtualControl`, borrowed from a
`DurableRuntimeControl` with its admitted execution binding, provider registry
and Clock. Store-only control exposes provider-free exact reads. Neither facade
accepts a whole scheduler snapshot, raw Machine delta, generic journal append,
caller-authored postcondition, or arbitrary storage key.

## Normalized state and admission

One scalar scheduler current binds bounded scheduling/frontier metadata and
the profile's authenticated roots. Regions, active-region membership, Runs,
work, occurrences, parked buckets, migrations, certificates and command
receipts live in separately keyed families. An ordinary open pins the StateRoot;
it does not replay all history or reconstruct the complete scheduler.

Every command first checks its exact all-ever receipt. An identical command
returns the original semantic receipt without provider I/O or a second CAS.
Reusing its identity with another body fails. A fresh command resolves only the
typed read requirements of its reducer from one pinned source. Extra,
mismatched, stale or missing authority is not silently replaced.

The reducer's complete normalized mutation set, any coupled M1/M4 transitions,
new Plans/Artifacts, and the command receipt enter one Store CAS. Physical
observed/committed revisions belong to the non-persisted `VirtualCommit`
envelope, not the self-authenticating semantic receipt. Failed admission or a
pre-CAS crash publishes no partial profile state.

## Materialization and claims

`RegionSource` pins an operation, binding and revision. Its cursor is opaque.
The framework checks bounded page progress, exact source identity, unique work
IDs, and every payload Artifact's bytes and reference before publishing the
cursor and work together. A changed provider generation cannot reinterpret a
retained region.

Weighted-deficit selection and integer priority aging operate on bounded,
materialized, capability-compatible work. Active-region selection uses an
authenticated ordered page and at most one wrap; it does not repeatedly scan a
fixed prefix. Logical source cardinality is independent of frontier cardinality.

A claim resolves the selected Run's fixed Plan or its retained Evolution
selector. Evolution selection, the Virtual occurrence and capacity-slot lease,
and their required M1 material share one CAS. The runtime owner's already
admitted full execution binding supplies first-use material; callers cannot
register arbitrary binding bytes or provide a different selector at claim time.

The public result is the closed `VirtualClaimOutcome`:

- `NoWork` carries the exact normalized receipt.
- `Claimed` additionally carries a non-null claim and complete verified
  `SealedPlan` loaded from the same pinned authority.

The persisted receipt retains Plan identity and binding reference, not another
Plan copy. No nullable public Plan or raw Plan reader exists.

## Ownership, settlement and wakeup

Each active claim binds worker, work epoch, capacity-slot lease epoch and an
issued Clock reference. Freshness authorization holds the Clock head guard
through the final Store CAS. Historical Clock resolution alone never permits
a mutation. Expiry changes nothing until an explicit recovery command is
admitted; old output loses after lease replacement or takeover.

Success, retry, park, failure and cancellation are closed dispositions. Retry
creates a later Virtual occurrence; it does not erase history or redefine the
Core component's independent provider Attempt. Lease renewal advances the lease
fence without changing the work epoch or implementation pin.

Park/wake uses exact reason and bucket keys. Identified M1 activation retains
its complete original target set and winning subset; M3 wakes only admitted
targets. Receipt replay precedes pending-membership validation, so lost
acknowledgement remains recoverable after those waits become terminal. An M1
Wait reason is the exact Wait content ID. The scalar frontier retains a bounded
directory of only Wait reasons that currently own parked Virtual work, together
with the exact source and mutation item/byte charge required to wake each
reason. Parking is rejected before CAS if waking all retained Wait reasons
together would exceed either aggregate bound. Activation intersects its winning
M1 subset with this directory, so unrelated M1 targets require no negative
Virtual lookups, and recomputes each selected charge from the exact keyed leaves
before removing it atomically.

## Opaque region migration

Migration is provider-neutral planning plus verification, not cursor
interpretation by the scheduler. The pinned migrator returns one non-Serde
proposal containing the complete `RegionMigrationPlan`, coverage-evidence
ArtifactRecord and exact target-source ArtifactRecords. The framework verifies
all target/reference/byte relationships, rejects duplicate or unrelated
material, and applies the combined 4 MiB material budget before admission.

Verified coverage, source retirement, target activation, new material and the
receipt commit together. Existing work and historical occurrences retain their
original region identities. A Ref-only proposal with missing bytes is not an
admissible migration and has no separate registration fallback.

## Archive and retention

Cold history is an immutable Resource, selected through its pinned archive
provider. The framework computes the content descriptor, causal-cut
certificate, terminal fences, summaries and index proofs. Hot state retains
the descriptor/certificate and authenticated lookup roots, never a full cold
archive in Machine Artifacts.

Compaction requires a completed or retired region without active work. Exact
rehydration verifies the selected occurrence proof against the retained
certificate and complete bounded archive authority; it cannot widen a request.
Archived work/command membership prevents rematerialization or identity reuse
without scanning every historical certificate.

An archive pin and its profile transition share one CAS. Only the typed archive
retirement transition may release that pin. Physical GC remains downstream of
the exact M1 roots and Resource retention authority.

## Verification boundary

Before marking the complete profile implemented, the public Durable path must
pass materialization, fairness, empty claim, lease renewal, recovery, late
output, park/wake, migration, archive, retirement and exact replay tests.
Fault sweeps must inject failure before and after the relevant CAS/provider
boundary, reopen authority and verify exact receipts and call counts. Pure
DTO/reducer tests do not replace these end-to-end witnesses.

The current source/limit inventory is owned by `versioning/version-domains.json`
and the named constants in `virtual_work.rs`. Public schema fixtures describe
the live controls and receipts only. The retired checkpoint/snapshot and
journal-base models have no current producer or restoration authority; they
must not be kept alive by a schema-only positive fixture.

Cross-domain ownership, distributed consensus, autoscaling, queue delivery,
recursive authoring and execution isolation remain proposed separate profiles
or provider concerns. This profile does not claim those guarantees.
