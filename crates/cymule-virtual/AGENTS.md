# Large Virtual Work Guidance

Status: implemented Rust persistence surface. The normalized profile contract,
borrowing Durable Virtual control, single-CAS lowering, and closed archive
provider share one authority. Public fault/reopen and lease/fence conformance
is exercised by `tests/public_durable.rs`.

## Ownership

- `cymule-profile-protocol::virtual_work` owns every public Virtual wire DTO,
  identity, bound, normalized current leaf, typed mutation, pure reducer, query,
  read result, and commit envelope.
- `cymule-durable::DurableRuntimeControl::virtual_work()` is the only public
  persistence authority. Its `commit` accepts a semantic
  `VirtualPersistenceCommand`; callers cannot supply a checkpoint, delta,
  StateRoot, receipt, provider product, journal record, transaction callback,
  or prepared capability.
- This crate owns only provider orchestration. Its production implementation is
  `ResourceBackedVirtualArchive`, a binding-and-revision-pinned implementation
  of the closed `VirtualArchiveProvider` trait.
- Never recreate a scheduler, durable controller, raw-journal bridge, generic
  mutation callback, or compatibility façade in this crate.

## Normalized state

- One bounded `VirtualCurrent` contains scalar scheduling policy, bounded
  ready/active frontier, the exact bounded M1-Wait capacity directory, exact
  archive provider binding, nine semantic family roots, cumulative cold-index
  roots, counts, and the latest receipt ID. The Wait directory contains only
  reasons that own parked work and closes their aggregate future source and
  mutation item/byte budgets; it is not history or a provider projection. The
  current never embeds a full historical snapshot.
- Durable stores the exact keyed families `Regions`, `ActiveRegions`, `Parked`,
  `ParkedIndex`, `Work`, `Occurrences`, `Runs`, `Migrations`, and
  `Certificates`. `Regions` retains topology audit history; `ActiveRegions`
  contains only non-retired, non-exhausted materialization candidates in
  authenticated map order. Migration deletes source and inserts target active
  entries atomically, while terminal materialization removes its active entry.
  Thus ordinary successor selection never scans accumulated retired history.
  Each leaf verifies its scheduler, family, content identity, item bound, and
  canonical byte bound before mutation.
- All-ever command replay uses `virtual_receipt_key(scheduler_id, command_id)`.
  Current reads use `virtual_current_key(scheduler_id)`. Ordinary submit,
  replay, claim, renewal, recovery, migration, rehydration, and weight changes
  must not enumerate an application journal or an accumulated receipt map.
- A reducer receives only the minimal `VirtualKeyedSource` loaded from one exact
  parent StateRoot. Durable represents every membership and non-membership as
  a `VirtualStateRead`, retries `prepare_virtual` only for its typed
  `ReadRequired { family, storage_key }`, and rejects orphan reads. A missing
  leaf is therefore never confused with an unread key. Its typed
  `VirtualMutationSet` and result body digest are bound by the outer
  `VirtualPersistenceReceipt`; physical StateRoot revision is carried only by
  the non-semantic commit envelope, avoiding a receipt/current fixed point.
- Materialization selection is a typed non-Serde capability over the exact
  `ActiveRegions` family root. Durable verifies a one-entry authenticated map
  page after `last_region`; only an authenticated empty suffix may trigger one
  additional page from the map head. `VirtualActiveRegionSelectionProof`
  therefore represents one page or exactly one wrap, never a scan, a third
  read, or a caller-selected region.

## Providers and commands

- Public commands carry semantic intent and optimistic scalar prerequisites
  only. Materialized pages, migration coverage, archive publications,
  rehydrated occurrences, Clock/lease evidence, ExecutionBinding bytes, and M4
  selection products are non-Serde reduction authority constructed inside
  Durable after exact provider/M1 reads. Durable must complete
  `preflight_virtual_provider` against the pinned source before invoking a
  RegionSource, migrator, compactor, or rehydration provider; provider-derived
  keys then return through the same typed `prepare_virtual` read loop.
- Provider selection is immutable. Initialization fixes one
  `VirtualArchiveBinding { binding, revision }`; regions fix exact source
  operation/binding/revision; migrations fix their migrator generation. A
  missing or changed generation fails closed. There is no default or fallback
  provider.
- Virtual initialization defines a strictly sorted, unique
  `VirtualRunDefinition` for every referenced Run. Execution is exactly
  `Direct { plan_id }` or `Evolution { evolution_id, template_id }` and remains
  in `VirtualRunCurrent`.
- A claim carries only an exact ExecutionBinding `ArtifactRef` selected from
  its owning runtime's admitted complete binding.
  Durable first runs the pure fairness preview, derives the selected Run's Plan
  (including an optional standard Evolution selection), authenticates existing
  Plan and binding material against the same pinned M1 authority, runs
  `binding.admit_plan`, then commits first-use runtime binding material,
  Virtual occurrence, lease, and any Evolution receipt/pin in one StateRoot
  CAS. An empty claim still validates the selected binding. Virtual Run IDs
  are scheduling namespaces, not synthetic M1 Runs.
- There is no separate execution-material registration command or circular
  pre-existing-binding prerequisite. Do not put Plan or binding bytes in Virtual
  initialization, source Artifacts, publication evidence, or claim commands.
- A fresh claim returns its exact pre-CAS verified Plan with the acknowledged
  immutable receipt. A later writer cannot turn that known success into a
  current-root lookup failure. Dedicated claim replay reads the exact receipt
  and original Plan within one pinned callback without Clock/provider access;
  generic receipt-only commit replay does not load a Plan.

## Archive and Resource lifecycle

- `cymule.virtual-archive-manifest/2` is canonical, bounded cold history. The
  framework derives its exact bytes, occurrence/receipt range proofs, Resource
  descriptor, completion certificate, and cumulative work/command sparse-index
  updates. A provider only stores and reads those immutable products.
- `ResourceBackedVirtualArchive` verifies the complete content-addressed object
  before returning a selected occurrence or receipt. Descriptor-scoped proof
  catalog records are immutable locators, never semantic authority; returned
  proofs must equal the proof recomputed from the verified manifest. Resource
  conflict, not-found, substrate, persistence, uncertain-commit, and integrity
  failures retain their exact category and stable structured evidence when
  mapped into the profile protocol boundary.
- Cumulative work and command locators are fixed-depth authenticated indexes.
  A normal exact lookup resolves one path from the current root; it never scans
  certificates, archive catalogs, or hot history.
- Immutable archive upload may precede the M1 CAS and may leave an unreferenced
  content-addressed object. Compaction authority begins only when one typed
  Virtual CAS commits the certificate/current/receipt and the exact Resource
  VirtualArchive pin together. No standalone Resource retain operation exists.
- Retirement is one typed Virtual command. The same CAS retires the exact
  certificate and commits its derived Resource archive release. Ordinary
  Resource release/delete paths cannot release a VirtualArchive pin.
- Resource lifecycle origin references resolve the exact outer Virtual receipt
  by scheduler and command identity and verify its nested pin/release outcome;
  certificate IDs and nested Resource receipt IDs are not receipt aliases.

## Bounds and arithmetic

- Enforce the profile constants for current, keyed leaf, reducer source,
  mutation set, command, receipt, control envelope, materialized Artifact
  bytes, and archive object bytes before persistence and after decoding.
- Aggregate byte bounds are authoritative in addition to per-item and per-leaf
  bounds. Many individually legal leaves must not form an unbounded reduction
  source or receipt.
- Fairness, epochs, cursor progress, counts, proof ranges, and index arithmetic
  use checked exact-integer operations. Saturation is permitted only for an
  explicitly documented display clamp, never for semantic selection or
  authority.

## Verification

- Keep `cargo test -p cymule-profile-protocol --all-targets --locked` and
  `cargo test -p cymule-virtual --all-targets --locked` green.
- Both crates must pass Clippy for all targets with `--no-deps -- -D warnings`;
  do not add lint allowances to persistence code.
- Provider tests must prove exact publication readback, process-loss reopen,
  cumulative membership/non-membership lookup, wrong selector rejection,
  complete-object corruption rejection, and descriptor/proof mismatch failure.
- The shared retired-contract fixture has rejection-only status. The current
  command, bounded current, and claim receipt decoders must reject the old
  checkpoint, journal-base, and coupled-journal receipt shapes without aliases.
- Durable integration tests own single-CAS compaction pinning, retirement
  release, GC retained/eligible transitions, wrong origin receipt rejection,
  exact replay without provider I/O, and large-history ordinary-path
  no-enumeration assertions.
- `tests/public_durable.rs` owns the public-control soak replacements for
  archive failure/reopen and multi-worker lease/fence recovery. Use the official
  Resource-backed archive over the filesystem adapter, inject faults only at
  real provider or Store CAS boundaries, and reconcile exact receipts after
  response loss. Pure reducer fixtures and a second fake archive are not
  substitutes for these witnesses.
