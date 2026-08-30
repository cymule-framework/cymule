# Timer Activation Guidance

- Timer schedule identity, due observation, value, and acknowledgement are
  durable plugin state. Wall-clock time is an observation supplied by `Clock`,
  never canonical Cymule state.
- The file-backed SQLite generation is the only timer authority. Do not expose
  an in-memory constructor or another process-local schedule/acknowledgement
  store, including for tests.
- The timer store accepts only the exact `cymule.activation-timer-store/3`
  physical generation. Initialize only a completely empty SQLite database
  inside one immediate transaction and verify the singleton generation row plus
  every fixed table/index DDL and UTF-8 database encoding before commit. Schema
  discovery reads at most the expected object count plus one, and every
  generation marker, object name, table name, and DDL projection is byte-capped
  before UTF-8 decode. Reject every nonempty mismatch with
  `unsupported_store_generation` before mutable PRAGMA or mutation; never
  ALTER, heal, or import it.
- Generation `/3` retains one `schedule_digest` over the complete activation
  ID, timer ID, due observation, and typed value. Fresh selection, retained
  delivery, schedule replay, and acknowledgement all load and validate the
  complete row, require strict canonical JSON bytes, and recompute that digest.
  Any direct field mismatch is stable `Integrity` before parked-wait selection
  or an M1 delivery; `/1` has no reader, importer, or fallback.
- `schedule` canonicalizes the value and rejects more than Core
  `MAX_ARTIFACT_BYTES` before opening a SQLite transaction or performing any
  write. Exact-limit values remain valid. Reopen treats an oversized retained
  value as stable `Integrity` rather than allocating it into a delivery.
- Every schedule replay, exact load, retained delivery, selection readback,
  and acknowledgement point read projects SQLite `length(...)` first and uses
  a `CASE` gate before Rust may receive `value_json` or `selected_wait_ids`.
  Value bytes use Core's artifact bound; the exactly-one content-ID target has
  a 75-byte canonical upper bound. An oversized generation-`/3` BLOB is stable
  `Integrity` and remains unacknowledged.
- The same preallocation gate owns every variable TEXT field. Activation and
  timer IDs use capped 2,049-byte BLOB projections plus exact
  512-scalar/2,048-byte verification; `schedule_digest` uses a capped 65-byte
  BLOB projection and the exact 64-byte lowercase-hex digest contract. SQL
  projects each field as a
  byte-capped BLOB and Rust performs the sole UTF-8 decode; invalid UTF-8 and
  oversized TEXT are `Integrity` before a full String reaches Rust. Fresh
  metadata also carries the activation byte/scalar lengths.
- Activation and timer identities use the shared 1..=512 Unicode-scalar,
  no-control-character contract. Never substitute UTF-8 byte length.
- `receive` may return only a currently due timer with an exact target selected
  through the bounded `ParkedWaitView` capability. The driver must not receive
  the complete parked-wait index; missing targets leave the timer pending. New
  due-source selection examines at most 256 stable
  `(due_unix_ms, activation_id)` rows per call and retains an exclusive cursor
  for the next poll. Reaching the end resets the cursor so new earlier rows also
  make progress; never restart at the first unmatched timer on every poll.
- Fresh due paging materializes only bounded activation/due/value-length
  metadata. It caps the activation identity projection, rejects oversized
  value metadata, then loads and authenticates exactly one complete row by
  activation ID before each parked-wait lookup. Never collect a page of timer
  payloads or schedule rows.
- Hot unacknowledged queries use the exact `acknowledged = 0` predicate. Fresh
  and retained timers use distinct due-order partial indexes for
  `selected_wait_ids IS NULL` and `IS NOT NULL`, so retained replay cannot scan
  an unselected due prefix. Exact point reads use the activation primary index;
  query-plan tests reject a table scan or temp B-tree for each path.
- Every newly selected or retained wait ID is an exact lowercase SHA-256
  content ID. An empty fresh selection means that source has no parked match
  and is not retained; every nonempty fresh selection and every retained timer
  delivery has exactly one target. Multiple fresh targets fail before
  retention, and an invalid retained set is adapter `Integrity`.
- Acknowledgement occurs only after the activation CAS. Lost acknowledgement
  must redeliver the identical activation ID, timer ID, target, and value.
- Redeliver retained due selections before selecting new timer sources. An
  earlier-due unselected timer or its view error must not block an already
  selected delivery. Due-time and the framework target maximum still apply to
  redelivery; a later caller limit does not reinterpret a retained selection.
- `acknowledge` must transactionally verify that durable target selection
  already exists. A scheduled but never selected timer cannot be acknowledged.
- SQLite contention uses a zero busy timeout and surfaces as a conflict. Tests
  use a manual clock; never depend on sleeps or wall-clock races.
- The live-process suite kills after schedule persistence, target selection,
  both M1 activation-CAS sides, and both acknowledgement sides. Reopen preserves
  the exact activation/source/targets/value and runs SQLite integrity probes.
- Live-process tests enter M1 only through public `DurableRuntimeControl`, its
  typed `drive_wait_source`, and Query/4 Run/wait reads. They must not import a
  private coordinator or `ResumableRuntime` to manufacture index/state access.
