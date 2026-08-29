# Clock Adapter Guidance

- Wall time is a substrate observation, never canonical Plan or Machine state.
- `cymule-durable-protocol` is the sole owner of Clock observation DTOs,
  versions, identities, and pure verification. Import it directly; this adapter
  must not re-export or copy those contracts. `cymule-durable` owns only the
  stateful resolution/current-head traits and durable error boundary used here.
- `SqliteClock` turns wall observations into a strictly increasing logical
  sequence per configured scope. Reopen and backward wall-clock movement must
  never reduce or repeat that sequence.
- Public Clock opens accept only a real file-backed SQLite `main` database.
  Use explicit read-write, create, URI, and no-mutex flags, then reject
  temporary or memory backends from SQLite's observed database list before
  schema or persistent PRAGMA mutation.
- Install SQLite's zero busy timeout immediately after opening the connection,
  before schema/authority preflight or any other query. Every `BUSY`/`LOCKED`
  result returns a conflict; do not wait on a process lock or hide retry policy
  inside the adapter.
- Callers carry only the opaque `cymule.clock-observation/2` reference in an
  idempotent execution, lease, or scheduling command. The selected Clock
  resolves the complete issued receipt and holds a non-blocking current-head
  writer guard while invoking the command Store CAS callback; checking the head
  and returning before that CAS is forbidden. The CAS retains the receipt with
  the admitted mutation. The read-only guard rolls back by drop after a
  successful callback and performs no fallible authority finalization that
  could misreport an already committed Store mutation.
  An unused observation after a crash is harmless. Clock issuance and command
  admission are distinct authorities; callers never copy or author logical
  time.
- Exact receipt resolution retains every issued observation for historical
  replay. Execution-current admission must compare that receipt with the
  matching scope-table head and keep the SQLite `IMMEDIATE` transaction open
  through the Store CAS; an older issued receipt cannot acquire or take over
  new work, and exact resolution is never a freshness fallback. A missing
  receipt that was never issued is `NotFound`; a retained receipt whose scope
  head or immutable head receipt is missing or inconsistent is `Integrity`.
- Every later allocation authenticates the retained scope head and its exact
  immutable receipt inside the same `IMMEDIATE` transaction before calculating
  or writing a successor. Validate both persisted integer fields first; a
  missing, malformed, mismatched, negative, or above-exact-range head or receipt
  is `Integrity` and must never be overwritten as an implicit repair.
- Reject a wall observation outside the exact cross-language integer range
  before changing a scope head or immutable receipt.
- Preserve the stable `clock_before_unix_epoch`, `clock_value_out_of_range`,
  and `clock_sqlite_failed` substrate codes at the public durable error
  boundary; human-readable messages are not classification authority.
- Retained negative or above-exact-range observation or scope-head integers are
  `Integrity` with field-specific stable `clock_observation_*_invalid` or
  `clock_scope_head_*_invalid` codes. They are persisted-authority corruption,
  never caller `Validation` or a retryable SQLite substrate failure.
- Any otherwise malformed retained observation receipt is likewise
  `Integrity`; lower pure-protocol `Validation` must not escape from persisted
  bytes as if the caller authored them.
- A Clock database is one exclusive Cymule authority. Opening a database that
  already contains any non-Clock `cymule_*` object fails before schema or
  persistent PRAGMA mutation. Store and Clock authorities use separate files.
- Test that boundary with a minimal foreign `cymule_*` SQLite object through
  `rusqlite`; the Clock adapter must not depend on a concrete Store plugin or
  create a publish-graph cycle merely to construct foreign authority.
- Source and scope identities are configuration, not authorization. Do not put
  credentials, hostnames, or provider-specific topology into observations.
- Real process-death tests bracket the public `observe` boundary. A committed
  observation with a lost caller receipt remains consumed, and a regressed wall
  clock cannot reuse its logical time after reopen. This is not an intra-WAL or
  power-loss claim.
