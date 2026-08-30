# HTTP Activation Guidance

- HTTP handlers enqueue identified signal observations; only the M1 wait driver
  selects exact parked targets and only `acknowledge` completes the HTTP
  response. Never return success before the activation CAS commits.
- The bounded waiter registry is backpressure, not durable authority.
  Producers must retry 503 responses with the same activation ID; a running
  request waits for commit acknowledgement.
- The SQLite spool is the only ingress, selection, and acknowledgement
  authority. Do not expose a process-local router/driver alternative,
  including for tests.
- Durable acknowledgement in SQLite is the response authority. In-process
  waiter notification is only a hint: register before the post-registration
  acknowledgement readback, and after an initially pending read wait only one
  fixed request window before exactly one more durable readback. Notification
  may end that window early but never authorizes success itself. If the bounded
  readback is still pending, return 503 so the producer retries the same
  activation ID; never start a polling or background-wait loop.
- The spool accepts only the exact `cymule.activation-http-spool/1` physical
  generation. Initialize only a completely empty SQLite database inside one
  immediate transaction, then reread the singleton generation row and every
  fixed table/index DDL before commit. Every later connection revalidates that
  same generation before PRAGMA, query, or write. A nonempty mismatch returns
  `unsupported_store_generation` without healing, ALTER, or import behavior.
- Read a bounded raw request body and pass it through the core recursive
  duplicate-rejecting decoder before authorization, digesting, or persistence.
  Reopened values use the same decoder; never let `serde_json::Value` collapse
  duplicate members first.
- Every retained, fresh, duplicate-ingress, acknowledgement, and selection
  read loads the complete signal row. Decode `value_json` and selected targets
  as strict canonical JSON, reconstruct the exact `HttpSignalRequest`, and
  recompute `request_digest` before target selection or delivery. A malformed,
  noncanonical, invalid, or mismatched retained field is stable `Integrity`;
  it must not call parked-wait selection or expose a delivery to M1.
- Every row query projects SQLite byte/scalar lengths before Rust receives a
  variable-length value. Activation and signal identities are capped at 512
  scalars and 2,048 UTF-8 bytes through `substr(..., 513)`; request digests use
  the exact 64-byte canonical-digest contract and `substr(..., 65)`; value JSON
  uses the 2 MiB body contract; selected-target JSON uses the exact
  `1 + MAX_WAIT_DELIVERY_TARGETS * 74` bound. Oversized TEXT/BLOB corruption is
  adapter `Integrity` without materializing the complete value in Rust.
- Duplicate IDs with identical source/value replay the original acceptance.
  Reuse with different semantics returns conflict and never reaches M1.
- Persist/classify one activation ID inside an immediate SQLite transaction.
  SQLite writer contention uses the zero busy timeout and surfaces as a typed
  conflict; never rely on a UNIQUE violation followed by transport retry for
  identity convergence.
- Concurrent HTTP/driver fixtures explicitly yield and retry only this
  adapter's typed non-blocking writer-contention result within a fixed test
  deadline. Other source-view or Store errors fail immediately; tests must not
  require every poll to win a concurrent ingress writer or introduce retry
  inside the production adapter.
- Durable matching first redelivers retained target selections, then pages the
  provider-neutral `ParkedWaitView` with its authenticated typed cursor and
  queries the spool through `(acknowledged, signal_key, activation_id)`. The
  driver must not receive or reconstruct the complete parked-wait index. Never
  scan a fixed activation-ID prefix; unrelated ingress cannot starve a later
  active source. Reset the cursor only for the view's typed `Stale` outcome;
  every actual view error remains an error.
- Every hot unacknowledged query uses `acknowledged = 0`, never `!= 1`.
  Retained and fresh selection states use distinct partial indexes whose
  predicates are respectively `selected_wait_ids IS NOT NULL` and `IS NULL`,
  so neither path scans the other state's prefix. Exact
  acknowledgement, duplicate-ingress, and selection readbacks use the
  activation primary index; query-plan tests reject full scans and temp
  B-trees.
- Validate a newly selected target set against the `max_targets` supplied to
  that exact `receive` call before SQLite may retain it. Once retained, the
  complete set is the original selection authority: a later caller's smaller
  bound must not reject, truncate, or reselect it. Retained sets remain subject
  to the framework-wide target maximum, and a concurrent loser of the
  selection update must classify the winning SQLite value as retained rather
  than reinterpret it under its own bound.
- Every new and retained wait ID is an exact lowercase SHA-256 content ID.
  Reject a forged new ID before the SQLite selection update; map a forged
  retained ID to the existing adapter `Integrity` boundary before delivery.
- Source-view regressions verify one-page reads, continuation beyond the scan
  budget, cursor preservation across actual errors, and retained-delivery
  replay without either source-page or target-selection reads.
- Authorization is an injected header/request policy. Never store credentials
  in deliveries, values, logs, or Cymule state.
- This plugin owns signal ingress only. Typed input completion remains with its
  owning higher-profile controller until a generic M1 input-source seam exists.
- The live-process suite kills after durable ingress, target selection, both M1
  activation-CAS sides, and both acknowledgement sides. An identical request
  must receive no success before acknowledgement and must converge after
  reopen; every SQLite file passes `integrity_check`.
- Live-process tests enter M1 only through public `DurableRuntimeControl`, its
  typed `drive_wait_source`, and Query/4 Run/wait reads. They must not import a
  private coordinator or `ResumableRuntime` to manufacture index/state access.
