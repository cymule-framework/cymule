# Timer Activation Guidance

- Timer schedule identity, due observation, value, and acknowledgement are
  durable plugin state. Wall-clock time is an observation supplied by `Clock`,
  never canonical Cymule state.
- The file-backed SQLite generation is the only timer authority. Do not expose
  an in-memory constructor or another process-local schedule/acknowledgement
  store, including for tests.
- The timer store accepts only the exact `cymule.activation-timer-store/1`
  physical generation. Initialize only a completely empty SQLite database
  inside one immediate transaction and verify the singleton generation row plus
  every fixed table/index DDL before commit. Reject every nonempty mismatch with
  `unsupported_store_generation` before PRAGMA or mutation; never ALTER, heal,
  or import it.
- Activation and timer identities use the shared 1..=512 Unicode-scalar,
  no-control-character contract. Never substitute UTF-8 byte length.
- `receive` may return only a currently due timer with an exact target selected
  through the bounded `ParkedWaitView` capability. The driver must not receive
  the complete parked-wait index; missing targets leave the timer pending. New
  due-source selection examines at most 256 stable
  `(due_unix_ms, activation_id)` rows per call and retains an exclusive cursor
  for the next poll. Reaching the end resets the cursor so new earlier rows also
  make progress; never restart at the first unmatched timer on every poll.
- Acknowledgement occurs only after the activation CAS. Lost acknowledgement
  must redeliver the identical activation ID, timer ID, target, and value.
- Redeliver retained due selections before selecting new timer sources. An
  earlier-due unselected timer or its view error must not block an already
  selected delivery; due-time and target bounds still apply to redelivery.
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
