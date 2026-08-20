# Timer Activation Guidance

- Timer schedule identity, due observation, value, and acknowledgement are
  durable plugin state. Wall-clock time is an observation supplied by `Clock`,
  never canonical Cymule state.
- `receive` may return only a currently due timer with an exact target selected
  from `ParkedWaitIndex`. Missing targets leave the timer pending.
- Acknowledgement occurs only after the activation CAS. Lost acknowledgement
  must redeliver the identical activation ID, timer ID, target, and value.
- `acknowledge` must transactionally verify that durable target selection
  already exists. A scheduled but never selected timer cannot be acknowledged.
- SQLite contention uses a zero busy timeout and surfaces as a conflict. Tests
  use a manual clock; never depend on sleeps or wall-clock races.
- The live-process suite kills after schedule persistence, target selection,
  both M1 activation-CAS sides, and both acknowledgement sides. Reopen preserves
  the exact activation/source/targets/value and runs SQLite integrity probes.
