# Timer Activation Guidance

- Timer schedule identity, due observation, value, and acknowledgement are
  durable plugin state. Wall-clock time is an observation supplied by `Clock`,
  never canonical Cymule state.
- `receive` may return only a currently due timer with an exact target selected
  from `ParkedWaitIndex`. Missing targets leave the timer pending.
- Acknowledgement occurs only after the activation CAS. Lost acknowledgement
  must redeliver the identical activation ID, timer ID, target, and value.
- SQLite contention uses a zero busy timeout and surfaces as a conflict. Tests
  use a manual clock; never depend on sleeps or wall-clock races.
