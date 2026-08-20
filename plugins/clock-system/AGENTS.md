# Clock Adapter Guidance

- Wall time is a substrate observation, never canonical Plan or Machine state.
- `SqliteClock` turns wall observations into a strictly increasing logical
  sequence per configured scope. Reopen and backward wall-clock movement must
  never reduce or repeat that sequence.
- SQLite writer contention uses a zero busy timeout and returns a conflict. Do
  not wait on a process lock or hide retry policy inside the adapter.
- Callers copy an observation's logical time into an idempotent lease or
  scheduling command. An unused observation after a crash is harmless; the
  command remains the durable semantic authority.
- Source and scope identities are configuration, not authorization. Do not put
  credentials, hostnames, or provider-specific topology into observations.
- Real process-death tests bracket the public `observe` boundary. A committed
  observation with a lost caller receipt remains consumed, and a regressed wall
  clock cannot reuse its logical time after reopen. This is not an intra-WAL or
  power-loss claim.
