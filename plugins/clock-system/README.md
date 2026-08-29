# Cymule System Clock

`cymule-clock-system` provides a production clock boundary for commands that
need lease or expiry observations without making ambient wall time part of
Cymule's semantic core.

`SqliteClock` observes the operating-system clock and persists a strictly
increasing logical value per scope in a file-backed SQLite database. It rejects
temporary, in-memory, and URI-selected memory backends before schema or
persistent PRAGMA mutation. If the wall clock moves backward or the process
restarts, later observations still advance. SQLite contention fails immediately,
including during authority/schema preflight, so retry and admission policy
remain with the caller.

`cymule-durable-protocol` is the public owner of the observation DTO, version,
identity, and pure verification contract. This adapter exports the stateful
`SqliteClock` and wall-clock boundary without aliasing those protocol types.

The resulting `ClockObservation` is evidence for constructing a typed command;
the command and its durable CAS receipt, not the clock database, decide whether
work was admitted. Exact resolution keeps older issued receipts available for
historical replay. Execution-current admission holds a non-blocking SQLite
writer transaction from the exact scope-head comparison through the command's
Store CAS callback, so the head cannot advance between freshness validation and
mutation. An older issued receipt cannot acquire or take over new work.

The Clock database is an exclusive authority file. `SqliteClock` rejects any
database containing non-Clock `cymule_*` objects before schema or persistent
PRAGMA mutation; a durable Store and a Clock must never share one SQLite file.
