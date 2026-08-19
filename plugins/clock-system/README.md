# Cymule System Clock

`cymule-clock-system` provides a production clock boundary for commands that
need lease or expiry observations without making ambient wall time part of
Cymule's semantic core.

`SqliteClock` observes the operating-system clock and persists a strictly
increasing logical value per scope. If the wall clock moves backward or the
process restarts, later observations still advance. SQLite contention fails
immediately so retry and admission policy remain with the caller.

The resulting `ClockObservation` is evidence for constructing a typed command;
the command and its durable CAS receipt, not the clock database, decide whether
work was admitted.
