# Cymule Timer Activation

`cymule-activation-timer` is a durable logical timer source backed by SQLite.
Schedules retain a stable activation ID, timer ID, due observation and typed
value. A timer is acknowledged only after Cymule commits its activation CAS;
the first exact wait targets are persisted before delivery, so lost
acknowledgement redelivers the same activation and targets even after those
waits leave the rebuilt parked index.

```sh
cargo add cymule-activation-timer
```

The default `SystemClock` reuses the wall-clock boundary from
`cymule-clock-system`. Tests and simulations can inject a deterministic
`Clock`. Clock values are substrate observations, not canonical framework time;
use `SqliteClock` when lease or scheduling commands require a logical value
that remains monotonic across restart and backward wall-clock movement.
