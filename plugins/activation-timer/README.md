# Cymule Timer Activation

`cymule-activation-timer` is a durable logical timer source backed by SQLite.
Schedules retain a stable activation ID, timer ID, due observation and typed
value. A timer is acknowledged only after Cymule commits its activation CAS;
lost acknowledgement redelivers the same delivery.

```sh
cargo add cymule-activation-timer
```

The default `SystemClock` observes Unix milliseconds. Tests and simulations can
inject a deterministic `Clock`. Clock values are substrate observations, not
canonical framework time.
