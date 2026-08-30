# Cymule Timer Activation

`cymule-activation-timer` is a durable logical timer source backed by SQLite.
Schedules retain a stable activation ID, timer ID, due observation and typed
value. Generation `/2` also retains one canonical `schedule_digest` over all
four fields. Fresh selection, retained redelivery, exact schedule replay, and
acknowledgement load the complete row, require strict canonical value/target
bytes, and recompute that digest before a delivery can reach M1. A timer is
acknowledged only after Cymule commits its activation CAS;
the exact wait target is persisted before delivery, so lost acknowledgement
redelivers the same activation and target even after that wait leaves the
rebuilt parked index.

The canonical timer value is bounded by Core `MAX_ARTIFACT_BYTES`. Scheduling
rejects an oversized value before any SQLite transaction or write, while the
exact limit remains valid. Every timer delivery targets exactly one wait whose
identity is a lowercase SHA-256 content ID; malformed, empty, or multi-target
retained selections are durable row corruption and surface as `Integrity`. An
empty fresh selection remains the normal no-matching-wait result and is never
persisted as a target set.

Every row read obtains SQLite byte lengths before the engine can materialize
the value or selected-target BLOB. SQL returns those bytes only when the value
is within Core's artifact limit and the one-target JSON is within its exact
75-byte canonical ceiling. Oversized generation-`/2` corruption therefore
returns `Integrity` from receive, replay, selection readback, and
acknowledgement without copying the BLOB into Rust.

Retained target selections always redeliver first and remain independent of a
later caller's target limit; fresh selections are checked against their own
call limit before retention. Fresh due-source discovery uses a fixed 256-row
scan budget and an exclusive `(due_unix_ms,
activation_id)` continuation between polls. Large prefixes of due timers with
no matching parked wait therefore cannot make one poll unbounded or starve a
later matching timer; the cursor resets after reaching the current end so newly
inserted earlier rows remain visible. The scan page contains only capped
activation identity, due-time, and value-length metadata. The driver loads and
authenticates one complete row at a time, so a 256-row page never accumulates
256 timer payloads in memory. Fresh and retained queries use
`acknowledged = 0` and the due composite index; exact replay and acknowledgement
reads use the activation primary index without a temporary sort.

Activation and timer identities share Cymule's cross-language boundary: 1..=512
Unicode scalar values with no control character. Multi-byte identities are not
measured by their UTF-8 byte length.

The SQLite store has one physical generation,
`cymule.activation-timer-store/2`. A completely empty database is initialized
atomically; every nonempty database must already contain the exact singleton
generation and fixed table/index DDL before configuration or data access.
Older, partial, foreign, or modified databases fail with
`unsupported_store_generation` and are not altered. This crate has no in-place
upgrade, importer, or process-local alternate authority. The predecessor `/1`
shape has no reader or decode fallback; retained internal-test state must be
drained and reseeded under the current schedule authority.

```sh
cargo add cymule-activation-timer
```

The default `SystemClock` reuses the wall-clock boundary from
`cymule-clock-system`. Tests and simulations can inject a deterministic
`Clock`. Clock values are substrate observations, not canonical framework time;
use `SqliteClock` when lease or scheduling commands require a logical value
that remains monotonic across restart and backward wall-clock movement.
