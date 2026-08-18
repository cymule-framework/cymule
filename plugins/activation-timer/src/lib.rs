//! Durable logical timer source for Cymule.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_durable::{
    DurableError, DurableResult, ParkedWaitIndex, WaitActivationSource, WaitDelivery,
    WaitSourceDriver,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};
use serde_json::Value;

/// Clock observation boundary for timer plugins.
pub trait Clock {
    /// Current observed Unix time in milliseconds.
    fn now_unix_ms(&self) -> DurableResult<u64>;
}

/// System clock implementation for production timer polling.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DurableError::Substrate(error.to_string()))?;
        u64::try_from(duration.as_millis())
            .map_err(|error| DurableError::Substrate(error.to_string()))
    }
}

/// SQLite-backed durable timer source.
pub struct SqliteTimerDriver<C = SystemClock> {
    connection: Connection,
    clock: C,
}

impl SqliteTimerDriver<SystemClock> {
    /// Open or create a file-backed timer source.
    pub fn open(path: impl AsRef<Path>) -> DurableResult<Self> {
        Self::open_with_clock(path, SystemClock)
    }

    /// Create an in-memory timer source.
    pub fn in_memory() -> DurableResult<Self> {
        Self::in_memory_with_clock(SystemClock)
    }
}

impl<C: Clock> SqliteTimerDriver<C> {
    /// Open a file-backed timer source with an injected clock.
    pub fn open_with_clock(path: impl AsRef<Path>, clock: C) -> DurableResult<Self> {
        let connection = Connection::open(path).map_err(sqlite_error)?;
        Self::initialize(connection, clock, true)
    }

    /// Create an in-memory timer source with an injected clock.
    pub fn in_memory_with_clock(clock: C) -> DurableResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error)?;
        Self::initialize(connection, clock, false)
    }

    fn initialize(connection: Connection, clock: C, file_backed: bool) -> DurableResult<Self> {
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        if file_backed {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(sqlite_error)?;
            connection
                .pragma_update(None, "synchronous", "FULL")
                .map_err(sqlite_error)?;
        }
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS cymule_timers (
                    activation_id TEXT PRIMARY KEY NOT NULL,
                    timer_id TEXT NOT NULL,
                    due_unix_ms INTEGER NOT NULL,
                    value_json BLOB NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0
                ) STRICT;
                CREATE INDEX IF NOT EXISTS cymule_timers_due
                    ON cymule_timers(acknowledged, due_unix_ms, activation_id);",
            )
            .map_err(sqlite_error)?;
        Ok(Self { connection, clock })
    }

    /// Schedule or exactly replay one logical timer delivery.
    pub fn schedule(
        &mut self,
        activation_id: &str,
        timer_id: &str,
        due_unix_ms: u64,
        value: &Value,
    ) -> DurableResult<()> {
        validate_identity("activation", activation_id)?;
        validate_identity("timer", timer_id)?;
        let due = i64::try_from(due_unix_ms)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let bytes = cymule_core::canonical_bytes(value)?;
        let existing: Option<(String, i64, Vec<u8>, bool)> = self
            .connection
            .query_row(
                "SELECT timer_id, due_unix_ms, value_json, acknowledged
                 FROM cymule_timers WHERE activation_id = ?1",
                [activation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(sqlite_error)?;
        if let Some((current_timer, current_due, current_value, _acknowledged)) = existing {
            if current_timer == timer_id && current_due == due && current_value == bytes {
                return Ok(());
            }
            return Err(DurableError::Conflict {
                expected: Some(activation_id.to_owned()),
                current: Some("timer-identity-reused".to_owned()),
            });
        }
        self.connection
            .execute(
                "INSERT INTO cymule_timers(
                    activation_id, timer_id, due_unix_ms, value_json, acknowledged
                 ) VALUES (?1, ?2, ?3, ?4, 0)",
                params![activation_id, timer_id, due, bytes],
            )
            .map_err(contention)?;
        Ok(())
    }
}

impl<C: Clock> WaitSourceDriver for SqliteTimerDriver<C> {
    fn receive(
        &mut self,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        let now = i64::try_from(self.clock.now_unix_ms()?)
            .map_err(|error| DurableError::Substrate(error.to_string()))?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT activation_id, timer_id, value_json
                 FROM cymule_timers
                 WHERE acknowledged = 0 AND due_unix_ms <= ?1
                 ORDER BY due_unix_ms, activation_id",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map([now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(sqlite_error)?;
        for row in rows {
            let (activation_id, timer_id, bytes) = row.map_err(sqlite_error)?;
            let source = WaitActivationSource::Timer { timer_id };
            let selection = index.select(&source, max_targets)?;
            if selection.wait_ids.is_empty() {
                continue;
            }
            let value = serde_json::from_slice(&bytes)?;
            return Ok(Some(WaitDelivery {
                activation_id,
                source,
                wait_ids: selection.wait_ids,
                value,
            }));
        }
        Ok(None)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        validate_identity("activation", activation_id)?;
        self.connection
            .execute(
                "UPDATE cymule_timers SET acknowledged = 1
                 WHERE activation_id = ?1 AND acknowledged = 0",
                [activation_id],
            )
            .map_err(contention)?;
        let acknowledged = self
            .connection
            .query_row(
                "SELECT acknowledged FROM cymule_timers WHERE activation_id = ?1",
                [activation_id],
                |row| row.get::<_, bool>(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        if acknowledged == Some(true) {
            Ok(())
        } else {
            Err(DurableError::NotFound(format!(
                "timer activation {activation_id} is missing"
            )))
        }
    }
}

fn validate_identity(kind: &str, identity: &str) -> DurableResult<()> {
    if identity.is_empty() || identity.len() > 512 || identity.chars().any(char::is_control) {
        return Err(DurableError::Validation(format!(
            "timer {kind} identity must contain 1..=512 printable characters"
        )));
    }
    Ok(())
}

fn contention(error: rusqlite::Error) -> DurableError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: Some("sqlite-timer-writer-available".to_owned()),
                current: Some("sqlite-timer-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(error),
    }
}

fn sqlite_error(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
