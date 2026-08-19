//! Restart-monotonic logical clock observations for Cymule adapters.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_durable::{DurableError, DurableResult};
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

/// Frozen clock-observation record version.
pub const CLOCK_OBSERVATION_VERSION: &str = "cymule.clock-observation/1";

/// One wall-clock source used below the logical clock adapter.
pub trait WallClock {
    /// Return the current observed Unix time in milliseconds.
    fn now_unix_ms(&self) -> DurableResult<u64>;
}

/// Operating-system wall-clock source.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DurableError::Substrate(error.to_string()))?;
        u64::try_from(duration.as_millis())
            .map_err(|error| DurableError::Substrate(error.to_string()))
    }
}

/// Content-identified logical-clock observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClockObservation {
    /// Record version.
    pub clock_version: String,
    /// Content identity of this observation.
    pub observation_id: String,
    /// Stable configured source identity.
    pub source_id: String,
    /// Caller-selected independent logical clock scope.
    pub scope: String,
    /// Strictly increasing value used by lease and expiry commands.
    pub logical_time: u64,
    /// Non-authoritative wall time observed while allocating the value.
    pub observed_unix_ms: u64,
}

impl ClockObservation {
    /// Validate the frozen shape and content identity.
    pub fn verify(&self) -> DurableResult<()> {
        if self.clock_version != CLOCK_OBSERVATION_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported clock observation version {}",
                self.clock_version
            )));
        }
        validate_identity("source", &self.source_id)?;
        validate_identity("scope", &self.scope)?;
        let expected = observation_id(
            &self.source_id,
            &self.scope,
            self.logical_time,
            self.observed_unix_ms,
        )?;
        if self.observation_id != expected {
            return Err(DurableError::Validation(
                "clock observation identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }
}

/// SQLite-backed strictly increasing logical clock.
pub struct SqliteClock<W = SystemWallClock> {
    connection: Connection,
    source_id: String,
    wall_clock: W,
}

impl SqliteClock<SystemWallClock> {
    /// Open or create a production clock database.
    pub fn open(path: impl AsRef<Path>, source_id: impl Into<String>) -> DurableResult<Self> {
        Self::open_with_wall_clock(path, source_id, SystemWallClock)
    }
}

impl<W: WallClock> SqliteClock<W> {
    /// Open with an injected wall clock, primarily for deterministic testing.
    pub fn open_with_wall_clock(
        path: impl AsRef<Path>,
        source_id: impl Into<String>,
        wall_clock: W,
    ) -> DurableResult<Self> {
        let source_id = source_id.into();
        validate_identity("source", &source_id)?;
        let connection = Connection::open(path).map_err(sqlite_error)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS cymule_clock_scopes (
                    source_id TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    logical_time INTEGER NOT NULL,
                    observed_unix_ms INTEGER NOT NULL,
                    PRIMARY KEY(source_id, scope)
                ) STRICT;",
            )
            .map_err(sqlite_error)?;
        Ok(Self {
            connection,
            source_id,
            wall_clock,
        })
    }

    /// Allocate the next logical time for one scope.
    pub fn observe(&mut self, scope: &str) -> DurableResult<ClockObservation> {
        validate_identity("scope", scope)?;
        let observed_unix_ms = self.wall_clock.now_unix_ms()?;
        let observed = i64::try_from(observed_unix_ms)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let retained: Option<i64> = transaction
            .query_row(
                "SELECT logical_time FROM cymule_clock_scopes
                 WHERE source_id = ?1 AND scope = ?2",
                params![self.source_id, scope],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        let logical = match retained {
            Some(retained) => retained
                .checked_add(1)
                .ok_or_else(|| DurableError::Validation("logical clock overflow".to_owned()))?
                .max(observed),
            None => observed,
        };
        transaction
            .execute(
                "INSERT INTO cymule_clock_scopes(
                    source_id, scope, logical_time, observed_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(source_id, scope) DO UPDATE SET
                    logical_time = excluded.logical_time,
                    observed_unix_ms = excluded.observed_unix_ms",
                params![self.source_id, scope, logical, observed],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        let logical_time =
            u64::try_from(logical).map_err(|error| DurableError::Substrate(error.to_string()))?;
        let observation = ClockObservation {
            clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: observation_id(&self.source_id, scope, logical_time, observed_unix_ms)?,
            source_id: self.source_id.clone(),
            scope: scope.to_owned(),
            logical_time,
            observed_unix_ms,
        };
        observation.verify()?;
        Ok(observation)
    }
}

fn observation_id(
    source_id: &str,
    scope: &str,
    logical_time: u64,
    observed_unix_ms: u64,
) -> DurableResult<String> {
    cymule_core::content_id(
        CLOCK_OBSERVATION_VERSION,
        &(source_id, scope, logical_time, observed_unix_ms),
    )
    .map_err(DurableError::from)
}

fn validate_identity(kind: &str, value: &str) -> DurableResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(DurableError::Validation(format!(
            "clock {kind} must contain 1..=512 printable characters"
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
                expected: Some("sqlite-clock-writer-available".to_owned()),
                current: Some("sqlite-clock-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(error),
    }
}

fn sqlite_error(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
