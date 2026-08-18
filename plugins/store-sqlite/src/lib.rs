//! `SQLite` realization of Cymule's complete-state durable CAS.

use std::path::Path;
use std::time::Duration;

use cymule_durable::{
    DurableError, DurableResult, DurableState, DurableStore, StoreCommit, StoredState,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, TransactionBehavior, params};

const SCHEMA: &str = "cymule.sqlite-store/1";

/// One SQLite-backed durable domain.
pub struct SqliteStore {
    connection: Connection,
    domain: String,
}

impl SqliteStore {
    /// Open or create a file-backed domain.
    pub fn open(path: impl AsRef<Path>, domain: impl Into<String>) -> DurableResult<Self> {
        let connection = Connection::open(path).map_err(sqlite_error)?;
        Self::initialize(connection, domain.into(), true)
    }

    /// Create an isolated in-memory domain, primarily for embedding and tests.
    pub fn in_memory(domain: impl Into<String>) -> DurableResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error)?;
        Self::initialize(connection, domain.into(), false)
    }

    fn initialize(
        connection: Connection,
        domain: String,
        file_backed: bool,
    ) -> DurableResult<Self> {
        if domain.is_empty() || domain.len() > 512 || domain.chars().any(char::is_control) {
            return Err(DurableError::Validation(
                "SQLite durable domain must contain 1..=512 non-control characters".to_owned(),
            ));
        }
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
                "CREATE TABLE IF NOT EXISTS cymule_state (
                    domain TEXT PRIMARY KEY NOT NULL,
                    schema_version TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    state_json BLOB NOT NULL
                ) STRICT;",
            )
            .map_err(sqlite_error)?;
        Ok(Self { connection, domain })
    }

    fn read(&self) -> DurableResult<Option<StoredState>> {
        let row: Option<(String, String, Vec<u8>)> = self
            .connection
            .query_row(
                "SELECT schema_version, revision, state_json
                 FROM cymule_state WHERE domain = ?1",
                [&self.domain],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sqlite_error)?;
        let Some((schema, revision, bytes)) = row else {
            return Ok(None);
        };
        if schema != SCHEMA {
            return Err(DurableError::Validation(format!(
                "unsupported SQLite store schema {schema}"
            )));
        }
        let state: DurableState = serde_json::from_slice(&bytes)?;
        let stored = StoredState { revision, state };
        stored.verify()?;
        Ok(Some(stored))
    }
}

impl DurableStore for SqliteStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.read()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        next.validate()?;
        let revision = next.revision()?;
        let bytes = cymule_core::canonical_bytes(next)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| contention(error, expected_revision))?;
        let current: Option<String> = transaction
            .query_row(
                "SELECT revision FROM cymule_state WHERE domain = ?1",
                [&self.domain],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)?;
        if current.as_deref() != expected_revision {
            return Err(DurableError::Conflict {
                expected: expected_revision.map(str::to_owned),
                current,
            });
        }
        transaction
            .execute(
                "INSERT INTO cymule_state(domain, schema_version, revision, state_json)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(domain) DO UPDATE SET
                   schema_version = excluded.schema_version,
                   revision = excluded.revision,
                   state_json = excluded.state_json",
                params![self.domain, SCHEMA, revision, bytes],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(StoreCommit { revision })
    }
}

fn contention(error: rusqlite::Error, expected: Option<&str>) -> DurableError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: expected.map(str::to_owned),
                current: Some("sqlite-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(error),
    }
}

fn sqlite_error(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
