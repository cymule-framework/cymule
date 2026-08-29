//! Restart-monotonic logical clock observations for Cymule adapters.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_core::MAX_EXACT_INTEGER;
use cymule_durable::{
    ClockObservationAuthority, DurableError, DurableResult, ExecutionClockAuthority,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, ClockObservationRef, clock_observation_id,
};
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params};

const SQLITE_SCHEMA_VERSION: &str = "cymule.clock-system/2";
const CLOCK_SCHEMA: [(&str, &str); 3] = [
    (
        "cymule_clock_meta",
        "CREATE TABLE cymule_clock_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            schema_version TEXT NOT NULL
         ) STRICT",
    ),
    (
        "cymule_clock_scopes_v2",
        "CREATE TABLE cymule_clock_scopes_v2 (
            source_id TEXT NOT NULL,
            source_generation TEXT NOT NULL,
            scope TEXT NOT NULL,
            logical_time INTEGER NOT NULL CHECK(logical_time >= 0 AND logical_time <= 9007199254740991),
            observed_unix_ms INTEGER NOT NULL CHECK(observed_unix_ms >= 0 AND observed_unix_ms <= 9007199254740991),
            PRIMARY KEY(source_id, source_generation, scope)
         ) STRICT",
    ),
    (
        "cymule_clock_observations_v2",
        "CREATE TABLE cymule_clock_observations_v2 (
            observation_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            source_generation TEXT NOT NULL,
            scope TEXT NOT NULL,
            logical_time INTEGER NOT NULL CHECK(logical_time >= 0 AND logical_time <= 9007199254740991),
            observed_unix_ms INTEGER NOT NULL CHECK(observed_unix_ms >= 0 AND observed_unix_ms <= 9007199254740991),
            UNIQUE(source_id, source_generation, scope, logical_time)
         ) STRICT",
    ),
];

/// One wall-clock source used below the logical clock adapter.
pub trait WallClock {
    /// Return the current observed Unix time in milliseconds.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying time authority cannot provide a
    /// valid observation.
    fn now_unix_ms(&self) -> DurableResult<u64>;
}

/// Operating-system wall-clock source.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| DurableError::Substrate {
                code: "clock_before_unix_epoch".to_owned(),
                message: error.to_string(),
            })?;
        u64::try_from(duration.as_millis()).map_err(|error| DurableError::Substrate {
            code: "clock_value_out_of_range".to_owned(),
            message: error.to_string(),
        })
    }
}

/// SQLite-backed strictly increasing logical clock.
pub struct SqliteClock<W = SystemWallClock> {
    connection: Connection,
    source_id: String,
    source_generation: String,
    wall_clock: W,
}

impl SqliteClock<SystemWallClock> {
    /// Open or create a file-backed production clock database.
    ///
    /// # Errors
    ///
    /// Returns an error for a process-local backend, invalid authority
    /// identities, unsupported schema, immediate `SQLite` contention, or
    /// another storage failure.
    pub fn open(
        path: impl AsRef<Path>,
        source_id: impl Into<String>,
        source_generation: impl Into<String>,
    ) -> DurableResult<Self> {
        Self::open_with_wall_clock(path, source_id, source_generation, SystemWallClock)
    }
}

impl<W: WallClock> SqliteClock<W> {
    /// Open a file-backed database with an injected wall clock, primarily for
    /// deterministic testing.
    ///
    /// # Errors
    ///
    /// Returns an error for a process-local backend, invalid authority
    /// identities, unsupported schema, immediate `SQLite` contention, or
    /// another storage failure.
    pub fn open_with_wall_clock(
        path: impl AsRef<Path>,
        source_id: impl Into<String>,
        source_generation: impl Into<String>,
        wall_clock: W,
    ) -> DurableResult<Self> {
        let source_id = source_id.into();
        let source_generation = source_generation.into();
        validate_identity("source", &source_id)?;
        validate_generation(&source_generation)?;
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(sqlite_error)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        require_file_backed_clock_authority(&connection)?;
        initialize_or_require(&mut connection)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        Ok(Self {
            connection,
            source_id,
            source_generation,
            wall_clock,
        })
    }

    /// Allocate the next logical time for one scope.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid scope, a failed wall-clock observation,
    /// logical-time overflow, immediate `SQLite` contention, or storage failure.
    pub fn observe(&mut self, scope: &str) -> DurableResult<ClockObservation> {
        validate_identity("scope", scope)?;
        let observed_unix_ms = self.wall_clock.now_unix_ms()?;
        if observed_unix_ms > MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "Clock observation exceeds the exact cross-language integer range".to_owned(),
            ));
        }
        let observed = i64::try_from(observed_unix_ms)
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let logical = next_logical_time(
            &transaction,
            &self.source_id,
            &self.source_generation,
            scope,
            observed,
        )?;
        transaction
            .execute(
                "INSERT INTO cymule_clock_scopes_v2(
                    source_id, source_generation, scope, logical_time, observed_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(source_id, source_generation, scope) DO UPDATE SET
                    logical_time = excluded.logical_time,
                    observed_unix_ms = excluded.observed_unix_ms",
                params![
                    self.source_id,
                    self.source_generation,
                    scope,
                    logical,
                    observed
                ],
            )
            .map_err(sqlite_error)?;
        let logical_time = u64::try_from(logical).map_err(|error| {
            clock_integrity("clock_scope_head_logical_time_invalid", error.to_string())
        })?;
        if logical_time > MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "Clock observation exceeds the exact cross-language integer range".to_owned(),
            ));
        }
        let observation_id = clock_observation_id(
            &self.source_id,
            &self.source_generation,
            scope,
            logical_time,
            observed_unix_ms,
        )?;
        transaction
            .execute(
                "INSERT INTO cymule_clock_observations_v2(
                    observation_id, source_id, source_generation, scope,
                    logical_time, observed_unix_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    observation_id,
                    self.source_id,
                    self.source_generation,
                    scope,
                    logical,
                    observed
                ],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        let observation = ClockObservation {
            clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id,
            source_id: self.source_id.clone(),
            source_generation: self.source_generation.clone(),
            scope: scope.to_owned(),
            logical_time,
            observed_unix_ms,
        };
        observation.verify()?;
        Ok(observation)
    }
}

fn next_logical_time(
    connection: &Connection,
    source_id: &str,
    source_generation: &str,
    scope: &str,
    observed: i64,
) -> DurableResult<i64> {
    let Some(retained) =
        load_retained_allocation_head(connection, source_id, source_generation, scope)?
    else {
        return Ok(observed);
    };
    let next = retained
        .logical_time
        .checked_add(1)
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            clock_integrity(
                "clock_logical_time_exhausted",
                "retained logical Clock head has exhausted the exact range",
            )
        })?;
    Ok(i64::try_from(next)
        .map_err(|error| {
            clock_integrity("clock_scope_head_logical_time_invalid", error.to_string())
        })?
        .max(observed))
}

fn load_retained_allocation_head(
    connection: &Connection,
    source_id: &str,
    source_generation: &str,
    scope: &str,
) -> DurableResult<Option<ClockObservation>> {
    let retained = connection
        .query_row(
            "SELECT logical_time, observed_unix_ms FROM cymule_clock_scopes_v2
             WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3",
            params![source_id, source_generation, scope],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?;
    let Some(retained) = retained else {
        return Ok(None);
    };
    let logical_time = retained_exact_integer(
        retained.0,
        "clock_scope_head_logical_time_invalid",
        "retained Clock scope-head logical time",
    )?;
    let observed_unix_ms = retained_exact_integer(
        retained.1,
        "clock_scope_head_wall_time_invalid",
        "retained Clock scope-head wall time",
    )?;
    let observation_id = clock_observation_id(
        source_id,
        source_generation,
        scope,
        logical_time,
        observed_unix_ms,
    )
    .map_err(|error| clock_integrity("clock_scope_head_invalid", error.to_string()))?;
    let reference = ClockObservationRef {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id,
        source_id: source_id.to_owned(),
        source_generation: source_generation.to_owned(),
        scope: scope.to_owned(),
    };
    let observation = match load_issued_observation(connection, &reference) {
        Ok(observation) => observation,
        Err(DurableError::NotFound(_)) => {
            return Err(DurableError::Integrity {
                code: "clock_scope_head_receipt_missing".to_owned(),
                message: format!(
                    "Clock scope {scope} has a retained head without its exact immutable receipt"
                ),
            });
        }
        Err(error) => return Err(error),
    };
    if observation.logical_time != logical_time || observation.observed_unix_ms != observed_unix_ms
    {
        return Err(DurableError::Integrity {
            code: "clock_scope_head_identity_mismatch".to_owned(),
            message: "Clock scope head does not match its exact immutable receipt".to_owned(),
        });
    }
    Ok(Some(observation))
}

fn require_file_backed_clock_authority(connection: &Connection) -> DurableResult<()> {
    let mut statement = connection
        .prepare("PRAGMA database_list")
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let mut main_filename = None;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let name = row.get::<_, String>(1).map_err(sqlite_error)?;
        if name == "main" {
            main_filename = Some(row.get::<_, String>(2).map_err(sqlite_error)?);
            break;
        }
    }
    let is_file_backed = main_filename
        .as_deref()
        .filter(|filename| !filename.is_empty())
        .is_some_and(|filename| Path::new(filename).is_file());
    if !is_file_backed {
        return Err(DurableError::Validation(
            "Clock SQLite authority must be file-backed".to_owned(),
        ));
    }
    Ok(())
}

fn initialize_or_require(connection: &mut Connection) -> DurableResult<()> {
    reject_non_clock_authority(connection)?;
    if clock_schema_objects(connection)?.is_empty() {
        initialize_empty(connection, &CLOCK_SCHEMA)?;
    } else {
        require_current_schema(connection)?;
    }
    Ok(())
}

fn initialize_empty(connection: &mut Connection, schema: &[(&str, &str)]) -> DurableResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(contention)?;
    reject_non_clock_authority(&transaction)?;
    if clock_schema_objects(&transaction)?.is_empty() {
        for (_, ddl) in schema {
            transaction.execute_batch(ddl).map_err(sqlite_error)?;
        }
        transaction
            .execute(
                "INSERT INTO cymule_clock_meta(singleton, schema_version) VALUES (1, ?1)",
                [SQLITE_SCHEMA_VERSION],
            )
            .map_err(sqlite_error)?;
        require_schema(&transaction, schema)?;
    } else {
        require_current_schema(&transaction)?;
    }
    transaction.commit().map_err(sqlite_error)
}

fn reject_non_clock_authority(connection: &Connection) -> DurableResult<()> {
    let foreign_authority: Option<String> = connection
        .query_row(
            "SELECT CASE
                    WHEN name GLOB 'cymule_*' THEN name
                    ELSE tbl_name
                 END
             FROM sqlite_master
             WHERE (name GLOB 'cymule_*' OR tbl_name GLOB 'cymule_*')
               AND NOT (name GLOB 'cymule_clock_*' OR tbl_name GLOB 'cymule_clock_*')
             ORDER BY type, name
             LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if let Some(name) = foreign_authority {
        return Err(DurableError::Validation(format!(
            "Clock database already belongs to non-Clock Cymule authority {name}"
        )));
    }
    Ok(())
}

fn require_current_schema(connection: &Connection) -> DurableResult<()> {
    require_schema(connection, &CLOCK_SCHEMA)
}

fn require_schema(connection: &Connection, schema: &[(&str, &str)]) -> DurableResult<()> {
    let observed = clock_schema_objects(connection)?;
    if observed.len() != schema.len() {
        return Err(DurableError::Validation(format!(
            "Clock SQLite object set is not the exact {SQLITE_SCHEMA_VERSION} generation"
        )));
    }
    for (name, expected_ddl) in schema {
        let expected_ddl = normalize_ddl(expected_ddl);
        let observed_ddl = observed
            .iter()
            .find(|(kind, observed_name, _, _)| kind == "table" && observed_name == name)
            .map(|(_, _, _, ddl)| normalize_ddl(ddl));
        if observed_ddl.as_deref() != Some(expected_ddl.as_str()) {
            return Err(DurableError::Validation(format!(
                "Clock SQLite table {name} does not match the exact {SQLITE_SCHEMA_VERSION} DDL"
            )));
        }
    }
    let version: Option<String> = connection
        .query_row(
            "SELECT schema_version FROM cymule_clock_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if version.as_deref() != Some(SQLITE_SCHEMA_VERSION) {
        return Err(DurableError::Validation(format!(
            "Clock SQLite schema generation {version:?} is not {SQLITE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn clock_schema_objects(
    connection: &Connection,
) -> DurableResult<Vec<(String, String, String, String)>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master
             WHERE name GLOB 'cymule_clock_*' OR tbl_name GLOB 'cymule_clock_*'
             ORDER BY type, name",
        )
        .map_err(sqlite_error)?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(sqlite_error)?
        .filter_map(|row| match row {
            Ok((kind, _, _, ddl)) if kind == "index" && ddl.is_empty() => None,
            row => Some(row),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn normalize_ddl(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
}

impl<W: WallClock> ClockObservationAuthority for SqliteClock<W> {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        verify_admitted_reference(reference, &self.source_id, &self.source_generation)?;
        load_issued_observation(&self.connection, reference)
    }
}

impl<W: WallClock> ExecutionClockAuthority for SqliteClock<W> {
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        verify_admitted_reference(reference, &self.source_id, &self.source_generation)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let resolved = load_issued_observation(&transaction, reference)?;
        let head_id = current_head_id(&transaction, &resolved)?;
        if head_id != reference.observation_id {
            return Err(DurableError::Conflict {
                expected: Some(reference.observation_id.clone()),
                current: Some(head_id),
            });
        }
        commit(&resolved)?;
        // The transaction is an exclusion guard and has no Clock writes. Its
        // rollback-on-drop cannot turn a completed external Store CAS into a
        // later Clock error or ambiguous response.
        drop(transaction);
        Ok(())
    }
}

fn verify_admitted_reference(
    reference: &ClockObservationRef,
    source_id: &str,
    source_generation: &str,
) -> DurableResult<()> {
    reference.verify()?;
    if reference.source_id != source_id || reference.source_generation != source_generation {
        return Err(DurableError::Validation(
            "Clock observation reference does not match the admitted source generation".to_owned(),
        ));
    }
    Ok(())
}

fn load_issued_observation(
    connection: &Connection,
    reference: &ClockObservationRef,
) -> DurableResult<ClockObservation> {
    let observation = connection
        .query_row(
            "SELECT source_id, source_generation, scope, logical_time, observed_unix_ms
             FROM cymule_clock_observations_v2 WHERE observation_id = ?1",
            params![reference.observation_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "Clock observation {} was not issued by the admitted source",
                reference.observation_id
            ))
        })?;
    let resolved = ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id: reference.observation_id.clone(),
        source_id: observation.0,
        source_generation: observation.1,
        scope: observation.2,
        logical_time: retained_exact_integer(
            observation.3,
            "clock_observation_logical_time_invalid",
            "retained Clock observation logical time",
        )?,
        observed_unix_ms: retained_exact_integer(
            observation.4,
            "clock_observation_wall_time_invalid",
            "retained Clock observation wall time",
        )?,
    };
    resolved
        .verify()
        .map_err(|error| clock_integrity("clock_observation_receipt_invalid", error.to_string()))?;
    if resolved.reference() != *reference {
        return Err(clock_integrity(
            "clock_observation_reference_mismatch",
            "Clock observation receipt does not match its requested reference",
        ));
    }
    Ok(resolved)
}

fn current_head_id(
    connection: &Connection,
    observation: &ClockObservation,
) -> DurableResult<String> {
    let head = connection
        .query_row(
            "SELECT logical_time, observed_unix_ms FROM cymule_clock_scopes_v2
                 WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3",
            params![
                observation.source_id,
                observation.source_generation,
                observation.scope
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| DurableError::Integrity {
            code: "clock_scope_head_missing".to_owned(),
            message: format!(
                "Clock observation {} has no retained scope head",
                observation.observation_id
            ),
        })?;
    let head_receipt_id = connection
        .query_row(
            "SELECT observation_id FROM cymule_clock_observations_v2
                 WHERE source_id = ?1 AND source_generation = ?2 AND scope = ?3
                   AND logical_time = ?4 AND observed_unix_ms = ?5",
            params![
                observation.source_id,
                observation.source_generation,
                observation.scope,
                head.0,
                head.1
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| DurableError::Integrity {
            code: "clock_scope_head_receipt_missing".to_owned(),
            message: format!(
                "Clock observation {} has a scope head without an immutable receipt",
                observation.observation_id
            ),
        })?;
    let head_logical_time = retained_exact_integer(
        head.0,
        "clock_scope_head_logical_time_invalid",
        "retained Clock scope-head logical time",
    )?;
    let head_observed_unix_ms = retained_exact_integer(
        head.1,
        "clock_scope_head_wall_time_invalid",
        "retained Clock scope-head wall time",
    )?;
    let head_id = clock_observation_id(
        &observation.source_id,
        &observation.source_generation,
        &observation.scope,
        head_logical_time,
        head_observed_unix_ms,
    )
    .map_err(|error| clock_integrity("clock_scope_head_invalid", error.to_string()))?;
    if head_id != head_receipt_id {
        return Err(DurableError::Integrity {
            code: "clock_scope_head_identity_mismatch".to_owned(),
            message: "Clock scope head receipt identity does not match its retained content"
                .to_owned(),
        });
    }
    Ok(head_id)
}

fn validate_identity(kind: &str, value: &str) -> DurableResult<()> {
    if value.is_empty() || value.chars().count() > 512 || value.chars().any(char::is_control) {
        return Err(DurableError::Validation(format!(
            "clock {kind} must contain 1..=512 printable characters"
        )));
    }
    Ok(())
}

fn validate_generation(value: &str) -> DurableResult<()> {
    let valid = value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    });
    if !valid {
        return Err(DurableError::Validation(
            "Clock source generation must be a lowercase sha256 identity".to_owned(),
        ));
    }
    Ok(())
}

fn contention(error: rusqlite::Error) -> DurableError {
    match error {
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
        error => DurableError::Substrate {
            code: "clock_sqlite_failed".to_owned(),
            message: error.to_string(),
        },
    }
}

fn sqlite_error(error: rusqlite::Error) -> DurableError {
    contention(error)
}

fn clock_integrity(code: &'static str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn retained_exact_integer(
    value: i64,
    code: &'static str,
    field: &'static str,
) -> DurableResult<u64> {
    let value = u64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_EXACT_INTEGER)
        .ok_or_else(|| {
            clock_integrity(
                code,
                format!("{field} is outside the exact cross-language integer range"),
            )
        })?;
    Ok(value)
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn failed_empty_initialization_rolls_back_before_exact_reopen() {
        let mut connection = Connection::open_in_memory().expect("test database opens");
        let broken = [
            CLOCK_SCHEMA[0],
            ("cymule_clock_scopes_v2", "CREATE TABL broken"),
        ];
        assert!(initialize_empty(&mut connection, &broken).is_err());
        assert!(
            clock_schema_objects(&connection)
                .expect("rolled-back schema reads")
                .is_empty()
        );
        initialize_empty(&mut connection, &CLOCK_SCHEMA).expect("exact retry initializes");
        require_current_schema(&connection).expect("exact generation reads back");
    }
}
