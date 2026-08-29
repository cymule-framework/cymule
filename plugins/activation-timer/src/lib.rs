//! Durable logical timer source for Cymule.

use std::path::Path;
use std::time::Duration;

pub use cymule_clock_system::{SystemWallClock as SystemClock, WallClock as Clock};
use cymule_durable::{DurableError, DurableResult, ParkedWaitView, WaitDelivery, WaitSourceDriver};
use cymule_durable_protocol::WaitActivationSource;
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

const TIMER_SOURCE_SCAN_LIMIT: usize = 256;

/// Exact physical generation accepted by the durable timer source.
pub const TIMER_STORE_SCHEMA_VERSION: &str = "cymule.activation-timer-store/1";
/// Stable code returned before mutation for every unsupported timer generation.
pub const UNSUPPORTED_STORE_GENERATION_CODE: &str = "unsupported_store_generation";

const TIMER_SCHEMA: [(&str, &str, &str, &str); 3] = [
    (
        "table",
        "cymule_timer_meta",
        "cymule_timer_meta",
        "CREATE TABLE cymule_timer_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            schema_version TEXT NOT NULL
         ) STRICT",
    ),
    (
        "table",
        "cymule_timers",
        "cymule_timers",
        "CREATE TABLE cymule_timers (
            activation_id TEXT PRIMARY KEY NOT NULL,
            timer_id TEXT NOT NULL,
            due_unix_ms INTEGER NOT NULL,
            value_json BLOB NOT NULL,
            selected_wait_ids BLOB,
            acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
         ) STRICT",
    ),
    (
        "index",
        "cymule_timers_due",
        "cymule_timers",
        "CREATE INDEX cymule_timers_due
            ON cymule_timers(acknowledged, due_unix_ms, activation_id)",
    ),
];

/// SQLite-backed durable timer source.
pub struct SqliteTimerDriver<C = SystemClock> {
    connection: Connection,
    clock: C,
    scan_cursor: Option<TimerScanCursor>,
}

struct RetainedTimer {
    activation_id: String,
    timer_id: String,
    value: Vec<u8>,
    wait_ids: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TimerScanCursor {
    due_unix_ms: i64,
    activation_id: String,
}

struct PendingTimer {
    due_unix_ms: i64,
    activation_id: String,
    timer_id: String,
}

impl SqliteTimerDriver<SystemClock> {
    /// Open or create a file-backed timer source.
    ///
    /// # Errors
    ///
    /// Returns an error when the `SQLite` timer database cannot be opened or
    /// initialized.
    pub fn open(path: impl AsRef<Path>) -> DurableResult<Self> {
        Self::open_with_clock(path, SystemClock)
    }
}

impl<C: Clock> SqliteTimerDriver<C> {
    /// Open a file-backed timer source with an injected clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the `SQLite` timer database cannot be opened or
    /// initialized.
    pub fn open_with_clock(path: impl AsRef<Path>, clock: C) -> DurableResult<Self> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(contention)?;
        Self::initialize(connection, clock)
    }

    fn initialize(mut connection: Connection, clock: C) -> DurableResult<Self> {
        require_file_backed_timer_store(&connection)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(contention)?;
        initialize_or_require_timer_store(&mut connection)?;
        let observed_journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(contention)?;
        if !observed_journal_mode.eq_ignore_ascii_case("wal") {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(contention)?;
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(contention)?;
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(contention)?;
        let synchronous: i64 = connection
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .map_err(contention)?;
        if !journal_mode.eq_ignore_ascii_case("wal") || synchronous != 2 {
            return Err(DurableError::Substrate {
                code: "timer_store_durability_not_applied".to_owned(),
                message: "timer SQLite store did not retain WAL plus FULL synchronous durability"
                    .to_owned(),
            });
        }
        Ok(Self {
            connection,
            clock,
            scan_cursor: None,
        })
    }

    /// Schedule or exactly replay one logical timer delivery.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities or time/value encodings, a
    /// conflicting replay, `SQLite` contention, or another storage failure.
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
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let existing: Option<(String, i64, Vec<u8>, bool)> = transaction
            .query_row(
                "SELECT timer_id, due_unix_ms, value_json, acknowledged
                 FROM cymule_timers WHERE activation_id = ?1",
                [activation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(contention)?;
        if let Some((current_timer, current_due, current_value, _acknowledged)) = existing {
            if current_timer == timer_id && current_due == due && current_value == bytes {
                transaction.commit().map_err(contention)?;
                return Ok(());
            }
            return Err(DurableError::Conflict {
                expected: Some(activation_id.to_owned()),
                current: Some("timer-identity-reused".to_owned()),
            });
        }
        transaction
            .execute(
                "INSERT INTO cymule_timers(
                    activation_id, timer_id, due_unix_ms, value_json, acknowledged
                 ) VALUES (?1, ?2, ?3, ?4, 0)",
                params![activation_id, timer_id, due, bytes],
            )
            .map_err(contention)?;
        transaction.commit().map_err(contention)
    }

    fn pending_timer_page(&self, now: i64) -> DurableResult<Vec<PendingTimer>> {
        let (cursor_due, cursor_activation) =
            self.scan_cursor.as_ref().map_or((i64::MIN, ""), |cursor| {
                (cursor.due_unix_ms, cursor.activation_id.as_str())
            });
        let scan_limit = i64::try_from(TIMER_SOURCE_SCAN_LIMIT)
            .expect("fixed timer scan limit fits SQLite INTEGER");
        let mut statement = self
            .connection
            .prepare(
                "SELECT due_unix_ms, activation_id, timer_id
                 FROM cymule_timers
                 WHERE acknowledged = 0 AND due_unix_ms <= ?1
                   AND selected_wait_ids IS NULL
                   AND (due_unix_ms, activation_id) > (?2, ?3)
                 ORDER BY due_unix_ms, activation_id
                 LIMIT ?4",
            )
            .map_err(contention)?;
        statement
            .query_map(
                params![now, cursor_due, cursor_activation, scan_limit],
                |row| {
                    Ok(PendingTimer {
                        due_unix_ms: row.get(0)?,
                        activation_id: row.get(1)?,
                        timer_id: row.get(2)?,
                    })
                },
            )
            .map_err(contention)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(contention)
    }

    fn receive_fresh_due(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
        now: i64,
    ) -> DurableResult<Option<WaitDelivery>> {
        let pending = self.pending_timer_page(now)?;
        if pending.is_empty() {
            self.scan_cursor = None;
            return Ok(None);
        }
        let terminal_page = pending.len() < TIMER_SOURCE_SCAN_LIMIT;
        for timer in pending {
            let cursor = TimerScanCursor {
                due_unix_ms: timer.due_unix_ms,
                activation_id: timer.activation_id.clone(),
            };
            let source = WaitActivationSource::Timer {
                timer_id: timer.timer_id,
            };
            let selection = view.select(&source, max_targets)?;
            if selection.wait_ids.is_empty() {
                self.scan_cursor = Some(cursor);
                continue;
            }
            let target_bytes = cymule_core::canonical_bytes(&selection.wait_ids)?;
            self.connection
                .execute(
                    "UPDATE cymule_timers SET selected_wait_ids = ?1
                     WHERE activation_id = ?2 AND selected_wait_ids IS NULL
                       AND acknowledged = 0",
                    params![target_bytes, timer.activation_id],
                )
                .map_err(contention)?;
            let retained: Option<(Vec<u8>, Vec<u8>)> = self
                .connection
                .query_row(
                    "SELECT selected_wait_ids, value_json FROM cymule_timers
                     WHERE activation_id = ?1 AND acknowledged = 0",
                    [&timer.activation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(contention)?;
            let Some((retained, value)) = retained else {
                self.scan_cursor = Some(cursor);
                continue;
            };
            let wait_ids = cymule_core::decode_json(&retained)?;
            validate_retained_targets(&wait_ids, max_targets)?;
            let value = cymule_core::decode_json(&value)?;
            self.scan_cursor = Some(cursor);
            return Ok(Some(WaitDelivery {
                activation_id: timer.activation_id,
                source,
                wait_ids,
                value,
            }));
        }
        if terminal_page {
            self.scan_cursor = None;
        }
        Ok(None)
    }
}

fn require_file_backed_timer_store(connection: &Connection) -> DurableResult<()> {
    if connection.path().is_none_or(str::is_empty) {
        return Err(DurableError::Validation(
            "timer SQLite store must be file-backed".to_owned(),
        ));
    }
    Ok(())
}

impl<C: Clock> WaitSourceDriver for SqliteTimerDriver<C> {
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        let now =
            i64::try_from(self.clock.now_unix_ms()?).map_err(|error| DurableError::Substrate {
                code: "timer_clock_value_out_of_range".to_owned(),
                message: error.to_string(),
            })?;
        let retained = self
            .connection
            .query_row(
                "SELECT activation_id, timer_id, value_json, selected_wait_ids
                 FROM cymule_timers
                 WHERE acknowledged = 0 AND due_unix_ms <= ?1
                   AND selected_wait_ids IS NOT NULL
                 ORDER BY due_unix_ms, activation_id LIMIT 1",
                [now],
                |row| {
                    Ok(RetainedTimer {
                        activation_id: row.get(0)?,
                        timer_id: row.get(1)?,
                        value: row.get(2)?,
                        wait_ids: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(contention)?;
        if let Some(retained) = retained {
            let wait_ids = cymule_core::decode_json(&retained.wait_ids)?;
            validate_retained_targets(&wait_ids, max_targets)?;
            return Ok(Some(WaitDelivery {
                activation_id: retained.activation_id,
                source: WaitActivationSource::Timer {
                    timer_id: retained.timer_id,
                },
                wait_ids,
                value: cymule_core::decode_json(&retained.value)?,
            }));
        }
        self.receive_fresh_due(view, max_targets, now)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        validate_identity("activation", activation_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let existing: Option<(bool, Option<Vec<u8>>)> = transaction
            .query_row(
                "SELECT acknowledged, selected_wait_ids
                 FROM cymule_timers WHERE activation_id = ?1",
                [activation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(contention)?;
        let Some((acknowledged, selected)) = existing else {
            return Err(DurableError::NotFound(format!(
                "timer activation {activation_id} is missing"
            )));
        };
        if selected.is_none() {
            return Err(DurableError::Validation(format!(
                "timer activation {activation_id} has not selected durable targets"
            )));
        }
        if !acknowledged {
            transaction
                .execute(
                    "UPDATE cymule_timers SET acknowledged = 1
                     WHERE activation_id = ?1 AND acknowledged = 0
                       AND selected_wait_ids IS NOT NULL",
                    [activation_id],
                )
                .map_err(contention)?;
        }
        transaction.commit().map_err(contention)
    }
}

fn validate_retained_targets(
    wait_ids: &std::collections::BTreeSet<String>,
    max_targets: usize,
) -> DurableResult<()> {
    if max_targets == 0 || wait_ids.is_empty() || wait_ids.len() > max_targets {
        return Err(DurableError::Validation(format!(
            "retained timer delivery has {} targets outside requested bound {max_targets}",
            wait_ids.len()
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, identity: &str) -> DurableResult<()> {
    cymule_core::validate_identity(&format!("timer {kind} identity"), identity).map_err(Into::into)
}

fn initialize_or_require_timer_store(connection: &mut Connection) -> DurableResult<()> {
    if sqlite_schema_objects(connection)?.is_empty() {
        initialize_empty_timer_store(connection)?;
    } else {
        require_current_timer_schema(connection)?;
    }
    Ok(())
}

fn initialize_empty_timer_store(connection: &mut Connection) -> DurableResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(contention)?;
    if sqlite_schema_objects(&transaction)?.is_empty() {
        for (_, _, _, ddl) in TIMER_SCHEMA {
            transaction.execute_batch(ddl).map_err(contention)?;
        }
        transaction
            .execute(
                "INSERT INTO cymule_timer_meta(singleton, schema_version) VALUES (1, ?1)",
                [TIMER_STORE_SCHEMA_VERSION],
            )
            .map_err(contention)?;
        require_current_timer_schema(&transaction)?;
    } else {
        require_current_timer_schema(&transaction)?;
    }
    transaction.commit().map_err(contention)
}

fn require_current_timer_schema(connection: &Connection) -> DurableResult<()> {
    let observed = sqlite_schema_objects(connection)?;
    if observed.len() != TIMER_SCHEMA.len() {
        return Err(unsupported_generation(&format!(
            "timer SQLite object set is not the exact {TIMER_STORE_SCHEMA_VERSION} generation"
        )));
    }
    for (expected_kind, expected_name, expected_table, expected_ddl) in TIMER_SCHEMA {
        let expected_ddl = normalize_ddl(expected_ddl);
        let observed_ddl = observed
            .iter()
            .find(|(kind, name, table, _)| {
                kind == expected_kind && name == expected_name && table == expected_table
            })
            .map(|(_, _, _, ddl)| normalize_ddl(ddl));
        if observed_ddl.as_deref() != Some(expected_ddl.as_str()) {
            return Err(unsupported_generation(&format!(
                "timer SQLite {expected_kind} {expected_name} does not match the exact {TIMER_STORE_SCHEMA_VERSION} DDL"
            )));
        }
    }
    let mut statement = connection
        .prepare(
            "SELECT COALESCE(CAST(singleton AS TEXT), ''),
                    COALESCE(CAST(schema_version AS TEXT), '')
             FROM cymule_timer_meta ORDER BY singleton",
        )
        .map_err(contention)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(contention)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(contention)?;
    if rows.as_slice() != [("1".to_owned(), TIMER_STORE_SCHEMA_VERSION.to_owned())] {
        return Err(unsupported_generation(&format!(
            "timer SQLite schema authority is not the singleton {TIMER_STORE_SCHEMA_VERSION} generation"
        )));
    }
    Ok(())
}

fn sqlite_schema_objects(
    connection: &Connection,
) -> DurableResult<Vec<(String, String, String, String)>> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_master
             WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*'
             ORDER BY type, name",
        )
        .map_err(contention)?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(contention)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(contention)
}

fn normalize_ddl(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_lowercase)
        .collect()
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
    DurableError::Substrate {
        code: "timer_sqlite_failed".to_owned(),
        message: error.to_string(),
    }
}

fn unsupported_generation(detail: &str) -> DurableError {
    DurableError::Validation(format!("{UNSUPPORTED_STORE_GENERATION_CODE}: {detail}"))
}
