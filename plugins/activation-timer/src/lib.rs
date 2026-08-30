//! Durable logical timer source for Cymule.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

pub use cymule_clock_system::{SystemWallClock as SystemClock, WallClock as Clock};
use cymule_durable::{
    DurableError, DurableResult, ParkedWaitView, WaitDelivery, WaitSourceDelivery, WaitSourceDriver,
};
use cymule_durable_protocol::{MAX_WAIT_DELIVERY_TARGETS, WaitActivationSource};
use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

const TIMER_SOURCE_SCAN_LIMIT: usize = 256;
const MAX_SELECTED_WAIT_IDS_BYTES: usize = 75;
const MAX_IDENTITY_SCALARS: usize = 512;
const MAX_IDENTITY_UTF8_BYTES: usize = MAX_IDENTITY_SCALARS * 4;
const CANONICAL_DIGEST_BYTES: usize = 64;
const FRESH_TIMER_SCAN_SQL: &str = "SELECT length(CAST(activation_id AS BLOB)),
                                            length(activation_id),
                                            substr(activation_id, 1, 513),
                                            due_unix_ms,
                                            length(value_json)
     FROM cymule_timers
     WHERE acknowledged = 0 AND due_unix_ms <= ?1
       AND selected_wait_ids IS NULL
       AND (due_unix_ms, activation_id) > (?2, ?3)
     ORDER BY due_unix_ms, activation_id
     LIMIT ?4";
const TIMER_POINT_READ_SQL: &str = "SELECT length(CAST(activation_id AS BLOB)),
                                           length(activation_id), substr(activation_id, 1, 513),
                                           length(CAST(timer_id AS BLOB)),
                                           length(timer_id), substr(timer_id, 1, 513),
                                           due_unix_ms,
                                           length(CAST(schedule_digest AS BLOB)),
                                           length(schedule_digest), substr(schedule_digest, 1, 65),
                                           acknowledged,
                                           length(value_json),
                                           CASE WHEN length(value_json) <= ?2
                                                THEN value_json ELSE NULL END,
                                           length(selected_wait_ids),
                                           CASE WHEN selected_wait_ids IS NULL THEN NULL
                                                WHEN length(selected_wait_ids) <= ?3
                                                THEN selected_wait_ids ELSE NULL END
                                    FROM cymule_timers WHERE activation_id = ?1";
const RETAINED_TIMER_READ_SQL: &str = "SELECT length(CAST(activation_id AS BLOB)),
                                              length(activation_id), substr(activation_id, 1, 513),
                                              length(CAST(timer_id AS BLOB)),
                                              length(timer_id), substr(timer_id, 1, 513),
                                              due_unix_ms,
                                              length(CAST(schedule_digest AS BLOB)),
                                              length(schedule_digest), substr(schedule_digest, 1, 65),
                                              acknowledged,
                                              length(value_json),
                                              CASE WHEN length(value_json) <= ?1
                                                   THEN value_json ELSE NULL END,
                                              length(selected_wait_ids),
                                              CASE WHEN selected_wait_ids IS NULL THEN NULL
                                                   WHEN length(selected_wait_ids) <= ?2
                                                   THEN selected_wait_ids ELSE NULL END
                                       FROM cymule_timers
                                       WHERE acknowledged = 0 AND due_unix_ms <= ?3
                                         AND selected_wait_ids IS NOT NULL
                                       ORDER BY due_unix_ms, activation_id LIMIT 1";

/// Exact physical generation accepted by the durable timer source.
pub const TIMER_STORE_SCHEMA_VERSION: &str = "cymule.activation-timer-store/2";
/// Stable code returned before mutation for every unsupported timer generation.
pub const UNSUPPORTED_STORE_GENERATION_CODE: &str = "unsupported_store_generation";

const TIMER_SCHEMA: [(&str, &str, &str, &str); 4] = [
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
            schedule_digest TEXT NOT NULL,
            selected_wait_ids BLOB,
            acknowledged INTEGER NOT NULL DEFAULT 0 CHECK(acknowledged IN (0, 1))
         ) STRICT",
    ),
    (
        "index",
        "cymule_timers_due_fresh",
        "cymule_timers",
        "CREATE INDEX cymule_timers_due_fresh
            ON cymule_timers(acknowledged, due_unix_ms, activation_id)
            WHERE selected_wait_ids IS NULL",
    ),
    (
        "index",
        "cymule_timers_due_retained",
        "cymule_timers",
        "CREATE INDEX cymule_timers_due_retained
            ON cymule_timers(acknowledged, due_unix_ms, activation_id)
            WHERE selected_wait_ids IS NOT NULL",
    ),
];

/// SQLite-backed durable timer source.
pub struct SqliteTimerDriver<C = SystemClock> {
    connection: Connection,
    clock: C,
    scan_cursor: Option<TimerScanCursor>,
}

struct StoredTimer {
    activation_id_bytes: i64,
    activation_id_scalars: i64,
    activation_id: String,
    timer_id_bytes: i64,
    timer_id_scalars: i64,
    timer_id: String,
    due_unix_ms: i64,
    schedule_digest_bytes: i64,
    schedule_digest_scalars: i64,
    schedule_digest: String,
    acknowledged: i64,
    value_bytes: i64,
    value: Option<Vec<u8>>,
    wait_ids_bytes: Option<i64>,
    wait_ids: Option<Vec<u8>>,
}

struct VerifiedTimer {
    activation_id: String,
    timer_id: String,
    due_unix_ms: u64,
    value: Value,
    wait_ids: Option<BTreeSet<String>>,
    acknowledged: bool,
}

struct TimerScanCandidate {
    activation_bytes: i64,
    activation_scalar_count: i64,
    activation_id: String,
    due_unix_ms: i64,
    value_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TimerScanCursor {
    due_unix_ms: i64,
    activation_id: String,
}

#[derive(Serialize)]
struct TimerSchedule<'a> {
    activation_id: &'a str,
    timer_id: &'a str,
    due_unix_ms: u64,
    value: &'a Value,
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
    /// canonical value above Core's artifact bound, a conflicting replay,
    /// `SQLite` contention, or another storage failure.
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
        if bytes.len() > cymule_core::MAX_ARTIFACT_BYTES {
            return Err(DurableError::Validation(format!(
                "timer value has {} canonical bytes; maximum is {}",
                bytes.len(),
                cymule_core::MAX_ARTIFACT_BYTES
            )));
        }
        let digest = timer_schedule_digest(activation_id, timer_id, due_unix_ms, value)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let existing: Option<StoredTimer> = transaction
            .query_row(
                TIMER_POINT_READ_SQL,
                params![
                    activation_id,
                    timer_value_byte_limit(),
                    selected_wait_ids_byte_limit()
                ],
                stored_timer_from_row,
            )
            .optional()
            .map_err(contention)?;
        if let Some(existing) = existing {
            let existing = verify_stored_timer(existing)?;
            if existing.activation_id == activation_id
                && existing.timer_id == timer_id
                && existing.due_unix_ms == due_unix_ms
                && existing.value == *value
            {
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
                    activation_id, timer_id, due_unix_ms, value_json,
                    schedule_digest, selected_wait_ids, acknowledged
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, 0)",
                params![activation_id, timer_id, due, bytes, digest],
            )
            .map_err(contention)?;
        transaction.commit().map_err(contention)
    }

    fn pending_timer_page(&self, now: i64) -> DurableResult<Vec<TimerScanCandidate>> {
        let (cursor_due, cursor_activation) =
            self.scan_cursor.as_ref().map_or((i64::MIN, ""), |cursor| {
                (cursor.due_unix_ms, cursor.activation_id.as_str())
            });
        let scan_limit = i64::try_from(TIMER_SOURCE_SCAN_LIMIT)
            .expect("fixed timer scan limit fits SQLite INTEGER");
        let mut statement = self
            .connection
            .prepare(FRESH_TIMER_SCAN_SQL)
            .map_err(contention)?;
        statement
            .query_map(
                params![now, cursor_due, cursor_activation, scan_limit],
                |row| {
                    Ok(TimerScanCandidate {
                        activation_bytes: row.get(0)?,
                        activation_scalar_count: row.get(1)?,
                        activation_id: row.get(2)?,
                        due_unix_ms: row.get(3)?,
                        value_bytes: row.get(4)?,
                    })
                },
            )
            .map_err(contention)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(contention)
    }

    fn load_timer(&self, activation_id: &str) -> DurableResult<VerifiedTimer> {
        let stored = self
            .connection
            .query_row(
                TIMER_POINT_READ_SQL,
                params![
                    activation_id,
                    timer_value_byte_limit(),
                    selected_wait_ids_byte_limit()
                ],
                stored_timer_from_row,
            )
            .optional()
            .map_err(contention)?
            .ok_or_else(|| {
                timer_row_integrity(
                    "timer_scan_candidate_missing",
                    format!("timer scan candidate {activation_id} disappeared"),
                )
            })?;
        verify_stored_timer(stored)
    }

    fn receive_fresh_due(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
        now: i64,
    ) -> DurableResult<Option<WaitSourceDelivery>> {
        let pending = self.pending_timer_page(now)?;
        if pending.is_empty() {
            self.scan_cursor = None;
            return Ok(None);
        }
        let terminal_page = pending.len() < TIMER_SOURCE_SCAN_LIMIT;
        let now_unix_ms =
            u64::try_from(now).expect("wall Clock u64 converted to nonnegative SQLite INTEGER");
        for candidate in pending {
            let activation_id = require_gated_timer_text(
                "activation_id",
                candidate.activation_bytes,
                candidate.activation_scalar_count,
                candidate.activation_id,
                MAX_IDENTITY_UTF8_BYTES,
                MAX_IDENTITY_SCALARS,
            )?;
            validate_identity("activation", &activation_id).map_err(|error| {
                timer_row_integrity("timer_schedule_activation_invalid", error.to_string())
            })?;
            let value_bytes = usize::try_from(candidate.value_bytes).map_err(|error| {
                timer_row_integrity("timer_schedule_value_size_invalid", error.to_string())
            })?;
            if value_bytes > cymule_core::MAX_ARTIFACT_BYTES {
                return Err(timer_row_integrity(
                    "timer_schedule_value_too_large",
                    format!(
                        "timer activation {} retains {value_bytes} value bytes; maximum is {}",
                        activation_id,
                        cymule_core::MAX_ARTIFACT_BYTES
                    ),
                ));
            }
            let cursor = TimerScanCursor {
                due_unix_ms: candidate.due_unix_ms,
                activation_id: activation_id.clone(),
            };
            let timer = self.load_timer(&activation_id)?;
            let verified_due = i64::try_from(timer.due_unix_ms).map_err(|error| {
                timer_row_integrity("timer_schedule_due_invalid", error.to_string())
            })?;
            if timer.activation_id != activation_id || verified_due != candidate.due_unix_ms {
                return Err(timer_row_integrity(
                    "timer_scan_candidate_changed",
                    "timer scan metadata changed before exact-row verification",
                ));
            }
            if timer.acknowledged {
                self.scan_cursor = Some(cursor);
                continue;
            }
            if let Some(wait_ids) = timer.wait_ids.clone() {
                self.scan_cursor = Some(cursor);
                return Ok(Some(WaitSourceDelivery::Retained(WaitDelivery {
                    activation_id: timer.activation_id,
                    source: WaitActivationSource::Timer {
                        timer_id: timer.timer_id,
                    },
                    wait_ids,
                    value: timer.value,
                })));
            }
            if timer.due_unix_ms > now_unix_ms {
                return Err(timer_row_integrity(
                    "timer_fresh_row_mismatch",
                    "fresh timer query returned a row outside its exact due predicate",
                ));
            }
            let source = WaitActivationSource::Timer {
                timer_id: timer.timer_id.clone(),
            };
            let selection = view.select(&source, max_targets)?;
            if selection.wait_ids.is_empty() {
                self.scan_cursor = Some(cursor);
                continue;
            }
            validate_new_targets(&selection.wait_ids, max_targets)?;
            let delivery = self.retain_fresh_selection(&timer, source, &selection.wait_ids)?;
            self.scan_cursor = Some(cursor);
            if let Some(delivery) = delivery {
                return Ok(Some(delivery));
            }
        }
        if terminal_page {
            self.scan_cursor = None;
        }
        Ok(None)
    }

    fn retain_fresh_selection(
        &self,
        timer: &VerifiedTimer,
        source: WaitActivationSource,
        selected_wait_ids: &BTreeSet<String>,
    ) -> DurableResult<Option<WaitSourceDelivery>> {
        let target_bytes = cymule_core::canonical_bytes(selected_wait_ids)?;
        let selected_now = self
            .connection
            .execute(
                "UPDATE cymule_timers SET selected_wait_ids = ?1
                 WHERE activation_id = ?2 AND selected_wait_ids IS NULL
                   AND acknowledged = 0",
                params![target_bytes, timer.activation_id],
            )
            .map_err(contention)?;
        let retained = self
            .connection
            .query_row(
                TIMER_POINT_READ_SQL,
                params![
                    &timer.activation_id,
                    timer_value_byte_limit(),
                    selected_wait_ids_byte_limit()
                ],
                stored_timer_from_row,
            )
            .optional()
            .map_err(contention)?;
        let Some(retained) = retained else {
            return Ok(None);
        };
        let retained = verify_stored_timer(retained)?;
        if retained.activation_id != timer.activation_id
            || retained.timer_id != timer.timer_id
            || retained.due_unix_ms != timer.due_unix_ms
            || retained.value != timer.value
        {
            return Err(timer_row_integrity(
                "timer_selection_schedule_changed",
                format!(
                    "timer activation {} changed schedule during target selection",
                    timer.activation_id
                ),
            ));
        }
        if retained.acknowledged {
            return Ok(None);
        }
        let wait_ids = retained.wait_ids.ok_or_else(|| {
            timer_row_integrity(
                "timer_selection_missing",
                format!(
                    "timer activation {} has no retained target set",
                    timer.activation_id
                ),
            )
        })?;
        let delivery = WaitDelivery {
            activation_id: retained.activation_id,
            source,
            wait_ids,
            value: retained.value,
        };
        if selected_now == 1 {
            if delivery.wait_ids != *selected_wait_ids {
                return Err(DurableError::Integrity {
                    code: "timer_selected_targets_changed".to_owned(),
                    message: format!(
                        "timer activation {} did not retain the target set selected by this receive call",
                        delivery.activation_id
                    ),
                });
            }
            return Ok(Some(WaitSourceDelivery::Selected(delivery)));
        }
        Ok(Some(WaitSourceDelivery::Retained(delivery)))
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
    ) -> DurableResult<Option<WaitSourceDelivery>> {
        validate_receive_bound(max_targets)?;
        let now =
            i64::try_from(self.clock.now_unix_ms()?).map_err(|error| DurableError::Substrate {
                code: "timer_clock_value_out_of_range".to_owned(),
                message: error.to_string(),
            })?;
        let retained: Option<StoredTimer> = self
            .connection
            .query_row(
                RETAINED_TIMER_READ_SQL,
                params![
                    timer_value_byte_limit(),
                    selected_wait_ids_byte_limit(),
                    now
                ],
                stored_timer_from_row,
            )
            .optional()
            .map_err(contention)?;
        if let Some(retained) = retained {
            let retained = verify_stored_timer(retained)?;
            let wait_ids = retained.wait_ids.ok_or_else(|| {
                timer_row_integrity(
                    "timer_selection_missing",
                    "retained timer query returned a row without selected targets",
                )
            })?;
            let now_unix_ms =
                u64::try_from(now).expect("wall Clock u64 converted to nonnegative SQLite INTEGER");
            if retained.acknowledged || retained.due_unix_ms > now_unix_ms {
                return Err(timer_row_integrity(
                    "timer_retained_row_mismatch",
                    "retained timer query returned a row outside its exact predicate",
                ));
            }
            return Ok(Some(WaitSourceDelivery::Retained(WaitDelivery {
                activation_id: retained.activation_id,
                source: WaitActivationSource::Timer {
                    timer_id: retained.timer_id,
                },
                wait_ids,
                value: retained.value,
            })));
        }
        self.receive_fresh_due(view, max_targets, now)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        validate_identity("activation", activation_id)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(contention)?;
        let existing: Option<StoredTimer> = transaction
            .query_row(
                TIMER_POINT_READ_SQL,
                params![
                    activation_id,
                    timer_value_byte_limit(),
                    selected_wait_ids_byte_limit()
                ],
                stored_timer_from_row,
            )
            .optional()
            .map_err(contention)?;
        let Some(existing) = existing else {
            return Err(DurableError::NotFound(format!(
                "timer activation {activation_id} is missing"
            )));
        };
        let existing = verify_stored_timer(existing)?;
        if existing.wait_ids.is_none() {
            return Err(DurableError::Validation(format!(
                "timer activation {activation_id} has not selected durable targets"
            )));
        }
        if !existing.acknowledged {
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

fn validate_new_targets(wait_ids: &BTreeSet<String>, max_targets: usize) -> DurableResult<()> {
    if wait_ids.len() != 1 || wait_ids.len() > max_targets {
        return Err(DurableError::Validation(format!(
            "new timer delivery must have exactly one target within requested bound {max_targets}; observed {}",
            wait_ids.len()
        )));
    }
    for wait_id in wait_ids {
        validate_wait_id(wait_id)?;
    }
    Ok(())
}

fn validate_receive_bound(max_targets: usize) -> DurableResult<()> {
    if !(1..=MAX_WAIT_DELIVERY_TARGETS).contains(&max_targets) {
        return Err(DurableError::Validation(format!(
            "timer wait target bound must be within 1..={MAX_WAIT_DELIVERY_TARGETS}"
        )));
    }
    Ok(())
}

fn validate_retained_targets(wait_ids: &BTreeSet<String>) -> DurableResult<()> {
    if wait_ids.len() != 1 {
        return Err(DurableError::Validation(format!(
            "retained timer delivery must have exactly one target; observed {}",
            wait_ids.len()
        )));
    }
    for wait_id in wait_ids {
        validate_wait_id(wait_id)?;
    }
    Ok(())
}

fn validate_identity(kind: &str, identity: &str) -> DurableResult<()> {
    cymule_core::validate_identity(&format!("timer {kind} identity"), identity).map_err(Into::into)
}

fn validate_wait_id(wait_id: &str) -> DurableResult<()> {
    cymule_core::validate_content_id("timer wait identity", wait_id).map_err(Into::into)
}

fn timer_value_byte_limit() -> i64 {
    i64::try_from(cymule_core::MAX_ARTIFACT_BYTES).expect("artifact byte limit fits SQLite INTEGER")
}

fn selected_wait_ids_byte_limit() -> i64 {
    i64::try_from(MAX_SELECTED_WAIT_IDS_BYTES)
        .expect("selected wait-ID byte limit fits SQLite INTEGER")
}

fn require_gated_timer_text(
    field: &'static str,
    sqlite_bytes: i64,
    sqlite_scalars: i64,
    value: String,
    maximum_bytes: usize,
    maximum_scalars: usize,
) -> DurableResult<String> {
    let bytes = usize::try_from(sqlite_bytes).map_err(|error| {
        timer_row_integrity(
            "timer_row_text_length_invalid",
            format!("timer {field} has invalid byte-length metadata: {error}"),
        )
    })?;
    let scalars = usize::try_from(sqlite_scalars).map_err(|error| {
        timer_row_integrity(
            "timer_row_text_length_invalid",
            format!("timer {field} has invalid scalar-length metadata: {error}"),
        )
    })?;
    if bytes > maximum_bytes || scalars > maximum_scalars {
        return Err(timer_row_integrity(
            "timer_row_text_too_large",
            format!(
                "timer {field} has {bytes} bytes and {scalars} scalars; maxima are {maximum_bytes} bytes and {maximum_scalars} scalars"
            ),
        ));
    }
    if value.len() != bytes || value.chars().count() != scalars {
        return Err(timer_row_integrity(
            "timer_row_text_projection_mismatch",
            format!("timer {field} does not match its SQLite length metadata"),
        ));
    }
    Ok(value)
}

fn require_gated_timer_blob(
    activation_id: &str,
    field: &'static str,
    sqlite_length: i64,
    bytes: Option<Vec<u8>>,
    maximum: usize,
) -> DurableResult<Vec<u8>> {
    let length = usize::try_from(sqlite_length).map_err(|error| {
        timer_row_integrity(
            "timer_row_blob_length_invalid",
            format!("timer {field} has invalid SQLite length metadata: {error}"),
        )
    })?;
    if length > maximum {
        return Err(timer_row_integrity(
            "timer_row_blob_too_large",
            format!(
                "timer activation {activation_id} retains {length} {field} bytes; maximum is {maximum}"
            ),
        ));
    }
    let bytes = bytes.ok_or_else(|| {
        timer_row_integrity(
            "timer_row_blob_gate_missing",
            format!("timer {field} passed its length bound but SQLite returned no bytes"),
        )
    })?;
    if bytes.len() != length {
        return Err(timer_row_integrity(
            "timer_row_blob_length_mismatch",
            format!(
                "timer {field} materialized {} bytes but SQLite declared {length}",
                bytes.len()
            ),
        ));
    }
    Ok(bytes)
}

fn timer_schedule_digest(
    activation_id: &str,
    timer_id: &str,
    due_unix_ms: u64,
    value: &Value,
) -> DurableResult<String> {
    cymule_core::canonical_digest(&TimerSchedule {
        activation_id,
        timer_id,
        due_unix_ms,
        value,
    })
    .map_err(DurableError::from)
}

fn verify_stored_timer(stored: StoredTimer) -> DurableResult<VerifiedTimer> {
    let activation_id = require_gated_timer_text(
        "activation_id",
        stored.activation_id_bytes,
        stored.activation_id_scalars,
        stored.activation_id,
        MAX_IDENTITY_UTF8_BYTES,
        MAX_IDENTITY_SCALARS,
    )?;
    validate_identity("activation", &activation_id).map_err(|error| {
        timer_row_integrity("timer_schedule_activation_invalid", error.to_string())
    })?;
    let timer_id = require_gated_timer_text(
        "timer_id",
        stored.timer_id_bytes,
        stored.timer_id_scalars,
        stored.timer_id,
        MAX_IDENTITY_UTF8_BYTES,
        MAX_IDENTITY_SCALARS,
    )?;
    validate_identity("timer", &timer_id)
        .map_err(|error| timer_row_integrity("timer_schedule_timer_invalid", error.to_string()))?;
    let schedule_digest = require_gated_timer_text(
        "schedule_digest",
        stored.schedule_digest_bytes,
        stored.schedule_digest_scalars,
        stored.schedule_digest,
        CANONICAL_DIGEST_BYTES,
        CANONICAL_DIGEST_BYTES,
    )?;
    let due_unix_ms = u64::try_from(stored.due_unix_ms)
        .map_err(|error| timer_row_integrity("timer_schedule_due_invalid", error.to_string()))?;
    let value_bytes = require_gated_timer_blob(
        &activation_id,
        "value_json",
        stored.value_bytes,
        stored.value,
        cymule_core::MAX_ARTIFACT_BYTES,
    )?;
    let value = decode_canonical_timer_row(&value_bytes, "value_json")?;
    let digest = timer_schedule_digest(&activation_id, &timer_id, due_unix_ms, &value)
        .map_err(|error| timer_row_integrity("timer_schedule_digest_invalid", error.to_string()))?;
    if schedule_digest != digest {
        return Err(timer_row_integrity(
            "timer_schedule_digest_mismatch",
            format!("timer activation {activation_id} does not match its retained schedule digest"),
        ));
    }
    let wait_ids = match (stored.wait_ids_bytes, stored.wait_ids) {
        (None, None) => None,
        (Some(length), bytes) => {
            let bytes = require_gated_timer_blob(
                &activation_id,
                "selected_wait_ids",
                length,
                bytes,
                MAX_SELECTED_WAIT_IDS_BYTES,
            )?;
            let wait_ids = decode_canonical_timer_row(&bytes, "selected_wait_ids")?;
            validate_retained_targets(&wait_ids).map_err(|error| {
                timer_row_integrity("timer_selected_targets_invalid", error.to_string())
            })?;
            Some(wait_ids)
        }
        (None, Some(_)) => {
            return Err(timer_row_integrity(
                "timer_selected_targets_length_missing",
                "timer selected_wait_ids has bytes without SQLite length metadata",
            ));
        }
    };
    let acknowledged = match stored.acknowledged {
        0 => false,
        1 => true,
        value => {
            return Err(timer_row_integrity(
                "timer_acknowledgement_invalid",
                format!("timer acknowledgement flag is {value}, expected 0 or 1"),
            ));
        }
    };
    if acknowledged && wait_ids.is_none() {
        return Err(timer_row_integrity(
            "timer_acknowledgement_without_selection",
            "acknowledged timer has no retained target set",
        ));
    }
    Ok(VerifiedTimer {
        activation_id,
        timer_id,
        due_unix_ms,
        value,
        wait_ids,
        acknowledged,
    })
}

fn stored_timer_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredTimer> {
    Ok(StoredTimer {
        activation_id_bytes: row.get(0)?,
        activation_id_scalars: row.get(1)?,
        activation_id: row.get(2)?,
        timer_id_bytes: row.get(3)?,
        timer_id_scalars: row.get(4)?,
        timer_id: row.get(5)?,
        due_unix_ms: row.get(6)?,
        schedule_digest_bytes: row.get(7)?,
        schedule_digest_scalars: row.get(8)?,
        schedule_digest: row.get(9)?,
        acknowledged: row.get(10)?,
        value_bytes: row.get(11)?,
        value: row.get(12)?,
        wait_ids_bytes: row.get(13)?,
        wait_ids: row.get(14)?,
    })
}

fn decode_canonical_timer_row<T>(bytes: &[u8], field: &str) -> DurableResult<T>
where
    T: DeserializeOwned + Serialize,
{
    let value = cymule_core::decode_json(bytes).map_err(|error| {
        timer_row_integrity(
            "timer_row_json_invalid",
            format!("timer {field} is malformed: {error}"),
        )
    })?;
    let canonical = cymule_core::canonical_bytes(&value).map_err(|error| {
        timer_row_integrity(
            "timer_row_json_invalid",
            format!("timer {field} cannot be canonically encoded: {error}"),
        )
    })?;
    if canonical != bytes {
        return Err(timer_row_integrity(
            "timer_row_json_noncanonical",
            format!("timer {field} is not strict canonical JSON"),
        ));
    }
    Ok(value)
}

fn timer_row_integrity(code: &'static str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        FRESH_TIMER_SCAN_SQL, MAX_SELECTED_WAIT_IDS_BYTES, RETAINED_TIMER_READ_SQL,
        SqliteTimerDriver, TIMER_POINT_READ_SQL, TIMER_SOURCE_SCAN_LIMIT,
        selected_wait_ids_byte_limit, timer_value_byte_limit,
    };
    use rusqlite::{Connection, params};

    fn query_plan<P: rusqlite::Params>(
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> Vec<String> {
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("query plan prepares");
        statement
            .query_map(params, |row| row.get(3))
            .expect("query plan executes")
            .collect::<Result<Vec<_>, _>>()
            .expect("query plan rows decode")
    }

    fn assert_indexed(plan: &[String], index: &str) {
        assert!(
            plan.iter().any(|step| step.contains(index)),
            "query plan omitted {index}: {plan:?}"
        );
        assert!(
            plan.iter()
                .all(|step| !step.contains("SCAN cymule_timers") && !step.contains("TEMP B-TREE")),
            "query plan used a table scan or temp sort: {plan:?}"
        );
    }

    #[test]
    fn fresh_scan_projection_contains_only_bounded_metadata() {
        let projection = FRESH_TIMER_SCAN_SQL
            .split("FROM")
            .next()
            .expect("fresh scan has a projection");
        assert!(projection.contains("activation_id"));
        assert!(projection.contains("substr(activation_id, 1, 513)"));
        assert!(projection.contains("length(activation_id)"));
        assert!(projection.contains("due_unix_ms"));
        assert!(projection.contains("length(value_json)"));
        for payload_column in ["timer_id", "schedule_digest", "selected_wait_ids"] {
            assert!(
                !projection.contains(payload_column),
                "fresh scan projection materialized {payload_column}"
            );
        }
        assert_eq!(TIMER_SOURCE_SCAN_LIMIT, 256);
        assert!(FRESH_TIMER_SCAN_SQL.contains("LIMIT ?4"));
        let target = BTreeSet::from([format!("sha256:{}", "a".repeat(64))]);
        assert_eq!(
            cymule_core::canonical_bytes(&target)
                .expect("single target encodes")
                .len(),
            MAX_SELECTED_WAIT_IDS_BYTES
        );
        let digest = super::timer_schedule_digest(
            "activation:digest",
            "timer:digest",
            1,
            &serde_json::json!(true),
        )
        .expect("schedule digest derives");
        assert_eq!(digest.len(), super::CANONICAL_DIGEST_BYTES);
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        assert!(super::TIMER_POINT_READ_SQL.contains("substr(schedule_digest, 1, 65)"));
    }

    #[test]
    fn hot_timer_queries_use_their_composite_or_primary_indexes() {
        let directory = tempfile::tempdir().expect("temporary directory creates");
        let driver = SqliteTimerDriver::open(directory.path().join("timer.sqlite"))
            .expect("timer store opens");
        let fresh = query_plan(
            &driver.connection,
            FRESH_TIMER_SCAN_SQL,
            params![100_i64, i64::MIN, "", 256_i64],
        );
        assert_indexed(&fresh, "cymule_timers_due_fresh");
        let retained = query_plan(
            &driver.connection,
            RETAINED_TIMER_READ_SQL,
            params![
                timer_value_byte_limit(),
                selected_wait_ids_byte_limit(),
                100_i64
            ],
        );
        assert_indexed(&retained, "cymule_timers_due_retained");
        let point = query_plan(
            &driver.connection,
            TIMER_POINT_READ_SQL,
            params![
                "activation:point",
                timer_value_byte_limit(),
                selected_wait_ids_byte_limit()
            ],
        );
        assert_indexed(&point, "sqlite_autoindex_cymule_timers_1");
    }
}
