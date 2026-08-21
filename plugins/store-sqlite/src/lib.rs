//! `SQLite` realization of Cymule's segmented durable head CAS.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use cymule_durable::{
    DurableError, DurableResult, DurableState, DurableStore, GcReceipt, StateCheckpoint,
    StateSegment, StoreBatch, StoreCommit, StoreHead, StoreStats, StoredState, restore,
};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const SCHEMA: &str = "cymule.sqlite-store/2";
const LEGACY_SCHEMA: &str = "cymule.sqlite-store/1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Authenticated evidence emitted by explicit legacy import.
pub struct LegacyMigrationReceipt {
    /// Migration receipt schema.
    pub migration_version: String,
    /// Imported durable domain.
    pub domain: String,
    /// Authenticated legacy revision.
    pub legacy_revision: String,
    /// New sequence-zero checkpoint.
    pub checkpoint_id: String,
    /// Content-addressed receipt identity.
    pub receipt_id: String,
}

/// One SQLite-backed segmented durable domain.
pub struct SqliteStore {
    connection: Connection,
    domain: String,
    writable: bool,
    last_reopened: Cell<u32>,
}

impl SqliteStore {
    /// Open or create a writable segmented store.
    pub fn open(path: impl AsRef<Path>, domain: impl Into<String>) -> DurableResult<Self> {
        let connection = Connection::open(path).map_err(sqlite_error)?;
        Self::initialize(connection, domain.into(), true)
    }

    /// Create an isolated in-memory segmented store.
    pub fn in_memory(domain: impl Into<String>) -> DurableResult<Self> {
        let connection = Connection::open_in_memory().map_err(sqlite_error)?;
        Self::initialize(connection, domain.into(), false)
    }

    /// Open an existing segmented store without configuration writes.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        domain: impl Into<String>,
    ) -> DurableResult<Self> {
        let domain = domain.into();
        validate_domain(&domain)?;
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(sqlite_error)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        require_v2(&connection)?;
        Ok(Self {
            connection,
            domain,
            writable: false,
            last_reopened: Cell::new(0),
        })
    }

    fn initialize(
        connection: Connection,
        domain: String,
        file_backed: bool,
    ) -> DurableResult<Self> {
        validate_domain(&domain)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        if has_table(&connection, "cymule_state")? {
            return Err(DurableError::Validation(
                "legacy cymule.sqlite-store/1 requires explicit offline SqliteStore::migrate_v1"
                    .to_owned(),
            ));
        }
        if file_backed {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(sqlite_error)?;
            connection
                .pragma_update(None, "synchronous", "FULL")
                .map_err(sqlite_error)?;
        }
        create_v2(&connection)?;
        require_v2(&connection)?;
        Ok(Self {
            connection,
            domain,
            writable: true,
            last_reopened: Cell::new(0),
        })
    }

    /// Convert the old whole-state table while no readers or writers are active.
    ///
    /// Normal `open` never performs this migration and never reads both formats.
    pub fn migrate_v1(path: impl AsRef<Path>) -> DurableResult<Vec<LegacyMigrationReceipt>> {
        let mut connection = Connection::open(path).map_err(sqlite_error)?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(sqlite_error)?;
        if !has_table(&connection, "cymule_state")? {
            return Err(DurableError::Validation(
                "legacy SQLite state table does not exist".to_owned(),
            ));
        }
        let segmented_tables = [
            "cymule_store_meta",
            "cymule_heads",
            "cymule_checkpoints",
            "cymule_segments",
            "cymule_gc_receipts",
            "cymule_migrations",
        ];
        for table in segmented_tables {
            if has_table(&connection, table)? {
                return Err(DurableError::Validation(
                    "refusing mixed legacy and segmented SQLite formats".to_owned(),
                ));
            }
        }
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Exclusive)
            .map_err(|error| contention(error, None))?;
        let rows = {
            let mut statement = transaction
                .prepare("SELECT domain, schema_version, revision, state_json FROM cymule_state ORDER BY domain")
                .map_err(sqlite_error)?;
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                    ))
                })
                .map_err(sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sqlite_error)?
        };
        create_v2(&transaction)?;
        let mut receipts = Vec::new();
        for (domain, schema, revision, bytes) in rows {
            if schema != LEGACY_SCHEMA {
                return Err(DurableError::Validation(format!(
                    "unsupported legacy SQLite store schema {schema}"
                )));
            }
            validate_domain(&domain)?;
            let state: DurableState = cymule_core::decode_json(&bytes)?;
            if state.revision()? != revision {
                return Err(DurableError::Validation(format!(
                    "legacy revision for {domain} does not authenticate its state"
                )));
            }
            let batch = StoreBatch::initialize(state)?;
            insert_json(
                &transaction,
                "cymule_checkpoints",
                &domain,
                &batch
                    .checkpoint()
                    .expect("initial checkpoint")
                    .checkpoint_id,
                batch.checkpoint().expect("initial checkpoint"),
            )?;
            transaction
                .execute(
                    "INSERT INTO cymule_heads(domain, head_json) VALUES (?1, ?2)",
                    params![domain, canonical(&batch.head())?],
                )
                .map_err(sqlite_error)?;
            let receipt_id = cymule_core::content_id(
                "cymule.sqlite-v1-migration/1",
                &(
                    domain.as_str(),
                    revision.as_str(),
                    batch.head().checkpoint_id.as_str(),
                ),
            )?;
            let receipt = LegacyMigrationReceipt {
                migration_version: "cymule.sqlite-v1-migration/1".to_owned(),
                domain: domain.clone(),
                legacy_revision: revision,
                checkpoint_id: batch.head().checkpoint_id.clone(),
                receipt_id,
            };
            transaction
                .execute(
                    "INSERT INTO cymule_migrations(receipt_id, receipt_json) VALUES (?1, ?2)",
                    params![receipt.receipt_id, canonical(&receipt)?],
                )
                .map_err(sqlite_error)?;
            receipts.push(receipt);
        }
        transaction
            .execute_batch("DROP TABLE cymule_state;")
            .map_err(sqlite_error)?;
        require_v2(&transaction)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(receipts)
    }

    fn read(&self) -> DurableResult<Option<StoredState>> {
        let Some(head) = read_head(&self.connection, &self.domain)? else {
            return Ok(None);
        };
        let (stored, reopened) = restore_sql(&self.connection, &self.domain, &head)?;
        self.last_reopened.set(reopened);
        Ok(Some(stored))
    }
}

impl DurableStore for SqliteStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.read()
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only SQLite stores cannot compare and commit".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| contention(error, expected))?;
        let current_head = read_head(&transaction, &self.domain)?;
        if current_head.as_ref() != expected {
            return Err(conflict(expected, current_head.as_ref()));
        }
        batch.verify_against(current_head.as_ref())?;
        if let Some(segment) = batch.segment() {
            insert_json(
                &transaction,
                "cymule_segments",
                &self.domain,
                &segment.segment_id,
                segment,
            )?;
        }
        if let Some(checkpoint) = batch.checkpoint() {
            insert_json(
                &transaction,
                "cymule_checkpoints",
                &self.domain,
                &checkpoint.checkpoint_id,
                checkpoint,
            )?;
        }
        transaction
            .execute(
                "INSERT INTO cymule_heads(domain, head_json) VALUES (?1, ?2)
             ON CONFLICT(domain) DO UPDATE SET head_json = excluded.head_json",
                params![self.domain, canonical(&batch.head())?],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(StoreCommit {
            revision: batch.head().revision.clone(),
            head: batch.head().clone(),
        })
    }

    fn reclaim_cold(&mut self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only SQLite stores cannot reclaim cold objects".to_owned(),
            ));
        }
        let stored = self.read()?.ok_or_else(|| {
            DurableError::NotFound("SQLite durable domain is not initialized".to_owned())
        })?;
        if &stored.head != expected {
            return Err(conflict(Some(expected), Some(&stored.head)));
        }
        let checkpoint = StateCheckpoint::for_revision(
            None,
            None,
            expected.sequence,
            expected.revision.clone(),
            Some(stored.state),
        )?;
        let mut head = expected.clone();
        head.checkpoint_id.clone_from(&checkpoint.checkpoint_id);
        head.checkpoint_depth = 0;
        head.suffix_head = None;
        head.suffix_len = 0;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| contention(error, Some(expected)))?;
        let current = read_head(&transaction, &self.domain)?;
        if current.as_ref() != Some(expected) {
            return Err(conflict(Some(expected), current.as_ref()));
        }
        let mut reclaimed = BTreeSet::new();
        for id in list_ids(&transaction, "cymule_checkpoints", &self.domain)? {
            reclaimed.insert(id);
        }
        for id in list_ids(&transaction, "cymule_segments", &self.domain)? {
            reclaimed.insert(id);
        }
        transaction
            .execute(
                "DELETE FROM cymule_checkpoints WHERE domain = ?1",
                [&self.domain],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM cymule_segments WHERE domain = ?1",
                [&self.domain],
            )
            .map_err(sqlite_error)?;
        insert_json(
            &transaction,
            "cymule_checkpoints",
            &self.domain,
            &checkpoint.checkpoint_id,
            &checkpoint,
        )?;
        let receipt = GcReceipt::new(&head, &reclaimed)?;
        insert_json(
            &transaction,
            "cymule_gc_receipts",
            &self.domain,
            &receipt.receipt_id,
            &receipt,
        )?;
        head.gc_receipt = Some(receipt.receipt_id.clone());
        transaction
            .execute(
                "UPDATE cymule_heads SET head_json = ?1 WHERE domain = ?2",
                params![canonical(&head)?, self.domain],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(receipt)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        Ok(StoreStats {
            checkpoints: count(&self.connection, "cymule_checkpoints", &self.domain)?,
            segments: count(&self.connection, "cymule_segments", &self.domain)?,
            reopened_segments: self.last_reopened.get(),
            gc_receipts: count(&self.connection, "cymule_gc_receipts", &self.domain)?,
        })
    }
}

fn create_v2(connection: &Connection) -> DurableResult<()> {
    connection
        .execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS cymule_store_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1), schema_version TEXT NOT NULL
         ) STRICT;
         INSERT INTO cymule_store_meta(singleton, schema_version) VALUES (1, '{SCHEMA}')
         ON CONFLICT(singleton) DO NOTHING;
         CREATE TABLE IF NOT EXISTS cymule_heads (
            domain TEXT PRIMARY KEY NOT NULL, head_json BLOB NOT NULL
         ) STRICT;
         CREATE TABLE IF NOT EXISTS cymule_checkpoints (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id)
         ) STRICT;
         CREATE TABLE IF NOT EXISTS cymule_segments (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id)
         ) STRICT;
         CREATE TABLE IF NOT EXISTS cymule_gc_receipts (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id)
         ) STRICT;
         CREATE TABLE IF NOT EXISTS cymule_migrations (
            receipt_id TEXT PRIMARY KEY NOT NULL, receipt_json BLOB NOT NULL
         ) STRICT;"
        ))
        .map_err(sqlite_error)
}

fn require_v2(connection: &Connection) -> DurableResult<()> {
    if has_table(connection, "cymule_state")? {
        return Err(DurableError::Validation(
            "legacy cymule.sqlite-store/1 requires explicit offline SqliteStore::migrate_v1"
                .to_owned(),
        ));
    }
    let schema: Option<String> = connection
        .query_row(
            "SELECT schema_version FROM cymule_store_meta WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    if schema.as_deref() != Some(SCHEMA) {
        return Err(DurableError::Validation(format!(
            "unsupported SQLite durable schema {schema:?}"
        )));
    }
    Ok(())
}

fn has_table(connection: &Connection, name: &str) -> DurableResult<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [name],
            |row| row.get(0),
        )
        .map_err(sqlite_error)
}

fn restore_sql(
    connection: &Connection,
    domain: &str,
    head: &StoreHead,
) -> DurableResult<(StoredState, u32)> {
    verify_gc_receipt(connection, domain, head)?;
    let mut checkpoints = BTreeMap::new();
    for id in list_ids(connection, "cymule_checkpoints", domain)? {
        let value: StateCheckpoint = read_object(connection, "cymule_checkpoints", domain, &id)?
            .ok_or_else(|| {
                DurableError::NotFound(format!("durable checkpoint {id} does not exist"))
            })?;
        if value.checkpoint_id != id {
            return Err(DurableError::Validation(
                "SQLite checkpoint locator does not match its content identity".to_owned(),
            ));
        }
        checkpoints.insert(id, value);
    }
    let mut values = BTreeMap::new();
    for id in list_ids(connection, "cymule_segments", domain)? {
        let value: StateSegment = read_object(connection, "cymule_segments", domain, &id)?
            .ok_or_else(|| {
                DurableError::NotFound(format!("durable segment {id} does not exist"))
            })?;
        if value.segment_id != id {
            return Err(DurableError::Validation(
                "SQLite segment locator does not match its content identity".to_owned(),
            ));
        }
        values.insert(id, value);
    }
    restore(
        head,
        |id| checkpoints.get(id).cloned(),
        |id| values.get(id).cloned(),
    )
}

fn verify_gc_receipt(connection: &Connection, domain: &str, head: &StoreHead) -> DurableResult<()> {
    let Some(receipt_id) = &head.gc_receipt else {
        return Ok(());
    };
    let receipt: GcReceipt = read_object(connection, "cymule_gc_receipts", domain, receipt_id)?
        .ok_or_else(|| DurableError::NotFound(format!("GC receipt {receipt_id} does not exist")))?;
    if receipt.receipt_id != *receipt_id {
        return Err(DurableError::Validation(
            "SQLite GC receipt locator does not match its content identity".to_owned(),
        ));
    }
    receipt.verify_for(head)
}

fn read_head(connection: &Connection, domain: &str) -> DurableResult<Option<StoreHead>> {
    let bytes: Option<Vec<u8>> = connection
        .query_row(
            "SELECT head_json FROM cymule_heads WHERE domain = ?1",
            [domain],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    bytes
        .map(|bytes| cymule_core::decode_json(&bytes).map_err(Into::into))
        .transpose()
}

fn read_object<T: DeserializeOwned>(
    connection: &Connection,
    table: &str,
    domain: &str,
    id: &str,
) -> DurableResult<Option<T>> {
    let sql = format!("SELECT object_json FROM {table} WHERE domain = ?1 AND object_id = ?2");
    let bytes: Option<Vec<u8>> = connection
        .query_row(&sql, params![domain, id], |row| row.get(0))
        .optional()
        .map_err(sqlite_error)?;
    bytes
        .map(|bytes| cymule_core::decode_json(&bytes).map_err(Into::into))
        .transpose()
}

fn insert_json<T: Serialize + DeserializeOwned + PartialEq>(
    transaction: &Transaction<'_>,
    table: &str,
    domain: &str,
    id: &str,
    value: &T,
) -> DurableResult<()> {
    let sql = format!(
        "INSERT OR IGNORE INTO {table}(domain, object_id, object_json) VALUES (?1, ?2, ?3)"
    );
    transaction
        .execute(&sql, params![domain, id, canonical(value)?])
        .map_err(sqlite_error)?;
    let retained: T = read_object(transaction, table, domain, id)?.ok_or_else(|| {
        DurableError::NotFound(format!("immutable SQLite object {id} disappeared"))
    })?;
    if &retained != value {
        return Err(DurableError::Validation(format!(
            "immutable SQLite object {id} has conflicting bytes"
        )));
    }
    Ok(())
}

fn list_ids(connection: &Connection, table: &str, domain: &str) -> DurableResult<Vec<String>> {
    let sql = format!("SELECT object_id FROM {table} WHERE domain = ?1 ORDER BY object_id");
    let mut statement = connection.prepare(&sql).map_err(sqlite_error)?;
    statement
        .query_map([domain], |row| row.get(0))
        .map_err(sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error)
}

fn count(connection: &Connection, table: &str, domain: &str) -> DurableResult<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE domain = ?1");
    let value: i64 = connection
        .query_row(&sql, [domain], |row| row.get(0))
        .map_err(sqlite_error)?;
    u64::try_from(value).map_err(|error| DurableError::Validation(error.to_string()))
}

fn canonical(value: &impl Serialize) -> DurableResult<Vec<u8>> {
    cymule_core::canonical_bytes(value).map_err(Into::into)
}

fn validate_domain(domain: &str) -> DurableResult<()> {
    if domain.is_empty() || domain.len() > 512 || domain.chars().any(char::is_control) {
        return Err(DurableError::Validation(
            "SQLite durable domain must contain 1..=512 non-control characters".to_owned(),
        ));
    }
    Ok(())
}

fn conflict(expected: Option<&StoreHead>, current: Option<&StoreHead>) -> DurableError {
    DurableError::Conflict {
        expected: expected.map(|head| head.revision.clone()),
        current: current.map(|head| head.revision.clone()),
    }
}

fn contention(error: rusqlite::Error, expected: Option<&StoreHead>) -> DurableError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: expected.map(|head| head.revision.clone()),
                current: Some("sqlite-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(error),
    }
}

fn sqlite_error(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
