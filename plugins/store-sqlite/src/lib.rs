//! Embedded database realization of Cymule's content-addressed durable-root CAS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use cymule_core::{
    MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES, MachineCommandArchiveEntry,
    MachineCommandArchiveObject, MachineCommandArchiveSegment, MachineCommandBatchRecord,
    MachineCommandIndexNode,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DurableError, DurableResult, DurableStore, GcReceipt,
    JournalRecordManifest, MAX_GC_RECEIPT_BYTES, MAX_GC_RECLAIMED_OBJECTS,
    MAX_STATE_ROOT_OBJECT_BYTES, MAX_STORE_HEAD_BYTES, StateRootManifest, StateRootObject,
    StateRootResolver, StoreBatch, StoreCommit, StoreHead, StoreReclamation, StoreStats,
    decode_state_root_object, load_application_journal_prefix,
    load_application_journal_prefix_replacement_authority,
    load_application_journal_record_manifest, load_coupled_checkpoint_receipt,
    reachable_machine_command_archive_ids, reachable_machine_command_index_objects,
    reachable_state_root_objects,
};
use rusqlite::{
    Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Serialize, de::DeserializeOwned};

const SCHEMA: &str = "cymule.sqlite-store/6";
const META_TABLE: &str = "cymule_store_meta";
const HEAD_TABLE: &str = "cymule_heads";
const STATE_ROOT_TABLE: &str = "cymule_state_root_objects";
const GC_RECEIPT_TABLE: &str = "cymule_gc_receipts";
const COMMAND_ARCHIVE_TABLE: &str = "cymule_machine_command_archive_objects";
const CURRENT_SCHEMA: [(&str, &str); 5] = [
    (
        META_TABLE,
        "CREATE TABLE cymule_store_meta (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1), schema_version TEXT NOT NULL
         ) STRICT",
    ),
    (
        HEAD_TABLE,
        "CREATE TABLE cymule_heads (
            domain TEXT PRIMARY KEY NOT NULL, head_json BLOB NOT NULL
         ) STRICT",
    ),
    (
        STATE_ROOT_TABLE,
        "CREATE TABLE cymule_state_root_objects (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id)
         ) STRICT",
    ),
    (
        GC_RECEIPT_TABLE,
        "CREATE TABLE cymule_gc_receipts (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id)
         ) STRICT",
    ),
    (
        COMMAND_ARCHIVE_TABLE,
        "CREATE TABLE cymule_machine_command_archive_objects (
            domain TEXT NOT NULL, object_id TEXT NOT NULL, batch_id TEXT,
            object_json BLOB NOT NULL,
            PRIMARY KEY(domain, object_id), UNIQUE(domain, batch_id)
         ) STRICT",
    ),
];

/// Stable code returned before mutation for every unsupported physical generation.
pub const UNSUPPORTED_STORE_GENERATION_CODE: &str = "unsupported_store_generation";
const HEAD_INTEGRITY_CODE: &str = "sqlite_head_integrity";
const STATE_ROOT_LOCATOR_INTEGRITY_CODE: &str = "sqlite_state_root_object_locator";
const STATE_ROOT_MANIFEST_KIND_INTEGRITY_CODE: &str = "sqlite_state_root_manifest_kind";
const STATE_ROOT_CLOSURE_INTEGRITY_CODE: &str = "sqlite_state_root_closure";
const GC_RECEIPT_LOCATOR_INTEGRITY_CODE: &str = "sqlite_gc_receipt_locator";
const COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE: &str = "sqlite_command_archive_object_locator";
const SQLITE_OPEN_FAILURE_CODE: &str = "sqlite_open_failure";
const SQLITE_CONFIGURATION_FAILURE_CODE: &str = "sqlite_configuration_failure";
const SQLITE_SCHEMA_FAILURE_CODE: &str = "sqlite_schema_failure";
const SQLITE_TRANSACTION_FAILURE_CODE: &str = "sqlite_transaction_failure";
const SQLITE_HEAD_READ_FAILURE_CODE: &str = "sqlite_head_read_failure";
const SQLITE_HEAD_CAS_FAILURE_CODE: &str = "sqlite_head_cas_failure";
const SQLITE_STATE_ROOT_FAILURE_CODE: &str = "sqlite_state_root_failure";
const SQLITE_ARCHIVE_FAILURE_CODE: &str = "sqlite_command_archive_failure";
const SQLITE_GC_INVENTORY_FAILURE_CODE: &str = "sqlite_gc_inventory_failure";
const SQLITE_GC_SWEEP_FAILURE_CODE: &str = "sqlite_gc_sweep_failure";
const SQLITE_GC_TRANSACTION_FAILURE_CODE: &str = "sqlite_gc_transaction_failure";
const SQLITE_STATS_FAILURE_CODE: &str = "sqlite_stats_failure";
const SQLITE_IDENTITY_INVENTORY_FAILURE_CODE: &str = "sqlite_identity_inventory_failure";
#[cfg(test)]
const SQLITE_TEST_BOUNDARY_IO_CODE: &str = "sqlite_test_boundary_io";
#[cfg(test)]
const SQLITE_INJECTED_RECONCILE_FAILURE_CODE: &str = "sqlite_injected_reconcile_failure";

#[cfg(test)]
std::thread_local! {
    static GC_RECEIPT_READ_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// One SQLite-backed content-addressed durable domain.
pub struct SqliteStore {
    connection: Connection,
    domain: String,
    writable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqliteBarrier {
    ObjectsStaged,
    HeadStaged,
    CommitComplete,
    GcAdvanceReceiptStaged,
    GcAdvanceSweepStaged,
    GcAdvanceHeadStaged,
    GcAdvanceCommitComplete,
    GcReconcileSweepStaged,
    GcReconcileCommitComplete,
}

impl SqliteStore {
    /// Open or create a writable persistent-root store.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid domain, an unsupported exact physical
    /// generation, immediate writer contention, or a storage failure.
    pub fn open(path: impl AsRef<Path>, domain: impl Into<String>) -> DurableResult<Self> {
        Self::open_connection(domain.into(), || Connection::open(path), true)
    }

    /// Create an isolated in-memory persistent-root store.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid domain or when the exact schema cannot
    /// be initialized atomically.
    pub fn in_memory(domain: impl Into<String>) -> DurableResult<Self> {
        Self::open_connection(domain.into(), Connection::open_in_memory, false)
    }

    /// Open an existing persistent-root store without configuration or schema writes.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid domain, a missing database, or any
    /// schema generation other than the exact current generation.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        domain: impl Into<String>,
    ) -> DurableResult<Self> {
        let domain = domain.into();
        validate_domain(&domain)?;
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|error| sqlite_operation_error(SQLITE_OPEN_FAILURE_CODE, error))?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(|error| sqlite_operation_error(SQLITE_CONFIGURATION_FAILURE_CODE, error))?;
        require_current(&connection)?;
        Ok(Self {
            connection,
            domain,
            writable: false,
        })
    }

    fn open_connection(
        domain: String,
        open: impl FnOnce() -> rusqlite::Result<Connection>,
        file_backed: bool,
    ) -> DurableResult<Self> {
        validate_domain(&domain)?;
        let mut connection =
            open().map_err(|error| sqlite_operation_error(SQLITE_OPEN_FAILURE_CODE, error))?;
        connection
            .busy_timeout(Duration::ZERO)
            .map_err(|error| sqlite_operation_error(SQLITE_CONFIGURATION_FAILURE_CODE, error))?;
        if non_sqlite_schema_object_count(&connection)? == 0 {
            initialize_empty(&mut connection)?;
        } else {
            require_current(&connection)?;
        }
        if file_backed {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(|error| contention(&error, None, SQLITE_CONFIGURATION_FAILURE_CODE))?;
            connection
                .pragma_update(None, "synchronous", "FULL")
                .map_err(|error| contention(&error, None, SQLITE_CONFIGURATION_FAILURE_CODE))?;
            let journal_mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(|error| {
                    sqlite_operation_error(SQLITE_CONFIGURATION_FAILURE_CODE, error)
                })?;
            let synchronous: i64 = connection
                .pragma_query_value(None, "synchronous", |row| row.get(0))
                .map_err(|error| {
                    sqlite_operation_error(SQLITE_CONFIGURATION_FAILURE_CODE, error)
                })?;
            if !journal_mode.eq_ignore_ascii_case("wal") || synchronous != 2 {
                return Err(DurableError::Substrate {
                    code: SQLITE_CONFIGURATION_FAILURE_CODE.to_owned(),
                    message: "SQLite did not retain WAL plus FULL synchronous durability"
                        .to_owned(),
                });
            }
        }
        Ok(Self {
            connection,
            domain,
            writable: true,
        })
    }

    fn read_head(&mut self) -> DurableResult<Option<StoreHead>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_TRANSACTION_FAILURE_CODE, error))?;
        let Some(head_record) = read_head_record(&transaction, &self.domain)? else {
            transaction
                .commit()
                .map_err(|error| sqlite_operation_error(SQLITE_TRANSACTION_FAILURE_CODE, error))?;
            return Ok(None);
        };
        let head = head_record.value;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_TRANSACTION_FAILURE_CODE, error))?;
        Ok(Some(head))
    }

    fn compare_and_commit_with_response(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
        after_commit: impl FnOnce() -> Result<(), String>,
    ) -> DurableResult<StoreCommit> {
        self.compare_and_commit_with_barriers(expected, batch, |_| Ok(()), after_commit)
    }

    fn compare_and_commit_with_barriers(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
        mut barrier: impl FnMut(SqliteBarrier) -> DurableResult<()>,
        after_commit: impl FnOnce() -> Result<(), String>,
    ) -> DurableResult<StoreCommit> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only SQLite stores cannot compare and commit".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| contention(&error, expected, SQLITE_TRANSACTION_FAILURE_CODE))?;
        let current_record = read_head_record(&transaction, &self.domain)?;
        let current_head = current_record.as_ref().map(|record| &record.value);
        if current_head != expected {
            return Err(conflict(expected, current_head));
        }
        batch.verify_against(current_head)?;
        let parent_manifest = current_head
            .map(|head| {
                let manifest = read_required_state_root_manifest(
                    &transaction,
                    &self.domain,
                    &head.state_root_manifest_id,
                )?;
                require_manifest_matches_head(&manifest, head)?;
                Ok::<StateRootManifest, DurableError>(manifest)
            })
            .transpose()?;
        batch
            .state_root_transition()
            .verify(parent_manifest.as_ref())?;
        for object in batch.state_root_transition().objects() {
            insert_state_root_object(&transaction, &self.domain, object)?;
        }
        for object in batch.machine_command_archive_objects() {
            insert_command_archive_object(&transaction, &self.domain, object)?;
        }
        barrier(SqliteBarrier::ObjectsStaged)?;
        write_head_cas(
            &transaction,
            &self.domain,
            current_record.as_ref(),
            batch.head(),
        )?;
        barrier(SqliteBarrier::HeadStaged)?;
        transaction.commit().map_err(commit_outcome_unknown)?;
        barrier(SqliteBarrier::CommitComplete)
            .map_err(|error| commit_outcome_unknown(error.to_string()))?;
        after_commit().map_err(commit_outcome_unknown)?;
        Ok(StoreCommit {
            revision: batch.head().revision.clone(),
            head: batch.head().clone(),
        })
    }

    fn reconcile_cold_reclamation_with_barriers(
        &mut self,
        expected: &StoreHead,
        mut barrier: impl FnMut(SqliteBarrier) -> DurableResult<()>,
    ) -> DurableResult<GcReceipt> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only SQLite stores cannot reconcile cold reclamation".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                contention(&error, Some(expected), SQLITE_GC_TRANSACTION_FAILURE_CODE)
            })?;
        let current_record = read_head_record(&transaction, &self.domain)?;
        let current = current_record.as_ref().map(|record| &record.value);
        if current != Some(expected) {
            return Err(conflict(Some(expected), current));
        }
        let receipt =
            verify_gc_receipt(&transaction, &self.domain, expected)?.ok_or_else(|| {
                DurableError::Validation(
                    "cold-reclamation reconciliation requires a head-pinned GC receipt".to_owned(),
                )
            })?;
        let (retained_state_roots, retained_archives) =
            Self::retained_gc_authority(&transaction, &self.domain, expected)?;
        Self::replay_gc_receipt_page(
            &transaction,
            &self.domain,
            &receipt,
            &retained_state_roots,
            &retained_archives,
        )?;
        barrier(SqliteBarrier::GcReconcileSweepStaged)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_GC_SWEEP_FAILURE_CODE, error))?;
        barrier(SqliteBarrier::GcReconcileCommitComplete)?;
        Ok(receipt)
    }

    fn advance_cold_reclamation_with_barriers(
        &mut self,
        expected: &StoreHead,
        mut barrier: impl FnMut(SqliteBarrier) -> DurableResult<()>,
    ) -> DurableResult<GcReceipt> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only SQLite stores cannot reclaim cold objects".to_owned(),
            ));
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                contention(&error, Some(expected), SQLITE_GC_TRANSACTION_FAILURE_CODE)
            })?;
        let current_record = read_head_record(&transaction, &self.domain)?;
        let current = current_record.as_ref().map(|record| &record.value);
        if current != Some(expected) {
            return Err(conflict(Some(expected), current));
        }
        let pinned_receipt = verify_gc_receipt(&transaction, &self.domain, expected)?;
        let (retained_state_roots, retained_archives) =
            Self::retained_gc_authority(&transaction, &self.domain, expected)?;
        if let Some(receipt) = &pinned_receipt {
            Self::replay_gc_receipt_page(
                &transaction,
                &self.domain,
                receipt,
                &retained_state_roots,
                &retained_archives,
            )?;
        }
        audit_gc_receipt_inventory(&transaction, &self.domain)?;

        let inventory = collect_gc_candidate_inventory(
            &transaction,
            &self.domain,
            &retained_state_roots,
            &retained_archives,
            pinned_receipt
                .as_ref()
                .map(|receipt| receipt.receipt_id.as_str()),
        )?;
        let receipt =
            GcReceipt::new_bounded(expected, inventory.prefix, inventory.remaining_objects)?;
        require_identity_absent(&transaction, &self.domain, &receipt.receipt_id)?;
        let mut next_head = expected.clone();
        next_head.gc_sequence = receipt.gc_sequence;
        next_head
            .physical_token
            .clone_from(&receipt.result_physical_token);
        next_head.gc_receipt = Some(receipt.receipt_id.clone());
        receipt.verify_for(&next_head)?;
        insert_gc_receipt(&transaction, &self.domain, &receipt)?;
        barrier(SqliteBarrier::GcAdvanceReceiptStaged)?;

        let deleted = delete_receipt_objects(&transaction, &self.domain, &receipt.reclaimed_ids)?;
        if deleted != receipt.reclaimed_objects {
            return Err(DurableError::Integrity {
                code: "sqlite_gc_receipt_delete_mismatch".to_owned(),
                message: format!(
                    "SQLite GC receipt authorizes {} objects but exactly {deleted} were present",
                    receipt.reclaimed_objects
                ),
            });
        }
        if receipt.remaining_objects == 0 {
            require_only_gc_receipt(&transaction, &self.domain, &receipt.receipt_id)?;
        }
        barrier(SqliteBarrier::GcAdvanceSweepStaged)?;
        write_head_cas(
            &transaction,
            &self.domain,
            current_record.as_ref(),
            &next_head,
        )?;
        barrier(SqliteBarrier::GcAdvanceHeadStaged)?;
        transaction.commit().map_err(commit_outcome_unknown)?;
        barrier(SqliteBarrier::GcAdvanceCommitComplete)
            .map_err(|error| commit_outcome_unknown(error.to_string()))?;
        Ok(receipt)
    }

    fn retained_machine_command_objects(
        connection: &Connection,
        domain: &str,
        expected: &StoreHead,
    ) -> DurableResult<BTreeSet<String>> {
        let Some(anchor) = &expected.machine_base_anchor else {
            return Ok(BTreeSet::new());
        };
        let mut segment_batches = BTreeSet::new();
        let mut retained = reachable_machine_command_archive_ids(anchor, |id| {
            let segment = read_command_archive_segment(connection, domain, id)?;
            if let Some(segment) = &segment {
                for declared in &segment.batches {
                    let indexed =
                        read_command_archive_batch(connection, domain, &declared.batch_id)?
                            .ok_or_else(|| DurableError::Integrity {
                                code: "sqlite_command_archive_reachable_missing".to_owned(),
                                message: format!(
                                    "reachable archive batch {} does not exist",
                                    declared.batch_id
                                ),
                            })?;
                    if indexed != *declared {
                        return Err(DurableError::Integrity {
                            code: "sqlite_command_archive_batch_mismatch".to_owned(),
                            message:
                                "indexed Machine batch differs from its reachable archive segment"
                                    .to_owned(),
                        });
                    }
                    segment_batches.insert(indexed.batch_receipt_id);
                }
            }
            Ok(segment)
        })
        .map_err(command_archive_closure_error)?;
        retained.extend(segment_batches);
        let index = reachable_machine_command_index_objects(&anchor.command_index_root, |id| {
            read_command_index_node(connection, domain, id)
        })
        .map_err(command_archive_closure_error)?;
        for entry_id in &index.archive_entry_ids {
            let entry =
                read_command_archive_entry(connection, domain, entry_id)?.ok_or_else(|| {
                    DurableError::Integrity {
                        code: "sqlite_command_archive_reachable_missing".to_owned(),
                        message: format!(
                            "reachable Machine command archive entry {entry_id} does not exist"
                        ),
                    }
                })?;
            let batch = read_command_archive_batch(connection, domain, &entry.command.batch_id)?
                .ok_or_else(|| DurableError::Integrity {
                    code: "sqlite_command_archive_reachable_missing".to_owned(),
                    message: format!(
                        "reachable Machine command archive batch {} does not exist",
                        entry.command.batch_id
                    ),
                })?;
            batch
                .verify_entry(&entry)
                .map_err(|error| DurableError::Integrity {
                    code: "sqlite_command_archive_batch_mismatch".to_owned(),
                    message: format!(
                        "reachable Machine command entry does not match its batch: {error}"
                    ),
                })?;
            retained.insert(batch.batch_receipt_id);
        }
        retained.extend(index.archive_entry_ids);
        retained.extend(index.node_ids);
        Ok(retained)
    }

    fn retained_gc_authority(
        connection: &Connection,
        domain: &str,
        expected: &StoreHead,
    ) -> DurableResult<(BTreeSet<String>, BTreeSet<String>)> {
        let manifest = read_required_state_root_manifest(
            connection,
            domain,
            &expected.state_root_manifest_id,
        )?;
        require_manifest_matches_head(&manifest, expected)?;
        let mut root_resolver =
            SqlStateRootResolver::with_manifest(connection, domain, manifest.clone());
        let retained_state_roots = reachable_state_root_objects(&manifest, &mut root_resolver)
            .map_err(state_root_closure_error)?;
        let retained_archives =
            Self::retained_machine_command_objects(connection, domain, expected)?;
        Ok((retained_state_roots, retained_archives))
    }

    fn replay_gc_receipt_page(
        transaction: &Transaction<'_>,
        domain: &str,
        receipt: &GcReceipt,
        retained_state_roots: &BTreeSet<String>,
        retained_archives: &BTreeSet<String>,
    ) -> DurableResult<()> {
        if receipt.reclaimed_ids.contains(&receipt.receipt_id)
            || !receipt.reclaimed_ids.is_disjoint(retained_state_roots)
            || !receipt.reclaimed_ids.is_disjoint(retained_archives)
        {
            return Err(DurableError::Integrity {
                code: "sqlite_gc_receipt_reclaims_current_authority".to_owned(),
                message: "the head-pinned GC receipt authorizes deletion of current authority"
                    .to_owned(),
            });
        }
        delete_receipt_objects(transaction, domain, &receipt.reclaimed_ids)?;
        Ok(())
    }

    fn stats_with_barrier(
        &self,
        after_state_root_snapshot: impl FnOnce() -> DurableResult<()>,
    ) -> DurableResult<StoreStats> {
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?;
        let (archive_segments, archive_entries, archive_batches, index_nodes) =
            command_archive_object_counts(&transaction, &self.domain)?;
        let state_root_objects = count(&transaction, STATE_ROOT_TABLE, &self.domain)?;
        after_state_root_snapshot()?;
        let stats = StoreStats {
            state_root_objects,
            machine_command_archive_segments: archive_segments,
            machine_command_archive_entries: archive_entries,
            machine_command_archive_batches: archive_batches,
            machine_command_index_nodes: index_nodes,
            gc_receipts: count(&transaction, GC_RECEIPT_TABLE, &self.domain)?,
        };
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?;
        Ok(stats)
    }
}

impl DurableStore for SqliteStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        self.read_head()
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<StateRootManifest>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let Some(head) = read_head_record(&transaction, &self.domain)?.map(|record| record.value)
        else {
            transaction
                .commit()
                .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
            return Ok(None);
        };
        if head.state_root_manifest_id != manifest_id {
            transaction
                .commit()
                .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
            return Ok(None);
        }
        let manifest = read_required_state_root_manifest(&transaction, &self.domain, manifest_id)?;
        require_manifest_matches_head(&manifest, &head)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(Some(manifest))
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &StateRootManifest,
        read: impl FnOnce(&mut dyn StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        current.verify()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(current.manifest_id().to_owned()),
                current: None,
            })?;
        if head.state_root_manifest_id != current.manifest_id()
            || head.revision != current.revision()
        {
            return Err(DurableError::Conflict {
                expected: Some(current.manifest_id().to_owned()),
                current: Some(head.state_root_manifest_id),
            });
        }
        let physical =
            read_required_state_root_manifest(&transaction, &self.domain, current.manifest_id())?;
        require_manifest_matches_head(&physical, &head)?;
        if &physical != current {
            return Err(DurableError::Integrity {
                code: "sqlite_state_root_manifest_snapshot_mismatch".to_owned(),
                message: "requested StateRoot manifest does not equal current physical authority"
                    .to_owned(),
            });
        }
        let mut resolver =
            SqlStateRootResolver::with_manifest(&transaction, &self.domain, physical);
        let value = read(&mut resolver)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<ApplicationJournalPrefix> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: None,
            })?;
        if head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
        {
            return Err(DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: Some(head.state_root_manifest_id),
            });
        }
        let physical =
            read_required_state_root_manifest(&transaction, &self.domain, manifest.manifest_id())?;
        require_manifest_matches_head(&physical, &head)?;
        if &physical != manifest {
            return Err(DurableError::Integrity {
                code: "sqlite_state_root_manifest_snapshot_mismatch".to_owned(),
                message: "requested StateRoot manifest does not equal current physical authority"
                    .to_owned(),
            });
        }
        let mut resolver =
            SqlStateRootResolver::with_manifest(&transaction, &self.domain, physical);
        let prefix = load_application_journal_prefix(manifest, &mut resolver, journal_id, count)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(prefix)
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<JournalRecordManifest>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: None,
            })?;
        if head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
        {
            return Err(DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: Some(head.state_root_manifest_id),
            });
        }
        let physical =
            read_required_state_root_manifest(&transaction, &self.domain, manifest.manifest_id())?;
        require_manifest_matches_head(&physical, &head)?;
        if &physical != manifest {
            return Err(DurableError::Integrity {
                code: "sqlite_state_root_manifest_snapshot_mismatch".to_owned(),
                message: "requested StateRoot manifest does not equal current physical authority"
                    .to_owned(),
            });
        }
        let mut resolver =
            SqlStateRootResolver::with_manifest(&transaction, &self.domain, physical);
        let value = load_application_journal_record_manifest(
            manifest,
            &mut resolver,
            journal_id,
            record_id,
        )?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<ApplicationJournalPrefixReplacementAuthority>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: None,
            })?;
        if head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
        {
            return Err(DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: Some(head.state_root_manifest_id),
            });
        }
        let physical =
            read_required_state_root_manifest(&transaction, &self.domain, manifest.manifest_id())?;
        require_manifest_matches_head(&physical, &head)?;
        if &physical != manifest {
            return Err(DurableError::Integrity {
                code: "sqlite_state_root_manifest_snapshot_mismatch".to_owned(),
                message: "requested StateRoot manifest does not equal current physical authority"
                    .to_owned(),
            });
        }
        let mut resolver =
            SqlStateRootResolver::with_manifest(&transaction, &self.domain, physical);
        let value = load_application_journal_prefix_replacement_authority(
            manifest,
            &mut resolver,
            replacement_id,
        )?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: None,
            })?;
        if head.state_root_manifest_id != manifest.manifest_id()
            || head.revision != manifest.revision()
        {
            return Err(DurableError::Conflict {
                expected: Some(manifest.manifest_id().to_owned()),
                current: Some(head.state_root_manifest_id),
            });
        }
        let physical =
            read_required_state_root_manifest(&transaction, &self.domain, manifest.manifest_id())?;
        require_manifest_matches_head(&physical, &head)?;
        if &physical != manifest {
            return Err(DurableError::Integrity {
                code: "sqlite_state_root_manifest_snapshot_mismatch".to_owned(),
                message: "requested StateRoot manifest does not equal current physical authority"
                    .to_owned(),
            });
        }
        let mut resolver =
            SqlStateRootResolver::with_manifest(&transaction, &self.domain, physical);
        let value = load_coupled_checkpoint_receipt(manifest, &mut resolver, coupling_id)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_STATE_ROOT_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        let value = read_command_archive_segment(&transaction, &self.domain, segment_id)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveEntry>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        let value = read_command_archive_entry(&transaction, &self.domain, entry_id)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<MachineCommandBatchRecord>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        let value = read_command_archive_batch(&transaction, &self.domain, batch_id)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<MachineCommandIndexNode>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        let value = read_command_index_node(&transaction, &self.domain, node_id)?;
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        Ok(value)
    }

    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        anchor.verify()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        let head = read_head_record(&transaction, &self.domain)?
            .map(|record| record.value)
            .ok_or_else(|| DurableError::Conflict {
                expected: Some(anchor.anchor_id.clone()),
                current: None,
            })?;
        if head.machine_base_anchor.as_ref() != Some(anchor) {
            return Err(DurableError::Conflict {
                expected: Some(anchor.anchor_id.clone()),
                current: head
                    .machine_base_anchor
                    .as_ref()
                    .map(|current| current.anchor_id.clone()),
            });
        }
        let manifest = read_required_state_root_manifest(
            &transaction,
            &self.domain,
            &head.state_root_manifest_id,
        )?;
        require_manifest_matches_head(&manifest, &head)?;
        let mut store_error = None;
        let index_proof = cymule_core::resolve_machine_command_index_proof(
            &anchor.command_index_root,
            command_id,
            |node_id| match read_command_index_node(&transaction, &self.domain, node_id) {
                Ok(Some(value)) => Ok(Some(value)),
                Ok(None) => {
                    store_error = Some(DurableError::Integrity {
                        code: "sqlite_command_archive_reachable_missing".to_owned(),
                        message: format!(
                            "reachable Machine command-index node {node_id} does not exist"
                        ),
                    });
                    Err(cymule_core::CoreError::NotFound(
                        "SQLite command-index node is missing".to_owned(),
                    ))
                }
                Err(error) => {
                    store_error = Some(error);
                    Err(cymule_core::CoreError::NotFound(
                        "SQLite command-index lookup failed".to_owned(),
                    ))
                }
            },
        );
        if let Some(error) = store_error {
            return Err(error);
        }
        let index_proof = index_proof?;
        let lookup = match index_proof.value.as_ref() {
            None => cymule_core::MachineCommandArchiveLookup::NonMember { index_proof },
            Some(value) => {
                let entry = read_command_archive_entry(
                    &transaction,
                    &self.domain,
                    &value.archive_entry_digest,
                )?
                .ok_or_else(|| DurableError::Integrity {
                    code: "sqlite_command_archive_reachable_missing".to_owned(),
                    message: format!(
                        "reachable Machine command archive entry {} does not exist",
                        value.archive_entry_digest
                    ),
                })?;
                if entry.identity()? != value.archive_entry_digest {
                    return Err(DurableError::Integrity {
                        code: COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE.to_owned(),
                        message: format!(
                            "Machine command archive entry {} does not match its index",
                            value.archive_entry_digest
                        ),
                    });
                }
                cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof,
                    entry: Box::new(entry),
                }
            }
        };
        transaction
            .commit()
            .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
        Ok(lookup)
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        self.compare_and_commit_with_response(expected, batch, || Ok(()))
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        self.reconcile_cold_reclamation_with_barriers(request.expected_head(), |_| Ok(()))
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        self.advance_cold_reclamation_with_barriers(request.expected_head(), |_| Ok(()))
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.stats_with_barrier(|| Ok(()))
    }
}

#[derive(Debug)]
struct HeadRecord {
    value: StoreHead,
    canonical_bytes: Vec<u8>,
}

struct SqlStateRootResolver<'a> {
    connection: &'a Connection,
    domain: &'a str,
    pinned_manifest_id: String,
    cache: BTreeMap<String, StateRootObject>,
}

impl<'a> SqlStateRootResolver<'a> {
    fn with_manifest(
        connection: &'a Connection,
        domain: &'a str,
        manifest: StateRootManifest,
    ) -> Self {
        let pinned_manifest_id = manifest.manifest_id().to_owned();
        Self {
            connection,
            domain,
            pinned_manifest_id: pinned_manifest_id.clone(),
            cache: BTreeMap::from([(pinned_manifest_id, StateRootObject::Manifest(manifest))]),
        }
    }
}

impl StateRootResolver for SqlStateRootResolver<'_> {
    fn pinned_manifest_id(&self) -> &str {
        &self.pinned_manifest_id
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        if let Some(object) = self.cache.get(object_id) {
            return Ok(Some(object.clone()));
        }
        let Some(object) = read_state_root_object(self.connection, self.domain, object_id)? else {
            return Ok(None);
        };
        self.cache.insert(object_id.to_owned(), object.clone());
        Ok(Some(object))
    }
}

fn initialize_empty(connection: &mut Connection) -> DurableResult<()> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| contention(&error, None, SQLITE_SCHEMA_FAILURE_CODE))?;
    if non_sqlite_schema_object_count(&transaction)? == 0 {
        for (_, ddl) in CURRENT_SCHEMA {
            transaction
                .execute_batch(ddl)
                .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))?;
        }
        transaction
            .execute(
                "INSERT INTO cymule_store_meta(singleton, schema_version) VALUES (1, ?1)",
                [SCHEMA],
            )
            .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))?;
    } else {
        require_current(&transaction)?;
    }
    transaction
        .commit()
        .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))
}

fn require_current(connection: &Connection) -> DurableResult<()> {
    let object_count = non_sqlite_schema_object_count(connection)?;
    if object_count != u64::try_from(CURRENT_SCHEMA.len()).expect("schema object count fits u64") {
        return Err(unsupported_generation(&format!(
            "SQLite non-system schema object set is not the exact {SCHEMA} authority"
        )));
    }
    for (table, expected_ddl) in CURRENT_SCHEMA {
        let exact: Option<bool> = connection
            .query_row(
                "SELECT type = 'table' AND tbl_name = ?2 AND sql = ?3
                 FROM sqlite_schema WHERE name = ?1",
                params![table, table, expected_ddl],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))?;
        if exact != Some(true) {
            return Err(unsupported_generation(&format!(
                "SQLite table {table} does not match the exact {SCHEMA} DDL"
            )));
        }
    }
    let schema_is_current: Option<bool> = connection
        .query_row(
            "SELECT schema_version = ?1 FROM cymule_store_meta WHERE singleton = 1",
            [SCHEMA],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))?;
    if schema_is_current != Some(true) {
        return Err(unsupported_generation(&format!(
            "SQLite durable metadata is not the exact {SCHEMA} generation"
        )));
    }
    Ok(())
}

fn non_sqlite_schema_object_count(connection: &Connection) -> DurableResult<u64> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE name NOT GLOB 'sqlite_*' AND tbl_name NOT GLOB 'sqlite_*'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_operation_error(SQLITE_SCHEMA_FAILURE_CODE, error))?;
    u64::try_from(count).map_err(|error| DurableError::Integrity {
        code: "sqlite_schema_object_count".to_owned(),
        message: error.to_string(),
    })
}

fn read_head_record(connection: &Connection, domain: &str) -> DurableResult<Option<HeadRecord>> {
    let maximum = i64::try_from(MAX_STORE_HEAD_BYTES)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let row: Option<(i64, Option<Vec<u8>>)> = connection
        .query_row(
            "SELECT length(head_json),
                    CASE WHEN length(head_json) <= ?2 THEN head_json ELSE NULL END
             FROM cymule_heads WHERE domain = ?1",
            params![domain, maximum],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| sqlite_operation_error(SQLITE_HEAD_READ_FAILURE_CODE, error))?;
    bounded_blob(
        row,
        MAX_STORE_HEAD_BYTES,
        HEAD_INTEGRITY_CODE,
        "SQLite head",
    )?
    .map(|bytes| {
        let value: StoreHead = decode_persisted(&bytes, HEAD_INTEGRITY_CODE, "SQLite head")?;
        value.verify().map_err(|error| DurableError::Integrity {
            code: HEAD_INTEGRITY_CODE.to_owned(),
            message: format!("SQLite head failed verification: {error}"),
        })?;
        Ok(HeadRecord {
            value,
            canonical_bytes: bytes,
        })
    })
    .transpose()
}

fn write_head_cas(
    transaction: &Transaction<'_>,
    domain: &str,
    expected: Option<&HeadRecord>,
    next: &StoreHead,
) -> DurableResult<()> {
    next.verify()?;
    let next_bytes = canonical(next)?;
    let changed = match expected {
        None => transaction
            .execute(
                "INSERT INTO cymule_heads(domain, head_json) VALUES (?1, ?2)",
                params![domain, next_bytes],
            )
            .map_err(|error| sqlite_operation_error(SQLITE_HEAD_CAS_FAILURE_CODE, error))?,
        Some(expected) => transaction
            .execute(
                "UPDATE cymule_heads SET head_json = ?1
                 WHERE domain = ?2 AND head_json = ?3",
                params![next_bytes, domain, expected.canonical_bytes],
            )
            .map_err(|error| sqlite_operation_error(SQLITE_HEAD_CAS_FAILURE_CODE, error))?,
    };
    if changed != 1 {
        return Err(DurableError::Conflict {
            expected: expected.map(|record| record.value.physical_token.clone()),
            current: read_head_record(transaction, domain)?
                .map(|record| record.value.physical_token),
        });
    }
    let retained =
        read_head_record(transaction, domain)?.ok_or_else(|| DurableError::Integrity {
            code: HEAD_INTEGRITY_CODE.to_owned(),
            message: "SQLite head disappeared inside its CAS transaction".to_owned(),
        })?;
    if retained.value != *next || retained.canonical_bytes != canonical(next)? {
        return Err(DurableError::Integrity {
            code: HEAD_INTEGRITY_CODE.to_owned(),
            message: "SQLite head CAS did not retain the exact canonical next head".to_owned(),
        });
    }
    Ok(())
}

fn require_manifest_matches_head(
    manifest: &StateRootManifest,
    head: &StoreHead,
) -> DurableResult<()> {
    if manifest.manifest_id() != head.state_root_manifest_id
        || manifest.revision() != head.revision
        || manifest.sequence() != head.sequence
        || manifest.machine_base_anchor() != head.machine_base_anchor.as_ref()
    {
        return Err(DurableError::Integrity {
            code: "sqlite_state_root_head_mismatch".to_owned(),
            message: "SQLite head does not match its exact StateRoot manifest".to_owned(),
        });
    }
    Ok(())
}

fn read_required_state_root_manifest(
    connection: &Connection,
    domain: &str,
    manifest_id: &str,
) -> DurableResult<StateRootManifest> {
    match read_state_root_object(connection, domain, manifest_id)? {
        Some(StateRootObject::Manifest(manifest)) => Ok(manifest),
        Some(_) => Err(DurableError::Integrity {
            code: STATE_ROOT_MANIFEST_KIND_INTEGRITY_CODE.to_owned(),
            message: format!(
                "SQLite StateRoot manifest locator {manifest_id} resolves to another object kind"
            ),
        }),
        None => Err(DurableError::Integrity {
            code: "sqlite_state_root_manifest_missing".to_owned(),
            message: format!("SQLite StateRoot manifest {manifest_id} does not exist"),
        }),
    }
}

fn read_state_root_object(
    connection: &Connection,
    domain: &str,
    id: &str,
) -> DurableResult<Option<StateRootObject>> {
    let value = read_canonical_object_bytes(connection, domain, id, STATE_ROOT_READ_SPEC)?
        .map(|bytes| {
            decode_state_root_object(&bytes).map_err(|error| DurableError::Integrity {
                code: "sqlite_state_root_object_bytes".to_owned(),
                message: format!(
                    "SQLite StateRoot object is not a valid bounded physical envelope: {error}"
                ),
            })
        })
        .transpose()?;
    if let Some(value) = &value {
        value.verify().map_err(|error| DurableError::Integrity {
            code: STATE_ROOT_LOCATOR_INTEGRITY_CODE.to_owned(),
            message: format!("SQLite StateRoot object {id} failed verification: {error}"),
        })?;
        if value.object_id() != id {
            return Err(DurableError::Integrity {
                code: STATE_ROOT_LOCATOR_INTEGRITY_CODE.to_owned(),
                message: format!(
                    "SQLite StateRoot object locator {id} resolves to {}",
                    value.object_id()
                ),
            });
        }
    }
    Ok(value)
}

fn insert_state_root_object(
    transaction: &Transaction<'_>,
    domain: &str,
    object: &StateRootObject,
) -> DurableResult<()> {
    object.verify()?;
    insert_canonical_object(
        transaction,
        STATE_ROOT_TABLE,
        domain,
        object.object_id(),
        object,
        MAX_STATE_ROOT_OBJECT_BYTES,
        SQLITE_STATE_ROOT_FAILURE_CODE,
    )
}

fn read_command_archive_object(
    connection: &Connection,
    domain: &str,
    id: &str,
) -> DurableResult<Option<MachineCommandArchiveObject>> {
    let Some(bytes) =
        read_canonical_object_bytes(connection, domain, id, COMMAND_ARCHIVE_READ_SPEC)?
    else {
        return Ok(None);
    };
    let value: MachineCommandArchiveObject = decode_persisted(
        &bytes,
        "sqlite_command_archive_object_bytes",
        "SQLite Machine command archive object",
    )?;
    let identity = value.identity().map_err(|error| DurableError::Integrity {
        code: COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE.to_owned(),
        message: format!("SQLite Machine command archive object is invalid: {error}"),
    })?;
    if identity != id {
        return Err(DurableError::Integrity {
            code: COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE.to_owned(),
            message: format!("SQLite Machine command archive locator {id} resolves to {identity}"),
        });
    }
    let indexed_batch_id = read_command_archive_object_batch_id(connection, domain, id)?;
    match (&value, indexed_batch_id.as_deref()) {
        (MachineCommandArchiveObject::Batch(batch), Some(batch_id))
            if batch.batch_id == batch_id => {}
        (MachineCommandArchiveObject::Batch(_), _) => {
            return Err(DurableError::Integrity {
                code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                message: format!(
                    "SQLite Machine command batch receipt {id} changed its stable batch index"
                ),
            });
        }
        (_, None) => {}
        (_, Some(_)) => {
            return Err(DurableError::Integrity {
                code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                message: format!(
                    "SQLite non-batch command archive object {id} carries a batch index"
                ),
            });
        }
    }
    Ok(Some(value))
}

fn insert_command_archive_object(
    transaction: &Transaction<'_>,
    domain: &str,
    object: &MachineCommandArchiveObject,
) -> DurableResult<()> {
    let identity = object.identity()?;
    require_identity_unaliased(transaction, domain, &identity, COMMAND_ARCHIVE_TABLE)?;
    let batch_id = match object {
        MachineCommandArchiveObject::Batch(batch) => Some(batch.batch_id.as_str()),
        _ => None,
    };
    if let Some(batch_id) = batch_id
        && let Some(retained) =
            read_command_archive_batch_receipt_id(transaction, domain, batch_id)?
        && retained != identity
    {
        return Err(DurableError::Integrity {
            code: "sqlite_command_archive_batch_index_conflict".to_owned(),
            message: format!(
                "Machine command batch {batch_id} already resolves to another receipt {retained}"
            ),
        });
    }
    let bytes = canonical(object)?;
    if bytes.len() > MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES {
        return Err(DurableError::Validation(format!(
            "{identity} exceeds its {MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES}-byte canonical physical-object bound"
        )));
    }
    transaction
        .execute(
            "INSERT INTO cymule_machine_command_archive_objects(
                domain, object_id, batch_id, object_json
             ) VALUES (?1, ?2, ?3, ?4) ON CONFLICT DO NOTHING",
            params![domain, identity, batch_id, bytes],
        )
        .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
    let retained =
        read_command_archive_object(transaction, domain, &identity)?.ok_or_else(|| {
            DurableError::Integrity {
                code: "sqlite_command_archive_object_missing".to_owned(),
                message: format!(
                    "SQLite Machine command archive object {identity} disappeared after insertion"
                ),
            }
        })?;
    if retained != *object {
        return Err(DurableError::Integrity {
            code: "sqlite_command_archive_object_bytes_conflict".to_owned(),
            message: format!(
                "SQLite Machine command archive object {identity} has conflicting canonical bytes"
            ),
        });
    }
    Ok(())
}

fn read_command_archive_object_batch_id(
    connection: &Connection,
    domain: &str,
    object_id: &str,
) -> DurableResult<Option<String>> {
    let content_id_length = physical_content_id_length()?;
    let row: Option<(Option<i64>, Option<String>)> = connection
        .query_row(
            "SELECT length(batch_id),
                    CASE WHEN length(batch_id) = ?3 THEN batch_id ELSE NULL END
             FROM cymule_machine_command_archive_objects
             WHERE domain = ?1 AND object_id = ?2",
            params![domain, object_id, content_id_length],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
    let Some((reported_length, value)) = row else {
        return Ok(None);
    };
    match (reported_length, value) {
        (None, None) => Ok(None),
        (Some(reported_length), value) => {
            let value = decode_physical_locator(reported_length, value, content_id_length)?;
            cymule_core::validate_content_id("Machine command batch", &value)?;
            Ok(Some(value))
        }
        (None, Some(_)) => Err(DurableError::Integrity {
            code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
            message: "SQLite command-batch index has bytes without a physical length".to_owned(),
        }),
    }
}

fn read_command_archive_batch_receipt_id(
    connection: &Connection,
    domain: &str,
    batch_id: &str,
) -> DurableResult<Option<String>> {
    cymule_core::validate_content_id("Machine command batch", batch_id)?;
    let content_id_length = physical_content_id_length()?;
    let row: Option<(i64, Option<String>)> = connection
        .query_row(
            "SELECT length(object_id),
                    CASE WHEN length(object_id) = ?3 THEN object_id ELSE NULL END
             FROM cymule_machine_command_archive_objects
             WHERE domain = ?1 AND batch_id = ?2",
            params![domain, batch_id, content_id_length],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| sqlite_operation_error(SQLITE_ARCHIVE_FAILURE_CODE, error))?;
    row.map(|(reported_length, value)| {
        decode_physical_locator(reported_length, value, content_id_length)
    })
    .transpose()
}

fn read_command_archive_batch(
    connection: &Connection,
    domain: &str,
    batch_id: &str,
) -> DurableResult<Option<MachineCommandBatchRecord>> {
    let Some(receipt_id) = read_command_archive_batch_receipt_id(connection, domain, batch_id)?
    else {
        return Ok(None);
    };
    let batch = read_command_archive_object(connection, domain, &receipt_id)?.ok_or_else(|| {
        DurableError::Integrity {
            code: "sqlite_command_archive_batch_index_dangling".to_owned(),
            message: format!(
                "Machine command batch {batch_id} points to missing receipt {receipt_id}"
            ),
        }
    })?;
    let MachineCommandArchiveObject::Batch(batch) = batch else {
        return Err(DurableError::Integrity {
            code: "sqlite_command_archive_object_type".to_owned(),
            message: format!(
                "SQLite command batch index {batch_id} resolves to a non-batch object"
            ),
        });
    };
    if batch.batch_id != batch_id || batch.batch_receipt_id != receipt_id {
        return Err(DurableError::Integrity {
            code: "sqlite_command_archive_batch_identity_mismatch".to_owned(),
            message: format!(
                "Machine command batch {batch_id} changed its stable or receipt identity"
            ),
        });
    }
    Ok(Some(*batch))
}

fn read_command_archive_segment(
    connection: &Connection,
    domain: &str,
    id: &str,
) -> DurableResult<Option<MachineCommandArchiveSegment>> {
    read_command_archive_object(connection, domain, id)?
        .map(|value| match value {
            MachineCommandArchiveObject::Segment(value) => Ok(*value),
            _ => Err(DurableError::Integrity {
                code: "sqlite_command_archive_object_type".to_owned(),
                message: format!("SQLite command archive object {id} is not a segment"),
            }),
        })
        .transpose()
}

fn read_command_archive_entry(
    connection: &Connection,
    domain: &str,
    id: &str,
) -> DurableResult<Option<MachineCommandArchiveEntry>> {
    read_command_archive_object(connection, domain, id)?
        .map(|value| match value {
            MachineCommandArchiveObject::Entry(value) => Ok(*value),
            _ => Err(DurableError::Integrity {
                code: "sqlite_command_archive_object_type".to_owned(),
                message: format!("SQLite command archive object {id} is not an entry"),
            }),
        })
        .transpose()
}

fn read_command_index_node(
    connection: &Connection,
    domain: &str,
    id: &str,
) -> DurableResult<Option<MachineCommandIndexNode>> {
    read_command_archive_object(connection, domain, id)?
        .map(|value| match value {
            MachineCommandArchiveObject::CommandIndexNode(value) => Ok(value),
            _ => Err(DurableError::Integrity {
                code: "sqlite_command_archive_object_type".to_owned(),
                message: format!("SQLite command archive object {id} is not an index node"),
            }),
        })
        .transpose()
}

fn verify_gc_receipt(
    connection: &Connection,
    domain: &str,
    head: &StoreHead,
) -> DurableResult<Option<GcReceipt>> {
    let Some(receipt_id) = &head.gc_receipt else {
        return Ok(None);
    };
    #[cfg(test)]
    GC_RECEIPT_READ_COUNT.with(|count| count.set(count.get() + 1));
    let receipt: GcReceipt =
        read_canonical_object(connection, domain, receipt_id, GC_RECEIPT_READ_SPEC)?.ok_or_else(
            || DurableError::Integrity {
                code: "sqlite_gc_receipt_missing".to_owned(),
                message: format!("SQLite GC receipt {receipt_id} does not exist"),
            },
        )?;
    if receipt.receipt_id != *receipt_id {
        return Err(DurableError::Integrity {
            code: GC_RECEIPT_LOCATOR_INTEGRITY_CODE.to_owned(),
            message: "SQLite GC receipt locator does not match its content identity".to_owned(),
        });
    }
    receipt
        .verify_for(head)
        .map_err(|error| DurableError::Integrity {
            code: GC_RECEIPT_LOCATOR_INTEGRITY_CODE.to_owned(),
            message: format!("SQLite GC receipt does not match its head: {error}"),
        })?;
    Ok(Some(receipt))
}

fn insert_gc_receipt(
    transaction: &Transaction<'_>,
    domain: &str,
    receipt: &GcReceipt,
) -> DurableResult<()> {
    insert_canonical_object(
        transaction,
        GC_RECEIPT_TABLE,
        domain,
        &receipt.receipt_id,
        receipt,
        MAX_GC_RECEIPT_BYTES,
        SQLITE_GC_SWEEP_FAILURE_CODE,
    )
}

fn audit_gc_receipt_inventory(connection: &Connection, domain: &str) -> DurableResult<()> {
    let content_id_length = physical_content_id_length()?;
    let maximum = i64::try_from(MAX_GC_RECEIPT_BYTES)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let mut statement = connection
        .prepare(
            "SELECT length(object_id),
                    CASE WHEN length(object_id) = ?2 THEN object_id ELSE NULL END,
                    length(object_json),
                    CASE WHEN length(object_json) <= ?3 THEN object_json ELSE NULL END
             FROM cymule_gc_receipts WHERE domain = ?1 ORDER BY object_id",
        )
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    let mut rows = statement
        .query(params![domain, content_id_length, maximum])
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?
    {
        let receipt_id = decode_physical_locator(
            row.get(0)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?,
            row.get(1)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?,
            content_id_length,
        )?;
        let bytes = bounded_blob(
            Some((
                row.get(2).map_err(|error| {
                    sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error)
                })?,
                row.get(3).map_err(|error| {
                    sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error)
                })?,
            )),
            MAX_GC_RECEIPT_BYTES,
            "sqlite_gc_receipt_bytes",
            "SQLite GC receipt",
        )?
        .expect("inventory row exists");
        let receipt: GcReceipt =
            decode_persisted(&bytes, "sqlite_gc_receipt_bytes", "SQLite GC receipt")?;
        if receipt.receipt_id != receipt_id {
            return Err(DurableError::Integrity {
                code: GC_RECEIPT_LOCATOR_INTEGRITY_CODE.to_owned(),
                message: format!(
                    "SQLite GC receipt locator {receipt_id} resolves to {}",
                    receipt.receipt_id
                ),
            });
        }
        receipt
            .verify_identity()
            .map_err(|error| DurableError::Integrity {
                code: "sqlite_gc_receipt_identity".to_owned(),
                message: format!("SQLite GC receipt {receipt_id} is invalid: {error}"),
            })?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ObjectReadSpec {
    table: &'static str,
    maximum_bytes: usize,
    integrity_code: &'static str,
    label: &'static str,
    substrate_code: &'static str,
}

const STATE_ROOT_READ_SPEC: ObjectReadSpec = ObjectReadSpec {
    table: STATE_ROOT_TABLE,
    maximum_bytes: MAX_STATE_ROOT_OBJECT_BYTES,
    integrity_code: "sqlite_state_root_object_bytes",
    label: "SQLite StateRoot object",
    substrate_code: SQLITE_STATE_ROOT_FAILURE_CODE,
};

const COMMAND_ARCHIVE_READ_SPEC: ObjectReadSpec = ObjectReadSpec {
    table: COMMAND_ARCHIVE_TABLE,
    maximum_bytes: MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES,
    integrity_code: "sqlite_command_archive_object_bytes",
    label: "SQLite Machine command archive object",
    substrate_code: SQLITE_ARCHIVE_FAILURE_CODE,
};

const GC_RECEIPT_READ_SPEC: ObjectReadSpec = ObjectReadSpec {
    table: GC_RECEIPT_TABLE,
    maximum_bytes: MAX_GC_RECEIPT_BYTES,
    integrity_code: "sqlite_gc_receipt_bytes",
    label: "SQLite GC receipt",
    substrate_code: SQLITE_GC_INVENTORY_FAILURE_CODE,
};

fn read_canonical_object<T: DeserializeOwned + Serialize>(
    connection: &Connection,
    domain: &str,
    id: &str,
    spec: ObjectReadSpec,
) -> DurableResult<Option<T>> {
    read_canonical_object_bytes(connection, domain, id, spec)?
        .map(|bytes| decode_persisted(&bytes, spec.integrity_code, spec.label))
        .transpose()
}

fn read_canonical_object_bytes(
    connection: &Connection,
    domain: &str,
    id: &str,
    spec: ObjectReadSpec,
) -> DurableResult<Option<Vec<u8>>> {
    let maximum = i64::try_from(spec.maximum_bytes)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let table = spec.table;
    let sql = format!(
        "SELECT length(object_json),
                CASE WHEN length(object_json) <= ?3 THEN object_json ELSE NULL END
         FROM {table} WHERE domain = ?1 AND object_id = ?2"
    );
    let row: Option<(i64, Option<Vec<u8>>)> = connection
        .query_row(&sql, params![domain, id, maximum], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(|error| sqlite_operation_error(spec.substrate_code, error))?;
    let bytes = bounded_blob(row, spec.maximum_bytes, spec.integrity_code, spec.label)?;
    if bytes.is_some() {
        require_identity_unaliased(connection, domain, id, table)?;
    }
    Ok(bytes)
}

fn bounded_blob(
    row: Option<(i64, Option<Vec<u8>>)>,
    maximum_bytes: usize,
    integrity_code: &str,
    label: &str,
) -> DurableResult<Option<Vec<u8>>> {
    let Some((reported_length, bytes)) = row else {
        return Ok(None);
    };
    let reported_length =
        usize::try_from(reported_length).map_err(|error| DurableError::Integrity {
            code: integrity_code.to_owned(),
            message: format!("{label} has an invalid physical BLOB length: {error}"),
        })?;
    if reported_length > maximum_bytes {
        return Err(DurableError::Integrity {
            code: integrity_code.to_owned(),
            message: format!(
                "{label} exceeds its {maximum_bytes}-byte canonical physical-object bound"
            ),
        });
    }
    let bytes = bytes.ok_or_else(|| DurableError::Integrity {
        code: integrity_code.to_owned(),
        message: format!("{label} did not return its admitted bounded physical BLOB"),
    })?;
    if bytes.len() != reported_length {
        return Err(DurableError::Integrity {
            code: integrity_code.to_owned(),
            message: format!("{label} physical BLOB length changed during its SQLite snapshot"),
        });
    }
    Ok(Some(bytes))
}

fn decode_persisted<T: DeserializeOwned + Serialize>(
    bytes: &[u8],
    integrity_code: &str,
    label: &str,
) -> DurableResult<T> {
    let value: T = cymule_core::decode_json(bytes).map_err(|error| DurableError::Integrity {
        code: integrity_code.to_owned(),
        message: format!("{label} is not strict canonical JSON: {error}"),
    })?;
    let expected = canonical(&value).map_err(|error| DurableError::Integrity {
        code: integrity_code.to_owned(),
        message: format!("{label} cannot be canonically encoded: {error}"),
    })?;
    if expected != bytes {
        return Err(DurableError::Integrity {
            code: integrity_code.to_owned(),
            message: format!("{label} bytes are not the exact canonical encoding"),
        });
    }
    Ok(value)
}

fn insert_canonical_object<T: Serialize>(
    transaction: &Transaction<'_>,
    table: &str,
    domain: &str,
    id: &str,
    value: &T,
    maximum_bytes: usize,
    substrate_code: &'static str,
) -> DurableResult<()> {
    require_identity_unaliased(transaction, domain, id, table)?;
    let bytes = canonical(value)?;
    if bytes.len() > maximum_bytes {
        return Err(DurableError::Validation(format!(
            "{id} exceeds its {maximum_bytes}-byte canonical physical-object bound"
        )));
    }
    let sql = format!(
        "INSERT INTO {table}(domain, object_id, object_json) VALUES (?1, ?2, ?3)
         ON CONFLICT(domain, object_id) DO NOTHING"
    );
    transaction
        .execute(&sql, params![domain, id, bytes])
        .map_err(|error| sqlite_operation_error(substrate_code, error))?;
    let maximum = i64::try_from(maximum_bytes)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let retained_sql = format!(
        "SELECT length(object_json),
                CASE WHEN length(object_json) <= ?3 THEN object_json ELSE NULL END
         FROM {table} WHERE domain = ?1 AND object_id = ?2"
    );
    let retained_row: Option<(i64, Option<Vec<u8>>)> = transaction
        .query_row(&retained_sql, params![domain, id, maximum], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(|error| sqlite_operation_error(substrate_code, error))?;
    let retained = bounded_blob(
        retained_row,
        maximum_bytes,
        "sqlite_immutable_object_bytes_conflict",
        "immutable SQLite object",
    )?
    .ok_or_else(|| DurableError::Integrity {
        code: "sqlite_immutable_object_missing".to_owned(),
        message: format!("immutable SQLite object {id} disappeared"),
    })?;
    let expected = canonical(value)?;
    if retained != expected {
        return Err(DurableError::Integrity {
            code: "sqlite_immutable_object_bytes_conflict".to_owned(),
            message: format!("immutable SQLite object {id} has conflicting canonical bytes"),
        });
    }
    Ok(())
}

struct GcCandidateInventory {
    prefix: BTreeSet<String>,
    remaining_objects: u64,
}

fn collect_gc_candidate_inventory(
    connection: &Connection,
    domain: &str,
    retained_state_roots: &BTreeSet<String>,
    retained_archives: &BTreeSet<String>,
    pinned_receipt_id: Option<&str>,
) -> DurableResult<GcCandidateInventory> {
    let content_id_length = physical_content_id_length()?;
    let mut statement = connection
        .prepare(
            "SELECT family, length(object_id),
                    CASE WHEN length(object_id) = ?2 THEN object_id ELSE NULL END
             FROM (
                 SELECT 0 AS family, object_id
                 FROM cymule_state_root_objects WHERE domain = ?1
                 UNION ALL
                 SELECT 1 AS family, object_id
                 FROM cymule_machine_command_archive_objects WHERE domain = ?1
                 UNION ALL
                 SELECT 2 AS family, object_id
                 FROM cymule_gc_receipts WHERE domain = ?1
             )
             ORDER BY object_id, family",
        )
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    let mut rows = statement
        .query(params![domain, content_id_length])
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    let mut receipt_prefix = pinned_receipt_id
        .map(|receipt_id| BTreeSet::from([receipt_id.to_owned()]))
        .unwrap_or_default();
    let mut other_prefix = BTreeSet::new();
    let mut candidate_count = 0_u64;
    let mut previous_object = None;
    let mut pinned_receipt_observed = pinned_receipt_id.is_none();
    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?
    {
        let family = row
            .get::<_, i64>(0)
            .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
        let object_id = decode_physical_locator(
            row.get(1)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?,
            row.get(2)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?,
            content_id_length,
        )?;
        if previous_object.as_deref() == Some(object_id.as_str()) {
            return Err(DurableError::Integrity {
                code: "sqlite_gc_cross_family_identity_alias".to_owned(),
                message: format!(
                    "SQLite physical object identity {object_id} exists in more than one family"
                ),
            });
        }
        previous_object = Some(object_id.clone());
        let retained = match family {
            0 => retained_state_roots.contains(&object_id),
            1 => retained_archives.contains(&object_id),
            2 => false,
            _ => {
                return Err(DurableError::Integrity {
                    code: "sqlite_gc_object_family".to_owned(),
                    message: format!("SQLite GC observed unknown object family {family}"),
                });
            }
        };
        if !retained {
            candidate_count =
                candidate_count
                    .checked_add(1)
                    .ok_or_else(|| DurableError::Integrity {
                        code: "sqlite_gc_candidate_count_overflow".to_owned(),
                        message: "SQLite GC candidate count overflowed".to_owned(),
                    })?;
            if family == 2 {
                pinned_receipt_observed |= pinned_receipt_id == Some(object_id.as_str());
                if pinned_receipt_id != Some(object_id.as_str())
                    && receipt_prefix.len() < MAX_GC_RECLAIMED_OBJECTS
                {
                    receipt_prefix.insert(object_id);
                }
            } else if other_prefix.len() < MAX_GC_RECLAIMED_OBJECTS {
                other_prefix.insert(object_id);
            }
        }
    }
    if !pinned_receipt_observed {
        return Err(DurableError::Integrity {
            code: "sqlite_gc_receipt_missing".to_owned(),
            message: "the head-pinned GC receipt disappeared during inventory".to_owned(),
        });
    }
    finish_gc_candidate_inventory(receipt_prefix, other_prefix, candidate_count)
}

fn finish_gc_candidate_inventory(
    receipt_prefix: BTreeSet<String>,
    other_prefix: BTreeSet<String>,
    candidate_count: u64,
) -> DurableResult<GcCandidateInventory> {
    let mut prefix = receipt_prefix;
    let remaining_slots = MAX_GC_RECLAIMED_OBJECTS - prefix.len();
    prefix.extend(other_prefix.into_iter().take(remaining_slots));
    let prefix_count = u64::try_from(prefix.len()).map_err(|error| DurableError::Integrity {
        code: "sqlite_gc_candidate_count".to_owned(),
        message: error.to_string(),
    })?;
    let remaining_objects =
        candidate_count
            .checked_sub(prefix_count)
            .ok_or_else(|| DurableError::Integrity {
                code: "sqlite_gc_candidate_count_mismatch".to_owned(),
                message: "SQLite GC prefix exceeds its candidate count".to_owned(),
            })?;
    Ok(GcCandidateInventory {
        prefix,
        remaining_objects,
    })
}

fn delete_receipt_objects(
    transaction: &Transaction<'_>,
    domain: &str,
    authorized: &BTreeSet<String>,
) -> DurableResult<u64> {
    const DELETE_BATCH_SIZE: usize = 512;
    audit_authorized_object_families(transaction, domain, authorized, DELETE_BATCH_SIZE)?;
    let mut deleted_total = 0_u64;
    for table in [STATE_ROOT_TABLE, COMMAND_ARCHIVE_TABLE, GC_RECEIPT_TABLE] {
        let mut remaining = authorized.iter();
        loop {
            let batch = remaining
                .by_ref()
                .take(DELETE_BATCH_SIZE)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let placeholders = std::iter::repeat_n("?", batch.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql =
                format!("DELETE FROM {table} WHERE domain = ? AND object_id IN ({placeholders})");
            let mut parameters = Vec::<&dyn rusqlite::ToSql>::with_capacity(batch.len() + 1);
            parameters.push(&domain);
            parameters.extend(
                batch
                    .iter()
                    .map(|object_id| *object_id as &dyn rusqlite::ToSql),
            );
            let deleted = transaction
                .execute(&sql, rusqlite::params_from_iter(parameters))
                .map_err(|error| sqlite_operation_error(SQLITE_GC_SWEEP_FAILURE_CODE, error))?;
            deleted_total = deleted_total
                .checked_add(
                    u64::try_from(deleted).map_err(|error| DurableError::Integrity {
                        code: "sqlite_gc_deleted_count".to_owned(),
                        message: error.to_string(),
                    })?,
                )
                .ok_or_else(|| DurableError::Integrity {
                    code: "sqlite_gc_deleted_count_overflow".to_owned(),
                    message: "SQLite GC deleted-object count overflowed".to_owned(),
                })?;
        }
    }
    Ok(deleted_total)
}

fn audit_authorized_object_families(
    connection: &Connection,
    domain: &str,
    authorized: &BTreeSet<String>,
    batch_size: usize,
) -> DurableResult<()> {
    let mut remaining = authorized.iter();
    loop {
        let batch = remaining.by_ref().take(batch_size).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(());
        }
        let placeholders = (2..batch.len() + 2)
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT family, object_id FROM (
                 SELECT 0 AS family, object_id
                 FROM {STATE_ROOT_TABLE} WHERE domain = ?1
                 UNION ALL
                 SELECT 1 AS family, object_id
                 FROM {COMMAND_ARCHIVE_TABLE} WHERE domain = ?1
                 UNION ALL
                 SELECT 2 AS family, object_id
                 FROM {GC_RECEIPT_TABLE} WHERE domain = ?1
             ) WHERE object_id IN ({placeholders})
             ORDER BY object_id, family"
        );
        let mut parameters = Vec::<&dyn rusqlite::ToSql>::with_capacity(batch.len() + 1);
        parameters.push(&domain);
        parameters.extend(
            batch
                .iter()
                .map(|object_id| *object_id as &dyn rusqlite::ToSql),
        );
        let mut statement = connection
            .prepare(&sql)
            .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
        let mut rows = statement
            .query(rusqlite::params_from_iter(parameters))
            .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
        let mut previous = None;
        while let Some(row) = rows
            .next()
            .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?
        {
            let family = row
                .get::<_, i64>(0)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
            let object_id = row
                .get::<_, String>(1)
                .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
            if previous.as_deref() == Some(object_id.as_str()) {
                return Err(DurableError::Integrity {
                    code: "sqlite_gc_cross_family_identity_alias".to_owned(),
                    message: format!(
                        "GC-authorized identity {object_id} exists in more than one family, including family {family}"
                    ),
                });
            }
            previous = Some(object_id);
        }
    }
}

fn require_identity_absent(
    connection: &Connection,
    domain: &str,
    object_id: &str,
) -> DurableResult<()> {
    let presence = physical_identity_presence(connection, domain, object_id)?;
    if presence.iter().any(|present| *present) {
        return Err(DurableError::Integrity {
            code: "sqlite_gc_new_receipt_identity_collision".to_owned(),
            message: format!(
                "new SQLite GC receipt identity {object_id} already exists in physical inventory"
            ),
        });
    }
    Ok(())
}

fn require_identity_unaliased(
    connection: &Connection,
    domain: &str,
    object_id: &str,
    expected_table: &str,
) -> DurableResult<()> {
    let expected_family = match expected_table {
        STATE_ROOT_TABLE => 0,
        COMMAND_ARCHIVE_TABLE => 1,
        GC_RECEIPT_TABLE => 2,
        _ => {
            return Err(DurableError::Validation(format!(
                "{expected_table} is not a closed SQLite physical object family"
            )));
        }
    };
    let presence = physical_identity_presence(connection, domain, object_id)?;
    if presence
        .iter()
        .enumerate()
        .any(|(family, present)| family != expected_family && *present)
    {
        return Err(DurableError::Integrity {
            code: "sqlite_physical_object_identity_alias".to_owned(),
            message: format!(
                "SQLite physical object identity {object_id} exists outside {expected_table}"
            ),
        });
    }
    Ok(())
}

fn physical_identity_presence(
    connection: &Connection,
    domain: &str,
    object_id: &str,
) -> DurableResult<[bool; 3]> {
    let presence: (bool, bool, bool) = connection
        .query_row(
            "SELECT
                 EXISTS(SELECT 1 FROM cymule_state_root_objects
                        WHERE domain=?1 AND object_id=?2),
                 EXISTS(SELECT 1 FROM cymule_machine_command_archive_objects
                        WHERE domain=?1 AND object_id=?2),
                 EXISTS(SELECT 1 FROM cymule_gc_receipts
                        WHERE domain=?1 AND object_id=?2)",
            (domain, object_id),
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| sqlite_operation_error(SQLITE_IDENTITY_INVENTORY_FAILURE_CODE, error))?;
    Ok([presence.0, presence.1, presence.2])
}

fn require_only_gc_receipt(
    connection: &Connection,
    domain: &str,
    expected_receipt_id: &str,
) -> DurableResult<()> {
    let (count, minimum, maximum): (i64, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT COUNT(*), MIN(object_id), MAX(object_id)
             FROM cymule_gc_receipts WHERE domain=?1",
            [domain],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    if count != 1
        || minimum.as_deref() != Some(expected_receipt_id)
        || maximum.as_deref() != Some(expected_receipt_id)
    {
        return Err(DurableError::Integrity {
            code: "sqlite_gc_receipt_inventory_not_terminal".to_owned(),
            message: format!(
                "SQLite GC advance must leave only current receipt {expected_receipt_id}, observed {count} receipt rows"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
fn list_ids(connection: &Connection, table: &str, domain: &str) -> DurableResult<Vec<String>> {
    let content_id_length = physical_content_id_length()?;
    let sql = format!(
        "SELECT length(object_id),
                CASE WHEN length(object_id) = ?2 THEN object_id ELSE NULL END
         FROM {table} WHERE domain = ?1 ORDER BY object_id"
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    let rows = statement
        .query_map(params![domain, content_id_length], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_operation_error(SQLITE_GC_INVENTORY_FAILURE_CODE, error))?;
    rows.into_iter()
        .map(|(observed_length, object_id)| {
            decode_physical_locator(observed_length, object_id, content_id_length)
        })
        .collect()
}

fn physical_content_id_length() -> DurableResult<i64> {
    i64::try_from("sha256:".len() + cymule_core::sha256_bytes(&[]).len())
        .map_err(|error| DurableError::Validation(error.to_string()))
}

fn decode_physical_locator(
    observed_length: i64,
    object_id: Option<String>,
    expected_length: i64,
) -> DurableResult<String> {
    let object_id = object_id.ok_or_else(|| DurableError::Integrity {
        code: "sqlite_object_locator_length".to_owned(),
        message: format!(
            "SQLite object locator has character length {observed_length}, not {expected_length}"
        ),
    })?;
    cymule_core::validate_content_id("SQLite object locator", &object_id).map_err(|error| {
        DurableError::Integrity {
            code: "sqlite_object_locator_shape".to_owned(),
            message: error.to_string(),
        }
    })?;
    Ok(object_id)
}

fn count(connection: &Connection, table: &str, domain: &str) -> DurableResult<u64> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE domain = ?1");
    let value: i64 = connection
        .query_row(&sql, [domain], |row| row.get(0))
        .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?;
    u64::try_from(value).map_err(|error| DurableError::Integrity {
        code: "sqlite_negative_object_count".to_owned(),
        message: error.to_string(),
    })
}

fn command_archive_object_counts(
    connection: &Connection,
    domain: &str,
) -> DurableResult<(u64, u64, u64, u64)> {
    let content_id_length = physical_content_id_length()?;
    let maximum = i64::try_from(MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES)
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let mut statement = connection
        .prepare(
            "SELECT length(object_id),
                    CASE WHEN length(object_id) = ?2 THEN object_id ELSE NULL END,
                    length(batch_id),
                    CASE WHEN length(batch_id) = ?2 THEN batch_id ELSE NULL END,
                    length(object_json),
                    CASE WHEN length(object_json) <= ?3 THEN object_json ELSE NULL END
             FROM cymule_machine_command_archive_objects
             WHERE domain = ?1 ORDER BY object_id",
        )
        .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?;
    let mut rows = statement
        .query(params![domain, content_id_length, maximum])
        .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?;
    let mut segments = 0_u64;
    let mut entries = 0_u64;
    let mut batches = 0_u64;
    let mut nodes = 0_u64;
    while let Some(row) = rows
        .next()
        .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?
    {
        let (object, indexed_batch_id) = decode_archive_inventory_row(row, content_id_length)?;
        match object {
            MachineCommandArchiveObject::Segment(_) => {
                if indexed_batch_id.is_some() {
                    return Err(DurableError::Integrity {
                        code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                        message: "SQLite archive segment carries a command-batch index".to_owned(),
                    });
                }
                segments = exact_increment(segments, "archive segment count")?;
            }
            MachineCommandArchiveObject::Entry(_) => {
                if indexed_batch_id.is_some() {
                    return Err(DurableError::Integrity {
                        code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                        message: "SQLite archive entry carries a command-batch index".to_owned(),
                    });
                }
                entries = exact_increment(entries, "archive entry count")?;
            }
            MachineCommandArchiveObject::Batch(batch) => {
                if indexed_batch_id.as_deref() != Some(batch.batch_id.as_str()) {
                    return Err(DurableError::Integrity {
                        code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                        message: format!(
                            "SQLite archive batch {} changed its stable index",
                            batch.batch_id
                        ),
                    });
                }
                batches = exact_increment(batches, "archive batch count")?;
            }
            MachineCommandArchiveObject::CommandIndexNode(_) => {
                if indexed_batch_id.is_some() {
                    return Err(DurableError::Integrity {
                        code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                        message: "SQLite archive index node carries a command-batch index"
                            .to_owned(),
                    });
                }
                nodes = exact_increment(nodes, "archive index-node count")?;
            }
        }
    }
    Ok((segments, entries, batches, nodes))
}

fn decode_archive_inventory_row(
    row: &rusqlite::Row<'_>,
    content_id_length: i64,
) -> DurableResult<(MachineCommandArchiveObject, Option<String>)> {
    let object_id = decode_physical_locator(
        row.get(0)
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
        row.get(1)
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
        content_id_length,
    )?;
    let indexed_batch_id = match (
        row.get::<_, Option<i64>>(2)
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
        row.get::<_, Option<String>>(3)
            .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
    ) {
        (None, None) => None,
        (Some(reported_length), value) => Some(decode_physical_locator(
            reported_length,
            value,
            content_id_length,
        )?),
        (None, Some(_)) => {
            return Err(DurableError::Integrity {
                code: "sqlite_command_archive_batch_index_mismatch".to_owned(),
                message: "SQLite command-batch index has bytes without a physical length"
                    .to_owned(),
            });
        }
    };
    let bytes = bounded_blob(
        Some((
            row.get(4)
                .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
            row.get(5)
                .map_err(|error| sqlite_operation_error(SQLITE_STATS_FAILURE_CODE, error))?,
        )),
        MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES,
        "sqlite_command_archive_object_bytes",
        "SQLite Machine command archive object",
    )?
    .expect("archive inventory row exists");
    let object: MachineCommandArchiveObject = decode_persisted(
        &bytes,
        "sqlite_command_archive_object_bytes",
        "SQLite Machine command archive object",
    )?;
    let identity = object.identity().map_err(|error| DurableError::Integrity {
        code: COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE.to_owned(),
        message: format!("SQLite Machine command archive object is invalid: {error}"),
    })?;
    if identity != object_id {
        return Err(DurableError::Integrity {
            code: COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE.to_owned(),
            message: format!(
                "SQLite Machine command archive locator {object_id} resolves to {identity}"
            ),
        });
    }
    Ok((object, indexed_batch_id))
}

fn exact_increment(value: u64, label: &str) -> DurableResult<u64> {
    value.checked_add(1).ok_or_else(|| DurableError::Integrity {
        code: "sqlite_object_count_overflow".to_owned(),
        message: format!("SQLite {label} overflowed"),
    })
}

fn canonical(value: &impl Serialize) -> DurableResult<Vec<u8>> {
    cymule_core::canonical_bytes(value).map_err(Into::into)
}

fn validate_domain(domain: &str) -> DurableResult<()> {
    if domain.is_empty() || domain.chars().count() > 512 || domain.chars().any(char::is_control) {
        return Err(DurableError::Validation(
            "SQLite durable domain must contain 1..=512 non-control characters".to_owned(),
        ));
    }
    Ok(())
}

fn conflict(expected: Option<&StoreHead>, current: Option<&StoreHead>) -> DurableError {
    DurableError::Conflict {
        expected: expected.map(|head| head.physical_token.clone()),
        current: current.map(|head| head.physical_token.clone()),
    }
}

fn contention(
    error: &rusqlite::Error,
    expected: Option<&StoreHead>,
    substrate_code: &'static str,
) -> DurableError {
    match error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: expected.map(|head| head.physical_token.clone()),
                current: Some("sqlite-writer-active".to_owned()),
            }
        }
        _ => sqlite_error(substrate_code, error),
    }
}

fn state_root_closure_error(error: DurableError) -> DurableError {
    match error {
        DurableError::Substrate { .. }
        | DurableError::Integrity { .. }
        | DurableError::CommitOutcomeUnknown { .. } => error,
        error => DurableError::Integrity {
            code: STATE_ROOT_CLOSURE_INTEGRITY_CODE.to_owned(),
            message: format!("SQLite StateRoot closure is invalid: {error}"),
        },
    }
}

fn command_archive_closure_error(error: DurableError) -> DurableError {
    match error {
        DurableError::Substrate { .. } | DurableError::Integrity { .. } => error,
        error => DurableError::Integrity {
            code: "sqlite_command_archive_closure".to_owned(),
            message: format!("SQLite Machine command archive closure is invalid: {error}"),
        },
    }
}

fn sqlite_error(code: &'static str, error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

fn sqlite_operation_error(code: &'static str, error: rusqlite::Error) -> DurableError {
    match error {
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ) =>
        {
            DurableError::Conflict {
                expected: None,
                current: Some("sqlite-writer-active".to_owned()),
            }
        }
        error => sqlite_error(code, error),
    }
}

fn commit_outcome_unknown(error: impl std::fmt::Display) -> DurableError {
    DurableError::CommitOutcomeUnknown {
        message: error.to_string(),
    }
}

fn unsupported_generation(detail: &str) -> DurableError {
    DurableError::Substrate {
        code: UNSUPPORTED_STORE_GENERATION_CODE.to_owned(),
        message: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_durable::{DurableStoreControl, StoreBatch, StoredState};
    #[cfg(unix)]
    use cymule_test_world::{ManagedChild, TestWorld};
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(unix)]
    use std::path::{Path, PathBuf};
    #[cfg(unix)]
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn advance_current(store: &mut impl DurableStore) -> DurableResult<GcReceipt> {
        DurableStoreControl::open(store)?.advance_cold_reclamation()
    }

    fn reconcile_current(store: &mut impl DurableStore) -> DurableResult<GcReceipt> {
        DurableStoreControl::open(store)?.reconcile_cold_reclamation()
    }

    fn gc_archive_candidate(name: &str) -> cymule_core::PlanCandidate {
        use cymule_core::{Definition, Expression, PlanCandidate, Region};
        PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: name.to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn standalone_gc_archive(
        name: &str,
    ) -> (MachineCommandArchiveSegment, cymule_core::MachineBaseAnchor) {
        use cymule_core::{Command, CommandEnvelope, Machine};
        use cymule_runtime::{ExecutionBinding, PluginManifest};
        let mut machine = Machine::new();
        let plan =
            cymule_core::seal_plan(gc_archive_candidate(name)).expect("GC archive Plan seals");
        machine.insert_plan(plan.clone()).expect("Plan stages");
        let binding = ExecutionBinding::for_local_process(
            &PluginManifest {
                plugin_version: cymule_runtime::PLUGIN_VERSION.to_owned(),
                implementation_id: format!("sqlite-gc-archive:{name}"),
                components: BTreeMap::new(),
                effects: BTreeMap::new(),
            },
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        )
        .expect("GC binding derives");
        binding.admit_plan(&plan).expect("GC binding admits Plan");
        let binding = machine
            .put_artifact(
                cymule_runtime::EXECUTION_BINDING_VERSION,
                binding.canonical_bytes().expect("binding encodes"),
            )
            .expect("binding stages");
        let input = machine
            .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"{}".to_vec())
            .expect("input stages");
        let command_id = format!("command:{name}");
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            command_id.clone(),
            vec![plan.clone()],
            vec![
                machine.artifact(&binding).expect("binding reads").clone(),
                machine.artifact(&input).expect("input reads").clone(),
            ],
        )
        .expect("GC archive material admits");
        let initial_attempt = cymule_core::InitialAttemptSpec {
            attempt_id: cymule_core::content_id("cymule.test.initial-attempt/1", &command_id)
                .expect("Attempt derives"),
            continuation_id: cymule_core::content_id(
                "cymule.test.initial-continuation/1",
                &command_id,
            )
            .expect("Continuation derives"),
            occurrence_binding: binding.artifact_id.clone(),
            continuation_epoch: 0,
            execution_fence: 1,
        };
        machine
            .submit(CommandEnvelope {
                command_version: cymule_core::COMMAND_VERSION.to_owned(),
                command_id,
                actor: "test".to_owned(),
                run_id: format!("run:{name}"),
                expected_precondition: None,
                command: Command::StartRun {
                    plan_id: plan.plan_id,
                    binding_context: binding.artifact_id,
                    input,
                    material_digest: material.material_digest().to_owned(),
                    initial_attempt,
                },
            })
            .expect("GC archive Run starts");
        let archive = machine
            .compact_event_history(0)
            .expect("GC history compacts")
            .archive_segment;
        let anchor = machine
            .base_anchor()
            .expect("GC anchor derives")
            .expect("GC anchor exists");
        (archive, anchor)
    }

    fn mismatched_gc_batch(
        original: &MachineCommandBatchRecord,
        mismatch: &str,
    ) -> MachineCommandBatchRecord {
        let mut forged = original.clone();
        if mismatch == "member" {
            forged.members[0].semantic_hash =
                cymule_core::canonical_digest(&"other semantic envelope")
                    .expect("semantic hash derives");
        } else {
            assert_eq!(
                forged.receipts[0].event_ids.len(),
                2,
                "StartRun archives both initial Events"
            );
            forged.receipts[0].event_ids.reverse();
            forged.event_ids = forged.receipts[0].event_ids.clone();
        }
        forged.batch_receipt_id = cymule_core::content_id(
            cymule_core::MACHINE_COMMAND_BATCH_RECEIPT_VERSION,
            &serde_json::json!({
                "receipt_version": cymule_core::MACHINE_COMMAND_BATCH_RECEIPT_VERSION,
                "batch_id": &forged.batch_id,
                "admission_parent_authority_root": &forged.admission_parent_authority_root,
                "material_source": &forged.material_source,
                "members": &forged.members,
                "receipts": &forged.receipts,
                "event_ids": &forged.event_ids,
                "result_authority_root": &forged.result_authority_root,
                "plan_ids": &forged.plan_ids,
                "artifacts": &forged.artifacts,
            }),
        )
        .expect("forged complete receipt rehashes");
        forged.verify().expect("forged batch is individually valid");
        assert_eq!(forged.batch_id, original.batch_id);
        assert_ne!(forged.batch_receipt_id, original.batch_receipt_id);
        forged
    }

    #[derive(Clone, Copy)]
    struct HistoryPlugin;

    impl cymule_runtime::PluginHost for HistoryPlugin {
        fn invoke(
            &mut self,
            request: cymule_runtime::PluginRequest,
        ) -> cymule_runtime::RuntimeResult<cymule_runtime::PluginResponse> {
            match request {
                cymule_runtime::PluginRequest::Describe => {
                    Ok(cymule_runtime::PluginResponse::Manifest {
                        manifest: cymule_runtime::PluginManifest {
                            plugin_version: cymule_runtime::PLUGIN_VERSION.to_owned(),
                            implementation_id: "official-store-history-tests@1".to_owned(),
                            components: BTreeMap::new(),
                            effects: BTreeMap::new(),
                        },
                    })
                }
                _ => Err(cymule_runtime::RuntimeError::plugin_defect(
                    "the history fixture has no external operations",
                )),
            }
        }
    }

    fn history_candidate(run_id: &str, signal: Option<&str>) -> cymule_core::PlanCandidate {
        let steps = signal.map_or_else(Vec::new, |key| {
            vec![cymule_core::Step {
                id: "wait.signal".to_owned(),
                operation: cymule_core::Operation::Wait {
                    wait: cymule_core::WaitSpec::Signal {
                        key: key.to_owned(),
                        consume_once: true,
                    },
                    bind: None,
                },
            }]
        });
        cymule_core::PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: run_id.to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![cymule_core::Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: cymule_core::Region {
                    steps,
                    result: cymule_core::Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn history_runtime<S: DurableStore>(
        store: S,
        clock_path: &std::path::Path,
        run_id: &str,
    ) -> (
        cymule_durable::DurableRuntimeControl<S, HistoryPlugin>,
        cymule_durable_protocol::ExecutionClaimRequest,
    ) {
        let mut clock = cymule_clock_system::SqliteClock::open(
            clock_path,
            "clock:official-store-history",
            "sha256:4444444444444444444444444444444444444444444444444444444444444444",
        )
        .expect("official Clock opens");
        let observation = clock
            .observe(
                &cymule_durable_protocol::execution_clock_scope(run_id)
                    .expect("execution scope derives"),
            )
            .expect("Clock issues a retained observation");
        let execution = cymule_durable_protocol::ExecutionClaimRequest {
            owner: format!("driver:{run_id}"),
            clock: observation.reference(),
            ttl: 10,
        };
        let admission =
            cymule_runtime::ExecutionBindingAdmission::from_manifest(HistoryPlugin, |manifest| {
                cymule_runtime::ExecutionBinding::for_local_process(
                    manifest,
                    "sha256:3333333333333333333333333333333333333333333333333333333333333333",
                )
                .map_err(cymule_runtime::RuntimeError::from)
            })
            .expect("real provider admission succeeds");
        (
            cymule_durable::DurableRuntimeControl::open(store, admission, clock)
                .expect("public runtime opens"),
            execution,
        )
    }

    fn history_park<S: DurableStore>(
        store: S,
        clock_path: &std::path::Path,
        run_id: &str,
        signal: &str,
    ) -> (S, String) {
        let (mut runtime, execution) = history_runtime(store, clock_path, run_id);
        let response = runtime
            .submit(cymule_durable::DurableCommand::StartRun {
                control_version: cymule_durable::DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                candidate: history_candidate(run_id, Some(signal)),
                input: serde_json::json!({"run": run_id}),
                execution,
            })
            .expect("real signal Run parks");
        let cymule_durable::DurableResponse::RunBoundary {
            boundary: cymule_durable::DurableBoundary::Suspended { wait_id },
        } = response
        else {
            panic!("signal Run did not suspend");
        };
        (runtime.into_parts().0, wait_id)
    }

    fn history_request(
        store: &mut impl DurableStore,
        id: &str,
        kind: cymule_durable::HistoryCompactionKind,
    ) -> cymule_durable::HistoryCompactionRequest {
        cymule_durable::HistoryCompactionRequest {
            compaction_id: id.to_owned(),
            expected_revision: store.load_head().unwrap().unwrap().revision,
            kind,
            requested_suffix: 0,
        }
    }

    fn assert_history_cold_replay(store: &mut impl DurableStore) {
        let audited = store
            .load_full_audit()
            .expect("offline physical closure audits")
            .expect("published authority exists");
        let archive = cymule_durable::load_machine_command_archive(
            audited.head.machine_base_anchor.as_ref().unwrap(),
            |id| store.load_machine_command_archive_segment(id),
        )
        .expect("complete archive lineage authenticates");
        cymule_core::Machine::restore_with_archive(audited.state.machine, archive)
            .expect("real persisted Core authority restores")
            .verify_replay()
            .expect("both hot and cold command history replay");
    }

    struct HistoryFixture<S> {
        store: S,
        first_head: StoreHead,
        first_request: cymule_durable::HistoryCompactionRequest,
        second_request: cymule_durable::HistoryCompactionRequest,
        first: cymule_durable::HistoryCompactionReceipt,
        second: cymule_durable::HistoryCompactionReceipt,
    }

    fn material_only_archive<S: DurableStore>(
        store: S,
        clock_path: &std::path::Path,
    ) -> HistoryFixture<S> {
        let run_id = "run:history:material";
        let signal = "signal:history:material";
        let (mut store, wait_id) = history_park(store, clock_path, run_id, signal);
        let first_request = history_request(
            &mut store,
            "history:material:prefix",
            cymule_durable::HistoryCompactionKind::EventPrefix,
        );
        let first = DurableStoreControl::open(&mut store)
            .expect("first maintenance opens")
            .compact_machine_history(&first_request)
            .expect("real waiting Event history compacts");
        let first_head = store.load_head().unwrap().unwrap();
        DurableStoreControl::open(&mut store)
            .expect("store-only activation opens")
            .submit(cymule_durable::DurableCommand::ActivateWait {
                control_version: cymule_durable::DURABLE_CONTROL_VERSION.to_owned(),
                activation_id: "activation:history:material".to_owned(),
                source: cymule_durable_protocol::WaitActivationSource::Signal {
                    key: signal.to_owned(),
                },
                wait_ids: BTreeSet::from([wait_id]),
                value: serde_json::json!({"ready": true}),
            })
            .expect("public activation admits a zero-command result-material batch");
        let second_request = history_request(
            &mut store,
            "history:material:event-free",
            cymule_durable::HistoryCompactionKind::EventFreeAdmissions,
        );
        let second = DurableStoreControl::open(&mut store)
            .expect("second maintenance opens")
            .compact_machine_history(&second_request)
            .expect("event-free material admission compacts");
        assert_eq!(
            second.parent_compaction.as_deref(),
            Some(first.compaction_id.as_str())
        );
        assert_eq!(second.result.archive_segment.event_count, 0);
        assert_eq!(second.result.archive_segment.batch_count, 1);
        HistoryFixture {
            store,
            first_head,
            first_request,
            second_request,
            first,
            second,
        }
    }

    fn assert_history_requests_replay(store: &mut impl DurableStore, fixture: &HistoryFixture<()>) {
        let before = store.load_head().unwrap().unwrap();
        let mut control = DurableStoreControl::open(&mut *store).unwrap();
        assert_eq!(
            control
                .compact_machine_history(&fixture.first_request)
                .unwrap(),
            fixture.first
        );
        assert_eq!(
            control
                .compact_machine_history(&fixture.second_request)
                .unwrap(),
            fixture.second
        );
        drop(control);
        assert_eq!(store.load_head().unwrap().unwrap(), before);
    }

    struct HistoryCancellation {
        request: cymule_durable::HistoryCompactionRequest,
        command: cymule_durable::DurableCommand,
        response: cymule_durable::DurableResponse,
        source_head: StoreHead,
    }

    fn history_cancelled<S: DurableStore>(
        store: S,
        clock_path: &std::path::Path,
    ) -> (S, HistoryCancellation) {
        let run_id = "run:history:cancel";
        let (mut store, _) = history_park(store, clock_path, run_id, "signal:history:cancel");
        let command = cymule_durable::DurableCommand::CancelRun {
            control_version: cymule_durable::DURABLE_CONTROL_VERSION.to_owned(),
            cancellation_id: "cancel:history:official-store".to_owned(),
            run_id: run_id.to_owned(),
            reason: serde_json::json!("cancel before compaction"),
        };
        let response = DurableStoreControl::open(&mut store)
            .unwrap()
            .submit(command.clone())
            .expect("public cancellation commits");
        let request = history_request(
            &mut store,
            "history:cancel:prefix",
            cymule_durable::HistoryCompactionKind::EventPrefix,
        );
        let source_head = store.load_head().unwrap().unwrap();
        (
            store,
            HistoryCancellation {
                request,
                command,
                response,
                source_head,
            },
        )
    }

    fn assert_history_replay_and_stale(
        store: &mut impl DurableStore,
        evidence: &HistoryCancellation,
        receipt: &cymule_durable::HistoryCompactionReceipt,
    ) {
        let head = store.load_head().unwrap().unwrap();
        let inventory = store.stats().unwrap();
        let mut control = DurableStoreControl::open(&mut *store).unwrap();
        assert_eq!(
            control.compact_machine_history(&evidence.request).unwrap(),
            *receipt,
        );
        assert_eq!(
            control
                .submit(evidence.command.clone())
                .expect("old cancellation replays"),
            evidence.response,
        );
        let mut changed = evidence.request.clone();
        changed.requested_suffix = 1;
        assert!(matches!(
            control.compact_machine_history(&changed),
            Err(DurableError::HistoryConflict { .. })
        ));
        changed.compaction_id = "history:stale:new-id".to_owned();
        assert!(matches!(
            control.compact_machine_history(&changed),
            Err(DurableError::Conflict { .. })
        ));
        drop(control);
        assert_eq!(store.load_head().unwrap().unwrap(), head);
        assert_eq!(store.stats().unwrap(), inventory);
    }

    fn history_missing_objects(
        store: &mut impl DurableStore,
        receipt: &cymule_durable::HistoryCompactionReceipt,
    ) -> Vec<MachineCommandArchiveObject> {
        let segment = store
            .load_machine_command_archive_segment(&receipt.result.archive_segment.segment_id)
            .unwrap()
            .unwrap();
        let head = store.load_head().unwrap().unwrap();
        let root = &head
            .machine_base_anchor
            .as_ref()
            .unwrap()
            .command_index_root;
        let node = store
            .load_machine_command_index_node(root)
            .unwrap()
            .unwrap();
        vec![
            MachineCommandArchiveObject::Entry(Box::new(segment.entries[0].clone())),
            MachineCommandArchiveObject::Batch(Box::new(segment.batches[0].clone())),
            MachineCommandArchiveObject::CommandIndexNode(node),
        ]
    }

    fn finish_history_compaction_chain<S: DurableStore>(
        mut store: S,
        clock_path: &std::path::Path,
        evidence: &HistoryCancellation,
        check_missing: impl FnOnce(&mut S, &cymule_durable::HistoryCompactionReceipt),
    ) {
        let first = DurableStoreControl::open(&mut store)
            .unwrap()
            .compact_machine_history(&evidence.request)
            .expect("lost acknowledgement resolves to its exact committed receipt");
        first.verify().unwrap();
        assert_eq!(first.source_revision, evidence.request.expected_revision);
        assert_eq!(
            store.load_head().unwrap().unwrap().sequence,
            evidence.source_head.sequence + 1,
        );
        assert_history_replay_and_stale(&mut store, evidence, &first);
        let run_id = "run:history:after-first";
        let (mut runtime, execution) = history_runtime(store, clock_path, run_id);
        assert!(matches!(
            runtime
                .submit(cymule_durable::DurableCommand::StartRun {
                    control_version: cymule_durable::DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: run_id.to_owned(),
                    candidate: history_candidate(run_id, None),
                    input: serde_json::json!({"run": run_id}),
                    execution,
                })
                .unwrap(),
            cymule_durable::DurableResponse::RunBoundary {
                boundary: cymule_durable::DurableBoundary::Completed { .. }
            }
        ));
        let mut store = runtime.into_parts().0;
        check_missing(&mut store, &first);
        let second_request = history_request(
            &mut store,
            "history:after-first:prefix",
            cymule_durable::HistoryCompactionKind::EventPrefix,
        );
        let second = DurableStoreControl::open(&mut store)
            .unwrap()
            .compact_machine_history(&second_request)
            .expect("second real Event prefix compacts");
        assert_eq!(
            second.parent_compaction.as_deref(),
            Some(first.compaction_id.as_str())
        );
        assert_history_replay_and_stale(&mut store, evidence, &first);
        assert_history_cold_replay(&mut store);
    }

    fn reject_missing_history_objects(
        store: &mut SqliteStore,
        receipt: &cymule_durable::HistoryCompactionReceipt,
    ) {
        for object in history_missing_objects(store, receipt) {
            let id = object.identity().unwrap();
            assert_eq!(store.connection.execute(
                "DELETE FROM cymule_machine_command_archive_objects WHERE domain=?1 AND object_id=?2",
                (&store.domain, &id),
            ).unwrap(), 1);
            let head = store.load_head().unwrap().unwrap();
            let inventory = gc_physical_inventory(store);
            let audited = store.load_full_audit();
            assert!(
                matches!(&audited, Err(DurableError::Integrity { .. })),
                "full audit must reject missing cold object {}: {:?}",
                object.identity().unwrap(),
                audited.as_ref().map(Option::is_some),
            );
            assert_eq!(store.load_head().unwrap().unwrap(), head);
            assert!(matches!(
                advance_current(store),
                Err(DurableError::Integrity { .. })
            ));
            assert_eq!(store.load_head().unwrap().unwrap(), head);
            assert_eq!(gc_physical_inventory(store), inventory);
            let transaction = store.connection.transaction().unwrap();
            insert_command_archive_object(&transaction, &store.domain, &object)
                .expect("restore exact original archive object");
            transaction.commit().unwrap();
        }
        let integrity: String = store
            .connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
    }

    #[test]
    fn public_history_compaction_reopens_replays_and_recovers_lost_ack() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("store.sqlite");
        let clock = temporary.path().join("clock.sqlite");
        let (store, evidence) =
            history_cancelled(SqliteStore::open(&path, "domain:history").unwrap(), &clock);
        let faulted = InterceptingSqliteStore {
            inner: store,
            interception: InitialCommitInterception::MissingResponse,
        };
        let mut control = DurableStoreControl::open(faulted).unwrap();
        assert!(matches!(
            control.compact_machine_history(&evidence.request),
            Err(DurableError::CommitOutcomeUnknown { .. })
        ));
        drop(control);
        let store = SqliteStore::open(&path, "domain:history")
            .expect("official SQLite Store reopens after loss");
        finish_history_compaction_chain(store, &clock, &evidence, reject_missing_history_objects);
    }

    #[test]
    fn gc_retains_material_only_batch_without_command_entries() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("store.sqlite");
        let clock = temporary.path().join("clock.sqlite");
        let fixture = material_only_archive(
            SqliteStore::open(&path, "domain:material-only").unwrap(),
            &clock,
        );
        let HistoryFixture {
            store,
            first_head,
            first_request,
            second_request,
            first,
            second,
        } = fixture;
        drop(store);
        let mut store =
            SqliteStore::open(&path, "domain:material-only").expect("physical Store reopens");
        let fixture = HistoryFixture {
            store: (),
            first_head,
            first_request,
            second_request,
            first,
            second,
        };
        assert_history_requests_replay(&mut store, &fixture);
        let head = store.load_head().unwrap().unwrap();
        let archive = store
            .load_machine_command_archive_segment(&fixture.second.result.archive_segment.segment_id)
            .unwrap()
            .unwrap();
        assert!(archive.entries.is_empty());
        let batch = &archive.batches[0];
        let mut expected = SqliteStore::retained_machine_command_objects(
            &store.connection,
            &store.domain,
            &fixture.first_head,
        )
        .unwrap();
        expected.extend([
            archive.header.segment_id.clone(),
            batch.batch_receipt_id.clone(),
        ]);
        assert_eq!(
            SqliteStore::retained_machine_command_objects(&store.connection, &store.domain, &head,)
                .unwrap(),
            expected,
        );
        store.connection.execute(
            "DELETE FROM cymule_machine_command_archive_objects WHERE domain=?1 AND object_id=?2",
            (&store.domain, &batch.batch_receipt_id),
        ).expect("remove exact independent batch");
        assert!(matches!(
            advance_current(&mut store),
            Err(DurableError::Integrity { .. })
        ));
        assert_eq!(store.load_head().unwrap().unwrap(), head);
        let transaction = store.connection.transaction().unwrap();
        insert_command_archive_object(
            &transaction,
            &store.domain,
            &MachineCommandArchiveObject::Batch(Box::new(batch.clone())),
        )
        .expect("restore exact fault-fixture bytes");
        transaction.commit().unwrap();
        let (mut runtime, execution) = history_runtime(store, &clock, "run:history:material");
        assert!(matches!(
            runtime
                .submit(cymule_durable::DurableCommand::ResumeRun {
                    control_version: cymule_durable::DURABLE_CONTROL_VERSION.to_owned(),
                    run_id: "run:history:material".to_owned(),
                    execution,
                })
                .unwrap(),
            cymule_durable::DurableResponse::RunBoundary {
                boundary: cymule_durable::DurableBoundary::Completed { .. }
            }
        ));
        assert_history_cold_replay(&mut runtime.into_parts().0);
    }

    type GcPhysicalRow = (String, String, Option<String>, Vec<u8>);

    fn gc_physical_inventory(store: &SqliteStore) -> Vec<GcPhysicalRow> {
        let mut statement = store.connection.prepare(
            "SELECT 'head',domain,NULL,head_json FROM cymule_heads WHERE domain=?1
             UNION ALL SELECT 'state_root',object_id,NULL,object_json FROM cymule_state_root_objects WHERE domain=?1
             UNION ALL SELECT 'archive',object_id,batch_id,object_json FROM cymule_machine_command_archive_objects WHERE domain=?1
             UNION ALL SELECT 'gc_receipt',object_id,NULL,object_json FROM cymule_gc_receipts WHERE domain=?1
             ORDER BY 1,2",
        ).expect("physical inventory query prepares");
        statement
            .query_map([&store.domain], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .expect("physical inventory reads")
            .collect::<Result<Vec<_>, _>>()
            .expect("physical inventory collects")
    }

    #[test]
    fn gc_archive_reachability_retains_the_indexed_batch_receipt() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut store =
            SqliteStore::open(directory.path().join("gc-batch.sqlite"), "domain:gc-batch")
                .expect("Store opens");
        DurableStoreControl::initialize(&mut store).expect("Store initializes");
        let mut head = store.load_head().expect("head reads").expect("head exists");
        let (archive, anchor) = standalone_gc_archive("gc_batch_authority");
        head.machine_base_anchor = Some(anchor);
        let objects = archive
            .persistence_objects()
            .expect("archive objects derive");
        let transaction = store
            .connection
            .transaction()
            .expect("archive transaction opens");
        for object in &objects {
            insert_command_archive_object(&transaction, &store.domain, object)
                .expect("archive object persists");
        }
        transaction.commit().expect("archive objects commit");
        let expected = objects
            .iter()
            .map(|object| object.identity().expect("object verifies"))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            SqliteStore::retained_machine_command_objects(&store.connection, &store.domain, &head)
                .expect("GC archive closure verifies"),
            expected
        );
        for batch in &archive.batches {
            assert_eq!(
                read_command_archive_batch_receipt_id(
                    &store.connection,
                    &store.domain,
                    &batch.batch_id
                )
                .expect("stable index resolves"),
                Some(batch.batch_receipt_id.clone())
            );
            assert_eq!(
                read_command_archive_batch(&store.connection, &store.domain, &batch.batch_id)
                    .expect("receipt resolves"),
                Some(batch.clone())
            );
        }
    }

    #[test]
    fn gc_rejects_self_valid_indexed_batches_that_disagree_with_reachable_entries() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut store =
            SqliteStore::open(directory.path().join("gc-batch.sqlite"), "domain:gc-batch")
                .expect("Store opens");
        DurableStoreControl::initialize(&mut store).expect("Store initializes");
        let mut head = store.load_head().expect("head reads").expect("head exists");
        let (archive, anchor) = standalone_gc_archive("gc_batch_mismatch");
        head.machine_base_anchor = Some(anchor);
        let transaction = store
            .connection
            .transaction()
            .expect("archive transaction opens");
        for object in archive
            .persistence_objects()
            .expect("archive objects derive")
        {
            insert_command_archive_object(&transaction, &store.domain, &object)
                .expect("archive object persists");
        }
        transaction.commit().expect("archive objects commit");
        let original = archive.batches.first().expect("batch exists");
        let entry = archive.entries.first().expect("entry exists");
        for mismatch in ["member", "receipt"] {
            let forged = mismatched_gc_batch(original, mismatch);
            assert!(forged.verify_entry(entry).is_err());
            let object = MachineCommandArchiveObject::Batch(Box::new(forged.clone()));
            let updated = store.connection.execute(
                "UPDATE cymule_machine_command_archive_objects SET object_id=?1, object_json=?2 WHERE domain=?3 AND batch_id=?4",
                params![forged.batch_receipt_id, cymule_core::canonical_bytes(&object).expect("batch encodes"), store.domain, forged.batch_id],
            ).expect("self-valid corrupt receipt fixture replaces the stable index target");
            assert_eq!(updated, 1);
            assert_eq!(
                read_command_archive_batch(&store.connection, &store.domain, &forged.batch_id)
                    .expect("stable index and object hashes verify"),
                Some(forged)
            );
            let before = gc_physical_inventory(&store);
            assert!(
                matches!(
                    SqliteStore::retained_machine_command_objects(&store.connection, &store.domain, &head),
                    Err(DurableError::Integrity { code, .. }) if code == "sqlite_command_archive_batch_mismatch"
                ),
                "GC must reject the {mismatch} mismatch before returning a reachable set"
            );
            assert_eq!(
                gc_physical_inventory(&store),
                before,
                "failed GC reachability must not delete or rewrite any physical object or head"
            );
        }
    }

    fn reset_gc_receipt_read_count() {
        GC_RECEIPT_READ_COUNT.with(|count| count.set(0));
    }

    fn gc_receipt_read_count() -> u64 {
        GC_RECEIPT_READ_COUNT.with(std::cell::Cell::get)
    }

    #[test]
    fn ordinary_semantic_reads_never_follow_the_pinned_gc_receipt() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("receipt-read-set.sqlite");
        let domain = "domain:receipt-read-set";
        let mut store = SqliteStore::open(&path, domain).expect("Store opens");
        DurableStoreControl::initialize(&mut store).expect("state initializes");
        let receipt = advance_current(&mut store).expect("receipt generation publishes");
        let head = store
            .load_head()
            .expect("head loads before poisoning")
            .expect("head exists");
        let manifest = store
            .load_state_root_manifest(&head.state_root_manifest_id)
            .expect("manifest loads before poisoning")
            .expect("manifest exists");
        let oversized = i64::try_from(MAX_GC_RECEIPT_BYTES + 1)
            .expect("receipt byte bound is representable in SQLite");
        store
            .connection
            .execute(
                "UPDATE cymule_gc_receipts SET object_json=zeroblob(?1)
                 WHERE domain=?2 AND object_id=?3",
                params![oversized, domain, &receipt.receipt_id],
            )
            .expect("pinned receipt becomes oversized physical poison");
        let replacement_id = format!("sha256:{}", "a".repeat(64));
        let coupling_id = format!("sha256:{}", "b".repeat(64));

        reset_gc_receipt_read_count();
        assert_eq!(
            store.load_head().expect("bounded head loads"),
            Some(head.clone())
        );
        assert_eq!(
            store
                .load_state_root_manifest(manifest.manifest_id())
                .expect("exact manifest loads"),
            Some(manifest.clone())
        );
        assert_eq!(
            store
                .load_full_audit()
                .expect("reachable projection audits")
                .expect("projection exists")
                .head,
            head
        );
        store
            .with_state_root_resolver(&manifest, |_| Ok(()))
            .expect("resolver callback remains receipt-independent");
        assert_eq!(
            store
                .application_journal_record_manifest(
                    &manifest,
                    "journal:poisoned-receipt",
                    "record:poisoned-receipt",
                )
                .expect("exact record lookup remains receipt-independent"),
            None
        );
        assert_eq!(
            store
                .application_journal_prefix_replacement_authority(&manifest, &replacement_id,)
                .expect("exact replacement lookup remains receipt-independent"),
            None
        );
        assert_eq!(
            store
                .coupled_checkpoint_receipt(&manifest, &coupling_id)
                .expect("exact coupling lookup remains receipt-independent"),
            None
        );
        store
            .stats()
            .expect("physical counts do not decode receipt payloads");
        assert_eq!(
            gc_receipt_read_count(),
            0,
            "ordinary small and oversized receipt generations have the same zero-byte read set"
        );

        assert!(matches!(
            reconcile_current(&mut store),
            Err(DurableError::Integrity { code, .. }) if code == "sqlite_gc_receipt_bytes"
        ));
        assert!(gc_receipt_read_count() > 0);
        assert_eq!(
            store.load_head().expect("head remains authoritative"),
            Some(head)
        );
    }

    #[test]
    fn stats_pin_one_snapshot_across_a_concurrent_gc_commit() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("stats-snapshot.sqlite");
        let domain = "domain:stats-snapshot";
        let mut writer = SqliteStore::open(&path, domain).expect("writer opens");
        DurableStoreControl::initialize(&mut writer).expect("state initializes");
        let orphan_id = format!("sha256:{}", "8".repeat(64));
        writer
            .connection
            .execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES (?1,?2,x'7b7d')",
                (domain, &orphan_id),
            )
            .expect("unreachable orphan inserts");
        let before = writer.stats().expect("pre-GC stats read");
        let observer = SqliteStore::open(&path, domain).expect("observer opens");
        let (snapshot_ready_tx, snapshot_ready_rx) = mpsc::sync_channel(0);
        let (gc_committed_tx, gc_committed_rx) = mpsc::sync_channel(0);

        let observer_thread = thread::spawn(move || {
            observer.stats_with_barrier(|| {
                snapshot_ready_tx
                    .send(())
                    .expect("observer announces its pinned snapshot");
                gc_committed_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("observer waits for the concurrent GC commit");
                Ok(())
            })
        });
        snapshot_ready_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("observer pins its snapshot");
        let receipt = advance_current(&mut writer).expect("concurrent GC commits");
        gc_committed_tx
            .send(())
            .expect("observer may complete its snapshot read");
        let during = observer_thread
            .join()
            .expect("observer thread completes")
            .expect("concurrent stats read succeeds");
        let after = writer.stats().expect("post-GC stats read");

        assert_eq!(receipt.reclaimed_ids, [orphan_id].into());
        assert_eq!(during, before);
        assert_eq!(after.state_root_objects + 1, before.state_root_objects);
        assert_eq!(after.gc_receipts, before.gc_receipts + 1);
    }

    enum InitialCommitInterception {
        MissingResponse,
        CrossFamilyAlias,
        NoncanonicalManifest,
        #[cfg(unix)]
        Barrier {
            selected: String,
            marker: PathBuf,
        },
    }

    struct InterceptingSqliteStore {
        inner: SqliteStore,
        interception: InitialCommitInterception,
    }

    impl DurableStore for InterceptingSqliteStore {
        fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
            self.inner.load_head()
        }

        fn load_state_root_manifest(
            &mut self,
            manifest_id: &str,
        ) -> DurableResult<Option<StateRootManifest>> {
            self.inner.load_state_root_manifest(manifest_id)
        }

        fn with_state_root_resolver<T>(
            &mut self,
            current: &StateRootManifest,
            read: impl FnOnce(&mut dyn StateRootResolver) -> DurableResult<T>,
        ) -> DurableResult<T> {
            self.inner.with_state_root_resolver(current, read)
        }

        fn application_journal_prefix(
            &mut self,
            manifest: &StateRootManifest,
            journal_id: &str,
            count: u64,
        ) -> DurableResult<ApplicationJournalPrefix> {
            self.inner
                .application_journal_prefix(manifest, journal_id, count)
        }

        fn application_journal_record_manifest(
            &mut self,
            manifest: &StateRootManifest,
            journal_id: &str,
            record_id: &str,
        ) -> DurableResult<Option<JournalRecordManifest>> {
            self.inner
                .application_journal_record_manifest(manifest, journal_id, record_id)
        }

        fn application_journal_prefix_replacement_authority(
            &mut self,
            manifest: &StateRootManifest,
            replacement_id: &str,
        ) -> DurableResult<Option<ApplicationJournalPrefixReplacementAuthority>> {
            self.inner
                .application_journal_prefix_replacement_authority(manifest, replacement_id)
        }

        fn coupled_checkpoint_receipt(
            &mut self,
            manifest: &StateRootManifest,
            coupling_id: &str,
        ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
            self.inner.coupled_checkpoint_receipt(manifest, coupling_id)
        }

        fn load_machine_command_archive_segment(
            &mut self,
            segment_id: &str,
        ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
            self.inner.load_machine_command_archive_segment(segment_id)
        }

        fn load_machine_command_archive_entry(
            &mut self,
            entry_id: &str,
        ) -> DurableResult<Option<MachineCommandArchiveEntry>> {
            self.inner.load_machine_command_archive_entry(entry_id)
        }

        fn load_machine_command_archive_batch(
            &mut self,
            batch_id: &str,
        ) -> DurableResult<Option<MachineCommandBatchRecord>> {
            self.inner.load_machine_command_archive_batch(batch_id)
        }

        fn load_machine_command_index_node(
            &mut self,
            node_id: &str,
        ) -> DurableResult<Option<MachineCommandIndexNode>> {
            self.inner.load_machine_command_index_node(node_id)
        }

        fn lookup_machine_command_archive(
            &mut self,
            anchor: &cymule_core::MachineBaseAnchor,
            command_id: &str,
        ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
            self.inner
                .lookup_machine_command_archive(anchor, command_id)
        }

        fn compare_and_commit(
            &mut self,
            expected: Option<&StoreHead>,
            batch: &StoreBatch,
        ) -> DurableResult<StoreCommit> {
            match &self.interception {
                InitialCommitInterception::MissingResponse => self
                    .inner
                    .compare_and_commit_with_response(expected, batch, || {
                        Err("simulated SQLite commit response loss".to_owned())
                    }),
                InitialCommitInterception::CrossFamilyAlias => {
                    let object_id = batch
                        .state_root_transition()
                        .objects()
                        .first()
                        .expect("initial transition contains objects")
                        .object_id();
                    self.inner
                        .connection
                        .execute(
                            "INSERT INTO cymule_machine_command_archive_objects(
                                 domain,object_id,object_json
                             ) VALUES (?1,?2,x'7b7d')",
                            (&self.inner.domain, object_id),
                        )
                        .expect("cross-family alias inserts");
                    self.inner.compare_and_commit(expected, batch)
                }
                InitialCommitInterception::NoncanonicalManifest => {
                    let manifest = batch
                        .state_root_transition()
                        .objects()
                        .iter()
                        .find(|object| object.object_id() == batch.head().state_root_manifest_id)
                        .expect("manifest object exists");
                    let mut noncanonical = vec![b' '];
                    noncanonical.extend(
                        cymule_core::canonical_bytes(manifest)
                            .expect("manifest encodes")
                            .iter(),
                    );
                    self.inner
                        .connection
                        .execute(
                            "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                             VALUES (?1,?2,?3)",
                            (&self.inner.domain, manifest.object_id(), noncanonical),
                        )
                        .expect("conflicting physical bytes insert");
                    self.inner.compare_and_commit(expected, batch)
                }
                #[cfg(unix)]
                InitialCommitInterception::Barrier { selected, marker } => {
                    self.inner.compare_and_commit_with_barriers(
                        expected,
                        batch,
                        |barrier| park_at_selected_barrier(selected, marker, barrier),
                        || Ok(()),
                    )
                }
            }
        }

        fn reconcile_cold_reclamation(
            &mut self,
            request: &StoreReclamation,
        ) -> DurableResult<GcReceipt> {
            self.inner.reconcile_cold_reclamation(request)
        }

        fn advance_cold_reclamation(
            &mut self,
            request: &StoreReclamation,
        ) -> DurableResult<GcReceipt> {
            self.inner.advance_cold_reclamation(request)
        }

        fn stats(&self) -> DurableResult<StoreStats> {
            self.inner.stats()
        }
    }

    #[test]
    fn invalid_domain_is_rejected_before_the_database_is_created() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("invalid-domain.sqlite");

        assert!(matches!(
            SqliteStore::open(&path, ""),
            Err(DurableError::Validation(_))
        ));
        assert!(
            !path.exists(),
            "domain admission must precede every filesystem mutation"
        );

        let maximum_unicode_domain = "界".repeat(512);
        SqliteStore::in_memory(maximum_unicode_domain)
            .expect("512 Unicode scalar values are an admitted Store domain");

        let over_limit_path = temporary.path().join("over-limit-domain.sqlite");
        assert!(matches!(
            SqliteStore::open(&over_limit_path, "界".repeat(513)),
            Err(DurableError::Validation(_))
        ));
        assert!(
            !over_limit_path.exists(),
            "over-limit domain rejection precedes every filesystem mutation"
        );
    }

    #[test]
    fn committed_transaction_with_missing_response_has_unknown_outcome() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("commit-response.sqlite");
        let store = InterceptingSqliteStore {
            inner: SqliteStore::open(&path, "domain:commit-response").expect("SQLite store opens"),
            interception: InitialCommitInterception::MissingResponse,
        };
        let Err(error) = DurableStoreControl::initialize(store) else {
            panic!("committed transaction cannot return without its response");
        };
        assert!(matches!(
            error,
            DurableError::CommitOutcomeUnknown { message }
                if message == "simulated SQLite commit response loss"
        ));

        let mut store = SqliteStore::open(&path, "domain:commit-response")
            .expect("committed SQLite store reopens");
        let retained = store
            .load_full_audit()
            .expect("committed transaction remains readable")
            .expect("state committed before response loss");
        retained.verify().expect("retained state verifies");
    }

    #[test]
    fn commit_rejects_a_cross_family_identity_alias_before_publishing_head() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("commit-cross-family-alias.sqlite");
        let domain = "domain:commit-cross-family-alias";
        let store = InterceptingSqliteStore {
            inner: SqliteStore::open(&path, domain).expect("SQLite store opens"),
            interception: InitialCommitInterception::CrossFamilyAlias,
        };
        let Err(error) = DurableStoreControl::initialize(store) else {
            panic!("cross-family alias cannot publish a semantic head");
        };
        assert!(matches!(
            error,
            DurableError::Integrity { code, .. }
                if code == "sqlite_physical_object_identity_alias"
        ));
        let connection = Connection::open(&path).expect("raw connection opens");
        let head_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM cymule_heads WHERE domain=?1",
                [domain],
                |row| row.get(0),
            )
            .expect("head count reads");
        assert_eq!(head_count, 0);
    }

    #[test]
    fn immutable_state_root_conflict_compares_raw_canonical_bytes() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("raw-byte-conflict.sqlite");
        let domain = "domain:raw-byte-conflict";
        let store = InterceptingSqliteStore {
            inner: SqliteStore::open(&path, domain).expect("SQLite store opens"),
            interception: InitialCommitInterception::NoncanonicalManifest,
        };
        let Err(error) = DurableStoreControl::initialize(store) else {
            panic!("conflicting immutable bytes cannot publish a semantic head");
        };
        assert!(matches!(error, DurableError::Integrity { .. }));
        let connection = Connection::open(&path).expect("raw connection opens");
        let head_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM cymule_heads WHERE domain=?1",
                [domain],
                |row| row.get(0),
            )
            .expect("head count reads");
        assert_eq!(head_count, 0);
    }

    #[test]
    fn reconciliation_response_loss_remains_same_head_retryable() {
        let domain = "domain:gc-reconcile-response";
        let store = SqliteStore::in_memory(domain).expect("in-memory SQLite store opens");
        let control =
            DurableStoreControl::initialize(store).expect("zero-Run authority initializes");
        let mut store = control.into_store();
        let orphan_id = format!("sha256:{}", "9".repeat(64));
        store
            .connection
            .execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES (?1,?2,x'7b7d')",
                (domain, &orphan_id),
            )
            .expect("cold orphan inserts");
        let receipt = advance_current(&mut store).expect("GC generation commits");
        let expected = store
            .load_full_audit()
            .expect("GC state loads")
            .expect("GC state exists")
            .head;
        store
            .connection
            .execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES (?1,?2,x'7b7d')",
                (domain, &orphan_id),
            )
            .expect("authorized object reappears");

        let error = store
            .reconcile_cold_reclamation_with_barriers(&expected, |barrier| {
                if barrier == SqliteBarrier::GcReconcileCommitComplete {
                    return Err(DurableError::Substrate {
                        code: SQLITE_INJECTED_RECONCILE_FAILURE_CODE.to_owned(),
                        message: "simulated reconciliation response loss".to_owned(),
                    });
                }
                Ok(())
            })
            .expect_err("same-head reconciliation response is lost");
        assert!(matches!(
            error,
            DurableError::Substrate { code, .. }
                if code == SQLITE_INJECTED_RECONCILE_FAILURE_CODE
        ));
        assert_eq!(
            store
                .load_full_audit()
                .expect("state reloads")
                .expect("state exists")
                .head,
            expected
        );
        assert_eq!(
            reconcile_current(&mut store).expect("same receipt retries idempotently"),
            receipt
        );
    }

    #[test]
    fn physical_blob_gate_rejects_oversize_and_length_mismatch() {
        assert!(matches!(
            bounded_blob(Some((5, None)), 4, "test_blob_bound", "test BLOB"),
            Err(DurableError::Integrity { .. })
        ));
        assert!(matches!(
            bounded_blob(
                Some((4, Some(vec![0; 3]))),
                4,
                "test_blob_bound",
                "test BLOB"
            ),
            Err(DurableError::Integrity { .. })
        ));
        assert_eq!(
            bounded_blob(
                Some((4, Some(vec![0; 4]))),
                4,
                "test_blob_bound",
                "test BLOB",
            )
            .expect("bounded BLOB reads"),
            Some(vec![0; 4])
        );
    }

    #[test]
    fn gc_deletes_only_receipt_authorized_candidates() {
        let mut store = SqliteStore::in_memory("domain:bounded-gc").expect("in-memory store opens");
        let transaction = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("GC test transaction begins");
        let first = format!("sha256:{}", "1".repeat(64));
        let second = format!("sha256:{}", "2".repeat(64));
        for object_id in [&first, &second] {
            transaction
                .execute(
                    "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                     VALUES ('domain:bounded-gc',?1,x'7b7d')",
                    [object_id],
                )
                .expect("candidate inserts");
        }
        assert_eq!(
            delete_receipt_objects(
                &transaction,
                "domain:bounded-gc",
                &BTreeSet::from([first.clone()]),
            )
            .expect("authorized batch deletes"),
            1
        );
        transaction.commit().expect("GC test transaction commits");
        let retained = list_ids(&store.connection, STATE_ROOT_TABLE, "domain:bounded-gc")
            .expect("remaining candidates list");
        assert_eq!(retained, [second]);
    }

    #[test]
    fn gc_inventory_prioritizes_receipt_lifecycle_before_ordinary_prefix() {
        let store = SqliteStore::in_memory("domain:gc-priority").expect("in-memory store opens");
        let candidates =
            i64::try_from(MAX_GC_RECLAIMED_OBJECTS).expect("candidate bound fits SQLite integer");
        store
            .connection
            .execute(
                "WITH RECURSIVE ids(value) AS (
                     SELECT 0
                     UNION ALL
                     SELECT value + 1 FROM ids WHERE value + 1 < ?1
                 )
                 INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 SELECT 'domain:gc-priority', 'sha256:' || printf('%064x', value), x'7b7d'
                 FROM ids",
                [candidates],
            )
            .expect("ordinary candidates insert");
        let receipt_ids = [
            format!("sha256:{}", "e".repeat(64)),
            format!("sha256:{}", "f".repeat(64)),
        ];
        for receipt_id in &receipt_ids {
            store
                .connection
                .execute(
                    "INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
                     VALUES ('domain:gc-priority',?1,x'7b7d')",
                    [receipt_id],
                )
                .expect("receipt-family candidate inserts");
        }

        let inventory = collect_gc_candidate_inventory(
            &store.connection,
            "domain:gc-priority",
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
        )
        .expect("GC inventory selects a bounded page");
        assert_eq!(inventory.prefix.len(), MAX_GC_RECLAIMED_OBJECTS);
        assert_eq!(inventory.remaining_objects, 2);
        for receipt_id in receipt_ids {
            assert!(inventory.prefix.contains(&receipt_id));
        }
        assert!(inventory.prefix.contains(&format!("sha256:{:064x}", 0)));
        assert!(
            !inventory
                .prefix
                .contains(&format!("sha256:{:064x}", MAX_GC_RECLAIMED_OBJECTS - 1))
        );
    }

    #[test]
    fn gc_inventory_pages_receipt_crash_orphans_without_displacing_pinned_receipt() {
        let store =
            SqliteStore::in_memory("domain:gc-receipt-pages").expect("in-memory store opens");
        let crash_receipts = i64::try_from(MAX_GC_RECLAIMED_OBJECTS + 1)
            .expect("candidate bound fits SQLite integer");
        store
            .connection
            .execute(
                "WITH RECURSIVE ids(value) AS (
                     SELECT 0
                     UNION ALL
                     SELECT value + 1 FROM ids WHERE value + 1 < ?1
                 )
                 INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
                 SELECT 'domain:gc-receipt-pages',
                        'sha256:' || printf('%064x', value), x'7b7d'
                 FROM ids",
                [crash_receipts],
            )
            .expect("crash-orphan receipts insert");
        let pinned = format!("sha256:{}", "f".repeat(64));
        store
            .connection
            .execute(
                "INSERT INTO cymule_gc_receipts(domain,object_id,object_json)
                 VALUES ('domain:gc-receipt-pages',?1,x'7b7d')",
                [&pinned],
            )
            .expect("pinned receipt inserts");
        for ordinary in [
            format!("sha256:{}", "a".repeat(64)),
            format!("sha256:{}", "b".repeat(64)),
        ] {
            store
                .connection
                .execute(
                    "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                     VALUES ('domain:gc-receipt-pages',?1,x'7b7d')",
                    [&ordinary],
                )
                .expect("ordinary candidate inserts");
        }

        let inventory = collect_gc_candidate_inventory(
            &store.connection,
            "domain:gc-receipt-pages",
            &BTreeSet::new(),
            &BTreeSet::new(),
            Some(&pinned),
        )
        .expect("receipt-heavy inventory selects a bounded page");
        assert_eq!(inventory.prefix.len(), MAX_GC_RECLAIMED_OBJECTS);
        assert!(inventory.prefix.contains(&pinned));
        assert_eq!(inventory.remaining_objects, 4);
        assert!(
            !inventory
                .prefix
                .contains(&format!("sha256:{}", "a".repeat(64)))
        );
    }

    #[test]
    fn gc_drains_more_than_one_receipt_bound_across_physical_generations() {
        let mut store = SqliteStore::in_memory("domain:multi-gc").expect("in-memory store opens");
        DurableStoreControl::initialize(&mut store).expect("state initializes");
        let retained_roots = store
            .stats()
            .expect("initial stats read")
            .state_root_objects;
        let candidates = i64::try_from(cymule_durable::MAX_GC_RECLAIMED_OBJECTS + 1)
            .expect("candidate bound fits SQLite integer");
        store
            .connection
            .execute(
                "WITH RECURSIVE ids(value) AS (
                     SELECT 0
                     UNION ALL
                     SELECT value + 1 FROM ids WHERE value + 1 < ?1
                 )
                 INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 SELECT 'domain:multi-gc', 'sha256:' || printf('%064x', value), x'7b7d'
                 FROM ids",
                [candidates],
            )
            .expect("bounded-GC candidates insert");

        let first = advance_current(&mut store).expect("first bounded GC generation commits");
        assert_eq!(
            first.reclaimed_objects,
            u64::try_from(cymule_durable::MAX_GC_RECLAIMED_OBJECTS).expect("GC bound fits u64")
        );
        assert_eq!(first.remaining_objects, 1);
        assert_eq!(
            store
                .stats()
                .expect("first-generation stats read")
                .state_root_objects,
            retained_roots + 1
        );
        assert_eq!(store.stats().expect("receipt stats read").gc_receipts, 1);
        let after_first = store
            .load_full_audit()
            .expect("first-generation state loads")
            .expect("state remains");
        let reconciled = reconcile_current(&mut store)
            .expect("lost acknowledgement reconciles the pinned non-terminal page");
        assert_eq!(reconciled, first);
        assert_eq!(
            store
                .load_full_audit()
                .expect("reconciled state loads")
                .expect("state remains")
                .head,
            after_first.head,
            "reconciliation must not advance a non-terminal page"
        );
        let replayed_id = first
            .reclaimed_ids
            .first()
            .expect("first page reclaims candidates")
            .clone();
        store
            .connection
            .execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES ('domain:multi-gc',?1,x'7b7d')",
                [&replayed_id],
            )
            .expect("previously authorized object reappears");
        let second = advance_current(&mut store).expect("second bounded GC generation commits");
        assert_eq!(second.reclaimed_objects, 2);
        assert!(
            second.reclaimed_ids.contains(&first.receipt_id),
            "the prior pinned receipt consumes one slot in the next page"
        );
        assert!(
            !second.reclaimed_ids.contains(&replayed_id),
            "advance replays the prior page before selecting a fresh page"
        );
        assert_eq!(second.remaining_objects, 0);
        assert_eq!(
            store
                .stats()
                .expect("second-generation stats read")
                .state_root_objects,
            retained_roots
        );
        assert_eq!(store.stats().expect("receipt stats read").gc_receipts, 1);
        assert!(matches!(
            store.reconcile_cold_reclamation_with_barriers(&after_first.head, |_| Ok(())),
            Err(DurableError::Conflict { .. })
        ));
    }

    #[cfg(unix)]
    fn park_at_selected_barrier(
        selected: &str,
        marker: &Path,
        barrier: SqliteBarrier,
    ) -> DurableResult<()> {
        let observed = format!("{barrier:?}");
        if observed == selected {
            fs::write(marker, observed.as_bytes()).map_err(|error| DurableError::Substrate {
                code: SQLITE_TEST_BOUNDARY_IO_CODE.to_owned(),
                message: error.to_string(),
            })?;
            loop {
                thread::park_timeout(Duration::from_mins(1));
            }
        }
        Ok(())
    }

    #[test]
    #[cfg(unix)]
    fn sqlite_internal_process_kill_worker_entry() {
        let Ok(database) = std::env::var("CYMULE_SQLITE_INTERNAL_KILL_DB") else {
            return;
        };
        let domain =
            std::env::var("CYMULE_SQLITE_INTERNAL_KILL_DOMAIN").expect("worker domain exists");
        let operation = std::env::var("CYMULE_SQLITE_INTERNAL_KILL_OPERATION")
            .expect("worker operation exists");
        let selected =
            std::env::var("CYMULE_SQLITE_INTERNAL_KILL_BARRIER").expect("worker barrier exists");
        let marker = PathBuf::from(
            std::env::var("CYMULE_SQLITE_INTERNAL_KILL_MARKER").expect("worker marker exists"),
        );
        let mut store = SqliteStore::open(&database, &domain).expect("worker store opens");
        match operation.as_str() {
            "commit" => {
                let store = InterceptingSqliteStore {
                    inner: store,
                    interception: InitialCommitInterception::Barrier {
                        selected: selected.clone(),
                        marker: marker.clone(),
                    },
                };
                DurableStoreControl::initialize(store)
                    .expect("worker commit reaches selected barrier");
            }
            "gc" => {
                let expected = store
                    .load_full_audit()
                    .expect("worker state loads")
                    .expect("worker state exists")
                    .head;
                store
                    .advance_cold_reclamation_with_barriers(&expected, |barrier| {
                        park_at_selected_barrier(&selected, &marker, barrier)
                    })
                    .expect("worker GC reaches selected barrier");
            }
            "gc-reconcile" => {
                let expected = store
                    .load_full_audit()
                    .expect("worker state loads")
                    .expect("worker state exists")
                    .head;
                store
                    .reconcile_cold_reclamation_with_barriers(&expected, |barrier| {
                        park_at_selected_barrier(&selected, &marker, barrier)
                    })
                    .expect("worker GC reconciliation reaches selected barrier");
            }
            other => panic!("unknown worker operation {other}"),
        }
        panic!("worker completed without reaching barrier {selected}");
    }

    #[cfg(unix)]
    fn assert_database_integrity(path: &Path) {
        let connection = Connection::open(path).expect("integrity database opens");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode reads");
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        let results = connection
            .prepare("PRAGMA integrity_check")
            .expect("integrity statement prepares")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("integrity check runs")
            .collect::<Result<Vec<_>, _>>()
            .expect("integrity rows read");
        assert_eq!(results, ["ok"]);
        connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect("WAL checkpoint completes");
        let after_checkpoint: String = connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .expect("post-checkpoint integrity check runs");
        assert_eq!(after_checkpoint, "ok");
    }

    #[cfg(unix)]
    fn spawn_barrier_worker(
        database: &Path,
        domain: &str,
        operation: &str,
        barrier: SqliteBarrier,
        marker: &Path,
    ) -> ManagedChild {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("tests::sqlite_internal_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_SQLITE_INTERNAL_KILL_DB", database)
            .env("CYMULE_SQLITE_INTERNAL_KILL_DOMAIN", domain)
            .env("CYMULE_SQLITE_INTERNAL_KILL_OPERATION", operation)
            .env(
                "CYMULE_SQLITE_INTERNAL_KILL_BARRIER",
                format!("{barrier:?}"),
            )
            .env("CYMULE_SQLITE_INTERNAL_KILL_MARKER", marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        ManagedChild::spawn(&mut command).expect("barrier worker starts")
    }

    #[test]
    #[cfg(unix)]
    fn process_kill_boundaries_preserve_atomic_state_root_commit() {
        for (seed, barrier, committed) in [
            (90_101, SqliteBarrier::ObjectsStaged, false),
            (90_102, SqliteBarrier::HeadStaged, false),
            (90_103, SqliteBarrier::CommitComplete, true),
        ] {
            let world = TestWorld::new(seed).expect("test world creates");
            let database = world
                .domain()
                .path("state-root.sqlite")
                .expect("database path resolves");
            let marker = world
                .domain()
                .path("barrier")
                .expect("marker path resolves");
            drop(SqliteStore::open(&database, "domain:kill").expect("schema initializes"));
            let mut child =
                spawn_barrier_worker(&database, "domain:kill", "commit", barrier, &marker);
            child
                .wait_for_content(
                    &marker,
                    format!("{barrier:?}").as_bytes(),
                    Duration::from_secs(20),
                )
                .expect("worker reaches exact commit barrier");
            assert_eq!(
                child.terminate().expect("worker is reaped").signal(),
                Some(9)
            );
            assert_database_integrity(&database);
            let mut store =
                SqliteStore::open(&database, "domain:kill").expect("store reopens after kill");
            let retained = store.load_full_audit().expect("post-kill load runs");
            assert_eq!(retained.is_some(), committed);
            let connection = Connection::open(&database).expect("readback database opens");
            let object_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM cymule_state_root_objects
                     WHERE domain='domain:kill'",
                    [],
                    |row| row.get(0),
                )
                .expect("object count reads");
            if let Some(retained) = retained {
                retained.verify().expect("committed state verifies");
                assert!(object_count > 0);
                let raw: Vec<u8> = connection
                    .query_row(
                        "SELECT head_json FROM cymule_heads WHERE domain='domain:kill'",
                        [],
                        |row| row.get(0),
                    )
                    .expect("head bytes read");
                assert_eq!(
                    raw,
                    cymule_core::canonical_bytes(&retained.head).expect("head encodes")
                );
            } else {
                assert_eq!(object_count, 0);
            }
        }
    }

    #[cfg(unix)]
    fn reconcile_committed_gc_after_process_death(
        store: &mut SqliteStore,
        retained: &StoredState,
        committed: bool,
    ) {
        if !committed {
            return;
        }
        let reconciled =
            reconcile_current(store).expect("post-kill lost acknowledgement reconciles");
        assert_eq!(
            retained.head.gc_receipt.as_deref(),
            Some(reconciled.receipt_id.as_str())
        );
        assert_eq!(
            store
                .load_full_audit()
                .expect("reconciled state loads")
                .expect("state remains")
                .head,
            retained.head
        );
    }

    #[test]
    #[cfg(unix)]
    fn process_kill_boundaries_preserve_atomic_gc_receipt_sweep_and_head() {
        for (seed, barrier, committed) in [
            (90_201, SqliteBarrier::GcAdvanceReceiptStaged, false),
            (90_202, SqliteBarrier::GcAdvanceSweepStaged, false),
            (90_203, SqliteBarrier::GcAdvanceHeadStaged, false),
            (90_204, SqliteBarrier::GcAdvanceCommitComplete, true),
        ] {
            let world = TestWorld::new(seed).expect("test world creates");
            let database = world
                .domain()
                .path("gc.sqlite")
                .expect("database path resolves");
            let marker = world
                .domain()
                .path("barrier")
                .expect("marker path resolves");
            let domain = "domain:gc-kill";
            let mut setup = SqliteStore::open(&database, domain).expect("setup store opens");
            DurableStoreControl::initialize(&mut setup).expect("state initializes");
            let before = setup
                .load_full_audit()
                .expect("setup state loads")
                .expect("setup state exists")
                .head;
            drop(setup);
            let orphan_id = format!("sha256:{}", "7".repeat(64));
            Connection::open(&database)
                .expect("raw database opens")
                .execute(
                    "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                     VALUES (?1,?2,?3)",
                    (domain, &orphan_id, b"{}".as_slice()),
                )
                .expect("unreachable orphan inserts");
            let mut child = spawn_barrier_worker(&database, domain, "gc", barrier, &marker);
            child
                .wait_for_content(
                    &marker,
                    format!("{barrier:?}").as_bytes(),
                    Duration::from_secs(20),
                )
                .expect("worker reaches exact GC barrier");
            assert_eq!(
                child.terminate().expect("worker is reaped").signal(),
                Some(9)
            );
            assert_database_integrity(&database);
            let mut store = SqliteStore::open(&database, domain).expect("store reopens after kill");
            let retained = store
                .load_full_audit()
                .expect("post-kill load succeeds")
                .expect("state remains");
            retained.verify().expect("retained state verifies");
            assert_eq!(retained.head.revision, before.revision);
            assert_eq!(retained.head.sequence, before.sequence);
            assert_eq!(
                retained.head.gc_sequence,
                before.gc_sequence + u64::from(committed)
            );
            assert_eq!(
                retained.head.physical_token == before.physical_token,
                !committed
            );
            reconcile_committed_gc_after_process_death(&mut store, &retained, committed);
            let connection = Connection::open(&database).expect("readback database opens");
            let orphan_exists: bool = connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM cymule_state_root_objects
                        WHERE domain=?1 AND object_id=?2
                     )",
                    (domain, &orphan_id),
                    |row| row.get(0),
                )
                .expect("orphan readback runs");
            assert_eq!(orphan_exists, !committed);
            let receipt_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM cymule_gc_receipts WHERE domain=?1",
                    [domain],
                    |row| row.get(0),
                )
                .expect("receipt count reads");
            assert_eq!(receipt_count, i64::from(committed));
            assert_eq!(
                head_bytes_for_test(&connection, domain),
                cymule_core::canonical_bytes(&retained.head).expect("head encodes")
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn process_kill_boundaries_preserve_same_head_gc_reconciliation() {
        for (seed, barrier, deletion_committed) in [
            (90_301, SqliteBarrier::GcReconcileSweepStaged, false),
            (90_302, SqliteBarrier::GcReconcileCommitComplete, true),
        ] {
            let world = TestWorld::new(seed).expect("test world creates");
            let database = world
                .domain()
                .path("gc-reconcile.sqlite")
                .expect("database path resolves");
            let marker = world
                .domain()
                .path("barrier")
                .expect("marker path resolves");
            let domain = "domain:gc-reconcile-kill";
            let mut setup = SqliteStore::open(&database, domain).expect("setup store opens");
            DurableStoreControl::initialize(&mut setup).expect("state initializes");
            let orphan_id = format!("sha256:{}", "3".repeat(64));
            let raw = Connection::open(&database).expect("raw database opens");
            raw.execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES (?1,?2,x'7b7d')",
                (domain, &orphan_id),
            )
            .expect("orphan inserts");
            advance_current(&mut setup).expect("GC generation commits");
            let expected = setup
                .load_full_audit()
                .expect("post-GC state loads")
                .expect("state exists")
                .head;
            raw.execute(
                "INSERT INTO cymule_state_root_objects(domain,object_id,object_json)
                 VALUES (?1,?2,x'7b7d')",
                (domain, &orphan_id),
            )
            .expect("authorized object reappears");
            drop(setup);

            let mut child =
                spawn_barrier_worker(&database, domain, "gc-reconcile", barrier, &marker);
            child
                .wait_for_content(
                    &marker,
                    format!("{barrier:?}").as_bytes(),
                    Duration::from_secs(20),
                )
                .expect("worker reaches exact reconciliation barrier");
            assert_eq!(
                child.terminate().expect("worker is reaped").signal(),
                Some(9)
            );
            assert_database_integrity(&database);
            let mut store = SqliteStore::open(&database, domain).expect("store reopens after kill");
            let retained = store
                .load_full_audit()
                .expect("post-kill state loads")
                .expect("state remains");
            assert_eq!(retained.head, expected);
            let connection = Connection::open(&database).expect("readback database opens");
            let orphan_exists: bool = connection
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM cymule_state_root_objects
                        WHERE domain=?1 AND object_id=?2
                     )",
                    (domain, &orphan_id),
                    |row| row.get(0),
                )
                .expect("orphan readback runs");
            assert_eq!(orphan_exists, !deletion_committed);
            let receipt_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM cymule_gc_receipts WHERE domain=?1",
                    [domain],
                    |row| row.get(0),
                )
                .expect("receipt count reads");
            assert_eq!(receipt_count, 1);
            assert_eq!(
                head_bytes_for_test(&connection, domain),
                cymule_core::canonical_bytes(&expected).expect("head encodes")
            );
        }
    }

    #[cfg(unix)]
    fn head_bytes_for_test(connection: &Connection, domain: &str) -> Vec<u8> {
        connection
            .query_row(
                "SELECT head_json FROM cymule_heads WHERE domain=?1",
                [domain],
                |row| row.get(0),
            )
            .expect("head bytes read")
    }
}
