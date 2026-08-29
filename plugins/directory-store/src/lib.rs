//! Atomic directory realization of the content-addressed `DurableStore` contract.

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
use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! run_test_crash_boundary {
    ($boundary:expr) => {{
        #[cfg(test)]
        test_crash_boundary($boundary)?;
        #[cfg(not(test))]
        let _ = $boundary;
    }};
}

macro_rules! run_test_failure_boundary {
    ($boundary:expr) => {{
        #[cfg(test)]
        test_failure_boundary($boundary)?;
        #[cfg(not(test))]
        let _ = $boundary;
    }};
}

/// Stable code returned before mutation for every unsupported physical generation.
pub const UNSUPPORTED_STORE_GENERATION_CODE: &str = "unsupported_store_generation";

const DIRECTORY_SCHEMA_VERSION: &str = "cymule.directory-store/5";
const DIRECTORY_META_FILE: &str = "store-meta.json";
const DIRECTORY_META_STAGING_FILE: &str = "store-meta.next";
const DIRECTORY_BOOTSTRAP_MARKER: &str = "cymule.directory-store-5";
const OBJECT_LOCK_FILE: &str = "objects.lock";
const OBJECT_STAGING_DIRECTORY: &str = "object-staging";
const STATE_ROOT_FAMILY: &str = "state-root-objects";
const GC_RECEIPT_FAMILY: &str = "gc-receipts";
const COMMAND_ARCHIVE_FAMILY: &str = "command-archives";
const DIRECTORY_FAMILIES: [&str; 3] =
    [STATE_ROOT_FAMILY, GC_RECEIPT_FAMILY, COMMAND_ARCHIVE_FAMILY];

const DIRECTORY_META_MAX_BYTES: u64 = 1_024;
const DIRECTORY_GC_PAGE_OBJECTS: usize = 1_024;
const HEAD_INTEGRITY_CODE: &str = "directory_head_object";
const STATE_ROOT_LOCATOR_INTEGRITY_CODE: &str = "directory_state_root_object_locator";
const STATE_ROOT_KIND_INTEGRITY_CODE: &str = "directory_state_root_manifest_kind";
const GC_RECEIPT_LOCATOR_INTEGRITY_CODE: &str = "directory_gc_receipt_locator";
const GC_RECEIPT_HEAD_INTEGRITY_CODE: &str = "directory_gc_receipt_head";
const COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE: &str = "directory_command_archive_object_locator";
const COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE: &str = "directory_command_archive_batch_index";
const PHYSICAL_ALIAS_INTEGRITY_CODE: &str = "directory_physical_object_alias";
const CLEANUP_IO_CODE: &str = "directory_cleanup_io";
const INVENTORY_IO_CODE: &str = "directory_inventory_io";
const OBJECT_READ_IO_CODE: &str = "directory_object_read_io";
const OBJECT_WRITE_IO_CODE: &str = "directory_object_write_io";
const HEAD_PUBLISH_IO_CODE: &str = "directory_head_publish_io";
const LOCK_IO_CODE: &str = "directory_lock_io";
const LOCK_RELEASE_IO_CODE: &str = "directory_lock_release_io";
const LAYOUT_IO_CODE: &str = "directory_layout_io";
const DIRECTORY_SYNC_IO_CODE: &str = "directory_sync_io";
#[cfg(test)]
const TEST_BOUNDARY_IO_CODE: &str = "directory_test_boundary_io";
#[cfg(test)]
const INJECTED_FAILURE_CODE: &str = "directory_injected_failure";
#[cfg(test)]
const INJECTED_HEAD_SYNC_FAILURE_CODE: &str = "directory_injected_head_sync_failure";
static OBJECT_STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
std::thread_local! {
    static GC_RECEIPT_READ_COUNT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn state_root_object_max_bytes() -> u64 {
    u64::try_from(MAX_STATE_ROOT_OBJECT_BYTES)
        .expect("StateRoot object bound is representable as a file length")
}

fn store_head_max_bytes() -> u64 {
    u64::try_from(MAX_STORE_HEAD_BYTES).expect("Store head bound is representable as a file length")
}

fn gc_receipt_max_bytes() -> u64 {
    u64::try_from(MAX_GC_RECEIPT_BYTES).expect("GC receipt bound is representable as a file length")
}

fn command_archive_object_max_bytes() -> u64 {
    u64::try_from(MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES)
        .expect("Machine command archive object bound is representable as a file length")
}

const fn command_archive_batch_index_max_bytes() -> u64 {
    512
}

#[derive(Debug, Clone)]
/// Directory-backed content-addressed durable domain.
pub struct DirectoryStore {
    root: PathBuf,
    writable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryStoreMeta {
    schema_version: String,
}

impl DirectoryStoreMeta {
    fn current() -> Self {
        Self {
            schema_version: DIRECTORY_SCHEMA_VERSION.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryGeneration {
    MissingOrEmpty,
    InitializationInProgress,
    Current,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalKind {
    StateRoot,
    GcReceipt,
    ArchiveSegment,
    ArchiveEntry,
    ArchiveBatch,
    ArchiveBatchIndex,
    ArchiveNode,
}

#[derive(Debug, Clone)]
struct PhysicalObject {
    kind: PhysicalKind,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectoryCommandBatchIndex {
    index_version: String,
    batch_id: String,
    batch_receipt_id: String,
}

impl DirectoryCommandBatchIndex {
    const VERSION: &'static str = "cymule.directory-command-batch-index/1";

    fn new(batch: &MachineCommandBatchRecord) -> DurableResult<Self> {
        batch.verify()?;
        let value = Self {
            index_version: Self::VERSION.to_owned(),
            batch_id: batch.batch_id.clone(),
            batch_receipt_id: batch.batch_receipt_id.clone(),
        };
        value.verify()?;
        Ok(value)
    }

    fn verify(&self) -> DurableResult<()> {
        if self.index_version != Self::VERSION {
            return Err(integrity(
                COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                "directory command-batch index has an unsupported generation",
            ));
        }
        if cymule_core::validate_content_id("Machine command batch", &self.batch_id).is_err()
            || cymule_core::validate_content_id(
                "Machine command batch receipt",
                &self.batch_receipt_id,
            )
            .is_err()
        {
            return Err(integrity(
                COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                "directory command-batch index contains a malformed stable or receipt identity",
            ));
        }
        Ok(())
    }
}

struct ReclamationPage {
    required_id: Option<String>,
    required_seen: bool,
    receipt_ids: BTreeSet<String>,
    ordinary_ids: BTreeSet<String>,
    total_candidates: u64,
}

impl ReclamationPage {
    fn new(required_id: Option<&str>) -> Self {
        Self {
            required_id: required_id.map(str::to_owned),
            required_seen: false,
            receipt_ids: BTreeSet::new(),
            ordinary_ids: BTreeSet::new(),
            total_candidates: 0,
        }
    }

    fn insert_smallest(ids: &mut BTreeSet<String>, id: &str, limit: usize) {
        if limit == 0 || ids.contains(id) {
            return;
        }
        if ids.len() < limit {
            ids.insert(id.to_owned());
        } else if let Some(last) = ids.last().cloned()
            && id < last.as_str()
        {
            ids.remove(&last);
            ids.insert(id.to_owned());
        }
    }

    fn ordinary_limit(&self) -> DurableResult<usize> {
        let reserved = usize::from(self.required_id.is_some())
            .checked_add(self.receipt_ids.len())
            .ok_or_else(|| {
                DurableError::Validation(
                    "directory GC receipt-page reservation count overflowed".to_owned(),
                )
            })?;
        DIRECTORY_GC_PAGE_OBJECTS
            .min(MAX_GC_RECLAIMED_OBJECTS)
            .checked_sub(reserved)
            .ok_or_else(|| {
                DurableError::Validation(
                    "directory GC receipt-page reservations exceed the exact page bound".to_owned(),
                )
            })
    }

    fn trim_ordinary(&mut self) -> DurableResult<()> {
        let limit = self.ordinary_limit()?;
        while self.ordinary_ids.len() > limit {
            self.ordinary_ids.pop_last();
        }
        Ok(())
    }

    fn consider(
        &mut self,
        id: &str,
        kind: PhysicalKind,
        retained: &BTreeSet<String>,
    ) -> DurableResult<()> {
        if retained.contains(id) {
            return Ok(());
        }
        self.total_candidates = self
            .total_candidates
            .checked_add(1)
            .filter(|count| *count <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                DurableError::Validation(
                    "directory GC candidate count exceeds the exact integer range".to_owned(),
                )
            })?;

        if self.required_id.as_deref() == Some(id) {
            if kind != PhysicalKind::GcReceipt {
                return Err(integrity(
                    "directory_gc_receipt_inventory",
                    "head-pinned GC receipt resolves through a non-receipt physical family",
                ));
            }
            self.required_seen = true;
            return Ok(());
        }

        let page_limit = DIRECTORY_GC_PAGE_OBJECTS.min(MAX_GC_RECLAIMED_OBJECTS);
        if kind == PhysicalKind::GcReceipt {
            let receipt_limit = page_limit
                .checked_sub(usize::from(self.required_id.is_some()))
                .ok_or_else(|| {
                    DurableError::Validation(
                        "directory GC required receipt exceeds the exact page bound".to_owned(),
                    )
                })?;
            Self::insert_smallest(&mut self.receipt_ids, id, receipt_limit);
            self.trim_ordinary()?;
            return Ok(());
        }

        let ordinary_limit = self.ordinary_limit()?;
        Self::insert_smallest(&mut self.ordinary_ids, id, ordinary_limit);
        Ok(())
    }

    fn finish(mut self) -> DurableResult<(BTreeSet<String>, u64)> {
        if self.required_id.is_some() && !self.required_seen {
            return Err(integrity(
                "directory_gc_receipt_missing",
                "head-pinned GC receipt disappeared from the exact physical inventory",
            ));
        }
        if let Some(required_id) = self.required_id {
            self.receipt_ids.insert(required_id);
        }
        self.receipt_ids.append(&mut self.ordinary_ids);
        let selected = u64::try_from(self.receipt_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let remaining = self.total_candidates.checked_sub(selected).ok_or_else(|| {
            integrity(
                "directory_gc_candidate_count",
                "bounded GC page exceeds its exact candidate inventory",
            )
        })?;
        Ok((self.receipt_ids, remaining))
    }
}

struct DirectoryStateRootResolver<'a> {
    store: &'a DirectoryStore,
    pinned_manifest_id: String,
}

impl StateRootResolver for DirectoryStateRootResolver<'_> {
    fn pinned_manifest_id(&self) -> &str {
        &self.pinned_manifest_id
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        self.store.read_state_root_object(object_id)
    }
}

impl DirectoryStore {
    /// Open or initialize the exact current physical generation.
    ///
    /// # Errors
    ///
    /// Returns an error when the root is unsafe, belongs to another
    /// generation, or cannot be synchronized durably.
    pub fn open(root: impl AsRef<Path>) -> DurableResult<Self> {
        #[cfg(unix)]
        {
            Self::open_with_directory_sync(root, sync_directory)
        }
        #[cfg(not(unix))]
        {
            let _ = root;
            Err(unsupported_generation(
                "writable directory-store/5 requires Unix directory fsync and atomic replacement",
            ))
        }
    }

    fn open_with_directory_sync(
        root: impl AsRef<Path>,
        mut sync_parent: impl FnMut(&Path) -> DurableResult<()>,
    ) -> DurableResult<Self> {
        let root = absolute_root(root.as_ref())?;
        match inspect_directory_generation(&root)? {
            DirectoryGeneration::Current => {
                require_current_directory_layout(&root)?;
                ensure_directory_durable(&root, &mut sync_parent)?;
                ensure_lock_file_durable(&root, "head.lock", &mut sync_parent)?;
                ensure_lock_file_durable(&root, OBJECT_LOCK_FILE, &mut sync_parent)?;
                sync_parent(&root)?;
                require_writable_directory_layout(&root)?;
            }
            DirectoryGeneration::MissingOrEmpty | DirectoryGeneration::InitializationInProgress => {
                initialize_directory_layout(&root, &mut sync_parent)?;
            }
        }
        Ok(Self {
            root,
            writable: true,
        })
    }

    /// Open an existing current-generation store without mutating it.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing, partial, unsafe, or unsupported layout.
    pub fn open_read_only(root: impl AsRef<Path>) -> DurableResult<Self> {
        let root = absolute_root(root.as_ref())?;
        match inspect_directory_generation(&root)? {
            DirectoryGeneration::Current => require_current_directory_layout(&root)?,
            DirectoryGeneration::MissingOrEmpty => {
                return Err(DurableError::NotFound(if root.exists() {
                    "directory durable store root has no current-generation authority".to_owned()
                } else {
                    "directory durable store root does not exist".to_owned()
                }));
            }
            DirectoryGeneration::InitializationInProgress => {
                return Err(unsupported_generation(
                    "directory current-generation initialization is incomplete",
                ));
            }
        }
        Ok(Self {
            root,
            writable: false,
        })
    }

    /// Return the store root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn head_path(&self) -> PathBuf {
        self.root.join("head.json")
    }

    fn object_path(&self, family: &str, id: &str) -> PathBuf {
        self.root
            .join(family)
            .join(cymule_core::sha256_bytes(id.as_bytes()))
    }

    fn state_root_object_path(&self, id: &str) -> PathBuf {
        self.object_path(STATE_ROOT_FAMILY, id)
    }

    fn gc_receipt_path(&self, id: &str) -> PathBuf {
        self.object_path(GC_RECEIPT_FAMILY, id)
    }

    fn command_archive_object_path(&self, kind: CommandArchiveObjectKind, id: &str) -> PathBuf {
        self.root.join(COMMAND_ARCHIVE_FAMILY).join(format!(
            "{}-{}",
            kind.filename_prefix(),
            cymule_core::sha256_bytes(id.as_bytes())
        ))
    }

    fn command_archive_batch_index_path(&self, batch_id: &str) -> PathBuf {
        self.root.join(COMMAND_ARCHIVE_FAMILY).join(format!(
            "batch-index-{}",
            cymule_core::sha256_bytes(batch_id.as_bytes())
        ))
    }

    fn claim(&self) -> DurableResult<File> {
        require_writable_directory_layout(&self.root)?;
        acquire_writer_lock(&self.root.join("head.lock"), false)
    }

    fn claim_objects(&self) -> DurableResult<File> {
        require_writable_directory_layout(&self.root)?;
        acquire_named_lock(
            &self.root.join(OBJECT_LOCK_FILE),
            "object_stager_available",
            "object_stager_active",
        )
    }

    fn read_head(&self) -> DurableResult<Option<StoreHead>> {
        require_current_directory_layout(&self.root)?;
        let head: Option<StoreHead> = read_optional_canonical(
            &self.head_path(),
            store_head_max_bytes(),
            HEAD_INTEGRITY_CODE,
            "directory Store head",
        )?;
        if let Some(head) = &head {
            head.verify().map_err(|error| {
                integrity(
                    HEAD_INTEGRITY_CODE,
                    format!("directory Store head is invalid: {error}"),
                )
            })?;
        }
        Ok(head)
    }

    fn read_state_root_object(&self, id: &str) -> DurableResult<Option<StateRootObject>> {
        require_family_directory(&self.root.join(STATE_ROOT_FAMILY))?;
        let path = self.state_root_object_path(id);
        let value = read_optional_state_root_object(
            &path,
            STATE_ROOT_LOCATOR_INTEGRITY_CODE,
            "directory state-root object",
        )?;
        if let Some(value) = &value {
            value.verify().map_err(|error| {
                integrity(
                    STATE_ROOT_LOCATOR_INTEGRITY_CODE,
                    format!("directory state-root object is invalid: {error}"),
                )
            })?;
            if value.object_id() != id || self.state_root_object_path(value.object_id()) != path {
                return Err(integrity(
                    STATE_ROOT_LOCATOR_INTEGRITY_CODE,
                    "directory state-root object locator does not match its content identity",
                ));
            }
            self.require_unique_physical_locator(id, &path)?;
        }
        Ok(value)
    }

    fn read_state_root_manifest(&self, id: &str) -> DurableResult<Option<StateRootManifest>> {
        self.read_state_root_object(id)?
            .map(|object| match object {
                StateRootObject::Manifest(manifest) => Ok(manifest),
                _ => Err(integrity(
                    STATE_ROOT_KIND_INTEGRITY_CODE,
                    format!("state-root manifest locator {id} resolves to another object kind"),
                )),
            })
            .transpose()
    }

    fn read_gc_receipt(&self, id: &str) -> DurableResult<Option<GcReceipt>> {
        #[cfg(test)]
        GC_RECEIPT_READ_COUNT.with(|count| count.set(count.get() + 1));
        require_family_directory(&self.root.join(GC_RECEIPT_FAMILY))?;
        let path = self.gc_receipt_path(id);
        let value: Option<GcReceipt> = read_optional_canonical(
            &path,
            gc_receipt_max_bytes(),
            GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
            "directory GC receipt",
        )?;
        if let Some(value) = &value {
            value.verify_identity().map_err(|error| {
                integrity(
                    GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                    format!("directory GC receipt is invalid: {error}"),
                )
            })?;
            if value.receipt_id != id || self.gc_receipt_path(&value.receipt_id) != path {
                return Err(integrity(
                    GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                    "directory GC receipt locator does not match its content identity",
                ));
            }
            self.require_unique_physical_locator(id, &path)?;
        }
        Ok(value)
    }

    fn read_command_archive_object(
        &self,
        kind: CommandArchiveObjectKind,
        id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveObject>> {
        require_family_directory(&self.root.join(COMMAND_ARCHIVE_FAMILY))?;
        let path = self.command_archive_object_path(kind, id);
        let value: Option<MachineCommandArchiveObject> = read_optional_canonical(
            &path,
            command_archive_object_max_bytes(),
            COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
            "directory Machine command archive object",
        )?;
        if let Some(value) = &value {
            let identity = value.identity().map_err(|error| {
                integrity(
                    COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                    format!("directory Machine command archive object is invalid: {error}"),
                )
            })?;
            let actual_kind = CommandArchiveObjectKind::from_object(value);
            if identity != id
                || actual_kind != kind
                || self.command_archive_object_path(actual_kind, &identity) != path
            {
                return Err(integrity(
                    COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                    "directory Machine command archive object locator does not match its content identity and kind",
                ));
            }
            self.require_unique_physical_locator(id, &path)?;
        }
        Ok(value)
    }

    fn read_command_archive_segment(
        &self,
        id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
        self.read_command_archive_object(CommandArchiveObjectKind::Segment, id)?
            .map(|value| match value {
                MachineCommandArchiveObject::Segment(value) => Ok(*value),
                _ => unreachable!("closed command archive kind was checked"),
            })
            .transpose()
    }

    fn read_command_archive_entry(
        &self,
        id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveEntry>> {
        self.read_command_archive_object(CommandArchiveObjectKind::Entry, id)?
            .map(|value| match value {
                MachineCommandArchiveObject::Entry(value) => Ok(*value),
                _ => unreachable!("closed command archive kind was checked"),
            })
            .transpose()
    }

    fn read_command_archive_batch_receipt(
        &self,
        receipt_id: &str,
    ) -> DurableResult<Option<MachineCommandBatchRecord>> {
        self.read_command_archive_object(CommandArchiveObjectKind::Batch, receipt_id)?
            .map(|value| match value {
                MachineCommandArchiveObject::Batch(value) => Ok(*value),
                _ => unreachable!("closed command archive kind was checked"),
            })
            .transpose()
    }

    fn read_command_archive_batch_index(
        &self,
        batch_id: &str,
    ) -> DurableResult<Option<DirectoryCommandBatchIndex>> {
        cymule_core::validate_content_id("Machine command batch", batch_id)?;
        require_family_directory(&self.root.join(COMMAND_ARCHIVE_FAMILY))?;
        let path = self.command_archive_batch_index_path(batch_id);
        let value: Option<DirectoryCommandBatchIndex> = read_optional_canonical(
            &path,
            command_archive_batch_index_max_bytes(),
            COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
            "directory Machine command batch index",
        )?;
        if let Some(value) = &value {
            value.verify()?;
            if value.batch_id != batch_id
                || self.command_archive_batch_index_path(&value.batch_id) != path
            {
                return Err(integrity(
                    COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                    "directory Machine command batch index locator changed its stable batch identity",
                ));
            }
            self.require_unique_physical_locator(batch_id, &path)?;
        }
        Ok(value)
    }

    fn read_command_archive_batch(
        &self,
        batch_id: &str,
    ) -> DurableResult<Option<MachineCommandBatchRecord>> {
        let Some(index) = self.read_command_archive_batch_index(batch_id)? else {
            return Ok(None);
        };
        let batch = self
            .read_command_archive_batch_receipt(&index.batch_receipt_id)?
            .ok_or_else(|| {
                integrity(
                    COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                    format!(
                        "Machine command batch {batch_id} points to missing receipt {}",
                        index.batch_receipt_id
                    ),
                )
            })?;
        if batch.batch_id != batch_id || batch.batch_receipt_id != index.batch_receipt_id {
            return Err(integrity(
                COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                format!("Machine command batch {batch_id} changed its stable or receipt identity"),
            ));
        }
        Ok(Some(batch))
    }

    fn read_command_index_node(&self, id: &str) -> DurableResult<Option<MachineCommandIndexNode>> {
        self.read_command_archive_object(CommandArchiveObjectKind::Node, id)?
            .map(|value| match value {
                MachineCommandArchiveObject::CommandIndexNode(value) => Ok(value),
                _ => unreachable!("closed command archive kind was checked"),
            })
            .transpose()
    }

    fn read_stable_head(&self) -> DurableResult<Option<StoreHead>> {
        let mut observed = (None, None);
        for _ in 0..3 {
            let before = self.read_head()?;
            let after = self.read_head()?;
            if before == after {
                return Ok(before);
            }
            observed = (
                before.map(|head| head.physical_token),
                after.map(|head| head.physical_token),
            );
        }
        Err(DurableError::Conflict {
            expected: observed.0,
            current: observed.1,
        })
    }

    fn stable_manifest(&self, manifest_id: &str) -> DurableResult<Option<StateRootManifest>> {
        let before = self.read_head()?;
        let Some(head) = before.as_ref() else {
            return Ok(None);
        };
        if head.state_root_manifest_id != manifest_id {
            return Ok(None);
        }
        let manifest = self.load_current_head_manifest(head);
        let after = self.read_head()?;
        if before != after {
            return Err(head_conflict(before.as_ref(), after.as_ref()));
        }
        manifest.map(Some)
    }

    fn stable_reachable_ids(
        &self,
        head: &StoreHead,
        retain_gc_receipt: bool,
    ) -> DurableResult<BTreeSet<String>> {
        let retained = (|| {
            let manifest = self.load_current_head_manifest(head)?;
            let mut resolver = DirectoryStateRootResolver {
                store: self,
                pinned_manifest_id: manifest.manifest_id().to_owned(),
            };
            let mut retained = reachable_state_root_objects(&manifest, &mut resolver)?;
            retained.extend(self.retained_machine_command_objects(head)?);
            if retain_gc_receipt && let Some(receipt_id) = &head.gc_receipt {
                retained.insert(receipt_id.clone());
            }
            Ok(retained)
        })();
        let after = self.read_head()?;
        if after.as_ref() != Some(head) {
            return Err(head_conflict(Some(head), after.as_ref()));
        }
        retained
    }

    fn retained_machine_command_objects(
        &self,
        head: &StoreHead,
    ) -> DurableResult<BTreeSet<String>> {
        let Some(anchor) = &head.machine_base_anchor else {
            return Ok(BTreeSet::new());
        };
        let mut segment_batches = BTreeSet::new();
        let mut retained = reachable_machine_command_archive_ids(anchor, |id| {
            let segment = self.read_command_archive_segment(id)?;
            if let Some(segment) = &segment {
                for declared in &segment.batches {
                    let indexed = self
                        .read_command_archive_batch(&declared.batch_id)?
                        .ok_or_else(|| {
                            integrity(
                                "directory_command_archive_closure_missing",
                                format!(
                                    "reachable archive batch {} does not exist",
                                    declared.batch_id
                                ),
                            )
                        })?;
                    if indexed != *declared {
                        return Err(integrity(
                            "directory_command_archive_batch_mismatch",
                            "indexed Machine batch differs from its reachable archive segment",
                        ));
                    }
                    segment_batches.insert(indexed.batch_id);
                    segment_batches.insert(indexed.batch_receipt_id);
                }
            }
            Ok(segment)
        })
        .map_err(map_archive_closure_error)?;
        retained.extend(segment_batches);
        let index = reachable_machine_command_index_objects(&anchor.command_index_root, |id| {
            self.read_command_index_node(id)
        })
        .map_err(map_archive_closure_error)?;
        for entry_id in &index.archive_entry_ids {
            let entry = self.read_command_archive_entry(entry_id)?.ok_or_else(|| {
                integrity(
                    "directory_command_archive_closure_missing",
                    format!("Machine command archive entry {entry_id} does not exist"),
                )
            })?;
            let batch = self
                .read_command_archive_batch(&entry.command.batch_id)?
                .ok_or_else(|| {
                    integrity(
                        "directory_command_archive_closure_missing",
                        format!(
                            "Machine command archive batch {} does not exist",
                            entry.command.batch_id
                        ),
                    )
                })?;
            batch.verify_entry(&entry).map_err(|error| {
                integrity(
                    "directory_command_archive_batch_mismatch",
                    format!("reachable Machine command entry does not match its batch: {error}"),
                )
            })?;
            retained.insert(batch.batch_id.clone());
            retained.insert(batch.batch_receipt_id.clone());
        }
        retained.extend(index.archive_entry_ids);
        retained.extend(index.node_ids);
        Ok(retained)
    }

    fn verify_archive_lookup_head(&self, head: &StoreHead) -> DurableResult<()> {
        self.load_current_head_manifest(head).map(|_| ())
    }

    fn resolve_machine_command_archive_at_head(
        &self,
        head: &StoreHead,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        self.verify_archive_lookup_head(head)?;
        let mut store_error = None;
        let index_proof = cymule_core::resolve_machine_command_index_proof(
            &anchor.command_index_root,
            command_id,
            |node_id| match self.read_command_index_node(node_id) {
                Ok(Some(value)) => Ok(Some(value)),
                Ok(None) => {
                    store_error = Some(integrity(
                        "directory_command_archive_closure_missing",
                        format!("reachable Machine command-index node {node_id} does not exist"),
                    ));
                    Err(cymule_core::CoreError::NotFound(
                        "directory command-index node is missing".to_owned(),
                    ))
                }
                Err(error) => {
                    store_error = Some(error);
                    Err(cymule_core::CoreError::NotFound(
                        "directory command-index lookup failed".to_owned(),
                    ))
                }
            },
        );
        if let Some(error) = store_error {
            return Err(error);
        }
        let index_proof = index_proof?;
        match index_proof.value.as_ref() {
            None => Ok(cymule_core::MachineCommandArchiveLookup::NonMember { index_proof }),
            Some(value) => {
                let entry = self
                    .read_command_archive_entry(&value.archive_entry_digest)?
                    .ok_or_else(|| {
                        integrity(
                            "directory_command_archive_closure_missing",
                            format!(
                                "reachable Machine command archive entry {} does not exist",
                                value.archive_entry_digest
                            ),
                        )
                    })?;
                if entry.identity()? != value.archive_entry_digest {
                    return Err(integrity(
                        COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                        format!(
                            "Machine command archive entry {} does not match its index",
                            value.archive_entry_digest
                        ),
                    ));
                }
                Ok(cymule_core::MachineCommandArchiveLookup::Member {
                    index_proof,
                    entry: Box::new(entry),
                })
            }
        }
    }

    fn clean_head_staging_residue(&self) -> DurableResult<()> {
        if remove_if_present(&self.root.join("head.next"))? {
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    fn clean_object_staging_residue(&self) -> DurableResult<()> {
        let directory = self.root.join(OBJECT_STAGING_DIRECTORY);
        let mut changed = false;
        for entry in fs::read_dir(&directory).map_err(|error| substrate(CLEANUP_IO_CODE, error))? {
            changed |= remove_if_present(
                &entry
                    .map_err(|error| substrate(CLEANUP_IO_CODE, error))?
                    .path(),
            )?;
        }
        if changed {
            sync_directory(&directory)?;
        }
        Ok(())
    }

    fn clean_object_staging_if_idle(&self) -> DurableResult<()> {
        match self.claim_objects() {
            Ok(claim) => finish_locked_operation(claim, self.clean_object_staging_residue()),
            Err(error) if is_object_staging_conflict(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn write_state_root_object(&self, object: &StateRootObject) -> DurableResult<()> {
        object.verify()?;
        self.require_unique_physical_locator(
            object.object_id(),
            &self.state_root_object_path(object.object_id()),
        )?;
        let bytes = cymule_core::canonical_bytes(object)?;
        write_immutable_bytes(
            &self.root.join(OBJECT_STAGING_DIRECTORY),
            &self.state_root_object_path(object.object_id()),
            &bytes,
            state_root_object_max_bytes(),
            STATE_ROOT_LOCATOR_INTEGRITY_CODE,
        )
    }

    fn write_gc_receipt(&self, receipt: &GcReceipt) -> DurableResult<()> {
        receipt.verify_identity()?;
        self.require_unique_physical_locator(
            &receipt.receipt_id,
            &self.gc_receipt_path(&receipt.receipt_id),
        )?;
        let bytes = cymule_core::canonical_bytes(receipt)?;
        write_immutable_bytes(
            &self.root.join(OBJECT_STAGING_DIRECTORY),
            &self.gc_receipt_path(&receipt.receipt_id),
            &bytes,
            gc_receipt_max_bytes(),
            GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
        )
    }

    fn write_command_archive_object(
        &self,
        object: &MachineCommandArchiveObject,
    ) -> DurableResult<()> {
        let identity = object.identity()?;
        let kind = CommandArchiveObjectKind::from_object(object);
        self.require_unique_physical_locator(
            &identity,
            &self.command_archive_object_path(kind, &identity),
        )?;
        let bytes = cymule_core::canonical_bytes(object)?;
        write_immutable_bytes(
            &self.root.join(OBJECT_STAGING_DIRECTORY),
            &self.command_archive_object_path(kind, &identity),
            &bytes,
            command_archive_object_max_bytes(),
            COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
        )?;
        if let MachineCommandArchiveObject::Batch(batch) = object {
            let index = DirectoryCommandBatchIndex::new(batch)?;
            let path = self.command_archive_batch_index_path(&index.batch_id);
            self.require_unique_physical_locator(&index.batch_id, &path)?;
            let bytes = cymule_core::canonical_bytes(&index)?;
            write_immutable_bytes(
                &self.root.join(OBJECT_STAGING_DIRECTORY),
                &path,
                &bytes,
                command_archive_batch_index_max_bytes(),
                COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
            )?;
        }
        Ok(())
    }

    fn compare_and_commit_with_head_sync(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
        sync_head_directory: impl FnOnce(&Path) -> DurableResult<()>,
    ) -> DurableResult<StoreCommit> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only directory stores cannot compare and commit".to_owned(),
            ));
        }
        let observed = self.read_head()?;
        if observed.as_ref() != expected {
            return Err(head_conflict(expected, observed.as_ref()));
        }
        let validation = (|| {
            batch.verify_against(observed.as_ref())?;
            let parent_manifest = observed
                .as_ref()
                .map(|head| self.load_current_head_manifest(head))
                .transpose()?;
            batch
                .state_root_transition()
                .verify(parent_manifest.as_ref())
        })();
        let after_validation = self.read_head()?;
        if observed != after_validation {
            return Err(head_conflict(observed.as_ref(), after_validation.as_ref()));
        }
        validation?;

        let object_claim = self.claim_objects()?;
        let object_operation = (|| {
            self.clean_object_staging_residue()?;
            for object in batch.state_root_transition().objects() {
                self.write_state_root_object(object)?;
            }
            for object in batch.machine_command_archive_objects() {
                self.write_command_archive_object(object)?;
            }
            sync_directory(&self.root.join(STATE_ROOT_FAMILY))?;
            sync_directory(&self.root.join(COMMAND_ARCHIVE_FAMILY))?;
            Ok(())
        })();
        if let Err(error) = object_operation {
            return finish_locked_operation(object_claim, Err(error));
        }
        run_test_crash_boundary!("commit_objects_durable");

        let claim = match self.claim() {
            Ok(claim) => claim,
            Err(error) => return finish_locked_operation(object_claim, Err(error)),
        };
        let operation = (|| {
            self.clean_head_staging_residue()?;
            let current = self.read_head()?;
            if current.as_ref() != expected {
                return Err(head_conflict(expected, current.as_ref()));
            }
            if let Some(current) = current.as_ref() {
                self.load_current_head_manifest(current)?;
            }
            sync_directory(&self.root.join(STATE_ROOT_FAMILY))?;
            sync_directory(&self.root.join(COMMAND_ARCHIVE_FAMILY))?;
            write_head_atomic(
                &self.head_path(),
                batch.head(),
                sync_head_directory,
                "commit_head_staged",
                "commit_head_renamed",
                "commit_head_durable",
            )?;
            Ok(StoreCommit {
                revision: batch.head().revision.clone(),
                head: batch.head().clone(),
            })
        })();
        let published = finish_publishing_operation(claim, operation);
        finish_publishing_operation(object_claim, published)
    }

    fn inspect_state_root_path(&self, path: PathBuf) -> DurableResult<(String, PhysicalObject)> {
        let filename = require_digest_filename(
            &path,
            "state-root object",
            STATE_ROOT_LOCATOR_INTEGRITY_CODE,
        )?;
        let object = read_required_state_root_object(
            &path,
            STATE_ROOT_LOCATOR_INTEGRITY_CODE,
            "directory state-root object",
        )?;
        object.verify().map_err(|error| {
            integrity(
                STATE_ROOT_LOCATOR_INTEGRITY_CODE,
                format!("directory state-root object is invalid: {error}"),
            )
        })?;
        let id = object.object_id().to_owned();
        if filename != cymule_core::sha256_bytes(id.as_bytes())
            || self.state_root_object_path(&id) != path
        {
            return Err(integrity(
                STATE_ROOT_LOCATOR_INTEGRITY_CODE,
                "directory state-root filename does not match its content identity",
            ));
        }
        self.require_unique_physical_locator(&id, &path)?;
        Ok((
            id,
            PhysicalObject {
                kind: PhysicalKind::StateRoot,
                path,
            },
        ))
    }

    fn inspect_gc_receipt_path(&self, path: PathBuf) -> DurableResult<(String, PhysicalObject)> {
        let filename =
            require_digest_filename(&path, "GC receipt", GC_RECEIPT_LOCATOR_INTEGRITY_CODE)?;
        let receipt: GcReceipt = read_required_canonical(
            &path,
            gc_receipt_max_bytes(),
            GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
            "directory GC receipt",
        )?;
        receipt.verify_identity().map_err(|error| {
            integrity(
                GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                format!("directory GC receipt is invalid: {error}"),
            )
        })?;
        if filename != cymule_core::sha256_bytes(receipt.receipt_id.as_bytes())
            || self.gc_receipt_path(&receipt.receipt_id) != path
        {
            return Err(integrity(
                GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                "directory GC receipt filename does not match its content identity",
            ));
        }
        let id = receipt.receipt_id;
        self.require_unique_physical_locator(&id, &path)?;
        Ok((
            id,
            PhysicalObject {
                kind: PhysicalKind::GcReceipt,
                path,
            },
        ))
    }

    fn inspect_command_archive_path(
        &self,
        path: PathBuf,
    ) -> DurableResult<(String, PhysicalObject)> {
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                integrity(
                    COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                    "directory command archive filename is not Unicode",
                )
            })?;
        if filename
            .strip_prefix("batch-index-")
            .is_some_and(is_lower_hex_digest)
        {
            let index: DirectoryCommandBatchIndex = read_required_canonical(
                &path,
                command_archive_batch_index_max_bytes(),
                COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                "directory Machine command batch index",
            )?;
            index.verify()?;
            if self.command_archive_batch_index_path(&index.batch_id) != path {
                return Err(integrity(
                    COMMAND_ARCHIVE_BATCH_INDEX_INTEGRITY_CODE,
                    "directory command-batch index filename does not match its stable batch identity",
                ));
            }
            self.require_unique_physical_locator(&index.batch_id, &path)?;
            return Ok((
                index.batch_id,
                PhysicalObject {
                    kind: PhysicalKind::ArchiveBatchIndex,
                    path,
                },
            ));
        }
        let filename_kind = CommandArchiveObjectKind::from_filename(filename).ok_or_else(|| {
            integrity(
                COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                format!("directory command archive filename {filename} has no closed kind"),
            )
        })?;
        let object: MachineCommandArchiveObject = read_required_canonical(
            &path,
            command_archive_object_max_bytes(),
            COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
            "directory Machine command archive object",
        )?;
        let id = object.identity().map_err(|error| {
            integrity(
                COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                format!("directory Machine command archive object is invalid: {error}"),
            )
        })?;
        let kind = CommandArchiveObjectKind::from_object(&object);
        if kind != filename_kind || self.command_archive_object_path(kind, &id) != path {
            return Err(integrity(
                COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                "directory command archive filename does not match its content identity and kind",
            ));
        }
        self.require_unique_physical_locator(&id, &path)?;
        Ok((
            id,
            PhysicalObject {
                kind: kind.physical_kind(),
                path,
            },
        ))
    }

    fn visit_physical_inventory(
        &self,
        mut visit: impl FnMut(&str, &PhysicalObject) -> DurableResult<()>,
    ) -> DurableResult<()> {
        require_current_directory_layout(&self.root)?;
        let directory = self.root.join(STATE_ROOT_FAMILY);
        require_family_directory(&directory)?;
        for entry in
            fs::read_dir(&directory).map_err(|error| substrate(INVENTORY_IO_CODE, error))?
        {
            let (id, object) = self.inspect_state_root_path(
                entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path(),
            )?;
            visit(&id, &object)?;
        }

        let directory = self.root.join(GC_RECEIPT_FAMILY);
        require_family_directory(&directory)?;
        for entry in
            fs::read_dir(&directory).map_err(|error| substrate(INVENTORY_IO_CODE, error))?
        {
            let (id, object) = self.inspect_gc_receipt_path(
                entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path(),
            )?;
            visit(&id, &object)?;
        }

        let directory = self.root.join(COMMAND_ARCHIVE_FAMILY);
        require_family_directory(&directory)?;
        for entry in
            fs::read_dir(&directory).map_err(|error| substrate(INVENTORY_IO_CODE, error))?
        {
            let (id, object) = self.inspect_command_archive_path(
                entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path(),
            )?;
            visit(&id, &object)?;
        }
        Ok(())
    }

    fn require_unique_physical_locator(&self, id: &str, current: &Path) -> DurableResult<()> {
        let paths = [
            self.state_root_object_path(id),
            self.gc_receipt_path(id),
            self.command_archive_object_path(CommandArchiveObjectKind::Segment, id),
            self.command_archive_object_path(CommandArchiveObjectKind::Entry, id),
            self.command_archive_object_path(CommandArchiveObjectKind::Batch, id),
            self.command_archive_object_path(CommandArchiveObjectKind::Node, id),
            self.command_archive_batch_index_path(id),
        ];
        for path in paths {
            if path == current {
                continue;
            }
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    return Err(integrity(
                        PHYSICAL_ALIAS_INTEGRITY_CODE,
                        format!(
                            "physical identity {id} is aliased by {} and {}",
                            current.display(),
                            path.display()
                        ),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(substrate(INVENTORY_IO_CODE, error)),
            }
        }
        Ok(())
    }

    fn locate_physical_object(&self, id: &str) -> DurableResult<Option<PhysicalObject>> {
        let mut found: Option<PhysicalObject> = None;
        let mut record = |object: PhysicalObject| -> DurableResult<()> {
            if let Some(previous) = &found {
                return Err(integrity(
                    PHYSICAL_ALIAS_INTEGRITY_CODE,
                    format!(
                        "physical identity {id} is aliased by {} and {}",
                        previous.path.display(),
                        object.path.display()
                    ),
                ));
            }
            found = Some(object);
            Ok(())
        };

        if self.read_state_root_object(id)?.is_some() {
            record(PhysicalObject {
                kind: PhysicalKind::StateRoot,
                path: self.state_root_object_path(id),
            })?;
        }
        if self.read_gc_receipt(id)?.is_some() {
            record(PhysicalObject {
                kind: PhysicalKind::GcReceipt,
                path: self.gc_receipt_path(id),
            })?;
        }
        for kind in CommandArchiveObjectKind::ALL {
            if self.read_command_archive_object(kind, id)?.is_some() {
                record(PhysicalObject {
                    kind: kind.physical_kind(),
                    path: self.command_archive_object_path(kind, id),
                })?;
            }
        }
        if self.read_command_archive_batch_index(id)?.is_some() {
            record(PhysicalObject {
                kind: PhysicalKind::ArchiveBatchIndex,
                path: self.command_archive_batch_index_path(id),
            })?;
        }
        Ok(found)
    }

    fn scan_reclamation_page(
        &self,
        retained: &BTreeSet<String>,
        required_id: Option<&str>,
    ) -> DurableResult<(BTreeSet<String>, u64)> {
        let mut page = ReclamationPage::new(required_id);
        self.visit_physical_inventory(|id, object| page.consider(id, object.kind, retained))?;
        page.finish()
    }

    fn pinned_gc_receipt(&self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        let receipt = self.verified_gc_receipt(expected)?.ok_or_else(|| {
            DurableError::Validation(
                "cold reclamation reconciliation requires a head-pinned GC receipt".to_owned(),
            )
        })?;
        if receipt.reclaimed_ids.len() > DIRECTORY_GC_PAGE_OBJECTS {
            return Err(integrity(
                GC_RECEIPT_HEAD_INTEGRITY_CODE,
                format!(
                    "directory GC receipt exceeds its {DIRECTORY_GC_PAGE_OBJECTS}-object physical page bound"
                ),
            ));
        }
        Ok(receipt)
    }

    fn verified_gc_receipt(&self, head: &StoreHead) -> DurableResult<Option<GcReceipt>> {
        let Some(receipt_id) = &head.gc_receipt else {
            return Ok(None);
        };
        let receipt = self.read_gc_receipt(receipt_id)?.ok_or_else(|| {
            integrity(
                "directory_gc_receipt_missing",
                format!("head-pinned GC receipt {receipt_id} does not exist"),
            )
        })?;
        verify_gc_receipt_for_head(&receipt, head)?;
        Ok(Some(receipt))
    }

    fn load_current_head_manifest(&self, head: &StoreHead) -> DurableResult<StateRootManifest> {
        let manifest = self
            .read_state_root_manifest(&head.state_root_manifest_id)?
            .ok_or_else(|| {
                integrity(
                    "directory_state_root_manifest_missing",
                    format!(
                        "head-pinned state-root manifest {} does not exist",
                        head.state_root_manifest_id
                    ),
                )
            })?;
        require_manifest_matches_head(&manifest, head)?;
        Ok(manifest)
    }

    fn require_current_head_manifest_snapshot(
        &self,
        head: &StoreHead,
        snapshot: &StateRootManifest,
    ) -> DurableResult<StateRootManifest> {
        let physical = self.load_current_head_manifest(head)?;
        if &physical != snapshot {
            return Err(integrity(
                "directory_state_root_manifest_snapshot_mismatch",
                "requested StateRoot manifest does not equal current physical authority",
            ));
        }
        Ok(physical)
    }

    fn with_current_head_manifest<T>(
        &mut self,
        snapshot: &StateRootManifest,
        read: impl FnOnce(&StateRootManifest, &mut DirectoryStateRootResolver<'_>) -> DurableResult<T>,
    ) -> DurableResult<T> {
        let before = self.read_head()?.ok_or_else(|| {
            DurableError::NotFound("directory Store has no current head".to_owned())
        })?;
        if before.state_root_manifest_id != snapshot.manifest_id() {
            return Err(DurableError::Conflict {
                expected: Some(snapshot.manifest_id().to_owned()),
                current: Some(before.state_root_manifest_id),
            });
        }
        let value = (|| {
            let physical = self.require_current_head_manifest_snapshot(&before, snapshot)?;
            let mut resolver = DirectoryStateRootResolver {
                store: self,
                pinned_manifest_id: physical.manifest_id().to_owned(),
            };
            read(&physical, &mut resolver)
        })();
        let after = self.read_head()?;
        if after.as_ref() != Some(&before) {
            return Err(head_conflict(Some(&before), after.as_ref()));
        }
        value
    }

    fn prepare_reclamation(
        &self,
        head: &StoreHead,
        receipt: &GcReceipt,
        retained: &BTreeSet<String>,
        retained_receipt: Option<&str>,
    ) -> DurableResult<Vec<PhysicalObject>> {
        verify_gc_receipt_for_head(receipt, head)?;
        if !receipt.reclaimed_ids.is_disjoint(retained)
            || retained_receipt.is_some_and(|id| receipt.reclaimed_ids.contains(id))
        {
            return Err(integrity(
                "directory_gc_reachable_reclamation",
                "GC receipt authorizes deletion of an object reachable from its exact head",
            ));
        }
        let mut objects = Vec::with_capacity(receipt.reclaimed_ids.len());
        for id in &receipt.reclaimed_ids {
            if let Some(object) = self.locate_physical_object(id)? {
                objects.push(object);
            }
        }
        Ok(objects)
    }

    fn sweep_reclamation(
        &self,
        receipt: &GcReceipt,
        objects: Vec<PhysicalObject>,
    ) -> DurableResult<()> {
        run_test_failure_boundary!("gc_sweep_started");
        let mut started = false;
        for object in objects {
            match fs::remove_file(&object.path) {
                Ok(()) => {
                    if !started {
                        started = true;
                        run_test_crash_boundary!("gc_deletion_started");
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(substrate(CLEANUP_IO_CODE, error)),
            }
        }
        if !receipt.reclaimed_ids.is_empty() {
            for family in DIRECTORY_FAMILIES {
                sync_directory(&self.root.join(family))?;
            }
            run_test_crash_boundary!("gc_family_synced");
            sync_directory(&self.root)?;
            run_test_crash_boundary!("gc_deletion_durable");
        }
        Ok(())
    }

    fn reconcile_previous_reclamation(
        &self,
        expected: &StoreHead,
        receipt: &GcReceipt,
        retained: &BTreeSet<String>,
    ) -> DurableResult<()> {
        let persisted = self.pinned_gc_receipt(expected)?;
        if persisted != *receipt {
            return Err(integrity(
                GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                "head-pinned GC receipt changed before advancement",
            ));
        }
        let objects =
            self.prepare_reclamation(expected, receipt, retained, expected.gc_receipt.as_deref())?;
        let claim = self.claim()?;
        let reconciliation = (|| {
            self.clean_head_staging_residue()?;
            let current = self.read_head()?;
            if current.as_ref() != Some(expected) {
                return Err(head_conflict(Some(expected), current.as_ref()));
            }
            let persisted = self.pinned_gc_receipt(expected)?;
            if persisted != *receipt {
                return Err(integrity(
                    GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                    "head-pinned GC receipt changed before advancement sweep",
                ));
            }
            self.sweep_reclamation(receipt, objects)
        })();
        finish_locked_operation(claim, reconciliation)
    }

    fn prepare_reclamation_generation(
        &self,
        expected: &StoreHead,
        previous_receipt: Option<&GcReceipt>,
        retained: &BTreeSet<String>,
    ) -> DurableResult<(GcReceipt, StoreHead, Vec<PhysicalObject>)> {
        self.clean_object_staging_residue()?;
        let current = self.read_head()?;
        if current.as_ref() != Some(expected) {
            return Err(head_conflict(Some(expected), current.as_ref()));
        }
        if let Some(previous_receipt) = previous_receipt {
            self.reconcile_previous_reclamation(expected, previous_receipt, retained)?;
        }

        let (reclaimed, remaining_objects) =
            self.scan_reclamation_page(retained, expected.gc_receipt.as_deref())?;
        let receipt = GcReceipt::new_bounded(expected, reclaimed, remaining_objects)?;
        let mut next_head = expected.clone();
        next_head.gc_sequence = receipt.gc_sequence;
        next_head
            .physical_token
            .clone_from(&receipt.result_physical_token);
        next_head.gc_receipt = Some(receipt.receipt_id.clone());
        receipt.verify_for(&next_head)?;

        self.write_gc_receipt(&receipt)?;
        sync_directory(&self.root.join(GC_RECEIPT_FAMILY))?;
        let objects =
            self.prepare_reclamation(&next_head, &receipt, retained, Some(&receipt.receipt_id))?;
        Ok((receipt, next_head, objects))
    }

    fn replay_gc_receipt(&self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        let receipt = self.pinned_gc_receipt(expected)?;
        let retained = self.stable_reachable_ids(expected, false)?;
        let object_claim = self.claim_objects()?;
        let preparation = (|| {
            let current = self.read_head()?;
            if current.as_ref() != Some(expected) {
                return Err(head_conflict(Some(expected), current.as_ref()));
            }
            let persisted = self.pinned_gc_receipt(expected)?;
            if persisted != receipt {
                return Err(integrity(
                    GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                    "head-pinned GC receipt changed during reconciliation",
                ));
            }
            let objects = self.prepare_reclamation(
                expected,
                &receipt,
                &retained,
                expected.gc_receipt.as_deref(),
            )?;
            Ok(objects)
        })();
        let objects = match preparation {
            Ok(objects) => objects,
            Err(error) => return finish_locked_operation(object_claim, Err(error)),
        };
        let claim = match self.claim() {
            Ok(claim) => claim,
            Err(error) => return finish_locked_operation(object_claim, Err(error)),
        };
        let operation = (|| {
            self.clean_head_staging_residue()?;
            let current = self.read_head()?;
            if current.as_ref() != Some(expected) {
                return Err(head_conflict(Some(expected), current.as_ref()));
            }
            let persisted = self.pinned_gc_receipt(expected)?;
            if persisted != receipt {
                return Err(integrity(
                    GC_RECEIPT_LOCATOR_INTEGRITY_CODE,
                    "head-pinned GC receipt changed before its bounded sweep",
                ));
            }
            self.sweep_reclamation(&receipt, objects)?;
            Ok(receipt)
        })();
        let replayed = finish_locked_operation(claim, operation);
        finish_locked_operation(object_claim, replayed)
    }
}

impl DurableStore for DirectoryStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        if self.writable {
            self.clean_object_staging_if_idle()?;
            let claim = self.claim()?;
            finish_locked_operation(claim, self.clean_head_staging_residue())?;
        }
        self.read_stable_head()
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<StateRootManifest>> {
        self.stable_manifest(manifest_id)
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &StateRootManifest,
        read: impl FnOnce(&mut dyn StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        current.verify()?;
        self.with_current_head_manifest(current, |_, resolver| read(resolver))
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<ApplicationJournalPrefix> {
        manifest.verify()?;
        self.with_current_head_manifest(manifest, |physical, resolver| {
            load_application_journal_prefix(physical, resolver, journal_id, count)
        })
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<JournalRecordManifest>> {
        manifest.verify()?;
        self.with_current_head_manifest(manifest, |physical, resolver| {
            load_application_journal_record_manifest(physical, resolver, journal_id, record_id)
        })
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<ApplicationJournalPrefixReplacementAuthority>> {
        manifest.verify()?;
        self.with_current_head_manifest(manifest, |physical, resolver| {
            load_application_journal_prefix_replacement_authority(
                physical,
                resolver,
                replacement_id,
            )
        })
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
        manifest.verify()?;
        self.with_current_head_manifest(manifest, |physical, resolver| {
            load_coupled_checkpoint_receipt(physical, resolver, coupling_id)
        })
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
        self.read_command_archive_segment(segment_id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<MachineCommandArchiveEntry>> {
        self.read_command_archive_entry(entry_id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<MachineCommandBatchRecord>> {
        self.read_command_archive_batch(batch_id)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<MachineCommandIndexNode>> {
        self.read_command_index_node(node_id)
    }

    fn lookup_machine_command_archive(
        &mut self,
        anchor: &cymule_core::MachineBaseAnchor,
        command_id: &str,
    ) -> DurableResult<cymule_core::MachineCommandArchiveLookup> {
        anchor.verify()?;
        let before = self.read_head()?.ok_or_else(|| DurableError::Conflict {
            expected: Some(anchor.anchor_id.clone()),
            current: None,
        })?;
        if before.machine_base_anchor.as_ref() != Some(anchor) {
            return Err(DurableError::Conflict {
                expected: Some(anchor.anchor_id.clone()),
                current: before
                    .machine_base_anchor
                    .as_ref()
                    .map(|current| current.anchor_id.clone()),
            });
        }

        let lookup = self.resolve_machine_command_archive_at_head(&before, anchor, command_id);
        let after = self.read_head()?;
        if after.as_ref() != Some(&before) {
            return Err(head_conflict(Some(&before), after.as_ref()));
        }
        lookup
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        self.compare_and_commit_with_head_sync(expected, batch, sync_directory)
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only directory stores cannot reconcile cold reclamation".to_owned(),
            ));
        }
        let expected = request.expected_head();
        expected.verify()?;
        let observed = self.read_head()?;
        if observed.as_ref() != Some(expected) {
            return Err(head_conflict(Some(expected), observed.as_ref()));
        }
        self.replay_gc_receipt(expected)
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        if !self.writable {
            return Err(DurableError::Validation(
                "read-only directory stores cannot advance cold reclamation".to_owned(),
            ));
        }
        let expected = request.expected_head();
        expected.verify()?;
        let observed = self.read_head()?;
        if observed.as_ref() != Some(expected) {
            return Err(head_conflict(Some(expected), observed.as_ref()));
        }
        let previous_receipt = expected
            .gc_receipt
            .as_ref()
            .map(|_| self.pinned_gc_receipt(expected))
            .transpose()?;
        let retained = self.stable_reachable_ids(expected, false)?;
        let object_claim = self.claim_objects()?;
        let object_operation =
            self.prepare_reclamation_generation(expected, previous_receipt.as_ref(), &retained);
        let (receipt, next_head, objects) = match object_operation {
            Ok(prepared) => prepared,
            Err(error) => return finish_locked_operation(object_claim, Err(error)),
        };
        run_test_crash_boundary!("gc_receipt_durable");

        let claim = match self.claim() {
            Ok(claim) => claim,
            Err(error) => return finish_locked_operation(object_claim, Err(error)),
        };
        let operation = (|| {
            self.clean_head_staging_residue()?;
            let current = self.read_head()?;
            if current.as_ref() != Some(expected) {
                return Err(head_conflict(Some(expected), current.as_ref()));
            }
            sync_directory(&self.root.join(GC_RECEIPT_FAMILY))?;
            write_head_atomic(
                &self.head_path(),
                &next_head,
                sync_directory,
                "gc_head_staged",
                "gc_head_renamed",
                "gc_head_durable",
            )?;
            self.sweep_reclamation(&receipt, objects).map_err(|error| {
                DurableError::CommitOutcomeUnknown {
                    message: format!(
                        "GC head and receipt are durable but reclamation requires reopen: {error}"
                    ),
                }
            })?;
            Ok(receipt)
        })();
        let published = finish_publishing_operation(claim, operation);
        finish_publishing_operation(object_claim, published)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        let before = self.read_head()?;
        let mut stats = StoreStats::default();
        let inventory = (|| {
            for entry in fs::read_dir(self.root.join(STATE_ROOT_FAMILY))
                .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
            {
                let path = entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path();
                require_regular_inventory_path(&path, STATE_ROOT_LOCATOR_INTEGRITY_CODE)?;
                require_digest_filename(
                    &path,
                    "state-root object",
                    STATE_ROOT_LOCATOR_INTEGRITY_CODE,
                )?;
                exact_count_increment(&mut stats.state_root_objects)?;
            }
            for entry in fs::read_dir(self.root.join(GC_RECEIPT_FAMILY))
                .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
            {
                let path = entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path();
                require_regular_inventory_path(&path, GC_RECEIPT_LOCATOR_INTEGRITY_CODE)?;
                require_digest_filename(&path, "GC receipt", GC_RECEIPT_LOCATOR_INTEGRITY_CODE)?;
                exact_count_increment(&mut stats.gc_receipts)?;
            }
            for entry in fs::read_dir(self.root.join(COMMAND_ARCHIVE_FAMILY))
                .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
            {
                let path = entry
                    .map_err(|error| substrate(INVENTORY_IO_CODE, error))?
                    .path();
                require_regular_inventory_path(&path, COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE)?;
                let filename = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        integrity(
                            COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                            "directory command archive filename is not Unicode",
                        )
                    })?;
                let count = match CommandArchiveObjectKind::from_filename(filename) {
                    Some(CommandArchiveObjectKind::Segment) => {
                        &mut stats.machine_command_archive_segments
                    }
                    Some(CommandArchiveObjectKind::Entry) => {
                        &mut stats.machine_command_archive_entries
                    }
                    Some(CommandArchiveObjectKind::Batch) => {
                        &mut stats.machine_command_archive_batches
                    }
                    Some(CommandArchiveObjectKind::Node) => &mut stats.machine_command_index_nodes,
                    None if filename
                        .strip_prefix("batch-index-")
                        .is_some_and(is_lower_hex_digest) =>
                    {
                        continue;
                    }
                    None => {
                        return Err(integrity(
                            COMMAND_ARCHIVE_LOCATOR_INTEGRITY_CODE,
                            format!(
                                "directory command archive filename {filename} has no closed kind"
                            ),
                        ));
                    }
                };
                exact_count_increment(count)?;
            }
            Ok(())
        })();
        let after = self.read_head()?;
        if before != after {
            return Err(head_conflict(before.as_ref(), after.as_ref()));
        }
        inventory?;
        Ok(stats)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandArchiveObjectKind {
    Segment,
    Entry,
    Batch,
    Node,
}

impl CommandArchiveObjectKind {
    const ALL: [Self; 4] = [Self::Segment, Self::Entry, Self::Batch, Self::Node];

    const fn filename_prefix(self) -> &'static str {
        match self {
            Self::Segment => "segment",
            Self::Entry => "entry",
            Self::Batch => "batch",
            Self::Node => "node",
        }
    }

    const fn physical_kind(self) -> PhysicalKind {
        match self {
            Self::Segment => PhysicalKind::ArchiveSegment,
            Self::Entry => PhysicalKind::ArchiveEntry,
            Self::Batch => PhysicalKind::ArchiveBatch,
            Self::Node => PhysicalKind::ArchiveNode,
        }
    }

    const fn from_object(value: &MachineCommandArchiveObject) -> Self {
        match value {
            MachineCommandArchiveObject::Segment(_) => Self::Segment,
            MachineCommandArchiveObject::Entry(_) => Self::Entry,
            MachineCommandArchiveObject::Batch(_) => Self::Batch,
            MachineCommandArchiveObject::CommandIndexNode(_) => Self::Node,
        }
    }

    fn from_filename(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| {
            name.strip_prefix(kind.filename_prefix())
                .and_then(|suffix| suffix.strip_prefix('-'))
                .is_some_and(is_lower_hex_digest)
        })
    }
}

fn require_family_directory(path: &Path) -> DurableResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(unsupported_generation(&format!(
            "directory Store family {} is not a no-follow directory",
            path.display()
        )));
    }
    Ok(())
}

fn require_regular_inventory_path(path: &Path, code: &str) -> DurableResult<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| substrate(INVENTORY_IO_CODE, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(integrity(
            code,
            format!(
                "directory inventory entry {} is not a no-follow regular file",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn exact_count_increment(value: &mut u64) -> DurableResult<()> {
    *value = value.checked_add(1).ok_or_else(|| {
        DurableError::Validation("directory physical object count overflowed".to_owned())
    })?;
    Ok(())
}

fn require_digest_filename(path: &Path, family: &str, code: &str) -> DurableResult<String> {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| integrity(code, format!("directory {family} filename is not Unicode")))?;
    if !is_lower_hex_digest(filename) {
        return Err(integrity(
            code,
            format!("directory {family} filename is not a canonical digest"),
        ));
    }
    Ok(filename.to_owned())
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn head_conflict(expected: Option<&StoreHead>, current: Option<&StoreHead>) -> DurableError {
    DurableError::Conflict {
        expected: expected.map(|head| head.physical_token.clone()),
        current: current.map(|head| head.physical_token.clone()),
    }
}

fn is_object_staging_conflict(error: &DurableError) -> bool {
    matches!(
        error,
        DurableError::Conflict {
            expected: Some(expected),
            current: Some(current),
        } if expected == "object_stager_available" && current == "object_stager_active"
    )
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
        return Err(integrity(
            "directory_state_root_head_mismatch",
            "directory head does not match its exact StateRoot manifest",
        ));
    }
    Ok(())
}

fn map_archive_closure_error(error: DurableError) -> DurableError {
    match error {
        DurableError::NotFound(message) => {
            integrity("directory_command_archive_closure_missing", message)
        }
        other => other,
    }
}

fn verify_gc_receipt_for_head(receipt: &GcReceipt, head: &StoreHead) -> DurableResult<()> {
    receipt.verify_for(head).map_err(|error| {
        integrity(
            GC_RECEIPT_HEAD_INTEGRITY_CODE,
            format!("directory GC receipt does not match its published head: {error}"),
        )
    })
}

fn read_optional_canonical<T>(
    path: &Path,
    max_bytes: u64,
    code: &str,
    label: &str,
) -> DurableResult<Option<T>>
where
    T: DeserializeOwned + Serialize,
{
    let Some(bytes) = read_optional_bounded_regular(path, max_bytes, code, label)? else {
        return Ok(None);
    };
    let value: T = cymule_core::decode_json(&bytes).map_err(|error| {
        integrity(
            code,
            format!("{label} is malformed canonical JSON: {error}"),
        )
    })?;
    let canonical = cymule_core::canonical_bytes(&value).map_err(|error| {
        integrity(
            code,
            format!("{label} cannot be canonically encoded: {error}"),
        )
    })?;
    if canonical != bytes {
        return Err(integrity(
            code,
            format!("{label} bytes are not the exact canonical encoding"),
        ));
    }
    Ok(Some(value))
}

fn read_optional_state_root_object(
    path: &Path,
    code: &str,
    label: &str,
) -> DurableResult<Option<StateRootObject>> {
    let Some(bytes) =
        read_optional_bounded_regular(path, state_root_object_max_bytes(), code, label)?
    else {
        return Ok(None);
    };
    decode_state_root_object(&bytes).map(Some).map_err(|error| {
        integrity(
            code,
            format!("{label} is not a valid bounded physical envelope: {error}"),
        )
    })
}

fn read_required_state_root_object(
    path: &Path,
    code: &str,
    label: &str,
) -> DurableResult<StateRootObject> {
    read_optional_state_root_object(path, code, label)?
        .ok_or_else(|| DurableError::NotFound(format!("{label} {} does not exist", path.display())))
}

fn read_required_canonical<T>(
    path: &Path,
    max_bytes: u64,
    code: &str,
    label: &str,
) -> DurableResult<T>
where
    T: DeserializeOwned + Serialize,
{
    read_optional_canonical(path, max_bytes, code, label)?
        .ok_or_else(|| DurableError::NotFound(format!("{label} {} does not exist", path.display())))
}

fn read_optional_bounded_regular(
    path: &Path,
    max_bytes: u64,
    code: &str,
    label: &str,
) -> DurableResult<Option<Vec<u8>>> {
    let read_limit = max_bytes.checked_add(1).ok_or_else(|| {
        DurableError::Validation(format!(
            "{label} byte bound cannot be probed exactly above u64::MAX"
        ))
    })?;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(integrity(
                code,
                format!("{label} {} is not a no-follow regular file", path.display()),
            ));
        }
        Ok(metadata) if metadata.len() > max_bytes => {
            return Err(integrity(
                code,
                format!("{label} exceeds its {max_bytes}-byte bound"),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(substrate(OBJECT_READ_IO_CODE, error)),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options, true);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(substrate(OBJECT_READ_IO_CODE, error)),
    };
    let metadata = file
        .metadata()
        .map_err(|error| substrate(OBJECT_READ_IO_CODE, error))?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(integrity(
            code,
            format!("{label} changed to an unsafe or over-limit file during open"),
        ));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|error| DurableError::Validation(error.to_string()))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| substrate(OBJECT_READ_IO_CODE, error))?;
    if u64::try_from(bytes.len()).map_err(|error| DurableError::Validation(error.to_string()))?
        > max_bytes
    {
        return Err(integrity(
            code,
            format!("{label} grew beyond its {max_bytes}-byte bound while reading"),
        ));
    }
    Ok(Some(bytes))
}

fn write_immutable_bytes(
    staging_directory: &Path,
    path: &Path,
    bytes: &[u8],
    max_bytes: u64,
    code: &str,
) -> DurableResult<()> {
    require_family_directory(staging_directory)?;
    if u64::try_from(bytes.len()).map_err(|error| DurableError::Validation(error.to_string()))?
        > max_bytes
    {
        return Err(DurableError::Validation(format!(
            "immutable directory object exceeds its {max_bytes}-byte bound"
        )));
    }
    if let Some(retained) =
        read_optional_bounded_regular(path, max_bytes, code, "immutable object")?
    {
        if retained != bytes {
            return Err(integrity(
                code,
                format!(
                    "immutable directory object {} has conflicting bytes",
                    path.display()
                ),
            ));
        }
        sync_regular_file(path, code)?;
        sync_directory(path.parent().expect("immutable object has a family"))?;
        return Ok(());
    }

    let staging = unique_object_staging_path(staging_directory, path)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    configure_no_follow(&mut options, false);
    let mut file = options
        .open(&staging)
        .map_err(|error| substrate(OBJECT_WRITE_IO_CODE, error))?;
    file.write_all(bytes)
        .map_err(|error| substrate(OBJECT_WRITE_IO_CODE, error))?;
    file.sync_all()
        .map_err(|error| substrate(OBJECT_WRITE_IO_CODE, error))?;
    drop(file);
    sync_directory(staging_directory)?;
    run_test_crash_boundary!("object_staged");

    match fs::hard_link(&staging, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let retained =
                read_optional_bounded_regular(path, max_bytes, code, "immutable object")?
                    .ok_or_else(|| {
                        integrity(
                            code,
                            "immutable object vanished during collision resolution",
                        )
                    })?;
            if retained != bytes {
                return Err(integrity(
                    code,
                    format!(
                        "immutable directory object {} has conflicting bytes",
                        path.display()
                    ),
                ));
            }
        }
        Err(error) => return Err(substrate(OBJECT_WRITE_IO_CODE, error)),
    }
    remove_if_present(&staging)?;
    sync_directory(staging_directory)?;
    sync_regular_file(path, code)?;
    sync_directory(path.parent().expect("immutable object has a family"))
}

fn unique_object_staging_path(staging_directory: &Path, path: &Path) -> DurableResult<PathBuf> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            DurableError::Validation("immutable object path has a non-Unicode filename".to_owned())
        })?;
    let sequence = OBJECT_STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(staging_directory.join(format!("{name}.{}.{}.next", std::process::id(), sequence)))
}

fn sync_regular_file(path: &Path, code: &str) -> DurableResult<()> {
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options, true);
    let file = options
        .open(path)
        .map_err(|error| substrate(DIRECTORY_SYNC_IO_CODE, error))?;
    if !file
        .metadata()
        .map_err(|error| substrate(DIRECTORY_SYNC_IO_CODE, error))?
        .is_file()
    {
        return Err(integrity(
            code,
            format!("{} is not a regular file", path.display()),
        ));
    }
    file.sync_all()
        .map_err(|error| substrate(DIRECTORY_SYNC_IO_CODE, error))
}

fn write_head_atomic(
    path: &Path,
    value: &impl Serialize,
    sync_head_directory: impl FnOnce(&Path) -> DurableResult<()>,
    staged_boundary: &str,
    renamed_boundary: &str,
    durable_boundary: &str,
) -> DurableResult<()> {
    let bytes = cymule_core::canonical_bytes(value)?;
    if u64::try_from(bytes.len()).map_err(|error| DurableError::Validation(error.to_string()))?
        > store_head_max_bytes()
    {
        return Err(DurableError::Validation(
            "directory Store head exceeds its fixed bound".to_owned(),
        ));
    }
    let staging = path.with_extension("next");
    remove_if_present(&staging)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    configure_no_follow(&mut options, false);
    let mut file = options
        .open(&staging)
        .map_err(|error| substrate(HEAD_PUBLISH_IO_CODE, error))?;
    file.write_all(&bytes)
        .map_err(|error| substrate(HEAD_PUBLISH_IO_CODE, error))?;
    file.sync_all()
        .map_err(|error| substrate(HEAD_PUBLISH_IO_CODE, error))?;
    drop(file);
    run_test_crash_boundary!(staged_boundary);
    fs::rename(&staging, path).map_err(|error| substrate(HEAD_PUBLISH_IO_CODE, error))?;
    run_test_crash_boundary!(renamed_boundary);
    sync_head_directory(path.parent().expect("head has parent")).map_err(|error| {
        DurableError::CommitOutcomeUnknown {
            message: error.to_string(),
        }
    })?;
    run_test_crash_boundary!(durable_boundary);
    Ok(())
}

fn configure_no_follow(options: &mut OpenOptions, nonblocking: bool) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut flags = nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC;
        if nonblocking {
            flags |= nix::libc::O_NONBLOCK;
        }
        options.custom_flags(flags);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(not(any(unix, windows)))]
    let _ = nonblocking;
}

fn acquire_writer_lock(path: &Path, initializer: bool) -> DurableResult<File> {
    acquire_named_lock(
        path,
        if initializer {
            "directory-generation-initializer-available"
        } else {
            "writer_available"
        },
        if initializer {
            "directory-generation-initializer-active"
        } else {
            "writer_active"
        },
    )
}

fn acquire_named_lock(
    path: &Path,
    available: &'static str,
    active: &'static str,
) -> DurableResult<File> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(unsupported_generation(
                "directory lock is not a no-follow regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(substrate(LOCK_IO_CODE, error)),
    }
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    configure_no_follow(&mut options, false);
    let lock = options
        .open(path)
        .map_err(|error| substrate(LOCK_IO_CODE, error))?;
    if !lock
        .metadata()
        .map_err(|error| substrate(LOCK_IO_CODE, error))?
        .is_file()
    {
        return Err(unsupported_generation(
            "directory lock changed to a non-regular file",
        ));
    }
    match FileExt::try_lock(&lock) {
        Ok(()) => Ok(lock),
        Err(TryLockError::WouldBlock) => Err(DurableError::Conflict {
            expected: Some(available.to_owned()),
            current: Some(active.to_owned()),
        }),
        Err(TryLockError::Error(error)) => Err(substrate(LOCK_IO_CODE, error)),
    }
}

fn finish_locked_operation<T>(lock: File, operation: DurableResult<T>) -> DurableResult<T> {
    let unlock = FileExt::unlock(&lock).map_err(|error| substrate(LOCK_RELEASE_IO_CODE, error));
    drop(lock);
    match operation {
        Ok(value) => unlock.map(|()| value),
        Err(error) => Err(error),
    }
}

fn finish_publishing_operation<T>(lock: File, operation: DurableResult<T>) -> DurableResult<T> {
    let unlock = FileExt::unlock(&lock).map_err(|error| substrate(LOCK_RELEASE_IO_CODE, error));
    drop(lock);
    match operation {
        Ok(value) => unlock
            .map(|()| value)
            .map_err(|error| DurableError::CommitOutcomeUnknown {
                message: format!(
                    "published directory head but explicit writer unlock failed; reopen authority: {error}"
                ),
            }),
        Err(error) => Err(error),
    }
}

fn absolute_root(root: &Path) -> DurableResult<PathBuf> {
    let root = if root.as_os_str().is_empty() {
        Path::new(".")
    } else {
        root
    };
    std::path::absolute(root).map_err(|error| substrate(LAYOUT_IO_CODE, error))
}

fn inspect_directory_generation(root: &Path) -> DurableResult<DirectoryGeneration> {
    match fs::symlink_metadata(root) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(unsupported_generation(
                "directory durable store root is not a no-follow directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DirectoryGeneration::MissingOrEmpty);
        }
        Err(error) => return Err(substrate(LAYOUT_IO_CODE, error)),
    }
    let entries = directory_root_entries(root)?;
    if entries.contains_key("state.json")
        || entries.contains_key("checkpoints")
        || entries.contains_key("segments")
    {
        return Err(unsupported_generation(
            "directory contains a pre-state-root physical authority",
        ));
    }
    if entries.contains_key(DIRECTORY_BOOTSTRAP_MARKER) {
        require_bootstrap_marker(root)?;
        if entries.contains_key(DIRECTORY_META_FILE) {
            require_current_directory_meta(root, DIRECTORY_META_FILE)?;
            return Ok(DirectoryGeneration::Current);
        }
        require_initializing_directory_layout(root, &entries)?;
        return Ok(DirectoryGeneration::InitializationInProgress);
    }
    if entries.contains_key(DIRECTORY_META_FILE) {
        require_current_directory_meta(root, DIRECTORY_META_FILE)?;
        return Err(unsupported_generation(
            "directory generation metadata has no atomic /5 bootstrap marker",
        ));
    }
    if entries.is_empty() {
        return Ok(DirectoryGeneration::MissingOrEmpty);
    }
    Err(unsupported_generation(
        "directory bytes have no cymule.directory-store/5 generation marker",
    ))
}

fn initialize_directory_layout(
    root: &Path,
    sync_parent: &mut impl FnMut(&Path) -> DurableResult<()>,
) -> DurableResult<()> {
    ensure_directory_durable(root, sync_parent)?;
    ensure_bootstrap_marker_durable(root, sync_parent)?;
    let lock = acquire_writer_lock(&root.join("head.lock"), true)?;
    let initialization = (|| {
        sync_regular_file(&root.join("head.lock"), "directory_lock_file")?;
        sync_parent(root)?;
        match inspect_directory_generation(root)? {
            DirectoryGeneration::Current => {
                require_current_directory_layout(root)?;
                ensure_lock_file_durable(root, OBJECT_LOCK_FILE, sync_parent)?;
                sync_parent(root)?;
                return require_writable_directory_layout(root);
            }
            DirectoryGeneration::MissingOrEmpty => {
                return Err(unsupported_generation(
                    "directory /5 bootstrap marker disappeared during initialization",
                ));
            }
            DirectoryGeneration::InitializationInProgress => {
                write_directory_meta_staging(root, sync_parent)?;
            }
        }
        for family in DIRECTORY_FAMILIES {
            ensure_directory_durable(&root.join(family), sync_parent)?;
        }
        ensure_directory_durable(&root.join(OBJECT_STAGING_DIRECTORY), sync_parent)?;
        ensure_lock_file_durable(root, OBJECT_LOCK_FILE, sync_parent)?;
        fs::rename(
            root.join(DIRECTORY_META_STAGING_FILE),
            root.join(DIRECTORY_META_FILE),
        )
        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
        sync_parent(root)?;
        run_test_crash_boundary!("generation_marker_published");
        require_writable_directory_layout(root)
    })();
    finish_locked_operation(lock, initialization)
}

fn ensure_bootstrap_marker_durable(
    root: &Path,
    sync_parent: &mut impl FnMut(&Path) -> DurableResult<()>,
) -> DurableResult<()> {
    let path = root.join(DIRECTORY_BOOTSTRAP_MARKER);
    let created = match fs::create_dir(&path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => return Err(substrate(LAYOUT_IO_CODE, error)),
    };
    require_bootstrap_marker(root)?;
    sync_parent(&path)?;
    sync_parent(root)?;
    if created {
        run_test_crash_boundary!("generation_bootstrap_durable");
    }
    Ok(())
}

fn write_directory_meta_staging(
    root: &Path,
    sync_parent: &mut impl FnMut(&Path) -> DurableResult<()>,
) -> DurableResult<()> {
    let path = root.join(DIRECTORY_META_STAGING_FILE);
    remove_if_present(&path)?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    configure_no_follow(&mut options, false);
    let mut file = options
        .open(&path)
        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
    file.write_all(&cymule_core::canonical_bytes(
        &DirectoryStoreMeta::current(),
    )?)
    .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
    file.sync_all()
        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
    drop(file);
    sync_parent(root)?;
    run_test_crash_boundary!("generation_marker_staged");
    Ok(())
}

fn ensure_lock_file_durable(
    root: &Path,
    name: &str,
    sync_parent: &mut impl FnMut(&Path) -> DurableResult<()>,
) -> DurableResult<()> {
    let path = root.join(name);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(unsupported_generation(&format!(
                "directory lock {name} is not a no-follow regular file"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut options = OpenOptions::new();
            options.create_new(true).read(true).write(true);
            configure_no_follow(&mut options, false);
            let file = options
                .open(&path)
                .map_err(|error| substrate(LOCK_IO_CODE, error))?;
            file.sync_all()
                .map_err(|error| substrate(LOCK_IO_CODE, error))?;
        }
        Err(error) => return Err(substrate(LOCK_IO_CODE, error)),
    }
    sync_regular_file(&path, "directory_lock_file")?;
    sync_parent(root)
}

fn require_current_directory_layout(root: &Path) -> DurableResult<()> {
    let entries = directory_root_entries(root)?;
    require_bootstrap_marker(root)?;
    if entries.get(DIRECTORY_BOOTSTRAP_MARKER) != Some(&(true, false)) {
        return Err(unsupported_generation(
            "directory generation has no exact atomic /5 bootstrap marker",
        ));
    }
    require_current_directory_meta(root, DIRECTORY_META_FILE)?;
    for family in DIRECTORY_FAMILIES {
        if entries.get(family) != Some(&(true, false)) {
            return Err(unsupported_generation(&format!(
                "directory generation {DIRECTORY_SCHEMA_VERSION} requires family {family}"
            )));
        }
    }
    if entries.get(OBJECT_STAGING_DIRECTORY) != Some(&(true, false)) {
        return Err(unsupported_generation(&format!(
            "directory generation {DIRECTORY_SCHEMA_VERSION} requires {OBJECT_STAGING_DIRECTORY}"
        )));
    }
    for (name, kind) in &entries {
        let valid = match name.as_str() {
            DIRECTORY_META_FILE | "head.json" | "head.lock" | "head.next" | OBJECT_LOCK_FILE => {
                *kind == (false, true)
            }
            DIRECTORY_BOOTSTRAP_MARKER | OBJECT_STAGING_DIRECTORY => *kind == (true, false),
            name if DIRECTORY_FAMILIES.contains(&name) => *kind == (true, false),
            _ => false,
        };
        if !valid {
            return Err(unsupported_generation(&format!(
                "directory generation {DIRECTORY_SCHEMA_VERSION} has unexpected or unsafe root entry {name}"
            )));
        }
    }
    Ok(())
}

fn require_writable_directory_layout(root: &Path) -> DurableResult<()> {
    require_current_directory_layout(root)?;
    let entries = directory_root_entries(root)?;
    for lock in ["head.lock", OBJECT_LOCK_FILE] {
        if entries.get(lock) != Some(&(false, true)) {
            return Err(unsupported_generation(&format!(
                "writable directory generation {DIRECTORY_SCHEMA_VERSION} requires regular {lock}"
            )));
        }
    }
    Ok(())
}

fn require_initializing_directory_layout(
    root: &Path,
    entries: &BTreeMap<String, (bool, bool)>,
) -> DurableResult<()> {
    require_bootstrap_marker(root)?;
    if entries.get(DIRECTORY_BOOTSTRAP_MARKER) != Some(&(true, false)) {
        return Err(unsupported_generation(
            "incomplete directory initialization has no exact /5 bootstrap marker",
        ));
    }
    for (name, kind) in entries {
        let valid = match name.as_str() {
            DIRECTORY_META_STAGING_FILE | "head.lock" | OBJECT_LOCK_FILE => *kind == (false, true),
            name if name == DIRECTORY_BOOTSTRAP_MARKER
                || name == OBJECT_STAGING_DIRECTORY
                || DIRECTORY_FAMILIES.contains(&name) =>
            {
                *kind == (true, false)
            }
            _ => false,
        };
        if !valid {
            return Err(unsupported_generation(&format!(
                "incomplete {DIRECTORY_SCHEMA_VERSION} initialization has unexpected or unsafe root entry {name}"
            )));
        }
    }
    Ok(())
}

fn require_bootstrap_marker(root: &Path) -> DurableResult<()> {
    let path = root.join(DIRECTORY_BOOTSTRAP_MARKER);
    let metadata = fs::symlink_metadata(&path).map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(unsupported_generation(
            "directory /5 bootstrap marker is not a no-follow directory",
        ));
    }
    if fs::read_dir(&path)
        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?
        .next()
        .is_some()
    {
        return Err(unsupported_generation(
            "directory /5 bootstrap marker is not empty",
        ));
    }
    Ok(())
}

fn require_current_directory_meta(root: &Path, name: &str) -> DurableResult<()> {
    let path = root.join(name);
    let bytes = read_optional_bounded_regular(
        &path,
        DIRECTORY_META_MAX_BYTES,
        "directory_generation_marker",
        "directory generation marker",
    )
    .map_err(map_generation_marker_read_error)?
    .ok_or_else(|| unsupported_generation("directory generation marker is missing"))?;
    let meta: DirectoryStoreMeta = cymule_core::decode_json(&bytes).map_err(|error| {
        unsupported_generation(&format!(
            "directory generation marker is malformed: {error}"
        ))
    })?;
    if cymule_core::canonical_bytes(&meta).map_err(|error| {
        unsupported_generation(&format!(
            "directory generation marker cannot encode: {error}"
        ))
    })? != bytes
    {
        return Err(unsupported_generation(
            "directory generation marker is not canonical",
        ));
    }
    if meta != DirectoryStoreMeta::current() {
        return Err(unsupported_generation(&format!(
            "directory generation {:?} is not {DIRECTORY_SCHEMA_VERSION}",
            meta.schema_version
        )));
    }
    Ok(())
}

fn map_generation_marker_read_error(error: DurableError) -> DurableError {
    match error {
        DurableError::Integrity { code, message } if code == "directory_generation_marker" => {
            unsupported_generation(&message)
        }
        other => other,
    }
}

fn directory_root_entries(root: &Path) -> DurableResult<BTreeMap<String, (bool, bool)>> {
    fs::read_dir(root)
        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?
        .map(|entry| {
            let entry = entry.map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
            let name = entry.file_name().into_string().map_err(|_| {
                unsupported_generation("directory root contains a non-Unicode entry name")
            })?;
            let kind = entry
                .file_type()
                .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
            Ok((name, (kind.is_dir(), kind.is_file())))
        })
        .collect()
}

fn ensure_directory_durable(
    path: &Path,
    sync_parent: &mut impl FnMut(&Path) -> DurableResult<()>,
) -> DurableResult<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let parent = if path == Path::new(".") {
        None
    } else {
        path.parent().map(|parent| {
            if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            }
        })
    };
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(DurableError::Validation(format!(
                "directory durable store path {} is not a no-follow directory",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = parent {
                ensure_directory_durable(parent, sync_parent)?;
            }
            match fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(path)
                        .map_err(|error| substrate(LAYOUT_IO_CODE, error))?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(DurableError::Validation(format!(
                            "directory durable store path {} changed to an unsafe entry",
                            path.display()
                        )));
                    }
                }
                Err(error) => return Err(substrate(LAYOUT_IO_CODE, error)),
            }
        }
        Err(error) => return Err(substrate(LAYOUT_IO_CODE, error)),
    }
    if let Some(parent) = parent {
        sync_parent(parent)?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> DurableResult<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(substrate(CLEANUP_IO_CODE, error)),
    }
}

#[cfg(test)]
fn test_crash_boundary(boundary: &str) -> DurableResult<()> {
    if std::env::var("CYMULE_DIRECTORY_TEST_BOUNDARY").as_deref() != Ok(boundary) {
        return Ok(());
    }
    let marker = std::env::var_os("CYMULE_DIRECTORY_TEST_MARKER").ok_or_else(|| {
        DurableError::Validation("directory crash boundary requires a marker".to_owned())
    })?;
    let marker = PathBuf::from(marker);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&marker)
        .map_err(|error| substrate(TEST_BOUNDARY_IO_CODE, error))?;
    file.write_all(boundary.as_bytes())
        .map_err(|error| substrate(TEST_BOUNDARY_IO_CODE, error))?;
    file.sync_all()
        .map_err(|error| substrate(TEST_BOUNDARY_IO_CODE, error))?;
    sync_directory(marker.parent().expect("test marker has a parent"))?;
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
fn test_failure_boundary(boundary: &str) -> DurableResult<()> {
    if std::env::var("CYMULE_DIRECTORY_TEST_FAILURE").as_deref() == Ok(boundary) {
        return Err(substrate(
            INJECTED_FAILURE_CODE,
            format!("injected directory failure at {boundary}"),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> DurableResult<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| substrate(DIRECTORY_SYNC_IO_CODE, error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(substrate(
            DIRECTORY_SYNC_IO_CODE,
            format!(
                "directory sync target {} is not a no-follow directory",
                path.display()
            ),
        ));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| substrate(DIRECTORY_SYNC_IO_CODE, error))
}

#[cfg(not(unix))]
fn sync_directory(path: &Path) -> DurableResult<()> {
    Err(substrate(
        DIRECTORY_SYNC_IO_CODE,
        format!("directory durability is unsupported on {}", path.display()),
    ))
}

fn integrity(code: &str, message: impl Into<String>) -> DurableError {
    DurableError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn substrate(code: &'static str, error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate {
        code: code.to_owned(),
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
#[cfg(unix)]
mod tests {
    use super::*;
    use cymule_core::{
        COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine, PlanCandidate,
        Region, seal_plan,
    };
    use cymule_durable::{DurableStoreControl, StoredState};
    use cymule_runtime::{
        EXECUTION_BINDING_VERSION, ExecutionBinding, PLUGIN_VERSION, PluginManifest,
    };
    use cymule_test_world::ManagedChild;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};
    use std::process::Command as ProcessCommand;
    use std::time::Duration;

    fn initialize(root: &Path) -> StoredState {
        let store = DirectoryStore::open(root).expect("worker Store opens");
        let control =
            DurableStoreControl::initialize(store).expect("zero-Run authority initializes");
        let mut store = control.into_store();
        store
            .load_full_audit()
            .expect("initial state loads")
            .expect("initial state exists")
    }

    fn advance_current(store: &mut impl DurableStore) -> DurableResult<GcReceipt> {
        DurableStoreControl::open(store)?.advance_cold_reclamation()
    }

    fn reconcile_current(store: &mut impl DurableStore) -> DurableResult<GcReceipt> {
        DurableStoreControl::open(store)?.reconcile_cold_reclamation()
    }

    #[test]
    fn generation_marker_mapping_preserves_non_shape_error_categories() {
        for error in [
            DurableError::Substrate {
                code: OBJECT_READ_IO_CODE.to_owned(),
                message: "permission denied".to_owned(),
            },
            DurableError::Persistence {
                code: "retained_marker_unavailable".to_owned(),
                message: "persistence unavailable".to_owned(),
            },
            DurableError::CommitOutcomeUnknown {
                message: "marker read outcome unknown".to_owned(),
            },
        ] {
            assert_eq!(map_generation_marker_read_error(error.clone()), error);
        }

        assert_eq!(
            map_generation_marker_read_error(DurableError::Integrity {
                code: "directory_generation_marker".to_owned(),
                message: "marker is not a regular file".to_owned(),
            }),
            DurableError::Substrate {
                code: UNSUPPORTED_STORE_GENERATION_CODE.to_owned(),
                message: "marker is not a regular file".to_owned(),
            }
        );
        let unrelated = DurableError::Integrity {
            code: "directory_unrelated_integrity".to_owned(),
            message: "unrelated corruption".to_owned(),
        };
        assert_eq!(
            map_generation_marker_read_error(unrelated.clone()),
            unrelated
        );
    }

    enum InitialCommitInterception {
        HeadSyncFailure,
        CrossFamilyAlias,
        MissingResponse,
    }

    struct InterceptingDirectoryStore {
        inner: DirectoryStore,
        interception: InitialCommitInterception,
    }

    impl DurableStore for InterceptingDirectoryStore {
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
            match self.interception {
                InitialCommitInterception::MissingResponse => {
                    self.inner.compare_and_commit(expected, batch)?;
                    Err(DurableError::CommitOutcomeUnknown {
                        message: "injected post-commit response loss".to_owned(),
                    })
                }
                InitialCommitInterception::HeadSyncFailure => self
                    .inner
                    .compare_and_commit_with_head_sync(expected, batch, |_| {
                        Err(DurableError::Substrate {
                            code: INJECTED_HEAD_SYNC_FAILURE_CODE.to_owned(),
                            message: "injected head directory sync failure".to_owned(),
                        })
                    }),
                InitialCommitInterception::CrossFamilyAlias => {
                    let object_id = batch
                        .state_root_transition()
                        .objects()
                        .first()
                        .expect("initial transition contains objects")
                        .object_id();
                    let alias = self.inner.root.join(COMMAND_ARCHIVE_FAMILY).join(format!(
                        "segment-{}",
                        cymule_core::sha256_bytes(object_id.as_bytes())
                    ));
                    fs::write(alias, b"{}").expect("cross-family alias writes");
                    self.inner.compare_and_commit(expected, batch)
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

    fn reset_gc_receipt_read_count() {
        GC_RECEIPT_READ_COUNT.with(|count| count.set(0));
    }

    fn gc_receipt_read_count() -> u64 {
        GC_RECEIPT_READ_COUNT.with(std::cell::Cell::get)
    }

    #[test]
    fn ordinary_semantic_reads_never_follow_the_pinned_gc_receipt() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        let receipt = advance_current(&mut store).expect("receipt head publishes");
        let head = store
            .read_head()
            .expect("receipt head reads")
            .expect("receipt head exists");
        let manifest = store
            .read_state_root_manifest(&head.state_root_manifest_id)
            .expect("current manifest reads")
            .expect("current manifest exists");
        let receipt_path = store.gc_receipt_path(&receipt.receipt_id);
        fs::remove_file(&receipt_path).expect("current receipt is removed");
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
                    "journal:missing-receipt",
                    "record:missing-receipt",
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
        assert_eq!(
            gc_receipt_read_count(),
            0,
            "ordinary semantic paths must read zero receipt bytes"
        );

        reset_gc_receipt_read_count();
        assert!(matches!(
            reconcile_current(&mut store),
            Err(DurableError::Integrity { code, .. }) if code == "directory_gc_receipt_missing"
        ));
        assert!(gc_receipt_read_count() > 0);

        assert_oversized_receipt_is_not_semantic_authority(
            &mut store,
            &head,
            &manifest,
            &receipt_path,
            &replacement_id,
            &coupling_id,
        );
    }

    fn assert_oversized_receipt_is_not_semantic_authority(
        store: &mut DirectoryStore,
        head: &StoreHead,
        manifest: &StateRootManifest,
        receipt_path: &Path,
        replacement_id: &str,
        coupling_id: &str,
    ) {
        fs::write(receipt_path, vec![b'x'; MAX_GC_RECEIPT_BYTES + 1])
            .expect("oversized current receipt writes");
        reset_gc_receipt_read_count();
        assert_eq!(
            store.load_head().expect("head ignores receipt bytes"),
            Some(head.clone())
        );
        assert_eq!(
            store
                .load_state_root_manifest(manifest.manifest_id())
                .expect("manifest ignores receipt bytes"),
            Some(manifest.clone())
        );
        assert_eq!(
            store
                .load_full_audit()
                .expect("semantic audit ignores oversized receipt bytes")
                .expect("projection exists")
                .head,
            *head
        );
        store
            .with_state_root_resolver(manifest, |_| Ok(()))
            .expect("resolver ignores receipt bytes");
        assert_eq!(
            store
                .application_journal_record_manifest(
                    manifest,
                    "journal:oversized-receipt",
                    "record:oversized-receipt",
                )
                .expect("exact record lookup ignores oversized receipt bytes"),
            None
        );
        assert_eq!(
            store
                .application_journal_prefix_replacement_authority(manifest, replacement_id)
                .expect("exact replacement lookup ignores oversized receipt bytes"),
            None
        );
        assert_eq!(
            store
                .coupled_checkpoint_receipt(manifest, coupling_id)
                .expect("exact coupling lookup ignores oversized receipt bytes"),
            None
        );
        store
            .stats()
            .expect("physical counts do not decode receipt payloads");
        assert_eq!(gc_receipt_read_count(), 0);
        assert!(matches!(
            reconcile_current(store),
            Err(DurableError::Integrity { code, .. }) if code == GC_RECEIPT_LOCATOR_INTEGRITY_CODE
        ));
        assert!(gc_receipt_read_count() > 0);
        assert_eq!(store.read_head().expect("head rereads"), Some(head.clone()));
    }

    #[test]
    fn current_head_state_root_reads_require_the_exact_physical_manifest_snapshot() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let current = initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        let mut substituted = serde_json::to_value(&current.state_root_manifest)
            .expect("StateRoot manifest serializes");
        substituted["revision"] = json!(format!("sha256:{}", "f".repeat(64)));
        let substituted: StateRootManifest =
            serde_json::from_value(substituted).expect("substituted manifest decodes");
        assert!(matches!(
            store.with_state_root_resolver(&substituted, |_| Ok(())),
            Err(DurableError::Integrity { .. })
        ));

        fs::remove_file(store.state_root_object_path(&current.head.state_root_manifest_id))
            .expect("head-pinned physical manifest is removed");
        assert_eq!(
            store.load_head().expect("bounded head load succeeds"),
            Some(current.head.clone()),
            "ordinary head load must not touch the StateRoot family"
        );
        assert!(matches!(
            store.load_full_audit(),
            Err(DurableError::Integrity { code, .. })
                if code == "directory_state_root_manifest_missing"
        ));
        assert!(matches!(
            store.load_state_root_manifest(&current.head.state_root_manifest_id),
            Err(DurableError::Integrity { code, .. })
                if code == "directory_state_root_manifest_missing"
        ));
        assert_eq!(store.read_head().expect("head rereads"), Some(current.head));
    }

    #[test]
    fn reclamation_page_is_deterministic_and_bounded_for_any_inventory_order() {
        let identities = (0_u64..2_048)
            .map(|index| format!("sha256:{index:064x}"))
            .collect::<Vec<_>>();
        let receipt_indexes = [1_800_usize, 1_900, 2_047];
        let required = identities[2_047].clone();
        let retained = BTreeSet::new();
        let select = |order: Vec<usize>| {
            let mut page = ReclamationPage::new(Some(&required));
            for index in order {
                let kind = if receipt_indexes.contains(&index) {
                    PhysicalKind::GcReceipt
                } else {
                    PhysicalKind::StateRoot
                };
                page.consider(&identities[index], kind, &retained)
                    .expect("candidate is admitted");
            }
            page.finish().expect("page finishes")
        };

        let ascending = (0..identities.len()).collect::<Vec<_>>();
        let descending = (0..identities.len()).rev().collect::<Vec<_>>();
        let scrambled = (0..identities.len())
            .map(|index| (index * 997) % identities.len())
            .collect::<Vec<_>>();
        let expected = select(ascending);
        assert_eq!(select(descending), expected);
        assert_eq!(select(scrambled), expected);
        assert_eq!(
            expected.0.len(),
            DIRECTORY_GC_PAGE_OBJECTS,
            "the in-memory selection never grows with the cold inventory"
        );
        assert_eq!(expected.1, 1_024);
        for index in receipt_indexes {
            assert!(expected.0.contains(&identities[index]));
        }
        let expected_other = identities
            .iter()
            .enumerate()
            .filter(|(index, _)| !receipt_indexes.contains(index))
            .map(|(_, id)| id)
            .take(DIRECTORY_GC_PAGE_OBJECTS - receipt_indexes.len())
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(expected_other.is_subset(&expected.0));

        let mut missing = ReclamationPage::new(Some(
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ));
        missing
            .consider(&identities[0], PhysicalKind::StateRoot, &retained)
            .expect("ordinary candidate is admitted");
        assert!(matches!(
            missing.finish(),
            Err(DurableError::Integrity { code, .. }) if code == "directory_gc_receipt_missing"
        ));

        let mut receipts = ReclamationPage::new(None);
        for identity in identities.iter().take(DIRECTORY_GC_PAGE_OBJECTS + 1) {
            receipts
                .consider(identity, PhysicalKind::GcReceipt, &retained)
                .expect("receipt candidates remain bounded instead of bricking GC");
        }
        let (receipt_page, receipt_remaining) = receipts.finish().expect("receipt page finishes");
        assert_eq!(receipt_page.len(), DIRECTORY_GC_PAGE_OBJECTS);
        assert_eq!(receipt_remaining, 1);
        assert_eq!(
            receipt_page.last(),
            identities.get(DIRECTORY_GC_PAGE_OBJECTS - 1)
        );

        let pinned = &identities[DIRECTORY_GC_PAGE_OBJECTS];
        let mut pinned_receipts = ReclamationPage::new(Some(pinned));
        for identity in identities.iter().take(DIRECTORY_GC_PAGE_OBJECTS + 1) {
            pinned_receipts
                .consider(identity, PhysicalKind::GcReceipt, &retained)
                .expect("pinned receipt inventory remains bounded");
        }
        let (pinned_page, pinned_remaining) = pinned_receipts
            .finish()
            .expect("pinned receipt page finishes");
        assert_eq!(pinned_page.len(), DIRECTORY_GC_PAGE_OBJECTS);
        assert_eq!(pinned_remaining, 1);
        assert!(pinned_page.contains(pinned));
        assert!(!pinned_page.contains(&identities[DIRECTORY_GC_PAGE_OBJECTS - 1]));
    }

    #[test]
    fn reclamation_page_rejects_reservations_beyond_its_exact_bound() {
        let mut page = ReclamationPage::new(None);
        page.receipt_ids = (0..=DIRECTORY_GC_PAGE_OBJECTS)
            .map(|index| format!("sha256:{index:064x}"))
            .collect();
        assert!(matches!(
            page.ordinary_limit(),
            Err(DurableError::Validation(message))
                if message == "directory GC receipt-page reservations exceed the exact page bound"
        ));
    }

    #[test]
    fn bounded_file_read_rejects_an_unprobeable_maximum() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let path = temporary.path().join("bounded-object");
        fs::write(&path, b"x").expect("bounded object writes");
        assert!(matches!(
            read_optional_bounded_regular(
                &path,
                u64::MAX,
                "directory_test_bound",
                "directory test object",
            ),
            Err(DurableError::Validation(message))
                if message
                    == "directory test object byte bound cannot be probed exactly above u64::MAX"
        ));
    }

    #[test]
    fn receipt_crash_residue_converges_through_explicit_bounded_generations() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let current = initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        for index in 0..=DIRECTORY_GC_PAGE_OBJECTS {
            let reclaimed = BTreeSet::from([format!("sha256:{index:064x}")]);
            let orphan = GcReceipt::new_bounded(&current.head, reclaimed, 0)
                .expect("crash-residue receipt builds");
            fs::write(
                store.gc_receipt_path(&orphan.receipt_id),
                cymule_core::canonical_bytes(&orphan).expect("orphan receipt encodes"),
            )
            .expect("crash-residue receipt writes");
        }
        sync_directory(&root.join(GC_RECEIPT_FAMILY)).expect("receipt residue synchronizes");

        let first = advance_current(&mut store)
            .expect("first bounded receipt-priority generation publishes");
        assert_eq!(first.reclaimed_ids.len(), DIRECTORY_GC_PAGE_OBJECTS);
        assert_eq!(first.remaining_objects, 1);
        let first_head = store
            .read_head()
            .expect("first receipt head reads")
            .expect("first receipt head exists");
        assert_eq!(store.stats().expect("non-final stats read").gc_receipts, 2);
        assert_eq!(
            reconcile_current(&mut store).expect("non-final receipt reconciles"),
            first
        );
        assert_eq!(
            store.read_head().expect("head rereads"),
            Some(first_head.clone())
        );

        let final_receipt =
            advance_current(&mut store).expect("explicit remainder generation publishes");
        assert_eq!(final_receipt.remaining_objects, 0);
        assert!(final_receipt.reclaimed_ids.contains(&first.receipt_id));
        assert_eq!(store.stats().expect("final stats read").gc_receipts, 1);
    }

    fn populate_gc_orphan(root: &Path) -> StoredState {
        let expected = initialize(root);
        let store = DirectoryStore::open(root).expect("GC Store opens");
        let claim = store.claim_objects().expect("GC orphan staging locks");
        let (archive, _) = standalone_archive("gc-process-kill");
        let staging = (|| {
            for object in archive.persistence_objects()? {
                store.write_command_archive_object(&object)?;
            }
            sync_directory(&store.root.join(COMMAND_ARCHIVE_FAMILY))
        })();
        finish_locked_operation(claim, staging).expect("GC archive orphan persists");
        expected
    }

    fn standalone_archive(
        suffix: &str,
    ) -> (MachineCommandArchiveSegment, cymule_core::MachineBaseAnchor) {
        let mut machine = Machine::new();
        let plan = seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: format!("directory_archive_{suffix}"),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal {
                        value: json!("archive"),
                    },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("archive Plan seals");
        machine
            .insert_plan(plan.clone())
            .expect("archive Plan inserts");
        let manifest = PluginManifest {
            plugin_version: PLUGIN_VERSION.to_owned(),
            implementation_id: format!("directory-archive-plugin-{suffix}/1"),
            components: BTreeMap::new(),
            effects: BTreeMap::new(),
        };
        let binding = ExecutionBinding::for_local_process(
            &manifest,
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        )
        .expect("archive binding seals");
        binding
            .admit_plan(&plan)
            .expect("archive binding admits Plan");
        let binding = machine
            .put_artifact(
                EXECUTION_BINDING_VERSION,
                binding.canonical_bytes().expect("archive binding encodes"),
            )
            .expect("archive binding Artifact stores");
        let input = machine
            .put_artifact(cymule_core::RUN_INPUT_ARTIFACT_KIND, b"{}".to_vec())
            .expect("archive input Artifact stores");
        let command_id = format!("command:directory-archive:{suffix}");
        let material = cymule_core::durable_internal::MachineMaterialAdmission::new(
            command_id.clone(),
            vec![plan.clone()],
            vec![
                machine.artifact(&binding).expect("binding reads").clone(),
                machine.artifact(&input).expect("input reads").clone(),
            ],
        )
        .expect("archive material admits");
        let initial_attempt = cymule_core::InitialAttemptSpec {
            attempt_id: cymule_core::content_id("cymule.test.initial-attempt/1", &command_id)
                .expect("archive Attempt derives"),
            continuation_id: cymule_core::content_id(
                "cymule.test.initial-continuation/1",
                &command_id,
            )
            .expect("archive Continuation derives"),
            occurrence_binding: binding.artifact_id.clone(),
            continuation_epoch: 0,
            execution_fence: 1,
        };
        machine
            .submit(CommandEnvelope {
                command_version: COMMAND_VERSION.to_owned(),
                command_id,
                actor: "test".to_owned(),
                run_id: format!("run:directory-archive:{suffix}"),
                expected_precondition: None,
                command: Command::StartRun {
                    plan_id: plan.plan_id,
                    binding_context: binding.artifact_id,
                    input,
                    material_digest: material.material_digest().to_owned(),
                    initial_attempt,
                },
            })
            .expect("archive Run starts");
        let archive = machine
            .compact_event_history(0)
            .expect("archive history compacts")
            .archive_segment;
        let anchor = machine
            .base_anchor()
            .expect("archive anchor derives")
            .expect("compaction publishes an anchor");
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
            &json!({
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
        store: &mut DirectoryStore,
        receipt: &cymule_durable::HistoryCompactionReceipt,
    ) {
        for object in history_missing_objects(store, receipt) {
            let path = store.command_archive_object_path(
                CommandArchiveObjectKind::from_object(&object),
                &object.identity().unwrap(),
            );
            let bytes = fs::read(&path).unwrap();
            fs::remove_file(&path).expect("remove one exact independent archive object");
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
            fs::write(path, bytes).expect("restore the exact original fault-fixture bytes");
        }
    }

    #[test]
    fn public_history_compaction_reopens_replays_and_recovers_lost_ack() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("store");
        let clock = temporary.path().join("clock.sqlite");
        let (store, evidence) = history_cancelled(DirectoryStore::open(&root).unwrap(), &clock);
        let faulted = InterceptingDirectoryStore {
            inner: store,
            interception: InitialCommitInterception::MissingResponse,
        };
        let mut control = DurableStoreControl::open(faulted).unwrap();
        assert!(matches!(
            control.compact_machine_history(&evidence.request),
            Err(DurableError::CommitOutcomeUnknown { .. })
        ));
        drop(control);
        let store =
            DirectoryStore::open(&root).expect("official Directory Store reopens after loss");
        finish_history_compaction_chain(store, &clock, &evidence, reject_missing_history_objects);
    }

    #[test]
    fn gc_retains_material_only_batch_without_command_entries() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let clock = temporary.path().join("clock.sqlite");
        let fixture = material_only_archive(DirectoryStore::open(&root).unwrap(), &clock);
        let HistoryFixture {
            store,
            first_head,
            first_request,
            second_request,
            first,
            second,
        } = fixture;
        drop(store);
        let mut store = DirectoryStore::open(&root).expect("physical Store reopens");
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
        let mut expected = store
            .retained_machine_command_objects(&fixture.first_head)
            .unwrap();
        expected.extend([
            archive.header.segment_id.clone(),
            batch.batch_id.clone(),
            batch.batch_receipt_id.clone(),
        ]);
        assert_eq!(
            store.retained_machine_command_objects(&head).unwrap(),
            expected
        );
        let index_path = store.command_archive_batch_index_path(&batch.batch_id);
        let index = fs::read(&index_path).unwrap();
        fs::remove_file(&index_path).expect("remove exact independent batch index");
        assert!(matches!(
            advance_current(&mut store),
            Err(DurableError::Integrity { .. })
        ));
        assert_eq!(store.load_head().unwrap().unwrap(), head);
        fs::write(index_path, index).expect("restore exact fault-fixture bytes");
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

    fn gc_physical_inventory(store: &DirectoryStore) -> BTreeMap<String, Vec<u8>> {
        let mut bytes = BTreeMap::new();
        store
            .visit_physical_inventory(|id, object| {
                bytes.insert(
                    id.to_owned(),
                    fs::read(&object.path).expect("physical object reads"),
                );
                Ok(())
            })
            .expect("self-valid physical inventory reads");
        bytes
    }

    #[test]
    fn gc_archive_reachability_retains_stable_batch_index_and_receipt() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let mut head = initialize(&root).head;
        let store = DirectoryStore::open(&root).expect("archive Store opens");
        let (archive, anchor) = standalone_archive("gc-batch-authority");
        head.machine_base_anchor = Some(anchor);
        let objects = archive
            .persistence_objects()
            .expect("archive objects derive");
        for object in &objects {
            store
                .write_command_archive_object(object)
                .expect("archive object persists");
        }
        let mut expected = objects
            .iter()
            .map(|object| object.identity().expect("object verifies"))
            .collect::<BTreeSet<_>>();
        for batch in &archive.batches {
            expected.insert(batch.batch_id.clone());
            assert_eq!(
                store
                    .read_command_archive_batch(&batch.batch_id)
                    .expect("stable index resolves"),
                Some(batch.clone())
            );
        }
        assert_eq!(
            store
                .retained_machine_command_objects(&head)
                .expect("GC archive closure verifies"),
            expected
        );
        for batch in &archive.batches {
            assert!(
                store
                    .command_archive_batch_index_path(&batch.batch_id)
                    .is_file()
            );
            assert!(
                store
                    .command_archive_object_path(
                        CommandArchiveObjectKind::Batch,
                        &batch.batch_receipt_id
                    )
                    .is_file()
            );
        }
    }

    #[test]
    fn gc_rejects_self_valid_indexed_batches_that_disagree_with_reachable_entries() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let mut head = initialize(&root).head;
        let store = DirectoryStore::open(&root).expect("archive Store opens");
        let (archive, anchor) = standalone_archive("gc-batch-mismatch");
        head.machine_base_anchor = Some(anchor);
        for object in archive
            .persistence_objects()
            .expect("archive objects derive")
        {
            store
                .write_command_archive_object(&object)
                .expect("archive object persists");
        }
        let original = archive.batches.first().expect("batch exists");
        let entry = archive.entries.first().expect("entry exists");
        for mismatch in ["member", "receipt"] {
            let forged = mismatched_gc_batch(original, mismatch);
            assert!(forged.verify_entry(entry).is_err());
            let forged_object = MachineCommandArchiveObject::Batch(Box::new(forged.clone()));
            let forged_path = store.command_archive_object_path(
                CommandArchiveObjectKind::Batch,
                &forged.batch_receipt_id,
            );
            fs::write(
                &forged_path,
                cymule_core::canonical_bytes(&forged_object).expect("batch encodes"),
            )
            .expect("self-valid corrupt receipt fixture writes");
            let index = DirectoryCommandBatchIndex::new(&forged).expect("stable index verifies");
            fs::write(
                store.command_archive_batch_index_path(&forged.batch_id),
                cymule_core::canonical_bytes(&index).expect("index encodes"),
            )
            .expect("corrupt stable-index fixture writes");
            assert_eq!(
                store
                    .read_command_archive_batch(&forged.batch_id)
                    .expect("stable index and object hashes verify"),
                Some(forged)
            );
            let before = gc_physical_inventory(&store);
            let head_before = fs::read(store.root.join("head.json")).expect("physical head reads");
            assert!(
                matches!(
                    store.retained_machine_command_objects(&head),
                    Err(DurableError::Integrity { code, .. }) if code == "directory_command_archive_batch_mismatch"
                ),
                "GC must reject the {mismatch} mismatch before returning a reachable set"
            );
            assert_eq!(
                gc_physical_inventory(&store),
                before,
                "failed GC reachability must not delete or rewrite any physical object"
            );
            assert_eq!(
                fs::read(store.root.join("head.json")).expect("physical head rereads"),
                head_before
            );
        }
    }

    #[test]
    fn typed_command_archive_orphans_are_validated_and_reclaimed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("archive Store opens");
        let first_receipt = advance_current(&mut store).expect("first GC generation publishes");
        let mut first_anchor = None;
        let mut first_batch = None;
        for index in 0..5 {
            let suffix = index.to_string();
            let (archive, anchor) = standalone_archive(&suffix);
            first_anchor.get_or_insert(anchor);
            if first_batch.is_none() {
                first_batch = archive.batches.first().cloned();
            }
            for object in archive
                .persistence_objects()
                .expect("archive persistence objects derive")
            {
                store
                    .write_command_archive_object(&object)
                    .expect("typed archive object persists");
            }
        }
        let stats = store.stats().expect("archive stats read");
        assert_eq!(stats.machine_command_archive_segments, 5);
        assert!(stats.machine_command_archive_entries > 0);
        assert_eq!(stats.machine_command_archive_batches, 5);
        assert!(stats.machine_command_index_nodes > 0);
        let expected_batch = first_batch.expect("first archive batch exists");
        assert_eq!(
            store
                .load_machine_command_archive_batch(&expected_batch.batch_id)
                .expect("batch index reads"),
            Some(expected_batch),
            "stable batch lookup must resolve the exact receipt object without a segment scan"
        );
        let mut lookup_store = store.clone();
        assert!(matches!(
            lookup_store.lookup_machine_command_archive(
                first_anchor.as_ref().expect("first archive anchor exists"),
                "command:directory-archive:0"
            ),
            Err(DurableError::Conflict { .. })
        ));

        let receipt =
            advance_current(&mut store).expect("a later GC reclaims post-receipt archive orphans");
        assert_ne!(receipt.receipt_id, first_receipt.receipt_id);
        assert_eq!(receipt.gc_sequence, first_receipt.gc_sequence + 1);
        assert_eq!(
            receipt.reclaimed_objects,
            u64::try_from(DIRECTORY_GC_PAGE_OBJECTS).expect("page size fits")
        );
        assert!(receipt.remaining_objects > 0);
        assert!(
            receipt.reclaimed_ids.contains(&first_receipt.receipt_id),
            "every successor page must reclaim its predecessor receipt"
        );
        let second_gc = store
            .load_full_audit()
            .expect("second GC state loads")
            .expect("second GC state exists");
        assert_eq!(store.stats().expect("receipt count reads").gc_receipts, 1);
        for _ in 0..2 {
            assert_eq!(
                reconcile_current(&mut store)
                    .expect("lost acknowledgement reconciles the exact non-final page"),
                receipt
            );
            assert_eq!(
                store
                    .load_full_audit()
                    .expect("reconciled state loads")
                    .expect("reconciled state exists")
                    .head,
                second_gc.head,
                "reconciliation must never advance a non-final page"
            );
        }
        let final_receipt = advance_current(&mut store).expect("final bounded GC page publishes");
        assert_eq!(final_receipt.remaining_objects, 0);
        assert!(final_receipt.reclaimed_ids.contains(&receipt.receipt_id));
        let stats = store.stats().expect("post-GC archive stats read");
        assert_eq!(stats.machine_command_archive_segments, 0);
        assert_eq!(stats.machine_command_archive_entries, 0);
        assert_eq!(stats.machine_command_archive_batches, 0);
        assert_eq!(stats.machine_command_index_nodes, 0);
        assert_eq!(stats.gc_receipts, 1);
        store
            .load_full_audit()
            .expect("post-archive-GC state loads")
            .expect("post-archive-GC state exists")
            .verify()
            .expect("post-archive-GC StateRoot closure verifies");
    }

    #[test]
    fn head_directory_sync_failure_is_an_unknown_commit_outcome() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let store = InterceptingDirectoryStore {
            inner: DirectoryStore::open(temporary.path()).expect("Store opens"),
            interception: InitialCommitInterception::HeadSyncFailure,
        };
        let Err(error) = DurableStoreControl::initialize(store) else {
            panic!("published head cannot return a success receipt");
        };
        assert!(matches!(
            error,
            DurableError::CommitOutcomeUnknown { message }
                if message.contains("injected head directory sync failure")
        ));
        let mut reopened =
            DirectoryStore::open_read_only(temporary.path()).expect("published head reopens");
        let retained = reopened
            .load_full_audit()
            .expect("published authority loads")
            .expect("state was renamed before sync failure");
        retained.verify().expect("retained StateRoot verifies");
    }

    #[test]
    fn commit_rejects_a_cross_family_identity_alias_before_publishing_head() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let store = InterceptingDirectoryStore {
            inner: DirectoryStore::open(temporary.path()).expect("Store opens"),
            interception: InitialCommitInterception::CrossFamilyAlias,
        };
        let Err(error) = DurableStoreControl::initialize(store) else {
            panic!("cross-family alias cannot publish a semantic head");
        };
        assert!(matches!(
            error,
            DurableError::Integrity { code, .. }
                if code == PHYSICAL_ALIAS_INTEGRITY_CODE
        ));
        assert!(
            !temporary.path().join("head.json").exists(),
            "cross-family alias admission cannot publish a semantic head"
        );
    }

    #[test]
    #[ignore = "child worker invoked by internal_sigkill_boundaries_reopen"]
    fn crash_worker() {
        let Some(root) = std::env::var_os("CYMULE_DIRECTORY_TEST_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        match std::env::var("CYMULE_DIRECTORY_TEST_OPERATION").as_deref() {
            Ok("initialize-layout") => {
                DirectoryStore::open(&root).expect("layout worker reaches boundary");
            }
            Ok("commit") => {
                initialize(&root);
            }
            Ok("stage-orphan") => {
                let store = DirectoryStore::open(&root).expect("append worker opens");
                let claim = store.claim_objects().expect("append stager locks");
                let (archive, _) = standalone_archive("object-staging");
                let object = archive
                    .persistence_objects()
                    .expect("archive objects derive")
                    .into_iter()
                    .next()
                    .expect("archive has one object");
                let result = store.write_command_archive_object(&object);
                finish_locked_operation(claim, result).expect("append worker reaches boundary");
            }
            Ok("gc") => {
                let mut store = DirectoryStore::open(&root).expect("GC worker opens");
                advance_current(&mut store).expect("GC worker reaches boundary");
            }
            Ok("reconcile") => {
                let mut store = DirectoryStore::open(&root).expect("reconcile worker opens");
                reconcile_current(&mut store).expect("reconcile worker reaches boundary");
            }
            Ok("gc-failure") => run_post_head_gc_failure_worker(&root),
            Ok("reconcile-failure") => run_prior_gc_failure_worker(&root, false),
            Ok("advance-prior-failure") => run_prior_gc_failure_worker(&root, true),
            operation => panic!("unknown crash worker operation {operation:?}"),
        }
        panic!("worker did not stop at its requested boundary");
    }

    #[test]
    #[ignore = "child worker invoked by relative_root_handle_is_cwd_independent"]
    fn relative_root_worker() {
        let Some(sandbox) = std::env::var_os("CYMULE_DIRECTORY_RELATIVE_ROOT_SANDBOX") else {
            return;
        };
        let sandbox = PathBuf::from(sandbox);
        let dot_root = sandbox.join("dot-store");
        let alternate = sandbox.join("alternate-cwd");
        fs::create_dir(&dot_root).expect("dot Store root creates");
        fs::create_dir(&alternate).expect("alternate cwd creates");
        std::env::set_current_dir(&dot_root).expect("worker enters dot Store root");
        let opened_root = std::env::current_dir().expect("worker resolves its opened cwd");
        let opened_parent = opened_root
            .parent()
            .expect("opened Store root has a parent")
            .to_path_buf();

        let mut synchronized = Vec::new();
        let store = DirectoryStore::open_with_directory_sync(".", |path| {
            synchronized.push(path.to_path_buf());
            sync_directory(path)
        })
        .expect("relative Store opens");
        assert!(store.root().is_absolute());
        assert_eq!(store.root(), opened_root);
        assert!(
            synchronized.iter().any(|path| path == &opened_parent),
            "opening dot must durably synchronize the Store entry in its parent"
        );
        let mut control =
            DurableStoreControl::initialize(store).expect("relative Store initializes");

        std::env::set_current_dir(&alternate).expect("worker changes cwd after open");
        control
            .advance_cold_reclamation()
            .expect("frozen absolute Store root remains writable");
        let mut store = control.into_store();
        store
            .load_full_audit()
            .expect("frozen Store reloads")
            .expect("frozen Store retains authority")
            .verify()
            .expect("frozen Store closure verifies");
        assert!(
            fs::read_dir(&alternate)
                .expect("alternate cwd lists")
                .next()
                .is_none(),
            "a relative handle must never resolve again after open"
        );
    }

    fn run_post_head_gc_failure_worker(root: &Path) -> ! {
        let mut store = DirectoryStore::open(root).expect("GC failure worker opens");
        assert!(matches!(
            advance_current(&mut store),
            Err(DurableError::CommitOutcomeUnknown { message })
                if message.contains("reclamation requires reopen")
        ));
        park_with_worker_marker(b"gc_sweep_unknown");
    }

    fn run_prior_gc_failure_worker(root: &Path, advance: bool) -> ! {
        let mut store = DirectoryStore::open(root).expect("prior-page failure worker opens");
        let expected = store
            .read_head()
            .expect("prior-page failure worker reads head")
            .expect("prior-page failure worker head exists");
        let result = if advance {
            advance_current(&mut store).map(|_| ())
        } else {
            reconcile_current(&mut store).map(|_| ())
        };
        assert!(matches!(
            result,
            Err(DurableError::Substrate { code, .. }) if code == INJECTED_FAILURE_CODE
        ));
        assert_eq!(store.read_head().expect("head rereads"), Some(expected));
        park_with_worker_marker(if advance {
            b"advance_prior_retryable"
        } else {
            b"reconcile_retryable"
        });
    }

    fn park_with_worker_marker(content: &[u8]) -> ! {
        let marker = PathBuf::from(
            std::env::var_os("CYMULE_DIRECTORY_TEST_MARKER").expect("failure worker marker exists"),
        );
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&marker)
            .expect("failure marker opens");
        file.write_all(content).expect("failure marker writes");
        file.sync_all().expect("failure marker syncs");
        sync_directory(marker.parent().expect("marker has parent"))
            .expect("failure marker directory syncs");
        loop {
            std::thread::park();
        }
    }

    fn kill_at(root: &Path, operation: &str, boundary: &str, marker: &Path) {
        let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("tests::crash_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("CYMULE_DIRECTORY_TEST_ROOT", root)
            .env("CYMULE_DIRECTORY_TEST_OPERATION", operation)
            .env("CYMULE_DIRECTORY_TEST_BOUNDARY", boundary)
            .env("CYMULE_DIRECTORY_TEST_MARKER", marker);
        let mut child = ManagedChild::spawn(&mut command).expect("worker spawns");
        child
            .wait_for_content(marker, boundary.as_bytes(), Duration::from_secs(15))
            .expect("worker reaches exact internal durability boundary");
        child.terminate().expect("worker is killed and reaped");
    }

    fn staging_residue(root: &Path) -> Vec<PathBuf> {
        let mut residue = Vec::new();
        for entry in fs::read_dir(root).expect("Store root lists") {
            let path = entry.expect("root entry reads").path();
            if path.extension().and_then(|value| value.to_str()) == Some("next") {
                residue.push(path);
            }
        }
        for family in DIRECTORY_FAMILIES {
            for entry in fs::read_dir(root.join(family)).expect("object family lists") {
                let path = entry.expect("object entry reads").path();
                if path.extension().and_then(|value| value.to_str()) == Some("next") {
                    residue.push(path);
                }
            }
        }
        for entry in
            fs::read_dir(root.join(OBJECT_STAGING_DIRECTORY)).expect("staging directory lists")
        {
            residue.push(entry.expect("staging entry reads").path());
        }
        residue.sort();
        residue
    }

    #[test]
    fn relative_root_handle_is_cwd_independent() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
        let status = command
            .arg("--exact")
            .arg("tests::relative_root_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("CYMULE_DIRECTORY_RELATIVE_ROOT_SANDBOX", temporary.path())
            .status()
            .expect("relative-root worker runs");
        assert!(status.success(), "relative-root worker failed: {status}");
        assert!(temporary.path().join("dot-store/head.json").is_file());
        assert!(
            fs::read_dir(temporary.path().join("alternate-cwd"))
                .expect("alternate cwd lists")
                .next()
                .is_none()
        );
    }

    #[test]
    fn object_staging_does_not_hold_the_head_cas_lock() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        DirectoryStore::open(&root).expect("empty /5 layout initializes");
        let marker = temporary.path().join("object-staging.marker");
        let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("tests::crash_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("CYMULE_DIRECTORY_TEST_ROOT", &root)
            .env("CYMULE_DIRECTORY_TEST_OPERATION", "commit")
            .env("CYMULE_DIRECTORY_TEST_BOUNDARY", "object_staged")
            .env("CYMULE_DIRECTORY_TEST_MARKER", &marker);
        let mut child = ManagedChild::spawn(&mut command).expect("staging worker spawns");
        child
            .wait_for_content(&marker, b"object_staged", Duration::from_secs(15))
            .expect("worker holds only object-staging exclusion");

        let mut observer = DirectoryStore::open(&root).expect("writable observer opens");
        assert!(
            observer
                .load_head()
                .expect("head read remains available during object fsync")
                .is_none()
        );
        let competing = DirectoryStore::open(&root).expect("competing writer opens");
        let Err(error) = DurableStoreControl::initialize(competing) else {
            panic!("second object stager must conflict immediately");
        };
        assert_eq!(
            error,
            DurableError::Conflict {
                expected: Some("object_stager_available".to_owned()),
                current: Some("object_stager_active".to_owned()),
            }
        );
        child
            .terminate()
            .expect("staging worker is killed and reaped");

        let recovered = DirectoryStore::open(&root).expect("staged Store reopens");
        DurableStoreControl::initialize(recovered)
            .expect("stale object staging is cleaned before retry");
        assert!(staging_residue(&root).is_empty());
    }

    #[test]
    fn receipt_head_load_remains_available_during_object_staging() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        advance_current(&mut store).expect("receipt head publishes");
        let receipt_state = store
            .load_full_audit()
            .expect("receipt state loads")
            .expect("receipt state exists");
        drop(store);

        let marker = temporary.path().join("receipt-object-staging.marker");
        let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("tests::crash_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("CYMULE_DIRECTORY_TEST_ROOT", &root)
            .env("CYMULE_DIRECTORY_TEST_OPERATION", "stage-orphan")
            .env("CYMULE_DIRECTORY_TEST_BOUNDARY", "object_staged")
            .env("CYMULE_DIRECTORY_TEST_MARKER", &marker);
        let mut child = ManagedChild::spawn(&mut command).expect("append worker spawns");
        child
            .wait_for_content(&marker, b"object_staged", Duration::from_secs(15))
            .expect("append worker holds object staging");

        let mut observer = DirectoryStore::open(&root).expect("observer opens");
        let loaded_head = observer
            .load_head()
            .expect("receipt head remains readable while staging is active")
            .expect("receipt head exists");
        assert_eq!(loaded_head, receipt_state.head);
        child
            .terminate()
            .expect("append worker is killed and reaped");

        let recovered_head = observer
            .load_head()
            .expect("idle load cleans staging without replaying the receipt")
            .expect("recovered head exists");
        assert_eq!(recovered_head, receipt_state.head);
        assert!(staging_residue(&root).is_empty());
    }

    #[test]
    fn writable_head_load_does_not_enumerate_committed_families() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let expected = initialize(&root);
        for family in DIRECTORY_FAMILIES {
            fs::set_permissions(root.join(family), fs::Permissions::from_mode(0o311))
                .expect("committed family enumeration is denied");
        }

        let result = (|| {
            let mut store = DirectoryStore::open(&root)?;
            store.load_head()
        })();
        for family in DIRECTORY_FAMILIES {
            fs::set_permissions(root.join(family), fs::Permissions::from_mode(0o700))
                .expect("committed family permissions restore");
        }
        let recovered = result
            .expect("writable head load does not enumerate committed families")
            .expect("head exists");
        assert_eq!(recovered, expected.head);
    }

    #[test]
    fn receipt_reconciliation_reads_only_its_authorized_page() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let current = initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        let orphan = GcReceipt::new_bounded(&current.head, BTreeSet::new(), 0)
            .expect("orphan receipt builds");
        store
            .write_gc_receipt(&orphan)
            .expect("orphan receipt persists");
        let orphan_path = store.gc_receipt_path(&orphan.receipt_id);
        let orphan_bytes = fs::read(&orphan_path).expect("orphan receipt reads");

        let receipt = advance_current(&mut store).expect("GC generation publishes");
        assert!(receipt.reclaimed_ids.contains(&orphan.receipt_id));
        assert!(!orphan_path.exists());
        fs::write(&orphan_path, orphan_bytes).expect("authorized object is reintroduced");
        let unrelated = root.join(STATE_ROOT_FAMILY).join("not-a-canonical-object");
        fs::write(&unrelated, b"malformed unrelated orphan")
            .expect("unrelated malformed orphan writes");
        let expected = store
            .read_head()
            .expect("receipt head reads")
            .expect("receipt head exists");

        let reopened = store
            .load_head()
            .expect("ordinary current authority loads")
            .expect("ordinary current authority exists");
        assert_eq!(reopened, expected);
        assert!(
            orphan_path.exists(),
            "ordinary load must not implicitly replay a pinned reclamation receipt"
        );
        assert!(
            unrelated.exists(),
            "ordinary load must not enumerate unrelated cold inventory"
        );

        assert_eq!(
            reconcile_current(&mut store).expect("exact receipt page reconciles"),
            receipt
        );
        assert_eq!(
            store.read_head().expect("head rereads"),
            Some(expected),
            "reconciliation must not publish a successor head"
        );
        assert!(!orphan_path.exists());
        assert!(
            unrelated.exists(),
            "reconciliation must not enumerate or interpret unrelated inventory"
        );
    }

    #[test]
    fn replay_rejects_a_receipt_above_the_directory_page_bound_before_unlink() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let current = initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        let orphan = GcReceipt::new_bounded(&current.head, BTreeSet::new(), 0)
            .expect("orphan receipt builds");
        store
            .write_gc_receipt(&orphan)
            .expect("orphan receipt persists");
        let orphan_path = store.gc_receipt_path(&orphan.receipt_id);

        let mut reclaimed = BTreeSet::from([orphan.receipt_id.clone()]);
        let mut index = 0_u64;
        while reclaimed.len() < DIRECTORY_GC_PAGE_OBJECTS + 1 {
            reclaimed.insert(format!("sha256:{index:064x}"));
            index += 1;
        }
        let oversized = GcReceipt::new_bounded(&current.head, reclaimed, 0)
            .expect("portable receipt permits a larger provider page");
        store
            .write_gc_receipt(&oversized)
            .expect("oversized receipt persists for replay test");
        let mut oversized_head = current.head.clone();
        oversized_head.gc_sequence = oversized.gc_sequence;
        oversized_head
            .physical_token
            .clone_from(&oversized.result_physical_token);
        oversized_head.gc_receipt = Some(oversized.receipt_id.clone());
        oversized
            .verify_for(&oversized_head)
            .expect("oversized head and receipt agree generically");
        fs::write(
            store.head_path(),
            cymule_core::canonical_bytes(&oversized_head).expect("oversized head encodes"),
        )
        .expect("oversized head writes");

        reset_gc_receipt_read_count();
        assert_eq!(
            store
                .load_head()
                .expect("bounded head ignores receipt page size"),
            Some(oversized_head.clone())
        );
        assert!(
            store
                .load_state_root_manifest(&oversized_head.state_root_manifest_id)
                .expect("manifest ignores receipt page size")
                .is_some()
        );
        assert_eq!(
            gc_receipt_read_count(),
            0,
            "ordinary small and oversized receipt generations have the same zero-byte read set"
        );
        assert!(matches!(
            reconcile_current(&mut store),
            Err(DurableError::Integrity { code, .. })
                if code == GC_RECEIPT_HEAD_INTEGRITY_CODE
        ));
        assert!(
            orphan_path.exists(),
            "provider-local replay admission must fail before the first unlink"
        );
        assert_eq!(
            store.read_head().expect("head rereads"),
            Some(oversized_head)
        );
    }

    #[test]
    fn internal_commit_sigkill_boundaries_reopen_closed_state() {
        for boundary in [
            "object_staged",
            "commit_objects_durable",
            "commit_head_staged",
            "commit_head_renamed",
            "commit_head_durable",
        ] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let root = temporary.path().join("store");
            DirectoryStore::open(&root).expect("empty /5 layout initializes");
            let marker = temporary.path().join("commit.marker");
            kill_at(&root, "commit", boundary, &marker);

            let mut reopened = DirectoryStore::open(&root).expect("killed commit reopens");
            let recovered = reopened
                .load_full_audit()
                .expect("killed commit has closed authority");
            if boundary == "object_staged" {
                assert!(
                    staging_residue(&root).is_empty(),
                    "writable load must clean dead object staging"
                );
            }
            match recovered {
                Some(state) => state
                    .verify()
                    .expect("published StateRoot closure verifies"),
                None if matches!(
                    boundary,
                    "object_staged" | "commit_objects_durable" | "commit_head_staged"
                ) =>
                {
                    initialize(&root)
                        .verify()
                        .expect("pre-head objects and staging residue are recoverable");
                }
                None => panic!("durable or renamed head disappeared at {boundary}"),
            }
            assert!(
                staging_residue(&root).is_empty(),
                "boundary {boundary} left staging residue after writable recovery"
            );
        }
    }

    #[test]
    fn internal_gc_sigkill_boundaries_replay_exact_receipt() {
        for boundary in [
            "gc_receipt_durable",
            "gc_head_staged",
            "gc_head_renamed",
            "gc_head_durable",
            "gc_deletion_started",
            "gc_deletion_durable",
        ] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let root = temporary.path().join("store");
            let expected = populate_gc_orphan(&root);
            let marker = temporary.path().join("gc.marker");
            kill_at(&root, "gc", boundary, &marker);

            let mut reopened = DirectoryStore::open(&root).expect("killed GC reopens");
            let recovered = reopened
                .load_full_audit()
                .expect("writable load validates the exact retained head")
                .expect("semantic state remains");
            recovered
                .verify()
                .expect("recovered StateRoot closure verifies");
            assert_eq!(recovered.revision, expected.revision, "{boundary}");
            assert_eq!(recovered.state, expected.state, "{boundary}");
            let final_receipt = advance_current(&mut reopened)
                .expect("explicit post-reopen generation collects crash residue");
            assert_eq!(final_receipt.remaining_objects, 0, "{boundary}");
            assert_eq!(
                reopened
                    .stats()
                    .expect("post-recovery physical stats read")
                    .gc_receipts,
                1,
                "{boundary}"
            );
            assert!(
                staging_residue(&root).is_empty(),
                "boundary {boundary} left staging residue after GC recovery"
            );
        }
    }

    #[test]
    fn generation_publication_boundaries_survive_process_death() {
        for boundary in [
            "generation_bootstrap_durable",
            "generation_marker_staged",
            "generation_marker_published",
        ] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let root = temporary.path().join("store");
            let marker = temporary.path().join("generation.marker");
            kill_at(&root, "initialize-layout", boundary, &marker);
            assert!(root.join(DIRECTORY_BOOTSTRAP_MARKER).is_dir());
            assert_eq!(
                root.join(DIRECTORY_META_STAGING_FILE).exists(),
                boundary == "generation_marker_staged"
            );
            assert_eq!(
                root.join(DIRECTORY_META_FILE).exists(),
                boundary == "generation_marker_published"
            );

            let mut reopened = DirectoryStore::open(&root).expect("staged generation resumes");
            assert!(
                reopened
                    .load_full_audit()
                    .expect("empty resumed Store loads")
                    .is_none()
            );
            require_current_directory_layout(&root).expect("resumed /5 layout is exact");
            assert!(staging_residue(&root).is_empty());
        }
    }

    #[test]
    fn receipt_replay_syncs_families_when_the_only_object_is_already_absent() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        advance_current(&mut store).expect("empty first GC receipt publishes");
        drop(store);

        let first_marker = temporary.path().join("single-delete.marker");
        kill_at(&root, "gc", "gc_deletion_started", &first_marker);
        let second_marker = temporary.path().join("family-sync.marker");
        kill_at(&root, "reconcile", "gc_family_synced", &second_marker);

        let mut reopened = DirectoryStore::open(&root).expect("replayed Store reopens");
        reconcile_current(&mut reopened).expect("final explicit replay completes");
        reopened
            .load_full_audit()
            .expect("reconciled state loads")
            .expect("semantic state remains")
            .verify()
            .expect("final StateRoot closure verifies");
    }

    #[test]
    fn prior_receipt_sweep_failure_keeps_the_old_head_authoritative() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        initialize(&root);
        let mut store = DirectoryStore::open(&root).expect("Store opens");
        advance_current(&mut store).expect("receipt head publishes");
        let expected = store
            .read_head()
            .expect("receipt head reads")
            .expect("receipt head exists");
        drop(store);

        for (operation, marker_content) in [
            ("reconcile-failure", b"reconcile_retryable".as_slice()),
            (
                "advance-prior-failure",
                b"advance_prior_retryable".as_slice(),
            ),
        ] {
            let marker = temporary.path().join(format!("{operation}.marker"));
            let mut command =
                ProcessCommand::new(std::env::current_exe().expect("test executable"));
            command
                .arg("--exact")
                .arg("tests::crash_worker")
                .arg("--ignored")
                .arg("--nocapture")
                .env("CYMULE_DIRECTORY_TEST_ROOT", &root)
                .env("CYMULE_DIRECTORY_TEST_OPERATION", operation)
                .env("CYMULE_DIRECTORY_TEST_FAILURE", "gc_sweep_started")
                .env("CYMULE_DIRECTORY_TEST_MARKER", &marker);
            let mut child = ManagedChild::spawn(&mut command).expect("failure worker spawns");
            child
                .wait_for_content(&marker, marker_content, Duration::from_secs(15))
                .expect("worker reports retryable prior-page failure");
            child
                .terminate()
                .expect("failure worker is killed and reaped");

            let mut reopened = DirectoryStore::open(&root).expect("Store reopens");
            assert_eq!(
                reopened.read_head().expect("head rereads"),
                Some(expected.clone())
            );
            reconcile_current(&mut reopened).expect("old receipt explicitly replays after failure");
            assert_eq!(
                reopened
                    .load_full_audit()
                    .expect("state loads after explicit receipt replay")
                    .expect("semantic state remains")
                    .head,
                expected
            );
        }
    }

    #[test]
    fn process_death_during_prior_receipt_sweep_keeps_the_old_head_authoritative() {
        for operation in ["reconcile", "gc"] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let root = temporary.path().join("store");
            let current = initialize(&root);
            let mut store = DirectoryStore::open(&root).expect("Store opens");
            let orphan = GcReceipt::new_bounded(&current.head, BTreeSet::new(), 0)
                .expect("orphan receipt builds");
            store
                .write_gc_receipt(&orphan)
                .expect("orphan receipt persists");
            let orphan_path = store.gc_receipt_path(&orphan.receipt_id);
            let receipt = advance_current(&mut store).expect("first GC generation publishes");
            assert!(receipt.reclaimed_ids.contains(&orphan.receipt_id));
            assert!(!orphan_path.exists());
            store
                .write_gc_receipt(&orphan)
                .expect("authorized object reappears durably");
            let expected = store
                .read_head()
                .expect("receipt head reads")
                .expect("receipt head exists");
            drop(store);

            let marker = temporary
                .path()
                .join(format!("{operation}-prior-delete.marker"));
            kill_at(&root, operation, "gc_deletion_started", &marker);

            let mut reopened = DirectoryStore::open(&root).expect("killed Store reopens");
            assert_eq!(
                reopened.read_head().expect("head rereads"),
                Some(expected.clone()),
                "a prior-page sweep cannot publish a successor head"
            );
            let retained = reopened
                .load_full_audit()
                .expect("ordinary reopen validates retained authority")
                .expect("semantic state remains");
            assert_eq!(retained.head, expected);
            reconcile_current(&mut reopened)
                .expect("the old receipt replays idempotently after process death");
            assert_eq!(
                reopened
                    .load_full_audit()
                    .expect("reconciled state loads")
                    .expect("semantic state remains")
                    .head,
                retained.head
            );
            assert!(!orphan_path.exists());
        }
    }

    #[test]
    fn post_head_sweep_failure_is_unknown_and_reopen_completes_it() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("store");
        let expected = populate_gc_orphan(&root);
        let marker = temporary.path().join("gc-failure.marker");
        let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
        command
            .arg("--exact")
            .arg("tests::crash_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("CYMULE_DIRECTORY_TEST_ROOT", &root)
            .env("CYMULE_DIRECTORY_TEST_OPERATION", "gc-failure")
            .env("CYMULE_DIRECTORY_TEST_FAILURE", "gc_sweep_started")
            .env("CYMULE_DIRECTORY_TEST_MARKER", &marker);
        let mut child = ManagedChild::spawn(&mut command).expect("failure worker spawns");
        child
            .wait_for_content(&marker, b"gc_sweep_unknown", Duration::from_secs(15))
            .expect("worker observes unknown post-head sweep outcome");
        child
            .terminate()
            .expect("failure worker is killed and reaped");

        let mut reopened = DirectoryStore::open(&root).expect("failed sweep Store reopens");
        reconcile_current(&mut reopened)
            .expect("reopen explicitly completes receipt-authorized sweep");
        let recovered = reopened
            .load_full_audit()
            .expect("reconciled authority loads")
            .expect("semantic state remains");
        recovered.verify().expect("recovered closure verifies");
        assert_eq!(recovered.revision, expected.revision);
        assert_eq!(recovered.state, expected.state);
    }
}

#[cfg(test)]
#[cfg(not(unix))]
mod non_unix_tests {
    use super::*;

    #[test]
    fn writable_generation_fails_before_initialization() {
        let temporary = tempfile::tempdir().expect("temporary parent creates");
        let root = temporary.path().join("store");
        let error =
            DirectoryStore::open(&root).expect_err("non-Unix writable generation is unsupported");
        assert!(matches!(
            error,
            DurableError::Substrate { code, .. }
                if code == UNSUPPORTED_STORE_GENERATION_CODE
        ));
        assert!(
            !root.exists(),
            "unsupported writable open must not create a partial generation"
        );
    }
}
