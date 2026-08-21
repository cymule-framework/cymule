//! Atomic directory realization of the segmented `DurableStore` contract.
use cymule_durable::{
    DurableError, DurableResult, DurableStore, GcReceipt, StateCheckpoint, StateSegment,
    StoreBatch, StoreCommit, StoreHead, StoreStats, StoredState, restore,
};
use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
/// Directory-backed segmented durable domain.
pub struct DirectoryStore {
    root: PathBuf,
}

/// Evidence emitted by explicit offline whole-state import.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyMigrationReceipt {
    /// Migration receipt schema.
    pub migration_version: String,
    /// Authenticated legacy semantic revision.
    pub legacy_revision: String,
    /// New sequence-zero checkpoint.
    pub checkpoint_id: String,
    /// Content-addressed receipt identity.
    pub receipt_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyStoredState {
    revision: String,
    state: cymule_durable::DurableState,
}

impl DirectoryStore {
    /// Open or create a segmented directory store.
    pub fn open(root: impl AsRef<Path>) -> DurableResult<Self> {
        let root = root.as_ref().to_path_buf();
        if root.join("state.json").exists() {
            let message = if root.join("head.json").exists() {
                "refusing mixed legacy and segmented directory formats"
            } else {
                "legacy directory store requires explicit offline DirectoryStore::migrate_v1"
            };
            return Err(DurableError::Validation(message.to_owned()));
        }
        for family in ["checkpoints", "segments", "gc-receipts"] {
            fs::create_dir_all(root.join(family)).map_err(substrate)?;
        }
        Ok(Self { root })
    }

    /// Convert one old whole-state directory while no readers or writers exist.
    pub fn migrate_v1(root: impl AsRef<Path>) -> DurableResult<LegacyMigrationReceipt> {
        let root = root.as_ref().to_path_buf();
        let legacy_path = root.join("state.json");
        if !legacy_path.exists() {
            return Err(DurableError::Validation(
                "legacy directory state.json does not exist".to_owned(),
            ));
        }
        for family in ["checkpoints", "segments", "gc-receipts"] {
            fs::create_dir_all(root.join(family)).map_err(substrate)?;
        }
        let store = Self { root };
        let legacy_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(store.root.join("state.lock"))
            .map_err(substrate)?;
        match FileExt::try_lock(&legacy_lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(DurableError::Conflict {
                    expected: Some("offline_legacy_writer".to_owned()),
                    current: Some("legacy_writer_active".to_owned()),
                });
            }
            Err(TryLockError::Error(error)) => return Err(substrate(error)),
        }
        let _claim = store.claim()?;
        let legacy: LegacyStoredState = read_required(&legacy_path, "legacy state")?;
        if legacy.state.revision()? != legacy.revision {
            return Err(DurableError::Validation(
                "legacy directory revision does not authenticate its state".to_owned(),
            ));
        }
        let batch = StoreBatch::initialize(legacy.state)?;
        if let Some(head) = store.read_head()?
            && head != *batch.head()
        {
            return Err(DurableError::Validation(
                "mixed directory head does not match the legacy import".to_owned(),
            ));
        }
        let checkpoint = batch.checkpoint().expect("initial checkpoint");
        store.write_immutable("checkpoints", &checkpoint.checkpoint_id, checkpoint)?;
        write_atomic(&store.root, &store.head_path(), batch.head())?;
        let receipt_id = cymule_core::content_id(
            "cymule.directory-v1-migration/1",
            &(legacy.revision.as_str(), checkpoint.checkpoint_id.as_str()),
        )?;
        let receipt = LegacyMigrationReceipt {
            migration_version: "cymule.directory-v1-migration/1".to_owned(),
            legacy_revision: legacy.revision,
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            receipt_id,
        };
        write_atomic(
            &store.root,
            &store.root.join("migration-v1-receipt.json"),
            &receipt,
        )?;
        fs::remove_file(legacy_path).map_err(substrate)?;
        sync_directory(&store.root)?;
        Ok(receipt)
    }
    /// Return the store root.
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn head_path(&self) -> PathBuf {
        self.root.join("head.json")
    }
    fn claim(&self) -> DurableResult<File> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join("head.lock"))
            .map_err(substrate)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(DurableError::Conflict {
                expected: Some("writer_available".to_owned()),
                current: Some("writer_active".to_owned()),
            }),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }
    fn object_path(&self, family: &str, id: &str) -> PathBuf {
        self.root
            .join(family)
            .join(cymule_core::sha256_bytes(id.as_bytes()))
    }
    fn read_head(&self) -> DurableResult<Option<StoreHead>> {
        read_optional(&self.head_path())
    }
    fn read_state(&self, head: &StoreHead) -> DurableResult<(StoredState, u32)> {
        if let Some(receipt_id) = &head.gc_receipt {
            let receipt: GcReceipt =
                read_required(&self.object_path("gc-receipts", receipt_id), receipt_id)?;
            if receipt.receipt_id != *receipt_id {
                return Err(DurableError::Validation(
                    "directory GC receipt locator does not match its content identity".to_owned(),
                ));
            }
            receipt.verify_for(head)?;
        }
        restore(
            head,
            |id| self.read_checkpoint(id),
            |id| self.read_segment(id),
        )
    }

    fn read_checkpoint(&self, id: &str) -> DurableResult<Option<StateCheckpoint>> {
        let path = self.object_path("checkpoints", id);
        let value: Option<StateCheckpoint> = read_optional(&path)?;
        if value
            .as_ref()
            .is_some_and(|value| value.checkpoint_id != id)
        {
            return Err(DurableError::Validation(
                "directory checkpoint locator does not match its content identity".to_owned(),
            ));
        }
        Ok(value)
    }

    fn read_segment(&self, id: &str) -> DurableResult<Option<StateSegment>> {
        let path = self.object_path("segments", id);
        let value: Option<StateSegment> = read_optional(&path)?;
        if value.as_ref().is_some_and(|value| value.segment_id != id) {
            return Err(DurableError::Validation(
                "directory segment locator does not match its content identity".to_owned(),
            ));
        }
        Ok(value)
    }

    fn clean_staging_residue(&self) -> DurableResult<()> {
        let mut root_changed = remove_if_present(&self.root.join("head.next"))?;
        for family in ["checkpoints", "segments", "gc-receipts"] {
            let directory = self.root.join(family);
            let mut changed = false;
            for entry in fs::read_dir(&directory).map_err(substrate)? {
                let path = entry.map_err(substrate)?.path();
                if path.extension().and_then(|value| value.to_str()) == Some("next") {
                    changed |= remove_if_present(&path)?;
                }
            }
            if changed {
                sync_directory(&directory)?;
                root_changed = true;
            }
        }
        if root_changed {
            sync_directory(&self.root)?;
        }
        Ok(())
    }
    fn write_immutable<T: Serialize + DeserializeOwned + PartialEq>(
        &self,
        family: &str,
        id: &str,
        value: &T,
    ) -> DurableResult<()> {
        let path = self.object_path(family, id);
        if path.exists() {
            let retained: T = read_required(&path, id)?;
            if retained != *value {
                return Err(DurableError::Validation(format!(
                    "immutable directory object {id} has conflicting bytes"
                )));
            }
            return Ok(());
        }
        write_atomic(&self.root, &path, value)
    }

    fn finish_reclamation(&self, receipt: &GcReceipt) -> DurableResult<()> {
        let mut deleted = false;
        for object_id in &receipt.reclaimed_ids {
            for family in ["checkpoints", "segments"] {
                let path = self.object_path(family, object_id);
                match fs::remove_file(&path) {
                    Ok(()) => {
                        if !deleted {
                            deleted = true;
                            test_gc_boundary("gc_deletion_started")?;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(substrate(error)),
                }
            }
        }
        sync_directory(&self.root.join("checkpoints"))?;
        sync_directory(&self.root.join("segments"))?;
        Ok(())
    }
}
impl DurableStore for DirectoryStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        let _claim = self.claim()?;
        self.clean_staging_residue()?;
        let Some(head) = self.read_head()? else {
            return Ok(None);
        };
        let state = self.read_state(&head)?.0;
        if let Some(receipt_id) = &head.gc_receipt {
            let receipt: GcReceipt =
                read_required(&self.object_path("gc-receipts", receipt_id), receipt_id)?;
            receipt.verify_for(&head)?;
            self.finish_reclamation(&receipt)?;
        }
        Ok(Some(state))
    }
    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let _claim = self.claim()?;
        self.clean_staging_residue()?;
        let current_head = self.read_head()?;
        if current_head.as_ref() != expected {
            return Err(DurableError::Conflict {
                expected: expected.map(|head| head.revision.clone()),
                current: current_head.map(|head| head.revision),
            });
        }
        batch.verify_against(current_head.as_ref())?;
        if let Some(segment) = batch.segment() {
            self.write_immutable("segments", &segment.segment_id, segment)?;
        }
        if let Some(checkpoint) = batch.checkpoint() {
            self.write_immutable("checkpoints", &checkpoint.checkpoint_id, checkpoint)?;
        }
        write_atomic(&self.root, &self.head_path(), batch.head())?;
        Ok(StoreCommit {
            revision: batch.head().revision.clone(),
            head: batch.head().clone(),
        })
    }
    fn reclaim_cold(&mut self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        if let Some(receipt_id) = &expected.gc_receipt {
            let receipt: GcReceipt =
                read_required(&self.object_path("gc-receipts", receipt_id), receipt_id)?;
            if receipt.receipt_id != *receipt_id {
                return Err(DurableError::Validation(
                    "directory GC receipt locator does not match its content identity".to_owned(),
                ));
            }
            receipt.verify_for(expected)?;
            self.finish_reclamation(&receipt)?;
            return Ok(receipt);
        }
        let stored = self.read_state(expected)?.0;
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
        let _claim = self.claim()?;
        self.clean_staging_residue()?;
        let current = self.read_head()?;
        if current.as_ref() != Some(expected) {
            return Err(DurableError::Conflict {
                expected: Some(expected.revision.clone()),
                current: current.map(|head| head.revision),
            });
        }
        let mut reclaimed = BTreeSet::new();
        for family in ["checkpoints", "segments"] {
            for entry in fs::read_dir(self.root.join(family)).map_err(substrate)? {
                let path = entry.map_err(substrate)?.path();
                let object_id = if family == "checkpoints" {
                    read_required::<StateCheckpoint>(&path, "cold checkpoint")?.checkpoint_id
                } else {
                    read_required::<StateSegment>(&path, "cold segment")?.segment_id
                };
                reclaimed.insert(object_id);
            }
        }
        reclaimed.remove(&checkpoint.checkpoint_id);
        self.write_immutable("checkpoints", &checkpoint.checkpoint_id, &checkpoint)?;
        test_gc_boundary("gc_checkpoint_persisted")?;
        let receipt = GcReceipt::new(&head, &reclaimed)?;
        self.write_immutable("gc-receipts", &receipt.receipt_id, &receipt)?;
        test_gc_boundary("gc_receipt_persisted")?;
        head.gc_receipt = Some(receipt.receipt_id.clone());
        write_atomic(&self.root, &self.head_path(), &head)?;
        test_gc_boundary("gc_head_published")?;
        self.finish_reclamation(&receipt)?;
        Ok(receipt)
    }
    fn stats(&self) -> DurableResult<StoreStats> {
        Ok(StoreStats {
            checkpoints: count_committed_files(&self.root.join("checkpoints"))?,
            segments: count_committed_files(&self.root.join("segments"))?,
            reopened_segments: self.read_head()?.map_or(0, |head| head.suffix_len),
            gc_receipts: count_committed_files(&self.root.join("gc-receipts"))?,
        })
    }
}
fn read_optional<T: DeserializeOwned>(path: &Path) -> DurableResult<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => cymule_core::decode_json(&bytes)
            .map(Some)
            .map_err(Into::into),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(substrate(error)),
    }
}
fn read_required<T: DeserializeOwned>(path: &Path, id: &str) -> DurableResult<T> {
    read_optional(path)?.ok_or_else(|| {
        DurableError::NotFound(format!("directory durable object {id} does not exist"))
    })
}
fn write_atomic(root: &Path, path: &Path, value: &impl Serialize) -> DurableResult<()> {
    let staging = path.with_extension("next");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&staging)
        .map_err(substrate)?;
    file.write_all(&cymule_core::canonical_bytes(value)?)
        .map_err(substrate)?;
    file.sync_all().map_err(substrate)?;
    drop(file);
    fs::rename(staging, path).map_err(substrate)?;
    sync_directory(path.parent().expect("object has parent"))?;
    if path.parent() != Some(root) {
        sync_directory(root)?;
    }
    Ok(())
}
fn count_committed_files(path: &Path) -> DurableResult<u64> {
    let count = fs::read_dir(path)
        .map_err(substrate)?
        .try_fold(0_u64, |count, entry| {
            let path = entry.map_err(substrate)?.path();
            if path.extension().and_then(|value| value.to_str()) == Some("next") {
                Ok(count)
            } else {
                count
                    .checked_add(1)
                    .ok_or_else(|| DurableError::Validation("file count overflowed".to_owned()))
            }
        })?;
    Ok(count)
}

fn remove_if_present(path: &Path) -> DurableResult<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(substrate(error)),
    }
}

#[cfg(not(test))]
#[allow(clippy::unnecessary_wraps)]
fn test_gc_boundary(_boundary: &str) -> DurableResult<()> {
    Ok(())
}

#[cfg(test)]
fn test_gc_boundary(boundary: &str) -> DurableResult<()> {
    if std::env::var("CYMULE_DIRECTORY_GC_TEST_BOUNDARY").as_deref() != Ok(boundary) {
        return Ok(());
    }
    let marker = std::env::var_os("CYMULE_DIRECTORY_GC_TEST_MARKER").ok_or_else(|| {
        DurableError::Validation("directory GC test boundary requires a marker".to_owned())
    })?;
    let marker = PathBuf::from(marker);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&marker)
        .map_err(substrate)?;
    file.write_all(boundary.as_bytes()).map_err(substrate)?;
    file.sync_all().map_err(substrate)?;
    sync_directory(marker.parent().expect("test marker has a parent"))?;
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use cymule_core::Machine;
    use cymule_durable::{DurableDelta, DurableOperation, DurableState, JournalRecord, StoreBatch};
    use cymule_test_world::ManagedChild;
    use serde_json::json;
    use std::process::Command;
    use std::time::Duration;

    fn populate(root: &Path) -> DurableResult<StoredState> {
        let mut store = DirectoryStore::open(root)?;
        let batch = StoreBatch::initialize(DurableState::new(Machine::new().snapshot()))?;
        store.compare_and_commit(None, &batch)?;
        let mut current = store.load()?.expect("initialized state");
        for index in 0..3 {
            let delta = DurableDelta::new(vec![DurableOperation::AppendJournal {
                journal_id: "journal:gc-crash".to_owned(),
                records: vec![JournalRecord::new(
                    format!("record:{index}"),
                    "test.gc-crash/1",
                    json!({"index": index}),
                )?],
            }])?;
            let batch = StoreBatch::transition(&current, delta)?;
            let commit = store.compare_and_commit(Some(&current.head), &batch)?;
            batch.apply_committed(&mut current, &commit)?;
        }
        Ok(current)
    }

    #[test]
    #[ignore = "child-process worker invoked by gc_internal_sigkill_boundaries_reopen"]
    fn gc_crash_worker() {
        let Some(root) = std::env::var_os("CYMULE_DIRECTORY_GC_TEST_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let mut store = DirectoryStore::open(root).expect("worker opens");
        let head = store.load().expect("worker loads").expect("state").head;
        store
            .reclaim_cold(&head)
            .expect("worker reaches crash boundary");
        panic!("worker did not stop at its requested GC boundary");
    }

    #[test]
    fn gc_internal_sigkill_boundaries_reopen() {
        for boundary in [
            "gc_checkpoint_persisted",
            "gc_receipt_persisted",
            "gc_head_published",
            "gc_deletion_started",
        ] {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let expected = populate(temporary.path()).expect("store populates");
            let marker = temporary.path().join("gc-boundary.marker");
            let mut command = Command::new(std::env::current_exe().expect("test executable"));
            command
                .arg("--exact")
                .arg("tests::gc_crash_worker")
                .arg("--ignored")
                .arg("--nocapture")
                .env("CYMULE_DIRECTORY_GC_TEST_ROOT", temporary.path())
                .env("CYMULE_DIRECTORY_GC_TEST_BOUNDARY", boundary)
                .env("CYMULE_DIRECTORY_GC_TEST_MARKER", &marker);
            let mut child = ManagedChild::spawn(&mut command).expect("worker spawns");
            child
                .wait_for_content(&marker, boundary.as_bytes(), Duration::from_secs(10))
                .expect("worker reaches exact durable boundary");
            child.terminate().expect("worker is killed and reaped");

            let mut reopened = DirectoryStore::open(temporary.path()).expect("store reopens");
            let recovered = reopened
                .load()
                .expect("ordinary load recovers")
                .expect("state remains");
            assert_eq!(recovered.state, expected.state, "boundary {boundary}");
            assert_eq!(recovered.revision, expected.revision, "boundary {boundary}");
            recovered.verify().expect("recovered state verifies");
        }
    }
}
#[cfg(unix)]
fn sync_directory(path: &Path) -> DurableResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(substrate)
}
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> DurableResult<()> {
    Ok(())
}
fn substrate(error: impl std::fmt::Display) -> DurableError {
    DurableError::Substrate(error.to_string())
}
