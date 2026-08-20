//! Atomic directory realization of the segmented `DurableStore` contract.
use cymule_durable::{
    DurableError, DurableResult, DurableStore, GcReceipt, StateCheckpoint, StateSegment,
    StoreBatch, StoreCommit, StoreHead, StoreStats, StoredState, restore,
};
use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
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
            && head != batch.head
        {
            return Err(DurableError::Validation(
                "mixed directory head does not match the legacy import".to_owned(),
            ));
        }
        let checkpoint = batch.checkpoint.as_ref().expect("initial checkpoint");
        store.write_immutable("checkpoints", &checkpoint.checkpoint_id, checkpoint)?;
        write_atomic(&store.root, &store.head_path(), &batch.head)?;
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
            receipt.verify_for(head)?;
        }
        let checkpoint: StateCheckpoint = read_required(
            &self.object_path("checkpoints", &head.checkpoint_id),
            &head.checkpoint_id,
        )?;
        let mut values = BTreeMap::new();
        let mut cursor = if head.suffix_len == 0 {
            checkpoint.covered_segment.clone()
        } else {
            head.suffix_head.clone()
        };
        while cursor.as_deref() != checkpoint.covered_segment.as_deref() {
            if values.len() >= cymule_durable::MAX_HOT_SEGMENTS as usize {
                return Err(DurableError::Validation(
                    "directory suffix exceeds reopen bound".to_owned(),
                ));
            }
            let id = cursor
                .as_ref()
                .ok_or_else(|| {
                    DurableError::Validation(
                        "directory suffix does not connect to checkpoint".to_owned(),
                    )
                })?
                .clone();
            let value: StateSegment = read_required(&self.object_path("segments", &id), &id)?;
            cursor.clone_from(&value.parent_segment);
            values.insert(id, value);
        }
        restore(
            head,
            |id| (id == checkpoint.checkpoint_id).then(|| checkpoint.clone()),
            |id| values.get(id).cloned(),
        )
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
}
impl DurableStore for DirectoryStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        let _claim = self.claim()?;
        self.read_head()?
            .map(|head| self.read_state(&head).map(|value| value.0))
            .transpose()
    }
    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let _claim = self.claim()?;
        let current_head = self.read_head()?;
        if current_head.as_ref() != expected {
            return Err(DurableError::Conflict {
                expected: expected.map(|head| head.revision.clone()),
                current: current_head.map(|head| head.revision),
            });
        }
        let current = current_head
            .as_ref()
            .map(|head| self.read_state(head).map(|value| value.0))
            .transpose()?;
        batch.verify_against(current.as_ref())?;
        if let Some(segment) = &batch.segment {
            self.write_immutable("segments", &segment.segment_id, segment)?;
        }
        if let Some(checkpoint) = &batch.checkpoint {
            self.write_immutable("checkpoints", &checkpoint.checkpoint_id, checkpoint)?;
        }
        write_atomic(&self.root, &self.head_path(), &batch.head)?;
        Ok(StoreCommit {
            revision: batch.head.revision.clone(),
            head: batch.head.clone(),
        })
    }
    fn reclaim_cold(&mut self, expected: &StoreHead) -> DurableResult<GcReceipt> {
        let _claim = self.claim()?;
        let current = self.read_head()?;
        if current.as_ref() != Some(expected) {
            return Err(DurableError::Conflict {
                expected: Some(expected.revision.clone()),
                current: current.map(|head| head.revision),
            });
        }
        let checkpoint: StateCheckpoint = read_required(
            &self.object_path("checkpoints", &expected.checkpoint_id),
            &expected.checkpoint_id,
        )?;
        let mut hot = BTreeSet::new();
        let mut cursor = if expected.suffix_len == 0 {
            checkpoint.covered_segment.clone()
        } else {
            expected.suffix_head.clone()
        };
        while cursor.as_deref() != checkpoint.covered_segment.as_deref() {
            let id = cursor.ok_or_else(|| {
                DurableError::Validation(
                    "directory suffix does not connect to checkpoint".to_owned(),
                )
            })?;
            let value: StateSegment = read_required(&self.object_path("segments", &id), &id)?;
            cursor = value.parent_segment;
            hot.insert(self.object_path("segments", &id));
        }
        let retained_checkpoint = self.object_path("checkpoints", &expected.checkpoint_id);
        let mut reclaimed = BTreeSet::new();
        for family in ["checkpoints", "segments"] {
            for entry in fs::read_dir(self.root.join(family)).map_err(substrate)? {
                let path = entry.map_err(substrate)?.path();
                let keep = if family == "checkpoints" {
                    path == retained_checkpoint
                } else {
                    hot.contains(&path)
                };
                if !keep {
                    let object_id = if family == "checkpoints" {
                        read_required::<StateCheckpoint>(&path, "cold checkpoint")?.checkpoint_id
                    } else {
                        read_required::<StateSegment>(&path, "cold segment")?.segment_id
                    };
                    reclaimed.insert(object_id);
                    fs::remove_file(path).map_err(substrate)?;
                }
            }
        }
        let receipt = GcReceipt::new(expected, &reclaimed)?;
        self.write_immutable("gc-receipts", &receipt.receipt_id, &receipt)?;
        let mut head = expected.clone();
        head.gc_receipt = Some(receipt.receipt_id.clone());
        write_atomic(&self.root, &self.head_path(), &head)?;
        Ok(receipt)
    }
    fn stats(&self) -> DurableResult<StoreStats> {
        Ok(StoreStats {
            checkpoints: count_files(&self.root.join("checkpoints"))?,
            segments: count_files(&self.root.join("segments"))?,
            reopened_segments: self.read_head()?.map_or(0, |head| head.suffix_len),
            gc_receipts: count_files(&self.root.join("gc-receipts"))?,
        })
    }
}
fn read_optional<T: DeserializeOwned>(path: &Path) -> DurableResult<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(Into::into),
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
fn count_files(path: &Path) -> DurableResult<u64> {
    u64::try_from(fs::read_dir(path).map_err(substrate)?.count())
        .map_err(|error| DurableError::Validation(error.to_string()))
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
