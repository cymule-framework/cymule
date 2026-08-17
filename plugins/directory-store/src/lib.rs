//! Atomic directory-backed realization of the provider-neutral `DurableStore`.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use cymule_durable::{
    DurableError, DurableResult, DurableState, DurableStore, StoreCommit, StoredState,
};
use fs4::{FileExt, TryLockError};

/// Local directory store using one locked, atomically replaced state file.
#[derive(Debug, Clone)]
pub struct DirectoryStore {
    root: PathBuf,
}

impl DirectoryStore {
    /// Create or open a store directory.
    pub fn open(root: impl AsRef<Path>) -> DurableResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(substrate)?;
        Ok(Self { root })
    }

    /// Store directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn state_path(&self) -> PathBuf {
        self.root.join("state.json")
    }

    fn staging_path(&self) -> PathBuf {
        self.root.join("state.next")
    }

    fn writer_claim(&self) -> DurableResult<File> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join("state.lock"))
            .map_err(substrate)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(DurableError::Conflict {
                    expected: Some("writer_available".to_owned()),
                    current: Some("writer_active".to_owned()),
                });
            }
            Err(TryLockError::Error(error)) => return Err(substrate(error)),
        }
        Ok(lock)
    }

    fn read_unlocked(&self) -> DurableResult<Option<StoredState>> {
        let path = self.state_path();
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(substrate(error)),
        };
        let stored: StoredState = serde_json::from_slice(&bytes)?;
        stored.verify()?;
        Ok(Some(stored))
    }
}

impl DurableStore for DirectoryStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        let _claim = self.writer_claim()?;
        self.read_unlocked()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let _claim = self.writer_claim()?;
        let current = self.read_unlocked()?;
        let current_revision = current.as_ref().map(|stored| stored.revision.clone());
        if expected_revision != current_revision.as_deref() {
            return Err(DurableError::Conflict {
                expected: expected_revision.map(str::to_owned),
                current: current_revision,
            });
        }

        let revision = next.revision()?;
        let stored = StoredState {
            revision: revision.clone(),
            state: next.clone(),
        };
        let bytes = cymule_core::canonical_bytes(&stored)?;
        let staging_path = self.staging_path();
        let mut staging = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staging_path)
            .map_err(substrate)?;
        staging.write_all(&bytes).map_err(substrate)?;
        staging.sync_all().map_err(substrate)?;
        drop(staging);
        fs::rename(staging_path, self.state_path()).map_err(substrate)?;
        sync_directory(&self.root)?;
        Ok(StoreCommit { revision })
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
