use std::sync::{Arc, Mutex, TryLockError};

use crate::{DurableError, DurableResult, DurableState, StoredState};

/// Result of one atomic durable compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreCommit {
    /// Newly committed revision.
    pub revision: String,
}

/// Provider-neutral single-domain durable state store.
pub trait DurableStore {
    /// Load the latest complete revision.
    fn load(&mut self) -> DurableResult<Option<StoredState>>;

    /// Atomically replace the complete state if `expected_revision` is current.
    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit>;
}

/// In-memory reference implementation for conformance and fault simulation.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    current: Arc<Mutex<Option<StoredState>>>,
}

impl MemoryStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self {
            current: Arc::new(Mutex::new(None)),
        }
    }
}

impl DurableStore for MemoryStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        match self.current.try_lock() {
            Ok(current) => Ok(current.clone()),
            Err(TryLockError::WouldBlock) => Err(DurableError::Conflict {
                expected: None,
                current: Some("memory-store-writer-active".to_owned()),
            }),
            Err(TryLockError::Poisoned(error)) => Err(DurableError::Substrate(error.to_string())),
        }
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let mut stored = match self.current.try_lock() {
            Ok(stored) => stored,
            Err(TryLockError::WouldBlock) => {
                return Err(DurableError::Conflict {
                    expected: expected_revision.map(str::to_owned),
                    current: Some("memory-store-writer-active".to_owned()),
                });
            }
            Err(TryLockError::Poisoned(error)) => {
                return Err(DurableError::Substrate(error.to_string()));
            }
        };
        let current = stored.as_ref().map(|current| current.revision.clone());
        if expected_revision != current.as_deref() {
            return Err(DurableError::Conflict {
                expected: expected_revision.map(str::to_owned),
                current,
            });
        }
        let revision = next.revision()?;
        *stored = Some(StoredState {
            revision: revision.clone(),
            state: next.clone(),
        });
        Ok(StoreCommit { revision })
    }
}
