//! Faults at actual persisted terminal-command page boundaries.

use std::cell::RefCell;
use std::rc::Rc;

use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DurableError, DurableResult, DurableStore, GcReceipt,
    JournalRecordManifest, MemoryStore, StateRootManifest, StateRootResolver, StoreBatch,
    StoreCommit, StoreHead, StoreReclamation, StoreStats,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageStage {
    Begin,
    Progress,
    Finalize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FaultMoment {
    Before,
    After,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PageFault {
    pub ordinal: usize,
    pub moment: FaultMoment,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PageTrace {
    pub cas_attempts: usize,
    pub attempts: Vec<PageStage>,
    pub commits: Vec<PageStage>,
    pub fault_hits: usize,
}

#[derive(Default)]
struct FaultState {
    armed: bool,
    fault: Option<PageFault>,
    trace: PageTrace,
}

#[derive(Clone)]
pub(super) struct PagedStore {
    inner: MemoryStore,
    state: Rc<RefCell<FaultState>>,
}

impl PagedStore {
    pub(super) fn new(inner: MemoryStore) -> Self {
        Self {
            inner,
            state: Rc::new(RefCell::new(FaultState::default())),
        }
    }

    pub(super) fn arm(&self, fault: Option<PageFault>) {
        *self.state.borrow_mut() = FaultState {
            armed: true,
            fault,
            trace: PageTrace::default(),
        };
    }

    pub(super) fn trace(&self) -> PageTrace {
        self.state.borrow().trace.clone()
    }

    fn stage(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<Option<PageStage>> {
        let before = match expected {
            Some(head) => {
                self.inner
                    .load_state_root_manifest(&head.state_root_manifest_id)?
                    .ok_or_else(|| DurableError::Integrity {
                        code: "paged_test_source_manifest_missing".to_owned(),
                        message: "fault selection lost its exact source manifest".to_owned(),
                    })?
                    .machine_frontier()
                    .pending_commands
                    .entries
            }
            None => 0,
        };
        let after = batch
            .state_root_transition()
            .manifest()
            .machine_frontier()
            .pending_commands
            .entries;
        Ok(match (before, after) {
            (0, 1) => Some(PageStage::Begin),
            (1, 1) => Some(PageStage::Progress),
            (1, 0) => Some(PageStage::Finalize),
            (0, 0) => None,
            _ => {
                return Err(DurableError::Validation(
                    "single-Run fixture admitted multiple pending terminal commands".to_owned(),
                ));
            }
        })
    }
}

impl DurableStore for PagedStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        self.inner.load_head()
    }

    fn load_state_root_manifest(&mut self, id: &str) -> DurableResult<Option<StateRootManifest>> {
        self.inner.load_state_root_manifest(id)
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
        id: &str,
        count: u64,
    ) -> DurableResult<ApplicationJournalPrefix> {
        self.inner.application_journal_prefix(manifest, id, count)
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
        id: &str,
    ) -> DurableResult<Option<ApplicationJournalPrefixReplacementAuthority>> {
        self.inner
            .application_journal_prefix_replacement_authority(manifest, id)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &StateRootManifest,
        id: &str,
    ) -> DurableResult<Option<CoupledCheckpointReceipt>> {
        self.inner.coupled_checkpoint_receipt(manifest, id)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        self.inner.load_machine_command_archive_segment(id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        self.inner.load_machine_command_archive_entry(id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        self.inner.load_machine_command_archive_batch(id)
    }

    fn load_machine_command_index_node(
        &mut self,
        id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        self.inner.load_machine_command_index_node(id)
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
        if !self.state.borrow().armed {
            return self.inner.compare_and_commit(expected, batch);
        }
        // Count even a non-paged write prepared against an obsolete source.
        self.state.borrow_mut().trace.cas_attempts += 1;
        let Some(stage) = self.stage(expected, batch)? else {
            return self.inner.compare_and_commit(expected, batch);
        };
        let fault = {
            let mut recorder = self.state.borrow_mut();
            let ordinal = recorder.trace.attempts.len();
            recorder.trace.attempts.push(stage);
            if recorder.fault.is_some_and(|fault| fault.ordinal == ordinal) {
                recorder.trace.fault_hits += 1;
                recorder.fault.take()
            } else {
                None
            }
        };
        if fault.is_some_and(|fault| fault.moment == FaultMoment::Before) {
            return Err(DurableError::Substrate {
                code: "injected_paged_pre_cas".to_owned(),
                message: format!("injected failure before {stage:?} CAS"),
            });
        }
        let committed = self.inner.compare_and_commit(expected, batch)?;
        self.state.borrow_mut().trace.commits.push(stage);
        if fault.is_some_and(|fault| fault.moment == FaultMoment::After) {
            return Err(DurableError::CommitOutcomeUnknown {
                message: format!("injected acknowledgement loss after {stage:?} CAS"),
            });
        }
        Ok(committed)
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(&mut self, request: &StoreReclamation) -> DurableResult<GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.inner.stats()
    }
}
