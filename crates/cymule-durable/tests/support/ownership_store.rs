//! Receipt loss selected from public, revision-pinned Attempt queries.

use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableCommand, DurableError,
    DurableResponse, DurableResult, DurableStore, DurableStoreControl, GcReceipt,
    JournalRecordManifest, MAX_DURABLE_QUERY_PAGE_BYTES, MemoryStore, OperationAttemptState,
    StateRootManifest, StateRootResolver, StoreBatch, StoreCommit, StoreHead, StoreReclamation,
    StoreStats,
};

/// Lose the first receipt whose committed first Attempt has the selected state.
pub(super) struct ReceiptLossStore {
    inner: MemoryStore,
    run_id: String,
    boundary: Option<OperationAttemptState>,
}

impl ReceiptLossStore {
    pub(super) fn new(inner: MemoryStore, run_id: &str, boundary: OperationAttemptState) -> Self {
        Self {
            inner,
            run_id: run_id.to_owned(),
            boundary: Some(boundary),
        }
    }
}

impl DurableStore for ReceiptLossStore {
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
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        self.inner.load_machine_command_archive_segment(segment_id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        self.inner.load_machine_command_archive_entry(entry_id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        self.inner.load_machine_command_archive_batch(batch_id)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        self.inner.load_machine_command_index_node(node_id)
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let receipt = self.inner.compare_and_commit(expected, batch)?;
        let Some(boundary) = self.boundary else {
            return Ok(receipt);
        };
        if batch
            .state_root_transition()
            .manifest()
            .roots()
            .operation_attempts
            .entries
            == 0
        {
            return Ok(receipt);
        }
        let query = DurableCommand::RunAttemptPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: self.run_id.clone(),
            expected_revision: Some(receipt.revision.clone()),
            cursor: None,
            limit: 2,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        };
        let response = DurableStoreControl::open(self.inner.clone())?.submit(query.clone())?;
        response.verify_query_for(&query)?;
        let DurableResponse::RunAttemptPage { page, .. } = response else {
            panic!("Attempt query returned another response")
        };
        if page
            .items
            .iter()
            .any(|attempt| attempt.attempt_ordinal == 1 && attempt.state == boundary)
        {
            self.boundary = None;
            return Err(DurableError::CommitOutcomeUnknown {
                message: format!("injected receipt loss after first Attempt became {boundary:?}"),
            });
        }
        Ok(receipt)
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
