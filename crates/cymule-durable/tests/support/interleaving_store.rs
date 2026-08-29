//! Deterministic unrelated-writer interleaving after one acknowledged CAS.

use cymule_core::{Expression, Operation, Step, WaitSpec};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand,
    DurableResponse, DurableResult, DurableStore, DurableStoreControl, GcReceipt,
    JournalRecordManifest, MemoryStore, StateRootManifest, StateRootResolver, StoreBatch,
    StoreCommit, StoreHead, StoreReclamation, StoreStats,
};
use cymule_durable_protocol::WaitActivationSource;
use serde_json::json;

use super::support;

/// Advance the shared Store through another handle after a selected successful
/// semantic CAS, before returning that CAS's exact acknowledgement.
pub(super) struct InterleavingStore {
    inner: MemoryStore,
    commits_before_interleaving: usize,
    interleave: Option<Box<dyn FnOnce(MemoryStore) -> DurableResult<()>>>,
}

impl InterleavingStore {
    pub(super) fn new(
        inner: MemoryStore,
        commits_before_interleaving: usize,
        interleave: impl FnOnce(MemoryStore) -> DurableResult<()> + 'static,
    ) -> Self {
        Self {
            inner,
            commits_before_interleaving,
            interleave: Some(Box::new(interleave)),
        }
    }

    pub(super) fn into_inner(self) -> MemoryStore {
        self.inner
    }
}

pub(super) fn park_unrelated_signal(
    store: MemoryStore,
    run_id: &str,
    signal_key: &str,
) -> DurableResult<(MemoryStore, String)> {
    let mut candidate = support::identity_candidate(run_id);
    candidate.definitions[0].body.steps.insert(
        0,
        Step {
            id: "wait.unrelated-writer".to_owned(),
            operation: Operation::Wait {
                wait: WaitSpec::Signal {
                    key: signal_key.to_owned(),
                    consume_once: true,
                },
                bind: None,
            },
        },
    );
    candidate.definitions[0].body.result = Expression::Input;
    let mut runtime = support::open_control(store, support::EmptyPlugin, support::empty_binding())?;
    let response = runtime.submit(DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        candidate,
        input: json!({"unrelated": run_id}),
        execution: support::execution(run_id),
    })?;
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("unrelated writer Run did not park")
    };
    Ok((runtime.into_parts().0, wait_id))
}

pub(super) fn activate_unrelated_signal(
    store: MemoryStore,
    activation_id: &str,
    signal_key: &str,
    wait_id: String,
) -> DurableResult<MemoryStore> {
    let mut control = DurableStoreControl::open(store)?;
    control.submit(DurableCommand::ActivateWait {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        activation_id: activation_id.to_owned(),
        source: WaitActivationSource::Signal {
            key: signal_key.to_owned(),
        },
        wait_ids: std::collections::BTreeSet::from([wait_id]),
        value: json!({"unrelated": "advanced"}),
    })?;
    Ok(control.into_store())
}

impl DurableStore for InterleavingStore {
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
        let commit = self.inner.compare_and_commit(expected, batch)?;
        if self.commits_before_interleaving == 0 {
            if let Some(interleave) = self.interleave.take() {
                interleave(self.inner.clone())?;
            }
        } else {
            self.commits_before_interleaving -= 1;
        }
        Ok(commit)
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
