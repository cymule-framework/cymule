//! Public offline Machine-history maintenance and exact replay conformance.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use std::cell::Cell;
use std::collections::BTreeSet;
use std::rc::Rc;

use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand,
    DurableError, DurableResponse, DurableResult, DurableStore, DurableStoreControl, GcReceipt,
    HistoryCompactionKind, HistoryCompactionRequest, JournalRecordManifest, MemoryStore,
    StateRootManifest, StateRootObject, StateRootResolver, StoreBatch, StoreCommit, StoreHead,
    StoreReclamation, StoreStats, load_machine_command_archive,
};
use cymule_durable_protocol::WaitActivationSource;
use serde_json::json;

use support::{EmptyPlugin, empty_binding, execution, identity_candidate, open_control};

#[derive(Clone, Copy)]
enum CasFault {
    Before,
    After,
}

#[derive(Default)]
struct Observations {
    fault: Cell<Option<CasFault>>,
    cas_attempts: Cell<usize>,
    commits: Cell<usize>,
    advance_after_compaction: Cell<bool>,
    advance_after_gc: Cell<bool>,
    tamper_gc_ack: Cell<bool>,
    lose_pending: Cell<bool>,
    forbid_source: Cell<bool>,
    forbidden_reads: Cell<usize>,
}

#[derive(Clone)]
struct ObservedStore {
    inner: MemoryStore,
    observations: Rc<Observations>,
}

struct GuardedResolver<'a> {
    inner: &'a mut dyn StateRootResolver,
    denied_plan_root: Option<&'a str>,
    observations: &'a Observations,
}

impl StateRootResolver for GuardedResolver<'_> {
    fn pinned_manifest_id(&self) -> &str {
        self.inner.pinned_manifest_id()
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        if self.denied_plan_root == Some(object_id) {
            self.observations
                .forbidden_reads
                .set(self.observations.forbidden_reads.get() + 1);
            return Err(DurableError::RuntimeDefect {
                code: "compaction_replay_read_core_source".to_owned(),
                message: "receipt replay traversed the current Core material source".to_owned(),
            });
        }
        self.inner.load_state_root_object(object_id)
    }
}

impl DurableStore for ObservedStore {
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
        let observations = Rc::clone(&self.observations);
        let denied_plan_root = observations
            .forbid_source
            .get()
            .then_some(current.roots().machine_plans.node.as_deref())
            .flatten();
        self.inner.with_state_root_resolver(current, |resolver| {
            read(&mut GuardedResolver {
                inner: resolver,
                denied_plan_root,
                observations: &observations,
            })
        })
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

    fn compare_and_commit(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let is_compaction = !batch.machine_command_archive_segments().is_empty();
        let fault = if is_compaction {
            self.observations
                .cas_attempts
                .set(self.observations.cas_attempts.get() + 1);
            self.observations.fault.take()
        } else {
            None
        };
        if matches!(fault, Some(CasFault::Before)) {
            return Err(DurableError::Substrate {
                code: "compaction_test_before_cas".to_owned(),
                message: "injected failure before the compaction CAS".to_owned(),
            });
        }
        let committed = self.inner.compare_and_commit(expected, batch)?;
        if batch
            .state_root_transition()
            .manifest()
            .machine_frontier()
            .pending_commands
            .entries
            != 0
            && self.observations.lose_pending.replace(false)
        {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "injected acknowledgement loss after paged-command reservation".to_owned(),
            });
        }
        if is_compaction {
            self.observations
                .commits
                .set(self.observations.commits.get() + 1);
            if self.observations.advance_after_compaction.replace(false) {
                start_identity(self.inner.clone(), "run:history:intervening-writer");
            }
        }
        if matches!(fault, Some(CasFault::After)) {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "injected compaction acknowledgement loss".to_owned(),
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
        let mut receipt = self.inner.advance_cold_reclamation(request)?;
        if self.observations.advance_after_gc.replace(false) {
            start_identity(self.inner.clone(), "run:history:gc-intervening-writer");
        }
        if self.observations.tamper_gc_ack.replace(false) {
            receipt.gc_sequence += 1;
        }
        Ok(receipt)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.inner.stats()
    }
}

fn head(store: &MemoryStore) -> StoreHead {
    store
        .clone()
        .load_head()
        .expect("head reads")
        .expect("head exists")
}

fn start_identity(store: MemoryStore, run_id: &str) -> MemoryStore {
    let mut runtime = open_control(store, EmptyPlugin, empty_binding()).expect("runtime opens");
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: identity_candidate(run_id),
            input: json!({"run": run_id}),
            execution: execution(run_id),
        })
        .expect("Run completes");
    support::expect_completed_value(response);
    runtime.into_parts().0
}

fn request(store: &MemoryStore, id: &str, kind: HistoryCompactionKind) -> HistoryCompactionRequest {
    HistoryCompactionRequest {
        compaction_id: id.to_owned(),
        expected_revision: head(store).revision,
        kind,
        requested_suffix: 0,
    }
}

fn assert_cold_replay(store: &MemoryStore) {
    let mut reader = store.clone();
    let audited = reader
        .load_full_audit()
        .expect("full audit passes")
        .expect("state exists");
    let anchor = audited
        .head
        .machine_base_anchor
        .as_ref()
        .expect("compaction pins a base");
    let archive =
        load_machine_command_archive(anchor, |id| reader.load_machine_command_archive_segment(id))
            .expect("complete cold archive verifies");
    cymule_core::Machine::restore_with_archive(audited.state.machine, archive)
        .expect("cold history restores exact Machine authority")
        .verify_replay()
        .expect("restored Machine replays");
}

#[test]
fn start_run_replays_from_its_exact_cold_batch_without_clock_or_provider_work() {
    let run_id = "run:history:cold-start-replay";
    let store = start_identity(MemoryStore::new(), run_id);
    let intent = request(
        &store,
        "history:cold-start-replay",
        HistoryCompactionKind::EventPrefix,
    );
    let mut maintenance = DurableStoreControl::open(store).expect("maintenance opens");
    maintenance
        .compact_machine_history(&intent)
        .expect("StartRun history compacts");
    let store = maintenance.into_store();
    let committed = head(&store);
    let mut runtime = open_control(store.clone(), EmptyPlugin, empty_binding())
        .expect("cold StartRun replay runtime opens");
    let replay = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: identity_candidate(run_id),
            input: json!({"run": run_id}),
            execution: execution(run_id),
        })
        .expect("cold exact StartRun replays");
    assert_eq!(
        support::expect_completed_value(replay),
        json!({"run": run_id})
    );
    assert_eq!(head(&store), committed);
}

fn assert_receipt_metadata_is_strict(receipt: &cymule_durable::HistoryCompactionReceipt) {
    let mut malformed = receipt.clone();
    "sha256:not-a-content-id".clone_into(&mut malformed.result.base_id);
    assert!(malformed.verify().is_err());
    malformed.clone_from(receipt);
    "not-a-digest".clone_into(&mut malformed.result.projection_digest);
    assert!(malformed.verify().is_err());
    malformed.clone_from(receipt);
    malformed.result.causal_frontier = BTreeSet::from(["not-an-event-id".to_owned()]);
    assert!(malformed.verify().is_err());
    malformed.clone_from(receipt);
    malformed.requested_suffix = cymule_core::MAX_EXACT_INTEGER + 1;
    malformed.result.retained_events = malformed.requested_suffix;
    assert!(malformed.verify().is_err());
}

#[test]
fn event_prefix_compaction_replays_before_current_material_reads() {
    let store = start_identity(MemoryStore::new(), "run:history:prefix");
    let observations = Rc::new(Observations::default());
    let observed = ObservedStore {
        inner: store.clone(),
        observations: Rc::clone(&observations),
    };
    let intent = request(&store, "history:prefix", HistoryCompactionKind::EventPrefix);
    let before = head(&store);
    let receipt = DurableStoreControl::open(observed.clone())
        .expect("maintenance opens")
        .compact_machine_history(&intent)
        .expect("causal prefix compacts");
    receipt.verify().expect("receipt verifies");
    assert_receipt_metadata_is_strict(&receipt);
    assert_eq!(observations.commits.get(), 1);
    assert_eq!(head(&store).sequence, before.sequence + 1);
    assert_eq!(receipt.result.retained_events, 0);
    start_identity(store.clone(), "run:history:after-prefix");
    let current = head(&store);
    observations.forbid_source.set(true);
    let mut reopened = DurableStoreControl::open(observed).expect("maintenance reopens");
    assert_eq!(
        reopened
            .compact_machine_history(&intent)
            .expect("historical receipt replays"),
        receipt
    );
    let mut changed = intent.clone();
    changed.expected_revision.clone_from(&current.revision);
    assert!(matches!(
        reopened.compact_machine_history(&changed),
        Err(DurableError::HistoryConflict { .. })
    ));
    "history:stale".clone_into(&mut changed.compaction_id);
    changed.expected_revision = before.revision;
    assert!(matches!(
        reopened.compact_machine_history(&changed),
        Err(DurableError::Conflict { .. })
    ));
    assert_eq!(observations.cas_attempts.get(), 1);
    assert_eq!(observations.forbidden_reads.get(), 0);
    assert_eq!(head(&store), current);
    observations.forbid_source.set(false);
    assert_cold_replay(&store);
}

#[test]
fn acknowledged_compaction_survives_a_legal_intervening_writer() {
    let store = start_identity(MemoryStore::new(), "run:history:concurrent-source");
    let observations = Rc::new(Observations::default());
    observations.advance_after_compaction.set(true);
    let observed = ObservedStore {
        inner: store.clone(),
        observations: Rc::clone(&observations),
    };
    let intent = request(
        &store,
        "history:concurrent",
        HistoryCompactionKind::EventPrefix,
    );
    let before = head(&store);
    let receipt = DurableStoreControl::open(observed.clone())
        .expect("maintenance opens")
        .compact_machine_history(&intent)
        .expect("a later writer does not invalidate the acknowledged compaction receipt");
    assert_eq!(receipt.source_revision, intent.expected_revision);
    assert_eq!(observations.commits.get(), 1);
    let after = head(&store);
    assert!(after.sequence > before.sequence + 1);
    let later = DurableStoreControl::open(store.clone())
        .expect("later writer authority opens")
        .submit(DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:history:intervening-writer".to_owned(),
            expected_revision: Some(after.revision.clone()),
        })
        .expect("later writer remains visible");
    assert!(
        matches!(later, DurableResponse::RunCurrent { current: Some(current), .. }
        if current.continuation_status == cymule_durable_protocol::ContinuationStatus::Completed)
    );
    let replay = DurableStoreControl::open(observed)
        .expect("current authority reopens")
        .compact_machine_history(&intent)
        .expect("the same immutable receipt remains reachable after the later writer");
    assert_eq!(replay, receipt);
    assert_eq!(observations.cas_attempts.get(), 1);
    assert_eq!(head(&store), after);
    assert_cold_replay(&store);
}

#[test]
fn acknowledged_gc_survives_a_legal_intervening_writer() {
    let store = start_identity(MemoryStore::new(), "run:history:gc-source");
    let observations = Rc::new(Observations::default());
    observations.advance_after_gc.set(true);
    let observed = ObservedStore {
        inner: store.clone(),
        observations,
    };
    let before = head(&store);
    let receipt = DurableStoreControl::open(observed)
        .expect("GC authority opens")
        .advance_cold_reclamation()
        .expect("a later semantic writer does not invalidate an acknowledged GC receipt");
    assert_eq!(receipt.revision, before.revision);
    assert_eq!(receipt.parent_physical_token, before.physical_token);
    assert_eq!(receipt.gc_sequence, before.gc_sequence + 1);
    let after = head(&store);
    assert!(after.sequence > before.sequence);
    assert_ne!(after.revision, receipt.revision);
    let later = DurableStoreControl::open(store.clone())
        .expect("later authority opens")
        .submit(DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:history:gc-intervening-writer".to_owned(),
            expected_revision: Some(after.revision.clone()),
        })
        .expect("later writer state reads");
    assert!(
        matches!(later, DurableResponse::RunCurrent { current: Some(current), .. }
        if current.continuation_status == cymule_durable_protocol::ContinuationStatus::Completed)
    );
    assert_eq!(head(&store), after);
}

#[test]
fn malformed_gc_acknowledgement_remains_unknown_until_exact_reopen() {
    let store = start_identity(MemoryStore::new(), "run:history:gc-bad-ack");
    let observations = Rc::new(Observations::default());
    observations.tamper_gc_ack.set(true);
    let before = head(&store);
    let error = DurableStoreControl::open(ObservedStore {
        inner: store.clone(),
        observations,
    })
    .expect("GC authority opens")
    .advance_cold_reclamation()
    .expect_err("an uncorrelated post-publication acknowledgement cannot succeed");
    assert!(matches!(error, DurableError::CommitOutcomeUnknown { .. }));
    let committed = head(&store);
    assert_eq!(committed.gc_sequence, before.gc_sequence + 1);
    let recovered = DurableStoreControl::open(store.clone())
        .expect("current GC authority reopens")
        .reconcile_cold_reclamation()
        .expect("the exact retained receipt resolves uncertainty");
    recovered
        .verify_for(&committed)
        .expect("recovered receipt is the actual acknowledged head");
    assert_eq!(head(&store), committed);
}

#[test]
fn compaction_before_and_after_cas_loss_recover_one_exact_archive() {
    for (label, fault) in [("before", CasFault::Before), ("after", CasFault::After)] {
        let store = start_identity(MemoryStore::new(), &format!("run:history:{label}"));
        let observations = Rc::new(Observations::default());
        observations.fault.set(Some(fault));
        let observed = ObservedStore {
            inner: store.clone(),
            observations: Rc::clone(&observations),
        };
        let intent = request(
            &store,
            &format!("history:{label}"),
            HistoryCompactionKind::EventPrefix,
        );
        let before = head(&store);
        let error = DurableStoreControl::open(observed.clone())
            .expect("faulted maintenance opens")
            .compact_machine_history(&intent)
            .expect_err("selected CAS boundary fails");
        match fault {
            CasFault::Before => {
                assert!(matches!(error, DurableError::Substrate { .. }));
                assert_eq!(head(&store), before);
            }
            CasFault::After => {
                assert!(matches!(error, DurableError::CommitOutcomeUnknown { .. }));
                assert_ne!(head(&store), before);
            }
        }
        let receipt = DurableStoreControl::open(observed)
            .expect("recovery opens current authority")
            .compact_machine_history(&intent)
            .expect("retry resolves or commits its exact request");
        assert_eq!(receipt.source_revision, intent.expected_revision);
        assert_eq!(observations.commits.get(), 1);
        assert_eq!(head(&store).sequence, before.sequence + 1);
        assert_eq!(
            store
                .stats()
                .expect("archive stats read")
                .machine_command_archive_segments,
            1
        );
        assert_cold_replay(&store);
    }
}

fn parked_store(run_id: &str, signal: &str) -> (MemoryStore, String) {
    let mut runtime =
        open_control(MemoryStore::new(), EmptyPlugin, empty_binding()).expect("runtime opens");
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: support::signal_candidate(run_id, signal, true),
            input: json!({"run": run_id}),
            execution: execution(run_id),
        })
        .expect("Run parks");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("signal Run did not park")
    };
    (runtime.into_parts().0, wait_id)
}

#[test]
fn event_free_material_admission_compacts_and_wait_resumes() {
    let run_id = "run:history:material-only";
    let signal = "signal:history:material-only";
    let (store, wait_id) = parked_store(run_id, signal);
    let prefix = request(
        &store,
        "history:material-prefix",
        HistoryCompactionKind::EventPrefix,
    );
    let mut control = DurableStoreControl::open(store.clone()).expect("maintenance opens");
    let first = control
        .compact_machine_history(&prefix)
        .expect("waiting history compacts");
    let first_anchor = head(&store)
        .machine_base_anchor
        .expect("first archive anchor exists");
    control
        .submit(DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:history:material-only".to_owned(),
            source: WaitActivationSource::Signal {
                key: signal.to_owned(),
            },
            wait_ids: BTreeSet::from([wait_id]),
            value: json!({"ready": true}),
        })
        .expect("activation admits only immutable result material and sidecars");
    let tail = request(
        &store,
        "history:material-tail",
        HistoryCompactionKind::EventFreeAdmissions,
    );
    let second = control
        .compact_machine_history(&tail)
        .expect("zero-command material batch compacts");
    assert_eq!(
        second.parent_compaction.as_deref(),
        Some(first.compaction_id.as_str())
    );
    assert_eq!(second.result.archive_segment.event_count, 0);
    assert_eq!(second.result.archive_segment.batch_count, 1);
    assert_eq!(
        head(&store)
            .machine_base_anchor
            .expect("second anchor exists")
            .archive_batch_count,
        first_anchor.archive_batch_count + 1
    );
    assert_gc_keeps_material_batches(&store, &mut control, &second);
    let mut runtime = open_control(control.into_store(), EmptyPlugin, empty_binding())
        .expect("compacted runtime reopens");
    let value = support::expect_completed_value(
        runtime
            .submit(DurableCommand::ResumeRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                execution: execution(run_id),
            })
            .expect("ready Run resumes after both compactions"),
    );
    assert_eq!(value, json!({"run": run_id}));
    assert_cold_replay(&runtime.into_parts().0);
}

fn assert_gc_keeps_material_batches(
    store: &MemoryStore,
    control: &mut DurableStoreControl<MemoryStore>,
    receipt: &cymule_durable::HistoryCompactionReceipt,
) {
    let mut reader = store.clone();
    let segment = reader
        .load_machine_command_archive_segment(&receipt.result.archive_segment.segment_id)
        .expect("material-only archive reads")
        .expect("material-only archive exists");
    assert_eq!(segment.batches.len(), 1);
    assert!(segment.batches[0].members.is_empty());
    for batch in &segment.batches {
        assert_eq!(
            reader
                .load_machine_command_archive_batch(&batch.batch_id)
                .expect("independent batch reads before GC"),
            Some(batch.clone())
        );
    }
    let before = head(store);
    let gc = control
        .advance_cold_reclamation()
        .expect("physical GC completes");
    assert!(gc.reclaimed_objects > 0);
    assert_eq!(gc.remaining_objects, 0);
    let after = head(store);
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.physical_token, gc.result_physical_token);
    for batch in segment.batches {
        let retained = reader
            .load_machine_command_archive_batch(&batch.batch_id)
            .expect("independent material-only batch reads after GC")
            .expect("GC must retain a reachable zero-command batch independently");
        assert_eq!(retained, batch);
    }
}

#[test]
fn cancellation_receipt_replays_through_the_compacted_core_batch() {
    let (store, _) = parked_store("run:history:cancel", "signal:history:cancel");
    let cancel = DurableCommand::CancelRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        cancellation_id: "cancel:history".to_owned(),
        run_id: "run:history:cancel".to_owned(),
        reason: json!("operator cancelled"),
    };
    let mut control = DurableStoreControl::open(store.clone()).expect("cancellation opens");
    let original = control.submit(cancel.clone()).expect("Run cancels");
    let intent = request(&store, "history:cancel", HistoryCompactionKind::EventPrefix);
    control
        .compact_machine_history(&intent)
        .expect("terminal Core history compacts");
    let replay = DurableStoreControl::open(control.into_store())
        .expect("readback reopens")
        .submit(cancel)
        .expect("typed cancellation finds its exact archived Core batch");
    assert_eq!(replay, original);
    assert_cold_replay(&store);
}

#[test]
fn pending_command_reservation_blocks_compaction_before_offline_source_reads() {
    let (store, _) = parked_store("run:history:pending", "signal:history:pending");
    let observations = Rc::new(Observations::default());
    observations.lose_pending.set(true);
    let observed = ObservedStore {
        inner: store.clone(),
        observations: Rc::clone(&observations),
    };
    let cancel = DurableCommand::CancelRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        cancellation_id: "cancel:history:pending".to_owned(),
        run_id: "run:history:pending".to_owned(),
        reason: json!("finish the retained cancellation"),
    };
    let error = DurableStoreControl::open(observed.clone())
        .expect("cancellation opens")
        .submit(cancel.clone())
        .expect_err("pending reservation acknowledgement is lost");
    assert!(matches!(error, DurableError::CommitOutcomeUnknown { .. }));
    let before = head(&store);
    observations.forbid_source.set(true);
    let intent = request(
        &store,
        "history:pending",
        HistoryCompactionKind::EventPrefix,
    );
    let error = DurableStoreControl::open(observed.clone())
        .expect("maintenance opens")
        .compact_machine_history(&intent)
        .expect_err("the frozen paged source cannot be compacted");
    assert!(matches!(error, DurableError::HistoryConflict { code, .. }
        if code == "state_root_machine_compaction_pending_transition"));
    assert_eq!(observations.forbidden_reads.get(), 0);
    assert_eq!(observations.cas_attempts.get(), 0);
    assert_eq!(head(&store), before);
    observations.forbid_source.set(false);
    DurableStoreControl::open(observed)
        .expect("cancellation recovery opens")
        .submit(cancel)
        .expect("the original frozen command finishes");
    let intent = request(
        &store,
        "history:after-pending",
        HistoryCompactionKind::EventPrefix,
    );
    DurableStoreControl::open(store.clone())
        .expect("finished source opens")
        .compact_machine_history(&intent)
        .expect("finished history now compacts");
    assert_cold_replay(&store);
}

#[test]
fn event_free_kind_rejects_the_removed_conflict_only_name() {
    let kind = HistoryCompactionKind::EventFreeAdmissions;
    assert_eq!(
        serde_json::to_value(kind).expect("kind encodes"),
        json!("event_free_admissions")
    );
    assert_eq!(
        serde_json::from_value::<HistoryCompactionKind>(json!("event_free_admissions"))
            .expect("canonical kind decodes"),
        kind
    );
    assert!(serde_json::from_value::<HistoryCompactionKind>(json!("conflict_admissions")).is_err());
}
