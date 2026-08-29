//! Public Resource handoff admission, atomicity, and pinned replay conformance.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use cymule_core::{
    ComponentContract, Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    ClockObservationAuthority, ComponentOccurrence, ComponentOutcome, CoupledCheckpointReceipt,
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableError, DurableResponse,
    DurableResult, DurableRunCurrent, DurableRunItem, DurableRunItemSelector,
    DurableRuntimeControl, DurableStore, DurableStoreControl, ExecutionClockAuthority, GcReceipt,
    JournalRecordManifest, MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES, MAX_DURABLE_QUERY_PAGE_BYTES,
    MAX_DURABLE_QUERY_PAGE_ITEMS, MemoryStore, StateRootManifest, StateRootResolver, StoreBatch,
    StoreCommit, StoreHead, StoreReclamation, StoreStats, StoredState, WaitCondition, WaitState,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, ClockObservationRef, ContinuationStatus,
    ExecutionClaimRequest, clock_observation_id, execution_clock_scope,
};
use cymule_profile_protocol::resource::{
    RESOURCE_HANDOFF_VERSION, ResourceCandidate, ResourceCommand, ResourceCommandOutcome,
    ResourceCommandReceipt, ResourceHandoff, ResourceHandoffActivation, ResourceOperation,
    ResourceProducerProvenance, resource_handle_artifact_kind, resource_handle_artifact_schema,
};
use cymule_runtime::{
    ExecutionBinding, ExecutionBindingAdmission, PLUGIN_VERSION, PluginHost, PluginManifest,
    PluginOperation, PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

const PRODUCER: &str = "run:resource-pinned-producer";
const CONSUMER: &str = "run:resource-pinned-consumer";
const SLOT: &str = "input.resource";
const BEFORE_CAS: u8 = 1;
const AFTER_CAS: u8 = 2;

#[derive(Clone)]
struct ProbeStore {
    inner: MemoryStore,
    fault: Arc<AtomicU8>,
}

impl DurableStore for ProbeStore {
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
        manifest: &StateRootManifest,
        read: impl FnOnce(&mut dyn StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        self.inner.with_state_root_resolver(manifest, read)
    }

    fn load_full_audit(&mut self) -> DurableResult<Option<StoredState>> {
        panic!("ordinary Resource control must not materialize the complete domain")
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
        let fault = self.fault.swap(0, Ordering::SeqCst);
        if fault == BEFORE_CAS {
            return Err(DurableError::Substrate {
                code: "resource_test_before_cas".to_owned(),
                message: "injected failure before Resource head publication".to_owned(),
            });
        }
        let commit = self.inner.compare_and_commit(expected, batch)?;
        if fault == AFTER_CAS {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "injected Resource acknowledgement loss after head publication".to_owned(),
            });
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

struct ExactClock(ClockObservation);

impl ClockObservationAuthority for ExactClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        if self.0.reference() != *reference {
            return Err(DurableError::NotFound(
                "unissued Resource test Clock receipt".to_owned(),
            ));
        }
        Ok(self.0.clone())
    }
}

impl ExecutionClockAuthority for ExactClock {
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        let observation = self.resolve(reference)?;
        commit(&observation)
    }
}

fn execution(run_id: &str, time: u64) -> (ExactClock, ExecutionClaimRequest) {
    let source = "clock:resource-handoff-test";
    let generation = format!("sha256:{}", "a".repeat(64));
    let scope = execution_clock_scope(run_id).expect("Clock scope derives");
    let observation = ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id: clock_observation_id(source, &generation, &scope, time, time)
            .expect("Clock observation identity derives"),
        source_id: source.to_owned(),
        source_generation: generation,
        scope,
        logical_time: time,
        observed_unix_ms: time,
    };
    let request = ExecutionClaimRequest {
        owner: "worker:resource-handoff-test".to_owned(),
        clock: observation.reference(),
        ttl: 1,
    };
    (ExactClock(observation), request)
}

#[derive(Clone)]
struct Producer(Arc<AtomicUsize>);

fn plugin_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "resource-handoff-test".to_owned(),
        components: BTreeMap::from([(
            "producer".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    }
}

impl PluginHost for Producer {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: plugin_manifest(),
            }),
            PluginRequest::Call { component, .. } if component == "producer" => {
                self.0.fetch_add(1, Ordering::SeqCst);
                let handle = ResourceCandidate::text("immutable producer output")
                    .seal()
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                Ok(PluginResponse::CallResult {
                    value: serde_json::to_value(handle)
                        .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?,
                })
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected request {other:?}"
            ))),
        }
    }
}

fn runtime(
    store: ProbeStore,
    calls: Arc<AtomicUsize>,
    clock: ExactClock,
) -> DurableRuntimeControl<ProbeStore, Producer> {
    let binding = ExecutionBinding::for_local_process(
        &plugin_manifest(),
        format!("sha256:{}", "b".repeat(64)),
    )
    .expect("producer binding derives");
    let admission = ExecutionBindingAdmission::admit(Producer(calls), binding)
        .expect("producer binding admits");
    DurableRuntimeControl::open(store, admission, clock).expect("pinned runtime opens")
}

fn plan(producer: bool, typed: bool, wait_schema: Value) -> PlanCandidate {
    let (components, operation, result) = if producer {
        (
            vec![ComponentContract {
                id: "producer".to_owned(),
                input_schema: json!({}),
                output_schema: resource_handle_artifact_schema(),
                output_artifact_kind: if typed {
                    resource_handle_artifact_kind().expect("Resource Handle kind derives")
                } else {
                    cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned()
                },
                requirements: BTreeMap::new(),
            }],
            Operation::Call {
                component: "producer".to_owned(),
                input: Expression::Input,
                bind: Some("output".to_owned()),
            },
            Expression::Binding {
                name: "output".to_owned(),
            },
        )
    } else {
        (
            Vec::new(),
            Operation::Wait {
                wait: WaitSpec::Input {
                    correlation: SLOT.to_owned(),
                    schema: wait_schema,
                },
                bind: Some("resource".to_owned()),
            },
            Expression::Binding {
                name: "resource".to_owned(),
            },
        )
    };
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: if producer {
            "resource_producer"
        } else {
            "resource_consumer"
        }
        .to_owned(),
        entry: "main".to_owned(),
        components,
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "site".to_owned(),
                    operation,
                }],
                result,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn checked_query(
    control: &mut DurableStoreControl<ProbeStore>,
    command: &DurableCommand,
) -> DurableResponse {
    let response = control
        .submit(command.clone())
        .expect("exact public query succeeds");
    response
        .verify_query_for(command)
        .expect("exact public query response verifies");
    response
}

fn item(
    control: &mut DurableStoreControl<ProbeStore>,
    run_id: &str,
    selector: DurableRunItemSelector,
) -> DurableRunItem {
    let response = checked_query(
        control,
        &DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
            selector,
            max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
        },
    );
    let DurableResponse::RunItem {
        item: Some(item), ..
    } = response
    else {
        panic!("exact query did not return an item: {response:?}");
    };
    *item
}

fn current(control: &mut DurableStoreControl<ProbeStore>, run_id: &str) -> DurableRunCurrent {
    let response = checked_query(
        control,
        &DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
        },
    );
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = response
    else {
        panic!("exact query did not return Run current: {response:?}");
    };
    *current
}

struct Fixture {
    store: ProbeStore,
    calls: Arc<AtomicUsize>,
    handoff: ResourceHandoff,
    wait_id: String,
}

impl Fixture {
    fn new(typed: bool, wait_schema: Value) -> Self {
        let store = ProbeStore {
            inner: MemoryStore::new(),
            fault: Arc::new(AtomicU8::new(0)),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let (clock, claim) = execution(PRODUCER, 1);
        let mut producer = runtime(store.clone(), calls.clone(), clock);
        let outcome = producer
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: PRODUCER.to_owned(),
                candidate: plan(true, typed, json!({})),
                input: json!({}),
                execution: claim,
            })
            .expect("producer completes");
        assert!(matches!(
            outcome,
            DurableResponse::RunBoundary {
                boundary: DurableBoundary::Completed { .. }
            }
        ));
        drop(producer);
        let mut control =
            DurableStoreControl::open(store.clone()).expect("producer pinned state reopens");
        let response = checked_query(
            &mut control,
            &DurableCommand::RunOccurrencePage {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: PRODUCER.to_owned(),
                expected_revision: None,
                cursor: None,
                limit: MAX_DURABLE_QUERY_PAGE_ITEMS,
                max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
            },
        );
        let DurableResponse::RunOccurrencePage { page, .. } = response else {
            panic!("producer occurrence page is absent");
        };
        assert_eq!(page.items.len(), 1);
        let occurrence_id = page.items[0].occurrence_id.clone();
        let DurableRunItem::Occurrence { occurrence } = item(
            &mut control,
            PRODUCER,
            DurableRunItemSelector::Occurrence {
                occurrence_id: occurrence_id.clone(),
            },
        ) else {
            panic!("producer occurrence has another item kind");
        };
        let ComponentOccurrence {
            outcome: Some(ComponentOutcome::Succeeded { output }),
            ..
        } = *occurrence
        else {
            panic!("producer occurrence did not complete successfully");
        };
        let (clock, claim) = execution(CONSUMER, 1);
        let mut consumer = runtime(store.clone(), calls.clone(), clock);
        let outcome = consumer
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: CONSUMER.to_owned(),
                candidate: plan(false, true, wait_schema),
                input: json!({}),
                execution: claim,
            })
            .expect("consumer parks");
        let DurableResponse::RunBoundary {
            boundary: DurableBoundary::Suspended { wait_id },
        } = outcome
        else {
            panic!("consumer did not park: {outcome:?}");
        };
        Self {
            store,
            calls,
            wait_id,
            handoff: ResourceHandoff {
                handoff_version: RESOURCE_HANDOFF_VERSION.to_owned(),
                transfer_id: "transfer:resource-pinned".to_owned(),
                producer: ResourceProducerProvenance {
                    run_id: PRODUCER.to_owned(),
                    occurrence_id,
                    result: output.clone(),
                },
                to_run: CONSUMER.to_owned(),
                slot: SLOT.to_owned(),
                resource: output,
            },
        }
    }

    fn control(&self) -> DurableStoreControl<ProbeStore> {
        DurableStoreControl::open(self.store.clone()).expect("pinned control reopens")
    }

    fn head(&self) -> StoreHead {
        self.store
            .clone()
            .load_head()
            .expect("head loads")
            .expect("domain is initialized")
    }

    fn transfer(&self) -> ResourceCommand {
        ResourceCommand::new(ResourceOperation::Transfer {
            handoff: self.handoff.clone(),
        })
        .expect("transfer command derives")
    }

    fn activation(&self, transfer: &ResourceCommandReceipt) -> ResourceCommand {
        let ResourceCommandOutcome::Transfer { receipt } = &transfer.outcome else {
            panic!("transfer command has another outcome");
        };
        ResourceCommand::new(ResourceOperation::ActivateTransfer {
            activation: ResourceHandoffActivation::new(&self.handoff, &self.wait_id)
                .expect("activation derives"),
            source_receipt_id: receipt.receipt_id.clone(),
        })
        .expect("activation command derives")
    }

    fn wait(&self, control: &mut DurableStoreControl<ProbeStore>) -> WaitCondition {
        let DurableRunItem::Wait { wait } = item(
            control,
            CONSUMER,
            DurableRunItemSelector::Wait {
                wait_id: self.wait_id.clone(),
            },
        ) else {
            panic!("input Wait has another item kind");
        };
        *wait
    }

    fn audit(&self) {
        // Explicit offline integrity verification is distinct from the guarded
        // public runtime Store, whose full-audit entry point always panics.
        self.store
            .inner
            .clone()
            .load_full_audit()
            .expect("offline integrity audit passes")
            .expect("audited domain exists")
            .verify()
            .expect("audited authority verifies");
    }
}

#[test]
fn pinned_handoff_replays_after_reopen_and_target_completion() {
    let fixture = Fixture::new(true, json!({}));
    let mut control = fixture.control();
    let mut stale = fixture.control();
    let before = fixture.head();
    let transfer = control
        .resource()
        .commit(&fixture.transfer())
        .expect("transfer commits");
    assert_eq!(fixture.head().sequence, before.sequence + 1);
    assert!(matches!(
        stale.resource().commit(&fixture.transfer()),
        Err(DurableError::Conflict { .. })
    ));
    let mut control = fixture.control();
    let retained_head = fixture.head();
    assert_eq!(
        control
            .resource()
            .commit(&fixture.transfer())
            .expect("transfer replays"),
        transfer
    );
    assert_eq!(fixture.head(), retained_head);
    let activation_command = fixture.activation(&transfer);
    let activation = control
        .resource()
        .commit(&activation_command)
        .expect("activation commits");
    assert_eq!(fixture.head().sequence, retained_head.sequence + 1);
    assert_eq!(
        current(&mut control, CONSUMER).continuation_status,
        ContinuationStatus::Ready
    );
    assert_eq!(
        fixture.wait(&mut control).result,
        Some(fixture.handoff.resource.clone())
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    let stable_head = fixture.head();
    assert_eq!(
        fixture
            .control()
            .resource()
            .commit(&activation_command)
            .expect("activation replays after reopen"),
        activation
    );
    assert_eq!(fixture.head(), stable_head);

    let (clock, claim) = execution(CONSUMER, 2);
    let mut consumer = runtime(fixture.store.clone(), fixture.calls.clone(), clock);
    let completed = consumer
        .submit(DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: CONSUMER.to_owned(),
            execution: claim,
        })
        .expect("activated consumer completes");
    assert!(matches!(
        completed,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    let terminal_head = fixture.head();
    let mut control = fixture.control();
    assert_eq!(
        control
            .resource()
            .commit(&activation_command)
            .expect("historical activation survives target completion"),
        activation
    );
    assert_eq!(
        control
            .resource()
            .commit(&fixture.transfer())
            .expect("historical transfer survives target completion"),
        transfer
    );
    assert_eq!(fixture.head(), terminal_head);
    assert_terminal_target_rejects_fresh_transfer(&fixture, &mut control);
    assert_eq!(fixture.head(), terminal_head);
    assert_handoff_indexes(&fixture, &mut control, &activation);
    fixture.audit();
}

fn assert_terminal_target_rejects_fresh_transfer(
    fixture: &Fixture,
    control: &mut DurableStoreControl<ProbeStore>,
) {
    let handoff = ResourceHandoff {
        transfer_id: "transfer:terminal-target".to_owned(),
        slot: "input.after-completion".to_owned(),
        ..fixture.handoff.clone()
    };
    let rejected = control.resource().commit(
        &ResourceCommand::new(ResourceOperation::Transfer { handoff })
            .expect("fresh terminal-target command derives"),
    );
    assert!(matches!(rejected, Err(DurableError::IllegalTransition(_))));
}

fn leave_receiver_running_before_its_wait(fixture: &Fixture, run_id: &str) {
    let (clock, execution) = execution(run_id, 1);
    let mut receiver = runtime(fixture.store.clone(), fixture.calls.clone(), clock);
    fixture.store.fault.store(AFTER_CAS, Ordering::SeqCst);
    assert!(matches!(
        receiver.submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: plan(false, true, json!({})),
            input: json!({}),
            execution,
        }),
        Err(DurableError::CommitOutcomeUnknown { .. })
    ));
    assert_eq!(
        current(&mut fixture.control(), run_id).continuation_status,
        ContinuationStatus::Running,
    );
}

fn recover_receiver_to_its_wait(fixture: &Fixture, run_id: &str) -> String {
    let (clock, execution) = execution(run_id, 3);
    let mut receiver = runtime(fixture.store.clone(), fixture.calls.clone(), clock);
    let response = receiver
        .submit(DurableCommand::TakeoverRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_fence: 1,
            execution,
        })
        .expect("expired initial ownership is taken over explicitly");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("recovered receiver did not reach its input Wait: {response:?}");
    };
    wait_id
}

#[test]
fn transfer_can_precede_the_wait_of_a_running_active_target() {
    let fixture = Fixture::new(true, json!({}));
    let run_id = "run:resource-receive-later";
    leave_receiver_running_before_its_wait(&fixture, run_id);
    let before = fixture.head();
    let mut handoff = fixture.handoff.clone();
    handoff.transfer_id = "transfer:before-target-wait".to_owned();
    handoff.to_run = run_id.to_owned();
    let command = ResourceCommand::new(ResourceOperation::Transfer {
        handoff: handoff.clone(),
    })
    .expect("early transfer command derives");
    let transferred = fixture
        .control()
        .resource()
        .commit(&command)
        .expect("transfer publishes only the future slot while the target is Running");
    assert_eq!(fixture.head().sequence, before.sequence + 1);
    assert_eq!(
        current(&mut fixture.control(), run_id).continuation_status,
        ContinuationStatus::Running
    );
    let ResourceCommandOutcome::Transfer { receipt } = transferred.outcome else {
        panic!("transfer returned another outcome");
    };
    let wait_id = recover_receiver_to_its_wait(&fixture, run_id);
    let activation = ResourceCommand::new(ResourceOperation::ActivateTransfer {
        activation: ResourceHandoffActivation::new(&handoff, wait_id).expect("activation derives"),
        source_receipt_id: receipt.receipt_id,
    })
    .expect("activation command derives");
    let activated = fixture
        .control()
        .resource()
        .commit(&activation)
        .expect("the published transfer activates only after the exact Wait exists");
    assert_eq!(
        current(&mut fixture.control(), run_id).continuation_status,
        ContinuationStatus::Ready
    );
    let after = fixture.head();
    assert_eq!(
        fixture
            .control()
            .resource()
            .commit(&activation)
            .expect("activation replays after reopening"),
        activated
    );
    assert_eq!(fixture.head(), after);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    fixture.audit();
}

#[test]
fn pinned_transfer_rejects_foreign_provenance_slot_reuse_and_untyped_output() {
    let fixture = Fixture::new(true, json!({}));
    let other_run = "run:resource-pinned-other-consumer";
    let (clock, execution) = execution(other_run, 1);
    let mut other = runtime(fixture.store.clone(), fixture.calls.clone(), clock);
    let parked = other
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: other_run.to_owned(),
            candidate: plan(false, true, json!({})),
            input: json!({}),
            execution,
        })
        .expect("independent target Run parks");
    assert!(matches!(
        parked,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Suspended { .. }
        }
    ));
    let before = fixture.head();
    for producer_run in [other_run, "run:missing-producer"] {
        let mut handoff = fixture.handoff.clone();
        handoff.producer.run_id = producer_run.to_owned();
        let command = ResourceCommand::new(ResourceOperation::Transfer { handoff })
            .expect("identity-valid foreign provenance command derives");
        let rejected = fixture.control().resource().commit(&command);
        if producer_run == other_run {
            assert!(matches!(rejected, Err(DurableError::Validation(_))));
        } else {
            assert!(matches!(rejected, Err(DurableError::NotFound(_))));
        }
        assert_eq!(fixture.head(), before);
    }
    let mut missing = fixture.handoff.clone();
    missing.producer.occurrence_id = format!("sha256:{}", "f".repeat(64));
    assert!(
        fixture
            .control()
            .resource()
            .commit(
                &ResourceCommand::new(ResourceOperation::Transfer { handoff: missing })
                    .expect("missing occurrence command derives")
            )
            .is_err()
    );
    assert_eq!(fixture.head(), before);
    fixture
        .control()
        .resource()
        .commit(&fixture.transfer())
        .expect("valid transfer commits");
    let retained = fixture.head();
    let mut competitor = fixture.handoff.clone();
    competitor.transfer_id = "transfer:competing-slot".to_owned();
    assert!(matches!(
        fixture.control().resource().commit(
            &ResourceCommand::new(ResourceOperation::Transfer {
                handoff: competitor
            })
            .expect("competitor command derives")
        ),
        Err(DurableError::HistoryConflict { .. })
    ));
    assert_eq!(fixture.head(), retained);

    let untyped = Fixture::new(false, json!({}));
    let untyped_head = untyped.head();
    assert!(
        untyped
            .control()
            .resource()
            .commit(&untyped.transfer())
            .is_err()
    );
    assert_eq!(untyped.head(), untyped_head);
    fixture.audit();
    untyped.audit();
}

#[test]
fn pinned_activation_rejects_foreign_source_slot_and_incompatible_wait_schema() {
    let fixture = Fixture::new(true, json!({}));
    let transfer = fixture
        .control()
        .resource()
        .commit(&fixture.transfer())
        .expect("source transfer commits");
    let mut wrong_source = fixture.activation(&transfer);
    let ResourceOperation::ActivateTransfer {
        source_receipt_id, ..
    } = &mut wrong_source.operation
    else {
        panic!("activation command is absent");
    };
    *source_receipt_id = format!("sha256:{}", "e".repeat(64));
    let before = fixture.head();
    assert!(matches!(
        fixture.control().resource().commit(&wrong_source),
        Err(DurableError::HistoryConflict { .. })
    ));
    assert_eq!(fixture.head(), before);
    assert_eq!(
        fixture.wait(&mut fixture.control()).state,
        WaitState::Pending
    );

    let mut wrong_slot = fixture.handoff.clone();
    wrong_slot.transfer_id = "transfer:other-slot".to_owned();
    wrong_slot.slot = "input.other".to_owned();
    let transfer = fixture
        .control()
        .resource()
        .commit(
            &ResourceCommand::new(ResourceOperation::Transfer {
                handoff: wrong_slot.clone(),
            })
            .expect("other slot command derives"),
        )
        .expect("unoccupied target slot commits");
    let ResourceCommandOutcome::Transfer { receipt } = transfer.outcome else {
        panic!("transfer receipt is absent");
    };
    let command = ResourceCommand::new(ResourceOperation::ActivateTransfer {
        activation: ResourceHandoffActivation::new(&wrong_slot, &fixture.wait_id)
            .expect("wrong-slot activation derives"),
        source_receipt_id: receipt.receipt_id,
    })
    .expect("wrong-slot command derives");
    let before = fixture.head();
    assert!(matches!(
        fixture.control().resource().commit(&command),
        Err(DurableError::HistoryConflict { .. })
    ));
    assert_eq!(fixture.head(), before);

    let schema = Fixture::new(true, json!({"type": "string"}));
    let transfer = schema
        .control()
        .resource()
        .commit(&schema.transfer())
        .expect("typed source transfer commits");
    let before = schema.head();
    let rejected = schema
        .control()
        .resource()
        .commit(&schema.activation(&transfer));
    assert!(
        matches!(
            &rejected,
            Err(DurableError::Contract(violation))
                if violation.phase == cymule_runtime::ContractPhase::Execution
                    && violation.target.boundary == cymule_runtime::ContractBoundary::Wait
                    && violation.target.id == schema.wait_id
                    && violation.target.side == cymule_runtime::ContractSide::Input
                    && matches!(violation.issues.as_slice(), [issue]
                        if issue.kind == cymule_runtime::ContractIssueKind::Validation
                            && issue.instance_path.is_empty()
                            && issue.schema_path == "/type")
        ),
        "unexpected schema rejection: {rejected:?}"
    );
    assert_eq!(schema.head(), before);
    assert_eq!(schema.wait(&mut schema.control()).state, WaitState::Pending);
    fixture.audit();
    schema.audit();
}

#[test]
fn resource_transfer_and_activation_cas_faults_reconcile_one_exact_receipt() {
    for activate in [false, true] {
        for fault in [BEFORE_CAS, AFTER_CAS] {
            let fixture = Fixture::new(true, json!({}));
            let command = if activate {
                let transfer = fixture
                    .control()
                    .resource()
                    .commit(&fixture.transfer())
                    .expect("source transfer commits before activation fault");
                fixture.activation(&transfer)
            } else {
                fixture.transfer()
            };
            let before = fixture.head();
            fixture.store.fault.store(fault, Ordering::SeqCst);
            let result = fixture.control().resource().commit(&command);
            if fault == BEFORE_CAS {
                assert!(matches!(result, Err(DurableError::Substrate { .. })));
                assert_eq!(fixture.head(), before);
                assert!(
                    fixture
                        .control()
                        .resource()
                        .command_receipt(&command.command_id)
                        .expect("absent command reads")
                        .is_none()
                );
            } else {
                assert!(matches!(
                    result,
                    Err(DurableError::CommitOutcomeUnknown { .. })
                ));
                assert_eq!(fixture.head().sequence, before.sequence + 1);
                assert!(
                    fixture
                        .control()
                        .resource()
                        .command_receipt(&command.command_id)
                        .expect("committed command reads")
                        .is_some()
                );
            }
            let mut reopened = fixture.control();
            let receipt = reopened
                .resource()
                .commit(&command)
                .expect("reopened authority resolves exact operation");
            assert_eq!(fixture.head().sequence, before.sequence + 1);
            let committed = fixture.head();
            assert_eq!(
                fixture
                    .control()
                    .resource()
                    .commit(&command)
                    .expect("stable replay succeeds"),
                receipt
            );
            assert_eq!(fixture.head(), committed);
            assert_eq!(
                fixture.wait(&mut reopened).state,
                if activate {
                    WaitState::Completed
                } else {
                    WaitState::Pending
                }
            );
            assert_eq!(
                reopened
                    .resource()
                    .handoff_page(CONSUMER, 0, 1)
                    .expect("target index reads")
                    .handoffs
                    .len(),
                1
            );
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            fixture.audit();
        }
    }
}

fn assert_handoff_indexes(
    fixture: &Fixture,
    control: &mut DurableStoreControl<ProbeStore>,
    activation: &ResourceCommandReceipt,
) {
    let ResourceCommandOutcome::ActivateTransfer { receipt } = &activation.outcome else {
        panic!("activation outcome is absent");
    };
    assert_eq!(
        control
            .resource()
            .handoff_activation_current(&receipt.activation.activation_id)
            .expect("activation current reads")
            .expect("activation is retained")
            .receipt,
        *receipt
    );
    assert_eq!(
        control
            .resource()
            .handoff_page(CONSUMER, 0, 1)
            .expect("one bounded handoff page reads")
            .handoffs,
        vec![fixture.handoff.clone()]
    );
}
