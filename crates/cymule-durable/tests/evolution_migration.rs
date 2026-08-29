//! Public safe-point migration, atomic epoch advancement, and exact replay.

/// Shared public-control fixtures and issued Clock authority.
pub mod support;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use cymule_core::{
    ArtifactRecord, ArtifactRef, AttemptProjection, Definition, Expression, Machine, Operation,
    Region, SealedPlan, Step, artifact_ref, canonical_bytes, plan_invocation_id, seal_plan,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand,
    DurableError, DurableResponse, DurableResult, DurableRunCurrent, DurableStore,
    DurableStoreControl, GcReceipt, JournalRecordManifest, MemoryStore, StateRootManifest,
    StateRootResolver, StoreBatch, StoreCommit, StoreHead, StoreReclamation, StoreStats,
    StoredState,
};
use cymule_durable_protocol::{Continuation, ContinuationStatus, WaitActivationSource};
use cymule_profile_protocol::evolution::{
    EVOLUTION_CONTROL_VERSION, EvolutionCommand, EvolutionCommit, EvolutionError,
    EvolutionPersistenceCommand, EvolutionProviders, EvolutionReceiptQuery, EvolutionResult,
    LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand, LiveEvolutionOutcome, MigrationAdapter,
    MigrationAdapterDescriptor, MigrationAdapterRequest, MigrationCapabilityChange,
    MigrationOutput, MigrationPreservation, MigrationReceipt, MigrationRequest,
    MigrationStateCoverage, NoEvolutionProviders, PlanEdge, PlanPatch, PlanTemplate,
    RolloutDecision, RolloutMode, ShadowDriver, SubflowReference, analyze_relink, diff_plans,
};
use cymule_runtime::ExecutionBinding;
use serde_json::{Value, json};

const EVOLUTION_ID: &str = "evolution:public-migration";
const TEMPLATE_ID: &str = "template:public-migration";
const LOGICAL_REF: &str = "migration.result";
const LOCAL_DEFINITION: &str = "migrated_result";
const SIGNAL_KEY: &str = "signal:public-migration";
const ADAPTER_ID: &str = "migration.root-frame";
const ADAPTER_REVISION: &str =
    "sha256:8888888888888888888888888888888888888888888888888888888888888888";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationStoreFault {
    ResolverRead,
    BeforeCas,
    AfterCas,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MigrationStoreTrace {
    resolver_reads: usize,
    cas_attempts: usize,
    cas_commits: usize,
    fault_hits: usize,
}

#[derive(Default)]
struct MigrationStoreState {
    fault: Option<MigrationStoreFault>,
    trace: MigrationStoreTrace,
}

#[derive(Clone)]
struct MigrationStore {
    inner: MemoryStore,
    state: Rc<RefCell<MigrationStoreState>>,
}

impl MigrationStore {
    fn new(inner: MemoryStore) -> Self {
        Self {
            inner,
            state: Rc::new(RefCell::new(MigrationStoreState::default())),
        }
    }

    fn arm(&self, fault: MigrationStoreFault) {
        *self.state.borrow_mut() = MigrationStoreState {
            fault: Some(fault),
            trace: MigrationStoreTrace::default(),
        };
    }

    fn reset_trace(&self) {
        *self.state.borrow_mut() = MigrationStoreState::default();
    }

    fn trace(&self) -> MigrationStoreTrace {
        self.state.borrow().trace.clone()
    }
}

impl DurableStore for MigrationStore {
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
        let fail = {
            let mut state = self.state.borrow_mut();
            state.trace.resolver_reads += 1;
            if state.fault == Some(MigrationStoreFault::ResolverRead) {
                state.fault = None;
                state.trace.fault_hits += 1;
                true
            } else {
                false
            }
        };
        if fail {
            return Err(DurableError::Substrate {
                code: "migration_test_resolver_read".to_owned(),
                message: "injected storage failure before migration preparation".to_owned(),
            });
        }
        self.inner.with_state_root_resolver(current, read)
    }

    fn load_full_audit(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load_full_audit()
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
        let fault = {
            let mut state = self.state.borrow_mut();
            state.trace.cas_attempts += 1;
            match state.fault {
                Some(MigrationStoreFault::BeforeCas | MigrationStoreFault::AfterCas) => {
                    state.trace.fault_hits += 1;
                    state.fault.take()
                }
                _ => None,
            }
        };
        if fault == Some(MigrationStoreFault::BeforeCas) {
            return Err(DurableError::Substrate {
                code: "migration_test_before_cas".to_owned(),
                message: "injected storage failure before migration head publication".to_owned(),
            });
        }
        let commit = self.inner.compare_and_commit(expected, batch)?;
        self.state.borrow_mut().trace.cas_commits += 1;
        if fault == Some(MigrationStoreFault::AfterCas) {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "injected lost acknowledgement after migration head publication"
                    .to_owned(),
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

fn artifact(kind: &str, value: &Value) -> ArtifactRecord {
    let bytes = canonical_bytes(value).expect("fixture JSON canonicalizes");
    ArtifactRecord {
        reference: artifact_ref(kind, &bytes).expect("fixture Artifact identity derives"),
        bytes,
    }
}

fn output_definition(value: &str) -> Definition {
    Definition {
        id: "result".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: Vec::new(),
            result: Expression::Literal {
                value: json!(value),
            },
        },
    }
}

fn persistence(command: LiveEvolutionCommand) -> EvolutionPersistenceCommand {
    EvolutionPersistenceCommand::new(EVOLUTION_ID, command).expect("public Evolution command seals")
}

fn apply(command_id: &str, command: EvolutionCommand) -> EvolutionPersistenceCommand {
    persistence(LiveEvolutionCommand::Apply {
        control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: command_id.to_owned(),
        template_id: TEMPLATE_ID.to_owned(),
        command: Box::new(command),
    })
}

fn commit_catalog(
    control: &mut DurableStoreControl<MemoryStore>,
    command: &EvolutionPersistenceCommand,
) -> EvolutionCommit {
    control
        .evolution(&mut NoEvolutionProviders)
        .commit(command)
        .expect("provider-free public Evolution command commits")
}

fn register_source(control: &mut DurableStoreControl<MemoryStore>) -> SealedPlan {
    commit_catalog(
        control,
        &persistence(LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "publish:migration-source".to_owned(),
            logical_ref: LOGICAL_REF.to_owned(),
            definition: output_definition("source"),
            references: Vec::new(),
        }),
    );
    let mut candidate = support::signal_candidate("public-migration", SIGNAL_KEY, true);
    candidate.definitions[0].body.steps.push(Step {
        id: "invoke.result".to_owned(),
        operation: Operation::Invoke {
            definition: LOCAL_DEFINITION.to_owned(),
            input: Expression::Input,
            bind: Some("result".to_owned()),
        },
    });
    candidate.definitions[0].body.result = Expression::Binding {
        name: "result".to_owned(),
    };
    let registered = commit_catalog(
        control,
        &persistence(LiveEvolutionCommand::RegisterTemplate {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "register:migration-template".to_owned(),
            template: PlanTemplate {
                template_id: TEMPLATE_ID.to_owned(),
                candidate,
                references: vec![SubflowReference::latest_compatible(
                    LOGICAL_REF,
                    LOCAL_DEFINITION,
                    json!({}),
                    json!({}),
                )],
            },
        }),
    );
    let LiveEvolutionOutcome::TemplateRegistered { linked } = registered.receipt.outcome else {
        panic!("template registration returned another outcome")
    };
    linked.plan
}

fn start_waiting(store: MemoryStore, plan: &SealedPlan, run_id: &str) -> (MemoryStore, String) {
    let mut runtime = support::open_control(store, support::EmptyPlugin, support::empty_binding())
        .expect("source runtime opens");
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: plan.candidate.clone(),
            input: json!({"preserved": "source input"}),
            execution: support::execution(run_id),
        })
        .expect("source Run reaches its declared Wait");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("source Run did not suspend at its declared Wait")
    };
    let (store, _) = runtime.into_parts();
    (store, wait_id)
}

fn activate_source(
    control: &mut DurableStoreControl<MemoryStore>,
    run_id: &str,
    wait_id: String,
) -> ArtifactRef {
    let response = control
        .submit(DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:migration-safe-point".to_owned(),
            source: WaitActivationSource::Signal {
                key: SIGNAL_KEY.to_owned(),
            },
            wait_ids: BTreeSet::from([wait_id]),
            value: json!({"review": "replace the result without widening authority"}),
        })
        .expect("identified activation creates a store-owned Ready safe point");
    let DurableResponse::WaitActivated { receipt } = response else {
        panic!("activation returned another response")
    };
    assert_eq!(receipt.ready_run_ids, BTreeSet::from([run_id.to_owned()]));
    receipt.activation.result
}

fn current<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    run_id: &str,
    revision: &str,
) -> DurableRunCurrent {
    let query = DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: Some(revision.to_owned()),
    };
    let response = control
        .submit(query.clone())
        .expect("exact Run current reads");
    response
        .verify_query_for(&query)
        .expect("Run query response verifies");
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = response
    else {
        panic!("migration Run current is absent")
    };
    *current
}

fn publish_target(
    control: &mut DurableStoreControl<MemoryStore>,
    source: &SealedPlan,
    binding: &ArtifactRef,
    evidence: ArtifactRef,
) -> (SealedPlan, PlanEdge) {
    let selected = commit_catalog(
        control,
        &apply(
            "select:migration-source:outer",
            EvolutionCommand::SelectOccurrence {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "select:migration-source:inner".to_owned(),
                occurrence_id: "occurrence:migration-source".to_owned(),
                selection_id: "selection:migration-source".to_owned(),
                execution_binding: binding.clone(),
            },
        ),
    );
    let LiveEvolutionOutcome::OccurrenceSelected { pin } = selected.receipt.outcome else {
        panic!("source selection returned another outcome")
    };
    assert_eq!(pin.plan_id, source.plan_id);
    assert_eq!(&pin.execution_binding, binding);

    let mut candidate = source.candidate.clone();
    candidate
        .definitions
        .iter_mut()
        .find(|definition| definition.id == LOCAL_DEFINITION)
        .expect("linked reusable definition exists")
        .body = output_definition("target").body;
    let target = seal_plan(candidate).expect("reviewed target seals");
    let patched = commit_catalog(
        control,
        &apply(
            "patch:migration-target:outer",
            EvolutionCommand::ApplyPatch {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "patch:migration-target:inner".to_owned(),
                patch: PlanPatch {
                    from_plan: source.plan_id.clone(),
                    target: target.candidate.clone(),
                    operations: diff_plans(source, &target).expect("exact Plan diff derives"),
                    evidence,
                },
            },
        ),
    );
    let LiveEvolutionOutcome::PatchApplied { edge } = patched.receipt.outcome else {
        panic!("reviewed patch returned another outcome")
    };
    assert_eq!(edge.from_plan, source.plan_id);
    assert_eq!(edge.to_plan, target.plan_id);
    commit_catalog(
        control,
        &apply(
            "rollout:migration-target:outer",
            EvolutionCommand::SetRollout {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "rollout:migration-target:inner".to_owned(),
                decision: RolloutDecision {
                    decision_id: "decision:migration-target".to_owned(),
                    fallback_plan: source.plan_id.clone(),
                    target_plan: target.plan_id.clone(),
                    mode: RolloutMode::Active,
                },
            },
        ),
    );
    (target, edge)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationProviderFault {
    TargetBinding,
    Describe,
    Migrate,
}

struct RootFrameAdapter {
    descriptor: MigrationAdapterDescriptor,
    state: ArtifactRecord,
    evidence: ArtifactRecord,
    described: usize,
    migrated: usize,
    observed_source: Option<MigrationAdapterRequest>,
    fault: Option<MigrationProviderFault>,
}

impl MigrationAdapter for RootFrameAdapter {
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor> {
        self.described += 1;
        if self.fault == Some(MigrationProviderFault::Describe) {
            return Err(EvolutionError::PluginDefect {
                code: "migration_test_describe_failed".to_owned(),
                message: "injected migration adapter Describe failure".to_owned(),
            });
        }
        Ok(self.descriptor.clone())
    }

    fn migrate(&mut self, request: &MigrationAdapterRequest) -> EvolutionResult<MigrationOutput> {
        self.migrated += 1;
        if self.fault == Some(MigrationProviderFault::Migrate) {
            return Err(EvolutionError::PluginDefect {
                code: "migration_test_migrate_failed".to_owned(),
                message: "injected migration adapter execution failure".to_owned(),
            });
        }
        request.verify()?;
        self.observed_source = Some(request.clone());
        let mut continuation = request.source_continuation.clone();
        continuation.plan_id.clone_from(&request.intent.to_plan);
        continuation
            .binding_context
            .clone_from(&request.target_binding.artifact_id);
        continuation.epoch = request.intent.expected_source_epoch + 1;
        continuation.state = Some(self.state.reference.clone());
        for frame in &mut continuation.frames {
            frame.invocation_id = plan_invocation_id(
                &continuation.run_id,
                &continuation.plan_id,
                "main",
                &frame.invocation_path,
            )?;
        }
        Ok(MigrationOutput {
            continuation,
            artifacts: vec![self.state.clone()],
            evidence: self.evidence.clone(),
        })
    }
}

struct MigrationProviders {
    target_binding: ExecutionBinding,
    adapter: RootFrameAdapter,
    binding_lookups: usize,
    adapter_lookups: usize,
    shadow_lookups: usize,
    binding_fault: bool,
}

impl MigrationProviders {
    fn calls(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.binding_lookups,
            self.adapter_lookups,
            self.shadow_lookups,
            self.adapter.described,
            self.adapter.migrated,
        )
    }

    fn arm(&mut self, fault: MigrationProviderFault) {
        self.binding_fault = fault == MigrationProviderFault::TargetBinding;
        self.adapter.fault = match fault {
            MigrationProviderFault::Describe | MigrationProviderFault::Migrate => Some(fault),
            MigrationProviderFault::TargetBinding => None,
        };
    }
}

impl EvolutionProviders for MigrationProviders {
    fn target_execution_binding(&mut self, plan_id: &str) -> EvolutionResult<ExecutionBinding> {
        self.binding_lookups += 1;
        if self.binding_fault {
            return Err(EvolutionError::NotFound(
                "injected target-binding registry failure".to_owned(),
            ));
        }
        if plan_id != self.adapter.descriptor.to_plan {
            return Err(EvolutionError::NotFound(format!(
                "unregistered target Plan {plan_id}"
            )));
        }
        Ok(self.target_binding.clone())
    }

    fn migration_adapter(
        &mut self,
        adapter_id: &str,
        adapter_revision: &str,
    ) -> EvolutionResult<&mut dyn MigrationAdapter> {
        self.adapter_lookups += 1;
        if adapter_id != self.adapter.descriptor.adapter_id
            || adapter_revision != self.adapter.descriptor.adapter_revision
        {
            return Err(EvolutionError::NotFound(format!(
                "unregistered migration adapter {adapter_id}@{adapter_revision}"
            )));
        }
        Ok(&mut self.adapter)
    }

    fn shadow_driver(
        &mut self,
        driver_id: &str,
        driver_revision: &str,
    ) -> EvolutionResult<&mut dyn ShadowDriver> {
        self.shadow_lookups += 1;
        Err(EvolutionError::NotFound(format!(
            "unregistered shadow driver {driver_id}@{driver_revision}"
        )))
    }
}

struct MigrationFixture {
    store: MigrationStore,
    control: DurableStoreControl<MigrationStore>,
    source: Continuation,
    source_attempts: BTreeMap<String, AttemptProjection>,
    command: EvolutionPersistenceCommand,
    providers: MigrationProviders,
    run_id: String,
}

fn fixture(run_id: &str) -> MigrationFixture {
    let mut control = DurableStoreControl::initialize(MemoryStore::new())
        .expect("empty durable domain initializes through its public facade");
    let source_plan = register_source(&mut control);
    let (mut store, wait_id) = start_waiting(control.into_store(), &source_plan, run_id);
    let mut control = DurableStoreControl::open(store.clone()).expect("store-only control reopens");
    let evidence = activate_source(&mut control, run_id, wait_id);
    let source_binding = support::empty_binding()
        .artifact_ref()
        .expect("source binding derives");
    let (target, edge) = publish_target(&mut control, &source_plan, &source_binding, evidence);
    let source_audit = store
        .load_full_audit()
        .expect("actual source state audits")
        .expect("source exists");
    let source = source_audit.state.continuations[run_id].clone();
    assert_eq!(source.status, ContinuationStatus::Ready);
    assert!(source.execution_claim.is_none());
    assert!(source.wait_set.is_empty());
    assert_eq!(source.frames.len(), 1);
    assert_eq!(source.frames[0].next_step, 1);
    let source_machine = Machine::restore(source_audit.state.machine)
        .expect("actual source command history verifies");
    let source_attempts = source_machine.projection().runs[run_id].attempts.clone();
    let compatibility = analyze_relink(&source_plan, &target).expect("compatibility derives");
    assert!(compatibility.is_compatible());
    let request = MigrationRequest {
        migration_id: "migration:public-root-frame".to_owned(),
        run_id: run_id.to_owned(),
        from_plan: source_plan.plan_id,
        to_plan: target.plan_id,
        plan_edge_id: edge.edge_id,
        compatibility_id: compatibility.compatibility_id,
        expected_source_epoch: source.epoch,
        adapter_id: ADAPTER_ID.to_owned(),
        adapter_revision: ADAPTER_REVISION.to_owned(),
    };
    let descriptor = MigrationAdapterDescriptor {
        adapter_id: request.adapter_id.clone(),
        adapter_revision: request.adapter_revision.clone(),
        from_plan: request.from_plan.clone(),
        to_plan: request.to_plan.clone(),
        plan_edge_id: request.plan_edge_id.clone(),
        compatibility_id: request.compatibility_id.clone(),
        from_schema: "state:source".to_owned(),
        to_schema: "state:target".to_owned(),
        state_coverage: MigrationStateCoverage::TotalReachableState,
        failure_and_cancellation: MigrationPreservation::Preserved,
        budget_and_ownership: MigrationPreservation::Preserved,
        authority_and_effects: MigrationCapabilityChange::NoWidening,
    };
    let store = MigrationStore::new(control.into_store());
    let control = DurableStoreControl::open(store.clone())
        .expect("fault-oriented store-only control reopens");
    MigrationFixture {
        store,
        control,
        source,
        source_attempts,
        command: apply(
            "migrate:public:outer",
            EvolutionCommand::Migrate {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "migrate:public:inner".to_owned(),
                request: Box::new(request),
            },
        ),
        providers: MigrationProviders {
            target_binding: ExecutionBinding::for_local_process(
                &support::empty_manifest(),
                "sha256:9999999999999999999999999999999999999999999999999999999999999999",
            )
            .expect("exact target binding derives"),
            adapter: RootFrameAdapter {
                descriptor,
                state: artifact("cymule.test-migration-state/1", &json!({"migrated": true})),
                evidence: artifact(
                    "cymule.test-migration-evidence/1",
                    &json!({"verified": true}),
                ),
                described: 0,
                migrated: 0,
                observed_source: None,
                fault: None,
            },
            binding_lookups: 0,
            adapter_lookups: 0,
            shadow_lookups: 0,
            binding_fault: false,
        },
        run_id: run_id.to_owned(),
    }
}

fn assert_same_root(fixture: &mut MigrationFixture, commit: &EvolutionCommit) {
    let LiveEvolutionOutcome::Migrated { receipt } = &commit.receipt.outcome else {
        panic!("migration returned another outcome")
    };
    let current = current(
        &mut fixture.control,
        &fixture.run_id,
        &commit.observed_revision,
    );
    assert_eq!(current.epoch, fixture.source.epoch + 1);
    assert_eq!(current.epoch, receipt.target_epoch);
    assert_eq!(current.plan_id, receipt.request.to_plan);
    assert_eq!(current.execution_binding, receipt.target_binding);
    assert_eq!(current.execution_fence, fixture.source.execution_fence);
    assert_eq!(current.continuation_status, ContinuationStatus::Ready);
    assert_eq!(
        receipt.source_binding.artifact_id,
        fixture.source.binding_context
    );
    assert_eq!(
        receipt.target_binding,
        fixture
            .providers
            .target_binding
            .artifact_ref()
            .expect("target binding derives")
    );
    assert_eq!(
        receipt.output_state,
        fixture.providers.adapter.state.reference
    );
    assert_eq!(
        receipt.evidence,
        fixture.providers.adapter.evidence.reference
    );
    for reference in [
        &receipt.target_binding,
        &receipt.output_state,
        &receipt.evidence,
    ] {
        let retained = fixture
            .control
            .read_artifact(reference, &commit.observed_revision)
            .expect("migration material reads at the exact committed revision");
        assert_eq!(retained.observed_revision, commit.observed_revision);
        assert_eq!(
            retained.value.expect("migration material exists").reference,
            *reference
        );
    }
    let retained = fixture
        .control
        .evolution(&mut fixture.providers)
        .read_receipt(&EvolutionReceiptQuery {
            evolution_id: EVOLUTION_ID.to_owned(),
            command_id: fixture.command.command.command_id().to_owned(),
            expected_revision: Some(commit.observed_revision.clone()),
        })
        .expect("migration receipt reads at the same committed revision");
    assert_eq!(retained.observed_revision, commit.observed_revision);
    assert_eq!(retained.receipt.as_ref(), Some(&commit.receipt));
    assert_audited_epoch(fixture, receipt);
}

fn assert_audited_epoch(fixture: &mut MigrationFixture, receipt: &MigrationReceipt) {
    // This explicit offline assertion audits actual public commits; it never
    // supplies a reconstructed Machine or Continuation to a mutation.
    let audited = fixture
        .store
        .load_full_audit()
        .expect("migrated store fully audits")
        .expect("migrated state exists");
    let continuation = &audited.state.continuations[&fixture.run_id];
    assert_eq!(continuation, &receipt.target_continuation);
    assert!(continuation.execution_claim.is_none());
    let machine =
        Machine::restore(audited.state.machine).expect("actual committed history verifies");
    let run = &machine.projection().runs[&fixture.run_id];
    assert_eq!(run.epoch, continuation.epoch);
    assert_eq!(run.current_plan, continuation.plan_id);
    assert_eq!(run.current_binding_context, continuation.binding_context);
    assert_eq!(
        run.binding_lineage,
        [
            receipt.source_binding.artifact_id.clone(),
            receipt.target_binding.artifact_id.clone()
        ]
    );
    assert_eq!(
        run.plan_lineage,
        [
            receipt.request.from_plan.clone(),
            receipt.request.to_plan.clone()
        ]
    );
    assert_eq!(
        run.attempts.len(),
        1,
        "migration does not acquire another execution Attempt"
    );
    assert_eq!(run.attempts, fixture.source_attempts);
    assert!(run.attempts.values().all(|attempt| !attempt.active));
}

fn assert_replay(fixture: &mut MigrationFixture, original: &EvolutionCommit) {
    let head = fixture.store.load_head().expect("head reads before replay");
    let stats = fixture
        .store
        .stats()
        .expect("physical counts read before replay");
    let calls = fixture.providers.calls();
    let mut reopened = DurableStoreControl::open(fixture.store.clone())
        .expect("fresh store-only authority reopens");
    let replay = reopened
        .evolution(&mut fixture.providers)
        .commit(&fixture.command)
        .expect("exact retained command replays independently of current Run state");
    assert_eq!(replay.committed_revision, None);
    assert_eq!(replay.receipt, original.receipt);
    assert_eq!(fixture.providers.calls(), calls);
    assert_eq!(
        fixture.store.load_head().expect("head reads after replay"),
        head
    );
    assert_eq!(
        fixture
            .store
            .stats()
            .expect("physical counts read after replay"),
        stats
    );
}

#[derive(Clone)]
struct MigrationAuthoritySnapshot {
    head: StoreHead,
    stats: StoreStats,
    current: DurableRunCurrent,
}

fn authority_snapshot(fixture: &mut MigrationFixture) -> MigrationAuthoritySnapshot {
    let head = fixture
        .store
        .load_head()
        .expect("migration source head reads")
        .expect("migration source exists");
    let stats = fixture
        .store
        .stats()
        .expect("migration source physical counts read");
    let current = current(&mut fixture.control, &fixture.run_id, &head.revision);
    MigrationAuthoritySnapshot {
        head,
        stats,
        current,
    }
}

fn assert_no_migration_write(fixture: &mut MigrationFixture, before: &MigrationAuthoritySnapshot) {
    assert_eq!(
        fixture.store.load_head().expect("head reads after failure"),
        Some(before.head.clone()),
        "pre-CAS failure must not publish another Store head"
    );
    assert_eq!(
        fixture
            .store
            .stats()
            .expect("physical counts read after failure"),
        before.stats,
        "pre-CAS failure must not retain immutable objects"
    );
    assert_eq!(
        current(&mut fixture.control, &fixture.run_id, &before.head.revision),
        before.current,
        "pre-CAS failure must not change M1 Run authority"
    );

    let mut providers = NoEvolutionProviders;
    let retained = fixture
        .control
        .evolution(&mut providers)
        .read_receipt(&EvolutionReceiptQuery {
            evolution_id: EVOLUTION_ID.to_owned(),
            command_id: fixture.command.command.command_id().to_owned(),
            expected_revision: Some(before.head.revision.clone()),
        })
        .expect("exact absent migration receipt reads");
    assert_eq!(retained.observed_revision, before.head.revision);
    assert!(
        retained.receipt.is_none(),
        "failed migration retained an M4 receipt"
    );

    let target_binding = fixture
        .providers
        .target_binding
        .artifact_ref()
        .expect("target binding derives");
    for reference in [
        &target_binding,
        &fixture.providers.adapter.state.reference,
        &fixture.providers.adapter.evidence.reference,
    ] {
        assert!(
            fixture
                .control
                .read_artifact(reference, &before.head.revision)
                .expect("failed migration material absence reads")
                .value
                .is_none(),
            "failed migration retained material {}",
            reference.artifact_id
        );
    }

    let audited = fixture
        .store
        .load_full_audit()
        .expect("source store still fully audits")
        .expect("source state remains");
    assert_eq!(audited.state.continuations[&fixture.run_id], fixture.source);
    let machine = Machine::restore(audited.state.machine)
        .expect("source Machine still restores after failed migration");
    assert_eq!(
        machine.projection().runs[&fixture.run_id].attempts,
        fixture.source_attempts
    );
}

fn commit_migration(fixture: &mut MigrationFixture) -> DurableResult<EvolutionCommit> {
    fixture
        .control
        .evolution(&mut fixture.providers)
        .commit(&fixture.command)
}

#[test]
fn migration_storage_read_failure_precedes_every_provider_and_write() {
    let mut fixture = fixture("run:public-evolution-migration:storage-read");
    let before = authority_snapshot(&mut fixture);
    fixture.store.arm(MigrationStoreFault::ResolverRead);

    let error = commit_migration(&mut fixture)
        .expect_err("injected exact-root read failure must stop migration preparation");
    assert!(matches!(
        error,
        DurableError::Substrate { ref code, .. } if code == "migration_test_resolver_read"
    ));
    assert_eq!(fixture.providers.calls(), (0, 0, 0, 0, 0));
    assert_eq!(
        fixture.store.trace(),
        MigrationStoreTrace {
            resolver_reads: 1,
            cas_attempts: 0,
            cas_commits: 0,
            fault_hits: 1,
        }
    );
    assert_no_migration_write(&mut fixture, &before);

    let committed = commit_migration(&mut fixture)
        .expect("retry after a definite read failure executes one fresh migration");
    assert_eq!(fixture.providers.calls(), (1, 1, 0, 1, 1));
    assert_eq!(fixture.store.trace().cas_attempts, 1);
    assert_eq!(fixture.store.trace().cas_commits, 1);
    assert_same_root(&mut fixture, &committed);
}

#[test]
fn migration_provider_failures_precede_the_single_cas_with_exact_call_boundaries() {
    for (label, fault, expected_calls, expected_code) in [
        (
            "target-binding",
            MigrationProviderFault::TargetBinding,
            (1, 0, 0, 0, 0),
            "target-binding registry",
        ),
        (
            "describe",
            MigrationProviderFault::Describe,
            (1, 1, 0, 1, 0),
            "migration_test_describe_failed",
        ),
        (
            "migrate",
            MigrationProviderFault::Migrate,
            (1, 1, 0, 1, 1),
            "migration_test_migrate_failed",
        ),
    ] {
        let mut fixture = fixture(&format!("run:public-evolution-migration:provider:{label}"));
        let before = authority_snapshot(&mut fixture);
        fixture.store.reset_trace();
        fixture.providers.arm(fault);

        let error = commit_migration(&mut fixture)
            .expect_err("injected migration provider failure must stop before CAS");
        match fault {
            MigrationProviderFault::TargetBinding => assert!(matches!(
                error,
                DurableError::NotFound(ref message) if message.contains(expected_code)
            )),
            MigrationProviderFault::Describe | MigrationProviderFault::Migrate => {
                assert!(matches!(
                    error,
                    DurableError::RuntimeDefect { ref code, .. } if code == expected_code
                ));
            }
        }
        assert_eq!(fixture.providers.calls(), expected_calls);
        assert_eq!(fixture.store.trace().cas_attempts, 0);
        assert_eq!(fixture.store.trace().cas_commits, 0);
        assert_no_migration_write(&mut fixture, &before);
    }
}

#[test]
fn migration_before_cas_failure_writes_nothing_and_definite_retry_runs_provider_again() {
    let mut fixture = fixture("run:public-evolution-migration:before-cas");
    let before = authority_snapshot(&mut fixture);
    fixture.store.arm(MigrationStoreFault::BeforeCas);

    let error = commit_migration(&mut fixture)
        .expect_err("injected pre-CAS failure must reject migration publication");
    assert!(matches!(
        error,
        DurableError::Substrate { ref code, .. } if code == "migration_test_before_cas"
    ));
    assert_eq!(fixture.providers.calls(), (1, 1, 0, 1, 1));
    let trace = fixture.store.trace();
    assert!(trace.resolver_reads > 0);
    assert_eq!(trace.cas_attempts, 1);
    assert_eq!(trace.cas_commits, 0);
    assert_eq!(trace.fault_hits, 1);
    assert_no_migration_write(&mut fixture, &before);

    let committed = commit_migration(&mut fixture)
        .expect("definite pre-CAS failure permits a fresh provider execution");
    assert_eq!(fixture.providers.calls(), (2, 2, 0, 2, 2));
    assert_eq!(fixture.store.trace().cas_attempts, 2);
    assert_eq!(fixture.store.trace().cas_commits, 1);
    let after = fixture
        .store
        .load_head()
        .expect("retried migration head reads")
        .expect("retried migration commits");
    assert_eq!(after.sequence, before.head.sequence + 1);
    assert_same_root(&mut fixture, &committed);
}

#[test]
fn migration_after_cas_lost_ack_replays_exact_alias_without_provider_or_second_cas() {
    let mut fixture = fixture("run:public-evolution-migration:after-cas");
    let before = authority_snapshot(&mut fixture);
    fixture.store.arm(MigrationStoreFault::AfterCas);

    let error = commit_migration(&mut fixture)
        .expect_err("lost migration CAS acknowledgement must remain unknown");
    assert!(matches!(error, DurableError::CommitOutcomeUnknown { .. }));
    assert_eq!(fixture.providers.calls(), (1, 1, 0, 1, 1));
    assert_eq!(fixture.store.trace().cas_attempts, 1);
    assert_eq!(fixture.store.trace().cas_commits, 1);
    assert_eq!(fixture.store.trace().fault_hits, 1);

    let after = fixture
        .store
        .load_head()
        .expect("post-CAS head reads")
        .expect("post-CAS migration exists");
    assert_eq!(after.sequence, before.head.sequence + 1);
    let post_commit_stats = fixture
        .store
        .stats()
        .expect("post-CAS physical counts read");
    fixture.control =
        DurableStoreControl::open(fixture.store.clone()).expect("post-CAS exact authority reopens");
    let calls = fixture.providers.calls();
    let replay = commit_migration(&mut fixture)
        .expect("exact migration alias resolves the lost acknowledgement");
    replay
        .verify_for(&fixture.command)
        .expect("replayed migration receipt verifies");
    assert_eq!(replay.committed_revision, None);
    assert_eq!(replay.observed_revision, after.revision);
    assert_eq!(fixture.providers.calls(), calls);
    assert_eq!(fixture.store.trace().cas_attempts, 1);
    assert_eq!(fixture.store.trace().cas_commits, 1);
    assert_eq!(
        fixture
            .store
            .stats()
            .expect("physical counts read after alias replay"),
        post_commit_stats
    );
    assert_eq!(
        fixture.store.load_head().expect("head reads after replay"),
        Some(after)
    );
    assert_same_root(&mut fixture, &replay);
    assert_replay(&mut fixture, &replay);
}

#[test]
fn migration_advances_core_and_continuation_epoch_atomically_then_resumes_after_reopen() {
    let mut fixture = fixture("run:public-evolution-migration:success");
    let before = fixture
        .store
        .load_head()
        .expect("head reads")
        .expect("source head exists");
    let target_binding = fixture
        .providers
        .target_binding
        .artifact_ref()
        .expect("target binding derives");
    assert_ne!(target_binding.artifact_id, fixture.source.binding_context);
    for reference in [
        &target_binding,
        &fixture.providers.adapter.state.reference,
        &fixture.providers.adapter.evidence.reference,
    ] {
        assert!(
            fixture
                .control
                .read_artifact(reference, &before.revision)
                .expect("pre-migration material absence reads")
                .value
                .is_none()
        );
    }
    let committed = fixture
        .control
        .evolution(&mut fixture.providers)
        .commit(&fixture.command)
        .expect("safe-point migration commits its Core and Continuation epoch together");
    committed
        .verify_for(&fixture.command)
        .expect("semantic migration receipt verifies");
    let after = fixture
        .store
        .load_head()
        .expect("result head reads")
        .expect("result exists");
    assert_eq!(
        after.sequence,
        before.sequence + 1,
        "migration publishes exactly one CAS"
    );
    assert_eq!(
        committed.committed_revision.as_deref(),
        Some(after.revision.as_str())
    );
    assert_eq!(fixture.providers.calls(), (1, 1, 0, 1, 1));
    assert_eq!(
        fixture
            .providers
            .adapter
            .observed_source
            .as_ref()
            .expect("adapter consumed a store-derived source")
            .source_continuation,
        fixture.source
    );
    assert_same_root(&mut fixture, &committed);
    assert_replay(&mut fixture, &committed);

    let mut resumed = support::open_control(
        fixture.store.clone(),
        support::EmptyPlugin,
        fixture.providers.target_binding.clone(),
    )
    .expect("target runtime reopens from the migrated head");
    let response = resumed
        .submit(DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: fixture.run_id.clone(),
            execution: support::execution(&fixture.run_id),
        })
        .expect("migrated Ready Run acquires its next Attempt and executes the target Plan");
    assert_eq!(support::expect_completed_value(response), json!("target"));
    drop(resumed);
    assert_replay(&mut fixture, &committed);
}
