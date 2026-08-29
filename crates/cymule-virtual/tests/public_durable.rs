//! Public Durable Virtual fault/reopen and multi-worker ownership witnesses.

/// Shared public-control fixtures and issued current-head Clock authority.
#[path = "../../cymule-durable/tests/support/mod.rs"]
pub mod support;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use cymule_core::{
    ArtifactRecord, COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Expression, Operation,
    PlanCandidate, SealedPlan, Step, artifact_ref, canonical_bytes, decode_json, seal_plan,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    ClockObservationAuthority, CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableCommand,
    DurableError, DurableResult, DurableRuntimeControl, DurableStore, DurableStoreControl,
    ExecutionClockAuthority, GcReceipt, HistoryCompactionKind, HistoryCompactionRequest,
    JournalRecordManifest, MemoryStore, StateRootManifest, StateRootObject, StateRootResolver,
    StoreBatch, StoreCommit, StoreHead, StoreReclamation, StoreStats,
};
use cymule_durable_protocol::{ClockObservation, ClockObservationRef};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, ResourceCatalogRecord, ResourceCatalogStore, ResourceChunk,
    ResourceCleanupReceipt, ResourceError, ResourceHandle, ResourceLocatorSet, ResourceObservation,
    ResourcePage, ResourcePinStatus, ResourcePublication, ResourceResult, ResourceWriteIntent,
    ResourceWriteSession,
};
use cymule_resource_fs::FsResourceStore;
use cymule_runtime::{
    EmbeddedRuntime, ExecutionBinding, ExecutionBindingAdmission, PLUGIN_VERSION, PluginHost,
    PluginManifest, PluginOperation, PluginRequest, PluginResponse, RESULT_ARTIFACT_KIND,
    RuntimeError, RuntimeResult,
};
use cymule_virtual::{
    ClaimedWork, FrontierLimits, MaterializedPage, ProtocolError, ProtocolResult,
    RegionSourceBinding, ResourceBackedVirtualArchive, SchedulingPolicy,
    VIRTUAL_CLAIM_CONTROL_VERSION, VIRTUAL_INITIALIZATION_CONTROL_VERSION,
    VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION, VIRTUAL_MATERIALIZATION_CONTROL_VERSION,
    VIRTUAL_RECOVERY_CONTROL_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VirtualArchiveBinding, VirtualArchiveProvider,
    VirtualClaimCommand, VirtualClaimOutcome, VirtualClaimPersistenceCommand, VirtualCommit,
    VirtualCompactionCommand, VirtualCompactionPersistenceCommand, VirtualCompactionReceipt,
    VirtualCurrent, VirtualCurrentQuery, VirtualInitializationCommand, VirtualLeaseRenewalCommand,
    VirtualLeaseRenewalPersistenceCommand, VirtualMaterializationCommand,
    VirtualPersistenceCommand, VirtualPersistenceEvidence, VirtualPersistenceOperation,
    VirtualPersistenceOutcome, VirtualPersistenceReceipt, VirtualProviders, VirtualReceiptQuery,
    VirtualRecoveryCommand, VirtualRecoveryPersistenceCommand, VirtualRegion,
    VirtualRegionMigratorProvider, VirtualRegionSourceProvider, VirtualRehydrationCommand,
    VirtualRehydrationPersistenceCommand, VirtualResolutionPersistenceCommand,
    VirtualRunDefinition, VirtualRunExecution, WorkItem, WorkOccurrence, WorkOccurrenceState,
    WorkResolution, WorkResolutionCommand, virtual_scheduler_journal_id,
};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultPoint {
    ArchiveBeforeCommit,
    ArchiveAfterCommit,
    StoreBeforeCas,
    StoreAfterCas,
}

#[derive(Default)]
struct Observations {
    fault: Cell<Option<FaultPoint>>,
    faults_hit: Cell<usize>,
    cas_calls: Cell<usize>,
    resource_calls: Cell<usize>,
    provider_lookups: Cell<usize>,
    source_calls: Cell<usize>,
    clock_calls: Cell<usize>,
    business_calls: Cell<usize>,
    plugin_calls: Cell<usize>,
    advance_after_cas: Cell<bool>,
    intervening_head: RefCell<Option<StoreHead>>,
    forbid_plan_reads: Cell<bool>,
    plan_reads: Cell<usize>,
    sessions: RefCell<Vec<ResourceWriteSession>>,
    publications: RefCell<Vec<ResourcePublication>>,
}

impl Observations {
    fn trip(&self, point: FaultPoint) -> bool {
        if self.fault.get() != Some(point) {
            return false;
        }
        self.fault.set(None);
        self.faults_hit.set(self.faults_hit.get() + 1);
        true
    }

    fn effects(&self) -> [usize; 7] {
        [
            self.cas_calls.get(),
            self.resource_calls.get(),
            self.provider_lookups.get(),
            self.source_calls.get(),
            self.clock_calls.get(),
            self.business_calls.get(),
            self.plugin_calls.get(),
        ]
    }
}

/// Only the physical CAS boundary is decorated; opaque batches are unchanged.
struct FaultStore {
    inner: MemoryStore,
    observations: Rc<Observations>,
}

struct ClaimReadProbe<'a> {
    inner: &'a mut dyn StateRootResolver,
    plan_root: Option<&'a str>,
    observations: &'a Observations,
}

impl StateRootResolver for ClaimReadProbe<'_> {
    fn pinned_manifest_id(&self) -> &str {
        self.inner.pinned_manifest_id()
    }

    fn load_state_root_object(
        &mut self,
        object_id: &str,
    ) -> DurableResult<Option<StateRootObject>> {
        if self.plan_root == Some(object_id) {
            self.observations
                .plan_reads
                .set(self.observations.plan_reads.get() + 1);
            if self.observations.forbid_plan_reads.get() {
                return Err(DurableError::RuntimeDefect {
                    code: "test_unexpected_virtual_plan_read".to_owned(),
                    message: "acknowledged claim or receipt-only replay reloaded a Plan".to_owned(),
                });
            }
        }
        self.inner.load_state_root_object(object_id)
    }
}

impl DurableStore for FaultStore {
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
        let observations = Rc::clone(&self.observations);
        self.inner.with_state_root_resolver(current, |resolver| {
            read(&mut ClaimReadProbe {
                inner: resolver,
                plan_root: current.roots().machine_plans.node.as_deref(),
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
        self.observations
            .cas_calls
            .set(self.observations.cas_calls.get() + 1);
        if self.observations.trip(FaultPoint::StoreBeforeCas) {
            return Err(DurableError::Persistence {
                code: "test_store_before_cas".to_owned(),
                message: "injected storage failure before the physical CAS".to_owned(),
            });
        }
        let receipt = self.inner.compare_and_commit(expected, batch)?;
        if self.observations.advance_after_cas.replace(false) {
            DurableStoreControl::open(self.inner.clone())
                .expect("intervening maintenance opens after the real claim CAS")
                .compact_machine_history(&HistoryCompactionRequest {
                    compaction_id: "history:virtual-claim-interleaving".to_owned(),
                    expected_revision: receipt.revision.clone(),
                    kind: HistoryCompactionKind::EventPrefix,
                    requested_suffix: 0,
                })
                .expect("another legitimate writer advances semantic head without Clock access");
            *self.observations.intervening_head.borrow_mut() = self.inner.load_head()?;
            self.observations.forbid_plan_reads.set(true);
        }
        if self.observations.trip(FaultPoint::StoreAfterCas) {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "injected response loss after the physical CAS".to_owned(),
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

/// Decorates the real filesystem adapter, never the Virtual archive algorithm.
struct FaultResourceStore {
    inner: FsResourceStore,
    observations: Rc<Observations>,
}

impl FaultResourceStore {
    fn observe(&self) {
        self.observations
            .resource_calls
            .set(self.observations.resource_calls.get() + 1);
    }
}

impl ArtifactStore for FaultResourceStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> ResourceResult<ResourceWriteSession> {
        self.observe();
        let session = self.inner.begin_write(intent)?;
        self.observations
            .sessions
            .borrow_mut()
            .push(session.clone());
        Ok(session)
    }

    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        self.observe();
        self.inner.write_chunk(session, offset, bytes)
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.observe();
        if self.observations.trip(FaultPoint::ArchiveBeforeCommit) {
            return Err(ResourceError::Persistence {
                code: "test_archive_before_publish".to_owned(),
                message: "injected failure before immutable Resource publication".to_owned(),
            });
        }
        let publication = self.inner.commit_write(session)?;
        self.observations
            .publications
            .borrow_mut()
            .push(publication.clone());
        if self.observations.trip(FaultPoint::ArchiveAfterCommit) {
            return Err(ResourceError::CommitOutcomeUnknown {
                message: "injected response loss after immutable Resource publication".to_owned(),
            });
        }
        Ok(publication)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.observe();
        self.inner.abort_write(session)
    }

    fn cleanup_receipt(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<Option<ResourceCleanupReceipt>> {
        self.observe();
        self.inner.cleanup_receipt(session)
    }
}

impl ArtifactResolver for FaultResourceStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        self.observe();
        self.inner.stat(resource, locators)
    }

    fn read(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk> {
        self.observe();
        self.inner.read(resource, locators, offset, max_bytes)
    }

    fn list(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> ResourceResult<ResourcePage> {
        self.observe();
        self.inner.list(resource, locators, cursor, limit)
    }
}

impl ResourceCatalogStore for FaultResourceStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        self.observe();
        self.inner.put_catalog_record(record)
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        self.observe();
        self.inner.get_catalog_record(namespace, key)
    }
}

struct CountingClock(Rc<Observations>);

impl ClockObservationAuthority for CountingClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        self.0.clock_calls.set(self.0.clock_calls.get() + 1);
        support::IssuedClock.resolve(reference)
    }
}

impl ExecutionClockAuthority for CountingClock {
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        self.0.clock_calls.set(self.0.clock_calls.get() + 1);
        support::IssuedClock.with_current_head(reference, commit)
    }
}

#[derive(Clone)]
struct CountingPlugin(Rc<Observations>);

impl PluginHost for CountingPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        self.0.plugin_calls.set(self.0.plugin_calls.get() + 1);
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: manifest(),
            }),
            PluginRequest::Call { component, input } if component == "test.virtual-business" => {
                if input.get("work_id").is_some() {
                    self.0.business_calls.set(self.0.business_calls.get() + 1);
                }
                Ok(PluginResponse::CallResult { value: input })
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected test request: {other:?}"
            ))),
        }
    }
}

fn manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "virtual-public-durable-tests@1".to_owned(),
        components: BTreeMap::from([(
            "test.virtual-business".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    }
}

fn binding() -> ExecutionBinding {
    ExecutionBinding::for_local_process(
        &manifest(),
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    )
    .expect("test provider binding seals")
}

fn candidate() -> PlanCandidate {
    let mut candidate = support::identity_candidate("virtual-public-durable");
    candidate.components.push(ComponentContract {
        id: "test.virtual-business".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
        requirements: BTreeMap::new(),
    });
    candidate.definitions[0].body.steps.push(Step {
        id: "call.business".to_owned(),
        operation: Operation::Call {
            component: "test.virtual-business".to_owned(),
            input: Expression::Input,
            bind: Some("result".to_owned()),
        },
    });
    candidate.definitions[0].body.result = Expression::Binding {
        name: "result".to_owned(),
    };
    candidate
}

fn artifact(kind: &str, value: &Value) -> ArtifactRecord {
    let bytes = canonical_bytes(value).expect("fixture value canonicalizes");
    ArtifactRecord {
        reference: artifact_ref(kind, &bytes).expect("fixture Artifact identity derives"),
        bytes,
    }
}

struct BoundedSource {
    region: VirtualRegion,
    page: MaterializedPage,
    observations: Rc<Observations>,
}

impl VirtualRegionSourceProvider for BoundedSource {
    fn source_binding(&self) -> RegionSourceBinding {
        self.region.source.clone()
    }

    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> ProtocolResult<MaterializedPage> {
        assert_eq!(
            region, &self.region,
            "provider receives the exact admitted cursor and source"
        );
        assert!(self.page.items.len() <= limit);
        self.observations
            .source_calls
            .set(self.observations.source_calls.get() + 1);
        Ok(self.page.clone())
    }
}

struct Providers {
    source: BoundedSource,
    archive: ResourceBackedVirtualArchive<FaultResourceStore>,
    observations: Rc<Observations>,
}

impl VirtualProviders for Providers {
    fn region_source(
        &mut self,
        binding: &RegionSourceBinding,
    ) -> ProtocolResult<&mut dyn VirtualRegionSourceProvider> {
        self.observations
            .provider_lookups
            .set(self.observations.provider_lookups.get() + 1);
        assert_eq!(binding, &self.source.source_binding());
        Ok(&mut self.source)
    }

    fn region_migrator(
        &mut self,
        binding: &str,
        revision: &str,
    ) -> ProtocolResult<&mut dyn VirtualRegionMigratorProvider> {
        Err(ProtocolError::Validation(format!(
            "no test migrator {binding}@{revision}"
        )))
    }

    fn archive(
        &mut self,
        binding: &VirtualArchiveBinding,
    ) -> ProtocolResult<&mut dyn VirtualArchiveProvider> {
        self.observations
            .provider_lookups
            .set(self.observations.provider_lookups.get() + 1);
        assert_eq!(binding, &self.archive.archive_binding());
        Ok(&mut self.archive)
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    store: MemoryStore,
    observations: Rc<Observations>,
    scheduler_id: String,
    region: VirtualRegion,
    page: MaterializedPage,
    plan: SealedPlan,
}

impl Fixture {
    fn new(label: &str, work_count: usize) -> Self {
        let store = MemoryStore::new();
        DurableStoreControl::initialize(store.clone()).expect("empty durable domain initializes");
        let source = artifact("test.virtual-source/1", &json!({"source": label}));
        let region = VirtualRegion {
            region_id: format!("region:{label}"),
            run_id: format!("virtual-run:{label}"),
            source: RegionSourceBinding {
                operation: "test.bounded-source".to_owned(),
                binding: "source:public-durable".to_owned(),
                revision: "1".to_owned(),
            },
            source_artifact: source.reference.clone(),
            cursor: cymule_virtual::VirtualCursor {
                version: "test.cursor/1".to_owned(),
                position: "start".to_owned(),
                exhausted: false,
            },
            estimated_total: Some(u64::try_from(work_count).expect("bounded work count")),
        };
        let page = source_page(&region, work_count);
        let fixture = Self {
            directory: tempfile::tempdir().expect("archive directory creates"),
            store,
            observations: Rc::new(Observations::default()),
            scheduler_id: format!("scheduler:{label}"),
            region,
            page,
            plan: seal_plan(candidate()).expect("business Plan seals"),
        };
        let bootstrap_run = format!("bootstrap:{label}");
        let response = fixture
            .runtime()
            .submit(DurableCommand::StartRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: bootstrap_run.clone(),
                candidate: candidate(),
                input: json!({"bootstrap": label}),
                execution: support::execution(&bootstrap_run),
            })
            .expect("public StartRun retains the immutable Plan and binding");
        assert_eq!(
            support::expect_completed_value(response),
            json!({"bootstrap": label})
        );
        fixture
            .commit(&command(VirtualPersistenceOperation::Initialize(
                VirtualInitializationCommand {
                    control_version: VIRTUAL_INITIALIZATION_CONTROL_VERSION.to_owned(),
                    scheduler_id: fixture.scheduler_id.clone(),
                    command_id: format!("initialize:{label}"),
                    limits: FrontierLimits {
                        max_materialized: 4,
                        max_active: 2,
                        max_active_per_run: 2,
                        materialize_batch: 4,
                    },
                    scheduling_policy: SchedulingPolicy::default(),
                    archive: archive_binding(),
                    regions: vec![fixture.region.clone()],
                    runs: vec![VirtualRunDefinition {
                        run_id: fixture.region.run_id.clone(),
                        execution: VirtualRunExecution::Direct {
                            plan_id: fixture.plan.plan_id.clone(),
                        },
                    }],
                    source_artifacts: vec![source],
                },
            )))
            .expect("scheduler initializes through public control");
        fixture
            .commit(&command(VirtualPersistenceOperation::Materialize(
                VirtualMaterializationCommand {
                    control_version: VIRTUAL_MATERIALIZATION_CONTROL_VERSION.to_owned(),
                    scheduler_id: fixture.scheduler_id.clone(),
                    command_id: format!("materialize:{label}"),
                    region_id: fixture.region.region_id.clone(),
                    expected_source: fixture.region.source.clone(),
                    expected_cursor: fixture.region.cursor.clone(),
                },
            )))
            .expect("source page and payloads commit together");
        assert_eq!(fixture.observations.source_calls.get(), 1);
        assert_eq!(fixture.observations.business_calls.get(), 0);
        fixture
    }

    fn runtime(&self) -> DurableRuntimeControl<FaultStore, CountingPlugin> {
        let admission =
            ExecutionBindingAdmission::admit(CountingPlugin(self.observations.clone()), binding())
                .expect("provider admission succeeds before writable open");
        DurableRuntimeControl::open(
            FaultStore {
                inner: self.store.clone(),
                observations: self.observations.clone(),
            },
            admission,
            CountingClock(self.observations.clone()),
        )
        .expect("runtime reopens at the exact current Store head")
    }

    fn providers(&self) -> Providers {
        let store = FsResourceStore::open(self.directory.path(), "fs:public-durable")
            .expect("official Resource filesystem adapter reopens");
        Providers {
            source: BoundedSource {
                region: self.region.clone(),
                page: self.page.clone(),
                observations: self.observations.clone(),
            },
            archive: ResourceBackedVirtualArchive::open(
                FaultResourceStore {
                    inner: store,
                    observations: self.observations.clone(),
                },
                "archive:public-durable",
                "1",
            )
            .expect("official immutable archive generation reopens"),
            observations: self.observations.clone(),
        }
    }

    fn commit(&self, command: &VirtualPersistenceCommand) -> DurableResult<VirtualCommit> {
        let commit = self
            .runtime()
            .virtual_work(&mut self.providers())
            .commit(command)?;
        commit
            .verify_for(command)
            .expect("returned exact command envelope verifies");
        Ok(commit)
    }

    fn current(&self) -> (String, VirtualCurrent) {
        let query = VirtualCurrentQuery {
            scheduler_id: self.scheduler_id.clone(),
            expected_revision: None,
        };
        let read = DurableStoreControl::open(self.store.clone())
            .expect("store-only reader reopens")
            .virtual_read()
            .read_current(&query)
            .expect("exact current reads without providers");
        read.verify_for(&query)
            .expect("current query correlation verifies");
        (
            read.observed_revision,
            read.current.expect("scheduler exists"),
        )
    }

    fn receipt(&self, command: &VirtualPersistenceCommand) -> Option<VirtualPersistenceReceipt> {
        self.retained(command.command_id())
    }

    fn retained(&self, command_id: &str) -> Option<VirtualPersistenceReceipt> {
        let query = VirtualReceiptQuery {
            scheduler_id: self.scheduler_id.clone(),
            command_id: command_id.to_owned(),
            expected_revision: Some(self.current().0),
        };
        let read = DurableStoreControl::open(self.store.clone())
            .expect("store-only reader reopens")
            .virtual_read()
            .read_receipt(&query)
            .expect("exact receipt membership or absence reads");
        read.verify_for(&query)
            .expect("receipt query correlation verifies");
        read.receipt
    }

    fn occurrence(&self, occurrence_id: &str) -> Option<WorkOccurrence> {
        DurableStoreControl::open(self.store.clone())
            .expect("store-only reader reopens")
            .virtual_read()
            .read_occurrence(&self.scheduler_id, occurrence_id, &self.current().0)
            .expect("exact occurrence reads")
            .value
            .map(|leaf| leaf.occurrence)
    }

    fn claim_command(
        &self,
        label: &str,
        slot: &str,
        owner: &str,
    ) -> VirtualClaimPersistenceCommand {
        let clock = support::execution(slot).clock;
        VirtualClaimPersistenceCommand {
            scheduler_id: self.scheduler_id.clone(),
            command: VirtualClaimCommand {
                control_version: VIRTUAL_CLAIM_CONTROL_VERSION.to_owned(),
                command_id: format!("claim:{label}"),
                owner: owner.to_owned(),
                slot_id: clock.scope.clone(),
                execution_binding: binding()
                    .artifact_ref()
                    .expect("admitted binding reference derives"),
                capabilities: BTreeSet::new(),
                clock,
                lease_ttl: 4,
            },
        }
    }

    fn claim(
        &self,
        command: &VirtualClaimPersistenceCommand,
    ) -> DurableResult<VirtualClaimOutcome> {
        let outcome = self
            .runtime()
            .virtual_work(&mut self.providers())
            .claim(command)?;
        outcome
            .verify()
            .expect("closed public claim outcome verifies");
        assert_eq!(outcome.receipt().command, self.claim_persistence(command));
        Ok(outcome)
    }

    fn claim_persistence(
        &self,
        claim: &VirtualClaimPersistenceCommand,
    ) -> VirtualPersistenceCommand {
        assert_eq!(claim.scheduler_id, self.scheduler_id);
        command(VirtualPersistenceOperation::Claim(claim.clone()))
    }

    fn execute(&self, outcome: &VirtualClaimOutcome) -> (ClaimedWork, ArtifactRecord) {
        let VirtualClaimOutcome::Claimed { claim, plan, .. } = outcome else {
            panic!("expected an executable claim")
        };
        assert_eq!(plan.as_ref(), &self.plan);
        let reference = &claim.item.payload;
        let payload = DurableStoreControl::open(self.store.clone())
            .expect("payload reader opens")
            .read_artifact(reference, &self.current().0)
            .expect("claim payload reads at exact revision")
            .value
            .expect("materialization retained the payload in its CAS");
        let input: Value = decode_json(&payload.bytes).expect("payload is strict canonical JSON");
        let result = EmbeddedRuntime::new(CountingPlugin(self.observations.clone()), binding())
            .expect("public worker runtime admits the exact binding")
            .execute(
                plan.as_ref().clone(),
                &input,
                format!("execution:{}", claim.occurrence_id),
            )
            .expect("claimed Plan executes through a real component invocation")
            .into_completed()
            .expect("business Plan completes");
        assert_eq!(result.plan_id, claim.plan_id);
        assert_eq!(result.value, input);
        (
            claim.as_ref().clone(),
            artifact(RESULT_ARTIFACT_KIND, &result.value),
        )
    }

    fn resolution(
        &self,
        label: &str,
        claim: &ClaimedWork,
        clock: ClockObservationRef,
        artifact: ArtifactRecord,
    ) -> VirtualPersistenceCommand {
        command(VirtualPersistenceOperation::Resolve(
            VirtualResolutionPersistenceCommand {
                scheduler_id: self.scheduler_id.clone(),
                command: WorkResolutionCommand {
                    control_version: VIRTUAL_WORK_CONTROL_VERSION.to_owned(),
                    command_id: format!("resolve:{label}"),
                    work_id: claim.item.work_id.clone(),
                    owner: claim.owner.clone(),
                    epoch: claim.epoch,
                    expected_lease_epoch: claim.lease.epoch,
                    clock,
                    resolution: WorkResolution::Succeeded {
                        result: artifact.reference.clone(),
                    },
                },
                artifact: Some(artifact),
            },
        ))
    }

    fn assert_replay(
        &self,
        command: &VirtualPersistenceCommand,
        expected: &VirtualPersistenceReceipt,
    ) {
        let mut runtime = self.runtime();
        let mut providers = self.providers();
        let before = self.observations.effects();
        let head = self.store.clone().load_head().expect("head reads");
        let replay = runtime
            .virtual_work(&mut providers)
            .commit(command)
            .expect("exact command replays after reopen");
        replay
            .verify_for(command)
            .expect("replay envelope verifies");
        assert_eq!(&replay.receipt, expected);
        assert_eq!(replay.committed_revision, None);
        assert_eq!(self.store.clone().load_head().expect("head rereads"), head);
        assert_eq!(
            self.observations.effects(),
            before,
            "replay performs no provider, Clock, business, or CAS operation"
        );
    }

    fn replay_claim(
        &self,
        command: &VirtualClaimPersistenceCommand,
        expected: &VirtualPersistenceReceipt,
    ) -> VirtualClaimOutcome {
        let mut runtime = self.runtime();
        let mut providers = self.providers();
        let before = self.observations.effects();
        let replay = runtime
            .virtual_work(&mut providers)
            .claim(command)
            .expect("claim reopens with its complete verified Plan");
        replay.verify().expect("historical claim outcome verifies");
        assert_eq!(replay.receipt(), expected);
        assert_eq!(
            self.observations.effects(),
            before,
            "claim replay never reacquires a slot, reads Clock, or invokes a provider"
        );
        replay
    }

    fn commit_with_loss(
        &self,
        command: &VirtualPersistenceCommand,
        lose: bool,
    ) -> VirtualPersistenceReceipt {
        if lose {
            self.observations.fault.set(Some(FaultPoint::StoreAfterCas));
            assert!(matches!(
                self.commit(command),
                Err(DurableError::CommitOutcomeUnknown { .. })
            ));
        } else {
            assert!(
                self.commit(command)
                    .expect("fresh typed transition commits")
                    .committed_revision
                    .is_some()
            );
        }
        let receipt = self
            .receipt(command)
            .expect("reopen finds the original committed receipt");
        self.assert_replay(command, &receipt);
        receipt
    }

    fn reject_without_mutation(&self, command: &VirtualPersistenceCommand) {
        let before = self
            .store
            .clone()
            .load_full_audit()
            .expect("before-state audit succeeds");
        let calls = self.observations.effects();
        let error = self
            .commit(command)
            .expect_err("stale or unexpired command must fail");
        assert!(
            matches!(
                error,
                DurableError::IllegalTransition(_) | DurableError::Conflict { .. }
            ),
            "unexpected rejection: {error:?}"
        );
        assert!(
            self.receipt(command).is_none(),
            "a rejected command has no receipt"
        );
        assert_eq!(
            self.store
                .clone()
                .load_full_audit()
                .expect("after-state audit succeeds"),
            before
        );
        assert_eq!(
            self.observations.cas_calls.get(),
            calls[0],
            "stale fence fails before CAS"
        );
        assert_eq!(self.observations.business_calls.get(), calls[5]);
    }
}

fn source_page(region: &VirtualRegion, count: usize) -> MaterializedPage {
    let mut items = Vec::new();
    let mut artifacts = Vec::new();
    for index in 0..count {
        let work_id = format!("work:{}:{index}", region.region_id);
        let payload = artifact("test.virtual-input/1", &json!({"work_id": work_id}));
        items.push(WorkItem {
            work_id,
            region_id: region.region_id.clone(),
            run_id: region.run_id.clone(),
            payload: payload.reference.clone(),
            capability: None,
            priority: 0,
            cost: 1,
        });
        artifacts.push(payload);
    }
    let mut next_cursor = region.cursor.clone();
    "end".clone_into(&mut next_cursor.position);
    next_cursor.exhausted = true;
    MaterializedPage {
        items,
        artifacts,
        next_cursor,
    }
}

fn archive_binding() -> VirtualArchiveBinding {
    VirtualArchiveBinding::new("archive:public-durable", "1")
        .expect("immutable archive binding seals")
}

fn command(operation: VirtualPersistenceOperation) -> VirtualPersistenceCommand {
    VirtualPersistenceCommand::new(operation).expect("typed Virtual command seals")
}

fn claimed(outcome: &VirtualClaimOutcome) -> ClaimedWork {
    let VirtualClaimOutcome::Claimed { claim, .. } = outcome else {
        panic!("expected claimed work")
    };
    claim.as_ref().clone()
}

fn compact_command(
    fixture: &Fixture,
    claim: &ClaimedWork,
    resolved: &VirtualPersistenceReceipt,
) -> VirtualPersistenceCommand {
    command(VirtualPersistenceOperation::Compact(
        VirtualCompactionPersistenceCommand {
            scheduler_id: fixture.scheduler_id.clone(),
            command: VirtualCompactionCommand::new(
                fixture.region.region_id.clone(),
                BTreeSet::from([resolved.receipt_id.clone()]),
                BTreeSet::from([claim.item.work_id.clone()]),
                BTreeSet::from([claim.occurrence_id.clone()]),
                BTreeSet::from([resolved.command.command_id().to_owned()]),
                archive_binding(),
            )
            .expect("public compaction constructor derives the exact content identity"),
        },
    ))
}

fn assert_archive_failure(
    fixture: &Fixture,
    compact: &VirtualPersistenceCommand,
    fault: FaultPoint,
) {
    let before = fixture
        .store
        .clone()
        .load_full_audit()
        .expect("pre-fault state audits");
    fixture.observations.fault.set(Some(fault));
    let error = fixture
        .commit(compact)
        .expect_err("selected real storage boundary fails");
    match fault {
        FaultPoint::ArchiveBeforeCommit => assert!(
            matches!(error, DurableError::Persistence { code, .. } if code == "test_archive_before_publish")
        ),
        FaultPoint::StoreBeforeCas => assert!(
            matches!(error, DurableError::Persistence { code, .. } if code == "test_store_before_cas")
        ),
        FaultPoint::ArchiveAfterCommit | FaultPoint::StoreAfterCas => {
            assert!(matches!(error, DurableError::CommitOutcomeUnknown { .. }));
        }
    }
    assert_eq!(
        fixture.observations.faults_hit.get(),
        1,
        "the selected fault actually fired"
    );
    if fault == FaultPoint::StoreAfterCas {
        assert!(
            fixture.receipt(compact).is_some(),
            "the committed receipt survives lost acknowledgement"
        );
        assert_eq!(fixture.current().1.body.counts.certificates, 1);
        assert_eq!(fixture.current().1.body.counts.hot_occurrences, 0);
    } else {
        assert!(fixture.receipt(compact).is_none());
        assert_eq!(
            fixture
                .store
                .clone()
                .load_full_audit()
                .expect("failed-state audit succeeds"),
            before,
            "archive publication or pre-CAS failure never partially advances the scheduler"
        );
    }
    assert_eq!(fixture.observations.business_calls.get(), 1);
    assert_eq!(fixture.observations.source_calls.get(), 1);
}

fn verify_archive_and_rehydrate(
    fixture: &Fixture,
    compact: &VirtualPersistenceReceipt,
    resolved: &VirtualPersistenceReceipt,
    occurrence: &WorkOccurrence,
) {
    let VirtualPersistenceOutcome::Compacted(compaction) = &compact.outcome else {
        panic!("expected compaction receipt")
    };
    let VirtualPersistenceEvidence::Compacted { archive } = &compact.evidence else {
        panic!("expected archive publication evidence")
    };
    assert_eq!(fixture.current().1.body.counts.hot_work, 0);
    assert_eq!(fixture.current().1.body.counts.hot_occurrences, 0);
    assert_eq!(fixture.current().1.body.counts.certificates, 1);
    assert!(fixture.occurrence(&occurrence.occurrence_id).is_none());
    let mut reads =
        DurableStoreControl::open(fixture.store.clone()).expect("Resource read authority opens");
    let pin = reads
        .resource()
        .pin_current(&compaction.resource_pin.pin.pin_id)
        .expect("compaction-owned Resource pin reads")
        .expect("archive pin committed atomically");
    assert_eq!(pin.pin, compaction.resource_pin.pin);
    assert_eq!(pin.status, ResourcePinStatus::Active);
    assert_eq!(
        reads
            .resource()
            .retention_current(&pin.pin.subject.family.retention_key)
            .expect("physical retention reads")
            .expect("retention exists")
            .active_pin_count,
        1
    );

    let mut reopened = fixture.providers();
    let archived = reopened
        .archive
        .archived_command(
            &archive.publication.resource,
            &virtual_scheduler_journal_id(&fixture.scheduler_id)
                .expect("scheduler journal derives"),
            resolved.command.command_id(),
        )
        .expect("official provider reopens the exact archived receipt");
    assert_eq!(&archived.receipt, resolved);
    assert_eq!(fixture.receipt(&resolved.command).as_ref(), Some(resolved));
    fixture.assert_replay(&resolved.command, resolved);
    let restored = reopened
        .archive
        .rehydrate_occurrence(&archive.publication.resource, &occurrence.occurrence_id)
        .expect("official provider verifies the complete object and selected occurrence proof");
    assert_eq!(&restored.occurrence, occurrence);
    verify_publication_reuse(fixture, &archive.publication);
    rehydrate_with_lost_receipt(fixture, compaction, occurrence);
    fixture.assert_replay(&compact.command, compact);
    assert_eq!(fixture.observations.business_calls.get(), 1);
    assert_eq!(fixture.observations.source_calls.get(), 1);
    fixture
        .store
        .clone()
        .load_full_audit()
        .expect("reopened full durable audit verifies")
        .expect("compacted durable domain remains initialized");
}

fn verify_publication_reuse(fixture: &Fixture, expected: &ResourcePublication) {
    let publications = fixture.observations.publications.borrow();
    assert!(
        !publications.is_empty(),
        "the real filesystem publication occurred"
    );
    assert!(
        publications
            .iter()
            .all(|publication| publication == expected),
        "retries reuse identical immutable publication receipts"
    );
    let sessions = fixture.observations.sessions.borrow();
    assert!(!sessions.is_empty());
    assert!(
        sessions.iter().all(|session| session == &sessions[0]),
        "retry never creates a second archive upload identity"
    );
    let mut store = FsResourceStore::open(fixture.directory.path(), "fs:public-durable")
        .expect("raw Resource adapter reopens");
    let retained = store
        .commit_write(&sessions[0])
        .expect("lost Resource publication reconciles by exact upload identity");
    assert_eq!(&retained, expected);
    store
        .stat(&expected.resource, &expected.locators)
        .expect("immutable physical bytes remain readable");
}

fn rehydrate_with_lost_receipt(
    fixture: &Fixture,
    compaction: &VirtualCompactionReceipt,
    occurrence: &WorkOccurrence,
) {
    let rehydrate = command(VirtualPersistenceOperation::Rehydrate(
        VirtualRehydrationPersistenceCommand {
            scheduler_id: fixture.scheduler_id.clone(),
            command: VirtualRehydrationCommand {
                control_version: VIRTUAL_REHYDRATION_CONTROL_VERSION.to_owned(),
                command_id: format!("rehydrate:{}", fixture.scheduler_id),
                certificate_id: compaction.certificate.certificate_id.clone(),
                occurrence_ids: BTreeSet::from([occurrence.occurrence_id.clone()]),
            },
        },
    ));
    fixture.commit_with_loss(&rehydrate, true);
    assert_eq!(
        fixture.occurrence(&occurrence.occurrence_id).as_ref(),
        Some(occurrence)
    );
    assert_eq!(fixture.current().1.body.counts.hot_occurrences, 1);
    assert_eq!(
        fixture.current().1.body.counts.hot_work,
        0,
        "rehydration does not reschedule terminal work"
    );
}

fn assert_generic_claim_alias_replay(
    fixture: &Fixture,
    claim: &VirtualClaimPersistenceCommand,
    persistence: &VirtualPersistenceCommand,
    expected: &VirtualClaimOutcome,
) {
    let mut runtime = fixture.runtime();
    let mut providers = fixture.providers();
    let replay_head = fixture
        .store
        .clone()
        .load_head()
        .expect("replay head reads");
    let replay_calls = fixture.observations.effects();
    let plan_reads = fixture.observations.plan_reads.get();
    fixture.observations.forbid_plan_reads.set(true);
    let replay = runtime
        .virtual_work(&mut providers)
        .commit(persistence)
        .expect("generic commit replays the exact retained Claim alias");
    fixture.observations.forbid_plan_reads.set(false);
    replay
        .verify_for(persistence)
        .expect("receipt-only Claim alias replay verifies");
    assert_eq!(&replay.receipt, expected.receipt());
    assert_eq!(replay.committed_revision, None);
    assert_eq!(fixture.observations.effects(), replay_calls);
    assert_eq!(fixture.observations.plan_reads.get(), plan_reads);
    assert_eq!(
        fixture.store.clone().load_head().expect("head rereads"),
        replay_head
    );

    let mut reused_claim = claim.clone();
    "worker:claim-facade:other".clone_into(&mut reused_claim.command.owner);
    let reused = fixture.claim_persistence(&reused_claim);
    let mut runtime = fixture.runtime();
    let mut providers = fixture.providers();
    let reused_calls = fixture.observations.effects();
    let reused_head = fixture.store.clone().load_head().expect("head reads");
    assert!(matches!(
        runtime.virtual_work(&mut providers).commit(&reused),
        Err(DurableError::HistoryConflict { code, .. }) if code == "virtual_command_reused"
    ));
    assert_eq!(fixture.observations.effects(), reused_calls);
    assert_eq!(
        fixture.store.clone().load_head().expect("head rereads"),
        reused_head
    );

    let dedicated = fixture.replay_claim(claim, expected.receipt());
    assert_eq!(&dedicated, expected);
}

#[test]
fn fresh_claim_requires_claim_facade_and_generic_commit_only_replays_its_alias() {
    let fixture = Fixture::new("claim-facade", 1);
    let claim = fixture.claim_command("claim-facade", "slot:claim-facade", "worker:claim-facade");
    let persistence = fixture.claim_persistence(&claim);

    let before_state = fixture
        .store
        .clone()
        .load_full_audit()
        .expect("fresh-claim source audits");
    let before_head = fixture.store.clone().load_head().expect("head reads");
    let mut runtime = fixture.runtime();
    let mut providers = fixture.providers();
    let before_calls = fixture.observations.effects();
    let error = runtime
        .virtual_work(&mut providers)
        .commit(&persistence)
        .expect_err("generic commit cannot create a fresh Claim");
    assert_eq!(
        error,
        DurableError::Validation(
            "fresh Virtual Claim requires DurableVirtualControl::claim".to_owned()
        )
    );
    assert_eq!(fixture.observations.effects(), before_calls);
    assert_eq!(
        fixture.store.clone().load_head().expect("head rereads"),
        before_head
    );
    assert_eq!(
        fixture
            .store
            .clone()
            .load_full_audit()
            .expect("rejected generic Claim leaves source auditable"),
        before_state
    );
    assert!(fixture.receipt(&persistence).is_none());

    let mut runtime = fixture.runtime();
    let mut providers = fixture.providers();
    let before_claim_calls = fixture.observations.effects();
    let outcome = runtime
        .virtual_work(&mut providers)
        .claim(&claim)
        .expect("dedicated Claim facade creates the fresh claim");
    outcome
        .verify()
        .expect("fresh closed claim outcome verifies");
    let VirtualClaimOutcome::Claimed {
        claim: claimed,
        plan,
        ..
    } = &outcome
    else {
        panic!("materialized work must produce a claimed outcome")
    };
    assert_eq!(plan.as_ref(), &fixture.plan);
    assert_eq!(claimed.plan_id, plan.plan_id);
    let after_claim_calls = fixture.observations.effects();
    assert_eq!(after_claim_calls[0], before_claim_calls[0] + 1, "one CAS");
    assert_eq!(
        after_claim_calls[4],
        before_claim_calls[4] + 1,
        "one current-Clock guard"
    );
    assert_eq!(
        after_claim_calls[2], before_claim_calls[2],
        "Claim selection performs no Virtual provider lookup"
    );
    assert_eq!(
        after_claim_calls[3], before_claim_calls[3],
        "Claim selection performs no RegionSource call"
    );
    assert_generic_claim_alias_replay(&fixture, &claim, &persistence, &outcome);
}

#[test]
fn fresh_claim_returns_its_pre_cas_plan_after_another_writer_advances_head() {
    let fixture = Fixture::new("claim-head-interleaving", 1);
    let claim_command = fixture.claim_command(
        "claim-head-interleaving",
        "slot:claim-head-interleaving",
        "worker:claim-head-interleaving",
    );
    let before = fixture
        .store
        .clone()
        .load_head()
        .expect("head reads")
        .expect("head exists");
    let cas_calls = fixture.observations.cas_calls.get();
    fixture.observations.advance_after_cas.set(true);
    let outcome = fixture
        .claim(&claim_command)
        .expect("acknowledged claim returns its already verified Plan despite the later writer");
    let VirtualClaimOutcome::Claimed { claim, plan, .. } = &outcome else {
        panic!("eligible work was not claimed")
    };
    assert_eq!(plan.as_ref(), &fixture.plan);
    let after = fixture
        .observations
        .intervening_head
        .borrow()
        .clone()
        .expect("the later writer committed its own exact head");
    assert_eq!(after.sequence, before.sequence + 2);
    assert_eq!(fixture.observations.cas_calls.get(), cas_calls + 1);
    assert_eq!(
        fixture.store.clone().load_head().expect("head rereads"),
        Some(after.clone())
    );
    assert_eq!(
        fixture
            .current()
            .1
            .body
            .frontier
            .active
            .get(&claim.item.work_id),
        Some(claim.as_ref())
    );
    let plan_reads = fixture.observations.plan_reads.get();
    fixture.assert_replay(
        &fixture.claim_persistence(&claim_command),
        outcome.receipt(),
    );
    assert_eq!(
        fixture.observations.plan_reads.get(),
        plan_reads,
        "generic receipt replay must not read an executable Plan"
    );
    fixture.observations.forbid_plan_reads.set(false);
    assert_eq!(
        fixture.replay_claim(&claim_command, outcome.receipt()),
        outcome
    );
    assert_eq!(
        fixture.store.clone().load_head().expect("final head reads"),
        Some(after)
    );
    assert_eq!(fixture.observations.cas_calls.get(), cas_calls + 1);
}

#[test]
fn archive_fault_sweep_never_partially_mutates_scheduler() {
    for (label, fault) in [
        ("archive-clean", None),
        (
            "archive-before-publication",
            Some(FaultPoint::ArchiveBeforeCommit),
        ),
        ("archive-lost-write", Some(FaultPoint::ArchiveAfterCommit)),
        ("archive-before-cas", Some(FaultPoint::StoreBeforeCas)),
        ("archive-lost-cas", Some(FaultPoint::StoreAfterCas)),
    ] {
        let fixture = Fixture::new(label, 1);
        let claim_command =
            fixture.claim_command(label, &format!("slot:{label}"), "worker:archive");
        let outcome = fixture
            .claim(&claim_command)
            .expect("work is claimed through public control");
        let (claim, result) = fixture.execute(&outcome);
        let resolution = fixture.resolution(label, &claim, claim.lease.clock.clone(), result);
        let resolved = fixture.commit_with_loss(&resolution, false);
        let occurrence = fixture
            .occurrence(&claim.occurrence_id)
            .expect("terminal occurrence reads");
        assert_eq!(occurrence.state, WorkOccurrenceState::Succeeded);
        let compact = compact_command(&fixture, &claim, &resolved);
        let before_sequence = fixture
            .store
            .clone()
            .load_head()
            .expect("head reads")
            .expect("initialized head exists")
            .sequence;
        if let Some(fault) = fault {
            assert_archive_failure(&fixture, &compact, fault);
        }
        let retained = fixture.receipt(&compact);
        let committed = fixture
            .commit(&compact)
            .expect("reopened exact compaction completes or replays");
        assert_eq!(committed.committed_revision.is_none(), retained.is_some());
        if let Some(retained) = retained {
            assert_eq!(committed.receipt, retained);
        }
        assert_eq!(fixture.receipt(&compact).as_ref(), Some(&committed.receipt));
        assert_eq!(
            fixture
                .store
                .clone()
                .load_head()
                .expect("head rereads")
                .expect("compacted head exists")
                .sequence,
            before_sequence + 1,
            "archive, pin, certificate, and receipt publish through exactly one successful CAS"
        );
        fixture.assert_replay(&compact, &committed.receipt);
        verify_archive_and_rehydrate(&fixture, &committed.receipt, &resolved, &occurrence);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LostWorkerReceipt {
    None,
    Claim,
    Renewal,
    Recovery,
    Resolution,
}

fn claim_with_lost_receipt(
    fixture: &Fixture,
    command: &VirtualClaimPersistenceCommand,
    lose: bool,
) -> VirtualClaimOutcome {
    let persisted = fixture.claim_persistence(command);
    if lose {
        fixture
            .observations
            .fault
            .set(Some(FaultPoint::StoreAfterCas));
        assert!(matches!(
            fixture.claim(command),
            Err(DurableError::CommitOutcomeUnknown { .. })
        ));
    } else {
        fixture.claim(command).expect("first worker claim commits");
    }
    let retained = fixture
        .receipt(&persisted)
        .expect("claim receipt survives worker restart");
    let replay = fixture.replay_claim(command, &retained);
    fixture.assert_replay(&persisted, &retained);
    replay
}

fn two_workers_claim(
    fixture: &Fixture,
    label: &str,
    loss: LostWorkerReceipt,
) -> (ClaimedWork, ArtifactRecord) {
    let slot_a = format!("slot:{label}:a");
    let slot_b = format!("slot:{label}:b");
    let first_command = fixture.claim_command(&format!("{label}:a"), &slot_a, "worker:a");
    let second_command = fixture.claim_command(&format!("{label}:b"), &slot_b, "worker:b");
    let mut stale_worker = fixture.runtime();
    let first = claim_with_lost_receipt(fixture, &first_command, loss == LostWorkerReceipt::Claim);
    let head = fixture
        .store
        .clone()
        .load_head()
        .expect("first worker head reads");
    let error = stale_worker
        .virtual_work(&mut fixture.providers())
        .claim(&second_command)
        .expect_err("the second worker cannot commit a selection from the old Store head");
    assert!(
        matches!(error, DurableError::Conflict { .. }),
        "unexpected stale-worker failure: {error:?}"
    );
    assert_eq!(
        fixture
            .store
            .clone()
            .load_head()
            .expect("stale-worker head rereads"),
        head
    );
    assert_eq!(fixture.observations.business_calls.get(), 0);
    assert!(
        fixture
            .receipt(&fixture.claim_persistence(&second_command))
            .is_none()
    );
    let second = fixture
        .claim(&second_command)
        .expect("second worker reopens and claims different work");
    // Worker A finishes computation but loses its execution slot before its
    // output can be accepted. Keep its real result for the late-output probes.
    let (first, late_result) = fixture.execute(&first);
    let (second, result) = fixture.execute(&second);
    assert_ne!(first.item.work_id, second.item.work_id);
    assert_ne!(first.lease.resource, second.lease.resource);
    assert_eq!(fixture.current().1.body.frontier.active.len(), 2);
    fixture.commit_with_loss(
        &fixture.resolution(
            &format!("{label}:b"),
            &second,
            second.lease.clock.clone(),
            result,
        ),
        false,
    );
    (first, late_result)
}

fn renew_worker(
    fixture: &Fixture,
    label: &str,
    claim: &ClaimedWork,
    loss: LostWorkerReceipt,
    late_result: &ArtifactRecord,
) -> ClaimedWork {
    let clock = support::execution(&format!("slot:{label}:a")).clock;
    let renewal = command(VirtualPersistenceOperation::RenewLease(
        VirtualLeaseRenewalPersistenceCommand {
            scheduler_id: fixture.scheduler_id.clone(),
            command: VirtualLeaseRenewalCommand {
                control_version: VIRTUAL_LEASE_RENEWAL_CONTROL_VERSION.to_owned(),
                command_id: format!("renew:{label}"),
                work_id: claim.item.work_id.clone(),
                owner: claim.owner.clone(),
                epoch: claim.epoch,
                expected_lease_epoch: claim.lease.epoch,
                clock: clock.clone(),
                lease_ttl: 6,
            },
        },
    ));
    let receipt = fixture.commit_with_loss(&renewal, loss == LostWorkerReceipt::Renewal);
    let VirtualPersistenceOutcome::LeaseRenewed(renewed) = receipt.outcome else {
        panic!("expected renewed lease")
    };
    assert_eq!(renewed.lease.epoch, claim.lease.epoch + 1);
    assert!(renewed.lease.expires_at > claim.lease.expires_at);
    let active = fixture.current().1.body.frontier.active[&claim.item.work_id].clone();
    assert_eq!(active.epoch, claim.epoch);
    assert_eq!(active.lease, renewed.lease);
    assert_eq!(active.execution_binding, claim.execution_binding);
    let stale = fixture.resolution(
        &format!("{label}:old-lease"),
        claim,
        clock,
        late_result.clone(),
    );
    fixture.reject_without_mutation(&stale);
    active
}

fn recovery_command(
    fixture: &Fixture,
    label: &str,
    claim: &ClaimedWork,
    clock: ClockObservationRef,
) -> VirtualPersistenceCommand {
    let error = artifact("test.virtual-error/1", &json!({"reason": "worker-lost"}));
    command(VirtualPersistenceOperation::Recover(
        VirtualRecoveryPersistenceCommand {
            scheduler_id: fixture.scheduler_id.clone(),
            command: VirtualRecoveryCommand {
                control_version: VIRTUAL_RECOVERY_CONTROL_VERSION.to_owned(),
                command_id: format!("recover:{label}"),
                work_id: claim.item.work_id.clone(),
                expected_owner: claim.owner.clone(),
                expected_epoch: claim.epoch,
                expected_lease_epoch: claim.lease.epoch,
                clock,
                resolution: WorkResolution::Retry {
                    error: error.reference.clone(),
                    next_reason: None,
                },
            },
            artifact: error,
        },
    ))
}

fn recover_worker(
    fixture: &Fixture,
    label: &str,
    claim: &ClaimedWork,
    loss: LostWorkerReceipt,
    late_result: &ArtifactRecord,
) {
    let slot = format!("slot:{label}:a");
    let unexpired = support::execution(&slot).clock;
    fixture.reject_without_mutation(&recovery_command(
        fixture,
        &format!("{label}:unexpired"),
        claim,
        unexpired,
    ));
    let mut expired = None;
    for _ in 0..16 {
        let reference = support::execution(&slot).clock;
        if support::IssuedClock
            .resolve(&reference)
            .expect("issued Clock resolves")
            .logical_time
            >= claim.lease.expires_at
        {
            expired = Some(reference);
            break;
        }
    }
    let expired = expired.expect("bounded logical observations reach lease expiry");
    assert_eq!(
        fixture.current().1.body.frontier.active[&claim.item.work_id],
        *claim,
        "Clock expiry alone never changes the durable claim"
    );
    let late = fixture.resolution(
        &format!("{label}:expired"),
        claim,
        expired.clone(),
        late_result.clone(),
    );
    fixture.reject_without_mutation(&late);
    let recovery = recovery_command(fixture, label, claim, expired);
    fixture.commit_with_loss(&recovery, loss == LostWorkerReceipt::Recovery);
    let old = fixture
        .occurrence(&claim.occurrence_id)
        .expect("recovered occurrence remains retained");
    assert_eq!(old.state, WorkOccurrenceState::RetryScheduled);
    assert_eq!(old.epoch, claim.epoch);
    assert_eq!(old.lease_epoch, claim.lease.epoch);
    assert!(
        !fixture
            .current()
            .1
            .body
            .frontier
            .active
            .contains_key(&claim.item.work_id)
    );
}

fn finish_recovered_work(
    fixture: &Fixture,
    label: &str,
    old: &ClaimedWork,
    loss: LostWorkerReceipt,
    late_result: &ArtifactRecord,
) {
    let command = fixture.claim_command(
        &format!("{label}:takeover"),
        &format!("slot:{label}:b"),
        "worker:b",
    );
    let outcome = fixture
        .claim(&command)
        .expect("second worker claims the explicitly recovered work");
    let next = claimed(&outcome);
    assert_eq!(next.item, old.item);
    assert_eq!(next.epoch, old.epoch + 1);
    assert_ne!(next.occurrence_id, old.occurrence_id);
    assert_eq!(next.plan_id, old.plan_id);
    assert_eq!(next.execution_binding, old.execution_binding);
    let mut stale = next.clone();
    stale.epoch = old.epoch;
    stale.occurrence_id.clone_from(&old.occurrence_id);
    fixture.reject_without_mutation(&fixture.resolution(
        &format!("{label}:old-work-epoch"),
        &stale,
        next.lease.clock.clone(),
        late_result.clone(),
    ));
    let (next, result) = fixture.execute(&outcome);
    let resolution = fixture.resolution(
        &format!("{label}:takeover"),
        &next,
        next.lease.clock.clone(),
        result,
    );
    let receipt = fixture.commit_with_loss(&resolution, loss == LostWorkerReceipt::Resolution);
    fixture.assert_replay(&resolution, &receipt);
    assert_eq!(
        fixture
            .occurrence(&old.occurrence_id)
            .expect("old attempt reads")
            .state,
        WorkOccurrenceState::RetryScheduled
    );
    assert_eq!(
        fixture
            .occurrence(&next.occurrence_id)
            .expect("new attempt reads")
            .state,
        WorkOccurrenceState::Succeeded
    );
    assert_eq!(
        fixture.observations.business_calls.get(),
        3,
        "two initial claims and one explicit recovery execute; receipt replay adds no business call"
    );
    assert_eq!(fixture.observations.source_calls.get(), 1);
    let (_, current) = fixture.current();
    assert!(current.body.frontier.active.is_empty());
    assert!(
        current
            .body
            .frontier
            .ready
            .values()
            .all(std::collections::VecDeque::is_empty)
    );
    assert_eq!(current.body.counts.hot_work, 2);
    assert_eq!(current.body.counts.hot_occurrences, 3);
    let empty = fixture.claim_command(
        &format!("{label}:empty"),
        &format!("slot:{label}:b"),
        "worker:b",
    );
    assert!(matches!(
        fixture
            .claim(&empty)
            .expect("empty frontier returns a closed public result"),
        VirtualClaimOutcome::NoWork { .. }
    ));
    assert_eq!(fixture.observations.business_calls.get(), 3);
    fixture
        .store
        .clone()
        .load_full_audit()
        .expect("all retained fences and receipts audit after reopen")
        .expect("worker durable domain remains initialized");
}

#[test]
fn multi_worker_claim_renew_recover_and_late_output_matrix_is_fenced() {
    for (label, loss) in [
        ("workers-clean", LostWorkerReceipt::None),
        ("workers-lost-claim", LostWorkerReceipt::Claim),
        ("workers-lost-renewal", LostWorkerReceipt::Renewal),
        ("workers-lost-recovery", LostWorkerReceipt::Recovery),
        ("workers-lost-result", LostWorkerReceipt::Resolution),
    ] {
        let fixture = Fixture::new(label, 2);
        let (first, late_result) = two_workers_claim(&fixture, label, loss);
        let renewed = renew_worker(&fixture, label, &first, loss, &late_result);
        recover_worker(&fixture, label, &renewed, loss, &late_result);
        finish_recovered_work(&fixture, label, &renewed, loss, &late_result);
        for command_id in [
            format!("claim:{label}:a"),
            format!("renew:{label}"),
            format!("recover:{label}"),
        ] {
            let retained = fixture
                .retained(&command_id)
                .expect("old ownership receipt remains exact");
            fixture.assert_replay(&retained.command, &retained);
            if let VirtualPersistenceOperation::Claim(command) = &retained.command.operation {
                let replay = fixture.replay_claim(command, &retained);
                assert_eq!(
                    claimed(&replay),
                    first,
                    "historical replay never substitutes the takeover claim"
                );
            }
        }
        assert_eq!(
            fixture.observations.faults_hit.get(),
            usize::from(loss != LostWorkerReceipt::None)
        );
    }
}
