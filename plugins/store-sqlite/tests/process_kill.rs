//! Real `SQLite` process-death sweeps across M1, paged terminal, and M4 CAS boundaries.

#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::Duration;

use cymule_clock_system::SqliteClock;
use cymule_core::{
    ArtifactRecord, ArtifactRef, COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Definition,
    DispatchPolicy, EffectContract, EffectProfile, Expression, IR_VERSION,
    MachineCommandArchiveSegment, MutationKind, Operation, PlanCandidate, ReconciliationMode,
    ReconciliationResolution, Region, SealedPlan, Step, WaitSpec, WorldOutcome, artifact_ref,
    canonical_bytes, plan_invocation_id, seal_plan, sha256_bytes,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    ComponentOccurrenceState, CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableBoundary,
    DurableCommand, DurableResponse, DurableResult, DurableRunCurrent, DurableRuntimeControl,
    DurableStore, DurableStoreControl, JournalRecordManifest, MAX_DURABLE_QUERY_PAGE_BYTES,
    OperationAttemptState, OutboxState, StateRootManifest, StateRootResolver, StoreCommit,
    StoreHead, StoreReclamation, StoreStats, StoredState,
};
use cymule_durable_protocol::{ContinuationStatus, ExecutionClaimRequest, execution_clock_scope};
use cymule_profile_protocol::evolution::{
    EVOLUTION_CONTROL_VERSION, EvolutionCommand, EvolutionCommit, EvolutionError,
    EvolutionPersistenceCommand, EvolutionProviders, EvolutionReceiptQuery, EvolutionResult,
    LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand, LiveEvolutionOutcome, MigrationAdapter,
    MigrationAdapterDescriptor, MigrationAdapterRequest, MigrationCapabilityChange,
    MigrationOutput, MigrationPreservation, MigrationRequest, MigrationStateCoverage,
    NoEvolutionProviders, PlanEdge, PlanPatch, PlanTemplate, RolloutDecision, RolloutMode,
    ShadowDriver, analyze_relink, diff_plans,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation,
    PluginRequest, PluginResponse, RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CLOCK_SOURCE: &str = "clock:store-sqlite-process-kill";
const CLOCK_GENERATION: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const CLAIM_TTL: u64 = 1;
const PAGED_RUN_ID: &str = "run:sqlite-paged-terminal-kill";
const PAGED_EFFECT_COUNT: usize = 2;
const EVOLUTION_DOMAIN: &str = "domain:sqlite-evolution-kill";
const EVOLUTION_ID: &str = "evolution:sqlite-process-kill";
const EVOLUTION_RUN_ID: &str = "run:sqlite-evolution-kill";
const EVOLUTION_TEMPLATE_ID: &str = "template:sqlite-evolution-kill";
const EVOLUTION_SIGNAL_KEY: &str = "signal:sqlite-evolution-kill";
const EVOLUTION_ADAPTER_ID: &str = "migration.sqlite-process-kill";
const EVOLUTION_ADAPTER_REVISION: &str =
    "sha256:7777777777777777777777777777777777777777777777777777777777777777";
static PROCESS_DEATH_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillPhase {
    BeforeCommit,
    AfterCommit,
}

impl KillPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before_commit",
            Self::AfterCommit => "after_commit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillSelection {
    EveryCommit,
    PagedTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalPageStage {
    Begin,
    Progress,
    Finalize,
}

impl TerminalPageStage {
    const fn label(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Progress => "progress",
            Self::Finalize => "finalize",
        }
    }
}

struct KillStore {
    inner: SqliteStore,
    phase: KillPhase,
    selection: KillSelection,
    fail_at: usize,
    calls: usize,
    marker: PathBuf,
    trace: Option<Arc<Mutex<Vec<TerminalPageStage>>>>,
}

impl KillStore {
    fn terminal_page_stage(
        &mut self,
        expected: Option<&StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<Option<TerminalPageStage>> {
        let before = match expected {
            Some(head) => {
                self.inner
                    .load_state_root_manifest(&head.state_root_manifest_id)?
                    .ok_or_else(|| cymule_durable::DurableError::Integrity {
                        code: "paged_kill_source_manifest_missing".to_owned(),
                        message: "paged process-kill selection lost its exact source manifest"
                            .to_owned(),
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
            (0, 1) => Some(TerminalPageStage::Begin),
            (1, 1) => Some(TerminalPageStage::Progress),
            (1, 0) => Some(TerminalPageStage::Finalize),
            (0, 0) => None,
            _ => {
                return Err(cymule_durable::DurableError::Validation(
                    "paged process-kill fixture admitted multiple pending commands".to_owned(),
                ));
            }
        })
    }

    fn stop(&self, stage: Option<TerminalPageStage>) -> ! {
        let payload = stage.map_or_else(
            || self.calls.to_string(),
            |stage| format!("{}:{}:{}", self.calls, stage.label(), self.phase.label()),
        );
        fs::write(&self.marker, payload).expect("kill marker writes");
        loop {
            thread::park_timeout(Duration::from_mins(1));
        }
    }
}

impl DurableStore for KillStore {
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
    ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
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
        expected: Option<&cymule_durable::StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let stage = match self.selection {
            KillSelection::EveryCommit => None,
            KillSelection::PagedTerminal => {
                let Some(stage) = self.terminal_page_stage(expected, batch)? else {
                    return self.inner.compare_and_commit(expected, batch);
                };
                Some(stage)
            }
        };
        self.calls += 1;
        if let (Some(stage), Some(trace)) = (stage, &self.trace) {
            trace
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(stage);
        }
        if self.calls != self.fail_at {
            return self.inner.compare_and_commit(expected, batch);
        }
        match self.phase {
            KillPhase::BeforeCommit => self.stop(stage),
            KillPhase::AfterCommit => {
                self.inner.compare_and_commit(expected, batch)?;
                self.stop(stage);
            }
        }
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.inner.stats()
    }
}

struct CountingStore {
    inner: SqliteStore,
    calls: Arc<AtomicUsize>,
    head_loads: Arc<AtomicUsize>,
    full_audits: Arc<AtomicUsize>,
}

impl DurableStore for CountingStore {
    fn load_head(&mut self) -> DurableResult<Option<StoreHead>> {
        self.head_loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load_head()
    }

    fn load_full_audit(&mut self) -> DurableResult<Option<StoredState>> {
        self.full_audits.fetch_add(1, Ordering::SeqCst);
        self.inner.load_full_audit()
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
    ) -> DurableResult<Option<MachineCommandArchiveSegment>> {
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
        expected: Option<&cymule_durable::StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<StoreCommit> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.compare_and_commit(expected, batch)
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(
        &mut self,
        request: &StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<StoreStats> {
        self.inner.stats()
    }
}

struct LedgerPlugin {
    database: PathBuf,
}

impl LedgerPlugin {
    fn initialize(path: &Path) {
        let connection = Connection::open(path).expect("effect ledger opens");
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS effect_ledger (
                    intent_id TEXT PRIMARY KEY NOT NULL,
                    input_json BLOB NOT NULL,
                    dispatch_count INTEGER NOT NULL,
                    reconciliation_count INTEGER NOT NULL DEFAULT 0
                ) STRICT;
                CREATE TABLE IF NOT EXISTS reconciliation_ledger (
                    intent_id TEXT PRIMARY KEY NOT NULL,
                    reconciliation_count INTEGER NOT NULL
                ) STRICT;
                CREATE TABLE IF NOT EXISTS component_ledger (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    call_count INTEGER NOT NULL,
                    return_count INTEGER NOT NULL
                ) STRICT;",
            )
            .expect("effect ledger initializes");
    }

    fn dispatch(&self, intent_id: &str, input: &Value) -> RuntimeResult<()> {
        let connection = Connection::open(&self.database)
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        let bytes = cymule_core::canonical_bytes(input)?;
        let existing: Option<(Vec<u8>, i64)> = connection
            .query_row(
                "SELECT input_json, dispatch_count FROM effect_ledger WHERE intent_id = ?1",
                [intent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        match existing {
            Some((retained, _)) if retained != bytes => Err(RuntimeError::plugin_defect(
                "effect intent was reused with different input",
            )),
            Some(_) => {
                connection
                    .execute(
                        "UPDATE effect_ledger SET dispatch_count = dispatch_count + 1
                         WHERE intent_id = ?1",
                        [intent_id],
                    )
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                Ok(())
            }
            None => {
                connection
                    .execute(
                        "INSERT INTO effect_ledger(
                            intent_id, input_json, dispatch_count, reconciliation_count
                         ) VALUES (?1, ?2, 1, 0)",
                        params![intent_id, bytes],
                    )
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                Ok(())
            }
        }
    }

    fn reconcile(&self, intent_id: &str) -> RuntimeResult<bool> {
        let connection = Connection::open(&self.database)
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO reconciliation_ledger(intent_id, reconciliation_count)
                 VALUES (?1, 1)
                 ON CONFLICT(intent_id) DO UPDATE SET
                    reconciliation_count = reconciliation_count + 1",
                [intent_id],
            )
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        let updated = connection
            .execute(
                "UPDATE effect_ledger
                 SET reconciliation_count = reconciliation_count + 1
                 WHERE intent_id = ?1",
                [intent_id],
            )
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        Ok(updated == 1)
    }

    fn counts(path: &Path) -> (usize, usize) {
        let connection = Connection::open(path).expect("effect ledger opens");
        let dispatches: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(dispatch_count), 0) FROM effect_ledger",
                [],
                |row| row.get(0),
            )
            .expect("effect counts read");
        let reconciliations: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(reconciliation_count), 0)
                 FROM reconciliation_ledger",
                [],
                |row| row.get(0),
            )
            .expect("reconciliation counts read");
        (
            usize::try_from(dispatches).expect("dispatch count is non-negative"),
            usize::try_from(reconciliations).expect("reconcile count is non-negative"),
        )
    }

    fn component_counts(path: &Path, run_id: &str) -> (usize, usize) {
        let connection = Connection::open(path).expect("component ledger opens");
        let counts: Option<(i64, i64)> = connection
            .query_row(
                "SELECT call_count, return_count FROM component_ledger WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .expect("component counts read");
        counts.map_or((0, 0), |(calls, returns)| {
            (
                usize::try_from(calls).expect("component call count is non-negative"),
                usize::try_from(returns).expect("component return count is non-negative"),
            )
        })
    }
}

impl PluginHost for LedgerPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "m1-process-kill-ledger/1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::from([(
                        "test.capture".to_owned(),
                        PluginEffect {
                            implementation_revision: "1".to_owned(),
                            can_reconcile: true,
                        },
                    )]),
                },
            }),
            PluginRequest::PrepareEffect { .. } => Ok(PluginResponse::Prepared),
            PluginRequest::DispatchEffect {
                intent_id,
                attempt,
                input,
                ..
            } => {
                self.dispatch(&intent_id, &input)?;
                Ok(PluginResponse::EffectResult {
                    attempt,
                    outcome: WorldOutcome::Applied,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect {
                intent_id,
                attempt,
                input,
                ..
            } => {
                let applied = self.reconcile(&intent_id)?;
                Ok(PluginResponse::ReconciliationResult {
                    attempt,
                    resolution: if applied {
                        ReconciliationResolution::ResolvedApplied
                    } else {
                        ReconciliationResolution::ResolvedNotApplied
                    },
                    value: applied.then_some(input),
                })
            }
            request @ PluginRequest::Call { .. } => Err(RuntimeError::plugin_defect(format!(
                "unexpected process-kill plugin request {request:?}"
            ))),
        }
    }
}

struct PagedTerminalPlugin {
    database: PathBuf,
    ledger: PathBuf,
    marker: PathBuf,
    phase: KillPhase,
    fail_at: usize,
    trace: Option<Arc<Mutex<Vec<TerminalPageStage>>>>,
}

impl PagedTerminalPlugin {
    fn record_call(&self) -> RuntimeResult<()> {
        let connection = Connection::open(&self.ledger)
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO component_ledger(run_id, call_count, return_count)
                 VALUES (?1, 1, 0)
                 ON CONFLICT(run_id) DO UPDATE SET call_count = call_count + 1",
                [PAGED_RUN_ID],
            )
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        Ok(())
    }

    fn record_return(&self) -> RuntimeResult<()> {
        let connection = Connection::open(&self.ledger)
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        let updated = connection
            .execute(
                "UPDATE component_ledger SET return_count = return_count + 1
                 WHERE run_id = ?1",
                [PAGED_RUN_ID],
            )
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        if updated != 1 {
            return Err(RuntimeError::plugin_defect(
                "paged provider return lost its durable call entry",
            ));
        }
        Ok(())
    }

    fn assert_in_flight(&self) -> RuntimeResult<()> {
        let mut store = SqliteStore::open(&self.database, "domain:paged-terminal-kill")
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
        let stored = store
            .load_full_audit()
            .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?
            .ok_or_else(|| RuntimeError::plugin_defect("paged provider has no durable state"))?;
        let continuation = stored
            .state
            .continuations
            .get(PAGED_RUN_ID)
            .ok_or_else(|| RuntimeError::plugin_defect("paged provider lost its Continuation"))?;
        if continuation.status != ContinuationStatus::Running
            || continuation.execution_claim.is_none()
            || stored.state.operation_attempts.len() != 1
            || stored
                .state
                .operation_attempts
                .values()
                .any(|attempt| attempt.state != OperationAttemptState::Running)
            || stored.state.component_occurrences.len() != 1
            || stored
                .state
                .component_occurrences
                .values()
                .any(|occurrence| occurrence.state != ComponentOccurrenceState::Pending)
            || stored.state.outbox.len() != PAGED_EFFECT_COUNT
            || stored
                .state
                .outbox
                .values()
                .any(|effect| effect.state != OutboxState::Pending)
            || stored
                .state_root_manifest
                .machine_frontier()
                .pending_commands
                .entries
                != 0
            || stored
                .state_root_manifest
                .machine_frontier()
                .paged_transitions
                .entries
                != 0
        {
            return Err(RuntimeError::plugin_defect(
                "paged cancellation did not begin from a real in-flight provider Attempt",
            ));
        }
        Ok(())
    }
}

impl PluginHost for PagedTerminalPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: paged_terminal_manifest(),
            }),
            PluginRequest::PrepareEffect { operation, .. } if operation == "test.pending" => {
                Ok(PluginResponse::Prepared)
            }
            PluginRequest::Call { component, .. } if component == "test.terminal" => {
                self.record_call()?;
                self.assert_in_flight()?;
                let store = KillStore {
                    inner: SqliteStore::open(&self.database, "domain:paged-terminal-kill")
                        .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?,
                    phase: self.phase,
                    selection: KillSelection::PagedTerminal,
                    fail_at: self.fail_at,
                    calls: 0,
                    marker: self.marker.clone(),
                    trace: self.trace.clone(),
                };
                let response = DurableStoreControl::open(store)
                    .and_then(|mut control| control.submit(paged_cancellation_command()))
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                if !matches!(response, DurableResponse::RunCancelled { .. }) {
                    return Err(RuntimeError::plugin_defect(
                        "paged provider cancellation returned another response",
                    ));
                }
                self.record_return()?;
                Ok(PluginResponse::CallResult {
                    value: json!({"late": "after-paged-cancel"}),
                })
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "unexpected paged terminal request {other:?}"
            ))),
        }
    }
}

fn paged_terminal_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "sqlite-paged-terminal-process-kill/1".to_owned(),
        components: BTreeMap::from([(
            "test.terminal".to_owned(),
            PluginOperation {
                implementation_revision: "1".to_owned(),
            },
        )]),
        effects: BTreeMap::from([(
            "test.pending".to_owned(),
            PluginEffect {
                implementation_revision: "1".to_owned(),
                can_reconcile: true,
            },
        )]),
    }
}

fn open_runtime<
    S: DurableStore,
    P: PluginHost,
    C: cymule_durable::ExecutionClockAuthority + 'static,
>(
    store: S,
    plugin: P,
    clock: C,
) -> DurableRuntimeControl<S, P> {
    let admission = cymule_runtime::ExecutionBindingAdmission::from_manifest(plugin, |manifest| {
        ExecutionBinding::for_local_process(
            manifest,
            format!(
                "sha256:{}",
                sha256_bytes(manifest.implementation_id.as_bytes())
            ),
        )
        .map_err(cymule_runtime::RuntimeError::from)
    })
    .expect("ledger execution binding admits");
    DurableRuntimeControl::open(store, admission, clock).expect("runtime opens")
}

fn open_clock(path: &Path) -> SqliteClock {
    SqliteClock::open(path, CLOCK_SOURCE, CLOCK_GENERATION).expect("SQLite clock opens")
}

fn issue_execution(
    clock: &mut SqliteClock,
    run_id: &str,
    owner: impl Into<String>,
) -> ExecutionClaimRequest {
    let observation = clock
        .observe(&execution_clock_scope(run_id).expect("execution Clock scope derives"))
        .expect("execution Clock observation is issued");
    ExecutionClaimRequest {
        owner: owner.into(),
        clock: observation.reference(),
        ttl: CLAIM_TTL,
    }
}

fn start_command(execution: &ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: "run:m1-kill".to_owned(),
        candidate: effect_candidate(),
        input: json!({"message": "process kill"}),
        execution: execution.clone(),
    }
}

fn current_command() -> DurableCommand {
    DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: "run:m1-kill".to_owned(),
        expected_revision: None,
    }
}

fn effect_page_command() -> DurableCommand {
    DurableCommand::RunEffectPage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: "run:m1-kill".to_owned(),
        expected_revision: None,
        cursor: None,
        limit: 256,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    }
}

fn effect_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "m1_process_kill_sweep".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: vec![EffectContract {
            id: "test.capture".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::OnScopeCommit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        }],
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "effect.capture".to_owned(),
                    operation: Operation::Effect {
                        effect: "test.capture".to_owned(),
                        input: Expression::Input,
                        occurrence: "primary".to_owned(),
                        bind: None,
                    },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn paged_terminal_candidate() -> PlanCandidate {
    let mut steps: Vec<_> = (0..PAGED_EFFECT_COUNT)
        .map(|index| Step {
            id: format!("pending.{index}"),
            operation: Operation::Effect {
                effect: "test.pending".to_owned(),
                input: Expression::Literal {
                    value: json!({"ordinal": index}),
                },
                occurrence: "primary".to_owned(),
                bind: None,
            },
        })
        .collect();
    steps.push(Step {
        id: "terminal.call".to_owned(),
        operation: Operation::Call {
            component: "test.terminal".to_owned(),
            input: Expression::Input,
            bind: Some("result".to_owned()),
        },
    });
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "sqlite_paged_terminal_process_kill".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "test.terminal".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
            requirements: BTreeMap::new(),
        }],
        effects: vec![EffectContract {
            id: "test.pending".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            profile: EffectProfile {
                mutation: MutationKind::Mutating,
                dispatch: DispatchPolicy::OnScopeCommit,
                reconciliation: ReconciliationMode::Queryable,
                keyed_idempotency: true,
                irreversible: false,
            },
            requirements: BTreeMap::new(),
        }],
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps,
                result: Expression::Binding {
                    name: "result".to_owned(),
                },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn paged_start_command(execution: &ExecutionClaimRequest) -> DurableCommand {
    DurableCommand::StartRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: PAGED_RUN_ID.to_owned(),
        candidate: paged_terminal_candidate(),
        input: json!({"source": "sqlite-paged-terminal-process-kill"}),
        execution: execution.clone(),
    }
}

fn paged_cancellation_command() -> DurableCommand {
    DurableCommand::CancelRun {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        cancellation_id: "cancel:sqlite-paged-terminal-kill".to_owned(),
        run_id: PAGED_RUN_ID.to_owned(),
        reason: json!({"cause": "sqlite-paged-terminal-process-kill"}),
    }
}

#[test]
fn m1_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_M1_KILL_DB") else {
        return;
    };
    let clock_database =
        PathBuf::from(std::env::var("CYMULE_M1_KILL_CLOCK_DB").expect("Clock database exists"));
    let phase = match std::env::var("CYMULE_M1_KILL_PHASE")
        .expect("kill phase exists")
        .as_str()
    {
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_M1_KILL_AT")
        .expect("kill boundary exists")
        .parse()
        .expect("kill boundary parses");
    let marker = PathBuf::from(std::env::var("CYMULE_M1_KILL_MARKER").expect("kill marker exists"));
    let ledger =
        PathBuf::from(std::env::var("CYMULE_M1_KILL_LEDGER").expect("effect ledger exists"));
    let store = KillStore {
        inner: SqliteStore::open(&database, "domain:m1-kill").expect("durable store opens"),
        phase,
        selection: KillSelection::EveryCommit,
        fail_at,
        calls: 0,
        marker,
        trace: None,
    };
    let mut clock = open_clock(&clock_database);
    let execution = issue_execution(&mut clock, "run:m1-kill", "driver:m1-kill-worker");
    let mut runtime = open_runtime(store, LedgerPlugin { database: ledger }, clock);
    runtime
        .submit(start_command(&execution))
        .expect("worker reaches its selected M1 CAS boundary");
    panic!("M1 kill worker unexpectedly completed");
}

#[test]
fn paged_terminal_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_PAGED_KILL_DB") else {
        return;
    };
    let clock_database = PathBuf::from(
        std::env::var("CYMULE_PAGED_KILL_CLOCK_DB").expect("paged Clock database exists"),
    );
    let ledger =
        PathBuf::from(std::env::var("CYMULE_PAGED_KILL_LEDGER").expect("paged ledger exists"));
    let marker =
        PathBuf::from(std::env::var("CYMULE_PAGED_KILL_MARKER").expect("paged marker exists"));
    let phase = match std::env::var("CYMULE_PAGED_KILL_PHASE")
        .expect("paged kill phase exists")
        .as_str()
    {
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown paged kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_PAGED_KILL_AT")
        .expect("paged kill boundary exists")
        .parse()
        .expect("paged kill boundary parses");
    let mut clock = open_clock(&clock_database);
    let execution = issue_execution(
        &mut clock,
        PAGED_RUN_ID,
        "driver:sqlite-paged-terminal-worker",
    );
    let mut runtime = open_runtime(
        SqliteStore::open(&database, "domain:paged-terminal-kill")
            .expect("paged durable store opens"),
        PagedTerminalPlugin {
            database: PathBuf::from(database),
            ledger,
            marker,
            phase,
            fail_at,
            trace: None,
        },
        clock,
    );
    let result = runtime.submit(paged_start_command(&execution));
    panic!("paged terminal kill worker unexpectedly completed: {result:?}");
}

fn m1_cas_boundary_count() -> usize {
    let baseline = TestWorld::new(0).expect("baseline test world creates");
    let baseline_database = baseline
        .domain()
        .path("durable.sqlite")
        .expect("baseline database path resolves");
    let baseline_clock_database = baseline
        .domain()
        .path("clock.sqlite")
        .expect("baseline Clock database path resolves");
    let baseline_ledger = baseline
        .domain()
        .path("effects.sqlite")
        .expect("baseline ledger path resolves");
    LedgerPlugin::initialize(&baseline_ledger);
    let calls = Arc::new(AtomicUsize::new(0));
    let head_loads = Arc::new(AtomicUsize::new(0));
    let full_audits = Arc::new(AtomicUsize::new(0));
    let mut baseline_clock = open_clock(&baseline_clock_database);
    let baseline_execution = issue_execution(
        &mut baseline_clock,
        "run:m1-kill",
        "driver:m1-kill-baseline",
    );
    let mut runtime = open_runtime(
        CountingStore {
            inner: SqliteStore::open(&baseline_database, "domain:m1-kill")
                .expect("baseline store opens"),
            calls: Arc::clone(&calls),
            head_loads: Arc::clone(&head_loads),
            full_audits: Arc::clone(&full_audits),
        },
        LedgerPlugin {
            database: baseline_ledger,
        },
        baseline_clock,
    );
    assert!(matches!(
        runtime
            .submit(start_command(&baseline_execution))
            .expect("baseline Run completes"),
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    assert!(
        head_loads.load(Ordering::SeqCst) > 0,
        "ordinary runtime reopen must authenticate the bounded Store head"
    );
    assert_eq!(
        full_audits.load(Ordering::SeqCst),
        0,
        "ordinary runtime reopen must not traverse the full durable projection"
    );
    let boundary_count = calls.load(Ordering::SeqCst);
    assert!(boundary_count >= 5, "effect Run crosses all durable stages");
    boundary_count
}

struct M1FaultCase {
    _world: TestWorld,
    database: PathBuf,
    clock_database: PathBuf,
    ledger: PathBuf,
    marker: PathBuf,
}

impl M1FaultCase {
    fn new(phase: &str, fail_at: usize, boundary_count: usize) -> Self {
        let phase_seed = usize::from(phase == "after_commit") * boundary_count + fail_at;
        let world =
            TestWorld::new(u64::try_from(phase_seed).expect("fault-matrix position fits u64"))
                .expect("fault test world creates");
        let database = world
            .domain()
            .path("durable.sqlite")
            .expect("durable database path resolves");
        let clock_database = world
            .domain()
            .path("clock.sqlite")
            .expect("Clock database path resolves");
        let ledger = world
            .domain()
            .path("effects.sqlite")
            .expect("effect ledger path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("kill marker path resolves");
        LedgerPlugin::initialize(&ledger);
        Self {
            _world: world,
            database,
            clock_database,
            ledger,
            marker,
        }
    }

    fn kill_at(&self, phase: &str, fail_at: usize) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("m1_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_M1_KILL_DB", &self.database)
            .env("CYMULE_M1_KILL_CLOCK_DB", &self.clock_database)
            .env("CYMULE_M1_KILL_LEDGER", &self.ledger)
            .env("CYMULE_M1_KILL_PHASE", phase)
            .env("CYMULE_M1_KILL_AT", fail_at.to_string())
            .env("CYMULE_M1_KILL_MARKER", &self.marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
        child
            .wait_for_content(
                &self.marker,
                fail_at.to_string().as_bytes(),
                Duration::from_secs(20),
            )
            .expect("kill worker reaches the selected CAS barrier");
        assert_eq!(
            child.terminate().expect("worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
    }

    fn recover(&self, phase: &str, fail_at: usize) {
        let mut store =
            SqliteStore::open(&self.database, "domain:m1-kill").expect("durable store reopens");
        let initialized = store
            .load_head()
            .expect("bounded durable head reads")
            .is_some();
        let mut clock = open_clock(&self.clock_database);
        let execution = issue_execution(
            &mut clock,
            "run:m1-kill",
            format!("driver:recovery:{phase}:{fail_at}"),
        );
        let mut runtime = open_runtime(
            store,
            LedgerPlugin {
                database: self.ledger.clone(),
            },
            clock,
        );
        let outcome = if initialized {
            recover_m1_after_kill(&mut runtime, &execution)
        } else {
            runtime.submit(start_command(&execution))
        }
        .unwrap_or_else(|error| panic!("recovery converges after {phase} CAS {fail_at}: {error}"));
        assert!(matches!(
            outcome,
            DurableResponse::RunBoundary {
                boundary: DurableBoundary::Completed { .. }
            }
        ));
        assert_m1_recovery(&mut runtime, &self.ledger, phase, fail_at);
        drop(runtime);
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
    }
}

fn recover_m1_after_kill(
    runtime: &mut DurableRuntimeControl<SqliteStore, LedgerPlugin>,
    execution: &ExecutionClaimRequest,
) -> DurableResult<DurableResponse> {
    let DurableResponse::RunCurrent { current, .. } = runtime.submit(current_command())? else {
        return Err(cymule_durable::DurableError::RuntimeDefect {
            code: "process_kill_query_mismatch".to_owned(),
            message: "Run-current query returned another response variant".to_owned(),
        });
    };
    let Some(current) = current else {
        // Genesis can commit before StartRun. The typed Run query, not the
        // physical head's existence, decides whether a Run must be recovered.
        return runtime.submit(start_command(execution));
    };
    match current.continuation_status {
        ContinuationStatus::Running => runtime.submit(DurableCommand::TakeoverRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:m1-kill".to_owned(),
            expected_fence: current.execution_fence,
            execution: execution.clone(),
        }),
        _ => runtime.submit(DurableCommand::ResumeRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:m1-kill".to_owned(),
            execution: execution.clone(),
        }),
    }
}

fn assert_m1_recovery(
    runtime: &mut DurableRuntimeControl<SqliteStore, LedgerPlugin>,
    ledger: &Path,
    phase: &str,
    fail_at: usize,
) {
    let DurableResponse::RunEffectPage { page, .. } = runtime
        .submit(effect_page_command())
        .expect("terminal Effect page reads")
    else {
        panic!("Effect page query returned another response variant");
    };
    assert!(!page.items.is_empty());
    assert!(
        page.items
            .iter()
            .all(|effect| matches!(effect.state, OutboxState::Applied | OutboxState::NotApplied))
    );
    assert!(page.next_cursor.is_none());
    let (dispatches, reconciliations) = LedgerPlugin::counts(ledger);
    assert!(
        dispatches <= 1,
        "provider dispatch must remain at most once for {phase} CAS {fail_at}"
    );
    assert!(reconciliations <= 1);
    if dispatches == 0 {
        assert_eq!(
            reconciliations, 1,
            "a killed post-claim/pre-dispatch window settles only through reconciliation"
        );
    }
}

#[test]
fn every_m1_effect_run_cas_boundary_survives_real_process_death() {
    let _process_test = PROCESS_DEATH_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let boundary_count = m1_cas_boundary_count();

    for phase in ["before_commit", "after_commit"] {
        for fail_at in 1..=boundary_count {
            let case = M1FaultCase::new(phase, fail_at, boundary_count);
            case.kill_at(phase, fail_at);
            case.recover(phase, fail_at);
        }
    }
}

fn submit_paged_cancellation(database: &Path) -> DurableResult<DurableResponse> {
    DurableStoreControl::open(SqliteStore::open(database, "domain:paged-terminal-kill")?)?
        .submit(paged_cancellation_command())
}

fn paged_cancellation_receipt(response: &DurableResponse) -> &cymule_durable::CancellationReceipt {
    let DurableResponse::RunCancelled { receipt } = response else {
        panic!("paged cancellation returned another response: {response:?}")
    };
    receipt.verify().expect("cancellation receipt verifies");
    receipt
}

fn assert_paged_terminal_closure(database: &Path, expected: &cymule_durable::CancellationReceipt) {
    let mut store =
        SqliteStore::open(database, "domain:paged-terminal-kill").expect("paged store reopens");
    let stored = store
        .load_full_audit()
        .expect("complete paged state audits")
        .expect("paged durable state exists");
    assert_eq!(
        stored
            .state
            .cancellation_receipts
            .get(&expected.command.cancellation_id),
        Some(expected)
    );
    let continuation = stored
        .state
        .continuations
        .get(PAGED_RUN_ID)
        .expect("cancelled Continuation exists");
    assert_eq!(continuation.status, ContinuationStatus::Cancelled);
    assert!(continuation.execution_claim.is_none());
    assert_eq!(stored.state.operation_attempts.len(), 1);
    assert!(
        stored
            .state
            .operation_attempts
            .values()
            .all(|attempt| attempt.state == OperationAttemptState::Superseded)
    );
    assert_eq!(stored.state.outbox.len(), PAGED_EFFECT_COUNT);
    assert!(
        stored
            .state
            .outbox
            .values()
            .all(|effect| effect.state == OutboxState::CancelledBeforeRelease)
    );
    assert_eq!(
        stored
            .state_root_manifest
            .machine_frontier()
            .pending_commands
            .entries,
        0
    );
    assert_eq!(
        stored
            .state_root_manifest
            .machine_frontier()
            .paged_transitions
            .entries,
        0
    );
}

fn discover_paged_terminal_boundaries() -> Vec<TerminalPageStage> {
    let world = TestWorld::new(20_001).expect("paged discovery world creates");
    let database = world
        .domain()
        .path("durable.sqlite")
        .expect("paged discovery database path resolves");
    let clock_database = world
        .domain()
        .path("clock.sqlite")
        .expect("paged discovery Clock path resolves");
    let ledger = world
        .domain()
        .path("provider.sqlite")
        .expect("paged discovery ledger path resolves");
    let marker = world
        .domain()
        .path("unused-kill-marker")
        .expect("paged discovery marker path resolves");
    LedgerPlugin::initialize(&ledger);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut clock = open_clock(&clock_database);
    let execution = issue_execution(&mut clock, PAGED_RUN_ID, "driver:paged-discovery");
    let mut runtime = open_runtime(
        SqliteStore::open(&database, "domain:paged-terminal-kill")
            .expect("paged discovery store opens"),
        PagedTerminalPlugin {
            database: database.clone(),
            ledger: ledger.clone(),
            marker,
            phase: KillPhase::BeforeCommit,
            fail_at: usize::MAX,
            trace: Some(Arc::clone(&trace)),
        },
        clock,
    );
    assert!(matches!(
        runtime.submit(paged_start_command(&execution)),
        Err(cymule_durable::DurableError::Conflict { .. })
    ));
    drop(runtime);
    assert_eq!(
        LedgerPlugin::component_counts(&ledger, PAGED_RUN_ID),
        (1, 1)
    );
    let stages = trace
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert!(stages.len() >= 3, "paged cancellation must cross Progress");
    assert_eq!(stages.first(), Some(&TerminalPageStage::Begin));
    assert_eq!(stages.last(), Some(&TerminalPageStage::Finalize));
    assert!(
        stages[1..stages.len() - 1]
            .iter()
            .all(|stage| *stage == TerminalPageStage::Progress)
    );
    let replay = submit_paged_cancellation(&database).expect("completed cancellation replays");
    let receipt = paged_cancellation_receipt(&replay);
    assert_paged_terminal_closure(&database, receipt);
    let exact = submit_paged_cancellation(&database).expect("second exact replay succeeds");
    assert_eq!(exact, replay);
    assert_eq!(
        LedgerPlugin::component_counts(&ledger, PAGED_RUN_ID),
        (1, 1)
    );
    assert_sqlite_integrity(&database);
    assert_sqlite_integrity(&clock_database);
    stages
}

struct PagedTerminalFaultCase {
    _world: TestWorld,
    database: PathBuf,
    clock_database: PathBuf,
    ledger: PathBuf,
    marker: PathBuf,
}

impl PagedTerminalFaultCase {
    fn new(phase: KillPhase, ordinal: usize, boundary_count: usize) -> Self {
        let position = usize::from(phase == KillPhase::AfterCommit)
            .checked_mul(boundary_count)
            .and_then(|offset| offset.checked_add(ordinal))
            .expect("paged fault position fits usize");
        let world =
            TestWorld::new(u64::try_from(30_000 + position).expect("paged fault seed fits u64"))
                .expect("paged fault world creates");
        let database = world
            .domain()
            .path("durable.sqlite")
            .expect("paged fault database path resolves");
        let clock_database = world
            .domain()
            .path("clock.sqlite")
            .expect("paged fault Clock path resolves");
        let ledger = world
            .domain()
            .path("provider.sqlite")
            .expect("paged fault ledger path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("paged fault marker path resolves");
        LedgerPlugin::initialize(&ledger);
        Self {
            _world: world,
            database,
            clock_database,
            ledger,
            marker,
        }
    }

    fn kill_at(&self, phase: KillPhase, ordinal: usize, stage: TerminalPageStage) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("paged_terminal_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_PAGED_KILL_DB", &self.database)
            .env("CYMULE_PAGED_KILL_CLOCK_DB", &self.clock_database)
            .env("CYMULE_PAGED_KILL_LEDGER", &self.ledger)
            .env("CYMULE_PAGED_KILL_MARKER", &self.marker)
            .env("CYMULE_PAGED_KILL_PHASE", phase.label())
            .env("CYMULE_PAGED_KILL_AT", ordinal.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("paged kill worker starts");
        let marker = format!("{}:{}:{}", ordinal, stage.label(), phase.label());
        child
            .wait_for_content(&self.marker, marker.as_bytes(), Duration::from_secs(20))
            .expect("paged worker reaches the selected CAS barrier");
        assert_eq!(
            child.terminate().expect("paged worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_eq!(
            LedgerPlugin::component_counts(&self.ledger, PAGED_RUN_ID),
            (1, 0),
            "SIGKILL must catch a real provider Call before it returns"
        );
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
    }

    fn recover(&self) {
        let converged = submit_paged_cancellation(&self.database)
            .expect("paged cancellation converges after process death");
        let receipt = paged_cancellation_receipt(&converged).clone();
        assert_paged_terminal_closure(&self.database, &receipt);
        let replay = submit_paged_cancellation(&self.database)
            .expect("exact cancellation receipt replays after reopen");
        assert_eq!(replay, converged);
        assert_eq!(
            LedgerPlugin::component_counts(&self.ledger, PAGED_RUN_ID),
            (1, 0),
            "paged recovery must never reinvoke the provider"
        );
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
    }
}

#[test]
fn every_paged_terminal_cas_boundary_survives_real_sqlite_process_death() {
    let _process_test = PROCESS_DEATH_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let stages = discover_paged_terminal_boundaries();
    for phase in [KillPhase::BeforeCommit, KillPhase::AfterCommit] {
        for (index, stage) in stages.iter().copied().enumerate() {
            let ordinal = index + 1;
            let case = PagedTerminalFaultCase::new(phase, ordinal, stages.len());
            case.kill_at(phase, ordinal, stage);
            case.recover();
        }
    }
}

fn assert_sqlite_integrity(path: &Path) {
    let connection = Connection::open(path).expect("durable database opens for integrity probe");
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal mode reads");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    let results = connection
        .prepare("PRAGMA integrity_check")
        .expect("integrity statement prepares")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("integrity check runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("integrity rows read");
    assert_eq!(results, ["ok"]);
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("WAL checkpoint completes");
    let after_checkpoint: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("post-checkpoint integrity check runs");
    assert_eq!(after_checkpoint, "ok");
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvolutionKillCommands {
    catalog: EvolutionPersistenceCommand,
    migration: EvolutionPersistenceCommand,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EvolutionProviderCounts {
    binding_lookup: u64,
    adapter_lookup: u64,
    describe: u64,
    migrate: u64,
}

#[derive(Debug, Clone)]
struct EvolutionProviderLedger {
    database: PathBuf,
}

impl EvolutionProviderLedger {
    fn initialize(database: &Path) {
        let connection = Connection::open(database).expect("Evolution provider ledger opens");
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=FULL;
                 CREATE TABLE provider_calls(
                    operation TEXT PRIMARY KEY,
                    call_count INTEGER NOT NULL CHECK(call_count >= 0)
                 ) STRICT;",
            )
            .expect("Evolution provider ledger initializes");
    }

    fn record(&self, operation: &str) {
        let connection = Connection::open(&self.database).expect("provider ledger opens");
        connection
            .execute(
                "INSERT INTO provider_calls(operation, call_count) VALUES (?1, 1)
                 ON CONFLICT(operation) DO UPDATE SET call_count = call_count + 1",
                [operation],
            )
            .expect("provider call persists");
    }

    fn count(&self, operation: &str) -> u64 {
        let connection = Connection::open(&self.database).expect("provider ledger opens");
        let count: i64 = connection
            .query_row(
                "SELECT call_count FROM provider_calls WHERE operation = ?1",
                [operation],
                |row| row.get(0),
            )
            .optional()
            .expect("provider count reads")
            .unwrap_or(0);
        u64::try_from(count).expect("provider count remains non-negative")
    }

    fn counts(&self) -> EvolutionProviderCounts {
        EvolutionProviderCounts {
            binding_lookup: self.count("binding_lookup"),
            adapter_lookup: self.count("adapter_lookup"),
            describe: self.count("describe"),
            migrate: self.count("migrate"),
        }
    }
}

struct SqliteKillMigrationAdapter {
    descriptor: MigrationAdapterDescriptor,
    state: ArtifactRecord,
    evidence: ArtifactRecord,
    ledger: EvolutionProviderLedger,
}

impl MigrationAdapter for SqliteKillMigrationAdapter {
    fn describe(&mut self) -> EvolutionResult<MigrationAdapterDescriptor> {
        self.ledger.record("describe");
        Ok(self.descriptor.clone())
    }

    fn migrate(&mut self, request: &MigrationAdapterRequest) -> EvolutionResult<MigrationOutput> {
        self.ledger.record("migrate");
        request.verify()?;
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

struct SqliteKillEvolutionProviders {
    target_binding: ExecutionBinding,
    adapter: SqliteKillMigrationAdapter,
    ledger: EvolutionProviderLedger,
}

impl EvolutionProviders for SqliteKillEvolutionProviders {
    fn target_execution_binding(&mut self, plan_id: &str) -> EvolutionResult<ExecutionBinding> {
        self.ledger.record("binding_lookup");
        if plan_id != self.adapter.descriptor.to_plan {
            return Err(EvolutionError::NotFound(format!(
                "unregistered SQLite kill-test target Plan {plan_id}"
            )));
        }
        Ok(self.target_binding.clone())
    }

    fn migration_adapter(
        &mut self,
        adapter_id: &str,
        adapter_revision: &str,
    ) -> EvolutionResult<&mut dyn MigrationAdapter> {
        self.ledger.record("adapter_lookup");
        if adapter_id != self.adapter.descriptor.adapter_id
            || adapter_revision != self.adapter.descriptor.adapter_revision
        {
            return Err(EvolutionError::NotFound(format!(
                "unregistered SQLite kill-test migration adapter {adapter_id}@{adapter_revision}"
            )));
        }
        Ok(&mut self.adapter)
    }

    fn shadow_driver(
        &mut self,
        driver_id: &str,
        driver_revision: &str,
    ) -> EvolutionResult<&mut dyn ShadowDriver> {
        Err(EvolutionError::NotFound(format!(
            "unregistered SQLite kill-test shadow driver {driver_id}@{driver_revision}"
        )))
    }
}

fn evolution_artifact(kind: &str, value: &Value) -> ArtifactRecord {
    let bytes = canonical_bytes(value).expect("Evolution fixture JSON canonicalizes");
    ArtifactRecord {
        reference: artifact_ref(kind, &bytes).expect("Evolution fixture Artifact derives"),
        bytes,
    }
}

fn evolution_source_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: IR_VERSION.to_owned(),
        name: "sqlite-evolution-process-kill".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "wait.signal".to_owned(),
                    operation: Operation::Wait {
                        wait: WaitSpec::Signal {
                            key: EVOLUTION_SIGNAL_KEY.to_owned(),
                            consume_once: true,
                        },
                        bind: None,
                    },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn evolution_persistence(command: LiveEvolutionCommand) -> EvolutionPersistenceCommand {
    EvolutionPersistenceCommand::new(EVOLUTION_ID, command).expect("SQLite Evolution command seals")
}

fn evolution_apply(command_id: &str, command: EvolutionCommand) -> EvolutionPersistenceCommand {
    evolution_persistence(LiveEvolutionCommand::Apply {
        control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: command_id.to_owned(),
        template_id: EVOLUTION_TEMPLATE_ID.to_owned(),
        command: Box::new(command),
    })
}

fn commit_evolution_catalog<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    command: &EvolutionPersistenceCommand,
) -> EvolutionCommit {
    control
        .evolution(&mut NoEvolutionProviders)
        .commit(command)
        .expect("provider-free SQLite Evolution setup commits")
}

fn ledger_execution_binding(database: &Path, process_digest: &str) -> ExecutionBinding {
    let mut plugin = LedgerPlugin {
        database: database.to_owned(),
    };
    let PluginResponse::Manifest { manifest } = plugin
        .invoke(PluginRequest::Describe)
        .expect("ledger provider Describe succeeds")
    else {
        panic!("ledger provider returned a non-manifest Describe response")
    };
    ExecutionBinding::for_local_process(&manifest, process_digest)
        .expect("Evolution fixture execution binding derives")
}

fn evolution_target_binding(database: &Path) -> ExecutionBinding {
    ledger_execution_binding(
        database,
        "sha256:9999999999999999999999999999999999999999999999999999999999999999",
    )
}

fn migration_request(command: &EvolutionPersistenceCommand) -> &MigrationRequest {
    let LiveEvolutionCommand::Apply { command, .. } = &command.command else {
        panic!("migration fixture retained a non-Apply command")
    };
    let EvolutionCommand::Migrate { request, .. } = command.as_ref() else {
        panic!("migration fixture retained another Apply variant")
    };
    request
}

fn evolution_providers(
    command: &EvolutionPersistenceCommand,
    provider_database: &Path,
    effect_database: &Path,
) -> SqliteKillEvolutionProviders {
    let request = migration_request(command);
    let ledger = EvolutionProviderLedger {
        database: provider_database.to_owned(),
    };
    SqliteKillEvolutionProviders {
        target_binding: evolution_target_binding(effect_database),
        adapter: SqliteKillMigrationAdapter {
            descriptor: MigrationAdapterDescriptor {
                adapter_id: request.adapter_id.clone(),
                adapter_revision: request.adapter_revision.clone(),
                from_plan: request.from_plan.clone(),
                to_plan: request.to_plan.clone(),
                plan_edge_id: request.plan_edge_id.clone(),
                compatibility_id: request.compatibility_id.clone(),
                from_schema: "state:sqlite-source".to_owned(),
                to_schema: "state:sqlite-target".to_owned(),
                state_coverage: MigrationStateCoverage::TotalReachableState,
                failure_and_cancellation: MigrationPreservation::Preserved,
                budget_and_ownership: MigrationPreservation::Preserved,
                authority_and_effects: MigrationCapabilityChange::NoWidening,
            },
            state: evolution_artifact(
                "cymule.test-sqlite-migration-state/1",
                &json!({"migrated": true}),
            ),
            evidence: evolution_artifact(
                "cymule.test-sqlite-migration-evidence/1",
                &json!({"verified": true}),
            ),
            ledger: ledger.clone(),
        },
        ledger,
    }
}

fn register_evolution_source(control: &mut DurableStoreControl<SqliteStore>) -> SealedPlan {
    let registered = commit_evolution_catalog(
        control,
        &evolution_persistence(LiveEvolutionCommand::RegisterTemplate {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "register:sqlite-evolution-kill".to_owned(),
            template: PlanTemplate {
                template_id: EVOLUTION_TEMPLATE_ID.to_owned(),
                candidate: evolution_source_candidate(),
                references: Vec::new(),
            },
        }),
    );
    let LiveEvolutionOutcome::TemplateRegistered { linked } = registered.receipt.outcome else {
        panic!("SQLite Evolution template registration returned another outcome")
    };
    linked.plan
}

fn evolution_run_current<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
) -> DurableRunCurrent {
    let query = DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: EVOLUTION_RUN_ID.to_owned(),
        expected_revision: None,
    };
    let response = control
        .submit(query.clone())
        .expect("SQLite Evolution Run current reads");
    response
        .verify_query_for(&query)
        .expect("SQLite Evolution Run query verifies");
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = response
    else {
        panic!("SQLite Evolution Run current is absent")
    };
    *current
}

fn start_ready_evolution_run(
    control: DurableStoreControl<SqliteStore>,
    source_plan: &SealedPlan,
    clock_database: &Path,
    effect_database: &Path,
) -> (DurableStoreControl<SqliteStore>, ArtifactRef, ArtifactRef) {
    let mut clock = open_clock(clock_database);
    let execution = issue_execution(
        &mut clock,
        EVOLUTION_RUN_ID,
        "driver:sqlite-evolution-setup",
    );
    let mut runtime = open_runtime(
        control.into_store(),
        LedgerPlugin {
            database: effect_database.to_owned(),
        },
        clock,
    );
    let response = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: EVOLUTION_RUN_ID.to_owned(),
            candidate: source_plan.candidate.clone(),
            input: json!({"source": "sqlite-evolution-process-kill"}),
            execution,
        })
        .expect("SQLite Evolution source Run reaches its Wait");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = response
    else {
        panic!("SQLite Evolution source Run did not suspend")
    };
    let (store, _) = runtime.into_parts();
    let mut control = DurableStoreControl::open(store).expect("store-only control reopens");
    let response = control
        .submit(DurableCommand::ActivateWait {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            activation_id: "activation:sqlite-evolution-kill".to_owned(),
            source: cymule_durable_protocol::WaitActivationSource::Signal {
                key: EVOLUTION_SIGNAL_KEY.to_owned(),
            },
            wait_ids: BTreeSet::from([wait_id]),
            value: json!({"review": "migrate after the SQLite crash-safe point"}),
        })
        .expect("SQLite Evolution Wait activation commits");
    let DurableResponse::WaitActivated { receipt } = response else {
        panic!("SQLite Evolution activation returned another response")
    };
    let implementation_id = "m1-process-kill-ledger/1";
    let source_binding = ledger_execution_binding(
        effect_database,
        &format!("sha256:{}", sha256_bytes(implementation_id.as_bytes())),
    )
    .artifact_ref()
    .expect("source execution binding reference derives");
    (control, receipt.activation.result, source_binding)
}

fn publish_evolution_target(
    control: &mut DurableStoreControl<SqliteStore>,
    source: &SealedPlan,
    source_binding: &ArtifactRef,
    evidence: ArtifactRef,
) -> (SealedPlan, PlanEdge) {
    let selected = commit_evolution_catalog(
        control,
        &evolution_apply(
            "select:sqlite-evolution-kill:outer",
            EvolutionCommand::SelectOccurrence {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "select:sqlite-evolution-kill:inner".to_owned(),
                occurrence_id: "occurrence:sqlite-evolution-kill".to_owned(),
                selection_id: "selection:sqlite-evolution-kill".to_owned(),
                execution_binding: source_binding.clone(),
            },
        ),
    );
    let LiveEvolutionOutcome::OccurrenceSelected { pin } = selected.receipt.outcome else {
        panic!("SQLite source occurrence selection returned another outcome")
    };
    assert_eq!(pin.plan_id, source.plan_id);

    let mut candidate = source.candidate.clone();
    candidate.definitions[0].body.result = Expression::Literal {
        value: json!("sqlite migration target"),
    };
    let target = seal_plan(candidate).expect("SQLite migration target Plan seals");
    let patched = commit_evolution_catalog(
        control,
        &evolution_apply(
            "patch:sqlite-evolution-kill:outer",
            EvolutionCommand::ApplyPatch {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "patch:sqlite-evolution-kill:inner".to_owned(),
                patch: PlanPatch {
                    from_plan: source.plan_id.clone(),
                    target: target.candidate.clone(),
                    operations: diff_plans(source, &target).expect("SQLite Plan diff derives"),
                    evidence,
                },
            },
        ),
    );
    let LiveEvolutionOutcome::PatchApplied { edge } = patched.receipt.outcome else {
        panic!("SQLite migration patch returned another outcome")
    };
    commit_evolution_catalog(
        control,
        &evolution_apply(
            "rollout:sqlite-evolution-kill:outer",
            EvolutionCommand::SetRollout {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "rollout:sqlite-evolution-kill:inner".to_owned(),
                decision: RolloutDecision {
                    decision_id: "decision:sqlite-evolution-kill".to_owned(),
                    fallback_plan: source.plan_id.clone(),
                    target_plan: target.plan_id.clone(),
                    mode: RolloutMode::Active,
                },
            },
        ),
    );
    (target, edge)
}

fn evolution_catalog_definition() -> Definition {
    Definition {
        id: "catalog_probe".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: Vec::new(),
            result: Expression::Literal {
                value: json!("provider-free catalog mutation"),
            },
        },
    }
}

fn create_evolution_kill_commands(
    database: &Path,
    clock_database: &Path,
    effect_database: &Path,
) -> EvolutionKillCommands {
    LedgerPlugin::initialize(effect_database);
    let store = SqliteStore::open(database, EVOLUTION_DOMAIN).expect("Evolution store opens");
    let mut control = DurableStoreControl::initialize(store).expect("Evolution domain initializes");
    let source = register_evolution_source(&mut control);
    let (mut control, evidence, source_binding) =
        start_ready_evolution_run(control, &source, clock_database, effect_database);
    let source_current = evolution_run_current(&mut control);
    assert_eq!(
        source_current.continuation_status,
        ContinuationStatus::Ready
    );
    let (target, edge) = publish_evolution_target(&mut control, &source, &source_binding, evidence);
    let compatibility = analyze_relink(&source, &target).expect("SQLite relink analyzes");
    assert!(compatibility.is_compatible());
    let request = MigrationRequest {
        migration_id: "migration:sqlite-evolution-kill".to_owned(),
        run_id: EVOLUTION_RUN_ID.to_owned(),
        from_plan: source.plan_id,
        to_plan: target.plan_id,
        plan_edge_id: edge.edge_id,
        compatibility_id: compatibility.compatibility_id,
        expected_source_epoch: source_current.epoch,
        adapter_id: EVOLUTION_ADAPTER_ID.to_owned(),
        adapter_revision: EVOLUTION_ADAPTER_REVISION.to_owned(),
    };
    drop(control);
    EvolutionKillCommands {
        catalog: evolution_persistence(LiveEvolutionCommand::PublishDefinition {
            control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
            command_id: "publish:sqlite-evolution-kill-probe".to_owned(),
            logical_ref: "sqlite.process-kill.catalog".to_owned(),
            definition: evolution_catalog_definition(),
            references: Vec::new(),
        }),
        migration: evolution_apply(
            "migrate:sqlite-evolution-kill:outer",
            EvolutionCommand::Migrate {
                control_version: EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "migrate:sqlite-evolution-kill:inner".to_owned(),
                request: Box::new(request),
            },
        ),
    }
}

fn store_evolution_commands(path: &Path, commands: &EvolutionKillCommands) {
    fs::write(
        path,
        canonical_bytes(commands).expect("Evolution kill commands canonicalize"),
    )
    .expect("Evolution kill commands persist");
}

fn load_evolution_commands(path: &Path) -> EvolutionKillCommands {
    cymule_core::decode_json(&fs::read(path).expect("Evolution kill commands read"))
        .expect("Evolution kill commands decode strictly")
}

fn commit_evolution_sequence<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    commands: &EvolutionKillCommands,
    provider_database: &Path,
    effect_database: &Path,
) -> (EvolutionCommit, EvolutionCommit) {
    let catalog = control
        .evolution(&mut NoEvolutionProviders)
        .commit(&commands.catalog)
        .expect("provider-free catalog command converges");
    let mut providers =
        evolution_providers(&commands.migration, provider_database, effect_database);
    let migration = control
        .evolution(&mut providers)
        .commit(&commands.migration)
        .expect("provider-backed migration converges");
    (catalog, migration)
}

#[test]
fn evolution_process_kill_worker_entry() {
    let Ok(database) = std::env::var("CYMULE_EVOLUTION_KILL_DB") else {
        return;
    };
    let provider_database = PathBuf::from(
        std::env::var("CYMULE_EVOLUTION_KILL_PROVIDER_DB")
            .expect("Evolution provider ledger exists"),
    );
    let effect_database = PathBuf::from(
        std::env::var("CYMULE_EVOLUTION_KILL_EFFECT_DB").expect("Evolution effect ledger exists"),
    );
    let commands_path = PathBuf::from(
        std::env::var("CYMULE_EVOLUTION_KILL_COMMANDS").expect("Evolution commands path exists"),
    );
    let marker = PathBuf::from(
        std::env::var("CYMULE_EVOLUTION_KILL_MARKER").expect("Evolution kill marker exists"),
    );
    let phase = match std::env::var("CYMULE_EVOLUTION_KILL_PHASE")
        .expect("Evolution kill phase exists")
        .as_str()
    {
        "before_commit" => KillPhase::BeforeCommit,
        "after_commit" => KillPhase::AfterCommit,
        phase => panic!("unknown Evolution kill phase {phase}"),
    };
    let fail_at = std::env::var("CYMULE_EVOLUTION_KILL_AT")
        .expect("Evolution kill boundary exists")
        .parse()
        .expect("Evolution kill boundary parses");
    let store = KillStore {
        inner: SqliteStore::open(database, EVOLUTION_DOMAIN).expect("Evolution store opens"),
        phase,
        selection: KillSelection::EveryCommit,
        fail_at,
        calls: 0,
        marker,
        trace: None,
    };
    let mut control = DurableStoreControl::open(store).expect("Evolution control opens");
    let commands = load_evolution_commands(&commands_path);
    let outcome = commit_evolution_sequence(
        &mut control,
        &commands,
        &provider_database,
        &effect_database,
    );
    panic!("Evolution kill worker unexpectedly completed: {outcome:?}");
}

struct EvolutionFaultCase {
    _world: TestWorld,
    database: PathBuf,
    clock_database: PathBuf,
    effect_database: PathBuf,
    provider_database: PathBuf,
    commands_path: PathBuf,
    marker: PathBuf,
}

impl EvolutionFaultCase {
    fn new(seed: u64) -> Self {
        let world = TestWorld::new(seed).expect("Evolution test world creates");
        let database = world
            .domain()
            .path("durable.sqlite")
            .expect("Evolution durable database path resolves");
        let clock_database = world
            .domain()
            .path("clock.sqlite")
            .expect("Evolution Clock database path resolves");
        let effect_database = world
            .domain()
            .path("effects.sqlite")
            .expect("Evolution effect database path resolves");
        let provider_database = world
            .domain()
            .path("providers.sqlite")
            .expect("Evolution provider database path resolves");
        let commands_path = world
            .domain()
            .path("commands.json")
            .expect("Evolution command path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("Evolution marker path resolves");
        EvolutionProviderLedger::initialize(&provider_database);
        let commands = create_evolution_kill_commands(&database, &clock_database, &effect_database);
        store_evolution_commands(&commands_path, &commands);
        Self {
            _world: world,
            database,
            clock_database,
            effect_database,
            provider_database,
            commands_path,
            marker,
        }
    }

    fn provider_ledger(&self) -> EvolutionProviderLedger {
        EvolutionProviderLedger {
            database: self.provider_database.clone(),
        }
    }

    fn kill_at(&self, phase: KillPhase, fail_at: usize) {
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .args([
                "--exact",
                "evolution_process_kill_worker_entry",
                "--nocapture",
            ])
            .env("CYMULE_EVOLUTION_KILL_DB", &self.database)
            .env("CYMULE_EVOLUTION_KILL_PROVIDER_DB", &self.provider_database)
            .env("CYMULE_EVOLUTION_KILL_EFFECT_DB", &self.effect_database)
            .env("CYMULE_EVOLUTION_KILL_COMMANDS", &self.commands_path)
            .env("CYMULE_EVOLUTION_KILL_MARKER", &self.marker)
            .env("CYMULE_EVOLUTION_KILL_PHASE", phase.label())
            .env("CYMULE_EVOLUTION_KILL_AT", fail_at.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("Evolution kill worker starts");
        child
            .wait_for_content(
                &self.marker,
                fail_at.to_string().as_bytes(),
                Duration::from_secs(20),
            )
            .expect("Evolution worker reaches the selected CAS barrier");
        assert_eq!(
            child
                .terminate()
                .expect("Evolution worker is reaped")
                .signal(),
            Some(9)
        );
        assert!(child.is_reaped());
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
        assert_sqlite_integrity(&self.provider_database);
    }

    fn recover(&self, phase: KillPhase, fail_at: usize, boundary_count: usize) {
        let commands = load_evolution_commands(&self.commands_path);
        let ledger = self.provider_ledger();
        let calls_before_reopen = ledger.counts();
        let store = SqliteStore::open(&self.database, EVOLUTION_DOMAIN)
            .expect("Evolution store reopens after SIGKILL");
        let mut control = DurableStoreControl::open(store).expect("Evolution control reopens");
        let (catalog, migration) = commit_evolution_sequence(
            &mut control,
            &commands,
            &self.provider_database,
            &self.effect_database,
        );
        catalog
            .verify_for(&commands.catalog)
            .expect("recovered catalog receipt verifies");
        migration
            .verify_for(&commands.migration)
            .expect("recovered migration receipt verifies");
        let calls_after_convergence = ledger.counts();
        if phase == KillPhase::AfterCommit && fail_at == 1 {
            assert_eq!(catalog.committed_revision, None);
        }
        if phase == KillPhase::AfterCommit && fail_at == boundary_count {
            assert_eq!(calls_before_reopen, EvolutionProviderCounts::once());
            assert_eq!(calls_after_convergence, calls_before_reopen);
            assert_eq!(migration.committed_revision, None);
        }
        let expected = if phase == KillPhase::BeforeCommit && fail_at == boundary_count {
            EvolutionProviderCounts::twice()
        } else {
            EvolutionProviderCounts::once()
        };
        assert_eq!(calls_after_convergence, expected);
        assert_evolution_receipt(&mut control, &commands.catalog, &catalog);
        assert_evolution_receipt(&mut control, &commands.migration, &migration);
        let current = evolution_run_current(&mut control);
        assert_eq!(
            current.plan_id,
            migration_request(&commands.migration).to_plan
        );

        let (catalog_replay, migration_replay) = commit_evolution_sequence(
            &mut control,
            &commands,
            &self.provider_database,
            &self.effect_database,
        );
        assert_eq!(catalog_replay.committed_revision, None);
        assert_eq!(migration_replay.committed_revision, None);
        assert_eq!(catalog_replay.receipt, catalog.receipt);
        assert_eq!(migration_replay.receipt, migration.receipt);
        assert_eq!(ledger.counts(), calls_after_convergence);
        let mut store = control.into_store();
        store
            .load_full_audit()
            .expect("recovered Evolution store fully audits")
            .expect("recovered Evolution state exists");
        drop(store);
        assert_sqlite_integrity(&self.database);
        assert_sqlite_integrity(&self.clock_database);
        assert_sqlite_integrity(&self.provider_database);
    }
}

impl EvolutionProviderCounts {
    const fn once() -> Self {
        Self {
            binding_lookup: 1,
            adapter_lookup: 1,
            describe: 1,
            migrate: 1,
        }
    }

    const fn twice() -> Self {
        Self {
            binding_lookup: 2,
            adapter_lookup: 2,
            describe: 2,
            migrate: 2,
        }
    }
}

fn assert_evolution_receipt<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    command: &EvolutionPersistenceCommand,
    commit: &EvolutionCommit,
) {
    let retained = control
        .evolution(&mut NoEvolutionProviders)
        .read_receipt(&EvolutionReceiptQuery {
            evolution_id: EVOLUTION_ID.to_owned(),
            command_id: command.command.command_id().to_owned(),
            expected_revision: None,
        })
        .expect("exact Evolution receipt reads after reopen");
    assert_eq!(retained.receipt.as_ref(), Some(&commit.receipt));
}

fn evolution_cas_boundary_count() -> usize {
    let case = EvolutionFaultCase::new(20_000);
    let calls = Arc::new(AtomicUsize::new(0));
    let head_loads = Arc::new(AtomicUsize::new(0));
    let full_audits = Arc::new(AtomicUsize::new(0));
    let store = CountingStore {
        inner: SqliteStore::open(&case.database, EVOLUTION_DOMAIN)
            .expect("baseline Evolution store opens"),
        calls: Arc::clone(&calls),
        head_loads: Arc::clone(&head_loads),
        full_audits: Arc::clone(&full_audits),
    };
    let commands = load_evolution_commands(&case.commands_path);
    let mut control = DurableStoreControl::open(store).expect("baseline Evolution control opens");
    let (catalog, migration) = commit_evolution_sequence(
        &mut control,
        &commands,
        &case.provider_database,
        &case.effect_database,
    );
    catalog
        .verify_for(&commands.catalog)
        .expect("baseline catalog receipt verifies");
    migration
        .verify_for(&commands.migration)
        .expect("baseline migration receipt verifies");
    assert_eq!(
        case.provider_ledger().counts(),
        EvolutionProviderCounts::once()
    );
    assert_evolution_receipt(&mut control, &commands.catalog, &catalog);
    assert_evolution_receipt(&mut control, &commands.migration, &migration);
    assert_eq!(full_audits.load(Ordering::SeqCst), 0);
    let mut store = control.into_store();
    store
        .load_full_audit()
        .expect("baseline Evolution store fully audits")
        .expect("baseline Evolution state exists");
    assert_eq!(full_audits.load(Ordering::SeqCst), 1);
    assert!(head_loads.load(Ordering::SeqCst) > 0);
    drop(store);
    let boundary_count = calls.load(Ordering::SeqCst);
    assert_eq!(
        boundary_count, 2,
        "the catalog mutation and migration each own one terminal CAS"
    );
    assert_sqlite_integrity(&case.database);
    assert_sqlite_integrity(&case.clock_database);
    assert_sqlite_integrity(&case.provider_database);
    boundary_count
}

#[test]
fn evolution_catalog_and_migration_survive_every_sqlite_cas_process_death() {
    let _process_test = PROCESS_DEATH_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let boundary_count = evolution_cas_boundary_count();
    for phase in [KillPhase::BeforeCommit, KillPhase::AfterCommit] {
        for fail_at in 1..=boundary_count {
            let phase_offset = usize::from(phase == KillPhase::AfterCommit) * boundary_count;
            let seed = 21_000_u64
                + u64::try_from(phase_offset + fail_at).expect("Evolution fault position fits u64");
            let case = EvolutionFaultCase::new(seed);
            case.kill_at(phase, fail_at);
            case.recover(phase, fail_at, boundary_count);
        }
    }
}
