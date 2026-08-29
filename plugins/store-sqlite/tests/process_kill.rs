//! Real process-death sweep across every M1 effect Run CAS boundary.

#![cfg(unix)]

use std::collections::BTreeMap;
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
    Definition, DispatchPolicy, EffectContract, EffectProfile, Expression,
    MachineCommandArchiveSegment, MutationKind, Operation, PlanCandidate, ReconciliationMode,
    ReconciliationResolution, Region, Step, WorldOutcome, sha256_bytes,
};
use cymule_durable::{
    ApplicationJournalPrefix, ApplicationJournalPrefixReplacementAuthority,
    CoupledCheckpointReceipt, DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand,
    DurableResponse, DurableResult, DurableRuntimeControl, DurableStore, JournalRecordManifest,
    MAX_DURABLE_QUERY_PAGE_BYTES, OutboxState, StateRootManifest, StateRootResolver, StoreCommit,
    StoreHead, StoreReclamation, StoreStats, StoredState,
};
use cymule_durable_protocol::{ContinuationStatus, ExecutionClaimRequest, execution_clock_scope};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

const CLOCK_SOURCE: &str = "clock:store-sqlite-process-kill";
const CLOCK_GENERATION: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const CLAIM_TTL: u64 = 1;
static PROCESS_DEATH_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KillPhase {
    BeforeCommit,
    AfterCommit,
}

struct KillStore {
    inner: SqliteStore,
    phase: KillPhase,
    fail_at: usize,
    calls: usize,
    marker: PathBuf,
}

impl KillStore {
    fn stop(&self) -> ! {
        fs::write(&self.marker, self.calls.to_string()).expect("kill marker writes");
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
        self.calls += 1;
        if self.calls != self.fail_at {
            return self.inner.compare_and_commit(expected, batch);
        }
        match self.phase {
            KillPhase::BeforeCommit => self.stop(),
            KillPhase::AfterCommit => {
                self.inner.compare_and_commit(expected, batch)?;
                self.stop();
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
        fail_at,
        calls: 0,
        marker,
    };
    let mut clock = open_clock(&clock_database);
    let execution = issue_execution(&mut clock, "run:m1-kill", "driver:m1-kill-worker");
    let mut runtime = open_runtime(store, LedgerPlugin { database: ledger }, clock);
    runtime
        .submit(start_command(&execution))
        .expect("worker reaches its selected M1 CAS boundary");
    panic!("M1 kill worker unexpectedly completed");
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
