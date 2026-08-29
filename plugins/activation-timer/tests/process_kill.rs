//! Real process-death witnesses for timer selection, M1 activation, and acknowledgement.

#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_activation_timer::{Clock, SqliteTimerDriver};
use cymule_clock_system::SqliteClock;
use cymule_core::{
    Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec, content_id,
};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableResponse, DurableResult,
    DurableRunCurrent, DurableRunItem, DurableRunItemSelector, DurableRuntimeControl, DurableStore,
    DurableWaitSummary, MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES, MAX_DURABLE_QUERY_PAGE_BYTES,
    ParkedWaitView, StoreBatch, StoreCommit, StoreHead, WaitAdmissionOutcome, WaitCondition,
    WaitDelivery, WaitSourceDelivery, WaitSourceDriver, WaitState,
};
use cymule_durable_protocol::{
    ContinuationStatus, ExecutionClaimRequest, WAIT_RESULT_ARTIFACT_KIND,
    WaitActivationDisposition, WaitActivationSource, execution_clock_scope,
};
use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::Connection;
use serde_json::json;

const RUN_ID: &str = "run:timer-process-kill";
const ACTIVATION_ID: &str = "activation:timer-process-kill";
const TIMER_ID: &str = "timer:process-kill";
const CLOCK_SOURCE_ID: &str = "clock:timer-process-kill";
const CLOCK_SOURCE_GENERATION: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

type TimerRuntime = DurableRuntimeControl<SqliteStore, EmptyPlugin>;

struct TimerScenario {
    _world: TestWorld,
    state_database: PathBuf,
    timer_database: PathBuf,
    clock_database: PathBuf,
    marker: PathBuf,
}

#[derive(Clone, Copy)]
struct FixedClock(u64);

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        Ok(self.0)
    }
}

struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "timer-process-kill@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            request => Err(RuntimeError::plugin_defect(format!(
                "unexpected timer process-kill request {request:?}"
            ))),
        }
    }
}

fn open_runtime<S: DurableStore>(
    store: S,
    clock_database: impl AsRef<Path>,
) -> DurableRuntimeControl<S, EmptyPlugin> {
    let admission = cymule_runtime::ExecutionBindingAdmission::for_local_process(
        EmptyPlugin,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("execution binding creates");
    DurableRuntimeControl::open(
        store,
        admission,
        SqliteClock::open(clock_database, CLOCK_SOURCE_ID, CLOCK_SOURCE_GENERATION)
            .expect("issued Clock opens"),
    )
    .expect("runtime opens")
}

fn execution_request(clock_database: impl AsRef<Path>) -> ExecutionClaimRequest {
    let scope = execution_clock_scope(RUN_ID).expect("Run Clock scope derives");
    let observation = SqliteClock::open(clock_database, CLOCK_SOURCE_ID, CLOCK_SOURCE_GENERATION)
        .expect("issued Clock opens")
        .observe(&scope)
        .expect("Clock observation is issued and retained");
    ExecutionClaimRequest {
        owner: content_id("cymule.test-driver/1", &RUN_ID).expect("execution owner seals"),
        clock: observation.reference(),
        ttl: 1,
    }
}

fn start_waiting_run<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    input: serde_json::Value,
    execution: ExecutionClaimRequest,
) -> String {
    match control
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: RUN_ID.to_owned(),
            candidate: candidate(),
            input,
            execution,
        })
        .expect("timer Run starts")
    {
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Suspended { wait_id },
        } => wait_id,
        response => panic!("timer Run returned unexpected start boundary {response:?}"),
    }
}

fn run_current<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
) -> DurableRunCurrent {
    match control
        .submit(DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: RUN_ID.to_owned(),
            expected_revision: None,
        })
        .expect("timer Run current query succeeds")
    {
        DurableResponse::RunCurrent {
            current: Some(current),
            ..
        } => *current,
        response => panic!("timer Run current query returned {response:?}"),
    }
}

fn wait_current<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    wait_id: &str,
) -> WaitCondition {
    match control
        .submit(DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: RUN_ID.to_owned(),
            expected_revision: None,
            selector: DurableRunItemSelector::Wait {
                wait_id: wait_id.to_owned(),
            },
            max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
        })
        .expect("timer exact wait query succeeds")
    {
        DurableResponse::RunItem {
            item: Some(item), ..
        } => match *item {
            DurableRunItem::Wait { wait } => *wait,
            item => panic!("timer exact wait query returned {item:?}"),
        },
        response => panic!("timer exact wait query returned {response:?}"),
    }
}

fn only_wait<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
) -> DurableWaitSummary {
    match control
        .submit(DurableCommand::RunWaitPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: RUN_ID.to_owned(),
            expected_revision: None,
            cursor: None,
            limit: 2,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        })
        .expect("timer Run wait page query succeeds")
    {
        DurableResponse::RunWaitPage { page, .. } => {
            let [wait] = page.items.as_slice() else {
                panic!("timer fixture Run does not own exactly one wait")
            };
            wait.clone()
        }
        response => panic!("timer Run wait page query returned {response:?}"),
    }
}

fn resume_completed_run<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    execution: ExecutionClaimRequest,
) {
    assert!(matches!(
        control
            .submit(DurableCommand::ResumeRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: RUN_ID.to_owned(),
                execution,
            })
            .expect("timer Run resumes"),
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. },
        }
    ));
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "timer_process_kill".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            body: Region {
                steps: vec![Step {
                    id: "wait.timer".to_owned(),
                    operation: Operation::Wait {
                        wait: WaitSpec::Timer {
                            timer_id: TIMER_ID.to_owned(),
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

#[derive(Clone, Copy)]
enum DriverBarrier {
    AfterSelection,
    BeforeAcknowledgement,
    AfterAcknowledgement,
}

struct BarrierDriver {
    inner: SqliteTimerDriver<FixedClock>,
    marker: PathBuf,
    phase: DriverBarrier,
}

struct KillStore {
    inner: SqliteStore,
    marker: PathBuf,
    after_commit: bool,
}

impl KillStore {
    fn stop(&self) -> ! {
        let boundary = if self.after_commit {
            "after_activation_commit"
        } else {
            "before_activation_commit"
        };
        fs::write(&self.marker, boundary).expect("activation CAS barrier writes");
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
    ) -> DurableResult<Option<cymule_durable::StateRootManifest>> {
        self.inner.load_state_root_manifest(manifest_id)
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &cymule_durable::StateRootManifest,
        read: impl FnOnce(&mut dyn cymule_durable::StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        self.inner.with_state_root_resolver(current, read)
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<cymule_durable::ApplicationJournalPrefix> {
        self.inner
            .application_journal_prefix(manifest, journal_id, count)
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<cymule_durable::JournalRecordManifest>> {
        self.inner
            .application_journal_record_manifest(manifest, journal_id, record_id)
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<cymule_durable::ApplicationJournalPrefixReplacementAuthority>> {
        self.inner
            .application_journal_prefix_replacement_authority(manifest, replacement_id)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<cymule_durable::CoupledCheckpointReceipt>> {
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
        if !self.after_commit {
            self.stop();
        }
        self.inner.compare_and_commit(expected, batch)?;
        self.stop()
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<cymule_durable::StoreStats> {
        self.inner.stats()
    }
}

impl BarrierDriver {
    fn stop(&self, boundary: &str) -> ! {
        fs::write(&self.marker, boundary).expect("timer barrier writes");
        loop {
            thread::park_timeout(Duration::from_mins(1));
        }
    }
}

impl WaitSourceDriver for BarrierDriver {
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitSourceDelivery>> {
        let delivery = self.inner.receive(view, max_targets)?;
        if delivery.is_some() && matches!(self.phase, DriverBarrier::AfterSelection) {
            self.stop("after_selection");
        }
        Ok(delivery)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        if matches!(self.phase, DriverBarrier::BeforeAcknowledgement) {
            self.stop("after_activation_before_ack");
        }
        self.inner.acknowledge(activation_id)?;
        self.stop("after_ack");
    }
}

struct RecordingDriver<D> {
    inner: D,
    delivery: Option<WaitDelivery>,
}

impl<D> RecordingDriver<D> {
    const fn new(inner: D) -> Self {
        Self {
            inner,
            delivery: None,
        }
    }
}

impl<D: WaitSourceDriver> WaitSourceDriver for RecordingDriver<D> {
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitSourceDelivery>> {
        let delivery = self.inner.receive(view, max_targets)?;
        self.delivery = delivery
            .as_ref()
            .map(|delivery| delivery.delivery().clone());
        Ok(delivery)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        self.inner.acknowledge(activation_id)
    }
}

#[test]
fn timer_process_kill_worker_entry() {
    let Ok(state_database) = std::env::var("CYMULE_TIMER_KILL_STATE_DB") else {
        return;
    };
    let timer_database =
        std::env::var("CYMULE_TIMER_KILL_TIMER_DB").expect("timer database exists");
    let clock_database =
        PathBuf::from(std::env::var("CYMULE_TIMER_KILL_CLOCK_DB").expect("Clock path exists"));
    let marker = PathBuf::from(std::env::var("CYMULE_TIMER_KILL_MARKER").expect("marker exists"));
    let mode = std::env::var("CYMULE_TIMER_KILL_MODE").expect("kill mode exists");
    let mut driver = SqliteTimerDriver::open_with_clock(timer_database, FixedClock(100))
        .expect("timer driver opens");
    match mode.as_str() {
        "after_schedule" => {
            driver
                .schedule(ACTIVATION_ID, TIMER_ID, 100, &json!({"due": true}))
                .expect("timer schedules");
            fs::write(marker, "after_schedule").expect("schedule barrier writes");
            loop {
                thread::park_timeout(Duration::from_mins(1));
            }
        }
        "after_selection" => {
            let mut runtime = open_runtime(
                SqliteStore::open(state_database, "domain:timer-process-kill")
                    .expect("durable store opens"),
                &clock_database,
            );
            let mut barrier = BarrierDriver {
                inner: driver,
                marker,
                phase: DriverBarrier::AfterSelection,
            };
            runtime
                .drive_wait_source(&mut barrier, 1)
                .expect("timer selection reaches its process-death barrier");
            panic!("timer kill worker unexpectedly passed its selection barrier");
        }
        "before_activation_commit" | "after_activation_commit" => {
            let after_commit = mode == "after_activation_commit";
            let mut runtime = open_runtime(
                KillStore {
                    inner: SqliteStore::open(state_database, "domain:timer-process-kill")
                        .expect("durable store opens"),
                    marker,
                    after_commit,
                },
                &clock_database,
            );
            runtime
                .drive_wait_source(&mut driver, 1)
                .expect("worker reaches activation CAS barrier");
            panic!("timer kill worker unexpectedly passed its CAS barrier");
        }
        "after_activation_before_ack" | "after_ack" => {
            let mut runtime = open_runtime(
                SqliteStore::open(state_database, "domain:timer-process-kill")
                    .expect("durable store opens"),
                &clock_database,
            );
            let phase = if mode == "after_activation_before_ack" {
                DriverBarrier::BeforeAcknowledgement
            } else {
                DriverBarrier::AfterAcknowledgement
            };
            let mut barrier = BarrierDriver {
                inner: driver,
                marker,
                phase,
            };
            runtime
                .drive_wait_source(&mut barrier, 1)
                .expect("worker reaches acknowledgement barrier");
            panic!("timer kill worker unexpectedly passed its barrier");
        }
        mode => panic!("unknown timer kill mode {mode}"),
    }
}

#[test]
fn timer_selection_activation_and_ack_survive_real_process_death() {
    for (seed, mode) in [
        (1, "after_schedule"),
        (2, "after_selection"),
        (3, "before_activation_commit"),
        (4, "after_activation_commit"),
        (5, "after_activation_before_ack"),
        (6, "after_ack"),
    ] {
        run_timer_kill_scenario(seed, mode);
    }
}

fn run_timer_kill_scenario(seed: u64, mode: &str) {
    let scenario = prepare_timer_scenario(seed, mode);
    kill_timer_worker(&scenario, mode);
    recover_timer_scenario(&scenario, mode);
}

fn prepare_timer_scenario(seed: u64, mode: &str) -> TimerScenario {
    let world = TestWorld::new(seed).expect("timer test world creates");
    let state_database = world
        .domain()
        .path("state.sqlite")
        .expect("state path resolves");
    let timer_database = world
        .domain()
        .path("timer.sqlite")
        .expect("timer path resolves");
    let clock_database = world
        .domain()
        .path("clock.sqlite")
        .expect("Clock path resolves");
    let marker = world
        .domain()
        .path("kill-ready")
        .expect("marker path resolves");
    let mut runtime = open_runtime(
        SqliteStore::open(&state_database, "domain:timer-process-kill")
            .expect("durable store opens"),
        &clock_database,
    );
    let wait_id = start_waiting_run(
        &mut runtime,
        json!({"case": mode}),
        execution_request(&clock_database),
    );
    assert_eq!(
        wait_current(&mut runtime, &wait_id).state,
        WaitState::Pending
    );
    drop(runtime);
    if mode != "after_schedule" {
        let mut driver = SqliteTimerDriver::open_with_clock(&timer_database, FixedClock(100))
            .expect("timer driver opens");
        driver
            .schedule(ACTIVATION_ID, TIMER_ID, 100, &json!({"due": true}))
            .expect("timer schedules");
    }
    TimerScenario {
        _world: world,
        state_database,
        timer_database,
        clock_database,
        marker,
    }
}

fn kill_timer_worker(scenario: &TimerScenario, mode: &str) {
    let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
    command
        .arg("--exact")
        .arg("timer_process_kill_worker_entry")
        .arg("--nocapture")
        .env("CYMULE_TIMER_KILL_STATE_DB", &scenario.state_database)
        .env("CYMULE_TIMER_KILL_TIMER_DB", &scenario.timer_database)
        .env("CYMULE_TIMER_KILL_CLOCK_DB", &scenario.clock_database)
        .env("CYMULE_TIMER_KILL_MARKER", &scenario.marker)
        .env("CYMULE_TIMER_KILL_MODE", mode)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = ManagedChild::spawn(&mut command).expect("timer worker starts");
    child
        .wait_for_content(&scenario.marker, mode.as_bytes(), Duration::from_secs(20))
        .expect("timer worker reaches exact barrier");
    assert_eq!(
        fs::read_to_string(&scenario.marker).expect("barrier reads"),
        mode
    );
    assert_eq!(
        child.terminate().expect("timer worker reaps").signal(),
        Some(9)
    );
    assert!(child.is_reaped());
}

fn recover_timer_scenario(scenario: &TimerScenario, mode: &str) {
    assert_sqlite_integrity(&scenario.state_database);
    assert_sqlite_integrity(&scenario.timer_database);
    assert_sqlite_integrity(&scenario.clock_database);
    let mut reopened = open_runtime(
        SqliteStore::open(&scenario.state_database, "domain:timer-process-kill")
            .expect("durable store reopens"),
        &scenario.clock_database,
    );
    let committed = matches!(
        mode,
        "after_activation_commit" | "after_activation_before_ack" | "after_ack"
    );
    assert_activation_closure(&mut reopened, committed);
    recover_timer_delivery(&mut reopened, &scenario.timer_database, mode);
    complete_timer_run(&mut reopened, &scenario.clock_database);
}

fn recover_timer_delivery(reopened: &mut TimerRuntime, timer_database: &Path, mode: &str) {
    let timer =
        SqliteTimerDriver::open_with_clock(timer_database, FixedClock(100)).expect("timer reopens");
    let mut timer = RecordingDriver::new(timer);
    let wait_id = only_wait(reopened).wait_id;
    let expected_delivery = WaitDelivery {
        activation_id: ACTIVATION_ID.to_owned(),
        source: WaitActivationSource::Timer {
            timer_id: TIMER_ID.to_owned(),
        },
        wait_ids: BTreeSet::from([wait_id]),
        value: json!({"due": true}),
    };
    if mode == "after_ack" {
        assert!(
            reopened
                .drive_wait_source(&mut timer, 1)
                .expect("timer polls")
                .is_none(),
            "acknowledged timer must not redeliver"
        );
        assert!(timer.delivery.is_none());
        return;
    }
    assert_eq!(
        reopened
            .drive_wait_source(&mut timer, 1)
            .expect("activation converges"),
        Some(WaitAdmissionOutcome {
            disposition: WaitActivationDisposition::Applied,
            ready_run_ids: BTreeSet::from([RUN_ID.to_owned()]),
        })
    );
    assert_eq!(timer.delivery.as_ref(), Some(&expected_delivery));
}

fn complete_timer_run(reopened: &mut TimerRuntime, clock_database: &Path) {
    assert_activation_closure(reopened, true);
    resume_completed_run(reopened, execution_request(clock_database));
    let current = run_current(reopened);
    assert_eq!(current.epoch, 1);
    assert_eq!(current.continuation_status, ContinuationStatus::Completed);
}

fn assert_activation_closure<S: DurableStore>(
    runtime: &mut DurableRuntimeControl<S, EmptyPlugin>,
    committed: bool,
) {
    let current = run_current(runtime);
    assert_eq!(
        current.continuation_status,
        if committed {
            ContinuationStatus::Ready
        } else {
            ContinuationStatus::Waiting
        }
    );
    let wait = only_wait(runtime);
    assert_eq!(
        wait.state,
        if committed {
            WaitState::Completed
        } else {
            WaitState::Pending
        }
    );
    let expected_result = cymule_core::artifact_ref(
        WAIT_RESULT_ARTIFACT_KIND,
        &cymule_core::canonical_bytes(&json!({"due": true})).expect("wait value canonicalizes"),
    )
    .expect("wait result identifies");
    assert_eq!(wait.result.as_ref(), committed.then_some(&expected_result));
}

fn assert_sqlite_integrity(path: &Path) {
    let connection = Connection::open(path).expect("SQLite database opens for integrity probe");
    let results = connection
        .prepare("PRAGMA integrity_check")
        .expect("integrity statement prepares")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("SQLite integrity check runs")
        .collect::<Result<Vec<_>, _>>()
        .expect("integrity rows read");
    assert_eq!(results, ["ok"]);
}
