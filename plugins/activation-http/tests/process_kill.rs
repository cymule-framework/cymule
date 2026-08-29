//! Real process-death witnesses for HTTP ingress, selection, M1 activation, and acknowledgement.

#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use cymule_activation_http::{AllowAll, SqliteHttpSignalDriver, durable_signal_router};
use cymule_clock_system::SqliteClock;
use cymule_core::{Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec};
use cymule_durable::{
    DURABLE_CONTROL_VERSION, DurableBoundary, DurableCommand, DurableError, DurableResponse,
    DurableResult, DurableRunCurrent, DurableRunItem, DurableRunItemSelector,
    DurableRuntimeControl, DurableStore, DurableWaitSummary,
    MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES, MAX_DURABLE_QUERY_PAGE_BYTES, ParkedWaitView,
    StoreBatch, StoreCommit, StoreHead, WaitAdmissionOutcome, WaitCondition, WaitDelivery,
    WaitSourceDriver, WaitState,
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
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;
use tower::ServiceExt;

const RUN_ID: &str = "run:http-process-kill";
const PEER_RUN_ID: &str = "run:http-broadcast-peer";
const ACTIVATION_ID: &str = "activation:http-process-kill";
const SIGNAL_KEY: &str = "signal:http-process-kill";
const CLOCK_SOURCE_ID: &str = "clock:http-process-kill";
const CLOCK_SOURCE_GENERATION: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

type HttpRuntime = DurableRuntimeControl<SqliteStore, EmptyPlugin>;

struct HttpScenario {
    _world: TestWorld,
    state_database: PathBuf,
    spool_database: PathBuf,
    clock_database: PathBuf,
    marker: PathBuf,
}

struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "http-process-kill@1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            request => Err(RuntimeError::plugin_defect(format!(
                "unexpected HTTP process-kill request {request:?}"
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
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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
    execution_request_for(clock_database, RUN_ID)
}

fn execution_request_for(clock_database: impl AsRef<Path>, run_id: &str) -> ExecutionClaimRequest {
    let scope = execution_clock_scope(run_id).expect("Run Clock scope derives");
    let observation = SqliteClock::open(clock_database, CLOCK_SOURCE_ID, CLOCK_SOURCE_GENERATION)
        .expect("issued Clock opens")
        .observe(&scope)
        .expect("Clock observation is issued and retained");
    ExecutionClaimRequest {
        owner: "driver:http-process-kill".to_owned(),
        clock: observation.reference(),
        ttl: 1,
    }
}

fn start_waiting_run<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    candidate: PlanCandidate,
    input: serde_json::Value,
    run_id: &str,
    execution: ExecutionClaimRequest,
) -> String {
    match control
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate,
            input,
            execution,
        })
        .expect("HTTP Run starts")
    {
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Suspended { wait_id },
        } => wait_id,
        response => panic!("HTTP Run returned unexpected start boundary {response:?}"),
    }
}

fn run_current<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    run_id: &str,
) -> DurableRunCurrent {
    match control
        .submit(DurableCommand::RunCurrent {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
        })
        .expect("HTTP Run current query succeeds")
    {
        DurableResponse::RunCurrent {
            current: Some(current),
            ..
        } => *current,
        response => panic!("HTTP Run current query returned {response:?}"),
    }
}

fn wait_current<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    run_id: &str,
    wait_id: &str,
) -> WaitCondition {
    match control
        .submit(DurableCommand::RunItem {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
            selector: DurableRunItemSelector::Wait {
                wait_id: wait_id.to_owned(),
            },
            max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
        })
        .expect("HTTP exact wait query succeeds")
    {
        DurableResponse::RunItem {
            item: Some(item), ..
        } => match *item {
            DurableRunItem::Wait { wait } => *wait,
            item => panic!("HTTP exact wait query returned {item:?}"),
        },
        response => panic!("HTTP exact wait query returned {response:?}"),
    }
}

fn only_wait<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    run_id: &str,
) -> DurableWaitSummary {
    match control
        .submit(DurableCommand::RunWaitPage {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            expected_revision: None,
            cursor: None,
            limit: 2,
            max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
        })
        .expect("HTTP Run wait page query succeeds")
    {
        DurableResponse::RunWaitPage { page, .. } => {
            let [wait] = page.items.as_slice() else {
                panic!("HTTP fixture Run does not own exactly one wait")
            };
            wait.clone()
        }
        response => panic!("HTTP Run wait page query returned {response:?}"),
    }
}

fn resume_completed_run<S: DurableStore>(
    control: &mut DurableRuntimeControl<S, EmptyPlugin>,
    run_id: &str,
    execution: ExecutionClaimRequest,
) {
    assert!(matches!(
        control
            .submit(DurableCommand::ResumeRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                run_id: run_id.to_owned(),
                execution,
            })
            .expect("HTTP Run resumes"),
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. },
        }
    ));
}

fn candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "http_process_kill".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            body: Region {
                steps: vec![Step {
                    id: "wait.signal".to_owned(),
                    operation: Operation::Wait {
                        wait: WaitSpec::Signal {
                            key: SIGNAL_KEY.to_owned(),
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

fn broadcast_candidate() -> PlanCandidate {
    let mut candidate = candidate();
    "http_broadcast_terminal_race".clone_into(&mut candidate.name);
    let Operation::Wait {
        wait: WaitSpec::Signal { consume_once, .. },
        ..
    } = &mut candidate.definitions[0].body.steps[0].operation
    else {
        panic!("HTTP fixture should contain one signal wait");
    };
    *consume_once = false;
    candidate
}

fn request(ok: bool) -> Request<Body> {
    Request::post("/v1/signals")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"activation_id":"{ACTIVATION_ID}","key":"{SIGNAL_KEY}","value":{{"ok":{ok}}}}}"#
        )))
        .expect("HTTP request builds")
}

#[derive(Clone, Copy)]
enum DriverBarrier {
    AfterSelection,
    BeforeAcknowledgement,
    AfterAcknowledgement,
}

struct BarrierDriver {
    inner: SqliteHttpSignalDriver,
    marker: PathBuf,
    phase: DriverBarrier,
}

impl BarrierDriver {
    fn stop(&self, boundary: &str) -> ! {
        fs::write(&self.marker, boundary).expect("HTTP acknowledgement barrier writes");
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
    ) -> DurableResult<Option<WaitDelivery>> {
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
    interrupt_after_selection: bool,
}

impl<D> RecordingDriver<D> {
    fn new(inner: D) -> Self {
        Self {
            inner,
            delivery: None,
            interrupt_after_selection: false,
        }
    }

    fn interrupt_after_selection(inner: D) -> Self {
        Self {
            inner,
            delivery: None,
            interrupt_after_selection: true,
        }
    }

    fn resume_after_selection(&mut self) {
        self.interrupt_after_selection = false;
    }
}

impl<D: WaitSourceDriver> WaitSourceDriver for RecordingDriver<D> {
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        let delivery = self.inner.receive(view, max_targets)?;
        self.delivery.clone_from(&delivery);
        if self.interrupt_after_selection && delivery.is_some() {
            return Err(DurableError::Substrate {
                code: "test_wait_selection_interrupted".to_owned(),
                message: "test interrupted after durable target selection and before M1 CAS"
                    .to_owned(),
            });
        }
        Ok(delivery)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        self.inner.acknowledge(activation_id)
    }
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
        fs::write(&self.marker, boundary).expect("HTTP activation CAS barrier writes");
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

#[tokio::test]
async fn http_process_kill_worker_entry() {
    let Ok(state_database) = std::env::var("CYMULE_HTTP_KILL_STATE_DB") else {
        return;
    };
    let spool_database =
        PathBuf::from(std::env::var("CYMULE_HTTP_KILL_SPOOL_DB").expect("spool path exists"));
    let clock_database =
        PathBuf::from(std::env::var("CYMULE_HTTP_KILL_CLOCK_DB").expect("Clock path exists"));
    let marker = PathBuf::from(std::env::var("CYMULE_HTTP_KILL_MARKER").expect("marker exists"));
    let mode = std::env::var("CYMULE_HTTP_KILL_MODE").expect("kill mode exists");
    let (router, driver) =
        durable_signal_router(&spool_database, 8, AllowAll).expect("HTTP source opens");
    let response = tokio::spawn(router.oneshot(request(true)));

    if mode == "after_ingress" {
        wait_for_spooled_request(&spool_database).await;
        assert!(
            !response.is_finished(),
            "ingress cannot acknowledge before M1"
        );
        fs::write(marker, "after_ingress").expect("HTTP ingress barrier writes");
        loop {
            thread::park_timeout(Duration::from_mins(1));
        }
    }

    match mode.as_str() {
        "after_selection" => {
            let mut runtime = open_runtime(
                SqliteStore::open(state_database, "domain:http-process-kill")
                    .expect("durable store opens"),
                &clock_database,
            );
            let mut driver = BarrierDriver {
                inner: driver,
                marker,
                phase: DriverBarrier::AfterSelection,
            };
            assert!(!response.is_finished(), "selection cannot acknowledge HTTP");
            loop {
                runtime
                    .drive_wait_source(&mut driver, 1)
                    .expect("HTTP source reaches selection barrier");
                tokio::task::yield_now().await;
            }
        }
        "before_activation_commit" | "after_activation_commit" => {
            let after_commit = mode == "after_activation_commit";
            let mut runtime = open_runtime(
                KillStore {
                    inner: SqliteStore::open(state_database, "domain:http-process-kill")
                        .expect("durable store opens"),
                    marker,
                    after_commit,
                },
                &clock_database,
            );
            let mut driver = driver;
            loop {
                if runtime
                    .drive_wait_source(&mut driver, 1)
                    .expect("HTTP source drive reaches CAS barrier")
                    .is_some()
                {
                    panic!("HTTP kill worker unexpectedly passed its CAS barrier");
                }
                tokio::task::yield_now().await;
            }
        }
        "after_activation_before_ack" | "after_ack" => {
            let mut runtime = open_runtime(
                SqliteStore::open(state_database, "domain:http-process-kill")
                    .expect("durable store opens"),
                &clock_database,
            );
            let phase = if mode == "after_activation_before_ack" {
                DriverBarrier::BeforeAcknowledgement
            } else {
                DriverBarrier::AfterAcknowledgement
            };
            let mut driver = BarrierDriver {
                inner: driver,
                marker,
                phase,
            };
            loop {
                if runtime
                    .drive_wait_source(&mut driver, 1)
                    .expect("HTTP source drive reaches acknowledgement barrier")
                    .is_some()
                {
                    panic!("HTTP kill worker unexpectedly passed its acknowledgement barrier");
                }
                tokio::task::yield_now().await;
            }
        }
        mode => panic!("unknown HTTP kill mode {mode}"),
    }
}

#[tokio::test]
async fn selected_broadcast_delivery_acks_after_one_target_cancels_and_reopens() {
    let world = TestWorld::new(17).expect("HTTP test world creates");
    let state_database = world
        .domain()
        .path("state.sqlite")
        .expect("state path resolves");
    let spool_database = world
        .domain()
        .path("http.sqlite")
        .expect("spool path resolves");
    let clock_database = world
        .domain()
        .path("clock.sqlite")
        .expect("Clock path resolves");
    let mut runtime = open_runtime(
        SqliteStore::open(&state_database, "domain:http-process-kill")
            .expect("durable store opens"),
        &clock_database,
    );
    let wait_ids = [RUN_ID, PEER_RUN_ID]
        .into_iter()
        .map(|run_id| {
            let wait_id = start_waiting_run(
                &mut runtime,
                broadcast_candidate(),
                json!({"run_id": run_id}),
                run_id,
                execution_request_for(&clock_database, run_id),
            );
            (run_id.to_owned(), wait_id)
        })
        .collect::<BTreeMap<_, _>>();
    let selected_wait_ids =
        BTreeSet::from([wait_ids[RUN_ID].clone(), wait_ids[PEER_RUN_ID].clone()]);
    let (router, source) =
        durable_signal_router(&spool_database, 8, AllowAll).expect("HTTP source opens");
    let response = tokio::spawn(router.oneshot(request(true)));
    let mut source = RecordingDriver::interrupt_after_selection(source);
    loop {
        match runtime.drive_wait_source(&mut source, 2) {
            Ok(None) => tokio::task::yield_now().await,
            Err(DurableError::Substrate { code, .. })
                if code == "test_wait_selection_interrupted" =>
            {
                break;
            }
            outcome => panic!("HTTP selection barrier returned {outcome:?}"),
        }
    }
    let selected = source
        .delivery
        .as_ref()
        .expect("HTTP source retained one exact selection")
        .clone();
    assert_eq!(
        selected,
        WaitDelivery {
            activation_id: ACTIVATION_ID.to_owned(),
            source: WaitActivationSource::Signal {
                key: SIGNAL_KEY.to_owned(),
            },
            wait_ids: selected_wait_ids.clone(),
            value: json!({"ok": true}),
        }
    );
    assert!(!response.is_finished(), "selection is not acknowledgement");
    cancel_selected_run(&mut runtime);
    source.resume_after_selection();
    let admitted = loop {
        let driven = runtime
            .drive_wait_source(&mut source, 2)
            .expect("retained mixed delivery admits and acknowledges");
        if let Some(outcome) = driven {
            break outcome;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(
        admitted,
        WaitAdmissionOutcome {
            disposition: WaitActivationDisposition::Applied,
            ready_run_ids: BTreeSet::from([PEER_RUN_ID.to_owned()]),
        }
    );
    assert_eq!(
        source.delivery.as_ref(),
        Some(&selected),
        "redelivery after cancellation must preserve the complete original provider selection"
    );
    while !response.is_finished() {
        assert!(
            runtime
                .drive_wait_source(&mut source, 2)
                .expect("post-ack waiter drain succeeds")
                .is_none()
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(
        response
            .await
            .expect("HTTP response task joins")
            .expect("HTTP response completes")
            .status(),
        StatusCode::ACCEPTED
    );
    assert_mixed_broadcast_state(&mut runtime, &wait_ids);
    let (store, _) = runtime.into_parts();
    drop(source);
    assert_broadcast_reopen(store, &clock_database, &spool_database, &wait_ids).await;
}

fn cancel_selected_run(runtime: &mut HttpRuntime) {
    assert!(matches!(
        runtime
            .submit(DurableCommand::CancelRun {
                control_version: DURABLE_CONTROL_VERSION.to_owned(),
                cancellation_id: "cancel:http-broadcast-race".to_owned(),
                run_id: RUN_ID.to_owned(),
                reason: json!({"code": "operator_cancelled"}),
            })
            .expect("selected Run cancellation commits"),
        DurableResponse::RunCancelled { .. }
    ));
}

fn assert_mixed_broadcast_state(runtime: &mut HttpRuntime, wait_ids: &BTreeMap<String, String>) {
    assert_eq!(
        wait_current(runtime, RUN_ID, &wait_ids[RUN_ID]).state,
        WaitState::Cancelled
    );
    assert_eq!(
        wait_current(runtime, PEER_RUN_ID, &wait_ids[PEER_RUN_ID]).state,
        WaitState::Completed
    );
    assert_eq!(
        run_current(runtime, RUN_ID).continuation_status,
        ContinuationStatus::Cancelled
    );
    assert_eq!(
        run_current(runtime, PEER_RUN_ID).continuation_status,
        ContinuationStatus::Ready
    );
}

async fn assert_broadcast_reopen(
    store: SqliteStore,
    clock_database: &Path,
    spool_database: &Path,
    wait_ids: &BTreeMap<String, String>,
) {
    let mut reopened = open_runtime(store, clock_database);
    let (router, source) =
        durable_signal_router(spool_database, 8, AllowAll).expect("HTTP source reopens");
    assert_eq!(
        router
            .oneshot(request(true))
            .await
            .expect("acknowledged request replays")
            .status(),
        StatusCode::ACCEPTED
    );
    let mut source = RecordingDriver::new(source);
    assert!(
        reopened
            .drive_wait_source(&mut source, 2)
            .expect("acknowledged source polls")
            .is_none()
    );
    assert!(source.delivery.is_none());
    assert_mixed_broadcast_state(&mut reopened, wait_ids);
}

#[tokio::test]
async fn http_ingress_selection_activation_and_ack_survive_real_process_death() {
    for (seed, mode) in [
        (11, "after_ingress"),
        (12, "after_selection"),
        (13, "before_activation_commit"),
        (14, "after_activation_commit"),
        (15, "after_activation_before_ack"),
        (16, "after_ack"),
    ] {
        run_http_kill_scenario(seed, mode).await;
    }
}

async fn run_http_kill_scenario(seed: u64, mode: &str) {
    let scenario = prepare_http_scenario(seed, mode);
    kill_http_worker(&scenario, mode);
    recover_http_scenario(&scenario, mode).await;
}

fn prepare_http_scenario(seed: u64, mode: &str) -> HttpScenario {
    let world = TestWorld::new(seed).expect("HTTP test world creates");
    let state_database = world
        .domain()
        .path("state.sqlite")
        .expect("state path resolves");
    let spool_database = world
        .domain()
        .path("http.sqlite")
        .expect("spool path resolves");
    let clock_database = world
        .domain()
        .path("clock.sqlite")
        .expect("Clock path resolves");
    let marker = world
        .domain()
        .path("kill-ready")
        .expect("marker path resolves");
    let mut runtime = open_runtime(
        SqliteStore::open(&state_database, "domain:http-process-kill")
            .expect("durable store opens"),
        &clock_database,
    );
    let wait_id = start_waiting_run(
        &mut runtime,
        candidate(),
        json!({"case": mode}),
        RUN_ID,
        execution_request(&clock_database),
    );
    assert_eq!(
        wait_current(&mut runtime, RUN_ID, &wait_id).state,
        WaitState::Pending
    );
    drop(runtime);
    HttpScenario {
        _world: world,
        state_database,
        spool_database,
        clock_database,
        marker,
    }
}

fn kill_http_worker(scenario: &HttpScenario, mode: &str) {
    let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
    command
        .arg("--exact")
        .arg("http_process_kill_worker_entry")
        .arg("--nocapture")
        .env("CYMULE_HTTP_KILL_STATE_DB", &scenario.state_database)
        .env("CYMULE_HTTP_KILL_SPOOL_DB", &scenario.spool_database)
        .env("CYMULE_HTTP_KILL_CLOCK_DB", &scenario.clock_database)
        .env("CYMULE_HTTP_KILL_MARKER", &scenario.marker)
        .env("CYMULE_HTTP_KILL_MODE", mode)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    let mut child = ManagedChild::spawn(&mut command).expect("HTTP worker starts");
    child
        .wait_for_content(&scenario.marker, mode.as_bytes(), Duration::from_secs(20))
        .expect("HTTP worker reaches exact barrier");
    assert_eq!(
        fs::read_to_string(&scenario.marker).expect("barrier reads"),
        mode
    );
    assert_eq!(
        child.terminate().expect("HTTP worker reaps").signal(),
        Some(9)
    );
    assert!(child.is_reaped());
}

async fn recover_http_scenario(scenario: &HttpScenario, mode: &str) {
    assert_sqlite_integrity(&scenario.state_database);
    assert_sqlite_integrity(&scenario.spool_database);
    assert_sqlite_integrity(&scenario.clock_database);
    let mut reopened = open_runtime(
        SqliteStore::open(&scenario.state_database, "domain:http-process-kill")
            .expect("durable store reopens"),
        &scenario.clock_database,
    );
    let committed = matches!(
        mode,
        "after_activation_commit" | "after_activation_before_ack" | "after_ack"
    );
    assert_activation_closure(&mut reopened, committed);
    let router = recover_http_delivery(&mut reopened, &scenario.spool_database, mode).await;
    complete_http_run(&mut reopened, &scenario.clock_database);
    assert_eq!(
        router
            .oneshot(request(false))
            .await
            .expect("conflicting retry responds")
            .status(),
        StatusCode::CONFLICT
    );
}

async fn recover_http_delivery(
    reopened: &mut HttpRuntime,
    spool_database: &Path,
    mode: &str,
) -> axum::Router {
    let (router, source) =
        durable_signal_router(spool_database, 8, AllowAll).expect("HTTP source reopens");
    let retry = tokio::spawn(router.clone().oneshot(request(true)));
    let mut source = RecordingDriver::new(source);
    let wait_id = only_wait(reopened, RUN_ID).wait_id;
    let expected_delivery = WaitDelivery {
        activation_id: ACTIVATION_ID.to_owned(),
        source: WaitActivationSource::Signal {
            key: SIGNAL_KEY.to_owned(),
        },
        wait_ids: BTreeSet::from([wait_id]),
        value: json!({"ok": true}),
    };
    if mode == "after_ack" {
        assert!(
            reopened
                .drive_wait_source(&mut source, 1)
                .expect("HTTP source polls")
                .is_none(),
            "acknowledged HTTP ingress must not redeliver"
        );
        assert!(source.delivery.is_none());
    } else {
        let ready = loop {
            if let Some(ready) = reopened
                .drive_wait_source(&mut source, 1)
                .expect("HTTP activation converges")
            {
                break ready;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(
            ready,
            WaitAdmissionOutcome {
                disposition: WaitActivationDisposition::Applied,
                ready_run_ids: BTreeSet::from([RUN_ID.to_owned()]),
            }
        );
        assert_eq!(source.delivery.as_ref(), Some(&expected_delivery));
    }
    assert_eq!(
        retry
            .await
            .expect("retry task joins")
            .expect("HTTP retry responds")
            .status(),
        StatusCode::ACCEPTED
    );
    router
}

fn complete_http_run(reopened: &mut HttpRuntime, clock_database: &Path) {
    assert_activation_closure(reopened, true);
    resume_completed_run(reopened, RUN_ID, execution_request(clock_database));
    let current = run_current(reopened, RUN_ID);
    assert_eq!(current.epoch, 1);
    assert_eq!(current.continuation_status, ContinuationStatus::Completed);
}

fn assert_activation_closure<S: DurableStore>(
    runtime: &mut DurableRuntimeControl<S, EmptyPlugin>,
    committed: bool,
) {
    let current = run_current(runtime, RUN_ID);
    assert_eq!(
        current.continuation_status,
        if committed {
            ContinuationStatus::Ready
        } else {
            ContinuationStatus::Waiting
        }
    );
    let wait = only_wait(runtime, RUN_ID);
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
        &cymule_core::canonical_bytes(&json!({"ok": true})).expect("wait value canonicalizes"),
    )
    .expect("wait result identifies");
    assert_eq!(wait.result.as_ref(), committed.then_some(&expected_result));
}

async fn wait_for_spooled_request(path: &Path) {
    loop {
        let connection = Connection::open(path).expect("HTTP spool opens for barrier read");
        let retained: Option<i64> = connection
            .query_row(
                "SELECT 1 FROM cymule_http_signals WHERE activation_id = ?1",
                [ACTIVATION_ID],
                |row| row.get(0),
            )
            .optional()
            .expect("HTTP spool barrier reads");
        if retained.is_some() {
            return;
        }
        tokio::task::yield_now().await;
    }
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
