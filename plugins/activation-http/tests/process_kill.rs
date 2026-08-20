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
use cymule_core::{Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec};
use cymule_durable::{
    ContinuationStatus, DriveOutcome, DurableResult, DurableStore, ParkedWaitIndex,
    ResumableRuntime, StoreBatch, StoreCommit, StoreHead, StoredState, WaitDelivery,
    WaitSourceDriver, WaitState,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;
use tower::ServiceExt;

const RUN_ID: &str = "run:http-process-kill";
const ACTIVATION_ID: &str = "activation:http-process-kill";
const SIGNAL_KEY: &str = "signal:http-process-kill";

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

fn open_runtime<S: DurableStore>(store: S) -> ResumableRuntime<S, EmptyPlugin> {
    let mut plugin = EmptyPlugin;
    let manifest = plugin.describe().expect("empty plugin describes");
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .expect("execution binding creates");
    ResumableRuntime::open(store, plugin, binding).expect("runtime opens")
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

fn request(ok: bool) -> Request<Body> {
    Request::post("/v1/signals")
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"activation_id":"{ACTIVATION_ID}","key":"{SIGNAL_KEY}","value":{{"ok":{ok}}}}}"#
        )))
        .expect("HTTP request builds")
}

#[derive(Clone, Copy)]
enum AckBarrier {
    Before,
    After,
}

struct BarrierDriver {
    inner: SqliteHttpSignalDriver,
    marker: PathBuf,
    phase: AckBarrier,
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
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>> {
        self.inner.receive(index, max_targets)
    }

    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()> {
        if matches!(self.phase, AckBarrier::Before) {
            self.stop("after_activation_before_ack");
        }
        self.inner.acknowledge(activation_id)?;
        self.stop("after_ack");
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
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
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
}

#[tokio::test]
async fn http_process_kill_worker_entry() {
    let Ok(state_database) = std::env::var("CYMULE_HTTP_KILL_STATE_DB") else {
        return;
    };
    let spool_database =
        PathBuf::from(std::env::var("CYMULE_HTTP_KILL_SPOOL_DB").expect("spool path exists"));
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
            let runtime = open_runtime(
                SqliteStore::open(state_database, "domain:http-process-kill")
                    .expect("durable store opens"),
            );
            let mut driver = driver;
            let index = runtime
                .coordinator()
                .parked_wait_index()
                .expect("parked index rebuilds");
            let delivery = loop {
                if let Some(delivery) = driver.receive(&index, 1).expect("HTTP source polls") {
                    break delivery;
                }
                tokio::task::yield_now().await;
            };
            assert_eq!(delivery.activation_id, ACTIVATION_ID);
            assert!(!response.is_finished(), "selection cannot acknowledge HTTP");
            fs::write(marker, "after_selection").expect("HTTP selection barrier writes");
            loop {
                thread::park_timeout(Duration::from_mins(1));
            }
        }
        "before_activation_commit" | "after_activation_commit" => {
            let after_commit = mode == "after_activation_commit";
            let mut runtime = open_runtime(KillStore {
                inner: SqliteStore::open(state_database, "domain:http-process-kill")
                    .expect("durable store opens"),
                marker,
                after_commit,
            });
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
            );
            let phase = if mode == "after_activation_before_ack" {
                AckBarrier::Before
            } else {
                AckBarrier::After
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
async fn http_ingress_selection_activation_and_ack_survive_real_process_death() {
    for (seed, mode) in [
        (11, "after_ingress"),
        (12, "after_selection"),
        (13, "before_activation_commit"),
        (14, "after_activation_commit"),
        (15, "after_activation_before_ack"),
        (16, "after_ack"),
    ] {
        let world = TestWorld::new(seed).expect("HTTP test world creates");
        let state_database = world
            .domain()
            .path("state.sqlite")
            .expect("state path resolves");
        let spool_database = world
            .domain()
            .path("http.sqlite")
            .expect("spool path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("marker path resolves");
        let mut runtime = open_runtime(
            SqliteStore::open(&state_database, "domain:http-process-kill")
                .expect("durable store opens"),
        );
        assert!(matches!(
            runtime
                .start(candidate(), &json!({"case": mode}), RUN_ID)
                .expect("HTTP Run starts"),
            DriveOutcome::Suspended { .. }
        ));
        drop(runtime);

        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("http_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_HTTP_KILL_STATE_DB", &state_database)
            .env("CYMULE_HTTP_KILL_SPOOL_DB", &spool_database)
            .env("CYMULE_HTTP_KILL_MARKER", &marker)
            .env("CYMULE_HTTP_KILL_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("HTTP worker starts");
        child
            .wait_for_content(&marker, mode.as_bytes(), Duration::from_secs(20))
            .expect("HTTP worker reaches exact barrier");
        assert_eq!(fs::read_to_string(&marker).expect("barrier reads"), mode);
        assert_eq!(
            child.terminate().expect("HTTP worker reaps").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        assert_sqlite_integrity(&state_database);
        assert_sqlite_integrity(&spool_database);
        let mut reopened = open_runtime(
            SqliteStore::open(&state_database, "domain:http-process-kill")
                .expect("durable store reopens"),
        );
        let committed_before_recovery = matches!(
            mode,
            "after_activation_commit" | "after_activation_before_ack" | "after_ack"
        );
        assert_activation_closure(&reopened, committed_before_recovery);
        let (router, mut source) =
            durable_signal_router(&spool_database, 8, AllowAll).expect("HTTP source reopens");
        let retry = tokio::spawn(router.clone().oneshot(request(true)));
        if mode == "after_ack" {
            assert_eq!(
                retry
                    .await
                    .expect("retry task joins")
                    .expect("HTTP retry responds")
                    .status(),
                StatusCode::ACCEPTED
            );
            assert!(
                source
                    .receive(
                        &reopened
                            .coordinator()
                            .parked_wait_index()
                            .expect("index rebuilds"),
                        1,
                    )
                    .expect("HTTP source polls")
                    .is_none(),
                "acknowledged HTTP ingress must not redeliver"
            );
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
            assert_eq!(ready, BTreeSet::from([RUN_ID.to_owned()]));
            assert_eq!(
                retry
                    .await
                    .expect("retry task joins")
                    .expect("HTTP retry responds")
                    .status(),
                StatusCode::ACCEPTED
            );
        }
        assert!(
            reopened
                .coordinator()
                .state()
                .expect("state validates")
                .wait_activations
                .contains_key(ACTIVATION_ID)
        );
        assert_activation_closure(&reopened, true);
        reopened
            .coordinator()
            .restore_machine()
            .expect("Machine replays");
        assert!(matches!(
            reopened.resume(RUN_ID).expect("Run resumes"),
            DriveOutcome::Completed(_)
        ));
        assert_eq!(
            reopened
                .coordinator()
                .state()
                .expect("state validates")
                .continuations[RUN_ID]
                .epoch,
            1
        );
        assert_eq!(
            router
                .oneshot(request(false))
                .await
                .expect("conflicting retry responds")
                .status(),
            StatusCode::CONFLICT
        );
    }
}

fn assert_activation_closure<S: DurableStore>(
    runtime: &ResumableRuntime<S, EmptyPlugin>,
    committed: bool,
) {
    let state = runtime.coordinator().state().expect("state validates");
    assert_eq!(
        state.wait_activations.contains_key(ACTIVATION_ID),
        committed
    );
    assert_eq!(state.wait_activations.len(), usize::from(committed));
    assert_eq!(
        state.continuations[RUN_ID].status,
        if committed {
            ContinuationStatus::Ready
        } else {
            ContinuationStatus::Waiting
        }
    );
    assert_eq!(
        state.waits.values().next().expect("wait exists").state,
        if committed {
            WaitState::Completed
        } else {
            WaitState::Pending
        }
    );
    assert_eq!(
        state
            .waits
            .values()
            .next()
            .expect("wait exists")
            .result
            .is_some(),
        committed
    );
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
