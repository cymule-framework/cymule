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
use cymule_core::{Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec};
use cymule_durable::{
    ContinuationStatus, DriveOutcome, DurableResult, DurableState, DurableStore, ParkedWaitIndex,
    ResumableRuntime, StoreCommit, StoredState, WaitDelivery, WaitSourceDriver, WaitState,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::Connection;
use serde_json::json;

const RUN_ID: &str = "run:timer-process-kill";
const ACTIVATION_ID: &str = "activation:timer-process-kill";
const TIMER_ID: &str = "timer:process-kill";

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

fn open_runtime<S: DurableStore>(store: S) -> ResumableRuntime<S, EmptyPlugin> {
    let mut plugin = EmptyPlugin;
    let manifest = plugin.describe().expect("empty plugin describes");
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("execution binding creates");
    ResumableRuntime::open(store, plugin, binding).expect("runtime opens")
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
                    },
                }],
                result: Expression::Input,
            },
        }],
        metadata: BTreeMap::new(),
    }
}

#[derive(Clone, Copy)]
enum AckBarrier {
    Before,
    After,
}

struct BarrierDriver {
    inner: SqliteTimerDriver<FixedClock>,
    marker: PathBuf,
    phase: AckBarrier,
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
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        if !self.after_commit {
            self.stop();
        }
        self.inner.compare_and_swap(expected_revision, next)?;
        self.stop()
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

#[test]
fn timer_process_kill_worker_entry() {
    let Ok(state_database) = std::env::var("CYMULE_TIMER_KILL_STATE_DB") else {
        return;
    };
    let timer_database =
        std::env::var("CYMULE_TIMER_KILL_TIMER_DB").expect("timer database exists");
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
            let runtime = open_runtime(
                SqliteStore::open(state_database, "domain:timer-process-kill")
                    .expect("durable store opens"),
            );
            let index = runtime
                .coordinator()
                .parked_wait_index()
                .expect("parked index rebuilds");
            let delivery = driver
                .receive(&index, 1)
                .expect("timer selection succeeds")
                .expect("timer delivery exists");
            assert_eq!(delivery.activation_id, ACTIVATION_ID);
            fs::write(marker, "after_selection").expect("selection barrier writes");
            loop {
                thread::park_timeout(Duration::from_mins(1));
            }
        }
        "before_activation_commit" | "after_activation_commit" => {
            let after_commit = mode == "after_activation_commit";
            let mut runtime = open_runtime(KillStore {
                inner: SqliteStore::open(state_database, "domain:timer-process-kill")
                    .expect("durable store opens"),
                marker,
                after_commit,
            });
            runtime
                .drive_wait_source(&mut driver, 1)
                .expect("worker reaches activation CAS barrier");
            panic!("timer kill worker unexpectedly passed its CAS barrier");
        }
        "after_activation_before_ack" | "after_ack" => {
            let mut runtime = open_runtime(
                SqliteStore::open(state_database, "domain:timer-process-kill")
                    .expect("durable store opens"),
            );
            let phase = if mode == "after_activation_before_ack" {
                AckBarrier::Before
            } else {
                AckBarrier::After
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
        let world = TestWorld::new(seed).expect("timer test world creates");
        let state_database = world
            .domain()
            .path("state.sqlite")
            .expect("state path resolves");
        let timer_database = world
            .domain()
            .path("timer.sqlite")
            .expect("timer path resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("marker path resolves");

        let mut runtime = open_runtime(
            SqliteStore::open(&state_database, "domain:timer-process-kill")
                .expect("durable store opens"),
        );
        assert!(matches!(
            runtime
                .start(candidate(), &json!({"case": mode}), RUN_ID)
                .expect("timer Run starts"),
            DriveOutcome::Suspended { .. }
        ));
        drop(runtime);
        if mode != "after_schedule" {
            let mut driver = SqliteTimerDriver::open_with_clock(&timer_database, FixedClock(100))
                .expect("timer driver opens");
            driver
                .schedule(ACTIVATION_ID, TIMER_ID, 100, &json!({"due": true}))
                .expect("timer schedules");
        }

        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("timer_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_TIMER_KILL_STATE_DB", &state_database)
            .env("CYMULE_TIMER_KILL_TIMER_DB", &timer_database)
            .env("CYMULE_TIMER_KILL_MARKER", &marker)
            .env("CYMULE_TIMER_KILL_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("timer worker starts");
        child
            .wait_for_path(&marker, Duration::from_secs(20))
            .expect("timer worker reaches exact barrier");
        assert_eq!(fs::read_to_string(&marker).expect("barrier reads"), mode);
        assert_eq!(
            child.terminate().expect("timer worker reaps").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        assert_sqlite_integrity(&state_database);
        assert_sqlite_integrity(&timer_database);
        let mut reopened = open_runtime(
            SqliteStore::open(&state_database, "domain:timer-process-kill")
                .expect("durable store reopens"),
        );
        let committed_before_recovery = matches!(
            mode,
            "after_activation_commit" | "after_activation_before_ack" | "after_ack"
        );
        assert_activation_closure(&reopened, committed_before_recovery);
        let mut timer = SqliteTimerDriver::open_with_clock(&timer_database, FixedClock(100))
            .expect("timer reopens");
        if mode == "after_ack" {
            assert!(
                timer
                    .receive(
                        &reopened
                            .coordinator()
                            .parked_wait_index()
                            .expect("index rebuilds"),
                        1,
                    )
                    .expect("timer polls")
                    .is_none(),
                "acknowledged timer must not redeliver"
            );
        } else {
            let retained = timer
                .receive(
                    &reopened
                        .coordinator()
                        .parked_wait_index()
                        .expect("index rebuilds"),
                    1,
                )
                .expect("timer redelivery reads")
                .expect("timer redelivers");
            assert_eq!(retained.activation_id, ACTIVATION_ID);
            assert_eq!(
                retained.source,
                cymule_durable::WaitActivationSource::Timer {
                    timer_id: TIMER_ID.to_owned(),
                }
            );
            assert_eq!(
                reopened
                    .drive_wait_source(&mut timer, 1)
                    .expect("activation converges"),
                Some(BTreeSet::from([RUN_ID.to_owned()]))
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
    let wait = state.waits.values().next().expect("wait exists");
    assert_eq!(
        wait.state,
        if committed {
            WaitState::Completed
        } else {
            WaitState::Pending
        }
    );
    assert_eq!(wait.result.is_some(), committed);
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
