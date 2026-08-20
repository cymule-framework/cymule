//! Real process-death sweep across every M1 effect Run CAS boundary.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::Duration;

use cymule_core::{
    Definition, DispatchPolicy, EffectContract, EffectProfile, Expression, MutationKind, Operation,
    PlanCandidate, ReconciliationMode, ReconciliationResolution, Region, Step, WorldOutcome,
};
use cymule_durable::{
    DriveOutcome, DurableResult, DurableState, DurableStore, OutboxState, ResumableRuntime,
    StoreCommit, StoredState,
};
use cymule_runtime::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use cymule_store_sqlite::SqliteStore;
use cymule_test_world::{ManagedChild, TestWorld};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

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
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        self.calls += 1;
        if self.calls != self.fail_at {
            return self.inner.compare_and_swap(expected_revision, next);
        }
        match self.phase {
            KillPhase::BeforeCommit => self.stop(),
            KillPhase::AfterCommit => {
                self.inner.compare_and_swap(expected_revision, next)?;
                self.stop();
            }
        }
    }
}

struct CountingStore {
    inner: SqliteStore,
    calls: Arc<AtomicUsize>,
}

impl DurableStore for CountingStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.compare_and_swap(expected_revision, next)
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
            .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
        let bytes = cymule_core::canonical_bytes(input)?;
        let existing: Option<(Vec<u8>, i64)> = connection
            .query_row(
                "SELECT input_json, dispatch_count FROM effect_ledger WHERE intent_id = ?1",
                [intent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
        match existing {
            Some((retained, _)) if retained != bytes => Err(RuntimeError::Plugin(
                "effect intent was reused with different input".to_owned(),
            )),
            Some(_) => {
                connection
                    .execute(
                        "UPDATE effect_ledger SET dispatch_count = dispatch_count + 1
                         WHERE intent_id = ?1",
                        [intent_id],
                    )
                    .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
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
                    .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
                Ok(())
            }
        }
    }

    fn reconcile(&self, intent_id: &str) -> RuntimeResult<bool> {
        let connection = Connection::open(&self.database)
            .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
        connection
            .execute(
                "INSERT INTO reconciliation_ledger(intent_id, reconciliation_count)
                 VALUES (?1, 1)
                 ON CONFLICT(intent_id) DO UPDATE SET
                    reconciliation_count = reconciliation_count + 1",
                [intent_id],
            )
            .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
        let updated = connection
            .execute(
                "UPDATE effect_ledger
                 SET reconciliation_count = reconciliation_count + 1
                 WHERE intent_id = ?1",
                [intent_id],
            )
            .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
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
                intent_id, input, ..
            } => {
                self.dispatch(&intent_id, &input)?;
                Ok(PluginResponse::EffectResult {
                    outcome: WorldOutcome::Applied,
                    value: Some(input),
                })
            }
            PluginRequest::ReconcileEffect {
                intent_id, input, ..
            } => Ok(PluginResponse::ReconciliationResult {
                resolution: if self.reconcile(&intent_id)? {
                    ReconciliationResolution::ResolvedApplied
                } else {
                    ReconciliationResolution::ResolvedNotApplied
                },
                value: Some(input),
            }),
            request @ PluginRequest::Call { .. } => Err(RuntimeError::Plugin(format!(
                "unexpected process-kill plugin request {request:?}"
            ))),
        }
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
        inner: SqliteStore::open(database, "domain:m1-kill").expect("durable store opens"),
        phase,
        fail_at,
        calls: 0,
        marker,
    };
    let mut runtime =
        ResumableRuntime::open(store, LedgerPlugin { database: ledger }).expect("runtime opens");
    runtime
        .start(
            effect_candidate(),
            &json!({"message": "process kill"}),
            "run:m1-kill",
        )
        .expect("worker reaches its selected M1 CAS boundary");
    panic!("M1 kill worker unexpectedly completed");
}

#[test]
fn every_m1_effect_run_cas_boundary_survives_real_process_death() {
    let baseline = TestWorld::new(0).expect("baseline test world creates");
    let baseline_database = baseline
        .domain()
        .path("durable.sqlite")
        .expect("baseline database path resolves");
    let baseline_ledger = baseline
        .domain()
        .path("effects.sqlite")
        .expect("baseline ledger path resolves");
    LedgerPlugin::initialize(&baseline_ledger);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = ResumableRuntime::open(
        CountingStore {
            inner: SqliteStore::open(&baseline_database, "domain:m1-kill")
                .expect("baseline store opens"),
            calls: Arc::clone(&calls),
        },
        LedgerPlugin {
            database: baseline_ledger,
        },
    )
    .expect("baseline runtime opens");
    assert!(matches!(
        runtime
            .start(
                effect_candidate(),
                &json!({"message": "process kill"}),
                "run:m1-kill",
            )
            .expect("baseline Run completes"),
        DriveOutcome::Completed(_)
    ));
    let boundary_count = calls.load(Ordering::SeqCst);
    assert!(boundary_count >= 5, "effect Run crosses all durable stages");

    for phase in ["before_commit", "after_commit"] {
        for fail_at in 1..=boundary_count {
            let phase_seed = usize::from(phase == "after_commit") * boundary_count + fail_at;
            let world =
                TestWorld::new(u64::try_from(phase_seed).expect("fault-matrix position fits u64"))
                    .expect("fault test world creates");
            let database = world
                .domain()
                .path("durable.sqlite")
                .expect("durable database path resolves");
            let ledger = world
                .domain()
                .path("effects.sqlite")
                .expect("effect ledger path resolves");
            let marker = world
                .domain()
                .path("kill-ready")
                .expect("kill marker path resolves");
            LedgerPlugin::initialize(&ledger);
            let mut command =
                Command::new(std::env::current_exe().expect("test executable resolves"));
            command
                .arg("--exact")
                .arg("m1_process_kill_worker_entry")
                .arg("--nocapture")
                .env("CYMULE_M1_KILL_DB", &database)
                .env("CYMULE_M1_KILL_LEDGER", &ledger)
                .env("CYMULE_M1_KILL_PHASE", phase)
                .env("CYMULE_M1_KILL_AT", fail_at.to_string())
                .env("CYMULE_M1_KILL_MARKER", &marker)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit());
            let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
            child
                .wait_for_path(&marker, Duration::from_secs(20))
                .expect("kill worker reaches the selected CAS barrier");
            assert!(!child.terminate().expect("worker is reaped").success());
            assert!(child.is_reaped());

            let store =
                SqliteStore::open(&database, "domain:m1-kill").expect("durable store reopens");
            let mut runtime = ResumableRuntime::open(
                store,
                LedgerPlugin {
                    database: ledger.clone(),
                },
            )
            .expect("runtime reopens");
            let outcome = if runtime.coordinator().revision().is_some() {
                runtime.resume("run:m1-kill")
            } else {
                runtime.start(
                    effect_candidate(),
                    &json!({"message": "process kill"}),
                    "run:m1-kill",
                )
            }
            .expect("recovery converges");
            assert!(matches!(outcome, DriveOutcome::Completed(_)));
            let state = runtime
                .coordinator()
                .state()
                .expect("durable state validates");
            assert!(state.outbox.values().all(|dispatch| matches!(
                dispatch.state,
                OutboxState::Applied | OutboxState::NotApplied
            )));
            runtime
                .coordinator()
                .restore_machine()
                .expect("Machine replays");
            let (dispatches, reconciliations) = LedgerPlugin::counts(&ledger);
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
    }
}
