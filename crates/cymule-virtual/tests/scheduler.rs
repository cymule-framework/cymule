//! Bounded-cardinality, fairness, parking, and restart tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{
    ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine,
    PlanCandidate, Region,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, FrameState, MemoryStore, WaitActivation,
    WaitActivationSource, WaitCondition, WaitKind, WaitState,
};
use cymule_virtual::{
    ClaimedWork, DurableVirtualController, FrontierLimits, MaterializedPage, ParkReason,
    ParkedWork, RegionSource, VirtualCheckpoint, VirtualCursor, VirtualError, VirtualRegion,
    VirtualResult, VirtualScheduler, WorkItem,
};
use serde_json::json;

struct MillionItemSource;

struct FailsAfterFirstRegion;
struct StalledSource;

impl RegionSource for MillionItemSource {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        let start: u64 = region.cursor.position.parse().expect("numeric cursor");
        let end = (start + limit as u64).min(1_000_000);
        let items = (start..end)
            .map(|index| WorkItem {
                work_id: format!("{}:{index}", region.region_id),
                region_id: region.region_id.clone(),
                run_id: region.run_id.clone(),
                payload: ArtifactRef {
                    artifact_id: format!("artifact:{index}"),
                    kind: "example/work".to_owned(),
                },
                capability: Some("cpu".to_owned()),
                priority: 0,
                cost: 1,
            })
            .collect();
        Ok(MaterializedPage {
            items,
            next_cursor: VirtualCursor {
                version: "million/1".to_owned(),
                position: end.to_string(),
                exhausted: end == 1_000_000,
            },
        })
    }
}

impl RegionSource for FailsAfterFirstRegion {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        _limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        if region.region_id == "region:b" {
            return Err(VirtualError::Source("simulated source failure".to_owned()));
        }
        Ok(MaterializedPage {
            items: vec![WorkItem {
                work_id: "region:a:partial".to_owned(),
                region_id: region.region_id.clone(),
                run_id: region.run_id.clone(),
                payload: ArtifactRef {
                    artifact_id: "artifact:partial".to_owned(),
                    kind: "example/work".to_owned(),
                },
                capability: None,
                priority: 0,
                cost: 1,
            }],
            next_cursor: VirtualCursor {
                version: region.cursor.version.clone(),
                position: "1".to_owned(),
                exhausted: true,
            },
        })
    }
}

impl RegionSource for StalledSource {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        _limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        Ok(MaterializedPage {
            items: Vec::new(),
            next_cursor: region.cursor.clone(),
        })
    }
}

fn region(id: &str, run_id: &str) -> VirtualRegion {
    VirtualRegion {
        region_id: id.to_owned(),
        run_id: run_id.to_owned(),
        source: "example.million".to_owned(),
        cursor: VirtualCursor {
            version: "million/1".to_owned(),
            position: "0".to_owned(),
            exhausted: false,
        },
        estimated_total: Some(1_000_000),
    }
}

fn limits() -> FrontierLimits {
    FrontierLimits {
        max_materialized: 8,
        max_active: 4,
        max_active_per_run: 2,
        materialize_batch: 4,
    }
}

fn durable_machine_with_wait() -> (Machine, Continuation, WaitCondition) {
    let mut machine = Machine::new();
    let plan = machine
        .seal_plan(PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "virtual_activation".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Literal { value: json!(null) },
                },
            }],
            metadata: BTreeMap::new(),
        })
        .expect("Plan seals");
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:virtual:start".to_owned(),
            actor: "test".to_owned(),
            run_id: "run:a".to_owned(),
            expected_precondition: None,
            command: Command::StartRun {
                plan_id: plan.plan_id.clone(),
                binding_context: "binding:test/1".to_owned(),
            },
        })
        .expect("Run starts");
    let continuation = Continuation {
        run_id: "run:a".to_owned(),
        plan_id: plan.plan_id,
        binding_context: "binding:test/1".to_owned(),
        frames: vec![FrameState {
            invocation_id: "main".to_owned(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: None,
        wait_set: BTreeSet::new(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Ready,
    };
    let wait = WaitCondition {
        wait_id: "wait:approval".to_owned(),
        run_id: "run:a".to_owned(),
        kind: WaitKind::Signal {
            key: "signal:approval".to_owned(),
        },
        consume_once: true,
        state: WaitState::Pending,
        result: None,
    };
    (machine, continuation, wait)
}

#[test]
fn frozen_virtual_checkpoint_fixture_matches_the_rust_contract() {
    let fixture = include_str!("../../../tests/fixtures/virtual-checkpoint.json");
    let checkpoint: VirtualCheckpoint =
        serde_json::from_str(fixture).expect("virtual checkpoint deserializes");
    assert_eq!(checkpoint.checkpoint_version, "cymule.virtual-checkpoint/1");
    VirtualScheduler::restore(limits(), checkpoint.snapshot.clone())
        .expect("fixture snapshot restores");
    assert_eq!(
        serde_json::to_value(checkpoint).expect("checkpoint serializes"),
        serde_json::from_str::<serde_json::Value>(fixture).expect("fixture parses")
    );
}

#[test]
fn million_item_regions_keep_a_bounded_fair_frontier_across_restore() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("region registers");
    assert_eq!(
        scheduler
            .fill(&mut MillionItemSource)
            .expect("frontier fills"),
        8
    );
    assert_eq!(scheduler.materialized_count(), 8);

    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let first = scheduler
        .claim("worker:1", &capabilities)
        .expect("claim")
        .expect("work");
    let second = scheduler
        .claim("worker:2", &capabilities)
        .expect("claim")
        .expect("work");
    assert_ne!(first.item.run_id, second.item.run_id);

    let snapshot = scheduler.snapshot();
    let mut restored = VirtualScheduler::restore(limits(), snapshot).expect("snapshot restores");
    assert_eq!(restored.materialized_count(), 8);
    assert_eq!(
        restored
            .fill(&mut MillionItemSource)
            .expect("backpressure applies"),
        0
    );

    restored
        .complete(&first.item.work_id, "worker:1", first.epoch)
        .expect("current owner completes");
    assert!(
        restored
            .complete(&second.item.work_id, "worker:wrong", second.epoch)
            .is_err()
    );
}

#[test]
fn parked_work_wakes_by_exact_indexed_reason() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let claim = scheduler
        .claim("worker:1", &BTreeSet::from(["cpu".to_owned()]))
        .expect("claim")
        .expect("work");
    let reason = ParkReason::Wait {
        key: "wait:approval".to_owned(),
    };
    scheduler
        .park(&claim.item.work_id, "worker:1", claim.epoch, reason.clone())
        .expect("work parks");
    assert_eq!(
        scheduler.snapshot().parked_index[&reason],
        BTreeSet::from([claim.item.work_id])
    );
    assert_eq!(scheduler.wake(&reason), 1);
    assert_eq!(scheduler.wake(&reason), 0);
}

#[test]
fn virtual_cursor_and_frontier_checkpoint_reopen_and_stale_cas_rolls_back() {
    let store = MemoryStore::new();
    let machine = Machine::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    assert_eq!(
        DurableVirtualController::fill_and_checkpoint(
            &mut current,
            &mut scheduler,
            &mut MillionItemSource,
            "journal:virtual",
            "virtual:checkpoint:1",
        )
        .expect("first page checkpoints"),
        4
    );
    let first_checkpoint_scheduler = scheduler.clone();
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let mut stale_scheduler = DurableVirtualController::load(&stale, "journal:virtual", limits())
        .expect("stale scheduler restores");
    assert_eq!(
        DurableVirtualController::fill_and_checkpoint(
            &mut current,
            &mut scheduler,
            &mut MillionItemSource,
            "journal:virtual",
            "virtual:checkpoint:2",
        )
        .expect("second page checkpoints"),
        4
    );
    assert!(matches!(
        DurableVirtualController::checkpoint(
            &mut current,
            &first_checkpoint_scheduler,
            "journal:virtual",
            "virtual:checkpoint:1",
        ),
        Err(VirtualError::Conflict(_))
    ));
    let stale_before = stale_scheduler.snapshot();
    assert!(matches!(
        DurableVirtualController::fill_and_checkpoint(
            &mut stale,
            &mut stale_scheduler,
            &mut MillionItemSource,
            "journal:virtual",
            "virtual:checkpoint:2",
        ),
        Err(VirtualError::Durable(_))
    ));
    assert_eq!(stale_scheduler.snapshot(), stale_before);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let restored = DurableVirtualController::load(&reopened, "journal:virtual", limits())
        .expect("scheduler restores");
    assert_eq!(restored.materialized_count(), 8);
    assert_eq!(restored.snapshot().regions["region:a"].cursor.position, "8");
}

#[test]
fn partial_source_failure_rolls_back_cursor_frontier_and_journal() {
    let machine = Machine::new();
    let mut coordinator = DurableCoordinator::open(MemoryStore::new())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("first region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("second region registers");
    let before = scheduler.snapshot();
    assert!(matches!(
        DurableVirtualController::fill_and_checkpoint(
            &mut coordinator,
            &mut scheduler,
            &mut FailsAfterFirstRegion,
            "journal:virtual",
            "virtual:partial-failure",
        ),
        Err(VirtualError::Source(_))
    ));
    assert_eq!(scheduler.snapshot(), before);
    assert!(
        coordinator
            .journal_records("journal:virtual")
            .expect("journal reads")
            .is_empty()
    );
}

#[test]
fn stalled_source_cursor_fails_without_mutating_the_frontier() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    let before = scheduler.snapshot();
    assert!(matches!(
        scheduler.fill(&mut StalledSource),
        Err(VirtualError::Source(_))
    ));
    assert_eq!(scheduler.snapshot(), before);
}

#[test]
fn restore_rejects_duplicate_work_and_per_run_claim_overflow() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");

    let mut duplicate = scheduler.snapshot();
    let item = duplicate.ready["run:a"][0].clone();
    duplicate.parked.insert(
        item.work_id.clone(),
        ParkedWork {
            item,
            reason: ParkReason::Backpressure {
                domain: "test".to_owned(),
            },
        },
    );
    assert!(matches!(
        VirtualScheduler::restore(limits(), duplicate),
        Err(VirtualError::Validation(_))
    ));

    let strict_limits = FrontierLimits {
        max_active_per_run: 1,
        ..limits()
    };
    let mut overflow = scheduler.snapshot();
    for epoch in 1..=2 {
        let item = overflow
            .ready
            .get_mut("run:a")
            .expect("queue")
            .pop_front()
            .expect("ready item");
        overflow.claim_epochs.insert(item.work_id.clone(), epoch);
        overflow.active.insert(
            item.work_id.clone(),
            ClaimedWork {
                item,
                owner: format!("worker:{epoch}"),
                epoch,
            },
        );
    }
    assert!(matches!(
        VirtualScheduler::restore(strict_limits, overflow),
        Err(VirtualError::Validation(_))
    ));
}

#[test]
fn wait_activation_and_indexed_virtual_wake_share_one_m1_cas() {
    let (mut machine, continuation, wait) = durable_machine_with_wait();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    current
        .put_continuation(continuation)
        .expect("continuation persists");
    current.register_wait(wait).expect("wait registers");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");

    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let claim = scheduler
        .claim("worker:1", &BTreeSet::from(["cpu".to_owned()]))
        .expect("claim")
        .expect("work");
    scheduler
        .park(
            &claim.item.work_id,
            "worker:1",
            claim.epoch,
            ParkReason::Wait {
                key: "wait:approval".to_owned(),
            },
        )
        .expect("work parks");
    let mut stale_scheduler = scheduler.clone();

    let result = machine.put_artifact("cymule.wait-activation-result/1", b"approved".to_vec());
    let activation = WaitActivation::new(
        "activation:virtual:1",
        WaitActivationSource::Signal {
            key: "signal:approval".to_owned(),
        },
        BTreeSet::from(["wait:approval".to_owned()]),
        result,
    )
    .expect("activation validates");
    assert_eq!(
        DurableVirtualController::activate_and_wake(
            &mut current,
            &mut scheduler,
            &machine,
            activation.clone(),
            "journal:virtual",
            "virtual:wake:1",
        )
        .expect("activation and wake commit"),
        1
    );
    assert!(scheduler.snapshot().parked.is_empty());
    assert_eq!(
        current.state().expect("state").waits["wait:approval"].state,
        WaitState::Completed
    );
    assert_eq!(
        current
            .journal_records("journal:virtual")
            .expect("journal reads")
            .len(),
        1
    );

    let stale_before = stale_scheduler.snapshot();
    assert!(matches!(
        DurableVirtualController::activate_and_wake(
            &mut stale,
            &mut stale_scheduler,
            &machine,
            activation,
            "journal:virtual",
            "virtual:wake:1",
        ),
        Err(VirtualError::Durable(_))
    ));
    assert_eq!(stale_scheduler.snapshot(), stale_before);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let restored = DurableVirtualController::load(&reopened, "journal:virtual", limits())
        .expect("scheduler restores");
    assert!(restored.snapshot().parked.is_empty());
    assert_eq!(
        restored.materialized_count(),
        scheduler.materialized_count()
    );
}

#[test]
fn parked_reason_index_rebuilds_from_a_durable_checkpoint() {
    let machine = Machine::new();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let claim = scheduler
        .claim("worker:1", &BTreeSet::from(["cpu".to_owned()]))
        .expect("claim")
        .expect("work");
    let reason = ParkReason::Wait {
        key: "wait:durable-index".to_owned(),
    };
    scheduler
        .park(&claim.item.work_id, "worker:1", claim.epoch, reason.clone())
        .expect("work parks");
    DurableVirtualController::checkpoint(
        &mut coordinator,
        &scheduler,
        "journal:virtual",
        "virtual:parked:1",
    )
    .expect("parked snapshot checkpoints");
    drop(coordinator);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let mut restored = DurableVirtualController::load(&reopened, "journal:virtual", limits())
        .expect("scheduler restores");
    assert_eq!(
        restored.snapshot().parked_index[&reason],
        BTreeSet::from([claim.item.work_id])
    );
    assert_eq!(restored.wake(&reason), 1);
}
