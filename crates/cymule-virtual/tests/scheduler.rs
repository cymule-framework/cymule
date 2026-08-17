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
    DurableVirtualController, FrontierLimits, MaterializedPage, ParkReason, ParkedWork,
    RegionSource, VIRTUAL_WORK_CONTROL_VERSION, VirtualCheckpoint, VirtualCursor, VirtualError,
    VirtualRegion, VirtualResult, VirtualScheduler, WorkItem, WorkOccurrenceState, WorkResolution,
    WorkResolutionCommand,
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
        .claim("worker:1", "binding:worker/1", &capabilities)
        .expect("claim")
        .expect("work");
    let second = scheduler
        .claim("worker:2", "binding:worker/2", &capabilities)
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
        .succeed(
            &first.item.work_id,
            "worker:1",
            first.epoch,
            ArtifactRef {
                artifact_id: "artifact:result:1".to_owned(),
                kind: "example/result".to_owned(),
            },
        )
        .expect("current owner completes");
    assert!(
        restored
            .succeed(
                &second.item.work_id,
                "worker:wrong",
                second.epoch,
                ArtifactRef {
                    artifact_id: "artifact:result:2".to_owned(),
                    kind: "example/result".to_owned(),
                },
            )
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
        .claim(
            "worker:1",
            "binding:worker/1",
            &BTreeSet::from(["cpu".to_owned()]),
        )
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
    let mut claimed = scheduler.clone();
    claimed
        .claim(
            "worker:1",
            "binding:worker/1",
            &BTreeSet::from(["cpu".to_owned()]),
        )
        .expect("first claim")
        .expect("first work");
    claimed
        .claim(
            "worker:2",
            "binding:worker/2",
            &BTreeSet::from(["cpu".to_owned()]),
        )
        .expect("second claim")
        .expect("second work");
    let overflow = claimed.snapshot();
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
        .claim(
            "worker:1",
            "binding:worker/1",
            &BTreeSet::from(["cpu".to_owned()]),
        )
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
        .claim(
            "worker:1",
            "binding:worker/1",
            &BTreeSet::from(["cpu".to_owned()]),
        )
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

#[test]
fn work_occurrence_retry_success_and_stale_fencing_are_explicit() {
    let single_limits = FrontierLimits {
        max_materialized: 1,
        max_active: 1,
        max_active_per_run: 1,
        materialize_batch: 1,
    };
    let mut scheduler = VirtualScheduler::new(single_limits).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let first = scheduler
        .claim("worker:1", "binding:worker/1", &capabilities)
        .expect("first claim")
        .expect("first work");
    assert_eq!(
        scheduler.snapshot().occurrences[&first.occurrence_id].state,
        WorkOccurrenceState::Running
    );
    let error = ArtifactRef {
        artifact_id: "artifact:retry-error".to_owned(),
        kind: "example/error".to_owned(),
    };
    let retry = WorkResolution::Retry {
        error: error.clone(),
        next_reason: None,
    };
    let retried = scheduler
        .resolve(&first.item.work_id, "worker:1", first.epoch, &retry)
        .expect("retry records");
    assert_eq!(retried.state, WorkOccurrenceState::RetryScheduled);
    assert_eq!(
        scheduler
            .resolve(&first.item.work_id, "worker:1", first.epoch, &retry)
            .expect("retry receipt replays"),
        retried
    );
    assert!(matches!(
        scheduler.resolve(
            &first.item.work_id,
            "worker:1",
            first.epoch,
            &WorkResolution::Failed {
                error: error.clone(),
            },
        ),
        Err(VirtualError::Conflict(_))
    ));

    let second = scheduler
        .claim("worker:2", "binding:worker/2", &capabilities)
        .expect("retry claim")
        .expect("retried work");
    assert_eq!(second.item.work_id, first.item.work_id);
    assert_eq!(second.epoch, first.epoch + 1);
    assert_ne!(second.occurrence_id, first.occurrence_id);
    let result = ArtifactRef {
        artifact_id: "artifact:success".to_owned(),
        kind: "example/result".to_owned(),
    };
    assert!(matches!(
        scheduler.succeed(&first.item.work_id, "worker:1", first.epoch, result.clone(),),
        Err(VirtualError::Conflict(_))
    ));
    let succeeded = scheduler
        .succeed(
            &second.item.work_id,
            "worker:2",
            second.epoch,
            result.clone(),
        )
        .expect("current attempt succeeds");
    assert_eq!(succeeded.state, WorkOccurrenceState::Succeeded);
    assert_eq!(succeeded.result, Some(result));
    assert_eq!(
        scheduler.occurrence(&second.occurrence_id),
        Some(&succeeded)
    );
    assert_eq!(scheduler.snapshot().occurrences.len(), 2);
    VirtualScheduler::restore(single_limits, scheduler.snapshot()).expect("history restores");
}

#[test]
fn retry_parking_failure_and_cancellation_have_distinct_terminal_records() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let parked_claim = scheduler
        .claim("worker:park", "binding:worker/park", &capabilities)
        .expect("park claim")
        .expect("park work");
    let retry_reason = ParkReason::Backpressure {
        domain: "retry:test".to_owned(),
    };
    let parked_retry = scheduler
        .resolve(
            &parked_claim.item.work_id,
            "worker:park",
            parked_claim.epoch,
            &WorkResolution::Retry {
                error: ArtifactRef {
                    artifact_id: "artifact:retry".to_owned(),
                    kind: "example/error".to_owned(),
                },
                next_reason: Some(retry_reason.clone()),
            },
        )
        .expect("retry parks");
    assert_eq!(parked_retry.state, WorkOccurrenceState::RetryScheduled);
    assert_eq!(scheduler.wake(&retry_reason), 1);

    let failed_claim = scheduler
        .claim("worker:fail", "binding:worker/fail", &capabilities)
        .expect("failure claim")
        .expect("failure work");
    let failure = scheduler
        .resolve(
            &failed_claim.item.work_id,
            "worker:fail",
            failed_claim.epoch,
            &WorkResolution::Failed {
                error: ArtifactRef {
                    artifact_id: "artifact:terminal-error".to_owned(),
                    kind: "example/error".to_owned(),
                },
            },
        )
        .expect("terminal failure records");
    assert_eq!(failure.state, WorkOccurrenceState::Failed);

    let cancelled_claim = scheduler
        .claim("worker:cancel", "binding:worker/cancel", &capabilities)
        .expect("cancellation claim")
        .expect("cancellation work");
    let cancelled = scheduler
        .resolve(
            &cancelled_claim.item.work_id,
            "worker:cancel",
            cancelled_claim.epoch,
            &WorkResolution::Cancelled {
                reason: ArtifactRef {
                    artifact_id: "artifact:cancel-reason".to_owned(),
                    kind: "example/cancellation".to_owned(),
                },
            },
        )
        .expect("cancellation records");
    assert_eq!(cancelled.state, WorkOccurrenceState::Cancelled);
    assert!(matches!(
        scheduler.succeed(
            &cancelled_claim.item.work_id,
            "worker:cancel",
            cancelled_claim.epoch,
            ArtifactRef {
                artifact_id: "artifact:late".to_owned(),
                kind: "example/result".to_owned(),
            },
        ),
        Err(VirtualError::Conflict(_))
    ));
}

#[test]
fn durable_claim_and_result_survive_reopen_and_stale_cas() {
    let mut machine = Machine::new();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    DurableVirtualController::fill_and_checkpoint(
        &mut current,
        &mut scheduler,
        &mut MillionItemSource,
        "journal:virtual",
        "virtual:work:fill",
    )
    .expect("frontier checkpoints");
    let claim = DurableVirtualController::claim_and_checkpoint(
        &mut current,
        &mut scheduler,
        "worker:durable",
        "binding:worker/durable",
        &BTreeSet::from(["cpu".to_owned()]),
        "journal:virtual",
        "virtual:work:claim",
    )
    .expect("claim checkpoints")
    .expect("work claims");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let mut stale_scheduler = DurableVirtualController::load(&stale, "journal:virtual", limits())
        .expect("stale scheduler restores");

    let result = machine.put_artifact("example/result", b"durable result".to_vec());
    let resolution = WorkResolution::Succeeded {
        result: result.clone(),
    };
    let command = WorkResolutionCommand {
        control_version: VIRTUAL_WORK_CONTROL_VERSION.to_owned(),
        command_id: "virtual:work:result".to_owned(),
        work_id: claim.item.work_id.clone(),
        owner: "worker:durable".to_owned(),
        epoch: claim.epoch,
        resolution: resolution.clone(),
    };
    let succeeded = DurableVirtualController::resolve_command_and_checkpoint(
        &mut current,
        &mut scheduler,
        &machine,
        &command,
        "journal:virtual",
    )
    .expect("result checkpoints");
    assert_eq!(succeeded.result, Some(result.clone()));

    let stale_before = stale_scheduler.snapshot();
    assert!(matches!(
        DurableVirtualController::resolve_command_and_checkpoint(
            &mut stale,
            &mut stale_scheduler,
            &machine,
            &command,
            "journal:virtual",
        ),
        Err(VirtualError::Durable(_))
    ));
    assert_eq!(stale_scheduler.snapshot(), stale_before);

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let mut restored = DurableVirtualController::load(&reopened, "journal:virtual", limits())
        .expect("scheduler restores");
    assert_eq!(
        restored.snapshot().occurrences[&claim.occurrence_id].state,
        WorkOccurrenceState::Succeeded
    );
    assert!(
        reopened
            .restore_machine()
            .expect("Machine restores")
            .artifact(&result)
            .is_some()
    );
    DurableVirtualController::claim_and_checkpoint(
        &mut reopened,
        &mut restored,
        "worker:later",
        "binding:worker/later",
        &BTreeSet::from(["cpu".to_owned()]),
        "journal:virtual",
        "virtual:work:later-claim",
    )
    .expect("later claim checkpoints")
    .expect("later work claims");
    let before_replay = restored.snapshot();
    let replayed = DurableVirtualController::resolve_command_and_checkpoint(
        &mut reopened,
        &mut restored,
        &machine,
        &command,
        "journal:virtual",
    )
    .expect("lost result receipt replays idempotently");
    assert_eq!(replayed, succeeded);
    assert_eq!(restored.snapshot(), before_replay);
    let mut conflicting = command;
    conflicting.resolution = WorkResolution::Failed { error: result };
    assert!(matches!(
        DurableVirtualController::resolve_command_and_checkpoint(
            &mut reopened,
            &mut restored,
            &machine,
            &conflicting,
            "journal:virtual",
        ),
        Err(VirtualError::Conflict(_))
    ));
    assert_eq!(restored.snapshot(), before_replay);
}
