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
    RegionMigrationCommand, RegionMigrationKind, RegionMigrationPlan, RegionMigrationRequest,
    RegionMigrator, RegionSource, SchedulingPolicy, VIRTUAL_REGION_MIGRATION_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_VERSION, VIRTUAL_WORK_CONTROL_VERSION, VirtualCheckpoint,
    VirtualCursor, VirtualError, VirtualRegion, VirtualResult, VirtualScheduler, WorkItem,
    WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
};
use serde_json::json;

struct MillionItemSource;

struct FailsAfterFirstRegion;
struct StalledSource;

struct FairSource {
    run_b_cost: u64,
}

struct PriorityAgingSource;

struct TestRegionMigrator {
    evidence: ArtifactRef,
    corrupt_binding: bool,
    binding: String,
}

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

impl RegionSource for FairSource {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        let start: u64 = region.cursor.position.parse().expect("numeric cursor");
        let end = start + limit as u64;
        let cost = if region.run_id == "run:b" {
            self.run_b_cost
        } else {
            1
        };
        Ok(MaterializedPage {
            items: (start..end)
                .map(|index| WorkItem {
                    work_id: format!("{}:{index}", region.region_id),
                    region_id: region.region_id.clone(),
                    run_id: region.run_id.clone(),
                    payload: ArtifactRef {
                        artifact_id: format!("artifact:fair:{index}"),
                        kind: "example/work".to_owned(),
                    },
                    capability: Some("cpu".to_owned()),
                    priority: 0,
                    cost,
                })
                .collect(),
            next_cursor: VirtualCursor {
                version: region.cursor.version.clone(),
                position: end.to_string(),
                exhausted: false,
            },
        })
    }
}

impl RegionSource for PriorityAgingSource {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        let start: u64 = region.cursor.position.parse().expect("numeric cursor");
        let end = (start + limit as u64).min(100);
        Ok(MaterializedPage {
            items: (start..end)
                .map(|index| WorkItem {
                    work_id: if index == 0 {
                        "work:aged-low".to_owned()
                    } else {
                        format!("work:new-high:{index}")
                    },
                    region_id: region.region_id.clone(),
                    run_id: region.run_id.clone(),
                    payload: ArtifactRef {
                        artifact_id: format!("artifact:aging:{index}"),
                        kind: "example/work".to_owned(),
                    },
                    capability: Some("cpu".to_owned()),
                    priority: if index == 0 { 0 } else { 5 },
                    cost: 1,
                })
                .collect(),
            next_cursor: VirtualCursor {
                version: region.cursor.version.clone(),
                position: end.to_string(),
                exhausted: end == 100,
            },
        })
    }
}

impl RegionMigrator for TestRegionMigrator {
    fn binding(&self) -> &str {
        &self.binding
    }

    fn plan(
        &mut self,
        request: &RegionMigrationRequest,
        sources: &[VirtualRegion],
    ) -> VirtualResult<RegionMigrationPlan> {
        let source = sources.first().expect("migration has sources");
        Ok(RegionMigrationPlan {
            migration_version: VIRTUAL_REGION_MIGRATION_VERSION.to_owned(),
            migration_id: request.migration_id.clone(),
            kind: request.kind,
            expected_sources: sources
                .iter()
                .map(|region| (region.region_id.clone(), region.cursor.clone()))
                .collect(),
            targets: (0..request.target_count)
                .map(|index| VirtualRegion {
                    region_id: format!("{}:target:{index}", request.migration_id),
                    run_id: source.run_id.clone(),
                    source: source.source.clone(),
                    cursor: VirtualCursor {
                        version: "migration/1".to_owned(),
                        position: index.to_string(),
                        exhausted: false,
                    },
                    estimated_total: None,
                })
                .collect(),
            migration_binding: if self.corrupt_binding {
                "binding:migrator/corrupt".to_owned()
            } else {
                self.binding.clone()
            },
            coverage_evidence: self.evidence.clone(),
        })
    }

    fn verify(&mut self, plan: &RegionMigrationPlan) -> VirtualResult<()> {
        if plan.coverage_evidence != self.evidence
            || plan.migration_binding == "binding:migrator/corrupt"
        {
            return Err(VirtualError::Source(
                "migration coverage evidence or binding did not verify".to_owned(),
            ));
        }
        Ok(())
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

fn fairness_limits() -> FrontierLimits {
    FrontierLimits {
        max_materialized: 160,
        max_active: 1,
        max_active_per_run: 1,
        materialize_batch: 80,
    }
}

fn weighted_dispatch_counts(run_b_weight: u32, run_b_cost: u64) -> (usize, usize) {
    let limits = fairness_limits();
    let mut scheduler = VirtualScheduler::new_with_policy(
        limits,
        SchedulingPolicy {
            base_quantum: 1,
            aging_interval: 1,
        },
    )
    .expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("first region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("second region registers");
    scheduler
        .set_run_weight("run:b", run_b_weight)
        .expect("weight updates");
    let mut source = FairSource { run_b_cost };
    scheduler.fill(&mut source).expect("frontier fills");
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let mut counts = BTreeMap::<String, usize>::new();
    for step in 0..80 {
        let owner = format!("worker:fair:{step}");
        let binding = format!("binding:worker/fair/{step}");
        if step == 40 {
            let mut restored =
                VirtualScheduler::restore(limits, scheduler.snapshot()).expect("snapshot restores");
            let predicted = restored
                .claim(&owner, &binding, &capabilities)
                .expect("restored claim")
                .expect("restored work");
            let actual = scheduler
                .claim(&owner, &binding, &capabilities)
                .expect("claim")
                .expect("work");
            assert_eq!(predicted.item.work_id, actual.item.work_id);
            *counts.entry(actual.item.run_id.clone()).or_default() += 1;
            scheduler
                .succeed(
                    &actual.item.work_id,
                    &owner,
                    actual.epoch,
                    ArtifactRef {
                        artifact_id: format!("artifact:fair-result:{step}"),
                        kind: "example/result".to_owned(),
                    },
                )
                .expect("work succeeds");
        } else {
            let claim = scheduler
                .claim(&owner, &binding, &capabilities)
                .expect("claim")
                .expect("work");
            *counts.entry(claim.item.run_id.clone()).or_default() += 1;
            scheduler
                .succeed(
                    &claim.item.work_id,
                    &owner,
                    claim.epoch,
                    ArtifactRef {
                        artifact_id: format!("artifact:fair-result:{step}"),
                        kind: "example/result".to_owned(),
                    },
                )
                .expect("work succeeds");
        }
        assert!(scheduler.materialized_count() <= limits.max_materialized);
    }
    (
        counts.get("run:a").copied().unwrap_or_default(),
        counts.get("run:b").copied().unwrap_or_default(),
    )
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
fn weighted_deficit_fairness_tracks_run_shares_and_item_cost() {
    let equal_cost = weighted_dispatch_counts(3, 1);
    assert_eq!(equal_cost, (20, 60));

    let proportional_cost = weighted_dispatch_counts(3, 3);
    assert_eq!(proportional_cost, (40, 40));
}

#[test]
fn bounded_materialization_round_robin_keeps_every_region_visible() {
    let limits = FrontierLimits {
        max_materialized: 1,
        max_active: 1,
        max_active_per_run: 1,
        materialize_batch: 1,
    };
    let mut scheduler = VirtualScheduler::new(limits).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("first region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("second region registers");
    let mut source = FairSource { run_b_cost: 1 };
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let mut counts = BTreeMap::<String, usize>::new();
    for step in 0..20 {
        scheduler.fill(&mut source).expect("one slot materializes");
        let owner = format!("worker:materialize:{step}");
        let claim = scheduler
            .claim(
                &owner,
                &format!("binding:worker/materialize/{step}"),
                &capabilities,
            )
            .expect("claim")
            .expect("work");
        *counts.entry(claim.item.run_id.clone()).or_default() += 1;
        scheduler
            .succeed(
                &claim.item.work_id,
                &owner,
                claim.epoch,
                ArtifactRef {
                    artifact_id: format!("artifact:materialize-result:{step}"),
                    kind: "example/result".to_owned(),
                },
            )
            .expect("work succeeds");
    }
    assert_eq!(counts.get("run:a"), Some(&10));
    assert_eq!(counts.get("run:b"), Some(&10));
}

#[test]
fn priority_aging_prevents_starvation_under_continuous_high_priority_arrivals() {
    let limits = FrontierLimits {
        max_materialized: 2,
        max_active: 1,
        max_active_per_run: 1,
        materialize_batch: 2,
    };
    let mut scheduler = VirtualScheduler::new_with_policy(
        limits,
        SchedulingPolicy {
            base_quantum: 1,
            aging_interval: 1,
        },
    )
    .expect("scheduler creates");
    scheduler
        .register(region("region:aging", "run:aging"))
        .expect("region registers");
    let mut source = PriorityAgingSource;
    scheduler.fill(&mut source).expect("initial frontier fills");
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let mut low_claimed_at = None;
    for step in 0..10 {
        if step == 3 {
            scheduler = VirtualScheduler::restore(limits, scheduler.snapshot())
                .expect("aging state restores");
        }
        let owner = format!("worker:aging:{step}");
        let claim = scheduler
            .claim(
                &owner,
                &format!("binding:worker/aging/{step}"),
                &capabilities,
            )
            .expect("claim")
            .expect("work");
        let is_low = claim.item.work_id == "work:aged-low";
        scheduler
            .succeed(
                &claim.item.work_id,
                &owner,
                claim.epoch,
                ArtifactRef {
                    artifact_id: format!("artifact:aging-result:{step}"),
                    kind: "example/result".to_owned(),
                },
            )
            .expect("work succeeds");
        if is_low {
            low_claimed_at = Some(step + 1);
            break;
        }
        scheduler
            .fill(&mut source)
            .expect("high priority work replenishes");
    }
    assert_eq!(low_claimed_at, Some(7));
}

#[test]
fn region_split_merge_retires_sources_without_rewriting_materialized_work() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let historical_work: BTreeSet<String> = scheduler.snapshot().ready["run:a"]
        .iter()
        .map(|item| item.work_id.clone())
        .collect();
    let evidence = ArtifactRef {
        artifact_id: "artifact:migration:split-evidence".to_owned(),
        kind: "example/migration-evidence".to_owned(),
    };
    let request = RegionMigrationRequest {
        migration_id: "migration:split:a".to_owned(),
        kind: RegionMigrationKind::Split,
        source_region_ids: BTreeSet::from(["region:a".to_owned()]),
        target_count: 2,
        migration_binding: "binding:migrator/1".to_owned(),
    };
    assert!(matches!(
        scheduler.plan_migration(
            &mut TestRegionMigrator {
                evidence: evidence.clone(),
                corrupt_binding: true,
                binding: "binding:migrator/1".to_owned(),
            },
            &request,
        ),
        Err(VirtualError::Source(_))
    ));
    let plan = scheduler
        .plan_migration(
            &mut TestRegionMigrator {
                evidence: evidence.clone(),
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &request,
        )
        .expect("split plans");
    let before_unverified = scheduler.snapshot();
    assert!(matches!(
        scheduler.migrate(
            &mut TestRegionMigrator {
                evidence: ArtifactRef {
                    artifact_id: "artifact:migration:wrong-evidence".to_owned(),
                    kind: "example/migration-evidence".to_owned(),
                },
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &plan,
        ),
        Err(VirtualError::Source(_))
    ));
    assert_eq!(scheduler.snapshot(), before_unverified);
    let mut verifier = TestRegionMigrator {
        evidence: evidence.clone(),
        corrupt_binding: false,
        binding: "binding:migrator/1".to_owned(),
    };
    let receipt = scheduler
        .migrate(&mut verifier, &plan)
        .expect("split applies");
    assert_eq!(
        receipt.retired_regions,
        BTreeSet::from(["region:a".to_owned()])
    );
    assert_eq!(receipt.active_targets.len(), 2);
    assert_eq!(
        scheduler
            .migrate(&mut verifier, &plan)
            .expect("split replay is idempotent"),
        receipt
    );
    assert_eq!(
        scheduler.snapshot().retired_regions["region:a"],
        "migration:split:a"
    );
    assert_eq!(
        scheduler.snapshot().ready["run:a"]
            .iter()
            .map(|item| item.work_id.clone())
            .collect::<BTreeSet<_>>(),
        historical_work
    );
    assert!(
        scheduler.snapshot().ready["run:a"]
            .iter()
            .all(|item| item.region_id == "region:a")
    );
    let mut conflicting = plan;
    conflicting.targets[0].cursor.position = "different".to_owned();
    assert!(matches!(
        scheduler.migrate(&mut verifier, &conflicting),
        Err(VirtualError::Conflict(_))
    ));

    let split_targets = receipt.active_targets;
    let merge_request = RegionMigrationRequest {
        migration_id: "migration:merge:a".to_owned(),
        kind: RegionMigrationKind::Merge,
        source_region_ids: split_targets.clone(),
        target_count: 1,
        migration_binding: "binding:migrator/1".to_owned(),
    };
    let merge_evidence = ArtifactRef {
        artifact_id: "artifact:migration:merge-evidence".to_owned(),
        kind: "example/migration-evidence".to_owned(),
    };
    let merge_plan = scheduler
        .plan_migration(
            &mut TestRegionMigrator {
                evidence: merge_evidence.clone(),
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &merge_request,
        )
        .expect("merge plans");
    let merged = scheduler
        .migrate(
            &mut TestRegionMigrator {
                evidence: merge_evidence,
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &merge_plan,
        )
        .expect("merge applies");
    assert_eq!(merged.retired_regions, split_targets);
    assert_eq!(merged.active_targets.len(), 1);
    VirtualScheduler::restore(limits(), scheduler.snapshot()).expect("migration history restores");
}

#[test]
fn cursor_change_rejects_migration_without_partial_retirement() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:stale", "run:stale"))
        .expect("region registers");
    let request = RegionMigrationRequest {
        migration_id: "migration:stale".to_owned(),
        kind: RegionMigrationKind::Split,
        source_region_ids: BTreeSet::from(["region:stale".to_owned()]),
        target_count: 2,
        migration_binding: "binding:migrator/1".to_owned(),
    };
    let stale_evidence = ArtifactRef {
        artifact_id: "artifact:migration:stale".to_owned(),
        kind: "example/migration-evidence".to_owned(),
    };
    let plan = scheduler
        .plan_migration(
            &mut TestRegionMigrator {
                evidence: stale_evidence.clone(),
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &request,
        )
        .expect("migration plans");
    scheduler
        .fill(&mut MillionItemSource)
        .expect("cursor advances");
    let before = scheduler.snapshot();
    assert!(matches!(
        scheduler.migrate(
            &mut TestRegionMigrator {
                evidence: stale_evidence,
                corrupt_binding: false,
                binding: "binding:migrator/1".to_owned(),
            },
            &plan,
        ),
        Err(VirtualError::Conflict(_))
    ));
    assert_eq!(scheduler.snapshot(), before);
    assert!(scheduler.snapshot().retired_regions.is_empty());
    assert!(scheduler.snapshot().migrations.is_empty());
}

#[test]
fn durable_region_migration_reopens_and_stale_cas_retires_nothing() {
    let mut machine = Machine::new();
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:durable-migration", "run:migration"))
        .expect("region registers");
    DurableVirtualController::fill_and_checkpoint(
        &mut current,
        &mut scheduler,
        &mut MillionItemSource,
        "journal:virtual",
        "virtual:migration:fill",
    )
    .expect("source cursor checkpoints");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let mut stale_scheduler = DurableVirtualController::load(&stale, "journal:virtual", limits())
        .expect("stale scheduler restores");

    let evidence = machine.put_artifact(
        "example/migration-evidence",
        b"complete non-overlapping split".to_vec(),
    );
    let request = RegionMigrationRequest {
        migration_id: "migration:durable-split".to_owned(),
        kind: RegionMigrationKind::Split,
        source_region_ids: BTreeSet::from(["region:durable-migration".to_owned()]),
        target_count: 2,
        migration_binding: "binding:migrator/durable@1".to_owned(),
    };
    let plan = scheduler
        .plan_migration(
            &mut TestRegionMigrator {
                evidence: evidence.clone(),
                corrupt_binding: false,
                binding: "binding:migrator/durable@1".to_owned(),
            },
            &request,
        )
        .expect("migration plans");
    let command = RegionMigrationCommand {
        control_version: VIRTUAL_REGION_MIGRATION_CONTROL_VERSION.to_owned(),
        command_id: "command:migration:durable-split".to_owned(),
        plan,
    };
    let mut verifier = TestRegionMigrator {
        evidence: evidence.clone(),
        corrupt_binding: false,
        binding: "binding:migrator/durable@1".to_owned(),
    };
    let receipt = DurableVirtualController::migrate_command_and_checkpoint(
        &mut current,
        &mut scheduler,
        &mut verifier,
        &machine,
        &command,
        "journal:virtual",
    )
    .expect("migration checkpoints");
    assert_eq!(receipt.active_targets.len(), 2);

    let stale_before = stale_scheduler.snapshot();
    assert!(matches!(
        DurableVirtualController::migrate_command_and_checkpoint(
            &mut stale,
            &mut stale_scheduler,
            &mut verifier,
            &machine,
            &command,
            "journal:virtual",
        ),
        Err(VirtualError::Durable(_))
    ));
    assert_eq!(stale_scheduler.snapshot(), stale_before);
    assert!(stale_scheduler.snapshot().retired_regions.is_empty());

    let mut reopened = DurableCoordinator::open(store).expect("store reopens");
    let mut restored = DurableVirtualController::load(&reopened, "journal:virtual", limits())
        .expect("scheduler restores");
    assert_eq!(
        restored
            .migration("migration:durable-split")
            .expect("receipt restores"),
        &receipt
    );
    assert!(
        reopened
            .restore_machine()
            .expect("Machine restores")
            .artifact(&evidence)
            .is_some()
    );
    restored
        .set_run_weight("run:migration", 2)
        .expect("later control updates");
    DurableVirtualController::checkpoint(
        &mut reopened,
        &restored,
        "journal:virtual",
        "virtual:migration:later-checkpoint",
    )
    .expect("later checkpoint commits");
    let before_replay = restored.snapshot();
    assert_eq!(
        DurableVirtualController::migrate_command_and_checkpoint(
            &mut reopened,
            &mut restored,
            &mut verifier,
            &machine,
            &command,
            "journal:virtual",
        )
        .expect("historical migration receipt replays"),
        receipt
    );
    assert_eq!(restored.snapshot(), before_replay);
    let mut conflicting = command;
    conflicting.plan.targets[0].cursor.position = "different".to_owned();
    assert!(matches!(
        DurableVirtualController::migrate_command_and_checkpoint(
            &mut reopened,
            &mut restored,
            &mut verifier,
            &machine,
            &conflicting,
            "journal:virtual",
        ),
        Err(VirtualError::Conflict(_))
    ));
    assert_eq!(restored.snapshot(), before_replay);
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

    let mut invalid_policy = scheduler.snapshot();
    invalid_policy.scheduling_policy.base_quantum = 0;
    assert!(matches!(
        VirtualScheduler::restore(limits(), invalid_policy),
        Err(VirtualError::Validation(_))
    ));
    let mut invalid_weight = scheduler.snapshot();
    invalid_weight.run_weights.insert("run:a".to_owned(), 0);
    assert!(matches!(
        VirtualScheduler::restore(limits(), invalid_weight),
        Err(VirtualError::Validation(_))
    ));
    let mut future_age = scheduler.snapshot();
    let ready_id = future_age.ready["run:a"][0].work_id.clone();
    future_age.ready_since.insert(ready_id, 1);
    assert!(matches!(
        VirtualScheduler::restore(limits(), future_age),
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

#[test]
fn weighted_fairness_and_aging_accounting_survive_m1_reopen() {
    let limits = FrontierLimits {
        max_materialized: 8,
        max_active: 2,
        max_active_per_run: 1,
        materialize_batch: 4,
    };
    let machine = Machine::new();
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let mut scheduler = VirtualScheduler::new_with_policy(
        limits,
        SchedulingPolicy {
            base_quantum: 2,
            aging_interval: 3,
        },
    )
    .expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("first region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("second region registers");
    scheduler
        .set_run_weight("run:b", 4)
        .expect("weight updates");
    DurableVirtualController::fill_and_checkpoint(
        &mut coordinator,
        &mut scheduler,
        &mut FairSource { run_b_cost: 2 },
        "journal:virtual",
        "virtual:fairness:fill",
    )
    .expect("frontier checkpoints");
    let claim = DurableVirtualController::claim_and_checkpoint(
        &mut coordinator,
        &mut scheduler,
        "worker:fairness",
        "binding:worker/fairness",
        &BTreeSet::from(["cpu".to_owned()]),
        "journal:virtual",
        "virtual:fairness:claim",
    )
    .expect("claim checkpoints")
    .expect("work claims");
    let reason = ParkReason::Backpressure {
        domain: "fairness:test".to_owned(),
    };
    scheduler
        .park(
            &claim.item.work_id,
            "worker:fairness",
            claim.epoch,
            reason.clone(),
        )
        .expect("claim parks");
    DurableVirtualController::checkpoint(
        &mut coordinator,
        &scheduler,
        "journal:virtual",
        "virtual:fairness:park",
    )
    .expect("park checkpoints");
    assert_eq!(scheduler.wake(&reason), 1);
    DurableVirtualController::checkpoint(
        &mut coordinator,
        &scheduler,
        "journal:virtual",
        "virtual:fairness:wake",
    )
    .expect("wake checkpoints");
    let expected = scheduler.snapshot();
    drop(coordinator);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let mut restored = DurableVirtualController::load(&reopened, "journal:virtual", limits)
        .expect("scheduler restores");
    assert_eq!(restored.snapshot(), expected);
    let mut original = VirtualScheduler::restore(limits, expected).expect("original restores");
    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let expected_claim = original
        .claim("worker:next", "binding:worker/next", &capabilities)
        .expect("expected claim")
        .expect("expected work");
    let restored_claim = restored
        .claim("worker:next", "binding:worker/next", &capabilities)
        .expect("restored claim")
        .expect("restored work");
    assert_eq!(restored_claim.item.work_id, expected_claim.item.work_id);
    assert_eq!(restored.snapshot(), original.snapshot());
}
