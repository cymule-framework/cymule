//! Plan DAG, rollout, migration, shadow, and rollback tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_core::{ArtifactRef, Definition, Expression, PlanCandidate, Region};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableResult,
    DurableState, DurableStore, FrameState, MemoryStore, StoreCommit, StoredState,
};
use cymule_evolution::{
    DurableEvolutionController, EvolutionController, EvolutionError, MigrationReceipt,
    PatchOperation, RolloutDecision, RolloutMode, ShadowComparison, diff_plans,
};
use serde_json::json;

fn plan(version: &str) -> cymule_core::SealedPlan {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: format!("evolution_{version}"),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: Vec::new(),
                result: Expression::Literal {
                    value: json!({"version": version}),
                },
            },
        }],
        metadata: BTreeMap::from([("version".to_owned(), version.to_owned())]),
    }
    .seal()
    .expect("plan seals")
}

fn artifact(id: &str) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_owned(),
        kind: "evolution/evidence".to_owned(),
    }
}

fn continuation(plan_id: &str) -> Continuation {
    Continuation {
        run_id: "run:active".to_owned(),
        plan_id: plan_id.to_owned(),
        binding_context: "binding:1".to_owned(),
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
    }
}

#[derive(Clone)]
struct LostReceiptStore {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

impl DurableStore for LostReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost evolution checkpoint receipt".to_owned(),
            ));
        }
        Ok(commit)
    }
}

#[test]
fn plan_dag_impact_and_cycles_fail_closed() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("root registers");
    let edge = controller
        .add_edge(
            &first.plan_id,
            &second,
            vec![PatchOperation {
                kind: "replace".to_owned(),
                target: "main:state-schema".to_owned(),
                before: Some("schema:1".to_owned()),
                after: Some("schema:2".to_owned()),
            }],
            artifact("evidence:patch"),
        )
        .expect("edge registers");
    let impact = controller
        .impact(
            &edge.edge_id,
            &[continuation(&first.plan_id)],
            &BTreeMap::from([("effect:released".to_owned(), first.plan_id.clone())]),
        )
        .expect("impact computes");
    assert!(impact.requires_migration);
    assert!(impact.affected_runs.contains("run:active"));
    assert!(impact.pinned_effects.contains("effect:released"));
    assert!(matches!(
        controller.add_edge(
            &second.plan_id,
            &first,
            Vec::new(),
            artifact("evidence:cycle"),
        ),
        Err(EvolutionError::Conflict(_))
    ));
}

#[test]
fn sealed_plan_diff_is_deterministic_and_registers_one_reviewed_edge() {
    let first = plan("1");
    let second = plan("2");
    let expected = diff_plans(&first, &second).expect("Plans diff");
    assert_eq!(
        expected,
        diff_plans(&first, &second).expect("repeated diff is stable")
    );
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0].kind, "replace");
    assert_eq!(expected[0].target, "definition:main");
    assert!(expected[0].before.is_some());
    assert!(expected[0].after.is_some());

    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("parent registers");
    let edge = controller
        .add_diff_edge(&first.plan_id, &second, artifact("evidence:auto-diff"))
        .expect("diff edge registers");
    assert_eq!(edge.operations, expected);
    assert_eq!(edge.from_plan, first.plan_id);
    assert_eq!(edge.to_plan, second.plan_id);
}

#[test]
fn canary_pins_occurrences_and_rollback_changes_only_future_selection() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("root registers");
    controller
        .register_plan(second.clone())
        .expect("target registers");
    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:canary".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Canary {
                basis_points: 5_000,
            },
        })
        .expect("canary sets");
    let pinned = controller
        .select_for_occurrence("occurrence:existing")
        .expect("occurrence selects");
    let repeated = controller
        .select_for_occurrence("occurrence:existing")
        .expect("selection repeats");
    assert_eq!(pinned, repeated);

    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:rollback".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id,
            mode: RolloutMode::RolledBack,
        })
        .expect("rollback sets");
    assert_eq!(
        controller
            .select_for_occurrence("occurrence:existing")
            .expect("old occurrence remains pinned"),
        pinned
    );
    assert_eq!(
        controller
            .select_for_occurrence("occurrence:new")
            .expect("new occurrence uses fallback"),
        first.plan_id
    );
}

#[test]
fn migration_requires_safe_point_and_shadow_evidence_is_idempotent() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    let migration = MigrationReceipt {
        migration_id: "migration:1".to_owned(),
        run_id: "run:active".to_owned(),
        from_plan: first.plan_id.clone(),
        to_plan: second.plan_id.clone(),
        input_state: artifact("state:1"),
        output_state: artifact("state:2"),
        evidence: artifact("evidence:migration"),
    };
    assert!(matches!(
        controller.record_migration(migration.clone(), false),
        Err(EvolutionError::Conflict(_))
    ));
    controller
        .record_migration(migration.clone(), true)
        .expect("safe migration records");
    controller
        .record_migration(migration, true)
        .expect("migration retry is idempotent");

    let shadow = ShadowComparison {
        comparison_id: "shadow:1".to_owned(),
        subject: "run:active".to_owned(),
        primary_plan: first.plan_id,
        shadow_plan: second.plan_id,
        primary_digest: "result:a".to_owned(),
        shadow_digest: "result:a".to_owned(),
        equivalent: true,
    };
    controller
        .record_shadow(shadow.clone())
        .expect("shadow records");
    controller
        .record_shadow(shadow)
        .expect("shadow retry is idempotent");
    EvolutionController::restore(controller.snapshot()).expect("snapshot restores");
}

#[test]
fn durable_mixed_version_pin_reopens_after_lost_checkpoint_receipt() {
    let first = plan("1");
    let second = plan("2");
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store)
        .expect("coordinator opens")
        .initialize(&cymule_core::Machine::new())
        .expect("coordinator initializes");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("fallback registers");
    DurableEvolutionController::checkpoint(
        &mut coordinator,
        &controller,
        "evolution:main",
        "checkpoint:plans",
    )
    .expect("plans checkpoint");
    DurableEvolutionController::add_diff_edge_and_checkpoint(
        &mut coordinator,
        &mut controller,
        "evolution:main",
        "checkpoint:edge",
        &first.plan_id,
        &second,
        artifact("evidence:durable:diff"),
    )
    .expect("diff edge checkpoints");
    let pinned_target = second.plan_id.clone();
    DurableEvolutionController::set_rollout_and_checkpoint(
        &mut coordinator,
        &mut controller,
        "evolution:main",
        "checkpoint:rollout",
        RolloutDecision {
            decision_id: "rollout:active".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Active,
        },
    )
    .expect("rollout checkpoints");

    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableEvolutionController::select_occurrence_and_checkpoint(
            &mut coordinator,
            &mut controller,
            "evolution:main",
            "checkpoint:occurrence:1",
            "occurrence:1",
        )
        .is_err()
    );
    assert!(controller.snapshot().occurrence_plans.is_empty());

    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("coordinator reopens");
    let mut restored =
        DurableEvolutionController::load(&reopened, "evolution:main").expect("journal replays");
    assert_eq!(
        restored.snapshot().occurrence_plans["occurrence:1"],
        second.plan_id
    );
    assert_eq!(
        DurableEvolutionController::select_occurrence_and_checkpoint(
            &mut reopened,
            &mut restored,
            "evolution:main",
            "checkpoint:occurrence:1",
            "occurrence:1",
        )
        .expect("lost receipt replays"),
        second.plan_id
    );

    DurableEvolutionController::set_rollout_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:main",
        "checkpoint:rollback",
        RolloutDecision {
            decision_id: "rollout:rollback".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id,
            mode: RolloutMode::RolledBack,
        },
    )
    .expect("rollback checkpoints");
    assert_eq!(
        restored
            .select_for_occurrence("occurrence:1")
            .expect("old occurrence stays pinned"),
        pinned_target
    );
    assert_eq!(
        DurableEvolutionController::select_occurrence_and_checkpoint(
            &mut reopened,
            &mut restored,
            "evolution:main",
            "checkpoint:occurrence:2",
            "occurrence:2",
        )
        .expect("new occurrence durably uses fallback"),
        first.plan_id
    );

    DurableEvolutionController::record_migration_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:main",
        "checkpoint:migration:1",
        MigrationReceipt {
            migration_id: "migration:durable:1".to_owned(),
            run_id: "run:active".to_owned(),
            from_plan: first.plan_id.clone(),
            to_plan: pinned_target.clone(),
            input_state: artifact("state:durable:1"),
            output_state: artifact("state:durable:2"),
            evidence: artifact("evidence:durable:migration"),
        },
        true,
    )
    .expect("migration checkpoints");
    DurableEvolutionController::record_shadow_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:main",
        "checkpoint:shadow:1",
        ShadowComparison {
            comparison_id: "shadow:durable:1".to_owned(),
            subject: "occurrence:2".to_owned(),
            primary_plan: first.plan_id,
            shadow_plan: pinned_target,
            primary_digest: "result:primary".to_owned(),
            shadow_digest: "result:shadow".to_owned(),
            equivalent: false,
        },
    )
    .expect("shadow evidence checkpoints");

    let store = reopened.into_store();
    let final_coordinator = DurableCoordinator::open(store).expect("final coordinator reopens");
    let final_state = DurableEvolutionController::load(&final_coordinator, "evolution:main")
        .expect("full evolution journal replays")
        .snapshot();
    assert!(final_state.migrations.contains_key("migration:durable:1"));
    assert!(final_state.shadows.contains_key("shadow:durable:1"));
}

#[test]
fn stale_evolution_checkpoint_rolls_back_the_in_memory_transition() {
    let first = plan("1");
    let second = plan("2");
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("current opens")
        .initialize(&cymule_core::Machine::new())
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store).expect("stale view opens");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("fallback registers");
    controller
        .register_plan(second.clone())
        .expect("target registers");
    DurableEvolutionController::checkpoint(
        &mut current,
        &controller,
        "evolution:stale",
        "checkpoint:plans",
    )
    .expect("current writer advances");

    let before = controller.snapshot();
    assert!(
        DurableEvolutionController::set_rollout_and_checkpoint(
            &mut stale,
            &mut controller,
            "evolution:stale",
            "checkpoint:stale-rollout",
            RolloutDecision {
                decision_id: "rollout:stale".to_owned(),
                fallback_plan: first.plan_id,
                target_plan: second.plan_id,
                mode: RolloutMode::Active,
            },
        )
        .is_err()
    );
    assert_eq!(controller.snapshot(), before);
    assert!(
        DurableEvolutionController::load(&current, "evolution:stale")
            .expect("current journal remains valid")
            .snapshot()
            .rollout
            .is_none()
    );
}
