//! Plan DAG, rollout, migration, shadow, and rollback tests.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRef, Definition, Expression, PlanCandidate, Region};
use cymule_durable::{Continuation, ContinuationStatus, FrameState};
use cymule_evolution::{
    EvolutionController, EvolutionError, MigrationReceipt, PatchOperation, RolloutDecision,
    RolloutMode, ShadowComparison,
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
