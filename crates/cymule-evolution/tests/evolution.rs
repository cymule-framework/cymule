//! Plan DAG, rollout, migration, shadow, and rollback tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_core::{
    ArtifactRef, ComponentContract, Definition, DispatchPolicy, EffectContract, EffectProfile,
    Expression, MutationKind, PlanCandidate, ReconciliationMode, Region, WaitSpec,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableResult,
    DurableState, DurableStore, FrameState, MemoryStore, StoreCommit, StoredState,
};
use cymule_evolution::{
    DefinitionRegistry, DurableDefinitionRegistry, DurableEvolutionController, EvolutionCommand,
    EvolutionController, EvolutionError, GateOutcome, MigrationAdapter, MigrationAdapterDescriptor,
    MigrationCapabilityChange, MigrationOutput, MigrationPreservation, MigrationReceipt,
    MigrationRequest, MigrationSafePoint, MigrationStateCoverage, ObservationOutcome,
    PatchOperation, PlanPatch, PlanTemplate, ReferenceStrategy, RelinkViolation, RestartRequest,
    RolloutDecision, RolloutGate, RolloutMode, RolloutObservation, ShadowBindingMode,
    ShadowComparison, ShadowDriver, ShadowDriverDescriptor, ShadowEffectMode, ShadowOutput,
    ShadowRequest, SubflowReference, analyze_relink, diff_plans,
};
use cymule_runtime::{
    EmbeddedRuntime, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
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
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            input: cymule_core::ArtifactRef {
                artifact_id: format!("sha256:{}", "0".repeat(64)),
                kind: "test/input".to_owned(),
            },
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

fn migration_safe_point(
    run_id: &str,
    plan_id: &str,
    input_state: ArtifactRef,
) -> (Continuation, MigrationSafePoint) {
    let mut continuation = continuation(plan_id);
    run_id.clone_into(&mut continuation.run_id);
    continuation.state = Some(input_state);
    let safe_point = MigrationSafePoint::derive(&continuation).expect("safe point derives");
    (continuation, safe_point)
}

fn resign_safe_point(safe_point: &mut MigrationSafePoint) {
    safe_point.safe_point_id = cymule_core::content_id(
        cymule_evolution::MIGRATION_SAFE_POINT_VERSION,
        &(
            safe_point.run_id.as_str(),
            safe_point.plan_id.as_str(),
            safe_point.epoch,
            &safe_point.state,
            &safe_point.continuation_digest,
        ),
    )
    .expect("safe point re-signs");
}

fn reusable_definition(version: &str, input_schema: serde_json::Value) -> Definition {
    Definition {
        id: "review".to_owned(),
        input_schema,
        output_schema: json!({}),
        body: Region {
            steps: Vec::new(),
            result: Expression::Literal {
                value: json!({"version": version}),
            },
        },
    }
}

fn effectful_reusable_definition(version: &str) -> Definition {
    Definition {
        id: "review".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![cymule_core::Step {
                id: "effect.capture".to_owned(),
                operation: cymule_core::Operation::Effect {
                    effect: "test.capture".to_owned(),
                    input: Expression::Input,
                    occurrence: "review".to_owned(),
                    bind: None,
                },
            }],
            result: Expression::Literal {
                value: json!({"version": version}),
            },
        },
    }
}

fn parent_template(strategy: ReferenceStrategy) -> PlanTemplate {
    PlanTemplate {
        template_id: "template:review-parent".to_owned(),
        candidate: PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: "review_parent".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: vec![cymule_core::Step {
                        id: "invoke.review".to_owned(),
                        operation: cymule_core::Operation::Invoke {
                            definition: "review_dependency".to_owned(),
                            input: Expression::Input,
                            bind: Some("reviewed".to_owned()),
                        },
                    }],
                    result: Expression::Binding {
                        name: "reviewed".to_owned(),
                    },
                },
            }],
            metadata: BTreeMap::new(),
        },
        references: vec![SubflowReference {
            logical_ref: "subflow:review".to_owned(),
            local_definition: "review_dependency".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            strategy,
        }],
    }
}

fn reference(logical_ref: &str, local_definition: &str) -> SubflowReference {
    SubflowReference {
        logical_ref: logical_ref.to_owned(),
        local_definition: local_definition.to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        strategy: ReferenceStrategy::LatestCompatible,
    }
}

fn invoking_definition(id: &str, dependency: &str) -> Definition {
    Definition {
        id: id.to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        body: Region {
            steps: vec![cymule_core::Step {
                id: format!("invoke.{dependency}"),
                operation: cymule_core::Operation::Invoke {
                    definition: dependency.to_owned(),
                    input: Expression::Input,
                    bind: Some("result".to_owned()),
                },
            }],
            result: Expression::Binding {
                name: "result".to_owned(),
            },
        },
    }
}

struct EmptyPlugin;

impl PluginHost for EmptyPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "test.empty/1".to_owned(),
                    components: BTreeMap::new(),
                    effects: BTreeMap::new(),
                },
            }),
            _ => Err(RuntimeError::Plugin(
                "empty plugin received an executable request".to_owned(),
            )),
        }
    }
}

struct DeclaredEffectPlugin;

impl PluginHost for DeclaredEffectPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: "test.declared-effect/1".to_owned(),
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
            _ => Err(RuntimeError::Plugin(
                "safe relink unexpectedly dispatched an effect".to_owned(),
            )),
        }
    }
}

struct TestMigrationAdapter {
    descriptor: MigrationAdapterDescriptor,
    calls: usize,
}

impl MigrationAdapter for TestMigrationAdapter {
    fn describe(&mut self) -> cymule_evolution::EvolutionResult<MigrationAdapterDescriptor> {
        Ok(self.descriptor.clone())
    }

    fn migrate(
        &mut self,
        request: &MigrationRequest,
    ) -> cymule_evolution::EvolutionResult<MigrationOutput> {
        self.calls += 1;
        Ok(MigrationOutput {
            output_state: artifact(&format!("state:migrated:{}", request.migration_id)),
            evidence: artifact(&format!("evidence:migration:{}", request.migration_id)),
        })
    }
}

struct TestShadowDriver {
    descriptor: ShadowDriverDescriptor,
    equivalent: bool,
    calls: usize,
}

impl ShadowDriver for TestShadowDriver {
    fn describe(&mut self) -> cymule_evolution::EvolutionResult<ShadowDriverDescriptor> {
        Ok(self.descriptor.clone())
    }

    fn execute(
        &mut self,
        request: &ShadowRequest,
    ) -> cymule_evolution::EvolutionResult<ShadowOutput> {
        self.calls += 1;
        Ok(ShadowOutput {
            primary_digest: format!("primary:{}", request.comparison_id),
            shadow_digest: format!("shadow:{}", request.comparison_id),
            equivalent: self.equivalent,
            evidence: artifact(&format!("evidence:shadow:{}", request.comparison_id)),
        })
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
fn latest_compatible_subflow_relinks_future_parent_without_rewriting_history() {
    let mut registry = DefinitionRegistry::new();
    let first = registry
        .publish("subflow:review", reusable_definition("1", json!({})))
        .expect("first revision publishes");
    let initial = registry
        .register_template(parent_template(ReferenceStrategy::LatestCompatible))
        .expect("parent links");
    assert_eq!(
        initial.resolved_revisions["subflow:review"],
        first.revision_id
    );

    let (second, relinked) = registry
        .publish_and_relink("subflow:review", reusable_definition("2", json!({})))
        .expect("compatible revision relinks");
    assert_eq!(relinked.len(), 1);
    assert_ne!(relinked[0].plan.plan_id, initial.plan.plan_id);
    assert_eq!(
        relinked[0].resolved_revisions["subflow:review"],
        second.revision_id
    );
    assert_eq!(
        registry
            .historical_link(&initial.plan.plan_id)
            .expect("old Plan remains historical"),
        &initial
    );

    let (_, incompatible_relink) = registry
        .publish_and_relink(
            "subflow:review",
            reusable_definition("3", json!({"type": "string"})),
        )
        .expect("incompatible head keeps latest compatible revision");
    assert_eq!(
        incompatible_relink[0].plan.plan_id,
        relinked[0].plan.plan_id
    );
    assert_eq!(
        registry
            .current_link("template:review-parent")
            .expect("current link exists")
            .resolved_revisions["subflow:review"],
        second.revision_id
    );

    let pinned_template = PlanTemplate {
        template_id: "template:pinned-parent".to_owned(),
        ..parent_template(ReferenceStrategy::Pinned {
            revision_id: first.revision_id.clone(),
        })
    };
    let pinned = registry
        .register_template(pinned_template)
        .expect("pinned parent links");
    assert_eq!(
        pinned.resolved_revisions["subflow:review"],
        first.revision_id
    );
}

#[test]
fn latest_compatible_is_the_actual_reference_default() {
    assert_eq!(
        ReferenceStrategy::default(),
        ReferenceStrategy::LatestCompatible
    );
    let reference: SubflowReference = serde_json::from_value(json!({
        "logical_ref": "subflow:review",
        "local_definition": "review_dependency",
        "input_schema": {},
        "output_schema": {}
    }))
    .expect("omitted strategy uses the safe default");
    assert_eq!(reference.strategy, ReferenceStrategy::LatestCompatible);
    assert_eq!(
        reference,
        SubflowReference::latest_compatible(
            "subflow:review",
            "review_dependency",
            json!({}),
            json!({}),
        )
    );
}

#[test]
fn automatic_relink_blocks_new_effect_surface_and_later_safe_head_advances() {
    let mut registry = DefinitionRegistry::new();
    let first = registry
        .publish("subflow:review", reusable_definition("1", json!({})))
        .expect("first revision publishes");
    let mut template = parent_template(ReferenceStrategy::LatestCompatible);
    template.candidate.effects = vec![EffectContract {
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
        requirements: BTreeMap::from([
            ("capability".to_owned(), "capture".to_owned()),
            ("authority".to_owned(), "external-write".to_owned()),
        ]),
    }];
    let initial = registry
        .register_template(template)
        .expect("initial safe parent links");
    assert_eq!(
        initial.resolved_revisions["subflow:review"],
        first.revision_id
    );

    let (effectful, blocked) = registry
        .publish_and_relink("subflow:review", effectful_reusable_definition("2"))
        .expect("effectful revision publishes without taking over the default");
    assert_eq!(blocked, vec![initial.clone()]);
    assert_ne!(effectful.revision_id, first.revision_id);
    assert_eq!(
        registry
            .current_link("template:review-parent")
            .expect("current remains")
            .plan
            .plan_id,
        initial.plan.plan_id
    );
    let blocked_candidate = registry
        .snapshot()
        .templates
        .get("template:review-parent")
        .expect("template retained")
        .clone();
    let mut explicit =
        DefinitionRegistry::restore(registry.snapshot()).expect("blocked head registry restores");
    assert_eq!(
        explicit
            .register_template(blocked_candidate)
            .expect("registered template retry preserves block")
            .plan
            .plan_id,
        initial.plan.plan_id
    );

    let (_, advanced) = registry
        .publish_and_relink("subflow:review", reusable_definition("3", json!({})))
        .expect("later no-widening revision advances");
    assert_eq!(advanced.len(), 1);
    assert_ne!(advanced[0].plan.plan_id, initial.plan.plan_id);
    assert_eq!(
        EmbeddedRuntime::new(DeclaredEffectPlugin)
            .execute(advanced[0].plan.clone(), &json!({}), "run:safe-head")
            .expect("safe head executes")
            .value,
        json!({"version": "3"})
    );
}

#[test]
fn relink_analysis_treats_new_component_requirements_as_capability_widening() {
    let contract = ComponentContract {
        id: "test.compute".to_owned(),
        input_schema: json!({}),
        output_schema: json!({}),
        requirements: BTreeMap::from([
            ("capability".to_owned(), "sandbox".to_owned()),
            ("authority".to_owned(), "workspace-read".to_owned()),
        ]),
    };
    let base = PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "surface_base".to_owned(),
        entry: "main".to_owned(),
        components: vec![contract.clone()],
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
    }
    .seal()
    .expect("base seals");
    let widened = PlanCandidate {
        name: "surface_widened".to_owned(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![cymule_core::Step {
                    id: "call.compute".to_owned(),
                    operation: cymule_core::Operation::Call {
                        component: "test.compute".to_owned(),
                        input: Expression::Input,
                        bind: Some("computed".to_owned()),
                    },
                }],
                result: Expression::Binding {
                    name: "computed".to_owned(),
                },
            },
        }],
        ..base.candidate.clone()
    }
    .seal()
    .expect("widened Plan seals");
    let report = analyze_relink(&base, &widened).expect("surface analyzes");
    assert!(!report.is_compatible());
    assert!(
        report
            .violations
            .contains(&RelinkViolation::ComponentAdded {
                component: "test.compute".to_owned()
            })
    );

    let mut changed_component_candidate = widened.candidate.clone();
    changed_component_candidate.components[0]
        .requirements
        .insert("capability".to_owned(), "network".to_owned());
    let changed_component = changed_component_candidate
        .seal()
        .expect("changed component Plan seals");
    let report = analyze_relink(&widened, &changed_component).expect("component contract analyzes");
    assert!(
        report
            .violations
            .contains(&RelinkViolation::ComponentContractChanged {
                component: "test.compute".to_owned()
            })
    );

    let effect = EffectContract {
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
        requirements: BTreeMap::from([("authority".to_owned(), "workspace-write".to_owned())]),
    };
    let effect_base = PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "surface_effect".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: vec![effect],
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![cymule_core::Step {
                    id: "effect.capture".to_owned(),
                    operation: cymule_core::Operation::Effect {
                        effect: "test.capture".to_owned(),
                        input: Expression::Input,
                        occurrence: "main".to_owned(),
                        bind: None,
                    },
                }],
                result: Expression::Literal { value: json!(null) },
            },
        }],
        metadata: BTreeMap::new(),
    }
    .seal()
    .expect("effect Plan seals");
    let mut changed_effect_candidate = effect_base.candidate.clone();
    changed_effect_candidate.effects[0]
        .requirements
        .insert("authority".to_owned(), "organization-write".to_owned());
    let changed_effect = changed_effect_candidate
        .seal()
        .expect("changed effect Plan seals");
    let report = analyze_relink(&effect_base, &changed_effect).expect("effect contract analyzes");
    assert!(
        report
            .violations
            .contains(&RelinkViolation::EffectContractChanged {
                effect: "test.capture".to_owned()
            })
    );

    let wait = WaitSpec::Signal {
        key: "signal:new-work".to_owned(),
        consume_once: true,
    };
    let waiting = PlanCandidate {
        name: "surface_waiting".to_owned(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![cymule_core::Step {
                    id: "wait.new-work".to_owned(),
                    operation: cymule_core::Operation::Wait { wait: wait.clone() },
                }],
                result: Expression::Literal { value: json!(null) },
            },
        }],
        ..base.candidate.clone()
    }
    .seal()
    .expect("waiting Plan seals");
    let report = analyze_relink(&base, &waiting).expect("wait surface analyzes");
    assert!(report.violations.contains(&RelinkViolation::WaitAdded {
        wait_digest: cymule_core::canonical_digest(&wait).expect("wait hashes")
    }));
}

#[test]
fn transitive_latest_compatible_module_relinks_and_executes_new_leaf() {
    let mut registry = DefinitionRegistry::new();
    let first_leaf = registry
        .publish("subflow:normalize", reusable_definition("1", json!({})))
        .expect("leaf publishes");
    let middle = registry
        .publish_module(
            "subflow:review",
            invoking_definition("review", "normalize_dependency"),
            vec![reference("subflow:normalize", "normalize_dependency")],
        )
        .expect("module publishes");
    let initial = registry
        .register_template(parent_template(ReferenceStrategy::LatestCompatible))
        .expect("transitive parent links");
    assert_eq!(
        initial.resolved_revisions["subflow:review"],
        middle.revision_id
    );
    assert_eq!(
        initial.resolved_revisions["subflow:normalize"],
        first_leaf.revision_id
    );
    assert_eq!(initial.plan.candidate.definitions.len(), 3);
    assert_eq!(
        EmbeddedRuntime::new(EmptyPlugin)
            .execute(initial.plan.clone(), &json!({}), "run:transitive:1")
            .expect("initial module executes")
            .value,
        json!({"version": "1"})
    );

    let (second_leaf, relinked) = registry
        .publish_and_relink("subflow:normalize", reusable_definition("2", json!({})))
        .expect("leaf update relinks transitive caller");
    assert_eq!(relinked.len(), 1);
    assert_eq!(
        relinked[0].resolved_revisions["subflow:normalize"],
        second_leaf.revision_id
    );
    assert_eq!(
        relinked[0].resolved_revisions["subflow:review"],
        middle.revision_id
    );
    assert_ne!(relinked[0].plan.plan_id, initial.plan.plan_id);
    assert_eq!(
        EmbeddedRuntime::new(EmptyPlugin)
            .execute(relinked[0].plan.clone(), &json!({}), "run:transitive:2")
            .expect("relinked module executes")
            .value,
        json!({"version": "2"})
    );
    assert_eq!(
        registry
            .historical_link(&initial.plan.plan_id)
            .expect("old transitive Plan remains pinned"),
        &initial
    );
    assert_eq!(
        DefinitionRegistry::restore(registry.snapshot())
            .expect("transitive registry snapshot restores")
            .current_link("template:review-parent")
            .expect("restored transitive current link"),
        &relinked[0]
    );
}

#[test]
fn reusable_module_dependency_cycles_fail_closed() {
    let mut registry = DefinitionRegistry::new();
    registry
        .publish_module(
            "subflow:a",
            invoking_definition("a", "b_dependency"),
            vec![reference("subflow:b", "b_dependency")],
        )
        .expect("first half publishes");
    registry
        .publish_module(
            "subflow:b",
            invoking_definition("b", "a_dependency"),
            vec![reference("subflow:a", "a_dependency")],
        )
        .expect("second half publishes");
    let mut template = parent_template(ReferenceStrategy::LatestCompatible);
    template.template_id = "template:cycle".to_owned();
    template.references = vec![reference("subflow:a", "review_dependency")];
    assert!(matches!(
        registry.register_template(template),
        Err(EvolutionError::Conflict(_))
    ));
}

#[test]
fn definition_registry_snapshot_restores_history_and_rejects_tampering() {
    let mut registry = DefinitionRegistry::new();
    registry
        .publish("subflow:review", reusable_definition("1", json!({})))
        .expect("first revision publishes");
    let initial = registry
        .register_template(parent_template(ReferenceStrategy::LatestCompatible))
        .expect("parent links");
    registry
        .publish_and_relink("subflow:review", reusable_definition("2", json!({})))
        .expect("second revision relinks");

    let snapshot = registry.snapshot();
    let restored = DefinitionRegistry::restore(snapshot.clone()).expect("snapshot restores");
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(
        restored
            .historical_link(&initial.plan.plan_id)
            .expect("historical Plan survives restore"),
        &initial
    );

    let mut tampered = snapshot;
    tampered
        .revisions
        .get_mut("subflow:review")
        .expect("revision stream exists")[0]
        .definition
        .output_schema = json!({"type": "string"});
    assert!(matches!(
        DefinitionRegistry::restore(tampered),
        Err(EvolutionError::Validation(_))
    ));
}

#[test]
fn durable_latest_compatible_relink_reopens_after_lost_receipt() {
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store)
        .expect("coordinator opens")
        .initialize(&cymule_core::Machine::new())
        .expect("coordinator initializes");
    let mut registry = DefinitionRegistry::new();
    let (first, _) = DurableDefinitionRegistry::publish_and_relink_and_checkpoint(
        &mut coordinator,
        &mut registry,
        "definitions:main",
        "checkpoint:revision:1",
        "subflow:review",
        reusable_definition("1", json!({})),
    )
    .expect("first revision checkpoints");
    let initial = DurableDefinitionRegistry::register_template_and_checkpoint(
        &mut coordinator,
        &mut registry,
        "definitions:main",
        "checkpoint:template",
        parent_template(ReferenceStrategy::LatestCompatible),
    )
    .expect("template checkpoints");
    assert_eq!(
        initial.resolved_revisions["subflow:review"],
        first.revision_id
    );

    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableDefinitionRegistry::publish_and_relink_and_checkpoint(
            &mut coordinator,
            &mut registry,
            "definitions:main",
            "checkpoint:revision:2",
            "subflow:review",
            reusable_definition("2", json!({})),
        )
        .is_err()
    );
    assert_eq!(
        registry
            .current_link("template:review-parent")
            .expect("in-memory rollback keeps old link")
            .plan
            .plan_id,
        initial.plan.plan_id
    );

    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("coordinator reopens");
    let mut restored = DurableDefinitionRegistry::load(&reopened, "definitions:main")
        .expect("committed registry replays");
    let relinked = restored
        .current_link("template:review-parent")
        .expect("new link restored")
        .clone();
    assert_ne!(relinked.plan.plan_id, initial.plan.plan_id);
    assert_eq!(
        restored
            .historical_link(&initial.plan.plan_id)
            .expect("old Plan remains pinned"),
        &initial
    );

    let (_, replayed) = DurableDefinitionRegistry::publish_and_relink_and_checkpoint(
        &mut reopened,
        &mut restored,
        "definitions:main",
        "checkpoint:revision:2",
        "subflow:review",
        reusable_definition("2", json!({})),
    )
    .expect("lost receipt retries idempotently");
    assert_eq!(replayed, vec![relinked]);
}

#[test]
fn stale_definition_registry_checkpoint_rolls_back_publication() {
    let store = MemoryStore::new();
    let mut current = DurableCoordinator::open(store.clone())
        .expect("current opens")
        .initialize(&cymule_core::Machine::new())
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store).expect("stale view opens");
    let mut current_registry = DefinitionRegistry::new();
    DurableDefinitionRegistry::publish_and_relink_and_checkpoint(
        &mut current,
        &mut current_registry,
        "definitions:stale",
        "checkpoint:current",
        "subflow:review",
        reusable_definition("1", json!({})),
    )
    .expect("current writer advances");

    let mut stale_registry = DefinitionRegistry::new();
    let before = stale_registry.snapshot();
    assert!(
        DurableDefinitionRegistry::publish_and_relink_and_checkpoint(
            &mut stale,
            &mut stale_registry,
            "definitions:stale",
            "checkpoint:stale",
            "subflow:review",
            reusable_definition("2", json!({})),
        )
        .is_err()
    );
    assert_eq!(stale_registry.snapshot(), before);
    assert_eq!(
        DurableDefinitionRegistry::load(&current, "definitions:stale")
            .expect("current journal remains valid")
            .snapshot(),
        current_registry.snapshot()
    );
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
fn reviewed_patch_seals_only_when_declared_operations_match_target() {
    let first = plan("1");
    let second = plan("2");
    let operations = diff_plans(&first, &second).expect("diff computes");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("parent registers");
    let mut wrong = operations.clone();
    wrong[0].after = Some("sha256:not-the-target".to_owned());
    assert!(matches!(
        controller.apply_patch(PlanPatch {
            from_plan: first.plan_id.clone(),
            target: second.candidate.clone(),
            operations: wrong,
            evidence: artifact("evidence:wrong-patch"),
        }),
        Err(EvolutionError::Conflict(_))
    ));
    assert_eq!(controller.snapshot().plans.len(), 1);

    let edge = controller
        .apply_patch(PlanPatch {
            from_plan: first.plan_id,
            target: second.candidate,
            operations: operations.clone(),
            evidence: artifact("evidence:reviewed-patch"),
        })
        .expect("exact patch admits");
    assert_eq!(edge.operations, operations);
    assert_eq!(edge.to_plan, second.plan_id);
}

#[test]
fn frozen_evolution_control_fixture_is_closed_and_verified() {
    let fixture = include_str!("../../../tests/fixtures/evolution-control.json");
    let command: EvolutionCommand = serde_json::from_str(fixture).expect("fixture deserializes");
    command.verify().expect("fixture verifies");
    let mut malformed: serde_json::Value = serde_json::from_str(fixture).expect("JSON parses");
    malformed["provider"] = json!("must-not-enter-M4-control");
    assert!(serde_json::from_value::<EvolutionCommand>(malformed).is_err());
    let mut wrong_version: serde_json::Value = serde_json::from_str(fixture).expect("JSON parses");
    wrong_version["control_version"] = json!("cymule.evolution-control/999");
    assert!(
        serde_json::from_value::<EvolutionCommand>(wrong_version)
            .expect("shape remains readable")
            .verify()
            .is_err()
    );
}

#[test]
fn impact_matches_definition_frames_and_external_semantic_sites() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("parent registers");
    let definition_edge = controller
        .add_edge(
            &first.plan_id,
            &second,
            vec![PatchOperation {
                kind: "replace".to_owned(),
                target: "definition:main".to_owned(),
                before: Some("definition:1".to_owned()),
                after: Some("definition:2".to_owned()),
            }],
            artifact("evidence:definition-impact"),
        )
        .expect("edge registers");
    assert!(
        controller
            .impact(
                &definition_edge.edge_id,
                &[continuation(&first.plan_id)],
                &BTreeMap::new(),
            )
            .expect("definition impact computes")
            .affected_runs
            .contains("run:active")
    );

    let third = plan("3");
    let site_edge = controller
        .add_edge(
            &first.plan_id,
            &third,
            vec![PatchOperation {
                kind: "replace".to_owned(),
                target: "virtual:region/alpha".to_owned(),
                before: Some("region:1".to_owned()),
                after: Some("region:2".to_owned()),
            }],
            artifact("evidence:site-impact"),
        )
        .expect("site edge registers");
    let external_sites = BTreeMap::from([(
        "run:active".to_owned(),
        BTreeSet::from(["region/alpha".to_owned()]),
    )]);
    assert!(
        controller
            .impact_with_sites(
                &site_edge.edge_id,
                &[continuation(&first.plan_id)],
                &BTreeMap::new(),
                &external_sites,
            )
            .expect("external site impact computes")
            .affected_runs
            .contains("run:active")
    );
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
    let input_state = artifact("state:1");
    let (_, safe_point) = migration_safe_point("run:active", &first.plan_id, input_state.clone());
    let migration = MigrationReceipt {
        migration_id: "migration:1".to_owned(),
        run_id: "run:active".to_owned(),
        from_plan: first.plan_id.clone(),
        to_plan: second.plan_id.clone(),
        safe_point_id: safe_point.safe_point_id.clone(),
        source_epoch: safe_point.epoch,
        adapter_id: "migration:test".to_owned(),
        adapter_revision: "1".to_owned(),
        from_schema: "schema:1".to_owned(),
        to_schema: "schema:2".to_owned(),
        input_state,
        output_state: artifact("state:2"),
        evidence: artifact("evidence:migration"),
    };
    let mut running = continuation(&first.plan_id);
    running.status = ContinuationStatus::Running;
    assert!(MigrationSafePoint::derive(&running).is_err());
    controller
        .record_migration(migration.clone(), &safe_point)
        .expect("safe migration records");
    controller
        .record_migration(migration, &safe_point)
        .expect("migration retry is idempotent");

    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:shadow".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Shadow,
        })
        .expect("shadow rollout sets");

    let shadow = ShadowComparison {
        comparison_id: "shadow:1".to_owned(),
        subject: "run:active".to_owned(),
        decision_id: "rollout:shadow".to_owned(),
        primary_plan: first.plan_id,
        shadow_plan: second.plan_id,
        driver_id: "shadow:test".to_owned(),
        driver_revision: "1".to_owned(),
        comparison_policy: "exact/1".to_owned(),
        primary_digest: "result:a".to_owned(),
        shadow_digest: "result:a".to_owned(),
        equivalent: true,
        evidence: artifact("evidence:shadow"),
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
fn checked_migration_adapter_is_safe_point_gated_pinned_and_idempotent() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller
        .register_plan(first.clone())
        .expect("source registers");
    controller
        .register_plan(second.clone())
        .expect("target registers");
    let descriptor = MigrationAdapterDescriptor {
        adapter_id: "migration:json-state".to_owned(),
        adapter_revision: "sha256:adapter-v1".to_owned(),
        from_plan: first.plan_id.clone(),
        to_plan: second.plan_id.clone(),
        from_schema: "sha256:schema-v1".to_owned(),
        to_schema: "sha256:schema-v2".to_owned(),
        state_coverage: MigrationStateCoverage::TotalReachableState,
        failure_and_cancellation: MigrationPreservation::Preserved,
        budget_and_ownership: MigrationPreservation::Preserved,
        authority_and_effects: MigrationCapabilityChange::NoWidening,
    };
    let input_state = artifact("state:checked:input");
    let (_, safe_point) = migration_safe_point("run:migrate", &first.plan_id, input_state.clone());
    let request = MigrationRequest {
        migration_id: "migration:checked:1".to_owned(),
        run_id: "run:migrate".to_owned(),
        from_plan: first.plan_id,
        to_plan: second.plan_id,
        safe_point_id: safe_point.safe_point_id.clone(),
        source_epoch: safe_point.epoch,
        input_state,
    };
    let mut adapter = TestMigrationAdapter {
        descriptor: descriptor.clone(),
        calls: 0,
    };
    let mut mismatched_request = request.clone();
    mismatched_request.safe_point_id = "sha256:not-the-safe-point".to_owned();
    assert!(matches!(
        controller.execute_migration(&mut adapter, mismatched_request, &safe_point),
        Err(EvolutionError::Conflict(_))
    ));
    assert_eq!(adapter.calls, 0, "unsafe migration never reaches plugin");

    adapter.descriptor.to_plan = request.from_plan.clone();
    assert!(matches!(
        controller.execute_migration(&mut adapter, request.clone(), &safe_point),
        Err(EvolutionError::Conflict(_))
    ));
    assert_eq!(adapter.calls, 0, "mismatched contract never reaches plugin");

    adapter.descriptor = descriptor;
    let receipt = controller
        .execute_migration(&mut adapter, request.clone(), &safe_point)
        .expect("checked migration executes");
    assert_eq!(receipt.adapter_revision, "sha256:adapter-v1");
    assert_eq!(adapter.calls, 1);
    assert_eq!(
        controller
            .execute_migration(&mut adapter, request, &safe_point)
            .expect("retry returns retained receipt"),
        receipt
    );
    assert_eq!(adapter.calls, 1, "receipt retry does not transform twice");
}

#[test]
fn migration_safe_point_rejects_every_non_quiescent_axis_and_tampering() {
    let (base, proof) = migration_safe_point(
        "run:safe-point-matrix",
        &plan("safe-point").plan_id,
        artifact("state:safe-point-matrix"),
    );
    proof
        .verify_continuation(&base)
        .expect("derived proof verifies");
    let mut invalid = Vec::new();
    let mut running = base.clone();
    running.status = ContinuationStatus::Running;
    invalid.push(running);
    let mut no_frame = base.clone();
    no_frame.frames.clear();
    invalid.push(no_frame);
    let mut waiting = base.clone();
    waiting.wait_set.insert("wait:unsafe".to_owned());
    invalid.push(waiting);
    let mut nested = base.clone();
    nested.scope_stack.push("scope:nested".to_owned());
    invalid.push(nested);
    let mut obligated = base.clone();
    obligated
        .effect_obligations
        .insert("obligation:unsafe".to_owned());
    invalid.push(obligated);
    let mut leased = base;
    leased.authority_leases.insert("lease:unsafe".to_owned());
    invalid.push(leased);
    for continuation in invalid {
        assert!(MigrationSafePoint::derive(&continuation).is_err());
    }
    for malformed in [
        {
            let mut malformed = proof.clone();
            malformed.safe_point_version = "cymule.migration-safe-point/unknown".to_owned();
            malformed
        },
        {
            let mut malformed = proof.clone();
            malformed.run_id.clear();
            resign_safe_point(&mut malformed);
            malformed
        },
        {
            let mut malformed = proof.clone();
            malformed.plan_id.clear();
            resign_safe_point(&mut malformed);
            malformed
        },
        {
            let mut malformed = proof.clone();
            malformed.continuation_digest.pop();
            resign_safe_point(&mut malformed);
            malformed
        },
    ] {
        assert!(malformed.verify().is_err());
    }
    let mut tampered = proof;
    tampered.epoch += 1;
    assert!(tampered.verify().is_err());
}

#[test]
fn restart_under_new_plan_is_explicit_safe_point_authorization() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    let (_, safe_point) = migration_safe_point(
        "run:restart-source",
        &first.plan_id,
        artifact("state:restart-source"),
    );
    let request = RestartRequest {
        restart_id: "restart:1".to_owned(),
        source_run: "run:restart-source".to_owned(),
        replacement_run: "run:restart-target".to_owned(),
        from_plan: first.plan_id,
        to_plan: second.plan_id.clone(),
        safe_point_id: safe_point.safe_point_id.clone(),
        source_epoch: safe_point.epoch,
        input: artifact("input:restart-target"),
        evidence: artifact("evidence:restart-policy"),
    };
    let receipt = controller
        .restart_under_new_plan(request.clone(), &safe_point)
        .expect("restart authorizes");
    assert_eq!(receipt.target_plan.plan_id, second.plan_id);
    assert_eq!(
        controller
            .restart_under_new_plan(request.clone(), &safe_point)
            .expect("restart retry is idempotent"),
        receipt
    );
    for mismatched in [
        {
            let mut mismatched = request.clone();
            mismatched.restart_id = "restart:wrong-epoch".to_owned();
            mismatched.source_epoch += 1;
            mismatched
        },
        {
            let mut mismatched = request.clone();
            mismatched.restart_id = "restart:wrong-source-run".to_owned();
            mismatched.source_run = "run:wrong-source".to_owned();
            mismatched
        },
        {
            let mut mismatched = request.clone();
            mismatched.restart_id = "restart:wrong-source-plan".to_owned();
            mismatched.from_plan = "sha256:wrong-source-plan".to_owned();
            mismatched
        },
    ] {
        assert!(matches!(
            controller.restart_under_new_plan(mismatched, &safe_point),
            Err(EvolutionError::Conflict(_))
        ));
    }
    let mut reused = request.clone();
    reused.input = artifact("input:restart-conflict");
    assert!(matches!(
        controller.restart_under_new_plan(reused, &safe_point),
        Err(EvolutionError::Conflict(_))
    ));
    let mut conflicting = request;
    conflicting.replacement_run = conflicting.source_run.clone();
    assert!(matches!(
        controller.restart_under_new_plan(conflicting, &safe_point),
        Err(EvolutionError::Conflict(_))
    ));
    EvolutionController::restore(controller.snapshot()).expect("restart snapshot restores");
}

#[test]
fn shadow_gate_promotes_and_failure_gate_rolls_back_future_only() {
    let first = plan("1");
    let second = plan("2");
    let mut controller = EvolutionController::new();
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:canary:good".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Canary {
                basis_points: 10_000,
            },
        })
        .expect("canary sets");
    assert_eq!(
        controller
            .select_for_occurrence("occurrence:good")
            .expect("target pins"),
        second.plan_id
    );
    let mut shadow_driver = TestShadowDriver {
        descriptor: ShadowDriverDescriptor {
            driver_id: "shadow:embedded".to_owned(),
            driver_revision: "sha256:shadow-v1".to_owned(),
            target_effects: ShadowEffectMode::SuppressedOrSimulated,
            occurrence_bindings: ShadowBindingMode::Pinned,
        },
        equivalent: true,
        calls: 0,
    };
    let shadow_request = ShadowRequest {
        comparison_id: "shadow:good".to_owned(),
        decision_id: "rollout:canary:good".to_owned(),
        subject: "occurrence:good".to_owned(),
        primary_plan: first.plan_id.clone(),
        shadow_plan: second.plan_id.clone(),
        input: artifact("input:good"),
        comparison_policy: "json-exact/1".to_owned(),
    };
    let comparison = controller
        .execute_shadow(&mut shadow_driver, shadow_request.clone())
        .expect("shadow executes");
    assert!(comparison.equivalent);
    assert_eq!(shadow_driver.calls, 1);
    controller
        .execute_shadow(&mut shadow_driver, shadow_request)
        .expect("shadow retry returns retained evidence");
    assert_eq!(shadow_driver.calls, 1);
    controller
        .record_observation(RolloutObservation {
            observation_id: "observation:good".to_owned(),
            decision_id: "rollout:canary:good".to_owned(),
            occurrence_id: "occurrence:good".to_owned(),
            plan_id: second.plan_id.clone(),
            outcome: ObservationOutcome::Succeeded,
            evidence: artifact("evidence:good"),
        })
        .expect("success records");
    let promote_gate = RolloutGate {
        gate_id: "gate:promote".to_owned(),
        decision_id: "rollout:canary:good".to_owned(),
        min_target_observations: 1,
        max_target_failures: 0,
        min_equivalent_shadows: 1,
        max_inequivalent_shadows: 0,
    };
    assert_eq!(
        controller
            .evaluate_gate(promote_gate.clone())
            .expect("gate evaluates")
            .outcome,
        GateOutcome::Promote
    );
    let promoted = controller
        .apply_gate(promote_gate, "rollout:active")
        .expect("gate promotes");
    assert_eq!(promoted.evaluation.outcome, GateOutcome::Promote);
    assert!(matches!(
        controller.snapshot().rollout.expect("current rollout").mode,
        RolloutMode::Active
    ));

    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:canary:bad".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Canary {
                basis_points: 10_000,
            },
        })
        .expect("second canary sets");
    controller
        .select_for_occurrence("occurrence:bad")
        .expect("bad target pins");
    controller
        .record_observation(RolloutObservation {
            observation_id: "observation:bad".to_owned(),
            decision_id: "rollout:canary:bad".to_owned(),
            occurrence_id: "occurrence:bad".to_owned(),
            plan_id: second.plan_id.clone(),
            outcome: ObservationOutcome::Failed,
            evidence: artifact("evidence:bad"),
        })
        .expect("failure records");
    let rollback = controller
        .apply_gate(
            RolloutGate {
                gate_id: "gate:rollback".to_owned(),
                decision_id: "rollout:canary:bad".to_owned(),
                min_target_observations: 100,
                max_target_failures: 0,
                min_equivalent_shadows: 100,
                max_inequivalent_shadows: 0,
            },
            "rollout:rolled-back",
        )
        .expect("failure threshold rolls back immediately");
    assert_eq!(rollback.evaluation.outcome, GateOutcome::Rollback);
    assert_eq!(
        controller
            .select_for_occurrence("occurrence:bad")
            .expect("admitted occurrence remains pinned"),
        second.plan_id
    );
    assert_eq!(
        controller
            .select_for_occurrence("occurrence:after-rollback")
            .expect("future occurrence selects fallback"),
        first.plan_id
    );
    let pinned_plan = controller
        .select_plan_for_occurrence("occurrence:bad")
        .expect("runtime receives old target Plan");
    let fallback_plan = controller
        .select_plan_for_occurrence("occurrence:after-rollback")
        .expect("runtime receives fallback Plan");
    assert_eq!(
        EmbeddedRuntime::new(EmptyPlugin)
            .execute(pinned_plan, &json!({}), "run:mixed:target")
            .expect("target Plan executes")
            .value,
        json!({"version": "2"})
    );
    assert_eq!(
        EmbeddedRuntime::new(EmptyPlugin)
            .execute(fallback_plan, &json!({}), "run:mixed:fallback")
            .expect("fallback Plan executes")
            .value,
        json!({"version": "1"})
    );
    let snapshot = controller.snapshot();
    EvolutionController::restore(snapshot.clone()).expect("gated rollout restores");
    let mut tampered = snapshot;
    tampered
        .transitions
        .values_mut()
        .next()
        .expect("transition exists")
        .evaluation
        .target_failures += 1;
    assert!(EvolutionController::restore(tampered).is_err());
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

    let migration_input = artifact("state:durable:1");
    let (migration_continuation, migration_safe_point) =
        migration_safe_point("run:active", &first.plan_id, migration_input.clone());
    reopened
        .put_continuation(migration_continuation)
        .expect("migration safe point persists");
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
            safe_point_id: migration_safe_point.safe_point_id.clone(),
            source_epoch: migration_safe_point.epoch,
            adapter_id: "migration:test".to_owned(),
            adapter_revision: "1".to_owned(),
            from_schema: "schema:1".to_owned(),
            to_schema: "schema:2".to_owned(),
            input_state: migration_input,
            output_state: artifact("state:durable:2"),
            evidence: artifact("evidence:durable:migration"),
        },
        &migration_safe_point,
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
            decision_id: "rollout:rollback".to_owned(),
            primary_plan: first.plan_id,
            shadow_plan: pinned_target,
            driver_id: "shadow:test".to_owned(),
            driver_revision: "1".to_owned(),
            comparison_policy: "exact/1".to_owned(),
            primary_digest: "result:primary".to_owned(),
            shadow_digest: "result:shadow".to_owned(),
            equivalent: false,
            evidence: artifact("evidence:durable:shadow"),
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
fn durable_rollout_gate_reopens_after_lost_transition_receipt() {
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
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    DurableEvolutionController::checkpoint(
        &mut coordinator,
        &controller,
        "evolution:gate",
        "checkpoint:plans",
    )
    .expect("plans checkpoint");
    DurableEvolutionController::set_rollout_and_checkpoint(
        &mut coordinator,
        &mut controller,
        "evolution:gate",
        "checkpoint:canary",
        RolloutDecision {
            decision_id: "rollout:durable-canary".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Canary {
                basis_points: 10_000,
            },
        },
    )
    .expect("canary checkpoints");
    DurableEvolutionController::select_occurrence_and_checkpoint(
        &mut coordinator,
        &mut controller,
        "evolution:gate",
        "checkpoint:occurrence",
        "occurrence:durable-gate",
    )
    .expect("occurrence checkpoints");
    DurableEvolutionController::record_observation_and_checkpoint(
        &mut coordinator,
        &mut controller,
        "evolution:gate",
        "checkpoint:observation",
        RolloutObservation {
            observation_id: "observation:durable-gate".to_owned(),
            decision_id: "rollout:durable-canary".to_owned(),
            occurrence_id: "occurrence:durable-gate".to_owned(),
            plan_id: second.plan_id.clone(),
            outcome: ObservationOutcome::Succeeded,
            evidence: artifact("evidence:durable-gate"),
        },
    )
    .expect("observation checkpoints");
    let gate = RolloutGate {
        gate_id: "gate:durable-promote".to_owned(),
        decision_id: "rollout:durable-canary".to_owned(),
        min_target_observations: 1,
        max_target_failures: 0,
        min_equivalent_shadows: 0,
        max_inequivalent_shadows: 0,
    };

    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableEvolutionController::apply_gate_and_checkpoint(
            &mut coordinator,
            &mut controller,
            "evolution:gate",
            "checkpoint:promotion",
            gate.clone(),
            "rollout:durable-active",
        )
        .is_err()
    );
    assert!(matches!(
        controller.snapshot().rollout.expect("local rollback").mode,
        RolloutMode::Canary { .. }
    ));

    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("coordinator reopens");
    let mut restored = DurableEvolutionController::load(&reopened, "evolution:gate")
        .expect("committed transition replays");
    assert!(matches!(
        restored
            .snapshot()
            .rollout
            .expect("promotion restored")
            .mode,
        RolloutMode::Active
    ));
    let replay = DurableEvolutionController::apply_gate_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:gate",
        "checkpoint:promotion",
        gate,
        "rollout:durable-active",
    );
    assert_eq!(
        replay.expect("lost transition receipt retries"),
        restored
            .snapshot()
            .transitions
            .values()
            .next()
            .expect("transition retained")
            .clone()
    );
    assert_eq!(
        restored
            .select_for_occurrence("occurrence:durable-gate")
            .expect("old pin survives"),
        second.plan_id
    );
}

#[test]
fn durable_restart_reopens_after_lost_receipt_and_rejects_stale_proof() {
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
    let (continuation, safe_point) = migration_safe_point(
        "run:restart-durable",
        &first.plan_id,
        artifact("state:restart-durable"),
    );
    coordinator
        .put_continuation(continuation.clone())
        .expect("safe point persists");
    let mut controller = EvolutionController::new();
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    DurableEvolutionController::checkpoint(
        &mut coordinator,
        &controller,
        "evolution:restart",
        "checkpoint:plans",
    )
    .expect("plans checkpoint");

    let mut advanced = continuation;
    advanced.epoch += 1;
    let stale_proof = MigrationSafePoint::derive(&advanced).expect("stale proof derives");
    let stale_request = RestartRequest {
        restart_id: "restart:stale".to_owned(),
        source_run: stale_proof.run_id.clone(),
        replacement_run: "run:restart-stale-target".to_owned(),
        from_plan: stale_proof.plan_id.clone(),
        to_plan: second.plan_id.clone(),
        safe_point_id: stale_proof.safe_point_id.clone(),
        source_epoch: stale_proof.epoch,
        input: artifact("input:restart-stale"),
        evidence: artifact("evidence:restart-stale"),
    };
    assert!(
        DurableEvolutionController::restart_under_new_plan_and_checkpoint(
            &mut coordinator,
            &mut controller,
            "evolution:restart",
            "checkpoint:restart-stale",
            stale_request,
            &stale_proof,
        )
        .is_err()
    );
    assert!(controller.snapshot().restarts.is_empty());

    let request = RestartRequest {
        restart_id: "restart:durable".to_owned(),
        source_run: safe_point.run_id.clone(),
        replacement_run: "run:restart-durable-target".to_owned(),
        from_plan: first.plan_id,
        to_plan: second.plan_id.clone(),
        safe_point_id: safe_point.safe_point_id.clone(),
        source_epoch: safe_point.epoch,
        input: artifact("input:restart-durable"),
        evidence: artifact("evidence:restart-durable"),
    };
    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableEvolutionController::restart_under_new_plan_and_checkpoint(
            &mut coordinator,
            &mut controller,
            "evolution:restart",
            "checkpoint:restart",
            request.clone(),
            &safe_point,
        )
        .is_err()
    );
    assert!(controller.snapshot().restarts.is_empty());

    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("coordinator reopens");
    let mut restored = DurableEvolutionController::load(&reopened, "evolution:restart")
        .expect("restart receipt restores");
    let receipt = DurableEvolutionController::restart_under_new_plan_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:restart",
        "checkpoint:restart",
        request,
        &safe_point,
    )
    .expect("lost restart receipt retries");
    assert_eq!(receipt.target_plan.plan_id, second.plan_id);
    assert_eq!(restored.snapshot().restarts.len(), 1);
}

#[test]
fn durable_migration_and_shadow_do_not_repeat_plugins_after_lost_receipts() {
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
    controller.register_plan(first.clone()).expect("registers");
    controller.register_plan(second.clone()).expect("registers");
    controller
        .set_rollout(RolloutDecision {
            decision_id: "rollout:durable-shadow".to_owned(),
            fallback_plan: first.plan_id.clone(),
            target_plan: second.plan_id.clone(),
            mode: RolloutMode::Shadow,
        })
        .expect("shadow rollout sets");
    let migration_input = artifact("state:durable-plugin");
    let (migration_continuation, migration_safe_point) = migration_safe_point(
        "run:durable-plugin",
        &first.plan_id,
        migration_input.clone(),
    );
    coordinator
        .put_continuation(migration_continuation)
        .expect("migration safe point persists");
    DurableEvolutionController::checkpoint(
        &mut coordinator,
        &controller,
        "evolution:plugins",
        "checkpoint:setup",
    )
    .expect("setup checkpoints");

    let migration_request = MigrationRequest {
        migration_id: "migration:durable-plugin".to_owned(),
        run_id: "run:durable-plugin".to_owned(),
        from_plan: first.plan_id.clone(),
        to_plan: second.plan_id.clone(),
        safe_point_id: migration_safe_point.safe_point_id.clone(),
        source_epoch: migration_safe_point.epoch,
        input_state: migration_input,
    };
    let mut migration = TestMigrationAdapter {
        descriptor: MigrationAdapterDescriptor {
            adapter_id: "migration:durable-plugin".to_owned(),
            adapter_revision: "sha256:durable-plugin-v1".to_owned(),
            from_plan: first.plan_id.clone(),
            to_plan: second.plan_id.clone(),
            from_schema: "sha256:schema-v1".to_owned(),
            to_schema: "sha256:schema-v2".to_owned(),
            state_coverage: MigrationStateCoverage::TotalReachableState,
            failure_and_cancellation: MigrationPreservation::Preserved,
            budget_and_ownership: MigrationPreservation::Preserved,
            authority_and_effects: MigrationCapabilityChange::NoWidening,
        },
        calls: 0,
    };
    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableEvolutionController::execute_migration_and_checkpoint(
            &mut coordinator,
            &mut controller,
            "evolution:plugins",
            "checkpoint:migration-plugin",
            &mut migration,
            migration_request.clone(),
            &migration_safe_point,
        )
        .is_err()
    );
    assert_eq!(migration.calls, 1);

    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("reopens after migration");
    let mut restored = DurableEvolutionController::load(&reopened, "evolution:plugins")
        .expect("migration receipt restores");
    DurableEvolutionController::execute_migration_and_checkpoint(
        &mut reopened,
        &mut restored,
        "evolution:plugins",
        "checkpoint:migration-plugin",
        &mut migration,
        migration_request,
        &migration_safe_point,
    )
    .expect("migration retry uses retained receipt");
    assert_eq!(migration.calls, 1);

    let shadow_request = ShadowRequest {
        comparison_id: "shadow:durable-plugin".to_owned(),
        decision_id: "rollout:durable-shadow".to_owned(),
        subject: "subject:durable-plugin".to_owned(),
        primary_plan: first.plan_id,
        shadow_plan: second.plan_id,
        input: artifact("input:durable-shadow"),
        comparison_policy: "json-exact/1".to_owned(),
    };
    let mut shadow = TestShadowDriver {
        descriptor: ShadowDriverDescriptor {
            driver_id: "shadow:durable-plugin".to_owned(),
            driver_revision: "sha256:durable-shadow-v1".to_owned(),
            target_effects: ShadowEffectMode::SuppressedOrSimulated,
            occurrence_bindings: ShadowBindingMode::Pinned,
        },
        equivalent: true,
        calls: 0,
    };
    armed.store(true, Ordering::SeqCst);
    assert!(
        DurableEvolutionController::execute_shadow_and_checkpoint(
            &mut reopened,
            &mut restored,
            "evolution:plugins",
            "checkpoint:shadow-plugin",
            &mut shadow,
            shadow_request.clone(),
        )
        .is_err()
    );
    assert_eq!(shadow.calls, 1);

    let store = reopened.into_store();
    let mut final_coordinator = DurableCoordinator::open(store).expect("reopens after shadow");
    let mut final_controller =
        DurableEvolutionController::load(&final_coordinator, "evolution:plugins")
            .expect("shadow evidence restores");
    DurableEvolutionController::execute_shadow_and_checkpoint(
        &mut final_coordinator,
        &mut final_controller,
        "evolution:plugins",
        "checkpoint:shadow-plugin",
        &mut shadow,
        shadow_request,
    )
    .expect("shadow retry uses retained evidence");
    assert_eq!(shadow.calls, 1);
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
