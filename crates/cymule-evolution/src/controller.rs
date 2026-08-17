use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRef, SealedPlan, canonical_digest, content_id};
use cymule_durable::Continuation;
use serde::Serialize;

use crate::{
    EvolutionError, EvolutionResult, EvolutionSnapshot, GateOutcome, ImpactCone, MigrationAdapter,
    MigrationReceipt, MigrationRequest, ObservationOutcome, PatchOperation, PlanEdge, PlanNode,
    PlanPatch, RolloutDecision, RolloutEvaluation, RolloutGate, RolloutMode, RolloutObservation,
    RolloutTransition, ShadowComparison, ShadowDriver, ShadowRequest,
};

/// Deterministic reducer for Plan DAG and future-version decisions.
pub struct EvolutionController {
    snapshot: EvolutionSnapshot,
}

impl EvolutionController {
    /// Create an empty controller.
    pub fn new() -> Self {
        Self {
            snapshot: EvolutionSnapshot {
                plans: BTreeMap::new(),
                edges: BTreeMap::new(),
                rollout: None,
                rollout_decisions: BTreeMap::new(),
                occurrence_plans: BTreeMap::new(),
                migrations: BTreeMap::new(),
                shadows: BTreeMap::new(),
                observations: BTreeMap::new(),
                transitions: BTreeMap::new(),
            },
        }
    }

    /// Restore a portable evolution snapshot after validating references.
    pub fn restore(mut snapshot: EvolutionSnapshot) -> EvolutionResult<Self> {
        if let Some(rollout) = &snapshot.rollout {
            snapshot
                .rollout_decisions
                .entry(rollout.decision_id.clone())
                .or_insert_with(|| rollout.clone());
        }
        let controller = Self { snapshot };
        controller.validate()?;
        Ok(controller)
    }

    /// Register an immutable root or independently imported Plan.
    pub fn register_plan(&mut self, plan: SealedPlan) -> EvolutionResult<()> {
        plan.verify()?;
        match self.snapshot.plans.get(&plan.plan_id) {
            Some(existing) if existing.plan == plan => Ok(()),
            Some(_) => Err(EvolutionError::Conflict(format!(
                "Plan {} already exists with different content",
                plan.plan_id
            ))),
            None => {
                self.snapshot.plans.insert(
                    plan.plan_id.clone(),
                    PlanNode {
                        plan,
                        incoming: BTreeSet::new(),
                    },
                );
                Ok(())
            }
        }
    }

    /// Add one reviewed parent-to-child patch edge.
    pub fn add_edge(
        &mut self,
        from_plan: &str,
        child: &SealedPlan,
        operations: Vec<PatchOperation>,
        evidence: ArtifactRef,
    ) -> EvolutionResult<PlanEdge> {
        if !self.snapshot.plans.contains_key(from_plan) {
            return Err(EvolutionError::NotFound(format!(
                "parent Plan {from_plan} is missing"
            )));
        }
        if from_plan == child.plan_id {
            return Err(EvolutionError::Conflict(
                "Plan edge cannot point to itself".to_owned(),
            ));
        }
        self.register_plan(child.clone())?;
        if self.reachable(&child.plan_id, from_plan) {
            return Err(EvolutionError::Conflict(
                "Plan edge would create a cycle".to_owned(),
            ));
        }
        let edge_id = content_id(
            "cymule.plan-edge/1",
            &(from_plan, &child.plan_id, &operations, &evidence),
        )?;
        let edge = PlanEdge {
            edge_id: edge_id.clone(),
            from_plan: from_plan.to_owned(),
            to_plan: child.plan_id.clone(),
            operations,
            evidence,
        };
        match self.snapshot.edges.get(&edge_id) {
            Some(existing) if existing == &edge => return Ok(edge),
            Some(_) => {
                return Err(EvolutionError::Conflict(
                    "edge identity has conflicting content".to_owned(),
                ));
            }
            None => {}
        }
        self.snapshot.edges.insert(edge_id.clone(), edge.clone());
        self.snapshot
            .plans
            .get_mut(&child.plan_id)
            .expect("child exists")
            .incoming
            .insert(edge_id);
        Ok(edge)
    }

    /// Diff two sealed Plans and add the resulting immutable edge.
    pub fn add_diff_edge(
        &mut self,
        from_plan: &str,
        child: &SealedPlan,
        evidence: ArtifactRef,
    ) -> EvolutionResult<PlanEdge> {
        let parent = self
            .snapshot
            .plans
            .get(from_plan)
            .ok_or_else(|| EvolutionError::NotFound(format!("parent Plan {from_plan} is missing")))?
            .plan
            .clone();
        let operations = diff_plans(&parent, child)?;
        if operations.is_empty() {
            return Err(EvolutionError::Validation(
                "Plan edge contains no semantic change".to_owned(),
            ));
        }
        self.add_edge(from_plan, child, operations, evidence)
    }

    /// Seal and admit one reviewed patch only when its declared diff is exact.
    pub fn apply_patch(&mut self, patch: PlanPatch) -> EvolutionResult<PlanEdge> {
        let parent = self
            .snapshot
            .plans
            .get(&patch.from_plan)
            .ok_or_else(|| {
                EvolutionError::NotFound(format!("parent Plan {} is missing", patch.from_plan))
            })?
            .plan
            .clone();
        let child = patch.target.seal()?;
        let actual = diff_plans(&parent, &child)?;
        if actual.is_empty() {
            return Err(EvolutionError::Validation(
                "Plan patch contains no semantic change".to_owned(),
            ));
        }
        if actual != patch.operations {
            return Err(EvolutionError::Conflict(
                "declared Plan patch does not match the deterministic target diff".to_owned(),
            ));
        }
        self.add_edge(&patch.from_plan, &child, actual, patch.evidence)
    }

    /// Compute conservative impact over active Continuations and released effects.
    pub fn impact(
        &self,
        edge_id: &str,
        continuations: &[Continuation],
        released_effects: &BTreeMap<String, String>,
    ) -> EvolutionResult<ImpactCone> {
        self.impact_with_sites(edge_id, continuations, released_effects, &BTreeMap::new())
    }

    /// Compute impact including plugin- or higher-profile-owned semantic sites.
    pub fn impact_with_sites(
        &self,
        edge_id: &str,
        continuations: &[Continuation],
        released_effects: &BTreeMap<String, String>,
        active_sites: &BTreeMap<String, BTreeSet<String>>,
    ) -> EvolutionResult<ImpactCone> {
        let edge = self
            .snapshot
            .edges
            .get(edge_id)
            .ok_or_else(|| EvolutionError::NotFound(format!("edge {edge_id} is missing")))?;
        let changed_targets: BTreeSet<String> = edge
            .operations
            .iter()
            .map(|operation| operation.target.clone())
            .collect();
        let affected_runs = continuations
            .iter()
            .filter(|continuation| {
                continuation.plan_id == edge.from_plan
                    && (continuation.frames.iter().any(|frame| {
                        changed_targets.iter().any(|target| {
                            target_matches(target, &frame.definition_id)
                                || target_matches(target, &frame.invocation_id)
                        })
                    }) || continuation.wait_set.iter().any(|wait| {
                        changed_targets
                            .iter()
                            .any(|target| target_matches(target, wait))
                    }) || continuation.scope_stack.iter().any(|scope| {
                        changed_targets
                            .iter()
                            .any(|target| target_matches(target, scope))
                    }) || continuation.effect_obligations.iter().any(|effect| {
                        changed_targets
                            .iter()
                            .any(|target| target_matches(target, effect))
                    }) || active_sites.get(&continuation.run_id).is_some_and(|sites| {
                        sites.iter().any(|site| {
                            changed_targets
                                .iter()
                                .any(|target| target_matches(target, site))
                        })
                    }) || (continuation.state.is_some()
                        && changed_targets
                            .iter()
                            .any(|target| target.contains("schema"))))
            })
            .map(|continuation| continuation.run_id.clone())
            .collect();
        let pinned_effects = released_effects
            .iter()
            .filter(|(_, plan)| *plan == &edge.from_plan)
            .map(|(effect, _)| effect.clone())
            .collect();
        Ok(ImpactCone {
            edge_id: edge_id.to_owned(),
            requires_migration: edge
                .operations
                .iter()
                .any(|operation| operation.target.contains("schema")),
            changed_targets,
            affected_runs,
            pinned_effects,
        })
    }

    /// Set a new decision for future occurrences.
    pub fn set_rollout(&mut self, decision: RolloutDecision) -> EvolutionResult<()> {
        validate_identity("rollout decision", &decision.decision_id)?;
        if self
            .snapshot
            .transitions
            .values()
            .any(|transition| transition.from_decision == decision.decision_id)
            && self
                .snapshot
                .rollout
                .as_ref()
                .map(|current| &current.decision_id)
                != Some(&decision.decision_id)
        {
            return Err(EvolutionError::Conflict(
                "a completed rollout decision cannot become current again".to_owned(),
            ));
        }
        if !self.snapshot.plans.contains_key(&decision.fallback_plan)
            || !self.snapshot.plans.contains_key(&decision.target_plan)
        {
            return Err(EvolutionError::NotFound(
                "rollout references an unknown Plan".to_owned(),
            ));
        }
        if let RolloutMode::Canary { basis_points } = decision.mode
            && basis_points > 10_000
        {
            return Err(EvolutionError::Validation(
                "canary basis_points must be <= 10000".to_owned(),
            ));
        }
        match self.snapshot.rollout_decisions.get(&decision.decision_id) {
            Some(existing) if existing != &decision => {
                return Err(EvolutionError::Conflict(format!(
                    "rollout decision {} was reused with different content",
                    decision.decision_id
                )));
            }
            Some(_) => {}
            None => {
                self.snapshot
                    .rollout_decisions
                    .insert(decision.decision_id.clone(), decision.clone());
            }
        }
        self.snapshot.rollout = Some(decision);
        Ok(())
    }

    /// Select and pin a Plan for one newly admitted occurrence.
    pub fn select_for_occurrence(&mut self, occurrence_id: &str) -> EvolutionResult<String> {
        if let Some(plan) = self.snapshot.occurrence_plans.get(occurrence_id) {
            return Ok(plan.clone());
        }
        let rollout = self
            .snapshot
            .rollout
            .as_ref()
            .ok_or_else(|| EvolutionError::NotFound("no rollout decision exists".to_owned()))?;
        let selected = match rollout.mode {
            RolloutMode::Shadow | RolloutMode::RolledBack => rollout.fallback_plan.clone(),
            RolloutMode::Active => rollout.target_plan.clone(),
            RolloutMode::Canary { basis_points } => {
                let digest = content_id(
                    "cymule.canary/1",
                    &(rollout.decision_id.as_str(), occurrence_id),
                )?;
                let bucket = u16::from_str_radix(&digest[7..11], 16)
                    .map_err(|error| EvolutionError::Validation(error.to_string()))?
                    % 10_000;
                if bucket < basis_points {
                    rollout.target_plan.clone()
                } else {
                    rollout.fallback_plan.clone()
                }
            }
        };
        self.snapshot
            .occurrence_plans
            .insert(occurrence_id.to_owned(), selected.clone());
        Ok(selected)
    }

    /// Select and return the exact immutable Plan for runtime dispatch.
    pub fn select_plan_for_occurrence(
        &mut self,
        occurrence_id: &str,
    ) -> EvolutionResult<SealedPlan> {
        let plan_id = self.select_for_occurrence(occurrence_id)?;
        self.snapshot
            .plans
            .get(&plan_id)
            .map(|node| node.plan.clone())
            .ok_or_else(|| EvolutionError::NotFound(format!("selected Plan {plan_id} is missing")))
    }

    /// Read one registered immutable Plan without changing selection state.
    pub fn plan(&self, plan_id: &str) -> Option<&SealedPlan> {
        self.snapshot.plans.get(plan_id).map(|node| &node.plan)
    }

    /// Record a state migration only at a semantic safe point.
    pub fn record_migration(
        &mut self,
        receipt: MigrationReceipt,
        safe_point: bool,
    ) -> EvolutionResult<()> {
        if !safe_point {
            return Err(EvolutionError::Conflict(
                "state migration requires a semantic safe point".to_owned(),
            ));
        }
        validate_identity("migration", &receipt.migration_id)?;
        validate_identity("migration adapter", &receipt.adapter_id)?;
        validate_identity("migration adapter revision", &receipt.adapter_revision)?;
        validate_identity("source schema", &receipt.from_schema)?;
        validate_identity("target schema", &receipt.to_schema)?;
        if !self.snapshot.plans.contains_key(&receipt.from_plan)
            || !self.snapshot.plans.contains_key(&receipt.to_plan)
        {
            return Err(EvolutionError::NotFound(
                "migration references an unknown Plan".to_owned(),
            ));
        }
        match self.snapshot.migrations.get(&receipt.migration_id) {
            Some(existing) if existing == &receipt => Ok(()),
            Some(_) => Err(EvolutionError::Conflict(
                "migration ID was reused with different evidence".to_owned(),
            )),
            None => {
                self.snapshot
                    .migrations
                    .insert(receipt.migration_id.clone(), receipt);
                Ok(())
            }
        }
    }

    /// Execute one pinned migration plugin only after all safety gates pass.
    pub fn execute_migration<A: MigrationAdapter>(
        &mut self,
        adapter: &mut A,
        request: MigrationRequest,
        safe_point: bool,
    ) -> EvolutionResult<MigrationReceipt> {
        if !safe_point {
            return Err(EvolutionError::Conflict(
                "state migration requires a semantic safe point".to_owned(),
            ));
        }
        let descriptor = adapter.describe()?;
        validate_identity("migration adapter", &descriptor.adapter_id)?;
        validate_identity("migration adapter revision", &descriptor.adapter_revision)?;
        if descriptor.from_plan != request.from_plan || descriptor.to_plan != request.to_plan {
            return Err(EvolutionError::Conflict(
                "migration adapter Plan contract does not match the request".to_owned(),
            ));
        }
        if let Some(existing) = self.snapshot.migrations.get(&request.migration_id) {
            if existing.run_id == request.run_id
                && existing.from_plan == request.from_plan
                && existing.to_plan == request.to_plan
                && existing.input_state == request.input_state
                && existing.adapter_id == descriptor.adapter_id
                && existing.adapter_revision == descriptor.adapter_revision
                && existing.from_schema == descriptor.from_schema
                && existing.to_schema == descriptor.to_schema
            {
                return Ok(existing.clone());
            }
            return Err(EvolutionError::Conflict(
                "migration ID was reused with a different request or adapter".to_owned(),
            ));
        }
        let output = adapter.migrate(&request)?;
        let receipt = MigrationReceipt {
            migration_id: request.migration_id,
            run_id: request.run_id,
            from_plan: request.from_plan,
            to_plan: request.to_plan,
            adapter_id: descriptor.adapter_id,
            adapter_revision: descriptor.adapter_revision,
            from_schema: descriptor.from_schema,
            to_schema: descriptor.to_schema,
            input_state: request.input_state,
            output_state: output.output_state,
            evidence: output.evidence,
        };
        self.record_migration(receipt.clone(), true)?;
        Ok(receipt)
    }

    /// Record idempotent shadow comparison evidence.
    pub fn record_shadow(&mut self, comparison: ShadowComparison) -> EvolutionResult<()> {
        validate_identity("shadow comparison", &comparison.comparison_id)?;
        validate_identity("rollout decision", &comparison.decision_id)?;
        validate_identity("shadow driver", &comparison.driver_id)?;
        validate_identity("shadow driver revision", &comparison.driver_revision)?;
        validate_identity("comparison policy", &comparison.comparison_policy)?;
        let decision = self
            .snapshot
            .rollout_decisions
            .get(&comparison.decision_id)
            .ok_or_else(|| {
                EvolutionError::NotFound(format!(
                    "rollout decision {} is missing",
                    comparison.decision_id
                ))
            })?;
        if self
            .snapshot
            .rollout
            .as_ref()
            .map(|current| &current.decision_id)
            != Some(&comparison.decision_id)
        {
            return Err(EvolutionError::Conflict(
                "shadow evidence belongs to a non-current rollout decision".to_owned(),
            ));
        }
        if comparison.primary_plan != decision.fallback_plan
            || comparison.shadow_plan != decision.target_plan
        {
            return Err(EvolutionError::Conflict(
                "shadow comparison does not match its rollout Plan pair".to_owned(),
            ));
        }
        if self.snapshot.shadows.values().any(|existing| {
            existing.decision_id == comparison.decision_id
                && existing.subject == comparison.subject
                && existing.comparison_id != comparison.comparison_id
        }) {
            return Err(EvolutionError::Conflict(
                "rollout subject already has different shadow evidence".to_owned(),
            ));
        }
        match self.snapshot.shadows.get(&comparison.comparison_id) {
            Some(existing) if existing == &comparison => Ok(()),
            Some(_) => Err(EvolutionError::Conflict(
                "shadow comparison ID was reused".to_owned(),
            )),
            None => {
                self.snapshot
                    .shadows
                    .insert(comparison.comparison_id.clone(), comparison);
                Ok(())
            }
        }
    }

    /// Run a non-authoritative shadow pair through a pinned provider plugin.
    pub fn execute_shadow<D: ShadowDriver>(
        &mut self,
        driver: &mut D,
        request: ShadowRequest,
    ) -> EvolutionResult<ShadowComparison> {
        let decision = self
            .snapshot
            .rollout_decisions
            .get(&request.decision_id)
            .ok_or_else(|| {
                EvolutionError::NotFound("shadow rollout decision is missing".to_owned())
            })?;
        if decision.fallback_plan != request.primary_plan
            || decision.target_plan != request.shadow_plan
        {
            return Err(EvolutionError::Conflict(
                "shadow request does not match its rollout Plan pair".to_owned(),
            ));
        }
        let descriptor = driver.describe()?;
        validate_identity("shadow driver", &descriptor.driver_id)?;
        validate_identity("shadow driver revision", &descriptor.driver_revision)?;
        if let Some(existing) = self.snapshot.shadows.get(&request.comparison_id) {
            if existing.subject == request.subject
                && existing.decision_id == request.decision_id
                && existing.primary_plan == request.primary_plan
                && existing.shadow_plan == request.shadow_plan
                && existing.driver_id == descriptor.driver_id
                && existing.driver_revision == descriptor.driver_revision
                && existing.comparison_policy == request.comparison_policy
            {
                return Ok(existing.clone());
            }
            return Err(EvolutionError::Conflict(
                "shadow comparison ID was reused with a different request or driver".to_owned(),
            ));
        }
        let output = driver.execute(&request)?;
        let comparison = ShadowComparison {
            comparison_id: request.comparison_id,
            subject: request.subject,
            decision_id: request.decision_id,
            primary_plan: request.primary_plan,
            shadow_plan: request.shadow_plan,
            driver_id: descriptor.driver_id,
            driver_revision: descriptor.driver_revision,
            comparison_policy: request.comparison_policy,
            primary_digest: output.primary_digest,
            shadow_digest: output.shadow_digest,
            equivalent: output.equivalent,
            evidence: output.evidence,
        };
        self.record_shadow(comparison.clone())?;
        Ok(comparison)
    }

    /// Record one terminal occurrence observation exactly once.
    pub fn record_observation(&mut self, observation: RolloutObservation) -> EvolutionResult<()> {
        validate_identity("rollout observation", &observation.observation_id)?;
        let decision = self
            .snapshot
            .rollout_decisions
            .get(&observation.decision_id)
            .ok_or_else(|| {
                EvolutionError::NotFound("observation decision is missing".to_owned())
            })?;
        if self
            .snapshot
            .rollout
            .as_ref()
            .map(|current| &current.decision_id)
            != Some(&observation.decision_id)
        {
            return Err(EvolutionError::Conflict(
                "observation belongs to a non-current rollout decision".to_owned(),
            ));
        }
        if observation.plan_id != decision.fallback_plan
            && observation.plan_id != decision.target_plan
        {
            return Err(EvolutionError::Conflict(
                "observation Plan is outside its rollout decision".to_owned(),
            ));
        }
        if self
            .snapshot
            .occurrence_plans
            .get(&observation.occurrence_id)
            != Some(&observation.plan_id)
        {
            return Err(EvolutionError::Conflict(
                "observation does not match the occurrence's immutable Plan pin".to_owned(),
            ));
        }
        if self.snapshot.observations.values().any(|existing| {
            existing.decision_id == observation.decision_id
                && existing.occurrence_id == observation.occurrence_id
                && existing.observation_id != observation.observation_id
        }) {
            return Err(EvolutionError::Conflict(
                "rollout occurrence already has a different observation".to_owned(),
            ));
        }
        match self.snapshot.observations.get(&observation.observation_id) {
            Some(existing) if existing == &observation => Ok(()),
            Some(_) => Err(EvolutionError::Conflict(
                "rollout observation ID was reused".to_owned(),
            )),
            None => {
                self.snapshot
                    .observations
                    .insert(observation.observation_id.clone(), observation);
                Ok(())
            }
        }
    }

    /// Evaluate exact recorded evidence against one deterministic gate.
    pub fn evaluate_gate(&self, gate: RolloutGate) -> EvolutionResult<RolloutEvaluation> {
        validate_identity("rollout gate", &gate.gate_id)?;
        let decision = self
            .snapshot
            .rollout_decisions
            .get(&gate.decision_id)
            .ok_or_else(|| EvolutionError::NotFound("gate decision is missing".to_owned()))?;
        let target_observations: Vec<_> = self
            .snapshot
            .observations
            .values()
            .filter(|observation| {
                observation.decision_id == gate.decision_id
                    && observation.plan_id == decision.target_plan
            })
            .collect();
        let shadows: Vec<_> = self
            .snapshot
            .shadows
            .values()
            .filter(|comparison| {
                comparison.matches_pair(
                    &gate.decision_id,
                    &decision.fallback_plan,
                    &decision.target_plan,
                )
            })
            .collect();
        let target_count = u64::try_from(target_observations.len())
            .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        let failure_count = u64::try_from(
            target_observations
                .iter()
                .filter(|observation| observation.outcome == ObservationOutcome::Failed)
                .count(),
        )
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        let equivalent_count = u64::try_from(
            shadows
                .iter()
                .filter(|comparison| comparison.equivalent)
                .count(),
        )
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        let inequivalent_count = u64::try_from(
            shadows
                .iter()
                .filter(|comparison| !comparison.equivalent)
                .count(),
        )
        .map_err(|error| EvolutionError::Validation(error.to_string()))?;
        let outcome = if failure_count > gate.max_target_failures
            || inequivalent_count > gate.max_inequivalent_shadows
        {
            GateOutcome::Rollback
        } else if target_count >= gate.min_target_observations
            && equivalent_count >= gate.min_equivalent_shadows
        {
            GateOutcome::Promote
        } else {
            GateOutcome::Pending
        };
        let evidence_ids: BTreeSet<String> = target_observations
            .iter()
            .map(|observation| observation.observation_id.clone())
            .chain(
                shadows
                    .iter()
                    .map(|comparison| comparison.comparison_id.clone()),
            )
            .collect();
        let evaluation_id = content_id(
            "cymule.rollout-evaluation/1",
            &(
                &gate,
                target_count,
                failure_count,
                equivalent_count,
                inequivalent_count,
                outcome,
                &evidence_ids,
            ),
        )?;
        Ok(RolloutEvaluation {
            evaluation_id,
            gate,
            target_observations: target_count,
            target_failures: failure_count,
            equivalent_shadows: equivalent_count,
            inequivalent_shadows: inequivalent_count,
            outcome,
            evidence_ids,
        })
    }

    /// Apply a ready gate as a new future-only promotion or rollback decision.
    pub fn apply_gate(
        &mut self,
        gate: RolloutGate,
        next_decision_id: impl Into<String>,
    ) -> EvolutionResult<RolloutTransition> {
        let next_decision_id = next_decision_id.into();
        validate_identity("rollout decision", &next_decision_id)?;
        if let Some(existing) = self.snapshot.transitions.values().find(|transition| {
            transition.from_decision == gate.decision_id
                && transition.to_decision == next_decision_id
                && transition.evaluation.gate == gate
        }) {
            let recomputed = self.evaluate_gate(gate)?;
            if existing.evaluation == recomputed {
                return Ok(existing.clone());
            }
            return Err(EvolutionError::Conflict(
                "rollout transition retry observes different gate evidence".to_owned(),
            ));
        }
        if self
            .snapshot
            .rollout
            .as_ref()
            .map(|rollout| &rollout.decision_id)
            != Some(&gate.decision_id)
        {
            return Err(EvolutionError::Conflict(
                "rollout gate is stale relative to the current decision".to_owned(),
            ));
        }
        let evaluation = self.evaluate_gate(gate)?;
        let mode = match evaluation.outcome {
            GateOutcome::Pending => {
                return Err(EvolutionError::Conflict(
                    "rollout gate requires more evidence".to_owned(),
                ));
            }
            GateOutcome::Promote => RolloutMode::Active,
            GateOutcome::Rollback => RolloutMode::RolledBack,
        };
        let source = self
            .snapshot
            .rollout_decisions
            .get(&evaluation.gate.decision_id)
            .expect("gate decision exists")
            .clone();
        let next = RolloutDecision {
            decision_id: next_decision_id,
            fallback_plan: source.fallback_plan,
            target_plan: source.target_plan,
            mode,
        };
        let transition_id = content_id(
            "cymule.rollout-transition/1",
            &(&source.decision_id, &next, &evaluation),
        )?;
        let transition = RolloutTransition {
            transition_id: transition_id.clone(),
            from_decision: source.decision_id,
            to_decision: next.decision_id.clone(),
            evaluation,
        };
        if self.snapshot.transitions.contains_key(&transition_id) {
            return Err(EvolutionError::Conflict(
                "rollout transition identity has conflicting content".to_owned(),
            ));
        }
        self.set_rollout(next)?;
        self.snapshot
            .transitions
            .insert(transition_id, transition.clone());
        Ok(transition)
    }

    /// Portable complete state.
    pub fn snapshot(&self) -> EvolutionSnapshot {
        self.snapshot.clone()
    }

    fn reachable(&self, from: &str, target: &str) -> bool {
        if from == target {
            return true;
        }
        self.snapshot
            .edges
            .values()
            .any(|edge| edge.from_plan == from && self.reachable(&edge.to_plan, target))
    }

    fn validate(&self) -> EvolutionResult<()> {
        for (plan_id, node) in &self.snapshot.plans {
            node.plan.verify()?;
            if node.plan.plan_id != *plan_id {
                return Err(EvolutionError::Validation(format!(
                    "Plan node key {plan_id} does not match its content identity"
                )));
            }
            let expected_incoming: BTreeSet<String> = self
                .snapshot
                .edges
                .values()
                .filter(|edge| edge.to_plan == *plan_id)
                .map(|edge| edge.edge_id.clone())
                .collect();
            if node.incoming != expected_incoming {
                return Err(EvolutionError::Validation(format!(
                    "Plan node {plan_id} has an invalid incoming edge index"
                )));
            }
        }
        for (edge_id, edge) in &self.snapshot.edges {
            if !self.snapshot.plans.contains_key(&edge.from_plan)
                || !self.snapshot.plans.contains_key(&edge.to_plan)
            {
                return Err(EvolutionError::NotFound(
                    "Plan edge references a missing node".to_owned(),
                ));
            }
            let expected_id = content_id(
                "cymule.plan-edge/1",
                &(
                    edge.from_plan.as_str(),
                    &edge.to_plan,
                    &edge.operations,
                    &edge.evidence,
                ),
            )?;
            if edge.edge_id != *edge_id || edge.edge_id != expected_id {
                return Err(EvolutionError::Validation(format!(
                    "Plan edge {edge_id} does not match its content identity"
                )));
            }
        }
        for (decision_id, decision) in &self.snapshot.rollout_decisions {
            validate_identity("rollout decision", &decision.decision_id)?;
            if decision.decision_id != *decision_id {
                return Err(EvolutionError::Validation(format!(
                    "rollout decision key {decision_id} does not match its identity"
                )));
            }
            if !self.snapshot.plans.contains_key(&decision.fallback_plan)
                || !self.snapshot.plans.contains_key(&decision.target_plan)
            {
                return Err(EvolutionError::NotFound(
                    "rollout decision references a missing Plan".to_owned(),
                ));
            }
            if matches!(decision.mode, RolloutMode::Canary { basis_points } if basis_points > 10_000)
            {
                return Err(EvolutionError::Validation(
                    "canary basis_points must be <= 10000".to_owned(),
                ));
            }
        }
        if let Some(current) = &self.snapshot.rollout
            && self.snapshot.rollout_decisions.get(&current.decision_id) != Some(current)
        {
            return Err(EvolutionError::Conflict(
                "current rollout is absent from immutable decision history".to_owned(),
            ));
        }
        for (occurrence_id, plan_id) in &self.snapshot.occurrence_plans {
            validate_identity("occurrence", occurrence_id)?;
            if !self.snapshot.plans.contains_key(plan_id) {
                return Err(EvolutionError::NotFound(format!(
                    "occurrence {occurrence_id} references a missing Plan"
                )));
            }
        }
        for (migration_id, receipt) in &self.snapshot.migrations {
            validate_identity("migration", &receipt.migration_id)?;
            if receipt.migration_id != *migration_id {
                return Err(EvolutionError::Validation(format!(
                    "migration key {migration_id} does not match its identity"
                )));
            }
            if !self.snapshot.plans.contains_key(&receipt.from_plan)
                || !self.snapshot.plans.contains_key(&receipt.to_plan)
            {
                return Err(EvolutionError::NotFound(
                    "migration references a missing Plan".to_owned(),
                ));
            }
        }
        let mut shadow_subjects = BTreeSet::new();
        for (comparison_id, comparison) in &self.snapshot.shadows {
            if comparison.comparison_id != *comparison_id {
                return Err(EvolutionError::Validation(format!(
                    "shadow key {comparison_id} does not match its identity"
                )));
            }
            if !shadow_subjects.insert((comparison.decision_id.clone(), comparison.subject.clone()))
            {
                return Err(EvolutionError::Conflict(
                    "rollout subject has duplicate shadow evidence".to_owned(),
                ));
            }
            let decision = self
                .snapshot
                .rollout_decisions
                .get(&comparison.decision_id)
                .ok_or_else(|| {
                    EvolutionError::NotFound(
                        "shadow comparison references a missing decision".to_owned(),
                    )
                })?;
            if !comparison.matches_pair(
                &decision.decision_id,
                &decision.fallback_plan,
                &decision.target_plan,
            ) {
                return Err(EvolutionError::Conflict(
                    "shadow comparison does not match its rollout pair".to_owned(),
                ));
            }
        }
        let mut observed_occurrences = BTreeSet::new();
        for (observation_id, observation) in &self.snapshot.observations {
            if observation.observation_id != *observation_id {
                return Err(EvolutionError::Validation(format!(
                    "observation key {observation_id} does not match its identity"
                )));
            }
            if !observed_occurrences.insert((
                observation.decision_id.clone(),
                observation.occurrence_id.clone(),
            )) {
                return Err(EvolutionError::Conflict(
                    "rollout occurrence has duplicate observations".to_owned(),
                ));
            }
            let decision = self
                .snapshot
                .rollout_decisions
                .get(&observation.decision_id)
                .ok_or_else(|| {
                    EvolutionError::NotFound(
                        "rollout observation references a missing decision".to_owned(),
                    )
                })?;
            if observation.plan_id != decision.fallback_plan
                && observation.plan_id != decision.target_plan
            {
                return Err(EvolutionError::Conflict(
                    "rollout observation is outside its decision".to_owned(),
                ));
            }
            if self
                .snapshot
                .occurrence_plans
                .get(&observation.occurrence_id)
                != Some(&observation.plan_id)
            {
                return Err(EvolutionError::Conflict(
                    "rollout observation does not match its occurrence pin".to_owned(),
                ));
            }
        }
        for (transition_id, transition) in &self.snapshot.transitions {
            if !self
                .snapshot
                .rollout_decisions
                .contains_key(&transition.from_decision)
                || !self
                    .snapshot
                    .rollout_decisions
                    .contains_key(&transition.to_decision)
                || transition.evaluation.gate.decision_id != transition.from_decision
            {
                return Err(EvolutionError::Conflict(
                    "rollout transition has invalid decision lineage".to_owned(),
                ));
            }
            let recomputed = self.evaluate_gate(transition.evaluation.gate.clone())?;
            if transition.evaluation != recomputed {
                return Err(EvolutionError::Validation(format!(
                    "rollout transition {transition_id} has invalid gate evidence"
                )));
            }
            let source = &self.snapshot.rollout_decisions[&transition.from_decision];
            let target = &self.snapshot.rollout_decisions[&transition.to_decision];
            let expected_mode = match transition.evaluation.outcome {
                GateOutcome::Pending => {
                    return Err(EvolutionError::Conflict(
                        "pending rollout evaluation cannot create a transition".to_owned(),
                    ));
                }
                GateOutcome::Promote => RolloutMode::Active,
                GateOutcome::Rollback => RolloutMode::RolledBack,
            };
            if target.fallback_plan != source.fallback_plan
                || target.target_plan != source.target_plan
                || target.mode != expected_mode
            {
                return Err(EvolutionError::Conflict(
                    "rollout transition target does not match its gate outcome".to_owned(),
                ));
            }
            let expected_id = content_id(
                "cymule.rollout-transition/1",
                &(&source.decision_id, target, &transition.evaluation),
            )?;
            if transition.transition_id != *transition_id || transition.transition_id != expected_id
            {
                return Err(EvolutionError::Validation(format!(
                    "rollout transition {transition_id} does not match its content identity"
                )));
            }
        }
        let mut visiting = BTreeSet::new();
        let mut complete = BTreeSet::new();
        for plan_id in self.snapshot.plans.keys() {
            if self.visit_cycle(plan_id, &mut visiting, &mut complete) {
                return Err(EvolutionError::Conflict(
                    "Plan DAG contains a cycle".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn visit_cycle(
        &self,
        plan_id: &str,
        visiting: &mut BTreeSet<String>,
        complete: &mut BTreeSet<String>,
    ) -> bool {
        if complete.contains(plan_id) {
            return false;
        }
        if !visiting.insert(plan_id.to_owned()) {
            return true;
        }
        let cyclic = self
            .snapshot
            .edges
            .values()
            .filter(|edge| edge.from_plan == plan_id)
            .any(|edge| self.visit_cycle(&edge.to_plan, visiting, complete));
        visiting.remove(plan_id);
        complete.insert(plan_id.to_owned());
        cyclic
    }
}

fn validate_identity(kind: &str, value: &str) -> EvolutionResult<()> {
    if value.is_empty() || value.len() > 256 {
        return Err(EvolutionError::Validation(format!(
            "{kind} identity must contain 1..=256 characters"
        )));
    }
    Ok(())
}

fn target_matches(target: &str, stable_site: &str) -> bool {
    target == stable_site
        || target.split_once(':').is_some_and(|(_, suffix)| {
            suffix == stable_site || suffix.starts_with(&format!("{stable_site}:"))
        })
        || target.starts_with(&format!("{stable_site}:"))
}

/// Compute a deterministic conservative diff between two sealed Plans.
pub fn diff_plans(from: &SealedPlan, to: &SealedPlan) -> EvolutionResult<Vec<PatchOperation>> {
    from.verify()?;
    to.verify()?;
    let mut operations = Vec::new();
    if from.candidate.ir_version != to.candidate.ir_version {
        operations.push(PatchOperation {
            kind: "replace".to_owned(),
            target: "ir_version".to_owned(),
            before: Some(canonical_digest(&from.candidate.ir_version)?),
            after: Some(canonical_digest(&to.candidate.ir_version)?),
        });
    }
    if from.candidate.entry != to.candidate.entry {
        operations.push(PatchOperation {
            kind: "replace".to_owned(),
            target: "entry".to_owned(),
            before: Some(canonical_digest(&from.candidate.entry)?),
            after: Some(canonical_digest(&to.candidate.entry)?),
        });
    }
    diff_named(
        "component",
        &from.candidate.components,
        &to.candidate.components,
        |component| &component.id,
        &mut operations,
    )?;
    diff_named(
        "effect",
        &from.candidate.effects,
        &to.candidate.effects,
        |effect| &effect.id,
        &mut operations,
    )?;
    diff_named(
        "definition",
        &from.candidate.definitions,
        &to.candidate.definitions,
        |definition| &definition.id,
        &mut operations,
    )?;
    operations.sort_by(|left, right| {
        left.target
            .cmp(&right.target)
            .then_with(|| left.kind.cmp(&right.kind))
    });
    Ok(operations)
}

fn diff_named<T: Serialize>(
    prefix: &str,
    from: &[T],
    to: &[T],
    identity: impl Fn(&T) -> &String,
    operations: &mut Vec<PatchOperation>,
) -> EvolutionResult<()> {
    let before: BTreeMap<&str, &T> = from
        .iter()
        .map(|value| (identity(value).as_str(), value))
        .collect();
    let after: BTreeMap<&str, &T> = to
        .iter()
        .map(|value| (identity(value).as_str(), value))
        .collect();
    for key in before
        .keys()
        .chain(after.keys())
        .copied()
        .collect::<BTreeSet<_>>()
    {
        let old = before.get(key).copied();
        let new = after.get(key).copied();
        let old_digest = old.map(canonical_digest).transpose()?;
        let new_digest = new.map(canonical_digest).transpose()?;
        if old_digest == new_digest {
            continue;
        }
        operations.push(PatchOperation {
            kind: match (old, new) {
                (None, Some(_)) => "add",
                (Some(_), None) => "remove",
                (Some(_), Some(_)) => "replace",
                (None, None) => unreachable!("key came from at least one Plan"),
            }
            .to_owned(),
            target: format!("{prefix}:{key}"),
            before: old_digest,
            after: new_digest,
        });
    }
    Ok(())
}

impl Default for EvolutionController {
    fn default() -> Self {
        Self::new()
    }
}
