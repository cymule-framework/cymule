use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRef, SealedPlan, content_id};
use cymule_durable::Continuation;

use crate::{
    EvolutionError, EvolutionResult, EvolutionSnapshot, ImpactCone, MigrationReceipt,
    PatchOperation, PlanEdge, PlanNode, RolloutDecision, RolloutMode, ShadowComparison,
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
                occurrence_plans: BTreeMap::new(),
                migrations: BTreeMap::new(),
                shadows: BTreeMap::new(),
            },
        }
    }

    /// Restore a portable evolution snapshot after validating references.
    pub fn restore(snapshot: EvolutionSnapshot) -> EvolutionResult<Self> {
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

    /// Compute conservative impact over active Continuations and released effects.
    pub fn impact(
        &self,
        edge_id: &str,
        continuations: &[Continuation],
        released_effects: &BTreeMap<String, String>,
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
                    && continuation.frames.iter().any(|frame| {
                        changed_targets.contains(&frame.invocation_id)
                            || changed_targets.iter().any(|target| {
                                target.starts_with(&format!("{}:", frame.invocation_id))
                            })
                    })
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

    /// Record idempotent shadow comparison evidence.
    pub fn record_shadow(&mut self, comparison: ShadowComparison) -> EvolutionResult<()> {
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
        for node in self.snapshot.plans.values() {
            node.plan.verify()?;
        }
        for edge in self.snapshot.edges.values() {
            if !self.snapshot.plans.contains_key(&edge.from_plan)
                || !self.snapshot.plans.contains_key(&edge.to_plan)
            {
                return Err(EvolutionError::NotFound(
                    "Plan edge references a missing node".to_owned(),
                ));
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

impl Default for EvolutionController {
    fn default() -> Self {
        Self::new()
    }
}
