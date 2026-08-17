use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cymule_core::{ReplayAvailability, canonical_digest, content_id};
use cymule_durable::WaitActivation;

use crate::{
    ArchivedWorkIndex, ClaimedWork, CompactedWorkIndex, FrontierLimits, MaterializedPage,
    ParkReason, ParkedWork, RegionMigrationKind, RegionMigrationPlan, RegionMigrationReceipt,
    RegionMigrationRequest, SchedulingPolicy, VIRTUAL_ARCHIVE_MANIFEST_VERSION,
    VIRTUAL_COMPACTION_CERTIFICATE_VERSION, VIRTUAL_COMPACTION_CONTROL_VERSION,
    VIRTUAL_REGION_MIGRATION_VERSION, VIRTUAL_REHYDRATION_CONTROL_VERSION,
    VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchive, VirtualArchiveManifest,
    VirtualCompactionCertificate, VirtualCompactionCommand, VirtualCompactionReceipt,
    VirtualCompletionSummary, VirtualError, VirtualRegion, VirtualRehydrationCommand,
    VirtualRehydrationReceipt, VirtualResult, VirtualSnapshot, WorkItem, WorkOccurrence,
    WorkOccurrenceState, WorkResolution, virtual_archive_record,
};

/// Replaceable source of bounded pages for one virtual region.
pub trait RegionSource {
    /// Materialize at most `limit` items after the supplied cursor.
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage>;
}

/// Replaceable adapter that can split or merge opaque source cursors.
pub trait RegionMigrator {
    /// Immutable adapter binding used for this migration occurrence.
    fn binding(&self) -> &str;

    /// Produce replacement regions and immutable coverage evidence from exact
    /// active source snapshots.
    fn plan(
        &mut self,
        request: &RegionMigrationRequest,
        sources: &[VirtualRegion],
    ) -> VirtualResult<RegionMigrationPlan>;

    /// Verify target coverage evidence under the plan's pinned binding.
    fn verify(&mut self, plan: &RegionMigrationPlan) -> VirtualResult<()>;
}

/// Deterministic bounded scheduler for virtual work.
#[derive(Clone)]
pub struct VirtualScheduler {
    limits: FrontierLimits,
    snapshot: VirtualSnapshot,
}

impl VirtualScheduler {
    /// Create an empty scheduler with explicit bounds.
    pub fn new(limits: FrontierLimits) -> VirtualResult<Self> {
        Self::new_with_policy(limits, SchedulingPolicy::default())
    }

    /// Create an empty scheduler with explicit bounds and scheduling policy.
    pub fn new_with_policy(
        limits: FrontierLimits,
        scheduling_policy: SchedulingPolicy,
    ) -> VirtualResult<Self> {
        if limits.max_materialized == 0
            || limits.max_active == 0
            || limits.max_active_per_run == 0
            || limits.materialize_batch == 0
        {
            return Err(VirtualError::Validation(
                "all frontier limits must be positive".to_owned(),
            ));
        }
        validate_scheduling_policy(scheduling_policy)?;
        Ok(Self {
            limits,
            snapshot: VirtualSnapshot {
                scheduling_policy,
                regions: BTreeMap::new(),
                ready: BTreeMap::new(),
                active: BTreeMap::new(),
                parked: BTreeMap::new(),
                parked_index: BTreeMap::new(),
                known: BTreeSet::new(),
                last_run: None,
                last_region: None,
                claim_epochs: BTreeMap::new(),
                occurrences: BTreeMap::new(),
                run_weights: BTreeMap::new(),
                run_deficits: BTreeMap::new(),
                dispatch_sequence: 0,
                ready_since: BTreeMap::new(),
                retired_regions: BTreeMap::new(),
                migrations: BTreeMap::new(),
                compactions: BTreeMap::new(),
                compaction_receipts: BTreeMap::new(),
                compacted_work: BTreeMap::new(),
                compacted_regions: BTreeMap::new(),
                rehydration_receipts: BTreeMap::new(),
            },
        })
    }

    /// Restore scheduler state under the same explicit limits.
    pub fn restore(limits: FrontierLimits, mut snapshot: VirtualSnapshot) -> VirtualResult<Self> {
        validate_scheduling_policy(snapshot.scheduling_policy)?;
        snapshot.parked_index = build_parked_index(&snapshot.parked);
        for run_id in snapshot.regions.values().map(|region| &region.run_id) {
            snapshot.run_weights.entry(run_id.clone()).or_insert(1);
            snapshot.run_deficits.entry(run_id.clone()).or_insert(0);
        }
        for item in snapshot.ready.values().flatten() {
            snapshot
                .ready_since
                .entry(item.work_id.clone())
                .or_insert(0);
        }
        let scheduler = Self { limits, snapshot };
        scheduler.validate_bounds()?;
        Ok(scheduler)
    }

    /// Register an idempotent virtual region.
    pub fn register(&mut self, region: VirtualRegion) -> VirtualResult<()> {
        validate_region(&region)?;
        if self
            .snapshot
            .retired_regions
            .contains_key(&region.region_id)
        {
            return Err(VirtualError::Conflict(format!(
                "region {} is retired",
                region.region_id
            )));
        }
        let run_id = region.run_id.clone();
        match self.snapshot.regions.get(&region.region_id) {
            Some(existing) if existing == &region => Ok(()),
            Some(_) => Err(VirtualError::Conflict(format!(
                "region {} already exists with different semantics",
                region.region_id
            ))),
            None => {
                self.snapshot
                    .regions
                    .insert(region.region_id.clone(), region);
                Ok(())
            }
        }?;
        self.snapshot.run_weights.entry(run_id.clone()).or_insert(1);
        self.snapshot
            .run_deficits
            .entry(run_id.clone())
            .or_insert(0);
        Ok(())
    }

    /// Set the positive future scheduling share for one registered Run.
    pub fn set_run_weight(&mut self, run_id: &str, weight: u32) -> VirtualResult<()> {
        if weight == 0 {
            return Err(VirtualError::Validation(
                "Run fairness weight must be positive".to_owned(),
            ));
        }
        if !self
            .snapshot
            .regions
            .values()
            .any(|region| region.run_id == run_id)
        {
            return Err(VirtualError::NotFound(format!(
                "Run {run_id} has no virtual region"
            )));
        }
        let changed = self.snapshot.run_weights.get(run_id).copied() != Some(weight);
        self.snapshot.run_weights.insert(run_id.to_owned(), weight);
        if changed {
            self.snapshot.run_deficits.insert(run_id.to_owned(), 0);
        } else {
            self.snapshot
                .run_deficits
                .entry(run_id.to_owned())
                .or_insert(0);
        }
        Ok(())
    }

    /// Ask one pinned adapter to plan an opaque cursor split or merge.
    pub fn plan_migration(
        &self,
        migrator: &mut impl RegionMigrator,
        request: &RegionMigrationRequest,
    ) -> VirtualResult<RegionMigrationPlan> {
        validate_migration_request(request)?;
        if migrator.binding() != request.migration_binding {
            return Err(VirtualError::Source(
                "selected migration adapter does not match the pinned binding".to_owned(),
            ));
        }
        let mut sources = Vec::with_capacity(request.source_region_ids.len());
        for region_id in &request.source_region_ids {
            if self.snapshot.retired_regions.contains_key(region_id) {
                return Err(VirtualError::Conflict(format!(
                    "region {region_id} is already retired"
                )));
            }
            sources.push(
                self.snapshot
                    .regions
                    .get(region_id)
                    .ok_or_else(|| {
                        VirtualError::NotFound(format!("region {region_id} is missing"))
                    })?
                    .clone(),
            );
        }
        let plan = migrator.plan(request, &sources)?;
        if plan.migration_id != request.migration_id
            || plan.kind != request.kind
            || plan.migration_binding != request.migration_binding
            || plan
                .expected_sources
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != request.source_region_ids
            || plan.targets.len() != request.target_count
        {
            return Err(VirtualError::Source(
                "migration adapter changed request identity, binding, sources, or target count"
                    .to_owned(),
            ));
        }
        migrator.verify(&plan)?;
        self.validate_migration_plan(&plan)?;
        Ok(plan)
    }

    /// Atomically retire source regions and activate replacement cursors.
    pub fn migrate(
        &mut self,
        migrator: &mut impl RegionMigrator,
        plan: &RegionMigrationPlan,
    ) -> VirtualResult<RegionMigrationReceipt> {
        if migrator.binding() != plan.migration_binding {
            return Err(VirtualError::Source(
                "selected migration adapter does not match the plan binding".to_owned(),
            ));
        }
        migrator.verify(plan)?;
        let before = self.snapshot.clone();
        match self.migrate_inner(plan) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    /// Query one retained migration receipt.
    pub fn migration(&self, migration_id: &str) -> Option<&RegionMigrationReceipt> {
        self.snapshot.migrations.get(migration_id)
    }

    fn migrate_inner(
        &mut self,
        plan: &RegionMigrationPlan,
    ) -> VirtualResult<RegionMigrationReceipt> {
        if let Some(existing) = self.snapshot.migrations.get(&plan.migration_id) {
            if existing.plan == *plan {
                return Ok(existing.clone());
            }
            return Err(VirtualError::Conflict(format!(
                "migration {} already exists with different semantics",
                plan.migration_id
            )));
        }
        self.validate_migration_plan(plan)?;
        let retired_regions: BTreeSet<String> = plan.expected_sources.keys().cloned().collect();
        let active_targets: BTreeSet<String> = plan
            .targets
            .iter()
            .map(|target| target.region_id.clone())
            .collect();
        for region_id in &retired_regions {
            self.snapshot
                .retired_regions
                .insert(region_id.clone(), plan.migration_id.clone());
        }
        for target in &plan.targets {
            self.snapshot
                .regions
                .insert(target.region_id.clone(), target.clone());
        }
        let receipt = RegionMigrationReceipt {
            plan: plan.clone(),
            retired_regions,
            active_targets,
        };
        self.snapshot
            .migrations
            .insert(plan.migration_id.clone(), receipt.clone());
        self.validate_bounds()?;
        Ok(receipt)
    }

    fn validate_migration_plan(&self, plan: &RegionMigrationPlan) -> VirtualResult<()> {
        validate_migration_plan_shape(plan)?;
        let source_ids: BTreeSet<String> = plan.expected_sources.keys().cloned().collect();
        let target_ids: BTreeSet<String> = plan
            .targets
            .iter()
            .map(|target| target.region_id.clone())
            .collect();
        if target_ids.len() != plan.targets.len() || !source_ids.is_disjoint(&target_ids) {
            return Err(VirtualError::Validation(
                "migration target IDs must be unique and distinct from sources".to_owned(),
            ));
        }
        let mut run_id = None::<&str>;
        let mut source_operation = None::<&str>;
        for (region_id, expected_cursor) in &plan.expected_sources {
            if self.snapshot.retired_regions.contains_key(region_id) {
                return Err(VirtualError::Conflict(format!(
                    "migration source {region_id} is retired"
                )));
            }
            let source = self.snapshot.regions.get(region_id).ok_or_else(|| {
                VirtualError::NotFound(format!("migration source {region_id} is missing"))
            })?;
            if &source.cursor != expected_cursor {
                return Err(VirtualError::Conflict(format!(
                    "migration source {region_id} cursor changed"
                )));
            }
            match run_id {
                Some(expected) if expected != source.run_id => {
                    return Err(VirtualError::Validation(
                        "migration sources must belong to one Run".to_owned(),
                    ));
                }
                None => run_id = Some(source.run_id.as_str()),
                Some(_) => {}
            }
            match source_operation {
                Some(expected) if expected != source.source => {
                    return Err(VirtualError::Validation(
                        "migration sources must use one source operation".to_owned(),
                    ));
                }
                None => source_operation = Some(source.source.as_str()),
                Some(_) => {}
            }
        }
        for target in &plan.targets {
            validate_region(target)?;
            if self.snapshot.regions.contains_key(&target.region_id)
                || Some(target.run_id.as_str()) != run_id
                || Some(target.source.as_str()) != source_operation
            {
                return Err(VirtualError::Conflict(format!(
                    "migration target {} conflicts with region, Run, or source authority",
                    target.region_id
                )));
            }
        }
        Ok(())
    }

    /// Fill the bounded ready frontier using deterministic region order.
    pub fn fill(&mut self, source: &mut impl RegionSource) -> VirtualResult<usize> {
        let before = self.snapshot.clone();
        match self.fill_inner(source) {
            Ok(added) => Ok(added),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    fn fill_inner(&mut self, source: &mut impl RegionSource) -> VirtualResult<usize> {
        let mut added = 0;
        let mut region_ids: Vec<String> = self.snapshot.regions.keys().cloned().collect();
        rotate_after(&mut region_ids, self.snapshot.last_region.as_deref());
        for region_id in region_ids {
            let available = self
                .limits
                .max_materialized
                .saturating_sub(self.materialized_count());
            if available == 0 {
                break;
            }
            let region = self.snapshot.regions[&region_id].clone();
            if region.cursor.exhausted || self.snapshot.retired_regions.contains_key(&region_id) {
                continue;
            }
            let limit = available.min(self.limits.materialize_batch);
            let page = source.materialize(&region, limit)?;
            if page.items.len() > limit {
                return Err(VirtualError::Source(format!(
                    "source {} returned {} items for limit {limit}",
                    region.source,
                    page.items.len()
                )));
            }
            if page.next_cursor.version != region.cursor.version {
                return Err(VirtualError::Source(format!(
                    "source {} changed cursor version without migration",
                    region.source
                )));
            }
            if page.next_cursor == region.cursor && !page.next_cursor.exhausted {
                return Err(VirtualError::Source(format!(
                    "source {} returned a non-terminal stalled cursor",
                    region.source
                )));
            }
            for item in page.items {
                validate_work_item(&item, &region).map_err(|error| {
                    VirtualError::Source(format!("source {}: {error}", region.source))
                })?;
                if !self.snapshot.known.insert(item.work_id.clone()) {
                    return Err(VirtualError::Source(format!(
                        "source {} returned an empty or repeated work identity",
                        region.source
                    )));
                }
                insert_ready(&mut self.snapshot, item);
                added += 1;
            }
            self.snapshot
                .regions
                .get_mut(&region_id)
                .expect("region exists")
                .cursor = page.next_cursor;
            self.snapshot.last_region = Some(region_id);
        }
        self.validate_bounds()?;
        Ok(added)
    }

    /// Claim one fair, capability-compatible item under a fencing epoch.
    pub fn claim(
        &mut self,
        owner: &str,
        occurrence_binding: &str,
        capabilities: &BTreeSet<String>,
    ) -> VirtualResult<Option<ClaimedWork>> {
        let before = self.snapshot.clone();
        match self.claim_inner(owner, occurrence_binding, capabilities) {
            Ok(claim) => Ok(claim),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    fn claim_inner(
        &mut self,
        owner: &str,
        occurrence_binding: &str,
        capabilities: &BTreeSet<String>,
    ) -> VirtualResult<Option<ClaimedWork>> {
        if owner.is_empty() || occurrence_binding.is_empty() {
            return Err(VirtualError::Validation(
                "claim owner and occurrence binding must not be empty".to_owned(),
            ));
        }
        if self.snapshot.active.len() >= self.limits.max_active {
            return Ok(None);
        }
        let candidates = self.eligible_candidates(capabilities);
        if candidates.is_empty() {
            return Ok(None);
        }
        let rounds = candidates
            .iter()
            .map(|candidate| {
                let deficit = self
                    .snapshot
                    .run_deficits
                    .get(&candidate.run_id)
                    .copied()
                    .unwrap_or_default();
                required_deficit_rounds(deficit, candidate.cost, self.quantum(&candidate.run_id))
            })
            .min()
            .unwrap_or_default();
        for candidate in &candidates {
            let quantum = self.quantum(&candidate.run_id);
            self.snapshot
                .run_deficits
                .entry(candidate.run_id.clone())
                .and_modify(|deficit| {
                    *deficit = deficit.saturating_add(quantum.saturating_mul(rounds));
                })
                .or_insert_with(|| quantum.saturating_mul(rounds));
        }
        let candidate = candidates
            .into_iter()
            .find(|candidate| {
                self.snapshot
                    .run_deficits
                    .get(&candidate.run_id)
                    .is_some_and(|deficit| *deficit >= candidate.cost)
            })
            .ok_or_else(|| {
                VirtualError::Validation(
                    "weighted scheduler did not make an eligible candidate affordable".to_owned(),
                )
            })?;
        let run_id = candidate.run_id;
        let queue = self
            .snapshot
            .ready
            .get_mut(&run_id)
            .expect("eligible queue exists");
        let item = queue
            .remove(candidate.item_index)
            .expect("selected item exists");
        self.snapshot.ready_since.remove(&item.work_id);
        let deficit = self
            .snapshot
            .run_deficits
            .get_mut(&run_id)
            .expect("eligible Run has deficit accounting");
        *deficit = deficit.saturating_sub(item.cost);
        self.snapshot.dispatch_sequence = self.snapshot.dispatch_sequence.saturating_add(1);
        let epoch = self
            .snapshot
            .claim_epochs
            .entry(item.work_id.clone())
            .and_modify(|epoch| *epoch += 1)
            .or_insert(1);
        let occurrence_id = work_occurrence_id(&item.work_id, *epoch)?;
        let claim = ClaimedWork {
            item: item.clone(),
            owner: owner.to_owned(),
            epoch: *epoch,
            occurrence_id: occurrence_id.clone(),
            occurrence_binding: occurrence_binding.to_owned(),
        };
        let occurrence = WorkOccurrence {
            occurrence_version: VIRTUAL_WORK_OCCURRENCE_VERSION.to_owned(),
            occurrence_id: occurrence_id.clone(),
            work_id: item.work_id,
            region_id: item.region_id,
            run_id: item.run_id,
            owner: owner.to_owned(),
            epoch: *epoch,
            occurrence_binding: occurrence_binding.to_owned(),
            state: WorkOccurrenceState::Running,
            result: None,
            error: None,
            next_reason: None,
        };
        if self
            .snapshot
            .occurrences
            .insert(occurrence_id, occurrence)
            .is_some()
        {
            return Err(VirtualError::Conflict(format!(
                "work {} claim epoch {} already exists",
                claim.item.work_id, claim.epoch
            )));
        }
        self.snapshot
            .active
            .insert(claim.item.work_id.clone(), claim.clone());
        self.snapshot.last_run = Some(run_id);
        Ok(Some(claim))
    }

    /// Resolve exactly one fenced active occurrence.
    pub fn resolve(
        &mut self,
        work_id: &str,
        owner: &str,
        epoch: u64,
        resolution: &WorkResolution,
    ) -> VirtualResult<WorkOccurrence> {
        let before = self.snapshot.clone();
        match self.resolve_inner(work_id, owner, epoch, resolution) {
            Ok(occurrence) => Ok(occurrence),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    /// Publish one terminal success for the current fenced claim.
    pub fn succeed(
        &mut self,
        work_id: &str,
        owner: &str,
        epoch: u64,
        result: cymule_core::ArtifactRef,
    ) -> VirtualResult<WorkOccurrence> {
        self.resolve(work_id, owner, epoch, &WorkResolution::Succeeded { result })
    }

    /// Park a currently active claim under an indexed reason.
    pub fn park(
        &mut self,
        work_id: &str,
        owner: &str,
        epoch: u64,
        reason: ParkReason,
    ) -> VirtualResult<WorkOccurrence> {
        self.resolve(work_id, owner, epoch, &WorkResolution::Parked { reason })
    }

    fn resolve_inner(
        &mut self,
        work_id: &str,
        owner: &str,
        epoch: u64,
        resolution: &WorkResolution,
    ) -> VirtualResult<WorkOccurrence> {
        validate_resolution(resolution)?;
        let occurrence_id = work_occurrence_id(work_id, epoch)?;
        let existing = self
            .snapshot
            .occurrences
            .get(&occurrence_id)
            .ok_or_else(|| {
                VirtualError::NotFound(format!("work occurrence {occurrence_id} is missing"))
            })?
            .clone();
        if existing.owner != owner || existing.work_id != work_id || existing.epoch != epoch {
            return Err(VirtualError::Conflict(format!(
                "stale resolution for {work_id}"
            )));
        }
        if existing.state != WorkOccurrenceState::Running {
            if occurrence_matches_resolution(&existing, resolution) {
                return Ok(existing);
            }
            return Err(VirtualError::Conflict(format!(
                "work occurrence {occurrence_id} already has a different disposition"
            )));
        }
        let claim =
            self.snapshot.active.get(work_id).ok_or_else(|| {
                VirtualError::NotFound(format!("active work {work_id} is missing"))
            })?;
        if claim.owner != owner
            || claim.epoch != epoch
            || claim.occurrence_id != occurrence_id
            || claim.occurrence_binding != existing.occurrence_binding
        {
            return Err(VirtualError::Conflict(format!(
                "stale resolution for {work_id}"
            )));
        }
        let item = claim.item.clone();
        let mut resolved = existing;
        match resolution {
            WorkResolution::Succeeded { result } => {
                resolved.state = WorkOccurrenceState::Succeeded;
                resolved.result = Some(result.clone());
            }
            WorkResolution::Retry { error, next_reason } => {
                resolved.state = WorkOccurrenceState::RetryScheduled;
                resolved.error = Some(error.clone());
                resolved.next_reason.clone_from(next_reason);
            }
            WorkResolution::Parked { reason } => {
                resolved.state = WorkOccurrenceState::Parked;
                resolved.next_reason = Some(reason.clone());
            }
            WorkResolution::Failed { error } => {
                resolved.state = WorkOccurrenceState::Failed;
                resolved.error = Some(error.clone());
            }
            WorkResolution::Cancelled { reason } => {
                resolved.state = WorkOccurrenceState::Cancelled;
                resolved.error = Some(reason.clone());
            }
        }
        self.snapshot.active.remove(work_id);
        self.snapshot
            .occurrences
            .insert(occurrence_id, resolved.clone());
        match resolution {
            WorkResolution::Retry {
                next_reason: Some(reason),
                ..
            }
            | WorkResolution::Parked { reason } => {
                insert_parked(&mut self.snapshot, item, reason.clone())?;
            }
            WorkResolution::Retry {
                next_reason: None, ..
            } => insert_ready(&mut self.snapshot, item),
            WorkResolution::Succeeded { .. }
            | WorkResolution::Failed { .. }
            | WorkResolution::Cancelled { .. } => {}
        }
        self.validate_bounds()?;
        Ok(resolved)
    }

    /// Wake every item matching one exact reason.
    pub fn wake(&mut self, reason: &ParkReason) -> usize {
        let ids = self
            .snapshot
            .parked_index
            .remove(reason)
            .unwrap_or_default();
        for id in &ids {
            let parked = self.snapshot.parked.remove(id).expect("parked item exists");
            insert_ready(&mut self.snapshot, parked.item);
        }
        ids.len()
    }

    /// Wake work indexed by the exact waits completed by one M1 activation.
    pub fn wake_activation(&mut self, activation: &WaitActivation) -> usize {
        activation
            .wait_ids
            .iter()
            .map(|wait_id| {
                self.wake(&ParkReason::Wait {
                    key: wait_id.clone(),
                })
            })
            .sum()
    }

    /// Current materialized ready, active, and parked count.
    pub fn materialized_count(&self) -> usize {
        self.snapshot
            .ready
            .values()
            .map(VecDeque::len)
            .sum::<usize>()
            + self.snapshot.active.len()
            + self.snapshot.parked.len()
    }

    /// Portable snapshot.
    pub fn snapshot(&self) -> VirtualSnapshot {
        self.snapshot.clone()
    }

    /// Query one binding-pinned attempt occurrence by stable identity.
    pub fn occurrence(&self, occurrence_id: &str) -> Option<&WorkOccurrence> {
        self.snapshot.occurrences.get(occurrence_id)
    }

    /// Move one completed region's exact occurrence history into a replaceable
    /// immutable archive and retain a verified bounded certificate.
    pub fn compact(
        &mut self,
        archive: &mut impl VirtualArchive,
        command: &VirtualCompactionCommand,
    ) -> VirtualResult<VirtualCompactionReceipt> {
        let before = self.snapshot.clone();
        match self.compact_inner(archive, command) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    fn compact_inner(
        &mut self,
        archive: &mut impl VirtualArchive,
        command: &VirtualCompactionCommand,
    ) -> VirtualResult<VirtualCompactionReceipt> {
        validate_compaction_command(command)?;
        if let Some(existing) = self.snapshot.compaction_receipts.get(&command.command_id) {
            if existing.command == *command {
                return Ok(existing.clone());
            }
            return Err(VirtualError::Conflict(format!(
                "virtual compaction command {} was reused with different semantics",
                command.command_id
            )));
        }
        if archive.binding() != command.compactor_binding {
            return Err(VirtualError::Source(
                "selected archive does not match the pinned compactor binding".to_owned(),
            ));
        }
        if self
            .snapshot
            .compacted_regions
            .contains_key(&command.region_id)
        {
            return Err(VirtualError::Conflict(format!(
                "region {} is already compacted",
                command.region_id
            )));
        }
        let region = self
            .snapshot
            .regions
            .get(&command.region_id)
            .ok_or_else(|| {
                VirtualError::NotFound(format!("region {} is missing", command.region_id))
            })?
            .clone();
        if !region.cursor.exhausted
            && !self
                .snapshot
                .retired_regions
                .contains_key(&command.region_id)
        {
            return Err(VirtualError::Conflict(format!(
                "region {} is neither exhausted nor retired",
                command.region_id
            )));
        }
        if self.region_is_materialized(&command.region_id) {
            return Err(VirtualError::Conflict(format!(
                "region {} still has ready, active, or parked work",
                command.region_id
            )));
        }
        let occurrences: BTreeMap<String, WorkOccurrence> = self
            .snapshot
            .occurrences
            .iter()
            .filter(|(_, occurrence)| occurrence.region_id == command.region_id)
            .map(|(id, occurrence)| (id.clone(), occurrence.clone()))
            .collect();
        if occurrences.is_empty() {
            return Err(VirtualError::Conflict(format!(
                "region {} has no occurrence history to compact",
                command.region_id
            )));
        }
        let work_index = archived_work_index(&occurrences)?;
        let manifest = VirtualArchiveManifest {
            manifest_version: VIRTUAL_ARCHIVE_MANIFEST_VERSION.to_owned(),
            region_id: region.region_id.clone(),
            run_id: region.run_id.clone(),
            source_causal_cut: command.source_causal_cut.clone(),
            occurrences,
            work_index,
        };
        let record = virtual_archive_record(&manifest)?;
        archive.put(&record.reference, &record.bytes)?;
        if archive.get(&record.reference)? != record.bytes {
            return Err(VirtualError::Source(
                "archive readback does not match the stored manifest".to_owned(),
            ));
        }
        let summary = completion_summary(&manifest)?;
        let retained_occurrence_bindings = manifest
            .occurrences
            .values()
            .map(|occurrence| occurrence.occurrence_binding.clone())
            .collect();
        let mut certificate = VirtualCompactionCertificate {
            certificate_version: VIRTUAL_COMPACTION_CERTIFICATE_VERSION.to_owned(),
            certificate_id: String::new(),
            source_causal_cut: command.source_causal_cut.clone(),
            summary,
            summary_state_digest: canonical_digest(&manifest)
                .map_err(|error| VirtualError::Validation(error.to_string()))?,
            unresolved_obligations: BTreeSet::new(),
            retained_occurrence_bindings,
            replay_availability: ReplayAvailability::Exact,
            rehydration_manifest: record.reference,
            compactor_binding: command.compactor_binding.clone(),
            compactor_revision: command.compactor_revision.clone(),
        };
        certificate.certificate_id = virtual_certificate_id(&certificate)?;
        validate_manifest_certificate(&manifest, &certificate)?;

        for occurrence_id in manifest.occurrences.keys() {
            self.snapshot.occurrences.remove(occurrence_id);
        }
        for archived in manifest.work_index.values() {
            self.snapshot.compacted_work.insert(
                archived.work_id.clone(),
                CompactedWorkIndex {
                    work_id: archived.work_id.clone(),
                    region_id: archived.region_id.clone(),
                    run_id: archived.run_id.clone(),
                    max_epoch: archived.max_epoch,
                    terminal_state: archived.terminal_state,
                    certificate_id: certificate.certificate_id.clone(),
                },
            );
        }
        let receipt = VirtualCompactionReceipt {
            command: command.clone(),
            certificate: certificate.clone(),
        };
        self.snapshot.compacted_regions.insert(
            command.region_id.clone(),
            certificate.certificate_id.clone(),
        );
        self.snapshot
            .compactions
            .insert(certificate.certificate_id.clone(), certificate);
        self.snapshot
            .compaction_receipts
            .insert(command.command_id.clone(), receipt.clone());
        self.validate_bounds()?;
        Ok(receipt)
    }

    /// Restore selected exact occurrence records from one verified cold archive.
    pub fn rehydrate(
        &mut self,
        archive: &mut impl VirtualArchive,
        command: &VirtualRehydrationCommand,
    ) -> VirtualResult<VirtualRehydrationReceipt> {
        let before = self.snapshot.clone();
        match self.rehydrate_inner(archive, command) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.snapshot = before;
                Err(error)
            }
        }
    }

    fn rehydrate_inner(
        &mut self,
        archive: &mut impl VirtualArchive,
        command: &VirtualRehydrationCommand,
    ) -> VirtualResult<VirtualRehydrationReceipt> {
        validate_rehydration_command(command)?;
        if let Some(existing) = self.snapshot.rehydration_receipts.get(&command.command_id) {
            if existing.command == *command {
                return Ok(existing.clone());
            }
            return Err(VirtualError::Conflict(format!(
                "virtual rehydration command {} was reused with different semantics",
                command.command_id
            )));
        }
        let certificate = self
            .snapshot
            .compactions
            .get(&command.certificate_id)
            .ok_or_else(|| {
                VirtualError::NotFound(format!(
                    "compaction certificate {} is missing",
                    command.certificate_id
                ))
            })?
            .clone();
        if archive.binding() != certificate.compactor_binding {
            return Err(VirtualError::Source(
                "selected archive does not match the certificate binding".to_owned(),
            ));
        }
        let bytes = archive.get(&certificate.rehydration_manifest)?;
        let manifest: VirtualArchiveManifest = serde_json::from_slice(&bytes)
            .map_err(|error| VirtualError::Source(error.to_string()))?;
        let record = virtual_archive_record(&manifest)?;
        if record.reference != certificate.rehydration_manifest || record.bytes != bytes {
            return Err(VirtualError::Source(
                "archive manifest bytes do not match their content reference".to_owned(),
            ));
        }
        validate_manifest_certificate(&manifest, &certificate)?;
        let mut restored = BTreeSet::new();
        for occurrence_id in &command.occurrence_ids {
            let occurrence = manifest.occurrences.get(occurrence_id).ok_or_else(|| {
                VirtualError::NotFound(format!(
                    "occurrence {occurrence_id} is absent from certificate {}",
                    certificate.certificate_id
                ))
            })?;
            match self.snapshot.occurrences.get(occurrence_id) {
                Some(existing) if existing == occurrence => {}
                Some(_) => {
                    return Err(VirtualError::Conflict(format!(
                        "rehydrated occurrence {occurrence_id} conflicts with hot history"
                    )));
                }
                None => {
                    self.snapshot
                        .occurrences
                        .insert(occurrence_id.clone(), occurrence.clone());
                }
            }
            restored.insert(occurrence_id.clone());
        }
        let receipt = VirtualRehydrationReceipt {
            command: command.clone(),
            restored_occurrence_ids: restored,
        };
        self.snapshot
            .rehydration_receipts
            .insert(command.command_id.clone(), receipt.clone());
        self.validate_bounds()?;
        Ok(receipt)
    }

    fn region_is_materialized(&self, region_id: &str) -> bool {
        self.snapshot
            .ready
            .values()
            .flatten()
            .any(|item| item.region_id == region_id)
            || self
                .snapshot
                .active
                .values()
                .any(|claim| claim.item.region_id == region_id)
            || self
                .snapshot
                .parked
                .values()
                .any(|parked| parked.item.region_id == region_id)
    }

    fn eligible_runs(&self) -> Vec<String> {
        let mut runs: Vec<String> = self
            .snapshot
            .ready
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(run, _)| run.clone())
            .collect();
        rotate_after(&mut runs, self.snapshot.last_run.as_deref());
        runs
    }

    fn eligible_candidates(&self, capabilities: &BTreeSet<String>) -> Vec<RunCandidate> {
        self.eligible_runs()
            .into_iter()
            .filter_map(|run_id| {
                let active_for_run = self
                    .snapshot
                    .active
                    .values()
                    .filter(|claim| claim.item.run_id == run_id)
                    .count();
                if active_for_run >= self.limits.max_active_per_run {
                    return None;
                }
                let queue = self.snapshot.ready.get(&run_id)?;
                let mut best = None::<(usize, i128)>;
                for (index, item) in queue.iter().enumerate() {
                    if item
                        .capability
                        .as_ref()
                        .is_some_and(|capability| !capabilities.contains(capability))
                    {
                        continue;
                    }
                    let score = self.effective_priority(item);
                    if best.is_none_or(|(_, current)| score > current) {
                        best = Some((index, score));
                    }
                }
                let (item_index, _) = best?;
                Some(RunCandidate {
                    run_id,
                    item_index,
                    cost: queue[item_index].cost,
                })
            })
            .collect()
    }

    fn effective_priority(&self, item: &WorkItem) -> i128 {
        let since = self
            .snapshot
            .ready_since
            .get(&item.work_id)
            .copied()
            .unwrap_or(self.snapshot.dispatch_sequence);
        let age = self.snapshot.dispatch_sequence.saturating_sub(since)
            / self.snapshot.scheduling_policy.aging_interval;
        i128::from(item.priority) + i128::from(age)
    }

    fn quantum(&self, run_id: &str) -> u64 {
        self.snapshot
            .scheduling_policy
            .base_quantum
            .saturating_mul(u64::from(
                self.snapshot.run_weights.get(run_id).copied().unwrap_or(1),
            ))
    }

    fn validate_bounds(&self) -> VirtualResult<()> {
        validate_scheduling_policy(self.snapshot.scheduling_policy)?;
        let registered_runs: BTreeSet<String> = self
            .snapshot
            .regions
            .values()
            .map(|region| region.run_id.clone())
            .collect();
        if self
            .snapshot
            .run_weights
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != registered_runs
            || self
                .snapshot
                .run_deficits
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != registered_runs
            || self
                .snapshot
                .run_weights
                .values()
                .any(|weight| *weight == 0)
        {
            return Err(VirtualError::Validation(
                "Run fairness accounting does not match registered Runs".to_owned(),
            ));
        }
        if self
            .snapshot
            .last_region
            .as_ref()
            .is_some_and(|region_id| !self.snapshot.regions.contains_key(region_id))
        {
            return Err(VirtualError::Validation(
                "last materialized region is not registered".to_owned(),
            ));
        }
        for (region_id, migration_id) in &self.snapshot.retired_regions {
            if !self.snapshot.regions.contains_key(region_id)
                || !self.snapshot.migrations.contains_key(migration_id)
            {
                return Err(VirtualError::Validation(format!(
                    "retired region {region_id} has no region or migration receipt"
                )));
            }
        }
        for (migration_id, receipt) in &self.snapshot.migrations {
            validate_migration_plan_shape(&receipt.plan)?;
            let expected_sources: BTreeSet<String> =
                receipt.plan.expected_sources.keys().cloned().collect();
            let expected_targets: BTreeSet<String> = receipt
                .plan
                .targets
                .iter()
                .map(|target| target.region_id.clone())
                .collect();
            if receipt.plan.migration_id != *migration_id
                || receipt.retired_regions != expected_sources
                || receipt.active_targets != expected_targets
            {
                return Err(VirtualError::Validation(format!(
                    "migration receipt {migration_id} does not match its plan"
                )));
            }
            for (source_id, cursor) in &receipt.plan.expected_sources {
                let source = self.snapshot.regions.get(source_id).ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "migration {migration_id} source {source_id} is missing"
                    ))
                })?;
                if &source.cursor != cursor
                    || self.snapshot.retired_regions.get(source_id) != Some(migration_id)
                {
                    return Err(VirtualError::Validation(format!(
                        "migration {migration_id} source retirement is inconsistent"
                    )));
                }
            }
            for target in &receipt.plan.targets {
                let current = self
                    .snapshot
                    .regions
                    .get(&target.region_id)
                    .ok_or_else(|| {
                        VirtualError::Validation(format!(
                            "migration {migration_id} target {} is missing",
                            target.region_id
                        ))
                    })?;
                if current.run_id != target.run_id || current.source != target.source {
                    return Err(VirtualError::Validation(format!(
                        "migration {migration_id} target authority changed"
                    )));
                }
            }
        }
        self.validate_compaction_state()?;
        if self.materialized_count() > self.limits.max_materialized {
            return Err(VirtualError::Validation(
                "snapshot exceeds max_materialized".to_owned(),
            ));
        }
        if self.snapshot.active.len() > self.limits.max_active {
            return Err(VirtualError::Validation(
                "snapshot exceeds max_active".to_owned(),
            ));
        }
        let mut materialized = BTreeSet::new();
        let mut ready_ids = BTreeSet::new();
        let mut active_per_run = BTreeMap::<String, usize>::new();
        for (run_id, queue) in &self.snapshot.ready {
            for item in queue {
                if &item.run_id != run_id {
                    return Err(VirtualError::Validation(format!(
                        "ready work {} is stored under the wrong Run",
                        item.work_id
                    )));
                }
                self.validate_snapshot_item(item, &mut materialized)?;
                ready_ids.insert(item.work_id.clone());
            }
        }
        for (work_id, claim) in &self.snapshot.active {
            if &claim.item.work_id != work_id
                || claim.owner.is_empty()
                || claim.epoch == 0
                || claim.occurrence_id.is_empty()
                || claim.occurrence_binding.is_empty()
            {
                return Err(VirtualError::Validation(format!(
                    "active claim {work_id} is malformed"
                )));
            }
            if self
                .snapshot
                .claim_epochs
                .get(work_id)
                .is_none_or(|epoch| *epoch < claim.epoch)
            {
                return Err(VirtualError::Validation(format!(
                    "active claim {work_id} is not covered by its fencing epoch"
                )));
            }
            let occurrence = self
                .snapshot
                .occurrences
                .get(&claim.occurrence_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!("active claim {work_id} has no occurrence"))
                })?;
            if occurrence.state != WorkOccurrenceState::Running
                || occurrence.work_id != *work_id
                || occurrence.owner != claim.owner
                || occurrence.epoch != claim.epoch
                || occurrence.occurrence_binding != claim.occurrence_binding
            {
                return Err(VirtualError::Validation(format!(
                    "active claim {work_id} disagrees with its occurrence"
                )));
            }
            *active_per_run.entry(claim.item.run_id.clone()).or_default() += 1;
            self.validate_snapshot_item(&claim.item, &mut materialized)?;
        }
        if active_per_run
            .values()
            .any(|count| *count > self.limits.max_active_per_run)
        {
            return Err(VirtualError::Validation(
                "snapshot exceeds max_active_per_run".to_owned(),
            ));
        }
        for (work_id, parked) in &self.snapshot.parked {
            if &parked.item.work_id != work_id {
                return Err(VirtualError::Validation(format!(
                    "parked work {work_id} has a mismatched identity"
                )));
            }
            self.validate_snapshot_item(&parked.item, &mut materialized)?;
        }
        if self.snapshot.parked_index != build_parked_index(&self.snapshot.parked) {
            return Err(VirtualError::Validation(
                "parked reason index does not match parked work".to_owned(),
            ));
        }
        if self
            .snapshot
            .ready_since
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            != ready_ids
            || self
                .snapshot
                .ready_since
                .values()
                .any(|sequence| *sequence > self.snapshot.dispatch_sequence)
        {
            return Err(VirtualError::Validation(
                "priority-aging timestamps do not match ready work".to_owned(),
            ));
        }
        let mut max_occurrence_epochs = BTreeMap::<String, u64>::new();
        for (occurrence_id, occurrence) in &self.snapshot.occurrences {
            validate_occurrence(occurrence_id, occurrence)?;
            if !self.snapshot.known.contains(&occurrence.work_id) {
                return Err(VirtualError::Validation(format!(
                    "work occurrence {occurrence_id} references unknown work"
                )));
            }
            let region = self
                .snapshot
                .regions
                .get(&occurrence.region_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "work occurrence {occurrence_id} references a missing region"
                    ))
                })?;
            if occurrence.run_id != region.run_id {
                return Err(VirtualError::Validation(format!(
                    "work occurrence {occurrence_id} escaped its Run"
                )));
            }
            max_occurrence_epochs
                .entry(occurrence.work_id.clone())
                .and_modify(|epoch| *epoch = (*epoch).max(occurrence.epoch))
                .or_insert(occurrence.epoch);
            match occurrence.state {
                WorkOccurrenceState::Running => {
                    if !self.snapshot.active.contains_key(&occurrence.work_id) {
                        return Err(VirtualError::Validation(format!(
                            "running work occurrence {occurrence_id} has no active claim"
                        )));
                    }
                }
                WorkOccurrenceState::RetryScheduled
                | WorkOccurrenceState::Parked
                | WorkOccurrenceState::Succeeded
                | WorkOccurrenceState::Failed
                | WorkOccurrenceState::Cancelled => {}
            }
        }
        for (work_id, epoch) in &self.snapshot.claim_epochs {
            let archived_epoch = self
                .snapshot
                .compacted_work
                .get(work_id)
                .map(|index| index.max_epoch);
            let observed_epoch = max_occurrence_epochs
                .get(work_id)
                .copied()
                .into_iter()
                .chain(archived_epoch)
                .max();
            if observed_epoch != Some(*epoch) {
                return Err(VirtualError::Validation(format!(
                    "work {work_id} claim epoch does not match its occurrence history"
                )));
            }
            if let Some(occurrence) = self
                .snapshot
                .occurrences
                .values()
                .find(|occurrence| occurrence.work_id == *work_id && occurrence.epoch == *epoch)
            {
                let is_materialized = materialized.contains(work_id);
                if matches!(
                    occurrence.state,
                    WorkOccurrenceState::RetryScheduled | WorkOccurrenceState::Parked
                ) && !is_materialized
                {
                    return Err(VirtualError::Validation(format!(
                        "rescheduled work {work_id} is not materialized"
                    )));
                }
                if matches!(
                    occurrence.state,
                    WorkOccurrenceState::Succeeded
                        | WorkOccurrenceState::Failed
                        | WorkOccurrenceState::Cancelled
                ) && is_materialized
                {
                    return Err(VirtualError::Validation(format!(
                        "terminal work {work_id} remains materialized"
                    )));
                }
            }
        }
        Ok(())
    }

    fn validate_compaction_state(&self) -> VirtualResult<()> {
        for (certificate_id, certificate) in &self.snapshot.compactions {
            validate_virtual_certificate(certificate)?;
            if certificate.certificate_id != *certificate_id {
                return Err(VirtualError::Validation(format!(
                    "compaction certificate {certificate_id} is stored under the wrong identity"
                )));
            }
            let region = self
                .snapshot
                .regions
                .get(&certificate.summary.region_id)
                .ok_or_else(|| {
                    VirtualError::Validation(format!(
                        "compaction certificate {certificate_id} references a missing region"
                    ))
                })?;
            if region.run_id != certificate.summary.run_id
                || self.snapshot.compacted_regions.get(&region.region_id) != Some(certificate_id)
            {
                return Err(VirtualError::Validation(format!(
                    "compaction certificate {certificate_id} escaped its region or Run"
                )));
            }
            let indexed_work = self
                .snapshot
                .compacted_work
                .values()
                .filter(|index| index.certificate_id == *certificate_id)
                .count();
            if u64::try_from(indexed_work).ok() != Some(certificate.summary.work_count) {
                return Err(VirtualError::Validation(format!(
                    "compaction certificate {certificate_id} work count disagrees with its retained index"
                )));
            }
        }
        for (region_id, certificate_id) in &self.snapshot.compacted_regions {
            if !self.snapshot.regions.contains_key(region_id)
                || !self.snapshot.compactions.contains_key(certificate_id)
            {
                return Err(VirtualError::Validation(format!(
                    "compacted region {region_id} has no region or certificate"
                )));
            }
        }
        for (work_id, index) in &self.snapshot.compacted_work {
            if index.work_id != *work_id
                || index.max_epoch == 0
                || !is_terminal_work_state(index.terminal_state)
                || !self.snapshot.known.contains(work_id)
                || !self
                    .snapshot
                    .compactions
                    .contains_key(&index.certificate_id)
            {
                return Err(VirtualError::Validation(format!(
                    "compacted work index {work_id} is malformed"
                )));
            }
            let region = self.snapshot.regions.get(&index.region_id).ok_or_else(|| {
                VirtualError::Validation(format!(
                    "compacted work index {work_id} references a missing region"
                ))
            })?;
            if region.run_id != index.run_id
                || self.snapshot.compacted_regions.get(&index.region_id)
                    != Some(&index.certificate_id)
            {
                return Err(VirtualError::Validation(format!(
                    "compacted work index {work_id} escaped its certificate authority"
                )));
            }
        }
        for (command_id, receipt) in &self.snapshot.compaction_receipts {
            if receipt.command.command_id != *command_id
                || receipt.command.control_version != VIRTUAL_COMPACTION_CONTROL_VERSION
                || receipt.command.region_id != receipt.certificate.summary.region_id
                || receipt.command.source_causal_cut != receipt.certificate.source_causal_cut
                || receipt.command.compactor_binding != receipt.certificate.compactor_binding
                || receipt.command.compactor_revision != receipt.certificate.compactor_revision
                || self
                    .snapshot
                    .compactions
                    .get(&receipt.certificate.certificate_id)
                    != Some(&receipt.certificate)
            {
                return Err(VirtualError::Validation(format!(
                    "compaction receipt {command_id} is inconsistent"
                )));
            }
        }
        for (command_id, receipt) in &self.snapshot.rehydration_receipts {
            if receipt.command.command_id != *command_id
                || receipt.command.control_version != VIRTUAL_REHYDRATION_CONTROL_VERSION
                || receipt.restored_occurrence_ids != receipt.command.occurrence_ids
                || !self
                    .snapshot
                    .compactions
                    .contains_key(&receipt.command.certificate_id)
                || receipt
                    .restored_occurrence_ids
                    .iter()
                    .any(|id| !self.snapshot.occurrences.contains_key(id))
            {
                return Err(VirtualError::Validation(format!(
                    "rehydration receipt {command_id} is inconsistent"
                )));
            }
        }
        Ok(())
    }

    fn validate_snapshot_item(
        &self,
        item: &WorkItem,
        materialized: &mut BTreeSet<String>,
    ) -> VirtualResult<()> {
        let region = self.snapshot.regions.get(&item.region_id).ok_or_else(|| {
            VirtualError::Validation(format!("work {} references a missing region", item.work_id))
        })?;
        validate_work_item(item, region)?;
        if !materialized.insert(item.work_id.clone()) {
            return Err(VirtualError::Validation(format!(
                "work {} appears in more than one scheduler state",
                item.work_id
            )));
        }
        if !self.snapshot.known.contains(&item.work_id) {
            return Err(VirtualError::Validation(format!(
                "materialized work {} is absent from the known set",
                item.work_id
            )));
        }
        Ok(())
    }
}

struct RunCandidate {
    run_id: String,
    item_index: usize,
    cost: u64,
}

fn required_deficit_rounds(deficit: u64, cost: u64, quantum: u64) -> u64 {
    if deficit >= cost {
        return 0;
    }
    let missing = cost - deficit;
    missing.saturating_add(quantum - 1) / quantum
}

fn validate_scheduling_policy(policy: SchedulingPolicy) -> VirtualResult<()> {
    if policy.base_quantum == 0 || policy.aging_interval == 0 {
        return Err(VirtualError::Validation(
            "scheduling quantum and aging interval must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_compaction_command(command: &VirtualCompactionCommand) -> VirtualResult<()> {
    if command.control_version != VIRTUAL_COMPACTION_CONTROL_VERSION
        || command.command_id.is_empty()
        || command.region_id.is_empty()
        || command.source_causal_cut.is_empty()
        || command.source_causal_cut.iter().any(String::is_empty)
        || command.compactor_binding.is_empty()
        || command.compactor_revision.is_empty()
    {
        return Err(VirtualError::Validation(
            "compaction command version, identities, causal cut, binding, and revision are required"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_rehydration_command(command: &VirtualRehydrationCommand) -> VirtualResult<()> {
    if command.control_version != VIRTUAL_REHYDRATION_CONTROL_VERSION
        || command.command_id.is_empty()
        || command.certificate_id.is_empty()
        || command.occurrence_ids.is_empty()
        || command.occurrence_ids.iter().any(String::is_empty)
    {
        return Err(VirtualError::Validation(
            "rehydration command version, identities, and occurrence selection are required"
                .to_owned(),
        ));
    }
    Ok(())
}

fn archived_work_index(
    occurrences: &BTreeMap<String, WorkOccurrence>,
) -> VirtualResult<BTreeMap<String, ArchivedWorkIndex>> {
    let mut latest = BTreeMap::<String, &WorkOccurrence>::new();
    for occurrence in occurrences.values() {
        validate_occurrence(&occurrence.occurrence_id, occurrence)?;
        latest
            .entry(occurrence.work_id.clone())
            .and_modify(|current| {
                if occurrence.epoch > current.epoch {
                    *current = occurrence;
                }
            })
            .or_insert(occurrence);
    }
    let mut result = BTreeMap::new();
    for (work_id, occurrence) in latest {
        if !is_terminal_work_state(occurrence.state) {
            return Err(VirtualError::Conflict(format!(
                "work {work_id} has no terminal greatest occurrence"
            )));
        }
        result.insert(
            work_id.clone(),
            ArchivedWorkIndex {
                work_id,
                region_id: occurrence.region_id.clone(),
                run_id: occurrence.run_id.clone(),
                max_epoch: occurrence.epoch,
                terminal_state: occurrence.state,
            },
        );
    }
    Ok(result)
}

fn completion_summary(
    manifest: &VirtualArchiveManifest,
) -> VirtualResult<VirtualCompletionSummary> {
    let mut outputs = BTreeSet::new();
    let mut evidence = BTreeSet::new();
    for occurrence in manifest.occurrences.values() {
        if let Some(result) = &occurrence.result {
            outputs.insert(result.clone());
        }
        if let Some(error) = &occurrence.error {
            evidence.insert(error.clone());
        }
    }
    let succeeded_count = manifest
        .work_index
        .values()
        .filter(|index| index.terminal_state == WorkOccurrenceState::Succeeded)
        .count();
    let failed_count = manifest
        .work_index
        .values()
        .filter(|index| index.terminal_state == WorkOccurrenceState::Failed)
        .count();
    let cancelled_count = manifest
        .work_index
        .values()
        .filter(|index| index.terminal_state == WorkOccurrenceState::Cancelled)
        .count();
    Ok(VirtualCompletionSummary {
        region_id: manifest.region_id.clone(),
        run_id: manifest.run_id.clone(),
        occurrence_count: u64::try_from(manifest.occurrences.len()).map_err(|_| {
            VirtualError::Validation("archive occurrence count exceeds u64".to_owned())
        })?,
        work_count: u64::try_from(manifest.work_index.len())
            .map_err(|_| VirtualError::Validation("archive work count exceeds u64".to_owned()))?,
        succeeded_count: u64::try_from(succeeded_count)
            .map_err(|_| VirtualError::Validation("success count exceeds u64".to_owned()))?,
        failed_count: u64::try_from(failed_count)
            .map_err(|_| VirtualError::Validation("failure count exceeds u64".to_owned()))?,
        cancelled_count: u64::try_from(cancelled_count)
            .map_err(|_| VirtualError::Validation("cancellation count exceeds u64".to_owned()))?,
        output_digest: canonical_digest(&outputs)
            .map_err(|error| VirtualError::Validation(error.to_string()))?,
        evidence_digest: canonical_digest(&evidence)
            .map_err(|error| VirtualError::Validation(error.to_string()))?,
        retained_debug_index_digest: canonical_digest(&manifest.work_index)
            .map_err(|error| VirtualError::Validation(error.to_string()))?,
    })
}

fn virtual_certificate_id(certificate: &VirtualCompactionCertificate) -> VirtualResult<String> {
    let mut identity = certificate.clone();
    identity.certificate_id.clear();
    content_id(VIRTUAL_COMPACTION_CERTIFICATE_VERSION, &identity)
        .map_err(|error| VirtualError::Validation(error.to_string()))
}

fn validate_virtual_certificate(certificate: &VirtualCompactionCertificate) -> VirtualResult<()> {
    if certificate.certificate_version != VIRTUAL_COMPACTION_CERTIFICATE_VERSION
        || certificate.certificate_id != virtual_certificate_id(certificate)?
        || certificate.source_causal_cut.is_empty()
        || certificate.source_causal_cut.iter().any(String::is_empty)
        || certificate.summary.region_id.is_empty()
        || certificate.summary.run_id.is_empty()
        || certificate.summary_state_digest.is_empty()
        || certificate.compactor_binding.is_empty()
        || certificate.compactor_revision.is_empty()
        || certificate.rehydration_manifest.artifact_id.is_empty()
        || certificate.rehydration_manifest.kind != crate::VIRTUAL_ARCHIVE_MANIFEST_KIND
        || !certificate.unresolved_obligations.is_empty()
        || certificate
            .retained_occurrence_bindings
            .iter()
            .any(String::is_empty)
        || certificate.replay_availability != ReplayAvailability::Exact
        || certificate.summary.work_count
            != certificate
                .summary
                .succeeded_count
                .saturating_add(certificate.summary.failed_count)
                .saturating_add(certificate.summary.cancelled_count)
    {
        return Err(VirtualError::Validation(format!(
            "compaction certificate {} is malformed",
            certificate.certificate_id
        )));
    }
    Ok(())
}

fn validate_manifest_certificate(
    manifest: &VirtualArchiveManifest,
    certificate: &VirtualCompactionCertificate,
) -> VirtualResult<()> {
    validate_virtual_certificate(certificate)?;
    if manifest.manifest_version != VIRTUAL_ARCHIVE_MANIFEST_VERSION
        || manifest.region_id != certificate.summary.region_id
        || manifest.run_id != certificate.summary.run_id
        || manifest.source_causal_cut != certificate.source_causal_cut
        || completion_summary(manifest)? != certificate.summary
        || canonical_digest(manifest)
            .map_err(|error| VirtualError::Validation(error.to_string()))?
            != certificate.summary_state_digest
        || virtual_archive_record(manifest)?.reference != certificate.rehydration_manifest
        || archived_work_index(&manifest.occurrences)? != manifest.work_index
        || manifest.occurrences.values().any(|occurrence| {
            occurrence.region_id != manifest.region_id || occurrence.run_id != manifest.run_id
        })
        || manifest
            .occurrences
            .values()
            .map(|occurrence| occurrence.occurrence_binding.clone())
            .collect::<BTreeSet<_>>()
            != certificate.retained_occurrence_bindings
    {
        return Err(VirtualError::Validation(format!(
            "archive manifest does not match compaction certificate {}",
            certificate.certificate_id
        )));
    }
    Ok(())
}

fn is_terminal_work_state(state: WorkOccurrenceState) -> bool {
    matches!(
        state,
        WorkOccurrenceState::Succeeded
            | WorkOccurrenceState::Failed
            | WorkOccurrenceState::Cancelled
    )
}

fn validate_migration_request(request: &RegionMigrationRequest) -> VirtualResult<()> {
    if request.migration_id.is_empty()
        || request.migration_binding.is_empty()
        || request.source_region_ids.is_empty()
        || request.source_region_ids.iter().any(String::is_empty)
    {
        return Err(VirtualError::Validation(
            "migration request identities and sources must not be empty".to_owned(),
        ));
    }
    validate_migration_cardinality(
        request.kind,
        request.source_region_ids.len(),
        request.target_count,
    )
}

fn validate_migration_plan_shape(plan: &RegionMigrationPlan) -> VirtualResult<()> {
    if plan.migration_version != VIRTUAL_REGION_MIGRATION_VERSION
        || plan.migration_id.is_empty()
        || plan.migration_binding.is_empty()
        || plan.expected_sources.is_empty()
        || plan.expected_sources.keys().any(String::is_empty)
    {
        return Err(VirtualError::Validation(
            "region migration version, identity, binding, and sources are required".to_owned(),
        ));
    }
    validate_artifact(&plan.coverage_evidence)?;
    validate_migration_cardinality(plan.kind, plan.expected_sources.len(), plan.targets.len())
}

fn validate_migration_cardinality(
    kind: RegionMigrationKind,
    source_count: usize,
    target_count: usize,
) -> VirtualResult<()> {
    let valid = match kind {
        RegionMigrationKind::Split => source_count == 1 && target_count >= 2,
        RegionMigrationKind::Merge => source_count >= 2 && target_count == 1,
    };
    if !valid {
        return Err(VirtualError::Validation(
            "split requires one source and multiple targets; merge requires multiple sources and one target"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_region(region: &VirtualRegion) -> VirtualResult<()> {
    if region.region_id.is_empty()
        || region.run_id.is_empty()
        || region.source.is_empty()
        || region.cursor.version.is_empty()
    {
        return Err(VirtualError::Validation(
            "virtual region identities and cursor version must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_work_item(item: &WorkItem, region: &VirtualRegion) -> VirtualResult<()> {
    if item.region_id != region.region_id || item.run_id != region.run_id {
        return Err(VirtualError::Validation(
            "materialized work escaped its region or Run".to_owned(),
        ));
    }
    if item.work_id.is_empty()
        || item.payload.artifact_id.is_empty()
        || item.payload.kind.is_empty()
        || item.cost == 0
        || item.capability.as_ref().is_some_and(String::is_empty)
    {
        return Err(VirtualError::Validation(format!(
            "work {} has an empty identity, capability, Artifact, or zero cost",
            item.work_id
        )));
    }
    Ok(())
}

fn validate_occurrence(occurrence_id: &str, occurrence: &WorkOccurrence) -> VirtualResult<()> {
    if occurrence.occurrence_version != VIRTUAL_WORK_OCCURRENCE_VERSION
        || occurrence.occurrence_id != occurrence_id
        || occurrence.occurrence_id != work_occurrence_id(&occurrence.work_id, occurrence.epoch)?
        || occurrence.work_id.is_empty()
        || occurrence.region_id.is_empty()
        || occurrence.run_id.is_empty()
        || occurrence.owner.is_empty()
        || occurrence.epoch == 0
        || occurrence.occurrence_binding.is_empty()
    {
        return Err(VirtualError::Validation(format!(
            "work occurrence {occurrence_id} has invalid identity or binding"
        )));
    }
    let shape_is_valid = match occurrence.state {
        WorkOccurrenceState::Running => {
            occurrence.result.is_none()
                && occurrence.error.is_none()
                && occurrence.next_reason.is_none()
        }
        WorkOccurrenceState::Succeeded => {
            occurrence
                .result
                .as_ref()
                .is_some_and(|result| validate_artifact(result).is_ok())
                && occurrence.error.is_none()
                && occurrence.next_reason.is_none()
        }
        WorkOccurrenceState::RetryScheduled => {
            occurrence.result.is_none()
                && occurrence
                    .error
                    .as_ref()
                    .is_some_and(|error| validate_artifact(error).is_ok())
                && occurrence
                    .next_reason
                    .as_ref()
                    .is_none_or(|reason| validate_park_reason(reason).is_ok())
        }
        WorkOccurrenceState::Parked => {
            occurrence.result.is_none()
                && occurrence.error.is_none()
                && occurrence
                    .next_reason
                    .as_ref()
                    .is_some_and(|reason| validate_park_reason(reason).is_ok())
        }
        WorkOccurrenceState::Failed | WorkOccurrenceState::Cancelled => {
            occurrence.result.is_none()
                && occurrence
                    .error
                    .as_ref()
                    .is_some_and(|error| validate_artifact(error).is_ok())
                && occurrence.next_reason.is_none()
        }
    };
    if !shape_is_valid {
        return Err(VirtualError::Validation(format!(
            "work occurrence {occurrence_id} has fields inconsistent with its state"
        )));
    }
    Ok(())
}

fn validate_resolution(resolution: &WorkResolution) -> VirtualResult<()> {
    match resolution {
        WorkResolution::Succeeded { result } => validate_artifact(result),
        WorkResolution::Retry { error, next_reason } => {
            validate_artifact(error)?;
            if let Some(reason) = next_reason {
                validate_park_reason(reason)?;
            }
            Ok(())
        }
        WorkResolution::Parked { reason } => validate_park_reason(reason),
        WorkResolution::Failed { error } => validate_artifact(error),
        WorkResolution::Cancelled { reason } => validate_artifact(reason),
    }
}

fn validate_artifact(artifact: &cymule_core::ArtifactRef) -> VirtualResult<()> {
    if artifact.artifact_id.is_empty() || artifact.kind.is_empty() {
        return Err(VirtualError::Validation(
            "work disposition Artifact identity and kind must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_park_reason(reason: &ParkReason) -> VirtualResult<()> {
    let identity = match reason {
        ParkReason::Wait { key } => key,
        ParkReason::Dependency { work_id } => work_id,
        ParkReason::Budget { account } => account,
        ParkReason::Capability { capability } => capability,
        ParkReason::Backpressure { domain } => domain,
    };
    if identity.is_empty() {
        return Err(VirtualError::Validation(
            "park reason identity must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn occurrence_matches_resolution(occurrence: &WorkOccurrence, resolution: &WorkResolution) -> bool {
    match resolution {
        WorkResolution::Succeeded { result } => {
            occurrence.state == WorkOccurrenceState::Succeeded
                && occurrence.result.as_ref() == Some(result)
                && occurrence.error.is_none()
                && occurrence.next_reason.is_none()
        }
        WorkResolution::Retry { error, next_reason } => {
            occurrence.state == WorkOccurrenceState::RetryScheduled
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(error)
                && occurrence.next_reason == *next_reason
        }
        WorkResolution::Parked { reason } => {
            occurrence.state == WorkOccurrenceState::Parked
                && occurrence.result.is_none()
                && occurrence.error.is_none()
                && occurrence.next_reason.as_ref() == Some(reason)
        }
        WorkResolution::Failed { error } => {
            occurrence.state == WorkOccurrenceState::Failed
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(error)
                && occurrence.next_reason.is_none()
        }
        WorkResolution::Cancelled { reason } => {
            occurrence.state == WorkOccurrenceState::Cancelled
                && occurrence.result.is_none()
                && occurrence.error.as_ref() == Some(reason)
                && occurrence.next_reason.is_none()
        }
    }
}

fn work_occurrence_id(work_id: &str, epoch: u64) -> VirtualResult<String> {
    content_id(VIRTUAL_WORK_OCCURRENCE_VERSION, &(work_id, epoch))
        .map_err(|error| VirtualError::Validation(error.to_string()))
}

fn insert_parked(
    snapshot: &mut VirtualSnapshot,
    item: WorkItem,
    reason: ParkReason,
) -> VirtualResult<()> {
    let work_id = item.work_id.clone();
    if snapshot
        .parked
        .insert(
            work_id.clone(),
            ParkedWork {
                item,
                reason: reason.clone(),
            },
        )
        .is_some()
    {
        return Err(VirtualError::Conflict(format!(
            "work {work_id} is already parked"
        )));
    }
    snapshot
        .parked_index
        .entry(reason)
        .or_default()
        .insert(work_id);
    Ok(())
}

fn build_parked_index(
    parked: &BTreeMap<String, ParkedWork>,
) -> BTreeMap<ParkReason, BTreeSet<String>> {
    let mut index = BTreeMap::<ParkReason, BTreeSet<String>>::new();
    for (work_id, parked) in parked {
        index
            .entry(parked.reason.clone())
            .or_default()
            .insert(work_id.clone());
    }
    index
}

fn insert_priority(queue: &mut VecDeque<WorkItem>, item: WorkItem) {
    let index = queue
        .iter()
        .position(|current| current.priority < item.priority)
        .unwrap_or(queue.len());
    queue.insert(index, item);
}

fn insert_ready(snapshot: &mut VirtualSnapshot, item: WorkItem) {
    snapshot
        .ready_since
        .insert(item.work_id.clone(), snapshot.dispatch_sequence);
    insert_priority(snapshot.ready.entry(item.run_id.clone()).or_default(), item);
}

fn rotate_after(items: &mut [String], last: Option<&str>) {
    let Some(last) = last else {
        return;
    };
    let index = items
        .iter()
        .position(|item| item.as_str() > last)
        .unwrap_or(0);
    items.rotate_left(index);
}
