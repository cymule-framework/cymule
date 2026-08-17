use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cymule_durable::WaitActivation;

use crate::{
    ClaimedWork, FrontierLimits, MaterializedPage, ParkReason, ParkedWork, VirtualError,
    VirtualRegion, VirtualResult, VirtualSnapshot, WorkItem,
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

/// Deterministic bounded scheduler for virtual work.
#[derive(Clone)]
pub struct VirtualScheduler {
    limits: FrontierLimits,
    snapshot: VirtualSnapshot,
}

impl VirtualScheduler {
    /// Create an empty scheduler with explicit bounds.
    pub fn new(limits: FrontierLimits) -> VirtualResult<Self> {
        if limits.max_materialized == 0
            || limits.max_active == 0
            || limits.max_active_per_run == 0
            || limits.materialize_batch == 0
        {
            return Err(VirtualError::Validation(
                "all frontier limits must be positive".to_owned(),
            ));
        }
        Ok(Self {
            limits,
            snapshot: VirtualSnapshot {
                regions: BTreeMap::new(),
                ready: BTreeMap::new(),
                active: BTreeMap::new(),
                parked: BTreeMap::new(),
                parked_index: BTreeMap::new(),
                known: BTreeSet::new(),
                last_run: None,
                claim_epochs: BTreeMap::new(),
            },
        })
    }

    /// Restore scheduler state under the same explicit limits.
    pub fn restore(limits: FrontierLimits, mut snapshot: VirtualSnapshot) -> VirtualResult<Self> {
        snapshot.parked_index = build_parked_index(&snapshot.parked);
        let scheduler = Self { limits, snapshot };
        scheduler.validate_bounds()?;
        Ok(scheduler)
    }

    /// Register an idempotent virtual region.
    pub fn register(&mut self, region: VirtualRegion) -> VirtualResult<()> {
        validate_region(&region)?;
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
        }
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
        let region_ids: Vec<String> = self.snapshot.regions.keys().cloned().collect();
        for region_id in region_ids {
            let available = self
                .limits
                .max_materialized
                .saturating_sub(self.materialized_count());
            if available == 0 {
                break;
            }
            let region = self.snapshot.regions[&region_id].clone();
            if region.cursor.exhausted {
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
                insert_priority(
                    self.snapshot.ready.entry(item.run_id.clone()).or_default(),
                    item,
                );
                added += 1;
            }
            self.snapshot
                .regions
                .get_mut(&region_id)
                .expect("region exists")
                .cursor = page.next_cursor;
        }
        self.validate_bounds()?;
        Ok(added)
    }

    /// Claim one fair, capability-compatible item under a fencing epoch.
    pub fn claim(
        &mut self,
        owner: &str,
        capabilities: &BTreeSet<String>,
    ) -> VirtualResult<Option<ClaimedWork>> {
        if self.snapshot.active.len() >= self.limits.max_active {
            return Ok(None);
        }
        let runs = self.eligible_runs();
        for run_id in runs {
            let active_for_run = self
                .snapshot
                .active
                .values()
                .filter(|claim| claim.item.run_id == run_id)
                .count();
            if active_for_run >= self.limits.max_active_per_run {
                continue;
            }
            let queue = self
                .snapshot
                .ready
                .get_mut(&run_id)
                .expect("eligible queue exists");
            let Some(index) = queue.iter().position(|item| {
                item.capability
                    .as_ref()
                    .is_none_or(|capability| capabilities.contains(capability))
            }) else {
                continue;
            };
            let item = queue.remove(index).expect("selected item exists");
            let epoch = self
                .snapshot
                .claim_epochs
                .entry(item.work_id.clone())
                .and_modify(|epoch| *epoch += 1)
                .or_insert(1);
            let claim = ClaimedWork {
                item,
                owner: owner.to_owned(),
                epoch: *epoch,
            };
            self.snapshot
                .active
                .insert(claim.item.work_id.clone(), claim.clone());
            self.snapshot.last_run = Some(run_id);
            return Ok(Some(claim));
        }
        Ok(None)
    }

    /// Complete exactly the current fenced claim.
    pub fn complete(&mut self, work_id: &str, owner: &str, epoch: u64) -> VirtualResult<WorkItem> {
        let claim =
            self.snapshot.active.get(work_id).ok_or_else(|| {
                VirtualError::NotFound(format!("active work {work_id} is missing"))
            })?;
        if claim.owner != owner || claim.epoch != epoch {
            return Err(VirtualError::Conflict(format!(
                "stale completion for {work_id}"
            )));
        }
        Ok(self
            .snapshot
            .active
            .remove(work_id)
            .expect("claim exists")
            .item)
    }

    /// Park a currently active claim under an indexed reason.
    pub fn park(
        &mut self,
        work_id: &str,
        owner: &str,
        epoch: u64,
        reason: ParkReason,
    ) -> VirtualResult<()> {
        let item = self.complete(work_id, owner, epoch)?;
        self.snapshot.parked.insert(
            work_id.to_owned(),
            ParkedWork {
                item,
                reason: reason.clone(),
            },
        );
        self.snapshot
            .parked_index
            .entry(reason)
            .or_default()
            .insert(work_id.to_owned());
        Ok(())
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
            insert_priority(
                self.snapshot
                    .ready
                    .entry(parked.item.run_id.clone())
                    .or_default(),
                parked.item,
            );
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

    fn eligible_runs(&self) -> Vec<String> {
        let mut runs: Vec<String> = self
            .snapshot
            .ready
            .iter()
            .filter(|(_, queue)| !queue.is_empty())
            .map(|(run, _)| run.clone())
            .collect();
        if let Some(last) = &self.snapshot.last_run
            && let Some(index) = runs.iter().position(|run| run > last)
        {
            runs.rotate_left(index);
        }
        runs
    }

    fn validate_bounds(&self) -> VirtualResult<()> {
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
            }
        }
        for (work_id, claim) in &self.snapshot.active {
            if &claim.item.work_id != work_id || claim.owner.is_empty() || claim.epoch == 0 {
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
