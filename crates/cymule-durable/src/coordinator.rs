use cymule_core::Machine;

use crate::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableError,
    DurableResult, DurableState, DurableStore, EffectDispatch, JournalRecord, OutboxState,
    SnapshotRecord, StoredState, WaitCondition, WaitState,
};

/// Transactional coordinator over one provider-neutral durable store.
pub struct DurableCoordinator<S> {
    store: S,
    stored: Option<StoredState>,
}

impl<S: DurableStore> DurableCoordinator<S> {
    /// Open a coordinator and verify the latest durable revision.
    pub fn open(mut store: S) -> DurableResult<Self> {
        let stored = store.load()?;
        if let Some(stored) = &stored {
            stored.verify()?;
        }
        Ok(Self { store, stored })
    }

    /// Initialize an empty durable state from a semantic Machine.
    pub fn initialize(mut self, machine: &Machine) -> DurableResult<Self> {
        self.initialize_in_place(machine)?;
        Ok(self)
    }

    /// Initialize an empty coordinator without consuming it.
    pub fn initialize_in_place(&mut self, machine: &Machine) -> DurableResult<String> {
        if self.stored.is_some() {
            return Err(DurableError::IllegalTransition(
                "durable store is already initialized".to_owned(),
            ));
        }
        let state = DurableState::new(machine.snapshot());
        let commit = self.store.compare_and_swap(None, &state)?;
        self.stored = Some(StoredState {
            revision: commit.revision.clone(),
            state,
        });
        Ok(commit.revision)
    }

    /// Current verified state.
    pub fn state(&self) -> DurableResult<&DurableState> {
        self.stored
            .as_ref()
            .map(|stored| &stored.state)
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))
    }

    /// Current revision.
    pub fn revision(&self) -> Option<&str> {
        self.stored.as_ref().map(|stored| stored.revision.as_str())
    }

    /// Restore the current semantic Machine from canonical durable inputs.
    pub fn restore_machine(&self) -> DurableResult<Machine> {
        Machine::restore(self.state()?.machine.clone()).map_err(Into::into)
    }

    /// Persist the current semantic Machine snapshot.
    pub fn persist_machine(&mut self, machine: &Machine) -> DurableResult<String> {
        self.mutate(|state| state.machine = machine.snapshot())
    }

    /// Insert or replace a continuation at a semantic safe point.
    pub fn put_continuation(&mut self, continuation: Continuation) -> DurableResult<String> {
        self.mutate(|state| {
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
        })
    }

    /// Atomically persist a Machine safe point, Continuation, and optional
    /// component occurrence.
    pub fn checkpoint(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        occurrence: Option<ComponentOccurrence>,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            if let Some(occurrence) = occurrence {
                match state.component_occurrences.get(&occurrence.occurrence_id) {
                    Some(existing) if existing == &occurrence => {}
                    Some(_) => {
                        return Err(DurableError::IllegalTransition(format!(
                            "component occurrence {} has conflicting content",
                            occurrence.occurrence_id
                        )));
                    }
                    None => {
                        state
                            .component_occurrences
                            .insert(occurrence.occurrence_id.clone(), occurrence);
                    }
                }
            }
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
            Ok(())
        })
    }

    /// Atomically persist an admitted/prepared Effect and its outbox entry.
    pub fn checkpoint_effect_enqueue(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        dispatch: EffectDispatch,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            match state.outbox.get(&dispatch.intent_id) {
                Some(existing) if existing == &dispatch => {}
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "effect {} already has a different outbox entry",
                        dispatch.intent_id
                    )));
                }
                None => {
                    state.outbox.insert(dispatch.intent_id.clone(), dispatch);
                }
            }
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
            Ok(())
        })
    }

    /// Atomically persist `DispatchStarted` and the fenced outbox claim.
    pub fn checkpoint_effect_claim(
        &mut self,
        machine: &Machine,
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state != OutboxState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "effect {intent_id} is not pending"
                )));
            }
            dispatch.state = OutboxState::Claimed;
            dispatch.claim_owner = Some(owner.to_owned());
            dispatch.claim_epoch = lease_epoch;
            state.machine = machine.snapshot();
            Ok(())
        })
    }

    /// Atomically persist an effect observation/reconciliation and outbox
    /// settlement under the exact original claim.
    pub fn checkpoint_effect_settlement(
        &mut self,
        machine: &Machine,
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
        outcome: OutboxState,
        result: Option<cymule_core::ArtifactRef>,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            if !matches!(
                outcome,
                OutboxState::Applied | OutboxState::NotApplied | OutboxState::Unknown
            ) {
                return Err(DurableError::Validation(
                    "settlement must be applied, not_applied, or unknown".to_owned(),
                ));
            }
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state != OutboxState::Claimed
                || dispatch.claim_owner.as_deref() != Some(owner)
                || dispatch.claim_epoch != lease_epoch
            {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{owner}:{lease_epoch}")),
                    current: dispatch
                        .claim_owner
                        .as_ref()
                        .map(|current| format!("{current}:{}", dispatch.claim_epoch)),
                });
            }
            dispatch.state = outcome;
            dispatch.result = result;
            state.machine = machine.snapshot();
            Ok(())
        })
    }

    /// Atomically persist the safe point and register its durable wait.
    pub fn park(
        &mut self,
        machine: &Machine,
        mut continuation: Continuation,
        wait: WaitCondition,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            match state.waits.get(&wait.wait_id) {
                Some(existing) if existing == &wait => {}
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait {} already exists with different semantics",
                        wait.wait_id
                    )));
                }
                None => {
                    state.waits.insert(wait.wait_id.clone(), wait.clone());
                }
            }
            continuation.wait_set.insert(wait.wait_id);
            continuation.status = ContinuationStatus::Waiting;
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
            Ok(())
        })
    }

    /// Store a wait result artifact and ready its Continuation atomically.
    pub fn complete_wait_with_machine(
        &mut self,
        machine: &Machine,
        wait_id: &str,
        result: cymule_core::ArtifactRef,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            let wait = state
                .waits
                .get_mut(wait_id)
                .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?;
            if wait.state == WaitState::Completed && wait.result.as_ref() == Some(&result) {
                return Ok(());
            }
            if wait.state != WaitState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "wait {wait_id} is not pending"
                )));
            }
            wait.state = WaitState::Completed;
            wait.result = Some(result);
            let continuation = state.continuations.get_mut(&wait.run_id).ok_or_else(|| {
                DurableError::NotFound(format!("continuation {} does not exist", wait.run_id))
            })?;
            continuation.wait_set.remove(wait_id);
            if continuation.wait_set.is_empty() {
                continuation.status = ContinuationStatus::Ready;
            }
            Ok(())
        })
    }

    /// Register an idempotent durable wait.
    pub fn register_wait(&mut self, wait: WaitCondition) -> DurableResult<String> {
        self.mutate_checked(|state| match state.waits.get(&wait.wait_id) {
            Some(existing) if existing == &wait => Ok(()),
            Some(_) => Err(DurableError::IllegalTransition(format!(
                "wait {} already exists with different semantics",
                wait.wait_id
            ))),
            None => {
                let continuation = state.continuations.get_mut(&wait.run_id).ok_or_else(|| {
                    DurableError::NotFound(format!("continuation {} does not exist", wait.run_id))
                })?;
                continuation.wait_set.insert(wait.wait_id.clone());
                continuation.status = ContinuationStatus::Waiting;
                state.waits.insert(wait.wait_id.clone(), wait);
                Ok(())
            }
        })
    }

    /// Complete one wait and make the continuation ready when no waits remain.
    pub fn complete_wait(
        &mut self,
        wait_id: &str,
        result: cymule_core::ArtifactRef,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            let wait = state
                .waits
                .get_mut(wait_id)
                .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?;
            if wait.state == WaitState::Completed && wait.result.as_ref() == Some(&result) {
                return Ok(());
            }
            if wait.state != WaitState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "wait {wait_id} is not pending"
                )));
            }
            wait.state = WaitState::Completed;
            wait.result = Some(result);
            let continuation = state.continuations.get_mut(&wait.run_id).ok_or_else(|| {
                DurableError::NotFound(format!("continuation {} does not exist", wait.run_id))
            })?;
            continuation.wait_set.remove(wait_id);
            if continuation.wait_set.is_empty() {
                continuation.status = ContinuationStatus::Ready;
            }
            Ok(())
        })
    }

    /// Acquire or renew one fenced lease using caller-supplied logical time.
    pub fn acquire_lease(
        &mut self,
        resource: &str,
        owner: &str,
        now: u64,
        ttl: u64,
    ) -> DurableResult<AuthorityLease> {
        let mut acquired = None;
        self.mutate_checked(|state| {
            if ttl == 0 {
                return Err(DurableError::Validation(
                    "lease TTL must be positive".to_owned(),
                ));
            }
            if let Some(current) = state.leases.get(resource)
                && current.owner != owner
                && current.expires_at > now
            {
                return Err(DurableError::Conflict {
                    expected: Some(owner.to_owned()),
                    current: Some(current.owner.clone()),
                });
            }
            let epoch = state
                .leases
                .get(resource)
                .map_or(1, |lease| lease.epoch + 1);
            let lease = AuthorityLease {
                resource: resource.to_owned(),
                owner: owner.to_owned(),
                epoch,
                expires_at: now.saturating_add(ttl),
            };
            state.leases.insert(resource.to_owned(), lease.clone());
            acquired = Some(lease);
            Ok(())
        })?;
        acquired.ok_or_else(|| DurableError::Encoding("lease result disappeared".to_owned()))
    }

    /// Enqueue an effect dispatch exactly once by structural intent ID.
    pub fn enqueue_effect(&mut self, dispatch: EffectDispatch) -> DurableResult<String> {
        self.mutate_checked(|state| match state.outbox.get(&dispatch.intent_id) {
            Some(existing) if existing == &dispatch => Ok(()),
            Some(_) => Err(DurableError::IllegalTransition(format!(
                "effect {} already has a different outbox entry",
                dispatch.intent_id
            ))),
            None => {
                state.outbox.insert(dispatch.intent_id.clone(), dispatch);
                Ok(())
            }
        })
    }

    /// Claim one pending dispatch under a fenced lease epoch.
    pub fn claim_effect(
        &mut self,
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state != OutboxState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "effect {intent_id} is not pending"
                )));
            }
            dispatch.state = OutboxState::Claimed;
            dispatch.claim_owner = Some(owner.to_owned());
            dispatch.claim_epoch = lease_epoch;
            Ok(())
        })
    }

    /// Record an authoritative dispatch observation under the original claim.
    pub fn settle_effect(
        &mut self,
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
        outcome: OutboxState,
        result: Option<cymule_core::ArtifactRef>,
    ) -> DurableResult<String> {
        self.mutate_checked(|state| {
            if !matches!(
                outcome,
                OutboxState::Applied | OutboxState::NotApplied | OutboxState::Unknown
            ) {
                return Err(DurableError::Validation(
                    "settlement must be applied, not_applied, or unknown".to_owned(),
                ));
            }
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state == outcome && dispatch.result == result {
                return Ok(());
            }
            if dispatch.state != OutboxState::Claimed
                || dispatch.claim_owner.as_deref() != Some(owner)
                || dispatch.claim_epoch != lease_epoch
            {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{owner}:{lease_epoch}")),
                    current: dispatch
                        .claim_owner
                        .as_ref()
                        .map(|current| format!("{current}:{}", dispatch.claim_epoch)),
                });
            }
            dispatch.state = outcome;
            dispatch.result = result;
            Ok(())
        })
    }

    /// Record one component result exactly once for execution replay.
    pub fn record_component(&mut self, occurrence: ComponentOccurrence) -> DurableResult<String> {
        self.mutate_checked(|state| {
            match state.component_occurrences.get(&occurrence.occurrence_id) {
                Some(existing) if existing == &occurrence => Ok(()),
                Some(_) => Err(DurableError::IllegalTransition(format!(
                    "component occurrence {} has conflicting content",
                    occurrence.occurrence_id
                ))),
                None => {
                    state
                        .component_occurrences
                        .insert(occurrence.occurrence_id.clone(), occurrence);
                    Ok(())
                }
            }
        })
    }

    /// Publish portable snapshot metadata.
    pub fn publish_snapshot(&mut self, snapshot: SnapshotRecord) -> DurableResult<String> {
        self.mutate_checked(|state| match state.snapshots.get(&snapshot.snapshot_id) {
            Some(existing) if existing == &snapshot => Ok(()),
            Some(_) => Err(DurableError::IllegalTransition(format!(
                "snapshot {} has conflicting content",
                snapshot.snapshot_id
            ))),
            None => {
                state
                    .snapshots
                    .insert(snapshot.snapshot_id.clone(), snapshot);
                Ok(())
            }
        })
    }

    /// Read one higher-profile journal in durable append order.
    pub fn journal_records(&self, journal_id: &str) -> DurableResult<&[JournalRecord]> {
        Ok(self
            .state()?
            .application_journals
            .get(journal_id)
            .map(Vec::as_slice)
            .unwrap_or_default())
    }

    /// Append one self-validating higher-profile record idempotently.
    pub fn append_journal_record(
        &mut self,
        journal_id: &str,
        record: JournalRecord,
    ) -> DurableResult<String> {
        if journal_id.is_empty() {
            return Err(DurableError::Validation(
                "application journal identity must not be empty".to_owned(),
            ));
        }
        record.verify()?;
        self.mutate_checked(|state| {
            let records = state
                .application_journals
                .entry(journal_id.to_owned())
                .or_default();
            match records
                .iter()
                .find(|existing| existing.record_id == record.record_id)
            {
                Some(existing) if existing == &record => Ok(()),
                Some(_) => Err(DurableError::IllegalTransition(format!(
                    "journal record {} already has different content",
                    record.record_id
                ))),
                None => {
                    records.push(record);
                    Ok(())
                }
            }
        })
    }

    /// Consume the store after coordination.
    pub fn into_store(self) -> S {
        self.store
    }

    fn mutate(&mut self, update: impl FnOnce(&mut DurableState)) -> DurableResult<String> {
        self.mutate_checked(|state| {
            update(state);
            Ok(())
        })
    }

    fn mutate_checked(
        &mut self,
        update: impl FnOnce(&mut DurableState) -> DurableResult<()>,
    ) -> DurableResult<String> {
        let stored = self
            .stored
            .as_ref()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?;
        let expected = stored.revision.clone();
        let mut next = stored.state.clone();
        update(&mut next)?;
        let commit = self.store.compare_and_swap(Some(&expected), &next)?;
        let revision = commit.revision;
        self.stored = Some(StoredState {
            revision: revision.clone(),
            state: next,
        });
        Ok(revision)
    }
}
