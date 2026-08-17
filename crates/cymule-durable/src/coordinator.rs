use cymule_core::Machine;

use crate::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableError,
    DurableResult, DurableState, DurableStore, EffectDispatch, JournalBatch, JournalRecord,
    OutboxState, SnapshotRecord, StoredState, WaitActivation, WaitCondition, WaitKind, WaitState,
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

    /// Atomically persist a Machine safe point, Continuation, higher-profile
    /// records, and one newly enqueued Effect.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid records or dispatch state, conflicting
    /// identity, stale CAS revision, or store failure.
    pub fn checkpoint_journal_effect_enqueue(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        journal_id: &str,
        records: &[JournalRecord],
        dispatch: EffectDispatch,
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        if dispatch.state != OutboxState::Pending
            || dispatch.claim_owner.is_some()
            || dispatch.claim_epoch != 0
            || dispatch.result.is_some()
        {
            return Err(DurableError::Validation(
                "new effect dispatch must be unclaimed and pending".to_owned(),
            ));
        }
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
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

    /// Atomically persist `DispatchStarted`, a fenced outbox claim, and the
    /// owning higher-profile lifecycle records.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid claim, missing or non-pending Effect,
    /// invalid records, stale CAS revision, or store failure.
    #[allow(clippy::too_many_arguments)]
    pub fn checkpoint_journal_effect_claim(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        journal_id: &str,
        records: &[JournalRecord],
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        if owner.is_empty() {
            return Err(DurableError::Validation(
                "effect claim owner must not be empty".to_owned(),
            ));
        }
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
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
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
            let already_settled = dispatch.state == outcome && dispatch.result == result;
            if !already_settled
                && !matches!(dispatch.state, OutboxState::Claimed | OutboxState::Unknown)
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

    /// Atomically persist an Effect observation, outbox settlement,
    /// Continuation obligation projection, and higher-profile lifecycle
    /// records under the original claim.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid outcome, missing or mismatched claim,
    /// invalid records, stale CAS revision, or store failure.
    #[allow(clippy::too_many_arguments)]
    pub fn checkpoint_journal_effect_settlement(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        journal_id: &str,
        records: &[JournalRecord],
        intent_id: &str,
        owner: &str,
        lease_epoch: u64,
        outcome: OutboxState,
        result: Option<cymule_core::ArtifactRef>,
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        if !matches!(
            outcome,
            OutboxState::Applied | OutboxState::NotApplied | OutboxState::Unknown
        ) {
            return Err(DurableError::Validation(
                "settlement must be applied, not_applied, or unknown".to_owned(),
            ));
        }
        self.mutate_checked(|state| {
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            let already_settled = dispatch.state == outcome && dispatch.result == result;
            if !already_settled
                && !matches!(dispatch.state, OutboxState::Claimed | OutboxState::Unknown)
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
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
            Ok(())
        })
    }

    /// Atomically persist a Machine safe point, Continuation, and
    /// higher-profile lifecycle records without dispatching an Effect.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid records, stale CAS revision, invalid state,
    /// or store failure.
    pub fn checkpoint_machine_journal(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
        journal_id: &str,
        records: &[JournalRecord],
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        self.mutate_checked(|state| {
            state.machine = machine.snapshot();
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
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
            ensure_direct_wait_completion(wait)?;
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
            ensure_direct_wait_completion(wait)?;
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

    /// Atomically admit one identified signal or timer delivery, complete all
    /// selected waits, and ready their Continuations.
    ///
    /// Concrete clock and signal plugins select exact pending wait IDs through
    /// their parked-wait indexes and may redeliver the same activation after a
    /// lost receipt. Reusing an activation ID with identical semantics is
    /// idempotent. Reusing it with different source, targets, or result fails.
    /// One signal activation may wake any number of broadcast waits but at most
    /// one consume-once wait; one timer activation targets exactly one wait.
    pub fn activate_waits(
        &mut self,
        machine: &Machine,
        activation: WaitActivation,
    ) -> DurableResult<String> {
        activation.verify()?;
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            match state.wait_activations.get(&activation.activation_id) {
                Some(existing) if existing == &activation => return Ok(()),
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait activation {} already exists with different semantics",
                        activation.activation_id
                    )));
                }
                None => {}
            }
            if machine.artifact(&activation.result).is_none() {
                return Err(DurableError::Validation(format!(
                    "wait activation {} result artifact is missing",
                    activation.activation_id
                )));
            }
            ensure_activation_machine(
                &state.machine,
                &machine_snapshot,
                &activation.result,
                &activation.activation_id,
            )?;

            let mut consume_once_targets = 0usize;
            let mut run_ids = std::collections::BTreeSet::new();
            for wait_id in &activation.wait_ids {
                let wait = state.waits.get(wait_id).ok_or_else(|| {
                    DurableError::NotFound(format!("wait {wait_id} does not exist"))
                })?;
                activation.source.ensure_matches(wait)?;
                if wait.state != WaitState::Pending {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait {wait_id} is not pending"
                    )));
                }
                if wait.consume_once {
                    consume_once_targets += 1;
                }
                run_ids.insert(wait.run_id.clone());
            }
            activation
                .source
                .validate_target_cardinality(activation.wait_ids.len(), consume_once_targets)?;

            state.machine = machine_snapshot;
            for wait_id in &activation.wait_ids {
                let wait = state.waits.get_mut(wait_id).ok_or_else(|| {
                    DurableError::NotFound(format!("wait {wait_id} does not exist"))
                })?;
                wait.state = WaitState::Completed;
                wait.result = Some(activation.result.clone());
            }
            for run_id in run_ids {
                let continuation = state.continuations.get_mut(&run_id).ok_or_else(|| {
                    DurableError::NotFound(format!("continuation {run_id} does not exist"))
                })?;
                for wait_id in &activation.wait_ids {
                    continuation.wait_set.remove(wait_id);
                }
                if continuation.wait_set.is_empty() {
                    continuation.status = ContinuationStatus::Ready;
                }
            }
            state
                .wait_activations
                .insert(activation.activation_id.clone(), activation);
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
            if !matches!(dispatch.state, OutboxState::Claimed | OutboxState::Unknown)
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
        self.mutate_checked(|state| append_journal_record(state, journal_id, record))
    }

    /// Atomically append records to multiple higher-profile journals.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/duplicate journal, invalid or conflicting
    /// records, stale CAS revision, or store failure.
    pub fn checkpoint_journals(&mut self, batches: &[JournalBatch]) -> DurableResult<String> {
        if batches.is_empty() {
            return Err(DurableError::Validation(
                "multi-journal checkpoint requires at least one journal".to_owned(),
            ));
        }
        let mut journal_ids = std::collections::BTreeSet::new();
        for batch in batches {
            validate_journal_batch(&batch.journal_id, &batch.records)?;
            if batch.records.is_empty() {
                return Err(DurableError::Validation(format!(
                    "application journal {} has no checkpoint records",
                    batch.journal_id
                )));
            }
            if !journal_ids.insert(&batch.journal_id) {
                return Err(DurableError::Validation(format!(
                    "application journal {} appears twice in one checkpoint",
                    batch.journal_id
                )));
            }
        }
        self.mutate_checked(|state| {
            for batch in batches {
                for record in &batch.records {
                    append_journal_record(state, &batch.journal_id, record.clone())?;
                }
            }
            Ok(())
        })
    }

    /// Atomically append higher-profile records and register one durable wait.
    pub fn checkpoint_journal_wait(
        &mut self,
        journal_id: &str,
        records: &[JournalRecord],
        wait: &WaitCondition,
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        if wait.state != WaitState::Pending || wait.result.is_some() {
            return Err(DurableError::Validation(
                "new journal checkpoint wait must be pending without a result".to_owned(),
            ));
        }
        self.mutate_checked(|state| {
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
            match state.waits.get(&wait.wait_id) {
                Some(existing) if existing == wait => {}
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait {} already exists with different semantics",
                        wait.wait_id
                    )));
                }
                None => {
                    state
                        .waits
                        .insert(wait.wait_id.clone(), WaitCondition::clone(wait));
                }
            }
            let continuation = state.continuations.get_mut(&wait.run_id).ok_or_else(|| {
                DurableError::NotFound(format!("continuation {} does not exist", wait.run_id))
            })?;
            continuation.wait_set.insert(wait.wait_id.clone());
            continuation.status = ContinuationStatus::Waiting;
            Ok(())
        })
    }

    /// Atomically complete one durable wait and append higher-profile records.
    pub fn checkpoint_journal_wait_completion(
        &mut self,
        journal_id: &str,
        records: &[JournalRecord],
        wait_id: &str,
        result: cymule_core::ArtifactRef,
    ) -> DurableResult<String> {
        validate_journal_batch(journal_id, records)?;
        self.mutate_checked(|state| {
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
            }
            let wait = state
                .waits
                .get_mut(wait_id)
                .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?;
            ensure_direct_wait_completion(wait)?;
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

fn ensure_direct_wait_completion(wait: &WaitCondition) -> DurableResult<()> {
    if !matches!(wait.kind, WaitKind::Input { .. }) {
        return Err(DurableError::Validation(format!(
            "wait {} requires an identified signal or timer activation",
            wait.wait_id
        )));
    }
    Ok(())
}

fn ensure_activation_machine(
    current: &cymule_core::MachineSnapshot,
    next: &cymule_core::MachineSnapshot,
    result: &cymule_core::ArtifactRef,
    activation_id: &str,
) -> DurableResult<()> {
    let mut expected = current.clone();
    if !expected
        .artifacts
        .iter()
        .any(|record| record.reference == *result)
    {
        let record = next
            .artifacts
            .iter()
            .find(|record| record.reference == *result)
            .ok_or_else(|| {
                DurableError::Validation(format!(
                    "wait activation {activation_id} result Artifact bytes are missing"
                ))
            })?;
        expected.artifacts.push(record.clone());
        expected
            .artifacts
            .sort_by(|left, right| left.reference.artifact_id.cmp(&right.reference.artifact_id));
    }
    if &expected != next {
        return Err(DurableError::Validation(format!(
            "wait activation {activation_id} Machine snapshot contains unrelated changes"
        )));
    }
    Ok(())
}

fn validate_journal_batch(journal_id: &str, records: &[JournalRecord]) -> DurableResult<()> {
    if journal_id.is_empty() {
        return Err(DurableError::Validation(
            "application journal identity must not be empty".to_owned(),
        ));
    }
    for record in records {
        record.verify()?;
    }
    Ok(())
}

fn append_journal_record(
    state: &mut DurableState,
    journal_id: &str,
    record: JournalRecord,
) -> DurableResult<()> {
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
}
