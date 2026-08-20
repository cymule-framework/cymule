use cymule_core::{
    EffectTransition, Event, EventPayload, Machine, MachineSnapshot, ReconciliationResolution,
    SealedPlan, WorldOutcome,
};

use crate::{
    AuthorityLease, ComponentOccurrence, Continuation, ContinuationStatus, DurableError,
    DurableResult, DurableState, DurableStore, EffectDispatch, HISTORY_COMPACTION_VERSION,
    HistoryCompactionReceipt, JournalBatch, JournalRecord, OutboxState, SnapshotRecord,
    StoredState, WaitActivation, WaitCondition, WaitKind, WaitState,
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

    /// Atomically initialize one Run and its first resumable Continuation.
    ///
    /// A process must never publish a canonical Run without the Continuation
    /// required to recover it. Receipt loss after this CAS is recoverable by
    /// reopening the store; a failure before the CAS leaves the store empty so
    /// the same start request can be retried.
    pub fn initialize_run(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
    ) -> DurableResult<String> {
        if self.stored.is_some() {
            return Err(DurableError::IllegalTransition(
                "durable store is already initialized".to_owned(),
            ));
        }
        ensure_run_start_machine(
            &Machine::new().snapshot(),
            &machine.snapshot(),
            &continuation,
        )?;
        let mut state = DurableState::new(machine.snapshot());
        state
            .continuations
            .insert(continuation.run_id.clone(), continuation);
        state.validate()?;
        let commit = self.store.compare_and_swap(None, &state)?;
        self.stored = Some(StoredState {
            revision: commit.revision.clone(),
            state,
        });
        Ok(commit.revision)
    }

    /// Atomically create one new Run and its first resumable Continuation.
    ///
    /// The first Run initializes an empty domain. Later Runs append one exact
    /// Plan/input/start/attempt delta to the existing Machine and publish their
    /// Continuation in the same CAS revision. Existing Run IDs fail closed;
    /// callers reopen and inspect the retained Run after an unknown receipt.
    pub fn create_run(
        &mut self,
        machine: &Machine,
        continuation: Continuation,
    ) -> DurableResult<String> {
        if self.stored.is_none() {
            return self.initialize_run(machine, continuation);
        }
        if self
            .state()?
            .continuations
            .contains_key(&continuation.run_id)
        {
            return Err(DurableError::IllegalTransition(format!(
                "Run {} already has a durable Continuation",
                continuation.run_id
            )));
        }
        let next_machine = machine.snapshot();
        ensure_run_start_machine(&self.state()?.machine, &next_machine, &continuation)?;
        self.mutate_checked(|state| {
            if state.continuations.contains_key(&continuation.run_id) {
                return Err(DurableError::IllegalTransition(format!(
                    "Run {} already has a durable Continuation",
                    continuation.run_id
                )));
            }
            state.machine = next_machine;
            state
                .continuations
                .insert(continuation.run_id.clone(), continuation);
            Ok(())
        })
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

    /// Rebuild the provider-neutral parked-wait index from durable authority.
    pub fn parked_wait_index(&self) -> DurableResult<crate::ParkedWaitIndex> {
        crate::ParkedWaitIndex::rebuild(self.state()?)
    }

    /// Restore the current semantic Machine from canonical durable inputs.
    pub fn restore_machine(&self) -> DurableResult<Machine> {
        Machine::restore(self.state()?.machine.clone()).map_err(Into::into)
    }

    /// Persist the current semantic Machine snapshot.
    pub fn persist_machine(&mut self, machine: &Machine) -> DurableResult<String> {
        self.mutate(|state| state.machine = machine.snapshot())
    }

    /// Compact one causal Event prefix and atomically publish its M1 receipt.
    pub fn compact_history(
        &mut self,
        compaction_id: &str,
        retain_suffix: usize,
    ) -> DurableResult<HistoryCompactionReceipt> {
        if compaction_id.is_empty() {
            return Err(DurableError::Validation(
                "history compaction identity must not be empty".to_owned(),
            ));
        }
        if let Some(existing) = self.state()?.history_compactions.get(compaction_id) {
            if existing.requested_suffix
                == u64::try_from(retain_suffix)
                    .map_err(|error| DurableError::Validation(error.to_string()))?
            {
                return Ok(existing.clone());
            }
            return Err(DurableError::IllegalTransition(format!(
                "history compaction {compaction_id} was reused with a different suffix"
            )));
        }
        let source_revision = self
            .revision()
            .ok_or_else(|| DurableError::NotFound("durable state is not initialized".to_owned()))?
            .to_owned();
        let parent_compaction = self
            .state()?
            .history_compactions
            .values()
            .max_by_key(|receipt| receipt.result.compacted_events)
            .map(|receipt| receipt.compaction_id.clone());
        let mut machine = self.restore_machine()?;
        let result = machine.compact_event_history(retain_suffix)?;
        let receipt = HistoryCompactionReceipt {
            compaction_version: HISTORY_COMPACTION_VERSION.to_owned(),
            compaction_id: compaction_id.to_owned(),
            parent_compaction,
            source_revision,
            requested_suffix: u64::try_from(retain_suffix)
                .map_err(|error| DurableError::Validation(error.to_string()))?,
            result,
        };
        receipt.verify()?;
        let snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            state.machine = snapshot;
            state
                .history_compactions
                .insert(compaction_id.to_owned(), receipt.clone());
            Ok(())
        })?;
        Ok(receipt)
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            match state.outbox.get(&dispatch.intent_id) {
                Some(existing) if existing == &dispatch => {
                    if state.machine != machine_snapshot
                        || state.continuations.get(&continuation.run_id) != Some(&continuation)
                    {
                        return Err(DurableError::IllegalTransition(format!(
                            "effect {} enqueue replay does not match current durable state",
                            dispatch.intent_id
                        )));
                    }
                    return Ok(());
                }
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "effect {} already has a different outbox entry",
                        dispatch.intent_id
                    )));
                }
                None => {
                    ensure_effect_enqueue_machine(&state.machine, &machine_snapshot, &dispatch)?;
                    state.machine = machine_snapshot.clone();
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            match state.outbox.get(&dispatch.intent_id) {
                Some(existing) if existing == &dispatch => {
                    if state.machine != machine_snapshot
                        || state.continuations.get(&continuation.run_id) != Some(&continuation)
                    {
                        return Err(DurableError::IllegalTransition(format!(
                            "effect {} enqueue replay does not match current durable state",
                            dispatch.intent_id
                        )));
                    }
                }
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "effect {} already has a different outbox entry",
                        dispatch.intent_id
                    )));
                }
                None => {
                    ensure_effect_enqueue_machine(&state.machine, &machine_snapshot, &dispatch)?;
                    state.machine = machine_snapshot.clone();
                    state.outbox.insert(dispatch.intent_id.clone(), dispatch);
                }
            }
            for record in records {
                append_journal_record(state, journal_id, record.clone())?;
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state != OutboxState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "effect {intent_id} is not pending"
                )));
            }
            ensure_effect_claim_machine(&state.machine, &machine_snapshot, dispatch)?;
            dispatch.state = OutboxState::Claimed;
            dispatch.claim_owner = Some(owner.to_owned());
            dispatch.claim_epoch = lease_epoch;
            state.machine = machine_snapshot.clone();
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            let dispatch = state.outbox.get_mut(intent_id).ok_or_else(|| {
                DurableError::NotFound(format!("effect {intent_id} is not in the outbox"))
            })?;
            if dispatch.state != OutboxState::Pending {
                return Err(DurableError::IllegalTransition(format!(
                    "effect {intent_id} is not pending"
                )));
            }
            ensure_effect_claim_machine(&state.machine, &machine_snapshot, dispatch)?;
            dispatch.state = OutboxState::Claimed;
            dispatch.claim_owner = Some(owner.to_owned());
            dispatch.claim_epoch = lease_epoch;
            state.machine = machine_snapshot.clone();
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
        let machine_snapshot = machine.snapshot();
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
            if already_settled && state.machine == machine_snapshot {
                return Ok(());
            }
            ensure_effect_settlement_machine(
                &state.machine,
                &machine_snapshot,
                dispatch,
                outcome,
                result.as_ref(),
            )?;
            dispatch.state = outcome;
            dispatch.result = result;
            state.machine = machine_snapshot.clone();
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
        let machine_snapshot = machine.snapshot();
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
            if !already_settled || state.machine != machine_snapshot {
                ensure_effect_settlement_machine(
                    &state.machine,
                    &machine_snapshot,
                    dispatch,
                    outcome,
                    result.as_ref(),
                )?;
                dispatch.state = outcome;
                dispatch.result = result;
                state.machine = machine_snapshot.clone();
            }
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
        self.checkpoint_wait_activation_journals(machine, activation, &[])
    }

    /// Atomically admit one wait activation together with higher-profile
    /// projection checkpoints.
    ///
    /// This is the cross-profile boundary used when an M1 activation wakes M3
    /// parked work or updates another derived controller. Any invalid or
    /// conflicting journal record rejects the activation and every wait update
    /// before CAS.
    pub fn checkpoint_wait_activation_journals(
        &mut self,
        machine: &Machine,
        activation: WaitActivation,
        batches: &[JournalBatch],
    ) -> DurableResult<String> {
        activation.verify()?;
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            let is_new = match state.wait_activations.get(&activation.activation_id) {
                Some(existing) if existing == &activation => false,
                Some(_) => {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait activation {} already exists with different semantics",
                        activation.activation_id
                    )));
                }
                None => true,
            };
            if !is_new {
                for batch in batches {
                    for record in &batch.records {
                        let existing =
                            state
                                .application_journals
                                .get(&batch.journal_id)
                                .and_then(|records| {
                                    records
                                        .iter()
                                        .find(|existing| existing.record_id == record.record_id)
                                });
                        if existing != Some(record) {
                            return Err(DurableError::IllegalTransition(format!(
                                "wait activation {} was committed without journal record {}",
                                activation.activation_id, record.record_id
                            )));
                        }
                    }
                }
                return Ok(());
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
            for batch in batches {
                for record in &batch.records {
                    append_journal_record(state, &batch.journal_id, record.clone())?;
                }
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
            let lease = proposed_lease(state, resource, owner, now, ttl)?;
            state.leases.insert(resource.to_owned(), lease.clone());
            acquired = Some(lease);
            Ok(())
        })?;
        acquired.ok_or_else(|| DurableError::Encoding("lease result disappeared".to_owned()))
    }

    /// Preview the exact lease a successful CAS would acquire without mutating
    /// durable state.
    pub fn preview_lease(
        &self,
        resource: &str,
        owner: &str,
        now: u64,
        ttl: u64,
    ) -> DurableResult<AuthorityLease> {
        proposed_lease(self.state()?, resource, owner, now, ttl)
    }

    /// Atomically acquire one exact previewed lease and append higher-profile
    /// journal records in the same durable CAS revision.
    pub fn checkpoint_lease_journals(
        &mut self,
        lease: &AuthorityLease,
        now: u64,
        ttl: u64,
        batches: &[JournalBatch],
    ) -> DurableResult<String> {
        if batches.is_empty() {
            return Err(DurableError::Validation(
                "lease journal checkpoint requires at least one journal".to_owned(),
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
                    "application journal {} appears twice in one lease checkpoint",
                    batch.journal_id
                )));
            }
        }
        self.mutate_checked(|state| {
            let expected = proposed_lease(state, &lease.resource, &lease.owner, now, ttl)?;
            if expected != *lease {
                return Err(DurableError::Conflict {
                    expected: Some(format!("{}:{}", lease.owner, lease.epoch)),
                    current: state
                        .leases
                        .get(&lease.resource)
                        .map(|current| format!("{}:{}", current.owner, current.epoch)),
                });
            }
            for batch in batches {
                for record in &batch.records {
                    append_journal_record(state, &batch.journal_id, record.clone())?;
                }
            }
            state.leases.insert(lease.resource.clone(), lease.clone());
            Ok(())
        })
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

    /// Atomically persist exact new Artifacts and higher-profile journal
    /// checkpoints without exposing a raw Machine mutation surface.
    ///
    /// The proposed Machine may differ from the current snapshot only by the
    /// listed immutable Artifacts. Plans, Events, commands, and unrelated
    /// Artifacts must remain byte-for-byte unchanged.
    pub fn checkpoint_artifact_journals(
        &mut self,
        machine: &Machine,
        artifacts: &std::collections::BTreeSet<cymule_core::ArtifactRef>,
        batches: &[JournalBatch],
    ) -> DurableResult<String> {
        if batches.is_empty() {
            return Err(DurableError::Validation(
                "Artifact journal checkpoint requires at least one journal".to_owned(),
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
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            ensure_artifact_machine(
                &state.machine,
                &machine_snapshot,
                artifacts,
                "Artifact journal checkpoint",
            )?;
            for batch in batches {
                for record in &batch.records {
                    append_journal_record(state, &batch.journal_id, record.clone())?;
                }
            }
            state.machine = machine_snapshot;
            Ok(())
        })
    }

    /// Atomically publish one input Artifact, complete its wait, and append
    /// higher-profile records.
    pub fn checkpoint_input_wait_journals(
        &mut self,
        machine: &Machine,
        result: &cymule_core::ArtifactRef,
        wait_id: &str,
        batches: &[JournalBatch],
    ) -> DurableResult<String> {
        if batches.is_empty() {
            return Err(DurableError::Validation(
                "input wait checkpoint requires at least one journal".to_owned(),
            ));
        }
        let mut journal_ids = std::collections::BTreeSet::new();
        for batch in batches {
            validate_journal_batch(&batch.journal_id, &batch.records)?;
            if batch.records.is_empty() || !journal_ids.insert(&batch.journal_id) {
                return Err(DurableError::Validation(
                    "input wait checkpoint requires non-empty unique journals".to_owned(),
                ));
            }
        }
        let machine_snapshot = machine.snapshot();
        self.mutate_checked(|state| {
            ensure_artifact_machine(
                &state.machine,
                &machine_snapshot,
                &std::collections::BTreeSet::from([result.clone()]),
                "input wait checkpoint",
            )?;
            let wait = state
                .waits
                .get_mut(wait_id)
                .ok_or_else(|| DurableError::NotFound(format!("wait {wait_id} does not exist")))?;
            ensure_direct_wait_completion(wait)?;
            match wait.state {
                WaitState::Completed if wait.result.as_ref() == Some(result) => {}
                WaitState::Pending => {
                    wait.state = WaitState::Completed;
                    wait.result = Some(result.clone());
                    let continuation =
                        state.continuations.get_mut(&wait.run_id).ok_or_else(|| {
                            DurableError::NotFound(format!(
                                "continuation {} does not exist",
                                wait.run_id
                            ))
                        })?;
                    continuation.wait_set.remove(wait_id);
                    if continuation.wait_set.is_empty() {
                        continuation.status = ContinuationStatus::Ready;
                    }
                }
                _ => {
                    return Err(DurableError::IllegalTransition(format!(
                        "wait {wait_id} cannot accept this input result"
                    )));
                }
            }
            for batch in batches {
                for record in &batch.records {
                    append_journal_record(state, &batch.journal_id, record.clone())?;
                }
            }
            state.machine = machine_snapshot;
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

fn ensure_effect_enqueue_machine(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    dispatch: &EffectDispatch,
) -> DurableResult<()> {
    let events = ensure_canonical_machine_delta(
        current,
        next,
        &std::collections::BTreeSet::from([dispatch.input.clone()]),
        "effect enqueue",
    )?;
    if !matches!(events.len(), 2 | 3) {
        return Err(DurableError::Validation(
            "effect enqueue must add propose/prepare and optional matching scope-commit Events"
                .to_owned(),
        ));
    }
    let proposed = &events[0];
    let prepared = &events[1];
    let proposed_scope = match &proposed.payload {
        EventPayload::EffectProposed {
            intent_id,
            scope_id,
            operation,
            mutating: _,
            args,
            occurrence_binding,
        } if proposed.run_id == dispatch.run_id
            && intent_id == &dispatch.intent_id
            && operation == &dispatch.operation
            && args == &dispatch.input
            && occurrence_binding == &dispatch.occurrence_binding =>
        {
            Some(scope_id)
        }
        _ => None,
    };
    let prepared_matches = prepared.run_id == dispatch.run_id
        && matches!(
            &prepared.payload,
            EventPayload::EffectTransitioned { intent_id, transition: EffectTransition::Prepare }
                if intent_id == &dispatch.intent_id
        );
    let scope_commit_matches = events.get(2).is_none_or(|committed| {
        committed.run_id == dispatch.run_id
            && matches!(
                (&committed.payload, proposed_scope),
                (EventPayload::ScopeCommitted { scope_id, .. }, Some(expected))
                    if scope_id == expected
            )
    });
    if proposed_scope.is_none() || !prepared_matches || !scope_commit_matches {
        return Err(DurableError::Validation(
            "effect enqueue Events do not match the pending outbox entry".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_run_start_machine(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    continuation: &Continuation,
) -> DurableResult<()> {
    if continuation.status != ContinuationStatus::Running
        || continuation.epoch != 0
        || continuation.frames.len() != 1
        || continuation.state.as_ref() != continuation.frames.first().map(|frame| &frame.input)
        || continuation.scope_stack != [cymule_core::ROOT_SCOPE_ID]
        || !continuation.wait_set.is_empty()
        || !continuation.effect_obligations.is_empty()
        || !continuation.authority_leases.is_empty()
        || !continuation.budget.is_empty()
        || !continuation.causal_frontier.is_empty()
    {
        return Err(DurableError::Validation(
            "new Run Continuation is not at its exact initial boundary".to_owned(),
        ));
    }
    let plan = next
        .plans
        .iter()
        .find(|plan| plan.plan_id == continuation.plan_id)
        .ok_or_else(|| DurableError::Validation("new Run Plan is missing".to_owned()))?;
    let input = continuation
        .state
        .as_ref()
        .ok_or_else(|| DurableError::Validation("new Run input is missing".to_owned()))?;
    let binding = cymule_core::ArtifactRef {
        identity_version: cymule_core::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: continuation.binding_context.clone(),
        kind: cymule_runtime::EXECUTION_BINDING_VERSION.to_owned(),
    };
    let binding_record = next
        .artifacts
        .iter()
        .find(|record| record.reference.artifact_id == binding.artifact_id)
        .ok_or_else(|| {
            DurableError::Validation("new Run execution binding Artifact is missing".to_owned())
        })?;
    if binding_record.reference != binding {
        return Err(DurableError::Validation(
            "new Run execution binding Artifact reference is malformed".to_owned(),
        ));
    }
    let binding_descriptor: cymule_runtime::ExecutionBinding =
        cymule_core::decode_json(&binding_record.bytes)?;
    binding_descriptor.verify()?;
    if binding_descriptor.artifact_ref()? != binding {
        return Err(DurableError::Validation(
            "new Run execution binding Artifact identity is invalid".to_owned(),
        ));
    }
    binding_descriptor.admit_plan(plan)?;
    let events = ensure_canonical_machine_delta_with_plan(
        current,
        next,
        Some(plan),
        &std::collections::BTreeSet::from([input.clone(), binding.clone()]),
        "Run creation",
    )?;
    let [started, attempt] = events.as_slice() else {
        return Err(DurableError::Validation(
            "Run creation must add exactly start and first-attempt Events".to_owned(),
        ));
    };
    let run_matches = started.run_id == continuation.run_id
        && matches!(
            &started.payload,
            EventPayload::RunStarted { plan_id, binding_context }
                if plan_id == &continuation.plan_id
                    && binding_context == &continuation.binding_context
        );
    let attempt_matches = attempt.run_id == continuation.run_id
        && matches!(
            &attempt.payload,
            EventPayload::AttemptStarted {
                attempt_id,
                continuation_id,
                occurrence_binding,
                epoch,
            } if attempt_id == &format!("attempt:{}:0", continuation.run_id)
                && continuation_id == &format!("continuation:{}", continuation.run_id)
                && occurrence_binding == &continuation.binding_context
                && *epoch == 0
        );
    if !run_matches || !attempt_matches {
        return Err(DurableError::Validation(
            "Run creation Events do not match the initial Continuation".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_effect_claim_machine(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    dispatch: &EffectDispatch,
) -> DurableResult<()> {
    let events = ensure_canonical_machine_delta(
        current,
        next,
        &std::collections::BTreeSet::new(),
        "effect claim",
    )?;
    let [authorized, started] = events.as_slice() else {
        return Err(DurableError::Validation(
            "effect claim must add exactly authorize-release and dispatch-started Events"
                .to_owned(),
        ));
    };
    let matches_transition = |event: &Event, transition: EffectTransition| {
        event.run_id == dispatch.run_id
            && matches!(
                &event.payload,
                EventPayload::EffectTransitioned { intent_id, transition: actual }
                    if intent_id == &dispatch.intent_id && actual == &transition
            )
    };
    if !matches_transition(authorized, EffectTransition::AuthorizeRelease)
        || !matches_transition(started, EffectTransition::StartDispatch)
    {
        return Err(DurableError::Validation(
            "effect claim Events do not match the pending outbox entry".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_effect_settlement_machine(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    dispatch: &EffectDispatch,
    outcome: OutboxState,
    result: Option<&cymule_core::ArtifactRef>,
) -> DurableResult<()> {
    if outcome == OutboxState::Unknown && result.is_some() {
        return Err(DurableError::Validation(
            "unknown effect outcome cannot publish a result Artifact".to_owned(),
        ));
    }
    let artifacts = result.cloned().into_iter().collect();
    let events = ensure_canonical_machine_delta(current, next, &artifacts, "effect settlement")?;
    let [observed] = events.as_slice() else {
        return Err(DurableError::Validation(
            "effect settlement must add exactly one observation or reconciliation Event".to_owned(),
        ));
    };
    if observed.run_id != dispatch.run_id {
        return Err(DurableError::Validation(
            "effect settlement Event escaped its Run".to_owned(),
        ));
    }
    let EventPayload::EffectTransitioned {
        intent_id,
        transition,
    } = &observed.payload
    else {
        return Err(DurableError::Validation(
            "effect settlement did not add an effect transition".to_owned(),
        ));
    };
    let transition_matches = intent_id == &dispatch.intent_id
        && matches!(
            (outcome, transition),
            (
                OutboxState::Applied,
                EffectTransition::Observe(WorldOutcome::Applied)
                    | EffectTransition::Reconcile(ReconciliationResolution::ResolvedApplied)
            ) | (
                OutboxState::NotApplied,
                EffectTransition::Observe(WorldOutcome::NotApplied)
                    | EffectTransition::Reconcile(ReconciliationResolution::ResolvedNotApplied)
            ) | (
                OutboxState::Unknown,
                EffectTransition::Observe(WorldOutcome::Unknown)
                    | EffectTransition::Reconcile(
                        ReconciliationResolution::StillUnknown
                            | ReconciliationResolution::GovernanceRequired
                    )
            )
        );
    if !transition_matches {
        return Err(DurableError::Validation(
            "effect settlement transition does not match the outbox outcome".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_canonical_machine_delta(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    artifacts: &std::collections::BTreeSet<cymule_core::ArtifactRef>,
    operation: &str,
) -> DurableResult<Vec<Event>> {
    ensure_canonical_machine_delta_with_plan(current, next, None, artifacts, operation)
}

fn ensure_canonical_machine_delta_with_plan(
    current: &MachineSnapshot,
    next: &MachineSnapshot,
    allowed_plan: Option<&SealedPlan>,
    artifacts: &std::collections::BTreeSet<cymule_core::ArtifactRef>,
    operation: &str,
) -> DurableResult<Vec<Event>> {
    if current.snapshot_version != next.snapshot_version || current.base != next.base {
        return Err(DurableError::Validation(format!(
            "{operation} cannot change Machine version or compacted base"
        )));
    }
    let mut expected_plans = current.plans.clone();
    if let Some(plan) = allowed_plan
        && !expected_plans
            .iter()
            .any(|existing| existing.plan_id == plan.plan_id)
    {
        plan.verify()?;
        expected_plans.push(plan.clone());
        expected_plans.sort_by(|left, right| left.plan_id.cmp(&right.plan_id));
    }
    if expected_plans != next.plans {
        return Err(DurableError::Validation(format!(
            "{operation} contains unrelated Plan changes"
        )));
    }
    if next.events.len() < current.events.len()
        || next.events[..current.events.len()] != current.events
    {
        return Err(DurableError::Validation(format!(
            "{operation} must append to the current Event history"
        )));
    }
    let new_events = next.events[current.events.len()..].to_vec();
    let mut expected_artifacts = current.artifacts.clone();
    for artifact in artifacts {
        if expected_artifacts
            .iter()
            .any(|record| record.reference == *artifact)
        {
            continue;
        }
        let record = next
            .artifacts
            .iter()
            .find(|record| record.reference == *artifact)
            .ok_or_else(|| {
                DurableError::Validation(format!(
                    "{operation} Artifact {} bytes are missing",
                    artifact.artifact_id
                ))
            })?;
        expected_artifacts.push(record.clone());
    }
    expected_artifacts
        .sort_by(|left, right| left.reference.artifact_id.cmp(&right.reference.artifact_id));
    if expected_artifacts != next.artifacts {
        return Err(DurableError::Validation(format!(
            "{operation} Machine contains unrelated Artifact changes"
        )));
    }

    let current_commands = current.command_digests()?;
    let next_commands = next.command_digests()?;
    if current_commands
        .iter()
        .any(|(id, digest)| next_commands.get(id) != Some(digest))
    {
        return Err(DurableError::Validation(format!(
            "{operation} changed or removed an existing command receipt"
        )));
    }
    let new_command_ids: std::collections::BTreeSet<String> = next_commands
        .keys()
        .filter(|id| !current_commands.contains_key(*id))
        .cloned()
        .collect();
    let event_command_ids: std::collections::BTreeSet<String> = new_events
        .iter()
        .map(|event| event.command_id.clone())
        .collect();
    if new_command_ids != event_command_ids || new_command_ids.len() != new_events.len() {
        return Err(DurableError::Validation(format!(
            "{operation} command receipts do not match its appended Events"
        )));
    }
    Ok(new_events)
}

fn ensure_activation_machine(
    current: &cymule_core::MachineSnapshot,
    next: &cymule_core::MachineSnapshot,
    result: &cymule_core::ArtifactRef,
    activation_id: &str,
) -> DurableResult<()> {
    ensure_artifact_machine(
        current,
        next,
        &std::collections::BTreeSet::from([result.clone()]),
        &format!("wait activation {activation_id}"),
    )
}

fn ensure_artifact_machine(
    current: &cymule_core::MachineSnapshot,
    next: &cymule_core::MachineSnapshot,
    artifacts: &std::collections::BTreeSet<cymule_core::ArtifactRef>,
    operation: &str,
) -> DurableResult<()> {
    let mut expected = current.clone();
    for artifact in artifacts {
        if expected
            .artifacts
            .iter()
            .any(|record| record.reference == *artifact)
        {
            continue;
        }
        let record = next
            .artifacts
            .iter()
            .find(|record| record.reference == *artifact)
            .ok_or_else(|| {
                DurableError::Validation(format!(
                    "{operation} Artifact {} bytes are missing",
                    artifact.artifact_id
                ))
            })?;
        expected.artifacts.push(record.clone());
    }
    expected
        .artifacts
        .sort_by(|left, right| left.reference.artifact_id.cmp(&right.reference.artifact_id));
    if &expected != next {
        return Err(DurableError::Validation(format!(
            "{operation} Machine snapshot contains unrelated changes"
        )));
    }
    Ok(())
}

fn proposed_lease(
    state: &DurableState,
    resource: &str,
    owner: &str,
    now: u64,
    ttl: u64,
) -> DurableResult<AuthorityLease> {
    if resource.is_empty() || owner.is_empty() || ttl == 0 {
        return Err(DurableError::Validation(
            "lease resource, owner, and positive TTL are required".to_owned(),
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
    let epoch = match state.leases.get(resource) {
        Some(lease) => lease.epoch.checked_add(1).ok_or_else(|| {
            DurableError::Validation(format!("lease {resource} exhausted its fencing epoch"))
        })?,
        None => 1,
    };
    let expires_at = now.checked_add(ttl).ok_or_else(|| {
        DurableError::Validation(format!(
            "lease {resource} expiry exceeds logical time range"
        ))
    })?;
    Ok(AuthorityLease {
        resource: resource.to_owned(),
        owner: owner.to_owned(),
        epoch,
        expires_at,
    })
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
