use super::super::*;
use super::*;
use crate::{COMMAND_VERSION, InvocationPathSegment};
use cymule_authenticated_collections as collections;

const TEST_RUN: &str = "run:inline-scope";

fn identity(label: &str) -> String {
    content_id("test.inline-scope/1", &label).expect("test identity derives")
}

#[derive(Clone, Copy)]
enum Action {
    Commit,
    Abort,
}

impl Action {
    fn command(self, scope_id: &str) -> Command {
        match self {
            Self::Commit => Command::CommitScope {
                scope_id: scope_id.to_owned(),
            },
            Self::Abort => Command::AbortScope {
                scope_id: scope_id.to_owned(),
            },
        }
    }
}

#[derive(Default, Clone)]
struct ProofStore {
    maps: BTreeMap<String, collections::MapNode>,
    logs: BTreeMap<String, collections::LogNode>,
}

impl collections::CollectionResolver for ProofStore {
    fn load_map_node(&mut self, id: &str) -> collections::Result<Option<collections::MapNode>> {
        Ok(self.maps.get(id).cloned())
    }

    fn load_log_node(&mut self, id: &str) -> collections::Result<Option<collections::LogNode>> {
        Ok(self.logs.get(id).cloned())
    }
}

impl ProofStore {
    fn map(&mut self, entries: Vec<(String, String)>) -> MachineMapRoot {
        let (root, nodes) = collections::build_map(entries)
            .expect("map builds")
            .into_parts();
        self.maps
            .extend(nodes.into_iter().map(|node| (node.object_id.clone(), node)));
        root
    }

    fn log(&mut self, values: &[String]) -> MachineLogRoot {
        let (root, nodes) = collections::build_log(values)
            .expect("log builds")
            .into_parts();
        self.logs
            .extend(nodes.into_iter().map(|node| (node.object_id.clone(), node)));
        root
    }

    fn index(&mut self, selector: &MachineRunIndexSelector, ids: &[String]) -> MachineMapRoot {
        self.map(
            ids.iter()
                .map(|id| {
                    (
                        id.clone(),
                        machine_index_membership_value_id(TEST_RUN, selector, id)
                            .expect("membership identity derives"),
                    )
                })
                .collect(),
        )
    }

    fn order(&mut self, selector: &MachineRunLogSelector, ids: &[String]) -> MachineLogRoot {
        self.log(
            &ids.iter()
                .map(|id| {
                    machine_order_entry_value_id(TEST_RUN, selector, id)
                        .expect("order identity derives")
                })
                .collect::<Vec<_>>(),
        )
    }

    fn index_page(
        &mut self,
        run_id: &str,
        selector: MachineRunIndexSelector,
        source: &MachineMapRoot,
        after: Option<&MapPosition>,
    ) -> MachineRunIndexPage {
        let proof = collections::prove_map_range(
            source,
            after,
            MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            collections::MAX_PAGE_BYTES,
            self,
        )
        .expect("map range proves");
        MachineRunIndexPage::verify_proof(run_id.to_owned(), selector, source, after, &proof)
            .expect("map range verifies")
    }

    fn log_page(
        &mut self,
        run_id: &str,
        selector: MachineRunLogSelector,
        source: &MachineLogRoot,
        start: u64,
        entries: Vec<String>,
    ) -> MachineRunLogPage {
        let proof = collections::prove_log_range(
            source,
            start,
            MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
            collections::MAX_PAGE_BYTES,
            self,
        )
        .expect("log range proves");
        MachineRunLogPage::verify_proof(run_id.to_owned(), selector, source, start, entries, &proof)
            .expect("log range verifies")
    }
}

fn root_scope() -> MachineScopeCurrent {
    MachineScopeCurrent {
        scope_current_version: MACHINE_SCOPE_CURRENT_VERSION.to_owned(),
        scope_id: ROOT_SCOPE_ID.to_owned(),
        parent_scope: None,
        invocation_id: identity("invocation"),
        invocation_path_digest: canonical_digest(&Vec::<InvocationPathSegment>::new())
            .expect("path hashes"),
        definition_id: "main".to_owned(),
        region_path_digest: canonical_digest(&Vec::<usize>::new()).expect("path hashes"),
        site_id: None,
        status: crate::ScopeStatus::Open,
        effect_count: 0,
        direct_open_child_count: 0,
        effect_lineage_root: lineage_genesis(MACHINE_SCOPE_EFFECT_LINEAGE_DOMAIN)
            .expect("lineage derives"),
        effects: MachineMapRoot::empty(),
        effect_order: MachineLogRoot::empty(),
        mutating_effect_lineage_root: lineage_genesis(MACHINE_SCOPE_MUTATING_EFFECT_LINEAGE_DOMAIN)
            .expect("lineage derives"),
        mutating_effects: MachineMapRoot::empty(),
        mutating_effect_order: MachineLogRoot::empty(),
        abort_transitions: MachineMapRoot::empty(),
        abort_blockers: MachineMapRoot::empty(),
    }
}

fn effect(scope: &MachineScopeCurrent, ordinal: usize) -> crate::EffectProjection {
    crate::EffectProjection {
        intent_id: identity(&format!("effect:{ordinal}")),
        origin_plan_id: identity("plan"),
        scope_id: scope.scope_id.clone(),
        invocation_id: scope.invocation_id.clone(),
        invocation_path: Vec::new(),
        definition_id: "main".to_owned(),
        region_path: Vec::new(),
        site_id: format!("effect_{ordinal}"),
        occurrence: format!("occurrence:{ordinal}"),
        effect_schema_version: crate::EFFECT_SCHEMA_VERSION.to_owned(),
        operation: "effect.test".to_owned(),
        profile: crate::EffectProfile {
            mutation: crate::MutationKind::Mutating,
            dispatch: crate::DispatchPolicy::OnScopeCommit,
            reconciliation: crate::ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        },
        args: crate::artifact_ref(crate::EFFECT_ARGS_ARTIFACT_KIND, b"{}").expect("args derive"),
        execution_binding: crate::artifact_ref(crate::EXECUTION_BINDING_ARTIFACT_KIND, b"binding")
            .expect("binding derives"),
        occurrence_binding: identity("binding-occurrence"),
        execution_availability: crate::EffectExecutionAvailability::Available,
        phase: crate::EffectPhase::Prepared,
        outcome: crate::WorldOutcome::Unobserved,
        reconciliation: crate::ReconciliationState::NotRequired,
    }
}

fn populate_scope(scope: &mut MachineScopeCurrent, ids: &[String], store: &mut ProofStore) {
    scope.effects = store.index(
        &MachineRunIndexSelector::ScopeEffects {
            scope_id: scope.scope_id.clone(),
        },
        ids,
    );
    scope.mutating_effects = store.index(
        &MachineRunIndexSelector::ScopeMutatingEffects {
            scope_id: scope.scope_id.clone(),
        },
        ids,
    );
    scope.abort_transitions = store.index(
        &MachineRunIndexSelector::ScopeAbortTransitions {
            scope_id: scope.scope_id.clone(),
        },
        ids,
    );
    scope.effect_order = store.order(
        &MachineRunLogSelector::ScopeEffects {
            scope_id: scope.scope_id.clone(),
        },
        ids,
    );
    scope.mutating_effect_order = store.order(
        &MachineRunLogSelector::ScopeMutatingEffects {
            scope_id: scope.scope_id.clone(),
        },
        ids,
    );
    scope.effect_count = u64::try_from(ids.len()).expect("count fits");
    for id in ids {
        scope.effect_lineage_root = lineage_append(
            MACHINE_SCOPE_EFFECT_LINEAGE_DOMAIN,
            &scope.effect_lineage_root,
            id,
        )
        .expect("lineage advances");
        scope.mutating_effect_lineage_root = lineage_append(
            MACHINE_SCOPE_MUTATING_EFFECT_LINEAGE_DOMAIN,
            &scope.mutating_effect_lineage_root,
            id,
        )
        .expect("lineage advances");
    }
    scope.verify().expect("Scope verifies");
}

fn run_current(
    scope: &MachineScopeCurrent,
    ids: &[String],
    store: &mut ProofStore,
) -> MachineRunCurrent {
    let plan = identity("plan");
    let binding = crate::artifact_ref(crate::EXECUTION_BINDING_ARTIFACT_KIND, b"binding")
        .expect("binding derives")
        .artifact_id;
    let scopes = vec![scope.scope_id.clone()];
    let plans = store.order(&MachineRunLogSelector::Plans, std::slice::from_ref(&plan));
    let bindings = store.order(
        &MachineRunLogSelector::Bindings,
        std::slice::from_ref(&binding),
    );
    let indexes = MachineRunIndexRoots {
        governance_effects: MachineMapRoot::empty(),
        unknown_effects: MachineMapRoot::empty(),
        pending_effects: store.index(&MachineRunIndexSelector::PendingEffects, ids),
        terminal_transition_effects: store
            .index(&MachineRunIndexSelector::TerminalTransitionEffects, ids),
        open_scopes: store.index(&MachineRunIndexSelector::OpenScopes, &scopes),
        unresolved_obligations: MachineMapRoot::empty(),
    };
    let current = MachineRunCurrent {
        run_current_version: MACHINE_RUN_CURRENT_VERSION.to_owned(),
        run_id: TEST_RUN.to_owned(),
        initial_plan: plan.clone(),
        current_plan: plan,
        plan_lineage_root: identity("plan-lineage"),
        plan_lineage_count: 1,
        plan_lineage: plans.clone(),
        initial_binding_context: binding.clone(),
        current_binding_context: binding,
        binding_lineage_root: identity("binding-lineage"),
        binding_lineage_count: 1,
        binding_lineage: bindings.clone(),
        epoch: 0,
        execution_status: crate::RunExecutionStatus::Active,
        world_settlement: indexes.settlement(),
        result: None,
        last_event: identity("last-event"),
        active_attempt_id: None,
        committed_effect_count: 0,
        reducer_state: MachineRunReducerState::Ready,
        children: MachineRunChildRoots {
            scopes: store.map(vec![(scope.scope_id.clone(), identity("scope-leaf"))]),
            effects: store.map(
                ids.iter()
                    .map(|id| (id.clone(), identity(&format!("effect-leaf:{id}"))))
                    .collect(),
            ),
            obligations: MachineMapRoot::empty(),
            attempts: MachineMapRoot::empty(),
        },
        order: MachineRunOrderRoots {
            scopes: store.order(&MachineRunLogSelector::Scopes, &scopes),
            effects: store.order(&MachineRunLogSelector::Effects, ids),
            obligations: MachineLogRoot::empty(),
            attempts: MachineLogRoot::empty(),
            plans,
            bindings,
        },
        indexes,
    };
    current.verify().expect("Run verifies");
    current
}

#[derive(Clone)]
struct Fixture {
    envelope: CommandEnvelope,
    scope: MachineScopeCurrent,
    inputs: MachineRunReadInputs,
    store: ProofStore,
}

impl Fixture {
    fn new(count: usize, action: Action) -> Self {
        Self::with_scope(count, action, root_scope())
    }

    fn with_scope(count: usize, action: Action, mut scope: MachineScopeCurrent) -> Self {
        let mut store = ProofStore::default();
        let effects = (0..count)
            .map(|index| effect(&scope, index))
            .collect::<Vec<_>>();
        let ids = effects
            .iter()
            .map(|effect| effect.intent_id.clone())
            .collect::<Vec<_>>();
        populate_scope(&mut scope, &ids, &mut store);
        let current = run_current(&scope, &ids, &mut store);
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:inline-scope".to_owned(),
            actor: "actor:inline-scope".to_owned(),
            run_id: TEST_RUN.to_owned(),
            expected_precondition: Some(current.precondition_token()),
            command: action.command(&scope.scope_id),
        };
        let requirement = MachineInlineScopeReadRequirement::from_scope(&envelope, &scope)
            .expect("sources derive");
        let index = store.index_page(
            TEST_RUN,
            requirement.index_selector.clone(),
            &requirement.index_root,
            None,
        );
        let log = store.log_page(
            TEST_RUN,
            requirement.log_selector.clone(),
            &requirement.log_root,
            0,
            ids.iter()
                .take(MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES)
                .cloned()
                .collect(),
        );
        let obligations = match action {
            Action::Commit => ids
                .iter()
                .map(|id| (effect_obligation_id(id).expect("obligation derives"), None))
                .collect(),
            Action::Abort => BTreeMap::new(),
        };
        let inputs = MachineRunReadInputs {
            machine_revision: identity("revision"),
            run_id: TEST_RUN.to_owned(),
            runs_root: store.map(vec![(TEST_RUN.to_owned(), identity("run-leaf"))]),
            facts_root: MachineMapRoot::empty(),
            run: Some(current),
            new_run_empty_root: None,
            new_run_empty_log: None,
            plans: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            scopes: BTreeMap::from([(scope.scope_id.clone(), Some(scope.clone()))]),
            scope_locations: BTreeMap::new(),
            effects: effects
                .into_iter()
                .map(|effect| (effect.intent_id.clone(), Some(effect)))
                .collect(),
            obligations,
            attempts: BTreeMap::new(),
            facts: BTreeMap::new(),
            start_material: None,
            index_pages: vec![index],
            log_pages: vec![log],
        };
        Self {
            envelope,
            scope,
            inputs,
            store,
        }
    }

    fn nested(count: usize, action: Action) -> Self {
        let mut scope = root_scope();
        scope.scope_id = "scope:inline-child".to_owned();
        scope.parent_scope = Some(ROOT_SCOPE_ID.to_owned());
        scope.site_id = Some("child".to_owned());
        let mut fixture = Self::with_scope(count, action, scope);
        let mut parent = root_scope();
        parent.direct_open_child_count = 1;
        fixture
            .inputs
            .scopes
            .insert(ROOT_SCOPE_ID.to_owned(), Some(parent));
        let scope_ids = vec![ROOT_SCOPE_ID.to_owned(), fixture.scope.scope_id.clone()];
        let current = fixture.inputs.run.as_mut().expect("Run exists");
        current.children.scopes = fixture.store.map(
            scope_ids
                .iter()
                .map(|id| (id.clone(), identity(&format!("scope-leaf:{id}"))))
                .collect(),
        );
        current.order.scopes = fixture
            .store
            .order(&MachineRunLogSelector::Scopes, &scope_ids);
        current.indexes.open_scopes = fixture
            .store
            .index(&MachineRunIndexSelector::OpenScopes, &scope_ids);
        current
            .verify()
            .expect("Run with two exact Scopes verifies");
        fixture
    }

    fn requirement(&self) -> MachineInlineScopeReadRequirement {
        MachineInlineScopeReadRequirement::from_scope(&self.envelope, &self.scope)
            .expect("requirement derives")
    }

    fn verify(&self) -> Result<InlineScopeClosure> {
        InlineScopeClosure::verify(&self.envelope, self.requirement(), &self.inputs)
    }

    fn replace_scope(&mut self, scope: MachineScopeCurrent) {
        self.inputs
            .scopes
            .insert(scope.scope_id.clone(), Some(scope.clone()));
        self.scope = scope;
    }

    fn frontier(&self) -> MachineAuthorityFrontier {
        let mut frontier = MachineAuthorityFrontier::genesis(
            MachineMapRoot::empty(),
            MachineMapRoot::empty(),
            MachineMapRoot::empty(),
            MachineMapRoot::empty(),
        )
        .expect("frontier derives");
        frontier.runs = self.inputs.runs_root.clone();
        frontier.authority_root = frontier.expected_authority_root().expect("frontier hashes");
        frontier.verify().expect("frontier verifies");
        frontier
    }
}

fn context(length: u32) -> BatchReadContext {
    BatchReadContext {
        batch_id: identity("batch"),
        position: 0,
        length,
    }
}

#[test]
fn private_batch_context_selects_bounded_scopes_and_preserves_paged_overflow() {
    let fixture = Fixture::new(1, Action::Commit);
    assert!(
        scope_read_requirement(None, &fixture.envelope, &fixture.scope)
            .expect("ordinary route")
            .is_none()
    );
    for length in [1, 5] {
        assert!(
            scope_read_requirement(Some(&context(length)), &fixture.envelope, &fixture.scope)
                .expect("inline route")
                .is_some()
        );
    }
    let mut non_scope = fixture.envelope.clone();
    non_scope.command = Command::AdvanceEpoch;
    assert!(
        scope_read_requirement(Some(&context(5)), &non_scope, &fixture.scope)
            .expect("ordinary command")
            .is_none()
    );
    let wide = Fixture::new(MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES + 1, Action::Commit);
    assert!(
        scope_read_requirement(Some(&context(1)), &wide.envelope, &wide.scope)
            .expect("single paged route")
            .is_none()
    );
    assert!(matches!(
        scope_read_requirement(Some(&context(5)), &wide.envelope, &wide.scope),
        Err(CoreError::PagedScopeRequired { entries: 257, .. })
    ));
    assert!(matches!(
        wide.verify(),
        Err(CoreError::PagedScopeRequired { entries: 257, .. })
    ));
}

#[test]
fn empty_single_and_maximum_scope_membership_and_order_are_complete() {
    for action in [Action::Commit, Action::Abort] {
        for count in [0, 1, MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES] {
            let fixture = Fixture::new(count, action);
            let witness = fixture
                .verify()
                .expect("whole authenticated Scope verifies");
            assert_eq!(witness.effect_ids, fixture.inputs.log_pages[0].entries());
            assert_eq!(witness.effect_ids.len(), count);
            assert_eq!(
                witness.obligation_ids.len(),
                if matches!(action, Action::Commit) {
                    count
                } else {
                    0
                }
            );
        }
    }
}

#[test]
fn scope_closure_requires_childless_open_exact_target_and_abortability() {
    let fixture = Fixture::new(1, Action::Commit);
    let mut changed = fixture.scope.clone();
    changed.direct_open_child_count = 1;
    assert!(scope_read_requirement(Some(&context(5)), &fixture.envelope, &changed).is_err());
    changed = fixture.scope.clone();
    changed.status = crate::ScopeStatus::ClosedCommitted;
    changed.abort_transitions = MachineMapRoot::empty();
    assert!(scope_read_requirement(Some(&context(5)), &fixture.envelope, &changed).is_err());
    let mut abort = Fixture::new(1, Action::Abort);
    abort.scope.abort_blockers = abort.scope.effects.clone();
    assert!(scope_read_requirement(Some(&context(1)), &abort.envelope, &abort.scope).is_err());
    let mut requirement = fixture.requirement();
    requirement.scope_id = "scope:missing-child".to_owned();
    let mut envelope = fixture.envelope.clone();
    envelope.command = Command::CommitScope {
        scope_id: requirement.scope_id.clone(),
    };
    assert!(matches!(
        InlineScopeClosure::verify(&envelope, requirement, &fixture.inputs),
        Err(CoreError::PinnedReadSetIncomplete {
            family: "Machine inline Scope current",
            ..
        })
    ));
}

#[test]
fn inline_scope_rejects_missing_and_extra_complete_pages() {
    let fixture = Fixture::new(1, Action::Commit);
    let mutations: [fn(&mut MachineRunReadInputs); 4] = [
        |inputs| inputs.index_pages.clear(),
        |inputs| inputs.log_pages.clear(),
        |inputs| inputs.index_pages.push(inputs.index_pages[0].clone()),
        |inputs| inputs.log_pages.push(inputs.log_pages[0].clone()),
    ];
    for mutate in mutations {
        let mut changed = fixture.clone();
        mutate(&mut changed.inputs);
        assert!(changed.verify().is_err());
    }
}

#[test]
fn inline_scope_rejects_valid_pages_with_other_root_selector_or_nonzero_start() {
    let fixture = Fixture::new(2, Action::Commit);
    let other = Fixture::new(1, Action::Commit);
    let mut changed = fixture.clone();
    changed.inputs.index_pages = other.inputs.index_pages;
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    let abort = Fixture::new(2, Action::Abort);
    changed.inputs.index_pages = abort.inputs.index_pages;
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    let cursor = changed.inputs.index_pages[0].page.entries()[0].0.clone();
    let requirement = changed.requirement();
    changed.inputs.index_pages = vec![changed.store.index_page(
        TEST_RUN,
        requirement.index_selector,
        &requirement.index_root,
        Some(&cursor),
    )];
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    let requirement = changed.requirement();
    let remaining = changed.inputs.log_pages[0].entries()[1..].to_vec();
    changed.inputs.log_pages = vec![changed.store.log_page(
        TEST_RUN,
        requirement.log_selector,
        &requirement.log_root,
        1,
        remaining,
    )];
    assert!(changed.verify().is_err());
}

#[test]
fn inline_scope_requires_membership_and_order_to_name_the_same_set() {
    let mut fixture = Fixture::new(1, Action::Commit);
    let foreign = vec![identity("foreign-ordered-effect")];
    let selector = fixture.requirement().log_selector;
    let log_root = fixture.store.order(&selector, &foreign);
    let log = fixture
        .store
        .log_page(TEST_RUN, selector, &log_root, 0, foreign);
    let mut scope = fixture.scope.clone();
    scope.mutating_effect_order = log_root;
    fixture.replace_scope(scope);
    fixture.inputs.log_pages = vec![log];
    assert!(
        matches!(fixture.verify(), Err(CoreError::IdentityMismatch(ref message)) if message.contains("same complete pinned membership"))
    );
}

#[test]
fn inline_scope_rejects_missing_extra_absent_and_reowned_effect_leaves() {
    let fixture = Fixture::new(1, Action::Commit);
    let id = fixture
        .inputs
        .effects
        .first_key_value()
        .expect("Effect exists")
        .0
        .clone();
    let mut changed = fixture.clone();
    changed.inputs.effects.clear();
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    changed.inputs.effects.insert(id.clone(), None);
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    let extra = effect(&changed.scope, 2);
    changed
        .inputs
        .effects
        .insert(extra.intent_id.clone(), Some(extra));
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    changed
        .inputs
        .effects
        .get_mut(&id)
        .and_then(Option::as_mut)
        .expect("Effect exists")
        .scope_id = "scope:other".to_owned();
    assert!(changed.verify().is_err());
    let mut changed = fixture;
    let observational = changed
        .inputs
        .effects
        .get_mut(&id)
        .and_then(Option::as_mut)
        .expect("Effect exists");
    observational.profile.mutation = crate::MutationKind::Observational;
    assert!(changed.verify().is_err());
}

#[test]
fn inline_commit_requires_exact_obligation_absence_and_abort_requires_none() {
    let fixture = Fixture::new(1, Action::Commit);
    let mut changed = fixture.clone();
    changed.inputs.obligations.clear();
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    changed
        .inputs
        .obligations
        .insert(identity("extra-obligation"), None);
    assert!(changed.verify().is_err());
    let mut changed = fixture.clone();
    let current_effect = changed
        .inputs
        .effects
        .values()
        .next()
        .and_then(Option::as_ref)
        .expect("Effect exists");
    let obligation =
        crate::machine::obligation_for_effect(current_effect).expect("obligation derives");
    changed
        .inputs
        .obligations
        .insert(obligation.obligation_id.clone(), Some(obligation));
    assert!(changed.verify().is_err());
    let mut abort = Fixture::new(1, Action::Abort);
    abort.inputs.obligations = fixture.inputs.obligations;
    assert!(abort.verify().is_err());
}

#[test]
fn inline_closure_reduces_through_the_shared_paged_scope_laws() {
    for action in [Action::Commit, Action::Abort] {
        for count in [0, 1, MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES] {
            let fixture = Fixture::new(count, action);
            let frontier = fixture.frontier();
            let reads = MachineRunReadSet::prepare_with_inline(
                &frontier,
                &fixture.envelope,
                fixture.inputs.clone(),
                Some(fixture.requirement()),
            )
            .expect("private inline reads prepare");
            let events = build_pinned_events(
                &reads,
                &fixture.envelope,
                &canonical_digest(&fixture.envelope).expect("command hashes"),
            )
            .expect("generic command admits");
            let [event] = events.as_slice() else {
                panic!("Scope command admits exactly one Event")
            };
            let reduction =
                reduce_inline_scope(&reads, event, &frontier).expect("shared Scope law reduces");
            let current = reduction.result_current.expect("Run current returns");
            assert_eq!(current.last_event, event.event_id);
            assert_eq!(current.reducer_state, MachineRunReducerState::Ready);
            assert_eq!(current.indexes.open_scopes.entries, 0);
            let scope = &reduction.scopes[ROOT_SCOPE_ID];
            match action {
                Action::Commit => {
                    assert_eq!(scope.status, crate::ScopeStatus::ClosedCommitted);
                    assert_eq!(reduction.obligations.len(), count);
                    assert_eq!(
                        current.committed_effect_count,
                        u64::try_from(count).expect("count fits")
                    );
                }
                Action::Abort => {
                    assert_eq!(scope.status, crate::ScopeStatus::ClosedAborted);
                    assert!(reduction.obligations.is_empty());
                    assert_eq!(
                        current.world_settlement,
                        crate::WorldSettlementStatus::Settled
                    );
                    assert!(
                        reduction.effects.values().all(
                            |effect| effect.phase == crate::EffectPhase::CancelledBeforeRelease
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn inline_reducer_rejects_a_different_event_payload_and_unverified_reads() {
    let fixture = Fixture::new(1, Action::Commit);
    let frontier = fixture.frontier();
    let reads = MachineRunReadSet::prepare_with_inline(
        &frontier,
        &fixture.envelope,
        fixture.inputs.clone(),
        Some(fixture.requirement()),
    )
    .expect("inline reads prepare");
    let events = build_pinned_events(
        &reads,
        &fixture.envelope,
        &canonical_digest(&fixture.envelope).expect("command hashes"),
    )
    .expect("generic command admits");
    let admitted = events.first().expect("Event exists");
    let payload = EventPayload::ScopeAborted {
        scope_id: ROOT_SCOPE_ID.to_owned(),
    };
    let (reads_keys, writes, coordination_key) = footprints(TEST_RUN, &payload);
    let event = Event::new(EventContent {
        command_id: admitted.command_id.clone(),
        command_hash: admitted.command_hash.clone(),
        run_id: admitted.run_id.clone(),
        parents: admitted.parents.clone(),
        reads: reads_keys,
        writes,
        coordination_key,
        payload,
    })
    .expect("changed Event reauthenticates");
    assert!(
        matches!(reduce_inline_scope(&reads, &event, &frontier), Err(CoreError::IdentityMismatch(ref message)) if message.contains("admitted Event payload"))
    );
    let unverified = MachineRunReadSet {
        inputs: fixture.inputs,
        inline_scope: None,
    };
    assert!(reduce_inline_scope(&unverified, &event, &frontier).is_err());
}

#[test]
fn inline_scope_rejects_a_complete_authenticated_page_owned_by_another_run() {
    let mut fixture = Fixture::new(1, Action::Commit);
    let requirement = fixture.requirement();
    let ids = fixture.inputs.log_pages[0].entries().to_vec();
    let other_run = "run:other-inline-scope";
    let root = fixture.store.map(
        ids.iter()
            .map(|id| {
                (
                    id.clone(),
                    machine_index_membership_value_id(other_run, &requirement.index_selector, id)
                        .expect("other Run membership derives"),
                )
            })
            .collect(),
    );
    let page = fixture
        .store
        .index_page(other_run, requirement.index_selector, &root, None);
    let mut scope = fixture.scope.clone();
    scope.mutating_effects = root;
    fixture.replace_scope(scope);
    fixture.inputs.index_pages = vec![page];
    assert!(
        matches!(fixture.verify(), Err(CoreError::IdentityMismatch(ref message)) if message.contains("same complete pinned membership"))
    );
}

#[test]
fn raw_missing_proof_nodes_never_become_verified_scope_pages() {
    let mut fixture = Fixture::new(2, Action::Commit);
    let requirement = fixture.requirement();
    let proof = collections::prove_map_range(
        &requirement.index_root,
        None,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        collections::MAX_PAGE_BYTES,
        &mut fixture.store,
    )
    .expect("map proof derives");
    let mut wire = serde_json::to_value(proof).expect("raw proof serializes");
    wire["nodes"].as_array_mut().expect("node array").pop();
    let bytes = crate::canonical_bytes(&wire).expect("proof serializes");
    let truncated = collections::decode_map_range_proof(&bytes).expect("proof shape still decodes");
    assert!(
        MachineRunIndexPage::verify_proof(
            TEST_RUN.to_owned(),
            requirement.index_selector,
            &requirement.index_root,
            None,
            &truncated
        )
        .is_err()
    );
    let proof = collections::prove_log_range(
        &requirement.log_root,
        0,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        collections::MAX_PAGE_BYTES,
        &mut fixture.store,
    )
    .expect("log proof derives");
    let mut wire = serde_json::to_value(proof).expect("raw proof serializes");
    wire["nodes"].as_array_mut().expect("node array").pop();
    let bytes = crate::canonical_bytes(&wire).expect("proof serializes");
    let truncated = collections::decode_log_range_proof(&bytes).expect("proof shape still decodes");
    assert!(
        MachineRunLogPage::verify_proof(
            TEST_RUN.to_owned(),
            requirement.log_selector,
            &requirement.log_root,
            0,
            fixture.inputs.log_pages[0].entries().to_vec(),
            &truncated
        )
        .is_err()
    );
}

#[test]
fn inline_read_set_accepts_only_the_exact_structural_scope_keys() {
    let mut fixture = Fixture::new(1, Action::Commit);
    let requirement = fixture.requirement();
    let mut extra = root_scope();
    extra.scope_id = "scope:unrelated-child".to_owned();
    extra.parent_scope = Some(ROOT_SCOPE_ID.to_owned());
    extra.site_id = Some("unrelated".to_owned());
    extra.verify().expect("extra Scope is well formed");
    fixture
        .inputs
        .scopes
        .insert(extra.scope_id.clone(), Some(extra));
    let frontier = fixture.frontier();
    assert!(
        MachineRunReadSet::prepare_with_inline(
            &frontier,
            &fixture.envelope,
            fixture.inputs,
            Some(requirement)
        )
        .is_err()
    );
}

#[test]
fn maximum_inline_closure_retains_and_updates_only_its_exact_direct_parent() {
    for action in [Action::Commit, Action::Abort] {
        let fixture = Fixture::nested(MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES, action);
        let frontier = fixture.frontier();
        let reads = MachineRunReadSet::prepare_with_inline(
            &frontier,
            &fixture.envelope,
            fixture.inputs.clone(),
            Some(fixture.requirement()),
        )
        .expect("256 entries and two exact Scope leaves fit the private budget");
        let events = build_pinned_events(
            &reads,
            &fixture.envelope,
            &canonical_digest(&fixture.envelope).expect("command hashes"),
        )
        .expect("nested Scope command admits");
        let event = events.first().expect("Scope Event exists");
        let reduction =
            reduce_inline_scope(&reads, event, &frontier).expect("nested Scope reduces");
        assert_eq!(reduction.scopes.len(), 2);
        assert_eq!(
            reduction.scopes[ROOT_SCOPE_ID].status,
            crate::ScopeStatus::Open
        );
        assert_eq!(reduction.scopes[ROOT_SCOPE_ID].direct_open_child_count, 0);
        assert_ne!(
            reduction.scopes[&fixture.scope.scope_id].status,
            crate::ScopeStatus::Open
        );
        assert_eq!(
            reduction
                .result_current
                .expect("Run current returns")
                .indexes
                .open_scopes
                .entries,
            1
        );
        let mut missing_parent = fixture.inputs.clone();
        missing_parent.scopes.remove(ROOT_SCOPE_ID);
        assert!(
            MachineRunReadSet::prepare_with_inline(
                &frontier,
                &fixture.envelope,
                missing_parent,
                Some(fixture.requirement())
            )
            .is_err()
        );
    }
}

fn scope_batch_with_material(
    fixture: &Fixture,
) -> (PreparedPinnedCommandBatch, MachineMaterialAdmission) {
    let bytes = b"inline-scope-framework-material".to_vec();
    let artifact = ArtifactRecord {
        reference: crate::artifact_ref(crate::EXECUTION_BINDING_ARTIFACT_KIND, &bytes)
            .expect("framework material identity derives"),
        bytes,
    };
    let material = MachineMaterialAdmission::new(
        fixture.envelope.command_id.clone(),
        Vec::new(),
        vec![artifact.clone()],
    )
    .expect("framework material closes");
    let parent_reads = MachineMaterialParentReads::new(
        BTreeMap::new(),
        BTreeMap::from([(artifact.reference.artifact_id, None)]),
    );
    let command = MachinePinnedBatchCommand {
        command_id: fixture.envelope.command_id.clone(),
        actor: fixture.envelope.actor.clone(),
        run_id: fixture.envelope.run_id.clone(),
        precondition: MachinePinnedBatchPrecondition::Parent(
            fixture.envelope.expected_precondition.clone(),
        ),
        command: fixture.envelope.command.clone(),
    };
    let batch = prepare_pinned_command_batch(
        &fixture.frontier(),
        vec![command],
        Some((material.clone(), parent_reads)),
    )
    .expect("sole Scope batch and complete material freeze");
    (batch, material)
}

fn fluent_scope_read(
    batch: &mut PreparedPinnedCommandBatch,
    fixture: &Fixture,
) -> PreparedPinnedReadCommand {
    let proof = MachinePinnedCommandProof::vacant(
        MachineCommandIndexProof::empty_nonmembership(&fixture.envelope.command_id)
            .expect("command absence derives"),
    );
    let PinnedMachineCommandPreparation::Lookup(lookup) = batch
        .prepare_next(&proof)
        .expect("batch prepares its own next command")
    else {
        panic!("fresh Scope command must resolve its Run");
    };
    let PinnedMachineRunPreparation::Reads(read) = lookup
        .resolve_run(MachinePinnedRunLookup::new(
            fixture.inputs.machine_revision.clone(),
            fixture.inputs.run_id.clone(),
            fixture.inputs.runs_root.clone(),
            fixture.inputs.run.clone(),
        ))
        .expect("exact Run resolves through the fluent chain")
    else {
        panic!("current Scope command must require semantic reads");
    };
    *read
}

// This test adapter binds only the typed cardinality/digest receipts. It does
// not replace the real source map/log proofs or implement any semantic reducer.
fn bind_test_root(plan: &MachinePreparedRootMutation) -> MachineRunRootUpdate {
    let count = plan.expected_count();
    let node = (count != 0)
        .then(|| {
            content_id(
                "test.inline-scope-physical-root/1",
                &(plan.target(), plan.mutation_digest(), count),
            )
        })
        .transpose()
        .expect("test physical result derives");
    let root = match plan.parent() {
        MachinePhysicalRoot::Map(_) => MachinePhysicalRoot::Map(MachineMapRoot {
            node,
            entries: count,
        }),
        MachinePhysicalRoot::Log(_) => {
            let ordered_root = node
                .clone()
                .unwrap_or_else(|| MachineLogRoot::empty().ordered_root);
            MachinePhysicalRoot::Log(MachineLogRoot {
                node,
                len: count,
                height: u8::from(count != 0),
                ordered_root,
            })
        }
    };
    plan.bind_result(root)
}

fn finish_fluent_scope_roots(prepared: PreparedPinnedMachineTransition) -> PinnedMachineTransition {
    let updates = prepared
        .scope_root_mutations()
        .expect("Scope root plans derive")
        .iter()
        .map(bind_test_root)
        .collect();
    let run = prepared
        .finish_scope_roots(updates)
        .expect("Scope roots bind");
    let updates = run
        .run_root_mutations()
        .expect("Run root plans derive")
        .iter()
        .map(bind_test_root)
        .collect();
    let global = run.finish_run_roots(updates).expect("Run roots bind");
    let updates = global
        .global_root_mutations()
        .expect("global root plans derive")
        .iter()
        .map(bind_test_root)
        .collect();
    global.finish(updates).expect("global roots bind")
}

fn assert_scope_material_admitted_once(
    parent: &MachineAuthorityFrontier,
    batch_id: &str,
    material: &MachineMaterialAdmission,
    result: &PinnedMachineBatchTransition,
) {
    let reference = &material.artifacts()[0].reference;
    assert_eq!(result.batch.batch_id, batch_id);
    assert_eq!(result.batch.parent_authority_root, parent.authority_root);
    assert_eq!(
        result.batch.admission_parent_authority_root,
        parent.authority_root
    );
    assert_eq!(
        result.batch.material_digest.as_deref(),
        Some(material.material_digest())
    );
    let source = result
        .batch
        .material_source
        .as_ref()
        .expect("complete source remains bound");
    assert_eq!(source.source_command_id, material.source_command_id());
    assert!(source.plan_ids.is_empty());
    assert_eq!(source.artifacts, vec![reference.clone()]);
    assert_eq!(result.frontier.batch_count, parent.batch_count + 1);
    assert_eq!(result.frontier.artifact_count, parent.artifact_count + 1);
    assert_eq!(
        result.frontier.batch_admission_commitment,
        append_material_commitment(
            MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN,
            &parent.batch_admission_commitment,
            batch_id,
        )
        .expect("one batch append derives")
    );
    assert_eq!(
        result.frontier.artifact_admission_commitment,
        append_material_commitment(
            MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN,
            &parent.artifact_admission_commitment,
            &reference.artifact_id,
        )
        .expect("one material append derives")
    );
    assert_eq!(result.machine.parent_authority_root, parent.authority_root);
    assert_eq!(
        result.machine.result_authority_root,
        result.frontier.authority_root
    );
    assert_eq!(result.machine.artifacts.len(), 1);
    assert_eq!(
        result.machine.artifact_admission_order,
        vec![reference.artifact_id.clone()]
    );
    assert_eq!(
        result.machine.artifacts[&reference.artifact_id],
        material.artifacts()[0]
    );
    assert_eq!(result.machine.batches.len(), 1);
    assert_eq!(
        result.machine.batch_admission_order,
        vec![batch_id.to_owned()]
    );
    assert_eq!(result.steps.len(), 1);
    assert!(result.steps[0].machine.artifacts.is_empty());
    assert!(result.steps[0].machine.batches.is_empty());
    assert_eq!(result.frontier.pending_commands, parent.pending_commands);
    assert_eq!(result.frontier.paged_transitions, parent.paged_transitions);
    result.batch.verify().expect("terminal batch verifies");
    result
        .frontier
        .verify()
        .expect("terminal frontier verifies");
}

#[test]
fn sole_scope_with_material_finishes_the_public_inline_batch_chain_once() {
    for action in [Action::Commit, Action::Abort] {
        let fixture = Fixture::new(1, action);
        let parent = fixture.frontier();
        let (mut batch, material) = scope_batch_with_material(&fixture);
        let batch_id = batch.batch_id().to_owned();
        assert_eq!(batch.current_frontier(), &parent);
        assert!(batch.material_delta().is_none());
        let read = fluent_scope_read(&mut batch, &fixture);
        assert!(
            read.inline_scope_read_requirement(&fixture.scope)
                .expect("exact Scope resolves")
                .is_some()
        );
        assert_eq!(batch.current_frontier().batch_count, parent.batch_count + 1);
        assert_eq!(
            batch.current_frontier().artifact_count,
            parent.artifact_count + 1
        );
        assert!(
            batch.material_delta().is_none(),
            "provisional material is not exposed as an independent write"
        );
        let PinnedMachineFreshPreparation::Prepared(prepared) = read
            .prepare(fixture.inputs)
            .expect("sole bounded Scope prepares")
        else {
            panic!("bounded sole Scope with material must not create PagedBegin");
        };
        let transition = finish_fluent_scope_roots(*prepared);
        let result = batch
            .accept_step(transition)
            .expect("exact step is accepted")
            .finish()
            .expect("one original batch finishes");
        assert_scope_material_admitted_once(&parent, &batch_id, &material, &result);
        let run = result.steps[0]
            .run
            .as_ref()
            .expect("Scope step has a Run result");
        assert_eq!(
            run.result_current.reducer_state,
            MachineRunReducerState::Ready
        );
        assert_eq!(
            run.scopes[ROOT_SCOPE_ID].status,
            match action {
                Action::Commit => crate::ScopeStatus::ClosedCommitted,
                Action::Abort => crate::ScopeStatus::ClosedAborted,
            }
        );
    }
}

fn assert_paged_scope_parent_is_unadmitted(
    begin: &PinnedMachinePagedBegin,
    parent: &MachineAuthorityFrontier,
    provisional: &MachineAuthorityFrontier,
    batch_id: &str,
    material: &MachineMaterialAdmission,
) {
    assert_eq!(begin.frontier.authority_root, parent.authority_root);
    assert_ne!(begin.frontier.authority_root, provisional.authority_root);
    assert_eq!(begin.frontier.plan_count, parent.plan_count);
    assert_eq!(begin.frontier.artifact_count, parent.artifact_count);
    assert_eq!(
        begin.frontier.artifact_admission_commitment,
        parent.artifact_admission_commitment
    );
    assert_eq!(begin.frontier.batch_count, parent.batch_count);
    assert_eq!(
        begin.frontier.batch_admission_commitment,
        parent.batch_admission_commitment
    );
    assert_eq!(begin.frontier.event_count, parent.event_count);
    assert_eq!(begin.frontier.admission_sequence, parent.admission_sequence);
    assert_eq!(begin.frontier.admission_head, parent.admission_head);
    assert_eq!(
        begin.frontier.pending_commands.entries,
        parent.pending_commands.entries + 1
    );
    assert_eq!(
        begin.frontier.paged_transitions.entries,
        parent.paged_transitions.entries + 1
    );
    let manifest = &begin.transition.batch_manifest;
    assert_eq!(manifest.batch_id, batch_id);
    assert_eq!(manifest.parent_authority_root, parent.authority_root);
    assert_eq!(
        manifest.material_digest.as_deref(),
        Some(material.material_digest())
    );
    let source = manifest
        .material_source
        .as_ref()
        .expect("original proposal remains staged");
    assert_eq!(source.source_command_id, material.source_command_id());
    assert_eq!(
        source.artifacts,
        vec![material.artifacts()[0].reference.clone()]
    );
    assert_eq!(begin.transition.staged_material.plans.entries, 0);
    assert_eq!(begin.transition.staged_material.artifacts.entries, 1);
    assert_eq!(begin.transition.effect_source.len, 257);
    assert_eq!(begin.transition.processed_count, 0);
    assert_eq!(begin.transition.phase, MachinePagedTransitionPhase::Effects);
    assert!(
        matches!(&begin.fenced_run.reducer_state, MachineRunReducerState::Transitioning { transition_id } if transition_id == &begin.transition.transition_id)
    );
    begin
        .transition
        .verify()
        .expect("original staged transition verifies");
}

#[test]
fn sole_wide_scope_discards_provisional_admission_before_staging_original_paged_batch() {
    for action in [Action::Commit, Action::Abort] {
        let fixture = Fixture::new(MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES + 1, action);
        let parent = fixture.frontier();
        let (mut batch, material) = scope_batch_with_material(&fixture);
        let batch_id = batch.batch_id().to_owned();
        let read = fluent_scope_read(&mut batch, &fixture);
        let provisional = batch.current_frontier().clone();
        assert_eq!(provisional.batch_count, parent.batch_count + 1);
        assert_eq!(provisional.artifact_count, parent.artifact_count + 1);
        assert!(batch.material_delta().is_none());
        assert!(
            read.inline_scope_read_requirement(&fixture.scope)
                .expect("wide sole Scope selects paging")
                .is_none()
        );
        let mut inputs = fixture.inputs;
        inputs.effects.clear();
        inputs.obligations.clear();
        inputs.index_pages.clear();
        inputs.log_pages.clear();
        let PinnedMachineFreshPreparation::PagedBegin(begin) =
            read.prepare(inputs).expect("wide Scope begin prepares")
        else {
            panic!("257 effects require the persisted paging protocol");
        };
        let staged = batch
            .into_paged_begin(*begin)
            .expect("original batch takes over the provisional context");
        let plans = staged
            .root_mutations()
            .expect("private material staging plans derive");
        assert_eq!(plans.len(), 2);
        assert!(plans.iter().all(|plan| matches!(
            plan.target(),
            MachineRunRootUpdateTarget::PagedMaterialPlans
                | MachineRunRootUpdateTarget::PagedMaterialArtifacts
        )));
        let updates = plans.iter().map(bind_test_root).collect();
        let begin = staged
            .finish(updates)
            .expect("material stages under the original manifest");
        let plans = begin
            .root_mutations()
            .expect("original reservation plans derive");
        assert_eq!(plans.len(), 3);
        let updates = plans.iter().map(bind_test_root).collect();
        let reservation = begin
            .finish(updates)
            .expect("one original-parent reservation finishes");
        assert_paged_scope_parent_is_unadmitted(
            &reservation,
            &parent,
            &provisional,
            &batch_id,
            &material,
        );
    }
}
