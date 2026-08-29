fn fixture_root_scope(fixture: &StartedFixture) -> MachineScopeCurrent {
    fixture.start.steps[0]
        .run
        .as_ref()
        .expect("start has Run delta")
        .scopes[ROOT_SCOPE_ID]
        .clone()
}

fn material_parent_reads(
    material: &MachineMaterialAdmission,
    retained: bool,
) -> MachineMaterialParentReads {
    MachineMaterialParentReads::new(
        material
            .plans()
            .iter()
            .map(|plan| (plan.plan_id.clone(), retained.then(|| plan.clone())))
            .collect(),
        material
            .artifacts()
            .iter()
            .map(|artifact| {
                (
                    artifact.reference.artifact_id.clone(),
                    retained.then(|| artifact.clone()),
                )
            })
            .collect(),
    )
}

fn begin_scope_batch(
    fixture: &StartedFixture,
    scope: &MachineScopeCurrent,
    command_id: &str,
    material: Option<MachineMaterialAdmission>,
) -> PinnedMachinePagedBegin {
    let command = MachinePinnedBatchCommand {
        command_id: command_id.to_owned(),
        actor: "actor:pinned-test".to_owned(),
        run_id: fixture.current.run_id.clone(),
        precondition: MachinePinnedBatchPrecondition::Parent(Some(
            fixture.current.precondition_token(),
        )),
        command: Command::CommitScope {
            scope_id: scope.scope_id.clone(),
        },
    };
    let material_reads = material
        .as_ref()
        .map(|value| material_parent_reads(value, false));
    let batch = prepare_pinned_command_batch(
        &fixture.frontier,
        vec![command],
        material.zip(material_reads),
    )
    .expect("paged batch freezes");
    assert_eq!(batch.current_frontier(), &fixture.frontier);
    assert!(batch.material_delta().is_none());
    let envelope = batch.next_envelope().expect("paged envelope derives");
    let proof = MachinePinnedCommandProof::vacant(
        MachineCommandIndexProof::empty_nonmembership(command_id).expect("command absence derives"),
    );
    let PinnedMachineCommandPreparation::Lookup(lookup) =
        prepare_pinned_command(batch.current_frontier(), &proof, envelope)
            .expect("paged command lookup prepares")
    else {
        panic!("fresh command needs lookup")
    };
    let PinnedMachineRunPreparation::Reads(read) = lookup
        .resolve_run(MachinePinnedRunLookup::new(
            revision(command_id),
            fixture.current.run_id.clone(),
            fixture.frontier.runs.clone(),
            Some(fixture.current.clone()),
        ))
        .expect("paged source Run resolves")
    else {
        panic!("fresh command needs reads")
    };
    let mut inputs = fact_inputs(fixture, command_id, "unused");
    inputs.facts.clear();
    inputs
        .scopes
        .insert(scope.scope_id.clone(), Some(scope.clone()));
    let PinnedMachineFreshPreparation::PagedBegin(begin) =
        read.prepare(inputs).expect("paged begin prepares")
    else {
        panic!("scope command must use paged protocol")
    };
    let staged = batch
        .into_paged_begin(*begin)
        .expect("original batch binds begin");
    let updates = staged
        .root_mutations()
        .expect("material roots derive")
        .iter()
        .map(fake_store_result)
        .collect();
    let begin = staged.finish(updates).expect("material roots bind");
    let updates = begin
        .root_mutations()
        .expect("reservation roots derive")
        .iter()
        .map(fake_store_result)
        .collect();
    begin.finish(updates).expect("reservation commits")
}

fn reopened_transition(
    transition: &MachinePagedTransitionCurrent,
) -> MachinePagedTransitionCurrent {
    let bytes = crate::canonical_bytes(transition).expect("transition encodes");
    let reopened: MachinePagedTransitionCurrent =
        crate::decode_json(&bytes).expect("transition reopens");
    reopened.verify().expect("reopened transition verifies");
    assert_eq!(&reopened, transition);
    reopened
}

fn finish_scope_batch(
    begin: &PinnedMachinePagedBegin,
    scope: MachineScopeCurrent,
    frontier: &MachineAuthorityFrontier,
    material: Option<(MachineMaterialAdmission, MachineMaterialParentReads)>,
) -> PinnedMachineBatchTransition {
    let transition = reopened_transition(&begin.transition);
    let final_inputs = MachinePagedFinalizeInputs::new(
        begin.fenced_run.clone(),
        BTreeMap::from([(scope.scope_id.clone(), scope)]),
        None,
        MachineCommandIndexProof::empty_nonmembership(&transition.command_id)
            .expect("final absence proof"),
        material,
    );
    let prepared = prepare_pinned_transition_final(frontier, &transition, final_inputs)
        .expect("paged final prepares");
    let updates = prepared
        .shadow_root_mutations()
        .expect("final shadow roots derive")
        .iter()
        .map(fake_store_result)
        .collect();
    let publish = prepared
        .finish_shadow_roots(updates)
        .expect("final shadow roots bind");
    let updates = publish
        .root_mutations()
        .expect("final publish roots derive")
        .iter()
        .map(fake_store_result)
        .collect();
    publish.finish(updates).expect("final batch publishes")
}

fn delta_entries(delta: &MachineRootDelta) -> Vec<MachineCommandArchiveEntry> {
    delta
        .admissions
        .iter()
        .map(|admission| MachineCommandArchiveEntry {
            admission: admission.clone(),
            command: delta.commands[&admission.command_id].clone(),
            events: delta
                .events
                .iter()
                .filter(|event| event.command_id == admission.command_id)
                .cloned()
                .collect(),
        })
        .collect()
}

#[test]
fn paged_scope_reopen_preserves_manifest_material_and_one_terminal_receipt() {
    let fixture = start_fixture();
    let scope = fixture_root_scope(&fixture);
    scope
        .verify()
        .expect("generated Scope verifies its raw digests");
    let artifact = binding_bytes(vec![42; 4096]);
    let material = MachineMaterialAdmission::new(
        "profile:agent-observation".to_owned(),
        Vec::new(),
        vec![artifact.clone()],
    )
    .expect("paged material derives");
    let begin = begin_scope_batch(
        &fixture,
        &scope,
        "command:paged-material",
        Some(material.clone()),
    );
    assert_eq!(
        begin.frontier.authority_root,
        fixture.frontier.authority_root
    );
    assert_eq!(
        begin.frontier.artifact_count,
        fixture.frontier.artifact_count
    );
    assert_eq!(begin.frontier.batch_count, fixture.frontier.batch_count);
    assert_eq!(begin.transition.staged_material.artifacts.entries, 1);
    assert_eq!(
        begin.transition.phase,
        MachinePagedTransitionPhase::Finalize
    );
    let transition = reopened_transition(&begin.transition);
    let requests = vec![MachinePinnedBatchCommand {
        command_id: transition.command_id.clone(),
        actor: transition.envelope.actor.clone(),
        run_id: transition.run_id.clone(),
        precondition: MachinePinnedBatchPrecondition::Parent(
            transition.envelope.expected_precondition.clone(),
        ),
        command: transition.envelope.command.clone(),
    }];
    transition
        .verify_batch_replay(&requests, Some(material.material_digest()))
        .expect("exact pending replay");
    assert!(transition.verify_batch_replay(&requests, None).is_err());
    let mut changed = requests.clone();
    changed[0].actor = "actor:other".to_owned();
    assert!(
        transition
            .verify_batch_replay(&changed, Some(material.material_digest()))
            .is_err()
    );
    let result = finish_scope_batch(
        &begin,
        scope,
        &begin.frontier,
        Some((material.clone(), material_parent_reads(&material, false))),
    );
    assert_eq!(result.batch.batch_id, transition.batch_manifest.batch_id);
    assert_eq!(
        result.batch.parent_authority_root,
        fixture.frontier.authority_root
    );
    assert_eq!(
        result.frontier.batch_count,
        fixture.frontier.batch_count + 1
    );
    assert_eq!(
        result.frontier.artifact_count,
        fixture.frontier.artifact_count + 1
    );
    assert_eq!(result.machine.events.len(), 1);
    assert_eq!(result.machine.admissions.len(), 1);
    assert_eq!(result.machine.batches.len(), 1);
    assert!(
        result.steps[0]
            .run
            .as_ref()
            .expect("final Run")
            .result_current
            .active_attempt_id
            .is_some()
    );
    verify_scope_batch_replay(&fixture, &result, &requests, &material);
}

fn verify_scope_batch_replay(
    fixture: &StartedFixture,
    result: &PinnedMachineBatchTransition,
    requests: &[MachinePinnedBatchCommand],
    material: &MachineMaterialAdmission,
) {
    let records = result
        .machine
        .commands
        .values()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        verify_pinned_command_batch_replay(
            &result.batch,
            requests,
            &records,
            Some(material.material_digest())
        )
        .expect("terminal batch replay"),
        result.batch.receipts
    );
    result
        .batch
        .verify_entry(&delta_entries(&result.machine)[0])
        .expect("terminal archive entry closes");
    let mut artifacts = fixture.start.machine.artifacts.clone();
    artifacts.extend(result.machine.artifacts.clone());
    let mut entries = delta_entries(&fixture.start.machine);
    entries.extend(delta_entries(&result.machine));
    let projection = Machine::replay(
        fixture.start.machine.plans.values().cloned(),
        artifacts.into_values(),
        vec![fixture.start.batch.clone(), result.batch.clone()],
        entries,
    )
    .expect("complete paged batch replays cold");
    assert_eq!(
        projection.runs[&fixture.current.run_id].scopes[ROOT_SCOPE_ID].status,
        crate::ScopeStatus::ClosedCommitted
    );
}

#[test]
fn paged_manifest_and_staging_tampering_fail_before_final_admission() {
    let fixture = start_fixture();
    let scope = fixture_root_scope(&fixture);
    let material = MachineMaterialAdmission::new(
        "command:paged-negative".to_owned(),
        Vec::new(),
        vec![binding("negative")],
    )
    .expect("material derives");
    let begin = begin_scope_batch(
        &fixture,
        &scope,
        material.source_command_id(),
        Some(material.clone()),
    );
    let wire = serde_json::to_value(&begin.transition).expect("transition encodes");
    for field in ["batch_manifest", "staged_material"] {
        let mut missing = wire.clone();
        missing.as_object_mut().expect("object").remove(field);
        assert!(serde_json::from_value::<MachinePagedTransitionCurrent>(missing).is_err());
    }
    let mut wrong = begin.transition.clone();
    wrong.batch_manifest.member.intent_hash = "1".repeat(64);
    assert!(wrong.verify().is_err());
    let mut wrong = begin.transition.clone();
    wrong.staged_material = MachinePagedMaterialRoots::empty();
    assert!(wrong.verify().is_err());
    let mut wrong = begin.transition.clone();
    wrong.next_index = 1;
    assert!(wrong.verify().is_err());
    let inputs = MachinePagedFinalizeInputs::new(
        begin.fenced_run.clone(),
        BTreeMap::from([(scope.scope_id.clone(), scope)]),
        None,
        MachineCommandIndexProof::empty_nonmembership(&begin.transition.command_id)
            .expect("absence proof"),
        None,
    );
    assert!(prepare_pinned_transition_final(&begin.frontier, &begin.transition, inputs).is_err());
}

#[test]
fn material_only_batches_close_without_core_commands_and_replay_exactly() {
    let fixture = start_fixture();
    let material = MachineMaterialAdmission::new(
        "profile:material-only".to_owned(),
        Vec::new(),
        vec![binding("profile-result")],
    )
    .expect("profile material derives");
    let prepared = prepare_machine_material_admission(
        &fixture.frontier,
        &material,
        &material_parent_reads(&material, false),
    )
    .expect("material-only batch prepares");
    assert!(prepared.delta.events.is_empty());
    assert!(prepared.delta.admissions.is_empty());
    assert!(prepared.delta.commands.is_empty());
    assert_eq!(prepared.delta.batches.len(), 1);
    assert_eq!(
        prepared.frontier.batch_count,
        fixture.frontier.batch_count + 1
    );
    let batch = prepared
        .delta
        .batches
        .values()
        .next()
        .expect("material batch exists")
        .clone();
    batch
        .verify()
        .expect("zero-command material batch verifies");
    let mut artifacts = fixture.start.machine.artifacts.clone();
    artifacts.extend(prepared.delta.artifacts.clone());
    let entries = delta_entries(&fixture.start.machine);
    let projection = Machine::replay(
        fixture.start.machine.plans.into_values(),
        artifacts.into_values(),
        vec![batch, fixture.start.batch],
        entries,
    )
    .expect("material batch order follows authority roots");
    assert!(projection.runs.contains_key(&fixture.current.run_id));
}

#[test]
fn paged_final_rechecks_material_membership_after_a_concurrent_admission() {
    let fixture = start_fixture();
    let scope = fixture_root_scope(&fixture);
    let artifact = binding("shared-material");
    let proposal = MachineMaterialAdmission::new(
        "profile:original-observation".to_owned(),
        Vec::new(),
        vec![artifact.clone()],
    )
    .expect("paged material derives");
    let begin = begin_scope_batch(
        &fixture,
        &scope,
        "command:paged-dedup",
        Some(proposal.clone()),
    );
    let concurrent = MachineMaterialAdmission::new(
        "profile:concurrent-material".to_owned(),
        Vec::new(),
        vec![artifact],
    )
    .expect("concurrent material derives");
    let admitted = prepare_machine_material_admission(
        &begin.frontier,
        &concurrent,
        &material_parent_reads(&concurrent, false),
    )
    .expect("unrelated material admits while Run is paged");
    let result = finish_scope_batch(
        &begin,
        scope,
        &admitted.frontier,
        Some((proposal.clone(), material_parent_reads(&proposal, true))),
    );
    assert!(result.machine.artifacts.is_empty());
    assert_eq!(
        result.frontier.artifact_count,
        admitted.frontier.artifact_count
    );
    assert_eq!(
        result.batch.parent_authority_root,
        fixture.frontier.authority_root
    );
    assert_eq!(
        result.batch.admission_parent_authority_root,
        admitted.frontier.authority_root
    );
    assert_eq!(
        result.batch.batch_id,
        begin.transition.batch_manifest.batch_id
    );
    let mut artifacts = fixture.start.machine.artifacts.clone();
    artifacts.extend(admitted.delta.artifacts);
    let mut batches = vec![fixture.start.batch.clone()];
    batches.extend(admitted.delta.batches.into_values());
    batches.push(result.batch);
    let mut entries = delta_entries(&fixture.start.machine);
    entries.extend(delta_entries(&result.machine));
    Machine::replay(
        fixture.start.machine.plans.into_values(),
        artifacts.into_values(),
        batches,
        entries,
    )
    .expect("interleaved source and admission parents replay exactly");
}

fn projected_effect(scope: &MachineScopeCurrent, index: usize) -> crate::EffectProjection {
    crate::EffectProjection {
        intent_id: revision(&format!("projected-effect-{index}")),
        origin_plan_id: revision("projected-plan"),
        scope_id: scope.scope_id.clone(),
        invocation_id: scope.invocation_id.clone(),
        invocation_path: Vec::new(),
        definition_id: "main".to_owned(),
        region_path: Vec::new(),
        site_id: format!("effect_{index}"),
        occurrence: format!("occurrence:{index}"),
        effect_schema_version: crate::EFFECT_SCHEMA_VERSION.to_owned(),
        operation: "effect.test".to_owned(),
        profile: crate::EffectProfile {
            mutation: crate::MutationKind::Mutating,
            dispatch: crate::DispatchPolicy::OnScopeCommit,
            reconciliation: crate::ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        },
        args: crate::artifact_ref(crate::EFFECT_ARGS_ARTIFACT_KIND, b"{}").expect("args ref"),
        execution_binding: binding("projected").reference,
        occurrence_binding: revision("projected-binding"),
        execution_availability: crate::EffectExecutionAvailability::Available,
        phase: crate::EffectPhase::Prepared,
        outcome: crate::WorldOutcome::Unobserved,
        reconciliation: crate::ReconciliationState::NotRequired,
    }
}

#[derive(Default)]
struct PagedLogResolver(BTreeMap<String, cymule_authenticated_collections::LogNode>);

impl cymule_authenticated_collections::CollectionResolver for PagedLogResolver {
    fn load_map_node(
        &mut self,
        _: &str,
    ) -> cymule_authenticated_collections::Result<Option<cymule_authenticated_collections::MapNode>>
    {
        Ok(None)
    }
    fn load_log_node(
        &mut self,
        id: &str,
    ) -> cymule_authenticated_collections::Result<Option<cymule_authenticated_collections::LogNode>>
    {
        Ok(self.0.get(id).cloned())
    }
}

fn paged_effect_source(
    fixture: &mut StartedFixture,
    scope: &mut MachineScopeCurrent,
    count: usize,
) -> (Vec<crate::EffectProjection>, PagedLogResolver) {
    let effects = (0..count)
        .map(|index| projected_effect(scope, index))
        .collect::<Vec<_>>();
    let selector = MachineRunLogSelector::ScopeMutatingEffects {
        scope_id: scope.scope_id.clone(),
    };
    let values = effects
        .iter()
        .map(|effect| {
            machine_order_entry_value_id(&fixture.current.run_id, &selector, &effect.intent_id)
                .expect("typed log value")
        })
        .collect::<Vec<_>>();
    let log = cymule_authenticated_collections::build_log(&values).expect("source log builds");
    let count = u64::try_from(count).expect("test count fits");
    let root = MachineMapRoot {
        node: Some(revision("projected-effect-map")),
        entries: count,
    };
    scope.effects = root.clone();
    scope.mutating_effects = root.clone();
    scope.abort_transitions = root.clone();
    scope.effect_order = log.root().clone();
    scope.mutating_effect_order = log.root().clone();
    scope.effect_count = count;
    fixture.current.children.effects = root.clone();
    fixture.current.order.effects = log.root().clone();
    fixture.current.indexes.pending_effects = root.clone();
    fixture.current.indexes.terminal_transition_effects = root;
    fixture.current.world_settlement = crate::WorldSettlementStatus::Pending;
    scope.verify().expect("projected Scope shape");
    fixture.current.verify().expect("projected Run shape");
    (
        effects,
        PagedLogResolver(
            log.objects()
                .iter()
                .map(|node| (node.object_id.clone(), node.clone()))
                .collect(),
        ),
    )
}

fn next_effect_page(
    begin: &PinnedMachinePagedBegin,
    scope: &MachineScopeCurrent,
    effects: &[crate::EffectProjection],
    resolver: &mut PagedLogResolver,
) -> MachinePagedReadInputs {
    let transition = &begin.transition;
    let start = usize::try_from(transition.next_index).expect("page index fits");
    let end = (start + MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES).min(effects.len());
    let selected = &effects[start..end];
    let proof = cymule_authenticated_collections::prove_log_range(
        &transition.effect_source,
        transition.next_index,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES,
        cymule_authenticated_collections::MAX_PAGE_BYTES,
        resolver,
    )
    .expect("source range proof");
    let selector = pinned_paged_log_selector(transition).expect("paged selector");
    let page = MachineRunLogPage::verify_proof(
        transition.run_id.clone(),
        selector,
        &transition.effect_source,
        transition.next_index,
        selected
            .iter()
            .map(|effect| effect.intent_id.clone())
            .collect(),
        &proof,
    )
    .expect("typed source page verifies");
    MachinePagedReadInputs::new(
        begin.fenced_run.clone(),
        page,
        BTreeMap::from([(scope.scope_id.clone(), scope.clone())]),
        selected
            .iter()
            .map(|effect| (effect.intent_id.clone(), effect.clone()))
            .collect(),
        selected
            .iter()
            .map(|effect| {
                (
                    effect_obligation_id(&effect.intent_id).expect("obligation id"),
                    None,
                )
            })
            .collect(),
    )
}

#[test]
fn paged_effect_pages_reopen_without_changing_batch_or_exposing_partial_results() {
    let mut fixture = start_fixture();
    let mut scope = fixture_root_scope(&fixture);
    let (effects, mut resolver) = paged_effect_source(
        &mut fixture,
        &mut scope,
        MAX_PINNED_MACHINE_INDEX_PAGE_ENTRIES + 1,
    );
    let mut begin = begin_scope_batch(&fixture, &scope, "command:paged-effects", None);
    let original = begin.transition.batch_manifest.clone();
    let mut previous_inputs = None;
    let mut pages = 0;
    while begin.transition.phase != MachinePagedTransitionPhase::Finalize {
        begin.transition = reopened_transition(&begin.transition);
        if let Some(stale) = &previous_inputs {
            assert!(
                prepare_pinned_transition_page(&begin.frontier, &begin.transition, stale).is_err()
            );
        }
        let inputs = next_effect_page(&begin, &scope, &effects, &mut resolver);
        let prepared = prepare_pinned_transition_page(&begin.frontier, &begin.transition, &inputs)
            .expect("page prepares");
        let updates = prepared
            .shadow_root_mutations()
            .expect("page shadow roots")
            .iter()
            .map(fake_store_result)
            .collect();
        let progress = prepared
            .finish_shadow_roots(updates)
            .expect("page shadow roots bind");
        let update = fake_store_result(progress.transition_root_mutation().expect("cursor root"));
        let progress = progress.finish(update).expect("one page persists");
        begin.frontier = progress.frontier;
        begin.transition = progress.transition;
        assert_eq!(begin.transition.batch_manifest, original);
        assert_eq!(
            begin.frontier.authority_root,
            fixture.frontier.authority_root
        );
        assert_eq!(begin.fenced_run.children.obligations.entries, 0);
        previous_inputs = Some(inputs);
        pages += 1;
    }
    assert_eq!(pages, 2);
    let result = finish_scope_batch(&begin, scope, &begin.frontier, None);
    assert_eq!(result.batch.batch_id, original.batch_id);
    assert_eq!(result.machine.events.len(), 1);
    let run = &result.steps[0]
        .run
        .as_ref()
        .expect("final Run")
        .result_current;
    assert_eq!(run.children.obligations.entries, 257);
    assert_eq!(run.committed_effect_count, 257);
}

#[test]
fn pinned_frame_accepts_definition_root_inherited_scope_and_rejects_changed_witness() {
    let fixture = start_fixture();
    let scope = fixture_root_scope(&fixture);
    let plan = fixture
        .start
        .machine
        .plans
        .values()
        .next()
        .expect("Plan exists");
    let location = crate::ExecutionFrameLocation {
        run_id: &fixture.current.run_id,
        plan_id: &plan.plan_id,
        invocation_id: &scope.invocation_id,
        invocation_path: &[],
        definition_id: "main",
        region_path: &[],
        scope_id: ROOT_SCOPE_ID,
        next_step: 0,
    };
    validate_pinned_execution_frame(plan, &location, &scope, &[], &[])
        .expect("root frame uses canonical digest witnesses");
    assert!(validate_pinned_execution_frame(plan, &location, &scope, &[], &[1]).is_err());
    let mut closed = scope.clone();
    closed.status = crate::ScopeStatus::ClosedCommitted;
    validate_pinned_execution_frame(plan, &location, &closed, &[], &[])
        .expect("closed terminal frame remains structurally inspectable");
}

#[test]
fn material_byte_budget_counts_legal_leaves_and_parent_reads_together() {
    let artifacts = (0_u8..6)
        .map(|value| binding_bytes(vec![value; crate::MAX_ARTIFACT_BYTES]))
        .collect::<Vec<_>>();
    for artifact in &artifacts {
        artifact
            .validate()
            .expect("each maximum-size Artifact is legal");
    }
    let material = MachineMaterialAdmission::new(
        "profile:bounded-material".to_owned(),
        Vec::new(),
        artifacts[..5].to_vec(),
    )
    .expect("five legal maximum Artifacts fit together");
    assert!(
        matches!(MachineMaterialAdmission::new("profile:oversized-material".to_owned(), Vec::new(), artifacts),
        Err(CoreError::Validation(message)) if message.contains("bytes"))
    );
    let frontier =
        MachineAuthorityFrontier::genesis(empty_map(), empty_map(), empty_map(), empty_map())
            .expect("empty frontier");
    let frontier = prepare_machine_material_admission(
        &frontier,
        &material,
        &material_parent_reads(&material, false),
    )
    .expect("legal material admits against proven absence")
    .frontier;
    let reads = material_parent_reads(&material, true);
    assert!(
        matches!(prepare_machine_material_admission(&frontier, &material, &reads),
        Err(CoreError::Validation(message)) if message.contains("bytes"))
    );
}

#[test]
fn effect_prepare_does_not_emit_an_unchanged_scope_map_write() {
    let mut fixture = start_fixture();
    let mut scope = fixture_root_scope(&fixture);
    let (mut effects, _) = paged_effect_source(&mut fixture, &mut scope, 1);
    let mut previous = effects.remove(0);
    previous.phase = crate::EffectPhase::Admitted;
    let command_id = "command:prepare-without-scope-change";
    let mut batch = prepare_pinned_command_batch(
        &fixture.frontier,
        vec![MachinePinnedBatchCommand {
            command_id: command_id.to_owned(),
            actor: "actor:pinned-test".to_owned(),
            run_id: fixture.current.run_id.clone(),
            precondition: MachinePinnedBatchPrecondition::Parent(Some(
                fixture.current.precondition_token(),
            )),
            command: Command::TransitionEffect {
                intent_id: previous.intent_id.clone(),
                transition: crate::EffectTransition::Prepare,
            },
        }],
        None,
    )
    .expect("batch freezes");
    let proof = MachinePinnedCommandProof::vacant(
        MachineCommandIndexProof::empty_nonmembership(command_id).expect("absence proof"),
    );
    let PinnedMachineCommandPreparation::Lookup(lookup) =
        batch.prepare_next(&proof).expect("prepare lookup")
    else {
        panic!("fresh lookup")
    };
    let PinnedMachineRunPreparation::Reads(read) = lookup
        .resolve_run(MachinePinnedRunLookup::new(
            revision(command_id),
            fixture.current.run_id.clone(),
            fixture.frontier.runs.clone(),
            Some(fixture.current.clone()),
        ))
        .expect("Run resolves")
    else {
        panic!("fresh reads")
    };
    let mut inputs = fact_inputs(&fixture, command_id, "unused");
    inputs.facts.clear();
    inputs.scopes.insert(scope.scope_id.clone(), Some(scope));
    inputs
        .effects
        .insert(previous.intent_id.clone(), Some(previous));
    let PinnedMachineFreshPreparation::Prepared(prepared) =
        read.prepare(inputs).expect("Effect Prepare reduces")
    else {
        panic!("Effect Prepare is bounded")
    };
    assert!(
        prepared
            .scope_root_mutations()
            .expect("Scope root plan")
            .is_empty()
    );
    let result = finish(*prepared);
    assert!(
        result
            .delta
            .run
            .as_ref()
            .expect("Run delta")
            .scopes
            .is_empty()
    );
    let batch = batch
        .accept_step(result)
        .expect("step accepts")
        .finish()
        .expect("batch closes");
    assert_eq!(batch.machine.events.len(), 1);
}

#[test]
fn failed_component_material_preserves_its_outer_advance_source() {
    let fixture = start_fixture();
    let bytes = br#"{"reason":"declared_failure"}"#.to_vec();
    let detail = ArtifactRecord {
        reference: crate::artifact_ref(crate::DECLARED_FAILURE_ARTIFACT_KIND, &bytes)
            .expect("failure detail ref"),
        bytes,
    };
    let material = MachineMaterialAdmission::new(
        "profile:advance-component-result".to_owned(),
        Vec::new(),
        vec![detail.clone()],
    )
    .expect("component result material");
    let command = MachinePinnedBatchCommand {
        command_id: "command:terminal-core-failure".to_owned(),
        actor: "actor:pinned-test".to_owned(),
        run_id: fixture.current.run_id.clone(),
        precondition: MachinePinnedBatchPrecondition::Parent(Some(
            fixture.current.precondition_token(),
        )),
        command: Command::FailRun {
            failure: crate::RunFailure {
                class: crate::RunFailureClass::DeclaredFailure,
                code: "declared_failure".to_owned(),
                detail: detail.reference,
            },
        },
    };
    let batch = prepare_pinned_command_batch(
        &fixture.frontier,
        vec![command],
        Some((material.clone(), material_parent_reads(&material, false))),
    )
    .expect("outer source differs from Core member legally");
    assert_eq!(
        batch
            .proposed_material()
            .expect("material retained")
            .source_command_id(),
        "profile:advance-component-result"
    );
    assert_ne!(
        batch.next_command().expect("Core command").command_id,
        material.source_command_id()
    );
}
