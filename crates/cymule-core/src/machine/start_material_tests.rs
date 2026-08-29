// StartRun material ownership and independent leaf/aggregate budget regressions.

const START_BUDGET_COMMAND: &str = "command:start-material-budget";
const START_BUDGET_RUN: &str = "run:start-material-budget";
const MEBIBYTE: usize = 1024 * 1024;

fn start_plan_with_canonical_bytes(size: usize) -> SealedPlan {
    let mut value = candidate();
    value
        .metadata
        .insert("budget-padding".to_owned(), String::new());
    let empty = seal_plan(value.clone()).expect("small budget Plan seals");
    let base = crate::canonical_bytes(&empty).expect("Plan encodes").len();
    assert!(size >= base);
    value
        .metadata
        .insert("budget-padding".to_owned(), "x".repeat(size - base));
    let plan = seal_plan(value).expect("large budget Plan seals");
    assert_eq!(
        crate::canonical_bytes(&plan).expect("Plan encodes").len(),
        size
    );
    plan
}

fn start_input_with_bytes(size: usize) -> ArtifactRecord {
    assert!(size >= 2);
    let mut bytes = vec![b'x'; size];
    bytes[0] = b'"';
    bytes[size - 1] = b'"';
    ArtifactRecord {
        reference: crate::artifact_ref(crate::RUN_INPUT_ARTIFACT_KIND, &bytes)
            .expect("large input identity derives"),
        bytes,
    }
}

fn start_budget_material(
    plan: SealedPlan,
    binding_size: usize,
    input_size: usize,
) -> MachineStartRunMaterial {
    MachineStartRunMaterial::new(
        START_BUDGET_COMMAND.to_owned(),
        plan,
        binding_bytes(vec![b'b'; binding_size]),
        start_input_with_bytes(input_size),
    )
    .expect("each proposed leaf is independently bounded")
}

fn start_budget_envelope(material: &MachineStartRunMaterial) -> CommandEnvelope {
    let (plan, binding, input) = material.parts().expect("complete material roles");
    CommandEnvelope {
        command_version: COMMAND_VERSION.to_owned(),
        command_id: START_BUDGET_COMMAND.to_owned(),
        actor: "actor:material-budget".to_owned(),
        run_id: START_BUDGET_RUN.to_owned(),
        expected_precondition: None,
        command: Command::StartRun {
            plan_id: plan.plan_id.clone(),
            binding_context: binding.reference.artifact_id.clone(),
            input: input.reference.clone(),
            material_digest: material.material_digest().to_owned(),
            initial_attempt: crate::InitialAttemptSpec {
                attempt_id: revision("material-budget-attempt"),
                continuation_id: revision("material-budget-continuation"),
                occurrence_binding: binding.reference.artifact_id.clone(),
                continuation_epoch: 0,
                execution_fence: 1,
            },
        },
    }
}

fn start_budget_inputs(
    material: MachineStartRunMaterial,
    retained: bool,
) -> (
    MachineAuthorityFrontier,
    CommandEnvelope,
    MachineRunReadInputs,
) {
    let mut frontier =
        MachineAuthorityFrontier::genesis(empty_map(), empty_map(), empty_map(), empty_map())
            .expect("budget source initializes");
    if retained {
        let absent = material_parent_reads(material.admission(), false);
        frontier = prepare_machine_material_admission(&frontier, material.admission(), &absent)
            .expect("actual retained parent material admits")
            .frontier;
    }
    let envelope = start_budget_envelope(&material);
    let parents = material_parent_reads(material.admission(), retained);
    let inputs = MachineRunReadInputs {
        machine_revision: revision("start-budget-read"),
        run_id: START_BUDGET_RUN.to_owned(),
        runs_root: frontier.runs.clone(),
        facts_root: frontier.facts.clone(),
        run: None,
        new_run_empty_root: Some(empty_map()),
        new_run_empty_log: Some(empty_log()),
        plans: parents.plans,
        artifacts: parents.artifacts,
        scopes: BTreeMap::new(),
        scope_locations: BTreeMap::new(),
        effects: BTreeMap::new(),
        obligations: BTreeMap::new(),
        attempts: BTreeMap::new(),
        facts: BTreeMap::new(),
        start_material: Some(material),
        index_pages: Vec::new(),
        log_pages: Vec::new(),
    };
    (frontier, envelope, inputs)
}

#[test]
fn start_material_roles_borrow_one_canonical_admission() {
    let (material, _) = start_fixture_material();
    let (plan, binding, input) = material.parts().expect("material roles");
    assert!(std::ptr::eq(
        plan,
        material
            .admission()
            .plans()
            .first()
            .expect("one stored Plan")
    ));
    for artifact in [binding, input] {
        assert!(
            material
                .admission()
                .artifacts()
                .iter()
                .any(|stored| std::ptr::eq(stored, artifact))
        );
    }
    let mut counted = 0;
    account_material_leaves(material.admission(), &mut counted).expect("independent leaves fit");
    let expected = crate::canonical_bytes(plan).expect("Plan bytes").len()
        + crate::canonical_bytes(binding)
            .expect("binding bytes")
            .len()
        + crate::canonical_bytes(input).expect("input bytes").len();
    assert_eq!(counted, expected, "each proposed payload is counted once");
    let mut exact = MAX_PINNED_MACHINE_READ_SET_BYTES - expected;
    account_material_leaves(material.admission(), &mut exact).expect("exact aggregate ceiling");
    assert_eq!(exact, MAX_PINNED_MACHINE_READ_SET_BYTES);
    let mut over = MAX_PINNED_MACHINE_READ_SET_BYTES - expected + 1;
    assert!(
        matches!(account_material_leaves(material.admission(), &mut over),
        Err(CoreError::Validation(message)) if message.contains("read set has"))
    );
}

#[test]
fn start_material_budget_accepts_large_input_and_an_exact_maximum_plan_leaf() {
    for plan_size in [None, Some(MAX_PINNED_MACHINE_READ_LEAF_BYTES)] {
        let plan = plan_size.map_or_else(
            || seal_plan(candidate()).expect("Plan seals"),
            start_plan_with_canonical_bytes,
        );
        let material = start_budget_material(plan, 32, 5 * MEBIBYTE);
        for retained in [false, true] {
            let (frontier, envelope, inputs) = start_budget_inputs(material.clone(), retained);
            MachineRunReadSet::prepare(&frontier, &envelope, inputs)
                .expect("true leaves fit even when their combined helper exceeds one leaf");
        }
    }
}

#[test]
fn start_material_budget_keeps_parent_reads_inside_the_64_mib_total() {
    for (plan_size, retained_fits) in [(10 * MEBIBYTE, true), (11 * MEBIBYTE, false)] {
        let material = start_budget_material(
            start_plan_with_canonical_bytes(plan_size),
            crate::MAX_ARTIFACT_BYTES,
            crate::MAX_ARTIFACT_BYTES,
        );
        let (frontier, envelope, absent) = start_budget_inputs(material.clone(), false);
        MachineRunReadSet::prepare(&frontier, &envelope, absent)
            .expect("three distinct proposed leaves fit below the total bound");
        let (frontier, envelope, retained) = start_budget_inputs(material, true);
        let result = MachineRunReadSet::prepare(&frontier, &envelope, retained);
        if retained_fits {
            result.expect("proposal and exact retained reads fit below 64 MiB");
        } else {
            assert!(matches!(result, Err(CoreError::Validation(message))
                if message.contains("read set has") && message.contains("67108864")));
        }
    }
}

#[test]
fn start_material_rejects_an_oversized_leaf_and_wrong_artifact_roles() {
    let plan = start_plan_with_canonical_bytes(MAX_PINNED_MACHINE_READ_LEAF_BYTES + 1);
    let result = MachineStartRunMaterial::new(
        START_BUDGET_COMMAND.to_owned(),
        plan,
        binding("bound"),
        input(),
    );
    assert!(matches!(result, Err(CoreError::Validation(message))
        if message.contains("Machine material Plan") && message.contains("12582912")));
    let plan = seal_plan(candidate()).expect("Plan seals");
    let result = MachineStartRunMaterial::new(
        START_BUDGET_COMMAND.to_owned(),
        plan.clone(),
        input(),
        input(),
    );
    assert!(
        matches!(result, Err(CoreError::Validation(message)) if message.contains("Artifact kinds"))
    );
    let result = MachineStartRunMaterial::new(
        START_BUDGET_COMMAND.to_owned(),
        plan,
        binding("bound"),
        binding("not-input"),
    );
    assert!(
        matches!(result, Err(CoreError::Validation(message)) if message.contains("Artifact kinds"))
    );
}

#[test]
fn start_material_parent_byte_conflicts_are_not_deduplicated_by_identity() {
    let material = start_budget_material(seal_plan(candidate()).expect("Plan seals"), 32, 1024);
    let (frontier, envelope, mut inputs) = start_budget_inputs(material, true);
    let input = inputs
        .artifacts
        .values_mut()
        .filter_map(Option::as_mut)
        .find(|record| record.reference.kind == crate::RUN_INPUT_ARTIFACT_KIND)
        .expect("retained input");
    input.bytes[1] = b'y';
    assert!(MachineRunReadSet::prepare(&frontier, &envelope, inputs).is_err());
}
