//! Runtime provider graph and binding admission conformance.

use std::collections::BTreeMap;

use cymule_runtime::{
    CompositionError, RUNTIME_COMPOSITION_VERSION, RuntimeCompositionGraph, RuntimeImplementation,
    RuntimeProviderDescriptor, ServiceKey,
};

const SCHEMA_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const CONFIGURATION_FINGERPRINT: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";

fn service(name: &str, revision: &str) -> ServiceKey {
    ServiceKey::new("cymule.test", name, revision)
}

fn provider(
    provider_id: &str,
    provides: Vec<ServiceKey>,
    requires: Vec<ServiceKey>,
) -> RuntimeProviderDescriptor {
    RuntimeProviderDescriptor {
        version: RUNTIME_COMPOSITION_VERSION.to_owned(),
        provider_id: provider_id.to_owned(),
        implementation: RuntimeImplementation {
            implementation_id: format!("cymule.test.{provider_id}"),
            revision: "sha256:implementation".to_owned(),
        },
        provides,
        requires,
        properties: BTreeMap::new(),
        configuration_schema_digest: SCHEMA_DIGEST.to_owned(),
        configuration_fingerprint: CONFIGURATION_FINGERPRINT.to_owned(),
    }
}

#[test]
fn normalized_provider_input_has_one_stable_content_identity() {
    let store = service("store", "1");
    let clock = service("clock", "1");
    let executor = service("executor", "2");
    let providers = vec![
        provider("worker", vec![executor], vec![store.clone(), clock.clone()]),
        provider("store", vec![store], vec![]),
        provider("clock", vec![clock], vec![]),
    ];
    let mut reordered = providers.clone();
    reordered.reverse();
    reordered[2].requires.reverse();

    let first = RuntimeCompositionGraph::build(providers).expect("graph is valid");
    let second = RuntimeCompositionGraph::build(reordered).expect("graph is valid");

    assert_eq!(first.descriptor(), second.descriptor());
    assert_eq!(first.topology(), ["clock", "store", "worker"]);
    assert_eq!(first.bindings(), second.bindings());
    assert_eq!(
        first.binding_context_id().expect("identity is canonical"),
        second.binding_context_id().expect("identity is canonical")
    );
    assert_eq!(
        first.binding_context_id().expect("identity is canonical"),
        "sha256:4ca38251ffa56c58d827ab0fb4022ef17d443583bbdb5751982c40970cf1d0d5"
    );
    let encoded = serde_json::to_value(first.descriptor()).expect("descriptor is serializable");
    assert!(encoded.get("topology").is_none());
    assert!(encoded.get("bindings").is_none());
}

#[test]
fn implementation_and_configuration_identity_change_the_binding() {
    let baseline = provider("store", vec![service("store", "1")], vec![]);
    let mut changed_revision = baseline.clone();
    changed_revision.implementation.revision = "sha256:other-implementation".to_owned();
    let mut changed_configuration = baseline.clone();
    changed_configuration.configuration_fingerprint =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222".to_owned();

    let id = |provider| {
        RuntimeCompositionGraph::build(vec![provider])
            .expect("provider is valid")
            .binding_context_id()
            .expect("identity is canonical")
    };
    let baseline_id = id(baseline);
    let revision_id = id(changed_revision);
    let configuration_id = id(changed_configuration);

    assert_ne!(baseline_id, revision_id);
    assert_ne!(baseline_id, configuration_id);
    assert_ne!(revision_id, configuration_id);
}

#[test]
fn duplicate_service_provider_and_missing_dependency_are_rejected() {
    let store = service("store", "1");
    let duplicate = RuntimeCompositionGraph::build(vec![
        provider("left", vec![store.clone()], vec![]),
        provider("right", vec![store.clone()], vec![]),
    ])
    .expect_err("one exact service has one provider");
    assert_eq!(
        duplicate,
        CompositionError::DuplicateServiceProvider {
            service: store.clone(),
            providers: vec!["left".to_owned(), "right".to_owned()],
        }
    );

    let missing =
        RuntimeCompositionGraph::build(vec![provider("worker", vec![], vec![store.clone()])])
            .expect_err("provider dependencies must be bound");
    assert_eq!(
        missing,
        CompositionError::MissingRequirement {
            consumer: "worker".to_owned(),
            required: store,
        }
    );
}

#[test]
fn service_revision_mismatch_is_distinct_from_absence() {
    let available = service("store", "1");
    let required = service("store", "2");
    let error = RuntimeCompositionGraph::build(vec![
        provider("store", vec![available.clone()], vec![]),
        provider("worker", vec![], vec![required.clone()]),
    ])
    .expect_err("service API revisions match exactly");

    assert_eq!(
        error,
        CompositionError::RevisionMismatch {
            consumer: "worker".to_owned(),
            required,
            available: vec![available],
        }
    );
}

#[test]
fn provider_dependency_cycles_are_rejected_deterministically() {
    let left = service("left", "1");
    let right = service("right", "1");
    let error = RuntimeCompositionGraph::build(vec![
        provider("left", vec![left.clone()], vec![right.clone()]),
        provider("right", vec![right], vec![left]),
    ])
    .expect_err("provider dependency graphs must be acyclic");

    assert_eq!(
        error,
        CompositionError::DependencyCycle {
            providers: vec!["left".to_owned(), "right".to_owned()],
        }
    );
}

#[test]
fn serialized_descriptor_accepts_only_normalized_provider_input() {
    let mut descriptor = RuntimeCompositionGraph::build(vec![
        provider("alpha", vec![service("alpha", "1")], vec![]),
        provider("beta", vec![service("beta", "1")], vec![]),
    ])
    .expect("graph is valid")
    .descriptor()
    .clone();
    descriptor.providers.reverse();

    assert_eq!(
        descriptor
            .binding_context_id()
            .expect_err("non-normalized provider input is rejected"),
        CompositionError::NonCanonicalBindingDescriptor
    );
}

#[test]
fn plan_requirements_must_match_bound_provider_properties_exactly() {
    let executor = service("executor", "1");
    let mut executor_provider = provider("executor", vec![executor.clone()], vec![]);
    executor_provider.properties = BTreeMap::from([
        ("capability".to_owned(), "evaluation-scorer".to_owned()),
        ("isolation.level".to_owned(), "process".to_owned()),
    ]);
    let graph = RuntimeCompositionGraph::build(vec![executor_provider]).expect("graph is valid");

    let admitted = graph
        .admit_plan_requirements(
            &executor,
            &BTreeMap::from([("capability".to_owned(), "evaluation-scorer".to_owned())]),
        )
        .expect("exact property matches are eligible for later policy admission");
    assert_eq!(admitted.provider_id, "executor");

    assert_eq!(
        graph
            .admit_plan_requirements(
                &executor,
                &BTreeMap::from([("durability".to_owned(), "required".to_owned())]),
            )
            .expect_err("missing properties do not bind"),
        CompositionError::MissingProviderProperty {
            provider_id: "executor".to_owned(),
            key: "durability".to_owned(),
        }
    );
    assert_eq!(
        graph
            .admit_plan_requirements(
                &executor,
                &BTreeMap::from([("isolation.level".to_owned(), "sandbox".to_owned())]),
            )
            .expect_err("different property values do not bind"),
        CompositionError::ProviderPropertyMismatch {
            provider_id: "executor".to_owned(),
            key: "isolation.level".to_owned(),
            required: "sandbox".to_owned(),
            actual: "process".to_owned(),
        }
    );
}

#[test]
fn ambiguous_plan_requirement_keys_fail_closed() {
    let executor = service("executor", "1");
    let graph =
        RuntimeCompositionGraph::build(vec![provider("executor", vec![executor.clone()], vec![])])
            .expect("graph is valid");

    for key in [
        "",
        "Capability",
        "isolation..level",
        "isolation._level",
        "trailing-",
    ] {
        assert!(matches!(
            graph.admit_plan_requirements(
                &executor,
                &BTreeMap::from([(key.to_owned(), "value".to_owned())]),
            ),
            Err(CompositionError::InvalidPlanRequirement { .. })
        ));
    }
}
