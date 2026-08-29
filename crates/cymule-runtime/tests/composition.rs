//! Runtime provider graph and binding admission conformance.

use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use cymule_runtime::{
    AdmittedPluginRouter, BoundPluginHost, CompositionError, EXECUTION_BINDING_VERSION,
    EngineFailure, EngineFailureCategory, EnginePhase, EngineRetryDisposition, ExecutionBinding,
    ExecutionOperationKind, MAX_COMPOSITION_TOKEN_SCALARS, MAX_EXECUTION_OPERATIONS_PER_KIND,
    MAX_PROVIDER_PROPERTIES, MAX_PROVIDER_PROPERTY_VALUE_SCALARS, MAX_PROVIDER_SERVICES,
    MAX_RUNTIME_PROVIDERS, PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest,
    PluginOperation, PluginRequest, PluginResponse, RUNTIME_COMPOSITION_VERSION,
    RuntimeCompositionGraph, RuntimeImplementation, RuntimeProviderDescriptor, RuntimeResult,
    ServiceKey,
};
use serde_json::{Value, json};

const SCHEMA_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const CONFIGURATION_FINGERPRINT: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const EFFECT_INTENT_ID: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

fn assert_selected_operation_mismatch(error: cymule_runtime::RuntimeError) {
    assert!(matches!(
        error,
        cymule_runtime::RuntimeError::Composition(error)
            if matches!(*error, CompositionError::SelectedOperationMismatch { .. })
                && error.code() == "selected_operation_mismatch"
    ));
}

#[test]
fn every_composition_variant_owns_stable_code_message_and_engine_projection() {
    let service = ServiceKey::new("cymule.test", "service", "1");
    let other_service = ServiceKey::new("cymule.test", "service", "2");
    let cases = vec![
        (
            CompositionError::InvalidProvider {
                provider_id: "provider".to_owned(),
                reason: "invalid".to_owned(),
            },
            "invalid_runtime_provider",
        ),
        (
            CompositionError::DuplicateProviderId {
                provider_id: "provider".to_owned(),
            },
            "duplicate_runtime_provider",
        ),
        (
            CompositionError::DuplicateServiceProvider {
                service: service.clone(),
                providers: vec!["one".to_owned(), "two".to_owned()],
            },
            "duplicate_service_provider",
        ),
        (
            CompositionError::MissingRequirement {
                consumer: "consumer".to_owned(),
                required: service.clone(),
            },
            "missing_runtime_requirement",
        ),
        (
            CompositionError::RevisionMismatch {
                consumer: "consumer".to_owned(),
                required: service.clone(),
                available: vec![other_service],
            },
            "runtime_revision_mismatch",
        ),
        (
            CompositionError::DependencyCycle {
                providers: vec!["one".to_owned(), "two".to_owned()],
            },
            "runtime_dependency_cycle",
        ),
        (
            CompositionError::NonCanonicalBindingDescriptor,
            "noncanonical_binding_descriptor",
        ),
        (
            CompositionError::UnboundService {
                service: service.clone(),
            },
            "runtime_service_unbound",
        ),
        (
            CompositionError::InvalidPlanRequirement {
                key: "requirement".to_owned(),
                reason: "invalid".to_owned(),
            },
            "invalid_plan_requirement",
        ),
        (
            CompositionError::MissingProviderProperty {
                provider_id: "provider".to_owned(),
                key: "property".to_owned(),
            },
            "missing_provider_property",
        ),
        (
            CompositionError::ProviderPropertyMismatch {
                provider_id: "provider".to_owned(),
                key: "property".to_owned(),
                required: "required".to_owned(),
                actual: "actual".to_owned(),
            },
            "provider_property_mismatch",
        ),
        (
            CompositionError::CompositionLimitExceeded {
                subject: "runtime providers",
                maximum: 64,
            },
            "composition_limit_exceeded",
        ),
        (
            CompositionError::Encoding("invalid encoding".to_owned()),
            "composition_encoding_failed",
        ),
        (
            CompositionError::InvalidExecutionBinding {
                reason: "invalid".to_owned(),
            },
            "invalid_execution_binding",
        ),
        (
            CompositionError::MissingOperationBinding {
                kind: ExecutionOperationKind::Component,
                operation: "component".to_owned(),
            },
            "missing_operation_binding",
        ),
        (
            CompositionError::SelectedOperationMismatch {
                kind: ExecutionOperationKind::Effect,
                operation: "effect".to_owned(),
            },
            "selected_operation_mismatch",
        ),
        (
            CompositionError::ManifestMismatch,
            "execution_manifest_mismatch",
        ),
    ];

    for (error, expected_code) in cases {
        assert_eq!(error.code(), expected_code);
        let expected_message = error.message();
        assert!(!expected_message.is_empty());
        assert_eq!(error.to_string(), expected_message);
        let failure = EngineFailure::from_runtime(error.into(), EnginePhase::ExecutePlan);
        assert_eq!(failure.category, EngineFailureCategory::PluginDefect);
        assert_eq!(failure.code.as_ref(), expected_code);
        assert_eq!(failure.message.as_ref(), expected_message);
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
        failure
            .verify()
            .expect("typed composition projection satisfies the Engine wire");
    }
}

fn service(name: &str, revision: &str) -> ServiceKey {
    ServiceKey::new("cymule.test", name, revision)
}

#[test]
fn composition_collection_identity_and_artifact_bounds_are_exact() {
    let providers = (0..MAX_RUNTIME_PROVIDERS)
        .map(|index| provider(&format!("provider-{index:02}"), Vec::new(), Vec::new()))
        .collect::<Vec<_>>();
    RuntimeCompositionGraph::build(providers.clone())
        .expect("the exact provider-count bound is admitted");
    let mut too_many_providers = providers;
    too_many_providers.push(provider("provider-over", Vec::new(), Vec::new()));
    assert!(matches!(
        RuntimeCompositionGraph::build(too_many_providers),
        Err(CompositionError::CompositionLimitExceeded {
            subject: "runtime provider count",
            maximum: MAX_RUNTIME_PROVIDERS,
        })
    ));

    let services = (0..MAX_PROVIDER_SERVICES)
        .map(|index| service(&format!("service-{index:03}"), "1"))
        .collect::<Vec<_>>();
    RuntimeCompositionGraph::build(vec![provider("services", services.clone(), Vec::new())])
        .expect("the exact per-provider service bound is admitted");
    let mut too_many_services = services;
    too_many_services.push(service("service-over", "1"));
    assert!(matches!(
        RuntimeCompositionGraph::build(vec![provider("services", too_many_services, Vec::new(),)]),
        Err(CompositionError::CompositionLimitExceeded {
            subject: "provider provided-service count",
            maximum: MAX_PROVIDER_SERVICES,
        })
    ));

    let mut aggregate_services = (0..16)
        .map(|provider_index| {
            provider(
                &format!("aggregate-{provider_index:02}"),
                (0..MAX_PROVIDER_SERVICES)
                    .map(|service_index| {
                        service(
                            &format!("aggregate-{provider_index:02}-{service_index:03}"),
                            "1",
                        )
                    })
                    .collect(),
                Vec::new(),
            )
        })
        .collect::<Vec<_>>();
    RuntimeCompositionGraph::build(aggregate_services.clone())
        .expect("the exact aggregate service bound is admitted");
    aggregate_services.push(provider(
        "aggregate-over",
        vec![service("aggregate-over", "1")],
        Vec::new(),
    ));
    assert!(matches!(
        RuntimeCompositionGraph::build(aggregate_services),
        Err(CompositionError::CompositionLimitExceeded {
            subject: "runtime service count",
            maximum: cymule_runtime::MAX_RUNTIME_SERVICES,
        })
    ));

    let mut exact_properties = provider("properties", Vec::new(), Vec::new());
    exact_properties.properties = (0..MAX_PROVIDER_PROPERTIES)
        .map(|index| (format!("property.{index:03}"), "value".to_owned()))
        .collect();
    RuntimeCompositionGraph::build(vec![exact_properties.clone()])
        .expect("the exact provider-property bound is admitted");
    exact_properties
        .properties
        .insert("property.over".to_owned(), "value".to_owned());
    assert!(matches!(
        RuntimeCompositionGraph::build(vec![exact_properties]),
        Err(CompositionError::CompositionLimitExceeded {
            subject: "provider property count",
            maximum: MAX_PROVIDER_PROPERTIES,
        })
    ));

    let mut scalar_provider = provider("scalars", Vec::new(), Vec::new());
    scalar_provider.implementation.implementation_id = "🧭".repeat(MAX_COMPOSITION_TOKEN_SCALARS);
    scalar_provider.properties.insert(
        "property".to_owned(),
        "🧭".repeat(MAX_PROVIDER_PROPERTY_VALUE_SCALARS),
    );
    RuntimeCompositionGraph::build(vec![scalar_provider.clone()])
        .expect("exact multi-byte scalar bounds are admitted");
    let mut property_over = scalar_provider.clone();
    property_over
        .properties
        .get_mut("property")
        .expect("property exists")
        .push('x');
    assert!(RuntimeCompositionGraph::build(vec![property_over]).is_err());
    scalar_provider.implementation.implementation_id.push('x');
    assert!(RuntimeCompositionGraph::build(vec![scalar_provider]).is_err());
    let mut control_provider = provider("control", Vec::new(), Vec::new());
    control_provider.implementation.implementation_id = "implementation\0forged".to_owned();
    assert!(RuntimeCompositionGraph::build(vec![control_provider]).is_err());

    let binding = ExecutionBinding::for_local_process(
        &execution_manifest(),
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("binding admits");
    let bytes = binding.canonical_bytes().expect("binding canonicalizes");
    assert_eq!(
        ExecutionBinding::decode(&bytes).expect("canonical binding decodes"),
        binding
    );
    let mut noncanonical = bytes.clone();
    noncanonical.push(b' ');
    assert!(matches!(
        ExecutionBinding::decode(&noncanonical),
        Err(CompositionError::InvalidExecutionBinding { .. })
    ));
    assert!(matches!(
        ExecutionBinding::decode(&vec![b' '; cymule_core::MAX_ARTIFACT_BYTES + 1]),
        Err(CompositionError::CompositionLimitExceeded {
            subject: "execution binding Artifact bytes",
            maximum: cymule_core::MAX_ARTIFACT_BYTES,
        })
    ));

    let operation = serde_json::to_value(
        binding
            .components
            .values()
            .next()
            .expect("fixture binding selects a component"),
    )
    .expect("operation binding serializes");
    let mut operation_bomb = serde_json::to_value(&binding).expect("binding serializes");
    operation_bomb["components"] = Value::Object(
        (0..=MAX_EXECUTION_OPERATIONS_PER_KIND)
            .map(|index| (format!("operation-{index:04}"), operation.clone()))
            .collect(),
    );
    let operation_bomb = cymule_core::canonical_bytes(&operation_bomb).expect("bomb canonicalizes");
    assert!(matches!(
        ExecutionBinding::decode(&operation_bomb),
        Err(CompositionError::Encoding(_))
    ));
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

fn execution_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.executor".to_owned(),
        components: BTreeMap::from([(
            "evaluate".to_owned(),
            PluginOperation {
                implementation_revision: "component-v1".to_owned(),
            },
        )]),
        effects: BTreeMap::from([(
            "publish".to_owned(),
            PluginEffect {
                implementation_revision: "effect-v1".to_owned(),
                can_reconcile: true,
            },
        )]),
    }
}

fn execution_provider(
    configuration_fingerprint: &str,
    revision: &str,
) -> RuntimeProviderDescriptor {
    RuntimeProviderDescriptor {
        version: RUNTIME_COMPOSITION_VERSION.to_owned(),
        provider_id: "executor".to_owned(),
        implementation: RuntimeImplementation {
            implementation_id: "cymule.test.executor".to_owned(),
            revision: revision.to_owned(),
        },
        provides: vec![
            ServiceKey::new("cymule.plugin.component", "evaluate", PLUGIN_VERSION),
            ServiceKey::new("cymule.plugin.effect", "publish", PLUGIN_VERSION),
        ],
        requires: Vec::new(),
        properties: BTreeMap::from([("isolation.level".to_owned(), "process".to_owned())]),
        configuration_schema_digest: SCHEMA_DIGEST.to_owned(),
        configuration_fingerprint: configuration_fingerprint.to_owned(),
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

#[test]
fn execution_binding_pins_provider_manifest_and_exact_operations() {
    let manifest = execution_manifest();
    let graph = RuntimeCompositionGraph::build(vec![execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )])
    .expect("provider graph admits");
    let manifests = BTreeMap::from([("executor".to_owned(), manifest.clone())]);
    let binding = ExecutionBinding::admit(&graph, &manifests).expect("manifest is selected");
    let reference = binding
        .artifact_ref()
        .expect("binding has an Artifact identity");

    assert_eq!(reference.kind, EXECUTION_BINDING_VERSION);
    assert_ne!(reference.artifact_id, graph.binding_context_id().unwrap());
    let component = binding
        .occurrence_binding(ExecutionOperationKind::Component, "evaluate")
        .expect("component binding derives");
    let effect = binding
        .occurrence_binding(ExecutionOperationKind::Effect, "publish")
        .expect("effect binding derives");
    assert!(component.starts_with("sha256:"));
    assert!(effect.starts_with("sha256:"));
    assert_ne!(component, effect);
    assert!(!component.contains("evaluate"));

    let changed_configuration = ExecutionBinding::admit(
        &RuntimeCompositionGraph::build(vec![execution_provider(
            "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )])
        .unwrap(),
        &manifests,
    )
    .unwrap();
    let changed_implementation = ExecutionBinding::admit(
        &RuntimeCompositionGraph::build(vec![execution_provider(
            CONFIGURATION_FINGERPRINT,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )])
        .unwrap(),
        &manifests,
    )
    .unwrap();
    assert_ne!(reference, changed_configuration.artifact_ref().unwrap());
    assert_ne!(reference, changed_implementation.artifact_ref().unwrap());

    let mut changed_manifest = manifest;
    changed_manifest
        .effects
        .get_mut("publish")
        .unwrap()
        .can_reconcile = false;
    assert_eq!(
        binding.verify_provider_manifest("executor", &changed_manifest),
        Err(CompositionError::ManifestMismatch)
    );
}

struct RecordingProvider {
    manifest: PluginManifest,
    calls: Arc<Mutex<Vec<String>>>,
}

impl PluginHost for RecordingProvider {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: self.manifest.clone(),
            }),
            PluginRequest::Call { component, .. } => {
                self.calls.lock().unwrap().push(component);
                Ok(PluginResponse::CallResult { value: json!(1) })
            }
            PluginRequest::PrepareEffect { operation, .. } => {
                self.calls.lock().unwrap().push(operation);
                Ok(PluginResponse::Prepared)
            }
            request => panic!("unexpected test request {request:?}"),
        }
    }
}

#[test]
fn admitted_router_dispatches_to_exact_composed_provider_not_capability_superset() {
    let manifest = execution_manifest();
    let mut component_provider = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    component_provider.provider_id = "component-provider".to_owned();
    component_provider.implementation.implementation_id = "cymule.test.component".to_owned();
    component_provider.provides = vec![ExecutionOperationKind::Component.service_key("evaluate")];
    let mut effect_provider = execution_provider(
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    effect_provider.provider_id = "effect-provider".to_owned();
    effect_provider.implementation.implementation_id = "cymule.test.effect".to_owned();
    effect_provider.provides = vec![ExecutionOperationKind::Effect.service_key("publish")];
    let graph = RuntimeCompositionGraph::build(vec![component_provider, effect_provider])
        .expect("multi-provider composition admits");
    let component_calls = Arc::new(Mutex::new(Vec::new()));
    let effect_calls = Arc::new(Mutex::new(Vec::new()));
    let mut component_manifest = manifest.clone();
    component_manifest.implementation_id = "cymule.test.component".to_owned();
    component_manifest.effects.clear();
    component_manifest.components.insert(
        "advertised_but_unbound".to_owned(),
        PluginOperation {
            implementation_revision: "unused-v1".to_owned(),
        },
    );
    let mut effect_manifest = manifest;
    effect_manifest.implementation_id = "cymule.test.effect".to_owned();
    effect_manifest.components.clear();
    let binding = ExecutionBinding::admit(
        &graph,
        &BTreeMap::from([
            ("component-provider".to_owned(), component_manifest.clone()),
            ("effect-provider".to_owned(), effect_manifest.clone()),
        ]),
    )
    .expect("independent provider manifests admit");
    let mut router = AdmittedPluginRouter::new(
        binding.clone(),
        BTreeMap::from([
            (
                "component-provider".to_owned(),
                Box::new(RecordingProvider {
                    manifest: component_manifest,
                    calls: component_calls.clone(),
                }) as Box<dyn PluginHost>,
            ),
            (
                "effect-provider".to_owned(),
                Box::new(RecordingProvider {
                    manifest: effect_manifest,
                    calls: effect_calls.clone(),
                }) as Box<dyn PluginHost>,
            ),
        ]),
    )
    .expect("router verifies selected provider capabilities");

    router
        .invoke_bound(
            &binding,
            &binding,
            PluginRequest::Call {
                component: "evaluate".to_owned(),
                input: json!({}),
            },
        )
        .expect("component routes");
    router
        .invoke_bound(
            &binding,
            &binding,
            PluginRequest::PrepareEffect {
                operation: "publish".to_owned(),
                intent_id: EFFECT_INTENT_ID.to_owned(),
                input: json!({}),
            },
        )
        .expect("effect routes");
    assert_eq!(*component_calls.lock().unwrap(), ["evaluate"]);
    assert_eq!(*effect_calls.lock().unwrap(), ["publish"]);
    assert!(
        router
            .invoke_bound(
                &binding,
                &binding,
                PluginRequest::Call {
                    component: "advertised_but_unbound".to_owned(),
                    input: json!({}),
                },
            )
            .is_err()
    );
}

#[test]
fn historical_operation_admission_ignores_unrelated_missing_provider_and_operation() {
    let mut old_effect_provider = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    old_effect_provider.provider_id = "old-effect".to_owned();
    old_effect_provider.implementation.implementation_id = "cymule.test.old-effect".to_owned();
    old_effect_provider.provides = vec![ExecutionOperationKind::Effect.service_key("publish")];
    let mut unrelated_provider = execution_provider(
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    unrelated_provider.provider_id = "unrelated-component".to_owned();
    unrelated_provider.implementation.implementation_id =
        "cymule.test.unrelated-component".to_owned();
    unrelated_provider.provides = vec![ExecutionOperationKind::Component.service_key("evaluate")];
    let historical_graph =
        RuntimeCompositionGraph::build(vec![old_effect_provider.clone(), unrelated_provider])
            .expect("historical graph admits");
    let old_manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.old-effect".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::from([(
            "publish".to_owned(),
            PluginEffect {
                implementation_revision: "effect-v1".to_owned(),
                can_reconcile: true,
            },
        )]),
    };
    let unrelated_manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.unrelated-component".to_owned(),
        components: BTreeMap::from([(
            "evaluate".to_owned(),
            PluginOperation {
                implementation_revision: "component-v1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    };
    let historical = ExecutionBinding::admit(
        &historical_graph,
        &BTreeMap::from([
            ("old-effect".to_owned(), old_manifest.clone()),
            ("unrelated-component".to_owned(), unrelated_manifest),
        ]),
    )
    .expect("historical binding admits");
    let current_graph =
        RuntimeCompositionGraph::build(vec![old_effect_provider]).expect("current graph admits");
    let current = ExecutionBinding::admit(
        &current_graph,
        &BTreeMap::from([("old-effect".to_owned(), old_manifest.clone())]),
    )
    .expect("current binding admits");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut router = AdmittedPluginRouter::new(
        current.clone(),
        BTreeMap::from([(
            "old-effect".to_owned(),
            Box::new(RecordingProvider {
                manifest: old_manifest,
                calls: calls.clone(),
            }) as Box<dyn PluginHost>,
        )]),
    )
    .expect("current router admits");
    router
        .invoke_bound(
            &current,
            &historical,
            PluginRequest::PrepareEffect {
                operation: "publish".to_owned(),
                intent_id: EFFECT_INTENT_ID.to_owned(),
                input: json!({}),
            },
        )
        .expect("selected historical operation remains available");
    assert_eq!(*calls.lock().unwrap(), ["publish"]);

    assert_exact_operation_manifest_validation(&historical);
}

fn assert_exact_operation_manifest_validation(historical: &ExecutionBinding) {
    let mut unrelated_changed = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.old-effect".to_owned(),
        components: BTreeMap::from([(
            "unrelated".to_owned(),
            PluginOperation {
                implementation_revision: "changed".to_owned(),
            },
        )]),
        effects: BTreeMap::from([(
            "publish".to_owned(),
            PluginEffect {
                implementation_revision: "effect-v1".to_owned(),
                can_reconcile: true,
            },
        )]),
    };
    assert!(
        historical
            .verify_operation_manifest(
                ExecutionOperationKind::Effect,
                "publish",
                &unrelated_changed,
            )
            .is_ok()
    );
    unrelated_changed
        .effects
        .get_mut("publish")
        .unwrap()
        .can_reconcile = false;
    assert_eq!(
        historical.verify_operation_manifest(
            ExecutionOperationKind::Effect,
            "publish",
            &unrelated_changed,
        ),
        Err(CompositionError::ManifestMismatch)
    );
    unrelated_changed.effects.remove("publish");
    assert_eq!(
        historical.verify_operation_manifest(
            ExecutionOperationKind::Effect,
            "publish",
            &unrelated_changed,
        ),
        Err(CompositionError::ManifestMismatch)
    );
    assert!(matches!(
        historical.verify_operation_manifest(
            ExecutionOperationKind::Component,
            "publish",
            &unrelated_changed,
        ),
        Err(CompositionError::MissingOperationBinding { .. })
    ));
}

#[test]
fn historical_operation_never_falls_back_to_current_provider() {
    let mut old_provider = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    old_provider.provider_id = "old".to_owned();
    old_provider.implementation.implementation_id = "cymule.test.old".to_owned();
    old_provider.provides = vec![ExecutionOperationKind::Effect.service_key("publish")];
    let old_graph = RuntimeCompositionGraph::build(vec![old_provider]).unwrap();
    let old_manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.old".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::from([(
            "publish".to_owned(),
            PluginEffect {
                implementation_revision: "effect-v1".to_owned(),
                can_reconcile: true,
            },
        )]),
    };
    let historical = ExecutionBinding::admit(
        &old_graph,
        &BTreeMap::from([("old".to_owned(), old_manifest)]),
    )
    .unwrap();

    let mut new_provider = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    new_provider.provider_id = "new".to_owned();
    new_provider.implementation.implementation_id = "cymule.test.new".to_owned();
    new_provider.provides = vec![ExecutionOperationKind::Effect.service_key("publish")];
    let new_graph = RuntimeCompositionGraph::build(vec![new_provider]).unwrap();
    let new_manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "cymule.test.new".to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::from([(
            "publish".to_owned(),
            PluginEffect {
                implementation_revision: "effect-v1".to_owned(),
                can_reconcile: true,
            },
        )]),
    };
    let current = ExecutionBinding::admit(
        &new_graph,
        &BTreeMap::from([("new".to_owned(), new_manifest.clone())]),
    )
    .unwrap();
    let new_calls = Arc::new(Mutex::new(Vec::new()));
    let mut router = AdmittedPluginRouter::new(
        current.clone(),
        BTreeMap::from([(
            "new".to_owned(),
            Box::new(RecordingProvider {
                manifest: new_manifest,
                calls: new_calls.clone(),
            }) as Box<dyn PluginHost>,
        )]),
    )
    .unwrap();
    let error = router
        .invoke_bound(
            &current,
            &historical,
            PluginRequest::PrepareEffect {
                operation: "publish".to_owned(),
                intent_id: EFFECT_INTENT_ID.to_owned(),
                input: json!({}),
            },
        )
        .expect_err("missing historical provider fails closed");
    assert_selected_operation_mismatch(error);
    assert!(new_calls.lock().unwrap().is_empty());
}

struct AllCallProvider {
    manifest: PluginManifest,
    calls: Arc<AtomicUsize>,
}

impl PluginHost for AllCallProvider {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: self.manifest.clone(),
            }),
            PluginRequest::PrepareEffect { .. } => Ok(PluginResponse::Prepared),
            request => panic!("unexpected test request {request:?}"),
        }
    }
}

#[test]
fn historical_operation_rejects_same_provider_descriptor_drift_before_any_call() {
    let manifest = execution_manifest();
    let historical_provider = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    let historical_graph = RuntimeCompositionGraph::build(vec![historical_provider]).unwrap();
    let historical = ExecutionBinding::admit(
        &historical_graph,
        &BTreeMap::from([("executor".to_owned(), manifest.clone())]),
    )
    .unwrap();

    for current_provider in [
        execution_provider(
            CONFIGURATION_FINGERPRINT,
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
        execution_provider(
            "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
    ] {
        let current_graph = RuntimeCompositionGraph::build(vec![current_provider]).unwrap();
        let current = ExecutionBinding::admit(
            &current_graph,
            &BTreeMap::from([("executor".to_owned(), manifest.clone())]),
        )
        .unwrap();
        let direct_calls = Arc::new(AtomicUsize::new(0));
        let mut direct = AllCallProvider {
            manifest: manifest.clone(),
            calls: direct_calls.clone(),
        };
        let direct_error = direct
            .invoke_bound(
                &current,
                &historical,
                PluginRequest::PrepareEffect {
                    operation: "publish".to_owned(),
                    intent_id: EFFECT_INTENT_ID.to_owned(),
                    input: json!({}),
                },
            )
            .expect_err("direct host requires runtime-owner descriptor equivalence");
        assert_selected_operation_mismatch(direct_error);
        assert_eq!(direct_calls.load(Ordering::SeqCst), 0);

        let calls = Arc::new(AtomicUsize::new(0));
        let mut router = AdmittedPluginRouter::new(
            current.clone(),
            BTreeMap::from([(
                "executor".to_owned(),
                Box::new(AllCallProvider {
                    manifest: manifest.clone(),
                    calls: calls.clone(),
                }) as Box<dyn PluginHost>,
            )]),
        )
        .unwrap();
        calls.store(0, Ordering::SeqCst);
        let error = router
            .invoke_bound(
                &current,
                &historical,
                PluginRequest::PrepareEffect {
                    operation: "publish".to_owned(),
                    intent_id: EFFECT_INTENT_ID.to_owned(),
                    input: json!({}),
                },
            )
            .expect_err("descriptor drift cannot realize a historical occurrence");
        assert_selected_operation_mismatch(error);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn historical_operation_rejects_transitive_dependency_rebinding_before_any_call() {
    let manifest = execution_manifest();
    let store_service = ServiceKey::new("cymule.test", "store", "1");
    let mut executor = execution_provider(
        CONFIGURATION_FINGERPRINT,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    );
    executor.requires = vec![store_service.clone()];
    let store =
        |provider_id: &str, implementation_id: &str, revision: &str| RuntimeProviderDescriptor {
            version: RUNTIME_COMPOSITION_VERSION.to_owned(),
            provider_id: provider_id.to_owned(),
            implementation: RuntimeImplementation {
                implementation_id: implementation_id.to_owned(),
                revision: revision.to_owned(),
            },
            provides: vec![store_service.clone()],
            requires: Vec::new(),
            properties: BTreeMap::new(),
            configuration_schema_digest: SCHEMA_DIGEST.to_owned(),
            configuration_fingerprint: CONFIGURATION_FINGERPRINT.to_owned(),
        };
    let old_store = store(
        "old-store",
        "cymule.test.old-store",
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    );
    let new_store = store(
        "new-store",
        "cymule.test.new-store",
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    );
    let store_manifest = |implementation_id: &str| PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: implementation_id.to_owned(),
        components: BTreeMap::new(),
        effects: BTreeMap::new(),
    };
    let historical = ExecutionBinding::admit(
        &RuntimeCompositionGraph::build(vec![executor.clone(), old_store]).unwrap(),
        &BTreeMap::from([
            ("executor".to_owned(), manifest.clone()),
            (
                "old-store".to_owned(),
                store_manifest("cymule.test.old-store"),
            ),
        ]),
    )
    .unwrap();
    let current = ExecutionBinding::admit(
        &RuntimeCompositionGraph::build(vec![executor, new_store]).unwrap(),
        &BTreeMap::from([
            ("executor".to_owned(), manifest.clone()),
            (
                "new-store".to_owned(),
                store_manifest("cymule.test.new-store"),
            ),
        ]),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut router = AdmittedPluginRouter::new(
        current.clone(),
        BTreeMap::from([(
            "executor".to_owned(),
            Box::new(AllCallProvider {
                manifest,
                calls: calls.clone(),
            }) as Box<dyn PluginHost>,
        )]),
    )
    .unwrap();
    calls.store(0, Ordering::SeqCst);
    let error = router
        .invoke_bound(
            &current,
            &historical,
            PluginRequest::PrepareEffect {
                operation: "publish".to_owned(),
                intent_id: EFFECT_INTENT_ID.to_owned(),
                input: json!({}),
            },
        )
        .expect_err("transitive provider rebinding fails closed");
    assert_selected_operation_mismatch(error);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn admitted_operation_token_is_host_bound_and_consumed() {
    let manifest = execution_manifest();
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let mut first = AllCallProvider {
        manifest: manifest.clone(),
        calls: first_calls.clone(),
    };
    let _second = AllCallProvider {
        manifest,
        calls: second_calls.clone(),
    };
    let admission = first
        .admit_bound_operation(
            &binding,
            &binding,
            ExecutionOperationKind::Effect,
            "publish",
        )
        .unwrap();
    assert!(admission.is_available());
    admission
        .invoke(PluginRequest::PrepareEffect {
            operation: "publish".to_owned(),
            intent_id: EFFECT_INTENT_ID.to_owned(),
            input: json!({}),
        })
        .expect("one-shot token invokes only its issuing host");
    assert_eq!(first_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn raw_host_cannot_override_selected_operation_equivalence() {
    let manifest = execution_manifest();
    let historical = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("historical binding admits");
    let current = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .expect("current binding admits");
    let calls = Arc::new(AtomicUsize::new(0));
    let mut host = AllCallProvider {
        manifest,
        calls: calls.clone(),
    };

    let admission = host
        .admit_bound_operation(
            &current,
            &historical,
            ExecutionOperationKind::Effect,
            "publish",
        )
        .expect("framework returns a closed unavailable admission");
    assert!(!admission.is_available());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "binding mismatch rejects before raw host Describe or invocation"
    );
    let error = admission
        .invoke(PluginRequest::PrepareEffect {
            operation: "publish".to_owned(),
            intent_id: EFFECT_INTENT_ID.to_owned(),
            input: json!({}),
        })
        .expect_err("framework-selected operation mismatch stays typed");
    assert_selected_operation_mismatch(error);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
