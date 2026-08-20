//! Runtime composition graph and process-local lifecycle conformance.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use cymule_runtime::{
    AcquiredRuntimeLayer, CompositionError, RUNTIME_COMPOSITION_VERSION, RuntimeComposition,
    RuntimeCompositionGraph, RuntimeImplementation, RuntimeLayerDescriptor, RuntimeLayerFactory,
    RuntimeLayerFailure, RuntimeLayerLifecycle, RuntimeLayerShareScope, RuntimeServiceBinding,
    RuntimeServices, ServiceKey,
};

const SCHEMA_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const CONFIGURATION_FINGERPRINT: &str =
    "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const FAILURE_CONTRACT: &str = "cymule.test-constructor-failure/1";

struct TestFactory {
    descriptor: RuntimeLayerDescriptor,
    acquire: Box<AcquireFn>,
}

type AcquireFn =
    dyn Fn(&RuntimeServices) -> Result<AcquiredRuntimeLayer, RuntimeLayerFailure> + Send + Sync;

impl RuntimeLayerFactory for TestFactory {
    fn descriptor(&self) -> &RuntimeLayerDescriptor {
        &self.descriptor
    }

    fn acquire(
        &self,
        dependencies: &RuntimeServices,
    ) -> Result<AcquiredRuntimeLayer, RuntimeLayerFailure> {
        (self.acquire)(dependencies)
    }
}

fn service(name: &str, revision: &str) -> ServiceKey {
    ServiceKey::new("cymule.test", name, revision)
}

fn layer(
    layer_id: &str,
    provides: Vec<ServiceKey>,
    requires: Vec<ServiceKey>,
) -> RuntimeLayerDescriptor {
    RuntimeLayerDescriptor {
        version: RUNTIME_COMPOSITION_VERSION.to_owned(),
        layer_id: layer_id.to_owned(),
        implementation: RuntimeImplementation {
            implementation_id: format!("cymule.test.{layer_id}"),
            revision: "sha256:implementation".to_owned(),
        },
        provides,
        requires,
        constructor_failure_contract: FAILURE_CONTRACT.to_owned(),
        configuration_schema_digest: SCHEMA_DIGEST.to_owned(),
        configuration_fingerprint: CONFIGURATION_FINGERPRINT.to_owned(),
        lifecycle: RuntimeLayerLifecycle::ProcessLocal,
        share_scope: RuntimeLayerShareScope::BindingContext,
    }
}

fn trace(trace: &AtomicU64, digit: u64) {
    trace
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            Some(current * 10 + digit)
        })
        .expect("the update closure always returns a value");
}

#[test]
fn graph_normalizes_input_and_has_a_stable_content_identity() {
    let store = service("store", "1");
    let clock = service("clock", "1");
    let executor = service("executor", "2");
    let layers = vec![
        layer("worker", vec![executor], vec![store.clone(), clock.clone()]),
        layer("store", vec![store], vec![]),
        layer("clock", vec![clock], vec![]),
    ];
    let mut reversed = layers.clone();
    reversed.reverse();
    reversed[2].requires.reverse();

    let first = RuntimeCompositionGraph::build(layers).expect("graph is valid");
    let second = RuntimeCompositionGraph::build(reversed).expect("graph is valid");

    assert_eq!(first.descriptor(), second.descriptor());
    assert_eq!(first.descriptor().topology, ["clock", "store", "worker"]);
    assert_eq!(
        first.binding_context_id().expect("identity is canonical"),
        second.binding_context_id().expect("identity is canonical")
    );
    assert_eq!(
        first.binding_context_id().expect("identity is canonical"),
        "sha256:d3bd88e3924b709fbb1149ff932220f9cd91216c9d8109e1ee5842cf6c300f28"
    );
}

#[test]
fn implementation_revision_and_configuration_identity_change_the_binding() {
    let store = service("store", "1");
    let baseline = layer("store", vec![store.clone()], vec![]);
    let mut changed_revision = baseline.clone();
    changed_revision.implementation.revision = "sha256:other-implementation".to_owned();
    let mut changed_configuration = baseline.clone();
    changed_configuration.configuration_fingerprint =
        "sha256:2222222222222222222222222222222222222222222222222222222222222222".to_owned();

    let baseline_id = RuntimeCompositionGraph::build(vec![baseline])
        .expect("baseline graph is valid")
        .binding_context_id()
        .expect("identity is canonical");
    let revision_id = RuntimeCompositionGraph::build(vec![changed_revision])
        .expect("revision graph is valid")
        .binding_context_id()
        .expect("identity is canonical");
    let configuration_id = RuntimeCompositionGraph::build(vec![changed_configuration])
        .expect("configuration graph is valid")
        .binding_context_id()
        .expect("identity is canonical");

    assert_ne!(baseline_id, revision_id);
    assert_ne!(baseline_id, configuration_id);
    assert_ne!(revision_id, configuration_id);
}

#[test]
fn binding_identity_rejects_a_modified_derived_projection() {
    let mut descriptor =
        RuntimeCompositionGraph::build(vec![layer("store", vec![service("store", "1")], vec![])])
            .expect("graph is valid")
            .descriptor()
            .clone();
    descriptor.topology.clear();

    assert_eq!(
        descriptor
            .binding_context_id()
            .expect_err("derived topology is part of canonical identity admission"),
        CompositionError::NonCanonicalBindingDescriptor
    );
}

#[test]
fn graph_rejects_duplicate_providers_and_missing_services() {
    let store = service("store", "1");
    let duplicate = RuntimeCompositionGraph::build(vec![
        layer("left", vec![store.clone()], vec![]),
        layer("right", vec![store.clone()], vec![]),
    ])
    .expect_err("an exact service has one provider");
    assert_eq!(
        duplicate,
        CompositionError::DuplicateProvider {
            service: store.clone(),
            providers: vec!["left".to_owned(), "right".to_owned()],
        }
    );

    let missing =
        RuntimeCompositionGraph::build(vec![layer("worker", vec![], vec![store.clone()])])
            .expect_err("requirements must be bound");
    assert_eq!(
        missing,
        CompositionError::MissingRequirement {
            consumer: "worker".to_owned(),
            required: store,
        }
    );
}

#[test]
fn graph_distinguishes_revision_mismatch_from_absence() {
    let available = service("store", "1");
    let required = service("store", "2");
    let error = RuntimeCompositionGraph::build(vec![
        layer("store", vec![available.clone()], vec![]),
        layer("worker", vec![], vec![required.clone()]),
    ])
    .expect_err("API revisions match exactly");

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
fn graph_rejects_dependency_cycles_deterministically() {
    let left = service("left", "1");
    let right = service("right", "1");
    let error = RuntimeCompositionGraph::build(vec![
        layer("left", vec![left.clone()], vec![right.clone()]),
        layer("right", vec![right], vec![left]),
    ])
    .expect_err("construction graphs must be acyclic");

    assert_eq!(
        error,
        CompositionError::DependencyCycle {
            layers: vec!["left".to_owned(), "right".to_owned()],
        }
    );
}

#[test]
fn acquisition_uses_dependencies_and_shutdown_releases_reverse_topology() {
    let trace_value = Arc::new(AtomicU64::new(0));
    let store_key = service("store", "1");
    let worker_key = service("worker", "1");

    let store_factory = {
        let descriptor = layer("store", vec![store_key.clone()], vec![]);
        let trace_value = Arc::clone(&trace_value);
        let store_key = store_key.clone();
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                trace(&trace_value, 1);
                let release_trace = Arc::clone(&trace_value);
                Ok(AcquiredRuntimeLayer::new(
                    vec![RuntimeServiceBinding::new(
                        store_key.clone(),
                        Arc::new(String::from("sqlite")),
                    )],
                    move || {
                        trace(&release_trace, 1);
                        Ok(())
                    },
                ))
            }),
        }
    };
    let worker_factory = {
        let descriptor = layer("worker", vec![worker_key.clone()], vec![store_key.clone()]);
        let trace_value = Arc::clone(&trace_value);
        let store_key = store_key.clone();
        let worker_key = worker_key.clone();
        TestFactory {
            descriptor,
            acquire: Box::new(move |dependencies| {
                let store = dependencies
                    .get::<String>(&store_key)
                    .expect("declared dependency was acquired first");
                trace(&trace_value, 2);
                let release_trace = Arc::clone(&trace_value);
                Ok(AcquiredRuntimeLayer::new(
                    vec![RuntimeServiceBinding::new(
                        worker_key.clone(),
                        Arc::new(format!("worker-on-{store}")),
                    )],
                    move || {
                        trace(&release_trace, 2);
                        Ok(())
                    },
                ))
            }),
        }
    };

    let composition =
        RuntimeComposition::acquire(vec![Box::new(worker_factory), Box::new(store_factory)])
            .expect("composition acquires in topology order");
    assert_eq!(trace_value.load(Ordering::SeqCst), 12);
    assert_eq!(
        composition
            .service::<String>(&worker_key)
            .expect("worker is bound")
            .as_str(),
        "worker-on-sqlite"
    );
    assert_eq!(
        composition.binding_context_id(),
        composition
            .descriptor()
            .binding_context_id()
            .expect("descriptor identity is stable")
    );

    composition.shutdown().expect("finalizers succeed");
    assert_eq!(trace_value.load(Ordering::SeqCst), 1_221);
}

#[test]
fn constructor_failure_releases_prior_layers_in_reverse_topology() {
    let trace_value = Arc::new(AtomicU64::new(0));
    let first_key = service("first", "1");
    let second_key = service("second", "1");
    let third_key = service("third", "1");

    let successful = |descriptor: RuntimeLayerDescriptor,
                      key: ServiceKey,
                      digit: u64,
                      trace_value: Arc<AtomicU64>| {
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                trace(&trace_value, digit);
                let release_trace = Arc::clone(&trace_value);
                Ok(AcquiredRuntimeLayer::new(
                    vec![RuntimeServiceBinding::new(key.clone(), Arc::new(digit))],
                    move || {
                        trace(&release_trace, digit);
                        Ok(())
                    },
                ))
            }),
        }
    };
    let first = successful(
        layer("first", vec![first_key.clone()], vec![]),
        first_key.clone(),
        1,
        Arc::clone(&trace_value),
    );
    let second = successful(
        layer("second", vec![second_key.clone()], vec![first_key]),
        second_key.clone(),
        2,
        Arc::clone(&trace_value),
    );
    let third = {
        let descriptor = layer("third", vec![third_key], vec![second_key]);
        let trace_value = Arc::clone(&trace_value);
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                trace(&trace_value, 3);
                Err(RuntimeLayerFailure::new(
                    FAILURE_CONTRACT,
                    "unavailable",
                    "test constructor failed",
                ))
            }),
        }
    };

    let result =
        RuntimeComposition::acquire(vec![Box::new(third), Box::new(second), Box::new(first)]);
    let Err(error) = result else {
        panic!("the final constructor must fail");
    };

    assert!(matches!(
        error,
        CompositionError::AcquisitionFailed { ref layer_id, .. } if layer_id == "third"
    ));
    assert_eq!(trace_value.load(Ordering::SeqCst), 12_321);
}

#[test]
fn acquired_service_mismatch_releases_current_then_prior_layers() {
    let trace_value = Arc::new(AtomicU64::new(0));
    let store_key = service("store", "1");
    let worker_key = service("worker", "1");
    let wrong_key = service("wrong", "1");

    let store = {
        let descriptor = layer("store", vec![store_key.clone()], vec![]);
        let trace_value = Arc::clone(&trace_value);
        let store_key = store_key.clone();
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                trace(&trace_value, 1);
                let release_trace = Arc::clone(&trace_value);
                Ok(AcquiredRuntimeLayer::new(
                    vec![RuntimeServiceBinding::new(store_key.clone(), Arc::new(1))],
                    move || {
                        trace(&release_trace, 1);
                        Ok(())
                    },
                ))
            }),
        }
    };
    let worker = {
        let descriptor = layer("worker", vec![worker_key], vec![store_key]);
        let trace_value = Arc::clone(&trace_value);
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                trace(&trace_value, 2);
                let release_trace = Arc::clone(&trace_value);
                Ok(AcquiredRuntimeLayer::new(
                    vec![RuntimeServiceBinding::new(wrong_key.clone(), Arc::new(2))],
                    move || {
                        trace(&release_trace, 2);
                        Ok(())
                    },
                ))
            }),
        }
    };

    let result = RuntimeComposition::acquire(vec![Box::new(worker), Box::new(store)]);
    let Err(error) = result else {
        panic!("acquired services must match the descriptor exactly");
    };
    assert!(matches!(
        error,
        CompositionError::AcquiredServicesMismatch { ref layer_id, .. } if layer_id == "worker"
    ));
    assert_eq!(trace_value.load(Ordering::SeqCst), 1_221);
}

#[test]
fn acquired_service_multiplicity_must_match_the_descriptor() {
    let store_key = service("store", "1");
    let factory = {
        let descriptor = layer("store", vec![store_key.clone()], vec![]);
        TestFactory {
            descriptor,
            acquire: Box::new(move |_| {
                Ok(AcquiredRuntimeLayer::new(
                    vec![
                        RuntimeServiceBinding::new(store_key.clone(), Arc::new(1_u64)),
                        RuntimeServiceBinding::new(store_key.clone(), Arc::new(2_u64)),
                    ],
                    || Ok(()),
                ))
            }),
        }
    };

    let result = RuntimeComposition::acquire(vec![Box::new(factory)]);
    let Err(error) = result else {
        panic!("one declared service cannot produce multiple instances");
    };
    assert!(matches!(
        error,
        CompositionError::AcquiredServicesMismatch { ref layer_id, .. } if layer_id == "store"
    ));
}
