//! Provider-neutral runtime service composition.
//!
//! A composition graph binds abstract runtime service contracts to concrete
//! process-local implementations. It is deliberately separate from capability
//! advertisement, policy admission, and authority. A successful binding says
//! only which implementation realizes a service contract; it never grants a
//! caller permission to use that service.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
    sync::Arc,
};

use cymule_core::content_id;
use serde::{Deserialize, Serialize};

/// Frozen runtime-composition descriptor version.
pub const RUNTIME_COMPOSITION_VERSION: &str = "cymule.runtime-composition/1";

/// Domain separator for immutable binding-context identities.
pub const BINDING_CONTEXT_ID_DOMAIN: &str = "cymule.binding-context/1";

/// A versioned, provider-neutral runtime service contract.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceKey {
    /// Stable contract namespace.
    pub namespace: String,
    /// Stable service name inside the namespace.
    pub name: String,
    /// Exact API revision required by a consumer or provided by a layer.
    pub api_revision: String,
}

impl ServiceKey {
    /// Construct a service key.
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        api_revision: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            api_revision: api_revision.into(),
        }
    }

    fn validate(&self) -> Result<(), String> {
        validate_token("service namespace", &self.namespace)?;
        validate_token("service name", &self.name)?;
        validate_token("service API revision", &self.api_revision)
    }
}

impl Display for ServiceKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}/{}@{}",
            self.namespace, self.name, self.api_revision
        )
    }
}

/// Stable identity of one concrete service implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeImplementation {
    /// Provider-neutral implementation identity.
    pub implementation_id: String,
    /// Immutable implementation revision, build, or content identity.
    pub revision: String,
}

/// Process lifecycle described by every runtime layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLayerLifecycle {
    /// Acquire and release the implementation inside the current process.
    ProcessLocal,
}

/// Sharing boundary described by every runtime layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLayerShareScope {
    /// Acquire one instance and share it only inside this binding context.
    BindingContext,
}

/// Immutable description of one runtime service layer.
///
/// The descriptor intentionally contains only a configuration schema digest
/// and an irreversible configuration fingerprint. Concrete configuration and
/// credentials remain private inputs of the corresponding
/// [`RuntimeLayerFactory`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLayerDescriptor {
    /// Descriptor format version.
    pub version: String,
    /// Stable identity of this layer inside the composition graph.
    pub layer_id: String,
    /// Concrete implementation identity and immutable revision.
    pub implementation: RuntimeImplementation,
    /// Service contracts realized by this layer.
    pub provides: Vec<ServiceKey>,
    /// Exact service contracts required to construct this layer.
    pub requires: Vec<ServiceKey>,
    /// Stable schema or closed-contract identity for constructor failures.
    pub constructor_failure_contract: String,
    /// Digest of the non-secret configuration schema, never its values.
    pub configuration_schema_digest: String,
    /// Irreversible digest of canonical non-secret configuration identity and
    /// secret/version reference identity, never configuration or secret values.
    pub configuration_fingerprint: String,
    /// Resource lifetime owned by the runtime composition.
    pub lifecycle: RuntimeLayerLifecycle,
    /// Memoization boundary for the acquired instance.
    pub share_scope: RuntimeLayerShareScope,
}

impl RuntimeLayerDescriptor {
    fn normalize(mut self) -> Result<Self, CompositionError> {
        if self.version != RUNTIME_COMPOSITION_VERSION {
            return Err(CompositionError::InvalidDescriptor {
                layer_id: self.layer_id,
                reason: format!(
                    "unsupported descriptor version {}; expected {RUNTIME_COMPOSITION_VERSION}",
                    self.version
                ),
            });
        }
        let layer_id = self.layer_id.clone();
        validate_token("layer ID", &self.layer_id).map_err(|reason| {
            CompositionError::InvalidDescriptor {
                layer_id: layer_id.clone(),
                reason,
            }
        })?;
        validate_token("implementation ID", &self.implementation.implementation_id).map_err(
            |reason| CompositionError::InvalidDescriptor {
                layer_id: layer_id.clone(),
                reason,
            },
        )?;
        validate_token("implementation revision", &self.implementation.revision).map_err(
            |reason| CompositionError::InvalidDescriptor {
                layer_id: layer_id.clone(),
                reason,
            },
        )?;
        validate_token(
            "constructor failure contract",
            &self.constructor_failure_contract,
        )
        .map_err(|reason| CompositionError::InvalidDescriptor {
            layer_id: layer_id.clone(),
            reason,
        })?;
        validate_digest(&self.configuration_schema_digest).map_err(|reason| {
            CompositionError::InvalidDescriptor {
                layer_id: layer_id.clone(),
                reason: format!("configuration schema digest {reason}"),
            }
        })?;
        validate_digest(&self.configuration_fingerprint).map_err(|reason| {
            CompositionError::InvalidDescriptor {
                layer_id: layer_id.clone(),
                reason: format!("configuration fingerprint {reason}"),
            }
        })?;

        for service in self.provides.iter().chain(&self.requires) {
            service
                .validate()
                .map_err(|reason| CompositionError::InvalidDescriptor {
                    layer_id: layer_id.clone(),
                    reason,
                })?;
        }
        self.provides.sort();
        self.requires.sort();
        if first_duplicate(&self.provides).is_some() {
            return Err(CompositionError::InvalidDescriptor {
                layer_id,
                reason: "a service may be provided only once by a layer".to_owned(),
            });
        }
        if first_duplicate(&self.requires).is_some() {
            return Err(CompositionError::InvalidDescriptor {
                layer_id,
                reason: "a service may be required only once by a layer".to_owned(),
            });
        }
        Ok(self)
    }
}

/// One exact service-to-implementation binding in a binding context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceBindingDescriptor {
    /// Bound service contract.
    pub service: ServiceKey,
    /// Layer that realizes the service.
    pub layer_id: String,
    /// Exact implementation pinned for this binding.
    pub implementation: RuntimeImplementation,
}

/// Canonical, provider-neutral description of one complete runtime binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingContextDescriptor {
    /// Descriptor format version.
    pub version: String,
    /// Normalized layers in deterministic construction order.
    pub layers: Vec<RuntimeLayerDescriptor>,
    /// Deterministic provider-before-consumer construction order.
    pub topology: Vec<String>,
    /// Complete service realization table, sorted by [`ServiceKey`].
    pub bindings: Vec<ServiceBindingDescriptor>,
}

impl BindingContextDescriptor {
    /// Verify that topology, normalized layers, and service bindings are the
    /// unique canonical projection of the declared layers.
    ///
    /// # Errors
    ///
    /// Returns graph admission errors for invalid layers and
    /// [`CompositionError::NonCanonicalBindingDescriptor`] when a derived
    /// field was altered or serialized non-canonically.
    pub fn verify(&self) -> Result<(), CompositionError> {
        if self.version != RUNTIME_COMPOSITION_VERSION {
            return Err(CompositionError::NonCanonicalBindingDescriptor);
        }
        let rebuilt = RuntimeCompositionGraph::build(self.layers.clone())?;
        if rebuilt.descriptor != *self {
            return Err(CompositionError::NonCanonicalBindingDescriptor);
        }
        Ok(())
    }

    /// Compute the immutable identity stored as the core's opaque binding
    /// context string.
    ///
    /// # Errors
    ///
    /// Returns [`CompositionError::Encoding`] when canonical serialization
    /// fails, or descriptor verification errors when this value is not the
    /// canonical graph projection.
    pub fn binding_context_id(&self) -> Result<String, CompositionError> {
        self.verify()?;
        content_id(BINDING_CONTEXT_ID_DOMAIN, self)
            .map_err(|error| CompositionError::Encoding(error.to_string()))
    }
}

/// A validated runtime dependency graph and its canonical binding descriptor.
#[derive(Clone, Debug)]
pub struct RuntimeCompositionGraph {
    descriptor: BindingContextDescriptor,
}

impl RuntimeCompositionGraph {
    /// Validate and normalize runtime layer descriptors.
    ///
    /// # Errors
    ///
    /// Returns a deterministic admission error for an invalid descriptor,
    /// ambiguous provider, unsatisfied service revision, or dependency cycle.
    pub fn build(layers: Vec<RuntimeLayerDescriptor>) -> Result<Self, CompositionError> {
        let mut normalized = layers
            .into_iter()
            .map(RuntimeLayerDescriptor::normalize)
            .collect::<Result<Vec<_>, _>>()?;
        normalized.sort_by(|left, right| left.layer_id.cmp(&right.layer_id));

        if let Some(duplicate) = first_duplicate_by(&normalized, |layer| &layer.layer_id) {
            return Err(CompositionError::DuplicateLayer {
                layer_id: duplicate.layer_id.clone(),
            });
        }

        let (provider_by_service, logical_revisions) = index_providers(&normalized)?;
        let topology_indexes =
            deterministic_topology(&normalized, &provider_by_service, &logical_revisions)?;
        let layers = topology_indexes
            .into_iter()
            .map(|index| normalized[index].clone())
            .collect::<Vec<_>>();
        let topology = layers.iter().map(|layer| layer.layer_id.clone()).collect();
        let mut bindings = layers
            .iter()
            .flat_map(|layer| {
                layer
                    .provides
                    .iter()
                    .map(move |service| ServiceBindingDescriptor {
                        service: service.clone(),
                        layer_id: layer.layer_id.clone(),
                        implementation: layer.implementation.clone(),
                    })
            })
            .collect::<Vec<_>>();
        bindings.sort_by(|left, right| left.service.cmp(&right.service));

        Ok(Self {
            descriptor: BindingContextDescriptor {
                version: RUNTIME_COMPOSITION_VERSION.to_owned(),
                layers,
                topology,
                bindings,
            },
        })
    }

    /// Return the canonical binding descriptor.
    pub fn descriptor(&self) -> &BindingContextDescriptor {
        &self.descriptor
    }

    /// Compute the immutable binding-context identity.
    ///
    /// # Errors
    ///
    /// Returns [`CompositionError::Encoding`] when canonical serialization
    /// fails.
    pub fn binding_context_id(&self) -> Result<String, CompositionError> {
        self.descriptor.binding_context_id()
    }
}

/// Structured constructor or process-local finalizer failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeLayerFailure {
    /// Stable failure contract declared by the layer.
    pub contract: String,
    /// Stable machine-readable failure code.
    pub code: String,
    /// Human-readable diagnostic.
    pub message: String,
}

impl RuntimeLayerFailure {
    /// Construct a layer failure.
    pub fn new(
        contract: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            contract: contract.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

/// One type-erased, process-local service implementation.
#[derive(Clone)]
pub struct RuntimeServiceBinding {
    key: ServiceKey,
    service: Arc<dyn Any + Send + Sync>,
}

impl RuntimeServiceBinding {
    /// Bind a concrete service value to an exact service contract.
    pub fn new<T>(key: ServiceKey, service: Arc<T>) -> Self
    where
        T: Any + Send + Sync,
    {
        Self { key, service }
    }

    /// Return the bound service contract.
    pub fn key(&self) -> &ServiceKey {
        &self.key
    }
}

type RuntimeFinalizer = Box<dyn FnOnce() -> Result<(), RuntimeLayerFailure> + Send>;

/// Services and finalizer produced by one successful layer acquisition.
pub struct AcquiredRuntimeLayer {
    services: Vec<RuntimeServiceBinding>,
    finalizer: RuntimeFinalizer,
}

impl AcquiredRuntimeLayer {
    /// Construct one acquired layer.
    pub fn new<F>(services: Vec<RuntimeServiceBinding>, finalizer: F) -> Self
    where
        F: FnOnce() -> Result<(), RuntimeLayerFailure> + Send + 'static,
    {
        Self {
            services,
            finalizer: Box::new(finalizer),
        }
    }

    fn release(self) -> Result<(), RuntimeLayerFailure> {
        (self.finalizer)()
    }
}

/// Immutable view of services already constructed for a dependent layer.
#[derive(Clone, Default)]
pub struct RuntimeServices {
    services: BTreeMap<ServiceKey, Arc<dyn Any + Send + Sync>>,
}

impl RuntimeServices {
    /// Whether an exact service contract is bound.
    pub fn contains(&self, key: &ServiceKey) -> bool {
        self.services.contains_key(key)
    }

    /// Resolve and downcast one exact service contract.
    pub fn get<T>(&self, key: &ServiceKey) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        self.services
            .get(key)
            .cloned()
            .and_then(|service| service.downcast::<T>().ok())
    }
}

/// Process-local constructor for one declared runtime layer.
///
/// Provider configuration and credentials may be held by the factory but are
/// never serialized into its descriptor.
pub trait RuntimeLayerFactory {
    /// Return the immutable descriptor used for graph admission and binding.
    fn descriptor(&self) -> &RuntimeLayerDescriptor;

    /// Acquire the layer from already constructed dependencies.
    ///
    /// # Errors
    ///
    /// Returns a structured failure under the descriptor's declared
    /// `constructor_failure_contract` when construction cannot complete.
    fn acquire(
        &self,
        dependencies: &RuntimeServices,
    ) -> Result<AcquiredRuntimeLayer, RuntimeLayerFailure>;
}

struct AcquiredLayerRecord {
    layer_id: String,
    acquired: AcquiredRuntimeLayer,
}

/// One acquired binding context and its process-local services.
pub struct RuntimeComposition {
    descriptor: BindingContextDescriptor,
    binding_context_id: String,
    services: RuntimeServices,
    acquired: Vec<AcquiredLayerRecord>,
}

impl RuntimeComposition {
    /// Validate descriptors, acquire providers before consumers, and release
    /// all successful acquisitions in reverse order if any constructor fails.
    ///
    /// # Errors
    ///
    /// Returns graph admission or construction failure. When rollback
    /// finalizers also fail, the primary error and every cleanup failure are
    /// preserved in [`CompositionError::ConstructionRollbackFailed`].
    pub fn acquire(factories: Vec<Box<dyn RuntimeLayerFactory>>) -> Result<Self, CompositionError> {
        let graph = RuntimeCompositionGraph::build(
            factories
                .iter()
                .map(|factory| factory.descriptor().clone())
                .collect(),
        )?;
        let binding_context_id = graph.binding_context_id()?;
        let descriptor = graph.descriptor().clone();
        let mut factories = factories
            .into_iter()
            .map(|factory| (factory.descriptor().layer_id.clone(), factory))
            .collect::<BTreeMap<_, _>>();
        let mut services = RuntimeServices::default();
        let mut acquired = Vec::<AcquiredLayerRecord>::new();

        for layer in &descriptor.layers {
            let Some(factory) = factories.remove(&layer.layer_id) else {
                return Err(CompositionError::InvalidDescriptor {
                    layer_id: layer.layer_id.clone(),
                    reason: "validated layer has no matching factory".to_owned(),
                });
            };
            let result = factory.acquire(&services);
            let acquired_layer = match result {
                Ok(acquired_layer) => acquired_layer,
                Err(failure) => {
                    if failure.contract != layer.constructor_failure_contract {
                        let primary = CompositionError::InvalidConstructorFailure {
                            layer_id: layer.layer_id.clone(),
                            expected_contract: layer.constructor_failure_contract.clone(),
                            actual_contract: failure.contract,
                        };
                        return Err(with_cleanup(primary, release_reverse(&mut acquired)));
                    }
                    let primary = CompositionError::AcquisitionFailed {
                        layer_id: layer.layer_id.clone(),
                        failure,
                    };
                    return Err(with_cleanup(primary, release_reverse(&mut acquired)));
                }
            };

            let mut produced = acquired_layer
                .services
                .iter()
                .map(|binding| binding.key.clone())
                .collect::<Vec<_>>();
            produced.sort();
            let declared = layer.provides.clone();
            if produced != declared {
                let mut current = vec![AcquiredLayerRecord {
                    layer_id: layer.layer_id.clone(),
                    acquired: acquired_layer,
                }];
                let mut cleanup = release_reverse(&mut current);
                cleanup.extend(release_reverse(&mut acquired));
                let primary = CompositionError::AcquiredServicesMismatch {
                    layer_id: layer.layer_id.clone(),
                    declared,
                    produced,
                };
                return Err(with_cleanup(primary, cleanup));
            }

            for binding in &acquired_layer.services {
                services
                    .services
                    .insert(binding.key.clone(), Arc::clone(&binding.service));
            }
            acquired.push(AcquiredLayerRecord {
                layer_id: layer.layer_id.clone(),
                acquired: acquired_layer,
            });
        }

        Ok(Self {
            descriptor,
            binding_context_id,
            services,
            acquired,
        })
    }

    /// Return the immutable descriptor of this acquired binding context.
    pub fn descriptor(&self) -> &BindingContextDescriptor {
        &self.descriptor
    }

    /// Return the content-addressed opaque binding context identity.
    pub fn binding_context_id(&self) -> &str {
        &self.binding_context_id
    }

    /// Resolve one concrete process-local service.
    pub fn service<T>(&self, key: &ServiceKey) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        self.services.get(key)
    }

    /// Release every acquired layer in reverse construction order.
    ///
    /// # Errors
    ///
    /// Returns every process-local finalizer failure after attempting all
    /// finalizers in reverse construction order.
    pub fn shutdown(mut self) -> Result<(), CompositionError> {
        let failures = release_reverse(&mut self.acquired);
        if failures.is_empty() {
            Ok(())
        } else {
            Err(CompositionError::ReleaseFailed { failures })
        }
    }
}

impl Drop for RuntimeComposition {
    fn drop(&mut self) {
        let _ = release_reverse(&mut self.acquired);
    }
}

/// One process-local finalizer failure associated with its owning layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerReleaseFailure {
    /// Layer whose finalizer failed.
    pub layer_id: String,
    /// Structured finalizer failure.
    pub failure: RuntimeLayerFailure,
}

/// Runtime composition admission, construction, or shutdown error.
#[derive(Debug, PartialEq, Eq)]
pub enum CompositionError {
    /// A layer descriptor violates the frozen composition contract.
    InvalidDescriptor {
        /// Layer being validated.
        layer_id: String,
        /// Rejection reason.
        reason: String,
    },
    /// Two layers use the same graph identity.
    DuplicateLayer {
        /// Repeated layer identity.
        layer_id: String,
    },
    /// More than one layer realizes an exact service contract.
    DuplicateProvider {
        /// Ambiguous service.
        service: ServiceKey,
        /// Deterministically ordered conflicting layers.
        providers: Vec<String>,
    },
    /// No layer realizes a required service name.
    MissingRequirement {
        /// Requiring layer.
        consumer: String,
        /// Missing exact contract.
        required: ServiceKey,
    },
    /// A service name is available only at incompatible API revisions.
    RevisionMismatch {
        /// Requiring layer.
        consumer: String,
        /// Required exact contract.
        required: ServiceKey,
        /// Available revisions of the same logical service.
        available: Vec<ServiceKey>,
    },
    /// Runtime layer dependencies contain a cycle.
    DependencyCycle {
        /// Deterministically ordered layers participating in or downstream of
        /// the remaining cyclic subgraph.
        layers: Vec<String>,
    },
    /// A serialized binding descriptor is not the unique canonical graph
    /// projection of its layers.
    NonCanonicalBindingDescriptor,
    /// A constructor returned a failure outside its declared contract.
    InvalidConstructorFailure {
        /// Failing layer.
        layer_id: String,
        /// Descriptor-declared failure contract.
        expected_contract: String,
        /// Constructor-returned failure contract.
        actual_contract: String,
    },
    /// A layer constructor failed under its declared contract.
    AcquisitionFailed {
        /// Failing layer.
        layer_id: String,
        /// Structured constructor failure.
        failure: RuntimeLayerFailure,
    },
    /// A constructor produced a service set different from its descriptor.
    AcquiredServicesMismatch {
        /// Invalid layer.
        layer_id: String,
        /// Descriptor-declared service set.
        declared: Vec<ServiceKey>,
        /// Actually produced service set.
        produced: Vec<ServiceKey>,
    },
    /// A primary construction error plus failures while releasing prior layers.
    ConstructionRollbackFailed {
        /// Primary graph or construction failure.
        primary: Box<CompositionError>,
        /// Reverse-order release failures.
        cleanup_failures: Vec<LayerReleaseFailure>,
    },
    /// Explicit shutdown reported one or more reverse-order finalizer failures.
    ReleaseFailed {
        /// Reverse-order release failures.
        failures: Vec<LayerReleaseFailure>,
    },
    /// Canonical descriptor encoding failed.
    Encoding(String),
}

impl Display for CompositionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDescriptor { layer_id, reason } => {
                write!(formatter, "invalid runtime layer {layer_id}: {reason}")
            }
            Self::DuplicateLayer { layer_id } => {
                write!(formatter, "duplicate runtime layer {layer_id}")
            }
            Self::DuplicateProvider { service, providers } => write!(
                formatter,
                "service {service} has duplicate providers {}",
                providers.join(", ")
            ),
            Self::MissingRequirement { consumer, required } => {
                write!(
                    formatter,
                    "layer {consumer} requires missing service {required}"
                )
            }
            Self::RevisionMismatch {
                consumer,
                required,
                available,
            } => write!(
                formatter,
                "layer {consumer} requires {required}, but only {} is available",
                available
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::DependencyCycle { layers } => {
                write!(
                    formatter,
                    "runtime layer dependency cycle: {}",
                    layers.join(", ")
                )
            }
            Self::NonCanonicalBindingDescriptor => {
                formatter.write_str("binding context descriptor is not canonical")
            }
            Self::InvalidConstructorFailure {
                layer_id,
                expected_contract,
                actual_contract,
            } => write!(
                formatter,
                "layer {layer_id} returned failure contract {actual_contract}; expected {expected_contract}"
            ),
            Self::AcquisitionFailed { layer_id, failure } => write!(
                formatter,
                "layer {layer_id} acquisition failed with {}: {}",
                failure.code, failure.message
            ),
            Self::AcquiredServicesMismatch { layer_id, .. } => {
                write!(formatter, "layer {layer_id} returned undeclared services")
            }
            Self::ConstructionRollbackFailed {
                primary,
                cleanup_failures,
            } => write!(
                formatter,
                "{primary}; {} process-local finalizer(s) also failed",
                cleanup_failures.len()
            ),
            Self::ReleaseFailed { failures } => write!(
                formatter,
                "{} process-local runtime layer finalizer(s) failed",
                failures.len()
            ),
            Self::Encoding(message) => write!(formatter, "composition encoding failed: {message}"),
        }
    }
}

impl std::error::Error for CompositionError {}

type ProviderIndex = BTreeMap<ServiceKey, usize>;
type LogicalRevisionIndex = BTreeMap<(String, String), BTreeSet<ServiceKey>>;

fn index_providers(
    layers: &[RuntimeLayerDescriptor],
) -> Result<(ProviderIndex, LogicalRevisionIndex), CompositionError> {
    let mut providers = ProviderIndex::new();
    let mut revisions = LogicalRevisionIndex::new();
    for (layer_index, layer) in layers.iter().enumerate() {
        for service in &layer.provides {
            if let Some(existing_index) = providers.insert(service.clone(), layer_index) {
                return Err(CompositionError::DuplicateProvider {
                    service: service.clone(),
                    providers: vec![
                        layers[existing_index].layer_id.clone(),
                        layer.layer_id.clone(),
                    ],
                });
            }
            revisions
                .entry((service.namespace.clone(), service.name.clone()))
                .or_default()
                .insert(service.clone());
        }
    }
    Ok((providers, revisions))
}

fn deterministic_topology(
    layers: &[RuntimeLayerDescriptor],
    providers: &ProviderIndex,
    revisions: &LogicalRevisionIndex,
) -> Result<Vec<usize>, CompositionError> {
    let mut outgoing = vec![BTreeSet::<usize>::new(); layers.len()];
    let mut indegree = vec![0_usize; layers.len()];
    for (consumer_index, layer) in layers.iter().enumerate() {
        for requirement in &layer.requires {
            let Some(provider_index) = providers.get(requirement).copied() else {
                let available = revisions
                    .get(&(requirement.namespace.clone(), requirement.name.clone()))
                    .map(|services| services.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                if available.is_empty() {
                    return Err(CompositionError::MissingRequirement {
                        consumer: layer.layer_id.clone(),
                        required: requirement.clone(),
                    });
                }
                return Err(CompositionError::RevisionMismatch {
                    consumer: layer.layer_id.clone(),
                    required: requirement.clone(),
                    available,
                });
            };
            if outgoing[provider_index].insert(consumer_index) {
                indegree[consumer_index] += 1;
            }
        }
    }

    let mut ready = layers
        .iter()
        .enumerate()
        .filter_map(|(index, layer)| {
            (indegree[index] == 0).then_some((layer.layer_id.clone(), index))
        })
        .collect::<BTreeSet<_>>();
    let mut topology = Vec::with_capacity(layers.len());
    while let Some((_, layer_index)) = ready.pop_first() {
        topology.push(layer_index);
        for consumer_index in &outgoing[layer_index] {
            indegree[*consumer_index] -= 1;
            if indegree[*consumer_index] == 0 {
                ready.insert((layers[*consumer_index].layer_id.clone(), *consumer_index));
            }
        }
    }
    if topology.len() != layers.len() {
        let layers = layers
            .iter()
            .zip(indegree)
            .filter_map(|(layer, degree)| (degree > 0).then_some(layer.layer_id.clone()))
            .collect();
        return Err(CompositionError::DependencyCycle { layers });
    }
    Ok(topology)
}

fn validate_token(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{label} must not be empty"));
    }
    if value.len() > 256 {
        return Err(format!("{label} exceeds 256 bytes"));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(format!("{label} must not contain whitespace"));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), String> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err("must be a sha256 identity".to_owned());
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("must use 64 lowercase hex digits".to_owned());
    }
    Ok(())
}

fn first_duplicate<T: Ord>(values: &[T]) -> Option<&T> {
    values
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then_some(&pair[0]))
}

fn first_duplicate_by<'a, T, K, F>(values: &'a [T], key: F) -> Option<&'a T>
where
    K: PartialEq + ?Sized + 'a,
    F: Fn(&'a T) -> &'a K,
{
    values
        .windows(2)
        .find_map(|pair| (key(&pair[0]) == key(&pair[1])).then_some(&pair[0]))
}

fn release_reverse(acquired: &mut Vec<AcquiredLayerRecord>) -> Vec<LayerReleaseFailure> {
    let mut failures = Vec::new();
    while let Some(record) = acquired.pop() {
        if let Err(failure) = record.acquired.release() {
            failures.push(LayerReleaseFailure {
                layer_id: record.layer_id,
                failure,
            });
        }
    }
    failures
}

fn with_cleanup(
    primary: CompositionError,
    cleanup_failures: Vec<LayerReleaseFailure>,
) -> CompositionError {
    if cleanup_failures.is_empty() {
        primary
    } else {
        CompositionError::ConstructionRollbackFailed {
            primary: Box::new(primary),
            cleanup_failures,
        }
    }
}
