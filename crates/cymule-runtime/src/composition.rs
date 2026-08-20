//! Provider-neutral runtime binding admission.
//!
//! This module describes which concrete provider revisions can realize exact
//! runtime service contracts. It does not construct provider objects, manage
//! process resources, or grant authority. Provider adapters use ordinary Rust
//! ownership for their live objects. Capability manifests remain untrusted
//! advertisements; policy and authority admission remain separate gates.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
};

use cymule_core::content_id;
use serde::{Deserialize, Serialize};

/// Frozen runtime-binding descriptor version.
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
    /// Exact API revision required by a consumer or provided by a provider.
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

/// Stable identity of one concrete provider implementation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeImplementation {
    /// Provider-neutral implementation identity.
    pub implementation_id: String,
    /// Immutable implementation revision, build, or content identity.
    pub revision: String,
}

/// Immutable input describing one runtime provider.
///
/// Configuration and secret values are deliberately absent. The fingerprint
/// binds canonical non-secret configuration identity and secret/version
/// reference identity without disclosing either value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProviderDescriptor {
    /// Descriptor format version.
    pub version: String,
    /// Stable provider identity inside this binding context.
    pub provider_id: String,
    /// Concrete implementation identity and immutable revision.
    pub implementation: RuntimeImplementation,
    /// Exact service contracts realized by this provider.
    pub provides: Vec<ServiceKey>,
    /// Exact service contracts required by this provider.
    pub requires: Vec<ServiceKey>,
    /// Provider properties eligible for exact Plan requirement matching.
    pub properties: BTreeMap<String, String>,
    /// Digest of the provider configuration schema, never configuration values.
    pub configuration_schema_digest: String,
    /// Irreversible configuration and secret-reference identity digest.
    pub configuration_fingerprint: String,
}

impl RuntimeProviderDescriptor {
    fn normalize(mut self) -> Result<Self, CompositionError> {
        let provider_id = self.provider_id.clone();
        if self.version != RUNTIME_COMPOSITION_VERSION {
            return Err(CompositionError::InvalidProvider {
                provider_id,
                reason: format!(
                    "unsupported descriptor version {}; expected {RUNTIME_COMPOSITION_VERSION}",
                    self.version
                ),
            });
        }
        validate_token("provider ID", &self.provider_id)
            .and_then(|()| {
                validate_token("implementation ID", &self.implementation.implementation_id)
            })
            .and_then(|()| validate_token("implementation revision", &self.implementation.revision))
            .map_err(|reason| CompositionError::InvalidProvider {
                provider_id: provider_id.clone(),
                reason,
            })?;
        validate_digest(&self.configuration_schema_digest).map_err(|reason| {
            CompositionError::InvalidProvider {
                provider_id: provider_id.clone(),
                reason: format!("configuration schema digest {reason}"),
            }
        })?;
        validate_digest(&self.configuration_fingerprint).map_err(|reason| {
            CompositionError::InvalidProvider {
                provider_id: provider_id.clone(),
                reason: format!("configuration fingerprint {reason}"),
            }
        })?;
        for service in self.provides.iter().chain(&self.requires) {
            service
                .validate()
                .map_err(|reason| CompositionError::InvalidProvider {
                    provider_id: provider_id.clone(),
                    reason,
                })?;
        }
        validate_properties(&self.properties).map_err(|reason| {
            CompositionError::InvalidProvider {
                provider_id: provider_id.clone(),
                reason,
            }
        })?;
        self.provides.sort();
        self.requires.sort();
        if first_duplicate(&self.provides).is_some() {
            return Err(CompositionError::InvalidProvider {
                provider_id,
                reason: "a service may be provided only once by a provider".to_owned(),
            });
        }
        if first_duplicate(&self.requires).is_some() {
            return Err(CompositionError::InvalidProvider {
                provider_id,
                reason: "a service may be required only once by a provider".to_owned(),
            });
        }
        Ok(self)
    }
}

/// Canonical provider input for one complete runtime binding context.
///
/// Topology and service bindings are intentionally absent. They are derived by
/// [`RuntimeCompositionGraph`] and cannot become competing serialized state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingContextDescriptor {
    /// Descriptor format version.
    pub version: String,
    /// Normalized provider inputs sorted by provider identity.
    pub providers: Vec<RuntimeProviderDescriptor>,
}

impl BindingContextDescriptor {
    /// Verify that this value is the unique normalized provider input.
    ///
    /// # Errors
    ///
    /// Returns provider graph admission errors or
    /// [`CompositionError::NonCanonicalBindingDescriptor`] for a non-normalized
    /// serialized value.
    pub fn verify(&self) -> Result<(), CompositionError> {
        if self.version != RUNTIME_COMPOSITION_VERSION {
            return Err(CompositionError::NonCanonicalBindingDescriptor);
        }
        let rebuilt = RuntimeCompositionGraph::build(self.providers.clone())?;
        if rebuilt.descriptor != *self {
            return Err(CompositionError::NonCanonicalBindingDescriptor);
        }
        Ok(())
    }

    /// Compute the immutable identity stored as core's opaque binding string.
    ///
    /// # Errors
    ///
    /// Returns descriptor verification errors or
    /// [`CompositionError::Encoding`] when canonical serialization fails.
    pub fn binding_context_id(&self) -> Result<String, CompositionError> {
        self.verify()?;
        content_id(BINDING_CONTEXT_ID_DOMAIN, self)
            .map_err(|error| CompositionError::Encoding(error.to_string()))
    }
}

/// One derived service-to-provider binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceBindingDescriptor {
    /// Bound service contract.
    pub service: ServiceKey,
    /// Provider that realizes the service.
    pub provider_id: String,
    /// Exact implementation selected for this binding.
    pub implementation: RuntimeImplementation,
}

/// Successful technical match between Plan requirements and provider
/// properties.
///
/// This result proves binding eligibility only. It is not policy or authority
/// admission, even when a Plan uses requirement keys such as `capability` or
/// `authority`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequirementAdmission {
    /// Required service contract.
    pub service: ServiceKey,
    /// Eligible provider.
    pub provider_id: String,
    /// Exact eligible implementation.
    pub implementation: RuntimeImplementation,
}

/// Validated provider dependency graph plus read-only derived projections.
#[derive(Clone, Debug)]
pub struct RuntimeCompositionGraph {
    descriptor: BindingContextDescriptor,
    topology: Vec<String>,
    bindings: Vec<ServiceBindingDescriptor>,
}

impl RuntimeCompositionGraph {
    /// Validate and normalize provider descriptors.
    ///
    /// # Errors
    ///
    /// Returns deterministic admission errors for invalid providers, duplicate
    /// services, missing or mismatched service revisions, or dependency cycles.
    pub fn build(providers: Vec<RuntimeProviderDescriptor>) -> Result<Self, CompositionError> {
        let mut providers = providers
            .into_iter()
            .map(RuntimeProviderDescriptor::normalize)
            .collect::<Result<Vec<_>, _>>()?;
        providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
        if let Some(duplicate) = first_duplicate_by(&providers, |provider| &provider.provider_id) {
            return Err(CompositionError::DuplicateProviderId {
                provider_id: duplicate.provider_id.clone(),
            });
        }

        let (service_providers, revisions) = index_services(&providers)?;
        let topology_indexes = deterministic_topology(&providers, &service_providers, &revisions)?;
        let topology = topology_indexes
            .into_iter()
            .map(|index| providers[index].provider_id.clone())
            .collect();
        let mut bindings = providers
            .iter()
            .flat_map(|provider| {
                provider
                    .provides
                    .iter()
                    .map(move |service| ServiceBindingDescriptor {
                        service: service.clone(),
                        provider_id: provider.provider_id.clone(),
                        implementation: provider.implementation.clone(),
                    })
            })
            .collect::<Vec<_>>();
        bindings.sort_by(|left, right| left.service.cmp(&right.service));

        Ok(Self {
            descriptor: BindingContextDescriptor {
                version: RUNTIME_COMPOSITION_VERSION.to_owned(),
                providers,
            },
            topology,
            bindings,
        })
    }

    /// Return normalized provider input used for content identity.
    pub fn descriptor(&self) -> &BindingContextDescriptor {
        &self.descriptor
    }

    /// Return the derived provider-before-consumer order.
    pub fn topology(&self) -> &[String] {
        &self.topology
    }

    /// Return the derived service binding table sorted by service key.
    pub fn bindings(&self) -> &[ServiceBindingDescriptor] {
        &self.bindings
    }

    /// Compute the immutable binding-context identity.
    ///
    /// # Errors
    ///
    /// Returns canonical descriptor encoding errors.
    pub fn binding_context_id(&self) -> Result<String, CompositionError> {
        self.descriptor.binding_context_id()
    }

    /// Admit one existing Plan component/effect `requirements` map against the
    /// properties of the provider bound to an exact service contract.
    ///
    /// Matching is exact key-to-key and value-to-value. Empty, whitespace,
    /// non-lowercase, or punctuation-ambiguous keys are rejected rather than
    /// normalized. A successful result remains technical eligibility only;
    /// policy and authority must be admitted independently.
    ///
    /// # Errors
    ///
    /// Returns an error when the service is unbound, requirement syntax is
    /// invalid, or the bound provider lacks an exact property match.
    pub fn admit_plan_requirements(
        &self,
        service: &ServiceKey,
        requirements: &BTreeMap<String, String>,
    ) -> Result<RequirementAdmission, CompositionError> {
        validate_requirements(requirements)?;
        let binding = self
            .bindings
            .binary_search_by(|binding| binding.service.cmp(service))
            .ok()
            .map(|index| &self.bindings[index])
            .ok_or_else(|| CompositionError::UnboundService {
                service: service.clone(),
            })?;
        let provider = self
            .descriptor
            .providers
            .binary_search_by(|provider| provider.provider_id.cmp(&binding.provider_id))
            .ok()
            .map(|index| &self.descriptor.providers[index])
            .ok_or(CompositionError::NonCanonicalBindingDescriptor)?;
        for (key, required) in requirements {
            let Some(actual) = provider.properties.get(key) else {
                return Err(CompositionError::MissingProviderProperty {
                    provider_id: provider.provider_id.clone(),
                    key: key.clone(),
                });
            };
            if actual != required {
                return Err(CompositionError::ProviderPropertyMismatch {
                    provider_id: provider.provider_id.clone(),
                    key: key.clone(),
                    required: required.clone(),
                    actual: actual.clone(),
                });
            }
        }
        Ok(RequirementAdmission {
            service: service.clone(),
            provider_id: binding.provider_id.clone(),
            implementation: binding.implementation.clone(),
        })
    }
}

/// Runtime provider binding admission error.
#[derive(Debug, PartialEq, Eq)]
pub enum CompositionError {
    /// One provider descriptor is invalid.
    InvalidProvider {
        /// Invalid provider identity.
        provider_id: String,
        /// Rejection reason.
        reason: String,
    },
    /// Two provider descriptors use the same identity.
    DuplicateProviderId {
        /// Repeated provider identity.
        provider_id: String,
    },
    /// More than one provider realizes an exact service contract.
    DuplicateServiceProvider {
        /// Ambiguous service.
        service: ServiceKey,
        /// Deterministically ordered providers.
        providers: Vec<String>,
    },
    /// No provider realizes a required service name.
    MissingRequirement {
        /// Requiring provider.
        consumer: String,
        /// Missing exact service contract.
        required: ServiceKey,
    },
    /// A service name is available only at incompatible API revisions.
    RevisionMismatch {
        /// Requiring provider.
        consumer: String,
        /// Required exact service contract.
        required: ServiceKey,
        /// Available revisions of the same logical service.
        available: Vec<ServiceKey>,
    },
    /// Provider dependencies contain a cycle.
    DependencyCycle {
        /// Deterministically ordered providers in the remaining cyclic graph.
        providers: Vec<String>,
    },
    /// A serialized descriptor is not normalized canonical provider input.
    NonCanonicalBindingDescriptor,
    /// No provider binds the requested service contract.
    UnboundService {
        /// Requested service.
        service: ServiceKey,
    },
    /// A Plan requirement key or value is not exact and canonical.
    InvalidPlanRequirement {
        /// Invalid requirement key.
        key: String,
        /// Rejection reason.
        reason: String,
    },
    /// The selected provider does not advertise a required property.
    MissingProviderProperty {
        /// Selected provider.
        provider_id: String,
        /// Missing property key.
        key: String,
    },
    /// The selected provider property differs from the Plan requirement.
    ProviderPropertyMismatch {
        /// Selected provider.
        provider_id: String,
        /// Property key.
        key: String,
        /// Required exact value.
        required: String,
        /// Advertised exact value.
        actual: String,
    },
    /// Canonical descriptor encoding failed.
    Encoding(String),
}

impl Display for CompositionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProvider {
                provider_id,
                reason,
            } => write!(
                formatter,
                "invalid runtime provider {provider_id}: {reason}"
            ),
            Self::DuplicateProviderId { provider_id } => {
                write!(formatter, "duplicate runtime provider {provider_id}")
            }
            Self::DuplicateServiceProvider { service, providers } => write!(
                formatter,
                "service {service} has duplicate providers {}",
                providers.join(", ")
            ),
            Self::MissingRequirement { consumer, required } => {
                write!(
                    formatter,
                    "provider {consumer} requires missing service {required}"
                )
            }
            Self::RevisionMismatch {
                consumer,
                required,
                available,
            } => write!(
                formatter,
                "provider {consumer} requires {required}, but only {} is available",
                available
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::DependencyCycle { providers } => write!(
                formatter,
                "runtime provider dependency cycle: {}",
                providers.join(", ")
            ),
            Self::NonCanonicalBindingDescriptor => {
                formatter.write_str("binding context descriptor is not canonical")
            }
            Self::UnboundService { service } => {
                write!(formatter, "service {service} is not bound")
            }
            Self::InvalidPlanRequirement { key, reason } => {
                write!(formatter, "invalid Plan requirement {key:?}: {reason}")
            }
            Self::MissingProviderProperty { provider_id, key } => {
                write!(formatter, "provider {provider_id} lacks property {key}")
            }
            Self::ProviderPropertyMismatch {
                provider_id,
                key,
                required,
                actual,
            } => write!(
                formatter,
                "provider {provider_id} property {key} is {actual:?}; required {required:?}"
            ),
            Self::Encoding(message) => write!(formatter, "composition encoding failed: {message}"),
        }
    }
}

impl std::error::Error for CompositionError {}

type ServiceProviderIndex = BTreeMap<ServiceKey, usize>;
type LogicalRevisionIndex = BTreeMap<(String, String), BTreeSet<ServiceKey>>;

fn index_services(
    providers: &[RuntimeProviderDescriptor],
) -> Result<(ServiceProviderIndex, LogicalRevisionIndex), CompositionError> {
    let mut service_providers = ServiceProviderIndex::new();
    let mut revisions = LogicalRevisionIndex::new();
    for (provider_index, provider) in providers.iter().enumerate() {
        for service in &provider.provides {
            if let Some(existing) = service_providers.insert(service.clone(), provider_index) {
                return Err(CompositionError::DuplicateServiceProvider {
                    service: service.clone(),
                    providers: vec![
                        providers[existing].provider_id.clone(),
                        provider.provider_id.clone(),
                    ],
                });
            }
            revisions
                .entry((service.namespace.clone(), service.name.clone()))
                .or_default()
                .insert(service.clone());
        }
    }
    Ok((service_providers, revisions))
}

fn deterministic_topology(
    providers: &[RuntimeProviderDescriptor],
    service_providers: &ServiceProviderIndex,
    revisions: &LogicalRevisionIndex,
) -> Result<Vec<usize>, CompositionError> {
    let mut outgoing = vec![BTreeSet::<usize>::new(); providers.len()];
    let mut indegree = vec![0_usize; providers.len()];
    for (consumer_index, provider) in providers.iter().enumerate() {
        for requirement in &provider.requires {
            let Some(provider_index) = service_providers.get(requirement).copied() else {
                let available = revisions
                    .get(&(requirement.namespace.clone(), requirement.name.clone()))
                    .map(|services| services.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                if available.is_empty() {
                    return Err(CompositionError::MissingRequirement {
                        consumer: provider.provider_id.clone(),
                        required: requirement.clone(),
                    });
                }
                return Err(CompositionError::RevisionMismatch {
                    consumer: provider.provider_id.clone(),
                    required: requirement.clone(),
                    available,
                });
            };
            if outgoing[provider_index].insert(consumer_index) {
                indegree[consumer_index] += 1;
            }
        }
    }

    let mut ready = providers
        .iter()
        .enumerate()
        .filter_map(|(index, provider)| {
            (indegree[index] == 0).then_some((provider.provider_id.clone(), index))
        })
        .collect::<BTreeSet<_>>();
    let mut topology = Vec::with_capacity(providers.len());
    while let Some((_, provider_index)) = ready.pop_first() {
        topology.push(provider_index);
        for consumer_index in &outgoing[provider_index] {
            indegree[*consumer_index] -= 1;
            if indegree[*consumer_index] == 0 {
                ready.insert((
                    providers[*consumer_index].provider_id.clone(),
                    *consumer_index,
                ));
            }
        }
    }
    if topology.len() != providers.len() {
        let providers = providers
            .iter()
            .zip(indegree)
            .filter_map(|(provider, degree)| (degree > 0).then_some(provider.provider_id.clone()))
            .collect();
        return Err(CompositionError::DependencyCycle { providers });
    }
    Ok(topology)
}

fn validate_requirements(requirements: &BTreeMap<String, String>) -> Result<(), CompositionError> {
    for (key, value) in requirements {
        validate_property_key(key).map_err(|reason| CompositionError::InvalidPlanRequirement {
            key: key.clone(),
            reason,
        })?;
        if value.is_empty() {
            return Err(CompositionError::InvalidPlanRequirement {
                key: key.clone(),
                reason: "value must not be empty".to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_properties(properties: &BTreeMap<String, String>) -> Result<(), String> {
    for (key, value) in properties {
        validate_property_key(key)?;
        if value.is_empty() {
            return Err(format!("provider property {key} has an empty value"));
        }
    }
    Ok(())
}

fn validate_property_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("property key must not be empty".to_owned());
    }
    if key.len() > 128 {
        return Err("property key exceeds 128 bytes".to_owned());
    }
    let bytes = key.as_bytes();
    let valid_character = |byte: u8| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    };
    let separator = |byte: u8| matches!(byte, b'.' | b'_' | b'-');
    if !bytes.iter().copied().all(valid_character)
        || separator(bytes[0])
        || separator(bytes[bytes.len() - 1])
        || bytes
            .windows(2)
            .any(|pair| separator(pair[0]) && separator(pair[1]))
    {
        return Err(
            "property key must be canonical lowercase ASCII with unambiguous separators".to_owned(),
        );
    }
    Ok(())
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
