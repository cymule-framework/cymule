use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use serde::{Deserialize, Serialize};

use crate::RuntimeError;

/// Current Engine transport protocol.
pub const ENGINE_PROTOCOL_VERSION: &str = "cymule.engine/5";
/// Maximum UTF-8 bytes in one complete Engine request envelope.
pub const MAX_ENGINE_REQUEST_BYTES: usize = 64 * 1024 * 1024;
/// Exact compact framing bytes around the inner request in a request envelope.
pub const ENGINE_REQUEST_ENVELOPE_FRAMING_BYTES: usize = 48;
/// Maximum compact bytes in the normalized inner request echoed by success.
pub const MAX_ENGINE_REQUEST_ECHO_BYTES: usize =
    MAX_ENGINE_REQUEST_BYTES - ENGINE_REQUEST_ENVELOPE_FRAMING_BYTES;
/// Maximum compact JSON bytes in one closed Engine success response payload.
///
/// This equals the request bound because every currently admitted response is
/// independently bounded by the same semantic object ceilings as its request.
pub const MAX_ENGINE_RESPONSE_PAYLOAD_BYTES: usize = MAX_ENGINE_REQUEST_BYTES;
/// Exact compact framing bytes around request and response in a success envelope.
pub const ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES: usize = 80;
/// Maximum UTF-8 bytes in one complete compact Engine response envelope.
///
/// A success contains the accepted inner request plus one response payload.
/// Current compact request framing is 48 bytes and success framing is 80 bytes,
/// so the exact maximum is twice the request bound plus their 32-byte delta.
/// Failure envelopes are smaller.
pub const MAX_ENGINE_RESPONSE_BYTES: usize = MAX_ENGINE_REQUEST_ECHO_BYTES
    + MAX_ENGINE_RESPONSE_PAYLOAD_BYTES
    + ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES;
/// Maximum retained Engine stderr bytes.
///
/// Stderr is diagnostic-only and deliberately has a separate, narrower bound
/// than the semantic response envelope.
pub const MAX_ENGINE_DIAGNOSTIC_BYTES: usize = 1024 * 1024;

/// Official directory-store provider identity understood by the CLI Engine.
pub const ENGINE_DIRECTORY_STORE_PROVIDER: &str = "cymule.directory-store/5";
/// Official SQLite-store provider identity understood by the CLI Engine.
pub const ENGINE_SQLITE_STORE_PROVIDER: &str = "cymule.sqlite-store/6";
/// Official sealed process-executor provider identity understood by the CLI Engine.
pub const ENGINE_PROCESS_EXECUTOR_PROVIDER: &str = "cymule.executor-process/1";
/// Official restart-monotonic `SQLite` Clock provider target.
pub const ENGINE_CLOCK_SYSTEM_PROVIDER: &str = "cymule.clock-system/2";
/// Sealed process protocol used by migration and shadow plugins.
pub const EVOLUTION_PLUGIN_PROTOCOL_VERSION: &str = "cymule.evolution-plugin/3";
/// Exact raw request/response bound for the sealed evolution process protocol.
pub const EVOLUTION_PLUGIN_MESSAGE_LIMIT: usize = 16 * 1024 * 1024;
/// Maximum ordered process arguments admitted by Engine and executor paths.
pub const MAX_PROCESS_ARGUMENTS: usize = 4_096;
/// Maximum explicit process environment entries admitted by Engine and executor paths.
pub const MAX_PROCESS_ENVIRONMENT_ENTRIES: usize = 4_096;
/// Maximum frozen runtime-closure entries admitted by Engine and executor paths.
pub const MAX_PROCESS_RUNTIME_ENTRIES: usize = 4_096;
/// Maximum migration target bindings carried by one Evolution Engine request.
pub const MAX_EVOLUTION_TARGET_EXECUTION_BINDINGS: usize = 1;

/// Provider-neutral locator for one durable store instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineStoreTarget {
    /// Stable provider contract identity.
    pub provider: String,
    /// Provider-owned opaque location.
    pub location: String,
    /// Provider-owned domain when one physical store contains many domains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

impl EngineStoreTarget {
    /// Address one official directory-backed domain.
    pub fn directory(location: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_DIRECTORY_STORE_PROVIDER.to_owned(),
            location: location.into(),
            domain: None,
        }
    }

    /// Address one official SQLite-backed domain.
    pub fn sqlite(location: impl Into<String>, domain: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_SQLITE_STORE_PROVIDER.to_owned(),
            location: location.into(),
            domain: Some(domain.into()),
        }
    }

    /// Validate the transport-level locator independently of a provider.
    ///
    /// # Errors
    ///
    /// Returns a typed Engine validation failure when any locator field exceeds
    /// its closed Unicode-scalar bound.
    pub fn verify(&self) -> EngineResult<()> {
        verify_request_scalar_bound(
            &self.provider,
            256,
            "store provider",
            "invalid_store_target",
        )?;
        verify_request_scalar_bound(
            &self.location,
            4096,
            "store location",
            "invalid_store_target",
        )?;
        if let Some(domain) = &self.domain {
            verify_request_scalar_bound(domain, 512, "store domain", "invalid_store_target")?;
        }
        Ok(())
    }
}

/// Provider-neutral locator for one immutable execution implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnginePluginTarget {
    /// Stable provider contract identity.
    pub provider: String,
    /// Complete process realization configuration. These exact values are
    /// copied into the process executor and participate in its immutable
    /// implementation revision.
    pub process: EngineProcessConfig,
    /// Expected immutable revision of the complete process realization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

/// Complete closed configuration for one process-backed Engine provider.
///
/// The cancellation flag is deliberately absent because it is request-lifetime
/// authority rather than executable meaning. Every other process input is
/// explicit on the Engine wire and therefore available to request echo and
/// execution-binding admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineProcessConfig {
    /// Exact executable selected for capture by the process executor.
    pub executable: String,
    /// Exact ordered child argument vector.
    #[serde(deserialize_with = "deserialize_process_arguments")]
    pub arguments: Vec<String>,
    /// Complete child environment after ambient clearing.
    #[serde(deserialize_with = "deserialize_process_environment")]
    pub environment: BTreeMap<String, String>,
    /// Optional working-directory tree captured by the process executor.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub working_directory: Option<String>,
    /// Lowercase SHA-256 identities of frozen loaders, interpreters, ABI
    /// descriptors, and other runtime facilities outside the captured
    /// executable and working tree.
    #[serde(deserialize_with = "deserialize_process_runtime_closure")]
    pub runtime_closure: BTreeMap<String, String>,
    /// Complete invocation deadline in milliseconds.
    pub timeout_ms: u64,
    /// Maximum encoded request, stdout, or stderr bytes.
    pub message_limit: u64,
    /// Maximum complete length-prefixed execution-closure footprint.
    pub closure_limit: u64,
}

fn deserialize_process_arguments<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::composition::deserialize_bounded_vec::<D, String, MAX_PROCESS_ARGUMENTS>(
        deserializer,
        "process arguments",
    )
}

fn deserialize_process_environment<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::composition::deserialize_bounded_map::<D, String, String, MAX_PROCESS_ENVIRONMENT_ENTRIES>(
        deserializer,
        "process environment",
    )
}

fn deserialize_process_runtime_closure<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::composition::deserialize_bounded_map::<D, String, String, MAX_PROCESS_RUNTIME_ENTRIES>(
        deserializer,
        "process runtime closure",
    )
}

impl EngineProcessConfig {
    /// Validate all bounded process configuration before provider or Store I/O.
    ///
    /// # Errors
    ///
    /// Returns request validation failure when any required process input is
    /// missing, ambient, malformed, or outside its closed bound.
    pub fn verify(&self) -> EngineResult<()> {
        let invalid_key = |value: &str| {
            value.is_empty() || value.contains('=') || value.chars().any(char::is_control)
        };
        let invalid = !std::path::Path::new(&self.executable).is_absolute()
            || self.executable.contains('\0')
            || self.arguments.len() > MAX_PROCESS_ARGUMENTS
            || self.arguments.iter().any(|value| value.contains('\0'))
            || self.environment.len() > MAX_PROCESS_ENVIRONMENT_ENTRIES
            || self
                .environment
                .iter()
                .any(|(key, value)| invalid_key(key) || value.contains('\0'))
            || self.working_directory.as_deref().is_some_and(|value| {
                value.is_empty()
                    || value.contains('\0')
                    || !std::path::Path::new(value).is_absolute()
            })
            || self.runtime_closure.len() > MAX_PROCESS_RUNTIME_ENTRIES
            || self.runtime_closure.is_empty()
            || self
                .runtime_closure
                .iter()
                .any(|(key, value)| invalid_key(key) || !is_sha256_id(value))
            || self.timeout_ms == 0
            || self.timeout_ms > 9_007_199_254_740_991
            || self.message_limit == 0
            || self.message_limit > 64 * 1024 * 1024
            || self.closure_limit == 0
            || self.closure_limit > 1024 * 1024 * 1024;
        if invalid {
            return Err(invalid_request(
                "invalid_process_config",
                "process configuration is outside the closed bounded contract",
            ));
        }
        verify_request_scalar_bound(
            &self.executable,
            4096,
            "process executable",
            "invalid_process_config",
        )?;
        if let Some(working_directory) = &self.working_directory {
            verify_request_scalar_bound(
                working_directory,
                4096,
                "process working directory",
                "invalid_process_config",
            )?;
        }
        Ok(())
    }
}

/// Exact persistence-backed Clock authority selected for durable mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineClockTarget {
    /// Stable Clock provider contract.
    pub provider: String,
    /// Provider-owned receipt-ledger location.
    pub location: String,
    /// Stable source identity.
    pub source_id: String,
    /// Immutable implementation/configuration generation.
    pub source_generation: String,
}

impl EngineClockTarget {
    /// Select the official SQLite-backed Clock authority.
    pub fn sqlite(
        location: impl Into<String>,
        source_id: impl Into<String>,
        source_generation: impl Into<String>,
    ) -> Self {
        Self {
            provider: ENGINE_CLOCK_SYSTEM_PROVIDER.to_owned(),
            location: location.into(),
            source_id: source_id.into(),
            source_generation: source_generation.into(),
        }
    }

    /// Validate the exact provider locator and generation.
    ///
    /// # Errors
    ///
    /// Returns a typed Engine validation failure when a locator field is
    /// malformed, unbounded, or the generation is not an exact digest.
    pub fn verify(&self) -> EngineResult<()> {
        verify_request_scalar_bound(
            &self.provider,
            256,
            "Clock provider",
            "invalid_clock_target",
        )?;
        verify_request_scalar_bound(
            &self.location,
            4096,
            "Clock location",
            "invalid_clock_target",
        )?;
        verify_request_printable_scalar_bound(
            &self.source_id,
            512,
            "Clock source",
            "invalid_clock_target",
        )?;
        if !is_sha256_id(&self.source_generation) {
            return Err(invalid_request(
                "invalid_clock_generation",
                "Clock source generation must be a lowercase sha256 identity",
            ));
        }
        Ok(())
    }
}

impl EnginePluginTarget {
    /// Select the official sealed process executor.
    pub fn process(process: EngineProcessConfig) -> Self {
        Self {
            provider: ENGINE_PROCESS_EXECUTOR_PROVIDER.to_owned(),
            process,
            revision: None,
        }
    }

    /// Pin the complete process realization by its SHA-256 revision.
    pub fn pinned_process(process: EngineProcessConfig, revision: impl Into<String>) -> Self {
        Self {
            provider: ENGINE_PROCESS_EXECUTOR_PROVIDER.to_owned(),
            process,
            revision: Some(revision.into()),
        }
    }

    /// Validate the transport-level locator independently of a provider.
    ///
    /// # Errors
    ///
    /// Returns request validation failure when the provider, complete process
    /// configuration, or optional implementation revision is invalid.
    pub fn verify(&self) -> EngineResult<()> {
        self.verify_transport()?;
        if usize::try_from(self.process.message_limit).ok() != Some(crate::MAX_PLUGIN_MESSAGE_BYTES)
        {
            return Err(invalid_request(
                "plugin_message_limit_mismatch",
                format!(
                    "plugin process message limit must equal the plugin protocol's {} byte bound",
                    crate::MAX_PLUGIN_MESSAGE_BYTES
                ),
            ));
        }
        Ok(())
    }

    fn verify_transport(&self) -> EngineResult<()> {
        verify_request_scalar_bound(
            &self.provider,
            256,
            "plugin provider",
            "invalid_plugin_target",
        )?;
        self.process.verify()?;
        if let Some(revision) = &self.revision
            && !is_sha256_id(revision)
        {
            return Err(invalid_request(
                "invalid_plugin_revision",
                "plugin revision must be a lowercase sha256 identity",
            ));
        }
        Ok(())
    }
}

/// Complete provider selection for one durable Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineDurableTarget {
    /// Durable state provider.
    pub store: EngineStoreTarget,
    /// Execution provider. Read-only queries intentionally omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<EnginePluginTarget>,
    /// Clock authority. Read-only queries intentionally omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock: Option<EngineClockTarget>,
}

impl EngineDurableTarget {
    /// Construct a read-only target.
    pub fn query(store: EngineStoreTarget) -> Self {
        Self {
            store,
            executor: None,
            clock: None,
        }
    }

    /// Construct a mutation target.
    pub fn execute(
        store: EngineStoreTarget,
        executor: EnginePluginTarget,
        clock: EngineClockTarget,
    ) -> Self {
        Self {
            store,
            executor: Some(executor),
            clock: Some(clock),
        }
    }

    /// Construct a provider-only terminal Effect settlement target.
    pub fn resolve(store: EngineStoreTarget, executor: EnginePluginTarget) -> Self {
        Self {
            store,
            executor: Some(executor),
            clock: None,
        }
    }

    /// Validate all provider-neutral locators.
    ///
    /// # Errors
    ///
    /// Returns a typed Engine validation failure when the Store, executor, or
    /// Clock target is invalid.
    pub fn verify(&self) -> EngineResult<()> {
        self.store.verify()?;
        if let Some(executor) = &self.executor {
            executor.verify()?;
        }
        if let Some(clock) = &self.clock {
            clock.verify()?;
        }
        Ok(())
    }
}

/// Exact process-hosted migration adapter selected for one live-evolution request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineMigrationProviderTarget {
    /// Semantic adapter identity named by the migration command.
    pub adapter_id: String,
    /// Immutable semantic adapter revision named by the migration command.
    pub adapter_revision: String,
    /// Complete revision-pinned process realization.
    pub process: EnginePluginTarget,
}

impl EngineMigrationProviderTarget {
    /// Validate the exact semantic and process identities.
    ///
    /// # Errors
    ///
    /// Returns validation failure when an identity is malformed or the process
    /// realization is not pinned to the same immutable revision.
    pub fn verify(&self) -> EngineResult<()> {
        verify_evolution_provider_target(
            "migration adapter",
            &self.adapter_id,
            &self.adapter_revision,
            &self.process,
        )
    }
}

/// Exact process-hosted shadow driver selected for one live-evolution request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineShadowProviderTarget {
    /// Semantic driver identity named by the shadow command.
    pub driver_id: String,
    /// Immutable semantic driver revision named by the shadow command.
    pub driver_revision: String,
    /// Complete revision-pinned process realization.
    pub process: EnginePluginTarget,
}

impl EngineShadowProviderTarget {
    /// Validate the exact semantic and process identities.
    ///
    /// # Errors
    ///
    /// Returns validation failure when an identity is malformed or the process
    /// realization is not pinned to the same immutable revision.
    pub fn verify(&self) -> EngineResult<()> {
        verify_evolution_provider_target(
            "shadow driver",
            &self.driver_id,
            &self.driver_revision,
            &self.process,
        )
    }
}

/// Complete bounded provider selection for one live-evolution Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineEvolutionTarget {
    /// Durable state provider shared with the M4 persistence authority.
    pub store: EngineStoreTarget,
    /// Exact target Plan to ordinary process-provider binding registry. Required
    /// on wire and empty for commands that do not migrate a Run.
    #[serde(deserialize_with = "deserialize_evolution_target_bindings")]
    pub target_execution_bindings: BTreeMap<String, EnginePluginTarget>,
    /// Exact migration adapter, required on wire as null when unused.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub migration_adapter: Option<EngineMigrationProviderTarget>,
    /// Exact shadow driver, required on wire as null when unused.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub shadow_driver: Option<EngineShadowProviderTarget>,
}

impl EngineEvolutionTarget {
    /// Construct a provider-free Evolution target for commands that invoke no provider.
    pub fn new(store: EngineStoreTarget) -> Self {
        Self {
            store,
            target_execution_bindings: BTreeMap::new(),
            migration_adapter: None,
            shadow_driver: None,
        }
    }

    /// Validate every configured locator and immutable provider revision.
    ///
    /// # Errors
    ///
    /// Returns a typed Engine validation failure when the Store or either
    /// provider target is outside the closed contract.
    pub fn verify(&self) -> EngineResult<()> {
        self.store.verify()?;
        if self.target_execution_bindings.len() > MAX_EVOLUTION_TARGET_EXECUTION_BINDINGS {
            return Err(invalid_request(
                "evolution_target_binding_limit_exceeded",
                format!(
                    "Evolution target may carry at most {MAX_EVOLUTION_TARGET_EXECUTION_BINDINGS} target execution binding"
                ),
            ));
        }
        for (plan_id, target) in &self.target_execution_bindings {
            if !is_sha256_id(plan_id) {
                return Err(invalid_request(
                    "invalid_evolution_target_plan",
                    "Evolution target execution binding key must be a lowercase Plan SHA-256 identity",
                ));
            }
            target.verify()?;
            if target.revision.is_none() {
                return Err(invalid_request(
                    "unpinned_evolution_target_binding",
                    "Evolution target execution binding process must carry its exact revision",
                ));
            }
        }
        if let Some(target) = &self.migration_adapter {
            target.verify()?;
        }
        if let Some(target) = &self.shadow_driver {
            target.verify()?;
        }
        Ok(())
    }
}

fn deserialize_evolution_target_bindings<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, EnginePluginTarget>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::composition::deserialize_bounded_map::<
        D,
        String,
        EnginePluginTarget,
        MAX_EVOLUTION_TARGET_EXECUTION_BINDINGS,
    >(deserializer, "Evolution target execution bindings")
}

fn verify_evolution_provider_target(
    kind: &str,
    semantic_id: &str,
    semantic_revision: &str,
    process: &EnginePluginTarget,
) -> EngineResult<()> {
    verify_request_printable_scalar_bound(
        semantic_id,
        256,
        kind,
        "invalid_evolution_provider_target",
    )?;
    if !is_sha256_id(semantic_revision) {
        return Err(invalid_request(
            "invalid_evolution_provider_revision",
            format!("{kind} revision must be a lowercase sha256 identity"),
        ));
    }
    process.verify_transport()?;
    if usize::try_from(process.process.message_limit).ok() != Some(EVOLUTION_PLUGIN_MESSAGE_LIMIT) {
        return Err(invalid_request(
            "evolution_provider_message_limit_mismatch",
            format!(
                "{kind} process message limit must equal the evolution protocol's {EVOLUTION_PLUGIN_MESSAGE_LIMIT} byte bound"
            ),
        ));
    }
    if process.revision.as_deref() != Some(semantic_revision) {
        return Err(invalid_request(
            "evolution_provider_revision_mismatch",
            format!("{kind} process revision must equal its semantic revision"),
        ));
    }
    Ok(())
}

/// One versioned Engine request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRequestEnvelope<T> {
    /// Frozen transport protocol version.
    pub engine_protocol: String,
    /// Closed operation payload.
    pub request: T,
}

impl<T> EngineRequestEnvelope<T> {
    /// Wrap an operation in the current Engine protocol.
    pub fn new(request: T) -> Self {
        Self {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            request,
        }
    }
}

/// One versioned Engine response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EngineResponseEnvelope<Q, T> {
    /// The operation completed and returned its typed value.
    Success {
        /// Frozen transport protocol version.
        engine_protocol: String,
        /// Complete strictly decoded request that produced this response.
        request: Q,
        /// Closed success payload.
        response: T,
    },
    /// The operation reached a classified terminal failure.
    Failure {
        /// Frozen transport protocol version.
        engine_protocol: String,
        /// Structured failure.
        error: EngineFailure,
    },
}

impl<Q, T> EngineResponseEnvelope<Q, T> {
    /// Construct a successful response in the current protocol.
    pub fn success(request: Q, response: T) -> Self {
        Self::Success {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            request,
            response,
        }
    }

    /// Construct a failed response in the current protocol.
    pub fn failure(error: EngineFailure) -> Self {
        Self::Failure {
            engine_protocol: ENGINE_PROTOCOL_VERSION.to_owned(),
            error,
        }
    }

    /// Verify the protocol version and return the operation result.
    ///
    /// # Errors
    ///
    /// Returns the verified Engine failure carried by a failure envelope, or a
    /// contract failure when the envelope version or error payload is invalid.
    pub fn into_result(self) -> EngineResult<(Q, T)> {
        match self {
            Self::Success {
                engine_protocol,
                request,
                response,
            } => {
                verify_protocol_version(&engine_protocol)?;
                Ok((request, response))
            }
            Self::Failure {
                engine_protocol,
                error,
            } => {
                verify_protocol_version(&engine_protocol)?;
                error.verify()?;
                Err(error)
            }
        }
    }
}

/// Result returned by Engine transports.
pub type EngineResult<T> = std::result::Result<T, EngineFailure>;

/// Stable, closed failure categories shared by every SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineFailureCategory {
    /// The Engine process or transport could not carry a response.
    TransportFailure,
    /// Input failed structural or semantic validation.
    Validation,
    /// A declared input, output, or schema contract was violated.
    ContractViolation,
    /// Policy, capability, authority, or transition admission failed.
    AdmissionDenied,
    /// The request conflicts with durable or immutable history.
    Conflict,
    /// A referenced object does not exist.
    NotFound,
    /// A plugin returned a declared application failure.
    ExpectedPluginFailure,
    /// A plugin violated its protocol or terminated defectively.
    PluginDefect,
    /// A concrete process, store, network, or other substrate failed.
    SubstrateFailure,
    /// The operation was explicitly cancelled.
    Cancelled,
    /// The operation exceeded its admitted deadline.
    TimedOut,
    /// The external world may have changed but no authoritative outcome exists.
    UnknownWorldOutcome,
}

/// Stable Engine processing phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnginePhase {
    /// Starting, writing to, reading from, or decoding the Engine transport.
    Transport,
    /// Decoding the versioned request envelope.
    DecodeRequest,
    /// Validating the request envelope and selected operation.
    ValidateRequest,
    /// Sealing a Plan Candidate.
    SealPlan,
    /// Verifying a Sealed Plan.
    VerifyPlan,
    /// Sealing a Resource Candidate.
    SealResource,
    /// Verifying a wait activation.
    VerifyWaitActivation,
    /// Verifying a durable command.
    VerifyDurableCommand,
    /// Issuing one retained logical Clock observation.
    ObserveClock,
    /// Verifying an evolution command.
    VerifyEvolutionCommand,
    /// Verifying a unified live-evolution command.
    VerifyLiveEvolutionCommand,
    /// Executing a Plan.
    ExecutePlan,
    /// Executing one stateful durable command.
    ExecuteDurable,
    /// Executing one stateful live-evolution command through Durable authority.
    ExecuteLiveEvolution,
    /// Describing a selected plugin.
    PluginDescribe,
    /// Calling a component plugin.
    PluginCall,
    /// Preparing an external effect.
    EffectPrepare,
    /// Dispatching an external effect.
    EffectDispatch,
    /// Reconciling an ambiguous external effect.
    EffectReconcile,
    /// Encoding the response envelope.
    EncodeResponse,
}

/// Side of a declared contract that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineContractSide {
    /// The schema declaration itself is invalid.
    Schema,
    /// A value supplied to an operation is invalid.
    Input,
    /// A value returned by an operation is invalid.
    Output,
}

/// A stable retry or recovery disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineRetryDisposition {
    /// Repeating the request cannot make progress without a semantic change.
    Never,
    /// Correct the request or contract, then submit a new semantic request.
    CorrectAndRetry,
    /// Refresh the causal precondition, then retry the same semantic intent.
    RefreshAndRetry,
    /// Retry the identical request and identity after substrate recovery.
    RetrySameRequest,
    /// Reconcile the original external intent before any further dispatch.
    Reconcile,
}

/// One machine-readable validation or contract issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineIssue {
    /// Stable issue code.
    pub code: Box<str>,
    /// Human-readable issue summary.
    pub message: Box<str>,
    /// JSON Pointer relative to the selected contract side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Box<str>>,
    /// JSON Pointer to the failing schema keyword for contract issues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_path: Option<Box<str>>,
}

/// One structured Engine failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineFailure {
    /// Stable semantic category.
    pub category: EngineFailureCategory,
    /// Processing phase in which the failure became authoritative.
    pub phase: EnginePhase,
    /// Stable implementation-independent error code.
    pub code: Box<str>,
    /// Human-readable summary that is not used for control flow.
    pub message: Box<str>,
    /// Declared contract identity, when the failure is contract-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<Box<str>>,
    /// Contract side, when the failure is contract-specific.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_side: Option<EngineContractSide>,
    /// Primary JSON Pointer, when one location is authoritative.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<Box<str>>,
    /// Complete bounded issue set.
    #[serde(
        default,
        deserialize_with = "deserialize_engine_issues",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub issues: Vec<EngineIssue>,
    /// Permitted recovery behavior, when the Engine can state it safely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_disposition: Option<EngineRetryDisposition>,
}

fn deserialize_engine_issues<'de, D>(deserializer: D) -> Result<Vec<EngineIssue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::composition::deserialize_bounded_vec::<D, EngineIssue, { crate::MAX_CONTRACT_ISSUES }>(
        deserializer,
        "Engine failure issues",
    )
}

impl EngineFailure {
    /// Construct a failure without optional contract detail.
    pub fn new(
        category: EngineFailureCategory,
        phase: EnginePhase,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            phase,
            code: code.into().into_boxed_str(),
            message: message.into().into_boxed_str(),
            contract: None,
            contract_side: None,
            path: None,
            issues: Vec::new(),
            retry_disposition: None,
        }
    }

    /// Construct a transport failure for a client that could not obtain an envelope.
    pub fn transport(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            EngineFailureCategory::TransportFailure,
            EnginePhase::Transport,
            code,
            message,
        )
    }

    /// Verify all bounded wire fields after deserialization.
    ///
    /// # Errors
    ///
    /// Returns a typed contract failure when a field, issue tree, or retry
    /// disposition is outside the closed Engine failure domain.
    pub fn verify(&self) -> EngineResult<()> {
        verify_code(&self.code, 200, "failure code")?;
        verify_non_empty_scalar_bound(&self.message, 8192, "failure message")?;
        if let Some(contract) = &self.contract {
            verify_non_empty_scalar_bound(contract, 500, "contract")?;
        }
        if let Some(path) = &self.path {
            verify_path(path, "failure path")?;
        }
        if self.issues.len() > crate::MAX_CONTRACT_ISSUES {
            return Err(invalid_failure(format!(
                "failure issues exceed {} entries",
                crate::MAX_CONTRACT_ISSUES
            )));
        }
        for issue in &self.issues {
            verify_non_empty_scalar_bound(&issue.code, 200, "issue code")?;
            verify_non_empty_scalar_bound(
                &issue.message,
                crate::MAX_CONTRACT_MESSAGE_SCALARS,
                "issue message",
            )?;
            if let Some(path) = &issue.path {
                verify_path(path, "issue path")?;
            }
            if let Some(path) = &issue.schema_path {
                verify_path(path, "issue schema path")?;
            }
        }
        let valid_retry = matches!(
            (self.category, self.retry_disposition),
            (
                EngineFailureCategory::TransportFailure | EngineFailureCategory::NotFound,
                None
            ) | (
                EngineFailureCategory::Validation,
                Some(EngineRetryDisposition::CorrectAndRetry)
            ) | (
                EngineFailureCategory::ContractViolation | EngineFailureCategory::AdmissionDenied,
                Some(EngineRetryDisposition::CorrectAndRetry | EngineRetryDisposition::Never)
            ) | (
                EngineFailureCategory::Conflict,
                Some(EngineRetryDisposition::RefreshAndRetry | EngineRetryDisposition::Never)
            ) | (
                EngineFailureCategory::ExpectedPluginFailure
                    | EngineFailureCategory::PluginDefect
                    | EngineFailureCategory::Cancelled,
                Some(EngineRetryDisposition::Never)
            ) | (
                EngineFailureCategory::SubstrateFailure,
                Some(EngineRetryDisposition::RetrySameRequest)
            ) | (
                EngineFailureCategory::TimedOut,
                Some(
                    EngineRetryDisposition::RetrySameRequest
                        | EngineRetryDisposition::RefreshAndRetry
                )
            ) | (
                EngineFailureCategory::UnknownWorldOutcome,
                Some(EngineRetryDisposition::Reconcile)
            )
        );
        if !valid_retry {
            return Err(invalid_failure(format!(
                "failure category {:?} is incompatible with retry disposition {:?}",
                self.category, self.retry_disposition
            )));
        }
        Ok(())
    }

    /// Return one failure that is guaranteed to satisfy the v4 wire contract.
    ///
    /// All semantic error projection funnels through this terminal guard before
    /// stdout serialization. An internal projection defect is itself represented
    /// by one valid v4 failure rather than dropping the response and falling back
    /// to an unversioned stderr-only diagnostic.
    #[must_use]
    pub fn into_wire_failure(self) -> Self {
        if self.verify().is_ok() {
            return self;
        }
        let mut failure = Self::new(
            EngineFailureCategory::PluginDefect,
            EnginePhase::Transport,
            "engine_failure_projection_invalid",
            "the Engine could not project its internal failure into cymule.engine/5",
        );
        failure.retry_disposition = Some(EngineRetryDisposition::Never);
        failure
    }

    /// Project a trusted-kernel error at an exact Engine boundary.
    pub fn from_core(error: &cymule_core::CoreError, phase: EnginePhase) -> Self {
        use cymule_core::CoreError;

        let (category, retry_disposition) = match &error {
            CoreError::CollectionProviderFailure(failure) => {
                return Self::from_collection_provider(failure, phase);
            }
            CoreError::Validation(_) => (
                EngineFailureCategory::Validation,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            CoreError::NotFound(_) => (EngineFailureCategory::NotFound, None),
            CoreError::IdentityMismatch(_)
            | CoreError::Causal(_)
            | CoreError::PinnedReadSetIncomplete { .. }
            | CoreError::Encoding(_) => (
                EngineFailureCategory::ContractViolation,
                Some(EngineRetryDisposition::Never),
            ),
            // Illegal transitions require a different admissible command;
            // archived commands require archive-aware replay proof. Retrying
            // either unchanged hot request cannot help.
            CoreError::IllegalTransition(_)
            | CoreError::PagedScopeRequired { .. }
            | CoreError::ArchivedCommandReplayRequired { .. } => (
                EngineFailureCategory::AdmissionDenied,
                Some(EngineRetryDisposition::CorrectAndRetry),
            ),
            CoreError::CommandReuse(_) => (
                EngineFailureCategory::Conflict,
                Some(EngineRetryDisposition::Never),
            ),
        };
        let mut failure = Self::new(category, phase, error.code(), error.to_string());
        failure.retry_disposition = retry_disposition;
        failure
    }

    fn from_collection_provider(
        failure: &cymule_authenticated_collections::ProviderFailure,
        phase: EnginePhase,
    ) -> Self {
        use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};
        let (category, code, message, retry) = match failure {
            ProviderFailure::Validation { message } => (
                EngineFailureCategory::Validation,
                "collection_provider_validation_failed",
                message.clone(),
                EngineRetryDisposition::CorrectAndRetry,
            ),
            ProviderFailure::Integrity { code, message } => (
                EngineFailureCategory::ContractViolation,
                code.as_str(),
                message.clone(),
                EngineRetryDisposition::Never,
            ),
            ProviderFailure::Conflict {
                evidence: ProviderConflict::Revision { expected, current },
            } => (
                EngineFailureCategory::Conflict,
                "revision_conflict",
                format!(
                    "collection provider revision changed: expected {expected:?}, current {current:?}"
                ),
                EngineRetryDisposition::RefreshAndRetry,
            ),
            ProviderFailure::Conflict {
                evidence: ProviderConflict::History { code, message },
            } => (
                EngineFailureCategory::Conflict,
                code.as_str(),
                message.clone(),
                EngineRetryDisposition::Never,
            ),
            ProviderFailure::Substrate { code, message } => (
                EngineFailureCategory::SubstrateFailure,
                code.as_str(),
                message.clone(),
                EngineRetryDisposition::RetrySameRequest,
            ),
        };
        let mut failure = Self::new(category, phase, code, message);
        failure.retry_disposition = Some(retry);
        failure
    }

    /// Project an embedded-runtime error at an exact Engine boundary.
    pub fn from_runtime(error: RuntimeError, phase: EnginePhase) -> Self {
        match error {
            RuntimeError::Core(error) => Self::from_core(&error, phase),
            RuntimeError::Contract(error) => Self::from_contract_violation(&error, phase),
            RuntimeError::Composition(error) => {
                let mut failure = Self::new(
                    EngineFailureCategory::PluginDefect,
                    phase,
                    error.code(),
                    error.message(),
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::ExpectedPluginFailure(error) => {
                let mut failure = Self::new(
                    EngineFailureCategory::ExpectedPluginFailure,
                    phase,
                    error.code,
                    error.message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::PluginDefect { code, message } => {
                let mut failure =
                    Self::new(EngineFailureCategory::PluginDefect, phase, code, message);
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::Suspended(boundary) => {
                let mut failure = Self::new(
                    EngineFailureCategory::AdmissionDenied,
                    phase,
                    "embedded_profile_suspended",
                    format!(
                        "wait site {} reached binding {:?}",
                        boundary.site_id, boundary.result_bind
                    ),
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::ReleaseRequired { intent_ids } => {
                let mut failure = Self::new(
                    EngineFailureCategory::AdmissionDenied,
                    phase,
                    "effect_release_required",
                    format!(
                        "prepared explicit effects require durable caller release: {}",
                        intent_ids.join(",")
                    ),
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::Substrate { code, message } => {
                let mut failure = Self::new(
                    EngineFailureCategory::SubstrateFailure,
                    phase,
                    code,
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
                failure
            }
            RuntimeError::Cancelled { code, message } => {
                let mut failure = Self::new(EngineFailureCategory::Cancelled, phase, code, message);
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
            RuntimeError::TimedOut { code, message } => {
                let mut failure = Self::new(EngineFailureCategory::TimedOut, phase, code, message);
                failure.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
                failure
            }
            RuntimeError::UnknownWorld { code, message } => {
                let mut failure = Self::new(
                    EngineFailureCategory::UnknownWorldOutcome,
                    phase,
                    code,
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Reconcile);
                failure
            }
            RuntimeError::Encoding(message) => {
                let mut failure = Self::new(
                    EngineFailureCategory::ContractViolation,
                    phase,
                    "runtime_encoding_failed",
                    message,
                );
                failure.retry_disposition = Some(EngineRetryDisposition::Never);
                failure
            }
        }
    }

    /// Project an executable Plan contract violation without flattening its
    /// boundary, side, or issue paths.
    pub fn from_contract_violation(error: &crate::ContractViolation, phase: EnginePhase) -> Self {
        use crate::{ContractBoundary, ContractIssueKind, ContractPhase, ContractSide};

        if error.verify().is_err() {
            let mut failure = Self::new(
                EngineFailureCategory::PluginDefect,
                phase,
                "invalid_contract_violation",
                "the runtime produced a contract violation outside its closed bounded authority",
            );
            failure.retry_disposition = Some(EngineRetryDisposition::Never);
            return failure;
        }

        let contract_kind = match error.target.boundary {
            ContractBoundary::Definition => "definition",
            ContractBoundary::Component => "component",
            ContractBoundary::Effect => "effect",
            ContractBoundary::Wait => "wait",
        };
        let contract_side = match error.phase {
            ContractPhase::Admission => EngineContractSide::Schema,
            ContractPhase::Execution => match error.target.side {
                ContractSide::Input => EngineContractSide::Input,
                ContractSide::Output => EngineContractSide::Output,
            },
        };
        let code = match error.phase {
            ContractPhase::Admission => "invalid_contract_schema",
            ContractPhase::Execution => "contract_value_mismatch",
        };
        let mut failure = Self::new(
            EngineFailureCategory::ContractViolation,
            phase,
            code,
            format!(
                "{contract_kind} {:?} {:?} contract rejected {} issue(s)",
                error.target.id,
                contract_side,
                error.issues.len()
            ),
        );
        failure.contract = Some(format!("{contract_kind}:{}", error.target.id).into_boxed_str());
        failure.contract_side = Some(contract_side);
        let issues = error
            .issues
            .iter()
            .map(|issue| EngineIssue {
                code: match issue.kind {
                    ContractIssueKind::Validation => code,
                    ContractIssueKind::Omitted => "contract_issues_omitted",
                }
                .into(),
                message: issue.message.clone().into_boxed_str(),
                path: Some(issue.instance_path.clone().into_boxed_str()),
                schema_path: Some(issue.schema_path.clone().into_boxed_str()),
            })
            .collect::<Vec<_>>();
        failure.path = issues.first().and_then(|issue| issue.path.clone());
        failure.issues = issues;
        failure.retry_disposition = Some(match error.phase {
            ContractPhase::Admission | ContractPhase::Execution => {
                EngineRetryDisposition::CorrectAndRetry
            }
        });
        failure
    }
}

impl Display for EngineFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for EngineFailure {}

fn verify_protocol_version(version: &str) -> EngineResult<()> {
    if version == ENGINE_PROTOCOL_VERSION {
        return Ok(());
    }
    let mut failure = EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "unsupported_engine_protocol",
        format!("expected {ENGINE_PROTOCOL_VERSION}, received {version:?}"),
    );
    failure.contract = Some(ENGINE_PROTOCOL_VERSION.into());
    failure.contract_side = Some(EngineContractSide::Schema);
    failure.retry_disposition = Some(EngineRetryDisposition::Never);
    Err(failure)
}

fn is_sha256_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn verify_code(value: &str, max: usize, label: &str) -> EngineResult<()> {
    verify_non_empty_scalar_bound(value, max, label)?;
    if !value.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() && index > 0 || byte == b'_'
    }) || !value.as_bytes()[0].is_ascii_lowercase()
    {
        return Err(invalid_failure(format!(
            "{label} must match ^[a-z][a-z0-9_]*$"
        )));
    }
    Ok(())
}

fn verify_non_empty_scalar_bound(value: &str, max: usize, label: &str) -> EngineResult<()> {
    if value.is_empty() || value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(invalid_failure(format!(
            "{label} must contain 1 to {max} non-control Unicode scalar values"
        )));
    }
    Ok(())
}

fn verify_request_scalar_bound(
    value: &str,
    max: usize,
    label: &str,
    code: &'static str,
) -> EngineResult<()> {
    if value.is_empty() || value.chars().count() > max {
        return Err(invalid_request(
            code,
            format!("{label} must contain 1 to {max} Unicode scalar values"),
        ));
    }
    Ok(())
}

fn verify_request_printable_scalar_bound(
    value: &str,
    max: usize,
    label: &str,
    code: &'static str,
) -> EngineResult<()> {
    let scalar_count = value.chars().count();
    if scalar_count == 0 || scalar_count > max || value.chars().any(char::is_control) {
        return Err(invalid_request(
            code,
            format!("{label} must contain 1 to {max} non-control Unicode scalar values"),
        ));
    }
    Ok(())
}

fn invalid_request(code: &'static str, message: impl Into<String>) -> EngineFailure {
    let mut failure = EngineFailure::new(
        EngineFailureCategory::Validation,
        EnginePhase::ValidateRequest,
        code,
        message,
    );
    failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
    failure
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn verify_path(path: &str, label: &str) -> EngineResult<()> {
    if path.chars().count() > crate::MAX_CONTRACT_POINTER_SCALARS
        || path.chars().any(char::is_control)
        || !path.is_empty() && !path.starts_with('/')
    {
        return Err(invalid_failure(format!(
            "{label} must be an empty or slash-prefixed JSON Pointer of at most {} Unicode scalar values",
            crate::MAX_CONTRACT_POINTER_SCALARS
        )));
    }
    Ok(())
}

fn invalid_failure(message: impl Into<String>) -> EngineFailure {
    EngineFailure::new(
        EngineFailureCategory::ContractViolation,
        EnginePhase::Transport,
        "invalid_engine_failure",
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn explicit_process_config() -> EngineProcessConfig {
        EngineProcessConfig {
            executable: "/opt/cymule/plugin".to_owned(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            working_directory: None,
            runtime_closure: BTreeMap::from([(
                "component-runtime".to_owned(),
                format!("sha256:{}", "a".repeat(64)),
            )]),
            timeout_ms: 60_000,
            message_limit: 8 * 1024 * 1024,
            closure_limit: 64 * 1024 * 1024,
        }
    }

    #[test]
    fn engine_response_bound_covers_exact_echo_payload_and_framing() {
        let request = serde_json::to_vec(&EngineRequestEnvelope::new(serde_json::Value::Null))
            .expect("request envelope serializes");
        let success = serde_json::to_vec(&EngineResponseEnvelope::success(
            serde_json::Value::Null,
            serde_json::Value::Null,
        ))
        .expect("success envelope serializes");
        let request_framing = request.len() - b"null".len();
        let success_framing = success.len() - 2 * b"null".len();
        assert_eq!(request_framing, ENGINE_REQUEST_ENVELOPE_FRAMING_BYTES);
        assert_eq!(success_framing, ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES);
        assert_eq!(
            success_framing - request_framing,
            ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES - ENGINE_REQUEST_ENVELOPE_FRAMING_BYTES
        );
        assert_eq!(
            MAX_ENGINE_RESPONSE_BYTES,
            MAX_ENGINE_REQUEST_ECHO_BYTES
                + MAX_ENGINE_RESPONSE_PAYLOAD_BYTES
                + ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES
        );
    }

    #[test]
    fn transport_failure_does_not_claim_retry_is_safe() {
        assert_eq!(
            EngineFailure::transport("engine_failed", "no envelope").retry_disposition,
            None
        );
    }

    #[test]
    fn clock_source_uses_printable_unicode_scalar_bounds() {
        let generation = format!("sha256:{}", "1".repeat(64));
        EngineClockTarget::sqlite("clock.sqlite", "🧭".repeat(512), &generation)
            .verify()
            .expect("512 multi-byte Unicode scalar values are valid");

        for source_id in ["🧭".repeat(513), "clock:\u{0085}forged".to_owned()] {
            EngineClockTarget::sqlite("clock.sqlite", source_id, &generation)
                .verify()
                .expect_err("an overlong or control-bearing Clock source is rejected");
        }
    }

    #[test]
    fn invalid_provider_targets_are_correctable_request_validation() {
        let invalid_targets = [
            EngineStoreTarget::directory("")
                .verify()
                .expect_err("empty store location fails"),
            EngineClockTarget::sqlite("clock.sqlite", "clock:test", "sha256:NOT-LOWERCASE")
                .verify()
                .expect_err("invalid Clock generation fails"),
            EnginePluginTarget::pinned_process(explicit_process_config(), "sha256:short")
                .verify()
                .expect_err("invalid plugin revision fails"),
        ];
        for failure in invalid_targets {
            assert_eq!(failure.category, EngineFailureCategory::Validation);
            assert_eq!(failure.phase, EnginePhase::ValidateRequest);
            assert_eq!(
                failure.retry_disposition,
                Some(EngineRetryDisposition::CorrectAndRetry)
            );
        }
    }

    #[test]
    fn official_store_constructors_pin_terminal_physical_generations() {
        let directory = EngineStoreTarget::directory("directory");
        assert_eq!(directory.provider, "cymule.directory-store/5");
        assert!(directory.domain.is_none());
        let sqlite = EngineStoreTarget::sqlite("sqlite", "domain");
        assert_eq!(sqlite.provider, "cymule.sqlite-store/6");
        assert_eq!(sqlite.domain.as_deref(), Some("domain"));
    }

    #[test]
    fn process_target_requires_and_validates_complete_explicit_configuration() {
        let target = EnginePluginTarget::process(explicit_process_config());
        target.verify().expect("complete process target verifies");
        let encoded = serde_json::to_value(&target).expect("process target serializes");
        assert_eq!(
            encoded["process"]["working_directory"],
            serde_json::Value::Null
        );

        let mut stale_location = encoded.clone();
        stale_location
            .as_object_mut()
            .expect("target is an object")
            .insert(
                "location".to_owned(),
                serde_json::Value::String("/opt/cymule/plugin".to_owned()),
            );
        assert!(
            serde_json::from_value::<EnginePluginTarget>(stale_location).is_err(),
            "the superseded duplicate process location must fail closed"
        );

        let mut missing_process = encoded.clone();
        missing_process
            .as_object_mut()
            .expect("target is an object")
            .remove("process");
        assert!(serde_json::from_value::<EnginePluginTarget>(missing_process).is_err());

        let mut unknown_process_field = encoded.clone();
        unknown_process_field["process"]
            .as_object_mut()
            .expect("process config is an object")
            .insert("runtime_label".to_owned(), serde_json::json!("mutable"));
        assert!(
            serde_json::from_value::<EnginePluginTarget>(unknown_process_field).is_err(),
            "unknown process authority fields must fail closed"
        );

        for field in [
            "executable",
            "arguments",
            "environment",
            "working_directory",
            "runtime_closure",
            "timeout_ms",
            "message_limit",
            "closure_limit",
        ] {
            let mut missing = encoded.clone();
            missing["process"]
                .as_object_mut()
                .expect("process config is an object")
                .remove(field);
            assert!(
                serde_json::from_value::<EnginePluginTarget>(missing).is_err(),
                "missing process field {field} must fail closed"
            );
        }

        for mutate in [
            |config: &mut EngineProcessConfig| config.executable = "plugin".to_owned(),
            |config: &mut EngineProcessConfig| {
                config.working_directory = Some("relative".to_owned());
            },
            |config: &mut EngineProcessConfig| config.timeout_ms = 0,
            |config: &mut EngineProcessConfig| config.message_limit = 64 * 1024 * 1024 + 1,
            |config: &mut EngineProcessConfig| config.runtime_closure.clear(),
            |config: &mut EngineProcessConfig| {
                config.runtime_closure.insert(
                    "component-runtime".to_owned(),
                    "unix:macos:arm64".to_owned(),
                );
            },
            |config: &mut EngineProcessConfig| {
                config.runtime_closure.insert(
                    "component-runtime".to_owned(),
                    format!("sha256:{}", "A".repeat(64)),
                );
            },
            |config: &mut EngineProcessConfig| {
                config
                    .runtime_closure
                    .insert("component-runtime".to_owned(), "sha256:short".to_owned());
            },
            |config: &mut EngineProcessConfig| {
                config
                    .environment
                    .insert("INVALID=KEY".to_owned(), "value".to_owned());
            },
        ] {
            let mut config = explicit_process_config();
            mutate(&mut config);
            let failure = EnginePluginTarget::process(config)
                .verify()
                .expect_err("invalid process configuration fails closed");
            assert_eq!(failure.code.as_ref(), "invalid_process_config");
            assert_eq!(failure.category, EngineFailureCategory::Validation);
        }

        for message_limit in [
            (crate::MAX_PLUGIN_MESSAGE_BYTES - 1) as u64,
            (crate::MAX_PLUGIN_MESSAGE_BYTES + 1) as u64,
        ] {
            let mut config = explicit_process_config();
            config.message_limit = message_limit;
            let failure = EnginePluginTarget::process(config)
                .verify()
                .expect_err("plugin/3 process limit must be exact");
            assert_eq!(failure.code.as_ref(), "plugin_message_limit_mismatch");
        }
    }

    #[test]
    fn process_target_entry_count_bounds_are_exact() {
        let mut arguments = explicit_process_config();
        arguments.arguments = vec![String::new(); MAX_PROCESS_ARGUMENTS];
        arguments
            .verify()
            .expect("the exact process argument count is admitted");
        serde_json::from_value::<EngineProcessConfig>(serde_json::to_value(&arguments).unwrap())
            .expect("the exact argument count deserializes");
        arguments.arguments.push(String::new());
        assert!(arguments.verify().is_err());
        assert!(
            serde_json::from_value::<EngineProcessConfig>(
                serde_json::to_value(&arguments).unwrap()
            )
            .is_err()
        );

        let mut environment = explicit_process_config();
        environment.environment = (0..MAX_PROCESS_ENVIRONMENT_ENTRIES)
            .map(|index| (format!("KEY_{index}"), String::new()))
            .collect();
        environment
            .verify()
            .expect("the exact process environment count is admitted");
        serde_json::from_value::<EngineProcessConfig>(serde_json::to_value(&environment).unwrap())
            .expect("the exact environment count deserializes");
        environment
            .environment
            .insert("KEY_OVER_LIMIT".to_owned(), String::new());
        assert!(environment.verify().is_err());
        assert!(
            serde_json::from_value::<EngineProcessConfig>(
                serde_json::to_value(&environment).unwrap()
            )
            .is_err()
        );

        let revision = format!("sha256:{}", "b".repeat(64));
        let mut runtime = explicit_process_config();
        runtime.runtime_closure = (0..MAX_PROCESS_RUNTIME_ENTRIES)
            .map(|index| (format!("runtime-{index}"), revision.clone()))
            .collect();
        runtime
            .verify()
            .expect("the exact runtime-closure count is admitted");
        serde_json::from_value::<EngineProcessConfig>(serde_json::to_value(&runtime).unwrap())
            .expect("the exact runtime-closure count deserializes");
        runtime
            .runtime_closure
            .insert("runtime-over-limit".to_owned(), revision);
        assert!(runtime.verify().is_err());
        assert!(
            serde_json::from_value::<EngineProcessConfig>(serde_json::to_value(&runtime).unwrap())
                .is_err()
        );
    }

    #[test]
    fn evolution_target_requires_nullable_providers_and_exact_process_revisions() {
        let revision = format!("sha256:{}", "1".repeat(64));
        let target = EngineEvolutionTarget::new(EngineStoreTarget::directory("store"));
        let encoded = serde_json::to_value(&target).expect("evolution target serializes");
        assert_eq!(encoded["target_execution_bindings"], serde_json::json!({}));
        assert!(encoded["migration_adapter"].is_null());
        assert!(encoded["shadow_driver"].is_null());

        for field in [
            "target_execution_bindings",
            "migration_adapter",
            "shadow_driver",
        ] {
            let mut missing = encoded.clone();
            missing
                .as_object_mut()
                .expect("evolution target is an object")
                .remove(field);
            assert!(
                serde_json::from_value::<EngineEvolutionTarget>(missing).is_err(),
                "missing required-nullable {field} must fail closed"
            );
        }

        let mut migration_process = explicit_process_config();
        migration_process.message_limit = EVOLUTION_PLUGIN_MESSAGE_LIMIT as u64;
        let migration = EngineMigrationProviderTarget {
            adapter_id: "migration:test".to_owned(),
            adapter_revision: revision.clone(),
            process: EnginePluginTarget::pinned_process(migration_process, &revision),
        };
        migration
            .verify()
            .expect("an exact semantic/process migration target verifies");

        let mut mismatched = migration.clone();
        mismatched.process.revision = Some(format!("sha256:{}", "2".repeat(64)));
        let failure = mismatched
            .verify()
            .expect_err("semantic and process revisions cannot diverge");
        assert_eq!(
            failure.code.as_ref(),
            "evolution_provider_revision_mismatch"
        );

        let mut wrong_bound = migration;
        wrong_bound.process.process.message_limit = (EVOLUTION_PLUGIN_MESSAGE_LIMIT - 1) as u64;
        let failure = wrong_bound
            .verify()
            .expect_err("evolution provider message bounds cannot be configured per caller");
        assert_eq!(
            failure.code.as_ref(),
            "evolution_provider_message_limit_mismatch"
        );

        let mut shadow_process = explicit_process_config();
        shadow_process.message_limit = EVOLUTION_PLUGIN_MESSAGE_LIMIT as u64;
        let shadow = EngineShadowProviderTarget {
            driver_id: "shadow:test".to_owned(),
            driver_revision: revision.clone(),
            process: EnginePluginTarget::pinned_process(shadow_process, revision),
        };
        shadow
            .verify()
            .expect("an exact semantic/process shadow target verifies");

        let target_plan = format!("sha256:{}", "3".repeat(64));
        let target_revision = format!("sha256:{}", "4".repeat(64));
        let mut with_binding = target;
        with_binding.target_execution_bindings.insert(
            target_plan,
            EnginePluginTarget::pinned_process(explicit_process_config(), target_revision),
        );
        with_binding
            .verify()
            .expect("one exact target execution binding verifies");
        let mut unpinned = with_binding.clone();
        unpinned
            .target_execution_bindings
            .values_mut()
            .next()
            .expect("target binding exists")
            .revision = None;
        assert_eq!(
            unpinned
                .verify()
                .expect_err("target binding must be pinned")
                .code
                .as_ref(),
            "unpinned_evolution_target_binding"
        );

        let mut target_bomb = serde_json::to_value(&with_binding).unwrap();
        let extra = format!("sha256:{}", "5".repeat(64));
        let binding_value = target_bomb["target_execution_bindings"]
            .as_object()
            .and_then(|bindings| bindings.values().next())
            .cloned()
            .expect("target binding serializes");
        target_bomb["target_execution_bindings"]
            .as_object_mut()
            .unwrap()
            .insert(extra, binding_value);
        assert!(serde_json::from_value::<EngineEvolutionTarget>(target_bomb).is_err());
    }

    #[test]
    fn wire_string_bounds_count_unicode_scalars_not_utf8_bytes() {
        EngineStoreTarget::directory("🧭".repeat(4096))
            .verify()
            .expect("4096 multi-byte location scalars are valid at the wire boundary");
        assert!(
            EngineStoreTarget::directory("🧭".repeat(4097))
                .verify()
                .is_err()
        );

        let mut failure = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "unicode_boundary",
            "🧭".repeat(8192),
        );
        failure.path = Some(format!("/{}", "🧭".repeat(999)).into_boxed_str());
        failure.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        failure
            .verify()
            .expect("multi-byte failure fields use schema scalar bounds");

        failure.message = "🧭".repeat(8193).into_boxed_str();
        assert!(failure.verify().is_err());
        failure.message = "valid".into();
        failure.path = Some(format!("/{}", "🧭".repeat(1000)).into_boxed_str());
        assert!(failure.verify().is_err());
    }

    #[test]
    fn failure_category_closes_retry_and_reconciliation_semantics() {
        let mut unsafe_unknown = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::EffectDispatch,
            "effect_outcome_unknown",
            "the external world may have changed",
        );
        unsafe_unknown.retry_disposition = Some(EngineRetryDisposition::RetrySameRequest);
        let envelope =
            EngineResponseEnvelope::<serde_json::Value, serde_json::Value>::failure(unsafe_unknown);
        assert_eq!(
            envelope
                .into_result()
                .expect_err("unknown world outcome cannot authorize redispatch")
                .code
                .as_ref(),
            "invalid_engine_failure"
        );

        let mut false_reconcile = EngineFailure::new(
            EngineFailureCategory::SubstrateFailure,
            EnginePhase::PluginCall,
            "plugin_unavailable",
            "plugin did not start",
        );
        false_reconcile.retry_disposition = Some(EngineRetryDisposition::Reconcile);
        assert!(
            false_reconcile
                .verify()
                .expect_err("reconcile belongs only to unknown world outcomes")
                .message
                .contains("incompatible")
        );

        let mut valid_unknown = EngineFailure::new(
            EngineFailureCategory::UnknownWorldOutcome,
            EnginePhase::EffectDispatch,
            "effect_outcome_unknown",
            "the external world may have changed",
        );
        valid_unknown.retry_disposition = Some(EngineRetryDisposition::Reconcile);
        valid_unknown
            .verify()
            .expect("unknown world outcome requires reconciliation");

        let cancelled = EngineFailure::from_runtime(
            RuntimeError::cancelled(
                "process_invocation_cancelled",
                "the invocation owner cancelled work",
            ),
            EnginePhase::PluginCall,
        );
        assert_eq!(cancelled.category, EngineFailureCategory::Cancelled);
        assert_eq!(
            cancelled.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
        cancelled
            .verify()
            .expect("typed cancellation satisfies the failure matrix");

        let mut durable_timeout = EngineFailure::new(
            EngineFailureCategory::TimedOut,
            EnginePhase::ExecuteDurable,
            "process_response_timed_out",
            "the persisted provider Attempt timed out",
        );
        durable_timeout.retry_disposition = Some(EngineRetryDisposition::RefreshAndRetry);
        durable_timeout
            .verify()
            .expect("durable timeout permits explicit refreshed takeover authority");
    }

    #[test]
    fn archived_command_replay_projects_as_typed_correctable_admission() {
        let failure = EngineFailure::from_core(
            &cymule_core::CoreError::ArchivedCommandReplayRequired {
                command_id: "command:archived".to_owned(),
                archive_head:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
                command_index_root:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(failure.category, EngineFailureCategory::AdmissionDenied);
        assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
        assert_eq!(failure.code.as_ref(), "archived_command_replay_required");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry)
        );
        failure
            .verify()
            .expect("archived replay is a closed correctable admission failure");
    }

    #[test]
    fn paged_scope_requires_a_different_admissible_command() {
        let failure = EngineFailure::from_core(
            &cymule_core::CoreError::PagedScopeRequired {
                run_id: "run:paged-scope".to_owned(),
                scope_id: "scope:paged-scope".to_owned(),
                entries: 4_097,
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(failure.category, EngineFailureCategory::AdmissionDenied);
        assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
        assert_eq!(failure.code.as_ref(), "paged_scope_required");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::CorrectAndRetry),
        );
        assert!(failure.message.contains("run:paged-scope"));
        assert!(failure.message.contains("scope:paged-scope"));
        failure
            .verify()
            .expect("paged admission failure is valid Engine wire");
    }

    #[test]
    fn collection_provider_failures_keep_all_five_typed_recovery_meanings() {
        use cymule_authenticated_collections::{ProviderConflict, ProviderFailure};
        for (provider, category, code, retry) in [
            (
                ProviderFailure::Validation {
                    message: "invalid provider input".to_owned(),
                },
                EngineFailureCategory::Validation,
                "collection_provider_validation_failed",
                EngineRetryDisposition::CorrectAndRetry,
            ),
            (
                ProviderFailure::Integrity {
                    code: "provider_corrupt".to_owned(),
                    message: "retained bytes changed".to_owned(),
                },
                EngineFailureCategory::ContractViolation,
                "provider_corrupt",
                EngineRetryDisposition::Never,
            ),
            (
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::Revision {
                        expected: Some("old".to_owned()),
                        current: Some("new".to_owned()),
                    },
                },
                EngineFailureCategory::Conflict,
                "revision_conflict",
                EngineRetryDisposition::RefreshAndRetry,
            ),
            (
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::History {
                        code: "provider_history_conflict".to_owned(),
                        message: "immutable history changed".to_owned(),
                    },
                },
                EngineFailureCategory::Conflict,
                "provider_history_conflict",
                EngineRetryDisposition::Never,
            ),
            (
                ProviderFailure::Substrate {
                    code: "provider_io_failed".to_owned(),
                    message: "provider read failed".to_owned(),
                },
                EngineFailureCategory::SubstrateFailure,
                "provider_io_failed",
                EngineRetryDisposition::RetrySameRequest,
            ),
        ] {
            let failure = EngineFailure::from_core(
                &cymule_core::CoreError::CollectionProviderFailure(provider.clone()),
                EnginePhase::ExecuteDurable,
            );
            assert_eq!(failure.category, category);
            assert_eq!(failure.code.as_ref(), code);
            assert_eq!(failure.retry_disposition, Some(retry));
            assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
            match provider {
                ProviderFailure::Validation { message }
                | ProviderFailure::Integrity { message, .. }
                | ProviderFailure::Substrate { message, .. }
                | ProviderFailure::Conflict {
                    evidence: ProviderConflict::History { message, .. },
                } => {
                    assert_eq!(failure.message.as_ref(), message);
                }
                ProviderFailure::Conflict {
                    evidence: ProviderConflict::Revision { .. },
                } => {
                    assert!(failure.message.contains("old") && failure.message.contains("new"));
                }
            }
            failure
                .verify()
                .expect("typed provider failure is valid Engine wire");
        }
    }

    #[test]
    fn incomplete_pinned_read_set_projects_as_a_non_retryable_framework_contract_failure() {
        let failure = EngineFailure::from_core(
            &cymule_core::CoreError::PinnedReadSetIncomplete {
                family: "runs",
                key: "run:missing".to_owned(),
            },
            EnginePhase::ExecuteDurable,
        );
        assert_eq!(failure.category, EngineFailureCategory::ContractViolation);
        assert_eq!(failure.phase, EnginePhase::ExecuteDurable);
        assert_eq!(failure.code.as_ref(), "pinned_read_set_incomplete");
        assert_eq!(
            failure.retry_disposition,
            Some(EngineRetryDisposition::Never)
        );
        failure
            .verify()
            .expect("a framework read-set defect is a closed terminal failure");
    }

    #[test]
    fn engine_v5_rejects_v4_envelopes() {
        let legacy = serde_json::json!({
            "outcome": "success",
            "engine_protocol": "cymule.engine/4",
            "request": {"type": "verify"},
            "response": {"verified": true}
        });
        let envelope: EngineResponseEnvelope<serde_json::Value, serde_json::Value> =
            serde_json::from_value(legacy).expect("closed legacy envelope decodes");
        assert_eq!(
            envelope
                .into_result()
                .expect_err("legacy Engine protocol must fail closed")
                .code
                .as_ref(),
            "unsupported_engine_protocol"
        );
    }

    #[test]
    fn failure_envelope_rejects_unknown_fields_and_invalid_bounds() {
        let unknown = serde_json::json!({
            "outcome": "failure",
            "engine_protocol": ENGINE_PROTOCOL_VERSION,
            "error": {
                "category": "validation",
                "phase": "seal_plan",
                "code": "validation_failed",
                "message": "invalid",
                "provider": "must-not-enter-engine-errors"
            }
        });
        assert!(
            serde_json::from_value::<EngineResponseEnvelope<serde_json::Value, serde_json::Value>>(
                unknown
            )
            .is_err()
        );

        let invalid = EngineResponseEnvelope::<serde_json::Value, serde_json::Value>::failure(
            EngineFailure::new(
                EngineFailureCategory::Validation,
                EnginePhase::SealPlan,
                "INVALID-CODE",
                "invalid",
            ),
        );
        let failure = invalid
            .into_result()
            .expect_err("invalid failure code is rejected after deserialization");
        assert_eq!(failure.code.as_ref(), "invalid_engine_failure");

        let mut control_message = EngineFailure::new(
            EngineFailureCategory::Validation,
            EnginePhase::ValidateRequest,
            "invalid_request",
            "invalid\nmessage",
        );
        control_message.retry_disposition = Some(EngineRetryDisposition::CorrectAndRetry);
        assert!(control_message.verify().is_err());
        control_message.message = "bounded message".into();
        control_message.issues = vec![EngineIssue {
            code: "schema_violation".into(),
            message: "invalid\u{0000}issue".into(),
            path: None,
            schema_path: None,
        }];
        assert!(control_message.verify().is_err());
    }

    #[test]
    fn success_requires_request_correlation_and_failure_forbids_it() {
        let missing_request = serde_json::json!({
            "outcome": "success",
            "engine_protocol": ENGINE_PROTOCOL_VERSION,
            "response": {"type": "verified"}
        });
        assert!(
            serde_json::from_value::<EngineResponseEnvelope<serde_json::Value, serde_json::Value>>(
                missing_request
            )
            .is_err()
        );

        let correlated = EngineResponseEnvelope::success(
            serde_json::json!({"type": "verify"}),
            serde_json::json!({"type": "verified"}),
        );
        assert_eq!(
            correlated
                .into_result()
                .expect("correlated success verifies"),
            (
                serde_json::json!({"type": "verify"}),
                serde_json::json!({"type": "verified"}),
            )
        );

        let failure_with_request = serde_json::json!({
            "outcome": "failure",
            "engine_protocol": ENGINE_PROTOCOL_VERSION,
            "request": {"type": "verify"},
            "error": {
                "category": "validation",
                "phase": "validate_request",
                "code": "invalid_request",
                "message": "invalid"
            }
        });
        assert!(
            serde_json::from_value::<EngineResponseEnvelope<serde_json::Value, serde_json::Value>>(
                failure_with_request
            )
            .is_err()
        );
    }
}
