//! Embedded Cymule runtime and provider-neutral plugin interfaces.

mod composition;
mod contract;
mod engine;
mod error;
mod plugin;
mod protocol;
mod strict_json;

pub use composition::{
    BINDING_CONTEXT_ID_DOMAIN, BindingContextDescriptor, CompositionError,
    EXECUTION_BINDING_VERSION, ExecutionBinding, ExecutionOperationBinding, ExecutionOperationKind,
    MAX_COMPOSITION_TOKEN_SCALARS, MAX_EXECUTION_OPERATIONS_PER_KIND, MAX_PROVIDER_PROPERTIES,
    MAX_PROVIDER_PROPERTY_VALUE_SCALARS, MAX_PROVIDER_SERVICES, MAX_RUNTIME_PROVIDERS,
    MAX_RUNTIME_SERVICES, OCCURRENCE_BINDING_ID_DOMAIN, RUNTIME_COMPOSITION_VERSION,
    RequirementAdmission, RuntimeCompositionGraph, RuntimeImplementation,
    RuntimeProviderDescriptor, ServiceBindingDescriptor, ServiceKey,
};
pub use contract::{
    CONTRACT_SCHEMA_DIALECT, ContractBoundary, ContractIssue, ContractIssueKind, ContractPhase,
    ContractResult, ContractSide, ContractTarget, ContractValidator, ContractViolation,
    MAX_CONCRETE_CONTRACT_ISSUES, MAX_CONTRACT_ISSUES, MAX_CONTRACT_MESSAGE_SCALARS,
    MAX_CONTRACT_POINTER_SCALARS, MAX_CONTRACT_VIOLATION_BYTES, PlanAdmissionError,
    PlanAdmissionResult, PlanContracts,
};
pub use engine::{
    EffectReconciliationBoundary, EffectReleaseBoundary, EmbeddedRuntime, ExecutionOutcome,
    ExecutionResult, RESULT_ARTIFACT_KIND, SuspensionBoundary, verify_execution_request,
    verify_plan,
};
pub use error::{RuntimeError, RuntimeResult};
pub use plugin::{
    AdmittedPluginRouter, BoundOperationAdmission, BoundPluginHost,
    EFFECT_PROVIDER_ATTEMPT_VERSION, EffectProviderAttempt, EffectReconciliationDecision,
    ExecutionBindingAdmission, MAX_PLUGIN_MESSAGE_BYTES, PLUGIN_VERSION, PluginEffect,
    PluginExpectedFailure, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, decode_plugin_request, decode_plugin_response, effect_provider_attempt_id,
};
pub use protocol::{
    ENGINE_CLOCK_SYSTEM_PROVIDER, ENGINE_DIRECTORY_STORE_PROVIDER,
    ENGINE_PROCESS_EXECUTOR_PROVIDER, ENGINE_PROTOCOL_VERSION,
    ENGINE_REQUEST_ENVELOPE_FRAMING_BYTES, ENGINE_SQLITE_STORE_PROVIDER,
    ENGINE_SUCCESS_ENVELOPE_FRAMING_BYTES, EVOLUTION_PLUGIN_MESSAGE_LIMIT,
    EVOLUTION_PLUGIN_PROTOCOL_VERSION, EngineClockTarget, EngineContractSide, EngineDurableTarget,
    EngineEvolutionTarget, EngineFailure, EngineFailureCategory, EngineIssue,
    EngineMigrationProviderTarget, EnginePhase, EnginePluginTarget, EngineProcessConfig,
    EngineRequestEnvelope, EngineResponseEnvelope, EngineResult, EngineRetryDisposition,
    EngineShadowProviderTarget, EngineStoreTarget, MAX_ENGINE_DIAGNOSTIC_BYTES,
    MAX_ENGINE_REQUEST_BYTES, MAX_ENGINE_REQUEST_ECHO_BYTES, MAX_ENGINE_RESPONSE_BYTES,
    MAX_ENGINE_RESPONSE_PAYLOAD_BYTES, MAX_EVOLUTION_TARGET_EXECUTION_BINDINGS,
    MAX_PROCESS_ARGUMENTS, MAX_PROCESS_ENVIRONMENT_ENTRIES, MAX_PROCESS_RUNTIME_ENTRIES,
};
pub use strict_json::{
    decode_strict_json_value, validate_json_typed_roundtrip, validate_json_typed_roundtrip_bytes,
    validate_strict_json,
};
