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
    OCCURRENCE_BINDING_ID_DOMAIN, RUNTIME_COMPOSITION_VERSION, RequirementAdmission,
    RuntimeCompositionGraph, RuntimeImplementation, RuntimeProviderDescriptor,
    ServiceBindingDescriptor, ServiceKey,
};
pub use contract::{
    CONTRACT_SCHEMA_DIALECT, ContractBoundary, ContractIssue, ContractPhase, ContractResult,
    ContractSide, ContractTarget, ContractValidator, ContractViolation, PlanAdmissionError,
    PlanAdmissionResult, PlanContracts,
};
pub use engine::{
    EffectReconciliationBoundary, EffectReleaseBoundary, EmbeddedRuntime, ExecutionOutcome,
    ExecutionResult, SuspensionBoundary, verify_plan,
};
pub use error::{RuntimeError, RuntimeResult};
pub use plugin::{
    AdmittedPluginRouter, PLUGIN_VERSION, PluginEffect, PluginExpectedFailure, PluginHost,
    PluginManifest, PluginOperation, PluginRequest, PluginResponse,
};
pub use protocol::{
    ENGINE_DIRECTORY_STORE_PROVIDER, ENGINE_PROCESS_EXECUTOR_PROVIDER, ENGINE_PROTOCOL_VERSION,
    ENGINE_SQLITE_STORE_PROVIDER, EVOLUTION_PLUGIN_PROTOCOL_VERSION, EngineContractSide,
    EngineDurableTarget, EngineEvolutionTarget, EngineFailure, EngineFailureCategory, EngineIssue,
    EnginePhase, EnginePluginTarget, EngineRequestEnvelope, EngineResponseEnvelope, EngineResult,
    EngineRetryDisposition, EngineStoreTarget,
};
pub use strict_json::validate_strict_json;
