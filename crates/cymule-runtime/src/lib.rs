//! Embedded Cymule runtime and provider-neutral plugin interfaces.

mod composition;
mod contract;
mod engine;
mod error;
mod plugin;
mod protocol;

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
pub use engine::{EmbeddedRuntime, ExecutionResult, seal_plan, verify_plan};
pub use error::{RuntimeError, RuntimeResult};
pub use plugin::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, ProcessPlugin,
};
pub use protocol::{
    ENGINE_PROTOCOL_VERSION, EngineContractSide, EngineFailure, EngineFailureCategory, EngineIssue,
    EnginePhase, EngineRequestEnvelope, EngineResponseEnvelope, EngineResult,
    EngineRetryDisposition,
};
