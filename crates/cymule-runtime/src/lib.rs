//! Embedded Cymule runtime and provider-neutral plugin interfaces.

mod composition;
mod contract;
mod engine;
mod error;
mod plugin;

pub use composition::{
    AcquiredRuntimeLayer, BINDING_CONTEXT_ID_DOMAIN, BindingContextDescriptor, CompositionError,
    LayerReleaseFailure, RUNTIME_COMPOSITION_VERSION, RuntimeComposition, RuntimeCompositionGraph,
    RuntimeImplementation, RuntimeLayerDescriptor, RuntimeLayerFactory, RuntimeLayerFailure,
    RuntimeLayerLifecycle, RuntimeLayerShareScope, RuntimeServiceBinding, RuntimeServices,
    ServiceBindingDescriptor, ServiceKey,
};
pub use contract::{
    CONTRACT_SCHEMA_DIALECT, ContractBoundary, ContractIssue, ContractPhase, ContractResult,
    ContractSide, ContractTarget, ContractValidator, ContractViolation, PlanContracts,
};
pub use engine::{EmbeddedRuntime, ExecutionResult};
pub use error::{RuntimeError, RuntimeResult};
pub use plugin::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, ProcessPlugin,
};
