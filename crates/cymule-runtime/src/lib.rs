//! Embedded Cymule runtime and provider-neutral plugin interfaces.

mod contract;
mod engine;
mod error;
mod plugin;

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
