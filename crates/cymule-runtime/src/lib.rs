//! Embedded Cymule runtime and provider-neutral plugin interfaces.

mod engine;
mod error;
mod plugin;

pub use engine::{EmbeddedRuntime, ExecutionResult};
pub use error::{RuntimeError, RuntimeResult};
pub use plugin::{
    PLUGIN_VERSION, PluginEffect, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, ProcessPlugin,
};
