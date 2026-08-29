//! Public Evolution profile contracts and the closed process-provider wire.
//!
//! Provider-independent semantics, normalized M4 state, exact receipts, and
//! detached reduction are owned exclusively by
//! [`cymule_profile_protocol::evolution`]. This crate intentionally contains no
//! Durable transaction, generic history append, rollback, or persistence
//! façade.

mod wire;

pub use cymule_profile_protocol::evolution::*;
pub use wire::{
    EVOLUTION_PLUGIN_PROTOCOL_VERSION, EvolutionPluginFailure, EvolutionPluginMigrationRequest,
    EvolutionPluginRequest, EvolutionPluginRequestEnvelope, EvolutionPluginResponse,
    EvolutionPluginResponseEnvelope, MAX_EVOLUTION_PLUGIN_MESSAGE_BYTES,
    decode_evolution_plugin_request, decode_evolution_plugin_response,
};
