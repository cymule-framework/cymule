//! Closed provider-neutral durable execution protocol.
//!
//! This crate owns the shared logical Clock, execution-claim, Continuation,
//! frame, wait-owner, and identified wait-activation contracts below both M1
//! persistence and higher-profile reducers. It contains only deterministic DTO,
//! identity, and pure verification authority.

#![forbid(unsafe_code)]

mod error;
mod model;

pub use error::{DurableProtocolError, DurableProtocolResult};
pub use model::*;
