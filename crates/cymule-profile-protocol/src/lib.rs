//! Closed persistence protocol shared by Cymule profile controllers.
//!
//! This crate is the sole owner of each profile's closed DTOs, content
//! identities, bounded typed authority views, and provider-independent pure
//! reducers. Provider-facing profile crates contain only concrete adapters and
//! closed process wires; they do not fork state machines or persistence DTOs.
//! Keeping the reducers below `cymule-durable` lets Durable assemble exact
//! pinned sources and commit typed postconditions without exposing a raw
//! journal, schema, record, Plan, or Artifact mutation capability.
//! Shared Clock, Continuation/frame, execution-claim, wait-owner, and
//! identified wait-activation contracts are owned one layer lower by
//! `cymule-durable-protocol`; this crate imports them without a compatibility
//! re-export.

#![forbid(unsafe_code)]

pub mod agent;
mod error;
pub mod evolution;
pub mod resource;
pub mod virtual_work;

pub use error::{ProtocolError, ProtocolResult};
