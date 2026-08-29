//! Typed provider adapters for Cymule's normalized Virtual-work profile.

mod archive;

pub use archive::ResourceBackedVirtualArchive;
pub use cymule_profile_protocol::virtual_work::*;
pub use cymule_profile_protocol::{ProtocolError, ProtocolResult};
