//! Provider-neutral cross-Run resource descriptors and transfer contracts.

mod error;
mod handoff;
mod model;
mod resolver;
mod store;
mod type_contract;

pub use error::{ResourceError, ResourceResult, ResourceSchemaIssue};
pub use handoff::{
    RESOURCE_HANDOFF_ACTIVATION_VERSION, RESOURCE_HANDOFF_VERSION, ResourceHandoff,
    ResourceHandoffActivation, ResourceHandoffController,
};
pub use model::{
    INLINE_RESOURCE_LIMIT, InlineData, RESOURCE_VERSION, ResourceCandidate, ResourceHandle,
    ResourceIntegrity, ResourceLocation, ResourceReplayClass, ResourceShape,
};
pub use resolver::{
    ArtifactResolver, MAX_LIST_PAGE, MAX_READ_CHUNK, ResourceChunk, ResourceClient, ResourceEntry,
    ResourceObservation, ResourcePage,
};
pub use store::{
    ArtifactStore, MAX_WRITE_CHUNK, ResourceWriteIntent, ResourceWriteSession, ResourceWriter,
};
pub use type_contract::{
    ARTIFACT_TYPE_CONTRACT_KIND, ARTIFACT_TYPE_CONTRACT_VERSION, ArtifactTypeCandidate,
    ArtifactTypeContract, ArtifactTypeRegistry, CANONICAL_JSON_MEDIA_TYPE, JSON_SCHEMA_DIALECT,
};
