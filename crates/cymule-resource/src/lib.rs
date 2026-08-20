//! Provider-neutral cross-Run resource descriptors and transfer contracts.

mod codec;
mod error;
mod handoff;
mod model;
mod resolver;
mod store;

pub use codec::{
    ARTIFACT_CODEC_VERSION, ArtifactCodecCandidate, ArtifactCodecDescriptor, ArtifactCodecRegistry,
    CANONICAL_JSON_MEDIA_TYPE, JSON_SCHEMA_DIALECT,
};
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
