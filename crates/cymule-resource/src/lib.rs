//! Provider-neutral cross-Run resource descriptors and transfer contracts.

mod catalog;
mod error;
mod handoff;
mod lifecycle;
mod manifest;
mod model;
mod resolver;
mod store;
mod type_contract;

pub use catalog::{RESOURCE_CATALOG_RECORD_VERSION, ResourceCatalogRecord, ResourceCatalogStore};
pub use error::{ResourceError, ResourceResult, ResourceSchemaIssue};
pub use handoff::{
    RESOURCE_HANDOFF_ACTIVATION_VERSION, RESOURCE_HANDOFF_VERSION, ResourceHandoff,
    ResourceHandoffActivation, ResourceHandoffController, ResourceProducerProvenance,
};
pub use lifecycle::{
    RESOURCE_CLEANUP_RECEIPT_VERSION, RESOURCE_DELETE_INTENT_VERSION,
    RESOURCE_DELETE_RECEIPT_VERSION, RESOURCE_GC_RECEIPT_VERSION,
    RESOURCE_LIFECYCLE_JOURNAL_VERSION, RESOURCE_PIN_RECEIPT_VERSION,
    RESOURCE_RELEASE_RECEIPT_VERSION, ResourceCleanupReceipt, ResourceDeleteIntent,
    ResourceDeleteReceipt, ResourceDeleter, ResourceDeletionObservation, ResourceGcDisposition,
    ResourceGcReceipt, ResourceLifecycle, ResourceLifecycleController, ResourceLifecycleLedger,
    ResourcePinReceipt, ResourceReleaseReceipt,
};
pub use manifest::{
    ManifestInclusionProof, MerkleSide, MerkleStep, RESOURCE_LIST_PROOF_VERSION,
    RESOURCE_MANIFEST_MEDIA_TYPE, RESOURCE_MANIFEST_VERSION, ResourceListProof,
    ResourceManifestDescriptor, ResourceManifestEntry, SealedResourceManifest,
};
pub use model::{
    INLINE_RESOURCE_LIMIT, InlineData, RESOURCE_LOCATOR_VERSION, RESOURCE_VERSION,
    ResourceCandidate, ResourceHandle, ResourceIntegrity, ResourceLocation, ResourceLocatorSet,
    ResourcePublication, ResourceReplayClass, ResourceShape,
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
    ArtifactTypeContract, ArtifactTypeRegistry, CANONICAL_JSON_MEDIA_TYPE, FrameworkArtifactType,
    JSON_SCHEMA_DIALECT, framework_artifact_contract, framework_artifact_contracts,
};
