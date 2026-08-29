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

pub use catalog::{
    MAX_RESOURCE_CATALOG_RECORD_BYTES, RESOURCE_CATALOG_RECORD_VERSION, ResourceCatalogRecord,
    ResourceCatalogStore,
};
pub use error::{ResourceError, ResourceResult, ResourceSchemaIssue};
pub use handoff::{
    MAX_HANDOFF_INDEX_PAGE, RESOURCE_HANDOFF_ACTIVATION_INDEX_VERSION,
    RESOURCE_HANDOFF_ACTIVATION_VERSION, RESOURCE_HANDOFF_INDEX_VERSION, RESOURCE_HANDOFF_VERSION,
    ResourceHandoff, ResourceHandoffActivation, ResourceHandoffActivationCurrent,
    ResourceHandoffActivationIndexEntry, ResourceHandoffActivationReceipt,
    ResourceHandoffController, ResourceHandoffCurrent, ResourceHandoffIndexEntry,
    ResourceHandoffPage, ResourceHandoffReceipt, ResourceProducerProvenance,
};
pub use lifecycle::{
    MAX_RESOURCE_CLEANUP_PLAN_BYTES, RESOURCE_AGENT_STREAM_PIN_VERSION,
    RESOURCE_ARCHIVE_PIN_VERSION, RESOURCE_ARCHIVE_RELEASE_VERSION, RESOURCE_CLEANUP_PLAN_VERSION,
    RESOURCE_CLEANUP_RECEIPT_VERSION, RESOURCE_COMMAND_RECEIPT_VERSION, RESOURCE_COMMAND_VERSION,
    RESOURCE_DELETE_CURRENT_VERSION, RESOURCE_DELETE_INTENT_VERSION,
    RESOURCE_DELETE_RECEIPT_VERSION, RESOURCE_DELETION_TARGET_VERSION, RESOURCE_GC_RECEIPT_VERSION,
    RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION, RESOURCE_PIN_CURRENT_VERSION,
    RESOURCE_PIN_RECEIPT_VERSION, RESOURCE_PROFILE_PIN_VERSION, RESOURCE_RELEASE_RECEIPT_VERSION,
    RESOURCE_RETENTION_CURRENT_VERSION, RESOURCE_RETENTION_FAMILY_VERSION,
    RESOURCE_RETENTION_KEY_VERSION, RESOURCE_RETENTION_SUBJECT_VERSION, ResourceArchiveRelease,
    ResourceCleanupPlan, ResourceCleanupReceipt, ResourceCleanupTarget, ResourceCleanupTargetKind,
    ResourceCommand, ResourceCommandOutcome, ResourceCommandReceipt, ResourceDeleteCurrent,
    ResourceDeleteIntent, ResourceDeletePostcondition, ResourceDeleteReceipt, ResourceDeleteStatus,
    ResourceDeleter, ResourceDeletionTarget, ResourceGcDisposition, ResourceGcReceipt,
    ResourceLifecycleController, ResourceLifecycleProfile, ResourceLifecycleReceiptLocator,
    ResourceLifecycleReceiptRef, ResourceOperation, ResourcePin, ResourcePinCurrent,
    ResourcePinKind, ResourcePinPostcondition, ResourcePinReceipt, ResourcePinStatus,
    ResourceProfilePin, ResourceReleaseReceipt, ResourceRetentionCurrent,
    ResourceRetentionDisposition, ResourceRetentionFamily, ResourceRetentionSubject,
    project_resource_begin_delete_intent, project_resource_pin_receipt,
    project_resource_reconcile_delete_receipt, project_resource_release_receipt,
    reduce_resource_begin_delete_intent, reduce_resource_gc_receipt, reduce_resource_pin_receipt,
    reduce_resource_reconcile_delete_receipt, reduce_resource_release_receipt,
    resource_agent_stream_pin_owner, resource_archive_pin_id, resource_archive_pin_owner,
    resource_retention_key,
};
pub use manifest::{
    CanonicalResourceManifestLine, MAX_MANIFEST_ENTRY_BYTES, MAX_MANIFEST_PAGE_BYTES,
    MAX_MANIFEST_PROOF_DEPTH, ManifestInclusionProof, ManifestPredecessorProof, MerkleSide,
    MerkleStep, RESOURCE_LIST_PROOF_VERSION, RESOURCE_MANIFEST_MEDIA_TYPE,
    RESOURCE_MANIFEST_VERSION, ResourceListProof, ResourceManifestAccumulator,
    ResourceManifestDescriptor, ResourceManifestEntry, ResourceManifestStreamVerifier,
    SealedResourceManifest, canonical_manifest_entry_bytes, manifest_leaf_digest,
    manifest_node_digest, resource_manifest_descriptor_id,
};
pub use model::{
    FRAMEWORK_RESOURCE_HANDLE_TYPE_KEY, INLINE_RESOURCE_LIMIT, InlineData,
    MAX_RESOURCE_ANNOTATIONS, MAX_RESOURCE_DESCRIPTOR_BYTES, MAX_RESOURCE_LOCATIONS,
    MAX_RESOURCE_LOCATOR_SET_BYTES, MAX_RESOURCE_PUBLIC_URL_BYTES, MAX_RESOURCE_PUBLIC_URL_SCALARS,
    RESOURCE_LOCATOR_VERSION, RESOURCE_VERSION, ResourceCandidate, ResourceHandle,
    ResourceIntegrity, ResourceLocation, ResourceLocatorSet, ResourcePublication,
    ResourceReplayClass, ResourceShape, decode_resource_handle_artifact,
    resource_handle_artifact_contract_id, resource_handle_artifact_kind,
    resource_handle_artifact_schema,
};
pub use resolver::{
    ArtifactResolver, MAX_LIST_PAGE, MAX_READ_CHUNK, RESOURCE_LIST_CURSOR_VERSION, ResourceChunk,
    ResourceClient, ResourceEntry, ResourceListCursor, ResourceObservation, ResourcePage,
};
pub use store::{
    ArtifactStore, MAX_WRITE_CHUNK, ResourceWriteIntent, ResourceWriteSession, ResourceWriter,
};
pub use type_contract::{
    ARTIFACT_TYPE_CONTRACT_KIND, ARTIFACT_TYPE_CONTRACT_VERSION, ArtifactTypeCandidate,
    ArtifactTypeContract, ArtifactTypeRegistry, CANONICAL_JSON_MEDIA_TYPE, FrameworkArtifactType,
    JSON_SCHEMA_DIALECT, MAX_ARTIFACT_TYPE_SCHEMA_BYTES, MAX_ARTIFACT_TYPE_SCHEMA_DEPTH,
    MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES, MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS,
    MAX_ARTIFACT_TYPE_SCHEMA_NODES, framework_artifact_contract, framework_artifact_contracts,
};
