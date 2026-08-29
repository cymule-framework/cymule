//! Apache `object_store` Resource adapter for Cymule.

use std::future::Future;
use std::sync::Arc;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_READ_CHUNK, MAX_RESOURCE_CATALOG_RECORD_BYTES,
    MAX_WRITE_CHUNK, RESOURCE_LOCATOR_VERSION, ResourceCandidate, ResourceCatalogRecord,
    ResourceCatalogStore, ResourceChunk, ResourceCleanupPlan, ResourceCleanupReceipt,
    ResourceDeleter, ResourceDeletionTarget, ResourceError, ResourceHandle, ResourceIntegrity,
    ResourceLocation, ResourceLocatorSet, ResourceObservation, ResourcePage, ResourcePublication,
    ResourceResult, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
};
use futures_util::{StreamExt as _, stream::BoxStream};
use object_store::path::Path;
#[cfg(any(feature = "azure", feature = "gcp"))]
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
    PutPayload, PutResult,
};
use object_store::{
    Error as ObjectError, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(any(test, feature = "azure", feature = "gcp"))]
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

#[cfg(test)]
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc::{Receiver, SyncSender},
};

const BINDING_VERSION: &str = "cymule.resource-object-store/5";
const UPLOAD_RECORD_VERSION: &str = "cymule.resource-object-store-upload/7";
const OBJECT_INDEX_NAMESPACE: &str = "cymule.resource-object-store-content/1";
#[cfg(any(test, feature = "azure", feature = "gcp"))]
const PHYSICAL_LAYOUT_VERSION: &str = "cymule.resource-object-store-layout/2";
#[cfg(any(test, feature = "azure", feature = "gcp"))]
const PHYSICAL_LAYOUT_MARKER: &str = "layout.json";
#[cfg(any(test, feature = "azure", feature = "gcp"))]
const INVENTORY_CAPABILITY_VERSION: &str = "cymule.resource-object-store-inventory/1";
const PART_SIZE: usize = 8 * 1024 * 1024;
const MAX_OBJECT_INDEX_BYTES: u64 = 16 * 1024;
const MAX_UPLOAD_RECORD_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CHUNK_NODE_BYTES: u64 = 4 * 1024;
const CHUNK_TREE_LEVELS: u8 = 14;
const CHUNK_TREE_RADIX: u8 = 16;
const CHUNK_NODE_VERSION: &str = "cymule.resource-object-store-upload-node/1";
const CHUNK_GENERATION_VERSION: &str = "cymule.resource-object-store-upload-generation/1";
const UPLOAD_GC_RECORD_VERSION: &str = "cymule.resource-object-store-upload-gc/2";
const UPLOAD_GC_OBJECT_PAGE: usize = 512;
const MAX_UPLOAD_CONTENT_RELATIVE_PATH_BYTES: u64 = 20 + 1 + 6 + 64 + 5;
const MAX_OBJECT_RELATIVE_PATH_BYTES: u64 = 64 + 1 + 6 + 20;
const MAX_UPLOAD_GC_RECORD_BYTES: u64 =
    4 * 1024 + (UPLOAD_GC_OBJECT_PAGE as u64) * (MAX_OBJECT_RELATIVE_PATH_BYTES + 256);
const UPLOAD_GC_CHUNK_PAGE: u64 = 64;

mod inventory_sealed {
    pub trait Sealed {}
}

/// Object-store provider whose inventory is a strong, ordered GC authority.
///
/// Implementations are sealed because Apache [`ObjectStore`] deliberately does
/// not promise listing order or list-after-write/delete consistency. Cymule
/// provides implementations only for concrete providers whose service contract
/// supplies both properties. Each returned stream must contain every object
/// below `prefix` exactly once, in strictly increasing path order, and an
/// `after` cursor must be exclusive.
trait ObjectStoreInventory: inventory_sealed::Sealed + ObjectStore {
    /// Return the provider's strong, lexicographically ordered inventory.
    #[doc(hidden)]
    fn ordered_inventory(
        &self,
        prefix: Option<&Path>,
        after: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>>;
}

#[cfg(test)]
macro_rules! ordered_inventory_provider {
    ($provider:path) => {
        impl inventory_sealed::Sealed for $provider {}

        impl ObjectStoreInventory for $provider {
            fn ordered_inventory(
                &self,
                prefix: Option<&Path>,
                after: Option<&Path>,
            ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                match after {
                    Some(after) => ObjectStore::list_with_offset(self, prefix, after),
                    None => ObjectStore::list(self, prefix),
                }
            }
        }
    };
}

#[cfg(test)]
ordered_inventory_provider!(object_store::memory::InMemory);

#[cfg(any(feature = "azure", feature = "gcp"))]
macro_rules! delegate_object_store {
    ($wrapper:ty) => {
        #[async_trait::async_trait]
        impl ObjectStore for $wrapper {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                options: PutOptions,
            ) -> object_store::Result<PutResult> {
                self.inner.put_opts(location, payload, options).await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                options: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, options).await
            }

            async fn get_opts(
                &self,
                location: &Path,
                options: GetOptions,
            ) -> object_store::Result<GetResult> {
                self.inner.get_opts(location, options).await
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, object_store::Result<Path>>,
            ) -> BoxStream<'static, object_store::Result<Path>> {
                self.inner.delete_stream(locations)
            }

            fn list(
                &self,
                prefix: Option<&Path>,
            ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&Path>,
            ) -> object_store::Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(
                &self,
                from: &Path,
                to: &Path,
                options: CopyOptions,
            ) -> object_store::Result<()> {
                self.inner.copy_opts(from, to, options).await
            }
        }
    };
}

/// Google Cloud Storage backend restricted to the official strong inventory service.
#[cfg(feature = "gcp")]
#[derive(Debug)]
pub struct GoogleCloudInventory {
    inner: object_store::gcp::GoogleCloudStorage,
}

#[cfg(feature = "gcp")]
impl GoogleCloudInventory {
    /// Build an inventory-authoritative GCS backend with application-default credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the bucket or local
    /// application-default credential configuration.
    pub fn from_application_default_credentials(
        bucket_name: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::build_official(
            object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(bucket_name),
        )
    }

    /// Build an inventory-authoritative GCS backend with one in-memory service-account key.
    ///
    /// # Errors
    ///
    /// Returns an error when the service-account document selects a custom GCS
    /// base URL or the pinned provider rejects the bucket or credentials.
    pub fn from_service_account_key(
        bucket_name: impl Into<String>,
        service_account_key: impl Into<String>,
    ) -> ResourceResult<Self> {
        let service_account_key = service_account_key.into();
        verify_google_cloud_service_account_key(&service_account_key)?;
        Self::build_official(
            object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket_name)
                .with_service_account_key(service_account_key),
        )
    }

    /// Build an inventory-authoritative GCS backend with one bearer token.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the bucket or token.
    pub fn from_bearer_token(
        bucket_name: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::build_official(
            object_store::gcp::GoogleCloudStorageBuilder::new()
                .with_bucket_name(bucket_name)
                .with_bearer_token(bearer_token),
        )
    }

    fn build_official(
        builder: object_store::gcp::GoogleCloudStorageBuilder,
    ) -> ResourceResult<Self> {
        Ok(Self {
            inner: builder.build().map_err(object_error)?,
        })
    }
}

#[cfg(feature = "gcp")]
impl std::fmt::Display for GoogleCloudInventory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[cfg(feature = "gcp")]
delegate_object_store!(GoogleCloudInventory);
#[cfg(feature = "gcp")]
impl inventory_sealed::Sealed for GoogleCloudInventory {}
#[cfg(feature = "gcp")]
impl ObjectStoreInventory for GoogleCloudInventory {
    fn ordered_inventory(
        &self,
        prefix: Option<&Path>,
        after: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match after {
            Some(after) => self.inner.list_with_offset(prefix, after),
            None => self.inner.list(prefix),
        }
    }
}

/// Azure Blob backend restricted to the official strong inventory service.
#[cfg(feature = "azure")]
#[derive(Debug)]
pub struct AzureBlobInventory {
    inner: object_store::azure::MicrosoftAzure,
}

#[cfg(feature = "azure")]
impl AzureBlobInventory {
    /// Build an inventory-authoritative Azure Blob backend with an account access key.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the account,
    /// container, or access key.
    pub fn from_access_key(
        account: impl Into<String>,
        container: impl Into<String>,
        access_key: impl Into<String>,
    ) -> ResourceResult<Self> {
        let (account, container) = validate_azure_inventory_location(account, container)?;
        Self::build_official(
            object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_access_key(access_key),
        )
    }

    /// Build an inventory-authoritative Azure Blob backend with one bearer token.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the account,
    /// container, or token.
    pub fn from_bearer_token(
        account: impl Into<String>,
        container: impl Into<String>,
        bearer_token: impl Into<String>,
    ) -> ResourceResult<Self> {
        let (account, container) = validate_azure_inventory_location(account, container)?;
        Self::build_official(
            object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_bearer_token_authorization(bearer_token),
        )
    }

    /// Build an inventory-authoritative Azure Blob backend with client-secret credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the account,
    /// container, or client-secret credentials.
    pub fn from_client_secret(
        account: impl Into<String>,
        container: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> ResourceResult<Self> {
        let (account, container) = validate_azure_inventory_location(account, container)?;
        Self::build_official(
            object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_client_secret_authorization(client_id, client_secret, tenant_id),
        )
    }

    /// Build an inventory-authoritative Azure Blob backend with a parsed SAS query.
    ///
    /// # Errors
    ///
    /// Returns an error when the pinned provider rejects the account,
    /// container, or SAS query.
    pub fn from_sas_query_pairs(
        account: impl Into<String>,
        container: impl Into<String>,
        query_pairs: Vec<(String, String)>,
    ) -> ResourceResult<Self> {
        let (account, container) = validate_azure_inventory_location(account, container)?;
        Self::build_official(
            object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container)
                .with_sas_authorization(query_pairs),
        )
    }

    fn build_official(builder: object_store::azure::MicrosoftAzureBuilder) -> ResourceResult<Self> {
        Ok(Self {
            inner: builder.build().map_err(object_error)?,
        })
    }
}

#[cfg(feature = "azure")]
impl std::fmt::Display for AzureBlobInventory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

#[cfg(feature = "azure")]
delegate_object_store!(AzureBlobInventory);
#[cfg(feature = "azure")]
impl inventory_sealed::Sealed for AzureBlobInventory {}
#[cfg(feature = "azure")]
impl ObjectStoreInventory for AzureBlobInventory {
    fn ordered_inventory(
        &self,
        prefix: Option<&Path>,
        after: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match after {
            Some(after) => self.inner.list_with_offset(prefix, after),
            None => self.inner.list(prefix),
        }
    }
}

#[cfg(test)]
static TEST_FAULT_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UploadState {
    Open,
    Publishing,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkLeaf {
    generation: String,
    offset: u64,
    size: u64,
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkNodeReference {
    node_id: String,
    leaf_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkNodeChild {
    slot: u8,
    node: ChunkNodeReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ChunkNode {
    Branch {
        depth: u8,
        children: Vec<ChunkNodeChild>,
    },
    Leaf {
        generation: String,
        offset: u64,
        size: u64,
        digest: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadPublication {
    digest: String,
    size: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    record_version: String,
    intent: ResourceWriteIntent,
    upload_id: String,
    store_binding: String,
    chunk_generation: String,
    content_epoch: u64,
    next_offset: u64,
    chunk_count: u64,
    chunk_root: Option<ChunkNodeReference>,
    migration: Option<ChunkMigration>,
    state: UploadState,
    publication: Option<UploadPublication>,
    cleanup_plan: Option<ResourceCleanupPlan>,
    cleanup_receipt: Option<ResourceCleanupReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkMigration {
    target_epoch: u64,
    migrated_offset: u64,
    migrated_count: u64,
    target_root: Option<ChunkNodeReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase", deny_unknown_fields)]
enum UploadGcPhase {
    Idle,
    FenceUploads {
        after: Option<String>,
    },
    SweepContent {
        after: Option<String>,
        deleted_in_pass: u64,
        page: Option<UploadGcPage>,
    },
    SweepDeletedObjects {
        after: Option<String>,
        deleted_in_pass: u64,
        page: Option<DeletedObjectGcPage>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadGcPage {
    paths: Vec<String>,
    end_of_inventory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeletedObjectGcTarget {
    path: String,
    tombstone_id: String,
    size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeletedObjectGcPage {
    examined_objects: u64,
    completed_after: Option<String>,
    targets: Vec<DeletedObjectGcTarget>,
    end_of_inventory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadGcRecord {
    record_version: String,
    store_binding: String,
    current_epoch: u64,
    phase: UploadGcPhase,
}

/// Bounded progress from one explicit upload-content reclamation step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadContentReclamation {
    /// Current non-reusable content epoch accepted by upload-head CAS.
    pub current_epoch: u64,
    /// Number of upload heads examined by this page.
    pub examined_uploads: u64,
    /// Number of immutable candidates or publication-family objects examined.
    pub examined_objects: u64,
    /// Number of retained old-epoch or deleted-family targets proved absent.
    ///
    /// The same admitted page reports the same count on replay. This is not a
    /// unique deletion total and must not be summed across retries or concurrent
    /// drivers.
    pub confirmed_absent_objects: u64,
    /// Whether upload and deleted-family sweeps reached their no-target traversal.
    ///
    /// This is not a permanent absence claim under a stale concurrent writer:
    /// an old-epoch candidate or deleted-family part created after its
    /// lexicographic position was passed belongs to a later explicit cycle.
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectIndexPayload {
    digest: String,
    size: u64,
    part_size: u64,
    part_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum ObjectPublicationHead {
    Published { content: ObjectIndexPayload },
    Deleted { content: ObjectIndexPayload },
}

impl ObjectPublicationHead {
    fn content(&self) -> &ObjectIndexPayload {
        match self {
            Self::Published { content } | Self::Deleted { content } => content,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectFamilyEntry {
    Index,
    Part,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObjectFamilyInventory {
    index_present: bool,
    part_count: u64,
}

#[cfg(any(test, feature = "azure", feature = "gcp"))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalLayoutMarker {
    layout_version: String,
}

#[cfg(any(test, feature = "azure", feature = "gcp"))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryCapabilityMarker {
    marker_version: String,
    store_binding: String,
    ordinal: u8,
}

#[derive(Serialize)]
struct UploadIdentity<'a> {
    store_binding: &'a str,
    write_id: &'a str,
}

#[derive(Serialize)]
struct ChunkGenerationIdentity<'a> {
    store_binding: &'a str,
    upload_id: &'a str,
}

#[cfg(test)]
struct ChunkCandidatePause {
    reached: SyncSender<()>,
    resume: Receiver<()>,
}

#[cfg(test)]
#[derive(Default)]
struct TestFaults {
    publishing_receipt: AtomicBool,
    chunk_ack_receipt: AtomicBool,
    chunk_candidate: AtomicBool,
    content_part: AtomicBool,
    cleanup_plan: AtomicBool,
    upload_gc_ack: AtomicBool,
    chunk_candidate_pause: Mutex<Option<ChunkCandidatePause>>,
    begin_upload_pause: Mutex<Option<ChunkCandidatePause>>,
}

#[cfg(test)]
impl TestFaults {
    fn pause(point: &Mutex<Option<ChunkCandidatePause>>) {
        let pause = point.lock().expect("test pause remains healthy").take();
        if let Some(pause) = pause {
            pause
                .reached
                .send(())
                .expect("test pause observer remains connected");
            pause.resume.recv().expect("test pause resumes");
        }
    }
}

/// Synchronous Cymule adapter over one admitted asynchronous object store.
///
/// Arbitrary Apache backends are not inventory authority and have no public
/// constructor:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use cymule_resource_object_store::ObjectResourceStore;
/// use object_store::memory::InMemory;
///
/// let _ = ObjectResourceStore::new(
///     Arc::new(InMemory::new()),
///     "resources",
///     "object:unreviewed",
/// );
/// ```
pub struct ObjectResourceStore {
    store: Arc<dyn ObjectStoreInventory>,
    runtime: Option<Runtime>,
    prefix: String,
    binding: String,
    #[cfg(test)]
    test_faults: TestFaults,
}

impl ObjectResourceStore {
    /// Construct an adapter over an admitted Google Cloud Storage provider.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, an unsupported physical
    /// layout, a failed inventory canary, or a provider failure.
    #[cfg(feature = "gcp")]
    pub fn from_google_cloud(
        store: GoogleCloudInventory,
        prefix: impl Into<String>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::from_inventory(Arc::new(store), prefix, binding)
    }

    /// Construct an adapter over an admitted Azure Blob provider.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, an unsupported physical
    /// layout, a failed inventory canary, or a provider failure.
    #[cfg(feature = "azure")]
    pub fn from_azure_blob(
        store: AzureBlobInventory,
        prefix: impl Into<String>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::from_inventory(Arc::new(store), prefix, binding)
    }

    #[cfg(test)]
    fn new(
        store: Arc<dyn ObjectStoreInventory>,
        prefix: impl Into<String>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::from_inventory(store, prefix, binding)
    }

    /// Construct the shared adapter after a closed provider constructor has
    /// established the concrete inventory authority.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, an unsupported physical
    /// layout, a backend without required conditional operations, or runtime
    /// construction and provider failures.
    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn from_inventory(
        store: Arc<dyn ObjectStoreInventory>,
        prefix: impl Into<String>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        let binding = binding.into();
        cymule_core::validate_identity("object-store Resource binding", &binding)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if prefix.chars().any(char::is_control)
            || prefix.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(ResourceError::Validation(
                "object-store prefix is invalid".to_owned(),
            ));
        }
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("cymule-object-store")
            .build()
            .map_err(substrate)?;
        let adapter = Self {
            store,
            runtime: Some(runtime),
            prefix,
            binding,
            #[cfg(test)]
            test_faults: TestFaults::default(),
        };
        adapter.initialize_physical_layout()?;
        adapter.check_inventory_canary()?;
        adapter.initialize_upload_gc()?;
        Ok(adapter)
    }

    /// Immutable resolver/store binding retained by Resource locations.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Advance one bounded step of immutable upload-content reclamation.
    ///
    /// A new cycle first advances the global content epoch, then fences or
    /// incrementally migrates every open/publishing upload head before it can
    /// sweep older epochs. It then scans the binding's published families in
    /// bounded pages and removes only payload protected by a permanent Deleted
    /// index. Live publications and terminal index fences are never removed.
    /// A `complete` result closes only the just-finished cursor traversal;
    /// callers must schedule future cycles for late stale-writer candidates,
    /// including parts created after lifecycle deletion completed.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt reclamation authority, stale conditional
    /// updates, malformed provider inventory, or provider failure.
    pub fn reconcile_upload_content(&mut self) -> ResourceResult<UploadContentReclamation> {
        let (record, version) = self.load_upload_gc_record()?;
        match &record.phase {
            UploadGcPhase::Idle => self.begin_upload_gc_cycle(record, version),
            UploadGcPhase::FenceUploads { .. } => self.reconcile_upload_gc_heads(record, version),
            UploadGcPhase::SweepContent { .. } => self.reconcile_upload_gc_objects(record, version),
            UploadGcPhase::SweepDeletedObjects { .. } => {
                self.reconcile_deleted_object_page(record, version)
            }
        }
    }

    fn begin_upload_gc_cycle(
        &self,
        mut record: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        record.current_epoch = record
            .current_epoch
            .checked_add(1)
            .filter(|epoch| *epoch <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store upload-content epoch is exhausted".to_owned(),
                )
            })?;
        record.phase = UploadGcPhase::FenceUploads { after: None };
        self.update_upload_gc_record(&record, version)?;
        Ok(UploadContentReclamation {
            current_epoch: record.current_epoch,
            examined_uploads: 0,
            examined_objects: 0,
            confirmed_absent_objects: 0,
            complete: false,
        })
    }

    fn reconcile_upload_gc_heads(
        &self,
        mut gc: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        let UploadGcPhase::FenceUploads { after } = &gc.phase else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload reclamation changed phase".to_owned(),
            ));
        };
        let prior_after = after.clone();
        let upload_prefix = self.upload_prefix()?;
        let (paths, end_of_inventory) =
            self.list_object_page(&upload_prefix, prior_after.as_deref())?;
        let mut examined_uploads = 0_u64;
        let mut completed_after = prior_after;
        for path in paths {
            let upload_id = self.upload_id_from_record_path(&path)?;
            let (mut upload, upload_version) = self.load_record(&upload_id)?;
            examined_uploads = examined_uploads.checked_add(1).ok_or_else(|| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store upload scan count overflow".to_owned(),
                )
            })?;
            if self.abort_deleted_publication(&mut upload, upload_version.clone())? {
                completed_after = Some(Self::relative_inventory_path(&upload_prefix, &path)?);
                continue;
            }
            if matches!(upload.state, UploadState::Open | UploadState::Publishing)
                && !self.migrate_upload_content_page(
                    &mut upload,
                    upload_version,
                    gc.current_epoch,
                )?
            {
                gc.phase = UploadGcPhase::FenceUploads {
                    after: completed_after,
                };
                self.update_upload_gc_record(&gc, version)?;
                return Ok(UploadContentReclamation {
                    current_epoch: gc.current_epoch,
                    examined_uploads,
                    examined_objects: 0,
                    confirmed_absent_objects: 0,
                    complete: false,
                });
            }
            completed_after = Some(Self::relative_inventory_path(&upload_prefix, &path)?);
        }
        gc.phase = if end_of_inventory {
            UploadGcPhase::SweepContent {
                after: None,
                deleted_in_pass: 0,
                page: None,
            }
        } else {
            UploadGcPhase::FenceUploads {
                after: completed_after,
            }
        };
        self.update_upload_gc_record(&gc, version)?;
        Ok(UploadContentReclamation {
            current_epoch: gc.current_epoch,
            examined_uploads,
            examined_objects: 0,
            confirmed_absent_objects: 0,
            complete: false,
        })
    }

    fn reconcile_upload_gc_objects(
        &self,
        mut gc: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        let UploadGcPhase::SweepContent {
            after,
            deleted_in_pass,
            page,
        } = &gc.phase
        else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload reclamation changed phase".to_owned(),
            ));
        };
        let prior_after = after.clone();
        let prior_deleted = *deleted_in_pass;
        let content_prefix = self.upload_content_prefix()?;
        let Some(page) = page.clone() else {
            return self.prepare_upload_gc_page(gc, version);
        };
        let mut examined_objects = 0_u64;
        let mut confirmed_absent_objects = 0_u64;
        let mut completed_after = prior_after;
        for relative in &page.paths {
            let epoch = Self::content_epoch_from_relative_path(relative)?;
            let path = Path::parse(format!("{content_prefix}/{relative}")).map_err(substrate)?;
            examined_objects = examined_objects.checked_add(1).ok_or_else(|| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store content scan count overflow".to_owned(),
                )
            })?;
            if epoch < gc.current_epoch {
                self.delete_if_present(&path)?;
                if !self.is_absent(&path)? {
                    return Err(integrity(
                        "object_store_gc_authority_invalid",
                        "object-store deletion was not immediately visible to exact reads"
                            .to_owned(),
                    ));
                }
                confirmed_absent_objects =
                    confirmed_absent_objects.checked_add(1).ok_or_else(|| {
                        integrity(
                            "object_store_gc_authority_invalid",
                            "object-store confirmed-absence count overflow".to_owned(),
                        )
                    })?;
            } else {
                self.require_inventory_object(&path, &gc, &version)?;
            }
            completed_after = Some(relative.clone());
        }
        #[cfg(test)]
        if let Ok(marker) = std::env::var("CYMULE_OBJECT_STORE_GC_DELETE_MARKER") {
            std::fs::write(marker, b"gc-page-deleted").expect("GC deletion barrier persists");
            loop {
                std::thread::park_timeout(std::time::Duration::from_mins(1));
            }
        }
        let deleted_in_pass = prior_deleted
            .checked_add(confirmed_absent_objects)
            .filter(|count| *count <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store reclamation pass count exceeds exact integers".to_owned(),
                )
            })?;
        let sweep_complete = page.end_of_inventory && deleted_in_pass == 0;
        gc.phase = if sweep_complete {
            UploadGcPhase::SweepDeletedObjects {
                after: None,
                deleted_in_pass: 0,
                page: None,
            }
        } else if page.end_of_inventory {
            UploadGcPhase::SweepContent {
                after: None,
                deleted_in_pass: 0,
                page: None,
            }
        } else {
            UploadGcPhase::SweepContent {
                after: completed_after,
                deleted_in_pass,
                page: None,
            }
        };
        self.update_upload_gc_record(&gc, version)?;
        Ok(UploadContentReclamation {
            current_epoch: gc.current_epoch,
            examined_uploads: 0,
            examined_objects,
            confirmed_absent_objects,
            complete: false,
        })
    }

    fn prepare_upload_gc_page(
        &self,
        mut gc: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        let UploadGcPhase::SweepContent {
            after,
            deleted_in_pass,
            page: None,
        } = &gc.phase
        else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store reclamation cannot select another deletion page".to_owned(),
            ));
        };
        let content_prefix = self.upload_content_prefix()?;
        let (paths, end_of_inventory) = self.list_object_page(&content_prefix, after.as_deref())?;
        let mut relative_paths = Vec::with_capacity(paths.len());
        for path in paths {
            let epoch = self.content_epoch_from_path(&path)?;
            self.require_inventory_object(&path, &gc, &version)?;
            if epoch > gc.current_epoch {
                self.verify_upload_gc_source(&gc, &version)?;
                return Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store upload content is from a future epoch".to_owned(),
                ));
            }
            relative_paths.push(Self::relative_inventory_path(&content_prefix, &path)?);
        }
        let examined_objects = u64::try_from(relative_paths.len()).map_err(|_| {
            integrity(
                "object_store_gc_authority_invalid",
                "object-store content page count cannot be represented".to_owned(),
            )
        })?;
        gc.phase = UploadGcPhase::SweepContent {
            after: after.clone(),
            deleted_in_pass: *deleted_in_pass,
            page: Some(UploadGcPage {
                paths: relative_paths,
                end_of_inventory,
            }),
        };
        self.update_upload_gc_record(&gc, version)?;
        Ok(UploadContentReclamation {
            current_epoch: gc.current_epoch,
            examined_uploads: 0,
            examined_objects,
            confirmed_absent_objects: 0,
            complete: false,
        })
    }

    fn reconcile_deleted_object_page(
        &self,
        mut gc: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        let UploadGcPhase::SweepDeletedObjects {
            deleted_in_pass,
            page,
            ..
        } = &gc.phase
        else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store deleted-payload reclamation changed phase".to_owned(),
            ));
        };
        let prior_deleted = *deleted_in_pass;
        let Some(page) = page.clone() else {
            return self.prepare_deleted_object_page(gc, version);
        };
        // Validate the whole retained plan before deleting its first member.
        // A concurrent lifecycle deleter does not move the GC head, so absence
        // under this exact irreversible tombstone is valid replay authority.
        for target in &page.targets {
            self.verify_deleted_gc_target(target)?;
        }
        let prefix = self.objects_prefix()?;
        for target in &page.targets {
            let path = Path::parse(format!("{prefix}/{}", target.path)).map_err(substrate)?;
            self.delete_if_present(&path)?;
            if !self.is_absent(&path)? {
                return Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store deleted-publication payload remains present".to_owned(),
                ));
            }
        }
        let confirmed_absent_objects = page.targets.len() as u64;
        let deleted_in_pass = prior_deleted
            .checked_add(confirmed_absent_objects)
            .filter(|count| *count <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store deleted-payload count exceeds exact integers".to_owned(),
                )
            })?;
        let complete = page.end_of_inventory && deleted_in_pass == 0;
        gc.phase = if complete {
            UploadGcPhase::Idle
        } else if page.end_of_inventory {
            UploadGcPhase::SweepDeletedObjects {
                after: None,
                deleted_in_pass: 0,
                page: None,
            }
        } else {
            UploadGcPhase::SweepDeletedObjects {
                after: page.completed_after,
                deleted_in_pass,
                page: None,
            }
        };
        self.update_upload_gc_record(&gc, version)?;
        Ok(UploadContentReclamation {
            current_epoch: gc.current_epoch,
            examined_uploads: 0,
            examined_objects: page.examined_objects,
            confirmed_absent_objects,
            complete,
        })
    }

    fn prepare_deleted_object_page(
        &self,
        mut gc: UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UploadContentReclamation> {
        let UploadGcPhase::SweepDeletedObjects {
            after,
            deleted_in_pass,
            page: None,
        } = &gc.phase
        else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store deleted-payload reclamation already has a page".to_owned(),
            ));
        };
        let prefix = self.objects_prefix()?;
        let (paths, end_of_inventory) = self.list_object_page(&prefix, after.as_deref())?;
        let examined_objects = paths.len() as u64;
        let mut completed_after = after.clone();
        let mut targets = Vec::new();
        for path in paths {
            let relative = Self::relative_inventory_path(&prefix, &path)?;
            let (digest, part_index) = Self::object_member_from_relative_path(&relative)?;
            completed_after = Some(relative.clone());
            let Some(part_index) = part_index else {
                continue;
            };
            let (record, head, _) = match self.load_object_head(&digest) {
                Ok(value) => value,
                // Parts from an in-progress publication have no visible head
                // yet; neither absence nor a live head authorizes collection.
                Err(ResourceError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            };
            let ObjectPublicationHead::Deleted { content } = head else {
                continue;
            };
            let target = DeletedObjectGcTarget {
                path: relative,
                tombstone_id: record.record_id,
                size: object_part_size(content.size, part_index)?,
            };
            self.verify_deleted_gc_target(&target)?;
            targets.push(target);
        }
        gc.phase = UploadGcPhase::SweepDeletedObjects {
            after: after.clone(),
            deleted_in_pass: *deleted_in_pass,
            page: Some(DeletedObjectGcPage {
                examined_objects,
                completed_after,
                targets,
                end_of_inventory,
            }),
        };
        self.update_upload_gc_record(&gc, version)?;
        Ok(UploadContentReclamation {
            current_epoch: gc.current_epoch,
            examined_uploads: 0,
            examined_objects,
            confirmed_absent_objects: 0,
            complete: false,
        })
    }

    fn verify_deleted_gc_target(&self, target: &DeletedObjectGcTarget) -> ResourceResult<()> {
        let (digest, part_index) = Self::object_member_from_relative_path(&target.path)?;
        let part_index = part_index.ok_or_else(|| {
            integrity(
                "object_store_gc_authority_invalid",
                "object-store reclamation cannot remove an index fence".to_owned(),
            )
        })?;
        let (record, head, _) = self
            .load_object_head(&digest)
            .map_err(|error| match error {
                ResourceError::NotFound(_) => integrity(
                    "object_store_gc_authority_invalid",
                    "object-store reclamation lost its permanent deletion fence".to_owned(),
                ),
                other => other,
            })?;
        let ObjectPublicationHead::Deleted { content } = head else {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store reclamation target has no permanent deletion fence".to_owned(),
            ));
        };
        if record.record_id != target.tombstone_id
            || object_part_size(content.size, part_index)? != target.size
        {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store reclamation target changed its exact tombstone".to_owned(),
            ));
        }
        let path = self.object_part_path(&digest, part_index)?;
        let store = Arc::clone(&self.store);
        match self.block_on(async move { store.head(&path).await })? {
            Ok(metadata) if metadata.size == target.size => Ok(()),
            Err(ObjectError::NotFound { .. }) => Ok(()),
            Ok(_) => Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store retired part changed its exact target size".to_owned(),
            )),
            Err(error) => Err(object_error(error)),
        }
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> ResourceResult<T> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            substrate_with_code(
                "object_store_runtime_closed",
                "object runtime is closed".to_owned(),
            )
        })?;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                return Err(substrate_with_code(
                    "object_store_runtime_reentry",
                    "call synchronous object-store methods from spawn_blocking".to_owned(),
                ));
            }
            Ok(tokio::task::block_in_place(|| runtime.block_on(future)))
        } else {
            Ok(runtime.block_on(future))
        }
    }

    fn key(&self, suffix: &str) -> ResourceResult<Path> {
        let value = if self.prefix.is_empty() {
            suffix.to_owned()
        } else {
            format!("{}/{suffix}", self.prefix)
        };
        Path::parse(value).map_err(substrate)
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn initialize_physical_layout(&self) -> ResourceResult<()> {
        let marker_path = self.key(PHYSICAL_LAYOUT_MARKER)?;
        match self.read_layout_marker(&marker_path) {
            Ok(()) => return Ok(()),
            Err(ResourceError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let store = Arc::clone(&self.store);
        let prefix = if self.prefix.is_empty() {
            None
        } else {
            Some(Path::parse(&self.prefix).map_err(substrate)?)
        };
        let first = self.block_on(async move { store.list(prefix.as_ref()).next().await })?;
        if let Some(result) = first {
            let existing = result.map_err(object_error)?;
            if existing.location == marker_path {
                return self.read_layout_marker(&marker_path);
            }
            return Err(integrity(
                "object_store_layout_invalid",
                format!(
                    "unsupported object-store Resource physical generation: marker is absent before existing object {}",
                    existing.location
                ),
            ));
        }
        let marker = PhysicalLayoutMarker {
            layout_version: PHYSICAL_LAYOUT_VERSION.to_owned(),
        };
        let bytes = cymule_core::canonical_bytes(&marker).map_err(core_error)?;
        let store = Arc::clone(&self.store);
        let path = marker_path.clone();
        match self.block_on(async move {
            store
                .put_opts(
                    &path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        })? {
            Ok(_) | Err(ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. }) => {
                self.read_layout_marker(&marker_path)
            }
            Err(error) => Err(object_error(error)),
        }
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn read_layout_marker(&self, marker_path: &Path) -> ResourceResult<()> {
        let store = Arc::clone(&self.store);
        let path = marker_path.clone();
        let result = self.block_on(async move { store.get(&path).await })?;
        let result = match result {
            Ok(result) => result,
            Err(ObjectError::NotFound { .. }) => {
                return Err(ResourceError::NotFound(
                    "object-store physical generation marker".to_owned(),
                ));
            }
            Err(error) => return Err(object_error(error)),
        };
        let expected_size = result.meta.size;
        if expected_size > 4096 {
            return Err(integrity(
                "object_store_layout_invalid",
                "object-store physical generation marker is oversized".to_owned(),
            ));
        }
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        if u64::try_from(bytes.len()).map_err(|_| {
            integrity(
                "object_store_layout_invalid",
                "object-store physical generation marker exceeds platform bounds".to_owned(),
            )
        })? != expected_size
        {
            return Err(integrity(
                "object_store_layout_invalid",
                "object-store physical generation marker changed size while reading".to_owned(),
            ));
        }
        let marker: PhysicalLayoutMarker =
            cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        if marker.layout_version != PHYSICAL_LAYOUT_VERSION {
            return Err(integrity(
                "object_store_layout_invalid",
                format!(
                    "unsupported object-store Resource physical generation {}",
                    marker.layout_version
                ),
            ));
        }
        if cymule_core::canonical_bytes(&marker)
            .map_err(core_error)?
            .as_slice()
            != bytes.as_ref()
        {
            return Err(integrity(
                "object_store_layout_invalid",
                "object-store physical generation marker is not canonical".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn inventory_capability_prefix(&self) -> ResourceResult<Path> {
        self.key(&format!(
            "inventory-capability/{}",
            self.content_namespace()
        ))
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn inventory_capability_path(&self, ordinal: u8) -> ResourceResult<Path> {
        self.key(&format!(
            "inventory-capability/{}/{ordinal:02}.json",
            self.content_namespace()
        ))
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn inventory_probe_page(
        &self,
        prefix: &Path,
        after: Option<&Path>,
        limit: usize,
    ) -> ResourceResult<Vec<Path>> {
        let store = Arc::clone(&self.store);
        let prefix = prefix.clone();
        let after = after.cloned();
        self.block_on(async move {
            let mut inventory = store.ordered_inventory(Some(&prefix), after.as_ref());
            let mut paths = Vec::with_capacity(limit);
            while paths.len() < limit {
                let Some(item) = inventory.next().await else {
                    break;
                };
                paths.push(item.map_err(object_error)?.location);
            }
            Ok::<_, ResourceError>(paths)
        })?
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn check_inventory_canary(&self) -> ResourceResult<()> {
        let prefix = self.inventory_capability_prefix()?;
        let mut expected = Vec::with_capacity(3);
        for ordinal in 0..3 {
            let marker = InventoryCapabilityMarker {
                marker_version: INVENTORY_CAPABILITY_VERSION.to_owned(),
                store_binding: self.binding_id(),
                ordinal,
            };
            let path = self.inventory_capability_path(ordinal)?;
            self.put_immutable_bytes(
                &path,
                cymule_core::canonical_bytes(&marker).map_err(core_error)?,
            )?;
            expected.push(path);
        }

        let complete = self.inventory_probe_page(&prefix, None, expected.len() + 1)?;
        if complete != expected {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store provider failed its complete strong ordered-inventory startup canary"
                    .to_owned(),
            ));
        }
        let suffix = self.inventory_probe_page(&prefix, expected.first(), expected.len())?;
        if suffix != expected[1..] {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store provider failed its exclusive inventory-continuation startup canary"
                    .to_owned(),
            ));
        }
        if !self
            .inventory_probe_page(&prefix, expected.last(), 1)?
            .is_empty()
        {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store provider repeated inventory after its terminal cursor".to_owned(),
            ));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "azure", feature = "gcp"))]
    fn initialize_upload_gc(&self) -> ResourceResult<()> {
        let record = UploadGcRecord {
            record_version: UPLOAD_GC_RECORD_VERSION.to_owned(),
            store_binding: self.binding_id(),
            current_epoch: 0,
            phase: UploadGcPhase::Idle,
        };
        match self.put_upload_gc_record(&record, PutMode::Create) {
            Ok(_) => {
                let _ = self.load_upload_gc_record()?;
            }
            Err(ResourceError::Conflict { ref code, .. }) if is_create_conflict(code) => {
                let _ = self.load_upload_gc_record()?;
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    fn verify_upload_gc_record(&self, record: &UploadGcRecord) -> ResourceResult<()> {
        if record.record_version != UPLOAD_GC_RECORD_VERSION
            || record.store_binding != self.binding_id()
            || record.current_epoch > cymule_core::MAX_EXACT_INTEGER
        {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload-content reclamation head is malformed".to_owned(),
            ));
        }
        match &record.phase {
            UploadGcPhase::Idle => {}
            UploadGcPhase::FenceUploads { after } => {
                if let Some(after) = after {
                    let _ = Self::upload_id_from_relative_record_path(after)?;
                }
            }
            UploadGcPhase::SweepContent {
                after,
                deleted_in_pass,
                page,
            } => {
                if *deleted_in_pass > cymule_core::MAX_EXACT_INTEGER {
                    return Err(integrity(
                        "object_store_gc_authority_invalid",
                        "object-store upload reclamation count exceeds exact integers".to_owned(),
                    ));
                }
                if let Some(after) = after {
                    let _ = Self::content_epoch_from_relative_path(after)?;
                }
                if let Some(page) = page {
                    Self::verify_upload_gc_page(record.current_epoch, after.as_deref(), page)?;
                }
            }
            UploadGcPhase::SweepDeletedObjects {
                after,
                deleted_in_pass,
                page,
            } => {
                if *deleted_in_pass > cymule_core::MAX_EXACT_INTEGER {
                    return Err(integrity(
                        "object_store_gc_authority_invalid",
                        "object-store retired-payload count exceeds exact integers".to_owned(),
                    ));
                }
                if let Some(after) = after {
                    let _ = Self::object_member_from_relative_path(after)?;
                }
                if let Some(page) = page {
                    Self::verify_deleted_object_gc_page(after.as_deref(), page)?;
                }
            }
        }
        Ok(())
    }

    fn verify_upload_gc_page(
        current_epoch: u64,
        after: Option<&str>,
        page: &UploadGcPage,
    ) -> ResourceResult<()> {
        if page.paths.len() > UPLOAD_GC_OBJECT_PAGE
            || (page.paths.is_empty() && !page.end_of_inventory)
        {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload reclamation page has invalid cardinality".to_owned(),
            ));
        }
        let mut previous = after;
        for path in &page.paths {
            let path_bytes = u64::try_from(path.len()).map_err(|_| {
                integrity(
                    "object_store_gc_authority_invalid",
                    "object-store upload reclamation path exceeds platform bounds".to_owned(),
                )
            })?;
            if path_bytes > MAX_UPLOAD_CONTENT_RELATIVE_PATH_BYTES
                || previous.is_some_and(|previous| path.as_str() <= previous)
                || Self::content_epoch_from_relative_path(path)? > current_epoch
            {
                return Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store upload reclamation page is malformed".to_owned(),
                ));
            }
            previous = Some(path);
        }
        Ok(())
    }

    fn verify_deleted_object_gc_page(
        after: Option<&str>,
        page: &DeletedObjectGcPage,
    ) -> ResourceResult<()> {
        if page.examined_objects > UPLOAD_GC_OBJECT_PAGE as u64
            || page.targets.len() as u64 > page.examined_objects
            || (!page.end_of_inventory && page.examined_objects != UPLOAD_GC_OBJECT_PAGE as u64)
        {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store retired-payload page has invalid cardinality".to_owned(),
            ));
        }
        if page.examined_objects == 0 {
            if page.completed_after.as_deref() != after || !page.end_of_inventory {
                return Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store empty retired-payload page advanced its cursor".to_owned(),
                ));
            }
            return Ok(());
        }
        let completed_after = page.completed_after.as_deref().ok_or_else(|| {
            integrity(
                "object_store_gc_authority_invalid",
                "object-store retired-payload page lacks its examined frontier".to_owned(),
            )
        })?;
        let _ = Self::object_member_from_relative_path(completed_after)?;
        if after.is_some_and(|after| completed_after <= after) {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store retired-payload page did not advance its cursor".to_owned(),
            ));
        }
        let mut previous = after;
        for target in &page.targets {
            let (_, part) = Self::object_member_from_relative_path(&target.path)?;
            if part.is_none()
                || target.size == 0
                || target.size > PART_SIZE as u64
                || digest_key(&target.tombstone_id).is_err()
                || previous.is_some_and(|previous| target.path.as_str() <= previous)
                || target.path.as_str() > completed_after
            {
                return Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store retired-payload target escaped its exact page".to_owned(),
                ));
            }
            previous = Some(&target.path);
        }
        Ok(())
    }

    fn load_upload_gc_record(&self) -> ResourceResult<(UploadGcRecord, UpdateVersion)> {
        let path = self.upload_gc_path()?;
        let store = Arc::clone(&self.store);
        let result = self
            .block_on(async move { store.get(&path).await })?
            .map_err(object_error)?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if result.meta.size == 0 || result.meta.size > MAX_UPLOAD_GC_RECORD_BYTES {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                format!(
                    "object-store upload-content reclamation head exceeds {MAX_UPLOAD_GC_RECORD_BYTES} bytes"
                ),
            ));
        }
        let expected_size = result.meta.size;
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        if bytes.len() as u64 != expected_size {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload-content reclamation head changed while reading".to_owned(),
            ));
        }
        let record: UploadGcRecord = cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        self.verify_upload_gc_record(&record)?;
        if cymule_core::canonical_bytes(&record)
            .map_err(core_error)?
            .as_slice()
            != bytes.as_ref()
        {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                "object-store upload-content reclamation head is not canonical".to_owned(),
            ));
        }
        Ok((record, version))
    }

    fn put_upload_gc_record(
        &self,
        record: &UploadGcRecord,
        mode: PutMode,
    ) -> ResourceResult<UpdateVersion> {
        self.verify_upload_gc_record(record)?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        if bytes.len() as u64 > MAX_UPLOAD_GC_RECORD_BYTES {
            return Err(integrity(
                "object_store_gc_authority_invalid",
                format!(
                    "object-store upload-content reclamation head exceeds {MAX_UPLOAD_GC_RECORD_BYTES} bytes"
                ),
            ));
        }
        let path = self.upload_gc_path()?;
        let store = Arc::clone(&self.store);
        let result = self
            .block_on(async move {
                store
                    .put_opts(
                        &path,
                        bytes.into(),
                        PutOptions {
                            mode,
                            ..Default::default()
                        },
                    )
                    .await
            })?
            .map(Into::into)
            .map_err(object_error);
        #[cfg(test)]
        if result.is_ok() && self.test_faults.upload_gc_ack.swap(false, Ordering::SeqCst) {
            return Err(substrate_with_code(
                "object_store_gc_acknowledgement_lost",
                "injected lost upload-content GC acknowledgement".to_owned(),
            ));
        }
        result
    }

    fn upload_id(&self, write_id: &str) -> ResourceResult<String> {
        let identity = cymule_core::content_id(
            BINDING_VERSION,
            &UploadIdentity {
                store_binding: &self.binding,
                write_id,
            },
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        Ok(format!(
            "upload:{}",
            identity
                .strip_prefix("sha256:")
                .expect("content identity has SHA-256 prefix")
        ))
    }

    fn upload_key(upload_id: &str) -> ResourceResult<&str> {
        let key = upload_id.strip_prefix("upload:").ok_or_else(|| {
            ResourceError::Validation("invalid object-store upload identity".to_owned())
        })?;
        if key.len() != 64
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ResourceError::Validation(
                "object-store upload identity must be an exact lowercase SHA-256 key".to_owned(),
            ));
        }
        Ok(key)
    }

    fn validate_session(&self, session: &ResourceWriteSession) -> ResourceResult<()> {
        if session.store_binding != self.binding
            || session.upload_id != self.upload_id(&session.write_id)?
        {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload session is not authenticated by its write ID and binding"
                    .to_owned(),
            ));
        }
        let _ = Self::upload_key(&session.upload_id)?;
        Ok(())
    }

    fn upload_prefix(&self) -> ResourceResult<Path> {
        self.key(&format!("uploads/{}", self.content_namespace()))
    }

    fn record_path(&self, upload_id: &str) -> ResourceResult<Path> {
        Ok(self
            .upload_prefix()?
            .join(Self::upload_key(upload_id)?)
            .join("record.json"))
    }

    fn chunk_generation(&self, upload_id: &str) -> ResourceResult<String> {
        let _ = Self::upload_key(upload_id)?;
        cymule_core::content_id(
            CHUNK_GENERATION_VERSION,
            &ChunkGenerationIdentity {
                store_binding: &self.binding,
                upload_id,
            },
        )
        .map_err(core_error)
    }

    fn chunk_data_path(&self, epoch: u64, digest: &str) -> ResourceResult<Path> {
        self.key(&format!(
            "upload-content/{}/epochs/{epoch:020}/data/{}",
            self.content_namespace(),
            digest_key(digest)?
        ))
    }

    fn chunk_node_path(&self, epoch: u64, node_id: &str) -> ResourceResult<Path> {
        self.key(&format!(
            "upload-content/{}/epochs/{epoch:020}/nodes/{}.json",
            self.content_namespace(),
            digest_key(node_id)?
        ))
    }

    fn upload_content_prefix(&self) -> ResourceResult<Path> {
        self.key(&format!(
            "upload-content/{}/epochs",
            self.content_namespace()
        ))
    }

    fn upload_gc_path(&self) -> ResourceResult<Path> {
        self.key(&format!(
            "upload-content/{}/gc.json",
            self.content_namespace()
        ))
    }

    fn object_index_path(&self, digest: &str) -> ResourceResult<Path> {
        let key = digest_key(digest)?;
        self.key(&format!(
            "objects/{}/{key}/index.json",
            self.content_namespace()
        ))
    }

    fn object_part_path(&self, digest: &str, part_index: u64) -> ResourceResult<Path> {
        let key = digest_key(digest)?;
        self.key(&format!(
            "objects/{}/{key}/parts/{part_index:020}",
            self.content_namespace()
        ))
    }

    fn object_family_prefix(&self, digest: &str) -> ResourceResult<Path> {
        let key = digest_key(digest)?;
        self.key(&format!("objects/{}/{key}", self.content_namespace()))
    }

    fn objects_prefix(&self) -> ResourceResult<Path> {
        self.key(&format!("objects/{}", self.content_namespace()))
    }

    fn content_namespace(&self) -> String {
        hex_digest(&Sha256::digest(self.binding.as_bytes()))
    }

    fn binding_id(&self) -> String {
        format!("sha256:{}", self.content_namespace())
    }

    fn verify_deleted_object_absence(&self, index: &ObjectIndexPayload) -> ResourceResult<()> {
        let (_, head, _) = self.load_object_head(&index.digest)?;
        if head
            != (ObjectPublicationHead::Deleted {
                content: index.clone(),
            })
        {
            return Err(integrity(
                "object_store_cleanup_invalid",
                "object-store deletion lost its permanent exact publication fence".to_owned(),
            ));
        }
        let inventory = self.inspect_object_family(index)?;
        if !inventory.index_present || inventory.part_count != 0 {
            return Err(integrity(
                "object_store_cleanup_invalid",
                "object-store payload remains present after deletion readback".to_owned(),
            ));
        }
        Ok(())
    }

    fn catalog_path(&self, namespace: &str, key: &str) -> ResourceResult<Path> {
        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0]);
        hasher.update(key.as_bytes());
        self.key(&format!(
            "catalog/{}/{}.json",
            self.content_namespace(),
            hex_digest(&hasher.finalize())
        ))
    }

    fn load_record(&self, upload_id: &str) -> ResourceResult<(UploadRecord, UpdateVersion)> {
        let path = self.record_path(upload_id)?;
        let store = Arc::clone(&self.store);
        let result = self
            .block_on(async move { store.get(&path).await })?
            .map_err(object_error)?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let expected_size = result.meta.size;
        if expected_size == 0 || expected_size > MAX_UPLOAD_RECORD_BYTES {
            return Err(integrity(
                "object_store_upload_record_invalid",
                format!(
                    "object-store upload record exceeds its {MAX_UPLOAD_RECORD_BYTES}-byte physical bound"
                ),
            ));
        }
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        if u64::try_from(bytes.len()).map_err(|_| {
            integrity(
                "object_store_upload_record_invalid",
                "object-store upload record exceeds platform bounds".to_owned(),
            )
        })? != expected_size
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload record changed size while reading".to_owned(),
            ));
        }
        let record: UploadRecord = cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        if cymule_core::canonical_bytes(&record)
            .map_err(core_error)?
            .as_slice()
            != bytes.as_ref()
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload record is not canonical".to_owned(),
            ));
        }
        self.verify_upload_record(&record)?;
        if record.upload_id != upload_id {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload record path changed".to_owned(),
            ));
        }
        Ok((record, version))
    }

    fn put_record(&self, record: &UploadRecord, mode: PutMode) -> ResourceResult<UpdateVersion> {
        self.verify_upload_record(record)?;
        let path = self.record_path(&record.upload_id)?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        if u64::try_from(bytes.len()).map_err(|_| {
            ResourceError::Validation(
                "object-store upload record exceeds platform bounds".to_owned(),
            )
        })? > MAX_UPLOAD_RECORD_BYTES
        {
            return Err(ResourceError::Validation(format!(
                "object-store upload record exceeds its {MAX_UPLOAD_RECORD_BYTES}-byte physical bound"
            )));
        }
        let store = Arc::clone(&self.store);
        let result = self
            .block_on(async move {
                store
                    .put_opts(
                        &path,
                        bytes.into(),
                        PutOptions {
                            mode,
                            ..Default::default()
                        },
                    )
                    .await
            })?
            .map_err(object_error)?;
        let version = result.into();
        #[cfg(test)]
        if record.state == UploadState::Publishing
            && self
                .test_faults
                .publishing_receipt
                .swap(false, Ordering::SeqCst)
        {
            return Err(substrate_with_code(
                "object_store_publishing_receipt_lost",
                "injected lost Publishing receipt".to_owned(),
            ));
        }
        Ok(version)
    }

    fn commit_chunk_acknowledgement(
        &self,
        record: &UploadRecord,
        version: UpdateVersion,
        planned: &ChunkLeaf,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        let result = self.put_record(record, PutMode::Update(version));
        #[cfg(test)]
        let result = match result {
            Ok(_)
                if self
                    .test_faults
                    .chunk_ack_receipt
                    .swap(false, Ordering::SeqCst) =>
            {
                Err(substrate_with_code(
                    "object_store_chunk_acknowledgement_lost",
                    "injected lost chunk acknowledgement receipt".to_owned(),
                ))
            }
            other => other,
        };
        if let Err(error) = result {
            let (reopened, _) = self.load_record(&record.upload_id)?;
            if reopened.next_offset >= record.next_offset {
                let retained = self.find_chunk_leaf(&reopened, planned.offset)?;
                if retained.as_ref() == Some(planned)
                    && self.chunk_bytes(&reopened, planned)? == bytes
                {
                    return Ok(());
                }
            }
            return Err(error);
        }
        Ok(())
    }

    fn update_upload_gc_record(
        &self,
        record: &UploadGcRecord,
        version: UpdateVersion,
    ) -> ResourceResult<UpdateVersion> {
        match self.put_upload_gc_record(record, PutMode::Update(version)) {
            Ok(version) => Ok(version),
            Err(error) => {
                let (retained, retained_version) = self.load_upload_gc_record()?;
                if retained == *record {
                    Ok(retained_version)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn list_object_page(
        &self,
        prefix: &Path,
        after: Option<&str>,
    ) -> ResourceResult<(Vec<Path>, bool)> {
        let store = Arc::clone(&self.store);
        let prefix = prefix.clone();
        let after = after
            .map(|relative| Path::parse(format!("{prefix}/{relative}")))
            .transpose()
            .map_err(substrate)?;
        self.block_on(async move {
            let mut stream = store.ordered_inventory(Some(&prefix), after.as_ref());
            let child_prefix = format!("{prefix}/");
            let mut paths = Vec::with_capacity(UPLOAD_GC_OBJECT_PAGE);
            let mut previous = after;
            while paths.len() < UPLOAD_GC_OBJECT_PAGE {
                let Some(result) = stream.next().await else {
                    return Ok::<_, ResourceError>((paths, true));
                };
                let metadata = result.map_err(object_error)?;
                if !metadata.location.to_string().starts_with(&child_prefix)
                    || previous
                        .as_ref()
                        .is_some_and(|previous| metadata.location <= *previous)
                {
                    return Err(integrity(
                        "object_store_inventory_contract_violation",
                        "object-store inventory escaped its prefix or lost strict ordering"
                            .to_owned(),
                    ));
                }
                previous = Some(metadata.location.clone());
                paths.push(metadata.location);
            }
            Ok((paths, false))
        })?
    }

    fn relative_inventory_path(prefix: &Path, path: &Path) -> ResourceResult<String> {
        let prefix = format!("{prefix}/");
        path.to_string()
            .strip_prefix(&prefix)
            .filter(|relative| !relative.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                integrity(
                    "object_store_inventory_contract_violation",
                    "object-store inventory path escaped its exact prefix".to_owned(),
                )
            })
    }

    fn upload_id_from_record_path(&self, path: &Path) -> ResourceResult<String> {
        let relative = Self::relative_inventory_path(&self.upload_prefix()?, path)?;
        Self::upload_id_from_relative_record_path(&relative)
    }

    fn upload_id_from_relative_record_path(relative: &str) -> ResourceResult<String> {
        let key = relative.strip_suffix("/record.json").ok_or_else(|| {
            integrity(
                "object_store_inventory_contract_violation",
                "object-store upload namespace contains a non-head object".to_owned(),
            )
        })?;
        if key.contains('/') {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store upload-head path has unexpected depth".to_owned(),
            ));
        }
        let upload_id = format!("upload:{key}");
        let _ = Self::upload_key(&upload_id)?;
        Ok(upload_id)
    }

    fn content_epoch_from_path(&self, path: &Path) -> ResourceResult<u64> {
        let relative = Self::relative_inventory_path(&self.upload_content_prefix()?, path)?;
        Self::content_epoch_from_relative_path(&relative)
    }

    fn content_epoch_from_relative_path(relative: &str) -> ResourceResult<u64> {
        let (epoch, suffix) = relative.split_once('/').ok_or_else(|| {
            integrity(
                "object_store_inventory_contract_violation",
                "object-store upload-content object lacks its epoch".to_owned(),
            )
        })?;
        let valid_suffix = suffix
            .strip_prefix("data/")
            .is_some_and(Self::is_digest_key)
            || suffix
                .strip_prefix("nodes/")
                .and_then(|node| node.strip_suffix(".json"))
                .is_some_and(Self::is_digest_key);
        if epoch.len() != 20 || !epoch.bytes().all(|byte| byte.is_ascii_digit()) || !valid_suffix {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store upload-content object has a malformed physical path".to_owned(),
            ));
        }
        let epoch = epoch.parse::<u64>().map_err(core_integrity)?;
        if epoch > cymule_core::MAX_EXACT_INTEGER {
            return Err(integrity(
                "object_store_inventory_contract_violation",
                "object-store upload-content epoch exceeds exact integers".to_owned(),
            ));
        }
        Ok(epoch)
    }

    fn object_member_from_relative_path(relative: &str) -> ResourceResult<(String, Option<u64>)> {
        let malformed = || {
            integrity(
                "object_store_inventory_contract_violation",
                "object-store publication inventory has a malformed physical path".to_owned(),
            )
        };
        if relative.len() as u64 > MAX_OBJECT_RELATIVE_PATH_BYTES {
            return Err(malformed());
        }
        let (key, suffix) = relative.split_once('/').ok_or_else(malformed)?;
        if !Self::is_digest_key(key) {
            return Err(malformed());
        }
        let digest = format!("sha256:{key}");
        if suffix == "index.json" {
            return Ok((digest, None));
        }
        let encoded = suffix.strip_prefix("parts/").ok_or_else(malformed)?;
        if encoded.len() != 20 || !encoded.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(malformed());
        }
        let part_index = encoded.parse::<u64>().map_err(|_| malformed())?;
        if part_index >= object_part_count(cymule_core::MAX_EXACT_INTEGER) {
            return Err(malformed());
        }
        Ok((digest, Some(part_index)))
    }

    fn is_digest_key(key: &str) -> bool {
        key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    fn publication_for_record(
        &self,
        record: &UploadRecord,
    ) -> ResourceResult<Option<ResourcePublication>> {
        let Some(retained) = &record.publication else {
            return Ok(None);
        };
        if retained.size > cymule_core::MAX_EXACT_INTEGER || digest_key(&retained.digest).is_err() {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload publication descriptor is malformed".to_owned(),
            ));
        }
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: record.intent.shape,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: retained.digest.clone(),
                size: retained.size,
            },
            manifest: None,
            annotations: record.intent.annotations.clone(),
        }
        .seal()
        .map_err(|error| integrity("object_store_upload_record_invalid", error.to_string()))?;
        let publication = ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: self.binding.clone(),
                locations: vec![ResourceLocation::Opaque {
                    reference: retained.digest.clone(),
                }],
            },
            resource,
        };
        publication
            .verify()
            .map_err(|error| integrity("object_store_upload_record_invalid", error.to_string()))?;
        Ok(Some(publication))
    }

    fn verify_upload_record_budget(&self, record: &UploadRecord) -> ResourceResult<()> {
        let mut terminal = record.clone();
        terminal.next_offset = cymule_core::MAX_EXACT_INTEGER;
        terminal.chunk_count = cymule_core::MAX_EXACT_INTEGER;
        terminal.chunk_root = Some(ChunkNodeReference {
            node_id: format!("sha256:{}", "0".repeat(64)),
            leaf_count: cymule_core::MAX_EXACT_INTEGER,
        });
        terminal.state = UploadState::Committed;
        terminal.publication = Some(UploadPublication {
            digest: format!("sha256:{}", "0".repeat(64)),
            size: cymule_core::MAX_EXACT_INTEGER,
        });
        let plan = Self::expected_cleanup_plan(&terminal)?;
        terminal.cleanup_receipt = Some(plan.receipt()?);
        terminal.cleanup_plan = Some(plan);
        let mut migrating = record.clone();
        migrating.next_offset = cymule_core::MAX_EXACT_INTEGER;
        migrating.chunk_count = cymule_core::MAX_EXACT_INTEGER;
        migrating.chunk_root.clone_from(&terminal.chunk_root);
        migrating.migration = Some(ChunkMigration {
            target_epoch: cymule_core::MAX_EXACT_INTEGER,
            migrated_offset: cymule_core::MAX_EXACT_INTEGER,
            migrated_count: cymule_core::MAX_EXACT_INTEGER,
            target_root: terminal.chunk_root.clone(),
        });
        for candidate in [&terminal, &migrating] {
            self.verify_upload_record(candidate)?;
            let encoded = cymule_core::canonical_bytes(candidate).map_err(core_error)?;
            if encoded.len() as u64 > MAX_UPLOAD_RECORD_BYTES {
                return Err(ResourceError::Validation(format!(
                    "object-store write metadata cannot fit its {MAX_UPLOAD_RECORD_BYTES}-byte terminal record budget"
                )));
            }
        }
        Ok(())
    }

    fn verify_upload_record(&self, record: &UploadRecord) -> ResourceResult<()> {
        if record.record_version != UPLOAD_RECORD_VERSION {
            return Err(integrity(
                "object_store_upload_record_invalid",
                format!(
                    "unsupported object-store upload record version {}",
                    record.record_version
                ),
            ));
        }
        record.intent.validate()?;
        if record.store_binding != self.binding
            || record.upload_id != self.upload_id(&record.intent.write_id)?
            || record.chunk_generation != self.chunk_generation(&record.upload_id)?
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object store refused an upload record outside its exact binding".to_owned(),
            ));
        }
        if record.content_epoch > cymule_core::MAX_EXACT_INTEGER
            || record.next_offset > cymule_core::MAX_EXACT_INTEGER
            || record.chunk_count > cymule_core::MAX_EXACT_INTEGER
            || (record.chunk_count == 0) != record.chunk_root.is_none()
            || (record.next_offset == 0) != (record.chunk_count == 0)
            || record
                .chunk_root
                .as_ref()
                .is_some_and(|root| root.leaf_count != record.chunk_count)
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload frontier does not match its immutable chunk root".to_owned(),
            ));
        }
        if let Some(root) = &record.chunk_root {
            Self::verify_chunk_node_reference(root)?;
        }
        Self::verify_upload_migration(record)?;
        if record.intent.shape != ResourceShape::Object
            || (matches!(
                record.state,
                UploadState::Publishing | UploadState::Committed
            ) && record.publication.is_none())
            || (matches!(record.state, UploadState::Open | UploadState::Aborted)
                && record.publication.is_some())
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload state does not match its retained frontier".to_owned(),
            ));
        }
        if let Some(publication) = self.publication_for_record(record)? {
            if publication.locators.resolver_binding != self.binding
                || publication.resource.shape != record.intent.shape
                || publication.resource.media_type != record.intent.media_type
                || publication.resource.annotations != record.intent.annotations
                || publication.resource.integrity.content_size() != Some(record.next_offset)
            {
                return Err(integrity(
                    "object_store_upload_record_invalid",
                    "object-store publication changed its admitted write intent".to_owned(),
                ));
            }
            let _ = self.resource_digest(&publication.resource, &publication.locators)?;
        }
        match (&record.cleanup_plan, &record.cleanup_receipt) {
            (None, None) => {}
            (None, Some(_)) => {
                return Err(integrity(
                    "object_store_upload_record_invalid",
                    "object-store cleanup receipt lacks its immutable plan".to_owned(),
                ));
            }
            (Some(plan), receipt) => {
                if !matches!(record.state, UploadState::Committed | UploadState::Aborted)
                    || *plan != Self::expected_cleanup_plan(record)?
                {
                    return Err(integrity(
                        "object_store_upload_record_invalid",
                        "object-store cleanup plan changed its upload authority".to_owned(),
                    ));
                }
                if let Some(receipt) = receipt {
                    receipt.verify()?;
                    if receipt.plan != *plan {
                        return Err(integrity(
                            "object_store_upload_record_invalid",
                            "object-store cleanup receipt changed its immutable plan".to_owned(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn verify_upload_migration(record: &UploadRecord) -> ResourceResult<()> {
        if let Some(migration) = &record.migration {
            if !matches!(record.state, UploadState::Open | UploadState::Publishing)
                || migration.target_epoch <= record.content_epoch
                || migration.target_epoch > cymule_core::MAX_EXACT_INTEGER
                || migration.migrated_offset > record.next_offset
                || migration.migrated_count > record.chunk_count
                || (migration.migrated_count == 0) != migration.target_root.is_none()
                || (migration.migrated_offset == 0) != (migration.migrated_count == 0)
                || migration
                    .target_root
                    .as_ref()
                    .is_some_and(|root| root.leaf_count != migration.migrated_count)
            {
                return Err(integrity(
                    "object_store_upload_record_invalid",
                    "object-store upload migration does not match its source frontier".to_owned(),
                ));
            }
            if let Some(root) = &migration.target_root {
                Self::verify_chunk_node_reference(root)?;
            }
        }
        Ok(())
    }

    fn verify_chunk_leaf(record: &UploadRecord, chunk: &ChunkLeaf) -> ResourceResult<u64> {
        if chunk.generation != record.chunk_generation
            || chunk.size == 0
            || chunk.size > MAX_WRITE_CHUNK as u64
            || chunk.offset > cymule_core::MAX_EXACT_INTEGER
            || digest_key(&chunk.digest).is_err()
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store immutable chunk leaf is malformed".to_owned(),
            ));
        }
        chunk
            .offset
            .checked_add(chunk.size)
            .filter(|end| *end <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                integrity(
                    "object_store_upload_record_invalid",
                    "object upload frontier overflow".to_owned(),
                )
            })
    }

    fn verify_chunk_node_reference(reference: &ChunkNodeReference) -> ResourceResult<()> {
        if digest_key(&reference.node_id).is_err()
            || reference.leaf_count == 0
            || reference.leaf_count > cymule_core::MAX_EXACT_INTEGER
        {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store chunk node reference is malformed".to_owned(),
            ));
        }
        Ok(())
    }

    fn expected_cleanup_plan(record: &UploadRecord) -> ResourceResult<ResourceCleanupPlan> {
        let session = ResourceWriteSession {
            write_id: record.intent.write_id.clone(),
            upload_id: record.upload_id.clone(),
            store_binding: record.store_binding.clone(),
        };
        ResourceCleanupPlan::new(&session, Vec::new())
    }

    fn abort_deleted_publication(
        &self,
        record: &mut UploadRecord,
        version: UpdateVersion,
    ) -> ResourceResult<bool> {
        if record.state != UploadState::Publishing {
            return Ok(false);
        }
        let publication = self.publication_for_record(record)?.ok_or_else(|| {
            integrity(
                "object_store_upload_record_invalid",
                "object-store Publishing upload lost its publication".to_owned(),
            )
        })?;
        let (_, expected) = Self::object_index_record(&publication.resource)?;
        let (_, head, _) = match self.load_object_head(&expected.digest) {
            Ok(value) => value,
            Err(ResourceError::NotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        let ObjectPublicationHead::Deleted { content } = head else {
            return Ok(false);
        };
        if content != expected {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload deletion fence changed its exact content".to_owned(),
            ));
        }
        let mut terminal = record.clone();
        terminal.state = UploadState::Aborted;
        terminal.publication = None;
        terminal.migration = None;
        let plan = Self::expected_cleanup_plan(&terminal)?;
        terminal.cleanup_receipt = Some(plan.receipt()?);
        terminal.cleanup_plan = Some(plan);
        if let Err(error) = self.put_record(&terminal, PutMode::Update(version)) {
            let (retained, _) = self.load_record(&record.upload_id)?;
            if retained != terminal {
                return Err(error);
            }
        }
        *record = terminal;
        Ok(true)
    }

    fn resource_digest<'a>(
        &self,
        resource: &'a ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<&'a str> {
        locators.verify_for(resource)?;
        if locators.resolver_binding != self.binding {
            return Err(ResourceError::NotFound(format!(
                "Resource {} has no object-store location for {}",
                resource.resource_id, self.binding
            )));
        }
        let expected = resource.integrity.content_digest().ok_or_else(|| {
            ResourceError::Validation(
                "object-store resources require content-addressed integrity".to_owned(),
            )
        })?;
        match locators.locations.as_slice() {
            [ResourceLocation::Opaque { reference }] if reference == expected => Ok(expected),
            [ResourceLocation::Opaque { .. }] => Err(integrity(
                "object_store_upload_record_invalid",
                "object-store locator does not match the Resource content digest".to_owned(),
            )),
            _ => Err(ResourceError::Validation(
                "object-store resources require exactly one digest locator".to_owned(),
            )),
        }
    }

    fn chunk_node_leaf_count(node: &ChunkNode) -> ResourceResult<u64> {
        match node {
            ChunkNode::Branch { depth, children } => {
                if *depth >= CHUNK_TREE_LEVELS
                    || children.is_empty()
                    || children.len() > usize::from(CHUNK_TREE_RADIX)
                {
                    return Err(integrity(
                        "object_store_upload_content_invalid",
                        "object-store chunk branch is malformed".to_owned(),
                    ));
                }
                let mut leaf_count = 0_u64;
                let mut previous_slot = None;
                for child in children {
                    if child.slot >= CHUNK_TREE_RADIX
                        || previous_slot.is_some_and(|slot| slot >= child.slot)
                    {
                        return Err(integrity(
                            "object_store_upload_content_invalid",
                            "object-store chunk branch slots are not canonical".to_owned(),
                        ));
                    }
                    Self::verify_chunk_node_reference(&child.node)?;
                    leaf_count =
                        leaf_count
                            .checked_add(child.node.leaf_count)
                            .ok_or_else(|| {
                                integrity(
                                    "object_store_upload_content_invalid",
                                    "object-store chunk branch leaf count overflow".to_owned(),
                                )
                            })?;
                    previous_slot = Some(child.slot);
                }
                if leaf_count > cymule_core::MAX_EXACT_INTEGER {
                    return Err(integrity(
                        "object_store_upload_content_invalid",
                        "object-store chunk branch exceeds the shared exact-integer range"
                            .to_owned(),
                    ));
                }
                Ok(leaf_count)
            }
            ChunkNode::Leaf {
                generation,
                offset,
                size,
                digest,
            } => {
                if digest_key(generation).is_err()
                    || *offset > cymule_core::MAX_EXACT_INTEGER
                    || *size == 0
                    || *size > MAX_WRITE_CHUNK as u64
                    || offset
                        .checked_add(*size)
                        .is_none_or(|end| end > cymule_core::MAX_EXACT_INTEGER)
                    || digest_key(digest).is_err()
                {
                    return Err(integrity(
                        "object_store_upload_content_invalid",
                        "object-store chunk leaf node is malformed".to_owned(),
                    ));
                }
                Ok(1)
            }
        }
    }

    fn put_chunk_node(&self, epoch: u64, node: &ChunkNode) -> ResourceResult<ChunkNodeReference> {
        let leaf_count = Self::chunk_node_leaf_count(node)?;
        let node_id = cymule_core::content_id(CHUNK_NODE_VERSION, node).map_err(core_error)?;
        let bytes = cymule_core::canonical_bytes(node).map_err(core_error)?;
        if bytes.len() as u64 > MAX_CHUNK_NODE_BYTES {
            return Err(ResourceError::Validation(format!(
                "object-store chunk node exceeds its {MAX_CHUNK_NODE_BYTES}-byte physical bound"
            )));
        }
        self.put_immutable_bytes(&self.chunk_node_path(epoch, &node_id)?, bytes)?;
        Ok(ChunkNodeReference {
            node_id,
            leaf_count,
        })
    }

    fn load_chunk_node(
        &self,
        epoch: u64,
        reference: &ChunkNodeReference,
    ) -> ResourceResult<ChunkNode> {
        Self::verify_chunk_node_reference(reference)?;
        let path = self.chunk_node_path(epoch, &reference.node_id)?;
        let store = Arc::clone(&self.store);
        let bytes = self.block_on(async move {
            let metadata = store.head(&path).await.map_err(retained_chunk_error)?;
            if metadata.size == 0 || metadata.size > MAX_CHUNK_NODE_BYTES {
                return Err(integrity("object_store_upload_content_invalid", format!(
                    "object-store chunk node exceeds its {MAX_CHUNK_NODE_BYTES}-byte physical bound"
                )));
            }
            let bytes = store
                .get_range(&path, 0..metadata.size)
                .await
                .map_err(retained_chunk_error)?;
            if bytes.len() as u64 != metadata.size {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk node range was truncated".to_owned(),
                ));
            }
            Ok::<_, ResourceError>(bytes)
        })??;
        let node: ChunkNode = cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        if cymule_core::canonical_bytes(&node)
            .map_err(core_error)?
            .as_slice()
            != bytes.as_ref()
            || cymule_core::content_id(CHUNK_NODE_VERSION, &node).map_err(core_error)?
                != reference.node_id
            || Self::chunk_node_leaf_count(&node)? != reference.leaf_count
        {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store chunk node locator or canonical bytes changed".to_owned(),
            ));
        }
        Ok(node)
    }

    fn chunk_tree_slot(offset: u64, depth: u8) -> u8 {
        let shift = u32::from((CHUNK_TREE_LEVELS - depth - 1) * 4);
        u8::try_from((offset >> shift) & u64::from(CHUNK_TREE_RADIX - 1))
            .expect("radix slot fits in four bits")
    }

    fn insert_chunk_leaf(
        &self,
        record: &UploadRecord,
        leaf: &ChunkLeaf,
    ) -> ResourceResult<ChunkNodeReference> {
        Self::verify_chunk_leaf(record, leaf)?;
        let mut path = Vec::with_capacity(usize::from(CHUNK_TREE_LEVELS));
        let mut current = record.chunk_root.clone();
        for depth in 0..CHUNK_TREE_LEVELS {
            let children = if let Some(reference) = current {
                match self.load_chunk_node(record.content_epoch, &reference)? {
                    ChunkNode::Branch {
                        depth: retained_depth,
                        children,
                    } if retained_depth == depth => children,
                    _ => {
                        return Err(integrity(
                            "object_store_upload_content_invalid",
                            "object-store chunk tree changed shape".to_owned(),
                        ));
                    }
                }
            } else {
                Vec::new()
            };
            let slot = Self::chunk_tree_slot(leaf.offset, depth);
            current = children
                .binary_search_by_key(&slot, |child| child.slot)
                .ok()
                .map(|index| children[index].node.clone());
            path.push((depth, children, slot));
        }
        if current.is_some() {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store chunk tree already contains this exact offset".to_owned(),
            ));
        }
        let mut child = self.put_chunk_node(
            record.content_epoch,
            &ChunkNode::Leaf {
                generation: leaf.generation.clone(),
                offset: leaf.offset,
                size: leaf.size,
                digest: leaf.digest.clone(),
            },
        )?;
        for (depth, mut children, slot) in path.into_iter().rev() {
            match children.binary_search_by_key(&slot, |entry| entry.slot) {
                Ok(index) => children[index].node = child,
                Err(index) => children.insert(index, ChunkNodeChild { slot, node: child }),
            }
            child =
                self.put_chunk_node(record.content_epoch, &ChunkNode::Branch { depth, children })?;
        }
        Ok(child)
    }

    fn find_chunk_leaf(
        &self,
        record: &UploadRecord,
        offset: u64,
    ) -> ResourceResult<Option<ChunkLeaf>> {
        let Some(mut current) = record.chunk_root.clone() else {
            return Ok(None);
        };
        for depth in 0..CHUNK_TREE_LEVELS {
            let ChunkNode::Branch {
                depth: retained_depth,
                children,
            } = self.load_chunk_node(record.content_epoch, &current)?
            else {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk tree terminated before its leaf depth".to_owned(),
                ));
            };
            if retained_depth != depth {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk tree depth changed".to_owned(),
                ));
            }
            let slot = Self::chunk_tree_slot(offset, depth);
            let Ok(index) = children.binary_search_by_key(&slot, |child| child.slot) else {
                return Ok(None);
            };
            current = children[index].node.clone();
        }
        let ChunkNode::Leaf {
            generation,
            offset: retained_offset,
            size,
            digest,
        } = self.load_chunk_node(record.content_epoch, &current)?
        else {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store chunk tree leaf changed kind".to_owned(),
            ));
        };
        let leaf = ChunkLeaf {
            generation,
            offset: retained_offset,
            size,
            digest,
        };
        Self::verify_chunk_leaf(record, &leaf)?;
        if leaf.offset != offset {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store chunk tree key changed from its leaf offset".to_owned(),
            ));
        }
        Ok(Some(leaf))
    }

    fn migrate_upload_content_page(
        &self,
        record: &mut UploadRecord,
        version: UpdateVersion,
        target_epoch: u64,
    ) -> ResourceResult<bool> {
        if !matches!(record.state, UploadState::Open | UploadState::Publishing) {
            return Ok(true);
        }
        if record.content_epoch == target_epoch {
            if record.migration.is_some() {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store current-epoch upload retained a migration".to_owned(),
                ));
            }
            return Ok(true);
        }
        if record.content_epoch > target_epoch {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store upload content epoch advanced beyond reclamation".to_owned(),
            ));
        }
        if record.migration.is_none() {
            record.migration = Some(ChunkMigration {
                target_epoch,
                migrated_offset: 0,
                migrated_count: 0,
                target_root: None,
            });
            self.put_record(record, PutMode::Update(version))?;
            return Ok(false);
        }
        let migration = record.migration.clone().ok_or_else(|| {
            integrity(
                "object_store_upload_content_invalid",
                "object-store upload migration disappeared".to_owned(),
            )
        })?;
        if migration.target_epoch != target_epoch {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store upload migration target changed".to_owned(),
            ));
        }
        let mut target = record.clone();
        target.content_epoch = target_epoch;
        target.next_offset = migration.migrated_offset;
        target.chunk_count = migration.migrated_count;
        target.chunk_root = migration.target_root;
        target.migration = None;
        let mut migrated = 0_u64;
        while target.next_offset < record.next_offset && migrated < UPLOAD_GC_CHUNK_PAGE {
            let leaf = self
                .find_chunk_leaf(record, target.next_offset)?
                .ok_or_else(|| {
                    integrity(
                        "object_store_upload_content_invalid",
                        "object-store source upload lost a migration leaf".to_owned(),
                    )
                })?;
            let bytes = self.chunk_bytes(record, &leaf)?;
            self.put_immutable_bytes(&self.chunk_data_path(target_epoch, &leaf.digest)?, bytes)?;
            let end = Self::verify_chunk_leaf(record, &leaf)?;
            target.chunk_root = Some(self.insert_chunk_leaf(&target, &leaf)?);
            target.next_offset = end;
            target.chunk_count = target.chunk_count.checked_add(1).ok_or_else(|| {
                integrity(
                    "object_store_upload_content_invalid",
                    "object-store migrated chunk count overflow".to_owned(),
                )
            })?;
            migrated += 1;
        }
        if target.next_offset == record.next_offset {
            if target.chunk_count != record.chunk_count {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store migrated chunk count changed".to_owned(),
                ));
            }
            record.content_epoch = target_epoch;
            record.chunk_root = target.chunk_root;
            record.migration = None;
        } else {
            record.migration = Some(ChunkMigration {
                target_epoch,
                migrated_offset: target.next_offset,
                migrated_count: target.chunk_count,
                target_root: target.chunk_root,
            });
        }
        self.put_record(record, PutMode::Update(version))?;
        Ok(record.migration.is_none())
    }

    fn chunk_bytes(&self, record: &UploadRecord, chunk: &ChunkLeaf) -> ResourceResult<Vec<u8>> {
        Self::verify_chunk_leaf(record, chunk)?;
        let path = self.chunk_data_path(record.content_epoch, &chunk.digest)?;
        let store = Arc::clone(&self.store);
        let expected_size = chunk.size;
        let bytes = self.block_on(async move {
            let metadata = store.head(&path).await.map_err(retained_chunk_error)?;
            if metadata.size != expected_size {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk size changed".to_owned(),
                ));
            }
            let bytes = store
                .get_range(&path, 0..expected_size)
                .await
                .map_err(retained_chunk_error)?;
            if bytes.len() as u64 != expected_size {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk range was truncated".to_owned(),
                ));
            }
            Ok::<_, ResourceError>(bytes)
        })??;
        if format!("sha256:{}", hex_digest(&Sha256::digest(&bytes))) != chunk.digest {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store chunk digest changed".to_owned(),
            ));
        }
        Ok(bytes.to_vec())
    }

    fn verify_acknowledged_chunk_retry(
        &self,
        record: &UploadRecord,
        offset: u64,
        end: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        if end > record.next_offset {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store chunk retry exceeds its acknowledged frontier".to_owned(),
            ));
        }
        let Some(chunk) = self.find_chunk_leaf(record, offset)? else {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store chunk retry does not begin at an acknowledged offset".to_owned(),
            ));
        };
        if chunk.size != bytes.len() as u64
            || chunk.digest != format!("sha256:{}", hex_digest(&Sha256::digest(bytes)))
            || self.chunk_bytes(record, &chunk)? != bytes
        {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store chunk retry changed retained bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_committed_chunk_retry(
        &mut self,
        record: &UploadRecord,
        offset: u64,
        end: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        let publication = self.publication_for_record(record)?.ok_or_else(|| {
            integrity(
                "object_store_upload_record_invalid",
                "committed object-store upload has no publication".to_owned(),
            )
        })?;
        if end > record.next_offset {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store write retry exceeds committed bytes".to_owned(),
            ));
        }
        let max_bytes = u32::try_from(bytes.len()).map_err(|_| {
            ResourceError::Validation("object-store chunk exceeds read bounds".to_owned())
        })?;
        // Committed content no longer depends on the upload epoch or radix root.
        // The original commit performs full digest verification after replay.
        let retained = self.read(
            &publication.resource,
            &publication.locators,
            offset,
            max_bytes,
        )?;
        if retained.bytes != bytes {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store write retry changed committed bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn visit_acknowledged_chunks(
        &self,
        record: &UploadRecord,
        mut visit: impl FnMut(&ChunkLeaf, &[u8]) -> ResourceResult<()>,
    ) -> ResourceResult<()> {
        let mut offset = 0_u64;
        let mut observed_count = 0_u64;
        while offset < record.next_offset {
            let chunk = self.find_chunk_leaf(record, offset)?.ok_or_else(|| {
                integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk tree lost an acknowledged frontier leaf".to_owned(),
                )
            })?;
            let end = Self::verify_chunk_leaf(record, &chunk)?;
            if end > record.next_offset {
                return Err(integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk log crosses its acknowledged frontier".to_owned(),
                ));
            }
            let bytes = self.chunk_bytes(record, &chunk)?;
            visit(&chunk, &bytes)?;
            offset = end;
            observed_count = observed_count.checked_add(1).ok_or_else(|| {
                integrity(
                    "object_store_upload_content_invalid",
                    "object-store chunk count overflow".to_owned(),
                )
            })?;
        }
        if offset != record.next_offset || observed_count != record.chunk_count {
            return Err(integrity(
                "object_store_upload_content_invalid",
                "object-store chunk tree does not close its acknowledged frontier".to_owned(),
            ));
        }
        Ok(())
    }

    fn object_index_record(
        resource: &ResourceHandle,
    ) -> ResourceResult<(ResourceCatalogRecord, ObjectIndexPayload)> {
        resource.verify()?;
        if resource.shape != ResourceShape::Object {
            return Err(ResourceError::Validation(
                "object-store content indexes require object Resources".to_owned(),
            ));
        }
        let ResourceIntegrity::Content { digest, size } = &resource.integrity else {
            return Err(ResourceError::Validation(
                "object-store content indexes require content integrity".to_owned(),
            ));
        };
        let payload = ObjectIndexPayload {
            digest: digest.clone(),
            size: *size,
            part_size: PART_SIZE as u64,
            part_count: object_part_count(*size),
        };
        let record = Self::object_head_record(&ObjectPublicationHead::Published {
            content: payload.clone(),
        })?;
        Ok((record, payload))
    }

    fn object_head_record(head: &ObjectPublicationHead) -> ResourceResult<ResourceCatalogRecord> {
        ResourceCatalogRecord::new(
            OBJECT_INDEX_NAMESPACE,
            head.content().digest.clone(),
            cymule_core::canonical_bytes(head).map_err(core_error)?,
        )
    }

    fn load_object_index(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ObjectIndexPayload> {
        let digest = self.resource_digest(resource, locators)?;
        let retained = self.load_retained_object_index(digest)?;
        let (_, expected) = Self::object_index_record(resource)?;
        if retained != expected {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content index changed from the semantic Resource".to_owned(),
            ));
        }
        Ok(retained)
    }

    fn load_retained_object_index(&self, digest: &str) -> ResourceResult<ObjectIndexPayload> {
        let (_, head, _) = self.load_object_head(digest)?;
        match head {
            ObjectPublicationHead::Published { content } => Ok(content),
            ObjectPublicationHead::Deleted { .. } => Err(ResourceError::NotFound(format!(
                "object-store publication {digest} is permanently deleted"
            ))),
        }
    }

    fn load_object_head(
        &self,
        digest: &str,
    ) -> ResourceResult<(ResourceCatalogRecord, ObjectPublicationHead, UpdateVersion)> {
        let path = self.object_index_path(digest)?;
        let store = Arc::clone(&self.store);
        let (bytes, version) = self.block_on(async move {
            let result = store.get(&path).await.map_err(object_error)?;
            if result.meta.size == 0 || result.meta.size > MAX_OBJECT_INDEX_BYTES {
                return Err(integrity(
                    "object_store_object_invalid",
                    "object-store content index has an invalid encoded size".to_owned(),
                ));
            }
            let expected_size = result.meta.size;
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let bytes = result.bytes().await.map_err(object_error)?;
            if bytes.len() as u64 != expected_size {
                return Err(integrity(
                    "object_store_object_invalid",
                    "object-store content index body was truncated".to_owned(),
                ));
            }
            Ok::<_, ResourceError>((bytes, version))
        })??;
        let record: ResourceCatalogRecord =
            cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        record.verify()?;
        if record.namespace != OBJECT_INDEX_NAMESPACE
            || record.key != digest
            || cymule_core::canonical_bytes(&record)
                .map_err(core_error)?
                .as_slice()
                != bytes.as_ref()
        {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content index identity changed".to_owned(),
            ));
        }
        let head: ObjectPublicationHead =
            cymule_core::decode_json(&record.payload).map_err(core_integrity)?;
        let payload = head.content();
        if cymule_core::canonical_bytes(&head)
            .map_err(core_error)?
            .as_slice()
            != record.payload.as_slice()
            || payload.digest != digest
            || payload.size > cymule_core::MAX_EXACT_INTEGER
            || payload.part_size != PART_SIZE as u64
            || payload.part_count != object_part_count(payload.size)
        {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content index payload changed".to_owned(),
            ));
        }
        let expected = Self::object_head_record(&head)?;
        if record != expected {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content index record changed".to_owned(),
            ));
        }
        Ok((record, head, version))
    }

    fn publish_object_index(&self, index_record: &ResourceCatalogRecord) -> ResourceResult<()> {
        let path = self.object_index_path(&index_record.key)?;
        let bytes = cymule_core::canonical_bytes(index_record).map_err(core_error)?;
        let store = Arc::clone(&self.store);
        let result = self.block_on(async move {
            store
                .put_opts(
                    &path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        })?;
        match result {
            Ok(_) | Err(ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. }) => {}
            Err(error) => return Err(object_error(error)),
        }
        let (retained, head, _) = self.load_object_head(&index_record.key)?;
        if matches!(head, ObjectPublicationHead::Deleted { .. }) {
            return Err(conflict(
                "object_store_publication_deleted",
                "object-store publication is permanently fenced against writes".to_owned(),
            ));
        }
        if retained != *index_record {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store publication index retained different content".to_owned(),
            ));
        }
        Ok(())
    }

    fn fence_object_deletion(
        &self,
        index: &ObjectIndexPayload,
        source: Option<(ResourceCatalogRecord, ObjectPublicationHead, UpdateVersion)>,
    ) -> ResourceResult<()> {
        let tombstone = ObjectPublicationHead::Deleted {
            content: index.clone(),
        };
        let mode = match source {
            Some((_, head, version)) => {
                if head.content() != index {
                    return Err(integrity(
                        "object_store_cleanup_invalid",
                        "object-store deletion source changed its exact target".to_owned(),
                    ));
                }
                if head == tombstone {
                    return Ok(());
                }
                PutMode::Update(version)
            }
            None => PutMode::Create,
        };
        let bytes = cymule_core::canonical_bytes(&Self::object_head_record(&tombstone)?)
            .map_err(core_error)?;
        let path = self.object_index_path(&index.digest)?;
        let store = Arc::clone(&self.store);
        let result = self.block_on(async move {
            store
                .put_opts(
                    &path,
                    bytes.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
        })?;
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                // A lost acknowledgement or a concurrent identical deletion
                // resolves only through the irreversible exact same-key fence.
                let (_, retained, _) = self.load_object_head(&index.digest)?;
                if retained == tombstone {
                    Ok(())
                } else {
                    Err(object_error(error))
                }
            }
        }
    }

    fn inspect_object_family(
        &self,
        index: &ObjectIndexPayload,
    ) -> ResourceResult<ObjectFamilyInventory> {
        let prefix = self.object_family_prefix(&index.digest)?;
        let store = Arc::clone(&self.store);
        self.block_on(async move {
            let mut stream = store.ordered_inventory(Some(&prefix), None);
            let mut previous = None;
            let mut inventory = ObjectFamilyInventory {
                index_present: false,
                part_count: 0,
            };
            while let Some(result) = stream.next().await {
                let metadata = result.map_err(object_error)?;
                if previous
                    .as_ref()
                    .is_some_and(|previous| metadata.location <= *previous)
                {
                    return Err(integrity(
                        "object_store_inventory_contract_violation",
                        "object-store content family inventory lost strict ordering".to_owned(),
                    ));
                }
                match validate_object_family_entry(&prefix, index, &metadata)? {
                    ObjectFamilyEntry::Index => inventory.index_present = true,
                    ObjectFamilyEntry::Part => {
                        inventory.part_count =
                            inventory.part_count.checked_add(1).ok_or_else(|| {
                                integrity(
                                    "object_store_object_invalid",
                                    "object-store content family part count overflow".to_owned(),
                                )
                            })?;
                    }
                }
                previous = Some(metadata.location);
            }
            Ok(inventory)
        })?
    }

    fn verify_object_family_closure(&self, index: &ObjectIndexPayload) -> ResourceResult<()> {
        let inventory = self.inspect_object_family(index)?;
        if !inventory.index_present || inventory.part_count != index.part_count {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content family is missing its exact index or part closure".to_owned(),
            ));
        }
        Ok(())
    }

    fn delete_object_family(&self, index: &ObjectIndexPayload) -> ResourceResult<()> {
        let prefix = self.object_family_prefix(&index.digest)?;
        let store = Arc::clone(&self.store);
        self.block_on(async move {
            let mut stream = store.ordered_inventory(Some(&prefix), None);
            let mut previous = None;
            while let Some(result) = stream.next().await {
                let metadata = result.map_err(object_error)?;
                if previous
                    .as_ref()
                    .is_some_and(|previous| metadata.location <= *previous)
                {
                    return Err(integrity(
                        "object_store_inventory_contract_violation",
                        "object-store deletion inventory lost strict ordering".to_owned(),
                    ));
                }
                let entry = validate_object_family_entry(&prefix, index, &metadata)
                    .map_err(deletion_integrity_error)?;
                previous = Some(metadata.location.clone());
                if entry == ObjectFamilyEntry::Index {
                    continue;
                }
                match store.delete(&metadata.location).await {
                    Ok(()) | Err(ObjectError::NotFound { .. }) => {}
                    Err(error) => return Err(object_error(error)),
                }
                match store.head(&metadata.location).await {
                    Err(ObjectError::NotFound { .. }) => {}
                    Ok(_) => {
                        return Err(integrity(
                            "object_store_cleanup_invalid",
                            "object-store deleted part remains present".to_owned(),
                        ));
                    }
                    Err(error) => return Err(object_error(error)),
                }
            }
            Ok::<_, ResourceError>(())
        })??;
        // The exact index key is a permanent deletion fence, never a payload
        // deletion target. Removing it would admit a delayed Create publisher.
        Ok(())
    }

    fn put_immutable_bytes(&self, path: &Path, bytes: Vec<u8>) -> ResourceResult<()> {
        let store = Arc::clone(&self.store);
        let destination = path.clone();
        let expected = bytes.clone();
        let result = self.block_on(async move {
            store
                .put_opts(
                    &destination,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        })?;
        match result {
            Ok(_) | Err(ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. }) => {}
            Err(error) => return Err(object_error(error)),
        }
        let store = Arc::clone(&self.store);
        let retained_path = path.clone();
        let expected_size = expected.len() as u64;
        let retained = self.block_on(async move {
            let metadata = store.head(&retained_path).await.map_err(object_error)?;
            if metadata.size != expected_size {
                return Err(integrity(
                    "object_store_object_invalid",
                    "object-store immutable content size changed".to_owned(),
                ));
            }
            store
                .get_range(&retained_path, 0..expected_size)
                .await
                .map_err(object_error)
        })??;
        if retained.as_ref() != expected.as_slice() {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store immutable content key retained different bytes".to_owned(),
            ));
        }
        Ok(())
    }

    fn object_part_bytes(
        &self,
        digest: &str,
        part_index: u64,
        expected_size: u64,
    ) -> ResourceResult<Vec<u8>> {
        self.object_part_range(digest, part_index, expected_size, 0, expected_size)
    }

    fn object_part_range(
        &self,
        digest: &str,
        part_index: u64,
        expected_size: u64,
        start: u64,
        end: u64,
    ) -> ResourceResult<Vec<u8>> {
        if expected_size == 0 || expected_size > PART_SIZE as u64 {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content part has an invalid retained size".to_owned(),
            ));
        }
        if start >= end || end > expected_size {
            return Err(ResourceError::Validation(
                "object-store content part range is invalid".to_owned(),
            ));
        }
        let path = self.object_part_path(digest, part_index)?;
        let store = Arc::clone(&self.store);
        let observed = self.block_on(async move {
            let metadata = store.head(&path).await.map_err(object_error)?;
            if metadata.size != expected_size {
                return Err(integrity(
                    "object_store_object_invalid",
                    "published object part size does not match its index".to_owned(),
                ));
            }
            let bytes = store
                .get_range(&path, start..end)
                .await
                .map_err(object_error)?;
            if bytes.len() as u64 != end - start {
                return Err(integrity(
                    "object_store_object_invalid",
                    "published object part was truncated".to_owned(),
                ));
            }
            Ok::<_, ResourceError>(bytes.to_vec())
        })??;
        Ok(observed)
    }

    fn verify_object(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<()> {
        let index = self.load_object_index(resource, locators)?;
        self.verify_object_bytes(&index)
    }

    fn verify_object_bytes(&self, index: &ObjectIndexPayload) -> ResourceResult<()> {
        let mut hasher = Sha256::new();
        let mut observed_size = 0_u64;
        for part_index in 0..index.part_count {
            let expected_size = object_part_size(index.size, part_index)?;
            let bytes = self.object_part_bytes(&index.digest, part_index, expected_size)?;
            hasher.update(&bytes);
            observed_size = observed_size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| {
                    integrity(
                        "object_store_object_invalid",
                        "object size overflow".to_owned(),
                    )
                })?;
        }
        let observed_digest = format!("sha256:{}", hex_digest(&hasher.finalize()));
        if observed_digest != index.digest || observed_size != index.size {
            return Err(integrity(
                "object_store_object_invalid",
                "published object bytes do not match their digest".to_owned(),
            ));
        }
        Ok(())
    }

    fn publish_object_parts(
        &self,
        record: &UploadRecord,
        publication: &ResourcePublication,
    ) -> ResourceResult<()> {
        let retained_publication = self.publication_for_record(record)?;
        if record.state != UploadState::Publishing
            || retained_publication.as_ref() != Some(publication)
        {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store publication lost its exact acknowledged inventory".to_owned(),
            ));
        }
        let (index_record, index) = Self::object_index_record(&publication.resource)?;
        let mut hasher = Sha256::new();
        let mut observed_size = 0_u64;
        let mut part_index = 0_u64;
        let mut part = Vec::with_capacity(PART_SIZE);
        self.visit_acknowledged_chunks(record, |_, bytes| {
            hasher.update(bytes);
            observed_size = observed_size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| {
                    integrity(
                        "object_store_object_invalid",
                        "object size overflow".to_owned(),
                    )
                })?;
            let mut cursor = 0_usize;
            while cursor < bytes.len() {
                let count = (PART_SIZE - part.len()).min(bytes.len() - cursor);
                part.extend_from_slice(&bytes[cursor..cursor + count]);
                cursor += count;
                if part.len() == PART_SIZE {
                    let path = self.object_part_path(&index.digest, part_index)?;
                    self.put_immutable_bytes(&path, std::mem::take(&mut part))?;
                    #[cfg(test)]
                    if self.test_faults.content_part.swap(false, Ordering::SeqCst) {
                        return Err(substrate_with_code(
                            "object_store_content_publication_interrupted",
                            "injected publish interruption after content part publication"
                                .to_owned(),
                        ));
                    }
                    part = Vec::with_capacity(PART_SIZE);
                    part_index = part_index.checked_add(1).ok_or_else(|| {
                        integrity(
                            "object_store_object_invalid",
                            "object part count overflow".to_owned(),
                        )
                    })?;
                }
            }
            Ok(())
        })?;
        if !part.is_empty() {
            let path = self.object_part_path(&index.digest, part_index)?;
            self.put_immutable_bytes(&path, part)?;
            part_index = part_index.checked_add(1).ok_or_else(|| {
                integrity(
                    "object_store_object_invalid",
                    "object part count overflow".to_owned(),
                )
            })?;
        }
        let observed_digest = format!("sha256:{}", hex_digest(&hasher.finalize()));
        if observed_digest != index.digest
            || observed_size != index.size
            || part_index != index.part_count
        {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store retained chunks changed after Publishing admission".to_owned(),
            ));
        }
        self.publish_object_index(&index_record)
    }

    fn complete_upload_publication(
        &self,
        record: &mut UploadRecord,
        version: UpdateVersion,
        publication: &ResourcePublication,
    ) -> ResourceResult<UpdateVersion> {
        self.publish_object_parts(record, publication)?;
        self.verify_object(&publication.resource, &publication.locators)?;
        record.state = UploadState::Committed;
        self.put_record(record, PutMode::Update(version))
    }

    fn delete_if_present(&self, path: &Path) -> ResourceResult<()> {
        if self.is_absent(path)? {
            return Ok(());
        }
        let store = Arc::clone(&self.store);
        let path = path.clone();
        match self.block_on(async move { store.delete(&path).await })? {
            Ok(()) | Err(ObjectError::NotFound { .. }) => Ok(()),
            Err(error) => Err(object_error(error)),
        }
    }

    fn require_inventory_object(
        &self,
        path: &Path,
        source: &UploadGcRecord,
        source_version: &UpdateVersion,
    ) -> ResourceResult<()> {
        let store = Arc::clone(&self.store);
        let path = path.clone();
        match self.block_on(async move { store.head(&path).await })? {
            Ok(_) => Ok(()),
            Err(ObjectError::NotFound { .. }) => {
                self.verify_upload_gc_source(source, source_version)?;
                Err(integrity(
                    "object_store_gc_authority_invalid",
                    "object-store strong inventory listed an absent object".to_owned(),
                ))
            }
            Err(error) => Err(object_error(error)),
        }
    }

    fn verify_upload_gc_source(
        &self,
        source: &UploadGcRecord,
        source_version: &UpdateVersion,
    ) -> ResourceResult<()> {
        let (current, current_version) = self.load_upload_gc_record()?;
        if current != *source || current_version != *source_version {
            return Err(conflict(
                "object_store_precondition_failed",
                "object-store GC head advanced before inventory validation".to_owned(),
            ));
        }
        Ok(())
    }

    fn is_absent(&self, path: &Path) -> ResourceResult<bool> {
        let store = Arc::clone(&self.store);
        let path = path.clone();
        match self.block_on(async move { store.head(&path).await })? {
            Err(ObjectError::NotFound { .. }) => Ok(true),
            Ok(_) => Ok(false),
            Err(error) => Err(object_error(error)),
        }
    }

    fn cleanup_upload(
        &self,
        session: &ResourceWriteSession,
        record: &mut UploadRecord,
        mut version: UpdateVersion,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        if let Some(receipt) = &record.cleanup_receipt {
            receipt.verify()?;
            return Ok(receipt.clone());
        }
        if record.cleanup_plan.is_none() {
            record.cleanup_plan = Some(Self::expected_cleanup_plan(record)?);
            version = self.put_record(record, PutMode::Update(version))?;
            #[cfg(test)]
            if self.test_faults.cleanup_plan.swap(false, Ordering::SeqCst) {
                return Err(substrate_with_code(
                    "object_store_cleanup_plan_receipt_lost",
                    "injected cleanup interruption after empty plan publication".to_owned(),
                ));
            }
        }
        let plan = record.cleanup_plan.clone().ok_or_else(|| {
            integrity(
                "object_store_cleanup_invalid",
                "object-store cleanup plan was not persisted".to_owned(),
            )
        })?;
        if plan != Self::expected_cleanup_plan(record)?
            || plan.write_id != session.write_id
            || plan.upload_id != session.upload_id
            || plan.store_binding != session.store_binding
        {
            return Err(integrity(
                "object_store_cleanup_invalid",
                "object-store cleanup plan changed its upload authority".to_owned(),
            ));
        }
        if !plan.targets.is_empty() {
            return Err(integrity(
                "object_store_cleanup_invalid",
                "object-store cleanup plan claimed mutable session staging".to_owned(),
            ));
        }
        let receipt = plan.receipt()?;
        receipt.verify()?;
        record.cleanup_receipt = Some(receipt.clone());
        self.put_record(record, PutMode::Update(version))?;
        Ok(receipt)
    }
}

impl ResourceCatalogStore for ObjectResourceStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        record.verify()?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        let path = self.catalog_path(&record.namespace, &record.key)?;
        let store = Arc::clone(&self.store);
        let result = self.block_on(async move {
            store
                .put_opts(
                    &path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        })?;
        match result {
            Ok(_) => Ok(()),
            Err(ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. }) => {
                let existing = self
                    .get_catalog_record(&record.namespace, &record.key)?
                    .ok_or_else(|| {
                        conflict(
                            "object_store_catalog_conflict",
                            "object-store catalog create conflicted without retained content"
                                .to_owned(),
                        )
                    })?;
                if existing == *record {
                    Ok(())
                } else {
                    Err(conflict(
                        "object_store_catalog_conflict",
                        format!(
                            "object-store catalog record {}/{} has conflicting content",
                            record.namespace, record.key
                        ),
                    ))
                }
            }
            Err(error) => Err(object_error(error)),
        }
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        let _ = ResourceCatalogRecord::new(namespace, key, Vec::new())?;
        let path = self.catalog_path(namespace, key)?;
        let store = Arc::clone(&self.store);
        let result = self.block_on(async move { store.get(&path).await })?;
        let result = match result {
            Ok(result) => result,
            Err(ObjectError::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(object_error(error)),
        };
        let expected_size = result.meta.size;
        if expected_size > MAX_RESOURCE_CATALOG_RECORD_BYTES {
            return Err(integrity(
                "object_store_catalog_invalid",
                format!(
                    "object-store catalog record exceeds {MAX_RESOURCE_CATALOG_RECORD_BYTES} encoded bytes"
                ),
            ));
        }
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        if u64::try_from(bytes.len()).map_err(|_| {
            integrity(
                "object_store_catalog_invalid",
                "object-store catalog record exceeds platform bounds".to_owned(),
            )
        })? != expected_size
        {
            return Err(integrity(
                "object_store_catalog_invalid",
                "object-store catalog record changed size while reading".to_owned(),
            ));
        }
        let record: ResourceCatalogRecord =
            cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        record.verify()?;
        if record.namespace != namespace || record.key != key {
            return Err(integrity(
                "object_store_catalog_invalid",
                "object-store catalog locator does not match its record identity".to_owned(),
            ));
        }
        Ok(Some(record))
    }
}

impl Drop for ObjectResourceStore {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl ArtifactStore for ObjectResourceStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> ResourceResult<ResourceWriteSession> {
        intent.validate()?;
        if intent.shape != ResourceShape::Object {
            return Err(ResourceError::Validation(
                "initial object-store profile accepts object Resources only".to_owned(),
            ));
        }
        let upload_id = self.upload_id(&intent.write_id)?;
        let chunk_generation = self.chunk_generation(&upload_id)?;
        let content_epoch = self.load_upload_gc_record()?.0.current_epoch;
        let record = UploadRecord {
            record_version: UPLOAD_RECORD_VERSION.to_owned(),
            intent: intent.clone(),
            upload_id: upload_id.clone(),
            store_binding: self.binding.clone(),
            chunk_generation,
            content_epoch,
            next_offset: 0,
            chunk_count: 0,
            chunk_root: None,
            migration: None,
            state: UploadState::Open,
            publication: None,
            cleanup_plan: None,
            cleanup_receipt: None,
        };
        self.verify_upload_record_budget(&record)?;
        #[cfg(test)]
        TestFaults::pause(&self.test_faults.begin_upload_pause);
        let (mut retained, retained_version) = match self.put_record(&record, PutMode::Create) {
            Ok(version) => (record, version),
            Err(ResourceError::Conflict { ref code, .. }) if is_create_conflict(code) => {
                let (existing, version) = self.load_record(&upload_id)?;
                if existing.intent != *intent || existing.state == UploadState::Aborted {
                    return Err(conflict(
                        "object_store_upload_conflict",
                        format!("object-store write ID {} was reused", intent.write_id),
                    ));
                }
                (existing, version)
            }
            Err(error) => return Err(error),
        };
        let current_epoch = self.load_upload_gc_record()?.0.current_epoch;
        if retained.state == UploadState::Open && retained.content_epoch != current_epoch {
            if retained.next_offset != 0
                || retained.chunk_count != 0
                || retained.chunk_root.is_some()
                || retained.migration.is_some()
            {
                return Err(conflict(
                    "object_store_upload_conflict",
                    "object-store upload awaits content-epoch reconciliation".to_owned(),
                ));
            }
            retained.content_epoch = current_epoch;
            match self.put_record(&retained, PutMode::Update(retained_version)) {
                Ok(_) => {}
                Err(ResourceError::Conflict { ref code, .. })
                    if code == "object_store_precondition_failed" =>
                {
                    let (reopened, _) = self.load_record(&upload_id)?;
                    let reopened_epoch = self.load_upload_gc_record()?.0.current_epoch;
                    if reopened.state != UploadState::Open
                        || reopened.content_epoch != reopened_epoch
                        || reopened.migration.is_some()
                    {
                        return Err(conflict(
                            "object_store_upload_conflict",
                            "object-store upload epoch changed during admission".to_owned(),
                        ));
                    }
                    retained = reopened;
                }
                Err(error) => return Err(error),
            }
        }
        if retained.state == UploadState::Open
            && retained.content_epoch != self.load_upload_gc_record()?.0.current_epoch
        {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload epoch changed during admission".to_owned(),
            ));
        }
        Ok(ResourceWriteSession {
            write_id: intent.write_id.clone(),
            upload_id,
            store_binding: self.binding.clone(),
        })
    }

    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        self.validate_session(session)?;
        if bytes.is_empty() || bytes.len() > MAX_WRITE_CHUNK {
            return Err(ResourceError::Validation(
                "object-store session or chunk is invalid".to_owned(),
            ));
        }
        let size = u64::try_from(bytes.len()).map_err(|_| {
            ResourceError::Validation("object-store chunk exceeds platform bounds".to_owned())
        })?;
        let end = offset
            .checked_add(size)
            .filter(|end| *end <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation(
                    "object-store chunk exceeds the shared exact-integer range".to_owned(),
                )
            })?;
        let (mut record, version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload identity changed".to_owned(),
            ));
        }
        match record.state {
            UploadState::Committed => {
                return self.verify_committed_chunk_retry(&record, offset, end, bytes);
            }
            UploadState::Publishing => {
                return self.verify_acknowledged_chunk_retry(&record, offset, end, bytes);
            }
            UploadState::Aborted => {
                return Err(conflict(
                    "object_store_upload_conflict",
                    "object-store upload was aborted".to_owned(),
                ));
            }
            UploadState::Open => {}
        }
        let current_epoch = self.load_upload_gc_record()?.0.current_epoch;
        if record.migration.is_some() || record.content_epoch != current_epoch {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload is not open in the current content epoch".to_owned(),
            ));
        }
        if offset < record.next_offset {
            return self.verify_acknowledged_chunk_retry(&record, offset, end, bytes);
        }
        if offset != record.next_offset {
            return Err(conflict(
                "object_store_upload_conflict",
                format!(
                    "object-store upload expected offset {}, received {offset}",
                    record.next_offset
                ),
            ));
        }
        let planned = ChunkLeaf {
            generation: record.chunk_generation.clone(),
            offset,
            size,
            digest: format!("sha256:{}", hex_digest(&Sha256::digest(bytes))),
        };
        self.put_immutable_bytes(
            &self.chunk_data_path(record.content_epoch, &planned.digest)?,
            bytes.to_vec(),
        )?;
        let new_root = self.insert_chunk_leaf(&record, &planned)?;
        #[cfg(test)]
        if self
            .test_faults
            .chunk_candidate
            .swap(false, Ordering::SeqCst)
        {
            return Err(substrate_with_code(
                "object_store_chunk_candidate_interrupted",
                "injected interruption after immutable chunk candidate publication".to_owned(),
            ));
        }
        #[cfg(test)]
        TestFaults::pause(&self.test_faults.chunk_candidate_pause);
        record.next_offset = end;
        record.chunk_count = record.chunk_count.checked_add(1).ok_or_else(|| {
            ResourceError::Validation("object-store chunk count exceeds platform bounds".to_owned())
        })?;
        record.chunk_root = Some(new_root);
        self.commit_chunk_acknowledgement(&record, version, &planned, bytes)
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.validate_session(session)?;
        let (mut record, mut version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store commit identity changed".to_owned(),
            ));
        }
        if self.abort_deleted_publication(&mut record, version.clone())? {
            return Err(conflict(
                "object_store_publication_deleted",
                "object-store publication is permanently fenced against writes".to_owned(),
            ));
        }
        if record.migration.is_some() {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload content migration is incomplete".to_owned(),
            ));
        }
        if let Some(publication) = self.publication_for_record(&record)? {
            if record.state == UploadState::Publishing {
                version = self.complete_upload_publication(&mut record, version, &publication)?;
            } else if record.state != UploadState::Committed {
                return Err(integrity(
                    "object_store_upload_record_invalid",
                    "object-store publication exists outside publishing state".to_owned(),
                ));
            } else {
                self.verify_object(&publication.resource, &publication.locators)?;
            }
            self.cleanup_upload(session, &mut record, version)?;
            return Ok(publication);
        }
        if record.state != UploadState::Open {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store upload cannot commit from its current state".to_owned(),
            ));
        }
        let mut hasher = Sha256::new();
        let mut observed_size = 0_u64;
        self.visit_acknowledged_chunks(&record, |_, bytes| {
            hasher.update(bytes);
            observed_size = observed_size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| {
                    integrity(
                        "object_store_upload_record_invalid",
                        "object size overflow".to_owned(),
                    )
                })?;
            Ok(())
        })?;
        let hex = hex_digest(&hasher.finalize());
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{hex}"),
                size: observed_size,
            },
            manifest: None,
            annotations: record.intent.annotations.clone(),
        }
        .seal()?;
        let publication = ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: self.binding.clone(),
                locations: vec![ResourceLocation::Opaque {
                    reference: format!("sha256:{hex}"),
                }],
            },
            resource,
        };
        publication.verify()?;
        let ResourceIntegrity::Content { digest, size } = &publication.resource.integrity else {
            return Err(integrity(
                "object_store_upload_record_invalid",
                "object-store upload publication is not content addressed".to_owned(),
            ));
        };
        record.state = UploadState::Publishing;
        record.publication = Some(UploadPublication {
            digest: digest.clone(),
            size: *size,
        });
        version = self.put_record(&record, PutMode::Update(version))?;
        version = self.complete_upload_publication(&mut record, version, &publication)?;
        self.cleanup_upload(session, &mut record, version)?;
        Ok(publication)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.validate_session(session)?;
        let (mut record, version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store abort identity changed".to_owned(),
            ));
        }
        if self.abort_deleted_publication(&mut record, version.clone())? {
            return record.cleanup_receipt.ok_or_else(|| {
                integrity(
                    "object_store_cleanup_invalid",
                    "object-store retired upload lost its terminal cleanup receipt".to_owned(),
                )
            });
        }
        if matches!(
            record.state,
            UploadState::Publishing | UploadState::Committed
        ) {
            if record.state == UploadState::Publishing {
                let _ = self.commit_write(session)?;
                let reopened = self.load_record(&session.upload_id)?;
                record = reopened.0;
                return self.cleanup_upload(session, &mut record, reopened.1);
            }
            return self.cleanup_upload(session, &mut record, version);
        }
        record.migration = None;
        record.state = UploadState::Aborted;
        let version = self.put_record(&record, PutMode::Update(version))?;
        self.cleanup_upload(session, &mut record, version)
    }

    fn cleanup_receipt(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<Option<ResourceCleanupReceipt>> {
        self.validate_session(session)?;
        let record = self.load_record(&session.upload_id)?.0;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "object_store_upload_conflict",
                "object-store cleanup receipt identity changed".to_owned(),
            ));
        }
        Ok(record.cleanup_receipt)
    }
}

impl ArtifactResolver for ObjectResourceStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        self.verify_object(resource, locators)?;
        Ok(ResourceObservation {
            media_type: resource.media_type.clone(),
            integrity: resource.integrity.clone(),
        })
    }

    fn read(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk> {
        resource.verify()?;
        let ResourceIntegrity::Content { size, .. } = &resource.integrity else {
            return Err(ResourceError::Validation(
                "object-store reads require content integrity".to_owned(),
            ));
        };
        let size = *size;
        if max_bytes == 0 || max_bytes > MAX_READ_CHUNK || offset > size {
            return Err(ResourceError::Validation(format!(
                "object-store read range requires 1..={MAX_READ_CHUNK} bytes"
            )));
        }
        let index = self.load_object_index(resource, locators)?;
        if offset == size {
            self.verify_object_family_closure(&index)?;
            return Ok(ResourceChunk {
                offset,
                bytes: Vec::new(),
                eof: true,
            });
        }
        let requested_end = offset.checked_add(u64::from(max_bytes)).ok_or_else(|| {
            ResourceError::Validation("object-store read range overflow".to_owned())
        })?;
        let end = requested_end.min(size);
        let mut cursor = offset;
        let mut bytes = Vec::with_capacity(
            usize::try_from(end - offset)
                .map_err(|error| integrity("object_store_object_invalid", error.to_string()))?,
        );
        while cursor < end {
            let part_index = cursor / PART_SIZE as u64;
            let part_start = part_index.checked_mul(PART_SIZE as u64).ok_or_else(|| {
                integrity(
                    "object_store_object_invalid",
                    "object part offset overflow".to_owned(),
                )
            })?;
            let part_size = object_part_size(index.size, part_index)?;
            let start_in_part = cursor - part_start;
            let requested_part_end = start_in_part.checked_add(end - cursor).ok_or_else(|| {
                integrity(
                    "object_store_object_invalid",
                    "object part range overflow".to_owned(),
                )
            })?;
            let end_in_part = part_size.min(requested_part_end);
            let range = self.object_part_range(
                &index.digest,
                part_index,
                part_size,
                start_in_part,
                end_in_part,
            )?;
            cursor = cursor.checked_add(range.len() as u64).ok_or_else(|| {
                integrity(
                    "object_store_object_invalid",
                    "object read overflow".to_owned(),
                )
            })?;
            bytes.extend(range);
        }
        Ok(ResourceChunk {
            offset,
            bytes,
            eof: end == size,
        })
    }

    fn list(
        &mut self,
        _resource: &ResourceHandle,
        _locators: &ResourceLocatorSet,
        _cursor: Option<&str>,
        _limit: u32,
    ) -> ResourceResult<ResourcePage> {
        Err(ResourceError::Validation(
            "initial object-store plugin supports object Resources only".to_owned(),
        ))
    }
}

impl ResourceDeleter for ObjectResourceStore {
    fn binding(&self) -> &str {
        &self.binding
    }

    fn delete_and_verify_absent(&mut self, target: &ResourceDeletionTarget) -> ResourceResult<()> {
        target.verify()?;
        if target.subject.family.store_binding != self.binding {
            return Err(conflict(
                "object_store_cleanup_conflict",
                "object-store deleter does not own the durable deletion target".to_owned(),
            ));
        }
        if target.manifest.is_some() {
            return Err(ResourceError::Validation(
                "object-store deleter supports object Resource targets only".to_owned(),
            ));
        }
        let digest = &target.subject.family.content_digest;
        // The retained M1 target authorizes this deterministic family, including
        // members already removed by an earlier interrupted deletion.
        let index = ObjectIndexPayload {
            digest: digest.clone(),
            size: target.content_size,
            part_size: PART_SIZE as u64,
            part_count: object_part_count(target.content_size),
        };
        let inventory = self
            .inspect_object_family(&index)
            .map_err(deletion_integrity_error)?;
        let source = if inventory.index_present {
            let source = self
                .load_object_head(digest)
                .map_err(deletion_integrity_error)?;
            if source.1.content() != &index {
                return Err(integrity(
                    "object_store_cleanup_invalid",
                    "object-store content family index changed from the deletion target".to_owned(),
                ));
            }
            Some(source)
        } else {
            None
        };
        if inventory.part_count == index.part_count {
            self.verify_object_bytes(&index)
                .map_err(deletion_integrity_error)?;
        }
        self.fence_object_deletion(&index, source)?;
        self.delete_object_family(&index)?;
        self.verify_deleted_object_absence(&index)
    }
}

fn validate_object_family_entry(
    prefix: &Path,
    index: &ObjectIndexPayload,
    metadata: &ObjectMeta,
) -> ResourceResult<ObjectFamilyEntry> {
    if metadata.location == prefix.clone().join("index.json") {
        if metadata.size == 0 || metadata.size > MAX_OBJECT_INDEX_BYTES {
            return Err(integrity(
                "object_store_object_invalid",
                "object-store content index has an invalid encoded size".to_owned(),
            ));
        }
        return Ok(ObjectFamilyEntry::Index);
    }
    let location = metadata.location.to_string();
    let part_prefix = format!("{prefix}/parts/");
    let encoded_index = location.strip_prefix(&part_prefix).ok_or_else(|| {
        integrity(
            "object_store_object_invalid",
            "object-store content family contains an unrecognized physical object".to_owned(),
        )
    })?;
    if encoded_index.len() != 20 || !encoded_index.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(integrity(
            "object_store_object_invalid",
            "object-store content family contains an unrecognized physical object".to_owned(),
        ));
    }
    let part_index = encoded_index.parse::<u64>().map_err(|error| {
        integrity(
            "object_store_object_invalid",
            format!("object-store content part index is malformed: {error}"),
        )
    })?;
    if part_index >= index.part_count || metadata.size != object_part_size(index.size, part_index)?
    {
        return Err(integrity(
            "object_store_object_invalid",
            "object-store content family contains an unrecognized or malformed part".to_owned(),
        ));
    }
    Ok(ObjectFamilyEntry::Part)
}

fn deletion_integrity_error(error: ResourceError) -> ResourceError {
    match error {
        ResourceError::Integrity { code, message } if code == "object_store_object_invalid" => {
            integrity("object_store_cleanup_invalid", message)
        }
        other => other,
    }
}

fn object_error(error: ObjectError) -> ResourceError {
    match error {
        error @ ObjectError::AlreadyExists { .. } => {
            conflict("object_store_already_exists", error.to_string())
        }
        error @ ObjectError::Precondition { .. } => {
            conflict("object_store_precondition_failed", error.to_string())
        }
        error @ ObjectError::NotModified { .. } => {
            conflict("object_store_not_modified", error.to_string())
        }
        error @ ObjectError::NotFound { .. } => ResourceError::NotFound(error.to_string()),
        error @ ObjectError::InvalidPath { .. } => {
            substrate_with_code("object_store_invalid_path", error)
        }
        #[cfg(any(test, feature = "azure", feature = "gcp"))]
        error @ ObjectError::JoinError { .. } => {
            substrate_with_code("object_store_join_failure", error)
        }
        error @ ObjectError::NotSupported { .. } => {
            substrate_with_code("object_store_unsupported_operation", error)
        }
        error @ ObjectError::NotImplemented { .. } => {
            substrate_with_code("object_store_unimplemented_operation", error)
        }
        error @ ObjectError::PermissionDenied { .. } => {
            substrate_with_code("object_store_permission_denied", error)
        }
        error @ ObjectError::Unauthenticated { .. } => {
            substrate_with_code("object_store_authentication_failed", error)
        }
        error @ ObjectError::UnknownConfigurationKey { .. } => {
            substrate_with_code("object_store_configuration_invalid", error)
        }
        error @ ObjectError::Generic { .. } => {
            substrate_with_code("object_store_provider_failure", error)
        }
        error => substrate_with_code("object_store_unclassified_provider_failure", error),
    }
}

fn is_create_conflict(code: &str) -> bool {
    matches!(
        code,
        "object_store_already_exists" | "object_store_precondition_failed"
    )
}

fn retained_chunk_error(error: ObjectError) -> ResourceError {
    match error {
        ObjectError::NotFound { .. } => integrity(
            "object_store_acknowledged_chunk_missing",
            "object-store acknowledged chunk authority is missing".to_owned(),
        ),
        other => object_error(other),
    }
}

fn core_error(error: impl std::fmt::Display) -> ResourceError {
    integrity("object_store_canonical_encoding_failed", error.to_string())
}

fn core_integrity(error: impl std::fmt::Display) -> ResourceError {
    integrity("object_store_canonical_integrity_failed", error.to_string())
}

fn conflict(code: &'static str, message: impl Into<String>) -> ResourceError {
    ResourceError::Conflict {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn integrity(code: &'static str, message: impl Into<String>) -> ResourceError {
    ResourceError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn substrate(error: impl std::fmt::Display) -> ResourceError {
    substrate_with_code("object_store_provider_failure", error)
}

fn substrate_with_code(code: &'static str, error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Substrate {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

fn digest_key(digest: &str) -> ResourceResult<&str> {
    let key = digest.strip_prefix("sha256:").ok_or_else(|| {
        ResourceError::Validation("object-store digest is not SHA-256".to_owned())
    })?;
    if key.len() != 64
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResourceError::Validation(
            "object-store digest is malformed".to_owned(),
        ));
    }
    Ok(key)
}

fn object_part_count(size: u64) -> u64 {
    size.div_ceil(PART_SIZE as u64)
}

#[cfg(feature = "gcp")]
fn verify_google_cloud_service_account_key(credentials: &str) -> ResourceResult<()> {
    let value: serde_json::Value = cymule_core::decode_json(credentials.as_bytes())
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    if value.get("gcs_base_url").is_some() {
        return Err(ResourceError::Validation(
            "GCS inventory authority rejects credential-selected endpoint overrides".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(feature = "azure")]
fn validate_azure_inventory_location(
    account: impl Into<String>,
    container: impl Into<String>,
) -> ResourceResult<(String, String)> {
    let account = account.into();
    let container = container.into();
    let valid_account = (3..=24).contains(&account.len())
        && account
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
    let valid_container = (3..=63).contains(&container.len())
        && container
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && container
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && container
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !container.contains("--");
    if !valid_account || !valid_container {
        return Err(ResourceError::Validation(
            "Azure inventory authority requires official account and container names".to_owned(),
        ));
    }
    Ok((account, container))
}

fn object_part_size(size: u64, part_index: u64) -> ResourceResult<u64> {
    let count = object_part_count(size);
    if part_index >= count {
        return Err(integrity(
            "object_store_object_invalid",
            "object part index exceeds retained size".to_owned(),
        ));
    }
    let start = part_index.checked_mul(PART_SIZE as u64).ok_or_else(|| {
        integrity(
            "object_store_object_invalid",
            "object part offset overflow".to_owned(),
        )
    })?;
    Ok((size - start).min(PART_SIZE as u64))
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

#[cfg(test)]
mod public_conformance_tests;

#[cfg(test)]
mod publication_replay_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fmt::{Display, Formatter};
    use std::sync::atomic::AtomicBool;
    #[cfg(unix)]
    use std::{
        os::unix::process::ExitStatusExt as _,
        process::{Command, Stdio},
        time::Duration,
    };

    use cymule_resource::{ResourceClient, ResourceWriteIntent};
    use futures_util::{StreamExt as _, TryStreamExt as _};
    #[cfg(unix)]
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutPayload, PutResult,
    };

    use super::*;

    fn provider_failure() -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::other("provider failure"))
    }

    #[test]
    fn provider_error_variants_keep_stable_resource_codes() {
        let conflict_cases = [
            (
                ObjectError::AlreadyExists {
                    path: "object".to_owned(),
                    source: provider_failure(),
                },
                "object_store_already_exists",
            ),
            (
                ObjectError::Precondition {
                    path: "object".to_owned(),
                    source: provider_failure(),
                },
                "object_store_precondition_failed",
            ),
            (
                ObjectError::NotModified {
                    path: "object".to_owned(),
                    source: provider_failure(),
                },
                "object_store_not_modified",
            ),
        ];
        for (error, expected_code) in conflict_cases {
            assert!(matches!(
                object_error(error),
                ResourceError::Conflict { code, .. } if code == expected_code
            ));
        }

        let substrate_cases = [
            (
                ObjectError::NotSupported {
                    source: provider_failure(),
                },
                "object_store_unsupported_operation",
            ),
            (
                ObjectError::NotImplemented {
                    operation: "put".to_owned(),
                    implementer: "provider".to_owned(),
                },
                "object_store_unimplemented_operation",
            ),
            (
                ObjectError::PermissionDenied {
                    path: "object".to_owned(),
                    source: provider_failure(),
                },
                "object_store_permission_denied",
            ),
            (
                ObjectError::Unauthenticated {
                    path: "object".to_owned(),
                    source: provider_failure(),
                },
                "object_store_authentication_failed",
            ),
            (
                ObjectError::UnknownConfigurationKey {
                    store: "provider",
                    key: "endpoint".to_owned(),
                },
                "object_store_configuration_invalid",
            ),
            (
                ObjectError::Generic {
                    store: "provider",
                    source: provider_failure(),
                },
                "object_store_provider_failure",
            ),
        ];
        for (error, expected_code) in substrate_cases {
            assert!(matches!(
                object_error(error),
                ResourceError::Substrate { code, .. } if code == expected_code
            ));
        }

        assert!(matches!(
            object_error(ObjectError::NotFound {
                path: "object".to_owned(),
                source: provider_failure(),
            }),
            ResourceError::NotFound(_)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn physical_generation_marker_rejects_legacy_prefix_before_same_write_id_can_fork() {
        let backend = Arc::new(InMemory::new());
        backend
            .put(
                &Path::from("legacy/uploads/old/record.json"),
                br#"{"record_version":"cymule.resource-object-store-upload/3"}"#
                    .to_vec()
                    .into(),
            )
            .await
            .expect("legacy physical bytes seed");
        assert!(matches!(
            ObjectResourceStore::new(backend.clone(), "legacy", "object:legacy"),
            Err(ResourceError::Integrity { code, .. })
                if code == "object_store_layout_invalid"
        ));
        assert!(
            backend
                .head(&Path::from("legacy/layout.json"))
                .await
                .is_err(),
            "failed generation admission must not create a second authority marker"
        );

        backend
            .put(
                &Path::from("wrong/layout.json"),
                br#"{"layout_version":"cymule.resource-object-store-layout/0"}"#
                    .to_vec()
                    .into(),
            )
            .await
            .expect("wrong marker seeds");
        assert!(matches!(
            ObjectResourceStore::new(backend, "wrong", "object:wrong"),
            Err(ResourceError::Integrity { code, .. })
                if code == "object_store_layout_invalid"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binding_uses_public_unicode_scalar_boundary_before_provider_mutation() {
        let accepted = Arc::new(InMemory::new());
        ObjectResourceStore::new(accepted, "accepted", "界".repeat(512))
            .expect("512-scalar binding opens");

        let rejected = Arc::new(InMemory::new());
        assert!(matches!(
            ObjectResourceStore::new(rejected.clone(), "rejected", "界".repeat(513)),
            Err(ResourceError::Validation(_))
        ));
        assert!(
            rejected
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .expect("rejected provider remains readable")
                .is_empty(),
            "invalid binding must be rejected before layout, canary, or GC mutation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_generation_rejects_legacy_record_before_projection() {
        let backend = Arc::new(InMemory::new());
        let mut store =
            ObjectResourceStore::new(backend.clone(), "legacy-catalog", "object:legacy-catalog")
                .expect("adapter builds");
        let namespace = "test.catalog.legacy/1";
        let key = "record";
        let payload = b"legacy catalog".to_vec();
        let record = ResourceCatalogRecord {
            record_version: "cymule.resource-catalog-record/1".to_owned(),
            namespace: namespace.to_owned(),
            key: key.to_owned(),
            record_id: cymule_core::content_id(
                "cymule.resource-catalog-record/1",
                &(namespace, key, payload.as_slice()),
            )
            .expect("legacy catalog identity derives"),
            payload,
        };
        backend
            .put(
                &store
                    .catalog_path(namespace, key)
                    .expect("catalog locator derives"),
                cymule_core::canonical_bytes(&record)
                    .expect("legacy record encodes")
                    .into(),
            )
            .await
            .expect("legacy catalog record seeds");
        assert!(matches!(
            store.get_catalog_record(namespace, key),
            Err(ResourceError::Validation(message)) if message.contains("version")
        ));
    }
    #[cfg(unix)]
    use cymule_test_world::{ManagedChild, TestWorld};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum InventoryFault {
        Unordered,
        Duplicate,
        Missing,
        InclusiveOffset,
        StaleAfterDelete,
    }

    #[derive(Debug)]
    struct AdversarialInventoryStore {
        inner: InMemory,
        fault: InventoryFault,
        armed: AtomicBool,
        stale: Arc<Mutex<Vec<ObjectMeta>>>,
    }

    impl AdversarialInventoryStore {
        fn new(fault: InventoryFault, armed: bool) -> Self {
            Self {
                inner: InMemory::new(),
                fault,
                armed: AtomicBool::new(armed),
                stale: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn arm(&self) {
            self.armed.store(true, Ordering::SeqCst);
        }
    }

    impl Display for AdversarialInventoryStore {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("adversarial-inventory-store")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for AdversarialInventoryStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    impl inventory_sealed::Sealed for AdversarialInventoryStore {}

    impl ObjectStoreInventory for AdversarialInventoryStore {
        fn ordered_inventory(
            &self,
            prefix: Option<&Path>,
            after: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let prefix = prefix.cloned();
            let after = after.cloned();
            let inner = self.inner.clone();
            let armed = self.armed.load(Ordering::SeqCst);
            let fault = self.fault;
            let stale = Arc::clone(&self.stale);
            futures_util::stream::once(async move {
                let stream = if armed && fault == InventoryFault::InclusiveOffset && after.is_some()
                {
                    inner.list(prefix.as_ref())
                } else if let Some(after) = &after {
                    inner.list_with_offset(prefix.as_ref(), after)
                } else {
                    inner.list(prefix.as_ref())
                };
                let mut objects: Vec<ObjectMeta> = stream.try_collect().await?;
                if armed {
                    match fault {
                        InventoryFault::Unordered if objects.len() > 1 => objects.swap(0, 1),
                        InventoryFault::Duplicate if !objects.is_empty() => {
                            objects.insert(1, objects[0].clone());
                        }
                        InventoryFault::Missing if objects.len() > 1 => {
                            objects.remove(1);
                        }
                        InventoryFault::StaleAfterDelete => {
                            let mut retained =
                                stale.lock().expect("stale inventory remains healthy");
                            if objects.is_empty() {
                                objects.clone_from(&retained);
                            } else {
                                retained.clone_from(&objects);
                            }
                        }
                        InventoryFault::Unordered
                        | InventoryFault::Duplicate
                        | InventoryFault::Missing
                        | InventoryFault::InclusiveOffset => {}
                    }
                }
                Ok::<_, ObjectError>(futures_util::stream::iter(objects.into_iter().map(Ok)))
            })
            .try_flatten()
            .boxed()
        }
    }

    #[cfg(unix)]
    #[derive(Debug)]
    struct PersistentConditionalLocalStore {
        inner: LocalFileSystem,
        cas_lock: std::fs::File,
        publish_barrier: Option<std::path::PathBuf>,
    }

    #[cfg(unix)]
    impl PersistentConditionalLocalStore {
        fn new(root: &std::path::Path, publish_barrier: Option<std::path::PathBuf>) -> Self {
            Self {
                inner: LocalFileSystem::new_with_prefix(root).expect("persistent backend opens"),
                cas_lock: std::fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(root.join(".conditional-cas.lock"))
                    .expect("persistent CAS lock opens"),
                publish_barrier,
            }
        }

        fn maybe_block_after_first_part(&self, location: &Path) {
            let Some(marker) = &self.publish_barrier else {
                return;
            };
            if !location.to_string().ends_with("parts/00000000000000000000") {
                return;
            }
            std::fs::write(marker, b"content-part-persisted")
                .expect("process-death barrier persists");
            loop {
                std::thread::park_timeout(Duration::from_mins(1));
            }
        }
    }

    #[cfg(unix)]
    impl Display for PersistentConditionalLocalStore {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("persistent-conditional-local-test-store")
        }
    }

    #[cfg(unix)]
    #[async_trait::async_trait]
    impl ObjectStore for PersistentConditionalLocalStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            mut options: PutOptions,
        ) -> object_store::Result<PutResult> {
            if let PutMode::Update(expected) = &options.mode {
                <std::fs::File as fs4::FileExt>::lock(&self.cas_lock).map_err(|error| {
                    ObjectError::Generic {
                        store: "persistent conditional local test store",
                        source: Box::new(error),
                    }
                })?;
                let current = self.inner.head(location).await;
                let current = match current {
                    Ok(current) => current,
                    Err(error) => {
                        <std::fs::File as fs4::FileExt>::unlock(&self.cas_lock)
                            .expect("persistent CAS lock releases after read failure");
                        return Err(error);
                    }
                };
                if current.e_tag != expected.e_tag || current.version != expected.version {
                    <std::fs::File as fs4::FileExt>::unlock(&self.cas_lock)
                        .expect("persistent CAS lock releases");
                    return Err(ObjectError::Precondition {
                        path: location.to_string(),
                        source: "test backend conditional version changed".into(),
                    });
                }
                options.mode = PutMode::Overwrite;
                let result = self.inner.put_opts(location, payload, options).await;
                <std::fs::File as fs4::FileExt>::unlock(&self.cas_lock)
                    .expect("persistent CAS lock releases");
                let result = result?;
                self.maybe_block_after_first_part(location);
                return Ok(result);
            }
            let result = self.inner.put_opts(location, payload, options).await?;
            self.maybe_block_after_first_part(location);
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[cfg(unix)]
    impl inventory_sealed::Sealed for PersistentConditionalLocalStore {}

    #[cfg(unix)]
    impl ObjectStoreInventory for PersistentConditionalLocalStore {
        fn ordered_inventory(
            &self,
            prefix: Option<&Path>,
            after: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            let inventory = self.inner.list(prefix);
            let after = after.cloned();
            futures_util::stream::once(async move {
                let mut objects = inventory.try_collect::<Vec<_>>().await?;
                objects
                    .retain(|object| after.as_ref().is_none_or(|after| object.location > *after));
                objects.sort_by(|left, right| left.location.cmp(&right.location));
                Ok::<_, ObjectError>(futures_util::stream::iter(objects.into_iter().map(Ok)))
            })
            .try_flatten()
            .boxed()
        }
    }

    fn reconcile_upload_content_to_completion(store: &mut ObjectResourceStore) {
        for _ in 0..1024 {
            if store
                .reconcile_upload_content()
                .expect("upload-content reclamation advances")
                .complete
            {
                return;
            }
        }
        panic!("upload-content reclamation did not complete within its structural bound");
    }

    fn persist_first_upload_gc_page(store: &mut ObjectResourceStore) {
        for _ in 0..1024 {
            if matches!(
                store
                    .load_upload_gc_record()
                    .expect("upload GC authority loads")
                    .0
                    .phase,
                UploadGcPhase::SweepContent { page: Some(_), .. }
            ) {
                return;
            }
            store
                .reconcile_upload_content()
                .expect("upload GC advances to an admitted content page");
        }
        panic!("upload-content reclamation did not admit a page within its structural bound");
    }

    fn upload_epoch_is_empty(store: &ObjectResourceStore, epoch: u64) -> bool {
        let prefix = store
            .key(&format!(
                "upload-content/{}/epochs/{epoch:020}",
                store.content_namespace()
            ))
            .expect("upload epoch prefix derives");
        let backend = Arc::clone(&store.store);
        store
            .block_on(async move { backend.list(Some(&prefix)).next().await })
            .expect("upload epoch lists")
            .is_none()
    }

    #[test]
    fn provider_inventory_admission_rejects_unordered_duplicate_and_missing_results() {
        for fault in [
            InventoryFault::Unordered,
            InventoryFault::Duplicate,
            InventoryFault::Missing,
        ] {
            let backend = Arc::new(AdversarialInventoryStore::new(fault, true));
            assert!(matches!(
                ObjectResourceStore::new(
                    backend,
                    format!("inventory-admission-{fault:?}"),
                    format!("object:inventory-admission-{fault:?}"),
                ),
                Err(ResourceError::Integrity { code, .. })
                    if code == "object_store_inventory_contract_violation"
            ));
        }
    }

    #[cfg(feature = "gcp")]
    #[test]
    fn gcs_inventory_wrapper_rejects_credential_selected_endpoints() {
        let credentials = r#"{
            "private_key":"unused",
            "private_key_id":"unused",
            "client_email":"unused",
            "gcs_base_url":"https://example.invalid"
        }"#;
        assert!(matches!(
            GoogleCloudInventory::from_service_account_key("bucket", credentials),
            Err(ResourceError::Validation(_))
        ));
        assert!(matches!(
            GoogleCloudInventory::from_service_account_key(
                "bucket",
                r#"{"client_email":"first@example.invalid","client_email":"second@example.invalid"}"#,
            ),
            Err(ResourceError::Validation(_))
        ));
        let provider = GoogleCloudInventory::from_bearer_token("bucket", "secret-token")
            .expect("closed official GCS configuration builds without provider I/O");
        assert!(!provider.to_string().contains("secret-token"));
    }

    #[cfg(feature = "azure")]
    #[test]
    fn azure_inventory_wrapper_builds_only_from_closed_official_inputs() {
        let provider =
            AzureBlobInventory::from_bearer_token("account", "container", "secret-token")
                .expect("closed official Azure configuration builds without provider I/O");
        assert!(!provider.to_string().contains("secret-token"));
        assert!(matches!(
            AzureBlobInventory::from_bearer_token(
                "attacker.example/path",
                "container",
                "secret-token",
            ),
            Err(ResourceError::Validation(_))
        ));
        assert!(matches!(
            AzureBlobInventory::from_bearer_token("account", "bad--container", "secret-token"),
            Err(ResourceError::Validation(_))
        ));
    }

    #[test]
    fn provider_inventory_enforces_a_strict_exclusive_cross_page_continuation() {
        let backend = Arc::new(AdversarialInventoryStore::new(
            InventoryFault::InclusiveOffset,
            false,
        ));
        let store = ObjectResourceStore::new(
            backend.clone(),
            "inclusive-inventory",
            "object:inclusive-inventory",
        )
        .expect("adapter admits healthy inventory");
        let prefix = store.upload_prefix().expect("upload prefix derives");
        for ordinal in 0..=UPLOAD_GC_OBJECT_PAGE {
            let key = format!("{ordinal:064x}");
            store
                .put_immutable_bytes(
                    &prefix.clone().join(key).join("record.json"),
                    vec![u8::try_from(ordinal % 251).expect("probe byte fits")],
                )
                .expect("probe object writes");
        }
        let (first_page, first_complete) = store
            .list_object_page(&prefix, None)
            .expect("first bounded inventory page reads");
        assert_eq!(first_page.len(), UPLOAD_GC_OBJECT_PAGE);
        assert!(!first_complete);
        let cursor = ObjectResourceStore::relative_inventory_path(
            &prefix,
            first_page.last().expect("first page has a cursor"),
        )
        .expect("relative cursor derives");
        let (last_page, last_complete) = store
            .list_object_page(&prefix, Some(&cursor))
            .expect("exclusive continuation reads the terminal page");
        assert_eq!(last_page.len(), 1);
        assert!(last_complete);
        backend.arm();
        assert!(matches!(
            store.list_object_page(&prefix, Some(&cursor)),
            Err(ResourceError::Integrity { code, .. })
                if code == "object_store_inventory_contract_violation"
        ));
    }

    #[test]
    fn lost_gc_head_acknowledgement_reloads_the_exact_persisted_phase() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store =
            ObjectResourceStore::new(backend, "gc-response-loss", "object:gc-response-loss")
                .expect("adapter builds");
        store
            .test_faults
            .upload_gc_ack
            .store(true, Ordering::SeqCst);
        let progress = store
            .reconcile_upload_content()
            .expect("lost acknowledgement is reloaded from exact GC authority");
        assert_eq!(progress.current_epoch, 1);
        assert!(!progress.complete);
        reconcile_upload_content_to_completion(&mut store);
    }

    #[test]
    fn upload_gc_page_authority_has_an_exact_fixed_record_bound() {
        let backend = Arc::new(InMemory::new());
        let store = ObjectResourceStore::new(backend, "gc-page-bound", "object:gc-page-bound")
            .expect("adapter builds");
        let paths = (0..UPLOAD_GC_OBJECT_PAGE)
            .map(|ordinal| format!("{:020}/nodes/{ordinal:064x}.json", 0))
            .collect::<Vec<_>>();
        let record = UploadGcRecord {
            record_version: UPLOAD_GC_RECORD_VERSION.to_owned(),
            store_binding: store.binding_id(),
            current_epoch: 1,
            phase: UploadGcPhase::SweepContent {
                after: None,
                deleted_in_pass: 0,
                page: Some(UploadGcPage {
                    paths: paths.clone(),
                    end_of_inventory: false,
                }),
            },
        };
        store
            .verify_upload_gc_record(&record)
            .expect("maximum GC page verifies");
        let encoded = cymule_core::canonical_bytes(&record).expect("maximum GC page encodes");
        assert!(
            u64::try_from(encoded.len()).expect("record length fits") <= MAX_UPLOAD_GC_RECORD_BYTES
        );

        let mut oversized = record;
        let UploadGcPhase::SweepContent {
            page: Some(page), ..
        } = &mut oversized.phase
        else {
            panic!("test GC page remains present");
        };
        page.paths
            .push(format!("{:020}/nodes/{:064x}.json", 0, paths.len()));
        assert!(matches!(
            store.verify_upload_gc_record(&oversized),
            Err(ResourceError::Integrity { code, .. })
                if code == "object_store_gc_authority_invalid"
        ));
    }

    #[test]
    fn lost_gc_page_completion_acknowledgement_replays_the_admitted_page() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend.clone(),
            "gc-page-response-loss",
            "object:gc-page-response-loss",
        )
        .expect("adapter builds");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:gc-page-response-loss".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .write_chunk(&session, 0, b"lost page acknowledgement")
            .expect("candidate writes");
        store.abort_write(&session).expect("upload aborts");
        persist_first_upload_gc_page(&mut store);
        let (admitted, version) = store
            .load_upload_gc_record()
            .expect("admitted page authority reloads");
        let expected_confirmed = match &admitted.phase {
            UploadGcPhase::SweepContent {
                page: Some(page), ..
            } => u64::try_from(page.paths.len()).expect("page cardinality fits"),
            _ => panic!("admitted content page remains current"),
        };

        store
            .test_faults
            .upload_gc_ack
            .store(true, Ordering::SeqCst);
        let progress = store
            .reconcile_upload_content()
            .expect("persisted page completion is confirmed after response loss");
        assert_eq!(progress.confirmed_absent_objects, expected_confirmed);
        let postcondition = store
            .load_upload_gc_record()
            .expect("completed page authority loads")
            .0;
        drop(store);
        let mut reopened = ObjectResourceStore::new(
            backend,
            "gc-page-response-loss",
            "object:gc-page-response-loss",
        )
        .expect("adapter reopens after lost acknowledgement");
        let replay = reopened
            .reconcile_upload_gc_objects(admitted, version)
            .expect("the same admitted page replays after all its targets are absent");
        assert_eq!(replay, progress);
        assert_eq!(
            reopened
                .load_upload_gc_record()
                .expect("replay head loads")
                .0,
            postcondition,
            "replay must preserve the exact completed page head"
        );
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }

    #[test]
    fn two_drivers_replay_one_admitted_deletion_page_exactly() {
        let backend = Arc::new(InMemory::new());
        let mut setup = ObjectResourceStore::new(
            backend.clone(),
            "exact-concurrent-gc",
            "object:exact-concurrent-gc",
        )
        .expect("setup adapter builds");
        let session = setup
            .begin_write(&ResourceWriteIntent {
                write_id: "write:exact-concurrent-gc".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        setup
            .write_chunk(&session, 0, b"one admitted concurrent page")
            .expect("candidate writes");
        setup.abort_write(&session).expect("upload aborts");
        persist_first_upload_gc_page(&mut setup);
        let (record, version) = setup
            .load_upload_gc_record()
            .expect("admitted page authority loads");
        let expected_confirmed = match &record.phase {
            UploadGcPhase::SweepContent {
                page: Some(page), ..
            } => u64::try_from(page.paths.len()).expect("page cardinality fits"),
            _ => panic!("admitted content page remains current"),
        };
        drop(setup);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let backend = backend.clone();
            let barrier = barrier.clone();
            let record = record.clone();
            let version = version.clone();
            workers.push(std::thread::spawn(move || {
                let store = ObjectResourceStore::new(
                    backend,
                    "exact-concurrent-gc",
                    "object:exact-concurrent-gc",
                )
                .expect("concurrent adapter builds");
                barrier.wait();
                store.reconcile_upload_gc_objects(record, version)
            }));
        }
        let progress = workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .expect("GC worker joins")
                    .expect("admitted page replay converges")
            })
            .collect::<Vec<_>>();
        assert_eq!(progress.len(), 2);
        assert_eq!(progress[0], progress[1]);
        assert_eq!(progress[0].confirmed_absent_objects, expected_confirmed);
        assert_eq!(progress[0].examined_objects, expected_confirmed);
        assert_eq!(progress[0].current_epoch, record.current_epoch);
        assert!(!progress[0].complete);

        let mut reopened =
            ObjectResourceStore::new(backend, "exact-concurrent-gc", "object:exact-concurrent-gc")
                .expect("authority reopens");
        let mut expected_head = record;
        expected_head.phase = UploadGcPhase::SweepContent {
            after: None,
            deleted_in_pass: 0,
            page: None,
        };
        assert_eq!(
            reopened
                .load_upload_gc_record()
                .expect("completed head loads")
                .0,
            expected_head,
            "concurrent drivers must close the same admitted page exactly once"
        );
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }

    #[test]
    fn begin_write_converges_a_head_created_after_the_gc_inventory_fence() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut writer = ObjectResourceStore::new(
            backend.clone(),
            "begin-epoch-race",
            "object:begin-epoch-race",
        )
        .expect("writer adapter builds");
        let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
        *writer
            .test_faults
            .begin_upload_pause
            .lock()
            .expect("begin-upload pause remains healthy") = Some(ChunkCandidatePause {
            reached: reached_sender,
            resume: resume_receiver,
        });
        let begin = std::thread::spawn(move || {
            writer.begin_write(&ResourceWriteIntent {
                write_id: "write:begin-epoch-race".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
        });
        reached_receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("begin reaches the pre-create epoch barrier");

        let mut collector =
            ObjectResourceStore::new(backend, "begin-epoch-race", "object:begin-epoch-race")
                .expect("collector adapter builds");
        reconcile_upload_content_to_completion(&mut collector);
        resume_sender.send(()).expect("paused begin resumes");
        let session = begin
            .join()
            .expect("begin thread joins")
            .expect("stale empty head converges to the current epoch");
        let head = collector
            .load_record(&session.upload_id)
            .expect("converged upload head loads")
            .0;
        assert_eq!(head.content_epoch, 1);
        collector
            .write_chunk(&session, 0, b"current epoch")
            .expect("converged session writes in the current epoch");
    }

    #[test]
    fn concurrent_gc_drivers_converge_without_forking_the_epoch_authority() {
        let backend = Arc::new(InMemory::new());
        let mut setup =
            ObjectResourceStore::new(backend.clone(), "concurrent-gc", "object:concurrent-gc")
                .expect("setup adapter builds");
        let session = setup
            .begin_write(&ResourceWriteIntent {
                write_id: "write:concurrent-gc".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        setup
            .write_chunk(&session, 0, b"concurrent orphan")
            .expect("candidate writes");
        setup.abort_write(&session).expect("upload aborts");
        drop(setup);

        let complete = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let backend = backend.clone();
            let complete = complete.clone();
            workers.push(std::thread::spawn(move || {
                let mut store =
                    ObjectResourceStore::new(backend, "concurrent-gc", "object:concurrent-gc")
                        .expect("concurrent adapter builds");
                for _ in 0..4096 {
                    if complete.load(Ordering::SeqCst) {
                        return;
                    }
                    match store.reconcile_upload_content() {
                        Ok(progress) if progress.complete => {
                            complete.store(true, Ordering::SeqCst);
                            return;
                        }
                        Ok(_) => {}
                        Err(ResourceError::Conflict { code, .. })
                            if code == "object_store_precondition_failed" => {}
                        Err(error) => panic!("concurrent reclamation failed: {error}"),
                    }
                }
                panic!("concurrent reclamation exceeded its structural retry bound");
            }));
        }
        for worker in workers {
            worker.join().expect("GC worker joins");
        }
        let mut reopened =
            ObjectResourceStore::new(backend, "concurrent-gc", "object:concurrent-gc")
                .expect("authority reopens");
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }

    #[test]
    fn stale_inventory_after_delete_fails_closed() {
        let backend = Arc::new(AdversarialInventoryStore::new(
            InventoryFault::StaleAfterDelete,
            false,
        ));
        let mut store = ObjectResourceStore::new(
            backend.clone(),
            "stale-delete-inventory",
            "object:stale-delete-inventory",
        )
        .expect("adapter admits healthy inventory");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:stale-delete-inventory".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .write_chunk(&session, 0, b"stale inventory orphan")
            .expect("chunk writes");
        store.abort_write(&session).expect("upload aborts");
        backend.arm();
        for _ in 0..1024 {
            match store.reconcile_upload_content() {
                Err(ResourceError::Integrity { code, .. })
                    if code == "object_store_gc_authority_invalid" =>
                {
                    return;
                }
                Ok(_) => {}
                Err(error) => panic!("unexpected reclamation failure: {error}"),
            }
        }
        panic!("stale deletion inventory was not rejected");
    }

    #[test]
    fn tiny_chunk_cardinality_keeps_the_upload_head_fixed_and_cleanup_empty() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend,
            "tiny-chunk-cardinality",
            "object:tiny-chunk-cardinality",
        )
        .expect("adapter builds");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:tiny-chunk-cardinality".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .write_chunk(&session, 0, b"x")
            .expect("first tiny chunk writes");
        let first_size = cymule_core::canonical_bytes(
            &store.load_record(&session.upload_id).expect("head loads").0,
        )
        .expect("head encodes")
        .len();
        for offset in 1..=1024_u64 {
            store
                .write_chunk(&session, offset, b"x")
                .expect("tiny chunk writes");
        }
        let final_record = store
            .load_record(&session.upload_id)
            .expect("head reloads")
            .0;
        assert_eq!(final_record.next_offset, 1025);
        assert_eq!(final_record.chunk_count, 1025);
        assert_eq!(
            final_record.chunk_root.as_ref().map(|root| root.leaf_count),
            Some(1025)
        );
        let final_size = cymule_core::canonical_bytes(&final_record)
            .expect("head encodes")
            .len();
        assert!(
            final_size <= first_size + 9,
            "head size may grow only with its three fixed-width count scalars"
        );

        let receipt = store.abort_write(&session).expect("abort terminates head");
        assert_eq!(receipt.removed_staging_objects, 0);
        assert_eq!(receipt.removed_chunks, 0);
        assert!(receipt.plan.targets.is_empty());
    }

    #[test]
    fn immutable_chunk_candidate_and_lost_ack_recover_exactly_after_reopen() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store =
            ObjectResourceStore::new(backend.clone(), "chunk-recovery", "object:chunk-recovery")
                .expect("adapter builds");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:chunk-recovery".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .test_faults
            .chunk_candidate
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            store.write_chunk(&session, 0, b"first"),
            Err(ResourceError::Substrate { code, .. })
                if code == "object_store_chunk_candidate_interrupted"
        ));
        let head = store
            .load_record(&session.upload_id)
            .expect("unmodified head remains")
            .0;
        assert_eq!(head.next_offset, 0);
        assert_eq!(head.chunk_count, 0);
        assert!(head.chunk_root.is_none());
        drop(store);

        let mut reopened =
            ObjectResourceStore::new(backend, "chunk-recovery", "object:chunk-recovery")
                .expect("adapter reopens");
        reopened
            .write_chunk(&session, 0, b"first")
            .expect("planned chunk converges");
        reopened
            .test_faults
            .chunk_ack_receipt
            .store(true, Ordering::SeqCst);
        reopened
            .write_chunk(&session, 5, b"second")
            .expect("lost acknowledgement is confirmed from exact retained authority");
        let publication = reopened
            .commit_write(&session)
            .expect("recovered chunks commit");
        let mut output = Vec::new();
        ResourceClient::new(reopened)
            .copy_to(&publication, 1024, &mut output)
            .expect("recovered content reads");
        assert_eq!(output, b"firstsecond");
    }

    #[test]
    fn crashed_unacknowledged_candidate_is_reclaimed_after_abort_and_reopen() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend.clone(),
            "candidate-orphan-gc",
            "object:candidate-orphan-gc",
        )
        .expect("adapter builds");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:candidate-orphan-gc".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .test_faults
            .chunk_candidate
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            store.write_chunk(&session, 0, b"unacknowledged orphan"),
            Err(ResourceError::Substrate { code, .. })
                if code == "object_store_chunk_candidate_interrupted"
        ));
        drop(store);

        let mut reopened =
            ObjectResourceStore::new(backend, "candidate-orphan-gc", "object:candidate-orphan-gc")
                .expect("adapter reopens");
        reopened
            .abort_write(&session)
            .expect("unchanged upload head aborts");
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }

    #[test]
    fn aborted_head_fences_a_paused_cross_instance_chunk_candidate() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut writer_store = ObjectResourceStore::new(
            backend.clone(),
            "concurrent-abort",
            "object:concurrent-abort",
        )
        .expect("writer adapter builds");
        let mut aborter_store = ObjectResourceStore::new(
            backend.clone(),
            "concurrent-abort",
            "object:concurrent-abort",
        )
        .expect("aborter adapter builds");
        let session = writer_store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:concurrent-abort".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
        *writer_store
            .test_faults
            .chunk_candidate_pause
            .lock()
            .expect("chunk candidate pause remains healthy") = Some(ChunkCandidatePause {
            reached: reached_sender,
            resume: resume_receiver,
        });
        let writer_session = session.clone();
        let writer = std::thread::spawn(move || {
            writer_store.write_chunk(&writer_session, 0, b"late immutable candidate")
        });
        reached_receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("writer reaches immutable candidate barrier");

        let receipt = aborter_store
            .abort_write(&session)
            .expect("abort terminalizes the unchanged head");
        assert!(receipt.plan.targets.is_empty());
        resume_sender.send(()).expect("paused writer resumes");
        assert!(matches!(
            writer.join().expect("writer thread joins"),
            Err(ResourceError::Conflict { code, .. })
                if code == "object_store_precondition_failed"
        ));
        let terminal = aborter_store
            .load_record(&session.upload_id)
            .expect("terminal head reloads")
            .0;
        assert_eq!(terminal.state, UploadState::Aborted);
        assert_eq!(terminal.next_offset, 0);
        assert_eq!(terminal.chunk_count, 0);
        assert!(terminal.chunk_root.is_none());
        drop(aborter_store);

        let mut reopened =
            ObjectResourceStore::new(backend, "concurrent-abort", "object:concurrent-abort")
                .expect("adapter reopens");
        assert_eq!(
            reopened
                .abort_write(&session)
                .expect("abort receipt replays"),
            receipt
        );
        assert!(matches!(
            reopened.commit_write(&session),
            Err(ResourceError::Conflict { code, .. })
                if code == "object_store_upload_conflict"
        ));
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }

    #[test]
    fn reclamation_preserves_a_shared_chunk_reachable_from_an_active_upload() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend,
            "shared-live-reclamation",
            "object:shared-live-reclamation",
        )
        .expect("adapter builds");
        let first = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:shared-live:first".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("first write begins");
        let second = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:shared-live:second".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("second write begins");
        store
            .write_chunk(&first, 0, b"shared immutable chunk")
            .expect("first shared chunk writes");
        store
            .write_chunk(&second, 0, b"shared immutable chunk")
            .expect("second shared chunk writes");
        store.abort_write(&first).expect("first upload aborts");

        reconcile_upload_content_to_completion(&mut store);
        assert!(upload_epoch_is_empty(&store, 0));
        let active = store
            .load_record(&second.upload_id)
            .expect("active upload reloads")
            .0;
        assert_eq!(active.content_epoch, 1);
        assert_eq!(active.next_offset, 22);
        let publication = store
            .commit_write(&second)
            .expect("migrated active upload commits");
        let mut output = Vec::new();
        ResourceClient::new(store)
            .copy_to(&publication, 1024, &mut output)
            .expect("migrated shared chunk reads");
        assert_eq!(output, b"shared immutable chunk");
    }

    #[test]
    fn lost_publishing_receipt_rebuilds_exact_visible_parts_without_multipart_state() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend.clone(),
            "publishing-recovery",
            "object:publishing-recovery",
        )
        .expect("adapter builds");
        let intent = ResourceWriteIntent {
            write_id: "write:publishing-recovery".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).expect("write begins");
        let first = vec![0x5a; PART_SIZE];
        store
            .write_chunk(&session, 0, &first)
            .expect("first fixed part writes");
        store
            .write_chunk(&session, PART_SIZE as u64, b"tail")
            .expect("tail writes");
        store
            .test_faults
            .publishing_receipt
            .store(true, Ordering::SeqCst);
        assert!(matches!(
            store.commit_write(&session),
            Err(ResourceError::Substrate { code, .. })
                if code == "object_store_publishing_receipt_lost"
        ));
        drop(store);

        let mut reopened =
            ObjectResourceStore::new(backend, "publishing-recovery", "object:publishing-recovery")
                .expect("adapter reopens");
        let publication = reopened
            .commit_write(&session)
            .expect("Publishing recovery converges from retained chunks");
        let mut output = Vec::new();
        ResourceClient::new(reopened)
            .copy_to(&publication, 1024 * 1024, &mut output)
            .expect("recovered object copies exactly");
        assert_eq!(output.len(), PART_SIZE + 4);
        assert_eq!(&output[..PART_SIZE], first.as_slice());
        assert_eq!(&output[PART_SIZE..], b"tail");
    }

    #[test]
    fn partial_visible_content_parts_recover_after_injected_publish_error() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store =
            ObjectResourceStore::new(backend.clone(), "part-recovery", "object:part-recovery")
                .expect("adapter builds");
        let intent = ResourceWriteIntent {
            write_id: "write:part-recovery".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).expect("write begins");
        let first = vec![0xa5; PART_SIZE];
        store
            .write_chunk(&session, 0, &first)
            .expect("first fixed part writes");
        store
            .write_chunk(&session, PART_SIZE as u64, b"tail")
            .expect("tail writes");
        store.test_faults.content_part.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.commit_write(&session),
            Err(ResourceError::Substrate { code, .. })
                if code == "object_store_content_publication_interrupted"
        ));
        drop(store);

        let mut reopened =
            ObjectResourceStore::new(backend, "part-recovery", "object:part-recovery")
                .expect("adapter reopens");
        let cleanup = reopened
            .abort_write(&session)
            .expect("abort converges Publishing and cleans upload chunks");
        assert!(cleanup.verified_absent);
        let publication = reopened
            .commit_write(&session)
            .expect("converged publication replays after cleanup");
        let mut output = Vec::new();
        ResourceClient::new(reopened)
            .copy_to(&publication, 1024 * 1024, &mut output)
            .expect("recovered object copies exactly");
        assert_eq!(&output[..PART_SIZE], first.as_slice());
        assert_eq!(&output[PART_SIZE..], b"tail");
    }

    #[test]
    fn cleanup_rebuilds_the_exact_receipt_after_plan_response_loss() {
        let _fault_guard = TEST_FAULT_LOCK
            .lock()
            .expect("fault test lock remains healthy");
        let backend = Arc::new(InMemory::new());
        let mut store = ObjectResourceStore::new(
            backend.clone(),
            "cleanup-recovery",
            "object:cleanup-recovery",
        )
        .expect("adapter builds");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:cleanup-recovery".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .write_chunk(&session, 0, b"first")
            .expect("first chunk writes");
        store
            .write_chunk(&session, 5, b"second")
            .expect("second chunk writes");
        store.test_faults.cleanup_plan.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.abort_write(&session),
            Err(ResourceError::Substrate { code, .. })
                if code == "object_store_cleanup_plan_receipt_lost"
        ));
        assert_eq!(
            store
                .cleanup_receipt(&session)
                .expect("incomplete cleanup remains queryable"),
            None
        );
        drop(store);

        let mut reopened =
            ObjectResourceStore::new(backend, "cleanup-recovery", "object:cleanup-recovery")
                .expect("adapter reopens");
        let receipt = reopened
            .abort_write(&session)
            .expect("persisted plan converges after response loss");
        assert_eq!(receipt.removed_staging_objects, 0);
        assert_eq!(receipt.removed_chunks, 0);
        assert_eq!(
            reopened.abort_write(&session).expect("abort replays"),
            receipt
        );
        assert_eq!(
            reopened
                .cleanup_receipt(&session)
                .expect("terminal receipt query succeeds"),
            Some(receipt)
        );
    }

    #[cfg(unix)]
    fn process_kill_intent() -> ResourceWriteIntent {
        ResourceWriteIntent {
            write_id: "write:object-store-process-kill".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        }
    }

    #[cfg(unix)]
    #[test]
    fn object_store_process_kill_worker_entry() {
        let Ok(root) = std::env::var("CYMULE_OBJECT_STORE_KILL_ROOT") else {
            return;
        };
        let marker =
            std::env::var("CYMULE_OBJECT_STORE_KILL_MARKER").expect("process-death marker exists");
        let backend = Arc::new(PersistentConditionalLocalStore::new(
            std::path::Path::new(&root),
            Some(marker.into()),
        ));
        let mut store = ObjectResourceStore::new(backend, "process-kill", "object:process-kill")
            .expect("adapter builds");
        let session = store
            .begin_write(&process_kill_intent())
            .expect("write begins");
        store
            .write_chunk(&session, 0, &vec![0xa5; 8 * 1024 * 1024])
            .expect("first chunk persists");
        store
            .write_chunk(&session, 8 * 1024 * 1024, b"tail")
            .expect("tail persists");
        let _ = store.commit_write(&session);
        panic!("publish barrier returned without SIGKILL");
    }

    #[cfg(unix)]
    #[test]
    fn persistent_object_store_recovers_after_real_sigkill_at_visible_part_boundary() {
        let world = TestWorld::new(0x000b_1ec7).expect("object-store test world creates");
        let backend_root = world
            .domain()
            .path("backend")
            .expect("backend path resolves");
        std::fs::create_dir(&backend_root).expect("backend root creates");
        let marker = world
            .domain()
            .path("content-part-persisted")
            .expect("barrier path resolves");
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("tests::object_store_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_OBJECT_STORE_KILL_ROOT", &backend_root)
            .env("CYMULE_OBJECT_STORE_KILL_MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
        child
            .wait_for_content(&marker, b"content-part-persisted", Duration::from_secs(30))
            .expect("worker reaches persisted visible-part barrier");
        assert_eq!(
            child.terminate().expect("worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        let backend = Arc::new(PersistentConditionalLocalStore::new(&backend_root, None));
        let mut reopened = ObjectResourceStore::new(backend, "process-kill", "object:process-kill")
            .expect("adapter reopens from persistent backend");
        let session = reopened
            .begin_write(&process_kill_intent())
            .expect("same write resumes retained authority");
        let publication = reopened
            .commit_write(&session)
            .expect("Publishing recovery converges after real SIGKILL");
        let mut output = Vec::new();
        ResourceClient::new(reopened)
            .copy_to(&publication, 1024 * 1024, &mut output)
            .expect("recovered publication copies exactly");
        assert_eq!(output.len(), 8 * 1024 * 1024 + 4);
        assert!(output[..8 * 1024 * 1024].iter().all(|byte| *byte == 0xa5));
        assert_eq!(&output[8 * 1024 * 1024..], b"tail");
    }

    #[cfg(unix)]
    #[test]
    fn object_store_gc_process_kill_worker_entry() {
        let Ok(root) = std::env::var("CYMULE_OBJECT_STORE_GC_KILL_ROOT") else {
            return;
        };
        let backend = Arc::new(PersistentConditionalLocalStore::new(
            std::path::Path::new(&root),
            None,
        ));
        let mut store = ObjectResourceStore::new(backend, "gc-process-kill", "object:gc-kill")
            .expect("GC worker adapter builds");
        persist_first_upload_gc_page(&mut store);
        store
            .reconcile_upload_content()
            .expect("GC admitted deletion page executes");
        panic!("GC deletion barrier did not stop the worker");
    }

    #[cfg(unix)]
    #[test]
    fn persisted_gc_phase_and_orphan_reclamation_survive_real_sigkill() {
        let world = TestWorld::new(0x000b_1ec8).expect("GC test world creates");
        let backend_root = world
            .domain()
            .path("backend")
            .expect("backend path resolves");
        std::fs::create_dir(&backend_root).expect("backend root creates");
        let marker = world
            .domain()
            .path("gc-page-deleted")
            .expect("GC barrier path resolves");
        let backend = Arc::new(PersistentConditionalLocalStore::new(&backend_root, None));
        let mut setup = ObjectResourceStore::new(backend, "gc-process-kill", "object:gc-kill")
            .expect("setup adapter builds");
        let session = setup
            .begin_write(&ResourceWriteIntent {
                write_id: "write:gc-process-kill".to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("orphan write begins");
        setup
            .write_chunk(&session, 0, b"process-death orphan")
            .expect("orphan chunk writes");
        setup.abort_write(&session).expect("orphan upload aborts");
        drop(setup);

        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("tests::object_store_gc_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_OBJECT_STORE_GC_KILL_ROOT", &backend_root)
            .env("CYMULE_OBJECT_STORE_GC_DELETE_MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("GC worker starts");
        child
            .wait_for_content(&marker, b"gc-page-deleted", Duration::from_secs(30))
            .expect("worker reaches the post-delete pre-head-CAS barrier");
        assert_eq!(
            child.terminate().expect("GC worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        let backend = Arc::new(PersistentConditionalLocalStore::new(&backend_root, None));
        let mut reopened = ObjectResourceStore::new(backend, "gc-process-kill", "object:gc-kill")
            .expect("GC authority reopens");
        let retained = reopened.load_upload_gc_record().expect("GC head loads").0;
        assert_eq!(retained.current_epoch, 1);
        assert!(matches!(
            retained.phase,
            UploadGcPhase::SweepContent { page: Some(_), .. }
        ));
        reconcile_upload_content_to_completion(&mut reopened);
        assert!(upload_epoch_is_empty(&reopened, 0));
    }
}
