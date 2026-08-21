//! Apache `object_store` Resource adapter for Cymule.

use std::future::Future;
use std::sync::Arc;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_WRITE_CHUNK, RESOURCE_CLEANUP_RECEIPT_VERSION,
    RESOURCE_LOCATOR_VERSION, ResourceCandidate, ResourceCatalogRecord, ResourceCatalogStore,
    ResourceChunk, ResourceCleanupReceipt, ResourceDeleteIntent, ResourceDeleter,
    ResourceDeletionObservation, ResourceError, ResourceHandle, ResourceIntegrity,
    ResourceLocation, ResourceLocatorSet, ResourceObservation, ResourcePage, ResourcePublication,
    ResourceResult, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
};
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{
    Error as ObjectError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};

const BINDING_VERSION: &str = "cymule.resource-object-store/2";
const UPLOAD_RECORD_VERSION: &str = "cymule.resource-object-store-upload/2";
const PART_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UploadState {
    Open,
    Publishing,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChunkState {
    Planned,
    Acknowledged,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRecord {
    offset: u64,
    size: usize,
    digest: String,
    state: ChunkState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    record_version: String,
    intent: ResourceWriteIntent,
    upload_id: String,
    next_offset: u64,
    chunks: Vec<ChunkRecord>,
    state: UploadState,
    publication: Option<ResourcePublication>,
}

/// Synchronous Cymule adapter over one asynchronous Apache object store.
pub struct ObjectResourceStore {
    store: Arc<dyn ObjectStore>,
    runtime: Option<Runtime>,
    prefix: String,
    binding: String,
}

impl ObjectResourceStore {
    /// Construct an adapter over a configured object-store implementation.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        let prefix = prefix.into().trim_matches('/').to_owned();
        let binding = binding.into();
        if binding.is_empty()
            || binding.chars().any(char::is_control)
            || prefix.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(ResourceError::Validation(
                "object-store prefix or binding is invalid".to_owned(),
            ));
        }
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("cymule-object-store")
            .build()
            .map_err(substrate)?;
        Ok(Self {
            store,
            runtime: Some(runtime),
            prefix,
            binding,
        })
    }

    /// Immutable resolver/store binding retained by Resource locations.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> ResourceResult<T> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| ResourceError::Substrate("object runtime is closed".to_owned()))?;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                return Err(ResourceError::Substrate(
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

    fn upload_id(write_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(BINDING_VERSION.as_bytes());
        hasher.update([0]);
        hasher.update(write_id.as_bytes());
        format!("upload:{}", hex_digest(&hasher.finalize()))
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
            || session.upload_id != Self::upload_id(&session.write_id)
        {
            return Err(ResourceError::Conflict(
                "object-store upload session is not authenticated by its write ID and binding"
                    .to_owned(),
            ));
        }
        let _ = Self::upload_key(&session.upload_id)?;
        Ok(())
    }

    fn record_path(&self, upload_id: &str) -> ResourceResult<Path> {
        self.key(&format!(
            "uploads/{}/record.json",
            Self::upload_key(upload_id)?
        ))
    }

    fn chunk_path(&self, upload_id: &str, offset: u64) -> ResourceResult<Path> {
        self.key(&format!(
            "uploads/{}/chunks/{offset:020}",
            Self::upload_key(upload_id)?
        ))
    }

    fn staging_path(&self, upload_id: &str) -> ResourceResult<Path> {
        self.key(&format!("staging/{}", Self::upload_key(upload_id)?))
    }

    fn catalog_path(&self, namespace: &str, key: &str) -> ResourceResult<Path> {
        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0]);
        hasher.update(key.as_bytes());
        self.key(&format!("catalog/{}.json", hex_digest(&hasher.finalize())))
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
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        let record: UploadRecord = cymule_core::decode_json(&bytes).map_err(substrate)?;
        if record.upload_id != upload_id {
            return Err(ResourceError::Integrity(
                "object-store upload record identity changed".to_owned(),
            ));
        }
        if record.record_version != UPLOAD_RECORD_VERSION {
            return Err(ResourceError::Integrity(format!(
                "unsupported object-store upload record version {}",
                record.record_version
            )));
        }
        Ok((record, version))
    }

    fn put_record(&self, record: &UploadRecord, mode: PutMode) -> ResourceResult<UpdateVersion> {
        let path = self.record_path(&record.upload_id)?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
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
        Ok(result.into())
    }

    fn resource_path(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<Path> {
        locators.verify_for(resource)?;
        if locators.resolver_binding != self.binding {
            return Err(ResourceError::NotFound(format!(
                "Resource {} has no object-store location for {}",
                resource.resource_id, self.binding
            )));
        }
        let reference = locators
            .locations
            .iter()
            .find_map(|location| match location {
                ResourceLocation::Opaque { reference } => Some(reference.as_str()),
                ResourceLocation::PublicUrl { .. } => None,
            })
            .ok_or_else(|| {
                ResourceError::NotFound(format!(
                    "Resource {} has no object-store location for {}",
                    resource.resource_id, self.binding
                ))
            })?;
        let digest = reference.strip_prefix("sha256:").ok_or_else(|| {
            ResourceError::Validation("object-store reference is not SHA-256".to_owned())
        })?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ResourceError::Validation(
                "object-store digest reference is malformed".to_owned(),
            ));
        }
        self.key(&format!("objects/{digest}"))
    }

    fn chunk_bytes(&self, upload_id: &str, chunk: &ChunkRecord) -> ResourceResult<Vec<u8>> {
        let path = self.chunk_path(upload_id, chunk.offset)?;
        let store = Arc::clone(&self.store);
        let result = self
            .block_on(async move { store.get(&path).await })?
            .map_err(object_error)?;
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        if bytes.len() != chunk.size {
            return Err(ResourceError::Integrity(
                "object-store chunk size changed".to_owned(),
            ));
        }
        if format!("sha256:{}", hex_digest(&Sha256::digest(&bytes))) != chunk.digest {
            return Err(ResourceError::Integrity(
                "object-store chunk digest changed".to_owned(),
            ));
        }
        Ok(bytes.to_vec())
    }

    fn verify_object(&self, path: &Path, expected: &str, size: u64) -> ResourceResult<()> {
        let store = Arc::clone(&self.store);
        let path = path.clone();
        let observed = self.block_on(async move {
            let result = store.get(&path).await.map_err(object_error)?;
            let mut stream = result.into_stream();
            let mut hasher = Sha256::new();
            let mut observed_size = 0_u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(object_error)?;
                hasher.update(&chunk);
                observed_size = observed_size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| ResourceError::Integrity("object size overflow".to_owned()))?;
            }
            Ok::<_, ResourceError>((hex_digest(&hasher.finalize()), observed_size))
        })??;
        if observed.0 != expected || observed.1 != size {
            return Err(ResourceError::Integrity(
                "published object bytes do not match their digest".to_owned(),
            ));
        }
        Ok(())
    }

    fn delete_if_present(&self, path: &Path) -> ResourceResult<bool> {
        let present = !self.is_absent(path)?;
        if !present {
            return Ok(false);
        }
        let store = Arc::clone(&self.store);
        let path = path.clone();
        match self.block_on(async move { store.delete(&path).await })? {
            Ok(()) => Ok(true),
            Err(ObjectError::NotFound { .. }) => Ok(false),
            Err(error) => Err(object_error(error)),
        }
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
        chunks: &[ChunkRecord],
    ) -> ResourceResult<ResourceCleanupReceipt> {
        let staging = self.staging_path(&session.upload_id)?;
        let removed_staging_objects = u64::from(self.delete_if_present(&staging)?);
        let mut removed_chunks = 0_u64;
        for chunk in chunks {
            let path = self.chunk_path(&session.upload_id, chunk.offset)?;
            removed_chunks += u64::from(self.delete_if_present(&path)?);
        }
        let mut verified_absent = self.is_absent(&staging)?;
        for chunk in chunks {
            verified_absent &=
                self.is_absent(&self.chunk_path(&session.upload_id, chunk.offset)?)?;
        }
        let receipt = ResourceCleanupReceipt {
            receipt_version: RESOURCE_CLEANUP_RECEIPT_VERSION.to_owned(),
            write_id: session.write_id.clone(),
            upload_id: session.upload_id.clone(),
            store_binding: self.binding.clone(),
            removed_staging_objects,
            removed_chunks,
            verified_absent,
        };
        receipt.verify()?;
        Ok(receipt)
    }
}

impl ResourceCatalogStore for ObjectResourceStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        record.verify()?;
        let path = self.catalog_path(&record.namespace, &record.key)?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
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
            Err(ObjectError::AlreadyExists { .. }) | Err(ObjectError::Precondition { .. }) => {
                let existing = self
                    .get_catalog_record(&record.namespace, &record.key)?
                    .ok_or_else(|| {
                        ResourceError::Conflict(
                            "object-store catalog create conflicted without retained content"
                                .to_owned(),
                        )
                    })?;
                if existing == *record {
                    Ok(())
                } else {
                    Err(ResourceError::Conflict(format!(
                        "object-store catalog record {}/{} has conflicting content",
                        record.namespace, record.key
                    )))
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
        let bytes = self
            .block_on(async move { result.bytes().await })?
            .map_err(object_error)?;
        let record: ResourceCatalogRecord = cymule_core::decode_json(&bytes).map_err(substrate)?;
        record.verify()?;
        if record.namespace != namespace || record.key != key {
            return Err(ResourceError::Integrity(
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
        let upload_id = Self::upload_id(&intent.write_id);
        let record = UploadRecord {
            record_version: UPLOAD_RECORD_VERSION.to_owned(),
            intent: intent.clone(),
            upload_id: upload_id.clone(),
            next_offset: 0,
            chunks: Vec::new(),
            state: UploadState::Open,
            publication: None,
        };
        match self.put_record(&record, PutMode::Create) {
            Ok(_) => {}
            Err(ResourceError::Conflict(_)) => {
                let (existing, _) = self.load_record(&upload_id)?;
                if existing.intent != *intent || existing.state == UploadState::Aborted {
                    return Err(ResourceError::Conflict(format!(
                        "object-store write ID {} was reused",
                        intent.write_id
                    )));
                }
            }
            Err(error) => return Err(error),
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
        let (mut record, mut version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id || record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "object-store upload is not open for this write".to_owned(),
            ));
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ResourceError::Integrity("object size overflow".to_owned()))?;
        if offset < record.next_offset {
            let chunk = record
                .chunks
                .iter()
                .find(|chunk| {
                    chunk.offset == offset
                        && chunk.size == bytes.len()
                        && chunk.state == ChunkState::Acknowledged
                })
                .ok_or_else(|| {
                    ResourceError::Conflict(
                        "object-store chunk overlaps the retained frontier".to_owned(),
                    )
                })?;
            return if self.chunk_bytes(&session.upload_id, chunk)? == bytes {
                Ok(())
            } else {
                Err(ResourceError::Conflict(
                    "object-store chunk retry changed retained bytes".to_owned(),
                ))
            };
        }
        if offset != record.next_offset {
            return Err(ResourceError::Conflict(format!(
                "object-store upload expected offset {}, received {offset}",
                record.next_offset
            )));
        }
        let digest = format!("sha256:{}", hex_digest(&Sha256::digest(bytes)));
        let chunk_index = if let Some((index, planned)) = record
            .chunks
            .iter()
            .enumerate()
            .find(|(_, chunk)| chunk.offset == offset)
        {
            if planned.size != bytes.len() || planned.digest != digest {
                return Err(ResourceError::Conflict(
                    "object-store planned chunk changed bytes".to_owned(),
                ));
            }
            index
        } else {
            record.chunks.push(ChunkRecord {
                offset,
                size: bytes.len(),
                digest: digest.clone(),
                state: ChunkState::Planned,
            });
            version = self.put_record(&record, PutMode::Update(version))?;
            record.chunks.len() - 1
        };
        let path = self.chunk_path(&session.upload_id, offset)?;
        let payload = bytes.to_vec();
        let store = Arc::clone(&self.store);
        match self.block_on(async move {
            store
                .put_opts(
                    &path,
                    payload.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await
        })? {
            Ok(_) | Err(ObjectError::AlreadyExists { .. }) => {}
            Err(error) => return Err(object_error(error)),
        }
        if self.chunk_bytes(&session.upload_id, &record.chunks[chunk_index])? != bytes {
            return Err(ResourceError::Conflict(
                "object-store chunk identity retained different bytes".to_owned(),
            ));
        }
        record.next_offset = end;
        record.chunks[chunk_index].state = ChunkState::Acknowledged;
        match self.put_record(&record, PutMode::Update(version)) {
            Ok(_) => Ok(()),
            Err(ResourceError::Conflict(_)) => {
                let (reopened, _) = self.load_record(&session.upload_id)?;
                if reopened.next_offset >= end
                    && reopened.chunks.iter().any(|chunk| {
                        chunk.offset == offset
                            && chunk.size == bytes.len()
                            && chunk.digest == digest
                            && chunk.state == ChunkState::Acknowledged
                    })
                {
                    Ok(())
                } else {
                    Err(ResourceError::Conflict(
                        "object-store upload frontier changed".to_owned(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.validate_session(session)?;
        let (mut record, mut version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(ResourceError::Conflict(
                "object-store commit identity changed".to_owned(),
            ));
        }
        if let Some(publication) = &record.publication {
            publication.verify()?;
            let destination = self.resource_path(&publication.resource, &publication.locators)?;
            if record.state == UploadState::Publishing && self.is_absent(&destination)? {
                let staging = self.staging_path(&session.upload_id)?;
                if self.is_absent(&staging)? {
                    return Err(ResourceError::Integrity(
                        "object-store publishing inventory lost both staging and destination"
                            .to_owned(),
                    ));
                }
                let store = Arc::clone(&self.store);
                let from = staging;
                let to = destination.clone();
                match self.block_on(async move { store.copy_if_not_exists(&from, &to).await })? {
                    Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
                    Err(error) => return Err(object_error(error)),
                }
            }
            self.verify_object(
                &destination,
                publication
                    .resource
                    .integrity
                    .content_digest()
                    .ok_or_else(|| {
                        ResourceError::Integrity(
                            "object-store publication is not content addressed".to_owned(),
                        )
                    })?
                    .strip_prefix("sha256:")
                    .expect("validated digest"),
                publication
                    .resource
                    .integrity
                    .content_size()
                    .ok_or_else(|| {
                        ResourceError::Integrity(
                            "object-store publication is not content addressed".to_owned(),
                        )
                    })?,
            )?;
            if record.state == UploadState::Publishing {
                record.state = UploadState::Committed;
                self.put_record(&record, PutMode::Update(version))?;
            } else if record.state != UploadState::Committed {
                return Err(ResourceError::Integrity(
                    "object-store publication exists outside publishing state".to_owned(),
                ));
            }
            self.cleanup_upload(session, &record.chunks)?;
            return Ok(publication.clone());
        }
        if record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "object-store upload cannot commit from its current state".to_owned(),
            ));
        }
        if record
            .chunks
            .iter()
            .any(|chunk| chunk.state != ChunkState::Acknowledged)
        {
            return Err(ResourceError::Conflict(
                "object-store upload has an unacknowledged planned chunk".to_owned(),
            ));
        }
        let mut hasher = Sha256::new();
        let mut observed_size = 0_u64;
        let staging = self.staging_path(&session.upload_id)?;
        let mut part = Vec::with_capacity(PART_SIZE);
        if record.chunks.is_empty() {
            let store = Arc::clone(&self.store);
            let path = staging.clone();
            self.block_on(async move { store.put(&path, Vec::<u8>::new().into()).await })?
                .map_err(object_error)?;
        } else {
            let store = Arc::clone(&self.store);
            let staging_for_upload = staging.clone();
            let mut multipart = self
                .block_on(async move { store.put_multipart(&staging_for_upload).await })?
                .map_err(object_error)?;
            for chunk in &record.chunks {
                let bytes = self.chunk_bytes(&session.upload_id, chunk)?;
                hasher.update(&bytes);
                observed_size = observed_size
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| ResourceError::Integrity("object size overflow".to_owned()))?;
                part.extend_from_slice(&bytes);
                while part.len() >= PART_SIZE {
                    let remainder = part.split_off(PART_SIZE);
                    let payload = std::mem::replace(&mut part, remainder);
                    self.block_on(multipart.put_part(payload.into()))?
                        .map_err(object_error)?;
                }
            }
            if !part.is_empty() {
                self.block_on(multipart.put_part(part.into()))?
                    .map_err(object_error)?;
            }
            self.block_on(multipart.complete())?.map_err(object_error)?;
        }
        let hex = hex_digest(&hasher.finalize());
        let destination = self.key(&format!("objects/{hex}"))?;
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
        record.state = UploadState::Publishing;
        record.publication = Some(publication.clone());
        version = self.put_record(&record, PutMode::Update(version))?;
        let store = Arc::clone(&self.store);
        let from = staging.clone();
        let to = destination.clone();
        match self.block_on(async move { store.copy_if_not_exists(&from, &to).await })? {
            Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
            Err(error) => return Err(object_error(error)),
        }
        self.verify_object(&destination, &hex, observed_size)?;
        record.state = UploadState::Committed;
        self.put_record(&record, PutMode::Update(version))?;
        self.cleanup_upload(session, &record.chunks)?;
        Ok(publication)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.validate_session(session)?;
        let (mut record, version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(ResourceError::Conflict(
                "object-store abort identity changed".to_owned(),
            ));
        }
        if matches!(
            record.state,
            UploadState::Publishing | UploadState::Committed
        ) {
            if record.state == UploadState::Publishing {
                let _ = self.commit_write(session)?;
                record = self.load_record(&session.upload_id)?.0;
            }
            return self.cleanup_upload(session, &record.chunks);
        }
        record.state = UploadState::Aborted;
        self.put_record(&record, PutMode::Update(version))?;
        self.cleanup_upload(session, &record.chunks)
    }
}

impl ArtifactResolver for ObjectResourceStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        let path = self.resource_path(resource, locators)?;
        let store = Arc::clone(&self.store);
        let metadata = self
            .block_on(async move { store.head(&path).await })?
            .map_err(object_error)?;
        if let ResourceIntegrity::Content { size, .. } = &resource.integrity
            && metadata.size != *size
        {
            return Err(ResourceError::Integrity(
                "object-store object size changed".to_owned(),
            ));
        }
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
        self.stat(resource, locators)?;
        let ResourceIntegrity::Content { size, .. } = &resource.integrity else {
            return Err(ResourceError::Validation(
                "object-store reads require content integrity".to_owned(),
            ));
        };
        let size = *size;
        if max_bytes == 0 || offset > size {
            return Err(ResourceError::Validation(
                "object-store read range is invalid".to_owned(),
            ));
        }
        let end = offset.saturating_add(u64::from(max_bytes)).min(size);
        let path = self.resource_path(resource, locators)?;
        let store = Arc::clone(&self.store);
        let bytes = self
            .block_on(async move { store.get_range(&path, offset..end).await })?
            .map_err(object_error)?;
        Ok(ResourceChunk {
            offset,
            bytes: bytes.to_vec(),
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

    fn delete_resource(
        &mut self,
        intent: &ResourceDeleteIntent,
    ) -> ResourceResult<ResourceDeletionObservation> {
        intent.verify()?;
        if intent.store_binding != self.binding {
            return Err(ResourceError::Conflict(
                "object-store deleter does not own the durable delete intent".to_owned(),
            ));
        }
        let path =
            self.resource_path(&intent.publication.resource, &intent.publication.locators)?;
        let removed_bytes = {
            let store = Arc::clone(&self.store);
            let probe = path.clone();
            match self.block_on(async move { store.head(&probe).await })? {
                Ok(metadata) => metadata.size,
                Err(ObjectError::NotFound { .. }) => 0,
                Err(error) => return Err(object_error(error)),
            }
        };
        let _ = self.delete_if_present(&path)?;
        Ok(ResourceDeletionObservation {
            removed_bytes,
            verified_absent: self.is_absent(&path)?,
        })
    }
}

fn object_error(error: ObjectError) -> ResourceError {
    let kind = match &error {
        ObjectError::AlreadyExists { .. } | ObjectError::Precondition { .. } => 0,
        ObjectError::NotFound { .. } => 1,
        _ => 2,
    };
    let message = error.to_string();
    drop(error);
    match kind {
        0 => ResourceError::Conflict(message),
        1 => ResourceError::NotFound(message),
        _ => ResourceError::Substrate(message),
    }
}

fn core_error(error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Integrity(error.to_string())
}

fn substrate(error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Substrate(error.to_string())
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}
