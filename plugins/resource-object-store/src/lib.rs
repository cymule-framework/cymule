//! Apache `object_store` Resource adapter for Cymule.

use std::future::Future;
use std::sync::Arc;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_WRITE_CHUNK, ResourceCandidate, ResourceChunk,
    ResourceError, ResourceHandle, ResourceIntegrity, ResourceLocation, ResourceObservation,
    ResourcePage, ResourceResult, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
};
use futures_util::StreamExt;
use object_store::path::Path;
use object_store::{
    Error as ObjectError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};

const BINDING_VERSION: &str = "cymule.resource-object-store/1";
const PART_SIZE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UploadState {
    Open,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRecord {
    offset: u64,
    size: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    intent: ResourceWriteIntent,
    upload_id: String,
    next_offset: u64,
    chunks: Vec<ChunkRecord>,
    state: UploadState,
    handle: Option<ResourceHandle>,
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
        upload_id.strip_prefix("upload:").ok_or_else(|| {
            ResourceError::Validation("invalid object-store upload identity".to_owned())
        })
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
        let record: UploadRecord = serde_json::from_slice(&bytes).map_err(substrate)?;
        if record.upload_id != upload_id {
            return Err(ResourceError::Integrity(
                "object-store upload record identity changed".to_owned(),
            ));
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

    fn resource_path(&self, resource: &ResourceHandle) -> ResourceResult<Path> {
        let reference = resource
            .locations
            .iter()
            .find_map(|location| match location {
                ResourceLocation::Resolver { binding, reference } if binding == &self.binding => {
                    Some(reference.as_str())
                }
                _ => None,
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
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
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
            intent: intent.clone(),
            upload_id: upload_id.clone(),
            next_offset: 0,
            chunks: Vec::new(),
            state: UploadState::Open,
            handle: None,
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
        if session.store_binding != self.binding
            || bytes.is_empty()
            || bytes.len() > MAX_WRITE_CHUNK
        {
            return Err(ResourceError::Validation(
                "object-store session or chunk is invalid".to_owned(),
            ));
        }
        let (mut record, version) = self.load_record(&session.upload_id)?;
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
                .find(|chunk| chunk.offset == offset && chunk.size == bytes.len())
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
        let chunk = ChunkRecord {
            offset,
            size: bytes.len(),
        };
        if self.chunk_bytes(&session.upload_id, &chunk)? != bytes {
            return Err(ResourceError::Conflict(
                "object-store chunk identity retained different bytes".to_owned(),
            ));
        }
        record.next_offset = end;
        record.chunks.push(chunk);
        match self.put_record(&record, PutMode::Update(version)) {
            Ok(_) => Ok(()),
            Err(ResourceError::Conflict(_)) => {
                let (reopened, _) = self.load_record(&session.upload_id)?;
                if reopened.next_offset >= end
                    && reopened
                        .chunks
                        .iter()
                        .any(|chunk| chunk.offset == offset && chunk.size == bytes.len())
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

    fn commit_write(&mut self, session: &ResourceWriteSession) -> ResourceResult<ResourceHandle> {
        let (mut record, version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id || session.store_binding != self.binding {
            return Err(ResourceError::Conflict(
                "object-store commit identity changed".to_owned(),
            ));
        }
        if let Some(handle) = &record.handle {
            handle.verify()?;
            return Ok(handle.clone());
        }
        if record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "object-store upload cannot commit from its current state".to_owned(),
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
        let store = Arc::clone(&self.store);
        let from = staging.clone();
        let to = destination.clone();
        match self.block_on(async move { store.copy_if_not_exists(&from, &to).await })? {
            Ok(()) | Err(ObjectError::AlreadyExists { .. }) => {}
            Err(error) => return Err(object_error(error)),
        }
        self.verify_object(&destination, &hex, observed_size)?;
        let handle = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{hex}"),
                size: observed_size,
            },
            locations: vec![ResourceLocation::Resolver {
                binding: self.binding.clone(),
                reference: format!("sha256:{hex}"),
            }],
            annotations: record.intent.annotations.clone(),
        }
        .seal()?;
        record.state = UploadState::Committed;
        record.handle = Some(handle.clone());
        match self.put_record(&record, PutMode::Update(version)) {
            Ok(_) => {}
            Err(ResourceError::Conflict(_)) => {
                let (reopened, _) = self.load_record(&session.upload_id)?;
                if reopened.handle.as_ref() != Some(&handle) {
                    return Err(ResourceError::Conflict(
                        "object-store commit receipt changed".to_owned(),
                    ));
                }
            }
            Err(error) => return Err(error),
        }
        let store = Arc::clone(&self.store);
        let _ = self.block_on(async move { store.delete(&staging).await });
        Ok(handle)
    }

    fn abort_write(&mut self, session: &ResourceWriteSession) -> ResourceResult<()> {
        let (mut record, version) = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id || session.store_binding != self.binding {
            return Err(ResourceError::Conflict(
                "object-store abort identity changed".to_owned(),
            ));
        }
        if record.state == UploadState::Committed {
            return Ok(());
        }
        record.state = UploadState::Aborted;
        self.put_record(&record, PutMode::Update(version))?;
        Ok(())
    }
}

impl ArtifactResolver for ObjectResourceStore {
    fn stat(&mut self, resource: &ResourceHandle) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        let path = self.resource_path(resource)?;
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
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk> {
        self.stat(resource)?;
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
        let path = self.resource_path(resource)?;
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
        _cursor: Option<&str>,
        _limit: u32,
    ) -> ResourceResult<ResourcePage> {
        Err(ResourceError::Validation(
            "initial object-store plugin supports object Resources only".to_owned(),
        ))
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
