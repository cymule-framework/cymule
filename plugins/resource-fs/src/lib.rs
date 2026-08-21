//! Filesystem resource store and resolver for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_WRITE_CHUNK, RESOURCE_CLEANUP_RECEIPT_VERSION,
    RESOURCE_LOCATOR_VERSION, ResourceCandidate, ResourceCatalogRecord, ResourceCatalogStore,
    ResourceChunk, ResourceCleanupReceipt, ResourceDeleteIntent, ResourceDeleter,
    ResourceDeletionObservation, ResourceEntry, ResourceError, ResourceHandle, ResourceIntegrity,
    ResourceLocation, ResourceLocatorSet, ResourceManifestEntry, ResourceObservation, ResourcePage,
    ResourcePublication, ResourceResult, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
    SealedResourceManifest,
};
use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const BINDING_VERSION: &str = "cymule.resource-fs/2";
const DIRECTORY_MEDIA_TYPE: &str = cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE;
const UPLOAD_RECORD_VERSION: &str = "cymule.resource-fs-upload/3";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UploadState {
    Open,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    record_version: String,
    intent: ResourceWriteIntent,
    upload_id: String,
    state: UploadState,
    committed_length: u64,
    publication: Option<ResourcePublication>,
}

/// Content-addressed local resource adapter.
#[derive(Debug, Clone)]
pub struct FsResourceStore {
    root: PathBuf,
    binding: String,
}

impl FsResourceStore {
    /// Open or create one filesystem resource namespace.
    pub fn open(root: impl AsRef<Path>, binding: impl Into<String>) -> ResourceResult<Self> {
        let root = root.as_ref().to_path_buf();
        let binding = binding.into();
        if binding.is_empty() || binding.chars().any(char::is_control) {
            return Err(ResourceError::Validation(
                "filesystem resource binding must be non-empty and printable".to_owned(),
            ));
        }
        for child in ["uploads", "objects", "catalog", "locks", "staging"] {
            fs::create_dir_all(root.join(child)).map_err(substrate)?;
        }
        Ok(Self { root, binding })
    }

    /// Immutable adapter binding retained in Resource locations.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Rebuild this adapter's replaceable locator set for one semantic Handle.
    pub fn publication(&self, resource: &ResourceHandle) -> ResourceResult<ResourcePublication> {
        resource.verify()?;
        let digest = resource.integrity.content_digest().ok_or_else(|| {
            ResourceError::Validation(
                "filesystem publication requires content-addressed integrity".to_owned(),
            )
        })?;
        let publication = ResourcePublication {
            resource: resource.clone(),
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: self.binding.clone(),
                locations: vec![ResourceLocation::Opaque {
                    reference: digest.to_owned(),
                }],
            },
        };
        publication.verify()?;
        Ok(publication)
    }

    /// Encode a sorted, validated directory manifest for a chunked write.
    pub fn encode_manifest(entries: &[ResourceManifestEntry]) -> ResourceResult<Vec<u8>> {
        Ok(SealedResourceManifest::seal(entries.to_vec())?.bytes)
    }

    /// Import one file as a content-addressed object Resource.
    pub fn import_file(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
        media_type: impl Into<String>,
    ) -> ResourceResult<ResourcePublication> {
        let intent = ResourceWriteIntent {
            write_id: write_id.into(),
            shape: ResourceShape::Object,
            media_type: media_type.into(),
            annotations: BTreeMap::new(),
        };
        let session = self.begin_write(&intent)?;
        if let Some(publication) = self.load_record(&session.upload_id)?.publication {
            self.verify_import_replay(path.as_ref(), &publication)?;
            return Ok(publication);
        }
        let mut file = File::open(path).map_err(substrate)?;
        let mut offset = 0_u64;
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let count = file.read(&mut buffer).map_err(substrate)?;
            if count == 0 {
                break;
            }
            self.write_chunk(&session, offset, &buffer[..count])?;
            offset = offset
                .checked_add(count as u64)
                .ok_or_else(|| ResourceError::Integrity("file size overflow".to_owned()))?;
        }
        let handle = self.commit_write(&session)?;
        if !matches!(
            handle.resource.integrity,
            ResourceIntegrity::Content { size, .. } if size == offset
        ) {
            return Err(ResourceError::Conflict(
                "filesystem import length changed committed bytes".to_owned(),
            ));
        }
        Ok(handle)
    }

    fn verify_import_replay(
        &self,
        source: &Path,
        publication: &ResourcePublication,
    ) -> ResourceResult<()> {
        publication.verify()?;
        let object = self.resource_path(&publication.resource, &publication.locators)?;
        let ResourceIntegrity::Content { digest, size } = &publication.resource.integrity else {
            return Err(ResourceError::Integrity(
                "filesystem import publication is not content addressed".to_owned(),
            ));
        };
        verify_content(&object, digest, *size)?;
        let mut source = File::open(source).map_err(substrate)?;
        let mut retained = File::open(object).map_err(substrate)?;
        let mut source_buffer = vec![0_u8; 1024 * 1024];
        let mut retained_buffer = vec![0_u8; 1024 * 1024];
        loop {
            let source_count = source.read(&mut source_buffer).map_err(substrate)?;
            let retained_count = retained.read(&mut retained_buffer).map_err(substrate)?;
            if source_count != retained_count
                || source_buffer[..source_count] != retained_buffer[..retained_count]
            {
                return Err(ResourceError::Conflict(
                    "filesystem import replay changed committed bytes".to_owned(),
                ));
            }
            if source_count == 0 {
                return Ok(());
            }
        }
    }

    /// Import a directory recursively as a manifest Resource.
    pub fn import_directory(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
    ) -> ResourceResult<ResourcePublication> {
        let path = path.as_ref();
        let write_id = write_id.into();
        let mut children = fs::read_dir(path)
            .map_err(substrate)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(substrate)?;
        children.sort_by_key(std::fs::DirEntry::file_name);
        let mut entries = Vec::with_capacity(children.len());
        for child in children {
            let file_type = child.file_type().map_err(substrate)?;
            if file_type.is_symlink() {
                return Err(ResourceError::Validation(format!(
                    "filesystem import rejects symlink {}",
                    child.path().display()
                )));
            }
            let name = child.file_name().into_string().map_err(|_| {
                ResourceError::Validation("filesystem names must be UTF-8".to_owned())
            })?;
            validate_name(&name)?;
            let child_write_id = format!("{write_id}/{name}");
            let resource = if file_type.is_dir() {
                self.import_directory(child.path(), child_write_id)?
            } else if file_type.is_file() {
                self.import_file(child.path(), child_write_id, "application/octet-stream")?
            } else {
                return Err(ResourceError::Validation(format!(
                    "unsupported filesystem entry {}",
                    child.path().display()
                )));
            };
            entries.push(ResourceManifestEntry {
                name,
                resource: resource.resource,
            });
        }
        let bytes = Self::encode_manifest(&entries)?;
        let intent = ResourceWriteIntent {
            write_id,
            shape: ResourceShape::Directory,
            media_type: DIRECTORY_MEDIA_TYPE.to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = self.begin_write(&intent)?;
        for (index, chunk) in bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
            self.write_chunk(&session, (index * MAX_WRITE_CHUNK) as u64, chunk)?;
        }
        self.commit_write(&session)
    }

    fn upload_key(upload_id: &str) -> ResourceResult<&str> {
        let key = upload_id.strip_prefix("upload:").ok_or_else(|| {
            ResourceError::Validation("invalid filesystem upload identity".to_owned())
        })?;
        if key.len() != 64
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ResourceError::Validation(
                "filesystem upload identity must be an exact lowercase SHA-256 key".to_owned(),
            ));
        }
        Ok(key)
    }

    fn upload_id(write_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(BINDING_VERSION.as_bytes());
        hasher.update([0]);
        hasher.update(write_id.as_bytes());
        format!("upload:{}", hex_digest(&hasher.finalize()))
    }

    fn validate_session(&self, session: &ResourceWriteSession) -> ResourceResult<()> {
        if session.store_binding != self.binding
            || session.upload_id != Self::upload_id(&session.write_id)
        {
            return Err(ResourceError::Conflict(
                "filesystem upload session is not authenticated by its write ID and binding"
                    .to_owned(),
            ));
        }
        let _ = Self::upload_key(&session.upload_id)?;
        Ok(())
    }

    fn record_path(&self, upload_id: &str) -> ResourceResult<PathBuf> {
        Ok(self
            .root
            .join("uploads")
            .join(format!("{}.json", Self::upload_key(upload_id)?)))
    }

    fn data_path(&self, upload_id: &str) -> ResourceResult<PathBuf> {
        Ok(self
            .root
            .join("uploads")
            .join(format!("{}.data", Self::upload_key(upload_id)?)))
    }

    fn claim(&self, upload_id: &str) -> ResourceResult<File> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(
                self.root
                    .join("locks")
                    .join(format!("{}.lock", Self::upload_key(upload_id)?)),
            )
            .map_err(substrate)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(ResourceError::Conflict(format!(
                "filesystem upload {upload_id} has an active writer"
            ))),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn catalog_token(namespace: &str, key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0]);
        hasher.update(key.as_bytes());
        hex_digest(&hasher.finalize())
    }

    fn catalog_path(&self, namespace: &str, key: &str) -> PathBuf {
        self.root
            .join("catalog")
            .join(format!("{}.json", Self::catalog_token(namespace, key)))
    }

    fn claim_catalog(&self, namespace: &str, key: &str) -> ResourceResult<File> {
        let token = Self::catalog_token(namespace, key);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(
                self.root
                    .join("locks")
                    .join(format!("catalog-{token}.lock")),
            )
            .map_err(substrate)?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(ResourceError::Conflict(format!(
                "filesystem catalog record {namespace}/{key} has an active writer"
            ))),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn load_record(&self, upload_id: &str) -> ResourceResult<UploadRecord> {
        let record: UploadRecord =
            cymule_core::decode_json(&fs::read(self.record_path(upload_id)?).map_err(substrate)?)
                .map_err(substrate)?;
        if record.upload_id != upload_id {
            return Err(ResourceError::Integrity(
                "filesystem upload record identity changed".to_owned(),
            ));
        }
        if record.record_version != UPLOAD_RECORD_VERSION {
            return Err(ResourceError::Integrity(format!(
                "unsupported filesystem upload record version {}",
                record.record_version
            )));
        }
        Ok(record)
    }

    fn store_record(&self, record: &UploadRecord) -> ResourceResult<()> {
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        let destination = self.record_path(&record.upload_id)?;
        let staging = self
            .root
            .join("staging")
            .join(format!("record-{}", Self::upload_key(&record.upload_id)?));
        write_synced(&staging, &bytes)?;
        fs::rename(staging, destination).map_err(substrate)?;
        sync_directory(&self.root.join("uploads"))?;
        sync_directory(&self.root.join("staging"))
    }

    fn cleanup_upload_files(
        &self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        let data = self.data_path(&session.upload_id)?;
        let staging = self
            .root
            .join("staging")
            .join(format!("object-{}", Self::upload_key(&session.upload_id)?));
        let mut removed_staging_objects = 0_u64;
        for path in [&data, &staging] {
            if path.exists() {
                remove_if_exists(path)?;
                removed_staging_objects += 1;
            }
        }
        sync_directory(&self.root.join("uploads"))?;
        sync_directory(&self.root.join("staging"))?;
        let receipt = ResourceCleanupReceipt {
            receipt_version: RESOURCE_CLEANUP_RECEIPT_VERSION.to_owned(),
            write_id: session.write_id.clone(),
            upload_id: session.upload_id.clone(),
            store_binding: self.binding.clone(),
            removed_staging_objects,
            removed_chunks: 0,
            verified_absent: !data.exists() && !staging.exists(),
        };
        receipt.verify()?;
        Ok(receipt)
    }

    fn resource_path(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<PathBuf> {
        locators.verify_for(resource)?;
        if locators.resolver_binding != self.binding {
            return Err(ResourceError::NotFound(format!(
                "Resource {} has no location for binding {}",
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
                    "Resource {} has no location for binding {}",
                    resource.resource_id, self.binding
                ))
            })?;
        let digest = reference.strip_prefix("sha256:").ok_or_else(|| {
            ResourceError::Validation("filesystem reference is not a SHA-256 digest".to_owned())
        })?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ResourceError::Validation(
                "filesystem reference digest is malformed".to_owned(),
            ));
        }
        Ok(self.root.join("objects").join(digest))
    }
}

impl ResourceCatalogStore for FsResourceStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        record.verify()?;
        let _claim = self.claim_catalog(&record.namespace, &record.key)?;
        let destination = self.catalog_path(&record.namespace, &record.key);
        if destination.exists() {
            let existing: ResourceCatalogRecord =
                cymule_core::decode_json(&fs::read(&destination).map_err(substrate)?)
                    .map_err(substrate)?;
            existing.verify()?;
            return if existing == *record {
                Ok(())
            } else {
                Err(ResourceError::Conflict(format!(
                    "filesystem catalog record {}/{} has conflicting content",
                    record.namespace, record.key
                )))
            };
        }
        let token = Self::catalog_token(&record.namespace, &record.key);
        let staging = self
            .root
            .join("staging")
            .join(format!("catalog-{token}.next"));
        write_synced(
            &staging,
            &cymule_core::canonical_bytes(record).map_err(core_error)?,
        )?;
        fs::rename(staging, destination).map_err(substrate)?;
        sync_directory(&self.root.join("catalog"))?;
        sync_directory(&self.root)?;
        Ok(())
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        let _ = ResourceCatalogRecord::new(namespace, key, Vec::new())?;
        let path = self.catalog_path(namespace, key);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(substrate(error)),
        };
        let record: ResourceCatalogRecord = cymule_core::decode_json(&bytes).map_err(substrate)?;
        record.verify()?;
        if record.namespace != namespace || record.key != key {
            return Err(ResourceError::Integrity(
                "filesystem catalog locator does not match its record identity".to_owned(),
            ));
        }
        Ok(Some(record))
    }
}

impl ArtifactStore for FsResourceStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> ResourceResult<ResourceWriteSession> {
        intent.validate()?;
        let upload_id = Self::upload_id(&intent.write_id);
        let _claim = self.claim(&upload_id)?;
        let path = self.record_path(&upload_id)?;
        if path.exists() {
            let record = self.load_record(&upload_id)?;
            if record.intent != *intent || record.state == UploadState::Aborted {
                return Err(ResourceError::Conflict(format!(
                    "filesystem write ID {} was reused",
                    intent.write_id
                )));
            }
        } else {
            self.store_record(&UploadRecord {
                record_version: UPLOAD_RECORD_VERSION.to_owned(),
                intent: intent.clone(),
                upload_id: upload_id.clone(),
                state: UploadState::Open,
                committed_length: 0,
                publication: None,
            })?;
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
                "filesystem write session or chunk is invalid".to_owned(),
            ));
        }
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.write_id() != session.write_id {
            return Err(ResourceError::Conflict(
                "filesystem upload identity changed".to_owned(),
            ));
        }
        if record.state == UploadState::Committed {
            let publication = record.publication.as_ref().ok_or_else(|| {
                ResourceError::Integrity(
                    "committed filesystem upload has no Resource publication".to_owned(),
                )
            })?;
            publication.verify()?;
            let path = self.resource_path(&publication.resource, &publication.locators)?;
            let ResourceIntegrity::Content { digest, size } = &publication.resource.integrity
            else {
                return Err(ResourceError::Integrity(
                    "filesystem Resource is not content verified".to_owned(),
                ));
            };
            verify_content(&path, digest, *size)?;
            let end = offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| ResourceError::Integrity("resource size overflow".to_owned()))?;
            if end > *size {
                return Err(ResourceError::Conflict(
                    "filesystem write retry exceeds committed bytes".to_owned(),
                ));
            }
            let mut file = File::open(path).map_err(substrate)?;
            file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
            let mut retained = vec![0_u8; bytes.len()];
            file.read_exact(&mut retained).map_err(substrate)?;
            return if retained == bytes {
                Ok(())
            } else {
                Err(ResourceError::Conflict(
                    "filesystem write retry changed committed bytes".to_owned(),
                ))
            };
        }
        if record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "filesystem upload is not open for this write".to_owned(),
            ));
        }
        let path = self.data_path(&session.upload_id)?;
        let created = !path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(substrate)?;
        let mut length = file.metadata().map_err(substrate)?.len();
        if length < record.committed_length {
            return Err(ResourceError::Integrity(format!(
                "filesystem upload lost acknowledged bytes: retained {length}, committed {}",
                record.committed_length
            )));
        }
        if length > record.committed_length {
            file.set_len(record.committed_length).map_err(substrate)?;
            file.sync_all().map_err(substrate)?;
            length = record.committed_length;
        }
        if created {
            sync_directory(&self.root.join("uploads"))?;
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ResourceError::Integrity("resource size overflow".to_owned()))?;
        if offset < length {
            if end > length {
                return Err(ResourceError::Conflict(
                    "filesystem chunk overlaps the retained frontier".to_owned(),
                ));
            }
            file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
            let mut retained = vec![0_u8; bytes.len()];
            file.read_exact(&mut retained).map_err(substrate)?;
            return if retained == bytes {
                Ok(())
            } else {
                Err(ResourceError::Conflict(
                    "filesystem chunk retry changed retained bytes".to_owned(),
                ))
            };
        }
        if offset != length {
            return Err(ResourceError::Conflict(format!(
                "filesystem upload expected offset {length}, received {offset}"
            )));
        }
        file.seek(SeekFrom::End(0)).map_err(substrate)?;
        file.write_all(bytes).map_err(substrate)?;
        file.sync_all().map_err(substrate)?;
        record.committed_length = end;
        self.store_record(&record)
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.validate_session(session)?;
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(ResourceError::Conflict(
                "filesystem write identity changed".to_owned(),
            ));
        }
        if let Some(publication) = &record.publication {
            publication.verify()?;
            let publication = publication.clone();
            self.stat(&publication.resource, &publication.locators)?;
            self.cleanup_upload_files(session)?;
            return Ok(publication);
        }
        if record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "filesystem upload cannot commit from its current state".to_owned(),
            ));
        }
        let data_path = self.data_path(&session.upload_id)?;
        if !data_path.exists() {
            write_synced(&data_path, &[])?;
            sync_directory(&self.root.join("uploads"))?;
        }
        let data_length = fs::metadata(&data_path).map_err(substrate)?.len();
        if data_length < record.committed_length {
            return Err(ResourceError::Integrity(format!(
                "filesystem upload lost acknowledged bytes: retained {data_length}, committed {}",
                record.committed_length
            )));
        }
        if data_length > record.committed_length {
            let data = OpenOptions::new()
                .write(true)
                .open(&data_path)
                .map_err(substrate)?;
            data.set_len(record.committed_length).map_err(substrate)?;
            data.sync_all().map_err(substrate)?;
        }
        let manifest = if matches!(
            record.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) {
            Some(read_manifest(&data_path)?)
        } else {
            None
        };
        let mut data = File::open(&data_path).map_err(substrate)?;
        let mut hasher = Sha256::new();
        let mut size = 0_u64;
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let count = data.read(&mut buffer).map_err(substrate)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            size = size
                .checked_add(count as u64)
                .ok_or_else(|| ResourceError::Integrity("resource size overflow".to_owned()))?;
        }
        let hex = hex_digest(&hasher.finalize());
        let digest = format!("sha256:{hex}");
        let object = self.root.join("objects").join(&hex);
        let staging = self
            .root
            .join("staging")
            .join(format!("object-{}", Self::upload_key(&session.upload_id)?));
        if object.exists() {
            verify_content(&object, &digest, size)?;
        } else {
            fs::copy(&data_path, &staging).map_err(substrate)?;
            File::open(&staging)
                .and_then(|file| file.sync_all())
                .map_err(substrate)?;
            match fs::hard_link(&staging, &object) {
                Ok(()) => sync_directory(&self.root.join("objects"))?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(substrate(error)),
            }
        }
        verify_content(&object, &digest, size)?;
        sync_directory(&self.root.join("objects"))?;
        remove_if_exists(&staging)?;
        sync_directory(&self.root.join("staging"))?;
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: record.intent.shape,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content { digest, size },
            manifest: manifest.map(|manifest| manifest.descriptor),
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
        record.state = UploadState::Committed;
        record.publication = Some(publication.clone());
        self.store_record(&record)?;
        self.cleanup_upload_files(session)?;
        Ok(publication)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.validate_session(session)?;
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(ResourceError::Conflict(
                "filesystem abort identity changed".to_owned(),
            ));
        }
        if record.state == UploadState::Committed {
            return self.cleanup_upload_files(session);
        }
        record.state = UploadState::Aborted;
        self.store_record(&record)?;
        self.cleanup_upload_files(session)
    }
}

impl ArtifactResolver for FsResourceStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        let path = self.resource_path(resource, locators)?;
        if let ResourceIntegrity::Content { ref digest, size } = resource.integrity {
            verify_content(&path, digest, size)?;
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
        if max_bytes == 0 {
            return Err(ResourceError::Validation(
                "filesystem read limit must be positive".to_owned(),
            ));
        }
        resource.verify()?;
        locators.verify_for(resource)?;
        let path = self.resource_path(resource, locators)?;
        let mut file = File::open(path).map_err(substrate)?;
        let size = file.metadata().map_err(substrate)?.len();
        if !matches!(resource.integrity, ResourceIntegrity::Content { size: expected, .. } if expected == size)
        {
            return Err(ResourceError::Integrity(
                "filesystem object size changed".to_owned(),
            ));
        }
        if offset > size {
            return Err(ResourceError::Validation(
                "filesystem read offset exceeds resource size".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
        let mut bytes = vec![0_u8; max_bytes as usize];
        let count = file.read(&mut bytes).map_err(substrate)?;
        bytes.truncate(count);
        Ok(ResourceChunk {
            offset,
            eof: offset + count as u64 == size,
            bytes,
        })
    }

    fn list(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> ResourceResult<ResourcePage> {
        if !matches!(
            resource.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) || limit == 0
        {
            return Err(ResourceError::Validation(
                "filesystem list requires a collection shape and positive limit".to_owned(),
            ));
        }
        self.stat(resource, locators)?;
        let start_index = cursor
            .map(str::parse::<usize>)
            .transpose()
            .map_err(|error| ResourceError::Validation(error.to_string()))?
            .unwrap_or_default();
        let sealed = read_manifest(&self.resource_path(resource, locators)?)?;
        if resource.manifest.as_ref() != Some(&sealed.descriptor) {
            return Err(ResourceError::Integrity(
                "filesystem manifest descriptor changed".to_owned(),
            ));
        }
        if start_index > sealed.entries().len() {
            return Err(ResourceError::Validation(
                "filesystem manifest cursor exceeds entry count".to_owned(),
            ));
        }
        let end = sealed
            .entries()
            .len()
            .min(start_index.saturating_add(limit as usize));
        let entries: Vec<ResourceEntry> = sealed.entries()[start_index..end].to_vec();
        let next_cursor = (end < sealed.entries().len()).then(|| end.to_string());
        let proof = sealed.proof(
            start_index as u64,
            entries.len(),
            cursor,
            next_cursor.as_deref(),
        )?;
        Ok(ResourcePage {
            entries,
            next_cursor,
            proof,
        })
    }
}

impl ResourceDeleter for FsResourceStore {
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
                "filesystem deleter does not own the durable delete intent".to_owned(),
            ));
        }
        let path =
            self.resource_path(&intent.publication.resource, &intent.publication.locators)?;
        let removed_bytes = match fs::metadata(&path) {
            Ok(metadata) => {
                let size = metadata.len();
                remove_if_exists(&path)?;
                sync_directory(&self.root.join("objects"))?;
                size
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(substrate(error)),
        };
        Ok(ResourceDeletionObservation {
            removed_bytes,
            verified_absent: !path.exists(),
        })
    }
}

impl UploadRecord {
    fn write_id(&self) -> &str {
        &self.intent.write_id
    }
}

fn validate_name(name: &str) -> ResourceResult<()> {
    if name.is_empty()
        || name.starts_with('/')
        || name.contains(['/', '\\'])
        || name == "."
        || name == ".."
        || name.chars().any(char::is_control)
    {
        return Err(ResourceError::Validation(format!(
            "unsafe filesystem manifest name {name:?}"
        )));
    }
    Ok(())
}

fn read_manifest(path: &Path) -> ResourceResult<SealedResourceManifest> {
    let mut reader = BufReader::new(File::open(path).map_err(substrate)?);
    let mut entries = Vec::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(substrate)? == 0 {
            let sealed = SealedResourceManifest::seal(entries)?;
            let retained = fs::read(path).map_err(substrate)?;
            if sealed.bytes != retained {
                return Err(ResourceError::Integrity(
                    "filesystem manifest bytes are not canonical".to_owned(),
                ));
            }
            return Ok(sealed);
        }
        let entry: ResourceManifestEntry =
            cymule_core::decode_json(line.trim_end().as_bytes()).map_err(substrate)?;
        entries.push(entry);
    }
}

fn write_synced(path: &Path, bytes: &[u8]) -> ResourceResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(substrate)?;
    file.write_all(bytes).map_err(substrate)?;
    file.sync_all().map_err(substrate)
}

fn remove_if_exists(path: &Path) -> ResourceResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(substrate(error)),
    }
}

fn verify_content(path: &Path, expected_digest: &str, expected_size: u64) -> ResourceResult<()> {
    let metadata = fs::metadata(path).map_err(substrate)?;
    if metadata.len() != expected_size {
        return Err(ResourceError::Integrity(
            "filesystem object size changed".to_owned(),
        ));
    }
    let mut file = File::open(path).map_err(substrate)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(substrate)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let actual = format!("sha256:{}", hex_digest(&hasher.finalize()));
    if actual != expected_digest {
        return Err(ResourceError::Integrity(format!(
            "filesystem object digest changed: expected {expected_digest}, found {actual}"
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> ResourceResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(substrate)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> ResourceResult<()> {
    Ok(())
}

fn core_error(error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Integrity(error.to_string())
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn substrate(error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Substrate(error.to_string())
}
