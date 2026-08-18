//! Filesystem resource store and resolver for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_WRITE_CHUNK, ResourceCandidate, ResourceChunk,
    ResourceEntry, ResourceError, ResourceHandle, ResourceIntegrity, ResourceLocation,
    ResourceObservation, ResourcePage, ResourceResult, ResourceShape, ResourceWriteIntent,
    ResourceWriteSession,
};
use fs4::{FileExt, TryLockError};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const BINDING_VERSION: &str = "cymule.resource-fs/1";
const DIRECTORY_MEDIA_TYPE: &str = "application/vnd.cymule.directory+jsonl";

/// One entry in a content-addressed directory manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FsManifestEntry {
    /// Safe relative child name.
    pub name: String,
    /// Immutable child Resource Handle.
    pub resource: ResourceHandle,
}

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
    intent: ResourceWriteIntent,
    upload_id: String,
    state: UploadState,
    handle: Option<ResourceHandle>,
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
        for child in ["uploads", "objects", "locks", "staging"] {
            fs::create_dir_all(root.join(child)).map_err(substrate)?;
        }
        Ok(Self { root, binding })
    }

    /// Immutable adapter binding retained in Resource locations.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Encode a sorted, validated directory manifest for a chunked write.
    pub fn encode_manifest(entries: &[FsManifestEntry]) -> ResourceResult<Vec<u8>> {
        let mut previous: Option<&str> = None;
        let mut bytes = Vec::new();
        for entry in entries {
            validate_name(&entry.name)?;
            entry.resource.verify()?;
            if previous.is_some_and(|name| name >= entry.name.as_str()) {
                return Err(ResourceError::Validation(
                    "filesystem manifest entries must be strictly name-sorted".to_owned(),
                ));
            }
            previous = Some(&entry.name);
            bytes.extend(cymule_core::canonical_bytes(entry).map_err(core_error)?);
            bytes.push(b'\n');
        }
        Ok(bytes)
    }

    /// Import one file as a content-addressed object Resource.
    pub fn import_file(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
        media_type: impl Into<String>,
    ) -> ResourceResult<ResourceHandle> {
        let intent = ResourceWriteIntent {
            write_id: write_id.into(),
            shape: ResourceShape::Object,
            media_type: media_type.into(),
            annotations: BTreeMap::new(),
        };
        let session = self.begin_write(&intent)?;
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
        self.commit_write(&session)
    }

    /// Import a directory recursively as a manifest Resource.
    pub fn import_directory(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
    ) -> ResourceResult<ResourceHandle> {
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
            entries.push(FsManifestEntry { name, resource });
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
        upload_id.strip_prefix("upload:").ok_or_else(|| {
            ResourceError::Validation("invalid filesystem upload identity".to_owned())
        })
    }

    fn upload_id(write_id: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(BINDING_VERSION.as_bytes());
        hasher.update([0]);
        hasher.update(write_id.as_bytes());
        format!("upload:{}", hex_digest(&hasher.finalize()))
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

    fn load_record(&self, upload_id: &str) -> ResourceResult<UploadRecord> {
        let record: UploadRecord =
            serde_json::from_slice(&fs::read(self.record_path(upload_id)?).map_err(substrate)?)
                .map_err(substrate)?;
        if record.upload_id != upload_id {
            return Err(ResourceError::Integrity(
                "filesystem upload record identity changed".to_owned(),
            ));
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
        sync_directory(&self.root.join("uploads"))
    }

    fn resource_path(&self, resource: &ResourceHandle) -> ResourceResult<PathBuf> {
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
                    "Resource {} has no location for binding {}",
                    resource.resource_id, self.binding
                ))
            })?;
        let digest = reference.strip_prefix("sha256:").ok_or_else(|| {
            ResourceError::Validation("filesystem reference is not a SHA-256 digest".to_owned())
        })?;
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(ResourceError::Validation(
                "filesystem reference digest is malformed".to_owned(),
            ));
        }
        Ok(self.root.join("objects").join(digest))
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
                intent: intent.clone(),
                upload_id: upload_id.clone(),
                state: UploadState::Open,
                handle: None,
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
        if session.store_binding != self.binding
            || bytes.is_empty()
            || bytes.len() > MAX_WRITE_CHUNK
        {
            return Err(ResourceError::Validation(
                "filesystem write session or chunk is invalid".to_owned(),
            ));
        }
        let _claim = self.claim(&session.upload_id)?;
        let record = self.load_record(&session.upload_id)?;
        if record.write_id() != session.write_id || record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "filesystem upload is not open for this write".to_owned(),
            ));
        }
        let path = self.data_path(&session.upload_id)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(substrate)?;
        let length = file.metadata().map_err(substrate)?.len();
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
        file.sync_all().map_err(substrate)
    }

    fn commit_write(&mut self, session: &ResourceWriteSession) -> ResourceResult<ResourceHandle> {
        if session.store_binding != self.binding {
            return Err(ResourceError::Validation(
                "filesystem write binding changed".to_owned(),
            ));
        }
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(ResourceError::Conflict(
                "filesystem write identity changed".to_owned(),
            ));
        }
        if let Some(handle) = &record.handle {
            handle.verify()?;
            return Ok(handle.clone());
        }
        if record.state != UploadState::Open {
            return Err(ResourceError::Conflict(
                "filesystem upload cannot commit from its current state".to_owned(),
            ));
        }
        let data_path = self.data_path(&session.upload_id)?;
        if !data_path.exists() {
            write_synced(&data_path, &[])?;
        }
        if matches!(
            record.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) {
            validate_manifest(&data_path)?;
        }
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
        if !object.exists() {
            let staging = self
                .root
                .join("staging")
                .join(format!("object-{}", Self::upload_key(&session.upload_id)?));
            fs::copy(&data_path, &staging).map_err(substrate)?;
            File::open(&staging)
                .and_then(|file| file.sync_all())
                .map_err(substrate)?;
            match fs::hard_link(&staging, &object) {
                Ok(()) => sync_directory(&self.root.join("objects"))?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(substrate(error)),
            }
            fs::remove_file(staging).map_err(substrate)?;
        }
        let handle = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: record.intent.shape,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content { digest, size },
            locations: vec![ResourceLocation::Resolver {
                binding: self.binding.clone(),
                reference: format!("sha256:{hex}"),
            }],
            annotations: record.intent.annotations.clone(),
        }
        .seal()?;
        record.state = UploadState::Committed;
        record.handle = Some(handle.clone());
        self.store_record(&record)?;
        Ok(handle)
    }

    fn abort_write(&mut self, session: &ResourceWriteSession) -> ResourceResult<()> {
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id || session.store_binding != self.binding {
            return Err(ResourceError::Conflict(
                "filesystem abort identity changed".to_owned(),
            ));
        }
        if record.state == UploadState::Committed {
            return Ok(());
        }
        record.state = UploadState::Aborted;
        self.store_record(&record)?;
        match fs::remove_file(self.data_path(&session.upload_id)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(substrate(error)),
        }
    }
}

impl ArtifactResolver for FsResourceStore {
    fn stat(&mut self, resource: &ResourceHandle) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        let path = self.resource_path(resource)?;
        let metadata = fs::metadata(path).map_err(substrate)?;
        if let ResourceIntegrity::Content { size, .. } = resource.integrity
            && metadata.len() != size
        {
            return Err(ResourceError::Integrity(
                "filesystem object size changed".to_owned(),
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
        if max_bytes == 0 {
            return Err(ResourceError::Validation(
                "filesystem read limit must be positive".to_owned(),
            ));
        }
        self.stat(resource)?;
        let mut file = File::open(self.resource_path(resource)?).map_err(substrate)?;
        let size = file.metadata().map_err(substrate)?.len();
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
        self.stat(resource)?;
        let offset = cursor
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|error| ResourceError::Validation(error.to_string()))?
            .unwrap_or_default();
        let path = self.resource_path(resource)?;
        if offset > 0 {
            let mut boundary = File::open(&path).map_err(substrate)?;
            boundary
                .seek(SeekFrom::Start(offset - 1))
                .map_err(substrate)?;
            let mut previous = [0_u8; 1];
            boundary.read_exact(&mut previous).map_err(substrate)?;
            if previous[0] != b'\n' {
                return Err(ResourceError::Validation(
                    "filesystem manifest cursor is not an entry boundary".to_owned(),
                ));
            }
        }
        let mut file = BufReader::new(File::open(path).map_err(substrate)?);
        file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
        let mut entries = Vec::new();
        let mut position = offset;
        while entries.len() < limit as usize {
            let mut line = String::new();
            let count = file.read_line(&mut line).map_err(substrate)?;
            if count == 0 {
                break;
            }
            position = position
                .checked_add(count as u64)
                .ok_or_else(|| ResourceError::Integrity("manifest cursor overflow".to_owned()))?;
            let manifest: FsManifestEntry =
                serde_json::from_str(line.trim_end()).map_err(substrate)?;
            validate_name(&manifest.name)?;
            manifest.resource.verify()?;
            entries.push(ResourceEntry {
                name: manifest.name,
                resource: manifest.resource,
            });
        }
        let size = file.get_ref().metadata().map_err(substrate)?.len();
        Ok(ResourcePage {
            entries,
            next_cursor: (position < size).then(|| position.to_string()),
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

fn validate_manifest(path: &Path) -> ResourceResult<()> {
    let mut reader = BufReader::new(File::open(path).map_err(substrate)?);
    let mut previous: Option<String> = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).map_err(substrate)? == 0 {
            return Ok(());
        }
        let entry: FsManifestEntry = serde_json::from_str(line.trim_end()).map_err(substrate)?;
        validate_name(&entry.name)?;
        entry.resource.verify()?;
        if previous
            .as_deref()
            .is_some_and(|name| name >= entry.name.as_str())
        {
            return Err(ResourceError::Validation(
                "filesystem manifest entries are not strictly name-sorted".to_owned(),
            ));
        }
        previous = Some(entry.name);
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
