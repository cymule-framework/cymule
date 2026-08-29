//! Filesystem resource store and resolver for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_LIST_PAGE, MAX_MANIFEST_ENTRY_BYTES,
    MAX_MANIFEST_PAGE_BYTES, MAX_READ_CHUNK, MAX_RESOURCE_CATALOG_RECORD_BYTES, MAX_WRITE_CHUNK,
    ManifestInclusionProof, ManifestPredecessorProof, RESOURCE_LOCATOR_VERSION, ResourceCandidate,
    ResourceCatalogRecord, ResourceCatalogStore, ResourceChunk, ResourceCleanupPlan,
    ResourceCleanupReceipt, ResourceCleanupTarget, ResourceCleanupTargetKind, ResourceDeleter,
    ResourceDeletionTarget, ResourceEntry, ResourceError, ResourceHandle, ResourceIntegrity,
    ResourceListCursor, ResourceListProof, ResourceLocation, ResourceLocatorSet,
    ResourceManifestAccumulator, ResourceManifestDescriptor, ResourceManifestEntry,
    ResourceManifestStreamVerifier, ResourceObservation, ResourcePage, ResourcePublication,
    ResourceResult, ResourceRetentionFamily, ResourceShape, ResourceWriteIntent,
    ResourceWriteSession, canonical_manifest_entry_bytes, manifest_node_digest,
    resource_manifest_descriptor_id,
};
use fs4::{FileExt, TryLockError};
use nix::dir::Dir;
use nix::errno::Errno;
use nix::fcntl::{AtFlags, OFlag, open, openat, renameat};
use nix::sys::stat::{Mode, SFlag, fstatat, mkdirat};
use nix::unistd::{UnlinkatFlags, dup, linkat, unlinkat};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const BINDING_VERSION: &str = "cymule.resource-fs/6";
const CHILD_WRITE_ID_VERSION: &str = "cymule.resource-fs-child-write/1";
const DIRECTORY_MEDIA_TYPE: &str = cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE;
const UPLOAD_RECORD_VERSION: &str = "cymule.resource-fs-upload/9";
const MANIFEST_INDEX_VERSION: &str = "cymule.resource-fs-manifest-index/3";
const PHYSICAL_LAYOUT_VERSION: &str = "cymule.resource-fs-layout/2";
const PHYSICAL_LAYOUT_MARKER: &str = "layout.json";
const PHYSICAL_LAYOUT_STAGING_MARKER: &str = "layout.json.initializing";
const MAX_UPLOAD_RECORD_BYTES: u64 = 16 * 1024 * 1024;
/// Maximum number of nested child-directory edges in one recursive import.
pub const MAX_DIRECTORY_IMPORT_DEPTH: usize = 64;
#[cfg(not(test))]
const DIRECTORY_SORT_RUN_ENTRIES: usize = 1024;
#[cfg(test)]
const DIRECTORY_SORT_RUN_ENTRIES: usize = 16;
const DIRECTORY_SORT_MERGE_FAN_IN: u64 = 8;

#[cfg(test)]
static FAIL_AFTER_DIRECTORY_SORT_RUNS: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FAIL_AFTER_DELETION_TOMBSTONE: AtomicBool = AtomicBool::new(false);
const FIXED_DIRECTORIES: [&str; 6] = [
    "uploads",
    "objects",
    "catalog",
    "locks",
    "staging",
    "manifest-indexes",
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UploadState {
    Open,
    Publishing,
    Committed,
    Deleted,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadPublication {
    digest: String,
    size: u64,
    manifest: Option<ResourceManifestDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadRecord {
    record_version: String,
    intent: ResourceWriteIntent,
    upload_id: String,
    store_binding: String,
    state: UploadState,
    committed_length: u64,
    publication: Option<UploadPublication>,
    cleanup_plan: Option<ResourceCleanupPlan>,
    cleanup_receipt: Option<ResourceCleanupReceipt>,
}

enum ImportInput<'a> {
    FileSize(u64),
    Manifest(&'a ResourceManifestDescriptor),
}

impl ImportInput<'_> {
    fn verify(&self, publication: &ResourcePublication) -> ResourceResult<()> {
        let message = match self {
            Self::FileSize(expected)
                if matches!(&publication.resource.integrity,
                    ResourceIntegrity::Content { size, .. } if size == expected) =>
            {
                return Ok(());
            }
            Self::FileSize(_) => "filesystem import length changed committed bytes",
            Self::Manifest(expected)
                if publication.resource.manifest.as_ref() == Some(*expected) =>
            {
                return Ok(());
            }
            Self::Manifest(_) => "filesystem directory import replay changed its exact manifest",
        };
        Err(conflict("filesystem_import_conflict", message))
    }
}

#[derive(Serialize)]
struct UploadIdentity<'a> {
    store_binding: &'a str,
    write_id: &'a str,
}

#[derive(Serialize)]
struct ChildWriteIdentity<'a> {
    parent_write_id: &'a str,
    child_name: &'a str,
}

#[derive(Debug, Clone, Copy)]
struct ManifestPageRange {
    start_index: u64,
    end_index: u64,
    start_offset: u64,
    end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalLayoutMarker {
    layout_version: String,
}

#[derive(Debug)]
struct StoreDirectories {
    root: File,
    uploads: File,
    objects: File,
    catalog: File,
    locks: File,
    staging: File,
    manifest_indexes: File,
}

#[derive(Debug)]
struct RetentionClaim {
    file: File,
    family: ResourceRetentionFamily,
    deleted: bool,
}

/// Content-addressed local resource adapter.
#[derive(Debug, Clone)]
pub struct FsResourceStore {
    directories: Arc<StoreDirectories>,
    binding: String,
    read_only: bool,
}

impl FsResourceStore {
    /// Open or create one filesystem resource namespace.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid binding, unsupported physical layout,
    /// unsafe filesystem entry, or host I/O failure.
    pub fn open(root: impl AsRef<Path>, binding: impl Into<String>) -> ResourceResult<Self> {
        Self::open_with_mode(root.as_ref(), binding.into(), false)
    }

    /// Open one existing filesystem Resource namespace without creating,
    /// cleaning, truncating, or otherwise mutating any filesystem entry.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid binding, a missing or unsupported
    /// physical layout, an unsafe filesystem entry, or host I/O failure.
    pub fn open_read_only(
        root: impl AsRef<Path>,
        binding: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::open_with_mode(root.as_ref(), binding.into(), true)
    }

    fn open_with_mode(root: &Path, binding: String, read_only: bool) -> ResourceResult<Self> {
        validate_binding(&binding)?;
        if !read_only && !root.exists() {
            fs::create_dir(root).map_err(substrate)?;
        }
        let root_file = open_directory(root)?;
        if read_only {
            verify_layout_marker(&root_file)?;
        } else {
            initialize_layout_marker(&root_file)?;
        }
        if !read_only {
            for child in FIXED_DIRECTORIES {
                ensure_directory_at(&root_file, child)?;
            }
            root_file.sync_all().map_err(substrate)?;
        }
        verify_root_layout(&root_file)?;
        let objects_root = open_directory_at(&root_file, "objects")?;
        let catalog_root = open_directory_at(&root_file, "catalog")?;
        let manifest_indexes_root = open_directory_at(&root_file, "manifest-indexes")?;
        for directory in [&objects_root, &catalog_root, &manifest_indexes_root] {
            verify_binding_namespace_root(directory)?;
        }
        let binding_namespace = binding_namespace(&binding);
        if !read_only {
            for directory in [&objects_root, &catalog_root, &manifest_indexes_root] {
                ensure_directory_at(directory, &binding_namespace)?;
                directory.sync_all().map_err(substrate)?;
            }
        }
        let directories = Arc::new(StoreDirectories {
            uploads: open_directory_at(&root_file, "uploads")?,
            objects: open_directory_at(&objects_root, &binding_namespace)?,
            catalog: open_directory_at(&catalog_root, &binding_namespace)?,
            locks: open_directory_at(&root_file, "locks")?,
            staging: open_directory_at(&root_file, "staging")?,
            manifest_indexes: open_directory_at(&manifest_indexes_root, &binding_namespace)?,
            root: root_file,
        });
        Ok(Self {
            directories,
            binding,
            read_only,
        })
    }

    /// Immutable adapter binding retained in Resource locations.
    pub fn binding(&self) -> &str {
        &self.binding
    }

    fn ensure_writable(&self) -> ResourceResult<()> {
        if self.read_only {
            return Err(conflict(
                "filesystem_read_only",
                "filesystem Resource namespace was opened read-only".to_owned(),
            ));
        }
        Ok(())
    }

    /// Rebuild this adapter's replaceable locator set for one semantic Handle.
    ///
    /// # Errors
    ///
    /// Returns an error when the Resource is invalid or lacks content evidence.
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
    ///
    /// # Errors
    ///
    /// Returns an error when an entry is invalid, unsorted, or oversized.
    pub fn encode_manifest(entries: &[ResourceManifestEntry]) -> ResourceResult<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut manifest = ResourceManifestAccumulator::new();
        for entry in entries {
            let line = manifest.push(entry)?;
            bytes.extend_from_slice(line.bytes());
        }
        let _ = manifest.descriptor()?;
        Ok(bytes)
    }

    /// Import one file as a content-addressed object Resource.
    ///
    /// # Errors
    ///
    /// Returns an error for a symlink or non-regular input, invalid write
    /// intent, conflicting replay, integrity failure, or host I/O failure.
    pub fn import_file(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
        media_type: impl Into<String>,
    ) -> ResourceResult<ResourcePublication> {
        self.ensure_writable()?;
        let source = open_regular_file(path.as_ref())?;
        self.import_file_descriptor(source, write_id.into(), media_type.into())
    }

    fn import_file_descriptor(
        &mut self,
        mut source: File,
        write_id: String,
        media_type: String,
    ) -> ResourceResult<ResourcePublication> {
        let intent = ResourceWriteIntent {
            write_id,
            shape: ResourceShape::Object,
            media_type,
            annotations: BTreeMap::new(),
        };
        let session = self.begin_write(&intent)?;
        let record = self.load_record(&session.upload_id)?;
        if record.publication.is_some() {
            let publication = self.commit_write(&session)?;
            self.verify_import_replay(&mut source, &publication)?;
            return Ok(publication);
        }
        let mut offset = 0_u64;
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let count = source.read(&mut buffer).map_err(substrate)?;
            if count == 0 {
                break;
            }
            self.write_chunk(&session, offset, &buffer[..count])?;
            offset = offset.checked_add(count as u64).ok_or_else(|| {
                integrity("filesystem_import_invalid", "file size overflow".to_owned())
            })?;
        }
        self.commit_write_with_import(&session, Some(&ImportInput::FileSize(offset)))
    }

    fn commit_write_with_import(
        &mut self,
        session: &ResourceWriteSession,
        input: Option<&ImportInput<'_>>,
    ) -> ResourceResult<ResourcePublication> {
        self.ensure_writable()?;
        self.validate_session(session)?;
        // Keep final source validation and Publishing admission under one claim.
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem write identity changed".to_owned(),
            ));
        }
        if let Some(family) = self.reconcile_deleted_upload(session, &mut record)? {
            return Err(resource_deleted(&family));
        }
        if record.state == UploadState::Committed {
            let publication = self.publication_for_record(&record)?.ok_or_else(|| {
                integrity(
                    "filesystem_upload_record_invalid",
                    "committed filesystem upload has no publication".to_owned(),
                )
            })?;
            publication.verify()?;
            self.stat(&publication.resource, &publication.locators)?;
            self.cleanup_upload_files(session, &mut record)?;
            if let Some(input) = input {
                input.verify(&publication)?;
            }
            return Ok(publication);
        }
        if record.state == UploadState::Publishing {
            let publication = self.publication_for_record(&record)?.ok_or_else(|| {
                integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem Publishing intent has no publication".to_owned(),
                )
            })?;
            publication.verify()?;
            let publication = self.finish_publication(session, record, publication)?;
            if let Some(input) = input {
                input.verify(&publication)?;
            }
            return Ok(publication);
        }
        if record.state != UploadState::Open || record.publication.is_some() {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem upload cannot commit from its current state".to_owned(),
            ));
        }
        let mut data = self.open_acknowledged_upload_data(session, record.committed_length)?;
        let manifest = if matches!(
            record.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) {
            Some(self.prepare_manifest_index(session, &mut data)?)
        } else {
            None
        };
        let (digest, size) = if let Some(descriptor) = &manifest {
            (descriptor.digest.clone(), descriptor.size)
        } else {
            hash_file(&mut data)?
        };
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: record.intent.shape,
            media_type: record.intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: digest.clone(),
                size,
            },
            manifest,
            annotations: record.intent.annotations.clone(),
        }
        .seal()?;
        let publication = ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: self.binding.clone(),
                locations: vec![ResourceLocation::Opaque {
                    reference: digest.clone(),
                }],
            },
            resource,
        };
        publication.verify()?;
        if let Some(input) = input {
            input.verify(&publication)?;
        }
        record.state = UploadState::Publishing;
        record.publication = Some(UploadPublication {
            digest,
            size,
            manifest: publication.resource.manifest.clone(),
        });
        self.store_record(&record)?;
        self.finish_publication(session, record, publication)
    }

    fn verify_import_replay(
        &self,
        source: &mut File,
        publication: &ResourcePublication,
    ) -> ResourceResult<()> {
        publication.verify()?;
        let object_name =
            self.resolvable_resource_name(&publication.resource, &publication.locators)?;
        let ResourceIntegrity::Content { digest, size } = &publication.resource.integrity else {
            return Err(integrity(
                "filesystem_import_invalid",
                "filesystem import publication is not content addressed".to_owned(),
            ));
        };
        let mut retained = open_regular_at(&self.directories.objects, &object_name, false, false)?;
        verify_content_file(&mut retained, digest, *size)?;
        source.seek(SeekFrom::Start(0)).map_err(substrate)?;
        retained.seek(SeekFrom::Start(0)).map_err(substrate)?;
        let mut source_buffer = vec![0_u8; 1024 * 1024];
        let mut retained_buffer = vec![0_u8; 1024 * 1024];
        loop {
            let source_count = source.read(&mut source_buffer).map_err(substrate)?;
            let retained_count = retained.read(&mut retained_buffer).map_err(substrate)?;
            if source_count != retained_count
                || source_buffer[..source_count] != retained_buffer[..retained_count]
            {
                return Err(conflict(
                    "filesystem_import_conflict",
                    "filesystem import replay changed committed bytes".to_owned(),
                ));
            }
            if source_count == 0 {
                return Ok(());
            }
        }
    }

    /// Import a directory recursively as a manifest Resource.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe names, symlinks, non-regular entries,
    /// directory depth above [`MAX_DIRECTORY_IMPORT_DEPTH`], conflicting
    /// writes, integrity failures, or host I/O failure.
    pub fn import_directory(
        &mut self,
        path: impl AsRef<Path>,
        write_id: impl Into<String>,
    ) -> ResourceResult<ResourcePublication> {
        self.ensure_writable()?;
        let directory = open_directory(path.as_ref())?;
        self.import_directory_descriptor(&directory, write_id.into(), 0)
    }

    fn import_directory_descriptor(
        &mut self,
        directory: &File,
        write_id: String,
        depth: usize,
    ) -> ResourceResult<ResourcePublication> {
        let intent = ResourceWriteIntent {
            write_id,
            shape: ResourceShape::Directory,
            media_type: DIRECTORY_MEDIA_TYPE.to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = self.begin_write(&intent)?;
        let has_publication = self.prepare_directory_import(&session)?;
        if has_publication {
            let publication = self.commit_write(&session)?;
            self.verify_directory_import_replay(directory, &intent.write_id, &publication, depth)?;
            return Ok(publication);
        }
        let sort_name = Self::directory_sort_staging_name(&session)?;
        let sort_directory = open_directory_at(&self.directories.staging, &sort_name)?;
        let run_count = write_directory_sort_runs(directory, &sort_directory)?;
        #[cfg(test)]
        if FAIL_AFTER_DIRECTORY_SORT_RUNS.swap(false, Ordering::SeqCst) {
            return Err(substrate_with_code(
                "filesystem_sort_run_publication_interrupted",
                "injected interruption after directory sort-run publication".to_owned(),
            ));
        }
        let final_run = merge_directory_sort_runs(&sort_directory, run_count)?;
        let mut sorted_names = final_run
            .as_deref()
            .map(|name| open_regular_at(&sort_directory, name, false, false).map(BufReader::new))
            .transpose()?;
        let mut offset = 0_u64;
        let mut chunk = Vec::new();
        let mut manifest = ResourceManifestAccumulator::new();
        while let Some(name) = sorted_names
            .as_mut()
            .map(read_directory_sort_name)
            .transpose()?
            .flatten()
        {
            let child_write_id = child_write_id(&intent.write_id, &name)?;
            let child = open_import_child(directory, &name)?;
            let metadata = child.metadata().map_err(substrate)?;
            let is_directory = metadata.is_dir();
            if is_directory && !chunk.is_empty() {
                self.write_chunk(&session, offset, &chunk)?;
                offset = offset.checked_add(chunk.len() as u64).ok_or_else(|| {
                    integrity(
                        "filesystem_import_invalid",
                        "manifest byte offset overflow".to_owned(),
                    )
                })?;
                chunk = Vec::new();
            }
            let resource = if is_directory {
                self.import_directory_descriptor(
                    &child,
                    child_write_id,
                    next_directory_import_depth(depth)?,
                )?
            } else if metadata.is_file() {
                self.import_file_descriptor(
                    child,
                    child_write_id,
                    "application/octet-stream".to_owned(),
                )?
            } else {
                return Err(ResourceError::Validation(format!(
                    "unsupported filesystem entry {name:?}"
                )));
            };
            let entry = ResourceManifestEntry {
                name,
                resource: resource.resource,
            };
            let line = manifest.push(&entry)?;
            let next_chunk_size = chunk.len().checked_add(line.bytes().len()).ok_or_else(|| {
                integrity(
                    "filesystem_import_invalid",
                    "manifest write chunk size overflow".to_owned(),
                )
            })?;
            if next_chunk_size > MAX_WRITE_CHUNK && !chunk.is_empty() {
                self.write_chunk(&session, offset, &chunk)?;
                offset = offset.checked_add(chunk.len() as u64).ok_or_else(|| {
                    integrity(
                        "filesystem_import_invalid",
                        "manifest byte offset overflow".to_owned(),
                    )
                })?;
                chunk.clear();
            }
            chunk.extend_from_slice(line.bytes());
        }
        let expected_manifest = manifest.descriptor()?;
        if !chunk.is_empty() {
            self.write_chunk(&session, offset, &chunk)?;
        }
        self.commit_write_with_import(&session, Some(&ImportInput::Manifest(&expected_manifest)))
    }

    fn prepare_directory_import(&self, session: &ResourceWriteSession) -> ResourceResult<bool> {
        self.validate_session(session)?;
        let _claim = self.claim(&session.upload_id)?;
        let record = self.load_record(&session.upload_id)?;
        match record.state {
            UploadState::Publishing | UploadState::Committed => return Ok(true),
            UploadState::Deleted => {
                return Err(conflict(
                    "filesystem_resource_deleted",
                    "filesystem directory import belongs to a deleted retention family".to_owned(),
                ));
            }
            UploadState::Aborted => {
                return Err(conflict(
                    "filesystem_upload_conflict",
                    "filesystem directory import was aborted before staging preparation".to_owned(),
                ));
            }
            UploadState::Open => {}
        }
        let sort_name = Self::directory_sort_staging_name(session)?;
        remove_directory_sort_staging(&self.directories.staging, &sort_name)?;
        ensure_directory_at(&self.directories.staging, &sort_name)?;
        self.directories.staging.sync_all().map_err(substrate)?;
        Ok(false)
    }

    fn verify_directory_import_replay(
        &mut self,
        directory: &File,
        write_id: &str,
        publication: &ResourcePublication,
        depth: usize,
    ) -> ResourceResult<()> {
        publication.verify()?;
        let descriptor = publication.resource.manifest.as_ref().ok_or_else(|| {
            integrity(
                "filesystem_import_invalid",
                "filesystem directory publication lost its manifest descriptor".to_owned(),
            )
        })?;
        let source_count = count_import_directory_entries(directory)?;
        if source_count != descriptor.entry_count {
            return Err(conflict(
                "filesystem_import_conflict",
                "filesystem directory import replay changed its exact manifest".to_owned(),
            ));
        }
        let mut cursor = None;
        let mut observed_count = 0_u64;
        loop {
            let page = self.list(
                &publication.resource,
                &publication.locators,
                cursor.as_deref(),
                MAX_LIST_PAGE,
            )?;
            if page.entries.is_empty() && page.next_cursor.is_some() {
                return Err(integrity(
                    "filesystem_import_invalid",
                    "filesystem directory replay page failed to advance".to_owned(),
                ));
            }
            for expected in page.entries {
                observed_count = observed_count.checked_add(1).ok_or_else(|| {
                    integrity(
                        "filesystem_import_invalid",
                        "filesystem directory replay entry count overflow".to_owned(),
                    )
                })?;
                let child_write_id = child_write_id(write_id, &expected.name)?;
                let child = open_import_child(directory, &expected.name)?;
                let metadata = child.metadata().map_err(substrate)?;
                let retained = if metadata.is_dir() {
                    self.import_directory_descriptor(
                        &child,
                        child_write_id,
                        next_directory_import_depth(depth)?,
                    )?
                } else if metadata.is_file() {
                    self.import_file_descriptor(
                        child,
                        child_write_id,
                        "application/octet-stream".to_owned(),
                    )?
                } else {
                    return Err(ResourceError::Validation(format!(
                        "unsupported filesystem entry {:?}",
                        expected.name
                    )));
                };
                if retained.resource != expected.resource {
                    return Err(conflict(
                        "filesystem_import_conflict",
                        "filesystem directory import replay changed a child Resource".to_owned(),
                    ));
                }
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        if observed_count != descriptor.entry_count {
            return Err(integrity(
                "filesystem_import_invalid",
                "filesystem directory replay did not close its manifest entry count".to_owned(),
            ));
        }
        Ok(())
    }

    fn directory_sort_staging_name(session: &ResourceWriteSession) -> ResourceResult<String> {
        Ok(format!(
            "directory-sort-{}",
            Self::upload_key(&session.upload_id)?
        ))
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

    fn validate_session(&self, session: &ResourceWriteSession) -> ResourceResult<()> {
        if session.store_binding != self.binding
            || session.upload_id != self.upload_id(&session.write_id)?
        {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem upload session is not authenticated by its write ID and binding"
                    .to_owned(),
            ));
        }
        let _ = Self::upload_key(&session.upload_id)?;
        Ok(())
    }

    fn record_name(upload_id: &str) -> ResourceResult<String> {
        Ok(format!("{}.json", Self::upload_key(upload_id)?))
    }

    fn data_name(upload_id: &str) -> ResourceResult<String> {
        Ok(format!("{}.data", Self::upload_key(upload_id)?))
    }

    fn claim(&self, upload_id: &str) -> ResourceResult<File> {
        self.ensure_writable()?;
        let lock = open_regular_at(
            &self.directories.locks,
            &format!("{}.lock", Self::upload_key(upload_id)?),
            true,
            true,
        )?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(conflict(
                "filesystem_lock_busy",
                format!("filesystem upload {upload_id} has an active writer"),
            )),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn retention_token(family: &ResourceRetentionFamily) -> ResourceResult<&str> {
        family.verify()?;
        family.retention_key.strip_prefix("sha256:").ok_or_else(|| {
            integrity(
                "filesystem_retention_family_invalid",
                "filesystem retention family key is not SHA-256 addressed".to_owned(),
            )
        })
    }

    fn retention_lock_name(family: &ResourceRetentionFamily) -> ResourceResult<String> {
        Ok(format!("retention-{}.lock", Self::retention_token(family)?))
    }

    fn claim_retention(&self, family: &ResourceRetentionFamily) -> ResourceResult<RetentionClaim> {
        self.ensure_writable()?;
        family.verify()?;
        if family.store_binding != self.binding {
            return Err(conflict(
                "filesystem_cleanup_conflict",
                "filesystem retention family belongs to another store binding".to_owned(),
            ));
        }
        let lock_name = Self::retention_lock_name(family)?;
        let mut file = open_regular_at(&self.directories.locks, &lock_name, true, true)?;
        match FileExt::try_lock(&file) {
            Ok(()) => {
                let deleted = Self::retention_tombstone(&mut file)?;
                Ok(RetentionClaim {
                    file,
                    family: family.clone(),
                    deleted,
                })
            }
            Err(TryLockError::WouldBlock) => Err(conflict(
                "filesystem_lock_busy",
                format!(
                    "filesystem retention family {} has an active writer",
                    family.retention_key
                ),
            )),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn retention_tombstone(file: &mut File) -> ResourceResult<bool> {
        match file.metadata().map_err(substrate)?.len() {
            0 => Ok(false),
            1 => {
                file.seek(SeekFrom::Start(0)).map_err(substrate)?;
                let mut marker = [0_u8; 1];
                file.read_exact(&mut marker).map_err(substrate)?;
                if marker == *b"D" {
                    Ok(true)
                } else {
                    Err(integrity(
                        "filesystem_retention_family_invalid",
                        "filesystem retention-family control contains an invalid tombstone"
                            .to_owned(),
                    ))
                }
            }
            _ => Err(integrity(
                "filesystem_retention_family_invalid",
                "filesystem retention-family control has an invalid length".to_owned(),
            )),
        }
    }

    fn persist_deletion_tombstone(&self, claim: &mut RetentionClaim) -> ResourceResult<()> {
        if claim.deleted {
            claim.file.sync_all().map_err(substrate)?;
            self.directories.locks.sync_all().map_err(substrate)?;
            return Ok(());
        }
        claim.file.seek(SeekFrom::Start(0)).map_err(substrate)?;
        claim.file.write_all(b"D").map_err(substrate)?;
        claim.file.sync_all().map_err(substrate)?;
        self.directories.locks.sync_all().map_err(substrate)?;
        claim.deleted = Self::retention_tombstone(&mut claim.file)?;
        if !claim.deleted {
            return Err(integrity(
                "filesystem_retention_family_invalid",
                "filesystem deletion tombstone failed its durable readback".to_owned(),
            ));
        }
        Ok(())
    }

    fn family_is_deleted(&self, family: &ResourceRetentionFamily) -> ResourceResult<bool> {
        family.verify()?;
        if family.store_binding != self.binding {
            return Ok(false);
        }
        let lock_name = Self::retention_lock_name(family)?;
        let Some(mut file) = open_regular_at_optional(&self.directories.locks, &lock_name)? else {
            return Ok(false);
        };
        Self::retention_tombstone(&mut file)
    }

    fn catalog_token(namespace: &str, key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0]);
        hasher.update(key.as_bytes());
        hex_digest(&hasher.finalize())
    }

    fn catalog_name(namespace: &str, key: &str) -> String {
        format!("{}.json", Self::catalog_token(namespace, key))
    }

    fn claim_catalog(&self, namespace: &str, key: &str) -> ResourceResult<File> {
        let token = Self::catalog_token(namespace, key);
        self.ensure_writable()?;
        let lock = open_regular_at(
            &self.directories.locks,
            &format!("catalog-{token}.lock"),
            true,
            true,
        )?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(conflict(
                "filesystem_lock_busy",
                format!("filesystem catalog record {namespace}/{key} has an active writer"),
            )),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn manifest_key(descriptor: &ResourceManifestDescriptor) -> ResourceResult<&str> {
        descriptor.verify()?;
        descriptor
            .digest
            .strip_prefix("sha256:")
            .ok_or_else(|| ResourceError::Validation("manifest digest is malformed".to_owned()))
    }

    fn manifest_index_name(descriptor: &ResourceManifestDescriptor) -> ResourceResult<String> {
        Ok(Self::manifest_key(descriptor)?.to_owned())
    }

    fn manifest_index_staging_name(session: &ResourceWriteSession) -> ResourceResult<String> {
        Ok(format!(
            "manifest-index-{}",
            Self::upload_key(&session.upload_id)?
        ))
    }

    fn claim_manifest_index(
        &self,
        descriptor: &ResourceManifestDescriptor,
    ) -> ResourceResult<File> {
        let key = Self::manifest_key(descriptor)?;
        self.ensure_writable()?;
        let lock = open_regular_at(
            &self.directories.locks,
            &format!("manifest-{key}.lock"),
            true,
            true,
        )?;
        match FileExt::try_lock(&lock) {
            Ok(()) => Ok(lock),
            Err(TryLockError::WouldBlock) => Err(conflict(
                "filesystem_manifest_conflict",
                format!("filesystem manifest index {key} has an active writer"),
            )),
            Err(TryLockError::Error(error)) => Err(substrate(error)),
        }
    }

    fn manifest_index_header(
        descriptor: &ResourceManifestDescriptor,
    ) -> ResourceResult<ResourceCatalogRecord> {
        descriptor.verify()?;
        ResourceCatalogRecord::new(
            MANIFEST_INDEX_VERSION,
            descriptor.digest.clone(),
            cymule_core::canonical_bytes(descriptor).map_err(core_error)?,
        )
    }

    fn verify_manifest_index_header(
        header: &ResourceCatalogRecord,
        descriptor: &ResourceManifestDescriptor,
    ) -> ResourceResult<()> {
        header.verify()?;
        let expected = Self::manifest_index_header(descriptor)?;
        if header != &expected {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest index header changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn load_manifest_index_header(
        &mut self,
        descriptor: &ResourceManifestDescriptor,
    ) -> ResourceResult<(File, ResourceCatalogRecord)> {
        let index = open_directory_at(
            &self.directories.manifest_indexes,
            &Self::manifest_index_name(descriptor)?,
        )?;
        let header = self
            .get_catalog_record(MANIFEST_INDEX_VERSION, &descriptor.digest)?
            .ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest index catalog record is missing".to_owned(),
                )
            })?;
        Self::verify_manifest_index_header(&header, descriptor)?;
        let expected_offsets = descriptor
            .entry_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(8))
            .ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "manifest index size overflow".to_owned(),
                )
            })?;
        let offsets = open_regular_at(&index, "offsets.bin", false, false)?;
        if offsets.metadata().map_err(substrate)?.len() != expected_offsets {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest offset index size changed".to_owned(),
            ));
        }
        let expected_nodes = manifest_node_count(descriptor.entry_count)?
            .checked_mul(32)
            .ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "manifest node index overflow".to_owned(),
                )
            })?;
        let mut nodes = open_regular_at(&index, "nodes.bin", false, false)?;
        if nodes.metadata().map_err(substrate)?.len() != expected_nodes {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest node index size changed".to_owned(),
            ));
        }
        if descriptor.entry_count > 0 {
            nodes.seek(SeekFrom::End(-32)).map_err(substrate)?;
            let root = read_digest(&mut nodes)?;
            if root != descriptor.root_digest {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest node index root changed".to_owned(),
                ));
            }
        }
        Ok((index, header))
    }

    fn prepare_manifest_index(
        &self,
        session: &ResourceWriteSession,
        data: &mut File,
    ) -> ResourceResult<ResourceManifestDescriptor> {
        self.ensure_writable()?;
        let staging_name = Self::manifest_index_staging_name(session)?;
        remove_manifest_index_staging(&self.directories.staging, &staging_name)?;
        mkdirat(
            &self.directories.staging,
            staging_name.as_str(),
            Mode::from_bits_truncate(0o700),
        )
        .map_err(substrate)?;
        let staging = open_directory_at(&self.directories.staging, &staging_name)?;
        let mut offsets = create_regular_at(&staging, "offsets.bin")?;
        let mut nodes = create_regular_at(&staging, "nodes.bin")?;
        offsets.write_all(&0_u64.to_be_bytes()).map_err(substrate)?;

        data.seek(SeekFrom::Start(0)).map_err(substrate)?;
        let mut reader = BufReader::new(data.try_clone().map_err(substrate)?);
        let mut manifest = ResourceManifestAccumulator::new();
        while let Some(line) = read_capped_manifest_line(&mut reader)? {
            let payload = line.strip_suffix(b"\n").ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest requires one newline per canonical entry".to_owned(),
                )
            })?;
            let entry: ResourceManifestEntry = cymule_core::decode_json(payload)
                .map_err(|error| integrity("filesystem_manifest_invalid", error.to_string()))?;
            let canonical = manifest.push(&entry)?;
            if canonical.bytes() != line {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest entry bytes are not canonical".to_owned(),
                ));
            }
            nodes
                .write_all(&decode_digest(canonical.leaf_digest())?)
                .map_err(substrate)?;
            offsets
                .write_all(&manifest.byte_size().to_be_bytes())
                .map_err(substrate)?;
        }
        if manifest.byte_size() != data.metadata().map_err(substrate)?.len() {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest scan did not consume the exact upload bytes".to_owned(),
            ));
        }

        append_manifest_parent_levels(&mut nodes, manifest.entry_count())?;
        offsets.sync_all().map_err(substrate)?;
        nodes.sync_all().map_err(substrate)?;
        staging.sync_all().map_err(substrate)?;
        self.directories.staging.sync_all().map_err(substrate)?;

        let descriptor = manifest.descriptor()?;
        if descriptor.entry_count > 0
            && read_digest_at(&mut nodes, manifest_node_count(descriptor.entry_count)? - 1)?
                != descriptor.root_digest
        {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest streaming root does not match its persisted index".to_owned(),
            ));
        }
        Ok(descriptor)
    }

    fn persist_manifest_index(
        &mut self,
        session: &ResourceWriteSession,
        descriptor: &ResourceManifestDescriptor,
    ) -> ResourceResult<()> {
        self.ensure_writable()?;
        descriptor.verify()?;
        let _claim = self.claim_manifest_index(descriptor)?;
        let destination = Self::manifest_index_name(descriptor)?;
        let staging = Self::manifest_index_staging_name(session)?;
        if entry_exists(&self.directories.manifest_indexes, &destination)? {
            let retained = open_directory_at(&self.directories.manifest_indexes, &destination)?;
            sync_file_at(&retained, "offsets.bin")?;
            sync_file_at(&retained, "nodes.bin")?;
            retained.sync_all().map_err(substrate)?;
            self.directories
                .manifest_indexes
                .sync_all()
                .map_err(substrate)?;
            remove_manifest_index_staging(&self.directories.staging, &staging)?;
            self.directories.staging.sync_all().map_err(substrate)?;
        } else {
            if !entry_exists(&self.directories.staging, &staging)? {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    "filesystem Publishing manifest lost its prepared index".to_owned(),
                ));
            }
            renameat(
                &self.directories.staging,
                staging.as_str(),
                &self.directories.manifest_indexes,
                destination.as_str(),
            )
            .map_err(substrate)?;
            self.directories
                .manifest_indexes
                .sync_all()
                .map_err(substrate)?;
            self.directories.staging.sync_all().map_err(substrate)?;
        }
        self.put_catalog_record(&Self::manifest_index_header(descriptor)?)?;
        self.load_manifest_index_header(descriptor).map(|_| ())
    }

    fn manifest_offset(offsets: &mut File, position: u64) -> ResourceResult<u64> {
        offsets
            .seek(SeekFrom::Start(position.checked_mul(8).ok_or_else(
                || {
                    integrity(
                        "filesystem_manifest_invalid",
                        "manifest offset position overflow".to_owned(),
                    )
                },
            )?))
            .map_err(substrate)?;
        let mut encoded = [0_u8; 8];
        offsets.read_exact(&mut encoded).map_err(substrate)?;
        Ok(u64::from_be_bytes(encoded))
    }

    fn manifest_inclusion(
        nodes: &mut File,
        entry_count: u64,
        position: u64,
    ) -> ResourceResult<ManifestInclusionProof> {
        if position >= entry_count {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest inclusion exceeds its entry count".to_owned(),
            ));
        }
        let mut path = Vec::new();
        let mut width = entry_count;
        let mut node_position = position;
        let mut level_start = 0_u64;
        while width > 1 {
            let (sibling, side) = if node_position.is_multiple_of(2) {
                (
                    node_position
                        .checked_add(1)
                        .ok_or_else(|| {
                            integrity(
                                "filesystem_manifest_invalid",
                                "manifest sibling position overflow".to_owned(),
                            )
                        })?
                        .min(width - 1),
                    cymule_resource::MerkleSide::Right,
                )
            } else {
                (node_position - 1, cymule_resource::MerkleSide::Left)
            };
            let byte_offset = level_start
                .checked_add(sibling)
                .and_then(|node| node.checked_mul(32))
                .ok_or_else(|| {
                    integrity(
                        "filesystem_manifest_invalid",
                        "manifest node offset overflow".to_owned(),
                    )
                })?;
            nodes
                .seek(SeekFrom::Start(byte_offset))
                .map_err(substrate)?;
            path.push(cymule_resource::MerkleStep {
                side,
                digest: read_digest(nodes)?,
            });
            level_start = level_start.checked_add(width).ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "manifest level offset overflow".to_owned(),
                )
            })?;
            node_position /= 2;
            width = width.div_ceil(2);
        }
        Ok(ManifestInclusionProof {
            index: position,
            path,
        })
    }

    fn manifest_page_end(
        offsets: &mut File,
        descriptor: &ResourceManifestDescriptor,
        start_index: u64,
        start_offset: u64,
        limit: u32,
    ) -> ResourceResult<ManifestPageRange> {
        let requested_end = start_index
            .checked_add(u64::from(limit))
            .ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "manifest page index overflow".to_owned(),
                )
            })?
            .min(descriptor.entry_count);
        let mut end_index = start_index;
        let mut end_offset = start_offset;
        while end_index < requested_end {
            let next_offset = Self::manifest_offset(offsets, end_index + 1)?;
            let entry_bytes = next_offset.checked_sub(end_offset).ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest offsets are not monotonic".to_owned(),
                )
            })?;
            if entry_bytes == 0 || entry_bytes > MAX_MANIFEST_ENTRY_BYTES as u64 {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    format!(
                        "filesystem manifest entry exceeds {MAX_MANIFEST_ENTRY_BYTES} canonical bytes"
                    ),
                ));
            }
            let page_bytes = next_offset.checked_sub(start_offset).ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest page offset underflow".to_owned(),
                )
            })?;
            if page_bytes > MAX_MANIFEST_PAGE_BYTES {
                break;
            }
            end_index += 1;
            end_offset = next_offset;
        }
        if end_index == start_index && start_index < descriptor.entry_count {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest entry cannot fit the bounded page".to_owned(),
            ));
        }
        if end_offset > descriptor.size {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest page offset exceeds retained bytes".to_owned(),
            ));
        }
        Ok(ManifestPageRange {
            start_index,
            end_index,
            start_offset,
            end_offset,
        })
    }

    fn read_manifest_page(
        manifest: &mut File,
        index: &File,
        offsets: &mut File,
        descriptor: &ResourceManifestDescriptor,
        range: ManifestPageRange,
    ) -> ResourceResult<(Vec<ResourceEntry>, Vec<ManifestInclusionProof>)> {
        if range.start_offset > range.end_offset || range.end_offset > descriptor.size {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest page range exceeds retained bytes".to_owned(),
            ));
        }
        let mut reader = BufReader::new(manifest);
        let mut nodes = open_regular_at(index, "nodes.bin", false, false)?;
        reader
            .seek(SeekFrom::Start(range.start_offset))
            .map_err(substrate)?;
        let count = usize::try_from(range.end_index - range.start_index)
            .map_err(|error| integrity("filesystem_manifest_invalid", error.to_string()))?;
        let mut entries = Vec::with_capacity(count);
        let mut inclusions = Vec::with_capacity(count);
        let mut byte_offset = range.start_offset;
        for position in range.start_index..range.end_index {
            let expected_end = Self::manifest_offset(offsets, position + 1)?;
            let line_size = expected_end.checked_sub(byte_offset).ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest offsets are not monotonic".to_owned(),
                )
            })?;
            if line_size == 0 || line_size > MAX_MANIFEST_ENTRY_BYTES as u64 {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    format!(
                        "filesystem manifest entry exceeds {MAX_MANIFEST_ENTRY_BYTES} canonical bytes"
                    ),
                ));
            }
            let mut line = vec![
                0_u8;
                usize::try_from(line_size).map_err(|error| integrity(
                    "filesystem_manifest_invalid",
                    error.to_string()
                ))?
            ];
            reader.read_exact(&mut line).map_err(substrate)?;
            let payload = line.strip_suffix(b"\n").ok_or_else(|| {
                integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest line does not match its indexed byte range".to_owned(),
                )
            })?;
            let entry: ResourceEntry = cymule_core::decode_json(payload)
                .map_err(|error| integrity("filesystem_manifest_invalid", error.to_string()))?;
            if canonical_manifest_entry_bytes(&entry)?.as_slice() != payload {
                return Err(integrity(
                    "filesystem_manifest_invalid",
                    "filesystem manifest entry bytes are not canonical".to_owned(),
                ));
            }
            entries.push(entry);
            inclusions.push(Self::manifest_inclusion(
                &mut nodes,
                descriptor.entry_count,
                position,
            )?);
            byte_offset = expected_end;
        }
        if byte_offset != range.end_offset {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest page did not close its indexed range".to_owned(),
            ));
        }
        Ok((entries, inclusions))
    }

    fn read_manifest_predecessor(
        manifest: &mut File,
        index: &File,
        offsets: &mut File,
        descriptor: &ResourceManifestDescriptor,
        start_index: u64,
    ) -> ResourceResult<Option<ManifestPredecessorProof>> {
        let Some(position) = start_index.checked_sub(1) else {
            return Ok(None);
        };
        let start_offset = Self::manifest_offset(offsets, position)?;
        let end_offset = Self::manifest_offset(offsets, start_index)?;
        let (mut entries, mut inclusions) = Self::read_manifest_page(
            manifest,
            index,
            offsets,
            descriptor,
            ManifestPageRange {
                start_index: position,
                end_index: start_index,
                start_offset,
                end_offset,
            },
        )?;
        if entries.len() != 1 || inclusions.len() != 1 {
            return Err(integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest predecessor is not one exact indexed entry".to_owned(),
            ));
        }
        Ok(Some(ManifestPredecessorProof {
            entry: entries.pop().expect("one predecessor entry"),
            inclusion: inclusions.pop().expect("one predecessor inclusion"),
        }))
    }

    fn load_record(&self, upload_id: &str) -> ResourceResult<UploadRecord> {
        let mut file = open_regular_at(
            &self.directories.uploads,
            &Self::record_name(upload_id)?,
            false,
            false,
        )?;
        let bytes = read_bounded(
            &mut file,
            MAX_UPLOAD_RECORD_BYTES,
            "filesystem_upload_record_invalid",
        )?;
        let record: UploadRecord = cymule_core::decode_json(&bytes).map_err(core_integrity)?;
        if cymule_core::canonical_bytes(&record)
            .map_err(core_error)?
            .as_slice()
            != bytes.as_slice()
        {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem upload record is not canonical".to_owned(),
            ));
        }
        self.verify_upload_record(&record).map_err(|error| {
            integrity(
                "filesystem_upload_record_invalid",
                format!("filesystem upload record validation failed: {error}"),
            )
        })?;
        if record.upload_id != upload_id {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem upload record path changed".to_owned(),
            ));
        }
        Ok(record)
    }

    fn publication_for_record(
        &self,
        record: &UploadRecord,
    ) -> ResourceResult<Option<ResourcePublication>> {
        let Some(retained) = &record.publication else {
            return Ok(None);
        };
        let manifest_shape = matches!(
            record.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        );
        if manifest_shape != retained.manifest.is_some() {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem upload publication changed its manifest shape".to_owned(),
            ));
        }
        if let Some(manifest) = &retained.manifest {
            manifest.verify()?;
            if manifest.digest != retained.digest || manifest.size != retained.size {
                return Err(integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem upload publication changed its manifest identity".to_owned(),
                ));
            }
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
            manifest: retained.manifest.clone(),
            annotations: record.intent.annotations.clone(),
        }
        .seal()
        .map_err(|error| integrity("filesystem_upload_record_invalid", error.to_string()))?;
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
        publication.verify()?;
        Ok(Some(publication))
    }

    fn verify_upload_record_budget(&self, record: &UploadRecord) -> ResourceResult<()> {
        let mut terminal = record.clone();
        terminal.state = UploadState::Committed;
        terminal.committed_length = cymule_core::MAX_EXACT_INTEGER;
        let manifest = if matches!(
            terminal.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) {
            let root_digest = format!("sha256:{}", "0".repeat(64));
            Some(ResourceManifestDescriptor {
                manifest_version: cymule_resource::RESOURCE_MANIFEST_VERSION.to_owned(),
                media_type: DIRECTORY_MEDIA_TYPE.to_owned(),
                digest: resource_manifest_descriptor_id(
                    DIRECTORY_MEDIA_TYPE,
                    cymule_core::MAX_EXACT_INTEGER,
                    cymule_core::MAX_EXACT_INTEGER,
                    &root_digest,
                )?,
                size: cymule_core::MAX_EXACT_INTEGER,
                entry_count: cymule_core::MAX_EXACT_INTEGER,
                root_digest,
            })
        } else {
            None
        };
        terminal.publication = Some(UploadPublication {
            digest: manifest.as_ref().map_or_else(
                || format!("sha256:{}", "0".repeat(64)),
                |manifest| manifest.digest.clone(),
            ),
            size: cymule_core::MAX_EXACT_INTEGER,
            manifest,
        });
        let plan = Self::expected_cleanup_plan(&terminal)?;
        terminal.cleanup_receipt = Some(plan.receipt()?);
        terminal.cleanup_plan = Some(plan);
        self.verify_upload_record(&terminal)?;
        let bytes = cymule_core::canonical_bytes(&terminal).map_err(core_error)?;
        if bytes.len() as u64 > MAX_UPLOAD_RECORD_BYTES {
            return Err(ResourceError::Validation(format!(
                "filesystem write metadata cannot fit its {MAX_UPLOAD_RECORD_BYTES}-byte terminal record budget"
            )));
        }
        Ok(())
    }

    fn verify_upload_record(&self, record: &UploadRecord) -> ResourceResult<()> {
        if record.record_version != UPLOAD_RECORD_VERSION {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                format!(
                    "unsupported filesystem upload record version {}",
                    record.record_version
                ),
            ));
        }
        record.intent.validate()?;
        if record.store_binding != self.binding
            || record.upload_id != self.upload_id(&record.intent.write_id)?
            || record.committed_length > cymule_core::MAX_EXACT_INTEGER
        {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem upload record identity, binding, or frontier changed".to_owned(),
            ));
        }
        if matches!(record.state, UploadState::Open | UploadState::Aborted)
            != record.publication.is_none()
        {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem upload state does not match its publication intent".to_owned(),
            ));
        }
        if let Some(publication) = self.publication_for_record(record)? {
            if publication.resource.shape != record.intent.shape
                || publication.resource.media_type != record.intent.media_type
                || publication.resource.annotations != record.intent.annotations
                || publication.resource.integrity.content_size() != Some(record.committed_length)
                || publication.locators.resolver_binding != self.binding
            {
                return Err(integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem upload publication changed its admitted write intent".to_owned(),
                ));
            }
            let _ = self.resource_name(&publication.resource, &publication.locators)?;
        }
        match (&record.cleanup_plan, &record.cleanup_receipt) {
            (None, None) => {}
            (None, Some(_)) => {
                return Err(integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem cleanup receipt lacks its immutable plan".to_owned(),
                ));
            }
            (Some(plan), receipt) => {
                Self::verify_cleanup_plan(record, plan)?;
                if let Some(receipt) = receipt {
                    receipt.verify()?;
                    if receipt.plan != *plan {
                        return Err(integrity(
                            "filesystem_upload_record_invalid",
                            "filesystem cleanup receipt changed its immutable plan".to_owned(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn store_record(&self, record: &UploadRecord) -> ResourceResult<()> {
        self.ensure_writable()?;
        self.verify_upload_record(record)?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        if bytes.len() as u64 > MAX_UPLOAD_RECORD_BYTES {
            return Err(ResourceError::Validation(format!(
                "filesystem upload record exceeds its {MAX_UPLOAD_RECORD_BYTES}-byte physical bound"
            )));
        }
        let destination = Self::record_name(&record.upload_id)?;
        let staging = format!("record-{}", Self::upload_key(&record.upload_id)?);
        write_synced_at(&self.directories.staging, &staging, &bytes)?;
        renameat(
            &self.directories.staging,
            staging.as_str(),
            &self.directories.uploads,
            destination.as_str(),
        )
        .map_err(substrate)?;
        self.directories.uploads.sync_all().map_err(substrate)?;
        self.directories.staging.sync_all().map_err(substrate)
    }

    fn cleanup_upload_files(
        &self,
        session: &ResourceWriteSession,
        record: &mut UploadRecord,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.ensure_writable()?;
        let data = Self::data_name(&session.upload_id)?;
        let staging = format!("object-{}", Self::upload_key(&session.upload_id)?);
        let manifest_index_staging = Self::manifest_index_staging_name(session)?;
        let directory_sort_staging = Self::directory_sort_staging_name(session)?;
        if let Some(receipt) = &record.cleanup_receipt {
            receipt.verify()?;
            return Ok(receipt.clone());
        }
        if record.cleanup_plan.is_none() {
            record.cleanup_plan = Some(Self::expected_cleanup_plan(record)?);
            self.store_record(record)?;
        }
        let plan = record.cleanup_plan.clone().ok_or_else(|| {
            integrity(
                "filesystem_cleanup_invalid",
                "filesystem cleanup plan was not persisted".to_owned(),
            )
        })?;
        Self::verify_cleanup_plan(record, &plan)?;
        for target in &plan.targets {
            if target.identifier == format!("uploads/{data}") {
                remove_file_at(&self.directories.uploads, &data)?;
            } else if target.identifier == format!("staging/{staging}") {
                remove_file_at(&self.directories.staging, &staging)?;
            } else if target.identifier == format!("staging/{manifest_index_staging}") {
                remove_manifest_index_staging(&self.directories.staging, &manifest_index_staging)?;
            } else if target.identifier == format!("staging/{directory_sort_staging}") {
                remove_directory_sort_staging(&self.directories.staging, &directory_sort_staging)?;
            } else {
                return Err(integrity(
                    "filesystem_cleanup_invalid",
                    "filesystem cleanup plan contains a foreign target".to_owned(),
                ));
            }
        }
        self.directories.uploads.sync_all().map_err(substrate)?;
        self.directories.staging.sync_all().map_err(substrate)?;
        for target in &plan.targets {
            let absent = if target.identifier == format!("uploads/{data}") {
                !entry_exists(&self.directories.uploads, &data)?
            } else if target.identifier == format!("staging/{staging}") {
                !entry_exists(&self.directories.staging, &staging)?
            } else if target.identifier == format!("staging/{manifest_index_staging}") {
                !entry_exists(&self.directories.staging, &manifest_index_staging)?
            } else if target.identifier == format!("staging/{directory_sort_staging}") {
                !entry_exists(&self.directories.staging, &directory_sort_staging)?
            } else {
                return Err(integrity(
                    "filesystem_cleanup_invalid",
                    "filesystem cleanup plan contains a foreign target".to_owned(),
                ));
            };
            if !absent {
                return Err(integrity(
                    "filesystem_cleanup_invalid",
                    "filesystem cleanup target remains present".to_owned(),
                ));
            }
        }
        let receipt = plan.receipt()?;
        receipt.verify()?;
        record.cleanup_receipt = Some(receipt.clone());
        self.store_record(record)?;
        Ok(receipt)
    }

    fn verify_cleanup_plan(
        record: &UploadRecord,
        plan: &ResourceCleanupPlan,
    ) -> ResourceResult<()> {
        if plan.write_id != record.intent.write_id
            || plan.upload_id != record.upload_id
            || plan.store_binding != record.store_binding
            || !matches!(
                record.state,
                UploadState::Committed | UploadState::Deleted | UploadState::Aborted
            )
            || *plan != Self::expected_cleanup_plan(record)?
        {
            return Err(integrity(
                "filesystem_cleanup_invalid",
                "filesystem cleanup plan changed its upload authority".to_owned(),
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
        let key = Self::upload_key(&record.upload_id)?;
        let mut identifiers = vec![
            format!("uploads/{key}.data"),
            format!("staging/object-{key}"),
            format!("staging/manifest-index-{key}"),
        ];
        if matches!(
            record.intent.shape,
            ResourceShape::Directory | ResourceShape::Collection | ResourceShape::Snapshot
        ) {
            identifiers.push(format!("staging/directory-sort-{key}"));
        }
        let mut targets = identifiers
            .into_iter()
            .map(|identifier| ResourceCleanupTarget {
                kind: ResourceCleanupTargetKind::StagingObject,
                identifier,
            })
            .collect::<Vec<_>>();
        targets.sort();
        ResourceCleanupPlan::new(&session, targets)
    }

    fn open_acknowledged_upload_data(
        &self,
        session: &ResourceWriteSession,
        committed_length: u64,
    ) -> ResourceResult<File> {
        let name = Self::data_name(&session.upload_id)?;
        if !entry_exists(&self.directories.uploads, &name)? {
            let data = create_regular_at(&self.directories.uploads, &name)?;
            data.sync_all().map_err(substrate)?;
            self.directories.uploads.sync_all().map_err(substrate)?;
        }
        let data = open_regular_at(&self.directories.uploads, &name, false, true)?;
        let length = data.metadata().map_err(substrate)?.len();
        if length < committed_length {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                format!(
                    "filesystem upload lost acknowledged bytes: retained {length}, committed {committed_length}"
                ),
            ));
        }
        if length > committed_length {
            data.set_len(committed_length).map_err(substrate)?;
            data.sync_all().map_err(substrate)?;
        }
        Ok(data)
    }

    fn finish_publication(
        &mut self,
        session: &ResourceWriteSession,
        mut record: UploadRecord,
        publication: ResourcePublication,
    ) -> ResourceResult<ResourcePublication> {
        let family = ResourceRetentionFamily::from_publication(&publication)?;
        let retention_claim = self.claim_retention(&family)?;
        if retention_claim.deleted {
            self.close_deleted_upload(session, &mut record)?;
            return Err(resource_deleted(&family));
        }
        self.converge_publishing(session, &record, &publication, &retention_claim)?;
        record.state = UploadState::Committed;
        self.store_record(&record)?;
        self.cleanup_upload_files(session, &mut record)?;
        Ok(publication)
    }

    fn close_deleted_upload(
        &self,
        session: &ResourceWriteSession,
        record: &mut UploadRecord,
    ) -> ResourceResult<()> {
        if record.state != UploadState::Deleted {
            record.state = UploadState::Deleted;
            self.store_record(record)?;
        }
        self.cleanup_upload_files(session, record)?;
        Ok(())
    }

    fn reconcile_deleted_upload(
        &self,
        session: &ResourceWriteSession,
        record: &mut UploadRecord,
    ) -> ResourceResult<Option<ResourceRetentionFamily>> {
        if !matches!(
            record.state,
            UploadState::Publishing | UploadState::Committed | UploadState::Deleted
        ) {
            return Ok(None);
        }
        let publication = self.publication_for_record(record)?.ok_or_else(|| {
            integrity(
                "filesystem_upload_record_invalid",
                "published filesystem upload has no publication".to_owned(),
            )
        })?;
        let family = ResourceRetentionFamily::from_publication(&publication)?;
        let tombstoned = self.family_is_deleted(&family)?;
        if record.state == UploadState::Deleted && !tombstoned {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "deleted filesystem upload lacks its permanent retention-family tombstone"
                    .to_owned(),
            ));
        }
        if !tombstoned {
            return Ok(None);
        }
        self.close_deleted_upload(session, record)?;
        Ok(Some(family))
    }

    fn converge_publishing(
        &mut self,
        session: &ResourceWriteSession,
        record: &UploadRecord,
        publication: &ResourcePublication,
        retention_claim: &RetentionClaim,
    ) -> ResourceResult<()> {
        let family = ResourceRetentionFamily::from_publication(publication)?;
        if retention_claim.family != family {
            return Err(integrity(
                "filesystem_retention_family_invalid",
                "filesystem Publishing claim does not match its physical retention family"
                    .to_owned(),
            ));
        }
        if retention_claim.deleted {
            return Err(resource_deleted(&family));
        }
        let retained_publication = self.publication_for_record(record)?;
        if record.state != UploadState::Publishing
            || retained_publication.as_ref() != Some(publication)
        {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem content publication lacks its durable Publishing intent".to_owned(),
            ));
        }
        let ResourceIntegrity::Content { .. } = &publication.resource.integrity else {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem Publishing intent is not content addressed".to_owned(),
            ));
        };
        let object_name = self.resource_name(&publication.resource, &publication.locators)?;
        if entry_exists(&self.directories.objects, &object_name)? {
            let mut object =
                open_regular_at(&self.directories.objects, &object_name, false, false)?;
            verify_resource_file(&mut object, &publication.resource)?;
            object.sync_all().map_err(substrate)?;
        } else {
            let data_name = Self::data_name(&session.upload_id)?;
            let mut data = open_regular_at(&self.directories.uploads, &data_name, false, false)?;
            verify_resource_file(&mut data, &publication.resource)?;
            data.seek(SeekFrom::Start(0)).map_err(substrate)?;
            let staging_name = format!("object-{}", Self::upload_key(&session.upload_id)?);
            remove_file_at(&self.directories.staging, &staging_name)?;
            let mut staging = create_regular_at(&self.directories.staging, &staging_name)?;
            std::io::copy(&mut data, &mut staging).map_err(substrate)?;
            staging.sync_all().map_err(substrate)?;
            self.directories.staging.sync_all().map_err(substrate)?;
            match linkat(
                &self.directories.staging,
                staging_name.as_str(),
                &self.directories.objects,
                object_name.as_str(),
                AtFlags::empty(),
            ) {
                Ok(()) | Err(Errno::EEXIST) => {}
                Err(error) => return Err(substrate(error)),
            }
            self.directories.objects.sync_all().map_err(substrate)?;
            remove_file_at(&self.directories.staging, &staging_name)?;
            self.directories.staging.sync_all().map_err(substrate)?;
            let mut object =
                open_regular_at(&self.directories.objects, &object_name, false, false)?;
            verify_resource_file(&mut object, &publication.resource)?;
            object.sync_all().map_err(substrate)?;
        }
        self.directories.objects.sync_all().map_err(substrate)?;
        if let Some(descriptor) = &publication.resource.manifest {
            self.persist_manifest_index(session, descriptor)?;
        }
        Ok(())
    }

    fn verify_publishing_chunk_retry(
        &self,
        session: &ResourceWriteSession,
        record: &UploadRecord,
        offset: u64,
        end: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        if end > record.committed_length {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem write retry exceeds its publishing frontier".to_owned(),
            ));
        }
        let mut file = open_regular_at(
            &self.directories.uploads,
            &Self::data_name(&session.upload_id)?,
            false,
            false,
        )?;
        if file.metadata().map_err(substrate)?.len() != record.committed_length {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem Publishing data changed its acknowledged length".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
        let mut retained = vec![0_u8; bytes.len()];
        file.read_exact(&mut retained).map_err(substrate)?;
        if retained != bytes {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem write retry changed acknowledged publishing bytes".to_owned(),
            ));
        }
        file.sync_all().map_err(substrate)?;
        self.directories.uploads.sync_all().map_err(substrate)
    }

    fn verify_committed_chunk_retry(
        &self,
        publication: &ResourcePublication,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        publication.verify()?;
        let name = self.resolvable_resource_name(&publication.resource, &publication.locators)?;
        let ResourceIntegrity::Content { size, .. } = &publication.resource.integrity else {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                "filesystem Resource is not content verified".to_owned(),
            ));
        };
        let mut file = open_regular_at(&self.directories.objects, &name, false, false)?;
        if file.metadata().map_err(substrate)?.len() != *size {
            return Err(integrity(
                "filesystem_resource_integrity_failed",
                "filesystem object size changed".to_owned(),
            ));
        }
        let end = offset
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation(
                    "filesystem chunk exceeds the shared exact-integer range".to_owned(),
                )
            })?;
        if end > *size {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem write retry exceeds committed bytes".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
        let mut retained = vec![0_u8; bytes.len()];
        file.read_exact(&mut retained).map_err(substrate)?;
        if retained != bytes {
            // Diagnose corruption only on a mismatch; commit verifies the whole
            // publication once after any number of equal bounded chunk retries.
            verify_resource_file(&mut file, &publication.resource)?;
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem write retry changed committed bytes".to_owned(),
            ));
        }
        file.sync_all().map_err(substrate)?;
        self.directories.objects.sync_all().map_err(substrate)
    }

    fn resource_name(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<String> {
        locators.verify_for(resource)?;
        if locators.resolver_binding != self.binding {
            return Err(ResourceError::NotFound(format!(
                "Resource {} has no location for binding {}",
                resource.resource_id, self.binding
            )));
        }
        let expected = resource.integrity.content_digest().ok_or_else(|| {
            ResourceError::Validation(
                "filesystem resources require content-addressed integrity".to_owned(),
            )
        })?;
        let reference = match locators.locations.as_slice() {
            [ResourceLocation::Opaque { reference }] if reference == expected => reference,
            [ResourceLocation::Opaque { .. }] => {
                return Err(integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem locator does not match the Resource content digest".to_owned(),
                ));
            }
            _ => {
                return Err(ResourceError::Validation(
                    "filesystem resources require exactly one digest locator".to_owned(),
                ));
            }
        };
        let digest = reference
            .strip_prefix("sha256:")
            .expect("verified Resource content digest has SHA-256 prefix");
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ResourceError::Validation(
                "filesystem reference digest is malformed".to_owned(),
            ));
        }
        Ok(digest.to_owned())
    }

    fn retention_family(
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceRetentionFamily> {
        ResourceRetentionFamily::from_publication(&ResourcePublication {
            resource: resource.clone(),
            locators: locators.clone(),
        })
    }

    fn resolvable_resource_name(
        &self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<String> {
        let name = self.resource_name(resource, locators)?;
        let family = Self::retention_family(resource, locators)?;
        if self.family_is_deleted(&family)? {
            return Err(ResourceError::NotFound(format!(
                "Resource {} was deleted from retention family {}",
                resource.resource_id, family.retention_key
            )));
        }
        Ok(name)
    }
}

impl ResourceCatalogStore for FsResourceStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        self.ensure_writable()?;
        record.verify()?;
        let bytes = cymule_core::canonical_bytes(record).map_err(core_error)?;
        let _claim = self.claim_catalog(&record.namespace, &record.key)?;
        let destination = Self::catalog_name(&record.namespace, &record.key);
        let token = Self::catalog_token(&record.namespace, &record.key);
        let staging = format!("catalog-{token}.next");
        if entry_exists(&self.directories.catalog, &destination)? {
            let mut retained =
                open_regular_at(&self.directories.catalog, &destination, false, false)?;
            let existing: ResourceCatalogRecord = cymule_core::decode_json(&read_bounded(
                &mut retained,
                MAX_RESOURCE_CATALOG_RECORD_BYTES,
                "filesystem_catalog_invalid",
            )?)
            .map_err(core_integrity)?;
            existing.verify()?;
            if existing != *record {
                return Err(conflict(
                    "filesystem_catalog_conflict",
                    format!(
                        "filesystem catalog record {}/{} has conflicting content",
                        record.namespace, record.key
                    ),
                ));
            }
            retained.sync_all().map_err(substrate)?;
            self.directories.catalog.sync_all().map_err(substrate)?;
            remove_file_at(&self.directories.staging, &staging)?;
            self.directories.staging.sync_all().map_err(substrate)?;
            self.directories.root.sync_all().map_err(substrate)?;
            return Ok(());
        }
        write_synced_at(&self.directories.staging, &staging, &bytes)?;
        renameat(
            &self.directories.staging,
            staging.as_str(),
            &self.directories.catalog,
            destination.as_str(),
        )
        .map_err(substrate)?;
        self.directories.catalog.sync_all().map_err(substrate)?;
        self.directories.staging.sync_all().map_err(substrate)?;
        self.directories.root.sync_all().map_err(substrate)?;
        Ok(())
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        let _ = ResourceCatalogRecord::new(namespace, key, Vec::new())?;
        let name = Self::catalog_name(namespace, key);
        let Some(mut file) = open_regular_at_optional(&self.directories.catalog, &name)? else {
            return Ok(None);
        };
        let record: ResourceCatalogRecord = cymule_core::decode_json(&read_bounded(
            &mut file,
            MAX_RESOURCE_CATALOG_RECORD_BYTES,
            "filesystem_catalog_invalid",
        )?)
        .map_err(core_integrity)?;
        record.verify()?;
        if record.namespace != namespace || record.key != key {
            return Err(integrity(
                "filesystem_catalog_invalid",
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
        self.ensure_writable()?;
        intent.validate()?;
        let upload_id = self.upload_id(&intent.write_id)?;
        let candidate = UploadRecord {
            record_version: UPLOAD_RECORD_VERSION.to_owned(),
            intent: intent.clone(),
            upload_id: upload_id.clone(),
            store_binding: self.binding.clone(),
            state: UploadState::Open,
            committed_length: 0,
            publication: None,
            cleanup_plan: None,
            cleanup_receipt: None,
        };
        self.verify_upload_record_budget(&candidate)?;
        let _claim = self.claim(&upload_id)?;
        let name = Self::record_name(&upload_id)?;
        if entry_exists(&self.directories.uploads, &name)? {
            let mut record = self.load_record(&upload_id)?;
            if record.intent != *intent || record.state == UploadState::Aborted {
                return Err(conflict(
                    "filesystem_upload_conflict",
                    format!("filesystem write ID {} was reused", intent.write_id),
                ));
            }
            if record.state == UploadState::Deleted {
                let session = ResourceWriteSession {
                    write_id: intent.write_id.clone(),
                    upload_id,
                    store_binding: self.binding.clone(),
                };
                let family = self
                    .reconcile_deleted_upload(&session, &mut record)?
                    .ok_or_else(|| {
                        integrity(
                            "filesystem_upload_record_invalid",
                            "deleted filesystem upload did not retain its physical tombstone"
                                .to_owned(),
                        )
                    })?;
                return Err(resource_deleted(&family));
            }
        } else {
            self.store_record(&candidate)?;
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
        self.ensure_writable()?;
        self.validate_session(session)?;
        if bytes.is_empty() || bytes.len() > MAX_WRITE_CHUNK {
            return Err(ResourceError::Validation(
                "filesystem write session or chunk is invalid".to_owned(),
            ));
        }
        let size = u64::try_from(bytes.len()).map_err(|_| {
            ResourceError::Validation("filesystem chunk exceeds platform bounds".to_owned())
        })?;
        let end = offset
            .checked_add(size)
            .filter(|end| *end <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation(
                    "filesystem chunk exceeds the shared exact-integer range".to_owned(),
                )
            })?;
        let _claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.write_id() != session.write_id {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem upload identity changed".to_owned(),
            ));
        }
        if let Some(family) = self.reconcile_deleted_upload(session, &mut record)? {
            return Err(resource_deleted(&family));
        }
        if record.state == UploadState::Committed {
            let publication = self.publication_for_record(&record)?.ok_or_else(|| {
                integrity(
                    "filesystem_upload_record_invalid",
                    "committed filesystem upload has no Resource publication".to_owned(),
                )
            })?;
            return self.verify_committed_chunk_retry(&publication, offset, bytes);
        }
        if record.state == UploadState::Publishing {
            return self.verify_publishing_chunk_retry(session, &record, offset, end, bytes);
        }
        if record.state != UploadState::Open {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem upload is not open for this write".to_owned(),
            ));
        }
        let name = Self::data_name(&session.upload_id)?;
        let created = !entry_exists(&self.directories.uploads, &name)?;
        let mut file = open_regular_at(&self.directories.uploads, &name, true, true)?;
        let mut length = file.metadata().map_err(substrate)?.len();
        if length < record.committed_length {
            return Err(integrity(
                "filesystem_upload_record_invalid",
                format!(
                    "filesystem upload lost acknowledged bytes: retained {length}, committed {}",
                    record.committed_length
                ),
            ));
        }
        if length > record.committed_length {
            file.set_len(record.committed_length).map_err(substrate)?;
            file.sync_all().map_err(substrate)?;
            length = record.committed_length;
        }
        if created {
            self.directories.uploads.sync_all().map_err(substrate)?;
        }
        if offset < length {
            if end > length {
                return Err(conflict(
                    "filesystem_upload_conflict",
                    "filesystem chunk overlaps the retained frontier".to_owned(),
                ));
            }
            file.seek(SeekFrom::Start(offset)).map_err(substrate)?;
            let mut retained = vec![0_u8; bytes.len()];
            file.read_exact(&mut retained).map_err(substrate)?;
            if retained != bytes {
                return Err(conflict(
                    "filesystem_upload_conflict",
                    "filesystem chunk retry changed retained bytes".to_owned(),
                ));
            }
            file.sync_all().map_err(substrate)?;
            self.directories.uploads.sync_all().map_err(substrate)?;
            return Ok(());
        }
        if offset != length {
            return Err(conflict(
                "filesystem_upload_conflict",
                format!("filesystem upload expected offset {length}, received {offset}"),
            ));
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
        self.commit_write_with_import(session, None)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.ensure_writable()?;
        self.validate_session(session)?;
        let claim = self.claim(&session.upload_id)?;
        let mut record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem abort identity changed".to_owned(),
            ));
        }
        if matches!(
            record.state,
            UploadState::Publishing | UploadState::Committed | UploadState::Deleted
        ) {
            if record.state == UploadState::Publishing {
                drop(claim);
                let publication = self.commit_write(session);
                let _claim = self.claim(&session.upload_id)?;
                record = self.load_record(&session.upload_id)?;
                if matches!(record.state, UploadState::Committed | UploadState::Deleted) {
                    return self.cleanup_upload_files(session, &mut record);
                }
                publication?;
                return Err(integrity(
                    "filesystem_upload_record_invalid",
                    "filesystem publication returned without a terminal upload state".to_owned(),
                ));
            }
            return self.cleanup_upload_files(session, &mut record);
        }
        record.state = UploadState::Aborted;
        self.store_record(&record)?;
        self.cleanup_upload_files(session, &mut record)
    }

    fn cleanup_receipt(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<Option<ResourceCleanupReceipt>> {
        self.validate_session(session)?;
        let record = self.load_record(&session.upload_id)?;
        if record.intent.write_id != session.write_id {
            return Err(conflict(
                "filesystem_upload_conflict",
                "filesystem cleanup receipt identity changed".to_owned(),
            ));
        }
        Ok(record.cleanup_receipt)
    }
}

impl ArtifactResolver for FsResourceStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        resource.verify()?;
        let name = self.resolvable_resource_name(resource, locators)?;
        if matches!(resource.integrity, ResourceIntegrity::Content { .. }) {
            let mut file = open_regular_at(&self.directories.objects, &name, false, false)?;
            verify_resource_file(&mut file, resource)?;
        }
        if let Some(manifest) = &resource.manifest {
            self.load_manifest_index_header(manifest)?;
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
        if max_bytes == 0 || max_bytes > MAX_READ_CHUNK {
            return Err(ResourceError::Validation(format!(
                "filesystem read limit must be 1..={MAX_READ_CHUNK}"
            )));
        }
        resource.verify()?;
        locators.verify_for(resource)?;
        let name = self.resolvable_resource_name(resource, locators)?;
        let mut file = open_regular_at(&self.directories.objects, &name, false, false)?;
        let size = file.metadata().map_err(substrate)?.len();
        if !matches!(resource.integrity, ResourceIntegrity::Content { size: expected, .. } if expected == size)
        {
            return Err(integrity(
                "filesystem_resource_invalid",
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
            || limit > MAX_LIST_PAGE
        {
            return Err(ResourceError::Validation(format!(
                "filesystem list requires a collection shape and limit 1..={MAX_LIST_PAGE}"
            )));
        }
        resource.verify()?;
        locators.verify_for(resource)?;
        let descriptor = resource.manifest.as_ref().ok_or_else(|| {
            ResourceError::Validation(
                "filesystem exact listing requires a manifest descriptor".to_owned(),
            )
        })?;
        let manifest_name = self.resolvable_resource_name(resource, locators)?;
        let mut manifest =
            open_regular_at(&self.directories.objects, &manifest_name, false, false)?;
        if manifest.metadata().map_err(substrate)?.len() != descriptor.size {
            return Err(integrity(
                "filesystem_resource_invalid",
                "filesystem manifest size changed".to_owned(),
            ));
        }
        let (index, _) = self.load_manifest_index_header(descriptor)?;
        let mut offsets = open_regular_at(&index, "offsets.bin", false, false)?;
        let start_index = match cursor {
            Some(cursor) => {
                let cursor = ResourceListCursor::decode(cursor)?;
                if cursor.resource_id != resource.resource_id
                    || cursor.manifest_digest != descriptor.digest
                    || cursor.resolver_binding != self.binding
                    || cursor.request_limit != limit
                {
                    return Err(ResourceError::Validation(
                        "filesystem manifest cursor does not match this exact list request"
                            .to_owned(),
                    ));
                }
                cursor.next_index
            }
            None => 0,
        };
        if start_index > descriptor.entry_count {
            return Err(ResourceError::Validation(
                "filesystem manifest cursor exceeds entry count".to_owned(),
            ));
        }
        let start_offset = Self::manifest_offset(&mut offsets, start_index)?;
        let range =
            Self::manifest_page_end(&mut offsets, descriptor, start_index, start_offset, limit)?;
        let predecessor = Self::read_manifest_predecessor(
            &mut manifest,
            &index,
            &mut offsets,
            descriptor,
            start_index,
        )?;
        let (entries, inclusions) =
            Self::read_manifest_page(&mut manifest, &index, &mut offsets, descriptor, range)?;
        let publication = ResourcePublication {
            resource: resource.clone(),
            locators: locators.clone(),
        };
        let next_cursor = if range.end_index < descriptor.entry_count {
            Some(ResourceListCursor::for_page(
                &publication,
                cursor,
                limit,
                start_index,
                &entries,
            )?)
        } else {
            None
        };
        let proof = ResourceListProof::from_inclusions(
            descriptor,
            start_index,
            predecessor,
            inclusions,
            cursor,
            next_cursor.as_deref(),
        )?;
        proof.verify_page(descriptor, &entries, cursor, next_cursor.as_deref())?;
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

    fn delete_and_verify_absent(&mut self, target: &ResourceDeletionTarget) -> ResourceResult<()> {
        self.ensure_writable()?;
        target.verify()?;
        if target.subject.family.store_binding != self.binding {
            return Err(conflict(
                "filesystem_cleanup_conflict",
                "filesystem deleter does not own the durable deletion target".to_owned(),
            ));
        }
        let mut retention_claim = self.claim_retention(&target.subject.family)?;
        let name = target
            .subject
            .family
            .content_digest
            .strip_prefix("sha256:")
            .expect("verified deletion target has a SHA-256 digest");
        let mut retained = open_regular_at_optional(&self.directories.objects, name)?;
        if let Some(file) = &mut retained {
            verify_deletion_target_file(file, target)?;
        }
        self.persist_deletion_tombstone(&mut retention_claim)?;
        #[cfg(test)]
        if FAIL_AFTER_DELETION_TOMBSTONE.swap(false, Ordering::SeqCst) {
            return Err(substrate_with_code(
                "filesystem_test_interrupted_after_deletion_tombstone",
                "injected interruption after durable deletion tombstone readback",
            ));
        }
        if retained.is_some() {
            remove_file_at(&self.directories.objects, name)?;
        }
        self.directories.objects.sync_all().map_err(substrate)?;
        let mut manifest_absent = true;
        if let Some(descriptor) = &target.manifest {
            let index_name = Self::manifest_index_name(descriptor)?;
            remove_manifest_index_directory(&self.directories.manifest_indexes, &index_name)?;
            self.directories
                .manifest_indexes
                .sync_all()
                .map_err(substrate)?;
            let catalog_name = Self::catalog_name(MANIFEST_INDEX_VERSION, &descriptor.digest);
            remove_file_at(&self.directories.catalog, &catalog_name)?;
            self.directories.catalog.sync_all().map_err(substrate)?;
            manifest_absent = !entry_exists(&self.directories.manifest_indexes, &index_name)?
                && !entry_exists(&self.directories.catalog, &catalog_name)?;
        }
        if entry_exists(&self.directories.objects, name)? || !manifest_absent {
            return Err(integrity(
                "filesystem_cleanup_invalid",
                "filesystem deletion target remains present after provider readback".to_owned(),
            ));
        }
        Ok(())
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

fn validate_binding(binding: &str) -> ResourceResult<()> {
    cymule_core::validate_identity("filesystem Resource binding", binding)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn next_directory_import_depth(depth: usize) -> ResourceResult<usize> {
    if depth >= MAX_DIRECTORY_IMPORT_DEPTH {
        return Err(ResourceError::Validation(format!(
            "filesystem_import_depth_exceeded: filesystem directory import exceeds {MAX_DIRECTORY_IMPORT_DEPTH} nested child directories"
        )));
    }
    Ok(depth + 1)
}

fn child_write_id(parent_write_id: &str, child_name: &str) -> ResourceResult<String> {
    cymule_core::validate_identity("filesystem parent write", parent_write_id)
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    validate_name(child_name)?;
    cymule_core::content_id(
        CHILD_WRITE_ID_VERSION,
        &ChildWriteIdentity {
            parent_write_id,
            child_name,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn binding_namespace(binding: &str) -> String {
    hex_digest(&Sha256::digest(binding.as_bytes()))
}

fn verify_root_layout(root: &File) -> ResourceResult<()> {
    let allowed = FIXED_DIRECTORIES
        .into_iter()
        .chain(std::iter::once(PHYSICAL_LAYOUT_MARKER))
        .collect::<std::collections::BTreeSet<_>>();
    visit_directory_names(root, |entry| {
        if !allowed.contains(entry) {
            return Err(integrity(
                "filesystem_layout_invalid",
                format!("filesystem Resource root contains unsupported physical entry {entry:?}"),
            ));
        }
        Ok(())
    })
}

fn verify_binding_namespace_root(root: &File) -> ResourceResult<()> {
    visit_directory_names(root, |entry| {
        if entry.len() != 64
            || !entry
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(integrity(
                "filesystem_layout_invalid",
                format!("filesystem Resource binding namespace {entry:?} is malformed"),
            ));
        }
        let _ = open_directory_at(root, entry)?;
        Ok(())
    })
}

fn file_from_fd(fd: OwnedFd) -> File {
    fd.into()
}

fn open_directory(path: &Path) -> ResourceResult<File> {
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(substrate)?;
    let file = file_from_fd(descriptor);
    if !file.metadata().map_err(substrate)?.is_dir() {
        return Err(integrity(
            "filesystem_layout_invalid",
            "filesystem Resource root is not a directory".to_owned(),
        ));
    }
    Ok(file)
}

fn open_directory_at(parent: &File, name: &str) -> ResourceResult<File> {
    validate_fixed_name(name)?;
    let descriptor = openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::ENOENT => integrity(
            "filesystem_layout_invalid",
            format!("filesystem Resource directory {name:?} is missing"),
        ),
        Errno::ELOOP | Errno::ENOTDIR => integrity(
            "filesystem_layout_invalid",
            format!("filesystem Resource directory {name:?} is a symlink or non-directory"),
        ),
        other => substrate(other),
    })?;
    let file = file_from_fd(descriptor);
    if !file.metadata().map_err(substrate)?.is_dir() {
        return Err(integrity(
            "filesystem_layout_invalid",
            format!("filesystem Resource entry {name:?} is not a directory"),
        ));
    }
    Ok(file)
}

fn ensure_directory_at(parent: &File, name: &str) -> ResourceResult<()> {
    validate_fixed_name(name)?;
    match mkdirat(parent, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(Errno::EEXIST) => {}
        Err(error) => return Err(substrate(error)),
    }
    let directory = open_directory_at(parent, name)?;
    directory.sync_all().map_err(substrate)
}

fn open_regular_file(path: &Path) -> ResourceResult<File> {
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(substrate)?;
    let file = file_from_fd(descriptor);
    if !file.metadata().map_err(substrate)?.is_file() {
        return Err(ResourceError::Validation(
            "filesystem import accepts only a regular no-symlink file".to_owned(),
        ));
    }
    Ok(file)
}

fn open_import_child(parent: &File, name: &str) -> ResourceResult<File> {
    validate_name(name)?;
    let descriptor = openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| match error {
        Errno::ELOOP => {
            ResourceError::Validation(format!("filesystem import rejects symlink {name:?}"))
        }
        other => substrate(other),
    })?;
    let file = file_from_fd(descriptor);
    let metadata = file.metadata().map_err(substrate)?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(ResourceError::Validation(format!(
            "filesystem import rejects non-regular entry {name:?}"
        )));
    }
    Ok(file)
}

fn visit_directory_names(
    directory: &File,
    mut visit: impl FnMut(&str) -> ResourceResult<()>,
) -> ResourceResult<()> {
    let mut entries = Dir::from_fd(dup(directory).map_err(substrate)?).map_err(substrate)?;
    for entry in entries.iter() {
        let entry = entry.map_err(substrate)?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let name = raw
            .to_str()
            .map_err(|_| ResourceError::Validation("filesystem names must be UTF-8".to_owned()))?;
        validate_name(name)?;
        visit(name)?;
    }
    Ok(())
}

fn count_import_directory_entries(directory: &File) -> ResourceResult<u64> {
    let mut entries = Dir::from_fd(dup(directory).map_err(substrate)?).map_err(substrate)?;
    let mut count = 0_u64;
    for entry in entries.iter() {
        let entry = entry.map_err(substrate)?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let name = raw
            .to_str()
            .map_err(|_| ResourceError::Validation("filesystem names must be UTF-8".to_owned()))?;
        let child = open_import_child(directory, name)?;
        let metadata = child.metadata().map_err(substrate)?;
        if !metadata.is_file() && !metadata.is_dir() {
            return Err(ResourceError::Validation(format!(
                "unsupported filesystem entry {name:?}"
            )));
        }
        count = count
            .checked_add(1)
            .filter(|count| *count <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation(
                    "filesystem directory entry count exceeds the shared exact-integer range"
                        .to_owned(),
                )
            })?;
    }
    Ok(count)
}

fn directory_sort_run_name(pass: u64, index: u64) -> String {
    format!("run-{pass:020}-{index:020}")
}

fn is_directory_sort_run_name(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("run-") else {
        return false;
    };
    let Some((pass, index)) = rest.split_once('-') else {
        return false;
    };
    pass.len() == 20
        && index.len() == 20
        && pass.bytes().all(|byte| byte.is_ascii_digit())
        && index.bytes().all(|byte| byte.is_ascii_digit())
}

fn write_directory_sort_run(
    directory: &File,
    run_index: u64,
    names: &mut Vec<String>,
) -> ResourceResult<()> {
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(integrity(
            "filesystem_import_invalid",
            "filesystem directory enumeration contains duplicate names".to_owned(),
        ));
    }
    let name = directory_sort_run_name(0, run_index);
    let mut run = create_regular_at(directory, &name)?;
    for entry in names.iter() {
        run.write_all(entry.as_bytes()).map_err(substrate)?;
        run.write_all(b"\n").map_err(substrate)?;
    }
    run.sync_all().map_err(substrate)?;
    names.clear();
    Ok(())
}

fn write_directory_sort_runs(source: &File, staging: &File) -> ResourceResult<u64> {
    let mut source_entries = Dir::from_fd(dup(source).map_err(substrate)?).map_err(substrate)?;
    let mut names = Vec::with_capacity(DIRECTORY_SORT_RUN_ENTRIES);
    let mut buffered_bytes = 0_usize;
    let mut run_count = 0_u64;
    for entry in source_entries.iter() {
        let entry = entry.map_err(substrate)?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let name = raw
            .to_str()
            .map_err(|_| ResourceError::Validation("filesystem names must be UTF-8".to_owned()))?;
        validate_name(name)?;
        let name_bytes = name.len().checked_add(1).ok_or_else(|| {
            ResourceError::Validation("filesystem directory name size overflow".to_owned())
        })?;
        if name_bytes > MAX_MANIFEST_ENTRY_BYTES {
            return Err(ResourceError::Validation(format!(
                "filesystem directory name cannot fit a {MAX_MANIFEST_ENTRY_BYTES}-byte manifest entry"
            )));
        }
        if !names.is_empty()
            && (names.len() == DIRECTORY_SORT_RUN_ENTRIES
                || buffered_bytes
                    .checked_add(name_bytes)
                    .is_none_or(|size| size > MAX_WRITE_CHUNK))
        {
            write_directory_sort_run(staging, run_count, &mut names)?;
            run_count = run_count.checked_add(1).ok_or_else(|| {
                integrity(
                    "filesystem_import_invalid",
                    "filesystem directory sort run count overflow".to_owned(),
                )
            })?;
            buffered_bytes = 0;
        }
        names.push(name.to_owned());
        buffered_bytes = buffered_bytes.checked_add(name_bytes).ok_or_else(|| {
            integrity(
                "filesystem_import_invalid",
                "filesystem directory sort buffer overflow".to_owned(),
            )
        })?;
    }
    if !names.is_empty() {
        write_directory_sort_run(staging, run_count, &mut names)?;
        run_count = run_count.checked_add(1).ok_or_else(|| {
            integrity(
                "filesystem_import_invalid",
                "filesystem directory sort run count overflow".to_owned(),
            )
        })?;
    }
    staging.sync_all().map_err(substrate)?;
    Ok(run_count)
}

fn read_directory_sort_name<R: BufRead>(reader: &mut R) -> ResourceResult<Option<String>> {
    let Some(mut line) = read_capped_manifest_line(reader)? else {
        return Ok(None);
    };
    if line.pop() != Some(b'\n') || line.is_empty() {
        return Err(integrity(
            "filesystem_import_invalid",
            "filesystem directory sort run has a malformed name record".to_owned(),
        ));
    }
    let name = String::from_utf8(line).map_err(|_| {
        integrity(
            "filesystem_import_invalid",
            "filesystem directory sort name is not UTF-8".to_owned(),
        )
    })?;
    validate_name(&name)
        .map_err(|error| integrity("filesystem_import_invalid", error.to_string()))?;
    Ok(Some(name))
}

fn merge_directory_sort_group(
    directory: &File,
    input_pass: u64,
    start: u64,
    end: u64,
    output_pass: u64,
    output_index: u64,
) -> ResourceResult<()> {
    let capacity = usize::try_from(end - start)
        .map_err(|error| integrity("filesystem_import_invalid", error.to_string()))?;
    let mut readers = Vec::with_capacity(capacity);
    let mut heads = Vec::with_capacity(capacity);
    for index in start..end {
        let name = directory_sort_run_name(input_pass, index);
        let mut reader = BufReader::new(open_regular_at(directory, &name, false, false)?);
        heads.push(read_directory_sort_name(&mut reader)?);
        readers.push(reader);
    }
    let output_name = directory_sort_run_name(output_pass, output_index);
    let mut output = create_regular_at(directory, &output_name)?;
    let mut previous: Option<String> = None;
    loop {
        let selected = heads
            .iter()
            .enumerate()
            .filter_map(|(index, name)| name.as_ref().map(|name| (index, name)))
            .min_by(|left, right| left.1.cmp(right.1))
            .map(|(index, _)| index);
        let Some(selected) = selected else {
            break;
        };
        let current = heads[selected]
            .take()
            .expect("selected directory sort head exists");
        if previous
            .as_deref()
            .is_some_and(|previous| previous >= current.as_str())
        {
            return Err(integrity(
                "filesystem_import_invalid",
                "filesystem directory sort runs are not globally unique and ordered".to_owned(),
            ));
        }
        output.write_all(current.as_bytes()).map_err(substrate)?;
        output.write_all(b"\n").map_err(substrate)?;
        previous = Some(current);
        heads[selected] = read_directory_sort_name(&mut readers[selected])?;
    }
    output.sync_all().map_err(substrate)?;
    directory.sync_all().map_err(substrate)?;
    for index in start..end {
        remove_file_at(directory, &directory_sort_run_name(input_pass, index))?;
    }
    directory.sync_all().map_err(substrate)
}

fn merge_directory_sort_runs(
    directory: &File,
    mut run_count: u64,
) -> ResourceResult<Option<String>> {
    if run_count == 0 {
        return Ok(None);
    }
    let mut pass = 0_u64;
    while run_count > 1 {
        let next_pass = pass.checked_add(1).ok_or_else(|| {
            integrity(
                "filesystem_import_invalid",
                "filesystem directory sort pass overflow".to_owned(),
            )
        })?;
        let output_count = run_count.div_ceil(DIRECTORY_SORT_MERGE_FAN_IN);
        for output_index in 0..output_count {
            let start = output_index
                .checked_mul(DIRECTORY_SORT_MERGE_FAN_IN)
                .ok_or_else(|| {
                    integrity(
                        "filesystem_import_invalid",
                        "filesystem directory sort group offset overflow".to_owned(),
                    )
                })?;
            let end = start
                .checked_add(DIRECTORY_SORT_MERGE_FAN_IN)
                .map(|end| end.min(run_count))
                .ok_or_else(|| {
                    integrity(
                        "filesystem_import_invalid",
                        "filesystem directory sort group end overflow".to_owned(),
                    )
                })?;
            merge_directory_sort_group(directory, pass, start, end, next_pass, output_index)?;
        }
        pass = next_pass;
        run_count = output_count;
    }
    Ok(Some(directory_sort_run_name(pass, 0)))
}

fn validate_fixed_name(name: &str) -> ResourceResult<()> {
    if name.is_empty()
        || name.contains(['/', '\\'])
        || matches!(name, "." | "..")
        || name.chars().any(char::is_control)
    {
        return Err(integrity(
            "filesystem_layout_invalid",
            format!("unsafe filesystem-owned entry name {name:?}"),
        ));
    }
    Ok(())
}

fn entry_exists(directory: &File, name: &str) -> ResourceResult<bool> {
    validate_fixed_name(name)?;
    match fstatat(directory, name, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(stat) => {
            if SFlag::from_bits_truncate(stat.st_mode).contains(SFlag::S_IFLNK) {
                return Err(integrity(
                    "filesystem_layout_invalid",
                    format!("filesystem-owned entry {name:?} is a symlink"),
                ));
            }
            Ok(true)
        }
        Err(Errno::ENOENT) => Ok(false),
        Err(error) => Err(substrate(error)),
    }
}

fn open_regular_at(
    directory: &File,
    name: &str,
    create: bool,
    writable: bool,
) -> ResourceResult<File> {
    validate_fixed_name(name)?;
    let mut flags = OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC;
    flags |= if writable {
        OFlag::O_RDWR
    } else {
        OFlag::O_RDONLY
    };
    if create {
        flags |= OFlag::O_CREAT;
    }
    let descriptor = openat(directory, name, flags, Mode::from_bits_truncate(0o600)).map_err(
        |error| match error {
            Errno::ELOOP => integrity(
                "filesystem_layout_invalid",
                format!("filesystem-owned entry {name:?} is a symlink"),
            ),
            Errno::ENOENT => {
                ResourceError::NotFound(format!("filesystem-owned entry {name:?} is missing"))
            }
            other => substrate(other),
        },
    )?;
    let file = file_from_fd(descriptor);
    if !file.metadata().map_err(substrate)?.is_file() {
        return Err(integrity(
            "filesystem_layout_invalid",
            format!("filesystem-owned entry {name:?} is not a regular file"),
        ));
    }
    Ok(file)
}

fn open_regular_at_optional(directory: &File, name: &str) -> ResourceResult<Option<File>> {
    if !entry_exists(directory, name)? {
        return Ok(None);
    }
    open_regular_at(directory, name, false, false).map(Some)
}

fn create_regular_at(directory: &File, name: &str) -> ResourceResult<File> {
    validate_fixed_name(name)?;
    let descriptor = openat(
        directory,
        name,
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(substrate)?;
    Ok(file_from_fd(descriptor))
}

fn write_synced_at(directory: &File, name: &str, bytes: &[u8]) -> ResourceResult<()> {
    let mut file = open_regular_at(directory, name, true, true)?;
    file.set_len(0).map_err(substrate)?;
    file.write_all(bytes).map_err(substrate)?;
    file.sync_all().map_err(substrate)
}

fn sync_file_at(directory: &File, name: &str) -> ResourceResult<()> {
    open_regular_at(directory, name, false, false)?
        .sync_all()
        .map_err(substrate)
}

fn remove_file_at(directory: &File, name: &str) -> ResourceResult<()> {
    if !entry_exists(directory, name)? {
        return Ok(());
    }
    let metadata = fstatat(directory, name, AtFlags::AT_SYMLINK_NOFOLLOW).map_err(substrate)?;
    if !SFlag::from_bits_truncate(metadata.st_mode).contains(SFlag::S_IFREG) {
        return Err(integrity(
            "filesystem_layout_invalid",
            format!("filesystem-owned entry {name:?} is not a regular file"),
        ));
    }
    match unlinkat(directory, name, UnlinkatFlags::NoRemoveDir) {
        Ok(()) | Err(Errno::ENOENT) => Ok(()),
        Err(error) => Err(substrate(error)),
    }
}

fn remove_manifest_index_staging(parent: &File, name: &str) -> ResourceResult<()> {
    remove_manifest_index_directory(parent, name)
}

fn remove_directory_sort_staging(parent: &File, name: &str) -> ResourceResult<()> {
    if !entry_exists(parent, name)? {
        return Ok(());
    }
    let directory = open_directory_at(parent, name)?;
    let mut entries = Dir::from_fd(dup(&directory).map_err(substrate)?).map_err(substrate)?;
    for entry in entries.iter() {
        let entry = entry.map_err(substrate)?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let run = raw.to_str().map_err(|_| {
            integrity(
                "filesystem_cleanup_invalid",
                "filesystem directory sort staging name is not UTF-8".to_owned(),
            )
        })?;
        if !is_directory_sort_run_name(run) {
            return Err(integrity(
                "filesystem_cleanup_invalid",
                format!("filesystem directory sort staging contains unexpected entry {run:?}"),
            ));
        }
        remove_file_at(&directory, run)?;
    }
    directory.sync_all().map_err(substrate)?;
    match unlinkat(parent, name, UnlinkatFlags::RemoveDir) {
        Ok(()) | Err(Errno::ENOENT) => Ok(()),
        Err(error) => Err(substrate(error)),
    }
}

fn remove_manifest_index_directory(parent: &File, name: &str) -> ResourceResult<()> {
    if !entry_exists(parent, name)? {
        return Ok(());
    }
    let directory = open_directory_at(parent, name)?;
    let mut entries = Dir::from_fd(dup(&directory).map_err(substrate)?).map_err(substrate)?;
    for entry in entries.iter() {
        let entry = entry.map_err(substrate)?;
        let raw = entry.file_name();
        if raw.to_bytes() == b"." || raw.to_bytes() == b".." {
            continue;
        }
        let entry = raw.to_str().map_err(|_| {
            integrity(
                "filesystem_cleanup_invalid",
                "filesystem manifest index staging name is not UTF-8".to_owned(),
            )
        })?;
        if !matches!(entry, "offsets.bin" | "nodes.bin") {
            return Err(integrity(
                "filesystem_cleanup_invalid",
                format!("filesystem manifest index contains unexpected entry {entry:?}"),
            ));
        }
        remove_file_at(&directory, entry)?;
    }
    directory.sync_all().map_err(substrate)?;
    match unlinkat(parent, name, UnlinkatFlags::RemoveDir) {
        Ok(()) | Err(Errno::ENOENT) => Ok(()),
        Err(error) => Err(substrate(error)),
    }
}

fn read_bounded(
    file: &mut File,
    limit: u64,
    integrity_code: &'static str,
) -> ResourceResult<Vec<u8>> {
    let size = file.metadata().map_err(substrate)?.len();
    if size > limit {
        return Err(integrity(
            integrity_code,
            format!("filesystem metadata entry exceeds {limit} bytes"),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(substrate)?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(size).map_err(|error| integrity(integrity_code, error.to_string()))?,
    );
    file.read_to_end(&mut bytes).map_err(substrate)?;
    if bytes.len() as u64 != size {
        return Err(integrity(
            integrity_code,
            "filesystem metadata entry changed during its descriptor read".to_owned(),
        ));
    }
    Ok(bytes)
}

fn initialize_layout_marker(root: &File) -> ResourceResult<()> {
    let initializer_descriptor = openat(
        root,
        ".",
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(substrate)?;
    let initializer = file_from_fd(initializer_descriptor);
    match FileExt::try_lock(&initializer) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            return Err(conflict(
                "filesystem_layout_conflict",
                "filesystem Resource namespace layout has an active initializer".to_owned(),
            ));
        }
        Err(TryLockError::Error(error)) => return Err(substrate(error)),
    }

    if entry_exists(root, PHYSICAL_LAYOUT_MARKER)? {
        let marker_size = open_regular_at(root, PHYSICAL_LAYOUT_MARKER, false, false)?
            .metadata()
            .map_err(substrate)?
            .len();
        if marker_size > 0 {
            return verify_layout_marker(root);
        }
        visit_directory_names(root, |entry| {
            if matches!(
                entry,
                PHYSICAL_LAYOUT_MARKER | PHYSICAL_LAYOUT_STAGING_MARKER
            ) {
                Ok(())
            } else {
                Err(integrity(
                    "filesystem_layout_invalid",
                    "zero-length filesystem layout marker is recoverable only in an otherwise uninitialized namespace"
                        .to_owned(),
                ))
            }
        })?;
        remove_file_at(root, PHYSICAL_LAYOUT_MARKER)?;
        root.sync_all().map_err(substrate)?;
    } else {
        visit_directory_names(root, |entry| {
            if entry == PHYSICAL_LAYOUT_STAGING_MARKER {
                Ok(())
            } else {
                Err(integrity(
                    "filesystem_layout_invalid",
                    "unsupported filesystem Resource physical generation: layout marker is absent"
                        .to_owned(),
                ))
            }
        })?;
    }

    remove_file_at(root, PHYSICAL_LAYOUT_STAGING_MARKER)?;
    root.sync_all().map_err(substrate)?;
    write_layout_marker(root)
}

fn write_layout_marker(root: &File) -> ResourceResult<()> {
    let marker = PhysicalLayoutMarker {
        layout_version: PHYSICAL_LAYOUT_VERSION.to_owned(),
    };
    let bytes = cymule_core::canonical_bytes(&marker).map_err(core_error)?;
    let descriptor = openat(
        root,
        PHYSICAL_LAYOUT_STAGING_MARKER,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(substrate)?;
    let mut file = file_from_fd(descriptor);
    file.write_all(&bytes).map_err(substrate)?;
    file.sync_all().map_err(substrate)?;
    renameat(
        root,
        PHYSICAL_LAYOUT_STAGING_MARKER,
        root,
        PHYSICAL_LAYOUT_MARKER,
    )
    .map_err(substrate)?;
    root.sync_all().map_err(substrate)
}

fn verify_layout_marker(root: &File) -> ResourceResult<()> {
    let Some(mut file) = open_regular_at_optional(root, PHYSICAL_LAYOUT_MARKER)? else {
        return Err(integrity(
            "filesystem_layout_invalid",
            "unsupported filesystem Resource physical generation: layout marker is absent"
                .to_owned(),
        ));
    };
    let marker: PhysicalLayoutMarker =
        cymule_core::decode_json(&read_bounded(&mut file, 4096, "filesystem_layout_invalid")?)
            .map_err(core_integrity)?;
    if marker.layout_version != PHYSICAL_LAYOUT_VERSION {
        return Err(integrity(
            "filesystem_layout_invalid",
            format!(
                "unsupported filesystem Resource physical generation {}",
                marker.layout_version
            ),
        ));
    }
    let canonical = cymule_core::canonical_bytes(&marker).map_err(core_error)?;
    if read_bounded(&mut file, 4096, "filesystem_layout_invalid")? != canonical {
        return Err(integrity(
            "filesystem_layout_invalid",
            "filesystem Resource physical generation marker is not canonical".to_owned(),
        ));
    }
    Ok(())
}

fn read_capped_manifest_line<R: BufRead>(reader: &mut R) -> ResourceResult<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(substrate)?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(line))
            };
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        let next_len = line.len().checked_add(count).ok_or_else(|| {
            integrity(
                "filesystem_manifest_invalid",
                "filesystem manifest entry size overflow".to_owned(),
            )
        })?;
        if next_len > MAX_MANIFEST_ENTRY_BYTES {
            return Err(integrity(
                "filesystem_manifest_invalid",
                format!(
                    "filesystem manifest entry exceeds {MAX_MANIFEST_ENTRY_BYTES} canonical bytes"
                ),
            ));
        }
        let terminal = available[count - 1] == b'\n';
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if terminal {
            return Ok(Some(line));
        }
    }
}

fn manifest_node_count(entry_count: u64) -> ResourceResult<u64> {
    let mut total = 0_u64;
    let mut width = entry_count;
    while width > 0 {
        total = total.checked_add(width).ok_or_else(|| {
            integrity(
                "filesystem_manifest_invalid",
                "manifest node count overflow".to_owned(),
            )
        })?;
        if width == 1 {
            break;
        }
        width = width.div_ceil(2);
    }
    Ok(total)
}

fn append_manifest_parent_levels(nodes: &mut File, entry_count: u64) -> ResourceResult<()> {
    let mut level_start = 0_u64;
    let mut width = entry_count;
    while width > 1 {
        let parent_count = width.div_ceil(2);
        for parent in 0..parent_count {
            let left_index = level_start
                .checked_add(parent.checked_mul(2).ok_or_else(|| {
                    integrity(
                        "filesystem_manifest_invalid",
                        "manifest node position overflow".to_owned(),
                    )
                })?)
                .ok_or_else(|| {
                    integrity(
                        "filesystem_manifest_invalid",
                        "manifest node position overflow".to_owned(),
                    )
                })?;
            let right_index = left_index + u64::from(parent * 2 + 1 < width);
            let left = read_digest_at(nodes, left_index)?;
            let right = read_digest_at(nodes, right_index)?;
            let digest = manifest_node_digest(&left, &right)?;
            nodes.seek(SeekFrom::End(0)).map_err(substrate)?;
            nodes
                .write_all(&decode_digest(&digest)?)
                .map_err(substrate)?;
        }
        level_start = level_start.checked_add(width).ok_or_else(|| {
            integrity(
                "filesystem_manifest_invalid",
                "manifest node level overflow".to_owned(),
            )
        })?;
        width = parent_count;
    }
    Ok(())
}

fn decode_digest(digest: &str) -> ResourceResult<[u8; 32]> {
    let encoded = digest.strip_prefix("sha256:").ok_or_else(|| {
        integrity(
            "filesystem_manifest_invalid",
            "manifest index digest is malformed".to_owned(),
        )
    })?;
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(integrity(
            "filesystem_manifest_invalid",
            "manifest index digest is malformed".to_owned(),
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let high = char::from(pair[0])
            .to_digit(16)
            .expect("validated hex digit");
        let low = char::from(pair[1])
            .to_digit(16)
            .expect("validated hex digit");
        decoded[index] = u8::try_from((high << 4) | low).expect("two hex digits fit one byte");
    }
    Ok(decoded)
}

fn read_digest(reader: &mut File) -> ResourceResult<String> {
    let mut digest = [0_u8; 32];
    reader.read_exact(&mut digest).map_err(substrate)?;
    Ok(format!("sha256:{}", hex_digest(&digest)))
}

fn read_digest_at(reader: &mut File, position: u64) -> ResourceResult<String> {
    reader
        .seek(SeekFrom::Start(position.checked_mul(32).ok_or_else(
            || {
                integrity(
                    "filesystem_manifest_invalid",
                    "manifest digest position overflow".to_owned(),
                )
            },
        )?))
        .map_err(substrate)?;
    read_digest(reader)
}

fn hash_file(file: &mut File) -> ResourceResult<(String, u64)> {
    file.seek(SeekFrom::Start(0)).map_err(substrate)?;
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(substrate)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        size = size.checked_add(count as u64).ok_or_else(|| {
            integrity(
                "filesystem_resource_integrity_failed",
                "resource size overflow".to_owned(),
            )
        })?;
    }
    Ok((format!("sha256:{}", hex_digest(&hasher.finalize())), size))
}

fn verify_content_file(
    file: &mut File,
    expected_digest: &str,
    expected_size: u64,
) -> ResourceResult<()> {
    let metadata = file.metadata().map_err(substrate)?;
    if metadata.len() != expected_size {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            "filesystem object size changed".to_owned(),
        ));
    }
    let (actual, size) = hash_file(file)?;
    if size != expected_size || actual != expected_digest {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            format!("filesystem object digest changed: expected {expected_digest}, found {actual}"),
        ));
    }
    Ok(())
}

fn verify_resource_file(file: &mut File, resource: &ResourceHandle) -> ResourceResult<()> {
    let ResourceIntegrity::Content {
        digest: expected_digest,
        size: expected_size,
    } = &resource.integrity
    else {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            "filesystem Resource is not content addressed".to_owned(),
        ));
    };
    let Some(expected_manifest) = &resource.manifest else {
        return verify_content_file(file, expected_digest, *expected_size);
    };
    if expected_digest != &expected_manifest.digest || expected_size != &expected_manifest.size {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            "filesystem manifest Resource has split content authority".to_owned(),
        ));
    }
    if file.metadata().map_err(substrate)?.len() != *expected_size {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            "filesystem manifest size changed".to_owned(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(substrate)?;
    let mut verifier = ResourceManifestStreamVerifier::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(substrate)?;
        if count == 0 {
            break;
        }
        verifier.push(&buffer[..count])?;
    }
    let observed = verifier.finish()?;
    if &observed != expected_manifest {
        return Err(integrity(
            "filesystem_resource_integrity_failed",
            "filesystem manifest bytes do not close their semantic descriptor".to_owned(),
        ));
    }
    Ok(())
}

fn verify_deletion_target_file(
    file: &mut File,
    target: &ResourceDeletionTarget,
) -> ResourceResult<()> {
    target.verify()?;
    let Some(expected_manifest) = &target.manifest else {
        return verify_content_file(
            file,
            &target.subject.family.content_digest,
            target.content_size,
        );
    };
    if file.metadata().map_err(substrate)?.len() != target.content_size {
        return Err(integrity(
            "filesystem_cleanup_invalid",
            "filesystem deletion manifest size changed".to_owned(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(substrate)?;
    let mut verifier = ResourceManifestStreamVerifier::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer).map_err(substrate)?;
        if count == 0 {
            break;
        }
        verifier.push(&buffer[..count])?;
    }
    if verifier.finish()? != *expected_manifest {
        return Err(integrity(
            "filesystem_cleanup_invalid",
            "filesystem deletion manifest bytes changed".to_owned(),
        ));
    }
    Ok(())
}

fn core_error(error: impl std::fmt::Display) -> ResourceError {
    integrity("filesystem_canonical_encoding_failed", error.to_string())
}

fn core_integrity(error: impl std::fmt::Display) -> ResourceError {
    integrity("filesystem_canonical_integrity_failed", error.to_string())
}

fn conflict(code: &'static str, message: impl Into<String>) -> ResourceError {
    ResourceError::Conflict {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn resource_deleted(family: &ResourceRetentionFamily) -> ResourceError {
    conflict(
        "filesystem_resource_deleted",
        format!(
            "filesystem retention family {} is permanently deleted",
            family.retention_key
        ),
    )
}

fn integrity(code: &'static str, message: impl Into<String>) -> ResourceError {
    ResourceError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
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
    substrate_with_code("filesystem_io_failure", error)
}

fn substrate_with_code(code: &'static str, error: impl std::fmt::Display) -> ResourceError {
    ResourceError::Substrate {
        code: code.to_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    fn prepare_publishing_upload(
        store: &mut FsResourceStore,
        write_id: &str,
        bytes: &[u8],
    ) -> (ResourceWriteSession, UploadRecord) {
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: write_id.to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store
            .write_chunk(&session, 0, bytes)
            .expect("bytes persist");
        let mut record = store.load_record(&session.upload_id).expect("record reads");
        record.state = UploadState::Publishing;
        record.publication = Some(UploadPublication {
            digest: format!("sha256:{}", hex_digest(&Sha256::digest(bytes))),
            size: bytes.len() as u64,
            manifest: None,
        });
        store
            .store_record(&record)
            .expect("Publishing intent persists");
        (session, record)
    }

    fn prepare_manifest_publishing_replay(
        store: &mut FsResourceStore,
        publication: &ResourcePublication,
        manifest_bytes: &[u8],
    ) -> (ResourceWriteSession, ResourceManifestDescriptor, String) {
        let descriptor = publication
            .resource
            .manifest
            .clone()
            .expect("directory retains a manifest descriptor");
        let intent = ResourceWriteIntent {
            write_id: "write:late-manifest-publication".to_owned(),
            shape: publication.resource.shape,
            media_type: publication.resource.media_type.clone(),
            annotations: publication.resource.annotations.clone(),
        };
        let session = store.begin_write(&intent).expect("late write begins");
        store
            .write_chunk(&session, 0, manifest_bytes)
            .expect("late manifest bytes persist");
        let mut record = store
            .load_record(&session.upload_id)
            .expect("late upload record reads");
        let mut data = store
            .open_acknowledged_upload_data(&session, record.committed_length)
            .expect("late upload data opens");
        assert_eq!(
            store
                .prepare_manifest_index(&session, &mut data)
                .expect("late manifest index prepares"),
            descriptor
        );
        record.state = UploadState::Publishing;
        record.publication = Some(UploadPublication {
            digest: descriptor.digest.clone(),
            size: descriptor.size,
            manifest: Some(descriptor.clone()),
        });
        store
            .store_record(&record)
            .expect("late Publishing intent persists");
        let staging = FsResourceStore::manifest_index_staging_name(&session)
            .expect("late index staging name derives");
        assert!(
            entry_exists(&store.directories.staging, &staging)
                .expect("late index staging is visible")
        );
        (session, descriptor, staging)
    }

    #[test]
    fn deletion_tombstone_survives_interruption_before_manifest_payload_unlink() {
        let directory = tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let source = directory.path().join("source");
        fs::create_dir(&source).expect("source directory creates");
        fs::write(source.join("entry.txt"), b"entry").expect("source entry writes");
        let binding = "fs:delete-interruption";
        let mut store = FsResourceStore::open(&root, binding).expect("store opens");
        let publication = store
            .import_directory(&source, "import:delete-interruption")
            .expect("directory publishes");
        let target = ResourceDeletionTarget::from_publication(&publication)
            .expect("manifest deletion target derives");
        let object_name = store
            .resource_name(&publication.resource, &publication.locators)
            .expect("manifest object name derives");
        let manifest_bytes = fs::read(
            root.join("objects")
                .join(binding_namespace(binding))
                .join(&object_name),
        )
        .expect("published manifest bytes read");
        let (late_session, descriptor, late_index_staging) =
            prepare_manifest_publishing_replay(&mut store, &publication, &manifest_bytes);
        let index_name =
            FsResourceStore::manifest_index_name(&descriptor).expect("manifest index name derives");
        let catalog_name =
            FsResourceStore::catalog_name(MANIFEST_INDEX_VERSION, &descriptor.digest);

        FAIL_AFTER_DELETION_TOMBSTONE.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.delete_and_verify_absent(&target),
            Err(ResourceError::Substrate { code, .. })
                if code == "filesystem_test_interrupted_after_deletion_tombstone"
        ));
        let family = &target.subject.family;
        let control_name =
            FsResourceStore::retention_lock_name(family).expect("retention control name derives");
        assert_eq!(
            fs::read(root.join("locks").join(control_name))
                .expect("durable tombstone reads after interruption"),
            b"D"
        );
        assert!(entry_exists(&store.directories.objects, &object_name).unwrap());
        assert!(entry_exists(&store.directories.manifest_indexes, &index_name).unwrap());
        assert!(entry_exists(&store.directories.catalog, &catalog_name).unwrap());
        assert!(matches!(
            store.stat(&publication.resource, &publication.locators),
            Err(ResourceError::NotFound(_))
        ));
        drop(store);

        let mut reopened = FsResourceStore::open(&root, binding).expect("store reopens");
        assert!(matches!(
            reopened.commit_write(&late_session),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_resource_deleted"
        ));
        assert!(
            reopened
                .cleanup_receipt(&late_session)
                .expect("late cleanup receipt reads")
                .is_some()
        );
        assert!(!entry_exists(&reopened.directories.staging, &late_index_staging).unwrap());
        assert!(
            !entry_exists(
                &reopened.directories.uploads,
                &FsResourceStore::data_name(&late_session.upload_id).unwrap()
            )
            .unwrap()
        );
        reopened
            .delete_and_verify_absent(&target)
            .expect("reopened deletion converges retained payload and manifest metadata");
        assert!(!entry_exists(&reopened.directories.objects, &object_name).unwrap());
        assert!(!entry_exists(&reopened.directories.manifest_indexes, &index_name).unwrap());
        assert!(!entry_exists(&reopened.directories.catalog, &catalog_name).unwrap());
        assert!(matches!(
            reopened.stat(&publication.resource, &publication.locators),
            Err(ResourceError::NotFound(_))
        ));
    }

    #[test]
    fn import_validation_rechecks_the_current_frontier_without_revoking_terminal_authority() {
        let directory = tempdir().expect("temporary directory");
        let mut store =
            FsResourceStore::open(directory.path(), "fs:import-frontier").expect("store opens");
        let intent = ResourceWriteIntent {
            write_id: "import:current-frontier".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).unwrap();
        store.write_chunk(&session, 0, b"A").unwrap();
        let completed_input = ImportInput::FileSize(1);
        let mut other = FsResourceStore::open(directory.path(), "fs:import-frontier")
            .expect("independent writer opens");
        other
            .write_chunk(&session, 1, b"B")
            .expect("frontier advances after source ingest");
        let retained = store.load_record(&session.upload_id).unwrap();
        assert!(matches!(
            store.commit_write_with_import(&session, Some(&completed_input)),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
        ));
        assert_eq!(store.load_record(&session.upload_id).unwrap(), retained);
        assert_eq!(retained.state, UploadState::Open);
        assert!(store.cleanup_receipt(&session).unwrap().is_none());
        let publication = store
            .commit_write_with_import(&session, Some(&ImportInput::FileSize(2)))
            .expect("complete input admits the current frontier");
        let terminal = store.load_record(&session.upload_id).unwrap();
        let receipt = store.cleanup_receipt(&session).unwrap();
        assert!(matches!(
            store.commit_write_with_import(&session, Some(&completed_input)),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
        ));
        assert_eq!(store.load_record(&session.upload_id).unwrap(), terminal);
        assert_eq!(store.cleanup_receipt(&session).unwrap(), receipt);
        assert_eq!(store.commit_write(&session).unwrap(), publication);

        let (publishing, record) = prepare_publishing_upload(&mut store, "import:pending", b"AB");
        let expected = store.publication_for_record(&record).unwrap().unwrap();
        assert!(matches!(
            store.commit_write_with_import(&publishing, Some(&completed_input)),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
        ));
        assert_eq!(
            store.load_record(&publishing.upload_id).unwrap().state,
            UploadState::Committed
        );
        assert!(store.cleanup_receipt(&publishing).unwrap().is_some());
        assert_eq!(store.commit_write(&publishing).unwrap(), expected);
    }

    #[test]
    fn publishing_chunk_retries_only_compare_the_acknowledged_frontier() {
        for content_published in [false, true] {
            let directory = tempdir().expect("temporary directory creates");
            let root = directory.path().join("store");
            let mut store = FsResourceStore::open(&root, "fs:chunk-replay").expect("store opens");
            let bytes = b"acknowledged bytes";
            let (session, record) =
                prepare_publishing_upload(&mut store, "write:chunk-replay", bytes);
            let expected = store
                .publication_for_record(&record)
                .unwrap()
                .expect("Publishing retains its publication");
            if content_published {
                let family = ResourceRetentionFamily::from_publication(&expected).unwrap();
                let claim = store.claim_retention(&family).unwrap();
                store
                    .converge_publishing(&session, &record, &expected, &claim)
                    .expect("content publishes before the terminal head");
            }
            drop(store);
            let mut reopened =
                FsResourceStore::open(&root, "fs:chunk-replay").expect("store reopens");
            assert_eq!(reopened.begin_write(&record.intent).unwrap(), session);
            reopened
                .write_chunk(&session, 0, bytes)
                .expect("exact replay succeeds");
            reopened
                .write_chunk(&session, 3, &bytes[3..9])
                .expect("an acknowledged range replays without extending it");
            for (offset, changed) in [
                (0, b"changed".as_slice()),
                (bytes.len() as u64, b"x".as_slice()),
                (bytes.len() as u64 - 1, b"xx".as_slice()),
            ] {
                assert!(matches!(
                    reopened.write_chunk(&session, offset, changed),
                    Err(ResourceError::Conflict { code, .. })
                        if code == "filesystem_upload_conflict"
                ));
            }
            let mut foreign = session.clone();
            foreign.store_binding = "fs:foreign".to_owned();
            assert!(matches!(
                reopened.write_chunk(&foreign, 0, bytes),
                Err(ResourceError::Conflict { code, .. })
                    if code == "filesystem_upload_conflict"
            ));
            assert_eq!(reopened.load_record(&session.upload_id).unwrap(), record);
            assert!(reopened.cleanup_receipt(&session).unwrap().is_none());
            assert_eq!(reopened.commit_write(&session).unwrap(), expected);
        }
    }

    #[test]
    fn publishing_chunk_retry_never_repairs_or_recreates_acknowledged_data() {
        let directory = tempdir().expect("temporary directory creates");
        let mut store =
            FsResourceStore::open(directory.path(), "fs:readonly-replay").expect("store opens");
        let bytes = b"immutable frontier";
        let (session, record) =
            prepare_publishing_upload(&mut store, "write:readonly-replay", bytes);
        let data_name = FsResourceStore::data_name(&session.upload_id).unwrap();
        let data = open_regular_at(&store.directories.uploads, &data_name, false, true)
            .expect("test opens its acknowledged data");
        for changed_length in [bytes.len() as u64 + 1, bytes.len() as u64 - 1] {
            data.set_len(changed_length)
                .expect("test changes the physical frontier");
            assert!(matches!(
                store.write_chunk(&session, 0, bytes),
                Err(ResourceError::Integrity { code, .. })
                    if code == "filesystem_upload_record_invalid"
            ));
            assert_eq!(data.metadata().unwrap().len(), changed_length);
            assert_eq!(store.load_record(&session.upload_id).unwrap(), record);
        }
        remove_file_at(&store.directories.uploads, &data_name).unwrap();
        assert!(store.write_chunk(&session, 0, bytes).is_err());
        assert!(!entry_exists(&store.directories.uploads, &data_name).unwrap());
        assert_eq!(store.load_record(&session.upload_id).unwrap(), record);
    }

    #[test]
    fn committed_chunk_replay_is_bounded_and_commit_still_verifies_the_complete_object() {
        let directory = tempdir().expect("temporary directory creates");
        let mut store =
            FsResourceStore::open(directory.path(), "fs:bounded-replay").expect("store opens");
        let bytes = b"prefix and suffix";
        let (session, _) = prepare_publishing_upload(&mut store, "write:bounded-replay", bytes);
        let publication = store.commit_write(&session).expect("publication commits");
        let record = store.load_record(&session.upload_id).unwrap();
        let object_name = store
            .resource_name(&publication.resource, &publication.locators)
            .expect("exact object name resolves");
        let mut object = open_regular_at(&store.directories.objects, &object_name, false, true)
            .expect("test opens its committed object");
        object.seek(SeekFrom::End(-1)).unwrap();
        object.write_all(b"!").unwrap();
        store
            .write_chunk(&session, 0, b"prefix")
            .expect("equal prefix replay does not read the unrelated suffix");
        assert_eq!(store.load_record(&session.upload_id).unwrap(), record);
        assert!(matches!(
            store.commit_write(&session),
            Err(ResourceError::Integrity { code, .. })
                if code == "filesystem_resource_integrity_failed"
        ));
        assert!(matches!(
            store.write_chunk(&session, 0, b"changed"),
            Err(ResourceError::Integrity { code, .. })
                if code == "filesystem_resource_integrity_failed"
        ));
    }

    #[test]
    fn terminal_cleanup_receipts_never_authorize_new_content_or_aborted_replay() {
        let directory = tempdir().expect("temporary directory creates");
        let mut store =
            FsResourceStore::open(directory.path(), "fs:terminal-replay").expect("store opens");
        let bytes = b"terminal bytes";
        let (session, _) = prepare_publishing_upload(&mut store, "write:terminal-replay", bytes);
        let publication = store.commit_write(&session).expect("publication commits");
        let receipt = store.cleanup_receipt(&session).unwrap().unwrap();
        let object_name = store
            .resource_name(&publication.resource, &publication.locators)
            .unwrap();
        remove_file_at(&store.directories.objects, &object_name).unwrap();
        let record = store.load_record(&session.upload_id).unwrap();
        assert!(store.write_chunk(&session, 0, bytes).is_err());
        assert!(store.commit_write(&session).is_err());
        assert!(!entry_exists(&store.directories.objects, &object_name).unwrap());
        assert_eq!(store.load_record(&session.upload_id).unwrap(), record);
        assert_eq!(store.cleanup_receipt(&session).unwrap(), Some(receipt));

        let aborted = store
            .begin_write(&ResourceWriteIntent {
                write_id: "write:aborted-replay".to_owned(),
                ..record.intent
            })
            .unwrap();
        store.write_chunk(&aborted, 0, bytes).unwrap();
        let receipt = store.abort_write(&aborted).unwrap();
        let record = store.load_record(&aborted.upload_id).unwrap();
        assert!(matches!(
            store.write_chunk(&aborted, 0, bytes),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
        ));
        assert_eq!(store.load_record(&aborted.upload_id).unwrap(), record);
        assert_eq!(store.cleanup_receipt(&aborted).unwrap(), Some(receipt));
    }

    #[test]
    fn directory_import_preparation_never_recreates_staging_after_abort() {
        for prepared_before_abort in [false, true] {
            let directory = tempdir().expect("temporary directory creates");
            let mut importer = FsResourceStore::open(directory.path(), "fs:directory-abort")
                .expect("importer opens");
            let session = importer
                .begin_write(&ResourceWriteIntent {
                    write_id: "write:directory-abort".to_owned(),
                    shape: ResourceShape::Directory,
                    media_type: DIRECTORY_MEDIA_TYPE.to_owned(),
                    annotations: BTreeMap::new(),
                })
                .unwrap();
            if prepared_before_abort {
                assert!(!importer.prepare_directory_import(&session).unwrap());
            }
            let mut aborter = FsResourceStore::open(directory.path(), "fs:directory-abort")
                .expect("independent aborter opens");
            let receipt = aborter.abort_write(&session).expect("abort closes cleanup");
            let record = importer.load_record(&session.upload_id).unwrap();
            let sort_name = FsResourceStore::directory_sort_staging_name(&session).unwrap();
            assert!(!entry_exists(&importer.directories.staging, &sort_name).unwrap());
            assert!(matches!(
                importer.prepare_directory_import(&session),
                Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
            ));
            assert!(!entry_exists(&importer.directories.staging, &sort_name).unwrap());
            assert_eq!(importer.load_record(&session.upload_id).unwrap(), record);
            assert_eq!(importer.cleanup_receipt(&session).unwrap(), Some(receipt));
        }
    }

    #[test]
    fn file_import_recovery_closes_publishing_before_replaying_source() {
        for content_published in [false, true] {
            let directory = tempdir().expect("temporary directory creates");
            let source = directory.path().join("source.txt");
            let bytes = b"retained publication bytes";
            fs::write(&source, bytes).expect("source writes");
            let root = directory.path().join("store");
            let mut store = FsResourceStore::open(&root, "fs:publishing").expect("store opens");
            let session = store
                .begin_write(&ResourceWriteIntent {
                    write_id: "import:publishing".to_owned(),
                    shape: ResourceShape::Object,
                    media_type: "text/plain".to_owned(),
                    annotations: BTreeMap::new(),
                })
                .expect("write begins");
            store
                .write_chunk(&session, 0, bytes)
                .expect("bytes persist");
            let mut record = store.load_record(&session.upload_id).expect("record reads");
            record.state = UploadState::Publishing;
            record.publication = Some(UploadPublication {
                digest: format!("sha256:{}", hex_digest(&Sha256::digest(bytes))),
                size: bytes.len() as u64,
                manifest: None,
            });
            store
                .store_record(&record)
                .expect("Publishing intent persists");
            let expected = store
                .publication_for_record(&record)
                .expect("publication derives")
                .expect("Publishing retains its exact publication");
            if content_published {
                let family = ResourceRetentionFamily::from_publication(&expected).unwrap();
                let claim = store.claim_retention(&family).unwrap();
                store
                    .converge_publishing(&session, &record, &expected, &claim)
                    .expect("content publishes before the terminal checkpoint");
            }
            drop(store);

            let mut reopened = FsResourceStore::open(&root, "fs:publishing")
                .expect("store reopens at the interrupted boundary");
            assert_eq!(
                reopened
                    .import_file(&source, "import:publishing", "text/plain")
                    .expect("the public import resumes Publishing"),
                expected
            );
            let terminal = reopened
                .load_record(&session.upload_id)
                .expect("terminal reads");
            assert_eq!(terminal.state, UploadState::Committed);
            let receipt = reopened
                .cleanup_receipt(&session)
                .expect("cleanup reads")
                .expect("publication closes with durable cleanup");
            assert_eq!(receipt, terminal.cleanup_plan.unwrap().receipt().unwrap());
            assert!(
                !entry_exists(
                    &reopened.directories.uploads,
                    &FsResourceStore::data_name(&session.upload_id,).unwrap()
                )
                .unwrap()
            );
            assert_eq!(
                reopened
                    .import_file(&source, "import:publishing", "text/plain")
                    .unwrap(),
                expected
            );
            assert_eq!(reopened.cleanup_receipt(&session).unwrap(), Some(receipt));
        }
    }

    #[test]
    fn child_write_identity_is_structural_and_accepts_maximum_parent() {
        let maximum_parent = "界".repeat(512);
        let derived = child_write_id(&maximum_parent, "child.txt")
            .expect("maximum parent derives one bounded child identity");
        cymule_core::validate_identity("derived child write", &derived)
            .expect("derived child identity fits the public identity authority");

        let left_raw = format!("{}/{}", "parent/a", "b");
        let right_raw = format!("{}/{}", "parent", "a/b");
        assert_eq!(left_raw, right_raw);
        let left = cymule_core::content_id(
            CHILD_WRITE_ID_VERSION,
            &ChildWriteIdentity {
                parent_write_id: "parent/a",
                child_name: "b",
            },
        )
        .expect("left typed identity derives");
        let right = cymule_core::content_id(
            CHILD_WRITE_ID_VERSION,
            &ChildWriteIdentity {
                parent_write_id: "parent",
                child_name: "a/b",
            },
        )
        .expect("right typed identity derives");
        assert_ne!(left, right);
        assert!(child_write_id("parent", "a/b").is_err());
        assert!(child_write_id(&"界".repeat(513), "child.txt").is_err());
    }

    #[test]
    fn binding_uses_public_unicode_scalar_boundary_before_root_mutation() {
        let directory = tempdir().expect("temporary directory creates");
        let accepted_root = directory.path().join("accepted");
        FsResourceStore::open(&accepted_root, "界".repeat(512)).expect("512-scalar binding opens");
        let rejected_root = directory.path().join("rejected");
        assert!(FsResourceStore::open(&rejected_root, "界".repeat(513)).is_err());
        assert!(!rejected_root.exists());
    }

    #[test]
    fn wide_directory_sort_is_bounded_and_rebuilds_after_interruption() {
        let directory = tempdir().expect("temporary directory creates");
        let source = directory.path().join("source");
        fs::create_dir(&source).expect("source directory creates");
        let merge_fan_in =
            usize::try_from(DIRECTORY_SORT_MERGE_FAN_IN).expect("directory sort fan-in fits usize");
        let entry_count = DIRECTORY_SORT_RUN_ENTRIES * (merge_fan_in + 1) + 17;
        for index in (0..entry_count).rev() {
            fs::write(source.join(format!("entry-{index:05}")), b"x").expect("source entry writes");
        }
        let root = directory.path().join("store");
        let mut store = FsResourceStore::open(&root, "fs:wide-sort").expect("store opens");
        FAIL_AFTER_DIRECTORY_SORT_RUNS.store(true, Ordering::SeqCst);
        assert!(matches!(
            store.import_directory(&source, "import:wide-sort"),
            Err(ResourceError::Substrate { code, .. })
                if code == "filesystem_sort_run_publication_interrupted"
        ));
        let upload_id = store
            .upload_id("import:wide-sort")
            .expect("upload identity derives");
        let sort_name = format!(
            "directory-sort-{}",
            FsResourceStore::upload_key(&upload_id).expect("upload key derives")
        );
        let retained_sort = root.join("staging").join(&sort_name);
        assert!(retained_sort.is_dir());
        assert!(
            fs::read_dir(&retained_sort)
                .expect("retained sort directory lists")
                .count()
                > merge_fan_in,
            "the extreme fixture must require more than one merge pass"
        );
        drop(store);

        let mut reopened = FsResourceStore::open(&root, "fs:wide-sort").expect("store reopens");
        let publication = reopened
            .import_directory(&source, "import:wide-sort")
            .expect("sort authority rebuilds and import commits");
        let manifest = publication
            .resource
            .manifest
            .as_ref()
            .expect("directory publication has a manifest");
        assert_eq!(manifest.entry_count, entry_count as u64);
        assert!(
            !retained_sort.exists(),
            "terminal cleanup removes the single sort-tree authority"
        );
        let replay_session = reopened
            .begin_write(&ResourceWriteIntent {
                write_id: "import:wide-sort".to_owned(),
                shape: ResourceShape::Directory,
                media_type: DIRECTORY_MEDIA_TYPE.to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("terminal directory session reopens");
        let receipt = reopened
            .cleanup_receipt(&replay_session)
            .expect("directory cleanup receipt reads")
            .expect("terminal cleanup receipt exists");
        assert_eq!(receipt.removed_staging_objects, 4);
        drop(reopened);

        let mut replay = FsResourceStore::open(&root, "fs:wide-sort").expect("store reopens");
        assert_eq!(
            replay
                .import_directory(&source, "import:wide-sort")
                .expect("published wide import replays exactly"),
            publication
        );
        assert!(!retained_sort.exists());
    }
}
