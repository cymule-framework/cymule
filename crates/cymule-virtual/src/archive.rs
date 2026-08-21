use std::collections::BTreeMap;

use cymule_core::canonical_bytes;
use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_READ_CHUNK, MAX_WRITE_CHUNK, MerkleSide, MerkleStep,
    RESOURCE_VERSION, ResourceCandidate, ResourceHandle, ResourceIntegrity, ResourcePublication,
    ResourceShape, ResourceWriteIntent,
};
use serde::{Deserialize, Serialize};

use crate::{VirtualArchiveManifest, VirtualError, VirtualResult, WorkOccurrence};

/// Stable media type for a canonical virtual archive manifest.
pub const VIRTUAL_ARCHIVE_MANIFEST_KIND: &str =
    "application/vnd.cymule.virtual-archive-manifest+json";
/// Maximum provider I/O performed by one archive call.
pub const MAX_VIRTUAL_ARCHIVE_CHUNK: u32 = 8 * 1024 * 1024;

const OCCURRENCE_LEAF_DOMAIN: &str = "cymule.virtual-archive-occurrence-leaf/1";
const OCCURRENCE_NODE_DOMAIN: &str = "cymule.virtual-archive-occurrence-node/1";

/// Bounded proof locating one exact occurrence inside an immutable archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualArchiveOccurrenceProof {
    /// Exact occurrence identity.
    pub occurrence_id: String,
    /// Zero-based position in the canonical occurrence map.
    pub index: u64,
    /// Byte offset of the canonical occurrence value in the archive Resource.
    pub offset: u64,
    /// Exact canonical occurrence byte length.
    pub length: u64,
    /// Digest of the canonical occurrence value.
    pub digest: String,
    /// Sibling path from the occurrence leaf to the certificate root.
    pub path: Vec<MerkleStep>,
}

/// Replaceable immutable byte archive for completed virtual history.
pub trait VirtualArchive {
    /// Immutable implementation binding selected for this operation.
    fn binding(&self) -> &str;

    /// Idempotently store one exact framework-computed archive object.
    fn put(&mut self, object: &VirtualArchiveObject) -> VirtualResult<()>;

    /// Read one exact bounded byte range from the immutable archive Resource.
    fn read_range(
        &mut self,
        descriptor: &ResourceHandle,
        offset: u64,
        max_bytes: u32,
    ) -> VirtualResult<Vec<u8>>;

    /// Return the bounded provider-side lookup proof for one occurrence.
    fn occurrence_proof(
        &mut self,
        descriptor: &ResourceHandle,
        occurrence_id: &str,
    ) -> VirtualResult<VirtualArchiveOccurrenceProof>;
}

/// Cold archive descriptor, chunkable bytes, and provider-side proof catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualArchiveObject {
    /// Semantic content descriptor retained in hot state.
    pub descriptor: ResourceHandle,
    /// Exact canonical bytes sent only to the cold archive.
    pub bytes: Vec<u8>,
    /// Merkle root authenticating every occurrence range proof.
    pub occurrence_root_digest: String,
    /// Provider-side range proofs, never retained in hot Machine state.
    pub occurrence_proofs: BTreeMap<String, VirtualArchiveOccurrenceProof>,
}

/// Non-semantic locator and proof catalog retained by the archive provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceArchiveCatalog {
    /// Replaceable Resource publications keyed by semantic Resource ID.
    pub publications: BTreeMap<String, ResourcePublication>,
    /// Range proofs keyed by archive Resource then occurrence identity.
    pub occurrence_proofs: BTreeMap<String, BTreeMap<String, VirtualArchiveOccurrenceProof>>,
}

/// Production adapter that stores archives through standard Resource interfaces.
pub struct ResourceBackedVirtualArchive<S> {
    store: S,
    binding: String,
    catalog: ResourceArchiveCatalog,
}

impl<S: ArtifactStore + ArtifactResolver> ResourceBackedVirtualArchive<S> {
    /// Open over an admitted Resource provider and durable provider-side catalog.
    pub fn open(
        store: S,
        binding: impl Into<String>,
        catalog: ResourceArchiveCatalog,
    ) -> VirtualResult<Self> {
        let binding = binding.into();
        if binding.is_empty() || binding.chars().any(char::is_control) {
            return Err(VirtualError::Validation(
                "Resource-backed archive binding must be non-empty and printable".to_owned(),
            ));
        }
        for (resource_id, publication) in &catalog.publications {
            publication
                .verify()
                .map_err(|error| resource_error(&error))?;
            if resource_id != &publication.resource.resource_id {
                return Err(VirtualError::Source(
                    "Resource archive catalog key changed".to_owned(),
                ));
            }
        }
        Ok(Self {
            store,
            binding,
            catalog,
        })
    }

    /// Return the provider and its durable, non-semantic locator catalog.
    pub fn into_parts(self) -> (S, ResourceArchiveCatalog) {
        (self.store, self.catalog)
    }
}

impl<S: ArtifactStore + ArtifactResolver> VirtualArchive for ResourceBackedVirtualArchive<S> {
    fn binding(&self) -> &str {
        &self.binding
    }

    fn put(&mut self, object: &VirtualArchiveObject) -> VirtualResult<()> {
        if let Some(publication) = self
            .catalog
            .publications
            .get(&object.descriptor.resource_id)
        {
            if publication.resource != object.descriptor {
                return Err(VirtualError::Source(
                    "archive Resource ID was reused with a different descriptor".to_owned(),
                ));
            }
            self.catalog.occurrence_proofs.insert(
                object.descriptor.resource_id.clone(),
                object.occurrence_proofs.clone(),
            );
            return Ok(());
        }
        let intent = ResourceWriteIntent {
            write_id: format!("virtual-archive:{}", object.descriptor.resource_id),
            shape: ResourceShape::Object,
            media_type: VIRTUAL_ARCHIVE_MANIFEST_KIND.to_owned(),
            annotations: BTreeMap::new(),
        };
        intent.validate().map_err(|error| resource_error(&error))?;
        let session = self
            .store
            .begin_write(&intent)
            .map_err(|error| resource_error(&error))?;
        if session.write_id != intent.write_id {
            return Err(VirtualError::Source(
                "archive Resource store changed the write identity".to_owned(),
            ));
        }
        for (index, chunk) in object.bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
            self.store
                .write_chunk(&session, (index * MAX_WRITE_CHUNK) as u64, chunk)
                .map_err(|error| resource_error(&error))?;
        }
        let publication = self
            .store
            .commit_write(&session)
            .map_err(|error| resource_error(&error))?;
        publication
            .verify()
            .map_err(|error| resource_error(&error))?;
        if publication.resource != object.descriptor {
            return Err(VirtualError::Source(
                "Resource archive provider changed the framework descriptor".to_owned(),
            ));
        }
        self.catalog
            .publications
            .insert(object.descriptor.resource_id.clone(), publication);
        self.catalog.occurrence_proofs.insert(
            object.descriptor.resource_id.clone(),
            object.occurrence_proofs.clone(),
        );
        Ok(())
    }

    fn read_range(
        &mut self,
        descriptor: &ResourceHandle,
        offset: u64,
        max_bytes: u32,
    ) -> VirtualResult<Vec<u8>> {
        if max_bytes == 0 || max_bytes > MAX_VIRTUAL_ARCHIVE_CHUNK || max_bytes > MAX_READ_CHUNK {
            return Err(VirtualError::Validation(
                "virtual archive range exceeds the provider-neutral bound".to_owned(),
            ));
        }
        let publication = self
            .catalog
            .publications
            .get(&descriptor.resource_id)
            .ok_or_else(|| {
                VirtualError::NotFound("archive Resource locator is missing".to_owned())
            })?
            .clone();
        if publication.resource != *descriptor {
            return Err(VirtualError::Source(
                "archive locator catalog changed the semantic descriptor".to_owned(),
            ));
        }
        let chunk = self
            .store
            .read(descriptor, &publication.locators, offset, max_bytes)
            .map_err(|error| resource_error(&error))?;
        if chunk.offset != offset || chunk.bytes.len() > max_bytes as usize {
            return Err(VirtualError::Source(
                "archive provider returned an invalid byte range".to_owned(),
            ));
        }
        Ok(chunk.bytes)
    }

    fn occurrence_proof(
        &mut self,
        descriptor: &ResourceHandle,
        occurrence_id: &str,
    ) -> VirtualResult<VirtualArchiveOccurrenceProof> {
        self.catalog
            .occurrence_proofs
            .get(&descriptor.resource_id)
            .and_then(|proofs| proofs.get(occurrence_id))
            .cloned()
            .ok_or_else(|| VirtualError::NotFound(format!("archive occurrence {occurrence_id}")))
    }
}

/// Canonically encode a manifest and derive its semantic cold Resource descriptor.
pub fn virtual_archive_record(
    manifest: &VirtualArchiveManifest,
) -> VirtualResult<VirtualArchiveObject> {
    let bytes =
        canonical_bytes(manifest).map_err(|error| VirtualError::Validation(error.to_string()))?;
    let (occurrence_root_digest, occurrence_proofs) = occurrence_proofs(manifest, &bytes)?;
    let descriptor = ResourceCandidate {
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: VIRTUAL_ARCHIVE_MANIFEST_KIND.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: format!("sha256:{}", cymule_core::sha256_bytes(&bytes)),
            size: bytes.len() as u64,
        },
        manifest: None,
        annotations: BTreeMap::new(),
    }
    .seal()
    .map_err(|error| VirtualError::Validation(error.to_string()))?;
    Ok(VirtualArchiveObject {
        descriptor,
        bytes,
        occurrence_root_digest,
        occurrence_proofs,
    })
}

/// Verify one range proof against the root retained by the certificate.
pub fn verify_occurrence_proof(
    root_digest: &str,
    occurrence_count: u64,
    proof: &VirtualArchiveOccurrenceProof,
    occurrence: &WorkOccurrence,
) -> VirtualResult<()> {
    let occurrence_bytes =
        canonical_bytes(occurrence).map_err(|error| VirtualError::Validation(error.to_string()))?;
    if proof.occurrence_id != occurrence.occurrence_id
        || proof.index >= occurrence_count
        || proof.length != occurrence_bytes.len() as u64
        || proof.digest != format!("sha256:{}", cymule_core::sha256_bytes(&occurrence_bytes))
    {
        return Err(VirtualError::Source(
            "archive occurrence range does not match its proof".to_owned(),
        ));
    }
    let mut digest = occurrence_leaf(&proof.occurrence_id, &proof.digest)?;
    let mut position = proof.index;
    let mut width = occurrence_count;
    for step in &proof.path {
        if width <= 1 {
            return Err(VirtualError::Source(
                "archive occurrence proof is too long".to_owned(),
            ));
        }
        let expected = if position.is_multiple_of(2) {
            MerkleSide::Right
        } else {
            MerkleSide::Left
        };
        if step.side != expected {
            return Err(VirtualError::Source(
                "archive occurrence proof index path changed".to_owned(),
            ));
        }
        digest = match step.side {
            MerkleSide::Left => occurrence_node(&step.digest, &digest)?,
            MerkleSide::Right => occurrence_node(&digest, &step.digest)?,
        };
        position /= 2;
        width = width.div_ceil(2);
    }
    if width != 1 || digest != root_digest {
        return Err(VirtualError::Source(
            "archive occurrence proof does not reach the certificate root".to_owned(),
        ));
    }
    Ok(())
}

fn occurrence_proofs(
    manifest: &VirtualArchiveManifest,
    bytes: &[u8],
) -> VirtualResult<(String, BTreeMap<String, VirtualArchiveOccurrenceProof>)> {
    let mut ranges = Vec::new();
    let mut leaves = Vec::new();
    for (occurrence_id, occurrence) in &manifest.occurrences {
        let occurrence_bytes = canonical_bytes(occurrence)
            .map_err(|error| VirtualError::Validation(error.to_string()))?;
        if occurrence_bytes.len() > MAX_VIRTUAL_ARCHIVE_CHUNK as usize {
            return Err(VirtualError::Validation(
                "one archived occurrence exceeds the bounded range-read contract".to_owned(),
            ));
        }
        let mut needle = canonical_bytes(occurrence_id)
            .map_err(|error| VirtualError::Validation(error.to_string()))?;
        needle.push(b':');
        needle.extend_from_slice(&occurrence_bytes);
        let key_offset = find_unique(bytes, &needle)?;
        let offset = key_offset + needle.len() - occurrence_bytes.len();
        let digest = format!("sha256:{}", cymule_core::sha256_bytes(&occurrence_bytes));
        leaves.push(occurrence_leaf(occurrence_id, &digest)?);
        ranges.push((
            occurrence_id.clone(),
            offset as u64,
            occurrence_bytes.len() as u64,
            digest,
        ));
    }
    let levels = merkle_levels(leaves)?;
    let root = levels
        .last()
        .and_then(|level| level.first())
        .cloned()
        .ok_or_else(|| VirtualError::Validation("virtual archive has no occurrences".to_owned()))?;
    let mut proofs = BTreeMap::new();
    for (index, (occurrence_id, offset, length, digest)) in ranges.into_iter().enumerate() {
        let mut position = index;
        let mut path = Vec::new();
        for level in levels.iter().take(levels.len().saturating_sub(1)) {
            let (sibling, side) = if position.is_multiple_of(2) {
                (
                    level.get(position + 1).unwrap_or(&level[position]),
                    MerkleSide::Right,
                )
            } else {
                (&level[position - 1], MerkleSide::Left)
            };
            path.push(MerkleStep {
                side,
                digest: sibling.clone(),
            });
            position /= 2;
        }
        proofs.insert(
            occurrence_id.clone(),
            VirtualArchiveOccurrenceProof {
                occurrence_id,
                index: index as u64,
                offset,
                length,
                digest,
                path,
            },
        );
    }
    Ok((root, proofs))
}

fn merkle_levels(mut current: Vec<String>) -> VirtualResult<Vec<Vec<String>>> {
    let mut levels = Vec::new();
    while !current.is_empty() {
        levels.push(current.clone());
        if current.len() == 1 {
            break;
        }
        current = current
            .chunks(2)
            .map(|pair| occurrence_node(&pair[0], pair.get(1).unwrap_or(&pair[0])))
            .collect::<VirtualResult<Vec<_>>>()?;
    }
    Ok(levels)
}

fn occurrence_leaf(occurrence_id: &str, digest: &str) -> VirtualResult<String> {
    cymule_core::content_id(OCCURRENCE_LEAF_DOMAIN, &(occurrence_id, digest))
        .map_err(|error| VirtualError::Validation(error.to_string()))
}

fn occurrence_node(left: &str, right: &str) -> VirtualResult<String> {
    cymule_core::content_id(OCCURRENCE_NODE_DOMAIN, &(left, right))
        .map_err(|error| VirtualError::Validation(error.to_string()))
}

fn find_unique(haystack: &[u8], needle: &[u8]) -> VirtualResult<usize> {
    let mut matches = haystack
        .windows(needle.len())
        .enumerate()
        .filter_map(|(index, candidate)| (candidate == needle).then_some(index));
    let first = matches.next().ok_or_else(|| {
        VirtualError::Validation("canonical occurrence bytes are absent from archive".to_owned())
    })?;
    if matches.next().is_some() {
        return Err(VirtualError::Validation(
            "canonical occurrence bytes are ambiguous in archive".to_owned(),
        ));
    }
    Ok(first)
}

fn resource_error(error: &cymule_resource::ResourceError) -> VirtualError {
    VirtualError::Source(error.to_string())
}
