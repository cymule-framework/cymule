use cymule_core::{ArtifactRecord, ArtifactRef, canonical_bytes, sha256_bytes};

use crate::{VirtualArchiveManifest, VirtualError, VirtualResult};

/// Stable media type for a canonical virtual archive manifest.
pub const VIRTUAL_ARCHIVE_MANIFEST_KIND: &str =
    "application/vnd.cymule.virtual-archive-manifest+json";

/// Replaceable immutable byte archive for completed virtual history.
pub trait VirtualArchive {
    /// Immutable implementation binding selected for this operation.
    fn binding(&self) -> &str;

    /// Idempotently store exact bytes under their framework-computed reference.
    fn put(&mut self, reference: &ArtifactRef, bytes: &[u8]) -> VirtualResult<()>;

    /// Load exact bytes for one content-addressed reference.
    fn get(&mut self, reference: &ArtifactRef) -> VirtualResult<Vec<u8>>;
}

/// Canonically encode a manifest and derive its ordinary Cymule Artifact ID.
pub fn virtual_archive_record(manifest: &VirtualArchiveManifest) -> VirtualResult<ArtifactRecord> {
    let bytes =
        canonical_bytes(manifest).map_err(|error| VirtualError::Validation(error.to_string()))?;
    let mut preimage = Vec::with_capacity(VIRTUAL_ARCHIVE_MANIFEST_KIND.len() + bytes.len() + 20);
    preimage.extend_from_slice(b"cymule.artifact/1\0");
    preimage.extend_from_slice(VIRTUAL_ARCHIVE_MANIFEST_KIND.as_bytes());
    preimage.push(0);
    preimage.extend_from_slice(&bytes);
    Ok(ArtifactRecord {
        reference: ArtifactRef {
            artifact_id: format!("sha256:{}", sha256_bytes(&preimage)),
            kind: VIRTUAL_ARCHIVE_MANIFEST_KIND.to_owned(),
        },
        bytes,
    })
}
