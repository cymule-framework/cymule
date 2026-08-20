use cymule_core::canonical_bytes;
use cymule_resource::{
    RESOURCE_VERSION, ResourceCandidate, ResourceHandle, ResourceIntegrity, ResourceShape,
};

use crate::{VirtualArchiveManifest, VirtualError, VirtualResult};

/// Stable media type for a canonical virtual archive manifest.
pub const VIRTUAL_ARCHIVE_MANIFEST_KIND: &str =
    "application/vnd.cymule.virtual-archive-manifest+json";

/// Replaceable immutable byte archive for completed virtual history.
pub trait VirtualArchive {
    /// Immutable implementation binding selected for this operation.
    fn binding(&self) -> &str;

    /// Idempotently store exact bytes under their framework-computed reference.
    fn put(&mut self, descriptor: &ResourceHandle, bytes: &[u8]) -> VirtualResult<()>;

    /// Load exact bytes for one content-addressed reference.
    fn get(&mut self, descriptor: &ResourceHandle) -> VirtualResult<Vec<u8>>;
}

/// Cold archive manifest descriptor and exact bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualArchiveObject {
    /// Semantic content descriptor retained in hot state.
    pub descriptor: ResourceHandle,
    /// Exact canonical bytes sent only to the cold archive.
    pub bytes: Vec<u8>,
}

/// Canonically encode a manifest and derive its semantic cold Resource descriptor.
pub fn virtual_archive_record(
    manifest: &VirtualArchiveManifest,
) -> VirtualResult<VirtualArchiveObject> {
    let bytes =
        canonical_bytes(manifest).map_err(|error| VirtualError::Validation(error.to_string()))?;
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
        annotations: std::collections::BTreeMap::new(),
    }
    .seal()
    .map_err(|error| VirtualError::Validation(error.to_string()))?;
    Ok(VirtualArchiveObject { descriptor, bytes })
}
