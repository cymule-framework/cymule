use std::io::Write;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ResourceError, ResourceHandle, ResourceIntegrity, ResourceListProof, ResourceLocatorSet,
    ResourceManifestEntry, ResourcePublication, ResourceResult, ResourceShape,
};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Maximum bytes requested from a resolver in one call.
pub const MAX_READ_CHUNK: u32 = 8 * 1024 * 1024;
/// Maximum directory/collection entries requested in one page.
pub const MAX_LIST_PAGE: u32 = 1000;

/// Resolver observation for one immutable or live resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceObservation {
    /// Observed media type.
    pub media_type: String,
    /// Observed replay/integrity evidence.
    pub integrity: ResourceIntegrity,
}

/// Bounded byte range returned by a resolver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceChunk {
    /// Byte offset of this chunk.
    pub offset: u64,
    /// Returned bytes.
    pub bytes: Vec<u8>,
    /// Whether the resource ended after this chunk.
    pub eof: bool,
}

/// One directory or collection entry.
pub type ResourceEntry = ResourceManifestEntry;

/// One bounded directory or collection page.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourcePage {
    /// Entries in provider-defined stable page order.
    pub entries: Vec<ResourceEntry>,
    /// Opaque cursor for the next page.
    pub next_cursor: Option<String>,
    /// Inclusion evidence binding every entry to the exact content manifest.
    pub proof: ResourceListProof,
}

/// Replaceable read/list boundary for external resources.
pub trait ArtifactResolver {
    /// Observe current metadata without reading the full value.
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation>;

    /// Read one bounded byte range.
    fn read(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk>;

    /// List one bounded collection/directory/snapshot page.
    fn list(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> ResourceResult<ResourcePage>;
}

/// Validating facade over one resolver adapter.
pub struct ResourceClient<R> {
    resolver: R,
    list_frontiers: std::collections::BTreeMap<(String, String), u64>,
}

impl<R: ArtifactResolver> ResourceClient<R> {
    /// Wrap one resolver implementation.
    pub const fn new(resolver: R) -> Self {
        Self {
            resolver,
            list_frontiers: std::collections::BTreeMap::new(),
        }
    }

    /// Verify that a resolver observes the retained media type and integrity or
    /// immutable-version evidence.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid Handle, resolver failure, or observation
    /// that would reinterpret the retained Resource.
    pub fn observe(
        &mut self,
        publication: &ResourcePublication,
    ) -> ResourceResult<ResourceObservation> {
        publication.verify()?;
        let observation = self
            .resolver
            .stat(&publication.resource, &publication.locators)?;
        if observation.media_type != publication.resource.media_type
            || observation.integrity != publication.resource.integrity
        {
            return Err(ResourceError::Integrity(
                "resolver observation does not match the retained Resource".to_owned(),
            ));
        }
        Ok(observation)
    }

    /// Read and verify a content-addressed object into a caller-owned sink.
    ///
    /// The caller chooses the chunk size and sink; Cymule never requires the
    /// full object to exist in memory.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid descriptors/chunk sizes, malformed resolver
    /// responses, sink failures, size mismatch, or digest mismatch.
    pub fn copy_to(
        &mut self,
        publication: &ResourcePublication,
        chunk_size: u32,
        sink: &mut impl Write,
    ) -> ResourceResult<u64> {
        self.observe(publication)?;
        if chunk_size == 0 || chunk_size > MAX_READ_CHUNK {
            return Err(ResourceError::Validation(format!(
                "resource read chunk must be 1..={MAX_READ_CHUNK} bytes"
            )));
        }
        let ResourceIntegrity::Content {
            digest: expected_digest,
            size: expected_size,
        } = &publication.resource.integrity
        else {
            return Err(ResourceError::Validation(
                "copy_to requires content-addressed integrity evidence".to_owned(),
            ));
        };
        let mut offset = 0_u64;
        let mut hasher = Sha256::new();
        loop {
            let chunk = self.resolver.read(
                &publication.resource,
                &publication.locators,
                offset,
                chunk_size,
            )?;
            if chunk.offset != offset || chunk.bytes.len() > chunk_size as usize {
                return Err(ResourceError::Substrate(
                    "resolver returned an invalid resource chunk".to_owned(),
                ));
            }
            if chunk.bytes.is_empty() && !chunk.eof {
                return Err(ResourceError::Substrate(
                    "resolver returned an empty non-terminal chunk".to_owned(),
                ));
            }
            sink.write_all(&chunk.bytes)
                .map_err(|error| ResourceError::Substrate(error.to_string()))?;
            hasher.update(&chunk.bytes);
            offset = offset
                .checked_add(chunk.bytes.len() as u64)
                .ok_or_else(|| ResourceError::Integrity("resource size overflow".to_owned()))?;
            if offset > *expected_size {
                return Err(ResourceError::Integrity(format!(
                    "resource exceeded expected size {expected_size}"
                )));
            }
            if chunk.eof {
                break;
            }
        }
        if offset != *expected_size {
            return Err(ResourceError::Integrity(format!(
                "resource size {offset} does not match {expected_size}"
            )));
        }
        let digest = hasher.finalize();
        let mut observed = String::with_capacity(71);
        observed.push_str("sha256:");
        for byte in digest {
            observed.push(char::from(HEX[usize::from(byte >> 4)]));
            observed.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        if &observed != expected_digest {
            return Err(ResourceError::Integrity(format!(
                "resource digest {observed} does not match {expected_digest}"
            )));
        }
        Ok(offset)
    }

    /// List one validated bounded page.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-listable shape, invalid limit, malformed
    /// entries, duplicate names, cursor violations, or resolver failure.
    pub fn list_page(
        &mut self,
        publication: &ResourcePublication,
        cursor: Option<&str>,
        limit: u32,
    ) -> ResourceResult<ResourcePage> {
        self.observe(publication)?;
        if !matches!(
            publication.resource.shape,
            ResourceShape::Collection | ResourceShape::Directory | ResourceShape::Snapshot
        ) {
            return Err(ResourceError::Validation(
                "only collection, directory, or snapshot resources can be listed".to_owned(),
            ));
        }
        if limit == 0 || limit > MAX_LIST_PAGE {
            return Err(ResourceError::Validation(format!(
                "resource page limit must be 1..={MAX_LIST_PAGE}"
            )));
        }
        let expected_start = match cursor {
            None => 0,
            Some(cursor) => *self
                .list_frontiers
                .get(&(publication.resource.resource_id.clone(), cursor.to_owned()))
                .ok_or_else(|| {
                    ResourceError::Validation(
                        "Resource list cursor has no verified predecessor page".to_owned(),
                    )
                })?,
        };
        let page =
            self.resolver
                .list(&publication.resource, &publication.locators, cursor, limit)?;
        if page.entries.len() > limit as usize
            || page.next_cursor.as_deref().is_some_and(str::is_empty)
            || page.next_cursor.as_deref() == cursor
        {
            return Err(ResourceError::Substrate(
                "resolver returned an invalid resource page".to_owned(),
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for entry in &page.entries {
            if entry.name.is_empty()
                || entry.name.starts_with('/')
                || entry.name.contains('\\')
                || entry.name.chars().any(char::is_control)
                || entry
                    .name
                    .split('/')
                    .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
                || !names.insert(&entry.name)
            {
                return Err(ResourceError::Substrate(
                    "resolver returned an unsafe or duplicate resource entry".to_owned(),
                ));
            }
            entry.resource.verify()?;
        }
        let manifest = publication.resource.manifest.as_ref().ok_or_else(|| {
            ResourceError::Validation(
                "exact Resource listing requires a content-addressed manifest".to_owned(),
            )
        })?;
        page.proof
            .verify_page(manifest, &page.entries, cursor, page.next_cursor.as_deref())?;
        if page.proof.start_index != expected_start {
            return Err(ResourceError::Substrate(
                "Resource list page does not continue its verified predecessor".to_owned(),
            ));
        }
        let end = page
            .proof
            .start_index
            .checked_add(page.entries.len() as u64)
            .ok_or_else(|| ResourceError::Integrity("manifest page range overflow".to_owned()))?;
        if page.next_cursor.is_none() && end != manifest.entry_count {
            return Err(ResourceError::Substrate(
                "terminal Resource list page does not close the exact manifest".to_owned(),
            ));
        }
        if let Some(next_cursor) = &page.next_cursor {
            let key = (
                publication.resource.resource_id.clone(),
                next_cursor.clone(),
            );
            if self
                .list_frontiers
                .get(&key)
                .is_some_and(|existing| *existing != end)
            {
                return Err(ResourceError::Substrate(
                    "resolver reused one opaque cursor for different manifest frontiers".to_owned(),
                ));
            }
            self.list_frontiers.insert(key, end);
        }
        Ok(page)
    }

    /// Consume the client and return its adapter.
    pub fn into_inner(self) -> R {
        self.resolver
    }
}
