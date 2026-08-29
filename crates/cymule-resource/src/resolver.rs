use std::io::Write;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::manifest::{cursor_digest, validate_manifest_name, validate_manifest_page_order};
use crate::{
    ResourceError, ResourceHandle, ResourceIntegrity, ResourceListProof, ResourceLocatorSet,
    ResourceManifestEntry, ResourceManifestStreamVerifier, ResourcePublication, ResourceResult,
    ResourceShape,
};

const HEX: &[u8; 16] = b"0123456789abcdef";

enum ResourceCopyVerifier {
    Manifest {
        verifier: ResourceManifestStreamVerifier,
        expected: crate::ResourceManifestDescriptor,
    },
    Content {
        hasher: Sha256,
        expected_digest: String,
    },
}

impl ResourceCopyVerifier {
    fn push(&mut self, bytes: &[u8]) -> ResourceResult<()> {
        match self {
            Self::Manifest { verifier, .. } => verifier.push(bytes),
            Self::Content { hasher, .. } => {
                hasher.update(bytes);
                Ok(())
            }
        }
    }

    fn finish(self) -> ResourceResult<()> {
        match self {
            Self::Manifest { verifier, expected } => {
                if verifier.finish()? != expected {
                    return Err(ResourceError::Integrity {
                        code: "resource_manifest_bytes_descriptor_mismatch".to_owned(),
                        message:
                            "Resource manifest bytes do not close their exact semantic descriptor"
                                .to_owned(),
                    });
                }
            }
            Self::Content {
                hasher,
                expected_digest,
            } => {
                let digest = hasher.finalize();
                let mut observed = String::with_capacity(71);
                observed.push_str("sha256:");
                for byte in digest {
                    observed.push(char::from(HEX[usize::from(byte >> 4)]));
                    observed.push(char::from(HEX[usize::from(byte & 0x0f)]));
                }
                if observed != expected_digest {
                    return Err(ResourceError::Integrity {
                        code: "resource_digest_mismatch".to_owned(),
                        message: format!(
                            "resource digest {observed} does not match {expected_digest}"
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Maximum bytes requested from a resolver in one call.
pub const MAX_READ_CHUNK: u32 = 8 * 1024 * 1024;
/// Maximum directory/collection entries requested in one page.
pub const MAX_LIST_PAGE: u32 = 1000;
/// Frozen self-contained list cursor generation.
pub const RESOURCE_LIST_CURSOR_VERSION: &str = "cymule.resource-list-cursor/3";

const RESOURCE_LIST_PROGRESS_VERSION: &str = "cymule.resource-list-progress/3";
const MAX_LIST_CURSOR_BYTES: usize = 16 * 1024;

/// Self-contained, content-authenticated continuation for one verified list
/// page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceListCursor {
    /// Cursor wire version.
    pub cursor_version: String,
    /// Content identity of every following field.
    pub cursor_id: String,
    /// Exact semantic Resource being listed.
    pub resource_id: String,
    /// Exact semantic manifest descriptor content ID.
    pub manifest_digest: String,
    /// Immutable resolver implementation binding.
    pub resolver_binding: String,
    /// Digest of the cursor that requested the page represented here.
    pub request_cursor_digest: String,
    /// Exact page limit admitted for this cursor chain.
    pub request_limit: u32,
    /// First manifest index represented by the preceding page.
    pub start_index: u64,
    /// First manifest index of the next page.
    pub next_index: u64,
    /// Exact final name of the preceding page.
    pub last_name: String,
    /// Content identity of the exact preceding page entries and frontier.
    pub progress_digest: String,
}

#[derive(Serialize)]
struct ResourceListProgressIdentity<'a> {
    resource_id: &'a str,
    manifest_digest: &'a str,
    resolver_binding: &'a str,
    request_cursor_digest: &'a str,
    request_limit: u32,
    start_index: u64,
    next_index: u64,
    last_name: &'a str,
    entries: &'a [ResourceManifestEntry],
}

#[derive(Serialize)]
struct ResourceListCursorIdentity<'a> {
    resource_id: &'a str,
    manifest_digest: &'a str,
    resolver_binding: &'a str,
    request_cursor_digest: &'a str,
    request_limit: u32,
    start_index: u64,
    next_index: u64,
    last_name: &'a str,
    progress_digest: &'a str,
}

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

impl ResourceListCursor {
    /// Seal a successor cursor for one exact non-terminal verified page.
    ///
    /// Adapters call this after selecting the page's indexed entries and use
    /// the returned token as `ResourcePage::next_cursor`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid authority, an empty/non-progressing page,
    /// a terminal frontier, or failed identity derivation.
    pub fn for_page(
        publication: &ResourcePublication,
        request_cursor: Option<&str>,
        request_limit: u32,
        start_index: u64,
        entries: &[ResourceManifestEntry],
    ) -> ResourceResult<String> {
        publication.verify()?;
        if request_limit == 0 || request_limit > MAX_LIST_PAGE {
            return Err(ResourceError::Validation(format!(
                "resource page limit must be 1..={MAX_LIST_PAGE}"
            )));
        }
        let manifest = publication.resource.manifest.as_ref().ok_or_else(|| {
            ResourceError::Validation(
                "exact Resource listing requires a content-addressed manifest".to_owned(),
            )
        })?;
        let page_count = u64::try_from(entries.len()).map_err(|_| {
            ResourceError::Validation("Resource list page count exceeds platform bounds".to_owned())
        })?;
        if entries.is_empty() || page_count > u64::from(request_limit) {
            return Err(ResourceError::Validation(
                "a successor Resource cursor requires one bounded progressing page".to_owned(),
            ));
        }
        validate_manifest_page_order(entries)?;
        let first_entry = entries.first().ok_or_else(|| {
            ResourceError::Validation("successor Resource page lost its first entry".to_owned())
        })?;
        let last_name = &entries
            .last()
            .ok_or_else(|| {
                ResourceError::Validation("successor Resource page lost its last entry".to_owned())
            })?
            .name;
        match request_cursor {
            None if start_index == 0 => {}
            Some(request_cursor) => {
                let request = Self::decode(request_cursor)?;
                request.verify_request(publication, request_limit)?;
                if request.next_index != start_index || request.last_name >= first_entry.name {
                    return Err(ResourceError::Validation(
                        "Resource list page does not strictly continue its predecessor".to_owned(),
                    ));
                }
            }
            None => {
                return Err(ResourceError::Validation(
                    "a non-initial Resource list page requires its exact predecessor cursor"
                        .to_owned(),
                ));
            }
        }
        let next_index = start_index
            .checked_add(page_count)
            .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation("Resource list frontier overflow".to_owned())
            })?;
        if next_index >= manifest.entry_count {
            return Err(ResourceError::Validation(
                "a terminal Resource page must not produce a successor cursor".to_owned(),
            ));
        }
        let request_cursor_digest = cursor_digest(request_cursor)?;
        let progress_digest = cymule_core::content_id(
            RESOURCE_LIST_PROGRESS_VERSION,
            &ResourceListProgressIdentity {
                resource_id: &publication.resource.resource_id,
                manifest_digest: &manifest.digest,
                resolver_binding: &publication.locators.resolver_binding,
                request_cursor_digest: &request_cursor_digest,
                request_limit,
                start_index,
                next_index,
                last_name,
                entries,
            },
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let cursor_id = cursor_id(&ResourceListCursorIdentity {
            resource_id: &publication.resource.resource_id,
            manifest_digest: &manifest.digest,
            resolver_binding: &publication.locators.resolver_binding,
            request_cursor_digest: &request_cursor_digest,
            request_limit,
            start_index,
            next_index,
            last_name,
            progress_digest: &progress_digest,
        })?;
        let cursor = Self {
            cursor_version: RESOURCE_LIST_CURSOR_VERSION.to_owned(),
            cursor_id,
            resource_id: publication.resource.resource_id.clone(),
            manifest_digest: manifest.digest.clone(),
            resolver_binding: publication.locators.resolver_binding.clone(),
            request_cursor_digest,
            request_limit,
            start_index,
            next_index,
            last_name: last_name.clone(),
            progress_digest,
        };
        cursor.encode()
    }

    /// Decode an exact canonical URL-safe token and authenticate its content.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed, non-canonical, unsupported, or changed
    /// cursor bytes.
    pub fn decode(token: &str) -> ResourceResult<Self> {
        if token.is_empty() || token.len() > MAX_LIST_CURSOR_BYTES {
            return Err(ResourceError::Validation(
                "Resource list cursor has an invalid encoded size".to_owned(),
            ));
        }
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|error| {
                ResourceError::Validation(format!("invalid Resource list cursor: {error}"))
            })?;
        if bytes.len() > MAX_LIST_CURSOR_BYTES
            || base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes) != token
        {
            return Err(ResourceError::Validation(
                "Resource list cursor is not canonical URL-safe base64".to_owned(),
            ));
        }
        let cursor: Self = cymule_core::decode_json(&bytes)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if cymule_core::canonical_bytes(&cursor)
            .map_err(|error| ResourceError::Validation(error.to_string()))?
            != bytes
        {
            return Err(ResourceError::Validation(
                "Resource list cursor JSON is not canonical".to_owned(),
            ));
        }
        cursor.verify()?;
        Ok(cursor)
    }

    fn encode(&self) -> ResourceResult<String> {
        self.verify()?;
        let bytes = cymule_core::canonical_bytes(self)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if bytes.len() > MAX_LIST_CURSOR_BYTES {
            return Err(ResourceError::Validation(
                "Resource list cursor exceeds its encoded bound".to_owned(),
            ));
        }
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
    }

    fn verify(&self) -> ResourceResult<()> {
        if self.cursor_version != RESOURCE_LIST_CURSOR_VERSION {
            return Err(ResourceError::Validation(format!(
                "unsupported Resource list cursor version {:?}",
                self.cursor_version
            )));
        }
        cymule_core::validate_content_id("Resource list cursor", &self.cursor_id)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        cymule_core::validate_content_id("Resource list cursor Resource", &self.resource_id)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        cymule_core::validate_content_id("Resource list cursor manifest", &self.manifest_digest)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        cymule_core::validate_content_id(
            "Resource list cursor predecessor",
            &self.request_cursor_digest,
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        cymule_core::validate_content_id("Resource list cursor progress", &self.progress_digest)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let binding_scalars = self.resolver_binding.chars().count();
        if binding_scalars == 0
            || binding_scalars > 512
            || self.resolver_binding.chars().any(char::is_control)
            || self.request_limit == 0
            || self.request_limit > MAX_LIST_PAGE
            || self.start_index >= self.next_index
            || self.next_index > cymule_core::MAX_EXACT_INTEGER
            || self.next_index - self.start_index > u64::from(self.request_limit)
        {
            return Err(ResourceError::Validation(
                "Resource list cursor contains an invalid request or frontier".to_owned(),
            ));
        }
        validate_manifest_name(&self.last_name)?;
        let expected = cursor_id(&ResourceListCursorIdentity {
            resource_id: &self.resource_id,
            manifest_digest: &self.manifest_digest,
            resolver_binding: &self.resolver_binding,
            request_cursor_digest: &self.request_cursor_digest,
            request_limit: self.request_limit,
            start_index: self.start_index,
            next_index: self.next_index,
            last_name: &self.last_name,
            progress_digest: &self.progress_digest,
        })?;
        if self.cursor_id != expected {
            return Err(ResourceError::Integrity {
                code: "resource_list_cursor_identity_mismatch".to_owned(),
                message: format!(
                    "Resource list cursor {} does not match {expected}",
                    self.cursor_id
                ),
            });
        }
        Ok(())
    }

    fn verify_request(
        &self,
        publication: &ResourcePublication,
        request_limit: u32,
    ) -> ResourceResult<()> {
        self.verify()?;
        let manifest = publication.resource.manifest.as_ref().ok_or_else(|| {
            ResourceError::Validation(
                "exact Resource listing requires a content-addressed manifest".to_owned(),
            )
        })?;
        if self.resource_id != publication.resource.resource_id
            || self.manifest_digest != manifest.digest
            || self.resolver_binding != publication.locators.resolver_binding
            || self.request_limit != request_limit
            || self.next_index >= manifest.entry_count
        {
            return Err(ResourceError::Validation(
                "Resource list cursor does not match this exact request authority".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_successor(
        &self,
        publication: &ResourcePublication,
        request_cursor: Option<&str>,
        request_limit: u32,
        start_index: u64,
        entries: &[ResourceManifestEntry],
    ) -> ResourceResult<()> {
        self.verify_request(publication, request_limit)?;
        let manifest = publication
            .resource
            .manifest
            .as_ref()
            .expect("verified publication");
        let page_count = u64::try_from(entries.len()).map_err(|_| ResourceError::Integrity {
            code: "resource_list_page_count_overflow".to_owned(),
            message: "Resource list page count exceeds platform bounds".to_owned(),
        })?;
        let expected_next =
            start_index
                .checked_add(page_count)
                .ok_or_else(|| ResourceError::Integrity {
                    code: "resource_list_frontier_overflow".to_owned(),
                    message: "Resource list frontier overflow".to_owned(),
                })?;
        let expected_request_digest = cursor_digest(request_cursor)?;
        validate_manifest_page_order(entries)?;
        let expected_last_name = &entries
            .last()
            .ok_or_else(|| ResourceError::Integrity {
                code: "resource_list_successor_empty_page".to_owned(),
                message: "Resource list successor cursor cannot bind an empty page".to_owned(),
            })?
            .name;
        let expected_progress = cymule_core::content_id(
            RESOURCE_LIST_PROGRESS_VERSION,
            &ResourceListProgressIdentity {
                resource_id: &publication.resource.resource_id,
                manifest_digest: &manifest.digest,
                resolver_binding: &publication.locators.resolver_binding,
                request_cursor_digest: &expected_request_digest,
                request_limit,
                start_index,
                next_index: expected_next,
                last_name: expected_last_name,
                entries,
            },
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if entries.is_empty()
            || self.request_cursor_digest != expected_request_digest
            || self.start_index != start_index
            || self.next_index != expected_next
            || self.last_name != expected_last_name.as_str()
            || self.progress_digest != expected_progress
        {
            return Err(ResourceError::Substrate {
                code: "resource_list_successor_progress_mismatch".to_owned(),
                message: "Resource list successor cursor does not bind the exact page progress"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

fn cursor_id(identity: &ResourceListCursorIdentity<'_>) -> ResourceResult<String> {
    cymule_core::content_id(RESOURCE_LIST_CURSOR_VERSION, identity)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Replaceable read/list boundary for external resources.
pub trait ArtifactResolver {
    /// Observe current metadata and retained integrity evidence.
    /// Content-addressed adapters verify bytes through bounded streaming.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid authority, integrity failure, or provider I/O.
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation>;

    /// Read one bounded byte range.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid authority/range, integrity failure, or provider I/O.
    fn read(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk>;

    /// List one bounded collection/directory/snapshot page.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid authority/cursor, integrity failure, or provider I/O.
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
}

impl<R: ArtifactResolver> ResourceClient<R> {
    /// Wrap one resolver implementation.
    pub const fn new(resolver: R) -> Self {
        Self { resolver }
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
            return Err(ResourceError::Integrity {
                code: "resource_resolver_observation_mismatch".to_owned(),
                message: "resolver observation does not match the retained Resource".to_owned(),
            });
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
        let mut verifier = if let Some(expected) = &publication.resource.manifest {
            ResourceCopyVerifier::Manifest {
                verifier: ResourceManifestStreamVerifier::new(),
                expected: expected.clone(),
            }
        } else {
            ResourceCopyVerifier::Content {
                hasher: Sha256::new(),
                expected_digest: expected_digest.clone(),
            }
        };
        let mut offset = 0_u64;
        loop {
            let chunk = self.resolver.read(
                &publication.resource,
                &publication.locators,
                offset,
                chunk_size,
            )?;
            if chunk.offset != offset || chunk.bytes.len() > chunk_size as usize {
                return Err(ResourceError::Substrate {
                    code: "resource_resolver_chunk_invalid".to_owned(),
                    message: "resolver returned an invalid resource chunk".to_owned(),
                });
            }
            if chunk.bytes.is_empty() && !chunk.eof {
                return Err(ResourceError::Substrate {
                    code: "resource_resolver_chunk_empty_nonterminal".to_owned(),
                    message: "resolver returned an empty non-terminal chunk".to_owned(),
                });
            }
            let next_offset = offset
                .checked_add(chunk.bytes.len() as u64)
                .ok_or_else(|| ResourceError::Integrity {
                    code: "resource_size_overflow".to_owned(),
                    message: "resource size overflow".to_owned(),
                })?;
            if next_offset > *expected_size {
                return Err(ResourceError::Integrity {
                    code: "resource_size_exceeded".to_owned(),
                    message: format!("resource exceeded expected size {expected_size}"),
                });
            }
            sink.write_all(&chunk.bytes)
                .map_err(|error| ResourceError::Substrate {
                    code: "resource_sink_write_failed".to_owned(),
                    message: error.to_string(),
                })?;
            verifier.push(&chunk.bytes)?;
            offset = next_offset;
            if chunk.eof {
                break;
            }
        }
        if offset != *expected_size {
            return Err(ResourceError::Integrity {
                code: "resource_size_mismatch".to_owned(),
                message: format!("resource size {offset} does not match {expected_size}"),
            });
        }
        verifier.finish()?;
        Ok(offset)
    }

    /// List one validated bounded page.
    ///
    /// The manifest descriptor and returned inclusion proof are the page's
    /// integrity authority. This path deliberately does not call `stat`, which
    /// would force a complete content scan before every bounded page.
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
        publication.verify()?;
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
        let request_cursor = cursor.map(ResourceListCursor::decode).transpose()?;
        let expected_start = request_cursor
            .as_ref()
            .map_or(0, |cursor| cursor.next_index);
        if let Some(cursor) = &request_cursor {
            cursor.verify_request(publication, limit)?;
        }
        let page =
            self.resolver
                .list(&publication.resource, &publication.locators, cursor, limit)?;
        if page.entries.len() > limit as usize
            || page
                .next_cursor
                .as_deref()
                .is_some_and(|next| Some(next) == cursor)
        {
            return Err(ResourceError::Substrate {
                code: "resource_resolver_page_invalid".to_owned(),
                message: "resolver returned an invalid resource page".to_owned(),
            });
        }
        let manifest = publication.resource.manifest.as_ref().ok_or_else(|| {
            ResourceError::Validation(
                "exact Resource listing requires a content-addressed manifest".to_owned(),
            )
        })?;
        page.proof
            .verify_page(manifest, &page.entries, cursor, page.next_cursor.as_deref())?;
        if page.proof.start_index != expected_start {
            return Err(ResourceError::Substrate {
                code: "resource_list_predecessor_discontinuity".to_owned(),
                message: "Resource list page does not continue its verified predecessor".to_owned(),
            });
        }
        let page_count =
            u64::try_from(page.entries.len()).map_err(|_| ResourceError::Integrity {
                code: "resource_manifest_page_count_overflow".to_owned(),
                message: "manifest page count exceeds platform bounds".to_owned(),
            })?;
        let end = page
            .proof
            .start_index
            .checked_add(page_count)
            .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| ResourceError::Integrity {
                code: "resource_manifest_page_range_overflow".to_owned(),
                message: "manifest page range overflow".to_owned(),
            })?;
        match &page.next_cursor {
            None if end != manifest.entry_count => {
                return Err(ResourceError::Substrate {
                    code: "resource_list_terminal_page_incomplete".to_owned(),
                    message: "terminal Resource list page does not close the exact manifest"
                        .to_owned(),
                });
            }
            Some(_) if end >= manifest.entry_count => {
                return Err(ResourceError::Substrate {
                    code: "resource_list_terminal_cursor_present".to_owned(),
                    message: "terminal Resource list frontier returned a successor cursor"
                        .to_owned(),
                });
            }
            Some(next_cursor) => {
                ResourceListCursor::decode(next_cursor)?.verify_successor(
                    publication,
                    cursor,
                    limit,
                    expected_start,
                    &page.entries,
                )?;
            }
            None => {}
        }
        Ok(page)
    }

    /// Consume the client and return its adapter.
    pub fn into_inner(self) -> R {
        self.resolver
    }
}
