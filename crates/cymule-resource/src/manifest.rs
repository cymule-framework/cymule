use crate::{MAX_LIST_PAGE, ResourceError, ResourceHandle, ResourceResult};
pub use cymule_profile_protocol::resource::{
    MerkleSide, MerkleStep, RESOURCE_MANIFEST_MEDIA_TYPE, RESOURCE_MANIFEST_VERSION,
    ResourceManifestDescriptor, resource_manifest_descriptor_id,
};
use serde::{Deserialize, Deserializer, Serialize};

/// Frozen bounded list proof version.
pub const RESOURCE_LIST_PROOF_VERSION: &str = "cymule.resource-list-proof/5";
/// Maximum canonical bytes for one manifest JSON-line, including its newline.
pub const MAX_MANIFEST_ENTRY_BYTES: usize = 1024 * 1024;
/// Maximum canonical manifest bytes returned in one list page.
pub const MAX_MANIFEST_PAGE_BYTES: u64 = 8 * 1024 * 1024;
/// Maximum sibling steps for a tree within the shared 53-bit exact-integer range.
pub const MAX_MANIFEST_PROOF_DEPTH: usize = 53;

const MANIFEST_LEAF_DOMAIN: &str = "cymule.resource-manifest-leaf/2";
const MANIFEST_NODE_DOMAIN: &str = "cymule.resource-manifest-node/2";
const MANIFEST_CURSOR_DOMAIN: &str = "cymule.resource-manifest-cursor/2";

/// One provider-neutral entry in a content-addressed manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifestEntry {
    /// Safe relative logical name.
    pub name: String,
    /// Exact semantic child Resource descriptor.
    pub resource: ResourceHandle,
}

/// Root-verifiable boundary immediately preceding one non-initial list page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestPredecessorProof {
    /// Exact manifest entry at `start_index - 1`.
    pub entry: ResourceManifestEntry,
    /// Inclusion path proving the predecessor's exact tree position.
    pub inclusion: ManifestInclusionProof,
}

/// Inclusion path for one exact manifest entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestInclusionProof {
    /// Zero-based entry index in the complete manifest.
    pub index: u64,
    /// Sibling path from the entry leaf to the manifest root.
    pub path: Vec<MerkleStep>,
}

/// Cryptographic proof binding one bounded page to an exact manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceListProof {
    /// Proof wire version.
    pub proof_version: String,
    /// Exact semantic manifest descriptor content ID.
    pub manifest_digest: String,
    /// Exact complete manifest entry count.
    pub entry_count: u64,
    /// First entry index represented by this page.
    pub start_index: u64,
    /// Digest of the exact opaque cursor supplied for this page.
    pub request_cursor_digest: String,
    /// Digest of the exact opaque cursor returned for the following page.
    pub next_cursor_digest: String,
    /// Root-verifiable ordering boundary for a non-initial page.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub predecessor: Option<ManifestPredecessorProof>,
    /// One inclusion path for every returned entry, in order.
    pub inclusions: Vec<ManifestInclusionProof>,
}

/// Canonical manifest bytes and the proofs needed for bounded listing.
#[derive(Debug, Clone, PartialEq)]
pub struct SealedResourceManifest {
    /// Semantic content descriptor.
    pub descriptor: ResourceManifestDescriptor,
    /// Canonical JSON-lines bytes stored by an adapter.
    pub bytes: Vec<u8>,
    entries: Vec<ResourceManifestEntry>,
    levels: Vec<Vec<String>>,
}

/// One canonical JSON-line and the leaf identity derived from those exact
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalResourceManifestLine {
    bytes: Vec<u8>,
    leaf_digest: String,
}

impl CanonicalResourceManifestLine {
    /// Borrow the canonical JSON-line including its trailing newline.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Borrow the Merkle leaf derived from the same canonical JSON bytes.
    pub fn leaf_digest(&self) -> &str {
        &self.leaf_digest
    }
}

/// Streaming byte, count, and Merkle authority for one strictly ordered
/// manifest.
///
/// The accumulator retains at most one subtree root per binary level, so
/// publication validates arbitrarily large manifests in `O(log n)` memory.
/// Each entry is canonicalized once; the returned line, size, count, Merkle
/// root, and descriptor content ID all derive from that one byte sequence.
#[derive(Debug, Clone)]
pub struct ResourceManifestAccumulator {
    peaks: Vec<Option<String>>,
    entry_count: u64,
    byte_size: u64,
    previous_name: Option<String>,
}

/// Streaming canonical JSON-lines verifier for one complete manifest copy.
///
/// The verifier retains only one bounded line plus the logarithmic Merkle
/// accumulator. Its finished descriptor is the sole content authority for the
/// exact bytes it consumed.
#[derive(Debug, Clone)]
pub struct ResourceManifestStreamVerifier {
    accumulator: ResourceManifestAccumulator,
    line: Vec<u8>,
}

#[derive(Serialize)]
struct NodeIdentity<'a> {
    left: &'a str,
    right: &'a str,
}

impl SealedResourceManifest {
    /// Canonically seal a strictly sorted manifest and derive its byte and
    /// Merkle identities.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid, unsorted, or oversized entries or failed
    /// canonical identity derivation.
    pub fn seal(entries: Vec<ResourceManifestEntry>) -> ResourceResult<Self> {
        let mut accumulator = ResourceManifestAccumulator::new();
        let mut bytes = Vec::new();
        let mut leaves = Vec::with_capacity(entries.len());
        for entry in &entries {
            let line = accumulator.push(entry)?;
            bytes.extend_from_slice(line.bytes());
            leaves.push(line.leaf_digest().to_owned());
        }
        let levels = merkle_levels(leaves)?;
        let descriptor = accumulator.descriptor()?;
        let indexed_root = levels
            .last()
            .and_then(|level| level.first())
            .cloned()
            .unwrap_or(empty_root()?);
        if indexed_root != descriptor.root_digest {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_proof_index_root_mismatch".to_owned(),
                message: "Resource manifest proof index diverged from its accumulator root"
                    .to_owned(),
            });
        }
        Ok(Self {
            descriptor,
            bytes,
            entries,
            levels,
        })
    }

    /// Build the exact inclusion proof for one bounded contiguous page.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested range or cursor identity is invalid.
    pub fn proof(
        &self,
        start_index: u64,
        count: usize,
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
    ) -> ResourceResult<ResourceListProof> {
        validate_safe_integer("manifest page start", start_index)?;
        let page_count = u64::try_from(count).map_err(|_| {
            ResourceError::Validation("manifest page count exceeds platform bounds".to_owned())
        })?;
        if page_count > u64::from(MAX_LIST_PAGE) {
            return Err(ResourceError::Validation(format!(
                "Resource manifest proof page exceeds {MAX_LIST_PAGE} entries"
            )));
        }
        let start = usize::try_from(start_index).map_err(|_| {
            ResourceError::Validation("manifest page index exceeds platform bounds".to_owned())
        })?;
        let end = start
            .checked_add(count)
            .ok_or_else(|| ResourceError::Validation("manifest page range overflow".to_owned()))?;
        if end > self.entries.len() {
            return Err(ResourceError::Validation(
                "manifest proof range exceeds the entry count".to_owned(),
            ));
        }
        let predecessor = start.checked_sub(1).map(|index| ManifestPredecessorProof {
            entry: self.entries[index].clone(),
            inclusion: self.inclusion(index),
        });
        let inclusions = (start..end).map(|index| self.inclusion(index)).collect();
        let proof = ResourceListProof::from_inclusions(
            &self.descriptor,
            start_index,
            predecessor,
            inclusions,
            request_cursor,
            next_cursor,
        )?;
        proof.verify_page(
            &self.descriptor,
            &self.entries[start..end],
            request_cursor,
            next_cursor,
        )?;
        Ok(proof)
    }

    /// Borrow the exact sealed entries.
    pub fn entries(&self) -> &[ResourceManifestEntry] {
        &self.entries
    }

    /// Borrow the complete Merkle levels for persistence-backed bounded indexes.
    ///
    /// Levels are ordered from leaves through the single root. Adapters persist
    /// these immutable digests once at publication and read only the sibling
    /// nodes required for a requested page.
    pub fn merkle_levels(&self) -> impl ExactSizeIterator<Item = &[String]> {
        self.levels.iter().map(Vec::as_slice)
    }

    fn inclusion(&self, index: usize) -> ManifestInclusionProof {
        let mut position = index;
        let mut path = Vec::new();
        let parent_level_count = self
            .levels
            .len()
            .checked_sub(1)
            .expect("a manifest inclusion always has a non-empty Merkle tree");
        for level in self.levels.iter().take(parent_level_count) {
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
        ManifestInclusionProof {
            index: u64::try_from(index).expect("manifest index was admitted by its descriptor"),
            path,
        }
    }
}

impl Default for ResourceManifestAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceManifestAccumulator {
    /// Construct an empty manifest accumulator.
    pub fn new() -> Self {
        Self {
            peaks: Vec::new(),
            entry_count: 0,
            byte_size: 0,
            previous_name: None,
        }
    }

    /// Add one already validated manifest entry in strict manifest order.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry or its Resource is invalid or a Merkle
    /// identity cannot be derived.
    pub fn push(
        &mut self,
        entry: &ResourceManifestEntry,
    ) -> ResourceResult<CanonicalResourceManifestLine> {
        entry.resource.verify()?;
        validate_manifest_name(&entry.name)?;
        if self
            .previous_name
            .as_deref()
            .is_some_and(|previous| previous >= entry.name.as_str())
        {
            return Err(ResourceError::Validation(
                "Resource manifest entries must be strictly name-sorted".to_owned(),
            ));
        }
        let canonical = canonical_manifest_entry_bytes(entry)?;
        let leaf_digest = manifest_leaf_digest_from_canonical(&canonical);
        let mut line = canonical;
        line.push(b'\n');
        let line_size = u64::try_from(line.len()).map_err(|_| {
            ResourceError::Validation("manifest entry size exceeds platform bounds".to_owned())
        })?;
        self.byte_size = self
            .byte_size
            .checked_add(line_size)
            .filter(|size| *size <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                ResourceError::Validation(
                    "manifest byte size exceeds the shared exact-integer range".to_owned(),
                )
            })?;
        self.previous_name = Some(entry.name.clone());
        self.push_digest(leaf_digest.clone())?;
        Ok(CanonicalResourceManifestLine {
            bytes: line,
            leaf_digest,
        })
    }

    /// Return the number of entries incorporated by this accumulator.
    pub const fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Return the exact canonical byte length incorporated so far.
    pub const fn byte_size(&self) -> u64 {
        self.byte_size
    }

    /// Finish the one descriptor derived from the exact canonical lines
    /// incorporated by this accumulator.
    ///
    /// # Errors
    ///
    /// Returns an error when the Merkle root cannot be derived.
    pub fn descriptor(&self) -> ResourceResult<ResourceManifestDescriptor> {
        let root_digest = self.root_digest()?;
        let digest = manifest_descriptor_digest(
            RESOURCE_MANIFEST_MEDIA_TYPE,
            self.byte_size,
            self.entry_count,
            &root_digest,
        )?;
        Ok(ResourceManifestDescriptor {
            manifest_version: RESOURCE_MANIFEST_VERSION.to_owned(),
            media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
            digest,
            size: self.byte_size,
            entry_count: self.entry_count,
            root_digest,
        })
    }

    /// Finish the exact duplicate-right-edge Merkle root used by manifests.
    ///
    /// # Errors
    ///
    /// Returns an error when a Merkle parent identity cannot be derived.
    pub fn root_digest(&self) -> ResourceResult<String> {
        let mut right: Option<(String, usize)> = None;
        for (level, peak) in self.peaks.iter().enumerate() {
            let Some(peak) = peak else {
                continue;
            };
            right = Some(match right {
                None => (peak.clone(), level),
                Some((mut digest, mut height)) => {
                    while height < level {
                        digest = manifest_node_digest(&digest, &digest)?;
                        height += 1;
                    }
                    (manifest_node_digest(peak, &digest)?, level + 1)
                }
            });
        }
        match right {
            Some((digest, _)) => Ok(digest),
            None => empty_root(),
        }
    }

    fn push_digest(&mut self, mut digest: String) -> ResourceResult<()> {
        self.entry_count = self
            .entry_count
            .checked_add(1)
            .filter(|count| *count <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| ResourceError::Validation("manifest entry count overflow".to_owned()))?;
        let mut level = 0_usize;
        loop {
            if level == self.peaks.len() {
                self.peaks.push(Some(digest));
                return Ok(());
            }
            if let Some(left) = self.peaks[level].take() {
                digest = manifest_node_digest(&left, &digest)?;
                level += 1;
            } else {
                self.peaks[level] = Some(digest);
                return Ok(());
            }
        }
    }
}

impl Default for ResourceManifestStreamVerifier {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceManifestStreamVerifier {
    /// Construct an empty streaming manifest verifier.
    pub fn new() -> Self {
        Self {
            accumulator: ResourceManifestAccumulator::new(),
            line: Vec::new(),
        }
    }

    /// Incorporate the next contiguous manifest byte range.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, oversized, malformed, non-canonical, or
    /// semantically invalid JSON-line.
    pub fn push(&mut self, bytes: &[u8]) -> ResourceResult<()> {
        for byte in bytes {
            if *byte == b'\n' {
                if self.line.is_empty() {
                    return Err(ResourceError::Integrity {
                        code: "resource_manifest_empty_line".to_owned(),
                        message: "Resource manifest contains an empty JSON-line".to_owned(),
                    });
                }
                let entry: ResourceManifestEntry =
                    cymule_core::decode_json(&self.line).map_err(|error| {
                        ResourceError::Integrity {
                            code: "resource_manifest_line_decode_failed".to_owned(),
                            message: error.to_string(),
                        }
                    })?;
                let canonical = self.accumulator.push(&entry)?;
                if canonical.bytes().strip_suffix(b"\n") != Some(self.line.as_slice()) {
                    return Err(ResourceError::Integrity {
                        code: "resource_manifest_line_noncanonical".to_owned(),
                        message: "Resource manifest contains a non-canonical JSON-line".to_owned(),
                    });
                }
                self.line.clear();
            } else {
                self.line.push(*byte);
                if self.line.len() >= MAX_MANIFEST_ENTRY_BYTES {
                    return Err(ResourceError::Integrity {
                        code: "resource_manifest_entry_too_large".to_owned(),
                        message: format!(
                            "Resource manifest entry exceeds {MAX_MANIFEST_ENTRY_BYTES} canonical bytes"
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    /// Finish the stream and return its exact semantic descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error unless the input ends at a canonical JSON-line
    /// boundary and its descriptor can be derived.
    pub fn finish(self) -> ResourceResult<ResourceManifestDescriptor> {
        if !self.line.is_empty() {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_truncated_line".to_owned(),
                message: "Resource manifest does not end at a canonical JSON-line boundary"
                    .to_owned(),
            });
        }
        self.accumulator.descriptor()
    }
}

impl ResourceListProof {
    /// Construct one bounded page proof from retained Merkle inclusions.
    ///
    /// Adapters with a persistence-backed manifest index use this constructor
    /// without materializing the complete manifest or Merkle tree at list time.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor, range, cursor, or inclusion
    /// positions do not form one exact bounded page.
    pub fn from_inclusions(
        descriptor: &ResourceManifestDescriptor,
        start_index: u64,
        predecessor: Option<ManifestPredecessorProof>,
        inclusions: Vec<ManifestInclusionProof>,
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
    ) -> ResourceResult<Self> {
        descriptor.verify()?;
        validate_safe_integer("manifest page start", start_index)?;
        let inclusion_count = u64::try_from(inclusions.len()).map_err(|_| {
            ResourceError::Validation("manifest page count exceeds platform bounds".to_owned())
        })?;
        validate_safe_integer("manifest page count", inclusion_count)?;
        if inclusion_count > u64::from(MAX_LIST_PAGE) {
            return Err(ResourceError::Validation(format!(
                "Resource manifest proof page exceeds {MAX_LIST_PAGE} entries"
            )));
        }
        if proof_paths_exceed_depth(predecessor.as_ref(), &inclusions) {
            return Err(ResourceError::Validation(format!(
                "Resource manifest proof path exceeds {MAX_MANIFEST_PROOF_DEPTH} sibling steps"
            )));
        }
        let end = start_index
            .checked_add(inclusion_count)
            .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| ResourceError::Validation("manifest page range overflow".to_owned()))?;
        let predecessor_matches = match (&predecessor, start_index.checked_sub(1)) {
            (None, None) => true,
            (Some(predecessor), Some(expected_index)) => {
                predecessor.inclusion.index == expected_index
            }
            _ => false,
        };
        if !predecessor_matches
            || end > descriptor.entry_count
            || inclusions.iter().enumerate().any(|(offset, proof)| {
                u64::try_from(offset)
                    .ok()
                    .and_then(|offset| start_index.checked_add(offset))
                    != Some(proof.index)
            })
        {
            return Err(ResourceError::Validation(
                "manifest inclusions are not one bounded contiguous page".to_owned(),
            ));
        }
        Ok(Self {
            proof_version: RESOURCE_LIST_PROOF_VERSION.to_owned(),
            manifest_digest: descriptor.digest.clone(),
            entry_count: descriptor.entry_count,
            start_index,
            request_cursor_digest: cursor_digest(request_cursor)?,
            next_cursor_digest: cursor_digest(next_cursor)?,
            predecessor,
            inclusions,
        })
    }

    /// Verify a bounded page against the exact semantic manifest descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error when descriptor, cursor, entry, range, or Merkle
    /// evidence does not match.
    pub fn verify_page(
        &self,
        descriptor: &ResourceManifestDescriptor,
        entries: &[ResourceManifestEntry],
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
    ) -> ResourceResult<()> {
        descriptor.verify()?;
        validate_safe_integer("manifest proof entry count", self.entry_count)?;
        validate_safe_integer("manifest proof page start", self.start_index)?;
        let page_count = self.checked_page_count(entries.len())?;
        if self.proof_version != RESOURCE_LIST_PROOF_VERSION
            || self.manifest_digest != descriptor.digest
            || self.entry_count != descriptor.entry_count
            || self.inclusions.len() != entries.len()
            || self.request_cursor_digest != cursor_digest(request_cursor)?
            || self.next_cursor_digest != cursor_digest(next_cursor)?
        {
            return Err(ResourceError::Integrity {
                code: "resource_list_proof_binding_mismatch".to_owned(),
                message: "Resource list proof does not match its manifest descriptor or page"
                    .to_owned(),
            });
        }
        validate_safe_integer("manifest proof page count", page_count)?;
        let end = self
            .start_index
            .checked_add(page_count)
            .filter(|value| *value <= cymule_core::MAX_EXACT_INTEGER)
            .ok_or_else(|| ResourceError::Integrity {
                code: "resource_manifest_page_range_overflow".to_owned(),
                message: "manifest page range overflow".to_owned(),
            })?;
        if end > self.entry_count {
            return Err(ResourceError::Integrity {
                code: "resource_list_proof_range_exceeded".to_owned(),
                message: "Resource list proof exceeds the complete manifest".to_owned(),
            });
        }
        if entries.is_empty() {
            if self.entry_count != 0
                || self.start_index != 0
                || request_cursor.is_some()
                || next_cursor.is_some()
                || self.predecessor.is_some()
            {
                return Err(ResourceError::Integrity {
                    code: "resource_list_empty_page_noncanonical".to_owned(),
                    message: "only the canonical empty manifest may return an empty list page"
                        .to_owned(),
                });
            }
            return Ok(());
        }
        let first_entry = entries.first().ok_or_else(|| ResourceError::Integrity {
            code: "resource_list_first_entry_missing".to_owned(),
            message: "non-empty Resource list page lost its first entry".to_owned(),
        })?;
        let last_entry = entries.last().ok_or_else(|| ResourceError::Integrity {
            code: "resource_list_last_entry_missing".to_owned(),
            message: "non-empty Resource list page lost its last entry".to_owned(),
        })?;
        let decoded_request_cursor = request_cursor
            .map(crate::resolver::ResourceListCursor::decode)
            .transpose()
            .map_err(|error| ResourceError::Integrity {
                code: "resource_list_request_cursor_invalid".to_owned(),
                message: format!("Resource list request cursor is not exact: {error}"),
            })?;
        self.verify_predecessor(descriptor, first_entry, decoded_request_cursor.as_ref())?;
        self.verify_successor(
            descriptor,
            last_entry,
            request_cursor,
            next_cursor,
            end,
            decoded_request_cursor.as_ref(),
        )?;
        self.verify_entries(descriptor, entries)?;
        Ok(())
    }

    fn checked_page_count(&self, entry_count: usize) -> ResourceResult<u64> {
        let inclusion_count =
            u64::try_from(self.inclusions.len()).map_err(|_| ResourceError::Integrity {
                code: "resource_manifest_page_count_overflow".to_owned(),
                message: "manifest proof inclusion count exceeds platform bounds".to_owned(),
            })?;
        let page_count = u64::try_from(entry_count).map_err(|_| ResourceError::Integrity {
            code: "resource_manifest_page_count_overflow".to_owned(),
            message: "manifest page count exceeds platform bounds".to_owned(),
        })?;
        if inclusion_count > u64::from(MAX_LIST_PAGE) || page_count > u64::from(MAX_LIST_PAGE) {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_page_count_exceeded".to_owned(),
                message: format!("Resource manifest proof page exceeds {MAX_LIST_PAGE} entries"),
            });
        }
        if proof_paths_exceed_depth(self.predecessor.as_ref(), &self.inclusions) {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_proof_depth_exceeded".to_owned(),
                message: format!(
                    "Resource manifest proof path exceeds {MAX_MANIFEST_PROOF_DEPTH} sibling steps"
                ),
            });
        }
        Ok(page_count)
    }

    fn verify_predecessor(
        &self,
        descriptor: &ResourceManifestDescriptor,
        first_entry: &ResourceManifestEntry,
        request_cursor: Option<&crate::resolver::ResourceListCursor>,
    ) -> ResourceResult<()> {
        match (self.start_index, request_cursor, &self.predecessor) {
            (0, None, None) => {}
            (0, _, _) => {
                return Err(ResourceError::Integrity {
                    code: "resource_list_initial_predecessor_present".to_owned(),
                    message: "the initial Resource list page must not carry predecessor authority"
                        .to_owned(),
                });
            }
            (_, Some(cursor), Some(predecessor)) => {
                let expected_index =
                    self.start_index
                        .checked_sub(1)
                        .ok_or_else(|| ResourceError::Integrity {
                            code: "resource_manifest_predecessor_index_underflow".to_owned(),
                            message: "manifest predecessor index underflow".to_owned(),
                        })?;
                if predecessor.inclusion.index != expected_index {
                    return Err(ResourceError::Integrity {
                        code: "resource_list_predecessor_index_mismatch".to_owned(),
                        message: "Resource list predecessor does not occupy start_index - 1"
                            .to_owned(),
                    });
                }
                verify_inclusion(descriptor, &predecessor.entry, &predecessor.inclusion)?;
                if cursor.manifest_digest != descriptor.digest
                    || cursor.next_index != self.start_index
                    || cursor.last_name != predecessor.entry.name
                {
                    return Err(ResourceError::Integrity {
                        code: "resource_list_cursor_predecessor_mismatch".to_owned(),
                        message:
                            "Resource list request cursor does not bind its Merkle predecessor"
                                .to_owned(),
                    });
                }
                if predecessor.entry.name >= first_entry.name {
                    return Err(ResourceError::Integrity {
                        code: "resource_list_predecessor_order_mismatch".to_owned(),
                        message: "Resource manifest page does not strictly follow its predecessor"
                            .to_owned(),
                    });
                }
            }
            _ => {
                return Err(ResourceError::Integrity {
                    code: "resource_list_predecessor_missing".to_owned(),
                    message:
                        "a non-initial Resource list page requires its cursor and Merkle predecessor"
                            .to_owned(),
                });
            }
        }
        Ok(())
    }

    fn verify_successor(
        &self,
        descriptor: &ResourceManifestDescriptor,
        last_entry: &ResourceManifestEntry,
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
        end: u64,
        decoded_request_cursor: Option<&crate::resolver::ResourceListCursor>,
    ) -> ResourceResult<()> {
        match next_cursor {
            None if end != self.entry_count => {
                return Err(ResourceError::Integrity {
                    code: "resource_list_successor_cursor_missing".to_owned(),
                    message: "non-terminal Resource list page is missing its successor cursor"
                        .to_owned(),
                });
            }
            Some(_) if end >= self.entry_count => {
                return Err(ResourceError::Integrity {
                    code: "resource_list_terminal_cursor_present".to_owned(),
                    message: "terminal Resource list page must not carry a successor cursor"
                        .to_owned(),
                });
            }
            Some(next_cursor) => {
                let cursor =
                    crate::resolver::ResourceListCursor::decode(next_cursor).map_err(|error| {
                        ResourceError::Integrity {
                            code: "resource_list_successor_cursor_invalid".to_owned(),
                            message: format!(
                                "Resource list successor cursor is not exact: {error}"
                            ),
                        }
                    })?;
                let request_limit_matches = decoded_request_cursor
                    .as_ref()
                    .is_none_or(|request| request.request_limit == cursor.request_limit);
                if cursor.manifest_digest != descriptor.digest
                    || cursor.request_cursor_digest != cursor_digest(request_cursor)?
                    || cursor.start_index != self.start_index
                    || cursor.next_index != end
                    || cursor.last_name != last_entry.name
                    || !request_limit_matches
                {
                    return Err(ResourceError::Integrity {
                        code: "resource_list_successor_cursor_mismatch".to_owned(),
                        message:
                            "Resource list successor cursor does not bind the exact ordered page"
                                .to_owned(),
                    });
                }
            }
            None => {}
        }
        Ok(())
    }

    fn verify_entries(
        &self,
        descriptor: &ResourceManifestDescriptor,
        entries: &[ResourceManifestEntry],
    ) -> ResourceResult<()> {
        for pair in entries.windows(2) {
            if pair[0].name >= pair[1].name {
                return Err(ResourceError::Integrity {
                    code: "resource_list_page_order_invalid".to_owned(),
                    message: "Resource manifest page names are not strictly increasing".to_owned(),
                });
            }
        }
        let mut page_bytes = 0_u64;
        for (offset, (entry, inclusion)) in entries.iter().zip(&self.inclusions).enumerate() {
            let entry_bytes = canonical_manifest_entry_bytes(entry)?
                .len()
                .checked_add(1)
                .ok_or_else(|| ResourceError::Integrity {
                    code: "resource_manifest_page_size_overflow".to_owned(),
                    message: "manifest page size overflow".to_owned(),
                })?;
            page_bytes = page_bytes.checked_add(entry_bytes as u64).ok_or_else(|| {
                ResourceError::Integrity {
                    code: "resource_manifest_page_size_overflow".to_owned(),
                    message: "manifest page size overflow".to_owned(),
                }
            })?;
            if page_bytes > MAX_MANIFEST_PAGE_BYTES {
                return Err(ResourceError::Integrity {
                    code: "resource_manifest_page_too_large".to_owned(),
                    message: format!(
                        "Resource list page exceeds {MAX_MANIFEST_PAGE_BYTES} canonical bytes"
                    ),
                });
            }
            let expected_index = self
                .start_index
                .checked_add(u64::try_from(offset).map_err(|_| ResourceError::Integrity {
                    code: "resource_manifest_page_index_platform_overflow".to_owned(),
                    message: "manifest page index exceeds platform bounds".to_owned(),
                })?)
                .ok_or_else(|| ResourceError::Integrity {
                    code: "resource_manifest_page_index_overflow".to_owned(),
                    message: "manifest page index overflow".to_owned(),
                })?;
            if inclusion.index != expected_index {
                return Err(ResourceError::Integrity {
                    code: "resource_list_proof_index_discontinuity".to_owned(),
                    message: "Resource list proof contains a non-contiguous entry index".to_owned(),
                });
            }
            verify_inclusion(descriptor, entry, inclusion)?;
        }
        Ok(())
    }
}

fn proof_paths_exceed_depth(
    predecessor: Option<&ManifestPredecessorProof>,
    inclusions: &[ManifestInclusionProof],
) -> bool {
    predecessor
        .map(|proof| &proof.inclusion)
        .into_iter()
        .chain(inclusions)
        .any(|proof| proof.path.len() > MAX_MANIFEST_PROOF_DEPTH)
}

pub(crate) fn validate_manifest_name(name: &str) -> ResourceResult<()> {
    if name.is_empty()
        || name.chars().count() > 512
        || name.starts_with('/')
        || name.contains('\\')
        || name.chars().any(char::is_control)
        || name
            .split('/')
            .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
    {
        return Err(ResourceError::Validation(
            "Resource manifest entry name is unsafe".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_manifest_page_order(
    entries: &[ResourceManifestEntry],
) -> ResourceResult<()> {
    for entry in entries {
        validate_manifest_name(&entry.name)?;
        entry.resource.verify()?;
    }
    if entries.windows(2).any(|pair| pair[0].name >= pair[1].name) {
        return Err(ResourceError::Validation(
            "Resource manifest page names must be strictly increasing".to_owned(),
        ));
    }
    Ok(())
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn merkle_levels(mut current: Vec<String>) -> ResourceResult<Vec<Vec<String>>> {
    let mut levels = Vec::new();
    while !current.is_empty() {
        levels.push(current.clone());
        if current.len() == 1 {
            break;
        }
        let mut parent = Vec::with_capacity(current.len().div_ceil(2));
        for pair in current.chunks(2) {
            let left = &pair[0];
            let right = pair.get(1).unwrap_or(left);
            parent.push(manifest_node_digest(left, right)?);
        }
        current = parent;
    }
    Ok(levels)
}

/// Compute the exact leaf identity for one canonical manifest entry.
///
/// # Errors
///
/// Returns an error when the entry is not canonically encodable within the
/// entry bound or its identity cannot be derived.
pub fn manifest_leaf_digest(entry: &ResourceManifestEntry) -> ResourceResult<String> {
    Ok(manifest_leaf_digest_from_canonical(
        &canonical_manifest_entry_bytes(entry)?,
    ))
}

fn manifest_leaf_digest_from_canonical(canonical: &[u8]) -> String {
    let mut input = Vec::with_capacity(MANIFEST_LEAF_DOMAIN.len() + canonical.len() + 1);
    input.extend_from_slice(MANIFEST_LEAF_DOMAIN.as_bytes());
    input.push(0);
    input.extend_from_slice(canonical);
    format!("sha256:{}", cymule_core::sha256_bytes(&input))
}

/// Encode one manifest entry as bounded canonical JSON without its newline.
///
/// # Errors
///
/// Returns an error when canonical encoding fails or exceeds the entry bound.
pub fn canonical_manifest_entry_bytes(entry: &ResourceManifestEntry) -> ResourceResult<Vec<u8>> {
    let bytes = cymule_core::canonical_bytes(entry)
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    let line_size = bytes
        .len()
        .checked_add(1)
        .ok_or_else(|| ResourceError::Validation("manifest entry size overflow".to_owned()))?;
    if line_size > MAX_MANIFEST_ENTRY_BYTES {
        return Err(ResourceError::Validation(format!(
            "Resource manifest entry exceeds {MAX_MANIFEST_ENTRY_BYTES} canonical bytes"
        )));
    }
    Ok(bytes)
}

/// Compute one exact manifest Merkle parent identity.
///
/// # Errors
///
/// Returns an error when the parent identity cannot be derived.
pub fn manifest_node_digest(left: &str, right: &str) -> ResourceResult<String> {
    cymule_core::content_id(MANIFEST_NODE_DOMAIN, &NodeIdentity { left, right })
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn empty_root() -> ResourceResult<String> {
    cymule_profile_protocol::resource::resource_manifest_empty_root()
}

fn manifest_descriptor_digest(
    media_type: &str,
    size: u64,
    entry_count: u64,
    root_digest: &str,
) -> ResourceResult<String> {
    cymule_profile_protocol::resource::resource_manifest_descriptor_id(
        media_type,
        size,
        entry_count,
        root_digest,
    )
}

fn verify_inclusion(
    descriptor: &ResourceManifestDescriptor,
    entry: &ResourceManifestEntry,
    proof: &ManifestInclusionProof,
) -> ResourceResult<()> {
    validate_safe_integer("manifest proof index", proof.index)?;
    if proof.index >= descriptor.entry_count {
        return Err(ResourceError::Integrity {
            code: "resource_manifest_proof_index_out_of_range".to_owned(),
            message: "manifest proof index exceeds the entry count".to_owned(),
        });
    }
    entry.resource.verify()?;
    validate_manifest_name(&entry.name)?;
    let mut digest = manifest_leaf_digest(entry)?;
    let mut position = proof.index;
    let mut width = descriptor.entry_count;
    for step in &proof.path {
        if width <= 1 {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_inclusion_path_too_long".to_owned(),
                message: "manifest inclusion path is longer than the retained tree".to_owned(),
            });
        }
        validate_digest("manifest proof node", &step.digest)?;
        let expected_side = if position.is_multiple_of(2) {
            MerkleSide::Right
        } else {
            MerkleSide::Left
        };
        if step.side != expected_side {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_inclusion_path_index_mismatch".to_owned(),
                message: "manifest inclusion path does not match its retained entry index"
                    .to_owned(),
            });
        }
        if !width.is_multiple_of(2) && position == width - 1 && step.digest != digest {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_inclusion_tail_mismatch".to_owned(),
                message: "an odd manifest level must duplicate its exact final node".to_owned(),
            });
        }
        digest = match step.side {
            MerkleSide::Left => manifest_node_digest(&step.digest, &digest)?,
            MerkleSide::Right => manifest_node_digest(&digest, &step.digest)?,
        };
        position /= 2;
        width = width.div_ceil(2);
    }
    if width != 1 {
        return Err(ResourceError::Integrity {
            code: "resource_manifest_inclusion_path_too_short".to_owned(),
            message: "manifest inclusion path is shorter than the retained tree".to_owned(),
        });
    }
    if digest != descriptor.root_digest {
        return Err(ResourceError::Integrity {
            code: "resource_manifest_inclusion_root_mismatch".to_owned(),
            message: "manifest inclusion path does not reach the retained root".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn cursor_digest(cursor: Option<&str>) -> ResourceResult<String> {
    #[derive(Serialize)]
    struct CursorIdentity<'a> {
        cursor: Option<&'a str>,
    }
    cymule_core::content_id(MANIFEST_CURSOR_DOMAIN, &CursorIdentity { cursor })
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_digest(kind: &str, digest: &str) -> ResourceResult<()> {
    let valid = digest.strip_prefix("sha256:").is_some_and(|encoded| {
        encoded.len() == 64
            && encoded
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if !valid {
        return Err(ResourceError::Validation(format!(
            "{kind} digest must be lowercase SHA-256"
        )));
    }
    Ok(())
}

fn validate_safe_integer(kind: &str, value: u64) -> ResourceResult<()> {
    if value > cymule_core::MAX_EXACT_INTEGER {
        return Err(ResourceError::Validation(format!(
            "{kind} exceeds the shared exact-integer range"
        )));
    }
    Ok(())
}
