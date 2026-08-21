use serde::{Deserialize, Serialize};

use crate::{ResourceError, ResourceHandle, ResourceResult};

/// Frozen content manifest version.
pub const RESOURCE_MANIFEST_VERSION: &str = "cymule.resource-manifest/1";
/// Frozen bounded list proof version.
pub const RESOURCE_LIST_PROOF_VERSION: &str = "cymule.resource-list-proof/2";
/// Canonical JSON-lines media type used for Resource manifests.
pub const RESOURCE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.cymule.resource-manifest+jsonl";

const MANIFEST_LEAF_DOMAIN: &str = "cymule.resource-manifest-leaf/1";
const MANIFEST_NODE_DOMAIN: &str = "cymule.resource-manifest-node/1";
const MANIFEST_EMPTY_DOMAIN: &str = "cymule.resource-manifest-empty/1";
const MANIFEST_CURSOR_DOMAIN: &str = "cymule.resource-manifest-cursor/1";

/// Semantic descriptor for one exact listable Resource manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifestDescriptor {
    /// Manifest wire version.
    pub manifest_version: String,
    /// Canonical manifest media type.
    pub media_type: String,
    /// Digest of the exact canonical JSON-lines bytes.
    pub digest: String,
    /// Exact canonical byte length.
    pub size: u64,
    /// Number of entries represented by this manifest.
    pub entry_count: u64,
    /// Merkle root of all canonical entries in strict name order.
    pub root_digest: String,
}

/// One provider-neutral entry in a content-addressed manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifestEntry {
    /// Safe relative logical name.
    pub name: String,
    /// Exact semantic child Resource descriptor.
    pub resource: ResourceHandle,
}

/// Side of one sibling in a Merkle inclusion path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MerkleSide {
    /// The sibling hashes before the current node.
    Left,
    /// The sibling hashes after the current node.
    Right,
}

/// One sibling step in a manifest inclusion proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MerkleStep {
    /// Position of the sibling relative to the current node.
    pub side: MerkleSide,
    /// Lowercase SHA-256 content identity of the sibling node.
    pub digest: String,
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
    /// Exact canonical manifest byte digest.
    pub manifest_digest: String,
    /// Exact complete manifest entry count.
    pub entry_count: u64,
    /// First entry index represented by this page.
    pub start_index: u64,
    /// Digest of the exact opaque cursor supplied for this page.
    pub request_cursor_digest: String,
    /// Digest of the exact opaque cursor returned for the following page.
    pub next_cursor_digest: String,
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

#[derive(Serialize)]
struct NodeIdentity<'a> {
    left: &'a str,
    right: &'a str,
}

impl ResourceManifestDescriptor {
    /// Verify the closed descriptor shape and content identities.
    pub fn verify(&self) -> ResourceResult<()> {
        if self.manifest_version != RESOURCE_MANIFEST_VERSION {
            return Err(ResourceError::Validation(format!(
                "unsupported Resource manifest version {:?}",
                self.manifest_version
            )));
        }
        if self.media_type != RESOURCE_MANIFEST_MEDIA_TYPE {
            return Err(ResourceError::Validation(format!(
                "Resource manifests require media type {RESOURCE_MANIFEST_MEDIA_TYPE}"
            )));
        }
        validate_digest("manifest", &self.digest)?;
        validate_digest("manifest root", &self.root_digest)?;
        if self.entry_count == 0
            && (self.size != 0
                || self.digest != format!("sha256:{}", cymule_core::sha256_bytes(&[]))
                || self.root_digest != empty_root()?)
        {
            return Err(ResourceError::Validation(
                "empty Resource manifests require the canonical empty bytes and Merkle root"
                    .to_owned(),
            ));
        }
        if self.entry_count > 0 && self.size == 0 {
            return Err(ResourceError::Validation(
                "non-empty Resource manifests require canonical bytes".to_owned(),
            ));
        }
        Ok(())
    }
}

impl SealedResourceManifest {
    /// Canonically seal a strictly sorted manifest and derive its byte and
    /// Merkle identities.
    pub fn seal(entries: Vec<ResourceManifestEntry>) -> ResourceResult<Self> {
        validate_entries(&entries)?;
        let mut bytes = Vec::new();
        for entry in &entries {
            bytes.extend(
                cymule_core::canonical_bytes(entry)
                    .map_err(|error| ResourceError::Validation(error.to_string()))?,
            );
            bytes.push(b'\n');
        }
        let leaves = entries
            .iter()
            .map(leaf_digest)
            .collect::<ResourceResult<Vec<_>>>()?;
        let levels = merkle_levels(leaves)?;
        let root_digest = levels
            .last()
            .and_then(|level| level.first())
            .cloned()
            .unwrap_or(empty_root()?);
        Ok(Self {
            descriptor: ResourceManifestDescriptor {
                manifest_version: RESOURCE_MANIFEST_VERSION.to_owned(),
                media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
                digest: format!("sha256:{}", cymule_core::sha256_bytes(&bytes)),
                size: bytes.len() as u64,
                entry_count: entries.len() as u64,
                root_digest,
            },
            bytes,
            entries,
            levels,
        })
    }

    /// Build the exact inclusion proof for one bounded contiguous page.
    pub fn proof(
        &self,
        start_index: u64,
        count: usize,
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
    ) -> ResourceResult<ResourceListProof> {
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
        let inclusions = (start..end).map(|index| self.inclusion(index)).collect();
        Ok(ResourceListProof {
            proof_version: RESOURCE_LIST_PROOF_VERSION.to_owned(),
            manifest_digest: self.descriptor.digest.clone(),
            entry_count: self.descriptor.entry_count,
            start_index,
            request_cursor_digest: cursor_digest(request_cursor)?,
            next_cursor_digest: cursor_digest(next_cursor)?,
            inclusions,
        })
    }

    /// Borrow the exact sealed entries.
    pub fn entries(&self) -> &[ResourceManifestEntry] {
        &self.entries
    }

    fn inclusion(&self, index: usize) -> ManifestInclusionProof {
        let mut position = index;
        let mut path = Vec::new();
        for level in self.levels.iter().take(self.levels.len().saturating_sub(1)) {
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
            index: index as u64,
            path,
        }
    }
}

impl ResourceListProof {
    /// Verify a bounded page against the exact semantic manifest descriptor.
    pub fn verify_page(
        &self,
        descriptor: &ResourceManifestDescriptor,
        entries: &[ResourceManifestEntry],
        request_cursor: Option<&str>,
        next_cursor: Option<&str>,
    ) -> ResourceResult<()> {
        descriptor.verify()?;
        if self.proof_version != RESOURCE_LIST_PROOF_VERSION
            || self.manifest_digest != descriptor.digest
            || self.entry_count != descriptor.entry_count
            || self.inclusions.len() != entries.len()
            || self.request_cursor_digest != cursor_digest(request_cursor)?
            || self.next_cursor_digest != cursor_digest(next_cursor)?
        {
            return Err(ResourceError::Integrity(
                "Resource list proof does not match its manifest descriptor or page".to_owned(),
            ));
        }
        let end = self
            .start_index
            .checked_add(entries.len() as u64)
            .ok_or_else(|| ResourceError::Integrity("manifest page range overflow".to_owned()))?;
        if end > self.entry_count {
            return Err(ResourceError::Integrity(
                "Resource list proof exceeds the complete manifest".to_owned(),
            ));
        }
        for (offset, (entry, inclusion)) in entries.iter().zip(&self.inclusions).enumerate() {
            let expected_index = self.start_index + offset as u64;
            if inclusion.index != expected_index {
                return Err(ResourceError::Integrity(
                    "Resource list proof contains a non-contiguous entry index".to_owned(),
                ));
            }
            verify_inclusion(descriptor, entry, inclusion)?;
        }
        Ok(())
    }
}

fn validate_entries(entries: &[ResourceManifestEntry]) -> ResourceResult<()> {
    let mut previous: Option<&str> = None;
    for entry in entries {
        validate_name(&entry.name)?;
        entry.resource.verify()?;
        if previous.is_some_and(|name| name >= entry.name.as_str()) {
            return Err(ResourceError::Validation(
                "Resource manifest entries must be strictly name-sorted".to_owned(),
            ));
        }
        previous = Some(&entry.name);
    }
    Ok(())
}

fn validate_name(name: &str) -> ResourceResult<()> {
    if name.is_empty()
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
            parent.push(node_digest(left, right)?);
        }
        current = parent;
    }
    Ok(levels)
}

fn leaf_digest(entry: &ResourceManifestEntry) -> ResourceResult<String> {
    cymule_core::content_id(MANIFEST_LEAF_DOMAIN, entry)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn node_digest(left: &str, right: &str) -> ResourceResult<String> {
    cymule_core::content_id(MANIFEST_NODE_DOMAIN, &NodeIdentity { left, right })
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn empty_root() -> ResourceResult<String> {
    cymule_core::content_id(MANIFEST_EMPTY_DOMAIN, &())
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn verify_inclusion(
    descriptor: &ResourceManifestDescriptor,
    entry: &ResourceManifestEntry,
    proof: &ManifestInclusionProof,
) -> ResourceResult<()> {
    if proof.index >= descriptor.entry_count {
        return Err(ResourceError::Integrity(
            "manifest proof index exceeds the entry count".to_owned(),
        ));
    }
    entry.resource.verify()?;
    validate_name(&entry.name)?;
    let mut digest = leaf_digest(entry)?;
    let mut position = proof.index;
    let mut width = descriptor.entry_count;
    for step in &proof.path {
        if width <= 1 {
            return Err(ResourceError::Integrity(
                "manifest inclusion path is longer than the retained tree".to_owned(),
            ));
        }
        validate_digest("manifest proof node", &step.digest)?;
        let expected_side = if position.is_multiple_of(2) {
            MerkleSide::Right
        } else {
            MerkleSide::Left
        };
        if step.side != expected_side {
            return Err(ResourceError::Integrity(
                "manifest inclusion path does not match its retained entry index".to_owned(),
            ));
        }
        digest = match step.side {
            MerkleSide::Left => node_digest(&step.digest, &digest)?,
            MerkleSide::Right => node_digest(&digest, &step.digest)?,
        };
        position /= 2;
        width = width.div_ceil(2);
    }
    if width != 1 {
        return Err(ResourceError::Integrity(
            "manifest inclusion path is shorter than the retained tree".to_owned(),
        ));
    }
    if digest != descriptor.root_digest {
        return Err(ResourceError::Integrity(
            "manifest inclusion path does not reach the retained root".to_owned(),
        ));
    }
    Ok(())
}

fn cursor_digest(cursor: Option<&str>) -> ResourceResult<String> {
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
