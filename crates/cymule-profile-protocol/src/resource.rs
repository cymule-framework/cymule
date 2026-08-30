//! Resource descriptor and persistence-wire authority.
//!
//! This module is deliberately below both `cymule-resource` and
//! `cymule-durable`. It owns every serializable Resource value that either a
//! profile command or a durable receipt can contain. Profile controllers add
//! policy and provider I/O; the durable coordinator admits only the closed
//! commands defined here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

/// Frozen semantic Resource descriptor version.
pub const RESOURCE_VERSION: &str = "cymule.resource/4";
/// Frozen non-semantic locator-set version.
pub const RESOURCE_LOCATOR_VERSION: &str = "cymule.resource-locators/2";
/// Frozen content-manifest descriptor version.
pub const RESOURCE_MANIFEST_VERSION: &str = "cymule.resource-manifest/3";
/// Canonical JSON-lines media type used for Resource manifests.
pub const RESOURCE_MANIFEST_MEDIA_TYPE: &str = "application/vnd.cymule.resource-manifest+jsonl";
/// Frozen provider-backed immutable catalog record version.
pub const RESOURCE_CATALOG_RECORD_VERSION: &str = "cymule.resource-catalog-record/2";
/// Frozen typed Artifact contract descriptor version.
pub const ARTIFACT_TYPE_CONTRACT_VERSION: &str = "cymule.artifact-type-contract/1";
/// Media type emitted by the canonical JSON Artifact contract.
pub const CANONICAL_JSON_MEDIA_TYPE: &str = "application/json";
/// Closed JSON Schema dialect used by typed Artifact contracts.
pub const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
/// Logical framework type key for the exact Resource Handle contract.
pub const FRAMEWORK_RESOURCE_HANDLE_TYPE_KEY: &str = "cymule.framework-resource-handle/4";
/// Exact lowercase ASCII media-type grammar shared by Resource schemas.
pub const RESOURCE_MEDIA_TYPE_PATTERN: &str =
    r"^[a-z0-9!#$%&'*+.^_`|~-]+/[a-z0-9!#$%&'*+.^_`|~-]+$";
/// Maximum decoded inline payload size.
pub const INLINE_RESOURCE_LIMIT: usize = 1024 * 1024;
/// Maximum semantic annotation entries on one Resource descriptor.
pub const MAX_RESOURCE_ANNOTATIONS: usize = 64;
/// Maximum realization locations in one replaceable locator set.
pub const MAX_RESOURCE_LOCATIONS: usize = 16;
/// Maximum Unicode scalar count of one canonical public Resource URL.
pub const MAX_RESOURCE_PUBLIC_URL_SCALARS: usize = 8192;
/// Maximum UTF-8 byte count of one canonical public Resource URL.
pub const MAX_RESOURCE_PUBLIC_URL_BYTES: usize = 8192;
/// Maximum canonical JSON size of one Resource candidate or trusted handle.
pub const MAX_RESOURCE_DESCRIPTOR_BYTES: u64 = 4 * 1024 * 1024;
/// Maximum canonical JSON size of one replaceable locator set.
pub const MAX_RESOURCE_LOCATOR_SET_BYTES: u64 = 256 * 1024;
/// Maximum canonical JSON size of one immutable catalog record.
pub const MAX_RESOURCE_CATALOG_RECORD_BYTES: u64 = 16 * 1024 * 1024;

const MANIFEST_EMPTY_DOMAIN: &str = "cymule.resource-manifest-empty/2";

/// Frozen physical retention-key identity generation.
pub const RESOURCE_RETENTION_KEY_VERSION: &str = "cymule.resource-retention-key/1";
/// Frozen normalized physical retention-family version.
pub const RESOURCE_RETENTION_FAMILY_VERSION: &str = "cymule.resource-retention-family/1";
/// Frozen archive-pin identity generation.
pub const RESOURCE_ARCHIVE_PIN_VERSION: &str = "cymule.resource-archive-pin/1";
/// Frozen Agent finalized-stream pin identity generation.
pub const RESOURCE_AGENT_STREAM_PIN_VERSION: &str = "cymule.resource-agent-stream-pin/1";
/// Frozen normalized retention subject version.
pub const RESOURCE_RETENTION_SUBJECT_VERSION: &str = "cymule.resource-retention-subject/1";
/// Frozen provider deletion-target version.
pub const RESOURCE_DELETION_TARGET_VERSION: &str = "cymule.resource-deletion-target/1";
/// Frozen closed Resource command version.
pub const RESOURCE_COMMAND_VERSION: &str = "cymule.resource-command/1";
/// Frozen closed Resource command receipt version.
pub const RESOURCE_COMMAND_RECEIPT_VERSION: &str = "cymule.resource-command-receipt/1";
/// Frozen current retention projection version.
pub const RESOURCE_RETENTION_CURRENT_VERSION: &str = "cymule.resource-retention-current/1";
/// Frozen current pin projection version.
pub const RESOURCE_PIN_CURRENT_VERSION: &str = "cymule.resource-pin-current/2";
/// Frozen cross-profile lifecycle receipt-reference version.
pub const RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION: &str = "cymule.resource-lifecycle-receipt-ref/3";
/// Frozen cross-profile pin delta version.
pub const RESOURCE_PROFILE_PIN_VERSION: &str = "cymule.resource-profile-pin/1";
/// Frozen current deletion projection version.
pub const RESOURCE_DELETE_CURRENT_VERSION: &str = "cymule.resource-delete-current/1";
/// Frozen archive-only release delta consumed by a Virtual retirement CAS.
pub const RESOURCE_ARCHIVE_RELEASE_VERSION: &str = "cymule.resource-archive-release/1";

/// Frozen pin receipt version.
pub const RESOURCE_PIN_RECEIPT_VERSION: &str = "cymule.resource-pin-receipt/3";
/// Frozen release receipt version.
pub const RESOURCE_RELEASE_RECEIPT_VERSION: &str = "cymule.resource-release-receipt/3";
/// Frozen garbage-collection receipt version.
pub const RESOURCE_GC_RECEIPT_VERSION: &str = "cymule.resource-gc-receipt/3";
/// Frozen provider deletion intent version.
pub const RESOURCE_DELETE_INTENT_VERSION: &str = "cymule.resource-delete-intent/3";
/// Frozen provider deletion receipt version.
pub const RESOURCE_DELETE_RECEIPT_VERSION: &str = "cymule.resource-delete-receipt/3";

/// Frozen Run-to-Run handoff authority version.
pub const RESOURCE_HANDOFF_VERSION: &str = "cymule.resource-handoff/5";
/// Frozen handoff-to-input activation authority version.
pub const RESOURCE_HANDOFF_ACTIVATION_VERSION: &str = "cymule.resource-handoff-activation/3";
/// Frozen target index entry for one handoff.
pub const RESOURCE_HANDOFF_INDEX_VERSION: &str = "cymule.resource-handoff-index/1";
/// Frozen target index entry for one activation.
pub const RESOURCE_HANDOFF_ACTIVATION_INDEX_VERSION: &str =
    "cymule.resource-handoff-activation-index/1";
/// Hard maximum number of target-index entries returned by one page.
pub const MAX_HANDOFF_INDEX_PAGE: usize = 256;

/// Stable local JSON Schema issue at a typed Artifact boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceSchemaIssue {
    /// Exact immutable contract that rejected the value.
    pub contract_id: String,
    /// JSON Pointer to the rejected instance location.
    pub instance_path: String,
    /// JSON Pointer to the rejecting schema keyword.
    pub schema_path: String,
}

/// Stable Resource contract errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceError {
    /// A descriptor, request, or response is malformed.
    Validation(String),
    /// A typed Artifact value violates its registered schema contract.
    Schema(ResourceSchemaIssue),
    /// A stable identity was reused with different semantics.
    Conflict {
        /// Stable machine-readable conflict code.
        code: String,
        /// Human-readable conflict summary.
        message: String,
    },
    /// A referenced Resource, Run, or transfer is absent.
    NotFound(String),
    /// A resolver or store adapter failed.
    Substrate {
        /// Stable machine-readable substrate code.
        code: String,
        /// Human-readable substrate summary.
        message: String,
    },
    /// Durable Resource state could not be committed or replayed.
    Persistence {
        /// Stable machine-readable persistence code.
        code: String,
        /// Human-readable persistence summary.
        message: String,
    },
    /// The owning M1 transition may have committed, but its receipt was lost.
    CommitOutcomeUnknown {
        /// Human-readable detail about the uncertain durable commit response.
        message: String,
    },
    /// Retrieved bytes or immutable evidence did not match.
    Integrity {
        /// Stable machine-readable integrity code.
        code: String,
        /// Human-readable integrity summary.
        message: String,
    },
}

/// Result type shared by Resource descriptors and profile controllers.
pub type ResourceResult<T> = std::result::Result<T, ResourceError>;

impl Display for ResourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "validation_failed: {message}"),
            Self::Schema(issue) => write!(
                formatter,
                "schema_failed: contract={} instance={} schema={}",
                issue.contract_id, issue.instance_path, issue.schema_path
            ),
            Self::Conflict { code, message } => {
                write!(formatter, "conflict: {code}: {message}")
            }
            Self::NotFound(message) => write!(formatter, "not_found: {message}"),
            Self::Substrate { code, message } => {
                write!(formatter, "substrate_failed: {code}: {message}")
            }
            Self::Persistence { code, message } => {
                write!(formatter, "persistence_failed: {code}: {message}")
            }
            Self::CommitOutcomeUnknown { message } => {
                write!(formatter, "commit_outcome_unknown: {message}")
            }
            Self::Integrity { code, message } => {
                write!(formatter, "integrity_failed: {code}: {message}")
            }
        }
    }
}

impl std::error::Error for ResourceError {}

/// Logical Resource shape independent of a storage provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceShape {
    /// Small value retained directly in the descriptor.
    Inline,
    /// One opaque byte object or file.
    Object,
    /// An unordered or application-defined group of Resources.
    Collection,
    /// A hierarchical directory manifest.
    Directory,
    /// A sandbox, workspace, volume, or environment snapshot.
    Snapshot,
}

/// Inline payload encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "encoding", rename_all = "snake_case", deny_unknown_fields)]
pub enum InlineData {
    /// UTF-8 text.
    Utf8 {
        /// Retained UTF-8 value.
        text: String,
    },
    /// Structured JSON value.
    Json {
        /// Retained structured value.
        value: Value,
    },
    /// Base64-encoded arbitrary bytes.
    Base64 {
        /// Canonical padded RFC 4648 standard-alphabet base64 value.
        data: String,
    },
}

impl InlineData {
    /// Decode the retained payload into exact bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for noncanonical base64, invalid canonical JSON, or a
    /// payload beyond the inline byte limit.
    pub fn bytes(&self) -> ResourceResult<Vec<u8>> {
        let bytes = match self {
            Self::Utf8 { text } => text.as_bytes().to_vec(),
            Self::Json { value } => cymule_core::canonical_bytes(value)
                .map_err(|error| ResourceError::Validation(error.to_string()))?,
            Self::Base64 { data } => {
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|error| ResourceError::Validation(error.to_string()))?;
                if base64::engine::general_purpose::STANDARD.encode(&bytes) != *data {
                    return Err(ResourceError::Validation(
                        "inline bytes require canonical padded RFC 4648 base64".to_owned(),
                    ));
                }
                bytes
            }
        };
        if bytes.len() > INLINE_RESOURCE_LIMIT {
            return Err(ResourceError::Validation(format!(
                "inline Resource exceeds {INLINE_RESOURCE_LIMIT} bytes"
            )));
        }
        Ok(bytes)
    }
}

/// Evidence available for replaying one Resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceIntegrity {
    /// The complete value is retained directly in the descriptor.
    Inline,
    /// Retrieved content must match this identity and size.
    Content {
        /// Lowercase SHA-256 content identity.
        digest: String,
        /// Expected byte length.
        size: u64,
    },
    /// A resolver-specific immutable version is pinned.
    Version {
        /// Stable resolver/version namespace, not a credential.
        authority: String,
        /// Immutable version within that authority.
        version: String,
    },
    /// The Resource is intentionally mutable and live-only.
    Live {
        /// Stable logical identity for this live Resource.
        identity: String,
    },
}

impl ResourceIntegrity {
    /// Borrow the exact content digest when this is content evidence.
    pub fn content_digest(&self) -> Option<&str> {
        match self {
            Self::Content { digest, .. } => Some(digest),
            _ => None,
        }
    }

    /// Read the exact content size when this is content evidence.
    pub const fn content_size(&self) -> Option<u64> {
        match self {
            Self::Content { size, .. } => Some(*size),
            _ => None,
        }
    }
}

/// Semantic descriptor for one exact listable Resource manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceManifestDescriptor {
    /// Manifest wire version.
    pub manifest_version: String,
    /// Canonical manifest media type.
    pub media_type: String,
    /// Content ID of the root, canonical byte size, entry count, and media type.
    pub digest: String,
    /// Exact canonical byte length.
    pub size: u64,
    /// Number of entries represented by this manifest.
    pub entry_count: u64,
    /// Merkle root of all canonical entries in strict name order.
    pub root_digest: String,
}

/// Side of one sibling in a Resource Merkle inclusion path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MerkleSide {
    /// The sibling hashes before the current node.
    Left,
    /// The sibling hashes after the current node.
    Right,
}

/// One sibling step in a Resource Merkle inclusion proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MerkleStep {
    /// Position of the sibling relative to the current node.
    pub side: MerkleSide,
    /// Lowercase SHA-256 content identity of the sibling node.
    pub digest: String,
}

#[derive(Serialize)]
struct ManifestIdentity<'a> {
    media_type: &'a str,
    size: u64,
    entry_count: u64,
    root_digest: &'a str,
}

impl ResourceManifestDescriptor {
    /// Verify the closed descriptor shape and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any field, bound, empty shape, or derived identity
    /// violates the manifest contract.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource manifest",
            &self.manifest_version,
            RESOURCE_MANIFEST_VERSION,
        )?;
        if self.media_type != RESOURCE_MANIFEST_MEDIA_TYPE {
            return Err(ResourceError::Validation(format!(
                "Resource manifests require media type {RESOURCE_MANIFEST_MEDIA_TYPE}"
            )));
        }
        validate_digest("manifest", &self.digest)?;
        validate_digest("manifest root", &self.root_digest)?;
        validate_safe_integer("manifest byte size", self.size)?;
        validate_safe_integer("manifest entry count", self.entry_count)?;
        let expected = resource_manifest_descriptor_id(
            &self.media_type,
            self.size,
            self.entry_count,
            &self.root_digest,
        )?;
        if self.digest != expected {
            return Err(ResourceError::Integrity {
                code: "resource_manifest_descriptor_identity_mismatch".to_owned(),
                message: format!(
                    "Resource manifest descriptor {} does not match {expected}",
                    self.digest
                ),
            });
        }
        if self.entry_count == 0
            && (self.size != 0 || self.root_digest != resource_manifest_empty_root()?)
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

/// Derive the exact semantic identity of a manifest descriptor.
///
/// # Errors
///
/// Returns an error when canonical identity derivation fails.
pub fn resource_manifest_descriptor_id(
    media_type: &str,
    size: u64,
    entry_count: u64,
    root_digest: &str,
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_MANIFEST_VERSION,
        &ManifestIdentity {
            media_type,
            size,
            entry_count,
            root_digest,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Derive the unique empty-manifest Merkle root.
///
/// # Errors
///
/// Returns an error when canonical identity derivation fails.
pub fn resource_manifest_empty_root() -> ResourceResult<String> {
    cymule_core::content_id(MANIFEST_EMPTY_DOMAIN, &())
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Non-authoritative realization hint interpreted by one resolver binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceLocation {
    /// Credential-free public HTTP(S) URL.
    PublicUrl {
        /// Public URL without userinfo, query, or fragment.
        url: String,
    },
    /// Opaque reference interpreted only by a pinned resolver binding.
    Opaque {
        /// Opaque non-secret reference meaningful to that resolver.
        reference: String,
    },
}

/// Replaceable, non-semantic locations for one exact Resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLocatorSet {
    /// Locator wire version.
    pub locator_version: String,
    /// Exact semantic Resource identity located by this set.
    pub resource_id: String,
    /// Immutable resolver implementation binding.
    pub resolver_binding: String,
    /// Credential-free public or opaque resolver references.
    pub locations: Vec<ResourceLocation>,
}

/// One verified Resource publication plus its replaceable realization record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePublication {
    /// Immutable semantic descriptor.
    pub resource: ResourceHandle,
    /// Replaceable resolver locations.
    pub locators: ResourceLocatorSet,
}

/// Candidate Resource descriptor before trusted identity sealing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCandidate {
    /// Resource wire version.
    pub resource_version: String,
    /// Logical shape.
    pub shape: ResourceShape,
    /// IANA-style media type or stable application media type.
    pub media_type: String,
    /// Retained value for an inline Resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<InlineData>,
    /// Replay/integrity evidence.
    pub integrity: ResourceIntegrity,
    /// Exact content manifest for a listable shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ResourceManifestDescriptor>,
    /// Semantic metadata included in Resource identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Trusted Resource descriptor with a location-independent identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandle {
    /// Content/semantic identity computed by the Resource sealer.
    pub resource_id: String,
    /// Resource wire version.
    pub resource_version: String,
    /// Logical shape.
    pub shape: ResourceShape,
    /// IANA-style media type or stable application media type.
    pub media_type: String,
    /// Retained value for an inline Resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<InlineData>,
    /// Replay/integrity evidence.
    pub integrity: ResourceIntegrity,
    /// Exact content manifest for a listable shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ResourceManifestDescriptor>,
    /// Semantic metadata included in Resource identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

impl Eq for ResourceHandle {}

#[derive(Serialize)]
struct ResourceIdentity<'a> {
    resource_version: &'a str,
    shape: ResourceShape,
    media_type: &'a str,
    inline: Option<&'a InlineData>,
    integrity: &'a ResourceIntegrity,
    manifest: Option<&'a ResourceManifestDescriptor>,
    annotations: &'a BTreeMap<String, String>,
}

/// Replay availability implied by retained Resource evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceReplayClass {
    /// Inline bytes are retained or fetched content is exactly verifiable.
    ContentVerified,
    /// Exact retrieval requires the original immutable-version resolver.
    ResolverRequired,
    /// Only a mutable live reference exists.
    LiveOnly,
}

impl ResourceCandidate {
    /// Construct inline UTF-8 text.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Inline,
            media_type: "text/plain".to_owned(),
            inline: Some(InlineData::Utf8 { text: text.into() }),
            integrity: ResourceIntegrity::Inline,
            manifest: None,
            annotations: BTreeMap::new(),
        }
    }

    /// Construct one inline JSON value.
    pub fn json(value: Value) -> Self {
        Self {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Inline,
            media_type: "application/json".to_owned(),
            inline: Some(InlineData::Json { value }),
            integrity: ResourceIntegrity::Inline,
            manifest: None,
            annotations: BTreeMap::new(),
        }
    }

    /// Validate and compute the location-independent Resource ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate violates its closed shape, field,
    /// count, byte, or identity contract.
    pub fn seal(self) -> ResourceResult<ResourceHandle> {
        self.validate()?;
        let resource_id = resource_id(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            self.manifest.as_ref(),
            &self.annotations,
        )?;
        let handle = ResourceHandle {
            resource_id,
            resource_version: self.resource_version,
            shape: self.shape,
            media_type: self.media_type,
            inline: self.inline,
            integrity: self.integrity,
            manifest: self.manifest,
            annotations: self.annotations,
        };
        validate_canonical_size(
            "Resource descriptor",
            &handle,
            MAX_RESOURCE_DESCRIPTOR_BYTES,
        )?;
        Ok(handle)
    }

    /// Validate a candidate without computing its identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any candidate field or canonical-size bound is
    /// invalid.
    pub fn validate(&self) -> ResourceResult<()> {
        validate_resource_fields(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            self.manifest.as_ref(),
            &self.annotations,
        )?;
        validate_canonical_size("Resource descriptor", self, MAX_RESOURCE_DESCRIPTOR_BYTES)
    }
}

impl ResourceHandle {
    /// Verify every field and recompute the trusted Resource ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor is invalid or its Resource ID does
    /// not match the complete semantic body.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_resource_fields(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            self.manifest.as_ref(),
            &self.annotations,
        )?;
        validate_canonical_size("Resource descriptor", self, MAX_RESOURCE_DESCRIPTOR_BYTES)?;
        let expected = resource_id(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            self.manifest.as_ref(),
            &self.annotations,
        )?;
        if expected != self.resource_id {
            return Err(ResourceError::Integrity {
                code: "resource_identity_mismatch".to_owned(),
                message: format!("Resource ID {} does not match {expected}", self.resource_id),
            });
        }
        Ok(())
    }

    /// Classify retained replay evidence without performing I/O.
    pub const fn replay_class(&self) -> ResourceReplayClass {
        match self.integrity {
            ResourceIntegrity::Inline | ResourceIntegrity::Content { .. } => {
                ResourceReplayClass::ContentVerified
            }
            ResourceIntegrity::Version { .. } => ResourceReplayClass::ResolverRequired,
            ResourceIntegrity::Live { .. } => ResourceReplayClass::LiveOnly,
        }
    }
}

#[derive(Serialize)]
struct ResourceHandleArtifactContractIdentity<'a> {
    contract_version: &'a str,
    artifact_kind: &'a str,
    media_type: &'a str,
    schema_digest: &'a str,
    schema: &'a Value,
}

/// Return the sole frozen JSON Schema for framework Resource Handle Artifacts.
///
/// The Resource contract registry consumes this same value; Durable uses its
/// content-derived typed kind directly. No caller may supply an alternate
/// schema or logical type key at handoff admission.
pub fn resource_handle_artifact_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "required": ["resource_id", "resource_version", "shape", "media_type", "integrity"],
        "properties": {
            "resource_id": resource_artifact_digest_schema(),
            "resource_version": {"const": RESOURCE_VERSION},
            "shape": {"enum": ["inline", "object", "collection", "directory", "snapshot"]},
            "media_type": {
                "type": "string",
                "minLength": 3,
                "maxLength": 255,
                "pattern": RESOURCE_MEDIA_TYPE_PATTERN
            },
            "inline": resource_artifact_inline_schema(),
            "integrity": resource_artifact_integrity_schema(),
            "manifest": resource_artifact_manifest_schema(),
            "annotations": {
                "type": "object",
                "maxProperties": MAX_RESOURCE_ANNOTATIONS,
                "propertyNames": {"type": "string", "minLength": 1, "maxLength": 2048, "pattern": r"^[^\u0000-\u001f\u007f-\u009f]+$"},
                "additionalProperties": {"type": "string", "maxLength": 4096, "pattern": r"^[^\u0000-\u001f\u007f-\u009f]*$"}
            }
        },
        "allOf": [{
            "if": {"properties": {"shape": {"const": "inline"}}, "required": ["shape"]},
            "then": {
                "required": ["inline"],
                "not": {"required": ["manifest"]},
                "properties": {"integrity": {"type": "object", "properties": {"kind": {"const": "inline"}}, "required": ["kind"]}}
            },
            "else": {
                "not": {"required": ["inline"]},
                "properties": {"integrity": {"not": {"type": "object", "properties": {"kind": {"const": "inline"}}, "required": ["kind"]}}}
            }
        }]
    })
}

/// Derive the immutable contract identity for framework Resource Handles.
///
/// # Errors
///
/// Returns an error when the frozen schema cannot be canonically identified.
pub fn resource_handle_artifact_contract_id() -> ResourceResult<String> {
    let schema = resource_handle_artifact_schema();
    let schema_digest = cymule_core::canonical_digest(&schema)
        .map(|digest| format!("sha256:{digest}"))
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    cymule_core::content_id(
        ARTIFACT_TYPE_CONTRACT_VERSION,
        &ResourceHandleArtifactContractIdentity {
            contract_version: ARTIFACT_TYPE_CONTRACT_VERSION,
            artifact_kind: FRAMEWORK_RESOURCE_HANDLE_TYPE_KEY,
            media_type: CANONICAL_JSON_MEDIA_TYPE,
            schema_digest: &schema_digest,
            schema: &schema,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Derive the exact persisted Artifact kind for framework Resource Handles.
///
/// # Errors
///
/// Returns an error when the frozen contract identity is invalid.
pub fn resource_handle_artifact_kind() -> ResourceResult<String> {
    let contract_id = resource_handle_artifact_contract_id()?;
    let digest = contract_id
        .strip_prefix("sha256:")
        .ok_or_else(|| ResourceError::Integrity {
            code: "resource_handle_contract_id_malformed".to_owned(),
            message: "Resource Handle contract ID is malformed".to_owned(),
        })?;
    Ok(format!("cymule.typed-json/sha256-{digest}"))
}

/// Decode one exact canonical Resource Handle Artifact without a registry.
///
/// This is the closed Durable handoff gate. It checks Artifact identity, the
/// frozen typed kind, strict canonical JSON, and the complete Resource identity
/// before returning a semantic Handle.
///
/// # Errors
///
/// Returns an error when the Artifact reference, kind, bytes, JSON wire, or
/// embedded Resource identity is invalid.
pub fn decode_resource_handle_artifact(
    artifact: &cymule_core::ArtifactRecord,
) -> ResourceResult<ResourceHandle> {
    artifact
        .reference
        .validate()
        .map_err(|error| ResourceError::Integrity {
            code: "resource_handle_artifact_reference_invalid".to_owned(),
            message: error.to_string(),
        })?;
    let expected_kind = resource_handle_artifact_kind()?;
    if artifact.reference.kind != expected_kind {
        return Err(ResourceError::Validation(
            "Artifact is not the frozen framework Resource Handle type".to_owned(),
        ));
    }
    let expected = cymule_core::artifact_ref(&expected_kind, &artifact.bytes).map_err(|error| {
        ResourceError::Integrity {
            code: "resource_handle_artifact_identity_invalid".to_owned(),
            message: error.to_string(),
        }
    })?;
    if artifact.reference != expected {
        return Err(ResourceError::Integrity {
            code: "resource_handle_artifact_bytes_mismatch".to_owned(),
            message: "Resource Handle Artifact identity does not match its exact bytes".to_owned(),
        });
    }
    let value: Value = cymule_core::decode_json(&artifact.bytes)
        .map_err(|_| ResourceError::Validation("Artifact is not strict JSON".to_owned()))?;
    let canonical = cymule_core::canonical_bytes(&value)
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    if canonical != artifact.bytes {
        return Err(ResourceError::Integrity {
            code: "resource_handle_artifact_noncanonical".to_owned(),
            message: "Resource Handle Artifact bytes are not canonical".to_owned(),
        });
    }
    let handle: ResourceHandle = serde_json::from_value(value).map_err(|_| {
        ResourceError::Validation("Resource Handle Artifact wire shape is invalid".to_owned())
    })?;
    handle.verify()?;
    Ok(handle)
}

fn resource_artifact_manifest_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["manifest_version", "media_type", "digest", "size", "entry_count", "root_digest"],
        "properties": {
            "manifest_version": {"const": RESOURCE_MANIFEST_VERSION},
            "media_type": {"const": RESOURCE_MANIFEST_MEDIA_TYPE},
            "digest": resource_artifact_digest_schema(),
            "size": resource_artifact_safe_integer_schema(),
            "entry_count": resource_artifact_safe_integer_schema(),
            "root_digest": resource_artifact_digest_schema()
        }
    })
}

fn resource_artifact_inline_schema() -> Value {
    serde_json::json!({"oneOf": [
        {"type": "object", "additionalProperties": false, "required": ["encoding", "text"], "properties": {"encoding": {"const": "utf8"}, "text": {"type": "string"}}},
        {"type": "object", "additionalProperties": false, "required": ["encoding", "value"], "properties": {"encoding": {"const": "json"}, "value": true}},
        {"type": "object", "additionalProperties": false, "required": ["encoding", "data"], "properties": {"encoding": {"const": "base64"}, "data": {"type": "string"}}}
    ]})
}

fn resource_artifact_integrity_schema() -> Value {
    serde_json::json!({"oneOf": [
        {"type": "object", "additionalProperties": false, "required": ["kind"], "properties": {"kind": {"const": "inline"}}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "digest", "size"], "properties": {"kind": {"const": "content"}, "digest": resource_artifact_digest_schema(), "size": resource_artifact_safe_integer_schema()}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "authority", "version"], "properties": {"kind": {"const": "version"}, "authority": resource_artifact_non_empty_schema(), "version": resource_artifact_non_empty_schema()}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "identity"], "properties": {"kind": {"const": "live"}, "identity": resource_artifact_non_empty_schema()}}
    ]})
}

fn resource_artifact_digest_schema() -> Value {
    serde_json::json!({"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"})
}

fn resource_artifact_non_empty_schema() -> Value {
    serde_json::json!({"type": "string", "minLength": 1, "maxLength": 512, "pattern": r"^[^\u0000-\u001f\u007f-\u009f]+$"})
}

fn resource_artifact_safe_integer_schema() -> Value {
    serde_json::json!({
        "type": "integer",
        "minimum": 0,
        "maximum": cymule_core::MAX_EXACT_INTEGER
    })
}

impl ResourceLocatorSet {
    /// Validate this realization record against an exact semantic descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor or locator set violates its
    /// version, target, count, size, URL, or uniqueness contract.
    pub fn verify_for(&self, resource: &ResourceHandle) -> ResourceResult<()> {
        resource.verify()?;
        require_version(
            "Resource locator",
            &self.locator_version,
            RESOURCE_LOCATOR_VERSION,
        )?;
        if self.resource_id != resource.resource_id {
            return Err(ResourceError::Integrity {
                code: "resource_locator_target_mismatch".to_owned(),
                message: "Resource locator set targets a different semantic descriptor".to_owned(),
            });
        }
        validate_identity("resolver binding", &self.resolver_binding)?;
        if self.locations.len() > MAX_RESOURCE_LOCATIONS {
            return Err(ResourceError::Validation(format!(
                "Resource locator set exceeds {MAX_RESOURCE_LOCATIONS} locations"
            )));
        }
        if resource.shape != ResourceShape::Inline && self.locations.is_empty() {
            return Err(ResourceError::Validation(
                "external Resource publication requires at least one location".to_owned(),
            ));
        }
        if resource.shape == ResourceShape::Inline && !self.locations.is_empty() {
            return Err(ResourceError::Validation(
                "inline Resource publication cannot have external locations".to_owned(),
            ));
        }
        validate_canonical_size("Resource locator set", self, MAX_RESOURCE_LOCATOR_SET_BYTES)?;
        let mut seen = BTreeSet::new();
        for location in &self.locations {
            validate_location(location)?;
            let identity = cymule_core::canonical_digest(location)
                .map_err(|error| ResourceError::Validation(error.to_string()))?;
            if !seen.insert(identity) {
                return Err(ResourceError::Validation(
                    "Resource locator set repeats an identical location".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl ResourcePublication {
    /// Verify semantic identity and its independently replaceable locator set.
    ///
    /// # Errors
    ///
    /// Returns an error when either the Resource or locator set is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        self.locators.verify_for(&self.resource)
    }
}

/// One immutable provider-side metadata record addressed by namespace and key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCatalogRecord {
    /// Catalog wire version.
    pub record_version: String,
    /// Stable catalog namespace owned by the consuming profile.
    pub namespace: String,
    /// Stable logical key within the namespace.
    pub key: String,
    /// Content identity of namespace, key, and exact payload bytes.
    pub record_id: String,
    /// Exact canonical payload bytes interpreted by the owning profile.
    pub payload: Vec<u8>,
}

impl ResourceCatalogRecord {
    /// Seal one immutable catalog payload.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid keys, an oversized canonical record, or
    /// failed identity derivation.
    pub fn new(
        namespace: impl Into<String>,
        key: impl Into<String>,
        payload: Vec<u8>,
    ) -> ResourceResult<Self> {
        let namespace = namespace.into();
        let key = key.into();
        validate_extended_identity("catalog namespace", &namespace)?;
        validate_extended_identity("catalog key", &key)?;
        let candidate = Self {
            record_version: RESOURCE_CATALOG_RECORD_VERSION.to_owned(),
            namespace,
            key,
            record_id: format!("sha256:{}", "0".repeat(64)),
            payload,
        };
        validate_canonical_size(
            "Resource catalog record",
            &candidate,
            MAX_RESOURCE_CATALOG_RECORD_BYTES,
        )?;
        let record_id = cymule_core::content_id(
            RESOURCE_CATALOG_RECORD_VERSION,
            &(
                candidate.namespace.as_str(),
                candidate.key.as_str(),
                candidate.payload.as_slice(),
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        Ok(Self {
            record_id,
            ..candidate
        })
    }

    /// Verify the immutable record identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, keys, canonical bound, or content ID
    /// is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource catalog record",
            &self.record_version,
            RESOURCE_CATALOG_RECORD_VERSION,
        )?;
        validate_extended_identity("catalog namespace", &self.namespace)?;
        validate_extended_identity("catalog key", &self.key)?;
        validate_canonical_size(
            "Resource catalog record",
            self,
            MAX_RESOURCE_CATALOG_RECORD_BYTES,
        )?;
        let expected = cymule_core::content_id(
            RESOURCE_CATALOG_RECORD_VERSION,
            &(
                self.namespace.as_str(),
                self.key.as_str(),
                self.payload.as_slice(),
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if self.record_id != expected {
            return Err(ResourceError::Integrity {
                code: "resource_catalog_record_identity_mismatch".to_owned(),
                message: format!(
                    "Resource catalog record {} does not match {expected}",
                    self.record_id
                ),
            });
        }
        Ok(())
    }
}

/// Exact physical content family shared across semantic Resource descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRetentionFamily {
    /// Retention-family wire version.
    pub family_version: String,
    /// Content identity of the store binding and exact content digest.
    pub retention_key: String,
    /// Immutable provider implementation binding.
    pub store_binding: String,
    /// Exact lowercase SHA-256 content digest.
    pub content_digest: String,
}

/// One semantic Resource's auditable membership in a physical content family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRetentionSubject {
    /// Retention subject wire version.
    pub subject_version: String,
    /// Exact semantic Resource identity represented by the caller.
    pub resource_id: String,
    /// Exact physical content family shared by every descriptor for these bytes.
    pub family: ResourceRetentionFamily,
}

#[derive(Serialize)]
struct ResourceRetentionIdentity<'a> {
    store_binding: &'a str,
    content_digest: &'a str,
}

impl ResourceRetentionFamily {
    /// Normalize one verified publication into its physical retention family.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication lacks exact content evidence or
    /// the retention identity cannot be derived.
    pub fn from_publication(publication: &ResourcePublication) -> ResourceResult<Self> {
        publication.verify()?;
        let content_digest = publication
            .resource
            .integrity
            .content_digest()
            .ok_or_else(|| {
                ResourceError::Validation(
                    "Resource retention requires content-addressed integrity".to_owned(),
                )
            })?
            .to_owned();
        let retention_key =
            resource_retention_key_for(&publication.locators.resolver_binding, &content_digest)?;
        let family = Self {
            family_version: RESOURCE_RETENTION_FAMILY_VERSION.to_owned(),
            retention_key,
            store_binding: publication.locators.resolver_binding.clone(),
            content_digest,
        };
        family.verify()?;
        Ok(family)
    }

    /// Verify the normalized physical identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, binding, digest, or retention key is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource retention family",
            &self.family_version,
            RESOURCE_RETENTION_FAMILY_VERSION,
        )?;
        validate_content_id("Resource retention key", &self.retention_key)?;
        validate_identity("Resource store binding", &self.store_binding)?;
        validate_digest("Resource content", &self.content_digest)?;
        let expected = resource_retention_key_for(&self.store_binding, &self.content_digest)?;
        if self.retention_key != expected {
            return Err(ResourceError::Integrity {
                code: "resource_retention_key_mismatch".to_owned(),
                message: format!(
                    "Resource retention key {} does not match {expected}",
                    self.retention_key
                ),
            });
        }
        Ok(())
    }
}

impl ResourceRetentionSubject {
    /// Normalize an immutable semantic handle and resolver binding before its
    /// first provider publication.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle lacks exact content evidence or the
    /// resolver binding cannot identify one physical retention family.
    pub fn from_handle(resolver_binding: &str, resource: &ResourceHandle) -> ResourceResult<Self> {
        resource.verify()?;
        validate_identity("Resource store binding", resolver_binding)?;
        let content_digest = resource
            .integrity
            .content_digest()
            .ok_or_else(|| {
                ResourceError::Validation(
                    "Resource retention requires content-addressed integrity".to_owned(),
                )
            })?
            .to_owned();
        let family = ResourceRetentionFamily {
            family_version: RESOURCE_RETENTION_FAMILY_VERSION.to_owned(),
            retention_key: resource_retention_key_for(resolver_binding, &content_digest)?,
            store_binding: resolver_binding.to_owned(),
            content_digest,
        };
        let subject = Self {
            subject_version: RESOURCE_RETENTION_SUBJECT_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            family,
        };
        subject.verify()?;
        Ok(subject)
    }

    /// Normalize one verified publication into its semantic retention subject.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication or derived retention family is
    /// invalid.
    pub fn from_publication(publication: &ResourcePublication) -> ResourceResult<Self> {
        publication.verify()?;
        let subject = Self {
            subject_version: RESOURCE_RETENTION_SUBJECT_VERSION.to_owned(),
            resource_id: publication.resource.resource_id.clone(),
            family: ResourceRetentionFamily::from_publication(publication)?,
        };
        subject.verify()?;
        Ok(subject)
    }

    /// Verify the semantic identity and its exact physical family.
    ///
    /// # Errors
    ///
    /// Returns an error when the subject version, Resource ID, or family is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource retention subject",
            &self.subject_version,
            RESOURCE_RETENTION_SUBJECT_VERSION,
        )?;
        validate_content_id("Resource", &self.resource_id)?;
        self.family.verify()
    }
}

/// Derive the exact physical retention identity for a store binding and bytes.
///
/// # Errors
///
/// Returns an error for an invalid binding or digest, or failed canonical
/// identity derivation.
pub fn resource_retention_key_for(
    store_binding: &str,
    content_digest: &str,
) -> ResourceResult<String> {
    validate_identity("Resource store binding", store_binding)?;
    validate_digest("Resource content", content_digest)?;
    cymule_core::content_id(
        RESOURCE_RETENTION_KEY_VERSION,
        &ResourceRetentionIdentity {
            store_binding,
            content_digest,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Provider-neutral, minimal target admitted for exact physical deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeletionTarget {
    /// Deletion-target wire version.
    pub target_version: String,
    /// Exact physical family fenced by lifecycle state.
    pub subject: ResourceRetentionSubject,
    /// Exact content byte size proved by the publication.
    pub content_size: u64,
    /// Exact manifest descriptor when deleting a listable Resource.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub manifest: Option<ResourceManifestDescriptor>,
}

impl ResourceDeletionTarget {
    /// Normalize one verified publication into the only provider deletion data.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication lacks exact content evidence or
    /// the deletion target cannot be verified.
    pub fn from_publication(publication: &ResourcePublication) -> ResourceResult<Self> {
        publication.verify()?;
        let content_size = publication
            .resource
            .integrity
            .content_size()
            .ok_or_else(|| {
                ResourceError::Validation(
                    "Resource deletion requires content-addressed integrity".to_owned(),
                )
            })?;
        let target = Self {
            target_version: RESOURCE_DELETION_TARGET_VERSION.to_owned(),
            subject: ResourceRetentionSubject::from_publication(publication)?,
            content_size,
            manifest: publication.resource.manifest.clone(),
        };
        target.verify()?;
        Ok(target)
    }

    /// Verify the exact provider-neutral deletion target.
    ///
    /// # Errors
    ///
    /// Returns an error when the target version, family, size, or manifest is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource deletion target",
            &self.target_version,
            RESOURCE_DELETION_TARGET_VERSION,
        )?;
        self.subject.verify()?;
        validate_safe_integer("Resource deletion content size", self.content_size)?;
        if let Some(manifest) = &self.manifest {
            manifest.verify()?;
            if manifest.digest != self.subject.family.content_digest
                || manifest.size != self.content_size
            {
                return Err(ResourceError::Integrity {
                    code: "resource_deletion_manifest_target_mismatch".to_owned(),
                    message:
                        "Resource deletion manifest does not match its physical content target"
                            .to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// Closed authority class retaining one Resource pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourcePinKind {
    /// Pin explicitly managed by the Resource lifecycle controller.
    Explicit,
    /// Permanent pin coupled to one immutable Virtual archive certificate.
    VirtualArchive {
        /// Exact archive Resource identity.
        archive_id: String,
    },
    /// Permanent pin coupled to one finalized Agent stream in one Session.
    AgentStream {
        /// Exact Agent Session identity.
        session_id: String,
        /// Exact finalized stream identity.
        stream_id: String,
    },
}

impl ResourcePinKind {
    fn verify(&self) -> ResourceResult<()> {
        match self {
            Self::Explicit => Ok(()),
            Self::VirtualArchive { archive_id } => {
                validate_content_id("Virtual archive", archive_id)
            }
            Self::AgentStream {
                session_id,
                stream_id,
            } => {
                validate_identity("Agent Session", session_id)?;
                validate_identity("Agent stream", stream_id)
            }
        }
    }
}

/// One exact retention obligation admitted by a Resource or owning profile command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePin {
    /// Stable content-derived pin identity.
    pub pin_id: String,
    /// Exact retained physical content family.
    pub subject: ResourceRetentionSubject,
    /// Stable owner of the retention obligation.
    pub owner: String,
    /// Closed authority that alone may release this pin.
    pub kind: ResourcePinKind,
}

#[derive(Serialize)]
struct ResourcePinIdentity<'a> {
    subject: &'a ResourceRetentionSubject,
    owner: &'a str,
    kind: &'a ResourcePinKind,
}

impl ResourcePin {
    /// Seal one caller-identified pin owned by the Resource lifecycle API.
    ///
    /// # Errors
    ///
    /// Returns an error when the subject, pin identity, or owner is invalid.
    pub fn explicit(
        pin_id: impl Into<String>,
        subject: ResourceRetentionSubject,
        owner: impl Into<String>,
    ) -> ResourceResult<Self> {
        let pin_id = pin_id.into();
        let owner = owner.into();
        subject.verify()?;
        validate_identity("Resource pin", &pin_id)?;
        validate_identity("Resource pin owner", &owner)?;
        let pin = Self {
            pin_id,
            subject,
            owner,
            kind: ResourcePinKind::Explicit,
        };
        pin.verify()?;
        Ok(pin)
    }

    /// Seal one content-identified pin owned by another closed profile.
    ///
    /// # Errors
    ///
    /// Returns an error when the subject or closed profile authority is
    /// invalid, or its pin identity cannot be derived.
    pub fn profile(
        subject: ResourceRetentionSubject,
        kind: ResourcePinKind,
    ) -> ResourceResult<Self> {
        if kind == ResourcePinKind::Explicit {
            return Err(ResourceError::Validation(
                "profile pin requires a closed owning profile".to_owned(),
            ));
        }
        subject.verify()?;
        kind.verify()?;
        let owner = resource_profile_pin_owner(&kind)?;
        let pin_id = match &kind {
            ResourcePinKind::VirtualArchive { archive_id } => resource_archive_pin_id(archive_id)?,
            ResourcePinKind::AgentStream { .. } => {
                resource_agent_stream_pin_id(&subject, &owner, &kind)?
            }
            ResourcePinKind::Explicit => unreachable!("explicit profile pin was rejected"),
        };
        let pin = Self {
            pin_id,
            subject,
            owner,
            kind,
        };
        pin.verify()?;
        Ok(pin)
    }

    /// Verify the exact pin identity and closed release authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the subject, owner, authority, or derived pin ID
    /// is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        self.subject.verify()?;
        validate_identity("Resource pin owner", &self.owner)?;
        self.kind.verify()?;
        match &self.kind {
            ResourcePinKind::Explicit => validate_identity("Resource pin", &self.pin_id)?,
            ResourcePinKind::VirtualArchive { archive_id } => {
                if self.subject.resource_id != *archive_id {
                    return Err(ResourceError::Integrity {
                        code: "resource_archive_pin_subject_mismatch".to_owned(),
                        message: "Resource archive pin subject does not match its archive identity"
                            .to_owned(),
                    });
                }
                let expected = resource_archive_pin_id(archive_id)?;
                if self.pin_id != expected {
                    return Err(ResourceError::Integrity {
                        code: "resource_archive_pin_identity_mismatch".to_owned(),
                        message: format!(
                            "Resource archive pin {} does not match {expected}",
                            self.pin_id
                        ),
                    });
                }
                let expected_owner = resource_archive_pin_owner(archive_id)?;
                if self.owner != expected_owner {
                    return Err(ResourceError::Integrity {
                        code: "resource_archive_pin_owner_mismatch".to_owned(),
                        message: format!(
                            "Resource archive pin owner {} does not match {expected_owner}",
                            self.owner
                        ),
                    });
                }
            }
            ResourcePinKind::AgentStream {
                session_id,
                stream_id,
            } => {
                let expected_owner = resource_agent_stream_pin_owner(session_id, stream_id)?;
                if self.owner != expected_owner {
                    return Err(ResourceError::Integrity {
                        code: "resource_agent_stream_pin_owner_mismatch".to_owned(),
                        message: format!(
                            "Resource Agent stream pin owner {} does not match {expected_owner}",
                            self.owner
                        ),
                    });
                }
                let expected =
                    resource_agent_stream_pin_id(&self.subject, &self.owner, &self.kind)?;
                if self.pin_id != expected {
                    return Err(ResourceError::Integrity {
                        code: "resource_agent_stream_pin_identity_mismatch".to_owned(),
                        message: format!(
                            "Resource Agent stream pin {} does not match {expected}",
                            self.pin_id
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ResourceArchivePinIdentity<'a> {
    purpose: &'static str,
    archive_id: &'a str,
}

/// Derive the permanent pin identity for one immutable Virtual archive.
///
/// # Errors
///
/// Returns an error for an invalid archive ID or failed identity derivation.
pub fn resource_archive_pin_id(archive_id: &str) -> ResourceResult<String> {
    validate_content_id("Virtual archive", archive_id)?;
    cymule_core::content_id(
        RESOURCE_ARCHIVE_PIN_VERSION,
        &ResourceArchivePinIdentity {
            purpose: "pin",
            archive_id,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Derive the sole Virtual profile owner for one immutable archive pin.
///
/// # Errors
///
/// Returns an error for an invalid archive ID or failed identity derivation.
pub fn resource_archive_pin_owner(archive_id: &str) -> ResourceResult<String> {
    validate_content_id("Virtual archive", archive_id)?;
    cymule_core::content_id(
        RESOURCE_ARCHIVE_PIN_VERSION,
        &ResourceArchivePinIdentity {
            purpose: "owner",
            archive_id,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Derive the sole Agent profile owner for one finalized stream pin.
///
/// # Errors
///
/// Returns an error for invalid Session or stream identities, or failed
/// identity derivation.
pub fn resource_agent_stream_pin_owner(
    session_id: &str,
    stream_id: &str,
) -> ResourceResult<String> {
    validate_identity("Agent Session", session_id)?;
    validate_identity("Agent stream", stream_id)?;
    cymule_core::content_id(
        RESOURCE_AGENT_STREAM_PIN_VERSION,
        &("owner", session_id, stream_id),
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn resource_profile_pin_owner(kind: &ResourcePinKind) -> ResourceResult<String> {
    match kind {
        ResourcePinKind::VirtualArchive { archive_id } => resource_archive_pin_owner(archive_id),
        ResourcePinKind::AgentStream {
            session_id,
            stream_id,
        } => resource_agent_stream_pin_owner(session_id, stream_id),
        ResourcePinKind::Explicit => Err(ResourceError::Validation(
            "explicit Resource pins do not have a profile-derived owner".to_owned(),
        )),
    }
}

fn resource_agent_stream_pin_id(
    subject: &ResourceRetentionSubject,
    owner: &str,
    kind: &ResourcePinKind,
) -> ResourceResult<String> {
    if !matches!(kind, ResourcePinKind::AgentStream { .. }) {
        return Err(ResourceError::Validation(
            "Agent stream pin identity requires AgentStream authority".to_owned(),
        ));
    }
    cymule_core::content_id(
        RESOURCE_AGENT_STREAM_PIN_VERSION,
        &(
            "pin",
            ResourcePinIdentity {
                subject,
                owner,
                kind,
            },
        ),
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Pin delta consumed only inside another profile's typed durable transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceProfilePin {
    /// Delta wire version.
    pub pin_version: String,
    /// Exact pin to introduce atomically with the owning profile record.
    pub pin: ResourcePin,
}

impl ResourceProfilePin {
    /// Construct a closed cross-profile pin delta.
    ///
    /// # Errors
    ///
    /// Returns an error when the pin is invalid or is not profile-owned.
    pub fn new(pin: ResourcePin) -> ResourceResult<Self> {
        if pin.kind == ResourcePinKind::Explicit {
            return Err(ResourceError::Validation(
                "profile pin delta cannot carry an explicit Resource pin".to_owned(),
            ));
        }
        pin.verify()?;
        Ok(Self {
            pin_version: RESOURCE_PROFILE_PIN_VERSION.to_owned(),
            pin,
        })
    }

    /// Verify the cross-profile pin delta.
    ///
    /// # Errors
    ///
    /// Returns an error when the version or profile-owned pin is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource profile pin",
            &self.pin_version,
            RESOURCE_PROFILE_PIN_VERSION,
        )?;
        if self.pin.kind == ResourcePinKind::Explicit {
            return Err(ResourceError::Validation(
                "profile pin delta cannot carry an explicit Resource pin".to_owned(),
            ));
        }
        self.pin.verify()
    }
}

/// Archive-only release delta consumed with a Virtual archive retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceArchiveRelease {
    /// Archive release wire version.
    pub release_version: String,
    /// Exact Virtual retirement command identity.
    pub release_id: String,
    /// Exact immutable archive identity.
    pub archive_id: String,
    /// Exact archive pin being released.
    pub pin_id: String,
}

impl ResourceArchiveRelease {
    /// Construct a release delta from the exact retained archive pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the pin is invalid, is not archive-owned, or the
    /// resulting release cannot be verified.
    pub fn new(release_id: impl Into<String>, pin: &ResourcePin) -> ResourceResult<Self> {
        pin.verify()?;
        let ResourcePinKind::VirtualArchive { archive_id } = &pin.kind else {
            return Err(ResourceError::Validation(
                "archive release requires a Virtual archive pin".to_owned(),
            ));
        };
        let release = Self {
            release_version: RESOURCE_ARCHIVE_RELEASE_VERSION.to_owned(),
            release_id: release_id.into(),
            archive_id: archive_id.clone(),
            pin_id: pin.pin_id.clone(),
        };
        release.verify()?;
        Ok(release)
    }

    /// Verify the closed archive release delta.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, retirement ID, archive ID, or pin ID
    /// is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource archive release",
            &self.release_version,
            RESOURCE_ARCHIVE_RELEASE_VERSION,
        )?;
        validate_content_id("Virtual retirement command", &self.release_id)?;
        validate_content_id("Virtual archive", &self.archive_id)?;
        let expected = resource_archive_pin_id(&self.archive_id)?;
        require_exact_id("Resource archive pin", &self.pin_id, &expected)
    }
}

/// Closed garbage-collection decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceGcDisposition {
    /// At least one exact pin still retains the physical content family.
    Retained,
    /// No exact pin remains; provider bytes may be fenced for deletion.
    Eligible,
}

/// Durable evidence retaining one exact Resource pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePinReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact admitted command identity.
    pub command_id: String,
    /// Exact retained pin.
    pub pin: ResourcePin,
    /// Resulting active pin count for the physical content family.
    pub active_pin_count: u64,
}

/// Durable evidence releasing one exact Resource pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceReleaseReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact Resource or owning-profile command identity.
    pub command_id: String,
    /// Stable release operation identity.
    pub release_id: String,
    /// Exact released pin, including its release authority.
    pub pin: ResourcePin,
    /// Resulting active pin count for the physical content family.
    pub active_pin_count: u64,
}

/// Durable evidence for one exact garbage-collection decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceGcReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact admitted command identity.
    pub command_id: String,
    /// Stable collection operation identity.
    pub gc_id: String,
    /// Exact physical content family evaluated.
    pub family: ResourceRetentionFamily,
    /// Exact active pin count observed by the lifecycle authority.
    pub active_pin_count: u64,
    /// Closed collection decision.
    pub disposition: ResourceGcDisposition,
}

/// Durable fence authorizing one exact provider deletion attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeleteIntent {
    /// Intent wire version.
    pub intent_version: String,
    /// Content identity of every following field.
    pub intent_id: String,
    /// Exact begin-delete command identity.
    pub command_id: String,
    /// Stable caller-supplied deletion identity.
    pub delete_id: String,
    /// Exact keyed Resource command that produced the eligible GC receipt.
    pub gc_command_id: String,
    /// Exact eligible GC receipt authorizing this fence.
    pub gc_receipt_id: String,
    /// Minimal provider-neutral physical target.
    pub target: ResourceDeletionTarget,
}

/// Verified terminal provider deletion receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeleteReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact reconcile-delete command identity.
    pub command_id: String,
    /// Exact durable deletion fence consumed by the provider attempt.
    pub intent: ResourceDeleteIntent,
}

/// Exact publication deletion and absence-readback authority selected by binding.
///
/// This is an I/O boundary, not a persistence mutation capability. The
/// durable coordinator supplies the retained target and derives the terminal
/// receipt only after this method returns success. Absence means that a new
/// read or stat cannot resolve published payload and that the provider has
/// proved the target generation's current payload objects absent. A provider
/// may retain permanent non-payload fence metadata when that is required to
/// prevent an in-flight writer from republishing the deleted identity; such a
/// fence must never resolve as content or authorize recreation.
pub trait ResourceDeleter {
    /// Immutable implementation binding owned by this deleter.
    fn binding(&self) -> &str;

    /// Idempotently delete the exact published target and prove its payload absent.
    ///
    /// # Errors
    ///
    /// Returns an error unless provider readback proves the exact published
    /// payload absent and future reads remain fenced from the deleted identity.
    fn delete_and_verify_absent(&mut self, target: &ResourceDeletionTarget) -> ResourceResult<()>;
}

/// Exact producer occurrence and result provenance for one transferred Resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceProducerProvenance {
    /// Run that owns the completed producer occurrence.
    pub run_id: String,
    /// Exact completed component occurrence.
    pub occurrence_id: String,
    /// Exact typed Artifact emitted by the producer.
    pub result: cymule_core::ArtifactRef,
}

/// One immutable Run-to-Run Resource transfer authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoff {
    /// Handoff wire version.
    pub handoff_version: String,
    /// Stable caller-supplied transfer identity.
    pub transfer_id: String,
    /// Exact producer authority.
    pub producer: ResourceProducerProvenance,
    /// Exact target Run.
    pub to_run: String,
    /// Exact target input correlation slot.
    pub slot: String,
    /// Exact typed Artifact containing the verified Resource descriptor.
    pub resource: cymule_core::ArtifactRef,
}

/// One immutable activation of a transfer into its exact target input Wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffActivation {
    /// Activation wire version.
    pub activation_version: String,
    /// Content identity of the transfer, target, Wait, and exact result.
    pub activation_id: String,
    /// Exact source transfer identity.
    pub transfer_id: String,
    /// Exact target Run.
    pub to_run: String,
    /// Exact target input Wait.
    pub wait_id: String,
    /// Exact Resource Artifact admitted as the Wait result.
    pub result: cymule_core::ArtifactRef,
}

#[derive(Serialize)]
struct HandoffActivationIdentity<'a> {
    transfer_id: &'a str,
    to_run: &'a str,
    wait_id: &'a str,
    result: &'a cymule_core::ArtifactRef,
}

impl ResourceHandoff {
    /// Verify the closed transfer authority.
    ///
    /// # Errors
    ///
    /// Returns an error when any transfer identity, Artifact reference, or
    /// source/target relationship is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource handoff",
            &self.handoff_version,
            RESOURCE_HANDOFF_VERSION,
        )?;
        validate_identity("Resource transfer", &self.transfer_id)?;
        validate_identity("Resource producer Run", &self.producer.run_id)?;
        validate_identity("Resource producer occurrence", &self.producer.occurrence_id)?;
        validate_identity("Resource target Run", &self.to_run)?;
        validate_identity("Resource target slot", &self.slot)?;
        self.producer
            .result
            .validate()
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        self.resource
            .validate()
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if self.producer.result != self.resource {
            return Err(ResourceError::Integrity {
                code: "resource_handoff_producer_result_mismatch".to_owned(),
                message:
                    "Resource handoff descriptor does not match its producer Artifact reference"
                        .to_owned(),
            });
        }
        if self.producer.run_id == self.to_run {
            return Err(ResourceError::Validation(
                "Resource handoff requires distinct source and target Runs".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ResourceHandoffActivation {
    /// Seal the unique activation for one transfer and exact input Wait.
    ///
    /// # Errors
    ///
    /// Returns an error when the handoff or Wait identity is invalid, or the
    /// activation identity cannot be derived.
    pub fn new(handoff: &ResourceHandoff, wait_id: impl Into<String>) -> ResourceResult<Self> {
        handoff.verify()?;
        let wait_id = wait_id.into();
        validate_identity("Resource activation Wait", &wait_id)?;
        let activation_id = resource_handoff_activation_id(
            &handoff.transfer_id,
            &handoff.to_run,
            &wait_id,
            &handoff.resource,
        )?;
        let activation = Self {
            activation_version: RESOURCE_HANDOFF_ACTIVATION_VERSION.to_owned(),
            activation_id,
            transfer_id: handoff.transfer_id.clone(),
            to_run: handoff.to_run.clone(),
            wait_id,
            result: handoff.resource.clone(),
        };
        activation.verify()?;
        Ok(activation)
    }

    /// Verify the closed activation identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the activation version, identities, Artifact, or
    /// derived activation ID is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource handoff activation",
            &self.activation_version,
            RESOURCE_HANDOFF_ACTIVATION_VERSION,
        )?;
        validate_identity("Resource activation transfer", &self.transfer_id)?;
        validate_identity("Resource activation target Run", &self.to_run)?;
        validate_identity("Resource activation Wait", &self.wait_id)?;
        self.result
            .validate()
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let expected = resource_handoff_activation_id(
            &self.transfer_id,
            &self.to_run,
            &self.wait_id,
            &self.result,
        )?;
        if self.activation_id != expected {
            return Err(ResourceError::Integrity {
                code: "resource_handoff_activation_identity_mismatch".to_owned(),
                message: format!(
                    "Resource handoff activation {} does not match {expected}",
                    self.activation_id
                ),
            });
        }
        Ok(())
    }
}

/// Derive the unique identity of one exact handoff activation.
///
/// # Errors
///
/// Returns an error when any identity or Artifact reference is invalid, or
/// canonical identity derivation fails.
pub fn resource_handoff_activation_id(
    transfer_id: &str,
    to_run: &str,
    wait_id: &str,
    result: &cymule_core::ArtifactRef,
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_HANDOFF_ACTIVATION_VERSION,
        &HandoffActivationIdentity {
            transfer_id,
            to_run,
            wait_id,
            result,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Payload-free, position-bound target index entry for one transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffIndexEntry {
    /// Index entry wire version.
    pub index_version: String,
    /// Exact target Run.
    pub to_run: String,
    /// Zero-based append position within this target's transfer index.
    pub target_index: u64,
    /// Exact target input slot; unique within the target Run.
    pub slot: String,
    /// Exact transfer authority selected by this entry.
    pub transfer_id: String,
    /// Exact transfer receipt selected by this entry.
    pub authority_receipt_id: String,
}

/// Payload-free, position-bound target index entry for one activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffActivationIndexEntry {
    /// Index entry wire version.
    pub index_version: String,
    /// Exact target Run.
    pub to_run: String,
    /// Zero-based append position within this target's activation index.
    pub activation_index: u64,
    /// Exact transfer authority selected by this activation.
    pub transfer_id: String,
    /// Exact activation authority selected by this entry.
    pub activation_id: String,
    /// Exact activation receipt selected by this entry.
    pub authority_receipt_id: String,
}

/// Durable transfer receipt binding authority and target index atomically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffReceipt {
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact admitted transfer command identity.
    pub command_id: String,
    /// Canonical transfer authority.
    pub handoff: ResourceHandoff,
    /// Exact payload-free target index entry.
    pub index: ResourceHandoffIndexEntry,
}

/// Durable activation receipt binding source, target index, and Wait completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffActivationReceipt {
    /// Content identity of every following field.
    pub receipt_id: String,
    /// Exact admitted activation command identity.
    pub command_id: String,
    /// Canonical activation authority.
    pub activation: ResourceHandoffActivation,
    /// Exact prior transfer receipt consumed by this activation.
    pub source_receipt_id: String,
    /// Exact payload-free target activation index entry.
    pub index: ResourceHandoffActivationIndexEntry,
    /// Exact durable coupled Wait-completion receipt.
    pub coupled_wait_receipt_id: String,
}

/// One bounded page of target-index entries and their exact authorities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceHandoffPage {
    /// Exact handoffs resolved from one contiguous index page.
    pub handoffs: Vec<ResourceHandoff>,
    /// Next stable append position, or `None` at the current end.
    pub next_index: Option<u64>,
}

/// One closed Resource state transition admitted by the durable coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCommand {
    /// Command wire version.
    pub command_version: String,
    /// Stable stage-and-operation identity.
    pub command_id: String,
    /// Closed Resource transition semantics.
    pub operation: ResourceOperation,
}

/// Closed Resource transition semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceOperation {
    /// Introduce one explicit Resource-owned retention pin.
    Pin {
        /// Exact explicit pin.
        pin: ResourcePin,
    },
    /// Release one explicit Resource-owned pin.
    Release {
        /// Stable caller operation identity.
        release_id: String,
        /// Exact pin expected to remain active.
        pin_id: String,
        /// Exact owner expected on the retained pin.
        owner: String,
    },
    /// Snapshot collection eligibility for one exact physical content family.
    GarbageCollect {
        /// Stable caller operation identity.
        gc_id: String,
        /// Exact physical content family evaluated.
        family: ResourceRetentionFamily,
    },
    /// Fence one provider-neutral target after an exact eligible GC receipt.
    BeginDelete {
        /// Stable caller deletion identity.
        delete_id: String,
        /// Exact keyed Resource command that produced the eligible GC receipt.
        gc_command_id: String,
        /// Exact eligible GC receipt consumed by the fence.
        gc_receipt_id: String,
        /// Exact provider-neutral target.
        target: ResourceDeletionTarget,
    },
    /// Reconcile one fenced deletion through its exact provider binding.
    ReconcileDelete {
        /// Stable caller deletion identity.
        delete_id: String,
        /// Exact retained deletion intent consumed by this completion.
        intent_id: String,
    },
    /// Publish one transfer authority and its target index atomically.
    Transfer {
        /// Canonical transfer authority.
        handoff: ResourceHandoff,
    },
    /// Activate one exact prior transfer into one exact input Wait.
    ActivateTransfer {
        /// Canonical activation authority.
        activation: ResourceHandoffActivation,
        /// Exact prior transfer receipt consumed by the activation.
        source_receipt_id: String,
    },
}

impl ResourceOperation {
    fn identity(&self) -> (&'static str, &str) {
        match self {
            Self::Pin { pin } => ("pin", &pin.pin_id),
            Self::Release { release_id, .. } => ("release", release_id),
            Self::GarbageCollect { gc_id, .. } => ("garbage_collect", gc_id),
            Self::BeginDelete { delete_id, .. } => ("begin_delete", delete_id),
            Self::ReconcileDelete { delete_id, .. } => ("reconcile_delete", delete_id),
            Self::Transfer { handoff } => ("transfer", &handoff.transfer_id),
            Self::ActivateTransfer { activation, .. } => {
                ("activate_transfer", &activation.activation_id)
            }
        }
    }

    fn verify(&self) -> ResourceResult<()> {
        match self {
            Self::Pin { pin } => {
                pin.verify()?;
                if pin.kind != ResourcePinKind::Explicit {
                    return Err(ResourceError::Validation(
                        "Resource Pin command accepts only explicit pins".to_owned(),
                    ));
                }
            }
            Self::Release {
                release_id,
                pin_id,
                owner,
            } => {
                validate_identity("Resource release", release_id)?;
                validate_identity("Resource pin", pin_id)?;
                validate_identity("Resource pin owner", owner)?;
            }
            Self::GarbageCollect { gc_id, family } => {
                validate_identity("Resource GC", gc_id)?;
                family.verify()?;
            }
            Self::BeginDelete {
                delete_id,
                gc_command_id,
                gc_receipt_id,
                target,
            } => {
                validate_identity("Resource delete", delete_id)?;
                validate_content_id("Resource GC command", gc_command_id)?;
                validate_content_id("Resource GC receipt", gc_receipt_id)?;
                target.verify()?;
            }
            Self::ReconcileDelete {
                delete_id,
                intent_id,
            } => {
                validate_identity("Resource delete", delete_id)?;
                validate_content_id("Resource delete intent", intent_id)?;
            }
            Self::Transfer { handoff } => handoff.verify()?,
            Self::ActivateTransfer {
                activation,
                source_receipt_id,
            } => {
                activation.verify()?;
                validate_content_id("Resource handoff source receipt", source_receipt_id)?;
            }
        }
        Ok(())
    }
}

impl ResourceCommand {
    /// Seal one closed Resource command under its stable operation identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation is invalid or its command identity
    /// cannot be derived.
    pub fn new(operation: ResourceOperation) -> ResourceResult<Self> {
        operation.verify()?;
        let (stage, operation_id) = operation.identity();
        let command_id = resource_command_id(stage, operation_id)?;
        let command = Self {
            command_version: RESOURCE_COMMAND_VERSION.to_owned(),
            command_id,
            operation,
        };
        command.verify()?;
        Ok(command)
    }

    /// Verify the command shape and stable stage-and-operation identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, operation, or derived command ID is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource command",
            &self.command_version,
            RESOURCE_COMMAND_VERSION,
        )?;
        self.operation.verify()?;
        let (stage, operation_id) = self.operation.identity();
        let expected = resource_command_id(stage, operation_id)?;
        if self.command_id != expected {
            return Err(ResourceError::Integrity {
                code: "resource_command_identity_mismatch".to_owned(),
                message: format!(
                    "Resource command {} does not match {expected}",
                    self.command_id
                ),
            });
        }
        Ok(())
    }
}

fn resource_command_id(stage: &str, operation_id: &str) -> ResourceResult<String> {
    cymule_core::content_id(RESOURCE_COMMAND_VERSION, &(stage, operation_id))
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

/// Closed result of one admitted Resource command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceCommandOutcome {
    /// Exact pin receipt.
    Pin {
        /// Durable pin evidence.
        receipt: ResourcePinReceipt,
    },
    /// Exact explicit release receipt.
    Release {
        /// Durable release evidence.
        receipt: ResourceReleaseReceipt,
    },
    /// Exact collection decision.
    GarbageCollect {
        /// Durable GC evidence.
        receipt: ResourceGcReceipt,
    },
    /// Exact provider deletion fence.
    BeginDelete {
        /// Durable delete intent.
        intent: ResourceDeleteIntent,
    },
    /// Exact terminal provider deletion evidence.
    ReconcileDelete {
        /// Durable deletion receipt.
        receipt: ResourceDeleteReceipt,
    },
    /// Exact transfer authority and target index.
    Transfer {
        /// Durable transfer receipt.
        receipt: ResourceHandoffReceipt,
    },
    /// Exact activation, target index, source, and Wait receipt binding.
    ActivateTransfer {
        /// Durable activation receipt.
        receipt: ResourceHandoffActivationReceipt,
    },
}

/// Exact durable receipt retaining both the admitted command and its outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCommandReceipt {
    /// Command receipt wire version.
    pub receipt_version: String,
    /// Content identity of the exact command and outcome.
    pub receipt_id: String,
    /// Exact admitted command.
    pub command: ResourceCommand,
    /// Closed command outcome.
    pub outcome: ResourceCommandOutcome,
}

/// Current lifecycle of one physical content family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceRetentionDisposition {
    /// At least one exact pin retains the bytes.
    Active,
    /// No exact pin currently retains the bytes.
    Unretained,
    /// One exact deletion intent fences the bytes against new pins.
    DeleteFenced,
    /// Provider readback proved the bytes absent.
    Deleted,
}

/// Closed owning profile for one Resource lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleProfile {
    /// Explicit pin, release, GC, or deletion owned by the Resource command union.
    Resource,
    /// Permanent finalized-stream pin owned by one Agent command.
    Agent,
    /// Archive pin or retirement owned by one Virtual command.
    Virtual,
}

/// Closed exact locator for the owning profile command and outer receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceLifecycleReceiptLocator {
    /// Resource-owned lifecycle command and receipt in the Resource keyed maps.
    Resource {
        /// Exact Resource command identity.
        command_id: String,
        /// Exact outer Resource command-receipt identity.
        receipt_id: String,
    },
    /// Agent-owned finalized-stream command and receipt in the Agent keyed maps.
    Agent {
        /// Exact Agent command identity.
        command_id: String,
        /// Exact outer Agent command-receipt identity.
        receipt_id: String,
    },
    /// Agent-owned publication reservation retained in its exact stream current.
    AgentPublicationReservation {
        /// Exact Agent Finalize command identity.
        command_id: String,
        /// Owning Agent Session identity.
        session_id: String,
        /// Owning Agent stream identity.
        stream_id: String,
        /// Exact immutable reservation identity embedded in the stream current.
        reservation_id: String,
    },
    /// Virtual-owned archive transition in one scheduler receipt partition.
    Virtual {
        /// Exact Virtual scheduler partition.
        scheduler_id: String,
        /// Exact Virtual semantic command identity used by its receipt key.
        command_id: String,
        /// Exact outer `VirtualPersistenceReceipt` identity.
        outer_receipt_id: String,
    },
}

/// Bounded versioned reference to the owning profile command and receipt.
///
/// The referenced typed command and receipt live in their profile-specific
/// keyed `StateRoot` maps. Durable resolution must load both exact identities,
/// run the owning profile verifier, and match its Resource outcome to the
/// current projection. Keeping only this edge prevents current projections
/// and Agent before-witnesses from recursively embedding lifecycle history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLifecycleReceiptRef {
    /// Receipt-reference wire version.
    pub reference_version: String,
    /// Closed profile-specific exact lookup locator.
    pub locator: ResourceLifecycleReceiptLocator,
}

/// Keyed current projection for one physical content family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRetentionCurrent {
    /// Projection wire version.
    pub state_version: String,
    /// Exact physical content family.
    pub family: ResourceRetentionFamily,
    /// Exact active pin count.
    pub active_pin_count: u64,
    /// Current closed lifecycle state.
    pub disposition: ResourceRetentionDisposition,
    /// Exact bounded edge to the typed receipt that produced this projection.
    pub last_receipt: ResourceLifecycleReceiptRef,
}

/// Current lifecycle of one exact pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePinStatus {
    /// The pin is a durable pre-publication retention obligation. It blocks
    /// collection before provider I/O but does not yet claim the bytes exist.
    Reserved,
    /// The pin actively retains its physical content family.
    Active,
    /// The pin has one terminal release receipt.
    Released,
}

/// Keyed current projection for one exact pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePinCurrent {
    /// Projection wire version.
    pub state_version: String,
    /// Exact pin authority.
    pub pin: ResourcePin,
    /// Current terminal status.
    pub status: ResourcePinStatus,
    /// Exact bounded edge to the typed pin or release receipt that produced this projection.
    pub last_receipt: ResourceLifecycleReceiptRef,
}

/// Exact keyed postcondition of one pin or release transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourcePinPostcondition {
    /// Resulting physical-family projection.
    pub retention: ResourceRetentionCurrent,
    /// Resulting exact pin projection.
    pub pin: ResourcePinCurrent,
}

/// Current lifecycle of one exact provider deletion identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDeleteStatus {
    /// Provider deletion is durably fenced but not yet closed.
    Fenced,
    /// Provider absence was durably verified.
    Completed,
}

/// Keyed current projection for one exact provider deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeleteCurrent {
    /// Projection wire version.
    pub state_version: String,
    /// Exact durable deletion fence.
    pub intent: ResourceDeleteIntent,
    /// Current closed deletion status.
    pub status: ResourceDeleteStatus,
    /// Exact bounded edge to the Resource receipt that produced this projection.
    pub last_receipt: ResourceLifecycleReceiptRef,
}

/// Exact keyed postcondition of one deletion fence or completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceDeletePostcondition {
    /// Resulting physical-family projection.
    pub retention: ResourceRetentionCurrent,
    /// Resulting exact deletion projection.
    pub deletion: ResourceDeleteCurrent,
}

/// Keyed current transfer authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffCurrent {
    /// Exact transfer receipt binding authority and index.
    pub receipt: ResourceHandoffReceipt,
}

/// Keyed current activation authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandoffActivationCurrent {
    /// Exact activation receipt binding source, index, and Wait completion.
    pub receipt: ResourceHandoffActivationReceipt,
}

impl ResourcePinReceipt {
    /// Seal durable evidence for one exact admitted pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, pin, count, or derived receipt is
    /// invalid.
    pub fn new(
        command_id: impl Into<String>,
        pin: ResourcePin,
        active_pin_count: u64,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let receipt_id = cymule_core::content_id(
            RESOURCE_PIN_RECEIPT_VERSION,
            &(command_id.as_str(), &pin, active_pin_count),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let receipt = Self {
            receipt_version: RESOURCE_PIN_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command_id,
            pin,
            active_pin_count,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify this pin receipt and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any receipt field or identity is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource pin receipt",
            &self.receipt_version,
            RESOURCE_PIN_RECEIPT_VERSION,
        )?;
        validate_content_id("Resource command", &self.command_id)?;
        self.pin.verify()?;
        validate_safe_integer("Resource active pin count", self.active_pin_count)?;
        if self.active_pin_count == 0 {
            return Err(ResourceError::Integrity {
                code: "resource_pin_receipt_zero_active_pins".to_owned(),
                message: "Resource pin receipt cannot produce zero active pins".to_owned(),
            });
        }
        let expected = cymule_core::content_id(
            RESOURCE_PIN_RECEIPT_VERSION,
            &(self.command_id.as_str(), &self.pin, self.active_pin_count),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource pin receipt", &self.receipt_id, &expected)
    }
}

impl ResourceReleaseReceipt {
    /// Seal durable evidence for one exact released pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, release, pin, count, or derived
    /// receipt is invalid.
    pub fn new(
        command_id: impl Into<String>,
        release_id: impl Into<String>,
        pin: ResourcePin,
        active_pin_count: u64,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let release_id = release_id.into();
        let receipt_id = cymule_core::content_id(
            RESOURCE_RELEASE_RECEIPT_VERSION,
            &(
                command_id.as_str(),
                release_id.as_str(),
                &pin,
                active_pin_count,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let receipt = Self {
            receipt_version: RESOURCE_RELEASE_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command_id,
            release_id,
            pin,
            active_pin_count,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify this release receipt and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any receipt field or identity is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource release receipt",
            &self.receipt_version,
            RESOURCE_RELEASE_RECEIPT_VERSION,
        )?;
        validate_content_id("Resource command", &self.command_id)?;
        validate_identity("Resource release", &self.release_id)?;
        self.pin.verify()?;
        validate_safe_integer("Resource active pin count", self.active_pin_count)?;
        let expected = cymule_core::content_id(
            RESOURCE_RELEASE_RECEIPT_VERSION,
            &(
                self.command_id.as_str(),
                self.release_id.as_str(),
                &self.pin,
                self.active_pin_count,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource release receipt", &self.receipt_id, &expected)
    }
}

impl ResourceGcReceipt {
    /// Seal one exact collection decision.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, GC identity, family, count, or
    /// derived receipt is invalid.
    pub fn new(
        command_id: impl Into<String>,
        gc_id: impl Into<String>,
        family: ResourceRetentionFamily,
        active_pin_count: u64,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let gc_id = gc_id.into();
        let disposition = if active_pin_count == 0 {
            ResourceGcDisposition::Eligible
        } else {
            ResourceGcDisposition::Retained
        };
        let receipt_id = cymule_core::content_id(
            RESOURCE_GC_RECEIPT_VERSION,
            &(
                command_id.as_str(),
                gc_id.as_str(),
                &family,
                active_pin_count,
                disposition,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let receipt = Self {
            receipt_version: RESOURCE_GC_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command_id,
            gc_id,
            family,
            active_pin_count,
            disposition,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify this collection decision and its exact pin-count relation.
    ///
    /// # Errors
    ///
    /// Returns an error when any field, disposition relation, or receipt ID is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource GC receipt",
            &self.receipt_version,
            RESOURCE_GC_RECEIPT_VERSION,
        )?;
        validate_content_id("Resource command", &self.command_id)?;
        validate_identity("Resource GC", &self.gc_id)?;
        self.family.verify()?;
        validate_safe_integer("Resource active pin count", self.active_pin_count)?;
        if (self.active_pin_count == 0) != (self.disposition == ResourceGcDisposition::Eligible) {
            return Err(ResourceError::Integrity {
                code: "resource_gc_disposition_pin_count_mismatch".to_owned(),
                message: "Resource GC disposition does not match its exact active pin count"
                    .to_owned(),
            });
        }
        let expected = cymule_core::content_id(
            RESOURCE_GC_RECEIPT_VERSION,
            &(
                self.command_id.as_str(),
                self.gc_id.as_str(),
                &self.family,
                self.active_pin_count,
                self.disposition,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource GC receipt", &self.receipt_id, &expected)
    }
}

impl ResourceDeleteIntent {
    /// Seal one exact provider deletion fence.
    ///
    /// # Errors
    ///
    /// Returns an error when any command, GC authority, target, or derived
    /// intent identity is invalid.
    pub fn new(
        command_id: impl Into<String>,
        delete_id: impl Into<String>,
        gc_command_id: impl Into<String>,
        gc_receipt_id: impl Into<String>,
        target: ResourceDeletionTarget,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let delete_id = delete_id.into();
        let gc_command_id = gc_command_id.into();
        let gc_receipt_id = gc_receipt_id.into();
        let intent_id = cymule_core::content_id(
            RESOURCE_DELETE_INTENT_VERSION,
            &(
                command_id.as_str(),
                delete_id.as_str(),
                gc_command_id.as_str(),
                gc_receipt_id.as_str(),
                &target,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let intent = Self {
            intent_version: RESOURCE_DELETE_INTENT_VERSION.to_owned(),
            intent_id,
            command_id,
            delete_id,
            gc_command_id,
            gc_receipt_id,
            target,
        };
        intent.verify()?;
        Ok(intent)
    }

    /// Verify this exact provider deletion fence.
    ///
    /// # Errors
    ///
    /// Returns an error when any intent field or identity is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource delete intent",
            &self.intent_version,
            RESOURCE_DELETE_INTENT_VERSION,
        )?;
        validate_content_id("Resource command", &self.command_id)?;
        validate_identity("Resource delete", &self.delete_id)?;
        validate_content_id("Resource GC command", &self.gc_command_id)?;
        validate_content_id("Resource GC receipt", &self.gc_receipt_id)?;
        self.target.verify()?;
        let expected = cymule_core::content_id(
            RESOURCE_DELETE_INTENT_VERSION,
            &(
                self.command_id.as_str(),
                self.delete_id.as_str(),
                self.gc_command_id.as_str(),
                self.gc_receipt_id.as_str(),
                &self.target,
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource delete intent", &self.intent_id, &expected)
    }
}

impl ResourceDeleteReceipt {
    /// Seal terminal deletion evidence after exact absence readback.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, intent, or derived receipt identity
    /// is invalid.
    pub fn new(
        command_id: impl Into<String>,
        intent: ResourceDeleteIntent,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let receipt_id = cymule_core::content_id(
            RESOURCE_DELETE_RECEIPT_VERSION,
            &(command_id.as_str(), &intent),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let receipt = Self {
            receipt_version: RESOURCE_DELETE_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command_id,
            intent,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify this terminal provider absence receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when any receipt field or identity is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource delete receipt",
            &self.receipt_version,
            RESOURCE_DELETE_RECEIPT_VERSION,
        )?;
        validate_content_id("Resource command", &self.command_id)?;
        self.intent.verify()?;
        let expected = cymule_core::content_id(
            RESOURCE_DELETE_RECEIPT_VERSION,
            &(self.command_id.as_str(), &self.intent),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource delete receipt", &self.receipt_id, &expected)
    }
}

impl ResourceHandoffIndexEntry {
    /// Verify one exact target-index position and authority reference.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, target, index, slot, transfer, or
    /// receipt reference is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource handoff index",
            &self.index_version,
            RESOURCE_HANDOFF_INDEX_VERSION,
        )?;
        validate_identity("Resource target Run", &self.to_run)?;
        validate_safe_integer("Resource handoff target index", self.target_index)?;
        validate_identity("Resource target slot", &self.slot)?;
        validate_identity("Resource transfer", &self.transfer_id)?;
        validate_content_id("Resource handoff receipt", &self.authority_receipt_id)
    }
}

impl ResourceHandoffActivationIndexEntry {
    /// Verify one exact target activation-index position and authority reference.
    ///
    /// # Errors
    ///
    /// Returns an error when any activation index field or authority reference
    /// is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource handoff activation index",
            &self.index_version,
            RESOURCE_HANDOFF_ACTIVATION_INDEX_VERSION,
        )?;
        validate_identity("Resource target Run", &self.to_run)?;
        validate_safe_integer(
            "Resource handoff target activation index",
            self.activation_index,
        )?;
        validate_identity("Resource transfer", &self.transfer_id)?;
        validate_content_id("Resource handoff activation", &self.activation_id)?;
        validate_content_id(
            "Resource handoff activation receipt",
            &self.authority_receipt_id,
        )
    }
}

impl ResourceHandoffReceipt {
    /// Seal the exact transfer authority and assigned target-index position.
    ///
    /// # Errors
    ///
    /// Returns an error when the command, handoff, index, or derived receipt
    /// identity is invalid.
    pub fn new(
        command_id: impl Into<String>,
        handoff: ResourceHandoff,
        target_index: u64,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let receipt_id = resource_handoff_receipt_id(&command_id, &handoff, target_index)?;
        let receipt = Self {
            receipt_id: receipt_id.clone(),
            command_id,
            index: ResourceHandoffIndexEntry {
                index_version: RESOURCE_HANDOFF_INDEX_VERSION.to_owned(),
                to_run: handoff.to_run.clone(),
                target_index,
                slot: handoff.slot.clone(),
                transfer_id: handoff.transfer_id.clone(),
                authority_receipt_id: receipt_id,
            },
            handoff,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify authority, target index, command, and receipt bindings.
    ///
    /// # Errors
    ///
    /// Returns an error when any transfer or index binding is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_content_id("Resource command", &self.command_id)?;
        self.handoff.verify()?;
        self.index.verify()?;
        if self.index.to_run != self.handoff.to_run
            || self.index.slot != self.handoff.slot
            || self.index.transfer_id != self.handoff.transfer_id
            || self.index.authority_receipt_id != self.receipt_id
        {
            return Err(ResourceError::Integrity {
                code: "resource_handoff_receipt_index_mismatch".to_owned(),
                message: "Resource handoff receipt lost its exact target index binding".to_owned(),
            });
        }
        let expected =
            resource_handoff_receipt_id(&self.command_id, &self.handoff, self.index.target_index)?;
        require_exact_id("Resource handoff receipt", &self.receipt_id, &expected)
    }
}

fn resource_handoff_receipt_id(
    command_id: &str,
    handoff: &ResourceHandoff,
    target_index: u64,
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_COMMAND_RECEIPT_VERSION,
        &("handoff", command_id, handoff, target_index),
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

impl ResourceHandoffActivationReceipt {
    /// Seal activation authority, source receipt, index, and Wait completion.
    ///
    /// # Errors
    ///
    /// Returns an error when any activation, source, index, Wait, or receipt
    /// identity is invalid.
    pub fn new(
        command_id: impl Into<String>,
        activation: ResourceHandoffActivation,
        source_receipt_id: impl Into<String>,
        activation_index: u64,
        coupled_wait_receipt_id: impl Into<String>,
    ) -> ResourceResult<Self> {
        let command_id = command_id.into();
        let source_receipt_id = source_receipt_id.into();
        let coupled_wait_receipt_id = coupled_wait_receipt_id.into();
        let receipt_id = resource_handoff_activation_receipt_id(
            &command_id,
            &activation,
            &source_receipt_id,
            activation_index,
            &coupled_wait_receipt_id,
        )?;
        let receipt = Self {
            receipt_id: receipt_id.clone(),
            command_id,
            index: ResourceHandoffActivationIndexEntry {
                index_version: RESOURCE_HANDOFF_ACTIVATION_INDEX_VERSION.to_owned(),
                to_run: activation.to_run.clone(),
                activation_index,
                transfer_id: activation.transfer_id.clone(),
                activation_id: activation.activation_id.clone(),
                authority_receipt_id: receipt_id,
            },
            activation,
            source_receipt_id,
            coupled_wait_receipt_id,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify every activation authority and receipt edge.
    ///
    /// # Errors
    ///
    /// Returns an error when any activation receipt edge or identity is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_content_id("Resource command", &self.command_id)?;
        self.activation.verify()?;
        validate_content_id("Resource handoff source receipt", &self.source_receipt_id)?;
        validate_content_id(
            "Resource coupled Wait receipt",
            &self.coupled_wait_receipt_id,
        )?;
        self.index.verify()?;
        if self.index.to_run != self.activation.to_run
            || self.index.transfer_id != self.activation.transfer_id
            || self.index.activation_id != self.activation.activation_id
            || self.index.authority_receipt_id != self.receipt_id
        {
            return Err(ResourceError::Integrity {
                code: "resource_handoff_activation_receipt_index_mismatch".to_owned(),
                message: "Resource handoff activation receipt lost its exact target index binding"
                    .to_owned(),
            });
        }
        let expected = resource_handoff_activation_receipt_id(
            &self.command_id,
            &self.activation,
            &self.source_receipt_id,
            self.index.activation_index,
            &self.coupled_wait_receipt_id,
        )?;
        require_exact_id(
            "Resource handoff activation receipt",
            &self.receipt_id,
            &expected,
        )
    }
}

fn resource_handoff_activation_receipt_id(
    command_id: &str,
    activation: &ResourceHandoffActivation,
    source_receipt_id: &str,
    activation_index: u64,
    coupled_wait_receipt_id: &str,
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_COMMAND_RECEIPT_VERSION,
        &(
            "handoff_activation",
            command_id,
            activation,
            source_receipt_id,
            activation_index,
            coupled_wait_receipt_id,
        ),
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

impl ResourceCommandReceipt {
    /// Seal one exact admitted command and its closed outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the command and outcome disagree or the receipt
    /// identity cannot be derived.
    pub fn new(command: ResourceCommand, outcome: ResourceCommandOutcome) -> ResourceResult<Self> {
        verify_command_outcome(&command, &outcome)?;
        let receipt_id =
            cymule_core::content_id(RESOURCE_COMMAND_RECEIPT_VERSION, &(&command, &outcome))
                .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let receipt = Self {
            receipt_version: RESOURCE_COMMAND_RECEIPT_VERSION.to_owned(),
            receipt_id,
            command,
            outcome,
        };
        receipt.verify()?;
        Ok(receipt)
    }

    /// Verify the retained command, outcome, cross-fields, and receipt identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any command, outcome, cross-field, or receipt
    /// identity is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource command receipt",
            &self.receipt_version,
            RESOURCE_COMMAND_RECEIPT_VERSION,
        )?;
        self.command.verify()?;
        verify_command_outcome(&self.command, &self.outcome)?;
        let expected = cymule_core::content_id(
            RESOURCE_COMMAND_RECEIPT_VERSION,
            &(&self.command, &self.outcome),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        require_exact_id("Resource command receipt", &self.receipt_id, &expected)
    }
}

fn verify_command_outcome(
    command: &ResourceCommand,
    outcome: &ResourceCommandOutcome,
) -> ResourceResult<()> {
    command.verify()?;
    match (&command.operation, outcome) {
        (ResourceOperation::Pin { pin }, ResourceCommandOutcome::Pin { receipt }) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id || receipt.pin != *pin {
                return Err(command_outcome_mismatch());
            }
        }
        (
            ResourceOperation::Release {
                release_id,
                pin_id,
                owner,
            },
            ResourceCommandOutcome::Release { receipt },
        ) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id
                || receipt.release_id != *release_id
                || receipt.pin.pin_id != *pin_id
                || receipt.pin.owner != *owner
                || receipt.pin.kind != ResourcePinKind::Explicit
            {
                return Err(command_outcome_mismatch());
            }
        }
        (
            ResourceOperation::GarbageCollect { gc_id, family },
            ResourceCommandOutcome::GarbageCollect { receipt },
        ) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id
                || receipt.gc_id != *gc_id
                || receipt.family != *family
            {
                return Err(command_outcome_mismatch());
            }
        }
        (
            ResourceOperation::BeginDelete {
                delete_id,
                gc_command_id,
                gc_receipt_id,
                target,
            },
            ResourceCommandOutcome::BeginDelete { intent },
        ) => {
            intent.verify()?;
            if intent.command_id != command.command_id
                || intent.delete_id != *delete_id
                || intent.gc_command_id != *gc_command_id
                || intent.gc_receipt_id != *gc_receipt_id
                || intent.target != *target
            {
                return Err(command_outcome_mismatch());
            }
        }
        (
            ResourceOperation::ReconcileDelete {
                delete_id,
                intent_id,
            },
            ResourceCommandOutcome::ReconcileDelete { receipt },
        ) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id
                || receipt.intent.delete_id != *delete_id
                || receipt.intent.intent_id != *intent_id
            {
                return Err(command_outcome_mismatch());
            }
        }
        (ResourceOperation::Transfer { handoff }, ResourceCommandOutcome::Transfer { receipt }) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id || receipt.handoff != *handoff {
                return Err(command_outcome_mismatch());
            }
        }
        (
            ResourceOperation::ActivateTransfer {
                activation,
                source_receipt_id,
            },
            ResourceCommandOutcome::ActivateTransfer { receipt },
        ) => {
            receipt.verify()?;
            if receipt.command_id != command.command_id
                || receipt.activation != *activation
                || receipt.source_receipt_id != *source_receipt_id
            {
                return Err(command_outcome_mismatch());
            }
        }
        _ => return Err(command_outcome_mismatch()),
    }
    Ok(())
}

fn command_outcome_mismatch() -> ResourceError {
    ResourceError::Integrity {
        code: "resource_command_outcome_mismatch".to_owned(),
        message: "Resource command outcome does not match its exact admitted command".to_owned(),
    }
}

impl ResourceLifecycleReceiptRef {
    fn new(locator: ResourceLifecycleReceiptLocator) -> ResourceResult<Self> {
        let reference = Self {
            reference_version: RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION.to_owned(),
            locator,
        };
        reference.verify()?;
        Ok(reference)
    }

    /// Reference one exact Resource-owned lifecycle command receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the outer receipt is malformed or does not
    /// produce a current Resource lifecycle projection.
    pub fn from_resource(receipt: &ResourceCommandReceipt) -> ResourceResult<Self> {
        receipt.verify()?;
        if matches!(
            receipt.outcome,
            ResourceCommandOutcome::GarbageCollect { .. }
                | ResourceCommandOutcome::Transfer { .. }
                | ResourceCommandOutcome::ActivateTransfer { .. }
        ) {
            return Err(ResourceError::Validation(
                "Resource lifecycle reference must select a current-producing receipt".to_owned(),
            ));
        }
        Self::new(ResourceLifecycleReceiptLocator::Resource {
            command_id: receipt.command.command_id.clone(),
            receipt_id: receipt.receipt_id.clone(),
        })
    }

    /// Reference one exact Agent stream-finalization pin receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the Agent receipt is malformed or does not carry
    /// the exact finalized-stream Resource pin owned by the command.
    pub fn from_agent(
        command: &crate::agent::AgentCommand,
        receipt: &crate::agent::AgentCommandReceipt,
    ) -> ResourceResult<Self> {
        let pin = receipt
            .resource_pin_receipt_for(command)
            .map_err(|error| ResourceError::Validation(error.to_string()))?
            .ok_or_else(|| {
                ResourceError::Validation(
                    "Agent lifecycle reference requires an external stream-finalization pin"
                        .to_owned(),
                )
            })?;
        pin.verify()?;
        Self::new(ResourceLifecycleReceiptLocator::Agent {
            command_id: command.command_id.clone(),
            receipt_id: receipt.receipt_id.clone(),
        })
    }

    /// Reference one exact Agent external-stream publication reservation.
    ///
    /// The reservation is embedded in the keyed Agent stream current and is
    /// the only pre-publication authority allowed to hold a reserved Resource
    /// pin. Durable resolves and verifies that current before accepting this
    /// edge as lifecycle authority.
    ///
    /// # Errors
    ///
    /// Returns an error when any exact Agent or reservation identity is invalid.
    pub fn from_agent_publication_reservation(
        command_id: impl Into<String>,
        session_id: impl Into<String>,
        stream_id: impl Into<String>,
        reservation_id: impl Into<String>,
    ) -> ResourceResult<Self> {
        Self::new(
            ResourceLifecycleReceiptLocator::AgentPublicationReservation {
                command_id: command_id.into(),
                session_id: session_id.into(),
                stream_id: stream_id.into(),
                reservation_id: reservation_id.into(),
            },
        )
    }

    /// Reference one exact outer Virtual compaction receipt and archive pin.
    ///
    /// # Errors
    ///
    /// Returns an error unless the complete outer receipt is a verified
    /// compaction whose nested Resource pin exactly retains its certificate's
    /// immutable archive.
    pub fn from_virtual_compaction(
        receipt: &crate::virtual_work::VirtualPersistenceReceipt,
    ) -> ResourceResult<Self> {
        receipt
            .verify()
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let (
            crate::virtual_work::VirtualPersistenceOperation::Compact(command),
            crate::virtual_work::VirtualPersistenceOutcome::Compacted(outcome),
        ) = (&receipt.command.operation, &receipt.outcome)
        else {
            return Err(ResourceError::Validation(
                "Virtual compaction lifecycle reference requires an outer compaction receipt"
                    .to_owned(),
            ));
        };
        if outcome.command != command.command
            || outcome.resource_pin.command_id != command.command.command_id
            || outcome.resource_pin.pin.subject.resource_id
                != outcome.certificate.rehydration_manifest.resource_id
        {
            return Err(ResourceError::Integrity {
                code: "resource_virtual_compaction_pin_mismatch".to_owned(),
                message: "Virtual compaction lifecycle reference changed its nested Resource pin"
                    .to_owned(),
            });
        }
        Self::new(ResourceLifecycleReceiptLocator::Virtual {
            scheduler_id: receipt.command.scheduler_id().to_owned(),
            command_id: receipt.command.command_id().to_owned(),
            outer_receipt_id: receipt.receipt_id.clone(),
        })
    }

    /// Reference one exact outer Virtual archive-retirement receipt and release.
    ///
    /// # Errors
    ///
    /// Returns an error unless the complete outer receipt is a verified
    /// retirement whose nested Resource release exactly owns that command.
    pub fn from_virtual_archive_retirement(
        receipt: &crate::virtual_work::VirtualPersistenceReceipt,
    ) -> ResourceResult<Self> {
        receipt
            .verify()
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let (
            crate::virtual_work::VirtualPersistenceOperation::RetireArchive(command),
            crate::virtual_work::VirtualPersistenceOutcome::ArchiveRetired(outcome),
        ) = (&receipt.command.operation, &receipt.outcome)
        else {
            return Err(ResourceError::Validation(
                "Virtual retirement lifecycle reference requires an outer retirement receipt"
                    .to_owned(),
            ));
        };
        if outcome.command != command.command
            || outcome.resource_release.command_id != command.command.command_id
        {
            return Err(ResourceError::Integrity {
                code: "resource_virtual_retirement_release_mismatch".to_owned(),
                message:
                    "Virtual retirement lifecycle reference changed its nested Resource release"
                        .to_owned(),
            });
        }
        Self::new(ResourceLifecycleReceiptLocator::Virtual {
            scheduler_id: receipt.command.scheduler_id().to_owned(),
            command_id: receipt.command.command_id().to_owned(),
            outer_receipt_id: receipt.receipt_id.clone(),
        })
    }

    /// Return the owning closed profile without introducing a serialized tag
    /// outside the locator union.
    pub const fn profile(&self) -> ResourceLifecycleProfile {
        match self.locator {
            ResourceLifecycleReceiptLocator::Resource { .. } => ResourceLifecycleProfile::Resource,
            ResourceLifecycleReceiptLocator::Agent { .. }
            | ResourceLifecycleReceiptLocator::AgentPublicationReservation { .. } => {
                ResourceLifecycleProfile::Agent
            }
            ResourceLifecycleReceiptLocator::Virtual { .. } => ResourceLifecycleProfile::Virtual,
        }
    }

    /// Borrow the exact owning semantic command identity.
    pub fn command_id(&self) -> &str {
        match &self.locator {
            ResourceLifecycleReceiptLocator::Resource { command_id, .. }
            | ResourceLifecycleReceiptLocator::Agent { command_id, .. }
            | ResourceLifecycleReceiptLocator::AgentPublicationReservation { command_id, .. }
            | ResourceLifecycleReceiptLocator::Virtual { command_id, .. } => command_id,
        }
    }

    /// Borrow the exact outer owning receipt identity.
    pub fn receipt_id(&self) -> &str {
        match &self.locator {
            ResourceLifecycleReceiptLocator::Resource { receipt_id, .. }
            | ResourceLifecycleReceiptLocator::Agent { receipt_id, .. } => receipt_id,
            ResourceLifecycleReceiptLocator::AgentPublicationReservation {
                reservation_id, ..
            } => reservation_id,
            ResourceLifecycleReceiptLocator::Virtual {
                outer_receipt_id, ..
            } => outer_receipt_id,
        }
    }

    /// Borrow the exact Virtual scheduler partition, when this is a Virtual
    /// lifecycle edge.
    pub fn virtual_scheduler_id(&self) -> Option<&str> {
        match &self.locator {
            ResourceLifecycleReceiptLocator::Virtual { scheduler_id, .. } => Some(scheduler_id),
            ResourceLifecycleReceiptLocator::Resource { .. }
            | ResourceLifecycleReceiptLocator::Agent { .. }
            | ResourceLifecycleReceiptLocator::AgentPublicationReservation { .. } => None,
        }
    }

    /// Verify only the bounded local edge shape.
    ///
    /// The Durable `StateRoot` resolver is the sole authority that may claim the
    /// referenced typed command and receipt exist and produced a matching
    /// lifecycle transition.
    ///
    /// # Errors
    ///
    /// Returns an error when the version or selected profile locator is
    /// invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource lifecycle receipt reference",
            &self.reference_version,
            RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION,
        )?;
        match &self.locator {
            ResourceLifecycleReceiptLocator::Resource {
                command_id,
                receipt_id,
            }
            | ResourceLifecycleReceiptLocator::Agent {
                command_id,
                receipt_id,
            } => {
                validate_content_id("Resource lifecycle command", command_id)?;
                validate_content_id("Resource lifecycle receipt", receipt_id)
            }
            ResourceLifecycleReceiptLocator::AgentPublicationReservation {
                command_id,
                session_id,
                stream_id,
                reservation_id,
            } => {
                validate_content_id("Agent publication reservation command", command_id)?;
                validate_identity("Agent publication reservation Session", session_id)?;
                validate_identity("Agent publication reservation stream", stream_id)?;
                validate_content_id("Agent publication reservation", reservation_id)
            }
            ResourceLifecycleReceiptLocator::Virtual {
                scheduler_id,
                command_id,
                outer_receipt_id,
            } => {
                validate_identity("Virtual lifecycle scheduler", scheduler_id)?;
                validate_content_id("Virtual lifecycle command", command_id)?;
                validate_content_id("Virtual outer persistence receipt", outer_receipt_id)
            }
        }
    }
}

/// Deterministically reduce one pin against the exact keyed lifecycle source.
///
/// The caller must first resolve and semantically verify any receipt reference
/// retained by the supplied currents. This pure reducer is shared by Resource,
/// Agent, and Virtual durable lowering so no profile can bypass a deletion
/// fence or invent its own pin-count rule.
///
/// # Errors
///
/// Returns an error when the command, pin, source currents, count, or lifecycle
/// transition is invalid.
pub fn reduce_resource_pin_receipt(
    command_id: &str,
    pin: &ResourcePin,
    retention: Option<&ResourceRetentionCurrent>,
    current_pin: Option<&ResourcePinCurrent>,
) -> ResourceResult<ResourcePinReceipt> {
    validate_content_id("Resource lifecycle command", command_id)?;
    pin.verify()?;
    let active_pin_count = match retention {
        Some(current) => {
            current.verify()?;
            if current.family != pin.subject.family {
                return Err(ResourceError::Integrity {
                    code: "resource_pin_source_family_mismatch".to_owned(),
                    message: "Resource pin source belongs to another physical family".to_owned(),
                });
            }
            match current.disposition {
                ResourceRetentionDisposition::Active => {
                    current.active_pin_count.checked_add(1).ok_or_else(|| {
                        ResourceError::Validation(
                            "Resource active pin count is exhausted".to_owned(),
                        )
                    })?
                }
                ResourceRetentionDisposition::Unretained => 1,
                ResourceRetentionDisposition::DeleteFenced
                | ResourceRetentionDisposition::Deleted => {
                    return Err(ResourceError::Conflict {
                        code: "resource_pin_family_fenced".to_owned(),
                        message: "Resource physical family is fenced against new pins".to_owned(),
                    });
                }
            }
        }
        None => 1,
    };
    if let Some(current) = current_pin {
        current.verify()?;
        if current.pin.pin_id != pin.pin_id {
            return Err(ResourceError::Integrity {
                code: "resource_pin_source_identity_mismatch".to_owned(),
                message: "Resource pin source selects another pin identity".to_owned(),
            });
        }
        return Err(ResourceError::Conflict {
            code: match current.status {
                ResourcePinStatus::Reserved => "resource_pin_publication_reserved",
                ResourcePinStatus::Active => "resource_pin_already_exists",
                ResourcePinStatus::Released => "resource_pin_already_released",
            }
            .to_owned(),
            message: match current.status {
                ResourcePinStatus::Reserved => {
                    "Resource pin is reserved by an in-flight publication"
                }
                ResourcePinStatus::Active => "Resource pin already exists",
                ResourcePinStatus::Released => "Resource pin is terminally released",
            }
            .to_owned(),
        });
    }
    validate_safe_integer("Resource active pin count", active_pin_count)?;
    ResourcePinReceipt::new(command_id.to_owned(), pin.clone(), active_pin_count)
}

/// Deterministically reduce one release against the exact keyed lifecycle source.
///
/// # Errors
///
/// Returns an error when the release does not own one active exact pin or its
/// source retention projection is invalid.
pub fn reduce_resource_release_receipt(
    command_id: &str,
    release_id: &str,
    pin_id: &str,
    owner: &str,
    retention: &ResourceRetentionCurrent,
    current_pin: &ResourcePinCurrent,
) -> ResourceResult<ResourceReleaseReceipt> {
    validate_content_id("Resource lifecycle command", command_id)?;
    validate_identity("Resource release", release_id)?;
    validate_identity("Resource pin", pin_id)?;
    validate_identity("Resource pin owner", owner)?;
    retention.verify()?;
    current_pin.verify()?;
    if current_pin.pin.pin_id != pin_id || current_pin.pin.owner != owner {
        return Err(ResourceError::Conflict {
            code: "resource_release_owner_mismatch".to_owned(),
            message: "Resource release does not own the exact retained pin".to_owned(),
        });
    }
    if current_pin.status != ResourcePinStatus::Active {
        return Err(ResourceError::Conflict {
            code: "resource_pin_already_released".to_owned(),
            message: "Resource pin is already terminally released".to_owned(),
        });
    }
    if retention.family != current_pin.pin.subject.family
        || retention.disposition != ResourceRetentionDisposition::Active
        || retention.active_pin_count == 0
    {
        return Err(ResourceError::Integrity {
            code: "resource_release_source_retention_mismatch".to_owned(),
            message: "Resource release source does not retain its exact physical family".to_owned(),
        });
    }
    let active_pin_count =
        retention
            .active_pin_count
            .checked_sub(1)
            .ok_or_else(|| ResourceError::Integrity {
                code: "resource_pin_count_underflow".to_owned(),
                message: "Resource pin count underflowed".to_owned(),
            })?;
    ResourceReleaseReceipt::new(
        command_id.to_owned(),
        release_id.to_owned(),
        current_pin.pin.clone(),
        active_pin_count,
    )
}

/// Materialize the keyed postcondition for one already-reduced pin receipt.
///
/// # Errors
///
/// Returns an error when the receipt, owner edge, source currents, or resulting
/// projections are invalid.
pub fn project_resource_pin_receipt(
    receipt: &ResourcePinReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: Option<&ResourceRetentionCurrent>,
    current_pin: Option<&ResourcePinCurrent>,
) -> ResourceResult<ResourcePinPostcondition> {
    receipt.verify()?;
    origin.verify()?;
    if origin.command_id() != receipt.command_id
        || origin.profile() != lifecycle_profile_for_pin(&receipt.pin)
    {
        return Err(ResourceError::Integrity {
            code: "resource_pin_receipt_owner_mismatch".to_owned(),
            message: "Resource pin receipt reference changed its owning profile or command"
                .to_owned(),
        });
    }
    let expected =
        reduce_resource_pin_receipt(&receipt.command_id, &receipt.pin, retention, current_pin)?;
    if expected != *receipt {
        return Err(ResourceError::Integrity {
            code: "resource_pin_receipt_source_mismatch".to_owned(),
            message: "Resource pin receipt does not match its exact keyed source".to_owned(),
        });
    }
    let postcondition = ResourcePinPostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: receipt.pin.subject.family.clone(),
            active_pin_count: receipt.active_pin_count,
            disposition: ResourceRetentionDisposition::Active,
            last_receipt: origin.clone(),
        },
        pin: ResourcePinCurrent {
            state_version: RESOURCE_PIN_CURRENT_VERSION.to_owned(),
            pin: receipt.pin.clone(),
            status: ResourcePinStatus::Active,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.pin.verify()?;
    Ok(postcondition)
}

/// Materialize a pre-publication Agent retention reservation.
///
/// This transition counts the exact profile pin before provider I/O while
/// retaining an explicit `Reserved` pin status. It therefore competes with a
/// deletion fence on the same physical-family current without claiming that
/// provider bytes already exist.
///
/// # Errors
///
/// Returns an error when the receipt is not an Agent stream pin, its
/// reservation edge is malformed, or its exact lifecycle source changed.
pub fn project_resource_pin_reservation_receipt(
    receipt: &ResourcePinReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: Option<&ResourceRetentionCurrent>,
    current_pin: Option<&ResourcePinCurrent>,
) -> ResourceResult<ResourcePinPostcondition> {
    receipt.verify()?;
    origin.verify()?;
    if !matches!(
        &origin.locator,
        ResourceLifecycleReceiptLocator::AgentPublicationReservation { .. }
    ) || origin.command_id() != receipt.command_id
        || !matches!(receipt.pin.kind, ResourcePinKind::AgentStream { .. })
    {
        return Err(ResourceError::Integrity {
            code: "resource_pin_reservation_owner_mismatch".to_owned(),
            message: "Resource publication reservation changed its Agent owner".to_owned(),
        });
    }
    let expected =
        reduce_resource_pin_receipt(&receipt.command_id, &receipt.pin, retention, current_pin)?;
    if expected != *receipt {
        return Err(ResourceError::Integrity {
            code: "resource_pin_reservation_source_mismatch".to_owned(),
            message: "Resource publication reservation changed its exact keyed source".to_owned(),
        });
    }
    let postcondition = ResourcePinPostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: receipt.pin.subject.family.clone(),
            active_pin_count: receipt.active_pin_count,
            disposition: ResourceRetentionDisposition::Active,
            last_receipt: origin.clone(),
        },
        pin: ResourcePinCurrent {
            state_version: RESOURCE_PIN_CURRENT_VERSION.to_owned(),
            pin: receipt.pin.clone(),
            status: ResourcePinStatus::Reserved,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.pin.verify()?;
    Ok(postcondition)
}

/// Promote one exact pre-publication Agent reservation to its active permanent
/// pin without changing the physical-family obligation count.
///
/// # Errors
///
/// Returns an error when the reserved pin, family count, reservation receipt,
/// or terminal Agent receipt edge does not close exactly.
pub fn project_resource_reserved_pin_receipt(
    receipt: &ResourcePinReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: &ResourceRetentionCurrent,
    current_pin: &ResourcePinCurrent,
) -> ResourceResult<ResourcePinPostcondition> {
    receipt.verify()?;
    origin.verify()?;
    retention.verify()?;
    current_pin.verify()?;
    let reservation_owner_matches = matches!(
        (&receipt.pin.kind, &current_pin.last_receipt.locator),
        (
            ResourcePinKind::AgentStream {
                session_id,
                stream_id,
            },
            ResourceLifecycleReceiptLocator::AgentPublicationReservation {
                command_id,
                session_id: reserved_session,
                stream_id: reserved_stream,
                ..
            },
        ) if command_id == &receipt.command_id
            && reserved_session == session_id
            && reserved_stream == stream_id
    );
    if origin.command_id() != receipt.command_id
        || origin.profile() != ResourceLifecycleProfile::Agent
        || !matches!(
            &origin.locator,
            ResourceLifecycleReceiptLocator::Agent { .. }
        )
        || current_pin.status != ResourcePinStatus::Reserved
        || current_pin.pin != receipt.pin
        || !reservation_owner_matches
        || retention.family != receipt.pin.subject.family
        || retention.disposition != ResourceRetentionDisposition::Active
        || retention.active_pin_count != receipt.active_pin_count
    {
        return Err(ResourceError::Conflict {
            code: "resource_pin_reservation_promotion_mismatch".to_owned(),
            message: "Resource publication reservation no longer owns the exact physical pin"
                .to_owned(),
        });
    }
    let postcondition = ResourcePinPostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: retention.family.clone(),
            active_pin_count: retention.active_pin_count,
            disposition: ResourceRetentionDisposition::Active,
            last_receipt: origin.clone(),
        },
        pin: ResourcePinCurrent {
            state_version: RESOURCE_PIN_CURRENT_VERSION.to_owned(),
            pin: current_pin.pin.clone(),
            status: ResourcePinStatus::Active,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.pin.verify()?;
    Ok(postcondition)
}

/// Materialize the keyed postcondition for one already-reduced release receipt.
///
/// # Errors
///
/// Returns an error when the receipt, owner edge, source currents, or resulting
/// projections are invalid.
pub fn project_resource_release_receipt(
    receipt: &ResourceReleaseReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: &ResourceRetentionCurrent,
    current_pin: &ResourcePinCurrent,
) -> ResourceResult<ResourcePinPostcondition> {
    receipt.verify()?;
    origin.verify()?;
    if origin.command_id() != receipt.command_id
        || origin.profile() != lifecycle_profile_for_pin(&receipt.pin)
        || matches!(&receipt.pin.kind, ResourcePinKind::AgentStream { .. })
    {
        return Err(ResourceError::Integrity {
            code: "resource_release_receipt_owner_mismatch".to_owned(),
            message: "Resource release receipt reference changed its owning profile or command"
                .to_owned(),
        });
    }
    let expected = reduce_resource_release_receipt(
        &receipt.command_id,
        &receipt.release_id,
        &receipt.pin.pin_id,
        &receipt.pin.owner,
        retention,
        current_pin,
    )?;
    if expected != *receipt {
        return Err(ResourceError::Integrity {
            code: "resource_release_receipt_source_mismatch".to_owned(),
            message: "Resource release receipt does not match its exact keyed source".to_owned(),
        });
    }
    let disposition = if receipt.active_pin_count == 0 {
        ResourceRetentionDisposition::Unretained
    } else {
        ResourceRetentionDisposition::Active
    };
    let postcondition = ResourcePinPostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: receipt.pin.subject.family.clone(),
            active_pin_count: receipt.active_pin_count,
            disposition,
            last_receipt: origin.clone(),
        },
        pin: ResourcePinCurrent {
            state_version: RESOURCE_PIN_CURRENT_VERSION.to_owned(),
            pin: receipt.pin.clone(),
            status: ResourcePinStatus::Released,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.pin.verify()?;
    Ok(postcondition)
}

/// Deterministically evaluate one physical family for garbage collection.
///
/// # Errors
///
/// Returns an error when the command, family, or retained lifecycle source is
/// invalid or terminal.
pub fn reduce_resource_gc_receipt(
    command_id: &str,
    gc_id: &str,
    family: &ResourceRetentionFamily,
    retention: Option<&ResourceRetentionCurrent>,
) -> ResourceResult<ResourceGcReceipt> {
    validate_content_id("Resource lifecycle command", command_id)?;
    validate_identity("Resource GC", gc_id)?;
    family.verify()?;
    let active_pin_count = match retention {
        Some(current) => {
            current.verify()?;
            if current.family != *family {
                return Err(ResourceError::Integrity {
                    code: "resource_gc_source_family_mismatch".to_owned(),
                    message: "Resource GC source belongs to another physical family".to_owned(),
                });
            }
            match current.disposition {
                ResourceRetentionDisposition::Active | ResourceRetentionDisposition::Unretained => {
                    current.active_pin_count
                }
                ResourceRetentionDisposition::DeleteFenced
                | ResourceRetentionDisposition::Deleted => {
                    return Err(ResourceError::Conflict {
                        code: "resource_gc_family_terminal".to_owned(),
                        message: "Resource GC cannot reopen a fenced or deleted family".to_owned(),
                    });
                }
            }
        }
        None => 0,
    };
    ResourceGcReceipt::new(
        command_id.to_owned(),
        gc_id.to_owned(),
        family.clone(),
        active_pin_count,
    )
}

/// Deterministically reduce one eligible GC receipt into a deletion intent.
///
/// # Errors
///
/// Returns an error when GC evidence, the target, or keyed lifecycle sources do
/// not authorize a new deletion fence.
pub fn reduce_resource_begin_delete_intent(
    command_id: &str,
    delete_id: &str,
    gc_receipt: &ResourceGcReceipt,
    target: &ResourceDeletionTarget,
    retention: Option<&ResourceRetentionCurrent>,
    current_delete: Option<&ResourceDeleteCurrent>,
) -> ResourceResult<ResourceDeleteIntent> {
    validate_content_id("Resource lifecycle command", command_id)?;
    validate_identity("Resource delete", delete_id)?;
    gc_receipt.verify()?;
    target.verify()?;
    if gc_receipt.disposition != ResourceGcDisposition::Eligible
        || gc_receipt.active_pin_count != 0
        || gc_receipt.family != target.subject.family
    {
        return Err(ResourceError::Conflict {
            code: "resource_delete_gc_ineligible".to_owned(),
            message:
                "Resource deletion requires an exact eligible GC receipt for its physical family"
                    .to_owned(),
        });
    }
    if let Some(current) = current_delete {
        current.verify()?;
        if current.intent.delete_id != delete_id {
            return Err(ResourceError::Integrity {
                code: "resource_delete_source_identity_mismatch".to_owned(),
                message: "Resource deletion source selects another deletion identity".to_owned(),
            });
        }
        return Err(ResourceError::Conflict {
            code: "resource_delete_already_exists".to_owned(),
            message: "Resource deletion identity already exists".to_owned(),
        });
    }
    if let Some(current) = retention {
        current.verify()?;
        if current.family != target.subject.family {
            return Err(ResourceError::Integrity {
                code: "resource_delete_source_family_mismatch".to_owned(),
                message: "Resource deletion source belongs to another physical family".to_owned(),
            });
        }
        if current.disposition != ResourceRetentionDisposition::Unretained
            || current.active_pin_count != 0
        {
            return Err(ResourceError::Conflict {
                code: "resource_delete_retention_not_unretained".to_owned(),
                message: "Resource deletion requires an unfenced zero-pin physical family"
                    .to_owned(),
            });
        }
    }
    ResourceDeleteIntent::new(
        command_id.to_owned(),
        delete_id.to_owned(),
        gc_receipt.command_id.clone(),
        gc_receipt.receipt_id.clone(),
        target.clone(),
    )
}

/// Materialize the keyed postcondition for one deletion fence.
///
/// # Errors
///
/// Returns an error when the intent, GC evidence, owner edge, sources, or
/// resulting projections are invalid.
pub fn project_resource_begin_delete_intent(
    intent: &ResourceDeleteIntent,
    gc_receipt: &ResourceGcReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: Option<&ResourceRetentionCurrent>,
    current_delete: Option<&ResourceDeleteCurrent>,
) -> ResourceResult<ResourceDeletePostcondition> {
    intent.verify()?;
    origin.verify()?;
    if origin.profile() != ResourceLifecycleProfile::Resource
        || origin.command_id() != intent.command_id
    {
        return Err(ResourceError::Integrity {
            code: "resource_delete_intent_owner_mismatch".to_owned(),
            message: "Resource deletion fence reference changed its owning command".to_owned(),
        });
    }
    let expected = reduce_resource_begin_delete_intent(
        &intent.command_id,
        &intent.delete_id,
        gc_receipt,
        &intent.target,
        retention,
        current_delete,
    )?;
    if expected != *intent {
        return Err(ResourceError::Integrity {
            code: "resource_delete_intent_source_mismatch".to_owned(),
            message: "Resource deletion intent does not match its exact keyed source".to_owned(),
        });
    }
    let postcondition = ResourceDeletePostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: intent.target.subject.family.clone(),
            active_pin_count: 0,
            disposition: ResourceRetentionDisposition::DeleteFenced,
            last_receipt: origin.clone(),
        },
        deletion: ResourceDeleteCurrent {
            state_version: RESOURCE_DELETE_CURRENT_VERSION.to_owned(),
            intent: intent.clone(),
            status: ResourceDeleteStatus::Fenced,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.deletion.verify()?;
    Ok(postcondition)
}

/// Deterministically close one fenced deletion after provider absence readback.
///
/// This pure reducer does not perform provider I/O. Durable may call it only
/// after `ResourceDeleter::delete_and_verify_absent` succeeds for the retained
/// target in the same reconcile operation.
///
/// # Errors
///
/// Returns an error when the command or exact fenced lifecycle source does not
/// authorize terminal deletion evidence.
pub fn reduce_resource_reconcile_delete_receipt(
    command_id: &str,
    delete_id: &str,
    intent_id: &str,
    retention: &ResourceRetentionCurrent,
    current_delete: &ResourceDeleteCurrent,
) -> ResourceResult<ResourceDeleteReceipt> {
    validate_content_id("Resource lifecycle command", command_id)?;
    validate_identity("Resource delete", delete_id)?;
    validate_content_id("Resource delete intent", intent_id)?;
    retention.verify()?;
    current_delete.verify()?;
    if current_delete.status != ResourceDeleteStatus::Fenced
        || current_delete.intent.delete_id != delete_id
        || current_delete.intent.intent_id != intent_id
        || retention.family != current_delete.intent.target.subject.family
        || retention.disposition != ResourceRetentionDisposition::DeleteFenced
        || retention.active_pin_count != 0
    {
        return Err(ResourceError::Conflict {
            code: "resource_delete_reconcile_source_mismatch".to_owned(),
            message: "Resource deletion reconciliation lost its exact fenced source".to_owned(),
        });
    }
    ResourceDeleteReceipt::new(command_id.to_owned(), current_delete.intent.clone())
}

/// Materialize the keyed postcondition for one provider-verified deletion.
///
/// # Errors
///
/// Returns an error when the receipt, owner edge, fenced source, or resulting
/// terminal projections are invalid.
pub fn project_resource_reconcile_delete_receipt(
    receipt: &ResourceDeleteReceipt,
    origin: ResourceLifecycleReceiptRef,
    retention: &ResourceRetentionCurrent,
    current_delete: &ResourceDeleteCurrent,
) -> ResourceResult<ResourceDeletePostcondition> {
    receipt.verify()?;
    origin.verify()?;
    if origin.profile() != ResourceLifecycleProfile::Resource
        || origin.command_id() != receipt.command_id
    {
        return Err(ResourceError::Integrity {
            code: "resource_delete_receipt_owner_mismatch".to_owned(),
            message: "Resource deletion completion reference changed its owning command".to_owned(),
        });
    }
    let expected = reduce_resource_reconcile_delete_receipt(
        &receipt.command_id,
        &receipt.intent.delete_id,
        &receipt.intent.intent_id,
        retention,
        current_delete,
    )?;
    if expected != *receipt {
        return Err(ResourceError::Integrity {
            code: "resource_delete_receipt_source_mismatch".to_owned(),
            message: "Resource deletion receipt does not match its exact fenced source".to_owned(),
        });
    }
    let postcondition = ResourceDeletePostcondition {
        retention: ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: receipt.intent.target.subject.family.clone(),
            active_pin_count: 0,
            disposition: ResourceRetentionDisposition::Deleted,
            last_receipt: origin.clone(),
        },
        deletion: ResourceDeleteCurrent {
            state_version: RESOURCE_DELETE_CURRENT_VERSION.to_owned(),
            intent: receipt.intent.clone(),
            status: ResourceDeleteStatus::Completed,
            last_receipt: origin,
        },
    };
    postcondition.retention.verify()?;
    postcondition.deletion.verify()?;
    Ok(postcondition)
}

fn lifecycle_profile_for_pin(pin: &ResourcePin) -> ResourceLifecycleProfile {
    match &pin.kind {
        ResourcePinKind::Explicit => ResourceLifecycleProfile::Resource,
        ResourcePinKind::AgentStream { .. } => ResourceLifecycleProfile::Agent,
        ResourcePinKind::VirtualArchive { .. } => ResourceLifecycleProfile::Virtual,
    }
}

impl ResourceRetentionCurrent {
    /// Verify one keyed retention projection without consulting unrelated history.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, family, count/disposition relation,
    /// or owning receipt edge is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource retention current",
            &self.state_version,
            RESOURCE_RETENTION_CURRENT_VERSION,
        )?;
        self.family.verify()?;
        validate_safe_integer("Resource active pin count", self.active_pin_count)?;
        self.last_receipt.verify()?;
        if (self.active_pin_count > 0) != (self.disposition == ResourceRetentionDisposition::Active)
        {
            return Err(ResourceError::Integrity {
                code: "resource_retention_disposition_pin_count_mismatch".to_owned(),
                message: "Resource retention disposition does not match its active pin count"
                    .to_owned(),
            });
        }
        if matches!(
            self.disposition,
            ResourceRetentionDisposition::DeleteFenced | ResourceRetentionDisposition::Deleted
        ) && self.last_receipt.profile() != ResourceLifecycleProfile::Resource
        {
            return Err(ResourceError::Integrity {
                code: "resource_retention_delete_receipt_profile_mismatch".to_owned(),
                message: "Resource deletion state must reference a Resource-owned receipt"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

impl ResourcePinCurrent {
    /// Verify one keyed pin projection.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, pin, status, or owning profile edge
    /// is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource pin current",
            &self.state_version,
            RESOURCE_PIN_CURRENT_VERSION,
        )?;
        self.pin.verify()?;
        self.last_receipt.verify()?;
        let expected_profile = match (&self.pin.kind, self.status) {
            (
                ResourcePinKind::Explicit | ResourcePinKind::VirtualArchive { .. },
                ResourcePinStatus::Reserved,
            ) => {
                return Err(ResourceError::Integrity {
                    code: "resource_pin_reservation_profile_mismatch".to_owned(),
                    message: "Only Agent external publication may reserve a Resource pin"
                        .to_owned(),
                });
            }
            (ResourcePinKind::Explicit, _) => ResourceLifecycleProfile::Resource,
            (ResourcePinKind::AgentStream { .. }, ResourcePinStatus::Reserved) => {
                if !matches!(
                    &self.last_receipt.locator,
                    ResourceLifecycleReceiptLocator::AgentPublicationReservation { .. }
                ) {
                    return Err(ResourceError::Integrity {
                        code: "resource_agent_stream_reservation_origin_mismatch".to_owned(),
                        message: "Reserved Agent stream pin requires its publication reservation"
                            .to_owned(),
                    });
                }
                ResourceLifecycleProfile::Agent
            }
            (ResourcePinKind::AgentStream { .. }, ResourcePinStatus::Active) => {
                ResourceLifecycleProfile::Agent
            }
            (ResourcePinKind::VirtualArchive { .. }, _) => ResourceLifecycleProfile::Virtual,
            (ResourcePinKind::AgentStream { .. }, ResourcePinStatus::Released) => {
                return Err(ResourceError::Integrity {
                    code: "resource_agent_stream_pin_released".to_owned(),
                    message: "Agent stream pins have no owning retirement transition".to_owned(),
                });
            }
        };
        if self.last_receipt.profile() != expected_profile {
            return Err(ResourceError::Integrity {
                code: "resource_pin_current_profile_mismatch".to_owned(),
                message: "Resource pin current references the wrong owning profile".to_owned(),
            });
        }
        Ok(())
    }
}

impl ResourceDeleteCurrent {
    /// Verify one keyed deletion projection and its bounded receipt edge.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, intent, status, or owning Resource
    /// receipt edge is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(
            "Resource delete current",
            &self.state_version,
            RESOURCE_DELETE_CURRENT_VERSION,
        )?;
        self.intent.verify()?;
        self.last_receipt.verify()?;
        if self.last_receipt.profile() != ResourceLifecycleProfile::Resource {
            return Err(ResourceError::Integrity {
                code: "resource_delete_current_profile_mismatch".to_owned(),
                message: "Resource deletion current references a non-Resource profile receipt"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

impl ResourceHandoffCurrent {
    /// Verify one keyed current transfer authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained transfer receipt is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        self.receipt.verify()
    }
}

impl ResourceHandoffActivationCurrent {
    /// Verify one keyed current activation authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the retained activation receipt is invalid.
    pub fn verify(&self) -> ResourceResult<()> {
        self.receipt.verify()
    }
}

fn resource_id(
    resource_version: &str,
    shape: ResourceShape,
    media_type: &str,
    inline: Option<&InlineData>,
    integrity: &ResourceIntegrity,
    manifest: Option<&ResourceManifestDescriptor>,
    annotations: &BTreeMap<String, String>,
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_VERSION,
        &ResourceIdentity {
            resource_version,
            shape,
            media_type,
            inline,
            integrity,
            manifest,
            annotations,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_resource_fields(
    resource_version: &str,
    shape: ResourceShape,
    media_type: &str,
    inline: Option<&InlineData>,
    integrity: &ResourceIntegrity,
    manifest: Option<&ResourceManifestDescriptor>,
    annotations: &BTreeMap<String, String>,
) -> ResourceResult<()> {
    require_version("Resource", resource_version, RESOURCE_VERSION)?;
    validate_resource_media_type(media_type)?;
    if annotations.len() > MAX_RESOURCE_ANNOTATIONS {
        return Err(ResourceError::Validation(format!(
            "Resource descriptor exceeds {MAX_RESOURCE_ANNOTATIONS} annotations"
        )));
    }
    match (shape, inline, integrity) {
        (ResourceShape::Inline, Some(data), ResourceIntegrity::Inline) => {
            data.bytes()?;
            if manifest.is_some() {
                return Err(ResourceError::Validation(
                    "inline Resource cannot have a collection manifest".to_owned(),
                ));
            }
        }
        (ResourceShape::Inline, _, _) => {
            return Err(ResourceError::Validation(
                "inline Resource requires retained data and inline integrity".to_owned(),
            ));
        }
        (_, None, ResourceIntegrity::Content { digest, size }) => {
            validate_digest("Resource content", digest)?;
            validate_safe_integer("Resource content byte size", *size)?;
        }
        (_, None, ResourceIntegrity::Version { authority, version }) => {
            validate_identity("Resource version authority", authority)?;
            validate_identity("Resource immutable version", version)?;
        }
        (_, None, ResourceIntegrity::Live { identity }) => {
            validate_identity("live Resource", identity)?;
        }
        (_, Some(_), _) | (_, None, ResourceIntegrity::Inline) => {
            return Err(ResourceError::Validation(
                "external Resource cannot retain inline data or inline integrity".to_owned(),
            ));
        }
    }
    if let Some(manifest) = manifest {
        if !matches!(
            shape,
            ResourceShape::Collection | ResourceShape::Directory | ResourceShape::Snapshot
        ) {
            return Err(ResourceError::Validation(
                "only collection, directory, or snapshot Resources have manifests".to_owned(),
            ));
        }
        manifest.verify()?;
        match integrity {
            ResourceIntegrity::Content { digest, size }
                if digest == &manifest.digest && size == &manifest.size => {}
            _ => {
                return Err(ResourceError::Validation(
                    "a Resource manifest must own content integrity through its descriptor ID and size"
                        .to_owned(),
                ));
            }
        }
    }
    for (key, value) in annotations {
        validate_extended_identity("Resource annotation key", key)?;
        if value.chars().count() > 4096 || value.chars().any(char::is_control) {
            return Err(ResourceError::Validation(
                "Resource annotation value must contain at most 4096 non-control characters"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

/// Validate the sole lowercase ASCII Resource media-type grammar.
///
/// Both type and subtype are non-empty RFC `token` subsets separated by one
/// slash. Parameters and every byte outside lowercase ASCII `tchar` are not
/// part of Resource identity.
///
/// # Errors
///
/// Returns an error when the media type is not the exact canonical wire.
pub fn validate_resource_media_type(media_type: &str) -> ResourceResult<()> {
    let mut parts = media_type.split('/');
    let media_type_token = parts.next().unwrap_or_default();
    let media_subtype_token = parts.next().unwrap_or_default();
    if media_type.len() > 255
        || media_type_token.is_empty()
        || media_subtype_token.is_empty()
        || parts.next().is_some()
        || !media_type_token
            .bytes()
            .all(is_resource_media_type_token_byte)
        || !media_subtype_token
            .bytes()
            .all(is_resource_media_type_token_byte)
    {
        return Err(ResourceError::Validation(
            "Resource media type is invalid".to_owned(),
        ));
    }
    Ok(())
}

const fn is_resource_media_type_token_byte(byte: u8) -> bool {
    matches!(
        byte,
        b'a'..=b'z'
            | b'0'..=b'9'
            | b'!'
            | b'#'
            | b'$'
            | b'%'
            | b'&'
            | b'\''
            | b'*'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~'
    )
}

fn validate_location(location: &ResourceLocation) -> ResourceResult<()> {
    match location {
        ResourceLocation::PublicUrl { url } => {
            if url.len() > MAX_RESOURCE_PUBLIC_URL_BYTES
                || url.chars().count() > MAX_RESOURCE_PUBLIC_URL_SCALARS
                || !url.is_ascii()
            {
                return Err(ResourceError::Validation(format!(
                    "public Resource URL must be canonical ASCII within {MAX_RESOURCE_PUBLIC_URL_SCALARS} scalars and {MAX_RESOURCE_PUBLIC_URL_BYTES} bytes"
                )));
            }
            let parsed = Url::parse(url).map_err(|error| {
                ResourceError::Validation(format!("invalid public Resource URL: {error}"))
            })?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || parsed.as_str() != url
                || !has_canonical_percent_encoding(url)
            {
                return Err(ResourceError::Validation(
                    "public Resource URL must be canonical credential-free HTTP(S) without query or fragment"
                        .to_owned(),
                ));
            }
        }
        ResourceLocation::Opaque { reference } => {
            validate_identity("Resource resolver reference", reference)?;
        }
    }
    Ok(())
}

fn has_canonical_percent_encoding(url: &str) -> bool {
    let bytes = url.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        let Some(encoded) = bytes.get(index + 1..index + 3) else {
            return false;
        };
        if !encoded
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'A'..=b'F').contains(byte))
        {
            return false;
        }
        let high = if encoded[0].is_ascii_digit() {
            encoded[0] - b'0'
        } else {
            encoded[0] - b'A' + 10
        };
        let low = if encoded[1].is_ascii_digit() {
            encoded[1] - b'0'
        } else {
            encoded[1] - b'A' + 10
        };
        let decoded = (high << 4) | low;
        if decoded.is_ascii_alphanumeric() || matches!(decoded, b'-' | b'.' | b'_' | b'~') {
            return false;
        }
        index += 3;
    }
    true
}

fn validate_canonical_size(kind: &str, value: &impl Serialize, maximum: u64) -> ResourceResult<()> {
    let bytes = cymule_core::canonical_bytes(value)
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
    let size = u64::try_from(bytes.len())
        .map_err(|_| ResourceError::Validation(format!("{kind} exceeds platform size bounds")))?;
    if size > maximum {
        return Err(ResourceError::Validation(format!(
            "{kind} exceeds {maximum} canonical JSON bytes"
        )));
    }
    Ok(())
}

fn require_version(kind: &str, actual: &str, expected: &str) -> ResourceResult<()> {
    if actual != expected {
        return Err(ResourceError::Validation(format!(
            "unsupported {kind} version {actual:?}; expected {expected}"
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> ResourceResult<()> {
    cymule_core::validate_identity(kind, value)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_extended_identity(kind: &str, value: &str) -> ResourceResult<()> {
    let scalar_count = value.chars().count();
    if scalar_count == 0 || scalar_count > 2048 || value.chars().any(char::is_control) {
        return Err(ResourceError::Validation(format!(
            "{kind} must contain 1..=2048 non-control characters"
        )));
    }
    Ok(())
}

fn validate_content_id(kind: &str, value: &str) -> ResourceResult<()> {
    cymule_core::validate_content_id(kind, value)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn require_exact_id(kind: &str, actual: &str, expected: &str) -> ResourceResult<()> {
    if actual != expected {
        return Err(ResourceError::Integrity {
            code: "resource_exact_identity_mismatch".to_owned(),
            message: format!("{kind} {actual} does not match {expected}"),
        });
    }
    Ok(())
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

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object_candidate() -> ResourceCandidate {
        ResourceCandidate {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{}", "a".repeat(64)),
                size: 1,
            },
            manifest: None,
            annotations: BTreeMap::new(),
        }
    }

    fn publication(bytes: &[u8]) -> ResourcePublication {
        let digest = format!("sha256:{}", cymule_core::sha256_bytes(bytes));
        let resource = ResourceCandidate {
            resource_version: RESOURCE_VERSION.to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: digest.clone(),
                size: u64::try_from(bytes.len()).expect("fixture length fits"),
            },
            manifest: None,
            annotations: BTreeMap::new(),
        }
        .seal()
        .expect("fixture Resource seals");
        let publication = ResourcePublication {
            locators: ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: "fixture:store".to_owned(),
                locations: vec![ResourceLocation::Opaque { reference: digest }],
            },
            resource,
        };
        publication.verify().expect("fixture publication verifies");
        publication
    }

    #[test]
    fn resource_media_type_grammar_is_closed_and_predecessor_is_rejected() {
        for media_type in [
            "text/plain",
            "application/json",
            "application/vnd.cymule.resource+json",
            "a!#$%&'*+.^_`|~-/b!#$%&'*+.^_`|~-",
        ] {
            validate_resource_media_type(media_type)
                .unwrap_or_else(|error| panic!("valid media type {media_type:?} failed: {error}"));
        }
        for media_type in [
            "text/\0plain",
            "text/",
            "/plain",
            "a/b/c",
            "Text/plain",
            "text/Plain",
            "text/plain;charset=utf-8",
            "text/ plain",
            "text/\u{7f}plain",
            "text\\plain",
        ] {
            assert!(
                validate_resource_media_type(media_type).is_err(),
                "invalid media type {media_type:?} was accepted"
            );
        }

        let schema = resource_handle_artifact_schema();
        assert_eq!(
            schema["properties"]["media_type"]["pattern"],
            RESOURCE_MEDIA_TYPE_PATTERN
        );

        let mut predecessor = ResourceCandidate::text("legacy");
        predecessor.resource_version = "cymule.resource/3".to_owned();
        assert!(matches!(
            predecessor.validate(),
            Err(ResourceError::Validation(message)) if message.contains("version")
        ));
    }

    #[test]
    fn resource_annotation_count_and_canonical_size_are_closed() {
        let mut candidate = object_candidate();
        for index in 0..MAX_RESOURCE_ANNOTATIONS {
            let prefix = format!("{index:02}");
            let key = format!(
                "{prefix}{}",
                "\\".repeat(2048_usize.saturating_sub(prefix.chars().count()))
            );
            candidate.annotations.insert(key, "🧪".repeat(4096));
        }
        candidate
            .validate()
            .expect("maximum escaped and multibyte annotations verify");
        assert!(
            u64::try_from(
                cymule_core::canonical_bytes(&candidate)
                    .expect("maximum descriptor encodes")
                    .len()
            )
            .expect("descriptor length fits")
                <= MAX_RESOURCE_DESCRIPTOR_BYTES
        );

        candidate
            .annotations
            .insert("annotation:overflow".to_owned(), String::new());
        assert!(matches!(
            candidate.validate(),
            Err(ResourceError::Validation(message)) if message.contains("annotations")
        ));

        let mut oversized_value = object_candidate();
        oversized_value
            .annotations
            .insert("annotation:value".to_owned(), "🧪".repeat(4097));
        assert!(matches!(
            oversized_value.validate(),
            Err(ResourceError::Validation(message)) if message.contains("4096")
        ));
    }

    #[test]
    fn locator_count_and_canonical_ascii_url_bounds_are_closed() {
        let resource = object_candidate().seal().expect("fixture Resource seals");
        let locations = (0..MAX_RESOURCE_LOCATIONS)
            .map(|index| {
                let prefix = format!("https://example.com/{index:02}/");
                ResourceLocation::PublicUrl {
                    url: format!(
                        "{prefix}{}",
                        "a".repeat(MAX_RESOURCE_PUBLIC_URL_BYTES - prefix.len())
                    ),
                }
            })
            .collect::<Vec<_>>();
        let maximum = ResourceLocatorSet {
            locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: "resolver:maximum".to_owned(),
            locations,
        };
        maximum
            .verify_for(&resource)
            .expect("maximum canonical URL locator set verifies");
        assert!(
            u64::try_from(
                cymule_core::canonical_bytes(&maximum)
                    .expect("maximum locator set encodes")
                    .len()
            )
            .expect("locator-set length fits")
                <= MAX_RESOURCE_LOCATOR_SET_BYTES
        );
        let mut legacy = maximum.clone();
        legacy.locator_version = "cymule.resource-locators/1".to_owned();
        assert!(matches!(
            legacy.verify_for(&resource),
            Err(ResourceError::Validation(message)) if message.contains("version")
        ));
        ResourceLocatorSet {
            locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: "resolver:escaped".to_owned(),
            locations: vec![ResourceLocation::PublicUrl {
                url: "https://example.com/a%2Fb".to_owned(),
            }],
        }
        .verify_for(&resource)
        .expect("uppercase escaped reserved byte is canonical");

        let mut too_many = maximum.clone();
        too_many.locations.push(ResourceLocation::PublicUrl {
            url: "https://overflow.example/".to_owned(),
        });
        assert!(matches!(
            too_many.verify_for(&resource),
            Err(ResourceError::Validation(message)) if message.contains("locations")
        ));

        for url in [
            format!(
                "https://example.com/{}",
                "a".repeat(MAX_RESOURCE_PUBLIC_URL_BYTES)
            ),
            "https://example.com/多字节".to_owned(),
            "https://example.com".to_owned(),
            "https://example.com/%61".to_owned(),
            "https://example.com/%2f".to_owned(),
        ] {
            let invalid = ResourceLocatorSet {
                locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
                resource_id: resource.resource_id.clone(),
                resolver_binding: "resolver:invalid".to_owned(),
                locations: vec![ResourceLocation::PublicUrl { url }],
            };
            assert!(matches!(
                invalid.verify_for(&resource),
                Err(ResourceError::Validation(_))
            ));
        }
    }

    #[test]
    fn catalog_record_accepts_the_largest_zero_payload_and_rejects_one_more_byte() {
        let namespace = "\\".repeat(2048);
        let key = "🧪".repeat(2048);
        let empty = ResourceCatalogRecord::new(&namespace, &key, Vec::new())
            .expect("empty worst-case catalog record seals");
        let mut legacy = empty.clone();
        legacy.record_version = "cymule.resource-catalog-record/1".to_owned();
        assert!(matches!(
            legacy.verify(),
            Err(ResourceError::Validation(message)) if message.contains("version")
        ));
        let empty_size = u64::try_from(
            cymule_core::canonical_bytes(&empty)
                .expect("empty catalog record encodes")
                .len(),
        )
        .expect("empty record size fits");
        let payload_len =
            usize::try_from((MAX_RESOURCE_CATALOG_RECORD_BYTES - empty_size).div_ceil(2))
                .expect("maximum payload length fits");
        let maximum = ResourceCatalogRecord::new(&namespace, &key, vec![0; payload_len])
            .expect("largest zero payload within the canonical bound seals");
        let maximum_size = u64::try_from(
            cymule_core::canonical_bytes(&maximum)
                .expect("maximum catalog record encodes")
                .len(),
        )
        .expect("maximum record size fits");
        assert!(maximum_size <= MAX_RESOURCE_CATALOG_RECORD_BYTES);
        assert!(matches!(
            ResourceCatalogRecord::new(&namespace, &key, vec![0; payload_len + 1]),
            Err(ResourceError::Validation(message)) if message.contains("canonical JSON bytes")
        ));
    }

    #[test]
    fn archive_pin_binds_archive_identity_to_exact_subject() {
        let first = publication(b"first");
        let second = publication(b"second");
        let subject =
            ResourceRetentionSubject::from_publication(&second).expect("second subject derives");
        let archive_id = first.resource.resource_id.clone();
        let forged = ResourcePin {
            pin_id: resource_archive_pin_id(&archive_id).expect("archive pin derives"),
            subject,
            owner: "virtual:archive".to_owned(),
            kind: ResourcePinKind::VirtualArchive { archive_id },
        };
        assert!(matches!(
            forged.verify(),
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_archive_pin_subject_mismatch"
        ));
    }

    #[test]
    fn archive_release_rejects_a_different_archive_pin() {
        let first = publication(b"first");
        let second = publication(b"second");
        let release = ResourceArchiveRelease {
            release_version: RESOURCE_ARCHIVE_RELEASE_VERSION.to_owned(),
            release_id: cymule_core::content_id("test.virtual-retirement/1", &"retire")
                .expect("retirement ID derives"),
            archive_id: first.resource.resource_id.clone(),
            pin_id: resource_archive_pin_id(&second.resource.resource_id)
                .expect("other archive pin derives"),
        };
        assert!(matches!(
            release.verify(),
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_exact_identity_mismatch"
        ));
    }

    #[test]
    fn agent_stream_pin_uses_its_own_frozen_identity_domain() {
        let publication = publication(b"agent");
        let subject = ResourceRetentionSubject::from_publication(&publication)
            .expect("Agent subject derives");
        let kind = ResourcePinKind::AgentStream {
            session_id: "session:one".to_owned(),
            stream_id: "stream:one".to_owned(),
        };
        let pin =
            ResourcePin::profile(subject.clone(), kind.clone()).expect("Agent profile pin seals");
        let owner = resource_agent_stream_pin_owner("session:one", "stream:one")
            .expect("Agent pin owner derives");
        let expected = cymule_core::content_id(
            RESOURCE_AGENT_STREAM_PIN_VERSION,
            &(
                "pin",
                ResourcePinIdentity {
                    subject: &subject,
                    owner: &owner,
                    kind: &kind,
                },
            ),
        )
        .expect("Agent pin ID derives");
        assert_eq!(pin.owner, owner);
        assert_eq!(pin.pin_id, expected);
        assert_ne!(
            pin.pin_id,
            cymule_core::content_id(
                RESOURCE_PIN_CURRENT_VERSION,
                &ResourcePinIdentity {
                    subject: &subject,
                    owner: &pin.owner,
                    kind: &kind,
                },
            )
            .expect("projection-domain control derives")
        );
    }

    #[test]
    fn profile_pin_owner_is_derived_and_cannot_be_replaced() {
        let publication = publication(b"owner");
        let subject = ResourceRetentionSubject::from_publication(&publication)
            .expect("profile subject derives");
        let archive_id = publication.resource.resource_id.clone();
        let archive = ResourcePin::profile(
            subject.clone(),
            ResourcePinKind::VirtualArchive { archive_id },
        )
        .expect("archive pin seals");
        let mut changed_archive = archive;
        changed_archive.owner = "virtual:other-owner".to_owned();
        assert!(matches!(
            changed_archive.verify(),
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_archive_pin_owner_mismatch"
        ));

        let agent = ResourcePin::profile(
            subject,
            ResourcePinKind::AgentStream {
                session_id: "session:owner".to_owned(),
                stream_id: "stream:owner".to_owned(),
            },
        )
        .expect("Agent pin seals");
        let mut changed_agent = agent;
        changed_agent.owner = "agent:other-owner".to_owned();
        assert!(matches!(
            changed_agent.verify(),
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_agent_stream_pin_owner_mismatch"
        ));
    }

    #[test]
    fn explicit_non_content_pin_has_one_releasable_command_identity() {
        let publication = publication(b"explicit");
        let subject = ResourceRetentionSubject::from_publication(&publication)
            .expect("explicit subject derives");
        let pin = ResourcePin::explicit("pin:run-output", subject, "run:consumer")
            .expect("explicit pin seals");
        let command = ResourceCommand::new(ResourceOperation::Release {
            release_id: "release:run-output".to_owned(),
            pin_id: pin.pin_id,
            owner: pin.owner,
        })
        .expect("explicit release command seals");
        command.verify().expect("explicit release command verifies");
    }

    #[test]
    fn shared_pin_reducer_rejects_every_profile_after_delete_fence() {
        let publication = publication(b"fenced");
        let subject = ResourceRetentionSubject::from_publication(&publication)
            .expect("fenced subject derives");
        let pin = ResourcePin::explicit("pin:fenced", subject.clone(), "run:late")
            .expect("late pin seals independently");
        let command_id = cymule_core::content_id("test.resource-command/1", &"late-pin")
            .expect("command ID derives");
        let current = ResourceRetentionCurrent {
            state_version: RESOURCE_RETENTION_CURRENT_VERSION.to_owned(),
            family: subject.family,
            active_pin_count: 0,
            disposition: ResourceRetentionDisposition::DeleteFenced,
            last_receipt: ResourceLifecycleReceiptRef::new(
                ResourceLifecycleReceiptLocator::Resource {
                    command_id: cymule_core::content_id("test.resource-command/1", &"fence")
                        .expect("fence command ID derives"),
                    receipt_id: cymule_core::content_id("test.resource-receipt/1", &"fence")
                        .expect("fence receipt ID derives"),
                },
            )
            .expect("fence edge seals"),
        };
        current.verify().expect("fenced current verifies locally");
        assert!(matches!(
            reduce_resource_pin_receipt(&command_id, &pin, Some(&current), None),
            Err(ResourceError::Conflict { code, .. }) if code == "resource_pin_family_fenced"
        ));
    }

    #[test]
    fn lifecycle_reference_uses_one_closed_profile_specific_locator() {
        let command_id = cymule_core::content_id("test.virtual-command/1", &"compact")
            .expect("Virtual command identity derives");
        let outer_receipt_id = cymule_core::content_id("test.virtual-receipt/1", &"compact")
            .expect("Virtual receipt identity derives");
        let reference =
            ResourceLifecycleReceiptRef::new(ResourceLifecycleReceiptLocator::Virtual {
                scheduler_id: "scheduler:locator".to_owned(),
                command_id: command_id.clone(),
                outer_receipt_id: outer_receipt_id.clone(),
            })
            .expect("Virtual lifecycle reference seals");
        assert_eq!(reference.profile(), ResourceLifecycleProfile::Virtual);
        assert_eq!(reference.command_id(), command_id);
        assert_eq!(reference.receipt_id(), outer_receipt_id);
        assert_eq!(reference.virtual_scheduler_id(), Some("scheduler:locator"));

        let wire = serde_json::to_value(&reference).expect("lifecycle reference serializes");
        assert_eq!(wire["locator"]["profile"], "virtual");
        assert_eq!(wire["locator"]["scheduler_id"], "scheduler:locator");
        assert!(wire["locator"].get("receipt_id").is_none());
        serde_json::from_value::<ResourceLifecycleReceiptRef>(wire.clone())
            .expect("closed Virtual locator round-trips");

        for missing in ["scheduler_id", "outer_receipt_id"] {
            let mut malformed = wire.clone();
            malformed["locator"]
                .as_object_mut()
                .expect("locator is an object")
                .remove(missing);
            assert!(serde_json::from_value::<ResourceLifecycleReceiptRef>(malformed).is_err());
        }

        let resource_with_fake_partition = serde_json::json!({
            "reference_version": RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION,
            "locator": {
                "profile": "resource",
                "command_id": command_id,
                "receipt_id": outer_receipt_id,
                "scheduler_id": "scheduler:forbidden"
            }
        });
        assert!(
            serde_json::from_value::<ResourceLifecycleReceiptRef>(resource_with_fake_partition)
                .is_err()
        );
    }
}
