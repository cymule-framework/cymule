use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::{ResourceError, ResourceResult};

/// Frozen resource descriptor version.
pub const RESOURCE_VERSION: &str = "cymule.resource/1";
/// Maximum decoded inline payload size.
pub const INLINE_RESOURCE_LIMIT: usize = 1024 * 1024;

/// Logical resource shape independent of a storage provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceShape {
    /// Small value retained directly in the descriptor.
    Inline,
    /// One opaque byte object or file.
    Object,
    /// An unordered or application-defined group of resources.
    Collection,
    /// A hierarchical directory manifest.
    Directory,
    /// A sandbox, workspace, volume, or environment snapshot.
    Snapshot,
}

/// Inline payload encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "encoding", rename_all = "snake_case")]
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
        /// RFC 4648 standard-alphabet base64 value.
        data: String,
    },
}

impl InlineData {
    /// Decode the retained payload into bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when base64 is invalid, JSON cannot be encoded, or the
    /// decoded payload exceeds the inline limit.
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
                "inline resource exceeds {INLINE_RESOURCE_LIMIT} bytes"
            )));
        }
        Ok(bytes)
    }
}

/// Evidence available for replaying one resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceIntegrity {
    /// The complete value is retained directly in the descriptor.
    Inline,
    /// Retrieved bytes must match this digest and size.
    Content {
        /// Lowercase SHA-256 content digest.
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
    /// The resource is intentionally mutable and live-only.
    Live {
        /// Stable logical identity for this live resource.
        identity: String,
    },
}

/// Non-authoritative realization hint for locating resource bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResourceLocation {
    /// Credential-free public HTTP(S) URL.
    PublicUrl {
        /// Public URL without userinfo, query, or fragment.
        url: String,
    },
    /// Opaque reference interpreted only by a pinned resolver binding.
    Resolver {
        /// Immutable resolver implementation binding.
        binding: String,
        /// Opaque non-secret reference meaningful to that resolver.
        reference: String,
    },
}

/// Candidate resource descriptor before trusted Rust identity sealing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCandidate {
    /// Resource wire version.
    pub resource_version: String,
    /// Logical shape.
    pub shape: ResourceShape,
    /// IANA-style media type or stable application media type.
    pub media_type: String,
    /// Retained value for `inline` resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<InlineData>,
    /// Replay/integrity evidence.
    pub integrity: ResourceIntegrity,
    /// Replaceable, non-authoritative realization hints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<ResourceLocation>,
    /// Semantic user/application metadata included in resource identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Trusted resource descriptor with a location-independent identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceHandle {
    /// Content/semantic identity computed by the Rust resource sealer.
    pub resource_id: String,
    /// Resource wire version.
    pub resource_version: String,
    /// Logical shape.
    pub shape: ResourceShape,
    /// IANA-style media type or stable application media type.
    pub media_type: String,
    /// Retained value for `inline` resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<InlineData>,
    /// Replay/integrity evidence.
    pub integrity: ResourceIntegrity,
    /// Replaceable, non-authoritative realization hints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<ResourceLocation>,
    /// Semantic user/application metadata included in resource identity.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct ResourceIdentity<'a> {
    resource_version: &'a str,
    shape: ResourceShape,
    media_type: &'a str,
    inline: Option<&'a InlineData>,
    integrity: &'a ResourceIntegrity,
    annotations: &'a BTreeMap<String, String>,
}

/// Replay availability implied by retained resource evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceReplayClass {
    /// Inline bytes are retained or fetched bytes are content-verifiable.
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
            media_type: "text/plain;charset=utf-8".to_owned(),
            inline: Some(InlineData::Utf8 { text: text.into() }),
            integrity: ResourceIntegrity::Inline,
            locations: Vec::new(),
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
            locations: Vec::new(),
            annotations: BTreeMap::new(),
        }
    }

    /// Validate and compute the location-independent Resource ID.
    ///
    /// # Errors
    ///
    /// Returns an error when shape, payload, evidence, locations, media type,
    /// annotations, or credential-safety constraints are invalid.
    pub fn seal(self) -> ResourceResult<ResourceHandle> {
        self.validate()?;
        let resource_id = resource_id(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            &self.annotations,
        )?;
        Ok(ResourceHandle {
            resource_id,
            resource_version: self.resource_version,
            shape: self.shape,
            media_type: self.media_type,
            inline: self.inline,
            integrity: self.integrity,
            locations: self.locations,
            annotations: self.annotations,
        })
    }

    /// Validate a candidate without computing its identity.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported versions or invalid descriptor fields.
    pub fn validate(&self) -> ResourceResult<()> {
        validate_fields(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            &self.locations,
            &self.annotations,
        )
    }
}

impl ResourceHandle {
    /// Verify every field and recompute the trusted Resource ID.
    ///
    /// # Errors
    ///
    /// Returns an error when descriptor semantics or identity do not match.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_fields(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            &self.locations,
            &self.annotations,
        )?;
        let expected = resource_id(
            &self.resource_version,
            self.shape,
            &self.media_type,
            self.inline.as_ref(),
            &self.integrity,
            &self.annotations,
        )?;
        if expected != self.resource_id {
            return Err(ResourceError::Integrity(format!(
                "resource ID {} does not match {expected}",
                self.resource_id
            )));
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

fn resource_id(
    resource_version: &str,
    shape: ResourceShape,
    media_type: &str,
    inline: Option<&InlineData>,
    integrity: &ResourceIntegrity,
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
            annotations,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_fields(
    resource_version: &str,
    shape: ResourceShape,
    media_type: &str,
    inline: Option<&InlineData>,
    integrity: &ResourceIntegrity,
    locations: &[ResourceLocation],
    annotations: &BTreeMap<String, String>,
) -> ResourceResult<()> {
    if resource_version != RESOURCE_VERSION {
        return Err(ResourceError::Validation(format!(
            "unsupported resource version {resource_version:?}"
        )));
    }
    validate_media_type(media_type)?;
    match (shape, inline, integrity) {
        (ResourceShape::Inline, Some(data), ResourceIntegrity::Inline) => {
            data.bytes()?;
            if !locations.is_empty() {
                return Err(ResourceError::Validation(
                    "inline resource cannot have external locations".to_owned(),
                ));
            }
        }
        (ResourceShape::Inline, _, _) => {
            return Err(ResourceError::Validation(
                "inline resource requires retained data and inline integrity".to_owned(),
            ));
        }
        (_, None, ResourceIntegrity::Content { digest, .. }) => validate_digest(digest)?,
        (_, None, ResourceIntegrity::Version { authority, version }) => {
            validate_token("version authority", authority)?;
            validate_token("immutable version", version)?;
        }
        (_, None, ResourceIntegrity::Live { identity }) => {
            validate_token("live resource identity", identity)?;
        }
        (_, Some(_), _) | (_, None, ResourceIntegrity::Inline) => {
            return Err(ResourceError::Validation(
                "external resource cannot retain inline data or inline integrity".to_owned(),
            ));
        }
    }
    if shape != ResourceShape::Inline && locations.is_empty() {
        return Err(ResourceError::Validation(
            "external resource requires at least one resolver location".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    for location in locations {
        validate_location(location)?;
        let identity = cymule_core::canonical_digest(location)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if !seen.insert(identity) {
            return Err(ResourceError::Validation(
                "resource repeats an identical location".to_owned(),
            ));
        }
    }
    for (key, value) in annotations {
        validate_token("annotation key", key)?;
        if value.len() > 4096 {
            return Err(ResourceError::Validation(
                "resource annotation value exceeds 4096 bytes".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_media_type(media_type: &str) -> ResourceResult<()> {
    if media_type.is_empty()
        || media_type.len() > 255
        || !media_type.contains('/')
        || !media_type.is_ascii()
        || media_type != media_type.to_ascii_lowercase()
        || media_type.chars().any(char::is_whitespace)
    {
        return Err(ResourceError::Validation(
            "resource media type is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> ResourceResult<()> {
    let Some(encoded) = digest.strip_prefix("sha256:") else {
        return Err(ResourceError::Validation(
            "content integrity currently requires a sha256 digest".to_owned(),
        ));
    };
    if encoded.len() != 64
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResourceError::Validation(
            "sha256 digest must contain 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_location(location: &ResourceLocation) -> ResourceResult<()> {
    match location {
        ResourceLocation::PublicUrl { url } => {
            let parsed = Url::parse(url).map_err(|error| {
                ResourceError::Validation(format!("invalid public URL: {error}"))
            })?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(ResourceError::Validation(
                    "public URL must be credential-free HTTP(S) without query or fragment"
                        .to_owned(),
                ));
            }
        }
        ResourceLocation::Resolver { binding, reference } => {
            validate_token("resolver binding", binding)?;
            validate_token("resolver reference", reference)?;
        }
    }
    Ok(())
}

fn validate_token(kind: &str, value: &str) -> ResourceResult<()> {
    if value.is_empty() || value.len() > 2048 || value.chars().any(char::is_control) {
        return Err(ResourceError::Validation(format!(
            "{kind} must contain 1..=2048 non-control characters"
        )));
    }
    Ok(())
}
