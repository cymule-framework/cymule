use std::collections::BTreeMap;

use cymule_core::{ArtifactRecord, artifact_ref, canonical_bytes, canonical_digest, content_id};
use jsonschema::{Draft, Validator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{ResourceError, ResourceResult, ResourceSchemaIssue};

/// Frozen typed Artifact codec descriptor version.
pub const ARTIFACT_CODEC_VERSION: &str = "cymule.artifact-codec/1";
/// Media type emitted by the canonical JSON codec.
pub const CANONICAL_JSON_MEDIA_TYPE: &str = "application/json";
/// Closed JSON Schema dialect used by typed Artifact codecs.
pub const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Candidate for one pure canonical JSON Artifact codec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCodecCandidate {
    /// Codec wire version.
    pub codec_version: String,
    /// Stable Artifact kind produced and consumed by this codec.
    pub artifact_kind: String,
    /// Encoded media type. Version 1 accepts canonical JSON only.
    pub media_type: String,
    /// Complete local JSON Schema Draft 2020-12 contract.
    pub schema: Value,
}

/// Verified immutable descriptor for one pure Artifact codec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCodecDescriptor {
    /// Content-addressed identity of the complete codec contract.
    pub codec_id: String,
    /// Codec wire version.
    pub codec_version: String,
    /// Stable Artifact kind produced and consumed by this codec.
    pub artifact_kind: String,
    /// Encoded media type.
    pub media_type: String,
    /// SHA-256 digest of the canonical schema bytes.
    pub schema_digest: String,
    /// Complete local JSON Schema Draft 2020-12 contract.
    pub schema: Value,
}

#[derive(Serialize)]
struct CodecIdentity<'a> {
    codec_version: &'a str,
    artifact_kind: &'a str,
    media_type: &'a str,
    schema_digest: &'a str,
    schema: &'a Value,
}

#[derive(Debug, Clone)]
struct CompiledCodec {
    descriptor: ArtifactCodecDescriptor,
    validator: Validator,
}

/// Pure registry for content-addressed Artifact codecs.
///
/// The registry does no I/O and owns no resolver, store, clock, network, or
/// provider state. Resource retrieval and integrity verification complete
/// before these codecs receive bytes.
#[derive(Debug, Clone, Default)]
pub struct ArtifactCodecRegistry {
    codecs: BTreeMap<String, CompiledCodec>,
    kinds: BTreeMap<String, String>,
}

impl ArtifactCodecCandidate {
    /// Construct a canonical JSON codec candidate.
    pub fn canonical_json(artifact_kind: impl Into<String>, schema: Value) -> Self {
        Self {
            codec_version: ARTIFACT_CODEC_VERSION.to_owned(),
            artifact_kind: artifact_kind.into(),
            media_type: CANONICAL_JSON_MEDIA_TYPE.to_owned(),
            schema,
        }
    }

    /// Validate and seal the codec contract with a content-addressed identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported encoding contract, external schema
    /// reference, invalid schema, or invalid Artifact kind.
    pub fn seal(self) -> ResourceResult<ArtifactCodecDescriptor> {
        validate_candidate(&self)?;
        let schema_digest = schema_digest(&self.schema)?;
        let codec_id = codec_id(
            &self.codec_version,
            &self.artifact_kind,
            &self.media_type,
            &schema_digest,
            &self.schema,
        )?;
        Ok(ArtifactCodecDescriptor {
            codec_id,
            codec_version: self.codec_version,
            artifact_kind: self.artifact_kind,
            media_type: self.media_type,
            schema_digest,
            schema: self.schema,
        })
    }
}

impl ArtifactCodecDescriptor {
    /// Verify the complete descriptor, schema digest, and codec identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any retained contract field is malformed or no
    /// longer matches its content identity.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_candidate(&ArtifactCodecCandidate {
            codec_version: self.codec_version.clone(),
            artifact_kind: self.artifact_kind.clone(),
            media_type: self.media_type.clone(),
            schema: self.schema.clone(),
        })?;
        let expected_schema_digest = schema_digest(&self.schema)?;
        if self.schema_digest != expected_schema_digest {
            return Err(ResourceError::Integrity(format!(
                "Artifact codec schema digest {} does not match {expected_schema_digest}",
                self.schema_digest
            )));
        }
        let expected_codec_id = codec_id(
            &self.codec_version,
            &self.artifact_kind,
            &self.media_type,
            &self.schema_digest,
            &self.schema,
        )?;
        if self.codec_id != expected_codec_id {
            return Err(ResourceError::Integrity(format!(
                "Artifact codec ID {} does not match {expected_codec_id}",
                self.codec_id
            )));
        }
        Ok(())
    }
}

impl ArtifactCodecRegistry {
    /// Create an empty pure codec registry.
    pub const fn new() -> Self {
        Self {
            codecs: BTreeMap::new(),
            kinds: BTreeMap::new(),
        }
    }

    /// Register one verified codec descriptor.
    ///
    /// Re-registering the exact descriptor is idempotent. One Artifact kind has
    /// exactly one codec contract in a registry; schema evolution uses a new
    /// versioned kind instead of silently reinterpreting retained bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid descriptor or conflicting kind.
    pub fn register(&mut self, descriptor: ArtifactCodecDescriptor) -> ResourceResult<()> {
        descriptor.verify()?;
        if let Some(existing_id) = self.kinds.get(&descriptor.artifact_kind) {
            if existing_id != &descriptor.codec_id {
                return Err(ResourceError::Conflict(format!(
                    "Artifact kind {:?} already uses codec {existing_id}",
                    descriptor.artifact_kind
                )));
            }
            return Ok(());
        }
        let validator = compile_schema(&descriptor.schema)?;
        self.kinds.insert(
            descriptor.artifact_kind.clone(),
            descriptor.codec_id.clone(),
        );
        self.codecs.insert(
            descriptor.codec_id.clone(),
            CompiledCodec {
                descriptor,
                validator,
            },
        );
        Ok(())
    }

    /// Read one registered immutable descriptor.
    pub fn descriptor(&self, codec_id: &str) -> Option<&ArtifactCodecDescriptor> {
        self.codecs.get(codec_id).map(|codec| &codec.descriptor)
    }

    /// Seal one typed value as schema-validated canonical JSON Artifact bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the codec is absent, serialization fails, or the
    /// value violates its schema.
    pub fn put_canonical_json<T: Serialize>(
        &self,
        codec_id: &str,
        value: &T,
    ) -> ResourceResult<ArtifactRecord> {
        let codec = self.codec(codec_id)?;
        let value = serde_json::to_value(value)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        validate_value(codec, &value)?;
        let bytes = canonical_bytes(&value)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let reference = artifact_ref(&codec.descriptor.artifact_kind, &bytes);
        Ok(ArtifactRecord { reference, bytes })
    }

    /// Decode, integrity-check, canonicality-check, and schema-validate an
    /// Artifact before materializing the caller's type.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent codec, wrong kind, tampered identity,
    /// non-canonical JSON, schema violation, or incompatible target type.
    pub fn decode_typed<T: DeserializeOwned>(
        &self,
        codec_id: &str,
        artifact: &ArtifactRecord,
    ) -> ResourceResult<T> {
        let value = self.decode_json(codec_id, artifact)?;
        serde_json::from_value(value).map_err(|error| ResourceError::Validation(error.to_string()))
    }

    /// Decode and validate one canonical JSON Artifact as an untyped JSON value.
    ///
    /// # Errors
    ///
    /// Returns the same boundary failures as [`Self::decode_typed`].
    pub fn decode_json(&self, codec_id: &str, artifact: &ArtifactRecord) -> ResourceResult<Value> {
        let codec = self.codec(codec_id)?;
        if artifact.reference.kind != codec.descriptor.artifact_kind {
            return Err(ResourceError::Validation(format!(
                "Artifact kind {:?} does not match codec kind {:?}",
                artifact.reference.kind, codec.descriptor.artifact_kind
            )));
        }
        let expected = artifact_ref(&artifact.reference.kind, &artifact.bytes);
        if artifact.reference.artifact_id != expected.artifact_id {
            return Err(ResourceError::Integrity(format!(
                "Artifact ID {} does not match {}",
                artifact.reference.artifact_id, expected.artifact_id
            )));
        }
        let value: Value = serde_json::from_slice(&artifact.bytes)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let canonical = canonical_bytes(&value)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if canonical != artifact.bytes {
            return Err(ResourceError::Integrity(
                "typed JSON Artifact bytes are not canonical".to_owned(),
            ));
        }
        validate_value(codec, &value)?;
        Ok(value)
    }

    fn codec(&self, codec_id: &str) -> ResourceResult<&CompiledCodec> {
        self.codecs
            .get(codec_id)
            .ok_or_else(|| ResourceError::NotFound(format!("Artifact codec {codec_id}")))
    }
}

fn validate_candidate(candidate: &ArtifactCodecCandidate) -> ResourceResult<()> {
    if candidate.codec_version != ARTIFACT_CODEC_VERSION {
        return Err(ResourceError::Validation(format!(
            "unsupported Artifact codec version {:?}",
            candidate.codec_version
        )));
    }
    validate_artifact_kind(&candidate.artifact_kind)?;
    if candidate.media_type != CANONICAL_JSON_MEDIA_TYPE {
        return Err(ResourceError::Validation(format!(
            "Artifact codec version 1 requires media type {CANONICAL_JSON_MEDIA_TYPE}"
        )));
    }
    validate_schema_references(&candidate.schema)?;
    compile_schema(&candidate.schema)?;
    Ok(())
}

fn validate_artifact_kind(kind: &str) -> ResourceResult<()> {
    if kind.is_empty()
        || kind.len() > 255
        || !kind.is_ascii()
        || kind.chars().any(char::is_whitespace)
        || !kind.contains('/')
    {
        return Err(ResourceError::Validation(
            "Artifact kind must be a versioned non-whitespace ASCII token".to_owned(),
        ));
    }
    Ok(())
}

fn validate_schema_references(schema: &Value) -> ResourceResult<()> {
    match schema {
        Value::Object(object) => {
            if let Some(dialect) = object.get("$schema")
                && dialect.as_str() != Some(JSON_SCHEMA_DIALECT)
            {
                return Err(ResourceError::Validation(format!(
                    "typed Artifact schema must use {JSON_SCHEMA_DIALECT}"
                )));
            }
            for keyword in ["$ref", "$dynamicRef"] {
                if let Some(reference) = object.get(keyword) {
                    let reference = reference.as_str().ok_or_else(|| {
                        ResourceError::Validation(format!(
                            "typed Artifact schema {keyword} must be a string"
                        ))
                    })?;
                    if !reference.starts_with('#') {
                        return Err(ResourceError::Validation(format!(
                            "typed Artifact schema {keyword} must remain document-local"
                        )));
                    }
                }
            }
            for value in object.values() {
                validate_schema_references(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_schema_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn compile_schema(schema: &Value) -> ResourceResult<Validator> {
    Validator::options()
        .with_draft(Draft::Draft202012)
        .build(schema)
        .map_err(|error| ResourceError::Validation(format!("invalid Artifact schema: {error}")))
}

fn validate_value(codec: &CompiledCodec, value: &Value) -> ResourceResult<()> {
    let mut errors = codec.validator.iter_errors(value);
    if let Some(error) = errors.next() {
        return Err(ResourceError::Schema(ResourceSchemaIssue {
            codec_id: codec.descriptor.codec_id.clone(),
            instance_path: error.instance_path().as_str().to_owned(),
            schema_path: error.schema_path().as_str().to_owned(),
            message: error.to_string(),
        }));
    }
    Ok(())
}

fn schema_digest(schema: &Value) -> ResourceResult<String> {
    canonical_digest(schema)
        .map(|digest| format!("sha256:{digest}"))
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn codec_id(
    codec_version: &str,
    artifact_kind: &str,
    media_type: &str,
    schema_digest: &str,
    schema: &Value,
) -> ResourceResult<String> {
    content_id(
        ARTIFACT_CODEC_VERSION,
        &CodecIdentity {
            codec_version,
            artifact_kind,
            media_type,
            schema_digest,
            schema,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}
