use std::collections::BTreeMap;

use cymule_core::{ArtifactRecord, artifact_ref, canonical_bytes, canonical_digest, content_id};
use jsonschema::{Draft, Validator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{ResourceError, ResourceResult, ResourceSchemaIssue};

/// Frozen typed Artifact contract descriptor version.
pub const ARTIFACT_TYPE_CONTRACT_VERSION: &str = "cymule.artifact-type-contract/1";
/// Opaque Artifact kind used to persist one recoverable type contract.
pub const ARTIFACT_TYPE_CONTRACT_KIND: &str = "cymule.artifact-type-contract/1";
/// Media type emitted by the canonical JSON contract.
pub const CANONICAL_JSON_MEDIA_TYPE: &str = "application/json";
/// Closed JSON Schema dialect used by typed Artifact contracts.
pub const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

/// Closed framework Artifact types with exact immutable contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameworkArtifactType {
    /// Semantic Resource Handle delivered across Runs.
    ResourceHandle,
    /// Content-addressed directory/collection manifest descriptor.
    ResourceManifest,
    /// Bounded inclusion proof for one manifest page.
    ResourceListProof,
    /// Exact producer-provenance Run handoff.
    ResourceHandoff,
    /// Pin/release/GC/delete/cleanup lifecycle receipt union.
    ResourceLifecycleReceipt,
}

/// Candidate for one pure canonical JSON Artifact contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTypeCandidate {
    /// Type-contract wire version.
    pub contract_version: String,
    /// Stable Artifact kind produced and consumed by this contract.
    pub artifact_kind: String,
    /// Encoded media type. Version 1 accepts canonical JSON only.
    pub media_type: String,
    /// Complete local JSON Schema Draft 2020-12 contract.
    pub schema: Value,
}

/// Verified immutable descriptor for one pure Artifact contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTypeContract {
    /// Content-addressed identity of the complete type contract.
    pub contract_id: String,
    /// Type-contract wire version.
    pub contract_version: String,
    /// Stable Artifact kind produced and consumed by this contract.
    pub artifact_kind: String,
    /// Encoded media type.
    pub media_type: String,
    /// SHA-256 digest of the canonical schema bytes.
    pub schema_digest: String,
    /// Complete local JSON Schema Draft 2020-12 contract.
    pub schema: Value,
}

#[derive(Serialize)]
struct ContractIdentity<'a> {
    contract_version: &'a str,
    artifact_kind: &'a str,
    media_type: &'a str,
    schema_digest: &'a str,
    schema: &'a Value,
}

#[derive(Debug, Clone)]
struct CompiledContract {
    descriptor: ArtifactTypeContract,
    validator: Validator,
}

/// Pure registry for content-addressed Artifact contracts.
///
/// The registry does no I/O and owns no resolver, store, clock, network, or
/// provider state. Resource retrieval and integrity verification complete
/// before these contracts receive bytes.
#[derive(Debug, Clone, Default)]
pub struct ArtifactTypeRegistry {
    contracts: BTreeMap<String, CompiledContract>,
}

impl ArtifactTypeCandidate {
    /// Construct a canonical JSON contract candidate.
    pub fn canonical_json(artifact_kind: impl Into<String>, schema: Value) -> Self {
        Self {
            contract_version: ARTIFACT_TYPE_CONTRACT_VERSION.to_owned(),
            artifact_kind: artifact_kind.into(),
            media_type: CANONICAL_JSON_MEDIA_TYPE.to_owned(),
            schema,
        }
    }

    /// Validate and seal the type contract with a content-addressed identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported encoding contract, external schema
    /// reference, invalid schema, or invalid Artifact kind.
    pub fn seal(self) -> ResourceResult<ArtifactTypeContract> {
        validate_candidate(&self)?;
        let schema_digest = schema_digest(&self.schema)?;
        let contract_id = contract_id(
            &self.contract_version,
            &self.artifact_kind,
            &self.media_type,
            &schema_digest,
            &self.schema,
        )?;
        Ok(ArtifactTypeContract {
            contract_id,
            contract_version: self.contract_version,
            artifact_kind: self.artifact_kind,
            media_type: self.media_type,
            schema_digest,
            schema: self.schema,
        })
    }
}

impl ArtifactTypeContract {
    /// Verify the complete descriptor, schema digest, and contract identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any retained contract field is malformed or no
    /// longer matches its content identity.
    pub fn verify(&self) -> ResourceResult<()> {
        validate_candidate(&ArtifactTypeCandidate {
            contract_version: self.contract_version.clone(),
            artifact_kind: self.artifact_kind.clone(),
            media_type: self.media_type.clone(),
            schema: self.schema.clone(),
        })?;
        let expected_schema_digest = schema_digest(&self.schema)?;
        if self.schema_digest != expected_schema_digest {
            return Err(ResourceError::Integrity(format!(
                "Artifact contract schema digest {} does not match {expected_schema_digest}",
                self.schema_digest
            )));
        }
        let expected_contract_id = contract_id(
            &self.contract_version,
            &self.artifact_kind,
            &self.media_type,
            &self.schema_digest,
            &self.schema,
        )?;
        if self.contract_id != expected_contract_id {
            return Err(ResourceError::Integrity(format!(
                "Artifact contract ID {} does not match {expected_contract_id}",
                self.contract_id
            )));
        }
        Ok(())
    }
}

impl ArtifactTypeRegistry {
    /// Create an empty pure type registry.
    pub const fn new() -> Self {
        Self {
            contracts: BTreeMap::new(),
        }
    }

    /// Construct a registry containing every frozen framework Resource contract.
    pub fn with_framework_contracts() -> ResourceResult<Self> {
        let mut registry = Self::new();
        for descriptor in framework_artifact_contracts()? {
            registry.register(descriptor)?;
        }
        Ok(registry)
    }

    /// Register one verified contract descriptor.
    ///
    /// Re-registering the exact descriptor is idempotent. Multiple immutable
    /// contracts may describe revisions of one logical kind because every
    /// typed Artifact reference pins its exact contract.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid descriptor or schema.
    pub fn register(&mut self, descriptor: ArtifactTypeContract) -> ResourceResult<()> {
        descriptor.verify()?;
        if self.contracts.contains_key(&descriptor.contract_id) {
            return Ok(());
        }
        let validator = compile_schema(&descriptor.schema)?;
        self.contracts.insert(
            descriptor.contract_id.clone(),
            CompiledContract {
                descriptor,
                validator,
            },
        );
        Ok(())
    }

    /// Rebuild a registry entry from a content-addressed contract Artifact.
    ///
    /// # Errors
    ///
    /// Returns an error when the Artifact kind, identity, canonical bytes, or
    /// retained contract identity is invalid.
    pub fn register_artifact(&mut self, artifact: &ArtifactRecord) -> ResourceResult<()> {
        if artifact.reference.kind != ARTIFACT_TYPE_CONTRACT_KIND {
            return Err(ResourceError::Validation(
                "Artifact is not an Artifact type contract".to_owned(),
            ));
        }
        verify_artifact(artifact)?;
        let value = decode_canonical_json(&artifact.bytes)?;
        let contract: ArtifactTypeContract = serde_json::from_value(value).map_err(|_| {
            ResourceError::Validation("Artifact type contract wire shape is invalid".to_owned())
        })?;
        self.register(contract)
    }

    /// Read one registered immutable descriptor.
    pub fn descriptor(&self, contract_id: &str) -> Option<&ArtifactTypeContract> {
        self.contracts
            .get(contract_id)
            .map(|contract| &contract.descriptor)
    }

    /// Seal one typed value as schema-validated canonical JSON Artifact bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the contract is absent, serialization fails, or the
    /// value violates its schema.
    pub fn put_canonical_json<T: Serialize>(
        &self,
        contract_id: &str,
        value: &T,
    ) -> ResourceResult<ArtifactRecord> {
        let contract = self.contract(contract_id)?;
        let value = serde_json::to_value(value).map_err(|_| {
            ResourceError::Validation("typed Artifact value cannot be serialized".to_owned())
        })?;
        validate_value(contract, &value)?;
        let bytes = canonical_bytes(&value)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        let kind = typed_json_kind(&contract.descriptor.contract_id)?;
        let reference = artifact_ref(kind, &bytes)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        Ok(ArtifactRecord { reference, bytes })
    }

    /// Decode, integrity-check, canonicality-check, and schema-validate an
    /// Artifact before materializing the caller's type.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent contract, wrong kind, tampered identity,
    /// non-canonical JSON, schema violation, or incompatible target type.
    pub fn decode_typed<T: DeserializeOwned>(
        &self,
        artifact: &ArtifactRecord,
    ) -> ResourceResult<T> {
        let value = self.decode_json(artifact)?;
        serde_json::from_value(value).map_err(|_| {
            ResourceError::Validation(
                "typed Artifact value cannot be materialized as the requested type".to_owned(),
            )
        })
    }

    /// Decode and validate one canonical JSON Artifact as an untyped JSON value.
    ///
    /// # Errors
    ///
    /// Returns the same boundary failures as [`Self::decode_typed`].
    pub fn decode_json(&self, artifact: &ArtifactRecord) -> ResourceResult<Value> {
        verify_artifact(artifact)?;
        let contract_id = contract_id_from_kind(&artifact.reference.kind)?;
        let contract = self.contract(&contract_id)?;
        let value = decode_canonical_json(&artifact.bytes)?;
        validate_value(contract, &value)?;
        Ok(value)
    }

    fn contract(&self, contract_id: &str) -> ResourceResult<&CompiledContract> {
        self.contracts
            .get(contract_id)
            .ok_or_else(|| ResourceError::NotFound(format!("Artifact contract {contract_id}")))
    }
}

/// Seal one exact framework-owned Artifact type contract.
pub fn framework_artifact_contract(
    artifact_type: FrameworkArtifactType,
) -> ResourceResult<ArtifactTypeContract> {
    let (artifact_kind, definition) = match artifact_type {
        FrameworkArtifactType::ResourceHandle => {
            ("cymule.framework-resource-handle/2", "resourceHandle")
        }
        FrameworkArtifactType::ResourceManifest => {
            ("cymule.framework-resource-manifest/1", "manifestDescriptor")
        }
        FrameworkArtifactType::ResourceListProof => {
            ("cymule.framework-resource-list-proof/2", "listProof")
        }
        FrameworkArtifactType::ResourceHandoff => {
            ("cymule.framework-resource-handoff/3", "resourceHandoff")
        }
        FrameworkArtifactType::ResourceLifecycleReceipt => (
            "cymule.framework-resource-lifecycle-receipt/1",
            "lifecycleReceipt",
        ),
    };
    ArtifactTypeCandidate::canonical_json(artifact_kind, framework_schema(definition)?).seal()
}

/// Seal every framework-owned Resource Artifact contract in stable order.
pub fn framework_artifact_contracts() -> ResourceResult<Vec<ArtifactTypeContract>> {
    [
        FrameworkArtifactType::ResourceHandle,
        FrameworkArtifactType::ResourceManifest,
        FrameworkArtifactType::ResourceListProof,
        FrameworkArtifactType::ResourceHandoff,
        FrameworkArtifactType::ResourceLifecycleReceipt,
    ]
    .into_iter()
    .map(framework_artifact_contract)
    .collect()
}

fn framework_schema(definition: &str) -> ResourceResult<Value> {
    let schema = match definition {
        "resourceHandle" => resource_handle_schema(),
        "manifestDescriptor" => manifest_descriptor_schema(),
        "listProof" => list_proof_schema(),
        "resourceHandoff" => resource_handoff_schema(),
        "lifecycleReceipt" => lifecycle_receipt_schema(),
        _ => {
            return Err(ResourceError::Validation(format!(
                "unknown framework Artifact definition {definition}"
            )));
        }
    };
    Ok(schema)
}

fn resource_handle_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "required": ["resource_id", "resource_version", "shape", "media_type", "integrity"],
        "properties": {
            "resource_id": digest_schema(),
            "resource_version": {"const": crate::RESOURCE_VERSION},
            "shape": {"enum": ["inline", "object", "collection", "directory", "snapshot"]},
            "media_type": {"type": "string", "minLength": 3, "maxLength": 255},
            "inline": inline_schema(),
            "integrity": integrity_schema(),
            "manifest": manifest_descriptor_schema_without_dialect(),
            "annotations": {
                "type": "object",
                "additionalProperties": {"type": "string", "maxLength": 4096}
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

fn manifest_descriptor_schema() -> Value {
    let mut schema = manifest_descriptor_schema_without_dialect();
    schema
        .as_object_mut()
        .expect("manifest schema is an object")
        .insert(
            "$schema".to_owned(),
            Value::String(JSON_SCHEMA_DIALECT.to_owned()),
        );
    schema
}

fn manifest_descriptor_schema_without_dialect() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["manifest_version", "media_type", "digest", "size", "entry_count", "root_digest"],
        "properties": {
            "manifest_version": {"const": crate::RESOURCE_MANIFEST_VERSION},
            "media_type": {"const": crate::RESOURCE_MANIFEST_MEDIA_TYPE},
            "digest": digest_schema(),
            "size": {"type": "integer", "minimum": 0},
            "entry_count": {"type": "integer", "minimum": 0},
            "root_digest": digest_schema()
        }
    })
}

fn list_proof_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "required": ["proof_version", "manifest_digest", "entry_count", "start_index", "request_cursor_digest", "next_cursor_digest", "inclusions"],
        "properties": {
            "proof_version": {"const": crate::RESOURCE_LIST_PROOF_VERSION},
            "manifest_digest": digest_schema(),
            "entry_count": {"type": "integer", "minimum": 0},
            "start_index": {"type": "integer", "minimum": 0},
            "request_cursor_digest": digest_schema(),
            "next_cursor_digest": digest_schema(),
            "inclusions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["index", "path"],
                    "properties": {
                        "index": {"type": "integer", "minimum": 0},
                        "path": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["side", "digest"],
                                "properties": {"side": {"enum": ["left", "right"]}, "digest": digest_schema()}
                            }
                        }
                    }
                }
            }
        }
    })
}

fn resource_handoff_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "required": ["handoff_version", "transfer_id", "producer", "to_run", "slot", "resource"],
        "properties": {
            "handoff_version": {"const": crate::RESOURCE_HANDOFF_VERSION},
            "transfer_id": non_empty_schema(),
            "producer": {
                "type": "object",
                "additionalProperties": false,
                "required": ["run_id", "occurrence_id", "result"],
                "properties": {
                    "run_id": non_empty_schema(),
                    "occurrence_id": non_empty_schema(),
                    "result": artifact_ref_schema()
                }
            },
            "to_run": non_empty_schema(),
            "slot": non_empty_schema(),
            "resource": artifact_ref_schema()
        }
    })
}

fn lifecycle_receipt_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "oneOf": [
            receipt_schema(
                crate::RESOURCE_PIN_RECEIPT_VERSION,
                &["pin_id", "resource_id", "owner"],
                &serde_json::json!({"pin_id": non_empty_schema(), "resource_id": digest_schema(), "owner": non_empty_schema()}),
            ),
            receipt_schema(
                crate::RESOURCE_RELEASE_RECEIPT_VERSION,
                &["release_id", "pin_id", "resource_id"],
                &serde_json::json!({"release_id": non_empty_schema(), "pin_id": non_empty_schema(), "resource_id": digest_schema()}),
            ),
            receipt_schema(
                crate::RESOURCE_GC_RECEIPT_VERSION,
                &["gc_id", "resource_id", "active_pin_count", "disposition"],
                &serde_json::json!({"gc_id": non_empty_schema(), "resource_id": digest_schema(), "active_pin_count": {"type": "integer", "minimum": 0}, "disposition": {"enum": ["retained", "eligible"]}}),
            ),
            receipt_schema(
                crate::RESOURCE_DELETE_RECEIPT_VERSION,
                &["delete_id", "gc_id", "resource_id", "store_binding", "removed_bytes", "verified_absent"],
                &serde_json::json!({"delete_id": non_empty_schema(), "gc_id": non_empty_schema(), "resource_id": digest_schema(), "store_binding": non_empty_schema(), "removed_bytes": {"type": "integer", "minimum": 0}, "verified_absent": {"const": true}}),
            ),
            receipt_schema(
                crate::RESOURCE_CLEANUP_RECEIPT_VERSION,
                &["write_id", "upload_id", "store_binding", "removed_staging_objects", "removed_chunks", "verified_absent"],
                &serde_json::json!({"write_id": non_empty_schema(), "upload_id": non_empty_schema(), "store_binding": non_empty_schema(), "removed_staging_objects": {"type": "integer", "minimum": 0}, "removed_chunks": {"type": "integer", "minimum": 0}, "verified_absent": {"const": true}}),
            )
        ]
    })
}

fn receipt_schema(version: &str, required: &[&str], properties: &Value) -> Value {
    let mut required = required
        .iter()
        .map(|value| Value::String((*value).to_owned()))
        .collect::<Vec<_>>();
    required.insert(0, Value::String("receipt_version".to_owned()));
    let mut properties = properties
        .as_object()
        .cloned()
        .expect("receipt properties are an object");
    properties.insert(
        "receipt_version".to_owned(),
        serde_json::json!({"const": version}),
    );
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties
    })
}

fn inline_schema() -> Value {
    serde_json::json!({"oneOf": [
        {"type": "object", "additionalProperties": false, "required": ["encoding", "text"], "properties": {"encoding": {"const": "utf8"}, "text": {"type": "string"}}},
        {"type": "object", "additionalProperties": false, "required": ["encoding", "value"], "properties": {"encoding": {"const": "json"}, "value": true}},
        {"type": "object", "additionalProperties": false, "required": ["encoding", "data"], "properties": {"encoding": {"const": "base64"}, "data": {"type": "string"}}}
    ]})
}

fn integrity_schema() -> Value {
    serde_json::json!({"oneOf": [
        {"type": "object", "additionalProperties": false, "required": ["kind"], "properties": {"kind": {"const": "inline"}}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "digest", "size"], "properties": {"kind": {"const": "content"}, "digest": digest_schema(), "size": {"type": "integer", "minimum": 0}}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "authority", "version"], "properties": {"kind": {"const": "version"}, "authority": non_empty_schema(), "version": non_empty_schema()}},
        {"type": "object", "additionalProperties": false, "required": ["kind", "identity"], "properties": {"kind": {"const": "live"}, "identity": non_empty_schema()}}
    ]})
}

fn artifact_ref_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["identity_version", "artifact_id", "kind"],
        "properties": {
            "identity_version": {"const": cymule_core::ARTIFACT_IDENTITY_VERSION},
            "artifact_id": digest_schema(),
            "kind": {"type": "string", "pattern": "^[a-z0-9][a-z0-9._+-]*/[a-z0-9][a-z0-9._+/-]*$"}
        }
    })
}

fn digest_schema() -> Value {
    serde_json::json!({"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"})
}

fn non_empty_schema() -> Value {
    serde_json::json!({"type": "string", "minLength": 1, "maxLength": 512})
}

impl ArtifactTypeContract {
    /// Persist this contract as a canonical, content-addressed Artifact.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor or canonical encoding is invalid.
    pub fn artifact_record(&self) -> ResourceResult<ArtifactRecord> {
        self.verify()?;
        let bytes =
            canonical_bytes(self).map_err(|error| ResourceError::Validation(error.to_string()))?;
        let reference = artifact_ref(ARTIFACT_TYPE_CONTRACT_KIND, &bytes)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        Ok(ArtifactRecord { reference, bytes })
    }
}

fn validate_candidate(candidate: &ArtifactTypeCandidate) -> ResourceResult<()> {
    if candidate.contract_version != ARTIFACT_TYPE_CONTRACT_VERSION {
        return Err(ResourceError::Validation(format!(
            "unsupported Artifact contract version {:?}",
            candidate.contract_version
        )));
    }
    validate_artifact_kind(&candidate.artifact_kind)?;
    if candidate.media_type != CANONICAL_JSON_MEDIA_TYPE {
        return Err(ResourceError::Validation(format!(
            "Artifact contract version 1 requires media type {CANONICAL_JSON_MEDIA_TYPE}"
        )));
    }
    validate_schema_references(&candidate.schema)?;
    compile_schema(&candidate.schema)?;
    Ok(())
}

fn validate_artifact_kind(kind: &str) -> ResourceResult<()> {
    artifact_ref(kind, &[])
        .map(|_| ())
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_schema_references(schema: &Value) -> ResourceResult<()> {
    let Value::Object(object) = schema else {
        return Ok(());
    };
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
    for keyword in [
        "not",
        "if",
        "then",
        "else",
        "items",
        "contains",
        "propertyNames",
        "additionalProperties",
        "unevaluatedProperties",
        "unevaluatedItems",
        "contentSchema",
    ] {
        if let Some(child) = object.get(keyword) {
            validate_schema_references(child)?;
        }
    }
    for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(Value::Array(children)) = object.get(keyword) {
            for child in children {
                validate_schema_references(child)?;
            }
        }
    }
    for keyword in [
        "$defs",
        "definitions",
        "properties",
        "patternProperties",
        "dependentSchemas",
    ] {
        if let Some(Value::Object(children)) = object.get(keyword) {
            for child in children.values() {
                validate_schema_references(child)?;
            }
        }
    }
    Ok(())
}

fn compile_schema(schema: &Value) -> ResourceResult<Validator> {
    Validator::options()
        .with_draft(Draft::Draft202012)
        .build(schema)
        .map_err(|error| ResourceError::Validation(format!("invalid Artifact schema: {error}")))
}

fn validate_value(contract: &CompiledContract, value: &Value) -> ResourceResult<()> {
    let mut errors = contract.validator.iter_errors(value);
    if let Some(error) = errors.next() {
        return Err(ResourceError::Schema(ResourceSchemaIssue {
            contract_id: contract.descriptor.contract_id.clone(),
            instance_path: error.instance_path().as_str().to_owned(),
            schema_path: error.schema_path().as_str().to_owned(),
        }));
    }
    Ok(())
}

fn typed_json_kind(contract_id: &str) -> ResourceResult<String> {
    let digest = contract_id
        .strip_prefix("sha256:")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            ResourceError::Validation("Artifact type contract ID is malformed".to_owned())
        })?;
    Ok(format!("cymule.typed-json/sha256-{digest}"))
}

fn contract_id_from_kind(kind: &str) -> ResourceResult<String> {
    let digest = kind
        .strip_prefix("cymule.typed-json/sha256-")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| {
            ResourceError::Validation("Artifact does not pin a typed JSON contract".to_owned())
        })?;
    Ok(format!("sha256:{digest}"))
}

fn verify_artifact(artifact: &ArtifactRecord) -> ResourceResult<()> {
    artifact
        .reference
        .validate()
        .map_err(|error| ResourceError::Integrity(error.to_string()))?;
    let expected = artifact_ref(&artifact.reference.kind, &artifact.bytes)
        .map_err(|error| ResourceError::Integrity(error.to_string()))?;
    if artifact.reference != expected {
        return Err(ResourceError::Integrity(format!(
            "Artifact ID {} does not match its identity version, kind, and bytes",
            artifact.reference.artifact_id
        )));
    }
    Ok(())
}

fn decode_canonical_json(bytes: &[u8]) -> ResourceResult<Value> {
    let value: Value = cymule_core::decode_json(bytes)
        .map_err(|_| ResourceError::Validation("Artifact is not valid JSON".to_owned()))?;
    let canonical =
        canonical_bytes(&value).map_err(|error| ResourceError::Validation(error.to_string()))?;
    if canonical != bytes {
        return Err(ResourceError::Integrity(
            "typed JSON Artifact bytes are not canonical".to_owned(),
        ));
    }
    Ok(value)
}

fn schema_digest(schema: &Value) -> ResourceResult<String> {
    canonical_digest(schema)
        .map(|digest| format!("sha256:{digest}"))
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn contract_id(
    contract_version: &str,
    artifact_kind: &str,
    media_type: &str,
    schema_digest: &str,
    schema: &Value,
) -> ResourceResult<String> {
    content_id(
        ARTIFACT_TYPE_CONTRACT_VERSION,
        &ContractIdentity {
            contract_version,
            artifact_kind,
            media_type,
            schema_digest,
            schema,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}
