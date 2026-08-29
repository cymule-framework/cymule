use std::collections::BTreeMap;

use cymule_core::{ArtifactRecord, artifact_ref, canonical_bytes, canonical_digest, content_id};
use jsonschema::{Draft, Registry, Retrieve, Uri, Validator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

pub use cymule_profile_protocol::resource::{
    ARTIFACT_TYPE_CONTRACT_VERSION, CANONICAL_JSON_MEDIA_TYPE, JSON_SCHEMA_DIALECT,
};
use cymule_profile_protocol::resource::{
    FRAMEWORK_RESOURCE_HANDLE_TYPE_KEY, resource_handle_artifact_schema,
};

use crate::{ResourceError, ResourceResult, ResourceSchemaIssue};

/// Opaque Artifact kind used to persist one recoverable type contract.
pub const ARTIFACT_TYPE_CONTRACT_KIND: &str = "cymule.artifact-type-contract/1";
/// Maximum canonical JSON bytes in one typed Artifact schema.
pub const MAX_ARTIFACT_TYPE_SCHEMA_BYTES: usize = 1024 * 1024;
/// Maximum JSON values in one typed Artifact schema, including its root and data keywords.
pub const MAX_ARTIFACT_TYPE_SCHEMA_NODES: usize = 16_384;
/// Maximum syntactic and local-reference-expanded depth, with the schema root at depth one.
pub const MAX_ARTIFACT_TYPE_SCHEMA_DEPTH: usize = 64;
/// Maximum schema-node visits when expanding local references, counting repeated targets.
pub const MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS: usize = 65_536;
/// Maximum cumulative canonical subschema bytes when expanding local references.
pub const MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES: usize = 16 * 1024 * 1024;
const FRAMEWORK_RESOURCE_HANDOFF_TYPE_KEY: &str = "cymule.framework-resource-handoff/5";
const FRAMEWORK_RESOURCE_LIST_PROOF_TYPE_KEY: &str = "cymule.framework-resource-list-proof/5";
const FRAMEWORK_RESOURCE_MANIFEST_TYPE_KEY: &str = "cymule.framework-resource-manifest/3";

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
    /// Complete acyclic local JSON Schema Draft 2020-12 contract within the shared budgets.
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
    /// Complete acyclic local JSON Schema Draft 2020-12 contract within the shared budgets.
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
    pub fn seal(mut self) -> ResourceResult<ArtifactTypeContract> {
        if let Err(error) = validate_contract_fields(
            &self.contract_version,
            &self.artifact_kind,
            &self.media_type,
            &self.schema,
        ) {
            discard_schema(&mut self.schema);
            return Err(error);
        }
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
        validate_contract_fields(
            &self.contract_version,
            &self.artifact_kind,
            &self.media_type,
            &self.schema,
        )?;
        let expected_schema_digest = schema_digest(&self.schema)?;
        if self.schema_digest != expected_schema_digest {
            return Err(ResourceError::Integrity {
                code: "artifact_contract_schema_digest_mismatch".to_owned(),
                message: format!(
                    "Artifact contract schema digest {} does not match {expected_schema_digest}",
                    self.schema_digest
                ),
            });
        }
        let expected_contract_id = contract_id(
            &self.contract_version,
            &self.artifact_kind,
            &self.media_type,
            &self.schema_digest,
            &self.schema,
        )?;
        if self.contract_id != expected_contract_id {
            return Err(ResourceError::Integrity {
                code: "artifact_contract_identity_mismatch".to_owned(),
                message: format!(
                    "Artifact contract ID {} does not match {expected_contract_id}",
                    self.contract_id
                ),
            });
        }
        Ok(())
    }

    /// Derive the exact Artifact kind that carries values admitted by this contract.
    ///
    /// Component declarations use this value as their required output Artifact
    /// kind, so a normal durable completion produces the same typed authority
    /// consumed by Resource handoff admission.
    ///
    /// # Errors
    ///
    /// Returns an error when this contract is invalid or its exact typed kind
    /// cannot be derived from the retained contract identity.
    pub fn typed_artifact_kind(&self) -> ResourceResult<String> {
        self.verify()?;
        typed_json_kind(&self.contract_id)
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
    ///
    /// # Errors
    ///
    /// Returns an error when a framework contract cannot be sealed or registered.
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
    pub fn register(&mut self, mut descriptor: ArtifactTypeContract) -> ResourceResult<()> {
        if let Err(error) = descriptor.verify() {
            discard_schema(&mut descriptor.schema);
            return Err(error);
        }
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
///
/// # Errors
///
/// Returns an error when the schema cannot be constructed, compiled, or sealed.
pub fn framework_artifact_contract(
    artifact_type: FrameworkArtifactType,
) -> ResourceResult<ArtifactTypeContract> {
    let (artifact_kind, definition) = match artifact_type {
        FrameworkArtifactType::ResourceHandle => {
            (FRAMEWORK_RESOURCE_HANDLE_TYPE_KEY, "resourceHandle")
        }
        FrameworkArtifactType::ResourceManifest => {
            (FRAMEWORK_RESOURCE_MANIFEST_TYPE_KEY, "manifestDescriptor")
        }
        FrameworkArtifactType::ResourceListProof => {
            (FRAMEWORK_RESOURCE_LIST_PROOF_TYPE_KEY, "listProof")
        }
        FrameworkArtifactType::ResourceHandoff => {
            (FRAMEWORK_RESOURCE_HANDOFF_TYPE_KEY, "resourceHandoff")
        }
    };
    ArtifactTypeCandidate::canonical_json(artifact_kind, framework_schema(definition)?).seal()
}

/// Seal every framework-owned Resource Artifact contract in stable order.
///
/// # Errors
///
/// Returns an error when any framework contract cannot be sealed.
pub fn framework_artifact_contracts() -> ResourceResult<Vec<ArtifactTypeContract>> {
    [
        FrameworkArtifactType::ResourceHandle,
        FrameworkArtifactType::ResourceManifest,
        FrameworkArtifactType::ResourceListProof,
        FrameworkArtifactType::ResourceHandoff,
    ]
    .into_iter()
    .map(framework_artifact_contract)
    .collect()
}

fn framework_schema(definition: &str) -> ResourceResult<Value> {
    let schema = match definition {
        "resourceHandle" => resource_handle_artifact_schema(),
        "manifestDescriptor" => manifest_descriptor_schema(),
        "listProof" => list_proof_schema(),
        "resourceHandoff" => resource_handoff_schema(),
        _ => {
            return Err(ResourceError::Validation(format!(
                "unknown framework Artifact definition {definition}"
            )));
        }
    };
    Ok(schema)
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
            "size": safe_non_negative_integer_schema(),
            "entry_count": safe_non_negative_integer_schema(),
            "root_digest": digest_schema()
        }
    })
}

fn list_proof_schema() -> Value {
    serde_json::json!({
        "$schema": JSON_SCHEMA_DIALECT,
        "type": "object",
        "additionalProperties": false,
        "required": ["proof_version", "manifest_digest", "entry_count", "start_index", "request_cursor_digest", "next_cursor_digest", "predecessor", "inclusions"],
        "properties": {
            "proof_version": {"const": crate::RESOURCE_LIST_PROOF_VERSION},
            "manifest_digest": digest_schema(),
            "entry_count": safe_non_negative_integer_schema(),
            "start_index": safe_non_negative_integer_schema(),
            "request_cursor_digest": digest_schema(),
            "next_cursor_digest": digest_schema(),
            "predecessor": {
                "oneOf": [
                    {"type": "null"},
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["entry", "inclusion"],
                        "properties": {
                            "entry": manifest_entry_schema(),
                            "inclusion": manifest_inclusion_schema()
                        }
                    }
                ]
            },
            "inclusions": {
                "type": "array",
                "maxItems": crate::MAX_LIST_PAGE,
                "items": manifest_inclusion_schema()
            }
        }
    })
}

fn manifest_entry_schema() -> Value {
    let mut resource = resource_handle_artifact_schema();
    resource
        .as_object_mut()
        .expect("Resource Handle schema is an object")
        .remove("$schema");
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["name", "resource"],
        "properties": {
            "name": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512,
                "pattern": r"^[^\u0000-\u001f\u007f-\u009f\\]+$"
            },
            "resource": resource
        }
    })
}

fn manifest_inclusion_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["index", "path"],
        "properties": {
            "index": safe_non_negative_integer_schema(),
            "path": {
                "type": "array",
                "maxItems": crate::MAX_MANIFEST_PROOF_DEPTH,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["side", "digest"],
                    "properties": {
                        "side": {"enum": ["left", "right"]},
                        "digest": digest_schema()
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
            "transfer_id": core_identity_schema(),
            "producer": {
                "type": "object",
                "additionalProperties": false,
                "required": ["run_id", "occurrence_id", "result"],
                "properties": {
                    "run_id": core_identity_schema(),
                    "occurrence_id": core_identity_schema(),
                    "result": artifact_ref_schema()
                }
            },
            "to_run": core_identity_schema(),
            "slot": core_identity_schema(),
            "resource": artifact_ref_schema()
        }
    })
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

fn safe_non_negative_integer_schema() -> Value {
    serde_json::json!({
        "type": "integer",
        "minimum": 0,
        "maximum": cymule_core::MAX_EXACT_INTEGER
    })
}

fn core_identity_schema() -> Value {
    serde_json::json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 512,
        "pattern": r"^[^\u0000-\u001f\u007f-\u009f]+$"
    })
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

fn validate_contract_fields(
    contract_version: &str,
    artifact_kind: &str,
    media_type: &str,
    schema: &Value,
) -> ResourceResult<()> {
    if contract_version != ARTIFACT_TYPE_CONTRACT_VERSION {
        return Err(ResourceError::Validation(format!(
            "unsupported Artifact contract version {contract_version:?}"
        )));
    }
    validate_artifact_kind(artifact_kind)?;
    if media_type != CANONICAL_JSON_MEDIA_TYPE {
        return Err(ResourceError::Validation(format!(
            "Artifact contract version 1 requires media type {CANONICAL_JSON_MEDIA_TYPE}"
        )));
    }
    validate_schema_budget(schema)?;
    validate_schema_references(schema)?;
    compile_schema(schema)?;
    Ok(())
}

fn validate_artifact_kind(kind: &str) -> ResourceResult<()> {
    artifact_ref(kind, &[])
        .map(|_| ())
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_schema_budget(schema: &Value) -> ResourceResult<()> {
    let mut pending = vec![(schema, 1_usize)];
    let mut visited = 0_usize;
    let mut encoded_bytes = 0_usize;
    while let Some((value, depth)) = pending.pop() {
        visited += 1;
        if depth > MAX_ARTIFACT_TYPE_SCHEMA_DEPTH {
            return Err(ResourceError::Validation(format!(
                "typed Artifact schema exceeds depth {MAX_ARTIFACT_TYPE_SCHEMA_DEPTH}"
            )));
        }
        match value {
            Value::Null | Value::Bool(true) => {
                account_schema_bytes(&mut encoded_bytes, 4)?;
            }
            Value::Bool(false) => {
                account_schema_bytes(&mut encoded_bytes, 5)?;
            }
            Value::Number(number) => {
                let number_bytes = canonical_bytes(number)
                    .map_err(|error| ResourceError::Validation(error.to_string()))?;
                account_schema_bytes(&mut encoded_bytes, number_bytes.len())?;
            }
            Value::String(text) => {
                account_schema_string_bytes(&mut encoded_bytes, text)?;
            }
            Value::Array(children) => {
                check_schema_nodes(visited, pending.len(), children.len())?;
                account_schema_bytes(&mut encoded_bytes, 2)?;
                account_schema_bytes(&mut encoded_bytes, children.len().saturating_sub(1))?;
                pending.extend(children.iter().map(|child| (child, depth + 1)));
            }
            Value::Object(children) => {
                check_schema_nodes(visited, pending.len(), children.len())?;
                account_schema_bytes(&mut encoded_bytes, 2)?;
                account_schema_bytes(&mut encoded_bytes, children.len().saturating_sub(1))?;
                for key in children.keys() {
                    account_schema_string_bytes(&mut encoded_bytes, key)?;
                    account_schema_bytes(&mut encoded_bytes, 1)?;
                }
                pending.extend(children.values().map(|child| (child, depth + 1)));
            }
        }
    }
    // Count exact JSON punctuation/string bytes and Core-canonical number
    // bytes before recursive encoding can clone the complete supplied Value.
    let bytes =
        canonical_bytes(schema).map_err(|error| ResourceError::Validation(error.to_string()))?;
    if bytes.len() > MAX_ARTIFACT_TYPE_SCHEMA_BYTES {
        return Err(schema_byte_budget_error());
    }
    Ok(())
}

fn check_schema_nodes(visited: usize, pending: usize, children: usize) -> ResourceResult<()> {
    if visited
        .checked_add(pending)
        .and_then(|count| count.checked_add(children))
        .is_none_or(|count| count > MAX_ARTIFACT_TYPE_SCHEMA_NODES)
    {
        return Err(ResourceError::Validation(format!(
            "typed Artifact schema exceeds {MAX_ARTIFACT_TYPE_SCHEMA_NODES} JSON values"
        )));
    }
    Ok(())
}

fn account_schema_bytes(encoded_bytes: &mut usize, additional: usize) -> ResourceResult<()> {
    *encoded_bytes = encoded_bytes
        .checked_add(additional)
        .filter(|count| *count <= MAX_ARTIFACT_TYPE_SCHEMA_BYTES)
        .ok_or_else(schema_byte_budget_error)?;
    Ok(())
}

fn account_schema_string_bytes(encoded_bytes: &mut usize, text: &str) -> ResourceResult<()> {
    account_schema_bytes(encoded_bytes, 2)?;
    for character in text.chars() {
        let bytes = match character {
            '"' | '\\' | '\u{0008}' | '\t' | '\n' | '\u{000c}' | '\r' => 2,
            '\u{0000}'..='\u{001f}' => 6,
            character => character.len_utf8(),
        };
        account_schema_bytes(encoded_bytes, bytes)?;
    }
    Ok(())
}

fn schema_byte_budget_error() -> ResourceError {
    ResourceError::Validation(format!(
        "typed Artifact schema exceeds {MAX_ARTIFACT_TYPE_SCHEMA_BYTES} canonical bytes"
    ))
}

fn discard_schema(schema: &mut Value) {
    // Owned direct-Value ingress also owns rejection cleanup. Ordinary Value
    // drop is recursive and must not overflow after rejecting a deep schema.
    let mut pending = vec![schema.take()];
    while let Some(value) = pending.pop() {
        match value {
            Value::Array(children) => pending.extend(children),
            Value::Object(children) => pending.extend(children.into_iter().map(|(_, child)| child)),
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
}

fn validate_schema_references(schema: &Value) -> ResourceResult<()> {
    let mut pending = vec![schema];
    while let Some(schema) = pending.pop() {
        let Value::Object(object) = schema else {
            continue;
        };
        validate_schema_reference_keywords(object)?;
        pending.extend(schema_children(object).into_iter().map(|(child, _)| child));
    }
    Ok(())
}

fn validate_schema_reference_keywords(
    object: &serde_json::Map<String, Value>,
) -> ResourceResult<()> {
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
    Ok(())
}

fn schema_children(object: &serde_json::Map<String, Value>) -> Vec<(&Value, usize)> {
    let mut children = Vec::new();
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
            children.push((child, 1));
        }
    }
    for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let Some(Value::Array(values)) = object.get(keyword) {
            children.extend(values.iter().map(|child| (child, 2)));
        }
    }
    for keyword in [
        "$defs",
        "definitions",
        "properties",
        "patternProperties",
        "dependentSchemas",
    ] {
        if let Some(Value::Object(values)) = object.get(keyword) {
            children.extend(values.values().map(|child| (child, 2)));
        }
    }
    children
}

#[derive(Default)]
struct SchemaExpansionBudget {
    visits: usize,
    canonical_bytes: usize,
}

impl SchemaExpansionBudget {
    fn admit(&mut self, schema: &Value, depth: usize, ancestors: &[&Value]) -> ResourceResult<()> {
        if depth > MAX_ARTIFACT_TYPE_SCHEMA_DEPTH {
            return Err(ResourceError::Validation(format!(
                "typed Artifact schema reference-expanded depth exceeds {MAX_ARTIFACT_TYPE_SCHEMA_DEPTH}"
            )));
        }
        if ancestors
            .iter()
            .any(|ancestor| std::ptr::eq(*ancestor, schema))
        {
            return Err(ResourceError::Validation(
                "typed Artifact schemas do not admit recursive reference cycles".to_owned(),
            ));
        }
        self.reserve(0, 1)?;
        self.visits += 1;
        let bytes = canonical_bytes(schema)
            .map_err(|error| ResourceError::Validation(error.to_string()))?
            .len();
        self.canonical_bytes = self
            .canonical_bytes
            .checked_add(bytes)
            .filter(|count| *count <= MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES)
            .ok_or_else(|| {
                ResourceError::Validation(format!(
                    "typed Artifact schema reference expansion exceeds {MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES} cumulative canonical bytes"
                ))
            })?;
        Ok(())
    }

    fn reserve(&self, pending: usize, additional: usize) -> ResourceResult<()> {
        if self
            .visits
            .checked_add(pending)
            .and_then(|count| count.checked_add(additional))
            .is_none_or(|count| count > MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS)
        {
            return Err(ResourceError::Validation(format!(
                "typed Artifact schema reference expansion exceeds {MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS} schema visits"
            )));
        }
        Ok(())
    }
}

fn validate_schema_reference_graph(
    schema: &Value,
    registry: &Registry<'_>,
    base_uri: Uri<String>,
) -> ResourceResult<()> {
    let resolver = registry.resolver(base_uri);
    let mut pending = vec![(schema, resolver, 1_usize, Vec::new(), true)];
    let mut budget = SchemaExpansionBudget::default();
    while let Some((schema, resolver, depth, mut ancestors, enter_subresource)) = pending.pop() {
        budget.admit(schema, depth, &ancestors)?;
        ancestors.push(schema);
        let resolver = if enter_subresource {
            resolver
                .in_subresource(Draft::Draft202012.create_resource_ref(schema))
                .map_err(|error| schema_reference_error(&error))?
        } else {
            resolver
        };
        let Value::Object(object) = schema else {
            continue;
        };
        validate_schema_reference_keywords(object)?;
        let children = schema_children(object);
        budget.reserve(pending.len(), children.len())?;
        for (child, depth_cost) in children {
            pending.push((
                child,
                resolver.clone(),
                depth + depth_cost,
                ancestors.clone(),
                true,
            ));
        }
        for keyword in ["$ref", "$dynamicRef"] {
            if let Some(reference) = object.get(keyword).and_then(Value::as_str) {
                budget.reserve(pending.len(), 1)?;
                let lookup = resolver
                    .lookup(reference)
                    .map_err(|error| schema_reference_error(&error))?;
                let (target, target_resolver, _) = lookup.into_inner();
                pending.push((target, target_resolver, depth + 1, ancestors.clone(), false));
            }
        }
    }
    Ok(())
}

fn schema_reference_error(error: &jsonschema::ReferencingError) -> ResourceError {
    ResourceError::Validation(format!(
        "typed Artifact schema reference cannot resolve: {error}"
    ))
}

#[derive(Debug)]
struct DenyExternalSchemaReferences;

impl Retrieve for DenyExternalSchemaReferences {
    fn retrieve(
        &self,
        _uri: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("typed Artifact schema retrieval is forbidden".into())
    }
}

fn compile_schema(schema: &Value) -> ResourceResult<Validator> {
    let resource = Draft::Draft202012.create_resource_ref(schema);
    let base_uri = resource.id().unwrap_or("json-schema:///");
    let registry = Registry::new()
        .draft(Draft::Draft202012)
        .retriever(DenyExternalSchemaReferences)
        .add(base_uri, resource)
        .map_err(|error| schema_reference_error(&error))?
        .prepare()
        .map_err(|error| schema_reference_error(&error))?;
    let base_uri =
        jsonschema::uri::from_str(base_uri).map_err(|error| schema_reference_error(&error))?;
    validate_schema_reference_graph(schema, &registry, base_uri)?;
    Validator::options()
        .with_draft(Draft::Draft202012)
        .with_retriever(DenyExternalSchemaReferences)
        .with_registry(&registry)
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
        .map_err(|error| ResourceError::Integrity {
            code: "artifact_reference_invalid".to_owned(),
            message: error.to_string(),
        })?;
    let expected = artifact_ref(&artifact.reference.kind, &artifact.bytes).map_err(|error| {
        ResourceError::Integrity {
            code: "artifact_identity_invalid".to_owned(),
            message: error.to_string(),
        }
    })?;
    if artifact.reference != expected {
        return Err(ResourceError::Integrity {
            code: "artifact_bytes_identity_mismatch".to_owned(),
            message: format!(
                "Artifact ID {} does not match its identity version, kind, and bytes",
                artifact.reference.artifact_id
            ),
        });
    }
    Ok(())
}

fn decode_canonical_json(bytes: &[u8]) -> ResourceResult<Value> {
    let value: Value = cymule_core::decode_json(bytes)
        .map_err(|_| ResourceError::Validation("Artifact is not valid JSON".to_owned()))?;
    let canonical =
        canonical_bytes(&value).map_err(|error| ResourceError::Validation(error.to_string()))?;
    if canonical != bytes {
        return Err(ResourceError::Integrity {
            code: "typed_json_artifact_noncanonical".to_owned(),
            message: "typed JSON Artifact bytes are not canonical".to_owned(),
        });
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
