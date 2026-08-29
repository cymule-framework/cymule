//! Typed Artifact contract identity, boundary, and coexistence tests.

use std::collections::BTreeMap;

use cymule_core::{Machine, artifact_ref};
use cymule_resource::{
    ArtifactTypeCandidate, ArtifactTypeRegistry, FrameworkArtifactType,
    MAX_ARTIFACT_TYPE_SCHEMA_BYTES, MAX_ARTIFACT_TYPE_SCHEMA_DEPTH, MAX_ARTIFACT_TYPE_SCHEMA_NODES,
    ResourceCandidate, ResourceError, ResourceIntegrity, ResourceShape,
    framework_artifact_contracts,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Evaluation {
    score: u64,
    accepted: bool,
}

fn evaluation_schema() -> Value {
    json!({
        "$schema": cymule_resource::JSON_SCHEMA_DIALECT,
        "type": "object",
        "properties": {
            "accepted": {"type": "boolean"},
            "score": {"type": "integer", "minimum": 0}
        },
        "required": ["accepted", "score"],
        "additionalProperties": false
    })
}

fn registry() -> (ArtifactTypeRegistry, String) {
    let descriptor =
        ArtifactTypeCandidate::canonical_json("example.evaluation/1", evaluation_schema())
            .seal()
            .expect("contract seals");
    let contract_id = descriptor.contract_id.clone();
    let mut registry = ArtifactTypeRegistry::new();
    registry.register(descriptor).expect("contract registers");
    (registry, contract_id)
}

#[test]
fn descriptor_and_artifact_identity_are_canonical() {
    let first_schema = evaluation_schema();
    let second_schema: Value = serde_json::from_str(
        r#"{
          "additionalProperties": false,
          "required": ["accepted", "score"],
          "properties": {
            "score": {"minimum": 0, "type": "integer"},
            "accepted": {"type": "boolean"}
          },
          "type": "object",
          "$schema": "https://json-schema.org/draft/2020-12/schema"
        }"#,
    )
    .expect("schema parses");
    let first = ArtifactTypeCandidate::canonical_json("example.evaluation/1", first_schema)
        .seal()
        .expect("first seals");
    let second = ArtifactTypeCandidate::canonical_json("example.evaluation/1", second_schema)
        .seal()
        .expect("second seals");
    assert_eq!(first.schema_digest, second.schema_digest);
    assert_eq!(first.contract_id, second.contract_id);
    assert_eq!(
        first.contract_id,
        "sha256:a57f049eedfe4da8766fe31d38a5bdfa67995c40a989b34ee2ba98fb78ff29f2"
    );

    let contract_id = first.contract_id.clone();
    let mut registry = ArtifactTypeRegistry::new();
    registry.register(first).expect("first registers");
    registry.register(second).expect("exact replay registers");
    let left = registry
        .put_canonical_json(&contract_id, &json!({"score": 7, "accepted": true}))
        .expect("left seals");
    let right = registry
        .put_canonical_json(&contract_id, &json!({"accepted": true, "score": 7}))
        .expect("right seals");
    assert_eq!(left, right);
    assert_eq!(left.bytes, br#"{"accepted":true,"score":7}"#);
    assert_eq!(
        left.reference.artifact_id,
        "sha256:fe9daa1fcb12d9d3b0de31d1db619927f9eb95aefe318979df66dea498739b4a"
    );
    assert_eq!(left.reference.identity_version, "cymule.artifact/2");
    assert_eq!(
        left.reference.kind,
        format!(
            "cymule.typed-json/sha256-{}",
            contract_id.strip_prefix("sha256:").expect("digest")
        )
    );
    assert_eq!(
        left.reference,
        artifact_ref(&left.reference.kind, &left.bytes).expect("reference derives")
    );

    let mut machine = Machine::new();
    assert_eq!(
        machine
            .put_artifact(&left.reference.kind, left.bytes.clone())
            .expect("Machine stores"),
        left.reference
    );
}

#[test]
fn canonical_json_roundtrips_after_schema_validation() {
    let (registry, contract_id) = registry();
    let expected = Evaluation {
        score: 42,
        accepted: true,
    };
    let artifact = registry
        .put_canonical_json(&contract_id, &expected)
        .expect("valid value seals");
    assert!(artifact.reference.kind.starts_with("cymule.typed-json/"));
    let decoded: Evaluation = registry
        .decode_typed(&artifact)
        .expect("typed value decodes");
    assert_eq!(decoded, expected);
}

#[test]
fn write_boundary_rejects_schema_violation_and_kind_rebinding() {
    let (mut registry, contract_id) = registry();
    let error = registry
        .put_canonical_json(&contract_id, &json!({"accepted": "yes", "score": 1}))
        .expect_err("schema violation fails");
    let ResourceError::Schema(issue) = error else {
        panic!("expected structured schema issue");
    };
    assert_eq!(issue.contract_id, contract_id);
    assert_eq!(issue.instance_path, "/accepted");
    assert_eq!(issue.schema_path, "/properties/accepted/type");

    let incompatible =
        ArtifactTypeCandidate::canonical_json("example.evaluation/1", json!({"type": "string"}))
            .seal()
            .expect("incompatible contract seals independently");
    let incompatible_id = incompatible.contract_id.clone();
    registry
        .register(incompatible)
        .expect("immutable revisions coexist");
    let first = registry
        .put_canonical_json(&contract_id, &json!({"accepted": true, "score": 1}))
        .expect("first contract writes");
    let second = registry
        .put_canonical_json(&incompatible_id, &"value")
        .expect("second contract writes");
    assert_ne!(first.reference.kind, second.reference.kind);
    assert_ne!(first.reference.artifact_id, second.reference.artifact_id);
}

#[test]
fn read_boundary_rejects_wrong_kind_tamper_noncanonical_and_wrong_schema() {
    let (registry, contract_id) = registry();
    let valid = registry
        .put_canonical_json(&contract_id, &json!({"score": 3, "accepted": false}))
        .expect("valid value seals");

    let mut wrong_kind = valid.clone();
    wrong_kind.reference.kind = "example.other/1".to_owned();
    assert!(matches!(
        registry.decode_json(&wrong_kind),
        Err(ResourceError::Integrity { .. })
    ));

    let mut tampered = valid.clone();
    tampered.bytes[10] ^= 1;
    assert!(matches!(
        registry.decode_json(&tampered),
        Err(ResourceError::Integrity { .. })
    ));

    let mut machine = Machine::new();
    let noncanonical_ref = machine
        .put_artifact(
            valid.reference.kind.clone(),
            br#"{ "score": 3, "accepted": false }"#.to_vec(),
        )
        .expect("raw Artifact stores");
    let noncanonical = machine
        .artifact(&noncanonical_ref)
        .expect("raw Artifact remains available");
    assert!(matches!(
        registry.decode_json(noncanonical),
        Err(ResourceError::Integrity { .. })
    ));

    let wrong_schema_ref = machine
        .put_artifact(
            valid.reference.kind.clone(),
            br#"{"accepted":"no","score":3}"#.to_vec(),
        )
        .expect("raw Artifact stores");
    let wrong_schema = machine
        .artifact(&wrong_schema_ref)
        .expect("schema-invalid Artifact is retained as raw bytes");
    assert!(matches!(
        registry.decode_json(wrong_schema),
        Err(ResourceError::Schema(_))
    ));
}

#[test]
fn descriptor_tamper_and_external_schema_references_fail_closed() {
    let mut descriptor =
        ArtifactTypeCandidate::canonical_json("example.evaluation/1", evaluation_schema())
            .seal()
            .expect("contract seals");
    descriptor.schema = json!({"type": "string"});
    assert!(matches!(
        descriptor.verify(),
        Err(ResourceError::Integrity { .. })
    ));

    for schema in [
        json!({"$ref": "https://example.com/schema.json"}),
        json!({"$dynamicRef": "other.json#item"}),
        json!({"properties": {"value": {"$ref": "other.json"}}}),
    ] {
        assert!(matches!(
            ArtifactTypeCandidate::canonical_json("example.external/1", schema).seal(),
            Err(ResourceError::Validation(_))
        ));
    }

    ArtifactTypeCandidate::canonical_json(
        "example.data/1",
        json!({
            "type": "object",
            "properties": {"payload": {"const": {"$ref": "ordinary-data"}}}
        }),
    )
    .seal()
    .expect("$ref text inside const is ordinary instance data");
}

fn schema_with_depth(depth: usize) -> Value {
    (1..depth).fold(Value::Bool(true), |child, _| {
        Value::Object(serde_json::Map::from_iter([("not".to_owned(), child)]))
    })
}

#[test]
fn schema_byte_budget_accepts_exact_canonical_limit_and_rejects_one_more() {
    let empty_schema = json!({"$comment": ""});
    let overhead = cymule_core::canonical_bytes(&empty_schema)
        .expect("schema canonicalizes")
        .len();
    let schema = json!({"$comment": "a".repeat(MAX_ARTIFACT_TYPE_SCHEMA_BYTES - overhead)});
    let contract = ArtifactTypeCandidate::canonical_json("example.byte-budget/1", schema)
        .seal()
        .expect("exact canonical schema budget seals");
    assert_eq!(
        cymule_core::canonical_bytes(&contract.schema)
            .expect("bounded schema canonicalizes")
            .len(),
        MAX_ARTIFACT_TYPE_SCHEMA_BYTES,
    );
    contract.verify().expect("exact schema budget verifies");

    let mut oversized = contract;
    oversized.schema["$comment"] = json!("a".repeat(MAX_ARTIFACT_TYPE_SCHEMA_BYTES - overhead + 1));
    assert!(matches!(
        oversized.verify(),
        Err(ResourceError::Validation(message)) if message.contains("canonical bytes")
    ));
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.byte-budget/1", oversized.schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("canonical bytes")
    ));
}

#[test]
fn schema_byte_budget_counts_canonical_escapes_not_only_source_text() {
    let schema = json!({"$comment": "\u{0001}".repeat(MAX_ARTIFACT_TYPE_SCHEMA_BYTES / 6)});
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.escaped-budget/1", schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("canonical bytes")
    ));
}

#[test]
fn schema_size_preflight_matches_core_numbers_unicode_and_json_escapes() {
    let controls = (0_u8..=31).map(char::from).collect::<String>();
    let prefix = format!("{controls}\"\\界😀\u{2028}");
    let mut schema = json!({
        "$comment": prefix,
        "x-data": {"key\"\\界😀": [null, true, false, 1.0, -0.0, 0.000_000_1, cymule_core::MAX_EXACT_INTEGER]}
    });
    let current = cymule_core::canonical_bytes(&schema)
        .expect("mixed schema data canonicalizes")
        .len();
    schema["$comment"] = json!(format!(
        "{prefix}{}",
        "a".repeat(MAX_ARTIFACT_TYPE_SCHEMA_BYTES - current)
    ));
    assert_eq!(
        cymule_core::canonical_bytes(&schema)
            .expect("exact-limit mixed schema canonicalizes")
            .len(),
        MAX_ARTIFACT_TYPE_SCHEMA_BYTES,
    );
    let mut contract = ArtifactTypeCandidate::canonical_json("example.preflight-size/1", schema)
        .seal()
        .expect("preflight agrees with Core at the exact mixed-data limit");
    contract
        .verify()
        .expect("mixed exact-limit contract verifies");
    let comment = contract.schema["$comment"]
        .as_str()
        .expect("test comment is a string");
    contract.schema["$comment"] = json!(format!("{comment}a"));
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("canonical bytes")
    ));
}

#[test]
fn schema_node_budget_counts_data_values_and_rejects_one_more() {
    let schema = json!({"x-data": vec![Value::Null; MAX_ARTIFACT_TYPE_SCHEMA_NODES - 2]});
    let mut contract = ArtifactTypeCandidate::canonical_json("example.node-budget/1", schema)
        .seal()
        .expect("exact schema node budget seals");
    contract
        .verify()
        .expect("exact schema node budget verifies");
    contract.schema["x-data"]
        .as_array_mut()
        .expect("test schema data is an array")
        .push(Value::Null);
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("JSON values")
    ));
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.node-budget/1", contract.schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("JSON values")
    ));
}

#[test]
fn schema_depth_budget_accepts_exact_limit_and_rejects_one_more() {
    let mut contract = ArtifactTypeCandidate::canonical_json(
        "example.depth-budget/1",
        schema_with_depth(MAX_ARTIFACT_TYPE_SCHEMA_DEPTH),
    )
    .seal()
    .expect("exact schema depth budget seals");
    contract
        .verify()
        .expect("exact schema depth budget verifies");
    contract.schema = schema_with_depth(MAX_ARTIFACT_TYPE_SCHEMA_DEPTH + 1);
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("depth")
    ));
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.depth-budget/1", contract.schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("depth")
    ));
}

#[test]
fn directly_supplied_deep_schemas_reject_without_recursive_clone_or_drop() {
    const HOSTILE_DEPTH: usize = 32_768;
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json(
            "example.deep-budget/1",
            schema_with_depth(HOSTILE_DEPTH),
        )
        .seal(),
        Err(ResourceError::Validation(message)) if message.contains("depth")
    ));

    let mut contract =
        ArtifactTypeCandidate::canonical_json("example.deep-budget/1", Value::Bool(true))
            .seal()
            .expect("initial shallow schema seals");
    contract.schema = schema_with_depth(HOSTILE_DEPTH);
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("depth")
    ));
    let contract_id = contract.contract_id.clone();
    let mut registry = ArtifactTypeRegistry::new();
    assert!(matches!(
        registry.register(contract),
        Err(ResourceError::Validation(message)) if message.contains("depth")
    ));
    assert!(registry.descriptor(&contract_id).is_none());
}

#[test]
fn contract_artifact_rebuilds_registry_and_schema_errors_mask_values() {
    let contract = ArtifactTypeCandidate::canonical_json(
        "example.secret/1",
        json!({
            "type": "object",
            "properties": {"token": {"type": "integer"}},
            "required": ["token"],
            "additionalProperties": false
        }),
    )
    .seal()
    .expect("contract seals");
    let contract_id = contract.contract_id.clone();
    let contract_artifact = contract.artifact_record().expect("contract persists");
    assert_eq!(
        contract_artifact.reference.kind,
        cymule_resource::ARTIFACT_TYPE_CONTRACT_KIND
    );

    let mut rebuilt = ArtifactTypeRegistry::new();
    rebuilt
        .register_artifact(&contract_artifact)
        .expect("registry rebuilds from retained Artifact");
    let secret = "token-secret-must-not-leak";
    let error = rebuilt
        .put_canonical_json(&contract_id, &json!({"token": secret}))
        .expect_err("secret violates schema");
    assert!(matches!(error, ResourceError::Schema(_)));
    assert!(!error.to_string().contains(secret));
    assert!(!format!("{error:?}").contains(secret));
}

#[test]
fn opaque_resource_and_artifact_bytes_do_not_require_a_contract() {
    let bytes = b"opaque snapshot bytes";
    let resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Snapshot,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: format!("sha256:{}", cymule_core::sha256_bytes(bytes)),
            size: bytes.len() as u64,
        },
        manifest: None,
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("opaque Resource seals without schema");
    resource.verify().expect("opaque Resource verifies");

    let mut machine = Machine::new();
    let reference = machine
        .put_artifact("example.snapshot/1", bytes.to_vec())
        .expect("opaque Artifact stores");
    let artifact = machine
        .artifact(&reference)
        .expect("raw Artifact is retained");
    assert_eq!(artifact.bytes, bytes);

    let (registry, _) = registry();
    assert!(matches!(
        registry.decode_json(artifact),
        Err(ResourceError::Validation(_))
    ));
}

#[test]
fn framework_artifacts_use_closed_exact_contracts() {
    let contracts = framework_artifact_contracts().expect("framework contracts seal");
    assert_eq!(contracts.len(), 4);
    let ids: std::collections::BTreeSet<_> = contracts
        .iter()
        .map(|contract| contract.contract_id.as_str())
        .collect();
    assert_eq!(ids.len(), contracts.len());

    let registry =
        ArtifactTypeRegistry::with_framework_contracts().expect("framework registry compiles");
    let resource = ResourceCandidate::text("framework value")
        .seal()
        .expect("Resource seals");
    let resource_contract =
        cymule_resource::framework_artifact_contract(FrameworkArtifactType::ResourceHandle)
            .expect("Resource Handle contract seals");
    let typed_resource_kind = resource_contract
        .typed_artifact_kind()
        .expect("Resource Handle persisted kind derives");
    assert_eq!(
        resource_contract.contract_id,
        cymule_resource::resource_handle_artifact_contract_id()
            .expect("protocol contract ID derives")
    );
    assert_eq!(
        typed_resource_kind,
        cymule_resource::resource_handle_artifact_kind().expect("protocol typed kind derives")
    );
    assert_eq!(
        typed_resource_kind,
        format!(
            "cymule.typed-json/sha256-{}",
            resource_contract
                .contract_id
                .strip_prefix("sha256:")
                .expect("contract digest")
        )
    );
    assert_ne!(typed_resource_kind, resource_contract.artifact_kind);
    let artifact = registry
        .put_canonical_json(&resource_contract.contract_id, &resource)
        .expect("exact framework value seals");
    assert_eq!(artifact.reference.kind, typed_resource_kind);
    assert_eq!(
        cymule_resource::decode_resource_handle_artifact(&artifact)
            .expect("closed protocol decoder accepts the exact Artifact"),
        resource
    );
    assert_eq!(
        registry
            .decode_typed::<cymule_resource::ResourceHandle>(&artifact)
            .expect("framework value decodes"),
        resource
    );

    let mut widened = serde_json::to_value(&resource).expect("Resource serializes");
    widened.as_object_mut().expect("Resource is object").insert(
        "signed_url".to_owned(),
        json!("https://example.test/?token=secret"),
    );
    assert!(matches!(
        registry.put_canonical_json(&resource_contract.contract_id, &widened),
        Err(ResourceError::Schema(_))
    ));
}
