//! Typed Artifact contract identity, boundary, and coexistence tests.

use std::collections::BTreeMap;

use cymule_core::{Machine, artifact_ref};
use cymule_resource::{
    ArtifactTypeCandidate, ArtifactTypeRegistry, ResourceCandidate, ResourceError,
    ResourceIntegrity, ResourceLocation, ResourceShape,
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
        Err(ResourceError::Integrity(_))
    ));

    let mut tampered = valid.clone();
    tampered.bytes[10] ^= 1;
    assert!(matches!(
        registry.decode_json(&tampered),
        Err(ResourceError::Integrity(_))
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
        Err(ResourceError::Integrity(_))
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
        Err(ResourceError::Integrity(_))
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
        locations: vec![ResourceLocation::Resolver {
            binding: "binding:snapshot-store/1".to_owned(),
            reference: "snapshot:opaque".to_owned(),
        }],
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
