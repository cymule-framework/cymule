//! Typed Artifact codec identity, boundary, and coexistence tests.

use std::collections::BTreeMap;

use cymule_core::{Machine, artifact_ref};
use cymule_resource::{
    ArtifactCodecCandidate, ArtifactCodecRegistry, ResourceCandidate, ResourceError,
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

fn registry() -> (ArtifactCodecRegistry, String) {
    let descriptor =
        ArtifactCodecCandidate::canonical_json("example.evaluation/1", evaluation_schema())
            .seal()
            .expect("codec seals");
    let codec_id = descriptor.codec_id.clone();
    let mut registry = ArtifactCodecRegistry::new();
    registry.register(descriptor).expect("codec registers");
    (registry, codec_id)
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
    let first = ArtifactCodecCandidate::canonical_json("example.evaluation/1", first_schema)
        .seal()
        .expect("first seals");
    let second = ArtifactCodecCandidate::canonical_json("example.evaluation/1", second_schema)
        .seal()
        .expect("second seals");
    assert_eq!(first.schema_digest, second.schema_digest);
    assert_eq!(first.codec_id, second.codec_id);

    let codec_id = first.codec_id.clone();
    let mut registry = ArtifactCodecRegistry::new();
    registry.register(first).expect("first registers");
    registry.register(second).expect("exact replay registers");
    let left = registry
        .put_canonical_json(&codec_id, &json!({"score": 7, "accepted": true}))
        .expect("left seals");
    let right = registry
        .put_canonical_json(&codec_id, &json!({"accepted": true, "score": 7}))
        .expect("right seals");
    assert_eq!(left, right);
    assert_eq!(left.bytes, br#"{"accepted":true,"score":7}"#);
    assert_eq!(
        left.reference,
        artifact_ref("example.evaluation/1", &left.bytes)
    );

    let mut machine = Machine::new();
    assert_eq!(
        machine.put_artifact("example.evaluation/1", left.bytes.clone()),
        left.reference
    );
}

#[test]
fn canonical_json_roundtrips_after_schema_validation() {
    let (registry, codec_id) = registry();
    let expected = Evaluation {
        score: 42,
        accepted: true,
    };
    let artifact = registry
        .put_canonical_json(&codec_id, &expected)
        .expect("valid value seals");
    assert_eq!(artifact.reference.kind, "example.evaluation/1");
    let decoded: Evaluation = registry
        .decode_typed(&codec_id, &artifact)
        .expect("typed value decodes");
    assert_eq!(decoded, expected);
}

#[test]
fn write_boundary_rejects_schema_violation_and_kind_rebinding() {
    let (mut registry, codec_id) = registry();
    let error = registry
        .put_canonical_json(&codec_id, &json!({"accepted": "yes", "score": 1}))
        .expect_err("schema violation fails");
    let ResourceError::Schema(issue) = error else {
        panic!("expected structured schema issue");
    };
    assert_eq!(issue.codec_id, codec_id);
    assert_eq!(issue.instance_path, "/accepted");
    assert_eq!(issue.schema_path, "/properties/accepted/type");

    let incompatible =
        ArtifactCodecCandidate::canonical_json("example.evaluation/1", json!({"type": "string"}))
            .seal()
            .expect("incompatible codec seals independently");
    assert!(matches!(
        registry.register(incompatible),
        Err(ResourceError::Conflict(_))
    ));
}

#[test]
fn read_boundary_rejects_wrong_kind_tamper_noncanonical_and_wrong_schema() {
    let (registry, codec_id) = registry();
    let valid = registry
        .put_canonical_json(&codec_id, &json!({"score": 3, "accepted": false}))
        .expect("valid value seals");

    let mut wrong_kind = valid.clone();
    wrong_kind.reference.kind = "example.other/1".to_owned();
    assert!(matches!(
        registry.decode_json(&codec_id, &wrong_kind),
        Err(ResourceError::Validation(_))
    ));

    let mut tampered = valid.clone();
    tampered.bytes[10] ^= 1;
    assert!(matches!(
        registry.decode_json(&codec_id, &tampered),
        Err(ResourceError::Integrity(_))
    ));

    let mut machine = Machine::new();
    let noncanonical_ref = machine.put_artifact(
        "example.evaluation/1",
        br#"{ "score": 3, "accepted": false }"#.to_vec(),
    );
    let noncanonical = machine
        .artifact(&noncanonical_ref)
        .expect("raw Artifact remains available");
    assert!(matches!(
        registry.decode_json(&codec_id, noncanonical),
        Err(ResourceError::Integrity(_))
    ));

    let wrong_schema_ref = machine.put_artifact(
        "example.evaluation/1",
        br#"{"accepted":"no","score":3}"#.to_vec(),
    );
    let wrong_schema = machine
        .artifact(&wrong_schema_ref)
        .expect("schema-invalid Artifact is retained as raw bytes");
    assert!(matches!(
        registry.decode_json(&codec_id, wrong_schema),
        Err(ResourceError::Schema(_))
    ));
}

#[test]
fn descriptor_tamper_and_external_schema_references_fail_closed() {
    let mut descriptor =
        ArtifactCodecCandidate::canonical_json("example.evaluation/1", evaluation_schema())
            .seal()
            .expect("codec seals");
    descriptor.schema = json!({"type": "string"});
    assert!(matches!(
        descriptor.verify(),
        Err(ResourceError::Integrity(_))
    ));

    for schema in [
        json!({"$ref": "https://example.com/schema.json"}),
        json!({"$dynamicRef": "other.json#item"}),
    ] {
        assert!(matches!(
            ArtifactCodecCandidate::canonical_json("example.external/1", schema).seal(),
            Err(ResourceError::Validation(_))
        ));
    }
}

#[test]
fn opaque_resource_and_artifact_bytes_do_not_require_a_codec() {
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
    let reference = machine.put_artifact("example.snapshot/1", bytes.to_vec());
    let artifact = machine
        .artifact(&reference)
        .expect("raw Artifact is retained");
    assert_eq!(artifact.bytes, bytes);

    let (registry, codec_id) = registry();
    assert!(matches!(
        registry.decode_json(&codec_id, artifact),
        Err(ResourceError::Validation(_))
    ));
}
