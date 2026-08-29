//! Bounded local schema-reference expansion before compiler admission.

use cymule_resource::{
    ArtifactTypeCandidate, ArtifactTypeRegistry, MAX_ARTIFACT_TYPE_SCHEMA_DEPTH,
    MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES, MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS, ResourceError,
};
use serde_json::{Value, json};

fn reference_chain_schema(definition_count: usize) -> Value {
    let definitions = (0..definition_count)
        .map(|index| {
            let schema = if index + 1 < definition_count {
                json!({"$ref": format!("#/$defs/node-{}", index + 1)})
            } else {
                Value::Bool(true)
            };
            (format!("node-{index}"), schema)
        })
        .collect::<serde_json::Map<_, _>>();
    json!({"$defs": definitions, "$ref": "#/$defs/node-0"})
}

#[test]
fn local_reference_expansion_accepts_depth_64_and_rejects_deeper_before_compilation() {
    let contract = ArtifactTypeCandidate::canonical_json(
        "example.ref-depth/1",
        reference_chain_schema(MAX_ARTIFACT_TYPE_SCHEMA_DEPTH - 2),
    )
    .seal()
    .expect("exact reference-expanded depth seals");
    contract.verify().expect("exact expanded depth verifies");
    for definition_count in [MAX_ARTIFACT_TYPE_SCHEMA_DEPTH - 1, 1_000] {
        assert!(matches!(
            ArtifactTypeCandidate::canonical_json(
                "example.ref-depth/1",
                reference_chain_schema(definition_count),
            )
            .seal(),
            Err(ResourceError::Validation(message)) if message.contains("reference-expanded depth")
        ));
    }
}

#[test]
fn local_reference_expansion_preserves_wide_shallow_schemas() {
    let properties = (0..1_000)
        .map(|index| {
            (
                format!("property-{index}"),
                json!({"$ref": "#/$defs/value"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let contract = ArtifactTypeCandidate::canonical_json(
        "example.wide-refs/1",
        json!({"$defs": {"value": true}, "type": "object", "properties": properties}),
    )
    .seal()
    .expect("a thousand shallow references remain within the expansion budget");
    let contract_id = contract.contract_id.clone();
    let mut registry = ArtifactTypeRegistry::new();
    registry
        .register(contract)
        .expect("wide contract registers");
    registry
        .put_canonical_json(&contract_id, &json!({"property-999": "value"}))
        .expect("wide local reference contract validates a typed value");
}

#[test]
fn local_reference_graph_uses_pinned_pointer_anchor_and_subresource_resolution() {
    let contract = ArtifactTypeCandidate::canonical_json(
        "example.resolved-refs/1",
        json!({
            "$id": "https://example.invalid/types/root",
            "$defs": {
                "number": {"$anchor": "number", "type": "integer"},
                "with space": {"type": "string"},
                "sub": {
                    "$id": "nested",
                    "$defs": {"leaf": {"type": "boolean"}},
                    "$ref": "#/$defs/leaf"
                }
            },
            "type": "object",
            "properties": {
                "number": {"$ref": "#number"},
                "text": {"$ref": "#/$defs/with%20space"},
                "flag": {"$ref": "#/$defs/sub"}
            }
        }),
    )
    .seal()
    .expect("local references use the compiler's exact URI resolution");
    let contract_id = contract.contract_id.clone();
    let mut registry = ArtifactTypeRegistry::new();
    registry
        .register(contract)
        .expect("resolved contract registers");
    registry
        .put_canonical_json(
            &contract_id,
            &json!({"number": 1, "text": "value", "flag": true}),
        )
        .expect("local URI forms retain their declared meaning");
    assert!(matches!(
        registry.put_canonical_json(&contract_id, &json!({"flag": "wrong"})),
        Err(ResourceError::Schema(_))
    ));
}

#[test]
fn recursive_and_unresolvable_local_references_fail_before_compilation() {
    for schema in [
        json!({"$ref": "#"}),
        json!({"$dynamicAnchor": "self", "$dynamicRef": "#self"}),
        json!({
            "$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}},
            "$ref": "#/$defs/a"
        }),
        json!({
            "$defs": {"node": {"properties": {"child": {"$ref": "#/$defs/node"}}}},
            "$ref": "#/$defs/node"
        }),
    ] {
        assert!(matches!(
            ArtifactTypeCandidate::canonical_json("example.recursive-refs/1", schema).seal(),
            Err(ResourceError::Validation(message)) if message.contains("recursive reference cycles")
        ));
    }
    for reference in ["#/$defs/missing", "#missing", "#/$defs/%ZZ"] {
        assert!(matches!(
            ArtifactTypeCandidate::canonical_json(
                "example.missing-refs/1", json!({"$ref": reference}),
            )
            .seal(),
            Err(ResourceError::Validation(message)) if message.contains("reference cannot resolve")
        ));
    }
}

#[test]
fn referenced_data_repeats_dialect_and_document_local_admission() {
    let hidden_absolute_reference = json!({
        "$defs": {"target": {"$id": "https://example.invalid/target", "type": "boolean"}},
        "const": {"$ref": "https://example.invalid/target"},
        "$ref": "#/const"
    });
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.hidden-ref/1", hidden_absolute_reference)
            .seal(),
        Err(ResourceError::Validation(message)) if message.contains("document-local")
    ));
    let hidden_dialect = json!({
        "const": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "boolean"},
        "$ref": "#/const"
    });
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.hidden-dialect/1", hidden_dialect).seal(),
        Err(ResourceError::Validation(message)) if message.contains("must use")
    ));
}

fn canonical_schema_len(schema: &Value) -> usize {
    cymule_core::canonical_bytes(schema)
        .expect("test schema canonicalizes")
        .len()
}

#[test]
fn local_reference_expansion_enforces_exact_cumulative_byte_budget() {
    const REFERENCE_COUNT: usize = 1_000;
    let mut schema = json!({
        "$comment": "",
        "$defs": {"payload": {"$comment": ""}},
        "allOf": vec![json!({"$ref": "#/$defs/payload"}); REFERENCE_COUNT]
    });
    let expanded_bytes = |schema: &Value| {
        canonical_schema_len(schema)
            + (REFERENCE_COUNT + 1) * canonical_schema_len(&schema["$defs"]["payload"])
            + REFERENCE_COUNT * canonical_schema_len(&schema["allOf"][0])
    };
    let remaining = MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES - expanded_bytes(&schema);
    schema["$defs"]["payload"]["$comment"] = json!("a".repeat(remaining / (REFERENCE_COUNT + 2)));
    schema["$comment"] = json!("a".repeat(remaining % (REFERENCE_COUNT + 2)));
    assert_eq!(
        expanded_bytes(&schema),
        MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES
    );
    let mut contract = ArtifactTypeCandidate::canonical_json("example.ref-bytes/1", schema)
        .seal()
        .expect("exact cumulative expansion byte budget seals");
    contract
        .verify()
        .expect("exact expansion byte budget verifies");
    let padding = contract.schema["$comment"]
        .as_str()
        .expect("test comment is a string");
    contract.schema["$comment"] = json!(format!("{padding}a"));
    assert_eq!(
        expanded_bytes(&contract.schema),
        MAX_ARTIFACT_TYPE_SCHEMA_EXPANDED_BYTES + 1
    );
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("cumulative canonical bytes")
    ));
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.ref-bytes/1", contract.schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("cumulative canonical bytes")
    ));
}

#[test]
fn local_reference_expansion_enforces_exact_schema_visit_budget() {
    const ROOT_REFERENCES: usize = 150;
    const GROUP_REFERENCES: usize = 216;
    let schema = json!({
        "$defs": {
            "group": {"allOf": vec![json!({"$ref": "#/$defs/leaf"}); GROUP_REFERENCES]},
            "leaf": true
        },
        "allOf": vec![json!({"$ref": "#/$defs/group"}); ROOT_REFERENCES],
        "not": false
    });
    let group_visits = 1 + 2 * GROUP_REFERENCES;
    let expanded_visits = 3 + group_visits + ROOT_REFERENCES * (1 + group_visits);
    assert_eq!(expanded_visits, MAX_ARTIFACT_TYPE_SCHEMA_EXPANSIONS);
    let mut contract = ArtifactTypeCandidate::canonical_json("example.ref-visits/1", schema)
        .seal()
        .expect("exact reference-expanded schema visit budget seals");
    contract
        .verify()
        .expect("exact schema visit budget verifies");
    contract.schema["if"] = Value::Bool(true);
    assert!(matches!(
        contract.verify(),
        Err(ResourceError::Validation(message)) if message.contains("schema visits")
    ));
    assert!(matches!(
        ArtifactTypeCandidate::canonical_json("example.ref-visits/1", contract.schema).seal(),
        Err(ResourceError::Validation(message)) if message.contains("schema visits")
    ));
}
