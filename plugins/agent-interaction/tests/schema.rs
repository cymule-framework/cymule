//! Plugin-owned Draft 2020-12 schema and fixture conformance.

use cymule_agent::AgentSession;
use serde_json::{Value, json};

#[test]
fn agent_plugin_schema_validates_owned_fixtures_and_rejects_provider_fields() {
    let schema: Value = serde_json::from_str(include_str!("../schemas/agent-protocol.schema.json"))
        .expect("Agent schema parses");
    let resource_schema: Value =
        serde_json::from_str(include_str!("../../../schemas/resource.schema.json"))
            .expect("Resource schema parses");
    let registry = jsonschema::Registry::new()
        .add(
            "https://cymule.dev/schemas/resource.schema.json",
            resource_schema,
        )
        .expect("Resource schema URI registers")
        .prepare()
        .expect("schema registry prepares");
    let validator = jsonschema::options()
        .with_registry(&registry)
        .build(&schema)
        .expect("Agent schema compiles");

    let occurrence: Value = serde_json::from_str(include_str!("fixtures/agent-occurrence.json"))
        .expect("occurrence fixture parses");
    validator
        .validate(&occurrence)
        .expect("occurrence fixture validates");
    let session: Value = serde_json::from_str(include_str!("fixtures/agent-session.json"))
        .expect("Session fixture parses");
    validator
        .validate(&session)
        .expect("Session fixture validates");
    assert_eq!(
        serde_json::from_value::<AgentSession>(session).expect("Session fixture deserializes"),
        AgentSession::new("session:fixture")
    );
    let records: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/agent-stream-records.json"))
            .expect("stream fixture parses");
    for record in &records {
        validator.validate(record).expect("stream record validates");
    }

    let mut artifact_occurrence = occurrence.clone();
    artifact_occurrence["response"]["response"]["content"] = json!([{
        "type": "artifact",
        "artifact": {
            "identity_version": "cymule.artifact/2",
            "artifact_id": format!("sha256:{}", "a".repeat(64)),
            "kind": "agent/output"
        }
    }]);
    validator
        .validate(&artifact_occurrence)
        .expect("canonical Artifact reference validates");
    for malformed_artifact in [
        json!({
            "artifact_id": format!("sha256:{}", "a".repeat(64)),
            "kind": "agent/output"
        }),
        json!({
            "identity_version": "cymule.artifact/1",
            "artifact_id": format!("sha256:{}", "a".repeat(64)),
            "kind": "agent/output"
        }),
        json!({
            "identity_version": "cymule.artifact/2",
            "artifact_id": "sha256:not-a-digest",
            "kind": "agent/output"
        }),
        json!({
            "identity_version": "cymule.artifact/2",
            "artifact_id": format!("sha256:{}", "A".repeat(64)),
            "kind": "agent/output"
        }),
        json!({
            "identity_version": "cymule.artifact/2",
            "artifact_id": format!("sha256:{}", "a".repeat(64)),
            "kind": "Invalid Kind"
        }),
    ] {
        let mut malformed = artifact_occurrence.clone();
        malformed["response"]["response"]["content"][0]["artifact"] = malformed_artifact;
        assert!(
            !validator.is_valid(&malformed),
            "Agent schema accepted a malformed Artifact reference"
        );
    }

    let mut malformed = occurrence;
    malformed["provider"] = Value::String("must-not-enter-plugin-semantics".to_owned());
    assert!(!validator.is_valid(&malformed));
}
