//! Plugin-owned Draft 2020-12 schema and fixture conformance.

use cymule_agent::AgentSession;
use serde_json::Value;

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

    let mut malformed = occurrence;
    malformed["provider"] = Value::String("must-not-enter-plugin-semantics".to_owned());
    assert!(!validator.is_valid(&malformed));
}
