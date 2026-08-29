//! Plugin-owned Draft 2020-12 schema and hard-cut Serde conformance.

use cymule_agent::{
    AgentHostBinding, AgentHostOccurrence, AgentUpdate, ContentBlock, ElicitationProjection,
    ElicitationResponse, ToolCall, ToolCallStatus, ToolRequest, Usage, WorkspaceOccurrenceOwner,
    WorkspaceScopeRequest,
};
use cymule_core::decode_json;
use cymule_profile_protocol::agent::{
    AgentContextScanLimits, AgentHostOccurrenceState, AgentHostRequest, AgentMessagePage,
    AgentMessagePageQuery, AgentOccurrenceSource, AgentSessionCurrent, AgentStreamChunk,
    AgentStreamCommand, AgentStreamCurrent, AgentStreamDelivery, AgentStreamEffect,
    AgentStreamPublicationContent, AgentStreamPublicationIntent, AgentStreamResourceSource,
    AgentStreamSource, AgentStreamState, AgentStreamTarget, AgentStreamTargetSource,
    AgentWorkspaceCommand, AgentWorkspaceSource, ContextRequest, ContextSnapshot,
    MAX_AGENT_RECOVERY_OBSERVATIONS, MAX_AGENT_VALUE_ENTRIES, MessageRole, ModelRequest,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

fn agent_schema() -> Value {
    decode_json(include_bytes!("../schemas/agent-protocol.schema.json"))
        .expect("Agent schema parses through the strict decoder")
}

fn schema_validator(definition: &str) -> jsonschema::Validator {
    let resource_schema: Value =
        decode_json(include_bytes!("../../../schemas/resource.schema.json"))
            .expect("Resource schema parses through the strict decoder");
    let registry = jsonschema::Registry::new()
        .add(
            "https://cymule.dev/schemas/resource.schema.json",
            resource_schema,
        )
        .expect("Resource schema URI registers")
        .add(
            "https://cymule.dev/schemas/agent-protocol.schema.json",
            agent_schema(),
        )
        .expect("Agent schema URI registers")
        .prepare()
        .expect("schema registry prepares");
    jsonschema::options()
        .with_registry(&registry)
        .build(&json!({
            "$ref": format!(
                "https://cymule.dev/schemas/agent-protocol.schema.json#/$defs/{definition}"
            )
        }))
        .unwrap_or_else(|error| panic!("{definition} schema compiles: {error}"))
}

fn assert_required_nullable<T: DeserializeOwned>(
    definition: &str,
    type_name: &str,
    explicit_null: &Value,
    fields: &[&str],
) {
    serde_json::from_value::<T>(explicit_null.clone())
        .unwrap_or_else(|error| panic!("{type_name} rejected explicit null: {error}"));
    let validator = schema_validator(definition);
    validator
        .validate(explicit_null)
        .unwrap_or_else(|error| panic!("{type_name} schema rejected explicit null: {error}"));
    for field in fields {
        let mut missing = explicit_null.clone();
        missing
            .as_object_mut()
            .expect("required-nullable fixture is an object")
            .remove(*field);
        assert!(
            serde_json::from_value::<T>(missing.clone()).is_err(),
            "{type_name} accepted missing required-nullable field {field}"
        );
        assert!(
            !validator.is_valid(&missing),
            "{type_name} schema accepted missing required-nullable field {field}"
        );
    }
}

fn assert_required_integer<T: DeserializeOwned>(
    definition: &str,
    type_name: &str,
    valid: &Value,
    fields: &[&str],
) {
    serde_json::from_value::<T>(valid.clone())
        .unwrap_or_else(|error| panic!("{type_name} rejected valid integer fields: {error}"));
    let validator = schema_validator(definition);
    validator.validate(valid).unwrap_or_else(|error| {
        panic!("{type_name} schema rejected valid integer fields: {error}")
    });
    for field in fields {
        for invalid in [None, Some(Value::Null), Some(Value::String("1".to_owned()))] {
            let mut wire = valid.clone();
            match invalid {
                None => {
                    wire.as_object_mut()
                        .expect("required-integer fixture is an object")
                        .remove(*field);
                }
                Some(value) => wire[*field] = value,
            }
            assert!(
                serde_json::from_value::<T>(wire.clone()).is_err(),
                "{type_name} accepted invalid required integer {field}"
            );
            assert!(
                !validator.is_valid(&wire),
                "{type_name} schema accepted invalid required integer {field}"
            );
        }
    }
}

fn stream_current() -> Value {
    let open = AgentStreamCommand::Open {
        stream_id: "stream:schema".to_owned(),
        session_id: "session:schema".to_owned(),
        target: AgentStreamTarget::Message {
            message_id: "message:schema".to_owned(),
            role: MessageRole::Agent,
        },
        delivery: AgentStreamDelivery::Staged,
    };
    let opened = AgentStreamSource::Open {
        session: AgentSessionCurrent::new("session:schema").expect("schema Session constructs"),
        stream: None,
        target: AgentStreamTargetSource::Message { current: None },
    }
    .reduce(&format!("sha256:{}", "a".repeat(64)), &open)
    .expect("schema stream opens with its exact capacity counter");
    serde_json::to_value(opened.stream).expect("schema stream current encodes")
}

#[test]
fn schema_accepts_terminal_currents_and_rejects_removed_aggregate_records() {
    let schema = agent_schema();
    assert_eq!(schema["title"], "Cymule Agent Protocol cymule.agent/6");
    assert_eq!(
        schema["$id"],
        "https://cymule.dev/schemas/agent-protocol.schema.json"
    );
    let resource_schema: Value =
        decode_json(include_bytes!("../../../schemas/resource.schema.json"))
            .expect("Resource schema parses through the strict decoder");
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

    let session = serde_json::to_value(
        AgentSessionCurrent::new("session:schema").expect("Session current constructs"),
    )
    .expect("Session current encodes");
    validator
        .validate(&session)
        .expect("bounded Session current validates");
    let mut missing_tool_directory = session.clone();
    missing_tool_directory
        .as_object_mut()
        .expect("Session current is an object")
        .remove("nonterminal_tools");
    assert!(!validator.is_valid(&missing_tool_directory));
    let occurrence: Value = decode_json(include_bytes!("fixtures/agent-occurrence.json"))
        .expect("occurrence fixture parses through the strict decoder");
    validator
        .validate(&occurrence)
        .expect("host occurrence validates");
    let decoded: AgentHostOccurrence =
        serde_json::from_value(occurrence).expect("host occurrence decodes");
    decoded
        .validate()
        .expect("host occurrence semantics verify");
    let finalize = json!({
        "transition": "finalize",
        "session_id": "session:schema",
        "stream_id": "stream:schema"
    });
    validator
        .validate(&finalize)
        .expect("provider-free Finalize command validates");
    validator
        .validate(&stream_current())
        .expect("bounded stream current validates");

    assert!(!validator.is_valid(&json!({
        "session_id": "session:legacy",
        "state": "idle",
        "stop_reason": null,
        "messages": {},
        "message_order": [],
        "plan": null,
        "tools": {},
        "usage": null,
        "elicitations": {}
    })));
    assert!(!validator.is_valid(&json!({
        "record": "opened",
        "session_id": "session:legacy",
        "stream_id": "stream:legacy",
        "target": {
            "kind": "message",
            "message_id": "message:legacy",
            "role": "agent"
        }
    })));
}

#[test]
fn recovery_observation_schema_requires_its_content_identity() {
    let observed = AgentHostOccurrence::prepare(
        "occurrence:schema-observation",
        "session:schema-observation",
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:schema-observation".to_owned(),
            operation: "test.observe".to_owned(),
            input: json!({}),
        }),
        AgentHostBinding::standalone("host:schema-observation/1", "binding:schema-observation/1")
            .expect("observation binding constructs"),
    )
    .expect("observation occurrence prepares")
    .start()
    .expect("observation occurrence starts")
    .mark_unknown("dispatch response was lost")
    .expect("observation occurrence becomes unknown")
    .mark_unknown_with_evidence(
        "ignored replacement",
        vec![ContentBlock::Text {
            text: "provider readback remains inconclusive".to_owned(),
        }],
    )
    .expect("recovery observation appends");
    let validator = schema_validator("hostOccurrence");
    let mut wire = serde_json::to_value(observed).expect("observed occurrence encodes");
    validator
        .validate(&wire)
        .expect("identity-bound recovery observation validates");
    let mut missing_observations = wire.clone();
    missing_observations
        .as_object_mut()
        .expect("host occurrence is an object")
        .remove("recovery_observations");
    assert!(serde_json::from_value::<AgentHostOccurrence>(missing_observations.clone()).is_err());
    assert!(!validator.is_valid(&missing_observations));
    wire["recovery_observations"][0]
        .as_object_mut()
        .expect("recovery observation is an object")
        .remove("observation_id");
    assert!(!validator.is_valid(&wire));
}

#[test]
fn recovery_observation_capacity_preserves_the_terminal_slot() {
    let started = AgentHostOccurrence::prepare(
        "occurrence:schema-capacity",
        "session:schema-capacity",
        AgentHostRequest::Tool(ToolRequest {
            tool_call_id: "tool:schema-capacity".to_owned(),
            operation: "test.observe".to_owned(),
            input: json!({}),
        }),
        AgentHostBinding::standalone("host:test/1", "binding:schema-capacity/1")
            .expect("host binding constructs"),
    )
    .expect("occurrence prepares")
    .start()
    .expect("occurrence starts");
    let mut unknown = started
        .mark_unknown("dispatch response lost")
        .expect("Unknown constructs");
    for index in 0..MAX_AGENT_RECOVERY_OBSERVATIONS - 1 {
        unknown = unknown
            .mark_unknown_with_evidence(
                "ignored replacement",
                vec![ContentBlock::Text {
                    text: format!("unknown observation {index}"),
                }],
            )
            .expect("Unknown observation within the bound appends");
    }
    let validator = schema_validator("hostOccurrence");
    let mut wire = serde_json::to_value(&unknown).expect("Unknown occurrence encodes");
    validator
        .validate(&wire)
        .expect("63 Unknown observations validate");
    let extra = started
        .mark_unknown_with_evidence(
            "dispatch response lost",
            vec![ContentBlock::Text {
                text: "overflow observation".to_owned(),
            }],
        )
        .expect("extra observation is individually valid");
    wire["recovery_observations"]
        .as_array_mut()
        .expect("observation list is an array")
        .push(
            serde_json::to_value(&extra.recovery_observations[0])
                .expect("extra observation encodes"),
        );
    assert!(!validator.is_valid(&wire));
    assert!(
        serde_json::from_value::<AgentHostOccurrence>(wire)
            .expect("invalid-capacity snapshot decodes before validation")
            .validate()
            .is_err()
    );

    let terminal = unknown
        .mark_not_applied(vec![ContentBlock::Text {
            text: "provider proves no dispatch applied".to_owned(),
        }])
        .expect("NotApplied uses the reserved final slot");
    assert_eq!(
        terminal.recovery_observations.len(),
        MAX_AGENT_RECOVERY_OBSERVATIONS
    );
    let terminal_wire = serde_json::to_value(&terminal).expect("terminal occurrence encodes");
    validator
        .validate(&terminal_wire)
        .expect("64 observations including NotApplied validate");
    serde_json::from_value::<AgentHostOccurrence>(terminal_wire)
        .expect("terminal snapshot decodes")
        .validate()
        .expect("terminal snapshot verifies");
}

#[test]
fn stream_content_counter_is_required_and_bounded() {
    let validator = schema_validator("agentStreamCurrent");
    for field in ["staged_content_blocks", "final_update_bytes"] {
        let mut missing = stream_current();
        missing
            .as_object_mut()
            .expect("stream current is an object")
            .remove(field);
        assert!(!validator.is_valid(&missing));
        assert!(serde_json::from_value::<AgentStreamCurrent>(missing).is_err());
        let mut explicit_null = stream_current();
        explicit_null[field] = Value::Null;
        assert!(!validator.is_valid(&explicit_null));
        assert!(serde_json::from_value::<AgentStreamCurrent>(explicit_null).is_err());
    }
    let mut retired_name = stream_current();
    let final_update_bytes = retired_name["final_update_bytes"].clone();
    retired_name["staged_final_update_bytes"] = final_update_bytes;
    retired_name
        .as_object_mut()
        .expect("stream current is an object")
        .remove("final_update_bytes");
    assert!(!validator.is_valid(&retired_name));
    assert!(serde_json::from_value::<AgentStreamCurrent>(retired_name).is_err());

    let mut wire = stream_current();
    wire["next_chunk_sequence"] = json!(2);
    wire["chunk_head"] = json!(format!("sha256:{}", "b".repeat(64)));
    wire["staged_bytes"] = json!(8192);
    wire["staged_content_blocks"] = json!(MAX_AGENT_VALUE_ENTRIES);
    validator
        .validate(&wire)
        .expect("exact content count validates");
    serde_json::from_value::<AgentStreamCurrent>(wire.clone())
        .expect("exact-count stream decodes")
        .verify()
        .expect("exact-count current verifies");
    for count in [0, MAX_AGENT_VALUE_ENTRIES + 1] {
        wire["staged_content_blocks"] = json!(count);
        assert!(!validator.is_valid(&wire));
        assert!(
            serde_json::from_value::<AgentStreamCurrent>(wire.clone())
                .expect("invalid-count current decodes before validation")
                .verify()
                .is_err()
        );
    }
    let mut empty = stream_current();
    empty["staged_content_blocks"] = json!(1);
    assert!(!validator.is_valid(&empty));
    assert!(
        serde_json::from_value::<AgentStreamCurrent>(empty)
            .expect("empty current decodes before validation")
            .verify()
            .is_err()
    );
}

#[test]
fn external_stream_content_has_no_staged_payload_limit() {
    let validator = schema_validator("agentStreamCurrent");
    let mut wire = stream_current();
    wire["delivery"] = json!({
        "delivery": "external_resource",
        "resolver_binding": "resolver:schema/1",
        "content": {
            "media_type": "application/octet-stream",
            "digest": format!("sha256:{}", "c".repeat(64)),
            "size": 262_145
        }
    });
    wire["final_update_bytes"] = json!(1);
    validator
        .validate(&wire)
        .expect("external content is not a staged payload");
    serde_json::from_value::<AgentStreamCurrent>(wire.clone())
        .expect("external current decodes")
        .verify()
        .expect("external content larger than staging limit verifies");
    let mut missing_capacity = wire.clone();
    missing_capacity["final_update_bytes"] = json!(0);
    assert!(!validator.is_valid(&missing_capacity));
    assert!(
        serde_json::from_value::<AgentStreamCurrent>(missing_capacity)
            .expect("external current decodes before capacity validation")
            .verify()
            .is_err()
    );
    wire["staged_content_blocks"] = json!(1);
    assert!(!validator.is_valid(&wire));
    assert!(
        serde_json::from_value::<AgentStreamCurrent>(wire)
            .expect("external current decodes before validation")
            .verify()
            .is_err()
    );
}

#[test]
fn stream_final_update_capacity_has_the_exact_value_byte_bound() {
    let validator = schema_validator("agentStreamCurrent");
    let mut wire = stream_current();
    wire["next_chunk_sequence"] = json!(1);
    wire["chunk_head"] = json!(format!("sha256:{}", "b".repeat(64)));
    wire["staged_bytes"] = json!(260_000);
    wire["staged_content_blocks"] = json!(1);
    wire["final_update_bytes"] = json!(262_144);
    validator
        .validate(&wire)
        .expect("exact final-update capacity validates");
    serde_json::from_value::<AgentStreamCurrent>(wire.clone())
        .expect("exact-capacity current decodes")
        .verify()
        .expect("exact capacity verifies");
    for capacity in [0, 262_145] {
        wire["final_update_bytes"] = json!(capacity);
        assert!(!validator.is_valid(&wire));
        assert!(
            serde_json::from_value::<AgentStreamCurrent>(wire.clone())
                .expect("out-of-bound capacity decodes before validation")
                .verify()
                .is_err()
        );
    }
}

#[test]
fn stream_chunk_count_matches_its_terminal_content_bound() {
    let validator = schema_validator("agentStreamChunk");
    let mut chunk = AgentStreamChunk {
        sequence: 0,
        content: vec![
            ContentBlock::Text {
                text: "x".to_owned()
            };
            MAX_AGENT_VALUE_ENTRIES
        ],
    };
    chunk.verify().expect("exact-bound chunk verifies");
    validator
        .validate(&serde_json::to_value(&chunk).expect("chunk encodes"))
        .expect("exact-bound chunk schema validates");
    chunk.content.push(ContentBlock::Text {
        text: "one too many".to_owned(),
    });
    assert!(chunk.verify().is_err());
    assert!(!validator.is_valid(&serde_json::to_value(&chunk).expect("overflow chunk encodes")));
}

#[test]
fn every_terminal_required_nullable_member_rejects_omission() {
    assert_session_update_required_nullable();
    assert_occurrence_context_required_nullable();
    assert_stream_response_required_nullable();
}

fn assert_session_update_required_nullable() {
    let session = serde_json::to_value(
        AgentSessionCurrent::new("session:required-nullable").expect("Session current constructs"),
    )
    .expect("Session current encodes");
    assert_required_nullable::<AgentSessionCurrent>(
        "agentSessionCurrent",
        "AgentSessionCurrent",
        &session,
        &[
            "stop_reason",
            "plan",
            "usage",
            "message_head",
            "last_transition",
        ],
    );

    let usage = json!({"used": 0, "capacity": 1, "cost": null});
    assert_required_nullable::<Usage>("usage", "Usage", &usage, &["cost"]);

    let update = json!({
        "type": "state",
        "update_id": "update:required-nullable",
        "state": "running",
        "stop_reason": null
    });
    assert_required_nullable::<AgentUpdate>(
        "agentUpdate",
        "AgentUpdate::State",
        &update,
        &["stop_reason"],
    );

    let tool = json!({
        "tool_call_id": "tool:required-nullable",
        "operation": "workspace.read",
        "status": "in_progress",
        "input": {},
        "output": null,
        "locations": []
    });
    assert_required_nullable::<ToolCall>("toolCall", "ToolCall", &tool, &["output"]);

    let elicitation = json!({
        "wait_id": "wait:required-nullable",
        "request": {
            "request_id": "elicitation:required-nullable",
            "schema": {"type": "string"},
            "prompt": []
        },
        "response": null
    });
    assert_required_nullable::<ElicitationProjection>(
        "elicitationProjection",
        "ElicitationProjection",
        &elicitation,
        &["response"],
    );

    let owner = json!({
        "run_id": "run:required-nullable",
        "scope_id": "scope:required-nullable",
        "invocation_id": "invocation:required-nullable",
        "site_id": "workspace.finalize",
        "occurrence_key": "primary",
        "operation": "workspace.commit",
        "effect_intent_id": null
    });
    assert_required_nullable::<WorkspaceOccurrenceOwner>(
        "workspaceOccurrenceOwner",
        "WorkspaceOccurrenceOwner",
        &owner,
        &["effect_intent_id"],
    );

    let workspace = json!({
        "session_id": "session:required-nullable-workspace",
        "run_id": "run:required-nullable-workspace",
        "scope_id": "scope:required-nullable-workspace",
        "occurrence_id": "occurrence:required-nullable-workspace",
        "change_id": "change:required-nullable-workspace",
        "overlay": {
            "identity_version": "cymule.artifact/2",
            "artifact_id": format!("sha256:{}", "a".repeat(64)),
            "kind": "workspace/overlay"
        },
        "operation": "workspace.commit",
        "invocation_id": "invocation:required-nullable-workspace",
        "site_id": "site:required-nullable-workspace",
        "occurrence_key": "primary",
        "dispatch_lease": null
    });
    assert_required_nullable::<WorkspaceScopeRequest>(
        "workspaceScopeRequest",
        "WorkspaceScopeRequest",
        &workspace,
        &["dispatch_lease"],
    );
}

fn assert_occurrence_context_required_nullable() {
    let mut occurrence: Value = decode_json(include_bytes!("fixtures/agent-occurrence.json"))
        .expect("occurrence fixture parses through the strict decoder");
    occurrence["state"] = json!("prepared");
    occurrence["response"] = Value::Null;
    occurrence["failure"] = Value::Null;
    occurrence["recovery_observations"] = json!([]);
    assert_required_nullable::<AgentHostOccurrence>(
        "hostOccurrence",
        "AgentHostOccurrence",
        &occurrence,
        &["response", "failure"],
    );

    let context_request = json!({
        "session_id": "session:required-nullable",
        "source_message_head": null,
        "source_message_count": 0,
        "budget": 1,
        "scan_limits": {
            "max_entries": 1,
            "max_canonical_bytes": 1024
        }
    });
    assert_required_nullable::<ContextRequest>(
        "contextRequest",
        "ContextRequest",
        &context_request,
        &["source_message_head"],
    );
    let context_snapshot = json!({
        "snapshot_id": "snapshot:required-nullable",
        "source_message_head": null,
        "source_message_count": 0,
        "selected_messages": [],
        "content": [],
        "occurrence_binding": "binding:required-nullable/1"
    });
    assert_required_nullable::<ContextSnapshot>(
        "contextSnapshot",
        "ContextSnapshot",
        &context_snapshot,
        &["source_message_head"],
    );
}

#[test]
fn context_source_count_is_required_safe_and_coupled_to_its_head() {
    let request = json!({
        "session_id": "session:source-count",
        "source_message_head": null,
        "source_message_count": 0,
        "budget": 1,
        "scan_limits": {
            "max_entries": 1,
            "max_canonical_bytes": 1024
        }
    });
    assert_required_integer::<ContextRequest>(
        "contextRequest",
        "ContextRequest",
        &request,
        &["source_message_count"],
    );
    let snapshot = json!({
        "snapshot_id": "snapshot:source-count",
        "source_message_head": null,
        "source_message_count": 0,
        "selected_messages": [],
        "content": [],
        "occurrence_binding": "binding:source-count/1"
    });
    assert_required_integer::<ContextSnapshot>(
        "contextSnapshot",
        "ContextSnapshot",
        &snapshot,
        &["source_message_count"],
    );

    for (definition, valid) in [("contextRequest", request), ("contextSnapshot", snapshot)] {
        let validator = schema_validator(definition);
        let mut zero_with_head = valid.clone();
        zero_with_head["source_message_head"] = json!(format!("sha256:{}", "a".repeat(64)));
        assert!(!validator.is_valid(&zero_with_head));
        let mut count_without_head = valid.clone();
        count_without_head["source_message_count"] = json!(1);
        assert!(!validator.is_valid(&count_without_head));
        let mut exact_prefix = count_without_head;
        exact_prefix["source_message_head"] = json!(format!("sha256:{}", "b".repeat(64)));
        validator
            .validate(&exact_prefix)
            .expect("positive source count with a SHA-256 head validates");
        let mut unsafe_count = exact_prefix;
        unsafe_count["source_message_count"] = json!(9_007_199_254_740_992_u64);
        assert!(!validator.is_valid(&unsafe_count));
    }

    let mut typed_request: ContextRequest = serde_json::from_value(json!({
        "session_id": "session:typed-source-count",
        "source_message_head": null,
        "source_message_count": 0,
        "budget": 1,
        "scan_limits": {"max_entries": 1, "max_canonical_bytes": 1024}
    }))
    .expect("typed Context request decodes");
    typed_request.source_message_count = 1;
    assert!(
        AgentHostRequest::Context(typed_request)
            .validate_for_session("session:typed-source-count")
            .is_err()
    );
}

#[test]
fn context_selected_message_digest_is_an_exact_sha256_content_id() {
    let selected_snapshot = json!({
        "snapshot_id": "snapshot:selected-source-count",
        "source_message_head": format!("sha256:{}", "d".repeat(64)),
        "source_message_count": 1,
        "selected_messages": [{
            "index": 0,
            "message_id": "message:selected-source-count",
            "message_digest": format!("sha256:{}", "e".repeat(64))
        }],
        "content": [],
        "occurrence_binding": "binding:selected-source-count/1"
    });
    schema_validator("contextSnapshot")
        .validate(&selected_snapshot)
        .expect("non-empty Context snapshot accepts the exact sha256 message digest");
    let typed_snapshot: ContextSnapshot = serde_json::from_value(selected_snapshot.clone())
        .expect("non-empty Context snapshot decodes");
    AgentHostRequest::Model(ModelRequest {
        session_id: "session:selected-source-count".to_owned(),
        context: typed_snapshot,
        tools: Vec::new(),
    })
    .validate_for_session("session:selected-source-count")
    .expect("non-empty Context snapshot verifies in the Model request");
    for malformed in ["f".repeat(64), format!("sha512:{}", "f".repeat(64))] {
        let mut invalid = selected_snapshot.clone();
        invalid["selected_messages"][0]["message_digest"] = json!(malformed);
        assert!(!schema_validator("contextSnapshot").is_valid(&invalid));
        let typed: ContextSnapshot =
            serde_json::from_value(invalid).expect("digest string decodes before validation");
        assert!(
            AgentHostRequest::Model(ModelRequest {
                session_id: "session:selected-source-count".to_owned(),
                context: typed,
                tools: Vec::new(),
            })
            .validate_for_session("session:selected-source-count")
            .is_err()
        );
    }
}

#[test]
fn message_page_source_and_split_budgets_are_required_and_bounded() {
    let query = json!({
        "session_id": "session:page-shape",
        "expected_message_head": null,
        "source_message_count": 0,
        "end_exclusive": null,
        "max_entries": 1,
        "max_message_canonical_bytes": 4_194_304,
        "max_canonical_bytes": 4_194_304,
        "expected_revision": null
    });
    assert_required_integer::<AgentMessagePageQuery>(
        "agentMessagePageQuery",
        "AgentMessagePageQuery",
        &query,
        &["source_message_count", "max_message_canonical_bytes"],
    );
    let page = json!({
        "session_id": "session:page-shape",
        "expected_message_head": null,
        "source_message_count": 0,
        "end_exclusive": null,
        "entries": [],
        "next_end_exclusive": null
    });
    assert_required_integer::<AgentMessagePage>(
        "agentMessagePage",
        "AgentMessagePage",
        &page,
        &["source_message_count"],
    );

    let query_validator = schema_validator("agentMessagePageQuery");
    for capacity in [0_u64, 4 * 1024 * 1024 + 1] {
        let mut invalid = query.clone();
        invalid["max_message_canonical_bytes"] = json!(capacity);
        assert!(!query_validator.is_valid(&invalid));
        assert!(
            serde_json::from_value::<AgentMessagePageQuery>(invalid)
                .expect("numeric page budget decodes before semantic validation")
                .verify()
                .is_err()
        );
    }
    for definition in ["agentMessagePageQuery", "agentMessagePage"] {
        let valid = if definition == "agentMessagePageQuery" {
            query.clone()
        } else {
            page.clone()
        };
        let validator = schema_validator(definition);
        let mut zero_with_head = valid.clone();
        zero_with_head["expected_message_head"] = json!(format!("sha256:{}", "c".repeat(64)));
        assert!(!validator.is_valid(&zero_with_head));
        let mut count_without_head = valid;
        count_without_head["source_message_count"] = json!(1);
        assert!(!validator.is_valid(&count_without_head));
    }
    let mut page_with_budget = page;
    page_with_budget["max_message_canonical_bytes"] = json!(1);
    assert!(!schema_validator("agentMessagePage").is_valid(&page_with_budget));
    assert!(serde_json::from_value::<AgentMessagePage>(page_with_budget).is_err());
}

fn assert_stream_response_required_nullable() {
    assert_required_nullable::<AgentStreamCurrent>(
        "agentStreamCurrent",
        "AgentStreamCurrent",
        &stream_current(),
        &[
            "chunk_head",
            "final_update",
            "content_digest",
            "abort_reason",
        ],
    );

    let response = json!({
        "request_id": "elicitation:required-nullable-response",
        "accepted": false,
        "value": null,
        "occurrence_binding": "binding:required-nullable-response/1"
    });
    assert_required_nullable::<ElicitationResponse>(
        "elicitationResponse",
        "ElicitationResponse",
        &response,
        &["value"],
    );

    let accepted_null = json!({
        "request_id": "elicitation:accepted-null",
        "accepted": true,
        "value": null,
        "occurrence_binding": "binding:accepted-null/1"
    });
    serde_json::from_value::<ElicitationResponse>(accepted_null.clone())
        .expect("accepted JSON null remains distinct from an omitted value member");
    schema_validator("elicitationResponse")
        .validate(&accepted_null)
        .expect("schema accepts an explicit JSON null response value");
}

#[test]
fn tool_status_schema_matches_the_complete_closed_rust_union() {
    let validator = schema_validator("toolCall");
    for status in [
        ToolCallStatus::Pending,
        ToolCallStatus::AwaitingPermission,
        ToolCallStatus::InProgress,
        ToolCallStatus::Completed,
        ToolCallStatus::Failed,
        ToolCallStatus::Cancelled,
    ] {
        let output = matches!(
            status,
            ToolCallStatus::Completed | ToolCallStatus::Failed | ToolCallStatus::Cancelled
        )
        .then(|| {
            vec![cymule_agent::ContentBlock::Text {
                text: "terminal evidence".to_owned(),
            }]
        });
        let tool = ToolCall {
            tool_call_id: "tool:closed-status".to_owned(),
            operation: "workspace.read".to_owned(),
            status,
            input: json!({}),
            output,
            locations: Vec::new(),
        };
        let wire = serde_json::to_value(&tool).expect("ToolCall encodes");
        validator
            .validate(&wire)
            .unwrap_or_else(|error| panic!("schema rejected {status:?}: {error}"));
    }

    assert!(!validator.is_valid(&json!({
        "tool_call_id": "tool:removed-status",
        "operation": "workspace.read",
        "status": "requested",
        "input": {},
        "output": null,
        "locations": []
    })));
}

#[test]
fn provider_products_cannot_enter_serialized_commands_or_sources() {
    let forged_publication = json!({
        "publication_version": "cymule.resource-publication/1"
    });
    let finalize = json!({
        "transition": "finalize",
        "session_id": "session:provider-hard-cut",
        "stream_id": "stream:provider-hard-cut",
        "publication": forged_publication
    });
    assert!(serde_json::from_value::<AgentStreamCommand>(finalize.clone()).is_err());
    assert!(!schema_validator("agentStreamCommand").is_valid(&finalize));

    let workspace = json!({
        "transition": "settle_effect",
        "request": {
            "session_id": "session:workspace",
            "run_id": "run:workspace",
            "scope_id": "scope:workspace",
            "occurrence_id": "occurrence:workspace",
            "change_id": "change:workspace",
            "overlay": {
                "identity_version": "cymule.artifact/2",
                "artifact_id": format!("sha256:{}", "b".repeat(64)),
                "kind": "workspace/overlay"
            },
            "operation": "workspace.commit",
            "invocation_id": "invocation:workspace",
            "site_id": "site:workspace",
            "occurrence_key": "primary"
        },
        "resolution": "applied",
        "occurrence": {}
    });
    assert!(serde_json::from_value::<AgentWorkspaceCommand>(workspace).is_err());

    let workspace_source = AgentWorkspaceSource {
        occurrence: AgentOccurrenceSource {
            session: AgentSessionCurrent::new("session:workspace-source")
                .expect("Session current constructs"),
            current: None,
        },
    };
    let mut serialized = serde_json::to_value(workspace_source).expect("workspace source encodes");
    serialized
        .as_object_mut()
        .expect("workspace source is an object")
        .insert(
            "authority".to_owned(),
            json!({"authority": "observed", "resolution": {"resolution": "unknown", "evidence": []}}),
        );
    assert!(serde_json::from_value::<AgentWorkspaceSource>(serialized).is_err());

    let open = AgentStreamCommand::Open {
        session_id: "session:stream-source".to_owned(),
        stream_id: "stream:stream-source".to_owned(),
        target: AgentStreamTarget::Message {
            message_id: "message:stream-source".to_owned(),
            role: MessageRole::Agent,
        },
        delivery: AgentStreamDelivery::ExternalResource {
            resolver_binding: "resolver:stream-source".to_owned(),
            content: AgentStreamPublicationContent {
                media_type: "application/octet-stream".to_owned(),
                digest: format!("sha256:{}", "a".repeat(64)),
                size: 1,
            },
        },
    };
    let opened = AgentStreamSource::Open {
        session: AgentSessionCurrent::new("session:stream-source")
            .expect("Session current constructs"),
        stream: None,
        target: AgentStreamTargetSource::Message { current: None },
    }
    .reduce(&format!("sha256:{}", "c".repeat(64)), &open)
    .expect("external stream opens without provider I/O");
    let AgentStreamEffect::Opened { session } = opened.effect else {
        panic!("open effect shape changed")
    };
    let source = AgentStreamSource::Finalize {
        session,
        stream: opened.stream,
        chunks: Vec::new(),
        target: AgentStreamTargetSource::Message { current: None },
        update: None,
        resource: Some(Box::new(AgentStreamResourceSource {
            retention: None,
            pin: None,
        })),
    };
    let mut serialized = serde_json::to_value(source).expect("stream source encodes");
    serialized
        .as_object_mut()
        .expect("stream source is an object")
        .insert("publication".to_owned(), forged_publication);
    assert!(serde_json::from_value::<AgentStreamSource>(serialized).is_err());
}

#[test]
fn external_stream_delivery_requires_immutable_content() {
    let missing_content = json!({
        "transition": "open",
        "session_id": "session:missing-content",
        "stream_id": "stream:missing-content",
        "target": {
            "kind": "message",
            "message_id": "message:missing-content",
            "role": "agent"
        },
        "delivery": {
            "delivery": "external_resource",
            "resolver_binding": "resolver:missing-content/1"
        }
    });
    assert!(serde_json::from_value::<AgentStreamCommand>(missing_content.clone()).is_err());
    assert!(!schema_validator("agentStreamCommand").is_valid(&missing_content));
}

#[test]
fn publication_intent_wire_requires_source_and_content_authority() {
    let intent = json!({
        "intent_version": "cymule.agent-stream-publication-intent/1",
        "intent_id": format!("sha256:{}", "a".repeat(64)),
        "source_revision": format!("sha256:{}", "b".repeat(64)),
        "source_digest": "c".repeat(64),
        "session_id": "session:intent-wire",
        "stream_id": "stream:intent-wire",
        "command_id": format!("sha256:{}", "d".repeat(64)),
        "resolver_binding": "resolver:intent-wire/1",
        "target": {
            "kind": "message",
            "message_id": "message:intent-wire",
            "role": "agent"
        },
        "content": {
            "media_type": "application/octet-stream",
            "digest": format!("sha256:{}", "e".repeat(64)),
            "size": 1
        }
    });
    schema_validator("agentStreamPublicationIntent")
        .validate(&intent)
        .expect("complete publication intent wire validates");
    serde_json::from_value::<AgentStreamPublicationIntent>(intent.clone())
        .expect("complete publication intent wire decodes");
    for member in ["source_revision", "source_digest", "content"] {
        let mut missing = intent.clone();
        missing
            .as_object_mut()
            .expect("publication intent is an object")
            .remove(member);
        assert!(!schema_validator("agentStreamPublicationIntent").is_valid(&missing));
        assert!(serde_json::from_value::<AgentStreamPublicationIntent>(missing).is_err());
    }
}

#[test]
fn bounded_query_limits_are_not_silently_normalized() {
    let query = AgentMessagePageQuery {
        session_id: "session:query".to_owned(),
        expected_message_head: None,
        source_message_count: 0,
        end_exclusive: None,
        max_entries: 257,
        max_message_canonical_bytes: 4 * 1024 * 1024,
        max_canonical_bytes: 4 * 1024 * 1024,
        expected_revision: None,
    };
    assert!(query.verify().is_err());
    let request = ContextRequest {
        session_id: "session:query".to_owned(),
        source_message_head: None,
        source_message_count: 0,
        budget: 1,
        scan_limits: AgentContextScanLimits {
            max_entries: 4097,
            max_canonical_bytes: 16 * 1024 * 1024,
        },
    };
    assert!(
        AgentHostRequest::Context(request)
            .validate_for_session("session:query")
            .is_err()
    );

    let mut invalid_stream: AgentStreamCurrent =
        serde_json::from_value(stream_current()).expect("stream current decodes");
    invalid_stream.state = AgentStreamState::Finalized;
    assert!(invalid_stream.verify().is_err());
    let mut invalid_occurrence: AgentHostOccurrence =
        decode_json(include_bytes!("fixtures/agent-occurrence.json"))
            .expect("occurrence fixture decodes");
    invalid_occurrence.state = AgentHostOccurrenceState::Started;
    assert!(invalid_occurrence.validate().is_err());
}
