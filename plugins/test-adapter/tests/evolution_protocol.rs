//! Real process-boundary conformance for the shared evolution wire.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::process::{Command, Output, Stdio};

use cymule_durable_protocol::{Continuation, ContinuationStatus, FrameState};
use cymule_evolution::{
    EvolutionPluginMigrationRequest, EvolutionPluginRequest, EvolutionPluginRequestEnvelope,
    EvolutionPluginResponse, EvolutionPluginResponseEnvelope, MigrationRequest, ShadowRequest,
};

const REVISION: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FROM_PLAN: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const TO_PLAN: &str = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
const PLAN_EDGE: &str = "sha256:3333333333333333333333333333333333333333333333333333333333333333";
const COMPATIBILITY: &str =
    "sha256:4444444444444444444444444444444444444444444444444444444444444444";

fn invoke(input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cymule-test-adapter"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("test adapter starts");
    child
        .stdin
        .take()
        .expect("adapter stdin exists")
        .write_all(input)
        .expect("request writes");
    child.wait_with_output().expect("adapter exits")
}

fn invoke_typed(request: EvolutionPluginRequest) -> EvolutionPluginResponse {
    let output = invoke(
        &serde_json::to_vec(&EvolutionPluginRequestEnvelope::new(REVISION, request))
            .expect("request serializes"),
    );
    assert!(
        output.status.success(),
        "typed evolution request failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let envelope: EvolutionPluginResponseEnvelope =
        cymule_core::decode_json(&output.stdout).expect("adapter emits one closed typed response");
    envelope.into_result().expect("adapter returns success")
}

fn migration_request() -> EvolutionPluginMigrationRequest {
    let run_id = "run:process-migration";
    let source_binding = cymule_core::artifact_ref(
        cymule_runtime::EXECUTION_BINDING_VERSION,
        b"process source binding",
    )
    .expect("source binding derives");
    let target_binding = cymule_core::artifact_ref(
        cymule_runtime::EXECUTION_BINDING_VERSION,
        b"process target binding",
    )
    .expect("target binding derives");
    let input_state =
        cymule_core::artifact_ref("cymule.test/migration-input/1", b"process source state")
            .expect("source state derives");
    let frame_input = cymule_core::artifact_ref(
        "cymule.test/frame-input/1",
        b"process migration frame input",
    )
    .expect("frame input derives");
    let continuation = Continuation {
        continuation_version: cymule_durable_protocol::CONTINUATION_STATE_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        plan_id: FROM_PLAN.to_owned(),
        binding_context: source_binding.artifact_id.clone(),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: cymule_core::plan_invocation_id(run_id, FROM_PLAN, "main", &[])
                .expect("source invocation derives"),
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: frame_input,
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: Some(input_state.clone()),
        wait_set: BTreeSet::new(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        epoch: 1,
        execution_fence: 1,
        execution_claim: None,
        status: ContinuationStatus::Ready,
    };
    EvolutionPluginMigrationRequest {
        intent: MigrationRequest {
            migration_id: "migration:process".to_owned(),
            run_id: run_id.to_owned(),
            from_plan: FROM_PLAN.to_owned(),
            to_plan: TO_PLAN.to_owned(),
            plan_edge_id: PLAN_EDGE.to_owned(),
            compatibility_id: COMPATIBILITY.to_owned(),
            expected_source_epoch: 1,
            adapter_id: "test.migration".to_owned(),
            adapter_revision: REVISION.to_owned(),
        },
        source_witness_id: format!("sha256:{}", "5".repeat(64)),
        source_continuation: continuation,
        input_state,
        source_binding,
        target_binding,
    }
}

#[test]
fn process_descriptor_and_shadow_execution_use_the_shared_closed_wire() {
    let descriptor = invoke_typed(EvolutionPluginRequest::DescribeMigration {});
    assert!(matches!(
        descriptor,
        EvolutionPluginResponse::MigrationDescriptor { descriptor }
            if descriptor.plan_edge_id == PLAN_EDGE
                && descriptor.compatibility_id == COMPATIBILITY
    ));

    let migrated = invoke_typed(EvolutionPluginRequest::Migrate {
        request: Box::new(migration_request()),
    });
    let EvolutionPluginResponse::Migrated { output } = migrated else {
        panic!("migration execution returned the wrong response variant");
    };
    assert_eq!(output.continuation.plan_id, TO_PLAN);
    assert_eq!(output.continuation.epoch, 2);
    assert_eq!(output.continuation.status, ContinuationStatus::Ready);
    assert_eq!(output.artifacts.len(), 1);
    for record in output.artifacts.iter().chain([&output.evidence]) {
        assert_eq!(
            record.reference,
            cymule_core::artifact_ref(record.reference.kind.as_str(), &record.bytes)
                .expect("migration Artifact identity rederives")
        );
    }

    let response = invoke_typed(EvolutionPluginRequest::ExecuteShadow {
        request: Box::new(ShadowRequest {
            comparison_id: "comparison:process".to_owned(),
            decision_id: "decision:process".to_owned(),
            subject: "occurrence:process".to_owned(),
            primary_plan: format!("sha256:{}", "6".repeat(64)),
            shadow_plan: format!("sha256:{}", "7".repeat(64)),
            input: cymule_core::artifact_ref("cymule.test/shadow-input/1", b"process input")
                .expect("input Artifact derives"),
            driver_id: "test.shadow".to_owned(),
            driver_revision: REVISION.to_owned(),
            comparison_policy: "policy:exact".to_owned(),
        }),
    });
    let EvolutionPluginResponse::ShadowExecuted { output } = response else {
        panic!("shadow execution returned the wrong response variant");
    };
    assert!(output.equivalent);
    assert_eq!(output.primary_digest, output.shadow_digest);
    assert_eq!(
        output.evidence.reference,
        cymule_core::artifact_ref(
            output.evidence.reference.kind.as_str(),
            &output.evidence.bytes,
        )
        .expect("evidence identity rederives")
    );
}

#[test]
fn valid_but_unsupported_migration_returns_the_bounded_failure_envelope() {
    let mut request = migration_request();
    request.intent.plan_edge_id = format!("sha256:{}", "9".repeat(64));
    let input = serde_json::to_vec(&EvolutionPluginRequestEnvelope::new(
        REVISION,
        EvolutionPluginRequest::Migrate {
            request: Box::new(request),
        },
    ))
    .expect("request serializes");
    let output = invoke(&input);
    assert!(output.status.success());
    let envelope: EvolutionPluginResponseEnvelope = cymule_core::decode_json(&output.stdout)
        .expect("adapter emits one closed failure response");
    assert!(matches!(
        envelope.into_result(),
        Err(cymule_evolution::EvolutionError::Substrate { code, message })
            if code == "test_adapter_failed" && !message.is_empty()
    ));
}

#[test]
fn process_ingress_fails_closed_for_malformed_evolution_envelopes() {
    for malformed in [
        format!(
            r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
        ),
        format!(
            r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/1","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
        ),
        format!(
            r#"{{"implementation_revision":"{REVISION}","request":{{"type":"describe_shadow"}}}}"#
        ),
        format!(
            r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow","unsafe":9007199254740992}}}}"#
        ),
        format!(
            r#"{{"evolution_plugin_protocol":"cymule.evolution-plugin/3","implementation_revision":"{REVISION}","request":{{"type":"describe_shadow","wrong":true}}}}"#
        ),
    ] {
        let output = invoke(malformed.as_bytes());
        assert!(
            !output.status.success(),
            "malformed process request was admitted: {malformed}"
        );
        assert!(
            output.stdout.is_empty(),
            "invalid ingress emitted authority"
        );
    }
}
