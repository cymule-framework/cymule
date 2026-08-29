use std::collections::BTreeMap;

use cymule::{
    DispatchPolicy, EffectProfile, Expression, FlowBuilder, MutationKind, PlanCandidate,
    ReconciliationMode,
};
use cymule_core::COMPONENT_OUTPUT_ARTIFACT_KIND;
use serde_json::json;

pub fn build() -> PlanCandidate {
    FlowBuilder::new(
        "hello_world",
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}}
        }),
        json!({
            "type": "object",
            "required": ["message"],
            "properties": {"message": {"type": "string"}}
        }),
    )
    .component(
        "example.greet",
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}}
        }),
        json!({
            "type": "object",
            "required": ["message"],
            "properties": {"message": {"type": "string"}}
        }),
        COMPONENT_OUTPUT_ARTIFACT_KIND,
        BTreeMap::new(),
    )
    .effect_contract(
        "example.capture",
        json!({
            "type": "object",
            "required": ["message"],
            "properties": {"message": {"type": "string"}}
        }),
        json!({}),
        EffectProfile {
            mutation: MutationKind::Mutating,
            dispatch: DispatchPolicy::OnScopeCommit,
            reconciliation: ReconciliationMode::Queryable,
            keyed_idempotency: true,
            irreversible: false,
        },
        BTreeMap::new(),
    )
    .call("call.greet", "example.greet", Expression::Input, "greeting")
    .effect(
        "effect.capture",
        "example.capture",
        Expression::Binding {
            name: "greeting".to_owned(),
        },
        "primary",
        None,
    )
    .finish(Expression::Binding {
        name: "greeting".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_first_flow_matches_the_published_ir() {
        let published: PlanCandidate = cymule_core::decode_json(include_bytes!("../flow.json"))
            .expect("published IR parses through the strict JSON decoder");
        assert_eq!(build(), published);
    }
}
