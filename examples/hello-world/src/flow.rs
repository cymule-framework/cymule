use cymule_sdk::{
    DispatchPolicy, EffectProfile, Expression, FlowBuilder, MutationKind, PlanCandidate,
    ReconciliationMode,
};
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
    )
    .call("call.greet", "example.greet", Expression::Input, "greeting")
    .effect(
        "effect.capture",
        "example.capture",
        Expression::Binding {
            name: "greeting".to_owned(),
        },
        "primary",
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
        let published: PlanCandidate =
            serde_json::from_str(include_str!("../flow.json")).expect("published IR parses");
        assert_eq!(build(), published);
    }
}
