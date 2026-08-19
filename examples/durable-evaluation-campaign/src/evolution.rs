use std::collections::BTreeMap;

use cymule_core::{
    ComponentContract, Definition, Expression, IR_VERSION, Operation, PlanCandidate, Region, Step,
};
use cymule_evolution::{DefinitionRegistry, PlanTemplate, SubflowReference};
use serde_json::{Value, json};

use crate::plugin::{SCORER_COMPONENT, SUBJECT_COMPONENT};

pub const SCORER_REF: &str = "example.scorer";
pub const TEMPLATE_ID: &str = "example.evaluation-campaign";

pub fn scorer_definition(policy: &str, compatible: bool) -> Definition {
    Definition {
        id: "score".to_owned(),
        input_schema: if compatible {
            json!({})
        } else {
            json!({"type": "string"})
        },
        output_schema: json!({}),
        body: Region {
            steps: vec![Step {
                id: "score.case".to_owned(),
                operation: Operation::Call {
                    component: SCORER_COMPONENT.to_owned(),
                    input: Expression::Object {
                        fields: BTreeMap::from([
                            ("evaluation".to_owned(), Expression::Input),
                            (
                                "policy".to_owned(),
                                Expression::Literal {
                                    value: Value::String(policy.to_owned()),
                                },
                            ),
                        ]),
                    },
                    bind: Some("score".to_owned()),
                },
            }],
            result: Expression::Binding {
                name: "score".to_owned(),
            },
        },
    }
}

pub fn campaign_template() -> PlanTemplate {
    PlanTemplate {
        template_id: TEMPLATE_ID.to_owned(),
        candidate: PlanCandidate {
            ir_version: IR_VERSION.to_owned(),
            name: "durable_evaluation_campaign".to_owned(),
            entry: "main".to_owned(),
            components: vec![
                ComponentContract {
                    id: SUBJECT_COMPONENT.to_owned(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    requirements: BTreeMap::from([(
                        "capability".to_owned(),
                        "evaluation-subject".to_owned(),
                    )]),
                },
                ComponentContract {
                    id: SCORER_COMPONENT.to_owned(),
                    input_schema: json!({}),
                    output_schema: json!({}),
                    requirements: BTreeMap::from([(
                        "capability".to_owned(),
                        "evaluation-scorer".to_owned(),
                    )]),
                },
            ],
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: json!({}),
                output_schema: json!({}),
                body: Region {
                    steps: vec![
                        Step {
                            id: "subject.predict".to_owned(),
                            operation: Operation::Call {
                                component: SUBJECT_COMPONENT.to_owned(),
                                input: Expression::Input,
                                bind: Some("prediction".to_owned()),
                            },
                        },
                        Step {
                            id: "scorer.invoke".to_owned(),
                            operation: Operation::Invoke {
                                definition: "campaign_scorer".to_owned(),
                                input: Expression::Object {
                                    fields: BTreeMap::from([
                                        ("case".to_owned(), Expression::Input),
                                        (
                                            "prediction".to_owned(),
                                            Expression::Binding {
                                                name: "prediction".to_owned(),
                                            },
                                        ),
                                    ]),
                                },
                                bind: Some("score".to_owned()),
                            },
                        },
                    ],
                    result: Expression::Object {
                        fields: BTreeMap::from([
                            (
                                "prediction".to_owned(),
                                Expression::Binding {
                                    name: "prediction".to_owned(),
                                },
                            ),
                            (
                                "score".to_owned(),
                                Expression::Binding {
                                    name: "score".to_owned(),
                                },
                            ),
                        ]),
                    },
                },
            }],
            metadata: BTreeMap::from([
                (
                    "example".to_owned(),
                    "durable-evaluation-campaign".to_owned(),
                ),
                (
                    "subject_semantics".to_owned(),
                    "observational-retry-safe".to_owned(),
                ),
            ]),
        },
        references: vec![SubflowReference::latest_compatible(
            SCORER_REF,
            "campaign_scorer",
            json!({}),
            json!({}),
        )],
    }
}

pub fn current_plan(registry: &DefinitionRegistry) -> Result<cymule_evolution::LinkedPlan, String> {
    registry
        .current_link(TEMPLATE_ID)
        .cloned()
        .ok_or_else(|| "campaign template has no current linked Plan".to_owned())
}
