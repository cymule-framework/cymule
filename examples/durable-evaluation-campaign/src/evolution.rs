use std::collections::BTreeMap;

use cymule_core::{
    COMPONENT_OUTPUT_ARTIFACT_KIND, ComponentContract, Definition, Expression, IR_VERSION,
    Operation, PlanCandidate, Region, Step,
};
use cymule_evolution::{PlanTemplate, SubflowReference};
use serde_json::{Value, json};

use crate::{
    model::MAX_MESSAGE_SCALARS,
    plugin::{SCORER_COMPONENT, SUBJECT_COMPONENT},
};

pub const SCORER_REF: &str = "example.scorer";
pub const TEMPLATE_ID: &str = "example.evaluation-campaign";

fn ticket_label_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "category": {"enum": ["identity", "billing", "reliability", "general"]},
            "urgency": {"enum": ["normal", "high"]}
        },
        "required": ["category", "urgency"],
        "additionalProperties": false
    })
}

fn evaluation_case_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "pattern": r"^[^\u0000-\u001f\u007f-\u009f]+$"
            },
            "input": {
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_MESSAGE_SCALARS,
                        "pattern": r"^[^\u0000-\u001f\u007f-\u009f]+$"
                    }
                },
                "required": ["message"],
                "additionalProperties": false
            },
            "expected": ticket_label_schema()
        },
        "required": ["id", "input", "expected"],
        "additionalProperties": false
    })
}

fn prediction_schema() -> Value {
    ticket_label_schema()
}

fn scorer_evaluation_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "case": evaluation_case_schema(),
            "prediction": prediction_schema()
        },
        "required": ["case", "prediction"],
        "additionalProperties": false
    })
}

fn scorer_component_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "evaluation": scorer_evaluation_schema(),
            "policy": {"enum": ["strict", "weighted"]}
        },
        "required": ["evaluation", "policy"],
        "additionalProperties": false
    })
}

fn score_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "policy": {"enum": ["strict", "weighted"]},
            "points": {"type": "integer", "minimum": 0, "maximum": 2},
            "max_points": {"const": 2},
            "passed": {"type": "boolean"}
        },
        "required": ["policy", "points", "max_points", "passed"],
        "additionalProperties": false
    })
}

fn case_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "prediction": prediction_schema(),
            "score": score_schema()
        },
        "required": ["prediction", "score"],
        "additionalProperties": false
    })
}

pub fn scorer_definition(policy: &str, compatible: bool) -> Definition {
    Definition {
        id: "score".to_owned(),
        input_schema: if compatible {
            scorer_evaluation_schema()
        } else {
            json!({"type": "string"})
        },
        output_schema: score_schema(),
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
                    input_schema: evaluation_case_schema(),
                    output_schema: prediction_schema(),
                    output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
                    requirements: BTreeMap::from([(
                        "capability".to_owned(),
                        "evaluation-subject".to_owned(),
                    )]),
                },
                ComponentContract {
                    id: SCORER_COMPONENT.to_owned(),
                    input_schema: scorer_component_input_schema(),
                    output_schema: score_schema(),
                    output_artifact_kind: COMPONENT_OUTPUT_ARTIFACT_KIND.to_owned(),
                    requirements: BTreeMap::from([(
                        "capability".to_owned(),
                        "evaluation-scorer".to_owned(),
                    )]),
                },
            ],
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: evaluation_case_schema(),
                output_schema: case_output_schema(),
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
            scorer_evaluation_schema(),
            score_schema(),
        )],
    }
}
