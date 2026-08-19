//! Deterministic, credential-free subject and scorer process plugin.

use std::collections::BTreeMap;

use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginOperation, PluginRequest, PluginResponse,
    RuntimeError, RuntimeResult,
};
use serde_json::{Value, json};

use crate::model::{EvaluationCase, Prediction, Score, TicketLabel};

/// Abstract component operation for the evaluated subject.
pub const SUBJECT_COMPONENT: &str = "example.ticket-subject";
/// Abstract component operation for versioned scoring policy.
pub const SCORER_COMPONENT: &str = "example.ticket-scorer";
/// Immutable implementation identity retained in runtime bindings.
pub const IMPLEMENTATION_ID: &str = "example.deterministic-ticket-evaluator@1";

/// Pure local plugin used by the default example without external services.
pub struct EvaluationPlugin;

impl PluginHost for EvaluationPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: PluginManifest {
                    plugin_version: PLUGIN_VERSION.to_owned(),
                    implementation_id: IMPLEMENTATION_ID.to_owned(),
                    components: BTreeMap::from([
                        (
                            SUBJECT_COMPONENT.to_owned(),
                            PluginOperation {
                                implementation_revision: "1".to_owned(),
                            },
                        ),
                        (
                            SCORER_COMPONENT.to_owned(),
                            PluginOperation {
                                implementation_revision: "2".to_owned(),
                            },
                        ),
                    ]),
                    effects: BTreeMap::new(),
                },
            }),
            PluginRequest::Call { component, input } if component == SUBJECT_COMPONENT => {
                let case: EvaluationCase = serde_json::from_value(input)
                    .map_err(|error| RuntimeError::Plugin(error.to_string()))?;
                case.validate().map_err(RuntimeError::Plugin)?;
                Ok(PluginResponse::CallResult {
                    value: serde_json::to_value(predict(&case))?,
                })
            }
            PluginRequest::Call { component, input } if component == SCORER_COMPONENT => {
                Ok(PluginResponse::CallResult {
                    value: score(&input)?,
                })
            }
            request => Err(RuntimeError::Plugin(format!(
                "unsupported evaluation request: {request:?}"
            ))),
        }
    }
}

fn predict(case: &EvaluationCase) -> Prediction {
    let message = case.input.message.to_ascii_lowercase();
    let category = if contains_any(&message, &["password", "login", "locked", "sso"]) {
        "identity"
    } else if contains_any(&message, &["invoice", "charge", "refund", "payment"]) {
        "billing"
    } else if contains_any(&message, &["down", "outage", "unavailable", "slow"]) {
        "reliability"
    } else {
        "general"
    };
    let urgency = if contains_any(&message, &["urgent", "down", "outage", "every customer"])
        || message.contains("tomorrow")
    {
        "high"
    } else {
        "normal"
    };
    Prediction {
        category: category.to_owned(),
        urgency: urgency.to_owned(),
    }
}

fn score(input: &Value) -> RuntimeResult<Value> {
    let evaluation = input
        .get("evaluation")
        .ok_or_else(|| RuntimeError::Plugin("scorer input is missing evaluation".to_owned()))?;
    let policy = input
        .get("policy")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::Plugin("scorer input is missing policy".to_owned()))?;
    let expected: TicketLabel = serde_json::from_value(
        evaluation
            .get("case")
            .and_then(|case| case.get("expected"))
            .cloned()
            .ok_or_else(|| RuntimeError::Plugin("scorer input is missing expected".to_owned()))?,
    )?;
    let prediction: Prediction =
        serde_json::from_value(evaluation.get("prediction").cloned().ok_or_else(|| {
            RuntimeError::Plugin("scorer input is missing prediction".to_owned())
        })?)?;
    let category = u8::from(expected.category == prediction.category);
    let urgency = u8::from(expected.urgency == prediction.urgency);
    let result = match policy {
        "strict" => Score {
            policy: policy.to_owned(),
            points: if category + urgency == 2 { 2 } else { 0 },
            max_points: 2,
            passed: category + urgency == 2,
        },
        "weighted" => Score {
            policy: policy.to_owned(),
            points: category + urgency,
            max_points: 2,
            passed: category == 1,
        },
        _ => {
            return Err(RuntimeError::Plugin(format!(
                "unsupported scoring policy {policy:?}"
            )));
        }
    };
    Ok(json!(result))
}

fn contains_any(value: &str, candidates: &[&str]) -> bool {
    candidates.iter().any(|candidate| value.contains(candidate))
}
