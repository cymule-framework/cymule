//! Typed suite, prediction, and scorer values used by the example plugin.

use serde::{Deserialize, Serialize};

pub const SUITE_MEDIA_TYPE: &str = "application/x-ndjson";
pub const CAMPAIGN_METADATA_ARTIFACT_KIND: &str = "example.evaluation-campaign-metadata/1";
pub const CASE_ARTIFACT_KIND: &str = "example.evaluation-case/1";
pub const RESULT_ARTIFACT_KIND: &str = "example.evaluation-result/1";
pub const ERROR_ARTIFACT_KIND: &str = "example.evaluation-error/1";
pub const MAX_SUITE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_CASES: usize = 100_000;
pub const MAX_MESSAGE_SCALARS: usize = 16 * 1024;

/// One immutable evaluation case from the JSON Lines suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationCase {
    /// Stable identity unique within the suite.
    pub id: String,
    /// Input presented to the evaluated subject.
    pub input: TicketInput,
    /// Expected reference label.
    pub expected: TicketLabel,
}

/// User input presented to the ticket-routing subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketInput {
    /// Bounded support message.
    pub message: String,
}

/// Expected or predicted routing label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TicketLabel {
    /// Routing category.
    pub category: String,
    /// Urgency class.
    pub urgency: String,
}

/// Subject prediction returned across the process-plugin boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prediction {
    /// Predicted routing category.
    pub category: String,
    /// Predicted urgency class.
    pub urgency: String,
}

/// Integer scorer result with explicit policy identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Score {
    /// Scorer policy pinned by the linked Plan.
    pub policy: String,
    /// Awarded integer points.
    pub points: u8,
    /// Maximum possible integer points.
    pub max_points: u8,
    /// Policy-specific acceptance result.
    pub passed: bool,
}

/// Combined subject prediction and scorer output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseOutput {
    /// Evaluated subject output.
    pub prediction: Prediction,
    /// Scorer output under the occurrence's pinned Plan.
    pub score: Score,
}

impl CaseOutput {
    /// Verify the policy result against the exact case and returned prediction.
    pub fn validate_for(&self, case: &EvaluationCase, expected_policy: &str) -> Result<(), String> {
        if !matches!(
            self.prediction.category.as_str(),
            "identity" | "billing" | "reliability" | "general"
        ) || !matches!(self.prediction.urgency.as_str(), "normal" | "high")
        {
            return Err("prediction contains an unsupported label".to_owned());
        }
        let category = u8::from(case.expected.category == self.prediction.category);
        let urgency = u8::from(case.expected.urgency == self.prediction.urgency);
        if self.score.policy != expected_policy {
            return Err("score policy does not match the occurrence Plan".to_owned());
        }
        let (points, passed) = match expected_policy {
            "strict" => {
                let exact = category + urgency == 2;
                (if exact { 2 } else { 0 }, exact)
            }
            "weighted" => (category + urgency, category == 1),
            _ => return Err("score contains an unsupported policy".to_owned()),
        };
        if self.score.points != points || self.score.max_points != 2 || self.score.passed != passed
        {
            return Err("score does not match the campaign policy result".to_owned());
        }
        Ok(())
    }
}

impl EvaluationCase {
    /// Validate the bounded vocabulary and input shape used by this example.
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty()
            || self.id.chars().count() > 128
            || self.id.chars().any(char::is_control)
            || self.input.message.is_empty()
            || self.input.message.chars().count() > MAX_MESSAGE_SCALARS
            || self.input.message.chars().any(char::is_control)
        {
            return Err(format!("case {:?} has an invalid ID or message", self.id));
        }
        if !matches!(
            self.expected.category.as_str(),
            "identity" | "billing" | "reliability" | "general"
        ) || !matches!(self.expected.urgency.as_str(), "normal" | "high")
        {
            return Err(format!(
                "case {} has an unsupported expected label",
                self.id
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{EvaluationCase, MAX_MESSAGE_SCALARS, TicketInput, TicketLabel};

    #[test]
    fn string_bounds_match_json_schema_unicode_scalar_semantics() {
        let case = |id: String, message: String| EvaluationCase {
            id,
            input: TicketInput { message },
            expected: TicketLabel {
                category: "general".to_owned(),
                urgency: "normal".to_owned(),
            },
        };

        case("🧭".repeat(128), "界".repeat(MAX_MESSAGE_SCALARS))
            .validate()
            .expect("multi-byte values at the schema scalar bound are valid");
        assert!(
            case("🧭".repeat(129), "valid".to_owned())
                .validate()
                .is_err()
        );
        assert!(
            case(
                "case:valid".to_owned(),
                "界".repeat(MAX_MESSAGE_SCALARS + 1)
            )
            .validate()
            .is_err()
        );
    }
}
