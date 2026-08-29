use serde_json::Value;

/// Validate the shared exact JSON domain before typed Engine decoding.
///
/// Object keys are unique, numbers are finite, and mathematically integral
/// numbers fit the exact cross-language range
/// `-9007199254740991..=9007199254740991`.
///
/// # Errors
///
/// Returns a description when the bytes are not one closed strict JSON value.
pub fn validate_strict_json(input: &[u8]) -> Result<(), String> {
    cymule_core::decode_json::<Value>(input)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

/// Decode one duplicate-free strict JSON value and normalize every safe
/// mathematically integral number to an integer token.
///
/// JSON Schema defines `integer` mathematically, so `1`, `1.0`, and `1e0`
/// belong to the same typed domain. Normalizing before typed deserialization
/// gives Rust and every SDK one representation and makes success echoes stable.
/// Finite fractional numbers remain numbers for caller-defined JSON values.
///
/// # Errors
///
/// Returns a description when the bytes violate the shared strict JSON domain.
pub fn decode_strict_json_value(input: &[u8]) -> Result<Value, String> {
    cymule_core::decode_json(input).map_err(|error| error.to_string())
}

/// Reject an explicit `null` object member that typed serialization omits.
///
/// Serde's optional fields commonly deserialize both an absent member and an
/// explicit `null` to `None`, then omit `None` during serialization. Frozen
/// Engine schemas distinguish those two wires. This comparison deliberately
/// checks only that one lossy case: other representational differences are
/// left to the owning typed and schema contracts.
///
/// # Errors
///
/// Returns the exact JSON pointer when typed normalization erased an explicit
/// null member that the wire contract distinguishes from omission.
pub fn validate_json_member_presence(raw: &Value, normalized: &Value) -> Result<(), String> {
    fn pointer_member(member: &str) -> String {
        member.replace('~', "~0").replace('/', "~1")
    }

    fn visit(raw: &Value, normalized: &Value, path: &str) -> Result<(), String> {
        match (raw, normalized) {
            (Value::Object(raw), Value::Object(normalized)) => {
                for (member, raw_value) in raw {
                    let member_path = format!("{path}/{}", pointer_member(member));
                    match normalized.get(member) {
                        Some(normalized_value) => {
                            visit(raw_value, normalized_value, &member_path)?;
                        }
                        None if raw_value.is_null() => {
                            return Err(format!(
                                "typed normalization erased explicit null object member at {member_path}"
                            ));
                        }
                        None => {}
                    }
                }
            }
            (Value::Array(raw), Value::Array(normalized)) => {
                for (index, (raw_value, normalized_value)) in raw.iter().zip(normalized).enumerate()
                {
                    visit(raw_value, normalized_value, &format!("{path}/{index}"))?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    visit(raw, normalized, "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_json_rejects_duplicate_non_finite_and_unsafe_integer_values() {
        assert!(validate_strict_json(br#"{"value":1,"value":2}"#).is_err());
        assert!(validate_strict_json(br#"{"value":NaN}"#).is_err());
        assert!(validate_strict_json(br#"{"value":9007199254740992}"#).is_err());
        validate_strict_json(br#"{"value":9007199254740991}"#).expect("safe JSON validates");
        assert!(validate_strict_json(br#"{"value":9007199254740992.0}"#).is_err());
        validate_strict_json(br#"{"value":1.5}"#).expect("finite fractions remain valid JSON");
    }

    #[test]
    fn strict_json_normalizes_mathematical_integers_recursively() {
        for input in [
            br#"{"value":1.0,"nested":[-2e0]}"#.as_slice(),
            br#"{"value":1e0,"nested":[-2.0]}"#.as_slice(),
        ] {
            let value = decode_strict_json_value(input).expect("safe mathematical integers decode");
            assert_eq!(value["value"], serde_json::json!(1));
            assert_eq!(value["nested"][0], serde_json::json!(-2));
            assert_eq!(
                serde_json::to_string(&value).unwrap(),
                r#"{"nested":[-2],"value":1}"#
            );
        }

        let fractional =
            decode_strict_json_value(br#"{"value":1.5}"#).expect("finite fractional values decode");
        assert_eq!(fractional["value"], serde_json::json!(1.5));
    }

    #[test]
    fn member_presence_rejects_only_erased_explicit_nulls_recursively() {
        let raw = serde_json::json!({
            "outer": [{
                "erased": null,
                "retained": null,
                "non_null_difference": "raw",
                "number": 1.0,
            }],
        });
        let normalized = serde_json::json!({
            "outer": [{
                "retained": null,
                "number": 1,
            }],
        });
        let error = validate_json_member_presence(&raw, &normalized)
            .expect_err("an explicit nested null may not disappear");
        assert!(error.ends_with("/outer/0/erased"));

        let retained_only = serde_json::json!({
            "outer": [{
                "retained": null,
                "non_null_difference": "raw",
                "number": 1.0,
            }],
        });
        validate_json_member_presence(&retained_only, &normalized)
            .expect("required nullable members and other representations are unchanged here");
    }
}
