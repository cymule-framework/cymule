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

/// Require one lossless typed JSON round trip.
///
/// The raw value must already have passed [`decode_strict_json_value`], so safe
/// mathematical integers have one normalized token before this comparison.
/// Typed decoding may not erase or synthesize object members, collapse or
/// reorder array elements, or change any scalar. Frozen Engine schemas assign
/// one legal structure, including collection cardinality and order.
///
/// # Errors
///
/// Returns the exact JSON pointer of the first structural difference.
pub fn validate_json_typed_roundtrip(raw: &Value, normalized: &Value) -> Result<(), String> {
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
                        None => {
                            return Err(format!(
                                "typed normalization erased explicit object member at {member_path}"
                            ));
                        }
                    }
                }
                for member in normalized.keys() {
                    if !raw.contains_key(member) {
                        return Err(format!(
                            "typed normalization synthesized object member at {path}/{}",
                            pointer_member(member)
                        ));
                    }
                }
            }
            (Value::Array(raw), Value::Array(normalized)) => {
                if raw.len() != normalized.len() {
                    return Err(format!(
                        "typed normalization changed array length at {path}: {} became {}",
                        raw.len(),
                        normalized.len()
                    ));
                }
                for (index, (raw_value, normalized_value)) in raw.iter().zip(normalized).enumerate()
                {
                    visit(raw_value, normalized_value, &format!("{path}/{index}"))?;
                }
            }
            _ if raw == normalized => {}
            _ => {
                return Err(format!("typed normalization changed JSON value at {path}"));
            }
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
    fn typed_roundtrip_rejects_every_structural_change_recursively() {
        let raw = serde_json::json!({
            "outer": [{
                "erased": null,
                "retained": null,
                "non_null_difference": "raw",
                "number": 1,
            }],
        });
        let normalized = serde_json::json!({
            "outer": [{
                "retained": null,
                "number": 1,
            }],
        });
        let error = validate_json_typed_roundtrip(&raw, &normalized)
            .expect_err("an explicit nested null may not disappear");
        assert!(error.ends_with("/outer/0/erased"));

        let retained_only = serde_json::json!({
            "outer": [{
                "retained": null,
                "non_null_difference": "raw",
                "number": 1,
            }],
        });
        let error = validate_json_typed_roundtrip(&retained_only, &normalized)
            .expect_err("an explicitly present defaulted member may not disappear");
        assert!(error.ends_with("/outer/0/non_null_difference"));

        for erased in [
            serde_json::json!({"value": {}}),
            serde_json::json!({"value": []}),
            serde_json::json!({"value": ""}),
            serde_json::json!({"value": false}),
        ] {
            let error = validate_json_typed_roundtrip(&erased, &serde_json::json!({}))
                .expect_err("every explicitly present omitted default is lossy");
            assert!(error.ends_with("/value"));
        }

        validate_json_typed_roundtrip(
            &serde_json::json!({"retained": null, "number": 1}),
            &serde_json::json!({"retained": null, "number": 1}),
        )
        .expect("required nullable members and normalized numbers remain present");

        for (raw, normalized, expected) in [
            (
                serde_json::json!({"items": ["one", "one"]}),
                serde_json::json!({"items": ["one"]}),
                "/items",
            ),
            (
                serde_json::json!({"items": ["one", "two"]}),
                serde_json::json!({"items": ["two", "one"]}),
                "/items/0",
            ),
            (
                serde_json::json!({"value": "raw"}),
                serde_json::json!({"value": "normalized"}),
                "/value",
            ),
            (
                serde_json::json!({}),
                serde_json::json!({"defaulted": false}),
                "/defaulted",
            ),
        ] {
            let error = validate_json_typed_roundtrip(&raw, &normalized)
                .expect_err("typed round trips may not change JSON structure or scalars");
            assert!(error.contains(expected), "unexpected diagnostic: {error}");
        }
    }
}
