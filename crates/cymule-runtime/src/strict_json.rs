use std::collections::BTreeMap;

use serde_json::Value;

const MAX_JSON_DEPTH: usize = 128;
const MAX_JSON_NUMBER_TOKEN_BYTES: usize = 256;
const MAX_JSON_EXPONENT_DIGITS: usize = 6;

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
    decimal_fingerprint(input)?;
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
    decimal_fingerprint(input)?;
    cymule_core::decode_json(input).map_err(|error| error.to_string())
}

/// Require lossless structure and exact fractional-decimal evidence between
/// raw wire bytes and a typed reserialization.
///
/// # Errors
/// Returns the first structural or fractional JSON pointer that changed.
pub fn validate_json_typed_roundtrip_bytes(
    raw: &[u8],
    raw_value: &Value,
    normalized: &Value,
) -> Result<(), String> {
    validate_json_typed_roundtrip(raw_value, normalized)?;
    let normalized_bytes = serde_json::to_vec(normalized).map_err(|error| error.to_string())?;
    let raw_fractions = decimal_fingerprint(raw)?;
    let normalized_fractions = decimal_fingerprint(&normalized_bytes)?;
    if raw_fractions == normalized_fractions {
        return Ok(());
    }
    let mut path = Vec::new();
    first_fingerprint_difference(&raw_fractions, &normalized_fractions, &mut path);
    let mut changed = String::new();
    for member in path {
        changed.push('/');
        changed.push_str(&member.replace('~', "~0").replace('/', "~1"));
    }
    Err(format!(
        "typed normalization changed exact fractional JSON value at {changed}"
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecimalFingerprint {
    Empty,
    Fraction(String),
    Object(BTreeMap<String, DecimalFingerprint>),
    Array(BTreeMap<usize, DecimalFingerprint>),
}

fn decimal_fingerprint(input: &[u8]) -> Result<DecimalFingerprint, String> {
    let mut parser = FingerprintParser { input, index: 0 };
    let fingerprint = parser.value(0)?;
    parser.whitespace();
    if parser.index != input.len() {
        return Err("trailing JSON content".to_owned());
    }
    Ok(fingerprint)
}

struct FingerprintParser<'a> {
    input: &'a [u8],
    index: usize,
}

impl FingerprintParser<'_> {
    fn whitespace(&mut self) {
        while self
            .input
            .get(self.index)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.index += 1;
        }
    }

    fn value(&mut self, depth: usize) -> Result<DecimalFingerprint, String> {
        if depth > MAX_JSON_DEPTH {
            return Err(format!("JSON nesting exceeds {MAX_JSON_DEPTH} levels"));
        }
        self.whitespace();
        match self.input.get(self.index).copied() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string().map(|_| DecimalFingerprint::Empty),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            _ => Err("invalid JSON value".to_owned()),
        }
    }

    fn object(&mut self, depth: usize) -> Result<DecimalFingerprint, String> {
        let mut fractions = BTreeMap::new();
        self.index += 1;
        self.whitespace();
        if self.input.get(self.index) == Some(&b'}') {
            self.index += 1;
            return Ok(DecimalFingerprint::Empty);
        }
        loop {
            let member = self.string()?;
            self.whitespace();
            self.expect(b':')?;
            let child = self.value(depth + 1)?;
            if child != DecimalFingerprint::Empty {
                fractions.insert(member, child);
            }
            self.whitespace();
            if self.input.get(self.index) == Some(&b'}') {
                self.index += 1;
                return Ok(if fractions.is_empty() {
                    DecimalFingerprint::Empty
                } else {
                    DecimalFingerprint::Object(fractions)
                });
            }
            self.expect(b',')?;
            self.whitespace();
        }
    }

    fn array(&mut self, depth: usize) -> Result<DecimalFingerprint, String> {
        let mut fractions = BTreeMap::new();
        self.index += 1;
        self.whitespace();
        if self.input.get(self.index) == Some(&b']') {
            self.index += 1;
            return Ok(DecimalFingerprint::Empty);
        }
        let mut member = 0_usize;
        loop {
            let child = self.value(depth + 1)?;
            if child != DecimalFingerprint::Empty {
                fractions.insert(member, child);
            }
            member += 1;
            self.whitespace();
            if self.input.get(self.index) == Some(&b']') {
                self.index += 1;
                return Ok(if fractions.is_empty() {
                    DecimalFingerprint::Empty
                } else {
                    DecimalFingerprint::Array(fractions)
                });
            }
            self.expect(b',')?;
        }
    }

    fn string(&mut self) -> Result<String, String> {
        let start = self.index;
        self.expect(b'"')?;
        while let Some(byte) = self.input.get(self.index).copied() {
            self.index += 1;
            match byte {
                b'\\' => self.index = self.index.saturating_add(1),
                b'"' => {
                    return serde_json::from_slice(&self.input[start..self.index])
                        .map_err(|error| error.to_string());
                }
                _ => {}
            }
        }
        Err("unterminated JSON string".to_owned())
    }

    fn number(&mut self) -> Result<DecimalFingerprint, String> {
        let start = self.index;
        if self.input.get(self.index) == Some(&b'-') {
            self.index += 1;
        }
        while self.input.get(self.index).is_some_and(u8::is_ascii_digit) {
            self.index += 1;
        }
        if self.input.get(self.index) == Some(&b'.') {
            self.index += 1;
            while self.input.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
        }
        if matches!(self.input.get(self.index), Some(b'e' | b'E')) {
            self.index += 1;
            if matches!(self.input.get(self.index), Some(b'+' | b'-')) {
                self.index += 1;
            }
            while self.input.get(self.index).is_some_and(u8::is_ascii_digit) {
                self.index += 1;
            }
        }
        let token = std::str::from_utf8(&self.input[start..self.index])
            .map_err(|error| error.to_string())?;
        if token.len() > MAX_JSON_NUMBER_TOKEN_BYTES {
            return Err(format!(
                "JSON number token exceeds {MAX_JSON_NUMBER_TOKEN_BYTES} bytes"
            ));
        }
        if let Some(exponent) = token.find(['e', 'E']) {
            let exponent = &token[exponent + 1..];
            let digits = exponent
                .strip_prefix('+')
                .or_else(|| exponent.strip_prefix('-'))
                .unwrap_or(exponent);
            if digits.len() > MAX_JSON_EXPONENT_DIGITS {
                return Err(format!(
                    "JSON number exponent exceeds {MAX_JSON_EXPONENT_DIGITS} digits"
                ));
            }
        }
        Ok(canonical_fraction(token)?
            .map_or(DecimalFingerprint::Empty, DecimalFingerprint::Fraction))
    }

    fn literal(&mut self, literal: &[u8]) -> Result<DecimalFingerprint, String> {
        if self.input.get(self.index..self.index + literal.len()) == Some(literal) {
            self.index += literal.len();
            Ok(DecimalFingerprint::Empty)
        } else {
            Err("invalid JSON literal".to_owned())
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), String> {
        if self.input.get(self.index) != Some(&byte) {
            return Err(format!("expected JSON byte {byte}"));
        }
        self.index += 1;
        Ok(())
    }
}

fn first_fingerprint_difference(
    left: &DecimalFingerprint,
    right: &DecimalFingerprint,
    path: &mut Vec<String>,
) -> bool {
    match (left, right) {
        (DecimalFingerprint::Object(left), DecimalFingerprint::Object(right)) => {
            for member in left.keys().chain(right.keys()) {
                if left.get(member) == right.get(member) {
                    continue;
                }
                path.push(member.clone());
                if let (Some(left), Some(right)) = (left.get(member), right.get(member)) {
                    first_fingerprint_difference(left, right, path);
                }
                return true;
            }
            false
        }
        (DecimalFingerprint::Array(left), DecimalFingerprint::Array(right)) => {
            for member in left.keys().chain(right.keys()) {
                if left.get(member) == right.get(member) {
                    continue;
                }
                path.push(member.to_string());
                if let (Some(left), Some(right)) = (left.get(member), right.get(member)) {
                    first_fingerprint_difference(left, right, path);
                }
                return true;
            }
            false
        }
        _ => left != right,
    }
}

fn canonical_fraction(token: &str) -> Result<Option<String>, String> {
    let negative = token.starts_with('-');
    let unsigned = token.strip_prefix('-').unwrap_or(token);
    let exponent_index = unsigned.find(['e', 'E']).unwrap_or(unsigned.len());
    let mantissa = &unsigned[..exponent_index];
    let exponent = if exponent_index == unsigned.len() {
        0_i32
    } else {
        unsigned[exponent_index + 1..]
            .parse::<i32>()
            .map_err(|_| "invalid JSON number exponent".to_owned())?
    };
    let point = mantissa.find('.');
    let fraction_digits = point.map_or(0, |point| mantissa.len() - point - 1);
    let mut digits = mantissa.replace('.', "");
    let first_nonzero = digits.find(|character| character != '0');
    let Some(first_nonzero) = first_nonzero else {
        return Ok(None);
    };
    digits.drain(..first_nonzero);
    let mut scale = exponent
        .checked_sub(i32::try_from(fraction_digits).map_err(|error| error.to_string())?)
        .ok_or_else(|| "JSON number scale overflowed".to_owned())?;
    while digits.ends_with('0') {
        digits.pop();
        scale += 1;
    }
    if scale >= 0 {
        return Ok(None);
    }
    Ok(Some(format!(
        "{}{digits}e{scale}",
        if negative { "-" } else { "" }
    )))
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
    fn pointer(path: &[String]) -> String {
        let mut pointer = String::new();
        for member in path {
            pointer.push('/');
            pointer.push_str(&member.replace('~', "~0").replace('/', "~1"));
        }
        pointer
    }

    fn visit(raw: &Value, normalized: &Value, path: &mut Vec<String>) -> Result<(), String> {
        match (raw, normalized) {
            (Value::Object(raw), Value::Object(normalized)) => {
                for (member, raw_value) in raw {
                    path.push(member.clone());
                    match normalized.get(member) {
                        Some(normalized_value) => {
                            visit(raw_value, normalized_value, path)?;
                        }
                        None => {
                            return Err(format!(
                                "typed normalization erased explicit object member at {}",
                                pointer(path)
                            ));
                        }
                    }
                    path.pop();
                }
                for member in normalized.keys() {
                    if !raw.contains_key(member) {
                        path.push(member.clone());
                        return Err(format!(
                            "typed normalization synthesized object member at {}",
                            pointer(path)
                        ));
                    }
                }
            }
            (Value::Array(raw), Value::Array(normalized)) => {
                if raw.len() != normalized.len() {
                    return Err(format!(
                        "typed normalization changed array length at {}: {} became {}",
                        pointer(path),
                        raw.len(),
                        normalized.len()
                    ));
                }
                for (index, (raw_value, normalized_value)) in raw.iter().zip(normalized).enumerate()
                {
                    path.push(index.to_string());
                    visit(raw_value, normalized_value, path)?;
                    path.pop();
                }
            }
            _ if raw == normalized => {}
            _ => {
                return Err(format!(
                    "typed normalization changed JSON value at {}",
                    pointer(path)
                ));
            }
        }
        Ok(())
    }

    visit(raw, normalized, &mut Vec::new())
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

    #[test]
    fn typed_roundtrip_preserves_exact_fractional_decimal_evidence() {
        let normalized = serde_json::json!({"value": 0.1});
        let equivalent = decode_strict_json_value(br#"{"value":0.10}"#).unwrap();
        validate_json_typed_roundtrip_bytes(br#"{"value":0.10}"#, &equivalent, &normalized)
            .expect("mathematically identical fractional decimals normalize");
        let collision = br#"{"value":0.100000000000000005}"#;
        let collision_value = decode_strict_json_value(collision).unwrap();
        let error = validate_json_typed_roundtrip_bytes(collision, &collision_value, &normalized)
            .expect_err("fractional f64 collision is rejected");
        assert!(error.ends_with("/value"));

        for input in [
            format!(
                "{{\"value\":1e{}}}",
                "9".repeat(MAX_JSON_EXPONENT_DIGITS + 1)
            ),
            format!(
                "{{\"value\":{}}}",
                "1".repeat(MAX_JSON_NUMBER_TOKEN_BYTES + 1)
            ),
        ] {
            assert!(validate_strict_json(input.as_bytes()).is_err());
        }
    }
}
