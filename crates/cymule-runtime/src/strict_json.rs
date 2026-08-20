use std::collections::BTreeSet;

use serde::Deserialize;

/// Validate the shared exact JSON domain before typed Engine decoding.
///
/// Object keys are unique, numbers are finite, and integer-valued numbers fit
/// the exact cross-language range `-9007199254740991..=9007199254740991`.
pub fn validate_strict_json(input: &[u8]) -> Result<(), String> {
    struct StrictValue;
    impl<'de> serde::Deserialize<'de> for StrictValue {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(StrictVisitor)
        }
    }

    struct StrictVisitor;
    impl<'de> serde::de::Visitor<'de> for StrictVisitor {
        type Value = StrictValue;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("strict JSON")
        }

        fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
            Ok(StrictValue)
        }

        fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
            if value.unsigned_abs() <= 9_007_199_254_740_991 {
                Ok(StrictValue)
            } else {
                Err(E::custom("integer outside shared JSON range"))
            }
        }

        fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
            if value <= 9_007_199_254_740_991 {
                Ok(StrictValue)
            } else {
                Err(E::custom("integer outside shared JSON range"))
            }
        }

        fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
            if value.is_finite() && (value.fract() != 0.0 || value.abs() <= 9_007_199_254_740_991.0)
            {
                Ok(StrictValue)
            } else {
                Err(E::custom("number outside shared JSON range"))
            }
        }

        fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
            Ok(StrictValue)
        }

        fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
            Ok(StrictValue)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(StrictValue)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(StrictValue)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            while sequence.next_element::<StrictValue>()?.is_some() {}
            Ok(StrictValue)
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut keys = BTreeSet::new();
            while let Some(key) = map.next_key::<String>()? {
                if !keys.insert(key.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate JSON object key {key:?}"
                    )));
                }
                map.next_value::<StrictValue>()?;
            }
            Ok(StrictValue)
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(input);
    StrictValue::deserialize(&mut deserializer)
        .and_then(|_| deserializer.end())
        .map_err(|error| error.to_string())
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
    }
}
