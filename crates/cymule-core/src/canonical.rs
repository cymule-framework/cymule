use std::fmt;

use serde::de::{DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::model::MAX_EXACT_INTEGER;
use crate::{CoreError, Result};

const HEX: &[u8; 16] = b"0123456789abcdef";
const FIRST_UNSAFE_INTEGER_F64: f64 = 9_007_199_254_740_992.0;

/// Serialize a value using RFC 8785 JSON Canonicalization Scheme.
///
/// # Errors
///
/// Returns an encoding error when the value cannot be serialized without
/// violating Core's recursive I-JSON number contract.
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // Probe the original Serde data before serde_json can project NaN or
    // infinity to null or JCS can round an integer through IEEE-754. Then retain
    // the exact integer representation in Value and normalize safe integral
    // floats to the one canonical in-memory number shape.
    value
        .serialize(NumberProbe)
        .map_err(|error| CoreError::Encoding(error.to_string()))?;
    let direct = serde_json_canonicalizer::to_vec(value)
        .map_err(|error| CoreError::Encoding(error.to_string()))?;
    let mut normalized =
        serde_json::to_value(value).map_err(|error| CoreError::Encoding(error.to_string()))?;
    normalize_ijson_numbers(&mut normalized)?;
    let canonical = serde_json_canonicalizer::to_vec(&normalized)
        .map_err(|error| CoreError::Encoding(error.to_string()))?;
    if direct != canonical {
        return Err(CoreError::Encoding(
            "JSON serialization changed while validating canonical numbers".to_owned(),
        ));
    }
    Ok(canonical)
}

/// Compute a lowercase hexadecimal SHA-256 digest.
pub fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

/// Compute a domain-separated content identifier for canonical JSON.
///
/// # Errors
///
/// Returns an encoding error when `value` cannot be serialized canonically.
pub fn content_id<T: Serialize>(domain: &str, value: &T) -> Result<String> {
    let bytes = canonical_bytes(value)?;
    let mut input = Vec::with_capacity(domain.len() + bytes.len() + 1);
    input.extend_from_slice(domain.as_bytes());
    input.push(0);
    input.extend_from_slice(&bytes);
    Ok(format!("sha256:{}", sha256_bytes(&input)))
}

/// Compute a canonical digest without an identity prefix.
///
/// # Errors
///
/// Returns an encoding error when `value` cannot be serialized canonically.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(sha256_bytes(&canonical_bytes(value)?))
}

/// Validate one lowercase SHA-256 content identifier.
///
/// # Errors
///
/// Returns a validation error unless `value` is exactly `sha256:` followed by
/// 64 lowercase hexadecimal digits.
pub fn validate_content_id(kind: &str, value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(CoreError::Validation(format!(
            "{kind} must be a lowercase SHA-256 content ID"
        )));
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CoreError::Validation(format!(
            "{kind} must be a lowercase SHA-256 content ID"
        )));
    }
    Ok(())
}

/// Decode one JSON value while rejecting duplicate object members at every
/// depth before typed deserialization can collapse them.
///
/// # Errors
///
/// Returns an encoding error when the bytes are not one complete strict JSON
/// value, contain duplicate members or invalid numbers, or do not decode as
/// `T`.
pub fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    validate_json_number_lexemes(bytes)?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = UniqueValue::deserialize(&mut deserializer)
        .map_err(|error| CoreError::Encoding(error.to_string()))?
        .0;
    deserializer
        .end()
        .map_err(|error| CoreError::Encoding(error.to_string()))?;
    serde_json::from_value(value).map_err(|error| CoreError::Encoding(error.to_string()))
}

fn validate_json_number_lexemes(bytes: &[u8]) -> Result<()> {
    let mut index = 0_usize;
    while index < bytes.len() {
        if bytes[index] == b'"' {
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' => index = index.saturating_add(2),
                    b'"' => {
                        index += 1;
                        break;
                    }
                    _ => index += 1,
                }
            }
            continue;
        }
        if bytes[index] != b'-' && !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }
        let start = index;
        if bytes[index] == b'-' {
            index += 1;
            if index == bytes.len() || !bytes[index].is_ascii_digit() {
                continue;
            }
        }
        if bytes[index] == b'0' {
            index += 1;
        } else {
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }
        if index < bytes.len() && bytes[index] == b'.' {
            index += 1;
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }
        if index < bytes.len() && matches!(bytes[index], b'e' | b'E') {
            index += 1;
            if index < bytes.len() && matches!(bytes[index], b'+' | b'-') {
                index += 1;
            }
            while index < bytes.len() && bytes[index].is_ascii_digit() {
                index += 1;
            }
        }
        let lexeme = std::str::from_utf8(&bytes[start..index])
            .map_err(|error| CoreError::Encoding(error.to_string()))?;
        validate_json_number_lexeme(lexeme)?;
    }
    Ok(())
}

fn validate_json_number_lexeme(lexeme: &str) -> Result<()> {
    let unsigned = lexeme.strip_prefix('-').unwrap_or(lexeme);
    let exponent_start = unsigned.find(['e', 'E']).unwrap_or(unsigned.len());
    let mantissa = &unsigned[..exponent_start];
    let exponent = if exponent_start == unsigned.len() {
        0_i128
    } else {
        parse_decimal_exponent(&unsigned[exponent_start + 1..])
    };
    let fraction_length = mantissa
        .find('.')
        .map_or(0_usize, |dot| mantissa.len().saturating_sub(dot + 1));
    let scale = exponent.saturating_sub(i128::try_from(fraction_length).unwrap_or(i128::MAX));
    let total_digits = mantissa.bytes().filter(u8::is_ascii_digit).count();
    let leading_zeros = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .take_while(|digit| *digit == b'0')
        .count();
    let trailing_zeros = mantissa
        .bytes()
        .rev()
        .filter(u8::is_ascii_digit)
        .take_while(|digit| *digit == b'0')
        .count();
    let is_zero = leading_zeros == total_digits;
    let removed_trailing = if scale < 0 {
        usize::try_from(scale.unsigned_abs()).unwrap_or(usize::MAX)
    } else {
        0
    };
    let is_integer = is_zero || scale >= 0 || trailing_zeros >= removed_trailing;

    if is_integer {
        if !is_zero
            && !decimal_integer_is_safe(mantissa, leading_zeros, removed_trailing, scale.max(0))
        {
            return Err(unsafe_integer_error(lexeme));
        }
        return Ok(());
    }

    let value = lexeme
        .parse::<f64>()
        .map_err(|_| CoreError::Encoding(format!("JSON number {lexeme} cannot be represented")))?;
    if !value.is_finite() {
        return Err(CoreError::Encoding("JSON number must be finite".to_owned()));
    }
    if value.fract() == 0.0 {
        return Err(CoreError::Encoding(format!(
            "fractional JSON number {lexeme} is not distinguishable from an integer"
        )));
    }
    Ok(())
}

fn parse_decimal_exponent(value: &str) -> i128 {
    let (negative, digits) = value
        .strip_prefix('-')
        .map_or((false, value), |digits| (true, digits));
    let digits = digits.strip_prefix('+').unwrap_or(digits);
    let magnitude = digits.bytes().fold(0_i128, |current, digit| {
        current
            .saturating_mul(10)
            .saturating_add(i128::from(digit.saturating_sub(b'0')))
            .min(1_000_000_000)
    });
    if negative { -magnitude } else { magnitude }
}

fn decimal_integer_is_safe(
    mantissa: &str,
    leading_zeros: usize,
    removed_trailing: usize,
    appended_zeros: i128,
) -> bool {
    const MAX: &[u8; 16] = b"9007199254740991";
    let coefficient_digits = mantissa.bytes().filter(u8::is_ascii_digit).count();
    let retained = coefficient_digits
        .saturating_sub(leading_zeros)
        .saturating_sub(removed_trailing);
    let appended = usize::try_from(appended_zeros).unwrap_or(usize::MAX);
    let normalized_length = retained.saturating_add(appended);
    if normalized_length != MAX.len() {
        return normalized_length < MAX.len();
    }
    let mut normalized = [b'0'; 16];
    let mut output_position = 0_usize;
    let retained_end = coefficient_digits.saturating_sub(removed_trailing);
    for (source_position, digit) in mantissa.bytes().filter(u8::is_ascii_digit).enumerate() {
        if source_position >= leading_zeros && source_position < retained_end {
            normalized[output_position] = digit;
            output_position += 1;
        }
    }
    normalized.as_slice() <= MAX
}

fn normalize_ijson_numbers(value: &mut Value) -> Result<()> {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_ijson_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                normalize_ijson_numbers(value)?;
            }
        }
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if value.unsigned_abs() > MAX_EXACT_INTEGER {
                    return Err(unsafe_integer_error(value));
                }
            } else if let Some(value) = number.as_u64() {
                if value > MAX_EXACT_INTEGER {
                    return Err(unsafe_integer_error(value));
                }
            } else {
                let value = number.as_f64().ok_or_else(|| {
                    CoreError::Encoding("JSON number cannot be represented exactly".to_owned())
                })?;
                if !value.is_finite() {
                    return Err(CoreError::Encoding("JSON number must be finite".to_owned()));
                }
                if value.fract() == 0.0 {
                    if value.abs() >= FIRST_UNSAFE_INTEGER_F64 {
                        return Err(unsafe_integer_error(value));
                    }
                    *number = normalized_integral_number(value);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

fn unsafe_integer_error(value: impl fmt::Display) -> CoreError {
    CoreError::Encoding(format!(
        "JSON integer {value} exceeds the exact cross-language range"
    ))
}

fn normalized_integral_number(value: f64) -> serde_json::Number {
    debug_assert!(
        value.is_finite() && value.fract() == 0.0 && value.abs() < FIRST_UNSAFE_INTEGER_F64
    );
    let integer = format!("{value:.0}");
    if value.is_sign_negative() {
        serde_json::Number::from(
            integer
                .parse::<i64>()
                .expect("validated exact negative integer parses as i64"),
        )
    } else {
        serde_json::Number::from(
            integer
                .parse::<u64>()
                .expect("validated exact non-negative integer parses as u64"),
        )
    }
}

#[derive(Clone, Copy)]
struct NumberProbe;

impl serde::Serializer for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _value: bool) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i16(self, value: i16) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i32(self, value: i32) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i64(self, value: i64) -> std::result::Result<Self::Ok, Self::Error> {
        if value.unsigned_abs() > MAX_EXACT_INTEGER {
            return Err(serde::ser::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(())
    }

    fn serialize_i128(self, value: i128) -> std::result::Result<Self::Ok, Self::Error> {
        if value.unsigned_abs() > u128::from(MAX_EXACT_INTEGER) {
            return Err(serde::ser::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u16(self, value: u16) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u32(self, value: u32) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u64(self, value: u64) -> std::result::Result<Self::Ok, Self::Error> {
        if value > MAX_EXACT_INTEGER {
            return Err(serde::ser::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(())
    }

    fn serialize_u128(self, value: u128) -> std::result::Result<Self::Ok, Self::Error> {
        if value > u128::from(MAX_EXACT_INTEGER) {
            return Err(serde::ser::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_f64(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> std::result::Result<Self::Ok, Self::Error> {
        if !value.is_finite() {
            return Err(serde::ser::Error::custom("JSON number must be finite"));
        }
        if value.fract() == 0.0 && value.abs() >= FIRST_UNSAFE_INTEGER_F64 {
            return Err(serde::ser::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(())
    }

    fn serialize_char(self, _value: char) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_str(self, _value: &str) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bytes(self, _value: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(
        self,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(
        self,
        _name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_seq(
        self,
        _len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple(
        self,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self)
    }

    fn serialize_map(
        self,
        _len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
    }
}

impl SerializeSeq for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_element<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleVariant for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeMap for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_key<T: ?Sized + Serialize>(
        &mut self,
        key: &T,
    ) -> std::result::Result<(), Self::Error> {
        key.serialize(*self)
    }

    fn serialize_value<T: ?Sized + Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStruct for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for NumberProbe {
    type Ok = ();
    type Error = serde_json::Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(*self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(UniqueValueVisitor)
    }
}

struct UniqueValueSeed;

impl<'de> DeserializeSeed<'de> for UniqueValueSeed {
    type Value = UniqueValue;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueValue::deserialize(deserializer)
    }
}

struct UniqueValueVisitor;

impl<'de> Visitor<'de> for UniqueValueVisitor {
    type Value = UniqueValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object members")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.unsigned_abs() > MAX_EXACT_INTEGER {
            return Err(serde::de::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value > MAX_EXACT_INTEGER {
            return Err(serde::de::Error::custom(format!(
                "JSON integer {value} exceeds the exact cross-language range"
            )));
        }
        Ok(UniqueValue(Value::Number(value.into())))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !value.is_finite() {
            return Err(E::custom("JSON number is not finite"));
        }
        if value.fract() == 0.0 {
            if value.abs() >= FIRST_UNSAFE_INTEGER_F64 {
                return Err(E::custom(format!(
                    "JSON integer {value} exceeds the exact cross-language range"
                )));
            }
            return Ok(UniqueValue(Value::Number(normalized_integral_number(
                value,
            ))));
        }
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("JSON number is not finite"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        UniqueValue::deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(UniqueValueSeed)? {
            values.push(value.0);
        }
        Ok(UniqueValue(Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate JSON object member {key:?}"
                )));
            }
            let value = object.next_value_seed(UniqueValueSeed)?;
            values.insert(key, value.0);
        }
        Ok(UniqueValue(Value::Object(values)))
    }
}

#[cfg(test)]
mod tests {
    use serde::Serialize;
    use serde_json::Value;

    use super::{canonical_bytes, canonical_digest, content_id, decode_json};

    #[test]
    fn strict_json_decode_rejects_duplicate_members_before_collapse() {
        assert!(decode_json::<Value>(br#"{"outer":{"value":1,"value":2}}"#).is_err());
        assert!(decode_json::<Value>(br#"{"value":1} {"value":2}"#).is_err());
        assert_eq!(
            decode_json::<Value>(br#"{"outer":{"value":1}}"#).expect("JSON decodes"),
            serde_json::json!({"outer": {"value": 1}})
        );
    }

    #[test]
    fn canonical_numbers_reject_unsafe_integers_recursively() {
        let unsafe_positive = serde_json::json!({"nested": [9_007_199_254_740_992_u64]});
        let unsafe_negative = serde_json::json!({"nested": [-9_007_199_254_740_992_i64]});
        assert!(canonical_bytes(&unsafe_positive).is_err());
        assert!(canonical_digest(&unsafe_negative).is_err());
        assert!(content_id("test.value/1", &unsafe_positive).is_err());
        assert!(decode_json::<Value>(br#"{"nested":[9007199254740992]}"#).is_err());
        assert!(decode_json::<Value>(br#"{"nested":[-9007199254740992]}"#).is_err());
    }

    #[test]
    fn canonical_numbers_keep_finite_fractional_and_safe_integral_equivalence() {
        assert_eq!(
            canonical_bytes(&serde_json::json!({"value": 1})).expect("integer canonicalizes"),
            canonical_bytes(&serde_json::json!({"value": 1.0}))
                .expect("integral float canonicalizes")
        );
        assert_eq!(
            decode_json::<Value>(br#"{"value":1e0}"#).expect("safe integral exponent decodes"),
            serde_json::json!({"value": 1})
        );
        assert!(decode_json::<Value>(br#"{"value":1.25}"#).is_ok());
        assert!(decode_json::<Value>(br#"{"value":0.00000000000000000001}"#).is_ok());
        assert!(decode_json::<Value>(br#"{"value":9007199254740991.0}"#).is_ok());
    }

    #[test]
    fn strict_json_preserves_fractional_vs_integer_token_semantics() {
        for input in [
            br#"{"value":9007199254740991.1}"#.as_slice(),
            br#"{"value":-9007199254740991.1}"#.as_slice(),
            br#"{"value":1e-10000}"#.as_slice(),
        ] {
            assert!(decode_json::<Value>(input).is_err());
        }
        assert_eq!(
            decode_json::<Value>(br#"{"value":100e-2}"#).expect("exact decimal integer decodes"),
            serde_json::json!({"value": 1})
        );
    }

    #[derive(Serialize)]
    struct NestedFloat {
        nested: Vec<f64>,
    }

    #[test]
    fn canonical_numbers_reject_non_finite_and_unsafe_integral_floats() {
        assert!(
            canonical_bytes(&NestedFloat {
                nested: vec![f64::NAN],
            })
            .is_err()
        );
        assert!(
            canonical_bytes(&NestedFloat {
                nested: vec![9_007_199_254_740_992.0],
            })
            .is_err()
        );
    }
}
