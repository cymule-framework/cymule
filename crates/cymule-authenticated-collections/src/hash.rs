use sha2::{Digest, Sha256};

use crate::{CollectionError, Result};

const HEX: &[u8; 16] = b"0123456789abcdef";
const FRAME_VERSION: &[u8] = b"cymule.authenticated-collection-preimage/1";

pub(crate) fn hash_identifier(domain: &str, fields: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    frame_into(&mut hasher, FRAME_VERSION);
    frame_into(&mut hasher, domain.as_bytes());
    frame_into(
        &mut hasher,
        &u64::try_from(fields.len())
            .expect("field count fits u64")
            .to_be_bytes(),
    );
    for field in fields {
        frame_into(&mut hasher, field);
    }
    format!("sha256:{}", hex_digest(hasher.finalize().as_slice()))
}

pub(crate) fn hash_digest(domain: &str, fields: &[&[u8]]) -> String {
    hash_identifier(domain, fields)
        .strip_prefix("sha256:")
        .expect("hash identifier has prefix")
        .to_owned()
}

fn frame_into(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(
        u64::try_from(bytes.len())
            .expect("in-memory field length fits u64")
            .to_be_bytes(),
    );
    hasher.update(bytes);
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) fn validate_content_id(context: &str, value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(CollectionError::Validation(format!(
            "{context} must be a lowercase SHA-256 content ID"
        )));
    };
    validate_digest(context, digest)
}

pub(crate) fn validate_digest(context: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !is_lower_hex(value) {
        return Err(CollectionError::Validation(format!(
            "{context} must be a 64-character lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

pub(crate) fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub(crate) fn hash_bit(value: &str, depth: u16) -> Result<bool> {
    if depth >= 256 {
        return Err(CollectionError::Validation(
            "map hash-bit depth exceeds SHA-256".to_owned(),
        ));
    }
    validate_digest("map key hash", value)?;
    let nibble_index = usize::from(depth / 4);
    let bit_index = 3_u8
        .checked_sub(u8::try_from(depth % 4).expect("depth modulo four fits u8"))
        .expect("depth modulo four does not exceed three");
    let nibble = hex_value(value.as_bytes()[nibble_index])
        .ok_or_else(|| CollectionError::Validation("map key hash is not hexadecimal".to_owned()))?;
    Ok((nibble & (1 << bit_index)) != 0)
}

pub(crate) fn common_prefix_bits(left: &str, right: &str) -> Result<u16> {
    validate_digest("left map key hash", left)?;
    validate_digest("right map key hash", right)?;
    for depth in 0..256_u16 {
        if hash_bit(left, depth)? != hash_bit(right, depth)? {
            return Ok(depth);
        }
    }
    Ok(256)
}

pub(crate) fn normalized_hash_prefix(value: &str, depth: u16) -> Result<String> {
    validate_digest("map prefix source", value)?;
    if depth >= 256 {
        return Err(CollectionError::Validation(
            "map prefix depth exceeds SHA-256".to_owned(),
        ));
    }
    let mut output = value.as_bytes().to_vec();
    let full_nibbles = usize::from(depth / 4);
    let remaining_bits = u8::try_from(depth % 4).expect("depth modulo four fits u8");
    if remaining_bits > 0 {
        let nibble = hex_value(output[full_nibbles]).expect("validated hexadecimal");
        let mask = 0x0f_u8 << (4 - remaining_bits);
        output[full_nibbles] = hex_digit(nibble & mask);
    }
    let clear_from = full_nibbles + usize::from(remaining_bits > 0);
    output[clear_from..].fill(b'0');
    String::from_utf8(output).map_err(|error| CollectionError::Validation(error.to_string()))
}

pub(crate) fn hash_matches_prefix(hash: &str, prefix: &str, depth: u16) -> Result<bool> {
    Ok(normalized_hash_prefix(hash, depth)? == prefix)
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn hex_digit(value: u8) -> u8 {
    HEX[usize::from(value)]
}
