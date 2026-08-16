use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{CoreError, Result};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Serialize a value using RFC 8785 JSON Canonicalization Scheme.
pub fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json_canonicalizer::to_vec(value).map_err(|error| CoreError::Encoding(error.to_string()))
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
pub fn content_id<T: Serialize>(domain: &str, value: &T) -> Result<String> {
    let bytes = canonical_bytes(value)?;
    let mut input = Vec::with_capacity(domain.len() + bytes.len() + 1);
    input.extend_from_slice(domain.as_bytes());
    input.push(0);
    input.extend_from_slice(&bytes);
    Ok(format!("sha256:{}", sha256_bytes(&input)))
}

/// Compute a canonical digest without an identity prefix.
pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String> {
    Ok(sha256_bytes(&canonical_bytes(value)?))
}
