//! Provider-neutral authenticated persistent collections.
//!
//! This crate owns the only physical hash preimages, node verification, proof
//! verification, and deterministic mutation algorithms for Cymule's compressed
//! map and ordered AVL log. Collection values are opaque content identities;
//! semantic decoding and durable object I/O remain in higher layers.

mod error;
mod hash;
mod log;
mod map;

use std::fmt;
use std::marker::PhantomData;

use serde::Deserializer;
use serde::de::{Deserialize, DeserializeOwned, IgnoredAny, SeqAccess, Visitor};

pub use error::{CollectionError, ProviderConflict, ProviderFailure, Result};
pub use log::{
    LogApplyOutput, LogApplyProof, LogAudit, LogBuildOutput, LogExactProof, LogMutation, LogNode,
    LogObject, LogRangeProof, LogRoot, LogSplitOutput, LogSplitProof, VerifiedLogApply,
    VerifiedLogRange, VerifiedLogRead, VerifiedLogSplit, apply_log_mutations, audit_log, build_log,
    decode_log_apply_proof, decode_log_exact_proof, decode_log_node, decode_log_range_proof,
    decode_log_split_proof, prove_log_exact, prove_log_range, split_log, verify_log_apply,
    verify_log_exact, verify_log_range, verify_log_split,
};
pub use map::{
    ExpectedMapValue, MapApplyOutput, MapApplyProof, MapAudit, MapBuildOutput, MapExactProof,
    MapMutation, MapNode, MapObject, MapPosition, MapRangeProof, MapRoot, VerifiedMapApply,
    VerifiedMapPage, VerifiedMapRead, apply_map_mutations, audit_map, build_map,
    decode_map_apply_proof, decode_map_exact_proof, decode_map_node, decode_map_position,
    decode_map_range_proof, map_key_hash, prove_map_exact, prove_map_range, verify_map_apply,
    verify_map_exact, verify_map_range,
};

/// Largest exact integer shared by every persisted collection counter.
pub const MAX_EXACT_INTEGER: u64 = 9_007_199_254_740_991;
/// Maximum compressed-map path length, including its terminal node.
pub const MAX_MAP_PATH_NODES: usize = 257;
/// Maximum number of values in one ordered-log leaf.
pub const LOG_FANOUT: usize = 32;
/// Maximum supported AVL log height and exact proof-path node count.
pub const MAX_LOG_HEIGHT: usize = 76;
/// Maximum entries returned by one authenticated page.
pub const MAX_PAGE_ENTRIES: usize = 256;
/// Maximum aggregate key, derived-key-hash, and value-identity bytes in one page.
pub const MAX_PAGE_BYTES: usize = 1024 * 1024;
/// Maximum map key bytes that still admit one key/hash/value page entry.
pub const MAX_MAP_KEY_BYTES: usize = MAX_PAGE_BYTES - 64 - 71;
/// Maximum exact-key mutations accepted by one ordinary map apply.
pub const MAX_MAP_MUTATIONS_PER_APPLY: usize = 256;
/// Maximum ordered mutations accepted by one ordinary log apply.
pub const MAX_LOG_MUTATIONS_PER_APPLY: usize = 256;
/// Maximum primitive appended/removed values in one ordinary log apply.
pub const MAX_LOG_VALUES_PER_APPLY: usize = 256;
/// Maximum replacement values accepted by one constant-state prefix rewrite.
pub const MAX_LOG_PREFIX_REPLACEMENT_VALUES: usize = 16;
/// Maximum aggregate key/value bytes in one ordinary mutation batch.
pub const MAX_MUTATION_BYTES: usize = 4 * 1024 * 1024;
/// Maximum logical node bytes accepted in one portable proof.
pub const MAX_PROOF_BYTES: usize = 32 * 1024 * 1024;
/// Maximum transport bytes accepted before decoding one immutable node.
pub const MAX_NODE_TRANSPORT_BYTES: usize = (2 * MAX_MAP_KEY_BYTES) + 4096;
/// Maximum transport bytes accepted before decoding one portable proof.
pub const MAX_PROOF_TRANSPORT_BYTES: usize = (2
    * (MAX_PROOF_BYTES + MAX_MUTATION_BYTES + MAX_PAGE_BYTES))
    + (MAX_LOG_HEIGHT * MAX_LOG_VALUES_PER_APPLY * 4 * 256)
    + (MAX_MAP_MUTATIONS_PER_APPLY * 256)
    + (MAX_PAGE_ENTRIES * 512)
    + (MAX_LOG_VALUES_PER_APPLY * 4)
    + 4096;

fn decode_json_bounded<T: DeserializeOwned>(
    bytes: &[u8],
    maximum: usize,
    label: &str,
) -> Result<T> {
    if bytes.len() > maximum {
        return Err(CollectionError::Validation(format!(
            "{label} transport exceeds {maximum} bytes"
        )));
    }
    serde_json::from_slice(bytes)
        .map_err(|error| CollectionError::Validation(format!("invalid {label} transport: {error}")))
}

fn deserialize_bounded_vec<'de, D, T, const MAXIMUM: usize>(
    deserializer: D,
) -> std::result::Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T, const MAXIMUM: usize>(PhantomData<T>);

    impl<'de, T, const MAXIMUM: usize> Visitor<'de> for BoundedVecVisitor<T, MAXIMUM>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "an array with at most {MAXIMUM} elements")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence.size_hint().is_some_and(|length| length > MAXIMUM) {
                return Err(serde::de::Error::custom(format!(
                    "array exceeds {MAXIMUM} elements"
                )));
            }
            let mut values = Vec::new();
            while values.len() < MAXIMUM {
                let Some(value) = sequence.next_element()? else {
                    return Ok(values);
                };
                values.push(value);
            }
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(format!(
                    "array exceeds {MAXIMUM} elements"
                )));
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor::<T, MAXIMUM>(PhantomData))
}

/// Immutable-node resolver implemented by a physical object provider.
pub trait CollectionResolver {
    /// Load one exact compressed-map node by content identity.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the immutable object cannot be resolved.
    fn load_map_node(&mut self, object_id: &str) -> Result<Option<MapNode>>;

    /// Load one exact ordered-log node by content identity.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the immutable object cannot be resolved.
    fn load_log_node(&mut self, object_id: &str) -> Result<Option<LogNode>>;
}
