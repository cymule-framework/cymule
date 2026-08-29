use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::hash::{
    common_prefix_bits, hash_bit, hash_digest, hash_identifier, hash_matches_prefix,
    normalized_hash_prefix, validate_content_id, validate_digest,
};
use crate::{
    CollectionError, CollectionResolver, MAX_EXACT_INTEGER, MAX_MAP_KEY_BYTES,
    MAX_MAP_MUTATIONS_PER_APPLY, MAX_MAP_PATH_NODES, MAX_MUTATION_BYTES, MAX_PAGE_BYTES,
    MAX_PAGE_ENTRIES, MAX_PROOF_BYTES, Result,
};

/// Compressed-map node schema owned by this crate.
pub const MAP_NODE_VERSION: &str = "cymule.authenticated-map-node/1";
/// Compressed-map key hashing domain owned by this crate.
pub const MAP_KEY_VERSION: &str = "cymule.authenticated-map-key/1";
const MAP_MUTATION_VERSION: &str = "cymule.authenticated-map-mutation/1";
const MAX_MAP_APPLY_PROOF_NODES: usize = MAX_MAP_PATH_NODES * MAX_MAP_MUTATIONS_PER_APPLY;
const MAX_MAP_RANGE_PROOF_NODES: usize = (MAX_MAP_PATH_NODES * 4) + (MAX_PAGE_ENTRIES * 2) + 4;

fn deserialize_map_path_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<MapNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, MapNodeWire, MAX_MAP_PATH_NODES>(deserializer)
}

fn deserialize_map_range_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<MapNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, MapNodeWire, MAX_MAP_RANGE_PROOF_NODES>(deserializer)
}

fn deserialize_map_mutations<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<MapMutation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, MapMutation, MAX_MAP_MUTATIONS_PER_APPLY>(deserializer)
}

fn deserialize_map_apply_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<MapNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, MapNodeWire, MAX_MAP_APPLY_PROOF_NODES>(deserializer)
}

/// Root of one immutable compressed SHA-256-keyed map.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MapRoot {
    /// Root-node content identity; absent exactly when the map is empty.
    pub node: Option<String>,
    /// Exact number of key/value entries.
    pub entries: u64,
}

impl MapRoot {
    /// Construct the unique empty map root.
    pub const fn empty() -> Self {
        Self {
            node: None,
            entries: 0,
        }
    }

    /// Verify the closed root shape.
    ///
    /// # Errors
    ///
    /// Returns an error when identity presence and count disagree or a bound is
    /// exceeded.
    pub fn verify(&self) -> Result<()> {
        if (self.entries == 0) != self.node.is_none() || self.entries > MAX_EXACT_INTEGER {
            return Err(CollectionError::Validation(
                "authenticated-map root has inconsistent identity or entry count".to_owned(),
            ));
        }
        if let Some(node) = &self.node {
            validate_content_id("authenticated-map root", node)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MapChild {
    object_id: String,
    entries: u64,
}

impl MapChild {
    fn verify(&self) -> Result<()> {
        validate_content_id("authenticated-map child", &self.object_id)?;
        if self.entries == 0 || self.entries > MAX_EXACT_INTEGER {
            return Err(CollectionError::Validation(
                "authenticated-map child has an invalid entry count".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum MapNodeBody {
    Leaf {
        key: String,
        key_hash: String,
        value: String,
    },
    Branch {
        depth: u16,
        prefix: String,
        left: MapChild,
        right: MapChild,
    },
}

/// One immutable compressed-map node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapNode {
    /// Exact node schema.
    pub node_version: String,
    /// Content identity derived from the closed binary preimage.
    pub object_id: String,
    body: MapNodeBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapNodeWire {
    node_version: String,
    object_id: String,
    body: MapNodeBody,
}

impl MapNodeWire {
    fn into_verified(self) -> Result<MapNode> {
        let node = MapNode {
            node_version: self.node_version,
            object_id: self.object_id,
            body: self.body,
        };
        node.verify()?;
        Ok(node)
    }
}

/// Decode one immutable map node only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input, or when the node bytes do
/// not satisfy the exact schema and content identity.
pub fn decode_map_node(bytes: &[u8]) -> Result<MapNode> {
    crate::decode_json_bounded::<MapNodeWire>(
        bytes,
        crate::MAX_NODE_TRANSPORT_BYTES,
        "authenticated-map node",
    )?
    .into_verified()
}

/// Map-node object persisted by a provider.
pub type MapObject = MapNode;

impl MapNode {
    fn leaf(key: String, key_hash: String, value: String) -> Result<Self> {
        Self::from_body(MapNodeBody::Leaf {
            key,
            key_hash,
            value,
        })
    }

    fn branch(depth: u16, prefix: String, left: MapChild, right: MapChild) -> Result<Self> {
        Self::from_body(MapNodeBody::Branch {
            depth,
            prefix,
            left,
            right,
        })
    }

    fn from_body(body: MapNodeBody) -> Result<Self> {
        let object_id = map_node_id(&body);
        let node = Self {
            node_version: MAP_NODE_VERSION.to_owned(),
            object_id,
            body,
        };
        node.verify()?;
        Ok(node)
    }

    /// Verify local shape and the exact node preimage.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid key, prefix, child, count, or identity.
    pub fn verify(&self) -> Result<()> {
        if self.node_version != MAP_NODE_VERSION {
            return Err(CollectionError::Validation(format!(
                "unsupported authenticated-map node version {:?}",
                self.node_version
            )));
        }
        match &self.body {
            MapNodeBody::Leaf {
                key,
                key_hash,
                value,
            } => {
                validate_key(key)?;
                if *key_hash != map_key_hash(key)? {
                    return Err(CollectionError::Integrity {
                        code: "map_key_hash_mismatch",
                        message: format!("authenticated-map key {key:?} has the wrong hash"),
                    });
                }
                validate_content_id("authenticated-map value", value)?;
            }
            MapNodeBody::Branch {
                depth,
                prefix,
                left,
                right,
            } => {
                if *depth >= 256 {
                    return Err(CollectionError::Validation(
                        "authenticated-map branch depth exceeds SHA-256".to_owned(),
                    ));
                }
                validate_digest("authenticated-map branch prefix", prefix)?;
                if normalized_hash_prefix(prefix, *depth)? != *prefix {
                    return Err(CollectionError::Validation(
                        "authenticated-map branch prefix is not normalized".to_owned(),
                    ));
                }
                left.verify()?;
                right.verify()?;
                let _ = left
                    .entries
                    .checked_add(right.entries)
                    .filter(|entries| *entries <= MAX_EXACT_INTEGER)
                    .ok_or_else(|| {
                        CollectionError::Validation(
                            "authenticated-map branch entry count overflowed".to_owned(),
                        )
                    })?;
            }
        }
        let expected = map_node_id(&self.body);
        if self.object_id != expected {
            return Err(CollectionError::Integrity {
                code: "map_node_identity_mismatch",
                message: format!(
                    "authenticated-map node identity {} does not match {expected}",
                    self.object_id
                ),
            });
        }
        Ok(())
    }

    /// Exact number of entries committed by this node.
    ///
    /// # Errors
    ///
    /// Returns an error when child counts overflow the shared exact range.
    pub fn entries(&self) -> Result<u64> {
        match &self.body {
            MapNodeBody::Leaf { .. } => Ok(1),
            MapNodeBody::Branch { left, right, .. } => left
                .entries
                .checked_add(right.entries)
                .filter(|entries| *entries <= MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    CollectionError::Validation(
                        "authenticated-map node entry count overflowed".to_owned(),
                    )
                }),
        }
    }

    /// Immutable child-node identities referenced by this node.
    pub fn child_object_ids(&self) -> Vec<&str> {
        match &self.body {
            MapNodeBody::Leaf { .. } => Vec::new(),
            MapNodeBody::Branch { left, right, .. } => {
                vec![left.object_id.as_str(), right.object_id.as_str()]
            }
        }
    }

    /// Opaque semantic value identity referenced by a leaf.
    pub fn value_object_id(&self) -> Option<&str> {
        match &self.body {
            MapNodeBody::Leaf { value, .. } => Some(value),
            MapNodeBody::Branch { .. } => None,
        }
    }

    fn logical_bytes(&self) -> Result<usize> {
        let fixed = self
            .node_version
            .len()
            .checked_add(self.object_id.len())
            .ok_or_else(|| CollectionError::Validation("map node bytes overflowed".to_owned()))?;
        match &self.body {
            MapNodeBody::Leaf {
                key,
                key_hash,
                value,
            } => fixed
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(key_hash.len()))
                .and_then(|bytes| bytes.checked_add(value.len())),
            MapNodeBody::Branch {
                prefix,
                left,
                right,
                ..
            } => fixed
                .checked_add(prefix.len())
                .and_then(|bytes| bytes.checked_add(left.object_id.len()))
                .and_then(|bytes| bytes.checked_add(right.object_id.len()))
                .and_then(|bytes| bytes.checked_add(32)),
        }
        .ok_or_else(|| CollectionError::Validation("map node bytes overflowed".to_owned()))
    }
}

fn map_node_id(body: &MapNodeBody) -> String {
    match body {
        MapNodeBody::Leaf {
            key,
            key_hash,
            value,
        } => hash_identifier(
            MAP_NODE_VERSION,
            &[
                b"leaf",
                key.as_bytes(),
                key_hash.as_bytes(),
                value.as_bytes(),
            ],
        ),
        MapNodeBody::Branch {
            depth,
            prefix,
            left,
            right,
        } => hash_identifier(
            MAP_NODE_VERSION,
            &[
                b"branch",
                &depth.to_be_bytes(),
                prefix.as_bytes(),
                left.object_id.as_bytes(),
                &left.entries.to_be_bytes(),
                right.object_id.as_bytes(),
                &right.entries.to_be_bytes(),
            ],
        ),
    }
}

/// Derive the unique authenticated-map order hash for a key.
///
/// # Errors
///
/// Returns an error when the key is empty or exceeds the page byte bound.
pub fn map_key_hash(key: &str) -> Result<String> {
    validate_key(key)?;
    Ok(hash_digest(MAP_KEY_VERSION, &[key.as_bytes()]))
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(CollectionError::Validation(
            "authenticated-map key is empty".to_owned(),
        ));
    }
    if key.len() > MAX_MAP_KEY_BYTES {
        return Err(CollectionError::Validation(format!(
            "authenticated-map key exceeds {MAX_MAP_KEY_BYTES} UTF-8 bytes"
        )));
    }
    if key.chars().any(char::is_control) {
        return Err(CollectionError::Validation(
            "authenticated-map key contains a control character".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct NodeRef {
    object_id: String,
    entries: u64,
}

impl NodeRef {
    fn into_child(self) -> MapChild {
        MapChild {
            object_id: self.object_id,
            entries: self.entries,
        }
    }
}

impl From<MapChild> for NodeRef {
    fn from(child: MapChild) -> Self {
        Self {
            object_id: child.object_id,
            entries: child.entries,
        }
    }
}

#[derive(Debug, Clone)]
struct PathConstraint {
    depth: u16,
    prefix: String,
    right: bool,
}

fn verify_constraints(hash: &str, constraints: &[PathConstraint]) -> Result<()> {
    for constraint in constraints {
        if !hash_matches_prefix(hash, &constraint.prefix, constraint.depth)?
            || hash_bit(hash, constraint.depth)? != constraint.right
        {
            return Err(CollectionError::Integrity {
                code: "map_branch_partition_mismatch",
                message: "authenticated-map branch does not partition descendant hashes".to_owned(),
            });
        }
    }
    Ok(())
}

struct Overlay<'a, R: CollectionResolver + ?Sized> {
    resolver: &'a mut R,
    pending: BTreeMap<String, MapNode>,
    loaded: BTreeMap<String, MapNode>,
}

impl<'a, R: CollectionResolver + ?Sized> Overlay<'a, R> {
    fn new(resolver: &'a mut R) -> Self {
        Self {
            resolver,
            pending: BTreeMap::new(),
            loaded: BTreeMap::new(),
        }
    }

    fn load(&mut self, reference: &NodeRef) -> Result<MapNode> {
        let node = if let Some(node) = self.pending.get(&reference.object_id) {
            node.clone()
        } else if let Some(node) = self.loaded.get(&reference.object_id) {
            node.clone()
        } else {
            let node = self
                .resolver
                .load_map_node(&reference.object_id)?
                .ok_or_else(|| CollectionError::MissingObject(reference.object_id.clone()))?;
            node.verify()?;
            if node.object_id != reference.object_id {
                return Err(CollectionError::Integrity {
                    code: "map_node_locator_mismatch",
                    message: format!(
                        "map node locator {} resolves to {}",
                        reference.object_id, node.object_id
                    ),
                });
            }
            self.loaded.insert(node.object_id.clone(), node.clone());
            node
        };
        if node.entries()? != reference.entries {
            return Err(CollectionError::Integrity {
                code: "map_child_count_mismatch",
                message: format!(
                    "map child {} does not contain {} entries",
                    reference.object_id, reference.entries
                ),
            });
        }
        Ok(node)
    }

    fn store(&mut self, node: MapNode) -> Result<NodeRef> {
        node.verify()?;
        let reference = NodeRef {
            object_id: node.object_id.clone(),
            entries: node.entries()?,
        };
        if let Some(existing) = self.pending.get(&node.object_id)
            && existing != &node
        {
            return Err(CollectionError::Integrity {
                code: "map_node_identity_conflict",
                message: format!("map node {} has conflicting bytes", node.object_id),
            });
        }
        self.pending.insert(node.object_id.clone(), node);
        Ok(reference)
    }
}

/// Exact expected parent value for a map put.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ExpectedMapValue {
    /// The key must have an authenticated absence proof.
    Absent,
    /// The key must contain this exact opaque value identity.
    Exact(String),
}

/// One exact-key mutation over an authenticated map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum MapMutation {
    /// Insert or replace one key after checking its exact prior state.
    Put {
        /// Exact key.
        key: String,
        /// Exact authenticated prior state.
        expected: ExpectedMapValue,
        /// Opaque content identity of the new value.
        value: String,
    },
    /// Remove one exact existing key.
    Remove {
        /// Exact key.
        key: String,
        /// Exact opaque content identity that must be removed.
        expected: String,
    },
}

impl MapMutation {
    /// Construct an absence-checked insert.
    pub fn insert(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Put {
            key: key.into(),
            expected: ExpectedMapValue::Absent,
            value: value.into(),
        }
    }

    /// Construct an exact replacement.
    pub fn replace(
        key: impl Into<String>,
        expected: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self::Put {
            key: key.into(),
            expected: ExpectedMapValue::Exact(expected.into()),
            value: value.into(),
        }
    }

    /// Construct an exact removal.
    pub fn remove(key: impl Into<String>, expected: impl Into<String>) -> Self {
        Self::Remove {
            key: key.into(),
            expected: expected.into(),
        }
    }

    /// Exact key affected by this mutation.
    pub fn key(&self) -> &str {
        match self {
            Self::Put { key, .. } | Self::Remove { key, .. } => key,
        }
    }

    fn verify(&self) -> Result<()> {
        validate_key(self.key())?;
        match self {
            Self::Put {
                expected, value, ..
            } => {
                if let ExpectedMapValue::Exact(expected) = expected {
                    validate_content_id("expected map value", expected)?;
                    if expected == value {
                        return Err(CollectionError::Validation(
                            "map replacement must change the exact value".to_owned(),
                        ));
                    }
                }
                validate_content_id("new map value", value)
            }
            Self::Remove { expected, .. } => validate_content_id("removed map value", expected),
        }
    }
}

fn canonical_mutations(mutations: &[MapMutation]) -> Result<Vec<MapMutation>> {
    if !(1..=MAX_MAP_MUTATIONS_PER_APPLY).contains(&mutations.len()) {
        return Err(CollectionError::Validation(format!(
            "map mutation batch must contain 1..={MAX_MAP_MUTATIONS_PER_APPLY} entries"
        )));
    }
    let mut ordered = Vec::with_capacity(mutations.len());
    let mut keys = BTreeSet::new();
    let mut mutation_bytes = 0_usize;
    for mutation in mutations {
        mutation.verify()?;
        mutation_bytes = mutation_bytes
            .checked_add(map_mutation_bytes(mutation)?)
            .filter(|bytes| *bytes <= MAX_MUTATION_BYTES)
            .ok_or_else(|| {
                CollectionError::Validation(format!(
                    "map mutation batch exceeds {MAX_MUTATION_BYTES} logical bytes"
                ))
            })?;
        if !keys.insert(mutation.key().to_owned()) {
            return Err(CollectionError::Validation(format!(
                "map mutation batch repeats key {:?}",
                mutation.key()
            )));
        }
        ordered.push((
            map_key_hash(mutation.key())?,
            mutation.key().to_owned(),
            mutation.clone(),
        ));
    }
    ordered.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    Ok(ordered
        .into_iter()
        .map(|(_, _, mutation)| mutation)
        .collect())
}

fn map_mutation_bytes(mutation: &MapMutation) -> Result<usize> {
    match mutation {
        MapMutation::Put {
            key,
            expected,
            value,
        } => key
            .len()
            .checked_add(value.len())
            .and_then(|bytes| match expected {
                ExpectedMapValue::Absent => Some(bytes),
                ExpectedMapValue::Exact(expected) => bytes.checked_add(expected.len()),
            }),
        MapMutation::Remove { key, expected } => key.len().checked_add(expected.len()),
    }
    .ok_or_else(|| CollectionError::Validation("map mutation bytes overflowed".to_owned()))
}

fn mutation_digest(mutations: &[MapMutation]) -> String {
    let mut owned = Vec::new();
    for mutation in mutations {
        match mutation {
            MapMutation::Put {
                key,
                expected,
                value,
            } => {
                owned.push(b"put".to_vec());
                owned.push(key.as_bytes().to_vec());
                match expected {
                    ExpectedMapValue::Absent => owned.push(b"absent".to_vec()),
                    ExpectedMapValue::Exact(expected) => {
                        owned.push(b"exact".to_vec());
                        owned.push(expected.as_bytes().to_vec());
                    }
                }
                owned.push(value.as_bytes().to_vec());
            }
            MapMutation::Remove { key, expected } => {
                owned.push(b"remove".to_vec());
                owned.push(key.as_bytes().to_vec());
                owned.push(expected.as_bytes().to_vec());
            }
        }
    }
    let fields: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    hash_identifier(MAP_MUTATION_VERSION, &fields)
}

/// Raw exact-key proof transported across a provider boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapExactProof {
    root: MapRoot,
    key: String,
    key_hash: String,
    nodes: Vec<MapNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapExactProofWire {
    root: MapRoot,
    key: String,
    key_hash: String,
    #[serde(deserialize_with = "deserialize_map_path_nodes")]
    nodes: Vec<MapNodeWire>,
}

impl MapExactProofWire {
    fn into_proof(self) -> Result<MapExactProof> {
        Ok(MapExactProof {
            root: self.root,
            key: self.key,
            key_hash: self.key_hash,
            nodes: self
                .nodes
                .into_iter()
                .map(MapNodeWire::into_verified)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// Decode one exact map proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_map_exact`].
pub fn decode_map_exact_proof(bytes: &[u8]) -> Result<MapExactProof> {
    crate::decode_json_bounded::<MapExactProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-map exact proof",
    )?
    .into_proof()
}

/// Verified exact-key membership or canonical non-membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMapRead {
    root: MapRoot,
    key: String,
    value: Option<String>,
    rank: Option<u64>,
}

impl VerifiedMapRead {
    /// Exact root that authenticated this read.
    pub fn root(&self) -> &MapRoot {
        &self.root
    }

    /// Exact key authenticated by the proof.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Opaque value identity, or `None` only for verified non-membership.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Zero-based authenticated order rank for a member.
    pub fn rank(&self) -> Option<u64> {
        self.rank
    }
}

/// Generate one exact membership or canonical absence proof.
///
/// # Errors
///
/// Returns an error when the root or any resolved node is invalid.
pub fn prove_map_exact<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    resolver: &mut R,
) -> Result<MapExactProof> {
    root.verify()?;
    let key_hash = map_key_hash(key)?;
    let mut recorder = RecordingMapResolver::new(resolver);
    let _ = lookup(root, key, &key_hash, &mut recorder)?;
    Ok(MapExactProof {
        root: root.clone(),
        key: key.to_owned(),
        key_hash,
        nodes: recorder.nodes.into_values().collect(),
    })
}

/// Verify one exact membership or canonical absence proof against caller-owned
/// root and key authority.
///
/// # Errors
///
/// Returns an error for a wrong root, key, path, terminal, count, value, or
/// unused proof node.
pub fn verify_map_exact(
    expected_root: &MapRoot,
    expected_key: &str,
    proof: &MapExactProof,
) -> Result<VerifiedMapRead> {
    expected_root.verify()?;
    if &proof.root != expected_root || proof.key != expected_key {
        return Err(CollectionError::Integrity {
            code: "map_exact_authority_mismatch",
            message: "map exact proof is bound to another root or key".to_owned(),
        });
    }
    if proof.key_hash != map_key_hash(expected_key)? {
        return Err(CollectionError::Integrity {
            code: "map_exact_key_hash_mismatch",
            message: "map exact proof carries the wrong key hash".to_owned(),
        });
    }
    let mut resolver = ProofMapResolver::new(&proof.nodes)?;
    let outcome = lookup(expected_root, expected_key, &proof.key_hash, &mut resolver)?;
    resolver.finish()?;
    Ok(VerifiedMapRead {
        root: expected_root.clone(),
        key: expected_key.to_owned(),
        value: outcome.value,
        rank: outcome.rank,
    })
}

#[derive(Debug)]
struct LookupOutcome {
    value: Option<String>,
    rank: Option<u64>,
}

fn lookup<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    key_hash: &str,
    resolver: &mut R,
) -> Result<LookupOutcome> {
    root.verify()?;
    let Some(object_id) = &root.node else {
        return Ok(LookupOutcome {
            value: None,
            rank: None,
        });
    };
    let mut reference = NodeRef {
        object_id: object_id.clone(),
        entries: root.entries,
    };
    let mut constraints = Vec::new();
    let mut rank = 0_u64;
    for _ in 0..MAX_MAP_PATH_NODES {
        let node = load_resolved_map_node(resolver, &reference)?;
        match node.body {
            MapNodeBody::Leaf {
                key: stored_key,
                key_hash: stored_hash,
                value,
            } => {
                verify_constraints(&stored_hash, &constraints)?;
                return if stored_key == key && stored_hash == key_hash {
                    Ok(LookupOutcome {
                        value: Some(value),
                        rank: Some(rank),
                    })
                } else if stored_hash == key_hash {
                    Err(CollectionError::Integrity {
                        code: "map_key_hash_collision",
                        message: "distinct map keys have the same SHA-256 hash".to_owned(),
                    })
                } else {
                    Ok(LookupOutcome {
                        value: None,
                        rank: None,
                    })
                };
            }
            MapNodeBody::Branch {
                depth,
                prefix,
                left,
                right,
            } => {
                verify_constraints(&prefix, &constraints)?;
                if constraints
                    .last()
                    .is_some_and(|constraint: &PathConstraint| constraint.depth >= depth)
                {
                    return Err(CollectionError::Integrity {
                        code: "map_branch_depth_mismatch",
                        message: "map branch depth does not increase along the path".to_owned(),
                    });
                }
                if !hash_matches_prefix(key_hash, &prefix, depth)? {
                    return Ok(LookupOutcome {
                        value: None,
                        rank: None,
                    });
                }
                let goes_right = hash_bit(key_hash, depth)?;
                constraints.push(PathConstraint {
                    depth,
                    prefix,
                    right: goes_right,
                });
                if goes_right {
                    rank = rank
                        .checked_add(left.entries)
                        .filter(|value| *value < MAX_EXACT_INTEGER)
                        .ok_or_else(|| CollectionError::Integrity {
                            code: "map_rank_overflow",
                            message: "map membership rank overflowed".to_owned(),
                        })?;
                    reference = right.into();
                } else {
                    reference = left.into();
                }
            }
        }
    }
    Err(CollectionError::Integrity {
        code: "map_path_bound_exceeded",
        message: "map exact path exceeds the SHA-256 depth bound".to_owned(),
    })
}

fn load_resolved_map_node<R: CollectionResolver + ?Sized>(
    resolver: &mut R,
    reference: &NodeRef,
) -> Result<MapNode> {
    let node = resolver
        .load_map_node(&reference.object_id)?
        .ok_or_else(|| CollectionError::MissingObject(reference.object_id.clone()))?;
    node.verify()?;
    if node.object_id != reference.object_id || node.entries()? != reference.entries {
        return Err(CollectionError::Integrity {
            code: "map_resolved_child_mismatch",
            message: format!(
                "resolved map node {} contradicts its parent edge",
                reference.object_id
            ),
        });
    }
    Ok(node)
}

struct RecordingMapResolver<'a, R: CollectionResolver + ?Sized> {
    inner: &'a mut R,
    nodes: BTreeMap<String, MapNode>,
}

impl<'a, R: CollectionResolver + ?Sized> RecordingMapResolver<'a, R> {
    fn new(inner: &'a mut R) -> Self {
        Self {
            inner,
            nodes: BTreeMap::new(),
        }
    }
}

impl<R: CollectionResolver + ?Sized> CollectionResolver for RecordingMapResolver<'_, R> {
    fn load_map_node(&mut self, object_id: &str) -> Result<Option<MapNode>> {
        if let Some(node) = self.nodes.get(object_id) {
            return Ok(Some(node.clone()));
        }
        let node = self.inner.load_map_node(object_id)?;
        if let Some(node) = &node {
            self.nodes.insert(object_id.to_owned(), node.clone());
        }
        Ok(node)
    }

    fn load_log_node(&mut self, object_id: &str) -> Result<Option<crate::LogNode>> {
        self.inner.load_log_node(object_id)
    }
}

struct ProofMapResolver {
    nodes: BTreeMap<String, MapNode>,
    used: BTreeSet<String>,
}

impl ProofMapResolver {
    fn new(nodes: &[MapNode]) -> Result<Self> {
        Self::new_bounded(nodes, MAX_MAP_PATH_NODES, "map exact proof")
    }

    fn new_bounded(nodes: &[MapNode], maximum: usize, label: &str) -> Result<Self> {
        if nodes.len() > maximum {
            return Err(CollectionError::Validation(format!(
                "{label} exceeds its node bound"
            )));
        }
        verify_map_proof_bytes(nodes)?;
        let mut indexed = BTreeMap::new();
        for node in nodes {
            node.verify()?;
            if indexed
                .insert(node.object_id.clone(), node.clone())
                .is_some()
            {
                return Err(CollectionError::Integrity {
                    code: "map_proof_duplicate_node",
                    message: format!("map proof repeats node {}", node.object_id),
                });
            }
        }
        Ok(Self {
            nodes: indexed,
            used: BTreeSet::new(),
        })
    }

    fn finish(self) -> Result<()> {
        if self.used.len() != self.nodes.len() {
            return Err(CollectionError::Integrity {
                code: "map_proof_unused_node",
                message: "map exact proof includes a node outside its canonical search path"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

impl CollectionResolver for ProofMapResolver {
    fn load_map_node(&mut self, object_id: &str) -> Result<Option<MapNode>> {
        let node = self.nodes.get(object_id).cloned();
        if node.is_some() {
            self.used.insert(object_id.to_owned());
        }
        Ok(node)
    }

    fn load_log_node(&mut self, _object_id: &str) -> Result<Option<crate::LogNode>> {
        Ok(None)
    }
}

/// Full authenticated position of one map entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapPosition {
    /// Exact key.
    key: String,
    /// Exact derived order hash.
    key_hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapPositionWire {
    key: String,
    key_hash: String,
}

impl MapPositionWire {
    fn into_position(self) -> Result<MapPosition> {
        let position = MapPosition {
            key: self.key,
            key_hash: self.key_hash,
        };
        position.verify()?;
        Ok(position)
    }
}

/// Decode one authenticated map cursor only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or when the key and its
/// derived SHA-256 position disagree.
pub fn decode_map_position(bytes: &[u8]) -> Result<MapPosition> {
    crate::decode_json_bounded::<MapPositionWire>(
        bytes,
        crate::MAX_NODE_TRANSPORT_BYTES,
        "authenticated-map position",
    )?
    .into_position()
}

impl MapPosition {
    /// Derive the unique authenticated position for an exact key.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is empty or exceeds the collection bound.
    pub fn for_key(key: &str) -> Result<Self> {
        Ok(Self {
            key: key.to_owned(),
            key_hash: map_key_hash(key)?,
        })
    }

    /// Exact key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Exact derived order hash.
    pub fn key_hash(&self) -> &str {
        &self.key_hash
    }

    fn verify(&self) -> Result<()> {
        if self.key_hash != map_key_hash(&self.key)? {
            return Err(CollectionError::Validation(
                "map position key and hash disagree".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Raw bounded, rank-contiguous map page proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapRangeProof {
    root: MapRoot,
    after: Option<MapPosition>,
    limit: u16,
    max_bytes: u64,
    nodes: Vec<MapNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapRangeProofWire {
    root: MapRoot,
    after: Option<MapPositionWire>,
    limit: u16,
    max_bytes: u64,
    #[serde(deserialize_with = "deserialize_map_range_nodes")]
    nodes: Vec<MapNodeWire>,
}

impl MapRangeProofWire {
    fn into_proof(self) -> Result<MapRangeProof> {
        Ok(MapRangeProof {
            root: self.root,
            after: self.after.map(MapPositionWire::into_position).transpose()?,
            limit: self.limit,
            max_bytes: self.max_bytes,
            nodes: self
                .nodes
                .into_iter()
                .map(MapNodeWire::into_verified)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// Decode one map range proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_map_range`].
pub fn decode_map_range_proof(bytes: &[u8]) -> Result<MapRangeProof> {
    crate::decode_json_bounded::<MapRangeProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-map range proof",
    )?
    .into_proof()
}

/// Verified bounded, omission-free page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMapPage {
    root: MapRoot,
    after: Option<MapPosition>,
    entries: Vec<(MapPosition, String)>,
    next_position: Option<MapPosition>,
}

impl VerifiedMapPage {
    /// Exact source root.
    pub fn root(&self) -> &MapRoot {
        &self.root
    }

    /// Exact authenticated cursor, if any.
    pub fn after(&self) -> Option<&MapPosition> {
        self.after.as_ref()
    }

    /// Ordered exact entries.
    pub fn entries(&self) -> &[(MapPosition, String)] {
        &self.entries
    }

    /// Cursor for the next page, present only when an authenticated successor
    /// exists.
    pub fn next_position(&self) -> Option<&MapPosition> {
        self.next_position.as_ref()
    }

    /// Whether the page has an authenticated successor.
    pub const fn has_more(&self) -> bool {
        self.next_position.is_some()
    }
}

/// Generate one bounded omission-free page in `(key_hash, key)` order.
///
/// # Errors
///
/// Returns an error for an invalid cursor, limit, byte budget, root, or node.
pub fn prove_map_range<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    after: Option<&MapPosition>,
    limit: usize,
    max_bytes: usize,
    resolver: &mut R,
) -> Result<MapRangeProof> {
    validate_page_request(limit, max_bytes)?;
    root.verify()?;
    let mut recorder = RecordingMapResolver::new(resolver);
    let start = map_range_start(root, after, &mut recorder)?;
    let _ = collect_map_range(root, start, limit, max_bytes, &mut recorder)?;
    Ok(MapRangeProof {
        root: root.clone(),
        after: after.cloned(),
        limit: u16::try_from(limit)
            .map_err(|error| CollectionError::Validation(error.to_string()))?,
        max_bytes: u64::try_from(max_bytes)
            .map_err(|error| CollectionError::Validation(error.to_string()))?,
        nodes: recorder.nodes.into_values().collect(),
    })
}

/// Verify one bounded omission-free page against exact request authority.
///
/// # Errors
///
/// Returns an error for a wrong root/cursor/request, skipped or reordered rank,
/// substituted value, false terminal boundary, or invalid member proof.
pub fn verify_map_range(
    expected_root: &MapRoot,
    expected_after: Option<&MapPosition>,
    limit: usize,
    max_bytes: usize,
    proof: &MapRangeProof,
) -> Result<VerifiedMapPage> {
    validate_page_request(limit, max_bytes)?;
    expected_root.verify()?;
    if &proof.root != expected_root
        || proof.after.as_ref() != expected_after
        || usize::from(proof.limit) != limit
        || usize::try_from(proof.max_bytes).ok() != Some(max_bytes)
    {
        return Err(CollectionError::Integrity {
            code: "map_range_authority_mismatch",
            message: "map range proof is bound to another root, cursor, or request".to_owned(),
        });
    }
    if proof.nodes.len() > MAX_MAP_RANGE_PROOF_NODES {
        return Err(CollectionError::Validation(
            "map range proof exceeds its node bound".to_owned(),
        ));
    }
    verify_map_proof_bytes(&proof.nodes)?;
    let mut resolver =
        ProofMapResolver::new_bounded(&proof.nodes, MAX_MAP_RANGE_PROOF_NODES, "map range proof")?;
    let start = map_range_start(expected_root, expected_after, &mut resolver)?;
    let selection = collect_map_range(expected_root, start, limit, max_bytes, &mut resolver)?;
    resolver.finish()?;
    let next_position = selection.boundary.as_ref().and_then(|_| {
        selection
            .entries
            .last()
            .map(|(position, _)| position.clone())
    });
    Ok(VerifiedMapPage {
        root: expected_root.clone(),
        after: expected_after.cloned(),
        entries: selection.entries,
        next_position,
    })
}

fn map_range_start<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    after: Option<&MapPosition>,
    resolver: &mut R,
) -> Result<u64> {
    let Some(after) = after else {
        return Ok(0);
    };
    after.verify()?;
    let outcome = lookup(root, &after.key, &after.key_hash, resolver)?;
    if outcome.value.is_none() {
        return Err(CollectionError::Conflict(
            "map range cursor is not a member of its source root".to_owned(),
        ));
    }
    outcome
        .rank
        .and_then(|rank| rank.checked_add(1))
        .ok_or_else(|| CollectionError::Integrity {
            code: "map_page_rank_overflow",
            message: "map page cursor rank overflowed".to_owned(),
        })
}

struct MapRangeSelection {
    entries: Vec<(MapPosition, String)>,
    used_bytes: usize,
    boundary: Option<(MapPosition, String)>,
}

struct MapRangeTraversal {
    start: u64,
    limit: usize,
    max_bytes: usize,
    constraints: Vec<PathConstraint>,
    selection: MapRangeSelection,
}

impl MapRangeTraversal {
    fn new(start: u64, limit: usize, max_bytes: usize) -> Self {
        Self {
            start,
            limit,
            max_bytes,
            constraints: Vec::new(),
            selection: MapRangeSelection {
                entries: Vec::new(),
                used_bytes: 0,
                boundary: None,
            },
        }
    }

    fn visit<R: CollectionResolver + ?Sized>(
        &mut self,
        reference: &NodeRef,
        base_rank: u64,
        resolver: &mut R,
    ) -> Result<()> {
        if self.selection.boundary.is_some() {
            return Ok(());
        }
        let end_rank =
            base_rank
                .checked_add(reference.entries)
                .ok_or_else(|| CollectionError::Integrity {
                    code: "map_range_rank_overflow",
                    message: "map range subtree rank overflowed".to_owned(),
                })?;
        if end_rank <= self.start {
            return Ok(());
        }
        let node = load_resolved_map_node(resolver, reference)?;
        match node.body {
            MapNodeBody::Leaf {
                key,
                key_hash,
                value,
            } => self.visit_leaf(base_rank, key, key_hash, value),
            MapNodeBody::Branch {
                depth,
                prefix,
                left,
                right,
            } => self.visit_branch(base_rank, depth, prefix, left, right, resolver),
        }
    }

    fn visit_leaf(
        &mut self,
        base_rank: u64,
        key: String,
        key_hash: String,
        value: String,
    ) -> Result<()> {
        verify_constraints(&key_hash, &self.constraints)?;
        if base_rank < self.start {
            return Ok(());
        }
        let position = MapPosition { key, key_hash };
        position.verify()?;
        validate_content_id("map range value", &value)?;
        let entry_bytes = page_entry_bytes(&position, &value)?;
        let next_bytes = self
            .selection
            .used_bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| {
                CollectionError::Validation("map page byte count overflowed".to_owned())
            })?;
        if self.selection.entries.len() < self.limit && next_bytes <= self.max_bytes {
            self.selection.used_bytes = next_bytes;
            self.selection.entries.push((position, value));
        } else {
            self.selection.boundary = Some((position, value));
        }
        Ok(())
    }

    fn visit_branch<R: CollectionResolver + ?Sized>(
        &mut self,
        base_rank: u64,
        depth: u16,
        prefix: String,
        left: MapChild,
        right: MapChild,
        resolver: &mut R,
    ) -> Result<()> {
        verify_constraints(&prefix, &self.constraints)?;
        if self
            .constraints
            .last()
            .is_some_and(|constraint| constraint.depth >= depth)
        {
            return Err(CollectionError::Integrity {
                code: "map_branch_depth_mismatch",
                message: "map range branch depth does not increase".to_owned(),
            });
        }
        self.constraints.push(PathConstraint {
            depth,
            prefix: prefix.clone(),
            right: false,
        });
        let left_entries = left.entries;
        let left_result = self.visit(&left.into(), base_rank, resolver);
        self.constraints.pop();
        left_result?;
        let right_rank =
            base_rank
                .checked_add(left_entries)
                .ok_or_else(|| CollectionError::Integrity {
                    code: "map_range_rank_overflow",
                    message: "map range right rank overflowed".to_owned(),
                })?;
        self.constraints.push(PathConstraint {
            depth,
            prefix,
            right: true,
        });
        let right_result = self.visit(&right.into(), right_rank, resolver);
        self.constraints.pop();
        right_result
    }
}

fn collect_map_range<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    start: u64,
    limit: usize,
    max_bytes: usize,
    resolver: &mut R,
) -> Result<MapRangeSelection> {
    if start > root.entries {
        return Err(CollectionError::Integrity {
            code: "map_range_start_mismatch",
            message: "map range starts beyond its source root".to_owned(),
        });
    }
    let mut traversal = MapRangeTraversal::new(start, limit, max_bytes);
    let Some(object_id) = &root.node else {
        return Ok(traversal.selection);
    };
    let reference = NodeRef {
        object_id: object_id.clone(),
        entries: root.entries,
    };
    traversal.visit(&reference, 0, resolver)?;
    if traversal.selection.entries.is_empty() && traversal.selection.boundary.is_some() {
        return Err(CollectionError::Validation(
            "map page byte budget cannot admit its first exact entry".to_owned(),
        ));
    }
    Ok(traversal.selection)
}

fn validate_page_request(limit: usize, max_bytes: usize) -> Result<()> {
    if !(1..=MAX_PAGE_ENTRIES).contains(&limit) {
        return Err(CollectionError::Validation(format!(
            "map page limit must be within 1..={MAX_PAGE_ENTRIES}"
        )));
    }
    if !(1..=MAX_PAGE_BYTES).contains(&max_bytes) {
        return Err(CollectionError::Validation(format!(
            "map page byte budget must be within 1..={MAX_PAGE_BYTES}"
        )));
    }
    Ok(())
}

fn page_entry_bytes(position: &MapPosition, value: &str) -> Result<usize> {
    position
        .key
        .len()
        .checked_add(position.key_hash.len())
        .and_then(|bytes| bytes.checked_add(value.len()))
        .ok_or_else(|| CollectionError::Validation("map page bytes overflowed".to_owned()))
}

/// Raw proof of one exact map mutation batch and result root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MapApplyProof {
    parent: MapRoot,
    result: MapRoot,
    mutations: Vec<MapMutation>,
    nodes: Vec<MapNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapApplyProofWire {
    parent: MapRoot,
    result: MapRoot,
    #[serde(deserialize_with = "deserialize_map_mutations")]
    mutations: Vec<MapMutation>,
    #[serde(deserialize_with = "deserialize_map_apply_nodes")]
    nodes: Vec<MapNodeWire>,
}

/// Decode one map apply proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_map_apply`].
pub fn decode_map_apply_proof(bytes: &[u8]) -> Result<MapApplyProof> {
    let wire = crate::decode_json_bounded::<MapApplyProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-map apply proof",
    )?;
    Ok(MapApplyProof {
        parent: wire.parent,
        result: wire.result,
        mutations: wire.mutations,
        nodes: wire
            .nodes
            .into_iter()
            .map(MapNodeWire::into_verified)
            .collect::<Result<Vec<_>>>()?,
    })
}

/// Non-serializable verified map apply result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedMapApply {
    parent: MapRoot,
    result: MapRoot,
    mutations: Vec<MapMutation>,
    mutation_digest: String,
}

impl VerifiedMapApply {
    /// Exact authenticated parent root.
    pub fn parent(&self) -> &MapRoot {
        &self.parent
    }

    /// Exact recomputed result root.
    pub fn result(&self) -> &MapRoot {
        &self.result
    }

    /// Canonically ordered exact mutations.
    pub fn mutations(&self) -> &[MapMutation] {
        &self.mutations
    }

    /// Domain-separated identity of the exact mutation batch.
    pub fn mutation_digest(&self) -> &str {
        &self.mutation_digest
    }
}

/// Complete provider output for one verified map apply.
#[derive(Debug, Clone)]
pub struct MapApplyOutput {
    verified: VerifiedMapApply,
    proof: MapApplyProof,
    objects: Vec<MapNode>,
}

impl MapApplyOutput {
    /// Verified parent/result/mutation binding.
    pub fn verified(&self) -> &VerifiedMapApply {
        &self.verified
    }

    /// Portable independently verifiable apply proof.
    pub fn proof(&self) -> &MapApplyProof {
        &self.proof
    }

    /// Newly created immutable nodes that a provider must persist.
    pub fn objects(&self) -> &[MapNode] {
        &self.objects
    }

    /// Consume the output into its portable proof and newly created nodes.
    pub fn into_parts(self) -> (MapApplyProof, Vec<MapNode>) {
        (self.proof, self.objects)
    }
}

/// Apply one canonical exact mutation batch from an authenticated parent.
///
/// # Errors
///
/// Returns an error when an exact prior-state expectation fails, a node is
/// unavailable or invalid, or the result exceeds a collection bound.
pub fn apply_map_mutations<R: CollectionResolver + ?Sized>(
    parent: &MapRoot,
    mutations: &[MapMutation],
    resolver: &mut R,
) -> Result<MapApplyOutput> {
    parent.verify()?;
    let mutations = canonical_mutations(mutations)?;
    let mut overlay = Overlay::new(resolver);
    let result = apply_map_internal(parent, &mutations, &mut overlay)?;
    let proof_nodes = overlay.loaded.clone().into_values().collect();
    let objects = collect_reachable_created_map_nodes(&result, &mut overlay)?;
    let proof = MapApplyProof {
        parent: parent.clone(),
        result: result.clone(),
        mutations: mutations.clone(),
        nodes: proof_nodes,
    };
    let verified = verify_map_apply(parent, &mutations, &proof)?;
    Ok(MapApplyOutput {
        verified,
        proof,
        objects,
    })
}

/// Verify an apply proof by replaying the exact mutation algorithm.
///
/// # Errors
///
/// Returns an error for a stale parent, changed mutation, missing/extra node,
/// wrong count, or arbitrary same-count result root.
pub fn verify_map_apply(
    expected_parent: &MapRoot,
    expected_mutations: &[MapMutation],
    proof: &MapApplyProof,
) -> Result<VerifiedMapApply> {
    expected_parent.verify()?;
    let mutations = canonical_mutations(expected_mutations)?;
    if &proof.parent != expected_parent || proof.mutations != mutations {
        return Err(CollectionError::Integrity {
            code: "map_apply_authority_mismatch",
            message: "map apply proof is bound to another parent or mutation batch".to_owned(),
        });
    }
    let mut resolver = ApplyProofMapResolver::new(&proof.nodes)?;
    let mut overlay = Overlay::new(&mut resolver);
    let result = apply_map_internal(expected_parent, &mutations, &mut overlay)?;
    if result != proof.result {
        return Err(CollectionError::Integrity {
            code: "map_apply_result_mismatch",
            message: "map apply proof result does not match exact replay".to_owned(),
        });
    }
    let loaded: BTreeSet<String> = overlay.loaded.keys().cloned().collect();
    resolver.finish(&loaded)?;
    Ok(VerifiedMapApply {
        parent: expected_parent.clone(),
        result,
        mutation_digest: mutation_digest(&mutations),
        mutations,
    })
}

fn collect_reachable_created_map_nodes<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    overlay: &mut Overlay<'_, R>,
) -> Result<Vec<MapNode>> {
    let Some(object_id) = &root.node else {
        return Ok(Vec::new());
    };
    let mut stack = vec![NodeRef {
        object_id: object_id.clone(),
        entries: root.entries,
    }];
    let mut visited = BTreeSet::new();
    let mut created = BTreeMap::new();
    while let Some(reference) = stack.pop() {
        if !visited.insert(reference.object_id.clone()) {
            continue;
        }
        let Some(node) = overlay.pending.get(&reference.object_id).cloned() else {
            continue;
        };
        node.verify()?;
        if node.entries()? != reference.entries {
            return Err(CollectionError::Integrity {
                code: "map_created_child_shape_mismatch",
                message: format!(
                    "created map node {} contradicts the final reachable shape",
                    reference.object_id
                ),
            });
        }
        if overlay.loaded.contains_key(&node.object_id) {
            continue;
        }
        match overlay.resolver.load_map_node(&node.object_id)? {
            Some(existing) => {
                existing.verify()?;
                if existing != node {
                    return Err(CollectionError::Integrity {
                        code: "map_existing_node_identity_conflict",
                        message: format!(
                            "existing map node {} has conflicting bytes",
                            node.object_id
                        ),
                    });
                }
                continue;
            }
            None => {
                created.insert(node.object_id.clone(), node.clone());
            }
        }
        if let MapNodeBody::Branch { left, right, .. } = node.body {
            stack.push(right.into());
            stack.push(left.into());
        }
    }
    Ok(created.into_values().collect())
}

/// Complete O(total entries) verification of one map root.
#[derive(Debug, Clone)]
pub struct MapAudit {
    entries: Vec<(MapPosition, String)>,
    objects: Vec<MapNode>,
}

impl MapAudit {
    /// Every exact entry in authenticated `(key_hash, key)` order.
    pub fn entries(&self) -> &[(MapPosition, String)] {
        &self.entries
    }

    /// Complete reachable immutable node set.
    pub fn objects(&self) -> &[MapNode] {
        &self.objects
    }
}

/// Verify and materialize one complete map closure.
///
/// This explicit O(total entries) operation is for genesis, restore audit, and
/// offline repair only. Exact reads and pages never call it.
///
/// # Errors
///
/// Returns an error for a missing node, invalid path/prefix/count, duplicate
/// key, or root-closure mismatch.
pub fn audit_map<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    resolver: &mut R,
) -> Result<MapAudit> {
    root.verify()?;
    let Some(object_id) = &root.node else {
        return Ok(MapAudit {
            entries: Vec::new(),
            objects: Vec::new(),
        });
    };
    let mut stack = vec![(
        NodeRef {
            object_id: object_id.clone(),
            entries: root.entries,
        },
        Vec::<PathConstraint>::new(),
    )];
    let mut visited = BTreeSet::new();
    let mut keys = BTreeSet::new();
    let mut entries = Vec::new();
    let mut objects = Vec::new();
    while let Some((reference, constraints)) = stack.pop() {
        if !visited.insert(reference.object_id.clone()) {
            return Err(CollectionError::Integrity {
                code: "map_audit_repeated_node",
                message: "map closure repeats one reachable node".to_owned(),
            });
        }
        let node = load_resolved_map_node(resolver, &reference)?;
        match &node.body {
            MapNodeBody::Leaf {
                key,
                key_hash,
                value,
            } => {
                verify_constraints(key_hash, &constraints)?;
                if !keys.insert(key.clone()) {
                    return Err(CollectionError::Integrity {
                        code: "map_audit_duplicate_key",
                        message: "map closure repeats one exact key".to_owned(),
                    });
                }
                entries.push((
                    MapPosition {
                        key: key.clone(),
                        key_hash: key_hash.clone(),
                    },
                    value.clone(),
                ));
            }
            MapNodeBody::Branch {
                depth,
                prefix,
                left,
                right,
            } => {
                verify_constraints(prefix, &constraints)?;
                if constraints
                    .last()
                    .is_some_and(|constraint| constraint.depth >= *depth)
                {
                    return Err(CollectionError::Integrity {
                        code: "map_branch_depth_mismatch",
                        message: "map audit branch depth does not increase".to_owned(),
                    });
                }
                let mut left_constraints = constraints.clone();
                left_constraints.push(PathConstraint {
                    depth: *depth,
                    prefix: prefix.clone(),
                    right: false,
                });
                let mut right_constraints = constraints;
                right_constraints.push(PathConstraint {
                    depth: *depth,
                    prefix: prefix.clone(),
                    right: true,
                });
                stack.push((right.clone().into(), right_constraints));
                stack.push((left.clone().into(), left_constraints));
            }
        }
        objects.push(node);
    }
    if u64::try_from(entries.len()).ok() != Some(root.entries)
        || entries.windows(2).any(|pair| {
            let left = &pair[0].0;
            let right = &pair[1].0;
            (left.key_hash.as_str(), left.key.as_str())
                >= (right.key_hash.as_str(), right.key.as_str())
        })
    {
        return Err(CollectionError::Integrity {
            code: "map_audit_order_or_count_mismatch",
            message: "map closure does not match its exact root order or count".to_owned(),
        });
    }
    objects.sort_by(|left, right| left.object_id.cmp(&right.object_id));
    Ok(MapAudit { entries, objects })
}

/// Complete output of an explicit full map build.
#[derive(Debug, Clone)]
pub struct MapBuildOutput {
    root: MapRoot,
    objects: Vec<MapNode>,
}

impl MapBuildOutput {
    /// Exact rebuilt root.
    pub fn root(&self) -> &MapRoot {
        &self.root
    }

    /// Complete reachable immutable node set for the rebuilt root.
    pub fn objects(&self) -> &[MapNode] {
        &self.objects
    }

    /// Consume the build into its root and complete reachable nodes.
    pub fn into_parts(self) -> (MapRoot, Vec<MapNode>) {
        (self.root, self.objects)
    }
}

/// Build a map from an explicit complete genesis set.
///
/// This is an O(total entries) audit/genesis operation, not an ordinary open.
///
/// # Errors
///
/// Returns an error for duplicate keys, invalid identities, or invalid nodes.
pub fn build_map(entries: Vec<(String, String)>) -> Result<MapBuildOutput> {
    let entries_len = u64::try_from(entries.len())
        .map_err(|error| CollectionError::Validation(error.to_string()))?;
    if entries_len > MAX_EXACT_INTEGER {
        return Err(CollectionError::Validation(
            "full map build exceeds the exact entry range".to_owned(),
        ));
    }
    if entries.is_empty() {
        return Ok(MapBuildOutput {
            root: MapRoot::empty(),
            objects: Vec::new(),
        });
    }
    let mut keys = BTreeSet::new();
    let mut ordered = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        validate_key(&key)?;
        validate_content_id("full map value", &value)?;
        if !keys.insert(key.clone()) {
            return Err(CollectionError::Validation(format!(
                "full map build repeats key {key:?}"
            )));
        }
        ordered.push(MapBuildEntry {
            key_hash: map_key_hash(&key)?,
            key,
            value,
        });
    }
    ordered.sort_by(|left, right| (&left.key_hash, &left.key).cmp(&(&right.key_hash, &right.key)));
    let mut objects = BTreeMap::new();
    let reference = build_map_subtree(&ordered, &mut objects)?;
    let root = MapRoot {
        node: Some(reference.object_id),
        entries: reference.entries,
    };
    root.verify()?;
    if root.entries != entries_len {
        return Err(CollectionError::Integrity {
            code: "map_build_count_mismatch",
            message: "bottom-up map build produced the wrong entry count".to_owned(),
        });
    }
    Ok(MapBuildOutput {
        root,
        objects: objects.into_values().collect(),
    })
}

#[derive(Debug)]
struct MapBuildEntry {
    key_hash: String,
    key: String,
    value: String,
}

fn build_map_subtree(
    entries: &[MapBuildEntry],
    objects: &mut BTreeMap<String, MapNode>,
) -> Result<NodeRef> {
    let first = entries.first().ok_or_else(|| CollectionError::Integrity {
        code: "map_build_empty_subtree",
        message: "bottom-up map build received an empty subtree".to_owned(),
    })?;
    if entries.len() == 1 {
        return store_built_map_node(
            MapNode::leaf(
                first.key.clone(),
                first.key_hash.clone(),
                first.value.clone(),
            )?,
            objects,
        );
    }
    let last = entries.last().expect("nonempty map build subtree");
    if first.key_hash == last.key_hash {
        return Err(CollectionError::Integrity {
            code: "map_key_hash_collision",
            message: "distinct map keys have the same SHA-256 hash".to_owned(),
        });
    }
    let depth = common_prefix_bits(&first.key_hash, &last.key_hash)?;
    let mut split = 0;
    while split < entries.len() && !hash_bit(&entries[split].key_hash, depth)? {
        split += 1;
    }
    if split == entries.len() {
        return Err(CollectionError::Integrity {
            code: "map_build_partition_mismatch",
            message: "bottom-up map build could not partition its hash range".to_owned(),
        });
    }
    if split == 0 {
        return Err(CollectionError::Integrity {
            code: "map_build_partition_mismatch",
            message: "bottom-up map build produced an empty left partition".to_owned(),
        });
    }
    let left = build_map_subtree(&entries[..split], objects)?;
    let right = build_map_subtree(&entries[split..], objects)?;
    let prefix = normalized_hash_prefix(&first.key_hash, depth)?;
    store_built_map_node(
        MapNode::branch(depth, prefix, left.into_child(), right.into_child())?,
        objects,
    )
}

fn store_built_map_node(node: MapNode, objects: &mut BTreeMap<String, MapNode>) -> Result<NodeRef> {
    node.verify()?;
    let object_id = node.object_id.clone();
    let reference = NodeRef {
        object_id: object_id.clone(),
        entries: node.entries()?,
    };
    match objects.entry(object_id.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(node);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &node => {
            return Err(CollectionError::Integrity {
                code: "map_node_identity_conflict",
                message: format!("map node {object_id} has conflicting bytes"),
            });
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    Ok(reference)
}

fn apply_map_internal<R: CollectionResolver + ?Sized>(
    parent: &MapRoot,
    mutations: &[MapMutation],
    overlay: &mut Overlay<'_, R>,
) -> Result<MapRoot> {
    let mut root = parent.clone();
    for mutation in mutations {
        match mutation {
            MapMutation::Put {
                key,
                expected,
                value,
            } => {
                let key_hash = map_key_hash(key)?;
                let current = overlay_lookup(&root, key, &key_hash, overlay)?.value;
                match (expected, current.as_deref()) {
                    (ExpectedMapValue::Absent, None) => {}
                    (ExpectedMapValue::Exact(expected), Some(current)) if expected == current => {}
                    _ => {
                        return Err(CollectionError::Conflict(format!(
                            "map key {key:?} does not match its expected parent value"
                        )));
                    }
                }
                root = overlay_put(&root, key, &key_hash, value, overlay)?;
            }
            MapMutation::Remove { key, expected } => {
                let key_hash = map_key_hash(key)?;
                let current = overlay_lookup(&root, key, &key_hash, overlay)?.value;
                if current.as_deref() != Some(expected) {
                    return Err(CollectionError::Conflict(format!(
                        "map key {key:?} does not match its exact removal value"
                    )));
                }
                root = overlay_remove(&root, key, &key_hash, overlay)?;
            }
        }
    }
    root.verify()?;
    Ok(root)
}

fn overlay_lookup<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    key_hash: &str,
    overlay: &mut Overlay<'_, R>,
) -> Result<LookupOutcome> {
    let Some(object_id) = &root.node else {
        return Ok(LookupOutcome {
            value: None,
            rank: None,
        });
    };
    let mut reference = NodeRef {
        object_id: object_id.clone(),
        entries: root.entries,
    };
    let mut constraints = Vec::new();
    let mut rank = 0_u64;
    for _ in 0..MAX_MAP_PATH_NODES {
        let node = overlay.load(&reference)?;
        match node.body {
            MapNodeBody::Leaf {
                key: stored_key,
                key_hash: stored_hash,
                value,
            } => {
                verify_constraints(&stored_hash, &constraints)?;
                return if stored_key == key && stored_hash == key_hash {
                    Ok(LookupOutcome {
                        value: Some(value),
                        rank: Some(rank),
                    })
                } else if stored_hash == key_hash {
                    Err(CollectionError::Integrity {
                        code: "map_key_hash_collision",
                        message: "distinct map keys have the same SHA-256 hash".to_owned(),
                    })
                } else {
                    Ok(LookupOutcome {
                        value: None,
                        rank: None,
                    })
                };
            }
            MapNodeBody::Branch {
                depth,
                prefix,
                left,
                right,
            } => {
                verify_constraints(&prefix, &constraints)?;
                if constraints
                    .last()
                    .is_some_and(|constraint: &PathConstraint| constraint.depth >= depth)
                {
                    return Err(CollectionError::Integrity {
                        code: "map_branch_depth_mismatch",
                        message: "map branch depth does not increase along the path".to_owned(),
                    });
                }
                if !hash_matches_prefix(key_hash, &prefix, depth)? {
                    return Ok(LookupOutcome {
                        value: None,
                        rank: None,
                    });
                }
                let goes_right = hash_bit(key_hash, depth)?;
                constraints.push(PathConstraint {
                    depth,
                    prefix,
                    right: goes_right,
                });
                if goes_right {
                    rank = rank.checked_add(left.entries).ok_or_else(|| {
                        CollectionError::Integrity {
                            code: "map_rank_overflow",
                            message: "map rank overflowed".to_owned(),
                        }
                    })?;
                    reference = right.into();
                } else {
                    reference = left.into();
                }
            }
        }
    }
    Err(CollectionError::Integrity {
        code: "map_path_bound_exceeded",
        message: "map lookup exceeds the SHA-256 depth bound".to_owned(),
    })
}

fn overlay_put<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    key_hash: &str,
    value: &str,
    overlay: &mut Overlay<'_, R>,
) -> Result<MapRoot> {
    let (next, inserted) = match &root.node {
        None => (new_leaf(key, key_hash, value, overlay)?, true),
        Some(object_id) => insert_node(
            &NodeRef {
                object_id: object_id.clone(),
                entries: root.entries,
            },
            key,
            key_hash,
            value,
            overlay,
            &[],
            MAX_MAP_PATH_NODES,
        )?,
    };
    let entries = if inserted {
        root.entries
            .checked_add(1)
            .ok_or_else(|| CollectionError::Validation("map entry count overflowed".to_owned()))?
    } else {
        root.entries
    };
    if entries > MAX_EXACT_INTEGER || next.entries != entries {
        return Err(CollectionError::Integrity {
            code: "map_result_count_mismatch",
            message: "map put produced the wrong entry count".to_owned(),
        });
    }
    Ok(MapRoot {
        node: Some(next.object_id),
        entries,
    })
}

fn insert_node<R: CollectionResolver + ?Sized>(
    reference: &NodeRef,
    key: &str,
    key_hash: &str,
    value: &str,
    overlay: &mut Overlay<'_, R>,
    constraints: &[PathConstraint],
    remaining: usize,
) -> Result<(NodeRef, bool)> {
    if remaining == 0 {
        return Err(CollectionError::Integrity {
            code: "map_path_bound_exceeded",
            message: "map update path exceeds SHA-256".to_owned(),
        });
    }
    let node = overlay.load(reference)?;
    match node.body {
        MapNodeBody::Leaf {
            key: stored_key,
            key_hash: stored_hash,
            ..
        } => {
            verify_constraints(&stored_hash, constraints)?;
            if stored_key == key {
                if stored_hash != key_hash {
                    return Err(CollectionError::Integrity {
                        code: "map_key_hash_mismatch",
                        message: "map key changed its derived hash".to_owned(),
                    });
                }
                Ok((new_leaf(key, key_hash, value, overlay)?, false))
            } else {
                if stored_hash == key_hash {
                    return Err(CollectionError::Integrity {
                        code: "map_key_hash_collision",
                        message: "distinct map keys have the same SHA-256 hash".to_owned(),
                    });
                }
                let depth = common_prefix_bits(&stored_hash, key_hash)?;
                let inserted = new_leaf(key, key_hash, value, overlay)?;
                Ok((
                    new_branch(depth, key_hash, inserted, reference.clone(), overlay)?,
                    true,
                ))
            }
        }
        MapNodeBody::Branch {
            depth,
            prefix,
            left,
            right,
        } => {
            verify_constraints(&prefix, constraints)?;
            if constraints
                .last()
                .is_some_and(|constraint| constraint.depth >= depth)
            {
                return Err(CollectionError::Integrity {
                    code: "map_branch_depth_mismatch",
                    message: "map branch depth does not increase".to_owned(),
                });
            }
            let common = common_prefix_bits(&prefix, key_hash)?;
            if common < depth {
                let inserted = new_leaf(key, key_hash, value, overlay)?;
                return Ok((
                    new_branch(common, key_hash, inserted, reference.clone(), overlay)?,
                    true,
                ));
            }
            let goes_right = hash_bit(key_hash, depth)?;
            let selected: NodeRef = if goes_right {
                right.clone().into()
            } else {
                left.clone().into()
            };
            let mut next_constraints = constraints.to_vec();
            next_constraints.push(PathConstraint {
                depth,
                prefix: prefix.clone(),
                right: goes_right,
            });
            let (next_child, inserted) = insert_node(
                &selected,
                key,
                key_hash,
                value,
                overlay,
                &next_constraints,
                remaining - 1,
            )?;
            let (next_left, next_right) = if goes_right {
                (left.into(), next_child)
            } else {
                (next_child, right.into())
            };
            Ok((
                store_branch(depth, prefix, next_left, next_right, overlay)?,
                inserted,
            ))
        }
    }
}

fn overlay_remove<R: CollectionResolver + ?Sized>(
    root: &MapRoot,
    key: &str,
    key_hash: &str,
    overlay: &mut Overlay<'_, R>,
) -> Result<MapRoot> {
    let object_id = root
        .node
        .as_ref()
        .ok_or_else(|| CollectionError::Conflict(format!("map key {key:?} does not exist")))?;
    let next = remove_node(
        &NodeRef {
            object_id: object_id.clone(),
            entries: root.entries,
        },
        key,
        key_hash,
        overlay,
        &[],
        MAX_MAP_PATH_NODES,
    )?;
    let entries = root
        .entries
        .checked_sub(1)
        .ok_or_else(|| CollectionError::Integrity {
            code: "map_remove_count_underflow",
            message: "map removal underflowed its count".to_owned(),
        })?;
    if next.as_ref().map_or(0, |node| node.entries) != entries {
        return Err(CollectionError::Integrity {
            code: "map_result_count_mismatch",
            message: "map removal produced the wrong entry count".to_owned(),
        });
    }
    Ok(MapRoot {
        node: next.map(|node| node.object_id),
        entries,
    })
}

fn remove_node<R: CollectionResolver + ?Sized>(
    reference: &NodeRef,
    key: &str,
    key_hash: &str,
    overlay: &mut Overlay<'_, R>,
    constraints: &[PathConstraint],
    remaining: usize,
) -> Result<Option<NodeRef>> {
    if remaining == 0 {
        return Err(CollectionError::Integrity {
            code: "map_path_bound_exceeded",
            message: "map removal path exceeds SHA-256".to_owned(),
        });
    }
    let node = overlay.load(reference)?;
    match node.body {
        MapNodeBody::Leaf {
            key: stored_key,
            key_hash: stored_hash,
            ..
        } => {
            verify_constraints(&stored_hash, constraints)?;
            if stored_key == key && stored_hash == key_hash {
                Ok(None)
            } else {
                Err(CollectionError::Conflict(format!(
                    "map key {key:?} does not exist"
                )))
            }
        }
        MapNodeBody::Branch {
            depth,
            prefix,
            left,
            right,
        } => {
            verify_constraints(&prefix, constraints)?;
            if constraints
                .last()
                .is_some_and(|constraint| constraint.depth >= depth)
            {
                return Err(CollectionError::Integrity {
                    code: "map_branch_depth_mismatch",
                    message: "map branch depth does not increase".to_owned(),
                });
            }
            if !hash_matches_prefix(key_hash, &prefix, depth)? {
                return Err(CollectionError::Conflict(format!(
                    "map key {key:?} does not exist"
                )));
            }
            let goes_right = hash_bit(key_hash, depth)?;
            let selected: NodeRef = if goes_right {
                right.clone().into()
            } else {
                left.clone().into()
            };
            let mut next_constraints = constraints.to_vec();
            next_constraints.push(PathConstraint {
                depth,
                prefix: prefix.clone(),
                right: goes_right,
            });
            let next = remove_node(
                &selected,
                key,
                key_hash,
                overlay,
                &next_constraints,
                remaining - 1,
            )?;
            match (goes_right, next) {
                (true, None) => Ok(Some(left.into())),
                (false, None) => Ok(Some(right.into())),
                (true, Some(next_right)) => Ok(Some(store_branch(
                    depth,
                    prefix,
                    left.into(),
                    next_right,
                    overlay,
                )?)),
                (false, Some(next_left)) => Ok(Some(store_branch(
                    depth,
                    prefix,
                    next_left,
                    right.into(),
                    overlay,
                )?)),
            }
        }
    }
}

fn new_leaf<R: CollectionResolver + ?Sized>(
    key: &str,
    key_hash: &str,
    value: &str,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    overlay.store(MapNode::leaf(
        key.to_owned(),
        key_hash.to_owned(),
        value.to_owned(),
    )?)
}

fn new_branch<R: CollectionResolver + ?Sized>(
    depth: u16,
    key_hash: &str,
    inserted: NodeRef,
    existing: NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    let inserted_right = hash_bit(key_hash, depth)?;
    let (left, right) = if inserted_right {
        (existing, inserted)
    } else {
        (inserted, existing)
    };
    store_branch(
        depth,
        normalized_hash_prefix(key_hash, depth)?,
        left,
        right,
        overlay,
    )
}

fn store_branch<R: CollectionResolver + ?Sized>(
    depth: u16,
    prefix: String,
    left: NodeRef,
    right: NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    overlay.store(MapNode::branch(
        depth,
        prefix,
        left.into_child(),
        right.into_child(),
    )?)
}

struct ApplyProofMapResolver {
    nodes: BTreeMap<String, MapNode>,
    requested: BTreeSet<String>,
}

impl ApplyProofMapResolver {
    fn new(nodes: &[MapNode]) -> Result<Self> {
        let maximum = MAX_MAP_PATH_NODES
            .checked_mul(MAX_MAP_MUTATIONS_PER_APPLY)
            .and_then(|nodes| nodes.checked_mul(2))
            .ok_or_else(|| CollectionError::Validation("map proof bound overflowed".to_owned()))?;
        if nodes.len() > maximum {
            return Err(CollectionError::Validation(
                "map apply proof exceeds its node bound".to_owned(),
            ));
        }
        verify_map_proof_bytes(nodes)?;
        let mut indexed = BTreeMap::new();
        for node in nodes {
            node.verify()?;
            if indexed
                .insert(node.object_id.clone(), node.clone())
                .is_some()
            {
                return Err(CollectionError::Integrity {
                    code: "map_apply_duplicate_node",
                    message: format!("map apply proof repeats node {}", node.object_id),
                });
            }
        }
        Ok(Self {
            nodes: indexed,
            requested: BTreeSet::new(),
        })
    }

    fn finish(&self, loaded: &BTreeSet<String>) -> Result<()> {
        let supplied: BTreeSet<String> = self.nodes.keys().cloned().collect();
        if loaded != &supplied || !loaded.is_subset(&self.requested) {
            return Err(CollectionError::Integrity {
                code: "map_apply_node_closure_mismatch",
                message: "map apply proof has missing, unused, or substituted nodes".to_owned(),
            });
        }
        Ok(())
    }
}

fn verify_map_proof_bytes(nodes: &[MapNode]) -> Result<()> {
    let mut bytes = 0_usize;
    for node in nodes {
        bytes = bytes
            .checked_add(node.logical_bytes()?)
            .filter(|bytes| *bytes <= MAX_PROOF_BYTES)
            .ok_or_else(|| {
                CollectionError::Validation(format!(
                    "map proof exceeds {MAX_PROOF_BYTES} logical node bytes"
                ))
            })?;
    }
    Ok(())
}

impl CollectionResolver for ApplyProofMapResolver {
    fn load_map_node(&mut self, object_id: &str) -> Result<Option<MapNode>> {
        self.requested.insert(object_id.to_owned());
        Ok(self.nodes.get(object_id).cloned())
    }

    fn load_log_node(&mut self, _object_id: &str) -> Result<Option<crate::LogNode>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Memory {
        maps: BTreeMap<String, MapNode>,
        map_loads: usize,
    }

    impl CollectionResolver for Memory {
        fn load_map_node(&mut self, object_id: &str) -> Result<Option<MapNode>> {
            self.map_loads = self
                .map_loads
                .checked_add(1)
                .expect("test load counter remains bounded");
            Ok(self.maps.get(object_id).cloned())
        }

        fn load_log_node(&mut self, _object_id: &str) -> Result<Option<crate::LogNode>> {
            Ok(None)
        }
    }

    fn absorb_apply(memory: &mut Memory, output: &MapApplyOutput) {
        for node in output.objects() {
            memory.maps.insert(node.object_id.clone(), node.clone());
        }
    }

    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    }

    fn value(label: &str) -> String {
        hash_identifier("test-value/1", &[label.as_bytes()])
    }

    struct RangeFixture {
        root: MapRoot,
        memory: Memory,
        first: Vec<MapPosition>,
        middle_cursor: MapPosition,
        middle_successors: Vec<MapPosition>,
    }

    fn range_fixture(count: usize) -> RangeFixture {
        let built = build_map(
            (0..count)
                .map(|index| {
                    (
                        format!("range-key-{index:05}"),
                        value(&format!("range-value-{}", index % 127)),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .expect("range fixture builds");
        let root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.maps.insert(node.object_id.clone(), node.clone());
        }
        let audit = audit_map(&root, &mut memory).expect("range fixture audits");
        let middle = count / 2;
        RangeFixture {
            root,
            memory,
            first: audit
                .entries()
                .iter()
                .take(128)
                .map(|(position, _)| position.clone())
                .collect(),
            middle_cursor: audit.entries()[middle].0.clone(),
            middle_successors: audit
                .entries()
                .iter()
                .skip(middle + 1)
                .take(128)
                .map(|(position, _)| position.clone())
                .collect(),
        }
    }

    fn range_loads(
        root: &MapRoot,
        memory: &mut Memory,
        after: Option<&MapPosition>,
        expected: &[MapPosition],
        limit: usize,
    ) -> usize {
        memory.map_loads = 0;
        let proof = prove_map_range(root, after, limit, MAX_PAGE_BYTES, memory)
            .expect("range proof generates");
        let loads = memory.map_loads;
        assert_eq!(loads, proof.nodes.len());
        let page = verify_map_range(root, after, limit, MAX_PAGE_BYTES, &proof)
            .expect("range proof verifies");
        assert_eq!(page.entries().len(), limit);
        assert_eq!(
            page.entries()
                .iter()
                .map(|(position, _)| position)
                .collect::<Vec<_>>(),
            expected.iter().take(limit).collect::<Vec<_>>()
        );
        loads
    }

    fn fixture() -> (Memory, MapRoot) {
        let output = build_map(vec![
            ("alpha".to_owned(), value("a")),
            ("beta".to_owned(), value("b")),
            ("gamma".to_owned(), value("c")),
            ("delta".to_owned(), value("d")),
        ])
        .expect("map builds");
        let root = output.root().clone();
        let mut memory = Memory::default();
        for node in output.objects() {
            memory.maps.insert(node.object_id.clone(), node.clone());
        }
        (memory, root)
    }

    #[test]
    fn exact_membership_and_absence_reject_wrong_authority() {
        let (mut memory, root) = fixture();
        let proof = prove_map_exact(&root, "beta", &mut memory).expect("proof");
        let verified = verify_map_exact(&root, "beta", &proof).expect("verified");
        assert_eq!(verified.value(), Some(value("b").as_str()));

        let missing = prove_map_exact(&root, "missing", &mut memory).expect("absence proof");
        assert_eq!(
            verify_map_exact(&root, "missing", &missing)
                .expect("verified absence")
                .value(),
            None
        );
        assert!(verify_map_exact(&root, "other", &missing).is_err());

        let other = build_map(vec![("beta".to_owned(), value("other"))]).expect("other map");
        assert!(verify_map_exact(other.root(), "beta", &proof).is_err());

        let position = MapPosition::for_key("beta").expect("position derives");
        assert_eq!(position.key(), "beta");
        assert_eq!(position.key_hash(), map_key_hash("beta").expect("hash"));
        let mut encoded = serde_json::to_value(&position).expect("position encodes");
        encoded["key_hash"] = serde_json::Value::String(map_key_hash("other").expect("hash"));
        assert!(
            decode_map_position(&serde_json::to_vec(&encoded).expect("forged position encodes"))
                .is_err()
        );
    }

    #[test]
    fn range_proof_rejects_skip_reorder_substitution_and_false_terminal() {
        let (mut memory, root) = fixture();
        let proof = prove_map_range(&root, None, 2, MAX_PAGE_BYTES, &mut memory).expect("page");
        let page = verify_map_range(&root, None, 2, MAX_PAGE_BYTES, &proof).expect("verified page");
        assert_eq!(page.entries().len(), 2);
        assert!(page.has_more());

        let mut skipped = proof.clone();
        skipped.nodes.pop();
        assert!(verify_map_range(&root, None, 2, MAX_PAGE_BYTES, &skipped).is_err());

        let mut reordered = serde_json::to_value(&proof).expect("proof encodes");
        reordered["entries"] = serde_json::json!(["second", "first"]);
        assert!(
            decode_map_range_proof(
                &serde_json::to_vec(&reordered).expect("reordered wire encodes")
            )
            .is_err()
        );

        let mut substituted = proof.clone();
        let leaf = substituted
            .nodes
            .iter_mut()
            .find(|node| matches!(node.body, MapNodeBody::Leaf { .. }))
            .expect("range proof contains a leaf");
        if let MapNodeBody::Leaf {
            value: stored_value,
            ..
        } = &mut leaf.body
        {
            *stored_value = value("substitute");
        }
        assert!(verify_map_range(&root, None, 2, MAX_PAGE_BYTES, &substituted).is_err());

        let mut false_terminal = serde_json::to_value(&proof).expect("proof encodes");
        false_terminal["boundary"] = serde_json::Value::Null;
        assert!(
            decode_map_range_proof(
                &serde_json::to_vec(&false_terminal).expect("false terminal wire encodes")
            )
            .is_err()
        );

        let proof = prove_map_range(&root, None, 2, MAX_PAGE_BYTES, &mut memory).expect("page");
        let zero_node = proof.nodes[0].clone();
        let mut oversized = proof;
        oversized.nodes = vec![zero_node; MAX_MAP_RANGE_PROOF_NODES + 1];
        let error = verify_map_range(&root, None, 2, MAX_PAGE_BYTES, &oversized)
            .expect_err("node bound is checked before proof traversal");
        assert!(matches!(
            error, CollectionError::Validation(message) if message.contains("node bound")
        ));
    }

    #[test]
    fn apply_proof_rejects_stale_parent_and_same_count_arbitrary_root() {
        let (mut memory, root) = fixture();
        let mutation = MapMutation::replace("beta", value("b"), value("next"));
        let output = apply_map_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
            .expect("apply");
        verify_map_apply(&root, std::slice::from_ref(&mutation), output.proof())
            .expect("verified apply");

        let stale = MapRoot::empty();
        assert!(verify_map_apply(&stale, std::slice::from_ref(&mutation), output.proof()).is_err());

        let mut arbitrary = output.proof().clone();
        arbitrary.result = build_map(vec![
            ("one".to_owned(), value("1")),
            ("two".to_owned(), value("2")),
            ("three".to_owned(), value("3")),
            ("four".to_owned(), value("4")),
        ])
        .expect("same count map")
        .root()
        .clone();
        assert!(verify_map_apply(&root, &[mutation], &arbitrary).is_err());
    }

    #[test]
    fn long_map_sequence_matches_btree_model_and_every_proof() {
        let mut memory = Memory::default();
        let mut root = MapRoot::empty();
        let mut model = BTreeMap::<String, String>::new();
        let mut random = 7_u64;
        for step in 0..600_u64 {
            let random_value = next_random(&mut random);
            let key = format!("key-{:03}", random_value % 97);
            let mutation = match model.get(&key).cloned() {
                Some(previous) if random_value.is_multiple_of(5) => {
                    model.remove(&key);
                    MapMutation::remove(key.clone(), previous)
                }
                Some(previous) => {
                    let next = value(&format!("replacement-{step}-{random_value}"));
                    model.insert(key.clone(), next.clone());
                    MapMutation::replace(key.clone(), previous, next)
                }
                None => {
                    let next = value(&format!("insert-{step}-{random_value}"));
                    model.insert(key.clone(), next.clone());
                    MapMutation::insert(key.clone(), next)
                }
            };
            let output = apply_map_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("model mutation applies");
            verify_map_apply(&root, std::slice::from_ref(&mutation), output.proof())
                .expect("every apply proof verifies");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();

            let exact = prove_map_exact(&root, &key, &mut memory).expect("exact proof");
            assert_eq!(
                verify_map_exact(&root, &key, &exact)
                    .expect("exact verifies")
                    .value(),
                model.get(&key).map(String::as_str)
            );
            if step % 41 == 0 {
                let mut after = None;
                let mut actual = Vec::new();
                loop {
                    let proof =
                        prove_map_range(&root, after.as_ref(), 7, MAX_PAGE_BYTES, &mut memory)
                            .expect("page proof");
                    let page = verify_map_range(&root, after.as_ref(), 7, MAX_PAGE_BYTES, &proof)
                        .expect("page verifies");
                    actual.extend(
                        page.entries()
                            .iter()
                            .map(|(position, value)| (position.key.clone(), value.clone())),
                    );
                    let Some(next) = page.next_position().cloned() else {
                        break;
                    };
                    after = Some(next);
                }
                let mut expected: Vec<(String, String, String)> = model
                    .iter()
                    .map(|(key, value)| {
                        (
                            map_key_hash(key).expect("key hashes"),
                            key.clone(),
                            value.clone(),
                        )
                    })
                    .collect();
                expected.sort();
                assert_eq!(
                    actual,
                    expected
                        .into_iter()
                        .map(|(_, key, value)| (key, value))
                        .collect::<Vec<_>>()
                );
            }
        }
        let rebuilt = build_map(model.into_iter().collect()).expect("full rebuild");
        assert_eq!(rebuilt.root(), &root);
    }

    #[test]
    fn map_proofs_reject_missing_extra_and_corrupt_path_nodes() {
        let (mut memory, root) = fixture();
        let proof = prove_map_exact(&root, "gamma", &mut memory).expect("proof");

        let mut missing = proof.clone();
        missing.nodes.pop();
        assert!(verify_map_exact(&root, "gamma", &missing).is_err());

        let mut extra = proof.clone();
        let unrelated = prove_map_exact(&root, "alpha", &mut memory).expect("unrelated proof");
        if let Some(node) = unrelated.nodes.into_iter().find(|node| {
            !extra
                .nodes
                .iter()
                .any(|existing| existing.object_id == node.object_id)
        }) {
            extra.nodes.push(node);
            assert!(verify_map_exact(&root, "gamma", &extra).is_err());
        }

        let mut corrupted = proof;
        let branch = corrupted
            .nodes
            .iter_mut()
            .find(|node| matches!(node.body, MapNodeBody::Branch { .. }))
            .expect("fixture has a branch");
        if let MapNodeBody::Branch { prefix, .. } = &mut branch.body {
            prefix.replace_range(..1, if &prefix[..1] == "0" { "1" } else { "0" });
        }
        assert!(verify_map_exact(&root, "gamma", &corrupted).is_err());

        let mut wrong_count = prove_map_exact(&root, "gamma", &mut memory).expect("proof");
        let root_id = wrong_count.root.node.clone().expect("nonempty root");
        let root_node = wrong_count
            .nodes
            .iter_mut()
            .find(|node| node.object_id == root_id)
            .expect("proof contains root");
        if let MapNodeBody::Branch { left, right, .. } = &mut root_node.body {
            left.entries = left.entries.checked_add(1).expect("test count increments");
            right.entries = right.entries.checked_add(1).expect("test count increments");
        } else {
            panic!("fixture root is a branch");
        }
        root_node.object_id = map_node_id(&root_node.body);
        wrong_count.root.node = Some(root_node.object_id.clone());
        wrong_count.root.entries = root_node.entries().expect("forged local count validates");
        assert!(verify_map_exact(&wrong_count.root, "gamma", &wrong_count).is_err());

        let huge_untrusted_root = MapRoot {
            node: Some(value("missing-audit-root")),
            entries: MAX_EXACT_INTEGER,
        };
        let mut empty_memory = Memory::default();
        assert!(matches!(
            audit_map(&huge_untrusted_root, &mut empty_memory),
            Err(CollectionError::MissingObject(_))
        ));
    }

    #[test]
    fn map_apply_enforces_batch_count_and_aggregate_bytes_before_resolution() {
        let mut memory = Memory::default();
        let maximum: Vec<MapMutation> = (0..MAX_MAP_MUTATIONS_PER_APPLY)
            .map(|index| MapMutation::insert(format!("max-{index}"), value(&index.to_string())))
            .collect();
        apply_map_mutations(&MapRoot::empty(), &maximum, &mut memory)
            .expect("exact mutation-count maximum applies");

        let too_many: Vec<MapMutation> = (0..=MAX_MAP_MUTATIONS_PER_APPLY)
            .map(|index| MapMutation::insert(format!("key-{index}"), value(&index.to_string())))
            .collect();
        assert!(apply_map_mutations(&MapRoot::empty(), &too_many, &mut memory).is_err());

        let value_bytes = value("bytes");
        let fifth_len = MAX_MUTATION_BYTES - (4 * MAX_MAP_KEY_BYTES) - (5 * value_bytes.len());
        let exact_bytes = vec![
            MapMutation::insert(
                format!("a{}", "a".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("b{}", "b".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("c{}", "c".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("d{}", "d".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert("e".repeat(fifth_len), value_bytes.clone()),
        ];
        let mut exact_memory = Memory::default();
        apply_map_mutations(&MapRoot::empty(), &exact_bytes, &mut exact_memory)
            .expect("exact mutation-byte maximum applies");
        let oversized = vec![
            MapMutation::insert(
                format!("a{}", "a".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("b{}", "b".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("c{}", "c".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("d{}", "d".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert("e".repeat(fifth_len + 1), value_bytes),
        ];
        assert!(apply_map_mutations(&MapRoot::empty(), &oversized, &mut memory).is_err());
        assert!(memory.maps.is_empty());
    }

    #[test]
    fn map_transport_roundtrips_maximum_escaped_keys_and_rejects_element_overflow() {
        let escaped_key = "\"".repeat(MAX_MAP_KEY_BYTES);
        let built = build_map(vec![(escaped_key.clone(), value("escaped"))])
            .expect("maximum escaped key builds");
        let node = built.objects().first().expect("one leaf node");
        let node_bytes = serde_json::to_vec(node).expect("node encodes");
        assert!(node_bytes.len() <= crate::MAX_NODE_TRANSPORT_BYTES);
        assert_eq!(decode_map_node(&node_bytes).expect("node decodes"), *node);

        let mut memory = Memory::default();
        for object in built.objects() {
            memory.maps.insert(object.object_id.clone(), object.clone());
        }
        let exact = prove_map_exact(built.root(), &escaped_key, &mut memory).expect("exact proof");
        let exact_bytes = serde_json::to_vec(&exact).expect("exact proof encodes");
        let decoded = decode_map_exact_proof(&exact_bytes).expect("exact proof decodes");
        verify_map_exact(built.root(), &escaped_key, &decoded).expect("decoded proof verifies");
        let range = prove_map_range(built.root(), None, 1, MAX_PAGE_BYTES, &mut memory)
            .expect("maximum legal key fits one maximum-byte page");
        let page = verify_map_range(built.root(), None, 1, MAX_PAGE_BYTES, &range)
            .expect("maximum legal key page verifies");
        assert_eq!(page.entries()[0].0.key(), escaped_key);
        assert!(build_map(vec![("x".repeat(MAX_MAP_KEY_BYTES + 1), value("too-long"))]).is_err());

        let (mut bomb_memory, bomb_root) = fixture();
        let bomb_proof =
            prove_map_exact(&bomb_root, "beta", &mut bomb_memory).expect("small proof");
        let exact_value = serde_json::to_value(&bomb_proof).expect("proof encodes to value");
        let exact_node = exact_value["nodes"][0].clone();
        let mut too_many_nodes = exact_value;
        too_many_nodes["nodes"] =
            serde_json::Value::Array(vec![exact_node; MAX_MAP_PATH_NODES + 1]);
        let too_many_bytes = serde_json::to_vec(&too_many_nodes).expect("oversized proof encodes");
        assert!(too_many_bytes.len() < crate::MAX_PROOF_TRANSPORT_BYTES);
        assert!(decode_map_exact_proof(&too_many_bytes).is_err());

        let (mut page_memory, page_root) = fixture();
        let page = prove_map_range(&page_root, None, 1, MAX_PAGE_BYTES, &mut page_memory)
            .expect("page proof");
        let mut page_value = serde_json::to_value(&page).expect("page encodes");
        let node = page_value["nodes"][0].clone();
        page_value["nodes"] = serde_json::Value::Array(vec![node; MAX_MAP_RANGE_PROOF_NODES + 1]);
        assert!(
            decode_map_range_proof(
                &serde_json::to_vec(&page_value).expect("oversized page encodes")
            )
            .is_err()
        );

        let value_bytes = value("mutation-bytes");
        let fifth_len = MAX_MUTATION_BYTES - (4 * MAX_MAP_KEY_BYTES) - (5 * value_bytes.len());
        let mutations = vec![
            MapMutation::insert(
                format!("a{}", "\"".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("b{}", "\"".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("c{}", "\"".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(
                format!("d{}", "\"".repeat(MAX_MAP_KEY_BYTES - 1)),
                value_bytes.clone(),
            ),
            MapMutation::insert(format!("e{}", "\"".repeat(fifth_len - 1)), value_bytes),
        ];
        let applied = apply_map_mutations(&MapRoot::empty(), &mutations, &mut Memory::default())
            .expect("maximum escaped mutation batch applies");
        let apply_bytes = serde_json::to_vec(applied.proof()).expect("apply proof encodes");
        assert!(apply_bytes.len() <= crate::MAX_PROOF_TRANSPORT_BYTES);
        let decoded_apply = decode_map_apply_proof(&apply_bytes).expect("apply proof decodes");
        verify_map_apply(&MapRoot::empty(), &mutations, &decoded_apply)
            .expect("decoded apply verifies");

        let mut mutation_value = serde_json::to_value(applied.proof()).expect("proof encodes");
        let mutation = mutation_value["mutations"][0].clone();
        mutation_value["mutations"] =
            serde_json::Value::Array(vec![mutation; MAX_MAP_MUTATIONS_PER_APPLY + 1]);
        assert!(
            decode_map_apply_proof(
                &serde_json::to_vec(&mutation_value).expect("oversized mutations encode")
            )
            .is_err()
        );

        let too_long = MapMutation::insert("x".repeat(MAX_PAGE_BYTES + 1), value("too-long"));
        let mut untouched = Memory::default();
        assert!(apply_map_mutations(&MapRoot::empty(), &[too_long], &mut untouched).is_err());
        assert!(untouched.maps.is_empty());
        assert!(map_key_hash("control\nkey").is_err());
    }

    #[test]
    fn maximum_map_apply_persists_only_final_reachable_new_nodes() {
        let entries: Vec<(String, String)> = (0..1024)
            .map(|index| (format!("key-{index:04}"), value(&format!("old-{index}"))))
            .collect();
        let built = build_map(entries.clone()).expect("parent map builds");
        let parent = built.root().clone();
        let parent_ids: BTreeSet<String> = built
            .objects()
            .iter()
            .map(|node| node.object_id.clone())
            .collect();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.maps.insert(node.object_id.clone(), node.clone());
        }
        let mutations: Vec<MapMutation> = entries
            .iter()
            .take(MAX_MAP_MUTATIONS_PER_APPLY)
            .enumerate()
            .map(|(index, (key, previous))| {
                MapMutation::replace(
                    key.clone(),
                    previous.clone(),
                    value(&format!("new-{index}")),
                )
            })
            .collect();
        let output = apply_map_mutations(&parent, &mutations, &mut memory)
            .expect("maximum map batch applies");
        verify_map_apply(&parent, &mutations, output.proof()).expect("apply proof verifies");
        assert!(
            output
                .proof()
                .nodes
                .iter()
                .all(|node| parent_ids.contains(&node.object_id))
        );
        assert!(
            output
                .objects()
                .iter()
                .all(|node| !parent_ids.contains(&node.object_id))
        );
        assert!(output.objects().len() <= MAX_MAP_PATH_NODES * mutations.len());

        absorb_apply(&mut memory, &output);
        let audit = audit_map(output.verified().result(), &mut memory).expect("result audits");
        let reachable: BTreeSet<&str> = audit
            .objects()
            .iter()
            .map(|node| node.object_id.as_str())
            .collect();
        assert!(
            output
                .objects()
                .iter()
                .all(|node| reachable.contains(node.object_id.as_str()))
        );
    }

    #[test]
    fn range_resolver_loads_scale_with_height_plus_returned_entries() {
        let mut small = range_fixture(1_024);
        let mut large = range_fixture(65_536);
        let small_first = range_loads(&small.root, &mut small.memory, None, &small.first, 17);
        let large_first = range_loads(&large.root, &mut large.memory, None, &large.first, 17);
        let small_middle = range_loads(
            &small.root,
            &mut small.memory,
            Some(&small.middle_cursor),
            &small.middle_successors,
            17,
        );
        let large_middle = range_loads(
            &large.root,
            &mut large.memory,
            Some(&large.middle_cursor),
            &large.middle_successors,
            17,
        );
        let large_wide = range_loads(
            &large.root,
            &mut large.memory,
            Some(&large.middle_cursor),
            &large.middle_successors,
            128,
        );

        assert!(
            large_first <= small_first + (MAX_MAP_PATH_NODES / 8),
            "first page loads grew with total cardinality: {small_first} -> {large_first}"
        );
        assert!(
            large_middle <= small_middle + (MAX_MAP_PATH_NODES / 8),
            "cursor page loads grew with total cardinality: {small_middle} -> {large_middle}"
        );
        assert!(
            large_wide <= large_middle + (4 * (128 - 17)),
            "wider page exceeded a linear frontier: {large_middle} -> {large_wide}"
        );
    }
}
