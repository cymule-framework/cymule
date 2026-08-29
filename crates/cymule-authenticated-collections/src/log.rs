use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::hash::{hash_identifier, validate_content_id};
use crate::{
    CollectionError, CollectionResolver, LOG_FANOUT, MAX_EXACT_INTEGER, MAX_LOG_HEIGHT,
    MAX_LOG_MUTATIONS_PER_APPLY, MAX_LOG_PREFIX_REPLACEMENT_VALUES, MAX_LOG_VALUES_PER_APPLY,
    MAX_MUTATION_BYTES, MAX_PAGE_BYTES, MAX_PAGE_ENTRIES, MAX_PROOF_BYTES, Result,
};

/// Ordered-log node schema owned by this crate.
pub const LOG_NODE_VERSION: &str = "cymule.authenticated-log-node/1";
/// Empty ordered-log commitment domain owned by this crate.
pub const LOG_EMPTY_VERSION: &str = "cymule.authenticated-log-empty/1";
const LOG_MUTATION_VERSION: &str = "cymule.authenticated-log-mutation/1";
const MAX_LOG_EXACT_PROOF_NODES: usize = MAX_LOG_HEIGHT;
const MAX_LOG_SPLIT_PROOF_NODES: usize = MAX_LOG_EXACT_PROOF_NODES * 4;
const MAX_LOG_RANGE_PROOF_NODES: usize = (MAX_LOG_HEIGHT * 4) + (MAX_PAGE_ENTRIES * 2) + 4;
const MAX_LOG_APPLY_PROOF_NODES: usize = MAX_LOG_EXACT_PROOF_NODES * MAX_LOG_VALUES_PER_APPLY * 4;

fn deserialize_log_leaf_values<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, String, LOG_FANOUT>(deserializer)
}

fn deserialize_log_append_values<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, String, MAX_LOG_VALUES_PER_APPLY>(deserializer)
}

fn deserialize_log_prefix_replacement<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, String, MAX_LOG_PREFIX_REPLACEMENT_VALUES>(deserializer)
}

fn deserialize_log_exact_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LogNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, LogNodeWire, MAX_LOG_EXACT_PROOF_NODES>(deserializer)
}

fn deserialize_log_range_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LogNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, LogNodeWire, MAX_LOG_RANGE_PROOF_NODES>(deserializer)
}

fn deserialize_log_split_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LogNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, LogNodeWire, MAX_LOG_SPLIT_PROOF_NODES>(deserializer)
}

fn deserialize_log_mutations<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LogMutation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, LogMutation, MAX_LOG_MUTATIONS_PER_APPLY>(deserializer)
}

fn deserialize_log_apply_nodes<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<LogNodeWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    crate::deserialize_bounded_vec::<D, LogNodeWire, MAX_LOG_APPLY_PROOF_NODES>(deserializer)
}

/// Root of one immutable ordered AVL log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRoot {
    /// Root-node content identity; absent exactly when the log is empty.
    pub node: Option<String>,
    /// Exact ordered value count.
    pub len: u64,
    /// Exact AVL height; zero only for an empty log.
    pub height: u8,
    /// Complete ordered commitment; equal to the node identity when nonempty.
    pub ordered_root: String,
}

impl LogRoot {
    /// Construct the unique empty log root.
    pub fn empty() -> Self {
        Self {
            node: None,
            len: 0,
            height: 0,
            ordered_root: empty_log_commitment().clone(),
        }
    }

    /// Verify the closed root shape.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, count, height, and commitment disagree.
    pub fn verify(&self) -> Result<()> {
        if self.len == 0 {
            if self.node.is_some()
                || self.height != 0
                || self.ordered_root != *empty_log_commitment()
            {
                return Err(CollectionError::Validation(
                    "empty authenticated-log root has nonempty authority".to_owned(),
                ));
            }
            return Ok(());
        }
        let node = self.node.as_ref().ok_or_else(|| {
            CollectionError::Validation("nonempty authenticated log has no node".to_owned())
        })?;
        validate_content_id("authenticated-log root", node)?;
        if self.len > MAX_EXACT_INTEGER
            || self.height == 0
            || usize::from(self.height) > MAX_LOG_HEIGHT
            || self.ordered_root != *node
        {
            return Err(CollectionError::Validation(
                "authenticated-log root has invalid length, height, or commitment".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for LogRoot {
    fn default() -> Self {
        Self::empty()
    }
}

fn empty_log_commitment() -> &'static String {
    static EMPTY: OnceLock<String> = OnceLock::new();
    EMPTY.get_or_init(|| hash_identifier(LOG_EMPTY_VERSION, &[b"empty", &0_u64.to_be_bytes()]))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogChild {
    object_id: String,
    len: u64,
    height: u8,
}

impl LogChild {
    fn verify(&self) -> Result<()> {
        validate_content_id("authenticated-log child", &self.object_id)?;
        if self.len == 0
            || self.len > MAX_EXACT_INTEGER
            || self.height == 0
            || usize::from(self.height) > MAX_LOG_HEIGHT
        {
            return Err(CollectionError::Validation(
                "authenticated-log child has invalid length or height".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum LogNodeBody {
    Leaf {
        #[serde(deserialize_with = "deserialize_log_leaf_values")]
        values: Vec<String>,
    },
    Branch {
        left: LogChild,
        right: LogChild,
    },
}

/// One immutable ordered-log node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogNode {
    /// Exact node schema.
    pub node_version: String,
    /// Content identity derived from the closed binary preimage.
    pub object_id: String,
    body: LogNodeBody,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogNodeWire {
    node_version: String,
    object_id: String,
    body: LogNodeBody,
}

impl LogNodeWire {
    fn into_verified(self) -> Result<LogNode> {
        let node = LogNode {
            node_version: self.node_version,
            object_id: self.object_id,
            body: self.body,
        };
        node.verify()?;
        Ok(node)
    }
}

/// Decode one immutable log node only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input, or when the node bytes do
/// not satisfy the exact schema and content identity.
pub fn decode_log_node(bytes: &[u8]) -> Result<LogNode> {
    crate::decode_json_bounded::<LogNodeWire>(
        bytes,
        crate::MAX_NODE_TRANSPORT_BYTES,
        "authenticated-log node",
    )?
    .into_verified()
}

/// Log-node object persisted by a provider.
pub type LogObject = LogNode;

impl LogNode {
    fn leaf(values: Vec<String>) -> Result<Self> {
        Self::from_body(LogNodeBody::Leaf { values })
    }

    fn branch(left: LogChild, right: LogChild) -> Result<Self> {
        Self::from_body(LogNodeBody::Branch { left, right })
    }

    fn from_body(body: LogNodeBody) -> Result<Self> {
        let object_id = log_node_id(&body);
        let node = Self {
            node_version: LOG_NODE_VERSION.to_owned(),
            object_id,
            body,
        };
        node.verify()?;
        Ok(node)
    }

    /// Verify local AVL shape and exact node preimage.
    ///
    /// Repeated value identities are valid because this is an ordered log, not
    /// a set.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, counts, heights, balance, or
    /// node identity.
    pub fn verify(&self) -> Result<()> {
        if self.node_version != LOG_NODE_VERSION {
            return Err(CollectionError::Validation(format!(
                "unsupported authenticated-log node version {:?}",
                self.node_version
            )));
        }
        match &self.body {
            LogNodeBody::Leaf { values } => {
                if values.is_empty() || values.len() > LOG_FANOUT {
                    return Err(CollectionError::Validation(
                        "authenticated-log leaf has an invalid value count".to_owned(),
                    ));
                }
                for value in values {
                    validate_content_id("authenticated-log value", value)?;
                }
            }
            LogNodeBody::Branch { left, right } => {
                left.verify()?;
                right.verify()?;
                if left.height.abs_diff(right.height) > 1 {
                    return Err(CollectionError::Validation(
                        "authenticated-log branch violates AVL balance".to_owned(),
                    ));
                }
                let _ = self.value_count()?;
                let _ = self.height()?;
            }
        }
        let expected = log_node_id(&self.body);
        if self.object_id != expected {
            return Err(CollectionError::Integrity {
                code: "log_node_identity_mismatch",
                message: format!(
                    "authenticated-log node identity {} does not match {expected}",
                    self.object_id
                ),
            });
        }
        Ok(())
    }

    /// Exact ordered value count committed by this node.
    ///
    /// # Errors
    ///
    /// Returns an error when child counts overflow the shared exact range.
    pub fn value_count(&self) -> Result<u64> {
        match &self.body {
            LogNodeBody::Leaf { values } => u64::try_from(values.len())
                .map_err(|error| CollectionError::Validation(error.to_string())),
            LogNodeBody::Branch { left, right } => left
                .len
                .checked_add(right.len)
                .filter(|len| *len <= MAX_EXACT_INTEGER)
                .ok_or_else(|| {
                    CollectionError::Validation(
                        "authenticated-log child lengths overflowed".to_owned(),
                    )
                }),
        }
    }

    /// Exact AVL height committed by this node.
    ///
    /// # Errors
    ///
    /// Returns an error when the height exceeds the closed bound.
    pub fn height(&self) -> Result<u8> {
        let height = match &self.body {
            LogNodeBody::Leaf { .. } => 1,
            LogNodeBody::Branch { left, right } => left
                .height
                .max(right.height)
                .checked_add(1)
                .ok_or_else(|| {
                    CollectionError::Validation("authenticated-log height overflowed".to_owned())
                })?,
        };
        if usize::from(height) > MAX_LOG_HEIGHT {
            return Err(CollectionError::Validation(
                "authenticated-log height exceeds its bound".to_owned(),
            ));
        }
        Ok(height)
    }

    /// Immutable child-node identities referenced by this node.
    pub fn child_object_ids(&self) -> Vec<&str> {
        match &self.body {
            LogNodeBody::Leaf { .. } => Vec::new(),
            LogNodeBody::Branch { left, right } => {
                vec![left.object_id.as_str(), right.object_id.as_str()]
            }
        }
    }

    /// Ordered opaque value identities referenced by a leaf.
    pub fn value_object_ids(&self) -> &[String] {
        match &self.body {
            LogNodeBody::Leaf { values } => values,
            LogNodeBody::Branch { .. } => &[],
        }
    }

    fn logical_bytes(&self) -> Result<usize> {
        let fixed = self
            .node_version
            .len()
            .checked_add(self.object_id.len())
            .ok_or_else(|| CollectionError::Validation("log node bytes overflowed".to_owned()))?;
        match &self.body {
            LogNodeBody::Leaf { values } => values.iter().try_fold(fixed, |bytes, value| {
                bytes.checked_add(value.len()).ok_or_else(|| {
                    CollectionError::Validation("log node bytes overflowed".to_owned())
                })
            }),
            LogNodeBody::Branch { left, right } => fixed
                .checked_add(left.object_id.len())
                .and_then(|bytes| bytes.checked_add(right.object_id.len()))
                .and_then(|bytes| bytes.checked_add(32))
                .ok_or_else(|| CollectionError::Validation("log node bytes overflowed".to_owned())),
        }
    }
}

fn log_node_id(body: &LogNodeBody) -> String {
    match body {
        LogNodeBody::Leaf { values } => {
            let count = u64::try_from(values.len()).expect("leaf count fits u64");
            let mut fields = Vec::with_capacity(values.len() + 2);
            fields.push(b"leaf".as_slice());
            let count_bytes = count.to_be_bytes();
            fields.push(count_bytes.as_slice());
            for value in values {
                fields.push(value.as_bytes());
            }
            hash_identifier(LOG_NODE_VERSION, &fields)
        }
        LogNodeBody::Branch { left, right } => hash_identifier(
            LOG_NODE_VERSION,
            &[
                b"branch",
                left.object_id.as_bytes(),
                &left.len.to_be_bytes(),
                &[left.height],
                right.object_id.as_bytes(),
                &right.len.to_be_bytes(),
                &[right.height],
            ],
        ),
    }
}

#[derive(Debug, Clone)]
struct NodeRef {
    object_id: String,
    len: u64,
    height: u8,
}

impl NodeRef {
    fn into_child(self) -> LogChild {
        LogChild {
            object_id: self.object_id,
            len: self.len,
            height: self.height,
        }
    }
}

impl From<LogChild> for NodeRef {
    fn from(child: LogChild) -> Self {
        Self {
            object_id: child.object_id,
            len: child.len,
            height: child.height,
        }
    }
}

fn root_ref(root: &LogRoot) -> Result<Option<NodeRef>> {
    root.verify()?;
    Ok(root.node.as_ref().map(|object_id| NodeRef {
        object_id: object_id.clone(),
        len: root.len,
        height: root.height,
    }))
}

fn ref_root(reference: Option<NodeRef>) -> Result<LogRoot> {
    match reference {
        None => Ok(LogRoot::empty()),
        Some(reference) => {
            let root = LogRoot {
                node: Some(reference.object_id.clone()),
                len: reference.len,
                height: reference.height,
                ordered_root: reference.object_id,
            };
            root.verify()?;
            Ok(root)
        }
    }
}

struct Overlay<'a, R: CollectionResolver + ?Sized> {
    resolver: &'a mut R,
    pending: BTreeMap<String, LogNode>,
    loaded: BTreeMap<String, LogNode>,
}

impl<'a, R: CollectionResolver + ?Sized> Overlay<'a, R> {
    fn new(resolver: &'a mut R) -> Self {
        Self {
            resolver,
            pending: BTreeMap::new(),
            loaded: BTreeMap::new(),
        }
    }

    fn load(&mut self, reference: &NodeRef) -> Result<LogNode> {
        let node = if let Some(node) = self.pending.get(&reference.object_id) {
            node.clone()
        } else if let Some(node) = self.loaded.get(&reference.object_id) {
            node.clone()
        } else {
            let node = self
                .resolver
                .load_log_node(&reference.object_id)?
                .ok_or_else(|| CollectionError::MissingObject(reference.object_id.clone()))?;
            node.verify()?;
            if node.object_id != reference.object_id {
                return Err(CollectionError::Integrity {
                    code: "log_node_locator_mismatch",
                    message: format!(
                        "log node locator {} resolves to {}",
                        reference.object_id, node.object_id
                    ),
                });
            }
            self.loaded.insert(node.object_id.clone(), node.clone());
            node
        };
        if node.value_count()? != reference.len || node.height()? != reference.height {
            return Err(CollectionError::Integrity {
                code: "log_child_shape_mismatch",
                message: format!(
                    "log child {} contradicts its parent length or height",
                    reference.object_id
                ),
            });
        }
        Ok(node)
    }

    fn store(&mut self, node: LogNode) -> Result<NodeRef> {
        node.verify()?;
        let reference = NodeRef {
            object_id: node.object_id.clone(),
            len: node.value_count()?,
            height: node.height()?,
        };
        if let Some(existing) = self.pending.get(&node.object_id)
            && existing != &node
        {
            return Err(CollectionError::Integrity {
                code: "log_node_identity_conflict",
                message: format!("log node {} has conflicting bytes", node.object_id),
            });
        }
        self.pending.insert(node.object_id.clone(), node);
        Ok(reference)
    }
}

/// One ordered mutation over an authenticated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum LogMutation {
    /// Append these exact opaque identities in order. Repeats are valid.
    Append {
        /// Ordered identities to append.
        #[serde(deserialize_with = "deserialize_log_append_values")]
        values: Vec<String>,
    },
    /// Insert one exact value before an ordinal, or at the current end.
    InsertAt {
        /// Zero-based insertion ordinal in the current mutation state.
        index: u64,
        /// Exact opaque identity to insert.
        value: String,
    },
    /// Replace one exact ordinal and expected identity in place.
    ReplaceAt {
        /// Zero-based exact ordinal in the current mutation state.
        index: u64,
        /// Exact opaque identity currently at that ordinal.
        expected: String,
        /// Different exact opaque identity to store.
        value: String,
    },
    /// Replace one authenticated prefix while preserving the complete suffix.
    ReplacePrefix {
        /// Exact prefix length in the current mutation state.
        count: u64,
        /// Exact authenticated root produced by splitting at `count`.
        expected_prefix: LogRoot,
        /// Bounded ordered replacement; repeats are valid and empty removes the prefix.
        #[serde(deserialize_with = "deserialize_log_prefix_replacement")]
        replacement: Vec<String>,
    },
    /// Remove one exact ordinal and value identity.
    RemoveAt {
        /// Zero-based exact ordinal.
        index: u64,
        /// Exact opaque identity currently at that ordinal.
        expected: String,
    },
}

impl LogMutation {
    /// Construct one ordered append. Repeated values remain significant.
    pub fn append(values: Vec<String>) -> Self {
        Self::Append { values }
    }

    /// Construct one exact ordinal removal.
    pub fn remove_at(index: u64, expected: impl Into<String>) -> Self {
        Self::RemoveAt {
            index,
            expected: expected.into(),
        }
    }

    /// Construct one exact ordinal insertion.
    pub fn insert_at(index: u64, value: impl Into<String>) -> Self {
        Self::InsertAt {
            index,
            value: value.into(),
        }
    }

    /// Construct one exact in-place ordinal replacement.
    pub fn replace_at(index: u64, expected: impl Into<String>, value: impl Into<String>) -> Self {
        Self::ReplaceAt {
            index,
            expected: expected.into(),
            value: value.into(),
        }
    }

    /// Construct one authenticated bounded prefix replacement.
    pub fn replace_prefix(count: u64, expected_prefix: LogRoot, replacement: Vec<String>) -> Self {
        Self::ReplacePrefix {
            count,
            expected_prefix,
            replacement,
        }
    }

    fn verify(&self) -> Result<()> {
        match self {
            Self::Append { values } => {
                if values.is_empty() {
                    return Err(CollectionError::Validation(
                        "authenticated-log append is empty".to_owned(),
                    ));
                }
                for value in values {
                    validate_content_id("appended log value", value)?;
                }
                Ok(())
            }
            Self::InsertAt { index, value } => {
                if *index > MAX_EXACT_INTEGER {
                    return Err(CollectionError::Validation(
                        "log insertion index exceeds the exact range".to_owned(),
                    ));
                }
                validate_content_id("inserted log value", value)
            }
            Self::ReplaceAt {
                index,
                expected,
                value,
            } => {
                if *index > MAX_EXACT_INTEGER {
                    return Err(CollectionError::Validation(
                        "log replacement index exceeds the exact range".to_owned(),
                    ));
                }
                validate_content_id("replaced log value", expected)?;
                validate_content_id("replacement log value", value)?;
                if expected == value {
                    return Err(CollectionError::Validation(
                        "log replacement must change the exact value".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::ReplacePrefix {
                count,
                expected_prefix,
                replacement,
            } => {
                expected_prefix.verify()?;
                if *count != expected_prefix.len {
                    return Err(CollectionError::Validation(
                        "log prefix replacement count does not match its authenticated prefix"
                            .to_owned(),
                    ));
                }
                if replacement.len() > MAX_LOG_PREFIX_REPLACEMENT_VALUES {
                    return Err(CollectionError::Validation(format!(
                        "log prefix replacement exceeds {MAX_LOG_PREFIX_REPLACEMENT_VALUES} values"
                    )));
                }
                if *count == 0 && replacement.is_empty() {
                    return Err(CollectionError::Validation(
                        "log prefix replacement is empty".to_owned(),
                    ));
                }
                for value in replacement {
                    validate_content_id("prefix replacement log value", value)?;
                }
                Ok(())
            }
            Self::RemoveAt { index, expected } => {
                if *index > MAX_EXACT_INTEGER {
                    return Err(CollectionError::Validation(
                        "log removal index exceeds the exact range".to_owned(),
                    ));
                }
                validate_content_id("removed log value", expected)
            }
        }
    }
}

fn validate_mutations(mutations: &[LogMutation]) -> Result<Vec<LogMutation>> {
    if !(1..=MAX_LOG_MUTATIONS_PER_APPLY).contains(&mutations.len()) {
        return Err(CollectionError::Validation(format!(
            "log mutation batch must contain 1..={MAX_LOG_MUTATIONS_PER_APPLY} entries"
        )));
    }
    let mut primitive_count = 0_usize;
    let mut mutation_bytes = 0_usize;
    for mutation in mutations {
        mutation.verify()?;
        let (primitives, bytes) = match mutation {
            LogMutation::Append { values } => {
                let bytes = values.iter().try_fold(0_usize, |bytes, value| {
                    bytes.checked_add(value.len()).ok_or_else(|| {
                        CollectionError::Validation("log mutation bytes overflowed".to_owned())
                    })
                })?;
                (values.len(), bytes)
            }
            LogMutation::InsertAt { value, .. } => (1, value.len()),
            LogMutation::ReplaceAt {
                expected, value, ..
            } => (
                1,
                expected.len().checked_add(value.len()).ok_or_else(|| {
                    CollectionError::Validation("log mutation bytes overflowed".to_owned())
                })?,
            ),
            LogMutation::ReplacePrefix {
                expected_prefix,
                replacement,
                ..
            } => {
                let bytes = replacement.iter().try_fold(
                    expected_prefix
                        .node
                        .as_ref()
                        .map_or(0, String::len)
                        .checked_add(expected_prefix.ordered_root.len())
                        .ok_or_else(|| {
                            CollectionError::Validation("log mutation bytes overflowed".to_owned())
                        })?,
                    |bytes, value| {
                        bytes.checked_add(value.len()).ok_or_else(|| {
                            CollectionError::Validation("log mutation bytes overflowed".to_owned())
                        })
                    },
                )?;
                (replacement.len(), bytes)
            }
            LogMutation::RemoveAt { expected, .. } => (1, expected.len()),
        };
        primitive_count = primitive_count
            .checked_add(primitives)
            .filter(|count| *count <= MAX_LOG_VALUES_PER_APPLY)
            .ok_or_else(|| {
                CollectionError::Validation(format!(
                    "log mutation batch exceeds {MAX_LOG_VALUES_PER_APPLY} primitive values"
                ))
            })?;
        mutation_bytes = mutation_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_MUTATION_BYTES)
            .ok_or_else(|| {
                CollectionError::Validation(format!(
                    "log mutation batch exceeds {MAX_MUTATION_BYTES} logical bytes"
                ))
            })?;
    }
    Ok(mutations.to_vec())
}

fn mutation_digest(mutations: &[LogMutation]) -> String {
    let mut owned = Vec::new();
    for mutation in mutations {
        match mutation {
            LogMutation::Append { values } => {
                owned.push(b"append".to_vec());
                owned.push(
                    u64::try_from(values.len())
                        .expect("append count fits u64")
                        .to_be_bytes()
                        .to_vec(),
                );
                owned.extend(values.iter().map(|value| value.as_bytes().to_vec()));
            }
            LogMutation::RemoveAt { index, expected } => {
                owned.push(b"remove_at".to_vec());
                owned.push(index.to_be_bytes().to_vec());
                owned.push(expected.as_bytes().to_vec());
            }
            LogMutation::InsertAt { index, value } => {
                owned.push(b"insert_at".to_vec());
                owned.push(index.to_be_bytes().to_vec());
                owned.push(value.as_bytes().to_vec());
            }
            LogMutation::ReplaceAt {
                index,
                expected,
                value,
            } => {
                owned.push(b"replace_at".to_vec());
                owned.push(index.to_be_bytes().to_vec());
                owned.push(expected.as_bytes().to_vec());
                owned.push(value.as_bytes().to_vec());
            }
            LogMutation::ReplacePrefix {
                count,
                expected_prefix,
                replacement,
            } => {
                owned.push(b"replace_prefix".to_vec());
                owned.push(count.to_be_bytes().to_vec());
                owned.push(
                    expected_prefix
                        .node
                        .as_deref()
                        .unwrap_or("")
                        .as_bytes()
                        .to_vec(),
                );
                owned.push(expected_prefix.len.to_be_bytes().to_vec());
                owned.push(vec![expected_prefix.height]);
                owned.push(expected_prefix.ordered_root.as_bytes().to_vec());
                owned.push(
                    u64::try_from(replacement.len())
                        .expect("prefix replacement count fits u64")
                        .to_be_bytes()
                        .to_vec(),
                );
                owned.extend(replacement.iter().map(|value| value.as_bytes().to_vec()));
            }
        }
    }
    let fields: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    hash_identifier(LOG_MUTATION_VERSION, &fields)
}

/// Raw proof for one exact log ordinal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogExactProof {
    root: LogRoot,
    index: u64,
    nodes: Vec<LogNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogExactProofWire {
    root: LogRoot,
    index: u64,
    #[serde(deserialize_with = "deserialize_log_exact_nodes")]
    nodes: Vec<LogNodeWire>,
}

impl LogExactProofWire {
    fn into_proof(self) -> Result<LogExactProof> {
        Ok(LogExactProof {
            root: self.root,
            index: self.index,
            nodes: self
                .nodes
                .into_iter()
                .map(LogNodeWire::into_verified)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// Decode one exact log proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_log_exact`].
pub fn decode_log_exact_proof(bytes: &[u8]) -> Result<LogExactProof> {
    crate::decode_json_bounded::<LogExactProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-log exact proof",
    )?
    .into_proof()
}

/// Non-serializable verified binding of one exact log ordinal and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLogRead {
    root: LogRoot,
    index: u64,
    value: String,
}

impl VerifiedLogRead {
    /// Exact authenticated source root.
    pub fn root(&self) -> &LogRoot {
        &self.root
    }

    /// Exact authenticated ordinal.
    pub const fn index(&self) -> u64 {
        self.index
    }

    /// Exact opaque value identity at the ordinal.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Generate one exact ordinal proof.
///
/// # Errors
///
/// Returns an error for an out-of-range index or invalid/missing node.
pub fn prove_log_exact<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    resolver: &mut R,
) -> Result<LogExactProof> {
    root.verify()?;
    if index >= root.len {
        return Err(CollectionError::Validation(format!(
            "log index {index} exceeds length {}",
            root.len
        )));
    }
    let mut recorder = RecordingLogResolver::new(resolver);
    let _ = get_log_value(root, index, &mut recorder)?;
    Ok(LogExactProof {
        root: root.clone(),
        index,
        nodes: recorder.nodes.into_values().collect(),
    })
}

/// Verify one exact ordinal proof against caller-owned authority.
///
/// # Errors
///
/// Returns an error for a wrong root/index, path, length, height, leaf slice, or
/// unused node.
pub fn verify_log_exact(
    expected_root: &LogRoot,
    expected_index: u64,
    proof: &LogExactProof,
) -> Result<VerifiedLogRead> {
    expected_root.verify()?;
    if &proof.root != expected_root || proof.index != expected_index {
        return Err(CollectionError::Integrity {
            code: "log_exact_authority_mismatch",
            message: "log exact proof is bound to another root or index".to_owned(),
        });
    }
    let mut resolver = ProofLogResolver::new(&proof.nodes, MAX_LOG_HEIGHT)?;
    let value = get_log_value(expected_root, expected_index, &mut resolver)?;
    resolver.finish()?;
    Ok(VerifiedLogRead {
        root: expected_root.clone(),
        index: expected_index,
        value,
    })
}

/// Raw proof of one exact authenticated split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogSplitProof {
    parent: LogRoot,
    index: u64,
    prefix: LogRoot,
    suffix: LogRoot,
    nodes: Vec<LogNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSplitProofWire {
    parent: LogRoot,
    index: u64,
    prefix: LogRoot,
    suffix: LogRoot,
    #[serde(deserialize_with = "deserialize_log_split_nodes")]
    nodes: Vec<LogNodeWire>,
}

/// Decode one log split proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_log_split`].
pub fn decode_log_split_proof(bytes: &[u8]) -> Result<LogSplitProof> {
    let wire = crate::decode_json_bounded::<LogSplitProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-log split proof",
    )?;
    Ok(LogSplitProof {
        parent: wire.parent,
        index: wire.index,
        prefix: wire.prefix,
        suffix: wire.suffix,
        nodes: wire
            .nodes
            .into_iter()
            .map(LogNodeWire::into_verified)
            .collect::<Result<Vec<_>>>()?,
    })
}

/// Non-serializable verified binding of one parent to exact prefix/suffix roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLogSplit {
    parent: LogRoot,
    index: u64,
    prefix: LogRoot,
    suffix: LogRoot,
}

impl VerifiedLogSplit {
    /// Exact authenticated parent root.
    pub fn parent(&self) -> &LogRoot {
        &self.parent
    }

    /// Exact split ordinal.
    pub const fn index(&self) -> u64 {
        self.index
    }

    /// Exact authenticated prefix root.
    pub fn prefix(&self) -> &LogRoot {
        &self.prefix
    }

    /// Exact authenticated suffix root.
    pub fn suffix(&self) -> &LogRoot {
        &self.suffix
    }
}

/// Provider output for one authenticated O(log N) split.
#[derive(Debug, Clone)]
pub struct LogSplitOutput {
    verified: VerifiedLogSplit,
    proof: LogSplitProof,
    objects: Vec<LogNode>,
}

impl LogSplitOutput {
    /// Verified parent/index/prefix/suffix binding.
    pub fn verified(&self) -> &VerifiedLogSplit {
        &self.verified
    }

    /// Portable independently verifiable split proof.
    pub fn proof(&self) -> &LogSplitProof {
        &self.proof
    }

    /// Newly created final-reachable nodes for the prefix and suffix roots.
    pub fn objects(&self) -> &[LogNode] {
        &self.objects
    }

    /// Consume the output into its portable proof and created nodes.
    pub fn into_parts(self) -> (LogSplitProof, Vec<LogNode>) {
        (self.proof, self.objects)
    }
}

/// Split one authenticated log without materializing either side's values.
///
/// # Errors
///
/// Returns an error for an invalid index, root, or missing/invalid path node.
pub fn split_log<R: CollectionResolver + ?Sized>(
    parent: &LogRoot,
    index: u64,
    resolver: &mut R,
) -> Result<LogSplitOutput> {
    parent.verify()?;
    if index > parent.len {
        return Err(CollectionError::Validation(format!(
            "log split {index} exceeds length {}",
            parent.len
        )));
    }
    let mut overlay = Overlay::new(resolver);
    let (prefix, suffix) = split_root(parent, index, &mut overlay)?;
    let proof_nodes = overlay.loaded.clone().into_values().collect();
    let objects = collect_reachable_created_log_nodes_for_roots(&[&prefix, &suffix], &mut overlay)?;
    let proof = LogSplitProof {
        parent: parent.clone(),
        index,
        prefix: prefix.clone(),
        suffix: suffix.clone(),
        nodes: proof_nodes,
    };
    let verified = verify_log_split(parent, index, &proof)?;
    Ok(LogSplitOutput {
        verified,
        proof,
        objects,
    })
}

/// Verify one split proof by replaying the exact AVL split algorithm.
///
/// # Errors
///
/// Returns an error for a stale parent/index, missing/extra node, or substituted
/// prefix/suffix root.
pub fn verify_log_split(
    expected_parent: &LogRoot,
    expected_index: u64,
    proof: &LogSplitProof,
) -> Result<VerifiedLogSplit> {
    expected_parent.verify()?;
    if expected_index > expected_parent.len {
        return Err(CollectionError::Validation(format!(
            "log split {expected_index} exceeds length {}",
            expected_parent.len
        )));
    }
    if &proof.parent != expected_parent || proof.index != expected_index {
        return Err(CollectionError::Integrity {
            code: "log_split_authority_mismatch",
            message: "log split proof is bound to another parent or index".to_owned(),
        });
    }
    let maximum = MAX_LOG_HEIGHT
        .checked_mul(4)
        .ok_or_else(|| CollectionError::Validation("log split bound overflowed".to_owned()))?;
    let mut resolver = ApplyProofLogResolver::new(&proof.nodes, maximum)?;
    let mut overlay = Overlay::new(&mut resolver);
    let (prefix, suffix) = split_root(expected_parent, expected_index, &mut overlay)?;
    if prefix != proof.prefix || suffix != proof.suffix {
        return Err(CollectionError::Integrity {
            code: "log_split_result_mismatch",
            message: "log split proof substituted its prefix or suffix root".to_owned(),
        });
    }
    let loaded: BTreeSet<String> = overlay.loaded.keys().cloned().collect();
    resolver.finish(&loaded)?;
    Ok(VerifiedLogSplit {
        parent: expected_parent.clone(),
        index: expected_index,
        prefix,
        suffix,
    })
}

fn get_log_value<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    mut index: u64,
    resolver: &mut R,
) -> Result<String> {
    let mut reference = root_ref(root)?
        .ok_or_else(|| CollectionError::Validation("empty log has no exact ordinal".to_owned()))?;
    for _ in 0..=MAX_LOG_HEIGHT {
        let node = load_resolved_log_node(resolver, &reference)?;
        match node.body {
            LogNodeBody::Leaf { values } => {
                let index = usize::try_from(index)
                    .map_err(|error| CollectionError::Validation(error.to_string()))?;
                return values
                    .get(index)
                    .cloned()
                    .ok_or_else(|| CollectionError::Integrity {
                        code: "log_leaf_index_mismatch",
                        message: "log leaf does not contain its routed ordinal".to_owned(),
                    });
            }
            LogNodeBody::Branch { left, right } => {
                if index < left.len {
                    reference = left.into();
                } else {
                    index =
                        index
                            .checked_sub(left.len)
                            .ok_or_else(|| CollectionError::Integrity {
                                code: "log_index_underflow",
                                message: "log ordinal underflowed".to_owned(),
                            })?;
                    reference = right.into();
                }
            }
        }
    }
    Err(CollectionError::Integrity {
        code: "log_path_bound_exceeded",
        message: "log exact path exceeds the AVL height bound".to_owned(),
    })
}

fn load_resolved_log_node<R: CollectionResolver + ?Sized>(
    resolver: &mut R,
    reference: &NodeRef,
) -> Result<LogNode> {
    let node = resolver
        .load_log_node(&reference.object_id)?
        .ok_or_else(|| CollectionError::MissingObject(reference.object_id.clone()))?;
    node.verify()?;
    if node.object_id != reference.object_id
        || node.value_count()? != reference.len
        || node.height()? != reference.height
    {
        return Err(CollectionError::Integrity {
            code: "log_resolved_child_mismatch",
            message: format!(
                "resolved log node {} contradicts its parent edge",
                reference.object_id
            ),
        });
    }
    Ok(node)
}

struct RecordingLogResolver<'a, R: CollectionResolver + ?Sized> {
    inner: &'a mut R,
    nodes: BTreeMap<String, LogNode>,
}

impl<'a, R: CollectionResolver + ?Sized> RecordingLogResolver<'a, R> {
    fn new(inner: &'a mut R) -> Self {
        Self {
            inner,
            nodes: BTreeMap::new(),
        }
    }
}

impl<R: CollectionResolver + ?Sized> CollectionResolver for RecordingLogResolver<'_, R> {
    fn load_map_node(&mut self, object_id: &str) -> Result<Option<crate::MapNode>> {
        self.inner.load_map_node(object_id)
    }

    fn load_log_node(&mut self, object_id: &str) -> Result<Option<LogNode>> {
        if let Some(node) = self.nodes.get(object_id) {
            return Ok(Some(node.clone()));
        }
        let node = self.inner.load_log_node(object_id)?;
        if let Some(node) = &node {
            self.nodes.insert(object_id.to_owned(), node.clone());
        }
        Ok(node)
    }
}

struct ProofLogResolver {
    nodes: BTreeMap<String, LogNode>,
    used: BTreeSet<String>,
}

impl ProofLogResolver {
    fn new(nodes: &[LogNode], maximum: usize) -> Result<Self> {
        if nodes.len() > maximum {
            return Err(CollectionError::Validation(
                "log proof exceeds its node bound".to_owned(),
            ));
        }
        verify_log_proof_bytes(nodes)?;
        let mut indexed = BTreeMap::new();
        for node in nodes {
            node.verify()?;
            if indexed
                .insert(node.object_id.clone(), node.clone())
                .is_some()
            {
                return Err(CollectionError::Integrity {
                    code: "log_proof_duplicate_node",
                    message: format!("log proof repeats node {}", node.object_id),
                });
            }
        }
        Ok(Self {
            nodes: indexed,
            used: BTreeSet::new(),
        })
    }

    fn finish(&self) -> Result<()> {
        if self.used.len() != self.nodes.len() {
            return Err(CollectionError::Integrity {
                code: "log_proof_unused_node",
                message: "log proof includes a node outside its canonical path".to_owned(),
            });
        }
        Ok(())
    }
}

impl CollectionResolver for ProofLogResolver {
    fn load_map_node(&mut self, _object_id: &str) -> Result<Option<crate::MapNode>> {
        Ok(None)
    }

    fn load_log_node(&mut self, object_id: &str) -> Result<Option<LogNode>> {
        let node = self.nodes.get(object_id).cloned();
        if node.is_some() {
            self.used.insert(object_id.to_owned());
        }
        Ok(node)
    }
}

fn verify_log_proof_bytes(nodes: &[LogNode]) -> Result<()> {
    let mut bytes = 0_usize;
    for node in nodes {
        bytes = bytes
            .checked_add(node.logical_bytes()?)
            .filter(|bytes| *bytes <= MAX_PROOF_BYTES)
            .ok_or_else(|| {
                CollectionError::Validation(format!(
                    "log proof exceeds {MAX_PROOF_BYTES} logical node bytes"
                ))
            })?;
    }
    Ok(())
}

/// Raw bounded exact-ordinal range proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogRangeProof {
    root: LogRoot,
    start: u64,
    limit: u16,
    max_bytes: u64,
    nodes: Vec<LogNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRangeProofWire {
    root: LogRoot,
    start: u64,
    limit: u16,
    max_bytes: u64,
    #[serde(deserialize_with = "deserialize_log_range_nodes")]
    nodes: Vec<LogNodeWire>,
}

/// Decode one log range proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_log_range`].
pub fn decode_log_range_proof(bytes: &[u8]) -> Result<LogRangeProof> {
    let wire = crate::decode_json_bounded::<LogRangeProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-log range proof",
    )?;
    Ok(LogRangeProof {
        root: wire.root,
        start: wire.start,
        limit: wire.limit,
        max_bytes: wire.max_bytes,
        nodes: wire
            .nodes
            .into_iter()
            .map(LogNodeWire::into_verified)
            .collect::<Result<Vec<_>>>()?,
    })
}

/// Verified bounded, omission-free log range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLogRange {
    root: LogRoot,
    start: u64,
    values: Vec<String>,
    next_index: Option<u64>,
}

impl VerifiedLogRange {
    /// Exact source root.
    pub fn root(&self) -> &LogRoot {
        &self.root
    }

    /// Exact first ordinal.
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Ordered exact opaque identities. Repeats are preserved.
    pub fn values(&self) -> &[String] {
        &self.values
    }

    /// Next ordinal, present only when an authenticated successor exists.
    pub const fn next_index(&self) -> Option<u64> {
        self.next_index
    }

    /// Whether the range has an authenticated successor.
    pub const fn has_more(&self) -> bool {
        self.next_index.is_some()
    }
}

/// Generate one bounded exact-ordinal log range.
///
/// # Errors
///
/// Returns an error for invalid request bounds, root, or resolved nodes.
pub fn prove_log_range<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    start: u64,
    limit: usize,
    max_bytes: usize,
    resolver: &mut R,
) -> Result<LogRangeProof> {
    validate_range_request(root, start, limit, max_bytes)?;
    let mut recording = RecordingLogResolver::new(resolver);
    let _ = collect_log_range(root, start, limit, max_bytes, &mut recording)?;
    Ok(LogRangeProof {
        root: root.clone(),
        start,
        limit: u16::try_from(limit)
            .map_err(|error| CollectionError::Validation(error.to_string()))?,
        max_bytes: u64::try_from(max_bytes)
            .map_err(|error| CollectionError::Validation(error.to_string()))?,
        nodes: recording.nodes.into_values().collect(),
    })
}

/// Verify one bounded omission-free log range.
///
/// # Errors
///
/// Returns an error for a wrong root/start/request, skipped/reordered/substituted
/// ordinal, false terminal boundary, or invalid path.
pub fn verify_log_range(
    expected_root: &LogRoot,
    expected_start: u64,
    limit: usize,
    max_bytes: usize,
    proof: &LogRangeProof,
) -> Result<VerifiedLogRange> {
    validate_range_request(expected_root, expected_start, limit, max_bytes)?;
    if &proof.root != expected_root
        || proof.start != expected_start
        || usize::from(proof.limit) != limit
        || usize::try_from(proof.max_bytes).ok() != Some(max_bytes)
    {
        return Err(CollectionError::Integrity {
            code: "log_range_authority_mismatch",
            message: "log range proof is bound to another root, start, or request".to_owned(),
        });
    }
    if proof.nodes.len() > MAX_LOG_RANGE_PROOF_NODES {
        return Err(CollectionError::Validation(
            "log range proof exceeds its node bound".to_owned(),
        ));
    }
    let mut resolver = ProofLogResolver::new(&proof.nodes, MAX_LOG_RANGE_PROOF_NODES)?;
    let selection = collect_log_range(
        expected_root,
        expected_start,
        limit,
        max_bytes,
        &mut resolver,
    )?;
    resolver.finish()?;
    let next_index =
        expected_start
            .checked_add(u64::try_from(selection.values.len()).map_err(|error| {
                CollectionError::Validation(format!("log range length: {error}"))
            })?)
            .ok_or_else(|| CollectionError::Integrity {
                code: "log_range_index_overflow",
                message: "log range terminal index overflowed".to_owned(),
            })?;
    Ok(VerifiedLogRange {
        root: expected_root.clone(),
        start: expected_start,
        values: selection.values,
        next_index: selection.boundary.map(|_| next_index),
    })
}

struct LogRangeSelection {
    values: Vec<String>,
    used_bytes: usize,
    boundary: Option<String>,
}

fn collect_log_range<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    start: u64,
    limit: usize,
    max_bytes: usize,
    resolver: &mut R,
) -> Result<LogRangeSelection> {
    let mut selection = LogRangeSelection {
        values: Vec::new(),
        used_bytes: 0,
        boundary: None,
    };
    if let Some(reference) = root_ref(root)? {
        visit_log_range(
            &reference,
            0,
            start,
            limit,
            max_bytes,
            &mut selection,
            resolver,
        )?;
    }
    if selection.values.is_empty() && selection.boundary.is_some() {
        return Err(CollectionError::Validation(
            "log range byte budget cannot admit its first exact value".to_owned(),
        ));
    }
    Ok(selection)
}

fn visit_log_range<R: CollectionResolver + ?Sized>(
    reference: &NodeRef,
    base_index: u64,
    start: u64,
    limit: usize,
    max_bytes: usize,
    selection: &mut LogRangeSelection,
    resolver: &mut R,
) -> Result<()> {
    if selection.boundary.is_some() {
        return Ok(());
    }
    let end_index = base_index
        .checked_add(reference.len)
        .filter(|end| *end <= MAX_EXACT_INTEGER)
        .ok_or_else(|| CollectionError::Integrity {
            code: "log_range_index_overflow",
            message: "log range subtree end overflowed".to_owned(),
        })?;
    if end_index <= start {
        return Ok(());
    }
    let node = load_resolved_log_node(resolver, reference)?;
    match node.body {
        LogNodeBody::Leaf { values } => {
            for (offset, value) in values.into_iter().enumerate() {
                let index = base_index
                    .checked_add(u64::try_from(offset).map_err(|error| {
                        CollectionError::Validation(format!("log range offset: {error}"))
                    })?)
                    .ok_or_else(|| CollectionError::Integrity {
                        code: "log_range_index_overflow",
                        message: "log range leaf index overflowed".to_owned(),
                    })?;
                if index < start {
                    continue;
                }
                validate_content_id("authenticated-log range value", &value)?;
                let next_bytes =
                    selection
                        .used_bytes
                        .checked_add(value.len())
                        .ok_or_else(|| {
                            CollectionError::Validation(
                                "log range byte count overflowed".to_owned(),
                            )
                        })?;
                if selection.values.len() == limit || next_bytes > max_bytes {
                    selection.boundary = Some(value);
                    break;
                }
                selection.used_bytes = next_bytes;
                selection.values.push(value);
            }
        }
        LogNodeBody::Branch { left, right } => {
            visit_log_range(
                &left.clone().into(),
                base_index,
                start,
                limit,
                max_bytes,
                selection,
                resolver,
            )?;
            let right_base = base_index
                .checked_add(left.len)
                .filter(|index| *index <= MAX_EXACT_INTEGER)
                .ok_or_else(|| CollectionError::Integrity {
                    code: "log_range_index_overflow",
                    message: "log range right-child index overflowed".to_owned(),
                })?;
            visit_log_range(
                &right.into(),
                right_base,
                start,
                limit,
                max_bytes,
                selection,
                resolver,
            )?;
        }
    }
    Ok(())
}

fn validate_range_request(
    root: &LogRoot,
    start: u64,
    limit: usize,
    max_bytes: usize,
) -> Result<()> {
    root.verify()?;
    if start > root.len {
        return Err(CollectionError::Validation(format!(
            "log range start {start} exceeds length {}",
            root.len
        )));
    }
    if !(1..=MAX_PAGE_ENTRIES).contains(&limit) {
        return Err(CollectionError::Validation(format!(
            "log range limit must be within 1..={MAX_PAGE_ENTRIES}"
        )));
    }
    if !(1..=MAX_PAGE_BYTES).contains(&max_bytes) {
        return Err(CollectionError::Validation(format!(
            "log range byte budget must be within 1..={MAX_PAGE_BYTES}"
        )));
    }
    Ok(())
}

/// Raw proof of one exact ordered-log mutation batch and result root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogApplyProof {
    parent: LogRoot,
    result: LogRoot,
    mutations: Vec<LogMutation>,
    nodes: Vec<LogNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogApplyProofWire {
    parent: LogRoot,
    result: LogRoot,
    #[serde(deserialize_with = "deserialize_log_mutations")]
    mutations: Vec<LogMutation>,
    #[serde(deserialize_with = "deserialize_log_apply_nodes")]
    nodes: Vec<LogNodeWire>,
}

/// Decode one log apply proof only after enforcing the transport bound.
///
/// # Errors
///
/// Returns an error before parsing oversized input or for malformed node
/// transport. Authority is established only by [`verify_log_apply`].
pub fn decode_log_apply_proof(bytes: &[u8]) -> Result<LogApplyProof> {
    let wire = crate::decode_json_bounded::<LogApplyProofWire>(
        bytes,
        crate::MAX_PROOF_TRANSPORT_BYTES,
        "authenticated-log apply proof",
    )?;
    Ok(LogApplyProof {
        parent: wire.parent,
        result: wire.result,
        mutations: wire.mutations,
        nodes: wire
            .nodes
            .into_iter()
            .map(LogNodeWire::into_verified)
            .collect::<Result<Vec<_>>>()?,
    })
}

/// Non-serializable verified log apply result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedLogApply {
    parent: LogRoot,
    result: LogRoot,
    mutations: Vec<LogMutation>,
    mutation_digest: String,
}

impl VerifiedLogApply {
    /// Exact authenticated parent root.
    pub fn parent(&self) -> &LogRoot {
        &self.parent
    }

    /// Exact recomputed result root.
    pub fn result(&self) -> &LogRoot {
        &self.result
    }

    /// Exact ordered mutations.
    pub fn mutations(&self) -> &[LogMutation] {
        &self.mutations
    }

    /// Domain-separated identity of the exact mutation batch.
    pub fn mutation_digest(&self) -> &str {
        &self.mutation_digest
    }
}

/// Complete provider output for one verified log apply.
#[derive(Debug, Clone)]
pub struct LogApplyOutput {
    verified: VerifiedLogApply,
    proof: LogApplyProof,
    objects: Vec<LogNode>,
}

impl LogApplyOutput {
    /// Verified parent/result/mutation binding.
    pub fn verified(&self) -> &VerifiedLogApply {
        &self.verified
    }

    /// Portable independently verifiable apply proof.
    pub fn proof(&self) -> &LogApplyProof {
        &self.proof
    }

    /// Newly created immutable nodes that a provider must persist.
    pub fn objects(&self) -> &[LogNode] {
        &self.objects
    }

    /// Consume the output into its portable proof and newly created nodes.
    pub fn into_parts(self) -> (LogApplyProof, Vec<LogNode>) {
        (self.proof, self.objects)
    }
}

/// Apply one exact ordered mutation batch from an authenticated parent.
///
/// # Errors
///
/// Returns an error when an ordinal expectation fails, a node is missing or
/// invalid, or the result exceeds a collection bound.
pub fn apply_log_mutations<R: CollectionResolver + ?Sized>(
    parent: &LogRoot,
    mutations: &[LogMutation],
    resolver: &mut R,
) -> Result<LogApplyOutput> {
    parent.verify()?;
    let mutations = validate_mutations(mutations)?;
    let mut overlay = Overlay::new(resolver);
    let result = apply_log_internal(parent, &mutations, &mut overlay)?;
    let proof_nodes = overlay.loaded.clone().into_values().collect();
    let objects = collect_reachable_created_log_nodes(&result, &mut overlay)?;
    let proof = LogApplyProof {
        parent: parent.clone(),
        result: result.clone(),
        mutations: mutations.clone(),
        nodes: proof_nodes,
    };
    let verified = verify_log_apply(parent, &mutations, &proof)?;
    Ok(LogApplyOutput {
        verified,
        proof,
        objects,
    })
}

/// Verify an ordered-log apply proof by replaying the exact AVL algorithm.
///
/// # Errors
///
/// Returns an error for a stale parent, changed mutation, missing/extra node,
/// wrong length/height, or arbitrary same-length result root.
pub fn verify_log_apply(
    expected_parent: &LogRoot,
    expected_mutations: &[LogMutation],
    proof: &LogApplyProof,
) -> Result<VerifiedLogApply> {
    expected_parent.verify()?;
    let mutations = validate_mutations(expected_mutations)?;
    if &proof.parent != expected_parent || proof.mutations != mutations {
        return Err(CollectionError::Integrity {
            code: "log_apply_authority_mismatch",
            message: "log apply proof is bound to another parent or mutation batch".to_owned(),
        });
    }
    let maximum = MAX_LOG_HEIGHT
        .checked_mul(MAX_LOG_VALUES_PER_APPLY)
        .and_then(|nodes| nodes.checked_mul(4))
        .ok_or_else(|| CollectionError::Validation("log proof bound overflowed".to_owned()))?;
    let mut resolver = ApplyProofLogResolver::new(&proof.nodes, maximum)?;
    let mut overlay = Overlay::new(&mut resolver);
    let result = apply_log_internal(expected_parent, &mutations, &mut overlay)?;
    if result != proof.result {
        return Err(CollectionError::Integrity {
            code: "log_apply_result_mismatch",
            message: "log apply proof result does not match exact replay".to_owned(),
        });
    }
    let loaded: BTreeSet<String> = overlay.loaded.keys().cloned().collect();
    resolver.finish(&loaded)?;
    Ok(VerifiedLogApply {
        parent: expected_parent.clone(),
        result,
        mutation_digest: mutation_digest(&mutations),
        mutations,
    })
}

fn collect_reachable_created_log_nodes<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    overlay: &mut Overlay<'_, R>,
) -> Result<Vec<LogNode>> {
    collect_reachable_created_log_nodes_for_roots(&[root], overlay)
}

fn collect_reachable_created_log_nodes_for_roots<R: CollectionResolver + ?Sized>(
    roots: &[&LogRoot],
    overlay: &mut Overlay<'_, R>,
) -> Result<Vec<LogNode>> {
    let mut stack = Vec::new();
    for root in roots {
        if let Some(reference) = root_ref(root)? {
            stack.push(reference);
        }
    }
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
        if node.value_count()? != reference.len || node.height()? != reference.height {
            return Err(CollectionError::Integrity {
                code: "log_created_child_shape_mismatch",
                message: format!(
                    "created log node {} contradicts the final reachable shape",
                    reference.object_id
                ),
            });
        }
        if overlay.loaded.contains_key(&node.object_id) {
            continue;
        }
        match overlay.resolver.load_log_node(&node.object_id)? {
            Some(existing) => {
                existing.verify()?;
                if existing != node {
                    return Err(CollectionError::Integrity {
                        code: "log_existing_node_identity_conflict",
                        message: format!(
                            "existing log node {} has conflicting bytes",
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
        if let LogNodeBody::Branch { left, right } = node.body {
            stack.push(right.into());
            stack.push(left.into());
        }
    }
    Ok(created.into_values().collect())
}

/// Complete O(total values) verification of one ordered-log root.
#[derive(Debug, Clone)]
pub struct LogAudit {
    values: Vec<String>,
    objects: Vec<LogNode>,
}

impl LogAudit {
    /// Every opaque identity in exact ordinal order. Repeats are preserved.
    pub fn values(&self) -> &[String] {
        &self.values
    }

    /// Complete unique reachable immutable node set.
    pub fn objects(&self) -> &[LogNode] {
        &self.objects
    }
}

/// Verify and materialize one complete ordered-log closure.
///
/// This explicit O(total values) operation is for genesis, restore audit, and
/// offline repair only. Exact reads and ranges never call it.
///
/// # Errors
///
/// Returns an error for a missing node, invalid length/height/balance, or a
/// root-closure count mismatch.
pub fn audit_log<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    resolver: &mut R,
) -> Result<LogAudit> {
    root.verify()?;
    let Some(reference) = root_ref(root)? else {
        return Ok(LogAudit {
            values: Vec::new(),
            objects: Vec::new(),
        });
    };
    let mut stack = vec![reference];
    let mut values = Vec::new();
    let mut unique_nodes = BTreeMap::new();
    while let Some(reference) = stack.pop() {
        let node = load_resolved_log_node(resolver, &reference)?;
        match &node.body {
            LogNodeBody::Leaf {
                values: leaf_values,
            } => values.extend(leaf_values.iter().cloned()),
            LogNodeBody::Branch { left, right } => {
                stack.push(right.clone().into());
                stack.push(left.clone().into());
            }
        }
        unique_nodes
            .entry(node.object_id.clone())
            .or_insert_with(|| node.clone());
    }
    if u64::try_from(values.len()).ok() != Some(root.len) {
        return Err(CollectionError::Integrity {
            code: "log_audit_count_mismatch",
            message: "ordered-log closure does not match its root length".to_owned(),
        });
    }
    Ok(LogAudit {
        values,
        objects: unique_nodes.into_values().collect(),
    })
}

/// Complete output of an explicit full ordered-log build.
#[derive(Debug, Clone)]
pub struct LogBuildOutput {
    root: LogRoot,
    objects: Vec<LogNode>,
}

impl LogBuildOutput {
    /// Exact rebuilt root.
    pub fn root(&self) -> &LogRoot {
        &self.root
    }

    /// Complete reachable immutable node set for the rebuilt root.
    pub fn objects(&self) -> &[LogNode] {
        &self.objects
    }

    /// Consume the build into its root and complete reachable nodes.
    pub fn into_parts(self) -> (LogRoot, Vec<LogNode>) {
        (self.root, self.objects)
    }
}

/// Build a log from one explicit complete ordered genesis sequence.
///
/// Repeated identities, including adjacent repeats, are preserved.
/// This explicit audit/genesis operation replays the canonical append order and
/// is never used by an ordinary open or exact read.
///
/// # Errors
///
/// Returns an error for an empty sequence, invalid identity, or invalid node.
pub fn build_log(values: &[String]) -> Result<LogBuildOutput> {
    let value_count = u64::try_from(values.len())
        .map_err(|error| CollectionError::Validation(error.to_string()))?;
    if value_count > MAX_EXACT_INTEGER {
        return Err(CollectionError::Validation(
            "full log build exceeds the exact length range".to_owned(),
        ));
    }
    if values.is_empty() {
        return Ok(LogBuildOutput {
            root: LogRoot::empty(),
            objects: Vec::new(),
        });
    }
    for value in values {
        validate_content_id("full log value", value)?;
    }
    let mut objects = BTreeMap::new();
    let leaves = build_append_log_leaves(values, &mut objects)?;
    let reference = build_append_log_tree(&leaves, &mut objects)?;
    let root = ref_root(Some(reference))?;
    if root.len != value_count {
        return Err(CollectionError::Integrity {
            code: "log_build_count_mismatch",
            message: "bottom-up log build produced the wrong value count".to_owned(),
        });
    }
    Ok(LogBuildOutput {
        root,
        objects: objects.into_values().collect(),
    })
}

fn build_append_log_leaves(
    values: &[String],
    objects: &mut BTreeMap<String, LogNode>,
) -> Result<Vec<NodeRef>> {
    let mut leaves = Vec::new();
    let mut start = 0;
    while values.len() - start > LOG_FANOUT {
        let full_end = start + LOG_FANOUT;
        leaves.push(store_built_log_node(
            LogNode::leaf(values[start..full_end].to_vec())?,
            objects,
        )?);
        let singleton_end = full_end + 1;
        leaves.push(store_built_log_node(
            LogNode::leaf(values[full_end..singleton_end].to_vec())?,
            objects,
        )?);
        start = singleton_end;
    }
    if start < values.len() {
        leaves.push(store_built_log_node(
            LogNode::leaf(values[start..].to_vec())?,
            objects,
        )?);
    }
    Ok(leaves)
}

fn build_append_log_tree(
    leaves: &[NodeRef],
    objects: &mut BTreeMap<String, LogNode>,
) -> Result<NodeRef> {
    let leaf = leaves.first().ok_or_else(|| CollectionError::Integrity {
        code: "log_build_empty_subtree",
        message: "bottom-up log build received an empty subtree".to_owned(),
    })?;
    if leaves.len() == 1 {
        return Ok(leaf.clone());
    }
    let left_len = append_log_tree_left_len(leaves.len());
    let left = build_append_log_tree(&leaves[..left_len], objects)?;
    let right = build_append_log_tree(&leaves[left_len..], objects)?;
    store_built_log_node(
        LogNode::branch(left.into_child(), right.into_child())?,
        objects,
    )
}

fn append_log_tree_left_len(leaves: usize) -> usize {
    debug_assert!(leaves > 1);
    let power = 1_usize << (usize::BITS - 1 - leaves.leading_zeros());
    if leaves == power {
        return power / 2;
    }
    if leaves == 3 {
        return 2;
    }
    let lower_half = power / 2;
    if leaves <= power + lower_half {
        lower_half
    } else {
        power
    }
}

fn store_built_log_node(node: LogNode, objects: &mut BTreeMap<String, LogNode>) -> Result<NodeRef> {
    node.verify()?;
    let object_id = node.object_id.clone();
    let reference = NodeRef {
        object_id: object_id.clone(),
        len: node.value_count()?,
        height: node.height()?,
    };
    match objects.entry(object_id.clone()) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(node);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &node => {
            return Err(CollectionError::Integrity {
                code: "log_node_identity_conflict",
                message: format!("log node {object_id} has conflicting bytes"),
            });
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    Ok(reference)
}

fn apply_log_internal<R: CollectionResolver + ?Sized>(
    parent: &LogRoot,
    mutations: &[LogMutation],
    overlay: &mut Overlay<'_, R>,
) -> Result<LogRoot> {
    let mut root = parent.clone();
    for mutation in mutations {
        match mutation {
            LogMutation::Append { values } => {
                root = append_values(&root, values, overlay)?;
            }
            LogMutation::InsertAt { index, value } => {
                if *index > root.len {
                    return Err(CollectionError::Conflict(format!(
                        "log insertion ordinal {index} exceeds length {}",
                        root.len
                    )));
                }
                let (left, right) = split_root(&root, *index, overlay)?;
                let inserted = ref_root(Some(overlay.store(LogNode::leaf(vec![value.clone()])?)?))?;
                let prefix = concat_roots(&left, &inserted, overlay)?;
                root = concat_roots(&prefix, &right, overlay)?;
            }
            LogMutation::ReplaceAt {
                index,
                expected,
                value,
            } => {
                let current = overlay_get(&root, *index, overlay)?;
                if current != *expected {
                    return Err(CollectionError::Conflict(format!(
                        "log ordinal {index} does not match its expected value"
                    )));
                }
                let (left, remainder) = split_root(&root, *index, overlay)?;
                let (_, right) = split_root(&remainder, 1, overlay)?;
                let replacement =
                    ref_root(Some(overlay.store(LogNode::leaf(vec![value.clone()])?)?))?;
                let prefix = concat_roots(&left, &replacement, overlay)?;
                root = concat_roots(&prefix, &right, overlay)?;
            }
            LogMutation::ReplacePrefix {
                count,
                expected_prefix,
                replacement,
            } => {
                if *count > root.len {
                    return Err(CollectionError::Conflict(format!(
                        "log prefix length {count} exceeds length {}",
                        root.len
                    )));
                }
                let (prefix, suffix) = split_root(&root, *count, overlay)?;
                if prefix != *expected_prefix {
                    return Err(CollectionError::Conflict(
                        "log prefix does not match its expected authenticated root".to_owned(),
                    ));
                }
                let replacement_root = append_values(&LogRoot::empty(), replacement, overlay)?;
                if replacement_root == prefix {
                    return Err(CollectionError::Conflict(
                        "log prefix replacement does not change the authenticated prefix"
                            .to_owned(),
                    ));
                }
                root = concat_roots(&replacement_root, &suffix, overlay)?;
            }
            LogMutation::RemoveAt { index, expected } => {
                let current = overlay_get(&root, *index, overlay)?;
                if current != *expected {
                    return Err(CollectionError::Conflict(format!(
                        "log ordinal {index} does not match its expected value"
                    )));
                }
                let (left, remainder) = split_root(&root, *index, overlay)?;
                let (_, right) = split_root(&remainder, 1, overlay)?;
                root = concat_roots(&left, &right, overlay)?;
            }
        }
    }
    root.verify()?;
    Ok(root)
}

fn overlay_get<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    mut index: u64,
    overlay: &mut Overlay<'_, R>,
) -> Result<String> {
    if index >= root.len {
        return Err(CollectionError::Conflict(format!(
            "log ordinal {index} exceeds length {}",
            root.len
        )));
    }
    let mut reference = root_ref(root)?.ok_or_else(|| {
        CollectionError::Conflict("empty log has no removable ordinal".to_owned())
    })?;
    for _ in 0..=MAX_LOG_HEIGHT {
        let node = overlay.load(&reference)?;
        match node.body {
            LogNodeBody::Leaf { values } => {
                return values
                    .get(
                        usize::try_from(index)
                            .map_err(|error| CollectionError::Validation(error.to_string()))?,
                    )
                    .cloned()
                    .ok_or_else(|| CollectionError::Integrity {
                        code: "log_leaf_index_mismatch",
                        message: "log leaf does not contain routed ordinal".to_owned(),
                    });
            }
            LogNodeBody::Branch { left, right } => {
                if index < left.len {
                    reference = left.into();
                } else {
                    index -= left.len;
                    reference = right.into();
                }
            }
        }
    }
    Err(CollectionError::Integrity {
        code: "log_path_bound_exceeded",
        message: "log lookup exceeds the AVL height bound".to_owned(),
    })
}

fn append_values<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    values: &[String],
    overlay: &mut Overlay<'_, R>,
) -> Result<LogRoot> {
    let mut next = root.clone();
    for value in values {
        let leaf = overlay.store(LogNode::leaf(vec![value.clone()])?)?;
        next = concat_roots(&next, &ref_root(Some(leaf))?, overlay)?;
    }
    Ok(next)
}

fn concat_roots<R: CollectionResolver + ?Sized>(
    left: &LogRoot,
    right: &LogRoot,
    overlay: &mut Overlay<'_, R>,
) -> Result<LogRoot> {
    match (root_ref(left)?, root_ref(right)?) {
        (None, value) | (value, None) => ref_root(value),
        (Some(left), Some(right)) => ref_root(Some(join_refs(left, right, overlay)?)),
    }
}

fn join_refs<R: CollectionResolver + ?Sized>(
    left: NodeRef,
    right: NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    let right_limit = right
        .height
        .checked_add(1)
        .ok_or_else(|| CollectionError::Validation("log right height overflowed".to_owned()))?;
    if left.height > right_limit {
        let (left_left, left_right) = branch_children(&left, overlay)?;
        let joined = join_refs(left_right, right, overlay)?;
        return balance_refs(left_left, joined, overlay);
    }
    let left_limit = left
        .height
        .checked_add(1)
        .ok_or_else(|| CollectionError::Validation("log left height overflowed".to_owned()))?;
    if right.height > left_limit {
        let (right_left, right_right) = branch_children(&right, overlay)?;
        let joined = join_refs(left, right_left, overlay)?;
        return balance_refs(joined, right_right, overlay);
    }
    if left.height == 1 && right.height == 1 {
        let left_node = overlay.load(&left)?;
        let right_node = overlay.load(&right)?;
        if let (
            LogNodeBody::Leaf {
                values: mut left_values,
            },
            LogNodeBody::Leaf {
                values: right_values,
            },
        ) = (left_node.body, right_node.body)
            && left_values
                .len()
                .checked_add(right_values.len())
                .is_some_and(|len| len <= LOG_FANOUT)
        {
            left_values.extend(right_values);
            return overlay.store(LogNode::leaf(left_values)?);
        }
    }
    make_branch(left, right, overlay)
}

fn branch_children<R: CollectionResolver + ?Sized>(
    reference: &NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<(NodeRef, NodeRef)> {
    match overlay.load(reference)?.body {
        LogNodeBody::Branch { left, right } => Ok((left.into(), right.into())),
        LogNodeBody::Leaf { .. } => Err(CollectionError::Integrity {
            code: "log_expected_branch",
            message: "AVL operation expected a branch".to_owned(),
        }),
    }
}

fn make_branch<R: CollectionResolver + ?Sized>(
    left: NodeRef,
    right: NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    if left.height.abs_diff(right.height) > 1 {
        return Err(CollectionError::Integrity {
            code: "log_unbalanced_branch_construction",
            message: "AVL branch construction is unbalanced".to_owned(),
        });
    }
    overlay.store(LogNode::branch(left.into_child(), right.into_child())?)
}

fn balance_refs<R: CollectionResolver + ?Sized>(
    left: NodeRef,
    right: NodeRef,
    overlay: &mut Overlay<'_, R>,
) -> Result<NodeRef> {
    if left.height.abs_diff(right.height) <= 1 {
        return make_branch(left, right, overlay);
    }
    if left.height > right.height {
        let (left_left, left_right) = branch_children(&left, overlay)?;
        if left_left.height >= left_right.height {
            let rotated_right = make_branch(left_right, right, overlay)?;
            return make_branch(left_left, rotated_right, overlay);
        }
        let (middle_left, middle_right) = branch_children(&left_right, overlay)?;
        let rotated_left = make_branch(left_left, middle_left, overlay)?;
        let rotated_right = make_branch(middle_right, right, overlay)?;
        return make_branch(rotated_left, rotated_right, overlay);
    }
    let (right_left, right_right) = branch_children(&right, overlay)?;
    if right_right.height >= right_left.height {
        let rotated_left = make_branch(left, right_left, overlay)?;
        return make_branch(rotated_left, right_right, overlay);
    }
    let (middle_left, middle_right) = branch_children(&right_left, overlay)?;
    let rotated_left = make_branch(left, middle_left, overlay)?;
    let rotated_right = make_branch(middle_right, right_right, overlay)?;
    make_branch(rotated_left, rotated_right, overlay)
}

fn split_root<R: CollectionResolver + ?Sized>(
    root: &LogRoot,
    index: u64,
    overlay: &mut Overlay<'_, R>,
) -> Result<(LogRoot, LogRoot)> {
    if index > root.len {
        return Err(CollectionError::Validation(format!(
            "log split {index} exceeds length {}",
            root.len
        )));
    }
    let Some(reference) = root_ref(root)? else {
        return Ok((LogRoot::empty(), LogRoot::empty()));
    };
    let (left, right) = split_ref(reference, index, overlay)?;
    Ok((ref_root(left)?, ref_root(right)?))
}

fn split_ref<R: CollectionResolver + ?Sized>(
    reference: NodeRef,
    index: u64,
    overlay: &mut Overlay<'_, R>,
) -> Result<(Option<NodeRef>, Option<NodeRef>)> {
    if index == 0 {
        return Ok((None, Some(reference)));
    }
    if index == reference.len {
        return Ok((Some(reference), None));
    }
    if index > reference.len {
        return Err(CollectionError::Integrity {
            code: "log_split_range_mismatch",
            message: "log split exceeds its node".to_owned(),
        });
    }
    match overlay.load(&reference)?.body {
        LogNodeBody::Leaf { values } => {
            let index = usize::try_from(index)
                .map_err(|error| CollectionError::Validation(error.to_string()))?;
            let left = if index == 0 {
                None
            } else {
                Some(overlay.store(LogNode::leaf(values[..index].to_vec())?)?)
            };
            let right = if index == values.len() {
                None
            } else {
                Some(overlay.store(LogNode::leaf(values[index..].to_vec())?)?)
            };
            Ok((left, right))
        }
        LogNodeBody::Branch { left, right } => {
            let left: NodeRef = left.into();
            let right: NodeRef = right.into();
            match index.cmp(&left.len) {
                Ordering::Less => {
                    let (prefix, remainder) = split_ref(left, index, overlay)?;
                    let suffix = concat_optional(remainder, Some(right), overlay)?;
                    Ok((prefix, suffix))
                }
                Ordering::Equal => Ok((Some(left), Some(right))),
                Ordering::Greater => {
                    let offset =
                        index
                            .checked_sub(left.len)
                            .ok_or_else(|| CollectionError::Integrity {
                                code: "log_split_index_underflow",
                                message: "log split offset underflowed".to_owned(),
                            })?;
                    let (prefix_tail, suffix) = split_ref(right, offset, overlay)?;
                    let prefix = concat_optional(Some(left), prefix_tail, overlay)?;
                    Ok((prefix, suffix))
                }
            }
        }
    }
}

fn concat_optional<R: CollectionResolver + ?Sized>(
    left: Option<NodeRef>,
    right: Option<NodeRef>,
    overlay: &mut Overlay<'_, R>,
) -> Result<Option<NodeRef>> {
    match (left, right) {
        (None, value) | (value, None) => Ok(value),
        (Some(left), Some(right)) => join_refs(left, right, overlay).map(Some),
    }
}

struct ApplyProofLogResolver {
    nodes: BTreeMap<String, LogNode>,
    requested: BTreeSet<String>,
}

impl ApplyProofLogResolver {
    fn new(nodes: &[LogNode], maximum: usize) -> Result<Self> {
        if nodes.len() > maximum {
            return Err(CollectionError::Validation(
                "log apply proof exceeds its node bound".to_owned(),
            ));
        }
        verify_log_proof_bytes(nodes)?;
        let mut indexed = BTreeMap::new();
        for node in nodes {
            node.verify()?;
            if indexed
                .insert(node.object_id.clone(), node.clone())
                .is_some()
            {
                return Err(CollectionError::Integrity {
                    code: "log_apply_duplicate_node",
                    message: format!("log apply proof repeats node {}", node.object_id),
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
                code: "log_apply_node_closure_mismatch",
                message: "log apply proof has missing, unused, or substituted nodes".to_owned(),
            });
        }
        Ok(())
    }
}

impl CollectionResolver for ApplyProofLogResolver {
    fn load_map_node(&mut self, _object_id: &str) -> Result<Option<crate::MapNode>> {
        Ok(None)
    }

    fn load_log_node(&mut self, object_id: &str) -> Result<Option<LogNode>> {
        self.requested.insert(object_id.to_owned());
        Ok(self.nodes.get(object_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Memory {
        logs: BTreeMap<String, LogNode>,
        log_loads: usize,
    }

    impl CollectionResolver for Memory {
        fn load_map_node(&mut self, _object_id: &str) -> Result<Option<crate::MapNode>> {
            Ok(None)
        }

        fn load_log_node(&mut self, object_id: &str) -> Result<Option<LogNode>> {
            self.log_loads = self
                .log_loads
                .checked_add(1)
                .expect("test load counter remains bounded");
            Ok(self.logs.get(object_id).cloned())
        }
    }

    fn absorb_apply(memory: &mut Memory, output: &LogApplyOutput) {
        for node in output.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
    }

    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        *state
    }

    fn value(label: &str) -> String {
        hash_identifier("test-value/1", &[label.as_bytes()])
    }

    fn journal_fixture(count: usize) -> (Vec<String>, LogRoot, Memory) {
        let values: Vec<String> = (0..count)
            .map(|index| value(&format!("journal-{}", index % 127)))
            .collect();
        let built = build_log(&values).expect("large journal builds");
        let root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        (values, root, memory)
    }

    fn range_loads(root: &LogRoot, memory: &mut Memory, start: u64, limit: usize) -> usize {
        memory.log_loads = 0;
        let proof = prove_log_range(root, start, limit, MAX_PAGE_BYTES, memory)
            .expect("range proof generates");
        let loads = memory.log_loads;
        assert_eq!(loads, proof.nodes.len());
        let range = verify_log_range(root, start, limit, MAX_PAGE_BYTES, &proof)
            .expect("range proof verifies");
        assert_eq!(range.values().len(), limit);
        loads
    }

    #[test]
    fn ordered_log_preserves_adjacent_and_nonadjacent_duplicates() {
        let a = value("a");
        let b = value("b");
        let output = build_log(&[a.clone(), a.clone(), b.clone(), a.clone()])
            .expect("duplicates are valid ordered values");
        let root = output.root().clone();
        let mut memory = Memory::default();
        for node in output.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let range = prove_log_range(&root, 0, 8, MAX_PAGE_BYTES, &mut memory).expect("range");
        let verified = verify_log_range(&root, 0, 8, MAX_PAGE_BYTES, &range).expect("verified");
        assert_eq!(verified.values(), &[a.clone(), a, b, value("a")]);
    }

    #[test]
    fn range_rejects_skip_reorder_substitute_and_false_terminal() {
        let values: Vec<String> = (0..5).map(|index| value(&index.to_string())).collect();
        let output = build_log(&values).expect("log builds");
        let root = output.root().clone();
        let mut memory = Memory::default();
        for node in output.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let proof = prove_log_range(&root, 0, 2, MAX_PAGE_BYTES, &mut memory).expect("range");
        verify_log_range(&root, 0, 2, MAX_PAGE_BYTES, &proof).expect("verified");

        let mut skipped = proof.clone();
        skipped.nodes.pop();
        assert!(verify_log_range(&root, 0, 2, MAX_PAGE_BYTES, &skipped).is_err());

        let mut reordered = serde_json::to_value(&proof).expect("proof encodes");
        reordered["entries"] = serde_json::json!(["second", "first"]);
        assert!(
            decode_log_range_proof(
                &serde_json::to_vec(&reordered).expect("reordered wire encodes")
            )
            .is_err()
        );

        let mut substituted = proof.clone();
        let leaf = substituted
            .nodes
            .iter_mut()
            .find(|node| matches!(node.body, LogNodeBody::Leaf { .. }))
            .expect("range proof contains a leaf");
        if let LogNodeBody::Leaf { values } = &mut leaf.body {
            values[0] = value("wrong");
        }
        assert!(verify_log_range(&root, 0, 2, MAX_PAGE_BYTES, &substituted).is_err());

        let mut false_terminal = serde_json::to_value(&proof).expect("proof encodes");
        false_terminal["boundary"] = serde_json::Value::Null;
        assert!(
            decode_log_range_proof(
                &serde_json::to_vec(&false_terminal).expect("false terminal wire encodes")
            )
            .is_err()
        );

        let proof = prove_log_range(&root, 0, 2, MAX_PAGE_BYTES, &mut memory).expect("range");
        let zero_node = proof.nodes[0].clone();
        let mut oversized = proof;
        oversized.nodes = vec![zero_node; MAX_LOG_RANGE_PROOF_NODES + 1];
        let error = verify_log_range(&root, 0, 2, MAX_PAGE_BYTES, &oversized)
            .expect_err("node bound is checked before proof traversal");
        assert!(matches!(
            error, CollectionError::Validation(message) if message.contains("node bound")
        ));
    }

    #[test]
    fn apply_rejects_stale_parent_and_same_length_arbitrary_root() {
        let initial = build_log(&[value("a"), value("b")]).expect("initial");
        let root = initial.root().clone();
        let mut memory = Memory::default();
        for node in initial.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let mutation = LogMutation::append(vec![value("c")]);
        let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
            .expect("apply");
        verify_log_apply(&root, std::slice::from_ref(&mutation), output.proof())
            .expect("verified apply");
        assert!(
            verify_log_apply(
                &LogRoot::empty(),
                std::slice::from_ref(&mutation),
                output.proof()
            )
            .is_err()
        );

        let mut arbitrary = output.proof().clone();
        arbitrary.result = build_log(&[value("x"), value("y"), value("z")])
            .expect("same length")
            .root()
            .clone();
        assert!(verify_log_apply(&root, &[mutation], &arbitrary).is_err());
    }

    #[test]
    fn cross_leaf_append_and_first_middle_last_remove_match_vec_model() {
        let mut model: Vec<String> = (0..70).map(|index| value(&format!("v-{index}"))).collect();
        let built = build_log(&model).expect("multi-leaf log builds");
        let mut root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        for position in 0..3 {
            let index = match position {
                0 => 0,
                1 => model.len() / 2,
                _ => model.len() - 1,
            };
            let expected = model[index].clone();
            let mutation = LogMutation::remove_at(
                u64::try_from(index).expect("test index fits u64"),
                expected,
            );
            let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("remove applies");
            verify_log_apply(&root, std::slice::from_ref(&mutation), output.proof())
                .expect("remove proof verifies");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();
            model.remove(index);

            let proof = prove_log_range(&root, 0, MAX_PAGE_ENTRIES, MAX_PAGE_BYTES, &mut memory)
                .expect("range proof");
            assert_eq!(
                verify_log_range(&root, 0, MAX_PAGE_ENTRIES, MAX_PAGE_BYTES, &proof)
                    .expect("range verifies")
                    .values(),
                model
            );
        }
    }

    #[test]
    fn long_log_sequence_matches_vec_model_and_every_proof() {
        let mut root = LogRoot::empty();
        let mut memory = Memory::default();
        let mut model = Vec::<String>::new();
        let mut random = 19_u64;
        for step in 0..500_u64 {
            let random_value = next_random(&mut random);
            let mutation = match random_value % 8 {
                0 if !model.is_empty() => {
                    let index = usize::try_from(random_value).expect("test random fits usize")
                        % model.len();
                    let expected = model.remove(index);
                    LogMutation::remove_at(
                        u64::try_from(index).expect("test index fits u64"),
                        expected,
                    )
                }
                1 => {
                    let divisor = model.len().checked_add(1).expect("test length increments");
                    let index =
                        usize::try_from(random_value).expect("test random fits usize") % divisor;
                    let next = value(&format!("insert-{step}-{}", random_value % 11));
                    model.insert(index, next.clone());
                    LogMutation::insert_at(u64::try_from(index).expect("test index fits u64"), next)
                }
                2 if !model.is_empty() => {
                    let index = usize::try_from(random_value).expect("test random fits usize")
                        % model.len();
                    let expected = model[index].clone();
                    let next = value(&format!("replace-{step}-{}", random_value % 11));
                    model[index] = next.clone();
                    LogMutation::replace_at(
                        u64::try_from(index).expect("test index fits u64"),
                        expected,
                        next,
                    )
                }
                _ => {
                    let next = value(&format!("repeat-{}", random_value % 11));
                    model.push(next.clone());
                    LogMutation::append(vec![next])
                }
            };
            let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("model mutation applies");
            verify_log_apply(&root, std::slice::from_ref(&mutation), output.proof())
                .expect("every apply proof verifies");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();

            if !model.is_empty() {
                let index =
                    usize::try_from(random_value).expect("test random fits usize") % model.len();
                let exact = prove_log_exact(
                    &root,
                    u64::try_from(index).expect("test index fits u64"),
                    &mut memory,
                )
                .expect("exact proof");
                assert_eq!(
                    verify_log_exact(
                        &root,
                        u64::try_from(index).expect("test index fits u64"),
                        &exact
                    )
                    .expect("exact verifies")
                    .value(),
                    model[index].as_str()
                );
            }
            if step % 47 == 0 {
                let mut start = 0_u64;
                let mut actual = Vec::new();
                loop {
                    let proof = prove_log_range(&root, start, 9, MAX_PAGE_BYTES, &mut memory)
                        .expect("range proof");
                    let range = verify_log_range(&root, start, 9, MAX_PAGE_BYTES, &proof)
                        .expect("range verifies");
                    actual.extend_from_slice(range.values());
                    let Some(next) = range.next_index() else {
                        break;
                    };
                    start = next;
                }
                assert_eq!(actual, model);
            }
        }
    }

    #[test]
    fn append_only_full_build_matches_streaming_apply_root() {
        let values: Vec<String> = (0..600)
            .map(|index| value(&format!("lineage-{}", index % 23)))
            .collect();
        assert_eq!(
            build_log(&[]).expect("empty build").root(),
            &LogRoot::empty()
        );
        let mut memory = Memory::default();
        let mut root = LogRoot::empty();
        for (index, next) in values.iter().enumerate() {
            let mutation = LogMutation::append(vec![next.clone()]);
            let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("streaming append");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();
            assert_eq!(
                build_log(&values[..=index])
                    .expect("bottom-up append-only build")
                    .root(),
                &root,
                "bottom-up shape diverged after {} appends",
                index + 1
            );
        }
    }

    #[test]
    fn large_bottom_up_build_keeps_only_reachable_linear_nodes() {
        let values: Vec<String> = (0..10_000)
            .map(|index| value(&format!("large-lineage-{}", index % 101)))
            .collect();
        let built = build_log(&values).expect("large bottom-up log builds");
        let full_pairs = values.len() / (LOG_FANOUT + 1);
        let remainder = values.len() % (LOG_FANOUT + 1);
        let leaves = (2 * full_pairs) + usize::from(remainder != 0);
        let maximum_reachable_nodes = (2 * leaves) - 1;
        assert!(built.objects().len() <= maximum_reachable_nodes);

        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let audited = audit_log(built.root(), &mut memory).expect("large root audits");
        assert_eq!(audited.values(), values);
        assert_eq!(audited.objects().len(), built.objects().len());
    }

    #[test]
    fn log_proofs_reject_missing_extra_and_corrupt_height_nodes() {
        let values: Vec<String> = (0..80).map(|index| value(&index.to_string())).collect();
        let built = build_log(&values).expect("log builds");
        let root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let proof = prove_log_exact(&root, 57, &mut memory).expect("proof");
        let mut missing = proof.clone();
        missing.nodes.pop();
        assert!(verify_log_exact(&root, 57, &missing).is_err());

        let mut extra = proof.clone();
        let unrelated = prove_log_exact(&root, 0, &mut memory).expect("unrelated proof");
        if let Some(node) = unrelated.nodes.into_iter().find(|node| {
            !extra
                .nodes
                .iter()
                .any(|existing| existing.object_id == node.object_id)
        }) {
            extra.nodes.push(node);
            assert!(verify_log_exact(&root, 57, &extra).is_err());
        }

        let mut corrupt = proof;
        let branch = corrupt
            .nodes
            .iter_mut()
            .find(|node| matches!(node.body, LogNodeBody::Branch { .. }))
            .expect("proof has branch");
        if let LogNodeBody::Branch { left, .. } = &mut branch.body {
            left.height = left.height.checked_add(1).expect("test height increments");
        }
        assert!(verify_log_exact(&root, 57, &corrupt).is_err());

        let mut wrong_count = prove_log_exact(&root, 57, &mut memory).expect("proof");
        let root_id = wrong_count.root.node.clone().expect("nonempty root");
        let root_node = wrong_count
            .nodes
            .iter_mut()
            .find(|node| node.object_id == root_id)
            .expect("proof contains root");
        if let LogNodeBody::Branch { left, right } = &mut root_node.body {
            left.len = left.len.checked_add(1).expect("test count increments");
            right.len = right.len.checked_add(1).expect("test count increments");
        } else {
            panic!("fixture root is a branch");
        }
        root_node.object_id = log_node_id(&root_node.body);
        wrong_count.root.node = Some(root_node.object_id.clone());
        wrong_count.root.ordered_root = root_node.object_id.clone();
        wrong_count.root.len = root_node
            .value_count()
            .expect("forged local count validates");
        wrong_count.root.height = root_node.height().expect("forged local height validates");
        assert!(verify_log_exact(&wrong_count.root, 57, &wrong_count).is_err());

        let source_branch = built
            .objects()
            .iter()
            .find_map(|node| match &node.body {
                LogNodeBody::Branch { left, right } => Some((left.clone(), right.clone())),
                LogNodeBody::Leaf { .. } => None,
            })
            .expect("built log has a branch");
        let mut left = source_branch.0;
        let mut right = source_branch.1;
        left.height = 1;
        right.height = 3;
        assert!(LogNode::from_body(LogNodeBody::Branch { left, right }).is_err());

        let missing = value("missing-audit-root");
        let huge_untrusted_root = LogRoot {
            node: Some(missing.clone()),
            len: MAX_EXACT_INTEGER,
            height: 1,
            ordered_root: missing,
        };
        let mut empty_memory = Memory::default();
        assert!(matches!(
            audit_log(&huge_untrusted_root, &mut empty_memory),
            Err(CollectionError::MissingObject(_))
        ));
    }

    #[test]
    fn log_apply_enforces_primitive_count_before_resolution() {
        let maximum: Vec<String> = (0..MAX_LOG_VALUES_PER_APPLY)
            .map(|index| value(&format!("max-{index}")))
            .collect();
        let mut maximum_memory = Memory::default();
        apply_log_mutations(
            &LogRoot::empty(),
            &[LogMutation::append(maximum)],
            &mut maximum_memory,
        )
        .expect("exact primitive-count maximum applies");

        let values: Vec<String> = (0..=MAX_LOG_VALUES_PER_APPLY)
            .map(|index| value(&index.to_string()))
            .collect();
        let mut memory = Memory::default();
        assert!(
            apply_log_mutations(
                &LogRoot::empty(),
                &[LogMutation::append(values)],
                &mut memory
            )
            .is_err()
        );
        assert!(memory.logs.is_empty());
    }

    #[test]
    fn insert_and_replace_cover_first_middle_end_and_fail_closed() {
        let mut model: Vec<String> = (0..70).map(|index| value(&format!("v-{index}"))).collect();
        let built = build_log(&model).expect("parent log builds");
        let mut root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }

        for (step, index) in [0, model.len() / 2, model.len()].into_iter().enumerate() {
            let inserted = value(&format!("insert-{step}"));
            let mutation = LogMutation::insert_at(
                u64::try_from(index).expect("test index fits u64"),
                inserted.clone(),
            );
            let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("insert applies");
            verify_log_apply(&root, std::slice::from_ref(&mutation), output.proof())
                .expect("insert proof verifies");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();
            model.insert(index, inserted);
        }

        for (step, index) in [0, model.len() / 2, model.len() - 1]
            .into_iter()
            .enumerate()
        {
            let expected = model[index].clone();
            let replacement = value(&format!("replacement-{step}"));
            let mutation = LogMutation::replace_at(
                u64::try_from(index).expect("test index fits u64"),
                expected,
                replacement.clone(),
            );
            let output = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
                .expect("replacement applies");
            verify_log_apply(&root, std::slice::from_ref(&mutation), output.proof())
                .expect("replacement proof verifies");
            absorb_apply(&mut memory, &output);
            root = output.verified().result().clone();
            model[index] = replacement;
        }

        assert_eq!(
            audit_log(&root, &mut memory)
                .expect("result audits")
                .values(),
            model
        );
        let mut untouched = Memory {
            logs: memory.logs.clone(),
            log_loads: 0,
        };
        assert!(
            apply_log_mutations(
                &root,
                &[LogMutation::insert_at(root.len + 1, value("out-of-range"))],
                &mut untouched,
            )
            .is_err()
        );
        assert!(
            apply_log_mutations(
                &root,
                &[LogMutation::replace_at(0, value("wrong"), value("next"))],
                &mut untouched,
            )
            .is_err()
        );
        assert!(
            apply_log_mutations(
                &root,
                &[LogMutation::replace_at(
                    0,
                    model[0].clone(),
                    model[0].clone()
                )],
                &mut untouched,
            )
            .is_err()
        );
    }

    #[test]
    fn split_and_large_prefix_replacement_are_authenticated_and_replayable() {
        let (values, parent, mut memory) = journal_fixture(10_000);

        let split = split_log(&parent, 9_000, &mut memory).expect("large prefix splits");
        let verified = verify_log_split(&parent, 9_000, split.proof()).expect("split verifies");
        assert_eq!(verified.prefix().len, 9_000);
        assert_eq!(verified.suffix().len, 1_000);
        assert_eq!(
            verify_log_split(&parent, 9_000, split.proof()).expect("lost response replays"),
            verified
        );
        assert!(split.proof().nodes.len() <= MAX_LOG_SPLIT_PROOF_NODES);
        for node in split.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let prefix_audit = audit_log(verified.prefix(), &mut memory).expect("prefix audits");
        let suffix_audit = audit_log(verified.suffix(), &mut memory).expect("suffix audits");
        assert_eq!(prefix_audit.values(), &values[..9_000]);
        assert_eq!(suffix_audit.values(), &values[9_000..]);
        let split_reachable: BTreeSet<&str> = prefix_audit
            .objects()
            .iter()
            .chain(suffix_audit.objects())
            .map(|node| node.object_id.as_str())
            .collect();
        assert!(
            split
                .objects()
                .iter()
                .all(|node| split_reachable.contains(node.object_id.as_str()))
        );

        let replacement = vec![
            value("replacement-a"),
            value("replacement-a"),
            value("replacement-b"),
        ];
        let mutation =
            LogMutation::replace_prefix(9_000, verified.prefix().clone(), replacement.clone());
        let output = apply_log_mutations(&parent, std::slice::from_ref(&mutation), &mut memory)
            .expect("prefix replacement applies");
        let first = verify_log_apply(&parent, std::slice::from_ref(&mutation), output.proof())
            .expect("prefix replacement verifies");
        let replay = verify_log_apply(&parent, std::slice::from_ref(&mutation), output.proof())
            .expect("prefix replacement proof replays");
        assert_eq!(first, replay);
        absorb_apply(&mut memory, &output);
        let mut expected = replacement;
        expected.extend_from_slice(&values[9_000..]);
        let audit = audit_log(output.verified().result(), &mut memory).expect("result audits");
        assert_eq!(audit.values(), expected);
        let result_reachable: BTreeSet<&str> = audit
            .objects()
            .iter()
            .map(|node| node.object_id.as_str())
            .collect();
        assert!(
            output
                .objects()
                .iter()
                .all(|node| result_reachable.contains(node.object_id.as_str()))
        );

        let other_values: Vec<String> = (0..9_000)
            .map(|index| value(&format!("other-{index}")))
            .collect();
        let wrong_prefix = build_log(&other_values)
            .expect("wrong prefix builds")
            .root()
            .clone();
        assert!(
            apply_log_mutations(
                &parent,
                &[LogMutation::replace_prefix(
                    9_000,
                    wrong_prefix,
                    vec![value("x")]
                )],
                &mut memory,
            )
            .is_err()
        );
        assert!(verify_log_split(&LogRoot::empty(), 0, split.proof()).is_err());
        assert!(
            apply_log_mutations(
                &parent,
                &[LogMutation::replace_prefix(
                    9_000,
                    verified.prefix().clone(),
                    (0..=MAX_LOG_PREFIX_REPLACEMENT_VALUES)
                        .map(|index| value(&format!("too-many-{index}")))
                        .collect(),
                )],
                &mut memory,
            )
            .is_err()
        );
    }

    #[test]
    fn log_transport_decoders_roundtrip_and_reject_small_element_bombs() {
        let built = build_log(
            &(0..80)
                .map(|index| value(&index.to_string()))
                .collect::<Vec<_>>(),
        )
        .expect("fixture builds");
        let root = built.root().clone();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let node = built.objects().first().expect("fixture has node");
        let node_bytes = serde_json::to_vec(node).expect("node encodes");
        assert_eq!(&decode_log_node(&node_bytes).expect("node decodes"), node);
        let mut leaf_value =
            serde_json::to_value(LogNode::leaf(vec![value("leaf")]).expect("leaf"))
                .expect("leaf encodes");
        leaf_value["body"]["values"] =
            serde_json::Value::Array(vec![
                serde_json::Value::String(value("leaf"));
                LOG_FANOUT + 1
            ]);
        assert!(
            decode_log_node(&serde_json::to_vec(&leaf_value).expect("oversized leaf encodes"))
                .is_err()
        );

        let exact = prove_log_exact(&root, 37, &mut memory).expect("exact proof");
        let exact_bytes = serde_json::to_vec(&exact).expect("exact encodes");
        let decoded_exact = decode_log_exact_proof(&exact_bytes).expect("exact decodes");
        verify_log_exact(&root, 37, &decoded_exact).expect("decoded exact verifies");
        let mut exact_value = serde_json::to_value(&exact).expect("exact encodes");
        let exact_node = exact_value["nodes"][0].clone();
        exact_value["nodes"] =
            serde_json::Value::Array(vec![exact_node; MAX_LOG_EXACT_PROOF_NODES + 1]);
        assert!(
            decode_log_exact_proof(
                &serde_json::to_vec(&exact_value).expect("oversized exact encodes")
            )
            .is_err()
        );

        let range = prove_log_range(&root, 0, 1, MAX_PAGE_BYTES, &mut memory).expect("range");
        let range_bytes = serde_json::to_vec(&range).expect("range encodes");
        verify_log_range(
            &root,
            0,
            1,
            MAX_PAGE_BYTES,
            &decode_log_range_proof(&range_bytes).expect("range decodes"),
        )
        .expect("decoded range verifies");
        let mut range_value = serde_json::to_value(&range).expect("range encodes");
        let range_node = range_value["nodes"][0].clone();
        range_value["nodes"] =
            serde_json::Value::Array(vec![range_node; MAX_LOG_RANGE_PROOF_NODES + 1]);
        assert!(
            decode_log_range_proof(
                &serde_json::to_vec(&range_value).expect("oversized range encodes")
            )
            .is_err()
        );

        let split = split_log(&root, 40, &mut memory).expect("split");
        let split_bytes = serde_json::to_vec(split.proof()).expect("split encodes");
        verify_log_split(
            &root,
            40,
            &decode_log_split_proof(&split_bytes).expect("split decodes"),
        )
        .expect("decoded split verifies");
        let mut split_value = serde_json::to_value(split.proof()).expect("split encodes");
        let split_node = split_value["nodes"][0].clone();
        split_value["nodes"] =
            serde_json::Value::Array(vec![split_node; MAX_LOG_SPLIT_PROOF_NODES + 1]);
        assert!(
            decode_log_split_proof(
                &serde_json::to_vec(&split_value).expect("oversized split encodes")
            )
            .is_err()
        );

        let mutation = LogMutation::append(vec![value("appended")]);
        let applied = apply_log_mutations(&root, std::slice::from_ref(&mutation), &mut memory)
            .expect("append applies");
        let apply_bytes = serde_json::to_vec(applied.proof()).expect("apply encodes");
        verify_log_apply(
            &root,
            std::slice::from_ref(&mutation),
            &decode_log_apply_proof(&apply_bytes).expect("apply decodes"),
        )
        .expect("decoded apply verifies");
        let mut apply_value = serde_json::to_value(applied.proof()).expect("apply encodes");
        let encoded_mutation = apply_value["mutations"][0].clone();
        apply_value["mutations"] =
            serde_json::Value::Array(vec![encoded_mutation; MAX_LOG_MUTATIONS_PER_APPLY + 1]);
        assert!(
            decode_log_apply_proof(
                &serde_json::to_vec(&apply_value).expect("oversized apply encodes")
            )
            .is_err()
        );
    }

    #[test]
    fn maximum_log_apply_persists_only_final_reachable_new_nodes() {
        let mut model: Vec<String> = (0..512)
            .map(|index| value(&format!("old-{index}")))
            .collect();
        let built = build_log(&model).expect("parent log builds");
        let parent = built.root().clone();
        let parent_ids: BTreeSet<String> = built
            .objects()
            .iter()
            .map(|node| node.object_id.clone())
            .collect();
        let mut memory = Memory::default();
        for node in built.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        let mutations: Vec<LogMutation> = model
            .iter_mut()
            .take(MAX_LOG_MUTATIONS_PER_APPLY)
            .enumerate()
            .map(|(index, current)| {
                let previous = current.clone();
                let next = value(&format!("new-{index}"));
                *current = next.clone();
                LogMutation::replace_at(
                    u64::try_from(index).expect("test index fits u64"),
                    previous,
                    next,
                )
            })
            .collect();
        let output = apply_log_mutations(&parent, &mutations, &mut memory)
            .expect("maximum log batch applies");
        verify_log_apply(&parent, &mutations, output.proof()).expect("apply proof verifies");
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
        assert!(output.objects().len() <= MAX_LOG_APPLY_PROOF_NODES);
        absorb_apply(&mut memory, &output);
        let audit = audit_log(output.verified().result(), &mut memory).expect("result audits");
        assert_eq!(audit.values(), model);
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

    fn prefix_operation_load_counts(value_count: usize) -> (usize, usize) {
        let (values, parent, mut memory) = journal_fixture(value_count);
        let split_index = u64::try_from(values.len() - 17).expect("test index fits u64");
        memory.log_loads = 0;
        let split = split_log(&parent, split_index, &mut memory).expect("prefix splits");
        let split_loads = memory.log_loads;
        let prefix = split.verified().prefix().clone();
        for node in split.objects() {
            memory.logs.insert(node.object_id.clone(), node.clone());
        }
        memory.log_loads = 0;
        apply_log_mutations(
            &parent,
            &[LogMutation::replace_prefix(
                split_index,
                prefix,
                vec![value("load-a"), value("load-b"), value("load-a")],
            )],
            &mut memory,
        )
        .expect("prefix replacement applies");
        (split_loads, memory.log_loads)
    }

    #[test]
    fn split_and_prefix_replacement_resolver_loads_scale_logarithmically() {
        let small = prefix_operation_load_counts(1_024);
        let large = prefix_operation_load_counts(65_536);
        assert!(
            large.0 <= small.0 * 2,
            "split loads: {small:?} -> {large:?}"
        );
        assert!(
            large.1 <= small.1 * 2,
            "prefix replacement loads: {small:?} -> {large:?}"
        );
    }

    #[test]
    fn range_resolver_loads_scale_with_height_plus_returned_values() {
        let (_, small_root, mut small_memory) = journal_fixture(1_024);
        let (_, large_root, mut large_memory) = journal_fixture(65_536);
        let small_fixed = range_loads(&small_root, &mut small_memory, 512, 17);
        let large_fixed = range_loads(&large_root, &mut large_memory, 32_768, 17);
        let large_wide = range_loads(&large_root, &mut large_memory, 32_768, 128);

        assert!(
            large_fixed <= small_fixed + MAX_LOG_HEIGHT / 4,
            "fixed-width range loads grew with total cardinality: {small_fixed} -> {large_fixed}"
        );
        assert!(
            large_wide <= large_fixed + (4 * (128 - 17)),
            "wider range exceeded a linear frontier: {large_fixed} -> {large_wide}"
        );
    }
}
