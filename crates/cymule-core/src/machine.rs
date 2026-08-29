use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use crate::ir::{EffectContract, Operation, PlanCandidate, Region, validate_schema_instance};
use crate::model::{
    EffectIntentIdentityInput, OpenScopeEffectIndex, RunDerivedIndex, effect_intent_id,
    effect_obligation_id,
};
use crate::{
    ArtifactRecord, ArtifactRef, COMMAND_VERSION, Command, CommandEnvelope, CommandReceipt,
    CommandReceiptStatus, CoreError, Event, EventContent, EventPayload, InvocationPathSegment,
    ObligationProjection, Projection, ROOT_SCOPE_ID, ReplayAvailability, Result, RunProjection,
    SealedPlan, WorldOutcome, artifact_ref, canonical_digest, content_id, plan_invocation_id,
    plan_scope_id, sha256_bytes,
};

pub(crate) mod pinned;

const MACHINE_AUTHORITY_ROOT_DOMAIN: &str = "cymule.machine-authority-root/2";
const PROJECTION_ROOT_GENESIS_DOMAIN: &str = "cymule.projection-root-genesis/1";
const PROJECTION_ROOT_EVENT_DOMAIN: &str = "cymule.projection-root-event/1";
const SCOPE_OBLIGATION_COMMITMENT_DOMAIN: &str = "cymule.scope-obligation-lineage/1";
const MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN: &str = "cymule.machine-plan-admission-lineage/1";
const MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN: &str =
    "cymule.machine-artifact-admission-lineage/1";
const MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN: &str =
    "cymule.machine-command-batch-admission-lineage/1";
const MACHINE_COMMAND_ARCHIVE_BATCHES_DOMAIN: &str = "cymule.command-archive-batches/1";
const MACHINE_COMMAND_ARCHIVE_NODE_DOMAIN: &str = "cymule.command-archive-node/1";

struct MachineAuthorityRootInput<'a> {
    plan_commitment: &'a str,
    plan_count: u64,
    artifact_commitment: &'a str,
    artifact_count: u64,
    batch_commitment: &'a str,
    batch_count: u64,
    projection_root: &'a str,
    event_count: u64,
    admission_sequence: Option<u64>,
    admission_head: Option<&'a str>,
}

fn machine_authority_root(input: &MachineAuthorityRootInput<'_>) -> Result<String> {
    canonical_digest(&(
        MACHINE_AUTHORITY_ROOT_DOMAIN,
        input.plan_commitment,
        input.plan_count,
        input.artifact_commitment,
        input.artifact_count,
        input.batch_commitment,
        input.batch_count,
        input.projection_root,
        input.event_count,
        input.admission_sequence,
        input.admission_head,
    ))
}

/// Core-owned rolling commitment over one immutable-value admission lineage.
/// Exact membership and bytes remain owned by the authenticated physical maps;
/// this fixed-size value only binds insertion order and unique count.
#[derive(Debug, Clone)]
struct AdmissionCommitment {
    domain: &'static str,
    root: String,
}

type AdmissionCommitmentUndo = String;

impl AdmissionCommitment {
    fn new(domain: &'static str) -> Self {
        Self {
            domain,
            root: content_id(domain, &("genesis", ()))
                .expect("admission commitment genesis is canonical"),
        }
    }

    fn root(&self) -> &str {
        &self.root
    }

    fn insert_with_undo(&mut self, identity: &str) -> Result<AdmissionCommitmentUndo> {
        let undo = self.capture_undo(identity)?;
        self.root = content_id(self.domain, &("append", self.root.as_str(), identity))?;
        Ok(undo)
    }

    fn capture_undo(&self, identity: &str) -> Result<AdmissionCommitmentUndo> {
        crate::validate_content_id("Machine immutable admission", identity)?;
        Ok(self.root.clone())
    }

    fn restore(&mut self, undo: AdmissionCommitmentUndo) {
        self.root = undo;
    }
}

pub(crate) fn obligation_for_effect(
    effect: &crate::EffectProjection,
) -> Result<ObligationProjection> {
    if effect.profile.mutation != crate::MutationKind::Mutating {
        return Err(CoreError::Validation(format!(
            "observational Effect {} cannot create a blocking obligation",
            effect.intent_id
        )));
    }
    let obligation_id = effect_obligation_id(&effect.intent_id)?;
    Ok(ObligationProjection {
        obligation_id,
        intent_id: effect.intent_id.clone(),
        blocking: true,
        resolved: matches!(
            effect.outcome,
            WorldOutcome::Applied | WorldOutcome::NotApplied
        ),
    })
}

pub(crate) fn scope_obligation_summary(
    obligations: &[ObligationProjection],
) -> Result<(u64, String)> {
    let mut count = 0_u64;
    let mut commitment = scope_obligation_commitment_genesis()?;
    for obligation in obligations {
        if obligation.obligation_id != effect_obligation_id(&obligation.intent_id)?
            || !obligation.blocking
        {
            return Err(CoreError::Validation(format!(
                "scope obligation {} is not reducer-derived",
                obligation.obligation_id
            )));
        }
        count = count
            .checked_add(1)
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                CoreError::Validation(
                    "scope obligation count exceeds the exact integer range".to_owned(),
                )
            })?;
        commitment = scope_obligation_commitment_append(&commitment, obligation)?;
    }
    Ok((count, commitment))
}

pub(crate) fn scope_obligation_commitment_genesis() -> Result<String> {
    content_id(SCOPE_OBLIGATION_COMMITMENT_DOMAIN, &("genesis", ()))
}

pub(crate) fn scope_obligation_commitment_append(
    parent: &str,
    obligation: &ObligationProjection,
) -> Result<String> {
    crate::validate_content_id("scope obligation parent commitment", parent)?;
    if obligation.obligation_id != effect_obligation_id(&obligation.intent_id)?
        || !obligation.blocking
    {
        return Err(CoreError::Validation(format!(
            "scope obligation {} is not reducer-derived",
            obligation.obligation_id
        )));
    }
    content_id(
        SCOPE_OBLIGATION_COMMITMENT_DOMAIN,
        &("append", parent, obligation),
    )
}

#[derive(Debug, Clone)]
struct MachineAuthority {
    plans: AdmissionCommitment,
    artifacts: AdmissionCommitment,
    batches: AdmissionCommitment,
    batch_count: u64,
    projection_root: String,
}

struct MachineCutAdmissionFrontier {
    plans: AdmissionCommitment,
    plan_count: u64,
    artifacts: AdmissionCommitment,
    artifact_count: u64,
    batches: AdmissionCommitment,
    batch_count: u64,
}

#[derive(Default)]
struct MachineReplayAncestry {
    roots: BTreeMap<String, u64>,
    run_admissions: BTreeMap<String, u64>,
}

impl MachineReplayAncestry {
    fn observe(&mut self, machine: &Machine) -> Result<String> {
        let root = machine.authority_root()?;
        let sequence = machine
            .admission_parent()
            .map_or(0, |admission| admission.sequence);
        self.roots.insert(root.clone(), sequence);
        Ok(root)
    }

    fn verify_source(
        &self,
        batch: &MachineCommandBatchRecord,
        run_ids: impl Iterator<Item = String>,
    ) -> Result<()> {
        let source = self
            .roots
            .get(&batch.parent_authority_root)
            .ok_or_else(|| {
                CoreError::Causal(
                    "batch manifest source is not an authenticated replay ancestor".to_owned(),
                )
            })?;
        if run_ids
            .filter_map(|id| self.run_admissions.get(&id))
            .any(|sequence| sequence > source)
        {
            return Err(CoreError::Causal(
                "paged batch source was superseded by a same-Run admission".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for MachineAuthority {
    fn default() -> Self {
        Self {
            plans: AdmissionCommitment::new(MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN),
            artifacts: AdmissionCommitment::new(MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN),
            batches: AdmissionCommitment::new(MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN),
            batch_count: 0,
            projection_root: canonical_digest(&(PROJECTION_ROOT_GENESIS_DOMAIN, ()))
                .expect("empty projection-root preimage is canonical"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandRecord {
    envelope: CommandEnvelope,
    semantic_hash: String,
    receipt: CommandReceipt,
    batch_id: String,
    batch_position: u32,
    batch_len: u32,
}

/// Immutable exported form of one archived private command record.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivedCommandRecord {
    /// Exact admitted envelope.
    pub envelope: CommandEnvelope,
    /// Canonical envelope digest.
    pub semantic_hash: String,
    /// Exact retained receipt.
    pub receipt: CommandReceipt,
    /// Owning atomic batch identity.
    pub batch_id: String,
    /// Zero-based position in the owning batch.
    pub batch_position: u32,
    /// Exact owning batch length.
    pub batch_len: u32,
}

impl ArchivedCommandRecord {
    fn from_private(record: &CommandRecord) -> Self {
        Self {
            envelope: record.envelope.clone(),
            semantic_hash: record.semantic_hash.clone(),
            receipt: record.receipt.clone(),
            batch_id: record.batch_id.clone(),
            batch_position: record.batch_position,
            batch_len: record.batch_len,
        }
    }

    fn to_private(&self) -> CommandRecord {
        CommandRecord {
            envelope: self.envelope.clone(),
            semantic_hash: self.semantic_hash.clone(),
            receipt: self.receipt.clone(),
            batch_id: self.batch_id.clone(),
            batch_position: self.batch_position,
            batch_len: self.batch_len,
        }
    }

    /// Verify the closed command record independently of an admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope, semantic hash, or receipt is not a
    /// self-consistent admitted command record.
    pub fn verify(&self) -> Result<()> {
        verify_command_record(&self.to_private())
    }
}

/// One complete command admission archived outside the hot Machine snapshot.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandArchiveEntry {
    /// Ordered command admission.
    pub admission: CommandAdmission,
    /// Exact private command record exported for audit or proof.
    pub command: ArchivedCommandRecord,
    /// Complete exact applied Event batch; conflicts carry an empty batch.
    pub events: Vec<Event>,
}

impl MachineCommandArchiveEntry {
    fn leaf_digest(&self) -> Result<String> {
        content_id("cymule.command-archive-leaf/1", self)
    }

    /// Verify and return the content identity used by the command index value.
    ///
    /// # Errors
    ///
    /// Returns an error when any archived admission authority is malformed or
    /// its canonical leaf identity cannot be derived.
    pub fn identity(&self) -> Result<String> {
        self.verify()?;
        self.leaf_digest()
    }

    /// Verify the complete admission, command record, receipt, and Event disposition.
    ///
    /// # Errors
    ///
    /// Returns an error when the admission, command, receipt, and optional
    /// Event do not form one exact closed archive entry.
    pub fn verify(&self) -> Result<()> {
        self.verify_shape()
    }

    fn verify_shape(&self) -> Result<()> {
        let record = self.command.to_private();
        verify_admission_record(&self.admission, &record)?;
        if self.admission.event_ids
            != self
                .events
                .iter()
                .map(|event| event.event_id.clone())
                .collect::<Vec<_>>()
            || (self.admission.status == CommandReceiptStatus::Applied && self.events.is_empty())
            || (self.admission.status == CommandReceiptStatus::Conflict && !self.events.is_empty())
        {
            return Err(CoreError::IdentityMismatch(format!(
                "archived command {} does not match its Event disposition",
                self.admission.command_id
            )));
        }
        for event in &self.events {
            if event.command_id != self.admission.command_id
                || event.command_hash != self.admission.semantic_hash
            {
                return Err(CoreError::IdentityMismatch(
                    "archived Event batch changed command authority".to_owned(),
                ));
            }
            event.verify()?;
            verify_event_footprint(event)?;
            verify_command_event_correspondence(&record, event)?;
        }
        Ok(())
    }
}

const COMMAND_INDEX_DEPTH: usize = 256;
const COMMAND_INDEX_PROOF_VERSION: &str = "cymule.command-index-proof/2";

fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

/// Authenticated value retained for one archived command identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandIndexValue {
    /// Exact admission identity archived for this command.
    pub admission_id: String,
    /// Digest of the complete archived command entry, excluding its update witness.
    pub archive_entry_digest: String,
}

impl MachineCommandIndexValue {
    fn verify(&self) -> Result<()> {
        if !is_sha256_id(&self.admission_id) || !is_sha256_id(&self.archive_entry_digest) {
            return Err(CoreError::Validation(
                "command index value is malformed".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Membership or non-membership proof in the cumulative archived-command map.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandIndexProof {
    /// Proof schema version.
    pub proof_version: String,
    /// Exact command identity whose hashed path is proven.
    pub command_id: String,
    /// Archived value for membership; absent for non-membership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<MachineCommandIndexValue>,
    /// Depth of the canonical empty subtree proving non-membership. This is
    /// required and null for membership proofs. A non-membership proof stores
    /// only the siblings above this subtree.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub empty_depth: Option<u16>,
    /// Sibling hashes above the proven leaf or empty subtree, ordered toward
    /// the root. Membership carries exactly 256; non-membership carries exactly
    /// `empty_depth`.
    pub siblings: Vec<String>,
}

/// One immutable content-addressed node in the archived-command sparse map.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineCommandIndexNode {
    /// Internal binary node.
    Branch {
        /// Content-addressed node identity.
        node_id: String,
        /// Zero-based tree depth.
        depth: u16,
        /// Left child identity.
        left: String,
        /// Right child identity.
        right: String,
    },
    /// Occupied leaf.
    Member {
        /// Content-addressed leaf identity.
        node_id: String,
        /// Hashed 256-bit command path.
        key_hash: String,
        /// Original command identity, retained to close hash-collision behavior.
        command_id: String,
        /// Authenticated archive entry value.
        value: MachineCommandIndexValue,
    },
}

impl MachineCommandIndexNode {
    /// Return and verify this node's content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the node shape, child identities, command key, or
    /// declared content identity is invalid.
    pub fn identity(&self) -> Result<&str> {
        match self {
            Self::Branch {
                node_id,
                depth,
                left,
                right,
            } => {
                let depth = usize::from(*depth);
                if depth >= COMMAND_INDEX_DEPTH
                    || !is_sha256_id(left)
                    || !is_sha256_id(right)
                    || command_index_node(depth, left, right)? != *node_id
                {
                    return Err(CoreError::IdentityMismatch(
                        "command index branch node is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
            Self::Member {
                node_id,
                key_hash,
                command_id,
                value,
            } => {
                validate_identity("command ID", command_id)?;
                value.verify()?;
                let key = command_index_key(command_id)?;
                if key_hash != &command_index_key_id(&key)
                    || command_index_member_leaf(&key, command_id, value)? != *node_id
                {
                    return Err(CoreError::IdentityMismatch(
                        "command index member node is malformed".to_owned(),
                    ));
                }
                Ok(node_id)
            }
        }
    }

    /// Verify this persistent sparse-map node.
    ///
    /// # Errors
    ///
    /// Returns an error when the node is not exact authenticated index
    /// authority.
    pub fn verify(&self) -> Result<()> {
        self.identity().map(|_| ())
    }
}

impl MachineCommandIndexProof {
    /// Return the canonical empty archived-command map root.
    ///
    /// # Errors
    ///
    /// Returns an error if Core cannot derive the versioned empty sparse-map
    /// root.
    pub fn empty_root() -> Result<String> {
        Ok(command_index_empty_hashes()[0].clone())
    }

    /// Return the canonical empty subtree identity at depth 0..=256.
    ///
    /// # Errors
    ///
    /// Returns a validation error when `depth` is outside the closed sparse-map
    /// depth.
    pub fn empty_hash(depth: u16) -> Result<String> {
        let depth = usize::from(depth);
        command_index_empty_hashes()
            .get(depth)
            .cloned()
            .ok_or_else(|| {
                CoreError::Validation("command index empty depth exceeds 256".to_owned())
            })
    }

    /// Construct the canonical non-membership proof for an empty map.
    ///
    /// # Errors
    ///
    /// Returns a validation error when the command identity is invalid.
    pub fn empty_nonmembership(command_id: impl Into<String>) -> Result<Self> {
        let command_id = command_id.into();
        validate_identity("command ID", &command_id)?;
        Ok(Self {
            proof_version: COMMAND_INDEX_PROOF_VERSION.to_owned(),
            command_id,
            value: None,
            empty_depth: Some(0),
            siblings: Vec::new(),
        })
    }

    /// Verify this proof against one exact cumulative index root.
    ///
    /// # Errors
    ///
    /// Returns an error when the proof wire is noncanonical or does not reach
    /// `expected_root` for its exact command key.
    pub fn verify(&self, expected_root: &str) -> Result<()> {
        self.validate_shape()?;
        if self.is_canonical_empty_nonmembership(expected_root) {
            #[cfg(test)]
            COMMAND_INDEX_EMPTY_FAST_VERIFY_COUNT
                .with(|count| count.set(count.get().saturating_add(1)));
            return Ok(());
        }
        if !is_sha256_id(expected_root) || self.root()? != expected_root {
            return Err(CoreError::IdentityMismatch(
                "command index proof does not reach the expected root".to_owned(),
            ));
        }
        Ok(())
    }

    fn is_canonical_empty_nonmembership(&self, expected_root: &str) -> bool {
        expected_root == command_index_empty_hashes()[0]
            && self.value.is_none()
            && self.empty_depth == Some(0)
            && self.siblings.is_empty()
    }

    fn validate_shape(&self) -> Result<usize> {
        let depth = self.validate_shape_syntax()?;
        if self.value.is_none()
            && depth > 0
            && self.siblings.first() == command_index_empty_hashes().get(depth)
        {
            return Err(CoreError::Validation(
                "command index non-membership proof is not maximally compressed".to_owned(),
            ));
        }
        Ok(depth)
    }

    fn validate_shape_syntax(&self) -> Result<usize> {
        if self.proof_version != COMMAND_INDEX_PROOF_VERSION {
            return Err(CoreError::Validation(
                "command index proof has an unsupported version".to_owned(),
            ));
        }
        validate_identity("command ID", &self.command_id)?;
        let depth = match (&self.value, self.empty_depth) {
            (Some(value), None) if self.siblings.len() == COMMAND_INDEX_DEPTH => {
                value.verify()?;
                COMMAND_INDEX_DEPTH
            }
            (None, Some(depth))
                if usize::from(depth) <= COMMAND_INDEX_DEPTH
                    && self.siblings.len() == usize::from(depth) =>
            {
                usize::from(depth)
            }
            _ => {
                return Err(CoreError::Validation(
                    "command index proof has an unsupported membership shape".to_owned(),
                ));
            }
        };
        if self.siblings.iter().any(|sibling| !is_sha256_id(sibling)) {
            return Err(CoreError::Validation(
                "command index proof contains a malformed sibling".to_owned(),
            ));
        }
        Ok(depth)
    }

    fn materialized(&self) -> Result<Self> {
        let depth = self.validate_shape()?;
        if self.value.is_some() || depth == COMMAND_INDEX_DEPTH {
            return Ok(self.clone());
        }
        let empty = command_index_empty_hashes();
        let mut siblings = Vec::with_capacity(COMMAND_INDEX_DEPTH);
        siblings.extend(
            (0..COMMAND_INDEX_DEPTH - depth)
                .map(|level| empty[COMMAND_INDEX_DEPTH - level].clone()),
        );
        siblings.extend(self.siblings.iter().cloned());
        Ok(Self {
            proof_version: self.proof_version.clone(),
            command_id: self.command_id.clone(),
            value: None,
            empty_depth: Some(
                u16::try_from(COMMAND_INDEX_DEPTH)
                    .map_err(|error| CoreError::Validation(error.to_string()))?,
            ),
            siblings,
        })
    }

    fn compress_nonmembership(&self) -> Result<Self> {
        if self.value.is_some() {
            self.validate_shape()?;
            return Ok(self.clone());
        }
        let depth = self.validate_shape_syntax()?;
        let mut materialized = if depth == COMMAND_INDEX_DEPTH {
            self.clone()
        } else {
            self.materialized()?
        };
        let empty = command_index_empty_hashes();
        let omitted = materialized
            .siblings
            .iter()
            .enumerate()
            .take_while(|(level, sibling)| *sibling == &empty[COMMAND_INDEX_DEPTH - *level])
            .count();
        materialized.siblings.drain(..omitted);
        materialized.empty_depth = Some(
            u16::try_from(COMMAND_INDEX_DEPTH - omitted)
                .map_err(|error| CoreError::Validation(error.to_string()))?,
        );
        materialized.validate_shape()?;
        Ok(materialized)
    }

    fn root(&self) -> Result<String> {
        self.root_with_value(self.value.as_ref())
    }

    fn root_with_value(&self, value: Option<&MachineCommandIndexValue>) -> Result<String> {
        let proof_depth = self.validate_shape()?;
        let key = command_index_key(&self.command_id)?;
        let empty = command_index_empty_hashes();
        let mut current = match (&self.value, value) {
            (Some(expected), Some(value)) if expected == value => {
                command_index_member_leaf(&key, &self.command_id, value)?
            }
            (Some(_), _) => {
                return Err(CoreError::Validation(
                    "membership proof value cannot be replaced".to_owned(),
                ));
            }
            (None, Some(value)) => {
                value.verify()?;
                let mut current = command_index_member_leaf(&key, &self.command_id, value)?;
                for level in 0..COMMAND_INDEX_DEPTH - proof_depth {
                    let depth = COMMAND_INDEX_DEPTH - level - 1;
                    let sibling = &empty[COMMAND_INDEX_DEPTH - level];
                    current = if command_index_bit(&key, depth) {
                        command_index_node(depth, sibling, &current)?
                    } else {
                        command_index_node(depth, &current, sibling)?
                    };
                }
                current
            }
            (None, None) => empty[proof_depth].clone(),
        };
        for (level, sibling) in self.siblings.iter().enumerate() {
            let depth = proof_depth - level - 1;
            current = if command_index_bit(&key, depth) {
                command_index_node(depth, sibling, &current)?
            } else {
                command_index_node(depth, &current, sibling)?
            };
        }
        Ok(current)
    }
}

fn command_index_key(command_id: &str) -> Result<[u8; 32]> {
    validate_identity("command ID", command_id)?;
    let length = u32::try_from(command_id.len())
        .map_err(|error| CoreError::Validation(error.to_string()))?;
    let mut payload = Vec::with_capacity(4 + command_id.len());
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(command_id.as_bytes());
    decode_command_index_hash(&command_index_binary_hash(
        b"cymule.command-index-key/1",
        &payload,
    ))
}

fn command_index_bit(key: &[u8; 32], depth: usize) -> bool {
    key[depth / 8] & (1 << (7 - depth % 8)) != 0
}

fn command_index_member_leaf(
    key: &[u8; 32],
    command_id: &str,
    value: &MachineCommandIndexValue,
) -> Result<String> {
    let command_length = u32::try_from(command_id.len())
        .map_err(|error| CoreError::Validation(error.to_string()))?;
    let admission_id = decode_command_index_hash(&value.admission_id)?;
    let entry_digest = decode_command_index_hash(&value.archive_entry_digest)?;
    let mut payload = Vec::with_capacity(32 + 4 + command_id.len() + 64);
    payload.extend_from_slice(key);
    payload.extend_from_slice(&command_length.to_be_bytes());
    payload.extend_from_slice(command_id.as_bytes());
    payload.extend_from_slice(&admission_id);
    payload.extend_from_slice(&entry_digest);
    Ok(command_index_binary_hash(
        b"cymule.command-index-leaf/1",
        &payload,
    ))
}

fn command_index_key_id(key: &[u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut value = String::with_capacity("sha256:".len() + 64);
    value.push_str("sha256:");
    for byte in key {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn command_index_node(depth: usize, left: &str, right: &str) -> Result<String> {
    let depth = u16::try_from(depth).map_err(|error| CoreError::Validation(error.to_string()))?;
    let left = decode_command_index_hash(left)?;
    let right = decode_command_index_hash(right)?;
    let mut payload = Vec::with_capacity(2 + 64);
    payload.extend_from_slice(&depth.to_be_bytes());
    payload.extend_from_slice(&left);
    payload.extend_from_slice(&right);
    #[cfg(test)]
    COMMAND_INDEX_NODE_HASH_COUNT.with(|count| count.set(count.get().saturating_add(1)));
    Ok(command_index_binary_hash(
        b"cymule.command-index-node/1",
        &payload,
    ))
}

fn command_index_empty_hashes() -> &'static [String] {
    static EMPTY_HASHES: OnceLock<Vec<String>> = OnceLock::new();
    EMPTY_HASHES.get_or_init(|| {
        let mut hashes = vec![String::new(); COMMAND_INDEX_DEPTH + 1];
        hashes[COMMAND_INDEX_DEPTH] =
            command_index_binary_hash(b"cymule.command-index-empty-leaf/1", &[]);
        for depth in (0..COMMAND_INDEX_DEPTH).rev() {
            hashes[depth] = command_index_node(depth, &hashes[depth + 1], &hashes[depth + 1])
                .expect("canonical empty command index children are valid");
        }
        hashes
    })
}

fn command_index_binary_hash(domain: &[u8], payload: &[u8]) -> String {
    let mut input = Vec::with_capacity(domain.len() + 1 + payload.len());
    input.extend_from_slice(domain);
    input.push(0);
    input.extend_from_slice(payload);
    format!("sha256:{}", sha256_bytes(&input))
}

fn decode_command_index_hash(value: &str) -> Result<[u8; 32]> {
    if !is_sha256_id(value) {
        return Err(CoreError::Validation(
            "command index hash is malformed".to_owned(),
        ));
    }
    let hex = &value["sha256:".len()..];
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|error| CoreError::Encoding(error.to_string()))?;
    }
    Ok(decoded)
}

#[cfg(test)]
thread_local! {
    static COMMAND_INDEX_NODE_HASH_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static COMMAND_INDEX_EMPTY_FAST_VERIFY_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static COMPACTION_AUTHORITY_BUILD_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MACHINE_AUTHORITY_NODE_HASH_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static PROJECTION_ROOT_ADVANCE_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Immutable content-addressed archive-segment header.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandArchiveSegmentHeader {
    /// Archive segment schema version.
    pub segment_version: String,
    /// Content-addressed segment identity.
    pub segment_id: String,
    /// Previous archive segment, absent only for genesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_segment: Option<String>,
    /// Admission count covered by the parent.
    pub parent_count: u64,
    /// Applied Event count covered by the parent.
    pub parent_event_count: u64,
    /// Admission head covered by the parent, absent before the first command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_admission_head: Option<String>,
    /// Cumulative archived-command map root before this segment.
    pub parent_command_index_root: String,
    /// Number of entries in this segment.
    pub entry_count: u64,
    /// Number of applied Events in this segment.
    pub event_count: u64,
    /// Number of complete atomic batch records in this segment.
    pub batch_count: u64,
    /// Content root of the canonically ordered batch records.
    pub batches_root: String,
    /// Merkle root of the ordered complete entries.
    pub entries_root: String,
    /// Total archived admission count after this segment.
    pub result_count: u64,
    /// Total archived Event count after this segment.
    pub result_event_count: u64,
    /// Admission head after this segment.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub result_admission_head: Option<String>,
    /// Cumulative archived-command map root after this segment.
    pub result_command_index_root: String,
}

#[derive(serde::Serialize)]
struct MachineCommandArchiveSegmentPreimage<'a> {
    segment_version: &'static str,
    parent_segment: &'a Option<String>,
    parent_count: u64,
    parent_event_count: u64,
    parent_admission_head: &'a Option<String>,
    parent_command_index_root: &'a str,
    entry_count: u64,
    event_count: u64,
    batch_count: u64,
    batches_root: &'a str,
    entries_root: &'a str,
    result_count: u64,
    result_event_count: u64,
    result_admission_head: &'a Option<String>,
    result_command_index_root: &'a str,
}

impl MachineCommandArchiveSegmentHeader {
    /// Current command-archive segment schema.
    pub const VERSION: &'static str = "cymule.command-archive-segment/4";

    /// Verify the header's content identity and closed count lineage.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment version, counts, parent lineage,
    /// roots, or declared content identity is invalid.
    pub fn verify(&self) -> Result<()> {
        if self.segment_version != Self::VERSION
            || self.result_count
                != self
                    .parent_count
                    .checked_add(self.entry_count)
                    .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
                    .ok_or_else(|| {
                        CoreError::Validation("command archive count overflowed".to_owned())
                    })?
            || self.result_event_count
                != self
                    .parent_event_count
                    .checked_add(self.event_count)
                    .ok_or_else(|| {
                        CoreError::Validation("command archive Event count overflowed".to_owned())
                    })?
            || self.event_count
                > self.entry_count.checked_mul(2).ok_or_else(|| {
                    CoreError::Validation("command archive Event bound overflowed".to_owned())
                })?
            || self.batch_count == 0
            || self.batch_count > crate::MAX_EXACT_INTEGER
            || self.parent_event_count
                > self.parent_count.checked_mul(2).ok_or_else(|| {
                    CoreError::Validation(
                        "command archive parent Event bound overflowed".to_owned(),
                    )
                })?
            || self.result_event_count
                > self.result_count.checked_mul(2).ok_or_else(|| {
                    CoreError::Validation(
                        "command archive result Event bound overflowed".to_owned(),
                    )
                })?
            || (self.parent_count == 0 && self.parent_event_count != 0)
            || (self.parent_segment.is_none() && self.parent_count != 0)
            || (self.parent_count == 0) != self.parent_admission_head.is_none()
            || (self.result_count == 0) != self.result_admission_head.is_none()
            || self
                .parent_segment
                .as_deref()
                .is_some_and(|value| !is_sha256_id(value))
            || self
                .parent_admission_head
                .as_deref()
                .is_some_and(|value| !is_sha256_id(value))
            || !is_sha256_id(&self.parent_command_index_root)
            || !is_sha256_id(&self.entries_root)
            || !is_sha256_id(&self.batches_root)
            || self
                .result_admission_head
                .as_deref()
                .is_some_and(|value| !is_sha256_id(value))
            || !is_sha256_id(&self.result_command_index_root)
            || (self.parent_count == 0
                && self.parent_command_index_root != MachineCommandIndexProof::empty_root()?)
            || (self.result_count == 0
                && self.result_command_index_root != MachineCommandIndexProof::empty_root()?)
            || (self.entry_count == 0
                && (self.result_admission_head != self.parent_admission_head
                    || self.result_command_index_root != self.parent_command_index_root
                    || self.entries_root != command_archive_empty_root()?))
            || self.segment_id != self.expected_id()?
        {
            return Err(CoreError::Validation(
                "command archive segment header is malformed".to_owned(),
            ));
        }
        Ok(())
    }

    fn expected_id(&self) -> Result<String> {
        content_id(
            Self::VERSION,
            &MachineCommandArchiveSegmentPreimage {
                segment_version: Self::VERSION,
                parent_segment: &self.parent_segment,
                parent_count: self.parent_count,
                parent_event_count: self.parent_event_count,
                parent_admission_head: &self.parent_admission_head,
                parent_command_index_root: &self.parent_command_index_root,
                entry_count: self.entry_count,
                event_count: self.event_count,
                batch_count: self.batch_count,
                batches_root: &self.batches_root,
                entries_root: &self.entries_root,
                result_count: self.result_count,
                result_event_count: self.result_event_count,
                result_admission_head: &self.result_admission_head,
                result_command_index_root: &self.result_command_index_root,
            },
        )
    }
}

/// Independent immutable archive object emitted by one Machine compaction.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandArchiveSegment {
    /// Content-addressed segment header.
    pub header: MachineCommandArchiveSegmentHeader,
    /// Complete ordered entries covered by the Merkle root.
    pub entries: Vec<MachineCommandArchiveEntry>,
    /// Complete atomic batch records referenced by the entries.
    pub batches: Vec<MachineCommandBatchRecord>,
    /// Sequential non-membership witnesses updating the cumulative command map.
    pub command_index_updates: Vec<MachineCommandIndexProof>,
}

struct MachineCommandArchiveParent {
    segment: Option<String>,
    count: u64,
    event_count: u64,
    admission_head: Option<String>,
    command_index_root: String,
}

impl MachineCommandArchiveSegment {
    fn new(
        parent: MachineCommandArchiveParent,
        batches: Vec<MachineCommandBatchRecord>,
        entries: Vec<MachineCommandArchiveEntry>,
        command_index_updates: Vec<MachineCommandIndexProof>,
    ) -> Result<Self> {
        let MachineCommandArchiveParent {
            segment: parent_segment,
            count: parent_count,
            event_count: parent_event_count,
            admission_head: parent_admission_head,
            command_index_root: parent_command_index_root,
        } = parent;
        if batches.is_empty() {
            return Err(CoreError::Validation(
                "command archive segment cannot be empty".to_owned(),
            ));
        }
        let entry_count = u64::try_from(entries.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let batch_count = u64::try_from(batches.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let batches_root = content_id(MACHINE_COMMAND_ARCHIVE_BATCHES_DOMAIN, &batches)?;
        if entries.len() != command_index_updates.len() {
            return Err(CoreError::Validation(
                "command archive entries and index updates differ in length".to_owned(),
            ));
        }
        let entries_root = command_archive_merkle_root(&entries, &command_index_updates)?;
        let event_count = archive_event_count(&entries)?;
        let result_count = parent_count
            .checked_add(entry_count)
            .ok_or_else(|| CoreError::Validation("command archive count overflowed".to_owned()))?;
        let result_event_count = parent_event_count.checked_add(event_count).ok_or_else(|| {
            CoreError::Validation("command archive Event count overflowed".to_owned())
        })?;
        let result_admission_head = entries
            .last()
            .map(|entry| entry.admission.admission_id.clone())
            .or_else(|| parent_admission_head.clone());
        let result_command_index_root = apply_command_index_updates(
            &parent_command_index_root,
            &entries,
            &command_index_updates,
        )?;
        let mut header = MachineCommandArchiveSegmentHeader {
            segment_version: MachineCommandArchiveSegmentHeader::VERSION.to_owned(),
            segment_id: String::new(),
            parent_segment,
            parent_count,
            parent_event_count,
            parent_admission_head,
            parent_command_index_root,
            entry_count,
            event_count,
            batch_count,
            batches_root,
            entries_root,
            result_count,
            result_event_count,
            result_admission_head,
            result_command_index_root,
        };
        header.segment_id = header.expected_id()?;
        let segment = Self {
            header,
            entries,
            batches,
            command_index_updates,
        };
        segment.verify()?;
        Ok(segment)
    }

    /// Verify every entry, its admission links, Merkle root, and segment header.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment is not an exact contiguous admission,
    /// Event, Merkle, and command-index transition.
    pub fn verify(&self) -> Result<()> {
        self.header.verify()?;
        let batch_count = u64::try_from(self.batches.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let mut batches = BTreeMap::new();
        for batch in &self.batches {
            batch.verify()?;
            if batches.insert(batch.batch_id.clone(), batch).is_some() {
                return Err(CoreError::Validation(
                    "command archive repeats a batch identity".to_owned(),
                ));
            }
        }
        if u64::try_from(self.entries.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?
            != self.header.entry_count
            || archive_event_count(&self.entries)? != self.header.event_count
            || batch_count != self.header.batch_count
            || content_id(MACHINE_COMMAND_ARCHIVE_BATCHES_DOMAIN, &self.batches)?
                != self.header.batches_root
            || self.entries.len() != self.command_index_updates.len()
            || command_archive_merkle_root(&self.entries, &self.command_index_updates)?
                != self.header.entries_root
            || self
                .entries
                .last()
                .map(|entry| &entry.admission.admission_id)
                .or(self.header.parent_admission_head.as_ref())
                != self.header.result_admission_head.as_ref()
        {
            return Err(CoreError::IdentityMismatch(format!(
                "command archive segment {} does not match its entries",
                self.header.segment_id
            )));
        }
        for entry in &self.entries {
            let batch = batches.get(&entry.command.batch_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "archived command {} has no batch {}",
                    entry.admission.command_id, entry.command.batch_id
                ))
            })?;
            batch.verify_entry(entry)?;
        }
        if self
            .batches
            .iter()
            .flat_map(|batch| batch.members.iter().map(|member| &member.command_id))
            .ne(self
                .entries
                .iter()
                .map(|entry| &entry.command.envelope.command_id))
        {
            return Err(CoreError::IdentityMismatch(
                "archive segment does not contain complete ordered batches".to_owned(),
            ));
        }
        if apply_command_index_updates(
            &self.header.parent_command_index_root,
            &self.entries,
            &self.command_index_updates,
        )? != self.header.result_command_index_root
        {
            return Err(CoreError::IdentityMismatch(format!(
                "command archive segment {} does not match its command index update",
                self.header.segment_id
            )));
        }
        let mut expected_sequence = self.header.parent_count.checked_add(1).ok_or_else(|| {
            CoreError::Validation("command archive sequence overflowed".to_owned())
        })?;
        let mut expected_parent = self.header.parent_admission_head.as_deref();
        for entry in &self.entries {
            entry.verify_shape()?;
            let parent = expected_parent.map(|admission_id| CommandAdmissionParent {
                sequence: expected_sequence - 1,
                admission_id,
            });
            entry.admission.verify(parent)?;
            if entry.admission.sequence != expected_sequence
                || entry.admission.parent_admission.as_deref() != expected_parent
            {
                return Err(CoreError::Validation(format!(
                    "command archive segment {} has discontinuous admissions",
                    self.header.segment_id
                )));
            }
            expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                CoreError::Validation("command archive sequence overflowed".to_owned())
            })?;
            expected_parent = Some(&entry.admission.admission_id);
        }
        Ok(())
    }
}

fn archive_event_count(entries: &[MachineCommandArchiveEntry]) -> Result<u64> {
    entries.iter().try_fold(0_u64, |count, entry| {
        let len = u64::try_from(entry.events.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        count.checked_add(len).ok_or_else(|| {
            CoreError::Validation("command archive Event count overflowed".to_owned())
        })
    })
}

/// Side of one sibling in an ordered Merkle inclusion path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MerkleSiblingSide {
    /// Sibling precedes the current digest.
    Left,
    /// Sibling follows the current digest.
    Right,
}

/// One sibling in a command-archive inclusion proof.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandArchiveMerkleSibling {
    /// Sibling position.
    pub side: MerkleSiblingSide,
    /// Content-addressed sibling digest.
    pub digest: String,
}

/// Explicit inclusion proof for one complete entry inside its archive segment.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineArchivedCommandProof {
    /// Proof schema version.
    pub proof_version: String,
    /// Header of the segment containing the command.
    pub segment: MachineCommandArchiveSegmentHeader,
    /// Zero-based entry index within the segment.
    pub entry_index: u64,
    /// Complete archived entry.
    pub entry: MachineCommandArchiveEntry,
    /// Exact sparse-map update witness bound into the segment leaf.
    pub command_index_update: MachineCommandIndexProof,
    /// Ordered Merkle siblings from leaf to segment root.
    pub merkle_path: Vec<CommandArchiveMerkleSibling>,
}

/// Exact archived-command lookup resolved by the external immutable index store.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum MachineCommandArchiveLookup {
    /// The current cumulative index proves this command was archived.
    Member {
        /// O(log N) membership proof against the current base root.
        index_proof: MachineCommandIndexProof,
        /// Complete archived entry loaded by the proof's entry digest.
        entry: Box<MachineCommandArchiveEntry>,
    },
    /// The current cumulative index proves this command has never been archived.
    NonMember {
        /// O(log N) non-membership proof against the current base root.
        index_proof: MachineCommandIndexProof,
    },
}

impl MachineCommandArchiveSegment {
    /// Build one Merkle inclusion proof for this exact segment object.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment is invalid, `entry_index` is absent,
    /// or the proof cannot be derived canonically.
    pub fn command_proof(&self, entry_index: usize) -> Result<MachineArchivedCommandProof> {
        self.verify()?;
        let entry = self.entries.get(entry_index).cloned().ok_or_else(|| {
            CoreError::NotFound(format!("archive entry index {entry_index} does not exist"))
        })?;
        Ok(MachineArchivedCommandProof {
            proof_version: MachineArchivedCommandProof::VERSION.to_owned(),
            segment: self.header.clone(),
            entry_index: u64::try_from(entry_index)
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            entry,
            command_index_update: self.command_index_updates[entry_index].clone(),
            merkle_path: command_archive_merkle_path(
                &self.entries,
                &self.command_index_updates,
                entry_index,
            )?,
        })
    }

    /// Build a current-root sparse-Merkle membership proof from explicit archive objects.
    ///
    /// Normal stores should answer this from their persistent node index. This
    /// linear helper is for explicit archive audit, migration, and tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment chain, target index, sparse-map update
    /// witnesses, or resulting current-root membership proof is invalid.
    pub fn command_index_proof(
        &self,
        entry_index: usize,
        descendants: &[MachineCommandArchiveSegment],
    ) -> Result<MachineCommandIndexProof> {
        self.verify()?;
        if entry_index >= self.entries.len() {
            return Err(CoreError::NotFound(format!(
                "archive entry index {entry_index} does not exist"
            )));
        }
        let mut current = self.header.parent_command_index_root.clone();
        let mut membership = None;
        for (index, (entry, update)) in self
            .entries
            .iter()
            .zip(&self.command_index_updates)
            .enumerate()
        {
            update.verify(&current)?;
            let value = command_index_entry_value(entry)?;
            let (nodes, result) = record_command_index_member_update(update, &value)?;
            if index == entry_index {
                let mut proof = update.materialized()?;
                proof.value = Some(value);
                proof.empty_depth = None;
                proof.verify(&result)?;
                membership = Some(proof);
            } else if let Some(proof) = membership.take() {
                membership = Some(rebase_command_index_proof(
                    &proof, &current, &result, &nodes,
                )?);
            }
            current = result;
        }
        if current != self.header.result_command_index_root {
            return Err(CoreError::IdentityMismatch(
                "archive segment command index does not reach its result root".to_owned(),
            ));
        }
        let mut head = &self.header;
        for descendant in descendants {
            descendant.verify()?;
            if descendant.header.parent_segment.as_deref() != Some(head.segment_id.as_str())
                || descendant.header.parent_command_index_root != current
            {
                return Err(CoreError::Causal(
                    "command index proof descendants are discontinuous".to_owned(),
                ));
            }
            for (entry, update) in descendant
                .entries
                .iter()
                .zip(&descendant.command_index_updates)
            {
                update.verify(&current)?;
                let value = command_index_entry_value(entry)?;
                let (nodes, result) = record_command_index_member_update(update, &value)?;
                let proof = membership.take().ok_or_else(|| {
                    CoreError::IdentityMismatch(
                        "target command index membership was not initialized".to_owned(),
                    )
                })?;
                membership = Some(rebase_command_index_proof(
                    &proof, &current, &result, &nodes,
                )?);
                current = result;
            }
            head = &descendant.header;
        }
        let proof = membership.ok_or_else(|| {
            CoreError::IdentityMismatch(
                "target command index membership was not initialized".to_owned(),
            )
        })?;
        proof.verify(&current)?;
        Ok(proof)
    }

    /// Materialize the immutable sparse-map nodes introduced by this segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment or any authenticated sparse-map update
    /// cannot be verified and materialized exactly.
    pub fn command_index_nodes(&self) -> Result<Vec<MachineCommandIndexNode>> {
        self.verify()?;
        let mut nodes = BTreeMap::<String, MachineCommandIndexNode>::new();
        for (entry, update) in self.entries.iter().zip(&self.command_index_updates) {
            let update = update.materialized()?;
            let key = command_index_key(&update.command_id)?;
            let value = command_index_entry_value(entry)?;
            let mut node_id = command_index_member_leaf(&key, &update.command_id, &value)?;
            let member = MachineCommandIndexNode::Member {
                node_id: node_id.clone(),
                key_hash: command_index_key_id(&key),
                command_id: update.command_id.clone(),
                value,
            };
            insert_command_index_node(&mut nodes, member)?;
            for (level, sibling) in update.siblings.iter().enumerate() {
                let depth = COMMAND_INDEX_DEPTH - level - 1;
                let (left, right) = if command_index_bit(&key, depth) {
                    (sibling.clone(), node_id)
                } else {
                    (node_id, sibling.clone())
                };
                node_id = command_index_node(depth, &left, &right)?;
                let branch = MachineCommandIndexNode::Branch {
                    node_id: node_id.clone(),
                    depth: u16::try_from(depth)
                        .map_err(|error| CoreError::Validation(error.to_string()))?,
                    left,
                    right,
                };
                insert_command_index_node(&mut nodes, branch)?;
            }
        }
        Ok(nodes.into_values().collect())
    }

    /// Expand this segment into the immutable objects a Store commits atomically.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment cannot be expanded into a bounded set
    /// of individually valid content-addressed objects.
    pub fn persistence_objects(&self) -> Result<Vec<MachineCommandArchiveObject>> {
        self.verify()?;
        let nodes = self.command_index_nodes()?;
        let capacity = 1_usize
            .checked_add(self.entries.len())
            .and_then(|capacity| capacity.checked_add(self.batches.len()))
            .and_then(|capacity| capacity.checked_add(nodes.len()))
            .ok_or_else(|| {
                CoreError::Validation(
                    "Machine archive persistence-object count overflowed".to_owned(),
                )
            })?;
        let mut objects = Vec::with_capacity(capacity);
        objects.push(MachineCommandArchiveObject::Segment(Box::new(self.clone())));
        objects.extend(
            self.batches
                .iter()
                .cloned()
                .map(Box::new)
                .map(MachineCommandArchiveObject::Batch),
        );
        objects.extend(
            self.entries
                .iter()
                .cloned()
                .map(Box::new)
                .map(MachineCommandArchiveObject::Entry),
        );
        objects.extend(
            nodes
                .into_iter()
                .map(MachineCommandArchiveObject::CommandIndexNode),
        );
        for object in &objects {
            object.identity()?;
        }
        Ok(objects)
    }
}

/// Immutable objects sharing the command-archive persistence namespace.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(
    tag = "object_kind",
    content = "object",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum MachineCommandArchiveObject {
    /// One complete raw-audit and root-update segment.
    Segment(Box<MachineCommandArchiveSegment>),
    /// One complete archived command entry addressed by the SMT value.
    Entry(Box<MachineCommandArchiveEntry>),
    /// One complete atomic command-batch record.
    Batch(Box<MachineCommandBatchRecord>),
    /// One persistent sparse-Merkle node.
    CommandIndexNode(MachineCommandIndexNode),
}

/// Maximum canonical encoded size of one independently persisted command-
/// archive object. Providers must apply this bound before typed decoding and
/// Core applies it again before accepting an object's identity.
pub const MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES: usize = 64 * 1024 * 1024;

impl MachineCommandArchiveObject {
    /// Verify and return the object's domain-separated content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the object exceeds the canonical size bound or is
    /// not valid content-addressed archive authority.
    pub fn identity(&self) -> Result<String> {
        if crate::canonical_bytes(self)?.len() > MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES {
            return Err(CoreError::Validation(format!(
                "Machine command archive object exceeds {MAX_MACHINE_COMMAND_ARCHIVE_OBJECT_BYTES} canonical bytes"
            )));
        }
        match self {
            Self::Segment(segment) => {
                segment.verify()?;
                Ok(segment.header.segment_id.clone())
            }
            Self::Entry(entry) => entry.identity(),
            Self::Batch(batch) => {
                batch.verify()?;
                Ok(batch.batch_receipt_id.clone())
            }
            Self::CommandIndexNode(node) => Ok(node.identity()?.to_owned()),
        }
    }

    /// Verify this immutable object's content identity and closed shape.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is oversized, malformed, or has a
    /// mismatched content identity.
    pub fn verify(&self) -> Result<()> {
        self.identity().map(|_| ())
    }
}

fn insert_command_index_node(
    nodes: &mut BTreeMap<String, MachineCommandIndexNode>,
    node: MachineCommandIndexNode,
) -> Result<()> {
    let id = node.identity()?.to_owned();
    match nodes.get(&id) {
        Some(existing) if existing != &node => Err(CoreError::IdentityMismatch(format!(
            "command index node {id} has conflicting content"
        ))),
        Some(_) => Ok(()),
        None => {
            nodes.insert(id, node);
            Ok(())
        }
    }
}

/// Resolve one O(256) membership/non-membership proof from immutable sparse nodes.
///
/// # Errors
///
/// Returns an error when the root or command identity is malformed, the loader
/// omits or substitutes a required node, or the resolved path is inconsistent.
pub fn resolve_machine_command_index_proof(
    root: &str,
    command_id: &str,
    mut load: impl FnMut(&str) -> Result<Option<MachineCommandIndexNode>>,
) -> Result<MachineCommandIndexProof> {
    if !is_sha256_id(root) {
        return Err(CoreError::Validation(
            "command index root is malformed".to_owned(),
        ));
    }
    validate_identity("command ID", command_id)?;
    let key = command_index_key(command_id)?;
    let empty = command_index_empty_hashes();
    let mut current = root.to_owned();
    let mut siblings_root_to_leaf = Vec::with_capacity(COMMAND_INDEX_DEPTH);
    let mut depth = 0_usize;
    while depth < COMMAND_INDEX_DEPTH {
        if current == empty[depth] {
            current.clone_from(&empty[COMMAND_INDEX_DEPTH]);
            break;
        }
        let node = load(&current)?.ok_or_else(|| {
            CoreError::NotFound(format!("command index node {current} is unavailable"))
        })?;
        if node.identity()? != current {
            return Err(CoreError::IdentityMismatch(
                "command index resolver returned the wrong node".to_owned(),
            ));
        }
        let MachineCommandIndexNode::Branch {
            depth: node_depth,
            left,
            right,
            ..
        } = node
        else {
            return Err(CoreError::IdentityMismatch(
                "command index resolver reached a leaf before depth 256".to_owned(),
            ));
        };
        if usize::from(node_depth) != depth {
            return Err(CoreError::IdentityMismatch(
                "command index resolver returned a branch at the wrong depth".to_owned(),
            ));
        }
        if command_index_bit(&key, depth) {
            siblings_root_to_leaf.push(left);
            current = right;
        } else {
            siblings_root_to_leaf.push(right);
            current = left;
        }
        depth += 1;
    }

    let value = if current == empty[COMMAND_INDEX_DEPTH] {
        None
    } else {
        let node = load(&current)?.ok_or_else(|| {
            CoreError::NotFound(format!("command index leaf {current} is unavailable"))
        })?;
        if node.identity()? != current {
            return Err(CoreError::IdentityMismatch(
                "command index resolver returned the wrong leaf".to_owned(),
            ));
        }
        let MachineCommandIndexNode::Member {
            key_hash,
            command_id: occupied_command_id,
            value,
            ..
        } = node
        else {
            return Err(CoreError::IdentityMismatch(
                "command index resolver reached a branch at depth 256".to_owned(),
            ));
        };
        if key_hash != command_index_key_id(&key) || occupied_command_id != command_id {
            return Err(CoreError::IdentityMismatch(
                "command index key collision does not prove command identity".to_owned(),
            ));
        }
        Some(value)
    };
    siblings_root_to_leaf.reverse();
    let proof = MachineCommandIndexProof {
        proof_version: COMMAND_INDEX_PROOF_VERSION.to_owned(),
        command_id: command_id.to_owned(),
        empty_depth: value.is_none().then_some(
            u16::try_from(depth).map_err(|error| CoreError::Validation(error.to_string()))?,
        ),
        value,
        siblings: siblings_root_to_leaf,
    };
    proof.verify(root)?;
    Ok(proof)
}

impl MachineArchivedCommandProof {
    /// Current archived-command proof schema.
    pub const VERSION: &'static str = "cymule.archived-command-proof/1";

    fn verify_entry(&self) -> Result<()> {
        if self.proof_version != Self::VERSION {
            return Err(CoreError::Validation(
                "archived command proof has an unsupported version".to_owned(),
            ));
        }
        self.segment.verify()?;
        self.entry.verify_shape()?;
        if self.entry_index >= self.segment.entry_count {
            return Err(CoreError::Validation(
                "archived command proof entry index exceeds its segment".to_owned(),
            ));
        }
        let expected_sequence = self
            .segment
            .parent_count
            .checked_add(self.entry_index)
            .and_then(|value| value.checked_add(1))
            .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                CoreError::Validation("archived command proof sequence overflowed".to_owned())
            })?;
        if self.entry.admission.sequence != expected_sequence
            || (self.entry_index == 0
                && self.entry.admission.parent_admission != self.segment.parent_admission_head)
        {
            return Err(CoreError::IdentityMismatch(
                "archived command proof entry does not match its segment position".to_owned(),
            ));
        }
        let mut digest = command_archive_leaf_digest(&self.entry, &self.command_index_update)?;
        let mut index = usize::try_from(self.entry_index)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let mut width = usize::try_from(self.segment.entry_count)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        for sibling in &self.merkle_path {
            if width <= 1 {
                return Err(CoreError::Validation(
                    "archived command proof has a redundant Merkle sibling".to_owned(),
                ));
            }
            if !is_sha256_id(&sibling.digest) {
                return Err(CoreError::Validation(
                    "archived command proof contains a malformed sibling".to_owned(),
                ));
            }
            let expected_side = if index % 2 == 0 {
                MerkleSiblingSide::Right
            } else {
                MerkleSiblingSide::Left
            };
            if sibling.side != expected_side
                || (index % 2 == 0 && index + 1 == width && sibling.digest != digest)
            {
                return Err(CoreError::Validation(
                    "archived command proof path does not match its entry index".to_owned(),
                ));
            }
            digest = match sibling.side {
                MerkleSiblingSide::Left => command_archive_merkle_parent(&sibling.digest, &digest)?,
                MerkleSiblingSide::Right => {
                    command_archive_merkle_parent(&digest, &sibling.digest)?
                }
            };
            index /= 2;
            width = width.div_ceil(2);
        }
        if width != 1 || digest != self.segment.entries_root {
            return Err(CoreError::IdentityMismatch(
                "archived command proof does not reach its segment root".to_owned(),
            ));
        }

        Ok(())
    }
}

type CommandIndexNodeUpdates = BTreeMap<(usize, [u8; 32]), String>;

fn command_index_entry_value(
    entry: &MachineCommandArchiveEntry,
) -> Result<MachineCommandIndexValue> {
    Ok(MachineCommandIndexValue {
        admission_id: entry.admission.admission_id.clone(),
        archive_entry_digest: entry.leaf_digest()?,
    })
}

fn apply_command_index_updates(
    parent_root: &str,
    entries: &[MachineCommandArchiveEntry],
    updates: &[MachineCommandIndexProof],
) -> Result<String> {
    if entries.len() != updates.len() || !is_sha256_id(parent_root) {
        return Err(CoreError::Validation(
            "command index update batch has a malformed shape".to_owned(),
        ));
    }
    let mut current = parent_root.to_owned();
    for (entry, update) in entries.iter().zip(updates) {
        if update.command_id != entry.admission.command_id || update.value.is_some() {
            return Err(CoreError::IdentityMismatch(
                "command index update does not prove absence for its archive entry".to_owned(),
            ));
        }
        update.verify(&current)?;
        current = update.root_with_value(Some(&command_index_entry_value(entry)?))?;
    }
    Ok(current)
}

fn command_index_prefix(mut key: [u8; 32], depth: usize) -> [u8; 32] {
    if depth >= COMMAND_INDEX_DEPTH {
        return key;
    }
    let full_bytes = depth / 8;
    let remaining_bits = depth % 8;
    if remaining_bits == 0 {
        for byte in &mut key[full_bytes..] {
            *byte = 0;
        }
    } else {
        key[full_bytes] &= u8::MAX << (8 - remaining_bits);
        for byte in &mut key[full_bytes + 1..] {
            *byte = 0;
        }
    }
    key
}

fn command_index_sibling_address(key: &[u8; 32], parent_depth: usize) -> (usize, [u8; 32]) {
    let mut sibling = *key;
    sibling[parent_depth / 8] ^= 1 << (7 - parent_depth % 8);
    let child_depth = parent_depth + 1;
    (child_depth, command_index_prefix(sibling, child_depth))
}

fn rebase_command_index_proof(
    proof: &MachineCommandIndexProof,
    parent_root: &str,
    result_root: &str,
    nodes: &CommandIndexNodeUpdates,
) -> Result<MachineCommandIndexProof> {
    proof.verify(parent_root)?;
    let key = command_index_key(&proof.command_id)?;
    let mut rebased = proof.materialized()?;
    for (level, sibling) in rebased.siblings.iter_mut().enumerate() {
        let parent_depth = COMMAND_INDEX_DEPTH - level - 1;
        if let Some(updated) = nodes.get(&command_index_sibling_address(&key, parent_depth)) {
            sibling.clone_from(updated);
        }
    }
    let rebased = rebased.compress_nonmembership()?;
    rebased.verify(result_root)?;
    Ok(rebased)
}

fn record_command_index_member_update(
    proof: &MachineCommandIndexProof,
    value: &MachineCommandIndexValue,
) -> Result<(CommandIndexNodeUpdates, String)> {
    let proof = proof.materialized()?;
    let key = command_index_key(&proof.command_id)?;
    let mut nodes = CommandIndexNodeUpdates::new();
    let mut node = command_index_member_leaf(&key, &proof.command_id, value)?;
    nodes.insert((COMMAND_INDEX_DEPTH, key), node.clone());
    for (level, sibling) in proof.siblings.iter().enumerate() {
        let depth = COMMAND_INDEX_DEPTH - level - 1;
        node = if command_index_bit(&key, depth) {
            command_index_node(depth, sibling, &node)?
        } else {
            command_index_node(depth, &node, sibling)?
        };
        nodes.insert((depth, command_index_prefix(key, depth)), node.clone());
    }
    Ok((nodes, node))
}

fn sequential_command_index_updates(
    parent_root: &str,
    entries: &[MachineCommandArchiveEntry],
    base_proofs: &[MachineCommandIndexProof],
) -> Result<(
    Vec<MachineCommandIndexProof>,
    CommandIndexNodeUpdates,
    String,
)> {
    if entries.len() != base_proofs.len() {
        return Err(CoreError::Validation(
            "command index compaction proofs do not match archive entries".to_owned(),
        ));
    }
    let mut current = parent_root.to_owned();
    let mut nodes = CommandIndexNodeUpdates::new();
    let mut sequential = Vec::with_capacity(entries.len());
    for (entry, base_proof) in entries.iter().zip(base_proofs) {
        if base_proof.command_id != entry.admission.command_id || base_proof.value.is_some() {
            return Err(CoreError::IdentityMismatch(
                "hot command non-membership proof does not match its archive entry".to_owned(),
            ));
        }
        let update = rebase_command_index_proof(base_proof, parent_root, &current, &nodes)?;
        let value = command_index_entry_value(entry)?;
        let (new_nodes, result_root) = record_command_index_member_update(&update, &value)?;
        nodes.extend(new_nodes);
        current = result_root;
        sequential.push(update);
    }
    Ok((sequential, nodes, current))
}

#[derive(serde::Serialize)]
struct CommandArchiveLeafPreimage<'a> {
    entry: &'a MachineCommandArchiveEntry,
    command_index_update: &'a MachineCommandIndexProof,
}

fn command_archive_leaf_digest(
    entry: &MachineCommandArchiveEntry,
    update: &MachineCommandIndexProof,
) -> Result<String> {
    content_id(
        "cymule.command-archive-segment-leaf/2",
        &CommandArchiveLeafPreimage {
            entry,
            command_index_update: update,
        },
    )
}

fn command_archive_merkle_root(
    entries: &[MachineCommandArchiveEntry],
    updates: &[MachineCommandIndexProof],
) -> Result<String> {
    if entries.len() != updates.len() {
        return Err(CoreError::Validation(
            "command archive Merkle inputs differ in length".to_owned(),
        ));
    }
    if entries.is_empty() {
        return command_archive_empty_root();
    }
    let leaves = entries
        .iter()
        .zip(updates)
        .map(|(entry, update)| command_archive_leaf_digest(entry, update))
        .collect::<Result<Vec<_>>>()?;
    command_archive_merkle_reduce(leaves)
}

fn command_archive_empty_root() -> Result<String> {
    content_id(MACHINE_COMMAND_ARCHIVE_NODE_DOMAIN, &Vec::<String>::new())
}

fn command_archive_merkle_reduce(mut level: Vec<String>) -> Result<String> {
    while level.len() > 1 {
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        for pair in level.chunks(2) {
            let right = pair.get(1).unwrap_or(&pair[0]);
            next.push(command_archive_merkle_parent(&pair[0], right)?);
        }
        level = next;
    }
    level
        .pop()
        .ok_or_else(|| CoreError::Validation("command archive Merkle tree is empty".to_owned()))
}

fn command_archive_merkle_parent(left: &str, right: &str) -> Result<String> {
    content_id(MACHINE_COMMAND_ARCHIVE_NODE_DOMAIN, &(left, right))
}

fn command_archive_merkle_path(
    entries: &[MachineCommandArchiveEntry],
    updates: &[MachineCommandIndexProof],
    entry_index: usize,
) -> Result<Vec<CommandArchiveMerkleSibling>> {
    if entry_index >= entries.len() || entries.len() != updates.len() {
        return Err(CoreError::NotFound(format!(
            "archive entry index {entry_index} does not exist"
        )));
    }
    let mut level = entries
        .iter()
        .zip(updates)
        .map(|(entry, update)| command_archive_leaf_digest(entry, update))
        .collect::<Result<Vec<_>>>()?;
    let mut index = entry_index;
    let mut path = Vec::new();
    while level.len() > 1 {
        let (sibling_index, side) = if index.is_multiple_of(2) {
            ((index + 1).min(level.len() - 1), MerkleSiblingSide::Right)
        } else {
            (index - 1, MerkleSiblingSide::Left)
        };
        path.push(CommandArchiveMerkleSibling {
            side,
            digest: level[sibling_index].clone(),
        });
        let mut next = Vec::with_capacity(level.len().div_ceil(2));
        for pair in level.chunks(2) {
            let right = pair.get(1).unwrap_or(&pair[0]);
            next.push(command_archive_merkle_parent(&pair[0], right)?);
        }
        level = next;
        index /= 2;
    }
    Ok(path)
}

/// Ordered command-admission hash-chain generation.
pub const COMMAND_ADMISSION_VERSION: &str = "cymule.command-admission/3";
/// Ordered atomic Machine command-batch manifest generation.
pub const MACHINE_COMMAND_BATCH_VERSION: &str = "cymule.command-batch/1";
/// Terminal atomic Machine command-batch receipt generation.
pub const MACHINE_COMMAND_BATCH_RECEIPT_VERSION: &str = "cymule.command-batch-receipt/1";
/// Maximum commands in one atomic batch manifest.
pub const MAX_MACHINE_COMMAND_BATCH_MEMBERS: usize = 32;
/// Maximum proposed plus command-required Plan identities in one batch.
pub const MAX_MACHINE_COMMAND_BATCH_PLANS: usize =
    pinned::MAX_MACHINE_MATERIAL_PLANS + 2 * MAX_MACHINE_COMMAND_BATCH_MEMBERS;
/// Maximum proposed plus command-required Artifact references in one batch.
pub const MAX_MACHINE_COMMAND_BATCH_ARTIFACTS: usize =
    pinned::MAX_MACHINE_MATERIAL_ARTIFACTS + 2 * MAX_MACHINE_COMMAND_BATCH_MEMBERS;

/// One applied or conflicting command admission at an exact Projection frontier.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandAdmission {
    /// Admission schema version.
    pub admission_version: String,
    /// Content-addressed admission identity.
    pub admission_id: String,
    /// One-based position in the Machine admission chain.
    pub sequence: u64,
    /// Previous admission identity, absent only at genesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_admission: Option<String>,
    /// Exact admitted command identity.
    pub command_id: String,
    /// Digest of the exact admitted command envelope.
    pub semantic_hash: String,
    /// Digest of the complete private Command record and receipt.
    pub command_record_digest: String,
    /// Owning atomic batch identity.
    pub batch_id: String,
    /// Zero-based position in the owning batch.
    pub batch_position: u32,
    /// Exact owning batch length.
    pub batch_len: u32,
    /// Projection digest immediately before admission.
    pub before_projection_digest: String,
    /// Projection digest immediately after admission.
    pub after_projection_digest: String,
    /// Applied or conflict disposition.
    pub status: CommandReceiptStatus,
    /// Complete applied Event batch; conflicts carry an empty batch.
    pub event_ids: Vec<String>,
}

#[derive(serde::Serialize)]
struct CommandAdmissionPreimage<'a> {
    admission_version: &'static str,
    sequence: u64,
    parent_admission: &'a Option<String>,
    command_id: &'a str,
    semantic_hash: &'a str,
    command_record_digest: &'a str,
    batch_id: &'a str,
    batch_position: u32,
    batch_len: u32,
    before_projection_digest: &'a str,
    after_projection_digest: &'a str,
    status: CommandReceiptStatus,
    event_ids: &'a [String],
}

#[derive(Clone, Copy)]
struct CommandAdmissionParent<'a> {
    sequence: u64,
    admission_id: &'a str,
}

impl<'a> From<&'a CommandAdmission> for CommandAdmissionParent<'a> {
    fn from(value: &'a CommandAdmission) -> Self {
        Self {
            sequence: value.sequence,
            admission_id: &value.admission_id,
        }
    }
}

fn next_command_admission_sequence(parent: Option<CommandAdmissionParent<'_>>) -> Result<u64> {
    parent.map_or(Ok(1), |value| {
        value
            .sequence
            .checked_add(1)
            .filter(|sequence| *sequence <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                CoreError::Validation(
                    "command admission sequence exceeds the exact integer range".to_owned(),
                )
            })
    })
}

impl CommandAdmission {
    fn new(
        parent: Option<CommandAdmissionParent<'_>>,
        record: &CommandRecord,
        before_projection_digest: String,
        after_projection_digest: String,
    ) -> Result<Self> {
        let sequence = next_command_admission_sequence(parent)?;
        let parent_admission = parent.map(|value| value.admission_id.to_owned());
        let command_record_digest = canonical_digest(record)?;
        let event_ids = record.receipt.event_ids.clone();
        let mut admission = Self {
            admission_version: COMMAND_ADMISSION_VERSION.to_owned(),
            admission_id: String::new(),
            sequence,
            parent_admission,
            command_id: record.envelope.command_id.clone(),
            semantic_hash: record.semantic_hash.clone(),
            command_record_digest,
            batch_id: record.batch_id.clone(),
            batch_position: record.batch_position,
            batch_len: record.batch_len,
            before_projection_digest,
            after_projection_digest,
            status: record.receipt.status,
            event_ids,
        };
        admission.admission_id = admission.expected_id()?;
        admission.verify(parent)?;
        Ok(admission)
    }

    fn verify(&self, parent: Option<CommandAdmissionParent<'_>>) -> Result<()> {
        let expected_sequence = next_command_admission_sequence(parent)?;
        let expected_parent = parent.map(|value| value.admission_id);
        if self.admission_version != COMMAND_ADMISSION_VERSION
            || self.sequence != expected_sequence
            || self.sequence == 0
            || self.sequence > crate::MAX_EXACT_INTEGER
            || self.parent_admission.as_deref() != expected_parent
            || self.command_id.is_empty()
            || !is_canonical_digest(&self.semantic_hash)
            || !is_canonical_digest(&self.command_record_digest)
            || !is_sha256_id(&self.batch_id)
            || self.batch_len == 0
            || self.batch_position >= self.batch_len
            || !is_canonical_digest(&self.before_projection_digest)
            || !is_canonical_digest(&self.after_projection_digest)
            || match self.status {
                CommandReceiptStatus::Applied => {
                    self.event_ids.is_empty()
                        || self.event_ids.len() > 2
                        || self.event_ids.iter().any(|id| !is_sha256_id(id))
                }
                CommandReceiptStatus::Conflict => {
                    !self.event_ids.is_empty()
                        || self.before_projection_digest != self.after_projection_digest
                }
            }
        {
            return Err(CoreError::Validation(format!(
                "command admission {} has malformed chain or frontier evidence",
                self.admission_id
            )));
        }
        let expected_id = self.expected_id()?;
        if self.admission_id != expected_id {
            return Err(CoreError::IdentityMismatch(format!(
                "command admission {} does not match {expected_id}",
                self.admission_id
            )));
        }
        Ok(())
    }

    fn expected_id(&self) -> Result<String> {
        content_id(
            COMMAND_ADMISSION_VERSION,
            &CommandAdmissionPreimage {
                admission_version: COMMAND_ADMISSION_VERSION,
                sequence: self.sequence,
                parent_admission: &self.parent_admission,
                command_id: &self.command_id,
                semantic_hash: &self.semantic_hash,
                command_record_digest: &self.command_record_digest,
                batch_id: &self.batch_id,
                batch_position: self.batch_position,
                batch_len: self.batch_len,
                before_projection_digest: &self.before_projection_digest,
                after_projection_digest: &self.after_projection_digest,
                status: self.status,
                event_ids: &self.event_ids,
            },
        )
    }
}

/// One command position bound into an atomic batch record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandBatchMember {
    /// Zero-based exact position.
    pub position: u32,
    /// Exact command identity.
    pub command_id: String,
    /// Digest of the command intent excluding its batch-derived precondition.
    pub intent_hash: String,
    /// Digest of the final exact command envelope.
    pub semantic_hash: String,
}

/// Complete immutable material proposed by one exact framework command.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandBatchMaterialSource {
    /// Exact framework command that owns this material proposal.
    pub source_command_id: String,
    /// Complete ordered Plan identities included in the material digest.
    pub plan_ids: Vec<String>,
    /// Complete ordered Artifact references included in the material digest.
    pub artifacts: Vec<ArtifactRef>,
}

impl MachineCommandBatchMaterialSource {
    /// Verify the exact source identity and complete bounded material references.
    ///
    /// # Errors
    ///
    /// Returns an error when the proposal is empty, oversized, repeated,
    /// unordered, or contains an invalid identity or typed Artifact reference.
    pub fn verify(&self) -> Result<()> {
        validate_identity("Machine material source command", &self.source_command_id)?;
        if (self.plan_ids.is_empty() && self.artifacts.is_empty())
            || self.plan_ids.len() > pinned::MAX_MACHINE_MATERIAL_PLANS
            || self.artifacts.len() > pinned::MAX_MACHINE_MATERIAL_ARTIFACTS
            || !self.plan_ids.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .artifacts
                .windows(2)
                .all(|pair| pair[0].artifact_id < pair[1].artifact_id)
        {
            return Err(CoreError::Validation(
                "Machine batch material source is malformed or exceeds its closed item bounds"
                    .to_owned(),
            ));
        }
        for plan_id in &self.plan_ids {
            crate::validate_content_id("batch material Plan", plan_id)?;
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        Ok(())
    }
}

/// Persistent all-or-none authority for one atomic command batch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCommandBatchRecord {
    /// Batch record generation.
    pub batch_version: String,
    /// Parent-bound ordered manifest identity.
    pub batch_id: String,
    /// Exact source Machine authority root frozen by the batch manifest.
    pub parent_authority_root: String,
    /// Exact linear Machine authority immediately before terminal admission.
    /// A paged batch may admit after unrelated Runs have advanced this root.
    pub admission_parent_authority_root: String,
    /// Ordered member identities and hashes.
    pub members: Vec<MachineCommandBatchMember>,
    /// Optional material admission digest bound by the batch.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub material_digest: Option<String>,
    /// Complete source of the material digest, or null when no material was proposed.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub material_source: Option<MachineCommandBatchMaterialSource>,
    /// Ordered Plan identities required by this batch.
    pub plan_ids: Vec<String>,
    /// Ordered Artifact references required by this batch.
    pub artifacts: Vec<ArtifactRef>,
    /// Complete ordered receipts.
    pub receipts: Vec<CommandReceipt>,
    /// Flattened ordered Event identities across every receipt.
    pub event_ids: Vec<String>,
    /// Exact result Machine authority root.
    pub result_authority_root: String,
    /// Content identity of the complete terminal batch record.
    pub batch_receipt_id: String,
}

#[derive(serde::Serialize)]
struct MachineCommandBatchManifestPreimage<'a> {
    batch_version: &'static str,
    parent_authority_root: &'a str,
    members: Vec<(u32, &'a str, &'a str)>,
    material_digest: Option<&'a str>,
    material_source: Option<&'a MachineCommandBatchMaterialSource>,
    plan_ids: &'a [String],
    artifacts: &'a [ArtifactRef],
}

#[derive(serde::Serialize)]
struct MachineCommandBatchReceiptPreimage<'a> {
    receipt_version: &'static str,
    batch_id: &'a str,
    admission_parent_authority_root: &'a str,
    material_source: Option<&'a MachineCommandBatchMaterialSource>,
    members: &'a [MachineCommandBatchMember],
    receipts: &'a [CommandReceipt],
    event_ids: &'a [String],
    result_authority_root: &'a str,
    plan_ids: &'a [String],
    artifacts: &'a [ArtifactRef],
}

pub(crate) fn machine_command_batch_id(
    parent_authority_root: &str,
    members: &[MachineCommandBatchMember],
    material_digest: Option<&str>,
    material_source: Option<&MachineCommandBatchMaterialSource>,
    plan_ids: &[String],
    artifacts: &[ArtifactRef],
) -> Result<String> {
    if !is_canonical_digest(parent_authority_root) {
        return Err(CoreError::Validation(
            "Machine batch parent authority root is malformed".to_owned(),
        ));
    }
    content_id(
        MACHINE_COMMAND_BATCH_VERSION,
        &MachineCommandBatchManifestPreimage {
            batch_version: MACHINE_COMMAND_BATCH_VERSION,
            parent_authority_root,
            members: members
                .iter()
                .map(|member| {
                    (
                        member.position,
                        member.command_id.as_str(),
                        member.intent_hash.as_str(),
                    )
                })
                .collect(),
            material_digest,
            material_source,
            plan_ids,
            artifacts,
        },
    )
}

pub(crate) fn command_intent_hash(envelope: &CommandEnvelope) -> Result<String> {
    canonical_digest(&(
        envelope.command_version.as_str(),
        envelope.command_id.as_str(),
        envelope.actor.as_str(),
        envelope.run_id.as_str(),
        &envelope.command,
    ))
}

pub(crate) fn command_material_membership(
    command: &Command,
) -> Result<(Vec<String>, Vec<ArtifactRef>)> {
    let binding = |artifact_id: &str| ArtifactRef {
        identity_version: crate::ARTIFACT_IDENTITY_VERSION.to_owned(),
        artifact_id: artifact_id.to_owned(),
        kind: crate::EXECUTION_BINDING_ARTIFACT_KIND.to_owned(),
    };
    let mut plans = Vec::new();
    let mut artifacts = Vec::new();
    match command {
        Command::StartRun {
            plan_id,
            binding_context,
            input,
            ..
        } => {
            plans.push(plan_id.clone());
            artifacts.extend([binding(binding_context), input.clone()]);
        }
        Command::ProposeEffect {
            args,
            execution_binding,
            ..
        } => artifacts.extend([args.clone(), execution_binding.clone()]),
        Command::UpdateBinding { binding_context } => artifacts.push(binding(binding_context)),
        Command::MigrateRun {
            from_plan,
            to_plan,
            from_binding,
            to_binding,
            ..
        } => {
            plans.extend([from_plan.clone(), to_plan.clone()]);
            artifacts.extend([binding(from_binding), binding(to_binding)]);
        }
        Command::CompleteRun { result } => artifacts.extend(result.iter().cloned()),
        Command::FailRun { failure } => artifacts.push(failure.detail.clone()),
        Command::CancelRun { reason } => artifacts.push(reason.clone()),
        Command::BeginAttempt { .. }
        | Command::YieldAttempt { .. }
        | Command::AdvanceEpoch
        | Command::OpenScope { .. }
        | Command::TransitionEffect { .. }
        | Command::CommitScope { .. }
        | Command::AbortScope { .. }
        | Command::RecordFact { .. } => {}
    }
    plans.sort();
    plans.dedup();
    artifacts.sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
    artifacts.dedup_by(|left, right| left.artifact_id == right.artifact_id);
    for plan_id in &plans {
        crate::validate_content_id("batch Plan", plan_id)?;
    }
    for artifact in &artifacts {
        artifact.validate()?;
    }
    Ok((plans, artifacts))
}

fn single_command_batch_metadata(
    parent_authority_root: &str,
    envelope: &CommandEnvelope,
    semantic_hash: &str,
) -> Result<(String, u32, u32)> {
    let material_digest = match &envelope.command {
        Command::StartRun {
            material_digest, ..
        } => Some(material_digest.clone()),
        _ => None,
    };
    let member = MachineCommandBatchMember {
        position: 0,
        command_id: envelope.command_id.clone(),
        intent_hash: command_intent_hash(envelope)?,
        semantic_hash: semantic_hash.to_owned(),
    };
    let (plan_ids, artifacts) = command_material_membership(&envelope.command)?;
    let material_source = single_command_material_source(envelope, &plan_ids, &artifacts);
    Ok((
        machine_command_batch_id(
            parent_authority_root,
            &[member],
            material_digest.as_deref(),
            material_source.as_ref(),
            &plan_ids,
            &artifacts,
        )?,
        0,
        1,
    ))
}

fn build_single_command_batch_record(
    parent_authority_root: &str,
    result_authority_root: &str,
    record: &CommandRecord,
) -> Result<MachineCommandBatchRecord> {
    let material_digest = match &record.envelope.command {
        Command::StartRun {
            material_digest, ..
        } => Some(material_digest.clone()),
        _ => None,
    };
    let member = MachineCommandBatchMember {
        position: 0,
        command_id: record.envelope.command_id.clone(),
        intent_hash: command_intent_hash(&record.envelope)?,
        semantic_hash: record.semantic_hash.clone(),
    };
    let (plan_ids, artifacts) = command_material_membership(&record.envelope.command)?;
    let material_source = single_command_material_source(&record.envelope, &plan_ids, &artifacts);
    if record.batch_position != 0
        || record.batch_len != 1
        || record.batch_id
            != machine_command_batch_id(
                parent_authority_root,
                std::slice::from_ref(&member),
                material_digest.as_deref(),
                material_source.as_ref(),
                &plan_ids,
                &artifacts,
            )?
    {
        return Err(CoreError::IdentityMismatch(
            "single command record has the wrong batch authority".to_owned(),
        ));
    }
    let mut batch = MachineCommandBatchRecord {
        batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
        batch_id: record.batch_id.clone(),
        parent_authority_root: parent_authority_root.to_owned(),
        admission_parent_authority_root: parent_authority_root.to_owned(),
        members: vec![member],
        material_digest,
        material_source,
        plan_ids,
        artifacts,
        receipts: vec![record.receipt.clone()],
        event_ids: record.receipt.event_ids.clone(),
        result_authority_root: result_authority_root.to_owned(),
        batch_receipt_id: String::new(),
    };
    batch.batch_receipt_id = batch.expected_receipt_id()?;
    batch.verify()?;
    Ok(batch)
}

fn single_command_material_source(
    envelope: &CommandEnvelope,
    plan_ids: &[String],
    artifacts: &[ArtifactRef],
) -> Option<MachineCommandBatchMaterialSource> {
    matches!(envelope.command, Command::StartRun { .. }).then(|| {
        MachineCommandBatchMaterialSource {
            source_command_id: envelope.command_id.clone(),
            plan_ids: plan_ids.to_vec(),
            artifacts: artifacts.to_vec(),
        }
    })
}

impl MachineCommandBatchRecord {
    fn expected_receipt_id(&self) -> Result<String> {
        content_id(
            MACHINE_COMMAND_BATCH_RECEIPT_VERSION,
            &MachineCommandBatchReceiptPreimage {
                receipt_version: MACHINE_COMMAND_BATCH_RECEIPT_VERSION,
                batch_id: &self.batch_id,
                admission_parent_authority_root: &self.admission_parent_authority_root,
                material_source: self.material_source.as_ref(),
                members: &self.members,
                receipts: &self.receipts,
                event_ids: &self.event_ids,
                result_authority_root: &self.result_authority_root,
                plan_ids: &self.plan_ids,
                artifacts: &self.artifacts,
            },
        )
    }

    /// Verify the complete manifest, member receipts, and terminal identity.
    ///
    /// # Errors
    ///
    /// Returns an error when positions, hashes, receipts, Events, material, or
    /// parent/result authority and content identities are inconsistent.
    pub fn verify(&self) -> Result<()> {
        if self.batch_version != MACHINE_COMMAND_BATCH_VERSION
            || (self.members.is_empty() && self.material_source.is_none())
            || self.material_digest.is_some() != self.material_source.is_some()
            || self.members.len() > MAX_MACHINE_COMMAND_BATCH_MEMBERS
            || self.plan_ids.len() > MAX_MACHINE_COMMAND_BATCH_PLANS
            || self.artifacts.len() > MAX_MACHINE_COMMAND_BATCH_ARTIFACTS
            || self.members.len() != self.receipts.len()
            || !is_canonical_digest(&self.parent_authority_root)
            || !is_canonical_digest(&self.admission_parent_authority_root)
            || !is_canonical_digest(&self.result_authority_root)
            || self
                .material_digest
                .as_ref()
                .is_some_and(|digest| crate::validate_content_id("batch material", digest).is_err())
            || !self.plan_ids.windows(2).all(|pair| pair[0] < pair[1])
            || !self
                .artifacts
                .windows(2)
                .all(|pair| pair[0].artifact_id < pair[1].artifact_id)
        {
            return Err(CoreError::Validation(
                "Machine command batch record has malformed fixed authority".to_owned(),
            ));
        }
        self.verify_material_source()?;
        let mut command_ids = BTreeSet::new();
        for (index, (member, receipt)) in self.members.iter().zip(&self.receipts).enumerate() {
            validate_identity("batch command", &member.command_id)?;
            if usize::try_from(member.position).ok() != Some(index)
                || !command_ids.insert(member.command_id.clone())
                || !is_canonical_digest(&member.intent_hash)
                || !is_canonical_digest(&member.semantic_hash)
                || receipt.command_id != member.command_id
                || (receipt.status == CommandReceiptStatus::Conflict
                    && (self.members.len() != 1 || !receipt.event_ids.is_empty()))
                || (receipt.status == CommandReceiptStatus::Applied
                    && (receipt.event_ids.is_empty() || receipt.event_ids.len() > 2))
            {
                return Err(CoreError::IdentityMismatch(
                    "Machine command batch member order or receipt changed".to_owned(),
                ));
            }
        }
        let event_ids = self
            .receipts
            .iter()
            .flat_map(|receipt| receipt.event_ids.iter().cloned())
            .collect::<Vec<_>>();
        let mut unique_events = BTreeSet::new();
        for event_id in &event_ids {
            crate::validate_content_id("batch Event", event_id)?;
            if !unique_events.insert(event_id) {
                return Err(CoreError::IdentityMismatch(
                    "batch repeats an Event identity".to_owned(),
                ));
            }
        }
        for plan_id in &self.plan_ids {
            crate::validate_content_id("batch Plan", plan_id)?;
        }
        for artifact in &self.artifacts {
            artifact.validate()?;
        }
        if event_ids != self.event_ids
            || self.batch_id
                != machine_command_batch_id(
                    &self.parent_authority_root,
                    &self.members,
                    self.material_digest.as_deref(),
                    self.material_source.as_ref(),
                    &self.plan_ids,
                    &self.artifacts,
                )?
            || self.batch_receipt_id != self.expected_receipt_id()?
        {
            return Err(CoreError::IdentityMismatch(
                "Machine command batch content identity changed".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_material_source(&self) -> Result<()> {
        let Some(source) = &self.material_source else {
            return Ok(());
        };
        source.verify()?;
        if source.plan_ids.iter().any(|id| !self.plan_ids.contains(id))
            || source
                .artifacts
                .iter()
                .any(|reference| !self.artifacts.contains(reference))
            || (self.members.is_empty()
                && (source.plan_ids != self.plan_ids || source.artifacts != self.artifacts))
        {
            return Err(CoreError::Validation(
                "Machine batch material source is not its complete bounded proposal".to_owned(),
            ));
        }
        Ok(())
    }

    fn admits_material(&self) -> bool {
        self.members.is_empty()
            || self
                .receipts
                .iter()
                .any(|receipt| receipt.status == CommandReceiptStatus::Applied)
    }
}

impl MachineCommandBatchRecord {
    /// Verify one complete archive entry against its exact retained batch slot.
    ///
    /// # Errors
    ///
    /// Returns an error when the batch, entry, member intent, exact command
    /// hash, position, length, receipt, or ordered Event identities disagree.
    pub fn verify_entry(&self, entry: &MachineCommandArchiveEntry) -> Result<()> {
        self.verify()?;
        entry.verify()?;
        let position = usize::try_from(entry.command.batch_position)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let member = self.members.get(position).ok_or_else(|| {
            CoreError::IdentityMismatch("archived command is outside its batch".to_owned())
        })?;
        if entry.command.batch_id != self.batch_id
            || usize::try_from(entry.command.batch_len).ok() != Some(self.members.len())
            || member.command_id != entry.command.envelope.command_id
            || member.position != entry.command.batch_position
            || member.semantic_hash != entry.command.semantic_hash
            || member.intent_hash != command_intent_hash(&entry.command.envelope)?
            || self.receipts.get(position) != Some(&entry.command.receipt)
            || (self.parent_authority_root != self.admission_parent_authority_root
                && (self.members.len() != 1
                    || !matches!(
                        entry.command.envelope.command,
                        Command::CommitScope { .. }
                            | Command::AbortScope { .. }
                            | Command::FailRun { .. }
                            | Command::CancelRun { .. }
                    )))
        {
            return Err(CoreError::IdentityMismatch(
                "archived command does not match its complete batch member and receipt".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Borrowed durable-frame location re-resolved against one immutable Plan.
#[derive(Clone, Copy)]
pub struct ExecutionFrameLocation<'a> {
    /// Owning Run.
    pub run_id: &'a str,
    /// Exact immutable Plan interpreted by this frame.
    pub plan_id: &'a str,
    /// Structural invocation identity.
    pub invocation_id: &'a str,
    /// Entry-rooted invocation path.
    pub invocation_path: &'a [InvocationPathSegment],
    /// Resolved definition.
    pub definition_id: &'a str,
    /// Nested Region path.
    pub region_path: &'a [usize],
    /// Exact lexical scope.
    pub scope_id: &'a str,
    /// Next operation index.
    pub next_step: usize,
}

/// Resumable frame admission bound to the current Run execution authority.
#[derive(Clone, Copy)]
pub struct ResumableExecutionFrame<'a> {
    /// Structurally resolved frame location.
    pub location: ExecutionFrameLocation<'a>,
    /// Exact current execution-binding Artifact identity.
    pub binding_context: &'a str,
    /// Exact current Run/Continuation epoch. M1 separately validates its claim fence.
    pub epoch: u64,
}

/// Exact pending Effect set that owns one non-resumable current frame boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectBoundary {
    /// Current frame scope.
    pub scope_id: String,
    /// Complete deterministic pending Effect intent set reachable at the frame.
    pub intent_ids: BTreeSet<String>,
}

/// Durable disposition required to admit a closed current execution boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClosedBoundaryDisposition {
    /// A fenced execution claim is still held for the terminalizing CAS.
    Running,
    /// Execution yielded at an Effect release/reconciliation boundary.
    Ready,
    /// A wait owns resumption; closed-scope admission is always illegal.
    Waiting,
}

/// Whole-Continuation evidence for a closed current frame boundary.
#[derive(Clone, Copy)]
pub struct ClosedExecutionBoundary<'a> {
    /// Exact current frame authority.
    pub frame: ResumableExecutionFrame<'a>,
    /// Complete frame count in the persisted Continuation.
    pub frame_count: usize,
    /// Complete persisted scope stack.
    pub scope_stack: &'a [String],
    /// Complete active wait count.
    pub wait_count: usize,
    /// Persisted Continuation disposition.
    pub disposition: ClosedBoundaryDisposition,
    /// Whether M1 retains an execution claim; Core does not interpret its fence.
    pub has_execution_claim: bool,
}

/// Typed Core receipt proving one retained Run migration used for frame replacement.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MigrationFrameReplacementReceipt {
    /// Receipt schema version.
    pub receipt_version: String,
    /// Content-addressed receipt identity.
    pub receipt_id: String,
    /// Exact admitted migration command.
    pub command_id: String,
    /// Exact canonical `RunMigrated` Event.
    pub event_id: String,
    /// Migrated Run.
    pub run_id: String,
    /// Source Plan.
    pub from_plan: String,
    /// Target Plan.
    pub to_plan: String,
    /// Source execution binding.
    pub from_binding: String,
    /// Target execution binding.
    pub to_binding: String,
    /// Content-addressed migration safe point.
    pub safe_point_id: String,
    /// Exact target Continuation epoch.
    pub target_epoch: u64,
    /// Digest of the complete target Continuation/frame stack.
    pub target_continuation_digest: String,
    /// Exact source Run frontier observed by the migration command.
    pub observed_precondition: String,
    /// Exact target Run frontier produced by the migration command.
    pub current_precondition: String,
}

#[derive(serde::Serialize)]
struct MigrationFrameReplacementPreimage<'a> {
    receipt_version: &'static str,
    command_id: &'a str,
    event_id: &'a str,
    run_id: &'a str,
    from_plan: &'a str,
    to_plan: &'a str,
    from_binding: &'a str,
    to_binding: &'a str,
    safe_point_id: &'a str,
    target_epoch: u64,
    target_continuation_digest: &'a str,
    observed_precondition: &'a str,
    current_precondition: &'a str,
}

impl MigrationFrameReplacementReceipt {
    /// Current migration-frame replacement receipt schema.
    pub const VERSION: &'static str = "cymule.migration-frame-replacement/1";

    fn expected_id(&self) -> Result<String> {
        content_id(
            Self::VERSION,
            &MigrationFrameReplacementPreimage {
                receipt_version: Self::VERSION,
                command_id: &self.command_id,
                event_id: &self.event_id,
                run_id: &self.run_id,
                from_plan: &self.from_plan,
                to_plan: &self.to_plan,
                from_binding: &self.from_binding,
                to_binding: &self.to_binding,
                safe_point_id: &self.safe_point_id,
                target_epoch: self.target_epoch,
                target_continuation_digest: &self.target_continuation_digest,
                observed_precondition: &self.observed_precondition,
                current_precondition: &self.current_precondition,
            },
        )
    }

    /// Verify the closed receipt shape and content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any migration authority field is invalid or the
    /// receipt identity does not bind the complete payload.
    pub fn verify(&self) -> Result<()> {
        validate_migration_payload(
            &self.from_plan,
            &self.to_plan,
            &self.from_binding,
            &self.to_binding,
            &self.safe_point_id,
            self.target_epoch,
            &self.target_continuation_digest,
        )?;
        if self.receipt_version != Self::VERSION
            || !is_sha256_id(&self.event_id)
            || self.observed_precondition.is_empty()
            || self.current_precondition.is_empty()
            || self.receipt_id != self.expected_id()?
        {
            return Err(CoreError::Validation(
                "migration frame replacement receipt is malformed".to_owned(),
            ));
        }
        validate_identity("migration command ID", &self.command_id)?;
        validate_identity("migration Run ID", &self.run_id)
    }
}

const MACHINE_BASE_ID_DOMAIN: &str = "cymule.machine-base/4";
const MACHINE_PREFIX_VERSION: &str = "cymule.machine-prefix/4";

#[derive(serde::Serialize)]
struct MachinePrefixPreimage<'a> {
    prefix_version: &'static str,
    archive_head: &'a str,
    archive_count: u64,
    archive_event_count: u64,
    admission_head: Option<&'a str>,
    command_index_root: &'a str,
    projection_digest: &'a str,
    projection_root: &'a str,
}

/// One verified canonical base projection replacing a causally closed prefix.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineBaseSnapshot {
    /// Recomputable digest of the complete compacted evidence and projection.
    pub prefix_digest: String,
    /// Independent immutable archive-segment head.
    pub archive_head: String,
    /// Number of admissions covered by the archive head.
    pub archive_count: u64,
    /// Number of applied Events covered by the archive head.
    pub archive_event_count: u64,
    /// Exact `CommandAdmission` head covered by the archive.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub admission_head: Option<String>,
    /// Cumulative sparse-Merkle map root for every archived command identity.
    pub command_index_root: String,
    /// Cumulative Plan admission commitment at this cut.
    pub plan_admission_commitment: String,
    /// Cumulative Plan count at this cut.
    pub plan_count: u64,
    /// Cumulative Artifact admission commitment at this cut.
    pub artifact_admission_commitment: String,
    /// Cumulative Artifact count at this cut.
    pub artifact_count: u64,
    /// Cumulative command-batch admission commitment at this cut.
    pub batch_admission_commitment: String,
    /// Cumulative command-batch count at this cut.
    pub batch_count: u64,
    /// Projection after applying the complete compacted prefix.
    pub projection: Projection,
    /// Digest that authenticates the retained projection bytes.
    pub projection_digest: String,
    /// Incremental authenticated reducer root at this exact compacted cut.
    pub projection_root: String,
}

impl MachineBaseSnapshot {
    /// Verify the compacted base's closed evidence and reducer invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when compacted counts, archive lineage, projection
    /// authority, Run frontiers, or the base content identity is invalid.
    pub fn verify(&self) -> Result<()> {
        if !is_sha256_id(&self.prefix_digest)
            || !is_sha256_id(&self.archive_head)
            || self.archive_count > crate::MAX_EXACT_INTEGER
            || self.archive_event_count
                > self.archive_count.checked_mul(2).ok_or_else(|| {
                    CoreError::Validation("machine base Event bound overflowed".to_owned())
                })?
            || (self.archive_count == 0) != self.admission_head.is_none()
            || self
                .admission_head
                .as_deref()
                .is_some_and(|head| !is_sha256_id(head))
            || !is_sha256_id(&self.command_index_root)
            || crate::validate_content_id(
                "Machine base Plan admission commitment",
                &self.plan_admission_commitment,
            )
            .is_err()
            || crate::validate_content_id(
                "Machine base Artifact admission commitment",
                &self.artifact_admission_commitment,
            )
            .is_err()
            || self.plan_count > crate::MAX_EXACT_INTEGER
            || self.artifact_count > crate::MAX_EXACT_INTEGER
            || crate::validate_content_id(
                "Machine base batch admission commitment",
                &self.batch_admission_commitment,
            )
            .is_err()
            || self.batch_count == 0
            || self.batch_count > crate::MAX_EXACT_INTEGER
            || !is_canonical_digest(&self.projection_root)
            || (self.archive_count == 0
                && self.command_index_root != MachineCommandIndexProof::empty_root()?)
            || (self.archive_event_count == 0
                && (!self.projection.runs.is_empty()
                    || !self.projection.facts.is_empty()
                    || self.projection_root
                        != canonical_digest(&(PROJECTION_ROOT_GENESIS_DOMAIN, ()))?))
        {
            return Err(CoreError::Validation(
                "machine base snapshot has malformed prefix evidence".to_owned(),
            ));
        }
        let expected = self.projection.digest()?;
        self.projection.verify_reducer_invariants()?;
        if self.projection_digest != expected {
            return Err(CoreError::IdentityMismatch(format!(
                "machine base projection digest {} does not match {expected}",
                self.projection_digest
            )));
        }
        let expected_prefix = machine_prefix_digest(
            &self.archive_head,
            self.archive_count,
            self.archive_event_count,
            self.admission_head.as_deref(),
            &self.command_index_root,
            &expected,
            &self.projection_root,
        )?;
        if self.prefix_digest != expected_prefix {
            return Err(CoreError::IdentityMismatch(format!(
                "machine prefix digest {} does not match {expected_prefix}",
                self.prefix_digest
            )));
        }
        for run in self.projection.runs.values() {
            if !is_sha256_id(&run.last_event) {
                return Err(CoreError::Validation(format!(
                    "machine base Run {} has a malformed frontier",
                    run.run_id
                )));
            }
        }
        Ok(())
    }

    /// Verify and content-address the authenticated compacted prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the base is invalid or cannot be serialized
    /// canonically for identity derivation.
    pub fn identity(&self) -> Result<String> {
        self.verify()?;
        content_id(MACHINE_BASE_ID_DOMAIN, self)
    }

    fn event_ids(&self) -> BTreeSet<String> {
        self.projection
            .runs
            .values()
            .map(|run| run.last_event.clone())
            .collect()
    }
}

/// Content-addressed external authority for one exact compacted Machine base.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineBaseAnchor {
    /// Anchor schema version.
    pub anchor_version: String,
    /// Content-addressed anchor identity.
    pub anchor_id: String,
    /// Exact content identity of the base snapshot.
    pub base_id: String,
    /// Independent immutable archive-segment head.
    pub archive_head: String,
    /// Number of archived admissions.
    pub archive_count: u64,
    /// Number of archived applied Events.
    pub archive_event_count: u64,
    /// Cumulative complete batch count at the archived cut, including material-only batches.
    pub archive_batch_count: u64,
    /// Exact `CommandAdmission` chain head at the cut.
    #[serde(deserialize_with = "deserialize_required_nullable")]
    pub admission_head: Option<String>,
    /// Cumulative archived-command sparse-Merkle map root.
    pub command_index_root: String,
    /// Authenticated compacted-prefix digest.
    pub prefix_digest: String,
    /// Authenticated base Projection digest.
    pub projection_digest: String,
    /// Incremental authenticated reducer root at the base cut.
    pub projection_root: String,
}

#[derive(serde::Serialize)]
struct MachineBaseAnchorPreimage<'a> {
    anchor_version: &'static str,
    base_id: &'a str,
    archive_head: &'a str,
    archive_count: u64,
    archive_event_count: u64,
    archive_batch_count: u64,
    admission_head: Option<&'a str>,
    command_index_root: &'a str,
    prefix_digest: &'a str,
    projection_digest: &'a str,
    projection_root: &'a str,
}

impl MachineBaseAnchor {
    /// Current base-anchor schema.
    pub const VERSION: &'static str = "cymule.machine-base-anchor/2";

    fn from_verified_base(base: &MachineBaseSnapshot) -> Result<Self> {
        let mut anchor = Self {
            anchor_version: Self::VERSION.to_owned(),
            anchor_id: String::new(),
            base_id: content_id(MACHINE_BASE_ID_DOMAIN, base)?,
            archive_head: base.archive_head.clone(),
            archive_count: base.archive_count,
            archive_event_count: base.archive_event_count,
            archive_batch_count: base.batch_count,
            admission_head: base.admission_head.clone(),
            command_index_root: base.command_index_root.clone(),
            prefix_digest: base.prefix_digest.clone(),
            projection_digest: base.projection_digest.clone(),
            projection_root: base.projection_root.clone(),
        };
        anchor.anchor_id = anchor.expected_id()?;
        Ok(anchor)
    }

    /// Verify the anchor's closed shape and content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any anchored base/archive field is malformed or
    /// the declared anchor identity does not bind the complete payload.
    pub fn verify(&self) -> Result<()> {
        if self.anchor_version != Self::VERSION
            || !is_sha256_id(&self.anchor_id)
            || !is_sha256_id(&self.base_id)
            || !is_sha256_id(&self.archive_head)
            || self.archive_count > crate::MAX_EXACT_INTEGER
            || self.archive_batch_count == 0
            || self.archive_batch_count > crate::MAX_EXACT_INTEGER
            || self.archive_event_count
                > self.archive_count.checked_mul(2).ok_or_else(|| {
                    CoreError::Validation("Machine base anchor Event bound overflowed".to_owned())
                })?
            || (self.archive_count == 0) != self.admission_head.is_none()
            || self
                .admission_head
                .as_deref()
                .is_some_and(|head| !is_sha256_id(head))
            || !is_sha256_id(&self.command_index_root)
            || !is_sha256_id(&self.prefix_digest)
            || !is_canonical_digest(&self.projection_digest)
            || !is_canonical_digest(&self.projection_root)
            || (self.archive_count == 0
                && self.command_index_root != MachineCommandIndexProof::empty_root()?)
            || (self.archive_event_count == 0
                && (self.projection_digest != Projection::default().digest()?
                    || self.projection_root
                        != canonical_digest(&(PROJECTION_ROOT_GENESIS_DOMAIN, ()))?))
            || self.anchor_id != self.expected_id()?
        {
            return Err(CoreError::Validation(
                "Machine base anchor is malformed".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_trusted_base_fields(&self, base: &MachineBaseSnapshot) -> Result<()> {
        self.verify()?;
        if self.archive_head != base.archive_head
            || self.archive_count != base.archive_count
            || self.archive_event_count != base.archive_event_count
            || self.archive_batch_count != base.batch_count
            || self.admission_head != base.admission_head
            || self.command_index_root != base.command_index_root
            || self.prefix_digest != base.prefix_digest
            || self.projection_digest != base.projection_digest
            || self.projection_root != base.projection_root
            || base.projection.digest()? != self.projection_digest
            || content_id(MACHINE_BASE_ID_DOMAIN, base)? != self.base_id
        {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine base anchor {} does not match the supplied trusted base fields",
                self.anchor_id
            )));
        }
        base.projection.verify_reducer_invariants()?;
        Ok(())
    }

    fn expected_id(&self) -> Result<String> {
        content_id(
            Self::VERSION,
            &MachineBaseAnchorPreimage {
                anchor_version: Self::VERSION,
                base_id: &self.base_id,
                archive_head: &self.archive_head,
                archive_count: self.archive_count,
                archive_event_count: self.archive_event_count,
                archive_batch_count: self.archive_batch_count,
                admission_head: self.admission_head.as_deref(),
                command_index_root: &self.command_index_root,
                prefix_digest: &self.prefix_digest,
                projection_digest: &self.projection_digest,
                projection_root: &self.projection_root,
            },
        )
    }
}

/// Portable evidence returned after compacting a causally closed admission prefix.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineCompaction {
    /// Content-addressed base snapshot identity.
    pub base_id: String,
    /// Cumulative number of compacted event identities.
    pub compacted_events: u64,
    /// Number of full suffix Events retained for resume.
    pub retained_events: u64,
    /// Causal frontier connecting the base to retained execution.
    pub causal_frontier: BTreeSet<String>,
    /// Authenticated base projection digest.
    pub projection_digest: String,
    /// Independent immutable archive object introduced by this compaction.
    pub archive_segment: MachineCommandArchiveSegment,
}

/// Portable, provider-neutral snapshot of canonical inputs and optional base.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineSnapshot {
    /// Snapshot schema version.
    pub snapshot_version: String,
    /// Sealed Plans in exact unique admission order.
    pub plans: Vec<SealedPlan>,
    /// Immutable Artifacts in exact unique admission order.
    pub artifacts: Vec<ArtifactRecord>,
    /// Atomic command batches in exact unique admission order.
    pub batches: Vec<MachineCommandBatchRecord>,
    /// Optional verified projection for a compacted causal prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<MachineBaseSnapshot>,
    /// Exact externally pinned anchor for `base`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_anchor: Option<MachineBaseAnchor>,
    /// Canonical suffix Events in admitted causal order.
    pub events: Vec<Event>,
    /// Complete ordered applied/conflict `CommandAdmission` hash chain.
    pub admissions: Vec<CommandAdmission>,
    /// Command semantic hashes and receipts for idempotent recovery.
    commands: BTreeMap<String, CommandRecord>,
    /// Base-root non-membership proof for every hot command identity.
    command_index_proofs: BTreeMap<String, MachineCommandIndexProof>,
}

/// Closed typed decomposition of one materialized Machine snapshot.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRootParts {
    /// Root-parts schema version.
    pub root_parts_version: String,
    /// Exact Machine snapshot schema represented by these parts.
    pub snapshot_version: String,
    /// Sealed Plans keyed by exact Plan content identity.
    pub plans: BTreeMap<String, SealedPlan>,
    /// Exact unique Plan admission lineage.
    pub plan_admission_order: Vec<String>,
    /// Immutable Artifacts keyed by exact Artifact content identity.
    pub artifacts: BTreeMap<String, ArtifactRecord>,
    /// Exact unique Artifact admission lineage.
    pub artifact_admission_order: Vec<String>,
    /// Atomic command batches keyed by manifest identity.
    pub batches: BTreeMap<String, MachineCommandBatchRecord>,
    /// Exact unique command-batch admission lineage.
    pub batch_admission_order: Vec<String>,
    /// Optional authenticated compacted base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<MachineBaseSnapshot>,
    /// Exact externally pinned anchor for `base`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_anchor: Option<MachineBaseAnchor>,
    /// Canonical hot Event log in admitted order.
    pub events: Vec<Event>,
    /// Canonical hot command-admission log in admitted order.
    pub admissions: Vec<CommandAdmission>,
    /// Closed hot command records keyed by command identity.
    pub commands: BTreeMap<String, ArchivedCommandRecord>,
    /// Base-root non-membership proofs keyed by command identity.
    pub command_index_proofs: BTreeMap<String, MachineCommandIndexProof>,
}

impl MachineRootParts {
    /// Current closed Machine-root decomposition schema.
    pub const VERSION: &'static str = "cymule.machine-root-parts/3";

    fn into_snapshot_unchecked(self) -> MachineSnapshot {
        let plans = self
            .plan_admission_order
            .iter()
            .map(|plan_id| {
                self.plans
                    .get(plan_id)
                    .cloned()
                    .expect("verified Plan admission identity exists")
            })
            .collect();
        let artifacts = self
            .artifact_admission_order
            .iter()
            .map(|artifact_id| {
                self.artifacts
                    .get(artifact_id)
                    .cloned()
                    .expect("verified Artifact admission identity exists")
            })
            .collect();
        let batches = self
            .batch_admission_order
            .iter()
            .map(|batch_id| {
                self.batches
                    .get(batch_id)
                    .cloned()
                    .expect("verified batch admission identity exists")
            })
            .collect();
        MachineSnapshot {
            snapshot_version: self.snapshot_version,
            plans,
            artifacts,
            batches,
            base: self.base,
            base_anchor: self.base_anchor,
            events: self.events,
            admissions: self.admissions,
            commands: self
                .commands
                .into_iter()
                .map(|(command_id, record)| (command_id, record.to_private()))
                .collect(),
            command_index_proofs: self.command_index_proofs,
        }
    }

    fn verify_keys(&self) -> Result<()> {
        let plan_order = self
            .plan_admission_order
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let artifact_order = self
            .artifact_admission_order
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let batch_order = self
            .batch_admission_order
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if self.root_parts_version != Self::VERSION
            || self.snapshot_version != MachineSnapshot::VERSION
            || plan_order.len() != self.plan_admission_order.len()
            || artifact_order.len() != self.artifact_admission_order.len()
            || batch_order.len() != self.batch_admission_order.len()
            || plan_order != self.plans.keys().cloned().collect()
            || artifact_order != self.artifacts.keys().cloned().collect()
            || batch_order != self.batches.keys().cloned().collect()
            || self.plans.iter().any(|(id, plan)| id != &plan.plan_id)
            || self
                .artifacts
                .iter()
                .any(|(id, artifact)| id != &artifact.reference.artifact_id)
            || self.batches.iter().any(|(id, batch)| id != &batch.batch_id)
            || self
                .commands
                .iter()
                .any(|(id, record)| id != &record.envelope.command_id)
            || self
                .command_index_proofs
                .iter()
                .any(|(id, proof)| id != &proof.command_id)
        {
            return Err(CoreError::IdentityMismatch(
                "Machine root parts have a malformed version or keyed identity".to_owned(),
            ));
        }
        for record in self.commands.values() {
            record.verify()?;
        }
        for batch in self.batches.values() {
            batch.verify()?;
        }
        Ok(())
    }
}

/// Closed typed physical change for one exact [`MachineDelta`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineRootDelta {
    /// Root-delta schema version.
    pub root_delta_version: String,
    /// Semantic Machine-delta schema represented by this physical change.
    pub delta_version: String,
    /// Exact parent Machine authority root.
    pub parent_authority_root: String,
    /// Exact result Machine authority root.
    pub result_authority_root: String,
    /// Exact parent base-anchor identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_anchor_id: Option<String>,
    /// Exact result base-anchor identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_anchor_id: Option<String>,
    /// Newly admitted Plans keyed by content identity.
    pub plans: BTreeMap<String, SealedPlan>,
    /// Exact order in which those Plans extend the admission commitment.
    pub plan_admission_order: Vec<String>,
    /// Newly retained Artifacts keyed by content identity.
    pub artifacts: BTreeMap<String, ArtifactRecord>,
    /// Exact order in which those Artifacts extend the admission commitment.
    pub artifact_admission_order: Vec<String>,
    /// Newly admitted atomic command batches keyed by manifest identity.
    pub batches: BTreeMap<String, MachineCommandBatchRecord>,
    /// Exact command-batch admission order.
    pub batch_admission_order: Vec<String>,
    /// Exact removed hot Event prefix, in prior log order.
    pub removed_event_ids: Vec<String>,
    /// Exact removed hot admission prefix, in prior log order.
    pub removed_admission_ids: Vec<String>,
    /// Exact removed hot command identities.
    pub removed_command_ids: BTreeSet<String>,
    /// Exact removed hot command-batch identities.
    pub removed_batch_ids: BTreeSet<String>,
    /// Exact removed hot command-proof identities.
    pub removed_command_index_proof_ids: BTreeSet<String>,
    /// Replacement authenticated base when compaction occurs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<MachineBaseSnapshot>,
    /// Replacement base anchor when compaction occurs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_anchor: Option<MachineBaseAnchor>,
    /// Independently persisted archive-segment header for compaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_segment: Option<MachineCommandArchiveSegmentHeader>,
    /// Newly admitted Event suffix.
    pub events: Vec<Event>,
    /// Newly appended admission suffix.
    pub admissions: Vec<CommandAdmission>,
    /// Newly admitted closed command records keyed by command identity.
    pub commands: BTreeMap<String, ArchivedCommandRecord>,
    /// Newly admitted non-membership proofs keyed by command identity.
    pub command_index_proofs: BTreeMap<String, MachineCommandIndexProof>,
}

impl MachineRootDelta {
    /// Current closed Machine-root change schema.
    pub const VERSION: &'static str = "cymule.machine-root-delta/3";
}

/// Incremental canonical Machine mutation committed through a durable `StateRoot`.
///
/// The private command records keep idempotency authority inside the semantic
/// core while allowing durable adapters to persist only newly admitted values.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MachineDelta {
    #[serde(skip)]
    local_authority: LocalMachineDeltaAuthority,
    /// Delta schema version.
    pub delta_version: String,
    /// Exact parent hot-snapshot digest.
    pub parent_snapshot_digest: String,
    /// Exact result hot-snapshot digest.
    pub result_snapshot_digest: String,
    /// Exact parent base-anchor identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_anchor_id: Option<String>,
    /// Exact result base-anchor identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_anchor_id: Option<String>,
    /// Newly admitted sealed Plans.
    pub plans: Vec<SealedPlan>,
    /// Newly retained immutable Artifacts.
    pub artifacts: Vec<ArtifactRecord>,
    /// Newly admitted atomic command batches.
    pub batches: Vec<MachineCommandBatchRecord>,
    /// Exact current Event prefix compacted by this transition.
    pub compacted_event_ids: Vec<String>,
    /// Exact current command-admission prefix compacted by this transition.
    compacted_admission_ids: Vec<String>,
    /// Exact hot command identities removed by this transition.
    compacted_command_ids: BTreeSet<String>,
    /// Exact hot command-batch identities removed by this transition.
    compacted_batch_ids: BTreeSet<String>,
    /// Exact hot command-proof identities removed by this transition.
    compacted_command_index_proof_ids: BTreeSet<String>,
    /// Replacement authenticated base when compaction occurs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<MachineBaseSnapshot>,
    /// Replacement base anchor when compaction changes the base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_anchor: Option<MachineBaseAnchor>,
    /// Header of the independently persisted archive segment for compaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_segment: Option<MachineCommandArchiveSegmentHeader>,
    /// Newly admitted canonical Events in order.
    pub events: Vec<Event>,
    /// Newly appended `CommandAdmission` chain suffix.
    pub admissions: Vec<CommandAdmission>,
    /// Newly admitted command receipts, including conflict receipts without Events.
    commands: BTreeMap<String, CommandRecord>,
    /// Non-membership proofs for newly admitted hot command identities.
    command_index_proofs: BTreeMap<String, MachineCommandIndexProof>,
}

#[derive(Debug, Clone, Default)]
struct LocalMachineDeltaAuthority(Option<String>);

impl PartialEq for LocalMachineDeltaAuthority {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl MachineDelta {
    /// Current incremental Machine-delta schema.
    pub const VERSION: &'static str = "cymule.machine-delta/6";

    /// Derive a compaction delta bound to one independently persisted archive segment.
    ///
    /// # Errors
    ///
    /// Returns an error unless `previous`, `next`, and `archive_segment` form
    /// one exact canonical compaction transition.
    pub fn between_compaction(
        previous: &MachineSnapshot,
        next: &MachineSnapshot,
        archive_segment: &MachineCommandArchiveSegment,
    ) -> Result<Self> {
        archive_segment.verify()?;
        let mut delta = Self::derive(previous, next)?;
        let base = delta.base.as_ref().ok_or_else(|| {
            CoreError::Validation("archive segment requires a base-changing delta".to_owned())
        })?;
        if archive_segment.header.segment_id != base.archive_head
            || archive_segment.header.result_count != base.archive_count
            || archive_segment.header.result_event_count != base.archive_event_count
            || archive_segment.header.result_admission_head != base.admission_head
            || archive_segment.header.result_command_index_root != base.command_index_root
            || archive_segment.header.parent_segment
                != previous
                    .base
                    .as_ref()
                    .map(|value| value.archive_head.clone())
            || archive_segment.header.parent_count
                != previous
                    .base
                    .as_ref()
                    .map_or(0, |value| value.archive_count)
            || archive_segment.header.parent_event_count
                != previous
                    .base
                    .as_ref()
                    .map_or(0, |value| value.archive_event_count)
            || archive_segment.header.parent_admission_head
                != previous
                    .base
                    .as_ref()
                    .and_then(|value| value.admission_head.clone())
            || archive_segment.header.parent_command_index_root
                != current_command_index_root(previous.base.as_ref())?
        {
            return Err(CoreError::IdentityMismatch(
                "Machine compaction archive segment does not match parent and result bases"
                    .to_owned(),
            ));
        }
        delta.archive_segment = Some(archive_segment.header.clone());
        delta.bind_local_authority()?;
        let result_anchor = next.base_anchor.as_ref().ok_or_else(|| {
            CoreError::NotFound("compacted target snapshot has no base anchor".to_owned())
        })?;
        let mut reconstructed = previous.clone();
        if previous.base.is_some() {
            reconstructed.apply_compaction_delta_anchored(
                &delta,
                result_anchor,
                archive_segment,
            )?;
        } else {
            reconstructed.apply_compaction_delta(&delta, archive_segment)?;
        }
        if reconstructed != *next {
            return Err(CoreError::IdentityMismatch(
                "compaction Machine delta does not reconstruct the target snapshot".to_owned(),
            ));
        }
        Ok(delta)
    }

    fn derive(previous: &MachineSnapshot, next: &MachineSnapshot) -> Result<Self> {
        if previous.snapshot_version != MachineSnapshot::VERSION
            || next.snapshot_version != MachineSnapshot::VERSION
        {
            return Err(CoreError::Validation(
                "Machine delta requires current snapshot versions".to_owned(),
            ));
        }
        let plans = additions_by(
            &previous.plans,
            &next.plans,
            |value| value.plan_id.as_str(),
            "Plan",
        )?;
        let artifacts = additions_by(
            &previous.artifacts,
            &next.artifacts,
            |value| value.reference.artifact_id.as_str(),
            "Artifact",
        )?;
        let base_changed = previous.base != next.base;
        let (compacted_batch_ids, batches) = derive_batch_delta(previous, next, base_changed)?;
        let commands = map_additions(&previous.commands, &next.commands, "command", base_changed)?;
        let compacted_command_ids = if base_changed {
            previous
                .commands
                .keys()
                .filter(|id| !next.commands.contains_key(*id))
                .cloned()
                .collect()
        } else {
            BTreeSet::new()
        };
        let command_index_proofs = if base_changed {
            next.command_index_proofs.clone()
        } else {
            map_additions(
                &previous.command_index_proofs,
                &next.command_index_proofs,
                "command index proof",
                false,
            )?
        };
        let compacted_command_index_proof_ids = if base_changed {
            previous
                .command_index_proofs
                .keys()
                .filter(|id| !next.command_index_proofs.contains_key(*id))
                .cloned()
                .collect()
        } else {
            BTreeSet::new()
        };
        let (compacted_admission_ids, admissions) =
            derive_admission_delta(previous, next, base_changed)?;
        let (compacted_event_ids, base, base_anchor, events) = derive_event_delta(previous, next)?;
        let mut delta = Self {
            local_authority: LocalMachineDeltaAuthority::default(),
            delta_version: Self::VERSION.to_owned(),
            parent_snapshot_digest: snapshot_authority_root(previous)?,
            result_snapshot_digest: snapshot_authority_root(next)?,
            parent_anchor_id: previous
                .base_anchor
                .as_ref()
                .map(|anchor| anchor.anchor_id.clone()),
            result_anchor_id: next
                .base_anchor
                .as_ref()
                .map(|anchor| anchor.anchor_id.clone()),
            plans,
            artifacts,
            batches,
            compacted_event_ids,
            compacted_admission_ids,
            compacted_command_ids,
            compacted_batch_ids,
            compacted_command_index_proof_ids,
            base,
            base_anchor,
            archive_segment: None,
            events,
            admissions,
            commands,
            command_index_proofs,
        };
        delta.bind_local_authority()?;
        Ok(delta)
    }

    /// Whether this transition changes canonical Machine state.
    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
            && self.artifacts.is_empty()
            && self.batches.is_empty()
            && self.compacted_event_ids.is_empty()
            && self.compacted_admission_ids.is_empty()
            && self.compacted_command_ids.is_empty()
            && self.compacted_batch_ids.is_empty()
            && self.compacted_command_index_proof_ids.is_empty()
            && self.base.is_none()
            && self.base_anchor.is_none()
            && self.archive_segment.is_none()
            && self.events.is_empty()
            && self.admissions.is_empty()
            && self.commands.is_empty()
            && self.command_index_proofs.is_empty()
    }

    /// Export the exact closed physical change for persistent Machine roots.
    ///
    /// # Errors
    ///
    /// Returns an error when this delta lacks local derivation authority or
    /// cannot be exported as one internally consistent physical root change.
    pub fn root_delta(&self) -> Result<MachineRootDelta> {
        self.verify_local_authority()?;
        if self.delta_version != Self::VERSION
            || !is_canonical_digest(&self.parent_snapshot_digest)
            || !is_canonical_digest(&self.result_snapshot_digest)
        {
            return Err(CoreError::Validation(
                "Machine root delta has malformed semantic authority".to_owned(),
            ));
        }
        let plans = self
            .plans
            .iter()
            .cloned()
            .map(|plan| (plan.plan_id.clone(), plan))
            .collect::<BTreeMap<_, _>>();
        let artifacts = self
            .artifacts
            .iter()
            .cloned()
            .map(|artifact| (artifact.reference.artifact_id.clone(), artifact))
            .collect::<BTreeMap<_, _>>();
        let batches = self
            .batches
            .iter()
            .cloned()
            .map(|batch| (batch.batch_id.clone(), batch))
            .collect::<BTreeMap<_, _>>();
        for batch in batches.values() {
            batch.verify()?;
        }
        if plans.len() != self.plans.len()
            || artifacts.len() != self.artifacts.len()
            || batches.len() != self.batches.len()
        {
            return Err(CoreError::Validation(
                "Machine root delta repeats a Plan or Artifact identity".to_owned(),
            ));
        }
        let commands = self.verified_root_delta_commands()?;
        if self
            .command_index_proofs
            .iter()
            .any(|(id, proof)| id != &proof.command_id)
            || self.compacted_command_ids != self.compacted_command_index_proof_ids
            || self.compacted_admission_ids.len() != self.compacted_command_ids.len()
        {
            return Err(CoreError::IdentityMismatch(
                "Machine root delta has inconsistent proof or compaction identities".to_owned(),
            ));
        }
        Ok(MachineRootDelta {
            root_delta_version: MachineRootDelta::VERSION.to_owned(),
            delta_version: self.delta_version.clone(),
            parent_authority_root: self.parent_snapshot_digest.clone(),
            result_authority_root: self.result_snapshot_digest.clone(),
            parent_anchor_id: self.parent_anchor_id.clone(),
            result_anchor_id: self.result_anchor_id.clone(),
            plans,
            plan_admission_order: self.plans.iter().map(|plan| plan.plan_id.clone()).collect(),
            artifacts,
            artifact_admission_order: self
                .artifacts
                .iter()
                .map(|artifact| artifact.reference.artifact_id.clone())
                .collect(),
            batches,
            batch_admission_order: self
                .batches
                .iter()
                .map(|batch| batch.batch_id.clone())
                .collect(),
            removed_event_ids: self.compacted_event_ids.clone(),
            removed_admission_ids: self.compacted_admission_ids.clone(),
            removed_command_ids: self.compacted_command_ids.clone(),
            removed_batch_ids: self.compacted_batch_ids.clone(),
            removed_command_index_proof_ids: self.compacted_command_index_proof_ids.clone(),
            base: self.base.clone(),
            base_anchor: self.base_anchor.clone(),
            archive_segment: self.archive_segment.clone(),
            events: self.events.clone(),
            admissions: self.admissions.clone(),
            commands,
            command_index_proofs: self.command_index_proofs.clone(),
        })
    }

    fn verified_root_delta_commands(&self) -> Result<BTreeMap<String, ArchivedCommandRecord>> {
        let commands = self
            .commands
            .iter()
            .map(|(id, record)| {
                let exported = ArchivedCommandRecord::from_private(record);
                exported.verify()?;
                if id != &exported.envelope.command_id {
                    return Err(CoreError::IdentityMismatch(format!(
                        "Machine root delta command key {id} does not match its envelope"
                    )));
                }
                Ok((id.clone(), exported))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for admission in &self.admissions {
            let record = self.commands.get(&admission.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "Machine root delta admission {} has no command record",
                    admission.admission_id
                ))
            })?;
            verify_admission_record(admission, record)?;
        }
        Ok(commands)
    }

    fn verify_local_authority(&self) -> Result<()> {
        let expected = canonical_digest(self)?;
        if self.local_authority.0.as_deref() != Some(expected.as_str()) {
            return Err(CoreError::Validation(
                "prepared Machine delta must be the exact locally derived transition".to_owned(),
            ));
        }
        Ok(())
    }

    fn bind_local_authority(&mut self) -> Result<()> {
        self.local_authority = LocalMachineDeltaAuthority::default();
        self.local_authority = LocalMachineDeltaAuthority(Some(canonical_digest(self)?));
        Ok(())
    }
}

fn derive_batch_delta(
    previous: &MachineSnapshot,
    next: &MachineSnapshot,
    base_changed: bool,
) -> Result<(BTreeSet<String>, Vec<MachineCommandBatchRecord>)> {
    let compacted_batch_ids = if base_changed {
        previous
            .batches
            .iter()
            .filter(|batch| {
                !next
                    .batches
                    .iter()
                    .any(|next| next.batch_id == batch.batch_id)
            })
            .map(|batch| batch.batch_id.clone())
            .collect()
    } else {
        BTreeSet::new()
    };
    let retained_batches = previous
        .batches
        .iter()
        .filter(|batch| !compacted_batch_ids.contains(&batch.batch_id))
        .cloned()
        .collect::<Vec<_>>();
    let batches = additions_by(
        &retained_batches,
        &next.batches,
        |value| value.batch_id.as_str(),
        "command batch",
    )?;
    Ok((compacted_batch_ids, batches))
}

fn derive_admission_delta(
    previous: &MachineSnapshot,
    next: &MachineSnapshot,
    base_changed: bool,
) -> Result<(Vec<String>, Vec<CommandAdmission>)> {
    if !base_changed {
        if !next.admissions.starts_with(&previous.admissions) {
            return Err(CoreError::Validation(
                "Machine delta cannot rewrite or remove CommandAdmissions".to_owned(),
            ));
        }
        return Ok((
            Vec::new(),
            next.admissions[previous.admissions.len()..].to_vec(),
        ));
    }
    let retained_start = (0..=previous.admissions.len())
        .find(|index| next.admissions.starts_with(&previous.admissions[*index..]))
        .ok_or_else(|| {
            CoreError::Validation(
                "Machine compaction does not retain an exact CommandAdmission suffix".to_owned(),
            )
        })?;
    let retained = previous.admissions.len() - retained_start;
    Ok((
        previous.admissions[..retained_start]
            .iter()
            .map(|admission| admission.admission_id.clone())
            .collect(),
        next.admissions[retained..].to_vec(),
    ))
}

type EventDeltaParts = (
    Vec<String>,
    Option<MachineBaseSnapshot>,
    Option<MachineBaseAnchor>,
    Vec<Event>,
);

fn derive_event_delta(
    previous: &MachineSnapshot,
    next: &MachineSnapshot,
) -> Result<EventDeltaParts> {
    if previous.base == next.base {
        if previous.base_anchor != next.base_anchor {
            return Err(CoreError::Validation(
                "Machine delta cannot rewrite an unchanged base anchor".to_owned(),
            ));
        }
        if !next.events.starts_with(&previous.events) {
            return Err(CoreError::Validation(
                "Machine delta cannot rewrite or remove retained Events".to_owned(),
            ));
        }
        return Ok((
            Vec::new(),
            None,
            None,
            next.events[previous.events.len()..].to_vec(),
        ));
    }
    let base = next.base.clone().ok_or_else(|| {
        CoreError::Validation("Machine delta cannot remove a compacted base".to_owned())
    })?;
    let retained_start = (0..=previous.events.len())
        .find(|index| next.events.starts_with(&previous.events[*index..]))
        .ok_or_else(|| {
            CoreError::Validation(
                "Machine compaction does not retain an exact Event suffix".to_owned(),
            )
        })?;
    let retained = previous.events.len() - retained_start;
    Ok((
        previous.events[..retained_start]
            .iter()
            .map(|event| event.event_id.clone())
            .collect(),
        Some(base),
        next.base_anchor.clone(),
        next.events[retained..].to_vec(),
    ))
}

impl MachineSnapshot {
    /// Decompose this snapshot into fixed closed persistent-root families.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot is not complete canonical Machine
    /// authority or its keyed root families are inconsistent.
    pub fn root_parts(&self) -> Result<MachineRootParts> {
        validate_machine_root_snapshot(self)?;
        let parts = MachineRootParts {
            root_parts_version: MachineRootParts::VERSION.to_owned(),
            snapshot_version: self.snapshot_version.clone(),
            plans: self
                .plans
                .iter()
                .cloned()
                .map(|plan| (plan.plan_id.clone(), plan))
                .collect(),
            plan_admission_order: self.plans.iter().map(|plan| plan.plan_id.clone()).collect(),
            artifacts: self
                .artifacts
                .iter()
                .cloned()
                .map(|artifact| (artifact.reference.artifact_id.clone(), artifact))
                .collect(),
            artifact_admission_order: self
                .artifacts
                .iter()
                .map(|artifact| artifact.reference.artifact_id.clone())
                .collect(),
            batches: self
                .batches
                .iter()
                .cloned()
                .map(|batch| (batch.batch_id.clone(), batch))
                .collect(),
            batch_admission_order: self
                .batches
                .iter()
                .map(|batch| batch.batch_id.clone())
                .collect(),
            base: self.base.clone(),
            base_anchor: self.base_anchor.clone(),
            events: self.events.clone(),
            admissions: self.admissions.clone(),
            commands: self
                .commands
                .iter()
                .map(|(id, record)| (id.clone(), ArchivedCommandRecord::from_private(record)))
                .collect(),
            command_index_proofs: self.command_index_proofs.clone(),
        };
        parts.verify_keys()?;
        if parts.plans.len() != self.plans.len()
            || parts.artifacts.len() != self.artifacts.len()
            || parts.batches.len() != self.batches.len()
        {
            return Err(CoreError::Validation(
                "Machine snapshot repeats a Plan, Artifact, or batch identity".to_owned(),
            ));
        }
        Ok(parts)
    }

    /// Reconstruct and fully audit one snapshot from fixed persistent roots.
    ///
    /// # Errors
    ///
    /// Returns an error when the root families do not reconstruct one complete
    /// canonical Machine snapshot.
    pub fn from_root_parts(parts: MachineRootParts) -> Result<Self> {
        parts.verify_keys()?;
        let snapshot = parts.into_snapshot_unchecked();
        validate_machine_root_snapshot(&snapshot)?;
        Ok(snapshot)
    }

    /// Apply one authenticated incremental representation transactionally.
    /// The complete assembled snapshot is reopened on staged state before any
    /// byte is published, so a late semantic or authority failure rolls back the
    /// entire delta.
    ///
    /// # Errors
    ///
    /// Returns an error when the delta parent, assembled authority, replay, or
    /// declared result does not match this snapshot exactly.
    pub fn apply_delta(&mut self, delta: &MachineDelta) -> Result<()> {
        if delta.base.is_some() {
            return Err(CoreError::Validation(
                "Machine compaction requires explicit archive-segment application".to_owned(),
            ));
        }
        validate_delta_parent(self, delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, None, None)?;
        let restored = Machine::restore(staged.clone())?;
        if restored.snapshot() != staged {
            return Err(CoreError::IdentityMismatch(
                "Machine delta assembled a non-canonical snapshot".to_owned(),
            ));
        }
        validate_delta_result(&staged, delta)?;
        *self = staged;
        Ok(())
    }

    /// Apply a delta transactionally while validating the assembled snapshot
    /// against one exact trusted compacted-base anchor.
    ///
    /// # Errors
    ///
    /// Returns an error when parent or anchor authority, replay validation, or
    /// the declared result fails exact validation.
    pub fn apply_delta_anchored(
        &mut self,
        delta: &MachineDelta,
        expected_anchor: &MachineBaseAnchor,
    ) -> Result<()> {
        if delta.base.is_some() {
            return Err(CoreError::Validation(
                "Machine compaction requires explicit archive-segment application".to_owned(),
            ));
        }
        validate_delta_parent(self, delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, Some(expected_anchor), None)?;
        let restored = Machine::restore_anchored(staged.clone(), expected_anchor)?;
        if restored.snapshot() != staged {
            return Err(CoreError::IdentityMismatch(
                "Machine delta assembled a non-canonical anchored snapshot".to_owned(),
            ));
        }
        validate_delta_result(&staged, delta)?;
        *self = staged;
        Ok(())
    }

    /// Apply a genesis compaction delta with its independently persisted segment.
    ///
    /// # Errors
    ///
    /// Returns an error unless the delta, archive segment, compacted prefix,
    /// and reconstructed snapshot form one exact genesis compaction.
    pub fn apply_compaction_delta(
        &mut self,
        delta: &MachineDelta,
        archive_segment: &MachineCommandArchiveSegment,
    ) -> Result<()> {
        if self.base.is_some() {
            return Err(CoreError::Validation(
                "compaction over an existing base requires its trusted result anchor".to_owned(),
            ));
        }
        validate_delta_parent(self, delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, None, Some(archive_segment))?;
        let restored = Machine::restore_with_archive(staged.clone(), [archive_segment.clone()])?;
        if restored.snapshot() != staged {
            return Err(CoreError::IdentityMismatch(
                "Machine compaction delta assembled a non-canonical snapshot".to_owned(),
            ));
        }
        validate_delta_result(&staged, delta)?;
        *self = staged;
        Ok(())
    }

    /// Apply a later compaction delta after the Store atomically persists its segment.
    ///
    /// # Errors
    ///
    /// Returns an error unless the anchor, delta, archive segment, and complete
    /// assembled authority are exact and causally continuous.
    pub fn apply_compaction_delta_anchored(
        &mut self,
        delta: &MachineDelta,
        expected_result_anchor: &MachineBaseAnchor,
        archive_segment: &MachineCommandArchiveSegment,
    ) -> Result<()> {
        validate_delta_parent(self, delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, Some(expected_result_anchor), Some(archive_segment))?;
        let restored = Machine::restore_anchored(staged.clone(), expected_result_anchor)?;
        if restored.snapshot() != staged {
            return Err(CoreError::IdentityMismatch(
                "anchored compaction delta assembled a non-canonical snapshot".to_owned(),
            ));
        }
        validate_delta_result(&staged, delta)?;
        *self = staged;
        Ok(())
    }

    fn apply_delta_in_place(
        &mut self,
        delta: &MachineDelta,
        expected_anchor: Option<&MachineBaseAnchor>,
        archive_segment: Option<&MachineCommandArchiveSegment>,
    ) -> Result<()> {
        if self.snapshot_version != Self::VERSION || delta.delta_version != MachineDelta::VERSION {
            return Err(CoreError::Validation(
                "Machine snapshot and delta versions do not match".to_owned(),
            ));
        }
        let (mut commands, mut command_index_proofs) = self.merge_delta_authority(delta)?;
        self.apply_snapshot_compaction(
            delta,
            expected_anchor,
            archive_segment,
            &mut commands,
            &mut command_index_proofs,
        )?;
        self.validate_and_append_delta_events(delta)?;
        verify_hot_command_proofs(self.base.as_ref(), &commands, &command_index_proofs)?;
        self.commands = commands;
        self.command_index_proofs = command_index_proofs;
        verify_command_event_closure(&self.events, &self.commands)
    }

    fn merge_delta_authority(
        &mut self,
        delta: &MachineDelta,
    ) -> Result<(
        BTreeMap<String, CommandRecord>,
        BTreeMap<String, MachineCommandIndexProof>,
    )> {
        for plan in &delta.plans {
            plan.verify()?;
        }
        for artifact in &delta.artifacts {
            artifact.reference.validate()?;
            let expected =
                artifact_ref(artifact.reference.kind.clone(), artifact.bytes.as_slice())?;
            if expected != artifact.reference {
                return Err(CoreError::IdentityMismatch(format!(
                    "Artifact {} does not match its Machine delta bytes",
                    artifact.reference.artifact_id
                )));
            }
        }
        merge_snapshot_values(
            &mut self.plans,
            &delta.plans,
            |value| value.plan_id.as_str(),
            "Plan",
        )?;
        merge_snapshot_values(
            &mut self.artifacts,
            &delta.artifacts,
            |value| value.reference.artifact_id.as_str(),
            "Artifact",
        )?;
        for batch in &delta.batches {
            batch.verify()?;
        }
        merge_snapshot_values(
            &mut self.batches,
            &delta.batches,
            |batch| batch.batch_id.as_str(),
            "command batch",
        )?;
        let mut commands = self.commands.clone();
        for (id, record) in &delta.commands {
            if commands.insert(id.clone(), record.clone()).is_some() {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine delta command {id} conflicts with retained authority"
                )));
            }
        }
        let mut proofs = self.command_index_proofs.clone();
        if delta.base.is_none() {
            for (id, proof) in &delta.command_index_proofs {
                if id != &proof.command_id
                    || proof.value.is_some()
                    || proofs.insert(id.clone(), proof.clone()).is_some()
                {
                    return Err(CoreError::IdentityMismatch(format!(
                        "Machine delta command index proof {id} conflicts with retained authority"
                    )));
                }
            }
        }
        for admission in &delta.admissions {
            admission.verify(command_admission_parent(
                &self.admissions,
                self.base.as_ref(),
            ))?;
            let record = commands.get(&admission.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "CommandAdmission {} has no command record",
                    admission.admission_id
                ))
            })?;
            verify_admission_record(admission, record)?;
            self.admissions.push(admission.clone());
        }
        Ok((commands, proofs))
    }

    fn apply_snapshot_compaction(
        &mut self,
        delta: &MachineDelta,
        expected_anchor: Option<&MachineBaseAnchor>,
        archive_segment: Option<&MachineCommandArchiveSegment>,
        commands: &mut BTreeMap<String, CommandRecord>,
        command_index_proofs: &mut BTreeMap<String, MachineCommandIndexProof>,
    ) -> Result<()> {
        match &delta.base {
            None if delta.compacted_event_ids.is_empty() => {}
            Some(base) => {
                self.verify_snapshot_compacted_projection(delta, base, expected_anchor, commands)?;
                let archive_segment = archive_segment.ok_or_else(|| {
                    CoreError::NotFound(
                        "Machine compaction application has no archive segment".to_owned(),
                    )
                })?;
                if delta.archive_segment.as_ref() != Some(&archive_segment.header) {
                    return Err(CoreError::IdentityMismatch(
                        "Machine compaction segment header does not match its delta".to_owned(),
                    ));
                }
                let event_catalog = self
                    .events
                    .iter()
                    .map(|event| (event.event_id.clone(), event.clone()))
                    .collect::<BTreeMap<_, _>>();
                let archived_admission_count = verify_archive_segment_entries(
                    &self.admissions,
                    commands,
                    &event_catalog,
                    archive_segment,
                    &delta.compacted_event_ids,
                )?;
                let expected_archived_count = base
                    .archive_count
                    .checked_sub(self.base.as_ref().map_or(0, |value| value.archive_count))
                    .ok_or_else(|| {
                        CoreError::Validation(
                            "Machine delta archive count moved backwards".to_owned(),
                        )
                    })?;
                let expected_archived_count = usize::try_from(expected_archived_count)
                    .map_err(|error| CoreError::Validation(error.to_string()))?;
                if archived_admission_count != expected_archived_count {
                    return Err(CoreError::IdentityMismatch(
                        "Machine compaction archive entry count does not match its base cut"
                            .to_owned(),
                    ));
                }
                verify_delta_compaction_removals(
                    delta,
                    self.admissions
                        .get(..archived_admission_count)
                        .ok_or_else(|| {
                            CoreError::NotFound(
                                "Machine delta compaction removal prefix is absent".to_owned(),
                            )
                        })?,
                    commands,
                    command_index_proofs,
                )?;
                verify_compaction_admission_frontier(
                    base,
                    self.base.as_ref(),
                    &self.admissions,
                    archived_admission_count,
                )?;
                verify_delta_compaction_batches(
                    delta,
                    self.batches.iter(),
                    archive_segment,
                    self.base.as_ref(),
                    base,
                )?;
                for admission in self.admissions.drain(..archived_admission_count) {
                    commands.remove(&admission.command_id);
                    command_index_proofs.remove(&admission.command_id);
                }
                self.batches
                    .retain(|batch| !delta.compacted_batch_ids.contains(&batch.batch_id));
                self.events.drain(..delta.compacted_event_ids.len());
                let anchor = delta.base_anchor.as_ref().ok_or_else(|| {
                    CoreError::NotFound("Machine delta compaction has no base anchor".to_owned())
                })?;
                anchor.verify_trusted_base_fields(base)?;
                self.base_anchor = Some(anchor.clone());
                self.base = Some(base.clone());
                command_index_proofs.clone_from(&delta.command_index_proofs);
            }
            None => {
                return Err(CoreError::Validation(
                    "Machine delta compaction requires both a base and an Event prefix".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn verify_snapshot_compacted_projection(
        &self,
        delta: &MachineDelta,
        base: &MachineBaseSnapshot,
        expected_anchor: Option<&MachineBaseAnchor>,
        commands: &BTreeMap<String, CommandRecord>,
    ) -> Result<()> {
        if let Some(anchor) = expected_anchor {
            anchor.verify_trusted_base_fields(base)?;
        } else {
            base.verify()?;
        }
        verify_delta_archive_transition(self.base.as_ref(), base, delta.archive_segment.as_ref())?;
        if self
            .events
            .iter()
            .take(delta.compacted_event_ids.len())
            .map(|event| event.event_id.as_str())
            .ne(delta.compacted_event_ids.iter().map(String::as_str))
        {
            return Err(CoreError::Validation(
                "Machine delta compaction does not match the snapshot Event prefix".to_owned(),
            ));
        }
        let projection = self
            .base
            .as_ref()
            .map(|value| value.projection.clone())
            .unwrap_or_default();
        let mut authority = Self::authority_machine(&self.plans, &self.artifacts, projection)?;
        for event in self.events.iter().take(delta.compacted_event_ids.len()) {
            event.verify()?;
            verify_event_footprint(event)?;
            authority.validate_event_authority(event)?;
            authority.projection.apply_event(event)?;
            let record = commands.get(&event.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "compacted Event {} has no command record",
                    event.event_id
                ))
            })?;
            if record.semantic_hash != event.command_hash
                || !record.receipt.event_ids.contains(&event.event_id)
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "compacted Event {} evidence does not match",
                    event.event_id
                )));
            }
        }
        if authority.projection != base.projection {
            return Err(CoreError::IdentityMismatch(
                "Machine delta compacted projection does not match its Event prefix".to_owned(),
            ));
        }
        authority.validate_projection_authority(&base.projection)
    }

    fn validate_and_append_delta_events(&mut self, delta: &MachineDelta) -> Result<()> {
        let mut known_events = self
            .base
            .as_ref()
            .map(MachineBaseSnapshot::event_ids)
            .unwrap_or_default();
        let mut last_by_run = self
            .base
            .as_ref()
            .map(|base| {
                base.projection
                    .runs
                    .iter()
                    .map(|(run_id, run)| (run_id.clone(), run.last_event.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        for event in self.events.iter().chain(&delta.events) {
            event.verify()?;
            verify_event_footprint(event)?;
            if !known_events.insert(event.event_id.clone()) {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine delta repeats Event {}",
                    event.event_id
                )));
            }
            if event
                .parents
                .iter()
                .any(|parent| !known_events.contains(parent))
            {
                return Err(CoreError::Causal(format!(
                    "Machine delta Event {} references a missing or later parent",
                    event.event_id
                )));
            }
            if matches!(&event.payload, EventPayload::RunStarted { .. }) {
                if !event.parents.is_empty()
                    || last_by_run
                        .insert(event.run_id.clone(), event.event_id.clone())
                        .is_some()
                {
                    return Err(CoreError::Causal(format!(
                        "Machine delta repeats or reparents Run {} start",
                        event.run_id
                    )));
                }
            } else {
                let previous = last_by_run.get_mut(&event.run_id).ok_or_else(|| {
                    CoreError::Causal(format!(
                        "Machine delta Event {} precedes Run {} start",
                        event.event_id, event.run_id
                    ))
                })?;
                if !event.parents.contains(previous) {
                    return Err(CoreError::Causal(format!(
                        "Machine delta Event {} does not extend Run {} frontier {}",
                        event.event_id, event.run_id, previous
                    )));
                }
                previous.clone_from(&event.event_id);
            }
        }
        self.events.extend(delta.events.iter().cloned());
        Ok(())
    }

    fn authority_machine(
        plans: &[SealedPlan],
        artifacts: &[ArtifactRecord],
        projection: Projection,
    ) -> Result<Machine> {
        let mut authority = Machine::new();
        for plan in plans {
            authority.retain_plan(plan.clone())?;
        }
        for artifact in artifacts {
            authority.retain_artifact(artifact.clone())?;
        }
        authority.projection = projection;
        authority.projection.rebuild_derived_indexes()?;
        Ok(authority)
    }
}

fn verify_hot_command_proofs(
    base: Option<&MachineBaseSnapshot>,
    commands: &BTreeMap<String, CommandRecord>,
    proofs: &BTreeMap<String, MachineCommandIndexProof>,
) -> Result<()> {
    if commands.keys().ne(proofs.keys()) {
        return Err(CoreError::IdentityMismatch(
            "Machine hot commands and command index proofs do not match".to_owned(),
        ));
    }
    let root = current_command_index_root(base)?;
    for (command_id, proof) in proofs {
        if command_id != &proof.command_id || proof.value.is_some() {
            return Err(CoreError::IdentityMismatch(format!(
                "Machine hot command {command_id} has the wrong index proof"
            )));
        }
        proof.verify(&root)?;
    }
    Ok(())
}

fn validate_machine_root_snapshot(snapshot: &MachineSnapshot) -> Result<()> {
    if snapshot.snapshot_version != MachineSnapshot::VERSION {
        return Err(CoreError::Validation(format!(
            "unsupported Machine snapshot version {:?}",
            snapshot.snapshot_version
        )));
    }
    let restored = match snapshot.base_anchor.as_ref() {
        Some(anchor) => Machine::restore_anchored(snapshot.clone(), anchor)?,
        None => Machine::restore(snapshot.clone())?,
    };
    if restored.snapshot() != *snapshot {
        return Err(CoreError::IdentityMismatch(
            "Machine root parts do not reconstruct their exact canonical snapshot".to_owned(),
        ));
    }
    Ok(())
}

fn merge_snapshot_values<T: Clone + PartialEq>(
    retained: &mut Vec<T>,
    added: &[T],
    identity: impl Fn(&T) -> &str,
    kind: &str,
) -> Result<()> {
    let mut positions = retained
        .iter()
        .enumerate()
        .map(|(index, value)| (identity(value).to_owned(), index))
        .collect::<BTreeMap<_, _>>();
    if positions.len() != retained.len() {
        return Err(CoreError::Validation(format!(
            "Machine snapshot repeats an admitted {kind} identity"
        )));
    }
    for value in added {
        let id = identity(value).to_owned();
        match positions.get(&id).copied() {
            Some(index) if retained.get(index) == Some(value) => {}
            Some(_) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine delta rewrites {kind} {id}"
                )));
            }
            None => {
                positions.insert(id, retained.len());
                retained.push(value.clone());
            }
        }
    }
    Ok(())
}

fn verify_compaction_admission_frontier(
    next: &MachineBaseSnapshot,
    previous: Option<&MachineBaseSnapshot>,
    admissions: &[CommandAdmission],
    archived_count: usize,
) -> Result<()> {
    let prefix = admissions.get(..archived_count).ok_or_else(|| {
        CoreError::NotFound("Machine delta compaction removal prefix is absent".to_owned())
    })?;
    let frontier = command_admission_parent(prefix, previous);
    if frontier.as_ref().map(|parent| parent.admission_id) != next.admission_head.as_deref()
        || frontier.map_or(0, |parent| parent.sequence) != next.archive_count
    {
        return Err(CoreError::IdentityMismatch(
            "Machine delta base does not match its CommandAdmission cut".to_owned(),
        ));
    }
    Ok(())
}

fn verify_delta_compaction_batches<'a>(
    delta: &'a MachineDelta,
    batches: impl IntoIterator<Item = &'a MachineCommandBatchRecord>,
    segment: &MachineCommandArchiveSegment,
    previous: Option<&MachineBaseSnapshot>,
    next: &MachineBaseSnapshot,
) -> Result<()> {
    let mut batches = batches.into_iter();
    let prefix = batches.by_ref().take(segment.batches.len());
    let archived_ids = segment
        .batches
        .iter()
        .map(|batch| batch.batch_id.clone())
        .collect::<BTreeSet<_>>();
    if segment.batches.iter().ne(prefix) || archived_ids != delta.compacted_batch_ids {
        return Err(CoreError::IdentityMismatch(
            "Machine compaction batches do not match their complete hot prefix".to_owned(),
        ));
    }
    let mut commitment =
        AdmissionCommitment::new(MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN);
    let mut count = 0_u64;
    if let Some(previous) = previous {
        commitment
            .root
            .clone_from(&previous.batch_admission_commitment);
        count = previous.batch_count;
    }
    for batch in &segment.batches {
        commitment.insert_with_undo(&batch.batch_id)?;
        count = count
            .checked_add(1)
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("Machine batch count overflowed".to_owned()))?;
    }
    if commitment.root != next.batch_admission_commitment || count != next.batch_count {
        return Err(CoreError::IdentityMismatch(
            "Machine compaction batch frontier does not match its archive cut".to_owned(),
        ));
    }
    verify_compaction_source_cut(next, segment, batches)
}

fn verify_compaction_source_cut<'a>(
    base: &MachineBaseSnapshot,
    segment: &MachineCommandArchiveSegment,
    retained: impl IntoIterator<Item = &'a MachineCommandBatchRecord>,
) -> Result<()> {
    let cut_root = machine_authority_root(&MachineAuthorityRootInput {
        plan_commitment: &base.plan_admission_commitment,
        plan_count: base.plan_count,
        artifact_commitment: &base.artifact_admission_commitment,
        artifact_count: base.artifact_count,
        batch_commitment: &base.batch_admission_commitment,
        batch_count: base.batch_count,
        projection_root: &base.projection_root,
        event_count: base.archive_event_count,
        admission_sequence: (base.archive_count != 0).then_some(base.archive_count),
        admission_head: base.admission_head.as_deref(),
    })?;
    if segment
        .batches
        .last()
        .map(|batch| &batch.result_authority_root)
        != Some(&cut_root)
    {
        return Err(CoreError::IdentityMismatch(
            "Machine compaction base does not match its terminal archived batch authority"
                .to_owned(),
        ));
    }
    let mut known_roots = BTreeSet::from([cut_root.clone()]);
    let mut parent = cut_root;
    for batch in retained {
        batch.verify()?;
        if batch.admission_parent_authority_root != parent {
            return Err(CoreError::IdentityMismatch(
                "Machine retained batch admission chain is discontinuous after compaction"
                    .to_owned(),
            ));
        }
        if !known_roots.contains(&batch.parent_authority_root) {
            return Err(CoreError::Causal(format!(
                "Machine compaction cut discards frozen source {} required by retained batch {}",
                batch.parent_authority_root, batch.batch_id,
            )));
        }
        parent.clone_from(&batch.result_authority_root);
        known_roots.insert(parent.clone());
    }
    Ok(())
}

fn verify_delta_compaction_removals(
    delta: &MachineDelta,
    compacted_admissions: &[CommandAdmission],
    commands: &BTreeMap<String, CommandRecord>,
    command_index_proofs: &BTreeMap<String, MachineCommandIndexProof>,
) -> Result<()> {
    let admission_ids = compacted_admissions
        .iter()
        .map(|admission| admission.admission_id.clone())
        .collect::<Vec<_>>();
    let command_ids = compacted_admissions
        .iter()
        .map(|admission| admission.command_id.clone())
        .collect::<BTreeSet<_>>();
    if admission_ids != delta.compacted_admission_ids
        || command_ids != delta.compacted_command_ids
        || command_ids != delta.compacted_command_index_proof_ids
        || command_ids
            .iter()
            .any(|command_id| !commands.contains_key(command_id))
        || command_ids
            .iter()
            .any(|command_id| !command_index_proofs.contains_key(command_id))
    {
        return Err(CoreError::IdentityMismatch(
            "Machine compaction removal witness does not match its exact hot admission prefix"
                .to_owned(),
        ));
    }
    Ok(())
}

fn additions_by<T: Clone + PartialEq>(
    previous: &[T],
    next: &[T],
    identity: impl Fn(&T) -> &str,
    kind: &str,
) -> Result<Vec<T>> {
    let previous_ids = previous.iter().map(&identity).collect::<BTreeSet<_>>();
    let next_ids = next.iter().map(&identity).collect::<BTreeSet<_>>();
    if previous_ids.len() != previous.len() || next_ids.len() != next.len() {
        return Err(CoreError::Validation(format!(
            "Machine snapshot repeats an admitted {kind} identity"
        )));
    }
    if next.len() < previous.len()
        || previous
            .iter()
            .zip(next)
            .any(|(before, after)| identity(before) != identity(after) || before != after)
    {
        return Err(CoreError::Validation(format!(
            "Machine delta rewrites or reorders the {kind} admission lineage"
        )));
    }
    let additions = next[previous.len()..].to_vec();
    for value in &additions {
        let id = identity(value);
        if previous_ids.contains(id) {
            return Err(CoreError::Validation(format!(
                "Machine delta repeats admitted {kind} {id}"
            )));
        }
    }
    Ok(additions)
}

fn map_additions<T: Clone + PartialEq>(
    previous: &BTreeMap<String, T>,
    next: &BTreeMap<String, T>,
    kind: &str,
    allow_removals: bool,
) -> Result<BTreeMap<String, T>> {
    let mut additions = BTreeMap::new();
    for (id, value) in next {
        match previous.get(id) {
            Some(existing) if existing == value => {}
            Some(_) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine delta rewrites {kind} {id}"
                )));
            }
            None => {
                additions.insert(id.clone(), value.clone());
            }
        }
    }
    if !allow_removals && previous.keys().any(|id| !next.contains_key(id)) {
        return Err(CoreError::Validation(format!(
            "Machine delta removes an existing {kind}"
        )));
    }
    Ok(additions)
}

fn command_admission_parent<'a>(
    admissions: &'a [CommandAdmission],
    base: Option<&'a MachineBaseSnapshot>,
) -> Option<CommandAdmissionParent<'a>> {
    admissions.last().map(Into::into).or_else(|| {
        base.and_then(|base| {
            base.admission_head
                .as_deref()
                .map(|admission_id| CommandAdmissionParent {
                    sequence: base.archive_count,
                    admission_id,
                })
        })
    })
}

fn current_command_index_root(base: Option<&MachineBaseSnapshot>) -> Result<String> {
    base.map_or_else(MachineCommandIndexProof::empty_root, |base| {
        Ok(base.command_index_root.clone())
    })
}

fn validate_delta_parent(snapshot: &MachineSnapshot, delta: &MachineDelta) -> Result<()> {
    if delta.delta_version != MachineDelta::VERSION {
        return Err(CoreError::Validation(format!(
            "unsupported machine delta version {:?}",
            delta.delta_version
        )));
    }
    let anchor_id = snapshot
        .base_anchor
        .as_ref()
        .map(|anchor| anchor.anchor_id.as_str());
    if delta.base.is_none()
        && (!delta.compacted_event_ids.is_empty()
            || !delta.compacted_admission_ids.is_empty()
            || !delta.compacted_command_ids.is_empty()
            || !delta.compacted_command_index_proof_ids.is_empty())
    {
        return Err(CoreError::Validation(
            "ordinary Machine delta carries compaction removals".to_owned(),
        ));
    }
    if snapshot_authority_root(snapshot)? != delta.parent_snapshot_digest
        || anchor_id != delta.parent_anchor_id.as_deref()
    {
        return Err(CoreError::IdentityMismatch(
            "Machine delta parent snapshot or base anchor does not match".to_owned(),
        ));
    }
    delta.verify_local_authority()?;
    Ok(())
}

fn validate_delta_result(snapshot: &MachineSnapshot, delta: &MachineDelta) -> Result<()> {
    let anchor_id = snapshot
        .base_anchor
        .as_ref()
        .map(|anchor| anchor.anchor_id.as_str());
    if snapshot_authority_root(snapshot)? != delta.result_snapshot_digest
        || anchor_id != delta.result_anchor_id.as_deref()
    {
        return Err(CoreError::IdentityMismatch(
            "Machine delta result snapshot or base anchor does not match".to_owned(),
        ));
    }
    Ok(())
}

fn snapshot_authority_root(snapshot: &MachineSnapshot) -> Result<String> {
    let machine = match snapshot.base_anchor.as_ref() {
        Some(anchor) => Machine::restore_anchored(snapshot.clone(), anchor)?,
        None => Machine::restore(snapshot.clone())?,
    };
    machine.authority_root()
}

fn verify_delta_archive_transition(
    previous: Option<&MachineBaseSnapshot>,
    next: &MachineBaseSnapshot,
    header: Option<&MachineCommandArchiveSegmentHeader>,
) -> Result<()> {
    let header = header.ok_or_else(|| {
        CoreError::NotFound("Machine compaction delta has no archive segment header".to_owned())
    })?;
    header.verify()?;
    if header.parent_segment.as_deref() != previous.map(|base| base.archive_head.as_str())
        || header.parent_count != previous.map_or(0, |base| base.archive_count)
        || header.parent_event_count != previous.map_or(0, |base| base.archive_event_count)
        || header.parent_admission_head.as_deref()
            != previous.and_then(|base| base.admission_head.as_deref())
        || header.parent_command_index_root != current_command_index_root(previous)?
        || header.segment_id != next.archive_head
        || header.result_count != next.archive_count
        || header.result_event_count != next.archive_event_count
        || header.result_admission_head != next.admission_head
        || header.result_command_index_root != next.command_index_root
    {
        return Err(CoreError::IdentityMismatch(
            "Machine compaction archive header does not match parent and result bases".to_owned(),
        ));
    }
    Ok(())
}

fn verify_archive_segment_entries(
    admissions: &[CommandAdmission],
    commands: &BTreeMap<String, CommandRecord>,
    events: &BTreeMap<String, Event>,
    segment: &MachineCommandArchiveSegment,
    expected_event_ids: &[String],
) -> Result<usize> {
    segment.verify()?;
    let count = usize::try_from(segment.header.entry_count)
        .map_err(|error| CoreError::Validation(error.to_string()))?;
    let archived = admissions.get(..count).ok_or_else(|| {
        CoreError::NotFound("Machine compaction archive exceeds hot admissions".to_owned())
    })?;
    let mut archived_event_ids = Vec::new();
    for (admission, entry) in archived.iter().zip(&segment.entries) {
        let record = commands.get(&admission.command_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "archived command {} has no hot record",
                admission.command_id
            ))
        })?;
        let batch = admission
            .event_ids
            .iter()
            .map(|event_id| {
                events.get(event_id).cloned().ok_or_else(|| {
                    CoreError::NotFound(format!("archived Event {event_id} is missing"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let expected = MachineCommandArchiveEntry {
            admission: admission.clone(),
            command: ArchivedCommandRecord::from_private(record),
            events: batch,
        };
        if entry != &expected {
            return Err(CoreError::IdentityMismatch(format!(
                "archive segment entry does not match hot command {}",
                admission.command_id
            )));
        }
        archived_event_ids.extend(admission.event_ids.iter().cloned());
    }
    if archived_event_ids != expected_event_ids {
        return Err(CoreError::IdentityMismatch(
            "archive segment applied Events do not equal the delta compaction cut".to_owned(),
        ));
    }
    Ok(count)
}

fn archive_batch_records(
    batches: &BTreeMap<String, MachineCommandBatchRecord>,
    batch_order: &[String],
    admissions: &[CommandAdmission],
    cut: CommandArchiveCut,
) -> Result<Vec<MachineCommandBatchRecord>> {
    let last_admission = admissions.last();
    let command_ids = admissions
        .iter()
        .map(|admission| admission.command_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut records = Vec::new();
    for admission in admissions {
        batches.get(&admission.batch_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "CommandAdmission {} has no batch {}",
                admission.admission_id, admission.batch_id
            ))
        })?;
    }
    let mut reached_cut = last_admission.is_none();
    for batch_id in batch_order {
        let batch = batches.get(batch_id).ok_or_else(|| {
            CoreError::NotFound(format!("ordered archive batch {batch_id} is unavailable"))
        })?;
        if reached_cut && cut == CommandArchiveCut::ThroughAdmission {
            break;
        }
        if !seen.insert(batch_id.clone()) {
            return Err(CoreError::IdentityMismatch(
                "command archive repeats an ordered batch".to_owned(),
            ));
        }
        batch.verify()?;
        if batch
            .members
            .iter()
            .any(|member| !command_ids.contains(member.command_id.as_str()))
        {
            return Err(CoreError::Causal(format!(
                "command batch {} crosses the archive cut",
                batch.batch_id
            )));
        }
        reached_cut |= last_admission.is_some_and(|admission| batch_id == &admission.batch_id);
        records.push(batch.clone());
    }
    if !reached_cut {
        return Err(CoreError::NotFound(
            "command archive batch order does not reach its admission cut".to_owned(),
        ));
    }
    Ok(records)
}

impl MachineSnapshot {
    /// Current snapshot schema version.
    pub const VERSION: &'static str = "cymule.machine-snapshot/11";

    /// Content digest used by conditional durable-store writes.
    ///
    /// # Errors
    ///
    /// Returns an encoding error when the snapshot cannot be serialized under
    /// Core's canonical JSON contract.
    pub fn digest(&self) -> Result<String> {
        canonical_digest(self)
    }

    /// Stable content digests for idempotent command records, keyed by command
    /// identity. Durable layers use this to validate an exact canonical delta
    /// without exposing the private command-record representation.
    ///
    /// # Errors
    ///
    /// Returns an encoding error when any retained command record cannot be
    /// serialized canonically.
    pub fn command_digests(&self) -> Result<BTreeMap<String, String>> {
        self.commands
            .iter()
            .map(|(command_id, record)| {
                canonical_digest(record).map(|digest| (command_id.clone(), digest))
            })
            .collect()
    }
}

/// In-memory reference machine for the Semantic Interpreter and Embedded
/// profiles. All ambient I/O belongs in higher-level runtime crates.
#[derive(Debug, Clone, Default)]
pub struct Machine {
    plans: BTreeMap<String, SealedPlan>,
    plan_order: Vec<String>,
    staged_plans: BTreeMap<String, SealedPlan>,
    artifacts: BTreeMap<String, ArtifactRecord>,
    artifact_order: Vec<String>,
    staged_artifacts: BTreeMap<String, ArtifactRecord>,
    batches: BTreeMap<String, MachineCommandBatchRecord>,
    batch_order: Vec<String>,
    events: BTreeMap<String, Event>,
    event_order: Vec<String>,
    base: Option<Arc<MachineBaseSnapshot>>,
    base_anchor: Option<MachineBaseAnchor>,
    compacted_event_ids: BTreeSet<String>,
    projection: Projection,
    admissions: Vec<CommandAdmission>,
    commands: BTreeMap<String, CommandRecord>,
    command_index_proofs: BTreeMap<String, MachineCommandIndexProof>,
    authority: MachineAuthority,
}

/// Exact touched-key undo for one reducer application. A Core Event changes
/// one Run and, for `FactRecorded`, at most one fact. Capturing those entries
/// keeps failed admission rollback proportional to the Event footprint.
#[derive(Debug, Clone)]
struct ProjectionEntryUndo {
    run_id: String,
    run_existed: bool,
    current_plan: Option<String>,
    plan_lineage_len: usize,
    current_binding_context: Option<String>,
    binding_lineage_len: usize,
    epoch: u64,
    execution_status: Option<crate::RunExecutionStatus>,
    world_settlement: Option<crate::WorldSettlementStatus>,
    result: Option<ArtifactRef>,
    last_event: Option<String>,
    scopes: Vec<ScopeProjectionUndo>,
    effects: BTreeMap<String, Option<crate::EffectProjection>>,
    obligations: BTreeMap<String, Option<ObligationProjection>>,
    attempts: BTreeMap<String, Option<crate::AttemptProjection>>,
    fact: Option<(String, Option<String>)>,
    derived: Option<RunDerivedIndexUndo>,
    projection_root: String,
}

#[derive(Default)]
struct RunProjectionEntriesUndo {
    scopes: Vec<ScopeProjectionUndo>,
    effects: BTreeMap<String, Option<crate::EffectProjection>>,
    obligations: BTreeMap<String, Option<ObligationProjection>>,
    attempts: BTreeMap<String, Option<crate::AttemptProjection>>,
}

impl RunProjectionEntriesUndo {
    fn capture(run: &RunProjection, payload: &EventPayload) -> Self {
        let mut undo = Self::default();
        match payload {
            EventPayload::AttemptStarted { attempt_id, .. }
            | EventPayload::AttemptYielded { attempt_id, .. } => {
                undo.attempts
                    .insert(attempt_id.clone(), run.attempts.get(attempt_id).cloned());
            }
            EventPayload::EpochAdvanced { .. } => undo.capture_active_attempt(run),
            EventPayload::ScopeOpened { scope_id, .. } => {
                undo.scopes.push(ScopeProjectionUndo::Inserted {
                    scope_id: scope_id.clone(),
                });
            }
            EventPayload::EffectProposed {
                intent_id,
                scope_id,
                ..
            } => {
                undo.effects
                    .insert(intent_id.clone(), run.effects.get(intent_id).cloned());
                undo.scopes.push(ScopeProjectionUndo::IntentMembership {
                    scope_id: scope_id.clone(),
                    intent_id: intent_id.clone(),
                    was_present: run
                        .scopes
                        .get(scope_id)
                        .is_some_and(|scope| scope.intents.contains(intent_id)),
                    order_len: run
                        .scopes
                        .get(scope_id)
                        .map_or(0, |scope| scope.intent_order.len()),
                });
            }
            EventPayload::EffectTransitioned { intent_id, .. } => {
                undo.capture_effect_transition(run, intent_id);
            }
            EventPayload::ScopeCommitted { scope_id, .. } => {
                undo.capture_scope_status(run, scope_id);
                if let Some(index) = run.derived.open_scope_effects.get(scope_id) {
                    for intent_id in &index.mutating_intents {
                        let obligation_id = effect_obligation_id(intent_id)
                            .expect("retained Effect identity derives an obligation");
                        undo.obligations.insert(
                            obligation_id.clone(),
                            run.obligations.get(&obligation_id).cloned(),
                        );
                    }
                }
            }
            EventPayload::ScopeAborted { scope_id } => {
                undo.capture_scope_status(run, scope_id);
                if let Some(index) = run.derived.open_scope_effects.get(scope_id) {
                    for intent_id in &index.abort_transition_intents {
                        undo.effects
                            .insert(intent_id.clone(), run.effects.get(intent_id).cloned());
                    }
                }
            }
            EventPayload::RunFailed { .. } | EventPayload::RunCancelled { .. } => {
                for scope_id in &run.derived.open_scope_ids {
                    undo.capture_scope_status(run, scope_id);
                }
                for intent_id in &run.derived.terminal_transition_effects {
                    undo.capture_effect_transition(run, intent_id);
                }
                undo.capture_active_attempt(run);
            }
            _ => {}
        }
        undo
    }

    fn capture_scope_status(&mut self, run: &RunProjection, scope_id: &str) {
        if let Some(scope) = run.scopes.get(scope_id) {
            self.scopes.push(ScopeProjectionUndo::Status {
                scope_id: scope_id.to_owned(),
                previous: scope.status,
            });
        }
    }

    fn capture_effect_transition(&mut self, run: &RunProjection, intent_id: &str) {
        self.effects
            .insert(intent_id.to_owned(), run.effects.get(intent_id).cloned());
        for obligation_id in run
            .derived
            .obligation_by_intent
            .get(intent_id)
            .into_iter()
            .flatten()
        {
            self.obligations.insert(
                obligation_id.clone(),
                run.obligations.get(obligation_id).cloned(),
            );
        }
    }

    fn capture_active_attempt(&mut self, run: &RunProjection) {
        if let Some(attempt_id) = run.active_attempt_id() {
            self.attempts
                .insert(attempt_id.to_owned(), run.attempts.get(attempt_id).cloned());
        }
    }
}

#[derive(Debug, Clone)]
struct RunDerivedIndexUndo {
    initialized: bool,
    active_attempt: Option<String>,
    committed_effect_count: u64,
    governance_membership: BTreeMap<String, bool>,
    unknown_membership: BTreeMap<String, bool>,
    pending_membership: BTreeMap<String, bool>,
    terminal_membership: BTreeMap<String, bool>,
    unresolved_obligation_membership: BTreeMap<String, bool>,
    obligation_by_intent: BTreeMap<String, Option<BTreeSet<String>>>,
    open_descendants: BTreeMap<String, Option<u64>>,
    open_scope_membership: BTreeMap<String, bool>,
    open_scope_entries: BTreeMap<String, Option<OpenScopeEffectIndex>>,
    open_scope_effect_membership: BTreeMap<(String, String), BTreeSet<OpenScopeEffectSet>>,
    effect_count_by_scope: BTreeMap<String, Option<u64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OpenScopeEffectSet {
    All,
    Mutating,
    AbortTransition,
    AbortBlocker,
}

#[derive(Debug, Clone)]
enum ScopeProjectionUndo {
    Inserted {
        scope_id: String,
    },
    Status {
        scope_id: String,
        previous: crate::ScopeStatus,
    },
    IntentMembership {
        scope_id: String,
        intent_id: String,
        was_present: bool,
        order_len: usize,
    },
}

#[derive(Debug)]
struct StagedMaterialAdmissionUndo {
    plan_start: usize,
    artifact_start: usize,
    plan_node_undos: Vec<AdmissionCommitmentUndo>,
    artifact_node_undos: Vec<AdmissionCommitmentUndo>,
    plans: Vec<SealedPlan>,
    artifacts: Vec<ArtifactRecord>,
}

impl StagedMaterialAdmissionUndo {
    fn rollback(self, machine: &mut Machine) {
        for plan_id in machine.plan_order.split_off(self.plan_start) {
            machine.plans.remove(&plan_id);
        }
        for undo in self.plan_node_undos.into_iter().rev() {
            machine.authority.plans.restore(undo);
        }
        for artifact_id in machine.artifact_order.split_off(self.artifact_start) {
            machine.artifacts.remove(&artifact_id);
        }
        for undo in self.artifact_node_undos.into_iter().rev() {
            machine.authority.artifacts.restore(undo);
        }
        machine.staged_plans.extend(
            self.plans
                .into_iter()
                .map(|plan| (plan.plan_id.clone(), plan)),
        );
        machine.staged_artifacts.extend(
            self.artifacts
                .into_iter()
                .map(|artifact| (artifact.reference.artifact_id.clone(), artifact)),
        );
    }
}

impl ProjectionEntryUndo {
    fn capture(machine: &Machine, event: &Event) -> Self {
        let fact = match &event.payload {
            EventPayload::FactRecorded { key, .. } => {
                Some((key.clone(), machine.projection.facts.get(key).cloned()))
            }
            _ => None,
        };
        let run = machine.projection.runs.get(&event.run_id);
        let RunProjectionEntriesUndo {
            scopes,
            effects,
            obligations,
            attempts,
        } = run.map_or_else(RunProjectionEntriesUndo::default, |run| {
            RunProjectionEntriesUndo::capture(run, &event.payload)
        });
        let derived =
            run.map(|run| RunDerivedIndexUndo::capture(run, event, &effects, &obligations));
        Self {
            run_id: event.run_id.clone(),
            run_existed: run.is_some(),
            current_plan: run.map(|run| run.current_plan.clone()),
            plan_lineage_len: run.map_or(0, |run| run.plan_lineage.len()),
            current_binding_context: run.map(|run| run.current_binding_context.clone()),
            binding_lineage_len: run.map_or(0, |run| run.binding_lineage.len()),
            epoch: run.map_or(0, |run| run.epoch),
            execution_status: run.map(|run| run.execution_status.clone()),
            world_settlement: run.map(|run| run.world_settlement),
            result: run.and_then(|run| run.result.clone()),
            last_event: run.map(|run| run.last_event.clone()),
            scopes,
            effects,
            obligations,
            attempts,
            fact,
            derived,
            projection_root: machine.authority.projection_root.clone(),
        }
    }

    fn rollback(self, machine: &mut Machine) {
        if !self.run_existed {
            machine.projection.runs.remove(&self.run_id);
        } else if let Some(run) = machine.projection.runs.get_mut(&self.run_id) {
            if let Some(current_plan) = self.current_plan {
                run.current_plan = current_plan;
            }
            run.plan_lineage.truncate(self.plan_lineage_len);
            if let Some(current_binding_context) = self.current_binding_context {
                run.current_binding_context = current_binding_context;
            }
            run.binding_lineage.truncate(self.binding_lineage_len);
            run.epoch = self.epoch;
            if let Some(execution_status) = self.execution_status {
                run.execution_status = execution_status;
            }
            if let Some(world_settlement) = self.world_settlement {
                run.world_settlement = world_settlement;
            }
            run.result = self.result;
            if let Some(last_event) = self.last_event {
                run.last_event = last_event;
            }
            for scope in self.scopes.into_iter().rev() {
                match scope {
                    ScopeProjectionUndo::Inserted { scope_id } => {
                        run.scopes.remove(&scope_id);
                    }
                    ScopeProjectionUndo::Status { scope_id, previous } => {
                        if let Some(scope) = run.scopes.get_mut(&scope_id) {
                            scope.status = previous;
                        }
                    }
                    ScopeProjectionUndo::IntentMembership {
                        scope_id,
                        intent_id,
                        was_present,
                        order_len,
                    } => {
                        if let Some(scope) = run.scopes.get_mut(&scope_id) {
                            if was_present {
                                scope.intents.insert(intent_id);
                            } else {
                                scope.intents.remove(&intent_id);
                            }
                            scope.intent_order.truncate(order_len);
                        }
                    }
                }
            }
            restore_projection_entries(&mut run.effects, self.effects);
            restore_projection_entries(&mut run.obligations, self.obligations);
            restore_projection_entries(&mut run.attempts, self.attempts);
        }
        if let Some((key, value)) = self.fact {
            match value {
                Some(value) => {
                    machine.projection.facts.insert(key, value);
                }
                None => {
                    machine.projection.facts.remove(&key);
                }
            }
        }
        if let Some(run) = machine.projection.runs.get_mut(&self.run_id)
            && let Some(derived) = self.derived
        {
            derived.rollback(&mut run.derived);
        }
        machine.authority.projection_root = self.projection_root;
    }
}

fn rollback_projection_batch(machine: &mut Machine, undos: Vec<ProjectionEntryUndo>) {
    for undo in undos.into_iter().rev() {
        undo.rollback(machine);
    }
}

impl RunDerivedIndexUndo {
    fn capture(
        run: &RunProjection,
        event: &Event,
        effects: &BTreeMap<String, Option<crate::EffectProjection>>,
        obligations: &BTreeMap<String, Option<ObligationProjection>>,
    ) -> Self {
        let effect_ids = effects.keys().cloned().collect::<BTreeSet<_>>();
        let mut obligation_intents = BTreeSet::new();
        if let EventPayload::ScopeCommitted { scope_id, .. } = &event.payload
            && let Some(index) = run.derived.open_scope_effects.get(scope_id)
        {
            obligation_intents.extend(index.mutating_intents.iter().cloned());
        }

        let (descendant_ids, scope_lifecycle_ids) = scope_lifecycle_footprint(run, &event.payload);

        let mut effect_scope_pairs = BTreeSet::new();
        let mut effect_count_scope_ids = BTreeSet::new();
        match &event.payload {
            EventPayload::EffectProposed {
                intent_id,
                scope_id,
                ..
            } => {
                effect_scope_pairs.insert((scope_id.clone(), intent_id.clone()));
                effect_count_scope_ids.insert(scope_id.clone());
            }
            EventPayload::EffectTransitioned { intent_id, .. } => {
                if let Some(effect) = run.effects.get(intent_id)
                    && run.derived.open_scope_ids.contains(&effect.scope_id)
                {
                    effect_scope_pairs.insert((effect.scope_id.clone(), intent_id.clone()));
                }
            }
            _ => {}
        }

        Self {
            initialized: run.derived.initialized,
            active_attempt: run.derived.active_attempt.clone(),
            committed_effect_count: run.derived.committed_effect_count,
            governance_membership: membership_snapshot(
                &run.derived.governance_effects,
                &effect_ids,
            ),
            unknown_membership: membership_snapshot(&run.derived.unknown_effects, &effect_ids),
            pending_membership: membership_snapshot(&run.derived.pending_effects, &effect_ids),
            terminal_membership: membership_snapshot(
                &run.derived.terminal_transition_effects,
                &effect_ids,
            ),
            unresolved_obligation_membership: membership_snapshot(
                &run.derived.unresolved_blocking_obligations,
                &obligations.keys().cloned().collect(),
            ),
            obligation_by_intent: obligation_intents
                .into_iter()
                .map(|intent_id| {
                    let previous = run.derived.obligation_by_intent.get(&intent_id).cloned();
                    (intent_id, previous)
                })
                .collect(),
            open_descendants: descendant_ids
                .into_iter()
                .map(|scope_id| {
                    let previous = run.derived.open_descendants.get(&scope_id).copied();
                    (scope_id, previous)
                })
                .collect(),
            open_scope_membership: scope_lifecycle_ids
                .iter()
                .map(|scope_id| {
                    (
                        scope_id.clone(),
                        run.derived.open_scope_ids.contains(scope_id),
                    )
                })
                .collect(),
            open_scope_entries: scope_lifecycle_ids
                .into_iter()
                .map(|scope_id| {
                    let previous = run.derived.open_scope_effects.get(&scope_id).cloned();
                    (scope_id, previous)
                })
                .collect(),
            open_scope_effect_membership: effect_scope_pairs
                .into_iter()
                .map(|(scope_id, intent_id)| {
                    let scope = run.derived.open_scope_effects.get(&scope_id);
                    let previous = open_scope_effect_membership(scope, &intent_id);
                    ((scope_id, intent_id), previous)
                })
                .collect(),
            effect_count_by_scope: effect_count_scope_ids
                .into_iter()
                .map(|scope_id| {
                    let previous = run.derived.effect_count_by_scope.get(&scope_id).copied();
                    (scope_id, previous)
                })
                .collect(),
        }
    }

    fn rollback(self, derived: &mut RunDerivedIndex) {
        derived.initialized = self.initialized;
        derived.active_attempt = self.active_attempt;
        derived.committed_effect_count = self.committed_effect_count;
        restore_set_membership(&mut derived.governance_effects, self.governance_membership);
        restore_set_membership(&mut derived.unknown_effects, self.unknown_membership);
        restore_set_membership(&mut derived.pending_effects, self.pending_membership);
        restore_set_membership(
            &mut derived.terminal_transition_effects,
            self.terminal_membership,
        );
        restore_set_membership(
            &mut derived.unresolved_blocking_obligations,
            self.unresolved_obligation_membership,
        );
        restore_projection_entries(&mut derived.obligation_by_intent, self.obligation_by_intent);
        restore_projection_entries(&mut derived.open_descendants, self.open_descendants);
        restore_set_membership(&mut derived.open_scope_ids, self.open_scope_membership);
        restore_projection_entries(&mut derived.open_scope_effects, self.open_scope_entries);
        for ((scope_id, intent_id), previous) in self.open_scope_effect_membership {
            if let Some(scope) = derived.open_scope_effects.get_mut(&scope_id) {
                restore_one_membership(
                    &mut scope.all_intents,
                    &intent_id,
                    previous.contains(&OpenScopeEffectSet::All),
                );
                restore_one_membership(
                    &mut scope.mutating_intents,
                    &intent_id,
                    previous.contains(&OpenScopeEffectSet::Mutating),
                );
                restore_one_membership(
                    &mut scope.abort_transition_intents,
                    &intent_id,
                    previous.contains(&OpenScopeEffectSet::AbortTransition),
                );
                restore_one_membership(
                    &mut scope.abort_blockers,
                    &intent_id,
                    previous.contains(&OpenScopeEffectSet::AbortBlocker),
                );
            }
        }
        restore_projection_entries(
            &mut derived.effect_count_by_scope,
            self.effect_count_by_scope,
        );
    }
}

fn scope_lifecycle_footprint(
    run: &RunProjection,
    payload: &EventPayload,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut descendant_ids = BTreeSet::new();
    let mut scope_lifecycle_ids = BTreeSet::new();
    match payload {
        EventPayload::ScopeOpened {
            scope_id,
            parent_scope,
            ..
        } => {
            scope_lifecycle_ids.insert(scope_id.clone());
            descendant_ids.insert(parent_scope.clone());
        }
        EventPayload::ScopeCommitted { scope_id, .. } | EventPayload::ScopeAborted { scope_id } => {
            scope_lifecycle_ids.insert(scope_id.clone());
            if let Some(parent_scope) = run
                .scopes
                .get(scope_id)
                .and_then(|scope| scope.parent_scope.as_ref())
            {
                descendant_ids.insert(parent_scope.clone());
            }
        }
        EventPayload::RunFailed { .. } | EventPayload::RunCancelled { .. } => {
            scope_lifecycle_ids.extend(run.derived.open_scope_ids.iter().cloned());
            descendant_ids.extend(run.derived.open_descendants.keys().cloned());
        }
        _ => {}
    }
    (descendant_ids, scope_lifecycle_ids)
}

fn open_scope_effect_membership(
    scope: Option<&OpenScopeEffectIndex>,
    intent_id: &str,
) -> BTreeSet<OpenScopeEffectSet> {
    let mut previous = BTreeSet::new();
    if scope.is_some_and(|scope| scope.all_intents.contains(intent_id)) {
        previous.insert(OpenScopeEffectSet::All);
    }
    if scope.is_some_and(|scope| scope.mutating_intents.contains(intent_id)) {
        previous.insert(OpenScopeEffectSet::Mutating);
    }
    if scope.is_some_and(|scope| scope.abort_transition_intents.contains(intent_id)) {
        previous.insert(OpenScopeEffectSet::AbortTransition);
    }
    if scope.is_some_and(|scope| scope.abort_blockers.contains(intent_id)) {
        previous.insert(OpenScopeEffectSet::AbortBlocker);
    }
    previous
}

fn membership_snapshot(
    source: &BTreeSet<String>,
    identities: &BTreeSet<String>,
) -> BTreeMap<String, bool> {
    identities
        .iter()
        .map(|identity| (identity.clone(), source.contains(identity)))
        .collect()
}

fn restore_set_membership(target: &mut BTreeSet<String>, entries: BTreeMap<String, bool>) {
    for (identity, present) in entries {
        restore_one_membership(target, &identity, present);
    }
}

fn restore_one_membership(target: &mut BTreeSet<String>, identity: &str, present: bool) {
    if present {
        target.insert(identity.to_owned());
    } else {
        target.remove(identity);
    }
}

fn restore_projection_entries<T>(
    target: &mut BTreeMap<String, T>,
    entries: BTreeMap<String, Option<T>>,
) {
    for (identity, value) in entries {
        match value {
            Some(value) => {
                target.insert(identity, value);
            }
            None => {
                target.remove(&identity);
            }
        }
    }
}

struct MachineRestoreCatalog {
    snapshot: MachineSnapshot,
    plans: BTreeMap<String, SealedPlan>,
    artifacts: BTreeMap<String, ArtifactRecord>,
}

impl MachineRestoreCatalog {
    fn new(snapshot: MachineSnapshot) -> Result<Self> {
        let plans = &snapshot.plans;
        let artifacts = &snapshot.artifacts;
        let plan_catalog = plans
            .iter()
            .map(|plan| (plan.plan_id.clone(), plan.clone()))
            .collect::<BTreeMap<_, _>>();
        let artifact_catalog = artifacts
            .iter()
            .map(|artifact| (artifact.reference.artifact_id.clone(), artifact.clone()))
            .collect::<BTreeMap<_, _>>();
        if plan_catalog.len() != plans.len() || artifact_catalog.len() != artifacts.len() {
            return Err(CoreError::Validation(
                "Machine material catalog repeats an immutable identity".to_owned(),
            ));
        }
        for plan in plans {
            plan.verify()?;
        }
        for artifact in artifacts {
            artifact.validate()?;
        }
        Ok(Self {
            snapshot,
            plans: plan_catalog,
            artifacts: artifact_catalog,
        })
    }
}

fn verify_restore_archive_authority(
    snapshot: &MachineSnapshot,
    expected_anchor: Option<&MachineBaseAnchor>,
    archive_segments: &[MachineCommandArchiveSegment],
) -> Result<()> {
    match (
        snapshot.base.as_ref(),
        snapshot.base_anchor.as_ref(),
        expected_anchor,
    ) {
        (None, None, None) if archive_segments.is_empty() => {}
        (Some(base), Some(snapshot_anchor), Some(expected))
            if snapshot_anchor == expected && archive_segments.is_empty() =>
        {
            expected.verify_trusted_base_fields(base)?;
        }
        (Some(base), Some(snapshot_anchor), None) => {
            base.verify()?;
            let derived = MachineBaseAnchor::from_verified_base(base)?;
            if snapshot_anchor != &derived {
                return Err(CoreError::IdentityMismatch(
                    "raw Machine base anchor does not match its base".to_owned(),
                ));
            }
            if archive_segments.is_empty() {
                return Err(CoreError::ArchivedCommandReplayRequired {
                    command_id: "machine:full-audit".to_owned(),
                    archive_head: base.archive_head.clone(),
                    command_index_root: base.command_index_root.clone(),
                });
            }
        }
        _ => {
            return Err(CoreError::Validation(
                "Machine base, anchor, and archive authority are inconsistent".to_owned(),
            ));
        }
    }
    Ok(())
}

fn hot_batch_archive_entries(
    batch_id: &str,
    batch: &MachineCommandBatchRecord,
    admissions_by_command: &BTreeMap<&str, &CommandAdmission>,
    retained_events: &BTreeMap<&str, &Event>,
    commands: &BTreeMap<String, CommandRecord>,
) -> Result<Vec<MachineCommandArchiveEntry>> {
    let mut batch_entries = Vec::with_capacity(batch.members.len());
    for member in &batch.members {
        let admission = admissions_by_command
            .get(member.command_id.as_str())
            .copied()
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "hot batch {batch_id} has no admission for {}",
                    member.command_id
                ))
            })?;
        let record = commands.get(&member.command_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "hot batch {batch_id} has no command {}",
                member.command_id
            ))
        })?;
        let batch_events = admission
            .event_ids
            .iter()
            .map(|event_id| {
                retained_events
                    .get(event_id.as_str())
                    .copied()
                    .cloned()
                    .ok_or_else(|| {
                        CoreError::NotFound(format!("hot Event {event_id} is not retained"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        batch_entries.push(MachineCommandArchiveEntry {
            admission: admission.clone(),
            command: ArchivedCommandRecord::from_private(record),
            events: batch_events,
        });
    }
    Ok(batch_entries)
}

struct EventCompactionCut {
    event_ids: Vec<String>,
    projection: Projection,
    projection_root: String,
    admission_index: usize,
}

struct ArchiveEntryInputs {
    entries: Vec<MachineCommandArchiveEntry>,
    index_proofs: Vec<MachineCommandIndexProof>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CommandArchiveCut {
    ThroughAdmission,
    CompleteEventFreeTail,
}

struct PreparedCommandArchive {
    segment: MachineCommandArchiveSegment,
    parent_index_root: String,
    result_index_root: String,
    index_nodes: CommandIndexNodeUpdates,
}

impl Machine {
    /// Create an empty semantic machine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the incrementally authenticated root of the live Machine.
    ///
    /// Unlike [`MachineSnapshot::digest`], this does not serialize or hash the
    /// complete materialized Machine. It combines fixed-size roots and chain
    /// heads that are maintained as each canonical input or admission enters.
    ///
    /// # Errors
    ///
    /// Returns an error when authority counts overflow or the fixed frontier
    /// cannot be serialized canonically.
    pub fn authority_root(&self) -> Result<String> {
        self.authority_root_with_batch(self.authority.batches.root(), self.authority.batch_count)
    }

    fn authority_root_with_batch(&self, batch_root: &str, batch_count: u64) -> Result<String> {
        let admission = self.admission_parent();
        let plan_count = u64::try_from(self.plans.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let artifact_count = u64::try_from(self.artifacts.len())
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let event_count = u64::try_from(self.event_order.len())
            .ok()
            .and_then(|hot| {
                hot.checked_add(
                    self.base
                        .as_ref()
                        .map_or(0, |base| base.archive_event_count),
                )
            })
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("Machine Event count overflowed".to_owned()))?;
        machine_authority_root(&MachineAuthorityRootInput {
            plan_commitment: self.authority.plans.root(),
            plan_count,
            artifact_commitment: self.authority.artifacts.root(),
            artifact_count,
            batch_commitment: batch_root,
            batch_count,
            projection_root: &self.authority.projection_root,
            event_count,
            admission_sequence: admission.map(|value| value.sequence),
            admission_head: admission.map(|value| value.admission_id),
        })
    }

    /// Return the authenticated reducer frontier without serializing the
    /// complete Projection.
    pub fn projection_root(&self) -> &str {
        &self.authority.projection_root
    }

    fn projection_root_after(&self, event: &Event) -> Result<String> {
        canonical_digest(&(
            PROJECTION_ROOT_EVENT_DOMAIN,
            self.authority.projection_root.as_str(),
            event.event_id.as_str(),
        ))
    }

    fn reset_projection_root_to_base(&mut self, base: &MachineBaseSnapshot) -> Result<()> {
        self.authority
            .projection_root
            .clone_from(&base.projection_root);
        for event_id in &self.event_order {
            let event = self.events.get(event_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "retained Event {event_id} is missing while rebuilding its projection root"
                ))
            })?;
            self.authority.projection_root = canonical_digest(&(
                PROJECTION_ROOT_EVENT_DOMAIN,
                self.authority.projection_root.as_str(),
                event.event_id.as_str(),
            ))?;
        }
        Ok(())
    }

    fn lightweight_event_authority(&self, projection: Projection) -> Self {
        #[cfg(test)]
        COMPACTION_AUTHORITY_BUILD_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        let mut authority = Self {
            plans: self.plans.clone(),
            plan_order: self.plan_order.clone(),
            artifacts: self.artifacts.clone(),
            artifact_order: self.artifact_order.clone(),
            projection,
            authority: self.authority.clone(),
            ..Self::new()
        };
        authority.authority.projection_root = self.base.as_ref().map_or_else(
            || {
                canonical_digest(&(PROJECTION_ROOT_GENESIS_DOMAIN, ()))
                    .expect("projection genesis is canonical")
            },
            |base| base.projection_root.clone(),
        );
        authority
    }

    /// Export canonical inputs for durable persistence.
    ///
    /// # Panics
    ///
    /// Panics only if an internal admission lineage names a Plan, Artifact, or
    /// Event absent from the same already-validated live Machine.
    pub fn snapshot(&self) -> MachineSnapshot {
        MachineSnapshot {
            snapshot_version: MachineSnapshot::VERSION.to_owned(),
            plans: self
                .plan_order
                .iter()
                .map(|plan_id| {
                    self.plans
                        .get(plan_id)
                        .cloned()
                        .expect("Plan admission lineage references retained Plan")
                })
                .collect(),
            artifacts: self
                .artifact_order
                .iter()
                .map(|artifact_id| {
                    self.artifacts
                        .get(artifact_id)
                        .cloned()
                        .expect("Artifact admission lineage references retained Artifact")
                })
                .collect(),
            batches: self
                .batch_order
                .iter()
                .map(|batch_id| {
                    self.batches
                        .get(batch_id)
                        .cloned()
                        .expect("batch admission lineage references retained batch")
                })
                .collect(),
            base: self.base.as_ref().map(|base| base.as_ref().clone()),
            base_anchor: self.base_anchor.clone(),
            events: self.events().cloned().collect(),
            admissions: self.admissions.clone(),
            commands: self.commands.clone(),
            command_index_proofs: self.command_index_proofs.clone(),
        }
    }

    /// Derive the exact externally pinnable anchor for the current compacted base.
    ///
    /// # Errors
    ///
    /// This fixed-size accessor is currently infallible; the result shape is
    /// retained as part of the Machine authority API.
    pub fn base_anchor(&self) -> Result<Option<MachineBaseAnchor>> {
        Ok(self.base_anchor.clone())
    }

    fn admission_parent(&self) -> Option<CommandAdmissionParent<'_>> {
        command_admission_parent(&self.admissions, self.base.as_deref())
    }

    /// Read the retained receipt for one command identity.
    ///
    /// Receipts outlive Event compaction and expose the original observed
    /// precondition needed to reconstruct an exact semantic replay request.
    ///
    /// # Errors
    ///
    /// Returns an error when retained command authority is inconsistent or the
    /// command may exist only in the authenticated archive.
    pub fn command_receipt(&self, command_id: &str) -> Result<Option<&CommandReceipt>> {
        let Some(record) = self.commands.get(command_id) else {
            if let Some(base) = &self.base {
                return Err(CoreError::ArchivedCommandReplayRequired {
                    command_id: command_id.to_owned(),
                    archive_head: base.archive_head.clone(),
                    command_index_root: base.command_index_root.clone(),
                });
            }
            return Ok(None);
        };
        self.verify_retained_command_record(command_id, record)?;
        Ok(Some(&record.receipt))
    }

    /// Resolve an exact retained command replay without mutating the Machine.
    ///
    /// The complete envelope, including actor and original precondition, must
    /// match the retained semantic hash. This remains authoritative after the
    /// admitted Event body has moved into the compacted prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope is invalid or reuses a retained
    /// command identity with different semantics.
    pub fn replay_command(&self, envelope: &CommandEnvelope) -> Result<Option<CommandReceipt>> {
        validate_envelope(envelope)?;
        let Some(record) = self.commands.get(&envelope.command_id) else {
            if let Some(base) = &self.base {
                return Err(CoreError::ArchivedCommandReplayRequired {
                    command_id: envelope.command_id.clone(),
                    archive_head: base.archive_head.clone(),
                    command_index_root: base.command_index_root.clone(),
                });
            }
            return Ok(None);
        };
        self.verify_retained_command_record(&envelope.command_id, record)?;
        if record.semantic_hash != canonical_digest(envelope)? {
            return Err(CoreError::CommandReuse(format!(
                "command ID {} was already used with different semantics",
                envelope.command_id
            )));
        }
        Ok(Some(record.receipt.clone()))
    }

    /// Resolve one exact historical command through an explicit archive proof.
    ///
    /// # Errors
    ///
    /// Returns an error when the envelope, archive membership, archived entry,
    /// or retained semantic identity does not match exactly.
    pub fn replay_archived_command(
        &self,
        envelope: &CommandEnvelope,
        proof: &MachineArchivedCommandProof,
        index_proof: &MachineCommandIndexProof,
    ) -> Result<CommandReceipt> {
        validate_envelope(envelope)?;
        let base = self.base.as_ref().ok_or_else(|| {
            CoreError::Validation("uncompacted Machine has no command archive".to_owned())
        })?;
        index_proof.verify(&base.command_index_root)?;
        proof.verify_entry()?;
        let value = index_proof.value.as_ref().ok_or_else(|| {
            CoreError::Validation("archived replay requires a membership proof".to_owned())
        })?;
        if index_proof.command_id != envelope.command_id
            || value.admission_id != proof.entry.admission.admission_id
            || value.archive_entry_digest != proof.entry.leaf_digest()?
            || proof.entry.command.envelope != *envelope
            || proof.entry.admission.command_id != envelope.command_id
            || proof.entry.command.semantic_hash != canonical_digest(envelope)?
        {
            return Err(CoreError::CommandReuse(format!(
                "archived command ID {} has different semantics",
                envelope.command_id
            )));
        }
        Ok(proof.entry.command.receipt.clone())
    }

    /// Apply one verified incremental Machine mutation without replaying the
    /// retained Event history.
    ///
    /// # Errors
    ///
    /// Returns an error when the delta is not an ordinary exact child of this
    /// Machine or its staged result fails canonical validation.
    pub fn apply_delta(&mut self, delta: &MachineDelta) -> Result<()> {
        if delta.base.is_some() {
            return Err(CoreError::Validation(
                "Machine compaction requires explicit archive-segment application".to_owned(),
            ));
        }
        validate_delta_parent(&self.snapshot(), delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, None)?;
        validate_delta_result(&staged.snapshot(), delta)?;
        *self = staged;
        Ok(())
    }

    /// Apply one compaction delta only with the exact independently persisted segment.
    ///
    /// # Errors
    ///
    /// Returns an error unless the compaction delta and segment are exact
    /// children of this Machine and produce their declared canonical result.
    pub fn apply_compaction_delta(
        &mut self,
        delta: &MachineDelta,
        archive_segment: &MachineCommandArchiveSegment,
    ) -> Result<()> {
        validate_delta_parent(&self.snapshot(), delta)?;
        let mut staged = self.clone();
        staged.apply_delta_in_place(delta, Some(archive_segment))?;
        validate_delta_result(&staged.snapshot(), delta)?;
        *self = staged;
        Ok(())
    }

    fn apply_delta_in_place(
        &mut self,
        delta: &MachineDelta,
        archive_segment: Option<&MachineCommandArchiveSegment>,
    ) -> Result<()> {
        if delta.delta_version != MachineDelta::VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported machine delta version {:?}",
                delta.delta_version
            )));
        }
        self.apply_delta_compaction(delta, archive_segment)?;
        self.apply_delta_command_batches(delta)
    }

    fn apply_delta_compaction(
        &mut self,
        delta: &MachineDelta,
        archive_segment: Option<&MachineCommandArchiveSegment>,
    ) -> Result<()> {
        match &delta.base {
            None if delta.compacted_event_ids.is_empty() => {}
            Some(base) => {
                base.verify()?;
                verify_delta_archive_transition(
                    self.base.as_deref(),
                    base,
                    delta.archive_segment.as_ref(),
                )?;
                self.verify_delta_compacted_projection(delta, base)?;
                let count = delta.compacted_event_ids.len();
                let archived_admission_count =
                    self.verify_delta_compacted_admissions(delta, base, archive_segment)?;
                self.validate_projection_authority(&base.projection)?;
                for event_id in &delta.compacted_event_ids {
                    self.events.remove(event_id);
                }
                self.event_order.drain(..count);
                for admission in self.admissions.drain(..archived_admission_count) {
                    self.commands.remove(&admission.command_id);
                    self.command_index_proofs.remove(&admission.command_id);
                }
                self.batch_order
                    .retain(|id| !delta.compacted_batch_ids.contains(id));
                for id in &delta.compacted_batch_ids {
                    self.batches.remove(id);
                }
                self.compacted_event_ids = base.event_ids();
                let anchor = delta.base_anchor.as_ref().ok_or_else(|| {
                    CoreError::NotFound("Machine delta compaction has no base anchor".to_owned())
                })?;
                anchor.verify_trusted_base_fields(base)?;
                self.base_anchor = Some(anchor.clone());
                self.base = Some(Arc::new(base.clone()));
                self.reset_projection_root_to_base(base)?;
                self.command_index_proofs = delta
                    .command_index_proofs
                    .iter()
                    .filter(|(command_id, _)| self.commands.contains_key(*command_id))
                    .map(|(command_id, proof)| (command_id.clone(), proof.clone()))
                    .collect();
            }
            None => {
                return Err(CoreError::Validation(
                    "Machine delta compaction requires both a base and an Event prefix".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn verify_delta_compacted_projection(
        &self,
        delta: &MachineDelta,
        base: &MachineBaseSnapshot,
    ) -> Result<()> {
        let count = delta.compacted_event_ids.len();
        if self.event_order.get(..count) != Some(delta.compacted_event_ids.as_slice()) {
            return Err(CoreError::Validation(
                "Machine delta compaction does not match the retained Event prefix".to_owned(),
            ));
        }
        let projected = self
            .base
            .as_ref()
            .map(|value| value.projection.clone())
            .unwrap_or_default();
        let mut authority = self.lightweight_event_authority(projected);
        for event_id in &delta.compacted_event_ids {
            let event = self.events.get(event_id).ok_or_else(|| {
                CoreError::NotFound(format!("compacted Event {event_id} does not exist"))
            })?;
            authority.validate_event_authority(event)?;
            authority.projection.apply_event(event)?;
            let record = self.commands.get(&event.command_id).ok_or_else(|| {
                CoreError::NotFound(format!("compacted Event {event_id} has no command record"))
            })?;
            if record.semantic_hash != event.command_hash
                || !record.receipt.event_ids.contains(&event.event_id)
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "compacted Event {event_id} evidence does not match"
                )));
            }
        }
        if authority.projection != base.projection {
            return Err(CoreError::IdentityMismatch(
                "Machine delta compacted projection does not match its Event prefix".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_delta_compacted_admissions(
        &self,
        delta: &MachineDelta,
        base: &MachineBaseSnapshot,
        archive_segment: Option<&MachineCommandArchiveSegment>,
    ) -> Result<usize> {
        let archive_segment = archive_segment.ok_or_else(|| {
            CoreError::NotFound("Machine compaction application has no archive segment".to_owned())
        })?;
        if delta.archive_segment.as_ref() != Some(&archive_segment.header) {
            return Err(CoreError::IdentityMismatch(
                "Machine compaction segment header does not match its delta".to_owned(),
            ));
        }
        let archived_admission_count = verify_archive_segment_entries(
            &self.admissions,
            &self.commands,
            &self.events,
            archive_segment,
            &delta.compacted_event_ids,
        )?;
        let expected_archived_count = base
            .archive_count
            .checked_sub(self.base.as_ref().map_or(0, |value| value.archive_count))
            .ok_or_else(|| {
                CoreError::Validation("Machine delta archive count moved backwards".to_owned())
            })?;
        let expected_archived_count = usize::try_from(expected_archived_count)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        if archived_admission_count != expected_archived_count {
            return Err(CoreError::IdentityMismatch(
                "Machine compaction archive entry count does not match its base cut".to_owned(),
            ));
        }
        verify_delta_compaction_removals(
            delta,
            self.admissions
                .get(..archived_admission_count)
                .ok_or_else(|| {
                    CoreError::NotFound(
                        "Machine delta compaction removal prefix is absent".to_owned(),
                    )
                })?,
            &self.commands,
            &self.command_index_proofs,
        )?;
        verify_compaction_admission_frontier(
            base,
            self.base.as_deref(),
            &self.admissions,
            archived_admission_count,
        )?;
        let ordered_batches = self
            .batch_order
            .iter()
            .map(|id| {
                self.batches.get(id).ok_or_else(|| {
                    CoreError::NotFound(format!("ordered archive batch {id} is unavailable"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        verify_delta_compaction_batches(
            delta,
            ordered_batches.into_iter().chain(&delta.batches),
            archive_segment,
            self.base.as_deref(),
            base,
        )?;
        Ok(archived_admission_count)
    }

    fn apply_delta_command_batches(&mut self, delta: &MachineDelta) -> Result<()> {
        let new_events = delta
            .events
            .iter()
            .map(|event| (event.event_id.as_str(), event))
            .collect::<BTreeMap<_, _>>();
        let mut claimed_events = BTreeSet::new();
        let mut claimed_commands = BTreeSet::new();
        let mut entries = Vec::new();
        let command_index_root = current_command_index_root(self.base.as_deref())?;
        for admission in &delta.admissions {
            let command_id = &admission.command_id;
            let record = delta.commands.get(command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "Machine delta CommandAdmission {} has no command record",
                    admission.admission_id
                ))
            })?;
            if command_id != &record.receipt.command_id
                || self.commands.contains_key(command_id)
                || !claimed_commands.insert(command_id.clone())
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "Machine delta command {command_id} conflicts with retained authority"
                )));
            }
            verify_admission_record(admission, record)?;
            let index_proof = delta.command_index_proofs.get(command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "Machine delta command {command_id} has no index non-membership proof"
                ))
            })?;
            index_proof.verify(&command_index_root)?;
            if index_proof.command_id != *command_id || index_proof.value.is_some() {
                return Err(CoreError::IdentityMismatch(
                    "Machine delta command has no exact absence proof".to_owned(),
                ));
            }
            let mut events = Vec::new();
            for event_id in &record.receipt.event_ids {
                let expected = new_events.get(event_id.as_str()).ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "Machine delta command {command_id} references a non-delta Event {event_id}"
                    ))
                })?;
                if !claimed_events.insert(event_id.clone()) {
                    return Err(CoreError::IdentityMismatch(format!(
                        "Machine delta command {command_id} does not uniquely match Event {event_id}"
                    )));
                }
                events.push((*expected).clone());
            }
            entries.push(MachineCommandArchiveEntry {
                admission: admission.clone(),
                command: ArchivedCommandRecord::from_private(record),
                events,
            });
        }
        if claimed_events.len() != delta.events.len()
            || claimed_commands.len() != delta.commands.len()
        {
            return Err(CoreError::NotFound(
                "Machine delta Event or command has no unique CommandAdmission".to_owned(),
            ));
        }
        let entries = entries
            .iter()
            .map(|entry| (entry.admission.command_id.as_str(), entry))
            .collect();
        let plans = delta
            .plans
            .iter()
            .map(|plan| (plan.plan_id.clone(), plan.clone()))
            .collect();
        let artifacts = delta
            .artifacts
            .iter()
            .map(|artifact| (artifact.reference.artifact_id.clone(), artifact.clone()))
            .collect();
        let mut sources = MachineReplayAncestry::default();
        let initial_count = self.commands.len();
        for batch in &delta.batches {
            self.replay_archived_batch(batch, &entries, &plans, &artifacts, &mut sources)?;
        }
        if self.commands.len() != initial_count + delta.commands.len() {
            return Err(CoreError::IdentityMismatch(
                "Machine delta command batch closure is incomplete".to_owned(),
            ));
        }
        self.command_index_proofs
            .extend(delta.command_index_proofs.clone());
        self.validate_projection_authority(&self.projection)?;
        if self.commands.keys().ne(self.command_index_proofs.keys()) {
            return Err(CoreError::IdentityMismatch(
                "Machine hot commands and command index proofs do not match".to_owned(),
            ));
        }
        let retained_events = self.events().cloned().collect::<Vec<_>>();
        verify_command_event_closure(&retained_events, &self.commands)?;
        Ok(())
    }

    /// Restore a Machine by replaying the complete ordered `CommandAdmission` chain.
    ///
    /// # Errors
    ///
    /// Returns an error when an uncompacted snapshot does not reconstruct one
    /// complete canonical Machine authority.
    pub fn restore(snapshot: MachineSnapshot) -> Result<Self> {
        Self::restore_hot_internal(snapshot, None, &[]).map(|(machine, _)| machine)
    }

    /// Fully audit a compacted snapshot using its complete independent archive chain.
    ///
    /// # Errors
    ///
    /// Returns an error when the archive chain or hot suffix is incomplete,
    /// discontinuous, malformed, or reconstructs different authority.
    pub fn restore_with_archive(
        snapshot: MachineSnapshot,
        archive_segments: impl IntoIterator<Item = MachineCommandArchiveSegment>,
    ) -> Result<Self> {
        let archive_segments = archive_segments.into_iter().collect::<Vec<_>>();
        Self::restore_hot_internal(snapshot, None, &archive_segments).map(|(machine, _)| machine)
    }

    /// Restore from one caller-supplied exact trusted base anchor and replay only
    /// admissions after its cut. The caller must already have authenticated the
    /// supplied base bytes under `expected_anchor.base_id` through its Store head;
    /// use [`Self::restore`] for untrusted standalone snapshot bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the trusted anchor does not bind the exact base or
    /// the retained hot suffix fails canonical replay.
    pub fn restore_anchored(
        snapshot: MachineSnapshot,
        expected_anchor: &MachineBaseAnchor,
    ) -> Result<Self> {
        Self::restore_hot_internal(snapshot, Some(expected_anchor), &[]).map(|(machine, _)| machine)
    }

    fn restore_hot_internal(
        snapshot: MachineSnapshot,
        expected_anchor: Option<&MachineBaseAnchor>,
        archive_segments: &[MachineCommandArchiveSegment],
    ) -> Result<(Self, u64)> {
        if snapshot.snapshot_version != MachineSnapshot::VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported machine snapshot version {:?}",
                snapshot.snapshot_version
            )));
        }
        let _portable_snapshot_digest = snapshot.digest()?;
        verify_restore_archive_authority(&snapshot, expected_anchor, archive_segments)?;
        let catalog = MachineRestoreCatalog::new(snapshot)?;
        let snapshot = &catalog.snapshot;
        let mut machine = Self::new();
        let mut replay_source_roots = MachineReplayAncestry::default();
        let reducer_count = machine.restore_base_projection(
            snapshot,
            expected_anchor,
            archive_segments,
            &catalog,
            &mut replay_source_roots,
        )?;
        let reducer_count = machine.replay_hot_snapshot(
            snapshot,
            &catalog,
            &mut replay_source_roots,
            reducer_count,
        )?;
        machine.validate_projection_authority(&machine.projection)?;
        if machine.snapshot() != *snapshot {
            return Err(CoreError::IdentityMismatch(
                "restored Machine does not reproduce the exact hot snapshot".to_owned(),
            ));
        }
        Ok((machine, reducer_count))
    }

    fn restore_base_projection(
        &mut self,
        snapshot: &MachineSnapshot,
        expected_anchor: Option<&MachineBaseAnchor>,
        archive_segments: &[MachineCommandArchiveSegment],
        catalog: &MachineRestoreCatalog,
        replay_source_roots: &mut MachineReplayAncestry,
    ) -> Result<u64> {
        if expected_anchor.is_some() {
            let base = snapshot.base.as_ref().ok_or_else(|| {
                CoreError::NotFound("anchored restore has no Machine base".to_owned())
            })?;
            let plan_count = usize::try_from(base.plan_count)
                .map_err(|error| CoreError::Validation(error.to_string()))?;
            let artifact_count = usize::try_from(base.artifact_count)
                .map_err(|error| CoreError::Validation(error.to_string()))?;
            for plan in snapshot.plans.get(..plan_count).ok_or_else(|| {
                CoreError::NotFound("Machine base Plan prefix is unavailable".to_owned())
            })? {
                self.retain_plan(plan.clone())?;
            }
            for artifact in snapshot.artifacts.get(..artifact_count).ok_or_else(|| {
                CoreError::NotFound("Machine base Artifact prefix is unavailable".to_owned())
            })? {
                self.retain_artifact(artifact.clone())?;
            }
        }
        let mut reducer_count = 0_u64;
        if let Some(base) = &snapshot.base {
            if expected_anchor.is_none() {
                reducer_count = self.audit_archive_chain(
                    base,
                    archive_segments,
                    &catalog.plans,
                    &catalog.artifacts,
                    replay_source_roots,
                )?;
            } else {
                self.validate_projection_authority(&base.projection)?;
                self.projection = base.projection.clone();
                self.projection.rebuild_derived_indexes()?;
                self.authority
                    .batches
                    .root
                    .clone_from(&base.batch_admission_commitment);
                self.authority.batch_count = base.batch_count;
            }
            if self.authority.plans.root() != base.plan_admission_commitment
                || u64::try_from(self.plans.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?
                    != base.plan_count
            {
                return Err(CoreError::IdentityMismatch(
                    "Machine base Plan admission commitment or count does not match restored material"
                        .to_owned(),
                ));
            }
            if self.authority.artifacts.root() != base.artifact_admission_commitment
                || u64::try_from(self.artifacts.len())
                    .map_err(|error| CoreError::Validation(error.to_string()))?
                    != base.artifact_count
            {
                return Err(CoreError::IdentityMismatch(
                    "Machine base Artifact admission commitment or count does not match restored material"
                        .to_owned(),
                ));
            }
            if self.authority.batches.root() != base.batch_admission_commitment
                || self.authority.batch_count != base.batch_count
            {
                return Err(CoreError::IdentityMismatch(
                    "Machine base batch authority does not match its archive".to_owned(),
                ));
            }
            self.events.clear();
            self.event_order.clear();
            self.commands.clear();
            self.command_index_proofs.clear();
            self.admissions.clear();
            self.batches.clear();
            self.batch_order.clear();
            self.compacted_event_ids = base.event_ids();
            self.base = Some(Arc::new(base.clone()));
            self.base_anchor.clone_from(&snapshot.base_anchor);
            self.reset_projection_root_to_base(base)?;
        }
        Ok(reducer_count)
    }

    fn replay_hot_snapshot(
        &mut self,
        snapshot: &MachineSnapshot,
        catalog: &MachineRestoreCatalog,
        replay_source_roots: &mut MachineReplayAncestry,
        mut reducer_count: u64,
    ) -> Result<u64> {
        let MachineSnapshot {
            events,
            admissions,
            commands,
            command_index_proofs,
            ..
        } = snapshot;
        let expected_batch_order = snapshot
            .batches
            .iter()
            .map(|batch| batch.batch_id.clone())
            .collect::<Vec<_>>();
        let expected_batches = snapshot
            .batches
            .iter()
            .cloned()
            .map(|batch| (batch.batch_id.clone(), batch))
            .collect::<BTreeMap<_, _>>();
        let retained_events = events
            .iter()
            .map(|event| (event.event_id.as_str(), event))
            .collect::<BTreeMap<_, _>>();
        let mut replayed_events = BTreeSet::new();
        let admissions_by_command = admissions
            .iter()
            .map(|admission| (admission.command_id.as_str(), admission))
            .collect::<BTreeMap<_, _>>();
        for batch_id in &expected_batch_order {
            let batch = expected_batches.get(batch_id).ok_or_else(|| {
                CoreError::NotFound(format!("hot batch {batch_id} is unavailable"))
            })?;
            let batch_entries = hot_batch_archive_entries(
                batch_id,
                batch,
                &admissions_by_command,
                &retained_events,
                commands,
            )?;
            let batch_entry_map = batch_entries
                .iter()
                .map(|entry| (entry.admission.command_id.as_str(), entry))
                .collect::<BTreeMap<_, _>>();
            reducer_count = reducer_count
                .checked_add(self.replay_archived_batch(
                    batch,
                    &batch_entry_map,
                    &catalog.plans,
                    &catalog.artifacts,
                    replay_source_roots,
                )?)
                .ok_or_else(|| {
                    CoreError::Validation("restore reducer count overflowed".to_owned())
                })?;
            replayed_events.extend(batch.event_ids.iter().cloned());
        }
        self.command_index_proofs.clone_from(command_index_proofs);
        let command_index_root = current_command_index_root(self.base.as_deref())?;
        if command_index_proofs.keys().ne(commands.keys()) {
            return Err(CoreError::IdentityMismatch(
                "hot command proofs do not close the command set".to_owned(),
            ));
        }
        for (command_id, proof) in command_index_proofs {
            proof.verify(&command_index_root)?;
            if proof.command_id != *command_id || proof.value.is_some() {
                return Err(CoreError::IdentityMismatch(
                    "hot command proof is not its exact non-membership witness".to_owned(),
                ));
            }
        }
        if replayed_events.len() != events.len()
            || &self.commands != commands
            || &self.command_index_proofs != command_index_proofs
            || &self.admissions != admissions
            || self.batches != expected_batches
        {
            return Err(CoreError::IdentityMismatch(
                "hot command catalog does not match its admissions and Events".to_owned(),
            ));
        }
        Ok(reducer_count)
    }

    fn audit_archive_chain(
        &mut self,
        base: &MachineBaseSnapshot,
        segments: &[MachineCommandArchiveSegment],
        plans: &BTreeMap<String, SealedPlan>,
        artifacts: &BTreeMap<String, ArtifactRecord>,
        replay_source_roots: &mut MachineReplayAncestry,
    ) -> Result<u64> {
        let mut expected_parent_segment: Option<&str> = None;
        let mut expected_parent_count = 0_u64;
        let mut expected_parent_event_count = 0_u64;
        let mut expected_parent_admission: Option<&str> = None;
        let mut expected_command_index_root = MachineCommandIndexProof::empty_root()?;
        let mut reducer_count = 0_u64;
        for segment in segments {
            segment.verify()?;
            if segment.header.parent_segment.as_deref() != expected_parent_segment
                || segment.header.parent_count != expected_parent_count
                || segment.header.parent_event_count != expected_parent_event_count
                || segment.header.parent_admission_head.as_deref() != expected_parent_admission
                || segment.header.parent_command_index_root != expected_command_index_root
            {
                return Err(CoreError::Causal(
                    "command archive segment chain is discontinuous".to_owned(),
                ));
            }
            let entries = segment
                .entries
                .iter()
                .map(|entry| (entry.admission.command_id.as_str(), entry))
                .collect::<BTreeMap<_, _>>();
            for batch in &segment.batches {
                reducer_count = reducer_count
                    .checked_add(self.replay_archived_batch(
                        batch,
                        &entries,
                        plans,
                        artifacts,
                        replay_source_roots,
                    )?)
                    .ok_or_else(|| {
                        CoreError::Validation("archive audit reducer count overflowed".to_owned())
                    })?;
            }
            self.advance_archive_audit_anchor(segment)?;
            expected_parent_segment = Some(&segment.header.segment_id);
            expected_parent_count = segment.header.result_count;
            expected_parent_event_count = segment.header.result_event_count;
            expected_parent_admission = segment.header.result_admission_head.as_deref();
            expected_command_index_root.clone_from(&segment.header.result_command_index_root);
        }
        if expected_parent_segment != Some(base.archive_head.as_str())
            || expected_parent_count != base.archive_count
            || expected_parent_event_count != base.archive_event_count
            || expected_parent_admission != base.admission_head.as_deref()
            || expected_command_index_root != base.command_index_root
            || self.projection != base.projection
            || self.authority.projection_root != base.projection_root
        {
            return Err(CoreError::IdentityMismatch(
                "command archive audit does not reach the Machine base".to_owned(),
            ));
        }
        Ok(reducer_count)
    }

    fn replay_archived_batch(
        &mut self,
        batch: &MachineCommandBatchRecord,
        entries: &BTreeMap<&str, &MachineCommandArchiveEntry>,
        plans: &BTreeMap<String, SealedPlan>,
        artifacts: &BTreeMap<String, ArtifactRecord>,
        replay_source_roots: &mut MachineReplayAncestry,
    ) -> Result<u64> {
        batch.verify()?;
        let actual_parent = replay_source_roots.observe(self)?;
        if actual_parent != batch.admission_parent_authority_root {
            return Err(CoreError::IdentityMismatch(format!(
                "archived batch {} ({}) parent is {actual_parent}, not {}",
                batch.batch_id,
                batch
                    .members
                    .first()
                    .map_or("missing", |member| member.command_id.as_str()),
                batch.admission_parent_authority_root
            )));
        }
        replay_source_roots.verify_source(
            batch,
            batch
                .members
                .iter()
                .filter_map(|member| entries.get(member.command_id.as_str()))
                .map(|entry| entry.command.envelope.run_id.clone()),
        )?;
        self.verify_batch_material_source_from_catalog(batch, plans, artifacts)?;
        if batch.admits_material() {
            self.admit_batch_material_from_catalog(batch, plans, artifacts)?;
        }
        for member in &batch.members {
            let entry = entries
                .get(member.command_id.as_str())
                .copied()
                .ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "archived batch {} is missing command {}",
                        batch.batch_id, member.command_id
                    ))
                })?;
            batch.verify_entry(entry)?;
            self.replay_batch_entry(entry)?;
            if entry.admission.status == CommandReceiptStatus::Applied {
                replay_source_roots.run_admissions.insert(
                    entry.command.envelope.run_id.clone(),
                    entry.admission.sequence,
                );
            }
        }
        self.insert_batch(batch.clone())?;
        let actual_result = self.authority_root()?;
        if actual_result != batch.result_authority_root {
            return Err(CoreError::IdentityMismatch(format!(
                "archived batch {} reaches {actual_result}, not {}",
                batch.batch_id, batch.result_authority_root
            )));
        }
        replay_source_roots.observe(self)?;
        u64::try_from(batch.members.len()).map_err(|error| CoreError::Validation(error.to_string()))
    }

    fn replay_batch_entry(&mut self, entry: &MachineCommandArchiveEntry) -> Result<()> {
        entry.verify()?;
        entry.admission.verify(self.admission_parent())?;
        let record = entry.command.to_private();
        if self.commands.contains_key(&record.envelope.command_id) {
            return Err(CoreError::IdentityMismatch(
                "replay repeats a command".to_owned(),
            ));
        }
        let before = self.authority.projection_root.clone();
        let conflict = self.stale_command_receipt(&record.envelope)?;
        let events = if conflict.is_some() {
            Vec::new()
        } else {
            self.command_event_batch(&record.envelope, &record.semantic_hash)?
        };
        if events != entry.events {
            return Err(CoreError::IdentityMismatch(
                "archived command did not reproduce its exact Event batch".to_owned(),
            ));
        }
        let (undos, after) = self.apply_command_event_batch(&events)?;
        let receipt =
            conflict.unwrap_or_else(|| self.applied_command_receipt(&record.envelope, &events));
        let admission =
            CommandAdmission::new(self.admission_parent(), &record, before, after.clone());
        if receipt != record.receipt || admission.as_ref() != Ok(&entry.admission) {
            rollback_projection_batch(self, undos);
            return Err(CoreError::IdentityMismatch(
                "archived command did not reproduce its receipt or admission".to_owned(),
            ));
        }
        self.authority.projection_root = after;
        for event in events {
            self.append_event(event);
        }
        self.command_index_proofs.insert(
            record.envelope.command_id.clone(),
            MachineCommandIndexProof::empty_nonmembership(&record.envelope.command_id)?,
        );
        self.commands
            .insert(record.envelope.command_id.clone(), record);
        self.admissions.push(entry.admission.clone());
        Ok(())
    }

    fn verify_batch_material_source_from_catalog(
        &self,
        batch: &MachineCommandBatchRecord,
        plans: &BTreeMap<String, SealedPlan>,
        artifacts: &BTreeMap<String, ArtifactRecord>,
    ) -> Result<()> {
        let Some(source) = &batch.material_source else {
            return Ok(());
        };
        let source_plans = source
            .plan_ids
            .iter()
            .map(|plan_id| {
                plans
                    .get(plan_id)
                    .or_else(|| self.plans.get(plan_id))
                    .cloned()
                    .ok_or_else(|| {
                        CoreError::NotFound(format!("batch Plan {plan_id} is unavailable"))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let source_artifacts = source
            .artifacts
            .iter()
            .map(|reference| self.batch_artifact_from_catalog(reference, artifacts))
            .collect::<Result<Vec<_>>>()?;
        let material = pinned::MachineMaterialAdmission::new(
            source.source_command_id.clone(),
            source_plans,
            source_artifacts,
        )?;
        if batch.material_digest.as_deref() != Some(material.material_digest()) {
            return Err(CoreError::IdentityMismatch(
                "batch material digest does not match its complete source".to_owned(),
            ));
        }
        Ok(())
    }

    fn batch_artifact_from_catalog(
        &self,
        reference: &ArtifactRef,
        artifacts: &BTreeMap<String, ArtifactRecord>,
    ) -> Result<ArtifactRecord> {
        let artifact = artifacts
            .get(&reference.artifact_id)
            .or_else(|| self.artifacts.get(&reference.artifact_id))
            .cloned()
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "batch Artifact {} is unavailable",
                    reference.artifact_id
                ))
            })?;
        if artifact.reference != *reference {
            return Err(CoreError::IdentityMismatch(format!(
                "batch Artifact {} changed reference",
                reference.artifact_id
            )));
        }
        Ok(artifact)
    }

    fn admit_batch_material_from_catalog(
        &mut self,
        batch: &MachineCommandBatchRecord,
        plans: &BTreeMap<String, SealedPlan>,
        artifacts: &BTreeMap<String, ArtifactRecord>,
    ) -> Result<()> {
        for plan_id in &batch.plan_ids {
            if !self.plans.contains_key(plan_id) {
                self.retain_plan(plans.get(plan_id).cloned().ok_or_else(|| {
                    CoreError::NotFound(format!("batch Plan {plan_id} is unavailable"))
                })?)?;
            }
        }
        for reference in &batch.artifacts {
            if self.artifacts.contains_key(&reference.artifact_id) {
                continue;
            }
            let artifact = self.batch_artifact_from_catalog(reference, artifacts)?;
            self.retain_artifact(artifact)?;
        }
        Ok(())
    }

    fn advance_archive_audit_anchor(
        &mut self,
        segment: &MachineCommandArchiveSegment,
    ) -> Result<()> {
        let projection_digest = self.projection.digest()?;
        let projection_root = self.authority.projection_root.clone();
        let prefix_digest = machine_prefix_digest(
            &segment.header.segment_id,
            segment.header.result_count,
            segment.header.result_event_count,
            segment.header.result_admission_head.as_deref(),
            &segment.header.result_command_index_root,
            &projection_digest,
            &projection_root,
        )?;
        let base = MachineBaseSnapshot {
            prefix_digest,
            archive_head: segment.header.segment_id.clone(),
            archive_count: segment.header.result_count,
            archive_event_count: segment.header.result_event_count,
            admission_head: segment.header.result_admission_head.clone(),
            command_index_root: segment.header.result_command_index_root.clone(),
            plan_admission_commitment: self.authority.plans.root().to_owned(),
            plan_count: u64::try_from(self.plans.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            artifact_admission_commitment: self.authority.artifacts.root().to_owned(),
            artifact_count: u64::try_from(self.artifacts.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            batch_admission_commitment: self.authority.batches.root().to_owned(),
            batch_count: self.authority.batch_count,
            projection: self.projection.clone(),
            projection_digest,
            projection_root,
        };
        base.verify()?;
        self.base_anchor = Some(MachineBaseAnchor::from_verified_base(&base)?);
        self.compacted_event_ids = base.event_ids();
        self.base = Some(Arc::new(base));
        self.events.clear();
        self.event_order.clear();
        self.commands.clear();
        self.command_index_proofs.clear();
        self.admissions.clear();
        self.batches.clear();
        self.batch_order.clear();
        Ok(())
    }

    fn admission_frontier_at_cut(
        &self,
        batches: &[MachineCommandBatchRecord],
    ) -> Result<MachineCutAdmissionFrontier> {
        let mut cut = MachineCutAdmissionFrontier {
            plans: AdmissionCommitment::new(MACHINE_PLAN_ADMISSION_COMMITMENT_DOMAIN),
            plan_count: 0,
            artifacts: AdmissionCommitment::new(MACHINE_ARTIFACT_ADMISSION_COMMITMENT_DOMAIN),
            artifact_count: 0,
            batches: AdmissionCommitment::new(MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN),
            batch_count: 0,
        };
        if let Some(base) = &self.base {
            cut.plans.root.clone_from(&base.plan_admission_commitment);
            cut.plan_count = base.plan_count;
            cut.artifacts
                .root
                .clone_from(&base.artifact_admission_commitment);
            cut.artifact_count = base.artifact_count;
            cut.batches
                .root
                .clone_from(&base.batch_admission_commitment);
            cut.batch_count = base.batch_count;
        }
        let plan_count = usize::try_from(cut.plan_count)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let artifact_count = usize::try_from(cut.artifact_count)
            .map_err(|error| CoreError::Validation(error.to_string()))?;
        let mut plans = self
            .plan_order
            .get(..plan_count)
            .ok_or_else(|| CoreError::NotFound("Machine base Plan prefix is missing".to_owned()))?
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut artifacts = self
            .artifact_order
            .get(..artifact_count)
            .ok_or_else(|| {
                CoreError::NotFound("Machine base Artifact prefix is missing".to_owned())
            })?
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for batch in batches {
            if batch.admits_material() {
                for id in &batch.plan_ids {
                    if plans.insert(id.clone()) {
                        if self.plan_order.get(plans.len() - 1) != Some(id) {
                            return Err(CoreError::IdentityMismatch(
                                "cut Plan admission order differs from its batch".to_owned(),
                            ));
                        }
                        cut.plans.insert_with_undo(id)?;
                        cut.plan_count += 1;
                    }
                }
                for reference in &batch.artifacts {
                    let id = &reference.artifact_id;
                    if artifacts.insert(id.clone()) {
                        if self.artifact_order.get(artifacts.len() - 1) != Some(id) {
                            return Err(CoreError::IdentityMismatch(
                                "cut Artifact admission order differs from its batch".to_owned(),
                            ));
                        }
                        cut.artifacts.insert_with_undo(id)?;
                        cut.artifact_count += 1;
                    }
                }
            }
            cut.batches.insert_with_undo(&batch.batch_id)?;
            cut.batch_count += 1;
        }
        Ok(cut)
    }

    /// Compact a causally closed event prefix and retain a full suffix.
    ///
    /// # Errors
    ///
    /// Returns an error when the cut is empty or not causally and semantically
    /// closed, or its archive/base authority cannot be derived exactly.
    ///
    /// # Panics
    ///
    /// Panics only if the already-validated internal Event order loses an Event
    /// during this exclusive in-memory operation.
    pub fn compact_event_history(&mut self, retain_suffix: usize) -> Result<MachineCompaction> {
        let EventCompactionCut {
            event_ids: prefix_ids,
            projection,
            projection_root,
            admission_index: cut_admission_index,
        } = self.event_compaction_cut(retain_suffix)?;
        let cut = prefix_ids.len();
        let cut_admission = &self.admissions[cut_admission_index];
        let archive_inputs = self.event_compaction_entries(&prefix_ids, cut_admission_index)?;
        let PreparedCommandArchive {
            segment: archive_segment,
            parent_index_root: parent_command_index_root,
            result_index_root: result_command_index_root,
            index_nodes: command_index_nodes,
        } = self.prepare_compaction_archive(
            &self.admissions[..=cut_admission_index],
            archive_inputs,
            CommandArchiveCut::ThroughAdmission,
        )?;
        if archive_segment.header.result_count != cut_admission.sequence
            || archive_segment.header.result_admission_head.as_deref()
                != Some(cut_admission.admission_id.as_str())
            || archive_segment.header.result_command_index_root != result_command_index_root
        {
            return Err(CoreError::IdentityMismatch(
                "archive segment does not end at the Event compaction admission".to_owned(),
            ));
        }
        let base = self.event_compaction_base(projection, projection_root, &archive_segment)?;
        self.verify_retained_compaction_sources(&base, &archive_segment)?;
        let projection_digest = base.projection_digest.clone();
        let anchor = MachineBaseAnchor::from_verified_base(&base)?;
        let base_id = anchor.base_id.clone();
        for event_id in &prefix_ids {
            self.events.remove(event_id);
        }
        self.event_order.drain(..cut);
        for admission in self.admissions.drain(..=cut_admission_index) {
            self.commands.remove(&admission.command_id);
            self.command_index_proofs.remove(&admission.command_id);
        }
        let archived_batch_ids = archive_segment
            .batches
            .iter()
            .map(|batch| batch.batch_id.as_str())
            .collect::<BTreeSet<_>>();
        self.batch_order
            .retain(|batch_id| !archived_batch_ids.contains(batch_id.as_str()));
        for batch_id in archived_batch_ids {
            self.batches.remove(batch_id);
        }
        for proof in self.command_index_proofs.values_mut() {
            *proof = rebase_command_index_proof(
                proof,
                &parent_command_index_root,
                &result_command_index_root,
                &command_index_nodes,
            )?;
        }
        self.compacted_event_ids = base.event_ids();
        self.base_anchor = Some(anchor);
        self.reset_projection_root_to_base(&base)?;
        self.base = Some(Arc::new(base));
        let causal_frontier = self.compaction_frontier();
        Ok(MachineCompaction {
            base_id,
            compacted_events: archive_segment.header.result_event_count,
            retained_events: u64::try_from(self.event_order.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            causal_frontier,
            projection_digest,
            archive_segment,
        })
    }

    fn event_compaction_cut(&self, retain_suffix: usize) -> Result<EventCompactionCut> {
        if retain_suffix >= self.event_order.len() {
            return Err(CoreError::Validation(
                "event compaction must remove at least one retained Event".to_owned(),
            ));
        }
        let cut = self.event_order.len() - retain_suffix;
        let prefix_ids = self.event_order[..cut].to_vec();
        let prefix: Vec<Event> = prefix_ids
            .iter()
            .map(|event_id| {
                self.events
                    .get(event_id)
                    .expect("event order references existing Event")
                    .clone()
            })
            .collect();
        let projection = self
            .base
            .as_ref()
            .map(|base| base.projection.clone())
            .unwrap_or_default();
        let mut authority = self.lightweight_event_authority(projection);
        for event in &prefix {
            authority.validate_event_authority(event)?;
            authority.projection.apply_event(event)?;
            authority.authority.projection_root = authority.projection_root_after(event)?;
        }
        let projection_root = authority.authority.projection_root.clone();
        let projection = authority.projection;
        let last_compacted_event = prefix.last().ok_or_else(|| {
            CoreError::Validation("event compaction produced an empty prefix".to_owned())
        })?;
        let cut_admission_index = self
            .admissions
            .iter()
            .position(|admission| admission.command_id == last_compacted_event.command_id)
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "compacted Event {} has no CommandAdmission",
                    last_compacted_event.event_id
                ))
            })?;
        Ok(EventCompactionCut {
            event_ids: prefix_ids,
            projection,
            projection_root,
            admission_index: cut_admission_index,
        })
    }

    fn event_compaction_entries(
        &self,
        prefix_ids: &[String],
        cut_admission_index: usize,
    ) -> Result<ArchiveEntryInputs> {
        let prefix_event_ids = prefix_ids.iter().cloned().collect::<BTreeSet<_>>();
        let mut archive_entries = Vec::with_capacity(cut_admission_index + 1);
        let mut base_index_proofs = Vec::with_capacity(cut_admission_index + 1);
        let mut archived_event_ids = BTreeSet::new();
        for admission in &self.admissions[..=cut_admission_index] {
            let record = self.commands.get(&admission.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "CommandAdmission {} has no command record",
                    admission.admission_id
                ))
            })?;
            verify_admission_record(admission, record)?;
            let events = admission
                .event_ids
                .iter()
                .map(|event_id| {
                    let event = self.events.get(event_id).cloned().ok_or_else(|| {
                        CoreError::NotFound(format!("archived Event {event_id} is missing"))
                    })?;
                    if !prefix_event_ids.contains(event_id) {
                        return Err(CoreError::Causal(format!(
                            "archived admission {} crosses the Event compaction cut",
                            admission.admission_id
                        )));
                    }
                    archived_event_ids.insert(event_id.clone());
                    Ok(event)
                })
                .collect::<Result<Vec<_>>>()?;
            archive_entries.push(MachineCommandArchiveEntry {
                admission: admission.clone(),
                command: ArchivedCommandRecord::from_private(record),
                events,
            });
            base_index_proofs.push(
                self.command_index_proofs
                    .get(&admission.command_id)
                    .cloned()
                    .ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "archived command {} has no index non-membership proof",
                            admission.command_id
                        ))
                    })?,
            );
        }
        if archived_event_ids != prefix_event_ids {
            return Err(CoreError::NotFound(
                "compacted Event prefix does not equal archived applied admissions".to_owned(),
            ));
        }
        Ok(ArchiveEntryInputs {
            entries: archive_entries,
            index_proofs: base_index_proofs,
        })
    }

    fn prepare_compaction_archive(
        &self,
        admissions: &[CommandAdmission],
        inputs: ArchiveEntryInputs,
        cut: CommandArchiveCut,
    ) -> Result<PreparedCommandArchive> {
        let ArchiveEntryInputs {
            entries: archive_entries,
            index_proofs: base_index_proofs,
        } = inputs;
        let parent_segment = self.base.as_ref().map(|base| base.archive_head.clone());
        let parent_count = self.base.as_ref().map_or(0, |base| base.archive_count);
        let parent_event_count = self
            .base
            .as_ref()
            .map_or(0, |base| base.archive_event_count);
        let parent_admission_head = self
            .base
            .as_ref()
            .and_then(|base| base.admission_head.clone());
        let archive_batches =
            archive_batch_records(&self.batches, &self.batch_order, admissions, cut)?;
        let parent_command_index_root = current_command_index_root(self.base.as_deref())?;
        let (command_index_updates, command_index_nodes, result_command_index_root) =
            sequential_command_index_updates(
                &parent_command_index_root,
                &archive_entries,
                &base_index_proofs,
            )?;
        let archive_segment = MachineCommandArchiveSegment::new(
            MachineCommandArchiveParent {
                segment: parent_segment,
                count: parent_count,
                event_count: parent_event_count,
                admission_head: parent_admission_head,
                command_index_root: parent_command_index_root.clone(),
            },
            archive_batches,
            archive_entries,
            command_index_updates,
        )?;
        Ok(PreparedCommandArchive {
            segment: archive_segment,
            parent_index_root: parent_command_index_root,
            result_index_root: result_command_index_root,
            index_nodes: command_index_nodes,
        })
    }

    fn verify_retained_compaction_sources(
        &self,
        base: &MachineBaseSnapshot,
        segment: &MachineCommandArchiveSegment,
    ) -> Result<()> {
        let retained = self
            .batch_order
            .iter()
            .skip(segment.batches.len())
            .map(|id| {
                self.batches.get(id).ok_or_else(|| {
                    CoreError::NotFound(format!("retained batch {id} is unavailable"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        verify_compaction_source_cut(base, segment, retained)
    }

    fn event_compaction_base(
        &self,
        projection: Projection,
        projection_root: String,
        archive_segment: &MachineCommandArchiveSegment,
    ) -> Result<MachineBaseSnapshot> {
        let projection_digest = projection.digest()?;
        let prefix_digest = machine_prefix_digest(
            &archive_segment.header.segment_id,
            archive_segment.header.result_count,
            archive_segment.header.result_event_count,
            archive_segment.header.result_admission_head.as_deref(),
            &archive_segment.header.result_command_index_root,
            &projection_digest,
            &projection_root,
        )?;
        let cut_frontier = self.admission_frontier_at_cut(&archive_segment.batches)?;
        let base = MachineBaseSnapshot {
            prefix_digest,
            archive_head: archive_segment.header.segment_id.clone(),
            archive_count: archive_segment.header.result_count,
            archive_event_count: archive_segment.header.result_event_count,
            admission_head: archive_segment.header.result_admission_head.clone(),
            command_index_root: archive_segment.header.result_command_index_root.clone(),
            plan_admission_commitment: cut_frontier.plans.root,
            plan_count: cut_frontier.plan_count,
            artifact_admission_commitment: cut_frontier.artifacts.root,
            artifact_count: cut_frontier.artifact_count,
            batch_admission_commitment: cut_frontier.batches.root,
            batch_count: cut_frontier.batch_count,
            projection,
            projection_digest: projection_digest.clone(),
            projection_root,
        };
        base.verify()?;
        self.validate_projection_authority(&base.projection)?;
        Ok(base)
    }

    fn event_free_compaction_entries(&self) -> Result<ArchiveEntryInputs> {
        let mut archive_entries = Vec::with_capacity(self.admissions.len());
        let mut base_index_proofs = Vec::with_capacity(self.admissions.len());
        for admission in &self.admissions {
            let record = self.commands.get(&admission.command_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "CommandAdmission {} has no command record",
                    admission.admission_id
                ))
            })?;
            verify_admission_record(admission, record)?;
            archive_entries.push(MachineCommandArchiveEntry {
                admission: admission.clone(),
                command: ArchivedCommandRecord::from_private(record),
                events: Vec::new(),
            });
            base_index_proofs.push(
                self.command_index_proofs
                    .get(&admission.command_id)
                    .cloned()
                    .ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "archived command {} has no index non-membership proof",
                            admission.command_id
                        ))
                    })?,
            );
        }
        Ok(ArchiveEntryInputs {
            entries: archive_entries,
            index_proofs: base_index_proofs,
        })
    }

    /// Archive a non-empty Event-free hot tail of conflicts and material batches.
    ///
    /// Conflict receipts participate in the same ordered admission chain as
    /// applied commands even though they produce no Event. This rotation keeps
    /// that chain bounded without changing the Projection or Event frontier.
    /// Material-only batches use this same archive even before the first command.
    ///
    /// # Errors
    ///
    /// Returns an error unless the entire hot tail is a non-empty contiguous
    /// conflict/material-only batch sequence with exact archive authority.
    pub fn compact_event_free_admissions(&mut self) -> Result<MachineCompaction> {
        if self.admissions.is_empty() && self.batches.is_empty() {
            return Err(CoreError::Validation(
                "Event-free compaction requires at least one hot admission or material batch"
                    .to_owned(),
            ));
        }
        if !self.event_order.is_empty()
            || self.admissions.iter().any(|admission| {
                admission.status != CommandReceiptStatus::Conflict
                    || !admission.event_ids.is_empty()
            })
        {
            return Err(CoreError::Validation(
                "Event-free compaction requires a conflict/material-only hot admission tail"
                    .to_owned(),
            ));
        }

        let archive_inputs = self.event_free_compaction_entries()?;

        let PreparedCommandArchive {
            segment: archive_segment,
            result_index_root: result_command_index_root,
            ..
        } = self.prepare_compaction_archive(
            &self.admissions,
            archive_inputs,
            CommandArchiveCut::CompleteEventFreeTail,
        )?;
        if archive_segment.header.event_count != 0 {
            return Err(CoreError::IdentityMismatch(
                "Event-free archive segment unexpectedly contains an Event".to_owned(),
            ));
        }

        self.validate_projection_authority(&self.projection)?;
        let projection_digest = self.projection.digest()?;
        let projection_root = self.authority.projection_root.clone();
        let prefix_digest = machine_prefix_digest(
            &archive_segment.header.segment_id,
            archive_segment.header.result_count,
            archive_segment.header.result_event_count,
            archive_segment.header.result_admission_head.as_deref(),
            &archive_segment.header.result_command_index_root,
            &projection_digest,
            &projection_root,
        )?;
        let base = MachineBaseSnapshot {
            prefix_digest,
            archive_head: archive_segment.header.segment_id.clone(),
            archive_count: archive_segment.header.result_count,
            archive_event_count: archive_segment.header.result_event_count,
            admission_head: archive_segment.header.result_admission_head.clone(),
            command_index_root: result_command_index_root,
            plan_admission_commitment: self.authority.plans.root().to_owned(),
            plan_count: u64::try_from(self.plans.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            artifact_admission_commitment: self.authority.artifacts.root().to_owned(),
            artifact_count: u64::try_from(self.artifacts.len())
                .map_err(|error| CoreError::Validation(error.to_string()))?,
            batch_admission_commitment: self.authority.batches.root().to_owned(),
            batch_count: self.authority.batch_count,
            projection: self.projection.clone(),
            projection_digest: projection_digest.clone(),
            projection_root,
        };
        base.verify()?;
        self.verify_retained_compaction_sources(&base, &archive_segment)?;
        let anchor = MachineBaseAnchor::from_verified_base(&base)?;
        let base_id = anchor.base_id.clone();
        for admission in self.admissions.drain(..) {
            self.commands.remove(&admission.command_id);
            self.command_index_proofs.remove(&admission.command_id);
        }
        let archived_batch_ids = archive_segment
            .batches
            .iter()
            .map(|batch| batch.batch_id.as_str())
            .collect::<BTreeSet<_>>();
        self.batch_order
            .retain(|batch_id| !archived_batch_ids.contains(batch_id.as_str()));
        for batch_id in archived_batch_ids {
            self.batches.remove(batch_id);
        }
        self.compacted_event_ids = base.event_ids();
        self.base_anchor = Some(anchor);
        self.reset_projection_root_to_base(&base)?;
        self.base = Some(Arc::new(base));

        Ok(MachineCompaction {
            base_id,
            compacted_events: archive_segment.header.result_event_count,
            retained_events: 0,
            causal_frontier: self.compaction_frontier(),
            projection_digest,
            archive_segment,
        })
    }

    /// Insert and verify an already sealed plan.
    ///
    /// # Errors
    ///
    /// Returns an error when the Plan is invalid or its identity conflicts with
    /// previously retained immutable content.
    pub fn insert_plan(&mut self, plan: SealedPlan) -> Result<()> {
        plan.verify()?;
        if let Some(existing) = self.plans.get(&plan.plan_id) {
            if existing != &plan {
                return Err(CoreError::IdentityMismatch(format!(
                    "plan {} already exists with different content",
                    plan.plan_id
                )));
            }
            return Ok(());
        }
        match self.staged_plans.entry(plan.plan_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(plan);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &plan => {}
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "staged plan {} already exists with different content",
                    entry.key()
                )));
            }
        }
        Ok(())
    }

    /// Read a sealed plan.
    pub fn plan(&self, plan_id: &str) -> Option<&SealedPlan> {
        self.plans
            .get(plan_id)
            .or_else(|| self.staged_plans.get(plan_id))
    }

    fn retain_plan(&mut self, plan: SealedPlan) -> Result<Option<AdmissionCommitmentUndo>> {
        plan.verify()?;
        if let Some(existing) = self.plans.get(&plan.plan_id) {
            if existing != &plan {
                return Err(CoreError::IdentityMismatch(format!(
                    "plan {} already exists with different content",
                    plan.plan_id
                )));
            }
            return Ok(None);
        }
        let node_undo = self.authority.plans.insert_with_undo(&plan.plan_id)?;
        self.plan_order.push(plan.plan_id.clone());
        self.plans.insert(plan.plan_id.clone(), plan);
        Ok(Some(node_undo))
    }

    fn insert_batch(&mut self, batch: MachineCommandBatchRecord) -> Result<()> {
        batch.verify()?;
        if let Some(existing) = self.batches.get(&batch.batch_id) {
            if existing != &batch {
                return Err(CoreError::IdentityMismatch(format!(
                    "command batch {} already exists with different content",
                    batch.batch_id
                )));
            }
            return Ok(());
        }
        self.authority.batches.insert_with_undo(&batch.batch_id)?;
        self.batch_order.push(batch.batch_id.clone());
        self.batches.insert(batch.batch_id.clone(), batch);
        self.authority.batch_count = self
            .authority
            .batch_count
            .checked_add(1)
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("Machine batch count overflowed".to_owned()))?;
        Ok(())
    }

    /// Store immutable typed bytes and return their content reference.
    ///
    /// # Errors
    ///
    /// Returns an error when Artifact identity derivation or append-only
    /// authority maintenance fails.
    pub fn put_artifact(&mut self, kind: impl Into<String>, bytes: Vec<u8>) -> Result<ArtifactRef> {
        let reference = artifact_ref(kind, &bytes)?;
        let artifact = ArtifactRecord {
            reference: reference.clone(),
            bytes,
        };
        if let Some(existing) = self.artifacts.get(&reference.artifact_id) {
            if existing != &artifact {
                return Err(CoreError::IdentityMismatch(format!(
                    "Artifact {} already exists with different content",
                    reference.artifact_id
                )));
            }
            return Ok(reference);
        }
        match self.staged_artifacts.entry(reference.artifact_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(artifact);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &artifact => {}
            std::collections::btree_map::Entry::Occupied(entry) => {
                return Err(CoreError::IdentityMismatch(format!(
                    "staged Artifact {} already exists with different content",
                    entry.key()
                )));
            }
        }
        Ok(reference)
    }

    fn retain_artifact(
        &mut self,
        artifact: ArtifactRecord,
    ) -> Result<Option<AdmissionCommitmentUndo>> {
        artifact.validate()?;
        let artifact_id = artifact.reference.artifact_id.clone();
        if let Some(existing) = self.artifacts.get(&artifact_id) {
            if existing != &artifact {
                return Err(CoreError::IdentityMismatch(format!(
                    "Artifact {artifact_id} already exists with different content"
                )));
            }
            return Ok(None);
        }
        let node_undo = self.authority.artifacts.insert_with_undo(&artifact_id)?;
        self.artifact_order.push(artifact_id.clone());
        self.artifacts.insert(artifact_id, artifact);
        Ok(Some(node_undo))
    }

    /// Read retained or locally staged immutable Artifact bytes.
    /// Staged bytes do not enter a snapshot or semantic authority until a
    /// command admits their exact reference.
    pub fn artifact(&self, reference: &ArtifactRef) -> Option<&ArtifactRecord> {
        self.artifacts
            .get(&reference.artifact_id)
            .or_else(|| self.staged_artifacts.get(&reference.artifact_id))
            .filter(|record| record.reference == *reference)
    }

    fn admit_staged_command_material(
        &mut self,
        command: &Command,
    ) -> Result<StagedMaterialAdmissionUndo> {
        let (plan_ids, artifacts) = command_material_membership(command)?;
        let new_plan_ids = plan_ids
            .iter()
            .filter(|plan_id| !self.plans.contains_key(*plan_id))
            .cloned()
            .collect::<BTreeSet<_>>();
        let new_artifact_ids = artifacts
            .iter()
            .filter(|reference| !self.artifacts.contains_key(&reference.artifact_id))
            .map(|reference| reference.artifact_id.clone())
            .collect::<BTreeSet<_>>();
        if new_plan_ids
            .iter()
            .any(|id| !self.staged_plans.contains_key(id))
            || new_artifact_ids
                .iter()
                .any(|id| !self.staged_artifacts.contains_key(id))
        {
            return Err(CoreError::Validation(
                "staged Machine material is missing a command member".to_owned(),
            ));
        }
        let plans = plan_ids
            .iter()
            .filter_map(|plan_id| self.staged_plans.get(plan_id).cloned())
            .collect::<Vec<_>>();
        let staged_artifacts = artifacts
            .iter()
            .filter_map(|reference| {
                self.staged_artifacts
                    .get(&reference.artifact_id)
                    .map(|artifact| (reference, artifact.clone()))
            })
            .map(|(reference, artifact)| {
                if artifact.reference == *reference {
                    Ok(artifact)
                } else {
                    Err(CoreError::IdentityMismatch(format!(
                        "staged Artifact {} changed its typed reference",
                        reference.artifact_id
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let plan_start = self.plan_order.len();
        let artifact_start = self.artifact_order.len();
        let mut plan_node_undos = Vec::with_capacity(plans.len());
        let mut artifact_node_undos = Vec::with_capacity(staged_artifacts.len());
        for plan in plans.iter().cloned() {
            if let Some(undo) = self.retain_plan(plan)? {
                plan_node_undos.push(undo);
            }
        }
        for artifact in staged_artifacts.iter().cloned() {
            if let Some(undo) = self.retain_artifact(artifact)? {
                artifact_node_undos.push(undo);
            }
        }
        for plan in &plans {
            self.staged_plans.remove(&plan.plan_id);
        }
        for artifact in &staged_artifacts {
            self.staged_artifacts
                .remove(&artifact.reference.artifact_id);
        }
        Ok(StagedMaterialAdmissionUndo {
            plan_start,
            artifact_start,
            plan_node_undos,
            artifact_node_undos,
            plans,
            artifacts: staged_artifacts,
        })
    }

    /// Classify replay capability for a required artifact set.
    pub fn replay_availability(&self, required: &[ArtifactRef]) -> ReplayAvailability {
        let missing: Vec<String> = required
            .iter()
            .filter(|reference| {
                self.artifacts
                    .get(&reference.artifact_id)
                    .is_none_or(|record| record.reference != **reference)
            })
            .map(|reference| reference.artifact_id.clone())
            .collect();
        if missing.is_empty() {
            ReplayAvailability::Exact
        } else if self.events.is_empty() && self.base.is_none() {
            ReplayAvailability::Unavailable {
                reason: "canonical event history is unavailable".to_owned(),
            }
        } else {
            ReplayAvailability::ProjectionOnly { missing }
        }
    }

    /// Admit an idempotent command and reduce its canonical event.
    ///
    /// # Errors
    ///
    /// Returns an error when the command is invalid, reuses an identity with
    /// different semantics, requires an archive lookup, or reduction fails.
    pub fn submit(&mut self, envelope: CommandEnvelope) -> Result<CommandReceipt> {
        validate_envelope(&envelope)?;
        let semantic_hash = canonical_digest(&envelope)?;
        if let Some(record) = self.commands.get(&envelope.command_id) {
            self.verify_retained_command_record(&envelope.command_id, record)?;
            if record.semantic_hash == semantic_hash {
                return Ok(record.receipt.clone());
            }
            return Err(CoreError::CommandReuse(format!(
                "command ID {} was already used with different semantics",
                envelope.command_id
            )));
        }
        if let Some(base) = &self.base {
            return Err(CoreError::ArchivedCommandReplayRequired {
                command_id: envelope.command_id,
                archive_head: base.archive_head.clone(),
                command_index_root: base.command_index_root.clone(),
            });
        }
        let proof = MachineCommandIndexProof::empty_nonmembership(envelope.command_id.clone())?;
        self.submit_new_with_index_proof(envelope, proof)
    }

    /// Resolve an unknown command ID against the current archived-command map.
    ///
    /// Membership returns the original receipt or a command-reuse error without
    /// mutating hot state. Non-membership authorizes exactly one new hot
    /// admission and retains the proof for anchored restore and later compaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the command or lookup proof is invalid, exact
    /// archived semantics differ, or fresh admission and reduction fail.
    pub fn submit_with_archive_lookup(
        &mut self,
        envelope: CommandEnvelope,
        lookup: MachineCommandArchiveLookup,
    ) -> Result<CommandReceipt> {
        validate_envelope(&envelope)?;
        let semantic_hash = canonical_digest(&envelope)?;
        if let Some(record) = self.commands.get(&envelope.command_id) {
            self.verify_retained_command_record(&envelope.command_id, record)?;
            if record.semantic_hash == semantic_hash {
                return Ok(record.receipt.clone());
            }
            return Err(CoreError::CommandReuse(format!(
                "command ID {} was already used with different semantics",
                envelope.command_id
            )));
        }
        let root = current_command_index_root(self.base.as_deref())?;
        match lookup {
            MachineCommandArchiveLookup::NonMember { index_proof } => {
                if index_proof.command_id != envelope.command_id || index_proof.value.is_some() {
                    return Err(CoreError::IdentityMismatch(
                        "command index non-membership proof does not match the submitted command"
                            .to_owned(),
                    ));
                }
                index_proof.verify(&root)?;
                self.submit_new_with_index_proof(envelope, index_proof)
            }
            MachineCommandArchiveLookup::Member { index_proof, entry } => {
                if index_proof.command_id != envelope.command_id {
                    return Err(CoreError::IdentityMismatch(
                        "command index membership proof does not match the submitted command"
                            .to_owned(),
                    ));
                }
                index_proof.verify(&root)?;
                entry.verify_shape()?;
                let value = index_proof.value.as_ref().ok_or_else(|| {
                    CoreError::Validation(
                        "archived command membership lookup carried non-membership".to_owned(),
                    )
                })?;
                if entry.admission.command_id != envelope.command_id
                    || value.admission_id != entry.admission.admission_id
                    || value.archive_entry_digest != entry.leaf_digest()?
                {
                    return Err(CoreError::IdentityMismatch(
                        "archived command membership proof does not bind its complete entry"
                            .to_owned(),
                    ));
                }
                if entry.command.semantic_hash != semantic_hash
                    || entry.command.envelope != envelope
                {
                    return Err(CoreError::CommandReuse(format!(
                        "command ID {} was already used with different semantics",
                        index_proof.command_id
                    )));
                }
                Ok(entry.command.receipt)
            }
        }
    }

    fn command_event_batch(
        &self,
        envelope: &CommandEnvelope,
        semantic_hash: &str,
    ) -> Result<Vec<Event>> {
        let first_payload = self.admit_command(envelope)?;
        let mut payloads = vec![first_payload];
        if let Command::StartRun {
            initial_attempt, ..
        } = &envelope.command
        {
            payloads.push(EventPayload::AttemptStarted {
                attempt_id: initial_attempt.attempt_id.clone(),
                continuation_id: initial_attempt.continuation_id.clone(),
                occurrence_binding: initial_attempt.occurrence_binding.clone(),
                continuation_epoch: initial_attempt.continuation_epoch,
                execution_fence: initial_attempt.execution_fence,
            });
        }
        let mut parents = self
            .projection
            .runs
            .get(&envelope.run_id)
            .map_or_else(Vec::new, |run| vec![run.last_event.clone()]);
        let mut events = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let (reads, writes, coordination_key) = footprints(&envelope.run_id, &payload);
            let event = Event::new(EventContent {
                command_id: envelope.command_id.clone(),
                command_hash: semantic_hash.to_owned(),
                run_id: envelope.run_id.clone(),
                parents,
                reads,
                writes,
                coordination_key,
                payload,
            })?;
            parents = vec![event.event_id.clone()];
            events.push(event);
        }
        Ok(events)
    }

    fn apply_command_event_batch(
        &mut self,
        events: &[Event],
    ) -> Result<(Vec<ProjectionEntryUndo>, String)> {
        let mut undos = Vec::with_capacity(events.len());
        let mut pending = BTreeSet::new();
        let mut projection_root = self.authority.projection_root.clone();
        for event in events {
            if let Err(error) = self.validate_new_event_with_pending(event, &pending) {
                rollback_projection_batch(self, undos);
                return Err(error);
            }
            let undo = ProjectionEntryUndo::capture(self, event);
            if let Err(error) = self.projection.apply_event(event) {
                undo.rollback(self);
                rollback_projection_batch(self, undos);
                return Err(error);
            }
            if let Err(error) = verify_event_footprint(event) {
                undo.rollback(self);
                rollback_projection_batch(self, undos);
                return Err(error);
            }
            projection_root = canonical_digest(&(
                PROJECTION_ROOT_EVENT_DOMAIN,
                projection_root.as_str(),
                event.event_id.as_str(),
            ))?;
            pending.insert(event.event_id.clone());
            undos.push(undo);
        }
        Ok((undos, projection_root))
    }

    fn finalize_single_batch_authority(
        &mut self,
        parent_authority_root: &str,
        record: &CommandRecord,
        admission: CommandAdmission,
    ) -> Result<()> {
        self.admissions.push(admission);
        let next_batch_root = content_id(
            MACHINE_COMMAND_BATCH_ADMISSION_COMMITMENT_DOMAIN,
            &(
                "append",
                self.authority.batches.root(),
                record.batch_id.as_str(),
            ),
        )?;
        let next_batch_count = self
            .authority
            .batch_count
            .checked_add(1)
            .filter(|count| *count <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| CoreError::Validation("Machine batch count overflowed".to_owned()))?;
        let result_authority_root =
            self.authority_root_with_batch(&next_batch_root, next_batch_count)?;
        let batch = build_single_command_batch_record(
            parent_authority_root,
            &result_authority_root,
            record,
        )?;
        if let Err(error) = self.insert_batch(batch) {
            self.admissions.pop();
            return Err(error);
        }
        if self.authority_root()? != result_authority_root {
            return Err(CoreError::IdentityMismatch(
                "single command batch did not produce its declared authority root".to_owned(),
            ));
        }
        Ok(())
    }

    fn submit_new_with_index_proof(
        &mut self,
        envelope: CommandEnvelope,
        index_proof: MachineCommandIndexProof,
    ) -> Result<CommandReceipt> {
        if self.commands.contains_key(&envelope.command_id)
            || self.command_index_proofs.contains_key(&envelope.command_id)
        {
            return Err(CoreError::IdentityMismatch(
                "new command admission repeated a hot command identity".to_owned(),
            ));
        }
        if index_proof.command_id != envelope.command_id || index_proof.value.is_some() {
            return Err(CoreError::IdentityMismatch(
                "new command admission has the wrong non-membership proof".to_owned(),
            ));
        }
        let semantic_hash = canonical_digest(&envelope)?;
        let parent_authority_root = self.authority_root()?;
        let (batch_id, batch_position, batch_len) =
            single_command_batch_metadata(&parent_authority_root, &envelope, &semantic_hash)?;
        if let Some(receipt) = self.stale_command_receipt(&envelope)? {
            let record = CommandRecord {
                envelope,
                semantic_hash,
                receipt: receipt.clone(),
                batch_id,
                batch_position,
                batch_len,
            };
            let admission = CommandAdmission::new(
                self.admission_parent(),
                &record,
                self.authority.projection_root.clone(),
                self.authority.projection_root.clone(),
            )?;
            self.finalize_single_batch_authority(&parent_authority_root, &record, admission)?;
            self.commands
                .insert(record.envelope.command_id.clone(), record);
            self.command_index_proofs
                .insert(receipt.command_id.clone(), index_proof);
            return Ok(receipt);
        }
        let material_undo = self.admit_staged_command_material(&envelope.command)?;
        let result = self.submit_applied_single_command(
            envelope,
            semantic_hash,
            batch_id,
            &parent_authority_root,
            index_proof,
        );
        if result.is_err() {
            material_undo.rollback(self);
        }
        result
    }

    fn stale_command_receipt(&self, envelope: &CommandEnvelope) -> Result<Option<CommandReceipt>> {
        if matches!(envelope.command, Command::StartRun { .. }) {
            return Ok(None);
        }
        let observed = envelope.expected_precondition.clone().ok_or_else(|| {
            CoreError::Validation("mutating commands require expected_precondition".to_owned())
        })?;
        let current_precondition = self
            .projection
            .runs
            .get(&envelope.run_id)
            .map(crate::RunProjection::precondition_token);
        if Some(&observed) == current_precondition.as_ref() {
            return Ok(None);
        }
        Ok(Some(CommandReceipt {
            command_id: envelope.command_id.clone(),
            status: CommandReceiptStatus::Conflict,
            event_ids: Vec::new(),
            error_code: Some("stale_action".to_owned()),
            message: Some("the Run changed after the caller's view".to_owned()),
            observed_precondition: Some(observed),
            current_precondition,
        }))
    }

    fn applied_command_receipt(
        &self,
        envelope: &CommandEnvelope,
        events: &[Event],
    ) -> CommandReceipt {
        CommandReceipt {
            command_id: envelope.command_id.clone(),
            status: CommandReceiptStatus::Applied,
            event_ids: events.iter().map(|event| event.event_id.clone()).collect(),
            error_code: None,
            message: None,
            observed_precondition: envelope.expected_precondition.clone(),
            current_precondition: self
                .projection
                .runs
                .get(&envelope.run_id)
                .map(crate::RunProjection::precondition_token),
        }
    }

    fn submit_applied_single_command(
        &mut self,
        envelope: CommandEnvelope,
        semantic_hash: String,
        batch_id: String,
        parent_authority_root: &str,
        index_proof: MachineCommandIndexProof,
    ) -> Result<CommandReceipt> {
        let before_projection_digest = self.authority.projection_root.clone();
        let events = self.command_event_batch(&envelope, &semantic_hash)?;
        let (projection_undos, after_projection_digest) =
            self.apply_command_event_batch(&events)?;
        let receipt = self.applied_command_receipt(&envelope, &events);
        let record = CommandRecord {
            envelope,
            semantic_hash,
            receipt: receipt.clone(),
            batch_id,
            batch_position: 0,
            batch_len: 1,
        };
        let admission = match CommandAdmission::new(
            self.admission_parent(),
            &record,
            before_projection_digest.clone(),
            after_projection_digest.clone(),
        ) {
            Ok(admission) => admission,
            Err(error) => {
                rollback_projection_batch(self, projection_undos);
                return Err(error);
            }
        };
        self.authority.projection_root = after_projection_digest;
        for event in &events {
            self.append_event(event.clone());
        }
        if let Err(error) =
            self.finalize_single_batch_authority(parent_authority_root, &record, admission)
        {
            for event_id in &receipt.event_ids {
                self.events.remove(event_id);
            }
            self.event_order
                .retain(|event_id| !receipt.event_ids.contains(event_id));
            self.authority
                .projection_root
                .clone_from(&before_projection_digest);
            rollback_projection_batch(self, projection_undos);
            return Err(error);
        }
        #[cfg(test)]
        PROJECTION_ROOT_ADVANCE_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        self.commands
            .insert(record.envelope.command_id.clone(), record);
        self.command_index_proofs
            .insert(receipt.command_id.clone(), index_proof);
        Ok(receipt)
    }

    fn verify_retained_command_record(
        &self,
        command_id: &str,
        record: &CommandRecord,
    ) -> Result<()> {
        let proof = self.command_index_proofs.get(command_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "command {command_id} has no archive non-membership authority"
            ))
        })?;
        if proof.command_id != command_id || proof.value.is_some() {
            return Err(CoreError::IdentityMismatch(format!(
                "command {command_id} has malformed archive non-membership authority"
            )));
        }
        proof.verify(&current_command_index_root(self.base.as_deref())?)?;
        let index = self
            .admissions
            .iter()
            .position(|admission| admission.command_id == command_id)
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "command {command_id} has no CommandAdmission authority"
                ))
            })?;
        let admission = &self.admissions[index];
        let parent = index.checked_sub(1).map_or_else(
            || command_admission_parent(&[], self.base.as_deref()),
            |parent| Some((&self.admissions[parent]).into()),
        );
        admission.verify(parent)?;
        verify_admission_record(admission, record)
    }

    /// Append a crate-validated event and its already reduced projection.
    ///
    /// Public mutation must enter through [`Self::submit`]; this seam exists
    /// only for crate-owned typed command assembly after validation.
    pub(crate) fn append_event(&mut self, event: Event) {
        self.event_order.push(event.event_id.clone());
        self.events.insert(event.event_id.clone(), event);
    }

    fn validate_new_event_with_pending(
        &self,
        event: &Event,
        pending: &BTreeSet<String>,
    ) -> Result<()> {
        event.verify()?;
        if self.compacted_event_ids.contains(&event.event_id) {
            return Err(CoreError::IdentityMismatch(format!(
                "event {} belongs to the compacted prefix",
                event.event_id
            )));
        }
        if self.events.contains_key(&event.event_id) {
            return Err(CoreError::IdentityMismatch(format!(
                "event {} already exists",
                event.event_id
            )));
        }
        for parent in &event.parents {
            if !self.events.contains_key(parent)
                && !self.compacted_event_ids.contains(parent)
                && !pending.contains(parent)
            {
                return Err(CoreError::Causal(format!(
                    "event {} references missing parent {parent}",
                    event.event_id
                )));
            }
        }
        match &event.payload {
            EventPayload::RunStarted { .. } if !event.parents.is_empty() => {
                return Err(CoreError::Causal(format!(
                    "Run start event {} must not have a causal parent",
                    event.event_id
                )));
            }
            EventPayload::RunStarted { .. } => {}
            _ => {
                let run = self.projection.runs.get(&event.run_id).ok_or_else(|| {
                    CoreError::NotFound(format!("Run {} does not exist", event.run_id))
                })?;
                if !event.parents.contains(&run.last_event) {
                    return Err(CoreError::Causal(format!(
                        "event {} does not extend Run {} causal frontier {}",
                        event.event_id, event.run_id, run.last_event
                    )));
                }
            }
        }
        self.validate_event_authority(event)
    }

    /// Current rebuildable projection.
    pub const fn projection(&self) -> &Projection {
        &self.projection
    }

    /// Re-resolve one historical interpreter location from an immutable Plan.
    ///
    /// This structural inspection permits terminal Runs and closed scopes. It
    /// does not authorize resume or provider execution.
    ///
    /// # Errors
    ///
    /// Returns an error when retained Plan/Run/scope authority is missing or the
    /// frame does not resolve to its exact entry-rooted lexical location.
    pub fn validate_historical_execution_location(
        &self,
        location: &ExecutionFrameLocation<'_>,
    ) -> Result<String> {
        let ExecutionFrameLocation {
            run_id,
            plan_id,
            invocation_id,
            invocation_path,
            definition_id,
            region_path,
            scope_id,
            next_step,
        } = *location;
        let run = self.run(run_id)?;
        if !run.plan_lineage.iter().any(|retained| retained == plan_id) {
            return Err(CoreError::Validation(format!(
                "persisted frame Plan {plan_id} is outside Run {run_id} migration lineage"
            )));
        }
        let plan = self
            .plans
            .get(plan_id)
            .ok_or_else(|| CoreError::NotFound(format!("plan {plan_id} does not exist")))?;
        let (resolved_definition, expected_invocation) =
            resolve_invocation(&plan.candidate, plan_id, run, invocation_path, false)?;
        if resolved_definition != definition_id || expected_invocation != invocation_id {
            return Err(CoreError::Validation(
                "persisted frame does not match its entry-rooted invocation path".to_owned(),
            ));
        }
        let scope = run
            .scopes
            .get(scope_id)
            .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
        validate_execution_scope_fields(
            scope,
            invocation_id,
            invocation_path,
            definition_id,
            region_path,
            scope_id,
            false,
        )?;
        let region = region_at_path(&plan.candidate, definition_id, region_path)?;
        if next_step > region.steps.len() {
            return Err(CoreError::Validation(format!(
                "execution frame next step {next_step} exceeds Region length {}",
                region.steps.len()
            )));
        }
        Ok(scope_id.to_owned())
    }

    /// Admit one frame for resume under exact current Run authority.
    ///
    /// # Errors
    ///
    /// Returns an error unless the frame matches the active Run's exact current
    /// Plan, binding, epoch, open scope, and structural location.
    pub fn validate_resumable_execution_frame(
        &self,
        frame: &ResumableExecutionFrame<'_>,
    ) -> Result<String> {
        let run = self.run(frame.location.run_id)?;
        let scope = run.scopes.get(frame.location.scope_id).ok_or_else(|| {
            CoreError::NotFound(format!("scope {} does not exist", frame.location.scope_id))
        })?;
        if run.execution_status != crate::RunExecutionStatus::Active
            || scope.status != crate::ScopeStatus::Open
            || frame.location.plan_id != run.current_plan
            || frame.binding_context != run.current_binding_context
            || frame.epoch != run.epoch
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} frame is not resumable under current Plan, binding, epoch, and open scope",
                frame.location.run_id
            )));
        }
        self.validate_historical_execution_location(&frame.location)
    }

    /// Admit a current frame parked at one exact Effect settlement boundary.
    /// This validates current Plan/binding/epoch authority without pretending a
    /// committed scope is open for interpreter execution.
    ///
    /// # Errors
    ///
    /// Returns an error unless the complete closed boundary identifies the
    /// exact current pending Effect set under retained authority.
    pub fn validate_effect_boundary_frame(
        &self,
        boundary: &ClosedExecutionBoundary<'_>,
    ) -> Result<EffectBoundary> {
        self.validate_closed_boundary_shape(boundary, false)?;
        let frame = &boundary.frame;
        let run = self.run(frame.location.run_id)?;
        let scope = run.scopes.get(frame.location.scope_id).ok_or_else(|| {
            CoreError::NotFound(format!("scope {} does not exist", frame.location.scope_id))
        })?;
        if run.execution_status != crate::RunExecutionStatus::Active
            || scope.status == crate::ScopeStatus::ClosedAborted
            || frame.location.plan_id != run.current_plan
            || frame.binding_context != run.current_binding_context
            || frame.epoch != run.epoch
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} frame is not a current Effect boundary",
                frame.location.run_id
            )));
        }
        let mut matching_intents = BTreeSet::new();
        for intent_id in run.unsettled_effect_ids() {
            let effect = run.effects.get(intent_id).ok_or_else(|| {
                CoreError::Validation(format!(
                    "unsettled Effect index references missing intent {intent_id}"
                ))
            })?;
            if !run
                .plan_lineage
                .iter()
                .any(|plan_id| plan_id == &effect.origin_plan_id)
                || !run
                    .binding_lineage
                    .iter()
                    .any(|binding| binding == &effect.execution_binding.artifact_id)
                || effect.phase == crate::EffectPhase::CancelledBeforeRelease
                || matches!(
                    effect.outcome,
                    WorldOutcome::Applied | WorldOutcome::NotApplied
                )
            {
                continue;
            }
            let effect_plan = self.plans.get(&effect.origin_plan_id).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "effect origin Plan {} does not exist",
                    effect.origin_plan_id
                ))
            })?;
            let at_boundary = if effect.scope_id == frame.location.scope_id
                && effect.invocation_id == frame.location.invocation_id
                && effect.invocation_path == frame.location.invocation_path
                && effect.definition_id == frame.location.definition_id
                && effect.region_path == frame.location.region_path
            {
                let (_, step_index) = locate_step(
                    &effect_plan.candidate,
                    &effect.definition_id,
                    &effect.region_path,
                    &effect.site_id,
                )?;
                match effect.profile.dispatch {
                    crate::DispatchPolicy::Eager => frame.location.next_step == step_index,
                    crate::DispatchPolicy::OnScopeCommit | crate::DispatchPolicy::Explicit => {
                        frame.location.next_step > step_index
                    }
                }
            } else if let Some(child_scope) =
                direct_child_scope_on_path(run, &effect.scope_id, frame.location.scope_id)
            {
                let Some((&scope_step, parent_region_path)) = child_scope.region_path.split_last()
                else {
                    continue;
                };
                child_scope.status == crate::ScopeStatus::ClosedCommitted
                    && child_scope.invocation_id == frame.location.invocation_id
                    && child_scope.invocation_path == frame.location.invocation_path
                    && child_scope.definition_id == frame.location.definition_id
                    && parent_region_path == frame.location.region_path
                    && frame.location.next_step > scope_step
            } else {
                false
            };
            if at_boundary {
                matching_intents.insert(effect.intent_id.clone());
            }
        }
        if matching_intents.is_empty() {
            return Err(CoreError::Validation(format!(
                "Run {} frame does not identify an exact pending Effect boundary",
                frame.location.run_id
            )));
        }
        let scope_id = self.validate_historical_execution_location(&frame.location)?;
        Ok(EffectBoundary {
            scope_id,
            intent_ids: matching_intents,
        })
    }

    /// Admit a claim-free Ready frame after its closed scope's Effects settled.
    ///
    /// This boundary authorizes the interpreter to acquire its next claim and
    /// continue from a completed Effect settlement. It is neither an open-scope
    /// resumable frame nor the claim-owning Run completion boundary.
    ///
    /// # Errors
    ///
    /// Returns an error unless the claim-free Ready frame is current, closed,
    /// structurally exact, and all committed Effects are settled.
    pub fn validate_post_effect_ready_frame(
        &self,
        boundary: &ClosedExecutionBoundary<'_>,
    ) -> Result<String> {
        self.validate_closed_boundary_shape(boundary, false)?;
        let frame = &boundary.frame;
        let run = self.run(frame.location.run_id)?;
        let scope = run.scopes.get(frame.location.scope_id).ok_or_else(|| {
            CoreError::NotFound(format!("scope {} does not exist", frame.location.scope_id))
        })?;
        if boundary.disposition != ClosedBoundaryDisposition::Ready
            || boundary.has_execution_claim
            || run.execution_status != crate::RunExecutionStatus::Active
            || scope.status != crate::ScopeStatus::ClosedCommitted
            || frame.location.plan_id != run.current_plan
            || frame.binding_context != run.current_binding_context
            || frame.epoch != run.epoch
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} frame is not a current post-Effect Ready boundary",
                frame.location.run_id
            )));
        }
        if run.committed_effect_count() == 0
            || run.world_settlement != crate::WorldSettlementStatus::Settled
            || run.unsettled_effect_ids().next().is_some()
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} Ready boundary has no completely settled Effect set",
                frame.location.run_id
            )));
        }
        self.validate_historical_execution_location(&frame.location)
    }

    /// Admit a current frame parked after its exact Region completed, while the
    /// owning scope is committed and the next mutation may only terminalize the Run.
    ///
    /// # Errors
    ///
    /// Returns an error unless the claim-owning boundary is current, settled,
    /// structurally exact, and positioned at the end of its Region.
    pub fn validate_completion_boundary_frame(
        &self,
        boundary: &ClosedExecutionBoundary<'_>,
    ) -> Result<String> {
        self.validate_closed_boundary_shape(boundary, true)?;
        let frame = &boundary.frame;
        let run = self.run(frame.location.run_id)?;
        let scope = run.scopes.get(frame.location.scope_id).ok_or_else(|| {
            CoreError::NotFound(format!("scope {} does not exist", frame.location.scope_id))
        })?;
        if run.execution_status != crate::RunExecutionStatus::Active
            || scope.status != crate::ScopeStatus::ClosedCommitted
            || frame.location.plan_id != run.current_plan
            || frame.binding_context != run.current_binding_context
            || frame.epoch != run.epoch
            || run.world_settlement != crate::WorldSettlementStatus::Settled
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} frame is not a current completion boundary",
                frame.location.run_id
            )));
        }
        let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
            CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
        })?;
        let region = region_at_path(
            &plan.candidate,
            frame.location.definition_id,
            frame.location.region_path,
        )?;
        if frame.location.next_step != region.steps.len() {
            return Err(CoreError::Validation(format!(
                "Run {} completion boundary is not at the end of its Region",
                frame.location.run_id
            )));
        }
        self.validate_historical_execution_location(&frame.location)
    }

    fn validate_closed_boundary_shape(
        &self,
        boundary: &ClosedExecutionBoundary<'_>,
        completion: bool,
    ) -> Result<()> {
        let frame = &boundary.frame;
        let plan = self.plans.get(frame.location.plan_id).ok_or_else(|| {
            CoreError::NotFound(format!("plan {} does not exist", frame.location.plan_id))
        })?;
        let disposition_is_legal = if completion {
            boundary.disposition == ClosedBoundaryDisposition::Running
                && boundary.has_execution_claim
        } else {
            matches!(
                (boundary.disposition, boundary.has_execution_claim),
                (ClosedBoundaryDisposition::Running, true)
                    | (ClosedBoundaryDisposition::Ready, false)
            )
        };
        if boundary.frame_count != 1
            || boundary.scope_stack != [ROOT_SCOPE_ID]
            || boundary.wait_count != 0
            || !disposition_is_legal
            || frame.location.scope_id != ROOT_SCOPE_ID
            || !frame.location.invocation_path.is_empty()
            || !frame.location.region_path.is_empty()
            || frame.location.definition_id != plan.candidate.entry
            || frame.location.next_step
                != plan
                    .candidate
                    .definitions
                    .iter()
                    .find(|definition| definition.id == plan.candidate.entry)
                    .ok_or_else(|| {
                        CoreError::NotFound(format!(
                            "Plan {} entry definition is missing",
                            plan.plan_id
                        ))
                    })?
                    .body
                    .steps
                    .len()
        {
            return Err(CoreError::IllegalTransition(format!(
                "Run {} does not have one exact root terminal closed boundary",
                frame.location.run_id
            )));
        }
        Ok(())
    }

    /// Admit a migrated replacement frame only with its exact typed Core receipt.
    ///
    /// # Errors
    ///
    /// Returns an error unless the receipt is exact retained migration
    /// authority for the current replacement frame and Continuation digest.
    pub fn validate_migration_replacement_frame(
        &self,
        frame: &ResumableExecutionFrame<'_>,
        receipt: &MigrationFrameReplacementReceipt,
        target_continuation_digest: &str,
    ) -> Result<String> {
        receipt.verify()?;
        let retained = self
            .migration_frame_replacement_receipt(&receipt.command_id)?
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "migration command {} has no replacement receipt",
                    receipt.command_id
                ))
            })?;
        if retained != *receipt
            || receipt.run_id != frame.location.run_id
            || receipt.to_plan != frame.location.plan_id
            || receipt.to_binding != frame.binding_context
            || receipt.target_epoch != frame.epoch
            || receipt.target_continuation_digest != target_continuation_digest
        {
            return Err(CoreError::IdentityMismatch(
                "migration replacement frame does not match its exact retained receipt".to_owned(),
            ));
        }
        self.validate_resumable_execution_frame(frame)
    }

    /// Derive the typed replacement receipt for one retained migration command.
    ///
    /// # Errors
    ///
    /// Returns an error when retained command/Event authority is inconsistent or
    /// the migration payload cannot produce its exact typed receipt.
    pub fn migration_frame_replacement_receipt(
        &self,
        command_id: &str,
    ) -> Result<Option<MigrationFrameReplacementReceipt>> {
        let Some(record) = self.commands.get(command_id) else {
            return Ok(None);
        };
        self.verify_retained_command_record(command_id, record)?;
        let Command::MigrateRun {
            from_plan,
            to_plan,
            from_binding,
            to_binding,
            safe_point_id,
            target_epoch,
            target_continuation_digest,
        } = &record.envelope.command
        else {
            return Err(CoreError::Validation(format!(
                "command {command_id} is not a Run migration"
            )));
        };
        if record.receipt.status != CommandReceiptStatus::Applied
            || record.receipt.error_code.is_some()
            || record.receipt.message.is_some()
        {
            return Err(CoreError::IdentityMismatch(format!(
                "migration command {command_id} has no applied Event receipt"
            )));
        }
        let [event_id] = record.receipt.event_ids.as_slice() else {
            return Err(CoreError::IdentityMismatch(format!(
                "migration command {command_id} does not have exactly one Event"
            )));
        };
        let event_id = event_id.clone();
        let observed_precondition =
            record
                .receipt
                .observed_precondition
                .clone()
                .ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "migration command {command_id} has no source precondition"
                    ))
                })?;
        let current_precondition =
            record.receipt.current_precondition.clone().ok_or_else(|| {
                CoreError::NotFound(format!(
                    "migration command {command_id} has no target precondition"
                ))
            })?;
        let mut receipt = MigrationFrameReplacementReceipt {
            receipt_version: MigrationFrameReplacementReceipt::VERSION.to_owned(),
            receipt_id: String::new(),
            command_id: command_id.to_owned(),
            event_id,
            run_id: record.envelope.run_id.clone(),
            from_plan: from_plan.clone(),
            to_plan: to_plan.clone(),
            from_binding: from_binding.clone(),
            to_binding: to_binding.clone(),
            safe_point_id: safe_point_id.clone(),
            target_epoch: *target_epoch,
            target_continuation_digest: target_continuation_digest.clone(),
            observed_precondition,
            current_precondition,
        };
        receipt.receipt_id = receipt.expected_id()?;
        receipt.verify()?;
        Ok(Some(receipt))
    }

    /// Events in admission order.
    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.event_order
            .iter()
            .filter_map(|event_id| self.events.get(event_id))
    }

    /// Export the complete uncompacted command-admission closure for exact
    /// replay.
    ///
    /// # Errors
    ///
    /// Returns an error after compaction, because archived command entries then
    /// belong to the independently retained archive rather than the hot
    /// Machine.
    pub fn replay_entries(&self) -> Result<Vec<MachineCommandArchiveEntry>> {
        if self.base.is_some() {
            return Err(CoreError::Validation(
                "exact replay entries must be resolved from the command archive after compaction"
                    .to_owned(),
            ));
        }
        self.admissions
            .iter()
            .map(|admission| {
                let command = self.commands.get(&admission.command_id).ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "CommandAdmission {} has no command record",
                        admission.admission_id
                    ))
                })?;
                let events = admission
                    .event_ids
                    .iter()
                    .map(|event_id| {
                        self.events.get(event_id).cloned().ok_or_else(|| {
                            CoreError::NotFound(format!(
                                "CommandAdmission {} has no Event {event_id}",
                                admission.admission_id
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let entry = MachineCommandArchiveEntry {
                    admission: admission.clone(),
                    command: ArchivedCommandRecord::from_private(command),
                    events,
                };
                entry.verify_shape()?;
                Ok(entry)
            })
            .collect()
    }

    /// Rebuild a projection from an unordered complete command-admission set
    /// under exact sealed-Plan and Artifact authority.
    ///
    /// Event bodies are never a second replay authority. Every applied Event is
    /// admitted only through its exact command record, receipt, admission hash
    /// chain, and before/after Projection digests. Batches follow their exact
    /// admission-parent authority roots, including zero-command material batches.
    /// Entries retain their authenticated admission order and each command is
    /// reduced exactly once; replay never repeatedly scans the remaining Event
    /// set. Total cost also includes indexing plus reducer and canonical
    /// Projection work for each admitted command.
    ///
    /// # Errors
    ///
    /// Returns an error when any Plan, Artifact, command, admission, Event,
    /// causal edge, or Projection frontier is invalid or missing.
    pub fn replay(
        plans: impl IntoIterator<Item = SealedPlan>,
        artifacts: impl IntoIterator<Item = ArtifactRecord>,
        batches: impl IntoIterator<Item = MachineCommandBatchRecord>,
        entries: impl IntoIterator<Item = MachineCommandArchiveEntry>,
    ) -> Result<Projection> {
        let mut authority = Self::new();
        let plan_catalog = plans
            .into_iter()
            .map(|plan| (plan.plan_id.clone(), plan))
            .collect::<BTreeMap<_, _>>();
        let artifact_catalog = artifacts
            .into_iter()
            .map(|artifact| (artifact.reference.artifact_id.clone(), artifact))
            .collect::<BTreeMap<_, _>>();
        for plan in plan_catalog.values() {
            plan.verify()?;
        }
        for artifact in artifact_catalog.values() {
            artifact.validate()?;
        }
        let mut entries = entries.into_iter().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.admission.sequence);
        let mut command_ids = BTreeSet::new();
        let mut admission_ids = BTreeSet::new();
        let mut previous: Option<&CommandAdmission> = None;
        for entry in &entries {
            entry.verify_shape()?;
            entry
                .admission
                .verify(previous.map(CommandAdmissionParent::from))?;
            if !command_ids.insert(entry.admission.command_id.clone())
                || !admission_ids.insert(entry.admission.admission_id.clone())
            {
                return Err(CoreError::IdentityMismatch(
                    "exact replay repeats a command or admission identity".to_owned(),
                ));
            }
            previous = Some(&entry.admission);
        }
        let entry_map = entries
            .iter()
            .map(|entry| (entry.admission.command_id.as_str(), entry))
            .collect::<BTreeMap<_, _>>();
        let batches = batches.into_iter().collect::<Vec<_>>();
        let mut batch_ids = BTreeSet::new();
        let mut batches_by_parent = BTreeMap::new();
        for batch in &batches {
            batch.verify()?;
            if !batch_ids.insert(batch.batch_id.clone()) {
                return Err(CoreError::IdentityMismatch(
                    "exact replay repeats a batch".to_owned(),
                ));
            }
            if batches_by_parent
                .insert(batch.admission_parent_authority_root.as_str(), batch)
                .is_some()
            {
                return Err(CoreError::IdentityMismatch(
                    "exact replay has multiple batches for one admission parent".to_owned(),
                ));
            }
        }
        let mut sources = MachineReplayAncestry::default();
        while !batches_by_parent.is_empty() {
            let parent = authority.authority_root()?;
            let batch = batches_by_parent.remove(parent.as_str()).ok_or_else(|| {
                CoreError::IdentityMismatch(
                    "exact replay batch ancestry is incomplete or discontinuous".to_owned(),
                )
            })?;
            authority.replay_archived_batch(
                batch,
                &entry_map,
                &plan_catalog,
                &artifact_catalog,
                &mut sources,
            )?;
        }
        if authority.commands.len() != entries.len() {
            return Err(CoreError::IdentityMismatch(
                "replay has commands outside complete batches".to_owned(),
            ));
        }
        authority.validate_projection_authority(&authority.projection)?;
        Ok(authority.projection)
    }

    /// Replay all current events and verify the digest matches the live projection.
    ///
    /// # Errors
    ///
    /// Returns an error when derived indexes or reducer invariants fail, replay
    /// is incomplete, or restored authority differs from the live Machine.
    pub fn verify_replay(&self) -> Result<()> {
        self.projection.verify_derived_indexes()?;
        self.projection.verify_reducer_invariants()?;
        let restored = match self.base_anchor.as_ref() {
            Some(anchor) => Self::restore_anchored(self.snapshot(), anchor)?,
            None => Self::restore(self.snapshot())?,
        };
        if restored.projection != self.projection
            || restored.authority_root()? != self.authority_root()?
        {
            return Err(CoreError::IdentityMismatch(
                "replayed Machine projection or authority root does not match the live Machine"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn compaction_frontier(&self) -> BTreeSet<String> {
        let mut frontier: BTreeSet<String> = self
            .events()
            .flat_map(|event| event.parents.iter())
            .filter(|parent| self.compacted_event_ids.contains(*parent))
            .cloned()
            .collect();
        if let Some(base) = &self.base {
            frontier.extend(
                base.projection
                    .runs
                    .values()
                    .map(|run| run.last_event.clone()),
            );
        }
        frontier
    }

    fn admit_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::StartRun { .. } => self.admit_run_start_command(envelope),
            Command::BeginAttempt { .. } | Command::YieldAttempt { .. } | Command::AdvanceEpoch => {
                self.admit_attempt_command(envelope)
            }
            Command::OpenScope { .. } => self.admit_scope_open_command(envelope),
            Command::ProposeEffect { .. } => self.admit_effect_proposal_command(envelope),
            Command::CommitScope { .. } | Command::AbortScope { .. } => {
                self.admit_scope_close_command(envelope)
            }
            Command::MigrateRun { .. } => self.admit_migration_command(envelope),
            Command::CompleteRun { .. } | Command::FailRun { .. } | Command::CancelRun { .. } => {
                self.admit_run_termination_command(envelope)
            }
            Command::TransitionEffect {
                intent_id,
                transition,
            } => Ok(EventPayload::EffectTransitioned {
                intent_id: intent_id.clone(),
                transition: transition.clone(),
            }),
            Command::UpdateBinding { binding_context } => {
                let run = self.run(&envelope.run_id)?;
                self.require_execution_binding(binding_context)?;
                Ok(EventPayload::BindingUpdated {
                    previous: run.current_binding_context.clone(),
                    current: binding_context.clone(),
                })
            }
            Command::RecordFact { key, value } => {
                validate_identity("fact key", key)?;
                crate::validate_content_id("fact value", value)?;
                Ok(EventPayload::FactRecorded {
                    key: key.clone(),
                    value: value.clone(),
                })
            }
        }
    }

    fn admit_run_start_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::StartRun {
                plan_id,
                binding_context,
                input,
                material_digest,
                initial_attempt,
            } => {
                if self.projection.runs.contains_key(&envelope.run_id) {
                    return Err(CoreError::IllegalTransition(format!(
                        "Run {} already exists",
                        envelope.run_id
                    )));
                }
                let plan = self
                    .plans
                    .get(plan_id)
                    .ok_or_else(|| CoreError::NotFound(format!("plan {plan_id} does not exist")))?;
                let binding = self.require_execution_binding(binding_context)?;
                self.validate_run_input(plan, input)?;
                initial_attempt.verify(binding_context)?;
                let input_record = self.artifact(input).ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "Run input Artifact {} does not exist",
                        input.artifact_id
                    ))
                })?;
                let material = pinned::MachineMaterialAdmission::new(
                    envelope.command_id.clone(),
                    vec![plan.clone()],
                    vec![binding.clone(), input_record.clone()],
                )?;
                if material.material_digest() != material_digest {
                    return Err(CoreError::IdentityMismatch(
                        "StartRun command has the wrong immutable-material digest".to_owned(),
                    ));
                }
                Ok(EventPayload::RunStarted {
                    plan_id: plan_id.clone(),
                    entry_definition: plan.candidate.entry.clone(),
                    binding_context: binding_context.clone(),
                    input: input.clone(),
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_attempt_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::BeginAttempt {
                attempt_id,
                continuation_id,
                occurrence_binding,
                continuation_epoch,
                execution_fence,
            } => {
                crate::validate_content_id("Attempt", attempt_id)?;
                crate::validate_content_id("Continuation", continuation_id)?;
                crate::validate_content_id("occurrence binding", occurrence_binding)?;
                validate_attempt_numbers(*continuation_epoch, *execution_fence)?;
                Ok(EventPayload::AttemptStarted {
                    attempt_id: attempt_id.clone(),
                    continuation_id: continuation_id.clone(),
                    occurrence_binding: occurrence_binding.clone(),
                    continuation_epoch: *continuation_epoch,
                    execution_fence: *execution_fence,
                })
            }
            Command::YieldAttempt {
                attempt_id,
                continuation_epoch,
                execution_fence,
            } => {
                crate::validate_content_id("Attempt", attempt_id)?;
                validate_attempt_numbers(*continuation_epoch, *execution_fence)?;
                Ok(EventPayload::AttemptYielded {
                    attempt_id: attempt_id.clone(),
                    continuation_epoch: *continuation_epoch,
                    execution_fence: *execution_fence,
                })
            }
            Command::AdvanceEpoch => {
                let run = self.run(&envelope.run_id)?;
                Ok(EventPayload::EpochAdvanced {
                    epoch: run
                        .epoch
                        .checked_add(1)
                        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
                        .ok_or_else(|| {
                            CoreError::IllegalTransition("Run epoch overflowed".to_owned())
                        })?,
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_scope_open_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::OpenScope {
                scope_id,
                parent_scope,
                invocation_id,
                invocation_path,
                definition_id,
                region_path,
                site_id,
            } => {
                let run = self.run(&envelope.run_id)?;
                let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
                    CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
                })?;
                validate_execution_location(ExecutionLocation {
                    candidate: &plan.candidate,
                    plan_id: &run.current_plan,
                    run,
                    invocation_id,
                    invocation_path,
                    definition_id,
                    region_path,
                    scope_id: parent_scope,
                })?;
                let (_, step_index) =
                    locate_step(&plan.candidate, definition_id, region_path, site_id)?;
                let step = step_at(&plan.candidate, definition_id, region_path, step_index)?;
                if !matches!(step.operation, Operation::Scope { .. }) {
                    return Err(CoreError::Validation(format!(
                        "site {site_id} is not a scope operation"
                    )));
                }
                let mut body_path = region_path.clone();
                body_path.push(step_index);
                let expected_scope_id = plan_scope_id(
                    &envelope.run_id,
                    &run.current_plan,
                    invocation_id,
                    definition_id,
                    &body_path,
                )?;
                if *scope_id != expected_scope_id {
                    return Err(CoreError::Validation(format!(
                        "scope identity {scope_id} does not match {expected_scope_id}"
                    )));
                }
                Ok(EventPayload::ScopeOpened {
                    scope_id: scope_id.clone(),
                    parent_scope: parent_scope.clone(),
                    invocation_id: invocation_id.clone(),
                    invocation_path: invocation_path.clone(),
                    definition_id: definition_id.clone(),
                    region_path: body_path,
                    site_id: site_id.clone(),
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_effect_proposal_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::ProposeEffect {
                scope_id,
                invocation_id,
                invocation_path,
                definition_id,
                region_path,
                site_id,
                occurrence,
                operation,
                args,
                execution_binding,
                occurrence_binding,
            } => {
                let run =
                    self.admit_effect_run(&envelope.run_id, execution_binding, occurrence_binding)?;
                let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
                    CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
                })?;
                let scope = run.scopes.get(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != crate::ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                validate_execution_location(ExecutionLocation {
                    candidate: &plan.candidate,
                    plan_id: &run.current_plan,
                    run,
                    invocation_id,
                    invocation_path,
                    definition_id,
                    region_path,
                    scope_id,
                })?;
                let (step, _) = locate_step(&plan.candidate, definition_id, region_path, site_id)?;
                let Operation::Effect {
                    effect: declared_operation,
                    occurrence: declared_occurrence,
                    ..
                } = &step.operation
                else {
                    return Err(CoreError::Validation(format!(
                        "site {site_id} is not an effect operation"
                    )));
                };
                if declared_operation != operation || declared_occurrence != occurrence {
                    return Err(CoreError::Validation(format!(
                        "effect site {site_id} declares operation {declared_operation} and occurrence {declared_occurrence}, not {operation} and {occurrence}"
                    )));
                }
                self.validate_effect_args(&run.current_plan, declared_operation, args)?;
                let contract = self.effect_contract(&run.current_plan, declared_operation)?;
                let intent_id = effect_intent_id(&EffectIntentIdentityInput {
                    run_id: &envelope.run_id,
                    plan_id: &run.current_plan,
                    invocation_id,
                    site_id,
                    scope_id,
                    occurrence,
                    args,
                    effect_schema_version: crate::EFFECT_SCHEMA_VERSION,
                })?;
                Ok(EventPayload::EffectProposed {
                    intent_id,
                    origin_plan_id: run.current_plan.clone(),
                    scope_id: scope_id.clone(),
                    invocation_id: invocation_id.clone(),
                    invocation_path: invocation_path.clone(),
                    definition_id: definition_id.clone(),
                    region_path: region_path.clone(),
                    site_id: site_id.clone(),
                    occurrence: occurrence.clone(),
                    effect_schema_version: crate::EFFECT_SCHEMA_VERSION.to_owned(),
                    operation: operation.clone(),
                    profile: contract.profile.clone(),
                    args: Box::new(args.clone()),
                    execution_binding: Box::new(execution_binding.clone()),
                    occurrence_binding: occurrence_binding.clone(),
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_scope_close_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::CommitScope { scope_id } => {
                let run = self.run(&envelope.run_id)?;
                let scope = run.scopes.get(scope_id).ok_or_else(|| {
                    CoreError::NotFound(format!("scope {scope_id} does not exist"))
                })?;
                if scope.status != crate::ScopeStatus::Open {
                    return Err(CoreError::IllegalTransition(format!(
                        "scope {scope_id} is not open"
                    )));
                }
                let mut obligations = Vec::new();
                let mutating_intents =
                    run.scope_mutating_intent_ids(scope_id).ok_or_else(|| {
                        CoreError::Validation(format!(
                            "open scope {scope_id} has no derived Effect index"
                        ))
                    })?;
                for intent_id in mutating_intents {
                    let effect = run.effects.get(intent_id).ok_or_else(|| {
                        CoreError::NotFound(format!("effect {intent_id} does not exist"))
                    })?;
                    obligations.push(obligation_for_effect(effect)?);
                }
                let (obligation_count, obligation_commitment) =
                    scope_obligation_summary(&obligations)?;
                Ok(EventPayload::ScopeCommitted {
                    scope_id: scope_id.clone(),
                    obligation_count,
                    obligation_commitment,
                })
            }
            Command::AbortScope { scope_id } => Ok(EventPayload::ScopeAborted {
                scope_id: scope_id.clone(),
            }),
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_migration_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::MigrateRun {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                safe_point_id,
                target_epoch,
                target_continuation_digest,
            } => {
                let run = self.run(&envelope.run_id)?;
                validate_migration_payload(
                    from_plan,
                    to_plan,
                    from_binding,
                    to_binding,
                    safe_point_id,
                    *target_epoch,
                    target_continuation_digest,
                )?;
                if &run.current_plan != from_plan || &run.current_binding_context != from_binding {
                    return Err(CoreError::IllegalTransition(
                        "Run migration does not match current Plan and binding".to_owned(),
                    ));
                }
                if !self.plans.contains_key(to_plan) {
                    return Err(CoreError::NotFound(format!(
                        "migration target Plan {to_plan} does not exist"
                    )));
                }
                self.require_execution_binding(from_binding)?;
                self.require_execution_binding(to_binding)?;
                if run.active_attempt_id().is_some() {
                    return Err(CoreError::IllegalTransition(
                        "Run migration requires every prior Attempt to be inactive".to_owned(),
                    ));
                }
                if run.epoch.checked_add(1) != Some(*target_epoch) {
                    return Err(CoreError::IllegalTransition(
                        "Run migration target epoch is not the exact next epoch".to_owned(),
                    ));
                }
                Ok(EventPayload::RunMigrated {
                    from_plan: from_plan.clone(),
                    to_plan: to_plan.clone(),
                    from_binding: from_binding.clone(),
                    to_binding: to_binding.clone(),
                    safe_point_id: safe_point_id.clone(),
                    target_epoch: *target_epoch,
                    target_continuation_digest: target_continuation_digest.clone(),
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_run_termination_command(&self, envelope: &CommandEnvelope) -> Result<EventPayload> {
        match &envelope.command {
            Command::CompleteRun { result } => {
                if let Some(reference) = result
                    && self.artifact(reference).is_none()
                {
                    return Err(CoreError::NotFound(format!(
                        "Result artifact {} does not exist",
                        reference.artifact_id
                    )));
                }
                Ok(EventPayload::RunCompleted {
                    result: result.clone(),
                })
            }
            Command::FailRun { failure } => {
                failure.verify()?;
                if self.artifact(&failure.detail).is_none() {
                    return Err(CoreError::NotFound(format!(
                        "Run failure detail artifact {} does not exist",
                        failure.detail.artifact_id
                    )));
                }
                let run = self.run(&envelope.run_id)?;
                Ok(EventPayload::RunFailed {
                    failure: failure.clone(),
                    epoch: run
                        .epoch
                        .checked_add(1)
                        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
                        .ok_or_else(|| {
                            CoreError::IllegalTransition(
                                "Run failure execution fence overflowed".to_owned(),
                            )
                        })?,
                })
            }
            Command::CancelRun { reason } => {
                reason.validate()?;
                if self.artifact(reason).is_none() {
                    return Err(CoreError::NotFound(format!(
                        "Run cancellation reason artifact {} does not exist",
                        reason.artifact_id
                    )));
                }
                let run = self.run(&envelope.run_id)?;
                Ok(EventPayload::RunCancelled {
                    reason: reason.clone(),
                    epoch: run
                        .epoch
                        .checked_add(1)
                        .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
                        .ok_or_else(|| {
                            CoreError::IllegalTransition(
                                "Run cancellation execution fence overflowed".to_owned(),
                            )
                        })?,
                })
            }
            _ => Err(CoreError::Validation(
                "command admission was routed to an incompatible handler".to_owned(),
            )),
        }
    }

    fn admit_effect_run(
        &self,
        run_id: &str,
        execution_binding: &ArtifactRef,
        occurrence_binding: &str,
    ) -> Result<&RunProjection> {
        crate::validate_content_id("effect occurrence binding", occurrence_binding)?;
        if self.artifact(execution_binding).is_none() {
            return Err(CoreError::NotFound(format!(
                "effect execution binding Artifact {} does not exist",
                execution_binding.artifact_id
            )));
        }
        let run = self.run(run_id)?;
        execution_binding.validate()?;
        if execution_binding.kind != crate::EXECUTION_BINDING_ARTIFACT_KIND
            || execution_binding.artifact_id != run.current_binding_context
        {
            return Err(CoreError::IllegalTransition(
                "effect execution binding does not match the Run binding at admission".to_owned(),
            ));
        }
        Ok(run)
    }

    fn run(&self, run_id: &str) -> Result<&crate::RunProjection> {
        self.projection
            .runs
            .get(run_id)
            .ok_or_else(|| CoreError::NotFound(format!("Run {run_id} does not exist")))
    }

    fn effect_contract(&self, plan_id: &str, operation: &str) -> Result<&EffectContract> {
        self.plans
            .get(plan_id)
            .and_then(|plan| {
                plan.candidate
                    .effects
                    .iter()
                    .find(|contract| contract.id == operation)
            })
            .ok_or_else(|| {
                CoreError::NotFound(format!(
                    "effect operation {operation} does not exist in plan {plan_id}"
                ))
            })
    }

    fn validate_effect_args(
        &self,
        plan_id: &str,
        operation: &str,
        args: &ArtifactRef,
    ) -> Result<()> {
        args.validate()?;
        if args.kind != crate::EFFECT_ARGS_ARTIFACT_KIND {
            return Err(CoreError::Validation(format!(
                "effect argument Artifact must have exact kind {}",
                crate::EFFECT_ARGS_ARTIFACT_KIND
            )));
        }
        let artifact = self.artifact(args).ok_or_else(|| {
            CoreError::NotFound(format!(
                "effect argument Artifact {} does not exist",
                args.artifact_id
            ))
        })?;
        let value: serde_json::Value = crate::decode_json(&artifact.bytes)?;
        if crate::canonical_bytes(&value)? != artifact.bytes {
            return Err(CoreError::Validation(format!(
                "effect argument Artifact {} is not strict canonical JSON",
                args.artifact_id
            )));
        }
        let contract = self.effect_contract(plan_id, operation)?;
        validate_schema_instance("effect argument", &contract.input_schema, &value)
    }

    fn require_execution_binding(&self, artifact_id: &str) -> Result<&ArtifactRecord> {
        let artifact = self.artifacts.get(artifact_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "execution binding Artifact {artifact_id} does not exist"
            ))
        })?;
        if artifact.reference.kind != crate::EXECUTION_BINDING_ARTIFACT_KIND {
            return Err(CoreError::Validation(format!(
                "Artifact {artifact_id} is not a cymule.execution-binding/2 Artifact"
            )));
        }
        Ok(artifact)
    }

    fn validate_run_input(&self, plan: &SealedPlan, input: &ArtifactRef) -> Result<()> {
        input.validate()?;
        if input.kind != crate::RUN_INPUT_ARTIFACT_KIND {
            return Err(CoreError::Validation(format!(
                "Run input Artifact must have exact kind {}",
                crate::RUN_INPUT_ARTIFACT_KIND
            )));
        }
        let artifact = self.artifact(input).ok_or_else(|| {
            CoreError::NotFound(format!(
                "Run input Artifact {} does not exist",
                input.artifact_id
            ))
        })?;
        let value: serde_json::Value = crate::decode_json(&artifact.bytes)?;
        if crate::canonical_bytes(&value)? != artifact.bytes {
            return Err(CoreError::Validation(format!(
                "Run input Artifact {} is not strict canonical JSON",
                input.artifact_id
            )));
        }
        let entry = plan
            .candidate
            .definitions
            .iter()
            .find(|definition| definition.id == plan.candidate.entry)
            .ok_or_else(|| {
                CoreError::NotFound(format!("Plan {} entry definition is missing", plan.plan_id))
            })?;
        validate_schema_instance("Run input", &entry.input_schema, &value)
    }

    fn validate_event_authority(&self, event: &Event) -> Result<()> {
        self.validate_event_plan_authority(event)?;
        match &event.payload {
            EventPayload::EffectProposed {
                origin_plan_id,
                operation,
                args,
                execution_binding,
                occurrence_binding,
                ..
            } => {
                crate::validate_content_id("effect occurrence binding", occurrence_binding)?;
                self.validate_effect_args(origin_plan_id, operation, args)?;
                if self.artifact(execution_binding).is_none() {
                    return Err(CoreError::NotFound(format!(
                        "effect execution binding Artifact {} does not exist",
                        execution_binding.artifact_id
                    )));
                }
            }
            EventPayload::AttemptStarted {
                attempt_id,
                continuation_id,
                occurrence_binding,
                ..
            } => {
                crate::validate_content_id("Attempt", attempt_id)?;
                crate::validate_content_id("Continuation", continuation_id)?;
                crate::validate_content_id("occurrence binding", occurrence_binding)?;
            }
            EventPayload::AttemptYielded { attempt_id, .. } => {
                crate::validate_content_id("Attempt", attempt_id)?;
            }
            EventPayload::RunCompleted {
                result: Some(result),
            } if self.artifact(result).is_none() => {
                return Err(CoreError::NotFound(format!(
                    "Run result Artifact {} does not exist",
                    result.artifact_id
                )));
            }
            EventPayload::RunFailed { failure, .. } if self.artifact(&failure.detail).is_none() => {
                return Err(CoreError::NotFound(format!(
                    "Run failure detail Artifact {} does not exist",
                    failure.detail.artifact_id
                )));
            }
            EventPayload::RunCancelled { reason, .. } if self.artifact(reason).is_none() => {
                return Err(CoreError::NotFound(format!(
                    "Run cancellation reason Artifact {} does not exist",
                    reason.artifact_id
                )));
            }
            _ => {}
        }
        self.validate_scope_open(event)?;
        self.validate_effect_proposal(event)
    }

    fn validate_event_plan_authority(&self, event: &Event) -> Result<()> {
        match &event.payload {
            EventPayload::RunStarted {
                plan_id,
                entry_definition,
                binding_context,
                input,
            } => {
                self.validate_run_start_authority(
                    plan_id,
                    entry_definition,
                    binding_context,
                    input,
                )?;
            }
            EventPayload::BindingUpdated { previous, current } => {
                self.require_execution_binding(previous)?;
                self.require_execution_binding(current)?;
            }
            EventPayload::RunMigrated {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                safe_point_id,
                target_epoch,
                target_continuation_digest,
            } => {
                validate_migration_payload(
                    from_plan,
                    to_plan,
                    from_binding,
                    to_binding,
                    safe_point_id,
                    *target_epoch,
                    target_continuation_digest,
                )?;
                let run = self.run(&event.run_id)?;
                if run.epoch.checked_add(1) != Some(*target_epoch) {
                    return Err(CoreError::IllegalTransition(
                        "RunMigrated Event target epoch is not the exact next epoch".to_owned(),
                    ));
                }
                if !self.plans.contains_key(from_plan) || !self.plans.contains_key(to_plan) {
                    return Err(CoreError::NotFound(
                        "Run migration references a missing sealed Plan".to_owned(),
                    ));
                }
                self.require_execution_binding(from_binding)?;
                self.require_execution_binding(to_binding)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_run_start_authority(
        &self,
        plan_id: &str,
        entry_definition: &str,
        binding_context: &str,
        input: &ArtifactRef,
    ) -> Result<()> {
        let plan = self
            .plans
            .get(plan_id)
            .ok_or_else(|| CoreError::NotFound(format!("plan {plan_id} does not exist")))?;
        if plan.candidate.entry != entry_definition {
            return Err(CoreError::Validation(format!(
                "Run start entry {entry_definition} does not match Plan {plan_id}"
            )));
        }
        self.require_execution_binding(binding_context)?;
        self.validate_run_input(plan, input)?;
        Ok(())
    }

    fn validate_projection_authority(&self, projection: &Projection) -> Result<()> {
        projection.verify_reducer_invariants()?;
        for run in projection.runs.values() {
            if run.plan_lineage.first() != Some(&run.initial_plan)
                || run.plan_lineage.last() != Some(&run.current_plan)
            {
                return Err(CoreError::Validation(format!(
                    "Run {} has an inexact Plan migration lineage",
                    run.run_id
                )));
            }
            for plan_id in &run.plan_lineage {
                if !self.plans.contains_key(plan_id) {
                    return Err(CoreError::NotFound(format!(
                        "Run {} lineage Plan {plan_id} does not exist",
                        run.run_id
                    )));
                }
            }
            if run.binding_lineage.first() != Some(&run.initial_binding_context)
                || run.binding_lineage.last() != Some(&run.current_binding_context)
            {
                return Err(CoreError::Validation(format!(
                    "Run {} has an inexact execution-binding lineage",
                    run.run_id
                )));
            }
            for binding in &run.binding_lineage {
                self.require_execution_binding(binding)?;
            }
            if let Some(result) = &run.result
                && self.artifact(result).is_none()
            {
                return Err(CoreError::NotFound(format!(
                    "Run {} result Artifact {} does not exist",
                    run.run_id, result.artifact_id
                )));
            }
            match &run.execution_status {
                crate::RunExecutionStatus::Failed { failure } => {
                    failure.verify()?;
                    if self.artifact(&failure.detail).is_none() {
                        return Err(CoreError::NotFound(format!(
                            "Run {} failure detail Artifact {} does not exist",
                            run.run_id, failure.detail.artifact_id
                        )));
                    }
                }
                crate::RunExecutionStatus::Cancelled { reason } => {
                    reason.validate()?;
                    if self.artifact(reason).is_none() {
                        return Err(CoreError::NotFound(format!(
                            "Run {} cancellation reason Artifact {} does not exist",
                            run.run_id, reason.artifact_id
                        )));
                    }
                }
                crate::RunExecutionStatus::Active | crate::RunExecutionStatus::Completed => {}
            }

            let initial_plan = self.plans.get(&run.initial_plan).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "Run {} initial Plan {} does not exist",
                    run.run_id, run.initial_plan
                ))
            })?;
            let root = run.scopes.get(ROOT_SCOPE_ID).ok_or_else(|| {
                CoreError::NotFound(format!("Run {} has no root scope", run.run_id))
            })?;
            let expected_root = plan_invocation_id(
                &run.run_id,
                &run.initial_plan,
                &initial_plan.candidate.entry,
                &[],
            )?;
            if root.invocation_id != expected_root
                || root.definition_id != initial_plan.candidate.entry
                || root.parent_scope.is_some()
                || !root.invocation_path.is_empty()
                || !root.region_path.is_empty()
            {
                return Err(CoreError::Validation(format!(
                    "Run {} root scope does not match its initial Plan",
                    run.run_id
                )));
            }

            for scope in run
                .scopes
                .values()
                .filter(|scope| scope.scope_id != ROOT_SCOPE_ID)
            {
                self.validate_scope_projection_authority(run, scope)?;
            }
            for effect in run.effects.values() {
                self.validate_effect_projection_authority(run, effect)?;
            }
        }
        Ok(())
    }

    fn validate_scope_projection_authority(
        &self,
        run: &RunProjection,
        scope: &crate::ScopeProjection,
    ) -> Result<()> {
        let mut matching_plans = BTreeSet::new();
        for plan_id in &run.plan_lineage {
            if !matching_plans.contains(plan_id) {
                let plan = self.plans.get(plan_id).ok_or_else(|| {
                    CoreError::NotFound(format!(
                        "Run {} lineage Plan {plan_id} does not exist",
                        run.run_id
                    ))
                })?;
                if scope_matches_plan(&plan.candidate, plan_id, run, scope)? {
                    matching_plans.insert(plan_id.clone());
                }
            }
        }
        if matching_plans.len() != 1 {
            return Err(CoreError::Validation(format!(
                "scope {} does not have one exact Plan and lexical-path authority",
                scope.scope_id
            )));
        }
        Ok(())
    }

    fn validate_effect_projection_authority(
        &self,
        run: &RunProjection,
        effect: &crate::EffectProjection,
    ) -> Result<()> {
        self.validate_effect_projection_material(run, effect)?;
        let plan = self.plans.get(&effect.origin_plan_id).ok_or_else(|| {
            CoreError::NotFound(format!(
                "effect origin Plan {} does not exist",
                effect.origin_plan_id
            ))
        })?;
        let (definition, invocation_id) = resolve_invocation(
            &plan.candidate,
            &effect.origin_plan_id,
            run,
            &effect.invocation_path,
            false,
        )?;
        if definition != effect.definition_id || invocation_id != effect.invocation_id {
            return Err(CoreError::Validation(format!(
                "effect {} invocation does not match its origin Plan",
                effect.intent_id
            )));
        }
        validate_execution_location_scope_only(
            run,
            &effect.invocation_id,
            &effect.invocation_path,
            &effect.definition_id,
            &effect.region_path,
            &effect.scope_id,
            false,
        )?;
        let (step, _) = locate_step(
            &plan.candidate,
            &effect.definition_id,
            &effect.region_path,
            &effect.site_id,
        )?;
        let Operation::Effect {
            effect: operation,
            occurrence,
            ..
        } = &step.operation
        else {
            return Err(CoreError::Validation(format!(
                "effect {} site is not an Effect operation",
                effect.intent_id
            )));
        };
        let contract = self.effect_contract(&effect.origin_plan_id, operation)?;
        if operation != &effect.operation
            || occurrence != &effect.occurrence
            || contract.profile != effect.profile
        {
            return Err(CoreError::Validation(format!(
                "effect {} does not match its Plan-declared site, operation, occurrence, and profile",
                effect.intent_id
            )));
        }
        let expected_intent = effect_intent_id(&EffectIntentIdentityInput {
            run_id: &run.run_id,
            plan_id: &effect.origin_plan_id,
            invocation_id: &effect.invocation_id,
            site_id: &effect.site_id,
            scope_id: &effect.scope_id,
            occurrence: &effect.occurrence,
            args: &effect.args,
            effect_schema_version: &effect.effect_schema_version,
        })?;
        if effect.intent_id != expected_intent {
            return Err(CoreError::IdentityMismatch(format!(
                "effect intent {} does not match {expected_intent}",
                effect.intent_id
            )));
        }
        Ok(())
    }

    fn validate_effect_projection_material(
        &self,
        run: &RunProjection,
        effect: &crate::EffectProjection,
    ) -> Result<()> {
        if !run
            .plan_lineage
            .iter()
            .any(|plan_id| plan_id == &effect.origin_plan_id)
        {
            return Err(CoreError::Validation(format!(
                "effect {} origin Plan is outside Run {} migration lineage",
                effect.intent_id, run.run_id
            )));
        }
        if effect.effect_schema_version != crate::EFFECT_SCHEMA_VERSION {
            return Err(CoreError::Validation(format!(
                "effect {} has unsupported schema version {:?}",
                effect.intent_id, effect.effect_schema_version
            )));
        }
        self.validate_effect_args(&effect.origin_plan_id, &effect.operation, &effect.args)?;
        if self.artifact(&effect.execution_binding).is_none()
            || effect.execution_binding.kind != crate::EXECUTION_BINDING_ARTIFACT_KIND
        {
            return Err(CoreError::NotFound(format!(
                "effect execution binding Artifact {} does not exist with the exact kind",
                effect.execution_binding.artifact_id
            )));
        }
        if !run
            .binding_lineage
            .iter()
            .any(|binding| binding == &effect.execution_binding.artifact_id)
        {
            return Err(CoreError::Validation(format!(
                "effect {} execution binding is outside Run {} binding lineage",
                effect.intent_id, run.run_id
            )));
        }
        Ok(())
    }

    fn validate_effect_proposal(&self, event: &Event) -> Result<()> {
        let EventPayload::EffectProposed {
            origin_plan_id,
            scope_id,
            invocation_id,
            invocation_path,
            definition_id,
            region_path,
            site_id,
            occurrence,
            operation,
            profile,
            execution_binding,
            ..
        } = &event.payload
        else {
            return Ok(());
        };
        let run = self.run(&event.run_id)?;
        if origin_plan_id != &run.current_plan
            || execution_binding.artifact_id != run.current_binding_context
            || execution_binding.kind != crate::EXECUTION_BINDING_ARTIFACT_KIND
        {
            return Err(CoreError::Validation(
                "effect origin does not match the Run Plan and binding at admission".to_owned(),
            ));
        }
        let plan = self
            .plans
            .get(origin_plan_id)
            .ok_or_else(|| CoreError::NotFound(format!("plan {origin_plan_id} does not exist")))?;
        validate_execution_location(ExecutionLocation {
            candidate: &plan.candidate,
            plan_id: origin_plan_id,
            run,
            invocation_id,
            invocation_path,
            definition_id,
            region_path,
            scope_id,
        })?;
        let (step, _) = locate_step(&plan.candidate, definition_id, region_path, site_id)?;
        let Operation::Effect {
            effect: declared_operation,
            occurrence: declared_occurrence,
            ..
        } = &step.operation
        else {
            return Err(CoreError::Validation(format!(
                "site {site_id} is not an effect operation"
            )));
        };
        if declared_operation != operation || declared_occurrence != occurrence {
            return Err(CoreError::Validation(format!(
                "effect site {site_id} does not match its declared operation and occurrence"
            )));
        }
        let contract = self.effect_contract(origin_plan_id, operation)?;
        if &contract.profile != profile {
            return Err(CoreError::Validation(format!(
                "effect site {site_id} does not match its Plan-declared profile"
            )));
        }
        Ok(())
    }

    fn validate_scope_open(&self, event: &Event) -> Result<()> {
        let EventPayload::ScopeOpened {
            scope_id,
            parent_scope,
            invocation_id,
            invocation_path,
            definition_id,
            region_path,
            site_id,
            ..
        } = &event.payload
        else {
            return Ok(());
        };
        let run = self.run(&event.run_id)?;
        let plan = self.plans.get(&run.current_plan).ok_or_else(|| {
            CoreError::NotFound(format!("plan {} does not exist", run.current_plan))
        })?;
        let (&step_index, parent_region_path) = region_path.split_last().ok_or_else(|| {
            CoreError::Validation("opened scope body must have a non-empty Region path".to_owned())
        })?;
        validate_execution_location(ExecutionLocation {
            candidate: &plan.candidate,
            plan_id: &run.current_plan,
            run,
            invocation_id,
            invocation_path,
            definition_id,
            region_path: parent_region_path,
            scope_id: parent_scope,
        })?;
        let step = step_at(
            &plan.candidate,
            definition_id,
            parent_region_path,
            step_index,
        )?;
        if step.id != *site_id || !matches!(step.operation, Operation::Scope { .. }) {
            return Err(CoreError::Validation(format!(
                "scope site {site_id} does not match its exact lexical path"
            )));
        }
        let expected_scope_id = plan_scope_id(
            &event.run_id,
            &run.current_plan,
            invocation_id,
            definition_id,
            region_path,
        )?;
        if *scope_id != expected_scope_id {
            return Err(CoreError::Validation(format!(
                "scope identity {scope_id} does not match {expected_scope_id}"
            )));
        }
        Ok(())
    }
}

fn scope_matches_plan(
    candidate: &PlanCandidate,
    plan_id: &str,
    run: &RunProjection,
    scope: &crate::ScopeProjection,
) -> Result<bool> {
    let Some(site_id) = scope.site_id.as_deref() else {
        return Ok(false);
    };
    let Some((&step_index, parent_region_path)) = scope.region_path.split_last() else {
        return Ok(false);
    };
    let Ok((definition_id, invocation_id)) =
        resolve_invocation(candidate, plan_id, run, &scope.invocation_path, false)
    else {
        return Ok(false);
    };
    if definition_id != scope.definition_id || invocation_id != scope.invocation_id {
        return Ok(false);
    }
    let Some(parent_scope) = scope.parent_scope.as_deref() else {
        return Ok(false);
    };
    if validate_execution_location_scope_only(
        run,
        &scope.invocation_id,
        &scope.invocation_path,
        &scope.definition_id,
        parent_region_path,
        parent_scope,
        false,
    )
    .is_err()
    {
        return Ok(false);
    }
    let Ok(step) = step_at(
        candidate,
        &scope.definition_id,
        parent_region_path,
        step_index,
    ) else {
        return Ok(false);
    };
    if step.id != site_id || !matches!(step.operation, Operation::Scope { .. }) {
        return Ok(false);
    }
    Ok(scope.scope_id
        == plan_scope_id(
            &run.run_id,
            plan_id,
            &scope.invocation_id,
            &scope.definition_id,
            &scope.region_path,
        )?)
}

fn direct_child_scope_on_path<'a>(
    run: &'a RunProjection,
    descendant_scope: &str,
    ancestor_scope: &str,
) -> Option<&'a crate::ScopeProjection> {
    let mut child = run.scopes.get(descendant_scope)?;
    loop {
        let parent = child.parent_scope.as_deref()?;
        if parent == ancestor_scope {
            return Some(child);
        }
        child = run.scopes.get(parent)?;
    }
}

#[derive(Clone, Copy)]
struct ExecutionLocation<'a> {
    candidate: &'a PlanCandidate,
    plan_id: &'a str,
    run: &'a RunProjection,
    invocation_id: &'a str,
    invocation_path: &'a [InvocationPathSegment],
    definition_id: &'a str,
    region_path: &'a [usize],
    scope_id: &'a str,
}

fn validate_execution_location(location: ExecutionLocation<'_>) -> Result<()> {
    let ExecutionLocation {
        candidate,
        plan_id,
        run,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        scope_id,
    } = location;
    let (resolved_definition, expected_invocation) =
        resolve_invocation(candidate, plan_id, run, invocation_path, true)?;
    if resolved_definition != definition_id || expected_invocation != invocation_id {
        return Err(CoreError::Validation(
            "execution location does not match its entry-rooted invocation path".to_owned(),
        ));
    }
    let scope = run
        .scopes
        .get(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
    validate_execution_scope_fields(
        scope,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        scope_id,
        true,
    )
}

fn resolve_invocation<'a>(
    candidate: &'a PlanCandidate,
    plan_id: &str,
    run: &RunProjection,
    path: &[InvocationPathSegment],
    require_open: bool,
) -> Result<(&'a str, String)> {
    resolve_plan_invocation(
        candidate,
        plan_id,
        &run.run_id,
        path,
        |invocation_id, prefix, definition_id, segment| {
            validate_execution_location_scope_only(
                run,
                invocation_id,
                prefix,
                definition_id,
                &segment.region_path,
                &segment.scope_id,
                require_open,
            )
        },
    )
}

fn resolve_plan_invocation<'a>(
    candidate: &'a PlanCandidate,
    plan_id: &str,
    run_id: &str,
    path: &[InvocationPathSegment],
    mut validate_scope: impl FnMut(
        &str,
        &[InvocationPathSegment],
        &str,
        &InvocationPathSegment,
    ) -> Result<()>,
) -> Result<(&'a str, String)> {
    let mut definition_id = candidate.entry.as_str();
    let mut invocation_id = plan_invocation_id(run_id, plan_id, &candidate.entry, &[])?;
    let mut prefix = Vec::new();
    for segment in path {
        validate_scope(&invocation_id, &prefix, definition_id, segment)?;
        let (step, _) = locate_step(
            candidate,
            definition_id,
            &segment.region_path,
            &segment.site_id,
        )?;
        let Operation::Invoke { definition, .. } = &step.operation else {
            return Err(CoreError::Validation(format!(
                "invocation path site {} is not an invoke operation",
                segment.site_id
            )));
        };
        definition_id = definition;
        prefix.push(segment.clone());
        invocation_id = plan_invocation_id(run_id, plan_id, &candidate.entry, &prefix)?;
    }
    Ok((definition_id, invocation_id))
}

fn validate_execution_location_scope_only(
    run: &RunProjection,
    invocation_id: &str,
    invocation_path: &[InvocationPathSegment],
    definition_id: &str,
    region_path: &[usize],
    scope_id: &str,
    require_open: bool,
) -> Result<()> {
    let scope = run
        .scopes
        .get(scope_id)
        .ok_or_else(|| CoreError::NotFound(format!("scope {scope_id} does not exist")))?;
    validate_execution_scope_fields(
        scope,
        invocation_id,
        invocation_path,
        definition_id,
        region_path,
        scope_id,
        require_open,
    )
}

fn validate_execution_scope_fields(
    scope: &crate::ScopeProjection,
    invocation_id: &str,
    invocation_path: &[InvocationPathSegment],
    definition_id: &str,
    region_path: &[usize],
    scope_id: &str,
    require_open: bool,
) -> Result<()> {
    if require_open && scope.status != crate::ScopeStatus::Open {
        return Err(CoreError::IllegalTransition(format!(
            "scope {scope_id} is not open"
        )));
    }
    let enclosing_scope = invocation_path
        .last()
        .map_or(ROOT_SCOPE_ID, |segment| segment.scope_id.as_str());
    if region_path.is_empty() {
        if scope_id != enclosing_scope {
            return Err(CoreError::Validation(
                "top-level invocation path left its lexical scope".to_owned(),
            ));
        }
    } else if scope.invocation_id != invocation_id
        || scope.definition_id != definition_id
        || scope.region_path != region_path
    {
        return Err(CoreError::Validation(
            "invocation path leaves its exact lexical scope".to_owned(),
        ));
    }
    Ok(())
}

fn validate_plan_execution_frame(
    plan: &SealedPlan,
    location: &ExecutionFrameLocation<'_>,
    scope: &crate::ScopeProjection,
) -> Result<()> {
    plan.verify()?;
    validate_identity("Run", location.run_id)?;
    if plan.plan_id != location.plan_id || scope.scope_id != location.scope_id {
        return Err(CoreError::IdentityMismatch(
            "execution frame changed its Plan or Scope".to_owned(),
        ));
    }
    let (definition, invocation) = resolve_plan_invocation(
        &plan.candidate,
        &plan.plan_id,
        location.run_id,
        location.invocation_path,
        |invocation_id, prefix, definition_id, segment| {
            let enclosing = if segment.region_path.is_empty() {
                prefix.last().map_or_else(
                    || ROOT_SCOPE_ID.to_owned(),
                    |parent| parent.scope_id.clone(),
                )
            } else {
                plan_scope_id(
                    location.run_id,
                    &plan.plan_id,
                    invocation_id,
                    definition_id,
                    &segment.region_path,
                )?
            };
            if segment.scope_id != enclosing {
                return Err(CoreError::Validation(
                    "invocation path leaves its structural Scope".to_owned(),
                ));
            }
            Ok(())
        },
    )?;
    if definition != location.definition_id || invocation != location.invocation_id {
        return Err(CoreError::IdentityMismatch(
            "execution frame changed its structural invocation".to_owned(),
        ));
    }
    validate_execution_scope_fields(
        scope,
        location.invocation_id,
        location.invocation_path,
        location.definition_id,
        location.region_path,
        location.scope_id,
        false,
    )?;
    let region = region_at_path(
        &plan.candidate,
        location.definition_id,
        location.region_path,
    )?;
    if location.next_step > region.steps.len() {
        return Err(CoreError::Validation(
            "execution frame next step exceeds its exact Region".to_owned(),
        ));
    }
    Ok(())
}

fn locate_step<'a>(
    candidate: &'a PlanCandidate,
    definition_id: &str,
    region_path: &[usize],
    site_id: &str,
) -> Result<(&'a crate::Step, usize)> {
    let region = region_at_path(candidate, definition_id, region_path)?;
    region
        .steps
        .iter()
        .enumerate()
        .find(|(_, step)| step.id == site_id)
        .map(|(index, step)| (step, index))
        .ok_or_else(|| {
            CoreError::NotFound(format!(
                "site {site_id} does not exist at the exact lexical Region path"
            ))
        })
}

fn step_at<'a>(
    candidate: &'a PlanCandidate,
    definition_id: &str,
    region_path: &[usize],
    step_index: usize,
) -> Result<&'a crate::Step> {
    region_at_path(candidate, definition_id, region_path)?
        .steps
        .get(step_index)
        .ok_or_else(|| CoreError::NotFound(format!("step index {step_index} does not exist")))
}

fn region_at_path<'a>(
    candidate: &'a PlanCandidate,
    definition_id: &str,
    path: &[usize],
) -> Result<&'a Region> {
    let definition = candidate
        .definitions
        .iter()
        .find(|definition| definition.id == definition_id)
        .ok_or_else(|| CoreError::NotFound(format!("definition {definition_id} does not exist")))?;
    let mut region = &definition.body;
    for step_index in path {
        let step = region.steps.get(*step_index).ok_or_else(|| {
            CoreError::NotFound(format!("Region path step {step_index} does not exist"))
        })?;
        let Operation::Scope { body, .. } = &step.operation else {
            return Err(CoreError::Validation(format!(
                "Region path step {step_index} is not a scope"
            )));
        };
        region = body;
    }
    Ok(region)
}

fn verify_command_event_closure(
    events: &[Event],
    commands: &BTreeMap<String, CommandRecord>,
) -> Result<()> {
    let retained_events: BTreeMap<&str, &Event> = events
        .iter()
        .map(|event| (event.event_id.as_str(), event))
        .collect();
    let mut receipt_by_event = BTreeMap::new();

    for (command_id, record) in commands {
        validate_envelope(&record.envelope)?;
        if record.receipt.command_id != *command_id
            || record.envelope.command_id != *command_id
            || record.receipt.observed_precondition != record.envelope.expected_precondition
        {
            return Err(CoreError::IdentityMismatch(format!(
                "command snapshot key {command_id} does not match its retained envelope or receipt"
            )));
        }
        verify_command_record(record)?;
        for event_id in &record.receipt.event_ids {
            if let Some(prior_command) = receipt_by_event.insert(event_id.clone(), command_id) {
                return Err(CoreError::IdentityMismatch(format!(
                    "event {event_id} is claimed by commands {prior_command} and {command_id}"
                )));
            }
            let event = retained_events.get(event_id.as_str()).ok_or_else(|| {
                CoreError::NotFound(format!(
                    "command {command_id} references missing event {event_id}"
                ))
            })?;
            if event.command_id != *command_id
                || event.command_hash != record.semantic_hash
                || event.run_id != record.envelope.run_id
            {
                return Err(CoreError::IdentityMismatch(format!(
                    "command {command_id} does not match retained event {event_id}"
                )));
            }
            verify_command_event_correspondence(record, event)?;
        }
        if canonical_digest(&record.envelope)? != record.semantic_hash {
            return Err(CoreError::IdentityMismatch(format!(
                "command {command_id} semantic hash does not match its retained envelope"
            )));
        }
    }

    for event in events {
        if !receipt_by_event.contains_key(&event.event_id) {
            return Err(CoreError::NotFound(format!(
                "event {} has no command receipt",
                event.event_id
            )));
        }
    }
    Ok(())
}

fn verify_command_event_correspondence(record: &CommandRecord, event: &Event) -> Result<()> {
    if !command_matches_event(&record.envelope.command, event)? {
        return Err(CoreError::IdentityMismatch(format!(
            "command {} does not match retained Event {}",
            record.envelope.command_id, event.event_id
        )));
    }
    Ok(())
}

fn command_matches_event(command: &Command, event: &Event) -> Result<bool> {
    match command {
        Command::StartRun { .. }
        | Command::BeginAttempt { .. }
        | Command::YieldAttempt { .. }
        | Command::AdvanceEpoch => Ok(command_matches_attempt_event(command, event)),
        Command::OpenScope { .. } | Command::CommitScope { .. } | Command::AbortScope { .. } => {
            command_matches_scope_event(command, event)
        }
        Command::ProposeEffect { .. } | Command::TransitionEffect { .. } => {
            Ok(command_matches_effect_event(command, event))
        }
        Command::UpdateBinding { .. }
        | Command::MigrateRun { .. }
        | Command::RecordFact { .. }
        | Command::CompleteRun { .. }
        | Command::FailRun { .. }
        | Command::CancelRun { .. } => Ok(command_matches_run_control_event(command, event)),
    }
}

fn command_matches_attempt_event(command: &Command, event: &Event) -> bool {
    match (command, &event.payload) {
        (
            Command::StartRun {
                plan_id,
                binding_context,
                input,
                ..
            },
            EventPayload::RunStarted {
                plan_id: event_plan,
                binding_context: event_binding,
                input: event_input,
                ..
            },
        ) => plan_id == event_plan && binding_context == event_binding && input == event_input,
        (
            Command::StartRun {
                initial_attempt, ..
            },
            EventPayload::AttemptStarted {
                attempt_id,
                continuation_id,
                occurrence_binding,
                continuation_epoch,
                execution_fence,
            },
        ) => {
            initial_attempt.attempt_id == *attempt_id
                && initial_attempt.continuation_id == *continuation_id
                && initial_attempt.occurrence_binding == *occurrence_binding
                && initial_attempt.continuation_epoch == *continuation_epoch
                && initial_attempt.execution_fence == *execution_fence
        }
        (
            Command::BeginAttempt {
                attempt_id,
                continuation_id,
                occurrence_binding,
                continuation_epoch,
                execution_fence,
            },
            EventPayload::AttemptStarted {
                attempt_id: event_attempt,
                continuation_id: event_continuation,
                occurrence_binding: event_binding,
                continuation_epoch: event_epoch,
                execution_fence: event_fence,
            },
        ) => {
            attempt_id == event_attempt
                && continuation_id == event_continuation
                && occurrence_binding == event_binding
                && continuation_epoch == event_epoch
                && execution_fence == event_fence
        }
        (
            Command::YieldAttempt {
                attempt_id,
                continuation_epoch,
                execution_fence,
            },
            EventPayload::AttemptYielded {
                attempt_id: event_attempt,
                continuation_epoch: event_epoch,
                execution_fence: event_fence,
            },
        ) => {
            attempt_id == event_attempt
                && continuation_epoch == event_epoch
                && execution_fence == event_fence
        }
        (Command::AdvanceEpoch, EventPayload::EpochAdvanced { .. }) => true,
        _ => false,
    }
}

fn command_matches_scope_event(command: &Command, event: &Event) -> Result<bool> {
    Ok(match (command, &event.payload) {
        (
            Command::OpenScope {
                scope_id,
                parent_scope,
                invocation_id,
                invocation_path,
                definition_id,
                region_path,
                site_id,
            },
            EventPayload::ScopeOpened {
                scope_id: event_scope,
                parent_scope: event_parent,
                invocation_id: event_invocation,
                invocation_path: event_invocation_path,
                definition_id: event_definition,
                region_path: event_region_path,
                site_id: event_site,
            },
        ) => {
            let expected_region_len = region_path.len().checked_add(1).ok_or_else(|| {
                CoreError::Validation("scope command Region path length overflowed".to_owned())
            })?;
            scope_id == event_scope
                && parent_scope == event_parent
                && invocation_id == event_invocation
                && invocation_path == event_invocation_path
                && definition_id == event_definition
                && event_region_path.len() == expected_region_len
                && event_region_path.starts_with(region_path)
                && site_id == event_site
        }
        (
            Command::CommitScope { scope_id },
            EventPayload::ScopeCommitted {
                scope_id: event_scope,
                ..
            },
        )
        | (
            Command::AbortScope { scope_id },
            EventPayload::ScopeAborted {
                scope_id: event_scope,
            },
        ) => scope_id == event_scope,
        _ => false,
    })
}

fn command_matches_effect_event(command: &Command, event: &Event) -> bool {
    match (command, &event.payload) {
        (
            Command::ProposeEffect {
                scope_id,
                invocation_id,
                invocation_path,
                definition_id,
                region_path,
                site_id,
                occurrence,
                operation,
                args,
                execution_binding,
                occurrence_binding,
            },
            EventPayload::EffectProposed {
                scope_id: event_scope,
                invocation_id: event_invocation,
                invocation_path: event_invocation_path,
                definition_id: event_definition,
                region_path: event_region_path,
                site_id: event_site,
                occurrence: event_occurrence,
                operation: event_operation,
                args: event_args,
                execution_binding: event_execution_binding,
                occurrence_binding: event_occurrence_binding,
                ..
            },
        ) => {
            scope_id == event_scope
                && invocation_id == event_invocation
                && invocation_path == event_invocation_path
                && definition_id == event_definition
                && region_path == event_region_path
                && site_id == event_site
                && occurrence == event_occurrence
                && operation == event_operation
                && args == event_args.as_ref()
                && execution_binding == event_execution_binding.as_ref()
                && occurrence_binding == event_occurrence_binding
        }
        (
            Command::TransitionEffect {
                intent_id,
                transition,
            },
            EventPayload::EffectTransitioned {
                intent_id: event_intent,
                transition: event_transition,
            },
        ) => intent_id == event_intent && transition == event_transition,
        _ => false,
    }
}

fn command_matches_run_control_event(command: &Command, event: &Event) -> bool {
    match (command, &event.payload) {
        (
            Command::UpdateBinding { binding_context },
            EventPayload::BindingUpdated { current, .. },
        ) => binding_context == current,
        (
            Command::MigrateRun {
                from_plan,
                to_plan,
                from_binding,
                to_binding,
                safe_point_id,
                target_epoch,
                target_continuation_digest,
            },
            EventPayload::RunMigrated {
                from_plan: event_from_plan,
                to_plan: event_to_plan,
                from_binding: event_from_binding,
                to_binding: event_to_binding,
                safe_point_id: event_safe_point,
                target_epoch: event_target_epoch,
                target_continuation_digest: event_target_continuation_digest,
            },
        ) => {
            from_plan == event_from_plan
                && to_plan == event_to_plan
                && from_binding == event_from_binding
                && to_binding == event_to_binding
                && safe_point_id == event_safe_point
                && target_epoch == event_target_epoch
                && target_continuation_digest == event_target_continuation_digest
        }
        (
            Command::RecordFact { key, value },
            EventPayload::FactRecorded {
                key: event_key,
                value: event_value,
            },
        ) => key == event_key && value == event_value,
        (
            Command::CompleteRun { result },
            EventPayload::RunCompleted {
                result: event_result,
            },
        ) => result == event_result,
        (
            Command::FailRun { failure },
            EventPayload::RunFailed {
                failure: event_failure,
                ..
            },
        ) => failure == event_failure,
        (
            Command::CancelRun { reason },
            EventPayload::RunCancelled {
                reason: event_reason,
                ..
            },
        ) => reason == event_reason,
        _ => false,
    }
}

fn machine_prefix_digest(
    archive_head: &str,
    archive_count: u64,
    archive_event_count: u64,
    admission_head: Option<&str>,
    command_index_root: &str,
    projection_digest: &str,
    projection_root: &str,
) -> Result<String> {
    content_id(
        MACHINE_PREFIX_VERSION,
        &MachinePrefixPreimage {
            prefix_version: MACHINE_PREFIX_VERSION,
            archive_head,
            archive_count,
            archive_event_count,
            admission_head,
            command_index_root,
            projection_digest,
            projection_root,
        },
    )
}

fn is_sha256_id(value: &str) -> bool {
    value.len() == "sha256:".len() + 64
        && value.starts_with("sha256:")
        && value["sha256:".len()..].bytes().all(is_lower_hex)
}

fn is_canonical_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(is_lower_hex)
}

fn verify_command_record(record: &CommandRecord) -> Result<()> {
    validate_envelope(&record.envelope)?;
    if canonical_digest(&record.envelope)? != record.semantic_hash
        || record.receipt.command_id != record.envelope.command_id
        || !is_sha256_id(&record.batch_id)
        || record.batch_len == 0
        || record.batch_position >= record.batch_len
        || record.receipt.observed_precondition != record.envelope.expected_precondition
        || match record.receipt.status {
            CommandReceiptStatus::Applied => {
                record.receipt.event_ids.len()
                    != usize::from(matches!(record.envelope.command, Command::StartRun { .. })) + 1
                    || record.receipt.event_ids.iter().any(|id| !is_sha256_id(id))
                    || record.receipt.error_code.is_some()
                    || record.receipt.message.is_some()
                    || record.receipt.current_precondition.is_none()
            }
            CommandReceiptStatus::Conflict => {
                !record.receipt.event_ids.is_empty()
                    || record.receipt.error_code.as_deref() != Some("stale_action")
                    || record.receipt.message.as_deref()
                        != Some("the Run changed after the caller's view")
                    || record.receipt.observed_precondition.is_none()
                    || record.receipt.observed_precondition == record.receipt.current_precondition
            }
        }
    {
        return Err(CoreError::IdentityMismatch(format!(
            "command {} has malformed canonical receipt evidence",
            record.envelope.command_id
        )));
    }
    Ok(())
}

fn verify_admission_record(admission: &CommandAdmission, record: &CommandRecord) -> Result<()> {
    verify_command_record(record)?;
    if admission.command_id != record.envelope.command_id
        || admission.semantic_hash != record.semantic_hash
        || admission.command_record_digest != canonical_digest(record)?
        || admission.status != record.receipt.status
        || admission.event_ids != record.receipt.event_ids
        || admission.batch_id != record.batch_id
        || admission.batch_position != record.batch_position
        || admission.batch_len != record.batch_len
    {
        return Err(CoreError::IdentityMismatch(format!(
            "CommandAdmission {} does not match command {}",
            admission.admission_id, admission.command_id
        )));
    }
    Ok(())
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

fn verify_event_footprint(event: &Event) -> Result<()> {
    let (reads, writes, coordination_key) = footprints(&event.run_id, &event.payload);
    if event.reads != reads || event.writes != writes || event.coordination_key != coordination_key
    {
        return Err(CoreError::IdentityMismatch(format!(
            "event {} does not match its semantic footprint",
            event.event_id
        )));
    }
    Ok(())
}

fn validate_migration_payload(
    from_plan: &str,
    to_plan: &str,
    from_binding: &str,
    to_binding: &str,
    safe_point_id: &str,
    target_epoch: u64,
    target_continuation_digest: &str,
) -> Result<()> {
    if from_plan == to_plan
        || !is_sha256_id(from_plan)
        || !is_sha256_id(to_plan)
        || !is_sha256_id(from_binding)
        || !is_sha256_id(to_binding)
        || !is_sha256_id(safe_point_id)
        || target_epoch == 0
        || target_epoch > crate::MAX_EXACT_INTEGER
        || !is_canonical_digest(target_continuation_digest)
    {
        return Err(CoreError::Validation(
            "Run migration requires distinct content-addressed Plans, bindings, and safe point"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_envelope(envelope: &CommandEnvelope) -> Result<()> {
    if envelope.command_version != COMMAND_VERSION {
        return Err(CoreError::Validation(format!(
            "unsupported command version {:?}",
            envelope.command_version
        )));
    }
    for (kind, value) in [
        ("command ID", envelope.command_id.as_str()),
        ("actor", envelope.actor.as_str()),
        ("Run ID", envelope.run_id.as_str()),
    ] {
        validate_identity(kind, value)?;
    }
    Ok(())
}

/// Validate one public semantic identity under the cross-profile contract.
///
/// Length is measured in Unicode scalar values rather than UTF-8 bytes so all
/// language bindings admit the same wire identity.
///
/// # Errors
///
/// Returns an error when the identity is empty, exceeds 512 Unicode scalar
/// values, or contains a control character.
pub fn validate_identity(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.chars().count() > 512 || value.chars().any(char::is_control) {
        return Err(CoreError::Validation(format!(
            "{kind} must contain 1..=512 printable Unicode scalar values"
        )));
    }
    Ok(())
}

fn validate_attempt_numbers(continuation_epoch: u64, execution_fence: u64) -> Result<()> {
    if continuation_epoch > crate::MAX_EXACT_INTEGER
        || execution_fence == 0
        || execution_fence > crate::MAX_EXACT_INTEGER
    {
        return Err(CoreError::Validation(
            "attempt epoch and execution fence must use the exact cross-language integer range"
                .to_owned(),
        ));
    }
    Ok(())
}

fn footprints(
    run_id: &str,
    payload: &EventPayload,
) -> (BTreeSet<String>, BTreeSet<String>, Option<String>) {
    let run_key = format!("run:{run_id}");
    let mut reads = BTreeSet::new();
    let mut writes = BTreeSet::new();
    let coordination_key = match payload {
        EventPayload::RunStarted { .. } => {
            writes.insert(run_key.clone());
            Some(run_key)
        }
        EventPayload::FactRecorded { key, .. } => {
            let key = format!("fact:{key}");
            reads.insert(key.clone());
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::EffectProposed {
            intent_id,
            scope_id,
            ..
        } => {
            reads.insert(run_key);
            let effect_key = format!("effect:{run_id}:{intent_id}");
            let scope_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(scope_key.clone());
            writes.insert(effect_key);
            writes.insert(scope_key.clone());
            writes.insert(tree_key.clone());
            Some(tree_key)
        }
        EventPayload::EffectTransitioned { intent_id, .. } => {
            reads.insert(run_key);
            let effect_key = format!("effect:{run_id}:{intent_id}");
            reads.insert(effect_key.clone());
            writes.insert(effect_key.clone());
            Some(effect_key)
        }
        EventPayload::ScopeOpened {
            scope_id,
            parent_scope,
            ..
        } => {
            reads.insert(run_key);
            let parent_key = format!("scope:{run_id}:{parent_scope}");
            let child_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(parent_key.clone());
            writes.insert(parent_key.clone());
            writes.insert(child_key);
            writes.insert(tree_key.clone());
            Some(tree_key)
        }
        EventPayload::ScopeCommitted { scope_id, .. } | EventPayload::ScopeAborted { scope_id } => {
            reads.insert(run_key);
            let scope_key = format!("scope:{run_id}:{scope_id}");
            let tree_key = format!("scope-tree:{run_id}");
            reads.insert(scope_key.clone());
            writes.insert(scope_key.clone());
            writes.insert(tree_key.clone());
            Some(tree_key)
        }
        EventPayload::AttemptStarted { attempt_id, .. }
        | EventPayload::AttemptYielded { attempt_id, .. } => {
            reads.insert(run_key);
            let key = format!("attempt:{run_id}:{attempt_id}");
            writes.insert(key.clone());
            Some(key)
        }
        EventPayload::EpochAdvanced { .. }
        | EventPayload::BindingUpdated { .. }
        | EventPayload::RunMigrated { .. }
        | EventPayload::RunCompleted { .. }
        | EventPayload::RunFailed { .. }
        | EventPayload::RunCancelled { .. } => {
            reads.insert(run_key.clone());
            writes.insert(run_key.clone());
            Some(run_key)
        }
    };
    (reads, writes, coordination_key)
}

#[cfg(test)]
mod restore_cost_tests {
    use super::*;
    use crate::{Definition, Expression, IR_VERSION, PlanCandidate, Region, seal_plan};

    fn candidate() -> PlanCandidate {
        PlanCandidate {
            ir_version: IR_VERSION.to_owned(),
            name: "restore_cost".to_owned(),
            entry: "main".to_owned(),
            components: Vec::new(),
            effects: Vec::new(),
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: serde_json::json!({}),
                output_schema: serde_json::json!({}),
                body: Region {
                    steps: Vec::new(),
                    result: Expression::Input,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }

    fn fact_value(label: &str) -> String {
        crate::content_id("cymule.test.restore-cost-fact/1", &label)
            .expect("restore-cost fact value derives")
    }

    fn put_run_input(machine: &mut Machine) -> ArtifactRef {
        machine
            .put_artifact(crate::RUN_INPUT_ARTIFACT_KIND, b"{}".to_vec())
            .expect("Run input stores")
    }

    fn placeholder_initial_attempt() -> crate::InitialAttemptSpec {
        crate::InitialAttemptSpec {
            attempt_id: content_id("cymule.test.initial-attempt/1", &"placeholder")
                .expect("Attempt derives"),
            continuation_id: content_id("cymule.test.initial-continuation/1", &"placeholder")
                .expect("Continuation derives"),
            occurrence_binding: content_id("cymule.test.initial-binding/1", &"placeholder")
                .expect("binding derives"),
            continuation_epoch: 0,
            execution_fence: 1,
        }
    }

    fn submit(
        machine: &mut Machine,
        archive_segments: &[MachineCommandArchiveSegment],
        sequence: u64,
        run_id: &str,
        mut command: Command,
    ) {
        let command_id = format!("command:restore-cost:{sequence}");
        if let Command::StartRun {
            plan_id,
            binding_context,
            input,
            material_digest,
            initial_attempt,
        } = &mut command
        {
            initial_attempt.attempt_id = content_id(
                "cymule.test.initial-attempt/1",
                &(run_id, command_id.as_str()),
            )
            .expect("Attempt derives");
            initial_attempt.continuation_id = content_id(
                "cymule.test.initial-continuation/1",
                &(run_id, command_id.as_str()),
            )
            .expect("Continuation derives");
            initial_attempt
                .occurrence_binding
                .clone_from(binding_context);
            let plan = machine.plan(plan_id).expect("Start Plan exists");
            let binding = machine
                .artifacts
                .get(binding_context)
                .or_else(|| machine.staged_artifacts.get(binding_context))
                .expect("Start binding exists");
            let input_record = machine.artifact(input).expect("Start input exists");
            let material = pinned::MachineMaterialAdmission::new(
                command_id.clone(),
                vec![plan.clone()],
                vec![binding.clone(), input_record.clone()],
            )
            .expect("Start material derives");
            *material_digest = material.material_digest().to_owned();
        }
        let expected_precondition = machine
            .projection()
            .runs
            .get(run_id)
            .map(RunProjection::precondition_token);
        let envelope = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id,
            actor: "actor:restore-cost".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition,
            command,
        };
        let receipt = if let Some(base) = &machine.base {
            let nodes = archive_segments
                .iter()
                .map(MachineCommandArchiveSegment::command_index_nodes)
                .collect::<Result<Vec<_>>>()
                .expect("archive nodes materialize")
                .into_iter()
                .flatten()
                .map(|node| (node.identity().expect("node verifies").to_owned(), node))
                .collect::<BTreeMap<_, _>>();
            let proof = resolve_machine_command_index_proof(
                &base.command_index_root,
                &envelope.command_id,
                |node_id| Ok(nodes.get(node_id).cloned()),
            )
            .expect("new command non-membership resolves");
            machine.submit_with_archive_lookup(
                envelope,
                MachineCommandArchiveLookup::NonMember { index_proof: proof },
            )
        } else {
            machine.submit(envelope)
        }
        .expect("restore-cost command admits");
        assert_eq!(receipt.status, CommandReceiptStatus::Applied);
    }

    #[test]
    fn empty_command_index_submit_hashes_at_most_one_sparse_path() {
        MachineCommandIndexProof::empty_root().expect("empty root initializes once");
        COMMAND_INDEX_NODE_HASH_COUNT.with(|count| count.set(0));
        let mut machine = Machine::new();
        let receipt = machine
            .submit(CommandEnvelope {
                command_version: COMMAND_VERSION.to_owned(),
                command_id: "command:index-hash-budget".to_owned(),
                actor: "actor:index-hash-budget".to_owned(),
                run_id: "run:index-hash-budget".to_owned(),
                expected_precondition: Some("pre:missing".to_owned()),
                command: Command::RecordFact {
                    key: "fact:index-hash-budget".to_owned(),
                    value: fact_value("not-applied"),
                },
            })
            .expect("missing Run produces one canonical Conflict admission");
        assert_eq!(receipt.status, CommandReceiptStatus::Conflict);
        assert_eq!(
            COMMAND_INDEX_NODE_HASH_COUNT.with(std::cell::Cell::get),
            0,
            "an explicit canonical empty-root proof must not hash a sparse path"
        );
        let proof = machine
            .command_index_proofs
            .get("command:index-hash-budget")
            .expect("hot command retains its proof");
        assert_eq!(proof.empty_depth, Some(0));
        assert!(proof.siblings.is_empty());
        assert!(
            crate::canonical_bytes(proof)
                .expect("compact proof serializes")
                .len()
                < 256,
            "an empty-root proof must not persist 256 redundant sibling digests"
        );

        let mut missing_depth = serde_json::to_value(proof).expect("proof serializes");
        missing_depth
            .as_object_mut()
            .expect("proof is an object")
            .remove("empty_depth");
        assert!(serde_json::from_value::<MachineCommandIndexProof>(missing_depth).is_err());
    }

    #[test]
    fn wide_compaction_builds_one_lightweight_event_authority() {
        let mut machine = Machine::new();
        let plan = seal_plan(candidate()).expect("Plan seals");
        machine.insert_plan(plan.clone()).expect("Plan inserts");
        let binding = machine
            .put_artifact(
                crate::EXECUTION_BINDING_ARTIFACT_KIND,
                b"wide-compaction".to_vec(),
            )
            .expect("binding stores");
        let input = put_run_input(&mut machine);
        let run_id = "run:wide-compaction";
        submit(
            &mut machine,
            &[],
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input,
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        );
        for sequence in 2_u64..=129 {
            submit(
                &mut machine,
                &[],
                sequence,
                run_id,
                Command::RecordFact {
                    key: format!("fact:wide-compaction:{sequence}"),
                    value: fact_value("wide-compaction-stable"),
                },
            );
        }
        assert_eq!(machine.events().count(), 130);

        COMPACTION_AUTHORITY_BUILD_COUNT.with(|count| count.set(0));
        let result = machine
            .compact_event_history(0)
            .expect("wide Event prefix compacts");
        assert_eq!(result.compacted_events, 130);
        assert_eq!(result.retained_events, 0);
        assert_eq!(
            COMPACTION_AUTHORITY_BUILD_COUNT.with(std::cell::Cell::get),
            1,
            "compaction must build one lightweight authority, independent of cut width"
        );
    }

    fn started_test_machine(run_id: &str, binding_material: Vec<u8>) -> Machine {
        let mut machine = Machine::new();
        start_test_run(&mut machine, run_id, binding_material);
        machine
    }

    fn start_test_run(machine: &mut Machine, run_id: &str, binding_material: Vec<u8>) {
        let plan = seal_plan(candidate()).expect("Plan seals");
        machine.insert_plan(plan.clone()).expect("Plan inserts");
        let binding = machine
            .put_artifact(crate::EXECUTION_BINDING_ARTIFACT_KIND, binding_material)
            .expect("binding stores");
        let input = put_run_input(machine);
        submit(
            machine,
            &[],
            1,
            run_id,
            Command::StartRun {
                plan_id: plan.plan_id,
                binding_context: binding.artifact_id,
                input,
                material_digest: String::new(),
                initial_attempt: placeholder_initial_attempt(),
            },
        );
    }

    fn test_material_artifact(label: &str) -> ArtifactRecord {
        let bytes = label.as_bytes().to_vec();
        ArtifactRecord {
            reference: artifact_ref(crate::EXECUTION_BINDING_ARTIFACT_KIND, &bytes)
                .expect("material Artifact identity derives"),
            bytes,
        }
    }

    fn reauthenticate_test_batch(batch: &mut MachineCommandBatchRecord) {
        batch.batch_id = machine_command_batch_id(
            &batch.parent_authority_root,
            &batch.members,
            batch.material_digest.as_deref(),
            batch.material_source.as_ref(),
            &batch.plan_ids,
            &batch.artifacts,
        )
        .expect("batch manifest identity derives");
        batch.batch_receipt_id = batch.expected_receipt_id().expect("batch receipt derives");
    }

    fn append_test_material_batch(
        machine: &mut Machine,
        source_command_id: &str,
        artifacts: Vec<ArtifactRecord>,
    ) -> MachineCommandBatchRecord {
        let material = pinned::MachineMaterialAdmission::new(
            source_command_id.to_owned(),
            Vec::new(),
            artifacts,
        )
        .expect("material proposal derives");
        let references = material
            .artifacts()
            .iter()
            .map(|artifact| artifact.reference.clone())
            .collect::<Vec<_>>();
        let parent = machine.authority_root().expect("parent derives");
        let mut batch = MachineCommandBatchRecord {
            batch_version: MACHINE_COMMAND_BATCH_VERSION.to_owned(),
            batch_id: String::new(),
            parent_authority_root: parent.clone(),
            admission_parent_authority_root: parent,
            members: Vec::new(),
            material_digest: Some(material.material_digest().to_owned()),
            material_source: Some(MachineCommandBatchMaterialSource {
                source_command_id: source_command_id.to_owned(),
                plan_ids: Vec::new(),
                artifacts: references.clone(),
            }),
            plan_ids: Vec::new(),
            artifacts: references,
            receipts: Vec::new(),
            event_ids: Vec::new(),
            result_authority_root: String::new(),
            batch_receipt_id: String::new(),
        };
        for artifact in material.artifacts() {
            machine
                .retain_artifact(artifact.clone())
                .expect("material retains");
        }
        reauthenticate_test_batch(&mut batch);
        let mut commitment = machine.authority.batches.clone();
        commitment
            .insert_with_undo(&batch.batch_id)
            .expect("batch commitment advances");
        batch.result_authority_root = machine
            .authority_root_with_batch(commitment.root(), machine.authority.batch_count + 1)
            .expect("material batch result derives");
        batch.batch_receipt_id = batch.expected_receipt_id().expect("batch receipt derives");
        machine
            .insert_batch(batch.clone())
            .expect("material batch admits");
        batch
    }

    #[derive(Clone, Copy)]
    enum TestCompactionCut {
        EventPrefix(usize),
        EventFree,
    }

    fn assert_test_compaction_round_trip(
        machine: &mut Machine,
        archives: &mut Vec<MachineCommandArchiveSegment>,
        cut: TestCompactionCut,
    ) {
        let before = machine.snapshot();
        let expected_root = machine.authority_root().expect("authority derives");
        let compacted = match cut {
            TestCompactionCut::EventPrefix(retain_suffix) => {
                machine.compact_event_history(retain_suffix)
            }
            TestCompactionCut::EventFree => machine.compact_event_free_admissions(),
        }
        .expect("history compacts through the ordinary archive");
        let after = machine.snapshot();
        assert_eq!(
            machine.authority_root().expect("authority derives"),
            expected_root
        );
        MachineDelta::between_compaction(&before, &after, &compacted.archive_segment)
            .expect("compaction delta preserves the complete batch cut");
        archives.push(compacted.archive_segment);
        let restored = Machine::restore_with_archive(after.clone(), archives.clone())
            .expect("independent archive chain restores");
        assert_eq!(restored.snapshot(), after);
        assert_eq!(
            restored.authority_root().expect("authority derives"),
            expected_root
        );
        let anchor = machine
            .base_anchor()
            .expect("anchor derives")
            .expect("base exists");
        let anchored = Machine::restore_anchored(after.clone(), &anchor)
            .expect("anchored hot suffix restores");
        assert_eq!(anchored.snapshot(), after);
        assert_eq!(
            anchored.authority_root().expect("authority derives"),
            expected_root
        );
    }

    #[test]
    fn zero_material_batches_survive_multiple_command_and_compaction_cuts() {
        let mut machine = Machine::new();
        append_test_material_batch(
            &mut machine,
            "material:before-start",
            vec![test_material_artifact("before-start")],
        );
        let initial = machine.snapshot();
        assert!(initial.events.is_empty());
        assert!(initial.admissions.is_empty());
        assert_eq!(
            Machine::restore(initial.clone())
                .expect("material restores")
                .snapshot(),
            initial
        );
        let run_id = "run:zero-material-history";
        start_test_run(&mut machine, run_id, b"zero-history-binding".to_vec());
        append_test_material_batch(
            &mut machine,
            "material:after-start",
            vec![test_material_artifact("after-start")],
        );
        let mut archives = Vec::new();
        submit(
            &mut machine,
            &archives,
            2,
            run_id,
            Command::RecordFact {
                key: "fact:zero-material:first".to_owned(),
                value: fact_value("first"),
            },
        );
        let snapshot = machine.snapshot();
        let replayed = Machine::replay(
            snapshot.plans,
            snapshot.artifacts,
            snapshot.batches.into_iter().rev(),
            machine
                .replay_entries()
                .expect("entries export")
                .into_iter()
                .rev(),
        )
        .expect("unordered batches follow exact authority parents");
        assert_eq!(replayed, machine.projection);
        assert_test_compaction_round_trip(
            &mut machine,
            &mut archives,
            TestCompactionCut::EventPrefix(1),
        );
        assert_eq!(archives[0].batches.len(), 2);
        assert_eq!(archives[0].entries.len(), 1);
        assert_eq!(
            machine.batches[&machine.batch_order[0]]
                .material_source
                .as_ref()
                .expect("retained material source")
                .source_command_id,
            "material:after-start",
            "the exact Event cut leaves later material authority in the hot suffix"
        );
        append_test_material_batch(
            &mut machine,
            "material:after-first-cut",
            vec![test_material_artifact("after-first-cut")],
        );
        submit(
            &mut machine,
            &archives,
            3,
            run_id,
            Command::RecordFact {
                key: "fact:zero-material:second".to_owned(),
                value: fact_value("second"),
            },
        );
        assert_test_compaction_round_trip(
            &mut machine,
            &mut archives,
            TestCompactionCut::EventPrefix(1),
        );
        assert_eq!(archives[1].batches.len(), 2);
        assert_test_compaction_round_trip(
            &mut machine,
            &mut archives,
            TestCompactionCut::EventPrefix(0),
        );
        assert_eq!(machine.authority.batch_count, 6);
        assert!(machine.batches.is_empty());
    }

    #[test]
    fn material_only_archives_preserve_the_chain_before_and_after_the_first_command() {
        let mut machine = Machine::new();
        assert!(machine.compact_event_free_admissions().is_err());
        let mut archives = Vec::new();
        for source in ["material:first", "material:second"] {
            append_test_material_batch(&mut machine, source, vec![test_material_artifact(source)]);
            assert_test_compaction_round_trip(
                &mut machine,
                &mut archives,
                TestCompactionCut::EventFree,
            );
            let segment = archives.last().expect("segment persists");
            assert_eq!(segment.header.entry_count, 0);
            assert_eq!(segment.header.event_count, 0);
            assert_eq!(segment.header.result_count, 0);
            assert_eq!(segment.header.result_admission_head, None);
            assert_eq!(segment.header.parent_admission_head, None);
            assert_eq!(
                segment.header.parent_command_index_root,
                segment.header.result_command_index_root
            );
            assert!(segment.entries.is_empty());
            assert!(segment.command_index_updates.is_empty());
            assert_eq!(
                segment.persistence_objects().expect("objects verify").len(),
                2
            );
        }
        assert!(archives[1].header.parent_segment.is_some());
        assert_eq!(archives[1].header.parent_count, 0);
        assert_eq!(machine.base.as_ref().expect("base exists").batch_count, 2);
        assert_eq!(
            machine
                .base_anchor()
                .expect("anchor derives")
                .expect("base exists")
                .admission_head,
            None
        );
        assert!(
            Machine::restore_with_archive(machine.snapshot(), vec![archives[1].clone()]).is_err()
        );
        start_test_run(
            &mut machine,
            "run:material-cold-genesis",
            b"material-cold-binding".to_vec(),
        );
        assert_eq!(machine.admissions[0].sequence, 1);
        assert_eq!(machine.admissions[0].parent_admission, None);
        append_test_material_batch(
            &mut machine,
            "material:after-start",
            vec![test_material_artifact("material:after-start")],
        );
        assert_test_compaction_round_trip(
            &mut machine,
            &mut archives,
            TestCompactionCut::EventPrefix(0),
        );
        let prior_head = machine
            .base
            .as_ref()
            .expect("base exists")
            .admission_head
            .clone();
        assert!(prior_head.is_some());
        append_test_material_batch(
            &mut machine,
            "material:after-command-cut",
            vec![test_material_artifact("material:after-command-cut")],
        );
        assert_test_compaction_round_trip(
            &mut machine,
            &mut archives,
            TestCompactionCut::EventFree,
        );
        let last = archives.last().expect("segment persists");
        assert_eq!(last.header.parent_count, 1);
        assert_eq!(last.header.result_count, 1);
        assert_eq!(last.header.entry_count, 0);
        assert_eq!(last.header.parent_admission_head, prior_head);
        assert_eq!(last.header.result_admission_head, prior_head);
        let mut wrong_head = last.header.clone();
        wrong_head.result_admission_head =
            Some(content_id("test.archive-head/1", &"wrong").expect("head derives"));
        wrong_head.segment_id = wrong_head.expected_id().expect("header reauthenticates");
        assert!(wrong_head.verify().is_err());
    }

    #[test]
    fn material_archive_heads_require_explicit_nullable_wire_fields() {
        let mut machine = Machine::new();
        append_test_material_batch(
            &mut machine,
            "material:nullable-head",
            vec![test_material_artifact("nullable-head")],
        );
        let compacted = machine
            .compact_event_free_admissions()
            .expect("material rotates");
        let base = machine.base.as_ref().expect("base exists");
        let anchor = machine
            .base_anchor()
            .expect("anchor derives")
            .expect("base exists");
        let header = &compacted.archive_segment.header;
        let mut header_wire = serde_json::to_value(header).expect("header serializes");
        let mut base_wire = serde_json::to_value(base.as_ref()).expect("base serializes");
        let mut anchor_wire = serde_json::to_value(&anchor).expect("anchor serializes");
        assert_eq!(
            header_wire["result_admission_head"],
            serde_json::Value::Null
        );
        assert_eq!(base_wire["admission_head"], serde_json::Value::Null);
        assert_eq!(anchor_wire["admission_head"], serde_json::Value::Null);
        header_wire
            .as_object_mut()
            .expect("header object")
            .remove("result_admission_head");
        base_wire
            .as_object_mut()
            .expect("base object")
            .remove("admission_head");
        anchor_wire
            .as_object_mut()
            .expect("anchor object")
            .remove("admission_head");
        assert!(serde_json::from_value::<MachineCommandArchiveSegmentHeader>(header_wire).is_err());
        assert!(serde_json::from_value::<MachineBaseSnapshot>(base_wire).is_err());
        assert!(serde_json::from_value::<MachineBaseAnchor>(anchor_wire).is_err());
        let mut forged = header.clone();
        forged.result_admission_head =
            Some(content_id("test.archive-head/1", &"invented").expect("head derives"));
        forged.segment_id = forged.expected_id().expect("header reauthenticates");
        assert!(forged.verify().is_err());
        let mut empty = compacted.archive_segment;
        empty.batches.clear();
        empty.header.batch_count = 0;
        empty.header.batches_root =
            content_id(MACHINE_COMMAND_ARCHIVE_BATCHES_DOMAIN, &empty.batches)
                .expect("empty batches hash");
        empty.header.segment_id = empty.header.expected_id().expect("empty header hashes");
        assert!(empty.verify().is_err());
    }

    #[test]
    fn anchor_binds_the_exact_cumulative_archive_batch_count() {
        let mut machine = Machine::new();
        append_test_material_batch(
            &mut machine,
            "material:anchor-batch-count",
            vec![test_material_artifact("anchor-batch-count")],
        );
        machine
            .compact_event_free_admissions()
            .expect("material rotates");
        let mut snapshot = machine.snapshot();
        let anchor = snapshot.base_anchor.as_ref().expect("anchor exists");
        assert_eq!(
            anchor.archive_batch_count,
            snapshot.base.as_ref().expect("base exists").batch_count
        );
        let mut omitted = serde_json::to_value(anchor).expect("anchor serializes");
        omitted
            .as_object_mut()
            .expect("object")
            .remove("archive_batch_count");
        assert!(serde_json::from_value::<MachineBaseAnchor>(omitted).is_err());
        let mut forged = anchor.clone();
        forged.archive_batch_count += 1;
        forged.anchor_id = forged.expected_id().expect("anchor identity recomputes");
        snapshot.base_anchor = Some(forged.clone());
        assert!(Machine::restore_anchored(snapshot, &forged).is_err());
    }

    #[test]
    fn reauthenticated_material_source_tampering_fails_before_batch_reduction() {
        enum SourceMutation {
            Owner,
            Reference,
            ExtraArtifact,
            Digest,
        }
        let original_artifact = test_material_artifact("source-original");
        let mut machine = Machine::new();
        let original = append_test_material_batch(
            &mut machine,
            "material:source-original",
            vec![original_artifact.clone()],
        );
        for mutation in [
            SourceMutation::Owner,
            SourceMutation::Reference,
            SourceMutation::ExtraArtifact,
            SourceMutation::Digest,
        ] {
            let mut batch = original.clone();
            let source = batch.material_source.as_mut().expect("source exists");
            let mut artifacts = vec![original_artifact.clone()];
            match mutation {
                SourceMutation::Owner => {
                    source.source_command_id = "material:other-owner".to_owned();
                }
                SourceMutation::Reference => {
                    source.artifacts[0].kind = "application/octet-stream".to_owned();
                    batch.artifacts.clone_from(&source.artifacts);
                }
                SourceMutation::ExtraArtifact => {
                    let extra = test_material_artifact("unbound-extra-source");
                    source.artifacts.push(extra.reference.clone());
                    source
                        .artifacts
                        .sort_by(|left, right| left.artifact_id.cmp(&right.artifact_id));
                    batch.artifacts.clone_from(&source.artifacts);
                    artifacts.push(extra);
                }
                SourceMutation::Digest => {
                    batch.material_digest = Some(
                        content_id("test.material-digest/1", &"wrong").expect("digest derives"),
                    );
                }
            }
            reauthenticate_test_batch(&mut batch);
            batch
                .verify()
                .expect("forgery is reauthenticated and shape-valid");
            let error = Machine::replay(Vec::new(), artifacts, [batch], Vec::new())
                .expect_err("full material-source verification rejects the forgery");
            assert!(
                matches!(error, CoreError::IdentityMismatch(ref message)
                if message == "batch material digest does not match its complete source"
                    || message.ends_with("changed reference")),
                "{error}"
            );
        }
    }

    #[test]
    fn reused_material_only_batch_advances_semantic_authority_without_new_material() {
        let mut machine =
            started_test_machine("run:material-frontier", b"frontier-binding".to_vec());
        let before = machine.snapshot();
        let parent_root = machine.authority_root().expect("parent authority derives");
        let existing = before.artifacts[0].clone();
        append_test_material_batch(&mut machine, "material:reuse", vec![existing]);
        let after = machine.snapshot();
        assert_eq!(before.plans, after.plans);
        assert_eq!(before.artifacts, after.artifacts);
        assert_eq!(before.events, after.events);
        assert_eq!(before.admissions, after.admissions);
        assert_eq!(after.batches.len(), before.batches.len() + 1);
        assert_ne!(
            machine.authority_root().expect("result authority derives"),
            parent_root
        );
        machine
            .verify_replay()
            .expect("material-only batch remains replayable");
    }

    fn assert_compaction_delta_rejected_transactionally(
        before: &MachineSnapshot,
        delta: &MachineDelta,
        segment: &MachineCommandArchiveSegment,
    ) {
        let mut machine = Machine::restore(before.clone()).expect("uncompacted parent restores");
        let mut snapshot = before.clone();
        assert!(machine.apply_compaction_delta(delta, segment).is_err());
        assert_eq!(machine.snapshot(), *before);
        assert!(snapshot.apply_compaction_delta(delta, segment).is_err());
        assert_eq!(snapshot, *before);
    }

    #[test]
    fn zero_entry_compaction_requires_the_exact_batch_prefix_and_frontier() {
        let material = test_material_artifact("shared-compaction-material");
        let mut machine = Machine::new();
        append_test_material_batch(
            &mut machine,
            "material:actual-owner",
            vec![material.clone()],
        );
        let before = machine.snapshot();
        let compacted = machine
            .compact_event_free_admissions()
            .expect("actual material compacts");
        let after = machine.snapshot();
        let delta = MachineDelta::between_compaction(&before, &after, &compacted.archive_segment)
            .expect("exact material-only cut derives");
        let mut foreign = Machine::new();
        append_test_material_batch(&mut foreign, "material:foreign-owner", vec![material]);
        let foreign_cut = foreign
            .compact_event_free_admissions()
            .expect("foreign material compacts");
        let error = MachineDelta::between_compaction(
            &before,
            &foreign.snapshot(),
            &foreign_cut.archive_segment,
        )
        .expect_err("zero Event/admission witnesses cannot replace another batch");
        assert!(
            matches!(error, CoreError::IdentityMismatch(ref message)
            if message == "Machine compaction batches do not match their complete hot prefix"),
            "{error}"
        );
        let mut missing_cut = delta.clone();
        missing_cut.compacted_batch_ids.clear();
        missing_cut
            .bind_local_authority()
            .expect("malformed local delta hashes");
        assert_compaction_delta_rejected_transactionally(
            &before,
            &missing_cut,
            &compacted.archive_segment,
        );
        let mut wrong_frontier = after.clone();
        let base = wrong_frontier.base.as_mut().expect("base exists");
        base.batch_count += 1;
        base.verify().expect("wrong count remains shape-valid");
        wrong_frontier.base_anchor =
            Some(MachineBaseAnchor::from_verified_base(base).expect("base reauthenticates"));
        let error =
            MachineDelta::between_compaction(&before, &wrong_frontier, &compacted.archive_segment)
                .expect_err("compaction does not synthesize another admitted batch");
        assert!(
            matches!(error, CoreError::IdentityMismatch(ref message)
            if message == "Machine compaction batch frontier does not match its archive cut"),
            "{error}"
        );
    }

    #[test]
    fn one_command_over_a_large_run_has_constant_authority_work() {
        let run_id = "run:large-authority";
        let mut machine = started_test_machine(run_id, b"large-run-authority".to_vec());
        let initial_attempt_id = machine.projection.runs[run_id]
            .derived
            .active_attempt
            .clone()
            .expect("StartRun owns its initial Attempt");
        let initial_attempt = machine.projection.runs[run_id].attempts[&initial_attempt_id].clone();
        submit(
            &mut machine,
            &[],
            2,
            run_id,
            Command::YieldAttempt {
                attempt_id: initial_attempt_id,
                continuation_epoch: initial_attempt.continuation_epoch,
                execution_fence: initial_attempt.execution_fence,
            },
        );
        for ordinal in 1_u64..=1_000 {
            let attempt_id = content_id("test.attempt/1", &(run_id, ordinal)).expect("ID hashes");
            let continuation_id =
                content_id("test.continuation/1", &(run_id, ordinal)).expect("ID hashes");
            let occurrence_binding =
                content_id("test.occurrence/1", &(run_id, ordinal)).expect("ID hashes");
            submit(
                &mut machine,
                &[],
                ordinal * 2 + 1,
                run_id,
                Command::BeginAttempt {
                    attempt_id: attempt_id.clone(),
                    continuation_id,
                    occurrence_binding,
                    continuation_epoch: 0,
                    execution_fence: ordinal,
                },
            );
            submit(
                &mut machine,
                &[],
                ordinal * 2 + 2,
                run_id,
                Command::YieldAttempt {
                    attempt_id,
                    continuation_epoch: 0,
                    execution_fence: ordinal,
                },
            );
        }
        let snapshot = machine.snapshot();
        let parent_root = machine.authority_root().expect("root derives");
        MACHINE_AUTHORITY_NODE_HASH_COUNT.with(|count| count.set(0));
        PROJECTION_ROOT_ADVANCE_COUNT.with(|count| count.set(0));
        submit(
            &mut machine,
            &[],
            3_000,
            run_id,
            Command::RecordFact {
                key: "fact:large-authority:one".to_owned(),
                value: fact_value("large-authority-stable"),
            },
        );
        assert_eq!(machine.events.len(), snapshot.events.len() + 1);
        assert_eq!(machine.admissions.len(), snapshot.admissions.len() + 1);
        assert_ne!(
            machine.authority_root().expect("result root derives"),
            parent_root
        );
        assert_eq!(
            MACHINE_AUTHORITY_NODE_HASH_COUNT.with(std::cell::Cell::get),
            0,
            "a command without new Plan/Artifact values hashes no sparse-map path"
        );
        assert_eq!(
            PROJECTION_ROOT_ADVANCE_COUNT.with(std::cell::Cell::get),
            1,
            "one command advances exactly one fixed-size projection root"
        );
        machine
            .verify_replay()
            .expect("single-command result replays");
    }

    #[test]
    fn archive_genesis_and_admission_sequence_fail_closed() {
        let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let batches_root = content_id("cymule.command-archive-batches/1", &vec![digest.to_owned()])
            .expect("batch fixture root derives");
        let mut header = MachineCommandArchiveSegmentHeader {
            segment_version: MachineCommandArchiveSegmentHeader::VERSION.to_owned(),
            segment_id: String::new(),
            parent_segment: None,
            parent_count: 0,
            parent_event_count: 1,
            parent_admission_head: None,
            parent_command_index_root: digest.to_owned(),
            entry_count: 2,
            event_count: 0,
            batch_count: 1,
            batches_root,
            entries_root: digest.to_owned(),
            result_count: 2,
            result_event_count: 1,
            result_admission_head: Some(digest.to_owned()),
            result_command_index_root: digest.to_owned(),
        };
        header.segment_id = header.expected_id().expect("malformed header still hashes");
        assert!(
            header.verify().is_err(),
            "genesis cannot inherit Event count"
        );

        let parent = CommandAdmissionParent {
            sequence: crate::MAX_EXACT_INTEGER,
            admission_id: digest,
        };
        assert!(
            next_command_admission_sequence(Some(parent)).is_err(),
            "admission sequence cannot saturate at the terminal exact integer"
        );
    }

    #[test]
    fn deserialized_machine_delta_has_no_local_mutation_authority() {
        let snapshot = Machine::new().snapshot();
        let root = snapshot_authority_root(&snapshot).expect("empty root derives");
        let mut delta = MachineDelta {
            local_authority: LocalMachineDeltaAuthority::default(),
            delta_version: MachineDelta::VERSION.to_owned(),
            parent_snapshot_digest: root.clone(),
            result_snapshot_digest: root,
            parent_anchor_id: None,
            result_anchor_id: None,
            plans: Vec::new(),
            artifacts: Vec::new(),
            batches: Vec::new(),
            compacted_event_ids: Vec::new(),
            compacted_admission_ids: Vec::new(),
            compacted_command_ids: BTreeSet::new(),
            compacted_batch_ids: BTreeSet::new(),
            compacted_command_index_proof_ids: BTreeSet::new(),
            base: None,
            base_anchor: None,
            archive_segment: None,
            events: Vec::new(),
            admissions: Vec::new(),
            commands: BTreeMap::new(),
            command_index_proofs: BTreeMap::new(),
        };
        delta.bind_local_authority().expect("local delta binds");
        let bytes = crate::canonical_bytes(&delta).expect("delta serializes");
        let deserialized: MachineDelta = crate::decode_json(&bytes).expect("delta decodes");
        assert!(deserialized.root_delta().is_err());
        assert!(snapshot.clone().apply_delta(&deserialized).is_err());
    }

    #[test]
    fn anchored_restore_reduces_only_the_admission_suffix_across_compactions() {
        let run_id = "run:restore-cost";
        let mut machine = started_test_machine(run_id, b"restore-cost".to_vec());
        let initial_compaction = machine
            .compact_event_history(0)
            .expect("Run start compacts");
        let mut archive_segments = vec![initial_compaction.archive_segment];

        let mut prior_full_count = 0_u64;
        for sequence in 2_u64..=18 {
            let anchor = machine
                .base_anchor()
                .expect("anchor derives")
                .expect("base exists");
            let snapshot = machine.snapshot();
            let (_, anchored_count) =
                Machine::restore_hot_internal(snapshot.clone(), Some(&anchor), &[])
                    .expect("anchor restores");
            let (_, full_count) = Machine::restore_hot_internal(snapshot, None, &archive_segments)
                .expect("full audit restores");
            assert_eq!(
                anchored_count, 0,
                "fully compacted base has no reducer suffix"
            );
            assert_eq!(full_count, sequence - 1);
            assert!(full_count > prior_full_count);
            prior_full_count = full_count;

            submit(
                &mut machine,
                &archive_segments,
                sequence,
                run_id,
                Command::RecordFact {
                    key: format!("fact:{sequence}"),
                    value: fact_value("anchored-restore-stable"),
                },
            );
            let compaction = machine
                .compact_event_history(0)
                .expect("one new Event compacts");
            archive_segments.push(compaction.archive_segment);
        }

        let anchor = machine
            .base_anchor()
            .expect("anchor derives")
            .expect("base exists");
        let stale = CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:restore-cost:conflict".to_owned(),
            actor: "actor:restore-cost".to_owned(),
            run_id: run_id.to_owned(),
            expected_precondition: Some("pre:0:stale".to_owned()),
            command: Command::RecordFact {
                key: "fact:conflict".to_owned(),
                value: fact_value("never-applied"),
            },
        };
        let nodes = archive_segments
            .iter()
            .flat_map(|segment| segment.command_index_nodes().expect("nodes materialize"))
            .map(|node| (node.identity().expect("node verifies").to_owned(), node))
            .collect::<BTreeMap<_, _>>();
        let stale_proof = resolve_machine_command_index_proof(
            &machine
                .base
                .as_ref()
                .expect("base exists")
                .command_index_root,
            &stale.command_id,
            |node_id| Ok(nodes.get(node_id).cloned()),
        )
        .expect("stale command non-membership resolves");
        assert_eq!(
            machine
                .submit_with_archive_lookup(
                    stale,
                    MachineCommandArchiveLookup::NonMember {
                        index_proof: stale_proof,
                    },
                )
                .expect("stale command records")
                .status,
            CommandReceiptStatus::Conflict
        );
        let (_, suffix_count) =
            Machine::restore_hot_internal(machine.snapshot(), Some(&anchor), &[])
                .expect("anchored Conflict suffix restores");
        assert_eq!(suffix_count, 1, "only the post-anchor Conflict reduces");
    }
}
