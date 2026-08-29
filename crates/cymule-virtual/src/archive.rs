//! Resource-backed implementation of the closed Virtual archive provider.

use std::collections::BTreeMap;

use cymule_core::{canonical_bytes, content_id, decode_json, sha256_bytes};
use cymule_profile_protocol::virtual_work::{
    ArchivedCommandIndex, ArchivedWorkIndex, MAX_VIRTUAL_ARCHIVE_BYTES,
    VIRTUAL_ARCHIVE_MANIFEST_KIND, VirtualArchiveBinding, VirtualArchiveCommandIndexNode,
    VirtualArchiveCommandIndexProof, VirtualArchiveCommandIndexUpdate, VirtualArchiveCommandProof,
    VirtualArchiveLayout, VirtualArchiveManifest, VirtualArchiveOccurrenceProof,
    VirtualArchiveProvider, VirtualArchiveWorkIndexNode, VirtualArchiveWorkIndexUpdate,
    VirtualArchiveWorkProof, VirtualArchivedCommand, VirtualRehydratedOccurrence,
    build_virtual_archive_layout, build_virtual_command_index_update,
    build_virtual_work_index_update, resolve_virtual_command_index_proof,
    resolve_virtual_work_index_proof,
};
use cymule_profile_protocol::{ProtocolError, ProtocolResult};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_READ_CHUNK, MAX_WRITE_CHUNK, RESOURCE_VERSION,
    ResourceCandidate, ResourceCatalogRecord, ResourceCatalogStore, ResourceError, ResourceHandle,
    ResourceIntegrity, ResourcePublication, ResourceShape, ResourceWriteIntent,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const ARCHIVE_PUBLICATION_CATALOG: &str = "cymule.virtual-archive-publication/2";
const ARCHIVE_OCCURRENCE_PROOF_CATALOG: &str = "cymule.virtual-archive-occurrence-proof/2";
const ARCHIVE_COMMAND_PROOF_CATALOG: &str = "cymule.virtual-archive-command-proof/2";
const ARCHIVE_WORK_INDEX_NODE_CATALOG: &str = "cymule.virtual-archive-work-index-node/2";
const ARCHIVE_COMMAND_INDEX_NODE_CATALOG: &str = "cymule.virtual-archive-command-index-node/2";
const ARCHIVE_WRITE_ID_DOMAIN: &str = "cymule.virtual-archive-write/2";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivePublicationCatalogEntry {
    resource_id: String,
    publication: ResourcePublication,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveOccurrenceProofCatalogEntry {
    resource_id: String,
    proof: VirtualArchiveOccurrenceProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveCommandProofCatalogEntry {
    resource_id: String,
    proof: VirtualArchiveCommandProof,
}

/// Exact Resource-backed archive generation used by Durable's provider registry.
pub struct ResourceBackedVirtualArchive<S> {
    store: S,
    archive: VirtualArchiveBinding,
}

impl<S> ResourceBackedVirtualArchive<S> {
    /// Open one immutable archive/index generation over a Resource adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when the binding or revision is not a valid immutable
    /// provider selector.
    pub fn open(
        store: S,
        binding: impl Into<String>,
        revision: impl Into<String>,
    ) -> ProtocolResult<Self> {
        Ok(Self {
            store,
            archive: VirtualArchiveBinding::new(binding, revision)?,
        })
    }

    /// Consume the provider and return its Resource adapter.
    pub fn into_inner(self) -> S {
        self.store
    }
}

impl<S> VirtualArchiveProvider for ResourceBackedVirtualArchive<S>
where
    S: ArtifactStore + ArtifactResolver + ResourceCatalogStore,
{
    fn archive_binding(&self) -> VirtualArchiveBinding {
        self.archive.clone()
    }

    fn work_index_proof(
        &mut self,
        root_digest: &str,
        work_id: &str,
    ) -> ProtocolResult<VirtualArchiveWorkProof> {
        resolve_virtual_work_index_proof(root_digest, work_id, |node_id| {
            load_work_index_node(&mut self.store, node_id)
        })
    }

    fn insert_work_index(
        &mut self,
        parent_root_digest: &str,
        value: &ArchivedWorkIndex,
    ) -> ProtocolResult<VirtualArchiveWorkIndexUpdate> {
        let proof = self.work_index_proof(parent_root_digest, &value.work_id)?;
        let (update, nodes) = build_virtual_work_index_update(parent_root_digest, proof, value)?;
        for node in nodes {
            retain_work_index_node(&mut self.store, &node)?;
        }
        let retained = self.work_index_proof(&update.result_root_digest, &value.work_id)?;
        if retained.value.as_ref() != Some(value) {
            return Err(archive_integrity(
                "virtual_archive_work_index_readback_mismatch",
                "Virtual archived-work readback changed the inserted terminal fence",
            ));
        }
        Ok(update)
    }

    fn command_index_proof(
        &mut self,
        root_digest: &str,
        journal_id: &str,
        command_id: &str,
    ) -> ProtocolResult<VirtualArchiveCommandIndexProof> {
        resolve_virtual_command_index_proof(root_digest, journal_id, command_id, |node_id| {
            load_command_index_node(&mut self.store, node_id)
        })
    }

    fn insert_command_index(
        &mut self,
        parent_root_digest: &str,
        value: &ArchivedCommandIndex,
    ) -> ProtocolResult<VirtualArchiveCommandIndexUpdate> {
        let proof =
            self.command_index_proof(parent_root_digest, &value.journal_id, &value.command_id)?;
        let (update, nodes) = build_virtual_command_index_update(parent_root_digest, proof, value)?;
        for node in nodes {
            retain_command_index_node(&mut self.store, &node)?;
        }
        let retained = self.command_index_proof(
            &update.result_root_digest,
            &value.journal_id,
            &value.command_id,
        )?;
        if retained.value.as_ref() != Some(value) {
            return Err(archive_integrity(
                "virtual_archive_command_index_readback_mismatch",
                "Virtual archived-command readback changed the inserted locator",
            ));
        }
        Ok(update)
    }

    fn publish_archive(
        &mut self,
        manifest: &VirtualArchiveManifest,
    ) -> ProtocolResult<ResourcePublication> {
        let layout = build_virtual_archive_layout(manifest)?;
        let descriptor = archive_descriptor(&layout.bytes)?;
        let publication = match load_publication(&mut self.store, &descriptor.resource_id)? {
            Some(publication) => {
                if publication.resource != descriptor {
                    return Err(archive_integrity(
                        "virtual_archive_resource_descriptor_mismatch",
                        "Virtual archive Resource ID was reused for another descriptor",
                    ));
                }
                publication
            }
            None => write_archive(&mut self.store, &descriptor, &layout.bytes)?,
        };

        let readback = read_complete_publication(&mut self.store, &publication)?;
        if readback != layout.bytes {
            return Err(archive_integrity(
                "virtual_archive_manifest_readback_mismatch",
                "Virtual archive readback changed the canonical manifest bytes",
            ));
        }
        retain_layout_catalog(&mut self.store, &descriptor.resource_id, &layout)?;
        retain_publication(&mut self.store, &publication)?;
        Ok(publication)
    }

    fn rehydrate_occurrence(
        &mut self,
        descriptor: &ResourceHandle,
        occurrence_id: &str,
    ) -> ProtocolResult<VirtualRehydratedOccurrence> {
        let (manifest, layout) = load_archive(&mut self.store, descriptor)?;
        let proof = load_occurrence_proof(&mut self.store, descriptor, occurrence_id)?;
        if layout.occurrence_proofs.get(occurrence_id) != Some(&proof) {
            return Err(archive_integrity(
                "virtual_archive_occurrence_proof_mismatch",
                "Virtual occurrence proof catalog changed the authenticated archive range",
            ));
        }
        let occurrence = manifest
            .occurrences
            .get(occurrence_id)
            .ok_or_else(|| ProtocolError::NotFound {
                message: format!("Virtual archive does not contain occurrence {occurrence_id}"),
            })?
            .clone();
        verify_exact_range(
            &layout.bytes,
            proof.offset,
            proof.length,
            &proof.digest,
            &occurrence,
        )?;
        Ok(VirtualRehydratedOccurrence { occurrence, proof })
    }

    fn archived_command(
        &mut self,
        descriptor: &ResourceHandle,
        journal_id: &str,
        command_id: &str,
    ) -> ProtocolResult<VirtualArchivedCommand> {
        let (manifest, layout) = load_archive(&mut self.store, descriptor)?;
        if manifest.journal_id.as_deref() != Some(journal_id) {
            return Err(archive_integrity(
                "virtual_archive_command_journal_mismatch",
                "Virtual archived-command lookup changed its owning journal",
            ));
        }
        let proof = load_command_proof(&mut self.store, descriptor, journal_id, command_id)?;
        if layout.command_proofs.get(command_id) != Some(&proof) {
            return Err(archive_integrity(
                "virtual_archive_command_proof_mismatch",
                "Virtual command proof catalog changed the authenticated archive range",
            ));
        }
        let receipt = manifest
            .command_receipts
            .get(command_id)
            .ok_or_else(|| ProtocolError::NotFound {
                message: format!("Virtual archive does not contain command {command_id}"),
            })?
            .clone();
        verify_exact_range(
            &layout.bytes,
            proof.offset,
            proof.length,
            &proof.digest,
            &receipt,
        )?;
        Ok(VirtualArchivedCommand { receipt, proof })
    }
}

fn archive_descriptor(bytes: &[u8]) -> ProtocolResult<ResourceHandle> {
    if bytes.is_empty() || bytes.len() > MAX_VIRTUAL_ARCHIVE_BYTES {
        return Err(ProtocolError::Validation(format!(
            "Virtual archive bytes must contain 1..={MAX_VIRTUAL_ARCHIVE_BYTES} bytes"
        )));
    }
    let size =
        u64::try_from(bytes.len()).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    ResourceCandidate {
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: VIRTUAL_ARCHIVE_MANIFEST_KIND.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: format!("sha256:{}", sha256_bytes(bytes)),
            size,
        },
        manifest: None,
        annotations: BTreeMap::new(),
    }
    .seal()
    .map_err(resource_error)
}

fn write_archive<S>(
    store: &mut S,
    descriptor: &ResourceHandle,
    bytes: &[u8],
) -> ProtocolResult<ResourcePublication>
where
    S: ArtifactStore,
{
    let intent = ResourceWriteIntent {
        write_id: content_id(ARCHIVE_WRITE_ID_DOMAIN, &descriptor.resource_id)?,
        shape: ResourceShape::Object,
        media_type: VIRTUAL_ARCHIVE_MANIFEST_KIND.to_owned(),
        annotations: BTreeMap::new(),
    };
    intent.validate().map_err(resource_error)?;
    let session = store.begin_write(&intent).map_err(resource_error)?;
    session.validate_for(&intent).map_err(resource_error)?;
    let mut offset = 0_u64;
    for chunk in bytes.chunks(MAX_WRITE_CHUNK) {
        store
            .write_chunk(&session, offset, chunk)
            .map_err(resource_error)?;
        let written = u64::try_from(chunk.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        offset = offset.checked_add(written).ok_or_else(|| {
            ProtocolError::Validation("Virtual archive write offset overflowed".to_owned())
        })?;
    }
    let publication = store.commit_write(&session).map_err(resource_error)?;
    publication.verify().map_err(resource_error)?;
    if publication.resource != *descriptor {
        return Err(archive_integrity(
            "virtual_archive_write_descriptor_mismatch",
            "Resource adapter changed the framework-derived Virtual archive descriptor",
        ));
    }
    Ok(publication)
}

fn load_archive<S>(
    store: &mut S,
    descriptor: &ResourceHandle,
) -> ProtocolResult<(VirtualArchiveManifest, VirtualArchiveLayout)>
where
    S: ArtifactResolver + ResourceCatalogStore,
{
    descriptor.verify().map_err(resource_error)?;
    let publication = load_publication(store, &descriptor.resource_id)?.ok_or_else(|| {
        ProtocolError::NotFound {
            message: format!(
                "Virtual archive publication {} is missing",
                descriptor.resource_id
            ),
        }
    })?;
    if publication.resource != *descriptor {
        return Err(archive_integrity(
            "virtual_archive_publication_descriptor_mismatch",
            "Virtual archive publication changed its semantic descriptor",
        ));
    }
    let bytes = read_complete_publication(store, &publication)?;
    let manifest: VirtualArchiveManifest = decode_json(&bytes)?;
    let layout = build_virtual_archive_layout(&manifest)?;
    if layout.bytes != bytes || archive_descriptor(&bytes)? != *descriptor {
        return Err(archive_integrity(
            "virtual_archive_object_descriptor_mismatch",
            "Virtual archive bytes do not close their exact semantic descriptor",
        ));
    }
    Ok((manifest, layout))
}

fn read_complete_publication<S>(
    store: &mut S,
    publication: &ResourcePublication,
) -> ProtocolResult<Vec<u8>>
where
    S: ArtifactResolver,
{
    publication.verify().map_err(resource_error)?;
    if publication.resource.shape != ResourceShape::Object
        || publication.resource.media_type != VIRTUAL_ARCHIVE_MANIFEST_KIND
    {
        return Err(ProtocolError::Validation(
            "Virtual archive publication has the wrong Resource shape or media type".to_owned(),
        ));
    }
    let ResourceIntegrity::Content {
        digest: expected_digest,
        size: expected_size,
    } = &publication.resource.integrity
    else {
        return Err(ProtocolError::Validation(
            "Virtual archives require content-addressed Resource integrity".to_owned(),
        ));
    };
    if *expected_size == 0
        || *expected_size
            > u64::try_from(MAX_VIRTUAL_ARCHIVE_BYTES)
                .map_err(|error| ProtocolError::Validation(error.to_string()))?
    {
        return Err(ProtocolError::Validation(
            "Virtual archive Resource size exceeds the bounded object contract".to_owned(),
        ));
    }
    let observation = store
        .stat(&publication.resource, &publication.locators)
        .map_err(resource_error)?;
    if observation.media_type != publication.resource.media_type
        || observation.integrity != publication.resource.integrity
    {
        return Err(archive_integrity(
            "virtual_archive_observation_mismatch",
            "Virtual archive provider observation changed retained integrity",
        ));
    }
    let capacity = usize::try_from(*expected_size)
        .map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut offset = 0_u64;
    let mut terminal = false;
    while offset < *expected_size {
        let remaining = expected_size - offset;
        let limit = u32::try_from(remaining.min(u64::from(MAX_READ_CHUNK)))
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        let chunk = store
            .read(&publication.resource, &publication.locators, offset, limit)
            .map_err(resource_error)?;
        if chunk.offset != offset
            || chunk.bytes.is_empty()
            || chunk.bytes.len() > limit as usize
            || terminal
        {
            return Err(archive_integrity(
                "virtual_archive_range_shape_mismatch",
                "Virtual archive provider returned an invalid bounded byte range",
            ));
        }
        let read = u64::try_from(chunk.bytes.len())
            .map_err(|error| ProtocolError::Validation(error.to_string()))?;
        offset = offset.checked_add(read).ok_or_else(|| {
            ProtocolError::Validation("Virtual archive read offset overflowed".to_owned())
        })?;
        if offset > *expected_size || (chunk.eof && offset != *expected_size) {
            return Err(archive_integrity(
                "virtual_archive_length_mismatch",
                "Virtual archive provider changed the retained byte length",
            ));
        }
        terminal = chunk.eof;
        bytes.extend_from_slice(&chunk.bytes);
    }
    if !terminal || format!("sha256:{}", sha256_bytes(&bytes)) != *expected_digest {
        return Err(archive_integrity(
            "virtual_archive_digest_mismatch",
            "Virtual archive provider changed the complete content-addressed object",
        ));
    }
    Ok(bytes)
}

fn retain_layout_catalog<S>(
    store: &mut S,
    resource_id: &str,
    layout: &VirtualArchiveLayout,
) -> ProtocolResult<()>
where
    S: ResourceCatalogStore,
{
    for proof in layout.occurrence_proofs.values() {
        let key = occurrence_proof_key(resource_id, &proof.occurrence_id)?;
        put_catalog(
            store,
            ARCHIVE_OCCURRENCE_PROOF_CATALOG,
            &key,
            &ArchiveOccurrenceProofCatalogEntry {
                resource_id: resource_id.to_owned(),
                proof: proof.clone(),
            },
        )?;
    }
    for proof in layout.command_proofs.values() {
        let key = command_proof_key(resource_id, &proof.journal_id, &proof.command_id)?;
        put_catalog(
            store,
            ARCHIVE_COMMAND_PROOF_CATALOG,
            &key,
            &ArchiveCommandProofCatalogEntry {
                resource_id: resource_id.to_owned(),
                proof: proof.clone(),
            },
        )?;
    }
    Ok(())
}

fn retain_publication<S>(store: &mut S, publication: &ResourcePublication) -> ProtocolResult<()>
where
    S: ResourceCatalogStore,
{
    put_catalog(
        store,
        ARCHIVE_PUBLICATION_CATALOG,
        &publication.resource.resource_id,
        &ArchivePublicationCatalogEntry {
            resource_id: publication.resource.resource_id.clone(),
            publication: publication.clone(),
        },
    )
}

fn load_publication<S>(
    store: &mut S,
    resource_id: &str,
) -> ProtocolResult<Option<ResourcePublication>>
where
    S: ResourceCatalogStore,
{
    let Some(entry) = load_catalog::<ArchivePublicationCatalogEntry, _>(
        store,
        ARCHIVE_PUBLICATION_CATALOG,
        resource_id,
    )?
    else {
        return Ok(None);
    };
    entry.publication.verify().map_err(resource_error)?;
    if entry.resource_id != resource_id || entry.publication.resource.resource_id != resource_id {
        return Err(archive_integrity(
            "virtual_archive_publication_catalog_mismatch",
            "Virtual archive publication catalog changed identity",
        ));
    }
    Ok(Some(entry.publication))
}

fn load_occurrence_proof<S>(
    store: &mut S,
    descriptor: &ResourceHandle,
    occurrence_id: &str,
) -> ProtocolResult<VirtualArchiveOccurrenceProof>
where
    S: ResourceCatalogStore,
{
    let key = occurrence_proof_key(&descriptor.resource_id, occurrence_id)?;
    let entry = load_catalog::<ArchiveOccurrenceProofCatalogEntry, _>(
        store,
        ARCHIVE_OCCURRENCE_PROOF_CATALOG,
        &key,
    )?
    .ok_or_else(|| ProtocolError::NotFound {
        message: format!("Virtual archive occurrence proof {occurrence_id} is missing"),
    })?;
    if entry.resource_id != descriptor.resource_id || entry.proof.occurrence_id != occurrence_id {
        return Err(archive_integrity(
            "virtual_archive_occurrence_catalog_mismatch",
            "Virtual archive occurrence proof catalog changed identity",
        ));
    }
    Ok(entry.proof)
}

fn load_command_proof<S>(
    store: &mut S,
    descriptor: &ResourceHandle,
    journal_id: &str,
    command_id: &str,
) -> ProtocolResult<VirtualArchiveCommandProof>
where
    S: ResourceCatalogStore,
{
    let key = command_proof_key(&descriptor.resource_id, journal_id, command_id)?;
    let entry = load_catalog::<ArchiveCommandProofCatalogEntry, _>(
        store,
        ARCHIVE_COMMAND_PROOF_CATALOG,
        &key,
    )?
    .ok_or_else(|| ProtocolError::NotFound {
        message: format!("Virtual archive command proof {command_id} is missing"),
    })?;
    if entry.resource_id != descriptor.resource_id
        || entry.proof.journal_id != journal_id
        || entry.proof.command_id != command_id
    {
        return Err(archive_integrity(
            "virtual_archive_command_catalog_mismatch",
            "Virtual archive command proof catalog changed identity",
        ));
    }
    Ok(entry.proof)
}

fn retain_work_index_node<S>(
    store: &mut S,
    node: &VirtualArchiveWorkIndexNode,
) -> ProtocolResult<()>
where
    S: ResourceCatalogStore,
{
    let node_id = node.identity()?.to_owned();
    if let Some(existing) = load_work_index_node(store, &node_id)? {
        if existing != *node {
            return Err(archive_integrity(
                "virtual_archive_work_node_content_mismatch",
                format!("Virtual archived-work node {node_id} has conflicting content"),
            ));
        }
        return Ok(());
    }
    put_catalog(store, ARCHIVE_WORK_INDEX_NODE_CATALOG, &node_id, node)
}

fn load_work_index_node<S>(
    store: &mut S,
    node_id: &str,
) -> ProtocolResult<Option<VirtualArchiveWorkIndexNode>>
where
    S: ResourceCatalogStore,
{
    let node = load_catalog::<VirtualArchiveWorkIndexNode, _>(
        store,
        ARCHIVE_WORK_INDEX_NODE_CATALOG,
        node_id,
    )?;
    if let Some(node) = &node
        && node.identity()? != node_id
    {
        return Err(archive_integrity(
            "virtual_archive_work_node_identity_mismatch",
            "Virtual archived-work catalog changed node identity",
        ));
    }
    Ok(node)
}

fn retain_command_index_node<S>(
    store: &mut S,
    node: &VirtualArchiveCommandIndexNode,
) -> ProtocolResult<()>
where
    S: ResourceCatalogStore,
{
    let node_id = node.identity()?.to_owned();
    if let Some(existing) = load_command_index_node(store, &node_id)? {
        if existing != *node {
            return Err(archive_integrity(
                "virtual_archive_command_node_content_mismatch",
                format!("Virtual archived-command node {node_id} has conflicting content"),
            ));
        }
        return Ok(());
    }
    put_catalog(store, ARCHIVE_COMMAND_INDEX_NODE_CATALOG, &node_id, node)
}

fn load_command_index_node<S>(
    store: &mut S,
    node_id: &str,
) -> ProtocolResult<Option<VirtualArchiveCommandIndexNode>>
where
    S: ResourceCatalogStore,
{
    let node = load_catalog::<VirtualArchiveCommandIndexNode, _>(
        store,
        ARCHIVE_COMMAND_INDEX_NODE_CATALOG,
        node_id,
    )?;
    if let Some(node) = &node
        && node.identity()? != node_id
    {
        return Err(archive_integrity(
            "virtual_archive_command_node_identity_mismatch",
            "Virtual archived-command catalog changed node identity",
        ));
    }
    Ok(node)
}

fn put_catalog<T, S>(store: &mut S, namespace: &str, key: &str, value: &T) -> ProtocolResult<()>
where
    T: Serialize,
    S: ResourceCatalogStore,
{
    let payload = canonical_bytes(value)?;
    let record = ResourceCatalogRecord::new(namespace, key, payload).map_err(resource_error)?;
    store.put_catalog_record(&record).map_err(resource_error)
}

fn load_catalog<T, S>(store: &mut S, namespace: &str, key: &str) -> ProtocolResult<Option<T>>
where
    T: DeserializeOwned,
    S: ResourceCatalogStore,
{
    let Some(record) = store
        .get_catalog_record(namespace, key)
        .map_err(resource_error)?
    else {
        return Ok(None);
    };
    record.verify().map_err(resource_error)?;
    if record.namespace != namespace || record.key != key {
        return Err(archive_integrity(
            "virtual_archive_catalog_key_mismatch",
            "Resource catalog returned a record for another exact key",
        ));
    }
    decode_json(&record.payload)
        .map(Some)
        .map_err(ProtocolError::from)
}

fn occurrence_proof_key(resource_id: &str, occurrence_id: &str) -> ProtocolResult<String> {
    content_id(
        ARCHIVE_OCCURRENCE_PROOF_CATALOG,
        &(resource_id, occurrence_id),
    )
    .map_err(ProtocolError::from)
}

fn command_proof_key(
    resource_id: &str,
    journal_id: &str,
    command_id: &str,
) -> ProtocolResult<String> {
    content_id(
        ARCHIVE_COMMAND_PROOF_CATALOG,
        &(resource_id, journal_id, command_id),
    )
    .map_err(ProtocolError::from)
}

fn verify_exact_range<T>(
    bytes: &[u8],
    offset: u64,
    length: u64,
    digest: &str,
    value: &T,
) -> ProtocolResult<()>
where
    T: Serialize,
{
    let start =
        usize::try_from(offset).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let length =
        usize::try_from(length).map_err(|error| ProtocolError::Validation(error.to_string()))?;
    let end = start.checked_add(length).ok_or_else(|| {
        ProtocolError::Validation("Virtual archive proof range overflowed".to_owned())
    })?;
    let range = bytes.get(start..end).ok_or_else(|| {
        archive_integrity(
            "virtual_archive_proof_range_out_of_bounds",
            "Virtual archive proof range exceeds the immutable object",
        )
    })?;
    if range.is_empty()
        || format!("sha256:{}", sha256_bytes(range)) != digest
        || canonical_bytes(value)? != range
    {
        return Err(archive_integrity(
            "virtual_archive_proof_value_mismatch",
            "Virtual archive proof range changed its exact canonical value",
        ));
    }
    Ok(())
}

fn archive_integrity(code: &str, message: impl Into<String>) -> ProtocolError {
    ProtocolError::Integrity {
        code: code.to_owned(),
        message: message.into(),
    }
}

fn resource_error(error: ResourceError) -> ProtocolError {
    match error {
        ResourceError::Validation(message) => ProtocolError::Validation(message),
        ResourceError::Schema(issue) => ProtocolError::Validation(format!(
            "Resource contract {} rejected instance {} at schema {}",
            issue.contract_id, issue.instance_path, issue.schema_path
        )),
        ResourceError::Conflict { code, message } => ProtocolError::Conflict { code, message },
        ResourceError::NotFound(message) => ProtocolError::NotFound { message },
        ResourceError::Substrate { code, message } => ProtocolError::Substrate { code, message },
        ResourceError::Persistence { code, message } => {
            ProtocolError::Persistence { code, message }
        }
        ResourceError::CommitOutcomeUnknown { message } => {
            ProtocolError::CommitOutcomeUnknown { message }
        }
        ResourceError::Integrity { code, message } => ProtocolError::Integrity { code, message },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_error_mapping_preserves_stable_failure_codes() {
        assert_eq!(
            resource_error(ResourceError::Integrity {
                code: "archive_digest_mismatch".to_owned(),
                message: "wrong bytes".to_owned(),
            }),
            ProtocolError::Integrity {
                code: "archive_digest_mismatch".to_owned(),
                message: "wrong bytes".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::Conflict {
                code: "archive_generation_conflict".to_owned(),
                message: "wrong revision".to_owned(),
            }),
            ProtocolError::Conflict {
                code: "archive_generation_conflict".to_owned(),
                message: "wrong revision".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::Substrate {
                code: "archive_store_unavailable".to_owned(),
                message: "offline".to_owned(),
            }),
            ProtocolError::Substrate {
                code: "archive_store_unavailable".to_owned(),
                message: "offline".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::Persistence {
                code: "archive_catalog_write_failed".to_owned(),
                message: "catalog failed".to_owned(),
            }),
            ProtocolError::Persistence {
                code: "archive_catalog_write_failed".to_owned(),
                message: "catalog failed".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::NotFound("archive absent".to_owned())),
            ProtocolError::NotFound {
                message: "archive absent".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::CommitOutcomeUnknown {
                message: "receipt lost".to_owned(),
            }),
            ProtocolError::CommitOutcomeUnknown {
                message: "receipt lost".to_owned(),
            }
        );
        assert_eq!(
            resource_error(ResourceError::Validation("bad descriptor".to_owned())),
            ProtocolError::Validation("bad descriptor".to_owned())
        );
        let schema = cymule_resource::ResourceSchemaIssue {
            contract_id: "contract:test".to_owned(),
            instance_path: "/value".to_owned(),
            schema_path: "/type".to_owned(),
        };
        assert_eq!(
            resource_error(ResourceError::Schema(schema)),
            ProtocolError::Validation(
                "Resource contract contract:test rejected instance /value at schema /type"
                    .to_owned()
            )
        );
    }
}
