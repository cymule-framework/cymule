//! Adversarial conformance tests for the Resource-backed Virtual archive.

use std::collections::{BTreeMap, BTreeSet};

use cymule_core::{ArtifactRecord, artifact_ref, content_id};
use cymule_durable_protocol::{CLOCK_OBSERVATION_VERSION, ClockObservationRef};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, ResourceCatalogRecord, ResourceCatalogStore, ResourceChunk,
    ResourceCleanupReceipt, ResourceHandle, ResourceLocatorSet, ResourceObservation, ResourcePage,
    ResourcePublication, ResourceResult, ResourceWriteIntent, ResourceWriteSession,
};
use cymule_resource_fs::FsResourceStore;
use cymule_virtual::{
    ArchivedCommandIndex, ArchivedWorkIndex, FrontierLimits, ProtocolError, RegionSourceBinding,
    ResourceBackedVirtualArchive, SchedulingPolicy, VIRTUAL_ARCHIVE_MANIFEST_VERSION,
    VIRTUAL_INITIALIZATION_CONTROL_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchiveBinding,
    VirtualArchiveManifest, VirtualArchiveProvider, VirtualArchiveWorkIndexUpdate, VirtualCursor,
    VirtualInitializationCommand, VirtualKeyedSource, VirtualOperationAuthority,
    VirtualPersistenceCommand, VirtualPersistenceOperation, VirtualPersistenceReceipt,
    VirtualPreparationError, VirtualReductionAuthority, VirtualRegion, VirtualRunDefinition,
    VirtualRunExecution, VirtualStateFamily, VirtualStateRead, VirtualStateRoots, WorkOccurrence,
    WorkOccurrenceState, build_virtual_work_index_update, prepare_virtual,
    resolve_virtual_work_index_proof, virtual_command_index_empty_root,
    virtual_scheduler_journal_id, virtual_state_root_id, virtual_work_index_empty_root,
};

const ARCHIVE_OCCURRENCE_PROOF_CATALOG: &str = "cymule.virtual-archive-occurrence-proof/2";

#[test]
fn retired_virtual_contracts_have_no_current_decoder() {
    let fixture: serde_json::Value = cymule_core::decode_json(include_bytes!(
        "../../../tests/harness/fixtures/retired-virtual-contracts.json"
    ))
    .expect("retired fixture is strict JSON");
    assert_eq!(fixture["status"], "historical");
    let cases = fixture["cases"]
        .as_array()
        .expect("retired cases are an array");
    assert_eq!(cases.len(), 3);
    let mut names = BTreeSet::new();
    for case in cases {
        let name = case["name"].as_str().expect("retired case is named");
        assert!(names.insert(name));
        let value = case
            .get("value")
            .expect("retired payload is required")
            .clone();
        let rejected = match name {
            "virtual_checkpoint_v4" => {
                serde_json::from_value::<VirtualPersistenceCommand>(value).is_err()
            }
            "virtual_journal_base_v2" => {
                serde_json::from_value::<cymule_virtual::VirtualCurrent>(value).is_err()
            }
            "coupled_claim_receipt" => {
                serde_json::from_value::<cymule_virtual::VirtualClaimReceipt>(value).is_err()
            }
            _ => panic!("unknown retired fixture {name}"),
        };
        assert!(
            rejected,
            "retired {name} must never enter a current decoder"
        );
    }
}

struct CatalogSubstitution {
    namespace: String,
    key: String,
    payload: Vec<u8>,
}

struct FaultInjectingStore {
    inner: FsResourceStore,
    corrupt_reads: bool,
    catalog_substitution: Option<CatalogSubstitution>,
}

impl FaultInjectingStore {
    fn corrupt_reads(inner: FsResourceStore) -> Self {
        Self {
            inner,
            corrupt_reads: true,
            catalog_substitution: None,
        }
    }

    fn substitute_catalog(
        inner: FsResourceStore,
        namespace: impl Into<String>,
        key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            inner,
            corrupt_reads: false,
            catalog_substitution: Some(CatalogSubstitution {
                namespace: namespace.into(),
                key: key.into(),
                payload,
            }),
        }
    }
}

impl ArtifactStore for FaultInjectingStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> ResourceResult<ResourceWriteSession> {
        self.inner.begin_write(intent)
    }

    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        self.inner.write_chunk(session, offset, bytes)
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.inner.commit_write(session)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        self.inner.abort_write(session)
    }

    fn cleanup_receipt(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<Option<ResourceCleanupReceipt>> {
        self.inner.cleanup_receipt(session)
    }
}

impl ArtifactResolver for FaultInjectingStore {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
    ) -> ResourceResult<ResourceObservation> {
        self.inner.stat(resource, locators)
    }

    fn read(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> ResourceResult<ResourceChunk> {
        let mut chunk = self.inner.read(resource, locators, offset, max_bytes)?;
        if self.corrupt_reads
            && let Some(first) = chunk.bytes.first_mut()
        {
            *first ^= 1;
        }
        Ok(chunk)
    }

    fn list(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> ResourceResult<ResourcePage> {
        self.inner.list(resource, locators, cursor, limit)
    }
}

impl ResourceCatalogStore for FaultInjectingStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        self.inner.put_catalog_record(record)
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        if let Some(substitution) = &self.catalog_substitution
            && substitution.namespace == namespace
            && substitution.key == key
        {
            return ResourceCatalogRecord::new(namespace, key, substitution.payload.clone())
                .map(Some);
        }
        self.inner.get_catalog_record(namespace, key)
    }
}

fn terminal_occurrence() -> WorkOccurrence {
    let work_id = "work:archive-provider";
    let occurrence_id = content_id(VIRTUAL_WORK_OCCURRENCE_VERSION, &(work_id, 1_u64))
        .expect("occurrence identity derives");
    WorkOccurrence {
        occurrence_version: VIRTUAL_WORK_OCCURRENCE_VERSION.to_owned(),
        occurrence_id,
        work_id: work_id.to_owned(),
        region_id: "region:archive-provider".to_owned(),
        run_id: "run:archive-provider".to_owned(),
        owner: "worker:archive-provider".to_owned(),
        epoch: 1,
        lease_epoch: 1,
        lease_clock: ClockObservationRef {
            clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: content_id("test.clock-observation/1", &1_u64)
                .expect("clock observation identity derives"),
            source_id: "clock:archive-provider".to_owned(),
            source_generation: content_id("test.clock-generation/1", &())
                .expect("clock generation identity derives"),
            scope: "slot:archive-provider".to_owned(),
        },
        plan_id: content_id("test.virtual-plan/1", &"archive-provider")
            .expect("Plan identity derives"),
        execution_binding: artifact_ref("cymule.execution-binding/2", b"binding")
            .expect("ExecutionBinding reference derives"),
        state: WorkOccurrenceState::Succeeded,
        result: Some(
            artifact_ref("test.virtual-result/1", b"result").expect("result reference derives"),
        ),
        error: None,
        next_reason: None,
    }
}

fn archived_receipt() -> VirtualPersistenceReceipt {
    let bytes = b"archive source".to_vec();
    let source = ArtifactRecord {
        reference: artifact_ref("test.virtual-source/1", &bytes)
            .expect("source Artifact identity derives"),
        bytes,
    };
    let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Initialize(
        VirtualInitializationCommand {
            control_version: VIRTUAL_INITIALIZATION_CONTROL_VERSION.to_owned(),
            scheduler_id: "scheduler:archive-provider".to_owned(),
            command_id: "command:archive-provider".to_owned(),
            limits: FrontierLimits {
                max_materialized: 8,
                max_active: 4,
                max_active_per_run: 2,
                materialize_batch: 4,
            },
            scheduling_policy: SchedulingPolicy::default(),
            archive: VirtualArchiveBinding::new(
                "archive:resource-backed",
                "archive:resource-backed:revision:1",
            )
            .expect("archive binding seals"),
            regions: vec![VirtualRegion {
                region_id: "region:archive-provider".to_owned(),
                run_id: "run:archive-provider".to_owned(),
                source: RegionSourceBinding {
                    operation: "test.archive-source".to_owned(),
                    binding: "source:archive-provider".to_owned(),
                    revision: "source:archive-provider:revision:1".to_owned(),
                },
                source_artifact: source.reference.clone(),
                cursor: VirtualCursor {
                    version: "cursor:archive-provider".to_owned(),
                    position: "start".to_owned(),
                    exhausted: false,
                },
                estimated_total: None,
            }],
            runs: vec![VirtualRunDefinition {
                run_id: "run:archive-provider".to_owned(),
                execution: VirtualRunExecution::Direct {
                    plan_id: content_id("test.virtual-plan/1", &"archive-provider")
                        .expect("Plan identity derives"),
                },
            }],
            source_artifacts: vec![source],
        },
    ))
    .expect("initialization command seals");
    let mut reads = Vec::new();
    let reduction = loop {
        let source = VirtualKeyedSource::from_reads(command.scheduler_id(), None, reads.clone())
            .expect("genesis pinned source seals");
        let authority =
            VirtualReductionAuthority::new(source, VirtualOperationAuthority::Initialize);
        match prepare_virtual(&command, &authority) {
            Ok(reduction) => break reduction,
            Err(VirtualPreparationError::ReadRequired {
                family,
                storage_key,
            }) => reads.push(
                VirtualStateRead::new(family, storage_key, None)
                    .expect("genesis non-membership read seals"),
            ),
            Err(VirtualPreparationError::Protocol(error)) => {
                panic!("initialization preparation failed: {error}")
            }
        }
    };
    let node = content_id("test.virtual-map-node/1", &"archive-provider")
        .expect("StateRoot node identity derives");
    reduction
        .finish(VirtualStateRoots {
            regions: virtual_state_root_id(VirtualStateFamily::Regions, Some(&node), 1)
                .expect("region root seals"),
            active_regions: virtual_state_root_id(
                VirtualStateFamily::ActiveRegions,
                Some(&node),
                1,
            )
            .expect("active-region root seals"),
            parked: virtual_state_root_id(VirtualStateFamily::Parked, None, 0)
                .expect("parked root seals"),
            parked_index: virtual_state_root_id(VirtualStateFamily::ParkedIndex, None, 0)
                .expect("parked-index root seals"),
            work: virtual_state_root_id(VirtualStateFamily::Work, None, 0)
                .expect("work root seals"),
            occurrences: virtual_state_root_id(VirtualStateFamily::Occurrences, None, 0)
                .expect("occurrence root seals"),
            runs: virtual_state_root_id(VirtualStateFamily::Runs, Some(&node), 1)
                .expect("Run root seals"),
            migrations: virtual_state_root_id(VirtualStateFamily::Migrations, None, 0)
                .expect("migration root seals"),
            certificates: virtual_state_root_id(VirtualStateFamily::Certificates, None, 0)
                .expect("certificate root seals"),
        })
        .expect("initialization postcondition seals")
        .receipt
}

fn archive_manifest(
    occurrence: &WorkOccurrence,
) -> (
    VirtualArchiveManifest,
    ArchivedWorkIndex,
    VirtualArchiveWorkIndexUpdate,
) {
    let archived = ArchivedWorkIndex {
        work_id: occurrence.work_id.clone(),
        region_id: occurrence.region_id.clone(),
        run_id: occurrence.run_id.clone(),
        occurrence_id: occurrence.occurrence_id.clone(),
        max_epoch: occurrence.epoch,
        terminal_state: occurrence.state,
    };
    let parent_root = virtual_work_index_empty_root();
    let absence = resolve_virtual_work_index_proof(&parent_root, &archived.work_id, |_| Ok(None))
        .expect("empty cumulative index proves absence");
    let (update, _) = build_virtual_work_index_update(&parent_root, absence, &archived)
        .expect("work index insertion derives");
    let receipt = archived_receipt();
    let journal_id = virtual_scheduler_journal_id(receipt.command.scheduler_id())
        .expect("scheduler journal derives");
    let manifest = VirtualArchiveManifest {
        manifest_version: VIRTUAL_ARCHIVE_MANIFEST_VERSION.to_owned(),
        region_id: occurrence.region_id.clone(),
        run_id: occurrence.run_id.clone(),
        journal_id: Some(journal_id),
        source_causal_cut: BTreeSet::from(["cut:archive-provider".to_owned()]),
        occurrences: BTreeMap::from([(occurrence.occurrence_id.clone(), occurrence.clone())]),
        work_index: BTreeMap::from([(archived.work_id.clone(), archived.clone())]),
        parent_work_index_root_digest: parent_root,
        work_index_updates: vec![update.clone()],
        result_work_index_root_digest: update.result_root_digest.clone(),
        command_receipts: BTreeMap::from([(receipt.command.command_id().to_owned(), receipt)]),
    };
    manifest.verify().expect("archive manifest verifies");
    (manifest, archived, update)
}

#[test]
fn resource_archive_reopens_exact_object_and_both_cumulative_indexes() {
    let occurrence = terminal_occurrence();
    occurrence.verify().expect("terminal occurrence verifies");
    let (manifest, archived, expected_work_update) = archive_manifest(&occurrence);

    let directory = tempfile::tempdir().expect("temporary archive directory creates");
    let root = directory.path().join("provider");
    let store = FsResourceStore::open(&root, "fs:archive-provider").expect("Resource store opens");
    let mut archive = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("archive provider opens");

    let work_update = archive
        .insert_work_index(&manifest.parent_work_index_root_digest, &archived)
        .expect("work locator persists");
    assert_eq!(work_update, expected_work_update);
    let publication = archive
        .publish_archive(&manifest)
        .expect("archive object publishes with exact readback");
    let archived_receipt = manifest
        .command_receipts
        .get("command:archive-provider")
        .expect("archived receipt exists")
        .clone();
    let journal_id = manifest
        .journal_id
        .as_deref()
        .expect("archived receipt journal exists");
    let command = ArchivedCommandIndex {
        journal_id: journal_id.to_owned(),
        command_id: archived_receipt.command.command_id().to_owned(),
        certificate_id: content_id(
            "test.virtual-certificate/1",
            &publication.resource.resource_id,
        )
        .expect("certificate identity derives"),
        archive_resource_id: publication.resource.resource_id.clone(),
    };
    let command_update = archive
        .insert_command_index(&virtual_command_index_empty_root(), &command)
        .expect("command locator persists");
    drop(archive);

    let store =
        FsResourceStore::open(&root, "fs:archive-provider").expect("Resource store reopens");
    let mut reopened = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("archive provider reopens");
    let restored = reopened
        .rehydrate_occurrence(&publication.resource, &occurrence.occurrence_id)
        .expect("complete object and selected proof rehydrate");
    assert_eq!(restored.occurrence, occurrence);
    let retained_work = reopened
        .work_index_proof(&work_update.result_root_digest, &archived.work_id)
        .expect("work locator resolves after process loss");
    assert_eq!(retained_work.value.as_ref(), Some(&archived));
    let retained_command = reopened
        .command_index_proof(
            &command_update.result_root_digest,
            &command.journal_id,
            &command.command_id,
        )
        .expect("command locator resolves after process loss");
    assert_eq!(retained_command.value.as_ref(), Some(&command));
    let retained_receipt = reopened
        .archived_command(
            &publication.resource,
            &command.journal_id,
            &command.command_id,
        )
        .expect("one exact archived receipt range resolves after process loss");
    assert_eq!(retained_receipt.receipt, archived_receipt);
    let absent_work = reopened
        .work_index_proof(&work_update.result_root_digest, "work:absent")
        .expect("work non-membership resolves from one cumulative path");
    assert!(absent_work.value.is_none());
    let absent_command = reopened
        .command_index_proof(
            &command_update.result_root_digest,
            &command.journal_id,
            "command:absent",
        )
        .expect("command non-membership resolves from one cumulative path");
    assert!(absent_command.value.is_none());
}

#[test]
fn archive_provider_preserves_exact_not_found_boundaries() {
    let occurrence = terminal_occurrence();
    let (manifest, _, _) = archive_manifest(&occurrence);
    let published_directory = tempfile::tempdir().expect("published archive directory creates");
    let store = FsResourceStore::open(published_directory.path(), "fs:archive-provider")
        .expect("published Resource store opens");
    let mut archive = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("archive provider opens");
    let publication = archive
        .publish_archive(&manifest)
        .expect("archive object publishes");
    let absent_occurrence = content_id(
        VIRTUAL_WORK_OCCURRENCE_VERSION,
        &("work:archive-absent", 1_u64),
    )
    .expect("absent occurrence identity derives");
    assert!(matches!(
        archive.rehydrate_occurrence(&publication.resource, &absent_occurrence),
        Err(ProtocolError::NotFound { message })
            if message.contains("occurrence proof")
    ));
    let journal_id = manifest
        .journal_id
        .as_deref()
        .expect("archive journal exists");
    assert!(matches!(
        archive.archived_command(&publication.resource, journal_id, "command:archive-absent"),
        Err(ProtocolError::NotFound { message })
            if message.contains("command proof")
    ));

    let missing_directory = tempfile::tempdir().expect("missing archive directory creates");
    let store = FsResourceStore::open(missing_directory.path(), "fs:archive-provider")
        .expect("empty Resource store opens");
    let mut missing = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("empty archive provider opens");
    assert!(matches!(
        missing.rehydrate_occurrence(&publication.resource, &occurrence.occurrence_id),
        Err(ProtocolError::NotFound { message })
            if message.contains("publication")
    ));
}

#[test]
fn archive_provider_rejects_corrupted_complete_object() {
    let occurrence = terminal_occurrence();
    let (manifest, _, _) = archive_manifest(&occurrence);
    let directory = tempfile::tempdir().expect("temporary archive directory creates");
    let store = FsResourceStore::open(directory.path(), "fs:archive-provider")
        .expect("Resource store opens");
    let mut archive = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("archive provider opens");
    let publication = archive
        .publish_archive(&manifest)
        .expect("archive object publishes");
    let store = FaultInjectingStore::corrupt_reads(archive.into_inner());
    let mut corrupted = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("fault-injecting provider opens");

    let error = corrupted
        .rehydrate_occurrence(&publication.resource, &occurrence.occurrence_id)
        .expect_err("corrupted complete bytes must fail before a selected value is returned");
    assert!(matches!(
        error,
        ProtocolError::Integrity { code, .. }
            if code == "virtual_archive_digest_mismatch"
    ));
}

#[test]
fn archive_provider_rejects_descriptor_scoped_proof_mismatch() {
    let occurrence = terminal_occurrence();
    let (manifest, _, _) = archive_manifest(&occurrence);
    let directory = tempfile::tempdir().expect("temporary archive directory creates");
    let store = FsResourceStore::open(directory.path(), "fs:archive-provider")
        .expect("Resource store opens");
    let mut archive = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("archive provider opens");
    let publication = archive
        .publish_archive(&manifest)
        .expect("archive object publishes");
    let mut store = archive.into_inner();

    let retained_key = content_id(
        ARCHIVE_OCCURRENCE_PROOF_CATALOG,
        &(
            publication.resource.resource_id.as_str(),
            occurrence.occurrence_id.as_str(),
        ),
    )
    .expect("retained proof key derives");
    let retained = store
        .get_catalog_record(ARCHIVE_OCCURRENCE_PROOF_CATALOG, &retained_key)
        .expect("retained proof catalog reads")
        .expect("retained proof catalog exists");
    let absent_occurrence_id = content_id(
        VIRTUAL_WORK_OCCURRENCE_VERSION,
        &("work:absent", occurrence.epoch),
    )
    .expect("absent occurrence identity derives");
    let absent_key = content_id(
        ARCHIVE_OCCURRENCE_PROOF_CATALOG,
        &(
            publication.resource.resource_id.as_str(),
            absent_occurrence_id.as_str(),
        ),
    )
    .expect("absent proof key derives");
    let store = FaultInjectingStore::substitute_catalog(
        store,
        ARCHIVE_OCCURRENCE_PROOF_CATALOG,
        absent_key,
        retained.payload,
    );
    let mut mismatched = ResourceBackedVirtualArchive::open(
        store,
        "archive:resource-backed",
        "archive:resource-backed:revision:1",
    )
    .expect("fault-injecting provider opens");

    let error = mismatched
        .rehydrate_occurrence(&publication.resource, &absent_occurrence_id)
        .expect_err("descriptor-scoped proof identity mismatch must fail closed");
    assert!(matches!(
        error,
        ProtocolError::Integrity { code, .. }
            if code == "virtual_archive_occurrence_catalog_mismatch"
    ));
}

#[test]
fn archive_provider_rejects_mutable_or_oversized_selector_spellings() {
    let directory = tempfile::tempdir().expect("temporary archive directory creates");
    let store = FsResourceStore::open(directory.path(), "fs:archive-provider")
        .expect("Resource store opens");
    assert!(ResourceBackedVirtualArchive::open(store.clone(), "", "revision:1").is_err());
    assert!(ResourceBackedVirtualArchive::open(store.clone(), "archive:valid", "").is_err());
    assert!(
        ResourceBackedVirtualArchive::open(store.clone(), "archive:valid", "revision\nmutable")
            .is_err()
    );
    assert!(ResourceBackedVirtualArchive::open(store, "x".repeat(257), "revision:1").is_err());
}
