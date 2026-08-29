//! Public archive retries across Resource publication and catalog boundaries.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use cymule_core::{artifact_ref, content_id};
use cymule_durable_protocol::{CLOCK_OBSERVATION_VERSION, ClockObservationRef};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, ResourceCatalogRecord, ResourceCatalogStore, ResourceChunk,
    ResourceCleanupReceipt, ResourceError, ResourceHandle, ResourceLocatorSet, ResourceObservation,
    ResourcePage, ResourcePublication, ResourceResult, ResourceWriteIntent, ResourceWriteSession,
};
use cymule_resource_fs::FsResourceStore;
use cymule_virtual::{
    ArchivedWorkIndex, ProtocolError, ResourceBackedVirtualArchive,
    VIRTUAL_ARCHIVE_MANIFEST_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchiveManifest,
    VirtualArchiveProvider, WorkOccurrence, WorkOccurrenceState, build_virtual_work_index_update,
    resolve_virtual_work_index_proof, virtual_work_index_empty_root,
};

const PUBLICATION_CATALOG: &str = "cymule.virtual-archive-publication/2";
const STORE_BINDING: &str = "fs:archive-publication-replay";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    PublishingBeforeContent,
    CommitAcknowledgement,
    BeforePublicationCatalog,
    PublicationCatalogAcknowledgement,
}

struct InterruptedStore {
    inner: FsResourceStore,
    objects: PathBuf,
    failure: Option<FailurePoint>,
    session: Option<ResourceWriteSession>,
    begin_calls: usize,
    chunk_calls: usize,
    commit_calls: usize,
}

impl InterruptedStore {
    fn open(root: &Path, failure: Option<FailurePoint>) -> Self {
        Self {
            inner: FsResourceStore::open(root, STORE_BINDING).expect("Resource store opens"),
            objects: root
                .join("objects")
                .join(cymule_core::sha256_bytes(STORE_BINDING.as_bytes())),
            failure,
            session: None,
            begin_calls: 0,
            chunk_calls: 0,
            commit_calls: 0,
        }
    }

    fn take_failure(&mut self, point: FailurePoint) -> bool {
        if self.failure == Some(point) {
            self.failure = None;
            true
        } else {
            false
        }
    }
}

fn lost_acknowledgement() -> ResourceError {
    ResourceError::Substrate {
        code: "test_archive_acknowledgement_lost".to_owned(),
        message: "publication boundary completed before acknowledgement loss".to_owned(),
    }
}

impl ArtifactStore for InterruptedStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> ResourceResult<ResourceWriteSession> {
        self.begin_calls += 1;
        let session = self.inner.begin_write(intent)?;
        self.session = Some(session.clone());
        Ok(session)
    }

    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        self.chunk_calls += 1;
        self.inner.write_chunk(session, offset, bytes)
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        self.commit_calls += 1;
        if self.take_failure(FailurePoint::PublishingBeforeContent) {
            let permissions = fs::metadata(&self.objects)
                .expect("objects metadata reads")
                .permissions();
            fs::set_permissions(&self.objects, fs::Permissions::from_mode(0o500))
                .expect("test suspends content-directory writes");
            let result = self.inner.commit_write(session);
            fs::set_permissions(&self.objects, permissions)
                .expect("test restores its content directory");
            assert!(matches!(result, Err(ResourceError::Substrate { .. })));
            return result;
        }
        let publication = self.inner.commit_write(session)?;
        if self.take_failure(FailurePoint::CommitAcknowledgement) {
            return Err(lost_acknowledgement());
        }
        Ok(publication)
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

impl ArtifactResolver for InterruptedStore {
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
        self.inner.read(resource, locators, offset, max_bytes)
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

impl ResourceCatalogStore for InterruptedStore {
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()> {
        if record.namespace == PUBLICATION_CATALOG
            && self.take_failure(FailurePoint::BeforePublicationCatalog)
        {
            return Err(lost_acknowledgement());
        }
        self.inner.put_catalog_record(record)?;
        if record.namespace == PUBLICATION_CATALOG
            && self.take_failure(FailurePoint::PublicationCatalogAcknowledgement)
        {
            return Err(lost_acknowledgement());
        }
        Ok(())
    }

    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>> {
        self.inner.get_catalog_record(namespace, key)
    }
}

fn manifest() -> VirtualArchiveManifest {
    let occurrence = WorkOccurrence {
        occurrence_version: VIRTUAL_WORK_OCCURRENCE_VERSION.to_owned(),
        occurrence_id: content_id(VIRTUAL_WORK_OCCURRENCE_VERSION, &("work:replay", 1_u64))
            .unwrap(),
        work_id: "work:replay".to_owned(),
        region_id: "region:replay".to_owned(),
        run_id: "run:replay".to_owned(),
        owner: "worker:replay".to_owned(),
        epoch: 1,
        lease_epoch: 1,
        lease_clock: ClockObservationRef {
            clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: content_id("test.clock-observation/1", &1_u64).unwrap(),
            source_id: "clock:replay".to_owned(),
            source_generation: content_id("test.clock-generation/1", &()).unwrap(),
            scope: "slot:replay".to_owned(),
        },
        plan_id: content_id("test.virtual-plan/1", &"replay").unwrap(),
        execution_binding: artifact_ref("cymule.execution-binding/2", b"binding").unwrap(),
        state: WorkOccurrenceState::Succeeded,
        result: Some(artifact_ref("test.virtual-result/1", b"result").unwrap()),
        error: None,
        next_reason: None,
    };
    let work = ArchivedWorkIndex {
        work_id: occurrence.work_id.clone(),
        region_id: occurrence.region_id.clone(),
        run_id: occurrence.run_id.clone(),
        occurrence_id: occurrence.occurrence_id.clone(),
        max_epoch: occurrence.epoch,
        terminal_state: occurrence.state,
    };
    let root = virtual_work_index_empty_root();
    let proof = resolve_virtual_work_index_proof(&root, &work.work_id, |_| Ok(None)).unwrap();
    let (update, _) = build_virtual_work_index_update(&root, proof, &work).unwrap();
    let manifest = VirtualArchiveManifest {
        manifest_version: VIRTUAL_ARCHIVE_MANIFEST_VERSION.to_owned(),
        region_id: occurrence.region_id.clone(),
        run_id: occurrence.run_id.clone(),
        journal_id: None,
        source_causal_cut: BTreeSet::from(["cut:replay".to_owned()]),
        occurrences: BTreeMap::from([(occurrence.occurrence_id.clone(), occurrence)]),
        work_index: BTreeMap::from([(work.work_id.clone(), work)]),
        parent_work_index_root_digest: root,
        result_work_index_root_digest: update.result_root_digest.clone(),
        work_index_updates: vec![update],
        command_receipts: BTreeMap::new(),
    };
    manifest.verify().expect("typed archive manifest verifies");
    manifest
}

fn open_archive(store: InterruptedStore) -> ResourceBackedVirtualArchive<InterruptedStore> {
    ResourceBackedVirtualArchive::open(store, "archive:replay", "archive:replay:1")
        .expect("real archive provider opens")
}

#[test]
fn public_archive_reopens_across_resource_publication_and_catalog_acknowledgement_windows() {
    for failure in [
        FailurePoint::PublishingBeforeContent,
        FailurePoint::CommitAcknowledgement,
        FailurePoint::BeforePublicationCatalog,
        FailurePoint::PublicationCatalogAcknowledgement,
    ] {
        let directory = tempfile::tempdir().expect("temporary archive directory creates");
        let root = directory.path().join("store");
        let manifest = manifest();
        let mut archive = open_archive(InterruptedStore::open(&root, Some(failure)));
        assert!(matches!(
            archive.publish_archive(&manifest),
            Err(ProtocolError::Substrate { .. })
        ));
        let mut interrupted = archive.into_inner();
        assert!(
            interrupted.failure.is_none(),
            "the requested boundary must execute"
        );
        let session = interrupted.session.as_ref().unwrap().clone();
        let record_path = root.join("uploads").join(format!(
            "{}.json",
            session.upload_id.strip_prefix("upload:").unwrap()
        ));
        let record: serde_json::Value = cymule_core::decode_json(&fs::read(&record_path).unwrap())
            .expect("durable upload record decodes");
        let expected_phase = if failure == FailurePoint::PublishingBeforeContent {
            "publishing"
        } else {
            "committed"
        };
        assert_eq!(record["state"], expected_phase);
        let original_receipt = interrupted.cleanup_receipt(&session).unwrap();
        assert_eq!(original_receipt.is_none(), expected_phase == "publishing");
        drop(interrupted);

        let mut reopened = open_archive(InterruptedStore::open(&root, None));
        let publication = reopened
            .publish_archive(&manifest)
            .expect("the original public begin/chunk/commit flow converges after reopen");
        let occurrence = manifest.occurrences.values().next().unwrap();
        assert_eq!(
            reopened
                .rehydrate_occurrence(&publication.resource, &occurrence.occurrence_id)
                .expect("exact archived occurrence rehydrates")
                .occurrence,
            *occurrence
        );
        let mut completed = reopened.into_inner();
        let replayed_write =
            usize::from(failure != FailurePoint::PublicationCatalogAcknowledgement);
        assert_eq!(completed.begin_calls, replayed_write);
        assert_eq!(completed.chunk_calls, replayed_write);
        assert_eq!(completed.commit_calls, replayed_write);
        let receipt = completed.cleanup_receipt(&session).unwrap().unwrap();
        if let Some(original) = original_receipt {
            assert_eq!(receipt, original);
        }
        let catalog = completed
            .get_catalog_record(PUBLICATION_CATALOG, &publication.resource.resource_id)
            .unwrap()
            .expect("publication catalog exists");
        let record_bytes = fs::read(&record_path).unwrap();
        drop(completed);

        let mut replay = open_archive(InterruptedStore::open(&root, None));
        assert_eq!(replay.publish_archive(&manifest).unwrap(), publication);
        let mut replay = replay.into_inner();
        assert_eq!(
            (replay.begin_calls, replay.chunk_calls, replay.commit_calls),
            (0, 0, 0)
        );
        assert_eq!(replay.cleanup_receipt(&session).unwrap(), Some(receipt));
        assert_eq!(
            replay
                .get_catalog_record(PUBLICATION_CATALOG, &publication.resource.resource_id)
                .unwrap(),
            Some(catalog)
        );
        assert_eq!(fs::read(record_path).unwrap(), record_bytes);
    }
}
