//! Real Resource-backed archive and phase-local upload replay conformance.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use cymule_core::{artifact_ref, content_id};
use cymule_durable_protocol::{CLOCK_OBSERVATION_VERSION, ClockObservationRef};
use cymule_virtual::{
    ArchivedWorkIndex, ProtocolError, ResourceBackedVirtualArchive,
    VIRTUAL_ARCHIVE_MANIFEST_VERSION, VIRTUAL_WORK_OCCURRENCE_VERSION, VirtualArchiveManifest,
    VirtualArchiveProvider, WorkOccurrence, WorkOccurrenceState, build_virtual_work_index_update,
    resolve_virtual_work_index_proof, virtual_work_index_empty_root,
};
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
    PutPayload, PutResult,
};

use super::*;

const PUBLICATION_CATALOG: &str = "cymule.virtual-archive-publication/2";
const PREFIX: &str = "publication-replay";
const BINDING: &str = "object:publication-replay";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    PublishingHeadAcknowledgement,
    FirstContentPartAcknowledgement,
    CommittedHeadAcknowledgement,
    BeforePublicationCatalog,
    PublicationCatalogAcknowledgement,
}

#[derive(Debug)]
struct PublicationFaultStore {
    inner: InMemory,
    fault: Mutex<Option<FailurePoint>>,
    upload: Mutex<Option<ResourceWriteSession>>,
    puts: AtomicUsize,
    upload_creates: AtomicUsize,
    gets: AtomicUsize,
    part_bytes: AtomicU64,
    upload_content_reads: AtomicUsize,
    delete_before: AtomicBool,
    put_pause: Mutex<Option<HeadPause>>,
    head_pause: Mutex<Option<HeadPause>>,
    inventory_pause: Mutex<Option<HeadPause>>,
}

#[derive(Debug)]
struct HeadPause {
    target: Path,
    reached: SyncSender<()>,
    resume: Receiver<()>,
}

impl PublicationFaultStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fault: Mutex::new(None),
            upload: Mutex::new(None),
            puts: AtomicUsize::new(0),
            upload_creates: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            part_bytes: AtomicU64::new(0),
            upload_content_reads: AtomicUsize::new(0),
            delete_before: AtomicBool::new(false),
            put_pause: Mutex::new(None),
            head_pause: Mutex::new(None),
            inventory_pause: Mutex::new(None),
        }
    }

    fn arm(&self, failure: FailurePoint) {
        *self.fault.lock().unwrap() = Some(failure);
    }

    fn take_failure(&self, location: &Path, payload: &PutPayload) -> Option<FailurePoint> {
        let path = location.as_ref();
        let state = if path.contains("/uploads/") && path.ends_with("/record.json") {
            let bytes = payload
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect::<Vec<_>>();
            let record: UploadRecord = cymule_core::decode_json(&bytes).unwrap();
            *self.upload.lock().unwrap() = Some(ResourceWriteSession {
                write_id: record.intent.write_id,
                upload_id: record.upload_id,
                store_binding: record.store_binding,
            });
            Some(record.state)
        } else {
            None
        };
        let publication_catalog = if path.contains("/catalog/") {
            let bytes = payload
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect::<Vec<_>>();
            let record: ResourceCatalogRecord = cymule_core::decode_json(&bytes).unwrap();
            record.namespace == PUBLICATION_CATALOG
        } else {
            false
        };
        let mut fault = self.fault.lock().unwrap();
        let matches = match *fault {
            Some(FailurePoint::PublishingHeadAcknowledgement) => {
                state == Some(UploadState::Publishing)
            }
            Some(FailurePoint::FirstContentPartAcknowledgement) => {
                path.ends_with("/parts/00000000000000000000")
            }
            Some(FailurePoint::CommittedHeadAcknowledgement) => {
                state == Some(UploadState::Committed)
            }
            Some(
                FailurePoint::BeforePublicationCatalog
                | FailurePoint::PublicationCatalogAcknowledgement,
            ) => publication_catalog,
            None => false,
        };
        if matches { fault.take() } else { None }
    }
}

impl std::fmt::Display for PublicationFaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("publication-fault-test-store")
    }
}

fn lost_acknowledgement() -> ObjectError {
    ObjectError::Generic {
        store: "publication-fault-test-store",
        source: std::io::Error::other("injected publication acknowledgement loss").into(),
    }
}

#[async_trait::async_trait]
impl ObjectStore for PublicationFaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let pause = {
            let mut pause = self.put_pause.lock().unwrap();
            if pause
                .as_ref()
                .is_some_and(|pause| pause.target == *location)
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause
                .reached
                .send(())
                .expect("PUT boundary observer remains connected");
            pause
                .resume
                .recv_timeout(Duration::from_secs(10))
                .expect("exact PUT boundary resumes");
        }
        if location.as_ref().contains("/uploads/") && matches!(options.mode, PutMode::Create) {
            self.upload_creates.fetch_add(1, Ordering::SeqCst);
        }
        let failure = self.take_failure(location, &payload);
        if failure == Some(FailurePoint::BeforePublicationCatalog) {
            return Err(lost_acknowledgement());
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        if failure.is_some() {
            return Err(lost_acknowledgement());
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let pause = {
            let mut pause = self.head_pause.lock().unwrap();
            if options.head
                && pause
                    .as_ref()
                    .is_some_and(|pause| pause.target == *location)
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause
                .reached
                .send(())
                .expect("HEAD boundary observer remains connected");
            pause
                .resume
                .recv_timeout(Duration::from_secs(10))
                .expect("exact HEAD boundary resumes");
        }
        let body = !options.head;
        let result = self.inner.get_opts(location, options).await?;
        if body && location.as_ref().contains("/parts/") {
            self.part_bytes
                .fetch_add(result.range.end - result.range.start, Ordering::SeqCst);
        }
        if body && location.as_ref().contains("/upload-content/") {
            self.upload_content_reads.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let failure = Arc::new(AtomicBool::new(
            self.delete_before.swap(false, Ordering::SeqCst),
        ));
        locations
            .then(move |location| {
                let inner = inner.clone();
                let failure = Arc::clone(&failure);
                async move {
                    let path = location?;
                    if failure.swap(false, Ordering::SeqCst) {
                        return Err(lost_acknowledgement());
                    }
                    inner.delete(&path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

impl inventory_sealed::Sealed for PublicationFaultStore {}

impl ObjectStoreInventory for PublicationFaultStore {
    fn ordered_inventory(
        &self,
        prefix: Option<&Path>,
        after: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let pause = {
            let mut pause = self.inventory_pause.lock().unwrap();
            if pause
                .as_ref()
                .is_some_and(|pause| Some(&pause.target) == prefix)
            {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            pause
                .reached
                .send(())
                .expect("inventory boundary observer remains connected");
            pause
                .resume
                .recv_timeout(Duration::from_secs(10))
                .expect("exact inventory boundary resumes");
        }
        match after {
            Some(after) => self.inner.list_with_offset(prefix, after),
            None => self.inner.list(prefix),
        }
    }
}

fn open_store(backend: &Arc<PublicationFaultStore>) -> ObjectResourceStore {
    ObjectResourceStore::new(backend.clone(), PREFIX, BINDING).expect("Resource adapter opens")
}

fn manifest() -> VirtualArchiveManifest {
    let occurrence = WorkOccurrence {
        occurrence_version: VIRTUAL_WORK_OCCURRENCE_VERSION.to_owned(),
        occurrence_id: content_id(
            VIRTUAL_WORK_OCCURRENCE_VERSION,
            &("work:object-replay", 1_u64),
        )
        .unwrap(),
        work_id: "work:object-replay".to_owned(),
        region_id: "region:object-replay".to_owned(),
        run_id: "run:object-replay".to_owned(),
        owner: "worker:object-replay".to_owned(),
        epoch: 1,
        lease_epoch: 1,
        lease_clock: ClockObservationRef {
            clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
            observation_id: content_id("test.clock-observation/1", &1_u64).unwrap(),
            source_id: "clock:object-replay".to_owned(),
            source_generation: content_id("test.clock-generation/1", &()).unwrap(),
            scope: "slot:object-replay".to_owned(),
        },
        plan_id: content_id("test.virtual-plan/1", &"object-replay").unwrap(),
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
        source_causal_cut: BTreeSet::from(["cut:object-replay".to_owned()]),
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

fn open_archive(store: ObjectResourceStore) -> ResourceBackedVirtualArchive<ObjectResourceStore> {
    ResourceBackedVirtualArchive::open(store, "archive:object-replay", "archive:object-replay:1")
        .expect("real Resource-backed archive opens")
}

#[test]
fn public_archive_replays_the_original_write_at_every_publication_receipt_boundary() {
    let _fault_guard = TEST_FAULT_LOCK
        .lock()
        .expect("fault test lock remains healthy");
    for failure in [
        FailurePoint::PublishingHeadAcknowledgement,
        FailurePoint::FirstContentPartAcknowledgement,
        FailurePoint::CommittedHeadAcknowledgement,
        FailurePoint::BeforePublicationCatalog,
        FailurePoint::PublicationCatalogAcknowledgement,
    ] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut archive = open_archive(open_store(&backend));
        let manifest = manifest();
        backend.arm(failure);
        assert!(matches!(
            archive.publish_archive(&manifest),
            Err(ProtocolError::Substrate { .. })
        ));
        assert!(
            backend.fault.lock().unwrap().is_none(),
            "the requested fault boundary executes"
        );
        let session = backend.upload.lock().unwrap().as_ref().unwrap().clone();
        let mut interrupted = archive.into_inner();
        let (record, _) = interrupted.load_record(&session.upload_id).unwrap();
        let publishing = matches!(
            failure,
            FailurePoint::PublishingHeadAcknowledgement
                | FailurePoint::FirstContentPartAcknowledgement
        );
        assert_eq!(
            record.state,
            if publishing {
                UploadState::Publishing
            } else {
                UploadState::Committed
            }
        );
        let expected = interrupted
            .publication_for_record(&record)
            .unwrap()
            .unwrap();
        let original_receipt = interrupted.cleanup_receipt(&session).unwrap();
        let original_catalog = interrupted
            .get_catalog_record(PUBLICATION_CATALOG, &expected.resource.resource_id)
            .unwrap();
        assert_eq!(
            original_catalog.is_some(),
            failure == FailurePoint::PublicationCatalogAcknowledgement
        );
        drop(interrupted);

        let mut reopened = open_archive(open_store(&backend));
        let before = backend.upload_creates.load(Ordering::SeqCst);
        assert_eq!(reopened.publish_archive(&manifest).unwrap(), expected);
        assert_eq!(
            backend.upload_creates.load(Ordering::SeqCst) - before,
            usize::from(original_catalog.is_none()),
            "a missing catalog retries the same public begin/chunk/commit sequence"
        );
        let occurrence = manifest.occurrences.values().next().unwrap();
        assert_eq!(
            reopened
                .rehydrate_occurrence(&expected.resource, &occurrence.occurrence_id)
                .unwrap()
                .occurrence,
            *occurrence
        );
        let mut completed = reopened.into_inner();
        let receipt = completed.cleanup_receipt(&session).unwrap().unwrap();
        if let Some(original) = original_receipt {
            assert_eq!(receipt, original);
        }
        let catalog = completed
            .get_catalog_record(PUBLICATION_CATALOG, &expected.resource.resource_id)
            .unwrap()
            .unwrap();
        if let Some(original) = original_catalog {
            assert_eq!(catalog, original);
        }
        let terminal = completed.load_record(&session.upload_id).unwrap().0;
        assert_eq!(terminal.state, UploadState::Committed);
        drop(completed);

        let mut replay = open_archive(open_store(&backend));
        let before = backend.upload_creates.load(Ordering::SeqCst);
        assert_eq!(replay.publish_archive(&manifest).unwrap(), expected);
        assert_eq!(backend.upload_creates.load(Ordering::SeqCst), before);
        let mut replay = replay.into_inner();
        assert_eq!(replay.load_record(&session.upload_id).unwrap().0, terminal);
        assert_eq!(replay.cleanup_receipt(&session).unwrap(), Some(receipt));
        assert_eq!(
            replay
                .get_catalog_record(PUBLICATION_CATALOG, &expected.resource.resource_id)
                .unwrap(),
            Some(catalog)
        );
    }
}

fn intent(write_id: &str) -> ResourceWriteIntent {
    ResourceWriteIntent {
        write_id: write_id.to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    }
}

fn finish_gc(store: &mut ObjectResourceStore) {
    for _ in 0..100 {
        if store
            .reconcile_upload_content()
            .expect("bounded GC step succeeds")
            .complete
        {
            return;
        }
    }
    panic!("bounded fixture GC did not complete");
}

fn advance_to_deleted_object_sweep(store: &mut ObjectResourceStore) {
    for _ in 0..100 {
        if matches!(
            store.load_upload_gc_record().unwrap().0.phase,
            UploadGcPhase::SweepDeletedObjects { page: None, .. }
        ) {
            return;
        }
        assert!(!store.reconcile_upload_content().unwrap().complete);
    }
    panic!("bounded fixture GC did not reach the deleted-publication traversal");
}

fn assert_epoch_empty(store: &ObjectResourceStore, epoch: u64) {
    let prefix = store
        .key(&format!(
            "upload-content/{}/epochs/{epoch:020}",
            store.content_namespace()
        ))
        .unwrap();
    let objects = store
        .block_on(
            store
                .store
                .ordered_inventory(Some(&prefix), None)
                .try_collect::<Vec<_>>(),
        )
        .unwrap()
        .unwrap();
    assert!(
        objects.is_empty(),
        "retired upload data and radix nodes are actually absent"
    );
}

#[test]
fn publishing_replay_reads_one_retained_root_during_partial_epoch_migration() {
    let _fault_guard = TEST_FAULT_LOCK
        .lock()
        .expect("fault test lock remains healthy");
    let backend = Arc::new(PublicationFaultStore::new());
    let mut store = open_store(&backend);
    let intent = intent("write:migrating-publishing");
    let session = store.begin_write(&intent).unwrap();
    for index in 0..=UPLOAD_GC_CHUNK_PAGE {
        store
            .write_chunk(
                &session,
                index * 2,
                &u16::try_from(index).unwrap().to_be_bytes(),
            )
            .unwrap();
    }
    backend.arm(FailurePoint::PublishingHeadAcknowledgement);
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Substrate { .. })
    ));
    let (original, _) = store.load_record(&session.upload_id).unwrap();
    let expected = store.publication_for_record(&original).unwrap().unwrap();
    for _ in 0..10 {
        store.reconcile_upload_content().unwrap();
        let (record, _) = store.load_record(&session.upload_id).unwrap();
        if record
            .migration
            .as_ref()
            .is_some_and(|migration| migration.migrated_count == UPLOAD_GC_CHUNK_PAGE)
        {
            break;
        }
    }
    let (migrating, _) = store.load_record(&session.upload_id).unwrap();
    assert_eq!(
        migrating.migration.as_ref().unwrap().migrated_count,
        UPLOAD_GC_CHUNK_PAGE
    );
    assert_ne!(
        migrating.content_epoch,
        store.load_upload_gc_record().unwrap().0.current_epoch
    );
    let puts = backend.puts.load(Ordering::SeqCst);
    store
        .write_chunk(&session, 0, &0_u16.to_be_bytes())
        .unwrap();
    store
        .write_chunk(
            &session,
            UPLOAD_GC_CHUNK_PAGE * 2,
            &u16::try_from(UPLOAD_GC_CHUNK_PAGE).unwrap().to_be_bytes(),
        )
        .unwrap();
    for (offset, bytes) in [
        (0, vec![1, 2]),
        (1, vec![0, 0]),
        (0, vec![0]),
        (migrating.next_offset, vec![0]),
        (migrating.next_offset - 1, vec![0, 0]),
        (migrating.next_offset + 1, vec![0]),
    ] {
        assert!(matches!(
            store.write_chunk(&session, offset, &bytes),
            Err(ResourceError::Conflict { .. })
        ));
    }
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Conflict { .. })
    ));
    assert_eq!(store.load_record(&session.upload_id).unwrap().0, migrating);
    assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
    assert!(store.cleanup_receipt(&session).unwrap().is_none());
    finish_gc(&mut store);
    assert_epoch_empty(&store, original.content_epoch);
    drop(store);
    let mut reopened = open_store(&backend);
    assert_eq!(reopened.begin_write(&intent).unwrap(), session);
    for index in 0..=UPLOAD_GC_CHUNK_PAGE {
        reopened
            .write_chunk(
                &session,
                index * 2,
                &u16::try_from(index).unwrap().to_be_bytes(),
            )
            .unwrap();
    }
    assert_eq!(reopened.commit_write(&session).unwrap(), expected);
}

#[test]
fn committed_replay_uses_only_bounded_published_ranges_after_upload_gc() {
    let _fault_guard = TEST_FAULT_LOCK
        .lock()
        .expect("fault test lock remains healthy");
    let backend = Arc::new(PublicationFaultStore::new());
    let mut store = open_store(&backend);
    let intent = intent("write:committed-gc-replay");
    let session = store.begin_write(&intent).unwrap();
    let bytes = vec![0x5a; PART_SIZE * 2 + 33];
    for (index, chunk) in bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
        store
            .write_chunk(&session, (index * MAX_WRITE_CHUNK) as u64, chunk)
            .unwrap();
    }
    let publication = store.commit_write(&session).unwrap();
    let terminal = store.load_record(&session.upload_id).unwrap().0;
    let receipt = store.cleanup_receipt(&session).unwrap().unwrap();
    finish_gc(&mut store);
    assert_epoch_empty(&store, terminal.content_epoch);
    drop(store);
    let mut reopened = open_store(&backend);
    assert_eq!(reopened.begin_write(&intent).unwrap(), session);
    let puts = backend.puts.load(Ordering::SeqCst);
    let parts = backend.part_bytes.load(Ordering::SeqCst);
    let upload_reads = backend.upload_content_reads.load(Ordering::SeqCst);
    let ranges = [
        0..1,
        PART_SIZE - 2..PART_SIZE + 2,
        bytes.len() - 3..bytes.len(),
    ];
    let mut expected_read = 0_u64;
    for range in ranges {
        expected_read += range.len() as u64;
        reopened
            .write_chunk(&session, range.start as u64, &bytes[range])
            .unwrap();
    }
    assert_eq!(
        backend.part_bytes.load(Ordering::SeqCst) - parts,
        expected_read
    );
    assert_eq!(
        backend.upload_content_reads.load(Ordering::SeqCst),
        upload_reads
    );
    assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
    assert_eq!(
        reopened.load_record(&session.upload_id).unwrap().0,
        terminal
    );
    for (offset, changed) in [
        (0, b"changed".as_slice()),
        (bytes.len() as u64, b"x".as_slice()),
        (bytes.len() as u64 - 1, b"xx".as_slice()),
        (bytes.len() as u64 + 1, b"x".as_slice()),
    ] {
        assert!(matches!(
            reopened.write_chunk(&session, offset, changed),
            Err(ResourceError::Conflict { .. })
        ));
    }
    assert!(matches!(
        reopened.write_chunk(&session, cymule_core::MAX_EXACT_INTEGER, b"x"),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        reopened.write_chunk(&session, 0, &[]),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        reopened.write_chunk(&session, 0, &vec![0; MAX_WRITE_CHUNK + 1]),
        Err(ResourceError::Validation(_))
    ));
    let mut foreign = session.clone();
    foreign.store_binding = "object:foreign".to_owned();
    let gets = backend.gets.load(Ordering::SeqCst);
    assert!(matches!(
        reopened.write_chunk(&foreign, 0, b"x"),
        Err(ResourceError::Conflict { .. })
    ));
    foreign = session.clone();
    foreign.write_id = "write:foreign".to_owned();
    assert!(matches!(
        reopened.write_chunk(&foreign, 0, b"x"),
        Err(ResourceError::Conflict { .. })
    ));
    assert_eq!(backend.gets.load(Ordering::SeqCst), gets);
    assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
    assert_eq!(reopened.commit_write(&session).unwrap(), publication);
    assert_eq!(reopened.cleanup_receipt(&session).unwrap(), Some(receipt));
    assert_eq!(
        reopened.load_record(&session.upload_id).unwrap().0,
        terminal
    );
}

#[test]
fn committed_receipt_cannot_republish_deleted_content_or_revive_an_aborted_upload() {
    let _fault_guard = TEST_FAULT_LOCK
        .lock()
        .expect("fault test lock remains healthy");
    let backend = Arc::new(PublicationFaultStore::new());
    let mut store = open_store(&backend);
    let session = store.begin_write(&intent("write:deleted")).unwrap();
    store.write_chunk(&session, 0, b"retained bytes").unwrap();
    let publication = store.commit_write(&session).unwrap();
    let receipt = store.cleanup_receipt(&session).unwrap().unwrap();
    store
        .delete_and_verify_absent(&ResourceDeletionTarget::from_publication(&publication).unwrap())
        .unwrap();
    let terminal = store.load_record(&session.upload_id).unwrap().0;
    let puts = backend.puts.load(Ordering::SeqCst);
    assert!(store.write_chunk(&session, 0, b"retained bytes").is_err());
    assert!(store.commit_write(&session).is_err());
    assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
    let (_, index) = ObjectResourceStore::object_index_record(&publication.resource).unwrap();
    store.verify_deleted_object_absence(&index).unwrap();
    assert_eq!(store.load_record(&session.upload_id).unwrap().0, terminal);
    assert_eq!(store.cleanup_receipt(&session).unwrap(), Some(receipt));

    let session = store.begin_write(&intent("write:aborted")).unwrap();
    store.write_chunk(&session, 0, b"retained bytes").unwrap();
    let receipt = store.abort_write(&session).unwrap();
    let terminal = store.load_record(&session.upload_id).unwrap().0;
    let puts = backend.puts.load(Ordering::SeqCst);
    assert!(matches!(
        store.write_chunk(&session, 0, b"retained bytes"),
        Err(ResourceError::Conflict { .. })
    ));
    assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
    assert_eq!(store.load_record(&session.upload_id).unwrap().0, terminal);
    assert_eq!(store.cleanup_receipt(&session).unwrap(), Some(receipt));
}

#[test]
fn inflight_publisher_cannot_restore_a_deleted_resource_after_same_session_recovery() {
    for (part, empty) in [
        (Some(0), false),
        (Some(1), false),
        (Some(2), false),
        (None, false),
        (None, true),
    ] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut first = open_store(&backend);
        let live_session = first.begin_write(&intent("write:unrelated-live")).unwrap();
        first
            .write_chunk(&live_session, 0, b"keep the live publication")
            .unwrap();
        let live = first.commit_write(&live_session).unwrap();
        let session = first.begin_write(&intent("write:inflight-delete")).unwrap();
        let bytes = if empty {
            Vec::new()
        } else {
            vec![0xa7; PART_SIZE * 2 + 31]
        };
        for (number, bytes) in bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
            first
                .write_chunk(&session, (number * MAX_WRITE_CHUNK) as u64, bytes)
                .unwrap();
        }
        let digest = format!("sha256:{}", cymule_core::sha256_bytes(&bytes));
        let target = match part {
            Some(part) => first.object_part_path(&digest, part).unwrap(),
            None => first.object_index_path(&digest).unwrap(),
        };
        let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
        *backend.put_pause.lock().unwrap() = Some(HeadPause {
            target,
            reached: reached_sender,
            resume: resume_receiver,
        });
        let first_session = session.clone();
        let pending = std::thread::spawn(move || first.commit_write(&first_session));
        reached_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("first publisher retains bytes before the final provider PUT");
        let mut second = open_store(&backend);
        assert_eq!(
            second.load_record(&session.upload_id).unwrap().0.state,
            UploadState::Publishing
        );
        let publication = second
            .commit_write(&session)
            .expect("second driver recovers the exact same publication");
        let cleanup = second.cleanup_receipt(&session).unwrap().unwrap();
        second
            .delete_and_verify_absent(
                &ResourceDeletionTarget::from_publication(&publication).unwrap(),
            )
            .expect("the retained exact deletion target proves absence");
        assert!(matches!(
            second.stat(&publication.resource, &publication.locators),
            Err(ResourceError::NotFound(_))
        ));
        let terminal = second.load_record(&session.upload_id).unwrap();
        let tombstone = second.load_object_head(&digest).unwrap();
        resume_sender.send(()).unwrap();
        assert!(pending.join().unwrap().is_err());
        assert!(
            matches!(
                second.stat(&publication.resource, &publication.locators),
                Err(ResourceError::NotFound(_))
            ),
            "a publisher that retained bytes before deletion cannot restore visibility"
        );
        assert!(matches!(
            second.read(&publication.resource, &publication.locators, 0, 1),
            Err(ResourceError::NotFound(_))
        ));
        assert!(matches!(
            second.read(
                &publication.resource,
                &publication.locators,
                bytes.len() as u64,
                1
            ),
            Err(ResourceError::NotFound(_))
        ));
        assert_eq!(second.load_object_head(&digest).unwrap(), tombstone);
        assert_eq!(second.load_record(&session.upload_id).unwrap(), terminal);
        assert_eq!(second.cleanup_receipt(&session).unwrap(), Some(cleanup));
        finish_gc(&mut second);
        let (_, index) = ObjectResourceStore::object_index_record(&publication.resource).unwrap();
        second.verify_deleted_object_absence(&index).unwrap();
        assert_eq!(second.load_object_head(&digest).unwrap(), tombstone);
        second
            .stat(&live.resource, &live.locators)
            .expect("live content survives retired-family GC");
    }
}

#[test]
fn late_payload_after_a_completed_gc_cycle_is_collected_by_the_next_cycle() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut first = open_store(&backend);
    let session = first.begin_write(&intent("write:late-after-gc")).unwrap();
    let bytes = b"bytes retained past a complete reclamation cycle";
    first.write_chunk(&session, 0, bytes).unwrap();
    let digest = format!("sha256:{}", cymule_core::sha256_bytes(bytes));
    let part = first.object_part_path(&digest, 0).unwrap();
    let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
    let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
    *backend.put_pause.lock().unwrap() = Some(HeadPause {
        target: part.clone(),
        reached: reached_sender,
        resume: resume_receiver,
    });
    let first_session = session.clone();
    let pending = std::thread::spawn(move || first.commit_write(&first_session));
    reached_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let mut second = open_store(&backend);
    let publication = second.commit_write(&session).unwrap();
    let terminal = second.load_record(&session.upload_id).unwrap().0;
    second
        .delete_and_verify_absent(&ResourceDeletionTarget::from_publication(&publication).unwrap())
        .unwrap();
    let tombstone = second.load_object_head(&digest).unwrap();
    finish_gc(&mut second);
    assert_epoch_empty(&second, terminal.content_epoch);
    assert!(second.is_absent(&part).unwrap());
    let completed_epoch = second.load_upload_gc_record().unwrap().0.current_epoch;
    resume_sender.send(()).unwrap();
    assert!(pending.join().unwrap().is_err());
    assert!(
        !second.is_absent(&part).unwrap(),
        "the old writer really created a late physical orphan"
    );
    assert!(matches!(
        second.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
    drop(second);
    let mut reopened = open_store(&backend);
    finish_gc(&mut reopened);
    assert!(reopened.load_upload_gc_record().unwrap().0.current_epoch > completed_epoch);
    assert!(
        reopened.is_absent(&part).unwrap(),
        "ordinary future GC collects late published parts"
    );
    assert_eq!(reopened.load_object_head(&digest).unwrap(), tombstone);
}

#[test]
fn deleted_publication_terminalizes_an_independent_publishing_upload_without_migrating_it() {
    for explicit_abort in [false, true] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut first = open_store(&backend);
        let session = first
            .begin_write(&intent("write:retired-publisher"))
            .unwrap();
        first
            .write_chunk(&session, 0, b"same physical content")
            .unwrap();
        backend.arm(FailurePoint::PublishingHeadAcknowledgement);
        assert!(first.commit_write(&session).is_err());
        let original = first.load_record(&session.upload_id).unwrap().0;
        assert_eq!(original.state, UploadState::Publishing);
        let mut second = open_store(&backend);
        let other = second
            .begin_write(&intent("write:deletion-winner"))
            .unwrap();
        second
            .write_chunk(&other, 0, b"same physical content")
            .unwrap();
        let publication = second.commit_write(&other).unwrap();
        second
            .delete_and_verify_absent(
                &ResourceDeletionTarget::from_publication(&publication).unwrap(),
            )
            .unwrap();
        let winning_terminal = second.load_record(&other.upload_id).unwrap();
        let receipt = if explicit_abort {
            Some(first.abort_write(&session).unwrap())
        } else {
            None
        };
        finish_gc(&mut second);
        let aborted = second.load_record(&session.upload_id).unwrap().0;
        assert_eq!(aborted.state, UploadState::Aborted);
        assert_eq!(aborted.content_epoch, original.content_epoch);
        assert_eq!(aborted.chunk_root, original.chunk_root);
        assert_eq!(aborted.next_offset, original.next_offset);
        assert!(aborted.publication.is_none());
        assert!(aborted.migration.is_none());
        let cleanup = second.cleanup_receipt(&session).unwrap().unwrap();
        assert!(cleanup.plan.targets.is_empty());
        if let Some(receipt) = receipt {
            assert_eq!(receipt, cleanup);
        }
        assert_epoch_empty(&second, original.content_epoch);
        assert_eq!(
            second.load_record(&other.upload_id).unwrap(),
            winning_terminal
        );
        assert!(second.commit_write(&session).is_err());
        assert_eq!(second.cleanup_receipt(&session).unwrap(), Some(cleanup));
    }
}

#[test]
fn deleted_family_gc_accepts_concurrent_lifecycle_deletion_without_a_gc_head_change() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut first = open_store(&backend);
    let session = first.begin_write(&intent("write:gc-delete-race")).unwrap();
    first
        .write_chunk(&session, 0, b"concurrently removed payload")
        .unwrap();
    let publication = first.commit_write(&session).unwrap();
    let target = ResourceDeletionTarget::from_publication(&publication).unwrap();
    let digest = &target.subject.family.content_digest;
    let part = first.object_part_path(digest, 0).unwrap();
    backend.delete_before.store(true, Ordering::SeqCst);
    assert!(matches!(
        first.delete_and_verify_absent(&target),
        Err(ResourceError::Substrate { .. })
    ));
    assert!(!first.is_absent(&part).unwrap());
    assert!(matches!(
        first.load_object_head(digest).unwrap().1,
        ObjectPublicationHead::Deleted { .. }
    ));
    advance_to_deleted_object_sweep(&mut first);
    let source = first.load_upload_gc_record().unwrap();
    let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
    let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
    *backend.head_pause.lock().unwrap() = Some(HeadPause {
        target: part,
        reached: reached_sender,
        resume: resume_receiver,
    });
    let pending = std::thread::spawn(move || first.reconcile_upload_content());
    reached_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let mut second = open_store(&backend);
    second.delete_and_verify_absent(&target).unwrap();
    assert_eq!(second.load_upload_gc_record().unwrap(), source);
    resume_sender.send(()).unwrap();
    pending
        .join()
        .unwrap()
        .expect("exact tombstone admits a concurrently absent target");
    let admitted = second.load_upload_gc_record().unwrap().0;
    let UploadGcPhase::SweepDeletedObjects {
        page: Some(page), ..
    } = admitted.phase
    else {
        panic!("page must be admitted")
    };
    assert_eq!(page.targets.len(), 1);
    assert_eq!(
        second
            .reconcile_upload_content()
            .unwrap()
            .confirmed_absent_objects,
        1
    );
    finish_gc(&mut second);
    assert!(matches!(
        second.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
}

#[test]
fn deleted_family_gc_advances_empty_target_pages_across_live_publications() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut store = open_store(&backend);
    let mut live = Vec::new();
    for number in 0..260_u64 {
        let session = store
            .begin_write(&intent(&format!("write:live-page-{number}")))
            .unwrap();
        store
            .write_chunk(&session, 0, &number.to_be_bytes())
            .unwrap();
        live.push(store.commit_write(&session).unwrap());
    }
    advance_to_deleted_object_sweep(&mut store);
    let progress = store.reconcile_upload_content().unwrap();
    assert_eq!(progress.examined_objects, UPLOAD_GC_OBJECT_PAGE as u64);
    let source = store.load_upload_gc_record().unwrap().0;
    let UploadGcPhase::SweepDeletedObjects {
        page: Some(page), ..
    } = source.phase
    else {
        panic!("page is retained")
    };
    assert_eq!(page.examined_objects, UPLOAD_GC_OBJECT_PAGE as u64);
    assert!(page.targets.is_empty());
    assert!(!page.end_of_inventory);
    let completed_after = page.completed_after.clone();
    let progress = store.reconcile_upload_content().unwrap();
    assert!(!progress.complete);
    assert_eq!(progress.confirmed_absent_objects, 0);
    let UploadGcPhase::SweepDeletedObjects {
        after, page: None, ..
    } = store.load_upload_gc_record().unwrap().0.phase
    else {
        panic!("empty-target page advances")
    };
    assert_eq!(after, completed_after);
    finish_gc(&mut store);
    for publication in live {
        store
            .stat(&publication.resource, &publication.locators)
            .expect("GC preserves every live physical family");
    }
}

#[test]
fn missing_index_deletion_create_conflicts_with_a_publisher_before_removing_payload() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut publisher = open_store(&backend);
    let session = publisher
        .begin_write(&intent("write:missing-index-race"))
        .unwrap();
    publisher
        .write_chunk(&session, 0, b"index publication race")
        .unwrap();
    let digest = format!(
        "sha256:{}",
        cymule_core::sha256_bytes(b"index publication race")
    );
    let index = publisher.object_index_path(&digest).unwrap();
    let (published_sender, published_receiver) = std::sync::mpsc::sync_channel(0);
    let (publish_sender, publish_receiver) = std::sync::mpsc::sync_channel(0);
    *backend.put_pause.lock().unwrap() = Some(HeadPause {
        target: index.clone(),
        reached: published_sender,
        resume: publish_receiver,
    });
    let publisher_session = session.clone();
    let publishing = std::thread::spawn(move || publisher.commit_write(&publisher_session));
    published_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let mut deleter = open_store(&backend);
    let publication = deleter
        .publication_for_record(&deleter.load_record(&session.upload_id).unwrap().0)
        .unwrap()
        .unwrap();
    let target = ResourceDeletionTarget::from_publication(&publication).unwrap();
    let (deletion_sender, deletion_receiver) = std::sync::mpsc::sync_channel(0);
    let (delete_sender, delete_receiver) = std::sync::mpsc::sync_channel(0);
    *backend.put_pause.lock().unwrap() = Some(HeadPause {
        target: index,
        reached: deletion_sender,
        resume: delete_receiver,
    });
    let pending_target = target.clone();
    let deleting = std::thread::spawn(move || deleter.delete_and_verify_absent(&pending_target));
    deletion_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    publish_sender.send(()).unwrap();
    assert_eq!(publishing.join().unwrap().unwrap(), publication);
    delete_sender.send(()).unwrap();
    assert!(matches!(
        deleting.join().unwrap(),
        Err(ResourceError::Conflict { .. })
    ));
    let mut reopened = open_store(&backend);
    reopened
        .stat(&publication.resource, &publication.locators)
        .expect("a lost Create precondition deletes no payload");
    reopened
        .delete_and_verify_absent(&target)
        .expect("an explicit fresh attempt fences the exact now-published head");
    assert!(matches!(
        reopened.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
}

#[test]
fn deleted_family_gc_rejects_a_changed_fence_or_live_target_before_any_payload_delete() {
    for inject_live_target in [false, true] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut store = open_store(&backend);
        let session = store
            .begin_write(&intent("write:retired-plan-target"))
            .unwrap();
        store.write_chunk(&session, 0, b"retired payload").unwrap();
        let retired = store.commit_write(&session).unwrap();
        let deletion = ResourceDeletionTarget::from_publication(&retired).unwrap();
        let retired_part = store
            .object_part_path(&deletion.subject.family.content_digest, 0)
            .unwrap();
        backend.delete_before.store(true, Ordering::SeqCst);
        assert!(store.delete_and_verify_absent(&deletion).is_err());
        let live_session = store
            .begin_write(&intent("write:live-plan-target"))
            .unwrap();
        store
            .write_chunk(&live_session, 0, b"live payload")
            .unwrap();
        let live = store.commit_write(&live_session).unwrap();
        let live_digest = live.resource.integrity.content_digest().unwrap();
        let live_part = store.object_part_path(live_digest, 0).unwrap();
        let live_head = store.load_object_head(live_digest).unwrap().0;
        advance_to_deleted_object_sweep(&mut store);
        store.reconcile_upload_content().unwrap();
        let (mut corrupted, _) = store.load_upload_gc_record().unwrap();
        let UploadGcPhase::SweepDeletedObjects {
            page: Some(page), ..
        } = &mut corrupted.phase
        else {
            panic!("deletion page is retained")
        };
        assert_eq!(page.targets.len(), 1);
        if inject_live_target {
            page.targets.push(DeletedObjectGcTarget {
                path: ObjectResourceStore::relative_inventory_path(
                    &store.objects_prefix().unwrap(),
                    &live_part,
                )
                .unwrap(),
                tombstone_id: live_head.record_id,
                size: b"live payload".len() as u64,
            });
            page.targets
                .sort_by(|left, right| left.path.cmp(&right.path));
        } else {
            page.targets[0].tombstone_id = live_head.record_id;
        }
        store
            .verify_upload_gc_record(&corrupted)
            .expect("the corrupted plan is structurally valid");
        store
            .block_on(backend.inner.put(
                &store.upload_gc_path().unwrap(),
                cymule_core::canonical_bytes(&corrupted).unwrap().into(),
            ))
            .unwrap()
            .unwrap();
        let puts = backend.puts.load(Ordering::SeqCst);
        assert!(matches!(
            store.reconcile_upload_content(),
            Err(ResourceError::Integrity { .. })
        ));
        assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
        assert!(
            !store.is_absent(&retired_part).unwrap(),
            "whole-plan validation precedes its first delete"
        );
        store
            .stat(&live.resource, &live.locators)
            .expect("a retained page cannot turn a live head into deletion authority");
    }
}

#[test]
fn two_gc_drivers_replay_one_retired_payload_page_after_reopen_and_lost_acknowledgement() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut store = open_store(&backend);
    let session = store
        .begin_write(&intent("write:retired-page-replay"))
        .unwrap();
    store
        .write_chunk(&session, 0, b"one retired page target")
        .unwrap();
    let publication = store.commit_write(&session).unwrap();
    let target = ResourceDeletionTarget::from_publication(&publication).unwrap();
    let part = store
        .object_part_path(&target.subject.family.content_digest, 0)
        .unwrap();
    backend.delete_before.store(true, Ordering::SeqCst);
    assert!(store.delete_and_verify_absent(&target).is_err());
    advance_to_deleted_object_sweep(&mut store);
    store.reconcile_upload_content().unwrap();
    let admitted = store.load_upload_gc_record().unwrap();
    drop(store);
    let mut first = open_store(&backend);
    assert_eq!(first.load_upload_gc_record().unwrap(), admitted);
    let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
    let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
    *backend.head_pause.lock().unwrap() = Some(HeadPause {
        target: part.clone(),
        reached: reached_sender,
        resume: resume_receiver,
    });
    let pending = std::thread::spawn(move || first.reconcile_upload_content());
    reached_receiver
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    let mut second = open_store(&backend);
    second
        .test_faults
        .upload_gc_ack
        .store(true, Ordering::SeqCst);
    let winner = second
        .reconcile_upload_content()
        .expect("exact phase readback resolves the lost GC acknowledgement");
    assert_eq!(winner.confirmed_absent_objects, 1);
    let terminal = second.load_upload_gc_record().unwrap();
    resume_sender.send(()).unwrap();
    let replay = pending
        .join()
        .unwrap()
        .expect("the same immutable tombstone authorizes absent-page replay");
    assert_eq!(replay, winner);
    assert_eq!(second.load_upload_gc_record().unwrap(), terminal);
    assert!(second.is_absent(&part).unwrap());
    finish_gc(&mut second);
    assert!(matches!(
        second
            .load_object_head(&target.subject.family.content_digest)
            .unwrap()
            .1,
        ObjectPublicationHead::Deleted { .. }
    ));
}

#[test]
fn gc_inventory_missing_readback_distinguishes_a_lost_head_version_from_provider_violation() {
    for authorized_other_driver in [true, false] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut first = open_store(&backend);
        let session = first
            .begin_write(&intent("write:gc-presence-race"))
            .unwrap();
        first
            .write_chunk(&session, 0, b"old epoch candidate")
            .unwrap();
        first.abort_write(&session).unwrap();
        first.reconcile_upload_content().unwrap();
        first.reconcile_upload_content().unwrap();
        let (source, source_version) = first.load_upload_gc_record().unwrap();
        assert!(matches!(
            source.phase,
            UploadGcPhase::SweepContent { page: None, .. }
        ));
        let prefix = first.upload_content_prefix().unwrap();
        let (paths, _) = first.list_object_page(&prefix, None).unwrap();
        let target = paths.first().unwrap().clone();
        let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
        *backend.head_pause.lock().unwrap() = Some(HeadPause {
            target: target.clone(),
            reached: reached_sender,
            resume: resume_receiver,
        });
        let pending = std::thread::spawn(move || first.reconcile_upload_content());
        reached_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("first driver freezes its head and inventory before target HEAD");
        let mut second = open_store(&backend);
        if authorized_other_driver {
            second
                .reconcile_upload_content()
                .expect("second driver admits its exact page");
            let (admitted, _) = second.load_upload_gc_record().unwrap();
            let UploadGcPhase::SweepContent {
                page: Some(page), ..
            } = admitted.phase
            else {
                panic!("second driver must durably admit the page before deletion");
            };
            assert!(page.paths.contains(
                &ObjectResourceStore::relative_inventory_path(&prefix, &target).unwrap()
            ));
            let progress = second
                .reconcile_upload_content()
                .expect("second driver deletes and retires the admitted page");
            assert!(progress.confirmed_absent_objects > 0);
            assert_epoch_empty(&second, 0);
        } else {
            second
                .block_on(backend.inner.delete(&target))
                .unwrap()
                .unwrap();
        }
        let (current, current_version) = second.load_upload_gc_record().unwrap();
        assert_eq!(
            current, source,
            "the admitted end-page retirement is a real same-content ABA"
        );
        assert_eq!(current_version == source_version, !authorized_other_driver);
        let puts = backend.puts.load(Ordering::SeqCst);
        resume_sender.send(()).unwrap();
        let failure = pending
            .join()
            .unwrap()
            .expect_err("stale inventory must never admit another page");
        if authorized_other_driver {
            assert!(
                matches!(failure, ResourceError::Conflict { code, .. } if code == "object_store_precondition_failed")
            );
        } else {
            assert!(
                matches!(failure, ResourceError::Integrity { code, .. } if code == "object_store_gc_authority_invalid")
            );
        }
        assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
        assert_eq!(
            second.load_upload_gc_record().unwrap(),
            (current, current_version)
        );
    }
}

#[test]
fn private_fault_injection_belongs_only_to_the_admitted_adapter_instance() {
    let backend = Arc::new(PublicationFaultStore::new());
    let mut first = open_store(&backend);
    let mut second = open_store(&backend);
    let first_session = first
        .begin_write(&intent("write:first-fault-owner"))
        .unwrap();
    first.write_chunk(&first_session, 0, b"first").unwrap();
    let second_session = second
        .begin_write(&intent("write:second-fault-owner"))
        .unwrap();
    second.write_chunk(&second_session, 0, b"second").unwrap();
    first
        .test_faults
        .publishing_receipt
        .store(true, Ordering::SeqCst);
    second
        .commit_write(&second_session)
        .expect("another adapter never consumes the fault");
    assert!(matches!(
        first.commit_write(&first_session),
        Err(ResourceError::Substrate { code, .. }) if code == "object_store_publishing_receipt_lost"
    ));
    assert_eq!(
        first.load_record(&first_session.upload_id).unwrap().0.state,
        UploadState::Publishing
    );
    first
        .commit_write(&first_session)
        .expect("the original instance recovers its exact publication");
}

#[test]
fn gc_future_epoch_readback_distinguishes_a_new_cycle_from_an_unadmitted_future_object() {
    for authorized_new_cycle in [true, false] {
        let backend = Arc::new(PublicationFaultStore::new());
        let mut first = open_store(&backend);
        first.reconcile_upload_content().unwrap();
        first.reconcile_upload_content().unwrap();
        let (source, source_version) = first.load_upload_gc_record().unwrap();
        assert!(matches!(
            source.phase,
            UploadGcPhase::SweepContent { page: None, .. }
        ));
        let prefix = first.upload_content_prefix().unwrap();
        let (reached_sender, reached_receiver) = std::sync::mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = std::sync::mpsc::sync_channel(0);
        *backend.inventory_pause.lock().unwrap() = Some(HeadPause {
            target: prefix,
            reached: reached_sender,
            resume: resume_receiver,
        });
        let pending = std::thread::spawn(move || first.reconcile_upload_content());
        reached_receiver
            .recv_timeout(Duration::from_secs(10))
            .expect("first driver freezes its head before its inventory read");
        let mut second = open_store(&backend);
        let bytes = b"new epoch bytes";
        let digest = format!("sha256:{}", cymule_core::sha256_bytes(bytes));
        let future_path = second
            .chunk_data_path(source.current_epoch + 1, &digest)
            .unwrap();
        if authorized_new_cycle {
            finish_gc(&mut second);
            let progress = second
                .reconcile_upload_content()
                .expect("second driver begins the next GC cycle");
            assert_eq!(progress.current_epoch, source.current_epoch + 1);
            let session = second.begin_write(&intent("write:next-cycle")).unwrap();
            second
                .write_chunk(&session, 0, bytes)
                .expect("new writer uses the new current epoch");
            assert_eq!(
                second
                    .load_record(&session.upload_id)
                    .unwrap()
                    .0
                    .content_epoch,
                progress.current_epoch
            );
        } else {
            second
                .block_on(backend.inner.put(&future_path, bytes.to_vec().into()))
                .unwrap()
                .unwrap();
        }
        assert!(!second.is_absent(&future_path).unwrap());
        let (current, current_version) = second.load_upload_gc_record().unwrap();
        assert_eq!(current == source, !authorized_new_cycle);
        assert_eq!(current_version == source_version, !authorized_new_cycle);
        let puts = backend.puts.load(Ordering::SeqCst);
        resume_sender.send(()).unwrap();
        let failure = pending
            .join()
            .unwrap()
            .expect_err("the original frozen head cannot admit future-epoch inventory");
        if authorized_new_cycle {
            assert!(
                matches!(failure, ResourceError::Conflict { code, .. } if code == "object_store_precondition_failed")
            );
        } else {
            assert!(
                matches!(failure, ResourceError::Integrity { code, .. } if code == "object_store_gc_authority_invalid")
            );
        }
        assert_eq!(backend.puts.load(Ordering::SeqCst), puts);
        assert_eq!(
            second.load_upload_gc_record().unwrap(),
            (current, current_version)
        );
        assert!(!second.is_absent(&future_path).unwrap());
    }
}
