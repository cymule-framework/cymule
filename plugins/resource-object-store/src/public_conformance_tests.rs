//! Apache in-memory object store conformance for Cymule writes and reads.

use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use crate::ObjectResourceStore;
use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_READ_CHUNK, MAX_RESOURCE_CATALOG_RECORD_BYTES,
    RESOURCE_CATALOG_RECORD_VERSION, ResourceCandidate, ResourceCatalogRecord,
    ResourceCatalogStore, ResourceClient, ResourceDeleter, ResourceDeletionTarget, ResourceError,
    ResourceIntegrity, ResourceLocation, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
    resource_retention_key,
};
use futures_util::{StreamExt as _, TryStreamExt as _, stream::BoxStream};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[derive(Debug)]
struct DeleteReceiptLossStore {
    inner: InMemory,
    lost_receipt_path: Arc<Mutex<Option<Path>>>,
    delete_calls: Arc<AtomicUsize>,
}

impl DeleteReceiptLossStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            lost_receipt_path: Arc::new(Mutex::new(None)),
            delete_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn lose_receipt_after(&self, path: Path) {
        *self
            .lost_receipt_path
            .lock()
            .expect("delete fault remains healthy") = Some(path);
    }
}

impl std::fmt::Display for DeleteReceiptLossStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("delete-receipt-loss-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for DeleteReceiptLossStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.inner.put_opts(location, payload, options).await?;
        let lose_receipt = {
            let mut fault = self
                .lost_receipt_path
                .lock()
                .expect("put fault remains healthy");
            if fault.as_ref() == Some(location) {
                *fault = None;
                true
            } else {
                false
            }
        };
        if lose_receipt {
            return Err(object_store::Error::Generic {
                store: "delete-receipt-loss-store",
                source: std::io::Error::other(
                    "provider deletion fence committed before acknowledgement loss",
                )
                .into(),
            });
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
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let inner = self.inner.clone();
        let lost_receipt_path = Arc::clone(&self.lost_receipt_path);
        let delete_calls = Arc::clone(&self.delete_calls);
        locations
            .then(move |location| {
                let inner = inner.clone();
                let lost_receipt_path = Arc::clone(&lost_receipt_path);
                let delete_calls = Arc::clone(&delete_calls);
                async move {
                    let path = location?;
                    delete_calls.fetch_add(1, Ordering::SeqCst);
                    inner.delete(&path).await?;
                    let lose_receipt = {
                        let mut fault = lost_receipt_path
                            .lock()
                            .expect("delete fault remains healthy");
                        if fault.as_ref() == Some(&path) {
                            *fault = None;
                            true
                        } else {
                            false
                        }
                    };
                    if lose_receipt {
                        return Err(object_store::Error::Generic {
                            store: "delete-receipt-loss-store",
                            source: std::io::Error::other(
                                "provider delete completed before acknowledgement loss",
                            )
                            .into(),
                        });
                    }
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

impl crate::inventory_sealed::Sealed for DeleteReceiptLossStore {}

impl crate::ObjectStoreInventory for DeleteReceiptLossStore {
    fn ordered_inventory(
        &self,
        prefix: Option<&Path>,
        after: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match after {
            Some(after) => self.inner.list_with_offset(prefix, after),
            None => self.inner.list(prefix),
        }
    }
}

fn write_object(
    store: &mut ObjectResourceStore,
    write_id: &str,
    bytes: &[u8],
) -> cymule_resource::ResourcePublication {
    let session = write_open_object(store, write_id, bytes);
    store.commit_write(&session).expect("object write commits")
}

fn write_open_object(
    store: &mut ObjectResourceStore,
    write_id: &str,
    bytes: &[u8],
) -> ResourceWriteSession {
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: write_id.to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("object write begins");
    let mut offset = 0_u64;
    for chunk in bytes.chunks(cymule_resource::MAX_WRITE_CHUNK) {
        store
            .write_chunk(&session, offset, chunk)
            .expect("object chunk persists");
        offset += chunk.len() as u64;
    }
    session
}

fn binding_namespace(binding: &str) -> String {
    cymule_core::sha256_bytes(binding.as_bytes())
}

fn oversized_catalog_record() -> ResourceCatalogRecord {
    let namespace = "test.catalog.large/1".to_owned();
    let key = "oversized".to_owned();
    let payload = vec![255_u8; 4 * 1024 * 1024];
    let record_id = cymule_core::content_id(
        RESOURCE_CATALOG_RECORD_VERSION,
        &(namespace.as_str(), key.as_str(), payload.as_slice()),
    )
    .expect("oversized record identity derives");
    ResourceCatalogRecord {
        record_version: RESOURCE_CATALOG_RECORD_VERSION.to_owned(),
        namespace,
        key,
        record_id,
        payload,
    }
}

#[test]
fn object_store_chunk_retry_commit_and_read_are_exact() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:test").expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:object".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"hello ")
        .expect("first chunk");
    store
        .write_chunk(&session, 0, b"hello ")
        .expect("chunk retry succeeds");
    assert!(matches!(
        store.write_chunk(&session, 0, b"changed"),
        Err(ResourceError::Conflict { code, .. }) if code == "object_store_upload_conflict"
    ));
    store
        .write_chunk(&session, 6, b"object store")
        .expect("second chunk");
    let resource = store.commit_write(&session).expect("write commits");
    assert_eq!(
        store.commit_write(&session).expect("commit replays"),
        resource
    );
    let mut client = ResourceClient::new(store);
    let mut output = Vec::new();
    client
        .copy_to(&resource, 4, &mut output)
        .expect("object copies");
    assert_eq!(output, b"hello object store");
}

#[test]
fn empty_object_copies_through_the_resource_client_without_a_zero_range_request() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "empty", "object:empty").expect("adapter builds");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:empty".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("empty write begins");
    let publication = store.commit_write(&session).expect("empty write commits");
    store
        .stat(&publication.resource, &publication.locators)
        .expect("empty object stat verifies");
    let direct = store
        .read(&publication.resource, &publication.locators, 0, 1)
        .expect("empty object read returns terminal EOF");
    assert!(direct.bytes.is_empty());
    assert!(direct.eof);
    let mut copied = Vec::new();
    ResourceClient::new(store)
        .copy_to(&publication, 1, &mut copied)
        .expect("empty object copies");
    assert!(copied.is_empty());
}

#[test]
fn direct_object_store_read_rejects_a_range_above_the_provider_bound() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "bounded", "object:read-bound").expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:read-bound".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"bounded")
        .expect("chunk writes");
    let publication = store.commit_write(&session).expect("write commits");

    assert!(matches!(
        store.read(
            &publication.resource,
            &publication.locators,
            0,
            MAX_READ_CHUNK + 1,
        ),
        Err(ResourceError::Validation(message)) if message.contains("read range")
    ));
    let size = publication
        .resource
        .integrity
        .content_size()
        .expect("content size");
    for offset in [size + 1, u64::MAX] {
        assert!(matches!(
            store.read(
                &publication.resource,
                &publication.locators,
                offset,
                1,
            ),
            Err(ResourceError::Validation(message)) if message.contains("read range")
        ));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn object_store_chunk_rejects_an_out_of_range_frontier_before_content_mutation() {
    let backend = Arc::new(InMemory::new());
    let prefix = "exact-frontier";
    let binding = "object:exact-frontier";
    let mut store =
        ObjectResourceStore::new(backend.clone(), prefix, binding).expect("adapter builds");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:exact-frontier".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    let content_prefix = Path::from(format!(
        "{prefix}/upload-content/{}/epochs",
        binding_namespace(binding)
    ));
    let before = backend.list(Some(&content_prefix)).count().await;

    assert!(matches!(
        store.write_chunk(&session, cymule_core::MAX_EXACT_INTEGER, b"x"),
        Err(ResourceError::Validation(message)) if message.contains("exact-integer")
    ));
    assert_eq!(backend.list(Some(&content_prefix)).count().await, before);
}

#[test]
fn object_store_rejects_forged_upload_sessions() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:test").expect("adapter builds");
    let forged = ResourceWriteSession {
        write_id: "write:forged".to_owned(),
        upload_id: "upload:../../outside".to_owned(),
        store_binding: "object:test".to_owned(),
    };
    assert!(matches!(
        store.write_chunk(&forged, 0, b"escape"),
        Err(ResourceError::Conflict { code, .. }) if code == "object_store_upload_conflict"
    ));
}

#[test]
fn shared_prefix_uploads_are_bound_to_the_complete_configured_binding() {
    let backend = Arc::new(InMemory::new());
    let intent = ResourceWriteIntent {
        write_id: "write:shared-prefix-binding".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let mut first =
        ObjectResourceStore::new(backend.clone(), "shared", "object:first").expect("first opens");
    let first_session = first.begin_write(&intent).expect("first upload begins");
    first
        .write_chunk(&first_session, 0, b"first binding")
        .expect("first chunk persists");

    let mut second =
        ObjectResourceStore::new(backend, "shared", "object:second").expect("second opens");
    let second_session = second
        .begin_write(&intent)
        .expect("second binding starts an independent upload");
    assert_ne!(first_session.upload_id, second_session.upload_id);
    assert!(matches!(
        second.write_chunk(&first_session, 13, b" forbidden"),
        Err(ResourceError::Conflict { code, .. }) if code == "object_store_upload_conflict"
    ));
    assert!(matches!(
        second.abort_write(&first_session),
        Err(ResourceError::Conflict { code, .. }) if code == "object_store_upload_conflict"
    ));
    let mut relabeled = first_session.clone();
    relabeled.store_binding = "object:second".to_owned();
    assert!(matches!(
        second.abort_write(&relabeled),
        Err(ResourceError::Conflict { code, .. }) if code == "object_store_upload_conflict"
    ));

    first
        .write_chunk(&first_session, 13, b" continues")
        .expect("owning binding continues");
    first
        .commit_write(&first_session)
        .expect("owning binding commits");
    second
        .abort_write(&second_session)
        .expect("second binding cleans only its upload");
}

#[test]
fn shared_prefix_content_and_deletion_are_partitioned_by_binding() {
    let backend = Arc::new(InMemory::new());
    let intent = ResourceWriteIntent {
        write_id: "write:shared-content".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let mut first = ObjectResourceStore::new(backend.clone(), "shared-content", "object:first")
        .expect("first opens");
    let first_session = first.begin_write(&intent).expect("first write begins");
    first
        .write_chunk(&first_session, 0, b"same bytes")
        .expect("first bytes write");
    let first_publication = first.commit_write(&first_session).expect("first commits");

    let mut second =
        ObjectResourceStore::new(backend, "shared-content", "object:second").expect("second opens");
    let second_session = second.begin_write(&intent).expect("second write begins");
    second
        .write_chunk(&second_session, 0, b"same bytes")
        .expect("second bytes write");
    let second_publication = second
        .commit_write(&second_session)
        .expect("second commits");
    assert_eq!(
        first_publication.resource.integrity,
        second_publication.resource.integrity
    );
    assert_ne!(
        resource_retention_key(&first_publication).expect("first retention key"),
        resource_retention_key(&second_publication).expect("second retention key")
    );

    let target = ResourceDeletionTarget::from_publication(&first_publication)
        .expect("first deletion target derives");
    first
        .delete_and_verify_absent(&target)
        .expect("first binding deletes only its family");
    second
        .stat(&second_publication.resource, &second_publication.locators)
        .expect("second binding's identical bytes remain present");
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_prefix_gc_isolates_open_committed_and_aborted_upload_heads_after_reopen() {
    let backend = Arc::new(InMemory::new());
    let prefix = "shared-binding-gc";
    let mut uploads = Vec::new();
    for binding in ["object:gc-first", "object:gc-second"] {
        let mut store =
            ObjectResourceStore::new(backend.clone(), prefix, binding).expect("binding opens");
        let open = write_open_object(&mut store, "write:open", b"open frontier");
        let committed = write_object(&mut store, "write:committed", b"committed object");
        let aborted = write_open_object(&mut store, "write:aborted", b"aborted orphan");
        store.abort_write(&aborted).expect("upload terminalizes");
        let upload_prefix = Path::from(format!("{prefix}/uploads/{}", binding_namespace(binding)));
        assert_eq!(
            prefix_snapshot(backend.as_ref(), &upload_prefix)
                .await
                .len(),
            3,
            "all three heads must belong to the physical binding namespace"
        );
        uploads.push((binding, open, committed, aborted));
    }

    for (ordinal, (binding, open, committed, aborted)) in uploads.iter().enumerate() {
        let foreign_binding = uploads[1 - ordinal].0;
        let foreign_before = binding_snapshot(backend.as_ref(), prefix, foreign_binding).await;
        let old_epoch = Path::from(format!(
            "{prefix}/upload-content/{}/epochs/{:020}",
            binding_namespace(binding),
            0
        ));
        assert!(
            !prefix_snapshot(backend.as_ref(), &old_epoch)
                .await
                .is_empty()
        );
        let mut complete = false;
        let mut confirmed_absence = false;
        for _ in 0..64 {
            let mut reopened = ObjectResourceStore::new(backend.clone(), prefix, *binding)
                .expect("reclamation resumes from its exact persisted binding head");
            let progress = reopened
                .reconcile_upload_content()
                .expect("foreign open and terminal heads cannot enter this binding's GC");
            confirmed_absence |= progress.confirmed_absent_objects > 0;
            if progress.complete {
                complete = true;
                break;
            }
        }
        assert!(complete, "bounded fixture reclamation must finish");
        assert!(
            confirmed_absence,
            "GC must verify its admitted old-epoch targets"
        );
        assert!(
            prefix_snapshot(backend.as_ref(), &old_epoch)
                .await
                .is_empty(),
            "old-epoch candidates must actually be reclaimed"
        );
        assert_eq!(
            binding_snapshot(backend.as_ref(), prefix, foreign_binding).await,
            foreign_before,
            "GC must not mutate another binding's heads, content, or reclamation authority"
        );
        let mut reopened = ObjectResourceStore::new(backend.clone(), prefix, *binding)
            .expect("active writer reopens after reclamation");
        reopened
            .write_chunk(open, 13, b":resumed")
            .expect("the migrated active head admits more bytes");
        let resumed = reopened.commit_write(open).expect("active upload commits");
        reopened
            .stat(&committed.resource, &committed.locators)
            .expect("committed content is not upload-content GC");
        assert!(
            reopened
                .cleanup_receipt(aborted)
                .expect("terminal upload receipt remains readable")
                .is_some()
        );
        let mut output = Vec::new();
        ResourceClient::new(reopened)
            .copy_to(&resumed, 5, &mut output)
            .expect("migrated acknowledged bytes remain exact");
        assert_eq!(output, b"open frontier:resumed");
    }
}

async fn binding_snapshot(
    backend: &impl ObjectStore,
    prefix: &str,
    binding: &str,
) -> BTreeMap<String, Vec<u8>> {
    let mut snapshot = BTreeMap::new();
    for family in [
        "uploads",
        "upload-content",
        "objects",
        "catalog",
        "inventory-capability",
    ] {
        let family = Path::from(format!("{prefix}/{family}/{}", binding_namespace(binding)));
        snapshot.extend(prefix_snapshot(backend, &family).await);
    }
    snapshot
}

#[test]
fn shared_prefix_catalog_records_are_partitioned_by_binding() {
    let backend = Arc::new(InMemory::new());
    let mut first = ObjectResourceStore::new(backend.clone(), "shared-catalog", "object:first")
        .expect("first opens");
    let mut second =
        ObjectResourceStore::new(backend, "shared-catalog", "object:second").expect("second opens");
    let first_record = ResourceCatalogRecord::new("test.catalog/1", "same-key", b"first".to_vec())
        .expect("first record seals");
    let second_record =
        ResourceCatalogRecord::new("test.catalog/1", "same-key", b"second".to_vec())
            .expect("second record seals");
    first
        .put_catalog_record(&first_record)
        .expect("first binding publishes");
    second
        .put_catalog_record(&second_record)
        .expect("second binding publishes independent bytes");
    assert_eq!(
        first
            .get_catalog_record("test.catalog/1", "same-key")
            .expect("first reads"),
        Some(first_record)
    );
    assert_eq!(
        second
            .get_catalog_record("test.catalog/1", "same-key")
            .expect("second reads"),
        Some(second_record)
    );
}

#[test]
fn catalog_rejects_oversized_records_before_provider_mutation() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(backend, "catalog-bound", "object:catalog-bound")
        .expect("adapter builds");
    let record = oversized_catalog_record();

    assert!(matches!(
        store.put_catalog_record(&record),
        Err(ResourceError::Validation(message)) if message.contains("canonical JSON bytes")
    ));
    assert_eq!(
        store
            .get_catalog_record("test.catalog.large/1", "oversized")
            .expect("catalog remains readable"),
        None,
        "oversized record must not cross the provider commit boundary"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_rejects_oversized_provider_metadata_before_body_materialization() {
    let backend = Arc::new(InMemory::new());
    let binding = "object:catalog-read-bound";
    let prefix = "catalog-read-bound";
    let namespace = "test.catalog.large/1";
    let key = "oversized";
    let mut store =
        ObjectResourceStore::new(backend.clone(), prefix, binding).expect("adapter builds");
    let mut locator_identity = namespace.as_bytes().to_vec();
    locator_identity.push(0);
    locator_identity.extend_from_slice(key.as_bytes());
    let path = Path::from(format!(
        "{prefix}/catalog/{}/{}.json",
        binding_namespace(binding),
        cymule_core::sha256_bytes(&locator_identity)
    ));
    backend
        .put(
            &path,
            vec![
                0_u8;
                usize::try_from(MAX_RESOURCE_CATALOG_RECORD_BYTES + 1).expect("catalog bound fits")
            ]
            .into(),
        )
        .await
        .expect("oversized provider body seeds");

    assert!(matches!(
        store.get_catalog_record(namespace, key),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_catalog_invalid"
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_rejects_oversized_provider_metadata_before_resume_materialization() {
    let backend = Arc::new(InMemory::new());
    let prefix = "upload-read-bound";
    let mut store = ObjectResourceStore::new(backend.clone(), prefix, "object:upload-read-bound")
        .expect("adapter builds");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:upload-read-bound".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("upload begins");
    let key = session
        .upload_id
        .strip_prefix("upload:")
        .expect("upload prefix");
    backend
        .put(
            &Path::from(format!(
                "{prefix}/uploads/{}/{key}/record.json",
                binding_namespace("object:upload-read-bound")
            )),
            vec![0_u8; 16 * 1024 * 1024 + 1].into(),
        )
        .await
        .expect("oversized provider body seeds");

    assert!(matches!(
        store.write_chunk(&session, 0, b"must not download"),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_upload_record_invalid"
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn annotation_capacity_is_rejected_before_provider_mutation_and_maximum_can_begin() {
    let backend = Arc::new(InMemory::new());
    let prefix = "metadata-admission";
    let mut store = ObjectResourceStore::new(backend.clone(), prefix, "object:metadata-admission")
        .expect("adapter builds");
    let annotations = (0..=cymule_resource::MAX_RESOURCE_ANNOTATIONS)
        .map(|index| (format!("annotation-{index:04}"), String::new()))
        .collect();
    let oversized = ResourceWriteIntent {
        write_id: "write:metadata-admission".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations,
    };

    assert!(matches!(
        store.begin_write(&oversized),
        Err(ResourceError::Validation(message)) if message.contains("annotations")
    ));
    assert!(
        backend
            .list(Some(&Path::from(format!("{prefix}/uploads"))))
            .next()
            .await
            .is_none(),
        "metadata rejection must not publish an upload head"
    );
    let maximum_annotations = (0..cymule_resource::MAX_RESOURCE_ANNOTATIONS)
        .map(|index| (format!("annotation-{index:04}"), "🧪".repeat(4096)))
        .collect();
    store
        .begin_write(&ResourceWriteIntent {
            write_id: oversized.write_id,
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: maximum_annotations,
        })
        .expect("maximum legal metadata preflights before the first provider mutation");
}

#[test]
fn object_store_deleter_is_idempotent_and_proves_absence() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:test").expect("adapter builds");
    let write = ResourceWriteIntent {
        write_id: "write:delete".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&write).expect("write begins");
    store
        .write_chunk(&session, 0, b"deleted")
        .expect("chunk writes");
    let publication = store.commit_write(&session).expect("write commits");
    let target =
        ResourceDeletionTarget::from_publication(&publication).expect("deletion target derives");
    store
        .delete_and_verify_absent(&target)
        .expect("delete succeeds and proves absence");
    store
        .delete_and_verify_absent(&target)
        .expect("absent target replays");
    assert!(matches!(
        store.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_recovers_after_every_part_delete_loses_its_receipt() {
    for part_count in [1_u64, 3] {
        let bytes = if part_count == 1 {
            b"one retained part".to_vec()
        } else {
            vec![0xa5; 2 * crate::PART_SIZE + 7]
        };
        for lost_after in 0..part_count {
            let backend = Arc::new(DeleteReceiptLossStore::new());
            let prefix = "delete-receipt-recovery";
            let binding = "object:delete-receipt-recovery";
            let mut store = ObjectResourceStore::new(backend.clone(), prefix, binding)
                .expect("deletion adapter opens");
            let publication = write_object(&mut store, "write:delete-receipt-recovery", &bytes);
            let target = ResourceDeletionTarget::from_publication(&publication)
                .expect("exact durable deletion target derives");
            let digest_key = target
                .subject
                .family
                .content_digest
                .strip_prefix("sha256:")
                .expect("content digest has its prefix");
            let family = Path::from(format!(
                "{prefix}/objects/{}/{digest_key}",
                binding_namespace(binding)
            ));
            let lost_path = family
                .clone()
                .join("parts")
                .join(format!("{lost_after:020}"));
            let mut foreign = ObjectResourceStore::new(
                backend.clone(),
                prefix,
                "object:foreign-deletion-binding",
            )
            .expect("foreign binding opens");
            let foreign_publication = write_object(&mut foreign, "write:foreign", b"keep me");
            backend.lose_receipt_after(lost_path.clone());
            assert!(matches!(
                store.delete_and_verify_absent(&target),
                Err(ResourceError::Substrate { code, .. })
                    if code == "object_store_provider_failure"
            ));
            assert!(matches!(
                backend.head(&lost_path).await,
                Err(object_store::Error::NotFound { .. })
            ));
            assert_eq!(
                backend.delete_calls.load(Ordering::SeqCst),
                usize::try_from(lost_after + 1).expect("delete boundary fits usize")
            );
            drop(store);

            let mut reopened = ObjectResourceStore::new(backend.clone(), prefix, binding)
                .expect("adapter reopens after acknowledgement loss");
            reopened
                .delete_and_verify_absent(&target)
                .expect("the same exact target converges every partial deletion");
            reopened
                .delete_and_verify_absent(&target)
                .expect("terminal absence replays");
            let remaining = backend
                .list(Some(&family))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].location, family.clone().join("index.json"));
            assert!(matches!(
                reopened.stat(&publication.resource, &publication.locators),
                Err(ResourceError::NotFound(_))
            ));
            foreign
                .stat(&foreign_publication.resource, &foreign_publication.locators)
                .expect("foreign binding remains intact");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_fence_lost_acknowledgement_replays_only_the_exact_terminal_head() {
    let backend = Arc::new(DeleteReceiptLossStore::new());
    let mut store = ObjectResourceStore::new(backend.clone(), "fence-ack", "object:fence-ack")
        .expect("adapter opens");
    let publication = write_object(&mut store, "write:fence-ack", b"retained");
    let target = ResourceDeletionTarget::from_publication(&publication).unwrap();
    let index_path = store
        .object_index_path(&target.subject.family.content_digest)
        .unwrap();
    backend.lose_receipt_after(index_path.clone());
    store
        .delete_and_verify_absent(&target)
        .expect("exact tombstone readback resolves the lost CAS response");
    assert!(backend.lost_receipt_path.lock().unwrap().is_none());
    let (record, head, version) = store
        .load_object_head(&target.subject.family.content_digest)
        .unwrap();
    assert!(matches!(head, crate::ObjectPublicationHead::Deleted { .. }));
    assert_eq!(backend.delete_calls.load(Ordering::SeqCst), 1);
    store.delete_and_verify_absent(&target).unwrap();
    let alias_resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: publication.resource.media_type.clone(),
        inline: None,
        integrity: publication.resource.integrity.clone(),
        manifest: None,
        annotations: BTreeMap::from([(
            "name".to_owned(),
            "another semantic descriptor".to_owned(),
        )]),
    }
    .seal()
    .unwrap();
    let alias = cymule_resource::ResourcePublication {
        locators: cymule_resource::ResourceLocatorSet {
            resource_id: alias_resource.resource_id.clone(),
            ..publication.locators.clone()
        },
        resource: alias_resource,
    };
    assert_ne!(alias.resource.resource_id, publication.resource.resource_id);
    store
        .delete_and_verify_absent(&ResourceDeletionTarget::from_publication(&alias).unwrap())
        .expect("semantic aliases share the same physical terminal fence");
    assert_eq!(
        store
            .load_object_head(&target.subject.family.content_digest)
            .unwrap(),
        (record, head, version),
        "replay never replaces or deletes the permanent fence"
    );
    backend
        .head(&index_path)
        .await
        .expect("non-payload fence remains retained");
    assert!(matches!(
        store.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_preflight_rejects_bad_size_digest_index_and_siblings_before_mutation() {
    for invalid in [
        "target-size",
        "part-size",
        "part-digest",
        "index",
        "sibling",
    ] {
        let backend = Arc::new(DeleteReceiptLossStore::new());
        let prefix = "delete-preflight";
        let binding = "object:delete-preflight";
        let mut store = ObjectResourceStore::new(backend.clone(), prefix, binding)
            .expect("deletion adapter opens");
        let publication = write_object(&mut store, "write:delete-preflight", b"retained");
        let mut target = ResourceDeletionTarget::from_publication(&publication)
            .expect("exact durable deletion target derives");
        let digest_key = target
            .subject
            .family
            .content_digest
            .strip_prefix("sha256:")
            .expect("content digest has its prefix");
        let family = Path::from(format!(
            "{prefix}/objects/{}/{digest_key}",
            binding_namespace(binding)
        ));
        let part = family.clone().join("parts").join("00000000000000000000");
        match invalid {
            "target-size" => target.content_size += 1,
            "part-size" => {
                backend
                    .put(&part, b"wrong-length".to_vec().into())
                    .await
                    .expect("wrong part size seeds");
            }
            "part-digest" => {
                backend
                    .put(&part, b"tampered".to_vec().into())
                    .await
                    .expect("same-size changed part seeds");
            }
            "index" => {
                backend
                    .put(&family.clone().join("index.json"), b"{}".to_vec().into())
                    .await
                    .expect("malformed index seeds");
            }
            "sibling" => {
                backend
                    .put(
                        &family.clone().join("unknown.bin"),
                        b"foreign".to_vec().into(),
                    )
                    .await
                    .expect("unknown sibling seeds");
            }
            _ => unreachable!("closed invalid fixture"),
        }
        let before = prefix_snapshot(backend.as_ref(), &family).await;
        assert!(
            store.delete_and_verify_absent(&target).is_err(),
            "invalid {invalid} must fail before physical deletion"
        );
        assert_eq!(backend.delete_calls.load(Ordering::SeqCst), 0);
        assert_eq!(prefix_snapshot(backend.as_ref(), &family).await, before);
    }
}

async fn prefix_snapshot(backend: &impl ObjectStore, prefix: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut snapshot = BTreeMap::new();
    let entries: Vec<ObjectMeta> = backend
        .list(Some(prefix))
        .try_collect()
        .await
        .expect("bounded fixture inventory reads");
    for entry in entries {
        let bytes = backend
            .get(&entry.location)
            .await
            .expect("fixture object reads")
            .bytes()
            .await
            .expect("fixture bytes read");
        snapshot.insert(entry.location.to_string(), bytes.to_vec());
    }
    snapshot
}

#[test]
fn absent_family_deletion_does_not_materialize_the_declared_size() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(
        backend,
        "malicious-absent-delete",
        "object:malicious-absent-delete",
    )
    .expect("adapter builds");
    let resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: format!("sha256:{}", cymule_core::sha256_bytes(b"absent family")),
            size: 9_007_199_254_740_991,
        },
        manifest: None,
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("safe-integer-sized Resource seals");
    let digest = resource
        .integrity
        .content_digest()
        .expect("digest exists")
        .to_owned();
    let publication = cymule_resource::ResourcePublication {
        locators: cymule_resource::ResourceLocatorSet {
            locator_version: cymule_resource::RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: "object:malicious-absent-delete".to_owned(),
            locations: vec![ResourceLocation::Opaque { reference: digest }],
        },
        resource: resource.clone(),
    };
    publication.verify().expect("publication verifies");
    let target = ResourceDeletionTarget::from_publication(&publication)
        .expect("absent deletion target derives");
    store
        .delete_and_verify_absent(&target)
        .expect("empty physical family is immediately absent");
}

#[tokio::test(flavor = "multi_thread")]
async fn terminal_read_rejects_a_nonempty_object_with_a_missing_part() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(
        backend.clone(),
        "terminal-read-closure",
        "object:terminal-read-closure",
    )
    .expect("adapter builds");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:terminal-read-closure".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    store
        .write_chunk(&session, 0, b"nonempty")
        .expect("chunk writes");
    let publication = store.commit_write(&session).expect("write commits");
    let size = publication
        .resource
        .integrity
        .content_size()
        .expect("content size exists");
    let digest = publication
        .resource
        .integrity
        .content_digest()
        .expect("content digest exists")
        .strip_prefix("sha256:")
        .expect("digest prefix");
    backend
        .delete(&Path::from(format!(
            "terminal-read-closure/objects/{}/{digest}/parts/{:020}",
            binding_namespace("object:terminal-read-closure"),
            0
        )))
        .await
        .expect("part deletion injects corruption");

    assert!(matches!(
        store.read(
            &publication.resource,
            &publication.locators,
            size,
            1,
        ),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_object_invalid"
    ));
}

#[test]
fn forged_digest_locator_cannot_redirect_read_or_exact_target_deletion() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "locator", "object:locator").expect("adapter builds");
    let mut publications = Vec::new();
    for (write_id, bytes) in [("write:locator-a", b"AAAA"), ("write:locator-b", b"BBBB")] {
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: write_id.to_owned(),
                shape: ResourceShape::Object,
                media_type: "application/octet-stream".to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("write begins");
        store.write_chunk(&session, 0, bytes).expect("bytes write");
        publications.push(store.commit_write(&session).expect("write commits"));
    }
    let mut forged = publications[0].locators.clone();
    forged.locations = vec![ResourceLocation::Opaque {
        reference: publications[1]
            .resource
            .integrity
            .content_digest()
            .expect("second digest")
            .to_owned(),
    }];
    assert!(matches!(
        store.read(&publications[0].resource, &forged, 0, 4),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_upload_record_invalid"
    ));
    let target = ResourceDeletionTarget::from_publication(&publications[0])
        .expect("exact deletion target derives without locator authority");
    store
        .delete_and_verify_absent(&target)
        .expect("exact first target deletes");
    assert!(matches!(
        store.stat(&publications[0].resource, &publications[0].locators),
        Err(ResourceError::NotFound(_))
    ));
    store
        .stat(&publications[1].resource, &publications[1].locators)
        .expect("unrelated object remains present");
}

#[test]
fn object_store_rejects_non_object_shape() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:test").expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:directory".to_owned(),
        shape: ResourceShape::Directory,
        media_type: "application/json".to_owned(),
        annotations: BTreeMap::new(),
    };
    assert!(matches!(
        store.begin_write(&intent),
        Err(ResourceError::Validation(_))
    ));
}

#[test]
fn abort_terminalizes_the_head_without_claiming_immutable_chunk_deletion() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:test").expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:abort-cleanup".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"first")
        .expect("first chunk stages");
    store
        .write_chunk(&session, 5, b"second")
        .expect("second chunk stages");
    let receipt = store.abort_write(&session).expect("abort terminalizes");
    receipt.verify().expect("cleanup receipt verifies");
    assert!(receipt.verified_absent);
    assert_eq!(receipt.removed_chunks, 0);
    assert_eq!(receipt.removed_staging_objects, 0);
    assert!(receipt.plan.targets.is_empty());
    let replay = store.abort_write(&session).expect("abort replays");
    assert_eq!(replay, receipt);
    assert_eq!(
        store
            .cleanup_receipt(&session)
            .expect("cleanup receipt query succeeds"),
        Some(receipt)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn synchronous_adapter_bridges_from_a_multithread_runtime() {
    let backend = Arc::new(InMemory::new());
    let mut store =
        ObjectResourceStore::new(backend, "async", "object:async").expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:async".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"bridged")
        .expect("chunk writes");
    store.commit_write(&session).expect("write commits");
}

#[tokio::test(flavor = "multi_thread")]
async fn stat_streams_and_rejects_same_size_object_tampering() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(backend.clone(), "tamper", "object:tamper")
        .expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:tamper".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"abcdefgh")
        .expect("bytes persist");
    let publication = store.commit_write(&session).expect("write commits");
    let ResourceIntegrity::Content { digest, .. } = &publication.resource.integrity else {
        panic!("object-store publication must be content addressed");
    };
    let key = digest
        .strip_prefix("sha256:")
        .expect("digest prefix exists");
    backend
        .put(
            &Path::from(format!(
                "tamper/objects/{}/{key}/parts/{:020}",
                binding_namespace("object:tamper"),
                0
            )),
            b"ABCDEFGH".to_vec().into(),
        )
        .await
        .expect("same-size tamper overwrites object");

    assert!(matches!(
        store.stat(&publication.resource, &publication.locators),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_object_invalid"
    ));
    assert!(matches!(
        ResourceClient::new(store).copy_to(&publication, 2, &mut Vec::new()),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_object_invalid"
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_record_binding_tamper_is_detected_before_resume() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(backend.clone(), "records", "object:alpha")
        .expect("adapter builds");
    let intent = ResourceWriteIntent {
        write_id: "write:record-binding".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("upload begins");
    let path = Path::from(format!(
        "records/uploads/{}/{}/record.json",
        binding_namespace("object:alpha"),
        session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload prefix")
    ));
    let result = backend.get(&path).await.expect("record reads");
    let mut bytes = result.bytes().await.expect("record bytes read").to_vec();
    let position = bytes
        .windows(b"object:alpha".len())
        .position(|window| window == b"object:alpha")
        .expect("record contains binding");
    bytes[position..position + b"object:alpha".len()].copy_from_slice(b"object:bravo");
    backend
        .put(&path, bytes.into())
        .await
        .expect("record binding tamper overwrites");
    assert!(matches!(
        store.write_chunk(&session, 0, b"must not resume"),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_upload_record_invalid"
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_refuses_to_report_absence_while_unrecognized_old_parts_remain() {
    let backend = Arc::new(InMemory::new());
    let mut store = ObjectResourceStore::new(backend.clone(), "old-parts", "object:old-parts")
        .expect("adapter builds");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:old-parts".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    store
        .write_chunk(&session, 0, b"current")
        .expect("current bytes write");
    let publication = store.commit_write(&session).expect("write commits");
    let digest = publication
        .resource
        .integrity
        .content_digest()
        .expect("content digest")
        .strip_prefix("sha256:")
        .expect("digest prefix")
        .to_owned();
    backend
        .put(
            &Path::from(format!(
                "old-parts/objects/{}/{digest}/parts/{:020}",
                binding_namespace("object:old-parts"),
                1
            )),
            b"legacy-extra-part".to_vec().into(),
        )
        .await
        .expect("legacy extra part seeds");
    let target =
        ResourceDeletionTarget::from_publication(&publication).expect("deletion target derives");
    assert!(matches!(
        store.delete_and_verify_absent(&target),
        Err(ResourceError::Integrity { code, .. })
            if code == "object_store_cleanup_invalid"
    ));
    backend
        .head(&Path::from(format!(
            "old-parts/objects/{}/{digest}/parts/{:020}",
            binding_namespace("object:old-parts"),
            1
        )))
        .await
        .expect("old bytes remain observable after fail-closed deletion");
}
