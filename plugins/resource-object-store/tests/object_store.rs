//! Apache in-memory object store conformance for Cymule writes and reads.

use std::collections::BTreeMap;
use std::sync::Arc;

use cymule_resource::{
    ArtifactStore, RESOURCE_DELETE_INTENT_VERSION, ResourceClient, ResourceDeleteIntent,
    ResourceDeleter, ResourceError, ResourceShape, ResourceWriteIntent, ResourceWriteSession,
};
use cymule_resource_object_store::ObjectResourceStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use tempfile::tempdir;

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
        Err(ResourceError::Conflict(_))
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
        Err(ResourceError::Conflict(_) | ResourceError::Validation(_))
    ));
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
    let delete = ResourceDeleteIntent {
        intent_version: RESOURCE_DELETE_INTENT_VERSION.to_owned(),
        delete_id: "delete:object".to_owned(),
        gc_id: "gc:object".to_owned(),
        resource_id: publication.resource.resource_id.clone(),
        store_binding: "object:test".to_owned(),
        publication,
    };
    let first = store.delete_resource(&delete).expect("delete succeeds");
    assert_eq!(first.removed_bytes, 7);
    assert!(first.verified_absent);
    let replay = store.delete_resource(&delete).expect("delete replays");
    assert_eq!(replay.removed_bytes, 0);
    assert!(replay.verified_absent);
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
fn abort_deletes_every_owned_chunk_and_returns_verified_receipt() {
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
    let receipt = store.abort_write(&session).expect("abort cleans");
    receipt.verify().expect("cleanup receipt verifies");
    assert!(receipt.verified_absent);
    assert_eq!(receipt.removed_chunks, 2);
    assert_eq!(receipt.removed_staging_objects, 0);
    let replay = store.abort_write(&session).expect("abort replays");
    assert_eq!(replay.removed_chunks, 0);
    assert!(replay.verified_absent);
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

#[test]
fn backend_without_conditional_metadata_update_fails_closed() {
    let directory = tempdir().expect("temporary directory creates");
    let backend = Arc::new(
        LocalFileSystem::new_with_prefix(directory.path()).expect("local object backend opens"),
    );
    let mut store =
        ObjectResourceStore::new(backend, "cymule", "object:local").expect("adapter opens");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:unsupported-cas".to_owned(),
            shape: ResourceShape::Object,
            media_type: "text/plain".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    assert!(matches!(
        store.write_chunk(&session, 0, b"cannot weaken CAS"),
        Err(ResourceError::Substrate(_))
    ));
}
