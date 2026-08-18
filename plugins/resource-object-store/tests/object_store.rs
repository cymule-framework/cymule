//! Apache in-memory object store conformance for Cymule writes and reads.

use std::collections::BTreeMap;
use std::sync::Arc;

use cymule_resource::{
    ArtifactStore, ResourceClient, ResourceError, ResourceShape, ResourceWriteIntent,
};
use cymule_resource_object_store::ObjectResourceStore;
use object_store::memory::InMemory;

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
