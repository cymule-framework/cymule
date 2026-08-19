//! Filesystem Resource write, retry, reopen, and directory tests.

use std::collections::BTreeMap;
use std::fs;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, ResourceClient, ResourceError, ResourceShape,
    ResourceWriteIntent,
};
use cymule_resource_fs::FsResourceStore;
use tempfile::tempdir;

#[test]
fn chunk_retry_commit_and_reopen_preserve_exact_bytes() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:one".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"hello ")
        .expect("first chunk");
    store
        .write_chunk(&session, 0, b"hello ")
        .expect("identical retry succeeds");
    assert!(matches!(
        store.write_chunk(&session, 0, b"changed"),
        Err(ResourceError::Conflict(_))
    ));
    store
        .write_chunk(&session, 6, b"cymule")
        .expect("second chunk");
    let resource = store.commit_write(&session).expect("write commits");
    assert_eq!(
        store.commit_write(&session).expect("commit replays"),
        resource
    );
    let reopened = FsResourceStore::open(directory.path(), "fs:test").expect("store reopens");
    let mut client = ResourceClient::new(reopened);
    let mut bytes = Vec::new();
    client
        .copy_to(&resource, 3, &mut bytes)
        .expect("resource copies");
    assert_eq!(bytes, b"hello cymule");
}

#[test]
fn file_import_replays_after_publication_without_changing_bytes() {
    let directory = tempdir().expect("temporary directory");
    let source = directory.path().join("suite.jsonl");
    fs::write(&source, b"one\ntwo\n").expect("source writes");
    let mut store =
        FsResourceStore::open(directory.path().join("store"), "fs:test").expect("store opens");
    let first = store
        .import_file(&source, "import:suite", "application/x-ndjson")
        .expect("first import publishes");
    let replay = store
        .import_file(&source, "import:suite", "application/x-ndjson")
        .expect("published import replays");
    assert_eq!(replay, first);

    fs::write(&source, b"one\nBAD\n").expect("source changes");
    assert!(matches!(
        store.import_file(&source, "import:suite", "application/x-ndjson"),
        Err(ResourceError::Conflict(_))
    ));
    fs::write(&source, b"one\n").expect("source truncates");
    assert!(matches!(
        store.import_file(&source, "import:suite", "application/x-ndjson"),
        Err(ResourceError::Conflict(_))
    ));
}

#[test]
fn recursive_directory_import_lists_bounded_sorted_pages() {
    let directory = tempdir().expect("temporary directory");
    let source = directory.path().join("source");
    fs::create_dir_all(source.join("nested")).expect("source creates");
    fs::write(source.join("b.txt"), b"b").expect("b writes");
    fs::write(source.join("a.txt"), b"a").expect("a writes");
    fs::write(source.join("nested/c.txt"), b"c").expect("c writes");
    let mut store =
        FsResourceStore::open(directory.path().join("store"), "fs:test").expect("store opens");
    let resource = store
        .import_directory(&source, "import:source")
        .expect("directory imports");
    let first = store.list(&resource, None, 2).expect("first page lists");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["a.txt", "b.txt"]
    );
    let second = store
        .list(&resource, first.next_cursor.as_deref(), 2)
        .expect("second page lists");
    assert_eq!(second.entries[0].name, "nested");
    assert!(second.next_cursor.is_none());
}

#[test]
fn malformed_directory_manifest_fails_before_publication() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:bad-manifest".to_owned(),
        shape: ResourceShape::Directory,
        media_type: "application/vnd.cymule.directory+jsonl".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"not-json\n")
        .expect("bytes stage");
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Substrate(_))
    ));
}
