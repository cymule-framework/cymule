//! Filesystem Resource write, retry, reopen, and directory tests.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(unix)]
use std::path::Path;
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::Duration;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, ResourceClient, ResourceError, ResourceIntegrity,
    ResourceShape, ResourceWriteIntent,
};
use cymule_resource_fs::FsResourceStore;
#[cfg(unix)]
use cymule_test_world::{ManagedChild, TestWorld};
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

#[test]
fn unacknowledged_partial_suffix_is_discarded_before_chunk_retry() {
    let directory = tempdir().expect("temporary directory creates");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:partial-suffix".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"durable ")
        .expect("acknowledged prefix writes");
    drop(store);

    let data_path = only_path_with_extension(&root.join("uploads"), "data");
    let mut data = OpenOptions::new()
        .append(true)
        .open(&data_path)
        .expect("upload data opens");
    data.write_all(b"par").expect("partial suffix writes");
    data.sync_all()
        .expect("partial suffix syncs for recovery fixture");
    drop(data);

    let mut reopened = FsResourceStore::open(&root, "fs:test").expect("store reopens");
    reopened
        .write_chunk(&session, 8, b"resource")
        .expect("retry truncates only the unacknowledged suffix");
    let resource = reopened.commit_write(&session).expect("write commits");
    let mut bytes = Vec::new();
    ResourceClient::new(reopened)
        .copy_to(&resource, 4, &mut bytes)
        .expect("resource copies");
    assert_eq!(bytes, b"durable resource");
}

#[test]
fn same_size_object_and_manifest_tampering_fail_digest_verification() {
    let directory = tempdir().expect("temporary directory creates");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:tamper-object".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"abcdefgh")
        .expect("object bytes write");
    let object = store.commit_write(&session).expect("object commits");
    let object_path = only_path_with_no_extension(&root.join("objects"));
    fs::write(&object_path, b"ABCDEFGH").expect("same-size object tamper writes");
    assert!(matches!(
        store.stat(&object),
        Err(ResourceError::Integrity(message)) if message.contains("digest")
    ));
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Integrity(message)) if message.contains("digest")
    ));
    assert!(matches!(
        store.write_chunk(&session, 0, b"abcdefgh"),
        Err(ResourceError::Integrity(message)) if message.contains("digest")
    ));
    let mut copied = Vec::new();
    assert!(matches!(
        ResourceClient::new(store).copy_to(&object, 3, &mut copied),
        Err(ResourceError::Integrity(message)) if message.contains("digest")
    ));

    let directory_root = directory.path().join("directory-store");
    let source_directory = directory.path().join("source-directory");
    fs::create_dir(&source_directory).expect("source directory creates");
    fs::write(source_directory.join("a.txt"), b"a").expect("child writes");
    let mut directory_store =
        FsResourceStore::open(&directory_root, "fs:directory").expect("directory store opens");
    let manifest = directory_store
        .import_directory(&source_directory, "import:tamper-directory")
        .expect("directory imports");
    let manifest_path = resource_object_path(&directory_root, &manifest);
    let mut manifest_bytes = fs::read(&manifest_path).expect("manifest reads");
    let position = manifest_bytes
        .windows(5)
        .position(|window| window == b"a.txt")
        .expect("manifest child name exists");
    manifest_bytes[position] = b'b';
    fs::write(&manifest_path, manifest_bytes).expect("same-size manifest tamper writes");
    assert!(matches!(
        directory_store.list(&manifest, None, 8),
        Err(ResourceError::Integrity(message)) if message.contains("digest")
    ));
}

#[cfg(unix)]
fn process_kill_intent() -> ResourceWriteIntent {
    ResourceWriteIntent {
        write_id: "write:process-kill".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::from([("purpose".to_owned(), "crash-recovery".to_owned())]),
    }
}

#[cfg(unix)]
#[test]
fn resource_process_kill_worker_entry() {
    let Ok(root) = std::env::var("CYMULE_RESOURCE_KILL_ROOT") else {
        return;
    };
    let phase = std::env::var("CYMULE_RESOURCE_KILL_PHASE").expect("kill phase exists");
    let marker = std::env::var("CYMULE_RESOURCE_KILL_MARKER").expect("kill marker exists");
    let mut store = FsResourceStore::open(root, "fs:process-kill").expect("store opens");
    let session = store
        .begin_write(&process_kill_intent())
        .expect("write begins");
    store
        .write_chunk(&session, 0, b"durable ")
        .expect("first chunk writes");
    if phase == "after_commit" {
        store
            .write_chunk(&session, 8, b"resource")
            .expect("second chunk writes");
        store.commit_write(&session).expect("Resource commits");
    } else {
        assert_eq!(phase, "after_chunk");
    }
    fs::write(marker, phase).expect("kill marker writes");
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

#[cfg(unix)]
#[test]
fn filesystem_resource_recovers_from_real_process_death_before_and_after_publication() {
    for phase in ["after_chunk", "after_commit"] {
        let world = TestWorld::new(u64::from(phase == "after_commit"))
            .expect("Resource test world creates");
        let store_root = world.domain().path("store").expect("store root resolves");
        let marker = world
            .domain()
            .path("kill-ready")
            .expect("marker path resolves");
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .arg("--exact")
            .arg("resource_process_kill_worker_entry")
            .arg("--nocapture")
            .env("CYMULE_RESOURCE_KILL_ROOT", &store_root)
            .env("CYMULE_RESOURCE_KILL_PHASE", phase)
            .env("CYMULE_RESOURCE_KILL_MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("kill worker starts");
        child
            .wait_for_content(&marker, phase.as_bytes(), Duration::from_secs(20))
            .expect("worker reaches exact Resource barrier");
        assert_eq!(fs::read_to_string(&marker).expect("barrier reads"), phase);
        assert_eq!(
            child.terminate().expect("worker is reaped").signal(),
            Some(9)
        );
        assert!(child.is_reaped());

        let mut store =
            FsResourceStore::open(&store_root, "fs:process-kill").expect("store reopens");
        let session = store
            .begin_write(&process_kill_intent())
            .expect("write resumes");
        store
            .write_chunk(&session, 0, b"durable ")
            .expect("first chunk replays");
        store
            .write_chunk(&session, 8, b"resource")
            .expect("second chunk converges");
        let resource = store.commit_write(&session).expect("commit converges");
        resource.verify().expect("Resource handle verifies");
        assert!(matches!(
            &resource.integrity,
            ResourceIntegrity::Content { digest, size }
                if digest == &format!("sha256:{}", cymule_core::sha256_bytes(b"durable resource"))
                    && *size == 16
        ));
        let replay = store.commit_write(&session).expect("commit replays");
        assert_eq!(resource, replay);
        let mut bytes = Vec::new();
        ResourceClient::new(store)
            .copy_to(&resource, 3, &mut bytes)
            .expect("Resource copies");
        assert_eq!(bytes, b"durable resource");
    }
}

fn only_path_with_extension(directory: &Path, extension: &str) -> std::path::PathBuf {
    let paths = fs::read_dir(directory)
        .expect("directory reads")
        .map(|entry| entry.expect("directory entry reads").path())
        .filter(|path| path.extension().is_some_and(|value| value == extension))
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "expected exactly one .{extension} path");
    paths.into_iter().next().expect("one path exists")
}

fn only_path_with_no_extension(directory: &Path) -> std::path::PathBuf {
    let paths = fs::read_dir(directory)
        .expect("directory reads")
        .map(|entry| entry.expect("directory entry reads").path())
        .filter(|path| path.extension().is_none())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "expected exactly one object path");
    paths.into_iter().next().expect("one path exists")
}

fn resource_object_path(
    root: &Path,
    resource: &cymule_resource::ResourceHandle,
) -> std::path::PathBuf {
    let ResourceIntegrity::Content { digest, .. } = &resource.integrity else {
        panic!("filesystem Resource must be content verified");
    };
    root.join("objects").join(
        digest
            .strip_prefix("sha256:")
            .expect("digest prefix exists"),
    )
}
