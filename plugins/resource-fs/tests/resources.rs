//! Filesystem Resource write, retry, reopen, and directory tests.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::Duration;

use cymule_resource::{
    ArtifactResolver, ArtifactStore, MAX_MANIFEST_ENTRY_BYTES, MAX_MANIFEST_PAGE_BYTES,
    MAX_READ_CHUNK, MAX_RESOURCE_CATALOG_RECORD_BYTES, MAX_WRITE_CHUNK,
    RESOURCE_CATALOG_RECORD_VERSION, RESOURCE_MANIFEST_MEDIA_TYPE, ResourceCandidate,
    ResourceCatalogRecord, ResourceCatalogStore, ResourceClient, ResourceDeleter,
    ResourceDeletionTarget, ResourceError, ResourceIntegrity, ResourceListCursor, ResourceLocation,
    ResourceManifestDescriptor, ResourceManifestEntry, ResourcePublication, ResourceShape,
    ResourceWriteIntent, ResourceWriteSession, resource_retention_key,
};
use cymule_resource_fs::{FsResourceStore, MAX_DIRECTORY_IMPORT_DEPTH};
#[cfg(unix)]
use cymule_test_world::{ManagedChild, TestWorld};
use tempfile::tempdir;

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

fn assert_resource_not_found(store: &mut FsResourceStore, publication: &ResourcePublication) {
    assert!(matches!(
        store.stat(&publication.resource, &publication.locators),
        Err(ResourceError::NotFound(_))
    ));
}

#[test]
fn read_only_open_never_creates_or_cleans_and_rejects_mutation() {
    let directory = tempdir().expect("temporary directory");
    let missing = directory.path().join("missing");
    assert!(FsResourceStore::open_read_only(&missing, "fs:read-only").is_err());
    assert!(!missing.exists(), "read-only open must not create its root");

    let root = directory.path().join("store");
    let mut writable = FsResourceStore::open(&root, "fs:read-only").expect("store initializes");
    let intent = ResourceWriteIntent {
        write_id: "write:read-only-published".to_owned(),
        shape: ResourceShape::Object,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = writable.begin_write(&intent).expect("write begins");
    writable
        .write_chunk(&session, 0, b"published")
        .expect("bytes persist");
    let publication = writable.commit_write(&session).expect("write commits");
    fs::write(root.join("staging").join("operator-owned-residue"), b"keep").expect("residue seeds");
    drop(writable);

    let mut read_only =
        FsResourceStore::open_read_only(&root, "fs:read-only").expect("read-only store opens");
    read_only
        .stat(&publication.resource, &publication.locators)
        .expect("read-only resolver reads retained content");
    assert!(matches!(
        read_only.begin_write(&ResourceWriteIntent {
            write_id: "write:read-only-forbidden".to_owned(),
            shape: ResourceShape::Object,
            media_type: "text/plain".to_owned(),
            annotations: BTreeMap::new(),
        }),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_read_only"
    ));
    assert_eq!(
        fs::read(root.join("staging").join("operator-owned-residue"))
            .expect("read-only open leaves residue untouched"),
        b"keep"
    );
}

#[test]
fn physical_generation_marker_rejects_unmarked_or_wrong_layouts() {
    let directory = tempdir().expect("temporary directory");
    let legacy = directory.path().join("legacy");
    fs::create_dir(&legacy).expect("legacy root creates");
    fs::write(legacy.join("old-upload.json"), b"legacy").expect("legacy bytes seed");
    assert!(matches!(
        FsResourceStore::open(&legacy, "fs:legacy"),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));
    assert!(!legacy.join("layout.json").exists());

    let wrong = directory.path().join("wrong");
    fs::create_dir(&wrong).expect("wrong root creates");
    fs::write(
        wrong.join("layout.json"),
        br#"{"layout_version":"cymule.resource-fs-layout/1"}"#,
    )
    .expect("pre-tombstone marker seeds");
    assert!(matches!(
        FsResourceStore::open(&wrong, "fs:wrong"),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));

    let mixed = directory.path().join("mixed");
    drop(FsResourceStore::open(&mixed, "fs:mixed").expect("current layout initializes"));
    fs::write(
        mixed.join("objects").join("a".repeat(64)),
        b"legacy flat bytes",
    )
    .expect("legacy flat object seeds");
    assert!(matches!(
        FsResourceStore::open(&mixed, "fs:mixed"),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));
}

#[test]
fn physical_generation_initialization_recovers_only_owned_crash_residue() {
    let directory = tempdir().expect("temporary directory");

    let interrupted = directory.path().join("interrupted");
    fs::create_dir(&interrupted).expect("interrupted root creates");
    fs::write(
        interrupted.join("layout.json.initializing"),
        b"partial temporary marker",
    )
    .expect("interrupted temporary marker seeds");
    drop(
        FsResourceStore::open(&interrupted, "fs:interrupted-layout")
            .expect("owned temporary marker is recoverable"),
    );
    assert!(!interrupted.join("layout.json.initializing").exists());
    assert!(
        fs::metadata(interrupted.join("layout.json"))
            .expect("final marker exists")
            .len()
            > 0
    );

    let zero_marker = directory.path().join("zero-marker");
    fs::create_dir(&zero_marker).expect("zero-marker root creates");
    fs::write(zero_marker.join("layout.json"), []).expect("zero marker seeds");
    drop(
        FsResourceStore::open(&zero_marker, "fs:zero-marker")
            .expect("zero marker in an otherwise new namespace is recoverable"),
    );
    assert!(
        fs::metadata(zero_marker.join("layout.json"))
            .expect("recovered marker exists")
            .len()
            > 0
    );

    let poisoned = directory.path().join("poisoned");
    fs::create_dir(&poisoned).expect("poisoned root creates");
    fs::write(poisoned.join("layout.json"), []).expect("zero marker seeds");
    fs::create_dir(poisoned.join("uploads")).expect("owned data directory seeds");
    assert!(matches!(
        FsResourceStore::open(&poisoned, "fs:poisoned-layout"),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));
    assert_eq!(
        fs::metadata(poisoned.join("layout.json"))
            .expect("poisoned marker remains for operator repair")
            .len(),
        0
    );
}

#[test]
fn catalog_bounds_gate_writes_and_metadata_before_materialization() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("catalog-bound");
    let mut store = FsResourceStore::open(&root, "fs:catalog-bound").expect("store opens");
    let record = oversized_catalog_record();

    assert!(matches!(
        store.put_catalog_record(&record),
        Err(ResourceError::Validation(message)) if message.contains("canonical JSON bytes")
    ));
    let binding_catalog = fs::read_dir(root.join("catalog"))
        .expect("catalog root lists")
        .next()
        .expect("binding namespace exists")
        .expect("binding namespace reads")
        .path();
    assert!(
        fs::read_dir(&binding_catalog)
            .expect("binding catalog lists")
            .next()
            .is_none(),
        "oversized record must not cross the catalog commit boundary"
    );

    let namespace = "test.catalog.large/1";
    let key = "oversized";
    let mut locator_identity = namespace.as_bytes().to_vec();
    locator_identity.push(0);
    locator_identity.extend_from_slice(key.as_bytes());
    let catalog_path = binding_catalog.join(format!(
        "{}.json",
        cymule_core::sha256_bytes(&locator_identity)
    ));
    let legacy_payload = b"legacy catalog".to_vec();
    let legacy_record = ResourceCatalogRecord {
        record_version: "cymule.resource-catalog-record/1".to_owned(),
        namespace: namespace.to_owned(),
        key: key.to_owned(),
        record_id: cymule_core::content_id(
            "cymule.resource-catalog-record/1",
            &(namespace, key, legacy_payload.as_slice()),
        )
        .expect("legacy catalog identity derives"),
        payload: legacy_payload,
    };
    fs::write(
        &catalog_path,
        cymule_core::canonical_bytes(&legacy_record).expect("legacy record encodes"),
    )
    .expect("legacy catalog record seeds");
    assert!(matches!(
        store.get_catalog_record(namespace, key),
        Err(ResourceError::Validation(message)) if message.contains("version")
    ));
    fs::File::create(catalog_path)
        .expect("oversized catalog file creates")
        .set_len(MAX_RESOURCE_CATALOG_RECORD_BYTES + 1)
        .expect("oversized catalog metadata seeds without allocating its body");
    assert!(matches!(
        store.get_catalog_record(namespace, key),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_catalog_invalid"
    ));
}

#[cfg(unix)]
#[test]
fn publishing_intent_precedes_content_and_abort_converges_after_reopen() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:publishing-order").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:publishing-order".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"publishing-before-content")
        .expect("bytes persist");
    let objects = fs::read_dir(root.join("objects"))
        .expect("binding namespace root lists")
        .next()
        .expect("binding namespace exists")
        .expect("binding namespace reads")
        .path();
    fs::set_permissions(&objects, fs::Permissions::from_mode(0o500))
        .expect("content directory becomes read-only");
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Substrate { code, .. }) if code == "filesystem_io_failure"
    ));
    fs::set_permissions(&objects, fs::Permissions::from_mode(0o700))
        .expect("content directory becomes writable again");

    let record = only_path_with_extension(&root.join("uploads"), "json");
    let record: serde_json::Value =
        cymule_core::decode_json(&fs::read(record).expect("record reads")).expect("record decodes");
    assert_eq!(record["state"], "publishing");
    assert!(
        fs::read_dir(&objects)
            .expect("objects list")
            .next()
            .is_none(),
        "content family must not precede its durable Publishing intent"
    );
    drop(store);

    let mut reopened = FsResourceStore::open(&root, "fs:publishing-order").expect("store reopens");
    let cleanup = reopened
        .abort_write(&session)
        .expect("abort converges Publishing before cleaning upload state");
    assert!(cleanup.verified_absent);
    let publication = reopened
        .commit_write(&session)
        .expect("converged publication remains replayable");
    let mut bytes = Vec::new();
    ResourceClient::new(reopened)
        .copy_to(&publication, 7, &mut bytes)
        .expect("converged bytes remain exact");
    assert_eq!(bytes, b"publishing-before-content");
}

#[cfg(unix)]
#[test]
fn fixed_directory_replacement_cannot_redirect_an_open_store() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let outside = directory.path().join("outside");
    fs::create_dir(&outside).expect("outside creates");
    let mut store = FsResourceStore::open(&root, "fs:dirfd").expect("store opens");
    fs::rename(root.join("objects"), root.join("objects-retained")).expect("owned directory moves");
    std::os::unix::fs::symlink(&outside, root.join("objects"))
        .expect("attacker symlink replaces visible path");

    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:dirfd".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins through retained descriptors");
    store
        .write_chunk(&session, 0, b"descriptor-owned")
        .expect("chunk writes");
    let publication = store
        .commit_write(&session)
        .expect("commit stays beneath held dirfd");
    let digest = publication
        .resource
        .integrity
        .content_digest()
        .expect("digest exists")
        .strip_prefix("sha256:")
        .expect("digest prefix");
    assert!(
        root.join("objects-retained")
            .read_dir()
            .expect("retained binding namespace lists")
            .any(|entry| entry
                .expect("binding namespace reads")
                .path()
                .join(digest)
                .is_file())
    );
    assert!(
        fs::read_dir(&outside)
            .expect("outside lists")
            .next()
            .is_none()
    );
    assert!(matches!(
        FsResourceStore::open(&root, "fs:dirfd"),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));
}

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
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
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
fn chunk_rejects_an_out_of_range_frontier_before_data_mutation() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("resources");
    let mut store = FsResourceStore::open(&root, "fs:exact-frontier").expect("store opens");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:exact-frontier".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");

    assert!(matches!(
        store.write_chunk(&session, cymule_core::MAX_EXACT_INTEGER, b"x"),
        Err(ResourceError::Validation(message)) if message.contains("exact-integer")
    ));
    let upload_key = session
        .upload_id
        .strip_prefix("upload:")
        .expect("upload key is canonical");
    assert!(
        !root
            .join("uploads")
            .join(format!("{upload_key}.data"))
            .exists()
    );
}

#[test]
fn direct_filesystem_read_rejects_a_range_above_the_provider_bound() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:read-bound").expect("store opens");
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
        Err(ResourceError::Validation(message)) if message.contains("read limit")
    ));
}

#[test]
fn forged_upload_session_cannot_escape_or_cross_bindings() {
    let directory = tempdir().expect("temporary directory");
    let sentinel = directory.path().join("sentinel");
    fs::write(&sentinel, b"outside").expect("sentinel writes");
    let mut store =
        FsResourceStore::open(directory.path().join("store"), "fs:test").expect("store opens");
    let forged = ResourceWriteSession {
        write_id: "write:forged".to_owned(),
        upload_id: "upload:../../sentinel".to_owned(),
        store_binding: "fs:test".to_owned(),
    };
    assert!(matches!(
        store.write_chunk(&forged, 0, b"escape"),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
    ));
    assert_eq!(fs::read(&sentinel).expect("sentinel reads"), b"outside");
}

#[test]
fn shared_root_uploads_are_bound_to_the_complete_configured_binding() {
    let directory = tempdir().expect("temporary directory");
    let intent = ResourceWriteIntent {
        write_id: "write:shared-root-binding".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let mut first = FsResourceStore::open(directory.path(), "fs:first").expect("first opens");
    let first_session = first.begin_write(&intent).expect("first upload begins");
    first
        .write_chunk(&first_session, 0, b"first binding")
        .expect("first chunk persists");

    let mut second = FsResourceStore::open(directory.path(), "fs:second").expect("second opens");
    let second_session = second
        .begin_write(&intent)
        .expect("second binding starts an independent upload");
    assert_ne!(first_session.upload_id, second_session.upload_id);
    assert!(matches!(
        second.write_chunk(&first_session, 13, b" forbidden"),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
    ));
    assert!(matches!(
        second.abort_write(&first_session),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
    ));
    let mut relabeled = first_session.clone();
    relabeled.store_binding = "fs:second".to_owned();
    assert!(matches!(
        second.abort_write(&relabeled),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_upload_conflict"
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
fn shared_root_content_and_deletion_are_partitioned_by_binding() {
    let directory = tempdir().expect("temporary directory");
    let intent = ResourceWriteIntent {
        write_id: "write:shared-content".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let mut first = FsResourceStore::open(directory.path(), "fs:first").expect("first opens");
    let first_session = first.begin_write(&intent).expect("first write begins");
    first
        .write_chunk(&first_session, 0, b"same bytes")
        .expect("first bytes write");
    let first_publication = first.commit_write(&first_session).expect("first commits");

    let mut second = FsResourceStore::open(directory.path(), "fs:second").expect("second opens");
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

#[test]
fn upload_record_binding_tamper_is_detected_before_resume() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:record").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:record-binding".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("upload begins");
    let record = directory.path().join("uploads").join(format!(
        "{}.json",
        session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload prefix")
    ));
    let mut bytes = fs::read(&record).expect("record reads");
    let position = bytes
        .windows(b"fs:record".len())
        .position(|window| window == b"fs:record")
        .expect("record contains binding");
    bytes[position..position + b"fs:record".len()].copy_from_slice(b"fs:forged");
    fs::write(record, bytes).expect("record binding tamper writes");
    assert!(matches!(
        store.write_chunk(&session, 0, b"must not resume"),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_upload_record_invalid"
    ));
}

#[test]
fn annotation_capacity_is_rejected_before_claim_and_maximum_metadata_can_begin() {
    let directory = tempdir().expect("temporary directory");
    let mut store =
        FsResourceStore::open(directory.path(), "fs:metadata-admission").expect("store opens");
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
    assert_eq!(
        fs::read_dir(directory.path().join("uploads"))
            .expect("upload directory reads")
            .count(),
        0,
        "metadata rejection must not publish an upload head"
    );
    assert_eq!(
        fs::read_dir(directory.path().join("locks"))
            .expect("lock directory reads")
            .count(),
        0,
        "metadata rejection must precede the writer claim mutation"
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
        .expect("maximum legal metadata preflights before the first mutation");
}

#[test]
fn filesystem_deleter_is_idempotent_and_proves_absence() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:delete".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"deleted")
        .expect("chunk writes");
    let publication = store.commit_write(&session).expect("write commits");
    let target =
        ResourceDeletionTarget::from_publication(&publication).expect("deletion target derives");
    let mut mismatched = target.clone();
    mismatched.content_size += 1;
    assert!(matches!(
        store.delete_and_verify_absent(&mismatched),
        Err(ResourceError::Integrity { .. })
    ));
    store
        .stat(&publication.resource, &publication.locators)
        .expect("target validation failure does not fence valid retained content");
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

#[test]
fn deletion_tombstone_fences_publishing_across_write_ids_and_reopen() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let binding = "fs:deletion-fence";
    let bytes = b"one physical retention family";
    let mut published = FsResourceStore::open(&root, binding).expect("store opens");
    let first = ResourceWriteIntent {
        write_id: "write:deletion-fence-first".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let first_session = published.begin_write(&first).expect("first write begins");
    published
        .write_chunk(&first_session, 0, bytes)
        .expect("first bytes persist");
    let publication = published
        .commit_write(&first_session)
        .expect("first publication commits");
    let target = ResourceDeletionTarget::from_publication(&publication)
        .expect("exact deletion target derives");

    let late = ResourceWriteIntent {
        write_id: "write:deletion-fence-late".to_owned(),
        ..first.clone()
    };
    let mut late_writer =
        FsResourceStore::open(&root, binding).expect("independent late writer opens");
    let late_session = late_writer.begin_write(&late).expect("late write begins");
    late_writer
        .write_chunk(&late_session, 0, bytes)
        .expect("late bytes persist");

    let retention_key = resource_retention_key(&publication).expect("retention key derives");
    let retention_token = retention_key
        .strip_prefix("sha256:")
        .expect("retention key is SHA-256 addressed");
    let retention_control = root
        .join("locks")
        .join(format!("retention-{retention_token}.lock"));
    let family_barrier = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&retention_control)
        .expect("retention-family control opens");
    fs4::FileExt::lock(&family_barrier).expect("deterministic family barrier locks");

    assert!(matches!(
        late_writer.commit_write(&late_session),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_lock_busy"
    ));
    assert!(matches!(
        published.delete_and_verify_absent(&target),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_lock_busy"
    ));
    assert_eq!(
        fs::read(&retention_control).expect("live retention control reads"),
        b"",
        "content remains live until deletion durably fences the family"
    );
    fs4::FileExt::unlock(&family_barrier).expect("family barrier unlocks");

    published
        .delete_and_verify_absent(&target)
        .expect("deletion persists its fence before removing content");
    assert_eq!(
        fs::read(&retention_control).expect("deletion tombstone reads"),
        b"D",
        "the exact family control permanently records deletion"
    );
    drop(late_writer);
    drop(published);

    let mut reopened = FsResourceStore::open(&root, binding).expect("deleted store reopens");
    assert_resource_not_found(&mut reopened, &publication);
    assert!(matches!(
        reopened.commit_write(&late_session),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_resource_deleted"
    ));
    assert!(matches!(
        reopened.begin_write(&late),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_resource_deleted"
    ));

    let third = ResourceWriteIntent {
        write_id: "write:deletion-fence-third".to_owned(),
        ..first
    };
    let third_session = reopened
        .begin_write(&third)
        .expect("different write ID begins");
    reopened
        .write_chunk(&third_session, 0, bytes)
        .expect("different write ID stages identical bytes");
    assert!(matches!(
        reopened.commit_write(&third_session),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_resource_deleted"
    ));
    reopened
        .delete_and_verify_absent(&target)
        .expect("terminal deletion replays after reopen");
    assert_resource_not_found(&mut reopened, &publication);
    assert_eq!(
        fs::read(&retention_control).expect("permanent tombstone remains"),
        b"D"
    );
}

#[test]
fn forged_digest_locator_cannot_redirect_read_or_exact_target_deletion() {
    let directory = tempdir().expect("temporary directory");
    let mut store = FsResourceStore::open(directory.path(), "fs:locator").expect("store opens");
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
            if code == "filesystem_upload_record_invalid"
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
        .expect("unrelated file remains present");
}

#[test]
fn short_open_file_import_rejects_before_publication_and_remains_retryable() {
    for source_bytes in [b"A".as_slice(), b"".as_slice()] {
        let directory = tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let source = directory.path().join("source");
        fs::write(&source, source_bytes).expect("short source writes");
        let binding = "fs:short-open-file";
        let mut store = FsResourceStore::open(&root, binding).expect("store opens");
        let intent = ResourceWriteIntent {
            write_id: "import:short-open-file".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).expect("original write begins");
        store
            .write_chunk(&session, 0, b"AB")
            .expect("complete prefix acknowledges");
        let key = session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload key");
        let record_path = root.join("uploads").join(format!("{key}.json"));
        let data_path = root.join("uploads").join(format!("{key}.data"));
        let record_before = fs::read(&record_path).expect("Open record reads");
        drop(store);

        let mut reopened = FsResourceStore::open(&root, binding).expect("store reopens");
        assert!(matches!(
            reopened.import_file(&source, &intent.write_id, &intent.media_type),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
        ));
        let record_after = fs::read(&record_path).expect("rejected record reads");
        let phase: serde_json::Value =
            cymule_core::decode_json(&record_after).expect("record decodes");
        assert_eq!(
            phase["state"], "open",
            "a short retry must not admit Publishing"
        );
        assert_eq!(record_after, record_before);
        assert_eq!(
            fs::read(data_path).expect("acknowledged bytes remain"),
            b"AB"
        );
        assert!(reopened.cleanup_receipt(&session).unwrap().is_none());
        let objects = root
            .join("objects")
            .join(cymule_core::sha256_bytes(binding.as_bytes()));
        assert!(fs::read_dir(objects).unwrap().next().is_none());

        fs::write(&source, b"AB").expect("complete source restores");
        let publication = reopened
            .import_file(&source, &intent.write_id, &intent.media_type)
            .expect("the same Open upload accepts its complete source");
        assert_eq!(reopened.begin_write(&intent).unwrap(), session);
        let receipt = reopened
            .cleanup_receipt(&session)
            .unwrap()
            .expect("cleanup completes");
        assert_eq!(
            reopened
                .import_file(&source, &intent.write_id, &intent.media_type)
                .unwrap(),
            publication
        );
        assert_eq!(reopened.cleanup_receipt(&session).unwrap(), Some(receipt));
    }
}

#[test]
fn short_open_directory_import_rejects_before_publication_and_remains_retryable() {
    for remaining in [1, 0] {
        let directory = tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let source = directory.path().join("source");
        fs::create_dir(&source).expect("source directory creates");
        fs::write(source.join("a"), b"A").expect("first child writes");
        fs::write(source.join("b"), b"B").expect("second child writes");
        let binding = "fs:short-open-directory";
        let mut store = FsResourceStore::open(&root, binding).expect("store opens");
        let entries = ["a", "b"].map(|name| ResourceManifestEntry {
            name: name.to_owned(),
            resource: store
                .import_file(
                    source.join(name),
                    format!("seed:{name}"),
                    "application/octet-stream",
                )
                .expect("child content publishes")
                .resource,
        });
        let manifest_bytes =
            FsResourceStore::encode_manifest(&entries).expect("full manifest encodes");
        let manifest = cymule_resource::SealedResourceManifest::seal(entries.to_vec())
            .expect("full semantic manifest seals");
        let intent = ResourceWriteIntent {
            write_id: "import:short-open-directory".to_owned(),
            shape: ResourceShape::Directory,
            media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).expect("parent write begins");
        store
            .write_chunk(&session, 0, &manifest_bytes)
            .expect("full parent bytes acknowledge");
        let key = session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload key");
        let record_path = root.join("uploads").join(format!("{key}.json"));
        let data_path = root.join("uploads").join(format!("{key}.data"));
        let record_before = fs::read(&record_path).expect("Open parent record reads");
        fs::remove_file(source.join("b")).expect("source loses its final entry");
        if remaining == 0 {
            fs::remove_file(source.join("a")).expect("source becomes empty");
        }
        drop(store);

        let mut reopened = FsResourceStore::open(&root, binding).expect("store reopens");
        assert!(matches!(
            reopened.import_directory(&source, &intent.write_id),
            Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
        ));
        let record_after = fs::read(&record_path).expect("rejected parent record reads");
        let phase: serde_json::Value =
            cymule_core::decode_json(&record_after).expect("record decodes");
        assert_eq!(
            phase["state"], "open",
            "a short manifest must not admit Publishing"
        );
        assert_eq!(record_after, record_before);
        assert_eq!(
            fs::read(data_path).expect("parent bytes remain"),
            manifest_bytes
        );
        assert!(reopened.cleanup_receipt(&session).unwrap().is_none());
        let object = root
            .join("objects")
            .join(cymule_core::sha256_bytes(binding.as_bytes()))
            .join(manifest.descriptor.digest.strip_prefix("sha256:").unwrap());
        assert!(
            !object.exists(),
            "the unmatched parent manifest must stay unpublished"
        );

        fs::write(source.join("a"), b"A").expect("first source entry restores");
        fs::write(source.join("b"), b"B").expect("second source entry restores");
        let publication = reopened
            .import_directory(&source, &intent.write_id)
            .expect("the same Open parent accepts its complete manifest");
        assert_eq!(
            publication.resource.manifest.as_ref(),
            Some(&manifest.descriptor)
        );
        let receipt = reopened
            .cleanup_receipt(&session)
            .unwrap()
            .expect("parent cleanup completes");
        assert_eq!(
            reopened
                .import_directory(&source, &intent.write_id)
                .unwrap(),
            publication
        );
        assert_eq!(reopened.cleanup_receipt(&session).unwrap(), Some(receipt));
    }
}

#[test]
fn import_input_mismatch_does_not_hide_acknowledged_or_published_byte_corruption() {
    for committed in [false, true] {
        let directory = tempdir().expect("temporary directory");
        let root = directory.path().join("store");
        let source = directory.path().join("empty-source");
        fs::write(&source, b"").expect("empty source writes");
        let mut store = FsResourceStore::open(&root, "fs:import-corruption").expect("store opens");
        let intent = ResourceWriteIntent {
            write_id: "import:corrupted-frontier".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        };
        let session = store.begin_write(&intent).expect("write begins");
        store
            .write_chunk(&session, 0, b"AB")
            .expect("bytes acknowledge");
        let key = session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload key");
        let expected_code = if committed {
            let publication = store.commit_write(&session).expect("publication commits");
            fs::write(resource_object_path(&root, &publication.resource), b"AC")
                .expect("same-size published corruption seeds");
            "filesystem_resource_integrity_failed"
        } else {
            OpenOptions::new()
                .write(true)
                .open(root.join("uploads").join(format!("{key}.data")))
                .expect("acknowledged data opens")
                .set_len(1)
                .expect("acknowledged byte loss seeds");
            "filesystem_upload_record_invalid"
        };
        let record_path = root.join("uploads").join(format!("{key}.json"));
        let record_before = fs::read(&record_path).expect("record reads");
        let receipt = store.cleanup_receipt(&session).unwrap();
        assert!(matches!(
            store.import_file(&source, &intent.write_id, &intent.media_type),
            Err(ResourceError::Integrity { code, .. }) if code == expected_code
        ));
        assert_eq!(fs::read(record_path).unwrap(), record_before);
        assert_eq!(store.cleanup_receipt(&session).unwrap(), receipt);
    }
}

#[cfg(unix)]
#[test]
fn import_input_mismatch_does_not_hide_persisted_record_metadata_corruption() {
    let directory = tempdir().expect("temporary directory");
    let committed_root = directory.path().join("committed-store");
    let committed_source = directory.path().join("committed-source");
    fs::write(&committed_source, b"AB").expect("complete source writes");
    let committed_binding = "fs:committed-metadata-corruption";
    let committed_write_id = "import:committed-metadata-corruption";
    let mut committed =
        FsResourceStore::open(&committed_root, committed_binding).expect("committed store opens");
    committed
        .import_file(
            &committed_source,
            committed_write_id,
            "application/octet-stream",
        )
        .expect("file import commits");
    fs::write(&committed_source, b"").expect("short replay source writes");
    let committed_record = only_path_with_extension(&committed_root.join("uploads"), "json");
    let mut committed_value: serde_json::Value =
        cymule_core::decode_json(&fs::read(&committed_record).expect("committed record reads"))
            .expect("committed record decodes");
    let committed_cleanup = committed_value["cleanup_receipt"].clone();
    committed_value["intent"]["media_type"] = serde_json::json!("INVALID");
    let committed_corruption =
        cymule_core::canonical_bytes(&committed_value).expect("corrupted record canonicalizes");
    fs::write(&committed_record, &committed_corruption)
        .expect("canonical committed corruption writes");
    let committed_error = committed.import_file(
        &committed_source,
        committed_write_id,
        "application/octet-stream",
    );
    assert!(
        matches!(
            &committed_error,
            Err(ResourceError::Integrity { code, .. })
                if code == "filesystem_upload_record_invalid"
        ),
        "unexpected committed metadata result: {committed_error:?}"
    );
    assert_eq!(
        fs::read(&committed_record).expect("committed corruption remains readable"),
        committed_corruption
    );
    let committed_after: serde_json::Value = cymule_core::decode_json(&committed_corruption)
        .expect("committed corruption remains canonical");
    assert_eq!(committed_after["cleanup_receipt"], committed_cleanup);

    let publishing_root = directory.path().join("publishing-store");
    let publishing_source = directory.path().join("publishing-source");
    fs::create_dir(&publishing_source).expect("publishing source creates");
    fs::write(publishing_source.join("a"), b"A").expect("publishing child writes");
    let publishing_binding = "fs:publishing-metadata-corruption";
    let publishing_write_id = "import:publishing-metadata-corruption";
    let (publishing, publishing_session, _) = leave_directory_import_publishing(
        &publishing_root,
        &publishing_source,
        publishing_binding,
        publishing_write_id,
    );
    fs::remove_file(publishing_source.join("a")).expect("short directory replay source writes");
    let publishing_key = publishing_session
        .upload_id
        .strip_prefix("upload:")
        .expect("Publishing upload key");
    let publishing_record = publishing_root
        .join("uploads")
        .join(format!("{publishing_key}.json"));
    let mut publishing_value: serde_json::Value =
        cymule_core::decode_json(&fs::read(&publishing_record).expect("Publishing record reads"))
            .expect("Publishing record decodes");
    assert_eq!(publishing_value["state"], "publishing");
    let publishing_cleanup = publishing_value["cleanup_receipt"].clone();
    publishing_value["publication"]["manifest"]["manifest_version"] =
        serde_json::json!("cymule.resource-manifest/corrupt");
    let publishing_corruption =
        cymule_core::canonical_bytes(&publishing_value).expect("corrupted record canonicalizes");
    fs::write(&publishing_record, &publishing_corruption)
        .expect("canonical Publishing corruption writes");
    drop(publishing);

    let mut reopened = FsResourceStore::open(&publishing_root, publishing_binding)
        .expect("Publishing store reopens");
    let publishing_error = reopened.import_directory(&publishing_source, publishing_write_id);
    assert!(
        matches!(
            &publishing_error,
            Err(ResourceError::Integrity { code, .. })
                if code == "filesystem_upload_record_invalid"
        ),
        "unexpected Publishing metadata result: {publishing_error:?}"
    );
    assert_eq!(
        fs::read(&publishing_record).expect("Publishing corruption remains readable"),
        publishing_corruption
    );
    let publishing_after: serde_json::Value = cymule_core::decode_json(&publishing_corruption)
        .expect("Publishing corruption remains canonical");
    assert_eq!(publishing_after["cleanup_receipt"], publishing_cleanup);
}

#[cfg(unix)]
#[test]
fn directory_import_reopens_publishing_manifest_and_finishes_its_index() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let source = directory.path().join("source");
    fs::create_dir(&source).expect("source directory creates");
    fs::write(source.join("a"), b"A").expect("source child writes");
    let binding = "fs:directory-publishing-recovery";
    let write_id = "import:directory-publishing-recovery";
    let (store, session, descriptor) =
        leave_directory_import_publishing(&root, &source, binding, write_id);
    let key = session
        .upload_id
        .strip_prefix("upload:")
        .expect("parent upload key");
    let record_path = root.join("uploads").join(format!("{key}.json"));
    let record: serde_json::Value =
        cymule_core::decode_json(&fs::read(&record_path).expect("Publishing record reads"))
            .expect("Publishing record decodes");
    assert_eq!(record["state"], "publishing");
    assert!(record["cleanup_receipt"].is_null());
    let index_name = descriptor.digest.strip_prefix("sha256:").unwrap();
    let index_root = only_binding_namespace(&root.join("manifest-indexes"));
    assert!(
        !index_root.join(index_name).exists(),
        "interrupted Publishing must not claim a manifest index"
    );
    drop(store);

    let mut reopened = FsResourceStore::open(&root, binding).expect("Publishing store reopens");
    let publication = reopened
        .import_directory(&source, write_id)
        .expect("public directory import converges Publishing");
    assert_eq!(publication.resource.manifest.as_ref(), Some(&descriptor));
    let page = reopened
        .list(&publication.resource, &publication.locators, None, 8)
        .expect("recovered manifest index lists");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name, "a");
    assert!(page.next_cursor.is_none());
    assert!(index_root.join(index_name).is_dir());
    assert!(
        reopened
            .cleanup_receipt(&session)
            .expect("cleanup query succeeds")
            .is_some()
    );
    let terminal: serde_json::Value =
        cymule_core::decode_json(&fs::read(record_path).expect("terminal record reads"))
            .expect("terminal record decodes");
    assert_eq!(terminal["state"], "committed");
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
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
    ));
    fs::write(&source, b"one\n").expect("source truncates");
    assert!(matches!(
        store.import_file(&source, "import:suite", "application/x-ndjson"),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
    ));
}

#[test]
fn recursive_directory_import_lists_bounded_sorted_pages() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let source = directory.path().join("source");
    fs::create_dir_all(source.join("nested")).expect("source creates");
    fs::write(source.join("b.txt"), b"b").expect("b writes");
    fs::write(source.join("a.txt"), b"a").expect("a writes");
    fs::write(source.join("nested/c.txt"), b"c").expect("c writes");
    let mut store = FsResourceStore::open(&root, "fs:test").expect("store opens");
    let resource = store
        .import_directory(&source, "import:source")
        .expect("directory imports");
    let first = store
        .list(&resource.resource, &resource.locators, None, 2)
        .expect("first page lists");
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["a.txt", "b.txt"]
    );
    let cursor = first.next_cursor.clone().expect("first page continues");
    drop(store);
    let mut reopened = FsResourceStore::open(&root, "fs:test").expect("store reopens");
    let second = reopened
        .list(&resource.resource, &resource.locators, Some(&cursor), 2)
        .expect("second page lists after restart");
    assert_eq!(second.entries[0].name, "nested");
    assert_eq!(
        second
            .proof
            .predecessor
            .as_ref()
            .expect("non-initial page proves its predecessor")
            .entry
            .name,
        "b.txt"
    );
    assert!(second.next_cursor.is_none());
}

#[test]
fn directory_import_child_write_identity_cannot_alias_another_root() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let first_source = directory.path().join("first-source");
    let second_source = directory.path().join("second-source");
    fs::create_dir_all(first_source.join("a")).expect("first nested source creates");
    fs::create_dir(&second_source).expect("second source creates");
    fs::write(first_source.join("a/b.txt"), b"first source").expect("first child writes");
    fs::write(second_source.join("b.txt"), b"other source").expect("different second child writes");

    let mut store = FsResourceStore::open(&root, "fs:write-collision").expect("store opens");
    let first = store
        .import_directory(&first_source, "import:root")
        .expect("first root and its nested child import");
    let second = store
        .import_directory(&second_source, "import:root/a")
        .expect("caller root identity remains distinct from the first root's derived child");
    drop(store);

    let mut reopened = FsResourceStore::open(&root, "fs:write-collision").expect("store reopens");
    assert_eq!(
        reopened
            .import_directory(&first_source, "import:root")
            .expect("first nested import replays after reopen"),
        first
    );
    assert_eq!(
        reopened
            .import_directory(&second_source, "import:root/a")
            .expect("second root import replays independently after reopen"),
        second
    );
}

#[test]
fn recursive_directory_import_replays_with_a_maximum_unicode_write_id() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let source = directory.path().join("source");
    fs::create_dir_all(source.join("nested")).expect("nested source creates");
    fs::write(source.join("root.txt"), b"root").expect("root child writes");
    fs::write(source.join("nested/child.txt"), b"nested").expect("nested child writes");
    let write_id = "界".repeat(512);
    assert_eq!(write_id.chars().count(), 512);
    assert!(write_id.len() > 512);

    let mut store = FsResourceStore::open(&root, "fs:maximum-write").expect("store opens");
    let first = store
        .import_directory(&source, write_id.as_str())
        .expect("maximum-scalar write ID imports recursively");
    first.verify().expect("initial publication verifies");
    drop(store);

    let mut reopened = FsResourceStore::open(&root, "fs:maximum-write").expect("store reopens");
    let replay = reopened
        .import_directory(&source, write_id.as_str())
        .expect("maximum-scalar write ID replays recursively after reopen");
    assert_eq!(replay, first);
}

#[test]
fn deep_directory_import_replays_after_reopen_and_rejects_changed_leaf() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let source = directory.path().join("source");
    fs::create_dir(&source).expect("source creates");
    let write_id = "import:deep";
    let mut leaf_directory = source.clone();
    let mut concatenated_identity_length = write_id.chars().count();
    for depth in 0..9 {
        let name = format!("level-{depth:02}-{}", "x".repeat(55));
        concatenated_identity_length += 1 + name.chars().count();
        leaf_directory.push(name);
        fs::create_dir(&leaf_directory).expect("deep source directory creates");
    }
    assert!(concatenated_identity_length > 512);
    let leaf = leaf_directory.join("leaf.txt");
    fs::write(&leaf, b"leaf-before").expect("deep source leaf writes");

    let mut store = FsResourceStore::open(&root, "fs:deep-write").expect("store opens");
    let first = store
        .import_directory(&source, write_id)
        .expect("deep source imports with bounded child write identities");
    drop(store);

    let mut reopened = FsResourceStore::open(&root, "fs:deep-write").expect("store reopens");
    let replay = reopened
        .import_directory(&source, write_id)
        .expect("deep source replays recursively after reopen");
    assert_eq!(replay, first);

    fs::write(&leaf, b"leaf-after!").expect("deep source leaf changes at the same size");
    assert!(matches!(
        reopened.import_directory(&source, write_id),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
    ));
}

#[test]
fn deep_single_chain_import_has_an_exact_validation_depth_bound_and_reopens() {
    let directory = tempdir().expect("temporary directory");
    let maximum_source = directory.path().join("maximum-source");
    create_single_directory_chain(&maximum_source, MAX_DIRECTORY_IMPORT_DEPTH);
    let maximum_root = directory.path().join("maximum-store");
    let mut maximum =
        FsResourceStore::open(&maximum_root, "fs:maximum-depth").expect("maximum store opens");
    let publication = maximum
        .import_directory(&maximum_source, "import:maximum-depth")
        .expect("the exact maximum depth imports");
    drop(maximum);
    let mut reopened =
        FsResourceStore::open(&maximum_root, "fs:maximum-depth").expect("maximum store reopens");
    assert_eq!(
        reopened
            .import_directory(&maximum_source, "import:maximum-depth")
            .expect("the exact maximum depth replays"),
        publication
    );

    let overflow_source = directory.path().join("overflow-source");
    create_single_directory_chain(&overflow_source, MAX_DIRECTORY_IMPORT_DEPTH + 1);
    let overflow_root = directory.path().join("overflow-store");
    let mut overflow =
        FsResourceStore::open(&overflow_root, "fs:overflow-depth").expect("overflow store opens");
    for error in [
        overflow.import_directory(&overflow_source, "import:overflow-depth"),
        overflow.import_directory(&overflow_source, "import:overflow-depth"),
    ] {
        let expected = format!(
            "filesystem_import_depth_exceeded: filesystem directory import exceeds {MAX_DIRECTORY_IMPORT_DEPTH} nested child directories"
        );
        assert!(
            matches!(
                &error,
                Err(ResourceError::Validation(message)) if message == &expected
            ),
            "unexpected overflow result: {error:?}"
        );
    }
    drop(overflow);
    let mut overflow_reopened =
        FsResourceStore::open(&overflow_root, "fs:overflow-depth").expect("overflow store reopens");
    let expected = format!(
        "filesystem_import_depth_exceeded: filesystem directory import exceeds {MAX_DIRECTORY_IMPORT_DEPTH} nested child directories"
    );
    assert!(matches!(
        overflow_reopened.import_directory(&overflow_source, "import:overflow-depth"),
        Err(ResourceError::Validation(message)) if message == expected
    ));
}

#[test]
fn directory_import_replay_rejects_a_truncated_manifest() {
    let directory = tempdir().expect("temporary directory");
    let source = directory.path().join("source-replay");
    fs::create_dir(&source).expect("source creates");
    fs::write(source.join("a.txt"), b"a").expect("first child writes");
    fs::write(source.join("b.txt"), b"b").expect("second child writes");
    let mut store =
        FsResourceStore::open(directory.path().join("store"), "fs:replay").expect("store opens");
    store
        .import_directory(&source, "import:replay")
        .expect("initial directory imports");

    fs::remove_file(source.join("b.txt")).expect("source truncates");
    assert!(matches!(
        store.import_directory(&source, "import:replay"),
        Err(ResourceError::Conflict { code, .. }) if code == "filesystem_import_conflict"
    ));
}

#[test]
fn empty_directory_manifest_has_a_bounded_empty_index_page() {
    let directory = tempdir().expect("temporary directory");
    let source = directory.path().join("empty-source");
    fs::create_dir(&source).expect("empty source creates");
    let mut store =
        FsResourceStore::open(directory.path().join("store"), "fs:empty").expect("store opens");
    let publication = store
        .import_directory(&source, "import:empty")
        .expect("empty directory imports");
    store
        .stat(&publication.resource, &publication.locators)
        .expect("empty manifest and index stat verify");
    let page = store
        .list(&publication.resource, &publication.locators, None, 1)
        .expect("empty page lists");
    assert!(page.entries.is_empty());
    assert!(page.next_cursor.is_none());
    page.proof
        .verify_page(
            publication.resource.manifest.as_ref().unwrap(),
            &[],
            None,
            None,
        )
        .expect("empty page proof verifies");
}

#[test]
fn manifest_index_reads_only_the_requested_page_and_retains_merkle_proof() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let source = directory.path().join("source-indexed");
    fs::create_dir(&source).expect("source creates");
    for name in ["a.txt", "b.txt", "c.txt"] {
        fs::write(source.join(name), name.as_bytes()).expect("child writes");
    }
    let mut store = FsResourceStore::open(&root, "fs:indexed").expect("store opens");
    let publication = store
        .import_directory(&source, "import:indexed")
        .expect("directory imports");
    let descriptor = publication
        .resource
        .manifest
        .as_ref()
        .expect("manifest descriptor exists");
    let index = find_unique_nested_path(
        &root.join("manifest-indexes"),
        descriptor
            .digest
            .strip_prefix("sha256:")
            .expect("manifest digest prefix"),
    );
    assert!(index.join("offsets.bin").is_file());
    assert!(index.join("nodes.bin").is_file());
    assert!(
        store
            .get_catalog_record("cymule.resource-fs-manifest-index/3", &descriptor.digest)
            .expect("manifest index catalog reads")
            .is_some()
    );

    let manifest = resource_object_path(&root, &publication.resource);
    let mut bytes = fs::read(&manifest).expect("manifest reads");
    let original_manifest = bytes.clone();
    let second = bytes
        .windows(5)
        .position(|window| window == b"b.txt")
        .expect("second entry exists");
    bytes[second] = b'z';
    fs::write(&manifest, bytes).expect("unrequested line is tampered in place");

    let first = store
        .list(&publication.resource, &publication.locators, None, 1)
        .expect("first bounded page does not scan the later tampered line");
    assert_eq!(first.entries[0].name, "a.txt");
    first
        .proof
        .verify_page(
            descriptor,
            &first.entries,
            None,
            first.next_cursor.as_deref(),
        )
        .expect("first page retains an exact Merkle proof");
    let cursor = ResourceListCursor::decode(
        first
            .next_cursor
            .as_deref()
            .expect("nonterminal page has a cursor"),
    )
    .expect("self-contained cursor verifies");
    assert_eq!(cursor.resource_id, publication.resource.resource_id);
    assert_eq!(cursor.next_index, 1);
    assert_eq!(cursor.last_name, "a.txt");
    drop(store);
    let mut reopened = FsResourceStore::open(&root, "fs:indexed").expect("store reopens");
    let tampered_page = reopened.list(
        &publication.resource,
        &publication.locators,
        first.next_cursor.as_deref(),
        1,
    );
    assert!(
        matches!(
            &tampered_page,
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_manifest_inclusion_root_mismatch"
        ),
        "unexpected tampered-page result: {tampered_page:?}"
    );

    fs::write(&manifest, original_manifest).expect("manifest bytes restore");
    let nodes_path = index.join("nodes.bin");
    let mut nodes = fs::read(&nodes_path).expect("Merkle index reads");
    nodes[32] ^= 0xff;
    fs::write(nodes_path, nodes).expect("Merkle sibling tamper writes");
    assert!(matches!(
        reopened.list(&publication.resource, &publication.locators, None, 1),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_manifest_inclusion_root_mismatch"
    ));
}

#[test]
fn list_client_accepts_a_complete_first_manifest_page() {
    let directory = tempdir().expect("temporary directory");
    for (case, shape, entry_count) in [
        ("empty", ResourceShape::Directory, 0),
        ("single", ResourceShape::Collection, 1),
        ("full", ResourceShape::Snapshot, 4),
    ] {
        let mut store = FsResourceStore::open(directory.path().join(case), "fs:first-page")
            .expect("store opens");
        let child = ResourceCandidate::text("child")
            .seal()
            .expect("child seals");
        let entries = (0..entry_count)
            .map(|index| ResourceManifestEntry {
                name: format!("{index:04}-entry"),
                resource: child.clone(),
            })
            .collect::<Vec<_>>();
        let bytes = FsResourceStore::encode_manifest(&entries).expect("manifest seals");
        let session = store
            .begin_write(&ResourceWriteIntent {
                write_id: format!("write:first-page-{case}"),
                shape,
                media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
                annotations: BTreeMap::new(),
            })
            .expect("manifest write begins");
        if !bytes.is_empty() {
            store
                .write_chunk(&session, 0, &bytes)
                .expect("manifest writes");
        }
        let publication = store.commit_write(&session).expect("manifest commits");
        let mut client = ResourceClient::new(store);
        let page = client
            .list_page(&publication, None, 4)
            .expect("a complete first page is terminal, not stalled");
        assert_eq!(page.entries, entries);
        assert!(page.next_cursor.is_none());
    }
}

#[test]
fn manifest_pages_stop_before_the_canonical_byte_bound() {
    let directory = tempdir().expect("temporary directory");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:page-bytes").expect("store opens");
    let mut child_candidate = ResourceCandidate::text("bounded child");
    child_candidate.annotations = (0..64)
        .map(|index| (format!("large-{index:03}"), "🧪".repeat(3800)))
        .collect();
    let child = child_candidate.seal().expect("large bounded child seals");
    let entries = (0..9)
        .map(|index| ResourceManifestEntry {
            name: format!("{index:04}-entry"),
            resource: child.clone(),
        })
        .collect::<Vec<_>>();
    let bytes = FsResourceStore::encode_manifest(&entries).expect("large bounded manifest seals");
    assert!(bytes.len() as u64 > MAX_MANIFEST_PAGE_BYTES);
    let intent = ResourceWriteIntent {
        write_id: "write:page-byte-bound".to_owned(),
        shape: ResourceShape::Directory,
        media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("manifest write begins");
    for (index, chunk) in bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
        store
            .write_chunk(&session, (index * MAX_WRITE_CHUNK) as u64, chunk)
            .expect("manifest chunk writes");
    }
    let publication = store.commit_write(&session).expect("manifest commits");
    let mut client = ResourceClient::new(store);
    let first = client
        .list_page(&publication, None, 1000)
        .expect("first bounded-byte page lists");
    assert!(!first.entries.is_empty());
    assert!(first.entries.len() < entries.len());
    let first_bytes = first
        .entries
        .iter()
        .map(|entry| cymule_core::canonical_bytes(entry).unwrap().len() as u64 + 1)
        .sum::<u64>();
    assert!(first_bytes <= MAX_MANIFEST_PAGE_BYTES);
    let second = client
        .list_page(&publication, first.next_cursor.as_deref(), 1000)
        .expect("second bounded-byte page lists");
    assert_eq!(first.entries.len() + second.entries.len(), entries.len());
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
    let commit_error = store.commit_write(&session);
    assert!(
        matches!(
            &commit_error,
            Err(ResourceError::Integrity { code, .. }) if code == "filesystem_manifest_invalid"
        ),
        "unexpected malformed-manifest result: {commit_error:?}"
    );
}

#[test]
fn manifest_ingress_rejects_an_oversized_line_at_the_streaming_cap() {
    let directory = tempdir().expect("temporary directory");
    let mut store =
        FsResourceStore::open(directory.path(), "fs:manifest-cap").expect("store opens");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:manifest-cap".to_owned(),
            shape: ResourceShape::Directory,
            media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    let oversized = vec![b'x'; MAX_MANIFEST_ENTRY_BYTES + 1];
    for (index, chunk) in oversized.chunks(MAX_WRITE_CHUNK).enumerate() {
        store
            .write_chunk(&session, (index * MAX_WRITE_CHUNK) as u64, chunk)
            .expect("bounded chunks stage");
    }
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_manifest_invalid"
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
fn same_size_object_and_manifest_tampering_fail_content_verification() {
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
        store.stat(&object.resource, &object.locators),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
    ));
    assert!(matches!(
        store.commit_write(&session),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
    ));
    assert!(matches!(
        store.write_chunk(&session, 0, b"abcdefgh"),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
    ));
    let mut copied = Vec::new();
    assert!(matches!(
        ResourceClient::new(store).copy_to(&object, 3, &mut copied),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
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
    let manifest_path = resource_object_path(&directory_root, &manifest.resource);
    let mut manifest_bytes = fs::read(&manifest_path).expect("manifest reads");
    let position = manifest_bytes
        .windows(5)
        .position(|window| window == b"a.txt")
        .expect("manifest child name exists");
    manifest_bytes[position] = b'b';
    fs::write(&manifest_path, manifest_bytes).expect("same-size manifest tamper writes");
    assert!(matches!(
        directory_store.stat(&manifest.resource, &manifest.locators),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
    ));
    assert!(matches!(
        ResourceClient::new(directory_store.clone()).copy_to(&manifest, 17, &mut Vec::new()),
        Err(ResourceError::Integrity { code, .. })
            if code == "filesystem_resource_integrity_failed"
    ));
    let tampered_list = directory_store.list(&manifest.resource, &manifest.locators, None, 8);
    assert!(
        matches!(
            &tampered_list,
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_manifest_inclusion_root_mismatch"
        ),
        "unexpected tampered-list result: {tampered_list:?}"
    );
}

#[test]
fn commit_replay_verifies_existing_object_and_removes_owned_staging() {
    let directory = tempdir().expect("temporary directory creates");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:test").expect("store opens");
    let first = ResourceWriteIntent {
        write_id: "write:first-publication".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let first_session = store.begin_write(&first).expect("first write begins");
    store
        .write_chunk(&first_session, 0, b"shared bytes")
        .expect("first bytes write");
    let first_publication = store.commit_write(&first_session).expect("first commits");

    let second = ResourceWriteIntent {
        write_id: "write:already-exists".to_owned(),
        ..first
    };
    let second_session = store.begin_write(&second).expect("second write begins");
    store
        .write_chunk(&second_session, 0, b"shared bytes")
        .expect("second bytes write");
    let staging = root.join("staging").join(format!(
        "object-{}",
        second_session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload prefix")
    ));
    fs::write(&staging, b"stale staging bytes").expect("stale staging fixture writes");
    let second_publication = store.commit_write(&second_session).expect("second commits");
    assert_eq!(
        second_publication.resource.resource_id,
        first_publication.resource.resource_id
    );
    assert!(!staging.exists(), "owned staging object must be removed");
    let upload_data = root.join("uploads").join(format!(
        "{}.data",
        second_session
            .upload_id
            .strip_prefix("upload:")
            .expect("upload prefix")
    ));
    assert!(
        !upload_data.exists(),
        "committed upload bytes must be cleaned"
    );

    let replay = store.commit_write(&second_session).expect("commit replays");
    assert_eq!(replay, second_publication);
    assert!(!staging.exists());
    assert!(!upload_data.exists());
}

#[test]
fn abort_returns_verified_cleanup_receipt() {
    let directory = tempdir().expect("temporary directory creates");
    let root = directory.path().join("store");
    let mut store = FsResourceStore::open(&root, "fs:test").expect("store opens");
    let intent = ResourceWriteIntent {
        write_id: "write:abort-cleanup".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("write begins");
    store
        .write_chunk(&session, 0, b"staged chunk")
        .expect("chunk stages");
    let receipt = store.abort_write(&session).expect("abort cleans");
    receipt.verify().expect("cleanup receipt verifies");
    assert!(receipt.verified_absent);
    assert_eq!(receipt.removed_staging_objects, 3);
    let replay = store.abort_write(&session).expect("abort replays");
    replay.verify().expect("replayed cleanup verifies");
    assert_eq!(replay, receipt);
    assert_eq!(
        store
            .cleanup_receipt(&session)
            .expect("cleanup receipt query succeeds"),
        Some(receipt)
    );
}

#[cfg(unix)]
#[test]
fn cleanup_plan_survives_a_provider_failure_before_terminal_receipt() {
    let directory = tempdir().expect("temporary directory creates");
    let root = directory.path().join("cleanup-plan-recovery");
    let mut store = FsResourceStore::open(&root, "fs:cleanup-plan-recovery").expect("store opens");
    let session = store
        .begin_write(&ResourceWriteIntent {
            write_id: "write:cleanup-plan-recovery".to_owned(),
            shape: ResourceShape::Object,
            media_type: "application/octet-stream".to_owned(),
            annotations: BTreeMap::new(),
        })
        .expect("write begins");
    store
        .write_chunk(&session, 0, b"planned cleanup")
        .expect("upload bytes persist");
    let key = session
        .upload_id
        .strip_prefix("upload:")
        .expect("upload prefix");
    let manifest_staging = root.join("staging").join(format!("manifest-index-{key}"));
    fs::create_dir(&manifest_staging).expect("owned manifest staging directory seeds");
    fs::write(manifest_staging.join("unexpected"), b"injected failure")
        .expect("unexpected staging entry seeds");
    let cleanup_error = store.abort_write(&session);
    assert!(
        matches!(
            &cleanup_error,
            Err(ResourceError::Integrity { code, .. }) if code == "filesystem_cleanup_invalid"
        ),
        "unexpected cleanup result: {cleanup_error:?}"
    );

    let record_path = only_path_with_extension(&root.join("uploads"), "json");
    let record: serde_json::Value =
        cymule_core::decode_json(&fs::read(&record_path).expect("cleanup authority record reads"))
            .expect("cleanup authority record decodes");
    assert!(record["cleanup_plan"].is_object());
    assert!(record["cleanup_receipt"].is_null());
    fs::remove_file(manifest_staging.join("unexpected")).expect("injected staging failure repairs");
    drop(store);

    let mut reopened =
        FsResourceStore::open(&root, "fs:cleanup-plan-recovery").expect("store reopens");
    let receipt = reopened
        .abort_write(&session)
        .expect("persisted cleanup plan converges");
    assert_eq!(receipt.removed_staging_objects, 3);
    assert_eq!(
        reopened
            .cleanup_receipt(&session)
            .expect("terminal receipt query succeeds"),
        Some(receipt)
    );
}

#[cfg(unix)]
#[test]
fn resource_nonregular_entry_worker() {
    let Ok(root) = std::env::var("CYMULE_RESOURCE_NONREGULAR_ROOT") else {
        return;
    };
    let phase = std::env::var("CYMULE_RESOURCE_NONREGULAR_PHASE").expect("phase exists");
    let marker = std::env::var("CYMULE_RESOURCE_NONREGULAR_MARKER").expect("marker exists");
    let result = match phase.as_str() {
        "read" => FsResourceStore::open_read_only(&root, "fs:nonregular").map(|_| ()),
        "write" => FsResourceStore::open(&root, "fs:nonregular")
            .expect("store opens")
            .begin_write(&process_kill_intent())
            .map(|_| ()),
        _ => panic!("unexpected nonregular phase"),
    };
    assert!(matches!(
        result,
        Err(ResourceError::Integrity { code, .. }) if code == "filesystem_layout_invalid"
    ));
    fs::write(marker, b"rejected").expect("rejection barrier writes");
    loop {
        thread::park_timeout(Duration::from_mins(1));
    }
}

#[cfg(unix)]
#[test]
fn public_file_boundaries_reject_fifos_without_waiting_for_peers() {
    for phase in ["read", "write"] {
        let world = TestWorld::new(u64::from(phase == "write")).expect("test world creates");
        let root = world.domain().path("store").expect("store path resolves");
        let marker = world
            .domain()
            .path("rejected")
            .expect("marker path resolves");
        let mut store = FsResourceStore::open(&root, "fs:nonregular").expect("store opens");
        let fifo = if phase == "read" {
            let path = root.join("layout.json");
            fs::remove_file(&path).expect("replace the isolated fixture marker");
            path
        } else {
            let session = store
                .begin_write(&process_kill_intent())
                .expect("identity admits");
            let key = session
                .upload_id
                .strip_prefix("upload:")
                .expect("upload key");
            fs::remove_file(root.join("uploads").join(format!("{key}.json")))
                .expect("remove the isolated fixture upload record");
            root.join("staging").join(format!("record-{key}"))
        };
        drop(store);
        nix::unistd::mkfifo(
            &fifo,
            nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
        )
        .expect("isolated nonregular entry creates");
        let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
        command
            .args(["--exact", "resource_nonregular_entry_worker", "--nocapture"])
            .env("CYMULE_RESOURCE_NONREGULAR_ROOT", &root)
            .env("CYMULE_RESOURCE_NONREGULAR_PHASE", phase)
            .env("CYMULE_RESOURCE_NONREGULAR_MARKER", &marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        let mut child = ManagedChild::spawn(&mut command).expect("boundary worker starts");
        child
            .wait_for_content(&marker, b"rejected", Duration::from_secs(5))
            .expect("nonregular entry rejects without waiting for a FIFO peer");
        child.terminate().expect("boundary worker is reaped");
        assert!(child.is_reaped());
    }
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
            &resource.resource.integrity,
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

#[cfg(unix)]
fn leave_directory_import_publishing(
    root: &Path,
    source: &Path,
    binding: &str,
    write_id: &str,
) -> (
    FsResourceStore,
    ResourceWriteSession,
    ResourceManifestDescriptor,
) {
    use std::os::unix::fs::PermissionsExt as _;

    let mut store = FsResourceStore::open(root, binding).expect("store opens");
    let child_write_id = cymule_core::content_id(
        "cymule.resource-fs-child-write/1",
        &serde_json::json!({
            "child_name": "a",
            "parent_write_id": write_id,
        }),
    )
    .expect("child write identity derives");
    let child = store
        .import_file(source.join("a"), child_write_id, "application/octet-stream")
        .expect("child import publishes");
    let entry = ResourceManifestEntry {
        name: "a".to_owned(),
        resource: child.resource,
    };
    let manifest_bytes =
        FsResourceStore::encode_manifest(std::slice::from_ref(&entry)).expect("manifest encodes");
    let descriptor = cymule_resource::SealedResourceManifest::seal(vec![entry])
        .expect("semantic manifest seals")
        .descriptor;
    let intent = ResourceWriteIntent {
        write_id: write_id.to_owned(),
        shape: ResourceShape::Directory,
        media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent).expect("parent write begins");
    store
        .write_chunk(&session, 0, &manifest_bytes)
        .expect("complete parent manifest acknowledges");

    let manifest_indexes = only_binding_namespace(&root.join("manifest-indexes"));
    let original_permissions = fs::metadata(&manifest_indexes)
        .expect("manifest-index namespace metadata reads")
        .permissions();
    fs::set_permissions(&manifest_indexes, fs::Permissions::from_mode(0o500))
        .expect("manifest-index namespace becomes read-only");
    let interrupted = store.import_directory(source, write_id);
    fs::set_permissions(&manifest_indexes, original_permissions)
        .expect("manifest-index namespace becomes writable again");
    assert!(
        matches!(
            &interrupted,
            Err(ResourceError::Substrate { code, .. }) if code == "filesystem_io_failure"
        ),
        "unexpected interrupted directory publication: {interrupted:?}"
    );
    (store, session, descriptor)
}

#[cfg(unix)]
fn only_binding_namespace(root: &Path) -> PathBuf {
    let paths = fs::read_dir(root)
        .expect("physical family root reads")
        .map(|entry| entry.expect("binding namespace reads").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "expected exactly one binding namespace");
    paths
        .into_iter()
        .next()
        .expect("one binding namespace exists")
}

fn create_single_directory_chain(root: &Path, nested_depth: usize) {
    fs::create_dir(root).expect("chain root creates");
    let mut current = root.to_path_buf();
    for _ in 0..nested_depth {
        current.push("d");
        fs::create_dir(&current).expect("nested chain directory creates");
    }
    fs::write(current.join("leaf"), b"deep leaf").expect("deep chain leaf writes");
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
        .flat_map(|entry| {
            let path = entry.expect("directory entry reads").path();
            if path.is_dir() {
                fs::read_dir(path)
                    .expect("binding namespace reads")
                    .map(|entry| entry.expect("object entry reads").path())
                    .collect::<Vec<_>>()
            } else {
                vec![path]
            }
        })
        .filter(|path| path.is_file() && path.extension().is_none())
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
    find_unique_nested_path(
        &root.join("objects"),
        digest
            .strip_prefix("sha256:")
            .expect("digest prefix exists"),
    )
}

fn find_unique_nested_path(root: &Path, name: &str) -> std::path::PathBuf {
    let paths = fs::read_dir(root)
        .expect("physical namespace root reads")
        .filter_map(|entry| {
            let candidate = entry
                .expect("physical namespace entry reads")
                .path()
                .join(name);
            candidate.exists().then_some(candidate)
        })
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 1, "expected one nested physical path {name}");
    paths.into_iter().next().expect("one nested path exists")
}
