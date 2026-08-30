//! Fault-oriented resource descriptor, resolver, store, and handoff tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use base64::Engine as _;
use cymule_core::{
    ComponentContract, Definition, Expression, Operation, PlanCandidate, Region, Step, WaitSpec,
    content_id, sha256_bytes,
};
use cymule_durable::{
    ClockObservationAuthority, ComponentOccurrence, DURABLE_CONTROL_VERSION, DurableBoundary,
    DurableCommand, DurableError, DurableResponse, DurableResult, DurableRunCurrent,
    DurableRunItem, DurableRunItemSelector, DurableRuntimeControl, DurableStore,
    DurableStoreControl, ExecutionClockAuthority, MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    MAX_DURABLE_QUERY_PAGE_BYTES, MAX_DURABLE_QUERY_PAGE_ITEMS, MemoryStore, StoreCommit,
    WaitCondition, WaitState,
};
use cymule_durable_protocol::{
    CLOCK_OBSERVATION_VERSION, ClockObservation, ClockObservationRef, ContinuationStatus,
    ExecutionClaimRequest, clock_observation_id, execution_clock_scope,
};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, FrameworkArtifactType, INLINE_RESOURCE_LIMIT, InlineData,
    MAX_HANDOFF_INDEX_PAGE, MAX_RESOURCE_ANNOTATIONS, RESOURCE_LOCATOR_VERSION, ResourceCandidate,
    ResourceChunk, ResourceCleanupPlan, ResourceCleanupReceipt, ResourceCleanupTarget,
    ResourceCleanupTargetKind, ResourceClient, ResourceDeleteStatus, ResourceDeleter,
    ResourceDeletionTarget, ResourceError, ResourceHandle, ResourceHandoff,
    ResourceHandoffActivation, ResourceHandoffController, ResourceIntegrity,
    ResourceLifecycleController, ResourceListCursor, ResourceLocation, ResourceLocatorSet,
    ResourceManifestEntry, ResourceObservation, ResourcePage, ResourceProducerProvenance,
    ResourcePublication, ResourceReplayClass, ResourceShape, ResourceWriteIntent,
    ResourceWriteSession, ResourceWriter, SealedResourceManifest, framework_artifact_contract,
};
use cymule_runtime::{
    ExecutionBinding, PLUGIN_VERSION, PluginHost, PluginManifest, PluginOperation, PluginRequest,
    PluginResponse, RuntimeError, RuntimeResult,
};
use serde_json::json;

#[derive(Clone)]
struct LostHandoffReceiptStore {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

struct CountingDeleter {
    binding: String,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct ProducerCheckpointPlugin;

impl PluginHost for ProducerCheckpointPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        match request {
            PluginRequest::Describe => Ok(PluginResponse::Manifest {
                manifest: producer_manifest(),
            }),
            PluginRequest::Call { component, .. } if component == "component.producer" => {
                let handle = ResourceCandidate::text("producer output")
                    .seal()
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                let value = serde_json::to_value(handle)
                    .map_err(|error| RuntimeError::plugin_defect(error.to_string()))?;
                Ok(PluginResponse::CallResult { value })
            }
            other => Err(RuntimeError::plugin_defect(format!(
                "resource producer fixture received {other:?}"
            ))),
        }
    }
}

#[derive(Clone)]
struct ProducerClock {
    observation: ClockObservation,
}

impl ClockObservationAuthority for ProducerClock {
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation> {
        if self.observation.reference() != *reference {
            return Err(DurableError::NotFound(format!(
                "Clock observation {} was not issued",
                reference.observation_id
            )));
        }
        Ok(self.observation.clone())
    }
}

impl ExecutionClockAuthority for ProducerClock {
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()> {
        if self.observation.reference() != *reference {
            return Err(DurableError::NotFound(format!(
                "Clock observation {} is not the issued producer head",
                reference.observation_id
            )));
        }
        commit(&self.observation)?;
        Ok(())
    }
}

impl ResourceDeleter for CountingDeleter {
    fn binding(&self) -> &str {
        &self.binding
    }

    fn delete_and_verify_absent(
        &mut self,
        target: &ResourceDeletionTarget,
    ) -> cymule_resource::ResourceResult<()> {
        target.verify()?;
        if target.subject.family.store_binding != self.binding {
            return Err(ResourceError::Conflict {
                code: "fixture_deleter_binding_mismatch".to_owned(),
                message: "fixture deleter received another binding's target".to_owned(),
            });
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl DurableStore for LostHandoffReceiptStore {
    fn load_head(&mut self) -> DurableResult<Option<cymule_durable::StoreHead>> {
        self.inner.load_head()
    }

    fn load_state_root_manifest(
        &mut self,
        manifest_id: &str,
    ) -> DurableResult<Option<cymule_durable::StateRootManifest>> {
        self.inner.load_state_root_manifest(manifest_id)
    }

    fn with_state_root_resolver<T>(
        &mut self,
        current: &cymule_durable::StateRootManifest,
        read: impl FnOnce(&mut dyn cymule_durable::StateRootResolver) -> DurableResult<T>,
    ) -> DurableResult<T> {
        self.inner.with_state_root_resolver(current, read)
    }

    fn application_journal_prefix(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        count: u64,
    ) -> DurableResult<cymule_durable::ApplicationJournalPrefix> {
        self.inner
            .application_journal_prefix(manifest, journal_id, count)
    }

    fn application_journal_record_manifest(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        journal_id: &str,
        record_id: &str,
    ) -> DurableResult<Option<cymule_durable::JournalRecordManifest>> {
        self.inner
            .application_journal_record_manifest(manifest, journal_id, record_id)
    }

    fn application_journal_prefix_replacement_authority(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        replacement_id: &str,
    ) -> DurableResult<Option<cymule_durable::ApplicationJournalPrefixReplacementAuthority>> {
        self.inner
            .application_journal_prefix_replacement_authority(manifest, replacement_id)
    }

    fn coupled_checkpoint_receipt(
        &mut self,
        manifest: &cymule_durable::StateRootManifest,
        coupling_id: &str,
    ) -> DurableResult<Option<cymule_durable::CoupledCheckpointReceipt>> {
        self.inner.coupled_checkpoint_receipt(manifest, coupling_id)
    }

    fn load_machine_command_archive_segment(
        &mut self,
        segment_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveSegment>> {
        self.inner.load_machine_command_archive_segment(segment_id)
    }

    fn load_machine_command_archive_entry(
        &mut self,
        entry_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandArchiveEntry>> {
        self.inner.load_machine_command_archive_entry(entry_id)
    }

    fn load_machine_command_archive_batch(
        &mut self,
        batch_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandBatchRecord>> {
        self.inner.load_machine_command_archive_batch(batch_id)
    }

    fn load_machine_command_index_node(
        &mut self,
        node_id: &str,
    ) -> DurableResult<Option<cymule_core::MachineCommandIndexNode>> {
        self.inner.load_machine_command_index_node(node_id)
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&cymule_durable::StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_commit(expected, batch)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::CommitOutcomeUnknown {
                message: "simulated lost handoff activation receipt".to_owned(),
            });
        }
        Ok(commit)
    }

    fn reconcile_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.reconcile_cold_reclamation(request)
    }

    fn advance_cold_reclamation(
        &mut self,
        request: &cymule_durable::StoreReclamation,
    ) -> DurableResult<cymule_durable::GcReceipt> {
        self.inner.advance_cold_reclamation(request)
    }

    fn stats(&self) -> DurableResult<cymule_durable::StoreStats> {
        self.inner.stats()
    }
}

fn external_candidate(shape: ResourceShape, integrity: ResourceIntegrity) -> ResourceCandidate {
    ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity,
        manifest: None,
        annotations: BTreeMap::from([("purpose".to_owned(), "conformance".to_owned())]),
    }
}

fn publication(
    resource: ResourceHandle,
    binding: &str,
    locations: Vec<ResourceLocation>,
) -> ResourcePublication {
    ResourcePublication {
        locators: ResourceLocatorSet {
            locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: binding.to_owned(),
            locations,
        },
        resource,
    }
}

fn object(bytes: &[u8]) -> ResourcePublication {
    let resource = external_candidate(
        ResourceShape::Object,
        ResourceIntegrity::Content {
            digest: format!("sha256:{}", sha256_bytes(bytes)),
            size: bytes.len() as u64,
        },
    )
    .seal()
    .expect("object seals");
    publication(
        resource,
        "binding:memory-resolver/1",
        vec![ResourceLocation::Opaque {
            reference: "object:fixture".to_owned(),
        }],
    )
}

#[test]
fn durable_lifecycle_delete_reconciles_lost_receipt_without_redispatch() {
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostHandoffReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let mut control = DurableStoreControl::initialize(store).expect("domain initializes");
    let publication = object(b"deleted");
    let pin = ResourceLifecycleController::pin(
        &mut control.resource(),
        "pin:output",
        &publication,
        "run:consumer",
    )
    .expect("pin commits");
    ResourceLifecycleController::release(
        &mut control.resource(),
        "release:output",
        &pin.pin.pin_id,
        "run:consumer",
    )
    .expect("release commits");
    let gc = ResourceLifecycleController::garbage_collect(
        &mut control.resource(),
        "gc:output",
        &publication,
    )
    .expect("GC commits");
    ResourceLifecycleController::begin_delete(
        &mut control.resource(),
        "delete:output",
        &gc,
        &publication,
    )
    .expect("delete intent commits");
    assert!(matches!(
        ResourceLifecycleController::pin(
            &mut control.resource(),
            "pin:too-late",
            &publication,
            "run:late"
        ),
        Err(ResourceError::Conflict { .. })
    ));

    let calls = Arc::new(AtomicUsize::new(0));
    let mut deleter = CountingDeleter {
        binding: "binding:memory-resolver/1".to_owned(),
        calls: calls.clone(),
    };
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        ResourceLifecycleController::reconcile_delete(
            &mut control.resource(),
            "delete:output",
            &mut deleter
        ),
        Err(ResourceError::CommitOutcomeUnknown { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let store = control.into_store();
    let mut reopened = DurableStoreControl::open(store).expect("domain reopens");
    let current = reopened
        .resource()
        .delete_current("delete:output")
        .expect("committed deletion current exact-loads after receipt loss")
        .expect("committed deletion current remains present");
    assert_eq!(current.status, ResourceDeleteStatus::Completed);
    let receipt = ResourceLifecycleController::reconcile_delete(
        &mut reopened.resource(),
        "delete:output",
        &mut deleter,
    )
    .expect("committed deletion receipt reconciles exactly");
    assert_eq!(receipt.intent.delete_id, "delete:output");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn inline_text_json_and_bytes_are_location_independent_exact_resources() {
    let text = ResourceCandidate::text("hello Cymule")
        .seal()
        .expect("text seals");
    let structured = ResourceCandidate::json(json!({"answer": 42}))
        .seal()
        .expect("JSON seals");
    let bytes = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Inline,
        media_type: "application/octet-stream".to_owned(),
        inline: Some(InlineData::Base64 {
            data: "AAEC".to_owned(),
        }),
        integrity: ResourceIntegrity::Inline,
        manifest: None,
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("bytes seal");

    for resource in [&text, &structured, &bytes] {
        resource.verify().expect("resource verifies");
        assert_eq!(
            resource.replay_class(),
            ResourceReplayClass::ContentVerified
        );
    }
    assert_eq!(
        text.inline.as_ref().expect("inline").bytes().unwrap(),
        b"hello Cymule"
    );
    assert_eq!(
        bytes.inline.as_ref().expect("inline").bytes().unwrap(),
        [0, 1, 2]
    );
    assert_ne!(text.resource_id, structured.resource_id);
}

#[test]
fn frozen_resource_fixture_has_one_cross_language_identity() {
    let candidate: ResourceCandidate = serde_json::from_str(include_str!(
        "../../../tests/fixtures/resource-candidate.json"
    ))
    .expect("fixture deserializes");
    let resource = candidate.seal().expect("fixture seals");
    assert_eq!(
        resource.resource_id,
        "sha256:a8615b23e5d8748ee2eec5db39d3eff74f31b3e3da5cf323f596d995b9363668"
    );
    let mut tampered = resource;
    tampered.media_type = "application/octet-stream".to_owned();
    assert!(matches!(
        tampered.verify(),
        Err(ResourceError::Integrity { .. })
    ));
}

#[test]
fn locations_do_not_change_identity_but_credentials_fail_closed() {
    let bytes = b"portable object";
    let integrity = ResourceIntegrity::Content {
        digest: format!("sha256:{}", sha256_bytes(bytes)),
        size: bytes.len() as u64,
    };
    let resource = external_candidate(ResourceShape::Object, integrity.clone())
        .seal()
        .expect("object seals");
    let first = publication(
        resource.clone(),
        "binding:http/1",
        vec![ResourceLocation::PublicUrl {
            url: "https://example.com/artifacts/object.bin".to_owned(),
        }],
    );
    let moved = publication(
        external_candidate(ResourceShape::Object, integrity)
            .seal()
            .expect("moved object seals"),
        "binding:remote-drive/7",
        vec![ResourceLocation::Opaque {
            reference: "item:stable-reference".to_owned(),
        }],
    );
    first.verify().expect("first publication verifies");
    moved.verify().expect("moved publication verifies");
    assert_eq!(first.resource.resource_id, moved.resource.resource_id);

    for url in [
        "https://user:secret@example.com/object",
        "https://example.com/object?token=secret",
        "https://example.com/object#access-token",
    ] {
        assert!(matches!(
            publication(
                resource.clone(),
                "binding:http/1",
                vec![ResourceLocation::PublicUrl {
                    url: url.to_owned()
                }],
            )
            .verify(),
            Err(ResourceError::Validation(_))
        ));
    }
}

#[test]
fn versioned_sandbox_and_live_remote_resource_have_honest_replay_classes() {
    let snapshot = external_candidate(
        ResourceShape::Snapshot,
        ResourceIntegrity::Version {
            authority: "sandbox-snapshot-format/1".to_owned(),
            version: "snapshot:2026-08-17:1".to_owned(),
        },
    )
    .seal()
    .expect("snapshot seals");
    let live_drive = external_candidate(
        ResourceShape::Directory,
        ResourceIntegrity::Live {
            identity: "drive-directory:team-docs".to_owned(),
        },
    )
    .seal()
    .expect("live directory seals");
    assert_eq!(
        snapshot.replay_class(),
        ResourceReplayClass::ResolverRequired
    );
    assert_eq!(live_drive.replay_class(), ResourceReplayClass::LiveOnly);
}

struct MemoryResolver {
    bytes: Vec<u8>,
    entries: Vec<cymule_resource::ResourceEntry>,
    corrupt: bool,
}

impl ArtifactResolver for MemoryResolver {
    fn stat(
        &mut self,
        resource: &ResourceHandle,
        _locators: &ResourceLocatorSet,
    ) -> cymule_resource::ResourceResult<ResourceObservation> {
        Ok(ResourceObservation {
            media_type: resource.media_type.clone(),
            integrity: resource.integrity.clone(),
        })
    }

    fn read(
        &mut self,
        _resource: &ResourceHandle,
        _locators: &ResourceLocatorSet,
        offset: u64,
        max_bytes: u32,
    ) -> cymule_resource::ResourceResult<ResourceChunk> {
        let start = usize::try_from(offset).map_err(|error| ResourceError::Substrate {
            code: "fixture_read_offset_overflow".to_owned(),
            message: error.to_string(),
        })?;
        let requested_end =
            start
                .checked_add(max_bytes as usize)
                .ok_or_else(|| ResourceError::Substrate {
                    code: "fixture_read_range_overflow".to_owned(),
                    message: "fixture read range overflow".to_owned(),
                })?;
        let end = self.bytes.len().min(requested_end);
        let mut bytes = self.bytes.get(start..end).unwrap_or_default().to_vec();
        if self.corrupt && start == 0 && !bytes.is_empty() {
            bytes[0] ^= 0xff;
        }
        Ok(ResourceChunk {
            offset,
            bytes,
            eof: end == self.bytes.len(),
        })
    }

    fn list(
        &mut self,
        resource: &ResourceHandle,
        locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> cymule_resource::ResourceResult<ResourcePage> {
        let publication = ResourcePublication {
            resource: resource.clone(),
            locators: locators.clone(),
        };
        let start = cursor.map_or(Ok(0), |cursor| {
            usize::try_from(ResourceListCursor::decode(cursor)?.next_index).map_err(|error| {
                ResourceError::Substrate {
                    code: "fixture_cursor_platform_overflow".to_owned(),
                    message: format!("cursor exceeds platform bounds: {error}"),
                }
            })
        })?;
        let requested_end =
            start
                .checked_add(limit as usize)
                .ok_or_else(|| ResourceError::Substrate {
                    code: "fixture_list_range_overflow".to_owned(),
                    message: "fixture list range overflow".to_owned(),
                })?;
        let end = self.entries.len().min(requested_end);
        let sealed = SealedResourceManifest::seal(self.entries.clone())?;
        let entries = self.entries[start..end].to_vec();
        let next_cursor = (end < self.entries.len())
            .then(|| {
                ResourceListCursor::for_page(&publication, cursor, limit, start as u64, &entries)
            })
            .transpose()?;
        Ok(ResourcePage {
            proof: sealed.proof(start as u64, entries.len(), cursor, next_cursor.as_deref())?,
            entries,
            next_cursor,
        })
    }
}

#[test]
fn bounded_resolver_streams_and_verifies_objects() {
    let bytes = b"0123456789abcdef".to_vec();
    let resource = object(&bytes);
    let mut sink = Vec::new();
    let mut client = ResourceClient::new(MemoryResolver {
        bytes: bytes.clone(),
        entries: Vec::new(),
        corrupt: false,
    });
    assert_eq!(
        client
            .copy_to(&resource, 3, &mut sink)
            .expect("copy verifies"),
        bytes.len() as u64
    );
    assert_eq!(sink, bytes);

    let mut corrupt = ResourceClient::new(MemoryResolver {
        bytes: bytes.clone(),
        entries: Vec::new(),
        corrupt: true,
    });
    assert!(matches!(
        corrupt.copy_to(&resource, 4, &mut Vec::new()),
        Err(ResourceError::Integrity { .. })
    ));
}

#[test]
fn bounded_resolver_verifies_directory_copy_and_pages() {
    let (directory, sealed, entries) = directory_publication_fixture();
    let mut copied_manifest = Vec::new();
    ResourceClient::new(MemoryResolver {
        bytes: sealed.bytes.clone(),
        entries: entries.clone(),
        corrupt: false,
    })
    .copy_to(&directory, 7, &mut copied_manifest)
    .expect("copied manifest closes its byte and Merkle descriptor");
    assert_eq!(copied_manifest, sealed.bytes);
    let changed_descriptor = SealedResourceManifest::seal(vec![ResourceManifestEntry {
        name: "other.txt".to_owned(),
        resource: ResourceCandidate::text("other").seal().unwrap(),
    }])
    .expect("alternate manifest seals")
    .descriptor;
    let changed_resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Directory,
        media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: changed_descriptor.digest.clone(),
            size: changed_descriptor.size,
        },
        manifest: Some(changed_descriptor),
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("alternate manifest descriptor seals semantically");
    let changed_publication = publication(
        changed_resource,
        "binding:directory/1",
        vec![ResourceLocation::Opaque {
            reference: "directory:fixture".to_owned(),
        }],
    );
    assert!(matches!(
        ResourceClient::new(MemoryResolver {
            bytes: sealed.bytes.clone(),
            entries: entries.clone(),
            corrupt: false,
        })
        .copy_to(&changed_publication, 7, &mut Vec::new()),
        Err(ResourceError::Integrity { .. })
    ));
    let mut unproven = ResourceClient::new(MemoryResolver {
        bytes: Vec::new(),
        entries: entries.clone(),
        corrupt: false,
    });
    assert!(matches!(
        unproven.list_page(&directory, Some("1"), 1),
        Err(ResourceError::Validation(_))
    ));
    let mut directory_client = ResourceClient::new(MemoryResolver {
        bytes: Vec::new(),
        entries: entries.clone(),
        corrupt: false,
    });
    let first = directory_client
        .list_page(&directory, None, 1)
        .expect("first page validates");
    assert_eq!(first.entries.len(), 1);
    let cursor = first.next_cursor.clone().expect("first page continues");
    let mut legacy_cursor =
        serde_json::to_value(ResourceListCursor::decode(&cursor).expect("current cursor decodes"))
            .expect("cursor serializes");
    legacy_cursor["cursor_version"] = json!("cymule.resource-list-cursor/2");
    let legacy_cursor = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(cymule_core::canonical_bytes(&legacy_cursor).expect("legacy cursor canonicalizes"));
    assert!(matches!(
        ResourceListCursor::decode(&legacy_cursor),
        Err(ResourceError::Validation(message)) if message.contains("unsupported")
    ));
    assert!(matches!(
        ResourceClient::new(MemoryResolver {
            bytes: Vec::new(),
            entries: entries.clone(),
            corrupt: false,
        })
        .list_page(&directory, Some(&cursor), 2),
        Err(ResourceError::Validation(_))
    ));
    let resolver = directory_client.into_inner();
    let mut reopened_client = ResourceClient::new(resolver);
    let second = reopened_client
        .list_page(&directory, first.next_cursor.as_deref(), 1)
        .expect("second page validates after client restart");
    assert_eq!(second.entries[0].name, "nested/second.json");
    assert!(second.next_cursor.is_none());
}

#[test]
fn copy_to_rejects_oversized_provider_bytes_before_mutating_the_sink() {
    let publication = object(b"short");
    let mut client = ResourceClient::new(MemoryResolver {
        bytes: b"longer".to_vec(),
        entries: Vec::new(),
        corrupt: false,
    });
    let mut sink = Vec::new();
    assert!(matches!(
        client.copy_to(&publication, 16, &mut sink),
        Err(ResourceError::Integrity { code, .. }) if code == "resource_size_exceeded"
    ));
    assert!(
        sink.is_empty(),
        "unverified excess bytes must never reach the sink"
    );
}

#[test]
fn manifest_sealing_rejects_one_entry_above_the_canonical_byte_bound() {
    let child = ResourceCandidate::text("x".repeat(INLINE_RESOURCE_LIMIT))
        .seal()
        .expect("child seals");
    let oversized = ResourceManifestEntry {
        name: "oversized-child".to_owned(),
        resource: child,
    };
    assert!(matches!(
        SealedResourceManifest::seal(vec![oversized]),
        Err(ResourceError::Validation(message)) if message.contains("manifest entry exceeds")
    ));
}

#[derive(Default)]
struct MemoryArtifactStore {
    sessions: BTreeMap<String, (ResourceWriteIntent, Vec<u8>)>,
    cleanup_receipts: BTreeMap<String, ResourceCleanupReceipt>,
}

impl ArtifactStore for MemoryArtifactStore {
    fn begin_write(
        &mut self,
        intent: &ResourceWriteIntent,
    ) -> cymule_resource::ResourceResult<ResourceWriteSession> {
        self.sessions
            .entry(intent.write_id.clone())
            .or_insert_with(|| (intent.clone(), Vec::new()));
        Ok(ResourceWriteSession {
            write_id: intent.write_id.clone(),
            upload_id: format!("upload:{}", intent.write_id),
            store_binding: "binding:memory-store/1".to_owned(),
        })
    }

    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> cymule_resource::ResourceResult<()> {
        let (_, stored) = self
            .sessions
            .get_mut(&session.write_id)
            .ok_or_else(|| ResourceError::NotFound(session.write_id.clone()))?;
        if offset != stored.len() as u64 {
            return Err(ResourceError::Conflict {
                code: "fixture_write_offset_mismatch".to_owned(),
                message: "write offset does not match retained prefix".to_owned(),
            });
        }
        stored.extend_from_slice(bytes);
        Ok(())
    }

    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> cymule_resource::ResourceResult<ResourcePublication> {
        let (intent, bytes) = self
            .sessions
            .get(&session.write_id)
            .cloned()
            .ok_or_else(|| ResourceError::NotFound(session.write_id.clone()))?;
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: intent.shape,
            media_type: intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{}", sha256_bytes(&bytes)),
                size: bytes.len() as u64,
            },
            manifest: None,
            annotations: intent.annotations.clone(),
        }
        .seal()?;
        let publication = publication(
            resource,
            &session.store_binding,
            vec![ResourceLocation::Opaque {
                reference: session.upload_id.clone(),
            }],
        );
        let receipt = cleanup_plan(session)?.receipt()?;
        self.cleanup_receipts
            .insert(session.upload_id.clone(), receipt);
        Ok(publication)
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> cymule_resource::ResourceResult<ResourceCleanupReceipt> {
        if let Some(receipt) = self.cleanup_receipts.get(&session.upload_id) {
            return Ok(receipt.clone());
        }
        let plan = cleanup_plan(session)?;
        self.sessions.remove(&session.write_id);
        let receipt = plan.receipt()?;
        self.cleanup_receipts
            .insert(session.upload_id.clone(), receipt.clone());
        Ok(receipt)
    }

    fn cleanup_receipt(
        &mut self,
        session: &ResourceWriteSession,
    ) -> cymule_resource::ResourceResult<Option<ResourceCleanupReceipt>> {
        Ok(self.cleanup_receipts.get(&session.upload_id).cloned())
    }
}

fn cleanup_plan(
    session: &ResourceWriteSession,
) -> cymule_resource::ResourceResult<ResourceCleanupPlan> {
    ResourceCleanupPlan::new(
        session,
        vec![ResourceCleanupTarget {
            kind: ResourceCleanupTargetKind::StagingObject,
            identifier: format!("staging/{}", session.upload_id),
        }],
    )
}

#[test]
fn chunked_store_interface_keeps_provider_details_out_of_resource_identity() {
    let mut writer = ResourceWriter::new(MemoryArtifactStore::default());
    let invalid = ResourceWriteIntent {
        write_id: "write:inline".to_owned(),
        shape: ResourceShape::Inline,
        media_type: "text/plain".to_owned(),
        annotations: BTreeMap::new(),
    };
    assert!(matches!(
        writer.begin(&invalid),
        Err(ResourceError::Validation(_))
    ));
    for (index, media_type) in [
        "text/\0plain",
        "text/",
        "/plain",
        "a/b/c",
        "Text/plain",
        "text/plain;charset=utf-8",
        "text/ plain",
    ]
    .into_iter()
    .enumerate()
    {
        let invalid_media_type = ResourceWriteIntent {
            write_id: format!("write:invalid-media-type:{index}"),
            shape: ResourceShape::Object,
            media_type: media_type.to_owned(),
            annotations: BTreeMap::new(),
        };
        assert!(matches!(
            writer.begin(&invalid_media_type),
            Err(ResourceError::Validation(_))
        ));
    }
    ResourceWriteIntent {
        write_id: "write:vendor-media-type".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/vnd.cymule.resource+json".to_owned(),
        annotations: BTreeMap::new(),
    }
    .validate()
    .expect("vendor media type with structured suffix verifies");
    let mut excessive_annotations = BTreeMap::new();
    for index in 0..=MAX_RESOURCE_ANNOTATIONS {
        excessive_annotations.insert(format!("annotation:{index}"), String::new());
    }
    let invalid_annotations = ResourceWriteIntent {
        write_id: "write:too-many-annotations".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: excessive_annotations,
    };
    assert!(matches!(
        writer.begin(&invalid_annotations),
        Err(ResourceError::Validation(message)) if message.contains("annotations")
    ));
    let intent = ResourceWriteIntent {
        write_id: "write:large-object".to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = writer.begin(&intent).expect("write begins");
    assert!(matches!(
        writer.write(&session, 0, b""),
        Err(ResourceError::Validation(_))
    ));
    writer.write(&session, 0, b"large ").expect("first chunk");
    assert!(matches!(
        writer.write(&session, 0, b"conflict"),
        Err(ResourceError::Conflict { .. })
    ));
    writer.write(&session, 6, b"object").expect("second chunk");
    let handle = writer.commit(&intent, &session).expect("write commits");
    assert_eq!(
        handle.resource.replay_class(),
        ResourceReplayClass::ContentVerified
    );
    assert!(matches!(
        handle.resource.integrity,
        ResourceIntegrity::Content { size: 12, .. }
    ));
    let cleanup = writer
        .cleanup_receipt(&intent, &session)
        .expect("commit cleanup receipt reads")
        .expect("commit retained terminal cleanup receipt");
    assert_eq!(
        writer
            .cleanup_receipt(&intent, &session)
            .expect("cleanup receipt replay reads"),
        Some(cleanup)
    );
}

fn producer_manifest() -> PluginManifest {
    PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "resource-producer-plugin".to_owned(),
        components: BTreeMap::from([(
            "component.producer".to_owned(),
            PluginOperation {
                implementation_revision: "producer-v1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    }
}

fn producer_binding() -> ExecutionBinding {
    ExecutionBinding::for_local_process(
        &producer_manifest(),
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .expect("producer binding seals")
}

fn execution_for(run_id: &str) -> (ProducerClock, ExecutionClaimRequest) {
    const SOURCE_ID: &str = "clock:resource-producer";
    const SOURCE_GENERATION: &str =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let scope = execution_clock_scope(run_id).expect("test Clock scope seals");
    let observation_id = clock_observation_id(SOURCE_ID, SOURCE_GENERATION, &scope, 1, 1)
        .expect("test Clock observation seals");
    let observation = ClockObservation {
        clock_version: CLOCK_OBSERVATION_VERSION.to_owned(),
        observation_id,
        source_id: SOURCE_ID.to_owned(),
        source_generation: SOURCE_GENERATION.to_owned(),
        scope,
        logical_time: 1,
        observed_unix_ms: 1,
    };
    observation
        .verify()
        .expect("producer Clock receipt verifies");
    let request = ExecutionClaimRequest {
        owner: content_id("cymule.test-driver/1", &run_id).expect("test owner seals"),
        clock: observation.reference(),
        ttl: 1,
    };
    (ProducerClock { observation }, request)
}

fn consumer_candidate() -> PlanCandidate {
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "resource_handoff".to_owned(),
        entry: "main".to_owned(),
        components: Vec::new(),
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "wait.resource-input".to_owned(),
                    operation: Operation::Wait {
                        wait: WaitSpec::Input {
                            correlation: "input.dataset".to_owned(),
                            schema: json!({}),
                        },
                        bind: None,
                    },
                }],
                result: Expression::Literal { value: json!(null) },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn producer_candidate() -> PlanCandidate {
    let resource_handle_contract =
        framework_artifact_contract(FrameworkArtifactType::ResourceHandle)
            .expect("Resource Handle contract builds");
    let output_artifact_kind = resource_handle_contract
        .typed_artifact_kind()
        .expect("Resource Handle persisted kind derives");
    assert_ne!(
        output_artifact_kind, resource_handle_contract.artifact_kind,
        "component output must use the typed persisted kind, not the logical type key"
    );
    PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "resource_producer".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "component.producer".to_owned(),
            input_schema: json!({}),
            output_schema: resource_handle_contract.schema.clone(),
            output_artifact_kind,
            requirements: BTreeMap::new(),
        }],
        effects: Vec::new(),
        definitions: vec![Definition {
            id: "main".to_owned(),
            input_schema: json!({}),
            output_schema: json!({}),
            body: Region {
                steps: vec![Step {
                    id: "site.producer.result".to_owned(),
                    operation: Operation::Call {
                        component: "component.producer".to_owned(),
                        input: Expression::Input,
                        bind: None,
                    },
                }],
                result: Expression::Literal { value: json!(null) },
            },
        }],
        metadata: BTreeMap::new(),
    }
}

fn start_producer<S: DurableStore + Clone>(store: S) -> (S, cymule_core::ArtifactRef, String) {
    let (clock, execution) = execution_for("run:producer");
    let admission = cymule_runtime::ExecutionBindingAdmission::admit(
        ProducerCheckpointPlugin,
        producer_binding(),
    )
    .expect("producer binding admits");
    let mut runtime =
        DurableRuntimeControl::open(store, admission, clock).expect("producer runtime opens");
    let completed = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: "run:producer".to_owned(),
            candidate: producer_candidate(),
            input: json!({}),
            execution,
        })
        .expect("producer completes through the closed runtime path");
    completed.verify().expect("producer boundary verifies");
    assert!(matches!(
        completed,
        DurableResponse::RunBoundary {
            boundary: DurableBoundary::Completed { .. }
        }
    ));
    let (store, _) = runtime.into_parts();
    let mut control = DurableStoreControl::open(store).expect("producer store reopens");
    let occurrence_id = query_occurrences(&mut control, "run:producer")
        .into_iter()
        .find(|occurrence| occurrence.run_id == "run:producer")
        .expect("producer occurrence is durable")
        .occurrence_id;
    let occurrence = query_occurrence(&mut control, "run:producer", &occurrence_id);
    let cymule_durable::ComponentOutcome::Succeeded { output: result } = occurrence
        .outcome
        .clone()
        .expect("producer occurrence is terminal")
    else {
        panic!("producer occurrence did not retain successful output");
    };
    (control.into_store(), result, occurrence_id)
}

fn start_consumer<S: DurableStore + Clone>(store: S, run_id: &str) -> (S, String) {
    let (clock, execution) = execution_for(run_id);
    let admission = cymule_runtime::ExecutionBindingAdmission::admit(
        ProducerCheckpointPlugin,
        producer_binding(),
    )
    .expect("consumer binding admits");
    let mut runtime =
        DurableRuntimeControl::open(store, admission, clock).expect("consumer runtime opens");
    let outcome = runtime
        .submit(DurableCommand::StartRun {
            control_version: DURABLE_CONTROL_VERSION.to_owned(),
            run_id: run_id.to_owned(),
            candidate: consumer_candidate(),
            input: json!({}),
            execution,
        })
        .expect("consumer starts and parks");
    outcome.verify().expect("consumer boundary verifies");
    let DurableResponse::RunBoundary {
        boundary: DurableBoundary::Suspended { wait_id },
    } = outcome
    else {
        panic!("consumer did not park: {outcome:?}");
    };
    let (store, _) = runtime.into_parts();
    (store, wait_id)
}

fn handoff_domain<S: DurableStore + Clone>(
    store: S,
    consumer_runs: &[&str],
) -> (
    DurableStoreControl<S>,
    cymule_core::ArtifactRef,
    String,
    BTreeMap<String, String>,
) {
    let (mut store, result, occurrence_id) = start_producer(store);
    let mut waits = BTreeMap::new();
    for run_id in consumer_runs {
        let (next_store, wait_id) = start_consumer(store, run_id);
        store = next_store;
        waits.insert((*run_id).to_owned(), wait_id);
    }
    (
        DurableStoreControl::open(store).expect("handoff domain reopens"),
        result,
        occurrence_id,
        waits,
    )
}

fn query_occurrences<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    run_id: &str,
) -> Vec<cymule_durable::DurableOccurrenceSummary> {
    let command = DurableCommand::RunOccurrencePage {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
        cursor: None,
        limit: MAX_DURABLE_QUERY_PAGE_ITEMS,
        max_canonical_bytes: MAX_DURABLE_QUERY_PAGE_BYTES,
    };
    let response = control
        .submit(command.clone())
        .expect("Run occurrence page query succeeds");
    response
        .verify_query_for(&command)
        .expect("Run occurrence page query verifies");
    let DurableResponse::RunOccurrencePage {
        run_id: response_run,
        page,
    } = response
    else {
        panic!("Run occurrence query returned another response variant");
    };
    assert_eq!(response_run, run_id);
    assert!(
        page.next_cursor.is_none(),
        "fixture occurrence page is bounded"
    );
    page.items
}

fn load_run_current<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    run_id: &str,
) -> DurableRunCurrent {
    let command = DurableCommand::RunCurrent {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
    };
    let response = control
        .submit(command.clone())
        .expect("Run current query succeeds");
    response
        .verify_query_for(&command)
        .expect("Run current query verifies");
    let DurableResponse::RunCurrent {
        current: Some(current),
        ..
    } = response
    else {
        panic!("Run current query did not return its exact projection");
    };
    *current
}

fn query_occurrence<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    run_id: &str,
    occurrence_id: &str,
) -> ComponentOccurrence {
    let command = DurableCommand::RunItem {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
        selector: DurableRunItemSelector::Occurrence {
            occurrence_id: occurrence_id.to_owned(),
        },
        max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    };
    let response = control
        .submit(command.clone())
        .expect("exact Run occurrence query succeeds");
    response
        .verify_query_for(&command)
        .expect("exact Run occurrence query verifies");
    let DurableResponse::RunItem {
        item: Some(item), ..
    } = response
    else {
        panic!("exact Run occurrence query did not return its item");
    };
    let DurableRunItem::Occurrence { occurrence } = *item else {
        panic!("exact Run occurrence query returned another item kind");
    };
    *occurrence
}

fn query_wait<S: DurableStore>(
    control: &mut DurableStoreControl<S>,
    run_id: &str,
    wait_id: &str,
) -> WaitCondition {
    let command = DurableCommand::RunItem {
        control_version: DURABLE_CONTROL_VERSION.to_owned(),
        run_id: run_id.to_owned(),
        expected_revision: None,
        selector: DurableRunItemSelector::Wait {
            wait_id: wait_id.to_owned(),
        },
        max_canonical_bytes: MAX_DURABLE_QUERY_EXACT_RESPONSE_BYTES,
    };
    let response = control
        .submit(command.clone())
        .expect("exact Run wait query succeeds");
    response
        .verify_query_for(&command)
        .expect("exact Run wait query verifies");
    let DurableResponse::RunItem {
        item: Some(item), ..
    } = response
    else {
        panic!("exact Run wait query did not return its item");
    };
    let DurableRunItem::Wait { wait } = *item else {
        panic!("exact Run wait query returned another item kind");
    };
    *wait
}

fn producer_provenance(
    occurrence_id: &str,
    result: &cymule_core::ArtifactRef,
) -> ResourceProducerProvenance {
    ResourceProducerProvenance {
        run_id: "run:producer".to_owned(),
        occurrence_id: occurrence_id.to_owned(),
        result: result.clone(),
    }
}

#[test]
fn resource_handoff_identities_use_the_core_unicode_scalar_contract() {
    let result = cymule_core::artifact_ref("test/resource", b"resource")
        .expect("resource reference derives");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:unicode-runs".to_owned(),
        producer: ResourceProducerProvenance {
            run_id: "源".repeat(512),
            occurrence_id: "occurrence:unicode".to_owned(),
            result: result.clone(),
        },
        to_run: "目".repeat(512),
        slot: "input.dataset".to_owned(),
        resource: result,
    };
    handoff
        .verify()
        .expect("512 multibyte Unicode scalars are valid Run identities");

    let mut generic = handoff.clone();
    generic.transfer_id = "🚀".repeat(512);
    generic.producer.occurrence_id = "🧩".repeat(512);
    generic.slot = "📦".repeat(512);
    generic
        .verify()
        .expect("512 multibyte Unicode scalars are valid generic handoff identities");

    for field in ["transfer", "occurrence", "slot"] {
        let mut too_long = handoff.clone();
        match field {
            "transfer" => too_long.transfer_id = "🚀".repeat(513),
            "occurrence" => too_long.producer.occurrence_id = "🧩".repeat(513),
            "slot" => too_long.slot = "📦".repeat(513),
            _ => unreachable!(),
        }
        assert!(
            matches!(too_long.verify(), Err(ResourceError::Validation(_))),
            "513-scalar {field} identity must fail"
        );
        let mut controlled = handoff.clone();
        match field {
            "transfer" => controlled.transfer_id = "id:\u{0085}forged".to_owned(),
            "occurrence" => controlled.producer.occurrence_id = "id:\u{0085}forged".to_owned(),
            "slot" => controlled.slot = "id:\u{0085}forged".to_owned(),
            _ => unreachable!(),
        }
        assert!(
            matches!(controlled.verify(), Err(ResourceError::Validation(_))),
            "C1-controlled {field} identity must fail"
        );
    }

    let mut too_long = handoff.clone();
    too_long.to_run.push('目');
    assert!(matches!(
        too_long.verify(),
        Err(ResourceError::Validation(_))
    ));
    let mut legacy_handoff = handoff.clone();
    legacy_handoff.handoff_version = "cymule.resource-handoff/4".to_owned();
    assert!(matches!(
        legacy_handoff.verify(),
        Err(ResourceError::Validation(_))
    ));
    let legacy_activation = ResourceHandoffActivation {
        activation_version: "cymule.resource-handoff-activation/2".to_owned(),
        activation_id: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        transfer_id: handoff.transfer_id.clone(),
        to_run: handoff.to_run.clone(),
        wait_id: "wait:legacy-activation".to_owned(),
        result: handoff.resource.clone(),
    };
    assert!(matches!(
        legacy_activation.verify(),
        Err(ResourceError::Validation(_))
    ));
    let mut controlled = handoff;
    controlled.producer.run_id = "run:\u{0085}forged".to_owned();
    assert!(matches!(
        controlled.verify(),
        Err(ResourceError::Validation(_))
    ));
}

#[test]
fn run_to_run_handoff_is_idempotent_conflict_checked_and_reopenable() {
    let store = MemoryStore::new();
    let (mut coordinator, producer_result, producer_occurrence, _) =
        handoff_domain(store.clone(), &["run:consumer", "run:consumer-two"]);
    let mut stale = DurableStoreControl::open(store.clone()).expect("stale view opens");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:producer-result".to_owned(),
        producer: producer_provenance(&producer_occurrence, &producer_result),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: producer_result.clone(),
    };
    ResourceHandoffController::transfer(&mut coordinator.resource(), &handoff)
        .expect("handoff commits");
    assert!(matches!(
        ResourceHandoffController::transfer(&mut stale.resource(), &handoff),
        Err(ResourceError::Conflict { .. })
    ));
    ResourceHandoffController::transfer(&mut coordinator.resource(), &handoff)
        .expect("handoff retry is idempotent");
    assert_eq!(
        ResourceHandoffController::handoff(&mut coordinator.resource(), &handoff.transfer_id)
            .expect("canonical transfer identity reads"),
        Some(handoff.clone())
    );
    let mut conflicting = handoff.clone();
    conflicting.resource = cymule_core::artifact_ref("example.other/1", b"different output")
        .expect("conflicting reference derives");
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator.resource(), &conflicting),
        Err(ResourceError::Integrity { .. })
    ));
    let mut reused_transfer = handoff.clone();
    reused_transfer.slot = "input.changed".to_owned();
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator.resource(), &reused_transfer),
        Err(ResourceError::Conflict { .. })
    ));
    let mut reused_across_target = handoff.clone();
    reused_across_target.to_run = "run:consumer-two".to_owned();
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator.resource(), &reused_across_target),
        Err(ResourceError::Conflict { .. })
    ));
    let mut competing_slot = handoff.clone();
    competing_slot.transfer_id = "transfer:competing-producer".to_owned();
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator.resource(), &competing_slot),
        Err(ResourceError::Conflict { .. })
    ));
    drop(coordinator);

    let mut reopened = DurableStoreControl::open(store).expect("store reopens");
    let incoming = ResourceHandoffController::incoming_page(
        &mut reopened.resource(),
        "run:consumer",
        0,
        MAX_HANDOFF_INDEX_PAGE,
    )
    .expect("handoff page replays");
    assert_eq!(incoming.handoffs, vec![handoff]);
    assert_eq!(incoming.next_index, None);
    assert!(matches!(
        ResourceHandoffController::incoming_page(&mut reopened.resource(), "run:consumer", 0, 0,),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        ResourceHandoffController::incoming_page(
            &mut reopened.resource(),
            "run:consumer",
            0,
            MAX_HANDOFF_INDEX_PAGE + 1
        ),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        ResourceHandoffController::incoming_page(&mut reopened.resource(), "run:consumer", 2, 1,),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        ResourceHandoffController::incoming_page(
            &mut reopened.resource(),
            "run:consumer",
            cymule_core::MAX_EXACT_INTEGER + 1,
            1,
        ),
        Err(ResourceError::Validation(_))
    ));
}

#[test]
fn resource_handoff_atomically_activates_matching_input_wait() {
    let transfer_id = format!("transfer:{}", "t".repeat(480));
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostHandoffReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let (mut coordinator, producer_result, producer_occurrence, waits) =
        handoff_domain(store.clone(), &["run:consumer"]);
    let wait_id = waits["run:consumer"].clone();
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id,
        producer: producer_provenance(&producer_occurrence, &producer_result),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: producer_result.clone(),
    };
    let source_receipt = ResourceHandoffController::transfer(&mut coordinator.resource(), &handoff)
        .expect("source handoff authority commits before activation");
    source_receipt
        .verify()
        .expect("source receipt binds target index authority");
    let expected_activation =
        ResourceHandoffActivation::new(&handoff, &wait_id).expect("expected activation derives");
    armed.store(true, Ordering::SeqCst);
    let lost = ResourceHandoffController::activate(&mut coordinator.resource(), &handoff, &wait_id);
    assert!(
        matches!(
            &lost,
            Err(ResourceError::CommitOutcomeUnknown { message })
                if message.contains("simulated lost handoff activation receipt")
        ),
        "unexpected activation result: {lost:?}"
    );
    let store = coordinator.into_store();
    let mut coordinator = DurableStoreControl::open(store).expect("lost receipt state reopens");
    let activation = ResourceHandoffController::activation(
        &mut coordinator.resource(),
        &expected_activation.activation_id,
    )
    .expect("typed activation reconciliation reads")
    .expect("committed activation is retained");
    assert_activation_matches_handoff(&activation, &handoff, &wait_id);
    let wait = query_wait(&mut coordinator, "run:consumer", &activation.wait_id);
    assert_eq!(wait.result.as_ref(), Some(&activation.result));
    assert_eq!(
        load_run_current(&mut coordinator, "run:consumer").continuation_status,
        ContinuationStatus::Ready
    );
    assert_eq!(
        ResourceHandoffController::incoming_page(
            &mut coordinator.resource(),
            "run:consumer",
            0,
            1,
        )
        .expect("incoming handoff page replays")
        .handoffs,
        vec![handoff.clone()]
    );
    let replay =
        ResourceHandoffController::activate(&mut coordinator.resource(), &handoff, &wait_id)
            .expect("activation retry returns its exact durable receipt");
    replay.verify().expect("activation receipt verifies");
    assert_eq!(replay.activation, activation);
    assert_eq!(replay.source_receipt_id, source_receipt.receipt_id);
    assert_eq!(replay.index.activation_id, activation.activation_id);
    assert_eq!(replay.index.to_run, handoff.to_run);
    assert_eq!(replay.index.transfer_id, handoff.transfer_id);
    assert_eq!(replay.index.authority_receipt_id, replay.receipt_id);
    assert_eq!(
        ResourceHandoffController::activation(
            &mut coordinator.resource(),
            &activation.activation_id,
        )
        .expect("activation authority lookup succeeds"),
        Some(activation.clone())
    );
    assert!(matches!(
        ResourceHandoffController::activate(
            &mut coordinator.resource(),
            &handoff,
            "wait:different-redelivery"
        ),
        Err(ResourceError::Conflict { .. })
    ));
    let store = coordinator.into_store();
    let mut reopened = DurableStoreControl::open(store).expect("store reopens");
    assert_eq!(
        query_wait(&mut reopened, "run:consumer", &activation.wait_id).state,
        WaitState::Completed
    );
}

fn directory_publication_fixture() -> (
    ResourcePublication,
    SealedResourceManifest,
    Vec<ResourceManifestEntry>,
) {
    let entries = vec![
        ResourceManifestEntry {
            name: "first.txt".to_owned(),
            resource: ResourceCandidate::text("first").seal().unwrap(),
        },
        ResourceManifestEntry {
            name: "nested/second.json".to_owned(),
            resource: ResourceCandidate::json(json!({"second": true}))
                .seal()
                .unwrap(),
        },
    ];
    let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
    assert_ne!(
        sealed.descriptor.digest,
        format!("sha256:{}", sha256_bytes(&sealed.bytes)),
        "manifest descriptor identity is not a parallel raw-byte digest"
    );
    let directory_resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Directory,
        media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: sealed.descriptor.digest.clone(),
            size: sealed.descriptor.size,
        },
        manifest: Some(sealed.descriptor.clone()),
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("directory seals");
    let directory = publication(
        directory_resource,
        "binding:directory/1",
        vec![ResourceLocation::Opaque {
            reference: "directory:fixture".to_owned(),
        }],
    );
    (directory, sealed, entries)
}

fn assert_activation_matches_handoff(
    activation: &ResourceHandoffActivation,
    handoff: &ResourceHandoff,
    wait_id: &str,
) {
    activation.verify().expect("activation identity verifies");
    let mut changed_activation = activation.clone();
    changed_activation.wait_id.push_str(":changed");
    assert!(matches!(
        changed_activation.verify(),
        Err(ResourceError::Integrity { .. })
    ));
    assert!(activation.activation_id.starts_with("sha256:"));
    assert_eq!(activation.transfer_id, handoff.transfer_id);
    assert_eq!(activation.to_run, handoff.to_run);
    assert_eq!(activation.wait_id, wait_id);
    assert_eq!(activation.result, handoff.resource);
}
