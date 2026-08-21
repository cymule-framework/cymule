//! Fault-oriented resource descriptor, resolver, store, and handoff tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use cymule_core::seal_plan;
use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, ComponentContract, Definition, Expression, Machine,
    Operation, PlanCandidate, Region, Step, WaitSpec, sha256_bytes,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableResult,
    DurableStore, FrameState, MemoryStore, StoreCommit, StoredState, WaitCondition, WaitKind,
    WaitState,
};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, ArtifactTypeRegistry, FrameworkArtifactType, InlineData,
    RESOURCE_CLEANUP_RECEIPT_VERSION, RESOURCE_LOCATOR_VERSION, ResourceCandidate, ResourceChunk,
    ResourceCleanupReceipt, ResourceClient, ResourceDeleteIntent, ResourceDeleter,
    ResourceDeletionObservation, ResourceError, ResourceHandle, ResourceHandoff,
    ResourceHandoffController, ResourceIntegrity, ResourceLifecycleController, ResourceLocation,
    ResourceLocatorSet, ResourceManifestEntry, ResourceObservation, ResourcePage,
    ResourceProducerProvenance, ResourcePublication, ResourceReplayClass, ResourceShape,
    ResourceWriteIntent, ResourceWriteSession, ResourceWriter, SealedResourceManifest,
    framework_artifact_contract,
};
use cymule_runtime::{
    EXECUTION_BINDING_VERSION, ExecutionBinding, PLUGIN_VERSION, PluginManifest, PluginOperation,
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

impl ResourceDeleter for CountingDeleter {
    fn binding(&self) -> &str {
        &self.binding
    }

    fn delete_resource(
        &mut self,
        intent: &ResourceDeleteIntent,
    ) -> cymule_resource::ResourceResult<ResourceDeletionObservation> {
        intent.verify()?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ResourceDeletionObservation {
            removed_bytes: 7,
            verified_absent: true,
        })
    }
}

impl DurableStore for LostHandoffReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_commit(
        &mut self,
        expected: Option<&cymule_durable::StoreHead>,
        batch: &cymule_durable::StoreBatch,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_commit(expected, batch)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost handoff activation receipt".to_owned(),
            ));
        }
        Ok(commit)
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
    let mut coordinator = DurableCoordinator::open(store)
        .expect("store opens")
        .initialize(&Machine::new())
        .expect("domain initializes");
    let publication = object(b"deleted");
    let resource_id = publication.resource.resource_id.clone();
    let pin = ResourceLifecycleController::pin(
        &mut coordinator,
        "pin:output",
        &resource_id,
        "run:consumer",
    )
    .expect("pin commits");
    ResourceLifecycleController::release(&mut coordinator, "release:output", &pin.pin_id)
        .expect("release commits");
    let gc =
        ResourceLifecycleController::garbage_collect(&mut coordinator, "gc:output", &resource_id)
            .expect("GC commits");
    ResourceLifecycleController::begin_delete(
        &mut coordinator,
        "delete:output",
        &gc.gc_id,
        &publication,
        "binding:memory-resolver/1",
    )
    .expect("delete intent commits");
    assert!(matches!(
        ResourceLifecycleController::pin(
            &mut coordinator,
            "pin:too-late",
            &resource_id,
            "run:late"
        ),
        Err(ResourceError::Conflict(_))
    ));

    let calls = Arc::new(AtomicUsize::new(0));
    let mut deleter = CountingDeleter {
        binding: "binding:memory-resolver/1".to_owned(),
        calls: calls.clone(),
    };
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        ResourceLifecycleController::reconcile_delete(
            &mut coordinator,
            "delete:output",
            &mut deleter
        ),
        Err(ResourceError::Persistence(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let store = coordinator.into_store();
    let mut reopened = DurableCoordinator::open(store).expect("domain reopens");
    let receipt =
        ResourceLifecycleController::reconcile_delete(&mut reopened, "delete:output", &mut deleter)
            .expect("committed deletion receipt replays");
    assert!(receipt.verified_absent);
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
        "sha256:d0ed5dfc870375b45356667f9abc75edcfd81644a754293c7d1c4871163187d1"
    );
    let mut tampered = resource;
    tampered.media_type = "application/octet-stream".to_owned();
    assert!(matches!(
        tampered.verify(),
        Err(ResourceError::Integrity(_))
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
        let start =
            usize::try_from(offset).map_err(|error| ResourceError::Substrate(error.to_string()))?;
        let end = self
            .bytes
            .len()
            .min(start.saturating_add(max_bytes as usize));
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
        _resource: &ResourceHandle,
        _locators: &ResourceLocatorSet,
        cursor: Option<&str>,
        limit: u32,
    ) -> cymule_resource::ResourceResult<ResourcePage> {
        let start = cursor
            .map_or(Ok(0), str::parse::<usize>)
            .map_err(|error| ResourceError::Substrate(error.to_string()))?;
        let end = self.entries.len().min(start.saturating_add(limit as usize));
        let sealed = SealedResourceManifest::seal(self.entries.clone())?;
        let entries = self.entries[start..end].to_vec();
        let next_cursor = (end < self.entries.len()).then(|| end.to_string());
        Ok(ResourcePage {
            proof: sealed.proof(start as u64, entries.len(), cursor, next_cursor.as_deref())?,
            entries,
            next_cursor,
        })
    }
}

#[test]
fn bounded_resolver_streams_and_verifies_objects_and_directory_pages() {
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
        Err(ResourceError::Integrity(_))
    ));

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
    let directory_resource = ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Directory,
        media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: sealed.descriptor.digest.clone(),
            size: sealed.descriptor.size,
        },
        manifest: Some(sealed.descriptor),
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
        entries,
        corrupt: false,
    });
    let first = directory_client
        .list_page(&directory, None, 1)
        .expect("first page validates");
    assert_eq!(first.entries.len(), 1);
    let second = directory_client
        .list_page(&directory, first.next_cursor.as_deref(), 1)
        .expect("second page validates");
    assert_eq!(second.entries[0].name, "nested/second.json");
    assert!(second.next_cursor.is_none());
}

#[derive(Default)]
struct MemoryArtifactStore {
    sessions: BTreeMap<String, (ResourceWriteIntent, Vec<u8>)>,
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
            return Err(ResourceError::Conflict(
                "write offset does not match retained prefix".to_owned(),
            ));
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
            .ok_or_else(|| ResourceError::NotFound(session.write_id.clone()))?;
        let resource = ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: intent.shape,
            media_type: intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{}", sha256_bytes(bytes)),
                size: bytes.len() as u64,
            },
            manifest: None,
            annotations: intent.annotations.clone(),
        }
        .seal()?;
        Ok(publication(
            resource,
            &session.store_binding,
            vec![ResourceLocation::Opaque {
                reference: session.upload_id.clone(),
            }],
        ))
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> cymule_resource::ResourceResult<ResourceCleanupReceipt> {
        self.sessions.remove(&session.write_id);
        Ok(ResourceCleanupReceipt {
            receipt_version: RESOURCE_CLEANUP_RECEIPT_VERSION.to_owned(),
            write_id: session.write_id.clone(),
            upload_id: session.upload_id.clone(),
            store_binding: session.store_binding.clone(),
            removed_staging_objects: 1,
            removed_chunks: 0,
            verified_absent: true,
        })
    }
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
        Err(ResourceError::Conflict(_))
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
}

fn machine_with_runs() -> (Machine, cymule_core::ArtifactRef) {
    let mut machine = Machine::new();
    let plan = seal_plan(PlanCandidate {
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
    })
    .expect("Plan seals");
    machine.insert_plan(plan.clone()).expect("Plan inserts");
    let manifest = PluginManifest {
        plugin_version: PLUGIN_VERSION.to_owned(),
        implementation_id: "resource-producer-plugin".to_owned(),
        components: BTreeMap::from([(
            "component.producer".to_owned(),
            PluginOperation {
                implementation_revision: "producer-v1".to_owned(),
            },
        )]),
        effects: BTreeMap::new(),
    };
    let binding = ExecutionBinding::for_local_process(
        &manifest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();
    let resource_handle_contract =
        framework_artifact_contract(FrameworkArtifactType::ResourceHandle)
            .expect("Resource Handle contract builds");
    let producer_plan = seal_plan(PlanCandidate {
        ir_version: cymule_core::IR_VERSION.to_owned(),
        name: "resource_producer".to_owned(),
        entry: "main".to_owned(),
        components: vec![ComponentContract {
            id: "component.producer".to_owned(),
            input_schema: json!({}),
            output_schema: resource_handle_contract.schema.clone(),
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
    })
    .unwrap();
    machine.insert_plan(producer_plan.clone()).unwrap();
    let binding_ref = machine
        .put_artifact(
            EXECUTION_BINDING_VERSION,
            binding.canonical_bytes().unwrap(),
        )
        .unwrap();
    for (run_id, plan_id, binding_context) in [
        (
            "run:producer",
            producer_plan.plan_id.as_str(),
            binding_ref.artifact_id.as_str(),
        ),
        (
            "run:consumer",
            plan.plan_id.as_str(),
            "binding:resource-test/1",
        ),
    ] {
        machine
            .submit(CommandEnvelope {
                command_version: COMMAND_VERSION.to_owned(),
                command_id: format!("command:start:{run_id}"),
                actor: "actor:resource-test".to_owned(),
                run_id: run_id.to_owned(),
                expected_precondition: None,
                command: Command::StartRun {
                    plan_id: plan_id.to_owned(),
                    binding_context: binding_context.to_owned(),
                },
            })
            .expect("Run starts");
    }
    let precondition = machine.projection().runs["run:producer"].precondition_token();
    machine
        .submit(CommandEnvelope {
            command_version: COMMAND_VERSION.to_owned(),
            command_id: "command:producer-attempt".to_owned(),
            actor: "actor:resource-test".to_owned(),
            run_id: "run:producer".to_owned(),
            expected_precondition: Some(precondition),
            command: Command::BeginAttempt {
                attempt_id: "attempt:producer:0".to_owned(),
                continuation_id: "continuation:producer".to_owned(),
                occurrence_binding: binding_ref.artifact_id,
                epoch: 0,
            },
        })
        .unwrap();
    machine
        .put_artifact("test/input", b"resource input".to_vec())
        .expect("input stores");
    machine
        .put_artifact("cymule.input/1", b"{}".to_vec())
        .expect("producer component input stores");
    let handle = ResourceCandidate::text("producer output")
        .seal()
        .expect("Resource seals");
    let mut registry = ArtifactTypeRegistry::new();
    let contract_id = resource_handle_contract.contract_id.clone();
    registry
        .register(resource_handle_contract)
        .expect("contract registers");
    let typed = registry
        .put_canonical_json(&contract_id, &handle)
        .expect("typed Resource Handle seals");
    let result = machine
        .put_artifact(typed.reference.kind, typed.bytes)
        .expect("producer Resource Handle stores");
    (machine, result)
}

fn record_producer(
    coordinator: &mut DurableCoordinator<impl DurableStore>,
    result: &cymule_core::ArtifactRef,
) -> String {
    let mut machine = coordinator.restore_machine().unwrap();
    let run = &machine.projection().runs["run:producer"];
    let input = cymule_core::artifact_ref("cymule.input/1", b"{}").unwrap();
    let source = Continuation {
        run_id: "run:producer".to_owned(),
        plan_id: run.current_plan.clone(),
        binding_context: run.current_binding_context.clone(),
        frames: vec![FrameState {
            definition_id: "main".to_owned(),
            invocation_id: "main".to_owned(),
            invocation_path: Vec::new(),
            scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
            input: input.clone(),
            region_path: Vec::new(),
            next_step: 0,
            locals: BTreeMap::new(),
        }],
        state: Some(input),
        wait_set: BTreeSet::new(),
        scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
        effect_obligations: BTreeSet::new(),
        authority_leases: BTreeSet::new(),
        budget: BTreeMap::new(),
        causal_frontier: BTreeSet::new(),
        epoch: 0,
        status: ContinuationStatus::Running,
    };
    coordinator
        .put_continuation(source.clone())
        .expect("producer Continuation stores");
    let component_input = machine
        .put_artifact("cymule.component-input/1", b"{}".to_vec())
        .unwrap();
    let mut target = source;
    target.frames[0].next_step = 1;
    coordinator
        .checkpoint_component(&machine, target, &component_input, result)
        .expect("producer occurrence records atomically");
    coordinator
        .state()
        .unwrap()
        .component_occurrences
        .values()
        .find(|occurrence| occurrence.run_id == "run:producer")
        .unwrap()
        .occurrence_id
        .clone()
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
fn run_to_run_handoff_is_idempotent_conflict_checked_and_reopenable() {
    let store = MemoryStore::new();
    let (machine, producer_result) = machine_with_runs();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let producer_occurrence = record_producer(&mut coordinator, &producer_result);
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:producer-result".to_owned(),
        producer: producer_provenance(&producer_occurrence, &producer_result),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: producer_result.clone(),
    };
    ResourceHandoffController::transfer(&mut coordinator, &handoff).expect("handoff commits");
    assert!(matches!(
        ResourceHandoffController::transfer(&mut stale, &handoff),
        Err(ResourceError::Persistence(_))
    ));
    ResourceHandoffController::transfer(&mut coordinator, &handoff)
        .expect("handoff retry is idempotent");
    let mut conflicting = handoff.clone();
    conflicting.resource = cymule_core::artifact_ref("example.other/1", b"different output")
        .expect("conflicting reference derives");
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator, &conflicting),
        Err(ResourceError::Validation(_))
    ));
    let mut competing_slot = handoff.clone();
    competing_slot.transfer_id = "transfer:competing-producer".to_owned();
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator, &competing_slot),
        Err(ResourceError::Conflict(_))
    ));
    drop(coordinator);

    let reopened = DurableCoordinator::open(store).expect("store reopens");
    let incoming =
        ResourceHandoffController::incoming(&reopened, "run:consumer").expect("handoff replays");
    assert_eq!(incoming, vec![handoff]);
}

#[test]
fn resource_handoff_atomically_activates_matching_input_wait() {
    let (machine, producer_result) = machine_with_runs();
    let consumer_plan = machine.projection().runs["run:consumer"]
        .current_plan
        .clone();
    let armed = Arc::new(AtomicBool::new(false));
    let store = LostHandoffReceiptStore {
        inner: MemoryStore::new(),
        armed: armed.clone(),
    };
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine)
        .expect("store initializes");
    let producer_occurrence = record_producer(&mut coordinator, &producer_result);
    coordinator
        .put_continuation(Continuation {
            run_id: "run:consumer".to_owned(),
            plan_id: consumer_plan,
            binding_context: "binding:resource-test/1".to_owned(),
            frames: vec![FrameState {
                definition_id: "main".to_owned(),
                invocation_id: "main".to_owned(),
                invocation_path: Vec::new(),
                scope_id: cymule_core::ROOT_SCOPE_ID.to_owned(),
                input: cymule_core::artifact_ref("test/input", b"resource input")
                    .expect("input reference derives"),
                region_path: Vec::new(),
                next_step: 1,
                locals: BTreeMap::new(),
            }],
            state: None,
            wait_set: BTreeSet::new(),
            scope_stack: vec![cymule_core::ROOT_SCOPE_ID.to_owned()],
            effect_obligations: BTreeSet::new(),
            authority_leases: BTreeSet::new(),
            budget: BTreeMap::new(),
            causal_frontier: BTreeSet::new(),
            epoch: 0,
            status: ContinuationStatus::Ready,
        })
        .expect("Continuation stores");
    coordinator
        .register_wait(WaitCondition {
            wait_id: "wait:resource-input".to_owned(),
            run_id: "run:consumer".to_owned(),
            kind: WaitKind::Input {
                correlation: "input.dataset".to_owned(),
                schema: json!({}),
            },
            consume_once: true,
            owner: cymule_durable::WaitOwner {
                invocation_id: "main".to_owned(),
                definition_id: "main".to_owned(),
                site_id: "wait.resource-input".to_owned(),
                region_path: Vec::new(),
                step_index: 0,
                bind: None,
            },
            state: WaitState::Pending,
            result: None,
        })
        .expect("input wait registers");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:input-activation".to_owned(),
        producer: producer_provenance(&producer_occurrence, &producer_result),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: producer_result.clone(),
    };
    armed.store(true, Ordering::SeqCst);
    let lost = ResourceHandoffController::activate_input(
        &mut coordinator,
        &handoff,
        "wait:resource-input",
    );
    assert!(
        matches!(
            &lost,
            Err(ResourceError::Persistence(message))
                if message.contains("simulated lost handoff activation receipt")
        ),
        "unexpected activation result: {lost:?}"
    );
    let store = coordinator.into_store();
    let mut coordinator = DurableCoordinator::open(store).expect("lost receipt state reopens");
    let activation = ResourceHandoffController::activate_input(
        &mut coordinator,
        &handoff,
        "wait:resource-input",
    )
    .expect("handoff activation retry replays");
    let state = coordinator.state().expect("state reads");
    assert_eq!(
        state.waits["wait:resource-input"].result.as_ref(),
        Some(&activation.result)
    );
    assert_eq!(
        state.continuations["run:consumer"].status,
        ContinuationStatus::Ready
    );
    assert_eq!(
        ResourceHandoffController::incoming(&coordinator, "run:consumer")
            .expect("incoming handoff replays"),
        vec![handoff.clone()]
    );
    let artifact = coordinator
        .restore_machine()
        .expect("Machine restores")
        .artifact(&activation.result)
        .expect("input Artifact exists")
        .clone();
    assert_eq!(artifact.reference, handoff.resource);
    let registry =
        ArtifactTypeRegistry::with_framework_contracts().expect("framework contracts build");
    let decoded: ResourceHandle = registry
        .decode_typed(&artifact)
        .expect("Resource Handle decodes");
    decoded.verify().expect("Resource Handle verifies");
    assert_eq!(
        ResourceHandoffController::activate_input(
            &mut coordinator,
            &handoff,
            "wait:resource-input"
        )
        .expect("activation retry is idempotent"),
        activation
    );

    let store = coordinator.into_store();
    let reopened = DurableCoordinator::open(store).expect("store reopens");
    assert_eq!(
        reopened.state().expect("state reads").waits["wait:resource-input"].state,
        WaitState::Completed
    );
    reopened
        .restore_machine()
        .expect("reopened Machine restores")
        .verify_replay()
        .expect("reopened Machine replays");
}
