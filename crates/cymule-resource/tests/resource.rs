//! Fault-oriented resource descriptor, resolver, store, and handoff tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cymule_core::seal_plan;
use cymule_core::{
    COMMAND_VERSION, Command, CommandEnvelope, Definition, Expression, Machine, PlanCandidate,
    Region, sha256_bytes,
};
use cymule_durable::{
    Continuation, ContinuationStatus, DurableCoordinator, DurableError, DurableResult,
    DurableState, DurableStore, MemoryStore, StoreCommit, StoredState, WaitCondition, WaitKind,
    WaitState,
};
use cymule_resource::{
    ArtifactResolver, ArtifactStore, InlineData, ResourceCandidate, ResourceChunk, ResourceClient,
    ResourceError, ResourceHandle, ResourceHandoff, ResourceHandoffController, ResourceIntegrity,
    ResourceLocation, ResourceObservation, ResourcePage, ResourceReplayClass, ResourceShape,
    ResourceWriteIntent, ResourceWriteSession, ResourceWriter,
};
use serde_json::json;

#[derive(Clone)]
struct LostHandoffReceiptStore {
    inner: MemoryStore,
    armed: Arc<AtomicBool>,
}

impl DurableStore for LostHandoffReceiptStore {
    fn load(&mut self) -> DurableResult<Option<StoredState>> {
        self.inner.load()
    }

    fn compare_and_swap(
        &mut self,
        expected_revision: Option<&str>,
        next: &DurableState,
    ) -> DurableResult<StoreCommit> {
        let commit = self.inner.compare_and_swap(expected_revision, next)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(DurableError::Substrate(
                "simulated lost handoff activation receipt".to_owned(),
            ));
        }
        Ok(commit)
    }
}

fn external_candidate(
    shape: ResourceShape,
    integrity: ResourceIntegrity,
    locations: Vec<ResourceLocation>,
) -> ResourceCandidate {
    ResourceCandidate {
        resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
        shape,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity,
        locations,
        annotations: BTreeMap::from([("purpose".to_owned(), "conformance".to_owned())]),
    }
}

fn object(bytes: &[u8]) -> ResourceHandle {
    external_candidate(
        ResourceShape::Object,
        ResourceIntegrity::Content {
            digest: format!("sha256:{}", sha256_bytes(bytes)),
            size: bytes.len() as u64,
        },
        vec![ResourceLocation::Resolver {
            binding: "binding:memory-resolver/1".to_owned(),
            reference: "object:fixture".to_owned(),
        }],
    )
    .seal()
    .expect("object seals")
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
        locations: Vec::new(),
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
        "sha256:c5034cf635af3800a5b41ca18fd78665cc6ec595f6e87418b4220f8d0919261b"
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
    let first = external_candidate(
        ResourceShape::Object,
        integrity.clone(),
        vec![ResourceLocation::PublicUrl {
            url: "https://example.com/artifacts/object.bin".to_owned(),
        }],
    )
    .seal()
    .expect("public object seals");
    let moved = external_candidate(
        ResourceShape::Object,
        integrity,
        vec![ResourceLocation::Resolver {
            binding: "binding:remote-drive/7".to_owned(),
            reference: "item:stable-reference".to_owned(),
        }],
    )
    .seal()
    .expect("moved object seals");
    assert_eq!(first.resource_id, moved.resource_id);

    for url in [
        "https://user:secret@example.com/object",
        "https://example.com/object?token=secret",
        "https://example.com/object#access-token",
    ] {
        assert!(matches!(
            external_candidate(
                ResourceShape::Object,
                ResourceIntegrity::Live {
                    identity: "live:credential-test".to_owned(),
                },
                vec![ResourceLocation::PublicUrl {
                    url: url.to_owned()
                }],
            )
            .seal(),
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
        vec![ResourceLocation::Resolver {
            binding: "binding:sandbox-resolver/4".to_owned(),
            reference: "snapshot:opaque-17".to_owned(),
        }],
    )
    .seal()
    .expect("snapshot seals");
    let live_drive = external_candidate(
        ResourceShape::Directory,
        ResourceIntegrity::Live {
            identity: "drive-directory:team-docs".to_owned(),
        },
        vec![ResourceLocation::Resolver {
            binding: "binding:drive-resolver/2".to_owned(),
            reference: "directory:team-docs".to_owned(),
        }],
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
    ) -> cymule_resource::ResourceResult<ResourceObservation> {
        Ok(ResourceObservation {
            media_type: resource.media_type.clone(),
            integrity: resource.integrity.clone(),
        })
    }

    fn read(
        &mut self,
        _resource: &ResourceHandle,
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
        cursor: Option<&str>,
        limit: u32,
    ) -> cymule_resource::ResourceResult<ResourcePage> {
        let start = cursor
            .map_or(Ok(0), str::parse::<usize>)
            .map_err(|error| ResourceError::Substrate(error.to_string()))?;
        let end = self.entries.len().min(start.saturating_add(limit as usize));
        Ok(ResourcePage {
            entries: self.entries[start..end].to_vec(),
            next_cursor: (end < self.entries.len()).then(|| end.to_string()),
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

    let directory = external_candidate(
        ResourceShape::Directory,
        ResourceIntegrity::Version {
            authority: "directory-manifest/1".to_owned(),
            version: "manifest:1".to_owned(),
        },
        vec![ResourceLocation::Resolver {
            binding: "binding:directory/1".to_owned(),
            reference: "directory:fixture".to_owned(),
        }],
    )
    .seal()
    .expect("directory seals");
    let entries = vec![
        cymule_resource::ResourceEntry {
            name: "first.txt".to_owned(),
            resource: ResourceCandidate::text("first").seal().unwrap(),
        },
        cymule_resource::ResourceEntry {
            name: "nested/second.json".to_owned(),
            resource: ResourceCandidate::json(json!({"second": true}))
                .seal()
                .unwrap(),
        },
    ];
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
    ) -> cymule_resource::ResourceResult<ResourceHandle> {
        let (intent, bytes) = self
            .sessions
            .get(&session.write_id)
            .ok_or_else(|| ResourceError::NotFound(session.write_id.clone()))?;
        ResourceCandidate {
            resource_version: cymule_resource::RESOURCE_VERSION.to_owned(),
            shape: intent.shape,
            media_type: intent.media_type.clone(),
            inline: None,
            integrity: ResourceIntegrity::Content {
                digest: format!("sha256:{}", sha256_bytes(bytes)),
                size: bytes.len() as u64,
            },
            locations: vec![ResourceLocation::Resolver {
                binding: session.store_binding.clone(),
                reference: session.upload_id.clone(),
            }],
            annotations: intent.annotations.clone(),
        }
        .seal()
    }

    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> cymule_resource::ResourceResult<()> {
        self.sessions.remove(&session.write_id);
        Ok(())
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
    assert_eq!(handle.replay_class(), ResourceReplayClass::ContentVerified);
    assert!(matches!(
        handle.integrity,
        ResourceIntegrity::Content { size: 12, .. }
    ));
}

fn machine_with_runs() -> Machine {
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
                steps: Vec::new(),
                result: Expression::Literal { value: json!(null) },
            },
        }],
        metadata: BTreeMap::new(),
    })
    .expect("Plan seals");
    machine.insert_plan(plan.clone()).expect("Plan inserts");
    for run_id in ["run:producer", "run:consumer"] {
        machine
            .submit(CommandEnvelope {
                command_version: COMMAND_VERSION.to_owned(),
                command_id: format!("command:start:{run_id}"),
                actor: "actor:resource-test".to_owned(),
                run_id: run_id.to_owned(),
                expected_precondition: None,
                command: Command::StartRun {
                    plan_id: plan.plan_id.clone(),
                    binding_context: "binding:resource-test/1".to_owned(),
                },
            })
            .expect("Run starts");
    }
    machine
}

#[test]
fn run_to_run_handoff_is_idempotent_conflict_checked_and_reopenable() {
    let store = MemoryStore::new();
    let mut coordinator = DurableCoordinator::open(store.clone())
        .expect("store opens")
        .initialize(&machine_with_runs())
        .expect("store initializes");
    let mut stale = DurableCoordinator::open(store.clone()).expect("stale view opens");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:producer-result".to_owned(),
        from_run: "run:producer".to_owned(),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: ResourceCandidate::text("producer output").seal().unwrap(),
    };
    ResourceHandoffController::transfer(&mut coordinator, &handoff).expect("handoff commits");
    assert!(matches!(
        ResourceHandoffController::transfer(&mut stale, &handoff),
        Err(ResourceError::Persistence(_))
    ));
    ResourceHandoffController::transfer(&mut coordinator, &handoff)
        .expect("handoff retry is idempotent");
    let mut conflicting = handoff.clone();
    conflicting.resource = ResourceCandidate::text("different output").seal().unwrap();
    assert!(matches!(
        ResourceHandoffController::transfer(&mut coordinator, &conflicting),
        Err(ResourceError::Conflict(_))
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
    let machine = machine_with_runs();
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
    coordinator
        .put_continuation(Continuation {
            run_id: "run:consumer".to_owned(),
            plan_id: consumer_plan,
            binding_context: "binding:resource-test/1".to_owned(),
            frames: Vec::new(),
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
            result_binding: None,
            state: WaitState::Pending,
            result: None,
        })
        .expect("input wait registers");
    let handoff = ResourceHandoff {
        handoff_version: cymule_resource::RESOURCE_HANDOFF_VERSION.to_owned(),
        transfer_id: "transfer:input-activation".to_owned(),
        from_run: "run:producer".to_owned(),
        to_run: "run:consumer".to_owned(),
        slot: "input.dataset".to_owned(),
        resource: ResourceCandidate::text("producer output")
            .seal()
            .expect("Resource seals"),
    };
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        ResourceHandoffController::activate_input(
            &mut coordinator,
            &handoff,
            "wait:resource-input"
        ),
        Err(ResourceError::Persistence(message))
            if message.contains("simulated lost handoff activation receipt")
    ));
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
    assert_eq!(
        serde_json::from_slice::<ResourceHandle>(&artifact.bytes).expect("Resource Handle decodes"),
        handoff.resource
    );
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
