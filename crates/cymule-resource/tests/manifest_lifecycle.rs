//! Property and fault tests for manifest proofs and Resource lifecycle receipts.

use std::collections::{BTreeMap, BTreeSet};

use cymule_resource::{
    MAX_LIST_PAGE, MAX_MANIFEST_PROOF_DEPTH, MAX_RESOURCE_CLEANUP_PLAN_BYTES,
    ManifestInclusionProof, ManifestPredecessorProof, MerkleSide, MerkleStep,
    RESOURCE_CLEANUP_PLAN_VERSION, RESOURCE_LOCATOR_VERSION, RESOURCE_MANIFEST_MEDIA_TYPE,
    RESOURCE_MANIFEST_VERSION, RESOURCE_VERSION, ResourceCandidate, ResourceCleanupPlan,
    ResourceCleanupTarget, ResourceCleanupTargetKind, ResourceError, ResourceIntegrity,
    ResourceListCursor, ResourceListProof, ResourceLocation, ResourceLocatorSet,
    ResourceManifestAccumulator, ResourceManifestDescriptor, ResourceManifestEntry,
    ResourceManifestStreamVerifier, ResourcePublication, ResourceShape, ResourceWriteSession,
    SealedResourceManifest, canonical_manifest_entry_bytes, manifest_leaf_digest,
    manifest_node_digest,
};
use jsonschema::{Draft, Validator};
use proptest::prelude::*;
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct TestResourceIdentity<'a> {
    resource_version: &'a str,
    shape: ResourceShape,
    media_type: &'a str,
    inline: Option<&'a cymule_resource::InlineData>,
    integrity: &'a ResourceIntegrity,
    manifest: Option<&'a cymule_resource::ResourceManifestDescriptor>,
    annotations: &'a BTreeMap<String, String>,
}

fn entries(values: &[u16]) -> Vec<ResourceManifestEntry> {
    let unique: BTreeSet<u16> = values.iter().copied().collect();
    unique
        .into_iter()
        .map(|value| ResourceManifestEntry {
            name: format!("entry-{value:05}.txt"),
            resource: ResourceCandidate::text(format!("payload-{value}"))
                .seal()
                .expect("generated Resource seals"),
        })
        .collect()
}

fn retained_publication(annotations: BTreeMap<String, String>) -> ResourcePublication {
    let bytes = b"retained";
    let digest = format!("sha256:{}", cymule_core::sha256_bytes(bytes));
    let resource = ResourceCandidate {
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: digest.clone(),
            size: bytes.len() as u64,
        },
        manifest: None,
        annotations,
    }
    .seal()
    .expect("Resource seals");
    ResourcePublication {
        locators: ResourceLocatorSet {
            locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: "store:test/1".to_owned(),
            locations: vec![ResourceLocation::Opaque { reference: digest }],
        },
        resource,
    }
}

fn manifest_publication(sealed: &SealedResourceManifest) -> ResourcePublication {
    descriptor_publication(&sealed.descriptor)
}

fn descriptor_publication(descriptor: &ResourceManifestDescriptor) -> ResourcePublication {
    let resource = ResourceCandidate {
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Directory,
        media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Content {
            digest: descriptor.digest.clone(),
            size: descriptor.size,
        },
        manifest: Some(descriptor.clone()),
        annotations: BTreeMap::new(),
    }
    .seal()
    .expect("manifest Resource seals");
    ResourcePublication {
        locators: ResourceLocatorSet {
            locator_version: RESOURCE_LOCATOR_VERSION.to_owned(),
            resource_id: resource.resource_id.clone(),
            resolver_binding: "store:manifest-proof/1".to_owned(),
            locations: vec![ResourceLocation::Opaque {
                reference: descriptor.digest.clone(),
            }],
        },
        resource,
    }
}

struct ForgedManifest {
    descriptor: ResourceManifestDescriptor,
    entries: Vec<ResourceManifestEntry>,
    levels: Vec<Vec<String>>,
}

impl ForgedManifest {
    fn new(entries: Vec<ResourceManifestEntry>) -> Self {
        assert!(!entries.is_empty());
        let size = entries
            .iter()
            .map(|entry| canonical_manifest_entry_bytes(entry).unwrap().len() as u64 + 1)
            .sum();
        let mut levels = vec![
            entries
                .iter()
                .map(|entry| manifest_leaf_digest(entry).unwrap())
                .collect::<Vec<_>>(),
        ];
        while levels.last().unwrap().len() > 1 {
            let parent = levels
                .last()
                .unwrap()
                .chunks(2)
                .map(|pair| {
                    manifest_node_digest(&pair[0], pair.get(1).unwrap_or(&pair[0])).unwrap()
                })
                .collect();
            levels.push(parent);
        }
        let root_digest = levels.last().unwrap()[0].clone();
        let entry_count = entries.len() as u64;
        let digest = cymule_profile_protocol::resource::resource_manifest_descriptor_id(
            RESOURCE_MANIFEST_MEDIA_TYPE,
            size,
            entry_count,
            &root_digest,
        )
        .unwrap();
        Self {
            descriptor: ResourceManifestDescriptor {
                manifest_version: RESOURCE_MANIFEST_VERSION.to_owned(),
                media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
                digest,
                size,
                entry_count,
                root_digest,
            },
            entries,
            levels,
        }
    }

    fn inclusion(&self, index: usize) -> ManifestInclusionProof {
        let mut position = index;
        let mut path = Vec::new();
        for level in self.levels.iter().take(self.levels.len() - 1) {
            let (digest, side) = if position.is_multiple_of(2) {
                (
                    level.get(position + 1).unwrap_or(&level[position]),
                    MerkleSide::Right,
                )
            } else {
                (&level[position - 1], MerkleSide::Left)
            };
            path.push(MerkleStep {
                side,
                digest: digest.clone(),
            });
            position /= 2;
        }
        ManifestInclusionProof {
            index: index as u64,
            path,
        }
    }
}

fn page_cursors(
    publication: &ResourcePublication,
    entries: &[ResourceManifestEntry],
    start: usize,
    count: usize,
) -> (Option<String>, Option<String>) {
    let request = (start > 0)
        .then(|| ResourceListCursor::for_page(publication, None, 1000, 0, &entries[..start]))
        .transpose()
        .expect("predecessor cursor seals");
    let end = start + count;
    let next = (end < entries.len())
        .then(|| {
            ResourceListCursor::for_page(
                publication,
                request.as_deref(),
                1000,
                start as u64,
                &entries[start..end],
            )
        })
        .transpose()
        .expect("successor cursor seals");
    (request, next)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn every_bounded_page_proves_exact_manifest_membership(
        values in prop::collection::vec(any::<u16>(), 0..96),
        requested_start in any::<usize>(),
        requested_count in 1usize..24,
    ) {
        let entries = entries(&values);
        let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
        let publication = manifest_publication(&sealed);
        let start = if entries.is_empty() { 0 } else { requested_start % entries.len() };
        let count = if entries.is_empty() { 0 } else { requested_count.min(entries.len() - start) };
        let (request, next) = page_cursors(&publication, &entries, start, count);
        let proof = sealed.proof(start as u64, count, request.as_deref(), next.as_deref()).expect("proof builds");
        proof
            .verify_page(&sealed.descriptor, &entries[start..start + count], request.as_deref(), next.as_deref())
            .expect("exact page verifies");

        if count > 0 {
            let mut changed = entries[start..start + count].to_vec();
            changed[0].name.push_str("-changed");
            prop_assert!(matches!(
                proof.verify_page(&sealed.descriptor, &changed, request.as_deref(), next.as_deref()),
                Err(ResourceError::Integrity { .. })
            ), "tampered manifest page must fail integrity verification");
        }
    }
}

#[test]
fn manifest_proof_rejects_wrong_root_index_and_descriptor() {
    let entries = entries(&[1, 2, 3, 4, 5]);
    let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
    let publication = manifest_publication(&sealed);
    let (request, next) = page_cursors(&publication, &entries, 1, 3);
    let proof = sealed
        .proof(1, 3, request.as_deref(), next.as_deref())
        .expect("proof builds");

    let mut wrong_root = sealed.descriptor.clone();
    wrong_root.root_digest = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        proof.verify_page(
            &wrong_root,
            &entries[1..4],
            request.as_deref(),
            next.as_deref()
        ),
        Err(ResourceError::Integrity { .. })
    ));

    let mut wrong_index = proof.clone();
    wrong_index.inclusions[1].index += 1;
    assert!(matches!(
        wrong_index.verify_page(
            &sealed.descriptor,
            &entries[1..4],
            request.as_deref(),
            next.as_deref()
        ),
        Err(ResourceError::Integrity { .. })
    ));

    let mut wrong_manifest = proof;
    wrong_manifest.manifest_digest = format!("sha256:{}", "f".repeat(64));
    assert!(matches!(
        wrong_manifest.verify_page(
            &sealed.descriptor,
            &entries[1..4],
            request.as_deref(),
            next.as_deref()
        ),
        Err(ResourceError::Integrity { .. })
    ));

    let (one_request, one_next) = page_cursors(&publication, &entries, 1, 1);
    let mut wrong_path = sealed
        .proof(1, 1, one_request.as_deref(), one_next.as_deref())
        .expect("proof builds");
    wrong_path.inclusions[0].path[0].side = cymule_resource::MerkleSide::Right;
    assert!(matches!(
        wrong_path.verify_page(
            &sealed.descriptor,
            &entries[1..2],
            one_request.as_deref(),
            one_next.as_deref()
        ),
        Err(ResourceError::Integrity { .. })
    ));

    assert!(matches!(
        sealed
            .proof(1, 3, request.as_deref(), next.as_deref())
            .expect("proof builds")
            .verify_page(&sealed.descriptor, &entries[1..4], None, next.as_deref()),
        Err(ResourceError::Integrity { .. })
    ));
}

#[test]
fn manifest_inclusion_requires_canonical_odd_tail_duplication_at_every_level() {
    for count in [3_usize, 6] {
        let values = (0..count.next_power_of_two())
            .map(|value| u16::try_from(value).expect("small manifest index fits"))
            .collect::<Vec<_>>();
        let expanded = ForgedManifest::new(entries(&values));
        let selected = &expanded.entries[..count];
        let canonical = SealedResourceManifest::seal(selected.to_vec())
            .expect("canonical odd-width manifest seals");
        canonical
            .proof(0, count, None, None)
            .expect("canonical full-page proof builds")
            .verify_page(&canonical.descriptor, selected, None, None)
            .expect("canonical duplicated tails verify at every level");

        let mut descriptor = expanded.descriptor.clone();
        descriptor.entry_count = count as u64;
        descriptor.size = canonical.descriptor.size;
        descriptor.digest = cymule_resource::resource_manifest_descriptor_id(
            &descriptor.media_type,
            descriptor.size,
            descriptor.entry_count,
            &descriptor.root_digest,
        )
        .expect("descriptor identity derives from the declared tuple");
        descriptor.verify().expect("descriptor shape is valid");
        let proof = ResourceListProof::from_inclusions(
            &descriptor,
            0,
            None,
            (0..count).map(|index| expanded.inclusion(index)).collect(),
            None,
            None,
        )
        .expect("entry-count-sized proof is structurally valid");
        assert!(matches!(
            proof.verify_page(&descriptor, selected, None, None),
            Err(ResourceError::Integrity { code, .. })
                if code == "resource_manifest_inclusion_tail_mismatch"
        ));
    }
}

#[test]
fn manifest_proof_enforces_the_public_page_entry_limit() {
    let values = (0..=MAX_LIST_PAGE)
        .map(|value| u16::try_from(value).expect("test page index fits u16"))
        .collect::<Vec<_>>();
    let entries = entries(&values);
    let sealed = SealedResourceManifest::seal(entries.clone()).expect("large manifest seals");
    let publication = manifest_publication(&sealed);
    let exact_count = usize::try_from(MAX_LIST_PAGE).expect("page bound fits usize");
    let next = ResourceListCursor::for_page(
        &publication,
        None,
        MAX_LIST_PAGE,
        0,
        &entries[..exact_count],
    )
    .expect("exact-limit successor cursor seals");
    let exact = sealed
        .proof(0, exact_count, None, Some(&next))
        .expect("exact-limit proof builds");
    exact
        .verify_page(
            &sealed.descriptor,
            &entries[..exact_count],
            None,
            Some(&next),
        )
        .expect("exact-limit proof verifies");

    let final_inclusion = sealed
        .proof(exact_count as u64, 1, Some(&next), None)
        .expect("terminal singleton proof builds")
        .inclusions
        .into_iter()
        .next()
        .expect("terminal proof contains one inclusion");
    let mut oversized_inclusions = exact.inclusions.clone();
    oversized_inclusions.push(final_inclusion);
    assert!(
        ResourceListProof::from_inclusions(
            &sealed.descriptor,
            0,
            None,
            oversized_inclusions.clone(),
            None,
            None,
        )
        .is_err()
    );

    let mut forged = exact;
    forged.inclusions = oversized_inclusions;
    assert!(matches!(
        forged.verify_page(&sealed.descriptor, &entries, None, Some(&next)),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_manifest_page_count_exceeded"
    ));
}

fn maximum_depth_manifest() -> (
    ResourceManifestDescriptor,
    Vec<ResourceManifestEntry>,
    Vec<ManifestInclusionProof>,
) {
    let entries = entries(&[1, 2]);
    let leaves = entries
        .iter()
        .map(|entry| manifest_leaf_digest(entry).expect("test leaf derives"))
        .collect::<Vec<_>>();
    let shared_step = MerkleStep {
        side: MerkleSide::Right,
        digest: format!("sha256:{}", "b".repeat(64)),
    };
    let mut left_path = vec![shared_step.clone(); MAX_MANIFEST_PROOF_DEPTH];
    left_path[0].digest.clone_from(&leaves[1]);
    let mut right_path = left_path.clone();
    right_path[0] = MerkleStep {
        side: MerkleSide::Left,
        digest: leaves[0].clone(),
    };
    let root_digest = left_path.iter().fold(leaves[0].clone(), |digest, step| {
        manifest_node_digest(&digest, &step.digest).expect("test parent derives")
    });
    let entry_count = cymule_core::MAX_EXACT_INTEGER;
    let size = cymule_core::MAX_EXACT_INTEGER;
    let descriptor = ResourceManifestDescriptor {
        manifest_version: RESOURCE_MANIFEST_VERSION.to_owned(),
        media_type: RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        digest: cymule_resource::resource_manifest_descriptor_id(
            RESOURCE_MANIFEST_MEDIA_TYPE,
            size,
            entry_count,
            &root_digest,
        )
        .expect("maximum-tree descriptor identity derives"),
        size,
        entry_count,
        root_digest,
    };
    let inclusions = vec![
        ManifestInclusionProof {
            index: 0,
            path: left_path,
        },
        ManifestInclusionProof {
            index: 1,
            path: right_path,
        },
    ];
    (descriptor, entries, inclusions)
}

#[test]
fn manifest_proof_accepts_53_siblings_and_rejects_54_before_page_verification() {
    let (descriptor, entries, inclusions) = maximum_depth_manifest();
    let publication = descriptor_publication(&descriptor);
    let next = ResourceListCursor::for_page(&publication, None, 2, 0, &entries)
        .expect("large-tree page cursor seals");
    let exact =
        ResourceListProof::from_inclusions(&descriptor, 0, None, inclusions, None, Some(&next))
            .expect("53-sibling paths fit the exact-integer tree");
    exact
        .verify_page(&descriptor, &entries, None, Some(&next))
        .expect("53-sibling inclusions reach their exact root");

    let mut oversized = exact;
    let extra = oversized.inclusions[0].path[0].clone();
    oversized.inclusions[0].path.push(extra);
    assert!(matches!(
        ResourceListProof::from_inclusions(
            &descriptor,
            0,
            None,
            oversized.inclusions.clone(),
            None,
            Some(&next),
        ),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        oversized.verify_page(&descriptor, &entries, None, Some(&next)),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_manifest_proof_depth_exceeded"
    ));
}

#[test]
fn manifest_proof_limits_predecessor_path_to_53_siblings_too() {
    let (descriptor, entries, inclusions) = maximum_depth_manifest();
    let publication = descriptor_publication(&descriptor);
    let request = ResourceListCursor::for_page(&publication, None, 1, 0, &entries[..1])
        .expect("predecessor cursor seals");
    let next = ResourceListCursor::for_page(&publication, Some(&request), 1, 1, &entries[1..])
        .expect("following page cursor seals");
    let exact = ResourceListProof::from_inclusions(
        &descriptor,
        1,
        Some(ManifestPredecessorProof {
            entry: entries[0].clone(),
            inclusion: inclusions[0].clone(),
        }),
        vec![inclusions[1].clone()],
        Some(&request),
        Some(&next),
    )
    .expect("53-sibling predecessor proof constructs");
    exact
        .verify_page(&descriptor, &entries[1..], Some(&request), Some(&next))
        .expect("maximum-depth predecessor and page reach the same root");

    let mut oversized = exact;
    let predecessor = oversized.predecessor.as_mut().expect("predecessor exists");
    let extra = predecessor.inclusion.path[0].clone();
    predecessor.inclusion.path.push(extra);
    assert!(matches!(
        ResourceListProof::from_inclusions(
            &descriptor,
            1,
            oversized.predecessor.clone(),
            oversized.inclusions.clone(),
            Some(&request),
            Some(&next),
        ),
        Err(ResourceError::Validation(_))
    ));
    assert!(matches!(
        oversized.verify_page(&descriptor, &entries[1..], Some(&request), Some(&next)),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_manifest_proof_depth_exceeded"
    ));
}

#[test]
fn manifest_proof_rejects_self_consistent_unsorted_page() {
    let mut unsorted = entries(&[1, 2]);
    unsorted.swap(0, 1);
    let forged = ForgedManifest::new(unsorted.clone());
    forged
        .descriptor
        .verify()
        .expect("forged descriptor is self-consistent");
    let proof = ResourceListProof::from_inclusions(
        &forged.descriptor,
        0,
        None,
        vec![forged.inclusion(0), forged.inclusion(1)],
        None,
        None,
    )
    .expect("self-consistent Merkle proof constructs");
    assert!(matches!(
        proof.verify_page(&forged.descriptor, &unsorted, None, None),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_list_page_order_invalid"
    ));
}

#[test]
fn manifest_proof_rejects_cross_page_reverse_and_duplicate_names() {
    let mut reverse_entries = entries(&[1, 2]);
    reverse_entries.swap(0, 1);
    let reverse = ForgedManifest::new(reverse_entries.clone());
    let publication = descriptor_publication(&reverse.descriptor);
    let cursor = ResourceListCursor::for_page(&publication, None, 1, 0, &reverse_entries[..1])
        .expect("first reverse page cursor seals");
    let proof = ResourceListProof::from_inclusions(
        &reverse.descriptor,
        1,
        Some(ManifestPredecessorProof {
            entry: reverse_entries[0].clone(),
            inclusion: reverse.inclusion(0),
        }),
        vec![reverse.inclusion(1)],
        Some(&cursor),
        None,
    )
    .expect("cross-page Merkle proof constructs");
    assert!(matches!(
        proof.verify_page(&reverse.descriptor, &reverse_entries[1..], Some(&cursor), None),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_list_predecessor_order_mismatch"
    ));

    let mut repeated_entries = entries(&[1, 2]);
    repeated_entries[1].name = repeated_entries[0].name.clone();
    let repeated = ForgedManifest::new(repeated_entries.clone());
    let publication = descriptor_publication(&repeated.descriptor);
    let cursor = ResourceListCursor::for_page(&publication, None, 1, 0, &repeated_entries[..1])
        .expect("first repeated-name page cursor seals");
    let proof = ResourceListProof::from_inclusions(
        &repeated.descriptor,
        1,
        Some(ManifestPredecessorProof {
            entry: repeated_entries[0].clone(),
            inclusion: repeated.inclusion(0),
        }),
        vec![repeated.inclusion(1)],
        Some(&cursor),
        None,
    )
    .expect("repeated-name Merkle proof constructs");
    assert!(matches!(
        proof.verify_page(
            &repeated.descriptor,
            &repeated_entries[1..],
            Some(&cursor),
            None
        ),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_list_predecessor_order_mismatch"
    ));
}

#[test]
fn manifest_proof_rejects_publicly_recomputed_cursor_without_exact_predecessor() {
    let forged = ForgedManifest::new(entries(&[1, 2]));
    let publication = descriptor_publication(&forged.descriptor);
    let false_predecessor = ResourceManifestEntry {
        name: "entry-00000.txt".to_owned(),
        resource: ResourceCandidate::text("false predecessor")
            .seal()
            .expect("fixture Resource seals"),
    };
    let cursor = ResourceListCursor::for_page(
        &publication,
        None,
        1,
        0,
        std::slice::from_ref(&false_predecessor),
    )
    .expect("public cursor constructor recomputes a self-consistent token");
    let proof = ResourceListProof::from_inclusions(
        &forged.descriptor,
        1,
        Some(ManifestPredecessorProof {
            entry: forged.entries[0].clone(),
            inclusion: forged.inclusion(0),
        }),
        vec![forged.inclusion(1)],
        Some(&cursor),
        None,
    )
    .expect("root-verifiable boundary proof constructs");
    let reloaded: ResourceListProof = cymule_core::decode_json(
        &cymule_core::canonical_bytes(&proof).expect("proof persists canonically"),
    )
    .expect("proof reloads after restart");
    assert!(matches!(
        reloaded.verify_page(
            &forged.descriptor,
            &forged.entries[1..],
            Some(&cursor),
            None
        ),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_list_cursor_predecessor_mismatch"
    ));
}

#[test]
fn manifest_proof_closes_initial_empty_and_terminal_page_shapes() {
    let empty = SealedResourceManifest::seal(Vec::new()).expect("empty manifest seals");
    let empty_proof = empty.proof(0, 0, None, None).expect("empty proof closes");
    let mut missing_predecessor = serde_json::to_value(&empty_proof).expect("proof serializes");
    missing_predecessor
        .as_object_mut()
        .expect("proof is an object")
        .remove("predecessor");
    assert!(serde_json::from_value::<ResourceListProof>(missing_predecessor).is_err());

    let sealed = SealedResourceManifest::seal(entries(&[1, 2])).expect("manifest seals");
    let publication = manifest_publication(&sealed);
    let successor = ResourceListCursor::for_page(&publication, None, 1, 0, &sealed.entries()[..1])
        .expect("non-terminal successor seals");
    let missing_successor = ResourceListProof::from_inclusions(
        &sealed.descriptor,
        0,
        None,
        vec![
            sealed
                .proof(0, 1, None, Some(&successor))
                .expect("valid first proof")
                .inclusions[0]
                .clone(),
        ],
        None,
        None,
    )
    .expect("shape-only proof constructs");
    assert!(matches!(
        missing_successor.verify_page(&sealed.descriptor, &sealed.entries()[..1], None, None),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_list_successor_cursor_missing"
    ));
    assert!(
        ResourceListProof::from_inclusions(
            &sealed.descriptor,
            0,
            Some(ManifestPredecessorProof {
                entry: sealed.entries()[0].clone(),
                inclusion: sealed
                    .proof(0, 1, None, Some(&successor))
                    .expect("valid first proof")
                    .inclusions[0]
                    .clone(),
            }),
            Vec::new(),
            None,
            None,
        )
        .is_err()
    );
}

#[test]
fn manifest_descriptor_digest_and_merkle_root_have_one_authority() {
    let first = SealedResourceManifest::seal(entries(&[1, 2])).expect("first manifest seals");
    let second = SealedResourceManifest::seal(entries(&[3, 4])).expect("second manifest seals");
    assert_ne!(first.descriptor.root_digest, second.descriptor.root_digest);

    let mut mixed = first.descriptor.clone();
    mixed.root_digest = second.descriptor.root_digest;
    let integrity = ResourceIntegrity::Content {
        digest: mixed.digest.clone(),
        size: mixed.size,
    };
    let annotations = BTreeMap::new();
    let resource_id = cymule_core::content_id(
        RESOURCE_VERSION,
        &TestResourceIdentity {
            resource_version: RESOURCE_VERSION,
            shape: ResourceShape::Directory,
            media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE,
            inline: None,
            integrity: &integrity,
            manifest: Some(&mixed),
            annotations: &annotations,
        },
    )
    .expect("malicious outer Resource ID recomputes");
    let handle = cymule_resource::ResourceHandle {
        resource_id,
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Directory,
        media_type: cymule_resource::RESOURCE_MANIFEST_MEDIA_TYPE.to_owned(),
        inline: None,
        integrity,
        manifest: Some(mixed),
        annotations,
    };
    assert!(matches!(
        handle.verify(),
        Err(ResourceError::Integrity { .. })
    ));
}

#[test]
fn empty_manifest_has_one_stable_descriptor_and_stream_authority() {
    let first = SealedResourceManifest::seal(Vec::new()).expect("empty manifest seals");
    let second = SealedResourceManifest::seal(Vec::new()).expect("empty manifest reseals");
    let streamed = ResourceManifestStreamVerifier::new()
        .finish()
        .expect("empty stream closes");
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(first.descriptor, streamed);
    assert_eq!(
        first.descriptor.digest,
        "sha256:936574b81c099aaf0318e91b80912c958a0527ee271eabd62da35fed55607928"
    );
    assert_ne!(
        first.descriptor.digest,
        format!("sha256:{}", cymule_core::sha256_bytes(&[]))
    );
    first
        .descriptor
        .verify()
        .expect("empty descriptor verifies");
    assert!(first.bytes.is_empty());
}

#[test]
fn streaming_merkle_matches_sealed_tree_and_old_generations_are_rejected() {
    for count in 0_u16..=257 {
        let values = (0..count).collect::<Vec<_>>();
        let entries = entries(&values);
        let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
        let mut streaming = ResourceManifestAccumulator::new();
        for entry in &entries {
            streaming.push(entry).expect("streaming entry admits");
        }
        assert_eq!(streaming.entry_count(), entries.len() as u64);
        assert_eq!(
            streaming
                .descriptor()
                .expect("streaming descriptor derives"),
            sealed.descriptor
        );
        assert_eq!(
            streaming.root_digest().expect("streaming root derives"),
            sealed.descriptor.root_digest
        );
    }

    let sealed = SealedResourceManifest::seal(entries(&[1, 2, 3])).expect("manifest seals");
    let mut legacy_descriptor = sealed.descriptor.clone();
    legacy_descriptor.manifest_version = "cymule.resource-manifest/2".to_owned();
    assert!(matches!(
        legacy_descriptor.verify(),
        Err(ResourceError::Validation(message)) if message.contains("unsupported")
    ));
    let publication = manifest_publication(&sealed);
    let (request, next) = page_cursors(&publication, sealed.entries(), 0, 1);
    let mut legacy_proof = sealed
        .proof(0, 1, request.as_deref(), next.as_deref())
        .expect("proof builds");
    legacy_proof.proof_version = "cymule.resource-list-proof/4".to_owned();
    assert!(matches!(
        legacy_proof.verify_page(
            &sealed.descriptor,
            &sealed.entries()[..1],
            request.as_deref(),
            next.as_deref()
        ),
        Err(ResourceError::Integrity { .. })
    ));
}

fn admit_retention_pin(
    pin: &cymule_resource::ResourcePin,
    current: Option<&cymule_resource::ResourceRetentionCurrent>,
) -> cymule_resource::ResourcePinPostcondition {
    let command = cymule_resource::ResourceCommand::new(cymule_resource::ResourceOperation::Pin {
        pin: pin.clone(),
    })
    .expect("pin command seals");
    let receipt =
        cymule_resource::reduce_resource_pin_receipt(&command.command_id, pin, current, None)
            .expect("pin reduces against the exact physical family");
    let command_receipt = cymule_resource::ResourceCommandReceipt::new(
        command,
        cymule_resource::ResourceCommandOutcome::Pin {
            receipt: receipt.clone(),
        },
    )
    .expect("pin command receipt seals");
    cymule_resource::project_resource_pin_receipt(
        &receipt,
        cymule_resource::ResourceLifecycleReceiptRef::from_resource(&command_receipt)
            .expect("pin origin closes"),
        current,
        None,
    )
    .expect("pin projects")
}

#[test]
fn physical_retention_key_prevents_same_bytes_with_different_annotations_from_deletion() {
    let first = retained_publication(BTreeMap::from([(
        "semantic-owner".to_owned(),
        "first".to_owned(),
    )]));
    let second = retained_publication(BTreeMap::from([(
        "semantic-owner".to_owned(),
        "second".to_owned(),
    )]));
    assert_ne!(first.resource.resource_id, second.resource.resource_id);
    assert_eq!(
        cymule_resource::resource_retention_key(&first).expect("first key derives"),
        cymule_resource::resource_retention_key(&second).expect("second key derives")
    );

    let first_subject = cymule_resource::ResourceRetentionSubject::from_publication(&first)
        .expect("first subject derives");
    let second_subject = cymule_resource::ResourceRetentionSubject::from_publication(&second)
        .expect("second subject derives");
    assert_ne!(first_subject.resource_id, second_subject.resource_id);
    assert_eq!(first_subject.family, second_subject.family);

    let first_pin = cymule_resource::ResourcePin::explicit("pin:first", first_subject, "run:first")
        .expect("first pin seals");
    let first_post = admit_retention_pin(&first_pin, None);

    let second_pin =
        cymule_resource::ResourcePin::explicit("pin:second", second_subject, "run:second")
            .expect("second pin seals");
    let second_post = admit_retention_pin(&second_pin, Some(&first_post.retention));
    assert_eq!(second_post.retention.active_pin_count, 2);

    let release_command =
        cymule_resource::ResourceCommand::new(cymule_resource::ResourceOperation::Release {
            release_id: "release:first".to_owned(),
            pin_id: first_pin.pin_id.clone(),
            owner: first_pin.owner.clone(),
        })
        .expect("release command seals");
    let release_receipt = cymule_resource::reduce_resource_release_receipt(
        &release_command.command_id,
        "release:first",
        &first_pin.pin_id,
        &first_pin.owner,
        &second_post.retention,
        &first_post.pin,
    )
    .expect("first pin releases");
    let release_command_receipt = cymule_resource::ResourceCommandReceipt::new(
        release_command,
        cymule_resource::ResourceCommandOutcome::Release {
            receipt: release_receipt.clone(),
        },
    )
    .expect("release command receipt seals");
    let release_post = cymule_resource::project_resource_release_receipt(
        &release_receipt,
        cymule_resource::ResourceLifecycleReceiptRef::from_resource(&release_command_receipt)
            .expect("release origin closes"),
        &second_post.retention,
        &first_post.pin,
    )
    .expect("release projects");
    assert_eq!(release_post.retention.active_pin_count, 1);

    let mut relabeled = release_post.retention.family.clone();
    relabeled.store_binding = "store:other/1".to_owned();
    assert!(matches!(
        relabeled.verify(),
        Err(ResourceError::Integrity { code, .. })
            if code == "resource_retention_key_mismatch"
    ));
}

#[test]
fn cleanup_receipt_is_the_unique_terminal_projection_of_its_persisted_plan() {
    let session = ResourceWriteSession {
        write_id: "写".repeat(512),
        upload_id: "upload:cleanup".to_owned(),
        store_binding: "store:test/1".to_owned(),
    };
    let targets = vec![
        ResourceCleanupTarget {
            kind: ResourceCleanupTargetKind::StagingObject,
            identifier: "staging/manifest-index".to_owned(),
        },
        ResourceCleanupTarget {
            kind: ResourceCleanupTargetKind::Chunk,
            identifier: "chunks/00000000000000000000".to_owned(),
        },
        ResourceCleanupTarget {
            kind: ResourceCleanupTargetKind::Chunk,
            identifier: "chunks/00000000000000000001".to_owned(),
        },
    ];
    let plan = ResourceCleanupPlan::new(&session, targets.clone()).expect("cleanup plan seals");
    let receipt = plan.receipt().expect("terminal receipt derives");
    assert_eq!(receipt.removed_staging_objects, 1);
    assert_eq!(receipt.removed_chunks, 2);
    assert_eq!(plan.receipt().expect("receipt re-derives"), receipt);
    receipt
        .verify()
        .expect("receipt verifies from its full plan");

    let mut changed_count = receipt.clone();
    changed_count.removed_chunks = 0;
    assert!(matches!(
        changed_count.verify(),
        Err(ResourceError::Integrity { .. })
    ));
    let mut changed_target = receipt.clone();
    changed_target.plan.targets[2]
        .identifier
        .push_str("-changed");
    assert!(matches!(
        changed_target.verify(),
        Err(ResourceError::Integrity { .. })
    ));
    let mut unsorted = targets;
    unsorted.swap(1, 2);
    assert!(matches!(
        ResourceCleanupPlan::new(&session, unsorted),
        Err(ResourceError::Validation(_))
    ));
}

fn cleanup_plan_template(
    session: &ResourceWriteSession,
    targets: Vec<ResourceCleanupTarget>,
) -> ResourceCleanupPlan {
    ResourceCleanupPlan {
        plan_version: RESOURCE_CLEANUP_PLAN_VERSION.to_owned(),
        plan_id: format!("sha256:{}", "0".repeat(64)),
        write_id: session.write_id.clone(),
        upload_id: session.upload_id.clone(),
        store_binding: session.store_binding.clone(),
        targets,
    }
}

fn canonical_len(value: &impl Serialize) -> usize {
    cymule_core::canonical_bytes(value)
        .expect("test value canonicalizes")
        .len()
}

fn exact_cleanup_plan_targets(session: &ResourceWriteSession) -> Vec<ResourceCleanupTarget> {
    const REGULAR_IDENTIFIER_LEN: usize = 480;
    let sample_prefix = format!("{:08}", 0);
    let sample = ResourceCleanupTarget {
        kind: ResourceCleanupTargetKind::Chunk,
        identifier: format!(
            "{sample_prefix}{}",
            "x".repeat(REGULAR_IDENTIFIER_LEN - sample_prefix.len())
        ),
    };
    let empty_size = canonical_len(&cleanup_plan_template(session, Vec::new()));
    let entry_size = canonical_len(&sample);
    let count = (MAX_RESOURCE_CLEANUP_PLAN_BYTES - empty_size) / (entry_size + 1);
    let mut targets = (0..count)
        .map(|index| {
            let prefix = format!("{index:08}");
            ResourceCleanupTarget {
                kind: ResourceCleanupTargetKind::Chunk,
                identifier: format!(
                    "{prefix}{}",
                    "x".repeat(REGULAR_IDENTIFIER_LEN - prefix.len())
                ),
            }
        })
        .collect::<Vec<_>>();

    loop {
        let current = canonical_len(&cleanup_plan_template(session, targets.clone()));
        let remaining = MAX_RESOURCE_CLEANUP_PLAN_BYTES - current;
        if remaining == 0 {
            return targets;
        }
        let last = targets.last_mut().expect("sized plan has targets");
        if last.identifier.chars().count() + remaining <= 512 {
            last.identifier.push_str(&"x".repeat(remaining));
            return targets;
        }
        let prefix = format!("{:08}", targets.len());
        let candidate = ResourceCleanupTarget {
            kind: ResourceCleanupTargetKind::Chunk,
            identifier: prefix.clone(),
        };
        let minimum_increment = canonical_len(&candidate) + 1;
        if remaining >= minimum_increment && remaining - minimum_increment + prefix.len() <= 512 {
            let mut candidate = candidate;
            candidate
                .identifier
                .push_str(&"x".repeat(remaining - minimum_increment));
            targets.push(candidate);
            return targets;
        }
        targets.pop().expect("one regular target can be removed");
    }
}

#[test]
fn cleanup_plan_accepts_exact_canonical_limit_and_rejects_one_more_byte() {
    let session = ResourceWriteSession {
        write_id: "write:cleanup-size".to_owned(),
        upload_id: "upload:cleanup-size".to_owned(),
        store_binding: "store:cleanup-size/1".to_owned(),
    };
    let targets = exact_cleanup_plan_targets(&session);
    let plan = ResourceCleanupPlan::new(&session, targets.clone())
        .expect("exact-limit cleanup plan seals");
    assert_eq!(canonical_len(&plan), MAX_RESOURCE_CLEANUP_PLAN_BYTES);
    plan.verify().expect("exact-limit cleanup plan verifies");

    let mut oversized_targets = targets;
    oversized_targets[0].identifier.push('x');
    assert!(matches!(
        ResourceCleanupPlan::new(&session, oversized_targets),
        Err(ResourceError::Validation(_))
    ));
    let mut oversized_plan = plan;
    oversized_plan.targets[0].identifier.push('x');
    assert!(matches!(
        oversized_plan.verify(),
        Err(ResourceError::Validation(_))
    ));
}

#[test]
fn resource_rust_and_schema_share_unicode_control_and_safe_integer_boundaries() {
    let schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/resource.schema.json"))
            .expect("Resource schema parses");
    let validator = Validator::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
        .expect("Resource schema compiles");

    let valid = ResourceCandidate {
        resource_version: RESOURCE_VERSION.to_owned(),
        shape: ResourceShape::Object,
        media_type: "application/octet-stream".to_owned(),
        inline: None,
        integrity: ResourceIntegrity::Version {
            authority: "界".repeat(512),
            version: "版本".to_owned(),
        },
        manifest: None,
        annotations: BTreeMap::new(),
    };
    valid.validate().expect("512 Unicode scalars admit in Rust");
    assert!(validator.is_valid(&serde_json::to_value(&valid).expect("candidate serializes")));

    let mut too_long = valid.clone();
    too_long.integrity = ResourceIntegrity::Version {
        authority: "界".repeat(513),
        version: "版本".to_owned(),
    };
    assert!(matches!(
        too_long.validate(),
        Err(ResourceError::Validation(_))
    ));
    assert!(!validator.is_valid(&serde_json::to_value(&too_long).expect("candidate serializes")));

    let mut controlled = valid;
    controlled.integrity = ResourceIntegrity::Version {
        authority: "authority:\u{0085}forged".to_owned(),
        version: "版本".to_owned(),
    };
    assert!(matches!(
        controlled.validate(),
        Err(ResourceError::Validation(_))
    ));
    assert!(!validator.is_valid(&serde_json::to_value(&controlled).expect("candidate serializes")));

    let unsafe_integer = json!({
        "resource_version": RESOURCE_VERSION,
        "shape": "object",
        "media_type": "application/octet-stream",
        "integrity": {
            "kind": "content",
            "digest": format!("sha256:{}", "a".repeat(64)),
            "size": cymule_core::MAX_EXACT_INTEGER + 1
        }
    });
    let unsafe_candidate: ResourceCandidate =
        serde_json::from_value(unsafe_integer.clone()).expect("u64 candidate decodes");
    assert!(matches!(
        unsafe_candidate.validate(),
        Err(ResourceError::Validation(_))
    ));
    assert!(!validator.is_valid(&unsafe_integer));
}
