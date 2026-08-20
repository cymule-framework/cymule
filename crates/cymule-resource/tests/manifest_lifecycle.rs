//! Property and fault tests for manifest proofs and Resource lifecycle receipts.

use std::collections::BTreeSet;

use cymule_resource::{
    ResourceCandidate, ResourceError, ResourceGcDisposition, ResourceLifecycle,
    ResourceLifecycleLedger, ResourceManifestEntry, SealedResourceManifest,
};
use proptest::prelude::*;

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn every_bounded_page_proves_exact_manifest_membership(
        values in prop::collection::vec(any::<u16>(), 0..96),
        requested_start in any::<usize>(),
        requested_count in 0usize..24,
    ) {
        let entries = entries(&values);
        let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
        let start = if entries.is_empty() { 0 } else { requested_start % (entries.len() + 1) };
        let count = requested_count.min(entries.len() - start);
        let proof = sealed.proof(start as u64, count).expect("proof builds");
        proof
            .verify_page(&sealed.descriptor, &entries[start..start + count])
            .expect("exact page verifies");

        if count > 0 {
            let mut changed = entries[start..start + count].to_vec();
            changed[0].name.push_str("-changed");
            prop_assert!(matches!(
                proof.verify_page(&sealed.descriptor, &changed),
                Err(ResourceError::Integrity(_))
            ));
        }
    }
}

#[test]
fn manifest_proof_rejects_wrong_root_index_and_descriptor() {
    let entries = entries(&[1, 2, 3, 4, 5]);
    let sealed = SealedResourceManifest::seal(entries.clone()).expect("manifest seals");
    let proof = sealed.proof(1, 3).expect("proof builds");

    let mut wrong_root = sealed.descriptor.clone();
    wrong_root.root_digest = format!("sha256:{}", "0".repeat(64));
    assert!(matches!(
        proof.verify_page(&wrong_root, &entries[1..4]),
        Err(ResourceError::Integrity(_))
    ));

    let mut wrong_index = proof.clone();
    wrong_index.inclusions[1].index += 1;
    assert!(matches!(
        wrong_index.verify_page(&sealed.descriptor, &entries[1..4]),
        Err(ResourceError::Integrity(_))
    ));

    let mut wrong_manifest = proof;
    wrong_manifest.manifest_digest = format!("sha256:{}", "f".repeat(64));
    assert!(matches!(
        wrong_manifest.verify_page(&sealed.descriptor, &entries[1..4]),
        Err(ResourceError::Integrity(_))
    ));
}

#[test]
fn pin_release_gc_delete_receipts_are_exact_and_replayable() {
    let resource_id = ResourceCandidate::text("retained")
        .seal()
        .expect("Resource seals")
        .resource_id;
    let mut ledger = ResourceLifecycleLedger::new();
    let pin = ledger
        .pin("pin:run-output", &resource_id, "run:consumer")
        .expect("pin commits");
    assert_eq!(
        ledger
            .pin("pin:run-output", &resource_id, "run:consumer")
            .expect("pin replays"),
        pin
    );
    let retained = ledger
        .garbage_collect("gc:while-pinned", &resource_id)
        .expect("GC decision commits");
    assert_eq!(retained.disposition, ResourceGcDisposition::Retained);
    assert!(matches!(
        ledger.record_delete("delete:early", &retained, "store:test/1", 8, true),
        Err(ResourceError::Conflict(_))
    ));

    let release = ledger
        .release("release:run-output", &pin.pin_id)
        .expect("release commits");
    assert_eq!(
        ledger
            .release("release:run-output", &pin.pin_id)
            .expect("release replays"),
        release
    );
    let eligible = ledger
        .garbage_collect("gc:after-release", &resource_id)
        .expect("eligible GC commits");
    assert_eq!(eligible.disposition, ResourceGcDisposition::Eligible);
    assert!(matches!(
        ledger.record_delete("delete:no-readback", &eligible, "store:test/1", 8, false),
        Err(ResourceError::Integrity(_))
    ));
    let deleted = ledger
        .record_delete("delete:verified", &eligible, "store:test/1", 8, true)
        .expect("verified delete records");
    assert!(deleted.verified_absent);
    assert_eq!(
        ledger
            .record_delete("delete:verified", &eligible, "store:test/1", 8, true)
            .expect("delete replays"),
        deleted
    );
    ledger.verify().expect("lifecycle ledger verifies");
    assert!(matches!(
        ledger.pin("pin:late", &resource_id, "run:late"),
        Err(ResourceError::Conflict(_))
    ));
}
