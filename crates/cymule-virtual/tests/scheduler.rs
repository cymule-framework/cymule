//! Bounded-cardinality, fairness, parking, and restart tests.

use std::collections::BTreeSet;

use cymule_core::ArtifactRef;
use cymule_virtual::{
    FrontierLimits, MaterializedPage, ParkReason, RegionSource, VirtualCursor, VirtualRegion,
    VirtualResult, VirtualScheduler, WorkItem,
};

struct MillionItemSource;

impl RegionSource for MillionItemSource {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        let start: u64 = region.cursor.position.parse().expect("numeric cursor");
        let end = (start + limit as u64).min(1_000_000);
        let items = (start..end)
            .map(|index| WorkItem {
                work_id: format!("{}:{index}", region.region_id),
                region_id: region.region_id.clone(),
                run_id: region.run_id.clone(),
                payload: ArtifactRef {
                    artifact_id: format!("artifact:{index}"),
                    kind: "example/work".to_owned(),
                },
                capability: Some("cpu".to_owned()),
                priority: 0,
                cost: 1,
            })
            .collect();
        Ok(MaterializedPage {
            items,
            next_cursor: VirtualCursor {
                version: "million/1".to_owned(),
                position: end.to_string(),
                exhausted: end == 1_000_000,
            },
        })
    }
}

fn region(id: &str, run_id: &str) -> VirtualRegion {
    VirtualRegion {
        region_id: id.to_owned(),
        run_id: run_id.to_owned(),
        source: "example.million".to_owned(),
        cursor: VirtualCursor {
            version: "million/1".to_owned(),
            position: "0".to_owned(),
            exhausted: false,
        },
        estimated_total: Some(1_000_000),
    }
}

fn limits() -> FrontierLimits {
    FrontierLimits {
        max_materialized: 8,
        max_active: 4,
        max_active_per_run: 2,
        materialize_batch: 4,
    }
}

#[test]
fn million_item_regions_keep_a_bounded_fair_frontier_across_restore() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler
        .register(region("region:b", "run:b"))
        .expect("region registers");
    assert_eq!(
        scheduler
            .fill(&mut MillionItemSource)
            .expect("frontier fills"),
        8
    );
    assert_eq!(scheduler.materialized_count(), 8);

    let capabilities = BTreeSet::from(["cpu".to_owned()]);
    let first = scheduler
        .claim("worker:1", &capabilities)
        .expect("claim")
        .expect("work");
    let second = scheduler
        .claim("worker:2", &capabilities)
        .expect("claim")
        .expect("work");
    assert_ne!(first.item.run_id, second.item.run_id);

    let snapshot = scheduler.snapshot();
    let mut restored = VirtualScheduler::restore(limits(), snapshot).expect("snapshot restores");
    assert_eq!(restored.materialized_count(), 8);
    assert_eq!(
        restored
            .fill(&mut MillionItemSource)
            .expect("backpressure applies"),
        0
    );

    restored
        .complete(&first.item.work_id, "worker:1", first.epoch)
        .expect("current owner completes");
    assert!(
        restored
            .complete(&second.item.work_id, "worker:wrong", second.epoch)
            .is_err()
    );
}

#[test]
fn parked_work_wakes_by_exact_indexed_reason() {
    let mut scheduler = VirtualScheduler::new(limits()).expect("scheduler creates");
    scheduler
        .register(region("region:a", "run:a"))
        .expect("region registers");
    scheduler.fill(&mut MillionItemSource).expect("fills");
    let claim = scheduler
        .claim("worker:1", &BTreeSet::from(["cpu".to_owned()]))
        .expect("claim")
        .expect("work");
    let reason = ParkReason::Wait {
        key: "approval".to_owned(),
    };
    scheduler
        .park(&claim.item.work_id, "worker:1", claim.epoch, reason.clone())
        .expect("work parks");
    assert_eq!(scheduler.wake(&reason), 1);
    assert_eq!(scheduler.wake(&reason), 0);
}
