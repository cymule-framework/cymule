//! Bounded suite validation and deterministic M3 region materialization.

use std::collections::BTreeSet;

use cymule_core::{ArtifactRef, content_id};
use cymule_virtual::{
    MaterializedPage, RegionSource, VirtualCursor, VirtualError, VirtualRegion, VirtualResult,
    WorkItem,
};

use crate::model::{CASE_ARTIFACT_KIND, EvaluationCase, MAX_CASES};

pub const CURSOR_VERSION: &str = "example.evaluation-suite-cursor/1";

/// Parse a bounded JSON Lines suite and reject ambiguous identities or fields.
pub fn parse_suite(bytes: &[u8]) -> Result<Vec<EvaluationCase>, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
    let mut cases = Vec::new();
    let mut ids = BTreeSet::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            return Err(format!("suite line {} is empty", index + 1));
        }
        if line.len() > 64 * 1024 {
            return Err(format!("suite line {} exceeds 64 KiB", index + 1));
        }
        let case: EvaluationCase = serde_json::from_str(line)
            .map_err(|error| format!("suite line {}: {error}", index + 1))?;
        case.validate()?;
        if !ids.insert(case.id.clone()) {
            return Err(format!("suite repeats case ID {:?}", case.id));
        }
        cases.push(case);
        if cases.len() > MAX_CASES {
            return Err(format!("suite exceeds {MAX_CASES} cases"));
        }
    }
    if cases.is_empty() {
        return Err("suite must contain at least one case".to_owned());
    }
    Ok(cases)
}

pub struct CaseSource<'a> {
    cases: &'a [EvaluationCase],
}

impl<'a> CaseSource<'a> {
    pub const fn new(cases: &'a [EvaluationCase]) -> Self {
        Self { cases }
    }
}

pub fn case_reference(case: &EvaluationCase) -> VirtualResult<ArtifactRef> {
    Ok(ArtifactRef {
        artifact_id: content_id(CASE_ARTIFACT_KIND, case)
            .map_err(|error| VirtualError::Source(error.to_string()))?,
        kind: CASE_ARTIFACT_KIND.to_owned(),
    })
}

impl RegionSource for CaseSource<'_> {
    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> VirtualResult<MaterializedPage> {
        if region.cursor.version != CURSOR_VERSION {
            return Err(VirtualError::Source(
                "evaluation suite cursor version changed".to_owned(),
            ));
        }
        let start: usize = region
            .cursor
            .position
            .parse()
            .map_err(|_| VirtualError::Source("suite cursor is not numeric".to_owned()))?;
        if start > self.cases.len() {
            return Err(VirtualError::Source(
                "suite cursor exceeds retained case count".to_owned(),
            ));
        }
        let end = start.saturating_add(limit).min(self.cases.len());
        let mut items = Vec::with_capacity(end.saturating_sub(start));
        for case in &self.cases[start..end] {
            items.push(WorkItem {
                work_id: format!("case:{}", case.id),
                region_id: region.region_id.clone(),
                run_id: region.run_id.clone(),
                payload: case_reference(case)?,
                capability: Some("evaluation".to_owned()),
                priority: 0,
                cost: 1,
            });
        }
        Ok(MaterializedPage {
            items,
            next_cursor: VirtualCursor {
                version: CURSOR_VERSION.to_owned(),
                position: end.to_string(),
                exhausted: end == self.cases.len(),
            },
        })
    }
}
