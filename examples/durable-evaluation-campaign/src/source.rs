//! Bounded suite validation and deterministic M3 region materialization.

use std::collections::BTreeSet;

use cymule_core::{ArtifactRecord, ArtifactRef, artifact_ref, canonical_bytes, decode_json};
use cymule_virtual::{
    MaterializedPage, ProtocolError, ProtocolResult, RegionSourceBinding, VirtualCursor,
    VirtualRegion, VirtualRegionSourceProvider, WorkItem,
};

use crate::model::{CASE_ARTIFACT_KIND, EvaluationCase, MAX_CASES};

pub const CURSOR_VERSION: &str = "example.evaluation-suite-cursor/1";
pub const SOURCE_OPERATION: &str = "example.evaluation-suite.materialize";
pub const SOURCE_IMPLEMENTATION_REVISION: &str = "example.evaluation-suite-source/2";

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
        let case: EvaluationCase = decode_json(line.as_bytes())
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
    source: RegionSourceBinding,
}

impl<'a> CaseSource<'a> {
    pub fn new(cases: &'a [EvaluationCase], source: RegionSourceBinding) -> Self {
        Self { cases, source }
    }
}

pub fn case_reference(case: &EvaluationCase) -> ProtocolResult<ArtifactRef> {
    let bytes = canonical_bytes(case).map_err(ProtocolError::from)?;
    artifact_ref(CASE_ARTIFACT_KIND, &bytes).map_err(ProtocolError::from)
}

fn case_record(case: &EvaluationCase) -> ProtocolResult<ArtifactRecord> {
    let bytes = canonical_bytes(case).map_err(ProtocolError::from)?;
    Ok(ArtifactRecord {
        reference: artifact_ref(CASE_ARTIFACT_KIND, &bytes).map_err(ProtocolError::from)?,
        bytes,
    })
}

impl VirtualRegionSourceProvider for CaseSource<'_> {
    fn source_binding(&self) -> RegionSourceBinding {
        self.source.clone()
    }

    fn materialize(
        &mut self,
        region: &VirtualRegion,
        limit: usize,
    ) -> ProtocolResult<MaterializedPage> {
        if region.cursor.version != CURSOR_VERSION {
            return Err(ProtocolError::Validation(
                "evaluation suite cursor version changed".to_owned(),
            ));
        }
        let start: usize = region
            .cursor
            .position
            .parse()
            .map_err(|_| ProtocolError::Validation("suite cursor is not numeric".to_owned()))?;
        let (end, page_len) = materialized_page_bounds(start, limit, self.cases.len())?;
        let mut items = Vec::with_capacity(page_len);
        let mut artifacts = Vec::with_capacity(page_len);
        for case in &self.cases[start..end] {
            let record = case_record(case)?;
            items.push(WorkItem {
                work_id: format!("case:{}", case.id),
                region_id: region.region_id.clone(),
                run_id: region.run_id.clone(),
                payload: record.reference.clone(),
                capability: Some("evaluation".to_owned()),
                priority: 0,
                cost: 1,
            });
            artifacts.push(record);
        }
        Ok(MaterializedPage {
            items,
            artifacts,
            next_cursor: VirtualCursor {
                version: CURSOR_VERSION.to_owned(),
                position: end.to_string(),
                exhausted: end == self.cases.len(),
            },
        })
    }
}

fn materialized_page_bounds(
    start: usize,
    limit: usize,
    case_count: usize,
) -> ProtocolResult<(usize, usize)> {
    if start > case_count {
        return Err(ProtocolError::Validation(
            "suite cursor exceeds retained case count".to_owned(),
        ));
    }
    let requested_end = start
        .checked_add(limit)
        .ok_or_else(|| ProtocolError::Validation("suite page end overflows usize".to_owned()))?;
    let end = requested_end.min(case_count);
    let page_len = end.checked_sub(start).ok_or_else(|| {
        ProtocolError::Validation("suite page bounds are internally inconsistent".to_owned())
    })?;
    Ok((end, page_len))
}

#[cfg(test)]
mod tests {
    use super::materialized_page_bounds;

    #[test]
    fn page_bounds_reject_overflow_instead_of_clamping_it() {
        assert!(materialized_page_bounds(1, usize::MAX, 1).is_err());
        assert_eq!(
            materialized_page_bounds(0, usize::MAX, 3).unwrap(),
            (3, 3),
            "a representable requested end may clamp only to the exact retained count"
        );
        assert!(materialized_page_bounds(4, 0, 3).is_err());
    }
}
