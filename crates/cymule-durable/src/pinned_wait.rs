//! Lazy authenticated pending-Wait capability over one pinned `StateRoot`.

use std::collections::BTreeSet;

use cymule_authenticated_collections::{
    MAX_PAGE_BYTES, MAX_PAGE_ENTRIES, MapPosition, MapRoot, prove_map_exact, prove_map_range,
    verify_map_exact, verify_map_range,
};

use super::{
    ObjectOverlay, StateRootLeafKind, StateRootManifest, StateRootResolver, StateRootValue,
    map_get, pending_wait_source_key,
};
use crate::{
    DurableError, DurableResult, ParkedWaitView, SignalKeyPage, SignalKeyPageOutcome,
    WaitActivationSource, WaitSelection, WaitSourceCursor, WaitState,
};
use cymule_durable_protocol::MAX_WAIT_DELIVERY_TARGETS;

/// `StateRoot`-backed lazy view. It owns no cache and cannot enumerate a whole
/// domain outside explicit authenticated pages.
pub(crate) struct PinnedParkedWaitView<'a, R: StateRootResolver + ?Sized> {
    manifest: &'a StateRootManifest,
    resolver: &'a mut R,
}

impl<'a, R: StateRootResolver + ?Sized> PinnedParkedWaitView<'a, R> {
    pub(crate) fn open(
        manifest: &'a StateRootManifest,
        resolver: &'a mut R,
    ) -> DurableResult<Self> {
        manifest.verify()?;
        super::ensure_resolver_pinned(manifest, resolver)?;
        Ok(Self { manifest, resolver })
    }

    fn source_root(
        &self,
        source: &WaitActivationSource,
    ) -> &cymule_authenticated_collections::MapRoot {
        match source {
            WaitActivationSource::Signal { .. } => &self.manifest.roots.pending_signal_sources,
            WaitActivationSource::Timer { .. } => &self.manifest.roots.pending_timer_sources,
        }
    }

    /// Authenticate a caller-retained target set against exact global Waits and
    /// their source. Pending targets prove membership in both pending indexes;
    /// terminal nonwinners prove absence from those same indexes. This is an
    /// exact-set admission, not a filter or a source scan.
    pub(crate) fn validate_delivery_targets(
        &mut self,
        source: &WaitActivationSource,
        wait_ids: &BTreeSet<String>,
    ) -> DurableResult<()> {
        source.verify()?;
        if !(1..=MAX_WAIT_DELIVERY_TARGETS).contains(&wait_ids.len()) {
            return Err(DurableError::Validation(format!(
                "wait target set must contain 1..={MAX_WAIT_DELIVERY_TARGETS} identities"
            )));
        }
        let source_key = pending_wait_source_key(source)?;
        let source_root = self.source_root(source).clone();
        let mut overlay = ObjectOverlay::new(self.resolver);
        let source_waits = map_get(&source_root, &source_key, &mut overlay)?
            .map(|descriptor| descriptor.decode_pending_wait_source(source))
            .transpose()?
            .unwrap_or_else(MapRoot::empty);
        let mut consume_once_targets = 0_usize;
        for wait_id in wait_ids {
            cymule_core::validate_content_id("wait delivery target", wait_id)?;
            let value =
                map_get(&self.manifest.roots.waits, wait_id, &mut overlay)?.ok_or_else(|| {
                    DurableError::HistoryConflict {
                        code: "state_root_wait_delivery_target_missing".to_owned(),
                        message: format!("wait delivery target {wait_id} has no exact current"),
                    }
                })?;
            let wait = validate_source_wait_value(source, wait_id, &value)?;
            match wait.state {
                WaitState::Pending => {
                    if map_get(&source_waits, wait_id, &mut overlay)?.as_ref() != Some(&value) {
                        return Err(DurableError::Integrity {
                            code: "state_root_pending_wait_source_current_mismatch".to_owned(),
                            message: format!(
                                "pending Wait {wait_id} differs from its exact source bucket"
                            ),
                        });
                    }
                    validate_pending_wait_value(
                        self.manifest,
                        source,
                        wait_id,
                        &value,
                        &mut overlay,
                    )?;
                }
                WaitState::Completed | WaitState::Cancelled => {
                    validate_terminal_wait_absence(
                        self.manifest,
                        &source_waits,
                        &wait,
                        &mut overlay,
                    )?;
                }
            }
            consume_once_targets += usize::from(wait.consume_once);
        }
        source.validate_target_cardinality(wait_ids.len(), consume_once_targets)?;
        Ok(())
    }

    fn select_source(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        source.verify()?;
        if !(1..=MAX_WAIT_DELIVERY_TARGETS).contains(&max_targets) {
            return Err(DurableError::Validation(format!(
                "wait target bound must be within 1..={MAX_WAIT_DELIVERY_TARGETS}"
            )));
        }
        let source_key = pending_wait_source_key(source)?;
        let source_root = self.source_root(source).clone();
        let mut overlay = ObjectOverlay::new(self.resolver);
        let Some(descriptor) = map_get(&source_root, &source_key, &mut overlay)? else {
            return Ok(WaitSelection {
                wait_ids: BTreeSet::new(),
                remaining: 0,
            });
        };
        let waits = descriptor.decode_pending_wait_source(source)?;
        let limit = match source {
            WaitActivationSource::Signal { .. } => max_targets,
            WaitActivationSource::Timer { .. } => 1,
        };
        let proof = prove_map_range(&waits, None, limit, MAX_PAGE_BYTES, &mut overlay)?;
        let page = verify_map_range(&waits, None, limit, MAX_PAGE_BYTES, &proof)?;
        let mut wait_ids = BTreeSet::new();
        let mut consume_once_selected = false;
        for (position, value_id) in page.entries() {
            let value = overlay.load_value(value_id)?;
            let wait = validate_pending_wait_value(
                self.manifest,
                source,
                position.key(),
                &value,
                &mut overlay,
            )?;
            if wait.consume_once {
                if consume_once_selected {
                    continue;
                }
                consume_once_selected = true;
            }
            wait_ids.insert(wait.wait_id);
        }
        let selected = u64::try_from(wait_ids.len())
            .map_err(|error| DurableError::Validation(error.to_string()))?;
        let remaining =
            waits
                .entries
                .checked_sub(selected)
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_pending_wait_selection_count_mismatch".to_owned(),
                    message: "pending-Wait selection exceeds its authenticated source count"
                        .to_owned(),
                })?;
        Ok(WaitSelection {
            wait_ids,
            remaining: usize::try_from(remaining)
                .map_err(|error| DurableError::Validation(error.to_string()))?,
        })
    }

    fn signal_page(
        &mut self,
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        if !(1..=MAX_PAGE_ENTRIES).contains(&limit) {
            return Err(DurableError::Validation(format!(
                "signal-source page limit must be within 1..={MAX_PAGE_ENTRIES}"
            )));
        }
        let root = self.manifest.roots.pending_signal_sources.clone();
        let current_revision = self.manifest.revision.clone();
        let current_root = cymule_core::canonical_digest(&root)?;
        let after = match cursor {
            Some(cursor) => {
                cursor.verify()?;
                if cursor.source_revision() != current_revision
                    || cursor.source_root() != current_root
                {
                    return Ok(SignalKeyPageOutcome::Stale {
                        current_revision,
                        current_root,
                    });
                }
                let position = MapPosition::for_key(cursor.canonical_key())?;
                if position.key_hash() != cursor.key_hash() {
                    return Err(DurableError::Integrity {
                        code: "wait_source_cursor_hash_mismatch".to_owned(),
                        message: "wait-source cursor key and authenticated hash disagree"
                            .to_owned(),
                    });
                }
                Some(position)
            }
            None => None,
        };
        let mut overlay = ObjectOverlay::new(self.resolver);
        let start = signal_page_start(&root, after.as_ref(), &mut overlay)?;
        let proof = prove_map_range(&root, after.as_ref(), limit, MAX_PAGE_BYTES, &mut overlay)?;
        let page = verify_map_range(&root, after.as_ref(), limit, MAX_PAGE_BYTES, &proof)?;
        let mut keys = Vec::with_capacity(page.entries().len());
        for (position, value_id) in page.entries() {
            let value = overlay.load_value(value_id)?;
            keys.push(signal_source_key(position, value)?);
        }
        let consumed = start
            .checked_add(
                u64::try_from(keys.len())
                    .map_err(|error| DurableError::Validation(error.to_string()))?,
            )
            .ok_or_else(|| DurableError::Validation("signal-source page overflowed".to_owned()))?;
        let remaining =
            root.entries
                .checked_sub(consumed)
                .ok_or_else(|| DurableError::Integrity {
                    code: "state_root_pending_signal_page_count_mismatch".to_owned(),
                    message: "signal-source page exceeds its authenticated root count".to_owned(),
                })?;
        let next_cursor = if remaining == 0 {
            None
        } else {
            page.entries()
                .last()
                .map(|(position, _)| {
                    WaitSourceCursor::new(
                        current_revision.clone(),
                        current_root.clone(),
                        position.key().to_owned(),
                    )
                })
                .transpose()?
        };
        Ok(SignalKeyPageOutcome::Page(SignalKeyPage {
            keys,
            next_cursor,
            remaining: usize::try_from(remaining)
                .map_err(|error| DurableError::Validation(error.to_string()))?,
        }))
    }
}

fn signal_page_start<R: StateRootResolver + ?Sized>(
    root: &MapRoot,
    after: Option<&MapPosition>,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<u64> {
    let Some(position) = after else {
        return Ok(0);
    };
    let exact = prove_map_exact(root, position.key(), overlay)?;
    let verified = verify_map_exact(root, position.key(), &exact)?;
    let rank = verified.rank().ok_or_else(|| DurableError::Integrity {
        code: "wait_source_cursor_member_missing".to_owned(),
        message: "wait-source cursor is absent from its pinned root".to_owned(),
    })?;
    rank.checked_add(1)
        .ok_or_else(|| DurableError::Validation("wait-source cursor rank overflowed".to_owned()))
}

fn signal_source_key(position: &MapPosition, value: StateRootValue) -> DurableResult<String> {
    let StateRootValue::PendingWaitSource { source, .. } = value else {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_source_value_kind_mismatch".to_owned(),
            message: format!(
                "pending signal source {} has the wrong descriptor",
                position.key()
            ),
        });
    };
    let WaitActivationSource::Signal { key } = source else {
        return Err(DurableError::Integrity {
            code: "state_root_pending_signal_source_kind_mismatch".to_owned(),
            message: "pending-signal source page contains a timer".to_owned(),
        });
    };
    if pending_wait_source_key(&WaitActivationSource::Signal { key: key.clone() })?
        != position.key()
    {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_source_key_mismatch".to_owned(),
            message: "pending-signal source changed its exact map key".to_owned(),
        });
    }
    Ok(key)
}

fn validate_pending_wait_value<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    source: &WaitActivationSource,
    wait_id: &str,
    value: &StateRootValue,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<crate::WaitCondition> {
    let wait = validate_source_wait_value(source, wait_id, value)?;
    if wait.state != WaitState::Pending {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_selection_mismatch".to_owned(),
            message: format!("pending Wait {wait_id} changed source, identity, or lifecycle"),
        });
    }
    if map_get(&manifest.roots.waits, wait_id, overlay)?.as_ref() != Some(value) {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_source_current_mismatch".to_owned(),
            message: format!("pending Wait {wait_id} differs from global current"),
        });
    }
    let run_indexes = map_get(&manifest.roots.run_query_indexes, &wait.run_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!("pending Wait {wait_id} has no Run current-membership descriptor"),
        })?
        .decode_run_query_indexes(&wait.run_id)?;
    if map_get(&run_indexes.pending_waits, wait_id, overlay)?.as_ref() != Some(value) {
        return Err(DurableError::Integrity {
            code: "state_root_pending_wait_run_index_mismatch".to_owned(),
            message: format!("pending Wait {wait_id} differs from its per-Run current index"),
        });
    }
    Ok(wait)
}

fn validate_source_wait_value(
    source: &WaitActivationSource,
    wait_id: &str,
    value: &StateRootValue,
) -> DurableResult<crate::WaitCondition> {
    let wait: crate::WaitCondition = value.decode(StateRootLeafKind::Wait)?;
    wait.verify_wire()?;
    if wait.wait_id != wait_id || !wait_matches_source(&wait, source) {
        return Err(DurableError::Integrity {
            code: "state_root_wait_delivery_source_mismatch".to_owned(),
            message: format!("selected Wait {wait_id} changed its identity or source"),
        });
    }
    Ok(wait)
}

fn validate_terminal_wait_absence<R: StateRootResolver + ?Sized>(
    manifest: &StateRootManifest,
    source_waits: &MapRoot,
    wait: &crate::WaitCondition,
    overlay: &mut ObjectOverlay<'_, R>,
) -> DurableResult<()> {
    let run_indexes = map_get(&manifest.roots.run_query_indexes, &wait.run_id, overlay)?
        .ok_or_else(|| DurableError::Integrity {
            code: "run_query_indexes_missing".to_owned(),
            message: format!(
                "terminal Wait {} has no Run current-membership descriptor",
                wait.wait_id
            ),
        })?
        .decode_run_query_indexes(&wait.run_id)?;
    if map_get(source_waits, &wait.wait_id, overlay)?.is_some()
        || map_get(&run_indexes.pending_waits, &wait.wait_id, overlay)?.is_some()
    {
        return Err(DurableError::Integrity {
            code: "state_root_terminal_wait_pending_membership".to_owned(),
            message: format!(
                "terminal Wait {} remains in a pending source or Run index",
                wait.wait_id
            ),
        });
    }
    Ok(())
}

impl<R: StateRootResolver + ?Sized> ParkedWaitView for PinnedParkedWaitView<'_, R> {
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        self.select_source(source, max_targets)
    }

    fn signal_key_page(
        &mut self,
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        self.signal_page(cursor, limit)
    }
}

fn wait_matches_source(wait: &crate::WaitCondition, source: &WaitActivationSource) -> bool {
    matches!(
        (&wait.kind, source),
        (
            crate::WaitKind::Signal { key },
            WaitActivationSource::Signal { key: expected }
        ) if key == expected
    ) || matches!(
        (&wait.kind, source),
        (
            crate::WaitKind::Timer { timer_id },
            WaitActivationSource::Timer { timer_id: expected }
        ) if timer_id == expected
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cymule_authenticated_collections::{MapRoot, build_map};
    use cymule_core::durable_internal::MachineAuthorityFrontier;
    use cymule_durable_protocol::WaitOwner;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct TestResolver {
        pinned: String,
        objects: BTreeMap<String, super::super::StateRootObject>,
        map_node_loads: usize,
        value_loads: usize,
    }

    impl StateRootResolver for TestResolver {
        fn pinned_manifest_id(&self) -> &str {
            &self.pinned
        }

        fn load_state_root_object(
            &mut self,
            object_id: &str,
        ) -> DurableResult<Option<super::super::StateRootObject>> {
            let object = self.objects.get(object_id).cloned();
            match &object {
                Some(super::super::StateRootObject::MapNode(_)) => self.map_node_loads += 1,
                Some(super::super::StateRootObject::Value(_)) => self.value_loads += 1,
                _ => {}
            }
            Ok(object)
        }
    }

    fn build_root<R: StateRootResolver + ?Sized>(
        entries: Vec<(String, String)>,
        overlay: &mut ObjectOverlay<'_, R>,
    ) -> MapRoot {
        let output = build_map(entries).expect("test map builds");
        overlay
            .insert_map_nodes(output.objects())
            .expect("test map nodes stage");
        output.root().clone()
    }

    fn seal_fixture(
        roots: super::super::StateRoots,
        overlay: ObjectOverlay<'_, super::super::EmptyStateRootResolver>,
    ) -> (StateRootManifest, TestResolver) {
        let frontier = MachineAuthorityFrontier::genesis(
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
            MapRoot::empty(),
        )
        .expect("Machine frontier derives");
        let revision = super::super::derive_genesis_revision(super::super::DurableRevisionState {
            durable_version: crate::DURABLE_STATE_VERSION,
            machine_snapshot_version: cymule_core::MachineSnapshot::VERSION,
            machine_frontier: &frontier,
            machine_base_anchor: None,
            roots: &roots,
        })
        .expect("fixture revision derives");
        let manifest = StateRootManifest::new(
            super::super::StateRootManifestMetadata {
                durable_version: crate::DURABLE_STATE_VERSION.to_owned(),
                revision,
                sequence: 0,
                parent_manifest: None,
                parent_revision: None,
                delta_digest: None,
                machine_snapshot_version: cymule_core::MachineSnapshot::VERSION.to_owned(),
            },
            frontier,
            None,
            roots,
        )
        .expect("fixture manifest seals");
        let objects = overlay.finish(&manifest).expect("fixture objects close");
        let resolver = TestResolver {
            pinned: manifest.manifest_id.clone(),
            objects: objects
                .into_iter()
                .map(|object| (object.object_id().to_owned(), object))
                .collect(),
            ..TestResolver::default()
        };
        (manifest, resolver)
    }

    fn source_page_fixture(source_count: usize) -> (StateRootManifest, TestResolver) {
        let mut empty = super::super::EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let dummy_id = overlay
            .insert_value(
                StateRootValue::encode(
                    StateRootLeafKind::MachineFact,
                    &cymule_core::content_id("cymule.test.wait-source-dummy/1", &source_count)
                        .expect("dummy identity derives"),
                )
                .expect("dummy value encodes"),
            )
            .expect("dummy value stages");
        let dummy_root = build_root(vec![("dummy".to_owned(), dummy_id)], &mut overlay);
        let mut sources = Vec::with_capacity(source_count);
        for index in 0..source_count {
            let source = WaitActivationSource::Signal {
                key: format!("signal:{index:05}"),
            };
            let storage_key = pending_wait_source_key(&source).expect("source key derives");
            let value_id = overlay
                .insert_value(
                    StateRootValue::pending_wait_source(source, dummy_root.clone())
                        .expect("source descriptor seals"),
                )
                .expect("source descriptor stages");
            sources.push((storage_key, value_id));
        }
        let mut roots = super::super::StateRoots::empty();
        roots.pending_signal_sources = build_root(sources, &mut overlay);
        seal_fixture(roots, overlay)
    }

    fn signal_wait(
        run_id: &str,
        key: &str,
        ordinal: usize,
        consume_once: bool,
    ) -> crate::WaitCondition {
        let wait_id =
            cymule_core::content_id("cymule.test.pending-wait/1", &(run_id, key, ordinal))
                .expect("Wait identity derives");
        let wait = crate::WaitCondition {
            wait_id,
            run_id: run_id.to_owned(),
            kind: crate::WaitKind::Signal {
                key: key.to_owned(),
            },
            consume_once,
            owner: WaitOwner {
                invocation_id: "invocation:test".to_owned(),
                definition_id: "definition:test".to_owned(),
                site_id: format!("site:{ordinal}"),
                region_path: Vec::new(),
                step_index: ordinal,
                bind: None,
            },
            state: WaitState::Pending,
            result: None,
        };
        wait.verify_wire().expect("Wait wire verifies");
        wait
    }

    fn delivery_fixture(
        consume_once: &[bool],
    ) -> (
        StateRootManifest,
        TestResolver,
        WaitActivationSource,
        BTreeSet<String>,
    ) {
        let run_id = "run:wait-delivery";
        let signal_key = "signal:delivery";
        let source = WaitActivationSource::Signal {
            key: signal_key.to_owned(),
        };
        let waits = consume_once
            .iter()
            .enumerate()
            .map(|(index, consume_once)| signal_wait(run_id, signal_key, index, *consume_once))
            .collect::<Vec<_>>();
        let wait_ids = waits
            .iter()
            .map(|wait| wait.wait_id.clone())
            .collect::<BTreeSet<_>>();
        let mut empty = super::super::EmptyStateRootResolver;
        let mut overlay = ObjectOverlay::new(&mut empty);
        let mut wait_entries = Vec::new();
        for wait in &waits {
            let value_id = overlay
                .insert_value(
                    StateRootValue::encode(StateRootLeafKind::Wait, wait)
                        .expect("Wait value encodes"),
                )
                .expect("Wait value stages");
            wait_entries.push((wait.wait_id.clone(), value_id));
        }
        let waits_root = build_root(wait_entries, &mut overlay);
        let descriptor = StateRootValue::run_query_indexes(
            run_id,
            super::super::RunQueryIndexRoots {
                pending_waits: waits_root.clone(),
                ..super::super::RunQueryIndexRoots::default()
            },
        )
        .expect("Run query descriptor seals");
        let descriptor_id = overlay
            .insert_value(descriptor)
            .expect("Run query descriptor stages");
        let run_queries = build_root(vec![(run_id.to_owned(), descriptor_id)], &mut overlay);
        let source_descriptor =
            StateRootValue::pending_wait_source(source.clone(), waits_root.clone())
                .expect("source descriptor seals");
        let source_id = overlay
            .insert_value(source_descriptor)
            .expect("source descriptor stages");
        let source_root = build_root(
            vec![(
                pending_wait_source_key(&source).expect("source key derives"),
                source_id,
            )],
            &mut overlay,
        );
        let mut roots = super::super::StateRoots::empty();
        roots.waits = waits_root;
        roots.run_query_indexes = run_queries;
        roots.pending_signal_sources = source_root;
        let (manifest, resolver) = seal_fixture(roots, overlay);
        (manifest, resolver, source, wait_ids)
    }

    fn finish_wait_targets(
        manifest: &mut StateRootManifest,
        resolver: &mut TestResolver,
        wait_ids: &BTreeSet<String>,
        state: WaitState,
    ) {
        assert!(matches!(state, WaitState::Completed | WaitState::Cancelled));
        let operations = wait_ids
            .iter()
            .map(|wait_id| {
                let mut wait = super::super::load_wait(manifest, resolver, wait_id)
                    .expect("exact Wait resolves")
                    .expect("exact Wait exists");
                wait.state = state;
                wait.result = (state == WaitState::Completed).then(|| {
                    cymule_core::artifact_ref(crate::WAIT_RESULT_ARTIFACT_KIND, b"null")
                        .expect("Wait result derives")
                });
                crate::DurableOperation::PutWait { value: wait }
            })
            .collect();
        let transition = manifest
            .apply(
                &crate::DurableDelta::new(operations).expect("Wait completion delta seals"),
                resolver,
            )
            .expect("Wait completion updates exact pending indexes");
        for object in transition.objects {
            resolver
                .objects
                .insert(object.object_id().to_owned(), object);
        }
        *manifest = transition.manifest;
        resolver.pinned.clone_from(&manifest.manifest_id);
    }

    #[test]
    fn signal_page_over_65k_sources_is_bounded_and_reopens() {
        let (manifest, mut resolver) = source_page_fixture(65_536);
        let (cursor, first_map_loads, first_value_loads) = {
            let mut view = PinnedParkedWaitView::open(&manifest, &mut resolver)
                .expect("pinned Wait view opens");
            let SignalKeyPageOutcome::Page(page) = view
                .signal_key_page(None, 2)
                .expect("first source page verifies")
            else {
                panic!("fresh page cannot be stale");
            };
            assert_eq!(page.keys.len(), 2);
            assert_eq!(page.remaining, 65_534);
            (
                page.next_cursor.expect("large page has a cursor"),
                resolver.map_node_loads,
                resolver.value_loads,
            )
        };
        assert!(first_map_loads < 256, "source page must not scan 65k keys");
        assert_eq!(first_value_loads, 2);

        let map_before = resolver.map_node_loads;
        let values_before = resolver.value_loads;
        let mut reopened =
            PinnedParkedWaitView::open(&manifest, &mut resolver).expect("view reopens");
        let SignalKeyPageOutcome::Page(page) = reopened
            .signal_key_page(Some(&cursor), 2)
            .expect("cursor page verifies after reopen")
        else {
            panic!("same manifest cursor cannot become stale");
        };
        assert_eq!(page.keys.len(), 2);
        assert!(resolver.map_node_loads - map_before < 256);
        assert_eq!(resolver.value_loads - values_before, 2);
    }

    #[test]
    fn signal_cursor_nonmember_fails_and_removed_root_is_stale() {
        let (manifest, mut resolver) = source_page_fixture(2);
        let current_root = cymule_core::canonical_digest(&manifest.roots.pending_signal_sources)
            .expect("root digest derives");
        let missing_key = pending_wait_source_key(&WaitActivationSource::Signal {
            key: "signal:missing".to_owned(),
        })
        .expect("missing source key derives");
        let forged = WaitSourceCursor::new(manifest.revision.clone(), current_root, missing_key)
            .expect("well-shaped nonmember cursor builds");
        let error = PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("view opens")
            .signal_key_page(Some(&forged), 1)
            .expect_err("nonmember cursor must fail");
        assert!(matches!(
            error,
            DurableError::Integrity { code, .. } if code == "wait_source_cursor_member_missing"
        ));

        let cursor = {
            let mut view =
                PinnedParkedWaitView::open(&manifest, &mut resolver).expect("view reopens");
            let SignalKeyPageOutcome::Page(page) =
                view.signal_key_page(None, 1).expect("first page verifies")
            else {
                panic!("fresh page cannot be stale");
            };
            page.next_cursor.expect("two sources yield a cursor")
        };
        let (empty_manifest, mut empty_resolver) = source_page_fixture(0);
        let SignalKeyPageOutcome::Stale {
            current_revision,
            current_root,
        } = PinnedParkedWaitView::open(&empty_manifest, &mut empty_resolver)
            .expect("empty successor view opens")
            .signal_key_page(Some(&cursor), 1)
            .expect("old cursor closes as stale")
        else {
            panic!("removed source root must stale the cursor");
        };
        assert_eq!(current_revision, empty_manifest.revision);
        assert_eq!(
            current_root,
            cymule_core::canonical_digest(&empty_manifest.roots.pending_signal_sources)
                .expect("empty root digest derives")
        );
    }

    #[test]
    fn delivery_targets_require_exact_membership_and_cardinality() {
        let (manifest, mut resolver, source, wait_ids) = delivery_fixture(&[false, true]);
        PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("view opens")
            .validate_delivery_targets(&source, &wait_ids)
            .expect("one broadcast plus one consume-once target verifies");

        let (manifest, mut resolver, source, wait_ids) = delivery_fixture(&[true, true]);
        let error = PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("view opens")
            .validate_delivery_targets(&source, &wait_ids)
            .expect_err("one signal cannot consume two consume-once waits");
        assert!(matches!(error, DurableError::Validation(_)));
    }

    #[test]
    fn selected_terminal_nonwinners_survive_reopen_and_source_removal() {
        for terminal in [WaitState::Completed, WaitState::Cancelled] {
            let (mut manifest, mut resolver, source, wait_ids) = delivery_fixture(&[false, false]);
            let first = wait_ids.iter().next().expect("two Waits exist").clone();
            finish_wait_targets(
                &mut manifest,
                &mut resolver,
                &BTreeSet::from([first.clone()]),
                terminal,
            );
            PinnedParkedWaitView::open(&manifest, &mut resolver)
                .expect("view reopens after one target finishes")
                .validate_delivery_targets(&source, &wait_ids)
                .expect("pending peer and terminal nonwinner remain one complete selection");

            let pending = wait_ids
                .difference(&BTreeSet::from([first]))
                .cloned()
                .collect();
            finish_wait_targets(&mut manifest, &mut resolver, &pending, terminal);
            assert_eq!(manifest.roots.pending_signal_sources.entries, 0);
            PinnedParkedWaitView::open(&manifest, &mut resolver)
                .expect("view reopens after the last source target finishes")
                .validate_delivery_targets(&source, &wait_ids)
                .expect("removed pending source still admits its exact terminal nonwinners");
        }
    }

    #[test]
    fn delivery_cannot_substitute_source_or_an_unretained_wait() {
        let (manifest, mut resolver, _, wait_ids) = delivery_fixture(&[false]);
        let wrong_source = WaitActivationSource::Signal {
            key: "signal:another-source".to_owned(),
        };
        let error = PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("view opens")
            .validate_delivery_targets(&wrong_source, &wait_ids)
            .expect_err("a global Wait key alone cannot authorize another source");
        assert!(matches!(
            error,
            DurableError::Integrity { code, .. }
                if code == "state_root_wait_delivery_source_mismatch"
        ));
        let missing = cymule_core::content_id("cymule.test.pending-wait/1", &"missing")
            .expect("well-shaped missing Wait identity derives");
        let error = PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("view reopens")
            .validate_delivery_targets(&wrong_source, &BTreeSet::from([missing]))
            .expect_err("a missing Wait cannot become a terminal nonwinner");
        assert!(matches!(
            error,
            DurableError::HistoryConflict { code, .. }
                if code == "state_root_wait_delivery_target_missing"
        ));
    }

    #[test]
    fn selection_inside_one_65k_wait_source_is_lazy_and_bounded() {
        let (manifest, mut resolver, source, _) = delivery_fixture(&vec![false; 65_536]);
        let mut view = PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("large source view opens without loading any Wait");
        let selected = view.select(&source, 1).expect("one exact Wait selects");
        assert_eq!(selected.wait_ids.len(), 1);
        assert_eq!(selected.remaining, 65_535);
        assert!(resolver.map_node_loads < 256);
        assert!(resolver.value_loads < 16);

        let map_before = resolver.map_node_loads;
        let values_before = resolver.value_loads;
        PinnedParkedWaitView::open(&manifest, &mut resolver)
            .expect("large source view reopens")
            .validate_delivery_targets(&source, &selected.wait_ids)
            .expect("selected delivery uses only its exact Wait and owner indexes");
        assert!(resolver.map_node_loads - map_before < 256);
        assert!(resolver.value_loads - values_before < 16);
    }
}
