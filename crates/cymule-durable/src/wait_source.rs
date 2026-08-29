use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{DurableError, DurableResult, DurableState, WaitActivationSource, WaitKind, WaitState};
use cymule_durable_protocol::MAX_WAIT_DELIVERY_TARGETS;

const IN_MEMORY_PARKED_WAIT_VIEW_VERSION: &str = "cymule.in-memory-parked-wait-view/1";

/// Rebuildable offline or test view over pending signal and timer waits.
///
/// Runtime source drivers read the pinned store through [`ParkedWaitView`]
/// instead of rebuilding this complete in-memory index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParkedWaitIndex {
    signals: BTreeMap<String, IndexedSignalWaits>,
    timers: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
struct IndexedSignalWaits {
    broadcast: BTreeSet<String>,
    consume_once: BTreeSet<String>,
}

/// One bounded, deterministic candidate page for a source delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSelection {
    /// Exact wait identities selected from the parked index.
    pub wait_ids: BTreeSet<String>,
    /// Number of matching waits not represented by this page.
    pub remaining: usize,
}

/// One bounded page of signal keys currently parked in M1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalKeyPage {
    /// Signal keys after the supplied cursor in authenticated-map order.
    pub keys: Vec<String>,
    /// Cursor to supply on the next page request.
    pub next_cursor: Option<WaitSourceCursor>,
    /// Number of indexed signal keys outside this page.
    pub remaining: usize,
}

/// Closed outcome of one authenticated signal-source page request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignalKeyPageOutcome {
    /// The supplied cursor belongs to the current source root and the page was read.
    Page(SignalKeyPage),
    /// The cursor was valid for an earlier root and must be discarded before
    /// paging the explicitly reported current authority.
    Stale {
        /// Current pinned source revision.
        current_revision: String,
        /// Current pending-signal source root.
        current_root: String,
    },
}

/// Opaque authenticated position for one exact pending-signal source root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSourceCursor {
    source_revision: String,
    source_root: String,
    canonical_key: String,
    key_hash: String,
}

impl WaitSourceCursor {
    pub(crate) fn new(
        source_revision: String,
        source_root: String,
        canonical_key: String,
    ) -> DurableResult<Self> {
        let key_hash = cymule_authenticated_collections::map_key_hash(&canonical_key)?;
        let cursor = Self {
            source_revision,
            source_root,
            canonical_key,
            key_hash,
        };
        cursor.verify()?;
        Ok(cursor)
    }

    /// Exact pinned source revision.
    pub fn source_revision(&self) -> &str {
        &self.source_revision
    }

    /// Exact authenticated pending-source root digest.
    pub fn source_root(&self) -> &str {
        &self.source_root
    }

    /// Exact terminal source key consumed by the next page.
    pub fn canonical_key(&self) -> &str {
        &self.canonical_key
    }

    /// Derived authenticated-map order hash.
    pub fn key_hash(&self) -> &str {
        &self.key_hash
    }

    /// Verify the complete source/root/key/hash authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the revision, root, or key hash is invalid, or the
    /// canonical key cannot be hashed.
    pub fn verify(&self) -> DurableResult<()> {
        cymule_core::validate_content_id("wait-source revision", &self.source_revision)?;
        if self.source_root.len() != 64
            || !self
                .source_root
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || self.key_hash != cymule_authenticated_collections::map_key_hash(&self.canonical_key)?
        {
            return Err(DurableError::Validation(
                "wait-source cursor is not an exact revision/root/key position".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Provider-neutral delivery returned by a signal or timer plugin.
#[derive(Debug, Clone, PartialEq)]
pub struct WaitDelivery {
    /// Stable substrate delivery identity used for redelivery deduplication.
    pub activation_id: String,
    /// Signal or timer identity declared by the Plan.
    pub source: WaitActivationSource,
    /// Exact targets chosen from the current parked index.
    pub wait_ids: BTreeSet<String>,
    /// Typed delivery value sealed as an Artifact during admission.
    pub value: Value,
}

/// Replaceable source plugin for durable signal or timer deliveries.
///
/// The framework supplies a bounded wait view and a hard target bound. The
/// plugin owns transport polling and acknowledgement, but cannot admit state
/// directly. If acknowledgement is lost after admission, it must redeliver the
/// same `activation_id`, source, targets, and value.
pub trait WaitSourceDriver {
    /// Return one delivery or `None` when no work is currently available.
    ///
    /// # Errors
    ///
    /// Returns an error when source polling fails or the wait view rejects a
    /// selection or page request.
    fn receive(
        &mut self,
        view: &mut dyn ParkedWaitView,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>>;

    /// Acknowledge a delivery after its M1 CAS commit.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot acknowledge the delivery.
    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()>;
}

/// Bounded read capability over pending wait authority.
///
/// Implementations may resolve only the selected source bucket or one source
/// page. A driver never receives a complete domain index or raw `StateRoot`.
pub trait ParkedWaitView {
    /// Select one exact bounded target set for a typed source.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid target limit or unreadable or
    /// inconsistent source authority.
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection>;

    /// Read one authenticated page of pending signal source keys.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit, malformed cursor, or unreadable
    /// or inconsistent source authority. A valid cursor from an earlier root
    /// returns [`SignalKeyPageOutcome::Stale`] instead.
    fn signal_key_page(
        &mut self,
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome>;
}

impl ParkedWaitIndex {
    /// Rebuild the derived index from authoritative durable waits.
    ///
    /// # Errors
    ///
    /// Returns an error when a pending wait has no owning Continuation or is
    /// absent from that Continuation's wait set.
    pub fn rebuild(state: &DurableState) -> DurableResult<Self> {
        let mut index = Self::default();
        for wait in state.waits.values() {
            index.insert(state, wait)?;
        }
        Ok(index)
    }

    fn insert(&mut self, state: &DurableState, wait: &crate::WaitCondition) -> DurableResult<()> {
        if wait.state != WaitState::Pending {
            return Ok(());
        }
        let continuation = state.continuations.get(&wait.run_id).ok_or_else(|| {
            DurableError::Validation(format!(
                "pending wait {} has no continuation {}",
                wait.wait_id, wait.run_id
            ))
        })?;
        if !continuation.wait_set.contains(&wait.wait_id) {
            return Err(DurableError::Validation(format!(
                "pending wait {} is absent from its continuation",
                wait.wait_id
            )));
        }
        match &wait.kind {
            WaitKind::Signal { key } => {
                let entry = self.signals.entry(key.clone()).or_default();
                if wait.consume_once {
                    entry.consume_once.insert(wait.wait_id.clone());
                } else {
                    entry.broadcast.insert(wait.wait_id.clone());
                }
            }
            WaitKind::Timer { timer_id } => {
                self.timers
                    .entry(timer_id.clone())
                    .or_default()
                    .insert(wait.wait_id.clone());
            }
            WaitKind::Input { .. } => {}
        }
        Ok(())
    }

    /// Select a deterministic bounded page for one source identity.
    ///
    /// A signal page contains at most one consume-once wait, followed by
    /// broadcast waits in stable identity order. A timer page contains one
    /// exact occurrence because one timer activation may target only one wait.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid target limit or an inconsistent index
    /// bucket or count.
    pub fn select(
        &self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        validate_limit(max_targets)?;
        match source {
            WaitActivationSource::Signal { key } => {
                let Some(indexed) = self.signals.get(key) else {
                    return Ok(WaitSelection {
                        wait_ids: BTreeSet::new(),
                        remaining: 0,
                    });
                };
                let total = checked_index_total(
                    indexed.broadcast.len(),
                    indexed.consume_once.len(),
                    "signal wait",
                )?;
                let mut wait_ids = BTreeSet::new();
                if let Some(wait_id) = indexed.consume_once.first() {
                    wait_ids.insert(wait_id.clone());
                }
                for wait_id in &indexed.broadcast {
                    if wait_ids.len() == max_targets {
                        break;
                    }
                    wait_ids.insert(wait_id.clone());
                }
                let remaining =
                    checked_index_remaining(total, wait_ids.len(), "signal wait selection")?;
                Ok(WaitSelection {
                    wait_ids,
                    remaining,
                })
            }
            WaitActivationSource::Timer { timer_id } => {
                let (wait_ids, remaining) =
                    match self.timers.get(timer_id) {
                        None => (BTreeSet::new(), 0),
                        Some(waits) => {
                            let wait_id = waits.first().cloned().ok_or_else(|| {
                                DurableError::Integrity {
                                    code: "wait_index_empty_timer_bucket".to_owned(),
                                    message: format!(
                                        "timer wait index retained an empty bucket for {timer_id}"
                                    ),
                                }
                            })?;
                            (
                                BTreeSet::from([wait_id]),
                                checked_index_remaining(waits.len(), 1, "timer wait selection")?,
                            )
                        }
                    };
                Ok(WaitSelection {
                    wait_ids,
                    remaining,
                })
            }
        }
    }

    /// Return a bounded provider-neutral page of active signal keys.
    ///
    /// Source plugins use this index page instead of scanning an arbitrary
    /// transport prefix. Following returned cursors visits every key at the
    /// same source root even when transport records have no parked match.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid limit, malformed cursor, absent cursor
    /// member at the same root, inconsistent count, or unencodable index.
    pub fn signal_key_page(
        &self,
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        validate_limit(limit)?;
        let (source_revision, source_root) = self.source_authority()?;
        if self.signals.is_empty() {
            if let Some(cursor) = cursor {
                cursor.verify()?;
                return Ok(SignalKeyPageOutcome::Stale {
                    current_revision: source_revision,
                    current_root: source_root,
                });
            }
            return Ok(SignalKeyPageOutcome::Page(SignalKeyPage {
                keys: Vec::new(),
                next_cursor: None,
                remaining: 0,
            }));
        }
        let mut ordered = self
            .signals
            .keys()
            .map(|key| {
                cymule_authenticated_collections::map_key_hash(key)
                    .map(|hash| (hash, key.clone()))
                    .map_err(DurableError::from)
            })
            .collect::<DurableResult<Vec<_>>>()?;
        ordered.sort();
        let start = match cursor {
            Some(cursor) => {
                cursor.verify()?;
                if cursor.source_revision != source_revision || cursor.source_root != source_root {
                    return Ok(SignalKeyPageOutcome::Stale {
                        current_revision: source_revision,
                        current_root: source_root,
                    });
                }
                ordered
                    .iter()
                    .position(|(hash, key)| {
                        hash == &cursor.key_hash && key == &cursor.canonical_key
                    })
                    .ok_or_else(|| DurableError::Integrity {
                        code: "wait_source_cursor_member_missing".to_owned(),
                        message: "wait-source cursor key is absent from its pinned root".to_owned(),
                    })?
                    .checked_add(1)
                    .ok_or_else(|| {
                        DurableError::Validation("wait-source cursor overflowed".to_owned())
                    })?
            }
            None => 0,
        };
        let keys = ordered
            .iter()
            .skip(start)
            .take(limit)
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let consumed = start
            .checked_add(keys.len())
            .ok_or_else(|| DurableError::Validation("wait-source page overflowed".to_owned()))?;
        let remaining = checked_index_remaining(ordered.len(), consumed, "signal-key page")?;
        let next_cursor = if remaining == 0 {
            None
        } else {
            keys.last()
                .map(|key| {
                    WaitSourceCursor::new(source_revision.clone(), source_root.clone(), key.clone())
                })
                .transpose()?
        };
        Ok(SignalKeyPageOutcome::Page(SignalKeyPage {
            keys,
            next_cursor,
            remaining,
        }))
    }

    fn source_authority(&self) -> DurableResult<(String, String)> {
        let revision = cymule_core::content_id(
            IN_MEMORY_PARKED_WAIT_VIEW_VERSION,
            &(&self.signals, &self.timers),
        )?;
        let root = cymule_core::canonical_digest(&self.signals)?;
        Ok((revision, root))
    }

    /// Verify that every delivery target is currently parked under its source.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid activation identity, empty or oversized
    /// target set, target parked under another source, or invalid signal/timer
    /// target cardinality.
    pub fn validate_delivery(&self, delivery: &WaitDelivery) -> DurableResult<()> {
        self.validate_targets(
            &delivery.activation_id,
            &delivery.source,
            &delivery.wait_ids,
        )
    }

    pub(crate) fn validate_targets(
        &self,
        activation_id: &str,
        source: &WaitActivationSource,
        wait_ids: &BTreeSet<String>,
    ) -> DurableResult<()> {
        cymule_core::validate_identity("wait delivery", activation_id)?;
        if wait_ids.is_empty() {
            return Err(DurableError::Validation(
                "wait delivery requires at least one target".to_owned(),
            ));
        }
        validate_limit(wait_ids.len())?;
        let mut consume_once = 0usize;
        for wait_id in wait_ids {
            let present = match source {
                WaitActivationSource::Signal { key } => {
                    let Some(indexed) = self.signals.get(key) else {
                        return Err(DurableError::Validation(format!(
                            "delivery target {wait_id} is not parked under its signal"
                        )));
                    };
                    if indexed.consume_once.contains(wait_id) {
                        consume_once += 1;
                        true
                    } else {
                        indexed.broadcast.contains(wait_id)
                    }
                }
                WaitActivationSource::Timer { timer_id } => self
                    .timers
                    .get(timer_id)
                    .is_some_and(|waits| waits.contains(wait_id)),
            };
            if !present {
                return Err(DurableError::Validation(format!(
                    "delivery target {wait_id} is not parked under its source"
                )));
            }
        }
        Ok(source.validate_target_cardinality(wait_ids.len(), consume_once)?)
    }
}

impl ParkedWaitView for ParkedWaitIndex {
    fn select(
        &mut self,
        source: &WaitActivationSource,
        max_targets: usize,
    ) -> DurableResult<WaitSelection> {
        ParkedWaitIndex::select(self, source, max_targets)
    }

    fn signal_key_page(
        &mut self,
        cursor: Option<&WaitSourceCursor>,
        limit: usize,
    ) -> DurableResult<SignalKeyPageOutcome> {
        ParkedWaitIndex::signal_key_page(self, cursor, limit)
    }
}

fn validate_limit(max_targets: usize) -> DurableResult<()> {
    if max_targets == 0 || max_targets > MAX_WAIT_DELIVERY_TARGETS {
        return Err(DurableError::Validation(format!(
            "wait delivery target limit must be between 1 and {MAX_WAIT_DELIVERY_TARGETS}"
        )));
    }
    Ok(())
}

fn checked_index_total(left: usize, right: usize, context: &str) -> DurableResult<usize> {
    left.checked_add(right)
        .ok_or_else(|| DurableError::Integrity {
            code: "wait_index_count_overflow".to_owned(),
            message: format!("{context} count exceeds the platform index range"),
        })
}

fn checked_index_remaining(total: usize, selected: usize, context: &str) -> DurableResult<usize> {
    total
        .checked_sub(selected)
        .ok_or_else(|| DurableError::Integrity {
            code: "wait_index_selection_count_mismatch".to_owned(),
            message: format!("{context} selected {selected} entries from total {total}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_index_count_arithmetic_rejects_overflow_and_reversed_selection() {
        assert!(matches!(
            checked_index_total(usize::MAX, 1, "test"),
            Err(DurableError::Integrity { code, .. }) if code == "wait_index_count_overflow"
        ));
        assert_eq!(
            checked_index_remaining(usize::MAX, 1, "test")
                .expect("maximum total minus one is exact"),
            usize::MAX - 1
        );
        assert!(matches!(
            checked_index_remaining(0, 1, "test"),
            Err(DurableError::Integrity { code, .. })
                if code == "wait_index_selection_count_mismatch"
        ));
    }
}
