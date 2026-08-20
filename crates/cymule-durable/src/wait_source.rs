use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included, Unbounded};

use serde_json::Value;

use crate::{DurableError, DurableResult, DurableState, WaitActivationSource, WaitKind, WaitState};

/// Hard safety bound for one source delivery.
pub const MAX_WAIT_DELIVERY_TARGETS: usize = 4_096;

/// Rebuildable index over pending signal and timer waits.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParkedWaitIndex {
    signals: BTreeMap<String, IndexedSignalWaits>,
    timers: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

/// One bounded round-robin page of signal keys currently parked in M1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalKeyPage {
    /// Signal keys after the supplied cursor, wrapping in stable key order.
    pub keys: Vec<String>,
    /// Cursor to supply on the next page request.
    pub next_cursor: Option<String>,
    /// Number of indexed signal keys outside this page.
    pub remaining: usize,
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
/// The framework supplies a rebuildable index and a hard target bound. The
/// plugin owns transport polling and acknowledgement, but cannot admit state
/// directly. If acknowledgement is lost after admission, it must redeliver the
/// same `activation_id`, source, targets, and value.
pub trait WaitSourceDriver {
    /// Return one delivery or `None` when no work is currently available.
    fn receive(
        &mut self,
        index: &ParkedWaitIndex,
        max_targets: usize,
    ) -> DurableResult<Option<WaitDelivery>>;

    /// Acknowledge a delivery after its M1 CAS commit.
    fn acknowledge(&mut self, activation_id: &str) -> DurableResult<()>;
}

impl ParkedWaitIndex {
    /// Rebuild the derived index from authoritative durable waits.
    pub fn rebuild(state: &DurableState) -> DurableResult<Self> {
        let mut index = Self::default();
        for wait in state.waits.values() {
            if wait.state != WaitState::Pending {
                continue;
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
                    let entry = index.signals.entry(key.clone()).or_default();
                    if wait.consume_once {
                        entry.consume_once.insert(wait.wait_id.clone());
                    } else {
                        entry.broadcast.insert(wait.wait_id.clone());
                    }
                }
                WaitKind::Timer { timer_id } => {
                    index
                        .timers
                        .entry(timer_id.clone())
                        .or_default()
                        .insert(wait.wait_id.clone());
                }
                WaitKind::Input { .. } => {}
            }
        }
        Ok(index)
    }

    /// Select a deterministic bounded page for one source identity.
    ///
    /// A signal page contains at most one consume-once wait, followed by
    /// broadcast waits in stable identity order. A timer page contains one
    /// exact occurrence because one timer activation may target only one wait.
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
                let total = indexed.broadcast.len() + indexed.consume_once.len();
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
                Ok(WaitSelection {
                    remaining: total.saturating_sub(wait_ids.len()),
                    wait_ids,
                })
            }
            WaitActivationSource::Timer { timer_id } => {
                let wait_ids = self
                    .timers
                    .get(timer_id)
                    .and_then(BTreeSet::first)
                    .cloned()
                    .into_iter()
                    .collect();
                let remaining = self
                    .timers
                    .get(timer_id)
                    .map_or(0, |waits| waits.len().saturating_sub(1));
                Ok(WaitSelection {
                    wait_ids,
                    remaining,
                })
            }
        }
    }

    /// Return a bounded provider-neutral round-robin page of active signal keys.
    ///
    /// Source plugins use this index page instead of scanning an arbitrary
    /// transport prefix. Reusing the returned cursor eventually visits every
    /// key even when earlier transport records have no parked match.
    pub fn signal_key_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> DurableResult<SignalKeyPage> {
        validate_limit(limit)?;
        if self.signals.is_empty() {
            return Ok(SignalKeyPage {
                keys: Vec::new(),
                next_cursor: None,
                remaining: 0,
            });
        }
        let keys: Vec<String> = match after {
            Some(cursor) => self
                .signals
                .range((Excluded(cursor.to_owned()), Unbounded))
                .chain(self.signals.range((Unbounded, Included(cursor.to_owned()))))
                .map(|(key, _)| key.clone())
                .take(limit)
                .collect(),
            None => self.signals.keys().take(limit).cloned().collect(),
        };
        Ok(SignalKeyPage {
            next_cursor: keys.last().cloned(),
            remaining: self.signals.len().saturating_sub(keys.len()),
            keys,
        })
    }

    /// Verify that every delivery target is currently parked under its source.
    pub fn validate_delivery(&self, delivery: &WaitDelivery) -> DurableResult<()> {
        if delivery.activation_id.is_empty() || delivery.wait_ids.is_empty() {
            return Err(DurableError::Validation(
                "wait delivery requires identity and targets".to_owned(),
            ));
        }
        validate_limit(delivery.wait_ids.len())?;
        let mut consume_once = 0usize;
        for wait_id in &delivery.wait_ids {
            let present = match &delivery.source {
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
        delivery
            .source
            .validate_target_cardinality(delivery.wait_ids.len(), consume_once)
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
