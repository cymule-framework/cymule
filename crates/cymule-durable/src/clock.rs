use cymule_durable_protocol::{ClockObservation, ClockObservationRef};

use crate::DurableResult;

/// Persistence-backed authority selected by runtime composition. Implementors
/// return only exact receipts they previously issued and retained.
pub trait ClockObservationAuthority {
    /// Resolve one exact retained receipt. Unknown or mismatched references fail.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or unknown reference, a mismatched
    /// retained receipt, or failure to read the selected Clock authority.
    fn resolve(&mut self, reference: &ClockObservationRef) -> DurableResult<ClockObservation>;
}

/// Freshness authority for execution-claim acquisition and takeover.
///
/// Historical retry and replay intentionally use
/// [`ClockObservationAuthority::resolve`]. A new execution claim additionally
/// requires that the retained receipt remain the selected source generation's
/// current head for the exact scope while the Store CAS executes.
pub trait ExecutionClockAuthority: ClockObservationAuthority {
    /// Run one Store mutation while `reference` is the exact current head.
    ///
    /// # Errors
    ///
    /// Returns an error when the reference is invalid, stale, or cannot be
    /// held as current, or when the Store mutation returns an error.
    fn with_current_head(
        &mut self,
        reference: &ClockObservationRef,
        commit: &mut dyn FnMut(&ClockObservation) -> DurableResult<()>,
    ) -> DurableResult<()>;
}
