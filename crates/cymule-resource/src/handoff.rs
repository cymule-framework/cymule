use cymule_durable::{DurableResourceControl, DurableStore};

pub use cymule_profile_protocol::resource::{
    MAX_HANDOFF_INDEX_PAGE, RESOURCE_HANDOFF_ACTIVATION_INDEX_VERSION,
    RESOURCE_HANDOFF_ACTIVATION_VERSION, RESOURCE_HANDOFF_INDEX_VERSION, RESOURCE_HANDOFF_VERSION,
    ResourceHandoff, ResourceHandoffActivation, ResourceHandoffActivationCurrent,
    ResourceHandoffActivationIndexEntry, ResourceHandoffActivationReceipt, ResourceHandoffCurrent,
    ResourceHandoffIndexEntry, ResourceHandoffPage, ResourceHandoffReceipt,
    ResourceProducerProvenance,
};

use crate::{
    ResourceCommand, ResourceCommandOutcome, ResourceError, ResourceOperation, ResourceResult,
    error::durable_resource_error,
};

/// M1-backed Run-to-Run Resource handoff authority.
///
/// Durable stores each transfer and activation under an exact key plus one
/// payload-free, per-target ordered index. No operation enumerates another
/// target or scans an all-domain journal.
pub struct ResourceHandoffController;

impl ResourceHandoffController {
    /// Atomically publish one transfer authority and target index entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the handoff is invalid, conflicts with retained
    /// authority, or Durable cannot verify or commit the exact command.
    pub fn transfer<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        handoff: &ResourceHandoff,
    ) -> ResourceResult<ResourceHandoffReceipt> {
        handoff.verify()?;
        let command = ResourceCommand::new(ResourceOperation::Transfer {
            handoff: handoff.clone(),
        })?;
        match control
            .commit(&command)
            .map_err(durable_resource_error)?
            .outcome
        {
            ResourceCommandOutcome::Transfer { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Atomically activate one exact prior transfer into its target input Wait.
    ///
    /// The same M1 CAS binds the exact source receipt, activation authority,
    /// per-target activation index, Wait result, and resulting Continuation.
    ///
    /// # Errors
    ///
    /// Returns an error when the handoff or Wait is invalid, the source
    /// transfer is absent or different, or Durable cannot commit activation.
    pub fn activate<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        handoff: &ResourceHandoff,
        wait_id: &str,
    ) -> ResourceResult<ResourceHandoffActivationReceipt> {
        handoff.verify()?;
        let source = control
            .handoff_current(&handoff.transfer_id)
            .map_err(durable_resource_error)?
            .ok_or_else(|| {
                ResourceError::NotFound(format!("Resource handoff {}", handoff.transfer_id))
            })?;
        source.verify()?;
        if source.receipt.handoff != *handoff {
            return Err(ResourceError::Conflict {
                code: "resource_handoff_reused".to_owned(),
                message: format!(
                    "Resource transfer {} retained different semantics",
                    handoff.transfer_id
                ),
            });
        }
        let activation = ResourceHandoffActivation::new(handoff, wait_id)?;
        let command = ResourceCommand::new(ResourceOperation::ActivateTransfer {
            activation,
            source_receipt_id: source.receipt.receipt_id,
        })?;
        match control
            .commit(&command)
            .map_err(durable_resource_error)?
            .outcome
        {
            ResourceCommandOutcome::ActivateTransfer { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Read one exact transfer authority by transfer identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is invalid or retained Durable state
    /// cannot be loaded and verified.
    pub fn handoff<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        transfer_id: &str,
    ) -> ResourceResult<Option<ResourceHandoff>> {
        cymule_core::validate_identity("Resource transfer", transfer_id)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        control
            .handoff_current(transfer_id)
            .map_err(durable_resource_error)?
            .map(|current| {
                current.verify()?;
                Ok(current.receipt.handoff)
            })
            .transpose()
    }

    /// Read one exact activation authority by activation identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the identity is invalid or retained Durable state
    /// cannot be loaded and verified.
    pub fn activation<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        activation_id: &str,
    ) -> ResourceResult<Option<ResourceHandoffActivation>> {
        cymule_core::validate_content_id("Resource handoff activation", activation_id)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        control
            .handoff_activation_current(activation_id)
            .map_err(durable_resource_error)?
            .map(|current| {
                current.verify()?;
                Ok(current.receipt.activation)
            })
            .transpose()
    }

    /// Resolve one bounded contiguous page from one target Run's exact index.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds or identity, an unreadable Durable
    /// index, or a page that violates its requested target or range.
    pub fn incoming_page<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        to_run: &str,
        start_index: u64,
        limit: usize,
    ) -> ResourceResult<ResourceHandoffPage> {
        cymule_core::validate_identity("Resource target Run", to_run)
            .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if start_index > cymule_core::MAX_EXACT_INTEGER {
            return Err(ResourceError::Validation(
                "Resource handoff page start exceeds the shared exact-integer range".to_owned(),
            ));
        }
        if !(1..=MAX_HANDOFF_INDEX_PAGE).contains(&limit) {
            return Err(ResourceError::Validation(format!(
                "Resource handoff page limit must be within 1..={MAX_HANDOFF_INDEX_PAGE}"
            )));
        }
        let page = control
            .handoff_page(to_run, start_index, limit)
            .map_err(durable_resource_error)?;
        validate_handoff_page(&page, to_run, start_index, limit)?;
        Ok(page)
    }
}

fn validate_handoff_page(
    page: &ResourceHandoffPage,
    to_run: &str,
    start_index: u64,
    limit: usize,
) -> ResourceResult<()> {
    if page.handoffs.len() > limit {
        return Err(ResourceError::Integrity {
            code: "resource_handoff_page_limit_exceeded".to_owned(),
            message: "Durable returned a Resource handoff page beyond the requested bound"
                .to_owned(),
        });
    }
    let page_count = u64::try_from(page.handoffs.len()).map_err(|_| ResourceError::Integrity {
        code: "resource_handoff_page_range_overflow".to_owned(),
        message: "Resource handoff page length exceeds platform bounds".to_owned(),
    })?;
    let end_index = start_index
        .checked_add(page_count)
        .filter(|end| *end <= cymule_core::MAX_EXACT_INTEGER)
        .ok_or_else(|| ResourceError::Integrity {
            code: "resource_handoff_page_range_overflow".to_owned(),
            message: "Resource handoff page range exceeds the shared exact-integer bound"
                .to_owned(),
        })?;
    for handoff in &page.handoffs {
        handoff.verify()?;
        if handoff.to_run != to_run {
            return Err(ResourceError::Integrity {
                code: "resource_handoff_page_target_mismatch".to_owned(),
                message: "Resource handoff page contains another target Run".to_owned(),
            });
        }
    }
    if let Some(next_index) = page.next_index
        && (next_index != end_index || page.handoffs.len() < limit)
    {
        return Err(ResourceError::Integrity {
            code: "resource_handoff_page_next_index_invalid".to_owned(),
            message: "Resource handoff page successor does not equal its full contiguous range"
                .to_owned(),
        });
    }
    Ok(())
}

fn outcome_mismatch(command_id: &str) -> ResourceError {
    ResourceError::Integrity {
        code: "resource_command_outcome_kind_mismatch".to_owned(),
        message: format!("Resource command {command_id} returned a different outcome kind"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handoff(transfer_id: &str, to_run: &str) -> ResourceHandoff {
        let resource = cymule_core::artifact_ref("test/resource", transfer_id.as_bytes())
            .expect("test Resource reference derives");
        ResourceHandoff {
            handoff_version: RESOURCE_HANDOFF_VERSION.to_owned(),
            transfer_id: transfer_id.to_owned(),
            producer: ResourceProducerProvenance {
                run_id: "run:producer".to_owned(),
                occurrence_id: format!("occurrence:{transfer_id}"),
                result: resource.clone(),
            },
            to_run: to_run.to_owned(),
            slot: format!("slot:{transfer_id}"),
            resource,
        }
    }

    #[test]
    fn handoff_page_requires_an_exact_contiguous_successor() {
        let start_index = 17;
        let exact = ResourceHandoffPage {
            handoffs: vec![
                handoff("transfer:one", "run:consumer"),
                handoff("transfer:two", "run:consumer"),
            ],
            next_index: Some(19),
        };
        validate_handoff_page(&exact, "run:consumer", start_index, 2)
            .expect("full contiguous page verifies");

        for next_index in [17, 18, 20, cymule_core::MAX_EXACT_INTEGER + 1, u64::MAX] {
            let page = ResourceHandoffPage {
                handoffs: exact.handoffs.clone(),
                next_index: Some(next_index),
            };
            assert!(matches!(
                validate_handoff_page(&page, "run:consumer", start_index, 2),
                Err(ResourceError::Integrity { code, .. })
                    if code == "resource_handoff_page_next_index_invalid"
            ));
        }
    }

    #[test]
    fn handoff_page_rejects_successors_on_short_or_empty_pages() {
        let start_index = 17;
        for handoffs in [vec![handoff("transfer:one", "run:consumer")], Vec::new()] {
            let page_count = u64::try_from(handoffs.len()).expect("test page count fits u64");
            let page = ResourceHandoffPage {
                handoffs,
                next_index: Some(start_index + page_count),
            };
            assert!(matches!(
                validate_handoff_page(&page, "run:consumer", start_index, 2),
                Err(ResourceError::Integrity { code, .. })
                    if code == "resource_handoff_page_next_index_invalid"
            ));
        }
    }

    #[test]
    fn handoff_page_accepts_short_and_empty_terminal_pages() {
        for handoffs in [vec![handoff("transfer:one", "run:consumer")], Vec::new()] {
            let page = ResourceHandoffPage {
                handoffs,
                next_index: None,
            };
            validate_handoff_page(&page, "run:consumer", 17, 2)
                .expect("a short or empty terminal page verifies");
        }
        let empty = ResourceHandoffPage {
            handoffs: Vec::new(),
            next_index: None,
        };
        validate_handoff_page(&empty, "run:consumer", cymule_core::MAX_EXACT_INTEGER, 2)
            .expect("empty terminal page may end at the exact-integer limit");
    }

    #[test]
    fn handoff_page_rejects_exact_integer_range_overflow() {
        let page = ResourceHandoffPage {
            handoffs: vec![handoff("transfer:overflow", "run:consumer")],
            next_index: None,
        };
        validate_handoff_page(&page, "run:consumer", cymule_core::MAX_EXACT_INTEGER - 1, 1)
            .expect("nonempty terminal page may end at the exact-integer limit");
        for start_index in [cymule_core::MAX_EXACT_INTEGER, u64::MAX] {
            assert!(matches!(
                validate_handoff_page(&page, "run:consumer", start_index, 1),
                Err(ResourceError::Integrity { code, .. })
                    if code == "resource_handoff_page_range_overflow"
            ));
        }
    }
}
