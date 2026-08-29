use serde::{Deserialize, Serialize};

use cymule_durable::{DurableResourceControl, DurableStore};
pub use cymule_profile_protocol::resource::{
    RESOURCE_AGENT_STREAM_PIN_VERSION, RESOURCE_ARCHIVE_PIN_VERSION,
    RESOURCE_ARCHIVE_RELEASE_VERSION, RESOURCE_COMMAND_RECEIPT_VERSION, RESOURCE_COMMAND_VERSION,
    RESOURCE_DELETE_CURRENT_VERSION, RESOURCE_DELETE_INTENT_VERSION,
    RESOURCE_DELETE_RECEIPT_VERSION, RESOURCE_DELETION_TARGET_VERSION, RESOURCE_GC_RECEIPT_VERSION,
    RESOURCE_LIFECYCLE_RECEIPT_REF_VERSION, RESOURCE_PIN_CURRENT_VERSION,
    RESOURCE_PIN_RECEIPT_VERSION, RESOURCE_PROFILE_PIN_VERSION, RESOURCE_RELEASE_RECEIPT_VERSION,
    RESOURCE_RETENTION_CURRENT_VERSION, RESOURCE_RETENTION_FAMILY_VERSION,
    RESOURCE_RETENTION_KEY_VERSION, RESOURCE_RETENTION_SUBJECT_VERSION, ResourceArchiveRelease,
    ResourceCommand, ResourceCommandOutcome, ResourceCommandReceipt, ResourceDeleteCurrent,
    ResourceDeleteIntent, ResourceDeletePostcondition, ResourceDeleteReceipt, ResourceDeleteStatus,
    ResourceDeleter, ResourceDeletionTarget, ResourceGcDisposition, ResourceGcReceipt,
    ResourceLifecycleProfile, ResourceLifecycleReceiptLocator, ResourceLifecycleReceiptRef,
    ResourceOperation, ResourcePin, ResourcePinCurrent, ResourcePinKind, ResourcePinPostcondition,
    ResourcePinReceipt, ResourcePinStatus, ResourceProfilePin, ResourceReleaseReceipt,
    ResourceRetentionCurrent, ResourceRetentionDisposition, ResourceRetentionFamily,
    ResourceRetentionSubject, project_resource_begin_delete_intent, project_resource_pin_receipt,
    project_resource_reconcile_delete_receipt, project_resource_release_receipt,
    reduce_resource_begin_delete_intent, reduce_resource_gc_receipt, reduce_resource_pin_receipt,
    reduce_resource_reconcile_delete_receipt, reduce_resource_release_receipt,
    resource_agent_stream_pin_owner, resource_archive_pin_id, resource_archive_pin_owner,
};

use crate::{
    ResourceError, ResourcePublication, ResourceResult, ResourceWriteSession,
    error::durable_resource_error,
};

/// Frozen immutable upload-cleanup plan version.
pub const RESOURCE_CLEANUP_PLAN_VERSION: &str = "cymule.resource-cleanup-plan/1";
/// Frozen terminal upload-cleanup receipt version.
pub const RESOURCE_CLEANUP_RECEIPT_VERSION: &str = "cymule.resource-cleanup-receipt/2";
/// Maximum canonical JSON bytes admitted for one public cleanup plan.
pub const MAX_RESOURCE_CLEANUP_PLAN_BYTES: usize = 16 * 1024 * 1024;

/// Closed class of one exact provider-owned cleanup target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceCleanupTargetKind {
    /// One staging object or staging metadata tree.
    StagingObject,
    /// One retained upload chunk.
    Chunk,
}

/// One exact provider-owned object in an immutable cleanup plan.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCleanupTarget {
    /// Closed physical target class.
    pub kind: ResourceCleanupTargetKind,
    /// Stable provider-local identity without credentials.
    pub identifier: String,
}

/// Immutable cleanup authority persisted before any physical deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCleanupPlan {
    /// Plan wire version.
    pub plan_version: String,
    /// Content identity of the exact session and ordered target set.
    pub plan_id: String,
    /// Exact caller write identity.
    pub write_id: String,
    /// Exact adapter upload identity.
    pub upload_id: String,
    /// Immutable store binding that owns every cleanup target.
    pub store_binding: String,
    /// Strictly ordered unique set of exact owned targets.
    pub targets: Vec<ResourceCleanupTarget>,
}

/// Verified terminal cleanup receipt for one staged chunked write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCleanupReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Immutable plan persisted before physical cleanup began.
    pub plan: ResourceCleanupPlan,
    /// Number of exact staging targets proved absent.
    pub removed_staging_objects: u64,
    /// Number of exact chunk targets proved absent.
    pub removed_chunks: u64,
    /// Store readback proved all owned staging and chunk objects absent.
    pub verified_absent: bool,
}

/// M1-backed Resource lifecycle authority.
///
/// Every operation touches only exact keyed Resource profile state in Durable.
/// There is no replayable all-domain lifecycle ledger and no public in-memory
/// mutation authority.
pub struct ResourceLifecycleController;

impl ResourceLifecycleController {
    /// Durably retain one caller-identified explicit pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication or pin is invalid, conflicts with
    /// keyed lifecycle state, or Durable cannot commit the command.
    pub fn pin<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        pin_id: &str,
        publication: &ResourcePublication,
        owner: &str,
    ) -> ResourceResult<ResourcePinReceipt> {
        let subject = ResourceRetentionSubject::from_publication(publication)?;
        let pin = ResourcePin::explicit(pin_id, subject, owner)?;
        let command = ResourceCommand::new(ResourceOperation::Pin { pin })?;
        match commit_resource(control, &command)?.outcome {
            ResourceCommandOutcome::Pin { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Durably release one exact explicit pin owned by the expected authority.
    ///
    /// Virtual archive and Agent stream pins are rejected by the protocol and
    /// can be released only by their owning profile's typed transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the release is invalid, does not own the exact
    /// active pin, or Durable cannot commit the command.
    pub fn release<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        release_id: &str,
        pin_id: &str,
        owner: &str,
    ) -> ResourceResult<ResourceReleaseReceipt> {
        let command = ResourceCommand::new(ResourceOperation::Release {
            release_id: release_id.to_owned(),
            pin_id: pin_id.to_owned(),
            owner: owner.to_owned(),
        })?;
        match commit_resource(control, &command)?.outcome {
            ResourceCommandOutcome::Release { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Durably evaluate collection eligibility for one exact physical family.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication or request is invalid, the family
    /// is terminal, or Durable cannot commit the command.
    pub fn garbage_collect<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        gc_id: &str,
        publication: &ResourcePublication,
    ) -> ResourceResult<ResourceGcReceipt> {
        let family = ResourceRetentionFamily::from_publication(publication)?;
        let command = ResourceCommand::new(ResourceOperation::GarbageCollect {
            gc_id: gc_id.to_owned(),
            family,
        })?;
        match commit_resource(control, &command)?.outcome {
            ResourceCommandOutcome::GarbageCollect { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Commit the exact durable deletion fence before provider I/O begins.
    ///
    /// # Errors
    ///
    /// Returns an error when the GC receipt or publication is invalid or does
    /// not describe one eligible family, or Durable cannot commit the fence.
    pub fn begin_delete<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        delete_id: &str,
        gc: &ResourceGcReceipt,
        publication: &ResourcePublication,
    ) -> ResourceResult<ResourceDeleteIntent> {
        gc.verify()?;
        if gc.disposition != ResourceGcDisposition::Eligible || gc.active_pin_count != 0 {
            return Err(ResourceError::Conflict {
                code: "resource_delete_gc_ineligible".to_owned(),
                message: "Resource deletion requires an exact eligible zero-pin GC receipt"
                    .to_owned(),
            });
        }
        let target = ResourceDeletionTarget::from_publication(publication)?;
        if target.subject.family != gc.family {
            return Err(ResourceError::Conflict {
                code: "resource_delete_gc_target_mismatch".to_owned(),
                message: "Resource deletion target does not match its GC receipt".to_owned(),
            });
        }
        let command = ResourceCommand::new(ResourceOperation::BeginDelete {
            delete_id: delete_id.to_owned(),
            gc_command_id: gc.command_id.clone(),
            gc_receipt_id: gc.receipt_id.clone(),
            target,
        })?;
        match commit_resource(control, &command)?.outcome {
            ResourceCommandOutcome::BeginDelete { intent } => Ok(intent),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Reconcile one retained deletion fence through its exact provider.
    ///
    /// Durable invokes the selected provider and derives the terminal receipt
    /// only after `delete_and_verify_absent` succeeds; callers cannot submit an
    /// absence boolean or forge a completion command.
    ///
    /// # Errors
    ///
    /// Returns an error when the deletion is absent or inconsistent, provider
    /// deletion/absence verification fails, or Durable cannot close the fence.
    pub fn reconcile_delete<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        delete_id: &str,
        deleter: &mut impl ResourceDeleter,
    ) -> ResourceResult<ResourceDeleteReceipt> {
        let current = control
            .delete_current(delete_id)
            .map_err(durable_resource_error)?
            .ok_or_else(|| ResourceError::NotFound(format!("Resource delete {delete_id}")))?;
        current.verify()?;
        if current.status == ResourceDeleteStatus::Completed {
            let command_receipt = control
                .command_receipt(current.last_receipt.command_id())
                .map_err(durable_resource_error)?
                .ok_or_else(|| ResourceError::Integrity {
                    code: "resource_delete_terminal_receipt_missing".to_owned(),
                    message: format!(
                        "Resource delete {delete_id} lost its terminal command receipt"
                    ),
                })?;
            command_receipt.verify()?;
            if command_receipt.receipt_id != current.last_receipt.receipt_id() {
                return Err(ResourceError::Integrity {
                    code: "resource_delete_terminal_receipt_mismatch".to_owned(),
                    message: format!(
                        "Resource delete {delete_id} current references another terminal receipt"
                    ),
                });
            }
            return match command_receipt.outcome {
                ResourceCommandOutcome::ReconcileDelete { receipt }
                    if receipt.intent == current.intent =>
                {
                    Ok(receipt)
                }
                _ => Err(ResourceError::Integrity {
                    code: "resource_delete_terminal_intent_mismatch".to_owned(),
                    message: format!(
                        "Resource delete {delete_id} terminal receipt changed its exact intent"
                    ),
                }),
            };
        }
        let command = ResourceCommand::new(ResourceOperation::ReconcileDelete {
            delete_id: delete_id.to_owned(),
            intent_id: current.intent.intent_id.clone(),
        })?;
        let receipt = control
            .reconcile_delete(&command, deleter)
            .map_err(durable_resource_error)?;
        match receipt.outcome {
            ResourceCommandOutcome::ReconcileDelete { receipt } => Ok(receipt),
            _ => Err(outcome_mismatch(&command.command_id)),
        }
    }

    /// Read one exact current pin projection.
    ///
    /// # Errors
    ///
    /// Returns an error when Durable cannot load or verify the exact keyed
    /// projection.
    pub fn pin_current<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        pin_id: &str,
    ) -> ResourceResult<Option<ResourcePinCurrent>> {
        control.pin_current(pin_id).map_err(durable_resource_error)
    }

    /// Read one exact current physical retention projection.
    ///
    /// # Errors
    ///
    /// Returns an error when Durable cannot load or verify the exact keyed
    /// projection.
    pub fn retention_current<S: DurableStore>(
        control: &mut DurableResourceControl<'_, S>,
        retention_key: &str,
    ) -> ResourceResult<Option<ResourceRetentionCurrent>> {
        control
            .retention_current(retention_key)
            .map_err(durable_resource_error)
    }
}

fn commit_resource<S: DurableStore>(
    control: &mut DurableResourceControl<'_, S>,
    command: &ResourceCommand,
) -> ResourceResult<ResourceCommandReceipt> {
    control.commit(command).map_err(durable_resource_error)
}

fn outcome_mismatch(command_id: &str) -> ResourceError {
    ResourceError::Integrity {
        code: "resource_command_outcome_kind_mismatch".to_owned(),
        message: format!("Resource command {command_id} returned a different outcome kind"),
    }
}

/// Derive the exact physical retention key for one verified publication.
///
/// # Errors
///
/// Returns an error when the publication or its physical retention family is
/// invalid.
pub fn resource_retention_key(publication: &ResourcePublication) -> ResourceResult<String> {
    Ok(ResourceRetentionFamily::from_publication(publication)?.retention_key)
}

#[derive(Serialize)]
struct ResourceCleanupPlanIdentity<'a> {
    write_id: &'a str,
    upload_id: &'a str,
    store_binding: &'a str,
    targets: &'a [ResourceCleanupTarget],
}

impl ResourceCleanupPlan {
    /// Seal one immutable cleanup plan before any target is removed.
    ///
    /// # Errors
    ///
    /// Returns an error when the session or ordered target set is invalid or
    /// its canonical identity cannot be derived.
    pub fn new(
        session: &ResourceWriteSession,
        targets: Vec<ResourceCleanupTarget>,
    ) -> ResourceResult<Self> {
        validate_identity("write", &session.write_id)?;
        validate_identity("upload", &session.upload_id)?;
        validate_identity("store binding", &session.store_binding)?;
        validate_cleanup_targets(&targets)?;
        let plan_id = cleanup_plan_id(
            &session.write_id,
            &session.upload_id,
            &session.store_binding,
            &targets,
        )?;
        let plan = Self {
            plan_version: RESOURCE_CLEANUP_PLAN_VERSION.to_owned(),
            plan_id,
            write_id: session.write_id.clone(),
            upload_id: session.upload_id.clone(),
            store_binding: session.store_binding.clone(),
            targets,
        };
        validate_cleanup_plan_size(&plan)?;
        Ok(plan)
    }

    /// Verify the immutable session, exact target set, and content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any field is invalid or the retained plan ID does
    /// not match the exact session and target set.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.plan_version, RESOURCE_CLEANUP_PLAN_VERSION)?;
        validate_identity("write", &self.write_id)?;
        validate_identity("upload", &self.upload_id)?;
        validate_identity("store binding", &self.store_binding)?;
        validate_cleanup_targets(&self.targets)?;
        validate_cleanup_plan_size(self)?;
        let expected = cleanup_plan_id(
            &self.write_id,
            &self.upload_id,
            &self.store_binding,
            &self.targets,
        )?;
        if self.plan_id != expected {
            return Err(ResourceError::Integrity {
                code: "resource_cleanup_plan_identity_mismatch".to_owned(),
                message: format!(
                    "Resource cleanup plan {} does not match {expected}",
                    self.plan_id
                ),
            });
        }
        Ok(())
    }

    /// Derive the sole terminal receipt after every exact target is absent.
    ///
    /// # Errors
    ///
    /// Returns an error when the plan is invalid or its exact target counts
    /// cannot be represented by the shared safe-integer contract.
    pub fn receipt(&self) -> ResourceResult<ResourceCleanupReceipt> {
        self.verify()?;
        Ok(ResourceCleanupReceipt {
            receipt_version: RESOURCE_CLEANUP_RECEIPT_VERSION.to_owned(),
            plan: self.clone(),
            removed_staging_objects: exact_target_count(
                &self.targets,
                ResourceCleanupTargetKind::StagingObject,
            )?,
            removed_chunks: exact_target_count(&self.targets, ResourceCleanupTargetKind::Chunk)?,
            verified_absent: true,
        })
    }
}

impl ResourceCleanupReceipt {
    /// Verify the closed plan-derived upload cleanup receipt.
    ///
    /// # Errors
    ///
    /// Returns an error when the plan is invalid or the terminal counts and
    /// absence assertion do not exactly match that plan.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_CLEANUP_RECEIPT_VERSION)?;
        self.plan.verify()?;
        if !self.verified_absent || self.plan.receipt()? != *self {
            return Err(ResourceError::Integrity {
                code: "resource_cleanup_receipt_mismatch".to_owned(),
                message:
                    "Resource cleanup receipt does not match its immutable plan and absence proof"
                        .to_owned(),
            });
        }
        Ok(())
    }
}

fn cleanup_plan_id(
    write_id: &str,
    upload_id: &str,
    store_binding: &str,
    targets: &[ResourceCleanupTarget],
) -> ResourceResult<String> {
    cymule_core::content_id(
        RESOURCE_CLEANUP_PLAN_VERSION,
        &ResourceCleanupPlanIdentity {
            write_id,
            upload_id,
            store_binding,
            targets,
        },
    )
    .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_cleanup_plan_size(plan: &ResourceCleanupPlan) -> ResourceResult<()> {
    let bytes = cymule_core::canonical_bytes(plan)
        .map_err(|error| ResourceError::Validation(error.to_string()))?
        .len();
    if bytes > MAX_RESOURCE_CLEANUP_PLAN_BYTES {
        return Err(ResourceError::Validation(format!(
            "Resource cleanup plan occupies {bytes} canonical bytes; maximum is {MAX_RESOURCE_CLEANUP_PLAN_BYTES}"
        )));
    }
    Ok(())
}

fn validate_cleanup_targets(targets: &[ResourceCleanupTarget]) -> ResourceResult<()> {
    let mut previous: Option<&ResourceCleanupTarget> = None;
    for target in targets {
        validate_identity("cleanup target", &target.identifier)?;
        if previous.is_some_and(|value| value >= target) {
            return Err(ResourceError::Validation(
                "Resource cleanup targets must be strictly ordered and unique".to_owned(),
            ));
        }
        previous = Some(target);
    }
    validate_safe_integer(
        "cleanup target count",
        u64::try_from(targets.len()).map_err(|_| {
            ResourceError::Validation(
                "Resource cleanup target count exceeds platform bounds".to_owned(),
            )
        })?,
    )
}

fn exact_target_count(
    targets: &[ResourceCleanupTarget],
    kind: ResourceCleanupTargetKind,
) -> ResourceResult<u64> {
    let count = u64::try_from(targets.iter().filter(|target| target.kind == kind).count())
        .map_err(|_| {
            ResourceError::Validation(
                "Resource cleanup target count exceeds platform bounds".to_owned(),
            )
        })?;
    validate_safe_integer("cleanup target count", count)?;
    Ok(count)
}

fn require_version(actual: &str, expected: &str) -> ResourceResult<()> {
    if actual != expected {
        return Err(ResourceError::Validation(format!(
            "unsupported Resource version {actual:?}; expected {expected}"
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> ResourceResult<()> {
    cymule_core::validate_identity(kind, value)
        .map_err(|error| ResourceError::Validation(error.to_string()))
}

fn validate_safe_integer(kind: &str, value: u64) -> ResourceResult<()> {
    if value > cymule_core::MAX_EXACT_INTEGER {
        return Err(ResourceError::Validation(format!(
            "Resource {kind} exceeds the shared exact-integer range"
        )));
    }
    Ok(())
}
