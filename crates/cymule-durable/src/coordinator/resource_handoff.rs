//! Exact-key Resource transfer and input activation over one pinned `StateRoot`.

use cymule_core::{ArtifactRecord, ArtifactRef, Operation, RunExecutionStatus, canonical_digest};
use cymule_profile_protocol::resource as resource_protocol;

use super::{
    DurableCoordinator, ExecutorRunRead, apply_wait_result, ensure_direct_wait_completion,
    load_executor_run_at_manifest, pinned_durable_run_current, region_at_path,
    verify_resource_handoff_activation_origin, verify_resource_handoff_origin,
};
use crate::model::{derive_wait_id, resource_handoff_input_coupling_id};
use crate::state_root::pinned_machine::PinnedMachineView;
use crate::{
    ComponentOccurrenceState, ComponentOutcome, ContinuationStatus, CoupledCheckpoint,
    CoupledCheckpointReceipt, DurableError, DurableOperation, DurableResult, DurableStore,
    StateRootManifest, StateRootResolver, WaitCondition, WaitKind, WaitState,
};

impl<S: DurableStore> DurableCoordinator<S> {
    pub(super) fn commit_resource_transfer(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        handoff: &resource_protocol::ResourceHandoff,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        handoff.verify()?;
        let target_index = self.read_current_state_root(|manifest, resolver| {
            validate_transfer_admission(manifest, resolver, handoff)?;
            if let Some(current) = crate::state_root::load_resource_handoff_current(
                manifest,
                resolver,
                &handoff.transfer_id,
            )? {
                verify_resource_handoff_origin(manifest, resolver, &current)?;
                return Err(DurableError::Integrity {
                    code: "resource_handoff_receipt_missing".to_owned(),
                    message: format!(
                        "Resource transfer {} exists without its exact command receipt",
                        handoff.transfer_id
                    ),
                });
            }
            if let Some(existing) = crate::state_root::load_resource_handoff_slot(
                manifest,
                resolver,
                &handoff.to_run,
                &handoff.slot,
            )? {
                return Err(DurableError::HistoryConflict {
                    code: "resource_handoff_slot_conflict".to_owned(),
                    message: format!(
                        "Resource target {} slot {} already belongs to transfer {}",
                        handoff.to_run, handoff.slot, existing.transfer_id
                    ),
                });
            }
            crate::state_root::load_resource_handoff_index_len(manifest, resolver, &handoff.to_run)
        })?;
        let handoff_receipt = resource_protocol::ResourceHandoffReceipt::new(
            command.command_id.clone(),
            handoff.clone(),
            target_index,
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::Transfer {
                receipt: handoff_receipt.clone(),
            },
        )?;
        let current = resource_protocol::ResourceHandoffCurrent {
            receipt: handoff_receipt.clone(),
        };
        current.verify()?;
        self.commit_profile_operations(vec![
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceHandoffCurrent { value: current },
            DurableOperation::PutResourceHandoffSlot {
                value: handoff_receipt.index.clone(),
            },
            DurableOperation::AppendResourceHandoffIndex {
                value: handoff_receipt.index,
            },
        ])?;
        Ok(receipt)
    }

    pub(super) fn commit_resource_activation(
        &mut self,
        command: &resource_protocol::ResourceCommand,
        activation: &resource_protocol::ResourceHandoffActivation,
        source_receipt_id: &str,
    ) -> DurableResult<resource_protocol::ResourceCommandReceipt> {
        activation.verify()?;
        let (wait, target, activation_index, machine_authority_root) = self
            .read_current_state_root(|manifest, resolver| {
                let source =
                    load_unconsumed_transfer(manifest, resolver, activation, source_receipt_id)?;
                let wait = crate::state_root::load_wait(manifest, resolver, &activation.wait_id)?
                    .ok_or_else(|| {
                    DurableError::NotFound(format!("wait {} does not exist", activation.wait_id))
                })?;
                let target = load_target_run(manifest, resolver, &activation.to_run)?;
                validate_pending_input_wait(&wait, &target, &source.receipt.handoff.slot)?;
                let artifact = load_resource_artifact(manifest, resolver, &activation.result)?;
                crate::executor::validate_wait_completion(
                    &wait,
                    &cymule_core::decode_json(&artifact.bytes)?,
                )?;
                let activation_index =
                    crate::state_root::load_resource_handoff_activation_index_len(
                        manifest,
                        resolver,
                        &activation.to_run,
                    )?;
                Ok((
                    wait,
                    target,
                    activation_index,
                    manifest.machine_authority_root().to_owned(),
                ))
            })?;
        let mut continuation = target.continuation;
        apply_wait_result(&wait, &activation.result, &mut continuation)?;
        let run_current = pinned_durable_run_current(&target.run, &continuation)?;
        let coupled = CoupledCheckpointReceipt::new(CoupledCheckpoint::ResourceHandoffInput {
            machine_authority_root,
            transfer_id: activation.transfer_id.clone(),
            activation_id: activation.activation_id.clone(),
            resource_command_id: command.command_id.clone(),
            source_receipt_id: source_receipt_id.to_owned(),
            run_id: activation.to_run.clone(),
            owner: wait.owner.clone(),
            wait_id: activation.wait_id.clone(),
            result: activation.result.clone(),
            continuation_digest: canonical_digest(&continuation)?,
        })?;
        let activation_receipt = resource_protocol::ResourceHandoffActivationReceipt::new(
            command.command_id.clone(),
            activation.clone(),
            source_receipt_id.to_owned(),
            activation_index,
            coupled.receipt_id.clone(),
        )?;
        let receipt = resource_protocol::ResourceCommandReceipt::new(
            command.clone(),
            resource_protocol::ResourceCommandOutcome::ActivateTransfer {
                receipt: activation_receipt.clone(),
            },
        )?;
        let current = resource_protocol::ResourceHandoffActivationCurrent {
            receipt: activation_receipt.clone(),
        };
        current.verify()?;
        let mut completed = wait;
        completed.state = WaitState::Completed;
        completed.result = Some(activation.result.clone());
        completed.verify_wire()?;
        self.commit_profile_operations(vec![
            DurableOperation::PutWait { value: completed },
            DurableOperation::PutContinuation {
                value: continuation,
            },
            DurableOperation::PutRunCurrent { value: run_current },
            DurableOperation::PutCoupledCheckpointReceipt { value: coupled },
            DurableOperation::PutResourceCommandReceipt {
                value: receipt.clone(),
            },
            DurableOperation::PutResourceHandoffActivationCurrent { value: current },
            DurableOperation::AppendResourceHandoffActivationIndex {
                value: activation_receipt.index,
            },
        ])?;
        Ok(receipt)
    }

    pub(super) fn verify_resource_activation_replay(
        &mut self,
        command_receipt: &resource_protocol::ResourceCommandReceipt,
        activation_receipt: &resource_protocol::ResourceHandoffActivationReceipt,
    ) -> DurableResult<()> {
        command_receipt.verify()?;
        activation_receipt.verify()?;
        let activation = &activation_receipt.activation;
        self.read_current_state_root(|manifest, resolver| {
            let current = crate::state_root::load_resource_handoff_activation_current(
                manifest,
                resolver,
                &activation.activation_id,
            )?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_activation_replay_current_missing".to_owned(),
                message: format!(
                    "Resource activation receipt {} lost its current authority",
                    activation_receipt.receipt_id
                ),
            })?;
            verify_resource_handoff_activation_origin(manifest, resolver, &current)?;
            if current.receipt != *activation_receipt
                || !matches!(
                    &command_receipt.outcome,
                    resource_protocol::ResourceCommandOutcome::ActivateTransfer { receipt }
                        if receipt == activation_receipt
                )
            {
                return Err(DurableError::Integrity {
                    code: "resource_activation_replay_current_mismatch".to_owned(),
                    message: format!(
                        "Resource activation receipt {} changed its exact command authority",
                        activation_receipt.receipt_id
                    ),
                });
            }
            let source = load_transfer(manifest, resolver, &activation.transfer_id)?;
            validate_activation_source(&source, activation, &activation_receipt.source_receipt_id)?;
            let wait = load_resource_activation_wait(
                manifest,
                resolver,
                command_receipt,
                activation_receipt,
            )?;
            ensure_direct_wait_completion(&wait)?;
            if wait.run_id != activation.to_run
                || wait.state != WaitState::Completed
                || wait.result.as_ref() != Some(&activation.result)
                || !matches!(
                    &wait.kind,
                    WaitKind::Input { correlation, .. }
                        if correlation == &source.receipt.handoff.slot
                )
            {
                return Err(DurableError::Integrity {
                    code: "resource_activation_completed_wait_mismatch".to_owned(),
                    message: format!(
                        "Resource activation {} lost its exact completed input Wait",
                        activation.activation_id
                    ),
                });
            }
            let artifact = load_resource_artifact(manifest, resolver, &activation.result)?;
            crate::executor::validate_wait_completion(
                &wait,
                &cymule_core::decode_json(&artifact.bytes)?,
            )?;
            // The receipt authenticates the original completed checkpoint. A
            // later resume or migration may legitimately change its current
            // frames and digest, but cannot make the consumed Wait pending.
            let material = PinnedMachineView::open(manifest, resolver)?
                .run_execution_material(&activation.to_run)?;
            pinned_durable_run_current(&material.run, &material.continuation)?;
            if material.continuation.wait_set.contains(&activation.wait_id) {
                return Err(DurableError::Integrity {
                    code: "resource_activation_completed_wait_still_pending".to_owned(),
                    message: format!(
                        "Resource activation {} remains in the target Continuation wait set",
                        activation.activation_id
                    ),
                });
            }
            Ok(())
        })
    }
}

fn load_target_run(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    run_id: &str,
) -> DurableResult<ExecutorRunRead> {
    let run = PinnedMachineView::open(manifest, resolver)?
        .run_current(run_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!("Resource target Run {run_id} does not exist"))
        })?;
    if run.execution_status != RunExecutionStatus::Active {
        return Err(DurableError::IllegalTransition(format!(
            "Resource target Run {run_id} is terminal"
        )));
    }
    let target = load_executor_run_at_manifest(manifest, resolver, run_id)?.ok_or_else(|| {
        DurableError::NotFound(format!("Resource target Run {run_id} does not exist"))
    })?;
    if !matches!(
        target.continuation.status,
        ContinuationStatus::Ready | ContinuationStatus::Waiting
    ) {
        return Err(DurableError::IllegalTransition(format!(
            "Resource target Run {run_id} is not ready or waiting"
        )));
    }
    pinned_durable_run_current(&target.run, &target.continuation)?;
    Ok(target)
}

fn validate_transfer_admission(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    handoff: &resource_protocol::ResourceHandoff,
) -> DurableResult<()> {
    let mut view = PinnedMachineView::open(manifest, resolver)?;
    let target = view.run_current(&handoff.to_run)?.ok_or_else(|| {
        DurableError::NotFound(format!(
            "Resource target Run {} does not exist",
            handoff.to_run
        ))
    })?;
    if target.execution_status != RunExecutionStatus::Active {
        return Err(DurableError::IllegalTransition(format!(
            "Resource target Run {} is terminal",
            handoff.to_run
        )));
    }
    view.run_current(&handoff.producer.run_id)?.ok_or_else(|| {
        DurableError::NotFound(format!(
            "Resource producer Run {} does not exist",
            handoff.producer.run_id
        ))
    })?;
    let frontier = view
        .component_attempt_frontier(&handoff.producer.occurrence_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "Resource producer occurrence {} does not exist",
                handoff.producer.occurrence_id
            ))
        })?;
    let occurrence = frontier.occurrence;
    if occurrence.run_id != handoff.producer.run_id
        || occurrence.state != ComponentOccurrenceState::Completed
        || !matches!(
            &occurrence.outcome,
            Some(ComponentOutcome::Succeeded { output })
                if output == &handoff.resource && output == &handoff.producer.result
        )
    {
        return Err(DurableError::Validation(format!(
            "Resource transfer {} does not match a completed successful producer occurrence",
            handoff.transfer_id
        )));
    }
    load_resource_artifact(manifest, resolver, &handoff.resource)?;
    Ok(())
}

fn load_resource_artifact(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    reference: &ArtifactRef,
) -> DurableResult<ArtifactRecord> {
    reference.validate()?;
    let artifact = PinnedMachineView::open(manifest, resolver)?
        .artifact(&reference.artifact_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!(
                "Resource Artifact {} does not exist",
                reference.artifact_id
            ))
        })?;
    if artifact.reference != *reference {
        return Err(DurableError::Integrity {
            code: "resource_handoff_artifact_reference_mismatch".to_owned(),
            message: format!(
                "Resource Artifact {} changed its exact type or identity",
                reference.artifact_id
            ),
        });
    }
    resource_protocol::decode_resource_handle_artifact(&artifact)?;
    Ok(artifact)
}

fn load_transfer(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    transfer_id: &str,
) -> DurableResult<resource_protocol::ResourceHandoffCurrent> {
    let source = crate::state_root::load_resource_handoff_current(manifest, resolver, transfer_id)?
        .ok_or_else(|| {
            DurableError::NotFound(format!("Resource transfer {transfer_id} does not exist"))
        })?;
    verify_resource_handoff_origin(manifest, resolver, &source)?;
    Ok(source)
}

fn validate_activation_source(
    source: &resource_protocol::ResourceHandoffCurrent,
    activation: &resource_protocol::ResourceHandoffActivation,
    source_receipt_id: &str,
) -> DurableResult<()> {
    if source.receipt.receipt_id != source_receipt_id
        || source.receipt.handoff.transfer_id != activation.transfer_id
        || source.receipt.handoff.to_run != activation.to_run
        || source.receipt.handoff.resource != activation.result
    {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_activation_source_mismatch".to_owned(),
            message: format!(
                "Resource activation {} does not consume its exact transfer receipt",
                activation.activation_id
            ),
        });
    }
    Ok(())
}

fn load_unconsumed_transfer(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    activation: &resource_protocol::ResourceHandoffActivation,
    source_receipt_id: &str,
) -> DurableResult<resource_protocol::ResourceHandoffCurrent> {
    let source = load_transfer(manifest, resolver, &activation.transfer_id)?;
    if let Some(current) = crate::state_root::load_resource_handoff_activation_current(
        manifest,
        resolver,
        &activation.activation_id,
    )? {
        verify_resource_handoff_activation_origin(manifest, resolver, &current)?;
        return Err(DurableError::Integrity {
            code: "resource_handoff_activation_receipt_missing".to_owned(),
            message: format!(
                "Resource activation {} exists without its exact command receipt",
                activation.activation_id
            ),
        });
    }
    if let Some(existing) = crate::state_root::load_resource_handoff_activation_by_transfer(
        manifest,
        resolver,
        &activation.transfer_id,
    )? {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_transfer_already_activated".to_owned(),
            message: format!(
                "Resource transfer {} is already owned by activation {} at target index {}",
                activation.transfer_id,
                existing.receipt.activation.activation_id,
                existing.receipt.index.activation_index
            ),
        });
    }
    validate_activation_source(&source, activation, source_receipt_id)?;
    Ok(source)
}

fn validate_pending_input_wait(
    wait: &WaitCondition,
    target: &ExecutorRunRead,
    slot: &str,
) -> DurableResult<()> {
    ensure_direct_wait_completion(wait)?;
    if wait.run_id != target.run.run_id
        || !matches!(&wait.kind, WaitKind::Input { correlation, .. } if correlation == slot)
    {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_activation_wait_mismatch".to_owned(),
            message: format!(
                "Resource input Wait {} does not match its exact target slot",
                wait.wait_id
            ),
        });
    }
    if wait.state != WaitState::Pending || wait.result.is_some() {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_activation_partial_history".to_owned(),
            message: format!(
                "Resource input Wait {} is completed without its typed activation receipt",
                wait.wait_id
            ),
        });
    }
    let continuation = &target.continuation;
    if continuation.status != ContinuationStatus::Waiting
        || !continuation.wait_set.contains(&wait.wait_id)
    {
        return Err(DurableError::HistoryConflict {
            code: "resource_handoff_activation_continuation_mismatch".to_owned(),
            message: format!(
                "Resource input Wait {} is not owned by its Waiting Continuation",
                wait.wait_id
            ),
        });
    }
    verify_input_wait_plan(wait, target)?;
    let owner = &wait.owner;
    let frame = continuation.frames.iter().find(|frame| {
        frame.invocation_id == owner.invocation_id
            && frame.definition_id == owner.definition_id
            && frame.region_path == owner.region_path
    });
    if !frame.is_some_and(|frame| {
        frame.next_step == owner.step_index + 1
            && owner
                .bind
                .as_ref()
                .is_none_or(|bind| !frame.locals.contains_key(bind))
    }) {
        return Err(DurableError::Integrity {
            code: "resource_input_wait_frame_mismatch".to_owned(),
            message: format!(
                "Resource input Wait {} has no matching pending frame",
                wait.wait_id
            ),
        });
    }
    Ok(())
}

fn verify_input_wait_plan(wait: &WaitCondition, target: &ExecutorRunRead) -> DurableResult<()> {
    let continuation = &target.continuation;
    let owner = &wait.owner;
    let definition = target
        .plan
        .candidate
        .definitions
        .iter()
        .find(|definition| definition.id == owner.definition_id)
        .ok_or_else(|| DurableError::Integrity {
            code: "resource_input_wait_definition_missing".to_owned(),
            message: format!(
                "Resource input Wait {} lost its Plan definition",
                wait.wait_id
            ),
        })?;
    let step = region_at_path(&definition.body, &owner.region_path)?
        .steps
        .get(owner.step_index)
        .ok_or_else(|| DurableError::Integrity {
            code: "resource_input_wait_site_missing".to_owned(),
            message: format!(
                "Resource input Wait {} lost its exact Plan site",
                wait.wait_id
            ),
        })?;
    let Operation::Wait {
        wait: cymule_core::WaitSpec::Input {
            correlation,
            schema,
        },
        bind,
    } = &step.operation
    else {
        return Err(DurableError::Integrity {
            code: "resource_input_wait_site_kind_mismatch".to_owned(),
            message: format!(
                "Resource input Wait {} does not name a Plan input site",
                wait.wait_id
            ),
        });
    };
    if step.id != owner.site_id
        || bind != &owner.bind
        || wait.consume_once
        || wait.kind
            != (WaitKind::Input {
                correlation: correlation.clone(),
                schema: schema.clone(),
            })
        || wait.wait_id
            != derive_wait_id(
                &wait.run_id,
                &continuation.plan_id,
                &owner.invocation_id,
                &owner.site_id,
            )?
    {
        return Err(DurableError::Integrity {
            code: "resource_input_wait_plan_mismatch".to_owned(),
            message: format!(
                "Resource input Wait {} changed its sealed Plan semantics",
                wait.wait_id
            ),
        });
    }
    Ok(())
}

fn load_resource_activation_wait(
    manifest: &StateRootManifest,
    resolver: &mut dyn StateRootResolver,
    command_receipt: &resource_protocol::ResourceCommandReceipt,
    activation_receipt: &resource_protocol::ResourceHandoffActivationReceipt,
) -> DurableResult<WaitCondition> {
    let activation = &activation_receipt.activation;
    let coupling_id = resource_handoff_input_coupling_id(&activation.activation_id)?;
    let coupled =
        crate::state_root::load_coupled_checkpoint_receipt(manifest, resolver, &coupling_id)?
            .ok_or_else(|| DurableError::Integrity {
                code: "resource_activation_wait_receipt_missing".to_owned(),
                message: format!(
                    "Resource activation {} lost its coupled Wait receipt",
                    activation.activation_id
                ),
            })?;
    let wait = crate::state_root::load_wait(manifest, resolver, &activation.wait_id)?.ok_or_else(
        || DurableError::Integrity {
            code: "resource_activation_wait_missing".to_owned(),
            message: format!(
                "Resource activation {} lost Wait {}",
                activation.activation_id, activation.wait_id
            ),
        },
    )?;
    match &coupled.checkpoint {
        CoupledCheckpoint::ResourceHandoffInput {
            transfer_id,
            activation_id,
            resource_command_id,
            source_receipt_id,
            run_id,
            owner,
            wait_id,
            result,
            ..
        } if coupled.receipt_id == activation_receipt.coupled_wait_receipt_id
            && transfer_id == &activation.transfer_id
            && activation_id == &activation.activation_id
            && resource_command_id == &command_receipt.command.command_id
            && source_receipt_id == &activation_receipt.source_receipt_id
            && run_id == &activation.to_run
            && owner == &wait.owner
            && wait_id == &activation.wait_id
            && result == &activation.result => {}
        _ => {
            return Err(DurableError::Integrity {
                code: "resource_activation_wait_receipt_mismatch".to_owned(),
                message: format!(
                    "Resource activation {} changed its coupled Wait postcondition",
                    activation.activation_id
                ),
            });
        }
    }
    Ok(wait)
}
