use std::collections::BTreeSet;

use cymule_core::content_id;
use serde::{Deserialize, Serialize};

use crate::{DurableCoordinator, DurableError, DurableResult, DurableStore, JournalRecord};

/// Frozen provider-neutral retry policy version.
pub const RETRY_POLICY_VERSION: &str = "cymule.retry-policy/1";
/// Durable retry decision record version.
pub const RETRY_DECISION_VERSION: &str = "cymule.retry-decision/1";

/// Closed failure classes used by durable retry admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// A declared domain failure.
    Expected,
    /// A substrate condition that may succeed later.
    Transient,
    /// An undeclared implementation failure.
    Defect,
    /// Explicit cancellation.
    Cancelled,
    /// The admitted logical deadline was reached.
    TimedOut,
    /// A fenced lease no longer authorizes this attempt.
    LeaseLost,
    /// A dispatched effect has an ambiguous external outcome.
    UnknownWorld,
}

/// Kind of work whose failure is being classified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FailureOperation {
    /// Pure or component computation.
    Computation,
    /// An effect declared safe to observe again.
    ObservationalEffect {
        /// Stable semantic Effect intent identity.
        intent_id: String,
    },
    /// An effect that may have changed the external world.
    MutatingEffect {
        /// Stable semantic Effect intent identity.
        intent_id: String,
    },
}

impl FailureOperation {
    fn verify(&self) -> DurableResult<()> {
        match self {
            Self::Computation => Ok(()),
            Self::ObservationalEffect { intent_id } | Self::MutatingEffect { intent_id }
                if !intent_id.is_empty() =>
            {
                Ok(())
            }
            Self::ObservationalEffect { .. } | Self::MutatingEffect { .. } => Err(
                DurableError::Validation("effect intent identity must not be empty".to_owned()),
            ),
        }
    }
}

/// One classified failed attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryFailure {
    /// Stable identity of the observed failure.
    pub failure_id: String,
    /// Closed semantic failure class.
    pub class: FailureClass,
    /// Operation whose attempt failed.
    pub operation: FailureOperation,
}

impl RetryFailure {
    fn verify(&self) -> DurableResult<()> {
        if self.failure_id.is_empty() {
            return Err(DurableError::Validation(
                "retry failure identity must not be empty".to_owned(),
            ));
        }
        self.operation.verify()?;
        if self.class == FailureClass::UnknownWorld
            && matches!(self.operation, FailureOperation::Computation)
        {
            return Err(DurableError::Validation(
                "unknown_world is valid only for an external effect".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Deterministic base delay between failed attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetryDelay {
    /// Retry at the same logical instant.
    Immediate,
    /// Add one fixed logical duration.
    Fixed {
        /// Logical duration added after every retryable failure.
        delay: u64,
    },
    /// Multiply an initial delay by an integer for each later failed attempt.
    Exponential {
        /// Delay after the first failed attempt.
        initial_delay: u64,
        /// Integer growth factor.
        multiplier: u32,
        /// Maximum base delay before recorded jitter is added.
        max_delay: u64,
    },
}

impl RetryDelay {
    fn verify(&self) -> DurableResult<()> {
        match self {
            Self::Immediate => Ok(()),
            Self::Fixed { delay } if *delay > 0 => Ok(()),
            Self::Fixed { .. } => Err(DurableError::Validation(
                "fixed retry delay must be positive".to_owned(),
            )),
            Self::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } if *initial_delay > 0 && *multiplier > 0 && *initial_delay <= *max_delay => Ok(()),
            Self::Exponential { .. } => Err(DurableError::Validation(
                "exponential retry delay requires a positive initial delay and multiplier, with initial_delay <= max_delay".to_owned(),
            )),
        }
    }

    fn delay_for_attempt(&self, attempt: u32) -> u64 {
        match self {
            Self::Immediate => 0,
            Self::Fixed { delay } => *delay,
            Self::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } => {
                let mut delay = *initial_delay;
                for _ in 1..attempt {
                    delay = match delay.checked_mul(u64::from(*multiplier)) {
                        Some(next) if next < *max_delay => next,
                        Some(_) | None => *max_delay,
                    };
                    if delay == *max_delay {
                        break;
                    }
                }
                delay
            }
        }
    }
}

/// Jitter policy. Randomness is never sampled by the durable reducer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JitterStrategy {
    /// Do not add jitter.
    None,
    /// Require a caller-recorded logical delay and evidence identity.
    Recorded {
        /// Inclusive maximum accepted logical jitter delay.
        max_delay: u64,
    },
}

impl JitterStrategy {
    fn verify(&self) -> DurableResult<()> {
        match self {
            Self::None => Ok(()),
            Self::Recorded { max_delay } if *max_delay > 0 => Ok(()),
            Self::Recorded { .. } => Err(DurableError::Validation(
                "recorded jitter maximum must be positive".to_owned(),
            )),
        }
    }
}

/// Immutable evidence for one externally sampled jitter value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JitterEvidence {
    /// Stable identity of the sampled observation.
    pub evidence_id: String,
    /// Logical duration added to the base schedule delay.
    pub delay: u64,
}

impl JitterEvidence {
    fn verify(&self) -> DurableResult<()> {
        if self.evidence_id.is_empty() {
            return Err(DurableError::Validation(
                "jitter evidence identity must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct RetryPolicyIdentity<'a> {
    retry_policy_version: &'a str,
    max_attempts: u32,
    retryable_failures: &'a BTreeSet<FailureClass>,
    delay: &'a RetryDelay,
    jitter: &'a JitterStrategy,
}

/// Immutable, content-addressed retry policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Frozen policy version.
    pub retry_policy_version: String,
    /// Content-addressed policy identity.
    pub policy_id: String,
    /// Total admitted attempts, including the first attempt.
    pub max_attempts: u32,
    /// Failure classes that may be retried.
    pub retryable_failures: BTreeSet<FailureClass>,
    /// Deterministic base delay strategy.
    pub delay: RetryDelay,
    /// Optional externally recorded jitter strategy.
    pub jitter: JitterStrategy,
}

impl RetryPolicy {
    /// Seal a provider-neutral retry policy and derive its stable identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the policy is malformed or cannot be canonically
    /// encoded.
    pub fn seal(
        max_attempts: u32,
        retryable_failures: BTreeSet<FailureClass>,
        delay: RetryDelay,
        jitter: JitterStrategy,
    ) -> DurableResult<Self> {
        let retry_policy_version = RETRY_POLICY_VERSION.to_owned();
        let policy_id = content_id(
            RETRY_POLICY_VERSION,
            &RetryPolicyIdentity {
                retry_policy_version: &retry_policy_version,
                max_attempts,
                retryable_failures: &retryable_failures,
                delay: &delay,
                jitter: &jitter,
            },
        )?;
        let policy = Self {
            retry_policy_version,
            policy_id,
            max_attempts,
            retryable_failures,
            delay,
            jitter,
        };
        policy.verify()?;
        Ok(policy)
    }

    /// Verify the frozen policy and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the version, schedule, jitter bound, attempt
    /// bound, or content-addressed identity is invalid.
    pub fn verify(&self) -> DurableResult<()> {
        if self.retry_policy_version != RETRY_POLICY_VERSION {
            return Err(DurableError::Validation(format!(
                "unsupported retry policy version {:?}",
                self.retry_policy_version
            )));
        }
        if self.max_attempts == 0 {
            return Err(DurableError::Validation(
                "retry policy must admit at least one attempt".to_owned(),
            ));
        }
        self.delay.verify()?;
        self.jitter.verify()?;
        let expected = content_id(
            RETRY_POLICY_VERSION,
            &RetryPolicyIdentity {
                retry_policy_version: &self.retry_policy_version,
                max_attempts: self.max_attempts,
                retryable_failures: &self.retryable_failures,
                delay: &self.delay,
                jitter: &self.jitter,
            },
        )?;
        if self.policy_id != expected {
            return Err(DurableError::Validation(
                "retry policy identity does not match its content".to_owned(),
            ));
        }
        Ok(())
    }

    /// Evaluate one failed attempt without reading ambient time or randomness.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed failure evidence, missing or out-of-bound
    /// recorded jitter, or logical-time arithmetic overflow.
    pub fn evaluate(&self, command: &RetryCommand) -> DurableResult<RetryDisposition> {
        self.verify()?;
        command.verify()?;
        match (&self.jitter, &command.jitter_evidence) {
            (JitterStrategy::None, Some(_)) => {
                return Err(DurableError::Validation(
                    "retry command supplied jitter evidence to a no-jitter policy".to_owned(),
                ));
            }
            (JitterStrategy::Recorded { max_delay }, Some(evidence))
                if evidence.delay > *max_delay =>
            {
                return Err(DurableError::Validation(format!(
                    "recorded jitter {} exceeds policy maximum {max_delay}",
                    evidence.delay
                )));
            }
            _ => {}
        }
        if let (FailureClass::UnknownWorld, FailureOperation::MutatingEffect { intent_id }) =
            (&command.failure.class, &command.failure.operation)
        {
            return Ok(RetryDisposition::Stop {
                reason: RetryStopReason::ReconciliationRequired {
                    intent_id: intent_id.clone(),
                },
            });
        }
        if command.attempt >= self.max_attempts {
            return Ok(RetryDisposition::Stop {
                reason: RetryStopReason::AttemptsExhausted,
            });
        }
        if !self.retryable_failures.contains(&command.failure.class) {
            return Ok(RetryDisposition::Stop {
                reason: RetryStopReason::FailureNotRetryable,
            });
        }

        let base_delay = self.delay.delay_for_attempt(command.attempt);
        let jitter_delay = match (&self.jitter, &command.jitter_evidence) {
            (JitterStrategy::None, None) => 0,
            (JitterStrategy::None, Some(_)) => {
                return Err(DurableError::Validation(
                    "retry command supplied jitter evidence to a no-jitter policy".to_owned(),
                ));
            }
            (JitterStrategy::Recorded { .. }, None) => {
                return Err(DurableError::Validation(
                    "recorded-jitter policy requires jitter evidence".to_owned(),
                ));
            }
            (JitterStrategy::Recorded { .. }, Some(evidence)) => {
                evidence.verify()?;
                evidence.delay
            }
        };
        let delay = base_delay.checked_add(jitter_delay).ok_or_else(|| {
            DurableError::Validation("retry delay exceeds logical time range".to_owned())
        })?;
        let next_due_at = command
            .logical_observed_at
            .checked_add(delay)
            .ok_or_else(|| {
                DurableError::Validation("retry due time exceeds logical time range".to_owned())
            })?;
        Ok(RetryDisposition::RetryAt {
            next_due_at,
            delay,
            jitter_evidence: command.jitter_evidence.clone(),
        })
    }
}

/// One retry-decision command after a failed attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryCommand {
    /// Stable retry stream identity.
    pub retry_id: String,
    /// Stable idempotency identity for this decision.
    pub decision_id: String,
    /// One-based failed attempt number.
    pub attempt: u32,
    /// Classified failure evidence.
    pub failure: RetryFailure,
    /// Explicit logical Clock observation.
    pub logical_observed_at: u64,
    /// Immutable binding used by the failed occurrence.
    pub occurrence_binding: String,
    /// Optional jitter observation required by a recorded-jitter policy.
    pub jitter_evidence: Option<JitterEvidence>,
}

impl RetryCommand {
    fn verify(&self) -> DurableResult<()> {
        if self.retry_id.is_empty()
            || self.decision_id.is_empty()
            || self.attempt == 0
            || self.occurrence_binding.is_empty()
        {
            return Err(DurableError::Validation(
                "retry command requires retry, decision, attempt, and binding identities"
                    .to_owned(),
            ));
        }
        self.failure.verify()?;
        if let Some(evidence) = &self.jitter_evidence {
            evidence.verify()?;
        }
        Ok(())
    }
}

/// Why a durable retry stream stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetryStopReason {
    /// The policy's total attempt bound was reached.
    AttemptsExhausted,
    /// The failure class is absent from the policy's retryable set.
    FailureNotRetryable,
    /// A mutating Effect has an ambiguous world outcome and must reconcile.
    ReconciliationRequired {
        /// Stable semantic Effect intent identity retained for reconciliation.
        intent_id: String,
    },
}

/// Closed output of one durable retry decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetryDisposition {
    /// The retry stream is terminal.
    Stop {
        /// Deterministic stop reason.
        reason: RetryStopReason,
    },
    /// A later attempt may start at this logical instant.
    RetryAt {
        /// Exact logical due time.
        next_due_at: u64,
        /// Total delay after base schedule and recorded jitter.
        delay: u64,
        /// Exact jitter observation retained for replay.
        #[serde(skip_serializing_if = "Option::is_none")]
        jitter_evidence: Option<JitterEvidence>,
    },
}

/// Complete persisted decision for one failed occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryDecision {
    /// Frozen decision record version.
    pub retry_decision_version: String,
    /// Content-addressed Policy selected for this retry stream.
    pub policy_id: String,
    /// Original command, including failure, Clock, jitter, and binding evidence.
    pub command: RetryCommand,
    /// Deterministic policy result.
    pub disposition: RetryDisposition,
}

impl RetryDecision {
    fn verify(&self, policy: &RetryPolicy) -> DurableResult<()> {
        if self.retry_decision_version != RETRY_DECISION_VERSION
            || self.policy_id != policy.policy_id
        {
            return Err(DurableError::Validation(
                "retry decision version or policy identity does not match".to_owned(),
            ));
        }
        self.command.verify()?;
        if self.disposition != policy.evaluate(&self.command)? {
            return Err(DurableError::Validation(format!(
                "retry decision {} does not match its policy",
                self.command.decision_id
            )));
        }
        Ok(())
    }
}

fn retry_journal_id(retry_id: &str) -> String {
    format!("cymule:retry:{retry_id}")
}

fn decode_decision(record: &JournalRecord, policy: &RetryPolicy) -> DurableResult<RetryDecision> {
    record.verify()?;
    if record.schema != RETRY_DECISION_VERSION {
        return Err(DurableError::Validation(format!(
            "retry journal contains unsupported schema {:?}",
            record.schema
        )));
    }
    let decision: RetryDecision = serde_json::from_value(record.payload.clone())?;
    decision.verify(policy)?;
    if record.record_id != decision.command.decision_id {
        return Err(DurableError::Validation(
            "retry journal record identity does not match its decision".to_owned(),
        ));
    }
    Ok(decision)
}

impl<S: DurableStore> DurableCoordinator<S> {
    /// Evaluate and atomically retain one deterministic retry decision.
    ///
    /// Reusing a decision identity with the identical command returns the
    /// original record. A post-commit acknowledgement loss is recovered by
    /// reopening the coordinator and submitting that same command.
    ///
    /// # Errors
    ///
    /// Returns an error when the Policy or command is invalid, stream order or
    /// due-time admission fails, an identity conflicts, or the durable CAS
    /// cannot commit.
    pub fn decide_retry(
        &mut self,
        policy: &RetryPolicy,
        command: RetryCommand,
    ) -> DurableResult<RetryDecision> {
        policy.verify()?;
        command.verify()?;
        let journal_id = retry_journal_id(&command.retry_id);
        let records = self.journal_records(&journal_id)?;
        let mut decisions = Vec::with_capacity(records.len());
        for record in records {
            decisions.push(decode_decision(record, policy)?);
        }
        if let Some(existing) = decisions
            .iter()
            .find(|decision| decision.command.decision_id == command.decision_id)
        {
            if existing.command == command {
                return Ok(existing.clone());
            }
            return Err(DurableError::IllegalTransition(format!(
                "retry decision {} was reused with different content",
                command.decision_id
            )));
        }
        if decisions
            .iter()
            .any(|decision| decision.command.failure.failure_id == command.failure.failure_id)
        {
            return Err(DurableError::IllegalTransition(format!(
                "retry failure {} already has a decision",
                command.failure.failure_id
            )));
        }

        match decisions.last() {
            None if command.attempt != 1 => {
                return Err(DurableError::IllegalTransition(
                    "the first retry decision must describe attempt 1".to_owned(),
                ));
            }
            Some(previous) => match &previous.disposition {
                RetryDisposition::Stop { .. } => {
                    return Err(DurableError::IllegalTransition(format!(
                        "retry stream {} is terminal",
                        command.retry_id
                    )));
                }
                RetryDisposition::RetryAt { next_due_at, .. } => {
                    let expected_attempt =
                        previous.command.attempt.checked_add(1).ok_or_else(|| {
                            DurableError::Validation(
                                "retry attempt exceeds supported range".to_owned(),
                            )
                        })?;
                    if command.attempt != expected_attempt {
                        return Err(DurableError::IllegalTransition(format!(
                            "retry stream {} expected attempt {expected_attempt}, received {}",
                            command.retry_id, command.attempt
                        )));
                    }
                    if command.logical_observed_at < *next_due_at {
                        return Err(DurableError::IllegalTransition(format!(
                            "retry attempt {} was observed before its admitted due time",
                            command.attempt
                        )));
                    }
                }
            },
            None => {}
        }

        let decision = RetryDecision {
            retry_decision_version: RETRY_DECISION_VERSION.to_owned(),
            policy_id: policy.policy_id.clone(),
            disposition: policy.evaluate(&command)?,
            command,
        };
        decision.verify(policy)?;
        let record = JournalRecord::new(
            decision.command.decision_id.clone(),
            RETRY_DECISION_VERSION,
            serde_json::to_value(&decision)?,
        )?;
        self.append_journal_record(&journal_id, record)?;
        Ok(decision)
    }

    /// Restore and verify all decisions for one retry stream.
    ///
    /// # Errors
    ///
    /// Returns an error when the Policy or retry identity is invalid, or a
    /// persisted decision cannot be verified against the pinned Policy.
    pub fn retry_decisions(
        &self,
        policy: &RetryPolicy,
        retry_id: &str,
    ) -> DurableResult<Vec<RetryDecision>> {
        policy.verify()?;
        if retry_id.is_empty() {
            return Err(DurableError::Validation(
                "retry stream identity must not be empty".to_owned(),
            ));
        }
        self.journal_records(&retry_journal_id(retry_id))?
            .iter()
            .map(|record| decode_decision(record, policy))
            .collect()
    }
}
