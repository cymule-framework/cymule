use std::collections::BTreeSet;
use std::ops::Deref;

use cymule_core::content_id;
use serde::{Deserialize, Serialize};

use crate::{
    ClockObservation, ClockObservationAuthority, ClockObservationRef, DurableError, DurableResult,
};

/// Frozen provider-neutral retry policy version.
pub const RETRY_POLICY_VERSION: &str = "cymule.retry-policy/1";
/// Serializable retry decision record version.
pub const RETRY_DECISION_VERSION: &str = "cymule.retry-decision/2";
/// Content-addressed recorded jitter evidence version.
pub const JITTER_EVIDENCE_VERSION: &str = "cymule.jitter-evidence/1";
/// Serializable retry stream reducer state version.
pub const RETRY_STREAM_VERSION: &str = "cymule.retry-stream/2";
const RETRY_CLOCK_SCOPE_ID_DOMAIN: &str = "cymule.retry-clock-scope/1";

/// Closed failure classes used by retry admission.
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
            Self::ObservationalEffect { intent_id } | Self::MutatingEffect { intent_id } => {
                crate::model::validate_sha256_identity("retry effect intent", intent_id)
            }
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
            Self::Fixed { delay } if *delay > 0 && *delay <= crate::MAX_EXACT_INTEGER => Ok(()),
            Self::Fixed { .. } => Err(DurableError::Validation(
                "fixed retry delay must be positive".to_owned(),
            )),
            Self::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } if *initial_delay > 0
                && *multiplier > 0
                && *initial_delay <= *max_delay
                && *max_delay <= crate::MAX_EXACT_INTEGER =>
            {
                Ok(())
            }
            Self::Exponential { .. } => Err(DurableError::Validation(
                "exponential retry delay requires a positive initial delay and multiplier, with initial_delay <= max_delay".to_owned(),
            )),
        }
    }

    fn delay_for_attempt(&self, attempt: u32) -> DurableResult<u64> {
        let exponent = attempt.checked_sub(1).ok_or_else(|| {
            DurableError::Validation("retry delay requires a positive attempt ordinal".to_owned())
        })?;
        Ok(match self {
            Self::Immediate => 0,
            Self::Fixed { delay } => *delay,
            Self::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } => capped_exponential_delay(
                *initial_delay,
                u64::from(*multiplier),
                exponent,
                *max_delay,
            ),
        })
    }
}

fn capped_exponential_delay(initial: u64, multiplier: u64, mut exponent: u32, cap: u64) -> u64 {
    let capped_mul = |left: u64, right: u64| {
        left.checked_mul(right)
            .filter(|value| *value < cap)
            .unwrap_or(cap)
    };
    let mut result = initial.min(cap);
    let mut factor = multiplier.min(cap);
    while exponent != 0 && result != cap {
        if exponent & 1 == 1 {
            result = capped_mul(result, factor);
        }
        exponent >>= 1;
        if exponent != 0 {
            factor = capped_mul(factor, factor);
        }
    }
    result
}

/// Jitter policy. Randomness is never sampled by the retry reducer.
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
            Self::Recorded { max_delay }
                if *max_delay > 0 && *max_delay <= crate::MAX_EXACT_INTEGER =>
            {
                Ok(())
            }
            Self::Recorded { .. } => Err(DurableError::Validation(
                "recorded jitter maximum must be positive".to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct JitterEvidenceIdentity<'a> {
    jitter_evidence_version: &'a str,
    source_binding: &'a str,
    delay: u64,
}

/// Immutable, content-addressed evidence for one externally sampled jitter value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JitterEvidence {
    /// Frozen evidence version.
    pub jitter_evidence_version: String,
    /// Content identity of the complete sampled observation.
    pub evidence_id: String,
    /// Immutable jitter-source binding that produced the sample.
    pub source_binding: String,
    /// Logical duration added to the base schedule delay.
    pub delay: u64,
}

impl JitterEvidence {
    /// Seal one externally sampled jitter value.
    ///
    /// # Errors
    ///
    /// Returns an error when the source binding is empty or the evidence cannot
    /// be canonically encoded.
    pub fn seal(source_binding: impl Into<String>, delay: u64) -> DurableResult<Self> {
        let source_binding = source_binding.into();
        if source_binding.is_empty() || delay > crate::MAX_EXACT_INTEGER {
            return Err(DurableError::Validation(
                "jitter evidence requires a source and exact-range delay".to_owned(),
            ));
        }
        let jitter_evidence_version = JITTER_EVIDENCE_VERSION.to_owned();
        let evidence_id = content_id(
            JITTER_EVIDENCE_VERSION,
            &JitterEvidenceIdentity {
                jitter_evidence_version: &jitter_evidence_version,
                source_binding: &source_binding,
                delay,
            },
        )?;
        Ok(Self {
            jitter_evidence_version,
            evidence_id,
            source_binding,
            delay,
        })
    }

    /// Verify the evidence version and content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the source binding is empty or the identity does
    /// not match the complete jitter evidence content.
    pub fn verify(&self) -> DurableResult<()> {
        if self.jitter_evidence_version != JITTER_EVIDENCE_VERSION
            || self.source_binding.is_empty()
            || self.delay > crate::MAX_EXACT_INTEGER
        {
            return Err(DurableError::Validation(
                "jitter evidence version or source binding is invalid".to_owned(),
            ));
        }
        let expected = content_id(
            JITTER_EVIDENCE_VERSION,
            &JitterEvidenceIdentity {
                jitter_evidence_version: &self.jitter_evidence_version,
                source_binding: &self.source_binding,
                delay: self.delay,
            },
        )?;
        if self.evidence_id != expected {
            return Err(DurableError::Validation(
                "jitter evidence identity does not match its content".to_owned(),
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

    /// Resolve and evaluate one failed attempt without reading ambient time or
    /// randomness outside the selected Clock authority.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed failure evidence, missing or out-of-bound
    /// recorded jitter, or logical-time arithmetic overflow.
    pub fn evaluate<C: ClockObservationAuthority>(
        &self,
        command: RetryCommand,
        authority: &mut C,
    ) -> DurableResult<RetryDisposition> {
        let admission = command.admit(authority)?;
        self.evaluate_admitted(&admission)
    }

    fn evaluate_admitted(&self, admission: &RetryAdmission) -> DurableResult<RetryDisposition> {
        self.verify()?;
        admission.verify()?;
        let command = &admission.command;
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
        if let (
            FailureClass::UnknownWorld,
            FailureOperation::ObservationalEffect { intent_id }
            | FailureOperation::MutatingEffect { intent_id },
        ) = (&command.failure.class, &command.failure.operation)
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

        let base_delay = self.delay.delay_for_attempt(command.attempt)?;
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
        let delay = base_delay
            .checked_add(jitter_delay)
            .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
            .ok_or_else(|| {
                DurableError::Validation("retry delay exceeds logical time range".to_owned())
            })?;
        let next_due_at = admission
            .observation
            .logical_time
            .checked_add(delay)
            .filter(|value| *value <= crate::MAX_EXACT_INTEGER)
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
    /// Opaque reference to a receipt already issued by the selected Clock.
    pub clock: ClockObservationRef,
    /// Immutable binding used by the failed occurrence.
    pub occurrence_binding: String,
    /// Optional jitter observation required by a recorded-jitter policy.
    #[serde(deserialize_with = "deserialize_required_nullable")]
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
        self.clock.verify()?;
        if self.clock.scope != retry_clock_scope(&self.retry_id)? {
            return Err(DurableError::Validation(
                "retry Clock reference does not match its exact stream scope".to_owned(),
            ));
        }
        if let Some(evidence) = &self.jitter_evidence {
            evidence.verify()?;
        }
        Ok(())
    }
}

fn deserialize_required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

impl RetryCommand {
    /// Resolve this command through the selected issued-receipt authority.
    ///
    /// # Errors
    ///
    /// Returns an error when the command or its evidence is malformed, the
    /// Clock cannot resolve the reference, or the issued observation does not
    /// exactly match that reference.
    pub fn admit<C: ClockObservationAuthority>(
        self,
        authority: &mut C,
    ) -> DurableResult<RetryAdmission> {
        self.verify()?;
        let observation = authority.resolve(&self.clock)?;
        observation.verify()?;
        if observation.reference() != self.clock {
            return Err(DurableError::Validation(
                "Clock authority returned a different retry observation".to_owned(),
            ));
        }
        let admission = RetryAdmission {
            command: self,
            observation,
        };
        admission.verify()?;
        Ok(admission)
    }
}

/// One retry command admitted by a selected issued-receipt Clock authority.
/// The full receipt is retained for pure replay only after that admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryAdmission {
    command: RetryCommand,
    observation: ClockObservation,
}

impl RetryAdmission {
    /// Borrow the caller command and opaque Clock reference.
    pub const fn command(&self) -> &RetryCommand {
        &self.command
    }

    /// Borrow the full receipt retained after authority resolution.
    pub const fn observation(&self) -> &ClockObservation {
        &self.observation
    }

    fn verify(&self) -> DurableResult<()> {
        self.command.verify()?;
        self.observation.verify()?;
        if self.observation.reference() != self.command.clock {
            return Err(DurableError::Validation(
                "retry admission receipt does not match its command reference".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_issued<C: ClockObservationAuthority>(&self, authority: &mut C) -> DurableResult<()> {
        self.verify()?;
        let retained = authority.resolve(&self.command.clock)?;
        retained.verify()?;
        if retained != self.observation {
            return Err(DurableError::Validation(
                "retained retry admission does not match the selected Clock ledger".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Why a retry stream stopped.
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

/// Closed output of one retry decision.
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

/// Complete serializable decision for one failed occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryDecision {
    /// Frozen decision record version.
    pub retry_decision_version: String,
    /// Content-addressed Policy selected for this retry stream.
    pub policy_id: String,
    /// Original command plus the Clock receipt retained after authority admission.
    pub admission: RetryAdmission,
    /// Deterministic policy result.
    pub disposition: RetryDisposition,
}

impl RetryDecision {
    fn verify_admitted(&self, policy: &RetryPolicy) -> DurableResult<()> {
        if self.retry_decision_version != RETRY_DECISION_VERSION
            || self.policy_id != policy.policy_id
        {
            return Err(DurableError::Validation(
                "retry decision version or policy identity does not match".to_owned(),
            ));
        }
        self.admission.verify()?;
        if self.disposition != policy.evaluate_admitted(&self.admission)? {
            return Err(DurableError::Validation(format!(
                "retry decision {} does not match its policy",
                self.admission.command.decision_id
            )));
        }
        Ok(())
    }
}

/// Serializable state of one retry policy reducer.
///
/// This type deliberately does not perform a durable write or trust serialized
/// time evidence by itself. Apply and restore verification resolve every opaque
/// Clock reference through the selected issuance authority. An executor must
/// checkpoint the updated stream together with the failed occurrence,
/// Continuation/timer state, and next-attempt admission in its owning CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryStream {
    /// Frozen stream state version.
    pub retry_stream_version: String,
    /// Stable identity shared by every command in the stream.
    pub retry_id: String,
    /// Complete immutable canonical Policy, retained from the first checkpoint.
    pub policy: RetryPolicy,
    /// Ordered exact decisions already admitted by the reducer.
    pub decisions: Vec<RetryDecision>,
}

impl RetryStream {
    /// Verify the complete stream from its retained Policy, issued Clock
    /// receipts, and decision history.
    ///
    /// # Errors
    ///
    /// Returns an error for a malformed version or identity, altered Policy,
    /// non-sequential attempt, early Clock observation, duplicate decision, or
    /// a decision after a terminal result.
    pub fn verify<C: ClockObservationAuthority>(
        self,
        authority: &mut C,
    ) -> DurableResult<VerifiedRetryStream> {
        self.verify_header()?;
        let mut replay = VerifiedRetryStream::from_empty(Self {
            retry_stream_version: self.retry_stream_version.clone(),
            retry_id: self.retry_id.clone(),
            policy: self.policy.clone(),
            decisions: Vec::new(),
        });
        for expected in self.decisions {
            expected.admission.verify_issued(authority)?;
            let previous_len = replay.stream.decisions.len();
            let actual = replay
                .apply_admitted(expected.admission.clone())
                .map_err(|error| {
                    DurableError::Validation(format!(
                        "retry decision {} is duplicated or invalid: {error}",
                        expected.admission.command.decision_id
                    ))
                })?;
            if actual != expected || replay.stream.decisions.len() != previous_len + 1 {
                return Err(DurableError::Validation(format!(
                    "retry decision {} is duplicated or does not match replay",
                    expected.admission.command.decision_id
                )));
            }
        }
        Ok(replay)
    }

    fn verify_header(&self) -> DurableResult<()> {
        if self.retry_stream_version != RETRY_STREAM_VERSION || self.retry_id.is_empty() {
            return Err(DurableError::Validation(
                "retry stream version or identity is invalid".to_owned(),
            ));
        }
        self.policy.verify()?;
        Ok(())
    }

    fn apply_admitted(&mut self, admission: RetryAdmission) -> DurableResult<RetryDecision> {
        admission.verify()?;
        let command = &admission.command;
        if command.retry_id != self.retry_id {
            return Err(DurableError::IllegalTransition(format!(
                "retry command {} does not belong to stream {}",
                command.retry_id, self.retry_id
            )));
        }
        if self.decisions.iter().any(|decision| {
            decision.admission.command.decision_id == command.decision_id
                || decision.admission.command.failure.failure_id == command.failure.failure_id
        }) {
            return Err(DurableError::IllegalTransition(format!(
                "retry decision {} or failure {} is duplicated",
                command.decision_id, command.failure.failure_id
            )));
        }
        if let Some(previous) = self.decisions.last()
            && (previous.admission.command.clock.source_id != command.clock.source_id
                || previous.admission.command.clock.source_generation
                    != command.clock.source_generation
                || previous.admission.command.clock.scope != command.clock.scope)
        {
            return Err(DurableError::IllegalTransition(
                "retry stream cannot change its Clock source generation or scope".to_owned(),
            ));
        }
        match self.decisions.last() {
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
                    let expected_attempt = previous
                        .admission
                        .command
                        .attempt
                        .checked_add(1)
                        .ok_or_else(|| {
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
                    if admission.observation.logical_time < *next_due_at {
                        return Err(DurableError::IllegalTransition(format!(
                            "retry attempt {} was observed before its admitted due time",
                            command.attempt
                        )));
                    }
                }
            },
            None => {}
        }

        let retry_decision_version = RETRY_DECISION_VERSION.to_owned();
        let policy_id = self.policy.policy_id.clone();
        let disposition = self.policy.evaluate_admitted(&admission)?;
        let decision = RetryDecision {
            retry_decision_version,
            policy_id,
            admission,
            disposition,
        };
        decision.verify_admitted(&self.policy)?;
        self.decisions.push(decision.clone());
        Ok(decision)
    }
}

/// Runtime-verified retry reducer. Deserialization always yields an untrusted
/// [`RetryStream`]; callers must verify it once against the selected Clock
/// authority to obtain this wrapper. Each later command resolves and verifies
/// only its new observation instead of replaying the full retained prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRetryStream {
    stream: RetryStream,
    decision_ids: BTreeSet<String>,
    failure_ids: BTreeSet<String>,
}

impl VerifiedRetryStream {
    /// Create one empty verified retry stream with its canonical Policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the retry identity or Policy is invalid.
    pub fn new(retry_id: impl Into<String>, policy: RetryPolicy) -> DurableResult<Self> {
        let stream = RetryStream {
            retry_stream_version: RETRY_STREAM_VERSION.to_owned(),
            retry_id: retry_id.into(),
            policy,
            decisions: Vec::new(),
        };
        stream.verify_header()?;
        Ok(Self::from_empty(stream))
    }

    fn from_empty(stream: RetryStream) -> Self {
        debug_assert!(stream.decisions.is_empty());
        Self {
            stream,
            decision_ids: BTreeSet::new(),
            failure_ids: BTreeSet::new(),
        }
    }

    /// Borrow the serializable stream checkpoint.
    pub const fn stream(&self) -> &RetryStream {
        &self.stream
    }

    /// Consume this verified runtime view into its serializable checkpoint.
    pub fn into_stream(self) -> RetryStream {
        self.stream
    }

    /// Admit and incrementally apply one failed attempt.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or conflicting command evidence, a
    /// failure already decided, a changed Clock source, a non-sequential or
    /// early attempt, a terminal stream, or failed Clock or policy admission.
    ///
    /// # Panics
    ///
    /// Panics if the private verified decision index refers to a decision
    /// missing from the retained stream.
    pub fn apply<C: ClockObservationAuthority>(
        &mut self,
        command: RetryCommand,
        authority: &mut C,
    ) -> DurableResult<RetryDecision> {
        command.verify()?;
        if command.retry_id != self.stream.retry_id {
            return Err(DurableError::IllegalTransition(format!(
                "retry command {} does not belong to stream {}",
                command.retry_id, self.stream.retry_id
            )));
        }
        if self.decision_ids.contains(&command.decision_id) {
            let existing = self
                .stream
                .decisions
                .iter()
                .find(|decision| decision.admission.command.decision_id == command.decision_id)
                .expect("verified decision index points to retained decision");
            if existing.admission.command == command {
                return Ok(existing.clone());
            }
            return Err(DurableError::IllegalTransition(format!(
                "retry decision {} was reused with different content",
                command.decision_id
            )));
        }
        if self.failure_ids.contains(&command.failure.failure_id) {
            return Err(DurableError::IllegalTransition(format!(
                "retry failure {} already has a decision",
                command.failure.failure_id
            )));
        }
        if let Some(previous) = self.stream.decisions.last()
            && (previous.admission.command.clock.source_id != command.clock.source_id
                || previous.admission.command.clock.source_generation
                    != command.clock.source_generation
                || previous.admission.command.clock.scope != command.clock.scope)
        {
            return Err(DurableError::IllegalTransition(
                "retry stream cannot change its Clock source generation or scope".to_owned(),
            ));
        }
        let admission = command.admit(authority)?;
        self.apply_admitted(admission)
    }

    fn apply_admitted(&mut self, admission: RetryAdmission) -> DurableResult<RetryDecision> {
        let decision_id = admission.command.decision_id.clone();
        let failure_id = admission.command.failure.failure_id.clone();
        let decision = self.stream.apply_admitted(admission)?;
        if !self.decision_ids.insert(decision_id) || !self.failure_ids.insert(failure_id) {
            return Err(DurableError::Integrity {
                code: "verified_retry_index_diverged".to_owned(),
                message: "verified retry identity index diverged from its stream".to_owned(),
            });
        }
        Ok(decision)
    }

    /// Re-audit the complete serializable prefix against cold Clock authority.
    ///
    /// # Errors
    ///
    /// Returns an error when retained policy or decision evidence is invalid,
    /// replay diverges, or the Clock cannot authenticate an issued observation.
    pub fn audit<C: ClockObservationAuthority>(&self, authority: &mut C) -> DurableResult<()> {
        self.stream.clone().verify(authority).map(|_| ())
    }
}

impl Deref for VerifiedRetryStream {
    type Target = RetryStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl Serialize for VerifiedRetryStream {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.stream.serialize(serializer)
    }
}

fn retry_clock_scope(retry_id: &str) -> DurableResult<String> {
    if retry_id.is_empty() {
        return Err(DurableError::Validation(
            "retry identity must not be empty".to_owned(),
        ));
    }
    content_id(RETRY_CLOCK_SCOPE_ID_DOMAIN, &retry_id).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capped_exponential_delay_is_exact_and_logarithmic_at_u32_max() {
        let unchanged = RetryDelay::Exponential {
            initial_delay: 7,
            multiplier: 1,
            max_delay: 100,
        };
        assert_eq!(
            unchanged
                .delay_for_attempt(u32::MAX)
                .expect("positive attempt derives delay"),
            7
        );

        let capped = RetryDelay::Exponential {
            initial_delay: 3,
            multiplier: 2,
            max_delay: 1_000_000,
        };
        assert_eq!(
            capped
                .delay_for_attempt(1)
                .expect("first attempt derives delay"),
            3
        );
        assert_eq!(
            capped
                .delay_for_attempt(4)
                .expect("later attempt derives delay"),
            24
        );
        assert_eq!(
            capped
                .delay_for_attempt(u32::MAX)
                .expect("maximum attempt derives capped delay"),
            1_000_000
        );

        let overflowing = RetryDelay::Exponential {
            initial_delay: crate::MAX_EXACT_INTEGER - 1,
            multiplier: u32::MAX,
            max_delay: crate::MAX_EXACT_INTEGER,
        };
        assert_eq!(
            overflowing
                .delay_for_attempt(u32::MAX)
                .expect("overflowing exponent saturates only at the declared semantic cap"),
            crate::MAX_EXACT_INTEGER
        );
        assert!(matches!(
            capped.delay_for_attempt(0),
            Err(DurableError::Validation(_))
        ));
    }
}
