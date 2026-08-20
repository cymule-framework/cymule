//! Workspace-private deterministic test composition for Cymule.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::{Builder, TempDir};

/// Frozen language-neutral generated trace fixture version.
pub const TRACE_VERSION: &str = "cymule.test-trace/2";

/// Errors raised by deterministic test composition.
#[derive(Debug)]
pub enum TestWorldError {
    /// A clock, fault plan, path, or environment value is invalid.
    Invalid(String),
    /// A host operation failed.
    Io(io::Error),
    /// A managed child exited before its external barrier.
    ChildExited(ExitStatus),
    /// A managed child did not reach its external barrier in time.
    ChildTimedOut {
        /// Barrier path that was not created.
        path: PathBuf,
        /// Caller-selected host wait bound.
        timeout: Duration,
    },
    /// A generated trace fixture could not be encoded.
    Json(serde_json::Error),
}

impl fmt::Display for TestWorldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Io(error) => write!(formatter, "host operation failed: {error}"),
            Self::ChildExited(status) => {
                write!(
                    formatter,
                    "managed child exited before its barrier: {status}"
                )
            }
            Self::ChildTimedOut { path, timeout } => write!(
                formatter,
                "managed child did not create {} within {timeout:?}",
                path.display()
            ),
            Self::Json(error) => write!(formatter, "trace fixture encoding failed: {error}"),
        }
    }
}

impl std::error::Error for TestWorldError {}

impl From<io::Error> for TestWorldError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for TestWorldError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

/// Result returned by test-world operations.
pub type TestWorldResult<T> = Result<T, TestWorldError>;

/// An explicitly advanced logical clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManualClock {
    now: u64,
}

impl ManualClock {
    /// Start at an exact logical time.
    pub const fn new(now: u64) -> Self {
        Self { now }
    }

    /// Observe the current logical time without advancing it.
    pub const fn now(&self) -> u64 {
        self.now
    }

    /// Set a non-decreasing logical time.
    ///
    /// # Errors
    ///
    /// Returns an error when `next` moves backward.
    pub fn set(&mut self, next: u64) -> TestWorldResult<u64> {
        if next < self.now {
            return Err(TestWorldError::Invalid(format!(
                "manual clock cannot move backward from {} to {next}",
                self.now
            )));
        }
        self.now = next;
        Ok(self.now)
    }

    /// Advance by an exact logical delta.
    ///
    /// # Errors
    ///
    /// Returns an error when the logical time would overflow.
    pub fn advance(&mut self, delta: u64) -> TestWorldResult<u64> {
        let next = self
            .now
            .checked_add(delta)
            .ok_or_else(|| TestWorldError::Invalid("manual clock overflow".to_owned()))?;
        self.set(next)
    }
}

/// A finite sequence of exact clock observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptedClock {
    remaining: VecDeque<u64>,
    previous: Option<u64>,
}

impl ScriptedClock {
    /// Validate and retain a non-decreasing observation script.
    ///
    /// # Errors
    ///
    /// Returns an error when the script moves backward.
    pub fn new(observations: impl IntoIterator<Item = u64>) -> TestWorldResult<Self> {
        let remaining: VecDeque<u64> = observations.into_iter().collect();
        if remaining
            .iter()
            .zip(remaining.iter().skip(1))
            .any(|(left, right)| right < left)
        {
            return Err(TestWorldError::Invalid(
                "scripted clock observations must be non-decreasing".to_owned(),
            ));
        }
        Ok(Self {
            remaining,
            previous: None,
        })
    }

    /// Return the next scripted observation and fail when exhausted.
    ///
    /// # Errors
    ///
    /// Returns an error after the final scripted observation is consumed.
    pub fn observe(&mut self) -> TestWorldResult<u64> {
        let next = self
            .remaining
            .pop_front()
            .ok_or_else(|| TestWorldError::Invalid("scripted clock is exhausted".to_owned()))?;
        self.previous = Some(next);
        Ok(next)
    }

    /// Return the most recent observation, when one exists.
    pub const fn previous(&self) -> Option<u64> {
        self.previous
    }

    /// Return the number of observations not yet consumed.
    pub fn remaining(&self) -> usize {
        self.remaining.len()
    }
}

/// A small reproducible generator used only by test cases.
#[derive(Debug, Clone)]
pub struct SeededRandom {
    seed: u64,
    random: StdRng,
}

impl SeededRandom {
    /// Construct a generator from the seed printed in a failure report.
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            random: StdRng::seed_from_u64(seed),
        }
    }

    /// Return the retained replay seed.
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Generate the next deterministic word from the pinned standard generator.
    pub fn next_u64(&mut self) -> u64 {
        self.random.random()
    }

    /// Select an index in `0..upper` without floating point state.
    ///
    /// # Errors
    ///
    /// Returns an error when `upper` is zero.
    pub fn index(&mut self, upper: usize) -> TestWorldResult<usize> {
        if upper == 0 {
            return Err(TestWorldError::Invalid(
                "random index upper bound must be positive".to_owned(),
            ));
        }
        Ok(self.random.random_range(0..upper))
    }
}

/// One closed action injected at an identified test boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultAction {
    /// Fail before the operation becomes authoritative.
    ErrorBefore,
    /// Let the operation commit, then lose its acknowledgement.
    AcknowledgementLostAfter,
}

/// One identified fault occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultStep {
    /// Stable operation name owned by the test adapter.
    pub operation: String,
    /// Stable original command path. An empty path denotes a non-trace operation.
    pub path: Vec<usize>,
    /// One-based occurrence count for that operation.
    pub occurrence: u64,
    /// Closed fault action.
    pub action: FaultAction,
}

/// Serializable fault input with no mutable execution state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultPlan {
    /// Faults in authored order.
    pub steps: Vec<FaultStep>,
}

impl FaultPlan {
    /// Validate an exact plan and reject duplicate operation occurrences.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty operation, zero occurrence, or duplicate identity.
    pub fn new(steps: Vec<FaultStep>) -> TestWorldResult<Self> {
        let mut identities = BTreeSet::new();
        for step in &steps {
            if step.operation.is_empty() || step.occurrence == 0 {
                return Err(TestWorldError::Invalid(
                    "fault operation must be non-empty and occurrence must be positive".to_owned(),
                ));
            }
            if !identities.insert((step.operation.clone(), step.path.clone(), step.occurrence)) {
                return Err(TestWorldError::Invalid(format!(
                    "duplicate fault at {} path {:?} occurrence {}",
                    step.operation, step.path, step.occurrence
                )));
            }
        }
        Ok(Self { steps })
    }

    /// Create fresh mutable counters for one execution.
    pub fn schedule(&self) -> FaultSchedule {
        FaultSchedule {
            plan: self.clone(),
            counts: BTreeMap::new(),
            consumed: BTreeSet::new(),
        }
    }
}

/// One execution of a serializable fault plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultSchedule {
    plan: FaultPlan,
    counts: BTreeMap<(String, Vec<usize>), u64>,
    consumed: BTreeSet<usize>,
}

impl FaultSchedule {
    /// Observe one operation and return its selected action at most once.
    pub fn observe(&mut self, operation: &str) -> Option<FaultAction> {
        self.observe_path(operation, &[])
    }

    /// Observe one operation at its stable original command path.
    pub fn observe_path(&mut self, operation: &str, path: &[usize]) -> Option<FaultAction> {
        let identity = (operation.to_owned(), path.to_vec());
        let count = self.counts.entry(identity).or_insert(0);
        *count = count.saturating_add(1);
        let occurrence = *count;
        let selected = self
            .plan
            .steps
            .iter()
            .enumerate()
            .find_map(|(index, step)| {
                (step.operation == operation
                    && step.path == path
                    && step.occurrence == occurrence
                    && !self.consumed.contains(&index))
                .then_some((index, step.action))
            });
        if let Some((index, action)) = selected {
            self.consumed.insert(index);
            Some(action)
        } else {
            None
        }
    }

    /// Return how often an operation has been observed.
    pub fn observations(&self, operation: &str) -> u64 {
        self.observations_at(operation, &[])
    }

    /// Return how often an operation has been observed at one command path.
    pub fn observations_at(&self, operation: &str, path: &[usize]) -> u64 {
        self.counts
            .get(&(operation.to_owned(), path.to_vec()))
            .copied()
            .unwrap_or(0)
    }

    /// Return authored faults that were never reached by this execution.
    pub fn unconsumed_steps(&self) -> Vec<FaultStep> {
        self.plan
            .steps
            .iter()
            .enumerate()
            .filter(|(index, _)| !self.consumed.contains(index))
            .map(|(_, step)| step.clone())
            .collect()
    }
}

/// One deterministic observation retained by a test.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    /// Stable sequence assigned by the recorder.
    pub sequence: u64,
    /// Caller-owned logical time.
    pub logical_time: u64,
    /// Closed-by-the-test observation kind.
    pub kind: String,
    /// Deterministically ordered observation fields.
    pub fields: BTreeMap<String, Value>,
}

/// An ordered in-process observer with no global subscriber.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RecordingObserver {
    observations: Vec<Observation>,
}

impl RecordingObserver {
    /// Retain one observation at an explicit logical time.
    ///
    /// # Errors
    ///
    /// Returns an error if the observation sequence cannot be represented by `u64`.
    pub fn record(
        &mut self,
        logical_time: u64,
        kind: impl Into<String>,
        fields: BTreeMap<String, Value>,
    ) -> TestWorldResult<()> {
        let sequence = u64::try_from(self.observations.len())
            .map_err(|_| TestWorldError::Invalid("observation sequence overflow".to_owned()))?;
        self.observations.push(Observation {
            sequence,
            logical_time,
            kind: kind.into(),
            fields,
        });
        Ok(())
    }

    /// Borrow the complete ordered recording.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }
}

/// A temporary filesystem root for one durable-domain test.
#[derive(Debug)]
pub struct TempDurableDomain {
    domain_id: String,
    root: TempDir,
}

impl TempDurableDomain {
    fn new(seed: u64) -> TestWorldResult<Self> {
        let root = Builder::new().prefix("cymule-test-world-").tempdir()?;
        Ok(Self {
            domain_id: format!("domain:test-world:{seed}"),
            root,
        })
    }

    /// Return the explicit test domain identity.
    pub fn domain_id(&self) -> &str {
        &self.domain_id
    }

    /// Return the temporary domain root.
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// Resolve a relative test path without permitting root escape.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is absolute or contains a parent/root component.
    pub fn path(&self, relative: impl AsRef<Path>) -> TestWorldResult<PathBuf> {
        let relative = relative.as_ref();
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(TestWorldError::Invalid(
                "test-domain paths must stay relative to the temporary root".to_owned(),
            ));
        }
        Ok(self.root.path().join(relative))
    }
}

/// Deterministic inputs and owned host resources for one test case.
#[derive(Debug)]
pub struct TestWorld {
    seed: u64,
    clock: ManualClock,
    random: SeededRandom,
    faults: FaultSchedule,
    observer: RecordingObserver,
    domain: TempDurableDomain,
}

impl TestWorld {
    /// Create a world with no injected faults.
    ///
    /// # Errors
    ///
    /// Returns an error when its temporary durable-domain root cannot be created.
    pub fn new(seed: u64) -> TestWorldResult<Self> {
        Self::with_faults(seed, FaultPlan::default())
    }

    /// Create a world from an exact seed and fault plan.
    ///
    /// # Errors
    ///
    /// Returns an error when the fault plan is invalid or the temporary root cannot be created.
    pub fn with_faults(seed: u64, faults: FaultPlan) -> TestWorldResult<Self> {
        let faults = FaultPlan::new(faults.steps)?;
        Ok(Self {
            seed,
            clock: ManualClock::new(0),
            random: SeededRandom::new(seed),
            faults: faults.schedule(),
            observer: RecordingObserver::default(),
            domain: TempDurableDomain::new(seed)?,
        })
    }

    /// Return the exact replay seed.
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Borrow the logical clock.
    pub const fn clock(&self) -> &ManualClock {
        &self.clock
    }

    /// Mutably borrow the logical clock.
    pub const fn clock_mut(&mut self) -> &mut ManualClock {
        &mut self.clock
    }

    /// Mutably borrow the seeded generator.
    pub const fn random_mut(&mut self) -> &mut SeededRandom {
        &mut self.random
    }

    /// Mutably borrow the fault execution.
    pub const fn faults_mut(&mut self) -> &mut FaultSchedule {
        &mut self.faults
    }

    /// Borrow recorded observations.
    pub const fn observer(&self) -> &RecordingObserver {
        &self.observer
    }

    /// Mutably borrow the recording observer.
    pub const fn observer_mut(&mut self) -> &mut RecordingObserver {
        &mut self.observer
    }

    /// Borrow the temporary durable-domain root.
    pub const fn domain(&self) -> &TempDurableDomain {
        &self.domain
    }
}

/// A child process that is always killed if necessary and reaped on teardown.
#[derive(Debug)]
pub struct ManagedChild {
    child: Option<Child>,
    status: Option<ExitStatus>,
}

impl ManagedChild {
    /// Spawn one already configured command.
    ///
    /// # Errors
    ///
    /// Returns an error when the host cannot spawn the command.
    pub fn spawn(command: &mut Command) -> TestWorldResult<Self> {
        Ok(Self {
            child: Some(command.spawn()?),
            status: None,
        })
    }

    /// Return the operating-system process identity.
    pub fn id(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Wait for a child-owned external barrier without treating time as semantic input.
    ///
    /// # Errors
    ///
    /// Returns an error when status observation fails, the child exits, or the bound expires.
    pub fn wait_for_path(&mut self, path: &Path, timeout: Duration) -> TestWorldResult<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if fs::metadata(path).is_ok() {
                return Ok(());
            }
            let Some(child) = self.child.as_mut() else {
                return Err(TestWorldError::Invalid(
                    "managed child is already reaped".to_owned(),
                ));
            };
            if let Some(status) = child.try_wait()? {
                self.status = Some(status);
                self.child = None;
                return Err(TestWorldError::ChildExited(status));
            }
            if Instant::now() >= deadline {
                return Err(TestWorldError::ChildTimedOut {
                    path: path.to_owned(),
                    timeout,
                });
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Kill a running child and wait until the process is reaped.
    ///
    /// # Errors
    ///
    /// Returns an error when no child exists or the host cannot kill or wait for it.
    pub fn terminate(&mut self) -> TestWorldResult<ExitStatus> {
        if let Some(status) = self.status {
            return Ok(status);
        }
        let mut child = self.child.take().ok_or_else(|| {
            TestWorldError::Invalid("managed child has no process to terminate".to_owned())
        })?;
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        let status = child.wait()?;
        self.status = Some(status);
        Ok(status)
    }

    /// Return whether the child has been waited and no process handle remains.
    pub fn is_reaped(&self) -> bool {
        self.child.is_none() && self.status.is_some()
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

/// Stable identity retained while a generated trace is minimized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceIdentity {
    /// Generator seed.
    pub seed: u64,
    /// Original zero-based command indexes retained by this trace.
    pub path: Vec<usize>,
}

/// Stable identity of one generated invariant failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureFingerprint {
    /// Closed failure category owned by the model test.
    pub code: String,
    /// Stable command or lifecycle phase.
    pub phase: String,
    /// Exact violated invariant within that phase.
    pub invariant: String,
}

impl FailureFingerprint {
    /// Construct and validate one stable fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error when any identity field is empty or contains control characters.
    pub fn new(
        code: impl Into<String>,
        phase: impl Into<String>,
        invariant: impl Into<String>,
    ) -> TestWorldResult<Self> {
        let fingerprint = Self {
            code: code.into(),
            phase: phase.into(),
            invariant: invariant.into(),
        };
        for (name, value) in [
            ("code", &fingerprint.code),
            ("phase", &fingerprint.phase),
            ("invariant", &fingerprint.invariant),
        ] {
            if value.is_empty() || value.chars().any(char::is_control) {
                return Err(TestWorldError::Invalid(format!(
                    "failure fingerprint {name} must be non-empty printable text"
                )));
            }
        }
        Ok(fingerprint)
    }
}

/// One language-neutral generated command and fault trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceCase<C> {
    /// Frozen fixture version.
    pub trace_version: String,
    /// Replay identity and minimized command path.
    pub identity: TraceIdentity,
    /// Public commands generated by the Rust-owned model.
    pub commands: Vec<C>,
    /// Faults applied underneath those commands.
    pub faults: FaultPlan,
    /// Exact failure that a minimized regression fixture must reproduce.
    pub expected_failure: Option<FailureFingerprint>,
}

impl<C> TraceCase<C> {
    /// Construct a trace with an initial path covering every command.
    ///
    /// # Errors
    ///
    /// Returns an error when the fault plan is invalid.
    pub fn new(seed: u64, commands: Vec<C>, faults: FaultPlan) -> TestWorldResult<Self> {
        let faults = FaultPlan::new(faults.steps)?;
        Ok(Self {
            trace_version: TRACE_VERSION.to_owned(),
            identity: TraceIdentity {
                seed,
                path: (0..commands.len()).collect(),
            },
            commands,
            faults,
            expected_failure: None,
        })
    }
}

impl<C: Clone> TraceCase<C> {
    /// Delete commands and faults while preserving one exact failure fingerprint.
    #[must_use]
    pub fn minimize_failure(
        &self,
        expected: &FailureFingerprint,
        mut run: impl FnMut(&Self) -> Result<(), FailureFingerprint>,
    ) -> Self {
        let mut minimized = self.clone();
        minimized.expected_failure = Some(expected.clone());
        let mut index = 0;
        while minimized.commands.len() > 1 && index < minimized.commands.len() {
            let mut candidate = minimized.clone();
            candidate.commands.remove(index);
            candidate.identity.path.remove(index);
            if run(&candidate).as_ref().err() == Some(expected) {
                minimized = candidate;
            } else {
                index += 1;
            }
        }
        let mut fault_index = 0;
        while fault_index < minimized.faults.steps.len() {
            let mut candidate = minimized.clone();
            candidate.faults.steps.remove(fault_index);
            if run(&candidate).as_ref().err() == Some(expected) {
                minimized = candidate;
            } else {
                fault_index += 1;
            }
        }
        minimized
    }
}

impl<C: Serialize> TraceCase<C> {
    /// Encode the case as a fixture shared by every SDK.
    ///
    /// # Errors
    ///
    /// Returns an error when a command cannot be represented as JSON.
    pub fn fixture_json(&self) -> TestWorldResult<String> {
        Ok(serde_json::to_string_pretty(self)? + "\n")
    }
}

/// Exact Cargo replay target printed for a failing generated trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplaySpec<'a> {
    /// Cargo package name.
    pub package: &'a str,
    /// Integration-test target.
    pub test_target: &'a str,
    /// Exact Rust test name.
    pub test_name: &'a str,
}

impl ReplaySpec<'_> {
    /// Build a copy-paste replay command for the original failing trace.
    pub fn command(&self, seed: u64) -> String {
        format!(
            "CYMULE_TRACE_SEED={seed} cargo test -p {} --test {} -- --exact {} --nocapture",
            shell_word(self.package),
            shell_word(self.test_target),
            shell_word(self.test_name)
        )
    }
}

/// Human-readable evidence emitted by a generated model failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceFailure {
    /// Original seed.
    pub seed: u64,
    /// Retained original command indexes.
    pub path: Vec<usize>,
    /// Exact replay command for the original generated failure.
    pub replay_command: String,
    /// Minimized fixture ready to check in for every SDK.
    pub minimized_fixture: String,
    /// Exact failure retained by minimization.
    pub fingerprint: FailureFingerprint,
    /// Model or implementation failure.
    pub cause: String,
}

impl fmt::Display for TraceFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "generated trace failed\nseed: {}\npath: {:?}\nreplay: {}\nfingerprint: {}/{}/{}\ncause: {}\nminimized fixture:\n{}",
            self.seed,
            self.path,
            self.replay_command,
            self.fingerprint.code,
            self.fingerprint.phase,
            self.fingerprint.invariant,
            self.cause,
            self.minimized_fixture
        )
    }
}

impl std::error::Error for TraceFailure {}

/// Select either one requested replay seed or a bounded default seed range.
///
/// # Errors
///
/// Returns an error for non-UTF-8, malformed, zero, or unrepresentable case inputs.
pub fn requested_seeds(default_cases: usize) -> TestWorldResult<Vec<u64>> {
    if let Some(seed) = env::var_os("CYMULE_TRACE_SEED") {
        let seed = seed
            .into_string()
            .map_err(|_| TestWorldError::Invalid("CYMULE_TRACE_SEED must be UTF-8".to_owned()))?;
        let seed = seed.parse::<u64>().map_err(|error| {
            TestWorldError::Invalid(format!("CYMULE_TRACE_SEED is invalid: {error}"))
        })?;
        return Ok(vec![seed]);
    }
    let cases = match env::var("PROPTEST_CASES") {
        Ok(value) => value.parse::<usize>().map_err(|error| {
            TestWorldError::Invalid(format!("PROPTEST_CASES is invalid: {error}"))
        })?,
        Err(env::VarError::NotPresent) => default_cases,
        Err(env::VarError::NotUnicode(_)) => {
            return Err(TestWorldError::Invalid(
                "PROPTEST_CASES must be UTF-8".to_owned(),
            ));
        }
    };
    if cases == 0 {
        return Err(TestWorldError::Invalid(
            "generated trace case count must be positive".to_owned(),
        ));
    }
    (0..cases)
        .map(u64::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| TestWorldError::Invalid("generated trace case count is too large".to_owned()))
}

fn shell_word(value: &str) -> String {
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}
