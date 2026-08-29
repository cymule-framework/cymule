//! Hardened process plugin executor for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::mem::size_of;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::ptr::NonNull;
use std::sync::Arc;
#[cfg(all(test, unix))]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

use cymule_core::{canonical_bytes, sha256_bytes, validate_content_id};
use cymule_runtime::{
    MAX_PLUGIN_MESSAGE_BYTES, MAX_PROCESS_ARGUMENTS, MAX_PROCESS_ENVIRONMENT_ENTRIES,
    MAX_PROCESS_RUNTIME_ENTRIES, PluginHost, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult, decode_plugin_response, validate_strict_json,
};
use serde::Serialize;
use tempfile::Builder;

/// Default request/output safety bound.
pub const DEFAULT_PROCESS_MESSAGE_LIMIT: usize = 8 * 1024 * 1024;

/// Default maximum length-prefixed footprint of the complete execution closure.
pub const DEFAULT_PROCESS_CLOSURE_LIMIT: usize = 64 * 1024 * 1024;
const PROCESS_EXECUTION_BINDING_ID_DOMAIN: &str = "cymule.process-execution-binding/2";
const PROCESS_WORKING_DIRECTORY_ID_DOMAIN: &str = "cymule.process-working-directory/2";
const CLOSURE_LENGTH_BYTES: usize = size_of::<u64>();
const CLOSURE_MODE_BYTES: usize = size_of::<u32>();
const CLOSURE_DISCRIMINANT_BYTES: usize = size_of::<u8>();
const MAX_PROCESS_CONFIGURATION_FOOTPRINT: usize = 8 * 1024 * 1024;
const MAX_CAPTURED_DIRECTORY_ENTRIES: usize = 65_536;
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const SEALED_EXECUTABLE_MODE: u32 = 0o500;
const MAX_CONCURRENT_CANCELLATION_LAUNCHES: usize = 64;
const LAUNCH_IDLE: u8 = 0;
const LAUNCH_PENDING: u8 = 1;
const LAUNCH_STARTED: u8 = 2;
const LAUNCH_CANCELLED_BEFORE_START: u8 = 3;
const LAUNCH_CANCELLED_AFTER_START: u8 = 4;
const LAUNCH_EXPIRED_BEFORE_START: u8 = 5;
const LAUNCH_EXPIRED_AFTER_START: u8 = 6;
#[cfg(unix)]
const PARENT_RELATION_POLL_INTERVAL_MS: i32 = 10;
#[cfg(all(test, unix))]
static HANG_PLUGIN_PRE_EXEC: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, unix))]
static BLOCK_BEFORE_LAUNCH_GATE: AtomicBool = AtomicBool::new(false);
#[cfg(all(test, unix))]
static PRE_EXEC_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[cfg(all(test, unix))]
static PRE_EXEC_GROUP_MARKER: OnceLock<PathBuf> = OnceLock::new();
#[cfg(all(test, unix))]
static PRE_EXEC_READY_FD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

#[cfg(any(target_os = "macos", target_os = "ios"))]
const APPLE_DESCRIPTOR_QUERY_SLACK_ENTRIES: usize = 16;

#[cfg(unix)]
struct ChildDescriptorAuthority {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    mapping: NonNull<nix::libc::proc_fdinfo>,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    mapping_bytes: usize,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    buffer_bytes: i32,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    descriptor_domain_exclusive: i32,
    #[cfg(all(
        not(target_os = "linux"),
        not(target_os = "macos"),
        not(target_os = "ios")
    ))]
    descriptor_limit: i32,
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct ChildDescriptorAuthorityView {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    buffer_address: usize,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    buffer_bytes: i32,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    descriptor_domain_exclusive: i32,
    #[cfg(all(
        not(target_os = "linux"),
        not(target_os = "macos"),
        not(target_os = "ios")
    ))]
    descriptor_limit: i32,
}

#[cfg(unix)]
impl std::fmt::Debug for ChildDescriptorAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("ChildDescriptorAuthority");
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        debug
            .field("mapping", &self.mapping)
            .field("mapping_bytes", &self.mapping_bytes)
            .field("buffer_bytes", &self.buffer_bytes)
            .field(
                "descriptor_domain_exclusive",
                &self.descriptor_domain_exclusive,
            );
        #[cfg(all(
            not(target_os = "linux"),
            not(target_os = "macos"),
            not(target_os = "ios")
        ))]
        debug.field("descriptor_limit", &self.descriptor_limit);
        debug.finish()
    }
}

#[cfg(unix)]
impl Drop for ChildDescriptorAuthority {
    fn drop(&mut self) {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        {
            // SAFETY: this is the parent-side owner of the private mapping
            // created by `prepare_apple_descriptor_authority`. Forked children
            // either exec or _exit and never run this destructor.
            unsafe {
                let _ = nix::libc::munmap(self.mapping.as_ptr().cast(), self.mapping_bytes);
            }
        }
    }
}

#[cfg(unix)]
impl ChildDescriptorAuthority {
    fn prepare() -> RuntimeResult<Self> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self {})
        }
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        {
            prepare_apple_descriptor_authority()
        }
        #[cfg(all(
            not(target_os = "linux"),
            not(target_os = "macos"),
            not(target_os = "ios")
        ))]
        {
            Ok(Self {
                descriptor_limit: parent_descriptor_limit()?,
            })
        }
    }

    fn view(&mut self) -> ChildDescriptorAuthorityView {
        ChildDescriptorAuthorityView {
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            buffer_address: self.mapping.as_ptr().expose_provenance(),
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            buffer_bytes: self.buffer_bytes,
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            descriptor_domain_exclusive: self.descriptor_domain_exclusive,
            #[cfg(all(
                not(target_os = "linux"),
                not(target_os = "macos"),
                not(target_os = "ios")
            ))]
            descriptor_limit: self.descriptor_limit,
        }
    }
}

#[cfg(unix)]
#[repr(C)]
struct SharedCancellationState {
    cancelled: AtomicBool,
    launches: [AtomicU8; MAX_CONCURRENT_CANCELLATION_LAUNCHES],
}

#[cfg(unix)]
struct ProcessCancellationInner {
    shared: NonNull<SharedCancellationState>,
}

#[cfg(unix)]
// SAFETY: `shared` points to one process-shared anonymous mapping containing
// only lock-free atomic scalars. The mapping remains live until the final Arc
// owner is dropped after every registered launch has returned.
unsafe impl Send for ProcessCancellationInner {}

#[cfg(unix)]
// SAFETY: all accesses to the shared mapping use atomic operations.
unsafe impl Sync for ProcessCancellationInner {}

#[cfg(unix)]
impl Drop for ProcessCancellationInner {
    fn drop(&mut self) {
        let length = size_of::<SharedCancellationState>();
        // SAFETY: this is the unique final owner of the mapping created by
        // `ProcessCancellation::new`; no launch registration retains it.
        unsafe {
            std::ptr::drop_in_place(self.shared.as_ptr());
            let _ = nix::libc::munmap(self.shared.as_ptr().cast(), length);
        }
    }
}

/// Process-local owner-cancellation source shared with forked launch gates.
///
/// Cancellation is terminal. Each invocation registers one bounded launch
/// slot before fork; `cancel` and the child launch gate race on that exact
/// process-shared atomic receipt, so a completed pre-start cancellation cannot
/// be followed by provider execution.
#[derive(Clone)]
pub struct ProcessCancellation {
    #[cfg(unix)]
    inner: Arc<ProcessCancellationInner>,
}

impl std::fmt::Debug for ProcessCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl ProcessCancellation {
    /// Create one terminal cancellation source.
    ///
    /// # Errors
    ///
    /// Returns a substrate error when the process-shared launch authority
    /// cannot be allocated.
    pub fn new() -> RuntimeResult<Self> {
        #[cfg(unix)]
        {
            let length = size_of::<SharedCancellationState>();
            // SAFETY: the returned private address is initialized exactly once
            // before publication. `MAP_SHARED | MAP_ANON` makes later atomic
            // transitions visible to the parent, watchdog, and pre-exec child.
            let mapping = unsafe {
                nix::libc::mmap(
                    std::ptr::null_mut(),
                    length,
                    nix::libc::PROT_READ | nix::libc::PROT_WRITE,
                    nix::libc::MAP_SHARED | nix::libc::MAP_ANON,
                    -1,
                    0,
                )
            };
            if mapping == nix::libc::MAP_FAILED {
                return Err(RuntimeError::substrate(
                    "process_launch_authority_failed",
                    "process-shared cancellation authority could not be allocated",
                ));
            }
            let shared =
                NonNull::new(mapping.cast::<SharedCancellationState>()).ok_or_else(|| {
                    RuntimeError::substrate(
                        "process_launch_authority_failed",
                        "process-shared cancellation authority was invalid",
                    )
                })?;
            // SAFETY: `shared` addresses the writable mapping above and is not
            // yet observable by another thread or process.
            unsafe {
                shared.as_ptr().write(SharedCancellationState {
                    cancelled: AtomicBool::new(false),
                    launches: [const { AtomicU8::new(LAUNCH_IDLE) };
                        MAX_CONCURRENT_CANCELLATION_LAUNCHES],
                });
            }
            Ok(Self {
                inner: Arc::new(ProcessCancellationInner { shared }),
            })
        }
        #[cfg(not(unix))]
        Err(RuntimeError::plugin_defect(
            "process cancellation requires Unix shared launch authority",
        ))
    }

    /// Cancel every current and future invocation using this source.
    pub fn cancel(&self) {
        #[cfg(unix)]
        {
            let shared = self.shared();
            shared.cancelled.store(true, Ordering::SeqCst);
            for launch in &shared.launches {
                transition_launch_cancellation(launch);
            }
        }
    }

    /// Register one Unix signal as terminal cancellation for this source.
    ///
    /// The installed handler retains a clone for the process lifetime and
    /// performs only fixed-count lock-free atomic transitions.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the process signal handler cannot be installed.
    #[cfg(unix)]
    pub fn register_signal(&self, signal: i32) -> std::io::Result<()> {
        let handler = self.clone();
        // SAFETY: the callback invokes no allocator, lock, destructor, or I/O;
        // it only stores/CASes a fixed shared atomic array.
        unsafe {
            signal_hook::low_level::register(signal, move || handler.cancel())?;
        }
        Ok(())
    }

    /// Whether terminal cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        #[cfg(unix)]
        {
            self.shared().cancelled.load(Ordering::SeqCst)
        }
        #[cfg(not(unix))]
        true
    }

    #[cfg(unix)]
    fn shared(&self) -> &SharedCancellationState {
        // SAFETY: the Arc owns this initialized mapping for the returned borrow.
        unsafe { self.inner.shared.as_ref() }
    }

    #[cfg(unix)]
    fn register_launch(&self) -> RuntimeResult<LaunchAuthority> {
        if self.is_cancelled() {
            return Err(invocation_cancelled(false));
        }
        for (index, launch) in self.shared().launches.iter().enumerate() {
            if launch
                .compare_exchange(
                    LAUNCH_IDLE,
                    LAUNCH_PENDING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                if self.is_cancelled() {
                    transition_launch_cancellation(launch);
                }
                return Ok(LaunchAuthority {
                    cancellation: self.clone(),
                    index,
                });
            }
        }
        Err(RuntimeError::substrate(
            "process_launch_authority_exhausted",
            "process cancellation source has too many concurrent launches",
        ))
    }

    #[cfg(not(unix))]
    fn register_launch(&self) -> RuntimeResult<LaunchAuthority> {
        Err(RuntimeError::plugin_defect(
            "process launch authority requires Unix shared memory",
        ))
    }
}

#[cfg(unix)]
fn transition_launch_cancellation(launch: &AtomicU8) {
    loop {
        let observed = launch.load(Ordering::SeqCst);
        let target = match observed {
            LAUNCH_PENDING => LAUNCH_CANCELLED_BEFORE_START,
            LAUNCH_STARTED => LAUNCH_CANCELLED_AFTER_START,
            _ => return,
        };
        if launch
            .compare_exchange(observed, target, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return;
        }
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct LaunchAuthority {
    cancellation: ProcessCancellation,
    index: usize,
}

#[cfg(unix)]
impl LaunchAuthority {
    fn state(&self) -> &AtomicU8 {
        &self.cancellation.shared().launches[self.index]
    }

    fn state_ptr(&self) -> *const AtomicU8 {
        std::ptr::from_ref(self.state())
    }

    fn status(&self) -> u8 {
        self.state().load(Ordering::SeqCst)
    }

    fn expire(&self) {
        transition_launch_expiration(self.state());
    }

    fn check_pre_start(&self, deadline: Instant) -> RuntimeResult<()> {
        if Instant::now() >= deadline {
            self.expire();
        }
        match self.status() {
            LAUNCH_PENDING => Ok(()),
            LAUNCH_CANCELLED_BEFORE_START => Err(invocation_cancelled(false)),
            LAUNCH_EXPIRED_BEFORE_START => Err(invocation_timeout(false)),
            LAUNCH_STARTED | LAUNCH_CANCELLED_AFTER_START | LAUNCH_EXPIRED_AFTER_START => {
                Err(RuntimeError::substrate(
                    "process_launch_authority_invalid",
                    "process launch committed before the child launch gate",
                ))
            }
            _ => Err(RuntimeError::substrate(
                "process_launch_authority_invalid",
                "process launch authority entered an invalid state",
            )),
        }
    }

    fn check_running(&self, deadline: Instant, ambiguous_world_effect: bool) -> RuntimeResult<()> {
        if Instant::now() >= deadline {
            self.expire();
        }
        match self.status() {
            LAUNCH_STARTED => Ok(()),
            LAUNCH_CANCELLED_BEFORE_START => Err(invocation_cancelled(false)),
            LAUNCH_EXPIRED_BEFORE_START => Err(invocation_timeout(false)),
            LAUNCH_CANCELLED_AFTER_START => Err(invocation_cancelled(ambiguous_world_effect)),
            LAUNCH_EXPIRED_AFTER_START => Err(invocation_timeout(ambiguous_world_effect)),
            _ => Err(process_failure(
                ambiguous_world_effect,
                "process_launch_authority_invalid",
                "process launch authority did not retain a terminal start decision",
            )),
        }
    }

    fn classify_spawn_failure(&self, ambiguous_world_effect: bool) -> RuntimeError {
        match self.status() {
            LAUNCH_CANCELLED_BEFORE_START => invocation_cancelled(false),
            LAUNCH_EXPIRED_BEFORE_START => invocation_timeout(false),
            LAUNCH_STARTED | LAUNCH_CANCELLED_AFTER_START | LAUNCH_EXPIRED_AFTER_START => {
                process_failure(
                    ambiguous_world_effect,
                    "process_start_outcome_unknown",
                    "process launch committed but exec completion was not observed",
                )
            }
            _ => RuntimeError::substrate(
                "process_start_failed",
                "sealed process plugin could not be started",
            ),
        }
    }
}

#[cfg(unix)]
impl Drop for LaunchAuthority {
    fn drop(&mut self) {
        self.state().store(LAUNCH_IDLE, Ordering::SeqCst);
    }
}

#[cfg(unix)]
fn transition_launch_expiration(launch: &AtomicU8) {
    loop {
        let observed = launch.load(Ordering::SeqCst);
        let target = match observed {
            LAUNCH_PENDING => LAUNCH_EXPIRED_BEFORE_START,
            LAUNCH_STARTED => LAUNCH_EXPIRED_AFTER_START,
            _ => return,
        };
        if launch
            .compare_exchange(observed, target, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return;
        }
    }
}

#[cfg(not(unix))]
#[derive(Debug)]
struct LaunchAuthority;

#[cfg(not(unix))]
impl LaunchAuthority {
    const fn state_ptr(&self) -> *const AtomicU8 {
        std::ptr::null()
    }

    fn check_pre_start(&self, _deadline: Instant) -> RuntimeResult<()> {
        Err(RuntimeError::plugin_defect(
            "process launch authority requires Unix shared memory",
        ))
    }

    fn check_running(
        &self,
        _deadline: Instant,
        _ambiguous_world_effect: bool,
    ) -> RuntimeResult<()> {
        self.check_pre_start(Instant::now())
    }

    fn classify_spawn_failure(&self, _ambiguous_world_effect: bool) -> RuntimeError {
        RuntimeError::plugin_defect("process launch authority requires Unix shared memory")
    }
}

/// Explicit process execution policy.
#[derive(Debug, Clone)]
pub struct ProcessExecutorConfig {
    /// Executable whose current bytes are sealed during construction.
    pub executable: PathBuf,
    /// Exact argument vector.
    pub arguments: Vec<String>,
    /// Optional working-directory tree captured during construction.
    ///
    /// Each invocation receives a fresh private materialization of this tree;
    /// the mutable source path is provenance only after construction.
    pub working_directory: Option<PathBuf>,
    /// Complete allowed child environment after ambient clearing.
    pub environment: BTreeMap<String, String>,
    /// Provider-owned immutable revisions for runtime facilities outside the
    /// captured tree.
    ///
    /// Keys name facilities such as a declared runtime generation or a
    /// separately admitted loader; values are lowercase SHA-256 identities of
    /// frozen closure descriptors. A host OS and architecture label is
    /// compatibility metadata, not a runtime revision. The complete sorted map
    /// is part of the canonical execution-binding identity.
    pub runtime_closure: BTreeMap<String, String>,
    /// Maximum time from private materialization through child start, response
    /// I/O, process-group termination, and child/watchdog reaping.
    ///
    /// Host filesystem reclamation begins only after process authority has
    /// ended and is reported separately from this provider deadline.
    pub timeout: Duration,
    /// Maximum encoded request, stdout, or stderr bytes.
    pub message_limit: usize,
    /// Maximum length-prefixed footprint of all admitted execution inputs.
    ///
    /// This includes arguments, environment, runtime revisions, policy fields,
    /// executable bytes, every working-tree path/type/mode, and file bytes.
    pub closure_limit: usize,
    /// Process-local cancellation authority installed by the owning Engine.
    ///
    /// The token is deliberately excluded from the immutable execution-binding
    /// identity: it controls one host process lifetime, not program meaning.
    pub cancellation: Option<ProcessCancellation>,
}

impl ProcessExecutorConfig {
    /// Construct a policy around one executable and an explicit immutable
    /// runtime binding.
    pub fn new(executable: impl AsRef<Path>, runtime_closure: BTreeMap<String, String>) -> Self {
        Self {
            executable: executable.as_ref().to_path_buf(),
            arguments: Vec::new(),
            working_directory: None,
            environment: BTreeMap::new(),
            runtime_closure,
            timeout: Duration::from_mins(1),
            message_limit: DEFAULT_PROCESS_MESSAGE_LIMIT,
            closure_limit: DEFAULT_PROCESS_CLOSURE_LIMIT,
            cancellation: None,
        }
    }

    fn validate(&self) -> RuntimeResult<()> {
        if !self.executable.is_absolute()
            || self.timeout.is_zero()
            || self.message_limit == 0
            || self.message_limit > 64 * 1024 * 1024
            || self.closure_limit == 0
            || self.closure_limit > 1024 * 1024 * 1024
            || self.arguments.len() > MAX_PROCESS_ARGUMENTS
            || self.environment.len() > MAX_PROCESS_ENVIRONMENT_ENTRIES
            || self.runtime_closure.len() > MAX_PROCESS_RUNTIME_ENTRIES
            || self.runtime_closure.is_empty()
            || self
                .environment
                .iter()
                .any(|(key, value)| invalid_key(key) || value.contains('\0'))
            || self.runtime_closure.iter().any(|(key, value)| {
                invalid_key(key) || validate_content_id("process runtime revision", value).is_err()
            })
            || self.arguments.iter().any(|value| value.contains('\0'))
        {
            return Err(RuntimeError::plugin_defect(
                "process executor configuration is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ClosureBudget {
    remaining: usize,
    directory_entries: usize,
}

impl ClosureBudget {
    const fn new(limit: usize) -> Self {
        Self {
            remaining: limit,
            directory_entries: 0,
        }
    }

    fn charge(&mut self, bytes: usize) -> RuntimeResult<()> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(|| {
            RuntimeError::plugin_defect(
                "process execution closure exceeds the configured closure limit",
            )
        })?;
        Ok(())
    }

    fn charge_framed(&mut self, bytes: usize) -> RuntimeResult<()> {
        self.charge(CLOSURE_LENGTH_BYTES)?;
        self.charge(bytes)
    }

    fn maximum_blob_bytes(&self) -> RuntimeResult<usize> {
        self.remaining
            .checked_sub(CLOSURE_LENGTH_BYTES)
            .ok_or_else(|| {
                RuntimeError::plugin_defect(
                    "process execution closure exceeds the configured closure limit",
                )
            })
    }

    fn count_directory_entry(&mut self) -> RuntimeResult<()> {
        self.directory_entries = self.directory_entries.checked_add(1).ok_or_else(|| {
            RuntimeError::plugin_defect("process working directory entry count overflowed")
        })?;
        if self.directory_entries > MAX_CAPTURED_DIRECTORY_ENTRIES {
            return Err(RuntimeError::plugin_defect(
                "process working directory exceeds the configured entry limit",
            ));
        }
        Ok(())
    }

    fn charge_directory_entry_encoding(&mut self) -> RuntimeResult<()> {
        self.charge(CLOSURE_DISCRIMINANT_BYTES)
    }
}

fn charge_configuration(
    config: &ProcessExecutorConfig,
    budget: &mut ClosureBudget,
) -> RuntimeResult<()> {
    let initial_remaining = budget.remaining;
    budget.charge_framed(PROCESS_EXECUTION_BINDING_ID_DOMAIN.len())?;
    budget.charge(CLOSURE_LENGTH_BYTES)?;
    for argument in &config.arguments {
        budget.charge_framed(argument.len())?;
    }
    budget.charge(CLOSURE_LENGTH_BYTES)?;
    for (key, value) in &config.environment {
        budget.charge_framed(key.len())?;
        budget.charge_framed(value.len())?;
    }
    budget.charge(CLOSURE_LENGTH_BYTES)?;
    for (key, value) in &config.runtime_closure {
        budget.charge_framed(key.len())?;
        budget.charge_framed(value.len())?;
    }
    budget.charge(size_of::<u64>() + size_of::<u32>())?;
    budget.charge(size_of::<u64>() * 2)?;
    budget.charge(CLOSURE_DISCRIMINANT_BYTES)?;
    budget.charge(CLOSURE_MODE_BYTES * 2)?;
    let footprint = initial_remaining
        .checked_sub(budget.remaining)
        .ok_or_else(|| RuntimeError::plugin_defect("process configuration footprint overflowed"))?;
    if footprint > MAX_PROCESS_CONFIGURATION_FOOTPRINT {
        return Err(RuntimeError::plugin_defect(
            "process configuration exceeds the fixed encoded byte limit",
        ));
    }
    Ok(())
}

fn invalid_key(value: &str) -> bool {
    value.is_empty() || value.contains('=') || value.chars().any(char::is_control)
}

#[derive(Debug)]
struct CapturedFile {
    relative_path: String,
    bytes: Vec<u8>,
    mode: u32,
}

#[derive(Debug, Serialize)]
struct CapturedFileIdentity<'a> {
    path: &'a str,
    digest: String,
    mode: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct CapturedDirectoryEntry {
    relative_path: String,
    mode: u32,
}

#[derive(Debug, Serialize)]
struct CapturedDirectoryEntryIdentity<'a> {
    path: &'a str,
    mode: u32,
}

#[derive(Debug)]
struct CapturedDirectory {
    root_mode: u32,
    directories: Vec<CapturedDirectoryEntry>,
    files: Vec<CapturedFile>,
    identity: String,
}

#[derive(Serialize)]
struct CapturedDirectoryIdentity<'a> {
    version: &'static str,
    root_mode: u32,
    directory_mode: u32,
    directories: Vec<CapturedDirectoryEntryIdentity<'a>>,
    files: Vec<CapturedFileIdentity<'a>>,
}

#[derive(Serialize)]
struct ProcessBindingIdentity<'a> {
    version: &'static str,
    executable_digest: &'a str,
    arguments: &'a [String],
    environment: &'a BTreeMap<String, String>,
    working_directory: Option<&'a str>,
    runtime_closure: &'a BTreeMap<String, String>,
    timeout_seconds: u64,
    timeout_nanoseconds: u32,
    message_limit: usize,
    closure_limit: usize,
    occurrence_root_mode: u32,
    sealed_executable_mode: u32,
}

/// One-occurrence process plugin transport.
///
/// Construction captures executable bytes and the optional working-directory
/// tree. Every invocation materializes fresh private files from that immutable
/// in-memory authority, so a same-UID plugin may alter only its disposable
/// occurrence and cannot replace bytes used by a later invocation.
#[derive(Debug)]
pub struct ProcessExecutor {
    config: ProcessExecutorConfig,
    executable_bytes: Vec<u8>,
    executable_revision: String,
    working_directory: Option<CapturedDirectory>,
    implementation_revision: String,
}

impl ProcessExecutor {
    /// Validate the policy, capture its executable closure, and construct an executor.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, capture, or substrate error when the
    /// executable closure cannot be admitted exactly.
    pub fn new(config: ProcessExecutorConfig) -> RuntimeResult<Self> {
        #[cfg(not(unix))]
        ensure_supported_platform()?;
        config.validate()?;
        let mut closure_budget = ClosureBudget::new(config.closure_limit);
        charge_configuration(&config, &mut closure_budget)?;
        let bytes = capture_executable(&config.executable, closure_budget.maximum_blob_bytes()?)?;
        if bytes.is_empty() {
            return Err(RuntimeError::plugin_defect(
                "selected process plugin executable is empty",
            ));
        }
        closure_budget.charge_framed(bytes.len())?;
        let executable_revision = format!("sha256:{}", sha256_bytes(&bytes));
        let working_directory = config
            .working_directory
            .as_deref()
            .map(|path| capture_directory(path, &mut closure_budget))
            .transpose()?;
        let identity = ProcessBindingIdentity {
            version: PROCESS_EXECUTION_BINDING_ID_DOMAIN,
            executable_digest: &executable_revision,
            arguments: &config.arguments,
            environment: &config.environment,
            working_directory: working_directory
                .as_ref()
                .map(|tree| tree.identity.as_str()),
            runtime_closure: &config.runtime_closure,
            timeout_seconds: config.timeout.as_secs(),
            timeout_nanoseconds: config.timeout.subsec_nanos(),
            message_limit: config.message_limit,
            closure_limit: config.closure_limit,
            occurrence_root_mode: PRIVATE_DIRECTORY_MODE,
            sealed_executable_mode: SEALED_EXECUTABLE_MODE,
        };
        let implementation_revision = format!(
            "sha256:{}",
            sha256_bytes(&canonical_bytes(&identity).map_err(|_| {
                RuntimeError::plugin_defect("process execution binding could not be canonicalized")
            })?)
        );
        Ok(Self {
            config,
            executable_bytes: bytes,
            executable_revision,
            working_directory,
            implementation_revision,
        })
    }

    /// Read-only execution policy. Its source paths are provenance only.
    pub const fn config(&self) -> &ProcessExecutorConfig {
        &self.config
    }

    /// Digest of the exact executable bytes captured at construction.
    pub fn executable_revision(&self) -> &str {
        &self.executable_revision
    }

    /// Canonical identity of executable bytes, arguments, environment, working tree, and runtime closure.
    pub fn implementation_revision(&self) -> &str {
        &self.implementation_revision
    }

    /// Invoke one evolution-protocol request through its fixed raw-byte bound.
    ///
    /// Evolution owns strict encoding and decoding. This method transports only
    /// already-validated bytes and never falls back to the generic plugin decoder.
    ///
    /// # Errors
    ///
    /// Returns a typed pre-spawn request error or process transport error. The
    /// executor configuration must carry the exact evolution protocol limit.
    pub fn invoke_evolution_bytes(&self, input: &[u8]) -> RuntimeResult<Vec<u8>> {
        let message_limit = cymule_runtime::EVOLUTION_PLUGIN_MESSAGE_LIMIT;
        if self.config.message_limit != message_limit {
            return Err(RuntimeError::PluginDefect {
                code: "evolution_process_message_limit_mismatch".to_owned(),
                message:
                    "process executor message limit does not equal the evolution protocol bound"
                        .to_owned(),
            });
        }
        if input.len() > message_limit {
            return Err(RuntimeError::PluginDefect {
                code: "evolution_process_request_too_large".to_owned(),
                message: format!(
                    "evolution process request uses {} raw bytes, above the {message_limit} byte bound",
                    input.len()
                ),
            });
        }
        validate_outbound_json(input, "sealed process request")?;
        self.invoke_bytes(input, false, message_limit)
    }

    fn invoke_process(&self, request: &PluginRequest) -> RuntimeResult<PluginResponse> {
        if self.config.message_limit != MAX_PLUGIN_MESSAGE_BYTES {
            return Err(RuntimeError::PluginDefect {
                code: "plugin_process_message_limit_mismatch".to_owned(),
                message: format!(
                    "process executor message limit must equal the plugin protocol's {MAX_PLUGIN_MESSAGE_BYTES} byte bound"
                ),
            });
        }
        request.verify()?;
        let input = serde_json::to_vec(request)?;
        validate_outbound_json(&input, "process plugin request")?;
        let output = self.invoke_bytes(
            &input,
            is_world_mutating_effect(request),
            MAX_PLUGIN_MESSAGE_BYTES,
        )?;
        let response = decode_plugin_response(&output).map_err(|error| match error {
            RuntimeError::PluginDefect { code, message } => {
                post_start_failure(request, &code, &message)
            }
            error => post_start_failure(request, "invalid_plugin_response", &error.to_string()),
        })?;
        response
            .verify_for(request)
            .map_err(|error| response_validation_failure(request, error))?;
        Ok(response)
    }

    fn invoke_bytes(
        &self,
        input: &[u8],
        ambiguous_world_effect: bool,
        message_limit: usize,
    ) -> RuntimeResult<Vec<u8>> {
        let deadline = Instant::now()
            .checked_add(self.config.timeout)
            .ok_or_else(|| {
                RuntimeError::plugin_defect("process executor timeout exceeds the clock range")
            })?;
        if input.len() > message_limit {
            return Err(RuntimeError::plugin_defect(
                "process plugin request exceeds the configured byte limit",
            ));
        }
        let invocation = self.materialize_invocation(deadline)?;
        let outcome = self.invoke_materialized(
            &invocation,
            input,
            deadline,
            ambiguous_world_effect,
            message_limit,
        );
        finish_invocation(invocation, outcome)
    }

    fn invoke_materialized(
        &self,
        invocation: &InvocationFiles,
        input: &[u8],
        deadline: Instant,
        ambiguous_world_effect: bool,
        message_limit: usize,
    ) -> RuntimeResult<Vec<u8>> {
        let mut command = self.prepare_command(invocation);
        check_materialization_authority(deadline, self.config.cancellation.as_ref())?;
        let cancellation = self
            .config
            .cancellation
            .clone()
            .map_or_else(ProcessCancellation::new, Ok)?;
        let launch = cancellation.register_launch()?;
        let mut supervisor = ProcessGroupSupervisor::start(deadline, &launch)?;
        #[cfg(test)]
        if let Some(marker) = PRE_EXEC_GROUP_MARKER.get() {
            let process_group = supervisor.process_group().to_string();
            let marker = if marker.is_dir() {
                marker.join(&process_group)
            } else {
                marker.clone()
            };
            fs::write(marker, process_group).map_err(|_| {
                RuntimeError::substrate(
                    "process_supervisor_start_failed",
                    "process pre-exec test group could not be published",
                )
            })?;
        }
        let descriptor_authority = supervisor.descriptor_authority_view();
        if let Err(error) = configure_process_boundary(
            &mut command,
            supervisor.process_group(),
            supervisor.execution_deadline(),
            supervisor.engine_liveness_fd(),
            descriptor_authority,
            launch.state_ptr(),
        ) {
            supervisor.terminate();
            return Err(error);
        }
        launch.check_pre_start(deadline)?;
        let mut process = Self::spawn_process(command, supervisor, launch, ambiguous_world_effect)?;
        if let Err(error) = process
            .launch
            .check_running(deadline, ambiguous_world_effect)
        {
            process.terminate();
            return Err(error);
        }
        exchange_process(
            process,
            input,
            message_limit,
            deadline,
            ambiguous_world_effect,
        )
    }

    fn prepare_command(&self, invocation: &InvocationFiles) -> Command {
        let mut command = Command::new(&invocation.executable);
        command
            .args(&self.config.arguments)
            .env_clear()
            .envs(&self.config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // A null working-directory configuration is still explicit: it selects
        // the fresh private occurrence root and never inherits the Engine's
        // ambient cwd. A configured captured tree replaces that root.
        command.current_dir(
            invocation
                .working_directory
                .as_deref()
                .unwrap_or_else(|| invocation.directory.path()),
        );
        command
    }

    fn spawn_process(
        mut command: Command,
        mut supervisor: ProcessGroupSupervisor,
        launch: LaunchAuthority,
        ambiguous_world_effect: bool,
    ) -> RuntimeResult<RunningProcess> {
        let Ok(mut child) = command.spawn() else {
            let error = launch.classify_spawn_failure(ambiguous_world_effect);
            supervisor.terminate();
            return Err(error);
        };
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            terminate_process_tree(&mut child, &mut supervisor);
            return Err(process_failure(
                ambiguous_world_effect,
                "process_pipe_unavailable",
                "one or more plugin process pipes were unavailable",
            ));
        };
        if set_nonblocking(&stdin).is_err()
            || set_nonblocking(&stdout).is_err()
            || set_nonblocking(&stderr).is_err()
        {
            terminate_process_tree(&mut child, &mut supervisor);
            return Err(process_failure(
                ambiguous_world_effect,
                "process_pipe_configuration_failed",
                "plugin process pipes could not be configured for bounded I/O",
            ));
        }
        Ok(RunningProcess::new(
            child, supervisor, launch, stdin, stdout, stderr,
        ))
    }

    fn materialize_invocation(&self, deadline: Instant) -> RuntimeResult<InvocationFiles> {
        let cancellation = self.config.cancellation.as_ref();
        check_materialization_authority(deadline, cancellation)?;
        let directory = PrivateInvocationDirectory::create()?;
        check_materialization_authority(deadline, cancellation)?;
        let executable = directory.path().join("plugin");
        write_new_file_at(
            directory.root(),
            std::ffi::OsStr::new("plugin"),
            &self.executable_bytes,
            SEALED_EXECUTABLE_MODE,
            deadline,
            cancellation,
        )?;
        let working_directory = self
            .working_directory
            .as_ref()
            .map(|snapshot| materialize_directory(&directory, snapshot, deadline, cancellation))
            .transpose()?;
        check_materialization_authority(deadline, cancellation)?;
        Ok(InvocationFiles {
            directory,
            executable,
            working_directory,
        })
    }
}

fn capture_executable(path: &Path, limit: usize) -> RuntimeResult<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(nix::libc::O_NONBLOCK | nix::libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|error| {
        if matches!(
            error.kind(),
            ErrorKind::NotFound | ErrorKind::PermissionDenied
        ) {
            RuntimeError::substrate(
                "process_executable_read_failed",
                "selected process plugin executable could not be read",
            )
        } else {
            RuntimeError::plugin_defect(
                "selected process plugin executable must be an accessible regular file",
            )
        }
    })?;
    let metadata = file.metadata().map_err(|_| {
        RuntimeError::plugin_defect("selected process plugin executable metadata could not be read")
    })?;
    let limit_u64 = u64::try_from(limit)
        .map_err(|_| RuntimeError::plugin_defect("process closure limit is invalid"))?;
    if !metadata.is_file() || metadata.len() > limit_u64 {
        return Err(RuntimeError::plugin_defect(
            "selected process plugin executable must be a bounded regular file",
        ));
    }
    let read_limit = limit_u64
        .checked_add(1)
        .ok_or_else(|| RuntimeError::plugin_defect("process closure limit is invalid"))?;
    let mut bytes = Vec::new();
    file.take(read_limit).read_to_end(&mut bytes).map_err(|_| {
        RuntimeError::plugin_defect("selected process plugin executable could not be captured")
    })?;
    if bytes.len() > limit {
        return Err(RuntimeError::plugin_defect(
            "selected process plugin executable exceeds the configured closure limit",
        ));
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn ensure_supported_platform() -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires Unix process-group and permission semantics",
    ))
}

impl PluginHost for ProcessExecutor {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        self.config.validate()?;
        self.invoke_process(&request)
    }
}

#[derive(Debug)]
struct InvocationFiles {
    directory: PrivateInvocationDirectory,
    executable: PathBuf,
    working_directory: Option<PathBuf>,
}

#[derive(Debug)]
struct PrivateInvocationDirectory {
    path: Option<PathBuf>,
    #[cfg(unix)]
    parent: Option<nix::dir::Dir>,
    #[cfg(unix)]
    root: Option<nix::dir::Dir>,
    #[cfg(unix)]
    name: std::ffi::CString,
    #[cfg(unix)]
    identity: PrivateDirectoryIdentity,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrivateDirectoryIdentity {
    device: nix::libc::dev_t,
    inode: nix::libc::ino_t,
}

impl PrivateInvocationDirectory {
    #[allow(clippy::too_many_lines)]
    fn create() -> RuntimeResult<Self> {
        #[cfg(unix)]
        use std::os::unix::ffi::OsStrExt;
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;

        let mut builder = Builder::new();
        builder.prefix("cymule-executor-");
        #[cfg(unix)]
        builder.permissions(fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE));
        let temporary = builder.tempdir().map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process invocation directory could not be created",
            )
        })?;
        #[cfg(unix)]
        {
            use nix::fcntl::AtFlags;
            use nix::sys::stat::{FchmodatFlags, SFlag, fchmodat, fstat, fstatat};

            let raw_path = temporary.path();
            let raw_parent = raw_path.parent().ok_or_else(|| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory has no parent",
                )
            })?;
            let resolved_parent = fs::canonicalize(raw_parent).map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation parent could not be resolved",
                )
            })?;
            let name_os = raw_path.file_name().ok_or_else(|| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory has no name",
                )
            })?;
            let name = std::ffi::CString::new(name_os.as_bytes()).map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory name is invalid",
                )
            })?;
            let parent = open_absolute_directory(&resolved_parent)?;
            fchmodat(
                &parent,
                name.as_c_str(),
                normalized_mode(PRIVATE_DIRECTORY_MODE)?,
                FchmodatFlags::NoFollowSymlink,
            )
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory mode could not be fixed",
                )
            })?;
            let root = nix::dir::Dir::openat(
                &parent,
                name.as_c_str(),
                private_directory_open_flags(),
                nix::sys::stat::Mode::empty(),
            )
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory could not be opened",
                )
            })?;
            let root_stat = fstat(&root).map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation identity could not be read",
                )
            })?;
            let named_stat = fstatat(&parent, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)
                .map_err(|_| {
                    RuntimeError::substrate(
                        "process_seal_failed",
                        "private process invocation name could not be authenticated",
                    )
                })?;
            if SFlag::from_bits_truncate(root_stat.st_mode) != SFlag::S_IFDIR
                || root_stat.st_dev != named_stat.st_dev
                || root_stat.st_ino != named_stat.st_ino
                || u32::from(root_stat.st_mode & 0o7777) != PRIVATE_DIRECTORY_MODE
            {
                return Err(RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory identity is invalid",
                ));
            }
            let path = resolved_parent.join(name_os);
            let _ = temporary.keep();
            Ok(Self {
                path: Some(path),
                parent: Some(parent),
                root: Some(root),
                name,
                identity: PrivateDirectoryIdentity {
                    device: root_stat.st_dev,
                    inode: root_stat.st_ino,
                },
            })
        }
        #[cfg(not(unix))]
        {
            drop(temporary);
            Err(RuntimeError::plugin_defect(
                "private process invocation requires Unix descriptor semantics",
            ))
        }
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("private invocation directory remains owned")
    }

    #[cfg(unix)]
    fn root(&self) -> &nix::dir::Dir {
        self.root
            .as_ref()
            .expect("private invocation root remains owned")
    }

    fn close(mut self) -> std::io::Result<()> {
        self.path.take();
        #[cfg(unix)]
        {
            let parent = self
                .parent
                .take()
                .expect("private invocation parent closes once");
            let root = self
                .root
                .take()
                .expect("private invocation root closes once");
            reclaim_private_directory(&parent, root, &self.name, self.identity)
        }
        #[cfg(not(unix))]
        Err(std::io::Error::new(
            ErrorKind::Unsupported,
            "Unix descriptor-relative cleanup is required",
        ))
    }
}

impl Drop for PrivateInvocationDirectory {
    fn drop(&mut self) {
        if self.path.take().is_some() {
            #[cfg(unix)]
            if let (Some(parent), Some(root)) = (self.parent.take(), self.root.take()) {
                let _ = reclaim_private_directory(&parent, root, &self.name, self.identity);
            }
        }
    }
}

fn finish_invocation(
    invocation: InvocationFiles,
    outcome: RuntimeResult<Vec<u8>>,
) -> RuntimeResult<Vec<u8>> {
    let cleanup = invocation.directory.close();
    match (outcome, cleanup) {
        (Ok(output), Ok(())) => Ok(output),
        (Ok(_), Err(_)) => Err(RuntimeError::substrate(
            "process_cleanup_failed",
            "private process invocation directory could not be reclaimed",
        )),
        (Err(error), _) => Err(error),
    }
}

#[cfg(unix)]
fn reclaim_private_directory(
    parent: &nix::dir::Dir,
    mut current: nix::dir::Dir,
    root_name: &std::ffi::CStr,
    identity: PrivateDirectoryIdentity,
) -> std::io::Result<()> {
    use nix::dir::Dir;
    use nix::errno::Errno;
    use nix::fcntl::AtFlags;
    use nix::sys::stat::{FchmodatFlags, Mode, SFlag, fchmod, fchmodat, fstat, fstatat};
    use nix::unistd::{UnlinkatFlags, unlinkat};

    fchmod(&current, Mode::S_IRWXU).map_err(nix_io_error)?;
    let root_stat = fstat(&current).map_err(nix_io_error)?;
    if root_stat.st_dev != identity.device || root_stat.st_ino != identity.inode {
        return Err(std::io::Error::other(
            "private directory cleanup root identity changed",
        ));
    }
    rewind_cleanup_directory(&mut current);
    let mut names = Vec::<std::ffi::CString>::new();
    loop {
        if let Some(name) = next_cleanup_entry(&mut current)? {
            let metadata = match fstatat(&current, name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
                Ok(metadata) => metadata,
                Err(Errno::ENOENT) => continue,
                Err(error) => return Err(nix_io_error(error)),
            };
            if SFlag::from_bits_truncate(metadata.st_mode) == SFlag::S_IFDIR {
                let child = match Dir::openat(
                    &current,
                    name.as_c_str(),
                    private_directory_open_flags(),
                    Mode::empty(),
                ) {
                    Ok(child) => child,
                    Err(Errno::EACCES) => {
                        fchmodat(
                            &current,
                            name.as_c_str(),
                            Mode::S_IRWXU,
                            FchmodatFlags::NoFollowSymlink,
                        )
                        .map_err(nix_io_error)?;
                        Dir::openat(
                            &current,
                            name.as_c_str(),
                            private_directory_open_flags(),
                            Mode::empty(),
                        )
                        .map_err(nix_io_error)?
                    }
                    Err(error) => return Err(nix_io_error(error)),
                };
                let child_stat = fstat(&child).map_err(nix_io_error)?;
                if child_stat.st_dev != identity.device {
                    return Err(std::io::Error::other(
                        "private directory cleanup refuses a mounted descendant",
                    ));
                }
                fchmod(&child, Mode::S_IRWXU).map_err(nix_io_error)?;
                names.push(name);
                current = child;
                rewind_cleanup_directory(&mut current);
                continue;
            }
            match unlinkat(&current, name.as_c_str(), UnlinkatFlags::NoRemoveDir) {
                Ok(()) | Err(Errno::ENOENT) => {}
                Err(error) => return Err(nix_io_error(error)),
            }
            continue;
        }
        let Some(name) = names.pop() else {
            break;
        };
        let ancestor = Dir::openat(
            &current,
            "..",
            private_directory_open_flags(),
            Mode::empty(),
        )
        .map_err(nix_io_error)?;
        match unlinkat(&ancestor, name.as_c_str(), UnlinkatFlags::RemoveDir) {
            Ok(()) | Err(Errno::ENOENT) => {}
            Err(error) => return Err(nix_io_error(error)),
        }
        current = ancestor;
    }
    let retained = fstat(&current).map_err(nix_io_error)?;
    let named_root =
        fstatat(parent, root_name, AtFlags::AT_SYMLINK_NOFOLLOW).map_err(nix_io_error)?;
    if retained.st_dev != identity.device
        || retained.st_ino != identity.inode
        || named_root.st_dev != identity.device
        || named_root.st_ino != identity.inode
        || SFlag::from_bits_truncate(named_root.st_mode) != SFlag::S_IFDIR
    {
        return Err(std::io::Error::other(
            "private directory cleanup name no longer identifies the retained root",
        ));
    }
    unlinkat(parent, root_name, UnlinkatFlags::RemoveDir).map_err(nix_io_error)
}

#[cfg(unix)]
fn next_cleanup_entry(directory: &mut nix::dir::Dir) -> std::io::Result<Option<std::ffi::CString>> {
    for entry in directory.iter() {
        let entry = entry.map_err(nix_io_error)?;
        let name = entry.file_name();
        if name.to_bytes() != b"." && name.to_bytes() != b".." {
            return Ok(Some(name.to_owned()));
        }
    }
    Ok(None)
}

#[cfg(unix)]
fn rewind_cleanup_directory(directory: &mut nix::dir::Dir) {
    drop(directory.iter());
}

#[cfg(unix)]
fn private_directory_open_flags() -> nix::fcntl::OFlag {
    nix::fcntl::OFlag::O_RDONLY
        | nix::fcntl::OFlag::O_DIRECTORY
        | nix::fcntl::OFlag::O_CLOEXEC
        | nix::fcntl::OFlag::O_NOFOLLOW
        | nix::fcntl::OFlag::O_NONBLOCK
}

#[cfg(unix)]
fn nix_io_error(error: nix::errno::Errno) -> std::io::Error {
    std::io::Error::from_raw_os_error(error as i32)
}

#[derive(Debug)]
struct RunningProcess {
    child: Child,
    supervisor: ProcessGroupSupervisor,
    launch: LaunchAuthority,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
    stderr: ChildStderr,
    input_offset: usize,
    stdout_bytes: Vec<u8>,
    stderr_bytes: Vec<u8>,
    stdout_eof: bool,
    stderr_eof: bool,
    status: Option<ExitStatus>,
    group_closed: bool,
}

impl RunningProcess {
    fn new(
        child: Child,
        supervisor: ProcessGroupSupervisor,
        launch: LaunchAuthority,
        stdin: ChildStdin,
        stdout: ChildStdout,
        stderr: ChildStderr,
    ) -> Self {
        Self {
            child,
            supervisor,
            launch,
            stdin: Some(stdin),
            stdout,
            stderr,
            input_offset: 0,
            stdout_bytes: Vec::new(),
            stderr_bytes: Vec::new(),
            stdout_eof: false,
            stderr_eof: false,
            status: None,
            group_closed: false,
        }
    }

    fn advance_io(
        &mut self,
        input: &[u8],
        limit: usize,
        ambiguous_world_effect: bool,
    ) -> RuntimeResult<bool> {
        let mut progressed = self.advance_stdin(input, ambiguous_world_effect)?;
        progressed |= advance_process_output(
            &mut self.stdout,
            &mut self.stdout_bytes,
            &mut self.stdout_eof,
            limit,
            ambiguous_world_effect,
            "stdout",
        )?;
        progressed |= advance_process_output(
            &mut self.stderr,
            &mut self.stderr_bytes,
            &mut self.stderr_eof,
            limit,
            ambiguous_world_effect,
            "stderr",
        )?;
        Ok(progressed)
    }

    fn advance_stdin(&mut self, input: &[u8], ambiguous_world_effect: bool) -> RuntimeResult<bool> {
        let Some(writer) = self.stdin.as_mut() else {
            return Ok(false);
        };
        let wrote = write_available(writer, input, &mut self.input_offset).map_err(|_| {
            process_failure(
                ambiguous_world_effect,
                "process_io_failed",
                "plugin process did not consume its complete request",
            )
        })?;
        if self.input_offset == input.len() {
            self.stdin = None;
        }
        Ok(wrote)
    }

    fn observe_completion(&mut self, ambiguous_world_effect: bool) -> RuntimeResult<bool> {
        if self.status.is_some() {
            return Ok(false);
        }
        match self.child.try_wait() {
            Ok(Some(status)) => {
                self.status = Some(status);
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(_) => Err(process_failure(
                ambiguous_world_effect,
                "process_wait_failed",
                "process plugin completion could not be observed",
            )),
        }
    }

    fn close_completed_group(&mut self, ambiguous_world_effect: bool) -> RuntimeResult<()> {
        if self.status.is_none() || self.group_closed {
            return Ok(());
        }
        // A one-request plugin may not leave background children behind.
        // Closing the occurrence group also closes pipes retained by forked
        // descendants before the next nonblocking drain.
        self.supervisor.kill_group();
        self.group_closed = true;
        if self.stdin.is_some() {
            return Err(process_failure(
                ambiguous_world_effect,
                "process_io_failed",
                "plugin process exited before consuming its complete request",
            ));
        }
        Ok(())
    }

    const fn is_complete(&self) -> bool {
        self.status.is_some() && self.stdout_eof && self.stderr_eof
    }

    fn terminate(&mut self) {
        terminate_process_tree(&mut self.child, &mut self.supervisor);
    }

    fn finish(mut self, ambiguous_world_effect: bool) -> RuntimeResult<Vec<u8>> {
        self.supervisor.wait();
        let status = self.status.ok_or_else(|| {
            process_failure(
                ambiguous_world_effect,
                "process_wait_failed",
                "process plugin completion was not retained",
            )
        })?;
        validate_exit(ambiguous_world_effect, status)?;
        Ok(self.stdout_bytes)
    }
}

fn exchange_process(
    mut process: RunningProcess,
    input: &[u8],
    limit: usize,
    deadline: Instant,
    ambiguous_world_effect: bool,
) -> RuntimeResult<Vec<u8>> {
    loop {
        if let Err(error) = process
            .launch
            .check_running(deadline, ambiguous_world_effect)
        {
            process.terminate();
            return Err(error);
        }
        let mut progressed = match process.advance_io(input, limit, ambiguous_world_effect) {
            Ok(progressed) => progressed,
            Err(error) => {
                process.terminate();
                return Err(error);
            }
        };
        progressed |= match process.observe_completion(ambiguous_world_effect) {
            Ok(progressed) => progressed,
            Err(error) => {
                process.terminate();
                return Err(error);
            }
        };
        if !process.group_closed && process.supervisor.try_wait() {
            process.terminate();
            return Err(process_failure(
                ambiguous_world_effect,
                "process_supervisor_failed",
                "process parent-liveness supervisor exited before the occurrence closed",
            ));
        }
        if let Err(error) = process.close_completed_group(ambiguous_world_effect) {
            process.terminate();
            return Err(error);
        }
        if process.is_complete() {
            break;
        }
        if !progressed {
            thread::sleep(remaining(deadline).min(Duration::from_millis(1)));
        }
    }
    process.finish(ambiguous_world_effect)
}

fn advance_process_output(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    eof: &mut bool,
    limit: usize,
    ambiguous_world_effect: bool,
    stream: &str,
) -> RuntimeResult<bool> {
    if *eof {
        return Ok(false);
    }
    let (read, observed_eof) = read_available(reader, bytes, limit)
        .map_err(|error| process_read_failure(ambiguous_world_effect, &error, stream))?;
    *eof = observed_eof;
    Ok(read)
}

#[cfg(unix)]
#[derive(Debug)]
struct ProcessGroupSupervisor {
    process_group: i32,
    execution_deadline: nix::libc::timespec,
    engine_liveness: Option<UnixStream>,
    descriptor_authority: ChildDescriptorAuthority,
    reaped: bool,
    group_closed: bool,
}

#[cfg(unix)]
impl ProcessGroupSupervisor {
    fn start(deadline: Instant, launch: &LaunchAuthority) -> RuntimeResult<Self> {
        launch.check_pre_start(deadline)?;
        let watchdog_deadline = monotonic_deadline(remaining(deadline))?;
        // SAFETY: `getpid` observes only the current process identity and is
        // called on the parent side before the watchdog fork.
        let engine_pid = unsafe { nix::libc::getpid() };
        if engine_pid <= 0 {
            return Err(RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process Engine identity could not be observed",
            ));
        }
        let (watchdog_liveness, engine_liveness) = UnixStream::pair().map_err(|_| {
            RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process parent-liveness channel could not be created",
            )
        })?;
        let (mut engine_ready, watchdog_ready) = UnixStream::pair().map_err(|_| {
            RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process supervisor readiness channel could not be created",
            )
        })?;
        engine_ready.set_nonblocking(true).map_err(|_| {
            RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process supervisor readiness channel could not be bounded",
            )
        })?;

        // Apple allocates and sizes this table in the multithreaded parent.
        // Both later forked children inherit the same untouched pages and fill
        // their own copy, so neither post-fork boundary allocates or discovers
        // descriptors through a userspace directory walk.
        let mut descriptor_authority = ChildDescriptorAuthority::prepare()?;
        let watchdog_descriptor_authority = descriptor_authority.view();
        launch.check_pre_start(deadline)?;

        let watchdog_liveness_fd = watchdog_liveness.as_raw_fd();
        let watchdog_ready_fd = watchdog_ready.as_raw_fd();
        // SAFETY: the forked branch never returns to Rust. It executes only the
        // reviewed no-userspace-state syscall boundary (including Apple's
        // single-call libproc wrapper) over parent-created storage and
        // descriptors before terminating via SIGKILL or `_exit`.
        let watchdog_pid = unsafe { nix::libc::fork() };
        if watchdog_pid < 0 {
            return Err(RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process parent-liveness supervisor could not be forked",
            ));
        }
        if watchdog_pid == 0 {
            // SAFETY: this is the fork-only child path described above.
            unsafe {
                run_parent_liveness_watchdog(
                    watchdog_liveness_fd,
                    watchdog_ready_fd,
                    watchdog_deadline,
                    watchdog_descriptor_authority,
                    engine_pid,
                    launch.state_ptr(),
                )
            }
        }

        // Establish the exact process group from the parent before any ready,
        // deadline, or cleanup path can try to signal it. The child repeats the
        // same idempotent assignment before descriptor discovery, but parent
        // authority closes the scheduling gap where that child has not run.
        if unsafe { nix::libc::setpgid(watchdog_pid, watchdog_pid) } != 0 {
            // SAFETY: `watchdog_pid` is the exact child returned by fork. This
            // error path signals and reaps only that child before returning.
            unsafe {
                nix::libc::kill(watchdog_pid, nix::libc::SIGKILL);
                while nix::libc::waitpid(watchdog_pid, std::ptr::null_mut(), 0) < 0
                    && nix::errno::Errno::last_raw() == nix::libc::EINTR
                {}
            }
            return Err(RuntimeError::substrate(
                "process_supervisor_start_failed",
                "process parent-liveness group could not be established",
            ));
        }

        drop(watchdog_liveness);
        drop(watchdog_ready);
        let mut supervisor = Self {
            process_group: watchdog_pid,
            execution_deadline: watchdog_deadline,
            engine_liveness: Some(engine_liveness),
            descriptor_authority,
            reaped: false,
            group_closed: false,
        };
        let ready = wait_for_supervisor_ready(&mut engine_ready, deadline, launch);
        if let Err(error) = ready {
            supervisor.terminate();
            return Err(error);
        }
        if let Err(error) = launch.check_pre_start(deadline) {
            supervisor.terminate();
            return Err(error);
        }
        Ok(supervisor)
    }

    const fn process_group(&self) -> i32 {
        self.process_group
    }

    const fn execution_deadline(&self) -> nix::libc::timespec {
        self.execution_deadline
    }

    fn engine_liveness_fd(&self) -> i32 {
        self.engine_liveness
            .as_ref()
            .map_or(-1, std::os::fd::AsRawFd::as_raw_fd)
    }

    fn descriptor_authority_view(&mut self) -> ChildDescriptorAuthorityView {
        self.descriptor_authority.view()
    }

    fn kill_group(&mut self) {
        if self.group_closed {
            return;
        }
        kill_process_group(self.process_group);
        // Closing the Engine-owned endpoint is a second fail-closed path if the
        // explicit group signal raced or failed. The watchdog is itself a group
        // member, so its PGID cannot be reused while it can still act on EOF.
        self.engine_liveness.take();
        self.group_closed = true;
    }

    fn try_wait(&mut self) -> bool {
        use nix::errno::Errno;
        use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
        use nix::unistd::Pid;

        if self.reaped {
            return true;
        }
        match waitpid(
            Pid::from_raw(self.process_group),
            Some(WaitPidFlag::WNOHANG),
        ) {
            Ok(WaitStatus::StillAlive) | Err(Errno::EINTR) => false,
            Ok(_) | Err(_) => {
                self.reaped = true;
                true
            }
        }
    }

    fn wait(&mut self) {
        use nix::errno::Errno;
        use nix::sys::wait::waitpid;
        use nix::unistd::Pid;

        if self.reaped {
            return;
        }
        while let Err(Errno::EINTR) = waitpid(Pid::from_raw(self.process_group), None) {
            // Retry only the interrupted wait for this exact child.
        }
        self.reaped = true;
    }

    fn terminate(&mut self) {
        self.kill_group();
        self.wait();
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupSupervisor {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(unix)]
fn wait_for_supervisor_ready(
    readiness: &mut UnixStream,
    deadline: Instant,
    launch: &LaunchAuthority,
) -> RuntimeResult<()> {
    let mut byte = [0_u8; 1];
    loop {
        launch.check_pre_start(deadline)?;
        match readiness.read(&mut byte) {
            Ok(1) if byte[0] == 1 => {
                return launch.check_pre_start(deadline);
            }
            Ok(0) => {
                return Err(RuntimeError::substrate(
                    "process_supervisor_start_failed",
                    "process parent-liveness supervisor exited before readiness",
                ));
            }
            Ok(_) => {
                return Err(RuntimeError::substrate(
                    "process_supervisor_start_failed",
                    "process parent-liveness supervisor returned invalid readiness",
                ));
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                thread::sleep(remaining(deadline).min(Duration::from_millis(1)));
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(_) => {
                return Err(RuntimeError::substrate(
                    "process_supervisor_start_failed",
                    "process supervisor readiness could not be observed",
                ));
            }
        }
    }
}

#[cfg(unix)]
unsafe fn run_parent_liveness_watchdog(
    watchdog_liveness_fd: i32,
    watchdog_ready_fd: i32,
    deadline: nix::libc::timespec,
    descriptor_authority: ChildDescriptorAuthorityView,
    engine_pid: i32,
    launch_state: *const AtomicU8,
) -> ! {
    // The watchdog deliberately has no Rust cleanup or runtime path after
    // fork. Every operation is a reviewed syscall over values fully prepared
    // by the parent. Process-group authority is established before the Apple
    // descriptor query, so a blocked kernel query remains deadline-killable.
    unsafe {
        let mut blocked_signals = std::mem::MaybeUninit::<nix::libc::sigset_t>::uninit();
        if nix::libc::sigfillset(blocked_signals.as_mut_ptr()) != 0
            || nix::libc::sigprocmask(
                nix::libc::SIG_SETMASK,
                blocked_signals.as_ptr(),
                std::ptr::null_mut(),
            ) != 0
        {
            nix::libc::close(watchdog_ready_fd);
            nix::libc::_exit(127);
        }
        if nix::libc::setpgid(0, 0) != 0 {
            nix::libc::close(watchdog_ready_fd);
            nix::libc::_exit(127);
        }
        if !close_unrelated_descriptors(
            watchdog_liveness_fd,
            watchdog_ready_fd,
            descriptor_authority,
        ) {
            nix::libc::close(watchdog_ready_fd);
            nix::libc::_exit(127);
        }
        let ready = [1_u8; 1];
        if nix::libc::write(
            watchdog_ready_fd,
            ready.as_ptr().cast::<nix::libc::c_void>(),
            ready.len(),
        ) != 1
        {
            nix::libc::close(watchdog_ready_fd);
            nix::libc::_exit(127);
        }
        nix::libc::close(watchdog_ready_fd);

        wait_for_liveness_end_or_deadline(watchdog_liveness_fd, deadline, engine_pid, launch_state);
        let process_group = nix::libc::getpgrp();
        if process_group > 0 {
            nix::libc::kill(-process_group, nix::libc::SIGKILL);
        }
        nix::libc::_exit(127);
    }
}

#[cfg(unix)]
fn monotonic_deadline(timeout: Duration) -> RuntimeResult<nix::libc::timespec> {
    let mut now = std::mem::MaybeUninit::<nix::libc::timespec>::uninit();
    // SAFETY: `now` points to writable storage for one `timespec` and
    // `CLOCK_MONOTONIC` has no caller-owned state.
    if unsafe { nix::libc::clock_gettime(nix::libc::CLOCK_MONOTONIC, now.as_mut_ptr()) } != 0 {
        return Err(RuntimeError::substrate(
            "process_supervisor_start_failed",
            "process supervisor monotonic clock could not be read",
        ));
    }
    // SAFETY: `clock_gettime` initialized `now` after returning success.
    let now = unsafe { now.assume_init() };
    let seconds = nix::libc::time_t::try_from(timeout.as_secs()).map_err(|_| {
        RuntimeError::plugin_defect("process executor timeout exceeds the watchdog clock range")
    })?;
    let nanoseconds = nix::libc::c_long::from(timeout.subsec_nanos());
    let mut deadline_seconds = now.tv_sec.checked_add(seconds).ok_or_else(|| {
        RuntimeError::plugin_defect("process executor timeout exceeds the watchdog clock range")
    })?;
    let mut deadline_nanoseconds = now.tv_nsec.checked_add(nanoseconds).ok_or_else(|| {
        RuntimeError::plugin_defect("process executor timeout exceeds the watchdog clock range")
    })?;
    if deadline_nanoseconds >= 1_000_000_000 {
        deadline_nanoseconds -= 1_000_000_000;
        deadline_seconds = deadline_seconds.checked_add(1).ok_or_else(|| {
            RuntimeError::plugin_defect("process executor timeout exceeds the watchdog clock range")
        })?;
    }
    Ok(nix::libc::timespec {
        tv_sec: deadline_seconds,
        tv_nsec: deadline_nanoseconds,
    })
}

#[cfg(unix)]
unsafe fn wait_for_liveness_end_or_deadline(
    liveness_fd: i32,
    deadline: nix::libc::timespec,
    engine_pid: i32,
    launch_state: *const AtomicU8,
) {
    unsafe {
        loop {
            if nix::libc::getppid() != engine_pid {
                return;
            }
            let launch = &*launch_state;
            if matches!(
                launch.load(Ordering::SeqCst),
                LAUNCH_CANCELLED_BEFORE_START
                    | LAUNCH_CANCELLED_AFTER_START
                    | LAUNCH_EXPIRED_BEFORE_START
                    | LAUNCH_EXPIRED_AFTER_START
            ) {
                return;
            }
            let mut now = std::mem::MaybeUninit::<nix::libc::timespec>::uninit();
            if nix::libc::clock_gettime(nix::libc::CLOCK_MONOTONIC, now.as_mut_ptr()) != 0 {
                return;
            }
            let now = now.assume_init();
            let Some(timeout) = poll_timeout_milliseconds(now, deadline) else {
                transition_launch_expiration(launch);
                return;
            };
            let mut descriptor = nix::libc::pollfd {
                fd: liveness_fd,
                events: nix::libc::POLLIN,
                revents: 0,
            };
            let polled = nix::libc::poll(
                &raw mut descriptor,
                1,
                timeout.min(PARENT_RELATION_POLL_INTERVAL_MS),
            );
            if polled == 0 {
                continue;
            }
            if polled < 0 {
                if nix::errno::Errno::last_raw() == nix::libc::EINTR {
                    continue;
                }
                return;
            }
            let mut observed = [0_u8; 1];
            let read = nix::libc::read(
                liveness_fd,
                observed.as_mut_ptr().cast::<nix::libc::c_void>(),
                observed.len(),
            );
            if read <= 0 {
                return;
            }
        }
    }
}

#[cfg(unix)]
fn poll_timeout_milliseconds(
    now: nix::libc::timespec,
    deadline: nix::libc::timespec,
) -> Option<i32> {
    if now.tv_sec > deadline.tv_sec
        || (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec)
    {
        return None;
    }
    let mut seconds = deadline.tv_sec - now.tv_sec;
    let nanoseconds = if deadline.tv_nsec >= now.tv_nsec {
        deadline.tv_nsec - now.tv_nsec
    } else {
        seconds -= 1;
        1_000_000_000 + deadline.tv_nsec - now.tv_nsec
    };
    let seconds_milliseconds = i128::from(seconds).checked_mul(1_000)?;
    let rounded_nanoseconds = i128::from(nanoseconds).checked_add(999_999)? / 1_000_000;
    let milliseconds = seconds_milliseconds.checked_add(rounded_nanoseconds)?;
    Some(i32::try_from(milliseconds).unwrap_or(i32::MAX))
}

#[cfg(target_os = "linux")]
unsafe fn close_unrelated_descriptors(
    first_keep: i32,
    second_keep: i32,
    _descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    let (lower, upper) = if first_keep < second_keep {
        (first_keep, second_keep)
    } else {
        (second_keep, first_keep)
    };
    if lower < 0 || lower == upper {
        return false;
    }
    let Some(middle_first) = lower.checked_add(1) else {
        return false;
    };
    let Some(middle_last) = upper.checked_sub(1) else {
        return false;
    };
    let after_upper = upper.checked_add(1);
    unsafe {
        (lower == 0 || close_linux_descriptor_range(0, (lower - 1) as u32))
            && close_linux_descriptor_range(middle_first as u32, middle_last as u32)
            && after_upper.is_none_or(|first| close_linux_descriptor_range(first as u32, u32::MAX))
    }
}

#[cfg(target_os = "linux")]
unsafe fn close_linux_descriptor_range(first: u32, last: u32) -> bool {
    if first > last {
        return true;
    }
    unsafe { nix::libc::syscall(nix::libc::SYS_close_range, first, last, 0_u32) == 0 }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn close_unrelated_descriptors(
    first_keep: i32,
    second_keep: i32,
    descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    if first_keep < 0
        || second_keep < 0
        || first_keep == second_keep
        || first_keep >= descriptor_authority.descriptor_domain_exclusive
        || second_keep >= descriptor_authority.descriptor_domain_exclusive
    {
        return false;
    }
    let Some(descriptor_count) = (unsafe { apple_open_descriptor_count(descriptor_authority) })
    else {
        return false;
    };
    let descriptors = std::ptr::with_exposed_provenance::<nix::libc::proc_fdinfo>(
        descriptor_authority.buffer_address,
    );
    let mut saw_first = false;
    let mut saw_second = false;
    for index in 0..descriptor_count {
        // SAFETY: `apple_open_descriptor_count` authenticated this initialized
        // prefix and every index remains inside that exact prefix.
        let descriptor = unsafe { (*descriptors.add(index)).proc_fd };
        if descriptor == first_keep {
            saw_first = true;
        } else if descriptor == second_keep {
            saw_second = true;
        } else if unsafe { nix::libc::close(descriptor) } != 0 {
            return false;
        }
    }
    saw_first && saw_second
}

#[cfg(all(
    unix,
    not(target_os = "linux"),
    not(target_os = "macos"),
    not(target_os = "ios")
))]
unsafe fn close_unrelated_descriptors(
    first_keep: i32,
    second_keep: i32,
    descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    let descriptor_limit = descriptor_authority.descriptor_limit;
    if descriptor_limit < 3 {
        return false;
    }
    for descriptor in 0..descriptor_limit {
        if descriptor != first_keep && descriptor != second_keep {
            unsafe { nix::libc::close(descriptor) };
        }
    }
    true
}

#[cfg(not(unix))]
#[derive(Debug)]
struct ProcessGroupSupervisor;

#[cfg(not(unix))]
impl ProcessGroupSupervisor {
    fn start(_deadline: Instant, _launch: &LaunchAuthority) -> RuntimeResult<Self> {
        Err(RuntimeError::plugin_defect(
            "the process executor requires Unix parent-liveness semantics",
        ))
    }

    const fn process_group(&self) -> i32 {
        0
    }

    const fn execution_deadline(&self) {}

    const fn engine_liveness_fd(&self) -> i32 {
        -1
    }

    const fn descriptor_authority_view(&mut self) {}

    fn kill_group(&mut self) {}

    const fn try_wait(&mut self) -> bool {
        true
    }

    fn wait(&mut self) {}

    fn terminate(&mut self) {}
}

#[cfg(unix)]
fn capture_directory(path: &Path, budget: &mut ClosureBudget) -> RuntimeResult<CapturedDirectory> {
    if !path.is_absolute() {
        return Err(RuntimeError::plugin_defect(
            "process working directory must be an absolute directory",
        ));
    }
    let resolved = fs::canonicalize(path).map_err(|_| {
        RuntimeError::plugin_defect("process working directory could not be resolved")
    })?;
    let directory = open_absolute_directory(&resolved)?;
    capture_open_directory(directory, budget)
}

#[cfg(unix)]
fn capture_open_directory(
    directory: nix::dir::Dir,
    budget: &mut ClosureBudget,
) -> RuntimeResult<CapturedDirectory> {
    let mut directories = Vec::new();
    let mut files = Vec::new();
    budget.charge_framed(PROCESS_WORKING_DIRECTORY_ID_DOMAIN.len())?;
    budget.charge(CLOSURE_LENGTH_BYTES * 2)?;
    budget.charge(CLOSURE_MODE_BYTES * 2)?;
    capture_directory_entries(directory, &mut directories, &mut files, budget)?;
    directories.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let directory_identities = directories
        .iter()
        .map(|directory| CapturedDirectoryEntryIdentity {
            path: &directory.relative_path,
            mode: directory.mode,
        })
        .collect();
    let identities = files
        .iter()
        .map(|file| CapturedFileIdentity {
            path: &file.relative_path,
            digest: format!("sha256:{}", sha256_bytes(&file.bytes)),
            mode: file.mode,
        })
        .collect();
    let identity = CapturedDirectoryIdentity {
        version: PROCESS_WORKING_DIRECTORY_ID_DOMAIN,
        root_mode: PRIVATE_DIRECTORY_MODE,
        directory_mode: PRIVATE_DIRECTORY_MODE,
        directories: directory_identities,
        files: identities,
    };
    let identity = format!(
        "sha256:{}",
        sha256_bytes(&canonical_bytes(&identity).map_err(|_| {
            RuntimeError::plugin_defect("process working directory could not be canonicalized")
        })?)
    );
    Ok(CapturedDirectory {
        root_mode: PRIVATE_DIRECTORY_MODE,
        directories,
        files,
        identity,
    })
}

#[cfg(unix)]
fn open_absolute_directory(path: &Path) -> RuntimeResult<nix::dir::Dir> {
    use nix::dir::Dir;
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;

    let flags = OFlag::O_RDONLY
        | OFlag::O_DIRECTORY
        | OFlag::O_CLOEXEC
        | OFlag::O_NOFOLLOW
        | OFlag::O_NONBLOCK;
    let mut directory = Dir::open(Path::new("/"), flags, Mode::empty()).map_err(|_| {
        RuntimeError::plugin_defect("process working directory root could not be opened")
    })?;
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                directory = Dir::openat(&directory, name, flags, Mode::empty()).map_err(|_| {
                    RuntimeError::plugin_defect(
                        "process working directory components must be accessible directories without symlinks",
                    )
                })?;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(RuntimeError::plugin_defect(
                    "process working directory contains an invalid path component",
                ));
            }
        }
    }
    Ok(directory)
}

#[cfg(unix)]
struct DirectoryCaptureFrame {
    directory: nix::dir::Dir,
    relative_parent: String,
    entries: std::vec::IntoIter<std::ffi::CString>,
}

#[cfg(unix)]
fn directory_capture_frame(
    mut directory: nix::dir::Dir,
    relative_parent: String,
    budget: &mut ClosureBudget,
) -> RuntimeResult<DirectoryCaptureFrame> {
    let mut entries = Vec::new();
    for entry in directory.iter() {
        let entry = entry.map_err(|_| {
            RuntimeError::plugin_defect("process working directory could not be read")
        })?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        budget.count_directory_entry()?;
        entries.push(name.to_owned());
    }
    Ok(DirectoryCaptureFrame {
        directory,
        relative_parent,
        entries: entries.into_iter(),
    })
}

#[cfg(unix)]
fn capture_directory_entries(
    directory: nix::dir::Dir,
    directories: &mut Vec<CapturedDirectoryEntry>,
    files: &mut Vec<CapturedFile>,
    budget: &mut ClosureBudget,
) -> RuntimeResult<()> {
    use nix::fcntl::{OFlag, openat};
    use nix::sys::stat::{Mode, SFlag, fstat};

    let flags = OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK;
    let mut frames = vec![directory_capture_frame(directory, String::new(), budget)?];
    while let Some(frame) = frames.last_mut() {
        let Some(name) = frame.entries.next() else {
            let completed = frames
                .pop()
                .ok_or_else(|| RuntimeError::plugin_defect("process directory traversal failed"))?;
            if !completed.relative_parent.is_empty() {
                budget.charge(CLOSURE_MODE_BYTES)?;
                directories.push(CapturedDirectoryEntry {
                    relative_path: completed.relative_parent,
                    mode: PRIVATE_DIRECTORY_MODE,
                });
            }
            continue;
        };
        let relative =
            charge_and_join_relative_path(&frame.relative_parent, name.as_c_str(), budget)?;
        let descriptor =
            openat(&frame.directory, name.as_c_str(), flags, Mode::empty()).map_err(|_| {
                RuntimeError::plugin_defect(
                    "process working directory entries must be readable without following symlinks",
                )
            })?;
        let metadata = fstat(&descriptor).map_err(|_| {
            RuntimeError::plugin_defect("process working directory metadata could not be read")
        })?;
        let file_type = SFlag::from_bits_truncate(metadata.st_mode);
        if file_type == SFlag::S_IFDIR {
            let child = nix::dir::Dir::from_fd(descriptor).map_err(|_| {
                RuntimeError::plugin_defect("process working directory could not be opened")
            })?;
            frames.push(directory_capture_frame(child, relative, budget)?);
        } else if file_type == SFlag::S_IFREG {
            budget.charge(CLOSURE_MODE_BYTES)?;
            let (bytes, mode) = capture_directory_file(File::from(descriptor), budget)?;
            files.push(CapturedFile {
                relative_path: relative,
                bytes,
                mode,
            });
        } else {
            return Err(RuntimeError::plugin_defect(
                "process working directory contains a special file",
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn charge_and_join_relative_path(
    relative_parent: &str,
    name: &std::ffi::CStr,
    budget: &mut ClosureBudget,
) -> RuntimeResult<String> {
    let name = name.to_str().map_err(|_| {
        RuntimeError::plugin_defect("process working directory paths must be valid UTF-8")
    })?;
    let separator = usize::from(!relative_parent.is_empty());
    let path_length = relative_parent
        .len()
        .checked_add(separator)
        .and_then(|length| length.checked_add(name.len()))
        .ok_or_else(|| RuntimeError::plugin_defect("process working directory path overflowed"))?;
    budget.charge_directory_entry_encoding()?;
    budget.charge_framed(path_length)?;
    let mut relative = String::with_capacity(path_length);
    if !relative_parent.is_empty() {
        relative.push_str(relative_parent);
        relative.push('/');
    }
    relative.push_str(name);
    Ok(relative)
}

#[cfg(unix)]
fn capture_directory_file(
    mut file: File,
    budget: &mut ClosureBudget,
) -> RuntimeResult<(Vec<u8>, u32)> {
    let metadata = file.metadata().map_err(|_| {
        RuntimeError::plugin_defect("process working directory file metadata could not be read")
    })?;
    let limit_u64 = u64::try_from(budget.maximum_blob_bytes()?)
        .map_err(|_| RuntimeError::plugin_defect("process closure limit is invalid"))?;
    if !metadata.is_file() || metadata.len() > limit_u64 {
        return Err(RuntimeError::plugin_defect(
            "process working directory exceeds the configured closure limit",
        ));
    }
    let mode = executable_mode(&metadata);
    budget.charge(CLOSURE_LENGTH_BYTES)?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|_| {
            RuntimeError::plugin_defect("process working directory file could not be read")
        })?;
        if read == 0 {
            break;
        }
        budget.charge(read)?;
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok((bytes, mode))
}

#[cfg(not(unix))]
fn capture_directory(
    _path: &Path,
    _budget: &mut ClosureBudget,
) -> RuntimeResult<CapturedDirectory> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires Unix descriptor-relative capture semantics",
    ))
}

#[cfg(unix)]
fn materialize_directory(
    invocation: &PrivateInvocationDirectory,
    snapshot: &CapturedDirectory,
    deadline: Instant,
    cancellation: Option<&ProcessCancellation>,
) -> RuntimeResult<PathBuf> {
    use nix::dir::Dir;
    use nix::sys::stat::{FchmodatFlags, Mode, fchmod, fchmodat, mkdirat};

    check_materialization_authority(deadline, cancellation)?;
    let destination = invocation.path().join("cwd");
    mkdirat(invocation.root(), "cwd", Mode::S_IRWXU).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private working directory could not be created",
        )
    })?;
    fchmodat(
        invocation.root(),
        "cwd",
        normalized_mode(snapshot.root_mode)?,
        FchmodatFlags::NoFollowSymlink,
    )
    .map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private working directory mode could not be fixed",
        )
    })?;
    let working_root = Dir::openat(
        invocation.root(),
        "cwd",
        private_directory_open_flags(),
        Mode::empty(),
    )
    .map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private working directory could not be opened",
        )
    })?;
    fchmod(&working_root, normalized_mode(snapshot.root_mode)?).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private working directory mode could not be fixed",
        )
    })?;
    let mut cursor = RelativeDirectoryCursor::new(working_root);
    for directory in &snapshot.directories {
        check_materialization_authority(deadline, cancellation)?;
        cursor.create_directory(
            &directory.relative_path,
            directory.mode,
            deadline,
            cancellation,
        )?;
    }
    for file in &snapshot.files {
        check_materialization_authority(deadline, cancellation)?;
        cursor.write_file(
            &file.relative_path,
            &file.bytes,
            file.mode,
            deadline,
            cancellation,
        )?;
    }
    check_materialization_authority(deadline, cancellation)?;
    Ok(destination)
}

#[cfg(not(unix))]
fn materialize_directory(
    _invocation: &PrivateInvocationDirectory,
    _snapshot: &CapturedDirectory,
    _deadline: Instant,
    _cancellation: Option<&ProcessCancellation>,
) -> RuntimeResult<PathBuf> {
    Err(RuntimeError::plugin_defect(
        "Unix descriptor-relative materialization is required",
    ))
}

#[cfg(unix)]
struct RelativeDirectoryCursor {
    components: Vec<std::ffi::OsString>,
    directory: nix::dir::Dir,
}

#[cfg(unix)]
impl RelativeDirectoryCursor {
    fn new(root: nix::dir::Dir) -> Self {
        Self {
            components: Vec::new(),
            directory: root,
        }
    }

    fn create_directory(
        &mut self,
        relative: &str,
        mode: u32,
        deadline: Instant,
        cancellation: Option<&ProcessCancellation>,
    ) -> RuntimeResult<()> {
        use nix::dir::Dir;
        use nix::sys::stat::{FchmodatFlags, Mode, fchmod, fchmodat, fstat, mkdirat};

        let (parents, name) = split_relative_entry(relative)?;
        self.move_to(&parents, deadline, cancellation)?;
        mkdirat(&self.directory, name.as_os_str(), Mode::S_IRWXU).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory tree could not be created",
            )
        })?;
        fchmodat(
            &self.directory,
            name.as_os_str(),
            normalized_mode(mode)?,
            FchmodatFlags::NoFollowSymlink,
        )
        .map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory mode could not be fixed",
            )
        })?;
        let directory = Dir::openat(
            &self.directory,
            name.as_os_str(),
            private_directory_open_flags(),
            Mode::empty(),
        )
        .map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory tree could not be opened",
            )
        })?;
        fchmod(&directory, normalized_mode(mode)?).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory mode could not be fixed",
            )
        })?;
        let metadata = fstat(&directory).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory mode could not be verified",
            )
        })?;
        if u32::from(metadata.st_mode & 0o7777) != mode {
            return Err(RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory mode did not match its binding",
            ));
        }
        self.components.push(name);
        self.directory = directory;
        check_materialization_authority(deadline, cancellation)
    }

    fn write_file(
        &mut self,
        relative: &str,
        bytes: &[u8],
        mode: u32,
        deadline: Instant,
        cancellation: Option<&ProcessCancellation>,
    ) -> RuntimeResult<()> {
        let (parents, name) = split_relative_entry(relative)?;
        self.move_to(&parents, deadline, cancellation)?;
        write_new_file_at(&self.directory, &name, bytes, mode, deadline, cancellation)
    }

    fn move_to(
        &mut self,
        target: &[std::ffi::OsString],
        deadline: Instant,
        cancellation: Option<&ProcessCancellation>,
    ) -> RuntimeResult<()> {
        use nix::dir::Dir;
        use nix::sys::stat::Mode;

        let common = self
            .components
            .iter()
            .zip(target)
            .take_while(|(left, right)| left == right)
            .count();
        for _ in common..self.components.len() {
            check_materialization_authority(deadline, cancellation)?;
            self.directory = Dir::openat(
                &self.directory,
                "..",
                private_directory_open_flags(),
                Mode::empty(),
            )
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private working-directory parent could not be reopened",
                )
            })?;
        }
        self.components.truncate(common);
        for component in &target[common..] {
            check_materialization_authority(deadline, cancellation)?;
            let directory = Dir::openat(
                &self.directory,
                component.as_os_str(),
                private_directory_open_flags(),
                Mode::empty(),
            )
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private working-directory parent could not be opened",
                )
            })?;
            self.components.push(component.clone());
            self.directory = directory;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn split_relative_entry(
    relative: &str,
) -> RuntimeResult<(Vec<std::ffi::OsString>, std::ffi::OsString)> {
    let mut components = Vec::new();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(name) => components.push(name.to_os_string()),
            Component::RootDir
            | Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_) => {
                return Err(RuntimeError::substrate(
                    "process_seal_failed",
                    "captured working-directory path is not a normal relative entry",
                ));
            }
        }
    }
    let name = components.pop().ok_or_else(|| {
        RuntimeError::substrate(
            "process_seal_failed",
            "captured working-directory entry is empty",
        )
    })?;
    Ok((components, name))
}

#[cfg(unix)]
fn write_new_file_at(
    parent: &nix::dir::Dir,
    name: &std::ffi::OsStr,
    bytes: &[u8],
    mode: u32,
    deadline: Instant,
    cancellation: Option<&ProcessCancellation>,
) -> RuntimeResult<()> {
    use nix::fcntl::{OFlag, openat};
    use nix::sys::stat::{Mode, fchmod};
    use std::os::unix::fs::PermissionsExt;

    check_materialization_authority(deadline, cancellation)?;
    let descriptor = openat(
        parent,
        name,
        OFlag::O_WRONLY
            | OFlag::O_CREAT
            | OFlag::O_EXCL
            | OFlag::O_CLOEXEC
            | OFlag::O_NOFOLLOW
            | OFlag::O_NONBLOCK,
        Mode::S_IRUSR | Mode::S_IWUSR,
    )
    .map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file could not be created",
        )
    })?;
    let mut file = File::from(descriptor);
    write_file_contents(&mut file, bytes, deadline, cancellation)?;
    fchmod(&file, normalized_mode(mode)?).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file permissions could not be set",
        )
    })?;
    let metadata = file.metadata().map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file permissions could not be verified",
        )
    })?;
    if metadata.permissions().mode() & 0o7777 != mode {
        return Err(RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file mode did not match its binding",
        ));
    }
    check_materialization_authority(deadline, cancellation)
}

#[cfg(unix)]
fn normalized_mode(mode: u32) -> RuntimeResult<nix::sys::stat::Mode> {
    let bits = nix::libc::mode_t::try_from(mode).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure mode exceeds the platform domain",
        )
    })?;
    Ok(nix::sys::stat::Mode::from_bits_truncate(bits))
}

fn write_file_contents(
    file: &mut File,
    bytes: &[u8],
    deadline: Instant,
    cancellation: Option<&ProcessCancellation>,
) -> RuntimeResult<()> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        check_materialization_authority(deadline, cancellation)?;
        let end = checked_chunk_end(offset, bytes.len(), 64 * 1024).ok_or_else(|| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process closure write range overflowed",
            )
        })?;
        let written = file.write(&bytes[offset..end]).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process closure file could not be written",
            )
        })?;
        if written == 0 {
            return Err(RuntimeError::substrate(
                "process_seal_failed",
                "private process closure file accepted zero bytes",
            ));
        }
        offset += written;
    }
    Ok(())
}

fn check_materialization_authority(
    deadline: Instant,
    cancellation: Option<&ProcessCancellation>,
) -> RuntimeResult<()> {
    if cancellation.is_some_and(ProcessCancellation::is_cancelled) {
        return Err(invocation_cancelled(false));
    }
    if Instant::now() >= deadline {
        return Err(RuntimeError::timed_out(
            "process_response_timed_out",
            "process closure materialization exceeded the invocation deadline",
        ));
    }
    Ok(())
}

fn validate_outbound_json(input: &[u8], boundary: &str) -> RuntimeResult<()> {
    validate_strict_json(input).map_err(|_| RuntimeError::PluginDefect {
        code: "invalid_process_request".to_owned(),
        message: format!("{boundary} is outside the shared exact JSON domain"),
    })
}

fn is_world_mutating_effect(request: &PluginRequest) -> bool {
    matches!(
        request,
        PluginRequest::DispatchEffect { .. } | PluginRequest::ReconcileEffect { .. }
    )
}

fn post_start_failure(request: &PluginRequest, code: &str, message: &str) -> RuntimeError {
    if is_world_mutating_effect(request) {
        RuntimeError::unknown_world(code, message)
    } else {
        RuntimeError::PluginDefect {
            code: code.to_owned(),
            message: message.to_owned(),
        }
    }
}

fn response_validation_failure(request: &PluginRequest, error: RuntimeError) -> RuntimeError {
    if is_world_mutating_effect(request) {
        RuntimeError::unknown_world(
            "invalid_plugin_response",
            "effect process returned a response outside its request authority",
        )
    } else {
        error
    }
}

fn process_failure(ambiguous_world_effect: bool, code: &str, message: &str) -> RuntimeError {
    if ambiguous_world_effect {
        RuntimeError::unknown_world(code, message)
    } else {
        RuntimeError::substrate(code, message)
    }
}

fn process_read_failure(
    ambiguous_world_effect: bool,
    error: &std::io::Error,
    stream: &str,
) -> RuntimeError {
    if error.kind() == ErrorKind::FileTooLarge {
        return if ambiguous_world_effect {
            RuntimeError::unknown_world(
                "effect_dispatch_output_limit_exceeded",
                format!("world-mutating effect {stream} exceeded the admitted byte limit"),
            )
        } else {
            RuntimeError::PluginDefect {
                code: "plugin_output_limit_exceeded".to_owned(),
                message: format!("plugin {stream} exceeded the admitted byte limit"),
            }
        };
    }
    process_failure(
        ambiguous_world_effect,
        "process_io_failed",
        &format!("bounded plugin {stream} could not be collected"),
    )
}

fn invocation_timeout(ambiguous_world_effect: bool) -> RuntimeError {
    if ambiguous_world_effect {
        RuntimeError::unknown_world(
            "effect_dispatch_timed_out",
            "world-mutating effect process timed out after starting",
        )
    } else {
        RuntimeError::timed_out(
            "process_response_timed_out",
            "process plugin response deadline elapsed",
        )
    }
}

fn invocation_cancelled(ambiguous_world_effect: bool) -> RuntimeError {
    if ambiguous_world_effect {
        RuntimeError::unknown_world(
            "effect_dispatch_cancelled",
            "world-mutating effect was cancelled after process start without an authoritative outcome",
        )
    } else {
        RuntimeError::cancelled(
            "process_invocation_cancelled",
            "the owning Engine cancelled the process occurrence",
        )
    }
}

fn validate_exit(ambiguous_world_effect: bool, status: ExitStatus) -> RuntimeResult<()> {
    if status.success() {
        return Ok(());
    }
    Err(if ambiguous_world_effect {
        RuntimeError::unknown_world(
            "effect_dispatch_response_lost",
            "world-mutating effect process exited without an authoritative response",
        )
    } else {
        RuntimeError::PluginDefect {
            code: "plugin_process_failed".to_owned(),
            message: "process plugin exited without a valid response".to_owned(),
        }
    })
}

fn write_available(
    writer: &mut ChildStdin,
    bytes: &[u8],
    offset: &mut usize,
) -> std::io::Result<bool> {
    match writer.write(&bytes[*offset..]) {
        Ok(0) => Err(std::io::Error::new(
            ErrorKind::WriteZero,
            "process stdin accepted zero bytes",
        )),
        Ok(written) => {
            *offset += written;
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}

fn read_available(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<(bool, bool)> {
    let mut buffer = [0_u8; 16 * 1024];
    match reader.read(&mut buffer) {
        Ok(0) => Ok((false, true)),
        Ok(read) => {
            if checked_bounded_end(bytes.len(), read, limit).is_none() {
                return Err(std::io::Error::new(
                    ErrorKind::FileTooLarge,
                    "process output exceeded configured limit",
                ));
            }
            bytes.extend_from_slice(&buffer[..read]);
            Ok((true, false))
        }
        Err(error) if error.kind() == ErrorKind::WouldBlock => Ok((false, false)),
        Err(error) => Err(error),
    }
}

fn checked_bounded_end(current: usize, additional: usize, limit: usize) -> Option<usize> {
    current.checked_add(additional).filter(|end| *end <= limit)
}

fn checked_chunk_end(offset: usize, total: usize, maximum_chunk: usize) -> Option<usize> {
    let remaining = total.checked_sub(offset)?;
    offset.checked_add(remaining.min(maximum_chunk))
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

#[cfg(unix)]
fn set_nonblocking(pipe: &impl std::os::fd::AsFd) -> std::io::Result<()> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};

    let current = fcntl(pipe, FcntlArg::F_GETFL).map_err(std::io::Error::other)?;
    let flags = OFlag::from_bits_truncate(current) | OFlag::O_NONBLOCK;
    fcntl(pipe, FcntlArg::F_SETFL(flags))
        .map(|_| ())
        .map_err(std::io::Error::other)
}

#[cfg(not(unix))]
fn set_nonblocking<T>(_pipe: &T) -> std::io::Result<()> {
    Err(std::io::Error::new(
        ErrorKind::Unsupported,
        "Unix nonblocking pipe semantics are required",
    ))
}

#[cfg(unix)]
fn configure_process_boundary(
    command: &mut Command,
    process_group: i32,
    execution_deadline: nix::libc::timespec,
    engine_liveness_fd: i32,
    descriptor_authority: ChildDescriptorAuthorityView,
    launch_state: *const AtomicU8,
) -> RuntimeResult<()> {
    use std::os::unix::process::CommandExt;

    if engine_liveness_fd < 3 {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process parent-liveness descriptor is outside the child descriptor domain",
        ));
    }
    #[cfg(test)]
    let hang_before_exec = HANG_PLUGIN_PRE_EXEC.load(Ordering::Acquire);
    #[cfg(test)]
    let block_before_launch = BLOCK_BEFORE_LAUNCH_GATE.load(Ordering::Acquire);
    #[cfg(test)]
    let pre_exec_ready_fd = PRE_EXEC_READY_FD.load(Ordering::Acquire);
    let launch_state_address = launch_state.expose_provenance();
    command.process_group(process_group);
    // SAFETY: this callback runs only in the post-fork child. Its body invokes
    // the reviewed syscall-only close-on-exec marker and returns without
    // allocating, locking, or observing mutable parent state.
    unsafe {
        command.pre_exec(move || {
            let launch_state = std::ptr::with_exposed_provenance::<AtomicU8>(launch_state_address);
            if nix::libc::close(engine_liveness_fd) != 0
                && nix::errno::Errno::last_raw() != nix::libc::EBADF
            {
                return Err(std::io::Error::last_os_error());
            }
            if !mark_plugin_descriptors_close_on_exec(descriptor_authority) {
                return Err(std::io::Error::last_os_error());
            }
            #[cfg(test)]
            if block_before_launch {
                if pre_exec_ready_fd >= 0 {
                    let ready = [1_u8; 1];
                    if nix::libc::write(
                        pre_exec_ready_fd,
                        ready.as_ptr().cast::<nix::libc::c_void>(),
                        ready.len(),
                    ) != 1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                while (&*launch_state).load(Ordering::SeqCst) == LAUNCH_PENDING
                    && child_execution_deadline_is_open(execution_deadline)
                {
                    let _ = nix::libc::poll(std::ptr::null_mut(), 0, 1);
                }
            }
            if !child_execution_deadline_is_open(execution_deadline) {
                transition_launch_expiration(&*launch_state);
                return Err(std::io::Error::from_raw_os_error(nix::libc::ETIMEDOUT));
            }
            match (&*launch_state).compare_exchange(
                LAUNCH_PENDING,
                LAUNCH_STARTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {}
                Err(LAUNCH_CANCELLED_BEFORE_START) => {
                    return Err(std::io::Error::from_raw_os_error(nix::libc::ECANCELED));
                }
                Err(LAUNCH_EXPIRED_BEFORE_START) => {
                    return Err(std::io::Error::from_raw_os_error(nix::libc::ETIMEDOUT));
                }
                Err(_) => {
                    return Err(std::io::Error::from_raw_os_error(nix::libc::EIO));
                }
            }
            Ok(())
        });
        #[cfg(test)]
        if hang_before_exec {
            command.pre_exec(move || {
                if pre_exec_ready_fd >= 0 {
                    let ready = [1_u8; 1];
                    if nix::libc::write(
                        pre_exec_ready_fd,
                        ready.as_ptr().cast::<nix::libc::c_void>(),
                        ready.len(),
                    ) != 1
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                loop {
                    nix::libc::pause();
                }
            });
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn configure_process_boundary(
    _command: &mut Command,
    _process_group: i32,
    _execution_deadline: (),
    _engine_liveness_fd: i32,
    _descriptor_authority: (),
    _launch_state: *const AtomicU8,
) -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires Unix child execution semantics",
    ))
}

#[cfg(unix)]
unsafe fn child_execution_deadline_is_open(deadline: nix::libc::timespec) -> bool {
    let mut now = std::mem::MaybeUninit::<nix::libc::timespec>::uninit();
    unsafe {
        nix::libc::clock_gettime(nix::libc::CLOCK_MONOTONIC, now.as_mut_ptr()) == 0
            && timespec_precedes(now.assume_init(), deadline)
    }
}

#[cfg(unix)]
const fn timespec_precedes(candidate: nix::libc::timespec, deadline: nix::libc::timespec) -> bool {
    candidate.tv_sec < deadline.tv_sec
        || (candidate.tv_sec == deadline.tv_sec && candidate.tv_nsec < deadline.tv_nsec)
}

#[cfg(all(
    unix,
    not(target_os = "linux"),
    not(target_os = "macos"),
    not(target_os = "ios")
))]
fn parent_descriptor_limit() -> RuntimeResult<i32> {
    let mut limit = std::mem::MaybeUninit::<nix::libc::rlimit>::uninit();
    // SAFETY: `limit` is writable storage for one `rlimit`; this parent-side
    // call runs before `Command::spawn` forks.
    if unsafe { nix::libc::getrlimit(nix::libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process descriptor authority could not be bounded",
        ));
    }
    // SAFETY: `getrlimit` initialized `limit` after returning success. The
    // soft limit is mutable and may already be below an inherited high FD, so
    // it is never descriptor-close authority.
    let limit = unsafe { limit.assume_init() };
    if limit.rlim_max != nix::libc::RLIM_INFINITY {
        return i32::try_from(limit.rlim_max).map_err(|_| {
            RuntimeError::substrate(
                "process_start_failed",
                "process descriptor authority exceeds the child descriptor domain",
            )
        });
    }
    Err(RuntimeError::substrate(
        "process_start_failed",
        "process descriptor hard limit is unbounded and has no platform ceiling",
    ))
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn prepare_apple_descriptor_authority() -> RuntimeResult<ChildDescriptorAuthority> {
    let kernel_ceiling = apple_descriptor_limit()?;
    let entry_bytes = size_of::<nix::libc::proc_fdinfo>();
    if entry_bytes == 0 || usize::try_from(nix::libc::PROC_PIDLISTFD_SIZE).ok() != Some(entry_bytes)
    {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor record size was invalid",
        ));
    }
    let current_entries = apple_current_descriptor_entries(entry_bytes)?;
    let ceiling_entries = usize::try_from(kernel_ceiling).map_err(|_| {
        RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor ceiling was invalid",
        )
    })?;
    let capacity_entries = current_entries
        .max(ceiling_entries)
        .checked_add(APPLE_DESCRIPTOR_QUERY_SLACK_ENTRIES)
        .ok_or_else(|| {
            RuntimeError::substrate(
                "process_start_failed",
                "process platform descriptor table exceeds the child domain",
            )
        })?;
    let buffer_bytes_usize = capacity_entries.checked_mul(entry_bytes).ok_or_else(|| {
        RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table exceeds the child domain",
        )
    })?;
    let buffer_bytes = i32::try_from(buffer_bytes_usize).map_err(|_| {
        RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table exceeds the child domain",
        )
    })?;
    let descriptor_domain_exclusive = i32::try_from(capacity_entries).map_err(|_| {
        RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table exceeds the child domain",
        )
    })?;
    let mapping = map_apple_descriptor_table(buffer_bytes_usize)?;
    Ok(ChildDescriptorAuthority {
        mapping,
        mapping_bytes: buffer_bytes_usize,
        buffer_bytes,
        descriptor_domain_exclusive,
    })
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn apple_current_descriptor_entries(entry_bytes: usize) -> RuntimeResult<usize> {
    // SAFETY: this parent-side call only observes the current process identity.
    let process_id = unsafe { nix::libc::getpid() };
    if process_id <= 0 {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor owner was invalid",
        ));
    }
    // The Apple-exported libproc interface uses a null-buffer PROC_PIDLISTFDS
    // query for the byte count needed by the current table. This runs before
    // fork, where allocation and ordinary libc behavior remain legal.
    let current_bytes = unsafe {
        nix::libc::proc_pidinfo(
            process_id,
            nix::libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        )
    };
    if current_bytes <= 0
        || usize::try_from(current_bytes)
            .ok()
            .is_none_or(|bytes| bytes % entry_bytes != 0)
    {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table size was invalid",
        ));
    }
    let current_entries = usize::try_from(current_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_div(entry_bytes))
        .ok_or_else(|| {
            RuntimeError::substrate(
                "process_start_failed",
                "process platform descriptor table size was invalid",
            )
        })?;
    Ok(current_entries)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn map_apple_descriptor_table(
    buffer_bytes: usize,
) -> RuntimeResult<NonNull<nix::libc::proc_fdinfo>> {
    // A private anonymous mapping reserves the complete parent-validated
    // capacity without eagerly touching every sparse-ceiling page. Each forked
    // child gets its own COW view and the kernel writes only the exact returned
    // prefix; the parent never mutates the mapping.
    let mapping = unsafe {
        nix::libc::mmap(
            std::ptr::null_mut(),
            buffer_bytes,
            nix::libc::PROT_READ | nix::libc::PROT_WRITE,
            nix::libc::MAP_PRIVATE | nix::libc::MAP_ANON,
            -1,
            0,
        )
    };
    if mapping == nix::libc::MAP_FAILED {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table could not be allocated",
        ));
    }
    let Some(mapping) = NonNull::new(mapping.cast::<nix::libc::proc_fdinfo>()) else {
        // SAFETY: mmap succeeded above and returned this exact mapping.
        unsafe {
            let _ = nix::libc::munmap(mapping, buffer_bytes);
        }
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor table mapping was invalid",
        ));
    };
    Ok(mapping)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn apple_descriptor_limit() -> RuntimeResult<i32> {
    let mut ceiling = 0_i32;
    let mut length = size_of::<i32>();
    let name = c"kern.maxfilesperproc";
    // SAFETY: `ceiling` and `length` describe writable parent-side storage;
    // the query performs no work in a forked child.
    if unsafe {
        nix::libc::sysctlbyname(
            name.as_ptr(),
            std::ptr::from_mut(&mut ceiling).cast(),
            &raw mut length,
            std::ptr::null_mut(),
            0,
        )
    } != 0
        || length != size_of::<i32>()
        || ceiling < 3
    {
        return Err(RuntimeError::substrate(
            "process_start_failed",
            "process platform descriptor ceiling could not be read",
        ));
    }
    Ok(ceiling)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn apple_open_descriptor_count(
    descriptor_authority: ChildDescriptorAuthorityView,
) -> Option<usize> {
    validate_apple_descriptor_buffer(descriptor_authority)?;
    let buffer = std::ptr::with_exposed_provenance_mut::<nix::libc::proc_fdinfo>(
        descriptor_authority.buffer_address,
    );
    // Apple's exported libproc proc_pidinfo wrapper (declared since macOS 10.5)
    // is a private system interface, not a Cymule protocol contract. Its
    // reviewed Apple OSS Libc implementation is one __proc_info kernel call.
    // This is the only FD-table discovery operation admitted after fork: the
    // buffer was allocated by the parent, and this branch performs no
    // allocation, lock, destructor, or directory read before exec/_exit.
    let returned_bytes = unsafe {
        nix::libc::proc_pidinfo(
            nix::libc::getpid(),
            nix::libc::PROC_PIDLISTFDS,
            0,
            buffer.cast(),
            descriptor_authority.buffer_bytes,
        )
    };
    unsafe { validate_apple_descriptor_prefix(descriptor_authority, returned_bytes) }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn validate_apple_descriptor_buffer(
    descriptor_authority: ChildDescriptorAuthorityView,
) -> Option<usize> {
    let entry_bytes = size_of::<nix::libc::proc_fdinfo>();
    let buffer_bytes = usize::try_from(descriptor_authority.buffer_bytes).ok()?;
    let capacity = buffer_bytes.checked_div(entry_bytes)?;
    if descriptor_authority.buffer_address == 0
        || descriptor_authority.descriptor_domain_exclusive < 3
        || buffer_bytes % entry_bytes != 0
        || capacity == 0
        || usize::try_from(descriptor_authority.descriptor_domain_exclusive).ok() != Some(capacity)
    {
        return None;
    }
    Some(capacity)
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn validate_apple_descriptor_prefix(
    descriptor_authority: ChildDescriptorAuthorityView,
    returned_bytes: i32,
) -> Option<usize> {
    let capacity = validate_apple_descriptor_buffer(descriptor_authority)?;
    let entry_bytes = size_of::<nix::libc::proc_fdinfo>();
    let buffer_bytes = usize::try_from(descriptor_authority.buffer_bytes).ok()?;
    let returned_bytes = usize::try_from(returned_bytes).ok()?;
    if returned_bytes == 0 || returned_bytes >= buffer_bytes || returned_bytes % entry_bytes != 0 {
        return None;
    }
    let count = returned_bytes.checked_div(entry_bytes)?;
    if count > capacity {
        return None;
    }
    let buffer = std::ptr::with_exposed_provenance_mut::<nix::libc::proc_fdinfo>(
        descriptor_authority.buffer_address,
    );
    let mut previous = -1;
    for index in 0..count {
        // SAFETY: the kernel reported an aligned prefix within the parent-sized
        // buffer. Strictly increasing descriptors reject duplicates and any
        // malformed or out-of-order result before a descriptor is mutated.
        let descriptor = unsafe { (*buffer.add(index)).proc_fd };
        if descriptor < 0
            || descriptor >= descriptor_authority.descriptor_domain_exclusive
            || descriptor <= previous
        {
            return None;
        }
        previous = descriptor;
    }
    Some(count)
}

#[cfg(target_os = "linux")]
unsafe fn mark_plugin_descriptors_close_on_exec(
    _descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    unsafe {
        nix::libc::syscall(
            nix::libc::SYS_close_range,
            3_u32,
            u32::MAX,
            nix::libc::CLOSE_RANGE_CLOEXEC,
        ) == 0
    }
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn mark_plugin_descriptors_close_on_exec(
    descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    let Some(descriptor_count) = (unsafe { apple_open_descriptor_count(descriptor_authority) })
    else {
        return false;
    };
    let descriptors = std::ptr::with_exposed_provenance::<nix::libc::proc_fdinfo>(
        descriptor_authority.buffer_address,
    );
    for index in 0..descriptor_count {
        // SAFETY: `apple_open_descriptor_count` authenticated this initialized
        // prefix and every index remains inside that exact prefix.
        let descriptor = unsafe { (*descriptors.add(index)).proc_fd };
        if descriptor < 3 {
            continue;
        }
        let flags = unsafe { nix::libc::fcntl(descriptor, nix::libc::F_GETFD) };
        if flags < 0 {
            return false;
        }
        if flags & nix::libc::FD_CLOEXEC == 0
            && unsafe {
                nix::libc::fcntl(
                    descriptor,
                    nix::libc::F_SETFD,
                    flags | nix::libc::FD_CLOEXEC,
                )
            } != 0
        {
            return false;
        }
    }
    true
}

#[cfg(all(
    unix,
    not(target_os = "linux"),
    not(target_os = "macos"),
    not(target_os = "ios")
))]
unsafe fn mark_plugin_descriptors_close_on_exec(
    descriptor_authority: ChildDescriptorAuthorityView,
) -> bool {
    let descriptor_limit = descriptor_authority.descriptor_limit;
    unsafe {
        for descriptor in 3..descriptor_limit {
            let flags = nix::libc::fcntl(descriptor, nix::libc::F_GETFD);
            if flags < 0 && nix::errno::Errno::last_raw() == nix::libc::EBADF {
                continue;
            }
            if flags < 0
                || nix::libc::fcntl(
                    descriptor,
                    nix::libc::F_SETFD,
                    flags | nix::libc::FD_CLOEXEC,
                ) != 0
            {
                return false;
            }
        }
        true
    }
}

#[cfg(unix)]
fn kill_process_group(process_group: i32) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;

    if process_group > 0 {
        let _ = killpg(Pid::from_raw(process_group), Signal::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_process_group: i32) {}

fn terminate_process_tree(child: &mut Child, supervisor: &mut ProcessGroupSupervisor) {
    supervisor.kill_group();
    let _ = child.kill();
    let _ = child.wait();
    supervisor.wait();
}

#[cfg(unix)]
fn executable_mode(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o111 == 0 {
        0o600
    } else {
        0o700
    }
}

#[cfg(not(unix))]
fn executable_mode(_metadata: &fs::Metadata) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::{checked_bounded_end, checked_chunk_end};

    #[cfg(unix)]
    fn runtime_closure() -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([(
            "test-runtime".to_owned(),
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
        )])
    }

    #[test]
    fn byte_limits_reject_overflow_instead_of_clamping_it() {
        assert_eq!(
            checked_bounded_end(usize::MAX - 1, 1, usize::MAX),
            Some(usize::MAX)
        );
        assert_eq!(checked_bounded_end(usize::MAX - 1, 2, usize::MAX), None);
        assert_eq!(checked_bounded_end(1, usize::MAX, usize::MAX), None);
        assert_eq!(checked_bounded_end(8, 1, 8), None);
    }

    #[test]
    fn chunk_end_is_exact_at_the_address_space_boundary() {
        assert_eq!(
            checked_chunk_end(usize::MAX - 1, usize::MAX, 64 * 1024),
            Some(usize::MAX)
        );
        assert_eq!(
            checked_chunk_end(usize::MAX, usize::MAX, 64 * 1024),
            Some(usize::MAX)
        );
        assert_eq!(checked_chunk_end(usize::MAX, usize::MAX - 1, 1), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn apple_descriptor_table_is_exact_bounded_and_rejects_malformed_prefixes() {
        use super::{
            APPLE_DESCRIPTOR_QUERY_SLACK_ENTRIES, ChildDescriptorAuthority,
            ChildDescriptorAuthorityView, apple_open_descriptor_count,
            validate_apple_descriptor_prefix,
        };
        use nix::fcntl::{FcntlArg, fcntl};
        use std::mem::size_of;
        use std::os::fd::{FromRawFd, OwnedFd};

        let source = std::fs::File::open("/dev/null").expect("descriptor source opens");
        let high_fd = fcntl(&source, FcntlArg::F_DUPFD(512))
            .expect("high descriptor duplicates for exact-table evidence");
        // SAFETY: F_DUPFD returned this newly owned descriptor.
        let high = unsafe { OwnedFd::from_raw_fd(high_fd) };

        let mut authority = ChildDescriptorAuthority::prepare()
            .expect("Apple descriptor authority prepares in the parent");
        let buffer_bytes = authority.buffer_bytes;
        let capacity = usize::try_from(buffer_bytes).expect("buffer bytes are positive")
            / size_of::<nix::libc::proc_fdinfo>();
        let domain = usize::try_from(authority.descriptor_domain_exclusive)
            .expect("descriptor domain is positive");
        assert_eq!(capacity, domain);
        assert!(capacity >= APPLE_DESCRIPTOR_QUERY_SLACK_ENTRIES + 3);
        let view = authority.view();
        // SAFETY: this test runs in the ordinary parent and supplies the
        // parent-allocated table represented by `view`.
        let count = unsafe { apple_open_descriptor_count(view) }
            .expect("Apple returns one exact open-descriptor prefix");
        assert!(
            count < capacity,
            "the slack proves the table was not truncated"
        );
        // SAFETY: the parent owns this complete initialized anonymous mapping.
        let entries =
            unsafe { std::slice::from_raw_parts_mut(authority.mapping.as_ptr(), capacity) };
        assert!(
            entries[..count]
                .iter()
                .any(|entry| entry.proc_fd == high_fd),
            "the exact table includes an inherited high descriptor"
        );
        assert_eq!(
            entries[count].proc_fd, 0,
            "the kernel query writes only its reported prefix, not the capacity ceiling"
        );

        let returned_bytes = i32::try_from(
            count
                .checked_mul(size_of::<nix::libc::proc_fdinfo>())
                .expect("returned prefix byte size fits usize"),
        )
        .expect("returned prefix byte size fits c_int");
        let original_first = entries[0].proc_fd;
        let original_second = entries[1].proc_fd;
        entries[1].proc_fd = original_first;
        // SAFETY: the synthetic returned byte count is within this owned table.
        assert!(unsafe { validate_apple_descriptor_prefix(view, returned_bytes) }.is_none());
        entries[1].proc_fd = original_second;
        entries[0].proc_fd = i32::try_from(domain).expect("descriptor domain fits c_int");
        // A descriptor remains valid when a mutable kernel ceiling later falls;
        // the parent-sized table domain, not that later ceiling, is authority.
        let lowered_kernel_ceiling = high_fd.saturating_sub(1);
        assert!(lowered_kernel_ceiling < high_fd);
        assert!(high_fd < i32::try_from(domain).expect("descriptor domain fits c_int"));
        // SAFETY: the out-of-domain synthetic descriptor must fail closed.
        assert!(unsafe { validate_apple_descriptor_prefix(view, returned_bytes) }.is_none());
        entries[0].proc_fd = original_first;
        // SAFETY: unaligned and capacity-sized byte counts cannot describe an
        // exact initialized prefix.
        assert!(unsafe { validate_apple_descriptor_prefix(view, 0) }.is_none());
        assert!(unsafe { validate_apple_descriptor_prefix(view, -1) }.is_none());
        assert!(unsafe { validate_apple_descriptor_prefix(view, returned_bytes - 1) }.is_none());
        assert!(unsafe { validate_apple_descriptor_prefix(view, buffer_bytes) }.is_none());

        drop(high);

        let narrow_view = ChildDescriptorAuthorityView {
            buffer_address: view.buffer_address,
            buffer_bytes: view.buffer_bytes,
            descriptor_domain_exclusive: lowered_kernel_ceiling,
        };
        // SAFETY: a synthetic later-lowered kernel domain no longer equals the
        // retained buffer capacity and therefore cannot reinterpret it.
        assert!(unsafe { validate_apple_descriptor_prefix(narrow_view, returned_bytes) }.is_none());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn concurrent_apple_descriptor_discovery_visits_only_open_entries() {
        use super::{ChildDescriptorAuthority, apple_open_descriptor_count};
        use std::sync::{Arc, Barrier};

        const WORKERS: usize = 16;
        const ROUNDS: usize = 3;

        for round_index in 0..ROUNDS {
            let gate = Arc::new(Barrier::new(WORKERS + 1));
            std::thread::scope(|scope| {
                let handles = (0..WORKERS)
                    .map(|_| {
                        let gate = gate.clone();
                        scope.spawn(move || {
                            gate.wait();
                            let mut authority = ChildDescriptorAuthority::prepare()
                                .expect("Apple descriptor authority prepares concurrently");
                            let capacity = usize::try_from(authority.descriptor_domain_exclusive)
                                .expect("Apple descriptor domain is positive");
                            let view = authority.view();
                            // SAFETY: this ordinary parent thread owns the
                            // complete mapping represented by `view`.
                            let count = unsafe { apple_open_descriptor_count(view) }
                                .expect("Apple exact descriptor query succeeds concurrently");
                            (count, capacity)
                        })
                    })
                    .collect::<Vec<_>>();
                gate.wait();
                for (worker, handle) in handles.into_iter().enumerate() {
                    let (count, capacity) = handle.join().expect("Apple descriptor worker joins");
                    assert!(
                        count > 0 && count < capacity,
                        "concurrent Apple descriptor worker {worker} round {round_index} returned {count} entries for capacity {capacity}"
                    );
                }
            });
        }
    }

    #[cfg(unix)]
    #[test]
    fn watchdog_group_is_parent_established_before_start_returns() {
        use super::{ProcessCancellation, ProcessGroupSupervisor};
        use nix::unistd::{Pid, getpgid};
        use std::time::{Duration, Instant};

        let cancellation = ProcessCancellation::new().expect("cancellation authority creates");
        let launch = cancellation
            .register_launch()
            .expect("launch authority registers");
        let mut supervisor =
            ProcessGroupSupervisor::start(Instant::now() + Duration::from_secs(2), &launch)
                .expect("watchdog group starts");
        let watchdog = supervisor.process_group();
        assert_eq!(
            getpgid(Some(Pid::from_raw(watchdog))).expect("watchdog group reads"),
            Pid::from_raw(watchdog),
            "the exact watchdog PID is already its process-group authority"
        );
        supervisor.terminate();
    }

    #[cfg(unix)]
    #[test]
    fn opened_directory_fd_remains_the_capture_authority_after_path_replacement() {
        use super::{ClosureBudget, capture_open_directory, open_absolute_directory};
        use std::fs;
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().expect("fixture directory creates");
        let configured = fixture.path().join("configured");
        let retained = fixture.path().join("retained");
        let outside = fixture.path().join("outside");
        fs::create_dir(&configured).expect("configured directory creates");
        fs::create_dir(&outside).expect("outside directory creates");
        fs::write(configured.join("value"), b"captured").expect("captured value writes");
        fs::write(outside.join("value"), b"escaped").expect("outside value writes");

        let resolved = fs::canonicalize(&configured).expect("configured path resolves");
        let authority = open_absolute_directory(&resolved).expect("directory authority opens");
        fs::rename(&configured, &retained).expect("opened directory moves");
        symlink(&outside, &configured).expect("configured path is replaced");

        let mut budget = ClosureBudget::new(1024 * 1024);
        let captured =
            capture_open_directory(authority, &mut budget).expect("open authority captures");
        assert_eq!(captured.files.len(), 1);
        assert_eq!(captured.files[0].relative_path, "value");
        assert_eq!(captured.files[0].bytes, b"captured");
    }

    #[cfg(unix)]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn deep_single_chain_capture_is_stack_safe_and_deterministic() {
        use super::{
            CapturedDirectoryEntryIdentity, CapturedDirectoryIdentity, ClosureBudget,
            DEFAULT_PROCESS_CLOSURE_LIMIT, PRIVATE_DIRECTORY_MODE,
            PROCESS_WORKING_DIRECTORY_ID_DOMAIN, canonical_bytes, capture_open_directory,
            open_absolute_directory, sha256_bytes,
        };
        use cymule_runtime::PluginHost;
        use nix::dir::Dir;
        use nix::fcntl::OFlag;
        use nix::sys::stat::{Mode, mkdirat};
        use nix::unistd::{UnlinkatFlags, unlinkat};
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant};

        const DEPTH: usize = 1_024;
        const TEST_STACK_BYTES: usize = 128 * 1024;

        struct DeepDirectoryChain {
            directories: Vec<Dir>,
        }

        impl DeepDirectoryChain {
            fn create(root: &std::path::Path, depth: usize) -> Self {
                let flags = OFlag::O_RDONLY
                    | OFlag::O_DIRECTORY
                    | OFlag::O_CLOEXEC
                    | OFlag::O_NOFOLLOW
                    | OFlag::O_NONBLOCK;
                let mut directories = Vec::with_capacity(depth + 1);
                directories.push(Dir::open(root, flags, Mode::empty()).expect("chain root opens"));
                for _ in 0..depth {
                    let parent = directories.last().expect("chain retains its parent");
                    mkdirat(parent, "d", Mode::S_IRWXU).expect("chain directory creates");
                    directories.push(
                        Dir::openat(parent, "d", flags, Mode::empty())
                            .expect("chain directory opens"),
                    );
                }
                Self { directories }
            }
        }

        impl Drop for DeepDirectoryChain {
            fn drop(&mut self) {
                drop(self.directories.pop());
                while let Some(parent) = self.directories.pop() {
                    let _ = unlinkat(&parent, "d", UnlinkatFlags::RemoveDir);
                }
            }
        }

        let fixture = tempfile::tempdir().expect("fixture directory creates");
        let chain = DeepDirectoryChain::create(fixture.path(), DEPTH);
        let resolved = std::fs::canonicalize(fixture.path()).expect("fixture root resolves");
        let capture = |name: &str| {
            let root = resolved.clone();
            std::thread::Builder::new()
                .name(name.to_owned())
                .stack_size(TEST_STACK_BYTES)
                .spawn(move || {
                    let mut budget = ClosureBudget::new(DEFAULT_PROCESS_CLOSURE_LIMIT);
                    capture_open_directory(
                        open_absolute_directory(&root).expect("root authority opens"),
                        &mut budget,
                    )
                    .expect("deep chain captures without recursive stack growth")
                })
                .expect("capture thread starts")
                .join()
                .expect("capture thread finishes")
        };
        let first = capture("deep-directory-capture-1");
        let second = capture("deep-directory-capture-2");

        let mut path = String::new();
        let mut expected_directories = Vec::with_capacity(DEPTH);
        for _ in 0..DEPTH {
            if !path.is_empty() {
                path.push('/');
            }
            path.push('d');
            expected_directories.push(path.clone());
        }
        let expected_directory_identities = expected_directories
            .iter()
            .map(|path| CapturedDirectoryEntryIdentity {
                path,
                mode: PRIVATE_DIRECTORY_MODE,
            })
            .collect();
        let expected_identity = CapturedDirectoryIdentity {
            version: PROCESS_WORKING_DIRECTORY_ID_DOMAIN,
            root_mode: PRIVATE_DIRECTORY_MODE,
            directory_mode: PRIVATE_DIRECTORY_MODE,
            directories: expected_directory_identities,
            files: Vec::new(),
        };
        let expected_identity = format!(
            "sha256:{}",
            sha256_bytes(
                &canonical_bytes(&expected_identity).expect("expected identity canonicalizes")
            )
        );

        assert_eq!(
            first
                .directories
                .iter()
                .map(|directory| directory.relative_path.as_str())
                .collect::<Vec<_>>(),
            expected_directories
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        assert!(first.files.is_empty());
        assert_eq!(first.directories, second.directories);
        assert_eq!(first.identity, expected_identity);
        assert_eq!(first.identity, second.identity);

        let low_stack_root = resolved.clone();
        std::thread::Builder::new()
            .name("deep-directory-low-stack-materialization".to_owned())
            .stack_size(TEST_STACK_BYTES)
            .spawn(move || {
                let mut config = super::ProcessExecutorConfig::new("/bin/sh", runtime_closure());
                config.working_directory = Some(low_stack_root);
                config.timeout = Duration::from_secs(30);
                let executor = super::ProcessExecutor::new(config)
                    .expect("low-stack executor captures the deep directory");
                let deadline = Instant::now() + Duration::from_secs(30);
                let invocation = executor
                    .materialize_invocation(deadline)
                    .expect("deep directory materializes on the bounded stack");
                super::finish_invocation(invocation, Ok(Vec::new()))
                    .expect("deep directory reclaims on the bounded stack");
            })
            .expect("low-stack materialization thread starts")
            .join()
            .expect("low-stack materialization thread finishes");

        let plugin_fixture = tempfile::tempdir().expect("plugin fixture creates");
        let plugin = plugin_fixture.path().join("plugin.sh");
        std::fs::write(
            &plugin,
            "#!/bin/sh\n/bin/cat >/dev/null\nprintf '%s' '{\"type\":\"manifest\",\"manifest\":{\"plugin_version\":\"cymule.plugin/3\",\"implementation_id\":\"process:deep\",\"components\":{},\"effects\":{}}}'\n",
        )
        .expect("plugin fixture writes");
        std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o700))
            .expect("plugin fixture executes");
        let mut config = super::ProcessExecutorConfig::new(plugin, runtime_closure());
        config.working_directory = Some(resolved);
        config.message_limit = cymule_runtime::MAX_PLUGIN_MESSAGE_BYTES;
        config.timeout = Duration::from_secs(30);
        let mut executor = super::ProcessExecutor::new(config)
            .expect("public executor captures the deep directory");
        drop(chain);
        let manifest = executor
            .describe()
            .expect("deep captured directory materializes, executes, and reclaims");
        assert_eq!(manifest.implementation_id, "process:deep");
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_cleanup_unlinks_symlinks_and_recovers_zero_mode_directories() {
        use super::{PrivateInvocationDirectory, private_directory_open_flags};
        use nix::dir::Dir;
        use nix::sys::stat::{Mode, fchmod, mkdirat};
        use nix::unistd::symlinkat;

        let outside = tempfile::tempdir().expect("outside fixture creates");
        let sentinel = outside.path().join("sentinel");
        std::fs::write(&sentinel, b"retained").expect("outside sentinel writes");
        let invocation = PrivateInvocationDirectory::create().expect("invocation root creates");
        let invocation_path = invocation.path().to_path_buf();
        symlinkat(&sentinel, invocation.root(), "outside-link").expect("symlink creates");
        mkdirat(invocation.root(), "locked", Mode::S_IRWXU).expect("locked directory creates");
        let locked = Dir::openat(
            invocation.root(),
            "locked",
            private_directory_open_flags(),
            Mode::empty(),
        )
        .expect("locked directory opens");
        mkdirat(&locked, "nested", Mode::S_IRWXU).expect("nested directory creates");
        fchmod(&locked, Mode::empty()).expect("locked directory mode removes");
        fchmod(invocation.root(), Mode::empty()).expect("root mode removes");

        invocation.close().expect("descriptor cleanup succeeds");
        assert!(!invocation_path.exists(), "occurrence root is reclaimed");
        assert_eq!(
            std::fs::read(&sentinel).expect("outside sentinel remains"),
            b"retained"
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialized_modes_are_fixed_across_umasks() {
        for mask in ["000", "077", "777"] {
            let status = std::process::Command::new(
                std::env::current_exe().expect("unit test executable resolves"),
            )
            .arg("--exact")
            .arg("tests::umask_materialization_helper")
            .env("CYMULE_TEST_PROCESS_UMASK", mask)
            .status()
            .expect("umask helper starts");
            assert!(status.success(), "umask helper failed for {mask}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn umask_materialization_helper() {
        use super::{
            PRIVATE_DIRECTORY_MODE, ProcessExecutor, ProcessExecutorConfig, SEALED_EXECUTABLE_MODE,
            private_directory_open_flags,
        };
        use nix::fcntl::AtFlags;
        use nix::sys::stat::{Mode, fstat, fstatat, umask};
        use std::os::unix::fs::PermissionsExt;
        use std::time::{Duration, Instant};

        let Some(mask) = std::env::var_os("CYMULE_TEST_PROCESS_UMASK") else {
            return;
        };
        let mask = u32::from_str_radix(&mask.to_string_lossy(), 8).expect("umask parses");
        let mask = nix::libc::mode_t::try_from(mask).expect("umask fits mode_t");
        let source = tempfile::tempdir().expect("source fixture creates");
        let nested = source.path().join("nested");
        std::fs::create_dir(&nested).expect("nested source creates");
        std::fs::write(nested.join("plain"), b"plain").expect("plain source writes");
        std::fs::write(nested.join("tool"), b"tool").expect("tool source writes");
        std::fs::set_permissions(&nested, std::fs::Permissions::from_mode(0o555))
            .expect("source directory mode sets");
        std::fs::set_permissions(nested.join("tool"), std::fs::Permissions::from_mode(0o755))
            .expect("source tool mode sets");
        let mut config = ProcessExecutorConfig::new("/bin/sh", runtime_closure());
        config.working_directory = Some(source.path().to_path_buf());
        let executor = ProcessExecutor::new(config).expect("executor captures source");
        umask(Mode::from_bits_truncate(mask));
        let deadline = Instant::now() + Duration::from_secs(10);
        let invocation = executor
            .materialize_invocation(deadline)
            .expect("invocation materializes under umask");
        let root = fstat(invocation.directory.root()).expect("root mode reads");
        assert_eq!(u32::from(root.st_mode & 0o7777), PRIVATE_DIRECTORY_MODE);
        let plugin = fstatat(
            invocation.directory.root(),
            "plugin",
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .expect("plugin mode reads");
        assert_eq!(u32::from(plugin.st_mode & 0o7777), SEALED_EXECUTABLE_MODE);
        let cwd = nix::dir::Dir::openat(
            invocation.directory.root(),
            "cwd",
            private_directory_open_flags(),
            Mode::empty(),
        )
        .expect("cwd opens");
        assert_eq!(
            u32::from(fstat(&cwd).expect("cwd mode reads").st_mode & 0o7777),
            PRIVATE_DIRECTORY_MODE
        );
        let nested = nix::dir::Dir::openat(
            &cwd,
            "nested",
            private_directory_open_flags(),
            Mode::empty(),
        )
        .expect("nested cwd opens");
        assert_eq!(
            u32::from(fstat(&nested).expect("nested mode reads").st_mode & 0o7777),
            PRIVATE_DIRECTORY_MODE
        );
        assert_eq!(
            u32::from(
                fstatat(&nested, "plain", AtFlags::AT_SYMLINK_NOFOLLOW)
                    .expect("plain mode reads")
                    .st_mode
                    & 0o7777
            ),
            0o600
        );
        assert_eq!(
            u32::from(
                fstatat(&nested, "tool", AtFlags::AT_SYMLINK_NOFOLLOW)
                    .expect("tool mode reads")
                    .st_mode
                    & 0o7777
            ),
            0o700
        );
        drop(nested);
        drop(cwd);
        super::finish_invocation(invocation, Ok(Vec::new())).expect("umask invocation reclaims");
    }

    #[cfg(unix)]
    #[test]
    fn lowered_limits_cannot_leak_an_existing_high_descriptor() {
        let status = std::process::Command::new(
            std::env::current_exe().expect("unit test executable resolves"),
        )
        .arg("--exact")
        .arg("tests::lowered_limits_high_fd_helper")
        .env("CYMULE_TEST_LOWERED_FD_LIMITS", "1")
        .status()
        .expect("high-FD helper starts");
        assert!(status.success(), "high-FD helper failed");
    }

    #[cfg(unix)]
    #[test]
    fn lowered_limits_high_fd_helper() {
        use super::{ProcessExecutor, ProcessExecutorConfig};
        use cymule_runtime::PluginHost;
        use nix::fcntl::{FcntlArg, FdFlag, fcntl};
        use std::os::fd::BorrowedFd;
        use std::os::unix::fs::PermissionsExt;

        if std::env::var_os("CYMULE_TEST_LOWERED_FD_LIMITS").is_none() {
            return;
        }
        let fixture = tempfile::tempdir().expect("high-FD fixture creates");
        let plugin = fixture.path().join("plugin.sh");
        std::fs::write(
            &plugin,
            "#!/bin/sh\ntest ! -e \"/dev/fd/$1\" || exit 91\n/bin/cat >/dev/null\nprintf '%s' '{\"type\":\"manifest\",\"manifest\":{\"plugin_version\":\"cymule.plugin/3\",\"implementation_id\":\"process:high-fd-closed\",\"components\":{},\"effects\":{}}}'\n",
        )
        .expect("high-FD plugin writes");
        std::fs::set_permissions(&plugin, std::fs::Permissions::from_mode(0o700))
            .expect("high-FD plugin executes");
        let source = std::fs::File::open("/dev/null").expect("descriptor source opens");
        let high_fd = fcntl(&source, FcntlArg::F_DUPFD(512)).expect("high descriptor duplicates");
        // SAFETY: F_DUPFD returned this descriptor open for the helper lifetime.
        let high = unsafe { BorrowedFd::borrow_raw(high_fd) };
        fcntl(high, FcntlArg::F_SETFD(FdFlag::empty())).expect("high FD clears CLOEXEC");
        let mut config = ProcessExecutorConfig::new(plugin, runtime_closure());
        config.arguments = vec![high_fd.to_string()];
        let mut executor = ProcessExecutor::new(config).expect("high-FD executor captures");
        let mut limit = std::mem::MaybeUninit::<nix::libc::rlimit>::uninit();
        // SAFETY: this isolated helper owns writable storage for one rlimit.
        assert_eq!(
            unsafe { nix::libc::getrlimit(nix::libc::RLIMIT_NOFILE, limit.as_mut_ptr()) },
            0
        );
        // SAFETY: getrlimit succeeded above.
        let mut limit = unsafe { limit.assume_init() };
        limit.rlim_cur = 64;
        limit.rlim_max = 64;
        // SAFETY: this irreversibly lowers both limits only in the isolated
        // helper, after the descriptor above the new hard limit is open.
        assert_eq!(
            unsafe { nix::libc::setrlimit(nix::libc::RLIMIT_NOFILE, &raw const limit) },
            0
        );
        assert_eq!(
            executor
                .describe()
                .expect("high descriptor is excluded from exec")
                .implementation_id,
            "process:high-fd-closed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn watchdog_deadline_interrupts_a_child_blocked_before_exec() {
        use super::{
            HANG_PLUGIN_PRE_EXEC, PRE_EXEC_TEST_MUTEX, ProcessExecutor, ProcessExecutorConfig,
        };
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        struct ResetPreExecHang;
        impl Drop for ResetPreExecHang {
            fn drop(&mut self) {
                HANG_PLUGIN_PRE_EXEC.store(false, Ordering::Release);
            }
        }

        let _test_authority = PRE_EXEC_TEST_MUTEX
            .lock()
            .expect("pre-exec test authority locks");
        let mut config = ProcessExecutorConfig::new("/bin/sh", runtime_closure());
        config.arguments = vec!["-c".to_owned(), "exit 0".to_owned()];
        config.timeout = Duration::from_millis(50);
        let executor = ProcessExecutor::new(config).expect("executor captures shell");
        HANG_PLUGIN_PRE_EXEC.store(true, Ordering::Release);
        let _reset = ResetPreExecHang;
        let started = Instant::now();

        assert!(matches!(
            executor.invoke_bytes(b"{}", false, executor.config.message_limit),
            Err(cymule_runtime::RuntimeError::TimedOut { code, .. })
                if code == "process_response_timed_out"
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the pre-exec child remained beyond the absolute watchdog deadline"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_after_launch_gate_is_retained_as_unknown_for_mutation() {
        use super::{
            HANG_PLUGIN_PRE_EXEC, PRE_EXEC_READY_FD, PRE_EXEC_TEST_MUTEX, ProcessCancellation,
            ProcessExecutor, ProcessExecutorConfig,
        };
        use std::io::Read;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        struct ResetPreExecHang;
        impl Drop for ResetPreExecHang {
            fn drop(&mut self) {
                HANG_PLUGIN_PRE_EXEC.store(false, Ordering::Release);
                PRE_EXEC_READY_FD.store(-1, Ordering::Release);
            }
        }

        let _test_authority = PRE_EXEC_TEST_MUTEX
            .lock()
            .expect("pre-exec test authority locks");
        let cancellation = ProcessCancellation::new().expect("cancellation authority creates");
        let mut config = ProcessExecutorConfig::new("/bin/sh", runtime_closure());
        config.arguments = vec!["-c".to_owned(), "exit 0".to_owned()];
        config.timeout = Duration::from_secs(3);
        config.cancellation = Some(cancellation.clone());
        let executor = ProcessExecutor::new(config).expect("executor captures shell");
        let (mut readiness, ready_writer) = UnixStream::pair().expect("readiness pair creates");
        PRE_EXEC_READY_FD.store(ready_writer.as_raw_fd(), Ordering::Release);
        HANG_PLUGIN_PRE_EXEC.store(true, Ordering::Release);
        let _reset = ResetPreExecHang;
        let trigger = std::thread::spawn(move || {
            let mut ready = [0_u8; 1];
            readiness
                .read_exact(&mut ready)
                .expect("launch gate reports readiness");
            cancellation.cancel();
        });
        let started = Instant::now();

        assert!(matches!(
            executor.invoke_bytes(b"{}", true, executor.config.message_limit),
            Err(cymule_runtime::RuntimeError::UnknownWorld { code, .. })
                if code == "effect_dispatch_cancelled"
        ));
        drop(ready_writer);
        trigger.join().expect("cancellation trigger joins");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "post-gate cancellation waited for the provider deadline"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_before_launch_gate_performs_no_provider_io() {
        use super::{
            BLOCK_BEFORE_LAUNCH_GATE, PRE_EXEC_READY_FD, PRE_EXEC_TEST_MUTEX, ProcessCancellation,
            ProcessExecutor, ProcessExecutorConfig,
        };
        use std::io::Read;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        struct ResetLaunchGate;
        impl Drop for ResetLaunchGate {
            fn drop(&mut self) {
                BLOCK_BEFORE_LAUNCH_GATE.store(false, Ordering::Release);
                PRE_EXEC_READY_FD.store(-1, Ordering::Release);
            }
        }

        let _test_authority = PRE_EXEC_TEST_MUTEX
            .lock()
            .expect("pre-exec test authority locks");
        let cancellation = ProcessCancellation::new().expect("cancellation authority creates");
        let mut config = ProcessExecutorConfig::new("/bin/sh", runtime_closure());
        config.arguments = vec!["-c".to_owned(), "exit 97".to_owned()];
        config.timeout = Duration::from_secs(3);
        config.cancellation = Some(cancellation.clone());
        let executor = ProcessExecutor::new(config).expect("executor captures shell");
        let (mut readiness, ready_writer) = UnixStream::pair().expect("readiness pair creates");
        PRE_EXEC_READY_FD.store(ready_writer.as_raw_fd(), Ordering::Release);
        BLOCK_BEFORE_LAUNCH_GATE.store(true, Ordering::Release);
        let _reset = ResetLaunchGate;
        let trigger = std::thread::spawn(move || {
            let mut ready = [0_u8; 1];
            readiness
                .read_exact(&mut ready)
                .expect("launch gate reports readiness");
            cancellation.cancel();
        });

        assert!(matches!(
            executor.invoke_bytes(b"{}", true, executor.config.message_limit),
            Err(cymule_runtime::RuntimeError::Cancelled { code, .. })
                if code == "process_invocation_cancelled"
        ));
        drop(ready_writer);
        trigger.join().expect("cancellation trigger joins");
    }

    #[cfg(unix)]
    #[test]
    fn parent_death_during_pre_exec_helper() {
        use super::{HANG_PLUGIN_PRE_EXEC, PRE_EXEC_GROUP_MARKER, PRE_EXEC_READY_FD};
        use std::path::PathBuf;
        use std::sync::atomic::Ordering;

        let Some(marker) = std::env::var_os("CYMULE_TEST_PRE_EXEC_GROUP_MARKER") else {
            return;
        };
        let concurrency: usize = std::env::var("CYMULE_TEST_PRE_EXEC_CONCURRENCY")
            .expect("helper receives concurrency")
            .parse()
            .expect("helper concurrency is a count");
        assert!((1..=2).contains(&concurrency));
        let ready_fd = std::env::var("CYMULE_TEST_PRE_EXEC_READY_FD")
            .expect("helper receives readiness descriptor")
            .parse()
            .expect("helper readiness descriptor is an integer");
        PRE_EXEC_GROUP_MARKER
            .set(PathBuf::from(marker))
            .expect("helper installs one group marker");
        PRE_EXEC_READY_FD.store(ready_fd, Ordering::Release);
        HANG_PLUGIN_PRE_EXEC.store(true, Ordering::Release);
        std::thread::scope(|scope| {
            for _ in 0..concurrency {
                scope.spawn(invoke_pre_exec_hang);
            }
        });
    }

    #[cfg(unix)]
    fn invoke_pre_exec_hang() {
        use super::{ProcessExecutor, ProcessExecutorConfig};
        use std::time::Duration;

        let mut config = ProcessExecutorConfig::new("/bin/sh", runtime_closure());
        config.arguments = vec!["-c".to_owned(), "exit 0".to_owned()];
        config.timeout = Duration::from_secs(10);
        let executor = ProcessExecutor::new(config).expect("helper executor captures shell");
        let _ = executor.invoke_bytes(b"{}", false, executor.config.message_limit);
    }

    #[cfg(unix)]
    #[test]
    fn parent_sigkill_during_pre_exec_promptly_terminates_the_exact_group() {
        assert_parent_sigkill_closes_pre_exec_groups(1);
    }

    #[cfg(unix)]
    #[test]
    fn parent_sigkill_closes_two_mutually_inherited_pre_exec_groups() {
        assert_parent_sigkill_closes_pre_exec_groups(2);
    }

    #[cfg(unix)]
    fn assert_parent_sigkill_closes_pre_exec_groups(concurrency: usize) {
        let mut engine = start_pre_exec_engine(concurrency);
        wait_for_pre_exec_readiness(&mut engine.readiness, concurrency);
        let process_groups = read_process_groups(&engine.group_directory, concurrency);
        kill_engine(&mut engine.child);
        for process_group in process_groups {
            wait_for_process_group_reap(process_group);
        }
    }

    #[cfg(unix)]
    struct PreExecEngine {
        _fixture: tempfile::TempDir,
        child: std::process::Child,
        readiness: std::os::unix::net::UnixStream,
        group_directory: std::path::PathBuf,
    }

    #[cfg(unix)]
    fn start_pre_exec_engine(concurrency: usize) -> PreExecEngine {
        use nix::fcntl::{FcntlArg, FdFlag, fcntl};
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::process::{Command, Stdio};

        let fixture = tempfile::tempdir().expect("fixture directory creates");
        let group_directory = fixture.path().join("process-groups");
        std::fs::create_dir(&group_directory).expect("process-group directory creates");
        let (readiness, readiness_writer) =
            UnixStream::pair().expect("pre-exec readiness channel creates");
        readiness
            .set_nonblocking(true)
            .expect("pre-exec readiness channel is nonblocking");
        fcntl(&readiness_writer, FcntlArg::F_SETFD(FdFlag::empty()))
            .expect("helper readiness writer survives helper exec");
        let mut engine = Command::new(std::env::current_exe().expect("test executable resolves"));
        engine
            .arg("--exact")
            .arg("tests::parent_death_during_pre_exec_helper")
            .arg("--nocapture")
            .env("CYMULE_TEST_PRE_EXEC_GROUP_MARKER", &group_directory)
            .env("CYMULE_TEST_PRE_EXEC_CONCURRENCY", concurrency.to_string())
            .env(
                "CYMULE_TEST_PRE_EXEC_READY_FD",
                readiness_writer.as_raw_fd().to_string(),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = engine.spawn().expect("helper Engine starts");
        drop(readiness_writer);
        PreExecEngine {
            _fixture: fixture,
            child,
            readiness,
            group_directory,
        }
    }

    #[cfg(unix)]
    fn wait_for_pre_exec_readiness(
        readiness: &mut std::os::unix::net::UnixStream,
        expected: usize,
    ) {
        use std::io::{ErrorKind, Read};
        use std::time::{Duration, Instant};

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut observed = 0usize;
        let mut buffer = [0_u8; 2];
        while observed < expected && Instant::now() < deadline {
            match readiness.read(&mut buffer[observed..expected]) {
                Ok(0) => panic!("helper closed before every child reached pre-exec"),
                Ok(read) => observed += read,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("pre-exec readiness failed: {error}"),
            }
        }
        assert_eq!(observed, expected, "every child must reach pre-exec");
    }

    #[cfg(unix)]
    fn read_process_groups(directory: &std::path::Path, expected: usize) -> Vec<i32> {
        let mut groups = std::fs::read_dir(directory)
            .expect("process-group directory reads")
            .map(|entry| {
                entry
                    .expect("process-group marker reads")
                    .file_name()
                    .to_string_lossy()
                    .parse()
                    .expect("process-group marker is a pid")
            })
            .collect::<Vec<_>>();
        groups.sort_unstable();
        groups.dedup();
        assert_eq!(groups.len(), expected, "every pre-exec group is distinct");
        groups
    }

    #[cfg(unix)]
    fn kill_engine(engine: &mut std::process::Child) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        let engine_pid = i32::try_from(engine.id()).expect("Engine pid fits pid_t");
        kill(Pid::from_raw(engine_pid), Signal::SIGKILL).expect("Engine receives SIGKILL");
        let status = engine.wait().expect("Engine is reaped");
        assert!(!status.success());
    }

    #[cfg(unix)]
    fn wait_for_process_group_reap(process_group: i32) {
        use nix::errno::Errno;
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;
        use std::time::{Duration, Instant};

        let group = Pid::from_raw(process_group);
        let termination_deadline = Instant::now() + Duration::from_secs(1);
        let mut last_members = String::new();
        loop {
            match killpg(group, None) {
                Err(Errno::ESRCH) => break,
                Ok(()) if Instant::now() < termination_deadline => {}
                Err(Errno::EPERM) if Instant::now() < termination_deadline => {
                    last_members = process_group_members(process_group);
                    eprintln!(
                        "process group {process_group} remained nonterminal after Engine death:\n{last_members}"
                    );
                }
                observation => {
                    let _ = killpg(group, Signal::SIGKILL);
                    panic!(
                        "process group {process_group} was not reaped after Engine death: \
                         {observation:?}\n{last_members}"
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[cfg(unix)]
    fn process_group_members(process_group: i32) -> String {
        use std::process::Command;

        let output = Command::new("/bin/ps")
            .args(["-axo", "pid=,ppid=,pgid=,state=,uid=,command="])
            .output()
            .expect("process-table observation executes");
        let process_group = process_group.to_string();
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| {
                line.split_whitespace()
                    .nth(2)
                    .is_some_and(|value| value == process_group)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}
