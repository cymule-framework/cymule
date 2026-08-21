//! Hardened process plugin executor for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use cymule_core::{canonical_bytes, decode_json, sha256_bytes};
use cymule_runtime::{PluginHost, PluginRequest, PluginResponse, RuntimeError, RuntimeResult};
use serde::Serialize;
use tempfile::{Builder, TempDir};

/// Default request/output safety bound.
pub const DEFAULT_PROCESS_MESSAGE_LIMIT: usize = 8 * 1024 * 1024;

/// Default maximum bytes captured from a configured working directory.
pub const DEFAULT_PROCESS_CLOSURE_LIMIT: usize = 64 * 1024 * 1024;

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
    /// Immutable revisions for runtime facilities outside the captured tree.
    ///
    /// Keys name facilities such as an OS ABI or a separately admitted loader;
    /// values are provider-owned immutable revisions. The complete sorted map is
    /// part of the canonical execution-binding identity.
    pub runtime_closure: BTreeMap<String, String>,
    /// Maximum time for the complete spawn, response, tree cleanup, and I/O observation.
    pub timeout: Duration,
    /// Maximum encoded request, stdout, or stderr bytes.
    pub message_limit: usize,
    /// Maximum total bytes captured from `working_directory`.
    pub closure_limit: usize,
}

impl ProcessExecutorConfig {
    /// Construct a minimal policy around one executable.
    pub fn new(executable: impl AsRef<Path>) -> Self {
        let mut runtime_closure = BTreeMap::new();
        runtime_closure.insert(
            "host-abi".to_owned(),
            format!("unix:{}:{}", std::env::consts::OS, std::env::consts::ARCH),
        );
        Self {
            executable: executable.as_ref().to_path_buf(),
            arguments: Vec::new(),
            working_directory: None,
            environment: BTreeMap::new(),
            runtime_closure,
            timeout: Duration::from_mins(1),
            message_limit: DEFAULT_PROCESS_MESSAGE_LIMIT,
            closure_limit: DEFAULT_PROCESS_CLOSURE_LIMIT,
        }
    }

    fn validate(&self) -> RuntimeResult<()> {
        if !self.executable.is_absolute()
            || self.timeout.is_zero()
            || self.message_limit == 0
            || self.message_limit > 64 * 1024 * 1024
            || self.closure_limit == 0
            || self.closure_limit > 1024 * 1024 * 1024
            || self.runtime_closure.is_empty()
            || self
                .environment
                .iter()
                .any(|(key, value)| invalid_key(key) || value.contains('\0'))
            || self
                .runtime_closure
                .iter()
                .any(|(key, value)| invalid_key(key) || invalid_value(value))
            || self.arguments.iter().any(|value| value.contains('\0'))
        {
            return Err(RuntimeError::plugin_defect(
                "process executor configuration is invalid",
            ));
        }
        Ok(())
    }
}

fn invalid_key(value: &str) -> bool {
    value.is_empty() || value.contains('=') || value.chars().any(char::is_control)
}

fn invalid_value(value: &str) -> bool {
    value.is_empty() || value.contains('\0')
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

#[derive(Debug)]
struct CapturedDirectory {
    directories: Vec<String>,
    files: Vec<CapturedFile>,
    identity: String,
}

#[derive(Serialize)]
struct CapturedDirectoryIdentity<'a> {
    version: &'static str,
    directories: &'a [String],
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
    pub fn new(config: ProcessExecutorConfig) -> RuntimeResult<Self> {
        #[cfg(not(unix))]
        ensure_supported_platform()?;
        config.validate()?;
        let bytes = fs::read(&config.executable).map_err(|_| {
            RuntimeError::substrate(
                "process_executable_read_failed",
                "selected process plugin executable could not be read",
            )
        })?;
        if bytes.is_empty() {
            return Err(RuntimeError::plugin_defect(
                "selected process plugin executable is empty",
            ));
        }
        let executable_revision = format!("sha256:{}", sha256_bytes(&bytes));
        let working_directory = config
            .working_directory
            .as_deref()
            .map(|path| capture_directory(path, config.closure_limit))
            .transpose()?;
        let identity = ProcessBindingIdentity {
            version: "cymule.process-execution-binding/1",
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

    fn invoke_process(&self, request: &PluginRequest) -> RuntimeResult<PluginResponse> {
        let started_at = Instant::now();
        let deadline = started_at.checked_add(self.config.timeout).ok_or_else(|| {
            RuntimeError::plugin_defect("process executor timeout exceeds the clock range")
        })?;
        let input = serde_json::to_vec(request)?;
        if input.len() > self.config.message_limit {
            return Err(RuntimeError::plugin_defect(
                "process plugin request exceeds the configured byte limit",
            ));
        }
        let invocation = self.materialize_invocation()?;
        if Instant::now() >= deadline {
            return Err(RuntimeError::timed_out(
                "process_response_timed_out",
                "process closure materialization exceeded the invocation deadline",
            ));
        }
        let mut command = Command::new(&invocation.executable);
        command
            .args(&self.config.arguments)
            .env_clear()
            .envs(&self.config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(directory) = &invocation.working_directory {
            command.current_dir(directory);
        }
        configure_process_group(&mut command);
        let mut child = command.spawn().map_err(|_| {
            RuntimeError::substrate(
                "process_start_failed",
                "sealed process plugin could not be started",
            )
        })?;
        let process_group = child.id();
        let (Some(stdin), Some(mut stdout), Some(mut stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            terminate_process_tree(&mut child, process_group);
            return Err(post_start_failure(
                request,
                "process_pipe_unavailable",
                "one or more plugin process pipes were unavailable",
            ));
        };
        if set_nonblocking(&stdin).is_err()
            || set_nonblocking(&stdout).is_err()
            || set_nonblocking(&stderr).is_err()
        {
            terminate_process_tree(&mut child, process_group);
            return Err(post_start_failure(
                request,
                "process_pipe_configuration_failed",
                "plugin process pipes could not be configured for bounded I/O",
            ));
        }
        let mut stdin = Some(stdin);
        let limit = self.config.message_limit;
        let mut input_offset = 0usize;
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let mut stdout_eof = false;
        let mut stderr_eof = false;
        let mut status = None;
        let mut group_closed = false;

        loop {
            if Instant::now() >= deadline {
                terminate_process_tree(&mut child, process_group);
                let _ = read_available(&mut stdout, &mut stdout_bytes, limit);
                let _ = read_available(&mut stderr, &mut stderr_bytes, limit);
                return Err(invocation_timeout(request));
            }
            let mut progressed = false;
            if let Some(writer) = stdin.as_mut() {
                let Ok(wrote) = write_available(writer, &input, &mut input_offset) else {
                    terminate_process_tree(&mut child, process_group);
                    return Err(post_start_failure(
                        request,
                        "process_io_failed",
                        "plugin process did not consume its complete request",
                    ));
                };
                progressed |= wrote;
                if input_offset == input.len() {
                    stdin = None;
                }
            }
            if !stdout_eof {
                let Ok((read, eof)) = read_available(&mut stdout, &mut stdout_bytes, limit) else {
                    terminate_process_tree(&mut child, process_group);
                    return Err(post_start_failure(
                        request,
                        "process_io_failed",
                        "bounded plugin stdout could not be collected",
                    ));
                };
                progressed |= read;
                stdout_eof = eof;
            }
            if !stderr_eof {
                let Ok((read, eof)) = read_available(&mut stderr, &mut stderr_bytes, limit) else {
                    terminate_process_tree(&mut child, process_group);
                    return Err(post_start_failure(
                        request,
                        "process_io_failed",
                        "bounded plugin stderr could not be collected",
                    ));
                };
                progressed |= read;
                stderr_eof = eof;
            }
            if status.is_none() {
                match child.try_wait() {
                    Ok(Some(observed)) => {
                        status = Some(observed);
                        progressed = true;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        terminate_process_tree(&mut child, process_group);
                        return Err(post_start_failure(
                            request,
                            "process_wait_failed",
                            "process plugin completion could not be observed",
                        ));
                    }
                }
            }
            if status.is_some() && !group_closed {
                // A one-request plugin may not leave background children behind.
                // Closing the occurrence process group also closes pipes retained
                // by forked descendants before the next nonblocking drain.
                kill_process_group(process_group);
                group_closed = true;
                if stdin.is_some() {
                    terminate_process_tree(&mut child, process_group);
                    return Err(post_start_failure(
                        request,
                        "process_io_failed",
                        "plugin process exited before consuming its complete request",
                    ));
                }
            }
            if status.is_some() && stdout_eof && stderr_eof {
                break;
            }
            if !progressed {
                thread::sleep(remaining(deadline).min(Duration::from_millis(1)));
            }
        }
        validate_exit(
            request,
            status.expect("loop exits only after observing status"),
        )?;
        decode_json(&stdout_bytes).map_err(|_| {
            post_start_failure(
                request,
                "invalid_plugin_response",
                "process plugin returned an invalid protocol response",
            )
        })
    }

    fn materialize_invocation(&self) -> RuntimeResult<InvocationFiles> {
        let directory = Builder::new()
            .prefix("cymule-executor-")
            .tempdir()
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process invocation directory could not be created",
                )
            })?;
        set_private_directory_permissions(directory.path())?;
        let executable = directory.path().join("plugin");
        write_new_file(&executable, &self.executable_bytes, 0o500)?;
        let working_directory = self
            .working_directory
            .as_ref()
            .map(|snapshot| materialize_directory(directory.path(), snapshot))
            .transpose()?;
        Ok(InvocationFiles {
            _directory: directory,
            executable,
            working_directory,
        })
    }
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
    _directory: TempDir,
    executable: PathBuf,
    working_directory: Option<PathBuf>,
}

fn capture_directory(path: &Path, limit: usize) -> RuntimeResult<CapturedDirectory> {
    if !path.is_absolute() || !path.is_dir() {
        return Err(RuntimeError::plugin_defect(
            "process working directory must be an absolute directory",
        ));
    }
    let mut directories = Vec::new();
    let mut files = Vec::new();
    let mut total = 0usize;
    capture_directory_entries(path, path, &mut directories, &mut files, &mut total, limit)?;
    directories.sort();
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let identities = files
        .iter()
        .map(|file| CapturedFileIdentity {
            path: &file.relative_path,
            digest: format!("sha256:{}", sha256_bytes(&file.bytes)),
            mode: file.mode,
        })
        .collect();
    let identity = CapturedDirectoryIdentity {
        version: "cymule.process-working-directory/1",
        directories: &directories,
        files: identities,
    };
    let identity = format!(
        "sha256:{}",
        sha256_bytes(&canonical_bytes(&identity).map_err(|_| {
            RuntimeError::plugin_defect("process working directory could not be canonicalized")
        })?)
    );
    Ok(CapturedDirectory {
        directories,
        files,
        identity,
    })
}

fn capture_directory_entries(
    root: &Path,
    directory: &Path,
    directories: &mut Vec<String>,
    files: &mut Vec<CapturedFile>,
    total: &mut usize,
    limit: usize,
) -> RuntimeResult<()> {
    let mut entries = fs::read_dir(directory)
        .map_err(|_| RuntimeError::plugin_defect("process working directory could not be read"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| RuntimeError::plugin_defect("process working directory could not be read"))?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let relative = path.strip_prefix(root).map_err(|_| {
            RuntimeError::plugin_defect("process working directory path escaped its root")
        })?;
        let relative = relative.to_str().ok_or_else(|| {
            RuntimeError::plugin_defect("process working directory paths must be valid UTF-8")
        })?;
        if relative.is_empty() || Path::new(relative).is_absolute() {
            return Err(RuntimeError::plugin_defect(
                "process working directory contains an invalid relative path",
            ));
        }
        let metadata = fs::symlink_metadata(&path).map_err(|_| {
            RuntimeError::plugin_defect("process working directory metadata could not be read")
        })?;
        if metadata.file_type().is_symlink() {
            return Err(RuntimeError::plugin_defect(
                "process working directory symlinks are not admitted",
            ));
        }
        if metadata.is_dir() {
            directories.push(relative.to_owned());
            capture_directory_entries(root, &path, directories, files, total, limit)?;
        } else if metadata.is_file() {
            let bytes = fs::read(&path).map_err(|_| {
                RuntimeError::plugin_defect("process working directory file could not be read")
            })?;
            *total = total.checked_add(bytes.len()).ok_or_else(|| {
                RuntimeError::plugin_defect("process working directory size overflowed")
            })?;
            if *total > limit {
                return Err(RuntimeError::plugin_defect(
                    "process working directory exceeds the configured closure limit",
                ));
            }
            files.push(CapturedFile {
                relative_path: relative.to_owned(),
                bytes,
                mode: executable_mode(&metadata),
            });
        } else {
            return Err(RuntimeError::plugin_defect(
                "process working directory contains a special file",
            ));
        }
    }
    Ok(())
}

fn materialize_directory(root: &Path, snapshot: &CapturedDirectory) -> RuntimeResult<PathBuf> {
    let destination = root.join("cwd");
    fs::create_dir(&destination).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private working directory could not be created",
        )
    })?;
    for relative in &snapshot.directories {
        fs::create_dir(destination.join(relative)).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private working-directory tree could not be created",
            )
        })?;
    }
    for file in &snapshot.files {
        write_new_file(
            &destination.join(&file.relative_path),
            &file.bytes,
            file.mode,
        )?;
    }
    Ok(destination)
}

fn write_new_file(path: &Path, bytes: &[u8], mode: u32) -> RuntimeResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process closure file could not be created",
            )
        })?;
    file.write_all(bytes).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file could not be written",
        )
    })?;
    file.sync_all().map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure file could not be synchronized",
        )
    })?;
    set_file_permissions(path, mode)
}

fn is_dispatch(request: &PluginRequest) -> bool {
    matches!(request, PluginRequest::DispatchEffect { .. })
}

fn post_start_failure(request: &PluginRequest, code: &str, message: &str) -> RuntimeError {
    if is_dispatch(request) {
        RuntimeError::unknown_world(code, message)
    } else {
        RuntimeError::substrate(code, message)
    }
}

fn invocation_timeout(request: &PluginRequest) -> RuntimeError {
    if is_dispatch(request) {
        RuntimeError::unknown_world(
            "effect_dispatch_timed_out",
            "effect dispatch process timed out after starting",
        )
    } else {
        RuntimeError::timed_out(
            "process_response_timed_out",
            "process plugin response deadline elapsed",
        )
    }
}

fn validate_exit(request: &PluginRequest, status: ExitStatus) -> RuntimeResult<()> {
    if status.success() {
        return Ok(());
    }
    Err(if is_dispatch(request) {
        RuntimeError::unknown_world(
            "effect_dispatch_response_lost",
            "effect dispatch process exited without an authoritative response",
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
            if bytes.len().saturating_add(read) > limit {
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
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn kill_process_group(process_group: u32) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;

    if let Ok(process_group) = i32::try_from(process_group) {
        let _ = killpg(Pid::from_raw(process_group), Signal::SIGKILL);
    }
}

#[cfg(not(unix))]
fn kill_process_group(_process_group: u32) {}

fn terminate_process_tree(child: &mut Child, process_group: u32) {
    kill_process_group(process_group);
    let _ = child.kill();
    let _ = child.wait();
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

#[cfg(unix)]
fn set_file_permissions(path: &Path, mode: u32) -> RuntimeResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process closure permissions could not be set",
        )
    })
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> RuntimeResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process invocation directory permissions could not be set",
        )
    })
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path, _mode: u32) -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires Unix process-group and permission semantics",
    ))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires Unix process-group and permission semantics",
    ))
}
