//! Hardened process plugin executor for Cymule.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_core::{decode_json, sha256_bytes};
use cymule_runtime::{PluginHost, PluginRequest, PluginResponse, RuntimeError, RuntimeResult};
use tempfile::{Builder, TempDir};
use wait_timeout::ChildExt;

/// Default request/output safety bound.
pub const DEFAULT_PROCESS_MESSAGE_LIMIT: usize = 8 * 1024 * 1024;

/// Explicit process execution policy.
#[derive(Debug, Clone)]
pub struct ProcessExecutorConfig {
    /// Executable whose current bytes are sealed during construction.
    pub executable: PathBuf,
    /// Exact argument vector.
    pub arguments: Vec<String>,
    /// Optional child working directory.
    pub working_directory: Option<PathBuf>,
    /// Complete allowed child environment after ambient clearing.
    pub environment: BTreeMap<String, String>,
    /// Maximum time for one request process.
    pub timeout: Duration,
    /// Maximum encoded request, stdout, or stderr bytes.
    pub message_limit: usize,
}

impl ProcessExecutorConfig {
    /// Construct a minimal policy around one executable.
    pub fn new(executable: impl AsRef<Path>) -> Self {
        Self {
            executable: executable.as_ref().to_path_buf(),
            arguments: Vec::new(),
            working_directory: None,
            environment: BTreeMap::new(),
            timeout: Duration::from_mins(1),
            message_limit: DEFAULT_PROCESS_MESSAGE_LIMIT,
        }
    }

    fn validate(&self) -> RuntimeResult<()> {
        if !self.executable.is_absolute()
            || self.timeout.is_zero()
            || self.message_limit == 0
            || self.message_limit > 64 * 1024 * 1024
            || self
                .environment
                .keys()
                .any(|key| key.is_empty() || key.contains('=') || key.chars().any(char::is_control))
        {
            return Err(RuntimeError::plugin_defect(
                "process executor configuration is invalid",
            ));
        }
        Ok(())
    }
}

/// One-occurrence process plugin transport.
///
/// Construction copies the selected bytes into an executor-private directory.
/// Every invocation launches that sealed copy, so the advertised digest and
/// the bytes actually executed have one authority.
#[derive(Debug)]
pub struct ProcessExecutor {
    config: ProcessExecutorConfig,
    _sealed_directory: TempDir,
    sealed_executable: PathBuf,
    implementation_revision: String,
}

impl ProcessExecutor {
    /// Validate the policy, seal the executable bytes, and construct an executor.
    pub fn new(config: ProcessExecutorConfig) -> RuntimeResult<Self> {
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
        let directory = Builder::new()
            .prefix("cymule-executor-")
            .tempdir()
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process executable directory could not be created",
                )
            })?;
        set_private_directory_permissions(directory.path())?;
        let sealed_executable = directory.path().join("plugin");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sealed_executable)
            .map_err(|_| {
                RuntimeError::substrate(
                    "process_seal_failed",
                    "private process executable copy could not be created",
                )
            })?;
        file.write_all(&bytes).map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process executable copy could not be written",
            )
        })?;
        file.sync_all().map_err(|_| {
            RuntimeError::substrate(
                "process_seal_failed",
                "private process executable copy could not be synchronized",
            )
        })?;
        set_executable_permissions(&sealed_executable)?;
        Ok(Self {
            config,
            _sealed_directory: directory,
            sealed_executable,
            implementation_revision: format!("sha256:{}", sha256_bytes(&bytes)),
        })
    }

    /// Read-only execution policy. Its source path is provenance only.
    pub const fn config(&self) -> &ProcessExecutorConfig {
        &self.config
    }

    /// Digest of the sealed bytes launched by every invocation.
    pub fn implementation_revision(&self) -> &str {
        &self.implementation_revision
    }

    fn invoke_process(&self, request: &PluginRequest) -> RuntimeResult<PluginResponse> {
        let input = serde_json::to_vec(request)?;
        if input.len() > self.config.message_limit {
            return Err(RuntimeError::plugin_defect(
                "process plugin request exceeds the configured byte limit",
            ));
        }
        let mut command = Command::new(&self.sealed_executable);
        command
            .args(&self.config.arguments)
            .env_clear()
            .envs(&self.config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(directory) = &self.config.working_directory {
            command.current_dir(directory);
        }
        let mut child = command.spawn().map_err(|_| {
            RuntimeError::substrate(
                "process_start_failed",
                "sealed process plugin could not be started",
            )
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            post_start_failure(
                request,
                "process_stdin_unavailable",
                "plugin stdin was unavailable",
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            post_start_failure(
                request,
                "process_stdout_unavailable",
                "plugin stdout was unavailable",
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            post_start_failure(
                request,
                "process_stderr_unavailable",
                "plugin stderr was unavailable",
            )
        })?;
        let limit = self.config.message_limit;
        let writer = thread::spawn(move || write_input(stdin, &input));
        let stdout_reader = thread::spawn(move || read_limited(stdout, limit));
        let stderr_reader = thread::spawn(move || read_limited(stderr, limit));

        let status = match child.wait_timeout(self.config.timeout) {
            Ok(Some(status)) => status,
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_io(writer);
                let _ = join_io(stdout_reader);
                let _ = join_io(stderr_reader);
                return Err(if is_dispatch(request) {
                    RuntimeError::unknown_world(
                        "effect_dispatch_timed_out",
                        "effect dispatch process timed out after starting",
                    )
                } else {
                    RuntimeError::timed_out(
                        "process_response_timed_out",
                        "process plugin response deadline elapsed",
                    )
                });
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = join_io(writer);
                let _ = join_io(stdout_reader);
                let _ = join_io(stderr_reader);
                return Err(post_start_failure(
                    request,
                    "process_wait_failed",
                    "process plugin completion could not be observed",
                ));
            }
        };
        let write_result = join_io(writer);
        let stdout = join_io(stdout_reader);
        let stderr = join_io(stderr_reader);
        if write_result.is_err() || stdout.is_err() || stderr.is_err() {
            return Err(post_start_failure(
                request,
                "process_io_failed",
                "bounded process plugin I/O did not complete",
            ));
        }
        if !status.success() {
            return Err(if is_dispatch(request) {
                RuntimeError::unknown_world(
                    "effect_dispatch_response_lost",
                    "effect dispatch process exited without an authoritative response",
                )
            } else {
                RuntimeError::PluginDefect {
                    code: "plugin_process_failed".to_owned(),
                    message: "process plugin exited without a valid response".to_owned(),
                }
            });
        }
        decode_json(&stdout.expect("checked above")).map_err(|_| {
            post_start_failure(
                request,
                "invalid_plugin_response",
                "process plugin returned an invalid protocol response",
            )
        })
    }
}

impl PluginHost for ProcessExecutor {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        self.config.validate()?;
        self.invoke_process(&request)
    }
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

fn write_input(mut writer: impl Write, bytes: &[u8]) -> std::io::Result<Vec<u8>> {
    writer.write_all(bytes)?;
    Ok(Vec::new())
}

fn read_limited(reader: impl Read, limit: usize) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::FileTooLarge,
            "process output exceeded configured limit",
        ));
    }
    Ok(bytes)
}

fn join_io(reader: thread::JoinHandle<std::io::Result<Vec<u8>>>) -> std::io::Result<Vec<u8>> {
    reader
        .join()
        .map_err(|_| std::io::Error::other("process I/O task panicked"))?
}

#[cfg(unix)]
fn set_executable_permissions(path: &Path) -> RuntimeResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o500)).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process executable permissions could not be sealed",
        )
    })
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> RuntimeResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| {
        RuntimeError::substrate(
            "process_seal_failed",
            "private process executable directory permissions could not be sealed",
        )
    })
}

#[cfg(not(unix))]
fn set_executable_permissions(_path: &Path) -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires a Unix executable permission model",
    ))
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> RuntimeResult<()> {
    Err(RuntimeError::plugin_defect(
        "the process executor requires a Unix private-directory permission model",
    ))
}
