//! Hardened process plugin executor for Cymule.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use cymule_runtime::{
    PLUGIN_VERSION, PluginHost, PluginManifest, PluginRequest, PluginResponse, RuntimeError,
    RuntimeResult,
};
use wait_timeout::ChildExt;

/// Default request/output safety bound.
pub const DEFAULT_PROCESS_MESSAGE_LIMIT: usize = 8 * 1024 * 1024;

/// Explicit process execution policy.
#[derive(Debug, Clone)]
pub struct ProcessExecutorConfig {
    /// Executable path; no shell interpretation is performed.
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
            return Err(RuntimeError::Plugin(
                "process executor configuration is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

/// One-occurrence process plugin transport.
#[derive(Debug, Clone)]
pub struct ProcessExecutor {
    config: ProcessExecutorConfig,
}

impl ProcessExecutor {
    /// Validate and construct an executor.
    pub fn new(config: ProcessExecutorConfig) -> RuntimeResult<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Read-only execution policy.
    pub const fn config(&self) -> &ProcessExecutorConfig {
        &self.config
    }
}

impl PluginHost for ProcessExecutor {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        self.config.validate()?;
        let input = serde_json::to_vec(&request)?;
        if input.len() > self.config.message_limit {
            return Err(RuntimeError::Plugin(
                "process plugin request exceeds the configured byte limit".to_owned(),
            ));
        }
        let mut command = Command::new(&self.config.executable);
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
        let mut child = command.spawn().map_err(|error| {
            RuntimeError::Io(format!(
                "failed to start process plugin {}: {error}",
                self.config.executable.display()
            ))
        })?;
        child
            .stdin
            .take()
            .ok_or_else(|| RuntimeError::Io("process stdin was not captured".to_owned()))?
            .write_all(&input)?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RuntimeError::Io("process stdout was not captured".to_owned()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| RuntimeError::Io("process stderr was not captured".to_owned()))?;
        let limit = self.config.message_limit;
        let stdout_reader = thread::spawn(move || read_limited(stdout, limit));
        let stderr_reader = thread::spawn(move || read_limited(stderr, limit));
        let status = child.wait_timeout(self.config.timeout)?;
        let timed_out = status.is_none();
        let status = if let Some(status) = status {
            status
        } else {
            child.kill()?;
            child.wait()?
        };
        let stdout = join_reader(stdout_reader)?;
        let stderr = join_reader(stderr_reader)?;
        if timed_out {
            return Err(RuntimeError::Plugin(format!(
                "process_timeout_unknown: plugin {} exceeded {:?}",
                self.config.executable.display(),
                self.config.timeout
            )));
        }
        if !status.success() {
            return Err(RuntimeError::Plugin(format!(
                "process plugin {} exited with {status}: {}",
                self.config.executable.display(),
                String::from_utf8_lossy(&stderr).trim()
            )));
        }
        let response: PluginResponse = serde_json::from_slice(&stdout)?;
        if let PluginResponse::Error { code, message } = &response {
            return Err(RuntimeError::Plugin(format!("{code}: {message}")));
        }
        Ok(response)
    }

    fn describe(&mut self) -> RuntimeResult<PluginManifest> {
        let manifest = match self.invoke(PluginRequest::Describe)? {
            PluginResponse::Manifest { manifest } => manifest,
            response => {
                return Err(RuntimeError::Plugin(format!(
                    "describe returned unexpected response {response:?}"
                )));
            }
        };
        if manifest.plugin_version != PLUGIN_VERSION || manifest.implementation_id.is_empty() {
            return Err(RuntimeError::Plugin(
                "process plugin returned an invalid manifest".to_owned(),
            ));
        }
        Ok(manifest)
    }
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

fn join_reader(reader: thread::JoinHandle<std::io::Result<Vec<u8>>>) -> RuntimeResult<Vec<u8>> {
    reader
        .join()
        .map_err(|_| RuntimeError::Io("process output reader panicked".to_owned()))?
        .map_err(RuntimeError::from)
}
