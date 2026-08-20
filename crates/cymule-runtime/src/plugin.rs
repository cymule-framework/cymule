use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use cymule_core::{ReconciliationResolution, WorldOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RuntimeError, RuntimeResult};

/// Process plugin protocol version.
pub const PLUGIN_VERSION: &str = "cymule.plugin/1";

/// One abstract component operation advertised by a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginOperation {
    /// Stable implementation-specific revision.
    pub implementation_revision: String,
}

/// One abstract effect implementation advertised by a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginEffect {
    /// Stable implementation-specific revision.
    pub implementation_revision: String,
    /// Whether the adapter can authoritatively reconcile ambiguity.
    pub can_reconcile: bool,
}

/// Plugin capability advertisement. It does not grant authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    /// Protocol version.
    pub plugin_version: String,
    /// Immutable implementation identity used in occurrence bindings.
    pub implementation_id: String,
    /// Component implementations.
    #[serde(default)]
    pub components: BTreeMap<String, PluginOperation>,
    /// Effect implementations.
    #[serde(default)]
    pub effects: BTreeMap<String, PluginEffect>,
}

/// Versioned process-plugin request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginRequest {
    /// Describe supported abstract operations.
    Describe,
    /// Execute one component occurrence.
    Call {
        /// Abstract component operation.
        component: String,
        /// Typed input.
        input: Value,
    },
    /// Prepare an effect without claiming external application.
    PrepareEffect {
        /// Abstract effect operation.
        operation: String,
        /// Structural intent identity.
        intent_id: String,
        /// Typed input.
        input: Value,
    },
    /// Dispatch an authorized effect occurrence.
    DispatchEffect {
        /// Abstract effect operation.
        operation: String,
        /// Structural intent identity and provider idempotency source.
        intent_id: String,
        /// Typed input.
        input: Value,
    },
    /// Reconcile an unknown effect using the same occurrence binding.
    ReconcileEffect {
        /// Abstract effect operation.
        operation: String,
        /// Original structural intent identity.
        intent_id: String,
        /// Original typed input.
        input: Value,
    },
}

/// Versioned process-plugin response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PluginResponse {
    /// Capability advertisement.
    Manifest {
        /// Manifest.
        manifest: PluginManifest,
    },
    /// Component result.
    CallResult {
        /// Typed output.
        value: Value,
    },
    /// Preparation succeeded.
    Prepared,
    /// Dispatch produced an observed outcome.
    EffectResult {
        /// External-world observation.
        outcome: WorldOutcome,
        /// Optional typed operation result.
        #[serde(default)]
        value: Option<Value>,
    },
    /// Reconciliation produced a typed resolution.
    ReconciliationResult {
        /// Resolution.
        resolution: ReconciliationResolution,
        /// Optional typed operation result.
        #[serde(default)]
        value: Option<Value>,
    },
    /// Structured adapter error.
    Error {
        /// Stable adapter code.
        code: String,
        /// Human-readable summary.
        message: String,
    },
}

/// Abstract plugin transport.
pub trait PluginHost {
    /// Invoke one typed plugin request.
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse>;

    /// Fetch and validate the plugin manifest.
    fn describe(&mut self) -> RuntimeResult<PluginManifest> {
        match self.invoke(PluginRequest::Describe)? {
            PluginResponse::Manifest { manifest } => {
                if manifest.plugin_version != PLUGIN_VERSION {
                    return Err(RuntimeError::plugin_defect(format!(
                        "unsupported plugin version {:?}",
                        manifest.plugin_version
                    )));
                }
                if manifest.implementation_id.is_empty() {
                    return Err(RuntimeError::plugin_defect(
                        "plugin implementation_id is empty",
                    ));
                }
                Ok(manifest)
            }
            response => Err(RuntimeError::plugin_defect(format!(
                "describe returned unexpected response {response:?}"
            ))),
        }
    }
}

/// One-request-per-process plugin transport used by the Embedded profile.
#[derive(Debug, Clone)]
pub struct ProcessPlugin {
    executable: PathBuf,
}

impl ProcessPlugin {
    /// Create a process plugin transport.
    pub fn new(executable: impl AsRef<Path>) -> Self {
        Self {
            executable: executable.as_ref().to_path_buf(),
        }
    }
}

impl PluginHost for ProcessPlugin {
    fn invoke(&mut self, request: PluginRequest) -> RuntimeResult<PluginResponse> {
        let mut child = Command::new(&self.executable)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                RuntimeError::substrate(
                    "plugin_start_failed",
                    format!(
                        "failed to start plugin {}: {error}",
                        self.executable.display()
                    ),
                )
            })?;
        let input = serde_json::to_vec(&request)?;
        child
            .stdin
            .take()
            .ok_or_else(|| {
                RuntimeError::substrate("plugin_stdin_unavailable", "plugin stdin was not captured")
            })?
            .write_all(&input)?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(RuntimeError::PluginDefect {
                code: "plugin_process_failed".to_owned(),
                message: format!(
                    "plugin {} exited with {}: {}",
                    self.executable.display(),
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }
        let response: PluginResponse =
            serde_json::from_slice(&output.stdout).map_err(|error| RuntimeError::PluginDefect {
                code: "invalid_plugin_response".to_owned(),
                message: error.to_string(),
            })?;
        if let PluginResponse::Error { code, message } = &response {
            return Err(RuntimeError::PluginDefect {
                code: "plugin_reported_error".to_owned(),
                message: format!("{code}: {message}"),
            });
        }
        Ok(response)
    }
}
