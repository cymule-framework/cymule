use std::collections::BTreeMap;

use cymule_core::{ReconciliationResolution, WorldOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RuntimeError, RuntimeResult};

/// Process plugin protocol version.
pub const PLUGIN_VERSION: &str = "cymule.plugin/2";

/// Declared application failure returned by a component implementation.
///
/// This value is distinct from a plugin defect. Callers may branch on `code`;
/// `message` is display-only and never carries control semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginExpectedFailure {
    /// Stable application-owned failure code.
    pub code: String,
    /// Human-readable summary.
    pub message: String,
}

impl PluginExpectedFailure {
    /// Validate the bounded, machine-readable failure value.
    pub fn verify(&self) -> RuntimeResult<()> {
        let valid_code = !self.code.is_empty()
            && self.code.len() <= 200
            && self.code.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
            });
        if !valid_code || self.message.is_empty() || self.message.len() > 8192 {
            return Err(RuntimeError::plugin_defect(
                "plugin expected failure is not a bounded closed value",
            ));
        }
        Ok(())
    }
}

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
    /// A component completed with a declared application failure.
    ExpectedFailure {
        /// Closed application failure value.
        error: PluginExpectedFailure,
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
    /// The plugin reports that it could not honor the protocol correctly.
    Defect {
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
            PluginResponse::Defect { code, message } => {
                Err(RuntimeError::PluginDefect { code, message })
            }
            response => Err(RuntimeError::plugin_defect(format!(
                "describe returned unexpected response {response:?}"
            ))),
        }
    }
}
