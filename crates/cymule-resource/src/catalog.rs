use serde::{Deserialize, Serialize};

use crate::{ResourceError, ResourceResult};

/// Frozen provider-backed immutable catalog record version.
pub const RESOURCE_CATALOG_RECORD_VERSION: &str = "cymule.resource-catalog-record/1";

/// One immutable provider-side metadata record addressed by namespace and key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCatalogRecord {
    /// Catalog wire version.
    pub record_version: String,
    /// Stable catalog namespace owned by the consuming framework profile.
    pub namespace: String,
    /// Stable logical key within the namespace.
    pub key: String,
    /// Content identity of namespace, key, and exact payload bytes.
    pub record_id: String,
    /// Exact canonical payload bytes interpreted by the owning profile.
    pub payload: Vec<u8>,
}

impl ResourceCatalogRecord {
    /// Seal one immutable catalog payload.
    pub fn new(
        namespace: impl Into<String>,
        key: impl Into<String>,
        payload: Vec<u8>,
    ) -> ResourceResult<Self> {
        let namespace = namespace.into();
        let key = key.into();
        validate_identity("catalog namespace", &namespace)?;
        validate_identity("catalog key", &key)?;
        let record_id = cymule_core::content_id(
            RESOURCE_CATALOG_RECORD_VERSION,
            &(namespace.as_str(), key.as_str(), payload.as_slice()),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        Ok(Self {
            record_version: RESOURCE_CATALOG_RECORD_VERSION.to_owned(),
            namespace,
            key,
            record_id,
            payload,
        })
    }

    /// Verify the immutable record identity.
    pub fn verify(&self) -> ResourceResult<()> {
        if self.record_version != RESOURCE_CATALOG_RECORD_VERSION {
            return Err(ResourceError::Validation(format!(
                "unsupported Resource catalog record version {:?}",
                self.record_version
            )));
        }
        validate_identity("catalog namespace", &self.namespace)?;
        validate_identity("catalog key", &self.key)?;
        let expected = cymule_core::content_id(
            RESOURCE_CATALOG_RECORD_VERSION,
            &(
                self.namespace.as_str(),
                self.key.as_str(),
                self.payload.as_slice(),
            ),
        )
        .map_err(|error| ResourceError::Validation(error.to_string()))?;
        if self.record_id != expected {
            return Err(ResourceError::Integrity(format!(
                "Resource catalog record {} does not match {expected}",
                self.record_id
            )));
        }
        Ok(())
    }
}

/// Durable provider boundary for immutable non-semantic locator and proof data.
pub trait ResourceCatalogStore {
    /// Insert one record exactly once, accepting only an identical replay.
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()>;

    /// Load one exact record by stable namespace and key.
    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>>;
}

fn validate_identity(kind: &str, value: &str) -> ResourceResult<()> {
    if value.is_empty() || value.len() > 2048 || value.chars().any(char::is_control) {
        return Err(ResourceError::Validation(format!(
            "Resource {kind} must contain 1..=2048 non-control characters"
        )));
    }
    Ok(())
}
