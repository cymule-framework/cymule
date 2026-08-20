use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    ResourceCleanupReceipt, ResourceError, ResourceIntegrity, ResourcePublication, ResourceResult,
    ResourceShape,
};

/// Maximum bytes submitted to a store in one call.
pub const MAX_WRITE_CHUNK: usize = 8 * 1024 * 1024;

/// Provider-neutral metadata for a new chunked resource write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceWriteIntent {
    /// Caller-supplied idempotency identity.
    pub write_id: String,
    /// Logical resource shape.
    pub shape: ResourceShape,
    /// Intended media type.
    pub media_type: String,
    /// Semantic annotations included in the resulting Resource ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Stable upload session returned by a resource store adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceWriteSession {
    /// Caller write identity.
    pub write_id: String,
    /// Adapter-owned opaque upload identity.
    pub upload_id: String,
    /// Immutable store implementation binding.
    pub store_binding: String,
}

impl ResourceWriteIntent {
    /// Validate an external chunked-write request.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identities, inline shape, invalid media type,
    /// or oversized annotation fields.
    pub fn validate(&self) -> ResourceResult<()> {
        if self.write_id.is_empty()
            || self.write_id.len() > 512
            || self.write_id.chars().any(char::is_control)
        {
            return Err(ResourceError::Validation(
                "resource write ID must contain 1..=512 non-control characters".to_owned(),
            ));
        }
        if self.shape == ResourceShape::Inline {
            return Err(ResourceError::Validation(
                "inline resources do not use chunked ArtifactStore writes".to_owned(),
            ));
        }
        if self.media_type.is_empty()
            || self.media_type.len() > 255
            || !self.media_type.contains('/')
            || self.media_type.chars().any(char::is_whitespace)
        {
            return Err(ResourceError::Validation(
                "resource write media type is invalid".to_owned(),
            ));
        }
        for (key, value) in &self.annotations {
            if key.is_empty()
                || key.len() > 2048
                || key.chars().any(char::is_control)
                || value.len() > 4096
            {
                return Err(ResourceError::Validation(
                    "resource write annotation is invalid".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

impl ResourceWriteSession {
    fn validate_for(&self, intent: &ResourceWriteIntent) -> ResourceResult<()> {
        if self.write_id != intent.write_id
            || self.upload_id.is_empty()
            || self.store_binding.is_empty()
            || self.upload_id.chars().any(char::is_control)
            || self.store_binding.chars().any(char::is_control)
        {
            return Err(ResourceError::Substrate(
                "store returned an invalid write session".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Replaceable chunked write boundary for large resources.
pub trait ArtifactStore {
    /// Begin or resume one idempotent write.
    fn begin_write(&mut self, intent: &ResourceWriteIntent)
    -> ResourceResult<ResourceWriteSession>;

    /// Persist one contiguous chunk at the exact expected offset.
    fn write_chunk(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()>;

    /// Finalize the upload and return its verified immutable handle.
    fn commit_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication>;

    /// Abort one upload and return verified staging/chunk cleanup evidence.
    fn abort_write(
        &mut self,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt>;
}

/// Validating facade over one chunked store adapter.
pub struct ResourceWriter<S> {
    store: S,
}

impl<S: ArtifactStore> ResourceWriter<S> {
    /// Wrap one store implementation.
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Begin or resume a validated write.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid intent/session or adapter failure.
    pub fn begin(&mut self, intent: &ResourceWriteIntent) -> ResourceResult<ResourceWriteSession> {
        intent.validate()?;
        let session = self.store.begin_write(intent)?;
        session.validate_for(intent)?;
        Ok(session)
    }

    /// Write one bounded chunk.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/oversized chunk or adapter failure.
    pub fn write(
        &mut self,
        session: &ResourceWriteSession,
        offset: u64,
        bytes: &[u8],
    ) -> ResourceResult<()> {
        if bytes.is_empty() || bytes.len() > MAX_WRITE_CHUNK {
            return Err(ResourceError::Validation(format!(
                "resource write chunk must be 1..={MAX_WRITE_CHUNK} bytes"
            )));
        }
        self.store.write_chunk(session, offset, bytes)
    }

    /// Commit and verify that the returned immutable Handle matches the intent.
    ///
    /// # Errors
    ///
    /// Returns an error for adapter failure, invalid Resource identity, changed
    /// shape/media/annotations, or non-immutable write evidence.
    pub fn commit(
        &mut self,
        intent: &ResourceWriteIntent,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourcePublication> {
        intent.validate()?;
        session.validate_for(intent)?;
        let publication = self.store.commit_write(session)?;
        publication.verify()?;
        if publication.resource.shape != intent.shape
            || publication.resource.media_type != intent.media_type
            || publication.resource.annotations != intent.annotations
            || !matches!(
                publication.resource.integrity,
                ResourceIntegrity::Content { .. } | ResourceIntegrity::Version { .. }
            )
        {
            return Err(ResourceError::Substrate(
                "store committed a Resource that does not match its write intent".to_owned(),
            ));
        }
        Ok(publication)
    }

    /// Abort one validated upload session.
    ///
    /// # Errors
    ///
    /// Returns an error when the session does not match the intent or the
    /// adapter cannot record the abort.
    pub fn abort(
        &mut self,
        intent: &ResourceWriteIntent,
        session: &ResourceWriteSession,
    ) -> ResourceResult<ResourceCleanupReceipt> {
        intent.validate()?;
        session.validate_for(intent)?;
        let receipt = self.store.abort_write(session)?;
        receipt.verify()?;
        if receipt.write_id != intent.write_id
            || receipt.upload_id != session.upload_id
            || receipt.store_binding != session.store_binding
        {
            return Err(ResourceError::Substrate(
                "store returned cleanup evidence for another upload".to_owned(),
            ));
        }
        Ok(receipt)
    }

    /// Consume the facade and return its adapter.
    pub fn into_inner(self) -> S {
        self.store
    }
}
