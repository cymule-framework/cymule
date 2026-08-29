pub use cymule_profile_protocol::resource::{
    MAX_RESOURCE_CATALOG_RECORD_BYTES, RESOURCE_CATALOG_RECORD_VERSION, ResourceCatalogRecord,
};

use crate::ResourceResult;

/// Durable provider boundary for immutable non-semantic locator and proof data.
pub trait ResourceCatalogStore {
    /// Insert one record exactly once, accepting only an identical replay.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid records, conflicts, integrity failures, or
    /// provider failures.
    fn put_catalog_record(&mut self, record: &ResourceCatalogRecord) -> ResourceResult<()>;

    /// Load one exact record by stable namespace and key.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid keys, integrity failures, or provider failures.
    fn get_catalog_record(
        &mut self,
        namespace: &str,
        key: &str,
    ) -> ResourceResult<Option<ResourceCatalogRecord>>;
}
