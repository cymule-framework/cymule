use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use cymule_durable::{DurableCoordinator, DurableStore, JournalRecord};

use crate::{ResourceError, ResourcePublication, ResourceResult};

/// Frozen pin receipt version.
pub const RESOURCE_PIN_RECEIPT_VERSION: &str = "cymule.resource-pin-receipt/1";
/// Frozen release receipt version.
pub const RESOURCE_RELEASE_RECEIPT_VERSION: &str = "cymule.resource-release-receipt/1";
/// Frozen garbage-collection receipt version.
pub const RESOURCE_GC_RECEIPT_VERSION: &str = "cymule.resource-gc-receipt/1";
/// Frozen deletion receipt version.
pub const RESOURCE_DELETE_RECEIPT_VERSION: &str = "cymule.resource-delete-receipt/1";
/// Frozen upload-cleanup receipt version.
pub const RESOURCE_CLEANUP_RECEIPT_VERSION: &str = "cymule.resource-cleanup-receipt/1";
/// Frozen delete-intent version.
pub const RESOURCE_DELETE_INTENT_VERSION: &str = "cymule.resource-delete-intent/1";
/// Frozen lifecycle journal domain.
pub const RESOURCE_LIFECYCLE_JOURNAL_VERSION: &str = "cymule.resource-lifecycle/1";

const RESOURCE_LIFECYCLE_JOURNAL_ID: &str = "cymule.resource-lifecycle";

/// Durable evidence retaining one exact Resource pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePinReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Stable caller-supplied pin identity.
    pub pin_id: String,
    /// Exact semantic Resource identity.
    pub resource_id: String,
    /// Stable owner of the retention obligation.
    pub owner: String,
}

/// Durable evidence releasing one exact pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceReleaseReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Stable caller-supplied release identity.
    pub release_id: String,
    /// Exact pin being released.
    pub pin_id: String,
    /// Exact semantic Resource identity retained by the pin.
    pub resource_id: String,
}

/// Closed garbage-collection decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceGcDisposition {
    /// At least one exact pin still retains the Resource.
    Retained,
    /// No exact pin remains; provider bytes may be deleted.
    Eligible,
}

/// Durable evidence for one exact garbage-collection decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceGcReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Stable caller-supplied collection identity.
    pub gc_id: String,
    /// Exact semantic Resource identity evaluated.
    pub resource_id: String,
    /// Exact active pin count observed by the lifecycle authority.
    pub active_pin_count: u64,
    /// Closed collection decision.
    pub disposition: ResourceGcDisposition,
}

/// Verified provider deletion receipt gated by an eligible GC decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeleteReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Stable caller-supplied delete identity.
    pub delete_id: String,
    /// Exact eligible GC operation authorizing deletion.
    pub gc_id: String,
    /// Exact semantic Resource identity deleted.
    pub resource_id: String,
    /// Immutable store binding that performed verification.
    pub store_binding: String,
    /// Number of content bytes removed, or zero for an idempotent replay.
    pub removed_bytes: u64,
    /// Provider readback proved the exact content object absent.
    pub verified_absent: bool,
}

/// Verified cleanup receipt for one staged chunked write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCleanupReceipt {
    /// Receipt wire version.
    pub receipt_version: String,
    /// Exact caller write identity.
    pub write_id: String,
    /// Exact adapter upload identity.
    pub upload_id: String,
    /// Immutable store binding that performed cleanup.
    pub store_binding: String,
    /// Number of staged multipart/object records removed.
    pub removed_staging_objects: u64,
    /// Number of retained chunk objects removed.
    pub removed_chunks: u64,
    /// Store readback proved all owned staging and chunk objects absent.
    pub verified_absent: bool,
}

/// Durable fence authorizing one exact provider deletion attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDeleteIntent {
    /// Intent wire version.
    pub intent_version: String,
    /// Stable caller-supplied delete identity.
    pub delete_id: String,
    /// Exact eligible GC operation authorizing deletion.
    pub gc_id: String,
    /// Exact semantic Resource identity to remove.
    pub resource_id: String,
    /// Immutable store binding admitted for deletion.
    pub store_binding: String,
    /// Provider-neutral publication used by the admitted deleter.
    pub publication: ResourcePublication,
}

/// Provider observation returned after an idempotent deletion attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceDeletionObservation {
    /// Number of bytes removed by this call; zero on absence replay.
    pub removed_bytes: u64,
    /// Exact provider readback proved the content object absent.
    pub verified_absent: bool,
}

/// Provider-neutral lifecycle operations with exact idempotency receipts.
pub trait ResourceLifecycle {
    /// Retain one Resource under a stable owner pin.
    fn pin(
        &mut self,
        pin_id: &str,
        resource_id: &str,
        owner: &str,
    ) -> ResourceResult<ResourcePinReceipt>;

    /// Release one exact retained pin.
    fn release(&mut self, release_id: &str, pin_id: &str)
    -> ResourceResult<ResourceReleaseReceipt>;

    /// Evaluate collection eligibility under the current exact pins.
    fn garbage_collect(
        &mut self,
        gc_id: &str,
        resource_id: &str,
    ) -> ResourceResult<ResourceGcReceipt>;

    /// Record provider deletion only after exact absence verification.
    fn record_delete(
        &mut self,
        delete_id: &str,
        gc: &ResourceGcReceipt,
        store_binding: &str,
        removed_bytes: u64,
        verified_absent: bool,
    ) -> ResourceResult<ResourceDeleteReceipt>;
}

/// Physical deletion boundary gated by one exact eligible GC receipt.
pub trait ResourceDeleter {
    /// Immutable implementation binding owned by this deleter.
    fn binding(&self) -> &str;

    /// Delete one exact published content object and return provider readback.
    fn delete_resource(
        &mut self,
        intent: &ResourceDeleteIntent,
    ) -> ResourceResult<ResourceDeletionObservation>;
}

/// Deterministic lifecycle authority suitable for durable journal embedding.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLifecycleLedger {
    pins: BTreeMap<String, ResourcePinReceipt>,
    releases: BTreeMap<String, ResourceReleaseReceipt>,
    gc_receipts: BTreeMap<String, ResourceGcReceipt>,
    delete_intents: BTreeMap<String, ResourceDeleteIntent>,
    delete_receipts: BTreeMap<String, ResourceDeleteReceipt>,
}

impl ResourceLifecycleLedger {
    /// Construct an empty lifecycle authority.
    pub const fn new() -> Self {
        Self {
            pins: BTreeMap::new(),
            releases: BTreeMap::new(),
            gc_receipts: BTreeMap::new(),
            delete_intents: BTreeMap::new(),
            delete_receipts: BTreeMap::new(),
        }
    }

    /// Verify all retained receipts and their cross-record authority edges.
    pub fn verify(&self) -> ResourceResult<()> {
        for (pin_id, pin) in &self.pins {
            pin.verify()?;
            if pin_id != &pin.pin_id {
                return Err(ResourceError::Integrity(
                    "Resource pin ledger key changed".to_owned(),
                ));
            }
        }
        for (release_id, release) in &self.releases {
            release.verify()?;
            if release_id != &release.release_id
                || self
                    .pins
                    .get(&release.pin_id)
                    .is_none_or(|pin| pin.resource_id != release.resource_id)
            {
                return Err(ResourceError::Integrity(
                    "Resource release receipt lost its exact pin".to_owned(),
                ));
            }
        }
        for (gc_id, receipt) in &self.gc_receipts {
            receipt.verify()?;
            if gc_id != &receipt.gc_id {
                return Err(ResourceError::Integrity(
                    "Resource GC ledger key changed".to_owned(),
                ));
            }
        }
        for (delete_id, receipt) in &self.delete_receipts {
            receipt.verify()?;
            if delete_id != &receipt.delete_id
                || self.gc_receipts.get(&receipt.gc_id).is_none_or(|gc| {
                    gc.resource_id != receipt.resource_id
                        || gc.disposition != ResourceGcDisposition::Eligible
                })
                || self.delete_intents.get(delete_id).is_none_or(|intent| {
                    intent.gc_id != receipt.gc_id
                        || intent.resource_id != receipt.resource_id
                        || intent.store_binding != receipt.store_binding
                })
            {
                return Err(ResourceError::Integrity(
                    "Resource delete receipt lost its eligible GC authority".to_owned(),
                ));
            }
        }
        for (delete_id, intent) in &self.delete_intents {
            intent.verify()?;
            if delete_id != &intent.delete_id
                || self.gc_receipts.get(&intent.gc_id).is_none_or(|gc| {
                    gc.resource_id != intent.resource_id
                        || gc.disposition != ResourceGcDisposition::Eligible
                })
            {
                return Err(ResourceError::Integrity(
                    "Resource delete intent lost its eligible GC authority".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn pin_is_active(&self, pin_id: &str) -> bool {
        self.pins.contains_key(pin_id)
            && !self
                .releases
                .values()
                .any(|release| release.pin_id == pin_id)
    }

    /// Fence one exact provider deletion after a retained zero-pin GC decision.
    pub fn begin_delete(
        &mut self,
        delete_id: &str,
        gc_id: &str,
        publication: &ResourcePublication,
        store_binding: &str,
    ) -> ResourceResult<ResourceDeleteIntent> {
        validate_identity("delete", delete_id)?;
        validate_identity("GC", gc_id)?;
        validate_identity("store binding", store_binding)?;
        publication.verify()?;
        let gc = self
            .gc_receipts
            .get(gc_id)
            .ok_or_else(|| ResourceError::NotFound(format!("Resource GC {gc_id}")))?;
        if gc.disposition != ResourceGcDisposition::Eligible
            || gc.active_pin_count != 0
            || publication.resource.resource_id != gc.resource_id
            || publication.locators.resolver_binding != store_binding
            || self
                .pins
                .values()
                .any(|pin| pin.resource_id == gc.resource_id && self.pin_is_active(&pin.pin_id))
        {
            return Err(ResourceError::Conflict(
                "Resource delete intent requires a current zero-pin GC decision and exact provider binding"
                    .to_owned(),
            ));
        }
        let intent = ResourceDeleteIntent {
            intent_version: RESOURCE_DELETE_INTENT_VERSION.to_owned(),
            delete_id: delete_id.to_owned(),
            gc_id: gc_id.to_owned(),
            resource_id: gc.resource_id.clone(),
            store_binding: store_binding.to_owned(),
            publication: publication.clone(),
        };
        intent.verify()?;
        if let Some(existing) = self.delete_intents.get(delete_id) {
            return if existing == &intent {
                Ok(existing.clone())
            } else {
                Err(ResourceError::Conflict(format!(
                    "Resource delete {delete_id} was reused with different semantics"
                )))
            };
        }
        self.delete_intents
            .insert(delete_id.to_owned(), intent.clone());
        Ok(intent)
    }
}

/// M1-backed lifecycle authority for pins, releases, collection, and deletion.
pub struct ResourceLifecycleController;

impl ResourceLifecycleController {
    /// Durably retain one exact Resource pin.
    pub fn pin<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        pin_id: &str,
        resource_id: &str,
        owner: &str,
    ) -> ResourceResult<ResourcePinReceipt> {
        let mut ledger = Self::load(coordinator)?;
        let receipt = ledger.pin(pin_id, resource_id, owner)?;
        append(
            coordinator,
            "pin",
            pin_id,
            RESOURCE_PIN_RECEIPT_VERSION,
            &receipt,
        )?;
        Ok(receipt)
    }

    /// Durably release one exact pin.
    pub fn release<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        release_id: &str,
        pin_id: &str,
    ) -> ResourceResult<ResourceReleaseReceipt> {
        let mut ledger = Self::load(coordinator)?;
        let receipt = ledger.release(release_id, pin_id)?;
        append(
            coordinator,
            "release",
            release_id,
            RESOURCE_RELEASE_RECEIPT_VERSION,
            &receipt,
        )?;
        Ok(receipt)
    }

    /// Durably evaluate zero-pin collection eligibility.
    pub fn garbage_collect<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        gc_id: &str,
        resource_id: &str,
    ) -> ResourceResult<ResourceGcReceipt> {
        let mut ledger = Self::load(coordinator)?;
        let receipt = ledger.garbage_collect(gc_id, resource_id)?;
        append(
            coordinator,
            "gc",
            gc_id,
            RESOURCE_GC_RECEIPT_VERSION,
            &receipt,
        )?;
        Ok(receipt)
    }

    /// Commit the durable fence before any provider deletion is attempted.
    pub fn begin_delete<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        delete_id: &str,
        gc_id: &str,
        publication: &ResourcePublication,
        store_binding: &str,
    ) -> ResourceResult<ResourceDeleteIntent> {
        let mut ledger = Self::load(coordinator)?;
        let intent = ledger.begin_delete(delete_id, gc_id, publication, store_binding)?;
        append(
            coordinator,
            "delete-intent",
            delete_id,
            RESOURCE_DELETE_INTENT_VERSION,
            &intent,
        )?;
        Ok(intent)
    }

    /// Reconcile one durable delete intent against its exact provider.
    pub fn reconcile_delete<S: DurableStore>(
        coordinator: &mut DurableCoordinator<S>,
        delete_id: &str,
        deleter: &mut impl ResourceDeleter,
    ) -> ResourceResult<ResourceDeleteReceipt> {
        let mut ledger = Self::load(coordinator)?;
        if let Some(receipt) = ledger.delete_receipts.get(delete_id) {
            return Ok(receipt.clone());
        }
        let intent = ledger
            .delete_intents
            .get(delete_id)
            .cloned()
            .ok_or_else(|| ResourceError::NotFound(format!("Resource delete {delete_id}")))?;
        if deleter.binding() != intent.store_binding {
            return Err(ResourceError::Conflict(
                "selected Resource deleter does not match the durable delete intent".to_owned(),
            ));
        }
        let observation = deleter.delete_resource(&intent)?;
        let gc = ledger
            .gc_receipts
            .get(&intent.gc_id)
            .cloned()
            .ok_or_else(|| {
                ResourceError::Integrity("delete intent lost its GC receipt".to_owned())
            })?;
        let receipt = ledger.record_delete(
            delete_id,
            &gc,
            &intent.store_binding,
            observation.removed_bytes,
            observation.verified_absent,
        )?;
        append(
            coordinator,
            "delete",
            delete_id,
            RESOURCE_DELETE_RECEIPT_VERSION,
            &receipt,
        )?;
        Ok(receipt)
    }

    /// Rebuild and authenticate the complete lifecycle ledger from M1.
    pub fn load<S: DurableStore>(
        coordinator: &DurableCoordinator<S>,
    ) -> ResourceResult<ResourceLifecycleLedger> {
        let records = coordinator
            .journal_records(RESOURCE_LIFECYCLE_JOURNAL_ID)
            .map_err(|error| ResourceError::Persistence(error.to_string()))?;
        let mut ledger = ResourceLifecycleLedger::new();
        for record in records {
            record
                .verify()
                .map_err(|error| ResourceError::Persistence(error.to_string()))?;
            match record.schema.as_str() {
                RESOURCE_PIN_RECEIPT_VERSION => {
                    let receipt: ResourcePinReceipt = decode(record)?;
                    if ledger.pin(&receipt.pin_id, &receipt.resource_id, &receipt.owner)? != receipt
                    {
                        return Err(ResourceError::Integrity(
                            "Resource pin replay changed".to_owned(),
                        ));
                    }
                }
                RESOURCE_RELEASE_RECEIPT_VERSION => {
                    let receipt: ResourceReleaseReceipt = decode(record)?;
                    if ledger.release(&receipt.release_id, &receipt.pin_id)? != receipt {
                        return Err(ResourceError::Integrity(
                            "Resource release replay changed".to_owned(),
                        ));
                    }
                }
                RESOURCE_GC_RECEIPT_VERSION => {
                    let receipt: ResourceGcReceipt = decode(record)?;
                    if ledger.garbage_collect(&receipt.gc_id, &receipt.resource_id)? != receipt {
                        return Err(ResourceError::Integrity(
                            "Resource GC replay changed".to_owned(),
                        ));
                    }
                }
                RESOURCE_DELETE_INTENT_VERSION => {
                    let intent: ResourceDeleteIntent = decode(record)?;
                    if ledger.begin_delete(
                        &intent.delete_id,
                        &intent.gc_id,
                        &intent.publication,
                        &intent.store_binding,
                    )? != intent
                    {
                        return Err(ResourceError::Integrity(
                            "Resource delete intent replay changed".to_owned(),
                        ));
                    }
                }
                RESOURCE_DELETE_RECEIPT_VERSION => {
                    let receipt: ResourceDeleteReceipt = decode(record)?;
                    let gc = ledger
                        .gc_receipts
                        .get(&receipt.gc_id)
                        .cloned()
                        .ok_or_else(|| {
                            ResourceError::Integrity(
                                "Resource delete receipt precedes its GC".to_owned(),
                            )
                        })?;
                    if ledger.record_delete(
                        &receipt.delete_id,
                        &gc,
                        &receipt.store_binding,
                        receipt.removed_bytes,
                        receipt.verified_absent,
                    )? != receipt
                    {
                        return Err(ResourceError::Integrity(
                            "Resource deletion replay changed".to_owned(),
                        ));
                    }
                }
                schema => {
                    return Err(ResourceError::Integrity(format!(
                        "unsupported Resource lifecycle journal schema {schema}"
                    )));
                }
            }
        }
        ledger.verify()?;
        Ok(ledger)
    }
}

fn append<S: DurableStore, T: Serialize>(
    coordinator: &mut DurableCoordinator<S>,
    kind: &str,
    identity: &str,
    schema: &str,
    value: &T,
) -> ResourceResult<()> {
    let record = JournalRecord::new(
        format!("{kind}:{identity}"),
        schema,
        serde_json::to_value(value)
            .map_err(|error| ResourceError::Persistence(error.to_string()))?,
    )
    .map_err(|error| ResourceError::Persistence(error.to_string()))?;
    coordinator
        .append_journal_record(RESOURCE_LIFECYCLE_JOURNAL_ID, record)
        .map_err(|error| match error {
            cymule_durable::DurableError::IllegalTransition(message) => {
                ResourceError::Conflict(message)
            }
            other => ResourceError::Persistence(other.to_string()),
        })?;
    Ok(())
}

fn decode<T: for<'de> Deserialize<'de>>(record: &JournalRecord) -> ResourceResult<T> {
    serde_json::from_value(record.payload.clone())
        .map_err(|error| ResourceError::Integrity(error.to_string()))
}

impl ResourceLifecycle for ResourceLifecycleLedger {
    fn pin(
        &mut self,
        pin_id: &str,
        resource_id: &str,
        owner: &str,
    ) -> ResourceResult<ResourcePinReceipt> {
        validate_identity("pin", pin_id)?;
        validate_digest(resource_id)?;
        validate_identity("pin owner", owner)?;
        let receipt = ResourcePinReceipt {
            receipt_version: RESOURCE_PIN_RECEIPT_VERSION.to_owned(),
            pin_id: pin_id.to_owned(),
            resource_id: resource_id.to_owned(),
            owner: owner.to_owned(),
        };
        if let Some(existing) = self.pins.get(pin_id) {
            return if existing == &receipt {
                Ok(existing.clone())
            } else {
                Err(ResourceError::Conflict(format!(
                    "Resource pin {pin_id} was reused with different semantics"
                )))
            };
        }
        if self
            .delete_receipts
            .values()
            .any(|delete| delete.resource_id == resource_id && delete.verified_absent)
        {
            return Err(ResourceError::Conflict(
                "a deleted Resource cannot receive a historical pin".to_owned(),
            ));
        }
        if self
            .delete_intents
            .values()
            .any(|intent| intent.resource_id == resource_id)
        {
            return Err(ResourceError::Conflict(
                "a delete-fenced Resource cannot receive a new pin".to_owned(),
            ));
        }
        self.pins.insert(pin_id.to_owned(), receipt.clone());
        Ok(receipt)
    }

    fn release(
        &mut self,
        release_id: &str,
        pin_id: &str,
    ) -> ResourceResult<ResourceReleaseReceipt> {
        validate_identity("release", release_id)?;
        validate_identity("pin", pin_id)?;
        if let Some(existing) = self.releases.get(release_id) {
            if existing.pin_id == pin_id {
                return Ok(existing.clone());
            }
            return Err(ResourceError::Conflict(format!(
                "Resource release {release_id} was reused with different semantics"
            )));
        }
        let pin = self
            .pins
            .get(pin_id)
            .ok_or_else(|| ResourceError::NotFound(format!("Resource pin {pin_id}")))?;
        if self
            .releases
            .values()
            .any(|release| release.pin_id == pin_id)
        {
            return Err(ResourceError::Conflict(format!(
                "Resource pin {pin_id} was already released by another operation"
            )));
        }
        let receipt = ResourceReleaseReceipt {
            receipt_version: RESOURCE_RELEASE_RECEIPT_VERSION.to_owned(),
            release_id: release_id.to_owned(),
            pin_id: pin_id.to_owned(),
            resource_id: pin.resource_id.clone(),
        };
        self.releases.insert(release_id.to_owned(), receipt.clone());
        Ok(receipt)
    }

    fn garbage_collect(
        &mut self,
        gc_id: &str,
        resource_id: &str,
    ) -> ResourceResult<ResourceGcReceipt> {
        validate_identity("GC", gc_id)?;
        validate_digest(resource_id)?;
        if let Some(existing) = self.gc_receipts.get(gc_id) {
            if existing.resource_id == resource_id {
                return Ok(existing.clone());
            }
            return Err(ResourceError::Conflict(format!(
                "Resource GC {gc_id} was reused with different semantics"
            )));
        }
        let active_pin_count = self
            .pins
            .values()
            .filter(|pin| pin.resource_id == resource_id && self.pin_is_active(&pin.pin_id))
            .count() as u64;
        let receipt = ResourceGcReceipt {
            receipt_version: RESOURCE_GC_RECEIPT_VERSION.to_owned(),
            gc_id: gc_id.to_owned(),
            resource_id: resource_id.to_owned(),
            active_pin_count,
            disposition: if active_pin_count == 0 {
                ResourceGcDisposition::Eligible
            } else {
                ResourceGcDisposition::Retained
            },
        };
        self.gc_receipts.insert(gc_id.to_owned(), receipt.clone());
        Ok(receipt)
    }

    fn record_delete(
        &mut self,
        delete_id: &str,
        gc: &ResourceGcReceipt,
        store_binding: &str,
        removed_bytes: u64,
        verified_absent: bool,
    ) -> ResourceResult<ResourceDeleteReceipt> {
        validate_identity("delete", delete_id)?;
        gc.verify()?;
        validate_identity("store binding", store_binding)?;
        if !verified_absent {
            return Err(ResourceError::Integrity(
                "Resource deletion requires exact provider absence readback".to_owned(),
            ));
        }
        if self.gc_receipts.get(&gc.gc_id) != Some(gc)
            || gc.disposition != ResourceGcDisposition::Eligible
            || self
                .pins
                .values()
                .any(|pin| pin.resource_id == gc.resource_id && self.pin_is_active(&pin.pin_id))
        {
            return Err(ResourceError::Conflict(
                "Resource deletion requires a retained eligible GC receipt and no later pin"
                    .to_owned(),
            ));
        }
        let intent = self.delete_intents.get(delete_id).ok_or_else(|| {
            ResourceError::Conflict(
                "Resource deletion requires a prior durable delete intent".to_owned(),
            )
        })?;
        if intent.gc_id != gc.gc_id
            || intent.resource_id != gc.resource_id
            || intent.store_binding != store_binding
        {
            return Err(ResourceError::Conflict(
                "Resource deletion observation does not match its durable intent".to_owned(),
            ));
        }
        let receipt = ResourceDeleteReceipt {
            receipt_version: RESOURCE_DELETE_RECEIPT_VERSION.to_owned(),
            delete_id: delete_id.to_owned(),
            gc_id: gc.gc_id.clone(),
            resource_id: gc.resource_id.clone(),
            store_binding: store_binding.to_owned(),
            removed_bytes,
            verified_absent,
        };
        if let Some(existing) = self.delete_receipts.get(delete_id) {
            return if existing == &receipt {
                Ok(existing.clone())
            } else {
                Err(ResourceError::Conflict(format!(
                    "Resource delete {delete_id} was reused with different semantics"
                )))
            };
        }
        self.delete_receipts
            .insert(delete_id.to_owned(), receipt.clone());
        Ok(receipt)
    }
}

impl ResourceDeleteIntent {
    /// Verify the durable delete fence and publication.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.intent_version, RESOURCE_DELETE_INTENT_VERSION)?;
        validate_identity("delete", &self.delete_id)?;
        validate_identity("GC", &self.gc_id)?;
        validate_digest(&self.resource_id)?;
        validate_identity("store binding", &self.store_binding)?;
        self.publication.verify()?;
        if self.publication.resource.resource_id != self.resource_id
            || self.publication.locators.resolver_binding != self.store_binding
        {
            return Err(ResourceError::Validation(
                "Resource delete intent publication does not match its identity or binding"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

impl ResourcePinReceipt {
    /// Verify the closed pin receipt.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_PIN_RECEIPT_VERSION)?;
        validate_identity("pin", &self.pin_id)?;
        validate_digest(&self.resource_id)?;
        validate_identity("pin owner", &self.owner)
    }
}

impl ResourceReleaseReceipt {
    /// Verify the closed release receipt.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_RELEASE_RECEIPT_VERSION)?;
        validate_identity("release", &self.release_id)?;
        validate_identity("pin", &self.pin_id)?;
        validate_digest(&self.resource_id)
    }
}

impl ResourceGcReceipt {
    /// Verify the closed collection decision.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_GC_RECEIPT_VERSION)?;
        validate_identity("GC", &self.gc_id)?;
        validate_digest(&self.resource_id)?;
        if (self.active_pin_count == 0) != (self.disposition == ResourceGcDisposition::Eligible) {
            return Err(ResourceError::Integrity(
                "Resource GC disposition does not match its exact pin count".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ResourceDeleteReceipt {
    /// Verify the closed provider deletion receipt.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_DELETE_RECEIPT_VERSION)?;
        validate_identity("delete", &self.delete_id)?;
        validate_identity("GC", &self.gc_id)?;
        validate_digest(&self.resource_id)?;
        validate_identity("store binding", &self.store_binding)?;
        if !self.verified_absent {
            return Err(ResourceError::Integrity(
                "Resource delete receipt lacks provider absence verification".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ResourceCleanupReceipt {
    /// Verify a closed upload cleanup receipt.
    pub fn verify(&self) -> ResourceResult<()> {
        require_version(&self.receipt_version, RESOURCE_CLEANUP_RECEIPT_VERSION)?;
        validate_identity("write", &self.write_id)?;
        validate_identity("upload", &self.upload_id)?;
        validate_identity("store binding", &self.store_binding)?;
        if !self.verified_absent {
            return Err(ResourceError::Integrity(
                "Resource cleanup receipt lacks staging/chunk absence verification".to_owned(),
            ));
        }
        Ok(())
    }
}

fn require_version(actual: &str, expected: &str) -> ResourceResult<()> {
    if actual != expected {
        return Err(ResourceError::Validation(format!(
            "unsupported Resource lifecycle receipt version {actual:?}"
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> ResourceResult<()> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(ResourceError::Validation(format!(
            "Resource {kind} identity must contain 1..=512 non-control characters"
        )));
    }
    Ok(())
}

fn validate_digest(value: &str) -> ResourceResult<()> {
    let valid = value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if !valid {
        return Err(ResourceError::Validation(
            "Resource identity must be lowercase SHA-256".to_owned(),
        ));
    }
    Ok(())
}
