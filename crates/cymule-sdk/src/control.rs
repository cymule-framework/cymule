use cymule_virtual::{
    RegionMigrationCommand, RegionMigrationReceipt, VirtualClaimCommand, VirtualClaimReceipt,
    VirtualCompactionCommand, VirtualCompactionReceipt, VirtualLeaseRenewalCommand,
    VirtualLeaseRenewalReceipt, VirtualRecoveryCommand, VirtualRecoveryReceipt,
    VirtualRehydrationCommand, VirtualRehydrationReceipt, VirtualRunWeightCommand,
    VirtualRunWeightReceipt, WorkOccurrence, WorkResolutionCommand,
};

/// Transport-neutral query and control interface for M3 virtual work.
///
/// Implementations may use an embedded runtime, RPC, or another reviewed
/// transport. The durable M3 controller remains semantic authority.
pub trait VirtualWorkControl {
    /// Transport or remote-control error.
    type Error;

    /// Query one binding-pinned work occurrence by stable identity.
    fn occurrence(&self, occurrence_id: &str) -> Result<Option<WorkOccurrence>, Self::Error>;

    /// Submit one idempotent, owner/epoch-preconditioned work resolution.
    fn resolve(&mut self, command: &WorkResolutionCommand) -> Result<WorkOccurrence, Self::Error>;

    /// Submit one idempotent adapter-produced split or merge plan.
    fn migrate(
        &mut self,
        command: &RegionMigrationCommand,
    ) -> Result<RegionMigrationReceipt, Self::Error>;

    /// Submit one idempotent completed-region compaction request.
    fn compact(
        &mut self,
        command: &VirtualCompactionCommand,
    ) -> Result<VirtualCompactionReceipt, Self::Error>;

    /// Restore selected exact occurrences from one verified manifest.
    fn rehydrate(
        &mut self,
        command: &VirtualRehydrationCommand,
    ) -> Result<VirtualRehydrationReceipt, Self::Error>;
}

/// Transport-neutral worker scheduling control interface for M3 virtual work.
///
/// Implementations submit typed commands to a durable Rust controller. They do
/// not infer time, retry policy, capacity, or ownership from a transport.
pub trait VirtualSchedulingControl {
    /// Transport or remote-control error.
    type Error;

    /// Claim at most one work item through a fenced capacity slot.
    fn claim(&mut self, command: &VirtualClaimCommand) -> Result<VirtualClaimReceipt, Self::Error>;

    /// Renew one active claim under a later capacity-slot lease epoch.
    fn renew(
        &mut self,
        command: &VirtualLeaseRenewalCommand,
    ) -> Result<VirtualLeaseRenewalReceipt, Self::Error>;

    /// Apply an explicit disposition after the exact claim lease expires.
    fn recover(
        &mut self,
        command: &VirtualRecoveryCommand,
    ) -> Result<VirtualRecoveryReceipt, Self::Error>;

    /// Update one registered Run's future weighted scheduling share.
    fn set_run_weight(
        &mut self,
        command: &VirtualRunWeightCommand,
    ) -> Result<VirtualRunWeightReceipt, Self::Error>;
}
