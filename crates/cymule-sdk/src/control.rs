use cymule_virtual::{
    RegionMigrationCommand, RegionMigrationReceipt, WorkOccurrence, WorkResolutionCommand,
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
}
