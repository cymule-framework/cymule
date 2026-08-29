//! Durable local campaign orchestration over provider-neutral Cymule contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cymule_clock_system::{SqliteClock, WallClock};
use cymule_core::{
    ArtifactRecord, ArtifactRef, SealedPlan, artifact_ref, canonical_bytes, content_id,
    decode_json, sha256_bytes,
};
use cymule_durable::{
    ClockObservationAuthority, DurableResult, DurableRuntimeControl, DurableStore,
    DurableStoreControl,
};
use cymule_evolution::{
    EvolutionCurrentQuery, EvolutionPersistenceCommand, EvolutionPersistenceReceipt,
    EvolutionReceiptQuery, LIVE_EVOLUTION_CONTROL_VERSION, LiveEvolutionCommand,
    LiveEvolutionOutcome, LivePublicationCommand, LivePublicationReceipt, NoEvolutionProviders,
    RolloutMode,
};
use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_resource::{
    ArtifactStore, MAX_WRITE_CHUNK, ResourceClient, ResourceHandle, ResourceIntegrity,
    ResourceShape, ResourceWriteIntent,
};
use cymule_resource_fs::FsResourceStore;
use cymule_runtime::{
    AdmittedPluginRouter, EmbeddedRuntime, ExecutionBinding, ExecutionBindingAdmission,
    ExecutionOperationKind, PluginHost, PluginManifest, RUNTIME_COMPOSITION_VERSION,
    RuntimeCompositionGraph, RuntimeImplementation, RuntimeProviderDescriptor,
};
use cymule_store_sqlite::SqliteStore;
use cymule_virtual::{
    ClaimedWork, FrontierLimits, NoVirtualProviders, ProtocolError, ProtocolResult,
    RegionSourceBinding, ResourceBackedVirtualArchive, SchedulingPolicy,
    VIRTUAL_CLAIM_CONTROL_VERSION, VIRTUAL_INITIALIZATION_CONTROL_VERSION,
    VIRTUAL_MATERIALIZATION_CONTROL_VERSION, VIRTUAL_RECOVERY_CONTROL_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VirtualArchiveBinding, VirtualArchiveProvider,
    VirtualClaimCommand, VirtualClaimOutcome, VirtualClaimPersistenceCommand, VirtualCurrent,
    VirtualCurrentQuery, VirtualCursor, VirtualInitializationCommand,
    VirtualMaterializationCommand, VirtualPersistenceCommand, VirtualPersistenceOperation,
    VirtualPersistenceOutcome, VirtualProviders, VirtualRecoveryCommand,
    VirtualRecoveryPersistenceCommand, VirtualRegion, VirtualRegionLifecycle,
    VirtualRegionMigratorProvider, VirtualRegionSourceProvider,
    VirtualResolutionPersistenceCommand, VirtualRunDefinition, VirtualRunExecution,
    VirtualWorkPlacement, WorkItem, WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
};
use serde::{Deserialize, Serialize};

use crate::evolution::{SCORER_REF, TEMPLATE_ID, campaign_template, scorer_definition};
use crate::model::{
    CAMPAIGN_METADATA_ARTIFACT_KIND, CASE_ARTIFACT_KIND, CaseOutput, ERROR_ARTIFACT_KIND,
    EvaluationCase, MAX_SUITE_BYTES, RESULT_ARTIFACT_KIND, SUITE_MEDIA_TYPE,
};
use crate::plugin::{SCORER_COMPONENT, SUBJECT_COMPONENT};
use crate::source::{
    CURSOR_VERSION, CaseSource, SOURCE_IMPLEMENTATION_REVISION, SOURCE_OPERATION, case_reference,
    parse_suite,
};

const SCHEDULER_ID: &str = "example:virtual-work";
const EVOLUTION_ID: &str = "example:live-evolution";
const REGION_ID: &str = "region:evaluation-suite";
const RESOURCE_BINDING: &str = "example.fs-resources@1";
const RESOURCE_ARCHIVE_REVISION: &str = "example.resource-backed-virtual-archive/1";
const PROCESS_RUNTIME_BINDING: &str = "evaluation-plugin-runtime";
/// Immutable aggregate runtime generation owned by the bundled process plugin.
pub const BUNDLED_PLUGIN_RUNTIME_REVISION: &str =
    "sha256:a704325a40d4e608813aa5f14d01656f9c9a39aab37a43812116b9c7b6311ebe";
const DEFAULT_LEASE_TTL: u64 = 60_000;
const CAMPAIGN_PROCESS_CLOSURE_LIMIT: usize = 128 * 1024 * 1024;
const BASELINE_DEFINITION_COMMAND_ID: &str = "definitions:strict-scorer";
const BASELINE_TEMPLATE_COMMAND_ID: &str = "definitions:campaign-template";

type CampaignResult<T> = Result<T, Box<dyn std::error::Error>>;
type CampaignStore = DurableStoreControl<SqliteStore>;
type CampaignRuntime = DurableRuntimeControl<SqliteStore, AdmittedPluginRouter>;

/// Controlled process-exit boundary used by the documented crash drills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Do not stop early.
    None,
    /// Exit after the numbered durable claim and before subject invocation.
    AfterClaim(usize),
    /// Exit after the numbered terminal result checkpoint.
    AfterCommit(usize),
}

/// Explicit local adapters and execution policy for one campaign command.
#[derive(Debug, Clone)]
pub struct CampaignOptions {
    /// Directory containing `SQLite` state and content-addressed Resources.
    pub state_dir: PathBuf,
    /// Suite file for initialization or optional pinned-byte verification.
    pub suite_path: Option<PathBuf>,
    /// Stable campaign Run identity.
    pub run_id: String,
    /// Absolute process-plugin executable.
    pub plugin_executable: PathBuf,
    /// Provider-owned immutable generation for runtime facilities outside the
    /// captured executable and working tree.
    pub plugin_runtime_revision: String,
    /// Stable identity for this worker process.
    pub worker_id: String,
    /// Optional deterministic wall-clock sample consumed only by the durable
    /// Clock provider. Lease commands never trust this value directly.
    pub logical_now: Option<u64>,
    /// Positive logical capacity-slot lease duration.
    pub lease_ttl: u64,
    /// Optional controlled process-exit boundary.
    pub fault: FaultPoint,
}

impl CampaignOptions {
    /// Construct default local options around a required suite and executable.
    pub fn new(
        state_dir: impl Into<PathBuf>,
        suite_path: impl Into<PathBuf>,
        run_id: impl Into<String>,
        plugin_executable: impl Into<PathBuf>,
        plugin_runtime_revision: impl Into<String>,
    ) -> Self {
        Self {
            state_dir: state_dir.into(),
            suite_path: Some(suite_path.into()),
            run_id: run_id.into(),
            plugin_executable: plugin_executable.into(),
            plugin_runtime_revision: plugin_runtime_revision.into(),
            worker_id: format!("worker:local:{}", std::process::id()),
            logical_now: None,
            lease_ttl: DEFAULT_LEASE_TTL,
            fault: FaultPoint::None,
        }
    }
}

/// Terminal projection for one logical evaluation case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseReport {
    /// Stable case identity from the suite.
    pub case_id: String,
    /// Exact terminal attempt occurrence.
    pub occurrence_id: String,
    /// Immutable linked Plan selected before execution.
    pub plan_id: String,
    /// Terminal occurrence state.
    pub state: String,
    /// Typed subject and score output on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<CaseOutput>,
    /// Retained error evidence on failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// User-facing projection of retained campaign state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignReport {
    /// Stable campaign identity.
    pub run_id: String,
    /// Location-independent identity of the pinned suite Resource.
    pub suite_resource_id: String,
    /// Plan selected for the next future claim.
    pub current_plan_id: String,
    /// Cases declared by the pinned suite.
    pub total_cases: usize,
    /// All attempts, including recovered expired attempts.
    pub total_occurrences: usize,
    /// Attempts explicitly retried after lease expiry.
    pub recovered_attempts: usize,
    /// Cases with a terminal successful output.
    pub succeeded: usize,
    /// Cases with a terminal failure or cancellation.
    pub failed: usize,
    /// Sum of scorer points over successful cases.
    pub points: u64,
    /// Sum of maximum scorer points over successful cases.
    pub max_points: u64,
    /// Stable case-sorted terminal details.
    pub cases: Vec<CaseReport>,
}

/// Whether a run command reached completion or a requested crash boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunDisposition {
    /// No ready, active, or unmaterialized work remains.
    Complete,
    /// The caller requested process termination after a durable boundary.
    SimulatedCrash,
}

/// Run-command result containing disposition and current durable projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CampaignRun {
    /// Terminal command disposition.
    pub disposition: RunDisposition,
    /// Projection committed before the command returned.
    pub report: CampaignReport,
}

/// Result of publishing one scorer revision and relinking future work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionReport {
    /// Requested scorer policy name.
    pub policy: String,
    /// Whether its declared contract matches the parent reference.
    pub compatible: bool,
    /// Future-default Plan before publication.
    pub previous_plan_id: String,
    /// Future-default Plan after compatibility admission.
    pub current_plan_id: String,
    /// Whether the default advanced to a new immutable Plan.
    pub advanced: bool,
    /// Published immutable scorer revision.
    pub revision_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CampaignMetadata {
    metadata_version: String,
    run_id: String,
    suite: ResourceHandle,
    case_count: usize,
}

fn case_source_binding(suite: &ResourceHandle) -> CampaignResult<RegionSourceBinding> {
    Ok(RegionSourceBinding {
        operation: SOURCE_OPERATION.to_owned(),
        binding: content_id(
            "example.evaluation-suite-source-binding/1",
            &(
                suite.resource_id.as_str(),
                CASE_ARTIFACT_KIND,
                CURSOR_VERSION,
                "evaluation",
                0_i32,
                1_u64,
            ),
        )?,
        revision: SOURCE_IMPLEMENTATION_REVISION.to_owned(),
    })
}

/// Run or resume one campaign until completion or a requested fault boundary.
pub fn run(options: &CampaignOptions) -> CampaignResult<CampaignRun> {
    validate_options(options)?;
    fs::create_dir_all(&options.state_dir)?;
    let mut control = open_store_control(options, true)?;
    initialize_evolution(&mut control)?;
    let mut resource_store =
        FsResourceStore::open(options.state_dir.join("resources"), RESOURCE_BINDING)?;
    let (metadata, cases) = load_or_initialize_suite(options, &mut control, &mut resource_store)?;
    let mut providers = CampaignProviders {
        source: CaseSource::new(&cases, case_source_binding(&metadata.suite)?),
        archive: ResourceBackedVirtualArchive::open(
            resource_store,
            RESOURCE_BINDING,
            RESOURCE_ARCHIVE_REVISION,
        )?,
    };
    let mut claimed_this_process = 0_usize;
    let mut committed_this_process = 0_usize;
    let mut captured_runtime: Option<(CampaignRuntime, ExecutionBinding)> = None;
    loop {
        let mut control = open_store_control(options, false)?;
        let (revision, current, region) = read_campaign_current(&mut control)?;
        verify_campaign_region(&region, &options.run_id, &metadata)?;
        verify_campaign_virtual_authority(
            options,
            &mut control,
            &revision,
            &current,
            &region,
            &cases,
        )?;
        read_campaign_evolution(&mut control, &revision, current.body.counts.hot_occurrences)?;
        let frontier_is_empty = current.body.frontier.active.is_empty()
            && current
                .body
                .frontier
                .ready
                .values()
                .all(std::collections::VecDeque::is_empty);
        if frontier_is_empty && region.cursor.exhausted {
            if current.body.counts.parked != 0 {
                return Err("campaign cannot complete while work is parked".into());
            }
            break;
        }
        let (mut runtime, binding) = match captured_runtime.take() {
            Some((runtime, binding)) => (
                reopen_campaign_runtime(options, runtime, &binding)?,
                binding,
            ),
            None => campaign_runtime(options)?,
        };
        if recover_expired_claim(options, &current, &mut runtime, &mut providers)? {
            captured_runtime = Some((runtime, binding));
            continue;
        }
        if frontier_is_empty {
            let command = VirtualPersistenceCommand::new(
                VirtualPersistenceOperation::Materialize(VirtualMaterializationCommand {
                    control_version: VIRTUAL_MATERIALIZATION_CONTROL_VERSION.to_owned(),
                    scheduler_id: SCHEDULER_ID.to_owned(),
                    command_id: stable_id(
                        "fill",
                        &(metadata.suite.resource_id.as_str(), &region.cursor),
                    )?,
                    region_id: REGION_ID.to_owned(),
                    expected_source: case_source_binding(&metadata.suite)?,
                    expected_cursor: region.cursor,
                }),
            )?;
            let commit = runtime.virtual_work(&mut providers).commit(&command)?;
            if !matches!(commit.receipt.outcome,
                VirtualPersistenceOutcome::Materialized { materialized, .. } if materialized > 0)
            {
                return Err("campaign made no progress while work remained".into());
            }
            captured_runtime = Some((runtime, binding));
            continue;
        }
        let Some((claim, plan)) =
            claim_next(options, &current, &binding, &mut runtime, &mut providers)?
        else {
            return Err("campaign claim found no work in a nonempty frontier".into());
        };
        claimed_this_process += 1;
        if options.fault == FaultPoint::AfterClaim(claimed_this_process) {
            return Ok(CampaignRun {
                disposition: RunDisposition::SimulatedCrash,
                report: status(options)?,
            });
        }
        let mut claimed_control = open_read_only_control(options)?;
        let (claimed_revision, claimed_current, _) = read_campaign_current(&mut claimed_control)?;
        let evolution = read_campaign_evolution(
            &mut claimed_control,
            &claimed_revision,
            claimed_current.body.counts.hot_occurrences,
        )?;
        evolution.policy_for_plan(&claim.plan_id)?;
        let resolution = execute_claim(options, &claim, &plan, &evolution, &cases)?;
        runtime = reopen_campaign_runtime(options, runtime, &binding)?;
        runtime.virtual_work(&mut providers).commit(&resolution)?;
        captured_runtime = Some((runtime, binding));
        committed_this_process += 1;
        if options.fault == FaultPoint::AfterCommit(committed_this_process) {
            return Ok(CampaignRun {
                disposition: RunDisposition::SimulatedCrash,
                report: status(options)?,
            });
        }
    }
    Ok(CampaignRun {
        disposition: RunDisposition::Complete,
        report: status(options)?,
    })
}

/// Verify retained resources and project current campaign status without work.
pub fn status(options: &CampaignOptions) -> CampaignResult<CampaignReport> {
    validate_options(options)?;
    let mut control = open_read_only_control(options)?;
    let (revision, current, region) = read_campaign_current(&mut control)?;
    let metadata = retained_metadata(&mut control, &region.source_artifact, &revision)?;
    verify_metadata_run(&metadata, &options.run_id)?;
    verify_campaign_region(&region, &options.run_id, &metadata)?;
    let resource_store =
        FsResourceStore::open_read_only(options.state_dir.join("resources"), RESOURCE_BINDING)?;
    let suite_bytes = verify_suite_resource(&metadata.suite, &resource_store)?;
    let cases = parse_suite(&suite_bytes)?;
    if cases.len() != metadata.case_count {
        return Err("retained suite case count changed".into());
    }
    verify_requested_suite(options.suite_path.as_deref(), &metadata.suite)?;
    verify_campaign_virtual_authority(options, &mut control, &revision, &current, &region, &cases)?;
    let evolution =
        read_campaign_evolution(&mut control, &revision, current.body.counts.hot_occurrences)?;
    build_report(
        options,
        &mut control,
        &revision,
        &current,
        &evolution,
        &metadata,
        &cases,
    )
}

/// Publish a compatible or deliberately incompatible scorer revision.
pub fn evolve(options: &CampaignOptions, policy: &str) -> CampaignResult<EvolutionReport> {
    validate_options(options)?;
    let compatible = match policy {
        "weighted" => true,
        "incompatible" => false,
        _ => return Err("policy must be weighted or incompatible".into()),
    };
    // Complete the same read-only provenance checks as status before opening a writer.
    let _ = status(options)?;
    let mut control = open_store_control(options, false)?;
    let command = EvolutionPersistenceCommand::new(
        EVOLUTION_ID,
        campaign_publication_command(policy, compatible)?,
    )?;
    let commit = control
        .evolution(&mut NoEvolutionProviders)
        .commit(&command)?;
    let LiveEvolutionOutcome::PublicationApplied { receipt } = commit.receipt.outcome else {
        return Err("campaign publication returned a different live-evolution outcome".into());
    };
    let update = sole_publication_update(&receipt)?;
    let report = EvolutionReport {
        policy: policy.to_owned(),
        compatible,
        previous_plan_id: update.previous_plan_id.clone(),
        current_plan_id: update.current_plan_id.clone(),
        advanced: update.advanced,
        revision_id: receipt.revision.revision_id.clone(),
    };
    // This readback validates the actual current head independently of a replayed
    // publication receipt, whose historical before/after values remain unchanged.
    let _ = status(options)?;
    Ok(report)
}

fn store_domain(options: &CampaignOptions) -> String {
    format!("campaign:{}", options.run_id)
}

fn open_store_control(
    options: &CampaignOptions,
    initialize: bool,
) -> CampaignResult<CampaignStore> {
    let path = options.state_dir.join("campaign.sqlite");
    if !initialize {
        let mut reader = SqliteStore::open_read_only(&path, store_domain(options))?;
        if reader.load_head()?.is_none() {
            return Err("campaign has not been initialized".into());
        }
    }
    let mut store = SqliteStore::open(path, store_domain(options))?;
    if store.load_head()?.is_none() {
        if !initialize {
            return Err("campaign has not been initialized".into());
        }
        return DurableStoreControl::initialize(store).map_err(Into::into);
    }
    DurableStoreControl::open(store).map_err(Into::into)
}

fn open_read_only_control(options: &CampaignOptions) -> CampaignResult<CampaignStore> {
    let mut store = SqliteStore::open_read_only(
        options.state_dir.join("campaign.sqlite"),
        store_domain(options),
    )?;
    if store.load_head()?.is_none() {
        return Err("campaign has not been initialized".into());
    }
    DurableStoreControl::open(store).map_err(Into::into)
}

fn read_campaign_current(
    control: &mut CampaignStore,
) -> CampaignResult<(String, VirtualCurrent, VirtualRegion)> {
    let read = control.virtual_read().read_current(&VirtualCurrentQuery {
        scheduler_id: SCHEDULER_ID.to_owned(),
        expected_revision: None,
    })?;
    let current = read
        .current
        .ok_or("campaign evaluation region is missing")?;
    if current.body.counts.regions != 1 {
        return Err("campaign virtual state contains an unexpected region set".into());
    }
    let region = control
        .virtual_read()
        .read_region(SCHEDULER_ID, REGION_ID, &read.observed_revision)?
        .value
        .ok_or("campaign evaluation region is missing")?;
    if region.lifecycle != VirtualRegionLifecycle::Active
        || region.compaction_certificate_id.is_some()
    {
        return Err("campaign region has an unsupported lifecycle".into());
    }
    Ok((read.observed_revision, current, region.region))
}

fn load_or_initialize_suite(
    options: &CampaignOptions,
    control: &mut CampaignStore,
    resource_store: &mut FsResourceStore,
) -> CampaignResult<(CampaignMetadata, Vec<EvaluationCase>)> {
    let read = control.virtual_read().read_current(&VirtualCurrentQuery {
        scheduler_id: SCHEDULER_ID.to_owned(),
        expected_revision: None,
    })?;
    if read.current.is_some() {
        let (revision, current, region) = read_campaign_current(control)?;
        let metadata = retained_metadata(control, &region.source_artifact, &revision)?;
        verify_metadata_run(&metadata, &options.run_id)?;
        verify_campaign_region(&region, &options.run_id, &metadata)?;
        verify_requested_suite(options.suite_path.as_deref(), &metadata.suite)?;
        let bytes = verify_suite_resource(&metadata.suite, resource_store)?;
        let cases = parse_suite(&bytes)?;
        if cases.len() != metadata.case_count {
            return Err("retained suite case count changed".into());
        }
        verify_campaign_virtual_authority(options, control, &revision, &current, &region, &cases)?;
        read_campaign_evolution(control, &revision, current.body.counts.hot_occurrences)?;
        return Ok((metadata, cases));
    }
    // Only the exact two-command baseline may precede first region publication.
    read_campaign_evolution(control, &read.observed_revision, 0)?;
    let path = options
        .suite_path
        .as_deref()
        .ok_or("a suite path is required to initialize the campaign")?;
    let bytes = read_suite_file(path)?;
    let cases = parse_suite(&bytes)?;
    let suite = store_suite_bytes(resource_store, options, &bytes)?;
    let metadata = CampaignMetadata {
        metadata_version: CAMPAIGN_METADATA_ARTIFACT_KIND.to_owned(),
        run_id: options.run_id.clone(),
        suite,
        case_count: cases.len(),
    };
    let command = suite_initialization_command(options, &metadata)?;
    campaign_runtime(options)?
        .0
        .virtual_work(&mut NoVirtualProviders)
        .commit(&command)?;
    Ok((metadata, cases))
}

fn suite_initialization_command(
    options: &CampaignOptions,
    metadata: &CampaignMetadata,
) -> CampaignResult<VirtualPersistenceCommand> {
    let metadata_artifact = metadata_artifact(metadata)?;
    VirtualPersistenceCommand::new(VirtualPersistenceOperation::Initialize(
        VirtualInitializationCommand {
            control_version: VIRTUAL_INITIALIZATION_CONTROL_VERSION.to_owned(),
            scheduler_id: SCHEDULER_ID.to_owned(),
            command_id: "virtual:register-suite".to_owned(),
            limits: frontier_limits(),
            scheduling_policy: SchedulingPolicy::default(),
            archive: VirtualArchiveBinding::new(RESOURCE_BINDING, RESOURCE_ARCHIVE_REVISION)?,
            regions: vec![VirtualRegion {
                region_id: REGION_ID.to_owned(),
                run_id: options.run_id.clone(),
                source: case_source_binding(&metadata.suite)?,
                source_artifact: metadata_artifact.reference.clone(),
                cursor: VirtualCursor {
                    version: CURSOR_VERSION.to_owned(),
                    position: "0".to_owned(),
                    exhausted: false,
                },
                estimated_total: Some(u64::try_from(metadata.case_count)?),
            }],
            runs: vec![VirtualRunDefinition {
                run_id: options.run_id.clone(),
                execution: campaign_run_execution(),
            }],
            source_artifacts: vec![metadata_artifact],
        },
    ))
    .map_err(Into::into)
}

fn campaign_run_execution() -> VirtualRunExecution {
    VirtualRunExecution::Evolution {
        evolution_id: EVOLUTION_ID.to_owned(),
        template_id: TEMPLATE_ID.to_owned(),
    }
}

fn verify_campaign_region(
    region: &VirtualRegion,
    run_id: &str,
    metadata: &CampaignMetadata,
) -> CampaignResult<()> {
    let position: usize = region
        .cursor
        .position
        .parse()
        .map_err(|_| "campaign region cursor is not numeric")?;
    let expected_total = u64::try_from(metadata.case_count)?;
    let expected_source_artifact = metadata_artifact(metadata)?.reference;
    if region.run_id != run_id
        || region.source != case_source_binding(&metadata.suite)?
        || region.source_artifact != expected_source_artifact
        || region.cursor.version != CURSOR_VERSION
        || region.cursor.position != position.to_string()
        || position > metadata.case_count
        || region.cursor.exhausted != (position == metadata.case_count)
        || region.estimated_total != Some(expected_total)
    {
        return Err("durable region does not match the requested Run and pinned suite".into());
    }
    Ok(())
}

fn retained_metadata(
    control: &mut CampaignStore,
    reference: &ArtifactRef,
    revision: &str,
) -> CampaignResult<CampaignMetadata> {
    let artifact = control
        .read_artifact(reference, revision)?
        .value
        .ok_or("campaign suite metadata Artifact is missing")?;
    let metadata: CampaignMetadata = decode_json(&artifact.bytes)?;
    let expected = metadata_artifact(&metadata)?;
    if expected.reference != *reference || expected.bytes != artifact.bytes {
        return Err("campaign metadata Artifact is not canonical JSON".into());
    }
    if metadata.metadata_version != CAMPAIGN_METADATA_ARTIFACT_KIND {
        return Err("unsupported campaign metadata version".into());
    }
    metadata.suite.verify()?;
    Ok(metadata)
}

fn metadata_artifact(metadata: &CampaignMetadata) -> CampaignResult<ArtifactRecord> {
    let bytes = canonical_bytes(metadata)?;
    let reference = artifact_ref(CAMPAIGN_METADATA_ARTIFACT_KIND, &bytes)?;
    Ok(ArtifactRecord { reference, bytes })
}

fn verify_metadata_run(metadata: &CampaignMetadata, run_id: &str) -> CampaignResult<()> {
    if metadata.run_id != run_id {
        return Err("campaign metadata belongs to a different Run".into());
    }
    Ok(())
}

fn store_suite_bytes(
    store: &mut FsResourceStore,
    options: &CampaignOptions,
    bytes: &[u8],
) -> CampaignResult<ResourceHandle> {
    let intent = ResourceWriteIntent {
        write_id: stable_id(
            "suite-write",
            &(options.run_id.as_str(), sha256_bytes(bytes)),
        )?,
        shape: ResourceShape::Object,
        media_type: SUITE_MEDIA_TYPE.to_owned(),
        annotations: BTreeMap::new(),
    };
    let session = store.begin_write(&intent)?;
    for (index, chunk) in bytes.chunks(MAX_WRITE_CHUNK).enumerate() {
        store.write_chunk(
            &session,
            u64::try_from(
                index
                    .checked_mul(MAX_WRITE_CHUNK)
                    .ok_or("suite offset overflow")?,
            )?,
            chunk,
        )?;
    }
    let resource = store.commit_write(&session)?;
    let expected_digest = format!("sha256:{}", sha256_bytes(bytes));
    let expected_size = u64::try_from(bytes.len())?;
    if !matches!(
        resource.resource.integrity,
        ResourceIntegrity::Content { ref digest, size }
            if digest == &expected_digest && size == expected_size
    ) {
        return Err("filesystem store published different suite bytes".into());
    }
    Ok(resource.resource)
}

fn initialize_evolution(control: &mut CampaignStore) -> CampaignResult<()> {
    let read =
        control
            .evolution(&mut NoEvolutionProviders)
            .read_current(&EvolutionCurrentQuery {
                evolution_id: EVOLUTION_ID.to_owned(),
                expected_revision: None,
            })?;
    let definition = baseline_definition_command();
    let template = baseline_template_command();
    let definition_receipt = read_evolution_receipt(control, &definition, &read.observed_revision)?;
    let template_receipt = read_evolution_receipt(control, &template, &read.observed_revision)?;
    match (&read.current, definition_receipt, template_receipt) {
        (None, None, None) => {
            for command in [definition, template] {
                control
                    .evolution(&mut NoEvolutionProviders)
                    .commit(&EvolutionPersistenceCommand::new(EVOLUTION_ID, command)?)?;
            }
        }
        (Some(current), Some(receipt), None)
            if current.revision == 1 && current.last_receipt_id == receipt.receipt_id =>
        {
            control
                .evolution(&mut NoEvolutionProviders)
                .commit(&EvolutionPersistenceCommand::new(EVOLUTION_ID, template)?)?;
        }
        (Some(_), Some(_), Some(_)) => {}
        _ => {
            return Err(
                "campaign live-evolution authority is not an exact baseline or baseline prefix"
                    .into(),
            );
        }
    }
    Ok(())
}

fn baseline_definition_command() -> LiveEvolutionCommand {
    LiveEvolutionCommand::PublishDefinition {
        control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: BASELINE_DEFINITION_COMMAND_ID.to_owned(),
        logical_ref: SCORER_REF.to_owned(),
        definition: scorer_definition("strict", true),
        references: Vec::new(),
    }
}

fn baseline_template_command() -> LiveEvolutionCommand {
    LiveEvolutionCommand::RegisterTemplate {
        control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: BASELINE_TEMPLATE_COMMAND_ID.to_owned(),
        template: campaign_template(),
    }
}

fn campaign_publication_command(
    policy: &str,
    compatible: bool,
) -> CampaignResult<LiveEvolutionCommand> {
    let definition = scorer_definition(policy, compatible);
    let evidence_bytes = canonical_bytes(&(policy, &definition))?;
    Ok(LiveEvolutionCommand::PublishAndRelink {
        control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
        command_id: stable_id("evolve", &(policy, &definition))?,
        publication: LivePublicationCommand {
            logical_ref: SCORER_REF.to_owned(),
            definition,
            references: Vec::new(),
            evidence: ArtifactRecord {
                reference: artifact_ref("example.evolution-review/1", &evidence_bytes)?,
                bytes: evidence_bytes,
            },
            mode: RolloutMode::Active,
        },
    })
}

fn read_evolution_receipt(
    control: &mut CampaignStore,
    command: &LiveEvolutionCommand,
    revision: &str,
) -> CampaignResult<Option<EvolutionPersistenceReceipt>> {
    let expected = EvolutionPersistenceCommand::new(EVOLUTION_ID, command.clone())?;
    let read =
        control
            .evolution(&mut NoEvolutionProviders)
            .read_receipt(&EvolutionReceiptQuery {
                evolution_id: EVOLUTION_ID.to_owned(),
                command_id: command.command_id().to_owned(),
                expected_revision: Some(revision.to_owned()),
            })?;
    if read
        .receipt
        .as_ref()
        .is_some_and(|receipt| receipt.command != expected)
    {
        return Err("campaign command identity was reused with different semantics".into());
    }
    Ok(read.receipt)
}

/// Small application projection of the two executable policies, not an M4
/// registry, reducer, or history snapshot. Its identities come from exact
/// framework receipts and the explicit current-template read.
struct CampaignEvolution {
    baseline: String,
    weighted: Option<String>,
    current: String,
}

impl CampaignEvolution {
    fn policy_for_plan(&self, plan_id: &str) -> CampaignResult<&'static str> {
        if plan_id == self.baseline {
            Ok("strict")
        } else if self.weighted.as_deref() == Some(plan_id) {
            Ok("weighted")
        } else {
            Err("campaign occurrence references an unsupported Plan".into())
        }
    }
}

fn sole_publication_update(
    receipt: &LivePublicationReceipt,
) -> CampaignResult<&cymule_evolution::LiveTemplateUpdate> {
    let [update] = receipt.updates.as_slice() else {
        return Err("campaign publication must retain exactly one template update".into());
    };
    if update.template_id != TEMPLATE_ID {
        return Err("campaign publication targets a foreign template".into());
    }
    Ok(update)
}

fn read_campaign_evolution(
    control: &mut CampaignStore,
    revision: &str,
    occurrence_count: u64,
) -> CampaignResult<CampaignEvolution> {
    let current = control
        .evolution(&mut NoEvolutionProviders)
        .read_current(&EvolutionCurrentQuery {
            evolution_id: EVOLUTION_ID.to_owned(),
            expected_revision: Some(revision.to_owned()),
        })?
        .current
        .ok_or("campaign evolution authority is missing")?;
    let definition = read_evolution_receipt(control, &baseline_definition_command(), revision)?
        .ok_or("campaign live-evolution definition receipt is missing")?;
    let LiveEvolutionOutcome::DefinitionPublished {
        revision: baseline_revision,
    } = definition.outcome
    else {
        return Err("campaign definition command retained the wrong outcome".into());
    };
    let template = read_evolution_receipt(control, &baseline_template_command(), revision)?
        .ok_or("campaign live-evolution template receipt is missing")?;
    let LiveEvolutionOutcome::TemplateRegistered { linked } = template.outcome else {
        return Err("campaign template command retained the wrong outcome".into());
    };
    if linked.template_id != TEMPLATE_ID
        || linked.resolved_revisions.len() != 1
        || linked.resolved_revisions.get(SCORER_REF) != Some(&baseline_revision.revision_id)
    {
        return Err("campaign baseline does not own the strict linked Plan".into());
    }
    let baseline_plan_id = linked.plan.plan_id;
    let mut weighted_plan_id = None;
    let mut publication_count = 0_u64;
    for (policy, compatible) in [("weighted", true), ("incompatible", false)] {
        let command = campaign_publication_command(policy, compatible)?;
        let Some(receipt) = read_evolution_receipt(control, &command, revision)? else {
            continue;
        };
        let LiveEvolutionOutcome::PublicationApplied { receipt } = receipt.outcome else {
            return Err("campaign publication retained the wrong outcome".into());
        };
        let update = sole_publication_update(&receipt)?;
        if compatible {
            if !update.advanced
                || update.decision_id.is_none()
                || update.previous_plan_id != baseline_plan_id
                || update.current_plan_id == baseline_plan_id
            {
                return Err("weighted campaign publication did not make one exact advance".into());
            }
            weighted_plan_id = Some(update.current_plan_id.clone());
        } else if update.advanced
            || update.decision_id.is_some()
            || update.previous_plan_id != update.current_plan_id
        {
            return Err("incompatible campaign publication changed rollout authority".into());
        }
        publication_count += 1;
    }
    // Every application-authorized M4 command is one of the two baseline
    // commands, the two named publications, or a coupled Virtual occurrence.
    let expected_count = 2_u64
        .checked_add(publication_count)
        .and_then(|value| value.checked_add(occurrence_count))
        .ok_or("campaign evolution count overflow")?;
    if current.revision != expected_count {
        return Err("campaign evolution contains an unsupported control history".into());
    }
    let current_plan_id = control
        .evolution(&mut NoEvolutionProviders)
        .read_template_plan_id(EVOLUTION_ID, TEMPLATE_ID, revision)?
        .value
        .ok_or("campaign template has no current linked Plan")?;
    let evolution = CampaignEvolution {
        baseline: baseline_plan_id,
        weighted: weighted_plan_id,
        current: current_plan_id,
    };
    evolution.policy_for_plan(&evolution.current)?;
    Ok(evolution)
}

struct CampaignProviders<'a> {
    source: CaseSource<'a>,
    archive: ResourceBackedVirtualArchive<FsResourceStore>,
}

impl VirtualProviders for CampaignProviders<'_> {
    fn region_source(
        &mut self,
        binding: &RegionSourceBinding,
    ) -> ProtocolResult<&mut dyn VirtualRegionSourceProvider> {
        if self.source.source_binding() != *binding {
            return Err(ProtocolError::Validation(
                "campaign source binding is not the retained exact generation".to_owned(),
            ));
        }
        Ok(&mut self.source)
    }

    fn region_migrator(
        &mut self,
        _binding: &str,
        _revision: &str,
    ) -> ProtocolResult<&mut dyn VirtualRegionMigratorProvider> {
        Err(ProtocolError::Validation(
            "campaign does not register region migration".to_owned(),
        ))
    }

    fn archive(
        &mut self,
        binding: &VirtualArchiveBinding,
    ) -> ProtocolResult<&mut dyn VirtualArchiveProvider> {
        if self.archive.archive_binding() != *binding {
            return Err(ProtocolError::Validation(
                "campaign archive binding is not the retained exact generation".to_owned(),
            ));
        }
        Ok(&mut self.archive)
    }
}

fn claim_command_id(
    options: &CampaignOptions,
    slot_id: &str,
    clock_id: &str,
) -> CampaignResult<String> {
    stable_id(
        "claim",
        &(
            options.run_id.as_str(),
            options.worker_id.as_str(),
            slot_id,
            clock_id,
        ),
    )
}

fn claim_next(
    options: &CampaignOptions,
    current: &VirtualCurrent,
    binding: &ExecutionBinding,
    runtime: &mut CampaignRuntime,
    providers: &mut CampaignProviders<'_>,
) -> CampaignResult<Option<(ClaimedWork, SealedPlan)>> {
    let slot_id = stable_id(
        "slot",
        &(options.run_id.as_str(), options.worker_id.as_str()),
    )?;
    let mut clock = open_campaign_clock(options)?;
    let (clock_reference, lease_ttl) =
        if let Some(active) = current.body.frontier.active.values().next() {
            if active.owner != options.worker_id || active.lease.resource != slot_id {
                return Err("campaign active claim belongs to another worker".into());
            }
            let observed = clock.resolve(&active.lease.clock)?;
            let lease_ttl = active
                .lease
                .expires_at
                .checked_sub(observed.logical_time)
                .ok_or("retained claim has invalid lease authority")?;
            (active.lease.clock.clone(), lease_ttl)
        } else {
            (clock.observe(&slot_id)?.reference(), options.lease_ttl)
        };
    let command = VirtualClaimPersistenceCommand {
        scheduler_id: SCHEDULER_ID.to_owned(),
        command: VirtualClaimCommand {
            control_version: VIRTUAL_CLAIM_CONTROL_VERSION.to_owned(),
            command_id: claim_command_id(options, &slot_id, &clock_reference.observation_id)?,
            owner: options.worker_id.clone(),
            slot_id,
            execution_binding: binding.artifact_ref()?,
            capabilities: BTreeSet::from(["evaluation".to_owned()]),
            clock: clock_reference,
            lease_ttl,
        },
    };
    let outcome = runtime.virtual_work(providers).claim(&command)?;
    outcome.verify()?;
    if outcome.receipt().command
        != VirtualPersistenceCommand::new(VirtualPersistenceOperation::Claim(command))?
    {
        return Err("campaign claim returned another command receipt".into());
    }
    match outcome {
        VirtualClaimOutcome::NoWork { .. } => Ok(None),
        VirtualClaimOutcome::Claimed {
            receipt,
            claim,
            plan,
        } => {
            let VirtualPersistenceOutcome::Claimed(retained) = receipt.outcome else {
                return Err("campaign claim returned a non-claim outcome".into());
            };
            if retained.run_execution != Some(campaign_run_execution())
                || retained
                    .evolution_selection
                    .as_ref()
                    .is_none_or(|selection| {
                        selection.pin.template_id != TEMPLATE_ID
                            || selection.pin.occurrence_id != claim.occurrence_id
                            || selection.pin.plan_id != claim.plan_id
                            || selection.pin.execution_binding != claim.execution_binding
                    })
            {
                return Err("campaign claim has no exact coupled evolution selection".into());
            }
            Ok(Some((*claim, *plan)))
        }
    }
}

fn recover_expired_claim(
    options: &CampaignOptions,
    current: &VirtualCurrent,
    runtime: &mut CampaignRuntime,
    providers: &mut CampaignProviders<'_>,
) -> CampaignResult<bool> {
    let Some(active) = current.body.frontier.active.values().next() else {
        return Ok(false);
    };
    let mut clock = open_campaign_clock(options)?;
    let observation = clock.observe(&active.lease.resource)?;
    if active.lease.expires_at > observation.logical_time {
        if active.owner == options.worker_id {
            return Ok(false);
        }
        return Err(format!(
            "case {} is owned by {} until logical time {}; retry after lease expiry",
            active.item.work_id, active.owner, active.lease.expires_at,
        )
        .into());
    }
    let bytes = format!(
        "worker {} did not publish a result before lease {} expired",
        active.owner, active.lease.epoch,
    )
    .into_bytes();
    let artifact = ArtifactRecord {
        reference: artifact_ref(ERROR_ARTIFACT_KIND, &bytes)?,
        bytes,
    };
    let command = VirtualPersistenceCommand::new(VirtualPersistenceOperation::Recover(
        VirtualRecoveryPersistenceCommand {
            scheduler_id: SCHEDULER_ID.to_owned(),
            command: VirtualRecoveryCommand {
                control_version: VIRTUAL_RECOVERY_CONTROL_VERSION.to_owned(),
                command_id: stable_id("recover", &active.occurrence_id)?,
                work_id: active.item.work_id.clone(),
                expected_owner: active.owner.clone(),
                expected_epoch: active.epoch,
                expected_lease_epoch: active.lease.epoch,
                clock: observation.reference(),
                resolution: WorkResolution::Retry {
                    error: artifact.reference.clone(),
                    next_reason: None,
                },
            },
            artifact,
        },
    ))?;
    runtime.virtual_work(providers).commit(&command)?;
    Ok(true)
}

fn execute_claim(
    options: &CampaignOptions,
    claim: &ClaimedWork,
    plan: &SealedPlan,
    evolution: &CampaignEvolution,
    cases: &[EvaluationCase],
) -> CampaignResult<VirtualPersistenceCommand> {
    let expected_policy = evolution.policy_for_plan(&claim.plan_id)?;
    if plan.plan_id != claim.plan_id || claim.item.payload.kind != CASE_ARTIFACT_KIND {
        return Err("claimed Plan or case payload kind changed".into());
    }
    let case_id = claim
        .item
        .work_id
        .strip_prefix("case:")
        .ok_or("work ID is not an evaluation case")?;
    let case = cases
        .iter()
        .find(|candidate| candidate.id == case_id)
        .ok_or_else(|| format!("claimed case {case_id:?} is absent from the pinned suite"))?;
    case.validate()?;
    if case_reference(case)? != claim.item.payload {
        return Err("claimed case payload does not match the pinned suite bytes".into());
    }
    let (router, binding) = campaign_router(options)?;
    if binding.artifact_ref()? != claim.execution_binding {
        return Err("claim execution binding does not match admitted composition".into());
    }
    let mut runtime = EmbeddedRuntime::new(router, binding)?;
    let execution = runtime
        .execute(
            plan.clone(),
            &serde_json::to_value(case)?,
            stable_id(
                "evaluation",
                &(options.run_id.as_str(), claim.occurrence_id.as_str()),
            )?,
        )
        .and_then(cymule_runtime::ExecutionOutcome::into_completed)
        .map_err(|error| error.to_string())
        .and_then(|result| {
            let output: CaseOutput = serde_json::from_value(result.value)
                .map_err(|error| format!("evaluation returned invalid output: {error}"))?;
            output
                .validate_for(case, expected_policy)
                .map_err(|error| format!("evaluation returned invalid output: {error}"))?;
            Ok(output)
        });
    let (resolution, artifact) = match execution {
        Ok(output) => {
            let bytes = canonical_bytes(&output)?;
            let artifact = ArtifactRecord {
                reference: artifact_ref(RESULT_ARTIFACT_KIND, &bytes)?,
                bytes,
            };
            (
                WorkResolution::Succeeded {
                    result: artifact.reference.clone(),
                },
                artifact,
            )
        }
        Err(error) => {
            let bytes = error.into_bytes();
            let artifact = ArtifactRecord {
                reference: artifact_ref(ERROR_ARTIFACT_KIND, &bytes)?,
                bytes,
            };
            (
                WorkResolution::Failed {
                    error: artifact.reference.clone(),
                },
                artifact,
            )
        }
    };
    let mut clock = open_campaign_clock(options)?;
    VirtualPersistenceCommand::new(VirtualPersistenceOperation::Resolve(
        VirtualResolutionPersistenceCommand {
            scheduler_id: SCHEDULER_ID.to_owned(),
            command: WorkResolutionCommand {
                control_version: VIRTUAL_WORK_CONTROL_VERSION.to_owned(),
                command_id: stable_id("resolve", &claim.occurrence_id)?,
                work_id: claim.item.work_id.clone(),
                owner: claim.owner.clone(),
                epoch: claim.epoch,
                expected_lease_epoch: claim.lease.epoch,
                clock: clock.observe(&claim.lease.resource)?.reference(),
                resolution,
            },
            artifact: Some(artifact),
        },
    ))
    .map_err(Into::into)
}

fn campaign_executor(options: &CampaignOptions) -> CampaignResult<ProcessExecutor> {
    let mut config = ProcessExecutorConfig::new(
        &options.plugin_executable,
        BTreeMap::from([(
            PROCESS_RUNTIME_BINDING.to_owned(),
            options.plugin_runtime_revision.clone(),
        )]),
    );
    config.arguments = vec!["__plugin".to_owned()];
    config.closure_limit = CAMPAIGN_PROCESS_CLOSURE_LIMIT;
    ProcessExecutor::new(config).map_err(Into::into)
}

fn campaign_binding(
    manifest: PluginManifest,
    implementation_revision: &str,
) -> CampaignResult<ExecutionBinding> {
    let empty_digest = format!("sha256:{}", sha256_bytes(b"{}"));
    let providers = [
        (
            "evaluation-subject",
            SUBJECT_COMPONENT,
            "evaluation-subject",
        ),
        ("evaluation-scorer", SCORER_COMPONENT, "evaluation-scorer"),
    ]
    .into_iter()
    .map(
        |(provider_id, operation, capability)| RuntimeProviderDescriptor {
            version: RUNTIME_COMPOSITION_VERSION.to_owned(),
            provider_id: provider_id.to_owned(),
            implementation: RuntimeImplementation {
                implementation_id: manifest.implementation_id.clone(),
                revision: implementation_revision.to_owned(),
            },
            provides: vec![ExecutionOperationKind::Component.service_key(operation)],
            requires: Vec::new(),
            properties: BTreeMap::from([("capability".to_owned(), capability.to_owned())]),
            configuration_schema_digest: empty_digest.clone(),
            configuration_fingerprint: empty_digest.clone(),
        },
    )
    .collect();
    let graph = RuntimeCompositionGraph::build(providers)?;
    ExecutionBinding::admit(
        &graph,
        &BTreeMap::from([
            ("evaluation-subject".to_owned(), manifest.clone()),
            ("evaluation-scorer".to_owned(), manifest),
        ]),
    )
    .map_err(Into::into)
}

fn campaign_router(
    options: &CampaignOptions,
) -> CampaignResult<(AdmittedPluginRouter, ExecutionBinding)> {
    let mut subject = campaign_executor(options)?;
    let manifest = subject.describe()?;
    let scorer = campaign_executor(options)?;
    if subject.implementation_revision() != scorer.implementation_revision() {
        return Err("campaign provider closure changed during capture".into());
    }
    let binding = campaign_binding(manifest, subject.implementation_revision())?;
    let providers: BTreeMap<String, Box<dyn PluginHost>> = BTreeMap::from([
        (
            "evaluation-subject".to_owned(),
            Box::new(subject) as Box<dyn PluginHost>,
        ),
        (
            "evaluation-scorer".to_owned(),
            Box::new(scorer) as Box<dyn PluginHost>,
        ),
    ]);
    Ok((
        AdmittedPluginRouter::new(binding.clone(), providers)?,
        binding,
    ))
}

fn campaign_runtime(
    options: &CampaignOptions,
) -> CampaignResult<(CampaignRuntime, ExecutionBinding)> {
    let (router, binding) = campaign_router(options)?;
    let admission = ExecutionBindingAdmission::admit(router, binding.clone())?;
    let clock = open_campaign_clock(options)?;
    let store = SqliteStore::open(
        options.state_dir.join("campaign.sqlite"),
        store_domain(options),
    )?;
    Ok((
        DurableRuntimeControl::open(store, admission, clock)?,
        binding,
    ))
}

fn reopen_campaign_runtime(
    options: &CampaignOptions,
    runtime: CampaignRuntime,
    binding: &ExecutionBinding,
) -> CampaignResult<CampaignRuntime> {
    // Reuse only captured provider bytes. Admission remains live, and opening
    // the control reads a new exact head rather than reusing a mutable snapshot.
    let (store, router) = runtime.into_parts();
    let admission = ExecutionBindingAdmission::admit(router, binding.clone())?;
    let clock = open_campaign_clock(options)?;
    DurableRuntimeControl::open(store, admission, clock).map_err(Into::into)
}

fn verify_campaign_virtual_authority(
    options: &CampaignOptions,
    control: &mut CampaignStore,
    revision: &str,
    current: &VirtualCurrent,
    region: &VirtualRegion,
    cases: &[EvaluationCase],
) -> CampaignResult<()> {
    let position: usize = region
        .cursor
        .position
        .parse()
        .map_err(|_| "campaign region cursor is not numeric")?;
    if position > cases.len() {
        return Err("campaign region cursor exceeds the pinned suite".into());
    }
    let counts = current.body.counts;
    let work_is_exact = |item: &WorkItem| -> bool {
        item.region_id == REGION_ID
            && item.run_id == options.run_id
            && item.capability.as_deref() == Some("evaluation")
            && item.priority == 0
            && item.cost == 1
            && cases.iter().any(|case| {
                item.work_id == format!("case:{}", case.id)
                    && case_reference(case).is_ok_and(|reference| reference == item.payload)
            })
    };
    if current.body.limits != frontier_limits()
        || current.body.scheduling_policy != SchedulingPolicy::default()
        || current.body.archive
            != VirtualArchiveBinding::new(RESOURCE_BINDING, RESOURCE_ARCHIVE_REVISION)?
        || counts.regions != 1
        || counts.runs != 1
        || counts.parked != 0
        || counts.migrations != 0
        || counts.certificates != 0
        || counts.hot_work != u64::try_from(position)?
        || counts.active_regions != u64::from(!region.cursor.exhausted)
        || current
            .body
            .frontier
            .ready
            .keys()
            .any(|run| run != &options.run_id)
        || current
            .body
            .frontier
            .ready
            .values()
            .flatten()
            .any(|item| !work_is_exact(item))
        || current
            .body
            .frontier
            .active
            .values()
            .any(|claim| !work_is_exact(&claim.item))
    {
        return Err("campaign virtual-work authority is not exact and exclusive".into());
    }
    let run = control
        .virtual_read()
        .read_run(SCHEDULER_ID, &options.run_id, revision)?
        .value
        .ok_or("campaign Run fairness authority is missing")?;
    if run.execution != campaign_run_execution() || run.weight != 1 {
        return Err("campaign Run changed its immutable execution selector or weight".into());
    }
    Ok(())
}

fn build_report(
    options: &CampaignOptions,
    control: &mut CampaignStore,
    revision: &str,
    current: &VirtualCurrent,
    evolution: &CampaignEvolution,
    metadata: &CampaignMetadata,
    retained_cases: &[EvaluationCase],
) -> CampaignResult<CampaignReport> {
    if retained_cases.len() != metadata.case_count {
        return Err("retained suite case count changed".into());
    }
    let mut cases = Vec::new();
    let mut known_work = 0_u64;
    let mut observed_occurrences = 0_u64;
    let mut recovered_attempts = 0_u64;
    let mut succeeded = 0;
    let mut failed = 0;
    let mut points = 0_u64;
    let mut max_points = 0_u64;
    // The suite is the application-owned enumeration. Each case reads only its
    // exact current work and latest occurrence; older attempts are not replayed.
    for retained_case in retained_cases {
        let work_id = format!("case:{}", retained_case.id);
        let Some(work) = control
            .virtual_read()
            .read_work(SCHEDULER_ID, &work_id, revision)?
            .value
        else {
            continue;
        };
        known_work += 1;
        if work.item.region_id != REGION_ID
            || work.item.run_id != options.run_id
            || work.item.payload != case_reference(retained_case)?
        {
            return Err("campaign retained work changed its pinned suite identity".into());
        }
        observed_occurrences = observed_occurrences
            .checked_add(work.max_epoch)
            .ok_or("campaign occurrence count overflow")?;
        let Some(occurrence_id) = &work.latest_occurrence_id else {
            if work.max_epoch != 0 || work.placement != VirtualWorkPlacement::Ready {
                return Err("unclaimed campaign work changed its placement".into());
            }
            continue;
        };
        recovered_attempts = recovered_attempts
            .checked_add(
                work.max_epoch
                    .checked_sub(1)
                    .ok_or("claimed work has no occurrence epoch")?,
            )
            .ok_or("campaign recovery count overflow")?;
        let occurrence = control
            .virtual_read()
            .read_occurrence(SCHEDULER_ID, occurrence_id, revision)?
            .value
            .ok_or("campaign latest occurrence is missing")?
            .occurrence;
        if occurrence.work_id != work_id
            || occurrence.run_id != options.run_id
            || occurrence.region_id != REGION_ID
            || occurrence.epoch != work.max_epoch
        {
            return Err("campaign work and latest occurrence disagree".into());
        }
        let expected_policy = evolution.policy_for_plan(&occurrence.plan_id)?;
        let (state, output, error) = match occurrence.state {
            WorkOccurrenceState::Succeeded => {
                if work.placement != VirtualWorkPlacement::Terminal {
                    return Err("successful campaign work is not terminal".into());
                }
                let reference = occurrence.result.as_ref().ok_or("success has no result")?;
                if reference.kind != RESULT_ARTIFACT_KIND {
                    return Err("campaign success references the wrong Artifact kind".into());
                }
                let record = control
                    .read_artifact(reference, revision)?
                    .value
                    .ok_or("result Artifact is missing")?;
                let output: CaseOutput = decode_json(&record.bytes)?;
                if canonical_bytes(&output)? != record.bytes {
                    return Err("campaign result Artifact is not canonical JSON".into());
                }
                output.validate_for(retained_case, expected_policy)?;
                succeeded += 1;
                points += u64::from(output.score.points);
                max_points += u64::from(output.score.max_points);
                ("succeeded".to_owned(), Some(output), None)
            }
            WorkOccurrenceState::Failed | WorkOccurrenceState::Cancelled => {
                if work.placement != VirtualWorkPlacement::Terminal {
                    return Err("failed campaign work is not terminal".into());
                }
                let reference = occurrence.error.as_ref().ok_or("failure has no evidence")?;
                if reference.kind != ERROR_ARTIFACT_KIND {
                    return Err("campaign failure references the wrong Artifact kind".into());
                }
                let record = control
                    .read_artifact(reference, revision)?
                    .value
                    .ok_or("error Artifact is missing")?;
                failed += 1;
                (
                    format!("{:?}", occurrence.state).to_ascii_lowercase(),
                    None,
                    Some(String::from_utf8(record.bytes)?),
                )
            }
            WorkOccurrenceState::Running => {
                if work.placement != VirtualWorkPlacement::Active
                    || current
                        .body
                        .frontier
                        .active
                        .get(&work_id)
                        .is_none_or(|claim| {
                            claim.occurrence_id != occurrence.occurrence_id
                                || claim.execution_binding != occurrence.execution_binding
                                || claim.plan_id != occurrence.plan_id
                        })
                {
                    return Err("running campaign occurrence has no exact active claim".into());
                }
                continue;
            }
            WorkOccurrenceState::RetryScheduled => {
                if work.placement != VirtualWorkPlacement::Ready || occurrence.next_reason.is_some()
                {
                    return Err("campaign recovery is not immediately ready".into());
                }
                continue;
            }
            WorkOccurrenceState::Parked => {
                return Err("campaign contains unsupported parked work".into());
            }
        };
        cases.push(CaseReport {
            case_id: retained_case.id.clone(),
            occurrence_id: occurrence.occurrence_id,
            plan_id: occurrence.plan_id,
            state,
            output,
            error,
        });
    }
    if known_work != current.body.counts.hot_work
        || observed_occurrences != current.body.counts.hot_occurrences
    {
        return Err("campaign exact work reads disagree with the bounded current counts".into());
    }
    cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    Ok(CampaignReport {
        run_id: options.run_id.clone(),
        suite_resource_id: metadata.suite.resource_id.clone(),
        current_plan_id: evolution.current.clone(),
        total_cases: metadata.case_count,
        total_occurrences: usize::try_from(observed_occurrences)?,
        recovered_attempts: usize::try_from(recovered_attempts)?,
        succeeded,
        failed,
        points,
        max_points,
        cases,
    })
}

fn verify_suite_resource(
    resource: &ResourceHandle,
    store: &FsResourceStore,
) -> CampaignResult<Vec<u8>> {
    let publication = store.publication(resource)?;
    let ResourceIntegrity::Content { size, .. } = &publication.resource.integrity else {
        return Err("campaign suite is not content-addressed".into());
    };
    if *size > MAX_SUITE_BYTES {
        return Err("retained suite exceeds the example byte limit".into());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(*size)?);
    ResourceClient::new(store.clone()).copy_to(&publication, 64 * 1024, &mut bytes)?;
    Ok(bytes)
}

fn verify_requested_suite(path: Option<&Path>, resource: &ResourceHandle) -> CampaignResult<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let bytes = read_suite_file(path)?;
    let ResourceIntegrity::Content { digest, size } = &resource.integrity else {
        return Err("campaign suite is not content-addressed".into());
    };
    let observed = format!("sha256:{}", sha256_bytes(&bytes));
    if observed != *digest || u64::try_from(bytes.len())? != *size {
        return Err("requested suite bytes differ from the campaign's pinned Resource".into());
    }
    Ok(())
}

#[cfg(unix)]
fn read_suite_file(path: &Path) -> CampaignResult<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| {
            if error.raw_os_error() == Some(nix::libc::ELOOP) {
                "suite path must be a regular non-symlink file".into()
            } else {
                Box::<dyn std::error::Error>::from(error)
            }
        })?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err("suite path must be a regular non-symlink file".into());
    }
    if metadata.len() > MAX_SUITE_BYTES {
        return Err(format!("suite exceeds {MAX_SUITE_BYTES} bytes").into());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len().min(MAX_SUITE_BYTES))?);
    (&mut file)
        .take(MAX_SUITE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > MAX_SUITE_BYTES {
        return Err(format!("suite exceeds {MAX_SUITE_BYTES} bytes").into());
    }
    let retained_metadata = file.metadata()?;
    if u64::try_from(bytes.len())? != metadata.len()
        || metadata.dev() != retained_metadata.dev()
        || metadata.ino() != retained_metadata.ino()
        || metadata.len() != retained_metadata.len()
        || metadata.mtime() != retained_metadata.mtime()
        || metadata.mtime_nsec() != retained_metadata.mtime_nsec()
        || metadata.ctime() != retained_metadata.ctime()
        || metadata.ctime_nsec() != retained_metadata.ctime_nsec()
    {
        return Err("suite changed while it was being read".into());
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_suite_file(_path: &Path) -> CampaignResult<Vec<u8>> {
    Err("suite import requires a platform no-follow file-open primitive".into())
}

fn frontier_limits() -> FrontierLimits {
    FrontierLimits {
        max_materialized: 32,
        max_active: 1,
        max_active_per_run: 1,
        materialize_batch: 16,
    }
}

fn stable_id<T: Serialize>(kind: &str, value: &T) -> CampaignResult<String> {
    Ok(format!(
        "example:{kind}:{}",
        content_id("example.campaign/1", value)?
    ))
}

#[derive(Debug, Clone, Copy)]
struct CampaignWallClock {
    fixed_unix_ms: Option<u64>,
}

impl WallClock for CampaignWallClock {
    fn now_unix_ms(&self) -> DurableResult<u64> {
        if let Some(now) = self.fixed_unix_ms {
            return Ok(now);
        }
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| cymule_durable::DurableError::Substrate {
                code: "campaign_clock_before_unix_epoch".to_owned(),
                message: error.to_string(),
            })?;
        u64::try_from(elapsed.as_millis()).map_err(|error| {
            cymule_durable::DurableError::Substrate {
                code: "campaign_clock_value_out_of_range".to_owned(),
                message: error.to_string(),
            }
        })
    }
}

fn open_campaign_clock(
    options: &CampaignOptions,
) -> CampaignResult<SqliteClock<CampaignWallClock>> {
    let source_generation = content_id(
        "example.campaign-clock-source/1",
        &(
            env!("CARGO_PKG_VERSION"),
            cymule_runtime::ENGINE_CLOCK_SYSTEM_PROVIDER,
        ),
    )?;
    Ok(SqliteClock::open_with_wall_clock(
        options.state_dir.join("campaign-clock.sqlite"),
        "clock:durable-evaluation-campaign",
        source_generation,
        CampaignWallClock {
            fixed_unix_ms: options.logical_now,
        },
    )?)
}

fn validate_options(options: &CampaignOptions) -> CampaignResult<()> {
    if options.run_id.is_empty()
        || options.run_id.chars().count() > 128
        || options.run_id.chars().any(char::is_control)
        || options.worker_id.is_empty()
        || options.worker_id.chars().count() > 160
        || options.worker_id.chars().any(char::is_control)
        || options.lease_ttl < 2
        || options.lease_ttl > cymule_core::MAX_EXACT_INTEGER
        || options
            .logical_now
            .is_some_and(|now| now > cymule_core::MAX_EXACT_INTEGER)
        || !options.plugin_executable.is_absolute()
        || cymule_core::validate_content_id(
            "campaign plugin runtime revision",
            &options.plugin_runtime_revision,
        )
        .is_err()
        || matches!(
            options.fault,
            FaultPoint::AfterClaim(0) | FaultPoint::AfterCommit(0)
        )
    {
        return Err(
            "campaign Run, worker, lease, plugin executable, or runtime revision is invalid".into(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use cymule_resource::ResourceCandidate;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock works")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "cymule-campaign-unit-{label}-{}-{nonce}",
                std::process::id(),
            ));
            fs::create_dir(&path).expect("test directory creates");
            Self(path)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn options(state: &Path, run_id: &str) -> CampaignOptions {
        CampaignOptions::new(
            state,
            Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/support-tickets.jsonl"),
            run_id,
            std::env::current_exe().expect("test executable path resolves"),
            BUNDLED_PLUGIN_RUNTIME_REVISION,
        )
    }

    #[cfg(unix)]
    fn test_runtime(options: &CampaignOptions) -> CampaignRuntime {
        let manifest = crate::plugin::EvaluationPlugin
            .describe()
            .expect("test manifest reads");
        let binding = campaign_binding(manifest, BUNDLED_PLUGIN_RUNTIME_REVISION)
            .expect("test binding admits exact capability properties");
        let providers: BTreeMap<String, Box<dyn PluginHost>> = BTreeMap::from([
            (
                "evaluation-subject".to_owned(),
                Box::new(crate::plugin::EvaluationPlugin) as Box<dyn PluginHost>,
            ),
            (
                "evaluation-scorer".to_owned(),
                Box::new(crate::plugin::EvaluationPlugin) as Box<dyn PluginHost>,
            ),
        ]);
        let router = AdmittedPluginRouter::new(binding.clone(), providers).expect("router admits");
        let admission = ExecutionBindingAdmission::admit(router, binding).expect("provider admits");
        DurableRuntimeControl::open(
            SqliteStore::open(
                options.state_dir.join("campaign.sqlite"),
                store_domain(options),
            )
            .expect("test Store opens"),
            admission,
            open_campaign_clock(options).expect("test Clock opens"),
        )
        .expect("public runtime opens")
    }

    #[cfg(unix)]
    fn store_metadata(options: &CampaignOptions) -> CampaignMetadata {
        let bytes = read_suite_file(options.suite_path.as_deref().unwrap()).expect("suite reads");
        let mut store =
            FsResourceStore::open(options.state_dir.join("resources"), RESOURCE_BINDING)
                .expect("Resource Store opens");
        let suite =
            store_suite_bytes(&mut store, options, &bytes).expect("suite Resource publishes");
        CampaignMetadata {
            metadata_version: CAMPAIGN_METADATA_ARTIFACT_KIND.to_owned(),
            run_id: options.run_id.clone(),
            suite,
            case_count: parse_suite(&bytes).expect("suite parses").len(),
        }
    }

    #[test]
    fn option_identity_bounds_count_unicode_scalars() {
        let state = TestDir::new("unicode-option-bounds");
        let mut options = options(state.path(), &"界".repeat(128));
        options.worker_id = "🧭".repeat(160);
        validate_options(&options).expect("multi-byte identities at scalar bounds are valid");
        options.run_id = "界".repeat(129);
        assert!(validate_options(&options).is_err());
        options.run_id = "run:valid".to_owned();
        options.worker_id = "🧭".repeat(161);
        assert!(validate_options(&options).is_err());
        options.worker_id = "worker:valid".to_owned();
        options.plugin_runtime_revision = "unix:macos:arm64".to_owned();
        assert!(validate_options(&options).is_err());
    }

    #[test]
    fn region_source_binding_separates_operation_configuration_and_implementation() {
        let first = ResourceCandidate::text("suite one")
            .seal()
            .expect("first suite seals");
        let second = ResourceCandidate::text("suite two")
            .seal()
            .expect("second suite seals");
        let first_binding = case_source_binding(&first).expect("first source binds");
        let second_binding = case_source_binding(&second).expect("second source binds");
        assert_eq!(first_binding.operation, SOURCE_OPERATION);
        assert_eq!(first_binding.revision, SOURCE_IMPLEMENTATION_REVISION);
        assert_ne!(first_binding.binding, second_binding.binding);
        assert_eq!(first_binding.operation, second_binding.operation);
        assert_eq!(first_binding.revision, second_binding.revision);
    }

    #[test]
    fn campaign_region_requires_exact_source_cursor_and_metadata() {
        let metadata = CampaignMetadata {
            metadata_version: CAMPAIGN_METADATA_ARTIFACT_KIND.to_owned(),
            run_id: "run:campaign-region-test".to_owned(),
            suite: ResourceCandidate::text("suite")
                .seal()
                .expect("suite seals"),
            case_count: 1,
        };
        let valid = VirtualRegion {
            region_id: REGION_ID.to_owned(),
            run_id: metadata.run_id.clone(),
            source: case_source_binding(&metadata.suite).expect("source binds"),
            source_artifact: metadata_artifact(&metadata)
                .expect("metadata seals")
                .reference,
            cursor: VirtualCursor {
                version: CURSOR_VERSION.to_owned(),
                position: "0".to_owned(),
                exhausted: false,
            },
            estimated_total: Some(1),
        };
        verify_campaign_region(&valid, &metadata.run_id, &metadata).expect("exact region verifies");
        let mut wrong_run = valid.clone();
        wrong_run.run_id = "run:other".to_owned();
        let mut wrong_source = valid.clone();
        wrong_source.source.binding = format!("sha256:{}", "f".repeat(64));
        let mut wrong_artifact = valid.clone();
        wrong_artifact.source_artifact.artifact_id = format!("sha256:{}", "f".repeat(64));
        let mut wrong_cursor = valid.clone();
        wrong_cursor.cursor.version = "example.evaluation-suite-cursor/0".to_owned();
        let mut noncanonical_cursor = valid.clone();
        noncanonical_cursor.cursor.position = "00".to_owned();
        let mut wrong_exhaustion = valid.clone();
        wrong_exhaustion.cursor.exhausted = true;
        let mut wrong_total = valid;
        wrong_total.estimated_total = Some(2);
        for invalid in [
            wrong_run,
            wrong_source,
            wrong_artifact,
            wrong_cursor,
            noncanonical_cursor,
            wrong_exhaustion,
            wrong_total,
        ] {
            assert!(verify_campaign_region(&invalid, &metadata.run_id, &metadata).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn campaign_requires_one_exact_public_region() {
        let state = TestDir::new("foreign-region");
        let options = options(state.path(), "run:foreign-region");
        let mut control = open_store_control(&options, true).expect("domain initializes");
        initialize_evolution(&mut control).expect("baseline commits");
        let metadata = store_metadata(&options);
        let command = suite_initialization_command(&options, &metadata).expect("command seals");
        let VirtualPersistenceOperation::Initialize(mut initialize) = command.operation else {
            unreachable!()
        };
        let mut foreign = initialize.regions[0].clone();
        foreign.region_id = "region:foreign".to_owned();
        initialize.regions.push(foreign);
        let command =
            VirtualPersistenceCommand::new(VirtualPersistenceOperation::Initialize(initialize))
                .expect("two-region command is valid framework input");
        test_runtime(&options)
            .virtual_work(&mut NoVirtualProviders)
            .commit(&command)
            .expect("framework registers two exact regions");
        let mut reopened = open_read_only_control(&options).expect("observer opens");
        assert!(
            read_campaign_current(&mut reopened).is_err(),
            "the application does not admit an additional framework-valid region"
        );
    }

    #[cfg(unix)]
    #[test]
    fn resource_commit_after_baseline_reopens_at_the_region_checkpoint() {
        let state = TestDir::new("resource-before-region");
        let options = options(state.path(), "run:resource-before-region");
        let baseline_plan = {
            let mut control = open_store_control(&options, true).expect("domain initializes");
            initialize_evolution(&mut control).expect("baseline commits");
            let read = control
                .evolution(&mut NoEvolutionProviders)
                .read_current(&EvolutionCurrentQuery {
                    evolution_id: EVOLUTION_ID.to_owned(),
                    expected_revision: None,
                })
                .expect("baseline current reads");
            read_campaign_evolution(&mut control, &read.observed_revision, 0)
                .expect("baseline verifies")
                .current
        };
        let metadata = store_metadata(&options);
        let mut reopened = open_store_control(&options, false).expect("domain reopens");
        initialize_evolution(&mut reopened).expect("baseline replays exactly");
        let read = reopened
            .virtual_read()
            .read_current(&VirtualCurrentQuery {
                scheduler_id: SCHEDULER_ID.to_owned(),
                expected_revision: None,
            })
            .expect("scheduler absence reads");
        assert!(read.current.is_none());
        assert_eq!(
            read_campaign_evolution(&mut reopened, &read.observed_revision, 0)
                .expect("baseline remains exact")
                .current,
            baseline_plan
        );
        test_runtime(&options)
            .virtual_work(&mut NoVirtualProviders)
            .commit(
                &suite_initialization_command(&options, &metadata).expect("suite command seals"),
            )
            .expect("metadata and source region share one public CAS");
        let report = status(&options).expect("read-only status verifies the retained suite");
        assert_eq!(report.suite_resource_id, metadata.suite.resource_id);
        assert_eq!(report.current_plan_id, baseline_plan);
        assert_eq!(report.total_cases, metadata.case_count);
        assert_eq!(report.total_occurrences, 0);
    }

    #[cfg(unix)]
    #[test]
    fn reopening_control_reuses_admitted_provider_and_refreshes_the_head() {
        let state = TestDir::new("retained-provider");
        let mut options = options(state.path(), "run:retained-provider");
        let mut control = open_store_control(&options, true).expect("domain initializes");
        let runtime = test_runtime(&options);
        let binding = campaign_binding(
            crate::plugin::EvaluationPlugin
                .describe()
                .expect("test manifest reads"),
            BUNDLED_PLUGIN_RUNTIME_REVISION,
        )
        .expect("exact original binding derives");
        initialize_evolution(&mut control).expect("another control advances the head");
        let observed = control
            .evolution(&mut NoEvolutionProviders)
            .read_current(&EvolutionCurrentQuery {
                evolution_id: EVOLUTION_ID.to_owned(),
                expected_revision: None,
            })
            .expect("latest revision reads")
            .observed_revision;
        options.plugin_executable = state.path().join("not-a-provider");
        let mut reopened = reopen_campaign_runtime(&options, runtime, &binding)
            .expect("refresh reuses the admitted provider without capturing a mutable path");
        let read = reopened
            .virtual_work(&mut NoVirtualProviders)
            .read_current(&VirtualCurrentQuery {
                scheduler_id: SCHEDULER_ID.to_owned(),
                expected_revision: Some(observed.clone()),
            })
            .expect("reopened control uses the current head, not the cached predecessor");
        assert_eq!(read.observed_revision, observed);
        assert!(read.current.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn suite_initialization_requires_exact_baseline_before_publication() {
        let state = TestDir::new("missing-baseline");
        let options = options(state.path(), "run:missing-baseline");
        let mut control = open_store_control(&options, true).expect("domain initializes");
        let mut resources =
            FsResourceStore::open(options.state_dir.join("resources"), RESOURCE_BINDING)
                .expect("resource store opens");
        assert!(load_or_initialize_suite(&options, &mut control, &mut resources).is_err());
        let read = control
            .virtual_read()
            .read_current(&VirtualCurrentQuery {
                scheduler_id: SCHEDULER_ID.to_owned(),
                expected_revision: None,
            })
            .expect("scheduler absence reads");
        assert!(
            read.current.is_none(),
            "invalid baseline cannot publish a region"
        );
        assert!(evolve(&options, "weighted").is_err());
        assert!(
            control
                .evolution(&mut NoEvolutionProviders)
                .read_current(&EvolutionCurrentQuery {
                    evolution_id: EVOLUTION_ID.to_owned(),
                    expected_revision: None,
                })
                .expect("evolution absence reads")
                .current
                .is_none()
        );
    }

    #[test]
    fn additional_evolution_command_is_not_campaign_authority() {
        let state = TestDir::new("foreign-evolution");
        let options = options(state.path(), "run:foreign-evolution");
        let mut control = open_store_control(&options, true).expect("domain initializes");
        initialize_evolution(&mut control).expect("baseline commits");
        let command = EvolutionPersistenceCommand::new(
            EVOLUTION_ID,
            LiveEvolutionCommand::PublishDefinition {
                control_version: LIVE_EVOLUTION_CONTROL_VERSION.to_owned(),
                command_id: "foreign:definition".to_owned(),
                logical_ref: "foreign.scorer".to_owned(),
                definition: scorer_definition("strict", true),
                references: Vec::new(),
            },
        )
        .expect("foreign command is valid framework input");
        control
            .evolution(&mut NoEvolutionProviders)
            .commit(&command)
            .expect("framework publishes foreign definition");
        let read = control
            .evolution(&mut NoEvolutionProviders)
            .read_current(&EvolutionCurrentQuery {
                evolution_id: EVOLUTION_ID.to_owned(),
                expected_revision: None,
            })
            .expect("current reads");
        assert!(
            read_campaign_evolution(&mut control, &read.observed_revision, 0).is_err(),
            "an unrecognized M4 command is not hidden by a valid baseline receipt"
        );
    }
}
