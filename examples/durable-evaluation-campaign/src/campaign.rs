//! Durable local campaign orchestration over provider-neutral Cymule contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_core::{Machine, canonical_bytes, content_id, sha256_bytes};
use cymule_durable::{DurableCoordinator, JournalBatch, JournalRecord};
use cymule_evolution::{
    DurableLiveEvolutionController, LiveEvolutionController, LivePublicationCommand,
    LiveVirtualClaimCommand, RolloutMode,
};
use cymule_executor_process::{ProcessExecutor, ProcessExecutorConfig};
use cymule_resource::{
    ArtifactStore, MAX_WRITE_CHUNK, ResourceClient, ResourceHandle, ResourceIntegrity,
    ResourceShape, ResourceWriteIntent,
};
use cymule_resource_fs::FsResourceStore;
use cymule_runtime::{
    EmbeddedRuntime, ExecutionBinding, ExecutionOperationKind, PluginHost,
    RUNTIME_COMPOSITION_VERSION, RuntimeCompositionGraph, RuntimeImplementation,
    RuntimeProviderDescriptor,
};
use cymule_store_sqlite::SqliteStore;
use cymule_virtual::{
    ClaimedWork, DurableVirtualController, FrontierLimits, VIRTUAL_RECOVERY_CONTROL_VERSION,
    VIRTUAL_WORK_CONTROL_VERSION, VirtualCursor, VirtualRecoveryCommand, VirtualRegion,
    WorkOccurrenceState, WorkResolution, WorkResolutionCommand,
};
use serde::{Deserialize, Serialize};

use crate::evolution::{
    SCORER_REF, TEMPLATE_ID, campaign_template, current_plan, scorer_definition,
};
use crate::model::{
    CASE_ARTIFACT_KIND, CaseOutput, ERROR_ARTIFACT_KIND, EvaluationCase, MAX_SUITE_BYTES,
    RESULT_ARTIFACT_KIND, SUITE_ARTIFACT_KIND, SUITE_MEDIA_TYPE,
};
use crate::plugin::{SCORER_COMPONENT, SUBJECT_COMPONENT};
use crate::source::{CURSOR_VERSION, CaseSource, case_reference, parse_suite};

const VIRTUAL_JOURNAL: &str = "example:virtual-work";
const LIVE_EVOLUTION_JOURNAL: &str = "example:live-evolution";
const CAMPAIGN_METADATA_JOURNAL: &str = "example:campaign-metadata";
const REGION_ID: &str = "region:evaluation-suite";
const RESOURCE_BINDING: &str = "example.fs-resources@1";
const DEFAULT_LEASE_TTL: u64 = 60_000;

type CampaignResult<T> = Result<T, Box<dyn std::error::Error>>;

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
    /// Stable identity for this worker process.
    pub worker_id: String,
    /// Optional caller-supplied logical clock value.
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
    ) -> Self {
        Self {
            state_dir: state_dir.into(),
            suite_path: Some(suite_path.into()),
            run_id: run_id.into(),
            plugin_executable: plugin_executable.into(),
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

/// Run or resume one campaign until completion or a requested fault boundary.
pub fn run(options: &CampaignOptions) -> CampaignResult<CampaignRun> {
    validate_options(options)?;
    fs::create_dir_all(&options.state_dir)?;
    let (mut coordinator, mut machine) = open_coordinator(options)?;
    let mut resource_store =
        FsResourceStore::open(options.state_dir.join("resources"), RESOURCE_BINDING)?;
    let (metadata, cases) =
        load_or_initialize_suite(options, &mut coordinator, &mut machine, &mut resource_store)?;
    let mut evolution = initialize_evolution(&mut coordinator)?;
    let mut scheduler =
        DurableVirtualController::load(&coordinator, VIRTUAL_JOURNAL, frontier_limits())?;
    if !scheduler.snapshot().regions.contains_key(REGION_ID) {
        scheduler.register(VirtualRegion {
            region_id: REGION_ID.to_owned(),
            run_id: options.run_id.clone(),
            source: metadata.suite.resource_id.clone(),
            cursor: VirtualCursor {
                version: CURSOR_VERSION.to_owned(),
                position: "0".to_owned(),
                exhausted: false,
            },
            estimated_total: Some(u64::try_from(cases.len())?),
        })?;
        DurableVirtualController::checkpoint(
            &mut coordinator,
            &mut scheduler,
            VIRTUAL_JOURNAL,
            "virtual:register-suite",
        )?;
    }
    let region = scheduler
        .snapshot()
        .regions
        .get(REGION_ID)
        .cloned()
        .ok_or("evaluation region disappeared")?;
    if region.run_id != options.run_id || region.source != metadata.suite.resource_id {
        return Err("durable region does not match the requested Run and pinned suite".into());
    }

    let mut claimed_this_process = 0_usize;
    let mut committed_this_process = 0_usize;
    loop {
        let now = logical_now(options)?;
        recover_expired_claims(&mut coordinator, &mut scheduler, &mut machine, options, now)?;

        let claim = claim_next(
            &mut coordinator,
            &mut scheduler,
            &mut evolution,
            options,
            now,
        )?;
        let Some(claim) = claim else {
            let snapshot = scheduler.snapshot();
            let exhausted = snapshot
                .regions
                .get(REGION_ID)
                .is_some_and(|candidate| candidate.cursor.exhausted);
            if exhausted && scheduler.materialized_count() == 0 {
                break;
            }
            let before = snapshot
                .regions
                .get(REGION_ID)
                .map(|candidate| candidate.cursor.position.clone())
                .ok_or("evaluation region disappeared")?;
            let checkpoint_id = stable_id("fill", &(metadata.suite.resource_id.as_str(), &before))?;
            let added = {
                let mut source = CaseSource::new(&cases);
                DurableVirtualController::fill_and_checkpoint(
                    &mut coordinator,
                    &mut scheduler,
                    &mut source,
                    VIRTUAL_JOURNAL,
                    &checkpoint_id,
                )?
            };
            if added == 0 {
                return Err("campaign made no progress while work remained".into());
            }
            continue;
        };

        claimed_this_process += 1;
        if options.fault == FaultPoint::AfterClaim(claimed_this_process) {
            return Ok(CampaignRun {
                disposition: RunDisposition::SimulatedCrash,
                report: build_report(options, &coordinator, &scheduler, &evolution, &metadata)?,
            });
        }

        execute_claim(
            &mut coordinator,
            &mut scheduler,
            &mut machine,
            &evolution,
            options,
            &claim,
            &cases,
        )?;
        committed_this_process += 1;
        if options.fault == FaultPoint::AfterCommit(committed_this_process) {
            return Ok(CampaignRun {
                disposition: RunDisposition::SimulatedCrash,
                report: build_report(options, &coordinator, &scheduler, &evolution, &metadata)?,
            });
        }
        evolution = DurableLiveEvolutionController::load(&coordinator, LIVE_EVOLUTION_JOURNAL)?;
    }
    Ok(CampaignRun {
        disposition: RunDisposition::Complete,
        report: build_report(options, &coordinator, &scheduler, &evolution, &metadata)?,
    })
}

/// Verify retained resources and project current campaign status without work.
pub fn status(options: &CampaignOptions) -> CampaignResult<CampaignReport> {
    validate_options(options)?;
    let (coordinator, _machine) = open_read_only_coordinator(options)?;
    let mut resource_store =
        FsResourceStore::open(options.state_dir.join("resources"), RESOURCE_BINDING)?;
    let metadata = retained_metadata(&coordinator)?;
    verify_metadata_run(&metadata, &options.run_id)?;
    verify_suite_resource(&metadata.suite, &mut resource_store)?;
    verify_requested_suite(options.suite_path.as_deref(), &metadata.suite)?;
    let evolution = DurableLiveEvolutionController::load(&coordinator, LIVE_EVOLUTION_JOURNAL)?;
    let scheduler =
        DurableVirtualController::load(&coordinator, VIRTUAL_JOURNAL, frontier_limits())?;
    build_report(options, &coordinator, &scheduler, &evolution, &metadata)
}

/// Publish a compatible or deliberately incompatible scorer revision.
pub fn evolve(options: &CampaignOptions, policy: &str) -> CampaignResult<EvolutionReport> {
    validate_options(options)?;
    let (mut coordinator, _machine) = open_existing_coordinator(options)?;
    let mut evolution = DurableLiveEvolutionController::load(&coordinator, LIVE_EVOLUTION_JOURNAL)?;
    let previous = current_plan(&evolution)?;
    let compatible = match policy {
        "weighted" => true,
        "incompatible" => false,
        _ => return Err("policy must be weighted or incompatible".into()),
    };
    let definition = scorer_definition(policy, compatible);
    let checkpoint_id = stable_id("evolve", &(policy, &definition))?;
    let mut machine = coordinator.restore_machine()?;
    let evidence = machine.put_artifact(
        "example.evolution-review/1",
        canonical_bytes(&(policy, &definition))?,
    )?;
    let receipt = DurableLiveEvolutionController::publish_and_relink_and_checkpoint(
        &mut coordinator,
        &mut evolution,
        &machine,
        LIVE_EVOLUTION_JOURNAL,
        &checkpoint_id,
        LivePublicationCommand {
            logical_ref: SCORER_REF.to_owned(),
            definition,
            evidence,
            mode: RolloutMode::Active,
        },
    )?;
    let current = current_plan(&evolution)?;
    let advanced = receipt.updates.iter().any(|update| update.advanced);
    Ok(EvolutionReport {
        policy: policy.to_owned(),
        compatible,
        previous_plan_id: previous.plan.plan_id.clone(),
        current_plan_id: current.plan.plan_id.clone(),
        advanced,
        revision_id: receipt.revision.revision_id,
    })
}

fn open_coordinator(
    options: &CampaignOptions,
) -> CampaignResult<(DurableCoordinator<SqliteStore>, Machine)> {
    let store = SqliteStore::open(
        options.state_dir.join("campaign.sqlite"),
        format!("campaign:{}", options.run_id),
    )?;
    let mut coordinator = DurableCoordinator::open(store)?;
    if coordinator.revision().is_none() {
        coordinator.initialize_in_place(&Machine::new())?;
    }
    let machine = coordinator.restore_machine()?;
    Ok((coordinator, machine))
}

fn open_existing_coordinator(
    options: &CampaignOptions,
) -> CampaignResult<(DurableCoordinator<SqliteStore>, Machine)> {
    let store = SqliteStore::open(
        options.state_dir.join("campaign.sqlite"),
        format!("campaign:{}", options.run_id),
    )?;
    let coordinator = DurableCoordinator::open(store)?;
    if coordinator.revision().is_none() {
        return Err("campaign has not been initialized".into());
    }
    let machine = coordinator.restore_machine()?;
    Ok((coordinator, machine))
}

fn open_read_only_coordinator(
    options: &CampaignOptions,
) -> CampaignResult<(DurableCoordinator<SqliteStore>, Machine)> {
    let store = SqliteStore::open_read_only(
        options.state_dir.join("campaign.sqlite"),
        format!("campaign:{}", options.run_id),
    )?;
    let coordinator = DurableCoordinator::open(store)?;
    if coordinator.revision().is_none() {
        return Err("campaign has not been initialized".into());
    }
    let machine = coordinator.restore_machine()?;
    Ok((coordinator, machine))
}

fn load_or_initialize_suite(
    options: &CampaignOptions,
    coordinator: &mut DurableCoordinator<SqliteStore>,
    machine: &mut Machine,
    resource_store: &mut FsResourceStore,
) -> CampaignResult<(CampaignMetadata, Vec<EvaluationCase>)> {
    if let Ok(metadata) = retained_metadata(coordinator) {
        verify_metadata_run(&metadata, &options.run_id)?;
        verify_requested_suite(options.suite_path.as_deref(), &metadata.suite)?;
        let bytes = verify_suite_resource(&metadata.suite, resource_store)?;
        let cases = parse_suite(&bytes)?;
        if cases.len() != metadata.case_count {
            return Err("retained suite case count changed".into());
        }
        return Ok((metadata, cases));
    }
    let path = options
        .suite_path
        .as_deref()
        .ok_or("a suite path is required to initialize the campaign")?;
    let bytes = read_suite_file(path)?;
    let cases = parse_suite(&bytes)?;
    let suite = store_suite_bytes(resource_store, options, &bytes)?;
    let metadata = CampaignMetadata {
        metadata_version: "example.evaluation-campaign-metadata/1".to_owned(),
        run_id: options.run_id.clone(),
        suite,
        case_count: cases.len(),
    };
    let metadata_artifact =
        machine.put_artifact(SUITE_ARTIFACT_KIND, canonical_bytes(&metadata)?)?;
    let record = JournalRecord::new(
        "campaign:metadata",
        "example.evaluation-campaign-metadata/1",
        serde_json::to_value(&metadata)?,
    )?;
    coordinator.checkpoint_artifact_journals(
        machine,
        &BTreeSet::from([metadata_artifact]),
        &[JournalBatch {
            journal_id: CAMPAIGN_METADATA_JOURNAL.to_owned(),
            records: vec![record],
        }],
    )?;
    Ok((metadata, cases))
}

fn retained_metadata(
    coordinator: &DurableCoordinator<SqliteStore>,
) -> CampaignResult<CampaignMetadata> {
    let matches: Vec<_> = coordinator
        .state()?
        .machine
        .artifacts
        .iter()
        .filter(|record| record.reference.kind == SUITE_ARTIFACT_KIND)
        .collect();
    let [record] = matches.as_slice() else {
        return Err("campaign must retain exactly one suite metadata artifact".into());
    };
    let metadata: CampaignMetadata = serde_json::from_slice(&record.bytes)?;
    if metadata.metadata_version != "example.evaluation-campaign-metadata/1" {
        return Err("unsupported campaign metadata version".into());
    }
    metadata.suite.verify()?;
    Ok(metadata)
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

fn initialize_evolution(
    coordinator: &mut DurableCoordinator<SqliteStore>,
) -> CampaignResult<LiveEvolutionController> {
    let mut controller = DurableLiveEvolutionController::load(coordinator, LIVE_EVOLUTION_JOURNAL)?;
    if controller.snapshot().registry.revisions.is_empty() {
        DurableLiveEvolutionController::publish_and_checkpoint(
            coordinator,
            &mut controller,
            LIVE_EVOLUTION_JOURNAL,
            "definitions:strict-scorer",
            SCORER_REF,
            scorer_definition("strict", true),
        )?;
    }
    if controller.current_link(TEMPLATE_ID).is_none() {
        DurableLiveEvolutionController::register_template_and_checkpoint(
            coordinator,
            &mut controller,
            LIVE_EVOLUTION_JOURNAL,
            "definitions:campaign-template",
            campaign_template(),
        )?;
    }
    Ok(controller)
}

fn claim_next(
    coordinator: &mut DurableCoordinator<SqliteStore>,
    scheduler: &mut cymule_virtual::VirtualScheduler,
    evolution: &mut LiveEvolutionController,
    options: &CampaignOptions,
    now: u64,
) -> CampaignResult<Option<ClaimedWork>> {
    if let Some(active) = scheduler.snapshot().active.values().next().cloned() {
        if active.owner == options.worker_id && active.lease.expires_at > now {
            return Ok(Some(active));
        }
        return Ok(None);
    }
    let snapshot = scheduler.snapshot();
    if !snapshot.ready.values().any(|queue| !queue.is_empty()) {
        return Ok(None);
    }
    let command_id = stable_id(
        "claim",
        &(
            options.run_id.as_str(),
            cymule_core::canonical_digest(&snapshot)?,
            options.worker_id.as_str(),
            now,
        ),
    )?;
    let selection_id = stable_id("plan-selection", &command_id)?;
    let receipt = DurableLiveEvolutionController::claim_virtual_work_and_checkpoint(
        coordinator,
        evolution,
        scheduler,
        LIVE_EVOLUTION_JOURNAL,
        VIRTUAL_JOURNAL,
        &LiveVirtualClaimCommand {
            template_id: TEMPLATE_ID.to_owned(),
            selection_id,
            command_id,
            owner: options.worker_id.clone(),
            slot_id: stable_id(
                "slot",
                &(options.run_id.as_str(), options.worker_id.as_str()),
            )?,
            capabilities: BTreeSet::from(["evaluation".to_owned()]),
            logical_now: now,
            lease_ttl: options.lease_ttl,
        },
    )?;
    Ok(receipt.claim.claim)
}

fn recover_expired_claims(
    coordinator: &mut DurableCoordinator<SqliteStore>,
    scheduler: &mut cymule_virtual::VirtualScheduler,
    machine: &mut Machine,
    options: &CampaignOptions,
    now: u64,
) -> CampaignResult<()> {
    let Some(active) = scheduler.snapshot().active.values().next().cloned() else {
        return Ok(());
    };
    if active.owner == options.worker_id && active.lease.expires_at > now {
        return Ok(());
    }
    if active.lease.expires_at > now {
        return Err(format!(
            "case {} is owned by {} until logical time {}; retry after lease expiry",
            active.item.work_id, active.owner, active.lease.expires_at
        )
        .into());
    }
    let error = machine.put_artifact(
        ERROR_ARTIFACT_KIND,
        format!(
            "worker {} did not publish a result before lease {} expired",
            active.owner, active.lease.epoch
        )
        .into_bytes(),
    )?;
    let command = VirtualRecoveryCommand {
        control_version: VIRTUAL_RECOVERY_CONTROL_VERSION.to_owned(),
        command_id: stable_id("recover", &active.occurrence_id)?,
        work_id: active.item.work_id,
        expected_owner: active.owner,
        expected_epoch: active.epoch,
        expected_lease_epoch: active.lease.epoch,
        observed_at: now,
        resolution: WorkResolution::Retry {
            error,
            next_reason: None,
        },
    };
    DurableVirtualController::recover_expired_command_and_checkpoint(
        coordinator,
        scheduler,
        machine,
        &command,
        VIRTUAL_JOURNAL,
    )?;
    Ok(())
}

fn execute_claim(
    coordinator: &mut DurableCoordinator<SqliteStore>,
    scheduler: &mut cymule_virtual::VirtualScheduler,
    machine: &mut Machine,
    evolution: &LiveEvolutionController,
    options: &CampaignOptions,
    claim: &ClaimedWork,
    cases: &[EvaluationCase],
) -> CampaignResult<()> {
    let linked = evolution
        .historical_link_for(TEMPLATE_ID, &claim.occurrence_binding)
        .ok_or_else(|| {
            format!(
                "occurrence references unknown Plan {}",
                claim.occurrence_binding
            )
        })?;
    if claim.item.payload.kind != CASE_ARTIFACT_KIND {
        return Err("case payload kind changed".into());
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
    let mut config = ProcessExecutorConfig::new(&options.plugin_executable);
    config.arguments = vec!["__plugin".to_owned()];
    config.timeout = Duration::from_secs(5);
    config.message_limit = 1024 * 1024;
    let mut executor = ProcessExecutor::new(config)?;
    let manifest = executor.describe()?;
    let implementation_revision = format!(
        "sha256:{}",
        sha256_bytes(&fs::read(&options.plugin_executable)?)
    );
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
                revision: implementation_revision.clone(),
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
    let binding = ExecutionBinding::admit(&graph, &manifest)?;
    let mut runtime = EmbeddedRuntime::new(executor, binding)?;
    let execution = runtime
        .execute(
            linked.plan.clone(),
            &serde_json::to_value(case)?,
            format!("{}/{}", options.run_id, claim.occurrence_id),
        )
        .and_then(cymule_runtime::ExecutionOutcome::into_completed);
    let now = logical_now(options)?;
    let resolution = match execution {
        Ok(result) => {
            let output: CaseOutput = serde_json::from_value(result.value)?;
            WorkResolution::Succeeded {
                result: machine.put_artifact(RESULT_ARTIFACT_KIND, canonical_bytes(&output)?)?,
            }
        }
        Err(error) => WorkResolution::Failed {
            error: machine.put_artifact(ERROR_ARTIFACT_KIND, error.to_string().into_bytes())?,
        },
    };
    let command = WorkResolutionCommand {
        control_version: VIRTUAL_WORK_CONTROL_VERSION.to_owned(),
        command_id: stable_id("resolve", &claim.occurrence_id)?,
        work_id: claim.item.work_id.clone(),
        owner: claim.owner.clone(),
        epoch: claim.epoch,
        expected_lease_epoch: claim.lease.epoch,
        observed_at: now,
        resolution,
    };
    DurableVirtualController::resolve_command_and_checkpoint(
        coordinator,
        scheduler,
        machine,
        &command,
        VIRTUAL_JOURNAL,
    )?;
    Ok(())
}

fn build_report(
    options: &CampaignOptions,
    coordinator: &DurableCoordinator<SqliteStore>,
    scheduler: &cymule_virtual::VirtualScheduler,
    evolution: &LiveEvolutionController,
    metadata: &CampaignMetadata,
) -> CampaignResult<CampaignReport> {
    let machine = coordinator.restore_machine()?;
    let snapshot = scheduler.snapshot();
    let mut latest = BTreeMap::new();
    for occurrence in snapshot.occurrences.values() {
        latest
            .entry(occurrence.work_id.clone())
            .and_modify(|current: &mut &cymule_virtual::WorkOccurrence| {
                if occurrence.epoch > current.epoch {
                    *current = occurrence;
                }
            })
            .or_insert(occurrence);
    }
    let mut cases = Vec::new();
    let total_occurrences = snapshot.occurrences.len();
    let recovered_attempts = snapshot
        .occurrences
        .values()
        .filter(|occurrence| occurrence.state == WorkOccurrenceState::RetryScheduled)
        .count();
    let mut succeeded = 0;
    let mut failed = 0;
    let mut points = 0_u64;
    let mut max_points = 0_u64;
    for (work_id, occurrence) in latest {
        let case_id = work_id.strip_prefix("case:").unwrap_or(&work_id).to_owned();
        let (state, output, error) = match occurrence.state {
            WorkOccurrenceState::Succeeded => {
                let reference = occurrence.result.as_ref().ok_or("success has no result")?;
                let record = machine
                    .artifact(reference)
                    .ok_or("result Artifact is missing")?;
                let output: CaseOutput = serde_json::from_slice(&record.bytes)?;
                succeeded += 1;
                points += u64::from(output.score.points);
                max_points += u64::from(output.score.max_points);
                ("succeeded".to_owned(), Some(output), None)
            }
            WorkOccurrenceState::Failed | WorkOccurrenceState::Cancelled => {
                let reference = occurrence.error.as_ref().ok_or("failure has no evidence")?;
                let record = machine
                    .artifact(reference)
                    .ok_or("error Artifact is missing")?;
                failed += 1;
                (
                    format!("{:?}", occurrence.state).to_ascii_lowercase(),
                    None,
                    Some(String::from_utf8_lossy(&record.bytes).into_owned()),
                )
            }
            WorkOccurrenceState::RetryScheduled
            | WorkOccurrenceState::Parked
            | WorkOccurrenceState::Running => continue,
        };
        cases.push(CaseReport {
            case_id,
            occurrence_id: occurrence.occurrence_id.clone(),
            plan_id: occurrence.occurrence_binding.clone(),
            state,
            output,
            error,
        });
    }
    cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    Ok(CampaignReport {
        run_id: options.run_id.clone(),
        suite_resource_id: metadata.suite.resource_id.clone(),
        current_plan_id: current_plan(evolution)?.plan.plan_id,
        total_cases: metadata.case_count,
        total_occurrences,
        recovered_attempts,
        succeeded,
        failed,
        points,
        max_points,
        cases,
    })
}

fn verify_suite_resource(
    resource: &ResourceHandle,
    store: &mut FsResourceStore,
) -> CampaignResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let publication = store.publication(resource)?;
    ResourceClient::new(store.clone()).copy_to(&publication, 64 * 1024, &mut bytes)?;
    if u64::try_from(bytes.len())? > MAX_SUITE_BYTES {
        return Err("retained suite exceeds the example byte limit".into());
    }
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

fn read_suite_file(path: &Path) -> CampaignResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err("suite path must be a regular non-symlink file".into());
    }
    if metadata.len() > MAX_SUITE_BYTES {
        return Err(format!("suite exceeds {MAX_SUITE_BYTES} bytes").into());
    }
    let bytes = fs::read(path)?;
    if u64::try_from(bytes.len())? != metadata.len() {
        return Err("suite changed while it was being read".into());
    }
    Ok(bytes)
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

fn logical_now(options: &CampaignOptions) -> CampaignResult<u64> {
    if let Some(now) = options.logical_now {
        return Ok(now);
    }
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn validate_options(options: &CampaignOptions) -> CampaignResult<()> {
    if options.run_id.is_empty()
        || options.run_id.len() > 128
        || options.run_id.chars().any(char::is_control)
        || options.worker_id.is_empty()
        || options.worker_id.len() > 160
        || options.worker_id.chars().any(char::is_control)
        || options.lease_ttl == 0
        || !options.plugin_executable.is_absolute()
        || matches!(
            options.fault,
            FaultPoint::AfterClaim(0) | FaultPoint::AfterCommit(0)
        )
    {
        return Err("campaign Run, worker, lease, or plugin executable is invalid".into());
    }
    Ok(())
}
