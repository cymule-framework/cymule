// Package cymule provides Go authoring and Engine client APIs.
package cymule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sort"
)

// EngineProtocolVersion is the frozen Engine transport contract.
const EngineProtocolVersion = "cymule.engine/1"

// EngineIssue is one machine-readable validation or contract issue.
type EngineIssue struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Path       string `json:"path,omitempty"`
	SchemaPath string `json:"schema_path,omitempty"`
}

// EngineFailure is one closed semantic or transport failure.
type EngineFailure struct {
	Category         string        `json:"category"`
	Phase            string        `json:"phase"`
	Code             string        `json:"code"`
	Message          string        `json:"message"`
	Contract         string        `json:"contract,omitempty"`
	ContractSide     string        `json:"contract_side,omitempty"`
	Path             string        `json:"path,omitempty"`
	Issues           []EngineIssue `json:"issues,omitempty"`
	RetryDisposition string        `json:"retry_disposition,omitempty"`
}

// Error implements error without requiring callers to parse text.
func (failure EngineFailure) Error() string {
	return failure.Code + ": " + failure.Message
}

func (failure EngineFailure) validate() error {
	if !closedEngineCategory(failure.Category) || !closedEnginePhase(failure.Phase) ||
		!validEngineCode(failure.Code) || len(failure.Message) < 1 || len(failure.Message) > 8192 {
		return fmt.Errorf("Engine failure fields are invalid")
	}
	if failure.Contract != "" && len(failure.Contract) > 500 {
		return fmt.Errorf("Engine failure contract is invalid")
	}
	if failure.ContractSide != "" && failure.ContractSide != "schema" &&
		failure.ContractSide != "input" && failure.ContractSide != "output" {
		return fmt.Errorf("Engine failure contract side is unknown")
	}
	if !validEnginePath(failure.Path) || len(failure.Issues) > 100 {
		return fmt.Errorf("Engine failure path or issue set is invalid")
	}
	for _, issue := range failure.Issues {
		if len(issue.Code) < 1 || len(issue.Code) > 200 || len(issue.Message) < 1 ||
			len(issue.Message) > 2000 || !validEnginePath(issue.Path) ||
			!validEnginePath(issue.SchemaPath) {
			return fmt.Errorf("Engine issue is invalid")
		}
	}
	if failure.RetryDisposition != "" && failure.RetryDisposition != "never" &&
		failure.RetryDisposition != "correct_and_retry" &&
		failure.RetryDisposition != "refresh_and_retry" &&
		failure.RetryDisposition != "retry_same_request" && failure.RetryDisposition != "reconcile" {
		return fmt.Errorf("Engine retry disposition is unknown")
	}
	return nil
}

func closedEngineCategory(value string) bool {
	switch value {
	case "transport_failure", "validation", "contract_violation", "admission_denied", "conflict",
		"not_found", "expected_plugin_failure", "plugin_defect", "substrate_failure", "cancelled",
		"timed_out", "unknown_world_outcome":
		return true
	default:
		return false
	}
}

func closedEnginePhase(value string) bool {
	switch value {
	case "transport", "decode_request", "validate_request", "seal_plan", "verify_plan",
		"seal_resource", "verify_wait_activation", "verify_durable_command",
		"verify_evolution_command", "verify_live_evolution_command", "execute_plan",
		"plugin_describe", "plugin_call", "effect_prepare", "effect_dispatch", "effect_reconcile",
		"encode_response":
		return true
	default:
		return false
	}
}

func validEngineCode(value string) bool {
	if len(value) < 1 || len(value) > 200 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range []byte(value[1:]) {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validEnginePath(value string) bool {
	return len(value) <= 1000 && (value == "" || value[0] == '/')
}

// Expression is one frozen IR expression.
type Expression map[string]any

// InlineData retains one small resource value.
type InlineData struct {
	Encoding string
	Text     string
	Value    any
	Data     string
}

// MarshalJSON emits the closed encoding-specific wire shape.
func (data InlineData) MarshalJSON() ([]byte, error) {
	switch data.Encoding {
	case "utf8":
		return json.Marshal(struct {
			Encoding string `json:"encoding"`
			Text     string `json:"text"`
		}{data.Encoding, data.Text})
	case "json":
		return json.Marshal(struct {
			Encoding string `json:"encoding"`
			Value    any    `json:"value"`
		}{data.Encoding, data.Value})
	case "base64":
		return json.Marshal(struct {
			Encoding string `json:"encoding"`
			Data     string `json:"data"`
		}{data.Encoding, data.Data})
	default:
		return nil, fmt.Errorf("unsupported inline encoding %q", data.Encoding)
	}
}

// UnmarshalJSON reads one closed encoding-specific wire shape.
func (data *InlineData) UnmarshalJSON(input []byte) error {
	var tagged struct {
		Encoding string `json:"encoding"`
	}
	if err := json.Unmarshal(input, &tagged); err != nil {
		return err
	}
	switch tagged.Encoding {
	case "utf8":
		var value struct {
			Encoding string `json:"encoding"`
			Text     string `json:"text"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*data = InlineData{Encoding: value.Encoding, Text: value.Text}
	case "json":
		var value struct {
			Encoding string `json:"encoding"`
			Value    any    `json:"value"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*data = InlineData{Encoding: value.Encoding, Value: value.Value}
	case "base64":
		var value struct {
			Encoding string `json:"encoding"`
			Data     string `json:"data"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*data = InlineData{Encoding: value.Encoding, Data: value.Data}
	default:
		return fmt.Errorf("unsupported inline encoding %q", tagged.Encoding)
	}
	return nil
}

// ResourceIntegrity describes exact, version-pinned, or live replay evidence.
type ResourceIntegrity struct {
	Kind      string `json:"kind"`
	Digest    string `json:"digest,omitempty"`
	Size      uint64 `json:"size,omitempty"`
	Authority string `json:"authority,omitempty"`
	Version   string `json:"version,omitempty"`
	Identity  string `json:"identity,omitempty"`
}

// ResourceLocation is a non-authoritative realization hint.
type ResourceLocation struct {
	Kind      string `json:"kind"`
	URL       string `json:"url,omitempty"`
	Binding   string `json:"binding,omitempty"`
	Reference string `json:"reference,omitempty"`
}

// ResourceCandidate is sealed by the trusted Rust resource engine.
type ResourceCandidate struct {
	ResourceVersion string             `json:"resource_version"`
	Shape           string             `json:"shape"`
	MediaType       string             `json:"media_type"`
	Inline          *InlineData        `json:"inline,omitempty"`
	Integrity       ResourceIntegrity  `json:"integrity"`
	Locations       []ResourceLocation `json:"locations,omitempty"`
	Annotations     map[string]string  `json:"annotations,omitempty"`
}

// ResourceHandle is a location-independent trusted resource descriptor.
type ResourceHandle struct {
	ResourceID string `json:"resource_id"`
	ResourceCandidate
}

// ResourceHandoff transfers one Resource Handle between durable Runs.
type ResourceHandoff struct {
	HandoffVersion string         `json:"handoff_version"`
	TransferID     string         `json:"transfer_id"`
	FromRun        string         `json:"from_run"`
	ToRun          string         `json:"to_run"`
	Slot           string         `json:"slot"`
	Resource       ResourceHandle `json:"resource"`
}

// ArtifactRef identifies immutable typed bytes in the semantic artifact store.
type ArtifactRef struct {
	ArtifactID string `json:"artifact_id"`
	Kind       string `json:"kind"`
}

// RolloutMode selects shadow, canary, active, or rolled-back future work.
type RolloutMode struct {
	Mode        string `json:"mode"`
	BasisPoints uint16 `json:"basis_points,omitempty"`
}

// RolloutDecision is one immutable future-selection decision.
type RolloutDecision struct {
	DecisionID   string      `json:"decision_id"`
	FallbackPlan string      `json:"fallback_plan"`
	TargetPlan   string      `json:"target_plan"`
	Mode         RolloutMode `json:"mode"`
}

// RolloutObservation is one terminal pinned-occurrence outcome.
type RolloutObservation struct {
	ObservationID string      `json:"observation_id"`
	DecisionID    string      `json:"decision_id"`
	OccurrenceID  string      `json:"occurrence_id"`
	PlanID        string      `json:"plan_id"`
	Outcome       string      `json:"outcome"`
	Evidence      ArtifactRef `json:"evidence"`
}

// RolloutGate contains deterministic promotion and rollback thresholds.
type RolloutGate struct {
	GateID                 string `json:"gate_id"`
	DecisionID             string `json:"decision_id"`
	MinTargetObservations  uint64 `json:"min_target_observations"`
	MaxTargetFailures      uint64 `json:"max_target_failures"`
	MinEquivalentShadows   uint64 `json:"min_equivalent_shadows"`
	MaxInequivalentShadows uint64 `json:"max_inequivalent_shadows"`
}

// MigrationRequest asks a pinned adapter for one safe-point transformation.
type MigrationRequest struct {
	MigrationID string      `json:"migration_id"`
	RunID       string      `json:"run_id"`
	FromPlan    string      `json:"from_plan"`
	ToPlan      string      `json:"to_plan"`
	SafePointID string      `json:"safe_point_id"`
	SourceEpoch uint64      `json:"source_epoch"`
	InputState  ArtifactRef `json:"input_state"`
}

// RestartRequest authorizes one replacement Run under an exact new Plan.
type RestartRequest struct {
	RestartID      string      `json:"restart_id"`
	SourceRun      string      `json:"source_run"`
	ReplacementRun string      `json:"replacement_run"`
	FromPlan       string      `json:"from_plan"`
	ToPlan         string      `json:"to_plan"`
	SafePointID    string      `json:"safe_point_id"`
	SourceEpoch    uint64      `json:"source_epoch"`
	Input          ArtifactRef `json:"input"`
	Evidence       ArtifactRef `json:"evidence"`
}

// ShadowRequest asks a pinned driver for isolated comparison evidence.
type ShadowRequest struct {
	ComparisonID     string      `json:"comparison_id"`
	DecisionID       string      `json:"decision_id"`
	Subject          string      `json:"subject"`
	PrimaryPlan      string      `json:"primary_plan"`
	ShadowPlan       string      `json:"shadow_plan"`
	Input            ArtifactRef `json:"input"`
	ComparisonPolicy string      `json:"comparison_policy"`
}

// PatchOperation declares one exact semantic difference.
type PatchOperation struct {
	Kind   string  `json:"kind"`
	Target string  `json:"target"`
	Before *string `json:"before"`
	After  *string `json:"after"`
}

// PlanPatch carries a complete reviewed target and exact declared diff.
type PlanPatch struct {
	FromPlan   string           `json:"from_plan"`
	Target     PlanCandidate    `json:"target"`
	Operations []PatchOperation `json:"operations"`
	Evidence   ArtifactRef      `json:"evidence"`
}

// EvolutionCommand is one closed idempotent M4 transport envelope.
type EvolutionCommand struct {
	ControlVersion string              `json:"control_version"`
	CommandID      string              `json:"command_id"`
	Operation      string              `json:"operation"`
	Patch          *PlanPatch          `json:"patch,omitempty"`
	Decision       *RolloutDecision    `json:"decision,omitempty"`
	OccurrenceID   string              `json:"occurrence_id,omitempty"`
	Migration      *MigrationRequest   `json:"-"`
	Restart        *RestartRequest     `json:"-"`
	Shadow         *ShadowRequest      `json:"-"`
	Observation    *RolloutObservation `json:"observation,omitempty"`
	Gate           *RolloutGate        `json:"gate,omitempty"`
	NextDecisionID string              `json:"next_decision_id,omitempty"`
}

// MigrationSafePoint is one content-addressed durable continuation proof.
type MigrationSafePoint struct {
	SafePointVersion   string       `json:"safe_point_version"`
	SafePointID        string       `json:"safe_point_id"`
	RunID              string       `json:"run_id"`
	PlanID             string       `json:"plan_id"`
	Epoch              uint64       `json:"epoch"`
	State              *ArtifactRef `json:"state"`
	ContinuationDigest string       `json:"continuation_digest"`
}

// SubflowReference selects one reusable definition revision for a parent.
type SubflowReference struct {
	LogicalRef      string         `json:"logical_ref"`
	LocalDefinition string         `json:"local_definition"`
	InputSchema     map[string]any `json:"input_schema"`
	OutputSchema    map[string]any `json:"output_schema"`
	Strategy        string         `json:"strategy"`
	RevisionID      string         `json:"revision_id,omitempty"`
}

// PlanTemplate is one unsealed parent plus its logical reusable references.
type PlanTemplate struct {
	TemplateID string             `json:"template_id"`
	Candidate  PlanCandidate      `json:"candidate"`
	References []SubflowReference `json:"references"`
}

// LivePublicationCommand atomically publishes and advances compatible parents.
type LivePublicationCommand struct {
	LogicalRef string      `json:"logical_ref"`
	Definition Definition  `json:"definition"`
	Evidence   ArtifactRef `json:"evidence"`
	Mode       RolloutMode `json:"mode"`
}

// LiveEvolutionCommand targets the unified registry, DAG, rollout, and pin authority.
type LiveEvolutionCommand struct {
	ControlVersion string                  `json:"control_version"`
	CommandID      string                  `json:"command_id"`
	Operation      string                  `json:"operation"`
	LogicalRef     string                  `json:"logical_ref,omitempty"`
	Definition     *Definition             `json:"definition,omitempty"`
	Template       *PlanTemplate           `json:"template,omitempty"`
	Publication    *LivePublicationCommand `json:"publication,omitempty"`
	TemplateID     string                  `json:"template_id,omitempty"`
	Command        *EvolutionCommand       `json:"command,omitempty"`
	SafePoint      *MigrationSafePoint     `json:"safe_point,omitempty"`
}

// MarshalJSON emits the exact operation-specific request shape.
func (command EvolutionCommand) MarshalJSON() ([]byte, error) {
	var request any
	switch command.Operation {
	case "migrate":
		request = command.Migration
	case "restart_under_new_plan":
		request = command.Restart
	case "shadow":
		request = command.Shadow
	}
	return json.Marshal(struct {
		ControlVersion string              `json:"control_version"`
		CommandID      string              `json:"command_id"`
		Operation      string              `json:"operation"`
		Patch          *PlanPatch          `json:"patch,omitempty"`
		Decision       *RolloutDecision    `json:"decision,omitempty"`
		OccurrenceID   string              `json:"occurrence_id,omitempty"`
		Request        any                 `json:"request,omitempty"`
		Observation    *RolloutObservation `json:"observation,omitempty"`
		Gate           *RolloutGate        `json:"gate,omitempty"`
		NextDecisionID string              `json:"next_decision_id,omitempty"`
	}{
		command.ControlVersion, command.CommandID, command.Operation, command.Patch,
		command.Decision, command.OccurrenceID, request, command.Observation,
		command.Gate, command.NextDecisionID,
	})
}

// UnmarshalJSON reads the closed operation-specific request into typed fields.
func (command *EvolutionCommand) UnmarshalJSON(input []byte) error {
	var wire struct {
		ControlVersion string              `json:"control_version"`
		CommandID      string              `json:"command_id"`
		Operation      string              `json:"operation"`
		Patch          *PlanPatch          `json:"patch"`
		Decision       *RolloutDecision    `json:"decision"`
		OccurrenceID   string              `json:"occurrence_id"`
		Request        json.RawMessage     `json:"request"`
		Observation    *RolloutObservation `json:"observation"`
		Gate           *RolloutGate        `json:"gate"`
		NextDecisionID string              `json:"next_decision_id"`
	}
	if err := json.Unmarshal(input, &wire); err != nil {
		return err
	}
	*command = EvolutionCommand{
		ControlVersion: wire.ControlVersion,
		CommandID:      wire.CommandID,
		Operation:      wire.Operation,
		Patch:          wire.Patch,
		Decision:       wire.Decision,
		OccurrenceID:   wire.OccurrenceID,
		Observation:    wire.Observation,
		Gate:           wire.Gate,
		NextDecisionID: wire.NextDecisionID,
	}
	switch wire.Operation {
	case "migrate":
		var request MigrationRequest
		if err := json.Unmarshal(wire.Request, &request); err != nil {
			return err
		}
		command.Migration = &request
	case "restart_under_new_plan":
		var request RestartRequest
		if err := json.Unmarshal(wire.Request, &request); err != nil {
			return err
		}
		command.Restart = &request
	case "shadow":
		var request ShadowRequest
		if err := json.Unmarshal(wire.Request, &request); err != nil {
			return err
		}
		command.Shadow = &request
	}
	return nil
}

// WaitActivationSource identifies a signal key or logical timer.
type WaitActivationSource struct {
	Kind    string `json:"kind"`
	Key     string `json:"key,omitempty"`
	TimerID string `json:"timer_id,omitempty"`
}

// WaitActivation is one identified external signal or timer delivery.
type WaitActivation struct {
	ActivationVersion string               `json:"activation_version"`
	ActivationID      string               `json:"activation_id"`
	Source            WaitActivationSource `json:"source"`
	WaitIDs           []string             `json:"wait_ids"`
	Result            ArtifactRef          `json:"result"`
}

// DurableCommand is one closed M1 mutation or read-only query.
type DurableCommand struct {
	Type           string                `json:"type"`
	ControlVersion string                `json:"control_version"`
	RunID          string                `json:"run_id,omitempty"`
	Candidate      *PlanCandidate        `json:"candidate,omitempty"`
	Input          json.RawMessage       `json:"input,omitempty"`
	ActivationID   string                `json:"activation_id,omitempty"`
	Source         *WaitActivationSource `json:"source,omitempty"`
	WaitIDs        []string              `json:"wait_ids,omitempty"`
	Value          json.RawMessage       `json:"value,omitempty"`
	IntentID       string                `json:"intent_id,omitempty"`
	QueryID        string                `json:"query_id,omitempty"`
}

// ParkReason identifies one exact indexed condition for virtual work.
type ParkReason struct {
	Kind       string `json:"kind"`
	Key        string `json:"key,omitempty"`
	WorkID     string `json:"work_id,omitempty"`
	Account    string `json:"account,omitempty"`
	Capability string `json:"capability,omitempty"`
	Domain     string `json:"domain,omitempty"`
}

// WorkItem is one materialized provider-neutral virtual item.
type WorkItem struct {
	WorkID     string      `json:"work_id"`
	RegionID   string      `json:"region_id"`
	RunID      string      `json:"run_id"`
	Payload    ArtifactRef `json:"payload"`
	Capability *string     `json:"capability"`
	Priority   int64       `json:"priority"`
	Cost       uint64      `json:"cost"`
}

// VirtualClaimLease fences one worker capacity slot.
type VirtualClaimLease struct {
	Resource  string `json:"resource"`
	Owner     string `json:"owner"`
	Epoch     uint64 `json:"epoch"`
	ExpiresAt uint64 `json:"expires_at"`
}

// ClaimedWork is one active occurrence and its current capacity-slot lease.
type ClaimedWork struct {
	Item              WorkItem          `json:"item"`
	Owner             string            `json:"owner"`
	Epoch             uint64            `json:"epoch"`
	OccurrenceID      string            `json:"occurrence_id"`
	OccurrenceBinding string            `json:"occurrence_binding"`
	Lease             VirtualClaimLease `json:"lease"`
}

// WorkOccurrence is one binding-pinned M3 work attempt.
type WorkOccurrence struct {
	OccurrenceVersion string       `json:"occurrence_version"`
	OccurrenceID      string       `json:"occurrence_id"`
	WorkID            string       `json:"work_id"`
	RegionID          string       `json:"region_id"`
	RunID             string       `json:"run_id"`
	Owner             string       `json:"owner"`
	Epoch             uint64       `json:"epoch"`
	LeaseEpoch        uint64       `json:"lease_epoch"`
	OccurrenceBinding string       `json:"occurrence_binding"`
	State             string       `json:"state"`
	Result            *ArtifactRef `json:"result"`
	Error             *ArtifactRef `json:"error"`
	NextReason        *ParkReason  `json:"next_reason"`
}

// WorkResolution is one success, retry, park, failure, or cancellation proposal.
type WorkResolution struct {
	Kind         string
	Result       *ArtifactRef
	Error        *ArtifactRef
	ParkReason   *ParkReason
	CancelReason *ArtifactRef
	NextReason   *ParkReason
}

// MarshalJSON emits one closed disposition-specific wire shape.
func (resolution WorkResolution) MarshalJSON() ([]byte, error) {
	switch resolution.Kind {
	case "succeeded":
		return json.Marshal(struct {
			Resolution string       `json:"resolution"`
			Result     *ArtifactRef `json:"result"`
		}{resolution.Kind, resolution.Result})
	case "retry":
		return json.Marshal(struct {
			Resolution string       `json:"resolution"`
			Error      *ArtifactRef `json:"error"`
			NextReason *ParkReason  `json:"next_reason"`
		}{resolution.Kind, resolution.Error, resolution.NextReason})
	case "parked":
		return json.Marshal(struct {
			Resolution string      `json:"resolution"`
			Reason     *ParkReason `json:"reason"`
		}{resolution.Kind, resolution.ParkReason})
	case "failed":
		return json.Marshal(struct {
			Resolution string       `json:"resolution"`
			Error      *ArtifactRef `json:"error"`
		}{resolution.Kind, resolution.Error})
	case "cancelled":
		return json.Marshal(struct {
			Resolution string       `json:"resolution"`
			Reason     *ArtifactRef `json:"reason"`
		}{resolution.Kind, resolution.CancelReason})
	default:
		return nil, fmt.Errorf("unsupported work resolution %q", resolution.Kind)
	}
}

// UnmarshalJSON reads one disposition-specific wire shape.
func (resolution *WorkResolution) UnmarshalJSON(input []byte) error {
	var tagged struct {
		Resolution string `json:"resolution"`
	}
	if err := json.Unmarshal(input, &tagged); err != nil {
		return err
	}
	switch tagged.Resolution {
	case "succeeded":
		var value struct {
			Result ArtifactRef `json:"result"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*resolution = WorkResolution{Kind: tagged.Resolution, Result: &value.Result}
	case "retry":
		var value struct {
			Error      ArtifactRef `json:"error"`
			NextReason *ParkReason `json:"next_reason"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*resolution = WorkResolution{
			Kind: tagged.Resolution, Error: &value.Error, NextReason: value.NextReason,
		}
	case "parked":
		var value struct {
			Reason ParkReason `json:"reason"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*resolution = WorkResolution{Kind: tagged.Resolution, ParkReason: &value.Reason}
	case "failed":
		var value struct {
			Error ArtifactRef `json:"error"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*resolution = WorkResolution{Kind: tagged.Resolution, Error: &value.Error}
	case "cancelled":
		var value struct {
			Reason ArtifactRef `json:"reason"`
		}
		if err := json.Unmarshal(input, &value); err != nil {
			return err
		}
		*resolution = WorkResolution{Kind: tagged.Resolution, CancelReason: &value.Reason}
	default:
		return fmt.Errorf("unsupported work resolution %q", tagged.Resolution)
	}
	return nil
}

// WorkResolutionCommand preconditions one idempotent M3 control mutation.
type WorkResolutionCommand struct {
	ControlVersion     string         `json:"control_version"`
	CommandID          string         `json:"command_id"`
	WorkID             string         `json:"work_id"`
	Owner              string         `json:"owner"`
	Epoch              uint64         `json:"epoch"`
	ExpectedLeaseEpoch uint64         `json:"expected_lease_epoch"`
	ObservedAt         uint64         `json:"observed_at"`
	Resolution         WorkResolution `json:"resolution"`
}

// VirtualCursor is an opaque provider-owned logical source position.
type VirtualCursor struct {
	Version   string `json:"version"`
	Position  string `json:"position"`
	Exhausted bool   `json:"exhausted"`
}

// VirtualRegion is one active or retired virtual source region.
type VirtualRegion struct {
	RegionID       string        `json:"region_id"`
	RunID          string        `json:"run_id"`
	Source         string        `json:"source"`
	Cursor         VirtualCursor `json:"cursor"`
	EstimatedTotal *uint64       `json:"estimated_total"`
}

// RegionMigrationRequest is passed to a replaceable cursor migration adapter.
type RegionMigrationRequest struct {
	MigrationID      string   `json:"migration_id"`
	Kind             string   `json:"kind"`
	SourceRegionIDs  []string `json:"source_region_ids"`
	TargetCount      uint64   `json:"target_count"`
	MigrationBinding string   `json:"migration_binding"`
}

// RegionMigrationPlan replaces exact source cursors with evidenced targets.
type RegionMigrationPlan struct {
	MigrationVersion string                   `json:"migration_version"`
	MigrationID      string                   `json:"migration_id"`
	Kind             string                   `json:"kind"`
	ExpectedSources  map[string]VirtualCursor `json:"expected_sources"`
	Targets          []VirtualRegion          `json:"targets"`
	MigrationBinding string                   `json:"migration_binding"`
	CoverageEvidence ArtifactRef              `json:"coverage_evidence"`
}

// RegionMigrationCommand applies one adapter-produced plan idempotently.
type RegionMigrationCommand struct {
	ControlVersion string              `json:"control_version"`
	CommandID      string              `json:"command_id"`
	Plan           RegionMigrationPlan `json:"plan"`
}

// RegionMigrationReceipt retains retirement and target activation evidence.
type RegionMigrationReceipt struct {
	Plan           RegionMigrationPlan `json:"plan"`
	RetiredRegions []string            `json:"retired_regions"`
	ActiveTargets  []string            `json:"active_targets"`
}

// ReplayAvailability reports exact, projection-only, or unavailable replay.
type ReplayAvailability struct {
	Status  string   `json:"status"`
	Missing []string `json:"missing,omitempty"`
	Reason  string   `json:"reason,omitempty"`
}

// VirtualCompletionSummary is one bounded completed-region projection.
type VirtualCompletionSummary struct {
	RegionID                 string `json:"region_id"`
	RunID                    string `json:"run_id"`
	OccurrenceCount          uint64 `json:"occurrence_count"`
	WorkCount                uint64 `json:"work_count"`
	SucceededCount           uint64 `json:"succeeded_count"`
	FailedCount              uint64 `json:"failed_count"`
	CancelledCount           uint64 `json:"cancelled_count"`
	OutputDigest             string `json:"output_digest"`
	EvidenceDigest           string `json:"evidence_digest"`
	RetainedDebugIndexDigest string `json:"retained_debug_index_digest"`
}

// VirtualCompactionCertificate authenticates exact cold occurrence history.
type VirtualCompactionCertificate struct {
	CertificateVersion         string                   `json:"certificate_version"`
	CertificateID              string                   `json:"certificate_id"`
	SourceCausalCut            []string                 `json:"source_causal_cut"`
	Summary                    VirtualCompletionSummary `json:"summary"`
	SummaryStateDigest         string                   `json:"summary_state_digest"`
	UnresolvedObligations      []string                 `json:"unresolved_obligations"`
	RetainedOccurrenceBindings []string                 `json:"retained_occurrence_bindings"`
	ReplayAvailability         ReplayAvailability       `json:"replay_availability"`
	RehydrationManifest        ArtifactRef              `json:"rehydration_manifest"`
	CompactorBinding           string                   `json:"compactor_binding"`
	CompactorRevision          string                   `json:"compactor_revision"`
}

// VirtualCompactionCommand requests one idempotent completed-region archive.
type VirtualCompactionCommand struct {
	ControlVersion    string   `json:"control_version"`
	CommandID         string   `json:"command_id"`
	RegionID          string   `json:"region_id"`
	SourceCausalCut   []string `json:"source_causal_cut"`
	CompactorBinding  string   `json:"compactor_binding"`
	CompactorRevision string   `json:"compactor_revision"`
}

// VirtualCompactionReceipt retains the command and verified certificate.
type VirtualCompactionReceipt struct {
	Command     VirtualCompactionCommand     `json:"command"`
	Certificate VirtualCompactionCertificate `json:"certificate"`
}

// VirtualRehydrationCommand selects exact archived occurrences to restore.
type VirtualRehydrationCommand struct {
	ControlVersion string   `json:"control_version"`
	CommandID      string   `json:"command_id"`
	CertificateID  string   `json:"certificate_id"`
	OccurrenceIDs  []string `json:"occurrence_ids"`
}

// VirtualRehydrationReceipt records exact restored occurrence identities.
type VirtualRehydrationReceipt struct {
	Command               VirtualRehydrationCommand `json:"command"`
	RestoredOccurrenceIDs []string                  `json:"restored_occurrence_ids"`
}

// VirtualClaimCommand requests at most one item through one capacity slot.
type VirtualClaimCommand struct {
	ControlVersion    string   `json:"control_version"`
	CommandID         string   `json:"command_id"`
	Owner             string   `json:"owner"`
	SlotID            string   `json:"slot_id"`
	OccurrenceBinding string   `json:"occurrence_binding"`
	Capabilities      []string `json:"capabilities"`
	LogicalNow        uint64   `json:"logical_now"`
	LeaseTTL          uint64   `json:"lease_ttl"`
}

// VirtualClaimReceipt contains claimed work or a durable empty observation.
type VirtualClaimReceipt struct {
	Command VirtualClaimCommand `json:"command"`
	Claim   *ClaimedWork        `json:"claim"`
}

// VirtualLeaseRenewalCommand advances one active capacity-slot lease fence.
type VirtualLeaseRenewalCommand struct {
	ControlVersion     string `json:"control_version"`
	CommandID          string `json:"command_id"`
	WorkID             string `json:"work_id"`
	Owner              string `json:"owner"`
	Epoch              uint64 `json:"epoch"`
	ExpectedLeaseEpoch uint64 `json:"expected_lease_epoch"`
	LogicalNow         uint64 `json:"logical_now"`
	LeaseTTL           uint64 `json:"lease_ttl"`
}

// VirtualLeaseRenewalReceipt retains the new slot fence.
type VirtualLeaseRenewalReceipt struct {
	Command VirtualLeaseRenewalCommand `json:"command"`
	Lease   VirtualClaimLease          `json:"lease"`
}

// VirtualRecoveryCommand explicitly retries, fails, or cancels expired work.
type VirtualRecoveryCommand struct {
	ControlVersion     string         `json:"control_version"`
	CommandID          string         `json:"command_id"`
	WorkID             string         `json:"work_id"`
	ExpectedOwner      string         `json:"expected_owner"`
	ExpectedEpoch      uint64         `json:"expected_epoch"`
	ExpectedLeaseEpoch uint64         `json:"expected_lease_epoch"`
	ObservedAt         uint64         `json:"observed_at"`
	Resolution         WorkResolution `json:"resolution"`
}

// VirtualRecoveryReceipt retains the expired occurrence disposition.
type VirtualRecoveryReceipt struct {
	Command    VirtualRecoveryCommand `json:"command"`
	Occurrence WorkOccurrence         `json:"occurrence"`
}

// VirtualRunWeightCommand updates one Run's future weighted share.
type VirtualRunWeightCommand struct {
	ControlVersion string `json:"control_version"`
	CommandID      string `json:"command_id"`
	RunID          string `json:"run_id"`
	Weight         uint32 `json:"weight"`
}

// VirtualRunWeightReceipt retains previous and current scheduling shares.
type VirtualRunWeightReceipt struct {
	Command        VirtualRunWeightCommand `json:"command"`
	PreviousWeight uint32                  `json:"previous_weight"`
	CurrentWeight  uint32                  `json:"current_weight"`
}

// VirtualArchive is a replaceable immutable byte archive.
type VirtualArchive interface {
	Binding() string
	Put(reference ArtifactRef, data []byte) error
	Get(reference ArtifactRef) ([]byte, error)
}

// RegionMigrator is a replaceable opaque-cursor split/merge adapter.
type RegionMigrator interface {
	Binding() string
	Plan(request RegionMigrationRequest, sources []VirtualRegion) (RegionMigrationPlan, error)
	Verify(plan RegionMigrationPlan) error
}

// EvolutionControl submits typed M4 commands to durable Rust authority.
type EvolutionControl interface {
	Submit(command EvolutionCommand) (any, error)
}

// LiveEvolutionControl submits commands to the unified durable authority.
type LiveEvolutionControl interface {
	Submit(command LiveEvolutionCommand) (any, error)
}

// DurableControl submits M1 mutations and queries to durable Rust authority.
type DurableControl interface {
	Submit(command DurableCommand) (any, error)
}

// VirtualWorkControl is a transport-neutral occurrence query/control boundary.
type VirtualWorkControl interface {
	Occurrence(occurrenceID string) (*WorkOccurrence, error)
	Resolve(command WorkResolutionCommand) (WorkOccurrence, error)
	Migrate(command RegionMigrationCommand) (RegionMigrationReceipt, error)
	Compact(command VirtualCompactionCommand) (VirtualCompactionReceipt, error)
	Rehydrate(command VirtualRehydrationCommand) (VirtualRehydrationReceipt, error)
}

// VirtualSchedulingControl is a transport-neutral worker-slot control boundary.
type VirtualSchedulingControl interface {
	Claim(command VirtualClaimCommand) (VirtualClaimReceipt, error)
	Renew(command VirtualLeaseRenewalCommand) (VirtualLeaseRenewalReceipt, error)
	Recover(command VirtualRecoveryCommand) (VirtualRecoveryReceipt, error)
	SetRunWeight(command VirtualRunWeightCommand) (VirtualRunWeightReceipt, error)
}

func evolutionCommand(commandID, operation string) EvolutionCommand {
	return EvolutionCommand{
		ControlVersion: "cymule.evolution-control/2",
		CommandID:      commandID,
		Operation:      operation,
	}
}

func liveEvolutionCommand(commandID, operation string) LiveEvolutionCommand {
	return LiveEvolutionCommand{
		ControlVersion: "cymule.live-evolution-control/1",
		CommandID:      commandID,
		Operation:      operation,
	}
}

// PublishLiveDefinition builds one reusable-definition publication command.
func PublishLiveDefinition(commandID, logicalRef string, definition Definition) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "publish_definition")
	command.LogicalRef = logicalRef
	command.Definition = &definition
	return command
}

// RegisterLiveTemplate builds one parent-template registration command.
func RegisterLiveTemplate(commandID string, template PlanTemplate) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "register_template")
	command.Template = &template
	return command
}

// PublishAndRelinkLive builds one transitive compatible-update command.
func PublishAndRelinkLive(commandID string, publication LivePublicationCommand) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "publish_and_relink")
	command.Publication = &publication
	return command
}

// ApplyLiveEvolution scopes one existing evolution operation to a parent template.
func ApplyLiveEvolution(
	commandID, templateID string,
	operation EvolutionCommand,
	safePoint *MigrationSafePoint,
) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "apply")
	command.TemplateID = templateID
	command.Command = &operation
	command.SafePoint = safePoint
	return command
}

// ApplyPlanPatch builds one exact reviewed patch command.
func ApplyPlanPatch(commandID string, patch PlanPatch) EvolutionCommand {
	command := evolutionCommand(commandID, "apply_patch")
	command.Patch = &patch
	return command
}

// SetEvolutionRollout builds one future-selection decision command.
func SetEvolutionRollout(commandID string, decision RolloutDecision) EvolutionCommand {
	command := evolutionCommand(commandID, "set_rollout")
	command.Decision = &decision
	return command
}

// SelectEvolutionOccurrence builds one immutable occurrence selection command.
func SelectEvolutionOccurrence(commandID, occurrenceID string) EvolutionCommand {
	command := evolutionCommand(commandID, "select_occurrence")
	command.OccurrenceID = occurrenceID
	return command
}

// MigrateEvolutionState builds one checked safe-point migration command.
func MigrateEvolutionState(commandID string, request MigrationRequest) EvolutionCommand {
	command := evolutionCommand(commandID, "migrate")
	command.Migration = &request
	return command
}

// RestartEvolutionRun builds one explicit replacement-Run authorization command.
func RestartEvolutionRun(commandID string, request RestartRequest) EvolutionCommand {
	command := evolutionCommand(commandID, "restart_under_new_plan")
	command.Restart = &request
	return command
}

// RunEvolutionShadow builds one isolated shadow-comparison command.
func RunEvolutionShadow(commandID string, request ShadowRequest) EvolutionCommand {
	command := evolutionCommand(commandID, "shadow")
	command.Shadow = &request
	return command
}

// ObserveEvolutionRollout builds one terminal rollout-observation command.
func ObserveEvolutionRollout(commandID string, observation RolloutObservation) EvolutionCommand {
	command := evolutionCommand(commandID, "observe")
	command.Observation = &observation
	return command
}

// ApplyEvolutionGate builds one deterministic promotion or rollback command.
func ApplyEvolutionGate(commandID string, gate RolloutGate, nextDecisionID string) EvolutionCommand {
	command := evolutionCommand(commandID, "apply_gate")
	command.Gate = &gate
	command.NextDecisionID = nextDecisionID
	return command
}

// SucceedWork creates a terminal-success control command.
func SucceedWork(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, result ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, observedAt, WorkResolution{
		Kind: "succeeded", Result: &result,
	})
}

// RetryWork creates a retry control command with an optional indexed condition.
func RetryWork(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, failure ArtifactRef, nextReason *ParkReason) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, observedAt, WorkResolution{
		Kind: "retry", Error: &failure, NextReason: nextReason,
	})
}

// ParkWork creates a non-failure parked disposition command.
func ParkWork(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, reason ParkReason) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, observedAt, WorkResolution{
		Kind: "parked", ParkReason: &reason,
	})
}

// FailWork creates a terminal-failure control command.
func FailWork(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, failure ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, observedAt, WorkResolution{
		Kind: "failed", Error: &failure,
	})
}

// CancelWork creates an active-occurrence cancellation command.
func CancelWork(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, reason ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, observedAt, WorkResolution{
		Kind: "cancelled", CancelReason: &reason,
	})
}

// MigrateRegions wraps one adapter-produced split/merge plan in a stable command.
func MigrateRegions(commandID string, plan RegionMigrationPlan) RegionMigrationCommand {
	return RegionMigrationCommand{
		ControlVersion: "cymule.virtual-region-migration-control/1",
		CommandID:      commandID,
		Plan:           plan,
	}
}

// CompactVirtualRegion creates one completed-region compaction command.
func CompactVirtualRegion(commandID, regionID string, sourceCausalCut []string, compactorBinding, compactorRevision string) VirtualCompactionCommand {
	return VirtualCompactionCommand{
		ControlVersion:    "cymule.virtual-compaction-control/1",
		CommandID:         commandID,
		RegionID:          regionID,
		SourceCausalCut:   uniqueSorted(sourceCausalCut),
		CompactorBinding:  compactorBinding,
		CompactorRevision: compactorRevision,
	}
}

// RehydrateVirtualOccurrences creates one exact archived-occurrence selection.
func RehydrateVirtualOccurrences(commandID, certificateID string, occurrenceIDs []string) VirtualRehydrationCommand {
	return VirtualRehydrationCommand{
		ControlVersion: "cymule.virtual-rehydration-control/1",
		CommandID:      commandID,
		CertificateID:  certificateID,
		OccurrenceIDs:  uniqueSorted(occurrenceIDs),
	}
}

// ClaimVirtualWork creates one idempotent capacity-slot claim command.
func ClaimVirtualWork(commandID, owner, slotID, occurrenceBinding string, capabilities []string, logicalNow, leaseTTL uint64) VirtualClaimCommand {
	return VirtualClaimCommand{
		ControlVersion:    "cymule.virtual-claim-control/1",
		CommandID:         commandID,
		Owner:             owner,
		SlotID:            slotID,
		OccurrenceBinding: occurrenceBinding,
		Capabilities:      uniqueSorted(capabilities),
		LogicalNow:        logicalNow,
		LeaseTTL:          leaseTTL,
	}
}

// RenewVirtualClaim creates one active-claim lease renewal command.
func RenewVirtualClaim(commandID, workID, owner string, epoch, expectedLeaseEpoch, logicalNow, leaseTTL uint64) VirtualLeaseRenewalCommand {
	return VirtualLeaseRenewalCommand{
		ControlVersion:     "cymule.virtual-lease-renewal-control/1",
		CommandID:          commandID,
		WorkID:             workID,
		Owner:              owner,
		Epoch:              epoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		LogicalNow:         logicalNow,
		LeaseTTL:           leaseTTL,
	}
}

// RecoverVirtualClaim creates one explicit expired-claim disposition command.
func RecoverVirtualClaim(commandID, workID, expectedOwner string, expectedEpoch, expectedLeaseEpoch, observedAt uint64, resolution WorkResolution) VirtualRecoveryCommand {
	return VirtualRecoveryCommand{
		ControlVersion:     "cymule.virtual-recovery-control/1",
		CommandID:          commandID,
		WorkID:             workID,
		ExpectedOwner:      expectedOwner,
		ExpectedEpoch:      expectedEpoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		ObservedAt:         observedAt,
		Resolution:         resolution,
	}
}

// SetVirtualRunWeight creates one future weighted-share update command.
func SetVirtualRunWeight(commandID, runID string, weight uint32) VirtualRunWeightCommand {
	return VirtualRunWeightCommand{
		ControlVersion: "cymule.virtual-run-weight-control/1",
		CommandID:      commandID,
		RunID:          runID,
		Weight:         weight,
	}
}

func workResolutionCommand(commandID, workID, owner string, epoch, expectedLeaseEpoch, observedAt uint64, resolution WorkResolution) WorkResolutionCommand {
	return WorkResolutionCommand{
		ControlVersion:     "cymule.virtual-work-control/1",
		CommandID:          commandID,
		WorkID:             workID,
		Owner:              owner,
		Epoch:              epoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		ObservedAt:         observedAt,
		Resolution:         resolution,
	}
}

// SignalWaitActivation creates a deterministic signal delivery record.
func SignalWaitActivation(activationID, key string, waitIDs []string, result ArtifactRef) WaitActivation {
	targets := uniqueSorted(waitIDs)
	return WaitActivation{
		ActivationVersion: "cymule.wait-activation/1",
		ActivationID:      activationID,
		Source:            WaitActivationSource{Kind: "signal", Key: key},
		WaitIDs:           targets,
		Result:            result,
	}
}

// TimerWaitActivation creates a single-target logical timer delivery record.
func TimerWaitActivation(activationID, timerID, waitID string, result ArtifactRef) WaitActivation {
	return WaitActivation{
		ActivationVersion: "cymule.wait-activation/1",
		ActivationID:      activationID,
		Source:            WaitActivationSource{Kind: "timer", TimerID: timerID},
		WaitIDs:           []string{waitID},
		Result:            result,
	}
}

// StartDurableRun builds one M1 Run-creation command.
func StartDurableRun(runID string, candidate PlanCandidate, input any) (DurableCommand, error) {
	if runID == "" {
		return DurableCommand{}, fmt.Errorf("durable Run identity must not be empty")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type:           "start_run",
		ControlVersion: "cymule.durable-control/1",
		RunID:          runID,
		Candidate:      &candidate,
		Input:          encoded,
	}, nil
}

// ResumeDurableRun builds one M1 resume command.
func ResumeDurableRun(runID string) DurableCommand {
	return DurableCommand{
		Type: "resume_run", ControlVersion: "cymule.durable-control/1", RunID: runID,
	}
}

// ActivateDurableSignal builds one identified signal-admission command.
func ActivateDurableSignal(
	activationID, key string,
	waitIDs []string,
	value any,
) (DurableCommand, error) {
	return activateDurableWait(
		activationID,
		WaitActivationSource{Kind: "signal", Key: key},
		waitIDs,
		value,
	)
}

// ActivateDurableTimer builds one identified timer-admission command.
func ActivateDurableTimer(
	activationID, timerID, waitID string,
	value any,
) (DurableCommand, error) {
	return activateDurableWait(
		activationID,
		WaitActivationSource{Kind: "timer", TimerID: timerID},
		[]string{waitID},
		value,
	)
}

func activateDurableWait(
	activationID string,
	source WaitActivationSource,
	waitIDs []string,
	value any,
) (DurableCommand, error) {
	targets := uniqueSorted(waitIDs)
	if activationID == "" || len(targets) == 0 {
		return DurableCommand{}, fmt.Errorf("durable activation requires identity and targets")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type:           "activate_wait",
		ControlVersion: "cymule.durable-control/1",
		ActivationID:   activationID,
		Source:         &source,
		WaitIDs:        targets,
		Value:          encoded,
	}, nil
}

// ReleaseDurableEffect builds one explicit effect-release command.
func ReleaseDurableEffect(intentID string) DurableCommand {
	return DurableCommand{
		Type: "release_effect", ControlVersion: "cymule.durable-control/1", IntentID: intentID,
	}
}

// QueryDurableRun builds one read-only Run query.
func QueryDurableRun(queryID, runID string) DurableCommand {
	return DurableCommand{
		Type: "query_run", ControlVersion: "cymule.durable-control/1",
		QueryID: queryID, RunID: runID,
	}
}

// QueryDurableDomain builds one read-only domain query.
func QueryDurableDomain(queryID string) DurableCommand {
	return DurableCommand{
		Type: "query_domain", ControlVersion: "cymule.durable-control/1", QueryID: queryID,
	}
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// NewResourceHandoff creates one M1 Run-to-Run handoff record.
func NewResourceHandoff(transferID, fromRun, toRun, slot string, resource ResourceHandle) ResourceHandoff {
	return ResourceHandoff{
		HandoffVersion: "cymule.resource-handoff/1",
		TransferID:     transferID,
		FromRun:        fromRun,
		ToRun:          toRun,
		Slot:           slot,
		Resource:       resource,
	}
}

// TextResource creates one inline UTF-8 Resource Candidate.
func TextResource(text string, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/1",
		Shape:           "inline",
		MediaType:       "text/plain;charset=utf-8",
		Inline:          &InlineData{Encoding: "utf8", Text: text},
		Integrity:       ResourceIntegrity{Kind: "inline"},
		Annotations:     annotations,
	}
}

// JSONResource creates one inline structured Resource Candidate.
func JSONResource(value any, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/1",
		Shape:           "inline",
		MediaType:       "application/json",
		Inline:          &InlineData{Encoding: "json", Value: value},
		Integrity:       ResourceIntegrity{Kind: "inline"},
		Annotations:     annotations,
	}
}

// ExternalResource creates a provider-neutral external Resource Candidate.
func ExternalResource(shape, mediaType string, integrity ResourceIntegrity, locations []ResourceLocation, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/1",
		Shape:           shape,
		MediaType:       mediaType,
		Integrity:       integrity,
		Locations:       locations,
		Annotations:     annotations,
	}
}

// EffectProfile describes provider-neutral effect behavior.
type EffectProfile struct {
	Mutation         string `json:"mutation"`
	Dispatch         string `json:"dispatch"`
	Reconciliation   string `json:"reconciliation"`
	KeyedIdempotency bool   `json:"keyed_idempotency"`
	Irreversible     bool   `json:"irreversible"`
}

// Contract declares one abstract operation.
type Contract struct {
	ID           string            `json:"id"`
	InputSchema  map[string]any    `json:"input_schema"`
	OutputSchema map[string]any    `json:"output_schema"`
	Requirements map[string]string `json:"requirements"`
}

// EffectContract declares one abstract world effect.
type EffectContract struct {
	ID           string            `json:"id"`
	InputSchema  map[string]any    `json:"input_schema"`
	OutputSchema map[string]any    `json:"output_schema"`
	Profile      EffectProfile     `json:"profile"`
	Requirements map[string]string `json:"requirements"`
}

// Step is a closed IR step encoded with operation-specific fields.
type Step map[string]any

// Region is a structured sequence and result.
type Region struct {
	Steps  []Step     `json:"steps"`
	Result Expression `json:"result"`
}

// Definition is one named Flow definition.
type Definition struct {
	ID           string         `json:"id"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema"`
	Body         Region         `json:"body"`
}

// PlanCandidate is the frozen language-neutral plan proposal.
type PlanCandidate struct {
	IRVersion   string            `json:"ir_version"`
	Name        string            `json:"name"`
	Entry       string            `json:"entry"`
	Components  []Contract        `json:"components"`
	Effects     []EffectContract  `json:"effects"`
	Definitions []Definition      `json:"definitions"`
	Metadata    map[string]string `json:"metadata"`
}

// SealedPlan is a trusted content-addressed plan.
type SealedPlan struct {
	PlanID    string        `json:"plan_id"`
	Candidate PlanCandidate `json:"candidate"`
}

// ExecutionResult is a terminal Embedded-profile result.
type ExecutionResult struct {
	RunID             string   `json:"run_id"`
	PlanID            string   `json:"plan_id"`
	Value             any      `json:"value"`
	ProjectionDigest  string   `json:"projection_digest"`
	PreconditionToken string   `json:"precondition_token"`
	Effects           []string `json:"effects"`
}

// FlowBuilder builds one-definition Plan Candidates.
type FlowBuilder struct {
	candidate PlanCandidate
}

// NewFlow creates a Flow builder.
func NewFlow(name string, inputSchema, outputSchema map[string]any) *FlowBuilder {
	return &FlowBuilder{candidate: PlanCandidate{
		IRVersion:  "cymule.ir/2",
		Name:       name,
		Entry:      "main",
		Components: []Contract{},
		Effects:    []EffectContract{},
		Definitions: []Definition{{
			ID:           "main",
			InputSchema:  inputSchema,
			OutputSchema: outputSchema,
			Body:         Region{Steps: []Step{}, Result: Expression{"kind": "literal", "value": nil}},
		}},
		Metadata: map[string]string{},
	}}
}

// Component declares an abstract component.
func (builder *FlowBuilder) Component(id string, inputSchema, outputSchema map[string]any) *FlowBuilder {
	builder.candidate.Components = append(builder.candidate.Components, Contract{
		ID: id, InputSchema: inputSchema, OutputSchema: outputSchema, Requirements: map[string]string{},
	})
	return builder
}

// EffectContract declares an abstract world effect.
func (builder *FlowBuilder) EffectContract(id string, inputSchema, outputSchema map[string]any, profile EffectProfile) *FlowBuilder {
	builder.candidate.Effects = append(builder.candidate.Effects, EffectContract{
		ID: id, InputSchema: inputSchema, OutputSchema: outputSchema, Profile: profile, Requirements: map[string]string{},
	})
	return builder
}

// Call appends a component call.
func (builder *FlowBuilder) Call(site, component string, input Expression, bind string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "call", "component": component, "input": input, "bind": bind,
	})
	return builder
}

// Definition adds one reusable definition to the same immutable Plan.
func (builder *FlowBuilder) Definition(definition Definition) *FlowBuilder {
	builder.candidate.Definitions = append(builder.candidate.Definitions, definition)
	return builder
}

// Invoke appends one reusable definition invocation.
func (builder *FlowBuilder) Invoke(site, definition string, input Expression, bind string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "invoke", "definition": definition, "input": input, "bind": bind,
	})
	return builder
}

// Effect appends an external effect.
func (builder *FlowBuilder) Effect(site, effect string, input Expression, occurrence string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "effect", "effect": effect, "input": input, "occurrence": occurrence,
	})
	return builder
}

// Wait appends a durable suspension boundary.
func (builder *FlowBuilder) Wait(site string, wait map[string]any) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "wait", "wait": wait,
	})
	return builder
}

// Scope appends a structured transactional or speculative scope.
func (builder *FlowBuilder) Scope(site, mode string, body Region, bind string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "scope", "mode": mode, "body": body, "bind": bind,
	})
	return builder
}

// Finish returns a complete candidate.
func (builder *FlowBuilder) Finish(result Expression) PlanCandidate {
	builder.candidate.Definitions[0].Body.Result = result
	return builder.candidate
}

// CliEngine invokes the trusted Rust command-line Engine.
type CliEngine struct {
	Executable string
}

// Seal validates and content-addresses a candidate.
func (engine CliEngine) Seal(candidate PlanCandidate) (SealedPlan, error) {
	var response struct {
		Type string     `json:"type"`
		Plan SealedPlan `json:"plan"`
	}
	err := engine.request(map[string]any{"type": "seal", "candidate": candidate}, &response)
	if err == nil && response.Type != "sealed" {
		err = unexpectedEngineResponse("sealed", response.Type)
	}
	return response.Plan, err
}

// SealResource validates and seals a Resource Candidate with the Rust engine.
func (engine CliEngine) SealResource(candidate ResourceCandidate) (ResourceHandle, error) {
	var response struct {
		Type     string         `json:"type"`
		Resource ResourceHandle `json:"resource"`
	}
	err := engine.request(map[string]any{"type": "seal_resource", "candidate": candidate}, &response)
	if err == nil && response.Type != "sealed_resource" {
		err = unexpectedEngineResponse("sealed_resource", response.Type)
	}
	return response.Resource, err
}

// VerifyWaitActivation validates a signal or timer delivery with the Rust engine.
func (engine CliEngine) VerifyWaitActivation(activation WaitActivation) (WaitActivation, error) {
	var response struct {
		Type       string         `json:"type"`
		Activation WaitActivation `json:"activation"`
	}
	err := engine.request(map[string]any{
		"type": "verify_wait_activation", "activation": activation,
	}, &response)
	if err == nil && response.Type != "verified_wait_activation" {
		err = unexpectedEngineResponse("verified_wait_activation", response.Type)
	}
	return response.Activation, err
}

// VerifyDurableCommand validates one M1 envelope with the Rust engine.
func (engine CliEngine) VerifyDurableCommand(command DurableCommand) (DurableCommand, error) {
	var response struct {
		Type    string         `json:"type"`
		Command DurableCommand `json:"command"`
	}
	err := engine.request(map[string]any{
		"type": "verify_durable_command", "command": command,
	}, &response)
	if err == nil && response.Type != "verified_durable_command" {
		err = unexpectedEngineResponse("verified_durable_command", response.Type)
	}
	return response.Command, err
}

// VerifyEvolutionCommand validates one M4 envelope with the Rust engine.
func (engine CliEngine) VerifyEvolutionCommand(command EvolutionCommand) (EvolutionCommand, error) {
	var response struct {
		Type    string           `json:"type"`
		Command EvolutionCommand `json:"command"`
	}
	err := engine.request(map[string]any{
		"type": "verify_evolution_command", "command": command,
	}, &response)
	if err == nil && response.Type != "verified_evolution_command" {
		err = unexpectedEngineResponse("verified_evolution_command", response.Type)
	}
	return response.Command, err
}

// VerifyLiveEvolutionCommand validates one unified control envelope.
func (engine CliEngine) VerifyLiveEvolutionCommand(
	command LiveEvolutionCommand,
) (LiveEvolutionCommand, error) {
	var response struct {
		Type    string               `json:"type"`
		Command LiveEvolutionCommand `json:"command"`
	}
	err := engine.request(map[string]any{
		"type": "verify_live_evolution_command", "command": command,
	}, &response)
	if err == nil && response.Type != "verified_live_evolution_command" {
		err = unexpectedEngineResponse("verified_live_evolution_command", response.Type)
	}
	return response.Command, err
}

// Run executes a sealed plan through one plugin realization.
func (engine CliEngine) Run(plan SealedPlan, input any, plugin, runID string) (ExecutionResult, error) {
	var response struct {
		Type   string          `json:"type"`
		Result ExecutionResult `json:"result"`
	}
	err := engine.request(map[string]any{
		"type": "run", "plan": plan, "input": input, "plugin": plugin, "run_id": runID,
	}, &response)
	if err == nil && response.Type != "executed" {
		err = unexpectedEngineResponse("executed", response.Type)
	}
	return response.Result, err
}

func (engine CliEngine) request(request any, response any) error {
	input, err := json.Marshal(struct {
		EngineProtocol string `json:"engine_protocol"`
		Request        any    `json:"request"`
	}{EngineProtocolVersion, request})
	if err != nil {
		return transportFailure("request_encoding_failed", err.Error())
	}
	command := exec.Command(engine.Executable, "rpc")
	command.Stdin = bytes.NewReader(input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		message := stderr.String()
		if len(message) > 8192 {
			message = message[:8192]
		}
		if message == "" {
			message = err.Error()
		}
		return transportFailure("engine_process_failed", message)
	}
	var envelope struct {
		Outcome        string          `json:"outcome"`
		EngineProtocol string          `json:"engine_protocol"`
		Response       json.RawMessage `json:"response"`
		Error          *EngineFailure  `json:"error"`
	}
	if err := decodeClosedJSON(stdout.Bytes(), &envelope); err != nil {
		return transportFailure("invalid_engine_response", err.Error())
	}
	if envelope.EngineProtocol != EngineProtocolVersion {
		return EngineFailure{
			Category: "contract_violation", Phase: "transport",
			Code:     "unsupported_engine_protocol",
			Message:  fmt.Sprintf("expected %s, received %q", EngineProtocolVersion, envelope.EngineProtocol),
			Contract: EngineProtocolVersion, ContractSide: "schema", RetryDisposition: "never",
		}
	}
	switch envelope.Outcome {
	case "failure":
		if envelope.Error == nil {
			return transportFailure("invalid_engine_response", "failure response omitted error")
		}
		if err := envelope.Error.validate(); err != nil {
			return transportFailure("invalid_engine_response", err.Error())
		}
		return *envelope.Error
	case "success":
		if err := decodeClosedJSON(envelope.Response, response); err != nil {
			return transportFailure("invalid_engine_response", err.Error())
		}
		return nil
	default:
		return transportFailure("invalid_engine_response", "response outcome is not closed")
	}
}

func decodeClosedJSON(input []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func transportFailure(code, message string) EngineFailure {
	return EngineFailure{
		Category: "transport_failure", Phase: "transport", Code: code, Message: message,
	}
}

func unexpectedEngineResponse(expected, received string) EngineFailure {
	return EngineFailure{
		Category: "contract_violation", Phase: "transport", Code: "unexpected_engine_response",
		Message: fmt.Sprintf("expected %s, received %q", expected, received), RetryDisposition: "never",
	}
}
