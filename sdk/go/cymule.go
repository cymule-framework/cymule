// Package cymule provides Go authoring and Engine client APIs.
package cymule

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"time"
)

// EngineProtocolVersion is the frozen Engine transport contract.
const EngineProtocolVersion = "cymule.engine/2"

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
		"execute_durable", "execute_live_evolution",
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
	Reference string `json:"reference,omitempty"`
}

// ResourceManifestDescriptor authenticates one exact bounded-list manifest.
type ResourceManifestDescriptor struct {
	ManifestVersion string `json:"manifest_version"`
	MediaType       string `json:"media_type"`
	Digest          string `json:"digest"`
	Size            uint64 `json:"size"`
	EntryCount      uint64 `json:"entry_count"`
	RootDigest      string `json:"root_digest"`
}

// ResourceCandidate is sealed by the trusted Rust resource engine.
type ResourceCandidate struct {
	ResourceVersion string                      `json:"resource_version"`
	Shape           string                      `json:"shape"`
	MediaType       string                      `json:"media_type"`
	Inline          *InlineData                 `json:"inline,omitempty"`
	Integrity       ResourceIntegrity           `json:"integrity"`
	Manifest        *ResourceManifestDescriptor `json:"manifest,omitempty"`
	Annotations     map[string]string           `json:"annotations,omitempty"`
}

// ResourceHandle is a location-independent trusted resource descriptor.
type ResourceHandle struct {
	ResourceID string `json:"resource_id"`
	ResourceCandidate
}

// ResourceHandoff transfers one typed Resource Handle Artifact between durable Runs.
type ResourceHandoff struct {
	HandoffVersion string                     `json:"handoff_version"`
	TransferID     string                     `json:"transfer_id"`
	Producer       ResourceProducerProvenance `json:"producer"`
	ToRun          string                     `json:"to_run"`
	Slot           string                     `json:"slot"`
	Resource       ArtifactRef                `json:"resource"`
}

// ResourceProducerProvenance pins the exact producer occurrence and result.
type ResourceProducerProvenance struct {
	RunID        string      `json:"run_id"`
	OccurrenceID string      `json:"occurrence_id"`
	Result       ArtifactRef `json:"result"`
}

// ArtifactRef identifies immutable typed bytes in the semantic artifact store.
type ArtifactRef struct {
	IdentityVersion string `json:"identity_version"`
	ArtifactID      string `json:"artifact_id"`
	Kind            string `json:"kind"`
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
	MigrationID        string                `json:"migration_id"`
	RunID              string                `json:"run_id"`
	FromPlan           string                `json:"from_plan"`
	ToPlan             string                `json:"to_plan"`
	PlanEdgeID         string                `json:"plan_edge_id"`
	CompatibilityID    string                `json:"compatibility_id"`
	SafePointID        string                `json:"safe_point_id"`
	SourceEpoch        uint64                `json:"source_epoch"`
	SourceContinuation MigrationContinuation `json:"source_continuation"`
	InputState         ArtifactRef           `json:"input_state"`
	SourceBinding      ArtifactRef           `json:"source_binding"`
	TargetBinding      ArtifactRef           `json:"target_binding"`
}

// MigrationInvocationPathSegment identifies one exact dynamic invocation edge.
type MigrationInvocationPathSegment struct {
	SiteID     string   `json:"site_id"`
	RegionPath []uint64 `json:"region_path"`
	ScopeID    string   `json:"scope_id"`
	Epoch      uint64   `json:"epoch"`
}

// MigrationFrame is one mapped interpreter frame and target program counter.
type MigrationFrame struct {
	DefinitionID   string                           `json:"definition_id"`
	InvocationID   string                           `json:"invocation_id"`
	InvocationPath []MigrationInvocationPathSegment `json:"invocation_path"`
	ScopeID        string                           `json:"scope_id"`
	Input          ArtifactRef                      `json:"input"`
	RegionPath     []uint64                         `json:"region_path"`
	NextStep       uint64                           `json:"next_step"`
	Locals         map[string]ArtifactRef           `json:"locals"`
}

// MigrationContinuation is the complete interpreter state at a migration boundary.
type MigrationContinuation struct {
	RunID             string            `json:"run_id"`
	PlanID            string            `json:"plan_id"`
	BindingContext    string            `json:"binding_context"`
	Frames            []MigrationFrame  `json:"frames"`
	State             *ArtifactRef      `json:"state"`
	WaitSet           []string          `json:"wait_set"`
	ScopeStack        []string          `json:"scope_stack"`
	EffectObligations []string          `json:"effect_obligations"`
	AuthorityLeases   []string          `json:"authority_leases"`
	Budget            map[string]uint64 `json:"budget"`
	CausalFrontier    []string          `json:"causal_frontier"`
	Epoch             uint64            `json:"epoch"`
	Status            string            `json:"status"`
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

// UnmarshalJSON rejects unknown or overlapping live-evolution operations.
func (command *LiveEvolutionCommand) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("live evolution command is not an object")
	}
	operation, ok := object["operation"].(string)
	if !ok {
		return fmt.Errorf("live evolution command operation is missing")
	}
	if object["control_version"] != "cymule.live-evolution-control/3" {
		return fmt.Errorf("unsupported live evolution control version")
	}
	expected := map[string][][]string{
		"publish_definition": {{"control_version", "command_id", "operation", "logical_ref", "definition"}},
		"register_template":  {{"control_version", "command_id", "operation", "template"}},
		"publish_and_relink": {{"control_version", "command_id", "operation", "publication"}},
		"apply": {
			{"control_version", "command_id", "operation", "template_id", "command"},
			{"control_version", "command_id", "operation", "template_id", "command", "safe_point"},
		},
	}[operation]
	if expected == nil {
		return fmt.Errorf("unsupported live evolution operation %q", operation)
	}
	if !slices.ContainsFunc(expected, func(fields []string) bool {
		return requireExactJSONFields(object, fields) == nil
	}) {
		return fmt.Errorf("live evolution command fields are not closed")
	}
	type wire LiveEvolutionCommand
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	*command = LiveEvolutionCommand(decoded)
	return nil
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
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("evolution command is not an object")
	}
	operation, ok := object["operation"].(string)
	if !ok {
		return fmt.Errorf("evolution command operation is missing")
	}
	if object["control_version"] != "cymule.evolution-control/4" {
		return fmt.Errorf("unsupported evolution control version")
	}
	expectedFields, ok := map[string][]string{
		"apply_patch":            {"control_version", "command_id", "operation", "patch"},
		"set_rollout":            {"control_version", "command_id", "operation", "decision"},
		"select_occurrence":      {"control_version", "command_id", "operation", "occurrence_id"},
		"migrate":                {"control_version", "command_id", "operation", "request"},
		"restart_under_new_plan": {"control_version", "command_id", "operation", "request"},
		"shadow":                 {"control_version", "command_id", "operation", "request"},
		"observe":                {"control_version", "command_id", "operation", "observation"},
		"apply_gate":             {"control_version", "command_id", "operation", "gate", "next_decision_id"},
	}[operation]
	if !ok {
		return fmt.Errorf("unsupported evolution operation %q", operation)
	}
	if err := requireExactJSONFields(object, expectedFields); err != nil {
		return err
	}
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
	if err := decodeClosedValue(value, &wire); err != nil {
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
		if err := decodeClosedJSON(wire.Request, &request); err != nil {
			return err
		}
		if err := validateMigrationRequest(request); err != nil {
			return err
		}
		command.Migration = &request
	case "restart_under_new_plan":
		var request RestartRequest
		if err := decodeClosedJSON(wire.Request, &request); err != nil {
			return err
		}
		if err := validateRestartRequest(request); err != nil {
			return err
		}
		command.Restart = &request
	case "shadow":
		var request ShadowRequest
		if err := decodeClosedJSON(wire.Request, &request); err != nil {
			return err
		}
		if err := validateShadowRequest(request); err != nil {
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

// DurableResponse is the closed stateful M1 result union.
type DurableResponse struct {
	Type        string          `json:"type"`
	Boundary    json.RawMessage `json:"boundary,omitempty"`
	ReadyRunIDs []string        `json:"ready_run_ids,omitempty"`
	Run         json.RawMessage `json:"run,omitempty"`
	Domain      json.RawMessage `json:"domain,omitempty"`
}

func (response DurableResponse) validate() error {
	switch response.Type {
	case "run_boundary":
		if len(response.Boundary) == 0 || len(response.ReadyRunIDs) != 0 || len(response.Run) != 0 || len(response.Domain) != 0 {
			return fmt.Errorf("durable response fields are not closed")
		}
		if err := validateDurableBoundary(response.Boundary); err != nil {
			return err
		}
	case "wait_activated":
		if len(response.Boundary) != 0 || response.ReadyRunIDs == nil || len(response.Run) != 0 || len(response.Domain) != 0 {
			return fmt.Errorf("durable response fields are not closed")
		}
		for _, runID := range response.ReadyRunIDs {
			if runID == "" {
				return fmt.Errorf("ready Run identity is empty")
			}
		}
	case "run":
		if len(response.Boundary) != 0 || len(response.ReadyRunIDs) != 0 || len(response.Run) == 0 || len(response.Domain) != 0 {
			return fmt.Errorf("durable response fields are not closed")
		}
		if string(response.Run) != "null" {
			if err := validateDurableRun(response.Run); err != nil {
				return err
			}
		}
	case "domain":
		if len(response.Boundary) != 0 || len(response.ReadyRunIDs) != 0 || len(response.Run) != 0 || len(response.Domain) == 0 {
			return fmt.Errorf("durable response fields are not closed")
		}
		var domain struct {
			Revision *string  `json:"revision"`
			RunIDs   []string `json:"run_ids"`
		}
		if err := validateClosedRaw(response.Domain, []string{"revision", "run_ids"}, &domain); err != nil {
			return err
		}
	default:
		return fmt.Errorf("durable response variant is unknown")
	}
	return nil
}

// LiveEvolutionResponse is one closed durable live-evolution result.
type LiveEvolutionResponse struct {
	Result     string          `json:"result"`
	Revision   json.RawMessage `json:"revision,omitempty"`
	Linked     json.RawMessage `json:"linked,omitempty"`
	Receipt    json.RawMessage `json:"receipt,omitempty"`
	Edge       json.RawMessage `json:"edge,omitempty"`
	PlanID     string          `json:"plan_id,omitempty"`
	Comparison json.RawMessage `json:"comparison,omitempty"`
	Transition json.RawMessage `json:"transition,omitempty"`
}

func (response LiveEvolutionResponse) validate() error {
	present := func(value json.RawMessage) bool { return len(value) != 0 }
	valid := false
	switch response.Result {
	case "definition_published":
		valid = present(response.Revision)
	case "template_registered":
		valid = present(response.Linked)
	case "publication_applied", "migrated", "restart_authorized":
		valid = present(response.Receipt)
	case "patch_applied":
		valid = present(response.Edge)
	case "applied":
		valid = true
	case "occurrence_selected":
		valid = response.PlanID != ""
	case "shadow_recorded":
		valid = present(response.Comparison)
	case "gate_applied":
		valid = present(response.Transition)
	}
	count := 0
	for _, item := range []bool{present(response.Revision), present(response.Linked), present(response.Receipt), present(response.Edge), response.PlanID != "", present(response.Comparison), present(response.Transition)} {
		if item {
			count++
		}
	}
	if !valid || (response.Result == "applied" && count != 0) || (response.Result != "applied" && count != 1) {
		return fmt.Errorf("live-evolution response fields are not closed")
	}
	var raw json.RawMessage
	var fields []string
	switch response.Result {
	case "definition_published":
		raw, fields = response.Revision, []string{"revision_version", "revision_id", "logical_ref", "sequence", "definition", "references"}
	case "template_registered":
		raw, fields = response.Linked, []string{"template_id", "plan", "resolved_revisions"}
	case "publication_applied":
		raw, fields = response.Receipt, []string{"revision", "updates"}
	case "patch_applied":
		raw, fields = response.Edge, []string{"edge_id", "from_plan", "to_plan", "operations", "evidence"}
	case "migrated":
		raw, fields = response.Receipt, []string{"migration_id", "run_id", "from_plan", "to_plan", "safe_point_id", "source_epoch", "target_epoch", "source_binding", "target_binding", "adapter_id", "adapter_revision", "from_schema", "to_schema", "input_state", "output_state", "evidence"}
	case "restart_authorized":
		raw, fields = response.Receipt, []string{"request", "target_plan"}
	case "shadow_recorded":
		raw, fields = response.Comparison, []string{"comparison_id", "subject", "decision_id", "primary_plan", "shadow_plan", "driver_id", "driver_revision", "comparison_policy", "primary_digest", "shadow_digest", "equivalent", "evidence"}
	case "gate_applied":
		raw, fields = response.Transition, []string{"transition_id", "from_decision", "to_decision", "evaluation"}
	}
	if len(raw) != 0 {
		var object map[string]json.RawMessage
		if err := validateClosedRaw(raw, fields, &object); err != nil {
			return err
		}
		for _, name := range []string{"evidence", "source_binding", "target_binding", "input_state", "output_state"} {
			if artifact, ok := object[name]; ok {
				var reference ArtifactRef
				if err := validateClosedRaw(artifact, []string{"identity_version", "artifact_id", "kind"}, &reference); err != nil {
					return err
				}
				if err := validateArtifactRef(reference); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateDurableBoundary(raw json.RawMessage) error {
	var tag struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	fields := map[string][]string{
		"suspended": {"status", "wait_id"}, "reconciliation_required": {"status", "intent_id"},
		"release_required": {"status", "intent_ids"}, "completed": {"status", "result"},
	}[tag.Status]
	if fields == nil {
		return fmt.Errorf("durable boundary variant is unknown")
	}
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, fields, &object); err != nil {
		return err
	}
	if tag.Status == "completed" {
		var result struct {
			RunID             string          `json:"run_id"`
			PlanID            string          `json:"plan_id"`
			Value             json.RawMessage `json:"value"`
			ProjectionDigest  string          `json:"projection_digest"`
			PreconditionToken string          `json:"precondition_token"`
			Effects           []string        `json:"effects"`
		}
		return validateClosedRaw(object["result"], []string{"run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"}, &result)
	}
	return nil
}

func validateDurableRun(raw json.RawMessage) error {
	var run struct {
		Revision     string            `json:"revision"`
		Continuation json.RawMessage   `json:"continuation"`
		Waits        []json.RawMessage `json:"waits"`
		Effects      []json.RawMessage `json:"effects"`
		Result       json.RawMessage   `json:"result"`
	}
	if err := validateClosedRaw(raw, []string{"revision", "continuation", "waits", "effects", "result"}, &run); err != nil {
		return err
	}
	var continuation struct {
		RunID             string            `json:"run_id"`
		PlanID            string            `json:"plan_id"`
		BindingContext    string            `json:"binding_context"`
		Frames            []json.RawMessage `json:"frames"`
		State             json.RawMessage   `json:"state"`
		WaitSet           []string          `json:"wait_set"`
		ScopeStack        []string          `json:"scope_stack"`
		EffectObligations []string          `json:"effect_obligations"`
		AuthorityLeases   []string          `json:"authority_leases"`
		Budget            map[string]uint64 `json:"budget"`
		CausalFrontier    []string          `json:"causal_frontier"`
		Epoch             uint64            `json:"epoch"`
		Status            string            `json:"status"`
	}
	if err := validateClosedRaw(run.Continuation, []string{"run_id", "plan_id", "binding_context", "frames", "state", "wait_set", "scope_stack", "effect_obligations", "authority_leases", "budget", "causal_frontier", "epoch", "status"}, &continuation); err != nil {
		return err
	}
	if continuation.RunID == "" || continuation.PlanID == "" || continuation.BindingContext == "" {
		return fmt.Errorf("Continuation identities are missing")
	}
	for _, frame := range continuation.Frames {
		var object map[string]json.RawMessage
		if err := validateClosedRaw(frame, []string{"definition_id", "invocation_id", "invocation_path", "scope_id", "input", "region_path", "next_step", "locals"}, &object); err != nil {
			return err
		}
	}
	for _, wait := range run.Waits {
		var object map[string]json.RawMessage
		if err := validateClosedRaw(wait, []string{"wait_id", "run_id", "kind", "consume_once", "owner", "state", "result"}, &object); err != nil {
			return err
		}
	}
	for _, effect := range run.Effects {
		var object map[string]json.RawMessage
		if err := validateClosedRaw(effect, []string{"intent_id", "run_id", "operation", "input", "occurrence_binding", "state", "claim_epoch", "claim_owner", "result"}, &object); err != nil {
			return err
		}
	}
	return nil
}

func validateClosedRaw(raw json.RawMessage, fields []string, target any) error {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("JSON value is not an object")
	}
	if err := requireExactJSONFields(object, fields); err != nil {
		return err
	}
	return decodeClosedValue(value, target)
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
	PlanID            string            `json:"plan_id"`
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
	PlanID            string       `json:"plan_id"`
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
	OccurrenceRootDigest       string                   `json:"occurrence_root_digest"`
	UnresolvedObligations      []string                 `json:"unresolved_obligations"`
	RetainedOccurrenceBindings []string                 `json:"retained_occurrence_bindings"`
	ReplayAvailability         ReplayAvailability       `json:"replay_availability"`
	RehydrationManifest        ResourceHandle           `json:"rehydration_manifest"`
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
	PlanID            string   `json:"plan_id"`
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
		ControlVersion: "cymule.evolution-control/4",
		CommandID:      commandID,
		Operation:      operation,
	}
}

func liveEvolutionCommand(commandID, operation string) LiveEvolutionCommand {
	return LiveEvolutionCommand{
		ControlVersion: "cymule.live-evolution-control/3",
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
func ClaimVirtualWork(commandID, owner, slotID, planID, occurrenceBinding string, capabilities []string, logicalNow, leaseTTL uint64) VirtualClaimCommand {
	return VirtualClaimCommand{
		ControlVersion:    "cymule.virtual-claim-control/2",
		CommandID:         commandID,
		Owner:             owner,
		SlotID:            slotID,
		PlanID:            planID,
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
func NewResourceHandoff(transferID string, producer ResourceProducerProvenance, toRun, slot string, resource ArtifactRef) ResourceHandoff {
	return ResourceHandoff{
		HandoffVersion: "cymule.resource-handoff/3",
		TransferID:     transferID,
		Producer:       producer,
		ToRun:          toRun,
		Slot:           slot,
		Resource:       resource,
	}
}

// TextResource creates one inline UTF-8 Resource Candidate.
func TextResource(text string, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/2",
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
		ResourceVersion: "cymule.resource/2",
		Shape:           "inline",
		MediaType:       "application/json",
		Inline:          &InlineData{Encoding: "json", Value: value},
		Integrity:       ResourceIntegrity{Kind: "inline"},
		Annotations:     annotations,
	}
}

// ExternalResource creates a provider-neutral external Resource Candidate.
func ExternalResource(shape, mediaType string, integrity ResourceIntegrity, manifest *ResourceManifestDescriptor, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/2",
		Shape:           shape,
		MediaType:       mediaType,
		Integrity:       integrity,
		Manifest:        manifest,
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

// UnmarshalJSON rejects absent or null required Sealed Plan fields.
func (plan *SealedPlan) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("sealed Plan is not an object")
	}
	if err := requireExactJSONFields(object, []string{"plan_id", "candidate"}); err != nil {
		return err
	}
	type wire SealedPlan
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	if decoded.PlanID == "" || decoded.Candidate.IRVersion == "" || decoded.Candidate.Entry == "" {
		return fmt.Errorf("sealed Plan required fields are missing")
	}
	*plan = SealedPlan(decoded)
	return nil
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

// SuspensionBoundary is a typed wait boundary without a resumable Embedded continuation.
type SuspensionBoundary struct {
	RunID        string         `json:"run_id"`
	PlanID       string         `json:"plan_id"`
	DefinitionID string         `json:"definition_id"`
	InvocationID string         `json:"invocation_id"`
	SiteID       string         `json:"site_id"`
	Wait         map[string]any `json:"wait"`
	ResultBind   *string        `json:"result_bind"`
}

// EffectReleaseBoundary identifies exact explicit Effects requiring caller release.
type EffectReleaseBoundary struct {
	RunID     string   `json:"run_id"`
	PlanID    string   `json:"plan_id"`
	IntentIDs []string `json:"intent_ids"`
}

// EffectReconciliationBoundary identifies one ambiguous Effect requiring reconciliation.
type EffectReconciliationBoundary struct {
	RunID    string `json:"run_id"`
	PlanID   string `json:"plan_id"`
	IntentID string `json:"intent_id"`
}

// ExecutionOutcome is the closed Embedded execution boundary.
type ExecutionOutcome struct {
	Status         string                        `json:"status"`
	Result         *ExecutionResult              `json:"result,omitempty"`
	Suspension     *SuspensionBoundary           `json:"suspension,omitempty"`
	Release        *EffectReleaseBoundary        `json:"release,omitempty"`
	Reconciliation *EffectReconciliationBoundary `json:"reconciliation,omitempty"`
}

// UnmarshalJSON rejects unknown or overlapping Embedded execution variants.
func (outcome *ExecutionOutcome) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("execution outcome is not an object")
	}
	status, ok := object["status"].(string)
	if !ok {
		return fmt.Errorf("execution outcome status is missing")
	}
	expected, ok := map[string][]string{
		"completed":               {"status", "result"},
		"suspended":               {"status", "suspension"},
		"release_required":        {"status", "release"},
		"reconciliation_required": {"status", "reconciliation"},
	}[status]
	if !ok {
		return fmt.Errorf("unsupported execution outcome %q", status)
	}
	if err := requireExactJSONFields(object, expected); err != nil {
		return err
	}
	type wire ExecutionOutcome
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	validPayload := (status == "completed" && decoded.Result != nil) ||
		(status == "suspended" && decoded.Suspension != nil) ||
		(status == "release_required" && decoded.Release != nil) ||
		(status == "reconciliation_required" && decoded.Reconciliation != nil)
	if !validPayload {
		return fmt.Errorf("execution outcome payload is null")
	}
	if err := validateExecutionPayload(status, decoded); err != nil {
		return err
	}
	*outcome = ExecutionOutcome(decoded)
	return nil
}

func validateExecutionPayload(status string, outcome struct {
	Status         string                        `json:"status"`
	Result         *ExecutionResult              `json:"result,omitempty"`
	Suspension     *SuspensionBoundary           `json:"suspension,omitempty"`
	Release        *EffectReleaseBoundary        `json:"release,omitempty"`
	Reconciliation *EffectReconciliationBoundary `json:"reconciliation,omitempty"`
}) error {
	switch status {
	case "completed":
		result := outcome.Result
		if result.RunID == "" || result.PlanID == "" || result.ProjectionDigest == "" ||
			result.PreconditionToken == "" || result.Effects == nil {
			return fmt.Errorf("completed execution required fields are missing")
		}
		for _, effect := range result.Effects {
			if effect == "" {
				return fmt.Errorf("completed execution effect identity is empty")
			}
		}
	case "suspended":
		boundary := outcome.Suspension
		if boundary.RunID == "" || boundary.PlanID == "" || boundary.DefinitionID == "" ||
			boundary.InvocationID == "" || boundary.SiteID == "" {
			return fmt.Errorf("suspension required fields are missing")
		}
		if err := validateWaitSpec(boundary.Wait); err != nil {
			return err
		}
		if boundary.ResultBind != nil && *boundary.ResultBind == "" {
			return fmt.Errorf("suspension result binding is empty")
		}
	case "release_required":
		release := outcome.Release
		if release.RunID == "" || release.PlanID == "" || len(release.IntentIDs) == 0 {
			return fmt.Errorf("effect release required fields are missing")
		}
		for _, intent := range release.IntentIDs {
			if intent == "" {
				return fmt.Errorf("effect release intent identity is empty")
			}
		}
	case "reconciliation_required":
		reconciliation := outcome.Reconciliation
		if reconciliation.RunID == "" || reconciliation.PlanID == "" || reconciliation.IntentID == "" {
			return fmt.Errorf("effect reconciliation required fields are missing")
		}
	}
	return nil
}

func validateWaitSpec(wait map[string]any) error {
	kind, ok := wait["kind"].(string)
	if !ok {
		return fmt.Errorf("wait contract kind is missing")
	}
	var fields []string
	switch kind {
	case "signal":
		fields = []string{"kind", "key", "consume_once"}
		if key, ok := wait["key"].(string); !ok || key == "" {
			return fmt.Errorf("signal wait key is invalid")
		}
		if _, ok := wait["consume_once"].(bool); !ok {
			return fmt.Errorf("signal wait consume_once is invalid")
		}
	case "timer":
		fields = []string{"kind", "timer_id"}
		if timerID, ok := wait["timer_id"].(string); !ok || timerID == "" {
			return fmt.Errorf("timer wait identity is invalid")
		}
	case "input":
		fields = []string{"kind", "correlation", "schema"}
		if correlation, ok := wait["correlation"].(string); !ok || correlation == "" {
			return fmt.Errorf("input wait correlation is invalid")
		}
		switch wait["schema"].(type) {
		case map[string]any, bool:
		default:
			return fmt.Errorf("input wait schema is invalid")
		}
	default:
		return fmt.Errorf("unsupported wait contract %q", kind)
	}
	return requireExactJSONFields(wait, fields)
}

func validateMigrationRequest(request MigrationRequest) error {
	if request.MigrationID == "" || request.RunID == "" || request.FromPlan == "" ||
		request.ToPlan == "" || request.PlanEdgeID == "" || request.CompatibilityID == "" ||
		request.SafePointID == "" {
		return fmt.Errorf("migration request required fields are missing")
	}
	for _, reference := range []ArtifactRef{request.InputState, request.SourceBinding, request.TargetBinding} {
		if err := validateArtifactRef(reference); err != nil {
			return err
		}
	}
	if err := validateMigrationContinuation(request.SourceContinuation); err != nil {
		return err
	}
	return nil
}

func validateMigrationContinuation(continuation MigrationContinuation) error {
	if continuation.RunID == "" || continuation.PlanID == "" || continuation.BindingContext == "" ||
		len(continuation.Frames) == 0 || len(continuation.ScopeStack) == 0 {
		return fmt.Errorf("migration Continuation required fields are missing")
	}
	switch continuation.Status {
	case "ready", "waiting", "running", "completed":
	default:
		return fmt.Errorf("migration Continuation status is invalid")
	}
	if continuation.State != nil {
		if err := validateArtifactRef(*continuation.State); err != nil {
			return err
		}
	}
	for _, frame := range continuation.Frames {
		if frame.DefinitionID == "" || frame.InvocationID == "" || frame.ScopeID == "" {
			return fmt.Errorf("migration frame required fields are missing")
		}
		if err := validateArtifactRef(frame.Input); err != nil {
			return err
		}
		for _, reference := range frame.Locals {
			if err := validateArtifactRef(reference); err != nil {
				return err
			}
		}
		for _, segment := range frame.InvocationPath {
			if segment.SiteID == "" || segment.ScopeID == "" {
				return fmt.Errorf("migration invocation segment required fields are missing")
			}
		}
	}
	return nil
}

func validateRestartRequest(request RestartRequest) error {
	if request.RestartID == "" || request.SourceRun == "" || request.ReplacementRun == "" ||
		request.FromPlan == "" || request.ToPlan == "" || request.SafePointID == "" {
		return fmt.Errorf("restart request required fields are missing")
	}
	for _, reference := range []ArtifactRef{request.Input, request.Evidence} {
		if err := validateArtifactRef(reference); err != nil {
			return err
		}
	}
	return nil
}

func validateShadowRequest(request ShadowRequest) error {
	if request.ComparisonID == "" || request.DecisionID == "" || request.Subject == "" ||
		request.PrimaryPlan == "" || request.ShadowPlan == "" || request.ComparisonPolicy == "" {
		return fmt.Errorf("shadow request required fields are missing")
	}
	return validateArtifactRef(request.Input)
}

func validateArtifactRef(reference ArtifactRef) error {
	if reference.IdentityVersion != "cymule.artifact/2" || reference.Kind == "" ||
		len(reference.ArtifactID) != len("sha256:")+64 || !strings.HasPrefix(reference.ArtifactID, "sha256:") {
		return fmt.Errorf("Artifact reference identity is invalid")
	}
	for _, character := range reference.ArtifactID[len("sha256:"):] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return fmt.Errorf("Artifact reference identity is invalid")
		}
	}
	return nil
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
func (builder *FlowBuilder) Component(id string, inputSchema, outputSchema map[string]any, requirements map[string]string) *FlowBuilder {
	builder.candidate.Components = append(builder.candidate.Components, Contract{
		ID: id, InputSchema: inputSchema, OutputSchema: outputSchema, Requirements: cloneStrings(requirements),
	})
	return builder
}

// EffectContract declares an abstract world effect.
func (builder *FlowBuilder) EffectContract(id string, inputSchema, outputSchema map[string]any, profile EffectProfile, requirements map[string]string) *FlowBuilder {
	builder.candidate.Effects = append(builder.candidate.Effects, EffectContract{
		ID: id, InputSchema: inputSchema, OutputSchema: outputSchema, Profile: profile, Requirements: cloneStrings(requirements),
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
func (builder *FlowBuilder) Wait(site string, wait map[string]any, bind ...string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	step := Step{"id": site, "op": "wait", "wait": wait}
	if len(bind) > 0 {
		step["bind"] = bind[0]
	}
	entry.Body.Steps = append(entry.Body.Steps, step)
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
	encoded, err := json.Marshal(builder.candidate)
	if err != nil {
		panic(fmt.Sprintf("Flow candidate is not JSON: %v", err))
	}
	var frozen PlanCandidate
	if err := decodeClosedJSON(encoded, &frozen); err != nil {
		panic(fmt.Sprintf("Flow candidate cannot be frozen: %v", err))
	}
	return frozen
}

func cloneStrings(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// CliEngine invokes the trusted Rust command-line Engine.
type CliEngine struct {
	Executable string
	Context    context.Context
	Timeout    time.Duration
}

// EngineStoreTarget selects one provider-owned durable domain.
type EngineStoreTarget struct {
	Provider string `json:"provider"`
	Location string `json:"location"`
	Domain   string `json:"domain,omitempty"`
}

// EnginePluginTarget selects one implementation, optionally by exact revision.
type EnginePluginTarget struct {
	Provider string `json:"provider"`
	Location string `json:"location"`
	Revision string `json:"revision,omitempty"`
}

// EngineDurableTarget separates durable storage from optional execution authority.
type EngineDurableTarget struct {
	Store    EngineStoreTarget   `json:"store"`
	Executor *EnginePluginTarget `json:"executor,omitempty"`
}

// EngineEvolutionTarget carries only the exact plugins needed by M4 operations.
type EngineEvolutionTarget struct {
	Store     EngineStoreTarget   `json:"store"`
	Migration *EnginePluginTarget `json:"migration,omitempty"`
	Shadow    *EnginePluginTarget `json:"shadow,omitempty"`
}

// DirectoryStore selects the official directory store.
func DirectoryStore(location string) EngineStoreTarget {
	return EngineStoreTarget{Provider: "cymule.directory-store/2", Location: location}
}

// SQLiteStore selects one domain in the official SQLite store.
func SQLiteStore(location, domain string) EngineStoreTarget {
	return EngineStoreTarget{Provider: "cymule.sqlite-store/2", Location: location, Domain: domain}
}

// ProcessPlugin selects the official sealed process provider.
func ProcessPlugin(location, revision string) EnginePluginTarget {
	return EnginePluginTarget{Provider: "cymule.executor-process/1", Location: location, Revision: revision}
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

// ExecuteDurable submits one stateful command to a durable Rust domain.
func (engine CliEngine) ExecuteDurable(target EngineDurableTarget, command DurableCommand) (DurableResponse, error) {
	var response struct {
		Type     string          `json:"type"`
		Response DurableResponse `json:"response"`
	}
	err := engine.request(map[string]any{
		"type": "execute_durable", "target": target, "command": command,
	}, &response)
	if err == nil && response.Type != "durable_executed" {
		err = unexpectedEngineResponse("durable_executed", response.Type)
	} else if err == nil {
		if validation := response.Response.validate(); validation != nil {
			err = transportFailure("invalid_engine_response", validation.Error())
		}
	}
	return response.Response, err
}

// ExecuteLiveEvolution submits one atomic command to durable evolution authority.
func (engine CliEngine) ExecuteLiveEvolution(
	target EngineEvolutionTarget, journalID string,
	command LiveEvolutionCommand,
) (LiveEvolutionResponse, error) {
	var response struct {
		Type     string                `json:"type"`
		Response LiveEvolutionResponse `json:"response"`
	}
	err := engine.request(map[string]any{
		"type": "execute_live_evolution", "target": target,
		"journal_id": journalID, "command": command,
	}, &response)
	if err == nil && response.Type != "live_evolution_executed" {
		err = unexpectedEngineResponse("live_evolution_executed", response.Type)
	} else if err == nil {
		if validation := response.Response.validate(); validation != nil {
			err = transportFailure("invalid_engine_response", validation.Error())
		}
	}
	return response.Response, err
}

// DurableEngine is the high-level provider-neutral durable Run client.
type DurableEngine struct {
	Store            EngineStoreTarget
	Executor         *EnginePluginTarget
	Migration        *EnginePluginTarget
	Shadow           *EnginePluginTarget
	Transport        CliEngine
	EvolutionJournal string
}

// Start creates or idempotently reopens one Run.
func (engine DurableEngine) Start(runID string, candidate PlanCandidate, input any) (DurableResponse, error) {
	command, err := StartDurableRun(runID, candidate, input)
	if err != nil {
		return DurableResponse{}, err
	}
	return engine.submit(command)
}

// Get reads one Run without reducing durable state.
func (engine DurableEngine) Get(runID string) (json.RawMessage, error) {
	response, err := engine.submit(QueryDurableRun("sdk:get:"+runID, runID))
	if err == nil && response.Type != "run" {
		err = unexpectedEngineResponse("run", response.Type)
	}
	return response.Run, err
}

// Resume advances one ready Run to its next boundary.
func (engine DurableEngine) Resume(runID string) (DurableResponse, error) {
	return engine.submit(ResumeDurableRun(runID))
}

// Signal admits one identified signal delivery.
func (engine DurableEngine) Signal(
	activationID, key string,
	waitIDs []string,
	value any,
) (DurableResponse, error) {
	command, err := ActivateDurableSignal(activationID, key, waitIDs, value)
	if err != nil {
		return DurableResponse{}, err
	}
	return engine.submit(command)
}

// Release releases one explicit effect intent.
func (engine DurableEngine) Release(intentID string) (DurableResponse, error) {
	return engine.submit(ReleaseDurableEffect(intentID))
}

// Evolve applies one atomic command to the same durable domain.
func (engine DurableEngine) Evolve(command LiveEvolutionCommand) (LiveEvolutionResponse, error) {
	journal := engine.EvolutionJournal
	if journal == "" {
		journal = "cymule.sdk.live-evolution"
	}
	return engine.Transport.ExecuteLiveEvolution(EngineEvolutionTarget{
		Store: engine.Store, Migration: engine.Migration, Shadow: engine.Shadow,
	}, journal, command)
}

func (engine DurableEngine) submit(command DurableCommand) (DurableResponse, error) {
	target := EngineDurableTarget{Store: engine.Store}
	if command.Type != "query_run" && command.Type != "query_domain" {
		target.Executor = engine.Executor
	}
	return engine.Transport.ExecuteDurable(target, command)
}

// Run executes a sealed plan through one plugin realization.
func (engine CliEngine) Run(plan SealedPlan, input any, plugin, runID string) (ExecutionOutcome, error) {
	var response struct {
		Type      string           `json:"type"`
		Execution ExecutionOutcome `json:"execution"`
	}
	err := engine.request(map[string]any{
		"type": "run", "plan": plan, "input": input, "plugin": plugin, "run_id": runID,
	}, &response)
	if err == nil && response.Type != "execution_boundary" {
		err = unexpectedEngineResponse("execution_boundary", response.Type)
	}
	return response.Execution, err
}

func (engine CliEngine) request(request any, response any) error {
	input, err := json.Marshal(struct {
		EngineProtocol string `json:"engine_protocol"`
		Request        any    `json:"request"`
	}{EngineProtocolVersion, request})
	if err != nil {
		return transportFailure("request_encoding_failed", err.Error())
	}
	if _, err := decodeUniqueJSON(input); err != nil {
		return transportFailure("request_encoding_failed", err.Error())
	}
	executable := engine.Executable
	if executable == "" {
		executable = "cymule"
	}
	ctx := engine.Context
	if ctx == nil {
		ctx = context.Background()
	}
	var cancel context.CancelFunc = func() {}
	if engine.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, engine.Timeout)
	}
	defer cancel()
	command := exec.CommandContext(ctx, executable, "rpc")
	command.Stdin = bytes.NewReader(input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return interruptedFailure(request, ctx.Err())
		}
		return transportFailure("engine_process_failed", "engine exited without a protocol response")
	}
	return decodeEngineResponse(stdout.Bytes(), response)
}

func decodeEngineResponse(input []byte, response any) error {
	var envelope struct {
		Outcome        string           `json:"outcome"`
		EngineProtocol string           `json:"engine_protocol"`
		Response       *json.RawMessage `json:"response"`
		Error          *EngineFailure   `json:"error"`
	}
	if err := decodeClosedJSON(input, &envelope); err != nil {
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
		if envelope.Error == nil || envelope.Response != nil {
			return transportFailure("invalid_engine_response", "failure response must contain only error")
		}
		if err := envelope.Error.validate(); err != nil {
			return transportFailure("invalid_engine_response", err.Error())
		}
		return *envelope.Error
	case "success":
		if envelope.Response == nil || envelope.Error != nil {
			return transportFailure("invalid_engine_response", "success response must contain only response")
		}
		if err := decodeClosedJSON(*envelope.Response, response); err != nil {
			return transportFailure("invalid_engine_response", err.Error())
		}
		return nil
	default:
		return transportFailure("invalid_engine_response", "response outcome is not closed")
	}
}

func decodeClosedJSON(input []byte, target any) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	return decodeClosedValue(value, target)
}

func decodeClosedValue(value any, target any) error {
	normalized, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(normalized))
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

func requireExactJSONFields(object map[string]any, expected []string) error {
	if len(object) != len(expected) {
		return fmt.Errorf("JSON object fields are not closed")
	}
	for _, field := range expected {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("JSON object omitted required field %q", field)
		}
	}
	return nil
}

func decodeUniqueJSON(input []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	value, err := readUniqueJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing JSON value")
		}
		return nil, err
	}
	return value, nil
}

func readUniqueJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		if number, ok := token.(json.Number); ok {
			if err := validateSharedJSONNumber(number); err != nil {
				return nil, err
			}
		}
		return token, nil
	}
	switch delimiter {
	case '{':
		members := make(map[string]any)
		for decoder.More() {
			memberToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			member, ok := memberToken.(string)
			if !ok {
				return nil, fmt.Errorf("JSON object member name is not a string")
			}
			if _, exists := members[member]; exists {
				return nil, fmt.Errorf("duplicate JSON object member %q", member)
			}
			value, err := readUniqueJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			members[member] = value
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim('}') {
			return nil, fmt.Errorf("JSON object did not close")
		}
		return members, nil
	case '[':
		values := make([]any, 0)
		for decoder.More() {
			value, err := readUniqueJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim(']') {
			return nil, fmt.Errorf("JSON array did not close")
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func validateSharedJSONNumber(value json.Number) error {
	text := value.String()
	if !strings.ContainsAny(text, ".eE") {
		integer, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return fmt.Errorf("invalid JSON integer")
		}
		limit := big.NewInt(9_007_199_254_740_991)
		if new(big.Int).Abs(integer).Cmp(limit) > 0 {
			return fmt.Errorf("integer is outside the shared JSON domain")
		}
		return nil
	}
	floating, err := value.Float64()
	if err != nil {
		return err
	}
	if math.Trunc(floating) == floating && math.Abs(floating) > 9_007_199_254_740_991 {
		return fmt.Errorf("number is outside the shared JSON domain")
	}
	return nil
}

func interruptedFailure(request any, cause error) EngineFailure {
	kind := "cancelled"
	if cause == context.DeadlineExceeded {
		kind = "timed_out"
	}
	mutating := false
	if object, ok := request.(map[string]any); ok {
		typeName, _ := object["type"].(string)
		mutating = typeName == "run" || typeName == "execute_live_evolution"
		if typeName == "execute_durable" {
			if command, ok := object["command"].(DurableCommand); ok {
				mutating = !strings.HasPrefix(command.Type, "query_")
			}
		}
	}
	if mutating {
		return EngineFailure{
			Category: "unknown_world_outcome", Phase: "transport",
			Code:             "engine_response_" + kind,
			Message:          "the Engine response was " + kind + " after a mutating request began",
			RetryDisposition: "reconcile",
		}
	}
	return EngineFailure{
		Category: kind, Phase: "transport", Code: "engine_response_" + kind,
		Message: "the Engine response was " + kind,
	}
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
