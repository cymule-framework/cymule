// Package cymule provides Go authoring and Engine client APIs.
package cymule

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

// EngineProtocolVersion is the frozen Engine transport contract.
const EngineProtocolVersion = "cymule.engine/5"
const maxExactInteger uint64 = 9_007_199_254_740_991
const maxEngineOutputBytes = 16 * 1024 * 1024
const maxEngineRequestBytes = 64 * 1024 * 1024
const maxInlineResourceBytes = 1024 * 1024
const maxArtifactBytes = 8 * 1024 * 1024
const maxArtifactBase64Bytes = ((maxArtifactBytes + 2) / 3) * 4
const ordinaryPluginMessageBytes uint64 = 8 * 1024 * 1024
const evolutionPluginMessageBytes uint64 = 16 * 1024 * 1024
const defaultEngineTimeout = 30 * time.Second
const engineTerminationGrace = 200 * time.Millisecond
const engineGroupExitLimit = 2 * time.Second
const emptyResourceManifestRoot = "sha256:6a754fadbb296b87040c37dab30caea63de1bd1a85142bc82a03a7cf82e64dfc"

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
		!validEngineCode(failure.Code) || !validNonControlUnicodeScalarLength(failure.Message, 1, 8192) {
		return fmt.Errorf("Engine failure fields are invalid")
	}
	if failure.Contract != "" && !validNonControlUnicodeScalarLength(failure.Contract, 1, 500) {
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
		if !validNonControlUnicodeScalarLength(issue.Code, 1, 200) ||
			!validNonControlUnicodeScalarLength(issue.Message, 1, 2000) || !validEnginePath(issue.Path) ||
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
	if !validEngineRetryMatrix(failure.Category, failure.RetryDisposition) {
		return fmt.Errorf("Engine failure category and retry disposition are incompatible")
	}
	return nil
}

func validEngineRetryMatrix(category, disposition string) bool {
	switch category {
	case "transport_failure", "not_found":
		return disposition == ""
	case "validation":
		return disposition == "correct_and_retry"
	case "contract_violation", "admission_denied":
		return disposition == "correct_and_retry" || disposition == "never"
	case "conflict":
		return disposition == "refresh_and_retry" || disposition == "never"
	case "expected_plugin_failure", "plugin_defect", "cancelled":
		return disposition == "never"
	case "substrate_failure":
		return disposition == "retry_same_request"
	case "timed_out":
		return disposition == "retry_same_request" || disposition == "refresh_and_retry"
	case "unknown_world_outcome":
		return disposition == "reconcile"
	default:
		return false
	}
}

func validateEngineFailureWire(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Engine failure is not an object")
	}
	if err := requireRequiredAllowedJSONFields(
		object,
		[]string{"category", "phase", "code", "message"},
		[]string{"category", "phase", "code", "message", "contract", "contract_side", "path", "issues", "retry_disposition"},
	); err != nil {
		return err
	}
	for _, field := range []string{"category", "phase", "code", "message", "contract", "contract_side", "path", "retry_disposition"} {
		if member, exists := object[field]; exists {
			if _, ok := member.(string); !ok {
				return fmt.Errorf("Engine failure field %q is not a string", field)
			}
		}
	}
	if contract, exists := object["contract"].(string); exists && contract == "" {
		return fmt.Errorf("Engine failure contract is empty")
	}
	if side, exists := object["contract_side"].(string); exists &&
		!slices.Contains([]string{"schema", "input", "output"}, side) {
		return fmt.Errorf("Engine failure contract side is unknown")
	}
	if disposition, exists := object["retry_disposition"].(string); exists &&
		!slices.Contains([]string{"never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile"}, disposition) {
		return fmt.Errorf("Engine failure retry disposition is unknown")
	}
	issuesValue, exists := object["issues"]
	if !exists {
		return nil
	}
	issues, ok := issuesValue.([]any)
	if !ok {
		return fmt.Errorf("Engine failure issues are not an array")
	}
	for _, issueValue := range issues {
		issue, ok := issueValue.(map[string]any)
		if !ok {
			return fmt.Errorf("Engine issue is not an object")
		}
		if err := requireRequiredAllowedJSONFields(
			issue,
			[]string{"code", "message"},
			[]string{"code", "message", "path", "schema_path"},
		); err != nil {
			return err
		}
		for _, field := range []string{"code", "message", "path", "schema_path"} {
			if member, exists := issue[field]; exists {
				if _, ok := member.(string); !ok {
					return fmt.Errorf("Engine issue field %q is not a string", field)
				}
			}
		}
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
		"seal_resource", "verify_wait_activation", "verify_durable_command", "observe_clock",
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
	return validNonControlUnicodeScalarLength(value, 0, 1000) && (value == "" || value[0] == '/')
}

func validNonControlUnicodeScalarLength(value string, minimum, maximum int) bool {
	return validUnicodeScalarLength(value, minimum, maximum) &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func validUnicodeScalarLength(value string, minimum, maximum int) bool {
	if !utf8.ValidString(value) {
		return false
	}
	length := utf8.RuneCountInString(value)
	return length >= minimum && length <= maximum
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
		if err := validateGoJSONStrings(reflect.ValueOf(data.Value)); err != nil {
			return nil, err
		}
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
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("inline Resource data is not an object")
	}
	encoding, ok := object["encoding"].(string)
	if !ok {
		return fmt.Errorf("inline Resource encoding is missing")
	}
	switch encoding {
	case "utf8":
		if err := requireExactJSONFields(object, []string{"encoding", "text"}); err != nil {
			return err
		}
		text, ok := object["text"].(string)
		if !ok {
			return fmt.Errorf("inline UTF-8 Resource text is invalid")
		}
		*data = InlineData{Encoding: encoding, Text: text}
	case "json":
		if err := requireExactJSONFields(object, []string{"encoding", "value"}); err != nil {
			return err
		}
		*data = InlineData{Encoding: encoding, Value: object["value"]}
	case "base64":
		if err := requireExactJSONFields(object, []string{"encoding", "data"}); err != nil {
			return err
		}
		encoded, ok := object["data"].(string)
		if !ok {
			return fmt.Errorf("inline base64 Resource data is invalid")
		}
		*data = InlineData{Encoding: encoding, Data: encoded}
	default:
		return fmt.Errorf("unsupported inline encoding %q", encoding)
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

// MarshalJSON emits the exact integrity variant, including a required zero content size.
func (integrity ResourceIntegrity) MarshalJSON() ([]byte, error) {
	switch integrity.Kind {
	case "inline":
		if integrity.Digest != "" || integrity.Size != 0 || integrity.Authority != "" ||
			integrity.Version != "" || integrity.Identity != "" {
			return nil, fmt.Errorf("inline Resource integrity carries variant fields")
		}
		return json.Marshal(struct {
			Kind string `json:"kind"`
		}{Kind: integrity.Kind})
	case "content":
		if integrity.Authority != "" || integrity.Version != "" || integrity.Identity != "" {
			return nil, fmt.Errorf("content Resource integrity carries variant fields")
		}
		return json.Marshal(struct {
			Kind   string `json:"kind"`
			Digest string `json:"digest"`
			Size   uint64 `json:"size"`
		}{Kind: integrity.Kind, Digest: integrity.Digest, Size: integrity.Size})
	case "version":
		if integrity.Digest != "" || integrity.Size != 0 || integrity.Identity != "" {
			return nil, fmt.Errorf("version Resource integrity carries variant fields")
		}
		return json.Marshal(struct {
			Kind      string `json:"kind"`
			Authority string `json:"authority"`
			Version   string `json:"version"`
		}{Kind: integrity.Kind, Authority: integrity.Authority, Version: integrity.Version})
	case "live":
		if integrity.Digest != "" || integrity.Size != 0 || integrity.Authority != "" || integrity.Version != "" {
			return nil, fmt.Errorf("live Resource integrity carries variant fields")
		}
		return json.Marshal(struct {
			Kind     string `json:"kind"`
			Identity string `json:"identity"`
		}{Kind: integrity.Kind, Identity: integrity.Identity})
	default:
		return nil, fmt.Errorf("Resource integrity kind is invalid")
	}
}

// UnmarshalJSON rejects cross-variant and omission-erasing integrity shapes.
func (integrity *ResourceIntegrity) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Resource integrity is not an object")
	}
	kind, ok := object["kind"].(string)
	if !ok {
		return fmt.Errorf("Resource integrity kind is missing")
	}
	expected := map[string][]string{
		"inline":  {"kind"},
		"content": {"kind", "digest", "size"},
		"version": {"kind", "authority", "version"},
		"live":    {"kind", "identity"},
	}[kind]
	if expected == nil {
		return fmt.Errorf("Resource integrity kind is invalid")
	}
	if err := requireExactJSONFields(object, expected); err != nil {
		return err
	}
	decoded := ResourceIntegrity{Kind: kind}
	switch kind {
	case "content":
		digest, digestOK := object["digest"].(string)
		size, sizeOK := object["size"].(json.Number)
		if !digestOK || !sizeOK {
			return fmt.Errorf("content Resource integrity is invalid")
		}
		parsed, err := parseSafeJSONUint(size, false)
		if err != nil {
			return fmt.Errorf("content Resource size is invalid")
		}
		decoded.Digest = digest
		decoded.Size = parsed
	case "version":
		authority, authorityOK := object["authority"].(string)
		version, versionOK := object["version"].(string)
		if !authorityOK || !versionOK {
			return fmt.Errorf("version Resource integrity is invalid")
		}
		decoded.Authority = authority
		decoded.Version = version
	case "live":
		identity, ok := object["identity"].(string)
		if !ok {
			return fmt.Errorf("live Resource integrity is invalid")
		}
		decoded.Identity = identity
	}
	*integrity = decoded
	return nil
}

// ResourceLocation is a non-authoritative realization hint.
type ResourceLocation struct {
	Kind      string `json:"kind"`
	URL       string `json:"url,omitempty"`
	Reference string `json:"reference,omitempty"`
}

// ResourceManifestDescriptor authenticates one exact bounded-list manifest.
// Digest is the domain-separated identity of MediaType, Size, EntryCount, and
// the canonical-entry Merkle RootDigest; it is not an independent raw-byte SHA.
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

// ResourceHandoffActivation binds one transfer to an exact target Wait and result.
type ResourceHandoffActivation struct {
	ActivationVersion string      `json:"activation_version"`
	ActivationID      string      `json:"activation_id"`
	TransferID        string      `json:"transfer_id"`
	ToRun             string      `json:"to_run"`
	WaitID            string      `json:"wait_id"`
	Result            ArtifactRef `json:"result"`
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

// ArtifactRecord carries one immutable Artifact reference and its exact self-verifying bytes.
type ArtifactRecord struct {
	Reference ArtifactRef `json:"reference"`
	Bytes     []byte      `json:"bytes"`
}

// MarshalJSON emits the sole strict padded-Base64 Artifact wire.
func (record ArtifactRecord) MarshalJSON() ([]byte, error) {
	if err := validateArtifactRecord(record); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Reference ArtifactRef `json:"reference"`
		Bytes     string      `json:"bytes"`
	}{Reference: record.Reference, Bytes: base64.StdEncoding.EncodeToString(record.Bytes)})
}

// UnmarshalJSON admits only the closed bounded canonical padded-Base64 Artifact wire.
func (record *ArtifactRecord) UnmarshalJSON(data []byte) error {
	value, err := decodeUniqueJSON(data)
	if err != nil {
		return err
	}
	members, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Artifact record is not an object")
	}
	if err := requireExactJSONFields(members, []string{"reference", "bytes"}); err != nil {
		return err
	}
	encoded, ok := members["bytes"].(string)
	if !ok || len(encoded) > maxArtifactBase64Bytes {
		return fmt.Errorf("Artifact record must contain exactly reference and bytes")
	}
	var reference ArtifactRef
	if err := decodeClosedValue(members["reference"], &reference); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	if len(decoded) > maxArtifactBytes || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return fmt.Errorf("Artifact record bytes are not canonical padded Base64")
	}
	candidate := ArtifactRecord{Reference: reference, Bytes: decoded}
	if err := validateArtifactRecord(candidate); err != nil {
		return err
	}
	*record = candidate
	return nil
}

// RolloutMode selects shadow, canary, active, or rolled-back future work.
type RolloutMode struct {
	Mode        string `json:"mode"`
	BasisPoints uint16 `json:"basis_points,omitempty"`
}

// MarshalJSON emits the exact Rust tagged-union shape, including canary zero.
func (mode RolloutMode) MarshalJSON() ([]byte, error) {
	switch mode.Mode {
	case "canary":
		return json.Marshal(struct {
			Mode        string `json:"mode"`
			BasisPoints uint16 `json:"basis_points"`
		}{Mode: mode.Mode, BasisPoints: mode.BasisPoints})
	case "shadow", "active", "rolled_back":
		if mode.BasisPoints != 0 {
			return nil, fmt.Errorf("non-canary rollout mode carries basis points")
		}
		return json.Marshal(struct {
			Mode string `json:"mode"`
		}{Mode: mode.Mode})
	default:
		return nil, fmt.Errorf("rollout mode is invalid")
	}
}

// UnmarshalJSON rejects missing canary basis points and unit-variant extras.
func (mode *RolloutMode) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("rollout mode is not an object")
	}
	kind, ok := object["mode"].(string)
	if !ok {
		return fmt.Errorf("rollout mode tag is missing")
	}
	expected := []string{"mode"}
	if kind == "canary" {
		expected = append(expected, "basis_points")
	} else if !slices.Contains([]string{"shadow", "active", "rolled_back"}, kind) {
		return fmt.Errorf("rollout mode is invalid")
	}
	if err := requireExactJSONFields(object, expected); err != nil {
		return err
	}
	if kind != "canary" {
		*mode = RolloutMode{Mode: kind}
		return nil
	}
	basis, ok := object["basis_points"].(json.Number)
	if !ok {
		return fmt.Errorf("canary rollout basis points are invalid")
	}
	parsed, err := parseSafeJSONUint(basis, false)
	if err != nil || parsed > 10_000 {
		return fmt.Errorf("canary rollout basis points are invalid")
	}
	*mode = RolloutMode{Mode: kind, BasisPoints: uint16(parsed)}
	return nil
}

// RolloutDecision is one immutable future-selection decision.
type RolloutDecision struct {
	DecisionID   string      `json:"decision_id"`
	FallbackPlan string      `json:"fallback_plan"`
	TargetPlan   string      `json:"target_plan"`
	Mode         RolloutMode `json:"mode"`
}

// OccurrencePin retains complete rollout and execution lineage for one occurrence.
type OccurrencePin struct {
	OccurrenceID     string      `json:"occurrence_id"`
	TemplateID       string      `json:"template_id"`
	DecisionID       string      `json:"decision_id"`
	PlanID           string      `json:"plan_id"`
	ExecutionBinding ArtifactRef `json:"execution_binding"`
	SelectionID      string      `json:"selection_id"`
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

// MigrationRequest asks a pinned adapter for one source-epoch transformation.
type MigrationRequest struct {
	MigrationID         string `json:"migration_id"`
	RunID               string `json:"run_id"`
	FromPlan            string `json:"from_plan"`
	ToPlan              string `json:"to_plan"`
	PlanEdgeID          string `json:"plan_edge_id"`
	CompatibilityID     string `json:"compatibility_id"`
	ExpectedSourceEpoch uint64 `json:"expected_source_epoch"`
	AdapterID           string `json:"adapter_id"`
	AdapterRevision     string `json:"adapter_revision"`
}

// MigrationInvocationPathSegment identifies one exact dynamic invocation edge.
type MigrationInvocationPathSegment struct {
	SiteID     string   `json:"site_id"`
	RegionPath []uint64 `json:"region_path"`
	ScopeID    string   `json:"scope_id"`
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
	ContinuationVersion string                      `json:"continuation_version"`
	RunID               string                      `json:"run_id"`
	PlanID              string                      `json:"plan_id"`
	BindingContext      string                      `json:"binding_context"`
	Frames              []MigrationFrame            `json:"frames"`
	State               *ArtifactRef                `json:"state"`
	WaitSet             []string                    `json:"wait_set"`
	ScopeStack          []string                    `json:"scope_stack"`
	Epoch               uint64                      `json:"epoch"`
	ExecutionFence      uint64                      `json:"execution_fence"`
	ExecutionClaim      *ContinuationExecutionClaim `json:"execution_claim"`
	Status              string                      `json:"status"`
}

// RestartRequest authorizes one replacement Run under an exact new Plan.
type RestartRequest struct {
	RestartID           string      `json:"restart_id"`
	ReplacementRun      string      `json:"replacement_run"`
	RunID               string      `json:"run_id"`
	FromPlan            string      `json:"from_plan"`
	ExpectedSourceEpoch uint64      `json:"expected_source_epoch"`
	ToPlan              string      `json:"to_plan"`
	Input               ArtifactRef `json:"input"`
	Evidence            ArtifactRef `json:"evidence"`
}

// ShadowRequest asks a pinned driver for isolated comparison evidence.
type ShadowRequest struct {
	ComparisonID     string      `json:"comparison_id"`
	DecisionID       string      `json:"decision_id"`
	Subject          string      `json:"subject"`
	PrimaryPlan      string      `json:"primary_plan"`
	ShadowPlan       string      `json:"shadow_plan"`
	DriverID         string      `json:"driver_id"`
	DriverRevision   string      `json:"driver_revision"`
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
	ControlVersion   string              `json:"control_version"`
	CommandID        string              `json:"command_id"`
	Operation        string              `json:"operation"`
	Patch            *PlanPatch          `json:"patch,omitempty"`
	Decision         *RolloutDecision    `json:"decision,omitempty"`
	OccurrenceID     string              `json:"occurrence_id,omitempty"`
	SelectionID      string              `json:"selection_id,omitempty"`
	ExecutionBinding *ArtifactRef        `json:"execution_binding,omitempty"`
	Migration        *MigrationRequest   `json:"-"`
	Restart          *RestartRequest     `json:"-"`
	Shadow           *ShadowRequest      `json:"-"`
	Observation      *RolloutObservation `json:"observation,omitempty"`
	Gate             *RolloutGate        `json:"gate,omitempty"`
	NextDecisionID   string              `json:"next_decision_id,omitempty"`
}

// SubflowReference selects one reusable definition revision for a parent.
type SubflowReference struct {
	LogicalRef      string            `json:"logical_ref"`
	LocalDefinition string            `json:"local_definition"`
	InputSchema     map[string]any    `json:"input_schema"`
	OutputSchema    map[string]any    `json:"output_schema"`
	Strategy        ReferenceStrategy `json:"strategy"`
}

// ReferenceStrategy is the closed latest-compatible or pinned link policy.
type ReferenceStrategy struct {
	Strategy   string `json:"strategy"`
	RevisionID string `json:"revision_id,omitempty"`
}

// PlanTemplate is one unsealed parent plus its logical reusable references.
type PlanTemplate struct {
	TemplateID string             `json:"template_id"`
	Candidate  PlanCandidate      `json:"candidate"`
	References []SubflowReference `json:"references"`
}

// LivePublicationCommand atomically publishes and advances compatible parents.
type LivePublicationCommand struct {
	LogicalRef string             `json:"logical_ref"`
	Definition Definition         `json:"definition"`
	References []SubflowReference `json:"references"`
	Evidence   ArtifactRecord     `json:"evidence"`
	Mode       RolloutMode        `json:"mode"`
}

// LiveEvolutionCommand targets the unified registry, DAG, rollout, and pin authority.
type LiveEvolutionCommand struct {
	ControlVersion string                  `json:"control_version"`
	CommandID      string                  `json:"command_id"`
	Operation      string                  `json:"operation"`
	LogicalRef     string                  `json:"logical_ref,omitempty"`
	Definition     *Definition             `json:"definition,omitempty"`
	References     *[]SubflowReference     `json:"references,omitempty"`
	Template       *PlanTemplate           `json:"template,omitempty"`
	Publication    *LivePublicationCommand `json:"publication,omitempty"`
	TemplateID     string                  `json:"template_id,omitempty"`
	Command        *EvolutionCommand       `json:"command,omitempty"`
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
	if object["control_version"] != "cymule.live-evolution-control/6" {
		return fmt.Errorf("unsupported live evolution control version")
	}
	expected := map[string][]string{
		"publish_definition": {"control_version", "command_id", "operation", "logical_ref", "definition", "references"},
		"register_template":  {"control_version", "command_id", "operation", "template"},
		"publish_and_relink": {"control_version", "command_id", "operation", "publication"},
		"apply":              {"control_version", "command_id", "operation", "template_id", "command"},
	}[operation]
	if expected == nil {
		return fmt.Errorf("unsupported live evolution operation %q", operation)
	}
	if requireExactJSONFields(object, expected) != nil {
		return fmt.Errorf("live evolution command fields are not closed")
	}
	type wire LiveEvolutionCommand
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedCommand := LiveEvolutionCommand(decoded)
	if err := validateLiveEvolutionCommandSemantics(decodedCommand); err != nil {
		return err
	}
	if !wireValuesEqual(value, decodedCommand) {
		return fmt.Errorf("live evolution command loses JSON member presence during typed decoding")
	}
	*command = decodedCommand
	return nil
}

func validateLiveEvolutionCommandSemantics(command LiveEvolutionCommand) error {
	if command.ControlVersion != "cymule.live-evolution-control/6" || !validWireIdentity(command.CommandID) {
		return fmt.Errorf("live-evolution command identity or version is invalid")
	}
	switch command.Operation {
	case "publish_definition":
		if command.Definition == nil || !validWireIdentity(command.LogicalRef) {
			return fmt.Errorf("definition publication command is invalid")
		}
		definition, err := json.Marshal(command.Definition)
		if err != nil {
			return err
		}
		_, err = validateDefinitionRaw(definition)
		if err != nil {
			return err
		}
		if command.References == nil {
			return fmt.Errorf("definition publication references are required")
		}
		return validatePublishedSubflowReferences(*command.References, command.Definition.ID)
	case "register_template":
		if command.Template == nil || !validWireIdentity(command.Template.TemplateID) {
			return fmt.Errorf("template registration command is invalid")
		}
		return validatePlanTemplateWire(*command.Template)
	case "publish_and_relink":
		if command.Publication == nil || !validWireIdentity(command.Publication.LogicalRef) {
			return fmt.Errorf("live publication command is invalid")
		}
		definition, err := json.Marshal(command.Publication.Definition)
		if err != nil {
			return err
		}
		if _, err := validateDefinitionRaw(definition); err != nil {
			return err
		}
		if err := validatePublishedSubflowReferences(
			command.Publication.References,
			command.Publication.Definition.ID,
		); err != nil {
			return err
		}
		if err := validateArtifactRecord(command.Publication.Evidence); err != nil {
			return err
		}
		return validateRolloutModeWire(command.Publication.Mode)
	case "apply":
		if command.Command == nil || !validWireIdentity(command.TemplateID) {
			return fmt.Errorf("live-evolution apply command is invalid")
		}
		if err := validateEvolutionCommandSemantics(*command.Command); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("live-evolution command operation is unknown")
	}
}

func validateEvolutionCommandSemantics(command EvolutionCommand) error {
	if command.ControlVersion != "cymule.evolution-control/5" || !validEvolutionIdentity(command.CommandID) {
		return fmt.Errorf("evolution command identity or version is invalid")
	}
	switch command.Operation {
	case "apply_patch":
		if command.Patch == nil || !validSHA256ID(command.Patch.FromPlan) {
			return fmt.Errorf("Plan patch command is invalid")
		}
		if err := validatePlanCandidateWire(command.Patch.Target); err != nil {
			return err
		}
		if err := validatePatchOperations(command.Patch.Operations); err != nil {
			return err
		}
		return validateArtifactRef(command.Patch.Evidence)
	case "set_rollout":
		if command.Decision == nil || !validEvolutionIdentity(command.Decision.DecisionID) ||
			!validSHA256ID(command.Decision.FallbackPlan) || !validSHA256ID(command.Decision.TargetPlan) {
			return fmt.Errorf("rollout decision command is invalid")
		}
		return validateRolloutModeWire(command.Decision.Mode)
	case "select_occurrence":
		if !validEvolutionIdentity(command.OccurrenceID) || !validEvolutionIdentity(command.SelectionID) ||
			command.ExecutionBinding == nil || command.ExecutionBinding.Kind != "cymule.execution-binding/2" {
			return fmt.Errorf("occurrence selection command is invalid")
		}
		return validateArtifactRef(*command.ExecutionBinding)
	case "migrate":
		if command.Migration == nil {
			return fmt.Errorf("migration command is invalid")
		}
		return validateMigrationRequest(*command.Migration)
	case "restart_under_new_plan":
		if command.Restart == nil || validateRestartRequest(*command.Restart) != nil {
			return fmt.Errorf("restart command is invalid")
		}
	case "shadow":
		if command.Shadow == nil {
			return fmt.Errorf("shadow command is invalid")
		}
		return validateShadowRequest(*command.Shadow)
	case "observe":
		if command.Observation == nil ||
			!validEvolutionIdentity(command.Observation.ObservationID) ||
			!validEvolutionIdentity(command.Observation.DecisionID) ||
			!validEvolutionIdentity(command.Observation.OccurrenceID) ||
			!validSHA256ID(command.Observation.PlanID) ||
			!slices.Contains([]string{"succeeded", "failed"}, command.Observation.Outcome) {
			return fmt.Errorf("rollout observation command is invalid")
		}
		return validateArtifactRef(command.Observation.Evidence)
	case "apply_gate":
		if command.Gate == nil || !validEvolutionIdentity(command.Gate.GateID) ||
			!validEvolutionIdentity(command.NextDecisionID) {
			return fmt.Errorf("rollout gate command is invalid")
		}
	default:
		return fmt.Errorf("evolution command operation is unknown")
	}
	return nil
}

func validatePatchOperations(operations []PatchOperation) error {
	if len(operations) == 0 {
		return fmt.Errorf("Plan patch requires a non-empty structural diff")
	}
	var previousTarget, previousKind string
	for index, operation := range operations {
		if !validEvolutionIdentity(operation.Kind) || !validEvolutionIdentity(operation.Target) {
			return fmt.Errorf("Plan patch operation identity is invalid")
		}
		validShape := false
		switch operation.Kind {
		case "add":
			validShape = operation.Before == nil && operation.After != nil && validBareSHA256(*operation.After)
		case "remove":
			validShape = operation.Before != nil && validBareSHA256(*operation.Before) && operation.After == nil
		case "replace":
			validShape = operation.Before != nil && operation.After != nil &&
				validBareSHA256(*operation.Before) && validBareSHA256(*operation.After) &&
				*operation.Before != *operation.After
		}
		if !validShape {
			return fmt.Errorf("Plan patch operation is malformed")
		}
		if index > 0 && (previousTarget > operation.Target ||
			previousTarget == operation.Target && previousKind >= operation.Kind) {
			return fmt.Errorf("Plan patch operations are not in canonical order")
		}
		previousTarget, previousKind = operation.Target, operation.Kind
	}
	return nil
}

func validEvolutionIdentity(value string) bool {
	return validWireIdentity(value)
}

func validateRolloutModeWire(mode RolloutMode) error {
	switch mode.Mode {
	case "shadow", "active", "rolled_back":
		if mode.BasisPoints != 0 {
			return fmt.Errorf("non-canary rollout mode carries basis points")
		}
	case "canary":
		if mode.BasisPoints > 10_000 {
			return fmt.Errorf("canary rollout basis points are invalid")
		}
	default:
		return fmt.Errorf("rollout mode is invalid")
	}
	return nil
}

// MarshalJSON emits the exact operation-specific request shape.
func (command EvolutionCommand) MarshalJSON() ([]byte, error) {
	if err := validateGoJSONStrings(reflect.ValueOf(command)); err != nil {
		return nil, err
	}
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
		ControlVersion   string              `json:"control_version"`
		CommandID        string              `json:"command_id"`
		Operation        string              `json:"operation"`
		Patch            *PlanPatch          `json:"patch,omitempty"`
		Decision         *RolloutDecision    `json:"decision,omitempty"`
		OccurrenceID     string              `json:"occurrence_id,omitempty"`
		SelectionID      string              `json:"selection_id,omitempty"`
		ExecutionBinding *ArtifactRef        `json:"execution_binding,omitempty"`
		Request          any                 `json:"request,omitempty"`
		Observation      *RolloutObservation `json:"observation,omitempty"`
		Gate             *RolloutGate        `json:"gate,omitempty"`
		NextDecisionID   string              `json:"next_decision_id,omitempty"`
	}{
		ControlVersion:   command.ControlVersion,
		CommandID:        command.CommandID,
		Operation:        command.Operation,
		Patch:            command.Patch,
		Decision:         command.Decision,
		OccurrenceID:     command.OccurrenceID,
		SelectionID:      command.SelectionID,
		ExecutionBinding: command.ExecutionBinding,
		Request:          request,
		Observation:      command.Observation,
		Gate:             command.Gate,
		NextDecisionID:   command.NextDecisionID,
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
	if object["control_version"] != "cymule.evolution-control/5" {
		return fmt.Errorf("unsupported evolution control version")
	}
	expectedFields, ok := map[string][]string{
		"apply_patch":            {"control_version", "command_id", "operation", "patch"},
		"set_rollout":            {"control_version", "command_id", "operation", "decision"},
		"select_occurrence":      {"control_version", "command_id", "operation", "occurrence_id", "selection_id", "execution_binding"},
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
		ControlVersion   string              `json:"control_version"`
		CommandID        string              `json:"command_id"`
		Operation        string              `json:"operation"`
		Patch            *PlanPatch          `json:"patch"`
		Decision         *RolloutDecision    `json:"decision"`
		OccurrenceID     string              `json:"occurrence_id"`
		SelectionID      string              `json:"selection_id"`
		ExecutionBinding *ArtifactRef        `json:"execution_binding"`
		Request          json.RawMessage     `json:"request"`
		Observation      *RolloutObservation `json:"observation"`
		Gate             *RolloutGate        `json:"gate"`
		NextDecisionID   string              `json:"next_decision_id"`
	}
	if err := decodeClosedValue(value, &wire); err != nil {
		return err
	}
	decodedCommand := EvolutionCommand{
		ControlVersion:   wire.ControlVersion,
		CommandID:        wire.CommandID,
		Operation:        wire.Operation,
		Patch:            wire.Patch,
		Decision:         wire.Decision,
		OccurrenceID:     wire.OccurrenceID,
		SelectionID:      wire.SelectionID,
		ExecutionBinding: wire.ExecutionBinding,
		Observation:      wire.Observation,
		Gate:             wire.Gate,
		NextDecisionID:   wire.NextDecisionID,
	}
	if wire.Operation == "select_occurrence" {
		if wire.OccurrenceID == "" || wire.SelectionID == "" || wire.ExecutionBinding == nil {
			return fmt.Errorf("occurrence selection lineage is incomplete")
		}
		if err := validateArtifactRef(*wire.ExecutionBinding); err != nil {
			return err
		}
		if wire.ExecutionBinding.Kind != "cymule.execution-binding/2" {
			return fmt.Errorf("occurrence binding is not an ExecutionBinding Artifact")
		}
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
		decodedCommand.Migration = &request
	case "restart_under_new_plan":
		var request RestartRequest
		if err := decodeClosedJSON(wire.Request, &request); err != nil {
			return err
		}
		if err := validateRestartRequest(request); err != nil {
			return err
		}
		decodedCommand.Restart = &request
	case "shadow":
		var request ShadowRequest
		if err := decodeClosedJSON(wire.Request, &request); err != nil {
			return err
		}
		if err := validateShadowRequest(request); err != nil {
			return err
		}
		decodedCommand.Shadow = &request
	}
	if err := validateEvolutionCommandSemantics(decodedCommand); err != nil {
		return err
	}
	if !wireValuesEqual(value, decodedCommand) {
		return fmt.Errorf("evolution command loses JSON member presence during typed decoding")
	}
	*command = decodedCommand
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

// ClockObservationRef identifies one receipt retained by an admitted Clock authority.
type ClockObservationRef struct {
	ClockVersion     string `json:"clock_version"`
	ObservationID    string `json:"observation_id"`
	SourceID         string `json:"source_id"`
	SourceGeneration string `json:"source_generation"`
	Scope            string `json:"scope"`
}

// ClockObservationResult binds one Engine-issued observation to its requested Run.
type ClockObservationResult struct {
	RunID       string              `json:"run_id"`
	Observation ClockObservationRef `json:"observation"`
}

// ClockObservation is one complete receipt resolved from persistent Clock authority.
type ClockObservation struct {
	ClockVersion     string `json:"clock_version"`
	ObservationID    string `json:"observation_id"`
	SourceID         string `json:"source_id"`
	SourceGeneration string `json:"source_generation"`
	Scope            string `json:"scope"`
	LogicalTime      uint64 `json:"logical_time"`
	ObservedUnixMS   uint64 `json:"observed_unix_ms"`
}

// ExecutionClaimRequest authorizes one exact durable Run driver.
type ExecutionClaimRequest struct {
	Owner string              `json:"owner"`
	Clock ClockObservationRef `json:"clock"`
	TTL   uint64              `json:"ttl"`
}

// ContinuationExecutionClaim is the retained single-driver authority for one Run.
type ContinuationExecutionClaim struct {
	ClaimVersion          string              `json:"claim_version"`
	RunID                 string              `json:"run_id"`
	ContinuationID        string              `json:"continuation_id"`
	Owner                 string              `json:"owner"`
	ContinuationAttemptID string              `json:"continuation_attempt_id"`
	Fence                 uint64              `json:"fence"`
	PlanID                string              `json:"plan_id"`
	ExecutionBindingRef   ArtifactRef         `json:"execution_binding_ref"`
	ClockObservationRef   ClockObservationRef `json:"clock_observation_ref"`
	LogicalAcquiredAt     uint64              `json:"logical_acquired_at"`
	LogicalTTL            uint64              `json:"logical_ttl"`
	LogicalExpiresAt      uint64              `json:"logical_expires_at"`
}

// DurableControlVersion is the only admitted durable control and query generation.
const DurableControlVersion = "cymule.durable-control/4"

const maxDurableQueryPageItems = 256
const maxDurableQueryPageBytes = 1024 * 1024
const maxDurableQuerySummaryBytes = 32 * 1024
const maxDurableStateRootLeafBytes = 12 * 1024 * 1024
const maxDurableQueryExactResponseBytes = 13 * 1024 * 1024

// DurablePageQueryKind selects one authenticated durable map.
type DurablePageQueryKind string

const (
	DurableRunIndexQuery       DurablePageQueryKind = "run_index"
	DurableRunWaitsQuery       DurablePageQueryKind = "run_waits"
	DurableRunEffectsQuery     DurablePageQueryKind = "run_effects"
	DurableRunOccurrencesQuery DurablePageQueryKind = "run_occurrences"
	DurableRunAttemptsQuery    DurablePageQueryKind = "run_attempts"
)

// DurablePagePosition is the complete authenticated position of a page's final item.
type DurablePagePosition struct {
	CanonicalKey string `json:"canonical_key"`
	KeyHash      string `json:"key_hash"`
}

// DurablePageCursor pins continuation to one query, owner, revision, root, and position.
type DurablePageCursor struct {
	QueryKind      DurablePageQueryKind `json:"query_kind"`
	RunID          *string              `json:"run_id"`
	SourceRevision string               `json:"source_revision"`
	SourceRoot     string               `json:"source_root"`
	Position       DurablePagePosition  `json:"position"`
}

// UnmarshalJSON requires the cursor's nullable owner member to be present.
func (cursor *DurablePageCursor) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("durable page cursor is not an object")
	}
	if err := requireExactJSONFields(object, []string{
		"query_kind", "run_id", "source_revision", "source_root", "position",
	}); err != nil {
		return err
	}
	type wire DurablePageCursor
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	*cursor = DurablePageCursor(decoded)
	return nil
}

// DurablePageQueryOptions are the closed bounds for one revision-pinned page.
type DurablePageQueryOptions struct {
	ExpectedRevision  *string
	Cursor            *DurablePageCursor
	Limit             uint32
	MaxCanonicalBytes uint64
}

// DurableRunItemSelector selects one exact typed Run-owned leaf.
type DurableRunItemSelector struct {
	Kind         string `json:"kind"`
	WaitID       string `json:"wait_id,omitempty"`
	IntentID     string `json:"intent_id,omitempty"`
	OccurrenceID string `json:"occurrence_id,omitempty"`
	AttemptID    string `json:"attempt_id,omitempty"`
}

// MarshalJSON emits exactly the identity owned by the selected item kind.
func (selector DurableRunItemSelector) MarshalJSON() ([]byte, error) {
	value := map[string]any{"kind": selector.Kind}
	switch selector.Kind {
	case "wait":
		value["wait_id"] = selector.WaitID
	case "effect":
		value["intent_id"] = selector.IntentID
	case "occurrence":
		value["occurrence_id"] = selector.OccurrenceID
	case "attempt":
		value["attempt_id"] = selector.AttemptID
	default:
		return nil, fmt.Errorf("exact Run-item selector kind is invalid")
	}
	return json.Marshal(value)
}

// UnmarshalJSON admits exactly one closed item selector.
func (selector *DurableRunItemSelector) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("exact Run-item selector is not an object")
	}
	kind, ok := object["kind"].(string)
	if !ok {
		return fmt.Errorf("exact Run-item selector kind is missing")
	}
	field := map[string]string{
		"wait": "wait_id", "effect": "intent_id", "occurrence": "occurrence_id", "attempt": "attempt_id",
	}[kind]
	if field == "" {
		return fmt.Errorf("exact Run-item selector kind is invalid")
	}
	if err := requireExactJSONFields(object, []string{"kind", field}); err != nil {
		return err
	}
	type wire DurableRunItemSelector
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	*selector = DurableRunItemSelector(decoded)
	return nil
}

// DurableRunItemQuery is one bounded exact-leaf read.
type DurableRunItemQuery struct {
	RunID             string
	ExpectedRevision  *string
	Selector          DurableRunItemSelector
	MaxCanonicalBytes uint64
}

// DurableQueryPage is one bounded revision/root-pinned summary page.
type DurableQueryPage struct {
	ObservedRevision string             `json:"observed_revision"`
	SourceRoot       string             `json:"source_root"`
	Items            []json.RawMessage  `json:"items"`
	NextCursor       *DurablePageCursor `json:"next_cursor"`
}

// UnmarshalJSON requires a terminal page to retain next_cursor as explicit null.
func (page *DurableQueryPage) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("durable query page is not an object")
	}
	if err := requireExactJSONFields(object, []string{
		"observed_revision", "source_root", "items", "next_cursor",
	}); err != nil {
		return err
	}
	type wire DurableQueryPage
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	*page = DurableQueryPage(decoded)
	return nil
}

// DurableCommand is one closed M1 mutation or read-only query.
type DurableCommand struct {
	Type              string                  `json:"type"`
	ControlVersion    string                  `json:"control_version"`
	RunID             string                  `json:"run_id,omitempty"`
	Candidate         *PlanCandidate          `json:"candidate,omitempty"`
	Input             json.RawMessage         `json:"input,omitempty"`
	ActivationID      string                  `json:"activation_id,omitempty"`
	Source            *WaitActivationSource   `json:"source,omitempty"`
	WaitIDs           []string                `json:"wait_ids,omitempty"`
	Value             json.RawMessage         `json:"value,omitempty"`
	IntentID          string                  `json:"intent_id,omitempty"`
	ResolutionID      string                  `json:"resolution_id,omitempty"`
	ExecutionBinding  *ArtifactRef            `json:"execution_binding,omitempty"`
	OccurrenceBinding string                  `json:"occurrence_binding,omitempty"`
	ClaimOwner        string                  `json:"claim_owner,omitempty"`
	ClaimEpoch        uint64                  `json:"claim_epoch,omitempty"`
	Resolution        string                  `json:"resolution,omitempty"`
	CancellationID    string                  `json:"cancellation_id,omitempty"`
	Reason            json.RawMessage         `json:"reason,omitempty"`
	ExpectedRevision  *string                 `json:"expected_revision,omitempty"`
	Cursor            *DurablePageCursor      `json:"cursor,omitempty"`
	Limit             uint32                  `json:"limit,omitempty"`
	MaxCanonicalBytes uint64                  `json:"max_canonical_bytes,omitempty"`
	Selector          *DurableRunItemSelector `json:"selector,omitempty"`
	ExpectedFence     uint64                  `json:"expected_fence,omitempty"`
	Execution         *ExecutionClaimRequest  `json:"execution,omitempty"`
}

func durableCommandFields(commandType string) []string {
	return map[string][]string{
		"start_run":      {"type", "control_version", "run_id", "candidate", "input", "execution"},
		"resume_run":     {"type", "control_version", "run_id", "execution"},
		"takeover_run":   {"type", "control_version", "run_id", "expected_fence", "execution"},
		"activate_wait":  {"type", "control_version", "activation_id", "source", "wait_ids", "value"},
		"release_effect": {"type", "control_version", "intent_id", "execution"},
		"resolve_effect": {
			"type", "control_version", "run_id", "value", "intent_id", "resolution_id",
			"execution_binding", "occurrence_binding", "claim_owner", "claim_epoch", "resolution",
		},
		"cancel_run": {"type", "control_version", "run_id", "cancellation_id", "reason"},
		"run_index_page": {
			"type", "control_version", "expected_revision", "cursor", "limit", "max_canonical_bytes",
		},
		"run_current": {"type", "control_version", "run_id", "expected_revision"},
		"run_wait_page": {
			"type", "control_version", "run_id", "expected_revision", "cursor", "limit", "max_canonical_bytes",
		},
		"run_effect_page": {
			"type", "control_version", "run_id", "expected_revision", "cursor", "limit", "max_canonical_bytes",
		},
		"run_occurrence_page": {
			"type", "control_version", "run_id", "expected_revision", "cursor", "limit", "max_canonical_bytes",
		},
		"run_attempt_page": {
			"type", "control_version", "run_id", "expected_revision", "cursor", "limit", "max_canonical_bytes",
		},
		"run_item": {
			"type", "control_version", "run_id", "expected_revision", "selector", "max_canonical_bytes",
		},
	}[commandType]
}

func durableCommandObject(command DurableCommand) (map[string]any, error) {
	value := map[string]any{"type": command.Type, "control_version": command.ControlVersion}
	switch command.Type {
	case "start_run":
		value["run_id"], value["candidate"], value["input"], value["execution"] = command.RunID, command.Candidate, command.Input, command.Execution
	case "resume_run":
		value["run_id"], value["execution"] = command.RunID, command.Execution
	case "takeover_run":
		value["run_id"], value["expected_fence"], value["execution"] = command.RunID, command.ExpectedFence, command.Execution
	case "activate_wait":
		value["activation_id"], value["source"], value["wait_ids"], value["value"] = command.ActivationID, command.Source, command.WaitIDs, command.Value
	case "release_effect":
		value["intent_id"], value["execution"] = command.IntentID, command.Execution
	case "resolve_effect":
		value["run_id"], value["value"], value["intent_id"] = command.RunID, command.Value, command.IntentID
		value["resolution_id"], value["execution_binding"] = command.ResolutionID, command.ExecutionBinding
		value["occurrence_binding"], value["claim_owner"] = command.OccurrenceBinding, command.ClaimOwner
		value["claim_epoch"], value["resolution"] = command.ClaimEpoch, command.Resolution
	case "cancel_run":
		value["run_id"], value["cancellation_id"], value["reason"] = command.RunID, command.CancellationID, command.Reason
	case "run_index_page":
		value["expected_revision"], value["cursor"] = command.ExpectedRevision, command.Cursor
		value["limit"], value["max_canonical_bytes"] = command.Limit, command.MaxCanonicalBytes
	case "run_current":
		value["run_id"], value["expected_revision"] = command.RunID, command.ExpectedRevision
	case "run_wait_page", "run_effect_page", "run_occurrence_page", "run_attempt_page":
		value["run_id"], value["expected_revision"], value["cursor"] = command.RunID, command.ExpectedRevision, command.Cursor
		value["limit"], value["max_canonical_bytes"] = command.Limit, command.MaxCanonicalBytes
	case "run_item":
		value["run_id"], value["expected_revision"] = command.RunID, command.ExpectedRevision
		value["selector"], value["max_canonical_bytes"] = command.Selector, command.MaxCanonicalBytes
	default:
		return nil, fmt.Errorf("durable command variant is unknown")
	}
	return value, nil
}

// MarshalJSON preserves required nullable members while omitting fields owned by other variants.
func (command DurableCommand) MarshalJSON() ([]byte, error) {
	value, err := durableCommandObject(command)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// UnmarshalJSON admits exactly one closed durable command variant.
func (command *DurableCommand) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("durable command is not an object")
	}
	commandType, ok := object["type"].(string)
	if !ok {
		return fmt.Errorf("durable command type is missing")
	}
	fields := durableCommandFields(commandType)
	if fields == nil {
		return fmt.Errorf("durable command variant is unknown")
	}
	if err := requireExactJSONFields(object, fields); err != nil {
		return err
	}
	type wire DurableCommand
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	*command = DurableCommand(decoded)
	return nil
}

// WaitActivationReceipt retains the complete selected, applied, and Ready-Run sets.
type WaitActivationReceipt struct {
	ReceiptVersion string         `json:"receipt_version"`
	Activation     WaitActivation `json:"activation"`
	AppliedWaitIDs []string       `json:"applied_wait_ids"`
	ReadyRunIDs    []string       `json:"ready_run_ids"`
}

// RunCancellationCommand is the complete semantic cancellation retained by its receipt.
type RunCancellationCommand struct {
	CancellationID string          `json:"cancellation_id"`
	RunID          string          `json:"run_id"`
	Reason         json.RawMessage `json:"reason"`
}

// RunCancellationBoundary is the canonical terminal boundary retained by a cancellation.
type RunCancellationBoundary struct {
	Status string      `json:"status"`
	Reason ArtifactRef `json:"reason"`
}

// RunCancellationReceipt retains the complete cancellation command and terminal authority.
type RunCancellationReceipt struct {
	ReceiptVersion string                  `json:"receipt_version"`
	Command        RunCancellationCommand  `json:"command"`
	Boundary       RunCancellationBoundary `json:"boundary"`
	ReceiptID      string                  `json:"receipt_id"`
}

// EffectResolutionReceiptCommand is the complete requested terminal reconciliation.
type EffectResolutionReceiptCommand struct {
	ResolutionID      string          `json:"resolution_id"`
	RunID             string          `json:"run_id"`
	IntentID          string          `json:"intent_id"`
	ExecutionBinding  ArtifactRef     `json:"execution_binding"`
	OccurrenceBinding string          `json:"occurrence_binding"`
	ClaimOwner        string          `json:"claim_owner"`
	ClaimEpoch        uint64          `json:"claim_epoch"`
	Resolution        string          `json:"resolution"`
	Value             json.RawMessage `json:"value"`
}

// EffectResolutionReceipt retains requested semantics and independent provider truth.
type EffectResolutionReceipt struct {
	ReceiptVersion   string                         `json:"receipt_version"`
	Command          EffectResolutionReceiptCommand `json:"command"`
	ActualResolution string                         `json:"actual_resolution"`
	ActualValue      json.RawMessage                `json:"actual_value"`
	Result           json.RawMessage                `json:"result"`
	ReceiptID        string                         `json:"receipt_id"`
}

// DurableResponse is the closed stateful M1 result union.
type DurableResponse struct {
	Type             string            `json:"type"`
	Boundary         json.RawMessage   `json:"boundary,omitempty"`
	Receipt          json.RawMessage   `json:"receipt,omitempty"`
	Page             *DurableQueryPage `json:"page,omitempty"`
	RunID            string            `json:"run_id,omitempty"`
	ObservedRevision string            `json:"observed_revision,omitempty"`
	SourceRoot       string            `json:"source_root,omitempty"`
	Current          json.RawMessage   `json:"current,omitempty"`
	Item             json.RawMessage   `json:"item,omitempty"`
}

func durableResponseFields(responseType string) []string {
	return map[string][]string{
		"run_boundary":        {"type", "boundary"},
		"wait_activated":      {"type", "receipt"},
		"run_cancelled":       {"type", "receipt"},
		"effect_resolved":     {"type", "receipt"},
		"run_index_page":      {"type", "page"},
		"run_current":         {"type", "observed_revision", "source_root", "current"},
		"run_wait_page":       {"type", "run_id", "page"},
		"run_effect_page":     {"type", "run_id", "page"},
		"run_occurrence_page": {"type", "run_id", "page"},
		"run_attempt_page":    {"type", "run_id", "page"},
		"run_item":            {"type", "run_id", "observed_revision", "source_root", "item"},
	}[responseType]
}

func durableResponseObject(response DurableResponse) (map[string]any, error) {
	value := map[string]any{"type": response.Type}
	switch response.Type {
	case "run_boundary":
		value["boundary"] = response.Boundary
	case "wait_activated", "run_cancelled", "effect_resolved":
		value["receipt"] = response.Receipt
	case "run_index_page":
		value["page"] = response.Page
	case "run_current":
		value["observed_revision"], value["source_root"] = response.ObservedRevision, response.SourceRoot
		if len(response.Current) == 0 {
			value["current"] = nil
		} else {
			value["current"] = response.Current
		}
	case "run_wait_page", "run_effect_page", "run_occurrence_page", "run_attempt_page":
		value["run_id"], value["page"] = response.RunID, response.Page
	case "run_item":
		value["run_id"], value["observed_revision"], value["source_root"] = response.RunID, response.ObservedRevision, response.SourceRoot
		if len(response.Item) == 0 {
			value["item"] = nil
		} else {
			value["item"] = response.Item
		}
	default:
		return nil, fmt.Errorf("durable response variant is unknown")
	}
	return value, nil
}

// MarshalJSON preserves required nullable query members.
func (response DurableResponse) MarshalJSON() ([]byte, error) {
	value, err := durableResponseObject(response)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// UnmarshalJSON rejects duplicate members and decodes exactly one durable response variant.
func (response *DurableResponse) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("durable response is not an object")
	}
	typeName, ok := object["type"].(string)
	if !ok {
		return fmt.Errorf("durable response type is missing")
	}
	fields := durableResponseFields(typeName)
	if fields == nil {
		return fmt.Errorf("durable response variant is unknown")
	}
	if err := requireExactJSONFields(object, fields); err != nil {
		return err
	}
	type wire DurableResponse
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedResponse := DurableResponse(decoded)
	if err := decodedResponse.validate(); err != nil {
		return err
	}
	if !wireValuesEqual(value, decodedResponse) {
		return fmt.Errorf("durable response loses JSON member presence during typed decoding")
	}
	*response = decodedResponse
	return nil
}

func (response DurableResponse) validate() error {
	switch response.Type {
	case "run_boundary":
		return validateDurableBoundary(response.Boundary)
	case "wait_activated":
		return validateWaitActivationReceiptRaw(response.Receipt)
	case "run_cancelled":
		return validateRunCancellationReceiptRaw(response.Receipt)
	case "effect_resolved":
		return validateEffectResolutionReceiptRaw(response.Receipt)
	case "run_index_page":
		return validateDurableQueryPage(response.Page, DurableRunIndexQuery, "", validateDurableRunIndexSummaryRaw)
	case "run_current":
		if err := validateDurableQuerySource(response.ObservedRevision, response.SourceRoot); err != nil {
			return err
		}
		if !rawMessageIsNull(response.Current) {
			if err := validateDurableRunCurrentRaw(response.Current); err != nil {
				return err
			}
		}
	case "run_wait_page":
		return validateDurableRunPage(response, DurableRunWaitsQuery, validateDurableWaitSummaryRaw)
	case "run_effect_page":
		return validateDurableRunPage(response, DurableRunEffectsQuery, validateDurableEffectSummaryRaw)
	case "run_occurrence_page":
		return validateDurableRunPage(response, DurableRunOccurrencesQuery, validateDurableOccurrenceSummaryRaw)
	case "run_attempt_page":
		return validateDurableRunPage(response, DurableRunAttemptsQuery, validateDurableAttemptSummaryRaw)
	case "run_item":
		if !validRunIdentity(response.RunID) {
			return fmt.Errorf("exact durable Run-item owner is invalid")
		}
		if err := validateDurableQuerySource(response.ObservedRevision, response.SourceRoot); err != nil {
			return err
		}
		if !rawMessageIsNull(response.Item) {
			owner, err := validateDurableRunItemRaw(response.Item)
			if err != nil {
				return err
			}
			if owner != response.RunID {
				return fmt.Errorf("exact durable Run item belongs to a different Run")
			}
		}
	default:
		return fmt.Errorf("durable response variant is unknown")
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	limit := maxDurableQueryPageBytes
	if response.Type == "run_item" {
		limit = maxDurableQueryExactResponseBytes
	}
	if slices.Contains([]string{
		"run_index_page", "run_current", "run_wait_page", "run_effect_page",
		"run_occurrence_page", "run_attempt_page", "run_item",
	}, response.Type) && len(encoded) > limit {
		return fmt.Errorf("durable query response exceeds its canonical byte limit")
	}
	return nil
}

func rawMessageIsNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func rawMessageMatchesTyped(raw json.RawMessage, value any) bool {
	decoded, err := decodeUniqueJSON(raw)
	return err == nil && wireValuesEqual(decoded, value)
}

func validateStrictlySortedIdentities(values []string, kind string) error {
	for index, value := range values {
		if !validClockIdentity(value) {
			return fmt.Errorf("%s identity is not a printable 1..=512-scalar value", kind)
		}
		if index > 0 && values[index-1] >= value {
			return fmt.Errorf("%s identities are not strictly sorted and unique", kind)
		}
	}
	return nil
}

func validateStrictlySortedContentIDs(values []string, kind string) error {
	if err := validateStrictlySortedIdentities(values, kind); err != nil {
		return err
	}
	for _, value := range values {
		if !validSHA256ID(value) {
			return fmt.Errorf("%s identity is not a content ID", kind)
		}
	}
	return nil
}

func decodeClosedRawValue(raw json.RawMessage, target any) error {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return err
	}
	return decodeClosedValue(value, target)
}

func validateWaitActivationReceiptRaw(raw json.RawMessage) error {
	var receipt WaitActivationReceipt
	if err := validateClosedRaw(raw, []string{
		"receipt_version", "activation", "applied_wait_ids", "ready_run_ids",
	}, &receipt); err != nil {
		return err
	}
	if !rawMessageMatchesTyped(raw, receipt) ||
		receipt.ReceiptVersion != "cymule.wait-activation-receipt/3" {
		return fmt.Errorf("wait activation receipt is invalid")
	}
	if err := validateWaitActivationResponse(receipt.Activation); err != nil {
		return err
	}
	if receipt.AppliedWaitIDs == nil || receipt.ReadyRunIDs == nil {
		return fmt.Errorf("wait activation receipt collections are invalid")
	}
	if err := validateStrictlySortedContentIDs(receipt.AppliedWaitIDs, "applied wait"); err != nil {
		return err
	}
	if err := validateStrictlySortedIdentities(receipt.ReadyRunIDs, "ready Run"); err != nil {
		return err
	}
	selected := make(map[string]struct{}, len(receipt.Activation.WaitIDs))
	for _, waitID := range receipt.Activation.WaitIDs {
		selected[waitID] = struct{}{}
	}
	for _, waitID := range receipt.AppliedWaitIDs {
		if _, ok := selected[waitID]; !ok {
			return fmt.Errorf("wait activation receipt applied an unselected target")
		}
	}
	if len(receipt.AppliedWaitIDs) == 0 && len(receipt.ReadyRunIDs) != 0 {
		return fmt.Errorf("terminal wait activation non-winner returned ready Runs")
	}
	return nil
}

func validateRunCancellationReceiptRaw(raw json.RawMessage) error {
	var receipt RunCancellationReceipt
	if err := validateClosedRaw(raw, []string{
		"receipt_version", "command", "boundary", "receipt_id",
	}, &receipt); err != nil {
		return err
	}
	if !rawMessageMatchesTyped(raw, receipt) ||
		receipt.ReceiptVersion != "cymule.run-cancellation-receipt/1" ||
		!validClockIdentity(receipt.Command.CancellationID) ||
		!validRunIdentity(receipt.Command.RunID) || len(receipt.Command.Reason) == 0 ||
		!validLowerHexDigest(receipt.ReceiptID) || receipt.Boundary.Status != "cancelled" ||
		validateArtifactRef(receipt.Boundary.Reason) != nil ||
		receipt.Boundary.Reason.Kind != "cymule.cancellation-reason/1" {
		return fmt.Errorf("Run cancellation receipt is invalid")
	}
	if _, err := decodeUniqueJSON(receipt.Command.Reason); err != nil {
		return fmt.Errorf("Run cancellation reason is outside strict JSON")
	}
	return nil
}

func validateEffectResolutionReceiptCommand(command EffectResolutionReceiptCommand) error {
	if !validClockIdentity(command.ResolutionID) || !validRunIdentity(command.RunID) ||
		!validSHA256ID(command.IntentID) || validateArtifactRef(command.ExecutionBinding) != nil ||
		command.ExecutionBinding.Kind != "cymule.execution-binding/2" ||
		!validSHA256ID(command.OccurrenceBinding) || !validClockIdentity(command.ClaimOwner) ||
		command.ClaimEpoch == 0 || command.ClaimEpoch > maxExactInteger ||
		!slices.Contains([]string{"resolved_applied", "resolved_not_applied"}, command.Resolution) ||
		len(command.Value) == 0 {
		return fmt.Errorf("effect resolution receipt command is invalid")
	}
	if _, err := decodeUniqueJSON(command.Value); err != nil {
		return fmt.Errorf("effect resolution receipt command value is outside strict JSON")
	}
	if command.Resolution == "resolved_not_applied" && !rawMessageIsNull(command.Value) {
		return fmt.Errorf("not-applied effect resolution command carries a value")
	}
	return nil
}

func validateEffectResolutionReceiptRaw(raw json.RawMessage) error {
	var receipt EffectResolutionReceipt
	if err := validateClosedRaw(raw, []string{
		"receipt_version", "command", "actual_resolution", "actual_value", "result",
		"receipt_id",
	}, &receipt); err != nil {
		return err
	}
	if !rawMessageMatchesTyped(raw, receipt) ||
		receipt.ReceiptVersion != "cymule.effect-resolution-receipt/1" ||
		!slices.Contains([]string{"resolved_applied", "resolved_not_applied"}, receipt.ActualResolution) ||
		len(receipt.ActualValue) == 0 || len(receipt.Result) == 0 ||
		!validLowerHexDigest(receipt.ReceiptID) {
		return fmt.Errorf("effect resolution receipt is invalid")
	}
	if err := validateEffectResolutionReceiptCommand(receipt.Command); err != nil {
		return err
	}
	if _, err := decodeUniqueJSON(receipt.ActualValue); err != nil {
		return fmt.Errorf("effect resolution actual value is outside strict JSON")
	}
	actualValueIsNull := rawMessageIsNull(receipt.ActualValue)
	resultIsNull := rawMessageIsNull(receipt.Result)
	if (receipt.ActualResolution == "resolved_applied" && resultIsNull) ||
		(receipt.ActualResolution == "resolved_not_applied" && (!actualValueIsNull || !resultIsNull)) {
		return fmt.Errorf("effect resolution actual value and result disagree")
	}
	if !resultIsNull {
		var result ArtifactRef
		if err := validateClosedRaw(receipt.Result, []string{"identity_version", "artifact_id", "kind"}, &result); err != nil {
			return err
		}
		if err := validateArtifactRef(result); err != nil {
			return err
		}
		if result.Kind != "cymule.effect-result/1" {
			return fmt.Errorf("effect resolution result kind is invalid")
		}
	}
	return nil
}

// LiveEvolutionOutcome is one closed durable live-evolution result.
type LiveEvolutionOutcome struct {
	Result     string          `json:"result"`
	Revision   json.RawMessage `json:"revision,omitempty"`
	Linked     json.RawMessage `json:"linked,omitempty"`
	Receipt    json.RawMessage `json:"receipt,omitempty"`
	Edge       json.RawMessage `json:"edge,omitempty"`
	Pin        *OccurrencePin  `json:"pin,omitempty"`
	Comparison json.RawMessage `json:"comparison,omitempty"`
	Transition json.RawMessage `json:"transition,omitempty"`
}

func (outcome LiveEvolutionOutcome) validate() error {
	present := func(value json.RawMessage) bool { return len(value) != 0 }
	valid := false
	switch outcome.Result {
	case "definition_published":
		valid = present(outcome.Revision)
	case "template_registered":
		valid = present(outcome.Linked)
	case "publication_applied", "migrated", "restart_authorized":
		valid = present(outcome.Receipt)
	case "patch_applied":
		valid = present(outcome.Edge)
	case "applied":
		valid = true
	case "occurrence_selected":
		valid = outcome.Pin != nil
	case "shadow_recorded":
		valid = present(outcome.Comparison)
	case "gate_applied":
		valid = present(outcome.Transition)
	}
	count := 0
	for _, item := range []bool{present(outcome.Revision), present(outcome.Linked), present(outcome.Receipt), present(outcome.Edge), outcome.Pin != nil, present(outcome.Comparison), present(outcome.Transition)} {
		if item {
			count++
		}
	}
	if !valid || (outcome.Result == "applied" && count != 0) || (outcome.Result != "applied" && count != 1) {
		return fmt.Errorf("live-evolution outcome fields are not closed")
	}
	switch outcome.Result {
	case "definition_published":
		return validateSubflowRevisionRaw(outcome.Revision)
	case "template_registered":
		return validateLinkedPlanRaw(outcome.Linked)
	case "publication_applied":
		return validatePublicationReceiptRaw(outcome.Receipt)
	case "patch_applied":
		return validatePlanEdgeRaw(outcome.Edge)
	case "applied":
		return nil
	case "occurrence_selected":
		return validateOccurrencePin(*outcome.Pin)
	case "migrated":
		return validateMigrationReceiptRaw(outcome.Receipt)
	case "restart_authorized":
		return validateRestartReceiptRaw(outcome.Receipt)
	case "shadow_recorded":
		return validateShadowComparisonRaw(outcome.Comparison)
	case "gate_applied":
		return validateRolloutTransitionRaw(outcome.Transition)
	}
	return fmt.Errorf("live-evolution outcome variant is unknown")
}

// EvolutionStateFamily selects one normalized durable Evolution map.
type EvolutionStateFamily string

const (
	EvolutionDefinitionCurrent              EvolutionStateFamily = "definition_current"
	EvolutionDefinitionCompatibilityCurrent EvolutionStateFamily = "definition_compatibility_current"
	EvolutionDefinitionRecord               EvolutionStateFamily = "definition_record"
	EvolutionDependencyCurrent              EvolutionStateFamily = "dependency_current"
	EvolutionTemplateCurrent                EvolutionStateFamily = "template_current"
	EvolutionLinkRecord                     EvolutionStateFamily = "link_record"
	EvolutionPlanRecord                     EvolutionStateFamily = "plan_record"
	EvolutionEdgeRecord                     EvolutionStateFamily = "edge_record"
	EvolutionRolloutCurrent                 EvolutionStateFamily = "rollout_current"
	EvolutionRolloutEvidenceCurrent         EvolutionStateFamily = "rollout_evidence_current"
	EvolutionRolloutDecision                EvolutionStateFamily = "rollout_decision"
	EvolutionOccurrenceCurrent              EvolutionStateFamily = "occurrence_current"
	EvolutionSelectionCurrent               EvolutionStateFamily = "selection_current"
	EvolutionMigrationRecord                EvolutionStateFamily = "migration_record"
	EvolutionRestartRecord                  EvolutionStateFamily = "restart_record"
	EvolutionShadowRecord                   EvolutionStateFamily = "shadow_record"
	EvolutionShadowSubjectCurrent           EvolutionStateFamily = "shadow_subject_current"
	EvolutionObservationRecord              EvolutionStateFamily = "observation_record"
	EvolutionObservationOccurrenceCurrent   EvolutionStateFamily = "observation_occurrence_current"
	EvolutionEvidenceCurrent                EvolutionStateFamily = "evidence_current"
	EvolutionDecisionTransitionCurrent      EvolutionStateFamily = "decision_transition_current"
	EvolutionTransitionRecord               EvolutionStateFamily = "transition_record"
)

var evolutionStateFamilies = []EvolutionStateFamily{
	EvolutionDefinitionCurrent,
	EvolutionDefinitionCompatibilityCurrent,
	EvolutionDefinitionRecord,
	EvolutionDependencyCurrent,
	EvolutionTemplateCurrent,
	EvolutionLinkRecord,
	EvolutionPlanRecord,
	EvolutionEdgeRecord,
	EvolutionRolloutCurrent,
	EvolutionRolloutEvidenceCurrent,
	EvolutionRolloutDecision,
	EvolutionOccurrenceCurrent,
	EvolutionSelectionCurrent,
	EvolutionMigrationRecord,
	EvolutionRestartRecord,
	EvolutionShadowRecord,
	EvolutionShadowSubjectCurrent,
	EvolutionObservationRecord,
	EvolutionObservationOccurrenceCurrent,
	EvolutionEvidenceCurrent,
	EvolutionDecisionTransitionCurrent,
	EvolutionTransitionRecord,
}

// EvolutionMutationWrite binds one ordered normalized durable write.
type EvolutionMutationWrite struct {
	Family     EvolutionStateFamily `json:"family"`
	StorageKey string               `json:"storage_key"`
	ValueID    string               `json:"value_id"`
}

// EvolutionPersistenceCommand is the exact semantic command stored by Durable.
type EvolutionPersistenceCommand struct {
	PersistenceVersion string               `json:"persistence_version"`
	PersistenceID      string               `json:"persistence_id"`
	EvolutionID        string               `json:"evolution_id"`
	Command            LiveEvolutionCommand `json:"command"`
}

// UnmarshalJSON preserves every required persistence-command member.
func (command *EvolutionPersistenceCommand) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("evolution persistence command is not an object")
	}
	if err := requireExactJSONFields(object, []string{"persistence_version", "persistence_id", "evolution_id", "command"}); err != nil {
		return err
	}
	type wire EvolutionPersistenceCommand
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedCommand := EvolutionPersistenceCommand(decoded)
	if err := decodedCommand.validate(); err != nil {
		return err
	}
	*command = decodedCommand
	return nil
}

func (command EvolutionPersistenceCommand) validate() error {
	if command.PersistenceVersion != "cymule.evolution-persistence-command/4" ||
		!validSHA256ID(command.PersistenceID) || !validWireIdentity(command.EvolutionID) {
		return fmt.Errorf("evolution persistence command identity is invalid")
	}
	return validateLiveEvolutionCommandSemantics(command.Command)
}

// EvolutionPersistenceReceipt is the stable semantic result of one command.
type EvolutionPersistenceReceipt struct {
	ReceiptVersion  string                      `json:"receipt_version"`
	ReceiptID       string                      `json:"receipt_id"`
	Command         EvolutionPersistenceCommand `json:"command"`
	ParentCurrentID *string                     `json:"parent_current_id"`
	SourceWitnessID *string                     `json:"source_witness_id"`
	Outcome         LiveEvolutionOutcome        `json:"outcome"`
	Mutations       []EvolutionMutationWrite    `json:"mutations"`
	MutationID      string                      `json:"mutation_id"`
}

// UnmarshalJSON distinguishes required null from an omitted nullable member.
func (receipt *EvolutionPersistenceReceipt) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("evolution persistence receipt is not an object")
	}
	if err := requireExactJSONFields(object, []string{"receipt_version", "receipt_id", "command", "parent_current_id", "source_witness_id", "outcome", "mutations", "mutation_id"}); err != nil {
		return err
	}
	type wire EvolutionPersistenceReceipt
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedReceipt := EvolutionPersistenceReceipt(decoded)
	if err := decodedReceipt.validate(); err != nil {
		return err
	}
	*receipt = decodedReceipt
	return nil
}

func (receipt EvolutionPersistenceReceipt) validate() error {
	if receipt.ReceiptVersion != "cymule.evolution-persistence-receipt/4" ||
		!validSHA256ID(receipt.ReceiptID) || !validSHA256ID(receipt.MutationID) {
		return fmt.Errorf("evolution persistence receipt identity is invalid")
	}
	if receipt.ParentCurrentID != nil && !validSHA256ID(*receipt.ParentCurrentID) {
		return fmt.Errorf("evolution persistence receipt parent is invalid")
	}
	if receipt.SourceWitnessID != nil && !validSHA256ID(*receipt.SourceWitnessID) {
		return fmt.Errorf("evolution persistence receipt source witness is invalid")
	}
	if err := receipt.Command.validate(); err != nil {
		return err
	}
	if err := receipt.Outcome.validate(); err != nil {
		return err
	}
	consumesSource := receipt.Command.Command.Operation == "apply" &&
		receipt.Command.Command.Command != nil &&
		(receipt.Command.Command.Command.Operation == "migrate" ||
			receipt.Command.Command.Command.Operation == "restart_under_new_plan")
	if consumesSource != (receipt.SourceWitnessID != nil) {
		return fmt.Errorf("evolution receipt source witness does not match its command")
	}
	if err := validateLiveEvolutionOutcomeForCommand(receipt.Command.Command, receipt.Outcome); err != nil {
		return err
	}
	if len(receipt.Mutations) > 8192 {
		return fmt.Errorf("evolution receipt exceeds the mutation bound")
	}
	previousFamily := -1
	previousKey := ""
	for _, mutation := range receipt.Mutations {
		family := slices.Index(evolutionStateFamilies, mutation.Family)
		if family < 0 || !validSHA256ID(mutation.StorageKey) || !validSHA256ID(mutation.ValueID) {
			return fmt.Errorf("evolution mutation write is invalid")
		}
		if family < previousFamily || (family == previousFamily && mutation.StorageKey <= previousKey) {
			return fmt.Errorf("evolution mutation writes are not strictly key ordered")
		}
		previousFamily = family
		previousKey = mutation.StorageKey
	}
	return nil
}

// EvolutionCommit is the physical revision observation for a semantic receipt.
type EvolutionCommit struct {
	ObservedRevision  string                      `json:"observed_revision"`
	CommittedRevision *string                     `json:"committed_revision"`
	Receipt           EvolutionPersistenceReceipt `json:"receipt"`
}

// UnmarshalJSON distinguishes required null from an omitted committed revision.
func (commit *EvolutionCommit) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("evolution commit is not an object")
	}
	if err := requireExactJSONFields(object, []string{"observed_revision", "committed_revision", "receipt"}); err != nil {
		return err
	}
	type wire EvolutionCommit
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedCommit := EvolutionCommit(decoded)
	if err := decodedCommit.validate(); err != nil {
		return err
	}
	*commit = decodedCommit
	return nil
}

func (commit EvolutionCommit) validate() error {
	if !validSHA256ID(commit.ObservedRevision) ||
		(commit.CommittedRevision != nil &&
			(!validSHA256ID(*commit.CommittedRevision) || *commit.CommittedRevision != commit.ObservedRevision)) {
		return fmt.Errorf("evolution commit revision is invalid")
	}
	return commit.Receipt.validate()
}

func validateSubflowRevisionRaw(raw json.RawMessage) error {
	var revision struct {
		RevisionVersion string            `json:"revision_version"`
		RevisionID      string            `json:"revision_id"`
		LogicalRef      string            `json:"logical_ref"`
		Sequence        json.RawMessage   `json:"sequence"`
		Definition      json.RawMessage   `json:"definition"`
		References      []json.RawMessage `json:"references"`
	}
	if err := validateClosedRaw(raw, []string{"revision_version", "revision_id", "logical_ref", "sequence", "definition", "references"}, &revision); err != nil {
		return err
	}
	if revision.RevisionVersion != "cymule.subflow-revision/2" || !validSHA256ID(revision.RevisionID) || !validRegistryIdentity(revision.LogicalRef) {
		return fmt.Errorf("subflow revision identity is invalid")
	}
	if _, err := validateSafeUintRaw(revision.Sequence, true); err != nil {
		return fmt.Errorf("subflow revision sequence is invalid")
	}
	definitionID, err := validateDefinitionRaw(revision.Definition)
	if err != nil {
		return err
	}
	return validatePublishedSubflowReferencesRaw(revision.References, definitionID)
}

func validatePublishedSubflowReferences(
	references []SubflowReference,
	definitionID string,
) error {
	if references == nil {
		return fmt.Errorf("publication references are required")
	}
	encoded, err := marshalStrictJSONValue(references)
	if err != nil {
		return err
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(encoded, &raw); err != nil {
		return err
	}
	return validatePublishedSubflowReferencesRaw(raw, definitionID)
}

func validatePublishedSubflowReferencesRaw(
	references []json.RawMessage,
	definitionID string,
) error {
	if references == nil || len(references) > 1024 {
		return fmt.Errorf("publication references are outside bounds")
	}
	encoded, err := marshalStrictJSONValue(references)
	if err != nil {
		return err
	}
	if len(encoded) > 1024*1024 {
		return fmt.Errorf("publication references are outside bounds")
	}
	localDefinitions := map[string]struct{}{definitionID: {}}
	previousLogicalRef := ""
	for index, reference := range references {
		logicalRef, localDefinition, strategy, err := validateSubflowReferenceRaw(reference)
		if err != nil {
			return err
		}
		if strategy != "pinned" {
			return fmt.Errorf("publication reference strategy must be pinned")
		}
		if index > 0 && previousLogicalRef >= logicalRef {
			return fmt.Errorf("publication references are not strictly ordered")
		}
		if _, exists := localDefinitions[localDefinition]; exists {
			return fmt.Errorf("publication repeats a local definition")
		}
		previousLogicalRef = logicalRef
		localDefinitions[localDefinition] = struct{}{}
	}
	return nil
}

func validateDefinitionRaw(raw json.RawMessage) (string, error) {
	var definition struct {
		ID           string          `json:"id"`
		InputSchema  json.RawMessage `json:"input_schema"`
		OutputSchema json.RawMessage `json:"output_schema"`
		Body         Region          `json:"body"`
	}
	if err := validateClosedRaw(raw, []string{"id", "input_schema", "output_schema", "body"}, &definition); err != nil {
		return "", err
	}
	if !validRegistryName(definition.ID) || !validJSONValueRaw(definition.InputSchema) || !validJSONValueRaw(definition.OutputSchema) {
		return "", fmt.Errorf("subflow definition is invalid")
	}
	if err := validateRegistryRegionWire(definition.Body); err != nil {
		return "", err
	}
	return definition.ID, nil
}

func validateSubflowReferenceRaw(raw json.RawMessage) (string, string, string, error) {
	var reference struct {
		LogicalRef      string          `json:"logical_ref"`
		LocalDefinition string          `json:"local_definition"`
		InputSchema     json.RawMessage `json:"input_schema"`
		OutputSchema    json.RawMessage `json:"output_schema"`
		Strategy        json.RawMessage `json:"strategy"`
	}
	if err := validateClosedRaw(raw, []string{"logical_ref", "local_definition", "input_schema", "output_schema", "strategy"}, &reference); err != nil {
		return "", "", "", err
	}
	if !validRegistryName(reference.LogicalRef) || !validRegistryName(reference.LocalDefinition) || !validJSONValueRaw(reference.InputSchema) || !validJSONValueRaw(reference.OutputSchema) {
		return "", "", "", fmt.Errorf("subflow reference is invalid")
	}
	var strategy map[string]json.RawMessage
	value, err := decodeUniqueJSON(reference.Strategy)
	if err != nil {
		return "", "", "", err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return "", "", "", fmt.Errorf("subflow reference strategy is invalid")
	}
	kind, ok := object["strategy"].(string)
	if !ok {
		return "", "", "", fmt.Errorf("subflow reference strategy is invalid")
	}
	fields := []string{"strategy"}
	if kind == "pinned" {
		fields = append(fields, "revision_id")
	} else if kind != "latest_compatible" {
		return "", "", "", fmt.Errorf("subflow reference strategy is invalid")
	}
	if err := requireExactJSONFields(object, fields); err != nil {
		return "", "", "", err
	}
	if err := json.Unmarshal(reference.Strategy, &strategy); err != nil {
		return "", "", "", err
	}
	if kind == "pinned" {
		var revisionID string
		if err := json.Unmarshal(strategy["revision_id"], &revisionID); err != nil || !validSHA256ID(revisionID) {
			return "", "", "", fmt.Errorf("pinned subflow revision identity is invalid")
		}
	}
	return reference.LogicalRef, reference.LocalDefinition, kind, nil
}

func validJSONValueRaw(raw json.RawMessage) bool {
	_, err := decodeUniqueJSON(raw)
	return err == nil
}

func validateLinkedPlanRaw(raw json.RawMessage) error {
	var linked struct {
		TemplateID        string            `json:"template_id"`
		Plan              SealedPlan        `json:"plan"`
		ResolvedRevisions map[string]string `json:"resolved_revisions"`
	}
	if err := validateClosedRaw(raw, []string{"template_id", "plan", "resolved_revisions"}, &linked); err != nil {
		return err
	}
	if !validWireIdentity(linked.TemplateID) || linked.ResolvedRevisions == nil {
		return fmt.Errorf("linked Plan fields are invalid")
	}
	for logicalRef, revisionID := range linked.ResolvedRevisions {
		if !validWireIdentity(logicalRef) || !validSHA256ID(revisionID) {
			return fmt.Errorf("resolved revision identity is invalid")
		}
	}
	return nil
}

func validatePublicationReceiptRaw(raw json.RawMessage) error {
	var receipt struct {
		Revision json.RawMessage   `json:"revision"`
		Updates  []json.RawMessage `json:"updates"`
	}
	if err := validateClosedRaw(raw, []string{"revision", "updates"}, &receipt); err != nil {
		return err
	}
	if err := validateSubflowRevisionRaw(receipt.Revision); err != nil {
		return err
	}
	if receipt.Updates == nil {
		return fmt.Errorf("publication updates are invalid")
	}
	previousTemplate := ""
	for _, rawUpdate := range receipt.Updates {
		var update struct {
			TemplateID     string          `json:"template_id"`
			PreviousPlanID string          `json:"previous_plan_id"`
			CurrentPlanID  string          `json:"current_plan_id"`
			DecisionID     json.RawMessage `json:"decision_id"`
			Advanced       json.RawMessage `json:"advanced"`
		}
		if err := validateClosedRaw(rawUpdate, []string{"template_id", "previous_plan_id", "current_plan_id", "decision_id", "advanced"}, &update); err != nil {
			return err
		}
		if !validWireIdentity(update.TemplateID) || !validSHA256ID(update.PreviousPlanID) || !validSHA256ID(update.CurrentPlanID) {
			return fmt.Errorf("publication update identity is invalid")
		}
		if previousTemplate != "" && previousTemplate >= update.TemplateID {
			return fmt.Errorf("publication updates are not strictly template-ordered")
		}
		previousTemplate = update.TemplateID
		decisionID, hasDecision, err := decodeNullableNonEmptyStringRaw(update.DecisionID)
		if err != nil {
			return fmt.Errorf("publication decision identity is invalid")
		}
		advanced, err := decodeBoolRaw(update.Advanced)
		if err != nil {
			return fmt.Errorf("publication update disposition is invalid")
		}
		validAdvance := advanced && hasDecision && validSHA256ID(decisionID) && update.PreviousPlanID != update.CurrentPlanID
		validNoAdvance := !advanced && !hasDecision && update.PreviousPlanID == update.CurrentPlanID
		if !validAdvance && !validNoAdvance {
			return fmt.Errorf("publication update does not match its Plan advance")
		}
	}
	return nil
}

func validatePlanEdgeRaw(raw json.RawMessage) error {
	var edge struct {
		EdgeID     string            `json:"edge_id"`
		FromPlan   string            `json:"from_plan"`
		ToPlan     string            `json:"to_plan"`
		Operations []json.RawMessage `json:"operations"`
	}
	if err := validateClosedRaw(raw, []string{"edge_id", "from_plan", "to_plan", "operations"}, &edge); err != nil {
		return err
	}
	if !validSHA256ID(edge.EdgeID) || !validSHA256ID(edge.FromPlan) || !validSHA256ID(edge.ToPlan) || edge.FromPlan == edge.ToPlan || len(edge.Operations) == 0 {
		return fmt.Errorf("Plan edge identity is invalid")
	}
	previousTarget, previousKind := "", ""
	for _, rawOperation := range edge.Operations {
		var operation struct {
			Kind   string          `json:"kind"`
			Target string          `json:"target"`
			Before json.RawMessage `json:"before"`
			After  json.RawMessage `json:"after"`
		}
		if err := validateClosedRaw(rawOperation, []string{"kind", "target", "before", "after"}, &operation); err != nil {
			return err
		}
		if !validWireIdentity(operation.Target) {
			return fmt.Errorf("Plan edge operation identity is invalid")
		}
		before, hasBefore, err := decodeNullableNonEmptyStringRaw(operation.Before)
		if err != nil {
			return fmt.Errorf("Plan edge prior identity is invalid")
		}
		after, hasAfter, err := decodeNullableNonEmptyStringRaw(operation.After)
		if err != nil {
			return fmt.Errorf("Plan edge target identity is invalid")
		}
		validShape := (operation.Kind == "add" && !hasBefore && hasAfter && validLowerHexDigest(after)) ||
			(operation.Kind == "remove" && hasBefore && validLowerHexDigest(before) && !hasAfter) ||
			(operation.Kind == "replace" && hasBefore && validLowerHexDigest(before) && hasAfter && validLowerHexDigest(after) && before != after)
		if !validShape {
			return fmt.Errorf("Plan edge operation is malformed")
		}
		if previousTarget != "" && (previousTarget > operation.Target || (previousTarget == operation.Target && previousKind >= operation.Kind)) {
			return fmt.Errorf("Plan edge operations are not in canonical order")
		}
		previousTarget, previousKind = operation.Target, operation.Kind
	}
	return nil
}

func validateMigrationReceiptRaw(raw json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, []string{
		"request", "source_witness_id", "source_binding", "target_binding",
		"source_execution_fence", "target_epoch", "adapter_id", "adapter_revision",
		"from_schema", "to_schema", "output_state", "target_continuation", "evidence",
	}, &object); err != nil {
		return err
	}
	values := make(map[string]string, 5)
	for _, name := range []string{"source_witness_id", "adapter_id", "adapter_revision", "from_schema", "to_schema"} {
		var value string
		if err := json.Unmarshal(object[name], &value); err != nil {
			return fmt.Errorf("migration receipt %s is invalid", name)
		}
		valid := validWireIdentity(value)
		if name == "source_witness_id" || name == "adapter_revision" {
			valid = validSHA256ID(value)
		}
		if !valid {
			return fmt.Errorf("migration receipt %s is invalid", name)
		}
		values[name] = value
	}
	if err := validateMigrationRequestRaw(object["request"]); err != nil {
		return err
	}
	var request MigrationRequest
	if err := decodeClosedJSON(object["request"], &request); err != nil {
		return err
	}
	sourceExecutionFence, err := validateSafeUintRaw(object["source_execution_fence"], false)
	if err != nil {
		return fmt.Errorf("migration source execution fence is invalid")
	}
	targetEpoch, err := validateSafeUintRaw(object["target_epoch"], true)
	if err != nil {
		return fmt.Errorf("migration target epoch is invalid")
	}
	for _, name := range []string{"source_binding", "target_binding", "output_state", "evidence"} {
		if err := validateArtifactRefRaw(object[name]); err != nil {
			return err
		}
	}
	var targetBinding ArtifactRef
	for _, name := range []string{"source_binding", "target_binding"} {
		var binding ArtifactRef
		if err := decodeClosedJSON(object[name], &binding); err != nil || binding.Kind != "cymule.execution-binding/2" {
			return fmt.Errorf("migration receipt %s is not an ExecutionBinding Artifact", name)
		}
		if name == "target_binding" {
			targetBinding = binding
		}
	}
	if err := validateMigrationContinuationRaw(object["target_continuation"]); err != nil {
		return err
	}
	var outputState ArtifactRef
	if err := decodeClosedJSON(object["output_state"], &outputState); err != nil {
		return err
	}
	var target MigrationContinuation
	if err := decodeClosedJSON(object["target_continuation"], &target); err != nil {
		return err
	}
	consistentTarget := request.ExpectedSourceEpoch < maxExactInteger && targetEpoch == request.ExpectedSourceEpoch+1 &&
		values["adapter_id"] == request.AdapterID && values["adapter_revision"] == request.AdapterRevision &&
		target.RunID == request.RunID && target.PlanID == request.ToPlan &&
		target.BindingContext == targetBinding.ArtifactID && target.Epoch == targetEpoch &&
		target.State != nil && *target.State == outputState && target.Status == "ready" &&
		target.ExecutionFence == sourceExecutionFence && target.ExecutionClaim == nil &&
		len(target.Frames) > 0 && len(target.WaitSet) == 0 && slices.Equal(target.ScopeStack, []string{"scope:root"})
	if !consistentTarget {
		return fmt.Errorf("migration receipt target Continuation does not match its request and output")
	}
	return nil
}

func validateRestartReceiptRaw(raw json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, []string{"request", "source_witness_id", "target_plan"}, &object); err != nil {
		return err
	}
	if err := validateRestartRequestRaw(object["request"]); err != nil {
		return err
	}
	var request RestartRequest
	if err := decodeClosedJSON(object["request"], &request); err != nil {
		return err
	}
	var sourceWitnessID string
	if err := json.Unmarshal(object["source_witness_id"], &sourceWitnessID); err != nil || !validSHA256ID(sourceWitnessID) {
		return fmt.Errorf("restart source witness identity is invalid")
	}
	var targetPlan SealedPlan
	if err := decodeClosedJSON(object["target_plan"], &targetPlan); err != nil {
		return err
	}
	if targetPlan.PlanID != request.ToPlan {
		return fmt.Errorf("restart target Plan does not match its request")
	}
	return nil
}

func validateMigrationRequestRaw(raw json.RawMessage) error {
	var request MigrationRequest
	if err := validateClosedRaw(raw, []string{
		"migration_id", "run_id", "from_plan", "to_plan", "plan_edge_id",
		"compatibility_id", "expected_source_epoch", "adapter_id", "adapter_revision",
	}, &request); err != nil {
		return err
	}
	if !rawMessageMatchesTyped(raw, request) {
		return fmt.Errorf("migration request loses JSON member presence")
	}
	return validateMigrationRequest(request)
}

func validateRestartRequestRaw(raw json.RawMessage) error {
	var request RestartRequest
	if err := validateClosedRaw(raw, []string{
		"restart_id", "replacement_run", "run_id", "from_plan",
		"expected_source_epoch", "to_plan", "input", "evidence",
	}, &request); err != nil {
		return err
	}
	if !rawMessageMatchesTyped(raw, request) {
		return fmt.Errorf("restart request loses JSON member presence")
	}
	return validateRestartRequest(request)
}

func validateMigrationContinuationRaw(raw json.RawMessage) error {
	fields := []string{"continuation_version", "run_id", "plan_id", "binding_context", "frames", "state", "wait_set", "scope_stack", "epoch", "execution_fence", "execution_claim", "status"}
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, fields, &object); err != nil {
		return err
	}
	var continuation MigrationContinuation
	if err := decodeClosedJSON(raw, &continuation); err != nil {
		return err
	}
	if err := validateMigrationContinuation(continuation); err != nil {
		return err
	}
	for _, name := range []string{"epoch", "execution_fence"} {
		if _, err := validateSafeUintRaw(object[name], false); err != nil {
			return fmt.Errorf("migration Continuation %s is invalid", name)
		}
	}
	for _, name := range []string{"wait_set", "scope_stack"} {
		var identities []string
		if err := json.Unmarshal(object[name], &identities); err != nil || identities == nil {
			return fmt.Errorf("migration Continuation %s is invalid", name)
		}
		for _, identity := range identities {
			if identity == "" {
				return fmt.Errorf("migration Continuation %s identity is invalid", name)
			}
		}
	}
	claim, err := decodeUniqueJSON(object["execution_claim"])
	if err != nil || claim != nil {
		return fmt.Errorf("migration Continuation execution claim is invalid")
	}
	state, err := decodeUniqueJSON(object["state"])
	if err != nil {
		return err
	}
	if state != nil {
		if err := validateArtifactRefRaw(object["state"]); err != nil {
			return err
		}
	}
	var frames []json.RawMessage
	if err := json.Unmarshal(object["frames"], &frames); err != nil || frames == nil {
		return fmt.Errorf("migration Continuation frames are invalid")
	}
	for _, frame := range frames {
		if err := validateMigrationFrameRaw(frame); err != nil {
			return err
		}
	}
	return nil
}

func validateMigrationFrameRaw(raw json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, []string{"definition_id", "invocation_id", "invocation_path", "scope_id", "input", "region_path", "next_step", "locals"}, &object); err != nil {
		return err
	}
	for _, name := range []string{"definition_id", "invocation_id", "scope_id"} {
		var value string
		if err := json.Unmarshal(object[name], &value); err != nil || value == "" {
			return fmt.Errorf("migration frame %s is invalid", name)
		}
	}
	if err := validateArtifactRefRaw(object["input"]); err != nil {
		return err
	}
	if _, err := validateSafeUintRaw(object["next_step"], false); err != nil {
		return fmt.Errorf("migration frame next step is invalid")
	}
	if err := validateSafeUintArrayRaw(object["region_path"]); err != nil {
		return fmt.Errorf("migration frame Region path is invalid")
	}
	var locals map[string]json.RawMessage
	if err := json.Unmarshal(object["locals"], &locals); err != nil || locals == nil {
		return fmt.Errorf("migration frame locals are invalid")
	}
	for _, reference := range locals {
		if err := validateArtifactRefRaw(reference); err != nil {
			return err
		}
	}
	var invocationPath []json.RawMessage
	if err := json.Unmarshal(object["invocation_path"], &invocationPath); err != nil || invocationPath == nil {
		return fmt.Errorf("migration invocation path is invalid")
	}
	for _, rawSegment := range invocationPath {
		var segment map[string]json.RawMessage
		if err := validateClosedRaw(rawSegment, []string{"site_id", "region_path", "scope_id"}, &segment); err != nil {
			return err
		}
		for _, name := range []string{"site_id", "scope_id"} {
			var value string
			if err := json.Unmarshal(segment[name], &value); err != nil || value == "" {
				return fmt.Errorf("migration invocation %s is invalid", name)
			}
		}
		if err := validateSafeUintArrayRaw(segment["region_path"]); err != nil {
			return fmt.Errorf("migration invocation Region path is invalid")
		}
	}
	return nil
}

func validateSafeUintArrayRaw(raw json.RawMessage) error {
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return fmt.Errorf("value is not an integer array")
	}
	for _, value := range values {
		if _, err := validateSafeUintRaw(value, false); err != nil {
			return err
		}
	}
	return nil
}

func validateShadowComparisonRaw(raw json.RawMessage) error {
	var comparison struct {
		ComparisonID     string          `json:"comparison_id"`
		Subject          string          `json:"subject"`
		DecisionID       string          `json:"decision_id"`
		PrimaryPlan      string          `json:"primary_plan"`
		ShadowPlan       string          `json:"shadow_plan"`
		DriverID         string          `json:"driver_id"`
		DriverRevision   string          `json:"driver_revision"`
		ComparisonPolicy string          `json:"comparison_policy"`
		PrimaryDigest    string          `json:"primary_digest"`
		ShadowDigest     string          `json:"shadow_digest"`
		Equivalent       json.RawMessage `json:"equivalent"`
		Evidence         json.RawMessage `json:"evidence"`
	}
	if err := validateClosedRaw(raw, []string{"comparison_id", "subject", "decision_id", "primary_plan", "shadow_plan", "driver_id", "driver_revision", "comparison_policy", "primary_digest", "shadow_digest", "equivalent", "evidence"}, &comparison); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"comparison_id": comparison.ComparisonID, "subject": comparison.Subject,
		"decision_id": comparison.DecisionID, "primary_plan": comparison.PrimaryPlan,
		"shadow_plan": comparison.ShadowPlan, "driver_id": comparison.DriverID,
		"driver_revision": comparison.DriverRevision, "comparison_policy": comparison.ComparisonPolicy,
		"primary_digest": comparison.PrimaryDigest, "shadow_digest": comparison.ShadowDigest,
	} {
		valid := validWireIdentity(value)
		if name == "primary_plan" || name == "shadow_plan" {
			valid = validSHA256ID(value)
		} else if name == "driver_revision" {
			valid = validSHA256ID(value)
		} else if name == "primary_digest" || name == "shadow_digest" {
			valid = validLowerHexDigest(value)
		}
		if !valid {
			return fmt.Errorf("shadow comparison %s is invalid", name)
		}
	}
	if comparison.PrimaryPlan == comparison.ShadowPlan {
		return fmt.Errorf("shadow comparison Plans must be distinct")
	}
	if err := validateBoolRaw(comparison.Equivalent); err != nil {
		return fmt.Errorf("shadow comparison result is invalid")
	}
	return validateArtifactRefRaw(comparison.Evidence)
}

func validateRolloutTransitionRaw(raw json.RawMessage) error {
	var transition struct {
		TransitionID string          `json:"transition_id"`
		FromDecision string          `json:"from_decision"`
		ToDecision   string          `json:"to_decision"`
		Evaluation   json.RawMessage `json:"evaluation"`
	}
	if err := validateClosedRaw(raw, []string{"transition_id", "from_decision", "to_decision", "evaluation"}, &transition); err != nil {
		return err
	}
	if !validSHA256ID(transition.TransitionID) || !validWireIdentity(transition.FromDecision) || !validWireIdentity(transition.ToDecision) || transition.FromDecision == transition.ToDecision {
		return fmt.Errorf("rollout transition identity is invalid")
	}
	var evaluation struct {
		EvaluationID       string          `json:"evaluation_id"`
		Gate               json.RawMessage `json:"gate"`
		TargetObservations json.RawMessage `json:"target_observations"`
		TargetFailures     json.RawMessage `json:"target_failures"`
		EquivalentShadows  json.RawMessage `json:"equivalent_shadows"`
		Inequivalent       json.RawMessage `json:"inequivalent_shadows"`
		Outcome            string          `json:"outcome"`
		EvidenceIDs        []string        `json:"evidence_ids"`
	}
	if err := validateClosedRaw(transition.Evaluation, []string{"evaluation_id", "gate", "target_observations", "target_failures", "equivalent_shadows", "inequivalent_shadows", "outcome", "evidence_ids"}, &evaluation); err != nil {
		return err
	}
	if !validSHA256ID(evaluation.EvaluationID) || !slices.Contains([]string{"pending", "promote", "rollback"}, evaluation.Outcome) || evaluation.EvidenceIDs == nil {
		return fmt.Errorf("rollout evaluation is invalid")
	}
	counts := make([]uint64, 0, 4)
	for _, count := range []json.RawMessage{evaluation.TargetObservations, evaluation.TargetFailures, evaluation.EquivalentShadows, evaluation.Inequivalent} {
		value, err := validateSafeUintRaw(count, false)
		if err != nil {
			return fmt.Errorf("rollout evaluation count is invalid")
		}
		counts = append(counts, value)
	}
	seenEvidence := make(map[string]struct{}, len(evaluation.EvidenceIDs))
	for _, evidenceID := range evaluation.EvidenceIDs {
		if !validWireIdentity(evidenceID) {
			return fmt.Errorf("rollout evidence identity is invalid")
		}
		if _, duplicate := seenEvidence[evidenceID]; duplicate {
			return fmt.Errorf("rollout evidence identities are not unique")
		}
		seenEvidence[evidenceID] = struct{}{}
	}
	if err := validateRolloutGateRaw(evaluation.Gate); err != nil {
		return err
	}
	var gate RolloutGate
	if err := decodeClosedJSON(evaluation.Gate, &gate); err != nil {
		return err
	}
	if gate.DecisionID != transition.FromDecision {
		return fmt.Errorf("rollout transition does not match its gate decision")
	}
	targetObservations, targetFailures, equivalentShadows, inequivalentShadows := counts[0], counts[1], counts[2], counts[3]
	if targetFailures > targetObservations {
		return fmt.Errorf("rollout failures exceed target observations")
	}
	evidenceCount := targetObservations + equivalentShadows + inequivalentShadows
	if evidenceCount != uint64(len(evaluation.EvidenceIDs)) {
		return fmt.Errorf("rollout evidence counts do not match its identity set")
	}
	expectedOutcome := "pending"
	if targetFailures > gate.MaxTargetFailures || inequivalentShadows > gate.MaxInequivalentShadows {
		expectedOutcome = "rollback"
	} else if targetObservations >= gate.MinTargetObservations && equivalentShadows >= gate.MinEquivalentShadows {
		expectedOutcome = "promote"
	}
	if evaluation.Outcome != expectedOutcome || evaluation.Outcome == "pending" {
		return fmt.Errorf("rollout outcome does not match its exact evidence")
	}
	return nil
}

func validateRolloutGateRaw(raw json.RawMessage) error {
	var gate struct {
		GateID                 string          `json:"gate_id"`
		DecisionID             string          `json:"decision_id"`
		MinTargetObservations  json.RawMessage `json:"min_target_observations"`
		MaxTargetFailures      json.RawMessage `json:"max_target_failures"`
		MinEquivalentShadows   json.RawMessage `json:"min_equivalent_shadows"`
		MaxInequivalentShadows json.RawMessage `json:"max_inequivalent_shadows"`
	}
	if err := validateClosedRaw(raw, []string{"gate_id", "decision_id", "min_target_observations", "max_target_failures", "min_equivalent_shadows", "max_inequivalent_shadows"}, &gate); err != nil {
		return err
	}
	if !validWireIdentity(gate.GateID) || !validWireIdentity(gate.DecisionID) {
		return fmt.Errorf("rollout gate identity is invalid")
	}
	for _, count := range []json.RawMessage{gate.MinTargetObservations, gate.MaxTargetFailures, gate.MinEquivalentShadows, gate.MaxInequivalentShadows} {
		if _, err := validateSafeUintRaw(count, false); err != nil {
			return fmt.Errorf("rollout gate count is invalid")
		}
	}
	return nil
}

func validateArtifactRefRaw(raw json.RawMessage) error {
	var reference ArtifactRef
	if err := validateClosedRaw(raw, []string{"identity_version", "artifact_id", "kind"}, &reference); err != nil {
		return err
	}
	return validateArtifactRef(reference)
}

func validateNullableNonEmptyStringRaw(raw json.RawMessage) error {
	_, _, err := decodeNullableNonEmptyStringRaw(raw)
	return err
}

func decodeNullableNonEmptyStringRaw(raw json.RawMessage) (string, bool, error) {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return "", false, err
	}
	if value == nil {
		return "", false, nil
	}
	text, ok := value.(string)
	if !ok || text == "" {
		return "", false, fmt.Errorf("value is not null or a non-empty string")
	}
	return text, true, nil
}

func validateBoolRaw(raw json.RawMessage) error {
	_, err := decodeBoolRaw(raw)
	return err
}

func decodeBoolRaw(raw json.RawMessage) (bool, error) {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return false, err
	}
	decoded, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("value is not a boolean")
	}
	return decoded, nil
}

func validateSafeUintRaw(raw json.RawMessage, positive bool) (uint64, error) {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return 0, err
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("value is not an unsigned integer")
	}
	return parseSafeJSONUint(number, positive)
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
		"effect_unavailable": {"status", "intent_id"},
		"effect_not_applied": {"status", "intent_id"},
		"release_required":   {"status", "intent_ids"}, "completed": {"status", "result"},
		"failed": {"status", "failure"}, "cancelled": {"status", "reason"},
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
		if err := validateClosedRaw(object["result"], []string{"run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"}, &result); err != nil {
			return err
		}
		if !validRunIdentity(result.RunID) || !validSHA256ID(result.PlanID) ||
			!validBareSHA256(result.ProjectionDigest) ||
			!validPreconditionToken(result.PreconditionToken) || result.Effects == nil {
			return fmt.Errorf("completed durable boundary is malformed")
		}
		return validateStrictlySortedContentIDs(result.Effects, "effect intent")
	}
	if tag.Status == "failed" {
		return validateRunFailureRaw(object["failure"])
	}
	if tag.Status == "cancelled" {
		var reason ArtifactRef
		if err := validateClosedRaw(object["reason"], []string{"identity_version", "artifact_id", "kind"}, &reason); err != nil {
			return err
		}
		return validateArtifactRef(reason)
	}
	if tag.Status == "release_required" {
		var intentIDs []string
		if err := json.Unmarshal(object["intent_ids"], &intentIDs); err != nil || len(intentIDs) == 0 {
			return fmt.Errorf("release-required durable boundary is malformed")
		}
		return validateStrictlySortedContentIDs(intentIDs, "effect intent")
	}
	identityField := map[string]string{
		"suspended": "wait_id", "reconciliation_required": "intent_id", "effect_unavailable": "intent_id",
		"effect_not_applied": "intent_id",
	}[tag.Status]
	if identityField != "" {
		var identity string
		if err := json.Unmarshal(object[identityField], &identity); err != nil ||
			(tag.Status == "suspended" && !validWireIdentity(identity)) ||
			(tag.Status != "suspended" && !validSHA256ID(identity)) {
			return fmt.Errorf("durable boundary identity is invalid")
		}
	}
	return nil
}

func validateRunFailureRaw(raw json.RawMessage) error {
	var failure struct {
		Class  string      `json:"class"`
		Code   string      `json:"code"`
		Detail ArtifactRef `json:"detail"`
	}
	if err := validateClosedRaw(raw, []string{"class", "code", "detail"}, &failure); err != nil {
		return err
	}
	if !slices.Contains([]string{"declared_failure", "runtime_defect", "substrate"}, failure.Class) || !validFailureCode(failure.Code) {
		return fmt.Errorf("Run failure classification is invalid")
	}
	return validateArtifactRef(failure.Detail)
}

func validFailureCode(code string) bool {
	if len(code) == 0 || len(code) > 200 || code[0] < 'a' || code[0] > 'z' {
		return false
	}
	for _, character := range code[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validateDurableQuerySource(observedRevision, sourceRoot string) error {
	if !validSHA256ID(observedRevision) || !validLowerHexDigest(sourceRoot) {
		return fmt.Errorf("durable query source revision or root is invalid")
	}
	return nil
}

func durablePageKeyHash(canonicalKey string) string {
	hasher := sha256.New()
	frame := func(value []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		hasher.Write(length[:])
		hasher.Write(value)
	}
	frame([]byte("cymule.authenticated-collection-preimage/1"))
	frame([]byte("cymule.authenticated-map-key/1"))
	var fieldCount [8]byte
	binary.BigEndian.PutUint64(fieldCount[:], 1)
	frame(fieldCount[:])
	frame([]byte(canonicalKey))
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func validateDurablePageCursor(cursor *DurablePageCursor) error {
	if cursor == nil || !slices.Contains([]DurablePageQueryKind{
		DurableRunIndexQuery, DurableRunWaitsQuery, DurableRunEffectsQuery,
		DurableRunOccurrencesQuery, DurableRunAttemptsQuery,
	}, cursor.QueryKind) {
		return fmt.Errorf("durable page cursor query kind is invalid")
	}
	if cursor.QueryKind == DurableRunIndexQuery {
		if cursor.RunID != nil || !validRunIdentity(cursor.Position.CanonicalKey) {
			return fmt.Errorf("Run-index cursor owner or key is invalid")
		}
	} else if cursor.RunID == nil || !validRunIdentity(*cursor.RunID) ||
		!validSHA256ID(cursor.Position.CanonicalKey) {
		return fmt.Errorf("Run-scoped cursor owner or key is invalid")
	}
	if err := validateDurableQuerySource(cursor.SourceRevision, cursor.SourceRoot); err != nil {
		return err
	}
	if !validLowerHexDigest(cursor.Position.KeyHash) ||
		cursor.Position.KeyHash != durablePageKeyHash(cursor.Position.CanonicalKey) {
		return fmt.Errorf("durable page cursor position is invalid")
	}
	return nil
}

func durableSummaryKey(raw json.RawMessage, queryKind DurablePageQueryKind) (string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return "", err
	}
	field := map[DurablePageQueryKind]string{
		DurableRunIndexQuery:       "run_id",
		DurableRunWaitsQuery:       "wait_id",
		DurableRunEffectsQuery:     "intent_id",
		DurableRunOccurrencesQuery: "occurrence_id",
		DurableRunAttemptsQuery:    "attempt_id",
	}[queryKind]
	var key string
	if field == "" || json.Unmarshal(object[field], &key) != nil {
		return "", fmt.Errorf("durable query summary key is invalid")
	}
	return key, nil
}

func normalizedJSONSize(raw json.RawMessage) (int, error) {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return 0, err
	}
	return canonicalJSONSize(value)
}

func canonicalJSONSize(value any) (int, error) {
	switch value := value.(type) {
	case nil:
		return len("null"), nil
	case bool:
		if value {
			return len("true"), nil
		}
		return len("false"), nil
	case string:
		return canonicalJSONStringSize(value), nil
	case json.Number:
		integer, isInteger, err := mathematicalJSONInteger(value)
		if err != nil {
			return 0, err
		}
		if isInteger {
			return len(integer.String()), nil
		}
		floating, err := value.Float64()
		if err != nil || math.IsNaN(floating) || math.IsInf(floating, 0) {
			return 0, fmt.Errorf("canonical JSON number is invalid")
		}
		return len(formatJCSNumber(floating)), nil
	case []any:
		total := 2
		for index, member := range value {
			memberSize, err := canonicalJSONSize(member)
			if err != nil {
				return 0, err
			}
			if index > 0 {
				memberSize++
			}
			if err := addCanonicalJSONSize(&total, memberSize); err != nil {
				return 0, err
			}
		}
		return total, nil
	case map[string]any:
		total := 2
		index := 0
		for key, member := range value {
			memberSize, err := canonicalJSONSize(member)
			if err != nil {
				return 0, err
			}
			entrySize := canonicalJSONStringSize(key) + 1 + memberSize
			if index > 0 {
				entrySize++
			}
			if err := addCanonicalJSONSize(&total, entrySize); err != nil {
				return 0, err
			}
			index++
		}
		return total, nil
	default:
		return 0, fmt.Errorf("canonical JSON contains unsupported value %T", value)
	}
}

func canonicalJSONStringSize(value string) int {
	total := 2
	for _, character := range value {
		switch character {
		case '"', '\\', '\b', '\t', '\n', '\f', '\r':
			total += 2
		case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x0b, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
			0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
			total += len(`\u0000`)
		default:
			total += utf8.RuneLen(character)
		}
	}
	return total
}

func formatJCSNumber(value float64) string {
	if value == 0 {
		return "0"
	}
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}
	scientific := strconv.FormatFloat(value, 'e', -1, 64)
	mantissa, exponentText, _ := strings.Cut(scientific, "e")
	exponent, _ := strconv.Atoi(exponentText)
	digits := strings.ReplaceAll(mantissa, ".", "")
	n := exponent + 1
	switch {
	case len(digits) <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-len(digits))
	case 0 < n && n <= 21:
		return sign + digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits
	default:
		exponent = n - 1
		exponentSign := ""
		if exponent >= 0 {
			exponentSign = "+"
		}
		fraction := ""
		if len(digits) > 1 {
			fraction = "." + digits[1:]
		}
		return sign + digits[:1] + fraction + "e" + exponentSign + strconv.Itoa(exponent)
	}
}

func addCanonicalJSONSize(total *int, addition int) error {
	maximum := int(^uint(0) >> 1)
	if addition < 0 || *total > maximum-addition {
		return fmt.Errorf("canonical JSON byte size overflows the host integer range")
	}
	*total += addition
	return nil
}

func validateDurableQueryPage(
	page *DurableQueryPage,
	queryKind DurablePageQueryKind,
	runID string,
	validateItem func(json.RawMessage) error,
) error {
	if page == nil || page.Items == nil || len(page.Items) > maxDurableQueryPageItems {
		return fmt.Errorf("durable query page item count is invalid")
	}
	if err := validateDurableQuerySource(page.ObservedRevision, page.SourceRoot); err != nil {
		return err
	}
	var previous DurablePagePosition
	for index, item := range page.Items {
		if err := validateItem(item); err != nil {
			return err
		}
		size, err := normalizedJSONSize(item)
		if err != nil || size > maxDurableQuerySummaryBytes {
			return fmt.Errorf("durable query summary exceeds its canonical byte limit")
		}
		key, err := durableSummaryKey(item, queryKind)
		if err != nil {
			return err
		}
		if runID != "" {
			var owner struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(item, &owner); err != nil || owner.RunID != runID {
				return fmt.Errorf("durable query item escaped its Run")
			}
		}
		position := DurablePagePosition{CanonicalKey: key, KeyHash: durablePageKeyHash(key)}
		if index > 0 && (previous.KeyHash > position.KeyHash ||
			previous.KeyHash == position.KeyHash && previous.CanonicalKey >= position.CanonicalKey) {
			return fmt.Errorf("durable query items are not in authenticated key order")
		}
		previous = position
	}
	if page.NextCursor != nil {
		if err := validateDurablePageCursor(page.NextCursor); err != nil {
			return err
		}
		var expectedRunID *string
		if runID != "" {
			expectedRunID = &runID
		}
		if page.NextCursor.QueryKind != queryKind ||
			!equalOptionalString(page.NextCursor.RunID, expectedRunID) ||
			page.NextCursor.SourceRevision != page.ObservedRevision ||
			page.NextCursor.SourceRoot != page.SourceRoot || len(page.Items) == 0 ||
			page.NextCursor.Position != previous {
			return fmt.Errorf("durable next cursor does not bind the terminal item and source")
		}
	}
	return nil
}

func equalOptionalString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func validateDurableRunPage(
	response DurableResponse,
	queryKind DurablePageQueryKind,
	validateItem func(json.RawMessage) error,
) error {
	if !validRunIdentity(response.RunID) {
		return fmt.Errorf("durable Run page owner is invalid")
	}
	return validateDurableQueryPage(response.Page, queryKind, response.RunID, validateItem)
}

func continuationExecutionStatus(continuationStatus string, executionStatus json.RawMessage) (string, error) {
	if err := validateRunExecutionStatusRaw(executionStatus); err != nil {
		return "", err
	}
	var status struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(executionStatus, &status); err != nil {
		return "", err
	}
	expected := map[string]string{
		"ready": "active", "waiting": "active", "running": "active",
		"completed": "completed", "failed": "failed", "cancelled": "cancelled",
	}[continuationStatus]
	if expected == "" || status.Status != expected {
		return "", fmt.Errorf("Continuation and execution summary axes disagree")
	}
	return status.Status, nil
}

func validateWorldSettlement(settlement, executionStatus string) error {
	if !slices.Contains([]string{"settled", "pending", "unknown", "governance_required"}, settlement) ||
		executionStatus == "completed" && settlement != "settled" {
		return fmt.Errorf("Run world-settlement axis is invalid")
	}
	return nil
}

func validateDurableRunIndexSummaryRaw(raw json.RawMessage) error {
	var summary struct {
		RunID              string          `json:"run_id"`
		ContinuationStatus string          `json:"continuation_status"`
		ExecutionStatus    json.RawMessage `json:"execution_status"`
		WorldSettlement    string          `json:"world_settlement"`
	}
	if err := validateClosedRaw(raw, []string{"run_id", "continuation_status", "execution_status", "world_settlement"}, &summary); err != nil {
		return err
	}
	if !validRunIdentity(summary.RunID) {
		return fmt.Errorf("Run-index summary identity is invalid")
	}
	executionStatus, err := continuationExecutionStatus(summary.ContinuationStatus, summary.ExecutionStatus)
	if err != nil {
		return err
	}
	return validateWorldSettlement(summary.WorldSettlement, executionStatus)
}

func validateDurableRunCurrentRaw(raw json.RawMessage) error {
	var current struct {
		RunID              string          `json:"run_id"`
		PlanID             string          `json:"plan_id"`
		ExecutionBinding   ArtifactRef     `json:"execution_binding"`
		ContinuationStatus string          `json:"continuation_status"`
		Epoch              uint64          `json:"epoch"`
		ExecutionFence     uint64          `json:"execution_fence"`
		Result             json.RawMessage `json:"result"`
		ExecutionStatus    json.RawMessage `json:"execution_status"`
		WorldSettlement    string          `json:"world_settlement"`
	}
	if err := validateClosedRaw(raw, []string{
		"run_id", "plan_id", "execution_binding", "continuation_status", "epoch",
		"execution_fence", "result", "execution_status", "world_settlement",
	}, &current); err != nil {
		return err
	}
	if !validRunIdentity(current.RunID) || !validSHA256ID(current.PlanID) ||
		validateArtifactRef(current.ExecutionBinding) != nil ||
		current.ExecutionBinding.Kind != "cymule.execution-binding/2" ||
		current.Epoch > maxExactInteger || current.ExecutionFence > maxExactInteger {
		return fmt.Errorf("Run-current projection is invalid")
	}
	executionStatus, err := continuationExecutionStatus(current.ContinuationStatus, current.ExecutionStatus)
	if err != nil {
		return err
	}
	if err := validateWorldSettlement(current.WorldSettlement, executionStatus); err != nil {
		return err
	}
	if executionStatus == "completed" {
		if rawMessageIsNull(current.Result) || validateArtifactRefRaw(current.Result) != nil {
			return fmt.Errorf("completed Run-current projection has no result")
		}
	} else if !rawMessageIsNull(current.Result) {
		return fmt.Errorf("non-completed Run-current projection has a result")
	}
	size, err := normalizedJSONSize(raw)
	if err != nil || size > maxDurableQuerySummaryBytes {
		return fmt.Errorf("Run-current projection exceeds its canonical byte limit")
	}
	return nil
}

func validateDurableWaitSummaryRaw(raw json.RawMessage) error {
	var summary struct {
		WaitID string          `json:"wait_id"`
		RunID  string          `json:"run_id"`
		State  string          `json:"state"`
		Result json.RawMessage `json:"result"`
	}
	if err := validateClosedRaw(raw, []string{"wait_id", "run_id", "state", "result"}, &summary); err != nil {
		return err
	}
	if !validSHA256ID(summary.WaitID) || !validRunIdentity(summary.RunID) ||
		!slices.Contains([]string{"pending", "completed", "cancelled"}, summary.State) {
		return fmt.Errorf("wait summary is invalid")
	}
	if summary.State == "completed" {
		if rawMessageIsNull(summary.Result) || validateArtifactRefRaw(summary.Result) != nil {
			return fmt.Errorf("completed wait summary has no valid result")
		}
	} else if !rawMessageIsNull(summary.Result) {
		return fmt.Errorf("non-completed wait summary has a result")
	}
	return nil
}

func validateDurableEffectSummaryRaw(raw json.RawMessage) error {
	var summary struct {
		IntentID              string          `json:"intent_id"`
		RunID                 string          `json:"run_id"`
		State                 string          `json:"state"`
		ExecutionAvailability string          `json:"execution_availability"`
		Reconciliation        string          `json:"reconciliation"`
		Result                json.RawMessage `json:"result"`
	}
	if err := validateClosedRaw(raw, []string{"intent_id", "run_id", "state", "execution_availability", "reconciliation", "result"}, &summary); err != nil {
		return err
	}
	if !validSHA256ID(summary.IntentID) || !validRunIdentity(summary.RunID) ||
		!slices.Contains([]string{"pending", "claimed", "applied", "not_applied", "unknown", "cancelled_before_release"}, summary.State) ||
		!slices.Contains([]string{"available", "unavailable"}, summary.ExecutionAvailability) {
		return fmt.Errorf("Effect summary is invalid")
	}
	allowedReconciliation := map[string][]string{
		"pending": {"not_required"}, "claimed": {"not_required"},
		"applied": {"not_required", "resolved"}, "not_applied": {"not_required", "resolved"},
		"unknown": {"pending", "governance_required"}, "cancelled_before_release": {"resolved"},
	}[summary.State]
	if !slices.Contains(allowedReconciliation, summary.Reconciliation) ||
		slices.Contains([]string{"pending", "claimed"}, summary.State) && summary.ExecutionAvailability != "available" {
		return fmt.Errorf("Effect summary lifecycle is inconsistent")
	}
	if summary.State == "applied" {
		if rawMessageIsNull(summary.Result) {
			return fmt.Errorf("applied Effect summary has no result Artifact")
		}
		return validateArtifactRefRaw(summary.Result)
	}
	if !rawMessageIsNull(summary.Result) {
		return fmt.Errorf("non-applied Effect summary has a result")
	}
	return nil
}

func validateDurableOccurrenceSummaryRaw(raw json.RawMessage) error {
	var summary struct {
		OccurrenceID string          `json:"occurrence_id"`
		RunID        string          `json:"run_id"`
		State        string          `json:"state"`
		Outcome      json.RawMessage `json:"outcome"`
	}
	if err := validateClosedRaw(raw, []string{"occurrence_id", "run_id", "state", "outcome"}, &summary); err != nil {
		return err
	}
	if !validSHA256ID(summary.OccurrenceID) || !validRunIdentity(summary.RunID) ||
		!slices.Contains([]string{"pending", "completed"}, summary.State) {
		return fmt.Errorf("component occurrence summary is invalid")
	}
	if summary.State == "completed" {
		if rawMessageIsNull(summary.Outcome) {
			return fmt.Errorf("completed occurrence summary has no outcome")
		}
		return validateComponentOutcomeRaw(summary.Outcome)
	}
	if !rawMessageIsNull(summary.Outcome) {
		return fmt.Errorf("pending occurrence summary has an outcome")
	}
	return nil
}

func validateDurableAttemptSummaryRaw(raw json.RawMessage) error {
	var summary struct {
		AttemptID      string          `json:"attempt_id"`
		OccurrenceID   string          `json:"occurrence_id"`
		RunID          string          `json:"run_id"`
		AttemptOrdinal uint64          `json:"attempt_ordinal"`
		State          string          `json:"state"`
		Outcome        json.RawMessage `json:"outcome"`
	}
	if err := validateClosedRaw(raw, []string{"attempt_id", "occurrence_id", "run_id", "attempt_ordinal", "state", "outcome"}, &summary); err != nil {
		return err
	}
	if !validSHA256ID(summary.AttemptID) || !validSHA256ID(summary.OccurrenceID) ||
		!validRunIdentity(summary.RunID) || summary.AttemptOrdinal == 0 ||
		summary.AttemptOrdinal > maxExactInteger ||
		!slices.Contains([]string{"running", "completed", "superseded"}, summary.State) {
		return fmt.Errorf("operation Attempt summary is invalid")
	}
	if summary.State == "completed" {
		if rawMessageIsNull(summary.Outcome) {
			return fmt.Errorf("completed Attempt summary has no outcome")
		}
		return validateComponentOutcomeRaw(summary.Outcome)
	}
	if !rawMessageIsNull(summary.Outcome) {
		return fmt.Errorf("non-completed Attempt summary has an outcome")
	}
	return nil
}

func validateDurableRunItemSelector(selector DurableRunItemSelector) error {
	valid := false
	switch selector.Kind {
	case "wait":
		valid = validSHA256ID(selector.WaitID) && selector.IntentID == "" && selector.OccurrenceID == "" && selector.AttemptID == ""
	case "effect":
		valid = selector.WaitID == "" && validSHA256ID(selector.IntentID) && selector.OccurrenceID == "" && selector.AttemptID == ""
	case "occurrence":
		valid = selector.WaitID == "" && selector.IntentID == "" && validSHA256ID(selector.OccurrenceID) && selector.AttemptID == ""
	case "attempt":
		valid = selector.WaitID == "" && selector.IntentID == "" && selector.OccurrenceID == "" && validSHA256ID(selector.AttemptID)
	}
	if !valid {
		return fmt.Errorf("exact Run-item selector is invalid")
	}
	return nil
}

func validateDurableWaitRaw(raw json.RawMessage) (string, error) {
	var wait struct {
		WaitID      string          `json:"wait_id"`
		RunID       string          `json:"run_id"`
		Kind        json.RawMessage `json:"kind"`
		ConsumeOnce bool            `json:"consume_once"`
		Owner       json.RawMessage `json:"owner"`
		State       string          `json:"state"`
		Result      json.RawMessage `json:"result"`
	}
	if err := validateClosedRaw(raw, []string{"wait_id", "run_id", "kind", "consume_once", "owner", "state", "result"}, &wait); err != nil {
		return "", err
	}
	if !validSHA256ID(wait.WaitID) || !validRunIdentity(wait.RunID) ||
		!slices.Contains([]string{"pending", "completed", "cancelled"}, wait.State) {
		return "", fmt.Errorf("wait condition identity or state is invalid")
	}
	if err := validateDurableWaitKindRaw(wait.Kind); err != nil {
		return "", err
	}
	var owner struct {
		InvocationID string            `json:"invocation_id"`
		DefinitionID string            `json:"definition_id"`
		SiteID       string            `json:"site_id"`
		RegionPath   []json.RawMessage `json:"region_path"`
		StepIndex    json.RawMessage   `json:"step_index"`
		Bind         json.RawMessage   `json:"bind"`
	}
	if err := validateClosedRaw(wait.Owner, []string{"invocation_id", "definition_id", "site_id", "region_path", "step_index", "bind"}, &owner); err != nil {
		return "", err
	}
	if !validClockIdentity(owner.InvocationID) || !validClockIdentity(owner.DefinitionID) ||
		!validClockIdentity(owner.SiteID) || owner.RegionPath == nil {
		return "", fmt.Errorf("wait owner is invalid")
	}
	for _, index := range append(owner.RegionPath, owner.StepIndex) {
		if _, err := validateSafeUintRaw(index, false); err != nil {
			return "", fmt.Errorf("wait owner index is invalid")
		}
	}
	if bind, present, err := decodeNullableNonEmptyStringRaw(owner.Bind); err != nil || present && !validClockIdentity(bind) {
		return "", fmt.Errorf("wait owner bind is invalid")
	}
	if wait.State == "completed" {
		if rawMessageIsNull(wait.Result) || validateArtifactRefRaw(wait.Result) != nil {
			return "", fmt.Errorf("completed wait has no valid result")
		}
	} else if !rawMessageIsNull(wait.Result) {
		return "", fmt.Errorf("non-completed wait has a result")
	}
	return wait.RunID, nil
}

func validateDurableEffectRaw(raw json.RawMessage) (string, error) {
	var effect struct {
		IntentID              string          `json:"intent_id"`
		RunID                 string          `json:"run_id"`
		OriginPlanID          string          `json:"origin_plan_id"`
		Operation             string          `json:"operation"`
		Input                 ArtifactRef     `json:"input"`
		ExecutionBinding      ArtifactRef     `json:"execution_binding"`
		OccurrenceBinding     string          `json:"occurrence_binding"`
		ExecutionAvailability string          `json:"execution_availability"`
		Reconciliation        string          `json:"reconciliation"`
		State                 string          `json:"state"`
		ClaimEpoch            uint64          `json:"claim_epoch"`
		ClaimOwner            json.RawMessage `json:"claim_owner"`
		Result                json.RawMessage `json:"result"`
	}
	if err := validateClosedRaw(raw, []string{"intent_id", "run_id", "origin_plan_id", "operation", "input", "execution_binding", "occurrence_binding", "execution_availability", "reconciliation", "state", "claim_epoch", "claim_owner", "result"}, &effect); err != nil {
		return "", err
	}
	claimOwner, claimed, claimErr := decodeNullableNonEmptyStringRaw(effect.ClaimOwner)
	if !validSHA256ID(effect.IntentID) || !validRunIdentity(effect.RunID) ||
		!validSHA256ID(effect.OriginPlanID) || !validClockIdentity(effect.Operation) ||
		validateArtifactRef(effect.Input) != nil || validateArtifactRef(effect.ExecutionBinding) != nil ||
		effect.ExecutionBinding.Kind != "cymule.execution-binding/2" ||
		!validSHA256ID(effect.OccurrenceBinding) || effect.ClaimEpoch > maxExactInteger ||
		claimErr != nil || claimed && !validClockIdentity(claimOwner) {
		return "", fmt.Errorf("Effect dispatch identity or binding is invalid")
	}
	allowedReconciliation := map[string][]string{
		"pending": {"not_required"}, "claimed": {"not_required"},
		"applied": {"not_required", "resolved"}, "not_applied": {"not_required", "resolved"},
		"unknown": {"pending", "governance_required"}, "cancelled_before_release": {"resolved"},
	}[effect.State]
	if allowedReconciliation == nil || !slices.Contains([]string{"available", "unavailable"}, effect.ExecutionAvailability) ||
		!slices.Contains(allowedReconciliation, effect.Reconciliation) {
		return "", fmt.Errorf("Effect dispatch lifecycle is invalid")
	}
	claimExpected := !slices.Contains([]string{"pending", "cancelled_before_release"}, effect.State)
	resultPresent := !rawMessageIsNull(effect.Result)
	if claimed != claimExpected || (claimed && effect.ClaimEpoch == 0) || (!claimed && effect.ClaimEpoch != 0) ||
		slices.Contains([]string{"pending", "claimed", "unknown", "not_applied", "cancelled_before_release"}, effect.State) && resultPresent ||
		slices.Contains([]string{"pending", "claimed"}, effect.State) && effect.ExecutionAvailability != "available" {
		return "", fmt.Errorf("Effect dispatch claim or result is inconsistent")
	}
	if resultPresent && validateArtifactRefRaw(effect.Result) != nil {
		return "", fmt.Errorf("Effect dispatch result is invalid")
	}
	return effect.RunID, nil
}

func validateDurableOccurrenceRaw(raw json.RawMessage) (string, error) {
	var occurrence struct {
		OccurrenceVersion      string            `json:"occurrence_version"`
		OccurrenceID           string            `json:"occurrence_id"`
		RunID                  string            `json:"run_id"`
		PlanID                 string            `json:"plan_id"`
		BindingContext         string            `json:"binding_context"`
		InvocationID           string            `json:"invocation_id"`
		InvocationPath         []json.RawMessage `json:"invocation_path"`
		DefinitionID           string            `json:"definition_id"`
		RegionPath             []json.RawMessage `json:"region_path"`
		SiteID                 string            `json:"site_id"`
		StepIndex              json.RawMessage   `json:"step_index"`
		Component              string            `json:"component"`
		Input                  ArtifactRef       `json:"input"`
		Outcome                json.RawMessage   `json:"outcome"`
		OccurrenceBinding      string            `json:"occurrence_binding"`
		ImplementationRevision string            `json:"implementation_revision"`
		AttemptCount           uint64            `json:"attempt_count"`
		LatestAttemptID        string            `json:"latest_attempt_id"`
		ContinuationDigest     json.RawMessage   `json:"continuation_digest"`
		State                  string            `json:"state"`
	}
	if err := validateClosedRaw(raw, []string{"occurrence_version", "occurrence_id", "run_id", "plan_id", "binding_context", "invocation_id", "invocation_path", "definition_id", "region_path", "site_id", "step_index", "component", "input", "outcome", "occurrence_binding", "implementation_revision", "attempt_count", "latest_attempt_id", "continuation_digest", "state"}, &occurrence); err != nil {
		return "", err
	}
	if occurrence.OccurrenceVersion != "cymule.component-occurrence/4" ||
		!validSHA256ID(occurrence.OccurrenceID) || !validRunIdentity(occurrence.RunID) ||
		!validSHA256ID(occurrence.PlanID) || !validSHA256ID(occurrence.BindingContext) ||
		!validClockIdentity(occurrence.InvocationID) || !validClockIdentity(occurrence.DefinitionID) ||
		!validClockIdentity(occurrence.SiteID) || !validClockIdentity(occurrence.Component) ||
		validateArtifactRef(occurrence.Input) != nil || !validSHA256ID(occurrence.OccurrenceBinding) ||
		!validClockIdentity(occurrence.ImplementationRevision) || occurrence.AttemptCount == 0 ||
		occurrence.AttemptCount > maxExactInteger || !validSHA256ID(occurrence.LatestAttemptID) ||
		occurrence.InvocationPath == nil || occurrence.RegionPath == nil {
		return "", fmt.Errorf("component occurrence identity or binding is invalid")
	}
	for _, index := range append(occurrence.RegionPath, occurrence.StepIndex) {
		if _, err := validateSafeUintRaw(index, false); err != nil {
			return "", fmt.Errorf("component occurrence index is invalid")
		}
	}
	for _, segmentRaw := range occurrence.InvocationPath {
		var segment struct {
			SiteID     string            `json:"site_id"`
			RegionPath []json.RawMessage `json:"region_path"`
			ScopeID    string            `json:"scope_id"`
		}
		if err := validateClosedRaw(segmentRaw, []string{"site_id", "region_path", "scope_id"}, &segment); err != nil {
			return "", err
		}
		if !validClockIdentity(segment.SiteID) || !validClockIdentity(segment.ScopeID) || segment.RegionPath == nil {
			return "", fmt.Errorf("component invocation path is invalid")
		}
		for _, index := range segment.RegionPath {
			if _, err := validateSafeUintRaw(index, false); err != nil {
				return "", fmt.Errorf("component invocation path index is invalid")
			}
		}
	}
	outcomePresent := !rawMessageIsNull(occurrence.Outcome)
	digest, digestPresent, digestErr := decodeNullableNonEmptyStringRaw(occurrence.ContinuationDigest)
	if digestErr != nil || digestPresent && !validLowerHexDigest(digest) ||
		occurrence.State == "pending" && (outcomePresent || digestPresent) ||
		occurrence.State == "completed" && (!outcomePresent || !digestPresent) ||
		!slices.Contains([]string{"pending", "completed"}, occurrence.State) {
		return "", fmt.Errorf("component occurrence lifecycle is invalid")
	}
	if outcomePresent {
		if err := validateComponentOutcomeRaw(occurrence.Outcome); err != nil {
			return "", err
		}
	}
	return occurrence.RunID, nil
}

func validateDurableAttemptRaw(raw json.RawMessage) (string, error) {
	var attempt struct {
		AttemptVersion        string          `json:"attempt_version"`
		AttemptID             string          `json:"attempt_id"`
		OccurrenceID          string          `json:"occurrence_id"`
		RunID                 string          `json:"run_id"`
		AttemptOrdinal        uint64          `json:"attempt_ordinal"`
		PreviousAttemptID     json.RawMessage `json:"previous_attempt_id"`
		ContinuationAttemptID string          `json:"continuation_attempt_id"`
		ExecutionClaimOwner   string          `json:"execution_claim_owner"`
		ExecutionClaimFence   uint64          `json:"execution_claim_fence"`
		OccurrenceBinding     string          `json:"operation_occurrence_binding"`
		TransportRequestID    string          `json:"transport_request_id"`
		State                 string          `json:"state"`
		Outcome               json.RawMessage `json:"outcome"`
	}
	if err := validateClosedRaw(raw, []string{"attempt_version", "attempt_id", "occurrence_id", "run_id", "attempt_ordinal", "previous_attempt_id", "continuation_attempt_id", "execution_claim_owner", "execution_claim_fence", "operation_occurrence_binding", "transport_request_id", "state", "outcome"}, &attempt); err != nil {
		return "", err
	}
	previousAttemptID, hasPreviousAttempt, previousAttemptErr := decodeNullableNonEmptyStringRaw(attempt.PreviousAttemptID)
	if attempt.AttemptVersion != "cymule.operation-attempt/2" || !validSHA256ID(attempt.AttemptID) ||
		!validSHA256ID(attempt.OccurrenceID) || !validRunIdentity(attempt.RunID) ||
		attempt.AttemptOrdinal == 0 || attempt.AttemptOrdinal > maxExactInteger ||
		previousAttemptErr != nil || hasPreviousAttempt && !validSHA256ID(previousAttemptID) ||
		(attempt.AttemptOrdinal == 1) != !hasPreviousAttempt ||
		!validSHA256ID(attempt.ContinuationAttemptID) || !validClockIdentity(attempt.ExecutionClaimOwner) ||
		attempt.ExecutionClaimFence == 0 || !validSHA256ID(attempt.OccurrenceBinding) ||
		attempt.ExecutionClaimFence > maxExactInteger || !validSHA256ID(attempt.TransportRequestID) ||
		!slices.Contains([]string{"running", "completed", "superseded"}, attempt.State) {
		return "", fmt.Errorf("operation Attempt identity or fence is invalid")
	}
	outcomePresent := !rawMessageIsNull(attempt.Outcome)
	if attempt.State == "completed" != outcomePresent {
		return "", fmt.Errorf("operation Attempt lifecycle is invalid")
	}
	if outcomePresent {
		if err := validateComponentOutcomeRaw(attempt.Outcome); err != nil {
			return "", err
		}
	}
	return attempt.RunID, nil
}

func validateDurableRunItemRaw(raw json.RawMessage) (string, error) {
	var tag struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &tag); err != nil {
		return "", err
	}
	field := map[string]string{"wait": "wait", "effect": "effect", "occurrence": "occurrence", "attempt": "attempt"}[tag.Kind]
	if field == "" {
		return "", fmt.Errorf("exact durable Run item kind is invalid")
	}
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, []string{"kind", field}, &object); err != nil {
		return "", err
	}
	size, err := normalizedJSONSize(object[field])
	if err != nil || size > maxDurableStateRootLeafBytes {
		return "", fmt.Errorf("exact durable Run item exceeds its canonical byte limit")
	}
	switch tag.Kind {
	case "wait":
		return validateDurableWaitRaw(object[field])
	case "effect":
		return validateDurableEffectRaw(object[field])
	case "occurrence":
		return validateDurableOccurrenceRaw(object[field])
	default:
		return validateDurableAttemptRaw(object[field])
	}
}

func validateDurableWaitKindRaw(raw json.RawMessage) error {
	var tag struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	fields := map[string][]string{
		"signal": {"kind", "key"},
		"timer":  {"kind", "timer_id"},
		"input":  {"kind", "correlation", "schema"},
	}[tag.Kind]
	if fields == nil {
		return fmt.Errorf("durable wait kind is unknown")
	}
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, fields, &object); err != nil {
		return err
	}
	identityField := map[string]string{"signal": "key", "timer": "timer_id", "input": "correlation"}[tag.Kind]
	var identity string
	if err := json.Unmarshal(object[identityField], &identity); err != nil || identity == "" {
		return fmt.Errorf("durable wait kind identity is invalid")
	}
	if tag.Kind == "input" {
		var schema any
		if err := json.Unmarshal(object["schema"], &schema); err != nil {
			return err
		}
		switch schema.(type) {
		case bool, map[string]any:
		default:
			return fmt.Errorf("durable input wait schema is invalid")
		}
	}
	return nil
}

func validateRunExecutionStatusRaw(raw json.RawMessage) error {
	var tag struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	fields := map[string][]string{
		"active": {"status"}, "completed": {"status"},
		"failed": {"status", "failure"}, "cancelled": {"status", "reason"},
	}[tag.Status]
	if fields == nil {
		return fmt.Errorf("Run execution status is unknown")
	}
	var object map[string]json.RawMessage
	if err := validateClosedRaw(raw, fields, &object); err != nil {
		return err
	}
	if tag.Status == "failed" {
		return validateRunFailureRaw(object["failure"])
	}
	if tag.Status == "cancelled" {
		var reason ArtifactRef
		if err := validateClosedRaw(object["reason"], []string{"identity_version", "artifact_id", "kind"}, &reason); err != nil {
			return err
		}
		return validateArtifactRef(reason)
	}
	return nil
}

func validateComponentOutcomeRaw(raw json.RawMessage) error {
	var tag struct {
		Outcome string `json:"outcome"`
	}
	if err := json.Unmarshal(raw, &tag); err != nil {
		return err
	}
	switch tag.Outcome {
	case "succeeded":
		var outcome struct {
			Outcome string      `json:"outcome"`
			Output  ArtifactRef `json:"output"`
		}
		if err := validateClosedRaw(raw, []string{"outcome", "output"}, &outcome); err != nil {
			return err
		}
		return validateArtifactRef(outcome.Output)
	case "expected_failure":
		var outcome struct {
			Outcome string      `json:"outcome"`
			Code    string      `json:"code"`
			Detail  ArtifactRef `json:"detail"`
		}
		if err := validateClosedRaw(raw, []string{"outcome", "code", "detail"}, &outcome); err != nil {
			return err
		}
		if !validFailureCode(outcome.Code) {
			return fmt.Errorf("expected component failure code is invalid")
		}
		return validateArtifactRef(outcome.Detail)
	default:
		return fmt.Errorf("component outcome variant is unknown")
	}
}

func validateContinuationExecutionClaim(claim ContinuationExecutionClaim) error {
	if claim.ClaimVersion != "cymule.continuation-execution-claim/1" || !validRunIdentity(claim.RunID) ||
		!validSHA256ID(claim.ContinuationID) || !validClockIdentity(claim.Owner) || !validSHA256ID(claim.ContinuationAttemptID) ||
		claim.Fence == 0 || claim.Fence > maxExactInteger || !validSHA256ID(claim.PlanID) ||
		claim.LogicalAcquiredAt > maxExactInteger || claim.LogicalTTL == 0 ||
		claim.LogicalTTL > maxExactInteger || claim.LogicalExpiresAt > maxExactInteger ||
		claim.LogicalAcquiredAt > maxExactInteger-claim.LogicalTTL ||
		claim.LogicalAcquiredAt+claim.LogicalTTL != claim.LogicalExpiresAt {
		return fmt.Errorf("Continuation execution claim is invalid")
	}
	if err := validateArtifactRef(claim.ExecutionBindingRef); err != nil {
		return err
	}
	if claim.ExecutionBindingRef.Kind != "cymule.execution-binding/2" {
		return fmt.Errorf("Continuation execution binding kind is invalid")
	}
	if !validClockObservationRef(claim.ClockObservationRef) {
		return fmt.Errorf("Continuation execution claim Clock reference is invalid")
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
	Item             WorkItem          `json:"item"`
	Owner            string            `json:"owner"`
	Epoch            uint64            `json:"epoch"`
	OccurrenceID     string            `json:"occurrence_id"`
	PlanID           string            `json:"plan_id"`
	ExecutionBinding ArtifactRef       `json:"execution_binding"`
	Lease            VirtualClaimLease `json:"lease"`
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
	ExecutionBinding  ArtifactRef  `json:"execution_binding"`
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
	if err := validateWorkResolution(resolution); err != nil {
		return nil, err
	}
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
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("work resolution is not an object")
	}
	tag, ok := object["resolution"].(string)
	if !ok {
		return fmt.Errorf("work resolution tag is missing")
	}
	fields := map[string][]string{
		"succeeded": {"resolution", "result"},
		"retry":     {"resolution", "error", "next_reason"},
		"parked":    {"resolution", "reason"},
		"failed":    {"resolution", "error"},
		"cancelled": {"resolution", "reason"},
	}[tag]
	if fields == nil {
		return fmt.Errorf("unsupported work resolution %q", tag)
	}
	if err := requireExactJSONFields(object, fields); err != nil {
		return err
	}
	decodeArtifact := func(field string) (*ArtifactRef, error) {
		member, ok := object[field].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("work resolution %s is not an Artifact", field)
		}
		var reference ArtifactRef
		if err := decodeClosedValue(member, &reference); err != nil {
			return nil, err
		}
		if err := validateArtifactRef(reference); err != nil {
			return nil, err
		}
		return &reference, nil
	}
	var decoded WorkResolution
	switch tag {
	case "succeeded":
		result, err := decodeArtifact("result")
		if err != nil {
			return err
		}
		decoded = WorkResolution{Kind: tag, Result: result}
	case "retry":
		failure, err := decodeArtifact("error")
		if err != nil {
			return err
		}
		var nextReason *ParkReason
		if object["next_reason"] != nil {
			reason, err := decodeParkReasonValue(object["next_reason"])
			if err != nil {
				return err
			}
			nextReason = &reason
		}
		decoded = WorkResolution{
			Kind: tag, Error: failure, NextReason: nextReason,
		}
	case "parked":
		reason, err := decodeParkReasonValue(object["reason"])
		if err != nil {
			return err
		}
		decoded = WorkResolution{Kind: tag, ParkReason: &reason}
	case "failed":
		failure, err := decodeArtifact("error")
		if err != nil {
			return err
		}
		decoded = WorkResolution{Kind: tag, Error: failure}
	case "cancelled":
		reason, err := decodeArtifact("reason")
		if err != nil {
			return err
		}
		decoded = WorkResolution{Kind: tag, CancelReason: reason}
	}
	if err := validateWorkResolution(decoded); err != nil {
		return err
	}
	if !wireValuesEqual(value, decoded) {
		return fmt.Errorf("work resolution loses JSON member presence during typed decoding")
	}
	*resolution = decoded
	return nil
}

func decodeParkReasonValue(value any) (ParkReason, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return ParkReason{}, fmt.Errorf("park reason is not an object")
	}
	kind, ok := object["kind"].(string)
	if !ok {
		return ParkReason{}, fmt.Errorf("park reason kind is missing")
	}
	field := map[string]string{
		"wait": "key", "dependency": "work_id", "budget": "account",
		"capability": "capability", "backpressure": "domain",
	}[kind]
	if field == "" || requireExactJSONFields(object, []string{"kind", field}) != nil {
		return ParkReason{}, fmt.Errorf("park reason fields are not closed")
	}
	identity, ok := object[field].(string)
	if !ok || !validClockIdentity(identity) {
		return ParkReason{}, fmt.Errorf("park reason identity is invalid")
	}
	var reason ParkReason
	if err := decodeClosedValue(object, &reason); err != nil {
		return ParkReason{}, err
	}
	if !wireValuesEqual(object, reason) {
		return ParkReason{}, fmt.Errorf("park reason loses JSON member presence")
	}
	return reason, nil
}

func validateWorkResolution(resolution WorkResolution) error {
	validArtifact := func(reference *ArtifactRef) bool {
		return reference != nil && validateArtifactRef(*reference) == nil
	}
	validReason := func(reason *ParkReason) bool {
		if reason == nil {
			return false
		}
		encoded, err := json.Marshal(reason)
		if err != nil {
			return false
		}
		value, err := decodeUniqueJSON(encoded)
		if err != nil {
			return false
		}
		_, err = decodeParkReasonValue(value)
		return err == nil
	}
	switch resolution.Kind {
	case "succeeded":
		if !validArtifact(resolution.Result) || resolution.Error != nil || resolution.ParkReason != nil ||
			resolution.CancelReason != nil || resolution.NextReason != nil {
			return fmt.Errorf("succeeded work resolution is invalid")
		}
	case "retry":
		if resolution.Result != nil || !validArtifact(resolution.Error) || resolution.ParkReason != nil ||
			resolution.CancelReason != nil || (resolution.NextReason != nil && !validReason(resolution.NextReason)) {
			return fmt.Errorf("retry work resolution is invalid")
		}
	case "parked":
		if resolution.Result != nil || resolution.Error != nil || !validReason(resolution.ParkReason) ||
			resolution.CancelReason != nil || resolution.NextReason != nil {
			return fmt.Errorf("parked work resolution is invalid")
		}
	case "failed":
		if resolution.Result != nil || !validArtifact(resolution.Error) || resolution.ParkReason != nil ||
			resolution.CancelReason != nil || resolution.NextReason != nil {
			return fmt.Errorf("failed work resolution is invalid")
		}
	case "cancelled":
		if resolution.Result != nil || resolution.Error != nil || resolution.ParkReason != nil ||
			!validArtifact(resolution.CancelReason) || resolution.NextReason != nil {
			return fmt.Errorf("cancelled work resolution is invalid")
		}
	default:
		return fmt.Errorf("unsupported work resolution %q", resolution.Kind)
	}
	return nil
}

// WorkResolutionCommand preconditions one idempotent M3 control mutation.
type WorkResolutionCommand struct {
	ControlVersion     string              `json:"control_version"`
	CommandID          string              `json:"command_id"`
	WorkID             string              `json:"work_id"`
	Owner              string              `json:"owner"`
	Epoch              uint64              `json:"epoch"`
	ExpectedLeaseEpoch uint64              `json:"expected_lease_epoch"`
	Clock              ClockObservationRef `json:"clock"`
	Resolution         WorkResolution      `json:"resolution"`
}

// VirtualCursor is an opaque provider-owned logical source position.
type VirtualCursor struct {
	Version   string `json:"version"`
	Position  string `json:"position"`
	Exhausted bool   `json:"exhausted"`
}

// RegionSourceBinding pins one exact source adapter generation.
type RegionSourceBinding struct {
	Operation string `json:"operation"`
	Binding   string `json:"binding"`
	Revision  string `json:"revision"`
}

// RegionSourceCheckpoint is one exact migration source precondition.
type RegionSourceCheckpoint struct {
	Source RegionSourceBinding `json:"source"`
	Cursor VirtualCursor       `json:"cursor"`
}

// VirtualRegion is one active or retired virtual source region.
type VirtualRegion struct {
	RegionID       string              `json:"region_id"`
	RunID          string              `json:"run_id"`
	Source         RegionSourceBinding `json:"source"`
	SourceArtifact ArtifactRef         `json:"source_artifact"`
	Cursor         VirtualCursor       `json:"cursor"`
	EstimatedTotal *uint64             `json:"estimated_total"`
}

// RegionMigrationRequest is passed to a replaceable cursor migration adapter.
type RegionMigrationRequest struct {
	MigrationID       string   `json:"migration_id"`
	Kind              string   `json:"kind"`
	SourceRegionIDs   []string `json:"source_region_ids"`
	TargetCount       uint64   `json:"target_count"`
	MigrationBinding  string   `json:"migration_binding"`
	MigrationRevision string   `json:"migration_revision"`
}

// RegionMigrationPlan replaces exact source cursors with evidenced targets.
type RegionMigrationPlan struct {
	MigrationVersion  string                            `json:"migration_version"`
	MigrationID       string                            `json:"migration_id"`
	Kind              string                            `json:"kind"`
	ExpectedSources   map[string]RegionSourceCheckpoint `json:"expected_sources"`
	Targets           []VirtualRegion                   `json:"targets"`
	MigrationBinding  string                            `json:"migration_binding"`
	MigrationRevision string                            `json:"migration_revision"`
	CoverageEvidence  ArtifactRef                       `json:"coverage_evidence"`
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

// VirtualArchiveBinding pins one immutable Rust archive provider generation.
type VirtualArchiveBinding struct {
	Binding  string `json:"binding"`
	Revision string `json:"revision"`
}

// VirtualCompactionCertificate authenticates exact cold occurrence history.
type VirtualCompactionCertificate struct {
	CertificateVersion        string                   `json:"certificate_version"`
	CertificateID             string                   `json:"certificate_id"`
	SourceCausalCut           []string                 `json:"source_causal_cut"`
	Summary                   VirtualCompletionSummary `json:"summary"`
	SummaryStateDigest        string                   `json:"summary_state_digest"`
	OccurrenceRootDigest      string                   `json:"occurrence_root_digest"`
	ParentWorkIndexRootDigest string                   `json:"parent_work_index_root_digest"`
	WorkIndexUpdatesDigest    string                   `json:"work_index_updates_digest"`
	WorkIndexRootDigest       string                   `json:"work_index_root_digest"`
	CommandRootDigest         *string                  `json:"command_root_digest"`
	CommandCount              uint64                   `json:"command_count"`
	UnresolvedObligations     []string                 `json:"unresolved_obligations"`
	RetainedExecutionBindings []ArtifactRef            `json:"retained_execution_bindings"`
	ReplayAvailability        ReplayAvailability       `json:"replay_availability"`
	RehydrationManifest       ResourceHandle           `json:"rehydration_manifest"`
	Archive                   VirtualArchiveBinding    `json:"archive"`
}

// VirtualCompactionCommand requests one idempotent completed-region archive.
type VirtualCompactionCommand struct {
	ControlVersion     string                `json:"control_version"`
	CommandID          string                `json:"command_id"`
	RegionID           string                `json:"region_id"`
	SourceCausalCut    []string              `json:"source_causal_cut"`
	WorkIDs            []string              `json:"work_ids"`
	OccurrenceIDs      []string              `json:"occurrence_ids"`
	ArchivedCommandIDs []string              `json:"archived_command_ids"`
	Archive            VirtualArchiveBinding `json:"archive"`
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
	ControlVersion   string              `json:"control_version"`
	CommandID        string              `json:"command_id"`
	Owner            string              `json:"owner"`
	SlotID           string              `json:"slot_id"`
	ExecutionBinding ArtifactRef         `json:"execution_binding"`
	Capabilities     []string            `json:"capabilities"`
	Clock            ClockObservationRef `json:"clock"`
	LeaseTTL         uint64              `json:"lease_ttl"`
}

// VirtualLeaseRenewalCommand advances one active capacity-slot lease fence.
type VirtualLeaseRenewalCommand struct {
	ControlVersion     string              `json:"control_version"`
	CommandID          string              `json:"command_id"`
	WorkID             string              `json:"work_id"`
	Owner              string              `json:"owner"`
	Epoch              uint64              `json:"epoch"`
	ExpectedLeaseEpoch uint64              `json:"expected_lease_epoch"`
	Clock              ClockObservationRef `json:"clock"`
	LeaseTTL           uint64              `json:"lease_ttl"`
}

// VirtualLeaseRenewalReceipt retains the new slot fence.
type VirtualLeaseRenewalReceipt struct {
	Command          VirtualLeaseRenewalCommand `json:"command"`
	ClockObservation ClockObservation           `json:"clock_observation"`
	Lease            VirtualClaimLease          `json:"lease"`
}

// VirtualRecoveryCommand explicitly retries, fails, or cancels expired work.
type VirtualRecoveryCommand struct {
	ControlVersion     string              `json:"control_version"`
	CommandID          string              `json:"command_id"`
	WorkID             string              `json:"work_id"`
	ExpectedOwner      string              `json:"expected_owner"`
	ExpectedEpoch      uint64              `json:"expected_epoch"`
	ExpectedLeaseEpoch uint64              `json:"expected_lease_epoch"`
	Clock              ClockObservationRef `json:"clock"`
	Resolution         WorkResolution      `json:"resolution"`
}

// VirtualRecoveryReceipt retains the expired occurrence disposition.
type VirtualRecoveryReceipt struct {
	Command          VirtualRecoveryCommand `json:"command"`
	ClockObservation ClockObservation       `json:"clock_observation"`
	Occurrence       WorkOccurrence         `json:"occurrence"`
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

func evolutionCommand(commandID, operation string) EvolutionCommand {
	return EvolutionCommand{
		ControlVersion: "cymule.evolution-control/5",
		CommandID:      commandID,
		Operation:      operation,
	}
}

func liveEvolutionCommand(commandID, operation string) LiveEvolutionCommand {
	return LiveEvolutionCommand{
		ControlVersion: "cymule.live-evolution-control/6",
		CommandID:      commandID,
		Operation:      operation,
	}
}

// PublishLiveDefinition builds one reusable-definition publication command.
func PublishLiveDefinition(
	commandID, logicalRef string,
	definition Definition,
	references []SubflowReference,
) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "publish_definition")
	command.LogicalRef = logicalRef
	command.Definition = &definition
	clonedReferences := slices.Clone(references)
	command.References = &clonedReferences
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
) LiveEvolutionCommand {
	command := liveEvolutionCommand(commandID, "apply")
	command.TemplateID = templateID
	command.Command = &operation
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
func SelectEvolutionOccurrence(commandID, occurrenceID, selectionID string, executionBinding ArtifactRef) EvolutionCommand {
	command := evolutionCommand(commandID, "select_occurrence")
	command.OccurrenceID = occurrenceID
	command.SelectionID = selectionID
	command.ExecutionBinding = &executionBinding
	return command
}

// MigrateEvolutionState builds one pinned source-epoch migration command.
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
func SucceedWork(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, result ArtifactRef) (WorkResolutionCommand, error) {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, clock, WorkResolution{
		Kind: "succeeded", Result: &result,
	})
}

// RetryWork creates a retry control command with an optional indexed condition.
func RetryWork(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, failure ArtifactRef, nextReason *ParkReason) (WorkResolutionCommand, error) {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, clock, WorkResolution{
		Kind: "retry", Error: &failure, NextReason: nextReason,
	})
}

// ParkWork creates a non-failure parked disposition command.
func ParkWork(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, reason ParkReason) (WorkResolutionCommand, error) {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, clock, WorkResolution{
		Kind: "parked", ParkReason: &reason,
	})
}

// FailWork creates a terminal-failure control command.
func FailWork(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, failure ArtifactRef) (WorkResolutionCommand, error) {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, clock, WorkResolution{
		Kind: "failed", Error: &failure,
	})
}

// CancelWork creates an active-occurrence cancellation command.
func CancelWork(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, reason ArtifactRef) (WorkResolutionCommand, error) {
	return workResolutionCommand(commandID, workID, owner, epoch, expectedLeaseEpoch, clock, WorkResolution{
		Kind: "cancelled", CancelReason: &reason,
	})
}

// MigrateRegions wraps one adapter-produced split/merge plan in a stable command.
func MigrateRegions(commandID string, plan RegionMigrationPlan) RegionMigrationCommand {
	return RegionMigrationCommand{
		ControlVersion: "cymule.virtual-region-migration-control/3",
		CommandID:      commandID,
		Plan:           plan,
	}
}

// CompactVirtualRegion copies one complete compaction intent with its Rust-issued command identity.
func CompactVirtualRegion(commandID, regionID string, sourceCausalCut, workIDs, occurrenceIDs, archivedCommandIDs []string, archive VirtualArchiveBinding) (VirtualCompactionCommand, error) {
	cut := uniqueSorted(sourceCausalCut)
	works := uniqueSorted(workIDs)
	occurrences := uniqueSorted(occurrenceIDs)
	commands := uniqueSorted(archivedCommandIDs)
	if !validSHA256ID(commandID) || !validRunIdentity(regionID) || len(cut) == 0 ||
		len(works) == 0 || len(works) > 1024 || len(occurrences) == 0 || len(occurrences) > 1024 ||
		len(commands) > 1024 || !validWireIdentity(archive.Binding) || !validWireIdentity(archive.Revision) {
		return VirtualCompactionCommand{}, fmt.Errorf("virtual compaction authority or selection bounds are invalid")
	}
	for _, identities := range [][]string{cut, works, commands} {
		for _, identity := range identities {
			if !validRunIdentity(identity) {
				return VirtualCompactionCommand{}, fmt.Errorf("virtual compaction selection identity is invalid")
			}
		}
	}
	for _, identity := range occurrences {
		if !validSHA256ID(identity) {
			return VirtualCompactionCommand{}, fmt.Errorf("virtual compaction occurrence is not a content identity")
		}
	}
	return VirtualCompactionCommand{
		ControlVersion: "cymule.virtual-compaction-control/1",
		CommandID:      commandID, RegionID: regionID, SourceCausalCut: cut,
		WorkIDs: works, OccurrenceIDs: occurrences, ArchivedCommandIDs: commands, Archive: archive,
	}, nil
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
func ClaimVirtualWork(commandID, owner, slotID string, executionBinding ArtifactRef, capabilities []string, clock ClockObservationRef, leaseTTL uint64) (VirtualClaimCommand, error) {
	if !validRunIdentity(commandID) || !validRunIdentity(owner) || !validRunIdentity(slotID) ||
		clock.Scope != slotID || !validClockObservationRef(clock) || leaseTTL == 0 || leaseTTL > maxExactInteger ||
		validateArtifactRef(executionBinding) != nil || executionBinding.Kind != "cymule.execution-binding/2" {
		return VirtualClaimCommand{}, fmt.Errorf("virtual claim authority is invalid")
	}
	for _, capability := range capabilities {
		if !validRunIdentity(capability) {
			return VirtualClaimCommand{}, fmt.Errorf("virtual claim capability is invalid")
		}
	}
	return VirtualClaimCommand{
		ControlVersion:   "cymule.virtual-claim-control/4",
		CommandID:        commandID,
		Owner:            owner,
		SlotID:           slotID,
		ExecutionBinding: executionBinding,
		Capabilities:     uniqueSorted(capabilities),
		Clock:            clock,
		LeaseTTL:         leaseTTL,
	}, nil
}

// RenewVirtualClaim creates one active-claim lease renewal command.
func RenewVirtualClaim(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, leaseTTL uint64) (VirtualLeaseRenewalCommand, error) {
	if !validRunIdentity(commandID) || !validRunIdentity(workID) || !validRunIdentity(owner) ||
		epoch == 0 || epoch > maxExactInteger || expectedLeaseEpoch == 0 || expectedLeaseEpoch > maxExactInteger ||
		!validClockObservationRef(clock) || leaseTTL == 0 || leaseTTL > maxExactInteger {
		return VirtualLeaseRenewalCommand{}, fmt.Errorf("virtual renewal authority is invalid")
	}
	return VirtualLeaseRenewalCommand{
		ControlVersion:     "cymule.virtual-lease-renewal-control/2",
		CommandID:          commandID,
		WorkID:             workID,
		Owner:              owner,
		Epoch:              epoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		Clock:              clock,
		LeaseTTL:           leaseTTL,
	}, nil
}

// RecoverVirtualClaim creates one explicit expired-claim disposition command.
func RecoverVirtualClaim(commandID, workID, expectedOwner string, expectedEpoch, expectedLeaseEpoch uint64, clock ClockObservationRef, resolution WorkResolution) (VirtualRecoveryCommand, error) {
	if !validRunIdentity(commandID) || !validRunIdentity(workID) || !validRunIdentity(expectedOwner) ||
		expectedEpoch == 0 || expectedEpoch > maxExactInteger || expectedLeaseEpoch == 0 || expectedLeaseEpoch > maxExactInteger ||
		!validClockObservationRef(clock) {
		return VirtualRecoveryCommand{}, fmt.Errorf("virtual recovery authority is invalid")
	}
	if resolution.Kind != "retry" && resolution.Kind != "failed" && resolution.Kind != "cancelled" {
		return VirtualRecoveryCommand{}, fmt.Errorf("virtual recovery accepts only retry, failure, or cancellation")
	}
	if err := validateWorkResolution(resolution); err != nil {
		return VirtualRecoveryCommand{}, err
	}
	return VirtualRecoveryCommand{
		ControlVersion:     "cymule.virtual-recovery-control/2",
		CommandID:          commandID,
		WorkID:             workID,
		ExpectedOwner:      expectedOwner,
		ExpectedEpoch:      expectedEpoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		Clock:              clock,
		Resolution:         resolution,
	}, nil
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

func workResolutionCommand(commandID, workID, owner string, epoch, expectedLeaseEpoch uint64, clock ClockObservationRef, resolution WorkResolution) (WorkResolutionCommand, error) {
	if !validRunIdentity(commandID) || !validRunIdentity(workID) || !validRunIdentity(owner) ||
		epoch == 0 || epoch > maxExactInteger || expectedLeaseEpoch == 0 || expectedLeaseEpoch > maxExactInteger ||
		!validClockObservationRef(clock) {
		return WorkResolutionCommand{}, fmt.Errorf("virtual work resolution authority is invalid")
	}
	if err := validateWorkResolution(resolution); err != nil {
		return WorkResolutionCommand{}, err
	}
	return WorkResolutionCommand{
		ControlVersion:     "cymule.virtual-work-control/2",
		CommandID:          commandID,
		WorkID:             workID,
		Owner:              owner,
		Epoch:              epoch,
		ExpectedLeaseEpoch: expectedLeaseEpoch,
		Clock:              clock,
		Resolution:         resolution,
	}, nil
}

// SignalWaitActivation creates a deterministic signal delivery record.
func SignalWaitActivation(activationID, key string, waitIDs []string, result ArtifactRef) WaitActivation {
	targets := uniqueSorted(waitIDs)
	return WaitActivation{
		ActivationVersion: "cymule.wait-activation/2",
		ActivationID:      activationID,
		Source:            WaitActivationSource{Kind: "signal", Key: key},
		WaitIDs:           targets,
		Result:            result,
	}
}

// TimerWaitActivation creates a single-target logical timer delivery record.
func TimerWaitActivation(activationID, timerID, waitID string, result ArtifactRef) WaitActivation {
	return WaitActivation{
		ActivationVersion: "cymule.wait-activation/2",
		ActivationID:      activationID,
		Source:            WaitActivationSource{Kind: "timer", TimerID: timerID},
		WaitIDs:           []string{waitID},
		Result:            result,
	}
}

// StartDurableRun builds one M1 Run-creation command.
func StartDurableRun(runID string, candidate PlanCandidate, input any, execution ExecutionClaimRequest) (DurableCommand, error) {
	if !validRunIdentity(runID) {
		return DurableCommand{}, fmt.Errorf("durable Run identity must contain 1..=512 non-control Unicode scalars")
	}
	encoded, err := marshalStrictJSONValue(input)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type:           "start_run",
		ControlVersion: "cymule.durable-control/4",
		RunID:          runID,
		Candidate:      &candidate,
		Input:          encoded,
		Execution:      &execution,
	}, nil
}

// ResumeDurableRun builds one M1 resume command.
func ResumeDurableRun(runID string, execution ExecutionClaimRequest) DurableCommand {
	return DurableCommand{
		Type: "resume_run", ControlVersion: "cymule.durable-control/4", RunID: runID, Execution: &execution,
	}
}

// TakeoverDurableRun builds one explicit expired-claim takeover command.
func TakeoverDurableRun(runID string, expectedFence uint64, execution ExecutionClaimRequest) (DurableCommand, error) {
	if !validRunIdentity(runID) || expectedFence == 0 || expectedFence > 9_007_199_254_740_991 || !validExecutionClaimRequest(&execution) {
		return DurableCommand{}, fmt.Errorf("durable takeover authority is invalid")
	}
	return DurableCommand{
		Type: "takeover_run", ControlVersion: "cymule.durable-control/4", RunID: runID,
		ExpectedFence: expectedFence, Execution: &execution,
	}, nil
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
	if activationID == "" || len(targets) == 0 ||
		slices.ContainsFunc(targets, func(target string) bool { return !validSHA256ID(target) }) {
		return DurableCommand{}, fmt.Errorf("durable activation requires identity and targets")
	}
	encoded, err := marshalStrictJSONValue(value)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type:           "activate_wait",
		ControlVersion: "cymule.durable-control/4",
		ActivationID:   activationID,
		Source:         &source,
		WaitIDs:        targets,
		Value:          encoded,
	}, nil
}

// ReleaseDurableEffect builds one explicit effect-release command.
func ReleaseDurableEffect(intentID string, execution ExecutionClaimRequest) DurableCommand {
	return DurableCommand{
		Type: "release_effect", ControlVersion: "cymule.durable-control/4", IntentID: intentID,
		Execution: &execution,
	}
}

// ResolveDurableEffect commits one exact claimed-effect reconciliation result.
func ResolveDurableEffect(
	resolutionID, runID, intentID string,
	executionBinding ArtifactRef,
	occurrenceBinding, claimOwner string,
	claimEpoch uint64,
	resolution string,
	value any,
) (DurableCommand, error) {
	if !validClockIdentity(resolutionID) || !validRunIdentity(runID) ||
		!validSHA256ID(intentID) || validateArtifactRef(executionBinding) != nil ||
		executionBinding.Kind != "cymule.execution-binding/2" ||
		!validSHA256ID(occurrenceBinding) || !validClockIdentity(claimOwner) ||
		claimEpoch == 0 || claimEpoch > maxExactInteger ||
		!slices.Contains([]string{"resolved_applied", "resolved_not_applied"}, resolution) {
		return DurableCommand{}, fmt.Errorf("durable effect resolution authority is invalid")
	}
	encoded, err := marshalStrictJSONValue(value)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type: "resolve_effect", ControlVersion: "cymule.durable-control/4",
		ResolutionID: resolutionID, RunID: runID, IntentID: intentID,
		ExecutionBinding: &executionBinding, OccurrenceBinding: occurrenceBinding,
		ClaimOwner: claimOwner, ClaimEpoch: claimEpoch, Resolution: resolution, Value: encoded,
	}, nil
}

// CancelDurableRun builds one provider-independent semantic cancellation.
func CancelDurableRun(cancellationID, runID string, reason any) (DurableCommand, error) {
	if cancellationID == "" || !validRunIdentity(runID) {
		return DurableCommand{}, fmt.Errorf("durable cancellation and Run identities are required")
	}
	encoded, err := marshalStrictJSONValue(reason)
	if err != nil {
		return DurableCommand{}, err
	}
	return DurableCommand{
		Type: "cancel_run", ControlVersion: "cymule.durable-control/4",
		CancellationID: cancellationID, RunID: runID, Reason: encoded,
	}, nil
}

// QueryDurableRunIndexPage builds one revision-pinned domain Run-index read.
func QueryDurableRunIndexPage(options DurablePageQueryOptions) (DurableCommand, error) {
	return durablePageCommand("run_index_page", DurableRunIndexQuery, "", options)
}

// QueryDurableRunCurrent builds one bounded semantic Run-current read.
func QueryDurableRunCurrent(runID string, expectedRevision *string) (DurableCommand, error) {
	if !validRunIdentity(runID) || !validOptionalContentID(expectedRevision) {
		return DurableCommand{}, fmt.Errorf("durable Run-current query is invalid")
	}
	return DurableCommand{
		Type: "run_current", ControlVersion: DurableControlVersion,
		RunID: runID, ExpectedRevision: expectedRevision,
	}, nil
}

// QueryDurableRunWaitPage builds one revision-pinned wait-summary read.
func QueryDurableRunWaitPage(runID string, options DurablePageQueryOptions) (DurableCommand, error) {
	return durablePageCommand("run_wait_page", DurableRunWaitsQuery, runID, options)
}

// QueryDurableRunEffectPage builds one revision-pinned Effect-summary read.
func QueryDurableRunEffectPage(runID string, options DurablePageQueryOptions) (DurableCommand, error) {
	return durablePageCommand("run_effect_page", DurableRunEffectsQuery, runID, options)
}

// QueryDurableRunOccurrencePage builds one revision-pinned occurrence-summary read.
func QueryDurableRunOccurrencePage(runID string, options DurablePageQueryOptions) (DurableCommand, error) {
	return durablePageCommand("run_occurrence_page", DurableRunOccurrencesQuery, runID, options)
}

// QueryDurableRunAttemptPage builds one revision-pinned Attempt-summary read.
func QueryDurableRunAttemptPage(runID string, options DurablePageQueryOptions) (DurableCommand, error) {
	return durablePageCommand("run_attempt_page", DurableRunAttemptsQuery, runID, options)
}

// QueryDurableRunItem builds one bounded exact-leaf read.
func QueryDurableRunItem(query DurableRunItemQuery) (DurableCommand, error) {
	if !validRunIdentity(query.RunID) || !validOptionalContentID(query.ExpectedRevision) ||
		validateDurableRunItemSelector(query.Selector) != nil ||
		query.MaxCanonicalBytes == 0 || query.MaxCanonicalBytes > maxDurableQueryExactResponseBytes {
		return DurableCommand{}, fmt.Errorf("exact durable Run-item query is invalid")
	}
	selector := query.Selector
	return DurableCommand{
		Type: "run_item", ControlVersion: DurableControlVersion, RunID: query.RunID,
		ExpectedRevision: query.ExpectedRevision, Selector: &selector,
		MaxCanonicalBytes: query.MaxCanonicalBytes,
	}, nil
}

func durablePageCommand(
	commandType string,
	queryKind DurablePageQueryKind,
	runID string,
	options DurablePageQueryOptions,
) (DurableCommand, error) {
	if runID != "" && !validRunIdentity(runID) || !validOptionalContentID(options.ExpectedRevision) ||
		options.Limit == 0 || options.Limit > maxDurableQueryPageItems ||
		options.MaxCanonicalBytes == 0 || options.MaxCanonicalBytes > maxDurableQueryPageBytes {
		return DurableCommand{}, fmt.Errorf("durable page query is outside its closed bounds")
	}
	if options.Cursor != nil {
		var expectedRunID *string
		if runID != "" {
			expectedRunID = &runID
		}
		if validateDurablePageCursor(options.Cursor) != nil ||
			options.Cursor.QueryKind != queryKind ||
			!equalOptionalString(options.Cursor.RunID, expectedRunID) ||
			options.ExpectedRevision == nil ||
			*options.ExpectedRevision != options.Cursor.SourceRevision {
			return DurableCommand{}, fmt.Errorf("durable page cursor belongs to another authority")
		}
	}
	return DurableCommand{
		Type: commandType, ControlVersion: DurableControlVersion, RunID: runID,
		ExpectedRevision: options.ExpectedRevision, Cursor: options.Cursor,
		Limit: options.Limit, MaxCanonicalBytes: options.MaxCanonicalBytes,
	}, nil
}

func validOptionalContentID(value *string) bool {
	return value == nil || validSHA256ID(*value)
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

// NewResourceHandoff creates one provenance-closed M1 Run-to-Run handoff record.
func NewResourceHandoff(transferID string, producer ResourceProducerProvenance, toRun, slot string, resource ArtifactRef) (ResourceHandoff, error) {
	if !validClockIdentity(transferID) || !validRunIdentity(producer.RunID) ||
		!validClockIdentity(producer.OccurrenceID) || !validRunIdentity(toRun) ||
		!validClockIdentity(slot) || producer.RunID == toRun ||
		validateArtifactRef(producer.Result) != nil || validateArtifactRef(resource) != nil ||
		producer.Result != resource {
		return ResourceHandoff{}, fmt.Errorf("resource handoff provenance is invalid")
	}
	return ResourceHandoff{
		HandoffVersion: "cymule.resource-handoff/5",
		TransferID:     transferID,
		Producer:       producer,
		ToRun:          toRun,
		Slot:           slot,
		Resource:       resource,
	}, nil
}

// TextResource creates one inline UTF-8 Resource Candidate.
func TextResource(text string, annotations map[string]string) ResourceCandidate {
	return ResourceCandidate{
		ResourceVersion: "cymule.resource/3",
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
		ResourceVersion: "cymule.resource/3",
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
		ResourceVersion: "cymule.resource/3",
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
	ID                 string            `json:"id"`
	InputSchema        map[string]any    `json:"input_schema"`
	OutputSchema       map[string]any    `json:"output_schema"`
	OutputArtifactKind string            `json:"output_artifact_kind"`
	Requirements       map[string]string `json:"requirements"`
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
	if err := validateSealedPlanWire(SealedPlan(decoded)); err != nil {
		return err
	}
	*plan = SealedPlan(decoded)
	return nil
}

func validateSealedPlanWire(plan SealedPlan) error {
	if !validSHA256ID(plan.PlanID) || plan.Candidate.IRVersion != "cymule.ir/3" ||
		!validPlanWireID(plan.Candidate.Name) || !validPlanWireID(plan.Candidate.Entry) ||
		plan.Candidate.Components == nil || plan.Candidate.Effects == nil ||
		len(plan.Candidate.Definitions) == 0 || plan.Candidate.Metadata == nil {
		return fmt.Errorf("sealed Plan identity or candidate is invalid")
	}
	for _, contract := range plan.Candidate.Components {
		if !validPlanWireID(contract.ID) || contract.InputSchema == nil ||
			contract.OutputSchema == nil || !validArtifactKind(contract.OutputArtifactKind) ||
			contract.Requirements == nil {
			return fmt.Errorf("sealed Plan component contract is invalid")
		}
	}
	for _, contract := range plan.Candidate.Effects {
		if !validPlanWireID(contract.ID) || contract.InputSchema == nil ||
			contract.OutputSchema == nil || contract.Requirements == nil ||
			!slices.Contains([]string{"observational", "mutating"}, contract.Profile.Mutation) ||
			!slices.Contains([]string{"eager", "on_scope_commit", "explicit"}, contract.Profile.Dispatch) ||
			!slices.Contains([]string{"queryable", "externally_attested", "human", "impossible"}, contract.Profile.Reconciliation) {
			return fmt.Errorf("sealed Plan Effect contract is invalid")
		}
	}
	for _, definition := range plan.Candidate.Definitions {
		if !validPlanWireID(definition.ID) || definition.InputSchema == nil ||
			definition.OutputSchema == nil {
			return fmt.Errorf("sealed Plan definition identity is invalid")
		}
		if err := validatePlanRegionWire(definition.Body); err != nil {
			return err
		}
	}
	return nil
}

func validatePlanRegionWire(region Region) error {
	if region.Steps == nil {
		return fmt.Errorf("sealed Plan Region steps are invalid")
	}
	for _, step := range region.Steps {
		if err := validatePlanStepWire(step); err != nil {
			return err
		}
	}
	return validatePlanExpressionWire(region.Result)
}

// Published reusable definitions are frozen registry drafts, not sealed Plans.
// Their wire must deserialize as the closed Rust IR, while Plan-wide semantic
// admission (including non-empty nested site IDs) happens only when a parent is
// linked and sealed by Rust authority.
func validateRegistryRegionWire(region Region) error {
	if region.Steps == nil {
		return fmt.Errorf("subflow definition Region steps are invalid")
	}
	for _, step := range region.Steps {
		if err := validateRegistryStepWire(step); err != nil {
			return err
		}
	}
	return validateRegistryExpressionWire(region.Result)
}

func validatePlanTemplateWire(template PlanTemplate) error {
	if !validRegistryIdentity(template.TemplateID) || template.References == nil {
		return fmt.Errorf("Plan template identity or references are invalid")
	}
	if err := validatePlanCandidateWire(template.Candidate); err != nil {
		return err
	}
	logicalReferences := make(map[string]struct{}, len(template.References))
	localDefinitions := make(map[string]struct{}, len(template.Candidate.Definitions)+len(template.References))
	for _, definition := range template.Candidate.Definitions {
		localDefinitions[definition.ID] = struct{}{}
	}
	for _, reference := range template.References {
		if !validRegistryName(reference.LogicalRef) || !validRegistryName(reference.LocalDefinition) {
			return fmt.Errorf("Plan template reference identity is invalid")
		}
		if _, exists := logicalReferences[reference.LogicalRef]; exists {
			return fmt.Errorf("Plan template repeats a logical reference")
		}
		logicalReferences[reference.LogicalRef] = struct{}{}
		if _, exists := localDefinitions[reference.LocalDefinition]; exists {
			return fmt.Errorf("Plan template repeats a local definition")
		}
		localDefinitions[reference.LocalDefinition] = struct{}{}
		switch reference.Strategy.Strategy {
		case "latest_compatible":
			if reference.Strategy.RevisionID != "" {
				return fmt.Errorf("latest-compatible reference carries a revision")
			}
		case "pinned":
			if !validSHA256ID(reference.Strategy.RevisionID) {
				return fmt.Errorf("pinned reference revision is invalid")
			}
		default:
			return fmt.Errorf("Plan template reference strategy is invalid")
		}
	}
	return nil
}

func validatePlanCandidateWire(candidate PlanCandidate) error {
	return validateSealedPlanWire(SealedPlan{
		PlanID:    "sha256:" + strings.Repeat("0", 64),
		Candidate: candidate,
	})
}

func validateRegistryStepWire(step Step) error {
	op, ok := step["op"].(string)
	if !ok {
		return fmt.Errorf("subflow definition operation tag is invalid")
	}
	fields := []string{"id", "op"}
	stringFields := []string{"id"}
	switch op {
	case "call":
		fields = append(fields, "component", "input")
		stringFields = append(stringFields, "component")
	case "invoke":
		fields = append(fields, "definition", "input")
		stringFields = append(stringFields, "definition")
	case "wait":
		fields = append(fields, "wait")
	case "effect":
		fields = append(fields, "effect", "input", "occurrence")
		stringFields = append(stringFields, "effect", "occurrence")
	case "scope":
		fields = append(fields, "body")
	default:
		return fmt.Errorf("subflow definition operation %q is not a wire operation", op)
	}
	if _, present := step["bind"]; present {
		fields = append(fields, "bind")
		stringFields = append(stringFields, "bind")
	}
	if err := requireExactJSONFields(step, fields); err != nil {
		return err
	}
	for _, field := range stringFields {
		if _, ok := step[field].(string); !ok {
			return fmt.Errorf("subflow definition operation %s is invalid", field)
		}
	}
	if input, present := step["input"]; present {
		expression, ok := input.(map[string]any)
		if !ok {
			return fmt.Errorf("subflow definition operation input is invalid")
		}
		if err := validateRegistryExpressionWire(Expression(expression)); err != nil {
			return err
		}
	}
	switch op {
	case "wait":
		wait, ok := step["wait"].(map[string]any)
		if !ok {
			return fmt.Errorf("subflow definition wait is invalid")
		}
		return validateRegistryWaitWire(wait)
	case "scope":
		body, ok := step["body"].(map[string]any)
		if !ok {
			return fmt.Errorf("subflow definition scope body is invalid")
		}
		var region Region
		if err := decodeClosedValue(body, &region); err != nil {
			return err
		}
		return validateRegistryRegionWire(region)
	default:
		return nil
	}
}

func validateRegistryWaitWire(wait map[string]any) error {
	kind, ok := wait["kind"].(string)
	if !ok {
		return fmt.Errorf("subflow definition wait tag is invalid")
	}
	var fields []string
	switch kind {
	case "signal":
		fields = []string{"kind", "key", "consume_once"}
		if _, ok := wait["key"].(string); !ok {
			return fmt.Errorf("subflow definition signal wait is invalid")
		}
		if _, ok := wait["consume_once"].(bool); !ok {
			return fmt.Errorf("subflow definition signal wait is invalid")
		}
	case "timer":
		fields = []string{"kind", "timer_id"}
		if _, ok := wait["timer_id"].(string); !ok {
			return fmt.Errorf("subflow definition timer wait is invalid")
		}
	case "input":
		fields = []string{"kind", "correlation", "schema"}
		if _, ok := wait["correlation"].(string); !ok {
			return fmt.Errorf("subflow definition input wait is invalid")
		}
	default:
		return fmt.Errorf("subflow definition wait %q is not a wire wait", kind)
	}
	return requireExactJSONFields(wait, fields)
}

func validateRegistryExpressionWire(expression Expression) error {
	kind, ok := expression["kind"].(string)
	if !ok {
		return fmt.Errorf("subflow definition expression tag is invalid")
	}
	switch kind {
	case "input":
		return requireExactJSONFields(expression, []string{"kind"})
	case "literal":
		return requireExactJSONFields(expression, []string{"kind", "value"})
	case "binding":
		if err := requireExactJSONFields(expression, []string{"kind", "name"}); err != nil {
			return err
		}
		if _, ok := expression["name"].(string); !ok {
			return fmt.Errorf("subflow definition binding expression is invalid")
		}
		return nil
	case "object":
		if err := requireExactJSONFields(expression, []string{"kind", "fields"}); err != nil {
			return err
		}
		fields, ok := expression["fields"].(map[string]any)
		if !ok {
			return fmt.Errorf("subflow definition object expression is invalid")
		}
		for _, value := range fields {
			nested, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("subflow definition object field expression is invalid")
			}
			if err := validateRegistryExpressionWire(Expression(nested)); err != nil {
				return err
			}
		}
		return nil
	case "array":
		if err := requireExactJSONFields(expression, []string{"kind", "items"}); err != nil {
			return err
		}
		items, ok := expression["items"].([]any)
		if !ok {
			return fmt.Errorf("subflow definition array expression is invalid")
		}
		for _, value := range items {
			nested, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("subflow definition array item expression is invalid")
			}
			if err := validateRegistryExpressionWire(Expression(nested)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("subflow definition expression %q is invalid", kind)
	}
}

func validatePlanStepWire(step Step) error {
	op, ok := step["op"].(string)
	if !ok {
		return fmt.Errorf("sealed Plan operation tag is invalid")
	}
	fields := []string{"id", "op"}
	stringFields := []string{"id"}
	switch op {
	case "call":
		fields = append(fields, "component", "input")
		stringFields = append(stringFields, "component")
	case "invoke":
		fields = append(fields, "definition", "input")
		stringFields = append(stringFields, "definition")
	case "wait":
		fields = append(fields, "wait")
	case "effect":
		fields = append(fields, "effect", "input", "occurrence")
		stringFields = append(stringFields, "effect", "occurrence")
	case "scope":
		fields = append(fields, "body")
	default:
		return fmt.Errorf("sealed Plan operation %q is not a wire operation", op)
	}
	if _, present := step["bind"]; present {
		fields = append(fields, "bind")
		stringFields = append(stringFields, "bind")
	}
	if err := requireExactJSONFields(step, fields); err != nil {
		return err
	}
	for _, field := range stringFields {
		value, ok := step[field].(string)
		if !ok || !validPlanWireID(value) {
			return fmt.Errorf("sealed Plan operation %s is invalid", field)
		}
	}
	if input, present := step["input"]; present {
		expression, ok := input.(map[string]any)
		if !ok {
			return fmt.Errorf("sealed Plan operation input is invalid")
		}
		if err := validatePlanExpressionWire(Expression(expression)); err != nil {
			return err
		}
	}
	switch op {
	case "wait":
		wait, ok := step["wait"].(map[string]any)
		if !ok {
			return fmt.Errorf("sealed Plan wait is invalid")
		}
		return validateWaitSpec(wait)
	case "scope":
		body, ok := step["body"].(map[string]any)
		if !ok {
			return fmt.Errorf("sealed Plan scope body is invalid")
		}
		var region Region
		if err := decodeClosedValue(body, &region); err != nil {
			return err
		}
		return validatePlanRegionWire(region)
	default:
		return nil
	}
}

func validatePlanExpressionWire(expression Expression) error {
	kind, ok := expression["kind"].(string)
	if !ok {
		return fmt.Errorf("sealed Plan expression tag is invalid")
	}
	switch kind {
	case "input":
		return requireExactJSONFields(expression, []string{"kind"})
	case "literal":
		return requireExactJSONFields(expression, []string{"kind", "value"})
	case "binding":
		if err := requireExactJSONFields(expression, []string{"kind", "name"}); err != nil {
			return err
		}
		name, ok := expression["name"].(string)
		if !ok || !validPlanWireID(name) {
			return fmt.Errorf("sealed Plan binding expression is invalid")
		}
		return nil
	case "object":
		if err := requireExactJSONFields(expression, []string{"kind", "fields"}); err != nil {
			return err
		}
		fields, ok := expression["fields"].(map[string]any)
		if !ok {
			return fmt.Errorf("sealed Plan object expression is invalid")
		}
		for _, value := range fields {
			nested, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("sealed Plan object field expression is invalid")
			}
			if err := validatePlanExpressionWire(Expression(nested)); err != nil {
				return err
			}
		}
		return nil
	case "array":
		if err := requireExactJSONFields(expression, []string{"kind", "items"}); err != nil {
			return err
		}
		items, ok := expression["items"].([]any)
		if !ok {
			return fmt.Errorf("sealed Plan array expression is invalid")
		}
		for _, value := range items {
			nested, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("sealed Plan array item expression is invalid")
			}
			if err := validatePlanExpressionWire(Expression(nested)); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("sealed Plan expression %q is invalid", kind)
	}
}

func validPlanWireID(value string) bool {
	return value != "" && utf8.RuneCountInString(value) <= 200
}

func validRegistryName(value string) bool {
	return value != "" && len(value) <= 160
}

func validRegistryIdentity(value string) bool {
	return validRegistryName(value) && validWireIdentity(value)
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
	if err := validateExecutionPayloadRawShape(status, object); err != nil {
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

func validateExecutionPayloadRawShape(status string, outcome map[string]any) error {
	payloadField := map[string]string{
		"completed": "result", "suspended": "suspension",
		"release_required": "release", "reconciliation_required": "reconciliation",
	}[status]
	payload, ok := outcome[payloadField].(map[string]any)
	if !ok {
		return fmt.Errorf("execution outcome payload is not an object")
	}
	fields := map[string][]string{
		"completed":               {"run_id", "plan_id", "value", "projection_digest", "precondition_token", "effects"},
		"suspended":               {"run_id", "plan_id", "definition_id", "invocation_id", "site_id", "wait", "result_bind"},
		"release_required":        {"run_id", "plan_id", "intent_ids"},
		"reconciliation_required": {"run_id", "plan_id", "intent_id"},
	}[status]
	if err := requireExactJSONFields(payload, fields); err != nil {
		return err
	}
	switch status {
	case "completed":
		if _, ok := payload["effects"].([]any); !ok {
			return fmt.Errorf("completed execution effects are not an array")
		}
	case "suspended":
		if _, ok := payload["wait"].(map[string]any); !ok {
			return fmt.Errorf("suspended execution wait is not an object")
		}
		if payload["result_bind"] != nil {
			if _, ok := payload["result_bind"].(string); !ok {
				return fmt.Errorf("suspended execution result binding is invalid")
			}
		}
	case "release_required":
		if _, ok := payload["intent_ids"].([]any); !ok {
			return fmt.Errorf("release-required execution intents are not an array")
		}
	}
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
		if !validRunIdentity(result.RunID) || !validSHA256ID(result.PlanID) ||
			!validBareSHA256(result.ProjectionDigest) || !validPreconditionToken(result.PreconditionToken) ||
			result.Effects == nil {
			return fmt.Errorf("completed execution required fields are missing")
		}
		return validateStrictlySortedContentIDs(result.Effects, "completed effect")
	case "suspended":
		boundary := outcome.Suspension
		if !validRunIdentity(boundary.RunID) || !validSHA256ID(boundary.PlanID) ||
			!validEngineWireIdentity(boundary.DefinitionID) || !validSHA256ID(boundary.InvocationID) ||
			!validEngineWireIdentity(boundary.SiteID) {
			return fmt.Errorf("suspension required fields are missing")
		}
		if err := validateWaitSpec(boundary.Wait); err != nil {
			return err
		}
		if boundary.ResultBind != nil && !validEngineWireIdentity(*boundary.ResultBind) {
			return fmt.Errorf("suspension result binding is empty")
		}
		var waitIdentity string
		switch boundary.Wait["kind"] {
		case "signal":
			waitIdentity, _ = boundary.Wait["key"].(string)
		case "timer":
			waitIdentity, _ = boundary.Wait["timer_id"].(string)
		case "input":
			waitIdentity, _ = boundary.Wait["correlation"].(string)
		}
		if !validEngineWireIdentity(waitIdentity) {
			return fmt.Errorf("suspension wait identity is invalid")
		}
	case "release_required":
		release := outcome.Release
		if !validRunIdentity(release.RunID) || !validSHA256ID(release.PlanID) || len(release.IntentIDs) == 0 {
			return fmt.Errorf("effect release required fields are missing")
		}
		return validateStrictlySortedContentIDs(release.IntentIDs, "released effect intent")
	case "reconciliation_required":
		reconciliation := outcome.Reconciliation
		if !validRunIdentity(reconciliation.RunID) || !validSHA256ID(reconciliation.PlanID) ||
			!validSHA256ID(reconciliation.IntentID) {
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
	if !validEvolutionIdentity(request.MigrationID) || !validRunIdentity(request.RunID) ||
		!validSHA256ID(request.FromPlan) || !validSHA256ID(request.ToPlan) ||
		request.FromPlan == request.ToPlan ||
		!validSHA256ID(request.PlanEdgeID) || !validSHA256ID(request.CompatibilityID) ||
		request.ExpectedSourceEpoch > maxExactInteger || !validEvolutionIdentity(request.AdapterID) ||
		!validSHA256ID(request.AdapterRevision) {
		return fmt.Errorf("migration request required fields are missing")
	}
	return nil
}

func validateMigrationContinuation(continuation MigrationContinuation) error {
	if continuation.ContinuationVersion != "cymule.continuation-state/1" || !validRunIdentity(continuation.RunID) || !validSHA256ID(continuation.PlanID) || !validSHA256ID(continuation.BindingContext) ||
		len(continuation.Frames) == 0 || continuation.WaitSet == nil || len(continuation.ScopeStack) == 0 ||
		continuation.Epoch > maxExactInteger || continuation.ExecutionFence > maxExactInteger {
		return fmt.Errorf("migration Continuation required fields are missing")
	}
	if continuation.Status != "ready" || continuation.ExecutionClaim != nil {
		return fmt.Errorf("migration Continuation must be ready without an execution claim")
	}
	if continuation.State != nil {
		if err := validateArtifactRef(*continuation.State); err != nil {
			return err
		}
	}
	for _, frame := range continuation.Frames {
		if frame.DefinitionID == "" || frame.InvocationID == "" || frame.ScopeID == "" ||
			frame.InvocationPath == nil || frame.RegionPath == nil || frame.Locals == nil ||
			frame.NextStep > maxExactInteger || hasUnsafeJSONInteger(frame.RegionPath) {
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
			if segment.SiteID == "" || segment.ScopeID == "" || segment.RegionPath == nil ||
				hasUnsafeJSONInteger(segment.RegionPath) {
				return fmt.Errorf("migration invocation segment required fields are missing")
			}
		}
	}
	return nil
}

func validateRestartRequest(request RestartRequest) error {
	if !validEvolutionIdentity(request.RestartID) || !validRunIdentity(request.ReplacementRun) ||
		!validRunIdentity(request.RunID) || request.ReplacementRun == request.RunID ||
		!validSHA256ID(request.FromPlan) || !validSHA256ID(request.ToPlan) ||
		request.FromPlan == request.ToPlan || request.ExpectedSourceEpoch > maxExactInteger {
		return fmt.Errorf("restart request required fields are missing")
	}
	for _, reference := range []ArtifactRef{request.Input, request.Evidence} {
		if err := validateArtifactRef(reference); err != nil {
			return err
		}
	}
	return nil
}

func hasUnsafeJSONInteger(values []uint64) bool {
	return slices.ContainsFunc(values, func(value uint64) bool {
		return value > maxExactInteger
	})
}

func validSHA256ID(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validLowerHexDigest(value[len("sha256:"):])
}

func validLowerHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func validPreconditionToken(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 4 || parts[0] != "pre" || parts[2] != "sha256" ||
		!validLowerHexDigest(parts[3]) {
		return false
	}
	if parts[1] == "" || (len(parts[1]) > 1 && parts[1][0] == '0') {
		return false
	}
	for _, character := range parts[1] {
		if character < '0' || character > '9' {
			return false
		}
	}
	epoch, err := strconv.ParseUint(parts[1], 10, 64)
	return err == nil && epoch <= maxExactInteger
}

func validWireIdentity(value string) bool {
	if !validUnicodeScalarLength(value, 1, 256) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validEngineWireIdentity(value string) bool {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validRunIdentity(value string) bool {
	if value == "" || !utf8.ValidString(value) || utf8.RuneCountInString(value) > 512 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validateShadowRequest(request ShadowRequest) error {
	if !validEvolutionIdentity(request.ComparisonID) || !validEvolutionIdentity(request.DecisionID) ||
		!validEvolutionIdentity(request.Subject) ||
		!validSHA256ID(request.PrimaryPlan) || !validSHA256ID(request.ShadowPlan) ||
		request.PrimaryPlan == request.ShadowPlan || !validEvolutionIdentity(request.DriverID) ||
		!validSHA256ID(request.DriverRevision) || !validEvolutionIdentity(request.ComparisonPolicy) {
		return fmt.Errorf("shadow request required fields are missing")
	}
	return validateArtifactRef(request.Input)
}

func validateArtifactRef(reference ArtifactRef) error {
	if reference.IdentityVersion != "cymule.artifact/2" || !validArtifactKind(reference.Kind) ||
		!validSHA256ID(reference.ArtifactID) {
		return fmt.Errorf("Artifact reference identity is invalid")
	}
	return nil
}

func validateArtifactRecord(record ArtifactRecord) error {
	if err := validateArtifactRef(record.Reference); err != nil {
		return err
	}
	if len(record.Bytes) > maxArtifactBytes {
		return fmt.Errorf("Artifact record exceeds the 8 MiB byte bound")
	}
	if record.Reference.ArtifactID != artifactRecordID(record.Reference.Kind, record.Bytes) {
		return fmt.Errorf("Artifact record identity does not match its bytes")
	}
	return nil
}

func artifactRecordID(kind string, data []byte) string {
	kindBytes := []byte(kind)
	preimage := make([]byte, 0, len("cymule.artifact/2")+4+len(kindBytes)+8+len(data))
	preimage = append(preimage, "cymule.artifact/2"...)
	var kindLength [4]byte
	binary.BigEndian.PutUint32(kindLength[:], uint32(len(kindBytes)))
	preimage = append(preimage, kindLength[:]...)
	preimage = append(preimage, kindBytes...)
	var bytesLength [8]byte
	binary.BigEndian.PutUint64(bytesLength[:], uint64(len(data)))
	preimage = append(preimage, bytesLength[:]...)
	preimage = append(preimage, data...)
	digest := sha256.Sum256(preimage)
	return fmt.Sprintf("sha256:%x", digest)
}

func validArtifactKind(kind string) bool {
	if kind == "" || len(kind) > 255 || !strings.Contains(kind, "/") {
		return false
	}
	for _, segment := range strings.Split(kind, "/") {
		if segment == "" {
			return false
		}
		for _, character := range []byte(segment) {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') &&
				character != '.' && character != '_' && character != '-' && character != '+' {
				return false
			}
		}
	}
	return true
}

func validateOccurrencePin(pin OccurrencePin) error {
	if !validWireIdentity(pin.OccurrenceID) || !validWireIdentity(pin.TemplateID) ||
		!validWireIdentity(pin.DecisionID) || !validSHA256ID(pin.PlanID) ||
		!validWireIdentity(pin.SelectionID) {
		return fmt.Errorf("occurrence pin lineage is incomplete")
	}
	if err := validateArtifactRef(pin.ExecutionBinding); err != nil {
		return err
	}
	if pin.ExecutionBinding.Kind != "cymule.execution-binding/2" {
		return fmt.Errorf("occurrence pin binding is not an ExecutionBinding Artifact")
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
		IRVersion:  "cymule.ir/3",
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
func (builder *FlowBuilder) Component(id string, inputSchema, outputSchema map[string]any, outputArtifactKind string, requirements map[string]string) *FlowBuilder {
	builder.candidate.Components = append(builder.candidate.Components, Contract{
		ID: id, InputSchema: inputSchema, OutputSchema: outputSchema,
		OutputArtifactKind: outputArtifactKind, Requirements: cloneStrings(requirements),
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

// Effect appends an external effect with an optional eager-observation result binding.
func (builder *FlowBuilder) Effect(site, effect string, input Expression, occurrence string, bind ...string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	step := Step{
		"id": site, "op": "effect", "effect": effect, "input": input, "occurrence": occurrence,
	}
	if len(bind) > 0 {
		step["bind"] = bind[0]
	}
	entry.Body.Steps = append(entry.Body.Steps, step)
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

// Scope appends a structured auto-commit scope.
func (builder *FlowBuilder) Scope(site string, body Region, bind string) *FlowBuilder {
	entry := &builder.candidate.Definitions[0]
	entry.Body.Steps = append(entry.Body.Steps, Step{
		"id": site, "op": "scope", "body": body, "bind": bind,
	})
	return builder
}

// Finish returns a complete candidate.
func (builder *FlowBuilder) Finish(result Expression) PlanCandidate {
	builder.candidate.Definitions[0].Body.Result = result
	if err := validateGoJSONStrings(reflect.ValueOf(builder.candidate)); err != nil {
		panic(fmt.Sprintf("Flow candidate is outside strict JSON: %v", err))
	}
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

// EngineProcessConfig is the complete ambient-cleared process realization.
type EngineProcessConfig struct {
	Executable       string            `json:"executable"`
	Arguments        []string          `json:"arguments"`
	Environment      map[string]string `json:"environment"`
	WorkingDirectory *string           `json:"working_directory"`
	RuntimeClosure   map[string]string `json:"runtime_closure"`
	TimeoutMS        uint64            `json:"timeout_ms"`
	MessageLimit     uint64            `json:"message_limit"`
	ClosureLimit     uint64            `json:"closure_limit"`
}

// UnmarshalJSON preserves the required nullable working-directory member and rejects open shapes.
func (process *EngineProcessConfig) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Engine process configuration is not an object")
	}
	if err := requireExactJSONFields(object, []string{
		"executable", "arguments", "environment", "working_directory", "runtime_closure",
		"timeout_ms", "message_limit", "closure_limit",
	}); err != nil {
		return err
	}
	type wire EngineProcessConfig
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedProcess := EngineProcessConfig(decoded)
	if err := validateEngineProcessConfig(decodedProcess); err != nil {
		return err
	}
	if !wireValuesEqual(value, decodedProcess) {
		return fmt.Errorf("Engine process configuration loses JSON member presence")
	}
	*process = decodedProcess
	return nil
}

// EnginePluginTarget selects one complete process implementation, optionally by exact revision.
type EnginePluginTarget struct {
	Provider string              `json:"provider"`
	Process  EngineProcessConfig `json:"process"`
	Revision string              `json:"revision,omitempty"`
}

// UnmarshalJSON rejects the superseded location-only target and explicit-null revision.
func (target *EnginePluginTarget) UnmarshalJSON(input []byte) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Engine plugin target is not an object")
	}
	if err := requireRequiredAllowedJSONFields(
		object, []string{"provider", "process"}, []string{"provider", "process", "revision"},
	); err != nil {
		return err
	}
	type wire EnginePluginTarget
	var decoded wire
	if err := decodeClosedValue(value, &decoded); err != nil {
		return err
	}
	decodedTarget := EnginePluginTarget(decoded)
	if err := validateEnginePluginTarget(decodedTarget, false); err != nil {
		return err
	}
	if !wireValuesEqual(value, decodedTarget) {
		return fmt.Errorf("Engine plugin target loses JSON member presence")
	}
	*target = decodedTarget
	return nil
}

// EngineClockTarget selects one exact persistence-backed Clock authority.
type EngineClockTarget struct {
	Provider         string `json:"provider"`
	Location         string `json:"location"`
	SourceID         string `json:"source_id"`
	SourceGeneration string `json:"source_generation"`
}

// EngineDurableTarget separates durable storage from optional execution authority.
type EngineDurableTarget struct {
	Store    EngineStoreTarget   `json:"store"`
	Executor *EnginePluginTarget `json:"executor,omitempty"`
	Clock    *EngineClockTarget  `json:"clock,omitempty"`
}

// EngineMigrationProviderTarget binds one semantic adapter to its exact process realization.
type EngineMigrationProviderTarget struct {
	AdapterID       string             `json:"adapter_id"`
	AdapterRevision string             `json:"adapter_revision"`
	Process         EnginePluginTarget `json:"process"`
}

// EngineShadowProviderTarget binds one semantic driver to its exact process realization.
type EngineShadowProviderTarget struct {
	DriverID       string             `json:"driver_id"`
	DriverRevision string             `json:"driver_revision"`
	Process        EnginePluginTarget `json:"process"`
}

// EngineEvolutionTarget carries the exact required-nullable M4 provider selection.
type EngineEvolutionTarget struct {
	Store                   EngineStoreTarget              `json:"store"`
	MigrationAdapter        *EngineMigrationProviderTarget `json:"migration_adapter"`
	ShadowDriver            *EngineShadowProviderTarget    `json:"shadow_driver"`
	TargetExecutionBindings map[string]EnginePluginTarget  `json:"target_execution_bindings"`
}

// DirectoryStore selects the official directory store.
func DirectoryStore(location string) EngineStoreTarget {
	return EngineStoreTarget{Provider: "cymule.directory-store/5", Location: location}
}

// SQLiteStore selects one domain in the official SQLite store.
func SQLiteStore(location, domain string) EngineStoreTarget {
	return EngineStoreTarget{Provider: "cymule.sqlite-store/6", Location: location, Domain: domain}
}

// ProcessPlugin selects the official sealed process provider without a revision pin.
func ProcessPlugin(process EngineProcessConfig) EnginePluginTarget {
	return EnginePluginTarget{Provider: "cymule.executor-process/1", Process: process}
}

// PinnedProcessPlugin selects an exact revision of the official sealed process provider.
func PinnedProcessPlugin(process EngineProcessConfig, revision string) EnginePluginTarget {
	return EnginePluginTarget{
		Provider: "cymule.executor-process/1", Process: process, Revision: revision,
	}
}

// SQLiteClock selects the official retained-receipt Clock authority.
func SQLiteClock(location, sourceID, sourceGeneration string) EngineClockTarget {
	return EngineClockTarget{
		Provider: "cymule.clock-system/2", Location: location,
		SourceID: sourceID, SourceGeneration: sourceGeneration,
	}
}

func validateEngineStoreTarget(target EngineStoreTarget) error {
	if !validUnicodeScalarLength(target.Provider, 1, 256) ||
		!validUnicodeScalarLength(target.Location, 1, 4096) {
		return fmt.Errorf("Engine Store target is invalid")
	}
	if target.Domain != "" && !validUnicodeScalarLength(target.Domain, 1, 512) {
		return fmt.Errorf("Engine Store target domain is invalid")
	}
	return nil
}

func validateEngineProcessConfig(process EngineProcessConfig, expectedMessageLimit ...uint64) error {
	if !filepath.IsAbs(process.Executable) || strings.ContainsRune(process.Executable, '\x00') ||
		!validUnicodeScalarLength(process.Executable, 1, 4096) || process.Arguments == nil ||
		len(process.Arguments) > 4096 || process.Environment == nil || len(process.Environment) > 4096 ||
		len(process.RuntimeClosure) == 0 || len(process.RuntimeClosure) > 4096 || process.TimeoutMS == 0 ||
		process.TimeoutMS > maxExactInteger || process.MessageLimit == 0 ||
		process.MessageLimit > 64*1024*1024 || process.ClosureLimit == 0 ||
		process.ClosureLimit > 1024*1024*1024 {
		return fmt.Errorf("Engine process configuration is invalid")
	}
	for _, argument := range process.Arguments {
		if strings.ContainsRune(argument, '\x00') {
			return fmt.Errorf("Engine process argument is invalid")
		}
	}
	for key, value := range process.Environment {
		if !validProcessMapKey(key) || strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("Engine process environment is invalid")
		}
	}
	if process.WorkingDirectory != nil {
		workingDirectory := *process.WorkingDirectory
		if !filepath.IsAbs(workingDirectory) || strings.ContainsRune(workingDirectory, '\x00') ||
			!validUnicodeScalarLength(workingDirectory, 1, 4096) {
			return fmt.Errorf("Engine process working directory is invalid")
		}
	}
	for key, value := range process.RuntimeClosure {
		if !validProcessMapKey(key) || !validSHA256ID(value) {
			return fmt.Errorf("Engine process runtime closure is invalid")
		}
	}
	if len(expectedMessageLimit) > 1 || len(expectedMessageLimit) == 0 &&
		process.MessageLimit != ordinaryPluginMessageBytes &&
		process.MessageLimit != evolutionPluginMessageBytes ||
		len(expectedMessageLimit) == 1 && process.MessageLimit != expectedMessageLimit[0] {
		return fmt.Errorf("Engine process message limit does not match its protocol context")
	}
	return nil
}

func validProcessMapKey(value string) bool {
	if value == "" || strings.ContainsRune(value, '=') {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validateEnginePluginTarget(
	target EnginePluginTarget,
	requireRevision bool,
	expectedMessageLimit ...uint64,
) error {
	if target.Provider != "cymule.executor-process/1" ||
		(requireRevision && target.Revision == "") ||
		(target.Revision != "" && !validSHA256ID(target.Revision)) {
		return fmt.Errorf("Engine plugin target is invalid")
	}
	return validateEngineProcessConfig(target.Process, expectedMessageLimit...)
}

func validateEngineClockTarget(target EngineClockTarget) error {
	if target.Provider != "cymule.clock-system/2" ||
		!validUnicodeScalarLength(target.Location, 1, 4096) ||
		!validClockIdentity(target.SourceID) || !validSHA256ID(target.SourceGeneration) {
		return fmt.Errorf("Engine Clock target is invalid")
	}
	return nil
}

func durableCommandNeedsExecutor(command DurableCommand) bool {
	return slices.Contains([]string{
		"start_run", "resume_run", "takeover_run", "release_effect", "resolve_effect",
	}, command.Type)
}

func durableCommandNeedsClock(command DurableCommand) bool {
	return slices.Contains([]string{"start_run", "resume_run", "takeover_run", "release_effect"}, command.Type)
}

func validateEngineDurableTarget(target EngineDurableTarget, command DurableCommand) error {
	if err := validateEngineStoreTarget(target.Store); err != nil {
		return err
	}
	needsExecutor := durableCommandNeedsExecutor(command)
	if needsExecutor != (target.Executor != nil) {
		return fmt.Errorf("durable Engine executor presence does not match its command")
	}
	if target.Executor != nil {
		if err := validateEnginePluginTarget(*target.Executor, false, ordinaryPluginMessageBytes); err != nil {
			return err
		}
	}
	needsClock := durableCommandNeedsClock(command)
	if needsClock != (target.Clock != nil) {
		return fmt.Errorf("durable Engine Clock presence does not match its command")
	}
	if target.Clock != nil {
		return validateEngineClockTarget(*target.Clock)
	}
	return nil
}

func validateEngineEvolutionTarget(target EngineEvolutionTarget, command LiveEvolutionCommand) error {
	if err := validateEngineStoreTarget(target.Store); err != nil {
		return err
	}
	required := "none"
	targetPlan := ""
	if command.Operation == "apply" && command.Command != nil {
		switch command.Command.Operation {
		case "migrate":
			required = "migration"
			if command.Command.Migration != nil {
				targetPlan = command.Command.Migration.ToPlan
			}
		case "shadow":
			required = "shadow"
		}
	}
	if target.TargetExecutionBindings == nil || len(target.TargetExecutionBindings) > 1 {
		return fmt.Errorf("target execution bindings are outside bounds")
	}
	for planID, executionTarget := range target.TargetExecutionBindings {
		if !validSHA256ID(planID) || targetPlan == "" || planID != targetPlan {
			return fmt.Errorf("target execution binding Plan is invalid")
		}
		if err := validateEnginePluginTarget(
			executionTarget, true, ordinaryPluginMessageBytes,
		); err != nil {
			return err
		}
	}
	validProviderShape := false
	switch required {
	case "migration":
		validProviderShape = target.ShadowDriver == nil &&
			(target.MigrationAdapter == nil && len(target.TargetExecutionBindings) == 0 ||
				target.MigrationAdapter != nil && len(target.TargetExecutionBindings) == 1)
	case "shadow":
		validProviderShape = target.MigrationAdapter == nil && len(target.TargetExecutionBindings) == 0
	default:
		validProviderShape = target.MigrationAdapter == nil && target.ShadowDriver == nil &&
			len(target.TargetExecutionBindings) == 0
	}
	if !validProviderShape {
		return fmt.Errorf("evolution Engine plugin presence does not match its command")
	}
	if target.MigrationAdapter != nil {
		adapter := target.MigrationAdapter
		if command.Command == nil || command.Command.Migration == nil ||
			adapter.AdapterID != command.Command.Migration.AdapterID ||
			adapter.AdapterRevision != command.Command.Migration.AdapterRevision {
			return fmt.Errorf("migration Engine target does not match its semantic command")
		}
		if err := validateEnginePluginTarget(adapter.Process, true, evolutionPluginMessageBytes); err != nil {
			return err
		}
		if adapter.Process.Revision != adapter.AdapterRevision {
			return fmt.Errorf("migration Engine process revision does not match the adapter revision")
		}
	}
	if target.ShadowDriver != nil {
		driver := target.ShadowDriver
		if command.Command == nil || command.Command.Shadow == nil ||
			driver.DriverID != command.Command.Shadow.DriverID ||
			driver.DriverRevision != command.Command.Shadow.DriverRevision {
			return fmt.Errorf("shadow Engine target does not match its semantic command")
		}
		if err := validateEnginePluginTarget(driver.Process, true, evolutionPluginMessageBytes); err != nil {
			return err
		}
		if driver.Process.Revision != driver.DriverRevision {
			return fmt.Errorf("shadow Engine process revision does not match the driver revision")
		}
	}
	return nil
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

// ObserveClock issues one retained logical Clock reference for a Run.
func (engine CliEngine) ObserveClock(target EngineClockTarget, runID string) (ClockObservationResult, error) {
	if !validRunIdentity(runID) || validateEngineClockTarget(target) != nil {
		return ClockObservationResult{}, validationFailure(
			"invalid_engine_request", "Clock observation request is invalid",
		)
	}
	var response struct {
		Type   string                 `json:"type"`
		Result ClockObservationResult `json:"result"`
	}
	err := engine.request(map[string]any{
		"type": "observe_clock", "target": target, "run_id": runID,
	}, &response)
	if err == nil && response.Type != "clock_observed" {
		err = unexpectedEngineResponse("clock_observed", response.Type)
	}
	return response.Result, err
}

func validateResourceHandle(resource ResourceHandle) error {
	if resource.ResourceVersion != "cymule.resource/3" || !validSHA256(resource.ResourceID) ||
		!slices.Contains([]string{"inline", "object", "collection", "directory", "snapshot"}, resource.Shape) ||
		!validResourceMediaType(resource.MediaType) {
		return fmt.Errorf("Resource Handle fields are invalid")
	}
	switch resource.Integrity.Kind {
	case "inline":
		if resource.Integrity.Digest != "" || resource.Integrity.Size != 0 ||
			resource.Integrity.Authority != "" || resource.Integrity.Version != "" ||
			resource.Integrity.Identity != "" {
			return fmt.Errorf("inline Resource integrity is invalid")
		}
	case "content":
		if !validSHA256(resource.Integrity.Digest) || resource.Integrity.Size > maxExactInteger ||
			resource.Integrity.Authority != "" || resource.Integrity.Version != "" ||
			resource.Integrity.Identity != "" {
			return fmt.Errorf("content Resource integrity is invalid")
		}
	case "version":
		if !validResourceToken(resource.Integrity.Authority) || !validResourceToken(resource.Integrity.Version) ||
			resource.Integrity.Digest != "" || resource.Integrity.Size != 0 || resource.Integrity.Identity != "" {
			return fmt.Errorf("version Resource integrity is invalid")
		}
	case "live":
		if !validResourceToken(resource.Integrity.Identity) || resource.Integrity.Digest != "" ||
			resource.Integrity.Size != 0 || resource.Integrity.Authority != "" || resource.Integrity.Version != "" {
			return fmt.Errorf("live Resource integrity is invalid")
		}
	default:
		return fmt.Errorf("Resource integrity kind is invalid")
	}
	if resource.Shape == "inline" {
		if resource.Inline == nil || resource.Integrity.Kind != "inline" || resource.Manifest != nil {
			return fmt.Errorf("inline Resource evidence is invalid")
		}
		if err := validateInlineResource(*resource.Inline); err != nil {
			return err
		}
	} else if resource.Inline != nil || resource.Integrity.Kind == "inline" {
		return fmt.Errorf("external Resource retained inline data")
	}
	if resource.Manifest != nil {
		if err := validateResourceManifest(*resource.Manifest); err != nil {
			return err
		}
		if !slices.Contains([]string{"collection", "directory", "snapshot"}, resource.Shape) ||
			resource.Integrity.Kind != "content" ||
			resource.Integrity.Digest != resource.Manifest.Digest ||
			resource.Integrity.Size != resource.Manifest.Size {
			return fmt.Errorf("Resource manifest does not match content integrity")
		}
	}
	if resource.Annotations != nil && len(resource.Annotations) == 0 {
		return fmt.Errorf("Resource annotations must be omitted when empty")
	}
	for key, value := range resource.Annotations {
		if !validResourceToken(key) || len(value) > 4096 {
			return fmt.Errorf("Resource annotation is invalid")
		}
	}
	return nil
}

func validateResourceHandleWire(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Resource Handle is not an object")
	}
	if err := requireRequiredAllowedJSONFields(
		object,
		[]string{"resource_id", "resource_version", "shape", "media_type", "integrity"},
		[]string{"resource_id", "resource_version", "shape", "media_type", "inline", "integrity", "manifest", "annotations"},
	); err != nil {
		return err
	}
	integrity, ok := object["integrity"].(map[string]any)
	if !ok {
		return fmt.Errorf("Resource integrity is not an object")
	}
	kind, ok := integrity["kind"].(string)
	if !ok {
		return fmt.Errorf("Resource integrity kind is missing")
	}
	integrityFields := map[string][]string{
		"inline":  {"kind"},
		"content": {"kind", "digest", "size"},
		"version": {"kind", "authority", "version"},
		"live":    {"kind", "identity"},
	}[kind]
	if integrityFields == nil {
		return fmt.Errorf("Resource integrity kind is invalid")
	}
	if err := requireExactJSONFields(integrity, integrityFields); err != nil {
		return err
	}
	if inline, exists := object["inline"]; exists {
		if err := validateInlineResourceWire(inline); err != nil {
			return err
		}
	}
	if manifest, exists := object["manifest"]; exists {
		manifestObject, ok := manifest.(map[string]any)
		if !ok {
			return fmt.Errorf("Resource manifest is not an object")
		}
		if err := requireExactJSONFields(manifestObject, []string{
			"manifest_version", "media_type", "digest", "size", "entry_count", "root_digest",
		}); err != nil {
			return err
		}
	}
	if annotations, exists := object["annotations"]; exists {
		annotationObject, ok := annotations.(map[string]any)
		if !ok || len(annotationObject) == 0 {
			return fmt.Errorf("Resource annotations are not an object")
		}
		for _, annotation := range annotationObject {
			if _, ok := annotation.(string); !ok {
				return fmt.Errorf("Resource annotation is not a string")
			}
		}
	}
	return nil
}

func validateInlineResourceWire(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("inline Resource data is not an object")
	}
	encoding, ok := object["encoding"].(string)
	if !ok {
		return fmt.Errorf("inline Resource encoding is missing")
	}
	fields := map[string][]string{
		"utf8":   {"encoding", "text"},
		"json":   {"encoding", "value"},
		"base64": {"encoding", "data"},
	}[encoding]
	if fields == nil {
		return fmt.Errorf("inline Resource encoding is invalid")
	}
	return requireExactJSONFields(object, fields)
}

func validateInlineResource(data InlineData) error {
	switch data.Encoding {
	case "utf8":
		if len(data.Text) > maxInlineResourceBytes {
			return fmt.Errorf("inline UTF-8 Resource exceeds the size limit")
		}
	case "json":
		encoded, err := json.Marshal(data.Value)
		if err != nil || len(encoded) > maxInlineResourceBytes {
			return fmt.Errorf("inline JSON Resource exceeds the size limit")
		}
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(data.Data)
		if err != nil || len(decoded) > maxInlineResourceBytes || base64.StdEncoding.EncodeToString(decoded) != data.Data {
			return fmt.Errorf("inline base64 Resource is not canonical")
		}
	default:
		return fmt.Errorf("inline Resource encoding is invalid")
	}
	return nil
}

func validateResourceManifest(manifest ResourceManifestDescriptor) error {
	if manifest.ManifestVersion != "cymule.resource-manifest/3" ||
		manifest.MediaType != "application/vnd.cymule.resource-manifest+jsonl" ||
		!validSHA256(manifest.Digest) || !validSHA256(manifest.RootDigest) ||
		manifest.Size > maxExactInteger || manifest.EntryCount > maxExactInteger ||
		manifest.Digest != resourceManifestDescriptorID(manifest) ||
		(manifest.EntryCount == 0 && manifest.Size != 0) ||
		(manifest.EntryCount > 0 && manifest.Size == 0) ||
		(manifest.EntryCount == 0 && manifest.RootDigest != emptyResourceManifestRoot) {
		return fmt.Errorf("Resource manifest is invalid")
	}
	return nil
}

func resourceManifestDescriptorID(manifest ResourceManifestDescriptor) string {
	identity := struct {
		EntryCount uint64 `json:"entry_count"`
		MediaType  string `json:"media_type"`
		RootDigest string `json:"root_digest"`
		Size       uint64 `json:"size"`
	}{
		EntryCount: manifest.EntryCount,
		MediaType:  manifest.MediaType,
		RootDigest: manifest.RootDigest,
		Size:       manifest.Size,
	}
	encoded, _ := json.Marshal(identity)
	preimage := append([]byte("cymule.resource-manifest/3\x00"), encoded...)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(preimage))
}

func validResourceMediaType(value string) bool {
	if len(value) < 1 || len(value) > 255 || !strings.Contains(value, "/") || value != strings.ToLower(value) {
		return false
	}
	for _, character := range []byte(value) {
		if character > unicode.MaxASCII || unicode.IsSpace(rune(character)) {
			return false
		}
	}
	return true
}

func validResourceToken(value string) bool {
	if len(value) < 1 || len(value) > 2048 || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validateWaitActivationResponse(activation WaitActivation) error {
	if activation.ActivationVersion != "cymule.wait-activation/2" ||
		!validClockIdentity(activation.ActivationID) || len(activation.WaitIDs) == 0 ||
		len(activation.WaitIDs) > 4096 {
		return transportFailure("invalid_engine_response", "wait activation fields are invalid")
	}
	if activation.Source.Kind == "signal" {
		if !validClockIdentity(activation.Source.Key) || activation.Source.TimerID != "" {
			return transportFailure("invalid_engine_response", "wait activation source is invalid")
		}
	} else if activation.Source.Kind == "timer" {
		if !validClockIdentity(activation.Source.TimerID) || activation.Source.Key != "" {
			return transportFailure("invalid_engine_response", "wait activation source is invalid")
		}
	} else {
		return transportFailure("invalid_engine_response", "wait activation source is invalid")
	}
	if err := validateStrictlySortedContentIDs(activation.WaitIDs, "wait activation target"); err != nil {
		return err
	}
	if err := validateArtifactRef(activation.Result); err != nil {
		return err
	}
	if activation.Result.Kind != "cymule.wait-result/1" {
		return transportFailure("invalid_engine_response", "wait activation result kind is invalid")
	}
	return nil
}

func validateDurableCommandResponse(command DurableCommand) error {
	if err := validateGoJSONStrings(reflect.ValueOf(command)); err != nil {
		return transportFailure("invalid_engine_response", "durable command is outside strict JSON")
	}
	if command.ControlVersion != DurableControlVersion {
		return transportFailure("invalid_engine_response", "durable command version is invalid")
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return transportFailure("invalid_engine_response", "durable command could not be encoded")
	}
	value, err := decodeUniqueJSON(encoded)
	if err != nil {
		return transportFailure("invalid_engine_response", "durable command is outside strict JSON")
	}
	var normalized DurableCommand
	if err := json.Unmarshal(encoded, &normalized); err != nil || !reflect.DeepEqual(command, normalized) {
		return transportFailure("invalid_engine_response", "durable command contains fields owned by another variant")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return transportFailure("invalid_engine_response", "durable command is not an object")
	}
	fields := durableCommandFields(command.Type)
	if fields == nil || requireExactJSONFields(object, fields) != nil {
		return transportFailure("invalid_engine_response", "durable command fields are not closed")
	}
	valid := false
	switch command.Type {
	case "start_run":
		valid = validRunIdentity(command.RunID) && command.Candidate != nil && len(command.Input) != 0 && validExecutionClaimRequest(command.Execution)
	case "resume_run":
		valid = validRunIdentity(command.RunID) && validExecutionClaimRequest(command.Execution)
	case "takeover_run":
		valid = validRunIdentity(command.RunID) && command.ExpectedFence > 0 && command.ExpectedFence <= 9_007_199_254_740_991 && validExecutionClaimRequest(command.Execution)
	case "activate_wait":
		valid = command.ActivationID != "" && command.Source != nil && len(command.WaitIDs) != 0 &&
			validateStrictlySortedContentIDs(command.WaitIDs, "durable activation target") == nil &&
			len(command.Value) != 0
	case "release_effect":
		valid = validSHA256ID(command.IntentID) && validExecutionClaimRequest(command.Execution)
	case "resolve_effect":
		valid = validClockIdentity(command.ResolutionID) && validRunIdentity(command.RunID) &&
			validSHA256ID(command.IntentID) && command.ExecutionBinding != nil &&
			validateArtifactRef(*command.ExecutionBinding) == nil &&
			command.ExecutionBinding.Kind == "cymule.execution-binding/2" &&
			validSHA256ID(command.OccurrenceBinding) && validClockIdentity(command.ClaimOwner) &&
			command.ClaimEpoch > 0 && command.ClaimEpoch <= maxExactInteger &&
			slices.Contains([]string{"resolved_applied", "resolved_not_applied"}, command.Resolution) &&
			len(command.Value) != 0 &&
			(command.Resolution != "resolved_not_applied" || rawMessageIsNull(command.Value))
	case "cancel_run":
		valid = command.CancellationID != "" && validRunIdentity(command.RunID) && len(command.Reason) != 0
	case "run_current":
		valid = validRunIdentity(command.RunID) && validOptionalContentID(command.ExpectedRevision)
	case "run_index_page", "run_wait_page", "run_effect_page", "run_occurrence_page", "run_attempt_page":
		queryKind := map[string]DurablePageQueryKind{
			"run_index_page": DurableRunIndexQuery, "run_wait_page": DurableRunWaitsQuery,
			"run_effect_page": DurableRunEffectsQuery, "run_occurrence_page": DurableRunOccurrencesQuery,
			"run_attempt_page": DurableRunAttemptsQuery,
		}[command.Type]
		if command.Type == "run_index_page" {
			valid = command.RunID == ""
		} else {
			valid = validRunIdentity(command.RunID)
		}
		valid = valid && validOptionalContentID(command.ExpectedRevision) &&
			command.Limit > 0 && command.Limit <= maxDurableQueryPageItems &&
			command.MaxCanonicalBytes > 0 && command.MaxCanonicalBytes <= maxDurableQueryPageBytes
		if valid && command.Cursor != nil {
			var expectedRunID *string
			if command.RunID != "" {
				expectedRunID = &command.RunID
			}
			valid = validateDurablePageCursor(command.Cursor) == nil &&
				command.Cursor.QueryKind == queryKind &&
				equalOptionalString(command.Cursor.RunID, expectedRunID) &&
				command.ExpectedRevision != nil &&
				*command.ExpectedRevision == command.Cursor.SourceRevision
		}
	case "run_item":
		valid = validRunIdentity(command.RunID) && validOptionalContentID(command.ExpectedRevision) &&
			command.Selector != nil && validateDurableRunItemSelector(*command.Selector) == nil &&
			command.MaxCanonicalBytes > 0 && command.MaxCanonicalBytes <= maxDurableQueryExactResponseBytes
	}
	if !valid {
		return transportFailure("invalid_engine_response", "durable command fields are invalid")
	}
	return nil
}

func validExecutionClaimRequest(request *ExecutionClaimRequest) bool {
	return request != nil && validClockIdentity(request.Owner) && request.TTL > 0 &&
		request.TTL <= 9_007_199_254_740_991 &&
		validClockObservationRef(request.Clock)
}

func validClockObservationRef(reference ClockObservationRef) bool {
	return reference.ClockVersion == "cymule.clock-observation/2" &&
		validSHA256(reference.ObservationID) && validSHA256(reference.SourceGeneration) &&
		validClockIdentity(reference.SourceID) && validClockIdentity(reference.Scope)
}

func validateClockObservationResult(result ClockObservationResult) error {
	if !validRunIdentity(result.RunID) || !validClockObservationRef(result.Observation) {
		return fmt.Errorf("Clock observation result is invalid")
	}
	return nil
}

func validClockIdentity(value string) bool {
	if len(value) == 0 || !utf8.ValidString(value) || utf8.RuneCountInString(value) > 512 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[7:] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func validBareSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
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
	if err := validateDurableCommandResponse(command); err != nil {
		return DurableResponse{}, validationFailure(
			"invalid_engine_request", "durable command failed local validation",
		)
	}
	if err := validateEngineDurableTarget(target, command); err != nil {
		return DurableResponse{}, validationFailure(
			"invalid_engine_request", "durable target failed local validation",
		)
	}
	expectedStartPlanID := ""
	if command.Type == "start_run" {
		sealed, err := engine.Seal(*command.Candidate)
		if err != nil {
			return DurableResponse{}, err
		}
		expectedStartPlanID = sealed.PlanID
	}
	var response struct {
		Type     string          `json:"type"`
		Response DurableResponse `json:"response"`
	}
	request := map[string]any{
		"type": "execute_durable", "target": target, "command": command,
	}
	err := engine.request(request, &response)
	if err == nil && response.Type != "durable_executed" {
		err = unexpectedEngineResponse("durable_executed", response.Type)
	}
	if err == nil {
		err = validateDurableResponseForCommand(command, response.Response, expectedStartPlanID)
		if err != nil {
			err = responseLossFailure(request, "invalid_engine_response")
		}
	}
	return response.Response, err
}

// ExecuteLiveEvolution submits one atomic command to durable evolution authority.
func (engine CliEngine) ExecuteLiveEvolution(
	target EngineEvolutionTarget, evolutionID string,
	command LiveEvolutionCommand,
) (EvolutionCommit, error) {
	if !validWireIdentity(evolutionID) {
		return EvolutionCommit{}, validationFailure(
			"invalid_engine_request", "evolution identity failed local validation",
		)
	}
	if err := validateLiveEvolutionCommandForSubmission(command); err != nil {
		return EvolutionCommit{}, validationFailure(
			"invalid_engine_request", "live-evolution command failed local validation",
		)
	}
	if err := validateEngineEvolutionTarget(target, command); err != nil {
		return EvolutionCommit{}, validationFailure(
			"invalid_engine_request", "live-evolution target failed local validation",
		)
	}
	expectedTargetPlanID := ""
	if command.Operation == "apply" && command.Command != nil &&
		command.Command.Operation == "apply_patch" && command.Command.Patch != nil {
		sealedTarget, err := engine.Seal(command.Command.Patch.Target)
		if err != nil {
			return EvolutionCommit{}, err
		}
		expectedTargetPlanID = sealedTarget.PlanID
	}

	var response struct {
		Type   string          `json:"type"`
		Commit EvolutionCommit `json:"commit"`
	}
	request := map[string]any{
		"type": "execute_live_evolution", "target": target,
		"evolution_id": evolutionID, "command": command,
	}
	err := engine.request(request, &response)
	if err == nil && response.Type != "live_evolution_executed" {
		err = unexpectedEngineResponse("live_evolution_executed", response.Type)
	}
	if err == nil && expectedTargetPlanID != "" {
		var edge struct {
			ToPlan string `json:"to_plan"`
		}
		if decodeErr := json.Unmarshal(response.Commit.Receipt.Outcome.Edge, &edge); decodeErr != nil || edge.ToPlan != expectedTargetPlanID {
			err = responseLossFailure(request, "invalid_engine_response")
		}
	}
	return response.Commit, err
}

func validateLiveEvolutionCommandForSubmission(command LiveEvolutionCommand) error {
	if err := validateGoJSONStrings(reflect.ValueOf(command)); err != nil {
		return err
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		return err
	}
	var decoded LiveEvolutionCommand
	if err := decodeClosedJSON(encoded, &decoded); err != nil {
		return err
	}
	if !wireValuesEqual(decoded, command) {
		return fmt.Errorf("live-evolution command does not round-trip exactly")
	}
	return validateLiveEvolutionCommandSemantics(decoded)
}

// DurableEngine is the high-level provider-neutral durable Run client.
type DurableEngine struct {
	Store                   EngineStoreTarget
	Executor                *EnginePluginTarget
	Clock                   *EngineClockTarget
	MigrationAdapter        *EngineMigrationProviderTarget
	ShadowDriver            *EngineShadowProviderTarget
	TargetExecutionBindings map[string]EnginePluginTarget
	Transport               CliEngine
	EvolutionID             string
}

// ObserveClock issues one exact retained Clock reference for a later command.
func (engine DurableEngine) ObserveClock(runID string) (ClockObservationRef, error) {
	if engine.Clock == nil {
		return ClockObservationRef{}, fmt.Errorf("durable Clock target is missing")
	}
	result, err := engine.Transport.ObserveClock(*engine.Clock, runID)
	if err != nil {
		return ClockObservationRef{}, err
	}
	if err := validateClockObservationResult(result); err != nil || result.RunID != runID ||
		result.Observation.SourceID != engine.Clock.SourceID ||
		result.Observation.SourceGeneration != engine.Clock.SourceGeneration {
		request := map[string]any{
			"type": "observe_clock", "target": *engine.Clock, "run_id": runID,
		}
		return ClockObservationRef{}, responseLossFailure(request, "invalid_engine_response")
	}
	return result.Observation, nil
}

// Start creates or idempotently reopens one Run.
func (engine DurableEngine) Start(runID string, candidate PlanCandidate, input any, execution ExecutionClaimRequest) (DurableResponse, error) {
	command, err := StartDurableRun(runID, candidate, input, execution)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable start command is invalid")
	}
	return engine.submit(command)
}

// RunIndexPage reads one bounded revision-pinned page of Run summaries.
func (engine DurableEngine) RunIndexPage(options DurablePageQueryOptions) (DurableQueryPage, error) {
	command, err := QueryDurableRunIndexPage(options)
	if err != nil {
		return DurableQueryPage{}, validationFailure("invalid_engine_request", "durable Run-index query is invalid")
	}
	return engine.queryPage(command, "run_index_page")
}

// RunCurrent reads one bounded semantic Run-current projection.
func (engine DurableEngine) RunCurrent(runID string, expectedRevision *string) (DurableResponse, error) {
	command, err := QueryDurableRunCurrent(runID, expectedRevision)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable Run-current query is invalid")
	}
	response, err := engine.submit(command)
	if err == nil && response.Type != "run_current" {
		err = unexpectedEngineResponse("run_current", response.Type)
	}
	return response, err
}

// RunWaitPage reads one bounded revision-pinned page of wait summaries.
func (engine DurableEngine) RunWaitPage(runID string, options DurablePageQueryOptions) (DurableQueryPage, error) {
	command, err := QueryDurableRunWaitPage(runID, options)
	if err != nil {
		return DurableQueryPage{}, validationFailure("invalid_engine_request", "durable wait-page query is invalid")
	}
	return engine.queryPage(command, "run_wait_page")
}

// RunEffectPage reads one bounded revision-pinned page of Effect summaries.
func (engine DurableEngine) RunEffectPage(runID string, options DurablePageQueryOptions) (DurableQueryPage, error) {
	command, err := QueryDurableRunEffectPage(runID, options)
	if err != nil {
		return DurableQueryPage{}, validationFailure("invalid_engine_request", "durable Effect-page query is invalid")
	}
	return engine.queryPage(command, "run_effect_page")
}

// RunOccurrencePage reads one bounded revision-pinned page of occurrence summaries.
func (engine DurableEngine) RunOccurrencePage(runID string, options DurablePageQueryOptions) (DurableQueryPage, error) {
	command, err := QueryDurableRunOccurrencePage(runID, options)
	if err != nil {
		return DurableQueryPage{}, validationFailure("invalid_engine_request", "durable occurrence-page query is invalid")
	}
	return engine.queryPage(command, "run_occurrence_page")
}

// RunAttemptPage reads one bounded revision-pinned page of Attempt summaries.
func (engine DurableEngine) RunAttemptPage(runID string, options DurablePageQueryOptions) (DurableQueryPage, error) {
	command, err := QueryDurableRunAttemptPage(runID, options)
	if err != nil {
		return DurableQueryPage{}, validationFailure("invalid_engine_request", "durable Attempt-page query is invalid")
	}
	return engine.queryPage(command, "run_attempt_page")
}

// RunItem reads one complete Run-owned typed leaf by exact identity.
func (engine DurableEngine) RunItem(query DurableRunItemQuery) (DurableResponse, error) {
	command, err := QueryDurableRunItem(query)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "exact durable Run-item query is invalid")
	}
	response, err := engine.submit(command)
	if err == nil && response.Type != "run_item" {
		err = unexpectedEngineResponse("run_item", response.Type)
	}
	return response, err
}

func (engine DurableEngine) queryPage(command DurableCommand, expectedType string) (DurableQueryPage, error) {
	response, err := engine.submit(command)
	if err != nil {
		return DurableQueryPage{}, err
	}
	if response.Type != expectedType || response.Page == nil {
		return DurableQueryPage{}, unexpectedEngineResponse(expectedType, response.Type)
	}
	return *response.Page, nil
}

// Resume advances one ready Run to its next boundary.
func (engine DurableEngine) Resume(runID string, execution ExecutionClaimRequest) (DurableResponse, error) {
	if !validRunIdentity(runID) {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable resume Run identity is invalid")
	}
	return engine.submit(ResumeDurableRun(runID, execution))
}

// Takeover explicitly replaces one expired persisted Running claim.
func (engine DurableEngine) Takeover(runID string, expectedFence uint64, execution ExecutionClaimRequest) (DurableResponse, error) {
	command, err := TakeoverDurableRun(runID, expectedFence, execution)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable takeover command is invalid")
	}
	return engine.submit(command)
}

// Signal admits one identified signal delivery.
func (engine DurableEngine) Signal(
	activationID, key string,
	waitIDs []string,
	value any,
) (DurableResponse, error) {
	command, err := ActivateDurableSignal(activationID, key, waitIDs, value)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable activation command is invalid")
	}
	return engine.submit(command)
}

// Release releases one explicit effect intent.
func (engine DurableEngine) Release(intentID string, execution ExecutionClaimRequest) (DurableResponse, error) {
	return engine.submit(ReleaseDurableEffect(intentID, execution))
}

// ResolveEffect commits one exact claimed-effect reconciliation result.
func (engine DurableEngine) ResolveEffect(
	resolutionID, runID, intentID string,
	executionBinding ArtifactRef,
	occurrenceBinding, claimOwner string,
	claimEpoch uint64,
	resolution string,
	value any,
) (DurableResponse, error) {
	command, err := ResolveDurableEffect(
		resolutionID, runID, intentID, executionBinding, occurrenceBinding,
		claimOwner, claimEpoch, resolution, value,
	)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable effect resolution command is invalid")
	}
	return engine.submit(command)
}

// Cancel commits one provider-independent semantic Run cancellation.
func (engine DurableEngine) Cancel(cancellationID, runID string, reason any) (DurableResponse, error) {
	command, err := CancelDurableRun(cancellationID, runID, reason)
	if err != nil {
		return DurableResponse{}, validationFailure("invalid_engine_request", "durable cancellation command is invalid")
	}
	return engine.submit(command)
}

// Evolve applies one atomic command to the same durable domain.
func (engine DurableEngine) Evolve(command LiveEvolutionCommand) (EvolutionCommit, error) {
	evolutionID := engine.EvolutionID
	if evolutionID == "" {
		evolutionID = "cymule.sdk.live-evolution"
	}
	target := EngineEvolutionTarget{
		Store: engine.Store, TargetExecutionBindings: map[string]EnginePluginTarget{},
	}
	if command.Operation == "apply" && command.Command != nil {
		switch command.Command.Operation {
		case "migrate":
			target.MigrationAdapter = engine.MigrationAdapter
			if command.Command.Migration != nil {
				planID := command.Command.Migration.ToPlan
				if executionTarget, ok := engine.TargetExecutionBindings[planID]; ok {
					target.TargetExecutionBindings[planID] = executionTarget
				}
			}
		case "shadow":
			target.ShadowDriver = engine.ShadowDriver
		}
	}
	return engine.Transport.ExecuteLiveEvolution(target, evolutionID, command)
}

func (engine DurableEngine) submit(command DurableCommand) (DurableResponse, error) {
	target := EngineDurableTarget{Store: engine.Store}
	storeOnly := command.Type == "activate_wait" || command.Type == "cancel_run" ||
		slices.Contains([]string{
			"run_index_page", "run_current", "run_wait_page", "run_effect_page",
			"run_occurrence_page", "run_attempt_page", "run_item",
		}, command.Type)
	if !storeOnly {
		target.Executor = engine.Executor
		if command.Type != "resolve_effect" {
			target.Clock = engine.Clock
		}
	}
	return engine.Transport.ExecuteDurable(target, command)
}

// Run executes a sealed plan through one complete plugin realization.
func (engine CliEngine) Run(plan SealedPlan, input any, plugin EnginePluginTarget, runID string) (ExecutionOutcome, error) {
	if !validRunIdentity(runID) {
		return ExecutionOutcome{}, validationFailure(
			"invalid_engine_request", "execution request Run identity is invalid",
		)
	}
	if err := validateEnginePluginTarget(plugin, false, ordinaryPluginMessageBytes); err != nil {
		return ExecutionOutcome{}, validationFailure(
			"invalid_engine_request", "execution plugin target is invalid",
		)
	}
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
	if err := validateGoJSONStrings(reflect.ValueOf(request)); err != nil {
		return validationFailure("invalid_engine_request", "Engine request contains invalid JSON text")
	}
	sentRequestBytes, err := json.Marshal(request)
	if err != nil {
		return validationFailure("invalid_engine_request", "Engine request could not be encoded")
	}
	input, err := json.Marshal(struct {
		EngineProtocol string          `json:"engine_protocol"`
		Request        json.RawMessage `json:"request"`
	}{EngineProtocolVersion, sentRequestBytes})
	if err != nil {
		return validationFailure("invalid_engine_request", "Engine request could not be encoded")
	}
	if len(input) > maxEngineRequestBytes {
		return validationFailure(
			"engine_request_too_large",
			fmt.Sprintf("complete Engine request exceeds %d UTF-8 bytes", maxEngineRequestBytes),
		)
	}
	inputValue, err := decodeUniqueJSON(input)
	if err != nil {
		return validationFailure("invalid_engine_request", "Engine request is outside the strict JSON domain")
	}
	inputObject, ok := inputValue.(map[string]any)
	if !ok {
		return validationFailure("invalid_engine_request", "Engine request envelope is not an object")
	}
	sentRequest, ok := inputObject["request"]
	if !ok {
		return validationFailure("invalid_engine_request", "Engine request envelope omitted its request")
	}
	executable := engine.Executable
	if executable == "" {
		executable = "cymule"
	}
	ctx := engine.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return interruptedFailure(request, ctx.Err(), false)
	}
	timeout := engine.Timeout
	if timeout < 0 {
		return validationFailure(
			"invalid_engine_timeout", "Engine timeout must be a positive duration",
		)
	}
	if timeout == 0 {
		timeout = defaultEngineTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if ctx.Err() != nil {
		return interruptedFailure(request, ctx.Err(), false)
	}
	command := exec.Command(executable, "rpc")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := command.StdinPipe()
	if err != nil {
		return transportFailure("engine_stdin_unavailable", "the Engine stdin pipe is unavailable")
	}
	stdout := newBoundedOutput(maxEngineOutputBytes)
	stderr := newBoundedOutput(maxEngineOutputBytes)
	command.Stdout = &stdout
	command.Stderr = &stderr
	if startErr := command.Start(); startErr != nil {
		_ = stdin.Close()
		if ctx.Err() != nil {
			return interruptedFailure(request, ctx.Err(), false)
		}
		return transportFailure("engine_start_failed", "the Engine process could not be started")
	}
	inputDone := make(chan error, 1)
	go func() {
		_, writeErr := stdin.Write(input)
		closeErr := stdin.Close()
		if writeErr == nil {
			writeErr = closeErr
		}
		inputDone <- writeErr
	}()
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- command.Wait()
	}()
	waitErr, interrupted, residualGroup, terminationErr := awaitEngineProcess(
		ctx, command.Process.Pid, waitDone,
	)
	// A complete Engine response remains authoritative when the child closed
	// stdin early; joining the writer prevents a goroutine leak, while its
	// expected EPIPE cannot replace a decoded success/failure envelope.
	inputErr := <-inputDone
	if terminationErr != nil {
		return responseLossFailure(request, "engine_process_termination_failed")
	}
	if residualGroup {
		return responseLossFailure(request, "engine_process_group_leaked")
	}
	if interrupted {
		return interruptedFailure(request, ctx.Err(), true)
	}
	if stdout.overflowed() || stderr.overflowed() {
		return responseLossFailure(request, "engine_output_limit_exceeded")
	}
	if waitErr != nil {
		return responseLossFailure(request, "engine_process_failed")
	}
	if inputErr != nil {
		value, decodeErr := decodeUniqueJSON(stdout.bytes())
		object, objectOK := value.(map[string]any)
		if decodeErr != nil || !objectOK || object["outcome"] != "failure" {
			return responseLossFailure(request, "engine_request_incomplete")
		}
	}
	return decodeEngineResponseForRequest(stdout.bytes(), response, sentRequest)
}

func awaitEngineProcess(
	ctx context.Context,
	processGroupID int,
	waitDone <-chan error,
) (waitErr error, interrupted bool, residualGroup bool, terminationErr error) {
	select {
	case waitErr = <-waitDone:
		residualGroup, terminationErr = terminateResidualEngineProcessGroup(processGroupID)
		return waitErr, false, residualGroup, terminationErr
	default:
	}
	select {
	case waitErr = <-waitDone:
		residualGroup, terminationErr = terminateResidualEngineProcessGroup(processGroupID)
		return waitErr, false, residualGroup, terminationErr
	case <-ctx.Done():
		select {
		case waitErr = <-waitDone:
			residualGroup, terminationErr = terminateResidualEngineProcessGroup(processGroupID)
			return waitErr, false, residualGroup, terminationErr
		default:
		}
		waitErr, terminationErr = terminateEngineProcessGroup(processGroupID, waitDone)
		return waitErr, true, false, terminationErr
	}
}

func terminateResidualEngineProcessGroup(processGroupID int) (bool, error) {
	exists, err := engineProcessGroupExists(processGroupID)
	if err != nil || !exists {
		return false, err
	}
	if err := signalEngineProcessGroup(processGroupID, syscall.SIGTERM); err != nil {
		return true, err
	}
	exited, err := waitForEngineProcessGroupExitWithin(processGroupID, engineTerminationGrace)
	if err != nil {
		return true, err
	}
	if exited {
		return true, nil
	}
	if err := signalEngineProcessGroup(processGroupID, syscall.SIGKILL); err != nil {
		return true, err
	}
	return true, waitForEngineProcessGroupExit(processGroupID)
}

func waitForEngineProcessGroupExitWithin(processGroupID int, limit time.Duration) (bool, error) {
	deadline := time.Now().Add(limit)
	for {
		exists, err := engineProcessGroupExists(processGroupID)
		if err != nil {
			return false, err
		}
		if !exists {
			return true, nil
		}
		if !time.Now().Before(deadline) {
			return false, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func terminateEngineProcessGroup(
	processGroupID int,
	waitDone <-chan error,
) (waitErr error, terminationErr error) {
	_ = signalEngineProcessGroup(processGroupID, syscall.SIGTERM)
	grace := time.NewTimer(engineTerminationGrace)
	waitReceived := false
	select {
	case waitErr = <-waitDone:
		waitReceived = true
		<-grace.C
	case <-grace.C:
	}
	if err := signalEngineProcessGroup(processGroupID, syscall.SIGKILL); err != nil {
		terminationErr = err
	}
	if !waitReceived {
		waitErr = <-waitDone
	}
	if err := waitForEngineProcessGroupExit(processGroupID); err != nil && terminationErr == nil {
		terminationErr = err
	}
	return waitErr, terminationErr
}

func signalEngineProcessGroup(processGroupID int, selectedSignal syscall.Signal) error {
	if processGroupID <= 0 {
		return fmt.Errorf("Engine process group identity is invalid")
	}
	if err := syscall.Kill(-processGroupID, selectedSignal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

func waitForEngineProcessGroupExit(processGroupID int) error {
	deadline := time.Now().Add(engineGroupExitLimit)
	for {
		exists, err := engineProcessGroupExists(processGroupID)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("Engine process group %d did not exit", processGroupID)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func engineProcessGroupExists(processGroupID int) (bool, error) {
	if processGroupID <= 0 {
		return false, fmt.Errorf("Engine process group identity is invalid")
	}
	err := syscall.Kill(-processGroupID, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, err
}

type boundedOutput struct {
	limit int
	data  []byte
}

func newBoundedOutput(limit int) boundedOutput {
	return boundedOutput{limit: limit}
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	remaining := output.limit + 1 - len(output.data)
	if remaining > 0 {
		retained := len(value)
		if retained > remaining {
			retained = remaining
		}
		output.data = append(output.data, value[:retained]...)
	}
	return len(value), nil
}

func (output *boundedOutput) overflowed() bool {
	return len(output.data) > output.limit
}

func (output *boundedOutput) bytes() []byte {
	return output.data
}

func decodeEngineResponse(input []byte, response any) error {
	return decodeEngineResponseForRequest(input, response, nil)
}

func decodeEngineResponseForRequest(input []byte, response any, request any) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return invalidEngineResponseForRequest(request, err.Error())
	}
	object, ok := value.(map[string]any)
	if !ok {
		return invalidEngineResponseForRequest(request, "response envelope is not an object")
	}
	protocol, ok := object["engine_protocol"].(string)
	if !ok {
		return invalidEngineResponseForRequest(request, "Engine protocol is not a string")
	}
	if protocol != EngineProtocolVersion {
		if request != nil && requestIsMutating(request) {
			return responseLossFailure(request, "unsupported_engine_protocol")
		}
		return EngineFailure{
			Category: "contract_violation", Phase: "transport",
			Code:     "unsupported_engine_protocol",
			Message:  fmt.Sprintf("expected %s, received %q", EngineProtocolVersion, protocol),
			Contract: EngineProtocolVersion, ContractSide: "schema", RetryDisposition: "never",
		}
	}
	outcome, ok := object["outcome"].(string)
	if !ok || !slices.Contains([]string{"success", "failure"}, outcome) {
		return invalidEngineResponseForRequest(request, "response outcome is not closed")
	}
	expectedFields := []string{"outcome", "engine_protocol", "request", "response"}
	if outcome == "failure" {
		expectedFields = []string{"outcome", "engine_protocol", "error"}
	}
	if err := requireExactJSONFields(object, expectedFields); err != nil {
		return invalidEngineResponseForRequest(request, err.Error())
	}
	if outcome == "failure" {
		if err := validateEngineFailureWire(object["error"]); err != nil {
			return invalidEngineResponseForRequest(request, err.Error())
		}
	}
	var envelope struct {
		Outcome        string           `json:"outcome"`
		EngineProtocol string           `json:"engine_protocol"`
		Request        *json.RawMessage `json:"request"`
		Response       *json.RawMessage `json:"response"`
		Error          *EngineFailure   `json:"error"`
	}
	if err := decodeClosedValue(value, &envelope); err != nil {
		return invalidEngineResponseForRequest(request, err.Error())
	}
	switch envelope.Outcome {
	case "failure":
		if envelope.Error == nil || envelope.Request != nil || envelope.Response != nil {
			return invalidEngineResponseForRequest(request, "failure response must contain only error")
		}
		if err := envelope.Error.validate(); err != nil {
			return invalidEngineResponseForRequest(request, err.Error())
		}
		return *envelope.Error
	case "success":
		if envelope.Request == nil || envelope.Response == nil || envelope.Error != nil {
			return invalidEngineResponseForRequest(request, "success response must contain request and response")
		}
		if request != nil && !rawWireValueEquals(*envelope.Request, request) {
			return invalidEngineResponseForRequest(request, "success request echo does not match the sent request")
		}
		if err := validateSuccessResponse(*envelope.Response); err != nil {
			return invalidEngineResponseForRequest(request, err.Error())
		}
		if request != nil {
			if err := validateSuccessResponseForRequest(request, *envelope.Response); err != nil {
				return invalidEngineResponseForRequest(request, err.Error())
			}
		}
		if err := decodeClosedJSON(*envelope.Response, response); err != nil {
			return invalidEngineResponseForRequest(request, err.Error())
		}
		return nil
	default:
		return invalidEngineResponseForRequest(request, "response outcome is not closed")
	}
}

func validateSuccessResponse(input json.RawMessage) error {
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("success response is not an object")
	}
	typeName, ok := object["type"].(string)
	if !ok {
		return fmt.Errorf("success response is not tagged")
	}
	payloadField, ok := map[string]string{
		"sealed":                          "plan",
		"sealed_resource":                 "resource",
		"verified_wait_activation":        "activation",
		"verified_durable_command":        "command",
		"clock_observed":                  "result",
		"verified_evolution_command":      "command",
		"verified_live_evolution_command": "command",
		"execution_boundary":              "execution",
		"durable_executed":                "response",
		"live_evolution_executed":         "commit",
	}[typeName]
	if typeName == "verified" {
		return requireExactJSONFields(object, []string{"type"})
	}
	if !ok {
		return fmt.Errorf("success response tag is unknown")
	}
	if err := requireExactJSONFields(object, []string{"type", payloadField}); err != nil {
		return err
	}
	payload := object[payloadField]
	switch typeName {
	case "sealed":
		var plan SealedPlan
		return decodeClosedValue(payload, &plan)
	case "sealed_resource":
		if err := validateResourceHandleWire(payload); err != nil {
			return err
		}
		var resource ResourceHandle
		if err := decodeClosedValue(payload, &resource); err != nil {
			return err
		}
		return validateResourceHandle(resource)
	case "verified_wait_activation":
		var activation WaitActivation
		if err := decodeClosedValue(payload, &activation); err != nil {
			return err
		}
		return validateWaitActivationResponse(activation)
	case "verified_durable_command":
		var command DurableCommand
		if err := decodeClosedValue(payload, &command); err != nil {
			return err
		}
		return validateDurableCommandResponse(command)
	case "clock_observed":
		var result ClockObservationResult
		if err := decodeClosedValue(payload, &result); err != nil {
			return err
		}
		return validateClockObservationResult(result)
	case "verified_evolution_command":
		var command EvolutionCommand
		return decodeClosedValue(payload, &command)
	case "verified_live_evolution_command":
		var command LiveEvolutionCommand
		return decodeClosedValue(payload, &command)
	case "execution_boundary":
		var execution ExecutionOutcome
		return decodeClosedValue(payload, &execution)
	case "durable_executed":
		var response DurableResponse
		if err := decodeClosedValue(payload, &response); err != nil {
			return err
		}
		return response.validate()
	case "live_evolution_executed":
		commitValue, ok := payload.(map[string]any)
		if !ok {
			return fmt.Errorf("evolution commit is not an object")
		}
		if err := requireExactJSONFields(commitValue, []string{"observed_revision", "committed_revision", "receipt"}); err != nil {
			return err
		}
		var commit EvolutionCommit
		if err := decodeClosedValue(payload, &commit); err != nil {
			return err
		}
		if !wireValuesEqual(payload, commit) {
			return fmt.Errorf("evolution commit loses JSON member presence during typed decoding")
		}
		return commit.validate()
	default:
		return fmt.Errorf("success response tag is unknown")
	}
}

func validateSuccessResponseForRequest(request any, input json.RawMessage) error {
	requestObject, ok := request.(map[string]any)
	if !ok {
		return fmt.Errorf("Engine request is not an object")
	}
	requestType, ok := requestObject["type"].(string)
	if !ok {
		return fmt.Errorf("Engine request is not tagged")
	}
	expectedType, ok := map[string]string{
		"seal":                          "sealed",
		"seal_resource":                 "sealed_resource",
		"verify_wait_activation":        "verified_wait_activation",
		"verify_durable_command":        "verified_durable_command",
		"observe_clock":                 "clock_observed",
		"verify_evolution_command":      "verified_evolution_command",
		"verify_live_evolution_command": "verified_live_evolution_command",
		"run":                           "execution_boundary",
		"execute_durable":               "durable_executed",
		"execute_live_evolution":        "live_evolution_executed",
	}[requestType]
	if !ok {
		return fmt.Errorf("Engine request tag is unknown")
	}
	value, err := decodeUniqueJSON(input)
	if err != nil {
		return err
	}
	responseObject, ok := value.(map[string]any)
	if !ok || responseObject["type"] != expectedType {
		return fmt.Errorf("success response does not match request")
	}
	if requestType == "seal" {
		candidate, ok := requestObject["candidate"]
		if !ok {
			return fmt.Errorf("Plan seal request candidate is invalid")
		}
		var success struct {
			Type string     `json:"type"`
			Plan SealedPlan `json:"plan"`
		}
		if err := decodeClosedJSON(input, &success); err != nil {
			return err
		}
		if !wireValuesEqual(success.Plan.Candidate, candidate) {
			return fmt.Errorf("sealed Plan does not match its candidate")
		}
		return nil
	}
	if requestType == "seal_resource" {
		candidate, ok := requestObject["candidate"].(map[string]any)
		if !ok {
			return fmt.Errorf("Resource seal request candidate is invalid")
		}
		resource, ok := responseObject["resource"].(map[string]any)
		if !ok {
			return fmt.Errorf("sealed Resource is invalid")
		}
		returnedCandidate := make(map[string]any, len(resource)-1)
		for key, value := range resource {
			if key != "resource_id" {
				returnedCandidate[key] = value
			}
		}
		if !resourceCandidatesEqual(returnedCandidate, candidate) {
			return fmt.Errorf("sealed Resource does not match its candidate")
		}
		return nil
	}
	verifiedPayload := map[string]string{
		"verify_wait_activation":        "activation",
		"verify_durable_command":        "command",
		"verify_evolution_command":      "command",
		"verify_live_evolution_command": "command",
	}[requestType]
	if verifiedPayload != "" {
		requested, requestOK := requestObject[verifiedPayload]
		returned, responseOK := responseObject[verifiedPayload]
		if !requestOK || !responseOK || !wireValuesEqual(returned, requested) {
			return fmt.Errorf("verified payload does not match its request")
		}
		return nil
	}
	if requestType == "observe_clock" {
		target, targetOK := requestObject["target"].(map[string]any)
		result, resultOK := responseObject["result"].(map[string]any)
		observation, observationOK := result["observation"].(map[string]any)
		if !targetOK || !resultOK || !observationOK || result["run_id"] != requestObject["run_id"] ||
			observation["source_id"] != target["source_id"] ||
			observation["source_generation"] != target["source_generation"] {
			return fmt.Errorf("Clock observation does not match its requested authority")
		}
		return nil
	}
	if requestType == "run" {
		return validateExecutionSuccessForRequest(requestObject, input)
	}
	if requestType == "execute_durable" {
		return validateDurableSuccessForRequest(requestObject, input)
	}
	if requestType != "execute_live_evolution" {
		return nil
	}
	evolutionID, ok := requestObject["evolution_id"].(string)
	if !ok {
		return fmt.Errorf("live-evolution request evolution identity is invalid")
	}
	command, ok := requestObject["command"]
	if !ok {
		return fmt.Errorf("live-evolution request command is invalid")
	}
	commit, ok := responseObject["commit"].(map[string]any)
	if !ok {
		return fmt.Errorf("evolution commit does not match its request")
	}
	receipt, ok := commit["receipt"].(map[string]any)
	if !ok {
		return fmt.Errorf("evolution commit does not match its request")
	}
	persisted, ok := receipt["command"].(map[string]any)
	if !ok || persisted["evolution_id"] != evolutionID || !wireValuesEqual(persisted["command"], command) {
		return fmt.Errorf("evolution commit does not match its request")
	}
	return nil
}

func resourceCandidatesEqual(left, right map[string]any) bool {
	return wireValuesEqual(left, right)
}

func validateExecutionSuccessForRequest(request map[string]any, input json.RawMessage) error {
	runID, ok := request["run_id"].(string)
	if !ok {
		return fmt.Errorf("execution request Run is invalid")
	}
	planValue, ok := request["plan"]
	if !ok {
		return fmt.Errorf("execution request Plan is missing")
	}
	var plan SealedPlan
	if err := decodeClosedValue(planValue, &plan); err != nil {
		return err
	}
	var success struct {
		Type      string           `json:"type"`
		Execution ExecutionOutcome `json:"execution"`
	}
	if err := decodeClosedJSON(input, &success); err != nil {
		return err
	}
	switch success.Execution.Status {
	case "completed":
		if success.Execution.Result.RunID != runID || success.Execution.Result.PlanID != plan.PlanID {
			return fmt.Errorf("completed execution does not match its requested Run and Plan")
		}
	case "suspended":
		boundary := success.Execution.Suspension
		if boundary.RunID != runID || boundary.PlanID != plan.PlanID {
			return fmt.Errorf("suspended execution does not match its requested Run and Plan")
		}
		definition := slices.IndexFunc(plan.Candidate.Definitions, func(definition Definition) bool {
			return definition.ID == boundary.DefinitionID
		})
		if definition < 0 {
			return fmt.Errorf("suspended execution definition is absent from its Plan")
		}
		step := findPlanStep(plan.Candidate.Definitions[definition].Body, boundary.SiteID)
		if step == nil || step["op"] != "wait" || !wireValuesEqual(step["wait"], boundary.Wait) {
			return fmt.Errorf("suspended execution does not match its Plan wait site")
		}
		bind, hasBind := step["bind"]
		if (!hasBind && boundary.ResultBind != nil) || (hasBind && (boundary.ResultBind == nil || bind != *boundary.ResultBind)) {
			return fmt.Errorf("suspended execution result binding does not match its Plan")
		}
	case "release_required":
		if success.Execution.Release.RunID != runID || success.Execution.Release.PlanID != plan.PlanID {
			return fmt.Errorf("effect release does not match its requested Run and Plan")
		}
	case "reconciliation_required":
		if success.Execution.Reconciliation.RunID != runID || success.Execution.Reconciliation.PlanID != plan.PlanID {
			return fmt.Errorf("effect reconciliation does not match its requested Run and Plan")
		}
	}
	return nil
}

func findPlanStep(region Region, siteID string) Step {
	for _, step := range region.Steps {
		if step["id"] == siteID {
			return step
		}
		if step["op"] == "scope" {
			body, ok := step["body"].(map[string]any)
			if !ok {
				continue
			}
			var nested Region
			if err := decodeClosedValue(body, &nested); err == nil {
				if found := findPlanStep(nested, siteID); found != nil {
					return found
				}
			}
		}
	}
	return nil
}

func validateDurableSuccessForRequest(request map[string]any, input json.RawMessage) error {
	command, ok := request["command"].(map[string]any)
	if !ok {
		return fmt.Errorf("durable request command is invalid")
	}
	commandType, ok := command["type"].(string)
	if !ok {
		return fmt.Errorf("durable request command type is invalid")
	}
	var success struct {
		Type     string          `json:"type"`
		Response DurableResponse `json:"response"`
	}
	if err := decodeClosedJSON(input, &success); err != nil {
		return err
	}
	expected := map[string]string{
		"start_run": "run_boundary", "resume_run": "run_boundary", "takeover_run": "run_boundary",
		"release_effect": "run_boundary", "resolve_effect": "effect_resolved", "cancel_run": "run_cancelled",
		"activate_wait": "wait_activated", "run_index_page": "run_index_page", "run_current": "run_current",
		"run_wait_page": "run_wait_page", "run_effect_page": "run_effect_page",
		"run_occurrence_page": "run_occurrence_page", "run_attempt_page": "run_attempt_page", "run_item": "run_item",
	}[commandType]
	if expected == "" || success.Response.Type != expected {
		return fmt.Errorf("durable response variant does not match its command")
	}
	if commandType == "activate_wait" {
		receipt, err := rawJSONObject(success.Response.Receipt)
		if err != nil {
			return err
		}
		activation, ok := receipt["activation"].(map[string]any)
		if !ok || activation["activation_id"] != command["activation_id"] ||
			!wireValuesEqual(activation["source"], command["source"]) ||
			!wireValuesEqual(activation["wait_ids"], command["wait_ids"]) {
			return fmt.Errorf("wait activation receipt does not match its command")
		}
		return nil
	}
	if commandType == "cancel_run" || commandType == "resolve_effect" {
		return validateDurableReceiptForRawCommand(command, success.Response)
	}
	if slices.Contains([]string{"start_run", "resume_run", "takeover_run"}, commandType) {
		var boundary struct {
			Status string `json:"status"`
			Result struct {
				RunID string `json:"run_id"`
			} `json:"result"`
		}
		if err := json.Unmarshal(success.Response.Boundary, &boundary); err != nil {
			return err
		}
		if boundary.Status == "completed" && boundary.Result.RunID != command["run_id"] {
			return fmt.Errorf("durable completion returned a different Run")
		}
	}
	if slices.Contains([]string{
		"run_index_page", "run_current", "run_wait_page", "run_effect_page",
		"run_occurrence_page", "run_attempt_page", "run_item",
	}, commandType) {
		var typedCommand DurableCommand
		if err := decodeClosedValue(command, &typedCommand); err != nil {
			return err
		}
		return validateDurableQueryResponseForCommand(typedCommand, success.Response)
	}
	return nil
}

func validateDurableResponseForCommand(command DurableCommand, response DurableResponse, expectedStartPlanID string) error {
	expected := map[string]string{
		"start_run": "run_boundary", "resume_run": "run_boundary", "takeover_run": "run_boundary",
		"release_effect": "run_boundary", "resolve_effect": "effect_resolved", "cancel_run": "run_cancelled",
		"activate_wait": "wait_activated", "run_index_page": "run_index_page", "run_current": "run_current",
		"run_wait_page": "run_wait_page", "run_effect_page": "run_effect_page",
		"run_occurrence_page": "run_occurrence_page", "run_attempt_page": "run_attempt_page", "run_item": "run_item",
	}[command.Type]
	if expected == "" || response.Type != expected {
		return fmt.Errorf("durable response variant does not match its command")
	}
	switch command.Type {
	case "start_run", "resume_run", "takeover_run":
		boundary, err := rawJSONObject(response.Boundary)
		if err != nil {
			return err
		}
		if boundary["status"] != "completed" {
			return nil
		}
		result, ok := boundary["result"].(map[string]any)
		if !ok || result["run_id"] != command.RunID {
			return fmt.Errorf("durable completion returned a different Run")
		}
		if command.Type == "start_run" && (expectedStartPlanID == "" || result["plan_id"] != expectedStartPlanID) {
			return fmt.Errorf("durable start returned a different Plan")
		}
	case "release_effect":
		boundary, err := rawJSONObject(response.Boundary)
		if err != nil {
			return err
		}
		switch boundary["status"] {
		case "reconciliation_required", "effect_unavailable", "effect_not_applied":
			if boundary["intent_id"] != command.IntentID {
				return fmt.Errorf("effect release returned a different intent")
			}
		case "release_required":
			intents, ok := boundary["intent_ids"].([]any)
			if !ok || !slices.Contains(intents, any(command.IntentID)) {
				return fmt.Errorf("effect release returned a different intent")
			}
		}
	case "cancel_run", "resolve_effect":
		return validateDurableReceiptForTypedCommand(command, response)
	case "activate_wait":
		var receipt WaitActivationReceipt
		if err := decodeClosedJSON(response.Receipt, &receipt); err != nil {
			return err
		}
		if receipt.Activation.ActivationID != command.ActivationID || command.Source == nil ||
			receipt.Activation.Source != *command.Source ||
			!slices.Equal(receipt.Activation.WaitIDs, command.WaitIDs) {
			return fmt.Errorf("wait activation receipt does not match its command")
		}
	case "run_index_page", "run_current", "run_wait_page", "run_effect_page", "run_occurrence_page", "run_attempt_page", "run_item":
		return validateDurableQueryResponseForCommand(command, response)
	}
	return nil
}

func validateDurableQueryResponseForCommand(command DurableCommand, response DurableResponse) error {
	if command.Type == "run_current" {
		if command.ExpectedRevision != nil && *command.ExpectedRevision != response.ObservedRevision {
			return fmt.Errorf("Run-current response observed another revision")
		}
		if !rawMessageIsNull(response.Current) {
			var current struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(response.Current, &current); err != nil || current.RunID != command.RunID {
				return fmt.Errorf("Run-current response belongs to a different Run")
			}
		}
		return nil
	}
	if command.Type == "run_item" {
		if response.RunID != command.RunID ||
			command.ExpectedRevision != nil && *command.ExpectedRevision != response.ObservedRevision {
			return fmt.Errorf("exact Run-item response belongs to another owner or revision")
		}
		if !rawMessageIsNull(response.Item) && (command.Selector == nil ||
			!durableRunItemMatchesSelector(response.Item, *command.Selector)) {
			return fmt.Errorf("exact Run-item response does not match its selector")
		}
		return validateDurableQueryResponseSize(response, command.MaxCanonicalBytes)
	}
	if response.Page == nil {
		return fmt.Errorf("durable page response omitted its page")
	}
	if command.Type != "run_index_page" && response.RunID != command.RunID {
		return fmt.Errorf("durable page response belongs to a different Run")
	}
	page := response.Page
	if command.ExpectedRevision != nil && *command.ExpectedRevision != page.ObservedRevision {
		return fmt.Errorf("durable page response observed another revision")
	}
	if command.Cursor != nil && (command.Cursor.SourceRevision != page.ObservedRevision ||
		command.Cursor.SourceRoot != page.SourceRoot) {
		return fmt.Errorf("durable page response changed its pinned source")
	}
	if uint32(len(page.Items)) > command.Limit {
		return fmt.Errorf("durable page response exceeds its requested item limit")
	}
	if command.Cursor != nil && len(page.Items) != 0 {
		queryKind := map[string]DurablePageQueryKind{
			"run_index_page": DurableRunIndexQuery, "run_wait_page": DurableRunWaitsQuery,
			"run_effect_page": DurableRunEffectsQuery, "run_occurrence_page": DurableRunOccurrencesQuery,
			"run_attempt_page": DurableRunAttemptsQuery,
		}[command.Type]
		firstKey, err := durableSummaryKey(page.Items[0], queryKind)
		if err != nil {
			return err
		}
		first := DurablePagePosition{CanonicalKey: firstKey, KeyHash: durablePageKeyHash(firstKey)}
		cursor := command.Cursor.Position
		if first.KeyHash < cursor.KeyHash || first.KeyHash == cursor.KeyHash && first.CanonicalKey <= cursor.CanonicalKey {
			return fmt.Errorf("durable continued page did not advance beyond its cursor")
		}
	}
	return validateDurableQueryResponseSize(response, command.MaxCanonicalBytes)
}

func validateDurableQueryResponseSize(response DurableResponse, maximum uint64) error {
	encoded, err := json.Marshal(response)
	if err != nil {
		return err
	}
	size, err := normalizedJSONSize(encoded)
	if err != nil || uint64(size) > maximum {
		return fmt.Errorf("durable query response exceeds its requested canonical byte budget")
	}
	return nil
}

func durableRunItemMatchesSelector(raw json.RawMessage, selector DurableRunItemSelector) bool {
	var tag struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal(raw, &tag) != nil || tag.Kind != selector.Kind {
		return false
	}
	field := map[string]string{"wait": "wait", "effect": "effect", "occurrence": "occurrence", "attempt": "attempt"}[tag.Kind]
	identityField := map[string]string{"wait": "wait_id", "effect": "intent_id", "occurrence": "occurrence_id", "attempt": "attempt_id"}[tag.Kind]
	expected := map[string]string{"wait": selector.WaitID, "effect": selector.IntentID, "occurrence": selector.OccurrenceID, "attempt": selector.AttemptID}[tag.Kind]
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return false
	}
	var item map[string]json.RawMessage
	if json.Unmarshal(object[field], &item) != nil {
		return false
	}
	var identity string
	return json.Unmarshal(item[identityField], &identity) == nil && identity == expected
}

func validateDurableReceiptForRawCommand(command map[string]any, response DurableResponse) error {
	receipt, err := rawJSONObject(response.Receipt)
	if err != nil {
		return err
	}
	receiptCommand, ok := receipt["command"].(map[string]any)
	if !ok {
		return fmt.Errorf("durable terminal receipt command is invalid")
	}
	commandType, _ := command["type"].(string)
	if commandType == "cancel_run" {
		expected := map[string]any{
			"cancellation_id": command["cancellation_id"],
			"run_id":          command["run_id"],
			"reason":          command["reason"],
		}
		if !wireValuesEqual(receiptCommand, expected) {
			return fmt.Errorf("Run cancellation receipt does not match its command")
		}
		return nil
	}
	expected := make(map[string]any, 9)
	for _, field := range []string{
		"resolution_id", "run_id", "intent_id", "execution_binding", "occurrence_binding",
		"claim_owner", "claim_epoch", "resolution", "value",
	} {
		expected[field] = command[field]
	}
	if !wireValuesEqual(receiptCommand, expected) {
		return fmt.Errorf("effect resolution receipt does not match its command")
	}
	return nil
}

func validateDurableReceiptForTypedCommand(command DurableCommand, response DurableResponse) error {
	if command.Type == "cancel_run" {
		var receipt RunCancellationReceipt
		if err := decodeClosedJSON(response.Receipt, &receipt); err != nil {
			return err
		}
		expected := RunCancellationCommand{
			CancellationID: command.CancellationID, RunID: command.RunID, Reason: command.Reason,
		}
		if !wireValuesEqual(receipt.Command, expected) {
			return fmt.Errorf("Run cancellation receipt does not match its command")
		}
		return nil
	}
	var receipt EffectResolutionReceipt
	if err := decodeClosedJSON(response.Receipt, &receipt); err != nil {
		return err
	}
	if command.ExecutionBinding == nil {
		return fmt.Errorf("effect resolution receipt does not match its command")
	}
	expected := EffectResolutionReceiptCommand{
		ResolutionID: command.ResolutionID, RunID: command.RunID, IntentID: command.IntentID,
		ExecutionBinding: *command.ExecutionBinding, OccurrenceBinding: command.OccurrenceBinding,
		ClaimOwner: command.ClaimOwner, ClaimEpoch: command.ClaimEpoch,
		Resolution: command.Resolution, Value: command.Value,
	}
	if !wireValuesEqual(receipt.Command, expected) {
		return fmt.Errorf("effect resolution receipt does not match its command")
	}
	return nil
}

func validateLiveEvolutionOutcomeForCommand(command LiveEvolutionCommand, outcome LiveEvolutionOutcome) error {
	switch command.Operation {
	case "publish_definition":
		if outcome.Result != "definition_published" || command.Definition == nil {
			return fmt.Errorf("definition publication outcome does not match its command")
		}
		revision, err := rawJSONObject(outcome.Revision)
		if err != nil || revision["logical_ref"] != command.LogicalRef ||
			!wireValuesEqual(revision["definition"], *command.Definition) ||
			command.References == nil ||
			!wireValuesEqual(revision["references"], *command.References) {
			return fmt.Errorf("definition publication outcome does not match its command")
		}
		return nil
	case "register_template":
		if outcome.Result != "template_registered" || command.Template == nil {
			return fmt.Errorf("template registration outcome does not match its command")
		}
		linked, err := rawJSONObject(outcome.Linked)
		if err != nil {
			return err
		}
		resolved, ok := linked["resolved_revisions"].(map[string]any)
		if !ok || linked["template_id"] != command.Template.TemplateID {
			return fmt.Errorf("template registration outcome does not match its command")
		}
		expected := make([]string, 0, len(command.Template.References))
		for _, reference := range command.Template.References {
			expected = append(expected, reference.LogicalRef)
		}
		sort.Strings(expected)
		actual := make([]string, 0, len(resolved))
		for logicalRef := range resolved {
			actual = append(actual, logicalRef)
		}
		sort.Strings(actual)
		if !slices.Equal(expected, actual) {
			return fmt.Errorf("template registration outcome does not match its command")
		}
		return nil
	case "publish_and_relink":
		if outcome.Result != "publication_applied" || command.Publication == nil {
			return fmt.Errorf("publication outcome does not match its command")
		}
		receipt, err := rawJSONObject(outcome.Receipt)
		if err != nil {
			return err
		}
		revision, ok := receipt["revision"].(map[string]any)
		if !ok || revision["logical_ref"] != command.Publication.LogicalRef ||
			!wireValuesEqual(revision["definition"], command.Publication.Definition) ||
			!wireValuesEqual(revision["references"], command.Publication.References) {
			return fmt.Errorf("publication outcome does not match its command")
		}
		return nil
	case "apply":
		if command.Command == nil {
			return fmt.Errorf("live-evolution apply command is missing")
		}
		return validateAppliedEvolutionOutcome(command.TemplateID, *command.Command, outcome)
	default:
		return fmt.Errorf("live-evolution outcome operation is unknown")
	}
}

func validateAppliedEvolutionOutcome(templateID string, command EvolutionCommand, outcome LiveEvolutionOutcome) error {
	switch command.Operation {
	case "apply_patch":
		if outcome.Result != "patch_applied" || command.Patch == nil {
			return fmt.Errorf("Plan patch outcome does not match its command")
		}
		edge, err := rawJSONObject(outcome.Edge)
		if err != nil || edge["from_plan"] != command.Patch.FromPlan ||
			!wireValuesEqual(edge["operations"], command.Patch.Operations) {
			return fmt.Errorf("Plan patch outcome does not match its command")
		}
	case "set_rollout", "observe":
		if outcome.Result != "applied" {
			return fmt.Errorf("live-evolution outcome does not match its command")
		}
	case "select_occurrence":
		if outcome.Result != "occurrence_selected" || outcome.Pin == nil || command.ExecutionBinding == nil ||
			outcome.Pin.TemplateID != templateID || outcome.Pin.OccurrenceID != command.OccurrenceID ||
			outcome.Pin.SelectionID != command.SelectionID ||
			outcome.Pin.ExecutionBinding != *command.ExecutionBinding {
			return fmt.Errorf("occurrence selection outcome does not match its command")
		}
	case "migrate":
		if outcome.Result != "migrated" || command.Migration == nil {
			return fmt.Errorf("migration outcome does not match its command")
		}
		receipt, err := rawJSONObject(outcome.Receipt)
		if err != nil || !wireValuesEqual(receipt["request"], *command.Migration) {
			return fmt.Errorf("migration outcome does not match its command")
		}
	case "restart_under_new_plan":
		if outcome.Result != "restart_authorized" || command.Restart == nil {
			return fmt.Errorf("restart outcome does not match its command")
		}
		receipt, err := rawJSONObject(outcome.Receipt)
		if err != nil || !wireValuesEqual(receipt["request"], *command.Restart) {
			return fmt.Errorf("restart outcome does not match its command")
		}
	case "shadow":
		if outcome.Result != "shadow_recorded" || command.Shadow == nil {
			return fmt.Errorf("shadow outcome does not match its command")
		}
		comparison, err := rawJSONObject(outcome.Comparison)
		if err != nil {
			return err
		}
		request := command.Shadow
		for field, expected := range map[string]string{
			"comparison_id": request.ComparisonID, "decision_id": request.DecisionID,
			"subject": request.Subject, "primary_plan": request.PrimaryPlan,
			"shadow_plan": request.ShadowPlan, "driver_id": request.DriverID,
			"driver_revision":   request.DriverRevision,
			"comparison_policy": request.ComparisonPolicy,
		} {
			if comparison[field] != expected {
				return fmt.Errorf("shadow outcome does not match its command")
			}
		}
	case "apply_gate":
		if outcome.Result != "gate_applied" || command.Gate == nil {
			return fmt.Errorf("rollout gate outcome does not match its command")
		}
		transition, err := rawJSONObject(outcome.Transition)
		if err != nil {
			return err
		}
		evaluation, ok := transition["evaluation"].(map[string]any)
		if !ok || transition["from_decision"] != command.Gate.DecisionID ||
			transition["to_decision"] != command.NextDecisionID ||
			!wireValuesEqual(evaluation["gate"], *command.Gate) {
			return fmt.Errorf("rollout gate outcome does not match its command")
		}
	default:
		return fmt.Errorf("live-evolution outcome operation is unknown")
	}
	return nil
}

func rawJSONObject(raw json.RawMessage) (map[string]any, error) {
	value, err := decodeUniqueJSON(raw)
	if err != nil {
		return nil, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("wire value is not an object")
	}
	return object, nil
}

func emptyJSONArray(value any) bool {
	items, ok := value.([]any)
	return ok && len(items) == 0
}

func rawWireValueEquals(raw json.RawMessage, value any) bool {
	decoded, err := decodeUniqueJSON(raw)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(decoded, value)
}

func wireValuesEqual(left, right any) bool {
	leftBytes, err := json.Marshal(left)
	if err != nil {
		return false
	}
	rightBytes, err := json.Marshal(right)
	if err != nil {
		return false
	}
	leftValue, err := decodeUniqueJSON(leftBytes)
	if err != nil {
		return false
	}
	rightValue, err := decodeUniqueJSON(rightBytes)
	if err != nil {
		return false
	}
	leftNormalized, err := json.Marshal(leftValue)
	if err != nil {
		return false
	}
	rightNormalized, err := json.Marshal(rightValue)
	return err == nil && bytes.Equal(leftNormalized, rightNormalized)
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

func requireRequiredAllowedJSONFields(object map[string]any, required, allowed []string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = struct{}{}
	}
	for field := range object {
		if _, ok := allowedSet[field]; !ok {
			return fmt.Errorf("JSON object field %q is not allowed", field)
		}
	}
	for _, field := range required {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("JSON object omitted required field %q", field)
		}
	}
	return nil
}

func marshalStrictJSONValue(value any) ([]byte, error) {
	if err := validateGoJSONStrings(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if _, err := decodeUniqueJSON(encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

func decodeUniqueJSON(input []byte) (any, error) {
	if !utf8.Valid(input) {
		return nil, fmt.Errorf("JSON text is not valid UTF-8")
	}
	if err := validateJSONStringScalars(input); err != nil {
		return nil, err
	}
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

func validateJSONStringScalars(input []byte) error {
	inString := false
	for index := 0; index < len(input); index++ {
		switch input[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString {
				continue
			}
			index++
			if index >= len(input) {
				return fmt.Errorf("JSON string ends in an escape")
			}
			if input[index] != 'u' {
				continue
			}
			first, ok := parseHexQuad(input, index+1)
			if !ok {
				return fmt.Errorf("JSON Unicode escape is malformed")
			}
			index += 4
			switch {
			case first >= 0xD800 && first <= 0xDBFF:
				if index+6 >= len(input) || input[index+1] != '\\' || input[index+2] != 'u' {
					return fmt.Errorf("JSON string contains an unpaired high surrogate")
				}
				second, valid := parseHexQuad(input, index+3)
				if !valid || second < 0xDC00 || second > 0xDFFF {
					return fmt.Errorf("JSON string contains an unpaired high surrogate")
				}
				index += 6
			case first >= 0xDC00 && first <= 0xDFFF:
				return fmt.Errorf("JSON string contains an unpaired low surrogate")
			}
		}
	}
	return nil
}

func parseHexQuad(input []byte, start int) (uint16, bool) {
	if start+4 > len(input) {
		return 0, false
	}
	var value uint16
	for _, character := range input[start : start+4] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value += uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

var rawMessageType = reflect.TypeOf(json.RawMessage{})
var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
var textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var textAppenderType = reflect.TypeOf((*encoding.TextAppender)(nil)).Elem()

func isOwnedStrictJSONMarshaler(valueType reflect.Type) bool {
	for valueType.Kind() == reflect.Pointer {
		valueType = valueType.Elem()
	}
	return slices.Contains([]reflect.Type{
		rawMessageType,
		reflect.TypeOf(ArtifactRecord{}),
		reflect.TypeOf(DurableCommand{}),
		reflect.TypeOf(DurableResponse{}),
		reflect.TypeOf(DurableRunItemSelector{}),
		reflect.TypeOf(InlineData{}),
		reflect.TypeOf(ResourceIntegrity{}),
		reflect.TypeOf(RolloutMode{}),
		reflect.TypeOf(EvolutionCommand{}),
		reflect.TypeOf(WorkResolution{}),
	}, valueType)
}

func implementsStrictJSONMarshaler(valueType reflect.Type) bool {
	if valueType.Implements(jsonMarshalerType) || valueType.Implements(textMarshalerType) ||
		valueType.Implements(textAppenderType) {
		return true
	}
	return valueType.Kind() != reflect.Pointer &&
		(reflect.PointerTo(valueType).Implements(jsonMarshalerType) ||
			reflect.PointerTo(valueType).Implements(textMarshalerType) ||
			reflect.PointerTo(valueType).Implements(textAppenderType))
}

func validateGoJSONStrings(value reflect.Value) error {
	return validateGoJSONStringsAt(value, make(map[jsonVisit]bool))
}

type jsonVisit struct {
	typeOf  reflect.Type
	pointer uintptr
}

func validateGoJSONStringsAt(value reflect.Value, active map[jsonVisit]bool) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return validateGoJSONStringsAt(value.Elem(), active)
	}
	if value.Type() == rawMessageType {
		if value.Len() == 0 {
			return nil
		}
		_, err := decodeUniqueJSON(value.Bytes())
		return err
	}
	if implementsStrictJSONMarshaler(value.Type()) && !isOwnedStrictJSONMarshaler(value.Type()) {
		return fmt.Errorf("strict JSON does not accept caller-defined marshalers")
	}
	if value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice {
		if value.IsNil() {
			return nil
		}
		visit := jsonVisit{typeOf: value.Type(), pointer: value.Pointer()}
		if active[visit] {
			return fmt.Errorf("JSON value contains a cycle")
		}
		active[visit] = true
		defer delete(active, visit)
		if value.Kind() == reflect.Pointer {
			return validateGoJSONStringsAt(value.Elem(), active)
		}
	}
	switch value.Kind() {
	case reflect.String:
		if !utf8.ValidString(value.String()) {
			return fmt.Errorf("JSON string is not valid UTF-8")
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateGoJSONStringsAt(iterator.Key(), active); err != nil {
				return err
			}
			if err := validateGoJSONStringsAt(iterator.Value(), active); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Type().Field(index).PkgPath != "" {
				continue
			}
			if err := validateGoJSONStringsAt(value.Field(index), active); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for index := 0; index < value.Len(); index++ {
			if err := validateGoJSONStringsAt(value.Index(index), active); err != nil {
				return err
			}
		}
	}
	return nil
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
			if integer, isInteger, err := mathematicalJSONInteger(number); err != nil {
				return nil, err
			} else if isInteger {
				return json.Number(integer.String()), nil
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
	integer, isInteger, err := mathematicalJSONInteger(value)
	if err != nil {
		return err
	}
	if isInteger {
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
	if math.Trunc(floating) == floating {
		return fmt.Errorf("non-integral JSON number is not distinguishable from an integer")
	}
	return nil
}

func parseSafeJSONUint(value json.Number, positive bool) (uint64, error) {
	integer, isInteger, err := mathematicalJSONInteger(value)
	if err != nil {
		return 0, err
	}
	if !isInteger {
		return 0, fmt.Errorf("value is not an unsigned integer")
	}
	if integer.Sign() < 0 || integer.Cmp(new(big.Int).SetUint64(maxExactInteger)) > 0 {
		return 0, fmt.Errorf("integer is outside the shared JSON domain")
	}
	decoded := integer.Uint64()
	if positive && decoded == 0 {
		return 0, fmt.Errorf("integer must be positive")
	}
	return decoded, nil
}

func mathematicalJSONInteger(value json.Number) (*big.Int, bool, error) {
	text := value.String()
	mantissa := text
	exponentText := "0"
	if index := strings.IndexAny(text, "eE"); index >= 0 {
		mantissa = text[:index]
		exponentText = text[index+1:]
	}
	exponent, ok := new(big.Int).SetString(exponentText, 10)
	if !ok {
		return nil, false, fmt.Errorf("invalid JSON number exponent")
	}
	negative := strings.HasPrefix(mantissa, "-")
	if negative {
		mantissa = mantissa[1:]
	}
	fractionDigits := 0
	if point := strings.IndexByte(mantissa, '.'); point >= 0 {
		fractionDigits = len(mantissa) - point - 1
		mantissa = mantissa[:point] + mantissa[point+1:]
	}
	coefficient, ok := new(big.Int).SetString(mantissa, 10)
	if !ok {
		return nil, false, fmt.Errorf("invalid JSON number coefficient")
	}
	if negative {
		coefficient.Neg(coefficient)
	}
	if coefficient.Sign() == 0 {
		return big.NewInt(0), true, nil
	}
	scale := new(big.Int).Sub(exponent, big.NewInt(int64(fractionDigits)))
	if scale.Sign() >= 0 {
		if !scale.IsInt64() || scale.Int64() > 16 {
			outside := new(big.Int).SetUint64(maxExactInteger + 1)
			if coefficient.Sign() < 0 {
				outside.Neg(outside)
			}
			return outside, true, nil
		}
		power := new(big.Int).Exp(big.NewInt(10), scale, nil)
		return new(big.Int).Mul(coefficient, power), true, nil
	}
	denominatorExponent := new(big.Int).Neg(scale)
	if !denominatorExponent.IsInt64() || denominatorExponent.Int64() > int64(len(mantissa)) {
		return nil, false, nil
	}
	denominator := new(big.Int).Exp(big.NewInt(10), denominatorExponent, nil)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(coefficient, denominator, remainder)
	if remainder.Sign() != 0 {
		return nil, false, nil
	}
	return quotient, true, nil
}

func interruptedFailure(request any, cause error, requestBegan bool) EngineFailure {
	kind := "cancelled"
	if cause == context.DeadlineExceeded {
		kind = "timed_out"
	}
	if requestBegan && requestIsMutating(request) {
		return EngineFailure{
			Category: "unknown_world_outcome", Phase: "transport",
			Code:             "engine_response_" + kind,
			Message:          "the Engine response was " + kind + " after a mutating request began",
			RetryDisposition: "reconcile",
		}
	}
	retry := "never"
	if kind == "timed_out" {
		retry = "retry_same_request"
	}
	return EngineFailure{
		Category: kind, Phase: "transport", Code: "engine_response_" + kind,
		Message: "the Engine response was " + kind, RetryDisposition: retry,
	}
}

func responseLossFailure(request any, code string) EngineFailure {
	if requestIsMutating(request) {
		return EngineFailure{
			Category: "unknown_world_outcome", Phase: "transport", Code: code,
			Message:          "the Engine response was unavailable after a mutating request began",
			RetryDisposition: "reconcile",
		}
	}
	return transportFailure(code, "the Engine response was unavailable")
}

func invalidEngineResponseForRequest(request any, detail string) EngineFailure {
	if request != nil && requestIsMutating(request) {
		return responseLossFailure(request, "invalid_engine_response")
	}
	return transportFailure("invalid_engine_response", detail)
}

func requestIsMutating(request any) bool {
	object, ok := request.(map[string]any)
	if !ok {
		return false
	}
	typeName, _ := object["type"].(string)
	if typeName == "run" || typeName == "observe_clock" || typeName == "execute_live_evolution" {
		return true
	}
	if typeName != "execute_durable" {
		return false
	}
	readOnly := []string{
		"run_index_page", "run_current", "run_wait_page", "run_effect_page",
		"run_occurrence_page", "run_attempt_page", "run_item",
	}
	switch command := object["command"].(type) {
	case DurableCommand:
		return !slices.Contains(readOnly, command.Type)
	case map[string]any:
		commandType, ok := command["type"].(string)
		return ok && !slices.Contains(readOnly, commandType)
	default:
		return false
	}
}

func transportFailure(code, message string) EngineFailure {
	return EngineFailure{
		Category: "transport_failure", Phase: "transport", Code: code, Message: message,
	}
}

func validationFailure(code, message string) EngineFailure {
	return EngineFailure{
		Category: "validation", Phase: "validate_request", Code: code, Message: message,
		RetryDisposition: "correct_and_retry",
	}
}

func unexpectedEngineResponse(expected, received string) EngineFailure {
	return EngineFailure{
		Category: "contract_violation", Phase: "transport", Code: "unexpected_engine_response",
		Message: fmt.Sprintf("expected %s, received %q", expected, received), RetryDisposition: "never",
	}
}
