// Package cymule provides Go authoring and Engine client APIs.
package cymule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
)

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

// ParkReason identifies one exact indexed condition for virtual work.
type ParkReason struct {
	Kind       string `json:"kind"`
	Key        string `json:"key,omitempty"`
	WorkID     string `json:"work_id,omitempty"`
	Account    string `json:"account,omitempty"`
	Capability string `json:"capability,omitempty"`
	Domain     string `json:"domain,omitempty"`
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
	ControlVersion string         `json:"control_version"`
	CommandID      string         `json:"command_id"`
	WorkID         string         `json:"work_id"`
	Owner          string         `json:"owner"`
	Epoch          uint64         `json:"epoch"`
	Resolution     WorkResolution `json:"resolution"`
}

// VirtualWorkControl is a transport-neutral occurrence query/control boundary.
type VirtualWorkControl interface {
	Occurrence(occurrenceID string) (*WorkOccurrence, error)
	Resolve(command WorkResolutionCommand) (WorkOccurrence, error)
}

// SucceedWork creates a terminal-success control command.
func SucceedWork(commandID, workID, owner string, epoch uint64, result ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, WorkResolution{
		Kind: "succeeded", Result: &result,
	})
}

// RetryWork creates a retry control command with an optional indexed condition.
func RetryWork(commandID, workID, owner string, epoch uint64, failure ArtifactRef, nextReason *ParkReason) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, WorkResolution{
		Kind: "retry", Error: &failure, NextReason: nextReason,
	})
}

// ParkWork creates a non-failure parked disposition command.
func ParkWork(commandID, workID, owner string, epoch uint64, reason ParkReason) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, WorkResolution{
		Kind: "parked", ParkReason: &reason,
	})
}

// FailWork creates a terminal-failure control command.
func FailWork(commandID, workID, owner string, epoch uint64, failure ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, WorkResolution{
		Kind: "failed", Error: &failure,
	})
}

// CancelWork creates an active-occurrence cancellation command.
func CancelWork(commandID, workID, owner string, epoch uint64, reason ArtifactRef) WorkResolutionCommand {
	return workResolutionCommand(commandID, workID, owner, epoch, WorkResolution{
		Kind: "cancelled", CancelReason: &reason,
	})
}

func workResolutionCommand(commandID, workID, owner string, epoch uint64, resolution WorkResolution) WorkResolutionCommand {
	return WorkResolutionCommand{
		ControlVersion: "cymule.virtual-work-control/1",
		CommandID:      commandID,
		WorkID:         workID,
		Owner:          owner,
		Epoch:          epoch,
		Resolution:     resolution,
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
		IRVersion:  "cymule.ir/1",
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
		err = fmt.Errorf("unexpected engine response %q", response.Type)
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
		err = fmt.Errorf("unexpected engine response %q", response.Type)
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
		err = fmt.Errorf("unexpected engine response %q", response.Type)
	}
	return response.Activation, err
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
		err = fmt.Errorf("unexpected engine response %q", response.Type)
	}
	return response.Result, err
}

func (engine CliEngine) request(request any, response any) error {
	input, err := json.Marshal(request)
	if err != nil {
		return err
	}
	command := exec.Command(engine.Executable, "rpc")
	command.Stdin = bytes.NewReader(input)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("engine failed: %w: %s", err, stderr.String())
	}
	return json.Unmarshal(stdout.Bytes(), response)
}
