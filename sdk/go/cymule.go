// Package cymule provides Go authoring and Engine client APIs.
package cymule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
)

// Expression is one frozen IR expression.
type Expression map[string]any

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
