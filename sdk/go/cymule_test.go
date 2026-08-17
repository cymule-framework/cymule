package cymule

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestJSONResourcePreservesNullValue(t *testing.T) {
	encoded, err := json.Marshal(JSONResource(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	inline := wire["inline"].(map[string]any)
	if value, exists := inline["value"]; !exists || value != nil {
		t.Fatalf("inline JSON null was not preserved: %#v", inline)
	}
}

func TestCrossLanguageEndToEnd(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	pluginPath := os.Getenv("CYMULE_TEST_PLUGIN")
	expectedPlanID := os.Getenv("CYMULE_EXPECTED_PLAN_ID")
	if enginePath == "" || pluginPath == "" || expectedPlanID == "" {
		t.Skip("cross-language binaries are not configured")
	}
	profile := EffectProfile{
		Mutation: "mutating", Dispatch: "on_scope_commit", Reconciliation: "queryable",
		KeyedIdempotency: true, Irreversible: false,
	}
	candidate := NewFlow("cross_language_echo", map[string]any{}, map[string]any{}).
		Component("test.echo", map[string]any{}, map[string]any{}).
		EffectContract("test.capture", map[string]any{}, map[string]any{}, profile).
		Call("call.echo", "test.echo", Expression{"kind": "input"}, "echoed").
		Effect("effect.capture", "test.capture", Expression{"kind": "binding", "name": "echoed"}, "primary").
		Finish(Expression{"kind": "binding", "name": "echoed"})

	engine := CliEngine{Executable: enginePath}
	plan, err := engine.Seal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if plan.PlanID != expectedPlanID {
		t.Fatalf("plan ID %q does not match expected", plan.PlanID)
	}
	input := map[string]any{"message": "hello from Go"}
	result, err := engine.Run(plan, input, pluginPath, "run:go-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Value, input) {
		t.Fatalf("result %#v does not equal input %#v", result.Value, input)
	}
	if len(result.Effects) != 1 {
		t.Fatalf("expected one effect, got %d", len(result.Effects))
	}
}

func TestResourceSealsThroughRustEngine(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	expectedResourceID := os.Getenv("CYMULE_EXPECTED_RESOURCE_ID")
	if enginePath == "" || expectedResourceID == "" {
		t.Skip("resource engine conformance is not configured")
	}
	resource, err := (CliEngine{Executable: enginePath}).SealResource(TextResource(
		"shared cross-run resource",
		map[string]string{"purpose": "cross-language-conformance"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if resource.ResourceID != expectedResourceID {
		t.Fatalf("resource ID %q does not match expected", resource.ResourceID)
	}
	if resource.Integrity.Kind != "inline" {
		t.Fatalf("unexpected integrity kind %q", resource.Integrity.Kind)
	}
}

func TestWaitActivationValidatesThroughRustEngine(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	fixturePath := os.Getenv("CYMULE_WAIT_ACTIVATION_FIXTURE")
	if enginePath == "" || fixturePath == "" {
		t.Skip("wait activation engine conformance is not configured")
	}
	activation := SignalWaitActivation(
		"activation:shared:1",
		"signal:continue",
		[]string{"wait:shared:1"},
		ArtifactRef{
			ArtifactID: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Kind:       "cymule.wait-activation-result/1",
		},
	)
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture WaitActivation
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(activation, fixture) {
		t.Fatalf("activation differs from shared fixture: %#v", activation)
	}
	verified, err := (CliEngine{Executable: enginePath}).VerifyWaitActivation(activation)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, activation) {
		t.Fatalf("unexpected activation: %#v", verified)
	}
}
