package cymule

import (
	"os"
	"reflect"
	"testing"
)

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
