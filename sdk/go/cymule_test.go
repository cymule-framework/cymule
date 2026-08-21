package cymule

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCliEnginePreservesPreCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (CliEngine{Context: ctx}).Seal(NewFlow("cancel", map[string]any{}, map[string]any{}).
		Finish(Expression{"kind": "input"}))
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Category != "cancelled" {
		t.Fatalf("expected structured cancellation, got %v", err)
	}
}

func TestStructuredEngineFailures(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	pluginPath := os.Getenv("CYMULE_TEST_PLUGIN")
	failurePath := os.Getenv("CYMULE_ENGINE_FAILURE_FIXTURE")
	if enginePath == "" || pluginPath == "" || failurePath == "" {
		t.Skip("Engine failure conformance is not configured")
	}
	fixtureBytes, err := os.ReadFile(failurePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases map[string]EngineFailure `json:"cases"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	candidateBytes, err := os.ReadFile(filepath.Join(filepath.Dir(failurePath), "cross-language-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var candidate PlanCandidate
	if err := json.Unmarshal(candidateBytes, &candidate); err != nil {
		t.Fatal(err)
	}
	engine := CliEngine{Executable: enginePath}
	invalid := candidate
	invalid.IRVersion = "cymule.ir/unsupported"
	_, err = engine.Seal(invalid)
	assertEngineFailure(t, err, fixture.Cases["invalid_plan_version"])
	plan, err := engine.Seal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(plan, map[string]any{"simulate": "expected_failure"}, pluginPath, "run:go-expected")
	assertEngineFailure(t, err, fixture.Cases["expected_plugin_failure"])
	_, err = engine.Run(plan, map[string]any{"message": "defect"}, enginePath, "run:go-defect")
	assertEngineFailure(t, err, fixture.Cases["plugin_defect"])
	_, err = engine.Run(plan, map[string]any{"message": "substrate"}, "/cymule-conformance/missing-plugin", "run:go-substrate")
	assertEngineFailure(t, err, fixture.Cases["substrate_failure"])
}

func assertEngineFailure(t *testing.T, err error, expected EngineFailure) {
	t.Helper()
	failure, ok := err.(EngineFailure)
	if !ok {
		t.Fatalf("expected EngineFailure, got %T: %v", err, err)
	}
	if failure.Category != expected.Category || failure.Phase != expected.Phase ||
		failure.Code != expected.Code || failure.RetryDisposition != expected.RetryDisposition {
		t.Fatalf("failure %#v does not match expected %#v", failure, expected)
	}
}

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

func TestEngineJSONRejectsDuplicateObjectMembers(t *testing.T) {
	var value map[string]any
	err := decodeClosedJSON(
		[]byte(`{"response":{"type":"verified","type":"executed"}}`),
		&value,
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate JSON object member") {
		t.Fatalf("expected duplicate member rejection, got %v", err)
	}
}

func TestEngineEnvelopeRequiresExclusiveOutcomePayload(t *testing.T) {
	invalid := []string{
		`{"engine_protocol":"cymule.engine/2","outcome":"success","response":{"type":"verified"},"error":{"category":"validation","phase":"transport","code":"invalid","message":"invalid"}}`,
		`{"engine_protocol":"cymule.engine/2","outcome":"failure","response":{"type":"verified"},"error":{"category":"validation","phase":"transport","code":"invalid","message":"invalid"}}`,
		`{"engine_protocol":"cymule.engine/2","outcome":"success"}`,
	}
	for _, input := range invalid {
		var response struct {
			Type string `json:"type"`
		}
		err := decodeEngineResponse([]byte(input), &response)
		if err == nil || !strings.Contains(err.Error(), "invalid_engine_response") {
			t.Fatalf("expected exclusive envelope rejection for %s, got %v", input, err)
		}
	}
}

func TestEvolutionAndExecutionResponseUnionsAreClosed(t *testing.T) {
	for _, input := range []string{
		`{"control_version":"cymule.evolution-control/3","command_id":"command:test","operation":"future"}`,
		`{"control_version":"cymule.evolution-control/3","command_id":"command:test","operation":"select_occurrence","occurrence_id":"occurrence:test","patch":{}}`,
		`{"control_version":"cymule.evolution-control/3","command_id":"command:test","operation":"migrate","request":{"unexpected":true}}`,
	} {
		var command EvolutionCommand
		if err := decodeClosedJSON([]byte(input), &command); err == nil {
			t.Fatalf("expected closed Evolution rejection for %s", input)
		}
	}
	var live LiveEvolutionCommand
	if err := decodeClosedJSON([]byte(
		`{"control_version":"cymule.live-evolution-control/2","command_id":"command:test","operation":"future"}`,
	), &live); err == nil {
		t.Fatal("expected closed live Evolution rejection")
	}

	for _, input := range []string{
		`{"status":"future"}`,
		`{"status":"completed","result":{},"suspension":{}}`,
		`{"status":"completed","result":{}}`,
		`{"status":"suspended","suspension":{"run_id":"run:test","plan_id":"sha256:test","definition_id":"main","invocation_id":"main","site_id":"wait:test","wait":{"kind":"future","unexpected":true},"result_bind":null}}`,
		`{"status":"release_required","release":null}`,
		`{"status":"reconciliation_required","reconciliation":null}`,
	} {
		var outcome ExecutionOutcome
		if err := decodeClosedJSON([]byte(input), &outcome); err == nil {
			t.Fatalf("expected closed execution rejection for %s", input)
		}
	}
	for _, input := range []string{
		`{"status":"release_required","release":{"run_id":"run:test","plan_id":"sha256:test","intent_ids":["intent:test"]}}`,
		`{"status":"reconciliation_required","reconciliation":{"run_id":"run:test","plan_id":"sha256:test","intent_id":"intent:test"}}`,
	} {
		var outcome ExecutionOutcome
		if err := decodeClosedJSON([]byte(input), &outcome); err != nil {
			t.Fatalf("expected closed execution boundary to decode for %s: %v", input, err)
		}
	}
}

func TestEvolutionControlValidatesThroughRust(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	fixturePath := os.Getenv("CYMULE_EVOLUTION_CONTROL_FIXTURE")
	if enginePath == "" || fixturePath == "" {
		t.Skip("evolution control conformance is not configured")
	}
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var expected EvolutionCommand
	if err := json.Unmarshal(fixtureBytes, &expected); err != nil {
		t.Fatal(err)
	}
	command := ApplyEvolutionGate(
		"command:evolution:fixture:promote",
		RolloutGate{
			GateID:                 "gate:fixture:promote",
			DecisionID:             "rollout:fixture:canary",
			MinTargetObservations:  3,
			MaxTargetFailures:      0,
			MinEquivalentShadows:   2,
			MaxInequivalentShadows: 0,
		},
		"rollout:fixture:active",
	)
	if !reflect.DeepEqual(command, expected) {
		t.Fatalf("evolution command differs from shared fixture: %#v", command)
	}
	verified, err := (CliEngine{Executable: enginePath}).VerifyEvolutionCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, command) {
		t.Fatalf("Rust engine changed evolution command: %#v", verified)
	}
	restartPath := os.Getenv("CYMULE_EVOLUTION_RESTART_FIXTURE")
	if restartPath == "" {
		t.Skip("evolution restart conformance is not configured")
	}
	restartBytes, err := os.ReadFile(restartPath)
	if err != nil {
		t.Fatal(err)
	}
	var restartExpected EvolutionCommand
	if err := json.Unmarshal(restartBytes, &restartExpected); err != nil {
		t.Fatal(err)
	}
	restart := RestartEvolutionRun(
		"command:evolution:fixture:restart",
		*restartExpected.Restart,
	)
	if !reflect.DeepEqual(restart, restartExpected) {
		t.Fatalf("restart command differs from shared fixture: %#v", restart)
	}
	restartVerified, err := (CliEngine{Executable: enginePath}).VerifyEvolutionCommand(restart)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restartVerified, restart) {
		t.Fatalf("Rust engine changed restart command: %#v", restartVerified)
	}
}

func TestUnifiedLiveEvolutionValidatesThroughRust(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	fixturePath := os.Getenv("CYMULE_LIVE_EVOLUTION_CONTROL_FIXTURE")
	if enginePath == "" || fixturePath == "" {
		t.Skip("live-evolution conformance is not configured")
	}
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var expected LiveEvolutionCommand
	if err := json.Unmarshal(fixtureBytes, &expected); err != nil {
		t.Fatal(err)
	}
	command := ApplyLiveEvolution(
		"command:live-evolution:fixture:select",
		"template:review-parent",
		*expected.Command,
		nil,
	)
	if !reflect.DeepEqual(command, expected) {
		t.Fatalf("live-evolution command differs from shared fixture: %#v", command)
	}
	verified, err := (CliEngine{Executable: enginePath}).VerifyLiveEvolutionCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, command) {
		t.Fatalf("Rust engine changed live-evolution command: %#v", verified)
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
		Component("test.echo", map[string]any{}, map[string]any{}, map[string]string{}).
		EffectContract("test.capture", map[string]any{}, map[string]any{}, profile, map[string]string{}).
		Definition(Definition{
			ID: "echo_subflow", InputSchema: map[string]any{}, OutputSchema: map[string]any{},
			Body: Region{
				Steps: []Step{{
					"id": "call.echo", "op": "call", "component": "test.echo",
					"input": Expression{"kind": "input"}, "bind": "echoed",
				}},
				Result: Expression{"kind": "binding", "name": "echoed"},
			},
		}).
		Invoke("invoke.echo-subflow", "echo_subflow", Expression{"kind": "input"}, "echoed").
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
	execution, err := engine.Run(plan, input, pluginPath, "run:go-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != "completed" || execution.Result == nil {
		t.Fatalf("expected completed execution, got %#v", execution)
	}
	if !reflect.DeepEqual(execution.Result.Value, input) {
		t.Fatalf("result %#v does not equal input %#v", execution.Result.Value, input)
	}
	if len(execution.Result.Effects) != 1 {
		t.Fatalf("expected one effect, got %d", len(execution.Result.Effects))
	}
	executor := ProcessPlugin(pluginPath, "")
	store := SQLiteStore(filepath.Join(t.TempDir(), "domain.sqlite"), "sdk-go")
	durable := DurableEngine{
		Store: store, Executor: &executor, Transport: engine,
	}
	response, err := durable.Start("run:go-durable-e2e", candidate, input)
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != "run_boundary" {
		t.Fatalf("unexpected durable response %q", response.Type)
	}
	if run, err := (DurableEngine{Store: store, Transport: engine}).Get("run:go-durable-e2e"); err != nil || string(run) == "null" {
		t.Fatalf("durable Run query failed: %s %v", run, err)
	}
	evolved, err := durable.Evolve(PublishLiveDefinition(
		"evolve:go:publish", "definition:go:echo", candidate.Definitions[0],
	))
	if err != nil || evolved.Result != "definition_published" {
		t.Fatalf("durable evolution failed: %#v %v", evolved, err)
	}
}

func TestMaliciousNestedEngineSuccessFailsClosed(t *testing.T) {
	malicious := os.Getenv("CYMULE_MALICIOUS_ENGINE")
	if malicious == "" {
		t.Skip("malicious Engine conformance is not configured")
	}
	_, err := (DurableEngine{Store: DirectoryStore("unused"), Transport: CliEngine{Executable: malicious}}).Get("run:fake")
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Code != "invalid_engine_response" {
		t.Fatalf("expected nested response rejection, got %v", err)
	}
}

func TestFlowFinishReturnsFrozenCandidate(t *testing.T) {
	builder := NewFlow("frozen", map[string]any{}, map[string]any{})
	candidate := builder.Finish(Expression{"kind": "input"})
	builder.Component("later", map[string]any{}, map[string]any{}, map[string]string{"capability": "late"})
	if len(candidate.Components) != 0 {
		t.Fatal("finished candidate changed after builder mutation")
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
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Kind:            "cymule.wait-activation-result/1",
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

func TestDurableControlValidatesThroughRustEngine(t *testing.T) {
	enginePath := os.Getenv("CYMULE_BIN")
	fixturePath := os.Getenv("CYMULE_DURABLE_CONTROL_FIXTURE")
	if enginePath == "" || fixturePath == "" {
		t.Skip("durable control conformance is not configured")
	}
	command := QueryDurableDomain("query:cross-language-domain")
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture DurableCommand
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(command, fixture) {
		t.Fatalf("durable command differs from shared fixture: %#v", command)
	}
	verified, err := (CliEngine{Executable: enginePath}).VerifyDurableCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verified, command) {
		t.Fatalf("Rust engine changed durable command: %#v", verified)
	}
	activation, err := ActivateDurableSignal(
		"activation:sdk", "signal:sdk", []string{"wait:z", "wait:a", "wait:z"},
		map[string]any{"accepted": true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(activation.WaitIDs, []string{"wait:a", "wait:z"}) {
		t.Fatalf("durable activation targets are not canonical: %#v", activation.WaitIDs)
	}
}

func TestVirtualWorkQueryAndControlFixturesStayExact(t *testing.T) {
	occurrencePath := os.Getenv("CYMULE_VIRTUAL_OCCURRENCE_FIXTURE")
	controlPath := os.Getenv("CYMULE_VIRTUAL_CONTROL_FIXTURE")
	if occurrencePath == "" || controlPath == "" {
		t.Skip("virtual work SDK conformance is not configured")
	}
	occurrenceBytes, err := os.ReadFile(occurrencePath)
	if err != nil {
		t.Fatal(err)
	}
	var occurrence WorkOccurrence
	if err := json.Unmarshal(occurrenceBytes, &occurrence); err != nil {
		t.Fatal(err)
	}
	if occurrence.OccurrenceBinding != "binding:worker/fixture@1" {
		t.Fatalf("unexpected occurrence: %#v", occurrence)
	}
	command := SucceedWork(
		"command:virtual:fixture:success",
		"work:fixture",
		"worker:fixture",
		1,
		1,
		101,
		ArtifactRef{
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			Kind:            "example/result",
		},
	)
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	controlBytes, err := os.ReadFile(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded WorkResolutionCommand
	if err := json.Unmarshal(controlBytes, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Resolution.Kind != "succeeded" || decoded.Resolution.Result == nil {
		t.Fatalf("unexpected decoded control: %#v", decoded)
	}
	var expected any
	var actual any
	if err := json.Unmarshal(controlBytes, &expected); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("control differs from shared fixture: %#v", actual)
	}
	migrationPath := os.Getenv("CYMULE_VIRTUAL_MIGRATION_FIXTURE")
	if migrationPath == "" {
		t.Skip("virtual region migration SDK conformance is not configured")
	}
	migrationBytes, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatal(err)
	}
	var migrationFixture RegionMigrationCommand
	if err := json.Unmarshal(migrationBytes, &migrationFixture); err != nil {
		t.Fatal(err)
	}
	migration := MigrateRegions(
		"command:migration:fixture-split",
		migrationFixture.Plan,
	)
	if !reflect.DeepEqual(migration, migrationFixture) {
		t.Fatalf("migration differs from shared fixture: %#v", migration)
	}
	compactionPath := os.Getenv("CYMULE_VIRTUAL_COMPACTION_FIXTURE")
	rehydrationPath := os.Getenv("CYMULE_VIRTUAL_REHYDRATION_FIXTURE")
	if compactionPath == "" || rehydrationPath == "" {
		t.Skip("virtual archive SDK conformance is not configured")
	}
	compactionBytes, err := os.ReadFile(compactionPath)
	if err != nil {
		t.Fatal(err)
	}
	var compactionFixture VirtualCompactionCommand
	if err := json.Unmarshal(compactionBytes, &compactionFixture); err != nil {
		t.Fatal(err)
	}
	compaction := CompactVirtualRegion(
		"command:compaction:fixture",
		"region:fixture",
		[]string{"virtual:fixture:terminal"},
		"binding:archive/fixture@1",
		"compactor:fixture/1",
	)
	if !reflect.DeepEqual(compaction, compactionFixture) {
		t.Fatalf("compaction differs from shared fixture: %#v", compaction)
	}
	rehydrationBytes, err := os.ReadFile(rehydrationPath)
	if err != nil {
		t.Fatal(err)
	}
	var rehydrationFixture VirtualRehydrationCommand
	if err := json.Unmarshal(rehydrationBytes, &rehydrationFixture); err != nil {
		t.Fatal(err)
	}
	rehydration := RehydrateVirtualOccurrences(
		"command:rehydration:fixture",
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		[]string{occurrence.OccurrenceID},
	)
	if !reflect.DeepEqual(rehydration, rehydrationFixture) {
		t.Fatalf("rehydration differs from shared fixture: %#v", rehydration)
	}
	claimPath := os.Getenv("CYMULE_VIRTUAL_CLAIM_FIXTURE")
	renewalPath := os.Getenv("CYMULE_VIRTUAL_LEASE_RENEWAL_FIXTURE")
	recoveryPath := os.Getenv("CYMULE_VIRTUAL_RECOVERY_FIXTURE")
	if claimPath == "" || renewalPath == "" || recoveryPath == "" {
		t.Skip("virtual scheduling SDK conformance is not configured")
	}
	claimBytes, err := os.ReadFile(claimPath)
	if err != nil {
		t.Fatal(err)
	}
	var claimFixture VirtualClaimCommand
	if err := json.Unmarshal(claimBytes, &claimFixture); err != nil {
		t.Fatal(err)
	}
	claim := ClaimVirtualWork(
		"command:claim:fixture",
		"worker:fixture",
		"slot:worker-fixture:0",
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"binding:worker/fixture@1",
		[]string{"sandbox", "cpu", "cpu"},
		100,
		30,
	)
	if !reflect.DeepEqual(claim, claimFixture) {
		t.Fatalf("claim differs from shared fixture: %#v", claim)
	}
	renewalBytes, err := os.ReadFile(renewalPath)
	if err != nil {
		t.Fatal(err)
	}
	var renewalFixture VirtualLeaseRenewalCommand
	if err := json.Unmarshal(renewalBytes, &renewalFixture); err != nil {
		t.Fatal(err)
	}
	renewal := RenewVirtualClaim(
		"command:renew:fixture",
		"work:fixture",
		"worker:fixture",
		1,
		1,
		120,
		30,
	)
	if !reflect.DeepEqual(renewal, renewalFixture) {
		t.Fatalf("renewal differs from shared fixture: %#v", renewal)
	}
	recoveryBytes, err := os.ReadFile(recoveryPath)
	if err != nil {
		t.Fatal(err)
	}
	var recoveryFixture VirtualRecoveryCommand
	if err := json.Unmarshal(recoveryBytes, &recoveryFixture); err != nil {
		t.Fatal(err)
	}
	recovery := RecoverVirtualClaim(
		"command:recovery:fixture",
		"work:fixture",
		"worker:fixture",
		1,
		2,
		150,
		recoveryFixture.Resolution,
	)
	recoveryBytesActual, err := json.Marshal(recovery)
	if err != nil {
		t.Fatal(err)
	}
	var recoveryActual any
	var recoveryExpected any
	if err := json.Unmarshal(recoveryBytesActual, &recoveryActual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recoveryBytes, &recoveryExpected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recoveryActual, recoveryExpected) {
		t.Fatalf("recovery differs from shared fixture: %#v", recoveryActual)
	}
	runWeightPath := os.Getenv("CYMULE_VIRTUAL_RUN_WEIGHT_FIXTURE")
	if runWeightPath == "" {
		t.Skip("virtual Run weight SDK conformance is not configured")
	}
	runWeightBytes, err := os.ReadFile(runWeightPath)
	if err != nil {
		t.Fatal(err)
	}
	var runWeightFixture VirtualRunWeightCommand
	if err := json.Unmarshal(runWeightBytes, &runWeightFixture); err != nil {
		t.Fatal(err)
	}
	runWeight := SetVirtualRunWeight("command:run-weight:fixture", "run:fixture", 3)
	if !reflect.DeepEqual(runWeight, runWeightFixture) {
		t.Fatalf("Run weight differs from shared fixture: %#v", runWeight)
	}
}
