package cymule

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fixtureExecution() ExecutionClaimRequest {
	return ExecutionClaimRequest{
		Owner: "driver:cross-language",
		Clock: ClockObservationRef{
			ClockVersion:     "cymule.clock-observation/2",
			ObservationID:    "sha256:" + strings.Repeat("1", 64),
			SourceID:         "clock:cross-language",
			SourceGeneration: "sha256:" + strings.Repeat("2", 64),
			Scope:            "sha256:7aa23baf73ce53a540a6f3eddaa0175e6be22d751e5d5090d5d77485f58fa74c",
		},
		TTL: 30,
	}
}

func fixedArtifact(digit, kind string) map[string]any {
	return map[string]any{
		"identity_version": "cymule.artifact/2",
		"artifact_id":      fixedContentID(digit),
		"kind":             kind,
	}
}

func fixedArtifactRecord(kind string, data []byte) ArtifactRecord {
	return ArtifactRecord{
		Reference: ArtifactRef{
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      artifactRecordID(kind, data),
			Kind:            kind,
		},
		Bytes: slices.Clone(data),
	}
}

func fixedContentID(digit string) string {
	return "sha256:" + strings.Repeat(digit, 64)
}

func TestArtifactRecordUsesBoundedCanonicalBase64Wire(t *testing.T) {
	record := fixedArtifactRecord("cymule.evolution-evidence/1", []byte("publication evidence"))
	wire, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(wire, &value); err != nil {
		t.Fatal(err)
	}
	if value["bytes"] != base64.StdEncoding.EncodeToString(record.Bytes) {
		t.Fatalf("Artifact record did not emit canonical Base64: %s", wire)
	}
	var decoded ArtifactRecord
	if err := json.Unmarshal(wire, &decoded); err != nil || !reflect.DeepEqual(decoded, record) {
		t.Fatalf("Artifact record Base64 round trip failed: %#v %v", decoded, err)
	}
	for name, malformed := range map[string]string{
		"numeric array": `{"reference":{"identity_version":"cymule.artifact/2","artifact_id":"sha256:` + strings.Repeat("0", 64) + `","kind":"cymule.evolution-evidence/1"},"bytes":[112]}`,
		"unpadded":      `{"reference":{"identity_version":"cymule.artifact/2","artifact_id":"sha256:` + strings.Repeat("0", 64) + `","kind":"cymule.evolution-evidence/1"},"bytes":"YQ"}`,
		"duplicate":     `{"reference":{"identity_version":"cymule.artifact/2","artifact_id":"sha256:` + strings.Repeat("0", 64) + `","kind":"cymule.evolution-evidence/1"},"bytes":"","bytes":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(malformed), &decoded); err == nil {
				t.Fatal("malformed Artifact record wire was accepted")
			}
		})
	}
	oversized := record
	oversized.Bytes = make([]byte, maxArtifactBytes+1)
	if _, err := json.Marshal(oversized); err == nil {
		t.Fatal("Artifact record above 8 MiB was serialized")
	}
}

func fixedDigest(digit string) string {
	return strings.Repeat(digit, 64)
}

func TestVirtualBuildersEnforceClosedWireAuthority(t *testing.T) {
	identity := strings.Repeat("🧪", 512)
	clock := fixtureExecution().Clock
	clock.Scope = identity
	binding := ArtifactRef{
		IdentityVersion: "cymule.artifact/2",
		ArtifactID:      fixedContentID("2"),
		Kind:            "cymule.execution-binding/2",
	}
	evidence := binding
	evidence.Kind = "example/evidence"
	resolution := WorkResolution{Kind: "retry", Error: new(evidence)}
	if _, err := ClaimVirtualWork(identity, identity, identity, binding, []string{identity}, clock, 30); err != nil {
		t.Fatalf("maximum Unicode scalar identity was rejected: %v", err)
	}
	for name, invalid := range map[string]string{
		"empty": "", "too long": strings.Repeat("🧪", 513), "C1": "id:\u0085", "surrogate bytes": "id:\xed\xa0\x80",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ClaimVirtualWork(invalid, identity, identity, binding, nil, clock, 30); err == nil {
				t.Fatal("invalid claim command identity was accepted")
			}
			if _, err := ClaimVirtualWork(identity, invalid, identity, binding, nil, clock, 30); err == nil {
				t.Fatal("invalid claim owner was accepted")
			}
			if _, err := ClaimVirtualWork(identity, identity, identity, binding, []string{invalid}, clock, 30); err == nil {
				t.Fatal("invalid claim capability was accepted")
			}
			if _, err := RenewVirtualClaim(identity, invalid, identity, 1, 1, clock, 30); err == nil {
				t.Fatal("invalid renewal work identity was accepted")
			}
			if _, err := RecoverVirtualClaim(identity, identity, invalid, 1, 1, clock, resolution); err == nil {
				t.Fatal("invalid recovery owner was accepted")
			}
			if _, err := SucceedWork(identity, invalid, identity, 1, 1, clock, evidence); err == nil {
				t.Fatal("invalid resolution work identity was accepted")
			}
		})
	}
	invalidBinding := binding
	invalidBinding.ArtifactID = "sha256:not-a-digest"
	if _, err := ClaimVirtualWork(identity, identity, identity, invalidBinding, nil, clock, 30); err == nil {
		t.Fatal("malformed claim binding was accepted")
	}
	for _, invalid := range []WorkResolution{
		{Kind: "succeeded", Result: new(evidence)},
		{Kind: "parked", ParkReason: &ParkReason{Kind: "wait", Key: "wait:fixture"}},
		{Kind: "retry", Error: new(evidence), Result: new(evidence)},
		{Kind: "failed"},
	} {
		if _, err := RecoverVirtualClaim(identity, identity, identity, 1, 1, clock, invalid); err == nil {
			t.Fatalf("invalid recovery resolution was accepted: %#v", invalid)
		}
	}
	if _, err := RecoverVirtualClaim(identity, identity, identity, 1, 1, clock, resolution); err != nil {
		t.Fatalf("closed recovery was rejected: %v", err)
	}
}

func fixedCandidate() PlanCandidate {
	return NewFlow("transport_test", map[string]any{}, map[string]any{}).
		Finish(Expression{"kind": "input"})
}

func fixedPlan() SealedPlan {
	return SealedPlan{
		PlanID:    fixedContentID("a"),
		Candidate: fixedCandidate(),
	}
}

func testProcessConfig(t *testing.T, executable string) EngineProcessConfig {
	t.Helper()
	absolute, err := filepath.Abs(executable)
	if err != nil {
		t.Fatal(err)
	}
	return EngineProcessConfig{
		Executable:  absolute,
		Arguments:   []string{},
		Environment: map[string]string{"CYMULE_TEST_EFFECT_LEDGER_PATH": filepath.Join(t.TempDir(), "effects.sqlite3")},
		RuntimeClosure: map[string]string{
			"component-runtime": "sha256:" + strings.Repeat("a", 64),
		},
		TimeoutMS:    60_000,
		MessageLimit: 8 * 1024 * 1024,
		ClosureLimit: 64 * 1024 * 1024,
	}
}

func testProcessPlugin(t *testing.T, executable string) EnginePluginTarget {
	t.Helper()
	return ProcessPlugin(testProcessConfig(t, executable))
}

func testPinnedProcessPlugin(t *testing.T, executable, revision string) EnginePluginTarget {
	t.Helper()
	config := testProcessConfig(t, executable)
	config.MessageLimit = evolutionPluginMessageBytes
	return PinnedProcessPlugin(config, revision)
}

func testDurableTargetForCommand(t *testing.T, command DurableCommand) EngineDurableTarget {
	t.Helper()
	target := EngineDurableTarget{Store: DirectoryStore("unused")}
	if durableCommandNeedsExecutor(command) {
		executor := testProcessPlugin(t, "/bin/true")
		target.Executor = &executor
	}
	if durableCommandNeedsClock(command) {
		clock := SQLiteClock("unused", "clock:test-target", fixedContentID("9"))
		target.Clock = &clock
	}
	return target
}

func testEvolutionTargetForCommand(t *testing.T, command LiveEvolutionCommand) EngineEvolutionTarget {
	t.Helper()
	target := EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}
	if command.Operation == "apply" && command.Command != nil {
		switch command.Command.Operation {
		case "migrate":
			request := command.Command.Migration
			if request == nil {
				t.Fatal("migration command has no request")
			}
			target.MigrationAdapter = &EngineMigrationProviderTarget{
				AdapterID: request.AdapterID, AdapterRevision: request.AdapterRevision,
				Process: testPinnedProcessPlugin(t, "/bin/true", request.AdapterRevision),
			}
		case "shadow":
			request := command.Command.Shadow
			if request == nil {
				t.Fatal("shadow command has no request")
			}
			target.ShadowDriver = &EngineShadowProviderTarget{
				DriverID: request.DriverID, DriverRevision: request.DriverRevision,
				Process: testPinnedProcessPlugin(t, "/bin/true", request.DriverRevision),
			}
		}
	}
	return target
}

func cloneWireMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var cloned map[string]any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}

func engineWithSuccess(t *testing.T, response map[string]any) CliEngine {
	return engineWithSuccessRequest(t, response, nil)
}

func engineWithSuccessRequest(t *testing.T, response map[string]any, echoedRequest any) CliEngine {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "engine")
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable+".response", encodedResponse, 0o600); err != nil {
		t.Fatal(err)
	}
	requestSource := "request=${payload#*\\\"request\\\":}\nrequest=${request%?}\n"
	if echoedRequest != nil {
		encodedRequest, err := json.Marshal(echoedRequest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(executable+".request", encodedRequest, 0o600); err != nil {
			t.Fatal(err)
		}
		requestSource = "request=$(/bin/cat \"$0.request\")\n"
	}
	script := []byte("#!/bin/sh\npayload=$(/bin/cat)\n" + requestSource +
		"printf '%s' '{\"outcome\":\"success\",\"engine_protocol\":\"" + EngineProtocolVersion + "\",\"request\":'\n" +
		"printf '%s' \"$request\"\n" +
		"printf '%s' ',\"response\":'\n" +
		"/bin/cat \"$0.response\"\n" +
		"printf '%s' '}'\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable}
}

func engineWithSealAndLiveSuccesses(t *testing.T, seal, live map[string]any) CliEngine {
	t.Helper()
	directory := t.TempDir()
	executable := filepath.Join(directory, "engine")
	for name, response := range map[string]map[string]any{"seal": seal, "live": live} {
		encodedResponse, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(executable+"."+name+".response", encodedResponse, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	script := []byte("#!/bin/sh\n" +
		"payload=$(/bin/cat)\n" +
		"case \"$payload\" in\n" +
		"  *'\"type\":\"seal\"'*) response=\"$0.seal.response\" ;;\n" +
		"  *'\"type\":\"execute_live_evolution\"'*) response=\"$0.live.response\" ;;\n" +
		"  *) exit 64 ;;\n" +
		"esac\n" +
		"request=${payload#*\\\"request\\\":}\n" +
		"request=${request%?}\n" +
		"printf '%s' '{\"outcome\":\"success\",\"engine_protocol\":\"" + EngineProtocolVersion + "\",\"request\":'\n" +
		"printf '%s' \"$request\"\n" +
		"printf '%s' ',\"response\":'\n" +
		"/bin/cat \"$response\"\n" +
		"printf '%s' '}'\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable}
}

func engineWithOversizedOutput(t *testing.T, stream string) CliEngine {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "engine")
	redirect := ""
	if stream == "stderr" {
		redirect = " 1>&2"
	}
	script := []byte("#!/bin/sh\n/bin/cat >/dev/null\n" +
		"dd if=/dev/zero bs=1048576 count=17" + redirect + " 2>/dev/null\n" +
		"exit 0\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable}
}

func blockingEngine(t *testing.T, ctx context.Context) (CliEngine, string) {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "engine")
	script := []byte("#!/bin/sh\n/bin/cat >/dev/null\n: > \"$0.started\"\n/bin/sleep 10\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable, Context: ctx}, executable + ".started"
}

func termIgnoringDescendantEngine(t *testing.T, ctx context.Context) (CliEngine, string, string, string) {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "engine")
	script := []byte("#!/bin/sh\n" +
		"trap '' TERM\n" +
		"/bin/cat >/dev/null\n" +
		": > \"$0.started\"\n" +
		"printf '%s\\n' \"$$\" > \"$0.pgid\"\n" +
		"(\n" +
		"  trap '' TERM\n" +
		"  /bin/sleep 0.6\n" +
		"  printf late > \"$0.late\"\n" +
		"  /bin/sleep 10\n" +
		") &\n" +
		"wait\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable, Context: ctx},
		executable + ".started", executable + ".pgid", executable + ".late"
}

func successfulEngineWithLingeringDescendant(t *testing.T, response map[string]any) (CliEngine, string, string) {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "engine")
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable+".response", encodedResponse, 0o600); err != nil {
		t.Fatal(err)
	}
	script := []byte("#!/bin/sh\n" +
		"payload=$(/bin/cat)\n" +
		"printf '%s\\n' \"$$\" > \"$0.pgid\"\n" +
		"(\n" +
		"  trap '' TERM HUP\n" +
		"  /bin/sleep 0.7\n" +
		"  printf late > \"$0.late\"\n" +
		"  /bin/sleep 10\n" +
		") >/dev/null 2>&1 &\n" +
		"request=${payload#*\\\"request\\\":}\n" +
		"request=${request%?}\n" +
		"printf '%s' '{\"outcome\":\"success\",\"engine_protocol\":\"" + EngineProtocolVersion + "\",\"request\":'\n" +
		"printf '%s' \"$request\"\n" +
		"printf '%s' ',\"response\":'\n" +
		"/bin/cat \"$0.response\"\n" +
		"printf '%s' '}'\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	return CliEngine{Executable: executable}, executable + ".pgid", executable + ".late"
}

func readProcessGroupID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		encoded, err := os.ReadFile(path)
		if err == nil {
			processGroupID, parseErr := strconv.Atoi(strings.TrimSpace(string(encoded)))
			if parseErr != nil {
				t.Fatalf("invalid process group marker: %v", parseErr)
			}
			return processGroupID
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("Engine process group marker was not created")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForEngineStart(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Engine process did not report startup")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type controlledDeadlineContext struct {
	done <-chan struct{}
}

func (ctx controlledDeadlineContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx controlledDeadlineContext) Done() <-chan struct{}       { return ctx.done }
func (ctx controlledDeadlineContext) Err() error {
	select {
	case <-ctx.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
func (ctx controlledDeadlineContext) Value(any) any { return nil }

func requireFailure(t *testing.T, err error, category, code, retry string) {
	t.Helper()
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Category != category || failure.Code != code || failure.RetryDisposition != retry {
		t.Fatalf("expected %s/%s/%s, got %#v (%v)", category, code, retry, failure, err)
	}
}

func fixedDurableRun() map[string]any {
	input := fixedArtifact("1", "cymule.input/1")
	return map[string]any{
		"revision": "revision:test",
		"continuation": map[string]any{
			"continuation_version": "cymule.continuation-state/1",
			"run_id":               "run:test", "plan_id": "sha256:" + strings.Repeat("2", 64),
			"binding_context": "sha256:" + strings.Repeat("3", 64),
			"frames": []any{map[string]any{
				"definition_id": "main", "invocation_id": "invocation:test",
				"invocation_path": []any{}, "scope_id": "scope:root", "input": input,
				"region_path": []any{}, "next_step": 0, "locals": map[string]any{},
			}},
			"state": nil, "wait_set": []any{}, "scope_stack": []any{"scope:root"},
			"epoch": 0, "execution_fence": 0, "execution_claim": nil, "status": "ready",
		},
		"waits": []any{}, "effects": []any{}, "component_occurrences": []any{},
		"operation_attempts": []any{}, "result": nil,
		"execution_status": map[string]any{"status": "active"}, "world_settlement": "settled",
	}
}

func fixedDurableRunCurrent(runID string) map[string]any {
	return map[string]any{
		"run_id": runID, "plan_id": fixedContentID("2"),
		"execution_binding":   fixedArtifact("3", "cymule.execution-binding/2"),
		"continuation_status": "ready", "epoch": 0, "execution_fence": 0,
		"result": nil, "execution_status": map[string]any{"status": "active"},
		"world_settlement": "settled",
	}
}

func fixedLiveEvolutionOutcomes(t *testing.T) map[string]map[string]any {
	t.Helper()
	artifact := fixedArtifact("4", "cymule.evolution-evidence/1")
	binding := fixedArtifact("5", "cymule.execution-binding/2")
	sourcePlanID := fixedContentID("b")
	targetPlanID := fixedPlan().PlanID
	revisionID := fixedContentID("0")
	edgeID := fixedContentID("c")
	compatibilityID := fixedContentID("d")
	sourceDecisionID := fixedContentID("6")
	targetDecisionID := fixedContentID("7")
	plan := map[string]any{"plan_id": fixedPlan().PlanID, "candidate": fixedCandidate()}
	revision := map[string]any{
		"revision_version": "cymule.subflow-revision/2", "revision_id": revisionID,
		"logical_ref": "definition:test", "sequence": 1,
		"definition": fixedCandidate().Definitions[0], "references": []any{},
	}
	frame := map[string]any{
		"definition_id": "main", "invocation_id": "invocation:test", "invocation_path": []any{},
		"scope_id": "scope:root", "input": artifact, "region_path": []any{},
		"next_step": 0, "locals": map[string]any{},
	}
	sourceContinuation := map[string]any{
		"continuation_version": "cymule.continuation-state/1",
		"run_id":               "run:test", "plan_id": sourcePlanID, "binding_context": binding["artifact_id"],
		"frames": []any{frame}, "state": artifact, "wait_set": []any{},
		"scope_stack": []any{"scope:root"}, "epoch": 0, "execution_fence": 3,
		"execution_claim": nil, "status": "ready",
	}
	targetContinuation := cloneWireMap(t, sourceContinuation)
	targetContinuation["plan_id"] = targetPlanID
	targetContinuation["epoch"] = 1
	migrationRequest := map[string]any{
		"migration_id": "migration:test", "run_id": "run:test", "from_plan": sourcePlanID,
		"to_plan": targetPlanID, "plan_edge_id": edgeID, "compatibility_id": compatibilityID,
		"expected_source_epoch": 0, "adapter_id": "adapter:test",
		"adapter_revision": fixedContentID("a"),
	}
	migrationReceipt := map[string]any{
		"request": migrationRequest, "source_witness_id": fixedContentID("e"),
		"source_binding": binding, "target_binding": binding, "source_execution_fence": 3,
		"target_epoch": 1, "adapter_id": "adapter:test",
		"adapter_revision": fixedContentID("a"), "from_schema": "schema:source",
		"to_schema": "schema:target", "output_state": artifact,
		"target_continuation": targetContinuation, "evidence": artifact,
	}
	gate := map[string]any{
		"gate_id": "gate:test", "decision_id": sourceDecisionID, "min_target_observations": 1,
		"max_target_failures": 0, "min_equivalent_shadows": 0, "max_inequivalent_shadows": 0,
	}
	return map[string]map[string]any{
		"definition_published": {"result": "definition_published", "revision": revision},
		"template_registered": {"result": "template_registered", "linked": map[string]any{
			"template_id": "template:test", "plan": plan, "resolved_revisions": map[string]any{"definition:test": revisionID},
		}},
		"publication_applied": {"result": "publication_applied", "receipt": map[string]any{
			"revision": revision, "updates": []any{map[string]any{
				"template_id": "template:test", "previous_plan_id": sourcePlanID,
				"current_plan_id": targetPlanID, "decision_id": targetDecisionID, "advanced": true,
			}},
		}},
		"patch_applied": {"result": "patch_applied", "edge": map[string]any{
			"edge_id": edgeID, "from_plan": sourcePlanID, "to_plan": targetPlanID,
			"operations": []any{map[string]any{"kind": "replace", "target": "definition:main", "before": fixedDigest("1"), "after": fixedDigest("2")}},
		}},
		"applied": {"result": "applied"},
		"occurrence_selected": {"result": "occurrence_selected", "pin": map[string]any{
			"occurrence_id": "occurrence:test", "template_id": "template:test", "decision_id": "decision:test",
			"plan_id": targetPlanID, "execution_binding": binding, "selection_id": "selection:test",
		}},
		"migrated": {"result": "migrated", "receipt": migrationReceipt},
		"restart_authorized": {"result": "restart_authorized", "receipt": map[string]any{
			"request": map[string]any{
				"restart_id": "restart:test", "replacement_run": "run:target", "run_id": "run:test",
				"from_plan": sourcePlanID, "expected_source_epoch": 0, "to_plan": targetPlanID,
				"input": artifact, "evidence": artifact,
			},
			"source_witness_id": fixedContentID("f"), "target_plan": plan,
		}},
		"shadow_recorded": {"result": "shadow_recorded", "comparison": map[string]any{
			"comparison_id": "comparison:test", "subject": "run:test", "decision_id": "decision:test",
			"primary_plan": sourcePlanID, "shadow_plan": targetPlanID, "driver_id": "driver:test",
			"driver_revision": fixedContentID("a"), "comparison_policy": "policy:test",
			"primary_digest": fixedDigest("a"), "shadow_digest": fixedDigest("b"),
			"equivalent": true, "evidence": artifact,
		}},
		"gate_applied": {"result": "gate_applied", "transition": map[string]any{
			"transition_id": fixedContentID("8"), "from_decision": sourceDecisionID, "to_decision": targetDecisionID,
			"evaluation": map[string]any{
				"evaluation_id": fixedContentID("9"), "gate": gate, "target_observations": 1,
				"target_failures": 0, "equivalent_shadows": 0, "inequivalent_shadows": 0,
				"outcome": "promote", "evidence_ids": []any{"observation:test"},
			},
		}},
	}
}

func liveEvolutionExecuted(evolutionID string, command LiveEvolutionCommand, outcome map[string]any) map[string]any {
	var sourceWitness any
	if command.Operation == "apply" && command.Command != nil &&
		(command.Command.Operation == "migrate" || command.Command.Operation == "restart_under_new_plan") {
		sourceWitness = fixedContentID("5")
	}
	return map[string]any{
		"type": "live_evolution_executed",
		"commit": map[string]any{
			"observed_revision":  fixedContentID("8"),
			"committed_revision": fixedContentID("8"),
			"receipt": map[string]any{
				"receipt_version": "cymule.evolution-persistence-receipt/4",
				"receipt_id":      fixedContentID("7"),
				"command": map[string]any{
					"persistence_version": "cymule.evolution-persistence-command/4",
					"persistence_id":      fixedContentID("6"),
					"evolution_id":        evolutionID,
					"command":             command,
				},
				"parent_current_id": nil,
				"source_witness_id": sourceWitness,
				"outcome":           outcome,
				"mutations":         []any{},
				"mutation_id":       fixedContentID("4"),
			},
		},
	}
}

type changingJSONMarshaler struct {
	calls *int
}

func (value changingJSONMarshaler) MarshalJSON() ([]byte, error) {
	(*value.calls)++
	if *value.calls == 1 {
		return []byte(`{"value":"first"}`), nil
	}
	return []byte(`{"value":"changed"}`), nil
}

type invalidTextMarshaler struct {
	calls *int
}

func (value invalidTextMarshaler) MarshalText() ([]byte, error) {
	(*value.calls)++
	return []byte{0xff}, nil
}

type invalidTextMapKey int

func (invalidTextMapKey) MarshalText() ([]byte, error) {
	return []byte{0xff}, nil
}

func TestDurableControlsAcceptMaximumExactInteger(t *testing.T) {
	execution := fixtureExecution()
	execution.TTL = 9_007_199_254_740_991
	command, err := TakeoverDurableRun("run:max-safe-fence", 9_007_199_254_740_991, execution)
	if err != nil || command.ExpectedFence != 9_007_199_254_740_991 {
		t.Fatalf("maximum exact fence was rejected: %#v %v", command, err)
	}
	accepted := fixtureExecution()
	accepted.Owner = strings.Repeat("é", 512)
	if _, err := TakeoverDurableRun("run:unicode-owner", 1, accepted); err != nil {
		t.Fatalf("512 Unicode scalar owner was rejected: %v", err)
	}
	for _, owner := range []string{"driver:\u0085forged", strings.Repeat("é", 513), string([]byte{0xff})} {
		rejected := fixtureExecution()
		rejected.Owner = owner
		if _, err := TakeoverDurableRun("run:invalid-owner", 1, rejected); err == nil {
			t.Fatalf("invalid owner %q was accepted", owner)
		}
	}
}

func TestRunIdentityUsesUnicodeScalarBoundaries(t *testing.T) {
	maximum := strings.Repeat("界", 512)
	if !validRunIdentity(maximum) {
		t.Fatal("512-scalar multibyte Run identity was rejected")
	}
	if validRunIdentity(strings.Repeat("界", 513)) || validRunIdentity("run:\u0085forged") ||
		validRunIdentity(string([]byte{0xff})) {
		t.Fatal("invalid Run identity was accepted")
	}
	if _, err := StartDurableRun(maximum, fixedCandidate(), nil, fixtureExecution()); err != nil {
		t.Fatalf("maximum multibyte Run identity was rejected by durable start: %v", err)
	}
	for _, invalid := range []string{strings.Repeat("界", 513), "run:\u0085forged", string([]byte{0xff})} {
		if _, err := StartDurableRun(invalid, fixedCandidate(), nil, fixtureExecution()); err == nil {
			t.Fatalf("invalid durable start Run identity %q was accepted", invalid)
		}
		if _, err := CancelDurableRun("cancel:test", invalid, nil); err == nil {
			t.Fatalf("invalid durable cancellation Run identity %q was accepted", invalid)
		}
	}

	response := map[string]any{
		"type": "durable_executed",
		"response": map[string]any{
			"type": "run_current", "observed_revision": fixedContentID("a"),
			"source_root": fixedDigest("b"), "current": nil,
		},
	}
	if _, err := (DurableEngine{
		Store: DirectoryStore("unused"), Transport: engineWithSuccess(t, response),
	}).RunCurrent(maximum, nil); err != nil {
		t.Fatalf("maximum Run identity was rejected by RunCurrent: %v", err)
	}

	current := fixedDurableRunCurrent(maximum)
	if err := validateDurableRunCurrentRaw(mustRawJSON(t, current)); err != nil {
		t.Fatalf("maximum multibyte Run identity was rejected in Run-current: %v", err)
	}

	migrated := fixedLiveEvolutionOutcomes(t)["migrated"]
	receipt := migrated["receipt"].(map[string]any)
	request := receipt["request"].(map[string]any)
	request["run_id"] = maximum
	receipt["target_continuation"].(map[string]any)["run_id"] = maximum
	var migrationOutcome LiveEvolutionOutcome
	if err := decodeClosedJSON(mustRawJSON(t, migrated), &migrationOutcome); err != nil {
		t.Fatalf("maximum multibyte Run identity was rejected in M4 migration: %v", err)
	}
}

func TestEvolutionIdentityUsesUnicodeScalarBoundaries(t *testing.T) {
	maximum := strings.Repeat("🚀", 256)
	if !validEvolutionIdentity(maximum) {
		t.Fatal("256-scalar multibyte Evolution identity was rejected")
	}
	for _, invalid := range []string{
		strings.Repeat("🚀", 257), "command:\u0085forged", string([]byte{0xff}),
	} {
		if validEvolutionIdentity(invalid) {
			t.Fatalf("invalid Evolution identity %q was accepted", invalid)
		}
	}
	binding := ArtifactRef{
		IdentityVersion: "cymule.artifact/2",
		ArtifactID:      fixedContentID("1"),
		Kind:            "cymule.execution-binding/2",
	}
	command := SelectEvolutionOccurrence(maximum, "occurrence:test", "selection:test", binding)
	if err := validateEvolutionCommandSemantics(command); err != nil {
		t.Fatalf("maximum multibyte Evolution command identity was rejected: %v", err)
	}
	command.CommandID = strings.Repeat("🚀", 257)
	if err := validateEvolutionCommandSemantics(command); err == nil {
		t.Fatal("oversized Evolution command identity was accepted")
	}
	live := PublishLiveDefinition(maximum, "definition:test", fixedCandidate().Definitions[0], []SubflowReference{})
	if err := validateLiveEvolutionCommandSemantics(live); err != nil {
		t.Fatalf("maximum multibyte live-evolution identity was rejected: %v", err)
	}
	live.CommandID = strings.Repeat("🚀", 257)
	if err := validateLiveEvolutionCommandSemantics(live); err == nil {
		t.Fatal("oversized live-evolution identity was accepted")
	}
}

func TestContinuationIdentityIsContentAddressedNotRunIDConcatenation(t *testing.T) {
	runID := strings.Repeat("界", 512)
	claim := ContinuationExecutionClaim{
		ClaimVersion:          "cymule.continuation-execution-claim/1",
		RunID:                 runID,
		ContinuationID:        fixedContentID("1"),
		Owner:                 "driver:test",
		ContinuationAttemptID: fixedContentID("2"),
		Fence:                 1,
		PlanID:                fixedContentID("3"),
		ExecutionBindingRef: ArtifactRef{
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      fixedContentID("4"),
			Kind:            "cymule.execution-binding/2",
		},
		ClockObservationRef: fixtureExecution().Clock,
		LogicalAcquiredAt:   1,
		LogicalExpiresAt:    2,
		LogicalTTL:          1,
	}
	if err := validateContinuationExecutionClaim(claim); err != nil {
		t.Fatalf("content-addressed Continuation identity was rejected: %v", err)
	}
	legacyAttempt := claim
	legacyAttempt.ContinuationAttemptID = "attempt:legacy"
	if err := validateContinuationExecutionClaim(legacyAttempt); err == nil {
		t.Fatal("non-content Continuation Attempt identity was accepted")
	}
	invalidOwner := claim
	invalidOwner.Owner = "driver:\u0085legacy"
	if err := validateContinuationExecutionClaim(invalidOwner); err == nil {
		t.Fatal("control-bearing Continuation claim owner was accepted")
	}
	legacyPlan := claim
	legacyPlan.PlanID = "plan:legacy"
	if err := validateContinuationExecutionClaim(legacyPlan); err == nil {
		t.Fatal("non-content Continuation claim Plan identity was accepted")
	}
	claim.ContinuationID = "continuation:" + runID
	if err := validateContinuationExecutionClaim(claim); err == nil {
		t.Fatal("caller Run identity concatenation was accepted as a Continuation identity")
	}
}

func TestCliEnginePreservesPreCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (CliEngine{Context: ctx}).Seal(NewFlow("cancel", map[string]any{}, map[string]any{}).
		Finish(Expression{"kind": "input"}))
	requireFailure(t, err, "cancelled", "engine_response_cancelled", "never")

	_, err = (CliEngine{Context: ctx}).ObserveClock(
		SQLiteClock("unused", "clock:pre-cancel", fixedContentID("1")), "run:pre-cancel",
	)
	requireFailure(t, err, "cancelled", "engine_response_cancelled", "never")
}

func TestCliEngineTimeoutSelectionIsFiniteAndContextBound(t *testing.T) {
	t.Run("zero value keeps the caller Context deadline authoritative", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		engine, _ := blockingEngine(t, ctx)
		startedAt := time.Now()
		_, err := engine.Seal(fixedCandidate())
		requireFailure(t, err, "timed_out", "engine_response_timed_out", "retry_same_request")
		if time.Since(startedAt) > 2*time.Second {
			t.Fatal("zero-value Engine ignored the caller Context deadline")
		}
	})

	t.Run("positive transport timeout overrides the default", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		engine, _ := blockingEngine(t, ctx)
		engine.Timeout = 200 * time.Millisecond
		startedAt := time.Now()
		_, err := engine.Seal(fixedCandidate())
		requireFailure(t, err, "timed_out", "engine_response_timed_out", "retry_same_request")
		if time.Since(startedAt) > time.Second {
			t.Fatal("explicit Engine timeout did not override the 30-second default")
		}
	})

	_, err := (CliEngine{Executable: "missing", Timeout: -time.Second}).Seal(fixedCandidate())
	requireFailure(t, err, "validation", "invalid_engine_timeout", "correct_and_retry")
}

func TestCliEngineClassifiesPostStartInterruptionByMutation(t *testing.T) {
	clockTarget := SQLiteClock("unused", "clock:interruption", fixedContentID("1"))
	tests := []struct {
		name        string
		deadline    bool
		invoke      func(CliEngine) error
		category    string
		code        string
		disposition string
	}{
		{
			name: "read cancellation",
			invoke: func(engine CliEngine) error {
				_, err := engine.Seal(fixedCandidate())
				return err
			},
			category: "cancelled", code: "engine_response_cancelled", disposition: "never",
		},
		{
			name: "read timeout", deadline: true,
			invoke: func(engine CliEngine) error {
				_, err := engine.Seal(fixedCandidate())
				return err
			},
			category: "timed_out", code: "engine_response_timed_out", disposition: "retry_same_request",
		},
		{
			name: "mutating cancellation",
			invoke: func(engine CliEngine) error {
				_, err := engine.ObserveClock(clockTarget, "run:cancelled")
				return err
			},
			category: "unknown_world_outcome", code: "engine_response_cancelled", disposition: "reconcile",
		},
		{
			name: "mutating timeout", deadline: true,
			invoke: func(engine CliEngine) error {
				_, err := engine.ObserveClock(clockTarget, "run:timed-out")
				return err
			},
			category: "unknown_world_outcome", code: "engine_response_timed_out", disposition: "reconcile",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			interrupted := make(chan struct{})
			var ctx context.Context
			var interrupt func()
			if testCase.deadline {
				ctx = controlledDeadlineContext{done: interrupted}
				interrupt = func() { close(interrupted) }
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				interrupt = cancel
			}
			engine, marker := blockingEngine(t, ctx)
			result := make(chan error, 1)
			go func() { result <- testCase.invoke(engine) }()
			waitForEngineStart(t, marker)
			interrupt()
			select {
			case err := <-result:
				requireFailure(t, err, testCase.category, testCase.code, testCase.disposition)
			case <-time.After(3 * time.Second):
				t.Fatal("interrupted Engine process did not terminate")
			}
		})
	}
}

func TestCliEngineTerminatesTermIgnoringProcessGroupsBeforeReturning(t *testing.T) {
	tests := []struct {
		name        string
		deadline    bool
		category    string
		disposition string
	}{
		{name: "cancellation", category: "cancelled", disposition: "never"},
		{name: "timeout", deadline: true, category: "timed_out", disposition: "retry_same_request"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var ctx context.Context
			trigger := func() {}
			if testCase.deadline {
				deadline := make(chan struct{})
				ctx = controlledDeadlineContext{done: deadline}
				trigger = func() { close(deadline) }
			} else {
				var cancelContext context.CancelFunc
				ctx, cancelContext = context.WithCancel(context.Background())
				trigger = cancelContext
				defer cancelContext()
			}
			engine, started, processGroupMarker, lateMarker := termIgnoringDescendantEngine(
				t, ctx,
			)
			result := make(chan error, 1)
			go func() {
				_, err := engine.Seal(fixedCandidate())
				result <- err
			}()
			waitForEngineStart(t, started)
			processGroupID := readProcessGroupID(t, processGroupMarker)
			trigger()
			select {
			case err := <-result:
				requireFailure(
					t, err, testCase.category, "engine_response_"+testCase.category,
					testCase.disposition,
				)
			case <-time.After(3 * time.Second):
				t.Fatal("Engine process group termination did not complete")
			}
			exists, err := engineProcessGroupExists(processGroupID)
			if err != nil {
				t.Fatal(err)
			}
			if exists {
				t.Fatal("Engine API returned while its process group was still alive")
			}
			time.Sleep(700 * time.Millisecond)
			if _, err := os.Stat(lateMarker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("terminated Engine descendant produced a late side effect: %v", err)
			}
		})
	}
}

func TestCliEngineRejectsNaturalExitWithLingeringProcessGroup(t *testing.T) {
	clockTarget := SQLiteClock("unused", "clock:lingering", fixedContentID("8"))
	tests := []struct {
		name     string
		response map[string]any
		invoke   func(CliEngine) error
		category string
		retry    string
	}{
		{
			name: "read",
			response: map[string]any{
				"type": "sealed", "plan": fixedPlan(),
			},
			invoke: func(engine CliEngine) error {
				_, err := engine.Seal(fixedCandidate())
				return err
			},
			category: "transport_failure",
		},
		{
			name: "mutation",
			response: map[string]any{
				"type": "clock_observed",
				"result": ClockObservationResult{
					RunID: "run:lingering",
					Observation: ClockObservationRef{
						ClockVersion: "cymule.clock-observation/2", ObservationID: fixedContentID("7"),
						SourceID: clockTarget.SourceID, SourceGeneration: clockTarget.SourceGeneration,
						Scope: "run:lingering",
					},
				},
			},
			invoke: func(engine CliEngine) error {
				_, err := engine.ObserveClock(clockTarget, "run:lingering")
				return err
			},
			category: "unknown_world_outcome", retry: "reconcile",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			engine, processGroupMarker, lateMarker := successfulEngineWithLingeringDescendant(t, testCase.response)
			err := testCase.invoke(engine)
			requireFailure(t, err, testCase.category, "engine_process_group_leaked", testCase.retry)
			processGroupID := readProcessGroupID(t, processGroupMarker)
			exists, groupErr := engineProcessGroupExists(processGroupID)
			if groupErr != nil {
				t.Fatal(groupErr)
			}
			if exists {
				t.Fatal("Engine API returned while a natural-exit process group remained alive")
			}
			time.Sleep(800 * time.Millisecond)
			if _, statErr := os.Stat(lateMarker); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("residual Engine descendant produced a late side effect: %v", statErr)
			}
		})
	}
}

func TestAwaitEngineProcessPrefersAnAlreadyCompletedNaturalExit(t *testing.T) {
	waitDone := make(chan error, 1)
	waitDone <- nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitErr, interrupted, residualGroup, terminationErr := awaitEngineProcess(ctx, 999_999, waitDone)
	if waitErr != nil || interrupted || residualGroup || terminationErr != nil {
		t.Fatalf(
			"completed Engine process was reclassified: wait=%v interrupted=%t residual=%t termination=%v",
			waitErr, interrupted, residualGroup, terminationErr,
		)
	}
}

func TestDurableEffectDispatchValidationIsTyped(t *testing.T) {
	artifact := map[string]any{
		"identity_version": "cymule.artifact/2",
		"artifact_id":      fixedContentID("1"),
		"kind":             "test/value",
	}
	binding := map[string]any{
		"identity_version": "cymule.artifact/2",
		"artifact_id":      fixedContentID("2"),
		"kind":             "cymule.execution-binding/2",
	}
	effect := map[string]any{
		"intent_id": fixedContentID("4"), "run_id": "run:test",
		"origin_plan_id": fixedContentID("3"), "operation": "publish",
		"input": artifact, "execution_binding": binding,
		"occurrence_binding": fixedContentID("5"), "execution_availability": "available",
		"reconciliation": "not_required", "state": "pending",
		"claim_epoch": 0, "claim_owner": nil, "result": nil,
	}
	if owner, err := validateDurableEffectRaw(mustRawJSON(t, effect)); err != nil || owner != "run:test" {
		t.Fatalf("valid typed Effect leaf rejected: owner=%q err=%v", owner, err)
	}
	for field, invalid := range map[string]any{
		"execution_binding": artifact, "occurrence_binding": "binding:not-content",
		"execution_availability": "future", "state": "future", "reconciliation": "future",
	} {
		changed := cloneWireMap(t, effect)
		changed[field] = invalid
		if _, err := validateDurableEffectRaw(mustRawJSON(t, changed)); err == nil {
			t.Fatalf("invalid Effect %s was accepted", field)
		}
	}
	for name, mutate := range map[string]func(map[string]any){
		"pending governance":    func(value map[string]any) { value["reconciliation"] = "governance_required" },
		"claimed without fence": func(value map[string]any) { value["state"] = "claimed" },
		"invalid claim owner": func(value map[string]any) {
			value["state"], value["claim_epoch"], value["claim_owner"] = "claimed", 1, "driver:\u0085legacy"
		},
		"unavailable pending": func(value map[string]any) { value["execution_availability"] = "unavailable" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := cloneWireMap(t, effect)
			mutate(changed)
			if _, err := validateDurableEffectRaw(mustRawJSON(t, changed)); err == nil {
				t.Fatal("inconsistent Effect lifecycle was accepted")
			}
		})
	}
}

func TestCliEngineClassifiesMutatingResponseLossAsUnknown(t *testing.T) {
	engine := CliEngine{Executable: filepath.Join("..", "..", "tests", "fixtures", "response-loss-engine")}
	command := ResumeDurableRun("run:response-loss", fixtureExecution())
	_, err := engine.ExecuteDurable(
		testDurableTargetForCommand(t, command), command,
	)
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Category != "unknown_world_outcome" || failure.RetryDisposition != "reconcile" {
		t.Fatalf("expected unknown-world reconciliation, got %v", err)
	}
	_, err = engine.ObserveClock(
		SQLiteClock("/tmp/cymule-response-loss-clock", "clock:response-loss", "sha256:"+strings.Repeat("4", 64)),
		"run:response-loss",
	)
	if !errors.As(err, &failure) || failure.Category != "unknown_world_outcome" || failure.RetryDisposition != "reconcile" {
		t.Fatalf("expected Clock response-loss reconciliation, got %v", err)
	}
}

func TestCliEngineBoundsAndDrainsBothProcessStreams(t *testing.T) {
	target := SQLiteClock("unused", "clock:output-limit", fixedContentID("4"))
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			_, err := engineWithOversizedOutput(t, stream).ObserveClock(target, "run:output-limit")
			requireFailure(t, err, "unknown_world_outcome", "engine_output_limit_exceeded", "reconcile")
		})
	}
}

func TestEngineSuccessRequiresOperationPayloads(t *testing.T) {
	candidate := fixedCandidate()
	plan := fixedPlan()
	readOnlyCases := []struct {
		name     string
		response map[string]any
		invoke   func(CliEngine) error
	}{
		{
			name: "missing sealed Plan", response: map[string]any{"type": "sealed"},
			invoke: func(engine CliEngine) error { _, err := engine.Seal(candidate); return err },
		},
		{
			name: "malformed sealed Plan", response: map[string]any{"type": "sealed", "plan": map[string]any{}},
			invoke: func(engine CliEngine) error { _, err := engine.Seal(candidate); return err },
		},
		{
			name: "missing verified evolution command", response: map[string]any{"type": "verified_evolution_command"},
			invoke: func(engine CliEngine) error {
				_, err := engine.VerifyEvolutionCommand(ApplyEvolutionGate("command:test", RolloutGate{}, "decision:test"))
				return err
			},
		},
		{
			name: "missing verified live command", response: map[string]any{"type": "verified_live_evolution_command"},
			invoke: func(engine CliEngine) error {
				_, err := engine.VerifyLiveEvolutionCommand(PublishLiveDefinition("command:test", "definition:test", candidate.Definitions[0], []SubflowReference{}))
				return err
			},
		},
	}
	for _, testCase := range readOnlyCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireFailure(t, testCase.invoke(engineWithSuccess(t, testCase.response)), "transport_failure", "invalid_engine_response", "")
		})
	}

	mutationCases := []struct {
		name     string
		response map[string]any
		invoke   func(CliEngine) error
	}{
		{
			name: "missing execution", response: map[string]any{"type": "execution_boundary"},
			invoke: func(engine CliEngine) error {
				_, err := engine.Run(plan, nil, testProcessPlugin(t, "/bin/true"), "run:test")
				return err
			},
		},
		{
			name: "malformed execution", response: map[string]any{"type": "execution_boundary", "execution": map[string]any{"status": "completed", "result": map[string]any{}}},
			invoke: func(engine CliEngine) error {
				_, err := engine.Run(plan, nil, testProcessPlugin(t, "/bin/true"), "run:test")
				return err
			},
		},
		{
			name: "missing Clock observation", response: map[string]any{"type": "clock_observed"},
			invoke: func(engine CliEngine) error {
				_, err := engine.ObserveClock(SQLiteClock("unused", "clock:test", "sha256:"+strings.Repeat("8", 64)), "run:test")
				return err
			},
		},
		{
			name: "malformed Clock observation", response: map[string]any{
				"type": "clock_observed", "result": map[string]any{
					"run_id": "run:test",
					"observation": map[string]any{
						"clock_version":  "cymule.clock-observation/1",
						"observation_id": "sha256:" + strings.Repeat("1", 64),
						"source_id":      "clock:test", "source_generation": "sha256:" + strings.Repeat("2", 64),
						"scope": "run:test",
					},
				},
			},
			invoke: func(engine CliEngine) error {
				_, err := engine.ObserveClock(SQLiteClock("unused", "clock:test", "sha256:"+strings.Repeat("8", 64)), "run:test")
				return err
			},
		},
		{
			name: "missing durable response", response: map[string]any{"type": "durable_executed"},
			invoke: func(engine CliEngine) error {
				command := ResumeDurableRun("run:test", fixtureExecution())
				_, err := engine.ExecuteDurable(testDurableTargetForCommand(t, command), command)
				return err
			},
		},
		{
			name: "missing live receipt", response: map[string]any{"type": "live_evolution_executed"},
			invoke: func(engine CliEngine) error {
				_, err := engine.ExecuteLiveEvolution(EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}, "journal:test", PublishLiveDefinition("command:test", "definition:test", candidate.Definitions[0], []SubflowReference{}))
				return err
			},
		},
	}
	for _, testCase := range mutationCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireFailure(t, testCase.invoke(engineWithSuccess(t, testCase.response)), "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestDurableClockRejectsTypedResultForAnotherRun(t *testing.T) {
	clock := SQLiteClock("unused", "clock:fake-run", fixedContentID("1"))
	response := map[string]any{
		"type": "clock_observed",
		"result": ClockObservationResult{
			RunID: "run:foreign",
			Observation: ClockObservationRef{
				ClockVersion: "cymule.clock-observation/2", ObservationID: fixedContentID("2"),
				SourceID: clock.SourceID, SourceGeneration: clock.SourceGeneration,
				Scope: fixedContentID("3"),
			},
		},
	}
	durable := DurableEngine{
		Store: DirectoryStore("unused"), Clock: &clock,
		Transport: engineWithSuccess(t, response),
	}
	_, err := durable.ObserveClock("run:expected")
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestEngineSuccessRequiresExactRequestEcho(t *testing.T) {
	sealResponse := map[string]any{"type": "sealed", "plan": fixedPlan()}
	wrongSealRequest := map[string]any{
		"type": "seal",
		"candidate": NewFlow("forged", map[string]any{}, map[string]any{}).
			Finish(Expression{"kind": "input"}),
	}
	_, err := engineWithSuccessRequest(t, sealResponse, wrongSealRequest).Seal(fixedCandidate())
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

	clockTarget := SQLiteClock("unused", "clock:test", fixedContentID("2"))
	clockResponse := map[string]any{
		"type": "clock_observed",
		"result": ClockObservationResult{
			RunID: "run:test",
			Observation: ClockObservationRef{
				ClockVersion: "cymule.clock-observation/2", ObservationID: fixedContentID("1"),
				SourceID: clockTarget.SourceID, SourceGeneration: clockTarget.SourceGeneration,
				Scope: fixedContentID("3"),
			},
		},
	}
	wrongClockRequest := map[string]any{
		"type": "observe_clock", "target": clockTarget, "run_id": "run:forged",
	}
	_, err = engineWithSuccessRequest(t, clockResponse, wrongClockRequest).
		ObserveClock(clockTarget, "run:test")
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	cancelCommand, err := CancelDurableRun("cancel:test", "run:test", map[string]any{"reason": "test"})
	if err != nil {
		t.Fatal(err)
	}
	forgedCancel, err := CancelDurableRun("cancel:forged", "run:test", map[string]any{"reason": "test"})
	if err != nil {
		t.Fatal(err)
	}
	durableTarget := EngineDurableTarget{Store: DirectoryStore("unused")}
	durableResponse := map[string]any{
		"type": "durable_executed",
		"response": map[string]any{
			"type": "run_index_page", "page": map[string]any{
				"observed_revision": fixedContentID("6"), "source_root": fixedDigest("7"),
				"items": []any{}, "next_cursor": nil,
			},
		},
	}
	wrongCancelRequest := map[string]any{
		"type": "execute_durable", "target": durableTarget, "command": forgedCancel,
	}
	_, err = engineWithSuccessRequest(t, durableResponse, wrongCancelRequest).
		ExecuteDurable(durableTarget, cancelCommand)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	outcome := fixedLiveEvolutionOutcomes(t)["definition_published"]
	liveCommand := PublishLiveDefinition("command:test", "definition:test", fixedCandidate().Definitions[0], []SubflowReference{})
	liveTarget := EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}
	liveResponse := liveEvolutionExecuted("journal:test", liveCommand, outcome)
	wrongLiveRequest := map[string]any{
		"type": "execute_live_evolution", "target": liveTarget,
		"evolution_id": "journal:forged", "command": liveCommand,
	}
	_, err = engineWithSuccessRequest(t, liveResponse, wrongLiveRequest).
		ExecuteLiveEvolution(liveTarget, "journal:test", liveCommand)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	missingNullableTargetMembers := map[string]any{
		"type": "execute_live_evolution",
		"target": map[string]any{
			"store": liveTarget.Store,
		},
		"evolution_id": "journal:test", "command": liveCommand,
	}
	_, err = engineWithSuccessRequest(t, liveResponse, missingNullableTargetMembers).
		ExecuteLiveEvolution(liveTarget, "journal:test", liveCommand)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestDurableReceiptsBindCancellationAndEffectResolutionCommands(t *testing.T) {
	reason := map[string]any{"code": "operator_request", "detail": nil}
	cancel, err := CancelDurableRun("cancel:receipt", "run:receipt", reason)
	if err != nil {
		t.Fatal(err)
	}
	cancelTarget := testDurableTargetForCommand(t, cancel)
	reasonRef := fixedArtifact("a", "cymule.cancellation-reason/1")
	cancelReceipt := map[string]any{
		"receipt_version": "cymule.run-cancellation-receipt/1",
		"command": map[string]any{
			"cancellation_id": cancel.CancellationID, "run_id": cancel.RunID, "reason": reason,
		},
		"boundary":   map[string]any{"status": "cancelled", "reason": reasonRef},
		"receipt_id": fixedDigest("9"),
	}
	cancelResponse := map[string]any{
		"type":     "durable_executed",
		"response": map[string]any{"type": "run_cancelled", "receipt": cancelReceipt},
	}
	if _, err := engineWithSuccess(t, cancelResponse).ExecuteDurable(cancelTarget, cancel); err != nil {
		t.Fatalf("matching cancellation receipt was rejected: %v", err)
	}
	forgedCancel := cloneWireMap(t, cancelResponse)
	forgedCancel["response"].(map[string]any)["receipt"].(map[string]any)["command"].(map[string]any)["reason"] = map[string]any{"code": "forged"}
	_, err = engineWithSuccess(t, forgedCancel).ExecuteDurable(cancelTarget, cancel)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	binding := ArtifactRef{
		IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("b"),
		Kind: "cymule.execution-binding/2",
	}
	resolution, err := ResolveDurableEffect(
		"resolution:receipt", "run:receipt", fixedContentID("c"), binding,
		fixedContentID("d"), "driver:receipt", 7, "resolved_applied", map[string]any{"accepted": true},
	)
	if err != nil {
		t.Fatal(err)
	}
	resolutionTarget := testDurableTargetForCommand(t, resolution)
	resolutionReceipt := map[string]any{
		"receipt_version": "cymule.effect-resolution-receipt/1",
		"command": map[string]any{
			"resolution_id": resolution.ResolutionID, "run_id": resolution.RunID,
			"intent_id": resolution.IntentID, "execution_binding": binding,
			"occurrence_binding": resolution.OccurrenceBinding,
			"claim_owner":        resolution.ClaimOwner, "claim_epoch": resolution.ClaimEpoch,
			"resolution": resolution.Resolution, "value": resolution.Value,
		},
		"actual_resolution": "resolved_not_applied", "actual_value": nil, "result": nil,
		"receipt_id": fixedDigest("8"),
	}
	resolutionResponse := map[string]any{
		"type":     "durable_executed",
		"response": map[string]any{"type": "effect_resolved", "receipt": resolutionReceipt},
	}
	if _, err := engineWithSuccess(t, resolutionResponse).ExecuteDurable(resolutionTarget, resolution); err != nil {
		t.Fatalf("matching effect resolution receipt was rejected: %v", err)
	}
	appliedNull := cloneWireMap(t, resolutionResponse)
	appliedReceipt := appliedNull["response"].(map[string]any)["receipt"].(map[string]any)
	appliedReceipt["actual_resolution"] = "resolved_applied"
	appliedReceipt["result"] = fixedArtifact("e", "cymule.effect-result/1")
	if _, err := engineWithSuccess(t, appliedNull).ExecuteDurable(resolutionTarget, resolution); err != nil {
		t.Fatalf("Applied JSON null with a result Artifact was rejected: %v", err)
	}
	appliedReceipt["result"] = nil
	_, err = engineWithSuccess(t, appliedNull).ExecuteDurable(resolutionTarget, resolution)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
	forgedResolution := cloneWireMap(t, resolutionResponse)
	forgedResolution["response"].(map[string]any)["receipt"].(map[string]any)["command"].(map[string]any)["claim_epoch"] = 8
	_, err = engineWithSuccess(t, forgedResolution).ExecuteDurable(resolutionTarget, resolution)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
	forgedValue := cloneWireMap(t, resolutionResponse)
	forgedValue["response"].(map[string]any)["receipt"].(map[string]any)["command"].(map[string]any)["value"] = map[string]any{"accepted": false}
	_, err = engineWithSuccess(t, forgedValue).ExecuteDurable(resolutionTarget, resolution)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
	forgedRequestedResolution := cloneWireMap(t, resolutionResponse)
	forgedCommand := forgedRequestedResolution["response"].(map[string]any)["receipt"].(map[string]any)["command"].(map[string]any)
	forgedCommand["resolution"], forgedCommand["value"] = "resolved_not_applied", nil
	_, err = engineWithSuccess(t, forgedRequestedResolution).ExecuteDurable(resolutionTarget, resolution)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
	forgedActualPair := cloneWireMap(t, resolutionResponse)
	forgedActualPair["response"].(map[string]any)["receipt"].(map[string]any)["result"] = fixedArtifact("e", "cymule.effect-result/1")
	_, err = engineWithSuccess(t, forgedActualPair).ExecuteDurable(resolutionTarget, resolution)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestDurableEffectCommandsRequireContentIntentIDs(t *testing.T) {
	validIntent := fixedContentID("c")
	if err := validateDurableCommandResponse(ReleaseDurableEffect(validIntent, fixtureExecution())); err != nil {
		t.Fatalf("content-addressed Effect release was rejected: %v", err)
	}
	if err := validateDurableCommandResponse(ReleaseDurableEffect("intent:legacy", fixtureExecution())); err == nil {
		t.Fatal("non-content Effect release identity was accepted")
	}
	binding := ArtifactRef{
		IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("d"),
		Kind: "cymule.execution-binding/2",
	}
	if _, err := ResolveDurableEffect(
		"resolution:test", "run:test", "intent:legacy", binding,
		fixedContentID("e"), "driver:test", 1, "resolved_not_applied", nil,
	); err == nil {
		t.Fatal("non-content Effect resolution identity was accepted")
	}
	if _, err := ResolveDurableEffect(
		"resolution:test", "run:test", validIntent, binding,
		"occurrence:not-content", "driver:test", 1, "resolved_not_applied", nil,
	); err == nil {
		t.Fatal("non-content Effect occurrence binding was accepted")
	}
}

func TestWaitActivationReceiptBindsTheSelectedDelivery(t *testing.T) {
	command, err := ActivateDurableSignal(
		"activation:receipt", "signal:receipt", []string{fixedContentID("b"), fixedContentID("a")},
		map[string]any{"accepted": true},
	)
	if err != nil {
		t.Fatal(err)
	}
	activation := SignalWaitActivation(
		command.ActivationID, command.Source.Key, command.WaitIDs,
		ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("7"), Kind: "cymule.wait-result/1"},
	)
	response := map[string]any{
		"type": "durable_executed", "response": map[string]any{
			"type": "wait_activated", "receipt": map[string]any{
				"receipt_version": "cymule.wait-activation-receipt/3", "activation": activation,
				"applied_wait_ids": []any{fixedContentID("a")}, "ready_run_ids": []any{"run:ready"},
			},
		},
	}
	target := testDurableTargetForCommand(t, command)
	if _, err := engineWithSuccess(t, response).ExecuteDurable(target, command); err != nil {
		t.Fatalf("matching wait activation receipt was rejected: %v", err)
	}
	forged := cloneWireMap(t, response)
	forged["response"].(map[string]any)["receipt"].(map[string]any)["activation"].(map[string]any)["activation_id"] = "activation:forged"
	_, err = engineWithSuccess(t, forged).ExecuteDurable(target, command)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
	unselected := cloneWireMap(t, response)
	unselected["response"].(map[string]any)["receipt"].(map[string]any)["applied_wait_ids"] = []any{fixedContentID("c")}
	_, err = engineWithSuccess(t, unselected).ExecuteDurable(target, command)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestVerifySuccessPayloadsBindTheExactRequest(t *testing.T) {
	artifact := ArtifactRef{
		IdentityVersion: "cymule.artifact/2",
		ArtifactID:      fixedContentID("1"),
		Kind:            "cymule.wait-result/1",
	}
	activation := SignalWaitActivation("activation:one", "signal:one", []string{fixedContentID("a")}, artifact)
	if returned, err := engineWithSuccess(t, map[string]any{
		"type": "verified_wait_activation", "activation": activation,
	}).VerifyWaitActivation(activation); err != nil || !reflect.DeepEqual(returned, activation) {
		t.Fatalf("matching wait activation was rejected: %#v %v", returned, err)
	}
	forgedActivation := activation
	forgedActivation.ActivationID = "activation:two"
	_, err := engineWithSuccess(t, map[string]any{
		"type": "verified_wait_activation", "activation": forgedActivation,
	}).VerifyWaitActivation(activation)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

	durable, err := QueryDurableRunCurrent("run:one", nil)
	if err != nil {
		t.Fatal(err)
	}
	if returned, err := engineWithSuccess(t, map[string]any{
		"type": "verified_durable_command", "command": durable,
	}).VerifyDurableCommand(durable); err != nil || !reflect.DeepEqual(returned, durable) {
		t.Fatalf("matching durable command was rejected: %#v %v", returned, err)
	}
	forgedDurable, err := QueryDurableRunCurrent("run:two", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engineWithSuccess(t, map[string]any{
		"type": "verified_durable_command", "command": forgedDurable,
	}).VerifyDurableCommand(durable)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

	binding := ArtifactRef{
		IdentityVersion: "cymule.artifact/2",
		ArtifactID:      fixedContentID("2"),
		Kind:            "cymule.execution-binding/2",
	}
	evolution := SelectEvolutionOccurrence("command:one", "occurrence:one", "selection:one", binding)
	if returned, err := engineWithSuccess(t, map[string]any{
		"type": "verified_evolution_command", "command": evolution,
	}).VerifyEvolutionCommand(evolution); err != nil || !reflect.DeepEqual(returned, evolution) {
		t.Fatalf("matching evolution command was rejected: %#v %v", returned, err)
	}
	forgedEvolution := SelectEvolutionOccurrence("command:two", "occurrence:one", "selection:one", binding)
	_, err = engineWithSuccess(t, map[string]any{
		"type": "verified_evolution_command", "command": forgedEvolution,
	}).VerifyEvolutionCommand(evolution)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

	live := PublishLiveDefinition("command:live:one", "definition:one", fixedCandidate().Definitions[0], []SubflowReference{})
	if returned, err := engineWithSuccess(t, map[string]any{
		"type": "verified_live_evolution_command", "command": live,
	}).VerifyLiveEvolutionCommand(live); err != nil || !reflect.DeepEqual(returned, live) {
		t.Fatalf("matching live-evolution command was rejected: %#v %v", returned, err)
	}
	forgedLive := PublishLiveDefinition("command:live:two", "definition:one", fixedCandidate().Definitions[0], []SubflowReference{})
	_, err = engineWithSuccess(t, map[string]any{
		"type": "verified_live_evolution_command", "command": forgedLive,
	}).VerifyLiveEvolutionCommand(live)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")
}

func TestCallerDefinedJSONMarshalerIsRejectedBeforeEncoding(t *testing.T) {
	calls := 0
	response := map[string]any{
		"type": "execution_boundary",
		"execution": map[string]any{
			"status": "completed",
			"result": map[string]any{
				"run_id": "run:test", "plan_id": fixedPlan().PlanID,
				"value":              map[string]any{"value": "first"},
				"projection_digest":  fixedDigest("a"),
				"precondition_token": "pre:0:" + fixedContentID("9"),
				"effects":            []any{},
			},
		},
	}
	_, err := engineWithSuccess(t, response).Run(
		fixedPlan(), changingJSONMarshaler{calls: &calls}, testProcessPlugin(t, "/bin/true"), "run:test",
	)
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if calls != 0 {
		t.Fatalf("rejected caller marshaler was invoked %d times", calls)
	}
}

func TestMalformedDurableSuccessPreservesQueryAndMutationClassification(t *testing.T) {
	malformed := map[string]any{
		"type": "durable_executed",
		"response": map[string]any{
			"type": "run_current", "observed_revision": fixedContentID("5"),
			"source_root": fixedDigest("6"),
		},
	}
	queryEngine := DurableEngine{Store: DirectoryStore("unused"), Transport: engineWithSuccess(t, malformed)}
	_, err := queryEngine.RunCurrent("run:test", nil)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

	command := ResumeDurableRun("run:test", fixtureExecution())
	_, err = engineWithSuccess(t, malformed).ExecuteDurable(testDurableTargetForCommand(t, command), command)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestDurableResponseCollectionsAreCanonical(t *testing.T) {
	activation := SignalWaitActivation(
		"activation:test", "signal:test", []string{fixedContentID("a"), fixedContentID("b")},
		ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("1"), Kind: "cymule.wait-result/1"},
	)
	valid := []map[string]any{
		{
			"type": "durable_executed", "response": map[string]any{
				"type": "wait_activated", "receipt": map[string]any{
					"receipt_version": "cymule.wait-activation-receipt/3", "activation": activation,
					"applied_wait_ids": []any{fixedContentID("a")}, "ready_run_ids": []any{"run:a", "run:b"},
				},
			},
		},
		{
			"type": "durable_executed", "response": map[string]any{
				"type": "run_boundary", "boundary": map[string]any{
					"status": "release_required", "intent_ids": []any{fixedContentID("4"), fixedContentID("5")},
				},
			},
		},
	}
	for index, response := range valid {
		if err := validateSuccessResponse(mustRawJSON(t, response)); err != nil {
			t.Fatalf("valid canonical durable response %d was rejected: %v", index, err)
		}
	}

	malformed := []map[string]any{
		{
			"type": "durable_executed", "response": map[string]any{
				"type": "wait_activated", "receipt": map[string]any{
					"receipt_version": "cymule.wait-activation-receipt/3", "activation": activation,
					"applied_wait_ids": []any{}, "ready_run_ids": []any{"run:a"},
				},
			},
		},
		{
			"type": "durable_executed", "response": map[string]any{
				"type": "wait_activated", "receipt": map[string]any{
					"receipt_version": "cymule.wait-activation-receipt/3", "activation": activation,
					"applied_wait_ids": []any{fixedContentID("a")}, "ready_run_ids": []any{"run:b", "run:a"},
				},
			},
		},
		{
			"type": "durable_executed", "response": map[string]any{
				"type": "run_boundary", "boundary": map[string]any{
					"status": "release_required", "intent_ids": []any{fixedContentID("5"), fixedContentID("4")},
				},
			},
		},
	}
	for index, response := range malformed {
		if err := validateSuccessResponse(mustRawJSON(t, response)); err == nil {
			t.Fatalf("non-canonical durable response %d was accepted", index)
		}
	}
}

func TestAppliedEffectSummaryRequiresCanonicalResult(t *testing.T) {
	fixturePath := os.Getenv("CYMULE_APPLIED_EFFECT_SUMMARY_FIXTURE")
	if fixturePath == "" {
		t.Skip("Applied Effect summary conformance is not configured")
	}
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	runID := fixture["run_id"].(string)
	command, err := QueryDurableRunEffectPage(runID, DurablePageQueryOptions{
		Limit: 1, MaxCanonicalBytes: maxDurableQueryPageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		state    string
		result   any
		accepted bool
	}{
		{"applied canonical null Artifact", "applied", fixture["result"], true},
		{"applied missing Artifact", "applied", nil, false},
		{"not applied absence", "not_applied", nil, true},
		{"not applied unexpected Artifact", "not_applied", fixture["result"], false},
	} {
		t.Run(test.name, func(t *testing.T) {
			summary := cloneWireMap(t, fixture)
			summary["state"], summary["result"] = test.state, test.result
			engine := engineWithSuccess(t, map[string]any{
				"type": "durable_executed",
				"response": map[string]any{
					"type": "run_effect_page", "run_id": runID,
					"page": map[string]any{
						"observed_revision": fixedContentID("5"), "source_root": fixedDigest("6"),
						"items": []any{summary}, "next_cursor": nil,
					},
				},
			})
			_, err := engine.ExecuteDurable(EngineDurableTarget{Store: DirectoryStore("unused")}, command)
			if test.accepted {
				if err != nil {
					t.Fatalf("valid summary rejected at Engine ingress: %v", err)
				}
			} else if failure, ok := errors.AsType[EngineFailure](err); !ok || failure.Code != "invalid_engine_response" {
				t.Fatalf("malformed summary did not fail as an invalid read response: %v", err)
			}
		})
	}
}

func TestDurableControlFourQueriesAreBoundedAndClosed(t *testing.T) {
	runID := "run:query-v4"
	revision := fixedContentID("5")
	sourceRoot := fixedDigest("6")
	options := DurablePageQueryOptions{Limit: 256, MaxCanonicalBytes: maxDurableQueryPageBytes}
	commands := make([]DurableCommand, 0, 7)
	for _, build := range []func() (DurableCommand, error){
		func() (DurableCommand, error) { return QueryDurableRunIndexPage(options) },
		func() (DurableCommand, error) { return QueryDurableRunCurrent(runID, nil) },
		func() (DurableCommand, error) { return QueryDurableRunWaitPage(runID, options) },
		func() (DurableCommand, error) { return QueryDurableRunEffectPage(runID, options) },
		func() (DurableCommand, error) { return QueryDurableRunOccurrencePage(runID, options) },
		func() (DurableCommand, error) { return QueryDurableRunAttemptPage(runID, options) },
		func() (DurableCommand, error) {
			return QueryDurableRunItem(DurableRunItemQuery{
				RunID: runID, Selector: DurableRunItemSelector{Kind: "wait", WaitID: fixedContentID("7")},
				MaxCanonicalBytes: maxDurableQueryExactResponseBytes,
			})
		},
	} {
		command, err := build()
		if err != nil {
			t.Fatal(err)
		}
		if err := validateDurableCommandResponse(command); err != nil {
			t.Fatalf("valid durable-control/4 query rejected: %v", err)
		}
		commands = append(commands, command)
	}
	wantTypes := []string{
		"run_index_page", "run_current", "run_wait_page", "run_effect_page",
		"run_occurrence_page", "run_attempt_page", "run_item",
	}
	for index, command := range commands {
		if command.Type != wantTypes[index] || command.ControlVersion != DurableControlVersion {
			t.Fatalf("query %d drifted: %#v", index, command)
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(mustRawJSON(t, command), &wire); err != nil {
			t.Fatal(err)
		}
		if _, present := wire["expected_revision"]; !present || !rawMessageIsNull(wire["expected_revision"]) {
			t.Fatalf("query %s omitted required-null expected_revision", command.Type)
		}
		if strings.HasSuffix(command.Type, "_page") {
			if _, present := wire["cursor"]; !present || !rawMessageIsNull(wire["cursor"]) {
				t.Fatalf("query %s omitted required-null cursor", command.Type)
			}
		}
	}

	currentResponse := map[string]any{
		"type": "run_current", "observed_revision": revision, "source_root": sourceRoot,
		"current": fixedDurableRunCurrent(runID),
	}
	var decoded DurableResponse
	if err := json.Unmarshal(mustRawJSON(t, currentResponse), &decoded); err != nil {
		t.Fatalf("valid Run-current response rejected: %v", err)
	}
	absentRun := cloneWireMap(t, currentResponse)
	absentRun["current"] = nil
	if err := json.Unmarshal(mustRawJSON(t, absentRun), &decoded); err != nil {
		t.Fatalf("required-null absent Run rejected: %v", err)
	}
	missingCurrent := cloneWireMap(t, currentResponse)
	delete(missingCurrent, "current")
	if err := json.Unmarshal(mustRawJSON(t, missingCurrent), &decoded); err == nil {
		t.Fatal("Run-current response omitted its required nullable current member")
	}
	missingResult := cloneWireMap(t, currentResponse)
	delete(missingResult["current"].(map[string]any), "result")
	if err := json.Unmarshal(mustRawJSON(t, missingResult), &decoded); err == nil {
		t.Fatal("Run-current projection omitted its required nullable result member")
	}

	summaries := []map[string]any{
		{"run_id": "run:index-a", "continuation_status": "ready", "execution_status": map[string]any{"status": "active"}, "world_settlement": "settled"},
		{"run_id": "run:index-b", "continuation_status": "completed", "execution_status": map[string]any{"status": "completed"}, "world_settlement": "settled"},
	}
	sort.Slice(summaries, func(left, right int) bool {
		leftHash, rightHash := durablePageKeyHash(summaries[left]["run_id"].(string)), durablePageKeyHash(summaries[right]["run_id"].(string))
		if leftHash == rightHash {
			return summaries[left]["run_id"].(string) < summaries[right]["run_id"].(string)
		}
		return leftHash < rightHash
	})
	terminalKey := summaries[len(summaries)-1]["run_id"].(string)
	runIndex := map[string]any{
		"type": "run_index_page", "page": map[string]any{
			"observed_revision": revision, "source_root": sourceRoot,
			"items": summaries,
			"next_cursor": map[string]any{
				"query_kind": "run_index", "run_id": nil,
				"source_revision": revision, "source_root": sourceRoot,
				"position": map[string]any{"canonical_key": terminalKey, "key_hash": durablePageKeyHash(terminalKey)},
			},
		},
	}
	if err := json.Unmarshal(mustRawJSON(t, runIndex), &decoded); err != nil {
		t.Fatalf("valid authenticated Run-index page rejected: %v", err)
	}
	missingCursor := cloneWireMap(t, runIndex)
	delete(missingCursor["page"].(map[string]any), "next_cursor")
	if err := json.Unmarshal(mustRawJSON(t, missingCursor), &decoded); err == nil {
		t.Fatal("terminal page omitted required-null next_cursor")
	}
	forgedHash := cloneWireMap(t, runIndex)
	forgedHash["page"].(map[string]any)["next_cursor"].(map[string]any)["position"].(map[string]any)["key_hash"] = fixedDigest("0")
	if err := json.Unmarshal(mustRawJSON(t, forgedHash), &decoded); err == nil {
		t.Fatal("forged authenticated cursor hash was accepted")
	}
	reversed := cloneWireMap(t, runIndex)
	items := reversed["page"].(map[string]any)["items"].([]any)
	items[0], items[1] = items[1], items[0]
	if err := json.Unmarshal(mustRawJSON(t, reversed), &decoded); err == nil {
		t.Fatal("reversed authenticated query order was accepted")
	}

	waitPage := map[string]any{
		"type": "run_wait_page", "run_id": runID,
		"page": map[string]any{
			"observed_revision": revision, "source_root": sourceRoot,
			"items":       []any{map[string]any{"wait_id": fixedContentID("7"), "run_id": runID, "state": "pending", "result": nil}},
			"next_cursor": nil,
		},
	}
	if err := json.Unmarshal(mustRawJSON(t, waitPage), &decoded); err != nil {
		t.Fatalf("valid wait-summary page rejected: %v", err)
	}
	missingWaitResult := cloneWireMap(t, waitPage)
	delete(missingWaitResult["page"].(map[string]any)["items"].([]any)[0].(map[string]any), "result")
	if err := json.Unmarshal(mustRawJSON(t, missingWaitResult), &decoded); err == nil {
		t.Fatal("wait summary omitted required-null result")
	}

	itemResponse := map[string]any{
		"type": "run_item", "run_id": runID, "observed_revision": revision,
		"source_root": sourceRoot, "item": nil,
	}
	if err := json.Unmarshal(mustRawJSON(t, itemResponse), &decoded); err != nil {
		t.Fatalf("required-null absent exact item rejected: %v", err)
	}
	missingItem := cloneWireMap(t, itemResponse)
	delete(missingItem, "item")
	if err := json.Unmarshal(mustRawJSON(t, missingItem), &decoded); err == nil {
		t.Fatal("exact item response omitted required-null item")
	}

	waitItem := cloneWireMap(t, itemResponse)
	waitItem["item"] = map[string]any{
		"kind": "wait", "wait": map[string]any{
			"wait_id": fixedContentID("7"), "run_id": runID,
			"kind":         map[string]any{"kind": "signal", "key": "signal:test"},
			"consume_once": true,
			"owner": map[string]any{
				"invocation_id": fixedContentID("8"), "definition_id": "main", "site_id": "wait.test",
				"region_path": []any{}, "step_index": 0, "bind": nil,
			},
			"state": "pending", "result": nil,
		},
	}
	if err := json.Unmarshal(mustRawJSON(t, waitItem), &decoded); err != nil {
		t.Fatalf("valid exact wait leaf rejected: %v", err)
	}
	if err := validateDurableResponseForCommand(commands[6], decoded, ""); err != nil {
		t.Fatalf("exact wait leaf did not bind its selector: %v", err)
	}

	legacy := []byte(`{"type":"query_run","control_version":"cymule.durable-control/3","query_id":"query:legacy","run_id":"run:query-v4"}`)
	var legacyCommand DurableCommand
	if err := json.Unmarshal(legacy, &legacyCommand); err == nil {
		t.Fatal("legacy durable-control/3 full-Run query was accepted")
	}
}

func TestLiveEvolutionSuccessVariantsAreRecursivelyClosed(t *testing.T) {
	valid := fixedLiveEvolutionOutcomes(t)
	for result, outcome := range valid {
		t.Run("valid "+result, func(t *testing.T) {
			var decoded LiveEvolutionOutcome
			if err := decodeClosedJSON(mustRawJSON(t, outcome), &decoded); err != nil {
				t.Fatalf("valid %s outcome failed decoding: %v", result, err)
			}
			if err := decoded.validate(); err != nil {
				t.Fatalf("valid %s response was rejected: %v", result, err)
			}
		})
	}

	type mutation func(map[string]any)
	malicious := map[string]mutation{
		"definition_published": func(response map[string]any) {
			response["revision"].(map[string]any)["revision_id"] = "revision:forged"
		},
		"template_registered": func(response map[string]any) {
			response["linked"].(map[string]any)["plan"] = map[string]any{"plan_id": fixedPlan().PlanID}
		},
		"publication_applied": func(response map[string]any) {
			updates := response["receipt"].(map[string]any)["updates"].([]any)
			updates[0].(map[string]any)["advanced"] = "true"
		},
		"patch_applied": func(response map[string]any) {
			operations := response["edge"].(map[string]any)["operations"].([]any)
			operations[0].(map[string]any)["unexpected"] = true
		},
		"applied": func(response map[string]any) { response["receipt"] = map[string]any{} },
		"occurrence_selected": func(response map[string]any) {
			response["pin"].(map[string]any)["occurrence_id"] = ""
		},
		"migrated": func(response map[string]any) {
			response["receipt"].(map[string]any)["adapter_revision"] = "revision:forged"
		},
		"restart_authorized": func(response map[string]any) {
			response["receipt"].(map[string]any)["target_plan"].(map[string]any)["candidate"] = map[string]any{}
		},
		"shadow_recorded": func(response map[string]any) {
			response["comparison"].(map[string]any)["driver_revision"] = "revision:forged"
		},
		"gate_applied": func(response map[string]any) {
			response["transition"].(map[string]any)["evaluation"].(map[string]any)["outcome"] = "future"
		},
	}
	for result, mutate := range malicious {
		t.Run("malicious "+result, func(t *testing.T) {
			outcome := cloneWireMap(t, valid[result])
			mutate(outcome)
			var decoded LiveEvolutionOutcome
			err := decodeClosedJSON(mustRawJSON(t, outcome), &decoded)
			if err == nil {
				err = decoded.validate()
			}
			if err == nil {
				t.Fatalf("malicious %s response passed recursive validation", result)
			}
		})
	}
}

func TestEveryLiveEvolutionCommandRequiresItsExactOutcomeVariant(t *testing.T) {
	outcomes := fixedLiveEvolutionOutcomes(t)
	decodeOutcome := func(result string) LiveEvolutionOutcome {
		t.Helper()
		var outcome LiveEvolutionOutcome
		if err := decodeClosedJSON(mustRawJSON(t, outcomes[result]), &outcome); err != nil {
			t.Fatal(err)
		}
		return outcome
	}
	binding := ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("5"), Kind: "cymule.execution-binding/2"}
	evidence := ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("4"), Kind: "cymule.evolution-evidence/1"}
	registerTemplate := PlanTemplate{
		TemplateID: "template:test", Candidate: fixedCandidate(),
		References: []SubflowReference{{
			LogicalRef: "definition:test", LocalDefinition: "dependency",
			InputSchema: map[string]any{}, OutputSchema: map[string]any{},
			Strategy: ReferenceStrategy{Strategy: "latest_compatible"},
		}},
	}
	publication := LivePublicationCommand{
		LogicalRef: "definition:test", Definition: fixedCandidate().Definitions[0],
		References: []SubflowReference{},
		Evidence:   fixedArtifactRecord("cymule.evolution-evidence/1", []byte("publication evidence")),
		Mode:       RolloutMode{Mode: "active"},
	}
	var patchWire struct {
		FromPlan   string           `json:"from_plan"`
		Operations []PatchOperation `json:"operations"`
	}
	if err := json.Unmarshal(mustRawJSON(t, outcomes["patch_applied"]["edge"]), &patchWire); err != nil {
		t.Fatal(err)
	}
	patch := PlanPatch{FromPlan: patchWire.FromPlan, Target: fixedCandidate(), Operations: patchWire.Operations, Evidence: evidence}
	var migrationEnvelope struct {
		Request MigrationRequest `json:"request"`
	}
	if err := json.Unmarshal(mustRawJSON(t, outcomes["migrated"]["receipt"]), &migrationEnvelope); err != nil {
		t.Fatal(err)
	}
	var restartEnvelope struct {
		Request RestartRequest `json:"request"`
	}
	if err := json.Unmarshal(mustRawJSON(t, outcomes["restart_authorized"]["receipt"]), &restartEnvelope); err != nil {
		t.Fatal(err)
	}
	comparison := outcomes["shadow_recorded"]["comparison"].(map[string]any)
	shadow := ShadowRequest{
		ComparisonID: comparison["comparison_id"].(string), DecisionID: comparison["decision_id"].(string),
		Subject: comparison["subject"].(string), PrimaryPlan: comparison["primary_plan"].(string),
		ShadowPlan: comparison["shadow_plan"].(string), DriverID: comparison["driver_id"].(string),
		DriverRevision: comparison["driver_revision"].(string), Input: evidence,
		ComparisonPolicy: comparison["comparison_policy"].(string),
	}
	transition := outcomes["gate_applied"]["transition"].(map[string]any)
	var gate RolloutGate
	if err := json.Unmarshal(mustRawJSON(t, transition["evaluation"].(map[string]any)["gate"]), &gate); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		command LiveEvolutionCommand
		result  string
	}{
		{name: "publish definition", command: PublishLiveDefinition("command:publish", "definition:test", fixedCandidate().Definitions[0], []SubflowReference{}), result: "definition_published"},
		{name: "register template", command: RegisterLiveTemplate("command:register", registerTemplate), result: "template_registered"},
		{name: "publish and relink", command: PublishAndRelinkLive("command:relink", publication), result: "publication_applied"},
		{name: "patch", command: ApplyLiveEvolution("command:apply", "template:test", ApplyPlanPatch("command:patch", patch)), result: "patch_applied"},
		{name: "set rollout", command: ApplyLiveEvolution("command:apply", "template:test", SetEvolutionRollout("command:set", RolloutDecision{})), result: "applied"},
		{name: "observe", command: ApplyLiveEvolution("command:apply", "template:test", ObserveEvolutionRollout("command:observe", RolloutObservation{})), result: "applied"},
		{name: "select occurrence", command: ApplyLiveEvolution("command:apply", "template:test", SelectEvolutionOccurrence("command:select", "occurrence:test", "selection:test", binding)), result: "occurrence_selected"},
		{name: "migrate", command: ApplyLiveEvolution("command:apply", "template:test", MigrateEvolutionState("command:migrate", migrationEnvelope.Request)), result: "migrated"},
		{name: "restart", command: ApplyLiveEvolution("command:apply", "template:test", RestartEvolutionRun("command:restart", restartEnvelope.Request)), result: "restart_authorized"},
		{name: "shadow", command: ApplyLiveEvolution("command:apply", "template:test", RunEvolutionShadow("command:shadow", shadow)), result: "shadow_recorded"},
		{name: "gate", command: ApplyLiveEvolution("command:apply", "template:test", ApplyEvolutionGate("command:gate", gate, transition["to_decision"].(string))), result: "gate_applied"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := validateLiveEvolutionOutcomeForCommand(testCase.command, decodeOutcome(testCase.result)); err != nil {
				t.Fatalf("matching outcome was rejected: %v", err)
			}
			if testCase.result != "applied" {
				forged := cloneWireMap(t, outcomes[testCase.result])
				switch testCase.result {
				case "definition_published":
					forged["revision"].(map[string]any)["logical_ref"] = "definition:forged"
				case "template_registered":
					forged["linked"].(map[string]any)["template_id"] = "template:forged"
				case "publication_applied":
					forged["receipt"].(map[string]any)["revision"].(map[string]any)["logical_ref"] = "definition:forged"
				case "patch_applied":
					forged["edge"].(map[string]any)["from_plan"] = fixedContentID("f")
				case "occurrence_selected":
					forged["pin"].(map[string]any)["selection_id"] = "selection:forged"
				case "migrated":
					forged["receipt"].(map[string]any)["request"].(map[string]any)["expected_source_epoch"] = 1
				case "restart_authorized":
					request := forged["receipt"].(map[string]any)["request"].(map[string]any)
					request["expected_source_epoch"] = 1
				case "shadow_recorded":
					forged["comparison"].(map[string]any)["subject"] = "run:forged"
				case "gate_applied":
					forged["transition"].(map[string]any)["to_decision"] = fixedContentID("f")
				}
				var forgedOutcome LiveEvolutionOutcome
				if err := decodeClosedJSON(mustRawJSON(t, forged), &forgedOutcome); err != nil {
					t.Fatal(err)
				}
				if err := validateLiveEvolutionOutcomeForCommand(testCase.command, forgedOutcome); err == nil {
					t.Fatal("same-variant outcome with changed command semantics was accepted")
				}
			}
			wrong := "definition_published"
			if testCase.result == wrong {
				wrong = "applied"
			}
			if err := validateLiveEvolutionOutcomeForCommand(testCase.command, decodeOutcome(wrong)); err == nil {
				t.Fatal("wrong outcome variant was accepted")
			}
		})
	}
}

func TestExecuteLiveEvolutionTreatsWrongSuccessAsUnknown(t *testing.T) {
	valid := fixedLiveEvolutionOutcomes(t)
	command := PublishLiveDefinition("command:test", "definition:test", fixedCandidate().Definitions[0], []SubflowReference{})
	target := EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}
	evolutionID := "journal:test"

	engine := engineWithSuccess(t, liveEvolutionExecuted(evolutionID, command, valid["definition_published"]))
	if _, err := engine.ExecuteLiveEvolution(target, evolutionID, command); err != nil {
		t.Fatalf("matching live-evolution response was rejected: %v", err)
	}

	malformed := cloneWireMap(t, valid["definition_published"])
	malformed["revision"].(map[string]any)["revision_version"] = "cymule.subflow-revision/1"
	cases := []struct {
		name     string
		response map[string]any
	}{
		{name: "known wrong outer tag", response: map[string]any{"type": "verified"}},
		{name: "legacy response payload", response: map[string]any{"type": "live_evolution_executed", "response": valid["definition_published"]}},
		{name: "wrong journal", response: liveEvolutionExecuted("journal:forged", command, valid["definition_published"])},
		{name: "wrong echoed command", response: liveEvolutionExecuted(evolutionID, PublishLiveDefinition("command:forged", "definition:test", fixedCandidate().Definitions[0], []SubflowReference{}), valid["definition_published"])},
		{name: "valid wrong inner result", response: liveEvolutionExecuted(evolutionID, command, valid["applied"])},
		{name: "malformed matching inner result", response: liveEvolutionExecuted(evolutionID, command, malformed)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := engineWithSuccess(t, testCase.response).ExecuteLiveEvolution(target, evolutionID, command)
			requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestExecuteLiveEvolutionSealsPatchTargetBeforeMutation(t *testing.T) {
	valid := fixedLiveEvolutionOutcomes(t)
	before, after := fixedDigest("1"), fixedDigest("2")
	command := ApplyLiveEvolution(
		"command:live:test", "template:test",
		ApplyPlanPatch("command:patch:test", PlanPatch{
			FromPlan: fixedContentID("b"), Target: fixedCandidate(),
			Operations: []PatchOperation{{Kind: "replace", Target: "definition:main", Before: &before, After: &after}},
			Evidence:   ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("4"), Kind: "cymule.evolution-evidence/1"},
		}),
	)
	target := EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}
	evolutionID := "journal:test"
	sealSuccess := map[string]any{"type": "sealed", "plan": fixedPlan()}
	liveSuccess := liveEvolutionExecuted(evolutionID, command, valid["patch_applied"])

	if _, err := engineWithSealAndLiveSuccesses(t, sealSuccess, liveSuccess).ExecuteLiveEvolution(target, evolutionID, command); err != nil {
		t.Fatalf("matching Rust-sealed target Plan was rejected: %v", err)
	}

	wrongTarget := cloneWireMap(t, valid["patch_applied"])
	wrongTarget["edge"].(map[string]any)["to_plan"] = fixedContentID("f")
	_, err := engineWithSealAndLiveSuccesses(
		t, sealSuccess,
		liveEvolutionExecuted(evolutionID, command, wrongTarget),
	).ExecuteLiveEvolution(target, evolutionID, command)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	wrongSeal := map[string]any{
		"type": "sealed",
		"plan": SealedPlan{
			PlanID: fixedContentID("f"),
			Candidate: NewFlow("different_target", map[string]any{}, map[string]any{}).
				Finish(Expression{"kind": "input"}),
		},
	}
	_, err = engineWithSealAndLiveSuccesses(t, wrongSeal, liveSuccess).
		ExecuteLiveEvolution(target, evolutionID, command)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")
}

func TestEvolutionCommitEchoesCompleteRegisterTemplateCommand(t *testing.T) {
	valid := fixedLiveEvolutionOutcomes(t)
	template := PlanTemplate{
		TemplateID: "template:test",
		Candidate:  fixedCandidate(),
		References: []SubflowReference{
			{
				LogicalRef: "definition:z", LocalDefinition: "linked_z",
				InputSchema: map[string]any{}, OutputSchema: map[string]any{},
				Strategy: ReferenceStrategy{Strategy: "latest_compatible"},
			},
			{
				LogicalRef: "definition:a", LocalDefinition: "linked_a",
				InputSchema: map[string]any{}, OutputSchema: map[string]any{},
				Strategy: ReferenceStrategy{Strategy: "latest_compatible"},
			},
		},
	}
	command := RegisterLiveTemplate("command:register:test", template)
	missingStrategy := template
	missingStrategy.References = slices.Clone(template.References)
	missingStrategy.References[0].Strategy = ReferenceStrategy{}
	if err := validateLiveEvolutionCommandSemantics(
		RegisterLiveTemplate("command:register:missing-strategy", missingStrategy),
	); err == nil {
		t.Fatal("template reference accepted an omitted strategy")
	}
	target := EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}}
	evolutionID := "journal:test"
	outcome := cloneWireMap(t, valid["template_registered"])
	outcome["linked"].(map[string]any)["resolved_revisions"] = map[string]any{
		"definition:a": fixedContentID("1"),
		"definition:z": fixedContentID("2"),
	}
	if _, err := engineWithSuccess(t, liveEvolutionExecuted(evolutionID, command, outcome)).
		ExecuteLiveEvolution(target, evolutionID, command); err != nil {
		t.Fatalf("matching complete command receipt was rejected: %v", err)
	}

	changedTemplateID := template
	changedTemplateID.TemplateID = "template:forged"
	changedCandidate := template
	changedCandidate.Candidate = NewFlow("forged_parent", map[string]any{}, map[string]any{}).
		Finish(Expression{"kind": "input"})
	changedReferences := template
	changedReferences.References = template.References[:1]
	cases := []struct {
		name    string
		command LiveEvolutionCommand
	}{
		{name: "outer command identity", command: RegisterLiveTemplate("command:register:forged", template)},
		{name: "template identity", command: RegisterLiveTemplate("command:register:test", changedTemplateID)},
		{name: "template candidate", command: RegisterLiveTemplate("command:register:test", changedCandidate)},
		{name: "template references", command: RegisterLiveTemplate("command:register:test", changedReferences)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := engineWithSuccess(t, liveEvolutionExecuted(evolutionID, testCase.command, outcome)).
				ExecuteLiveEvolution(target, evolutionID, command)
			requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestEvolutionCommitEchoesEverySemanticCommandField(t *testing.T) {
	outcomes := fixedLiveEvolutionOutcomes(t)
	evolutionID := "journal:test"
	evidence := ArtifactRef{
		IdentityVersion: "cymule.artifact/2",
		ArtifactID:      fixedContentID("4"),
		Kind:            "cymule.evolution-evidence/1",
	}

	publication := LivePublicationCommand{
		LogicalRef: "definition:test",
		Definition: fixedCandidate().Definitions[0],
		References: []SubflowReference{},
		Evidence:   fixedArtifactRecord("cymule.evolution-evidence/1", []byte("publication evidence")),
		Mode:       RolloutMode{Mode: "shadow"},
	}
	publishCommand := PublishAndRelinkLive("command:publish:test", publication)
	decision := RolloutDecision{
		DecisionID:   fixedContentID("6"),
		FallbackPlan: fixedContentID("b"),
		TargetPlan:   fixedContentID("a"),
		Mode:         RolloutMode{Mode: "active"},
	}
	setCommand := ApplyLiveEvolution(
		"command:apply:set", "template:test",
		SetEvolutionRollout("command:set:test", decision),
	)
	observation := RolloutObservation{
		ObservationID: "observation:test",
		DecisionID:    decision.DecisionID,
		OccurrenceID:  "occurrence:test",
		PlanID:        decision.TargetPlan,
		Outcome:       "succeeded",
		Evidence:      evidence,
	}
	observeCommand := ApplyLiveEvolution(
		"command:apply:observe", "template:test",
		ObserveEvolutionRollout("command:observe:test", observation),
	)

	var migrationEnvelope struct {
		Request MigrationRequest `json:"request"`
	}
	if err := json.Unmarshal(
		mustRawJSON(t, outcomes["migrated"]["receipt"]),
		&migrationEnvelope,
	); err != nil {
		t.Fatal(err)
	}
	migrationCommand := ApplyLiveEvolution(
		"command:apply:migrate", "template:test",
		MigrateEvolutionState("command:migrate:test", migrationEnvelope.Request),
	)

	matching := []struct {
		name    string
		command LiveEvolutionCommand
		outcome map[string]any
	}{
		{name: "publication", command: publishCommand, outcome: outcomes["publication_applied"]},
		{name: "rollout decision", command: setCommand, outcome: outcomes["applied"]},
		{name: "rollout observation", command: observeCommand, outcome: outcomes["applied"]},
		{name: "migration request", command: migrationCommand, outcome: outcomes["migrated"]},
	}
	for _, testCase := range matching {
		t.Run("matching "+testCase.name, func(t *testing.T) {
			if _, err := engineWithSuccess(t, liveEvolutionExecuted(evolutionID, testCase.command, testCase.outcome)).
				ExecuteLiveEvolution(testEvolutionTargetForCommand(t, testCase.command), evolutionID, testCase.command); err != nil {
				t.Fatalf("matching %s receipt was rejected: %v", testCase.name, err)
			}
		})
	}

	changedEvidence := publication
	changedEvidence.Evidence = fixedArtifactRecord(
		"cymule.evolution-evidence/1", []byte("forged publication evidence"),
	)
	changedMode := publication
	changedMode.Mode = RolloutMode{Mode: "canary", BasisPoints: 1}
	changedDecision := decision
	changedDecision.TargetPlan = fixedContentID("f")
	changedObservation := observation
	changedObservation.Outcome = "failed"
	changedMigration := migrationEnvelope.Request
	changedMigration.MigrationID = "migration:forged"

	cases := []struct {
		name    string
		actual  LiveEvolutionCommand
		echoed  LiveEvolutionCommand
		outcome map[string]any
	}{
		{
			name: "publication evidence", actual: publishCommand,
			echoed:  PublishAndRelinkLive("command:publish:test", changedEvidence),
			outcome: outcomes["publication_applied"],
		},
		{
			name: "publication mode", actual: publishCommand,
			echoed:  PublishAndRelinkLive("command:publish:test", changedMode),
			outcome: outcomes["publication_applied"],
		},
		{
			name: "inner command identity", actual: setCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:set", "template:test",
				SetEvolutionRollout("command:set:forged", decision),
			),
			outcome: outcomes["applied"],
		},
		{
			name: "template scope", actual: setCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:set", "template:forged",
				SetEvolutionRollout("command:set:test", decision),
			),
			outcome: outcomes["applied"],
		},
		{
			name: "rollout decision", actual: setCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:set", "template:test",
				SetEvolutionRollout("command:set:test", changedDecision),
			),
			outcome: outcomes["applied"],
		},
		{
			name: "ambiguous applied operation", actual: setCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:set", "template:test",
				ObserveEvolutionRollout("command:set:test", observation),
			),
			outcome: outcomes["applied"],
		},
		{
			name: "rollout observation", actual: observeCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:observe", "template:test",
				ObserveEvolutionRollout("command:observe:test", changedObservation),
			),
			outcome: outcomes["applied"],
		},
		{
			name: "migration request", actual: migrationCommand,
			echoed: ApplyLiveEvolution(
				"command:apply:migrate", "template:test",
				MigrateEvolutionState("command:migrate:test", changedMigration),
			),
			outcome: outcomes["migrated"],
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := engineWithSuccess(t, liveEvolutionExecuted(evolutionID, testCase.echoed, testCase.outcome)).
				ExecuteLiveEvolution(testEvolutionTargetForCommand(t, testCase.actual), evolutionID, testCase.actual)
			requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestEvolutionCommitRejectsOmissionErasingNullMembers(t *testing.T) {
	evolutionID := "journal:null-presence"
	decision := RolloutDecision{
		DecisionID: fixedContentID("6"), FallbackPlan: fixedContentID("b"),
		TargetPlan: fixedContentID("a"), Mode: RolloutMode{Mode: "active"},
	}
	apply := ApplyLiveEvolution(
		"command:apply:null", "template:test",
		SetEvolutionRollout("command:set:null", decision),
	)
	validApplied := liveEvolutionExecuted(evolutionID, apply, map[string]any{"result": "applied"})

	pinNull := cloneWireMap(t, validApplied)
	pinNull["commit"].(map[string]any)["receipt"].(map[string]any)["outcome"].(map[string]any)["pin"] = nil

	template := PlanTemplate{
		TemplateID: "template:test", Candidate: fixedCandidate(),
		References: []SubflowReference{{
			LogicalRef: "definition:test", LocalDefinition: "dependency",
			InputSchema: map[string]any{}, OutputSchema: map[string]any{},
			Strategy: ReferenceStrategy{Strategy: "latest_compatible"},
		}},
	}
	register := RegisterLiveTemplate("command:register:null", template)
	registerOutcome := fixedLiveEvolutionOutcomes(t)["template_registered"]
	revisionNull := cloneWireMap(t, liveEvolutionExecuted(evolutionID, register, registerOutcome))
	references := revisionNull["commit"].(map[string]any)["receipt"].(map[string]any)["command"].(map[string]any)["command"].(map[string]any)["template"].(map[string]any)["references"].([]any)
	references[0].(map[string]any)["strategy"].(map[string]any)["revision_id"] = nil

	cases := []struct {
		name     string
		command  LiveEvolutionCommand
		response map[string]any
	}{
		{name: "outcome pin", command: apply, response: pinNull},
		{name: "reference revision", command: register, response: revisionNull},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := engineWithSuccess(t, testCase.response).ExecuteLiveEvolution(
				testEvolutionTargetForCommand(t, testCase.command), evolutionID, testCase.command,
			)
			requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestLiveEvolutionApplyRejectsRetiredOuterAuthority(t *testing.T) {
	outcome := fixedLiveEvolutionOutcomes(t)["restart_authorized"]
	var envelope struct {
		Request RestartRequest `json:"request"`
	}
	if err := json.Unmarshal(mustRawJSON(t, outcome["receipt"]), &envelope); err != nil {
		t.Fatal(err)
	}
	command := ApplyLiveEvolution(
		"command:restart:nested-authority", "template:test",
		RestartEvolutionRun("command:restart:nested", envelope.Request),
	)
	if err := validateLiveEvolutionCommandSemantics(command); err != nil {
		t.Fatalf("terminal restart command was rejected: %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(mustRawJSON(t, command), &wire); err != nil {
		t.Fatal(err)
	}
	wire["safe_point"] = map[string]any{}
	var decoded LiveEvolutionCommand
	if err := json.Unmarshal(mustRawJSON(t, wire), &decoded); err == nil {
		t.Fatal("live-evolution apply accepted retired outer authority")
	}
}

func TestDurableEngineSelectsOnlyTheEvolutionProviderRequiredByOperation(t *testing.T) {
	outcomes := fixedLiveEvolutionOutcomes(t)
	evolutionID := "cymule.sdk.live-evolution"
	publish := PublishLiveDefinition(
		"command:provider:none", "definition:test", fixedCandidate().Definitions[0],
		[]SubflowReference{},
	)

	var migrationEnvelope struct {
		Request MigrationRequest `json:"request"`
	}
	if err := json.Unmarshal(mustRawJSON(t, outcomes["migrated"]["receipt"]), &migrationEnvelope); err != nil {
		t.Fatal(err)
	}
	migration := ApplyLiveEvolution(
		"command:provider:migration", "template:test",
		MigrateEvolutionState("command:migrate:provider", migrationEnvelope.Request),
	)

	comparison := outcomes["shadow_recorded"]["comparison"].(map[string]any)
	shadowRequest := ShadowRequest{
		ComparisonID: comparison["comparison_id"].(string), DecisionID: comparison["decision_id"].(string),
		Subject: comparison["subject"].(string), PrimaryPlan: comparison["primary_plan"].(string),
		ShadowPlan:       comparison["shadow_plan"].(string),
		DriverID:         comparison["driver_id"].(string),
		DriverRevision:   comparison["driver_revision"].(string),
		Input:            ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("4"), Kind: "cymule.evolution-evidence/1"},
		ComparisonPolicy: comparison["comparison_policy"].(string),
	}
	shadow := ApplyLiveEvolution(
		"command:provider:shadow", "template:test",
		RunEvolutionShadow("command:shadow:provider", shadowRequest),
	)
	migrationProvider := EngineMigrationProviderTarget{
		AdapterID:       migrationEnvelope.Request.AdapterID,
		AdapterRevision: migrationEnvelope.Request.AdapterRevision,
		Process:         testPinnedProcessPlugin(t, "/bin/true", migrationEnvelope.Request.AdapterRevision),
	}
	shadowProvider := EngineShadowProviderTarget{
		DriverID:       shadowRequest.DriverID,
		DriverRevision: shadowRequest.DriverRevision,
		Process:        testPinnedProcessPlugin(t, "/bin/true", shadowRequest.DriverRevision),
	}
	targetPlan := migrationEnvelope.Request.ToPlan
	targetExecutor := PinnedProcessPlugin(testProcessConfig(t, "/bin/true"), fixedContentID("9"))
	validTargetExecution := EngineEvolutionTarget{
		Store: DirectoryStore("unused"), MigrationAdapter: &migrationProvider,
		TargetExecutionBindings: map[string]EnginePluginTarget{targetPlan: targetExecutor},
	}
	if err := validateEngineEvolutionTarget(validTargetExecution, migration); err != nil {
		t.Fatalf("exact target execution binding was rejected: %v", err)
	}
	for name, bindings := range map[string]map[string]EnginePluginTarget{
		"too many": {
			targetPlan: targetExecutor, fixedContentID("8"): targetExecutor,
		},
		"wrong Plan": {fixedContentID("8"): targetExecutor},
		"unpinned":   {targetPlan: ProcessPlugin(testProcessConfig(t, "/bin/true"))},
	} {
		t.Run("target execution "+name, func(t *testing.T) {
			invalid := validTargetExecution
			invalid.TargetExecutionBindings = bindings
			if err := validateEngineEvolutionTarget(invalid, migration); err == nil {
				t.Fatal("invalid target execution binding was accepted")
			}
		})
	}
	for _, limit := range []uint64{evolutionPluginMessageBytes - 1, evolutionPluginMessageBytes + 1} {
		invalidProvider := migrationProvider
		invalidProvider.Process.Process.MessageLimit = limit
		if err := validateEngineEvolutionTarget(
			EngineEvolutionTarget{
				Store: DirectoryStore("unused"), MigrationAdapter: &invalidProvider,
				TargetExecutionBindings: map[string]EnginePluginTarget{},
			},
			migration,
		); err == nil {
			t.Fatal("Evolution process accepted a narrowed or widened message limit")
		}
	}

	cases := []struct {
		name    string
		command LiveEvolutionCommand
		outcome map[string]any
	}{
		{name: "none", command: publish, outcome: outcomes["definition_published"]},
		{name: "migration", command: migration, outcome: outcomes["migrated"]},
		{name: "shadow", command: shadow, outcome: outcomes["shadow_recorded"]},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			client := DurableEngine{
				Store:            DirectoryStore("unused"),
				MigrationAdapter: &migrationProvider, ShadowDriver: &shadowProvider,
				Transport: engineWithSuccess(t, liveEvolutionExecuted(evolutionID, testCase.command, testCase.outcome)),
			}
			if _, err := client.Evolve(testCase.command); err != nil {
				t.Fatalf("operation-scoped evolution target was rejected: %v", err)
			}
		})
	}

	executable := filepath.Join(t.TempDir(), "engine")
	marker := executable + ".started"
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n: > \"$0.started\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := (DurableEngine{
		Store: DirectoryStore("unused"), Transport: CliEngine{Executable: executable},
	}).Evolve(migration)
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("missing migration provider started the Engine: %v", statErr)
	}
}

func TestRolloutModeWirePreservesCanaryZero(t *testing.T) {
	encoded, err := json.Marshal(RolloutMode{Mode: "canary", BasisPoints: 0})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"mode":"canary","basis_points":0}` {
		t.Fatalf("canary zero lost its required field: %s", encoded)
	}
	for _, malformed := range []string{
		`{"mode":"canary"}`,
		`{"mode":"active","basis_points":0}`,
		`{"mode":"canary","basis_points":10001}`,
	} {
		var mode RolloutMode
		if err := decodeClosedJSON([]byte(malformed), &mode); err == nil {
			t.Fatalf("malformed rollout mode was accepted: %s", malformed)
		}
	}
}

func TestPublishedRevisionUsesRegistryDraftAdmission(t *testing.T) {
	outcome := cloneWireMap(t, fixedLiveEvolutionOutcomes(t)["definition_published"])
	draft := fixedCandidate().Definitions[0]
	draft.Body = Region{
		Steps: []Step{{
			"id": "", "op": "scope", "bind": "",
			"body": Region{
				Steps: []Step{{
					"id": "", "op": "call", "component": "",
					"input": Expression{"kind": "binding", "name": ""},
				}},
				Result: Expression{"kind": "binding", "name": ""},
			},
		}},
		Result: Expression{"kind": "input"},
	}
	outcome["revision"].(map[string]any)["definition"] = draft
	command := PublishLiveDefinition("command:draft", "definition:test", draft, []SubflowReference{})
	commit, err := engineWithSuccess(t, liveEvolutionExecuted("evolution:test", command, outcome)).
		ExecuteLiveEvolution(
			EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}},
			"evolution:test", command,
		)
	if err != nil {
		t.Fatalf("Rust-publishable registry draft was rejected after commit: %v", err)
	}
	if commit.Receipt.Outcome.Result != "definition_published" {
		t.Fatalf("unexpected registry draft outcome: %#v", commit)
	}

	malformed := cloneWireMap(t, outcome)
	definition := malformed["revision"].(map[string]any)["definition"].(map[string]any)
	definition["body"].(map[string]any)["steps"].([]any)[0].(map[string]any)["unexpected"] = true
	_, err = engineWithSuccess(t, liveEvolutionExecuted("evolution:test", command, malformed)).
		ExecuteLiveEvolution(
			EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}},
			"evolution:test", command,
		)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestInvalidLiveEvolutionCommandDoesNotStartTransport(t *testing.T) {
	var migrationEnvelope struct {
		Request MigrationRequest `json:"request"`
	}
	if err := json.Unmarshal(
		mustRawJSON(t, fixedLiveEvolutionOutcomes(t)["migrated"]["receipt"]),
		&migrationEnvelope,
	); err != nil {
		t.Fatal(err)
	}
	migrationEnvelope.Request.AdapterRevision = "adapter-revision:invalid"
	invalid := ApplyLiveEvolution(
		"command:apply:migrate", "template:test",
		MigrateEvolutionState("command:migrate:test", migrationEnvelope.Request),
	)
	directory := t.TempDir()
	executable := filepath.Join(directory, "engine")
	marker := executable + ".called"
	script := []byte("#!/bin/sh\n/bin/echo called > \"$0.called\"\nexit 99\n")
	if err := os.WriteFile(executable, script, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := (CliEngine{Executable: executable}).ExecuteLiveEvolution(
		EngineEvolutionTarget{Store: DirectoryStore("unused"), TargetExecutionBindings: map[string]EnginePluginTarget{}},
		"journal:test", invalid,
	)
	if err == nil {
		t.Fatal("migration with an invalid adapter revision was accepted")
	}
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid command started the Engine transport: %v", statErr)
	}
}

func TestLiveEvolutionOutcomeSelfConsistencyFailsClosed(t *testing.T) {
	valid := fixedLiveEvolutionOutcomes(t)
	type mutation func(map[string]any)
	cases := []struct {
		name   string
		result string
		mutate mutation
	}{
		{
			name: "publication advance without decision", result: "publication_applied",
			mutate: func(response map[string]any) {
				updates := response["receipt"].(map[string]any)["updates"].([]any)
				updates[0].(map[string]any)["decision_id"] = nil
			},
		},
		{
			name: "publication no-advance changes Plan", result: "publication_applied",
			mutate: func(response map[string]any) {
				update := response["receipt"].(map[string]any)["updates"].([]any)[0].(map[string]any)
				update["advanced"], update["decision_id"] = false, nil
			},
		},
		{
			name: "publication updates out of order", result: "publication_applied",
			mutate: func(response map[string]any) {
				receipt := response["receipt"].(map[string]any)
				updates := receipt["updates"].([]any)
				updates = append(updates, map[string]any{
					"template_id": "template:aaa", "previous_plan_id": fixedContentID("b"),
					"current_plan_id": fixedContentID("b"), "decision_id": nil, "advanced": false,
				})
				receipt["updates"] = updates
			},
		},
		{
			name: "Plan edge has invalid kind", result: "patch_applied",
			mutate: func(response map[string]any) {
				response["edge"].(map[string]any)["operations"].([]any)[0].(map[string]any)["kind"] = "future"
			},
		},
		{
			name: "Plan edge has invalid digest", result: "patch_applied",
			mutate: func(response map[string]any) {
				response["edge"].(map[string]any)["operations"].([]any)[0].(map[string]any)["before"] = "not-a-digest"
			},
		},
		{
			name: "Plan edge operations out of order", result: "patch_applied",
			mutate: func(response map[string]any) {
				response["edge"].(map[string]any)["operations"] = []any{
					map[string]any{"kind": "add", "target": "target:z", "before": nil, "after": fixedDigest("3")},
					map[string]any{"kind": "remove", "target": "target:a", "before": fixedDigest("4"), "after": nil},
				}
			},
		},
		{
			name: "Plan edge ID is not a content ID", result: "patch_applied",
			mutate: func(response map[string]any) { response["edge"].(map[string]any)["edge_id"] = "edge:test" },
		},
		{
			name: "Plan edge retains retired evidence", result: "patch_applied",
			mutate: func(response map[string]any) {
				response["edge"].(map[string]any)["evidence"] = fixedArtifact("1", "cymule.evolution-evidence/1")
			},
		},
		{
			name: "migration expected source epoch mismatch", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["request"].(map[string]any)["expected_source_epoch"] = 1
			},
		},
		{
			name: "migration reuses source Plan", result: "migrated",
			mutate: func(response map[string]any) {
				request := response["receipt"].(map[string]any)["request"].(map[string]any)
				request["to_plan"] = request["from_plan"]
			},
		},
		{
			name: "migration source binding has wrong kind", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["source_binding"].(map[string]any)["kind"] = "test/value"
			},
		},
		{
			name: "migration target binding mismatch", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["target_continuation"].(map[string]any)["binding_context"] = fixedContentID("f")
			},
		},
		{
			name: "migration Continuation generation missing", result: "migrated",
			mutate: func(response map[string]any) {
				delete(response["receipt"].(map[string]any)["target_continuation"].(map[string]any), "continuation_version")
			},
		},
		{
			name: "migration Continuation generation unsupported", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["target_continuation"].(map[string]any)["continuation_version"] = "cymule.continuation-state/999"
			},
		},
		{
			name: "migration target epoch is not successor", result: "migrated",
			mutate: func(response map[string]any) { response["receipt"].(map[string]any)["target_epoch"] = 2 },
		},
		{
			name: "migration target fence changed", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["target_continuation"].(map[string]any)["execution_fence"] = 4
			},
		},
		{
			name: "migration adapter revision mismatch", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["adapter_revision"] = fixedContentID("2")
			},
		},
		{
			name: "migration target state mismatch", result: "migrated",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["output_state"] = fixedArtifact("2", "cymule.evolution-evidence/1")
			},
		},
		{
			name: "restart target Plan mismatch", result: "restart_authorized",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["request"].(map[string]any)["to_plan"] = fixedContentID("1")
			},
		},
		{
			name: "restart reuses source Run", result: "restart_authorized",
			mutate: func(response map[string]any) {
				request := response["receipt"].(map[string]any)["request"].(map[string]any)
				request["replacement_run"] = request["run_id"]
			},
		},
		{
			name: "restart reuses source Plan", result: "restart_authorized",
			mutate: func(response map[string]any) {
				request := response["receipt"].(map[string]any)["request"].(map[string]any)
				request["to_plan"] = request["from_plan"]
			},
		},
		{
			name: "restart source witness is invalid", result: "restart_authorized",
			mutate: func(response map[string]any) {
				response["receipt"].(map[string]any)["source_witness_id"] = "witness:invalid"
			},
		},
		{
			name: "rollout gate decision mismatch", result: "gate_applied",
			mutate: func(response map[string]any) {
				evaluation := response["transition"].(map[string]any)["evaluation"].(map[string]any)
				evaluation["gate"].(map[string]any)["decision_id"] = fixedContentID("f")
			},
		},
		{
			name: "rollout failures exceed observations", result: "gate_applied",
			mutate: func(response map[string]any) {
				evaluation := response["transition"].(map[string]any)["evaluation"].(map[string]any)
				evaluation["target_failures"] = 2
			},
		},
		{
			name: "rollout evidence count mismatch", result: "gate_applied",
			mutate: func(response map[string]any) {
				response["transition"].(map[string]any)["evaluation"].(map[string]any)["evidence_ids"] = []any{}
			},
		},
		{
			name: "rollout outcome contradicts thresholds", result: "gate_applied",
			mutate: func(response map[string]any) {
				response["transition"].(map[string]any)["evaluation"].(map[string]any)["outcome"] = "rollback"
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			outcome := cloneWireMap(t, valid[testCase.result])
			testCase.mutate(outcome)
			var decoded LiveEvolutionOutcome
			err := decodeClosedJSON(mustRawJSON(t, outcome), &decoded)
			if err == nil {
				err = decoded.validate()
			}
			if err == nil {
				t.Fatalf("inconsistent %s response was accepted", testCase.result)
			}
		})
	}
}

func mustRawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
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
	_, err = engine.Run(plan, map[string]any{"simulate": "expected_failure"}, testProcessPlugin(t, pluginPath), "run:go-expected")
	assertEngineFailure(t, err, fixture.Cases["expected_plugin_failure"])
	_, err = engine.Run(plan, map[string]any{"message": "defect"}, testProcessPlugin(t, enginePath), "run:go-defect")
	assertEngineFailure(t, err, fixture.Cases["plugin_defect"])
	_, err = engine.Run(plan, map[string]any{"message": "substrate"}, testProcessPlugin(t, "/cymule-conformance/missing-plugin"), "run:go-substrate")
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

func TestEngineFailureLengthsCountUnicodeScalars(t *testing.T) {
	validFailure := func() EngineFailure {
		return EngineFailure{
			Category: "validation", Phase: "validate_request", Code: "invalid_request",
			Message: strings.Repeat("🚀", 8192), Contract: strings.Repeat("界", 500),
			ContractSide: "input", Path: "/" + strings.Repeat("路", 999),
			Issues: []EngineIssue{{
				Code: strings.Repeat("错", 200), Message: strings.Repeat("误", 2000),
				Path: "/" + strings.Repeat("值", 999), SchemaPath: "/" + strings.Repeat("模", 999),
			}},
			RetryDisposition: "correct_and_retry",
		}
	}
	valid := validFailure()
	if err := valid.validate(); err != nil {
		t.Fatalf("valid scalar-boundary Engine failure was rejected: %v", err)
	}
	var response map[string]any
	err := decodeEngineResponse(mustRawJSON(t, map[string]any{
		"outcome": "failure", "engine_protocol": EngineProtocolVersion, "error": valid,
	}), &response)
	requireFailure(t, err, "validation", "invalid_request", "correct_and_retry")

	cases := map[string]func(*EngineFailure){
		"message":         func(failure *EngineFailure) { failure.Message = strings.Repeat("🚀", 8193) },
		"control message": func(failure *EngineFailure) { failure.Message = "invalid\nmessage" },
		"contract": func(failure *EngineFailure) {
			failure.Contract = strings.Repeat("界", 501)
		},
		"control contract": func(failure *EngineFailure) { failure.Contract = "invalid\x00contract" },
		"path":             func(failure *EngineFailure) { failure.Path = "/" + strings.Repeat("路", 1000) },
		"control path":     func(failure *EngineFailure) { failure.Path = "/invalid\npath" },
		"issue code": func(failure *EngineFailure) {
			failure.Issues[0].Code = strings.Repeat("错", 201)
		},
		"control issue code": func(failure *EngineFailure) { failure.Issues[0].Code = "invalid\ncode" },
		"issue message": func(failure *EngineFailure) {
			failure.Issues[0].Message = strings.Repeat("误", 2001)
		},
		"control issue message": func(failure *EngineFailure) { failure.Issues[0].Message = "invalid\x00message" },
		"issue path": func(failure *EngineFailure) {
			failure.Issues[0].Path = "/" + strings.Repeat("值", 1000)
		},
		"issue schema path": func(failure *EngineFailure) {
			failure.Issues[0].SchemaPath = "/" + strings.Repeat("模", 1000)
		},
		"invalid UTF-8 message": func(failure *EngineFailure) {
			failure.Message = string([]byte{0xff})
		},
		"invalid UTF-8 contract": func(failure *EngineFailure) {
			failure.Contract = string([]byte{0xff})
		},
		"invalid UTF-8 issue": func(failure *EngineFailure) {
			failure.Issues[0].Code = string([]byte{0xff})
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			failure := validFailure()
			mutate(&failure)
			if err := failure.validate(); err == nil {
				t.Fatal("invalid Engine failure scalar boundary was accepted")
			}
		})
	}
}

func TestEngineFailureRetryMatrixIsClosed(t *testing.T) {
	allowed := map[string]map[string]bool{
		"transport_failure":       {"": true},
		"validation":              {"correct_and_retry": true},
		"contract_violation":      {"correct_and_retry": true, "never": true},
		"admission_denied":        {"correct_and_retry": true, "never": true},
		"conflict":                {"refresh_and_retry": true, "never": true},
		"not_found":               {"": true},
		"expected_plugin_failure": {"never": true},
		"plugin_defect":           {"never": true},
		"substrate_failure":       {"retry_same_request": true},
		"cancelled":               {"never": true},
		"timed_out":               {"retry_same_request": true, "refresh_and_retry": true},
		"unknown_world_outcome":   {"reconcile": true},
	}
	dispositions := []string{"", "never", "correct_and_retry", "refresh_and_retry", "retry_same_request", "reconcile"}
	for category, permitted := range allowed {
		for _, disposition := range dispositions {
			failure := EngineFailure{
				Category: category, Phase: "transport", Code: "matrix_test",
				Message: "matrix test", RetryDisposition: disposition,
			}
			err := failure.validate()
			if permitted[disposition] != (err == nil) {
				t.Fatalf("matrix %s/%q validation mismatch: %v", category, disposition, err)
			}
		}
	}
}

func TestForgedUnknownOutcomeRetryIsRejectedByRequestClass(t *testing.T) {
	input := mustRawJSON(t, map[string]any{
		"outcome": "failure", "engine_protocol": EngineProtocolVersion,
		"error": map[string]any{
			"category": "unknown_world_outcome", "phase": "transport",
			"code": "forged_retry", "message": "forged retry",
			"retry_disposition": "retry_same_request",
		},
	})
	var response map[string]any
	err := decodeEngineResponseForRequest(input, &response, map[string]any{"type": "seal"})
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")
	err = decodeEngineResponseForRequest(input, &response, map[string]any{"type": "observe_clock"})
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
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

func TestContentResourceIntegrityPreservesRequiredZeroSize(t *testing.T) {
	encoded, err := json.Marshal(ResourceIntegrity{
		Kind: "content", Digest: fixedContentID("1"), Size: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if size, exists := wire["size"]; !exists || size != float64(0) {
		t.Fatalf("required zero content size was omitted: %#v", wire)
	}
}

func TestResourceHandleValidationIsClosedAndComplete(t *testing.T) {
	emptyAnnotations, err := json.Marshal(TextResource("value", nil))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(emptyAnnotations), `"annotations"`) {
		t.Fatalf("empty Resource annotations were not omitted: %s", emptyAnnotations)
	}
	valid := map[string]any{
		"resource_id": fixedContentID("1"), "resource_version": "cymule.resource/3",
		"shape": "inline", "media_type": "text/plain;charset=utf-8",
		"inline":    map[string]any{"encoding": "utf8", "text": "value"},
		"integrity": map[string]any{"kind": "inline"},
	}
	if err := validateSuccessResponse(mustRawJSON(t, map[string]any{
		"type": "sealed_resource", "resource": valid,
	})); err != nil {
		t.Fatalf("valid Resource Handle was rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "non-hex Resource ID",
			mutate: func(resource map[string]any) {
				resource["resource_id"] = "sha256:" + strings.Repeat("g", 64)
			},
		},
		{
			name: "non-canonical media type",
			mutate: func(resource map[string]any) {
				resource["media_type"] = "Text/Plain"
			},
		},
		{
			name: "explicit empty annotations",
			mutate: func(resource map[string]any) {
				resource["annotations"] = map[string]any{}
			},
		},
		{
			name: "cross-variant integrity member",
			mutate: func(resource map[string]any) {
				resource["integrity"].(map[string]any)["digest"] = fixedContentID("2")
			},
		},
		{
			name: "missing content size",
			mutate: func(resource map[string]any) {
				resource["shape"] = "object"
				delete(resource, "inline")
				resource["integrity"] = map[string]any{"kind": "content", "digest": fixedContentID("2")}
			},
		},
		{
			name: "external inline data",
			mutate: func(resource map[string]any) {
				resource["shape"] = "object"
			},
		},
		{
			name: "non-canonical base64",
			mutate: func(resource map[string]any) {
				resource["inline"] = map[string]any{"encoding": "base64", "data": "A==="}
			},
		},
		{
			name: "manifest mismatch",
			mutate: func(resource map[string]any) {
				resource["shape"] = "collection"
				delete(resource, "inline")
				resource["integrity"] = map[string]any{
					"kind": "content", "digest": fixedContentID("2"), "size": 4,
				}
				resource["manifest"] = map[string]any{
					"manifest_version": "cymule.resource-manifest/3",
					"media_type":       "application/vnd.cymule.resource-manifest+jsonl",
					"digest":           fixedContentID("3"), "size": 4, "entry_count": 1,
					"root_digest": fixedContentID("4"),
				}
			},
		},
		{
			name: "control annotation key",
			mutate: func(resource map[string]any) {
				resource["annotations"] = map[string]any{"bad\u0085key": "value"}
			},
		},
		{
			name: "oversized annotation value",
			mutate: func(resource map[string]any) {
				resource["annotations"] = map[string]any{"key": strings.Repeat("x", 4097)}
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			resource := cloneWireMap(t, valid)
			testCase.mutate(resource)
			if err := validateSuccessResponse(mustRawJSON(t, map[string]any{
				"type": "sealed_resource", "resource": resource,
			})); err == nil {
				t.Fatal("invalid Resource Handle was accepted")
			}
		})
	}
}

func TestSealedResourceBindsTheCompleteRequestedCandidate(t *testing.T) {
	candidate := TextResource("requested", map[string]string{"purpose": "binding"})
	matching := ResourceHandle{ResourceID: fixedContentID("1"), ResourceCandidate: candidate}
	if returned, err := engineWithSuccess(t, map[string]any{
		"type": "sealed_resource", "resource": matching,
	}).SealResource(candidate); err != nil || !reflect.DeepEqual(returned, matching) {
		t.Fatalf("matching sealed Resource was rejected: %#v %v", returned, err)
	}
	forgedCandidate := TextResource("forged", map[string]string{"purpose": "binding"})
	forged := ResourceHandle{ResourceID: fixedContentID("2"), ResourceCandidate: forgedCandidate}
	_, err := engineWithSuccess(t, map[string]any{
		"type": "sealed_resource", "resource": forged,
	}).SealResource(candidate)
	requireFailure(t, err, "transport_failure", "invalid_engine_response", "")
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

func TestEngineJSONRejectsInvalidUTF8AndUnpairedSurrogates(t *testing.T) {
	if _, err := decodeUniqueJSON([]byte{'"', 0xff, '"'}); err == nil {
		t.Fatal("strict JSON accepted invalid UTF-8")
	}
	mutatingRequest := map[string]any{"type": "observe_clock"}
	malformed := []byte(`{"outcome":"failure","engine_protocol":"cymule.engine/5","error":{"category":"transport_failure","phase":"transport","code":"bad","message":"\ud800"}}`)
	var response map[string]any
	err := decodeEngineResponseForRequest(malformed, &response, mutatingRequest)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")

	directory := t.TempDir()
	executable := filepath.Join(directory, "engine")
	marker := executable + ".started"
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n: > \"$0.started\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	candidate := fixedCandidate()
	candidate.Name = string([]byte{0xff})
	_, err = (CliEngine{Executable: executable}).Seal(candidate)
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid UTF-8 request started the Engine process: %v", statErr)
	}
	if _, err := StartDurableRun("run:test", fixedCandidate(), string([]byte{0xff}), fixtureExecution()); err == nil {
		t.Fatal("durable input silently replaced invalid UTF-8")
	}
	if _, err := CancelDurableRun("cancel:test", "run:test", json.RawMessage(`"\ud800"`)); err == nil {
		t.Fatal("durable cancellation silently replaced an unpaired surrogate")
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	if _, err := StartDurableRun("run:test", fixedCandidate(), cyclic, fixtureExecution()); err == nil {
		t.Fatal("durable input accepted a cyclic non-JSON value")
	}
}

func TestCanonicalJSONSizeMatchesRustJCSWithoutHTMLEscaping(t *testing.T) {
	raw := json.RawMessage("{\"<\":\"<&>\u2028\",\"n\":0.000001,\"m\":1e-7}")
	size, err := normalizedJSONSize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if size != 36 {
		t.Fatalf("unexpected RFC 8785 byte size: got %d want 36", size)
	}
	decoded, err := decodeUniqueJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	htmlEscaped, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(htmlEscaped) <= size {
		t.Fatalf("test fixture did not distinguish HTML-escaped JSON: %q", htmlEscaped)
	}
}

func TestStrictJSONRejectsCallerMarshalersBeforeMutationStarts(t *testing.T) {
	calls := 0
	value := invalidTextMarshaler{calls: &calls}
	if _, err := StartDurableRun("run:text-marshaler", fixedCandidate(), value, fixtureExecution()); err == nil {
		t.Fatal("durable input accepted a caller-defined TextMarshaler")
	}
	if calls != 0 {
		t.Fatalf("rejected TextMarshaler was invoked %d times", calls)
	}
	if _, err := CancelDurableRun(
		"cancel:text-key", "run:text-key", map[invalidTextMapKey]string{1: "value"},
	); err == nil {
		t.Fatal("durable cancellation accepted a TextMarshaler map key")
	}

	executable := filepath.Join(t.TempDir(), "engine")
	marker := executable + ".started"
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n: > \"$0.started\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := (CliEngine{Executable: executable}).Run(
		fixedPlan(), value, testProcessPlugin(t, "/bin/true"), "run:text-marshaler",
	)
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if calls != 0 {
		t.Fatalf("transport preflight invoked rejected TextMarshaler %d times", calls)
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid TextMarshaler started the Engine: %v", statErr)
	}
}

func TestResourceManifestDescriptorIdentityIsRecomputed(t *testing.T) {
	canonical := ResourceManifestDescriptor{
		ManifestVersion: "cymule.resource-manifest/3",
		MediaType:       "application/vnd.cymule.resource-manifest+jsonl",
		RootDigest:      emptyResourceManifestRoot,
	}
	canonical.Digest = resourceManifestDescriptorID(canonical)
	if err := validateResourceManifest(canonical); err != nil {
		t.Fatalf("canonical empty Resource manifest was rejected: %v", err)
	}
	nonEmpty := ResourceManifestDescriptor{
		ManifestVersion: "cymule.resource-manifest/3",
		MediaType:       "application/vnd.cymule.resource-manifest+jsonl",
		Size:            10,
		EntryCount:      1,
		RootDigest:      fixedContentID("2"),
	}
	nonEmpty.Digest = resourceManifestDescriptorID(nonEmpty)
	if err := validateResourceManifest(nonEmpty); err != nil {
		t.Fatalf("canonical non-empty Resource manifest was rejected: %v", err)
	}
	mixed := nonEmpty
	mixed.RootDigest = fixedContentID("3")
	if err := validateResourceManifest(mixed); err == nil {
		t.Fatal("Resource manifest accepted a descriptor digest from another Merkle root")
	}
	for _, mutate := range []func(*ResourceManifestDescriptor){
		func(manifest *ResourceManifestDescriptor) {
			manifest.ManifestVersion = "cymule.resource-manifest/2"
			manifest.RootDigest = "sha256:b6009c22e4a61a949312181d089c38194269a3aa38098801fa38a6d8307050a3"
		},
		func(manifest *ResourceManifestDescriptor) { manifest.Digest = fixedContentID("1") },
		func(manifest *ResourceManifestDescriptor) { manifest.RootDigest = fixedContentID("2") },
	} {
		forged := canonical
		mutate(&forged)
		if err := validateResourceManifest(forged); err == nil {
			t.Fatal("non-canonical empty Resource manifest was accepted")
		}
	}
}

func TestResourceHandoffV5RequiresExactProducerProvenance(t *testing.T) {
	resource := ArtifactRef{
		IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("a"),
		Kind: "cymule.resource-handle/2",
	}
	producer := ResourceProducerProvenance{
		RunID: "run:producer", OccurrenceID: fixedContentID("b"), Result: resource,
	}
	handoff, err := NewResourceHandoff(
		"transfer:test", producer, "run:consumer", "input:resource", resource,
	)
	if err != nil {
		t.Fatalf("valid resource handoff was rejected: %v", err)
	}
	if handoff.HandoffVersion != "cymule.resource-handoff/5" {
		t.Fatalf("unexpected resource handoff version %q", handoff.HandoffVersion)
	}
	activation := ResourceHandoffActivation{
		ActivationVersion: "cymule.resource-handoff-activation/3",
		ActivationID:      fixedContentID("d"),
		TransferID:        handoff.TransferID,
		ToRun:             handoff.ToRun,
		WaitID:            "wait:resource",
		Result:            resource,
	}
	encoded, err := json.Marshal(activation)
	if err != nil {
		t.Fatalf("resource handoff activation did not serialize: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 6 {
		t.Fatalf("resource handoff activation shape drifted: %s (%v)", encoded, err)
	}
	forged := resource
	forged.ArtifactID = fixedContentID("c")
	if _, err := NewResourceHandoff(
		"transfer:test", producer, "run:consumer", "input:resource", forged,
	); err == nil {
		t.Fatal("resource handoff accepted a Resource other than the producer result")
	}
	if _, err := NewResourceHandoff(
		"transfer:test", producer, producer.RunID, "input:resource", resource,
	); err == nil {
		t.Fatal("resource handoff accepted a self-transfer")
	}
}

func TestCurrentProtocolGenerationsRejectImmediatePredecessors(t *testing.T) {
	candidate := fixedCandidate()
	if candidate.IRVersion != "cymule.ir/3" {
		t.Fatalf("builder emitted IR generation %q", candidate.IRVersion)
	}
	legacyCandidate := candidate
	legacyCandidate.IRVersion = "cymule.ir/2"
	if err := validatePlanCandidateWire(legacyCandidate); err == nil {
		t.Fatal("IR /2 candidate was accepted")
	}
	durable, err := QueryDurableRunCurrent("run:version", nil)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ControlVersion != "cymule.durable-control/4" {
		t.Fatalf("builder emitted durable generation %q", durable.ControlVersion)
	}
	durable.ControlVersion = "cymule.durable-control/3"
	if err := validateDurableCommandResponse(durable); err == nil {
		t.Fatal("durable control /3 was accepted")
	}
	if current := DirectoryStore("unused"); current.Provider != "cymule.directory-store/5" || validateEngineStoreTarget(current) != nil {
		t.Fatalf("builder emitted invalid directory Store generation %#v", current)
	}
	if current := SQLiteStore("unused", "domain:test"); current.Provider != "cymule.sqlite-store/6" || validateEngineStoreTarget(current) != nil {
		t.Fatalf("builder emitted invalid SQLite Store generation %#v", current)
	}
	for _, providerNeutral := range []EngineStoreTarget{
		{Provider: "acme.store/1", Location: "provider-owned-location"},
		{Provider: "acme.partitioned-store/7", Location: "provider-owned-location", Domain: "tenant-a"},
	} {
		if err := validateEngineStoreTarget(providerNeutral); err != nil {
			t.Fatalf("provider-neutral Store selector was rejected: %#v: %v", providerNeutral, err)
		}
	}
	live := PublishLiveDefinition(
		"command:version", "definition:version", candidate.Definitions[0],
		[]SubflowReference{},
	)
	if live.ControlVersion != "cymule.live-evolution-control/6" {
		t.Fatalf("builder emitted live-evolution generation %q", live.ControlVersion)
	}
	live.ControlVersion = "cymule.live-evolution-control/5"
	if err := validateLiveEvolutionCommandSemantics(live); err == nil {
		t.Fatal("live-evolution control /5 was accepted")
	}
}

func TestComponentOutputArtifactKindIsRequiredAndNonNull(t *testing.T) {
	candidate := NewFlow("component-output-kind", map[string]any{}, map[string]any{}).
		Component(
			"test.echo", map[string]any{}, map[string]any{}, "cymule.component-output/1",
			map[string]string{},
		).
		Finish(Expression{"kind": "input"})
	encoded := mustRawJSON(t, candidate)
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	component := wire["components"].([]any)[0].(map[string]any)
	withoutOutputArtifactKind := maps.Clone(component)
	delete(withoutOutputArtifactKind, "output_artifact_kind")
	for label, malformedComponent := range map[string]map[string]any{
		"missing": withoutOutputArtifactKind,
		"null":    maps.Clone(component),
	} {
		if label == "null" {
			malformedComponent["output_artifact_kind"] = nil
		}
		malformed := maps.Clone(wire)
		malformed["components"] = []any{malformedComponent}
		_, err := engineWithSuccess(t, map[string]any{
			"type": "sealed",
			"plan": map[string]any{
				"plan_id":   fixedContentID("1"),
				"candidate": malformed,
			},
		}).Seal(candidate)
		requireFailure(t, err, "transport_failure", "invalid_engine_response", "")
	}
}

func TestHighLevelDurableEngineForwardsProviderNeutralStoreTarget(t *testing.T) {
	transport := engineWithSuccess(t, map[string]any{
		"type": "durable_executed",
		"response": map[string]any{
			"type": "run_current", "observed_revision": fixedContentID("a"),
			"source_root": fixedDigest("b"), "current": nil,
		},
	})
	durable := DurableEngine{
		Store: EngineStoreTarget{
			Provider: "acme.partitioned-store/7",
			Location: "provider-owned-location",
			Domain:   "tenant-a",
		},
		Transport: transport,
	}
	current, err := durable.RunCurrent("run:provider-neutral-store", nil)
	if err != nil || !rawMessageIsNull(current.Current) {
		t.Fatalf("provider-neutral Store target did not reach the Engine: current=%s err=%v", current.Current, err)
	}
}

func TestEngineProcessTargetRequiresCompleteClosedConfiguration(t *testing.T) {
	target := ProcessPlugin(testProcessConfig(t, "/bin/true"))
	maximum := target.Process
	maximum.Arguments = make([]string, 4096)
	maximum.Environment = make(map[string]string, 4096)
	maximum.RuntimeClosure = make(map[string]string, 4096)
	for index := range 4096 {
		maximum.Environment[fmt.Sprintf("ENTRY_%04d", index)] = ""
		maximum.RuntimeClosure[fmt.Sprintf("runtime-%04d", index)] = "sha256:" + strings.Repeat("a", 64)
	}
	if err := validateEngineProcessConfig(maximum); err != nil {
		t.Fatalf("maximum process collection bounds were rejected: %v", err)
	}
	for _, overflow := range []func(*EngineProcessConfig){
		func(process *EngineProcessConfig) { process.Arguments = make([]string, 4097) },
		func(process *EngineProcessConfig) { process.Environment["ENTRY_OVERFLOW"] = "" },
		func(process *EngineProcessConfig) {
			process.RuntimeClosure["runtime-overflow"] = "sha256:" + strings.Repeat("a", 64)
		},
	} {
		candidate := maximum
		candidate.Environment = maps.Clone(maximum.Environment)
		candidate.RuntimeClosure = maps.Clone(maximum.RuntimeClosure)
		overflow(&candidate)
		if err := validateEngineProcessConfig(candidate); err == nil {
			t.Fatal("process configuration accepted 4097 collection entries")
		}
	}
	encoded, err := json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if _, exists := wire["location"]; exists {
		t.Fatal("process target retained the superseded location field")
	}
	process, ok := wire["process"].(map[string]any)
	if !ok {
		t.Fatal("process target omitted its complete process configuration")
	}
	if workingDirectory, exists := process["working_directory"]; !exists || workingDirectory != nil {
		t.Fatalf("required nullable working directory was not preserved: %#v", process)
	}

	malformed := []map[string]any{}
	missingWorkingDirectory := cloneWireMap(t, wire)
	delete(missingWorkingDirectory["process"].(map[string]any), "working_directory")
	malformed = append(malformed, missingWorkingDirectory)
	legacyLocation := map[string]any{
		"provider": "cymule.executor-process/1", "location": "/bin/true",
	}
	malformed = append(malformed, legacyLocation)
	relativeExecutable := cloneWireMap(t, wire)
	relativeExecutable["process"].(map[string]any)["executable"] = "bin/plugin"
	malformed = append(malformed, relativeExecutable)
	emptyRuntimeClosure := cloneWireMap(t, wire)
	emptyRuntimeClosure["process"].(map[string]any)["runtime_closure"] = map[string]any{}
	malformed = append(malformed, emptyRuntimeClosure)
	for _, limit := range []uint64{ordinaryPluginMessageBytes - 1, ordinaryPluginMessageBytes + 1} {
		wrongMessageLimit := cloneWireMap(t, wire)
		wrongMessageLimit["process"].(map[string]any)["message_limit"] = limit
		malformed = append(malformed, wrongMessageLimit)
	}
	hostRuntimeClosure := cloneWireMap(t, wire)
	hostRuntimeClosure["process"].(map[string]any)["runtime_closure"] = map[string]any{
		"host-abi": "unix:darwin:arm64",
	}
	malformed = append(malformed, hostRuntimeClosure)
	tamperedRuntimeClosure := cloneWireMap(t, wire)
	tamperedRuntimeClosure["process"].(map[string]any)["runtime_closure"] = map[string]any{
		"runtime": "sha256:" + strings.Repeat("A", 64),
	}
	malformed = append(malformed, tamperedRuntimeClosure)
	explicitNullRevision := cloneWireMap(t, wire)
	explicitNullRevision["revision"] = nil
	malformed = append(malformed, explicitNullRevision)
	for index, candidate := range malformed {
		var decoded EnginePluginTarget
		if err := json.Unmarshal(mustRawJSON(t, candidate), &decoded); err == nil {
			t.Fatalf("malformed process target %d was accepted", index)
		}
	}

	executable := filepath.Join(t.TempDir(), "engine")
	marker := executable + ".started"
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n: > \"$0.started\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	invalid := target
	invalid.Process.Executable = "relative/plugin"
	_, err = (CliEngine{Executable: executable}).Run(fixedPlan(), nil, invalid, "run:invalid-target")
	requireFailure(t, err, "validation", "invalid_engine_request", "correct_and_retry")
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("invalid process target started the Engine: %v", statErr)
	}
}

func TestEngineEnvelopeRequiresExclusiveOutcomePayload(t *testing.T) {
	invalid := []string{
		`{"engine_protocol":"cymule.engine/5","outcome":"success","response":{"type":"verified"}}`,
		`{"engine_protocol":"cymule.engine/5","outcome":"success","response":{"type":"verified"},"error":{"category":"validation","phase":"transport","code":"invalid","message":"invalid"}}`,
		`{"engine_protocol":"cymule.engine/5","outcome":"failure","response":{"type":"verified"},"error":{"category":"validation","phase":"transport","code":"invalid","message":"invalid"}}`,
		`{"engine_protocol":"cymule.engine/5","outcome":"failure","request":{},"error":{"category":"validation","phase":"transport","code":"invalid","message":"invalid"}}`,
		`{"engine_protocol":"cymule.engine/5","outcome":"success"}`,
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

func TestEngineFailureRejectsExplicitNullOptionalMembers(t *testing.T) {
	base := map[string]any{
		"category": "validation", "phase": "validate_request",
		"code": "invalid_request", "message": "invalid request",
	}
	cases := map[string]func(map[string]any){
		"contract":       func(failure map[string]any) { failure["contract"] = nil },
		"empty contract": func(failure map[string]any) { failure["contract"] = "" },
		"contract side":  func(failure map[string]any) { failure["contract_side"] = nil },
		"empty contract side": func(failure map[string]any) {
			failure["contract_side"] = ""
		},
		"path":              func(failure map[string]any) { failure["path"] = nil },
		"issues":            func(failure map[string]any) { failure["issues"] = nil },
		"retry disposition": func(failure map[string]any) { failure["retry_disposition"] = nil },
		"empty retry disposition": func(failure map[string]any) {
			failure["retry_disposition"] = ""
		},
		"issue path": func(failure map[string]any) {
			failure["issues"] = []any{map[string]any{
				"code": "invalid", "message": "invalid", "path": nil,
			}}
		},
		"issue schema path": func(failure map[string]any) {
			failure["issues"] = []any{map[string]any{
				"code": "invalid", "message": "invalid", "schema_path": nil,
			}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			failure := cloneWireMap(t, base)
			mutate(failure)
			envelope := mustRawJSON(t, map[string]any{
				"outcome": "failure", "engine_protocol": EngineProtocolVersion, "error": failure,
			})
			var response map[string]any
			readRequest := map[string]any{"type": "seal"}
			err := decodeEngineResponseForRequest(envelope, &response, readRequest)
			requireFailure(t, err, "transport_failure", "invalid_engine_response", "")

			mutatingRequest := map[string]any{"type": "observe_clock"}
			err = decodeEngineResponseForRequest(envelope, &response, mutatingRequest)
			requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
		})
	}
}

func TestValidRemoteTransportFailureIsPreserved(t *testing.T) {
	request := map[string]any{"type": "observe_clock"}
	input := mustRawJSON(t, map[string]any{
		"outcome": "failure", "engine_protocol": EngineProtocolVersion,
		"error": map[string]any{
			"category": "transport_failure", "phase": "transport",
			"code": "engine_unavailable", "message": "Engine unavailable",
		},
	})
	var response map[string]any
	err := decodeEngineResponseForRequest(input, &response, request)
	requireFailure(t, err, "transport_failure", "engine_unavailable", "")
}

func TestEngineRejectsProtocolV3(t *testing.T) {
	var response struct {
		Type string `json:"type"`
	}
	err := decodeEngineResponse(
		[]byte(`{"engine_protocol":"cymule.engine/4","outcome":"success","request":{},"response":{"type":"verified"}}`),
		&response,
	)
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Category != "contract_violation" ||
		failure.Code != "unsupported_engine_protocol" || failure.Contract != EngineProtocolVersion {
		t.Fatalf("expected strict Engine v4 rejection, got %#v (%v)", failure, err)
	}
}

func TestOldProtocolAfterMutationBeginsIsUnknown(t *testing.T) {
	mutatingRequest := map[string]any{"type": "observe_clock"}
	input := mustRawJSON(t, map[string]any{
		"engine_protocol": "cymule.engine/4", "outcome": "success",
		"request": mutatingRequest, "response": map[string]any{"type": "verified"},
	})
	var response map[string]any
	err := decodeEngineResponseForRequest(input, &response, mutatingRequest)
	requireFailure(t, err, "unknown_world_outcome", "unsupported_engine_protocol", "reconcile")

	readRequest := map[string]any{"type": "seal"}
	input = mustRawJSON(t, map[string]any{
		"engine_protocol": "cymule.engine/4", "outcome": "success",
		"request": readRequest, "response": map[string]any{"type": "verified"},
	})
	err = decodeEngineResponseForRequest(input, &response, readRequest)
	requireFailure(t, err, "contract_violation", "unsupported_engine_protocol", "never")
}

func TestMathematicalIntegerJSONTokensNormalizeBeforeTypedValidation(t *testing.T) {
	for _, lexeme := range []string{"1.0", "1e0"} {
		value, err := validateSafeUintRaw(json.RawMessage(lexeme), false)
		if err != nil || value != 1 {
			t.Fatalf("mathematical integer %s was not normalized: %d %v", lexeme, value, err)
		}
		decoded, err := decodeUniqueJSON([]byte(`{"value":` + lexeme + `}`))
		if err != nil {
			t.Fatalf("mathematical integer %s was rejected: %v", lexeme, err)
		}
		if decoded.(map[string]any)["value"].(json.Number).String() != "1" {
			t.Fatalf("mathematical integer %s did not normalize: %#v", lexeme, decoded)
		}
	}
	for _, lexeme := range []string{"1.5", "9007199254740991.1"} {
		if _, err := validateSafeUintRaw(json.RawMessage(lexeme), false); err == nil {
			t.Fatalf("fractional number populated an integer field: %s", lexeme)
		}
	}
	if _, err := decodeUniqueJSON([]byte(`{"value":1.5}`)); err != nil {
		t.Fatalf("finite arbitrary JSON decimal was rejected: %v", err)
	}
	if _, err := decodeUniqueJSON([]byte(`{"value":9007199254740992.0}`)); err == nil {
		t.Fatal("unsafe mathematical integer was accepted")
	}
	for _, lexeme := range []string{
		"1e-10000", "-1e-10000", "1.0000000000000001", "0.99999999999999999",
	} {
		if _, err := decodeUniqueJSON([]byte(`{"value":` + lexeme + `}`)); err == nil {
			t.Fatalf("mathematically fractional token collapsed to an integer: %s", lexeme)
		}
	}
}

func TestRealClientClassifiesUnsupportedEngineProtocolByMutationAuthority(t *testing.T) {
	executable := os.Getenv("CYMULE_UNSUPPORTED_ENGINE")
	if executable == "" {
		t.Skip("unsupported Engine protocol fixture is not configured")
	}
	engine := CliEngine{Executable: executable}
	_, err := engine.Seal(fixedCandidate())
	requireFailure(t, err, "contract_violation", "unsupported_engine_protocol", "never")
	_, err = engine.ObserveClock(
		SQLiteClock(
			"/tmp/cymule-go-unsupported-clock.sqlite",
			"clock:go:unsupported",
			fixedContentID("4"),
		),
		"run:go:unsupported",
	)
	requireFailure(t, err, "unknown_world_outcome", "unsupported_engine_protocol", "reconcile")
}

func TestEvolutionAndExecutionResponseUnionsAreClosed(t *testing.T) {
	for _, input := range []string{
		`{"control_version":"cymule.evolution-control/5","command_id":"command:test","operation":"future"}`,
		`{"control_version":"cymule.evolution-control/5","command_id":"command:test","operation":"select_occurrence","occurrence_id":"occurrence:test","patch":{}}`,
		`{"control_version":"cymule.evolution-control/5","command_id":"command:test","operation":"migrate","request":{"unexpected":true}}`,
	} {
		var command EvolutionCommand
		if err := decodeClosedJSON([]byte(input), &command); err == nil {
			t.Fatalf("expected closed Evolution rejection for %s", input)
		}
	}
	var live LiveEvolutionCommand
	if err := decodeClosedJSON([]byte(
		`{"control_version":"cymule.live-evolution-control/6","command_id":"command:test","operation":"future"}`,
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
		`{"status":"release_required","release":{"run_id":"run:test","plan_id":"sha256:1111111111111111111111111111111111111111111111111111111111111111","intent_ids":["sha256:2222222222222222222222222222222222222222222222222222222222222222"]}}`,
		`{"status":"reconciliation_required","reconciliation":{"run_id":"run:test","plan_id":"sha256:1111111111111111111111111111111111111111111111111111111111111111","intent_id":"sha256:2222222222222222222222222222222222222222222222222222222222222222"}}`,
	} {
		var outcome ExecutionOutcome
		if err := decodeClosedJSON([]byte(input), &outcome); err != nil {
			t.Fatalf("expected closed execution boundary to decode for %s: %v", input, err)
		}
	}
}

func TestWorkResolutionJSONIsStrictAndClosed(t *testing.T) {
	artifact := `{"identity_version":"cymule.artifact/2","artifact_id":"` + fixedContentID("1") + `","kind":"example/result"}`
	valid := []string{
		`{"resolution":"succeeded","result":` + artifact + `}`,
		`{"resolution":"retry","error":` + artifact + `,"next_reason":null}`,
		`{"resolution":"retry","error":` + artifact + `,"next_reason":{"kind":"wait","key":"wait:test"}}`,
		`{"resolution":"parked","reason":{"kind":"dependency","work_id":"work:test"}}`,
		`{"resolution":"failed","error":` + artifact + `}`,
		`{"resolution":"cancelled","reason":` + artifact + `}`,
	}
	for _, input := range valid {
		var resolution WorkResolution
		if err := json.Unmarshal([]byte(input), &resolution); err != nil {
			t.Fatalf("valid work resolution was rejected: %s: %v", input, err)
		}
		encoded, err := json.Marshal(resolution)
		if err != nil {
			t.Fatal(err)
		}
		left, leftErr := decodeUniqueJSON([]byte(input))
		right, rightErr := decodeUniqueJSON(encoded)
		if leftErr != nil || rightErr != nil || !reflect.DeepEqual(left, right) {
			t.Fatalf("work resolution did not round-trip exactly: %s -> %s", input, encoded)
		}
	}

	malformed := []string{
		`{"resolution":"failed","resolution":"succeeded","result":` + artifact + `}`,
		`{"resolution":"succeeded","result":null}`,
		`{"resolution":"succeeded","result":` + artifact + `,"error":` + artifact + `}`,
		`{"resolution":"retry","error":` + artifact + `}`,
		`{"resolution":"parked","reason":{"kind":"wait","key":"wait:test","work_id":"work:test"}}`,
		`{"resolution":"failed","error":{"identity_version":"cymule.artifact/1","artifact_id":"` + fixedContentID("1") + `","kind":"example/result"}}`,
	}
	for _, input := range malformed {
		var resolution WorkResolution
		if err := json.Unmarshal([]byte(input), &resolution); err == nil {
			t.Fatalf("malformed work resolution was accepted: %s", input)
		}
	}

	nested := `{"control_version":"cymule.virtual-work-control/2","command_id":"command:test","work_id":"work:test","owner":"worker:test","epoch":1,"expected_lease_epoch":1,"clock":` +
		`{"clock_version":"cymule.clock-observation/2","observation_id":"` + fixedContentID("2") + `","source_id":"clock:test","source_generation":"` + fixedContentID("3") + `","scope":"slot:test"},` +
		`"resolution":{"resolution":"succeeded","result":null}}`
	var command WorkResolutionCommand
	if err := json.Unmarshal([]byte(nested), &command); err == nil {
		t.Fatal("nested malformed work resolution was accepted")
	}
}

func TestExecutionOutcomeIdentityAndOrderingFailClosed(t *testing.T) {
	validCompleted := map[string]any{
		"status": "completed",
		"result": map[string]any{
			"run_id": "run:test", "plan_id": fixedContentID("1"), "value": nil,
			"projection_digest": fixedDigest("2"), "precondition_token": "pre:0:" + fixedContentID("9"),
			"effects": []any{fixedContentID("3"), fixedContentID("4")},
		},
	}
	validSuspended := map[string]any{
		"status": "suspended",
		"suspension": map[string]any{
			"run_id": "run:test", "plan_id": fixedContentID("1"),
			"definition_id": "main", "invocation_id": fixedContentID("5"), "site_id": "wait.test",
			"wait":        map[string]any{"kind": "signal", "key": "signal:test", "consume_once": true},
			"result_bind": nil,
		},
	}
	for _, outcome := range []map[string]any{validCompleted, validSuspended} {
		if err := validateSuccessResponse(mustRawJSON(t, map[string]any{
			"type": "execution_boundary", "execution": outcome,
		})); err != nil {
			t.Fatalf("valid execution outcome was rejected: %v", err)
		}
	}

	invalidDigest := cloneWireMap(t, validCompleted)
	invalidDigest["result"].(map[string]any)["projection_digest"] = "digest:test"
	invalidPrecondition := cloneWireMap(t, validCompleted)
	invalidPrecondition["result"].(map[string]any)["precondition_token"] = "precondition:test"
	unsafePrecondition := cloneWireMap(t, validCompleted)
	unsafePrecondition["result"].(map[string]any)["precondition_token"] = "pre:9007199254740992:" + fixedContentID("9")
	nonCanonicalPrecondition := cloneWireMap(t, validCompleted)
	nonCanonicalPrecondition["result"].(map[string]any)["precondition_token"] = "pre:+1:" + fixedContentID("9")
	missingValue := cloneWireMap(t, validCompleted)
	delete(missingValue["result"].(map[string]any), "value")
	nullEffects := cloneWireMap(t, validCompleted)
	nullEffects["result"].(map[string]any)["effects"] = nil
	duplicateEffects := cloneWireMap(t, validCompleted)
	duplicateEffects["result"].(map[string]any)["effects"] = []any{fixedContentID("3"), fixedContentID("3")}
	reversedEffects := cloneWireMap(t, validCompleted)
	reversedEffects["result"].(map[string]any)["effects"] = []any{fixedContentID("4"), fixedContentID("3")}
	invalidInvocation := cloneWireMap(t, validSuspended)
	invalidInvocation["suspension"].(map[string]any)["invocation_id"] = "invocation:test"
	missingResultBind := cloneWireMap(t, validSuspended)
	delete(missingResultBind["suspension"].(map[string]any), "result_bind")
	invalidRelease := map[string]any{
		"status": "release_required", "release": map[string]any{
			"run_id": "run:test", "plan_id": fixedContentID("1"),
			"intent_ids": []any{fixedContentID("3"), fixedContentID("3")},
		},
	}
	nullRelease := map[string]any{
		"status": "release_required", "release": map[string]any{
			"run_id": "run:test", "plan_id": fixedContentID("1"), "intent_ids": nil,
		},
	}
	invalidReconciliation := map[string]any{
		"status": "reconciliation_required", "reconciliation": map[string]any{
			"run_id": "run:test", "plan_id": fixedContentID("1"), "intent_id": "intent:test",
		},
	}
	for index, outcome := range []map[string]any{
		invalidDigest, invalidPrecondition, unsafePrecondition, nonCanonicalPrecondition, missingValue, nullEffects, duplicateEffects, reversedEffects,
		invalidInvocation, missingResultBind, invalidRelease, nullRelease, invalidReconciliation,
	} {
		if err := validateSuccessResponse(mustRawJSON(t, map[string]any{
			"type": "execution_boundary", "execution": outcome,
		})); err == nil {
			t.Fatalf("malformed execution outcome %d was accepted", index)
		}
	}
}

func TestSuspendedExecutionMatchesItsRequestedPlanSite(t *testing.T) {
	wait := map[string]any{"kind": "signal", "key": "signal:test", "consume_once": true}
	candidate := NewFlow("wait_plan", map[string]any{}, map[string]any{}).
		Wait("wait.test", wait, "value").
		Finish(Expression{"kind": "input"})
	plan := SealedPlan{PlanID: fixedContentID("1"), Candidate: candidate}
	execution := map[string]any{
		"status": "suspended",
		"suspension": map[string]any{
			"run_id": "run:test", "plan_id": plan.PlanID, "definition_id": "main",
			"invocation_id": fixedContentID("2"), "site_id": "wait.test",
			"wait": wait, "result_bind": "value",
		},
	}
	response := map[string]any{"type": "execution_boundary", "execution": execution}
	if _, err := engineWithSuccess(t, response).Run(plan, nil, testProcessPlugin(t, "/bin/true"), "run:test"); err != nil {
		t.Fatalf("matching suspended Plan site was rejected: %v", err)
	}
	wrong := cloneWireMap(t, response)
	wrong["execution"].(map[string]any)["suspension"].(map[string]any)["site_id"] = "wait.forged"
	_, err := engineWithSuccess(t, wrong).Run(plan, nil, testProcessPlugin(t, "/bin/true"), "run:test")
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestEvolutionRequestsRejectUnsafeExpectedSourceEpoch(t *testing.T) {
	outcomes := fixedLiveEvolutionOutcomes(t)
	requests := []map[string]any{
		cloneWireMap(t, outcomes["migrated"]["receipt"].(map[string]any)["request"].(map[string]any)),
		cloneWireMap(t, outcomes["restart_authorized"]["receipt"].(map[string]any)["request"].(map[string]any)),
	}
	operations := []string{"migrate", "restart_under_new_plan"}
	for index, request := range requests {
		request["expected_source_epoch"] = uint64(9_007_199_254_740_992)
		command := map[string]any{
			"control_version": "cymule.live-evolution-control/6",
			"command_id":      "command:unsafe-source-epoch",
			"operation":       "apply",
			"template_id":     "template:test",
			"command": map[string]any{
				"control_version": "cymule.evolution-control/5",
				"command_id":      "command:unsafe-source-epoch:child",
				"operation":       operations[index],
				"request":         request,
			},
		}
		var decoded LiveEvolutionCommand
		if err := decodeClosedJSON(mustRawJSON(t, command), &decoded); err == nil {
			t.Fatalf("%s accepted an unsafe expected source epoch", operations[index])
		}
	}
}

func TestMigrationReceiptEpochAndScopeStackAreClosed(t *testing.T) {
	artifact := map[string]any{
		"identity_version": "cymule.artifact/2",
		"artifact_id":      "sha256:" + strings.Repeat("1", 64),
		"kind":             "test/value",
	}
	binding := map[string]any{
		"identity_version": "cymule.artifact/2",
		"artifact_id":      "sha256:" + strings.Repeat("2", 64),
		"kind":             "cymule.execution-binding/2",
	}
	continuation := map[string]any{
		"continuation_version": "cymule.continuation-state/1",
		"run_id":               "run:test", "plan_id": "sha256:" + strings.Repeat("3", 64),
		"binding_context": binding["artifact_id"],
		"frames": []any{map[string]any{
			"definition_id": "main", "invocation_id": "main", "invocation_path": []any{},
			"scope_id": "scope:root", "input": artifact, "region_path": []any{},
			"next_step": 0, "locals": map[string]any{},
		}},
		"state": artifact, "wait_set": []any{}, "scope_stack": []any{"scope:root"},
		"epoch": 0, "execution_fence": 3, "execution_claim": nil, "status": "ready",
	}
	request := map[string]any{
		"migration_id": "migration:test", "run_id": "run:test",
		"from_plan": continuation["plan_id"], "to_plan": "sha256:" + strings.Repeat("4", 64),
		"plan_edge_id":          "sha256:" + strings.Repeat("5", 64),
		"compatibility_id":      "sha256:" + strings.Repeat("6", 64),
		"expected_source_epoch": 0, "adapter_id": "adapter:test",
		"adapter_revision": "sha256:" + strings.Repeat("8", 64),
	}
	receipt := map[string]any{
		"request": request, "source_witness_id": "sha256:" + strings.Repeat("7", 64),
		"source_binding": binding, "target_binding": binding, "source_execution_fence": 3,
		"target_epoch": 1, "adapter_id": "adapter:test",
		"adapter_revision": "sha256:" + strings.Repeat("8", 64),
		"from_schema":      "schema:source", "to_schema": "schema:target",
		"output_state": artifact, "target_continuation": map[string]any{
			"continuation_version": "cymule.continuation-state/1",
			"run_id":               "run:test", "plan_id": request["to_plan"],
			"binding_context": binding["artifact_id"], "frames": continuation["frames"],
			"state": artifact, "wait_set": []any{}, "scope_stack": []any{"scope:root"},
			"epoch": 1, "execution_fence": 3, "execution_claim": nil, "status": "ready",
		}, "evidence": artifact,
	}
	encode := func(value any) json.RawMessage {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	if err := (LiveEvolutionOutcome{Result: "migrated", Receipt: encode(receipt)}).validate(); err != nil {
		t.Fatalf("valid migration receipt rejected: %v", err)
	}
	receipt["target_epoch"] = 0
	if err := (LiveEvolutionOutcome{Result: "migrated", Receipt: encode(receipt)}).validate(); err == nil {
		t.Fatal("zero migration target epoch was accepted")
	}
	receipt["target_epoch"] = 1
	targetContinuation := receipt["target_continuation"].(map[string]any)
	targetContinuation["scope_stack"] = []any{}
	if err := (LiveEvolutionOutcome{Result: "migrated", Receipt: encode(receipt)}).validate(); err == nil {
		t.Fatal("empty migration target scope stack was accepted")
	}
	targetContinuation["scope_stack"] = []any{"scope:root"}
	binding["kind"] = "test/value"
	if err := (LiveEvolutionOutcome{Result: "migrated", Receipt: encode(receipt)}).validate(); err == nil {
		t.Fatal("migration receipt with a non-binding Artifact was accepted")
	}
	binding["kind"] = "cymule.execution-binding/2"
	receipt["adapter_id"] = ""
	if err := (LiveEvolutionOutcome{Result: "migrated", Receipt: encode(receipt)}).validate(); err == nil {
		t.Fatal("migration receipt with an empty adapter identity was accepted")
	}
	legacyInput := map[string]any{
		"identity_version": "cymule.artifact/1",
		"artifact_id":      artifact["artifact_id"],
		"kind":             artifact["kind"],
	}
	restartReceipt := map[string]any{
		"request": map[string]any{
			"restart_id": "restart:test", "replacement_run": "run:target", "run_id": "run:test",
			"from_plan": continuation["plan_id"], "expected_source_epoch": 0,
			"to_plan": request["to_plan"], "input": legacyInput, "evidence": artifact,
		},
		"source_witness_id": "sha256:" + strings.Repeat("7", 64),
		"target_plan":       map[string]any{"plan_id": request["to_plan"], "candidate": fixedCandidate()},
	}
	if err := (LiveEvolutionOutcome{Result: "restart_authorized", Receipt: encode(restartReceipt)}).validate(); err == nil {
		t.Fatal("restart receipt with a legacy input Artifact was accepted")
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
	selection := SelectEvolutionOccurrence(
		"command:evolution:fixture:select",
		"occurrence:fixture:1",
		"selection:fixture:1",
		*expected.Command.ExecutionBinding,
	)
	command := ApplyLiveEvolution(
		"command:live-evolution:fixture:select",
		"template:review-parent",
		selection,
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
		Mutation: "observational", Dispatch: "eager", Reconciliation: "queryable",
		KeyedIdempotency: true, Irreversible: false,
	}
	candidate := NewFlow("cross_language_echo", map[string]any{}, map[string]any{}).
		Component(
			"test.echo", map[string]any{}, map[string]any{}, "cymule.component-output/1",
			map[string]string{},
		).
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
		Effect("effect.capture", "test.capture", Expression{"kind": "binding", "name": "echoed"}, "primary", "observed").
		Scope("scope.finalize", Region{
			Steps: []Step{}, Result: Expression{"kind": "literal", "value": nil},
		}, "scope_result").
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
	execution, err := engine.Run(plan, input, testProcessPlugin(t, pluginPath), "run:go-e2e")
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
	executor := testProcessPlugin(t, pluginPath)
	store := SQLiteStore(filepath.Join(t.TempDir(), "domain.sqlite"), "sdk-go")
	clock := SQLiteClock(filepath.Join(t.TempDir(), "clock.sqlite"), "clock:sdk-go", "sha256:"+strings.Repeat("3", 64))
	durable := DurableEngine{
		Store: store, Executor: &executor, Clock: &clock, Transport: engine,
	}
	clockRef, err := durable.ObserveClock("run:go-durable-e2e")
	if err != nil {
		t.Fatal(err)
	}
	laterClockRef, err := durable.ObserveClock("run:go-durable-e2e")
	if err != nil {
		t.Fatal(err)
	}
	if laterClockRef.ObservationID == clockRef.ObservationID {
		t.Fatal("successive issued Clock observations reused one identity")
	}
	response, err := durable.Start("run:go-durable-e2e", candidate, input, ExecutionClaimRequest{
		Owner: "driver:sdk-go", Clock: laterClockRef, TTL: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Type != "run_boundary" {
		t.Fatalf("unexpected durable response %q", response.Type)
	}
	if current, err := (DurableEngine{Store: store, Transport: engine}).RunCurrent("run:go-durable-e2e", nil); err != nil || rawMessageIsNull(current.Current) {
		t.Fatalf("durable Run-current query failed: %s %v", current.Current, err)
	}
	evolved, err := durable.Evolve(PublishLiveDefinition(
		"evolve:go:publish", "definition:go:echo", candidate.Definitions[0],
		[]SubflowReference{},
	))
	if err != nil || evolved.Receipt.Outcome.Result != "definition_published" ||
		evolved.Receipt.Command.EvolutionID != "cymule.sdk.live-evolution" {
		t.Fatalf("durable evolution failed: %#v %v", evolved, err)
	}

	waitRunID := "run:go-durable-wait"
	waitClock, err := durable.ObserveClock(waitRunID)
	if err != nil {
		t.Fatal(err)
	}
	waitCandidate := NewFlow("go_durable_wait", map[string]any{}, map[string]any{}).
		Wait("wait.signal", map[string]any{
			"kind": "signal", "key": "signal:continue", "consume_once": true,
		}, "signal_value").
		Finish(Expression{"kind": "binding", "name": "signal_value"})
	waitBoundary, err := durable.Start(waitRunID, waitCandidate, nil, ExecutionClaimRequest{
		Owner: "driver:sdk-go-wait", Clock: waitClock, TTL: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	boundary, err := rawJSONObject(waitBoundary.Boundary)
	if err != nil || boundary["status"] != "suspended" {
		t.Fatalf("durable wait did not suspend: %#v %v", waitBoundary, err)
	}
	waitID, ok := boundary["wait_id"].(string)
	if !ok {
		t.Fatalf("durable wait boundary omitted its wait identity: %#v", boundary)
	}
	activated, err := durable.Signal(
		"activation:go-durable-wait", "signal:continue", []string{waitID},
		map[string]any{"accepted": true},
	)
	if err != nil {
		t.Fatal(err)
	}
	var activationReceipt WaitActivationReceipt
	if err := decodeClosedJSON(activated.Receipt, &activationReceipt); err != nil ||
		activationReceipt.Activation.ActivationID != "activation:go-durable-wait" ||
		!slices.Equal(activationReceipt.AppliedWaitIDs, []string{waitID}) {
		t.Fatalf("durable wait receipt is incomplete: %#v %v", activationReceipt, err)
	}
	cancelled, err := durable.Cancel(
		"cancel:go-durable-wait", waitRunID, map[string]any{"code": "e2e_cleanup"},
	)
	if err != nil {
		t.Fatal(err)
	}
	var cancellationReceipt RunCancellationReceipt
	if err := decodeClosedJSON(cancelled.Receipt, &cancellationReceipt); err != nil ||
		cancellationReceipt.Command.CancellationID != "cancel:go-durable-wait" ||
		cancellationReceipt.Command.RunID != waitRunID {
		t.Fatalf("durable cancellation receipt is incomplete: %#v %v", cancellationReceipt, err)
	}
}

func TestMaliciousNestedEngineSuccessFailsClosed(t *testing.T) {
	malicious := os.Getenv("CYMULE_MALICIOUS_ENGINE")
	if malicious == "" {
		t.Skip("malicious Engine conformance is not configured")
	}
	_, err := (DurableEngine{Store: DirectoryStore("unused"), Transport: CliEngine{Executable: malicious}}).RunCurrent("run:fake", nil)
	var failure EngineFailure
	if !errors.As(err, &failure) || failure.Code != "invalid_engine_response" || failure.Category != "transport_failure" {
		t.Fatalf("expected nested response rejection, got %v", err)
	}
}

func TestMaliciousEffectBoundaryFailsClosedAsMutationResponseLoss(t *testing.T) {
	malicious := os.Getenv("CYMULE_MALICIOUS_EFFECT_ENGINE")
	if malicious == "" {
		t.Skip("malicious Effect Engine conformance is not configured")
	}
	_, err := (CliEngine{Executable: malicious}).Run(
		fixedPlan(), nil, testProcessPlugin(t, "/bin/true"), "run:malicious-effect",
	)
	requireFailure(t, err, "unknown_world_outcome", "invalid_engine_response", "reconcile")
}

func TestFlowFinishReturnsFrozenCandidate(t *testing.T) {
	builder := NewFlow("frozen", map[string]any{}, map[string]any{})
	candidate := builder.Finish(Expression{"kind": "input"})
	builder.Component(
		"later", map[string]any{}, map[string]any{}, "cymule.component-output/1",
		map[string]string{"capability": "late"},
	)
	if builder.candidate.Components[0].OutputArtifactKind != "cymule.component-output/1" {
		t.Fatal("component builder omitted its explicit output Artifact kind")
	}
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
		[]string{"sha256:8d55f9d1981f4579ce12d106f25d85307ed27db86a4c106bbe17cb0ea8e9acc5"},
		ArtifactRef{
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			Kind:            "cymule.wait-result/1",
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
	cancelFixturePath := os.Getenv("CYMULE_DURABLE_CANCEL_FIXTURE")
	if enginePath == "" || fixturePath == "" || cancelFixturePath == "" {
		t.Skip("durable control conformance is not configured")
	}
	command, err := TakeoverDurableRun("run:cross-language", 7, fixtureExecution())
	if err != nil {
		t.Fatal(err)
	}
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
	cancel, err := CancelDurableRun(
		"cancel:cross-language", "run:cross-language",
		map[string]any{"code": "operator_request"},
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelFixtureBytes, err := os.ReadFile(cancelFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var cancelFixture DurableCommand
	if err := json.Unmarshal(cancelFixtureBytes, &cancelFixture); err != nil {
		t.Fatal(err)
	}
	var cancelReason, fixtureReason any
	if err := json.Unmarshal(cancel.Reason, &cancelReason); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(cancelFixture.Reason, &fixtureReason); err != nil {
		t.Fatal(err)
	}
	cancelShape := cancel
	fixtureShape := cancelFixture
	cancelShape.Reason = nil
	fixtureShape.Reason = nil
	if !reflect.DeepEqual(cancelShape, fixtureShape) || !reflect.DeepEqual(cancelReason, fixtureReason) {
		t.Fatalf("durable cancel differs from shared fixture: %#v", cancel)
	}
	verifiedCancel, err := (CliEngine{Executable: enginePath}).VerifyDurableCommand(cancel)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(verifiedCancel, cancel) {
		t.Fatalf("Rust engine changed durable cancel: %#v", verifiedCancel)
	}
	activation, err := ActivateDurableSignal(
		"activation:sdk", "signal:sdk", []string{fixedContentID("b"), fixedContentID("a"), fixedContentID("b")},
		map[string]any{"accepted": true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(activation.WaitIDs, []string{fixedContentID("a"), fixedContentID("b")}) {
		t.Fatalf("durable activation targets are not canonical: %#v", activation.WaitIDs)
	}
}

func TestSharedTerminalDurableBoundariesAreTyped(t *testing.T) {
	fixturePath := os.Getenv("CYMULE_DURABLE_TERMINAL_FIXTURE")
	if fixturePath == "" {
		t.Skip("durable terminal fixture is not configured")
	}
	fixtureBytes, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var responses []DurableResponse
	if err := json.Unmarshal(fixtureBytes, &responses); err != nil {
		t.Fatal(err)
	}
	if len(responses) != 4 {
		t.Fatalf("unexpected terminal response count: %d", len(responses))
	}
	for _, response := range responses {
		if err := response.validate(); err != nil {
			t.Fatalf("terminal response failed typed validation: %v", err)
		}
	}
	var effectNotApplied map[string]any
	if err := json.Unmarshal(responses[2].Boundary, &effectNotApplied); err != nil ||
		effectNotApplied["status"] != "effect_not_applied" ||
		effectNotApplied["intent_id"] != fixedContentID("2") {
		t.Fatalf("effect-not-applied boundary is not exact: %#v %v", effectNotApplied, err)
	}
	var effectUnavailable map[string]any
	if err := json.Unmarshal(responses[3].Boundary, &effectUnavailable); err != nil ||
		effectUnavailable["status"] != "effect_unavailable" ||
		effectUnavailable["intent_id"] != "sha256:982a836f8dcb860b0eedabf0fd133bc2f966992526e2703316cba497f929e03b" {
		t.Fatalf("effect-unavailable boundary is not exact: %#v %v", effectUnavailable, err)
	}
}

func TestVirtualCompactionCertificatePreservesRequiredNullableWire(t *testing.T) {
	certificate := VirtualCompactionCertificate{
		CertificateVersion:        "cymule.virtual-compaction-certificate/4",
		CertificateID:             fixedContentID("1"),
		SourceCausalCut:           []string{"virtual:terminal"},
		Summary:                   VirtualCompletionSummary{RegionID: "region:terminal", RunID: "run:terminal", OccurrenceCount: 1, WorkCount: 1, SucceededCount: 1, OutputDigest: fixedDigest("2"), EvidenceDigest: fixedDigest("3"), RetainedDebugIndexDigest: fixedDigest("4")},
		SummaryStateDigest:        fixedDigest("5"),
		OccurrenceRootDigest:      fixedContentID("6"),
		ParentWorkIndexRootDigest: fixedContentID("7"),
		WorkIndexUpdatesDigest:    fixedDigest("8"),
		WorkIndexRootDigest:       fixedContentID("9"),
		CommandRootDigest:         nil,
		CommandCount:              0,
		UnresolvedObligations:     []string{},
		RetainedExecutionBindings: []ArtifactRef{{IdentityVersion: "cymule.artifact/2", ArtifactID: fixedContentID("a"), Kind: "cymule.execution-binding/2"}},
		ReplayAvailability:        ReplayAvailability{Status: "exact"},
		RehydrationManifest: ResourceHandle{
			ResourceID: fixedContentID("b"),
			ResourceCandidate: ResourceCandidate{
				ResourceVersion: "cymule.resource/3",
				Shape:           "object",
				MediaType:       "application/octet-stream",
				Integrity:       ResourceIntegrity{Kind: "content", Digest: fixedDigest("c")},
			},
		},
		Archive: VirtualArchiveBinding{Binding: "compactor:terminal", Revision: "revision:terminal"},
	}
	encoded, err := json.Marshal(certificate)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"parent_work_index_root_digest",
		"work_index_updates_digest",
		"work_index_root_digest",
		"command_root_digest",
		"command_count",
	} {
		if _, ok := wire[field]; !ok {
			t.Fatalf("required compaction certificate field %q was omitted", field)
		}
	}
	if wire["command_root_digest"] != nil {
		t.Fatalf("required nullable command root was not encoded as null: %#v", wire)
	}
	missing := maps.Clone(wire)
	delete(missing, "command_root_digest")
	if reflect.DeepEqual(missing, wire) {
		t.Fatal("missing command root was indistinguishable from explicit null")
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
	if occurrence.ExecutionBinding.Kind != "cymule.execution-binding/2" {
		t.Fatalf("unexpected occurrence: %#v", occurrence)
	}
	controlBytes, err := os.ReadFile(controlPath)
	if err != nil {
		t.Fatal(err)
	}
	var decoded WorkResolutionCommand
	if err := json.Unmarshal(controlBytes, &decoded); err != nil {
		t.Fatal(err)
	}
	command, err := SucceedWork(
		"command:virtual:fixture:success", "work:fixture", "worker:fixture", 1, 1,
		decoded.Clock,
		ArtifactRef{IdentityVersion: "cymule.artifact/2", ArtifactID: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", Kind: "example/result"},
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(command)
	if err != nil {
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
	compaction, err := CompactVirtualRegion(
		compactionFixture.CommandID,
		"region:fixture",
		[]string{"virtual:fixture:terminal"},
		[]string{"work:fixture"},
		[]string{occurrence.OccurrenceID},
		[]string{},
		VirtualArchiveBinding{Binding: "binding:archive/fixture@1", Revision: "compactor:fixture/1"},
	)
	if err != nil {
		t.Fatal(err)
	}
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
	claim, err := ClaimVirtualWork(
		"command:claim:fixture",
		"worker:fixture",
		"slot:worker-fixture:0",
		ArtifactRef{
			IdentityVersion: "cymule.artifact/2",
			ArtifactID:      "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			Kind:            "cymule.execution-binding/2",
		},
		[]string{"sandbox", "cpu", "cpu"},
		claimFixture.Clock,
		30,
	)
	if err != nil {
		t.Fatal(err)
	}
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
	renewal, err := RenewVirtualClaim(
		"command:renew:fixture",
		"work:fixture",
		"worker:fixture",
		1,
		1,
		renewalFixture.Clock,
		30,
	)
	if err != nil {
		t.Fatal(err)
	}
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
	recovery, err := RecoverVirtualClaim(
		"command:recovery:fixture",
		"work:fixture",
		"worker:fixture",
		1,
		2,
		recoveryFixture.Clock,
		recoveryFixture.Resolution,
	)
	if err != nil {
		t.Fatal(err)
	}
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
