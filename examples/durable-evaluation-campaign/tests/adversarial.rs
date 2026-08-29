//! Black-box campaign, crash, evolution, and integrity tests.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_core::decode_json;
use cymule_example_durable_evaluation_campaign::CampaignReport;
#[cfg(unix)]
use cymule_test_world::ManagedChild;

const SUITE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/support-tickets.jsonl"
);
const EXTERNAL_PLUGIN_RUNTIME_REVISION: &str =
    "sha256:9c2eb89abcf3a401fe81faabe91050fa1b60db352b0cfaef57ea074132bc9f2d";

struct TestDir(PathBuf);

impl TestDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock works")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cymule-evaluation-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory creates");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_cymule-example-durable-evaluation-campaign")
}

fn invoke(arguments: &[&str]) -> Output {
    Command::new(binary())
        .args(arguments)
        .output()
        .expect("example process runs")
}

fn report(output: &Output) -> CampaignReport {
    assert!(
        output.status.success() || output.status.code() == Some(75),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    decode_json(&output.stdout).expect("campaign report decodes through the strict JSON decoder")
}

fn first_regular_file(root: &Path) -> PathBuf {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).expect("Resource directory lists") {
            let entry = entry.expect("Resource entry reads");
            let file_type = entry.file_type().expect("Resource entry type reads");
            if file_type.is_file() {
                return entry.path();
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    panic!("Resource directory contains no object file")
}

#[cfg(unix)]
fn shell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> String {
    shell_literal(&path.to_string_lossy())
}

#[test]
fn campaign_completes_and_reopens_without_new_occurrences() {
    let state = TestDir::new("complete");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let first = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:complete",
        "--logical-now",
        "10",
    ]);
    let first = report(&first);
    assert_eq!(first.total_cases, 12);
    assert_eq!(first.total_occurrences, 12);
    assert_eq!(first.succeeded, 12);
    assert_eq!(first.failed, 0);
    assert_eq!(first.points, 24);

    let reopened = invoke(&["status", "--state", state_arg, "--run-id", "run:complete"]);
    assert_eq!(report(&reopened), first);
}

#[test]
fn persisted_suite_is_sufficient_to_resume_without_the_original_file() {
    let state = TestDir::new("self-contained-resume");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let suite = state.path().join("original-suite.jsonl");
    fs::copy(SUITE, &suite).expect("suite copies");
    let suite_arg = suite.to_str().expect("UTF-8 path");
    let partial = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        suite_arg,
        "--run-id",
        "run:self-contained-resume",
        "--logical-now",
        "10",
        "--simulate-crash-after-commit",
        "2",
    ]);
    assert_eq!(partial.status.code(), Some(75));
    assert_eq!(report(&partial).succeeded, 2);
    fs::remove_file(suite).expect("original suite removes");

    let resumed = report(&invoke(&[
        "run",
        "--state",
        state_arg,
        "--run-id",
        "run:self-contained-resume",
        "--logical-now",
        "20",
    ]));
    assert_eq!(resumed.succeeded, resumed.total_cases);
    assert_eq!(resumed.total_occurrences, resumed.total_cases);
}

#[test]
fn five_minute_demo_exposes_recovery_and_future_only_evolution() {
    let parent = TestDir::new("feature-tour");
    let state = parent.path().join("tour");
    let output = invoke(&["demo", "--state", state.to_str().expect("UTF-8 path")]);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("demo output is UTF-8");
    assert!(stdout.contains("Cymule: safely upgrade a running evaluation"));
    assert!(stdout.contains("restart reused all 3"));
    assert!(stdout.contains("3 completed results kept the original policy"));
    assert!(stdout.contains("9 future results used the update"));
    assert!(stdout.contains("incompatible scoring update was blocked"));
    assert!(stdout.contains("without repeating completed evaluations"));
    assert!(state.join("campaign.sqlite").is_file());
}

#[test]
fn committed_work_survives_exit_and_compatible_evolution_is_future_only() {
    let state = TestDir::new("evolve");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let partial = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:evolve",
        "--logical-now",
        "10",
        "--simulate-crash-after-commit",
        "3",
    ]);
    assert_eq!(partial.status.code(), Some(75));
    let partial_report = report(&partial);
    assert_eq!(partial_report.succeeded, 3);
    let old_plan = partial_report.current_plan_id;

    let evolution = invoke(&[
        "evolve",
        "--state",
        state_arg,
        "--run-id",
        "run:evolve",
        "--policy",
        "weighted",
    ]);
    assert!(evolution.status.success());
    let evolution: serde_json::Value =
        decode_json(&evolution.stdout).expect("evolution report decodes");
    assert_eq!(evolution["advanced"], true);
    assert_ne!(evolution["current_plan_id"], old_plan);
    let replay = invoke(&[
        "evolve",
        "--state",
        state_arg,
        "--run-id",
        "run:evolve",
        "--policy",
        "weighted",
    ]);
    assert!(replay.status.success());
    let replay: serde_json::Value =
        decode_json(&replay.stdout).expect("replayed evolution report decodes");
    assert_eq!(replay, evolution);

    let final_run = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:evolve",
        "--logical-now",
        "20",
    ]);
    let final_report = report(&final_run);
    assert_eq!(final_report.succeeded, 12);
    assert_eq!(final_report.total_occurrences, 12);
    let strict = final_report
        .cases
        .iter()
        .filter(|case| {
            case.output
                .as_ref()
                .is_some_and(|output| output.score.policy == "strict")
        })
        .count();
    let weighted = final_report
        .cases
        .iter()
        .filter(|case| {
            case.output
                .as_ref()
                .is_some_and(|output| output.score.policy == "weighted")
        })
        .count();
    assert_eq!((strict, weighted), (3, 9));
    assert!(
        final_report
            .cases
            .iter()
            .filter(|case| case
                .output
                .as_ref()
                .is_some_and(|output| output.score.policy == "strict"))
            .all(|case| case.plan_id == old_plan)
    );
}

#[test]
fn incompatible_scorer_revision_cannot_take_over_future_work() {
    let state = TestDir::new("incompatible");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let partial = report(&invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:incompatible",
        "--logical-now",
        "10",
        "--simulate-crash-after-commit",
        "1",
    ]));
    let plan = partial.current_plan_id;
    let evolution = invoke(&[
        "evolve",
        "--state",
        state_arg,
        "--run-id",
        "run:incompatible",
        "--policy",
        "incompatible",
    ]);
    assert!(evolution.status.success());
    let evolution: serde_json::Value =
        decode_json(&evolution.stdout).expect("evolution report decodes");
    assert_eq!(evolution["advanced"], false);
    assert_eq!(evolution["current_plan_id"], plan);

    let final_report = report(&invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:incompatible",
        "--logical-now",
        "20",
    ]));
    assert_eq!(final_report.succeeded, 12);
    assert!(final_report.cases.iter().all(|case| {
        case.plan_id == plan
            && case
                .output
                .as_ref()
                .is_some_and(|output| output.score.policy == "strict")
    }));
}

#[test]
fn expired_claim_is_recovered_but_unexpired_claim_is_not_stolen() {
    let state = TestDir::new("lease");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let crashed = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:lease",
        "--logical-now",
        "10",
        "--lease-ttl",
        "5",
        "--simulate-crash-after-claim",
        "1",
    ]);
    assert_eq!(crashed.status.code(), Some(75));

    let early = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:lease",
        "--logical-now",
        "14",
        "--lease-ttl",
        "5",
    ]);
    assert!(!early.status.success());
    assert!(String::from_utf8_lossy(&early.stderr).contains("retry after lease expiry"));

    let recovered = report(&invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:lease",
        "--logical-now",
        "15",
        "--lease-ttl",
        "5",
    ]));
    assert_eq!(recovered.succeeded, 12);
    assert_eq!(recovered.total_occurrences, 13);
    assert_eq!(recovered.recovered_attempts, 1);
}

#[test]
fn same_worker_reuses_only_an_unexpired_claim_and_explicitly_recovers_an_expired_claim() {
    for (case, resume_at, expected_occurrences, expected_recoveries) in
        [("unexpired", "13", 12, 0), ("expired", "15", 13, 1)]
    {
        let state = TestDir::new(&format!("same-worker-{case}"));
        let state_arg = state.path().to_str().expect("UTF-8 path");
        let run_id = format!("run:same-worker:{case}");
        let crashed = invoke(&[
            "run",
            "--state",
            state_arg,
            "--suite",
            SUITE,
            "--run-id",
            &run_id,
            "--worker-id",
            "worker:stable",
            "--logical-now",
            "10",
            "--lease-ttl",
            "5",
            "--simulate-crash-after-claim",
            "1",
        ]);
        assert_eq!(crashed.status.code(), Some(75));

        let completed = report(&invoke(&[
            "run",
            "--state",
            state_arg,
            "--suite",
            SUITE,
            "--run-id",
            &run_id,
            "--worker-id",
            "worker:stable",
            "--logical-now",
            resume_at,
            "--lease-ttl",
            "5",
        ]));
        assert_eq!(completed.succeeded, 12, "{case}");
        assert_eq!(completed.total_occurrences, expected_occurrences, "{case}");
        assert_eq!(completed.recovered_attempts, expected_recoveries, "{case}");
    }
}

#[cfg(unix)]
#[test]
fn external_process_kill_reopens_to_a_valid_frontier_and_completes_once() {
    use std::os::unix::fs::PermissionsExt as _;

    const CASES: usize = 24;

    let state = TestDir::new("process-kill");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let suite = state.path().join("large-suite.jsonl");
    let mut bytes = Vec::new();
    for index in 0..CASES {
        let case = serde_json::json!({
            "id": format!("generated-{index:04}"),
            "input": {"message": format!("How do I use feature {index}?")},
            "expected": {"category": "general", "urgency": "normal"}
        });
        bytes.extend(serde_json::to_vec(&case).expect("case encodes"));
        bytes.push(b'\n');
    }
    fs::write(&suite, bytes).expect("large suite writes");
    let suite_arg = suite.to_str().expect("UTF-8 path");
    let barrier_plugin = state.path().join("barrier-plugin.sh");
    let invocation_count = state.path().join("plugin-invocations");
    let barrier = state.path().join("plugin-barrier");
    let plugin_pid = state.path().join("plugin-pid");
    let gate = state.path().join("plugin-gate");
    assert!(
        Command::new("mkfifo")
            .arg(&gate)
            .status()
            .expect("mkfifo runs")
            .success(),
        "plugin gate creates"
    );
    fs::write(
        &barrier_plugin,
        format!(
            concat!(
                "#!/bin/sh\n",
                "set -eu\n",
                "count_file={}\n",
                "barrier_file={}\n",
                "pid_file={}\n",
                "gate={}\n",
                "request=$(/bin/cat)\n",
                "case \"$request\" in\n",
                "  *'\"type\":\"call\"'*)\n",
                "    case \"$request\" in\n",
                "      *'\"component\":\"example.ticket-subject\"'*)\n",
                "        count=0\n",
                "        if [ -f \"$count_file\" ]; then IFS= read -r count < \"$count_file\"; fi\n",
                "        count=$((count + 1))\n",
                "        count_tmp=\"$count_file.$$\"\n",
                "        printf '%s\\n' \"$count\" > \"$count_tmp\"\n",
                "        /bin/mv \"$count_tmp\" \"$count_file\"\n",
                "        if [ \"$count\" -eq 3 ]; then\n",
                "          pid_tmp=\"$pid_file.$$\"\n",
                "          printf '%s\\n' \"$$\" > \"$pid_tmp\"\n",
                "          /bin/mv \"$pid_tmp\" \"$pid_file\"\n",
                "          barrier_tmp=\"$barrier_file.$$\"\n",
                "          printf 'before-subject:3\\n' > \"$barrier_tmp\"\n",
                "          /bin/mv \"$barrier_tmp\" \"$barrier_file\"\n",
                "          IFS= read -r _ < \"$gate\"\n",
                "        fi\n",
                "        ;;\n",
                "    esac\n",
                "    ;;\n",
                "esac\n",
                "printf '%s' \"$request\" | {} __plugin\n"
            ),
            shell_quote(&invocation_count),
            shell_quote(&barrier),
            shell_quote(&plugin_pid),
            shell_quote(&gate),
            shell_quote(Path::new(binary())),
        ),
    )
    .expect("barrier plugin wrapper writes");
    let mut permissions = fs::metadata(&barrier_plugin)
        .expect("barrier plugin metadata reads")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&barrier_plugin, permissions).expect("barrier plugin becomes executable");
    let barrier_plugin_arg = barrier_plugin.to_str().expect("UTF-8 path");
    let mut running = ManagedChild::spawn(
        Command::new(binary())
            .args([
                "run",
                "--state",
                state_arg,
                "--suite",
                suite_arg,
                "--run-id",
                "run:process-kill",
                "--worker-id",
                "worker:killed",
                "--plugin",
                barrier_plugin_arg,
                "--plugin-runtime-revision",
                EXTERNAL_PLUGIN_RUNTIME_REVISION,
                "--logical-now",
                "10",
                "--lease-ttl",
                "2",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .expect("campaign process starts");

    running
        .wait_for_content(&barrier, b"before-subject:3\n", Duration::from_mins(2))
        .expect("the third subject barrier is reached after retained progress");
    let before_kill = report(&invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:process-kill",
    ]));
    assert_eq!(before_kill.succeeded, 2);
    assert_eq!(before_kill.total_occurrences, 3);
    let killed = running
        .terminate()
        .expect("campaign is externally killed and reaped");
    assert!(!killed.success());
    assert!(running.is_reaped());
    let reopened = report(&invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:process-kill",
    ]));
    assert_eq!(reopened.succeeded, before_kill.succeeded);
    assert_eq!(reopened.total_occurrences, before_kill.total_occurrences);
    assert_eq!(reopened.failed, 0);

    let completed = report(&invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        suite_arg,
        "--run-id",
        "run:process-kill",
        "--worker-id",
        "worker:reopened",
        "--logical-now",
        "16",
        "--lease-ttl",
        "2",
    ]));
    assert_eq!(completed.succeeded, CASES);
    assert_eq!(completed.failed, 0);
    assert_eq!(completed.cases.len(), CASES);
    assert_eq!(completed.recovered_attempts, 1);
    assert_eq!(completed.total_occurrences, CASES + 1);
    assert_eq!(
        completed
            .cases
            .iter()
            .map(|case| &case.case_id)
            .collect::<BTreeSet<_>>()
            .len(),
        CASES
    );
}

#[test]
fn changed_input_and_tampered_retained_resource_fail_closed() {
    let state = TestDir::new("tamper");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let suite_copy = state.path().join("suite.jsonl");
    fs::copy(SUITE, &suite_copy).expect("suite copies");
    let suite_arg = suite_copy.to_str().expect("UTF-8 path");
    assert!(
        invoke(&[
            "run",
            "--state",
            state_arg,
            "--suite",
            suite_arg,
            "--run-id",
            "run:tamper",
            "--logical-now",
            "10",
        ])
        .status
        .success()
    );

    let mut changed = fs::read_to_string(&suite_copy).expect("suite reads");
    changed.push('\n');
    fs::write(&suite_copy, changed).expect("suite changes");
    let changed_run = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        suite_arg,
        "--run-id",
        "run:tamper",
        "--logical-now",
        "20",
    ]);
    assert!(!changed_run.status.success());
    assert!(String::from_utf8_lossy(&changed_run.stderr).contains("differ"));

    let objects = state.path().join("resources/objects");
    let object = first_regular_file(&objects);
    fs::write(object, b"tampered").expect("resource tampers");
    let status = invoke(&["status", "--state", state_arg, "--run-id", "run:tamper"]);
    assert!(!status.status.success());
    let error = String::from_utf8_lossy(&status.stderr);
    assert!(error.contains("size changed") || error.contains("digest"));
}

#[test]
fn status_does_not_repair_a_damaged_resource_namespace() {
    let state = TestDir::new("read-only-status");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    assert!(
        invoke(&[
            "run",
            "--state",
            state_arg,
            "--suite",
            SUITE,
            "--run-id",
            "run:read-only-status",
            "--logical-now",
            "10",
        ])
        .status
        .success()
    );

    let missing_directory = state.path().join("resources/staging");
    fs::remove_dir(&missing_directory).expect("empty staging directory removes");
    let status = invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:read-only-status",
    ]);
    assert!(!status.status.success());
    assert!(
        !missing_directory.exists(),
        "read-only status must not recreate missing Resource storage"
    );
}

#[test]
fn suite_parser_rejects_duplicate_ids_and_unknown_fields() {
    let duplicate = br#"{"id":"same","input":{"message":"one"},"expected":{"category":"general","urgency":"normal"}}
{"id":"same","input":{"message":"two"},"expected":{"category":"general","urgency":"normal"}}"#;
    assert!(
        cymule_example_durable_evaluation_campaign::parse_suite(duplicate)
            .expect_err("duplicate fails")
            .contains("repeats")
    );
    let unknown = br#"{"id":"one","input":{"message":"one"},"expected":{"category":"general","urgency":"normal"},"surprise":true}"#;
    assert!(
        cymule_example_durable_evaluation_campaign::parse_suite(unknown)
            .expect_err("unknown field fails")
            .contains("unknown field")
    );
    let duplicate_member = br#"{"id":"one","id":"two","input":{"message":"one"},"expected":{"category":"general","urgency":"normal"}}"#;
    assert!(
        cymule_example_durable_evaluation_campaign::parse_suite(duplicate_member)
            .expect_err("duplicate member fails")
            .contains("duplicate JSON object member")
    );
    let nested_duplicate = br#"{"id":"one","input":{"message":"one","message":"two"},"expected":{"category":"general","urgency":"normal"}}"#;
    assert!(
        cymule_example_durable_evaluation_campaign::parse_suite(nested_duplicate)
            .expect_err("nested duplicate member fails")
            .contains("duplicate JSON object member")
    );
    let control_message = br#"{"id":"one","input":{"message":"line one\u000aline two"},"expected":{"category":"general","urgency":"normal"}}"#;
    assert!(
        cymule_example_durable_evaluation_campaign::parse_suite(control_message)
            .expect_err("control character fails")
            .contains("invalid ID or message")
    );
}

#[test]
fn process_plugin_rejects_recursive_duplicate_json_members() {
    let mut child = Command::new(binary())
        .arg("__plugin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("plugin process starts");
    child
        .stdin
        .take()
        .expect("plugin stdin exists")
        .write_all(
            br#"{"type":"call","component":"example.ticket-subject","input":{"id":"one","id":"two","input":{"message":"hello"},"expected":{"category":"general","urgency":"normal"}}}"#,
        )
        .expect("duplicate request writes");
    let output = child.wait_with_output().expect("plugin process exits");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("duplicate JSON object member"));
}

#[cfg(unix)]
#[test]
fn invalid_plugin_outputs_are_terminal_once_and_never_retried_on_reopen() {
    use std::os::unix::fs::PermissionsExt as _;

    for mode in ["wrong-shape", "wrong-semantic"] {
        let state = TestDir::new(mode);
        let state_arg = state.path().to_str().expect("UTF-8 path");
        let plugin = state.path().join(format!("{mode}-plugin.sh"));
        let invocations = state.path().join(format!("{mode}-invocations"));
        let manifest = format!(
            concat!(
                "{{\"type\":\"manifest\",\"manifest\":{{",
                "\"plugin_version\":\"cymule.plugin/3\",",
                "\"implementation_id\":\"example.adversarial-{mode}@1\",",
                "\"components\":{{",
                "\"example.ticket-subject\":{{\"implementation_revision\":\"1\"}},",
                "\"example.ticket-scorer\":{{\"implementation_revision\":\"1\"}}",
                "}},\"effects\":{{}}}}}}"
            ),
            mode = mode,
        );
        let subject = if mode == "wrong-shape" {
            r#"{"type":"call_result","value":{"unexpected":true}}"#
        } else {
            r#"{"type":"call_result","value":{"category":"general","urgency":"normal"}}"#
        };
        let scorer = r#"{"type":"call_result","value":{"policy":"weighted","points":0,"max_points":2,"passed":false}}"#;
        fs::write(
            &plugin,
            format!(
                concat!(
                    "#!/bin/sh\n",
                    "set -eu\n",
                    "count_file={}\n",
                    "request=$(/bin/cat)\n",
                    "count=0\n",
                    "if [ -f \"$count_file\" ]; then IFS= read -r count < \"$count_file\"; fi\n",
                    "count=$((count + 1))\n",
                    "printf '%s\\n' \"$count\" > \"$count_file\"\n",
                    "case \"$request\" in\n",
                    "  *'\"type\":\"describe\"'*) response={} ;;\n",
                    "  *'\"component\":\"example.ticket-subject\"'*) response={} ;;\n",
                    "  *'\"component\":\"example.ticket-scorer\"'*) response={} ;;\n",
                    "  *) exit 64 ;;\n",
                    "esac\n",
                    "printf '%s' \"$response\"\n"
                ),
                shell_quote(&invocations),
                shell_literal(&manifest),
                shell_literal(subject),
                shell_literal(scorer),
            ),
        )
        .expect("adversarial plugin writes");
        let mut permissions = fs::metadata(&plugin)
            .expect("adversarial plugin metadata reads")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&plugin, permissions).expect("adversarial plugin becomes executable");
        let plugin_arg = plugin.to_str().expect("UTF-8 path");
        let run_id = format!("run:{mode}");

        let first = report(&invoke(&[
            "run",
            "--state",
            state_arg,
            "--suite",
            SUITE,
            "--run-id",
            &run_id,
            "--plugin",
            plugin_arg,
            "--plugin-runtime-revision",
            EXTERNAL_PLUGIN_RUNTIME_REVISION,
            "--logical-now",
            "10",
        ]));
        assert_eq!(first.succeeded, 0, "{mode}");
        assert_eq!(first.failed, first.total_cases, "{mode}");
        assert_eq!(first.total_occurrences, first.total_cases, "{mode}");
        assert!(
            first.cases.iter().all(|case| case.error.is_some()),
            "{mode}"
        );
        if mode == "wrong-semantic" {
            assert!(first.cases.iter().all(|case| {
                case.error
                    .as_deref()
                    .is_some_and(|error| error.contains("score policy does not match"))
            }));
        }
        let calls_before_reopen = fs::read_to_string(&invocations)
            .expect("adversarial invocation count reads before reopen");

        let reopened = report(&invoke(&[
            "run",
            "--state",
            state_arg,
            "--run-id",
            &run_id,
            "--plugin",
            plugin_arg,
            "--plugin-runtime-revision",
            EXTERNAL_PLUGIN_RUNTIME_REVISION,
            "--logical-now",
            "20",
        ]));
        assert_eq!(reopened, first, "{mode}");
        assert_eq!(
            fs::read_to_string(&invocations).expect("adversarial invocation count reopens"),
            calls_before_reopen,
            "{mode}"
        );
    }
}

#[test]
fn cli_rejects_ambiguous_or_zero_fault_options() {
    let state = TestDir::new("cli");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let repeated = invoke(&[
        "run", "--state", state_arg, "--state", state_arg, "--suite", SUITE,
    ]);
    assert!(!repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stderr).contains("repeated"));

    let missing_plugin = state.path().join("missing-plugin");
    let repeated_plugin = invoke(&[
        "run",
        "--state",
        state_arg,
        "--plugin",
        missing_plugin.to_str().expect("UTF-8 path"),
        "--plugin",
        missing_plugin.to_str().expect("UTF-8 path"),
    ]);
    assert!(!repeated_plugin.status.success());
    assert!(
        String::from_utf8_lossy(&repeated_plugin.stderr).contains("repeated"),
        "duplicate option admission must precede plugin path I/O"
    );

    let missing_runtime_revision = invoke(&[
        "run",
        "--state",
        state_arg,
        "--plugin",
        missing_plugin.to_str().expect("UTF-8 path"),
    ]);
    assert!(!missing_runtime_revision.status.success());
    assert!(
        String::from_utf8_lossy(&missing_runtime_revision.stderr)
            .contains("--plugin requires --plugin-runtime-revision"),
        "runtime binding admission must precede plugin path I/O"
    );

    let zero = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--simulate-crash-after-claim",
        "0",
    ]);
    assert!(!zero.status.success());
    assert!(String::from_utf8_lossy(&zero.stderr).contains("invalid"));

    let irrelevant = invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:irrelevant-option",
        "--logical-now",
        "10",
    ]);
    assert!(!irrelevant.status.success());
    assert!(String::from_utf8_lossy(&irrelevant.stderr).contains("not valid for status"));
}

#[test]
fn invalid_evolution_policy_fails_before_authority_io() {
    let state = TestDir::new("invalid-evolution-policy");
    let output = invoke(&[
        "evolve",
        "--state",
        state.path().to_str().expect("UTF-8 path"),
        "--run-id",
        "run:invalid-evolution-policy",
        "--policy",
        "unknown",
    ]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("weighted or incompatible"));
    assert!(!state.path().join("campaign.sqlite").exists());
    assert!(!state.path().join("resources").exists());
}

#[test]
fn evolution_requires_complete_campaign_authority() {
    let state = TestDir::new("incomplete-campaign");
    fs::write(state.path().join("resources"), b"not a Resource namespace")
        .expect("Resource path blocker writes");
    let state_arg = state.path().to_str().expect("UTF-8 path");
    let incomplete = invoke(&[
        "run",
        "--state",
        state_arg,
        "--suite",
        SUITE,
        "--run-id",
        "run:incomplete-campaign",
        "--logical-now",
        "10",
    ]);
    assert!(!incomplete.status.success());
    assert!(state.path().join("campaign.sqlite").is_file());

    let evolution = invoke(&[
        "evolve",
        "--state",
        state_arg,
        "--run-id",
        "run:incomplete-campaign",
        "--policy",
        "weighted",
    ]);
    assert!(!evolution.status.success());
    assert!(String::from_utf8_lossy(&evolution.stderr).contains("evaluation region is missing"));
}

#[cfg(unix)]
#[test]
fn suite_symlink_is_rejected() {
    use std::os::unix::fs::symlink;

    let state = TestDir::new("symlink");
    let link = state.path().join("suite-link.jsonl");
    symlink(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/support-tickets.jsonl"),
        &link,
    )
    .expect("symlink creates");
    let output = invoke(&[
        "run",
        "--state",
        state.path().to_str().expect("UTF-8 path"),
        "--suite",
        link.to_str().expect("UTF-8 path"),
        "--run-id",
        "run:symlink",
    ]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("non-symlink"));
}

#[cfg(unix)]
#[test]
fn suite_fifo_and_oversized_file_fail_before_unbounded_import() {
    let state = TestDir::new("bounded-suite");
    let fifo = state.path().join("suite.fifo");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo runs")
            .success()
    );
    let fifo_output = invoke(&[
        "run",
        "--state",
        state.path().to_str().expect("UTF-8 path"),
        "--suite",
        fifo.to_str().expect("UTF-8 path"),
        "--run-id",
        "run:fifo",
    ]);
    assert!(!fifo_output.status.success());
    assert!(String::from_utf8_lossy(&fifo_output.stderr).contains("regular"));

    let oversized = state.path().join("oversized.jsonl");
    fs::write(&oversized, vec![b'x'; 8 * 1024 * 1024 + 1]).expect("oversized suite writes");
    let oversized_output = invoke(&[
        "run",
        "--state",
        state.path().to_str().expect("UTF-8 path"),
        "--suite",
        oversized.to_str().expect("UTF-8 path"),
        "--run-id",
        "run:oversized",
    ]);
    assert!(!oversized_output.status.success());
    assert!(String::from_utf8_lossy(&oversized_output.stderr).contains("exceeds"));
}
