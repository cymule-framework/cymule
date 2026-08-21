//! Black-box campaign, crash, evolution, and integrity tests.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cymule_example_durable_evaluation_campaign::CampaignReport;

const SUITE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/support-tickets.jsonl"
);

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
    serde_json::from_slice(&output.stdout).expect("campaign report decodes")
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
}

#[cfg(unix)]
fn wait_for_content(path: &Path, expected: &[u8], child: &mut std::process::Child) {
    for _ in 0..24_000 {
        match fs::read(path) {
            Ok(bytes) if bytes == expected => return,
            Ok(_) | Err(_) => {}
        }
        assert!(
            child.try_wait().expect("campaign polls").is_none(),
            "campaign completed before the exact crash barrier was published"
        );
        thread::sleep(Duration::from_millis(5));
    }
    panic!("exact crash barrier was not published");
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
        serde_json::from_slice(&evolution.stdout).expect("evolution report decodes");
    assert_eq!(evolution["advanced"], true);
    assert_ne!(evolution["current_plan_id"], old_plan);

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
        serde_json::from_slice(&evolution.stdout).expect("evolution report decodes");
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
                "count=0\n",
                "if [ -f \"$count_file\" ]; then IFS= read -r count < \"$count_file\"; fi\n",
                "count=$((count + 1))\n",
                "count_tmp=\"$count_file.$$\"\n",
                "printf '%s\\n' \"$count\" > \"$count_tmp\"\n",
                "mv \"$count_tmp\" \"$count_file\"\n",
                "if [ \"$count\" -eq 24 ]; then\n",
                "  pid_tmp=\"$pid_file.$$\"\n",
                "  printf '%s\\n' \"$$\" > \"$pid_tmp\"\n",
                "  mv \"$pid_tmp\" \"$pid_file\"\n",
                "  barrier_tmp=\"$barrier_file.$$\"\n",
                "  printf 'before-plugin:24\\n' > \"$barrier_tmp\"\n",
                "  mv \"$barrier_tmp\" \"$barrier_file\"\n",
                "  IFS= read -r _ < \"$gate\"\n",
                "fi\n",
                "exec {} __plugin\n"
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
    let mut running = Command::new(binary())
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
            "--logical-now",
            "10",
            "--lease-ttl",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("campaign process starts");

    wait_for_content(&barrier, b"before-plugin:24\n", &mut running);
    let before_kill = report(&invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:process-kill",
    ]));
    assert_eq!(before_kill.succeeded, 3);
    assert_eq!(before_kill.total_occurrences, 4);
    running.kill().expect("campaign receives an external kill");
    let killed = running.wait().expect("killed campaign reaps");
    assert!(!killed.success());
    let plugin_pid = fs::read_to_string(&plugin_pid)
        .expect("barrier plugin PID reads")
        .trim()
        .parse::<u32>()
        .expect("barrier plugin PID parses");
    let _ = Command::new("/bin/kill")
        .args(["-KILL", &plugin_pid.to_string()])
        .status();

    let reopened = report(&invoke(&[
        "status",
        "--state",
        state_arg,
        "--run-id",
        "run:process-kill",
    ]));
    assert_eq!(reopened.succeeded, before_kill.succeeded);
    assert_eq!(reopened.total_occurrences, 4);
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
        "11",
        "--lease-ttl",
        "1",
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
    let object = fs::read_dir(objects)
        .expect("objects list")
        .next()
        .expect("suite object exists")
        .expect("object entry")
        .path();
    fs::write(object, b"tampered").expect("resource tampers");
    let status = invoke(&["status", "--state", state_arg, "--run-id", "run:tamper"]);
    assert!(!status.status.success());
    let error = String::from_utf8_lossy(&status.stderr);
    assert!(error.contains("size changed") || error.contains("digest"));
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
