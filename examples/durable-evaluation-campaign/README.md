# Durable Evaluation Campaign

This example runs a reproducible evaluation suite as durable, versioned work.
It is intentionally more substantial than Hello World: you can stop the
process, resume from SQLite, change the scorer for future cases, and inspect
which immutable Plan evaluated every result.

The bundled workload classifies support tickets so the example stays fast,
credential-free, and deterministic. The architecture is the useful part. A
real subject can be a model gateway, an MCP-backed Agent, a script, a sandbox,
or a remote service behind the same component plugin boundary. Cymule does not
own its internal loop.

## Run the complete feature tour

For the shortest path, run one command from the repository root:

```sh
cargo run -p cymule-example-durable-evaluation-campaign -- demo
```

The tour starts child processes for the crash, evolution, and resume phases. It
prints a compact five-line proof and the temporary state directory that remains
available for inspection. To choose that directory yourself, it must not
already exist:

```sh
cargo run -p cymule-example-durable-evaluation-campaign -- \
  demo --state /tmp/cymule-feature-tour
```

## Run the campaign

From the repository root:

```sh
cargo build -p cymule-example-durable-evaluation-campaign

CAMPAIGN_STATE=$(mktemp -d)

./target/debug/cymule-example-durable-evaluation-campaign run \
  --state "$CAMPAIGN_STATE" \
  --suite examples/durable-evaluation-campaign/fixtures/support-tickets.jsonl \
  --run-id run:support-evaluation
```

The JSON report includes the location-independent suite Resource ID, the Plan
selected for future work, aggregate points, and one terminal record per case:

```json
{
  "run_id": "run:support-evaluation",
  "suite_resource_id": "sha256:...",
  "current_plan_id": "sha256:...",
  "total_cases": 12,
  "total_occurrences": 12,
  "recovered_attempts": 0,
  "succeeded": 12,
  "failed": 0,
  "points": 24,
  "max_points": 24,
  "cases": [
    {
      "case_id": "account-locked",
      "occurrence_id": "sha256:...",
      "plan_id": "sha256:...",
      "state": "succeeded",
      "output": {
        "prediction": { "category": "identity", "urgency": "normal" },
        "score": { "policy": "strict", "points": 2, "max_points": 2, "passed": true }
      }
    }
  ]
}
```

Read the same durable projection without executing work:

```sh
./target/debug/cymule-example-durable-evaluation-campaign status \
  --state "$CAMPAIGN_STATE" \
  --run-id run:support-evaluation
```

`status` re-verifies the retained suite bytes against their Resource Handle.
Supplying `--suite FILE` additionally proves that a local file is byte-for-byte
the suite originally pinned by this campaign.

## See crash recovery and live evolution

Start a fresh campaign and terminate the process after three terminal result
checkpoints. Exit code `75` is deliberate:

```sh
EVOLUTION_STATE=$(mktemp -d)

./target/debug/cymule-example-durable-evaluation-campaign run \
  --state "$EVOLUTION_STATE" \
  --suite examples/durable-evaluation-campaign/fixtures/support-tickets.jsonl \
  --run-id run:evolving-evaluation \
  --simulate-crash-after-commit 3 || test "$?" -eq 75
```

Publish a compatible scorer revision:

```sh
./target/debug/cymule-example-durable-evaluation-campaign evolve \
  --state "$EVOLUTION_STATE" \
  --run-id run:evolving-evaluation \
  --policy weighted
```

Resume with the same suite and Run identity:

```sh
./target/debug/cymule-example-durable-evaluation-campaign run \
  --state "$EVOLUTION_STATE" \
  --suite examples/durable-evaluation-campaign/fixtures/support-tickets.jsonl \
  --run-id run:evolving-evaluation
```

The first three cases retain the strict scorer's Plan ID. Later cases use the
new weighted scorer and a new Plan ID. `latest_compatible` therefore behaves as
an automatic default for future work, not a mutable pointer inside history.

Try a revision with a changed input contract:

```sh
./target/debug/cymule-example-durable-evaluation-campaign evolve \
  --state "$EVOLUTION_STATE" \
  --run-id run:evolving-evaluation \
  --policy incompatible
```

The revision is retained, but `advanced` is `false`; compatibility admission
keeps the previous future-default Plan.

## What is actually exercised

| Concern | Cymule boundary | Local realization in this example |
| --- | --- | --- |
| Durable authority | complete-state CAS and application journals | `cymule-store-sqlite` |
| Suite and artifacts | content-verified Resource Handle | `cymule-resource-fs` |
| Large case space | bounded M3 cursor and ready frontier | pages of 16, frontier of 32 |
| Worker ownership | CAS-backed capacity-slot lease and attempt epoch | one process worker slot |
| Subject and scorer | abstract component plugin calls | bounded child process |
| Scorer evolution | reusable definition with `latest_compatible` | strict to weighted policy |
| Historical replay | occurrence-bound exact Plan and result Artifacts | retained in SQLite state |

The scheduler never loads more than its configured frontier, even though this
small fixture finishes in one page. Suite input is capped at 8 MiB and 100,000
cases for this local example. Those are application safety limits, not Cymule
semantic limits.

## Replace the deterministic subject

The binary launches itself in process-plugin mode only to make the default
experience self-contained. To integrate a real evaluator, keep the published
component contracts and point `ProcessExecutorConfig` at another absolute
executable implementing `cymule.plugin/1`:

- `example.ticket-subject` receives one typed case and returns a prediction;
- `example.ticket-scorer` receives the case, prediction, and Plan-pinned policy;
- `Describe` returns immutable implementation and operation revisions.

For a different domain, replace the fixture types and component names in this
example application. The durable store, Resource, scheduler, lease, occurrence,
and evolution interfaces do not depend on tickets or on any Agent protocol.

The bundled subject is pure and safe to invoke again if a worker dies before a
result checkpoint. Do not put a mutating operation behind this component and
assume retries are safe. Model real world mutations as Cymule Effects with a
provider idempotency key and authoritative reconciliation.

## Failure drills and limits

The focused test suite exercises:

```sh
cargo test -p cymule-example-durable-evaluation-campaign
```

It covers process exit after a committed result, expiry and explicit recovery
after exit with an active claim, refusal to steal an unexpired claim, changed
suite bytes, retained Resource tampering, duplicate case IDs, unknown fields,
symlink input, compatible future-only evolution, and incompatible-update
blocking. See [ADVERSARIAL_REVIEW.md](ADVERSARIAL_REVIEW.md) for the reviewed
failure model and remaining boundaries.

This is a single-domain reference application. It does not claim distributed
consensus, remote failover, untrusted-code isolation, or provider-level exactly
once behavior. The process executor enforces argument, environment, timeout,
and message bounds; it is not a sandbox.
