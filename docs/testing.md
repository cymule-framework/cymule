# Testing Cymule

Cymule uses evidence families rather than one undifferentiated test command. A
small documentation change should not rebuild four SDKs, while a semantic type
change must prove the Rust transition, frozen wire form, and every language
projection together. The checked-in harness selects the smallest conservative
set and escalates an unknown path to the complete suite.

## Design sources

SQLite demonstrates three practices that matter directly to a durable semantic
framework: independently maintained harnesses; injected out-of-memory, I/O, and
crash failures at successive operation boundaries; and an integrity check after
the injected fault is removed. Its TH3 profiles also separate minimum coverage,
fast regression, debug, configuration-matrix, and soak runs instead of treating
one command as every kind of evidence. See
[How SQLite Is Tested](https://www.sqlite.org/testing.html) and
[TH3](https://www.sqlite.org/th3.html).

DeepSeek Harness keeps fast repository hooks small, asks contributors to select
checks that match the changed behavior, and lets CI own exhaustive coverage and
platform matrices. Its CI separates static, coverage, consumer/artifact,
compatibility, SDK, and platform lanes, while one gate registry owns their exact
commands and prerequisites. See its
[repository guidance](https://github.com/deepseek-ai/deepseek-harness/blob/master/AGENTS.md),
[development guide](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/development.md),
and
[gate registry](https://github.com/deepseek-ai/deepseek-harness/blob/master/scripts/run-gates.ts).

Cymule adopts those testing properties, not either project's implementation.
The harness is Python standard-library orchestration over Cargo, pnpm, uv, Go,
and the MLIR smoke script. It does not replace their runners or put test meaning
inside CI YAML.

## Evidence families

| Family | Proves | Typical trigger |
| --- | --- | --- |
| Rust semantic | transitions, rejection, replay, and documentation compile | one Rust crate and its direct semantic consumers |
| Durable fault | stale CAS, receipt loss, reopen, ambiguity, and atomicity | M1 stores/controllers and M1-backed profiles |
| Frozen protocol | schemas, unknown-field rejection, and Rust-owned IDs | schemas, CLI, fixtures, or public wire types |
| SDK conformance | one real Rust engine contract from a language projection | that SDK alone, or any shared semantic wire change |
| User example | install-shaped success and reconciliation path | CLI, runtime, or example changes |
| Packaging | exact public package contents without publication | TypeScript packaging or release metadata |
| Documentation | repository-local references resolve | authored Markdown or handbook changes |
| Workbench | optional MLIR lowering smoke | compiler workbench changes or a complete run |

The manifest at `tests/harness/suites.toml` is the suite inventory. Commands are
argument arrays, not shell fragments. This keeps their execution auditable and
prevents a changed path from becoming an executable command.

## Daily workflow

Inspect the current worktree against a trusted base:

```sh
python3 scripts/test_harness.py plan --base origin/main
```

Run exactly the reported suites:

```sh
python3 scripts/test_harness.py run rust-virtual protocol sdk-rust sdk-typescript sdk-python sdk-go
```

List all available suites:

```sh
python3 scripts/test_harness.py list
```

Run the release- and profile-complete aggregate:

```sh
./scripts/verify.sh
```

Every execution writes a JSON report under `.cache/test-harness/` with the
exact HEAD, requested and expanded suites, command arguments, exit status, and
duration. CI uploads one report per lane.

CI lanes are static jobs selected by planner outputs, not one matrix job with
conditional setup steps. Each job therefore downloads only the toolchain actions
its suite actually needs; skipped Go, Node, pnpm, or uv setup is not part of an
unrelated lane's failure surface.

## Routing rules

Routing is a conservative union over every changed path:

- documentation-only changes select the meta lane;
- one language SDK selects only that language and its package check where
  applicable;
- a shared schema, fixture, Resource, or M3 wire change selects the frozen
  protocol plus all four SDKs;
- a core/runtime/CLI change selects the Rust workspace, protocol, SDKs, and user
  example;
- verification orchestration, workflow definitions, workspace manifests, and
  any unrecognized path escalate to the complete suite.

The router includes committed, staged, unstaged, and untracked paths locally.
CI uses the event's exact base/head range in a clean checkout when that base is
reachable and has a merge base. A force-push whose prior head is no longer in
the published history selects `full`; it never guesses a smaller range. A
missing route is therefore visible as extra work, not silent missing coverage.
Unit tests pin both narrow routes and the unknown-path escalation behavior.

## Fault-oriented semantic tests

Every durable transition test should state four things: the precondition, the
injected boundary, the allowed post-failure states, and the integrity probe used
after reopen. Use deterministic operation counters to sweep a fault across all
meaningful store, archive, dispatch, and acknowledgement boundaries. For every
counter value, reopen from durable state and assert that the transition is
either wholly absent or wholly committed; ambiguous external effects remain
`unknown` and are reconciled, never redispatched as a new intent.

Multi-worker M3 sweeps the authority windows separately: before claim CAS,
after claim commit before receipt, renewal versus expiry, normal result at
expiry, recovery CAS, recovery receipt loss, and late output after takeover.
Integrity probes compare both the M1 lease map and M3 journal/snapshot; a green
scheduler-only assertion cannot prove atomic ownership.

M1 effect tests separately inject prepare-response loss and durable receipt loss
after enqueue, scope commit, dispatch-start claim, Applied settlement, and
Unknown observation. They count prepare, dispatch, and reconciliation calls and
verify the exact Machine/outbox pair after reopen; a successful Run alone is not
proof that the provider was invoked once.

Fault adapters belong in test support or behind existing substrate interfaces.
Do not add test-only branches to the semantic reducer. Do not use wall-clock
races as correctness evidence when an explicit barrier, counter, epoch, or CAS
revision can identify the same interleaving. Seeded fuzz/property cases must
print the seed and minimize a failure into a permanent regression fixture.

When original history is compacted, the integrity probe also verifies the
certificate digest, retained obligations and bindings, declared replay
availability, and a partial rehydration round trip. A summary is never accepted
as a replacement source of canonical truth merely because it deserializes.

## Test depth

Three depths serve different feedback loops:

1. **Focused** runs the routed suites while editing and before an ordinary
   scoped commit.
2. **Complete** runs `full` for semantic version changes, profile claims,
   schemas shared by multiple SDKs, publication/release changes, harness or CI
   changes, and any unknown route.
3. **Soak** repeats deterministic fault sweeps, seeded generators, and larger
   configuration matrices. It is a release/nightly concern once those suites
   exist; it must not make focused feedback unusable.

Coverage percentage is supporting evidence, not the test taxonomy. Branch or
mutation coverage is valuable for the small Rust semantic kernel, but a profile
is complete only when its stated fault family, cross-language contract, reopen,
and replay properties pass.
