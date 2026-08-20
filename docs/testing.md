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
[repository guidance](https://github.com/deepseek-ai/deepseek-harness/blob/99f6f02fecdb7dff40c3fbc9470f5907c29f74ca/AGENTS.md),
[testing policy](https://github.com/deepseek-ai/deepseek-harness/blob/99f6f02fecdb7dff40c3fbc9470f5907c29f74ca/docs/testing.md),
and
[gate registry](https://github.com/deepseek-ai/deepseek-harness/blob/99f6f02fecdb7dff40c3fbc9470f5907c29f74ca/scripts/run-gates.ts).

The SQLite lesson is independence, not raw test count. Its development subset,
public-interface harness, differential SQL Logic Test, fuzzers, anomaly VFS,
coverage/mutation runs, and release soak can disagree independently. Cymule
therefore keeps semantic reduction, durable anomalies, frozen protocols,
language projections, built user paths, coverage, mutation, and platform
portability as separate witnesses. A green aggregate cannot erase a failure in
one witness.

The DeepSeek Harness lesson is ownership and dependency-aware scheduling. Its
package tests stay with the capability they exercise, local hooks remain small,
the checked-in gate graph owns prerequisites, and CI owns exhaustive/platform
work. Cymule keeps both suite definitions and path ownership in
`tests/harness/suites.toml`; CI YAML only installs the tools selected by that
catalog and runs the reported leaf suites.

Cymule adopts those testing properties, not either project's implementation.
The harness is Python standard-library orchestration over Cargo, pnpm, uv, Go,
and the MLIR smoke script. It does not replace their runners or put test meaning
inside CI YAML.

DeepSeek Harness's "everything is a plugin" boundary is also useful as a test
ownership rule: a plugin proves its own domain vocabulary and lifecycle against
published framework seams. Its tests do not make Session, stream, Agent Loop,
ACP, model, editor, or provider behavior part of Cymule core conformance.

## Evidence topology

Cymule deliberately uses several independent witnesses instead of one giant
runner:

| Witness | Boundary under test | Runner |
| --- | --- | --- |
| Semantic reducer | closed Rust commands, events, identities, replay | Cargo unit, integration, and property tests |
| Durable anomaly harness | CAS, reopen, receipt loss, unknown effects, fencing | deterministic Rust fault adapters |
| Protocol verifier | frozen JSON and rejection of malformed/unknown fields | Rust CLI plus Python JSON Schema validation |
| Cross-language differential | one Rust-sealed identity and execution result | native Rust, TypeScript, Python, and Go SDK tests |
| Black-box user path | built engine plus a real process or embedded plugin | Hello World plus durable campaign crash/evolution tests |
| Plugin conformance | only the optional capability's public seam | one leaf suite per plugin |

Cross-language differential also validates the unified
`cymule.live-evolution-control/1` fixture in Rust, TypeScript, Python, and Go.
The same leaf runs a shared negative fixture through the real Rust Engine and
compares failure category, phase, code, and explicitly justified retry
disposition. It never parses stderr to recover semantic meaning.
Stateful Rust tests separately prove that transitive registry relinking, Plan
DAG edges, rollout decisions, occurrence pins, and virtual worker claims share
the intended single-domain CAS boundaries.

The suite inventory is a dependency graph, not a checklist that every edit must
run. A leaf change runs its owner. A shared interface runs that owner and direct
consumers. A frozen semantic or wire change runs all affected projections. An
unknown path fails closed to `full`. Independent evidence can fail, run, and
evolve without coupling unrelated toolchains.

Every leaf also has one execution class in the same manifest:

| Class | Meaning | Isolation rule |
| --- | --- | --- |
| `deterministic` | replayable in-process, compiler, packaging, or static evidence | no external service is authority |
| `live_process` | a built child or real process-death boundary is part of the witness | use an explicit barrier and always reap the child |
| `live_provider` | a concrete adapter or provider boundary is exercised | keep provider setup and failures outside semantic suites |

The harness rejects an unclassified or multiply classified leaf and carries the
class into list, CI matrix, and JSON execution reports. This prevents a fast
deterministic regression from silently becoming dependent on process timing or
provider availability.

Reports use four distinct terminal states: `passed`, `failed`, `skipped`, and
`infrastructure_error`. Optional tools signal skip with exit code 77; missing
commands, spawn failures, and runner faults can never be serialized as a pass.
Any Cargo package source or manifest change also compiles the complete workspace
with all targets, while owner-specific routes retain the narrower behavioral
tests. This is a reverse-dependency compile witness, not a reason to run every
behavioral suite for every Rust edit.

## Evidence families

| Family | Proves | Typical trigger |
| --- | --- | --- |
| Rust semantic | transitions, rejection, replay, and documentation compile | one Rust crate and its direct semantic consumers |
| Durable fault | stale CAS, receipt loss, reopen, ambiguity, and atomicity | M1 stores/controllers and M1-backed profiles |
| Frozen protocol | schemas, unknown-field rejection, and Rust-owned IDs | schemas, CLI, fixtures, or public wire types |
| SDK conformance | one real Rust engine contract from a language projection | that SDK alone, or any shared semantic wire change |
| User example | install-shaped success, reconciliation, crash recovery, Resource integrity, and future-only evolution | CLI, runtime, or example changes |
| Packaging | exact public package contents without publication | TypeScript packaging or release metadata |
| Documentation | repository-local references resolve | authored Markdown or handbook changes |
| Workbench | optional MLIR lowering smoke | compiler workbench changes or a complete run |
| Coverage analysis | aggregate line/region non-regression signal | scheduled/manual semantic analysis |
| Mutation analysis | whether core tests detect injected wrong semantics | scheduled/manual `cymule-core` analysis |
| Platform portability | filesystem CAS and recovery across supported hosts | scheduled/manual Linux/macOS matrix |

The manifest at `tests/harness/suites.toml` is the suite and route inventory.
Commands are argument arrays, not shell fragments. The harness validates every
route target against that catalog before selection. This keeps execution
auditable, removes a second hard-coded routing authority, and prevents a changed
path from becoming an executable command.

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

Run generated causal replay cases and repeat the highest-risk deterministic
fault sweeps independently:

```sh
PROPTEST_CASES=16384 CYMULE_SOAK_REPETITIONS=5 \
  python3 scripts/test_harness.py run rust-soak
```

The public GitHub repository also runs that leaf suite weekly. Soak is not part
of a normal routed commit because repetition is a different kind of evidence,
not a reason to delay feedback from a local SDK or documentation change.
The repeated deterministic sweep includes the high-risk day-one plugin
boundaries: non-blocking SQLite contention, exact filesystem/object-store chunk
retry, ack-coupled HTTP/timers, restart-monotonic clock observation, process
timeout ambiguity, and MCP incomplete work that must remain caller-driven.

Coverage, mutation, and platform portability are independent scheduled/manual
workflows:

```sh
python3 scripts/test_harness.py run rust-coverage
python3 scripts/test_harness.py run rust-coverage-plugins
python3 scripts/test_harness.py run rust-mutation
python3 scripts/test_harness.py run rust-mutation-evolution-m4
python3 scripts/test_harness.py run rust-mutation-plugins
python3 scripts/test_harness.py run rust-portability
```

Coverage currently gates the measured four-crate semantic baseline at 72% line
and 78% region coverage. These are non-regression floors, not a claim that a
percentage proves correctness. Core mutation uses its independent
public-interface conformance tests. A separate M4 mutation witness targets
compatibility analysis, safe-point proofs, automatic relink admission, and
replacement-Run authorization so changes in those higher-profile laws cannot
hide behind core coverage. The scheduled workflow partitions core across eight
parallel zero-based `K/N` shards and the smaller M4 surface across four; each
copies the existing target into its scratch tree for incremental rebuilds.
Day-one plugin coverage is a separate witness with 72% line and 72% region
floors. It cannot raise or lower the semantic baseline and is not inferred from
core tests that happen to compile plugin dependencies.
Plugin mutation is independently sharded and bounded to the admission and
ambiguity functions whose failure could acknowledge the wrong state: SQLite
CAS, HTTP/timer acknowledgement, process execution limits, and MCP result
mapping. Resource streaming uses deterministic fault/soak and coverage evidence
until its larger mutation set is separately partitioned.
Portability repeats only core, durable, and directory-store witnesses on Linux
and macOS; it does not multiply every SDK lane by every operating system. The
catalog invokes its shell leaves through explicit `bash` so the command remains
an auditable argument array on both supported hosts.

The analysis scripts pin
[`cargo-llvm-cov` 0.9.0](https://github.com/taiki-e/cargo-llvm-cov/tree/v0.9.0)
and
[`cargo-mutants` 27.1.0](https://github.com/sourcefrog/cargo-mutants/tree/v27.1.0).
Tool absence or version drift is a visible failure; analysis never silently
skips.

Every execution writes a JSON report under `.cache/test-harness/` with the
exact HEAD, requested and expanded suites, command arguments, exit status, and
duration. CI also uploads the exact base/head/path/evidence routing plan before
any lane starts, then uploads one execution report per selected lane.

CI lanes are static jobs selected by planner outputs, not one matrix job with
conditional setup steps. Each job therefore downloads only the toolchain actions
its suite actually needs; skipped Go, Node, pnpm, or uv setup is not part of an
unrelated lane's failure surface.

Rust uses five independently reported lanes: workspace static/consumer compile,
semantic profiles, durable and live-process profiles, provider plugins, and
release-package bytes. `full` composes those leaves without first running one
duplicate workspace-wide behavioral suite. A failure therefore preserves the
other evidence instead of hiding it behind one long Rust job.

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

Compound anomaly tests inject a second failure during recovery from the first.
The current effect case loses the provider response after application, commits
`Unknown` during reopen, loses that durable acknowledgement, reopens again, and
proves one dispatch plus one reconciliation. This is separate from repeating a
single fault window.

Black-box process-kill campaigns remain separate from adapter fault injection.
The durable evaluation witness observes a committed M1/M3 projection through a
read-only SQLite connection, kills the writer process externally, reopens the
same Resource and virtual journal, explicitly recovers an expired claim when
present, and proves one terminal result per logical case. Store wrappers in
integration tests additionally place an external barrier immediately before or
after a selected real CAS, then the parent sends `SIGKILL`. This selects narrow
OS-death windows without adding a test branch to the reducer.

The M1 Run sweep treats every segmented head CAS as two distinct anomaly points:
failure before the write and lost acknowledgement after a successful write. It
first discovers the boundary count from a successful execution, injects one
fault at each position, disables the fault, reopens through the public durable
interface, and runs the same integrity probe. This permanently regresses the
split initialization window where a Run could previously become durable without
its first Continuation.

The production SQLite witness repeats that automatically discovered boundary
set with real child-process death on both CAS sides. A separate SQLite provider
ledger counts dispatch and reconciliation independently of Cymule state. The
filesystem Resource witness likewise kills after a retained chunk and after
publication. M4 kills both sides of unified publication, and the optional Agent
suite partitions occurrence, Session, and stream journal death from its
host-kind failure/refusal matrix.

Run that matrix independently for nested commit-gated, eager observational, and
explicit-release effects. Nested tests inspect the child scope on both sides of
commit. Eager tests prove settlement can precede root commit and that the bound
result survives reopen. Explicit tests prove ordinary resume cannot dispatch,
then lose claim or settlement receipts after the caller release and replay the
same terminal Result.

Wait-source tests rebuild the parked index from durable authority, select
within a deterministic hard bound, and reject targets under another source.
Inject acknowledgement loss after activation CAS, reopen, redeliver the exact
delivery, and assert one retained activation plus one later acknowledgement.

Fault adapters belong in test support or behind existing substrate interfaces.
Do not add test-only branches to the semantic reducer. Do not use wall-clock
races as correctness evidence when an explicit barrier, counter, epoch, or CAS
revision can identify the same interleaving. Seeded fuzz/property cases must
print the seed and minimize a failure into a permanent regression fixture.

Shared deterministic composition lives in the unpublished
`cymule-test-world` workspace crate because durable, SQLite, and Agent suites
all consume it. One owned `TestWorld` combines a logical clock, seeded random
source, identified fault schedule, recording observer, temporary durable-domain
root, and managed child lifecycle. None of those values enters production code,
uses global mutation, or creates a second authority.

The durable model trace generates the public `DurableCommand` sequence and the
underlying CAS fault plan once in Rust, reopens after each injected failure, and
checks every response against a small Run/domain model. A failure contains the
seed, retained original command indexes, a copy-paste Cargo replay command, and
a minimized `cymule.test-trace/1` JSON document. That document can be promoted
to `tests/fixtures/` for all SDKs; TypeScript, Python, and Go never implement
their own command generator or shrinker.

The core Proptest suite uses an explicit crate-local failure-persistence path
because integration tests have no discoverable `lib.rs` beside their source.
Generated `proptest-regressions/semantic_kernel.txt` cases are committed with
the fix and replay before new cases in focused, soak, mutation, and CI runs.

When original history is compacted, the integrity probe also verifies the
certificate digest, retained obligations and bindings, declared replay
availability, and a partial rehydration round trip. A summary is never accepted
as a replacement source of canonical truth merely because it deserializes.

Concrete durable adapters also test the bytes outside their normal write path.
The directory store is reopened after truncated JSON and after a valid envelope
whose revision digest was replaced; both must fail closed before exposing a
partial `DurableState`.

## Test depth

Three depths serve different feedback loops:

1. **Focused** runs the routed suites while editing and before an ordinary
   scoped commit.
2. **Complete** runs `full` for semantic version changes, profile claims,
   schemas shared by multiple SDKs, publication/release changes, harness or CI
   changes, and any unknown route.
3. **Soak** repeats deterministic fault sweeps, seeded generators, and larger
   configuration matrices in the independent `rust-soak` suite. It is a
   scheduled/release concern and must not make focused feedback unusable.
4. **Analysis** measures coverage, mutation sensitivity, and tier-one platform
   portability on independent schedules. It is neither a pre-commit hook nor a
   reason to run unrelated SDKs after every Rust edit.

Coverage percentage is supporting evidence, not the test taxonomy. Branch or
mutation coverage is valuable for the small Rust semantic kernel, but a profile
is complete only when its stated fault family, cross-language contract, reopen,
and replay properties pass.

Miri is not a current default witness because the trusted Rust crates contain no
`unsafe` code. Add it when unsafe implementation enters scope; until then,
mutation sensitivity and external durable-byte faults provide more relevant
evidence per unit of CI time. See the
[official Miri project](https://github.com/rust-lang/miri) for the trigger and
supported undefined-behavior checks.

Mutation exclusions are narrow and reviewed in `.cargo/mutants.toml`.
`Machine::verify_replay` is excluded because all state required to manufacture
a live-projection/event divergence is private and changed only by the admitted
reducer; mutating the integrity checker itself is observationally equivalent
through public APIs. Adding a test-only corruption path would weaken the core
for the benefit of the tool. No transition, validation, identity, or persistence
mutation is excluded.
