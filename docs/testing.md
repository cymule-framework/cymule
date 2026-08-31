# Testing Cymule

Cymule uses evidence families rather than one undifferentiated test command. A
small documentation change should not rebuild four SDKs, while a semantic type
change must prove the Rust transition, frozen wire form, and every language
projection together. The checked-in harness selects the smallest conservative
set and escalates an unknown path to the complete suite.

Implementation status: the terminal Engine v5/live-evolution commit matrix and
complete branch-wide catalog must be rerun after final source freeze. No prior
partial run is current evidence for version authority, SDKs, process death,
packages, release workflows, or MLIR.

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
`cymule.live-evolution-control/6` fixture in Rust, TypeScript, Python, and Go.
Before any operation-specific assertion, each SDK compares the success
envelope's complete inner request with the exact strict JSON value it serialized
and sent. The shared matrix covers every Engine request variant, an echo changed
one field at a time, missing/duplicate/unknown echo fields, and the former v4
response-only success. It separately changes omitted optional members to
explicit `null` so typed decoding cannot erase a wire mismatch. Failure fixtures
intentionally omit request and reject one as unknown because failure can precede
strict decoding. The matrix does not
recompute a Rust Plan, Resource, Clock, durable-control, or rollout result to
manufacture correlation. Invalid echo on mutating requests must produce
`unknown_world_outcome/reconcile`; read-only validation produces an invalid
response without replay permission.
Rust transport tests also inject explicit `null` into omission-only request,
success-response, and nested failure-issue members, then prove typed
reserialization detects the erased presence before execution or response
admission. A required-nullable Run query result remains a positive control.
Each successful Engine v5 `execute_live_evolution` response must be one
`EvolutionCommit`. Its observed revision is a valid StateRoot identity, its
`committed_revision` member is present with either an exact revision or `null`,
and its semantic receipt matches the exact `evolution_id` and complete command
serialized in the echoed request. The negative matrix independently changes the
outer partition, command identity and body, parent current, Durable source
witness, normalized write descriptor, publication evidence/mode, rollout
decision, observation, and outcome. Because the request may already have
committed, every malformed or mismatched success is classified as
`unknown_world_outcome` with `reconcile`; it is never exposed as an
uncorrelated success.
The same leaf runs a shared negative fixture through the real Rust Engine and
compares failure category, phase, code, and explicitly justified retry
disposition. It never parses stderr to recover semantic meaning.
Transport conformance also sends a legal success above 16 MiB, preserves a
fully validated failure from a nonzero process exit, rejects the complete
128 MiB plus 32-byte framing bound plus one on stdout and byte 1 MiB plus one on stderr, and exact-rejects distinct
fractional decimals that collide in a host binary float. Custom transport tests
change the complete accepted request echo rather than only an operation field.
An exponent-normalized request whose echo expands past a test bound must reject
before Store creation, proving normalized echo charging precedes dispatch.
Stateful Rust tests separately prove that transitive registry relinking, Plan
DAG edges, rollout decisions, occurrence pins, and virtual worker claims share
the intended single-domain CAS boundaries. Profile reducer tests start with one
bounded exact source view and request missing membership/non-membership proofs
explicitly. They cover source and postcondition count/byte max and max-plus-one,
all-ever command replay, historical definition/Plan/link/edge reuse,
source-decision-derived rollout identities, and evidence isolation through an
A1 -> A2 -> A1 -> A2 cycle. Edge and rollout-transition tests mutate every
retained identity input, reject their superseded `/1` generations, and reject
missing required DTO members. Virtual claim-outcome tests independently reject
NoWork carrying a Plan, Claimed omitting its claim or Plan, and Plan, claim,
Clock, or execution-binding substitution against the normalized receipt.
Durable tests separately prove that each closed
postcondition publishes its scalar current,
aliases, receipt, normalized leaves, Plans, Artifacts, and coupled M1/M3
mutations in one CAS; a shape-valid digest cannot delete or mutate an unrelated
retained family.

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
| Release security | immutable Action pins, exact-SHA admission, independent no-OIDC byte closure, minimal terminal upload, digest/provenance closure, and whole-history mirror separation | workflows, release scripts, version authorities, or private mirror control |
| Documentation | repository-local references resolve | authored Markdown or handbook changes |
| Workbench | optional MLIR lowering smoke | compiler workbench changes or a complete run |
| Coverage analysis | aggregate line/region non-regression signal | scheduled/manual semantic analysis |
| Mutation analysis | whether core tests detect injected wrong semantics | scheduled/manual `cymule-core` analysis |
| Platform portability | filesystem CAS and recovery across supported hosts, plus the explicit non-Unix DirectoryStore boundary | scheduled/manual Linux/macOS matrix and native Windows witness |

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

The portability harness runs the portable core, durable, and DirectoryStore
suites on Linux and macOS. The same scheduled/manual Compatibility workflow has
an independent native `windows-2025` PowerShell job which installs the pinned
MSVC Rust toolchain and executes the exact cfg(non-Unix) DirectoryStore unit.
That job verifies one test actually passed, so a filtered zero-test run cannot
stand in for non-Unix behavior. Neither portability path is Required CI.

Coverage currently gates the measured four-crate semantic baseline at 72% line
and 78% region coverage. These are non-regression floors, not a claim that a
percentage proves correctness. The M4 reducer has its own non-averaged
profile-protocol artifact and 63% line / 64% region floors; the report includes
only `src/evolution.rs` and `src/evolution/**`, so Evolution host or another
profile cannot mask it. Core mutation uses its independent public-interface
conformance tests. A separate M4 mutation witness targets
the `cymule-profile-protocol::evolution` reducer's compatibility analysis,
safe-point proofs, migration target-binding/M1-sidecar closure, automatic
relink/edge admission, rollout-transition identity, and replacement-Run
authorization. It requires a nonempty inventory containing
each named law before executing, so moving the reducer cannot silently turn the
gate into an empty pass. Process-wire conformance stays in the independent
`cymule-evolution` leaf. The scheduled workflow partitions core across eight
parallel zero-based `K/N` shards and the bounded M4 reducer surface across four;
each copies the existing target into its scratch tree for incremental rebuilds.
Day-one plugin coverage is a separate witness with 72% line and 72% region
floors. It cannot raise or lower the semantic baseline and is not inferred from
core tests that happen to compile plugin dependencies.
M4 quiescence faults prepare an Effect, park and activate a wait, then attempt
both migration and replacement authorization. Pending, claimed, and unknown
outbox states, unresolved Machine obligations, pending waits, active Attempts,
effect claim leases, and a moved store head must fail before adapter/target work
or at the final CAS. Origin-routing cases delete the target Effect, bind a
same-named target provider, and remove the historical handler; the current
provider call count remains zero and unavailable/governance retains the old
world outcome and obligation.
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
unrelated lane's failure surface. Independently of the narrow path selection,
every plan attaches the deterministic `version-domain-source` lane. It validates
the complete public-candidate snapshot and registry closure without converting
that path selection into `full`.

Rust uses five independently reported lanes: workspace static/consumer compile,
semantic profiles, durable and live-process profiles, provider plugins, and
release-package bytes. `full` composes those leaves without first running one
duplicate workspace-wide behavioral suite. A failure therefore preserves the
other evidence instead of hiding it behind one long Rust job.

SDK transport and the self-hosting campaign build executable fixtures with the
workspace `conformance` Cargo profile. It preserves dev/test behavior while
stripping debug-symbol bulk and disabling incremental state. The executor still
captures and charges the exact artifact against the unchanged 64 MiB SDK or
128 MiB campaign closure budget; CI host debug formats never redefine protocol
capacity.

The durable campaign is a separate single-threaded Cargo invocation. Each case
already creates concurrent process occurrences internally; running cases in
parallel tests shared-host I/O saturation rather than the declared recovery and
evolution semantics.

## Routing rules

Routing is a conservative union over every changed path:

- documentation-only changes select the meta lane, plus the mandatory
  version-domain source-closure lane shared by every plan;
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

Wide failure and cancellation tests discover the real Core Begin, Progress and
Finalize page sequence, then inject before-CAS failure and lost acknowledgement
at every recoverable page. They inspect the hidden Run-local outbox roots, Core
and provider Attempts, pending transition count, Continuation fence, and exact
provider call count after reopen. Separate cases commit another Run between
every source page and return a late same-Run result, proving that the terminal
transition neither replaces unrelated roots nor accepts a material-only sidecar.

Agent Context tests retain an old `(message_head,message_count)` prefix, append
new messages, and read the old prefix at a newer StateRoot revision with page
sizes one and 256. They compare complete `AgentMessageCurrent` sequences and
canonical-byte sums, independently exercise the complete page-wire budget, and
corrupt or remove a reachable value in Memory, Directory, and SQLite stores.
Recovery tests use a valid source descriptor and a real older message reference
which the original reader did not deliver; unresolved Completed must remain
Unknown with no second write, while terminal completion replay and NotApplied
remain valid.

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

The M1 Run sweep treats every StateRoot head CAS as two distinct anomaly points:
failure before the write and lost acknowledgement after a successful write. It
first discovers the boundary count from a successful execution, injects one
fault at each position, disables the fault, reopens through the public durable
interface, and runs the same integrity probe. This permanently regresses the
split initialization window where a Run could previously become durable without
its first Continuation.

The StateRoot complexity witness appends 256 fixed-size typed journal records.
It asserts one initial projection load, one new journal leaf per append, and a
history-independent bound on copied persistent map/log nodes. Exact historical
lookups traverse rooted paths without materializing cumulative history; explicit
GC streams a separately bounded physical inventory.

Reopen follows only IDs reachable from one observed head. Directory tests add
malformed unrelated immutable files and interrupted `.next` files; SQLite tests
add malformed unrelated rows. Neither may be decoded as authority. SQLite holds
one deferred read transaction from head observation through reachable lineage
validation, so a concurrent commit cannot splice two revisions into one reopen.

Directory GC has a separate internal process-death sweep. A child is stopped
with `SIGKILL` after the new GC receipt is durable, on both sides of head
publication, and after deletion starts or becomes directory-durable. Ordinary
`load()` must reopen the old head before publication or the new head afterward
and reconcile only that head's pinned deletion page. It never selects the next
page; only an explicit advance may publish another generation. The test hook is
compiled only into the crate's unit-test artifact; production reducer and
adapter builds have no pause or crash switch.

The production SQLite witness repeats that automatically discovered boundary
set with real child-process death on both CAS sides. A separate SQLite provider
ledger counts dispatch and reconciliation independently of Cymule state. Every
reopen runs `PRAGMA integrity_check`, checkpoints the WAL, and runs the integrity
probe again. The filesystem Resource witness likewise kills after a retained
chunk and after publication, then verifies the exact content digest. Its upload
record is the durable chunk frontier: bytes are synced before the frontier
advances, and reopen discards only a suffix that was never acknowledged.
SQLite provider tests also inject aborting triggers at immutable-object insert
and head update to prove the enclosing SQL transaction rolls back every row.
This reaches SQLite statement and transaction boundaries, not VFS I/O.

HTTP and timer sources own independent live-process leaves. Each kills a child
after ingress or schedule persistence, after target selection, on both M1
activation-CAS sides, and on both source-acknowledgement sides. Reopen must
redeliver the identical source delivery before acknowledgement, retain exactly
one activation, stop redelivery after acknowledgement, replay the Machine, and
resume the Run. The clock leaf kills before and after `observe`; an observation
whose caller receipt was lost still forces the next value to advance after
reopen and backward wall-clock movement. M4 kills both sides of its one typed
StateRoot CAS. The independent deterministic control matrix compares the
retained semantic receipt, scalar current, exact normalized writes, introduced
Plans and Artifacts, and coupled M1/M3 state across every outcome. It then
advances current state and resolves the historical command through the exact
all-ever alias, proving that the original outcome returns without provider
invocation or obsolete source revalidation. Migration, restart, and shadow
counters must not advance. A same-ID different-command probe must conflict
before those counters advance. Template registration separately proves
complete-content conflict and preservation of the initially linked Plan after
later relinking. The optional Agent suite discovers occurrence, Session, and
stream CAS counts from successful baselines before killing every boundary.
After each Agent kill it runs full SQLite integrity checks and a WAL checkpoint
before semantic recovery, repeats both after recovery, and applies the same
WAL/synchronous-full substrate to the host ledger used as reconciliation
evidence.

These are process-death and public-operation-boundary claims, not an emulated
power-loss claim. `SIGKILL` leaves the kernel page cache intact. Deterministic
SQLite `xWrite`/`xSync`/WAL/checkpoint fault injection and reordered or torn
unsynced filesystem images require an independent pinned test VFS or disk model;
the current safe `rusqlite` adapter exposes no mature faithful seam for that
matrix. That evidence family is therefore unsupported today and is never
reported as passed or inferred from `FULL` synchronous mode, `fsync`, a
temporary directory, or API-side process death.

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
For a persisted broadcast selection, cancel one target before admission and
prove the remaining pending target is the sole applied winner, the cancelled
target remains a terminal nonwinner, the source acknowledges, and reopen does
not redeliver the completed delivery.

Execution-ownership tests open two independent coordinators over the same
store. They prove that an active claim makes the second ordinary resume Busy
before provider I/O; merely observing expiry changes no revision; explicit
takeover pins the exact old fence and a receipt resolved from the selected
Clock source generation's current scope head; a Ready resume presented with an
older still-resolvable receipt fails before claim CAS and provider I/O while the
latest receipt succeeds. A second real SQLite Clock must also be unable to
advance that scope after validation but before the Store CAS: the first Clock's
non-blocking guard encloses the CAS, and a deliberately stale callback is never
invoked. The
semantic component occurrence stays constant while a later provider Attempt is
created; and both late old-fence output and stale result CAS lose. Lost
acknowledgement after result commit reopens the one completed occurrence without
another provider call.

The SQLite ownership witness repeats the critical handoff with two real child
processes and one issued-receipt Clock ledger. Only the Ready-claim winner may
reach the provider marker. After that process is killed, a later observation by
itself leaves the Running revision unchanged; the surviving process performs
the exact-fence takeover, and direct rooted-state inspection proves Clock receipt,
new claim, and old-Attempt supersession share one CAS before stale output is
rejected. This is process-death evidence, not a disk power-loss claim.

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
a minimized `cymule.test-trace/2` JSON document. That document can be promoted
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
