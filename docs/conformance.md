# Conformance

Status: the Semantic Interpreter, Embedded, Durable Single Domain, Large Virtual
Graph, Live Evolution M4, and optional Agent paths are source-implemented as one
partial terminal candidate. Multiple focused gates have passed, but review is
still changing the shared tree and the exact frozen-tree full gate is validation
pending. No release tag, package publication, operator migration, or deployment
exists for this candidate.

## Status ladder

- **Source-implemented** means the current checkout contains the terminal
  contract and code path and has focused evidence. It does not mean that the
  final tree is validated, published, operator-migrated, deployed, or
  production-proven.
- **Partial terminal candidate; validation pending** is the current integrated
  state for M1, M3, and M4. It advances only after review findings are closed,
  one exact tree is frozen, and that same tree passes the complete repository,
  version-domain, schema, SDK, documentation, fault, and process-death gates.
- **Released/published** requires separate immutable tag, package, provenance,
  and release-control receipts for that validated source. None exists for this
  candidate.
- **Operator-migrated** and **deployed** are separate external states proved by
  their runbook execution records and terminal environment readback. Neither
  has occurred for this candidate, and neither can be inferred from source or
  test presence.

## Profiles

| Profile | Status | Required behavior |
| --- | --- | --- |
| Semantic Interpreter M0 | Source-implemented; validation pending | frozen IR, canonical stores, admission, reducer, exact state replay |
| Embedded M0 | Source-implemented; validation pending | one-shot in-memory execution, suspension boundary, process plugins, SDK facade |
| Durable Single Domain | Partial terminal candidate; validation pending | small-head CAS over one authenticated typed StateRoot, bounded active-state reopen and exact historical lookup, receipt-backed cold reclamation, multi-Run atomic creation, complete Continuations, identified persistent wait sources, Run-local effect authority with paged terminal recovery, leases, commit-gated/eager/explicit outbox policy, occurrence replay, atomic Resource handoff input activation, history compaction, ambiguous-effect reconciliation, four-language controls, official local adapters, and real process-death CAS sweeps |
| Optional Agent Interaction plugin | Optional source implementation; validation pending | separately owned Session, occurrence, input, workspace, and stream behavior over generic M1 interfaces, including fresh-only host dispatch, historical Context prefix reads, capacity-safe stream finalization, recovery fail-closed without reader proof, all-host-kind cases, and real process-death matrices; not a framework profile |
| Large Virtual Graph M3 | Partial terminal candidate; validation pending | bounded virtual regions, M1 checkpoints, exact parked index, binding-pinned occurrences, weighted fairness, verified cursor migration, certified cold compaction/partial rehydration, fenced multi-worker slot leases/recovery, four SDK controls, and restore |
| Replicated Domain | Proposed | fenced ownership, failover, no split-brain commit |
| Multi-Tenant Authority Host | Proposed | authenticated principal/tenant membership, generation-bound tenant/domain routing, per-operation authorization, independent quotas, fail-closed audit admission, and local/remote canonical semantic equivalence |
| Strong Isolation | Proposed | untrusted code, secret, network, and tenant isolation |
| Live Evolution M4 | Partial terminal candidate; validation pending | unified registry/DAG/rollout/pin authority, reusable modules, default transitive latest-compatible relinking with reachable no-widening admission, template-plus-Plan history, exact patch admission, conservative extensible impact, exact-domain quiescence-gated migration and replacement, isolated shadow plugins, immutable mixed-version pins, deterministic canary gates, promotion/rollback, four SDK controls, complete Engine `/5` receipts, normalized `EvolutionCurrent` plus keyed StateRoot families, and current-head lost-receipt recovery |

The M0 rows do not claim persistence. The M1 source evidence covers
single-domain durable wait and nested-scope resumption, exact replay of recorded
component outputs, three
dispatch policies, reconciliation after an ambiguous dispatch, official
source adapters, authenticated StateRoot reopen, and real process death on both
sides of every discovered Run CAS. It does not imply distributed consensus,
provider-level exactly-once behavior, or multi-domain failover.

## Required semantic cases

The source test inventory covers the following cases. Multiple focused runs
have passed, but this inventory does not replace the pending frozen-tree full
gate:

- identical Plan Candidates seal to an identical Plan ID;
- every definition/component/effect/typed-wait schema compiles under exact
  Draft 2020-12 semantics before sealing; another declared dialect and external
  resolution fail, while `$ref`-shaped `const` data remains legal;
- Run/invocation/component/effect/wait/result values are checked at their exact
  boundary; invalid inputs call no operation plugin and create no associated
  durable record, while invalid outputs create no occurrence, Result Artifact,
  or outbox settlement;
- contract failures retain boundary, side, instance path, schema path, masked
  issues, and retry disposition through the Engine envelope and all SDKs;
- every Engine failure category accepts only its closed recovery-disposition
  set, including absent disposition only for transport and not-found and
  mandatory reconciliation for unknown world outcome;
- every Engine success contains the exact complete inner request value that was
  serialized and sent plus its response; all four SDKs reject a missing or
  mismatched echo, a valid response variant paired with the wrong request
  variant, and the former v4 response-only success before exposing the payload,
  including omitted-versus-explicit-null member mismatches, while failure
  contains no request and rejects one as unknown;
- strict raw request and response values are compared recursively with typed
  reserialization so omitted/synthesized members, collapsed or reordered
  arrays, and changed scalars fail; required nullable members remain accepted,
  including null Run query results;
- all four SDKs admit an exact 64 MiB UTF-8 Engine envelope, reject max plus one
  before process/custom transport invocation, preserve a complete early failure
  despite stdin closure, and reject an early-close forged success with the
  request's read-only or mutating recovery classification;
- the common request echo correlates Seal, Resource, verification, Clock,
  durable/cancel, execution, and live-evolution responses without an SDK
  recomputing Rust-owned derived identities; operation-specific response and
  durable-receipt validation still run after the echo matches, and malformed
  echo classification follows the actual sent request's mutating boundary;
- malformed plans and unknown references fail before hashing;
- a bound Effect is admitted only for the observational/eager profile, including
  inside nested scopes and non-entry definitions; invalid candidates fail
  sealing, Embedded execution, and durable start before canonical mutation or
  business plugin calls;
- scopes have one auto-commit wire form without a mode; legacy transactional
  and speculative mode fields fail closed as unknown members;
- missing causal parents and tampered event IDs are rejected;
- independent event order produces the same projection digest;
- command retry returns the original receipt and semantic reuse is rejected;
- stale precondition tokens return a structured conflict;
- a stale Attempt cannot yield after an epoch advance;
- scope commit closes internal state and transfers obligations exactly once;
- effect transitions reject illegal jumps;
- dispatch ambiguity becomes `unknown`, never a fresh intent;
- failed and cancelled Runs reject terminal `Observe(Applied|NotApplied)` and
  settle dispatched ambiguity only through `Reconcile`; completion rejects any
  unsettled Effect, including a non-blocking observational Effect, and Core,
  Engine schema, and all four SDK validators reject `Completed+Unknown` views;
- prepare response loss retries the same structural intent; effect enqueue,
  scope commit, dispatch-start claim, Applied/Unknown observation, and
  reconciliation receipt loss reopen without duplicate provider dispatch;
- effect/outbox checkpoints reject unrelated canonical Events, commands,
  Artifacts, or Plan changes, and `Unknown` Event plus outbox state commit in one
  CAS;
- wide failure/cancellation pages advance one Core-bound hidden Run-local
  outbox companion, preserve commits to other Runs between pages, fence late
  same-Run material results, and recover an admitted ExpectedFailure without a
  provider or Clock call;
- exact Start replay resolves hot or cold singleton batch/material authority
  before Clock access, while a genuinely fresh Start constructs its semantic
  stage only once inside the Clock-guarded CAS;
- Agent Context pages bind an immutable `(head,count)` prefix after later
  Session append, account message-current bytes independently from page-wire
  bytes, and return the same selected history for page sizes one and 256 across
  Memory, Directory, SQLite, and process-local implementations;
- an unresolved Context cannot turn a provider-authored recovery snapshot into
  Completed after the original reader capability is gone; an already committed
  completion replays and NotApplied evidence remains terminal;
- staged and external Agent streams reject a final AgentUpdate wrapper that
  would exceed its bound before storing a chunk or invoking a publication
  provider;
- a running virtual evaluation campaign is observed through a non-mutating
  SQLite connection, externally killed after visible durable progress, reopened
  under an expired-lease fence, and completed with one terminal result per
  logical case;
- nested Region paths and scope stacks survive reopen without repeating a
  completed component; nested effects cannot dispatch before child commit;
- a committed component occurrence replays without reinvocation, while an
  unclassified Call whose provider response precedes a failed atomic
  result/Continuation checkpoint may run again and therefore carries no
  external exactly-once promise;
- eager observations can bind a settled Artifact while their scope remains
  open, and explicit effects dispatch only after a stable caller release;
- reconciliation retains the original occurrence binding;
- `cymule.plugin/3` dispatch/reconciliation requests and provider results
  exact-match one content-addressed `cymule.effect-provider-attempt/1` bound to
  the retained intent claim owner and fence;
- first and later Run creation atomically publish exact Plan/input/start data
  and the initial Continuation without resetting existing Runs; identical start
  replay is non-mutating, conflicting Plan/input reuse fails, and later Run
  creation reopens after a lost receipt; a generated boundary sweep injects one
  failure before every CAS and one lost
  acknowledgement after every committed CAS, then reopens, validates the whole
  durable state, replays the Machine, and proves at-most-once dispatch;
- a second black-box sweep replaces injected errors with actual child-process
  termination at both sides of every automatically discovered Run CAS; an
  independent SQLite provider ledger proves dispatch is never repeated and
  only reconciliation can settle a killed post-claim window;
- identical signal/timer activation redelivery returns the original durable
  decision, source mismatch and conflicting ID reuse fail, one signal token
  consumes at most one consume-once wait, mixed terminal/pending broadcast
  targets retain the exact applied subset plus ready Runs, and stale writers
  commit nothing;
- parked indexes rebuild from pending waits, select within a hard bound, reject
  cross-source targets, and replay one committed activation when source
  acknowledgement is lost after CAS;
- a process reopening after wait activation advances the Continuation epoch and
  begins a new fenced Attempt before interpretation;
- virtual source cursors and bounded frontiers reopen from chained,
  content-addressed M1 journal deltas with bounded record size and linear byte
  growth; stale CAS rolls back the in-process scheduler, exact reason wake
  avoids a parked scan, activation plus M3 wake commit atomically, and exact
  replay after a later checkpoint returns the historical wake receipt without
  moving the current head;
- cursor-version changes, stalled cursors, repeated work identities, and partial
  source failures advance neither cursor nor materialized frontier;
- restored virtual snapshots reject duplicate work placement, missing region or
  known-set identity, malformed claim fencing, and per-Run frontier overflow;
- work claims pin binding and epoch before execution; identical disposition
  replay is idempotent, conflicts and stale owners fail, retry creates a later
  occurrence, and cancellation rejects late success;
- live-evolution selection, typed decision/Plan/binding occurrence pin, virtual
  capacity-slot claim, and lease share one CAS; an empty claim creates no pin,
  lost receipt reopen retains the exact pin and claim, and replay fails if either
  coupled journal record is absent or different;
- every successful stateful Engine `execute_live_evolution` operation returns a
  closed receipt containing
  its exact journal, complete command, and operation-correlated original
  outcome; a missing, altered, outcome-only, or command-ID-only success is
  rejected by Rust and all four SDKs;
- within one live-evolution journal, semantic reuse of an outer command ID fails
  before safe-point or plugin I/O, while exact historical replay after later
  checkpoints returns the original outcome and rehydrates the current head
  without losing later Plans, decisions, evidence, or occurrence pins;
- historical migration, restart, and shadow replay neither revalidates its old
  safe point nor repeats Describe or execution; checkpoint restore rejects a
  receipt whose journal, full command, outcome, snapshot, Plans, Artifacts, or
  coupled M1 state do not form one atomic materialized transition, and rejects
  a missing checkpoint cause, removed control receipt, or control/virtual-claim
  cause disguise;
- migration replay after a later resume and terminal completion verifies the
  original migrate/epoch command receipts, Event-precondition lineage, target
  Plan, and Artifact closure while leaving the completed Continuation and
  Machine head unchanged; a missing Shadow input performs zero provider calls
  and zero CAS, and schema conformance rejects both missing/null safe points for
  migration or restart and any present safe point for other operations;
- repeated exact template registration returns the initially linked Plan after
  later relinking, while the same template identity with different candidate or
  reference content conflicts before mutation;
- a resolution command replayed after later claims returns its original
  occurrence receipt, while semantic command-ID reuse fails without state change;
- weighted backlogged Runs receive deterministic cost-normalized shares (1:3
  weight yields 20:60 equal-cost dispatch and 40:40 when the weighted Run costs
  three units); snapshot restore predicts the same next claim;
- priority aging selects an old priority-zero item after six continuous
  priority-five dispatches, and one-slot region rotation gives both sources
  equal materialization visibility;
- split/merge requires pinned adapter verification and exact source cursors;
  retirement preserves existing work, stale cursor/evidence/CAS retires nothing,
  split-merge lineage reopens, and old commands return original receipts;
- TypeScript, Python, Rust, and Go parse the same opaque-cursor migration command
  without implementing cursor partitioning or coverage validation;
- an exhausted completed region compacts exact occurrence history to a
  content-addressed manifest, retains its terminal fence/binding certificate,
  survives M1 reopen, and restores only requested occurrence IDs;
- injected archive put/get failures, manifest tampering, and stale compaction
  CAS restore neither partial scheduler state nor partial Machine state; a
  later identical command returns its original receipt;
- TypeScript, Python, Rust, and Go construct the same compaction and rehydration
  commands without computing a manifest or certificate;
- distinct worker capacity slots claim independently while the same slot cannot
  overclaim; lease acquisition/renewal and M3 checkpoints share one M1 CAS;
- a normal result at logical expiry is rejected with no Artifact commit,
  pre-expiry recovery is rejected, and explicit post-expiry retry lets a later
  worker claim a greater work epoch while late old-worker output remains fenced;
- lost claim, renewal, and recovery acknowledgements reopen to exactly one
  transition and replay the original receipt; stale writers retain neither a
  partial lease nor partial scheduler update;
- Run-weight commands reset prior deficit for future selection, replay
  idempotently, survive reopen, and reject stale CAS or conflicting ID reuse;
- TypeScript, Python, Rust, and Go construct the same claim, renewal, recovery,
  and Run-weight commands without reading clocks or implementing a scheduler;
- TypeScript, Python, Rust, and Go construct and validate the same unified
  template-scoped live-evolution command through the Rust Engine;
- M3 claim/result checkpoints survive reopen, stale CAS rolls back scheduler
  state, and result/evidence Artifacts commit with occurrence state;
- TypeScript, Python, Rust, and Go parse one occurrence fixture and construct one
  idempotent owner/work-epoch/lease-epoch/time-fenced control command;
- a Binding Context update changes only future occurrences;
- replay availability is not reported as exact when an artifact is missing;
- TypeScript, Python, Rust, and Go author the same plan and execute through the
  same Rust kernel and external plugin;
- TypeScript, Python, Rust, and Go receive the same structured validation,
  plugin-defect, and pre-dispatch substrate failures through
  `cymule.engine/5`; v4 envelopes fail without fallback and missing-envelope
  transport failures carry no inferred retry permission;
- that shared Plan invokes a reusable definition, so all four SDKs produce one
  `cymule.ir/3` Plan ID and the Rust runtime binds the invoked result before its
  effect;
- TypeScript, Python, Rust, and Go submit the same Resource Candidate to the
  Rust resource sealer and receive the same Resource ID;
- TypeScript, Python, Rust, and Go construct the same identified wait activation
  fixture and validate its closed wire contract through the Rust Engine;
- TypeScript, Python, Rust, and Go construct the same closed explicit-takeover
  command and validate it through the Rust Engine; Rust restart-level
  conformance separately drives start, signal admission, resume, terminal
  replay, Run query, and domain query through the stateful authority;
- Resource identity excludes separate locator sets; credential-bearing public
  URLs fail; bounded reads and manifest-proof lists reject malformed adapters;
  content bytes are verified; keyed pin/release/GC/delete authorities and
  cleanup receipts resolve exactly without global history replay; and
  Run-to-Run handoffs bind producer occurrence/result provenance,
  survive M1 reopen, and reject conflicting transfer IDs.

The optional Agent interaction plugin runs a separate Rust conformance suite for
Session projection replay, binding-pinned occurrences, atomic input and stream
checkpoints, workspace effects, receipt-loss recovery, failures across all six
host call kinds, permission refusal, caller stop reasons, and real process death
around occurrence, Session, and stream journals. Those cases validate the
plugin's use of M1 interfaces; they are not required behavior of the core CLI
or four language SDKs.

## Cross-axis scenario

Status: source-implemented as independent fault families plus black-box
campaigns; final frozen-tree validation pending.
The suite composes mutating effects, ambiguous dispatch, future binding update,
pinned reconciliation, obligation settlement, nested auto-commit scopes,
stale-command and epoch fencing, then runs end-to-end process-death campaigns.
These witnesses remain partitioned so a change in one axis does not force every
unrelated fault family into the developer feedback loop.
