# Conformance

Status: implemented for the Semantic Interpreter and Embedded profiles.

## Profiles

| Profile | Status | Required behavior |
| --- | --- | --- |
| Semantic Interpreter M0 | Implemented | frozen IR, canonical stores, admission, reducer, exact state replay |
| Embedded M0 | Implemented | one-shot in-memory execution, suspension boundary, process plugins, SDK facade |
| Durable Single Domain | Implemented | segmented small-head CAS, authenticated bounded checkpoint-plus-suffix reopen, receipt-backed cold reclamation, multi-Run atomic creation, complete Continuations, identified persistent wait sources, leases, commit-gated/eager/explicit outbox policy, occurrence replay, atomic Resource handoff input activation, history compaction, ambiguous-effect reconciliation, four-language controls, production local adapters, and real process-death CAS sweeps |
| Optional Agent Interaction plugin | Implemented plugin suite | separately owned Session, occurrence, input, workspace, and stream behavior over generic M1 interfaces, including all-host-kind and real process-death matrices; not a framework profile |
| Large Virtual Graph M3 | Implemented | bounded virtual regions, M1 checkpoints, exact parked index, binding-pinned occurrences, weighted fairness, verified cursor migration, certified cold compaction/partial rehydration, fenced multi-worker slot leases/recovery, four SDK controls, and restore |
| Replicated Domain | Proposed | fenced ownership, failover, no split-brain commit |
| Strong Isolation | Proposed | untrusted code, secret, network, and tenant isolation |
| Live Evolution M4 | Implemented | unified registry/DAG/rollout/pin authority, reusable modules, default transitive latest-compatible relinking with reachable no-widening admission, template-plus-Plan history, exact patch admission, conservative extensible impact, Continuation-proved migration, explicit replacement-Run restart, isolated shadow plugins, immutable mixed-version pins, deterministic canary gates, promotion/rollback, four SDK controls, and lost-receipt recovery |

The M0 rows do not claim persistence. M1 proves single-domain durable wait and
nested-scope resumption, exact replay of recorded component outputs, three
dispatch policies, reconciliation after an ambiguous dispatch, production
source adapters, authenticated suffix recovery, and real process death on both
sides of every discovered Run CAS. It does not imply distributed consensus,
provider-level exactly-once behavior, or multi-domain failover.

## Required semantic cases

The local suite verifies:

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
- malformed plans and unknown references fail before hashing;
- missing causal parents and tampered event IDs are rejected;
- independent event order produces the same projection digest;
- command retry returns the original receipt and semantic reuse is rejected;
- stale precondition tokens return a structured conflict;
- a stale Attempt cannot yield after an epoch advance;
- scope commit closes internal state and transfers obligations exactly once;
- effect transitions reject illegal jumps;
- dispatch ambiguity becomes `unknown`, never a fresh intent;
- prepare response loss retries the same structural intent; effect enqueue,
  scope commit, dispatch-start claim, Applied/Unknown observation, and
  reconciliation receipt loss reopen without duplicate provider dispatch;
- effect/outbox checkpoints reject unrelated canonical Events, commands,
  Artifacts, or Plan changes, and `Unknown` Event plus outbox state commit in one
  CAS;
- a running virtual evaluation campaign is observed through a non-mutating
  SQLite connection, externally killed after visible durable progress, reopened
  under an expired-lease fence, and completed with one terminal result per
  logical case;
- nested Region paths and scope stacks survive reopen without repeating a
  completed component; nested effects cannot dispatch before child commit;
- eager observations can bind a settled Artifact while their scope remains
  open, and explicit effects dispatch only after a stable caller release;
- reconciliation retains the original occurrence binding;
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
  consumes at most one consume-once wait, and stale writers commit nothing;
- parked indexes rebuild from pending waits, select within a hard bound, reject
  cross-source targets, and replay one committed activation when source
  acknowledgement is lost after CAS;
- a process reopening after wait activation advances the Continuation epoch and
  begins a new fenced Attempt before interpretation;
- virtual source cursors and bounded frontiers reopen from chained M1 journal
  checkpoints, stale CAS rolls back the in-process scheduler, exact reason wake
  avoids a parked scan, and activation plus M3 wake commit atomically;
- cursor-version changes, stalled cursors, repeated work identities, and partial
  source failures advance neither cursor nor materialized frontier;
- restored virtual snapshots reject duplicate work placement, missing region or
  known-set identity, malformed claim fencing, and per-Run frontier overflow;
- work claims pin binding and epoch before execution; identical disposition
  replay is idempotent, conflicts and stale owners fail, retry creates a later
  occurrence, and cancellation rejects late success;
- live-evolution selection and virtual capacity-slot claim share one CAS; lost
  receipt reopen retains both the template-scoped Plan pin and the exact claim,
  and replay fails if either coupled journal record is absent or different;
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
  `cymule.engine/1`; missing-envelope transport failures carry no inferred
  retry permission;
- that shared Plan invokes a reusable definition, so all four SDKs produce one
  `cymule.ir/2` Plan ID and the Rust runtime binds the invoked result before its
  effect;
- TypeScript, Python, Rust, and Go submit the same Resource Candidate to the
  Rust resource sealer and receive the same Resource ID;
- TypeScript, Python, Rust, and Go construct the same identified wait activation
  fixture and validate its closed wire contract through the Rust Engine;
- TypeScript, Python, Rust, and Go construct the same closed durable-domain
  query command and validate it through the Rust Engine; Rust restart-level
  conformance then drives start, signal admission, resume, terminal replay, Run
  query, and domain query through the stateful authority;
- Resource identity ignores locations, public credential-bearing URLs fail,
  bounded reads/lists reject malformed adapters, content bytes are verified,
  and Run-to-Run handoffs survive M1 reopen and reject conflicting transfer IDs.

The optional Agent interaction plugin runs a separate Rust conformance suite for
Session projection replay, binding-pinned occurrences, atomic input and stream
checkpoints, workspace effects, receipt-loss recovery, failures across all six
host call kinds, permission refusal, caller stop reasons, and real process death
around occurrence, Session, and stream journals. Those cases validate the
plugin's use of M1 interfaces; they are not required behavior of the core CLI
or four language SDKs.

## Cross-axis scenario

Status: implemented as independent fault families plus black-box campaigns.
The suite composes mutating effects, ambiguous dispatch, future binding update,
pinned reconciliation, obligation settlement, nested/speculative scopes,
stale-command and epoch fencing, then runs end-to-end process-death campaigns.
These witnesses remain partitioned so a change in one axis does not force every
unrelated fault family into the developer feedback loop.
