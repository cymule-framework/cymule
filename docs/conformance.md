# Conformance

Status: implemented for the Semantic Interpreter and Embedded profiles.

## Profiles

| Profile | Status | Required behavior |
| --- | --- | --- |
| Semantic Interpreter M0 | Implemented | frozen IR, canonical stores, admission, reducer, exact state replay |
| Embedded M0 | Implemented | one-shot in-memory execution, suspension boundary, process plugins, SDK facade |
| Durable Single Domain | Partial | snapshot/base-plus-suffix restore, CAS, nested Continuation frames, identified bounded wait-source drivers, leases, commit-gated/eager/explicit outbox policy, occurrence replay, atomic Resource handoff input activation, directory-store reopen, history compaction, and ambiguous-effect reconciliation; production plugins and process-kill coverage remain |
| Optional Agent Interaction plugin | Partial plugin suite | separately owned Session, occurrence, input, workspace, and stream behavior over generic M1 interfaces; not a framework profile |
| Large Virtual Graph M3 | Implemented | bounded virtual regions, M1 checkpoints, exact parked index, binding-pinned occurrences, weighted fairness, verified cursor migration, certified cold compaction/partial rehydration, fenced multi-worker slot leases/recovery, four SDK controls, and restore |
| Replicated Domain | Proposed | fenced ownership, failover, no split-brain commit |
| Strong Isolation | Proposed | untrusted code, secret, network, and tenant isolation |
| Live Evolution M4 | Implemented | reusable modules, transitive latest-compatible relinking, exact patch admission, durable registry recovery, conservative extensible impact, checked migration and isolated shadow plugins, immutable mixed-version pins, deterministic canary gates, promotion/rollback, four SDK controls, and lost-receipt recovery |

The M0 rows do not claim persistence. The partial M1 implementation does prove
single-domain durable wait and nested-scope resumption, exact replay of recorded
component outputs, three dispatch policies, and reconciliation after an
ambiguous dispatch. It does not yet claim production source plugins, snapshot
suffix recovery, or process-kill coverage of every crash window.

## Required semantic cases

The local suite verifies:

- identical Plan Candidates seal to an identical Plan ID;
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
- nested Region paths and scope stacks survive reopen without repeating a
  completed component; nested effects cannot dispatch before child commit;
- eager observations can bind a settled Artifact while their scope remains
  open, and explicit effects dispatch only after a stable caller release;
- reconciliation retains the original occurrence binding;
- Run creation atomically publishes its initial Machine and Continuation; a
  generated boundary sweep injects one failure before every CAS and one lost
  acknowledgement after every committed CAS, then reopens, validates the whole
  durable state, replays the Machine, and proves at-most-once dispatch;
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
- M3 claim/result checkpoints survive reopen, stale CAS rolls back scheduler
  state, and result/evidence Artifacts commit with occurrence state;
- TypeScript, Python, Rust, and Go parse one occurrence fixture and construct one
  idempotent owner/work-epoch/lease-epoch/time-fenced control command;
- a Binding Context update changes only future occurrences;
- replay availability is not reported as exact when an artifact is missing;
- TypeScript, Python, Rust, and Go author the same plan and execute through the
  same Rust kernel and external plugin;
- that shared Plan invokes a reusable definition, so all four SDKs produce one
  `cymule.ir/2` Plan ID and the Rust runtime binds the invoked result before its
  effect;
- TypeScript, Python, Rust, and Go submit the same Resource Candidate to the
  Rust resource sealer and receive the same Resource ID;
- TypeScript, Python, Rust, and Go construct the same identified wait activation
  fixture and validate its closed wire contract through the Rust Engine;
- Resource identity ignores locations, public credential-bearing URLs fail,
  bounded reads/lists reject malformed adapters, content bytes are verified,
  and Run-to-Run handoffs survive M1 reopen and reject conflicting transfer IDs.

The optional Agent interaction plugin runs a separate Rust conformance suite for
Session projection replay, binding-pinned occurrences, atomic input and stream
checkpoints, workspace effects, and receipt-loss recovery. Those cases validate
the plugin's use of M1 interfaces; they are not required behavior of the core
CLI or four language SDKs.

## Cross-axis scenario

Status: partial. The current suite composes a mutating effect, ambiguous
dispatch, future binding update, pinned reconciliation, obligation settlement,
and replay. Stale-command and epoch-fencing axes are covered independently. A
single crash-injected scenario that also includes a speculative scope is an M1
durable-profile gate and is not claimed by version 0.1.0.
