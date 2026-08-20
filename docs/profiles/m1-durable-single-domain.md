# M1 Durable Single-Domain Profile

Status: implemented for one production single-domain authority.

## Implemented profile

- portable `cymule.machine-snapshot/5` with deterministic projection rebuild,
  restored command deduplication, full Effect preimages/profiles, authenticated
  compacted command/Event evidence, and strict rejection of earlier versions;
- cumulative causally closed Event-prefix compaction into an authenticated base
  projection plus exact full suffix, with M1 receipts, stale-CAS rejection,
  repeated compaction lineage, old-command replay, tamper rejection, and
  lost-acknowledgement reopen;
- provider-neutral whole-state compare-and-swap `DurableStore`;
- multi-Run domain creation: the first Run initializes the durable state and
  every later Run atomically appends its exact Plan, input, start/attempt
  Events, command receipts, and initial Continuation without resetting existing
  Runs; identical start replay returns the retained boundary and conflicting
  Plan/input reuse fails closed;
- typed, self-validating higher-profile journals committed by the same M1 CAS,
  allowing M2-M4 state to share one durable authority without entering the
  semantic kernel;
- atomic multi-journal checkpoints with conflict-before-CAS rollback, used when
  one higher-profile transition publishes several typed projections;
- complete typed Continuation fields for frame, state, waits, scopes,
  obligations, leases, budget, causal frontier, and epoch;
- `cymule.durable-state/2` frames with separate definition, structural
  invocation, immutable input Artifact, nested Region path, locals, and next
  step;
- idempotent wait registration/completion;
- identified `cymule.wait-activation/1` signal and timer receipts with declared
  source matching, atomic result/wait/Continuation updates, broadcast delivery,
  consume-once winner enforcement, redelivery idempotency, and stale-writer
  rejection;
- rebuildable parked signal/timer indexes with deterministic bounded selection,
  exact target validation, one-consumer signal selection, and timer occurrence
  isolation;
- replaceable `WaitSourceDriver` receive/acknowledge interface with a hard
  target bound and restart test proving acknowledgement loss redelivers one
  already committed activation;
- restart-safe `Ready` resumption that advances the epoch and commits a new
  fenced Attempt after the wait-owning Attempt has yielded;
- atomic wait-activation plus higher-profile journal checkpoints, used to keep
  M1 Continuations and M3 indexed wake projections in one CAS revision;
- exact-Artifact plus higher-profile journal checkpoints that reject unrelated
  Machine mutations, used for M3 terminal and failure evidence;
- logical-clock authority leases and fencing epochs;
- previewed authority leases atomically committed with higher-profile journal
  records, including stale-CAS rollback and lost-receipt reopen, used by M3
  worker capacity-slot claims and renewals;
- effect outbox enqueue, claim, settlement, and explicit `unknown`;
- repeated reconciliation of an `unknown` outbox entry under its original
  claim, including process reopen between `still_unknown` and terminal
  resolution without a second dispatch;
- canonical component occurrence inputs, outputs, binding, and revision;
- portable snapshot metadata;
- provider-neutral `cymule.resource/1` handles for inline values, objects,
  collections, directories, and sandbox/workspace snapshots, with
  content-verified, resolver-required, and live-only replay classification;
- bounded chunk/list `ArtifactResolver`, chunked `ArtifactStore`, and M1 typed
  Run-to-Run handoff journals with idempotent transfer IDs and reopen replay;
- atomic handoff-to-input activation that stores the canonical Resource Handle
  Artifact, transfer and activation records, input-wait completion, and
  Continuation readiness in one M1 CAS, including lost-receipt replay;
- Rust, TypeScript, Python, and Go Resource builders sealed to one shared ID by
  the trusted Rust Engine;
- non-blocking shared-memory CAS reference and atomic directory-store adapter;
- resumable sequential `call`/`wait` interpretation with process reopen, epoch
  advance, and component-result replay without reinvocation;
- nested Region interpretation with index-only persisted frame paths, durable
  scope stacks, child-result binding, and restart-safe child commit;
- reusable definition invocation with isolated input/locals, deterministic
  invocation identity, result binding, wait/reopen recovery, and component
  occurrence replay without reinvocation;
- nested commit-gated effects that remain staged while their child scope is
  open and dispatch exactly once after a durable child commit, including lost
  enqueue and child-commit receipt recovery;
- observational eager effects that settle and durably bind their result before
  scope commit, including claim, `Unknown`, and settlement receipt loss;
- explicit-release effects that remain prepared after scope commit, expose a
  stable release-required outcome, and replay a caller-authorized release and
  completed Result without duplicate dispatch;
- commit-gated root effects with atomic outbox enqueue, fenced
  `DispatchStarted`, settlement, and reconciliation recovery;
- stage-specific canonical delta validation that permits only the exact effect
  Events, command receipts, and input/result Artifacts corresponding to enqueue,
  claim, observation, or reconciliation; unrelated Machine changes fail before
  CAS;
- atomic `Unknown` observation Event plus outbox publication, including the
  recovery path from a committed `DispatchStarted` claim;
- crash-after-provider-application tests proving one or more restarts perform
  reconciliation without a second dispatch;
- fault tests for lost prepare response and lost durable receipts after enqueue,
  root scope commit, dispatch-start claim, Applied settlement, and Unknown
  observation, with exact prepare/dispatch/reconcile call counts across reopen;
- reopen, interrupted-staging, stale-writer, stale-claim, and idempotency tests.
- lost-acknowledgement recovery for later Run creation, proving reopen resumes
  one committed Run without duplicate start Events or component invocation.
- closed `cymule.durable-control/1` start, resume, wait-admission,
  effect-release, Run-query, and domain-query commands, with one Rust admission
  authority and Rust, TypeScript, Python, and Go transport contracts;
- real child-process death before and after every CAS in a complete mutating
  effect Run, with SQLite reopen, Machine replay, terminal outbox inspection,
  and independent provider dispatch/reconciliation counts;
- official production adapters for SQLite/atomic-directory state, filesystem
  and conditional object Resources, persistent HTTP signals, durable timers,
  and restart-monotonic logical clock observations.

No concrete storage product is part of this profile. An adapter conforms only
when it provides atomic whole-state CAS and passes the profile fault suite.

Version decision: Resources introduce independent `cymule.resource/1` and
`cymule.resource-handoff/1` domains. Identified signal/timer admission introduces
the independent `cymule.wait-activation/1` record inside M1 durable state. The
additive activation map defaults empty when older M1 state is read. The
`cymule.durable-control/1` domain is additive and delegates all reduction to
Rust. These additions do not further alter `cymule.semantic/4`, `cymule-core`,
`ArtifactRef`, Event, or Continuation wire shapes; they implement the existing
resource, durable-wait, consume-once, and epoch-fencing laws.
