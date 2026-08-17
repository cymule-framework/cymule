# M1 Durable Single-Domain Profile

Status: partial.

## Implemented foundation

- portable `MachineSnapshot` with deterministic projection rebuild and restored
  command deduplication;
- provider-neutral whole-state compare-and-swap `DurableStore`;
- typed, self-validating higher-profile journals committed by the same M1 CAS,
  allowing M2-M4 state to share one durable authority without entering the
  semantic kernel;
- atomic multi-journal checkpoints with conflict-before-CAS rollback, used when
  one higher-profile transition publishes several typed projections;
- complete typed Continuation fields for frame, state, waits, scopes,
  obligations, leases, budget, causal frontier, and epoch;
- idempotent wait registration/completion;
- identified `cymule.wait-activation/1` signal and timer receipts with declared
  source matching, atomic result/wait/Continuation updates, broadcast delivery,
  consume-once winner enforcement, redelivery idempotency, and stale-writer
  rejection;
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
- Rust, TypeScript, Python, and Go Resource builders sealed to one shared ID by
  the trusted Rust Engine;
- non-blocking shared-memory CAS reference and atomic directory-store adapter;
- resumable sequential `call`/`wait` interpretation with process reopen, epoch
  advance, and component-result replay without reinvocation;
- nested Region interpretation with index-only persisted frame paths, durable
  scope stacks, child-result binding, and restart-safe child commit;
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

## Remaining completion gates

- bounded timer and signal source drivers plus parked-index selection;
- snapshot compaction and suffix rehydration;
- all SDK control/query surfaces and restart-level end-to-end tests;
- production resolver/store plugins and durable activation from incoming
  handoffs into interpreter Continuation state.

No concrete storage product is part of this profile. An adapter conforms only
when it provides atomic whole-state CAS and passes the profile fault suite.

Version decision: Resources introduce independent `cymule.resource/1` and
`cymule.resource-handoff/1` domains. Identified signal/timer admission introduces
the independent `cymule.wait-activation/1` record inside partial M1 durable
state. The additive activation map defaults empty when older M1 state is read.
The additive `seal_resource` and `verify_wait_activation` Engine requests are
returned only to callers that request them; activation verification lets every
SDK validate the closed record without claiming stateful admission. These
additions do not alter `cymule.semantic/1`, `cymule-core`, `ArtifactRef`, Event,
or Continuation wire shapes; they implement the existing resource, durable-wait,
consume-once, and epoch-fencing laws.
