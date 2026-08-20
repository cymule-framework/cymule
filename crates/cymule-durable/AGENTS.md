# Durable Single-Domain Guidance

- This crate owns provider-neutral persistence and recovery contracts. It must
  not name a database, queue, object store, cloud, or transport product.
- A durable write is compare-and-swap over one complete `DurableState` revision.
  Adapters must not acknowledge a partial state transition.
- Every Run creation includes its Plan/input/start Events and initial
  Continuation in one state CAS. The first Run initializes the domain; later
  Runs append an exact Machine delta under the same authority. Both pre-commit
  failure and post-commit acknowledgement loss must reopen to a retryable or
  resumable state, and an identical start must never reset existing progress.
- Continuations contain only typed canonical references and explicit logical
  positions. Never persist process memory, closures, host-language stacks, or
  ambient time.
- Nested interpreter frames persist an index-only `region_path` into the sealed
  Plan and a matching scope stack. Resume must re-resolve that path from the
  immutable Plan; never serialize nested Region bodies into Continuations.
- Each durable frame separates `definition_id`, structural `invocation_id`, and
  immutable input Artifact. Invoked definitions push frames without pushing a
  scope; nested scopes retain the current definition, invocation, and input.
- Run creation stores the canonical `cymule.execution-binding/1` Artifact in
  the same Machine delta as the input, Plan, start Event, Attempt, and initial
  Continuation. Reopen must resolve and verify that exact Artifact; a newly
  selected provider revision cannot reinterpret historical work. The
  coordinator itself must use that resolved binding to admit the Plan before
  the creation CAS; callers cannot bypass authority admission by avoiding the
  resumable executor.
- Every `ArtifactRef` persisted outside the canonical Machine must validate as
  `cymule.artifact/2` and resolve to the exact typed record in that same
  Machine snapshot. This includes Continuation state, frame inputs and locals,
  wait/activation results, outbox inputs/results, and component occurrence
  inputs/outputs. Public coordinator methods never accept dangling or legacy
  references.
- Compile the admitted Plan through `cymule-runtime` and validate Run,
  invocation, component, effect, typed-wait, definition-result, and terminal
  Result values at their exact boundary. Invalid input may not create its
  Artifact, occurrence, outbox entry, or plugin dispatch; invalid output may
  not enter a checkpoint or settle an outbox claim.
- Wait completion, lease acquisition, outbox claims, occurrence recording, and
  snapshot publication must be idempotent and fenced.
- Event-prefix compaction retains an authenticated Machine base and full suffix
  under one CAS. Keep cumulative receipt lineage, exact compacted identities,
  old command receipts, and the suffix parent closure; receipt loss reopens to
  the committed base and stale writers do not alter it.
- Higher profiles that couple a logical lease to journal state must preview the
  exact next lease and use `checkpoint_lease_journals`; acquiring a lease and
  appending its claim/renewal in separate CAS revisions is invalid. Receipt loss
  may leave the transition committed, so reopen and stable command replay are
  required before proposing another lease epoch.
- Signal and timer waits complete only through an identified
  `cymule.wait-activation/1` record. Match the declared source, atomically store
  its result and ready every selected Continuation, and allow one signal token
  to consume at most one consume-once wait. Stable activation redelivery is
  idempotent; conflicting ID reuse and stale writers fail closed.
- `ParkedWaitIndex` is derived from pending waits and Continuation wait sets; it
  is never a second durable authority. Source drivers may poll or receive by
  any transport, but must return exact indexed targets within the framework
  bound and acknowledge only after admission succeeds.
- Activation admission may add only its result Artifact to the current Machine
  snapshot. Reject a caller snapshot that also changes Plans, Events, commands,
  or unrelated Artifacts; wait ingress is not a raw Machine mutation surface.
- A `Ready` Continuation resumed after a crash must advance its epoch and commit
  a new fenced Attempt before interpretation. Never reuse the yielded Attempt
  that originally parked the wait.
- `unknown` outbox entries remain active reconciliation work under their
  original claim. Reopen must query them again and may settle them as applied or
  not applied; it must never redispatch the original Effect or reuse one command
  ID for different reconciliation decisions.
- After `StartDispatch` is claimed, a missing, malformed, timed-out, or defective
  plugin response commits `Unknown` with the outbox settlement before returning
  `ReconciliationRequired`; it never reports same-request retry.
- Retry policy is a content-addressed, provider-neutral algebra, not a
  scheduler. `RetryStream` is only its serializable pure reducer state: it
  retains the complete canonical Policy, consumes a closed failure class, the
  failed occurrence binding, content-addressed logical Clock observation, and
  optional content-addressed jitter evidence, then produces an exact
  stop-or-retry-at transition. Do not claim durable retry from this reducer
  alone. An executor must checkpoint it with the failed occurrence,
  Continuation/timer state, and next-attempt admission in one owning CAS. One
  stream pins one Policy ID, advances one attempt at a time after its admitted
  due time, and becomes immutable after stopping. Any `unknown_world` external
  Effect stops with its original Effect intent retained for reconciliation,
  even when that failure class appears in the Policy's retryable set.
- Effect enqueue, claim, observation, and reconciliation checkpoints validate
  the exact appended Machine Events, command receipts, and allowed Artifacts
  against the proposed outbox transition. Never use a generic Machine write for
  `Unknown`; its observation Event and outbox state share one CAS.
- Exact Machine-delta checkpoints preserve the current compacted base. Effect,
  wait, journal, and Run-creation transitions must not carry an unrelated base,
  Plan, Event, command, or Artifact change.
- Eager observational effects keep their frame on the effect site until the
  durable result Artifact can be bound. Explicit effects return a stable
  release-required outcome and may claim only after their scope commits;
  release retry after receipt loss returns the recorded Result.
- Higher profiles may append typed, self-validating records through
  `application_journals` so they share the M1 CAS authority. M1 stores only the
  versioned envelope; the owning profile validates and reduces its payload.
- A higher-profile state projection coupled to a wait must use the atomic
  journal-plus-wait checkpoints. Separate CAS commits may not claim one logical
  suspension or completion.
- Higher-profile host calls coupled to a semantic Effect must use the atomic
  journal-plus-effect checkpoints so Machine, Continuation, outbox, and typed
  lifecycle records cannot acknowledge different sides of one transition.
- A logical transition spanning multiple higher-profile journals must use
  `checkpoint_journals`; separate appends cannot claim atomic visibility.
- When wait activation also wakes a higher-profile parked/index projection, use
  `checkpoint_wait_activation_journals` so the activation receipt, M1 waits,
  Continuations, and typed projection checkpoint share one CAS revision.
  Never attach a new projection record after that activation was committed;
  redelivery is idempotent only when every requested record already exists.
- Higher-profile result/evidence Artifacts use `checkpoint_artifact_journals`.
  The proposed Machine may add only the explicitly listed Artifacts; reject
  Plans, Events, commands, or unrelated Artifact changes before CAS.
- A higher-profile input delivery that also publishes its Artifact and records
  typed provenance uses `checkpoint_input_wait_journals`; Artifact, journal
  records, input wait, and Continuation readiness must never split across CAS.
- Reference in-memory synchronization is adapter-local and non-blocking.
  Contention must surface as a CAS conflict rather than waiting on a mutex.
- Concrete storage belongs under `plugins/` and must pass this crate's shared
  conformance suite, including reopen and stale-writer tests.
- Extend the deterministic CAS boundary sweep when a Run path adds a durable
  write. Its integrity probe must validate the whole state, replay the Machine,
  and inspect provider call counts after reopen.
- M1 changes require updates to the profile document, fault matrix, schemas,
  SDK control surfaces, and restart-level tests.
- `cymule.durable-control/1` is the only public M1 mutation/query union. It may
  start/resume Runs, admit identified waits, release explicit effects, and
  query Runs/domains; never expose raw Machine, Continuation, outbox, or journal
  writes through an SDK command.
- Keep deterministic injected CAS sweeps in this crate and real SQLite
  child-process death sweeps in `plugins/store-sqlite`. Both discover the Run
  boundary count from a successful execution and inspect provider calls as well
  as recovered state.
- The generated durable model trace is the single command-sequence generator.
  It composes `cymule-test-world` faults with reopen through public durable
  interfaces, checks each response against a small domain model, and emits a
  minimized language-neutral fixture on failure. SDKs must not duplicate it.
