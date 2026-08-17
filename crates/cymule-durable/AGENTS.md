# Durable Single-Domain Guidance

- This crate owns provider-neutral persistence and recovery contracts. It must
  not name a database, queue, object store, cloud, or transport product.
- A durable write is compare-and-swap over one complete `DurableState` revision.
  Adapters must not acknowledge a partial state transition.
- Continuations contain only typed canonical references and explicit logical
  positions. Never persist process memory, closures, host-language stacks, or
  ambient time.
- Wait completion, lease acquisition, outbox claims, occurrence recording, and
  snapshot publication must be idempotent and fenced.
- Signal and timer waits complete only through an identified
  `cymule.wait-activation/1` record. Match the declared source, atomically store
  its result and ready every selected Continuation, and allow one signal token
  to consume at most one consume-once wait. Stable activation redelivery is
  idempotent; conflicting ID reuse and stale writers fail closed.
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
- Reference in-memory synchronization is adapter-local and non-blocking.
  Contention must surface as a CAS conflict rather than waiting on a mutex.
- Concrete storage belongs under `plugins/` and must pass this crate's shared
  conformance suite, including reopen and stale-writer tests.
- M1 changes require updates to the profile document, fault matrix, schemas,
  SDK control surfaces, and restart-level tests.
