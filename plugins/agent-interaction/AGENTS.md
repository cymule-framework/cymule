# Agent Interaction Guidance

- This directory is an optional plugin. Session, AgentHost occurrence,
  elicitation, workspace-interaction, and stream controllers are not Cymule
  framework semantics and must not be re-exported by the core CLI or SDKs.
- ACP, MCP, A2A, model-provider, editor, and concrete Agent Loop support belongs
  in separately reviewable adapters above this plugin contract.
- The plugin owns `schemas/agent-protocol.schema.json`, its fixtures, PROFILE,
  and all corresponding conformance tests.

- This crate owns protocol-neutral agent interaction contracts and projections,
  not a model loop, model SDK, UI transport, or tool catalog.
- The caller or adapter owns Agent/script loop ordering, strategy, program
  counter, and continuation decisions. Never make context-model-tool ordering,
  loop phases, or a particular reasoning pattern part of Cymule semantics.
- `AgentInteractionController` owns only one caller-identified host occurrence:
  binding pinning, lifecycle persistence, retained-response replay, and explicit
  recovery boundaries. It must not infer or advance the caller's loop.
- Keep Messages, Tasks, Artifacts, session updates, permissions, elicitation,
  context selection, model calls, tool calls, and workspace changes distinct.
- External ACP, MCP, A2A, provider, and editor types belong in adapters. Preserve
  their opaque IDs and extensions without making them canonical authority.
- Session updates are idempotent ordered projections over durable occurrences.
  Streaming chunks are not durable output until explicitly finalized.
- Stream open/chunk/abort records live in a separate M1 journal. Finalization
  must atomically append the terminal stream record and the exact Message/Tool
  `AgentUpdate` through a multi-journal CAS. Chunks never appear in the Session
  projection, and finalized message identity is immutable.
- Large streamed content should finalize to a verified `ResourceHandle` block;
  do not accumulate unbounded provider bytes in Session messages.
- Keep the `ResourceHandle` enum payload boxed in memory so richer semantic
  descriptors do not inflate every `ContentBlock`; Serde wire shape stays flat.
- Journal adapters persist accepted updates before the in-process projection is
  advanced. Reopen by replaying the journal; never treat an in-memory Session as
  durable authority. Adapter-local exclusion must be non-blocking and must
  surface contention instead of turning a lock into interaction semantics.
- Validate every journal-reachable `ArtifactRef` recursively through the core
  `ArtifactRef::validate` authority before append and again during replay. A
  workspace reference matches a retained Machine Artifact only when the full
  version, digest, and kind are equal; an Artifact ID alone is insufficient.
- Every replaceable context, model, permission, tool, elicitation, and workspace
  host call must resolve its implementation binding, persist that binding with
  `prepared` and `started` before invocation, and retain it in a typed
  `completed` or `unknown` outcome. A missing receipt or host error is ambiguous
  and must block automatic redispatch until explicit recovery.
- Durable input requests atomically append the pending/resolved elicitation and
  Session state updates with the owning M1 wait transition. Never expose
  `RequiresAction` without a committed wait or ready a Continuation without the
  matching resolved Session projection.
- `AgentInputController::complete` borrows the already retained result
  `ArtifactRef`; callers keep ownership while the shared M1 CAS validates and
  records the exact reference.
- Every durable input request receives the exact Plan wait owner from its
  caller, including definition, invocation, Region path, site, step, and
  optional bind. The plugin never invents or omits M1 wait ownership.
- Compile elicitation schemas as local Draft 2020-12 documents before suspension
  and validate accepted values again from the persisted request before
  completion. External schema retrieval stays disabled; schema or instance
  failures must occur before the shared M1 CAS and leave no durable mutation.
- The durable elicitation wait admits either the declared accepted-value schema
  or JSON `null`, which is the explicit declined-input result. No other schema
  widening is allowed.
- Reconciliation queries the original binding and may settle only the original
  occurrence as `completed` or evidence-backed `not_applied`. A prepared call
  may be cancelled only with proof that dispatch never started. Reconciliation
  is an idempotent observation and never creates a replacement intent.
- A workspace overlay commit is a Plan-declared mutating Effect, not a special
  filesystem shortcut. Atomically couple its host occurrence to scope closure,
  the transferred obligation, outbox state, Machine snapshot, and Continuation.
  Closing the scope also removes its nested execution frames so the persisted
  Continuation resumes from its still-open lexical parent.
  Abort the scope only after a retained provider receipt proves the overlay was
  not committed. Filesystems, VCS implementations, sandboxes, and object stores
  remain adapters.
- Login, capability advertisement, permission, policy, credential access, and
  effect release are separate decisions and must remain fail closed.
- Add reducer and end-to-end tests for every new update or interaction state.
- Keep host-kind failures/refusals in `fault_matrix.rs`, real process-death
  journal windows in `process_kill.rs`, stream atomicity in `streaming.rs`, and
  ordinary controller semantics in `interaction.rs`; do not collapse these
  independent witnesses into one slow test file.
- The process-death suite reuses the workspace-private `TestWorld` temporary
  domain and `ManagedChild` barrier/reap lifecycle. Agent semantics never own a
  hidden crash switch, test clock, or global child registry.
- Discover occurrence, Session, and stream CAS counts from successful runs with
  the same preconditions as their workers. Sweep both sides of every discovered
  boundary, assert the marker's exact boundary number, and inspect Session plus
  stream closure before recovery so a split multi-journal commit cannot hide
  behind a successful retry.
- After every Agent SIGKILL boundary, run full SQLite integrity checks and a WAL
  checkpoint both before and after semantic recovery. The host-side test ledger
  uses the same WAL plus synchronous-full durability so reconciliation evidence
  is never inferred from a weaker crash substrate.
