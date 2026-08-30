# CLI Guidance

- The CLI is a transport and operator tool over the library contracts. It does
  not own semantic behavior.
- `rpc` JSON is the cross-language conformance boundary; changes require all SDK
  tests and a command-protocol version decision.
- `execute_durable` derives access from the closed command. The bounded `/4`
  `RunIndexPage`, `RunCurrent`, Run-owned page, and exact `RunItem` queries use
  the provider's genuine read-only opener and `DurableStoreControl`, and every
  response is verified against the exact request revision, root, selector,
  limit, and byte budget before it crosses Engine transport. Mutations use the
  writable opener, and the command's executor/Clock requirements must match
  target presence exactly. Missing and extraneous authority both fail before
  Store I/O. Only commands requiring
  execution construct an immutable process binding before delegating to
  `DurableRuntimeControl`. For those execution commands, supported-provider
  admission, exact executable-byte and revision capture, manifest binding, and
  Clock open must finish before the writable store opener; a rejected authority
  target creates no store state. `ResolveEffect` first opens the existing Store
  read-only and returns an exact retained receipt without constructing the old
  provider; only a still-unknown Effect enters provider preflight and writable
  control.
- Engine RPC stateful Evolution is exactly
  `execute_live_evolution(target, evolution_id, command) -> EvolutionCommit`.
  The CLI constructs `EvolutionPersistenceCommand`, fixes the bounded provider
  registry for the request lifetime, and delegates to Durable's single typed
  Evolution persistence control. Do not reintroduce a caller journal ID,
  journal-shaped receipt, parallel controller, or verify-only pseudo-commit.
  Provider configuration stays inert until Durable's pinned phase-zero read
  proves that a fresh command requires it: exact alias and retained-migration
  replay perform no capture or Describe. The CLI resolves the Store target and
  performs that exact receipt lookup through the existing read-only opener;
  an exact replay returns before any writable Store open or lock-file repair.
  A complete provider-free migration or shadow target is therefore a valid
  replay candidate, but a read miss immediately requires the fresh command's
  complete provider authority before writable Store creation. Fresh migration
  may resolve only its exact target Plan from the one-entry binding registry
  and its exact adapter; every other command forbids that registry, and no
  lookup may infer, default, or fall back to another target.
- `seal_resource` is an additive request/response pair over
  `cymule.resource/4`. The CLI delegates validation and identity to
  `cymule-resource`; it must never compute Resource IDs independently.
- `verify_wait_activation` validates only the versioned provider-neutral
  delivery record. Stateful source matching, consume-once admission, and
  Continuation readiness remain `cymule-durable` CAS operations.
- `verify_evolution_command` validates only the closed
  `cymule.evolution-control/5` envelope. Plan linking, adapter execution,
  evidence counting, and durable promotion remain `cymule-evolution` authority.
- Write only the response JSON to stdout. Diagnostics go to stderr.
- RPC domain failures return a successful process transport containing one
  `cymule.engine/5` failure envelope. A nonzero process status is reserved for
  failure to carry the protocol itself; never duplicate a semantic failure on
  stderr or emit an unversioned success payload.
- Engine v5 is the sole accepted transport generation. Reject every v4 request
  and response without probing a legacy shape or synthesizing an outcome-only
  live-evolution success.
- After strict decoding, retain the exact complete inner `EngineRequest` that is
  executed and echo it in every success beside the response. Never rebuild the
  echo from an operation-specific subset. Failures carry no request because
  envelope or request decoding itself may have failed. A predecessor success with
  only `response` is invalid.
- Before any request execution or provider/store I/O, compare the retained raw
  request with its typed reserialization and reject every explicit member
  erased by omission-only/defaulted serialization as request validation with
  `correct_and_retry`.
- Normalize every safe mathematically integral JSON number, including `1.0`
  and `1e0`, before typed request decoding. A success echoes the normalized
  typed request (`1`); finite fractional values remain legal only where the
  selected typed field accepts them, and unsafe integral values fail before
  authority I/O.
- `clock_observed` returns one typed `ClockObservationResult { run_id,
  observation }`. Construct it only through Durable Protocol's verifier, which
  binds the opaque scope to that Run; clients compare the returned Run with the
  exact request and never rederive the scope.
- Direct file/stdin commands use the same duplicate-free, exact-number and
  lossless typed round-trip gate as RPC. Explicit nulls and empty defaults that
  typed serialization omits, synthesized defaults, collapsed array elements,
  reordered arrays, and changed scalars are invalid wires.
- Embedded Run Plan/input/Run-ID and complete `EnginePluginTarget` admission
  precede process construction and describe. Embedded and durable process paths
  copy the exact arguments, explicit environment, working directory, runtime
  closure, deadline, and byte limits from the request into
  `ProcessExecutorConfig`; neither may retain an ambient constructor default.
  Every runtime-closure value is an exact lowercase SHA-256 content ID; mutable
  OS/architecture or version labels fail before process construction.
  Clock scope validation precedes Clock SQLite open. A semantically rejected
  request creates no provider or persistence state.
- The direct `run` command reads that same complete target from
  `--plugin-target`; it has no path-only `--plugin` compatibility constructor.
- Before any durable provider is constructed or opened, resolve lexical paths
  through their nearest existing canonical ancestor once, use that resolved
  location for the actual provider open, and reject overlapping provider-owned
  footprints. A directory Store owns its complete subtree; each SQLite Store or
  Clock owns its base plus `-wal`, `-shm`, and `-journal`. Pre-open rejection
  creates neither authority. For unresolved components, ask the target
  filesystem in a unique, cleaned probe directory under the existing parent
  whether names collide; never approximate case or Unicode normalization with
  string rewriting. Existing and newly opened bases and sidecars use
  cross-platform stable file identity, and the CLI rereads that identity after
  Clock open and after Store open before submitting a durable command so
  hardlink, symlink, case-folding, and open-race aliases cannot become two
  authorities. A race detected after an open may leave only the provider's
  initialized local file, never a submitted durable mutation.
- Durable execution preflight captures bytes, performs exactly one Describe,
  derives the binding from that observation, and returns the one-shot admitted
  provider token consumed after writable Store open.
- A typed durable commit outcome uncertainty maps to
  `unknown_world_outcome` with `reconcile`; it must never become
  `retry_same_request`.
- Structured Durable and Evolution failures retain their owning `code` and
  human message without string-prefix reconstruction. Ordinary profile
  `Persistence` failure maps to substrate failure with same-request retry only
  because uncertain Store publication is the separate
  `CommitOutcomeUnknown` reconciliation boundary.
- A hot projection that proves an exact historical command is archived maps to
  `admission_denied/archived_replay_required/correct_and_retry`; callers must
  select a transport with the explicit archive resolver instead of retrying the
  unchanged hot-only path.
- The typed `PagedScopeRequired` boundary maps directly to
  `admission_denied/paged_scope_required/correct_and_retry`, preserving its Run,
  Scope, and count in the human diagnostic. Never classify it from Display text
  or retry the same non-paged command automatically.
- Evolution's typed collection provider failures reuse Runtime's exact five-way
  projection. Revision and immutable-history conflicts retain different retry
  meanings; provider codes and messages remain separate fields.
- A timeout before durable store/claim admission permits the identical request.
  A timeout after a Running Attempt was persisted remains `timed_out` but
  requires `refresh_and_retry`: the caller must obtain fresh Clock authority and
  issue the explicit takeover command. It is never same-request retry.
- Pure Evolution verification errors stay typed through `EvolutionError`.
  `ReadRequired` is an internal pinned-view preparation request and maps to a
  terminal framework contract violation if it ever escapes into CLI transport;
  it is never a caller retry or lazy-read instruction.
- Never expose unrestricted raw event append.
- Local process execution uses only `cymule-executor-process`. It copies the
  selected executable into a private sealed location, hashes those exact launch
  bytes, and seals the advertised manifest into `cymule.execution-binding/2`
  before constructing the runtime. There is no second launcher, mutable-path,
  ambient-environment, or implementation-ID-only binding fallback.
- RPC SIGINT/SIGTERM uses the executor's lock-free shared cancellation token.
  Do not restore a heap-only signal flag or polling helper thread: forked launch
  decisions use the same retained pre-start/post-start receipt as Runtime errors.
- RPC stdin is Unix-only, uses cancellation-aware bounded polling, and admits at
  most 64 MiB including the Engine envelope. The reader terminates on max plus
  one before JSON allocation; non-Unix cancellation construction fails before
  stdin is read.
- RPC stdout uses the same cancellation source with nonblocking bounded polling.
  A blocked response write must terminate promptly on SIGINT/SIGTERM; because
  the protocol could not be carried, that path is a nonzero transport failure,
  never a fabricated success or a second semantic envelope.
- The package is `cymule-cli` and installs the `cymule` binary. Keep binary
  rustdoc disabled so it cannot collide with the public `cymule` facade library;
  user API documentation belongs to the facade and profile crates.
- Dispatch provider-neutral store targets to directory or SQLite adapters.
  Official ingress accepts exactly cymule.directory-store/5 and
  cymule.sqlite-store/6; predecessor selectors fail before path resolution or
  any Store I/O and have no alias.
  The internal provider enum must forward the complete `DurableStore` contract,
  including bounded `load_head`, current-root typed history reads, Machine
  archive access, and both explicit GC operations; it may not inherit a default
  missing-capability path for an operation implemented by both providers. Full
  projection traversal is only the explicitly named `load_full_audit` path and
  is never ordinary reopen. StateRoot lowering remains Durable-owned: the enum
  forwards `with_state_root_resolver` and never asks a Store provider to
  construct or reinterpret a semantic transition.
  Read-only commands never construct an executor.
- Migration and shadow process I/O uses the single public closed wire envelope
  from `cymule-evolution` behind the Durable Evolution provider registry. The
  CLI has no private copy, adapter host, or stateful execution route for that
  protocol.
