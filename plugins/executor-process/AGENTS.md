# Process Executor Guidance

- Start one supervised Unix process group per typed plugin request. The Engine
  retains one private liveness-channel endpoint; a fork-only watchdog is the
  group leader and kills that exact still-live group when Engine death closes
  the channel. Clear ambient environment and pass only explicit configuration;
  never inherit credentials or `PATH`.
- Capture executable bytes and the optional working-directory tree once, then
  create a fresh private materialization for every occurrence. A plugin may
  mutate its disposable occurrence but must never affect later invocations.
- Materialization and reclamation are iterative, descriptor-relative, and
  constant-FD across depth. Retain the parent/root descriptors and root inode,
  pass one component to `*at`, never fall back to `TempDir`/`remove_dir_all`,
  and authenticate the final name. `/2` fixes root/directories at `0700`, the
  sealed executable at `0500`, and working files at `0600` or `0700`.
- Resolve the configured working-directory root once, open every resolved root
  component without following a symlink, and traverse descendants exclusively
  through retained directory descriptors plus `openat(O_NOFOLLOW)`. Metadata
  and bytes must come from the same opened descriptor; never inspect a path and
  reopen it later. Keep traversal frames on an explicit heap stack: directory
  depth is bounded by the closure byte and entry ceilings, never by recursive
  growth of the executor thread's call stack.
- A missing configured working-directory tree selects the fresh private
  occurrence root. Never inherit the Engine process's ambient current
  directory.
- The canonical implementation revision covers the executable digest, ordered
  arguments, sorted explicit environment, captured working-tree identity,
  runtime-closure revisions, timeout, and byte limits. Never substitute a raw
  executable digest for that binding identity. Require the provider to supply a
  nonempty runtime binding whose values are lowercase SHA-256 identities of
  frozen closure descriptors; never synthesize one from the host OS and
  architecture or accept a mutable label as a revision.
- Bound request, stdout, stderr, process observation, tree termination, leader
  reap, and pipe drain under one absolute invocation deadline. Use nonblocking
  pipe I/O; do not add reader threads, process pools, or global locks.
- `closure_limit` bounds one deterministic length-prefixed footprint containing
  configuration strings and counts, policy fields, executable bytes, every
  captured path/type/mode, and file bytes. Enforce the fixed configuration and
  working-tree entry-count ceilings before growing a collection. Integer
  overflow is an over-limit failure, never a saturated valid length. Closure
  materialization chunk ends and post-fork descriptor ranges likewise derive
  from checked bounds rather than saturating arithmetic.
- The same absolute deadline and cancellation authority cover private closure
  materialization through child start and provider completion. Start the
  process-group watchdog before `Command::spawn`; it holds the monotonic
  deadline while the parent may be waiting on the child exec-error pipe. The
  child checks that same deadline immediately before exec, so a child that
  reaches the spawn boundary after watchdog expiry cannot invoke plugin code.
  Materialize in bounded chunks and never perform an unbounded whole-file read
  before enforcing the closure limit.
- Process-group termination and both child reaps end provider authority under
  the invocation deadline. Explicit temporary-directory deletion is a later
  host reclamation phase: a cleanup failure turns an otherwise successful call
  into `process_cleanup_failed`, while an existing provider error remains the
  terminal result. Do not claim filesystem deletion is deadline-bounded.
- Validate the actual serialized outbound plugin or adjacent provider-protocol
  bytes against the shared strict JSON domain before materialization or spawn.
  An invalid request or provider attempt is deterministic only while still on
  that pre-spawn side of the boundary.
- For `cymule.plugin/3`, require the executor configuration to equal Runtime's
  fixed Core-Artifact semantic ceiling before materialization or spawn. Neither
  a narrower nor wider transport configuration represents the complete frozen
  plugin domain. Evolution retains its separate exact 16 MiB entrypoint.
- Kill the complete occurrence process group after the leader exits or any
  failure occurs, then reap both the plugin child and the watchdog. Ordinary
  forked descendants must not outlive the occurrence or retain its pipes.
- After either Effect dispatch or reconciliation has spawned, every timeout,
  cancellation, I/O failure, output-limit failure, process failure, malformed
  response, or attempt mismatch is an unknown-world outcome. Kill and reap the
  group, return the ambiguity to the runtime, and never retry inside this
  plugin.
- Validate the response through the frozen `cymule.plugin/3` types. Stderr is
  diagnostic only and must not become a result channel.
- Conformance process fixtures must consume the complete request before writing
  a response. An early child exit that closes stdin is a failed dispatch, even
  if the child happened to emit response-shaped bytes; do not suppress EPIPE.
- This crate is Unix-only and deliberately rejects other platforms at
  construction. A process that deliberately escapes its process group requires
  an OS/container/Wasm sandbox executor; this transport does not claim
  untrusted-code isolation.
- No process pool, Agent Loop, shell interpretation, sandbox policy, or network
  authority belongs in this crate. Higher-isolation executors are separate
  plugins.
- The adjacent Evolution process protocol uses only
  `invoke_evolution_bytes`: its raw request and response limit is the fixed
  runtime-owned 16 MiB protocol constant, and Evolution alone performs strict
  typed decoding. Reject a max-plus-one raw request before strict JSON parsing,
  closure materialization, or process spawn. Do not add a generic JSON
  invocation fallback or a caller-selected per-invocation byte limit.
- The executor only transports and validates provider attempts. It must never
  keep a local Effect ledger or synthesize `Applied`/`NotApplied`; the invoked
  plugin's own world-authority ledger linearizes dispatch and reconciliation.
- The owning Engine may install a process-shared cancellation token. Owner
  cancellation, deadline expiry, and child launch commit compete on the same
  retained atomic receipt. A pre-start winner performs no provider I/O; after
  launch commit a mutating call is Unknown. Cancellation terminates and reaps
  the occurrence process group; the token is lifecycle
  control and is excluded from the immutable execution-binding identity.
- This crate's unsafe code is limited to the parent-created shared atomic
  launch mapping and fixed signal transition, fork-only watchdog, and plugin
  child pre-exec boundaries. Before plugin exec, Linux atomically marks every
  descriptor above stderr close-on-exec with `close_range(CLOEXEC)`. Apple
  allocates one bounded `proc_fdinfo` table in the parent; each forked branch
  uses the Apple-exported `proc_pidinfo(PROC_PIDLISTFDS)` private libproc
  wrapper's reviewed single `__proc_info` kernel call to enumerate its exact
  inherited open descriptors. Plugin
  pre-exec ORs `FD_CLOEXEC` into each actual descriptor above stderr and
  preserves every other descriptor flag. The Apple descriptor domain is the
  exact parent buffer capacity derived from the larger of the kernel process
  maximum and current FD-table requirement, plus fixed truncation slack; a
  later-lowered resource limit is not authority over an already-open high FD.
  Other Unix targets retain their finite parent-prefetched hard-limit fallback.
  The final executable inherits only stdin/stdout/stderr, while Rust's internal
  exec-error pipe remains usable until exec. After the watchdog fork, that
  branch closes the complete inherited descriptor table from the child-side
  authority: Linux uses the atomic `close_range` syscall; Apple closes only the
  exact enumerated descriptors other than its two retained channels; other
  Unix targets use the validated finite hard limit, never the mutable soft
  limit. Every post-fork table byte count, alignment, descriptor order/domain,
  duplicate, and retained-channel check fails closed. These branches may use
  only the reviewed descriptor-close/CLOEXEC, Apple `proc_pidinfo` syscall
  wrapper, `clock_gettime`, `poll`, `pause`, `sigfillset`, `sigprocmask`,
  `setpgid`, `write`, `read`, `getpid`, `getppid`, `getpgrp`, `kill`, and
  `_exit` operations; they must never return to Rust, allocate, lock, run
  destructors, read a descriptor directory, or call plugin code before exec,
  `_exit`, or `SIGKILL`. The parent establishes the exact watchdog process group
  immediately after `fork` and before readiness or any descriptor query; the
  child repeats that assignment before enumeration. A blocked query is
  therefore always killable by the retained deadline authority. The watchdog
  binds the exact fork-time parent PID and checks the direct-parent relation on
  each short poll tick, so mutually inherited pre-exec liveness writers cannot
  keep groups alive after Engine death.
