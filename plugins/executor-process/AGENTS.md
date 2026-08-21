# Process Executor Guidance

- Start one Unix process group per typed plugin request. Clear ambient
  environment and pass only explicit configuration; never inherit credentials
  or `PATH`.
- Capture executable bytes and the optional working-directory tree once, then
  create a fresh private materialization for every occurrence. A plugin may
  mutate its disposable occurrence but must never affect later invocations.
- The canonical implementation revision covers the executable digest, ordered
  arguments, sorted explicit environment, captured working-tree identity,
  runtime-closure revisions, timeout, and byte limits. Never substitute a raw
  executable digest for that binding identity.
- Bound request, stdout, stderr, process observation, tree termination, leader
  reap, and pipe drain under one absolute invocation deadline. Use nonblocking
  pipe I/O; do not add reader threads, process pools, or global locks.
- Kill the complete occurrence process group after the leader exits or any
  failure occurs. Ordinary forked descendants must not outlive the occurrence
  or retain its pipes.
- A timeout or lost process response is ambiguous for an effect. Kill and reap
  the child, return an error to the runtime, and let the existing outbox move
  the intent to `unknown`; never retry dispatch inside this plugin.
- Validate the response through the frozen `cymule.plugin/2` types. Stderr is
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
- Adjacent closed provider protocols may reuse the same sealed, bounded JSON
  process primitive without widening `cymule.plugin/2`.
- The owning Engine may install a process-local cancellation flag. Cancellation
  terminates and reaps the occurrence process group; the flag is lifecycle
  control and is excluded from the immutable execution-binding identity.
