# Cymule Process Executor

`cymule-executor-process` is a hardened one-request-per-process implementation
of Cymule's `PluginHost` contract.

```sh
cargo add cymule-executor-process
```

At construction, the executor captures the selected executable and optional
working-directory tree. The resolved root is opened component-by-component
without following symlinks; every descendant is opened relative to its retained
parent descriptor with `O_NOFOLLOW`, and metadata plus bytes come from that same
descriptor. Traversal uses an explicit heap stack, so an admitted directory
chain cannot exhaust the executor thread's call stack. Every request runs from a
fresh private materialization. Materialization and reclamation retain the root
and parent descriptors, pass one component to `*at`, use an iterative heap
cursor, and keep constant descriptor usage across depth. Root and directory
modes are fixed at `0700`, the sealed executable at `0500`, and working files at
authenticated `0600` or `0700`; umask and source-directory mode cannot
reinterpret that `/2` generation. Therefore paths may exceed `PATH_MAX`, and
so a plugin that changes its own file or working data cannot change a later
occurrence. When no working tree is configured, the child runs from that fresh
private occurrence root; it never inherits the caller's current directory. Its
canonical implementation revision covers the executable,
arguments, explicit environment, working tree, declared runtime closure,
deadline, and byte limits; use that revision in `cymule.execution-binding/2`.
Construction requires a nonempty, provider-owned `runtime_closure`; there is no
inferred host-ABI entry. An OS/architecture label does not identify an
interpreter, loader, shared library, sidecar, or other runtime facility. Supply
an immutable aggregate runtime generation or the immutable admitted revision of
each such facility. The executor treats this as a declared binding and does not
claim to discover a complete host dependency closure. Each supplied revision is
the lowercase `sha256:<64 hex>` content ID of that frozen provider descriptor;
arbitrary labels and mutable version names fail construction.

`closure_limit` covers the complete deterministic length-prefixed closure, not
only file payloads: configuration strings and counts, timeout and byte policy,
executable bytes, every working-tree path/type/mode, and all file bytes consume
the same budget. Arguments, environment entries, runtime revisions, and
working-tree entries also have fixed count ceilings, enforced before collection
growth. Empty-file or empty-directory fanout therefore cannot bypass the
closure bound.

Each request runs in a dedicated Unix process group with no inherited
environment. Before spawn, the actual serialized plugin, migration, or shadow
request must pass the shared duplicate-free, finite-number, exact-integer JSON
contract. Bounded, chunked private-closure materialization, child start, stdin,
stdout, stderr, process observation, group termination, and child/watchdog reap
share one monotonic deadline. The process-group watchdog starts before
`Command::spawn`, so it can terminate the exact group while the parent is still
waiting on Rust's child exec-error pipe. The child decides at one process-shared
launch gate immediately before exec. Owner cancellation and watchdog expiry
compete on that same receipt: a pre-start winner prevents provider I/O, while a
launch-committed mutating call remains an unknown-world outcome.

For `cymule.plugin/3`, Runtime's 8 MiB Core-Artifact ceiling is the semantic
request and response limit. `message_limit` must equal it exactly before an
ordinary plugin process may start; an oversized in-process or child-process
product fails before schema validation or Artifact construction.

Migration and shadow providers use the dedicated Evolution raw-byte entrypoint.
It fixes both directions to the protocol's 16 MiB limit and returns raw bytes to
the Evolution strict decoder; there is no generic JSON fallback or
caller-selected process bound for that protocol.

The final executable inherits only stdin, stdout, and stderr. Linux marks the
rest close-on-exec atomically with `close_range(CLOEXEC)`. On Apple platforms,
the parent allocates a bounded `proc_fdinfo` table before fork and each child
uses one `proc_pidinfo(PROC_PIDLISTFDS)` kernel-wrapper call to enumerate only
its actual inherited open descriptors. Plugin pre-exec preserves existing
descriptor flags while adding `FD_CLOEXEC`; the watchdog closes only enumerated
descriptors other than its two private channels. Other Unix targets retain the
finite hard-limit fallback. On Apple, lowering either `RLIMIT_NOFILE` value
after a higher descriptor was opened cannot remove that descriptor from the
parent-sized table authority. This preserves Rust's internal exec-error pipe until exec while
excluding every ambient Engine descriptor from plugin authority without a
sparse-ceiling scan.

Private-directory deletion is an explicit host reclamation phase after process
authority has ended, not part of the provider execution deadline. A reclamation
failure converts an otherwise successful invocation to `process_cleanup_failed`;
it never replaces an already terminal provider error. Reclamation never uses
path-recursive deletion, follows no symlink, restores `000` directories through
descriptors, and authenticates the root inode before final removal.

The Engine retains one endpoint of a private liveness channel. A minimal
fork-only watchdog leads the occurrence process group and monitors the other
endpoint through the reviewed no-userspace-state syscall boundary, including
Apple's single-call private libproc wrapper. It also binds the exact
fork-time Engine PID. The spawning parent thread blocks every signal across
`fork`, establishes the watchdog's exact process group as the only `setpgid`
writer, publishes a fixed group gate over the liveness socket while still
masked, and then restores its exact prior mask. The child inherits the fully
blocked mask and cannot enumerate
descriptors or publish readiness until it consumes that gate and verifies its
PID equals its process group. Fixed stage bytes distinguish group-gate,
group-verification, descriptor-table, descriptor-close, retained-channel, and
ready-write failures inside the child, while parent signal-mask and group-write
failures retain their own typed errors. Deadline termination therefore has
authority even if a later query blocks. The watchdog verifies on each short
poll tick that the Engine is still its direct parent. Engine exit or `SIGKILL`
therefore closes the group
even if multiple children are simultaneously blocked before exec while holding
one another's inherited channel descriptors. Normal completion, timeout,
cancellation, and I/O failure also terminate the whole group and reap both
direct children. Forked plugin children cannot keep a request open or perform
late work after the leader returns. Stderr remains diagnostic only.

Invalid requests and attempts are deterministic before spawn. Once either
Effect dispatch or reconciliation has spawned, a timeout, cancellation, I/O or
output failure, lost process, malformed response, or mismatched attempt is an
unknown-world outcome under the original intent. Reconciliation authority and
retry policy remain owned by the Cymule durable outbox.

This crate supports Unix process-group and permission semantics; construction
fails on other platforms. It is a transport, not a sandbox: a hostile process
can deliberately leave a process group or exercise the caller's same-UID OS
authority. Use an OS/container/Wasm executor plugin for untrusted code.
