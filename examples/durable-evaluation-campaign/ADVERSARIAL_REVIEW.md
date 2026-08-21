# Adversarial Review

This review treats the example as a user-visible recovery application rather
than a happy-path demo. The assertions below are backed by
`tests/adversarial.rs` unless a residual limit is stated explicitly.

## Authority and identity

- The suite is imported once as a content-addressed Resource. Reopen verifies
  its size and digest before status or execution.
- Every case ID is unique, printable, and bounded. Its Work ID and payload
  reference are deterministically derived from the pinned canonical case.
- A claim records the exact immutable linked Plan and execution-binding
  Artifact as separate identities before the process plugin is invoked. An
  evolution command cannot reinterpret an already admitted case.
- Command, checkpoint, revision, occurrence, Plan, Resource, and Artifact
  identities occupy separate domain-separated namespaces.

## Failure windows reviewed

| Boundary | Allowed state after exit | Reopen behavior |
| --- | --- | --- |
| Before claim CAS | case remains ready | another worker may claim it |
| After claim CAS, before plugin | one running fenced occurrence | no stealing before expiry; explicit retry after expiry |
| After pure plugin, before result CAS | running occurrence, outcome unrecorded | the pure subject may be invoked again after fenced recovery |
| After result CAS, before caller sees output | terminal occurrence and Artifact | report replays the retained result; no new occurrence |
| External process kill at the exact pre-invocation barrier | three terminal results plus one active claim | read-only observation remains non-mutating; expiry fences one explicit retry; all cases finish once |
| During compatible publication | old or new complete registry checkpoint | future work uses one verified immutable head |
| After compatible publication | earlier occurrences retain old Plan | later claims pin the new Plan |

The first implementation review found a split checkpoint between frontier
materialization and payload Artifact persistence. A crash there could have
left a durable claim whose payload was absent. The final design derives each
payload identity from canonical case bytes already retained in the suite
Resource, then verifies that derivation again before execution. No second CAS
is needed after a frontier checkpoint.

The review also found that a stable capacity-slot Resource would keep a newly
started process behind the previous process's completed lease. Slots are now
worker-specific while the scheduler's `max_active = 1` and store CAS remain the
admission authority. Two contenders can prepare locally, but only one claim CAS
can commit before any plugin call.

A third review finding covered acknowledgement loss between publishing the
suite Resource and checkpointing campaign metadata. Whole-file filesystem
imports now replay a committed write ID only when every chunk and the final
length match the already published object. The campaign can therefore retry
that boundary without creating different bytes or becoming stuck behind its
own committed upload.

The suite file is read and validated once, and those exact bytes are submitted
to the Resource store. The store does not reopen the caller's path after
parsing, so a path replacement cannot make current work differ from the pinned
Resource.

## Malformed and hostile inputs

The application fails closed on:

- files larger than 8 MiB, more than 100,000 cases, or lines over 64 KiB;
- non-UTF-8 JSON Lines, blank lines, unknown fields, duplicate IDs, control
  characters, unsupported labels, and oversized messages;
- a suite path that is not a regular non-symlink file;
- a local suite whose bytes differ from the campaign's pinned Resource;
- a retained Resource whose content no longer matches its size or SHA-256;
- process-plugin output outside the protocol, over 1 MiB, after five seconds,
  or from a non-zero exit;
- an occurrence referring to a Plan absent from verified registry history;
- an active lease that has not expired under the caller-supplied logical time.

## Evolution attacks

The example's scorer reference uses `latest_compatible`, the framework default.
A compatible definition produces a new immutable parent Plan for future claims.
A definition with a changed input schema is stored as history but cannot move
the current link. Existing occurrence bindings never change in either case.

The broader live-evolution controller additionally checks new component, effect,
capability, authority, migration, canary, shadow, and rollback evidence. This
example focuses on reusable-definition compatibility and occurrence pinning;
it does not pretend to demonstrate every rollout mode in one CLI flow.

## Residual boundaries

- The suite parser retains at most 8 MiB in memory while the scheduler keeps only a bounded
  schedulable frontier. A production billion-case source should implement a
  paged Resource/index adapter rather than raise this example limit.
- Process execution is isolation by protocol and resource bounds, not a
  security sandbox. Run untrusted subjects in an isolation plugin.
- A pure subject may be repeated after an unknown pre-checkpoint outcome. A
  mutating subject needs Effect idempotency and reconciliation instead.
- SQLite provides one local durable domain. Cross-host ownership and failover
  require a different DurableStore and deployment-level coordination.
- The external process-kill campaign covers this durable virtual-work path, not
  every internal effect, wait, compaction, or migration crash window. Those
  deterministic fault matrices remain independent evidence until equivalent
  black-box kill campaigns are added deliberately.
- The local filesystem Resource adapter protects content integrity, not secrecy
  or access control. Production resolvers own credentials and authorization.
