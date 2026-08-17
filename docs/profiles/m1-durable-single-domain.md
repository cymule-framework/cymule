# M1 Durable Single-Domain Profile

Status: partial.

## Implemented foundation

- portable `MachineSnapshot` with deterministic projection rebuild and restored
  command deduplication;
- provider-neutral whole-state compare-and-swap `DurableStore`;
- complete typed Continuation fields for frame, state, waits, scopes,
  obligations, leases, budget, causal frontier, and epoch;
- idempotent wait registration/completion;
- logical-clock authority leases and fencing epochs;
- effect outbox enqueue, claim, settlement, and explicit `unknown`;
- canonical component occurrence inputs, outputs, binding, and revision;
- portable snapshot metadata;
- shared-memory CAS reference and atomic directory-store adapter;
- resumable sequential `call`/`wait` interpretation with process reopen, epoch
  advance, and component-result replay without reinvocation;
- commit-gated root effects with atomic outbox enqueue, fenced
  `DispatchStarted`, settlement, and reconciliation recovery;
- crash-after-provider-application tests proving restart performs reconciliation
  without a second dispatch;
- reopen, interrupted-staging, stale-writer, stale-claim, and idempotency tests.

## Remaining completion gates

- resumable interpreter integration for nested scopes, observational eager
  effects, and explicit-release effects;
- timer and signal activation workers;
- atomic semantic event plus outbox publication;
- component-result replay without reinvoking plugins;
- crash injection at every prepare/commit/dispatch/receipt window;
- snapshot compaction and suffix rehydration;
- all SDK control/query surfaces and restart-level end-to-end tests.

No concrete storage product is part of this profile. An adapter conforms only
when it provides atomic whole-state CAS and passes the profile fault suite.
