# Conformance

Status: implemented for the Semantic Interpreter and Embedded profiles.

## Profiles

| Profile | Status | Required behavior |
| --- | --- | --- |
| Semantic Interpreter M0 | Implemented | frozen IR, canonical stores, admission, reducer, exact state replay |
| Embedded M0 | Implemented | one-shot in-memory execution, suspension boundary, process plugins, SDK facade |
| Durable Single Domain | Proposed | durable ack, timers, leases, crash recovery, snapshots |
| Replicated Domain | Proposed | fenced ownership, failover, no split-brain commit |
| Strong Isolation | Proposed | untrusted code, secret, network, and tenant isolation |
| Live Evolution | Partial | future binding update and pinning implemented; state migration proposed |
| Large Virtual Graph | Proposed | bounded materialization, parked index, compaction |

The implemented rows do not claim persistent VEC storage, durable resumption,
or exact execution replay of component outputs. Those are M1 gates.

## Required semantic cases

The local suite verifies:

- identical Plan Candidates seal to an identical Plan ID;
- malformed plans and unknown references fail before hashing;
- missing causal parents and tampered event IDs are rejected;
- independent event order produces the same projection digest;
- command retry returns the original receipt and semantic reuse is rejected;
- stale precondition tokens return a structured conflict;
- a stale Attempt cannot yield after an epoch advance;
- scope commit closes internal state and transfers obligations exactly once;
- effect transitions reject illegal jumps;
- dispatch ambiguity becomes `unknown`, never a fresh intent;
- reconciliation retains the original occurrence binding;
- a Binding Context update changes only future occurrences;
- replay availability is not reported as exact when an artifact is missing;
- TypeScript, Python, Rust, and Go author the same plan and execute through the
  same Rust kernel and external plugin.

## Cross-axis scenario

Status: partial. The current suite composes a mutating effect, ambiguous
dispatch, future binding update, pinned reconciliation, obligation settlement,
and replay. Stale-command and epoch-fencing axes are covered independently. A
single crash-injected scenario that also includes a speculative scope is an M1
durable-profile gate and is not claimed by version 0.1.0.
