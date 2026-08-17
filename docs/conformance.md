# Conformance

Status: implemented for the Semantic Interpreter and Embedded profiles.

## Profiles

| Profile | Status | Required behavior |
| --- | --- | --- |
| Semantic Interpreter M0 | Implemented | frozen IR, canonical stores, admission, reducer, exact state replay |
| Embedded M0 | Implemented | one-shot in-memory execution, suspension boundary, process plugins, SDK facade |
| Durable Single Domain | Partial | snapshot/restore, CAS, Continuation, identified signal/timer activation, lease, outbox, occurrence replay, Resource handoff, directory-store reopen, and ambiguous-effect reconciliation; nested scopes and the full crash matrix remain |
| Optional Agent Interaction plugin | Partial plugin suite | separately owned Session, occurrence, input, workspace, and stream behavior over generic M1 interfaces; not a framework profile |
| Large Virtual Graph | Partial | bounded virtual regions, M1 cursor/frontier checkpoints, exact parked index, atomic activation wake, fair capability claims, fencing, and restore; durable compaction remains |
| Replicated Domain | Proposed | fenced ownership, failover, no split-brain commit |
| Strong Isolation | Proposed | untrusted code, secret, network, and tenant isolation |
| Live Evolution | Partial | Plan DAG, impact, occurrence pins, deterministic canary/rollback, safe-point migration receipts, and shadow evidence; runtime rollout automation remains |

The M0 rows do not claim persistence. The partial M1 implementation does prove
single-domain durable wait resumption, exact replay of recorded component
outputs, and reconciliation after an ambiguous mutating dispatch. It does not
yet claim the complete nested-scope runtime or every crash window.

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
- identical signal/timer activation redelivery returns the original durable
  decision, source mismatch and conflicting ID reuse fail, one signal token
  consumes at most one consume-once wait, and stale writers commit nothing;
- a process reopening after wait activation advances the Continuation epoch and
  begins a new fenced Attempt before interpretation;
- virtual source cursors and bounded frontiers reopen from chained M1 journal
  checkpoints, stale CAS rolls back the in-process scheduler, exact reason wake
  avoids a parked scan, and activation plus M3 wake commit atomically;
- cursor-version changes, stalled cursors, repeated work identities, and partial
  source failures advance neither cursor nor materialized frontier;
- restored virtual snapshots reject duplicate work placement, missing region or
  known-set identity, malformed claim fencing, and per-Run frontier overflow;
- a Binding Context update changes only future occurrences;
- replay availability is not reported as exact when an artifact is missing;
- TypeScript, Python, Rust, and Go author the same plan and execute through the
  same Rust kernel and external plugin;
- TypeScript, Python, Rust, and Go submit the same Resource Candidate to the
  Rust resource sealer and receive the same Resource ID;
- TypeScript, Python, Rust, and Go construct the same identified wait activation
  fixture and validate its closed wire contract through the Rust Engine;
- Resource identity ignores locations, public credential-bearing URLs fail,
  bounded reads/lists reject malformed adapters, content bytes are verified,
  and Run-to-Run handoffs survive M1 reopen and reject conflicting transfer IDs.

The optional Agent interaction plugin runs a separate Rust conformance suite for
Session projection replay, binding-pinned occurrences, atomic input and stream
checkpoints, workspace effects, and receipt-loss recovery. Those cases validate
the plugin's use of M1 interfaces; they are not required behavior of the core
CLI or four language SDKs.

## Cross-axis scenario

Status: partial. The current suite composes a mutating effect, ambiguous
dispatch, future binding update, pinned reconciliation, obligation settlement,
and replay. Stale-command and epoch-fencing axes are covered independently. A
single crash-injected scenario that also includes a speculative scope is an M1
durable-profile gate and is not claimed by version 0.1.0.
