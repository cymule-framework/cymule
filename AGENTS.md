# Cymule Project Handbook

## Authority

Use this precedence order when guidance conflicts:

1. Executable tests, frozen schemas, and live code.
2. The nearest `AGENTS.md` in the target path.
3. `docs/specification.md` for normative semantics.
4. `docs/architecture.md` for realization guidance.
5. Other documentation.

## Project invariants

- Keep the trusted semantic core in Rust and intentionally small. The core owns
  canonical identity, admission laws, deterministic reduction, and replay. It
  must not depend on a database, queue, network client, model SDK, object store,
  process supervisor, or UI framework.
- Canonical truth consists of sealed plans, causal events, and immutable
  artifacts. Views, indexes, graphs, attention items, and schedulers are
  rebuildable projections.
- Cross-Run resources use provider-neutral, versioned descriptors. Keep content
  identity, media/kind, shape, and replay evidence separate from resolver
  locators and access grants. Never persist credentials in an Artifact or claim
  exact replay for a mutable external locator without immutable version or
  content evidence. Concrete object stores, drives, sandboxes, and URL fetchers
  belong behind resolver/store plugins.
- Plans describe meaning and requirements. Runtime bindings describe concrete
  realization. Never place provider names, credentials, endpoints, or deployment
  topology in canonical plan semantics.
- Every persisted occurrence pins an immutable occurrence binding. M0 persists
  Attempt and Effect bindings; new component or plugin-owned domain occurrence
  records must add the same protection before claiming exact execution replay.
  Updating future defaults must never reinterpret historical work.
- Scope closure commits declared state and transfers effect obligations. It does
  not claim that the external world has settled.
- Only observational effects may dispatch eagerly while a scope is open.
  Commit-gated effects wait for their owning scope; explicit effects remain
  prepared until a caller issues the release control after commit.
- An ambiguous dispatch becomes `unknown` and follows reconciliation. Never turn
  it into a fresh semantic intent or silently redispatch it.
- Durable Effect stages validate an exact canonical Machine delta against their
  outbox mutation. Enqueue, dispatch-start claim, observation, and reconciliation
  may not carry unrelated Plans, Events, commands, or Artifacts; `Unknown` Event
  and outbox state commit together.
- External signal and timer delivery is an identified durable activation, not a
  direct worker wake-up. Match the Plan-declared source, commit the activation
  receipt with wait results and Continuation readiness, and advance the epoch
  before a reopened `Ready` Continuation resumes.
- Virtual-work cursors and bounded scheduler frontiers checkpoint through the M1
  application-journal CAS. Exact parked-wait indexes are rebuildable projections;
  activation and the corresponding indexed wake checkpoint commit atomically.
- Every virtual-work claim creates a binding-pinned, epoch-fenced occurrence
  before execution. Success, retry, park, failure, and cancellation are closed
  dispositions; a retry creates a later occurrence and never rewrites history.
- Weighted fairness applies to materialized, capability-compatible backlogged
  Runs. Persist integer weights, deficits, dispatch sequence, and ready age;
  region materialization uses a separate round-robin visibility guarantee.
- Region split/merge treats cursors as opaque. A pinned migration adapter must
  verify coverage evidence before one CAS retires sources and activates targets;
  old regions and already materialized work remain historical authority.
- Completed virtual history may move cold only behind an immutable byte archive
  interface. Rust computes and verifies manifest Artifact identity, causal-cut
  certificate, summary digests, retained terminal fences/bindings, and replay
  availability; partial rehydration restores only explicitly selected exact
  occurrences. Archive products, locators, and credentials are plugin concerns.
- Multi-worker M3 execution uses abstract capacity-slot leases, not worker-pool
  topology. Claim and lease admission share one M1 CAS; renewal advances the
  slot fence; normal resolution carries work/lease epochs and logical time;
  expired recovery is an explicit retry/fail/cancel command. Lease expiry alone
  never mutates state, and old worker output loses after expiry or takeover.
- Public mutation enters through typed commands with idempotent IDs and causal
  preconditions. Raw canonical event append is internal only.
- Prefer optimistic CAS, immutable records, idempotency, fencing epochs, and
  partitioned single-writer authority over locks. Core semantics must never
  depend on a blocking lock. A concrete adapter may use narrowly scoped,
  non-blocking writer exclusion only when required to implement its CAS contract
  and must surface contention instead of waiting indefinitely.
- Cross-language SDKs author the same frozen IR and use the same engine contract.
  They must not implement a second reducer or invent language-specific semantics.
- New behavior is provider-neutral by default. Concrete persistence, activation,
  execution, model, tool, and effect integrations belong behind plugin or
  substrate interfaces.
- Domain-specific Sessions, Agent Loops, transport streams, protocol objects,
  and their controllers belong in optional plugins. Core crates, CLI, and SDKs
  expose only technology-neutral semantic and substrate contracts.
- Keep all source code, comments, documentation, commit messages, schemas, and
  user-facing project metadata in English.
- GitHub Actions is the only publication authority for every public artifact,
  including packages, release assets, and future registry distributions. Local
  commands may build, test, stage, and inspect release bytes, but must never
  publish them or require a long-lived registry token.

## Change discipline

- Read the nearest nested `AGENTS.md` before editing a directory.
- Treat generated files and lockfiles as derived artifacts; update their source
  and regenerate them in the same change.
- Any semantic change requires a version-domain decision and updates to the
  normative specification, schemas, conformance tests, and SDK fixtures.
- Add a root rule only when it applies across multiple project areas. Put domain
  detail in the nearest nested handbook.
- Do not claim a conformance profile unless its complete fault-oriented suite
  passes. Mark planned and partial behavior explicitly.

## Required verification

Use `python3 scripts/test_harness.py plan --base <trusted-ref>` and run its
selected suites before an ordinary scoped commit. Run `./scripts/verify.sh` for
semantic version changes, profile claims, shared schemas, release/publication
changes, harness or CI changes, unknown routes, and before claiming complete
repository verification. The suite model and fault-test rules live in
`docs/testing.md`. At minimum, applicable changes must preserve:

- Rust formatting, Clippy, unit tests, and documentation build;
- deterministic canonical IDs and replay digests;
- JSON Schema validation fixtures;
- TypeScript, Python, Rust, and Go SDK end-to-end tests against the Rust engine;
- the MLIR workbench smoke test when the pinned host MLIR toolchain is available.
