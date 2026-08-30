# Cymule Agent Interaction Plugin

`cymule-agent` is the optional, provider-neutral Agent profile for Cymule. It
defines typed Session metadata, messages, tools, host occurrences, input waits,
workspace decisions, and output streams without making an Agent loop part of
the semantic kernel.

The terminal persistence boundary is intentionally closed:

- mutations are `AgentCommand` values and return a self-authenticating
  `AgentCommandReceipt` inside an `AgentCommit` envelope;
- ordinary reads are exact keyed lookups or revision/head/generation-pinned
  bounded pages;
- Session current contains bounded metadata only—messages, tools,
  elicitations, occurrences, streams, chunks, and role-free target claims have
  independent keyed authority;
- there is no public Agent journal, raw record, free StateRoot mutation, or
  all-history read API;
- exact command replay returns the same semantic receipt. Its
  `observed_revision` is the StateRoot head observed by that call, so a late
  replay may report a newer revision without writing. The required-nullable
  `committed_revision` is non-null only for this call's acknowledged new write;
  replay returns null even when the observed head is unchanged.

External provider products are not serializable command input. An external
stream records its pinned resolver plus expected media type, content digest,
and byte size. Before provider I/O, one generation-bearing target claim and the
Resource reservation CAS exclude every ordinary Message/Tool writer for that
exact Session-local target. Durable finalization derives a closed serializable
intent binding the exact source revision/digest, Session, stream, Finalize command, target,
resolver, and content; the provider product remains non-Serde. The provider
must publish that intent idempotently and return an exact readback before one
CAS commits the publication, catalog record, permanent Agent-stream Resource
pin, Session/stream currents, command, and receipt. Unknown publication or a
post-I/O CAS failure returns the same intent; `reconcile_finalization` requires
that restored intent, exact-matches it against freshly read touched state, and
performs observation only, never another publish. Workspace provider binding
and reconciliation results use the same
specialized authority boundary. Ordinary `commit_agent` rejects both
authority-requiring paths.

External media types use Resource `/4`'s lowercase ASCII type/subtype token
grammar without parameters. A durably `NotApplied` publication may be aborted
in one Agent/Resource CAS that closes the stream and releases its reserved pin;
that CAS also releases the exact target claim for later reuse. Claimed or
Unknown publication attempts remain reconciliation-only. Promotion uses the
family's current pin count, so unrelated sibling releases do not block the exact
reservation.

Production writers use the provider- and Clock-bound
`cymule_durable::DurableAgentControl` borrowed from the owning Durable runtime;
Store control exposes Agent reads only. This crate implements
`AgentPersistence` directly for the Runtime writer: every method enters its
matching closed Durable command or exact-read seam. Structured
profile and Durable errors retain their closed category and original stable
fields; identity mismatches are Integrity failures and encoding is not caller
Validation. No mapping derives its contract from display text. There is no
generic transaction adapter or fallback persistence path.

Workspace `StartEffect` carries a required framework-issued lease request:
derived claim owner, exact Run-scoped Clock observation reference, and positive
TTL. Other workspace phases carry explicit null. Runtime resolves the current
Clock under the final CAS guard and commits the five Effect stages, scope and
Continuation, obligation, claimed outbox, lease, Agent occurrence, and typed M1
receipt together. Repeated Unknown evidence returns a typed `Unchanged` current
with no CAS and no fabricated receipt; new evidence remains an atomic
Agent/M1 transition.

Input suspension and completion are coupled to an existing Plan-owned M1 Wait.
The command binds the exact Run and complete `WaitOwner`; the checkpoint binds
the response-derived `cymule.wait-result/1` Artifact and typed M1 receipt
references. The Durable façade resolves those references and verifies the Wait,
Continuation, Session, and elicitation in the same CAS.

`EphemeralAgentPersistence` is an explicitly named process-lifetime
implementation for local tests and tools. It supports ordinary Session,
occurrence, and staged-stream commands, but deliberately rejects M1 input,
workspace authority, and external Resource finalization. It is never selected
implicitly by a controller.

Standalone host calls persist Prepared and Started before dispatch. Only the
call that receives a verified fresh Started acknowledgement may invoke the host;
concurrent command replay and lost acknowledgement never grant redispatch. Any
ordinary error after Started is recorded as Unknown and returned only as
`HostOutcomeUnknown { occurrence_id }`; a timeout or cancellation does not
pretend that external work never began. Reconciliation observations are
content-identified, append-only, bounded to 64 total entries, and preserved
across reopen. Repeating identical evidence performs no write, while new
evidence advances the occurrence and cannot replace older observations.
An unresolved Context call is deliberately stricter: recovery cannot accept a
Completed snapshot after the original pinned message-reader capability is gone.
Started is retained as Unknown with a stable reason, and repeated Completed
claims over an already Unknown Context perform no write and still return
`HostOutcomeUnknown`. A previously committed terminal Context response replays
normally, and NotApplied evidence remains terminal. There is no serialized
context-proof DTO.
The reference turn driver uses this same execution path for every host operation;
it does not maintain a second occurrence writer or dispatch rule.

Session construction is also explicit: controller `open` methods require the
Session key to be absent at the revision they read, while `resume` requires an
existing current. Missing durable state is never interpreted as an implicit
resume-time genesis.
An unknown first Prepare acknowledgement preserves the original absence pin,
so retry cannot silently join another caller's newly created same-named Session.

ACP, MCP, A2A, editor, model-provider, and concrete loop integrations belong in
separate adapters above this crate. The included `AgentTurnDriver` is a bounded
synchronous reference driver; it reads context only through a monotonic,
revision-pinned message-page capability. The capability fixes both source head
and message count. Its cumulative byte budget charges only canonical message
currents, while the complete page wire keeps a separate hard limit, so page
partitioning cannot renew or consume extra scan authority. A returned context
must echo that source descriptor and may cite only exact message
id/index/digest bindings delivered by the capability; unread or changed
references are rejected. The driver admits at most 64 model rounds per turn.
`with_max_model_rounds` validates `1..=64` and returns `AgentResult`.

Add the crate with:

```sh
cargo add cymule-agent
```

Run its conformance suite from the repository root:

```sh
cargo test -p cymule-agent --all-targets --locked
```

See [`PROFILE.md`](PROFILE.md) for the complete authority, storage, and replay
contract.
