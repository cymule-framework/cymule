# Cymule Semantic Specification

Implementation status: source integration is in progress for the bounded
semantic, embedded, durable single-domain, large-virtual-work, and
live-evolution profiles. Branch-wide verification and version-authority closure
remain pending the final source freeze. Distributed ownership, federation, and
strong isolation remain separate proposed profiles.

## 1. Normative language

`MUST`, `MUST NOT`, `SHOULD`, and `MAY` are normative terms. A realization may
claim only the profiles whose complete required conformance cases pass.

## 2. Public model

The default public model is:

```text
Flow -> Run -> Result

Inside a Flow:  call | wait | effect | scope
Outside a Run:  observe | decide | change
```

A Run may remain active across waits and retain intermediate Artifacts and
Effect observations before reaching a terminal Result. The Run is the default
live handle. Graphs are views. Incremental output transport and Agent streams
belong to their owning integration profiles; the frozen IR has no generic
stream-output operation.

## 3. Version domains

The sole version inventory is
[`versioning/version-domains.json`](../versioning/version-domains.json). The
[generated specification table](version-domains.md) is verified against that
registry, public Rust constants, schema canonical digests and `$ref` edges, and
the Cargo dependency graph. The inventory also scans private Rust Artifact
kinds and content-ID domains, every SDK, schemas, and release controllers. Code,
another document, or a package manifest may not introduce an unregistered
version authority merely because its literal is not a public constant.

Each installed runtime generation selects one exact closed decoder before
decoding. A protocol string is not a generation identity, and a decode failure
MUST NOT probe another shape or fall back to another generation. The release
BOM binds the registry digest, source SHA, schemas, and package manifests to one
workspace version. An immutable package version or release tag MUST NOT be
reused for different bytes.

Changing effect identity, scope isolation, authority, causal admission, replay,
or migration meaning requires a new semantic specification version.

### 3.1 Engine failures

Every CLI Engine request and response MUST use `cymule.engine/5`. Engine v4 is
unsupported: an implementation MUST reject it without trying a legacy decoder,
projecting a v5 receipt into an older outcome, or probing another response
shape. A semantic failure MUST be a successful transport response containing
exactly one closed failure object with category, phase, stable code, and
display-only message.
Contract identity, side, JSON Pointer, bounded issues, and retry disposition are
present only when the Engine has authoritative evidence for them. Failure
category and recovery disposition form one closed matrix: only transport
failure and not-found omit the disposition; every other category requires one
of its explicitly admitted dispositions, and unknown-world-outcome requires
reconciliation.

An Engine envelope contains exactly one of a success or failure. Success MUST
contain the complete inner `EngineRequest` value accepted by the strict decoder
and the closed response produced by executing that value. It MUST NOT echo only
an operation-specific projection. Failure contains exactly one structured error
and MUST NOT contain a request because envelope or request decoding may have
failed before a typed value existed. Every predecessor success shape is invalid
without fallback. Success response tags and their fields are closed.
Each request variant admits exactly one success-response variant: the request
and response discriminators form one closed pair and MUST NOT be validated as
independent unions.
Every public Run ID in a direct Engine request, embedded durable command, or M1
response projection MUST use the same 1..=512 non-control Unicode-scalar
identity contract. C0, DEL, and C1 controls are invalid; no enclosing Engine
shape may impose a narrower byte or character limit.
Nested discriminated responses, including completed-or-suspended execution and
returned evolution commands, reject unknown variants, overlapping variant
fields, and fields owned by another operation before the SDK returns them to a
caller.

An SDK MUST serialize one request envelope, strict-decode those exact emitted
bytes itself, retain the decoded inner `request`, and compare the success echo
for exact structural equality before interpreting the response. It MUST NOT
compare a pre-serialization host object whose omitted or normalized members
differ from the wire. Exact structural equality includes object-member
presence and array order but not object-member order: an omitted member and an
explicit `null` are different. Equality only after a typed optional/defaulting
decode that maps both to the same value is not conforming. This one echo
correlates Seal, Resource sealing, verification, Clock observation, durable
control and cancellation, execution, and live evolution. SDKs MUST NOT
recompute Rust-owned Plan IDs, Resource IDs, Clock scopes, rollout decisions, or
other derived results as a substitute for correlation. Echo equality does not
replace closed response validation or an operation's durable receipt. A
missing, malformed, or mismatched echo after a mutating request begins MUST be
reported as `unknown_world_outcome` with `reconcile`; the same defect on a
non-mutating request is an invalid response and grants no replay permission.

`observe_clock` success MUST be the typed
`ClockObservationResult { run_id, observation }`. Rust MUST construct that
result only after the durable-protocol verifier proves that the opaque
observation scope is derived for `run_id`. Every SDK MUST compare the returned
Run and Clock source generation with the exact request before exposing the
nested reference; SDKs MUST NOT implement the Clock-scope derivation.

Typed Engine admission MUST also compare each strictly parsed raw request or
response with its typed reserialization before executing or exposing it. The
comparison is lossless after mathematical-integer normalization: object-member
presence, array length and order, value kind, and scalar value must all remain
identical. Typed decoding must not omit or synthesize a member, collapse or
reorder a collection, or replace a scalar. This check does not reject a required
nullable member whose typed serialization retains `null`. Request
failure is `validation/correct_and_retry` before operation I/O; malformed
responses follow the read-only invalid-response versus mutating
`unknown_world_outcome/reconcile` boundary above.

Failure categories distinguish transport, validation, contract violation,
admission denial, conflict, absence, declared plugin failure, plugin defect,
substrate failure, cancellation, timeout, and unknown external-world outcome.
An adapter error is not a declared plugin failure unless the selected operation
contract declares it. Failure to receive an Engine envelope MUST NOT imply that
replaying a potentially mutating request is safe. `reconcile` is the only retry
disposition for an admitted external intent whose world outcome is unknown.
Contract projection MUST sort and deduplicate its issue set and retain at most
99 concrete issues plus one deterministic omission summary. Before stdout
serialization, the Engine MUST validate its own failure projection; an invalid
internal projection becomes one closed `plugin_defect/never`
`cymule.engine/5` envelope rather than an unversioned stderr-only failure.

The CLI RPC stdin boundary is Unix-only and MUST read through bounded polling
that observes SIGINT/SIGTERM cancellation while a partial pipe remains open.
The complete Engine envelope has one fixed 64 MiB raw limit; byte 64 MiB plus
one MUST terminate before strict JSON decoding or typed allocation. This bound
is wider than every currently admitted inner provider message plus envelope
overhead and is not an operation-specific payload limit. Every SDK MUST apply
the same bound to its actual UTF-8 encoded complete envelope before starting a
CLI process or invoking a custom transport. If a child closes stdin before the
complete request is written, only one fully validated failure envelope remains
authoritative; a success is response loss and follows the request's read-only
versus mutating recovery classification.

The CLI MUST emit compact JSON. One closed success response payload is bounded
to 64 MiB, and the complete success envelope is bounded to 128 MiB plus the
exact 32-byte compact-framing delta. The latter
is the single response-stream authority: it covers the maximum accepted inner
request echo plus the maximum response payload because the request framing
removed from the echo is 48 bytes while success framing is 80 bytes. SDKs MUST
retain at most that envelope plus one overflow byte on stdout. Stderr has a
separate 1 MiB diagnostic-only bound plus one overflow byte and never consumes
semantic response capacity. A complete valid failure envelope is authoritative
even when the Engine exits nonzero or closes stdin early; process status is
consulted only after failure admission. A success additionally requires a
complete request write and a successful process status.
The Engine measures the actual compact normalized request echo before dispatch
and rejects more than `64 MiB - 48 bytes` as local
`validation/correct_and_retry`; no Store, provider, or external effect may run
first. After execution it separately measures the actual compact response
payload at 64 MiB and the actual final envelope at
`(64 MiB - 48) + 64 MiB + 80`. JSON escaping and normalization are therefore
charged from the bytes that will actually be emitted, not inferred from the raw
input length.

SDKs MUST preserve the complete failure object. Process status and stderr are
transport diagnostics only and MUST NOT become a parallel semantic error path.
The CLI transport's `execute_durable` request MUST invoke the Rust durable
runtime against separate provider-neutral Store, executor, and Clock targets.
Target capability presence MUST equal the selected command requirements:
queries, wait activation, and cancellation carry neither executor nor Clock;
Effect resolution carries only an executor; start, resume, takeover, and Effect
release carry both. Missing or extraneous authority is request validation before
Store I/O. Queries MUST use a Store opener that cannot initialize, create a
writer lock, clean, reclaim, or reconfigure durable authority. Exact terminal
Effect-resolution replay MUST first use that read-only Store and return the
retained receipt without constructing the historical provider; only an
unresolved Effect proceeds to provider preflight and writable control. Its
`execute_live_evolution` request MUST commit one closed command through the
provider-bound `DurableEvolutionControl` in the same durable domain. Migration
and shadow processes MUST be sealed to the caller-pinned revision and speak
`cymule.evolution-plugin/3`. Validation-only requests are not execution
receipts. Duplicate JSON object keys, non-finite
numbers, and integers outside the shared exact range of
`-9007199254740991..=9007199254740991` MUST be rejected before semantic
admission. If a deadline or cancellation loses the response to a mutating
request, SDKs MUST report `unknown_world_outcome` with `reconcile`.
A store head publish or transaction commit whose receipt is unavailable has the
same closed failure and recovery disposition; it MUST NOT report
`retry_same_request`.
Strict Engine JSON additionally admits at most 128 nesting levels, 256 bytes
per number token, and six exponent digits. Its bounded raw scan precedes the
host parser. For every non-integral number it records a JSON-pointer-keyed
canonical decimal rational; typed reserialization must retain the same map.
Thus `0.10` and `1e-1` are equal, while
`0.1` and `0.100000000000000005` cannot collide through a host binary float.

Mutating durable preflight MUST resolve Store and Clock locations once through
their nearest existing canonical ancestor, use those same stable locations for
the actual provider open, and reject overlapping provider-owned footprints. A
directory Store owns its complete subtree. Each SQLite Store or Clock owns its
base file plus the `-wal`, `-shm`, and `-journal` sidecars; overlap in either
direction is one authority conflict. It MUST capture the selected
executor bytes, perform exactly one Describe, derive the immutable binding from
that observation, and return a framework-owned token containing the exact host
and binding. Writable Store open precedes token consumption; runtime open MUST
perform no second Describe or provider I/O. Process occurrence materialization,
spawn, pipe I/O, tree termination, and reap share one absolute deadline and
cancellation authority; private closure copy is bounded and rechecked at the
spawn linearization point. Embedded Run, durable execution, migration, and
shadow requests carry one complete process target: executable locator, ordered
arguments, explicit environment, optional working tree, runtime-closure
revisions, deadline, message bound, and closure bound. The CLI MUST copy that
complete target into the executor without an ambient/default fallback.

Before admitting a fresh live-evolution provider capability, the CLI MUST open
an existing Store read-only and validate an exact retained command receipt. A
retained receipt returns without requiring any migration or shadow process. A
fresh migration MUST carry exactly its matching adapter and one target-binding
entry keyed by `to_plan`; a fresh shadow command MUST carry exactly its matching
driver. Every other M4 command, including selection and restart, MUST carry none
of those authorities. Missing, extraneous, lazy, default, or ambient provider
authority MUST fail before writable Store creation or configuration.
Engine clients MUST therefore admit a completely provider-free migration or
shadow target as a retained-replay candidate while rejecting partial or
mismatched targets; they MUST NOT classify the command as fresh before the
CLI's exact read. Clock-observation successes MUST match the requested source,
generation, and Run-derived scope. A high-level durable client with no required
executor or Clock configuration MUST fail locally with `correct_and_retry`
before invoking either a custom or CLI transport.

`cymule.evolution-plugin/3` failures form one closed category union. Every
non-contract failure uses an ASCII `^[a-z][a-z0-9_]{0,199}$` code and a
1..=2000 Unicode-scalar message; contract failures preserve the complete typed
violation. Schema and Rust admission MUST accept and reject the same set.

### 3.2 Run execution and world settlement

Run execution and external-world settlement are separate canonical axes with
one cross-axis completion invariant: `completed` requires `settled`.
`RunExecutionStatus` is the closed `active | completed | failed | cancelled`
union. A failed status carries a typed `declared_failure | runtime_defect |
substrate` classification, stable code, and immutable detail Artifact; a
cancelled status carries its immutable semantic reason. Continuation readiness
and waiting do not appear on this Run axis.

The current durable executor authors `failed` only when a component returns the
declared `ExpectedFailure` variant. A provider `Defect`, transport failure, or
substrate error returns a driver error while the Run remains `active`; its
persisted claim must later complete, expire and be taken over, or be cancelled.
The `runtime_defect` and `substrate` failure classes remain closed core reducer
values for a higher profile that explicitly owns a `FailRun` decision; their
mere error return is not a terminal CAS.

`WorldSettlementStatus` is the closed `settled | pending | unknown |
governance_required` union derived from admitted Effect intents. Failed or
cancelled execution MUST NOT erase `DispatchStarted` or `Unknown` world state.
For a claimed dispatch without an observation, the terminal CAS first records
`Unknown` and then terminates execution atomically. After failed or cancelled
execution, existing ambiguous intents remain eligible only for `Reconcile`;
`Observe` cannot occur as a later command. Terminal execution cannot admit a new
component, wait, scope, Effect, or Attempt. The terminal CAS advances the
Continuation execution fence exactly once, clears its claim, and supersedes
every still-running provider Attempt so late output fails before result
admission.

`CompleteRun` is legal only when the Effect-derived world settlement is
`settled`. Observational Effects do not create blocking obligations, but each
must still be settled or cancelled before execution becomes `completed`;
`completed` plus `pending`, `unknown`, or `governance_required` is invalid.

A component occurrence records exactly one boundary-specific outcome:
`succeeded` with its output Artifact or `expected_failure` with its stable code
and detail Artifact. In the absence of Plan-level failure matching, a declared
expected failure MUST atomically commit that occurrence, `RunFailed`, the
post-call failed Continuation, and its advanced execution fence. Receipt loss
reopens the retained failure and MUST NOT invoke the component again.
When the terminal Effect or Scope set requires paging, the first successful
page CAS durably admits that exact failure decision and its detail material.
Recovery MUST rederive the same command, occurrence, Attempt, Continuation, and
material from the retained transition and continue its Core pages without a
provider or Clock call. No public command may supply a replacement failure or
arbitrary terminal postcondition.
`expected_failure` is legal only as a component `Call` response. Returning it
from Effect preparation is a plugin defect and MUST NOT create `RunFailed`.

`CancelRun` is a semantic command, not a process kill. Its one durable CAS
advances the Run fence, deactivates every Attempt, terminates open scopes,
cancels pending waits, and moves every not-yet-dispatched Effect to
`cancelled_before_release`. A dispatch claim without an observation becomes
`unknown` in that same CAS; an already unknown or settled Effect is retained.
Scope abort applies the same rule to its unreleased Effects. Entering
`cancelled_before_release` closes any implementation-unavailability
reconciliation state because no external dispatch occurred; it cannot leave a
governance obligation behind.
Late worker output loses its stale CAS, late explicit release of a cancelled
intent is rejected, and a late wait delivery records a stable
`terminal_non_winner` receipt without making the Continuation ready.

Timeout before `DispatchStarted` remains a typed timeout/substrate boundary.
Timeout or response loss after `DispatchStarted` records `Unknown` for the
original intent and requires reconciliation; it is never a fresh retry.

### 3.3 Executable Plan contracts

Every definition, component, effect, and typed input-wait schema MUST compile
as JSON Schema Draft 2020-12 before a Plan is sealed or admitted. If `$schema`
is present, it MUST equal the canonical Draft 2020-12 dialect URI. External
schema resolution is forbidden; fragment-local references remain legal. The
compiler MUST interpret schema-bearing keywords without treating `$ref`-shaped
application data inside `const` or `enum` as a reference.

The submitted schema remains unchanged in the canonical Plan preimage. A
runtime MUST validate Run and invocation inputs, component and effect inputs,
typed wait completions, component and effect outputs, definition returns, and
the terminal Result at their exact boundary. Invalid input MUST be rejected
before plugin dispatch or boundary-specific durable mutation. Invalid output
MUST NOT be bound, stored as a result Artifact, used to complete a component
occurrence, or used to settle an outbox entry. The pre-I/O component occurrence
and Attempt remain retained when output validation fails. A mutating Effect may already
have changed the world before its returned output is found invalid; its claimed
intent remains unresolved and follows the existing reconciliation path.

For a successful component Call, the runtime MUST validate the returned JSON
against that component contract's output schema, encode the validated value as
canonical JSON, and store exactly one result Artifact using the contract's
required `output_artifact_kind`. That kind is Plan semantics. The runtime MUST
NOT infer a default kind, substitute a provider-selected kind, or retain a
second compatibility Artifact.

An Effect step with a result `bind` MUST reference a contract whose profile is
exactly `mutation = observational` and `dispatch = eager`. Every other Effect
profile is deferred relative to its lexical scope and cannot supply a local
value at that site. Core Plan admission applies this rule recursively to every
definition and nested scope before Plan identity is accepted, execution starts,
durable authority is initialized, or a business plugin is invoked.

Contract failures MUST retain boundary identity, schema/input/output side,
instance and schema JSON Pointers, masked issues, and an explicit retry
disposition in the Engine failure envelope.

## 4. Canonical stores

Cymule has three logical semantic-content families:

1. **Plan store**: immutable content-addressed semantic plans.
2. **Event store**: admitted causal transitions with explicit parents.
3. **Artifact store**: immutable typed bytes, state, context, evidence, and
   occurrence bindings.

These names do not prescribe three physical databases. Complete command
records, admission proofs, ordered batches, and their receipts are additional
canonical admission authority, not disposable projections or a bare Event
log. Exact replay requires their closure as defined in [Replay](#13-replay).

Run state, ready work, graphs, attention, effect summaries, and indexes are
deterministic projections and MUST be rebuildable.

## 5. Canonical identity

Canonicalization contract: `cymule.jcs/1`.

Canonical objects are encoded with RFC 8785 JSON Canonicalization Scheme and
identified as `sha256:<lowercase hex>`. Standalone digest fields retain their
declared digest encoding and are not interchangeable with object IDs. The object
identifier is not part of the hashed preimage. Duplicate JSON property names
and non-finite numbers are invalid. Every raw JSON ingress, including Plan,
Engine, plugin, SDK, canonical
Artifact, and persisted-state bytes, MUST reject duplicate property names at
every nesting depth before typed decoding can collapse them. A permissive or
fallback decoder is not a compatible reader. A writer MUST validate the
semantic schema before hashing. These rules apply uniformly to the current
version domains; they do not authorize an older or differently shaped decoder.

`PlanId` identifies a sealed plan. `EventId` identifies the event payload and
causal parents. Every `ArtifactRef` MUST carry
`identity_version = "cymule.artifact/2"`. Its `ArtifactId` is SHA-256 over the
fixed identity-version bytes, a big-endian 32-bit Artifact-kind length and kind
bytes, then a big-endian
64-bit content length and the immutable bytes. Artifact kinds use the Core's
bounded lowercase ASCII path grammar, with at most 255 bytes; they are not a
closed list of application types. Snapshot restore MUST recompute this exact
identity. There is no v1 reader or writer.

One Artifact contains at most 8 MiB of raw bytes. `ArtifactRecord.bytes` uses
strict padded Base64 on JSON wires, not an integer array. An encoded record
admits at most 12 MiB of canonical JSON, and the decoder MUST check its raw and
encoded bounds before allocation. Noncanonical Base64, escaped Base64 strings,
noncanonical record JSON, or a reference that does not identify the decoded
bytes MUST fail. Larger application payloads require a Resource; changing
storage placement MUST NOT change Artifact identity.

### 5.1 Typed Artifact contracts

An opaque Artifact needs only its versioned kind and immutable bytes. Files,
directories, snapshots, and provider payloads MUST NOT be forced through a
schema. A typed canonical JSON Artifact additionally pins its exact immutable
`cymule.artifact-type-contract/1` ID in its reference kind. The contract retains
the logical Artifact kind, `application/json` media type, complete document-local
JSON Schema Draft 2020-12 value, and schema digest. Different contracts MUST
produce different Artifact references even for identical bytes.

Typed Artifact schemas have a stricter resource contract than general Plan
schemas: the complete document admits at most 1 MiB of canonical JSON, 16,384
JSON values, and depth 64. Its document-local reference graph MUST be acyclic
and resolvable, with reference-expanded depth at most 64, at most 65,536 schema
visits, and at most 16 MiB of cumulative canonical subschema bytes. The schema
graph validator enforces these bounds before compiler expansion; repeated
references count toward the expansion budgets. A schema-shaped value inside
`const` or `enum` is data unless a schema reference actually selects it.

Type contracts are themselves retained as canonical content-addressed Artifacts
so a registry can be reconstructed after process loss. Encoding MUST validate
before emitting RFC 8785 bytes. Decoding MUST derive the contract ID from the
Artifact reference, verify Artifact identity and canonical bytes, then validate
the value. A caller-supplied registry alias MUST NOT reinterpret a retained
Artifact. Validation issues expose only contract ID and JSON Pointer paths; they
MUST NOT include rejected instance values. Contract code performs no I/O;
resource resolution, clocks, networks, and conversions that require I/O remain
plugin responsibilities.

### 5.2 Cross-Run resource values

M1 Artifact exchange includes a versioned provider-neutral Resource descriptor.
A Resource has a logical shape (`inline`, `object`, `collection`, `directory`,
or `snapshot`), media type, replay evidence, semantic annotations, and an
optional exact manifest descriptor. Inline text/JSON/bytes and ordinary external
objects with verified SHA-256/size provide location-independent exact evidence;
manifest Resources instead use their canonical-entry Merkle root as the sole
content authority. Availability still
requires retained inline bytes or a usable location. An immutable provider version
requires the original resolver binding for exact retrieval. A `live` Resource
is useful state but is never exact replay evidence.

One `cymule.resource/4` descriptor MUST contain no more than 64 semantic
annotations and no more than 4 MiB of canonical JSON. Its media type MUST be
exactly one non-empty lowercase ASCII RFC-token type, one `/`, and one
non-empty lowercase ASCII RFC-token subtype. Only
`a-z`, `0-9`, and ``!#$%&'*+-.^_`|~`` are admitted in either token; parameters,
additional slashes, controls, whitespace, uppercase, and every other byte are
invalid. UTF-8 inline text uses media type `text/plain`; its encoding is already
closed by the inline `utf8` variant and is not duplicated as a media-type
parameter.
`cymule.resource-locators/2` is a separate replaceable realization record bound
to one exact Resource ID and resolver implementation. It MUST contain no more
than 16 locations or 256 KiB of canonical JSON. Locator sets contain only
credential-free public URLs or opaque non-secret references. Signed URLs,
access grants, sessions, and credential revisions are resolver call state, not
Resource, locator-set, Plan, or Artifact semantics.
Credentials MUST NOT enter a descriptor, Artifact, Event, Continuation, or IR.
A public URL MUST be an exact canonical ASCII HTTP(S) wire of at most 8,192
Unicode scalars and 8,192 UTF-8 bytes, without userinfo, query, or fragment.
Percent escapes MUST use uppercase hexadecimal and MUST NOT encode an
unreserved byte. Private object, drive, sandbox, and signed-URL access uses an
opaque resolver binding/reference whose reference is non-secret. Locations do not
participate in Resource ID; moving identical content MUST preserve identity.

Generation `/4` is a source-only semantic hard cut. The `/3` candidate, Handle,
and framework Resource Handle type-key generations have no compatibility
reader, normalizer, or migration path. Any internal test Store retaining those
bytes MUST be reset and reseeded with `/4`; no physical production migration
runbook exists for this unreleased generation.

Provider-side immutable locator and proof metadata MUST use
`cymule.resource-catalog-record/2`. Its complete canonical JSON wire MUST NOT
exceed 16 MiB. A concrete adapter MUST reject provider metadata above that bound
before materializing or decoding the body.

Read operations MUST be bounded chunks. An exact directory, collection, or
snapshot list MUST use `cymule.resource-manifest/3`. Its descriptor content ID
MUST be recomputed from the sole canonical-entry Merkle root, canonical byte
size, exact entry count, and manifest media type; no independent raw-byte digest
is admitted on the manifest path. Every complete copy MUST stream-parse the
canonical JSON-lines and reconstruct that exact descriptor. One manifest entry
MUST NOT exceed 1 MiB including its newline, and one page MUST NOT exceed 8 MiB.
Every bounded page MUST carry a `cymule.resource-list-proof/5` index-bound
inclusion path for every contiguous entry. Continuation MUST use the
self-contained `cymule.resource-list-cursor/3`; every non-initial page MUST also
prove the exact preceding entry against the same root, so the cursor is
continuity evidence rather than provenance. A resolver response that exceeds
the requested bound, repeats an unsafe entry, stalls with an empty non-terminal
chunk, fails page inclusion, or fails content verification MUST be rejected.
Chunked stores use idempotent write IDs, exact offsets, explicit commit, and
explicit abort. Commit convergence and abort MUST first persist one exact
provider-owned cleanup plan, delete only that plan's targets, prove them absent,
and return its exact receipt. A store whose writers publish immutable candidates
outside a mutable session tree MAY have an empty terminal cleanup plan; it MUST
fence such candidates by a non-reusable epoch and reclaim unreachable old-epoch
objects only through its explicit bounded, receipt-backed reclamation control.

A `cymule.resource-handoff/5` Run-to-Run handoff carries the exact typed Resource
Handle Artifact produced by the named Run/component occurrence under a stable
caller transfer ID and target slot. It never copies or wraps the external
Resource bytes. Transfer requires an existing Active target Run but MAY precede
its input Wait while execution is Running; it changes no Continuation. Each
transfer ID MUST select one keyed current authority in the
typed StateRoot. The target Run MUST own a separate slot map and a payload-free,
position-bound `cymule.resource-handoff-index/1` entry in its persistent-log
index; neither may duplicate the business payload. Authority, slot, and index
MUST be committed together by one StateRoot transition and small-head CAS.
Exact transfer lookup reads the keyed current authority and verifies its target
index binding; `incoming_page(start_index, limit)` reads and exact-resolves only
one contiguous target-index page, with `limit` restricted to `1..=256`.
Neither path MAY scan a generic/global handoff history, materialize the complete
target index, or retain a global full-payload mirror. The same CAS that publishes
the handoff MUST enforce target-slot uniqueness through the slot map, without a
historical scan. Repeating identical semantics is idempotent; reusing a transfer
ID or target slot with different semantics MUST fail. Multiple values use one
collection Resource.

`cymule.resource-handoff-activation/3` uses an activation-ID-keyed current
authority and a payload-free, position-bound
`cymule.resource-handoff-activation-index/1` entry in the target owner's
persistent-log index. Activation MUST exact-match the already committed transfer
and its retained coupled receipt. One M1 CAS MUST retain that exact source
receipt reference and publish the activation authority and index entry,
canonical Resource Handle Artifact, wait result, and Continuation readiness.
The already committed transfer is not republished. The target wait MUST be an
input wait whose Run and correlation match the handoff target and slot. Receipt loss MUST reconcile
the exact typed activation receipt and MUST NOT complete the wait twice. The
mutation path accepts only a fresh `Pending` wait; it never resubmits the
completion mutation as recovery.

The Resource Handle input Artifact MUST use the closed framework
`ArtifactTypeContract`; caller-selected schemas cannot reinterpret it. The same
closed framework registry owns Resource Handles, manifest descriptors, list
proofs, and handoffs. Lifecycle commands and receipts remain typed Durable and
profile authority; they are not framework Artifact types.

Resource lifecycle MUST use one closed `cymule.resource-command/1` union and
one `cymule.resource-command-receipt/1` keyed by command ID. The command union is
exactly `Pin`, `Release`, `GarbageCollect`, `BeginDelete`, `ReconcileDelete`,
`Transfer`, or `ActivateTransfer`; every receipt MUST embed the admitted command
and its matching typed outcome. The StateRoot MUST keep separate current maps
keyed by retention key, pin ID, and delete ID. A current projection contains no
sequence or recursive history: its required
`cymule.resource-lifecycle-receipt-ref/3` names the owning Resource, Agent, or
Virtual profile plus exact command and receipt. Exact replay MUST load the
owning typed command receipt by key and verify its nested outcome. Normal
operations MUST NOT scan or replay a global lifecycle journal.

Every receipt retains semantic Resource provenance, but pin counting, GC, and
deletion use `cymule.resource-retention-key/1`, derived from the exact store
binding plus content digest. Annotation, shape, or media-type differences MUST
NOT let one descriptor collect physical bytes still pinned by another. Generic
Resource `Pin` and `Release` MUST accept only explicit pins. A Virtual archive
pin MUST be introduced only in the same CAS as its compaction certificate, and
only Virtual `RetireArchive` may atomically commit terminal retirement and its
exact `cymule.resource-archive-release/1`; generic release MUST reject it. An
Agent external-stream pin MUST first be introduced as `Reserved` by the
pre-publication reservation CAS. The same CAS MUST acquire one independent
`cymule.agent-target-claim-current/3`, addressed exactly by Session, target kind,
and local Message/Tool identity; Message role MUST NOT enter its key. Direct
Message writes, every ordinary Tool transition, Session Close, staged Finalize,
and external Finalize MUST exact-read that family rather than scanning open
streams. A terminal target advances to `Materialized`. External publication
advances absence or `Released` to `Reserved` before provider I/O, then the same
Finalize command advances its exact reservation to `Materialized`. Durable
`NotApplied` Abort advances it to `Released`; later reuse MUST increment the
generation and bind the immediate predecessor claim plus its admitting command;
the retained receipt authenticates non-reservation predecessors. This prevents
ABA and generation jumps. The same CAS MUST write one unique immutable
`cymule.agent-target-claim-generation-record/1` at the exact
Session/target/generation key. Historical replay exact-loads that slot and the
current; full audit scans actual slots once, requires a gap-free one-based
sequence, and never loops to an untrusted claimed generation. Generation has no
business retry cap beyond the shared exact-integer domain.
`Materialized` has no successor. That CAS
and `BeginDelete` mutate the same
physical-family current, so exactly one can win before provider I/O. Only a
fresh reservation or NotApplied rearm acknowledgement MAY invoke publish.
Published reconciliation MUST promote the exact reserved pin to `Active` in the
Agent terminal `Finalize` transaction without incrementing the obligation
count. Promotion MUST bind the physical family's current active count; the
reservation-time aggregate is historical evidence and MUST NOT act as a lower
bound after an unrelated sibling pin release.
The reservation intent target MUST equal the immutable Agent stream target.
A self-authenticating reservation for another target is invalid even when its
Session, stream, resolver, content and derived identities are internally
consistent; implementations MUST close this direct edge without inventing a
replacement source digest authority.

A public Agent stream `Abort` MAY retire an external publication reservation
only when the latest claimed attempt is durably `NotApplied`.
`DispatchClaimed`, including an unresolved or Unknown provider outcome, MUST
reject Abort and preserve provider-ledger reconciliation authority. The provider
MUST key its durable dispatch ledger by `dispatch_id`; publication claim and a
terminal `NotApplied` tombstone are mutually exclusive. Reconciliation MUST
return Unknown for an in-flight publisher, and any publisher which observes the
tombstone MUST return NotApplied without issuing the world write. The persisted
Abort source and effect each carry one required-nullable Resource member:
ordinary Abort encodes explicit null, while reservation retirement retains the
exact retention/pin currents and typed reserved-pin release receipt. One and
only one StateRoot CAS MUST publish all seven coupled sides:

1. the exact Agent Abort command and semantic receipt;
2. the Aborted stream current with `publication_reservation = null`;
3. the Session current with its open-stream count decremented;
4. the target claim advanced from `Reserved` to `Released`;
5. the typed Resource release retained by the Agent effect;
6. the exact pin current advanced from `Reserved` to `Released`; and
7. the physical-family current with its active count decremented and its
   disposition derived from the resulting count.

Exact Abort replay MUST resolve the Agent receipt and authenticate both
terminal Resource sidecars and the Released target claim. Finalize replay MUST
authenticate the Materialized claim, Active pin, current retention family, and
catalog record; a valid later sibling retention-family receipt remains legal.
A missing or tampered claim, pin, family current, or catalog is Integrity, not a
successful receipt replay. Generic Resource
`Release` MUST reject both the pre-publication reservation and its terminal
Agent pin; this typed Abort is the sole release authority.

GC records the exact physical active-pin count and is deletion-eligible only at
zero. `BeginDelete` MUST exact-load the command receipt selected by
`gc_command_id`, match its nested `gc_receipt_id`, and atomically publish
`cymule.resource-delete-intent/3` before provider I/O. The intent contains only
the normalized `cymule.resource-deletion-target/1`, not a full publication.
`ReconcileDelete` MUST pass that retained target only to the exact bound
`ResourceDeleter`, which deletes the published payload and proves that a new
read or stat cannot resolve it and that the selected physical generation's
current payload objects are absent. A provider MAY retain permanent non-payload
fence metadata when that is required to prevent an already in-flight writer
from republishing the deleted identity. Such metadata MUST never resolve as
content, authorize recreation, or be reported as retained payload; explicit
bounded reclamation MUST collect any late unreachable payload objects without
removing the terminal fence. Terminal deletion therefore proves logical and
payload absence, not the disappearance of every provider control-plane key.
Durable, not the caller, MUST derive and commit the terminal receipt after
successful absence readback; no caller-authored completion receipt or absence
boolean is admitted. Historical exact receipt lookup returns the original
decision; later current state does not rewrite it.

## 6. Frozen IR

`cymule.ir/3` contains:

- named component and effect contracts;
- a required versioned `output_artifact_kind` on every component contract;
- structured definitions composed from `call`, `invoke`, `wait`, `effect`, and
  `scope`;
- unclassified component `call` boundaries for repeatable computation; a Call
  has no world-outcome or idempotency profile and may run again if its response
  is lost before a durable component-result checkpoint;
- reusable definition invocation inside the same immutable Plan with explicit
  input and result binding;
- acyclic definition invocation: sealing rejects self-recursion and every
  recursive SCC, including invokes nested in scopes;
- wait suspension with an optional result binding whose omission intentionally
  discards the admitted result;
- observational eager Effect result binding; deferred Effects never bind;
- one nested auto-commit scope form with no mode field;
- literal, input, binding, object, and array expressions;
- explicit input/output JSON Schemas;
- provider-neutral effect and execution properties.

The IR MUST NOT contain provider endpoints, credentials, queue names, database
products, worker addresses, or deployment topology. Frontends are proposal
producers. `cymule_core::seal_plan` is the only trusted sealer. It compiles every
schema as Draft 2020-12 with external retrieval disabled before computing the
Plan ID; Machine insertion and restore reverify the same admission.

Version decision: the current internal `cymule.ir/3` authority removes the
pre-release `scope.mode` field because `transactional` and `speculative`
produced different Plan IDs without different semantics. A scope object that
contains either legacy value is rejected as an unknown-field shape. There is no
default, alias, dual decoder, or historical Plan fallback; this cleanup does
not introduce the proposed broader next-generation IR.

## 7. Versioned effectful continuation

A lower, provider-independent `cymule-durable-protocol` crate is the sole Rust
authority for Clock observations, Continuations and frames, execution claims,
wait ownership, wait activation, their identities, and pure wire validation.
`cymule-durable` and every higher profile import those contracts directly; they
MUST NOT copy or canonically re-export them.

### 7.1 Continuation ownership

A durable-profile Continuation is persisted at each execution boundary and
contains:

```text
plan ID | future binding context | frame | typed state | wait set | scope
epoch | execution fence | active execution claim or null
```

The `cymule.continuation-state/1` frames separate the resolved definition ID,
structural invocation ID, immutable input Artifact, nested Region path, next
step, and local Artifact bindings. An invocation pushes a frame without opening
a scope. A nested scope retains the same definition, invocation, and input.
Continuations do not duplicate Effect obligations, coordination leases,
budgets, or causal frontiers. Quiescence derives these facts from the current
Machine projection and durable profile state at one exact store-head revision.
One Continuation admits at most 4 MiB of compact JSON, 1,024 frames, 4,096 wait
IDs, 1,024 scopes, 16,384 aggregate nested collection entries, and 262,144
aggregate identity Unicode scalars. One frame admits at most 1,024 invocation
segments, 256 Region indices per path, and 4,096 locals; local names use the
same 1..=512 non-control identity contract. Untrusted Continuation bytes MUST
pass the 4 MiB pre-decode bound and Core's duplicate-member and exact-number
JSON decoder before typed verification. These bounds are transport resource
authority and do not define a second content-identity encoding.
Every durable wait pins its exact owning definition, invocation, Region path,
site, and step; only the local bind inside that owner is optional. Registration,
parking, completion, activation, and restore MUST verify this ownership.
Activation atomically stores the result Artifact, completes the wait, writes
the local when present, and readies the Continuation. Resume consumes that
durable local in later expressions or the terminal return.

Process memory and host-language stacks are not canonical. A `Running`
Continuation MUST carry exactly one `cymule.continuation-execution-claim/1`.
Its Continuation identity is the lowercase SHA-256 content ID derived from the
Run under `cymule.continuation/1`, not a descriptive string. The claim pins one
driver, continuation Attempt, Plan, typed ExecutionBinding
Artifact, positive fence, resolved content-backed logical Clock observation,
exact Clock source generation, admitted positive TTL, and derived
acquisition/expiry points. An execution command carries only a
`cymule.clock-observation/2` reference: the selected persistence-backed
execution Clock authority MUST resolve an exact receipt it previously issued
and atomically verify that it is still the current head for the Run-derived
scope. SDKs and callers MUST NOT seal receipts, submit logical time directly,
or infer expiry. The exact historical resolver MUST retain older issued
receipts for replay and retry verification, but MUST NOT be a fallback for new
execution-claim admission. Acquisition from `Ready` MUST advance the epoch and
fence, start the continuation Attempt, retain the resolved Clock evidence, and
enter `Running` in one M1 CAS. A second ordinary resume MUST return busy before
business Call or Effect invocation. CLI capability preflight remains a separate
boundary and may Describe the selected provider before opening the Store.
The complete Clock receipt remains in hot `DurableState` only while an active
claim references it. Releasing or replacing the last reference MUST remove it
in that same state transition; the selected persistence-backed Clock remains
the cold exact resolver.

The official SQLite Clock MUST return only a real file-backed, writable `main`
authority. Open MUST read back exact WAL plus `synchronous=FULL`, commit and
read back one exact singleton metadata-row write probe, then read back both
durability settings again. A readable immutable, read-only, or DELETE-only URI
is not sufficient authority and MUST fail before a Clock instance is exposed.

Persisted `Running` recovery MUST be an explicit takeover carrying the exact
current fence and a verified current-scope-head observation from the retained
Clock source generation. It MUST NOT commit before logical expiry. Expiry alone
never changes state, and M1
does not renew automatically. Takeover advances the fence and continuation
Attempt in the same CAS and supersedes provider Attempts under the old fence.
Every provider result checkpoint MUST match the current claim; stale output is
rejected.
A component substrate interruption after its Attempt was committed is a
refresh-required execution interruption: the caller MUST obtain a current
Clock observation and use explicit takeover, never replay the identical
request. An Effect dispatch interruption remains an unknown world outcome and
requires reconciliation under its original intent.

The M0 kernel implements Run, Attempt, epoch, scope, effect obligation, and
binding projections. M1 defines and persists the complete first-class
Continuation field set through a provider-neutral CAS store and resumes every
safe point expressible by the frozen sequential/nested IR. The Embedded profile
does not claim this persistence because it deliberately uses one-shot memory.

### 7.2 Durable storage

The M1 store MUST lower each admitted transition into immutable typed
state-root objects and atomically move one small head that pins the exact
`cymule.durable-state-root/6` manifest. The fixed manifest separately roots
Machine authority, admitted material, ordered batches, compacted base, Events,
admissions, commands, proofs, and every closed M1 sidecar family; a complete
`MachineSnapshot` or `DurableState` value is never a storage object. Persistent maps copy only the
changed SHA-256 trie paths. Ordered logs use a history-authenticated persistent
AVL rope: split, concatenate, append, and prefix replacement copy only bounded
spines, while the manifest's parent, admitted-delta digest, result roots, and
revision authenticate that exact representation history. A prefix
`ordered_root` is therefore exact current-manifest evidence, not a promise that
different transition histories for the same materialized sequence have the
same physical root.

Generation `/6` adds the fixed `agent_target_claims` current map, immutable
`agent_target_claim_generations` membership index, and their closed leaf kinds.
The StateRoot writer accepts only the closed `ApplyAgentTargetClaim` transition,
exact-compares its retained source, writes current plus generation slot in the
same CAS, and exposes no generic put/remove seam. Full audit closes claims and
Message/Tool/stream targets in both directions and validates each actual
gap-free generation sequence once; reachability and GC retain every immutable
slot while reclaiming superseded current value objects. StateRoot/value `/4`
has no reader or importer; retained internal domains follow the registered
drain/export/recreate/requeue runbook.

The head MUST bind the semantic revision, manifest ID, semantic sequence,
monotonic GC sequence, latest GC receipt, and a content-addressed physical CAS
token. A GC transition keeps the semantic manifest/revision unchanged and
advances only the GC sequence and physical token. Ordinary reopen MUST read
only the bounded head and its exact fixed manifest, not materialize any
complete active projection, parked-wait index, journal, archive, or GC receipt.
Each command resolves its typed bounded neighborhood from that pinned root.
Full reachable-root traversal belongs to explicitly named offline audit and
maintenance operations. Before CAS the coordinator validates the semantic transition;
under writer exclusion a provider writes canonical immutable objects, exact
matches the expected physical token, and moves only the head. It MUST NOT clone,
diff, or hash the complete projection. Runtime open MUST exact-reject older or
mixed physical generations. No legacy importer is supplied; any future
operator-owned migration needs its own reviewed runbook and decoder.

Effect dispatch payloads MUST have one current Run-local authority. The global
outbox family MAY contain only an immutable intent-to-Run locator needed by
intent-only controls; it MUST NOT mirror a mutable dispatch payload or become a
fallback reader. Each Run's exact query descriptor owns its complete dispatch,
active-effect, and active-lease roots. A paged failure or cancellation MAY
retain one Durable-private terminal companion in that descriptor, but the
companion MUST bind the exact Core transition and unchanged Run source, reuse
the Core page identity rather than introduce another cursor, and remain hidden
until the final transition publishes that Run's roots. Unrelated Runs MUST
remain writable between pages. While a Core Run is `Transitioning`, every
ordinary same-Run sidecar mutation MUST fail unless it is the exact final
operation paired with that transition.

Cold reclamation has two closed operations. `reconcile_cold_reclamation` MUST
accept the complete current head, load its exact pinned GC receipt, and
idempotently finish only that receipt's bounded deletion page. It MUST NOT
publish another head or select another page, including when `remaining_objects`
is nonzero; acknowledgement-loss recovery reopens and reconciles that same
receipt. `advance_cold_reclamation` is the sole operation that may publish the
next physical generation. It MUST first complete the current pinned page, then
select a bounded inventory page, publish the new receipt and exact-head CAS, and
make no deletion externally durable without that receipt/head authority (an
atomic transactional provider may commit them together). The predecessor
head-pinned receipt is mandatory, every other pre-existing receipt-family
object has lexicographic priority over ordinary StateRoot and Machine archive
candidates, and overflow
remains explicit `remaining_objects` work for later `advance` calls. A terminal
generation with `remaining_objects == 0` MUST have reclaimed every older
receipt-family object and retain only its own current receipt. Reconciliation
never performs that advancement implicitly.

One typed state-root leaf carries at most 12 MiB of canonical JSON and one
encoded state-root object at most 64 MiB. Application data that exceeds its
own Artifact or profile bound MUST be externalized behind an immutable
`Resource` before admission. Plans and Continuations are execution authority,
not automatically externalizable payloads: their admitted shapes and bounds
still apply, and an oversized value MUST fail rather than become a locator.
Constructors and decoders enforce the leaf and object bounds; providers use
the exported object bound as a pre-read allocation gate, not a second policy.

A compacted `MachineBaseSnapshot` is not an externalizable payload: it is
canonical execution authority. The state root MUST encode its canonical bytes
as ordered, zero-based chunks of at most 4 MiB and retain one closed descriptor
that binds total byte length, SHA-256, chunk count, and the chunk AVL root.
Compaction may process the complete newly produced base once; an ordinary
transition MUST only retain or replace the descriptor. Reassembly MUST reject
missing, repeated, misordered, short non-final, wrong-length, wrong-digest, or
non-canonical typed chunks before the base becomes Machine authority.

Machine command compaction MUST atomically persist the exact
`cymule.command-archive-segment/4` objects, independently addressed entries,
sparse-index nodes, and the resulting `cymule.machine-base-anchor/2` with the
state transition. `cymule.command-index-proof/2` membership carries the complete
256-level path. Non-membership carries an exact `empty_depth` and only the
siblings above that canonical empty subtree; all lower hashes are fixed by the
domain and cannot be caller-authored. A missing `empty_depth`, a value combined
with an empty depth, an incorrect sibling count, or a proof rooted anywhere
other than the Store-pinned current index MUST fail before Machine mutation.
Normal reopen MUST NOT scan the cold archive; historical command replay and GC
MUST resolve the independently stored index and entry objects explicitly.

A durable domain MAY contain multiple independent Runs. Its parameter-free
initialization may first publish an empty domain; that CAS admits no Run or
provider work. Creating any Run MUST append only that Run's exact
Plan when new, immutable input and binding Artifacts, `RunStarted` and first
`AttemptStarted` Events and command receipts, plus its initial Continuation in
one CAS revision. It MUST preserve every existing Run and compacted Machine
base. A Run identity cannot reset execution: accepted replay requires the same
Plan, input, and binding material. It returns a retained terminal, wait, or
Effect boundary when one exists; a Running Run remains Busy, and a Ready Run
without such a boundary requires `ResumeRun`. Start remains subject to its
execution capability. Before a fresh Clock observation, the runtime MUST
perform one exact hot/cold command lookup. A retained Start validates its
complete singleton batch and exact Plan, binding, and input leaves and returns
without Clock or provider work. A command and Run proven absent proceeds to the
fresh Clock-guarded CAS without constructing a throwaway semantic stage;
changed semantics for an existing Run fail as a history conflict.
Response-loss recovery therefore observes retained authority rather than
assuming that every identical Start automatically drives again.
Public domain initialization is parameter-free and MUST contain zero Runs.
Only the private coordinator path may publish a Run together with its
Continuation, execution claim, and Clock receipt.

An integration profile MAY atomically update its typed state with a Continuation,
Wait, outbox or Effect only through the corresponding closed Durable control.
The profile protocol owns domain DTOs and pure reduction; Durable resolves its
exact source and admits the complete normalized mutation with one CAS. A plugin
MUST NOT submit raw journals, arbitrary deltas, postconditions or provider results
as canonical authority. Coupled transitions retain the owning typed receipt and,
where required, one closed `cymule.coupled-checkpoint-receipt/3` reference. Reads
MUST resolve that exact receipt and verify every M1 edge; a shaped digest or a
generic journal record is insufficient. Identical replay does not advance the
Store revision. Missing constituent authority is a history or integrity failure,
never permission to reconstruct an unrelated projection.

### 7.3 Wait delivery and public control

A signal or timer wait MUST complete through an identified
`cymule.wait-activation/2` record. The activation fixes one external delivery
ID, its declared signal key or timer ID, exact selected wait IDs, and one
immutable result Artifact. The `cymule.wait-activation-receipt/3` record retains
that complete proposal, the exact subset of selected waits on which activation
won while Pending, and the exact Runs made Ready. The receipt, completed waits,
result Artifact, and affected Continuation readiness MUST enter one M1 CAS
revision. Targets already terminal remain stable non-winners and do not block
the winning subset or transport acknowledgement.
Activation admission MAY add its exact result Artifact through a material-only
Core batch and the corresponding typed wait/Continuation updates. It MUST NOT
accept a caller-authored Machine snapshot or unrelated material. Direct
uncorrelated completion of signal and timer waits is invalid.

Repeating an identical activation ID is idempotent. Reusing it with a different
source, target set, or result MUST fail. One signal activation MAY wake multiple
non-consuming waits but MUST consume at most one wait whose signal policy is
consume-once. One timer activation MUST target exactly one matching timer wait.
Selection and eventual delivery belong to scheduler, signal, and clock
substrates; those substrates propose activations and never mutate canonical
state directly. M1 exposes a lazy revision-pinned `ParkedWaitView` over
authenticated source and per-source active-wait maps, not a second durable
queue. A source driver MUST distinguish a target set selected through that
view during the current receive call from a target set durably retained by an
earlier receive. A new selection MUST satisfy both the framework target bound
and the current caller's bound before it may become retained. A retained
selection MUST remain within the framework bound, but a later caller's smaller
bound MUST NOT reject, truncate, reselect, or otherwise reinterpret it. The
driver acknowledges transport delivery only after the activation CAS succeeds.
Lost acknowledgement MUST redeliver the identical activation ID, source,
targets, and value; admission then returns the retained decision.
The official HTTP `/2` and timer `/3` SQLite sources MUST load each complete
persisted row before fresh selection, retained replay, schedule/request replay,
or acknowledgement. Every variable SQLite text or blob field MUST be bounded
as bytes before typed decoding; invalid UTF-8, oversize data, missing bounded
material, and length disagreement are retained-authority `Integrity`, not a
substrate failure. Stored JSON MUST duplicate-reject, decode, and reproduce its
exact canonical bytes. HTTP MUST rebuild the complete signal request and match
its request digest. Timer `/3` MUST rebuild the complete activation ID, timer
ID, due observation, and value schedule and match its schedule digest. HTTP
`/1` and timer `/1` or `/2` have no reader, importer, alias, or fallback. The
fresh and retained selection-aware partial indexes are part of each exact fixed
DDL generation. Any mismatch is retained-authority `Integrity` and MUST expose
neither parked-wait selection nor an M1 delivery.
Open MUST NOT rebuild or enumerate the complete parked-wait index. A fresh
selection resolves only its bounded source page and exact target membership;
committed transitions update only touched paths. A stale authenticated page
cursor returns typed `Stale`, while malformed proof/cursor/provider failures
remain errors. Retained activation replay precedes pending-target lookup.
`wait_activations` is omitted when empty and MUST be non-empty when present.

The public `cymule.durable-control/4` union MUST remain closed and
provider-neutral. It MAY start or resume a Run, explicitly take over one
expired Running claim, admit one identified wait delivery, explicitly release
one prepared effect, resolve one retained unknown-world effect, cancel one Run,
or issue one of the seven bounded Run-index/current/child-page/exact-item
queries. Query pages MUST bind one exact revision and StateRoot, authenticate
their continuation position, honor the caller's item and canonical-byte
budgets, and never return a full Run/domain mirror or accept a query ID. A Run
Wait page MUST read only a same-CAS bounded summary leaf and MUST NOT load or
compile the complete Wait or its Input schema. Exact-item, activation, and
explicit full-audit paths retain the complete Wait; full audit MUST prove exact
bidirectional equality between every complete Wait and its Run summary. Start, resume,
takeover, and release MUST carry the exact driver, positive TTL, and an issued
Clock reference; takeover also carries the exact current fence. Wait admission
and cancellation are typed store-only commands and MUST require neither an
executor nor a Clock. Effect resolution MUST exact-match the retained
ExecutionBinding, occurrence binding, claim owner, claim fence, and requested
applied/not-applied decision, then ask the exact historical provider ledger to
linearize that attempt and close late dispatch admission. It requires no Run
claim or Clock and MUST NOT redispatch or resume execution. Provider
unavailability or a still-dispatching attempt returns typed
`ReconciliationRequired` and preserves `Unknown`; a terminal decision returns
one complete `cymule.effect-resolution-receipt/1`. After provider invocation,
any provider error, timeout, cancellation, substrate failure, wrong response,
wrong Attempt echo, forbidden resolution, or invalid output MUST preserve the
original `Unknown` intent and project `ReconciliationRequired`. The terminal
receipt authenticates only that intent and MUST NOT embed mutable Run
`world_settlement`; queries recompute the aggregate. Cancellation returns one
complete `cymule.run-cancellation-receipt/1`. The union MUST NOT expose raw
Event, Continuation, outbox,
or journal mutation. TypeScript, Python, Rust, and Go construct the same command
shape; only the Rust durable runtime reduces it.

When an activation or other wait completion makes a Continuation `Ready`, a
resume after any process boundary MUST advance its epoch and commit a new fenced
Attempt before interpretation. The yielded Attempt that parked the wait MUST
NOT be reused.

### 7.4 Large virtual work

M3 virtual materialization MUST commit each source-owned successor cursor with
the complete bounded ready, active and parked mutation it produced. Scalar
current, independently keyed leaves, normalized mutation set and exact command
receipt share the M1 StateRoot CAS. The pure reducer is owned by
`cymule-profile-protocol::virtual_work`; `cymule-virtual` supplies provider
realization, not a second scheduler. The mutation set has a hard 4 MiB canonical
bound. Retired checkpoint/snapshot and journal-base wire models have no current
mutation or replay authority. Reusing a command ID with different semantics
MUST fail. Failed or stale CAS publishes nothing; after acknowledgement loss,
the caller reopens and resolves the exact command receipt before provider I/O.
Each region MUST pin one exact source operation, adapter binding, and revision.
Each of those RegionSource identities is bounded to 256 Unicode scalar values
and MUST contain no control characters.
Caller-owned virtual Run, region, work, owner, command and capacity-slot
identities use 1..=512 Unicode scalar values and reject C0, DEL, and C1
controls. Derived receipt, index and internal coordination identities retain
their own exact content-ID domains; a valid caller identity is not permission
to construct those internal keys by concatenation.
Fill MUST reject a different adapter generation before calling it or changing
the cursor. Source generation changes are legal only in a verified region
migration that pins the old generation and cursor. For one cursor, a source
MUST return a deterministic bounded page and successor. Every page payload MUST
already resolve to an exact current-Machine Artifact or carry its bounded exact
ArtifactRecord; new payload records, cursor, and frontier MUST share one M1 CAS.
An undeclared cursor-version change, non-terminal stalled cursor, empty or
repeated work identity, oversized page, or partial source failure MUST leave the
entire retained scheduler state unchanged.

Parked work MUST have a rebuildable exact-reason index. Waking one reason MUST
not require scanning unrelated parked work. Work parked on an M1 wait uses the
exact wait content ID as its reason key. The bounded scalar frontier MUST retain
the complete set of Wait reasons that currently own parked M3 work and each
reason's exact ParkedIndex/Parked/Work source-item, source-byte, mutation-item,
and mutation-byte charge. A park transition MUST fail before CAS unless the
aggregate charge for waking every retained Wait reason fits the profile's hard
source and mutation bounds. An identified activation MUST intersect its applied
Wait subset with that directory instead of issuing negative lookups for
unrelated M1 targets, then recompute every selected charge from exact keyed
leaves. When that activation wakes M3 work, the activation receipt, M1 wait and
Continuation updates, directory removals, and M3 scheduler postcondition MUST
be admitted by one CAS or not at all.
The receipt retains the complete `/3` activation receipt and exact wake
count as one exclusive control/receipt pair and replays only its applied wait
subset. It carries no Clock observation.
An activation already committed without that M3 postcondition MUST NOT later be
relabeled as an atomic cross-profile transition; a retry succeeds only when the
exact typed receipt was committed with the activation. After that receipt's
acknowledgement is lost, an exact retry MUST return its historical wake receipt
without rolling the scheduler back from a later current head.

Every M3 work claim MUST resolve a typed exact ExecutionBinding ArtifactRef and create one
`cymule.virtual-work-occurrence/3` record before worker execution. Occurrence
identity is derived from logical work ID and monotonically increasing claim
epoch. Owner, binding, region, Run, and current capacity-slot lease epoch MUST
be retained. A stale owner, work epoch, or lease epoch MUST NOT resolve the
occurrence.

Durable multi-worker admission uses `cymule.virtual-claim-control/4`. A public
command fixes a stable worker identity, abstract capacity-slot ID, a typed exact
`cymule.execution-binding/2` ArtifactRef,
capability set, an opaque current-head `cymule.clock-observation/2` reference,
and positive lease TTL. The owning Clock resolves the complete issued receipt,
and that receipt enters the same M1 checkpoint as the claim. A slot is a
capacity/fencing token only; it MUST NOT encode a queue provider, network
address, process, container, cluster node, or Agent Loop. One slot may own at
most one active claim, while different slots may claim independently.

Before creating an occurrence, Rust MUST resolve the exact selected Plan and
admit the runtime owner's exact binding against it. First-use binding material
comes only from that already-admitted Runtime control and enters the claim CAS;
there is no arbitrary registration API or requirement to pre-register bytes
through a separate mutation. Historical bindings resolve from the pinned root.
The virtual Run identity is a scheduler/fairness namespace, not an M1
Machine Run; implementations MUST NOT create a synthetic parent Run to satisfy
claim admission. Mixed-version selection may use only a
Rust-verified opaque admission coupled to its exact occurrence pin in the same
CAS; no public raw Plan/binding-string reducer is a complete execution API.
The public claim result MUST be the closed `VirtualClaimOutcome`. `NoWork`
MUST carry only the exact normalized persistence receipt. `Claimed` MUST carry
that receipt, a non-null exact claim, and the complete verified `SealedPlan`
loaded from the same pinned StateRoot; the Plan identity and execution-binding
reference MUST equal the retained claim. The persisted receipt MUST continue to
retain only the Plan identity and binding reference. A nullable public Plan, a
Plan embedded in the receipt, or a raw Plan reader is invalid.
The exact next M1 `CoordinationLease` and the M3 claim receipt MUST enter one
CAS revision. A `CoordinationLease` owns only its coordination resource and
fencing epoch; it MUST NOT grant capability authorization. Capability
compatibility and execution-binding admission remain separate requirements. A
failed or stale CAS changes neither. If no work is eligible, the command MUST
checkpoint a replayable empty receipt and MUST NOT acquire the slot lease.
Repeating an admitted command returns its original claimed item or empty receipt
even after unrelated scheduler progress.

`cymule.virtual-lease-renewal-control/2` fixes work ID, owner, work epoch,
expected current lease epoch, a current-head Clock reference, and TTL. Renewal
atomically advances the M1 lease epoch and the active claim and occurrence lease fence. It does not
create a new work attempt or change that occurrence's ExecutionBinding pin. Receipt loss is
resolved by reopening and replaying the same command.

One running occurrence may end exactly once as `succeeded`, `retry_scheduled`,
`parked`, `failed`, or `cancelled`. Success stores one result Artifact. Retry and
terminal failure store failure evidence; cancellation stores its reason.
Parking stores an exact indexed condition. Repeating the same disposition is
idempotent; a different disposition for that occurrence MUST fail.

Retry does not mutate or erase the failed occurrence. It requeues or parks the
same logical work, and the next claim creates a new epoch and may pin a new
binding. Cancellation removes the active claim, so later worker output is stale
and rejected. Retryability, limits, delay, and escalation are explicit policy
decisions and MUST NOT be inferred from provider strings or transport status.
The separate retry-policy helper does not itself schedule Virtual work. An
untrusted serialized `RetryStream` MUST undergo one complete issued-Clock audit
before helper mutation. Successful audit returns `VerifiedRetryStream`; each
later apply resolves and validates only the new suffix observation and updates
incremental decision/failure identity indexes. Exact retained helper-command
replay does not re-query the Clock.

Claim and disposition transitions MUST enter normalized M1 Virtual state.
Result, failure, and cancellation Artifacts MUST commit with the occurrence and
frontier in the same CAS; the Machine proposal may add only those exact
Artifacts. Public control uses `cymule.virtual-work-control/2` with a stable
command ID and exact work, owner, work epoch, lease epoch, and opaque
current-head Clock reference. The resolved complete receipt is retained in the
same checkpoint. A normal worker result MUST be observed strictly
before the current lease expiry.
The receipt MUST retain the complete command and returned occurrence ID.
Repeating that command after any number of later transitions MUST return the
original occurrence receipt without reverting scheduler state. Reusing its ID
with different semantics MUST fail.

Lease expiry is not an automatic state mutation. After expiry,
`cymule.virtual-recovery-control/2` must name the exact work, owner, work epoch,
lease epoch, current-head Clock reference, and an explicit `retry`, terminal
`failed`, or `cancelled` disposition. The durable M1 lease must still equal that
expired fence. Recovery evidence Artifact and scheduler disposition commit in
one CAS. A concurrent renewal, worker result, or recovery has one winner; stale
proposals change nothing. Retry returns the logical item to ready or parked
state, and its next claim creates a greater work epoch, so output from the
failed worker remains fenced.

For continuously backlogged, materialized, capability-compatible Runs, M3
weighted selection uses positive integer Run weights and exact positive
`WorkItem.cost`. A scheduling round grants `base_quantum * weight` deficit and a
claim debits its cost. Implementations MUST use integer, durably retained
accounting and deterministic Run order; floating point, wall time, worker
latency, and queue-provider order are not scheduling authority.
The implemented exact transition treats a round as one cyclic scan and stops
its final scan at the first Run able to pay. Implementations MAY batch preceding
complete scans in which no Run can pay, but MUST settle the selected Run's final
quantum and cost as one exact net transition and MUST NOT grant the unvisited
suffix of that final scan. Every admitted fairness tuple of policy, weight, item
cost, and current deficit MUST therefore have a bounded deterministic fairness
transition, without an out-of-range grant-then-debit intermediate, saturation,
or retry fallback, whenever the other claim preconditions hold.
A Run weight change resets that Run's accumulated deficit before future
selection so credit earned under an older policy cannot leak into the new share.
Public weight changes use `cymule.virtual-run-weight-control/1`, retain previous
and current positive weights, and checkpoint an idempotent receipt. The command
affects future selection only and does not rewrite historical claims.

Priority is local to one Run. Effective priority is base priority plus
`floor((dispatch_sequence - ready_since) / aging_interval)`. Both sequences and
the positive aging interval MUST be durable. Stable ready order breaks equal
scores. Therefore a continuously eligible old item eventually outranks a
continuous stream with any fixed finite higher base priority.

Weighted dispatch does not claim knowledge of work that has not been
materialized. Bounded materialization MUST separately rotate across registered
non-exhausted regions so every source remains visible even with one frontier
slot. Once work is visible, weight/cost selection owns dispatch fairness.
Policy, Run weights, deficits, dispatch sequence, ready age, and last Run/region
selection MUST survive reopen and produce the same next claim.

M3 MUST treat every region cursor as opaque. Split and merge are planned and
verified by a replaceable `VirtualRegionMigratorProvider` pinned through an
immutable migration binding and revision. A split has exactly one active source
and at least two targets; a merge has at least two active sources and exactly
one target. All sources and targets
MUST belong to one Run and abstract source operation.

A migration plan fixes its stable ID, kind, exact source cursor map, replacement
regions, migration binding/revision, and immutable coverage-evidence Artifact. The
pinned provider MUST return the complete non-Serde proposal: plan, exact coverage
ArtifactRecord and exact target-source ArtifactRecords. The framework MUST verify
their exact reference/byte set and combined 4 MiB material bound. Before
admission, the pinned adapter MUST verify that evidence proves complete,
non-overlapping coverage of remaining source work. Framework shape validation
or possession of an evidence reference alone is not verification.

Admission MUST compare every source cursor with the current checkpoint, reject
retired sources and existing/duplicate target IDs, retain the coverage Artifact,
and atomically record source retirement, target activation, migration receipt,
and scheduler checkpoint. A stale cursor, failed adapter verification, evidence
failure, target conflict, or stale CAS MUST retire nothing.

Retirement is not deletion. Existing ready, active, parked, known, and
occurrence records keep their source region IDs and remain valid; retired regions
remain in lineage but cannot materialize new work. Replacement targets own only
future materialization. A stable migration command replay MUST return its
original receipt after later checkpoints, while semantic ID reuse fails.

A virtual region MAY move exact occurrence history to cold storage only after
it is exhausted or retired, has no ready, active, or parked work, and the
greatest occurrence for every represented logical work is `succeeded`, `failed`,
or `cancelled`. `VirtualArchiveProvider` is the exact pinned archive and proof
provider. It receives the verified typed manifest and returns immutable Resource
publication and index/proof products. It MUST NOT choose certificate fields,
redefine occurrence meaning, or place a provider locator or credential in
semantic state.

The Rust controller MUST canonically encode `cymule.virtual-archive-manifest/2`
and compute its semantic content Resource descriptor before calling the archive.
The manifest contains the exact occurrence records, final work/epoch index,
region/Run, and a non-empty causal checkpoint cut. A returned success followed
by a failed M1 CAS MAY leave that immutable object unreferenced; scheduler state,
Machine state, certificate, and checkpoint MUST remain unchanged.
For one Virtual partition, a new compaction cut MUST include the current semantic
transition head; an old command replays from its retained exact receipt before
this future-head check. No generic journal callback can author that cut.

`cymule.virtual-compaction-certificate/4` MUST authenticate the causal cut,
bounded completion summary, complete manifest digest and Resource descriptor,
index-bound occurrence range-proof root,
terminal work/debug index digest, retained typed ExecutionBinding ArtifactRefs, unresolved
obligations, replay availability, and pinned compactor revision. The current M3 compactor
admits only a completed subtree with no unresolved obligations and `exact`
replay through the retained manifest. Each compaction also inserts every
logical work identity, greatest occurrence and epoch, terminal state, and
region/Run into one cumulative fixed-depth sparse-Merkle index. Immutable nodes
are published before the same M1 base CAS binds the exact parent and result
roots; a failed CAS may leave only unreferenced content-addressed nodes. The hot
current retains the one result root, not per-work tombstones or a list that
normal lookup scans.

After the cumulative root becomes non-empty, ordinary materialization MUST use
an explicit Store-owned archive resolver. For every source-returned `work_id`,
Rust MUST verify one membership or canonical non-membership proof against the
current root before advancing a cursor, publishing payload Artifacts, or adding
frontier work. Membership is a duplicate and exposes the retained occurrence
and greatest claim fence; it MUST NOT rematerialize or restart the epoch at one.
Only verified non-membership admits new work. A missing resolver, missing node,
wrong root, malformed proof, or provider `not found` is failure, never inferred
absence or a fallback to hot-only `known`. The final M1 CAS still fences a root
or scheduler head that advanced after proof resolution. The completion summary
is a projection, never new canonical truth.

`cymule.virtual-rehydration-control/1` selects a non-empty exact set of
occurrence IDs from one certificate. Before inserting any record, the framework
MUST reload and verify immutable bytes, Resource identity, manifest/schema
version, manifest digest, causal cut, summary, final work index, region/Run,
retained bindings, and certificate binding. Missing, extra, corrupted, or
conflicting occurrence data restores nothing. Compaction and rehydration
commands are idempotent typed M1 transitions; semantic command-ID reuse and
stale CAS fail without partial scheduler or Machine mutation.

Implementation status: the current `ResourceBackedVirtualArchive` verifies the
complete manifest, bounded to 8 MiB, before returning the requested occurrence
and its exact range proof. Only selected records are restored, but range-only
archive I/O is proposed and MUST NOT be claimed for that adapter.

## 8. Causal events

A causal cut is a causally closed down-set of admitted events. Implementations
may represent it with maximal frontier IDs. An event carries logical read/write
footprints and an optional coordination key. Independent events MUST commute.

Every non-start Event MUST extend the current causal frontier of its Run. A
fact is one Machine-wide immutable authority keyed by its exact fact key, so a
fact Event reads and writes that shared key and coordinates conflicting first
writes across Runs. Run-local names MUST NOT disguise the shared fact
authority.

Non-monotone decisions, including consume-once delivery, scope decisions,
Effect release and default version advancement, MUST be coordinated within
their owning semantic domain. A general budget-reservation or
authority-widening protocol is proposed, not an operation of the current IR.

## 9. Commands

Canonical mutation enters through a typed command envelope:

```text
command ID | actor | target | expected precondition | semantic payload
```

The same command ID and complete semantic envelope MUST return the original
receipt. Actor, target and expected precondition are part of that envelope;
the same ID with different semantics MUST fail. A stale precondition MUST
return a typed conflict and current precondition token. Actor is provenance
validated for shape, not authentication or authorization; those belong to the
embedding boundary. Public callers cannot append raw Events.

## 10. Scopes and obligations

An IR `scope` has one meaning: it remains open across nested execution and
suspension, then commits automatically only after its body completes. It has no
`transactional` or `speculative` mode. A different isolation or decision model
requires a distinct future semantic operation rather than a label on this one.

Embedded M0 scope state transitions are:

```text
open -> closed_committed
open -> closed_aborted
```

A parent scope MUST remain open while any direct child is open. Opening a child,
changing scope membership, and closing either generation coordinate through the
same Run scope-tree authority; commit or abort of a parent with an open child is
illegal. Recursive closure follows because a child cannot close while its own
child remains open.

Commit atomically accepts declared state/evidence and transfers outstanding
world-effect requirements into deterministically derived obligations. Scope
closure does not claim that a provider action is applied. The current
`CompleteRun` law requires settled world state; a caller cannot override it
with a completion-policy flag.

A scope MUST NOT commit or abort while any descendant scope remains open.
Commit MUST derive exactly one blocking obligation for each mutating intent
owned by that scope, no obligation for an observational intent, and the
resolution bit from the retained Effect. `ScopeCommitted` authenticates the
exact obligation count and Core-owned proposal-order commitment; it does not
embed a caller-authored obligation vector. Missing, duplicate, extra or
invented obligation state is invalid.

M1 may close a complete bounded Scope neighborhood inline in an atomic batch.
A standalone larger closure uses typed persisted page progress and one final
semantic receipt; those progress CASes are not completed commands. A
multi-command closure that cannot fit the inline bound MUST return
`PagedScopeRequired` before publishing that batch or invoking a provider.

A plugin-mediated resource or workspace mutation MUST still enter through a
Plan-declared Effect. A domain controller cannot treat an external commit as an
internal state update, close a scope around an ambiguous result, or bypass the
effect obligation and reconciliation rules below. Domain-specific overlays,
sessions, receipts, and controllers are outside this specification.

## 11. Effects

Effect identity is structural and independent of execution fencing:

```text
run | Plan | invocation | stable site | scope | occurrence key
normalized arguments | effect schema version
```

Core admission resolves every entry-rooted invoke edge and requires the derived
dynamic invocation ID, definition, lexical Region path, active scope, stable
site, operation, and occurrence key to agree. A nested site cannot attach to the
root scope, and a site in an invoked definition cannot claim the entry
invocation.
The canonical Event and rebuildable projection retain the complete structural
preimage, origin Plan, origin execution-binding Artifact identity, immutable
operation occurrence binding, and Plan-declared Effect profile. The durable
outbox additionally retains the resolvable execution-binding Artifact
reference. Replay and every dispatch, output validation, and reconciliation
resolve only these origin pins; current defaults and same-named operations may
not reinterpret historical work.

Phase, world outcome, and reconciliation are orthogonal:

```text
phase: admitted -> prepared -> release_authorized -> dispatch_started
       pre-dispatch cancellation -> cancelled_before_release
outcome: unobserved | applied | not_applied | unknown
reconciliation: not_required | pending | resolved | governance_required
```

Dispatch policy is admission authority, not adapter preference. `eager` is
legal only for observational effects and may claim while the scope is open.
Only that exact profile may bind its settled output into lexical state. A
provider `ResolveApplied` decision with no `resolution_value` remains absent;
the provider MUST NOT substitute the Effect input or manufacture a value. Only
the owning Durable settlement boundary materializes the canonical null Artifact
when that value-less `Applied` outcome must bind a Result. A bound `NotApplied`
response returns `EffectNotApplied` only after releasing the Run's execution
claim; it never strands a `Running` Continuation behind a generic error.
`on_scope_commit` remains pending until its owning scope is committed.
`explicit` remains prepared after commit until the caller releases that exact
intent; repeating release after receipt loss MUST converge on the recorded
claim, reconciliation, settlement, or completed Result.

An `unknown` queryable or externally-attested effect enters `pending`. An
`unknown` human or impossible effect enters `governance-required`. Queryable and
externally-attested provider reconciliation may resolve or remain unknown but
MUST NOT return a terminal governance decision. Framework-proven implementation
loss is the separate `MarkUnavailable` rule below. Human and impossible modes
settle only through an explicit provider-neutral applied/not-applied resolution of the original intent;
the same exact Machine/outbox CAS closes its obligation without redispatch.

After dispatch ambiguity, the original intent becomes `unknown`. It MUST keep
its original occurrence binding and reconciler. It MUST NOT become a fresh
intent. An `unknown` outbox entry remains eligible for reconciliation under its
original claim across any number of process reopens. `still_unknown` MUST NOT
redispatch it, and a later applied or not-applied observation MUST be admissible
without changing identity. Compensation is a separately admitted effect.
Every durable Effect intent is the exact lowercase `sha256:<64 hex>` structural
content ID across commands, boundaries, outbox state, retry evidence, and
coupled receipts.

For the durable single-domain profile, each outbox stage MUST validate an exact
Core batch. Enqueue admits the matching `EffectProposed` and `Prepare`, plus
`AuthorizeRelease` only for an eager Effect, with its exact input Artifact and
post-step Continuation. Scope commit is a separate boundary, not an enqueue
side effect. Claim admits `StartDispatch` and `AuthorizeRelease` only if the
Effect is still Prepared, together with its exact lease and outbox fence.
Settlement admits one matching observation, reconciliation or
`MarkUnavailable` transition and only the result Artifact required by that
outcome. Unrelated Plans, command receipts, Artifacts and Events MUST remain
unchanged.
`Unknown` observation and the outbox `unknown` state MUST share one CAS.
Every persisted dispatch and query summary MUST carry a result if and only if
its outbox state is `applied`; that reference MUST have exact kind
`cymule.effect-result/1`. StateRoot leaf decode, exact lookup, and full audit
MUST reject a missing Applied result or a result attached to any other state.

The `cymule.plugin/3` dispatch and reconciliation boundaries MUST carry one
`cymule.effect-provider-attempt/1` whose content identity binds the semantic
intent ID, retained claim owner, and positive claim fence. Every provider result
MUST echo the exact request attempt. Missing, different, malformed, or stale
attempt evidence cannot settle the outbox and follows the same unknown-world
reconciliation rules; it MUST NOT create a new semantic intent.
The provider's per-intent ledger MUST linearize reconciliation against first
dispatch and close late dispatch admission before reporting a terminal result.
A still-dispatching attempt is not a terminal outcome and MUST surface typed
`ReconciliationRequired` while the original outbox remains `Unknown`.

After `StartDispatch`, a missing response, wrong response variant, or missing or
schema-invalid required output MUST first record `Unknown`. A missing, invalid,
or schema-invalid reconciliation output leaves the original intent `Unknown`
and eligible for another reconciliation attempt; neither case is safe for
provider redispatch.

Embedded execution reports `completed`, `suspended`, `release_required`, and
`reconciliation_required` as closed success-side boundaries. Release carries
the exact sorted intent set; reconciliation carries the original intent. These
states are never flattened into a string failure.

`PrepareEffect` response loss may repeat preparation only with the same
structural intent ID, immutable binding, and input. Adapters MUST make this
operation idempotent. A committed `DispatchStarted` with no authoritative
outcome enters reconciliation after reopen and MUST NOT redispatch.

The M1 resumable interpreter persists nested frames as index-only paths into the
sealed Plan plus a matching scope stack. A restart MUST resolve the path from
the immutable Plan, MUST NOT serialize a host-language call stack, and MUST NOT
dispatch a child commit-gated effect while its child scope remains open.

Before any component Call provider I/O, M1 MUST persist one
`cymule.component-occurrence/4` and one `cymule.operation-attempt/2` under the
active execution claim. The occurrence identity contains the Run, exact Plan,
structural invocation/scope/site, normalized input Artifact, and component
contract. It MUST NOT contain the continuation epoch, execution fence, worker,
or selected provider binding. The occurrence separately pins its exact admitted
binding and implementation revision. A provider Attempt contains its ordinal,
continuation Attempt, execution owner/fence, exact predecessor Attempt and
attempt-specific transport request ID. The occurrence retains its Attempt
count and latest-Attempt identity as the bounded current frontier; normal
admission MUST NOT scan the complete Attempt history. Explicit takeover keeps
the occurrence and supersedes the old Running
Attempt. A later admission creates the next Attempt; an existing Running
Attempt returns InFlight and MUST NOT invoke the provider again. Result,
Attempt completion, occurrence completion, output Artifact, and post-call
Continuation MUST commit together under the current fence.

## 12. Binding evolution

A Plan changes semantic meaning. A Binding Context changes realization defaults
for future occurrences. `cymule.execution-binding/2` is the closed executable
binding authority: normalized provider descriptors, each selected operation's
provider and implementation, and every advertised operation revision are serialized to
canonical bytes and stored as an immutable Machine Artifact. Plan requirements
MUST match the selected provider before Run creation. Run, Continuation, and
Attempt MUST pin that Artifact ID. Component and Effect occurrence bindings
MUST be content-derived from the descriptor Artifact ID, operation class,
abstract operation ID, and exact selected operation record; implementation-ID
string concatenation is not a binding. Embedded M0 persists this for Attempts
and Effect Intents, including reconciliation. M1 defines canonical component
occurrence records and commits the binding Artifact atomically with Run input.
Runtime dispatch MUST route each operation to the provider selected in that
Artifact. A provider manifest is capability evidence only: advertising an
unbound operation MUST NOT authorize or route it, and a bound operation whose
selected provider no longer advertises the exact revision MUST fail closed.
Before any current or historical component/effect provider call, the runtime
owner's admitted binding MUST exactly match the complete selected
OperationBinding and resolved transitive dependency closure of that selected
provider. Providers outside that closure are unrelated. Only after this
comparison and live-manifest validation may the framework construct a private
one-shot admission token; post-claim invocation MUST consume its `FnOnce`
provider closure, and callers MUST NOT fabricate, reuse, or bypass it.
Optional plugins MAY define additional domain occurrences, but they MUST
preserve the same immutable binding rule whenever replay or reconciliation
depends on a selected implementation. Session, stream, and domain-controller
semantics remain in the owning plugin rather than this framework specification.

The official process realization pins `cymule.process-execution-binding/2` and
`cymule.process-working-directory/2`. Those identities authenticate fixed
private permissions and the complete captured closure. Materialization and
reclamation MUST be descriptor-relative, iterative, no-symlink, and constant-FD
across depth. Reclamation MUST scan each directory at most once, enforce fixed
per-directory and per-occurrence entry ceilings before collection growth, and
authenticate every child, ancestor, and final root name. A cleanup failure after
a world-mutating invocation has started is an unknown-world outcome, never a
same-request retry grant. Owner cancellation, deadline expiry, and child launch MUST
linearize on one retained launch receipt: a pre-start cancellation performs no
provider I/O, while a launch-committed mutating invocation is an unknown-world
outcome until reconciled. The official executor MUST reject every platform
other than Linux and macOS before capturing or starting provider code unless
that platform gains an equally exact launch and descriptor authority.

The parent-liveness watchdog is the sole forked process on macOS. The spawning
thread MUST block all signals before that fork and restore its exact prior mask
on every returning path. Exactly one parent-side `setpgid` transition MUST
establish watchdog group authority and publish the group gate while the parent
remains masked. The watchdog MUST inherit that mask, consume the gate, verify
its PID equals its process group, and never race a second `setpgid`. Its parent-
sized table MUST enumerate the exact inherited open set with one
`proc_pidinfo(PROC_PIDLISTFDS)` kernel-wrapper call, reject malformed,
duplicate, out-of-domain, truncated, or misaligned results, and close every
descriptor except its two private channels before readiness. Every failure
before readiness MUST emit one fixed, allocation-free stage code.

The macOS plugin image MUST use raw `posix_spawn` with
`POSIX_SPAWN_CLOEXEC_DEFAULT`, `POSIX_SPAWN_START_SUSPENDED`, and
`POSIX_SPAWN_SETPGROUP`, plus explicit cwd, argv, environment, and standard-I/O
actions. The parent MUST verify the suspended child's watchdog group and let
cancellation, deadline, and start compete on the retained launch receipt. It
MUST kill and reap a cancellation or expiry winner without provider I/O and
MUST send `SIGCONT` only after the start transition wins. There is no macOS
plugin-side fork callback or inherited-descriptor scan. Linux MAY retain its
syscall-only pre-exec boundary, but both plugin and watchdog paths MUST use
`close_range` authority and must not allocate, lock, run destructors, or read a
descriptor directory after fork.

Changing a default MUST NOT rewrite an admitted occurrence. If its original
binding is unavailable, the occurrence enters an explicit unavailable or
governance path.

### 12.1 Semantic Plan evolution

A semantic update creates a new immutable Plan node and a content-addressed
edge from its reviewed base. Structural diff is deterministic evidence for that
edge; it does not mutate or alias the parent. Reusable definitions and subflows
MUST resolve to immutable semantic dependency edges when a Plan is sealed. An
updated dependency creates a new child and, when selected, newly linked
dependent Plan commits. A sealed Plan MUST NOT contain an ambient `latest`
pointer whose later resolution can change its meaning.

The dynamic authoring strategy for a logical reusable-definition reference is
the explicit `latest_compatible` variant. M4 selects the current revision for
that exact logical reference and input/output contract, not the global head
for an incompatible contract or a scan for the greatest historical sequence.
New content receives a monotonic publication sequence; republishing historical
content reuses its immutable revision and sequence while moving current heads.
Linking injects the exact selected definition into a new parent candidate,
seals a parent Plan, advances only the compatible future link, and retains
historical linked Plans. The current compatibility profile requires exact
input/output JSON Schema equality. A reusable revision MAY itself declare
strictly ordered, unique, exact-contract Pinned dependencies. LatestCompatible exists only on an
unsealed top-level PlanTemplate. Linking MUST resolve the complete acyclic
dependency closure, record every exact
revision, assign collision-resistant local definition identities, and seal the
entire module closure into the parent Plan. Publishing a transitive dependency
MUST NOT cross an existing pinned edge: the directly referenced module must
publish a new revision before its parent template can relink. Dependency cycles and
conflicting revision choices MUST fail closed. Contract changes require an
explicit checked adapter and MUST NOT be inferred from a logical name.

Before advancing an existing future head, M4 computes the reachable semantic
surface from the Plan entry through local invocations and nested scopes. A
candidate MUST NOT automatically add a reachable component, effect, or wait, or
change a reachable component/effect contract, safety profile, capability, or
authority requirement. A violation leaves the current head unchanged while the
new immutable revision remains independently addressable. Removing an old
surface is compatible. The wire always requires the closed strategy member;
there is no omitted default or unchecked-latest strategy.

M4 durable authority MUST be normalized into one scalar partition current and
independently keyed bounded StateRoot families. Definition and compatibility
heads, immutable revisions, reverse dependencies, templates, links, Plans,
edges, rollout current/evidence/decisions, occurrences, selections, migration,
restart, shadow, observation, evidence, and transition state MUST each have an
exact keyed authority. No leaf may contain a complete registry, accumulated
history, or replayable snapshot. Ordinary open and command handling MUST use
authenticated exact-key reads from one pinned root; full history traversal is
an explicit offline audit surface.

Revision, link, Plan, decision, occurrence, and receipt identities MUST verify
against their complete immutable semantic content. A `cymule.plan-edge/2`
identity MUST bind the exact source Plan, target Plan, and ordered deterministic
structural operations. Its immutable record MUST retain the first accepted
evidence Artifact. Publishing historical definition content MUST reuse the
retained revision and move only the current and exact-compatibility heads.
Relinking to an already retained Plan MUST reuse its immutable Plan, link, and
same-direction edge records; a genuinely new reverse direction creates one new
edge. A later publication evidence Artifact MUST remain independently retained
by that publication's complete semantic receipt and MUST NOT replace the edge's
first evidence. A generated future-decision identity MUST bind its source
decision, so cycling between historical Plans cannot reuse a decision identity
or overwrite an earlier evidence accumulator.

Future occurrence selection may choose a compatible Plan under an admitted
rollout. An already materialized invocation remains pinned to its original
Plan, and every `invoke` still resolves inside that same sealed Plan. A yielded
Continuation changes Plan only through the explicit migration boundary below,
with state and contract evidence plus an epoch advance. Independently versioned
parent/child invocation and automatic cross-version invocation adapters remain
proposed; the current linker does not supply that execution model.

`cymule-profile-protocol::evolution` MUST be the sole portable M4 DTO,
identity, bounded source-view, and pure-reducer authority.
`cymule-evolution` MAY re-export that contract and host the closed process wire,
but MUST NOT fork the reducer or persistence state machine.

The provider-registry-bound `DurableEvolutionControl` obtained from
`DurableStoreControl` is the sole public durable M4 mutation seam. It exposes
only exact current and receipt reads plus
`commit(EvolutionPersistenceCommand) -> EvolutionCommit`. Production M4 MUST
NOT expose a raw transaction, generic delta, untyped history append, StateRoot
mutation, caller-authored source view, or caller-authored postcondition.

An exact all-ever command alias MUST be checked after strict command validation
and before current reads, Durable source derivation, provider Describe, or
provider execution. Exact semantic replay MUST return the original receipt
with `committed_revision: null` and MUST NOT invoke a provider. Reusing the
command ID with different semantic content MUST conflict before I/O.

A fresh command MUST prepare from one pinned StateRoot and lower one bounded
typed postcondition. One CAS MUST publish the scalar current, command alias,
semantic receipt, normalized leaves, introduced Plans and Artifacts, and every
coupled M1 or M3 mutation. Any validation, provider, Store, CAS-conflict, or
pre-CAS process failure MUST publish no canonical mutation; unreferenced
immutable staging objects are not committed authority. Recovery MUST use
exact keyed receipt lookup; it MUST NOT replay a full history from genesis or
restore a historical snapshot.

The semantic receipt MUST bind the complete command, exact parent current,
optional Durable-derived source witness, closed outcome, and strictly ordered
typed write descriptors. It MUST NOT contain a physical result revision,
StateRoot manifest, CAS token, or child-current identity that creates a
fixed-point cycle. `EvolutionCommit` MUST carry the physical observed revision,
required-nullable committed revision, and that stable semantic receipt.

Registering an existing template identity requires exact equality of the
complete `PlanTemplate`, including candidate and logical references. Different
content is a conflict. Exact command replay returns the `LinkedPlan` retained by
the original receipt even when later publication has advanced the current link;
the current registry MUST NOT reinterpret that historical outcome.

`cymule.live-evolution-control/6` is the complete cross-language envelope. It
adds reusable-definition publication, parent-template registration, atomic
publish/relink, and template scope around the closed
`cymule.evolution-control/5` operations. Commands MUST contain only semantic
intent and explicit scalar optimistic preconditions. They MUST NOT contain a
safe point, Continuation, execution binding bytes, provider product, read set,
StateRoot, manifest, or CAS token. Durable MUST derive the typed source view and
Run authority from one pinned root; a fixed provider may produce only
non-serializable authority after deterministic preparation. Clients MUST NOT
sequence registry, rollout, and occurrence mutations through a second
persistence seam.

A Virtual Run MUST persist exactly one selector. `Direct` pins an exact Plan and
creates no M4 mutation. `Evolution` names one M4 partition and template. After
fairness selects the Run, Durable MUST derive the cross-profile selection
identity from the Virtual persistence identity, load and admit the exact
ExecutionBinding Artifact, and commit the Evolution receipt and occurrence pin
in the same CAS as the M3 claim and M1 execution authority. The claim MUST
retain the semantic `plan_id` separately from the typed execution-binding
ArtifactRef; the binding MUST never be the Plan ID. A caller-supplied claim-time
M4 selector or parallel selection receipt is invalid.

A reviewed patch carries the complete target Plan Candidate, an exact declared
operation list, and evidence. M4 MUST seal the target, recompute the structural
diff, and reject the patch unless the lists are identical. Direct edge admission
and retained edge/receipt validation MUST enforce the same exact non-empty diff.
The `/2` edge identity MUST exclude publication evidence so a historical
same-direction transition has one key, while its immutable record and every
semantic command receipt independently authenticate their own evidence. A
content-addressed but caller-invented operation list is not review evidence.
Impact analysis MUST inspect generic Continuation sites and MAY accept stable
active-site identities
from higher profiles; it MUST NOT import their domain models.

A migration adapter MUST be pinned by semantic identity and immutable
implementation revision. Durable MUST derive the exact-domain quiescence
witness, complete source Continuation, source binding, admitted Plans, and
Artifact membership from one pinned StateRoot. A fresh target binding MUST come
from the fixed exact-Plan registry after deterministic preparation; retained
replay MUST load its complete target binding from that root. At that root
the Continuation MUST be root-scoped, claim-free `Ready`, and non-empty, with no
pending wait, pending/claimed/unknown outbox entry, nonterminal Effect,
unresolved blocking obligation, active Attempt, active effect claim lease, or
open nested scope. Caller-authored safe-point or Continuation data MUST NOT
enter the semantic command.

The adapter descriptor MUST declare totality over reachable source state,
preservation of failure/cancellation and budget/ownership meaning, and no
authority or Effect widening. These are required claims of the pinned adapter,
not a kernel proof of arbitrary transformation code. Rust verifies exact source
and target identity, compatibility and returned material. A shadow driver MUST
suppress or simulate target mutating Effects and pin both occurrence bindings;
the embedding owns enforcing that provider's execution isolation. Migration
output and shadow comparisons are immutable evidence, never ambient authority.

Plugins MUST return complete content-addressed Artifact records for new output
or evidence. Rust MUST verify their bytes and commit them with the normalized
M4 mutations and coupled M1 state in the same CAS. That CAS MUST verify source
binding and target Plan/binding compatibility, replace Continuation
Plan/state/binding, advance Machine and Continuation epoch, preserve the
execution fence and complete Attempt history, and publish a claim-free `Ready`
target. Only a later ordinary resume may acquire a new claim and Attempt.

After deterministic exact reads expose the migration target Plan, the fixed
Evolution provider registry MUST resolve its complete target `ExecutionBinding`
by that Plan ID. Absence MUST fail without ambient fallback. The profile MUST
verify and admit the binding, materialize its canonical Artifact record, and do
so before provider I/O. Durable MUST retain that record in the same CAS as the
normalized M4 postcondition, Core `MigrateRun`, and replacement Continuation. A
caller-authored or Ref-only target assertion is not migration authority.
Replaying an exact retained migration record MUST return the original semantic
outcome without emitting or reapplying an M1 migration sidecar. It MUST resolve
the retained target binding Artifact from the same root and MUST NOT invoke the
target-binding registry or migration adapter.

`MigrationOutput.artifacts` MUST equal the exact set of ArtifactRefs newly
introduced by the target Continuation relative to its authenticated source,
including state, frame inputs, frame locals, and every other typed Continuation
ArtifactRef field. Retained source references MUST resolve to pre-migration
bytes and MUST NOT be repeated. Evidence is one separately owned ArtifactRecord
and MUST NOT duplicate Continuation or execution-binding data. The complete
migration Artifact product, including evidence, MUST NOT exceed 1,024 records
or 4 MiB of canonical bytes. Missing, duplicate, unreferenced, forged, or
over-limit records MUST fail with the stable non-retryable plugin defect code
`invalid_migration_artifact_product`; truncation and compatibility fallback are
forbidden.

Process-hosted migration and shadow providers MUST use exactly
`cymule.evolution-plugin/3`. Request and response MUST share one fixed 16 MiB
raw-message bound, and the selected process configuration MUST carry that exact
bound. Both directions MUST use the duplicate-rejecting, safe-number,
unknown-member, member-presence-preserving strict decoder. Provider failure MUST
retain one closed category (`cancelled`, `timed_out`, `contract`, `integrity`,
`plugin_defect`, or `substrate`) and preserve structured code/message or
contract-violation fields. Stderr and message prefixes MUST NOT classify a
semantic failure.

Every mapped target frame MUST resolve in the target Plan, and each adjacent
frame MUST be owned by the parent's exact current scope or invoke step. Source
and target epochs plus every serialized mapped Continuation index MUST remain
in `0..=9007199254740991`. Direct reduction and stored receipt reads MUST use
the same receipt self-consistency validator. After acknowledgement loss, exact
command-alias replay MUST return the retained migration without Describe,
provider invocation, or current-quiescence revalidation.

Exact implementation loss MUST NOT route to a current or same-named fallback.
Before `DispatchStarted`, `MarkUnavailable` atomically records unavailable
execution, `cancelled_before_release`, `not_applied` and resolved
reconciliation. No dispatch claim or result is invented. This terminal
pre-dispatch decision closes the corresponding obligation and cannot later be
reconciled as Applied.

After dispatch began, `MarkUnavailable` instead retains the original claim,
records `unknown` and requires governance. If immutable binding equivalence
proves implementation loss after the claim CAS and before observation, these
changes share one transition. Otherwise a recovered claim becomes `Unknown`
before a live provider call; a subsequent framework-owned manifest mismatch
may mark that retained claim unavailable. Provider Defect codes alone are not
implementation-unavailability evidence.

The public M1 `ResolveEffect` command accepts only an already Unknown Effect
with its exact retained binding, owner and positive claim fence. It asks the
historical provider ledger to linearize the requested decision; it cannot
resolve a pending pre-dispatch intent, redispatch, or resume execution. If that
provider is unavailable, resolution remains `ReconciliationRequired`.
The receipt separately retains the complete requested command and the provider's
actual terminal decision/value, which may differ when the ledger already knows
the outcome. Exact replay compares the complete requested command and returns
that original receipt; a changed request under the same resolution ID conflicts.

`restart_under_new_plan` is an explicit alternative to state migration. Under
the same Durable-derived source quiescence it authorizes a distinct replacement
Run, exact target Plan, explicit replacement input, and policy evidence. It MUST NOT mutate
the source Run, reuse its identity, or reinterpret old state implicitly. The
runtime initializes the replacement through normal Run admission; the evolution
controller records only the immutable authorization and target Plan.
Direct reduction and stored receipt reads MUST use the same Restart receipt
validator for all identities, the JSON-safe source epoch, input/evidence ArtifactRefs,
distinct source/replacement lineage, and exact retained target Plan.

Rollout observations MUST match both the decision and Plan retained by the
occurrence pin. A decision becoming non-current MUST NOT invalidate late
observation or shadow evidence for that retained decision. Only applying a gate
requires its decision to remain current. A gate counts exact retained
observation and shadow identities and freezes that evidence set in its
transition; later evidence cannot rewrite an applied evaluation. An
exceeded failure or inequivalence ceiling yields rollback immediately. Promotion
requires the minimum target-observation count and equivalent-shadow count
without exceeding either ceiling. Target observations include failed samples;
their failures are counted separately. Insufficient evidence is conceptually
pending, but a durable gate command returns Conflict and publishes no transition.
Promotion and rollback create new future-only decisions and
auditable transition receipts. Previously admitted occurrences do not change.
A `cymule.rollout-transition/2` identity MUST be recomputed from exactly its
retained `from_decision`, `to_decision`, and complete verified evaluation. It
MUST NOT bind an implicit decision object absent from the transition DTO.
A later candidate after rollback MUST use the last authoritative fallback; the
failed target cannot become fallback merely because it remains the latest
registry link.

The cross-language mutation MUST be exactly
`execute_live_evolution(target, evolution_id, command) -> EvolutionCommit`.
After matching the Engine v5 request echo, an SDK MUST verify the commit's
`observed_revision`, required-nullable `committed_revision`, and semantic
receipt against the exact partition and command it sent. The receipt MUST NOT
contain a generic history identity, StateRoot manifest, CAS token, or physical
result revision. A missing or mismatched success after mutation begins is an
unknown world outcome requiring reconciliation.

`cymule.evolution-control/5` is the closed cross-language command boundary.
SDKs may construct and transport its patch, selection, migration, shadow,
restart, observation, and gate operations, but only the Rust M4 controller
resolves dependencies, invokes plugins, evaluates evidence, or mutates durable
state. Every field that names an admitted Plan in commands, provider
descriptors, outcomes, receipts, or publication updates MUST be a lowercase
SHA-256 content ID; a generic non-empty identity is not a Plan reference.

## 13. Replay

- **Exact state replay** reduces the complete command record, receipt,
  admission, Event, and Artifact closure without external I/O. A bare Event set
  is diagnostic evidence, not replay authority.
- **Exact execution replay** additionally requires every nondeterministic call
  result and occurrence binding to have been recorded.
- **Resume** resolves authenticated current authority and admits new work
  beyond its frontier; ordinary M1 resume does not replay complete history.
- **Fork** would create a new lineage from a selected cut. A generic fork
  command is proposed, not part of the current Engine or Durable control.
- **Regeneration** means new nondeterministic work, not replay or an implicit
  missing-data recovery path.

Replay availability is `exact`, `projection_only`, or `unavailable` relative to
an explicit required-Artifact set. The Core helper checks exact retained
Artifact references and whether a base or Event history exists; it does not
probe provider availability or certify an execution environment. A caller
claiming exact execution replay MUST separately establish the required schemas,
interpreter, bindings and authority. Missing evidence invalidates that stronger
claim, and the runtime MUST NOT silently regenerate data. M0 verifies exact
canonical state replay; its one-shot component calls are not an exact
execution-replay implementation.
M1 prevents reinvocation only after a component occurrence outcome commits.
Before provider I/O it persists the Plan-scoped occurrence and one fenced
provider Attempt. Response loss before the result checkpoint leaves that
occurrence pending; expiry-proven takeover permits later admission of another
Attempt for the same occurrence, and a legacy Call may repeat provider cost.
Provider observations that need a bound result and ambiguous-outcome handling MUST use an
observational eager Effect; external mutations MUST use a mutating Effect. An
Agent integration may instead use its own identified durable host occurrence.

`cymule.machine-snapshot/11` retains the complete ordered command batches,
admission records, command receipts, hot Events, admitted Plans, and Artifact
records. A receipt MAY contain several ordered Events. Every Event MUST belong
to exactly one applied receipt and one complete batch; a conflict receipt names
no Event. A material-only batch has no command members but MUST retain its
nonempty exact material proposal. Staged proposals are not snapshot authority.
Keyed root parts MUST validate exact membership and order closure for Plans,
Artifacts, and batches before conversion or replay.

A causally closed compaction cut MAY replace a complete batch prefix with a
`cymule.machine-base/4` projection. The base binds its cut-time Plan, Artifact,
and batch commitments and counts, command admission head, cumulative Event
count, archived-command index, and reducer root. These are cut-time values, not
the final snapshot's material inventory. Full batch and command bodies remain
in the independent `cymule.command-archive-segment/4` chain and exact-addressed
entry, batch, and index objects. Material-only and conflict batches remain
part of archive closure even when they add no Events.

The exact Store-pinned `cymule.machine-base-anchor/2` authenticates the base and
archive frontier, including the cumulative archived batch count. Anchored hot
restore validates that base and replays only its complete hot suffix; complete
offline replay additionally verifies the archive chain. A remaining batch may
have frozen its semantic source before its actual admission parent. The cut
MUST preserve proof of that source's authenticated ancestry and unchanged Run
neighborhood. A cut that cannot satisfy this dependency MUST fail before
mutation, never skip ancestry validation or silently load the whole archive.

Compaction MUST preserve the semantic authority root. Base/chunk placement and
physical CAS lineage are separate authority. Every remaining Event retains its
complete body and causal parents, and all typed Artifact identities are
reverified. Older generations are rejected rather than upgraded implicitly.
M1 persists the base, exact archive closure, and new head in one CAS; stale
writers lose and acknowledgement loss resolves the same committed cut.

The public Rust maintenance seam is
`DurableStoreControl::compact_machine_history(HistoryCompactionRequest)`.
Its request contains only `compaction_id`, `expected_revision`, `kind` and
`requested_suffix`. `EventPrefix` selects an Event-prefix cut while
`EventFreeAdmissions` requires a zero requested suffix and also preserves
zero-Event conflict and material-only admissions. The coordinator MUST check
an exact retained receipt before current-source traversal, reject conflicting
request reuse, and derive a fresh cut from the exact pinned Store revision.
Pending material and paged transitions MUST be absent before the offline
operation consumes Core-prepared authority. It MAY traverse the complete
source and process the resulting base once; this capability does not belong
to ordinary open, queries or execution. All new base, archive batch/entry/index
objects and the maintenance receipt share the final head CAS. No caller-authored
snapshot, base, archive or postcondition is admitted, and no Engine/SDK
maintenance transport is currently implemented.

## 14. Implemented profile boundary

The Embedded implementation contains canonicalization, sealing, in-memory stores,
causal replay, command idempotency and stale-action rejection, attempt fencing,
scope/obligation semantics, effect lifecycle, process plugins, and all four SDK
chains. `wait` returns a typed site/wait/optional-bind boundary but no fake
Continuation or resume token; durable resume belongs to M1. The profile does
not claim complete Continuation persistence, exact replay of
unrecorded component outputs, persistent crash recovery, multi-process
consensus, tenant isolation, distributed scheduling, or provider-level
exactly-once.
