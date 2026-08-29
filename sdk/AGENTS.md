# SDK Guidance

- Every SDK emits the same frozen `cymule.ir/3` JSON shape and calls an Engine.
- Every CLI client sends and receives only `cymule.engine/5`; v4 envelopes are
  invalid and never trigger a compatibility decoder. Surface a typed Engine
  error that preserves the Rust failure object; never parse stderr or a human
  message into semantic categories, and never recommend replay merely because
  the transport ended without a response.
- Engine response JSON must reject duplicate object members recursively before
  shape validation. SDKs must not rely on the host parser's last-key-wins
  behavior or retry with a permissive decoder.
- Engine envelopes require response/error exclusivity. Validate the exact
  success tag and nested discriminated unions, including execution and returned
  evolution commands, before returning any payload to application code. Every
  success must contain the complete inner request plus response; failure has no
  request because decoding may have failed before it existed.
- Serialize one Engine request envelope, strict-decode those exact emitted
  bytes, retain its inner `request` value, and require the success echo to be
  exactly equal before inspecting the response. Do not compare against a
  pre-serialization host object whose omitted or normalized fields differ from
  the wire. Equality preserves object-member presence, so an omitted optional
  field does not equal an explicit `null`; do not compare only typed values after
  a lossy optional/defaulting decode. This one check correlates Seal,
  Resource, verification, Clock, durable/cancel, execution, and live-evolution
  successes. Missing/mismatched echoes and predecessor success shapes fail
  closed; SDKs never recompute Rust-owned Plan, Resource, Clock, rollout, or
  other derived identities as a substitute. If the sent request is mutating,
  invalid echo is `unknown_world_outcome` with `reconcile`; otherwise it is an
  invalid response with no inferred retry permission.
- Every language exposes one high-level `DurableEngine` with stateful `start`,
  seven bounded Run-index/current/child-page/exact-item reads,
  `observe_clock`, `resume`, `takeover`, `signal`, `release`, `cancel`, and
  `evolve` methods backed by the Rust CLI authority. Validation-only
  transport is never a durable operation.
- Reject duplicate JSON object keys, non-finite numbers, and integers outside
  the shared exact range before accepting a response or sending a request.
  Deadlines and cancellation preserve structured failure parity; response loss
  after mutation begins requires reconciliation.
- JSON integer authority is mathematical rather than lexical. Decimal or
  exponent tokens such as `1.0` and `1e0` normalize to integer `1` before a
  typed integer field is validated; a fractional value such as `1.5` cannot
  populate that field. Finite fractions remain legal in arbitrary JSON values,
  and every mathematically integral value outside the shared exact range fails
  closed.
- Contract issue decoding preserves both the failing value `path` and the
  failing `schema_path`; neither SDK may flatten them into display text.
- Every SDK exposes reusable definition declaration and `invoke` authoring with
  the same explicit local definition ID, input expression, site ID, and result
  binding. Linking logical latest-compatible references remains Rust authority.
- Every SDK preserves optional `wait.bind` and the closed Embedded
  completed-or-suspended outcome. Suspension has no client-side Continuation.
- SDKs must not compute authoritative Plan/Event IDs or implement a reducer.
- Every SDK exposes the same closed `cymule.evolution-control/5` command union
  through finite authoring builders and the real Engine facade. SDKs construct commands only; Rust resolves module
  revisions, invokes pinned migration/shadow plugins, counts observations, and
  admits promotion or rollback.
- The Rust and three-language SDKs have no separate generic `DurableControl`,
  `EvolutionControl`, or `LiveEvolutionControl` submit interface. Those
  definition-only interfaces had no implementation or caller; preserve the
  actual Engine/DurableEngine operations and typed commands instead. The Rust
  SDK likewise has no `VirtualWorkControl` or `VirtualSchedulingControl` trait;
  concrete Durable runtime controls own those operations.
- Every SDK also exposes `cymule.live-evolution-control/6`, which scopes the
  existing Plan operations to one parent template and adds definition
  publication, template registration, and atomic publish/relink. Migration and
  restart carry semantic source Run/Plan/epoch intent; the Durable reducer
  derives the authenticated source witness at its pinned StateRoot. SDKs never
  sequence these writes.
- A stateful live-evolution success is one `EvolutionCommit` containing the
  observed revision, required-nullable committed revision, and complete typed
  persistence receipt. After the outer request echo matches, every SDK binds
  the receipt's evolution identity and full serialized semantic command to
  that echoed request, then validates the closed outcome and mutation set;
  selected outcome fields or a command ID alone never establish durable
  correlation. A missing, malformed, or mismatched commit after mutation begins
  is `unknown_world_outcome` with `reconcile`.
- Migration and restart commands carry exact source Run, Plan, and epoch intent.
  SDKs never derive source witnesses, reinterpret old state, or initialize the
  replacement Run locally. The retired public safe-point shape has no
  compatibility reader.
- Occurrence selection carries distinct occurrence and selection identities plus
  the exact ExecutionBinding Artifact. Responses preserve the complete typed pin;
  SDKs never reconstruct its decision or Plan lineage.
- SDKs also author the same semantic-only `cymule.resource/3` candidates and
  producer-provenance `cymule.resource-handoff/5` wire records carrying the
  producer's exact typed Resource Handle Artifact. Locators, grants,
  signed URLs, and credential revisions never enter candidates. SDKs delegate
  Resource ID validation and sealing to the Rust Engine.
- Exact listable candidates carry only `cymule.resource-manifest/3`; SDK
  response validation recomputes its descriptor ID from the sole Merkle root,
  canonical byte size, entry count, and media type. The superseded manifest and
  list-proof generations are exact-rejected rather than normalized.
- Resource `annotations` are omission-only when empty. Builders omit the member
  instead of emitting `{}` so the submitted wire remains identical to the Rust
  request echo; non-empty annotations remain semantic identity input.
- Clock issuance crosses custom and CLI transports as
  `{ run_id, observation }`. High-level clients compare `run_id` and source
  generation with the exact request before returning the observation; only
  Rust verifies the opaque scope derivation.
- Keep APIs idiomatic in each language while preserving explicit site IDs,
  effect occurrence keys, scopes, risk profiles, and version information.
- Every component builder requires the Plan-owned `output_artifact_kind`; there
  is no SDK default. Ordinary JSON components use
  `cymule.component-output/1`. Resource producers use the exact
  `cymule.typed-json/sha256-...` kind derived from the sealed Resource Handle
  Artifact contract, never its logical framework type key.
- Scope builders emit the one auto-commit shape without a mode. Effect builders
  expose an optional result bind, but only an observational/eager contract is
  admitted by Rust; SDKs do not duplicate that semantic validator.
- Cross-language fixtures must produce the same Plan ID, Resource ID, and
  execution result.
- Every SDK preserves the required `cymule.artifact/2` identity version on every
  Artifact reference. SDKs never derive an identity from a bare reference or
  substitute a local typed-contract registry alias for the exact contract
  pinned by Rust. `LivePublicationCommand.evidence` is instead a complete
  `ArtifactRecord`; admission recomputes its artifact/2 preimage from `kind`
  plus exact bytes and rejects any mismatch, while public SDKs expose no second
  record builder or identity-sealing authority.
- Wait activation clients must preserve `cymule.wait-activation/2` delivery,
  source, exact targets, and Artifact identity. All SDKs submit the shared
  fixture to the Rust Engine; only a durable runtime admits it against state.
- Durable wait admission returns `cymule.wait-activation-receipt/3`; SDKs bind
  its activation ID, source, and complete selected wait set back to the exact
  submitted command before exposing the receipt.
- Stateful `activate_wait` uses only the store target: it attaches neither an
  executor nor a Clock, returns Ready Run IDs, and never resumes them
  implicitly.
- Every SDK exposes the same closed `cymule.durable-control/4` mutations and
  seven revision/root-pinned bounded queries. Page and exact-item budgets are
  explicit caller authority; no SDK retains the removed full Run/domain mirror
  or a query ID. Builders normalize only set-like target ordering; Rust alone seals
  Plans/Artifacts and admits Continuation, wait, or effect transitions.
- `effect_not_applied` is a closed durable Run boundary carrying one exact
  lowercase SHA-256 intent ID. An Effect-resolution receipt carries its exact
  command, provider resolution/value, optional result Artifact, and receipt ID;
  world settlement remains a Run projection and is not duplicated in that
  receipt.
- An actual Applied resolution always carries its result Artifact, including
  when the JSON value is explicit null. NotApplied carries null value and null
  result. Applied Effect query summaries likewise require a non-null result
  reference; all other summary states retain null. Never equate JSON null with
  absence of an Applied result.
- Every Effect dispatch, component occurrence, and Effect-resolution command
  carries an exact lowercase SHA-256 occurrence-binding content identity.
- High-level Store targets are provider-neutral (`provider`, `location`, and an
  optional `domain`). Official helpers select built-in generations; only Engine
  ingress decides provider support.
- Ordinary process plugins pin the exact 8 MiB plugin protocol limit. Migration
  and shadow targets pin the distinct exact 16 MiB Evolution protocol limit;
  neither boundary accepts local narrowing or widening. Every runtime-closure
  value is a lowercase SHA-256 content identity, never a host compatibility
  label.
- Start, resume, takeover, and effect-release clients preserve the exact driver
  identity, opaque issued Clock reference, TTL, and expected takeover fence.
  SDKs never read a local Clock, seal a receipt, infer expiry, renew, or take
  over automatically.
- Execution owner and Clock source/scope identities use 1..=512 Unicode scalar
  values and reject every Unicode control character, including C1
  U+0080..U+009F, identically in Rust, TypeScript, Python, Go, and JSON Schema.
- SDKs obtain opaque `cymule.clock-observation/2` references only through Engine
  v4 `observe_clock`. They never construct, seal, hash, or expose a complete
  future Clock receipt; durable mutation consumes an already issued reference
  under the selected source generation.
- Durable Run projections validate closed component outcomes, provider Attempt
  lifecycles, and the Effect-derived world-settlement aggregate recursively.
  `Completed` requires `Settled` and only settled Effect states; failed and
  cancelled Runs may retain ambiguity for reconciliation. An M4 migration target Continuation is
  `Ready` with no execution claim; a later ordinary resume acquires the next
  driver claim. Restart returns authorization and a target Plan, not a migrated
  Continuation; its distinct replacement Run follows normal start admission.
- Every SDK exposes `cancel_run` without attaching executor authority and
  preserves the closed failed/cancelled Run execution status, its separate
  world-settlement status, and applied/non-winner wait-activation disposition.
- Virtual work SDK contracts preserve stable control command and occurrence
  identities, owner/work/lease fencing, an opaque issued current-head Clock
  reference, and a typed exact ExecutionBinding ArtifactRef. Claim commands
  never accept a caller Plan ID, raw binding string, or logical time; Rust
  derives and admits the Plan and resolves the Clock receipt. The three public
  language SDKs expose authoring builders and persistence DTOs, not a claim
  transport that narrows Rust's verified `VirtualClaimOutcome` to a receipt.
  The complete claim receipt, Direct/Evolution selection, normalized state,
  coupled runtime admission, and `VirtualClaimOutcome` remain Rust-only.
  `VirtualWorkControl`, `VirtualArchive`, and `RegionMigrator` interfaces and
  the old claim/compaction receipt mirrors had no implementation or caller;
  do not restore them as apparent language support. Provider/runtime extension
  belongs to the actual Rust profile interfaces.
- Region migration contracts preserve exact source cursors, target descriptors,
  migration binding and revision, evidence, and required `source_artifact` provenance across
  languages. No SDK interprets cursor positions, certifies coverage, or admits
  a predecessor region shape that omits source provenance.
- Archive DTOs expose typed compaction/rehydration authoring only. SDKs preserve
  the Rust-issued compaction command ID, complete bounded work/occurrence/command
  selections, exact archive binding/revision, causal cuts, certificate identity,
  replay availability, and exact occurrence selections; Rust verifies content
  identity and performs M1/M3 admission. There is no three-language archive
  provider or scheduler transport.
- A virtual compaction certificate preserves its parent work-index root,
  ordered-update digest, resulting work-index root, command count, and
  required-nullable command root. Every SDK wire must retain all five members;
  an explicit null command root is not equivalent to an omitted member.
- Scheduling clients preserve capacity-slot, work epoch, lease epoch, opaque
  Clock reference, capabilities, binding, and explicit recovery disposition.
  They do not discover workers, author or read logical time, infer expiry,
  classify failures, or reduce state locally. Run-weight updates are typed
  future scheduling commands.
- Avoid runtime dependencies unless they materially improve correctness.
- Durable and live-evolution successes are recursively closed. Store, executor,
  migration, and shadow targets remain separate; queries send no executor.
- Live evolution carries the migration provider only for an exact nested
  `migrate` command and the shadow provider only for an exact nested `shadow`
  command. Every other command carries neither, even when both providers are
  configured on the high-level client.
- Every success payload is validated recursively, including Resource Handles,
  wait activations, durable commands, migration receipts, and mapped target
  Continuations. Mutating response loss is an unknown-world outcome in every
  language.
- Nested migration receipts carry the complete admitted migration request.
  SDKs validate that request inside the owning Engine v5 live-evolution receipt
  and enforce the shared JSON safe-integer bound for source/target epochs and
  mapped Continuations.
- Migration target epochs are positive, and every mapped Continuation has a
  non-empty scope stack plus required exact
  `continuation_version = cymule.continuation-state/1`. Go, Python, TypeScript,
  Rust, and JSON Schema reject the same missing, predecessor, zero, and empty
  boundary values; `cymule.continuation/1` is only its ID content domain.
- Migration binding references have exact kind `cymule.execution-binding/2`.
  Restart success recursively validates the complete request, including its
  safe epoch and input/evidence Artifacts, before exposing authority.
