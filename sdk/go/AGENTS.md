# Go SDK Guidance

- Keep the SDK on the Go standard library unless a dependency is essential.
- Public wire structs use explicit JSON tags and avoid interface-based semantic
  dispatch when a closed type can express the contract.
- The CLI Engine is a transport; do not add a Go reducer or authoritative hash.
- Control command builders use the implemented Engine/DurableEngine methods.
  Do not restore the caller-zero generic control-submit interfaces or aliases.
- Return `EngineFailure` as the typed Go error for remote failures and
  response-less transport failures. Callers use fields or `errors.As`, never
  parse `Error()` text for control flow. Schema `maxLength` constraints on
  failure messages, contracts, issues, and paths count Unicode scalar values,
  not UTF-8 bytes; the failure code remains the closed ASCII identifier.
  Enforce the complete category-to-retry-disposition matrix. Cancellation that
  linearizes before launch is safe; cancellation or timeout after a mutating
  process starts is an unknown world outcome. Read-only requests retain their
  safe cancellation/timeout disposition. An emitted `issues` member is
  non-empty and contains at most 100 closed issues; absence, not `[]`,
  represents no issues.
- Engine-process stderr is diagnostic-only and never becomes an
  `EngineFailure.Message`. Drain stdout and stderr concurrently; stdout retains
  the 128 MiB plus 32-byte framing response-envelope bound plus one byte, while stderr retains its
  independent 1 MiB diagnostic bound plus one byte. The first overflow wakes
  the single process owner, which kills and reaps the direct Child immediately.
  Overflow is response loss; a mutating request requires reconciliation.
- Every started Engine has one SDK-owned direct Child handle. Cancellation and
  deadlines immediately kill that Child, close stdin/stdout/stderr parent
  endpoints, and call `Cmd.Wait`; no grace extends the absolute deadline.
  Natural exit is observed with `waitid(WNOWAIT)` and reaped only after local
  request writing and stdout/stderr EOF finish. Leader exit closes the local
  stdin endpoint so an inherited but unread pipe cannot strand the writer.
  Once that retained terminal status is observed, a simultaneously ready
  timeout/cancellation/overflow may close local endpoints and reap but never
  signal or reclassify the already completed Child as interrupted. The call
  records that terminal win under the cancellation mutex, and endpoint closure
  still joins all three local completion channels before response parsing.
  A non-EINTR wait-authority error kills and synchronously reaps the still-owned
  Child; it never delegates `Cmd.Wait` to an unbounded goroutine. The
  Engine/executor watchdog owns descendants; the SDK never signals a raw PID or
  PGID after reaping.
- `EngineCancellation` is a one-shot SDK launch/completion authority. `Cancel`
  and `Cmd.Start` linearize under the same lock: cancellation-first means no
  process creation and a safe `cancelled` failure; launch-first means the
  request began and follows the read/mutation interruption classification.
  Completion arbitration runs only for a fully validated success or valid
  remote failure; cancellation never replaces a local transport, I/O,
  overflow, timeout, kill, wait, or termination failure.
  `CliEngine` exposes no `context.Context`; this token is the only cancellation
  authority, and its finite SDK-owned `Timeout` is the only deadline authority.
- Every process-backed request carries a complete `EngineProcessConfig` inside
  `EnginePluginTarget`: absolute executable, ordered arguments, ambient-cleared
  environment, required nullable working directory, non-empty runtime closure,
  timeout, message limit, and closure limit. There is no location-only target or
  string-path `Run` overload. Migration and shadow targets additionally require
  an exact revision.
- Store targets retain provider-neutral `provider` and `location` strings plus
  an optional `domain`. Official constructors emit `cymule.directory-store/5`
  and `cymule.sqlite-store/6`, while Engine ingress alone decides provider
  support.
- Decode exactly one Engine outcome: success has the complete inner request
  echo plus a response and no error; failure has an error and neither request
  nor response. Compare the echo with the actual serialized request before
  inspecting any success payload. Custom Evolution and execution union decoders
  reject unknown variants, operation-incompatible fields, and unknown nested
  request fields.
- Validate every success against its originating request before returning from
  the transport boundary. A known but wrong success tag, or a live-evolution
  receipt whose journal, complete echoed command, or outcome does not match the
  mutation is invalid response loss and surfaces `unknown_world_outcome` with
  reconciliation.
- Compare the raw live-evolution receipt command directly with the retained raw
  sent command before typed decoding can erase member presence. Required
  nullable members survive; omission-only members reject explicit `null`.
- `FlowBuilder.Component` requires an explicit Plan-owned output Artifact kind;
  it never supplies a default. Use `cymule.component-output/1` for ordinary
  component JSON and the exact `cymule.typed-json/sha256-...` kind derived from
  the Resource Handle contract for Resource producers, not its logical type key.
- Every `ArtifactRef` carries the exact `cymule.artifact/2` identity version;
  Go preserves it without deriving or upgrading a bare reference. Live
  publication evidence is a complete `ArtifactRecord`; strict admission
  recomputes its artifact/2 preimage from the exact bounded canonical Base64 wire, without
  adding a Go record builder or second sealing authority.
- `Definition` and `Invoke` author exact local reusable calls; logical subflow
  registry resolution remains Rust M4 authority.
- A published `SubflowRevision` definition is validated against the closed
  Serde IR shape and registry-owned name/reference relations, not full sealed
  Plan admission. Nested draft site and binding identifiers may remain empty
  until Rust links and seals a parent Plan.
- Applied Effect summaries, full leaves, exact Run items, and resolution
  receipts all use the same result gate: the result is non-null and has kind
  `cymule.effect-result/1`. Every non-applied state carries JSON `null`.
- Keep Resource Candidate, Handle, Integrity, Location, and Handoff wire structs
  explicit. Recursively validate the returned Handle and compare its complete
  candidate portion with the sent candidate; the Rust Engine remains the only
  Resource ID derivation authority.
- Resource `/4` builders and response validation share the exact lowercase
  ASCII type/subtype token grammar. `ExternalResource` returns an error for an
  invalid media type; it never emits a candidate that Rust must reinterpret.
- Keep WaitActivation and source structs closed and provider-neutral. Builders
  sort/deduplicate targets; Rust verification is not durable CAS admission.
- `DurableCommand` uses `json.RawMessage` for start input and activation value
  so JSON `null` remains present on the wire. Constructors return encoding
  errors rather than inventing a fallback value.
- Virtual work query/control structs preserve binding, owner, epoch, command,
  and disposition identity. Do not implement retry classification in the SDK.
- Region migration structs retain opaque source cursors, pinned adapter binding,
  revision, coverage evidence, and required `source_artifact` provenance on every region.
  Never partition cursor strings in Go client code.
- Archive and compaction DTOs retain the Rust-issued command ID, complete
  bounded selections, and exact archive binding/revision. Go exposes no
  scheduler/provider transport or normalized claim/compaction receipt mirror;
  it never recomputes certificate/manifest identity or widens rehydration.
- Scheduling structs retain slot, opaque issued Clock references, work/lease
  fences, capabilities, Run weight, and explicit recovery disposition. Do not
  accept caller-supplied logical time or use goroutine/process identity,
  `time.Now`, or local maps as durable worker authority.
- Existing claim, renewal, recovery, and resolution builders validate their
  512-scalar non-control identities, exact Artifact references, and closed
  disposition payloads. Recovery admits only retry, failed, or cancelled;
  command construction never evaluates a lease or executes scheduler state.
- Evolution command structs retain the closed operation, stable command ID,
  exact patch/request/observation/gate payload, and control version. Go never
  resolves module heads, runs adapters, or evaluates rollout evidence. M4
  identities contain 1..=256 non-control Unicode scalar values; count runes,
  not UTF-8 bytes. Run identities remain the separate 512-scalar contract.
- Unified live-evolution commands retain template scope around the closed
  semantic operation. Go clients do not sequence registry, rollout, and
  occurrence mutations independently.
- `ExecuteLiveEvolution` completes closed local command validation, including
  migration/restart source Run, Plan, and epoch intent, before starting an
  Engine process. The retired outer and nested safe-point shapes are rejected.
  Template reference strategies retain the Rust tagged-object wire rather than
  projecting it into a string plus sibling revision field.
- Occurrence selection and outcome types retain occurrence, selection,
  template, decision, Plan, and exact ExecutionBinding lineage without deriving
  any field in Go.
- Keep migration and restart source intent explicit and typed. Go transports do
  not derive Durable source witnesses or reuse a source Run identity for a
  replacement. Requests retain the exact source Run, Plan, and epoch; public
  safe-point and caller-authored source-Continuation fields do not exist.
- Run `gofmt` and `go test ./...` for every change.
- A zero-value `CliEngine.Timeout` installs the finite 30-second default;
  positive values override it. `CliEngine` accepts no caller Context deadline
  or second cancellation route. The timeout clock begins only after
  `Cmd.Start` succeeds, so pre-launch validation and launch ordering remain
  exclusively owned by the cancellation gate.
- Validate every nested `json.RawMessage` in durable and live-evolution success
  payloads before returning it.
- Durable Run views enforce canonical collection order, pending-wait equality,
  and bidirectional component-occurrence/provider-Attempt lifecycle closure.
  Occurrence, Attempt, Continuation-Attempt, and transport-request identities
  are content IDs. Attempt ordinals are contiguous, a completed occurrence has
  exactly one final completed Attempt, and a running Attempt matches the current
  claim fence and Continuation Attempt. A NotApplied Effect never carries a
  result.
  Durable response sets are strictly sorted and unique; terminal non-winners
  return no ready Runs. Embedded execution outcomes retain digest/content-ID
  shape and must match the requested Run, Plan, and suspended wait site.
- Effect dispatches, component occurrences, and Effect-resolution commands all
  require an exact lowercase SHA-256 occurrence-binding content identity.
- Caller Run identities contain 1..=512 non-control Unicode scalar values; count
  runes rather than UTF-8 bytes. Derived Continuation identities are retained
  content IDs and must never be reconstructed by concatenating the caller Run
  identity. The SDK-owned Get query identity is
  `sdk:get:sha256:<sha256(run UTF-8)>`, matching every other SDK without
  extending the caller identity.
- Live-evolution wire validation enforces directly observable receipt
  self-consistency, canonical ordering, identity shape, and cross-field
  relationships. Content-ID derivation and semantic admission remain Rust
  authority and must not be reimplemented in Go.
- Before an `apply_patch` live mutation, the Go CLI transport seals the target
  candidate through the same Rust Engine and requires the returned Plan edge to
  name that exact target Plan. Engine v5 returns the complete `EvolutionCommit`;
  operation-specific outcome fields are never used as a substitute for the
  echoed semantic command's request identity.
- Go request and response JSON rejects invalid UTF-8 and unpaired surrogate
  escapes before `encoding/json` can replace them. Caller-invalid transport
  preflight is a typed `validation/correct_and_retry` failure and never starts
  the Engine process.
- A bounded raw scan before `encoding/json` rejects depth above 128, number
  tokens above 256 bytes, and exponents above six digits. Fraction equality uses
  a canonical decimal rational, not binary float or lexical spelling.
- Durable cancellation and claimed-effect reconciliation return closed typed
  receipts. Bind cancellation ID, Run, and original reason exactly; bind every
  effect-resolution authority field and value exactly. Validate the Rust-owned
  boundary reason and result without deriving either Artifact identity in Go.
- Current durable terminal responses retain nested receipts: wait activation
  keeps the complete activation/applied/Ready sets, cancellation keeps its
  complete command and boundary, and Effect resolution keeps its complete
  requested command plus independent actual resolution/value. Receipt IDs are
  shape-validated and never recomputed by Go. Effect-resolution receipts do not
  duplicate Run world settlement; `effect_not_applied` is a distinct closed
  boundary carrying one exact content-addressed intent.
- Public closed JSON unions, including `WorkResolution`, reject duplicate keys,
  unknown or cross-variant fields, and required-null payloads. Strict request
  values reject caller-defined JSON/text marshalers before invocation so Go
  cannot silently replace invalid UTF-8 or invoke a stateful marshaler twice.
