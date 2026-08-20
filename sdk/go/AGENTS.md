# Go SDK Guidance

- Keep the SDK on the Go standard library unless a dependency is essential.
- Public wire structs use explicit JSON tags and avoid interface-based semantic
  dispatch when a closed type can express the contract.
- The CLI Engine is a transport; do not add a Go reducer or authoritative hash.
- Return `EngineFailure` as the typed Go error for remote failures and
  response-less transport failures. Callers use fields or `errors.As`, never
  parse `Error()` text for control flow.
- Engine-process stderr is diagnostic-only and never becomes an
  `EngineFailure.Message`; response-less exits use bounded SDK-owned status.
- Decode exactly one Engine outcome: success has a response and no error;
  failure has an error and no response. Custom Evolution and execution union
  decoders reject unknown variants, operation-incompatible fields, and unknown
  nested request fields.
- Every `ArtifactRef` carries the exact `cymule.artifact/2` identity version;
  Go preserves it without deriving or upgrading identities.
- `Definition` and `Invoke` author exact local reusable calls; logical subflow
  registry resolution remains Rust M4 authority.
- Keep Resource Candidate, Handle, Integrity, Location, and Handoff wire structs
  explicit. The Rust Engine is the only Resource ID authority.
- Keep WaitActivation and source structs closed and provider-neutral. Builders
  sort/deduplicate targets; Rust verification is not durable CAS admission.
- `DurableCommand` uses `json.RawMessage` for start input and activation value
  so JSON `null` remains present on the wire. Constructors return encoding
  errors rather than inventing a fallback value.
- Virtual work query/control structs preserve binding, owner, epoch, command,
  and disposition identity. Do not implement retry classification in the SDK.
- Region migration structs retain opaque source cursors, pinned adapter binding,
  and coverage evidence. Never partition cursor strings in Go client code.
- Archive and compaction structs retain exact wire fields. Go adapters store
  immutable bytes only; they do not recompute certificate or manifest identity,
  and rehydration never widens the requested occurrence set.
- Scheduling structs retain slot, logical time, work/lease fences, capabilities,
  Run weight, and explicit recovery disposition. Do not use goroutine/process
  identity, `time.Now`, or local maps as durable worker authority.
- Evolution command structs retain the closed operation, stable command ID,
  exact patch/request/observation/gate payload, and control version. Go never
  resolves module heads, runs adapters, or evaluates rollout evidence.
- Unified live-evolution commands retain template scope and safe-point proof
  around the closed operation. Go clients do not sequence registry, rollout,
  and occurrence mutations independently.
- Keep migration and restart proof fields explicit and typed. Go transports do
  not derive safe points or reuse a source Run identity for a replacement.
- Run `gofmt` and `go test ./...` for every change.
