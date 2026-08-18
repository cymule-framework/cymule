# Go SDK Guidance

- Keep the SDK on the Go standard library unless a dependency is essential.
- Public wire structs use explicit JSON tags and avoid interface-based semantic
  dispatch when a closed type can express the contract.
- The CLI Engine is a transport; do not add a Go reducer or authoritative hash.
- `Definition` and `Invoke` author exact local reusable calls; logical subflow
  registry resolution remains Rust M4 authority.
- Keep Resource Candidate, Handle, Integrity, Location, and Handoff wire structs
  explicit. The Rust Engine is the only Resource ID authority.
- Keep WaitActivation and source structs closed and provider-neutral. Builders
  sort/deduplicate targets; Rust verification is not durable CAS admission.
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
- Keep migration and restart proof fields explicit and typed. Go transports do
  not derive safe points or reuse a source Run identity for a replacement.
- Run `gofmt` and `go test ./...` for every change.
