# Go SDK Guidance

- Keep the SDK on the Go standard library unless a dependency is essential.
- Public wire structs use explicit JSON tags and avoid interface-based semantic
  dispatch when a closed type can express the contract.
- The CLI Engine is a transport; do not add a Go reducer or authoritative hash.
- Keep Resource Candidate, Handle, Integrity, Location, and Handoff wire structs
  explicit. The Rust Engine is the only Resource ID authority.
- Keep WaitActivation and source structs closed and provider-neutral. Builders
  sort/deduplicate targets; Rust verification is not durable CAS admission.
- Virtual work query/control structs preserve binding, owner, epoch, command,
  and disposition identity. Do not implement retry classification in the SDK.
- Region migration structs retain opaque source cursors, pinned adapter binding,
  and coverage evidence. Never partition cursor strings in Go client code.
- Run `gofmt` and `go test ./...` for every change.
