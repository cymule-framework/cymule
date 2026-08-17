# Go SDK Guidance

- Keep the SDK on the Go standard library unless a dependency is essential.
- Public wire structs use explicit JSON tags and avoid interface-based semantic
  dispatch when a closed type can express the contract.
- The CLI Engine is a transport; do not add a Go reducer or authoritative hash.
- Keep Resource Candidate, Handle, Integrity, Location, and Handoff wire structs
  explicit. The Rust Engine is the only Resource ID authority.
- Keep Agent stream target/chunk/record/projection structs explicit and send
  them to `VerifyAgentStream`; Go does not own stream transition semantics.
- Run `gofmt` and `go test ./...` for every change.
