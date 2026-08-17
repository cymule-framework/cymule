# Example Guidance

- Examples are user-facing product surfaces, not conformance fixtures.
- Keep each example self-contained, copyable, and honest about the profile it
  exercises.
- Example commands must avoid test-only paths, ambient credentials, and
  machine-global state.
- Store generated example state under the ignored repository-local `.cymule/`
  directory unless the caller supplies an explicit output directory.
- Run examples in repository verification so README commands cannot drift.
