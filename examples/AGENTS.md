# Example Guidance

- Examples are user-facing product surfaces, not conformance fixtures.
- Keep each example self-contained, copyable, and honest about the capabilities
  and limits it exercises; do not require users to understand milestone labels.
- Introduce examples through the user's operational problem and the outcome the
  example proves. Keep mechanism details after the first successful run.
- Example commands must avoid test-only paths, ambient credentials,
  machine-global state, and generated setup files.
- Run examples in repository verification so README commands cannot drift.
- Durable examples must checkpoint every prerequisite of published work in the
  same CAS or derive it from an already pinned Resource. Never publish a
  frontier that needs a later payload write to become executable.
- For crash and ownership demonstrations, use logical lease times and exact CAS
  fences. Do not use timing races, ambient process locks, or global state.
