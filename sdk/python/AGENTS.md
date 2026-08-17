# Python SDK Guidance

- Support maintained CPython versions with no runtime dependencies.
- Use typed dictionaries or dataclasses for public contracts and preserve exact
  wire names.
- Subprocess errors must include bounded stderr and never expose environment
  variables or credentials.
- The Rust engine remains the only authoritative sealer and reducer.
- Resource builders preserve exact wire names and send candidates to the Rust
  engine. Do not add a Python Resource ID implementation or accept credentials
  in public URL helpers.
- Wait activation builders sort and deduplicate exact targets while preserving
  delivery, source, and Artifact identities. Rust Engine verification is not a
  substitute for durable CAS admission against pending waits.
- Virtual work query/control types preserve command, occurrence, binding,
  owner, epoch, and disposition fields. SDK transports do not classify errors
  or decide retry policy.
