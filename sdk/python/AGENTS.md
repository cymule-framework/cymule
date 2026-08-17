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
- Agent stream records remain plain versioned wire data. Python must not infer
  finality, reorder chunks, or implement the authoritative content digest.
