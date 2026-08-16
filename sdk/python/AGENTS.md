# Python SDK Guidance

- Support maintained CPython versions with no runtime dependencies.
- Use typed dictionaries or dataclasses for public contracts and preserve exact
  wire names.
- Subprocess errors must include bounded stderr and never expose environment
  variables or credentials.
- The Rust engine remains the only authoritative sealer and reducer.

