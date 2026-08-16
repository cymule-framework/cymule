# Rust Crate Guidance

- Keep crates acyclic: `core <- runtime <- sdk/cli`.
- `cymule-core` is the trusted semantic kernel and must remain I/O-free.
- Runtime code may implement substrate interfaces but must not redefine event or
  transition meaning.
- Public types use closed enums, deterministic maps, explicit version fields,
  and `deny_unknown_fields` where forward interpretation would be unsafe.
- New dependencies require a concrete benefit and must not pull provider or
  async-runtime choices into the core.
- Every transition needs positive, illegal-transition, retry, and replay tests.

