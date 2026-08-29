# Rust Crate Guidance

- Keep crates acyclic: `core <- durable-protocol`; `core <- runtime`; and
  `durable-protocol/runtime <- profile-protocol/durable <- sdk/cli`.
- `cymule-core` is the trusted semantic kernel and must remain I/O-free.
- Runtime code may implement substrate interfaces but must not redefine event or
  transition meaning.
- Public types use closed enums, deterministic maps, explicit version fields,
  and `deny_unknown_fields` where forward interpretation would be unsafe.
- New dependencies require a concrete benefit and must not pull provider or
  async-runtime choices into the core.
- Every transition needs positive, illegal-transition, retry, and replay tests.
- Public crates share one release version and declare internal dependencies as
  workspace-owned `version + path` locations. `cymule` is the canonical Rust
  user facade; `cymule-cli` owns the installable `cymule` binary. Core profile
  crates remain directly publishable for advanced composition.
- Package verification compiles normalized `.crate` contents, not just the
  source workspace. Test adapters and examples are never registry packages.
