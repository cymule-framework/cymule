# Contributing

Thank you for contributing to Cymule.

## Development setup

Use the pinned Rust toolchain from `rust-toolchain.toml`, Go 1.26 or newer,
Python 3.12 or newer, Node.js 22 or newer, and pnpm 11 or newer. The optional
MLIR workbench is tested with LLVM/MLIR 22.1.8.

Run the complete local gate before opening a merge request:

```sh
./scripts/verify.sh
```

## Change categories

- Semantic changes must update the specification, version domains, schemas,
  conformance cases, and every affected SDK.
- Runtime realization changes must preserve the core event vocabulary and
  deterministic reducer.
- New provider integrations belong in plugins and must declare guarantees,
  failure behavior, reconciliation, and authority requirements.
- Documentation must distinguish implemented, partial, proposed, and historical
  behavior.

## Commits

Use focused English commit messages. Do not combine semantic changes with
unrelated cleanup.

