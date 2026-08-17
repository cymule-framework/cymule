# TypeScript SDK Guidance

- Support maintained Node.js LTS lines and use strict TypeScript.
- Keep the package dependency-free at runtime.
- Do not depend on object insertion order for identity; the Rust engine performs
  canonicalization and sealing.
- Keep Resource unions closed and dependency-free. Never normalize URLs or hash
  Resource Candidates in TypeScript; `CliEngine.sealResource` is authoritative.
- Use discriminated unions for IR and Engine protocol types.
- The public npm package name is `cymule`. Changes to exports, files, engine
  requirements, or minimum Node versions require a package dry-run and release
  workflow review.
- npm publication uses GitHub Actions trusted publishing with provenance. Never
  add a long-lived npm token to repository or organization secrets.
