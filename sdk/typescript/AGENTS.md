# TypeScript SDK Guidance

- Support maintained Node.js LTS lines and use strict TypeScript.
- Keep the package dependency-free at runtime.
- Do not depend on object insertion order for identity; the Rust engine performs
  canonicalization and sealing.
- Use discriminated unions for IR and Engine protocol types.

