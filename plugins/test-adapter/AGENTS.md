# Test Adapter Guidance

- This executable is conformance infrastructure, not a production provider.
- Keep behavior deterministic and input-driven.
- It must exercise normal application and unknown-then-reconcile paths without
  ambient network, filesystem, clock, or random dependencies.

