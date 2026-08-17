# Rust SDK Guidance

- The SDK is an authoring and client facade. It does not own semantic reduction.
- Builders must emit the same `cymule.ir/1` objects as other language SDKs.
- Keep convenient APIs lossless: effect risk, occurrence identity, scopes, and
  version information must remain explicit in the emitted plan.
- CLI transport is one Engine implementation, not the semantic definition.
- Resource builders emit `cymule.resource/1` candidates. Only the Rust Engine
  seals Resource IDs; the SDK must not duplicate the resource canonicalizer.
- Wait activation DTOs preserve stable delivery, source, target, and Artifact
  identities. CLI verification covers the closed record; only a durable runtime
  CAS can admit it against pending waits and enforce consume-once semantics.
