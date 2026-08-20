# Cross-Run Resource Guidance

- This crate owns provider-neutral resource descriptors, replay classification,
  bounded resolver/store interfaces, and M1 Run-to-Run handoff records. It does
  not own storage credentials, provider configuration, or semantic reduction.
- Resource identity excludes realization locations. Content digest, immutable
  version authority, logical shape, media type, and semantic annotations define
  identity; moving bytes must not create a different resource.
- Credentials, signed URLs, session tokens, and provider secrets never enter a
  Resource, Artifact, Event, Continuation, handoff, log, or fixture. Public URLs
  are credential-free; private resources use opaque resolver references.
- `live` resources are useful references but never exact replay evidence.
  Version-pinned resources require their original resolver binding. Only inline
  or verified content-addressed resources are location-independent exact data.
- Read and list APIs are bounded and cursor-based. Never require a directory,
  collection, snapshot, or large object to materialize in memory.
- Higher-profile handoffs use M1 application journals and stable caller-supplied
  transfer IDs. Reuse with different semantics fails closed.
- Handoff input activation requires a target input wait whose Run and
  correlation match the handoff. Commit the canonical Resource Handle Artifact,
  transfer and activation records, wait result, and Continuation readiness in
  one M1 CAS; lost receipts replay the same activation.
- Concrete local, object-storage, drive, WebDAV, sandbox, and HTTP adapters live
  under `plugins/` and should reuse mature maintained libraries where practical.
- Typed Artifact codecs are immutable, content-addressed, and pure. The
  `cymule.artifact-codec/1` domain owns canonical JSON plus a complete local JSON
  Schema Draft 2020-12 contract; schema evolution uses a new Artifact kind.
  Resolver/store I/O and integrity verification finish before codec decode.
  Opaque file, directory, collection, and snapshot bytes remain valid Resources
  without a codec or schema.
