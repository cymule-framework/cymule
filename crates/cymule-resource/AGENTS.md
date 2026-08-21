# Cross-Run Resource Guidance

- This crate owns provider-neutral resource descriptors, replay classification,
  bounded resolver/store interfaces, and M1 Run-to-Run handoff records. It does
  not own storage credentials, provider configuration, or semantic reduction.
- `cymule.resource/2` Resource identity excludes realization locations.
  `ResourceLocatorSet` is a separate replaceable resolver record; signed URLs,
  access grants, sessions, and credential revisions never enter the semantic
  descriptor or locator set. Content digest, immutable
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
- An exact directory, collection, or snapshot list is backed by canonical
  `cymule.resource-manifest/1` JSON-lines bytes and a semantic descriptor that
  pins byte digest, byte size, entry count, and Merkle root. Every returned page
  carries `cymule.resource-list-proof/2`; framework code verifies each entry's
  mathematical tree position plus request/next cursor continuity.
- Higher-profile handoffs use M1 application journals and stable caller-supplied
  transfer IDs. Reuse with different semantics fails closed.
- Handoff input activation requires a target input wait whose Run and
  correlation match the handoff. The handoff must name an existing producer
  component occurrence whose exact output is already a typed Resource Handle
  Artifact. Handoffs never wrap full external bytes in a second Artifact. Commit
  that exact reference,
  transfer and activation records, wait result, and Continuation readiness in
  one M1 CAS; lost receipts replay the same activation.
- Concrete local, object-storage, drive, WebDAV, sandbox, and HTTP adapters live
  under `plugins/` and should reuse mature maintained libraries where practical.
- Typed Artifact contracts are immutable, content-addressed, and pure. The
  `cymule.artifact-type-contract/1` domain owns canonical JSON plus a complete
  document-local JSON Schema Draft 2020-12 contract. Typed references encode the
  exact contract ID, and retained contract Artifacts rebuild the registry after
  restart; no kind-to-latest alias may reinterpret bytes. Resolver/store I/O and
  integrity verification finish before contract decode. Opaque file, directory,
  collection, and snapshot bytes remain valid Resources without a contract or
  schema. Schema errors expose pointer paths and never rejected values.
- Framework Resource Handles, manifests, list proofs, handoffs, and lifecycle
  receipts use the closed framework `ArtifactTypeContract` registry. Do not
  replace one of these exact contracts with a caller-chosen schema.
- Pin, release, GC, delete intent, reconciliation, and upload cleanup identities
  are stable M1 journal records. Delete intent precedes provider I/O, fences
  later pins, and completion requires exact absence readback. Abort and
  completed-write convergence remove every
  owned staging/chunk object and return verified cleanup evidence.
