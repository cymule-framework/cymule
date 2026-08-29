# Authenticated Collections Guidance

- This crate is the sole physical hashing and proof authority for Cymule's
  compressed SHA-256-keyed map and ordered AVL log. Core and Durable consume
  this crate; this crate must never depend on either of them.
- Keep the crate synchronous, provider-neutral, deterministic, and free of
  ambient I/O. Resolvers supply immutable collection nodes only; semantic value
  objects remain opaque content identities owned by the calling layer.
- Node and key identities use the crate's closed domain-separated,
  length-prefixed binary preimages. Serde encodings are transport only and must
  never become hash authority.
- Raw proofs are untrusted transport. Only successful verification may produce
  a verified read, page, or apply result, and verified capabilities are not
  serializable or publicly constructible. Nodes, cursors, and proofs are
  decoded only through this crate's byte- and element-bounded entry points;
  do not restore public `Deserialize` on those authority-bearing types.
- Exact absence, range continuity, terminal boundaries, parent roots, result
  roots, counts, and ordered log positions fail closed. Never accept a caller
  assertion in place of recomputation.
- Map and log range proofs carry one deduplicated node closure and are replayed
  by the canonical seek/successor traversal. Resolver reads and proof nodes are
  bounded by `O(height + returned entries)`; do not restore per-rank or
  per-ordinal exact proofs inside a page.
- Every validated map key must fit as the sole entry in a maximum-byte page.
  Keep key, page-accounting, mutation, node-transport, and proof-transport
  bounds derived from the same exported `MAX_MAP_KEY_BYTES` contract.
- Generic logs preserve order and permit repeated value identities. Any family
  that requires uniqueness enforces it above this crate.
- Ordered-log append, ordinal insert/replace/remove, authenticated split, and
  bounded prefix replacement share this crate's one AVL mutation/proof
  authority. Prefix receipts bind the verified split root; higher layers must
  not reconstruct or materialize a large prefix to derive that commitment.
- Apply and split proofs contain only the exact parent closure loaded during
  replay. Provider outputs contain only result-reachable immutable nodes that
  were absent from the resolver; never persist intermediate rotations.
- Provider failures cross the resolver seam only through the closed
  `ProviderFailure` and `ProviderConflict` variants. Preserve validation,
  integrity, revision/history conflict, and substrate fields exactly; never
  flatten a provider error through `Display` or parse display text.
- Keep exact and range operations bounded by their declared limits. Full
  traversal and genesis rebuild are explicit audit/maintenance operations and
  must not be used by ordinary exact lookup or page APIs.
