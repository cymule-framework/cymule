# Filesystem Resource Guidance

- Store bytes by verified SHA-256, never by caller path. A resolver reference is
  a non-secret digest interpreted only under the configured immutable binding.
- Upload IDs derive from caller write IDs. Chunk retry at an already persisted
  offset succeeds only when the retained bytes match exactly; gaps and changed
  bytes fail closed. A whole-file import must also replay after publication by
  comparing every submitted chunk with the committed content; this closes the
  Resource-published/consumer-checkpoint-not-yet-acknowledged recovery window.
- Write and fsync a unique staging object before linking it into the content
  namespace. Never expose partial bytes at a committed Resource location.
- Whether the content link is newly created or already exists, verify its exact
  digest and size, fsync the objects directory, remove this upload's staging
  object, fsync staging, and only then persist the committed publication. A
  committed import replay compares source and retained content in one sequential
  pass; never hash the whole object once per retried chunk.
- `cymule.resource-fs-upload/2` records the only acknowledged chunk frontier.
  Sync bytes and a newly created upload directory entry before atomically
  advancing that frontier. On reopen, truncate only bytes beyond it; bytes below
  it are immutable and missing acknowledged bytes are corruption.
- Every content-addressed stat and manifest list verifies the complete SHA-256
  digest, not only size or pathname. Object reads go through `ResourceClient`:
  stat verifies before streaming and the client independently hashes the exact
  streamed bytes once, avoiding an O(n-squared) hash per chunk. Sync both sides
  of cross-directory record publication and staging removal before acknowledging
  completion.
- Directory and snapshot resources are sorted JSON-lines manifests of child
  Resource Handles. Reject symlinks, unsafe names, duplicates, and malformed
  manifests; list with an opaque byte-offset cursor instead of materializing a
  complete tree.
- Cross-process writer claims are non-blocking. Contention returns a Resource
  conflict; retry policy belongs to the caller.
- Commit replay and abort remove the upload data and owned staging file, fsync
  both directories, verify absence, and return `cymule.resource-cleanup-receipt/1`.
