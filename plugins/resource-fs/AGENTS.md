# Filesystem Resource Guidance

- Store bytes by verified SHA-256, never by caller path. A resolver reference is
  a non-secret digest interpreted only under the configured immutable binding.
- Upload IDs derive from caller write IDs. Chunk retry at an already persisted
  offset succeeds only when the retained bytes match exactly; gaps and changed
  bytes fail closed.
- Write and fsync a unique staging object before linking it into the content
  namespace. Never expose partial bytes at a committed Resource location.
- Directory and snapshot resources are sorted JSON-lines manifests of child
  Resource Handles. Reject symlinks, unsafe names, duplicates, and malformed
  manifests; list with an opaque byte-offset cursor instead of materializing a
  complete tree.
- Cross-process writer claims are non-blocking. Contention returns a Resource
  conflict; retry policy belongs to the caller.
