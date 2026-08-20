# Cymule Filesystem Resources

`cymule-resource-fs` implements Cymule's bounded `ArtifactStore` and
`ArtifactResolver` contracts on a local filesystem. Objects are content
addressed, upload retries compare retained bytes, and committed paths never
expose partial data.

The upload journal uses a durable acknowledged-byte frontier. A restart drops
only an unacknowledged partial suffix and lets the caller replay the original
chunk. New upload entries, record replacement, object publication, and staging
removal are directory-synced before acknowledgement. Reads and directory lists
recompute the complete SHA-256 digest, so same-size object or manifest changes
fail closed.

```sh
cargo add cymule-resource-fs
```

Files import as object Resources. Directories import recursively into sorted
JSON-lines manifests of child Resource Handles; symbolic links are rejected.
Directory listing uses an opaque byte-offset cursor and does not materialize the
whole tree. This plugin is suitable for local and single-host deployments, not
shared distributed storage.
