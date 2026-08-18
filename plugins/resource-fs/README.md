# Cymule Filesystem Resources

`cymule-resource-fs` implements Cymule's bounded `ArtifactStore` and
`ArtifactResolver` contracts on a local filesystem. Objects are content
addressed, upload retries compare retained bytes, and committed paths never
expose partial data.

```sh
cargo add cymule-resource-fs
```

Files import as object Resources. Directories import recursively into sorted
JSON-lines manifests of child Resource Handles; symbolic links are rejected.
Directory listing uses an opaque byte-offset cursor and does not materialize the
whole tree. This plugin is suitable for local and single-host deployments, not
shared distributed storage.
