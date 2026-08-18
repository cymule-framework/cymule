# Cymule Object Store Resources

`cymule-resource-object-store` implements Cymule object Resource writes and
bounded reads on Apache `object_store`.

```sh
cargo add cymule-resource-object-store
```

The default build can wrap in-memory or local stores. Feature flags `aws`,
`azure`, `gcp`, and `http` enable Apache's maintained provider implementations;
provider names and credentials remain outside Cymule Resource semantics.

Writes persist conditional metadata and immutable chunks. Commit streams them
through multipart upload, publishes under the verified digest with
`copy_if_not_exists`, checks the resulting bytes, and records an idempotent
Resource Handle. Backends without the required conditional operation fail
closed. Collection/directory listing remains the filesystem-manifest plugin's
responsibility in this initial profile.
