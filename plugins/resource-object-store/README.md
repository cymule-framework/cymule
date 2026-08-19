# Cymule Object Store Resources

`cymule-resource-object-store` implements Cymule object Resource writes and
bounded reads on Apache `object_store`.

```sh
cargo add cymule-resource-object-store
```

Feature flags `aws`, `azure`, `gcp`, and `http` enable Apache's maintained
provider implementations; provider names and credentials remain outside Cymule
Resource semantics. A backend must implement conditional create, metadata
update, and copy. Apache `object_store`'s local filesystem backend does not
currently provide that complete CAS surface and therefore fails closed; use
`cymule-resource-fs` for local durable files.

Writes persist conditional metadata and immutable chunks. Commit streams them
through multipart upload, publishes under the verified digest with
`copy_if_not_exists`, checks the resulting bytes, and records an idempotent
Resource Handle. Backends without the required conditional operation fail
closed. Collection/directory listing remains the filesystem-manifest plugin's
responsibility in this initial profile.
