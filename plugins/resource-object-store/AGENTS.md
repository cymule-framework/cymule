# Object Store Resource Guidance

- Depend on Apache `object_store`; do not implement cloud authentication,
  signing, retries, HTTP, multipart protocols, or provider clients here.
- The adapter persists write intent, exact chunk frontier, and commit receipt as
  object-store records. Chunk objects use create preconditions; metadata
  advances with e-tag/version update preconditions.
- Persist planned chunk inventory before put and `publishing` plus the final
  descriptor before copy. Recovery completes that exact inventory.
- Authenticate sessions from `write_id` plus binding and accept only exact
  lowercase digest keys before constructing object paths.
- Commit streams retained chunks through a bounded multipart staging object,
  computes SHA-256, promotes with `copy_if_not_exists`, verifies downloaded
  bytes, and only then records the Resource publication. Commit and abort delete
  every owned staging and chunk object, read back absence, and return an exact
  cleanup receipt; ignored best-effort deletion is not conformance.
- A backend that cannot provide conditional create/copy must fail closed; never
  weaken content-addressed publication to an unchecked overwrite.
- The synchronous Cymule contract owns a private Tokio runtime. Call it from a
  synchronous worker or `spawn_blocking`, not a current-thread async runtime.
- Credentials remain exclusively in the configured `ObjectStore` instance and
  never enter Resource locations, upload records, errors, or logs.
