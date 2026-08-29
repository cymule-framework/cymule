# Cymule Object Store Resources

`cymule-resource-object-store` implements bounded Cymule object Resource writes,
reads, lifecycle deletion, and explicit upload-content reclamation on Apache
`object_store`.

```sh
cargo add cymule-resource-object-store
```

## Provider boundary

Apache `object_store` supplies byte operations, but its public `list` and
`list_with_offset` contract explicitly does not guarantee ordering. Upload
content GC additionally requires strong list-after-write/delete visibility,
complete lexicographic inventory, and exclusive continuation. The inventory
trait and shared constructor are private. Public construction accepts only one
of the closed reviewed provider wrappers; there is no erased
`Arc<dyn ObjectStore>` or caller-defined capability seam.

| Feature | Constructible backend | Inventory authority |
| --- | --- | --- |
| default | none | provider-free crate surface; in-memory inventory exists only inside `cfg(test)` conformance |
| `azure` | `AzureBlobInventory::{from_access_key,from_bearer_token,from_client_secret,from_sas_query_pairs}` followed by `ObjectResourceStore::from_azure_blob` | official Azure Blob endpoint only |
| `gcp` | `GoogleCloudInventory::{from_application_default_credentials,from_service_account_key,from_bearer_token}` followed by `ObjectResourceStore::from_google_cloud` | official Google Cloud Storage endpoint only |

The crate has no production `fs`, `http`, or `aws` feature. Local durable files
belong to `cymule-resource-fs`. Generic S3 is deliberately unsupported because
AWS guarantees lexicographic `ListObjectsV2` order for general-purpose buckets
but not directory buckets, while Apache exposes both through the same concrete
type. Azure custom endpoints, Azurite, Fabric endpoints, and GCS endpoint
overrides are outside every public wrapper constructor. GCS service-account
documents that select `gcs_base_url` are rejected before provider construction.
The wrapper itself creates the pinned provider, so a caller cannot smuggle in a
custom HTTP connector through an upstream builder before type erasure. Azure
constructors additionally enforce the official storage-account and container
name grammar before the upstream builder interpolates either value into a URL
([account rules](https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules#microsoftstorage),
[container rules](https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata)).

The service contracts relied on here are:

- [Azure strong consistency](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage)
  and [alphabetical Blob listing](https://learn.microsoft.com/en-us/rest/api/storageservices/enumerating-blob-resources);
- [Cloud Storage strong object listing](https://cloud.google.com/storage/docs/consistency)
  and [the XML listing order/continuation used by the pinned adapter](https://cloud.google.com/storage/docs/xml-api/get-bucket-list);
- [S3 general-purpose versus directory-bucket ordering](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
  which is why the S3 adapter is not admitted.

The workspace pins `object_store` exactly to `0.14.1`. That pin binds the
reviewed Azure/GCS adapters that translate continuation into the provider
requests; it does not upgrade the generic trait into an ordering promise. On
open, three immutable markers exercise complete order, exclusive continuation,
and list visibility. This startup canary catches a miswired endpoint or adapter;
it is not a runtime capability attestation and is not the reason Azure/GCS are
trusted. Conditional create and update remain the concrete provider wrappers'
write contract and every semantic head transition uses them directly. The
publication fence uses one exact object key, relying on Azure's
[conditional Blob writes](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations)
and GCS's [generation preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions).
It requires neither a cross-object transaction nor a multipart-upload fencing
guarantee.

## Physical and upload authority

Every prefix carries the exact
`cymule.resource-object-store-layout/2` marker. A non-empty unmarked prefix or
another generation is rejected. There is no compatibility reader or migrator
for layout 1.

Each `cymule.resource-object-store-upload/7` record is a fixed-cardinality CAS
head: it retains the authenticated write intent, a non-reusable generation, the
current content epoch, scalar byte/chunk frontiers, one authenticated radix-tree
root, optional bounded migration progress, compact publication evidence, and an
empty cleanup plan/receipt. Legal tiny chunks cannot grow this head. Upload
records have an exact 16 MiB physical budget;
`cymule.resource-catalog-record/2` uses the protocol's single 16 MiB
canonical-JSON budget. A write
preflights its largest terminal and migration shape before creating provider
state; reads reject oversized provider metadata before downloading the body and
require the realized byte count to match it.

Upload heads are partitioned as
`uploads/<binding-digest>/<upload-key>/record.json`. The same prefix owns
exact head access and GC enumeration, so multiple bindings can share a storage
prefix without one binding scanning or rejecting another's active or terminal
uploads.

Chunk bytes and 4 KiB authenticated radix nodes live in the global immutable
content-addressed namespace
`upload-content/<binding>/epochs/<epoch>/{data,nodes}`. A writer publishes a
candidate there and then advances only the exact upload head with conditional
CAS. An abort or completed publication terminalizes that head. A paused old
writer can leave immutable unreachable content, but its stale CAS cannot make
the content visible again. Session cleanup is consequently an empty plan; it
does not claim that a concurrently writable staging prefix was deleted.

Commit hashes the exact acknowledged frontier, records `Publishing`, creates
immutable 8 MiB content parts, and conditionally creates a small `Published`
content index last. This index is the only publication visibility authority.
Its permanent terminal state is `Deleted`; no operation removes the terminal
index or changes it back to `Published`. A writer paused before a part or index
PUT can therefore leave an invisible part after deletion, but cannot restore
the publication. Equal retries verify retained bytes. The sole opaque locator is the exact
content digest. `stat` verifies the complete content digest; bounded range
`read` validates the requested indexed parts, and terminal EOF requires a
complete physical family. `ResourceClient` independently verifies the exact
streamed bytes.
The ordinary begin/chunk/commit sequence can also be retried after publication
starts or after commit succeeds but the consumer's publication catalog has not
been saved. Pending publication compares each original acknowledged chunk
against its retained upload root; committed replay compares only the requested
published part ranges, including after upload-content GC. These comparisons do
not write or advance upload state. The original commit still checks the full
object, and a cleanup receipt cannot recreate a deleted object or revive an
aborted upload.

Lifecycle deletion uses only the already-authorized M1
`ResourceDeletionTarget`. Before removing anything, the adapter checks every
present physical member, verifies any retained index against that target, and
rejects unknown paths or wrong sizes. A complete remaining object also retains
full digest verification. An interrupted deletion may have already removed
parts; their absence is valid completion of that same target.
After complete preflight, deletion conditionally transitions the exact index
key to `Deleted` before removing recognized parts. An absent index is fenced
with conditional Create; another publisher winning that race is a conflict,
not permission to delete under stale evidence. Lost fence acknowledgement
resolves only by reading back the identical tombstone. Reopening resumes the
same target and requires both this permanent fence and current payload absence.
Tombstones contain only the exact physical digest, size, and part layout; they
do not retain content bytes or a second lifecycle receipt. Semantic annotation
differences do not create different physical deletion authority.

These guarantees are distinct: the publication stays logically absent;
successful deletion proves that its payload is absent at that readback; and
non-payload fence metadata remains permanently retained. A paused writer can
still create an unreachable payload part afterward because provider conditions
apply to individual keys. Future explicit GC cycles reclaim those bytes. The
adapter does not claim permanent physical absence of every provider key or
remote historical object version. The Resource lifecycle already makes the
same binding/digest deletion permanent, so a deleted family cannot be reopened.

## Explicit bounded reclamation

Immutable candidate orphans are reclaimed only through
`ObjectResourceStore::reconcile_upload_content`. One call advances one bounded
step: the controller first CAS-advances the epoch, then inventories and migrates every
Open/Publishing upload head in at most 64 chunks per call, and finally sweeps at
most 512 old-epoch data/node objects per page. Before any delete, the controller
validates the complete page and CAS-persists its fixed-size relative-path plan
in the GC head; a later call executes that exact plan. This two-phase page is
the response-loss/process-death authority: a missing old object while replaying
an admitted plan is an already completed delete, whereas a newly listed missing
object still fails closed. Shared chunks remain live when any active head is
migrated to the current epoch. A pending publication that now has an exact
permanent deletion tombstone instead terminalizes as Aborted with its original
empty cleanup receipt, so its unreachable chunks are not migrated forever.

The cycle then examines the binding's publication namespace in pages of at
most 512 objects, including live indexes and parts. It persists the final
examined cursor and only those part targets authorized by an exact permanent
Deleted tombstone before removing them. Each target binds the tombstone's
content ID and the part's exact path and size. Empty-target pages still advance
over live objects; live publication parts and all terminal indexes remain
untouched. Missing authorized parts are valid replay here because ordinary
lifecycle deletion can remove them concurrently without moving the GC head.
This traversal also collects late final parts whose upload epoch was already
reclaimed and whose lifecycle deletion receipt is already Completed.

The caller/operator must repeatedly invoke `reconcile_upload_content` until the
returned `complete` is true. Completion means the persisted cursor reached the
provider inventory end and that traversal admitted no reclaimable payload. It is
not a global absence proof while a stale writer can still publish immutable
candidate bytes. A candidate or retired-family part published after its
lexicographic position was passed, including during the completing traversal,
creates next-cycle work;
operators must schedule later cycles rather than treating one completion as a
permanent background-GC lease. CAS conflicts are retried by invoking the same
operation again. The persisted epoch/phase/cursor/page plan makes response loss,
concurrent drivers, restart, and process death replay the same authority.

The returned `confirmed_absent_objects` counts old-epoch or deleted-family
payload targets from that persisted page whose absence was verified. Replaying the same page reports the
same progress, including when another driver already removed its targets. This
is not an additive unique-deletion metric across retries or concurrent calls;
the persisted page, phase, and exact absence readback determine completion.

Inventory processing validates strict monotonicity across bounded pages and an
exclusive cursor, verifies newly selected old-epoch objects exist before deletion, and
requires immediate absence afterward. Unordered, duplicate, missing canary,
inclusive-offset, and stale-delete providers fail closed.
If another driver advances the exact GC head while inventory is being checked,
a missing object or an object from its newer epoch is a stale-proposal conflict,
not provider corruption. This distinction checks both the head and its
conditional version, including identical-content head changes; either
contradiction under an unchanged head remains an integrity failure. The separate
deleted-family traversal uses its exact immutable tombstone for already absent
parts, as described above; a GC-head check cannot invalidate an authorized
concurrent lifecycle deletion.

The synchronous adapter owns a private Tokio runtime. Invoke it from a
synchronous worker or `spawn_blocking`, not from a current-thread async runtime.
Credentials remain solely inside the configured provider and never enter
Resource handles, locations, records, errors, or logs.
