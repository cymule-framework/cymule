# Cymule Filesystem Resources

`cymule-resource-fs` implements Cymule's bounded `ArtifactStore` and
`ArtifactResolver` contracts on a local filesystem. Objects are content
addressed, upload retries compare retained bytes, and committed paths never
expose partial data.

The upload journal uses a durable acknowledged-byte frontier. A restart drops
only an unacknowledged partial suffix and lets the caller replay the original
chunk. New upload entries, record replacement, object publication, and staging
removal are directory-synced before acknowledgement. Reads and directory lists
fail closed on same-size tampering: ordinary-object `stat` recomputes complete
raw SHA-256, manifest `stat` reconstructs its root-derived descriptor, and
directory pages verify only their requested JSON-lines against a publication-
owned content-addressed Merkle index. Direct reads enforce the shared 8 MiB
range bound, and an equal metadata replay re-syncs retained files and directory
entries before returning success.

Each root carries the exact `cymule.resource-fs-layout/1` physical marker.
Opening a non-empty unmarked root or another generation fails before a current
upload can create a second authority. The root, fixed directories, generated
entries, and recursive import children are opened beneath no-follow directory
descriptors, so a symlink swap cannot redirect an already opened store.
First initialization writes and syncs a same-directory staging marker, atomically
renames it, and syncs the root. A retry cleans only that owned staging residue;
a zero-length final marker is recoverable only before any namespace data exists.
`FsResourceStore::open_read_only` opens only a complete current layout, creates
or cleans nothing, rejects mutation APIs, and keeps resolver reads available.
Root and binding-namespace validation streams directory entries, so opening a
malformed high-cardinality namespace does not first collect every name.

Commit records the complete `Publishing` intent before linking any content
family. Commit, abort, and reopen converge that same intent before removing
upload data, including acknowledgement loss after content publication. Cleanup
first persists an exact target plan in `cymule.resource-fs-upload/8`; only after
absence readback does it persist the unique plan-derived receipt, which remains
queryable and replays byte-for-byte. Equal chunk retries re-sync the retained
file and owning directory before success.
The same public begin/chunk/commit sequence can resume before a consumer has
saved its publication catalog: while publication is pending, chunk retries
only compare acknowledged upload bytes; after commit they compare the published
object range. Equal retries read only the requested range, and the original
commit verifies the complete object before returning the original publication.
An old cleanup receipt never recreates collected content or revives an abort.
An Open file or directory import validates its complete ingested length or
manifest under the final upload claim before entering `Publishing`. Retrying
only a shorter acknowledged prefix therefore returns a conflict while leaving
that parent upload Open and unpublished; the complete source can still finish
the same write later. Physical corruption retains its integrity error instead
of becoming an input conflict. Persisted upload metadata that no longer passes
its complete record contract is likewise reported as
`filesystem_upload_record_invalid`, never as fresh caller validation. Already
admitted Publishing/Committed intent still converges through its original
publication and cleanup authority.

```sh
cargo add cymule-resource-fs
```

Files import as object Resources. Directories import recursively into sorted
JSON-lines manifests of child Resource Handles; symbolic links are rejected.
Directory import externally sorts names through fixed-entry runs and fixed
merge fan-in beneath one deterministic upload-owned staging tree, then streams
the final run directly into bounded manifest chunks. It never retains the full
name set or the full child-entry set, and restart discards and rebuilds only
that owned sort tree before exact replay. Terminal cleanup treats the tree as
one fixed plan target and streams its children, so cleanup authority and receipt
size do not grow with directory cardinality. The caller's root is depth zero;
imports accept up to 64 nested child directories and return caller validation
with the stable `filesystem_import_depth_exceeded:` message prefix before
descending into level 65. The same bound applies to first import and
published-source replay without changing structural child write IDs or manifest
identity.
Sort preparation checks the current upload phase under a short writer claim,
so an abort completed after upload admission cannot recreate cleaned staging.
Directory listing uses the core self-contained cursor and does not materialize
the whole tree or index. Every non-initial page reads the exact preceding entry
and Merkle path in addition to its bounded page, so strict cross-page order is
root-verifiable after restart instead of trusting a publicly recomputable
cursor. Publication writes one immutable `ResourceCatalogRecord` header, fixed
offset table, and fixed Merkle-node table. Manifest ingress reads
one capped canonical line at a time and builds its root with `O(log n)` Merkle
memory; it never retains complete entries, bytes, and tree levels together. A
manifest object is addressed by its `cymule.resource-manifest/3` descriptor ID;
publication, stat, full copy, and deletion reconstruct that same descriptor and
never require a parallel raw-byte SHA for manifest bytes. A
page selects at most its requested entries within an 8 MiB canonical-byte
budget, and every entry is at most 1 MiB including its newline. Upload IDs and
records include the full configured binding, so bindings sharing a root cannot
resume each other's uploads. Binding-derived physical subdirectories also
isolate objects, catalogs, and manifest indexes, so deleting one binding cannot
remove identical bytes retained by another. Opaque locators must exactly equal the Handle's retained
content digest. This plugin is suitable for local and single-host deployments,
not shared distributed storage.

Lifecycle deletion is provider-bound: the adapter receives only the durable
`ResourceDeletionTarget`, exact-matches its physical-family binding, removes
that content family idempotently, syncs it, and succeeds only after absence
readback. Locator paths, caller-supplied absence flags, and removed-byte counts
are not deletion authority.

Catalog writes and reads accept only `cymule.resource-catalog-record/2` and
enforce its protocol-owned 16 MiB canonical JSON limit before creating an entry
or materializing provider bytes.

The `cymule.resource-fs-upload/8` journal is also fixed-cardinality: it stores a
scalar acknowledged frontier, compact publication descriptor, and the bounded
cleanup plan/receipt, never a vector of chunks or directory entries. Before it
acquires the cross-process writer claim or creates an upload/lock entry,
`begin_write` encodes the largest legal terminal record and enforces the exact
16 MiB physical budget. Record reads are bounded before decoding. Oversized
ordinary metadata is therefore rejected without leaving a lock, upload, sort
run, or other unreachable filesystem object.
