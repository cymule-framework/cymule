# Version Authority

`version-domains.json` is the only authored inventory of Cymule semantic, wire,
persistence, binding, receipt, and projection versions. The validator resolves
its defaults, verifies source and package ownership, authenticates schema bytes,
checks external `$ref` and Cargo dependency edges, and generates the normative
table in `docs/version-domains.md`.

The inventory covers public protocols and private production identity domains
alike. Artifact kinds, Merkle/hash separators, content-ID domains, persistence
records, provider generations, SDK literals, and release receipts must all have
one real owner and cannot hide behind a non-public Rust constant.

Rust production scanning removes only syntactically bounded `#[cfg(test)]`
module bodies. Rejection probes, historical fixtures, and caller-defined test
Artifact kinds do not become current domains; they remain separate conformance
evidence for exact rejection or historical-import behavior.

Each authority source is an exact named constant or exact comment-free literal
token with a semantic role. JSON anchors resolve through strict duplicate-free
I-JSON: a pointer either equals the version or explicitly selects one bounded
version token inside a string. Separate `literal_locations` enumerate DTO,
schema, adapter, and release consumers; these are not mislabeled as generators
or second writers. Named Rust separators passed directly to `content_id` must
declare `content_id_domain` or `catalog_namespace` ownership, which also closes
their dependency on `cymule.jcs/1`; `#[cfg(test)]` calls are excluded. A domain
may own multiple authenticated schemas, so root and supporting schema bytes
both enter the release BOM. A supporting schema record
names the exact decoded JSON Pointer or `$anchor` it owns; dependency validation
first proves that target exists and then follows only the reachable fragment,
not unrelated definitions elsewhere in the same file. Every tracked root and
plugin schema has at least one owner and exactly one document-root owner.
Supporting ownership and predecessor comparison use the exact `(path,
fragment)` pair, so one domain can own multiple real contracts in the same
document. The BOM retains one path/ID/digest record and rejects any conflicting
document digest; this never widens an external reference to the whole file.
Rust-only normalized receipts still declare their direct typed containment.
The source-mechanical governance test reads their actual Serde field types,
expands unversioned wrappers, and stops at reviewed wire/semantic fragment
owners. Their SDK readers are real re-exports covered by the Rust facade test;
neither a copied transport schema nor a definition-only interface is evidence.

`embeds` records direct typed containment and is a subset of `depends_on`.
Exact version dependencies may form a cross-domain strongly connected
component when the data model is recursively finite—for example, a Resource
Handle may name a manifest whose entries contain Resource Handles. This is not
version probing or a release-order cycle: every edge selects one exact domain,
self-edges remain invalid, and the Cargo release catalog is validated as a
separate topological graph.

`source_generation.source_snapshot_digest` hashes each ordered public-export
Git path, mode, object type, and exact blob bytes. Private mirror-controller
paths and the registry itself are excluded. Candidate, committed-history, and
rewritten-public calculations share that preimage, so the identity survives the
real public-history rewrite without collapsing executable files, symlinks, and
regular blobs. `baseline_source_snapshot_digest` identifies the reviewed
historical source snapshot. A genesis registry sets all four `predecessor_*`
members to null and is valid only when HEAD ancestry contains no registry from
another release generation; the same unreleased genesis generation may be
refined before it is frozen. Every domain in that first registry has explicit
null current-source provenance. A successor sets all four members:
`predecessor_registry_digest` is
the strict RFC 8785 digest of the real prior registry, and
`predecessor_source_snapshot_digest` locates those same public bytes in ancestry.
The verifier accepts neither a partial predecessor nor a fabricated genesis and
checks the declared predecessor registry/source generation together; it never
depends on a private commit ID that the public mirror rewrites.

After genesis, unchanged entries inherit an exact historical snapshot through
`defined_at_source_snapshot_digest`. Entries introduced, changed, or affected
through a dependency edge in the current candidate use an explicit null because
the current snapshot cannot contain its own digest. Supporting schema comparison
hashes only the declared reachable fragment. Strict authority JSON rejects
duplicate members, floats, out-of-range integers, invalid Unicode, and anything
outside the no-float I-JSON subset; canonical object keys use UTF-16 order.

Private and public commit SHAs are publication receipts, not registry
provenance. `cymule.release-bom/2` binds the immutable release source and package
evidence, never a mutable finalizer controller identity. The exact annotated
tag object and current controller are separately bound by the finalization
stage, attestation, and same-run control-plane receipt. Every source package has a required `publication`
member: Cargo and npm records contain exact registry bytes plus checksum or
Sigstore evidence, while the intentionally unpublished Python and Go surfaces
carry explicit `null`. npm evidence records the historical publisher
`signer_ref` and certificate-bound `signer_sha`; advancing the finalizer cannot
change the BOM bytes for the same release. The public
mirror writes both SHAs beside the common source-snapshot digest in its receipt.
The BOM, registry digest, schema digests, and package bytes together freeze a
published generation. Migration metadata is a closed mode/edge/runbook union;
every non-null runbook must be a present, non-ignored path in the source
candidate, including a new file that will enter the same commit.

Every `conformance` entry is the exact name of a concrete leaf in
`tests/harness/suites.toml`. Registry authority changes route all declared
leaves or the abstract `full` suite; an incomplete hand-maintained subset is
rejected before registry verification.

An installed binary owns one exact registry generation. It selects its closed
decoder before reading a protocol body; the same protocol string with another
schema digest is a different generation, never a reason to probe another shape.
The current registry is also exactly equal to the production identity-literal
inventory: historical rejection fixtures cannot enter it as extra domains.

## Current Virtual contract scope

Status: implemented source boundary; release evidence still comes from the
complete configured verification suite.

The current Rust profile owns normalized keyed state and complete typed
persistence receipts. `virtual-control.schema.json` freezes only the finite
authoring commands and bounded DTOs actually shared by the language SDKs.
The removed checkpoint and journal-base models had no Rust producer or reader;
their only remaining bytes are explicit historical rejection cases.

| Removed caller-zero surface | Current authority |
| --- | --- |
| Checkpoint/snapshot/journal-base schemas and positive fixtures | Rust keyed current state and typed persistence operations; historical payloads are rejected |
| Three-language `VirtualClaimReceipt`, coupled-journal helpers, and `VirtualCompactionReceipt` mirrors | Complete Rust persistence receipts and `VirtualClaimOutcome` |
| Three-language `VirtualWorkControl`, `RegionMigrator`, and `VirtualArchive` interfaces | Actual Rust runtime and provider interfaces; SDKs expose finite authoring DTOs only |
| Rust SDK control module and its five definition-only control traits; three-language generic Durable/Evolution submit interfaces | Implemented Engine/DurableEngine operations and concrete Durable runtime controls; all real commands, receipts, and provider extension traits remain |

The removal audit found definitions, README descriptions, and self-validating
fixtures, but no implementation or application caller for those SDK
interfaces. The SDK source-surface regression test prevents their reintroduction.
This cut adds no new Engine or provider transport. Region migration includes
its immutable provider revision; compaction includes complete bounded selections
and copies an identity emitted by the real Rust constructor without a local
identity implementation.
