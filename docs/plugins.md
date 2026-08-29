# Official Plugins

Status: implemented for the day-one local/single-domain set listed below.

Cymule core defines semantic identities and provider-neutral boundaries. Plugins
realize those boundaries with replaceable infrastructure or protocols; they do
not move provider names, credentials, topology, Agent Loops, or transport state
into Plans and Events.

## Day-one set

| Crate | Boundary | Maintained foundation | Implemented guarantees |
| --- | --- | --- | --- |
| `cymule-directory-store` | `DurableStore` | Rust filesystem APIs, `fs4` | immutable typed StateRoot and Machine-archive objects, atomic small-head replacement, bounded active-state reopen and typed historical lookup, receipt-backed bounded GC, exact rejection of predecessor physical generations, fsync, non-blocking writer claim |
| `cymule-store-sqlite` | `DurableStore` | SQLite through `rusqlite` | immutable typed StateRoot and Machine-archive rows plus small-head CAS, snapshot-pinned bounded reopen/lookup/GC, exact rejection of predecessor physical generations, WAL/full synchronous, zero busy timeout |
| `cymule-resource-fs` | `ArtifactStore` / `ArtifactResolver` | Rust filesystem APIs, `fs4`, Unix descriptor-relative APIs | exact physical marker, no-follow/beneath ownership, read-only opener, full-source validation before Publishing, bounded Publishing/Committed replay, persisted-record corruption classification, exact chunk retry with re-sync, O(log n)-memory manifest ingress and root-derived descriptor verification, bounded cursor listing |
| `cymule-resource-object-store` | object `ArtifactStore` / `ArtifactResolver` | exact-pinned Apache `object_store`; private inventory contract with closed typed Azure Blob/GCS constructors | layout/2 hard cut, catalog-record/2 metadata admission before allocation, fixed upload head plus authenticated immutable chunk tree, epoch-fenced bounded orphan GC, visible fixed-size part promotion, permanent non-payload Deleted index fence, logical/payload absence readback plus later bounded reclamation of unreachable late parts, digest verification, lost-ack/concurrent-driver/SIGKILL recovery; arbitrary erased stores, generic S3, HTTP, custom endpoints, emulators, and filesystem backends are outside this authority |
| `cymule-activation-http` | signal `WaitSourceDriver` | Axum, Tokio, SQLite | exact `cymule.activation-http-spool/1` physical generation with empty-only atomic initialization and no repair/import path, durable ingress spool, fair parked-key cursor and indexed matching beyond arbitrary transport prefixes, injected authorization, response only after activation acknowledgement, exact reopen/redelivery, duplicate/conflict handling |
| `cymule-activation-timer` | timer `WaitSourceDriver` | SQLite through `rusqlite` | exact `cymule.activation-timer-store/1` physical generation with empty-only atomic initialization and no repair/import path, durable schedules and selected targets, injected clock, selection-required acknowledgement, exact redelivery after restart until acknowledgement |
| `cymule-clock-system` | logical-time observation | OS clock, SQLite through `rusqlite` | strictly increasing per-scope values across restart and backward wall-clock movement, content-identified observations, zero-timeout contention |
| `cymule-executor-process` | `PluginHost` | `std::process`, `wait-timeout` | cleared environment, explicit args/env, bounded pipes, concurrent draining, timeout kill/reap |
| `cymule-observability-otel` | derived observation | `tracing`, OpenTelemetry Rust, OTLP | bounded identity records, caller-composed layer, no global subscriber or semantic authority |
| `cymule-agent` | optional Agent-domain contracts | typed profile reducers over the M1 StateRoot | Sessions/projections, fresh-only pinned host dispatch, head/count-pinned historical Context pages, input/workspace coupling, capacity-safe staged/external streams, and recovery without an Agent Loop |
| `cymule-agent-mcp` | MCP tool adapter | official `rmcp` | exact tool argument mapping, binding retention, explicit incomplete/error handling, Resource references without loop ownership |

The object-store adapter initially accepts `object` Resources. Filesystem
directory manifests provide the day-one hierarchical implementation; a
provider-neutral paginated object-manifest format remains a later additive
capability rather than an unbounded list fallback.

HTTP ingress initially owns signals. Typed external input remains with the
durable or Agent input controller until a generic typed-input source contract
exists; the plugin does not disguise input completion as a signal.

## RocksDB assessment

Status: proposed P1 adapter.

RocksDB is valuable for embedded services with large state, high write rates,
prefix/range access, column-family separation, snapshots, and compaction tuning.
It is not the best day-one default for Cymule's current `DurableStore`, which
moves one small CAS head over immutable StateRoot map/log nodes and exact typed
history lookup. Under that contract, SQLite already supplies transactional
compare-and-swap, exact schema inspection, and much lighter build/operations.

A future `cymule-store-rocksdb` becomes worthwhile when the durable substrate
profile exposes partitioned journals or incremental state families while
retaining one atomic admission fence. That adapter should use RocksDB
transactions or an explicit transactional metadata fence; a plain `WriteBatch`
plus an unlocked read is not a legal CAS. It must also prove crash recovery,
column-family migration, snapshot consistency, compaction behavior, backup,
and late/stale writer fencing. The C++ build and native packaging cost should
remain isolated in that plugin.

For a pure-Rust embedded alternative, `redb` is also worth evaluating at that
stage. Neither database belongs in the semantic core.

## Explicit non-goals

- Cloud queues do not become a canonical queue model; they implement activation
  or dispatch seams.
- Model-provider SDKs implement an application's `AgentHost`; they do not make
  model/tool turn order framework semantics.
- The official process executor captures a complete configured launch identity
  and rematerializes executable and working-tree bytes for each occurrence. It
  uses Unix process groups for bounded fork-tree cleanup and rejects non-Unix
  platforms. It is not an untrusted-code sandbox; Wasmtime Component Model,
  containers, and remote sandboxes are separate executor plugins.
- Telemetry failure never changes a Run result.
- Plugin credentials never enter Resource Handles, Plans, Events,
  Continuations, receipts, fixtures, or logs.
