# Official Plugins

Status: implemented for the day-one local/single-domain set listed below.

Cymule core defines semantic identities and provider-neutral boundaries. Plugins
realize those boundaries with replaceable infrastructure or protocols; they do
not move provider names, credentials, topology, Agent Loops, or transport state
into Plans and Events.

## Day-one set

| Crate | Boundary | Maintained foundation | Implemented guarantees |
| --- | --- | --- | --- |
| `cymule-directory-store` | `DurableStore` | Rust filesystem APIs, `fs4` | immutable segments/checkpoints, atomic head replacement, bounded reopen, receipt-backed GC, explicit offline v1 migration, fsync, non-blocking writer claim |
| `cymule-store-sqlite` | `DurableStore` | SQLite through `rusqlite` | immutable segments/checkpoints plus small-head CAS, bounded reopen, receipt-backed GC, explicit offline v1 migration, WAL/full synchronous, zero busy timeout |
| `cymule-resource-fs` | `ArtifactStore` / `ArtifactResolver` | Rust filesystem APIs, `fs4` | content addressing, exact chunk retry, atomic publication, recursive directory manifests, bounded cursor listing |
| `cymule-resource-object-store` | object `ArtifactStore` / `ArtifactResolver` | Apache `object_store` | conditional records/chunks, bounded multipart promotion, digest verification; maintained S3, GCS, Azure and HTTP clients; backends without required CAS operations fail closed |
| `cymule-activation-http` | signal `WaitSourceDriver` | Axum, Tokio, SQLite | durable ingress spool, fair parked-key cursor and indexed matching beyond arbitrary transport prefixes, injected authorization, response only after activation acknowledgement, exact reopen/redelivery, duplicate/conflict handling |
| `cymule-activation-timer` | timer `WaitSourceDriver` | SQLite through `rusqlite` | durable schedules and selected targets, injected clock, selection-required acknowledgement, exact redelivery after restart until acknowledgement |
| `cymule-clock-system` | logical-time observation | OS clock, SQLite through `rusqlite` | strictly increasing per-scope values across restart and backward wall-clock movement, content-identified observations, zero-timeout contention |
| `cymule-executor-process` | `PluginHost` | `std::process`, `wait-timeout` | cleared environment, explicit args/env, bounded pipes, concurrent draining, timeout kill/reap |
| `cymule-observability-otel` | derived observation | `tracing`, OpenTelemetry Rust, OTLP | bounded identity records, caller-composed layer, no global subscriber or semantic authority |
| `cymule-agent` | optional Agent-domain contracts | Cymule durable journals | Sessions/projections, pinned host occurrences, input/workspace/stream durability without an Agent Loop |
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
commits immutable deltas and periodic checkpoints behind one small-head CAS.
Under that contract, SQLite already supplies transactional compare-and-swap, simple
inspection, migrations, and much lighter build/operations.

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
- The process executor is not an untrusted-code sandbox. Wasmtime Component
  Model, containers, and remote sandboxes are separate executor plugins.
- Telemetry failure never changes a Run result.
- Plugin credentials never enter Resource Handles, Plans, Events,
  Continuations, receipts, fixtures, or logs.
