# Durable Evaluation Campaign Example

This directory is a user-facing reference application, not framework core or a
test fixture disguised as a product example.

## Boundaries

- Keep the evaluated subject and scorer behind the process-plugin interface.
  Never move model, Agent Loop, session, or provider behavior into Cymule core.
- Use provider-neutral Cymule contracts in campaign code. SQLite, the local
  filesystem, and the child process are replaceable local adapters selected by
  this example.
- Register the Resource-backed Virtual archive with its explicit immutable
  binding and implementation revision. Never infer that revision from mutable
  provider state or use a binding-only archive constructor.
- Every claimed case must retain its exact linked Plan ID separately from its
  immutable execution-binding Artifact. Evolution changes only future claims.
- Campaign-authored evolution review bytes travel as the exact
  `ArtifactRecord` inside the closed publication command. The sole
  live-evolution submit seam retains those bytes, the linked revision, and its
  full command receipt in one CAS; the campaign never prewrites an evidence
  journal.
- Read only through public Store, Evolution, and Virtual controls. Exact
  Artifact, region, Run, work, and latest-occurrence reads carry the same
  required observed revision; the pinned suite supplies case identities, not
  a whole-domain query. All immutable additions enter through their owning
  typed Evolution or Virtual command:
  region initialization retains suite metadata, publication retains evidence,
  and claim retains its execution binding in the same CAS as the transition.
  Never open a raw durable transaction, restore a scheduler or Evolution
  controller, enumerate a generic journal, or restore, clone, or diff a Machine
  in campaign code. Only the public claim result supplies executable Plan
  bytes, and its separate NoWork variant carries no Plan.
- Bind the subject and scorer as distinct runtime services with their exact
  capability properties, even when one executable implements both operations.
  The shared binary digest identifies implementation bytes; it does not replace
  per-operation requirement admission.
- Declare the Core-owned standard component output Artifact kind explicitly on
  both subject and scorer contracts. Never infer an occurrence output kind from
  a schema or provider manifest.
- Treat the subject as observational and safe to retry. A mutating subject must
  use an Effect plugin with idempotency and reconciliation instead.
- Let `ProcessExecutorConfig` own the process deadline default. The campaign
  must not shadow that framework policy with a second local default. Worker
  death before a retained result is recovered only through the explicit lease
  path; a returned evaluation error is a terminal case result, never an
  implicit retry policy.
- Keep the self-hosted debug executable capture explicitly bounded at 128 MiB;
  changing that bound changes the retained execution binding and requires
  focused process-plugin validation.
- The bundled process provider owns one explicit immutable runtime generation.
  A replacement `--plugin` must supply its provider-owned aggregate runtime
  revision as a lowercase `sha256:<64>` content ID; never substitute the host
  OS/architecture label or silently reuse the bundled provider's generation.
- The bundled and external process providers implement the exact 8 MiB
  `cymule.plugin/3` message domain. Do not narrow it with an example-local
  transport limit or widen it beyond Core's Artifact bound.
- Retain the captured durable provider for one worker process. Reopening
  control transfers that exact provider through `into_parts`, live
  `ExecutionBindingAdmission::admit`, and `open`; never recapture/hash its large
  executable for every scheduler mutation or reuse a mutable Store head. Reuse
  the Describe capture as the subject provider instead of discarding it. Each
  case still uses a fresh Embedded Machine and verifies its binding against the
  exact claimed reference. A completed campaign exits before any provider
  capture or Describe.
- Keep suite reads bounded and content-verified. Reject symlinks, duplicate case
  IDs, unknown fields, control characters, oversized files, and changed bytes.
  Pin one no-follow regular-file descriptor, reject an inode, size, mtime, or
  ctime generation change across the bounded read, and parse and store that same
  captured byte buffer. Never reopen the caller path between validation and
  Resource publication. Materialized page ends and capacities use exact checked
  arithmetic; an overflowing cursor-plus-limit is a source error, never a
  clamped valid page.
- Reject unknown, repeated, missing-value, and command-irrelevant CLI options
  before canonicalizing a plugin path or opening any campaign authority.
- `status` opens both SQLite and the filesystem Resource namespace read-only.
  Missing physical storage is an integrity failure; an observation command must
  never create or repair directories, markers, catalogs, or objects.
- `evolve` admits the closed policy selector before authority I/O, then proves
  the exact campaign region, retained metadata Artifact, and pinned suite
  Resource before publishing a future Plan. A Durable genesis without a valid
  campaign is not evolution authority.
- Use durable leases and CAS fencing for ownership. Do not add an ambient mutex,
  advisory process lock, or global singleton.
- Claim, recovery, and result commands carry only a current-head Clock receipt.
  The optional deterministic time in `CampaignOptions` is an injected wall-clock
  sample below `SqliteClock`, never lease authority consumed directly by the
  campaign. Execution signs a new current-head observation after plugin I/O and
  must resolve strictly before expiry; a claim TTL below two ticks cannot contain
  both claim and result observations and is rejected. Recovery signs a later
  current head to prove expiry.
- A matching worker identity may reuse only an unexpired active claim. Once its
  lease expires, the campaign first checkpoints the explicit recovery decision
  and only then issues a new claim; worker identity never bypasses lease expiry.
- The campaign ID is a VirtualRegion and fairness namespace, not a resumable M1
  parent Run. The region's required source Artifact owns the canonical suite
  metadata, keyed Evolution authority owns immutable Plan history, and each virtual
  claim atomically pins its selected Plan, typed ExecutionBinding Artifact,
  occurrence, and lease. Never create a synthetic claim-free or permanently
  Running parent Continuation.
- One campaign durable domain owns exactly one evaluation region. Before any
  subsequent mutation, require its Run, source binding, metadata Artifact,
  canonical cursor generation/position, exhaustion state, and estimated total
  to match the pinned suite exactly; never ignore additional regions.
- Establish the exact strict-definition and campaign-template receipts before
  publishing a suite region. Initialization accepts only an empty authority or
  the exact definition-only prefix. Every reopen rejects foreign revisions,
  templates, DAG edges, rollout decisions, or unused evolution features before
  any mutation.
- Keep M4 occurrence pins and M3 virtual occurrences in an exact one-to-one
  relation through the standard coupled claim receipt. The framework derives
  the selection identity from the Virtual persistence command; it need not
  equal the occurrence ID. Plan and ExecutionBinding agree, and every work
  identity belongs to the pinned suite and sole region.
- Status reads the exact current future Plan identity rather than inferring it
  by reducing publication history. Its small policy-name projection consumes
  only the two baseline receipts and two named publication receipts. Current
  M4 revision accounting includes the exact Virtual occurrence count and
  rejects an unrecognized command. The report reads each suite case's latest
  occurrence and verifies aggregate epoch counts, never replays old attempts.
- Reopening an unexpired same-owner claim reconstructs its exact command only
  from the retained slot Clock reference and historical Clock receipt. Its
  public claim replay returns the original receipt and complete Plan without a
  new lease or selection; expiry still requires explicit recovery first.
- Component, definition, and entry schemas are closed. A protocol-valid plugin
  result must also decode and match the policy selected by the occurrence Plan;
  every shape or semantic failure is checkpointed once as terminal Failed.
  Rust string bounds and JSON Schema `maxLength` both count Unicode scalar
  values, never UTF-8 bytes.
- The `demo` command is the root README's feature tour. It must execute real
  child processes, verify every phase before printing success, require no
  credentials or network services, and leave its state available for inspection.
- The external process-kill test is Unix-only and must use a replaceable plugin
  to publish an exact barrier after retained progress, a read-only status path,
  logical lease expiry, and terminal identity checks. Do not add timing races or
  kill hooks to semantic production code.
  Its barrier counts actual subject Calls, never Describe or an incidental
  process-invocation ordinal, and forwards the complete original request to the
  strict provider. `ManagedChild` kills and reaps the worker even when a
  pre-kill assertion fails.

## Validation

Run focused validation with:

```sh
cargo test -p cymule-example-durable-evaluation-campaign
./scripts/verify-example.sh
```

README commands must remain copyable from the repository root. Tests should
exercise the built binary, including reopen after process exit, incompatible
evolution admission, resource-integrity failure, recovery without the original
suite file, and invalid plugin output without implicit retry.
