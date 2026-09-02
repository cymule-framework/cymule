# ADR 0005: Separate Authority Domains from Hosting

Status: accepted on 2026-09-02 for architecture; the networked host profile is
proposed and has no implementation or conformance claim.

## Decision

Cymule remains a topology-neutral meta-framework. The semantic kernel does not
distinguish local from cloud execution. It addresses an **authority domain**:
one consistency boundary containing its own Store head, idempotency namespace
and receipts. Strong process, storage or infrastructure isolation is a separate
profile claim.

A separate **Authority Host** may expose many such domains to applications. The
host owns authentication, tenant routing, authorization, quotas, rate limits,
credential resolution and transport security. It authenticates a principal and
an explicit tenant context, validates membership or delegated act-as permission,
then resolves `(principal, tenant context, public domain alias, route
generation)` to exactly one internal authority-domain identity before opening
its Store. It authorizes the typed operation against that domain and only then
invokes the existing Engine facade. An `Actor` remains command provenance and
is never accepted as proof of identity or permission.

The host supports two explicit deployment patterns:

- a shared domain with application-owned tenant labels when cross-tenant
  atomicity is intentional. This mode makes no Cymule tenant-isolation claim
  unless the host maintains exact request-independent ownership/entitlement
  records and checks them on every operation; and
- one or more authority domains per tenant when the Host must enforce logical
  authority/data-access isolation, quota and blast-radius boundaries.

Neither pattern changes Plan, Run, Event, Artifact, Effect or command identity.
Tenant ownership MUST NOT be inferred from Actor, Plan metadata, Run-ID
prefixes or another caller-controlled label.
Mutable authority such as a Run uses one generation-bearing owner plus explicit
grants. Immutable content-addressed Plans and Artifacts use a multi-principal
entitlement key `(domain, kind, content identity, tenant/security-domain)`;
knowledge of a digest grants no read permission. For object creation in a
shared domain, the applicable owner or entitlement uses a generation-bearing
reservation keyed by exact request/command identity. The Host reserves before
dispatch, materializes it only from the matching success receipt, keeps an
unknown result Reserved for reconciliation by that tenant alone, and releases
only after authoritative NotApplied evidence. Released tombstones advance
generation to prevent ABA. A future implementation may instead couple these
records into the same domain Store CAS, but cannot claim that atomicity while
calling an unchanged Engine facade.
Local embedding and a remote service are realizations of the same domain
contract. A remote protocol, if implemented, must register its own exact
version; it must not add authentication fields to `cymule.engine/5` or make
network topology canonical. No protocol version is reserved by this ADR.

## Required host properties

The proposed host must fail closed when a route is absent, ambiguous, stale or
unauthorized. A request may name a public domain alias, but only host policy may
resolve it to a physical Store target. Clients must never supply provider,
location or credentials across the remote trust boundary.

Every exchange binds the authenticated principal, tenant context, selected
authority domain, route generation, host authorization-policy generation, a
host-keyed request commitment and correlation ID in append-only audit evidence.
The commitment uses an audit-key generation and an allowlisted canonical input;
it is never a bare digest of secret or low-entropy payload data. Key rotation,
verification retention and destruction belong to the host threat model. Audit
admission is fail-closed before dispatch. Later records
distinguish Attempted, Authorized, Dispatched, Completed and Unknown; failure
to retain a required transition after mutation may have begun is
`unknown_world_outcome/reconcile`; it must not be reported as semantic success,
confirmed failure or a safe retry. Those records are host evidence, not Machine
events or Plan identity. Authorization is re-evaluated for every command; a
coordination lease, Actor string, Run ownership or previously successful call
does not grant capability.

Authorization returns an operation-admission ticket bound to the principal,
tenant, internal domain, host-keyed request commitment, route generation,
policy generation, operation class and a bounded exact set of current
owner/entitlement generations. The ticket also reserves quota under at least
`(tenant, domain, operation class)` while a domain-wide cap may apply above it.
A Host state CAS revalidates every generation and quota reservation while
moving the ticket from Prepared to Admitted; this is the final pre-dispatch
linearization point, and no revocation or route mutation may interpose between
that CAS and dispatch under the ticket.
Revocation or route change before that point fails closed; after it, the
admitted call may be in flight and its result
must be resolved through its receipt/outcome contract rather than described as
retroactively cancelled.

Per-tenant/domain bounds cover concurrent calls, retained bytes and operation
class; per-domain totals provide an additional cap rather than the only noisy-
neighbour control.
Admission occurs before provider I/O. Cross-domain operations are not atomic;
they require an explicit protocol with independently retryable legs and
receipts. Domain migration requires a freeze/copy/verify/route-CAS/unfreeze
runbook and never silently reinterprets a live Store location.

## Conformance before implementation can be claimed

- identity confusion tests prove that principal, Actor, tenant and domain are
  four distinct values;
- horizontal and vertical authorization tests cover every closed Engine
  operation, including query operations;
- route-swap, stale-policy, lost-response and concurrent-revocation tests prove
  the documented admission linearization point and prevent cross-domain Store
  access before it;
- noisy-neighbour tests prove independent quotas and bounded work;
- audit records redact credentials and payload secrets while retaining enough
  evidence to correlate the exact accepted request;
- the same normalized Engine request produces identical canonical semantic
  identities and outcomes through an in-process host and a remote host;
  transport status, host evidence and physical revisions need not match.

## Rationale and precedent

This follows mature separation between namespace isolation and deployment
topology. Temporal treats a Namespace as an isolation boundary and documents
separate multi-tenant namespace patterns; Kubernetes likewise separates
namespace-based soft multi-tenancy from stronger virtual-control-plane or
cluster isolation. The design uses those patterns without importing either
product's API into Cymule semantics.

References:

- <https://docs.temporal.io/namespaces>
- <https://docs.temporal.io/best-practices/multi-tenant-patterns>
- <https://kubernetes.io/docs/concepts/security/multi-tenancy/>

## Consequences

- Application-layer multi-tenancy is a P0 requirement for the Authority Host
  profile, not a dependency of M0 or the current single-domain M1 profile.
- The same framework can run embedded, in a local daemon or behind a hosted
  service without changing semantic identities.
- Strong tenant isolation remains a separate claim from logical routing; a
  shared process and Store adapter cannot imply sandbox or infrastructure
  isolation.
