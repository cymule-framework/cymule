# Multi-Tenant Authority Host Profile

Status: proposed. No source implementation, schema, conformance result,
publication, migration or deployment is claimed.

This optional hosting profile exposes the existing topology-neutral Cymule
Engine through authenticated, authorized authority domains. It is required for
a hosted multi-tenant product, but is not part of the M0 semantic kernel or the
current M1 single-domain conformance claim.

The normative architecture decision is
[ADR 0005](../decisions/0005-authority-domains-and-hosts.md). Before this
profile can move to source-implemented, it requires an exact remote envelope,
principal and policy interfaces, route-generation CAS, audit DTOs, quota
contracts, a threat model and the complete adversarial conformance family
listed by that ADR.

The profile deliberately does not define separate local and cloud semantics.
An in-process router, a local daemon and a hosted service must select the same
authority-domain contract. Given the same normalized Engine request, they
produce the same canonical semantic identities and outcomes; transport status,
host evidence and physical revisions may differ.
