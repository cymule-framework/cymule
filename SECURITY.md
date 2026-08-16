# Security Policy

## Supported versions

Only the latest minor release receives security fixes while the project is below
version 1.0.

## Reporting a vulnerability

Report vulnerabilities privately to the repository maintainers through the
hosting platform's confidential issue mechanism. Do not include credentials,
private customer data, or exploit payloads in public issues.

## Security boundaries

- The Rust core treats workers, plugins, providers, SDKs, and projections as
  untrusted proposal sources.
- A plugin manifest advertises capabilities; it does not grant authority.
- Secrets are represented by opaque handles and are never canonical plan data.
- External dispatch can be at-most-once only when the selected adapter and
  provider contract support it. Ambiguity is represented as `unknown`.
- The embedded profile does not provide process or tenant isolation. Do not run
  untrusted plugins under that profile without an external isolation substrate.

