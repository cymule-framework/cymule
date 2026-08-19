# Durable Evaluation Campaign Example

This directory is a user-facing reference application, not framework core or a
test fixture disguised as a product example.

## Boundaries

- Keep the evaluated subject and scorer behind the process-plugin interface.
  Never move model, Agent Loop, session, or provider behavior into Cymule core.
- Use provider-neutral Cymule contracts in campaign code. SQLite, the local
  filesystem, and the child process are replaceable local adapters selected by
  this example.
- Every claimed case must retain its exact linked Plan ID as the occurrence
  binding. Evolution changes only future claims.
- Treat the subject as observational and safe to retry. A mutating subject must
  use an Effect plugin with idempotency and reconciliation instead.
- Keep suite reads bounded and content-verified. Reject symlinks, duplicate case
  IDs, unknown fields, control characters, oversized files, and changed bytes.
  Parse and store the same captured byte buffer; never reopen the caller path
  between validation and Resource publication.
- Use durable leases and CAS fencing for ownership. Do not add an ambient mutex,
  advisory process lock, or global singleton.

## Validation

Run focused validation with:

```sh
cargo test -p cymule-example-durable-evaluation-campaign
./scripts/verify-example.sh
```

README commands must remain copyable from the repository root. Tests should
exercise the built binary, including reopen after process exit, incompatible
evolution admission, and resource-integrity failure.
