# Migration registry

This directory records one-time compatibility and control-plane transitions.
Each runbook states its runtime status; source implementation does not imply
that an external migration has executed.

| Runbook | State | Authority |
| --- | --- | --- |
| [npm trusted-publisher caller generation 1](npm-trusted-publisher-caller-v1.md) | Source implemented; operator execution pending | npm trusted-publisher records for `cymule` and `@cymule/sdk` |
| [GitHub release authority generation 1](github-release-authority-v1.md) | Source implemented; operator execution pending | Release tag App, administration-read control-plane App and short-lived receipt, rulesets, environments, immutable Releases, and BOM attestation projection |
| [Public mirror receipt carrier generation 1](public-mirror-receipt-carrier-v1.md) | Source implemented; operator execution pending | Immutable mirror-created `cymule-mirror/<public-sha>` annotated tags and their creation/update/deletion rulesets |
| [Pre-StateRoot store generations](pre-segmented-store-generations.md) | Exact rejection implemented; migration unsupported | Directory `/5` and SQLite `/6` physical stores |
