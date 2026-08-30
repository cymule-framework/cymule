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
| [Durable StateRoot generation 5](durable-state-root-generation-5.md) | Source implemented; operator execution pending; drain/export/recreate/requeue required | StateRoot/value `/4` stores and Agent `/7` records replaced by fresh StateRoot/value `/5` and Agent `/8` authorities |
| [Filesystem Resource layout generation 2](resource-fs-layout-generation-2.md) | Exact rejection implemented; reset/drain/reseed required for retained internal `/1` roots | `cymule.resource-fs-layout/1` roots replaced by fresh `/2` roots |
| [Timer activation store generation 2](activation-timer-store-generation-2.md) | Exact rejection implemented; migration unsupported; drain/reset/reseed required for retained internal `/1` state | `cymule.activation-timer-store/1` databases replaced by fresh `/2` authorities |
