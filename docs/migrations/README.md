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
| [Filesystem Resource layout generation 2](resource-fs-layout-generation-2.md) | Exact rejection implemented; reset/drain/reseed required for retained internal `/1` roots | `cymule.resource-fs-layout/1` roots replaced by fresh `/2` roots |
| [HTTP activation spool generation 2](activation-http-spool-generation-2.md) | Source hard cut implemented; operator execution pending for retained internal `/1` state | `/1` databases drained and replaced by fresh `/2` authorities; requests requeued only through current ingress |
| [Timer activation store generation 3](activation-timer-store-generation-3.md) | Source hard cut implemented; operator execution pending for retained internal `/1` or `/2` state | predecessor databases drained and replaced by fresh `/3` authorities; timers recreated only through current scheduling |
| [Timer activation store generation 2](activation-timer-store-generation-2.md) | Historical predecessor runbook; its `/1` to `/2` hard cut was not executed by the recorded source change | Historical `/1` databases replaced by then-current `/2` authorities |
