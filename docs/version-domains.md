# Version Domains

Status: generated from `versioning/version-domains.json`; do not edit by hand.

| Exact version | Kind | Owner | Compatibility | Schema | Conformance |
| --- | --- | --- | --- | --- | --- |
| `cymule.activation-http-spool/1` | persistence | `cymule-activation-http` | exact-reject | — | `rust-activation-http` |
| `cymule.activation-timer-store/2` | persistence | `cymule-activation-timer` | exact-reject | — | `rust-activation-timer` |
| `cymule.agent-command-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-command-receipt-id/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-command-receipt/4` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-command/3` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-elicitation-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-host-binding/1` | binding | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin` |
| `cymule.agent-input-completion-key/1` | persistence | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.agent-input-completion-receipt/1` | receipt | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.agent-input-suspension-key/1` | persistence | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.agent-input-suspension-receipt/1` | receipt | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.agent-message-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-message-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-message-order-head/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-occurrence-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-occurrence-transition-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin` |
| `cymule.agent-open-stream-generation/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-recovery-observation/1` | persistence | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin`, `rust-profile-protocol` |
| `cymule.agent-session-current/2` | persistence | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-profile-protocol` |
| `cymule.agent-stream-chunk-head/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-stream-chunk-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-stream-final-update-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin` |
| `cymule.agent-stream-finalization-coupling-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin` |
| `cymule.agent-stream-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-stream-publication-intent/2` | persistence | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin`, `rust-profile-protocol` |
| `cymule.agent-stream-publication-reservation/2` | persistence | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin`, `rust-profile-protocol` |
| `cymule.agent-stream-publication/1` | binding | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin` |
| `cymule.agent-target-claim-current/1` | persistence | `cymule-profile-protocol` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin`, `rust-durable`, `rust-profile-protocol` |
| `cymule.agent-target-claim-id/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin`, `rust-durable`, `rust-profile-protocol` |
| `cymule.agent-target-claim-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin`, `rust-durable`, `rust-profile-protocol` |
| `cymule.agent-tool-derived-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin`, `rust-profile-protocol` |
| `cymule.agent-tool-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-unresolved-occurrence-generation/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-update-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-update-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.agent-workspace-claim-owner-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-agent-plugin` |
| `cymule.agent/8` | semantic | `cymule-agent` | exact-reject | `plugins/agent-interaction/schemas/agent-protocol.schema.json` | `protocol`, `rust-agent-plugin` |
| `cymule.application-journal-prefix-replacement-authority/2` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.application-journal-prefix-replacement-receipt/2` | receipt | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable`, `rust-virtual` |
| `cymule.application-journal-prefix/1` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-agent-plugin`, `rust-durable`, `rust-virtual` |
| `cymule.archived-command-proof/1` | receipt | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.artifact-type-contract/1` | semantic | `cymule-resource` | exact-reject | `schemas/artifact-type-contract.schema.json` | `protocol`, `rust-resource` |
| `cymule.artifact/2` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json`, `schemas/engine-protocol.schema.json` | `protocol`, `rust-core` |
| `cymule.authenticated-collection-preimage/1` | semantic | `cymule-authenticated-collections` | exact-reject | — | `rust-authenticated-collections` |
| `cymule.authenticated-log-empty/1` | semantic | `cymule-authenticated-collections` | exact-reject | — | `rust-authenticated-collections` |
| `cymule.authenticated-log-mutation/1` | semantic | `cymule-authenticated-collections` | exact-reject | — | `rust-authenticated-collections` |
| `cymule.authenticated-log-node/1` | persistence | `cymule-authenticated-collections` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-authenticated-collections` |
| `cymule.authenticated-map-key/1` | persistence | `cymule-authenticated-collections` | exact-reject | — | `rust-authenticated-collections` |
| `cymule.authenticated-map-mutation/1` | semantic | `cymule-authenticated-collections` | exact-reject | — | `rust-authenticated-collections` |
| `cymule.authenticated-map-node/1` | persistence | `cymule-authenticated-collections` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-authenticated-collections` |
| `cymule.binding-context/1` | binding | `cymule-runtime` | exact-reject | — | `protocol` |
| `cymule.canary/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.cancellation-reason/1` | semantic | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.clock-observation/2` | receipt | `cymule-durable-protocol` | exact-reject | `schemas/durable-control.schema.json`, `schemas/durable-storage.schema.json`, `schemas/engine-protocol.schema.json` | `protocol`, `rust-clock-system`, `rust-durable` |
| `cymule.clock-system/2` | persistence | `cymule-clock-system` | exact-reject | — | `protocol`, `rust-clock-system` |
| `cymule.command-admission/3` | receipt | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.command-archive-batches/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-archive-leaf/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-archive-node/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-archive-segment-leaf/2` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-archive-segment/4` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-batch-receipt/1` | receipt | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-batch/1` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-index-empty-leaf/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-index-key/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-index-leaf/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-index-node/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable`, `rust-store-plugins` |
| `cymule.command-index-proof/2` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core`, `rust-durable` |
| `cymule.command/6` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.component-input/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.component-occurrence/4` | persistence | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable`, `rust-resource` |
| `cymule.component-output/1` | semantic | `cymule-core` | exact-reject | — | `protocol`, `rust-core`, `rust-durable` |
| `cymule.continuation-attempt/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.continuation-execution-claim/1` | binding | `cymule-durable-protocol` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.continuation-state/1` | persistence | `cymule-durable-protocol` | exact-reject | `schemas/engine-protocol.schema.json`, `schemas/evolution-control.schema.json` | `protocol`, `rust-durable-protocol` |
| `cymule.continuation/1` | semantic | `cymule-durable-protocol` | exact-reject | — | `rust-durable` |
| `cymule.coupled-checkpoint-key/1` | semantic | `cymule-durable` | exact-reject | — | `rust-agent-plugin`, `rust-durable` |
| `cymule.coupled-checkpoint-receipt/3` | receipt | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-agent-plugin`, `rust-durable` |
| `cymule.crates-package-report/1` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.crates-publish-report/1` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.crates-release-stage/3` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.declared-failure/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.definition-contract/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.directory-command-batch-index/1` | persistence | `cymule-directory-store` | exact-reject | — | `rust-directory-plugin` |
| `cymule.directory-store/5` | binding | `cymule-directory-store` | exact-reject | — | `protocol`, `rust-directory-plugin` |
| `cymule.durable-command-id/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.durable-control/4` | wire | `cymule-durable` | exact-reject | `schemas/durable-control.schema.json` | `protocol`, `rust-durable` |
| `cymule.durable-gc-receipt/2` | receipt | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-head/2` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-physical-token/2` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-revision/3` | persistence | `cymule-durable` | exact-reject | — | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-state-root/5` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-state-value/5` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.durable-state/7` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.effect-args/1` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.effect-intent/2` | semantic | `cymule-core` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-agent-plugin`, `rust-core`, `rust-durable` |
| `cymule.effect-obligation/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.effect-provider-attempt/1` | receipt | `cymule-runtime` | exact-reject | `schemas/plugin-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.effect-resolution-receipt/1` | receipt | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.effect-result/1` | semantic | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.effect-schema/1` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-agent-plugin`, `rust-core`, `rust-durable` |
| `cymule.embedded-attempt/1` | semantic | `cymule-runtime` | exact-reject | — | `protocol` |
| `cymule.embedded-continuation/1` | semantic | `cymule-runtime` | exact-reject | — | `protocol` |
| `cymule.engine/5` | wire | `cymule-runtime` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol` |
| `cymule.ephemeral-agent-revision/1` | semantic | `cymule-agent` | exact-reject | — | `rust-agent-plugin` |
| `cymule.event/8` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.evolution-command-alias-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-control/5` | wire | `cymule-profile-protocol` | exact-reject | `schemas/evolution-control.schema.json` | `protocol`, `rust-evolution`, `sdk-rust` |
| `cymule.evolution-current-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-current/2` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.evolution-evidence-root/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-link-record/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-mutation-set/2` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-mutation-value/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-persistence-command/4` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-profile-protocol`, `sdk-rust` |
| `cymule.evolution-persistence-receipt/4` | receipt | `cymule-profile-protocol` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-profile-protocol`, `sdk-rust` |
| `cymule.evolution-plugin/3` | wire | `cymule-runtime` | exact-reject | `schemas/evolution-plugin.schema.json` | `protocol` |
| `cymule.evolution-receipt-key/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-state-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.evolution-state-leaf/3` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.execution-binding/2` | binding | `cymule-core` | exact-reject | `schemas/execution-binding.schema.json` | `protocol` |
| `cymule.execution-clock-scope/1` | semantic | `cymule-durable-protocol` | exact-reject | — | `rust-durable` |
| `cymule.executor-process/1` | binding | `cymule-runtime` | exact-reject | — | `protocol`, `rust-executor-plugin` |
| `cymule.framework-resource-handle/4` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-resource` |
| `cymule.framework-resource-handoff/5` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.framework-resource-list-proof/5` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.framework-resource-manifest/3` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.github-release-control-plane-receipt/2` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.github-release-settings-snapshot/2` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.history-compaction/2` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.in-memory-parked-wait-view/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.input/1` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core`, `rust-durable` |
| `cymule.invocation-input/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.invocation-result/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.invocation/2` | semantic | `cymule-core` | exact-reject | — | `protocol`, `rust-core`, `rust-durable` |
| `cymule.ir/3` | semantic | `cymule-core` | exact-reject | `schemas/plan-candidate.schema.json` | `protocol`, `rust-core` |
| `cymule.jcs/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.jitter-evidence/1` | receipt | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.linked-definition/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.live-evolution-control/6` | wire | `cymule-profile-protocol` | exact-reject | `schemas/live-evolution-control.schema.json` | `protocol`, `rust-evolution`, `sdk-rust` |
| `cymule.live-initial-decision/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.live-update-decision/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.machine-artifact-admission-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-authority-frontier/3` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.machine-authority-root/2` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.machine-base-anchor/2` | binding | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core`, `rust-durable` |
| `cymule.machine-base/4` | persistence | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-command-batch-admission-lineage/1` | semantic | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.machine-delta/6` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.machine-index-membership-value/1` | persistence | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-material-admission/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-order-entry-value/1` | persistence | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-paged-action/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-paged-processed-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-paged-transition/1` | semantic | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.machine-plan-admission-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-prefix/4` | persistence | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-root-delta/3` | projection | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.machine-root-parts/3` | projection | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.machine-run-binding-lineage/1` | binding | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-run-current/2` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.machine-run-plan-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-scope-current/1` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.machine-scope-effect-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-scope-mutating-effect-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.machine-snapshot/11` | persistence | `cymule-core` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-core` |
| `cymule.migration-frame-replacement/1` | receipt | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.migration-safe-point/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.npm-release-caller/1` | wire | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.npm-release-stage/3` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.occurrence-binding/1` | binding | `cymule-runtime` | exact-reject | — | `protocol` |
| `cymule.operation-attempt/2` | persistence | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.pending-wait-source/1` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-directory-plugin`, `rust-durable`, `rust-store-plugins` |
| `cymule.pinned-machine-sidecar-transition/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.pinned-machine-state-root-stage/1` | persistence | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.pinned-root-mutation/1` | persistence | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.plan-edge/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.plan/1` | semantic | `cymule-core` | exact-reject | `schemas/sealed-plan.schema.json` | `protocol`, `rust-core` |
| `cymule.plugin/3` | wire | `cymule-runtime` | exact-reject | `schemas/plugin-protocol.schema.json` | `protocol` |
| `cymule.prepared-paged-begin/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.prepared-paged-finalize/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.prepared-paged-step/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.prepared-pinned-read-command/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.prepared-pinned-run-lookup/1` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.process-execution-binding/2` | binding | `cymule-executor-process` | exact-reject | — | `rust-executor-plugin` |
| `cymule.process-working-directory/2` | semantic | `cymule-executor-process` | exact-reject | — | `rust-executor-plugin` |
| `cymule.projection-root-event/1` | projection | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.projection-root-genesis/1` | projection | `cymule-core` | exact-reject | — | `rust-core`, `rust-durable` |
| `cymule.public-mirror-receipt/2` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.public-source-snapshot/1` | receipt | `release-governance` | exact-reject | — | `protocol` |
| `cymule.release-bom/3` | receipt | `release-governance` | exact-reject | `schemas/release-bom.schema.json` | `protocol`, `release-workflows` |
| `cymule.release-finalization-stage/3` | receipt | `release-governance` | exact-reject | — | `release-workflows` |
| `cymule.relink-compatibility/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.resource-agent-stream-pin/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-archive-pin/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-resource` |
| `cymule.resource-archive-release/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-catalog-record/2` | projection | `cymule-profile-protocol` | exact-reject | — | `rust-resource` |
| `cymule.resource-cleanup-plan/1` | semantic | `cymule-resource` | exact-reject | — | `protocol`, `rust-resource`, `rust-resource-plugins` |
| `cymule.resource-cleanup-receipt/2` | receipt | `cymule-resource` | exact-reject | — | `protocol`, `rust-resource`, `rust-resource-plugins` |
| `cymule.resource-command-receipt/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-command/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-delete-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-delete-intent/3` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `rust-resource` |
| `cymule.resource-delete-receipt/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `rust-resource` |
| `cymule.resource-deletion-target/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-fs-child-write/1` | semantic | `cymule-resource-fs` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-fs-layout/2` | persistence | `cymule-resource-fs` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-fs-manifest-index/3` | persistence | `cymule-resource-fs` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-fs-upload/9` | persistence | `cymule-resource-fs` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-fs/6` | binding | `cymule-resource-fs` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-gc-receipt/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `rust-resource` |
| `cymule.resource-handoff-activation-index/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-handoff-activation/3` | wire | `cymule-profile-protocol` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-profile-protocol`, `rust-resource` |
| `cymule.resource-handoff-index/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-handoff/5` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-profile-protocol`, `rust-resource` |
| `cymule.resource-lifecycle-receipt-ref/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-list-cursor/3` | wire | `cymule-resource` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-resource`, `rust-resource-plugins` |
| `cymule.resource-list-progress/3` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.resource-list-proof/5` | semantic | `cymule-resource` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-resource` |
| `cymule.resource-locators/2` | binding | `cymule-profile-protocol` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-resource`, `sdk-rust` |
| `cymule.resource-manifest-cursor/2` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.resource-manifest-empty/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-resource` |
| `cymule.resource-manifest-leaf/2` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.resource-manifest-node/2` | semantic | `cymule-resource` | exact-reject | — | `rust-resource` |
| `cymule.resource-manifest/3` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-resource`, `sdk-rust` |
| `cymule.resource-object-store-content/1` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-inventory/1` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-layout/2` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-upload-gc/2` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-upload-generation/1` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-upload-node/1` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store-upload/7` | persistence | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-object-store/5` | binding | `cymule-resource-object-store` | exact-reject | — | `rust-resource-plugins` |
| `cymule.resource-pin-current/2` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-pin-receipt/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `rust-resource`, `sdk-rust` |
| `cymule.resource-profile-pin/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-release-receipt/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `rust-resource`, `sdk-rust` |
| `cymule.resource-retention-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.resource-retention-family/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.resource-retention-key/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-resource` |
| `cymule.resource-retention-subject/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.resource/4` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/resource.schema.json` | `protocol`, `rust-resource`, `sdk-rust` |
| `cymule.result/1` | semantic | `cymule-runtime` | exact-reject | — | `protocol`, `rust-durable` |
| `cymule.retry-clock-scope/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.retry-decision/2` | receipt | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.retry-policy/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.retry-stream/2` | persistence | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.rollout-evaluation/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.rollout-transition/2` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-evolution` |
| `cymule.run-cancellation-receipt/1` | receipt | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-durable` |
| `cymule.run-query-indexes/3` | persistence | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.run-quiescence/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-durable`, `rust-evolution` |
| `cymule.runtime-composition/1` | projection | `cymule-runtime` | exact-reject | — | `protocol` |
| `cymule.scope-obligation-lineage/1` | projection | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.scope-result/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.scope/2` | semantic | `cymule-core` | exact-reject | — | `protocol`, `rust-core`, `rust-durable` |
| `cymule.semantic/6` | semantic | `cymule-core` | exact-reject | — | `rust-core` |
| `cymule.shadow-subject/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.snapshot/2` | semantic | `cymule-durable` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable` |
| `cymule.sqlite-store/6` | persistence | `cymule-store-sqlite` | exact-reject | — | `protocol`, `rust-store-plugins` |
| `cymule.subflow-revision/2` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/engine-protocol.schema.json` | `protocol`, `rust-evolution`, `sdk-rust` |
| `cymule.transport-request/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.version-domain-registry/3` | persistence | `release-governance` | exact-reject | `schemas/version-domain-registry.schema.json` | `protocol` |
| `cymule.virtual-activation-control/1` | wire | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-active-region-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-archive-command-index-empty-leaf/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-archive-command-index-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-archive-command-index-leaf/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-archive-command-index-node/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-archive-command-index-node/2` | persistence | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-command-index-proof/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-archive-command-leaf/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-command-node/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-command-proof/2` | semantic | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-manifest/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-archive-occurrence-leaf/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-occurrence-node/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-occurrence-proof/2` | semantic | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-publication/2` | binding | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-retirement-control/1` | wire | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-archive-work-empty-leaf/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-work-index-node/2` | binding | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-work-key/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-work-leaf/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-work-node/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-archive-work-proof/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-archive-write/2` | semantic | `cymule-virtual` | exact-reject | — | `rust-virtual` |
| `cymule.virtual-certificate-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-claim-control/4` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-compaction-certificate/4` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-compaction-control/1` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-profile-protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-current-body/2` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-current-storage-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-current/3` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-evolution-selection/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-initialization-control/2` | wire | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-lease-renewal-control/2` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-materialization-control/2` | wire | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-migration-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-mutation-set/2` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-occurrence-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-parked-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-parked-index-page/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-persistence-command/2` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-persistence-receipt/3` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-receipt-storage-key/1` | receipt | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-recovery-control/2` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-region-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-region-migration-control/3` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-region-migration/3` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-rehydration-control/1` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-run-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-run-weight-control/1` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-scheduler-journal-id/1` | semantic | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-state-root/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-state-storage-key/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol` |
| `cymule.virtual-work-control/2` | wire | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.virtual-work-current/1` | persistence | `cymule-profile-protocol` | exact-reject | — | `rust-profile-protocol`, `sdk-rust` |
| `cymule.virtual-work-occurrence/3` | semantic | `cymule-profile-protocol` | exact-reject | `schemas/virtual-control.schema.json` | `protocol`, `rust-virtual`, `sdk-rust` |
| `cymule.wait-activation-material/1` | semantic | `cymule-durable` | exact-reject | — | `rust-durable` |
| `cymule.wait-activation-receipt/3` | receipt | `cymule-durable-protocol` | exact-reject | `schemas/durable-storage.schema.json` | `protocol`, `rust-durable`, `rust-virtual` |
| `cymule.wait-activation/2` | wire | `cymule-durable-protocol` | exact-reject | `schemas/wait-activation.schema.json` | `protocol`, `rust-durable` |
| `cymule.wait-result/1` | semantic | `cymule-durable-protocol` | exact-reject | — | `rust-durable` |
| `cymule.wait/2` | semantic | `cymule-durable` | exact-reject | `schemas/engine-protocol.schema.json`, `schemas/wait-condition.schema.json` | `protocol`, `rust-durable` |

The registry also freezes writers, accepted readers, embedding and dependency edges, source anchors, canonical schema digests, migration status, removal gates, and release-generation ownership.
