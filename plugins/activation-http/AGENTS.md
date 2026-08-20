# HTTP Activation Guidance

- HTTP handlers enqueue identified signal observations; only the M1 wait driver
  selects exact parked targets and only `acknowledge` completes the HTTP
  response. Never return success before the activation CAS commits.
- The bounded Tokio channel is backpressure, not durable authority. Producers
  must retry 503 responses with the same activation ID; a running request waits
  for commit acknowledgement.
- Duplicate IDs with identical source/value replay the original acceptance.
  Reuse with different semantics returns conflict and never reaches M1.
- Authorization is an injected header/request policy. Never store credentials
  in deliveries, values, logs, or Cymule state.
- This plugin owns signal ingress only. Typed input completion remains with its
  owning higher-profile controller until a generic M1 input-source seam exists.
- The live-process suite kills after durable ingress, target selection, both M1
  activation-CAS sides, and both acknowledgement sides. An identical request
  must receive no success before acknowledgement and must converge after
  reopen; every SQLite file passes `integrity_check`.
