# CLI Guidance

- The CLI is a transport and operator tool over the library contracts. It does
  not own semantic behavior.
- `rpc` JSON is the cross-language conformance boundary; changes require all SDK
  tests and a command-protocol version decision.
- `seal_resource` is an additive request/response pair over
  `cymule.resource/1`. The CLI delegates validation and identity to
  `cymule-resource`; it must never compute Resource IDs independently.
- Write only the response JSON to stdout. Diagnostics go to stderr.
- Never expose unrestricted raw event append.
