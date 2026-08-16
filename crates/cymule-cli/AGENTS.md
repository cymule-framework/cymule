# CLI Guidance

- The CLI is a transport and operator tool over the library contracts. It does
  not own semantic behavior.
- `rpc` JSON is the cross-language conformance boundary; changes require all SDK
  tests and a command-protocol version decision.
- Write only the response JSON to stdout. Diagnostics go to stderr.
- Never expose unrestricted raw event append.

