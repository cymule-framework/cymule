# Process Executor Guidance

- Start one isolated process per typed plugin request. Clear ambient environment
  and pass only explicit configuration; never inherit credentials or PATH.
- Bound request, stdout, stderr, and wall-time observations. Drain stdout and
  stderr concurrently so a child cannot deadlock the host by filling a pipe.
- A timeout or lost process response is ambiguous for an effect. Kill and reap
  the child, return an error to the runtime, and let the existing outbox move
  the intent to `unknown`; never retry dispatch inside this plugin.
- Validate the response through the frozen `cymule.plugin/1` types. Stderr is
  diagnostic only and must not become a result channel.
- No process pool, Agent Loop, shell interpretation, sandbox policy, or network
  authority belongs in this crate. Higher-isolation executors are separate
  plugins.
