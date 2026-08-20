# Test World Guidance

- This crate is workspace-private test support. It must remain `publish = false`
  and must not appear in the public release catalog or facade.
- Keep generated command semantics in one Rust model trace. SDK suites consume a
  minimized language-neutral JSON fixture instead of implementing generators.
- Test clocks, randomness, faults, observations, temporary domains, and child
  lifecycles are explicitly owned values. Never add global mutation, a hidden
  production switch, or a blocking synchronization primitive.
- A fault plan identifies an operation and one-based occurrence before the test
  starts. Tests must reopen public authority after the injected failure and run
  an integrity probe.
- A failing generated trace prints its seed, retained command path, exact replay
  command, and minimized JSON fixture. Promote the fixture with the bug fix.
- Real process tests must use an external barrier and a managed child. Always
  reap the process and let the temporary domain delete itself at teardown.
- Keep this shared crate only while at least three independently routed suites
  consume it. Capability-specific helpers stay with their owning suite.
