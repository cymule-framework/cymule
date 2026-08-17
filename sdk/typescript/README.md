# Cymule TypeScript SDK

This package authors `cymule.ir/1` Plan Candidates and calls a trusted Cymule
Engine. It does not implement canonical sealing or runtime semantics.

```sh
npm install cymule
```

```ts
import { CliEngine, FlowBuilder, ResourceBuilder } from "cymule";
```

Resource Candidates use the same Engine boundary:

```ts
const resource = new CliEngine("./target/debug/cymule").sealResource(
  ResourceBuilder.text("input for another Run"),
);
```

Use `ResourceBuilder.external` for content-addressed/version-pinned objects,
directories, collections, snapshots, and live references. Concrete access stays
behind resolver plugins; Resource Candidates never contain credentials.

`WaitActivationBuilder` creates provider-neutral signal or timer delivery
records. `CliEngine.verifyWaitActivation` validates the closed wire contract;
the durable runtime remains responsible for matching pending waits and admitting
the activation through CAS.

The package is published from GitHub Actions with npm trusted publishing and
provenance. The Rust Engine remains the semantic authority.
