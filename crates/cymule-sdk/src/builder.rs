use std::collections::BTreeMap;

use cymule_core::{
    ComponentContract, Definition, EffectContract, EffectProfile, Expression, Operation,
    PlanCandidate, Region, Step, WaitSpec,
};
use serde_json::Value;

/// Small code-first builder that emits the frozen language-neutral IR.
#[must_use]
pub struct FlowBuilder {
    name: String,
    input_schema: Value,
    output_schema: Value,
    components: Vec<ComponentContract>,
    effects: Vec<EffectContract>,
    definitions: Vec<Definition>,
    steps: Vec<Step>,
}

impl FlowBuilder {
    /// Start a one-definition Flow.
    pub fn new(name: impl Into<String>, input_schema: Value, output_schema: Value) -> Self {
        Self {
            name: name.into(),
            input_schema,
            output_schema,
            components: Vec::new(),
            effects: Vec::new(),
            definitions: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// Declare an abstract component contract.
    pub fn component(
        mut self,
        id: impl Into<String>,
        input_schema: Value,
        output_schema: Value,
        output_artifact_kind: impl Into<String>,
        requirements: BTreeMap<String, String>,
    ) -> Self {
        self.components.push(ComponentContract {
            id: id.into(),
            input_schema,
            output_schema,
            output_artifact_kind: output_artifact_kind.into(),
            requirements,
        });
        self
    }

    /// Declare an abstract effect contract.
    pub fn effect_contract(
        mut self,
        id: impl Into<String>,
        input_schema: Value,
        output_schema: Value,
        profile: EffectProfile,
        requirements: BTreeMap<String, String>,
    ) -> Self {
        self.effects.push(EffectContract {
            id: id.into(),
            input_schema,
            output_schema,
            profile,
            requirements,
        });
        self
    }

    /// Append a component call.
    pub fn call(
        mut self,
        site: impl Into<String>,
        component: impl Into<String>,
        input: Expression,
        bind: impl Into<String>,
    ) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Call {
                component: component.into(),
                input,
                bind: Some(bind.into()),
            },
        });
        self
    }

    /// Add one reusable definition to the same immutable Plan.
    pub fn definition(
        mut self,
        id: impl Into<String>,
        input_schema: Value,
        output_schema: Value,
        body: Region,
    ) -> Self {
        self.definitions.push(Definition {
            id: id.into(),
            input_schema,
            output_schema,
            body,
        });
        self
    }

    /// Append one reusable definition invocation.
    pub fn invoke(
        mut self,
        site: impl Into<String>,
        definition: impl Into<String>,
        input: Expression,
        bind: impl Into<String>,
    ) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Invoke {
                definition: definition.into(),
                input,
                bind: Some(bind.into()),
            },
        });
        self
    }

    /// Append an external effect.
    pub fn effect(
        mut self,
        site: impl Into<String>,
        effect: impl Into<String>,
        input: Expression,
        occurrence: impl Into<String>,
        bind: Option<String>,
    ) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Effect {
                effect: effect.into(),
                input,
                occurrence: occurrence.into(),
                bind,
            },
        });
        self
    }

    /// Append a durable suspension boundary.
    pub fn wait(mut self, site: impl Into<String>, wait: WaitSpec, bind: Option<String>) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Wait { wait, bind },
        });
        self
    }

    /// Append a nested scope built from an already structured Region.
    pub fn scope(mut self, site: impl Into<String>, body: Region, bind: impl Into<String>) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Scope {
                body: Box::new(body),
                bind: Some(bind.into()),
            },
        });
        self
    }

    /// Finish the candidate with one explicit result expression.
    pub fn finish(self, result: Expression) -> PlanCandidate {
        let mut definitions = vec![Definition {
            id: "main".to_owned(),
            input_schema: self.input_schema,
            output_schema: self.output_schema,
            body: Region {
                steps: self.steps,
                result,
            },
        }];
        definitions.extend(self.definitions);
        PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: self.name,
            entry: "main".to_owned(),
            components: self.components,
            effects: self.effects,
            definitions,
            metadata: BTreeMap::new(),
        }
    }
}
