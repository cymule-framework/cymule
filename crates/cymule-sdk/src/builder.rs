use std::collections::BTreeMap;

use cymule_core::{
    ComponentContract, Definition, EffectContract, EffectProfile, Expression, Operation,
    PlanCandidate, Region, ScopeMode, Step, WaitSpec,
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
            steps: Vec::new(),
        }
    }

    /// Declare an abstract component contract.
    pub fn component(
        mut self,
        id: impl Into<String>,
        input_schema: Value,
        output_schema: Value,
    ) -> Self {
        self.components.push(ComponentContract {
            id: id.into(),
            input_schema,
            output_schema,
            requirements: BTreeMap::new(),
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
    ) -> Self {
        self.effects.push(EffectContract {
            id: id.into(),
            input_schema,
            output_schema,
            profile,
            requirements: BTreeMap::new(),
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

    /// Append an external effect.
    pub fn effect(
        mut self,
        site: impl Into<String>,
        effect: impl Into<String>,
        input: Expression,
        occurrence: impl Into<String>,
    ) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Effect {
                effect: effect.into(),
                input,
                occurrence: occurrence.into(),
                bind: None,
            },
        });
        self
    }

    /// Append a durable suspension boundary.
    pub fn wait(mut self, site: impl Into<String>, wait: WaitSpec) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Wait { wait },
        });
        self
    }

    /// Append a nested scope built from an already structured Region.
    pub fn scope(
        mut self,
        site: impl Into<String>,
        mode: ScopeMode,
        body: Region,
        bind: impl Into<String>,
    ) -> Self {
        self.steps.push(Step {
            id: site.into(),
            operation: Operation::Scope {
                mode,
                body: Box::new(body),
                bind: Some(bind.into()),
            },
        });
        self
    }

    /// Finish the candidate with one explicit result expression.
    pub fn finish(self, result: Expression) -> PlanCandidate {
        PlanCandidate {
            ir_version: cymule_core::IR_VERSION.to_owned(),
            name: self.name,
            entry: "main".to_owned(),
            components: self.components,
            effects: self.effects,
            definitions: vec![Definition {
                id: "main".to_owned(),
                input_schema: self.input_schema,
                output_schema: self.output_schema,
                body: Region {
                    steps: self.steps,
                    result,
                },
            }],
            metadata: BTreeMap::new(),
        }
    }
}
