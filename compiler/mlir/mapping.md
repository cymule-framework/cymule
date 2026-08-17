# Experimental Operation Mapping

| MLIR operation | Frozen IR target |
| --- | --- |
| `cymule.flow` | one `Definition` and its `Region` |
| `cymule.input` | `Expression::Input` |
| `cymule.call` | `Operation::Call` |
| `cymule.invoke` | `Operation::Invoke` |
| `cymule.effect` | `Operation::Effect` |
| `cymule.result` | `Region.result` |

Attributes such as `site`, `component`, `definition`, `effect`, and `occurrence`
carry stable semantic identities. MLIR SSA names and block labels are
compiler-local and do not enter canonical plan identity.
