module {
  "cymule.flow"() ({
    %input = "cymule.input"() : () -> tensor<1xi8>
    %echoed = "cymule.invoke"(%input) {
      definition = "echo_subflow",
      site = "invoke.echo-subflow"
    } : (tensor<1xi8>) -> tensor<1xi8>
    "cymule.effect"(%echoed) {
      effect = "test.capture",
      occurrence = "primary",
      site = "effect.capture"
    } : (tensor<1xi8>) -> ()
    "cymule.result"(%echoed) : (tensor<1xi8>) -> ()
  }) {sym_name = "cross_language_echo"} : () -> ()
  "cymule.flow"() ({
    %input = "cymule.input"() : () -> tensor<1xi8>
    %echoed = "cymule.call"(%input) {
      component = "test.echo",
      site = "call.echo"
    } : (tensor<1xi8>) -> tensor<1xi8>
    "cymule.result"(%echoed) : (tensor<1xi8>) -> ()
  }) {sym_name = "echo_subflow"} : () -> ()
}
