defmodule FlowExtra.FormatterTest do
  use ExUnit.Case, async: true

  # A project that depends on flowextra formats with `import_deps: [:flowextra]`,
  # which reads flowextra's exported locals_without_parens. Without the export,
  # `mix format` rewrites every pipeline's DSL to `pipe(:decide, count: 14)`.
  @pipeline """
  defmodule P do
    use FlowExtra.Pipeline

    pipe :decide, count: 14
    pipe :check
    error_pipe :fallback
    error_pipe :if_error, count: 2
  end
  """

  test "the DSL keeps its parenthesis-free form in projects that import flowextra" do
    {opts, _} = Code.eval_file(Path.expand("../../../.formatter.exs", __DIR__))
    exported = get_in(opts, [:export, :locals_without_parens]) || []

    formatted =
      IO.iodata_to_binary(Code.format_string!(@pipeline, locals_without_parens: exported))

    assert formatted <> "\n" == @pipeline
  end
end
