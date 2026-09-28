defmodule WithSeveralErrorPipesTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use FlowExtra.Pipeline

    defstruct data: nil, error: nil

    pipe(:one)
    pipe(:two)
    pipe(:three)
    error_pipe(:if_error1)
    error_pipe(:if_error2)
    error_pipe(:if_error3)

    def one(_struct, _opts), do: raise("error")
    def two(struct, _opts), do: struct
    def three(struct, _opts), do: struct

    def if_error1(_error, _struct, _opts), do: raise("ignored")
    def if_error2(_error, _struct, _opts), do: raise("ignored")
    def if_error3(error, struct, _opts), do: %{struct | error: error}
  end

  test "returns struct with error" do
    pipeline = Pipeline.start()

    result = Pipeline.call(pipeline, %Pipeline{data: nil})

    assert result.__struct__ == Pipeline
    assert result.error.__struct__ == FlowExtra.PipeError
  end
end
