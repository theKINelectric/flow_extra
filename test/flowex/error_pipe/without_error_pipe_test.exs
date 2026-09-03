defmodule WithoutErrorPipeTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use Flowex.Pipeline

    defstruct data: nil
    pipe(:one)
    pipe(:two)
    pipe(:three)

    def one(_struct, _opts), do: raise("error")
    def two(struct, _opts), do: struct
    def three(struct, _opts), do: struct
  end

  test "raises exception" do
    pipeline = Pipeline.start()

    assert_raise Flowex.PipelineError, fn ->
      Pipeline.call(pipeline, %Pipeline{data: nil})
    end
  end
end
