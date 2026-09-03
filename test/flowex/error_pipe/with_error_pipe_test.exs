defmodule WithErrorPipeTest do
  use ExUnit.Case, async: true

  defmodule Pipeline do
    use Flowex.Pipeline

    defstruct data: nil, error: nil
    pipe(:one)
    pipe(:two)
    pipe(:three)
    error_pipe(:if_error)

    def one(_struct, _opts), do: raise(ArithmeticError, "error")
    def two(struct, _opts), do: struct
    def three(struct, _opts), do: struct

    def if_error(error, struct, _opts) do
      %{struct | error: error}
    end
  end

  test "returns struct with error" do
    pipeline = Pipeline.start()

    result = Pipeline.call(pipeline, %Pipeline{data: nil})

    assert result.__struct__ == Pipeline
    assert result.error.__struct__ == Flowex.PipeError
    assert result.error.error == %ArithmeticError{message: "error"}
  end

  describe "checks error" do
    setup do
      pipeline = Pipeline.start()
      result = Pipeline.call(pipeline, %Pipeline{data: nil})

      {:ok, error: result.error}
    end

    test "has message", %{error: error} do
      assert error.message == "error"
    end

    test "has pipe info", %{error: error} do
      assert error.pipe == {WithErrorPipeTest.Pipeline, :one, %{}}
    end

    test "has struct info", %{error: error} do
      assert error.struct == %{data: nil, error: nil}
    end
  end
end
