defmodule InterfacePipelineTest do
  use ExUnit.Case, async: true

  test "returns result" do
    pipeline = InterfacePipeline.start()

    result = InterfacePipeline.call(pipeline, %InterfacePipeline{x: 1, y: 2, ok: :ok})

    expected = %InterfacePipeline{
      a: 3,
      b: 2,
      foo: "Hello",
      ok: :ok,
      p: "Hello - 4",
      q: 2,
      x: 4,
      y: 2,
      z: :z
    }

    assert result == expected
  end

  describe "DataAvailable" do
    test "returns result" do
      pipeline = DataAvailable.start()

      result = DataAvailable.call(pipeline, %DataAvailable{top: 100, c1: 1})

      assert result == %DataAvailable{c1: 1, foo: :set_foo, top: 105}
    end
  end
end
