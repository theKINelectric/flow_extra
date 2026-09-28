defmodule AsyncFunPipelineTest do
  use ExUnit.Case, async: true

  describe ".run" do
    test "receives result" do
      pipeline = FunPipelineCast.start()

      FunPipelineCast.cast(pipeline, %FunPipelineCast{number: 2, pid: self()})

      assert_receive(3, 100)
    end

    test "returns the same results when running several times" do
      pipeline = FunPipelineCast.start()

      for _ <- 1..3 do
        FunPipelineCast.cast(pipeline, %FunPipelineCast{number: 2, pid: self()})
        assert_receive(3, 100)
      end
    end
  end
end
