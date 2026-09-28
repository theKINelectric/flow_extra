defmodule ModulePipelineTest do
  use ExUnit.Case, async: true

  @opts %{a: :a, b: :b, c: :c}

  describe ".start" do
    test "checks pipeline struct" do
      pipeline = ModulePipeline.start(@opts)

      assert %FlowExtra.Pipeline{} = pipeline
      assert pipeline.module == ModulePipeline
      assert is_tuple(pipeline.in_name)
      assert is_tuple(pipeline.out_name)
      assert is_tuple(pipeline.sup_name)
    end
  end

  describe ".stop" do
    test "stops supervisor" do
      pipeline = ModulePipeline.start(@opts)
      sup_pid = GenServer.whereis(pipeline.sup_name)

      assert Process.alive?(sup_pid)
      ModulePipeline.stop(pipeline)
      refute Process.alive?(sup_pid)
    end
  end

  describe ".call" do
    test "returns 3 and sets a, b, c" do
      pipeline = ModulePipeline.start(@opts)
      output = ModulePipeline.call(pipeline, %ModulePipeline{number: 2})

      assert output.number == 3
      assert output.a == :add_one
      assert output.b == :mult_by_two
      assert output.c == :minus_three
    end

    test "returns the same results when running several times" do
      pipeline = ModulePipeline.start(@opts)

      numbers =
        for _ <- 1..3 do
          ModulePipeline.call(pipeline, %ModulePipeline{number: 2}).number
        end

      assert numbers == [3, 3, 3]
    end
  end

  describe "when error inside stage" do
    test "returns struct with error" do
      pipeline = ModulePipeline.start(@opts)
      output = ModulePipeline.call(pipeline, %ModulePipeline{number: :not_a_number})

      assert output.__struct__ == ModulePipeline
      assert output.number.__struct__ == FlowExtra.PipeError

      error = output.number
      assert error.message == "bad argument in arithmetic expression"
      assert error.pipe == {AddOne, :call, %{a: :add_one, b: :b, c: :c}}
      assert error.struct[:number] == :not_a_number
    end
  end

  describe "several pipelines" do
    test "serves all concurrently" do
      pipeline1 = ModulePipeline.start(@opts)
      pipeline2 = ModulePipeline.start(@opts)
      pipeline3 = FunPipeline.start(@opts)
      pipeline4 = FunPipeline.start(@opts)

      output1 = ModulePipeline.call(pipeline1, %ModulePipeline{number: 2})
      output2 = ModulePipeline.call(pipeline2, %ModulePipeline{number: 2})
      output3 = FunPipeline.call(pipeline3, %FunPipeline{number: 2})
      output4 = FunPipeline.call(pipeline3, %FunPipeline{number: 2})

      assert output1.number == 3
      assert output2.number == 3
      assert output3.number == 3
      assert output4.number == 3
    end
  end
end
