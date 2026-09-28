defmodule ModulePipelineSyncTest do
  use ExUnit.Case, async: true

  @opts %{a: :a, b: :b, c: :c}

  describe ".start" do
    test "checks pipeline struct" do
      pipeline = ModulePipelineSync.start(@opts)

      assert %FlowExtra.Pipeline{} = pipeline
      assert pipeline.module == ModulePipelineSync
      assert is_tuple(pipeline.in_name)
      assert is_tuple(pipeline.out_name)
      assert is_tuple(pipeline.sup_name)
    end
  end

  describe ".stop" do
    test "stops supervisor" do
      pipeline = ModulePipelineSync.start(@opts)
      sup_pid = GenServer.whereis(pipeline.sup_name)

      assert Process.alive?(sup_pid)
      ModulePipelineSync.stop(pipeline)
      refute Process.alive?(sup_pid)
    end
  end

  describe ".call" do
    test "returns 3 and sets a, b, c" do
      pipeline = ModulePipelineSync.start(@opts)

      output = ModulePipelineSync.call(pipeline, %ModulePipelineSync{number: 2})

      assert output.number == 3
      assert output.a == :add_one
      assert output.b == :mult_by_two
      assert output.c == :minus_three
    end
  end

  describe "when error inside stage" do
    test "returns struct with error" do
      pipeline = ModulePipelineSync.start(@opts)

      output = ModulePipelineSync.call(pipeline, %ModulePipelineSync{number: :not_a_number})

      assert output.__struct__ == ModulePipelineSync
      assert output.number.__struct__ == FlowExtra.PipeError

      error = output.number
      assert error.message == "bad argument in arithmetic expression"
      assert error.pipe == {AddOne, :call, %{a: :add_one, b: :b, c: :c, o1: 1}}
      assert error.struct[:number] == :not_a_number
    end
  end

  describe "several pipelines" do
    test "serves all concurrently" do
      pipeline1 = ModulePipelineSync.start(@opts)
      pipeline2 = ModulePipelineSync.start(@opts)
      pipeline3 = FunPipelineSync.start(@opts)
      pipeline4 = FunPipelineSync.start(@opts)

      output1 = ModulePipelineSync.call(pipeline1, %ModulePipelineSync{number: 2})
      output2 = ModulePipelineSync.call(pipeline2, %ModulePipelineSync{number: 2})
      output3 = FunPipelineSync.call(pipeline3, %FunPipelineSync{number: 2})
      output4 = FunPipelineSync.call(pipeline3, %FunPipelineSync{number: 2})

      assert output1.number == 3
      assert output2.number == 3
      assert output3.number == 3
      assert output4.number == 3
    end
  end
end
