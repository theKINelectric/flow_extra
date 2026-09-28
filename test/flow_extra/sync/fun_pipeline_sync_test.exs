defmodule FunPipelineSyncTest do
  use ExUnit.Case, async: true

  describe ".start" do
    test "checks pipeline struct" do
      pipeline = FunPipelineSync.start(%{start_options: :start_options})

      assert %FlowExtra.Pipeline{} = pipeline
      assert pipeline.module == FunPipelineSync
      assert is_tuple(pipeline.in_name)
      assert is_tuple(pipeline.out_name)
      assert is_tuple(pipeline.sup_name)
    end
  end

  describe ".stop" do
    test "stops supervisor" do
      pipeline = FunPipelineSync.start()
      sup_pid = GenServer.whereis(pipeline.sup_name)

      pipe_pids =
        Supervisor.which_children(sup_pid) |> Enum.map(fn {_id, pid, :worker, [_]} -> pid end)

      assert Process.alive?(sup_pid)
      FunPipelineSync.stop(pipeline)
      refute Process.alive?(sup_pid)
      Enum.each(pipe_pids, &refute(Process.alive?(&1)))
    end
  end

  describe ".call" do
    test "returns 3 and sets a, b, c" do
      pipeline = FunPipelineSync.start(%{a: :a, b: :b, c: :c})

      output = FunPipelineSync.call(pipeline, %FunPipelineSync{number: 2})

      assert output.number == 3
      assert output.a == {:a, 1}
      assert output.b == {:b, 2}
      assert output.c == {:c, 3}
    end
  end
end
