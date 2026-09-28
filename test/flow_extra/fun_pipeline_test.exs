defmodule FunPipelineTest do
  use ExUnit.Case, async: true

  describe ".start" do
    test "checks pipeline struct" do
      pipeline = FunPipeline.start()

      assert %FlowExtra.Pipeline{} = pipeline
      assert pipeline.module == FunPipeline
      assert is_tuple(pipeline.in_name)
      assert is_tuple(pipeline.out_name)
      assert is_tuple(pipeline.sup_name)
    end
  end

  describe ".stop" do
    test "stops supervisor" do
      pipeline = FunPipeline.start()
      sup_pid = GenServer.whereis(pipeline.sup_name)

      # The wrapper's children are the admission owner (a worker) and the
      # line supervisor — every descendant must die with the stop.
      child_pids =
        Supervisor.which_children(sup_pid)
        |> Enum.flat_map(fn
          {_id, :restarting, _type, _modules} ->
            []

          {_id, pid, :worker, _modules} ->
            [pid]

          {_id, pid, :supervisor, _modules} ->
            line_children =
              try do
                Supervisor.which_children(pid) |> Enum.map(fn {_id, p, _t, _m} -> p end)
              rescue
                _ -> []
              end

            [pid | line_children]
        end)

      assert Process.alive?(sup_pid)
      FunPipeline.stop(pipeline)
      refute Process.alive?(sup_pid)
      Enum.each(child_pids, &refute(Process.alive?(&1)))
    end
  end

  describe ".call" do
    test "returns 3 and sets a, b, c" do
      pipeline = FunPipeline.start(%{a: :a, b: :b, c: :c})
      output = FunPipeline.call(pipeline, %FunPipeline{number: 2})

      assert output.number == 3
      assert output.a == :a
      assert output.b == :b
      assert output.c == :c
    end

    test "returns the same results when running several times" do
      pipeline = FunPipeline.start(%{a: :a, b: :b, c: :c})

      numbers =
        for _ <- 1..3 do
          FunPipeline.call(pipeline, %FunPipeline{number: 2}).number
        end

      assert numbers == [3, 3, 3]
    end
  end
end
