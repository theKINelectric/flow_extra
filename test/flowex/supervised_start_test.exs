defmodule SupervisedStartTest do
  use ExUnit.Case, async: true

  @opts %{a: :a, b: :b, c: :c}

  setup do
    {:ok, supervisor_pid} = Supervisor.start_link([], strategy: :one_for_one)

    {:ok, supervisor_pid: supervisor_pid}
  end

  test "checks pipeline1", %{supervisor_pid: supervisor_pid} do
    pipeline1 = FunPipeline.supervised_start(supervisor_pid, @opts)

    output = FunPipeline.call(pipeline1, %FunPipeline{number: 2})
    assert output.number == 3
  end

  test "checks pipeline2", %{supervisor_pid: supervisor_pid} do
    pipeline2 = FunPipeline.supervised_start(supervisor_pid, @opts)

    output = FunPipeline.call(pipeline2, %FunPipeline{number: 2})
    assert output.number == 3
  end

  test "check supervisors", %{supervisor_pid: supervisor_pid} do
    pipeline1 = FunPipeline.supervised_start(supervisor_pid, @opts)
    pipeline2 = FunPipeline.supervised_start(supervisor_pid, @opts)

    pipeline_sup_pids = [
      GenServer.whereis(pipeline1.sup_name),
      GenServer.whereis(pipeline2.sup_name)
    ]

    Supervisor.which_children(supervisor_pid)
    |> Enum.each(fn {id, pid, type, [module]} ->
      # T5: child ids are via-tuple names now ({:via, Registry, {key}}),
      # with key {pipeline_module, ref, role}.
      assert is_tuple(id)
      assert elem(id, 0) == :via
      assert elem(elem(elem(id, 2), 1), 2) == :supervisor
      assert pid in pipeline_sup_pids
      assert type == :supervisor
      assert module == Flowex.Supervisor
    end)
  end

  test "sync pipelines", %{supervisor_pid: supervisor_pid} do
    pipeline = FunPipelineSync.supervised_start(supervisor_pid, @opts)

    output = FunPipelineSync.call(pipeline, %FunPipelineSync{number: 2})
    assert output.number == 3
  end
end
