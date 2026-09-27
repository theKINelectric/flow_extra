defmodule Flowex.Names.StopOwnershipTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-003 trap (pursuit A2, "who owns the off switch"): stop_pipeline called
  Supervisor.stop on the pipeline supervisor, but a supervised pipeline's
  spec in the owning parent says restart: :permanent — and the permanent
  contract restarts a child even after normal termination. The deliberate
  stop resurrected the pipeline under the same registered name (audit probe:
  old supervisor pid replaced, a new one alive). Intentional removal must go
  through the owner: terminate the child, then delete its spec. Abnormal
  death keeps the restart policy.
  """

  # FunPipeline's pipe heads pattern-match their opts keys (%{a: a} needs :a),
  # so calls must carry a/b/c — the same shape SupervisedStartTest passes.
  @opts %{a: :a, b: :b, c: :c}

  test "stopping a supervised pipeline keeps it stopped and the sibling usable" do
    {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)
    first = FunPipeline.supervised_start(parent, @opts)
    second = FunPipeline.supervised_start(parent, @opts)

    FunPipeline.stop(first)

    # Barrier, not a stopwatch: wait for the registry slot to clear, then
    # prove nothing brings the name back.
    wait_until(fn -> GenServer.whereis(first.sup_name) == nil end)
    Process.sleep(100)
    assert GenServer.whereis(first.sup_name) == nil

    assert Enum.count(Supervisor.which_children(parent)) == 1
    assert %FunPipeline{number: 3} = FunPipeline.call(second, %FunPipeline{number: 2})

    Supervisor.stop(parent)
  end

  test "the same law on the sync engine" do
    {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)
    first = FunPipelineSync.supervised_start(parent, @opts)
    second = FunPipelineSync.supervised_start(parent, @opts)

    FunPipelineSync.stop(first)

    wait_until(fn -> GenServer.whereis(first.sup_name) == nil end)
    Process.sleep(100)
    assert GenServer.whereis(first.sup_name) == nil

    assert Enum.count(Supervisor.which_children(parent)) == 1

    assert %FunPipelineSync{number: 3} =
             FunPipelineSync.call(second, %FunPipelineSync{number: 2})

    Supervisor.stop(parent)
  end

  test "abnormal death still restarts under the owner" do
    {:ok, parent} = Supervisor.start_link([], strategy: :one_for_one)
    pipeline = FunPipeline.supervised_start(parent, @opts)
    old_pid = GenServer.whereis(pipeline.sup_name)

    Process.exit(old_pid, :kill)

    wait_until(fn ->
      new_pid = GenServer.whereis(pipeline.sup_name)
      is_pid(new_pid) and new_pid != old_pid
    end)

    assert %FunPipeline{number: 3} = FunPipeline.call(pipeline, %FunPipeline{number: 2})

    Supervisor.stop(parent)
  end

  defp wait_until(fun, attempts_left \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition was not met before the deadline")

  defp wait_until(fun, attempts_left) do
    unless fun.() do
      Process.sleep(10)
      wait_until(fun, attempts_left - 1)
    end
  end
end
