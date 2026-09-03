defmodule ParallelTest do
  use ExUnit.Case, async: true

  def result(p) do
    ParallelPipeline.call(p, %ParallelPipeline{n: 1})
  end

  test "takes ~0.5 sec for 1 call" do
    pipeline = ParallelPipeline.start()

    {time, _} = :timer.tc(fn -> result(pipeline) end)

    assert time > 500_000
  end

  test "takes ~0.5 sec for 4 parallel calls" do
    pipeline = ParallelPipeline.start()

    func = fn ->
      1..4
      |> Enum.map(fn _i -> Task.async(fn -> result(pipeline) end) end)
      |> Enum.map(&Task.await/1)
    end

    {time, _} = :timer.tc(func)

    assert time in 500_000..600_000
  end
end
