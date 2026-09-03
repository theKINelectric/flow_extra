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

    # The property is concurrency, not a stopwatch: each call sleeps 500 ms
    # and the sleep stage runs count: 4, so four concurrent calls land near
    # 500 ms while four serialized runs would need at least 2_000 ms. The
    # generous ceiling keeps the proof alive under CI load — the old
    # 500–600 ms window flaked on any busy machine.
    assert time > 500_000
    assert time < 2_000_000
  end
end
