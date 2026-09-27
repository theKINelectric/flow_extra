# D — architectural comparison (pursuit §7 / Astra's qualification order).
#
# Four executors over the same deterministic work:
#   direct        — plain function composition (the pipeline callbacks, in order)
#   task_stream   — Task.async_stream with the workload's concurrency budget
#   sync engine   — Flowex.Sync.Pipeline (one process walks the line)
#   async engine  — Flowex.Pipeline (GenStage stages; slow stages replicated)
#
# Workload matrix (bounded, synthetic — see the D record for what this can
# and cannot establish):
#   cheap   — three tiny pure stages
#   cpu     — one ~1-2ms CPU stage
#   uneven  — 1ms / 20ms / 1ms stages; the slow stage runs count: 10 on the
#             async engine, matching the task_stream budget; sync and direct
#             are serial by architecture (that difference is the measurement)
#   burst   — a burst of 2ms jobs from 20 concurrent callers
#
# Run: MIX_ENV=test mix run scripts/bench_comparison.exs
# Output: a markdown table of medians (3 trials) + items/s.

Logger.configure(level: :emergency)

defmodule Bench.Cheap do
  use Flowex.Pipeline
  defstruct n: nil
  pipe(:a)
  pipe(:b)
  pipe(:c)
  def a(%{n: n}, _o), do: %{n: n + 1}
  def b(%{n: n}, _o), do: %{n: n * 2}
  def c(%{n: n}, _o), do: %{n: n - 3}
end

defmodule Bench.CheapSync do
  use Flowex.Sync.Pipeline
  defstruct n: nil
  pipe(:a)
  pipe(:b)
  pipe(:c)
  def a(%{n: n}, _o), do: %{n: n + 1}
  def b(%{n: n}, _o), do: %{n: n * 2}
  def c(%{n: n}, _o), do: %{n: n - 3}
end

defmodule Bench.Cpu do
  use Flowex.Pipeline
  defstruct n: nil
  pipe(:grind)

  def grind(%{n: n}, _o) do
    for _ <- 1..150_000, do: :math.sqrt(n * 1.0001)
    %{n: n}
  end
end

defmodule Bench.CpuSync do
  use Flowex.Sync.Pipeline
  defstruct n: nil
  pipe(:grind)

  def grind(%{n: n}, _o) do
    for _ <- 1..150_000, do: :math.sqrt(n * 1.0001)
    %{n: n}
  end
end

defmodule Bench.Cpu8 do
  use Flowex.Pipeline
  defstruct n: nil
  pipe(:grind, count: 8)

  def grind(%{n: n}, _o) do
    for _ <- 1..150_000, do: :math.sqrt(n * 1.0001)
    %{n: n}
  end
end

defmodule Bench.Uneven do
  use Flowex.Pipeline
  defstruct n: nil
  pipe(:fast_a)
  pipe(:slow, count: 10)
  pipe(:fast_b)
  def fast_a(s, _o), do: s
  def slow(%{n: n}, _o), do: Process.sleep(20) && %{n: n}
  def fast_b(s, _o), do: s
end

defmodule Bench.UnevenSync do
  use Flowex.Sync.Pipeline
  defstruct n: nil
  pipe(:fast_a)
  pipe(:slow, count: 10)
  pipe(:fast_b)
  def fast_a(s, _o), do: s
  def slow(%{n: n}, _o), do: Process.sleep(20) && %{n: n}
  def fast_b(s, _o), do: s
end

defmodule Bench.Burst do
  use Flowex.Pipeline
  defstruct n: nil
  pipe(:work, count: 20)
  def work(%{n: n}, _o), do: Process.sleep(2) && %{n: n + 1}
end

defmodule Bench.BurstSync do
  use Flowex.Sync.Pipeline
  defstruct n: nil
  pipe(:work, count: 20)
  def work(%{n: n}, _o), do: Process.sleep(2) && %{n: n + 1}
end

defmodule Bench.Runner do
  @repeat 3

  # executors — each gets the pipeline module pair and a concurrency budget

  def direct(mod, items) do
    Enum.map(items, &compose(mod, &1))
  end

  def task_stream(mod, items, concurrency) do
    Task.async_stream(items, &compose(mod, &1), max_concurrency: concurrency, timeout: 30_000)
    |> Enum.map(fn {:ok, v} -> v end)
  end

  def sync(sync_pipeline, items) do
    Enum.map(
      items,
      &sync_pipeline.module.call(sync_pipeline, struct(sync_pipeline.module, n: &1))
    )
  end

  def async(pipeline, items, concurrency) do
    Task.async_stream(
      items,
      fn n ->
        pipeline.module.call(pipeline, struct(pipeline.module, n: n))
      end,
      max_concurrency: concurrency,
      timeout: 30_000
    )
    |> Enum.map(fn {:ok, v} -> v end)
  end

  defp compose(mod, n) do
    # Direct composition over the same callbacks, in declaration order.
    stages = Enum.map(mod.pipes(), fn {atom, _count, _opts, :pipe} -> atom end)

    acc =
      Enum.reduce(stages, struct(mod, n: n), fn stage, acc ->
        result = apply(mod, stage, [acc, %{}])
        Map.merge(acc, Map.delete(result, :__struct__))
      end)

    acc.n
  end

  def measure(label, fun) do
    times =
      for _ <- 1..@repeat do
        {micros, value} = :timer.tc(fun)
        true = is_list(value) and length(value) > 0
        micros
      end

    median = times |> Enum.sort() |> Enum.at(div(@repeat, 2))
    {label, median}
  end
end

workloads = [
  {:cheap,
   fn ->
     items = Enum.to_list(1..1500)
     p = Bench.Cheap.start(%{admission_capacity: 1500})
     s = Bench.CheapSync.start(%{})

     [
       Bench.Runner.measure("direct", fn -> Bench.Runner.direct(p.module, items) end),
       Bench.Runner.measure("task_stream", fn -> Bench.Runner.task_stream(p.module, items, 8) end),
       Bench.Runner.measure("sync", fn -> Bench.Runner.sync(s, items) end),
       Bench.Runner.measure("async", fn -> Bench.Runner.async(p, items, 8) end)
     ]
   end},
  {:cpu,
   fn ->
     items = Enum.to_list(1..240)
     p = Bench.Cpu.start(%{admission_capacity: 240})
     s = Bench.CpuSync.start(%{})

     [
       Bench.Runner.measure("direct", fn -> Bench.Runner.direct(p.module, items) end),
       Bench.Runner.measure("task_stream", fn -> Bench.Runner.task_stream(p.module, items, 8) end),
       Bench.Runner.measure("sync", fn -> Bench.Runner.sync(s, items) end),
       Bench.Runner.measure("async", fn -> Bench.Runner.async(p, items, 8) end)
     ]
   end},
  {:cpu8,
   fn ->
     items = Enum.to_list(1..240)
     p = Bench.Cpu8.start(%{admission_capacity: 240})
     s = Bench.CpuSync.start(%{})

     [
       Bench.Runner.measure("direct", fn -> Bench.Runner.direct(p.module, items) end),
       Bench.Runner.measure("task_stream", fn -> Bench.Runner.task_stream(p.module, items, 8) end),
       Bench.Runner.measure("async", fn -> Bench.Runner.async(p, items, 8) end)
     ]
   end},
  {:uneven,
   fn ->
     items = Enum.to_list(1..150)
     p = Bench.Uneven.start(%{admission_capacity: 150})
     s = Bench.UnevenSync.start(%{})

     [
       Bench.Runner.measure("direct", fn -> Bench.Runner.direct(p.module, items) end),
       Bench.Runner.measure("task_stream", fn -> Bench.Runner.task_stream(p.module, items, 10) end),
       Bench.Runner.measure("sync", fn -> Bench.Runner.sync(s, items) end),
       Bench.Runner.measure("async", fn -> Bench.Runner.async(p, items, 10) end)
     ]
   end},
  {:burst,
   fn ->
     items = Enum.to_list(1..400)
     p = Bench.Burst.start(%{admission_capacity: 400})
     s = Bench.BurstSync.start(%{})

     [
       Bench.Runner.measure("direct", fn -> Bench.Runner.direct(p.module, items) end),
       Bench.Runner.measure("task_stream", fn -> Bench.Runner.task_stream(p.module, items, 20) end),
       Bench.Runner.measure("sync", fn -> Bench.Runner.sync(s, items) end),
       Bench.Runner.measure("async", fn -> Bench.Runner.async(p, items, 20) end)
     ]
   end}
]

IO.puts("| workload | executor | median ms | items/s |")
IO.puts("| --- | --- | --- | --- |")

counts = %{cheap: 1500, cpu: 240, cpu8: 240, uneven: 150, burst: 400}

for {name, run} <- workloads do
  results = run.()

  for {executor, micros} <- results do
    ms = Float.round(micros / 1000, 1)
    per_sec = Float.round(counts[name] / (micros / 1_000_000), 0)

    IO.puts(
      "| #{name} | #{executor} | #{ms} | #{:erlang.float_to_binary(per_sec, decimals: 0)} |"
    )
  end
end

IO.puts("")

IO.puts(
  "Runtime: Elixir #{System.version()} / OTP #{System.otp_release()}; schedulers: #{System.schedulers_online()}"
)
