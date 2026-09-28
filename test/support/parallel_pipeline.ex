defmodule ParallelPipeline do
  use FlowExtra.Pipeline
  defstruct [:n]

  pipe(:add)
  pipe(:sleep, count: 4)

  def add(%{n: n}, _opts) do
    %{n: n + 1}
  end

  def sleep(%{n: n}, _opts) do
    :timer.sleep(500)
    %{n: n}
  end
end
