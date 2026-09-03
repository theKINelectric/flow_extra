defmodule CountDriftPipeline do
  @moduledoc """
  T3 trap fixture (TS-2, "count drift"): a pipeline declared with `count: 0`.
  On Elixir 1.20 `Enum.map(1..count, …)` treats `1..0` as a *decreasing* range
  (`[1, 0]`), so a zero-count pipe silently builds two stages.
  """

  use Flowex.Pipeline

  defstruct [:number]

  pipe :do_nothing, count: 0

  def do_nothing(struct, _opts), do: struct
end
