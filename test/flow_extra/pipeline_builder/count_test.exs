defmodule FlowExtra.PipelineBuilder.CountTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T3 trap (TS-2, "count drift"): `lib/flowextra/pipeline_builder.ex` consumes pipe
  counts with a bare `Enum.map(1..count, …)`. On modern Elixir that range is
  decreasing below 1, so `count: 0` builds two stages and `count: -1` builds
  three — the pipeline lies about its own topology. The builder must refuse
  non-positive counts at start time, before any stage exists.
  """

  test "a non-positive pipe count is refused at build time" do
    assert_raise ArgumentError, ~r/positive integer/, fn ->
      CountDriftPipeline.start(%{})
    end
  end
end
