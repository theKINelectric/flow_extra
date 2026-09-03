defmodule Flowex.Sync.EngineParityTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T2 trap, remainder (TS-1, "sync lies"): the parallel track (`Flowex.Stage`)
  casts the accumulated IP struct into each module pipe's own struct before
  calling (`struct(state.module, ip.struct)`) and strips `__struct__` from
  error-pipe results before merging back. The sync track passed raw
  accumulated maps — so any module pipe or error pipe with a `%__MODULE__{}`
  head (the README's documented pattern) crashed with FunctionClauseError in
  debug mode while production ran the same DSL fine. One DSL, one cast law:
  what a pipe sees must not depend on which engine walks the line.
  """

  test "module pipes see their own struct on both tracks (happy path)" do
    async_pipeline = ParityPipeline.start()
    sync_pipeline = ParityPipelineSync.start()

    async_result = ParityPipeline.call(async_pipeline, %ParityPipeline{number: 1})
    sync_result = ParityPipelineSync.call(sync_pipeline, %ParityPipelineSync{number: 1})

    assert async_result.number == 2
    assert Map.delete(async_result, :__struct__) == Map.delete(sync_result, :__struct__)
  end

  test "error pipes see their own struct on both tracks (error track)" do
    async_pipeline = ErrorParityPipeline.start()
    sync_pipeline = ErrorParityPipelineSync.start()

    async_result = ErrorParityPipeline.call(async_pipeline, %ErrorParityPipeline{number: 1})
    sync_result = ErrorParityPipelineSync.call(sync_pipeline, %ErrorParityPipelineSync{number: 1})

    assert async_result.number == 101
    assert Map.delete(async_result, :__struct__) == Map.delete(sync_result, :__struct__)
  end
end
