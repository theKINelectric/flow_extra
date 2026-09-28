defmodule FlowExtra.Sync.HappyTrackTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-004 trap (pursuit A3, "the wrong track"): the sync walker dispatched on
  error state alone, so a healthy packet entered the error stage's normal
  two-argument call — a dual-arity error module answered `:wrong_error_path`
  on input that never failed (the audit probe), and even the default
  handler's arity mismatch was rescued into a concealed ip.error on every
  successful sync call. Dispatch must consider stage type AND error state:
  healthy packets skip the error track entirely (what FlowExtra.Stage does),
  failed packets run the intended three-argument handler.
  """

  test "healthy packets never touch the error track on either engine" do
    async_pipeline = HappyTrackPipeline.start(%{observer: self()})
    sync_pipeline = HappyTrackPipelineSync.start(%{observer: self()})

    assert %HappyTrackPipeline{value: :original} =
             HappyTrackPipeline.call(async_pipeline, %HappyTrackPipeline{})

    assert %HappyTrackPipelineSync{value: :original} =
             HappyTrackPipelineSync.call(sync_pipeline, %HappyTrackPipelineSync{})

    refute_received :wrong_track
  end

  test "failed packets reach the three-argument handler on either engine" do
    async_pipeline = FailingTrackPipeline.start(%{observer: self()})
    sync_pipeline = FailingTrackPipelineSync.start(%{observer: self()})

    assert %FailingTrackPipeline{value: :original} =
             FailingTrackPipeline.call(async_pipeline, %FailingTrackPipeline{})

    assert %FailingTrackPipelineSync{value: :original} =
             FailingTrackPipelineSync.call(sync_pipeline, %FailingTrackPipelineSync{})

    assert_receive :right_track, 1_000
    refute_received :wrong_track
  end
end
