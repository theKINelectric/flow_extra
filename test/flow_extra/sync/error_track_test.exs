defmodule FlowExtra.Sync.ErrorTrackTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T2 trap (TS-1, "sync lies"): `lib/flow_extra/pipes/stage.ex` (the parallel track)
  builds `FlowExtra.PipeError` with all four keys — `error`, `message`, `pipe`,
  `struct` — while `lib/flow_extra/sync/gen_server.ex` builds it without `error:`.
  The sync error track must carry the original exception: parity with the
  parallel track is the law of the railway.
  """

  test "sync error track carries the original exception" do
    pipeline = SyncErrorTrackPipeline.start(%{})

    output =
      SyncErrorTrackPipeline.call(pipeline, %SyncErrorTrackPipeline{number: 2})

    assert %RuntimeError{message: "boom"} = output.caught_error
  end
end
