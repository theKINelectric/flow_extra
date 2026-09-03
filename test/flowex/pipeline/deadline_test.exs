defmodule Flowex.Pipeline.DeadlineTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T4 trap, remainder (TS-3, "time has no limit"): every call path passed
  `:infinity` — the async track's `wait_response` had no `after`, the sync
  track and the Client passed `:infinity` to `GenServer.call`. A slow (not
  crashed) pipeline left callers hanging forever; caller liveness rested
  entirely on the crash cascade, unasserted and undocumented. Every call must
  carry a deadline — 5_000 ms by default, overridable at the call site — and a
  timed-out async call must not leave the monitor's stray :DOWN in the
  caller's mailbox.
  """

  test "the async track raises PipelineError when the deadline passes" do
    pipeline = DeadlinePipeline.start()

    assert_raise Flowex.PipelineError, fn ->
      DeadlinePipeline.call(pipeline, %DeadlinePipeline{number: 1}, 50)
    end
  end

  test "a timed-out async call leaves no stray monitor message behind" do
    pipeline = DeadlinePipeline.start()

    assert_raise Flowex.PipelineError, fn ->
      DeadlinePipeline.call(pipeline, %DeadlinePipeline{number: 1}, 50)
    end

    refute_received _
  end

  test "the sync track dies when the deadline passes" do
    pipeline = DeadlinePipelineSync.start()

    assert catch_exit(
              DeadlinePipelineSync.call(pipeline, %DeadlinePipelineSync{number: 1}, 50)
            )
  end

  test "the client honors a deadline" do
    pipeline = DeadlinePipeline.start()
    {:ok, client} = Flowex.Client.start(pipeline)

    assert catch_exit(Flowex.Client.call(client, %DeadlinePipeline{number: 1}, 50))
  end

  test "the default deadline leaves generous calls alone" do
    pipeline = DeadlinePipeline.start()

    assert %DeadlinePipeline{number: 1} =
             DeadlinePipeline.call(pipeline, %DeadlinePipeline{number: 1})
  end
end
