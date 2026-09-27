defmodule Flowex.Pipeline.ReplyOwnershipTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-005 trap (pursuit B, "who owns the reply"): the consumer delivered
  the finished packet straight to the original caller PID, so a caught
  timeout left the completed %Flowex.IP{} in the caller's mailbox —
  correlation prevented a wrong answer, but nothing reclaimed abandoned
  replies, and a long-lived caller accumulated them. Astra's ruling: the
  reply destination must be revocable (a process alias), a call resolves,
  monitors, and submits to ONE consumer incarnation, and the deadline
  travels with the packet so no stage begins work after the caller's
  budget is gone. An arrived answer beats the deadline; anything later is
  dropped by the revoked alias.
  """

  test "a caught timeout leaves no late reply behind" do
    pipeline = ReplyTrapPipeline.start()

    error =
      assert_raise Flowex.PipelineError, fn ->
        ReplyTrapPipeline.call(pipeline, %ReplyTrapPipeline{report_to: self(), ref: :late}, 40)
      end

    assert error.reason == :timeout

    # Barrier, not a stopwatch: wait until the callback has provably
    # finished, then give the reply its remaining hop to the caller — the
    # completed packet must not arrive.
    assert_receive {:worked, :late}, 2_000
    refute_receive %Flowex.IP{}, 200
  end

  test "repeated caught timeouts accumulate nothing" do
    pipeline = ReplyTrapPipeline.start()

    for i <- 1..5 do
      assert_raise Flowex.PipelineError, fn ->
        ReplyTrapPipeline.call(pipeline, %ReplyTrapPipeline{report_to: self(), ref: i}, 40)
      end

      assert_receive {:worked, ^i}, 2_000
    end

    refute_receive %Flowex.IP{}, 200
  end

  test "no stage begins work after the packet's deadline is gone" do
    pipeline = ReplyTrapTwoStagePipeline.start()

    assert_raise Flowex.PipelineError, fn ->
      ReplyTrapTwoStagePipeline.call(
        pipeline,
        %ReplyTrapTwoStagePipeline{report_to: self(), ref: :expired},
        40
      )
    end

    # The first stage began inside the 40ms budget and provably finished
    # after it (an executing callback may outlive the waiter)…
    assert_receive {:first_done, :expired}, 2_000

    # …so the second stage must not begin: it is outside the budget, and
    # its reply to the revoked destination never lands anywhere.
    refute_receive {:second_stage_ran, :expired}, 200
    refute_received %Flowex.IP{}
  end
end
