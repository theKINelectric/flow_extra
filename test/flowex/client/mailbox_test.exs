defmodule Flowex.Client.MailboxTest do
  use ExUnit.Case, async: true

  @moduledoc """
  T4 trap (TS-4, "mailbox swallow"): `wait_response/3` in the parallel call
  path has a catch-all arm that discards any unrelated message found in the
  caller's mailbox, and its IP clause matches on requester pid alone, so a
  leaked result from an earlier call can satisfy a later one. A call must
  consume exactly its own answer — nothing more, nothing less.
  """

  setup do
    pipeline = MailboxPipeline.start(%{})
    {:ok, pipeline: pipeline}
  end

  test "a call leaves unrelated mailbox messages untouched", %{pipeline: pipeline} do
    send(self(), {:marker, :unrelated})

    output = MailboxPipeline.call(pipeline, %MailboxPipeline{number: 1})
    assert output.number == 1

    assert_receive {:marker, :unrelated}, 0
  end

  test "a leaked result from an earlier call is not consumed by a later call", %{
    pipeline: pipeline
  } do
    send(self(), %Flowex.IP{requester: self(), struct: %{stale: true}})

    output = MailboxPipeline.call(pipeline, %MailboxPipeline{number: 2})

    assert output.number == 2
  end
end
