defmodule ReplyTrapPipeline do
  @moduledoc """
  FX-005 instrument: one slow stage that reports completion to
  `struct.report_to` — the barrier that proves the finished packet exists
  after the caller has timed out.
  """

  use Flowex.Pipeline

  defstruct [:report_to, :ref]

  pipe(:work)

  def work(%{report_to: report_to, ref: ref}, _opts) do
    Process.sleep(80)
    if report_to, do: send(report_to, {:worked, ref})
    %{}
  end
end

defmodule ReplyTrapTwoStagePipeline do
  @moduledoc """
  FX-005 instrument: a slow first stage that reports its own completion,
  then a fast second stage that reports whether it began at all.
  """

  use Flowex.Pipeline

  defstruct [:report_to, :ref]

  pipe(:slow)
  pipe(:afterwards)

  def slow(%{report_to: report_to, ref: ref}, _opts) do
    Process.sleep(80)
    if report_to, do: send(report_to, {:first_done, ref})
    %{}
  end

  def afterwards(%{report_to: report_to, ref: ref}, _opts) do
    if report_to, do: send(report_to, {:second_stage_ran, ref})
    %{}
  end
end
