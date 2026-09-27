defmodule ClientTrapPipeline do
  @moduledoc """
  FX-006 instrument: 200ms of work reporting `{:worked, ref}` to
  `struct.report_to` — the barrier that proves whether a queued or timed-out
  request's callbacks actually ran.
  """

  use Flowex.Pipeline

  defstruct [:report_to, :ref]

  pipe(:work)

  def work(%{report_to: report_to, ref: ref}, _opts) do
    Process.sleep(200)
    if report_to, do: send(report_to, {:worked, ref})
    %{}
  end
end

defmodule ClientSlowPipeline do
  @moduledoc """
  FX-006 instrument: 5_500ms of work — deliberately past the engine's
  5_000ms default, so a completed call proves the caller's budget reached
  the engine instead of being re-defaulted.
  """

  use Flowex.Pipeline

  defstruct [:number]

  pipe(:slow)

  def slow(struct, _opts) do
    Process.sleep(5_500)
    struct
  end
end
