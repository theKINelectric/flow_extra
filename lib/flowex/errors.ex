defmodule Flowex.PipelineError do
  @moduledoc """
  The caller-side failure: the pipeline crashed, vanished, or missed its
  deadline.

  `reason` keeps the machine-readable cause — `:timeout`, `:noprocess`, or
  the consumer's exit reason as the monitor saw it. `message`, when given,
  is a human string. `Exception.message/1` renders a readable line either
  way (FX-008: atoms and exit terms are not strings).
  """

  defexception pipeline: nil, message: nil, reason: nil

  @impl true
  def message(%__MODULE__{message: message}) when is_binary(message), do: message

  def message(%__MODULE__{reason: reason, pipeline: pipeline}) do
    subject =
      if pipeline, do: "Flowex pipeline #{inspect(pipeline.module)}", else: "Flowex pipeline"

    detail =
      case reason do
        :timeout -> " did not answer before its deadline"
        :noprocess -> " is not running"
        reason -> " exited: #{inspect(reason)}"
      end

    subject <> detail
  end
end

defmodule Flowex.PipeError do
  @moduledoc """
  The stage-side failure: a pipe callback raised, rescued into the packet's
  error track. `error` keeps the original exception; `pipe` names the
  callback that failed.
  """

  defexception error: nil, message: nil, pipe: nil, struct: nil
end

defmodule Flowex.AdmissionError do
  @moduledoc """
  The pipeline's answer before admission (FX-001), named by what the
  caller actually knows.

  `:overloaded` and `:unavailable` are definite refusals — the owner
  itself returned them (capacity; settling or expired at dequeue): no
  permit was taken, nothing of this submission will execute.

  `:unacknowledged` is NOT a refusal: the caller learned the outcome by
  its acknowledgment timing out, and the submission may have been
  admitted in the last instant before the reply was lost. `request_ref`
  identifies it. Check `Flowex.Admission.report/1`'s `:refs` — a live
  view of unresolved reservations, not an admission history: the ref's
  presence means admitted-and-unresolved; its absence means
  released-or-never-admitted and proves nothing. These are policy
  outcomes, not pipeline failures.
  """

  defexception pipeline: nil, reason: nil, request_ref: nil

  @impl true
  def message(error = %__MODULE__{reason: :overloaded}) do
    "Flowex pipeline #{describe(error)} is at its admitted-work capacity — refused before admission"
  end

  def message(error = %__MODULE__{reason: :unavailable}) do
    "Flowex pipeline #{describe(error)} did not accept the submission — settling, or the attempt expired at dequeue; nothing was reserved"
  end

  def message(error = %__MODULE__{reason: :unacknowledged}) do
    "Flowex pipeline #{describe(error)} did not acknowledge the submission before its budget ended — " <>
      "it may have been admitted at the boundary (request #{inspect(error.request_ref)}); " <>
      "Flowex.Admission.report/1 :refs shows unresolved reservations, and absence there is inconclusive"
  end

  def message(error = %__MODULE__{reason: reason}) do
    "Flowex pipeline #{describe(error)} refused admission: #{inspect(reason)}"
  end

  defp describe(%__MODULE__{pipeline: nil}), do: ""

  defp describe(%__MODULE__{pipeline: pipeline}), do: inspect(pipeline.module)
end
