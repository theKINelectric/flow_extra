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
  The pipeline refused the request before admission (FX-001): its
  admitted-work capacity is exhausted (`:overloaded`), or admission was
  not confirmed within the submission's own budget (`:unavailable`).

  `:unavailable` is honest about its certainty. A refusal the owner
  explicitly returned — settling, or expired at dequeue — is definite:
  no permit was taken and no work began. `request_ref` being set marks
  an outcome the caller learned by its acknowledgment timing out: the
  submission may have been admitted in the last instant before the
  reply was lost. Reconcile it with `Flowex.Admission.report/1` (the
  `:refs` list) using that reference. This is a policy outcome, not a
  pipeline failure.
  """

  defexception pipeline: nil, reason: nil, request_ref: nil

  @impl true
  def message(error = %__MODULE__{reason: :overloaded}) do
    "Flowex pipeline #{describe(error)} is at its admitted-work capacity — refused before admission"
  end

  def message(error = %__MODULE__{reason: :unavailable, request_ref: nil}) do
    "Flowex pipeline #{describe(error)} did not confirm admission within the submission budget — refused before admission, retry"
  end

  def message(error = %__MODULE__{reason: :unavailable}) do
    "Flowex pipeline #{describe(error)} did not confirm admission within the submission budget — " <>
      "it may have been admitted at the boundary (request #{inspect(error.request_ref)}); " <>
      "reconcile with Flowex.Admission.report/1"
  end

  def message(error = %__MODULE__{reason: reason}) do
    "Flowex pipeline #{describe(error)} refused admission: #{inspect(reason)}"
  end

  defp describe(%__MODULE__{pipeline: nil}), do: ""

  defp describe(%__MODULE__{pipeline: pipeline}), do: inspect(pipeline.module)
end
