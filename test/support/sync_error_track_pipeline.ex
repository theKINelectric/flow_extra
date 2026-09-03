defmodule SyncErrorTrackPipeline do
  @moduledoc """
  T2 trap fixture (TS-1, "sync lies"): a sync pipeline whose first pipe raises
  and whose error pipe captures the PipeError's `error` key for inspection.
  """

  use Flowex.Sync.Pipeline

  defstruct [:number, :caught_error]

  pipe(:boom, count: 1)
  error_pipe(:catch_it, count: 1)

  def boom(_struct, _opts), do: raise(RuntimeError, message: "boom")

  def catch_it(error, _struct, _opts) do
    %{caught_error: error.error}
  end
end
