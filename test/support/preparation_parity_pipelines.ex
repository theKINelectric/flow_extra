defmodule PreparationCountingPipe do
  @moduledoc """
  FX-007 instrument: a module pipe whose `init/1` reports to
  `opts[:observer]` so preparation frequency is observable.
  """

  defstruct []

  def init(opts) do
    if observer = opts[:observer], do: send(observer, :module_init)
    opts
  end

  def call(struct, _opts), do: struct
end

defmodule PreparationCountingErrorPipe do
  @moduledoc "FX-007 instrument, error-stage twin."

  defstruct []

  def init(opts) do
    if observer = opts[:observer], do: send(observer, :error_module_init)
    opts
  end

  def call(_error, struct, _opts), do: struct
end

defmodule PreparationFrequencyPipeline do
  use Flowex.Pipeline

  defstruct value: nil

  pipe(:mark)
  pipe(PreparationCountingPipe)

  def mark(data, _opts), do: data
end

defmodule PreparationFrequencyPipelineSync do
  use Flowex.Sync.Pipeline

  defstruct value: nil

  pipe(:mark)
  pipe(PreparationCountingPipe)

  def mark(data, _opts), do: data
end

defmodule PreparationErrorStagePipeline do
  use Flowex.Pipeline

  defstruct value: :original

  pipe(:identity)
  error_pipe(PreparationCountingErrorPipe)

  def identity(data, _opts), do: data
end

defmodule PreparationErrorStagePipelineSync do
  use Flowex.Sync.Pipeline

  defstruct value: :original

  pipe(:identity)
  error_pipe(PreparationCountingErrorPipe)

  def identity(data, _opts), do: data
end

defmodule SyncCountCapPipeline do
  use Flowex.Sync.Pipeline

  defstruct []

  pipe(:noop, count: 1000)

  def noop(struct, _opts), do: struct
end
