defmodule PreparationTokenPipe do
  @moduledoc """
  FX-007 instrument: mints one unique token per `init/1` run, reports
  `{:prepared, engine_tag, init_process, token}` to `opts[:observer]`, and
  stamps the token into results — so preparation count, preparation
  process, and the survival of prepared options through requests and
  restarts are all observable.
  """

  defstruct [:prepared_token]

  def init(opts) do
    token = make_ref()

    if observer = opts[:observer],
      do: send(observer, {:prepared, opts[:engine], self(), token})

    Map.put(opts, :prepared_token, token)
  end

  def call(_struct, opts), do: %__MODULE__{prepared_token: opts[:prepared_token]}
end

defmodule PreparationTokenPipeline do
  use Flowex.Pipeline

  defstruct [:prepared_token]

  pipe(:mark)
  pipe(PreparationTokenPipe, count: 3)

  def mark(data, _opts), do: data
end

defmodule PreparationTokenPipelineSync do
  use Flowex.Sync.Pipeline

  defstruct [:prepared_token]

  pipe(:mark)
  pipe(PreparationTokenPipe, count: 3)

  def mark(data, _opts), do: data
end

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

defmodule LateBadCountPipeline do
  @moduledoc """
  FX-007 hardening fixture: a valid module pipe declared BEFORE an invalid
  count. The whole declaration must be validated before any module
  initializer runs, so this pipeline's start raises without a single
  `{:prepared, _, _, _}` observation.
  """

  use Flowex.Pipeline

  defstruct []

  pipe(PreparationTokenPipe)
  pipe(:noop, count: 1000)

  def noop(struct, _opts), do: struct
end

defmodule LateBadCountPipelineSync do
  use Flowex.Sync.Pipeline

  defstruct []

  pipe(PreparationTokenPipe)
  pipe(:noop, count: 1000)

  def noop(struct, _opts), do: struct
end
