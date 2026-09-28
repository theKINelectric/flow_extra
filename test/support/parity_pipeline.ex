defmodule ParityAddOne do
  @moduledoc "Module pipe with a struct-pattern head — the README contract."

  defstruct [:number]

  def init(opts), do: opts

  def call(%__MODULE__{number: number}, _opts) do
    %__MODULE__{number: number + 1}
  end
end

defmodule ParityErrorPipe do
  @moduledoc "Error pipe with a struct-pattern head, returning a struct — the hard case."

  defstruct [:number]

  def init(opts), do: opts

  def call(_error, %__MODULE__{number: number}, _opts) do
    %__MODULE__{number: number + 100}
  end
end

defmodule ParityPipeline do
  use FlowExtra.Pipeline

  defstruct [:number, :mark]

  pipe(ParityAddOne, count: 2)
  pipe(:set_mark, count: 1)
  error_pipe(ParityErrorPipe, count: 2)

  def set_mark(_struct, _opts), do: %{mark: :set}
end

defmodule ParityPipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct [:number, :mark]

  pipe(ParityAddOne, count: 2)
  pipe(:set_mark, count: 1)
  error_pipe(ParityErrorPipe, count: 2)

  def set_mark(_struct, _opts), do: %{mark: :set}
end

defmodule ErrorParityPipeline do
  use FlowExtra.Pipeline

  defstruct [:number, :mark]

  pipe(:boom, count: 1)
  error_pipe(ParityErrorPipe, count: 2)

  def boom(_struct, _opts), do: raise(ArgumentError, "boom")
end

defmodule ErrorParityPipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct [:number, :mark]

  pipe(:boom, count: 1)
  error_pipe(ParityErrorPipe, count: 2)

  def boom(_struct, _opts), do: raise(ArgumentError, "boom")
end
