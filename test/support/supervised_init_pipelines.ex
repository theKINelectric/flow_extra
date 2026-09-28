defmodule SupervisedInitFunPipeline do
  use FlowExtra.Pipeline

  defstruct initialized: nil

  pipe(:mark)

  def init(opts), do: Map.put(opts, :initialized, true)

  def mark(_struct, opts), do: %__MODULE__{initialized: opts[:initialized]}
end

defmodule SupervisedInitFunPipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct initialized: nil

  pipe(:mark)

  def init(opts), do: Map.put(opts, :initialized, true)

  def mark(_struct, opts), do: %__MODULE__{initialized: opts[:initialized]}
end

defmodule BadPipelineInitStandalone do
  use FlowExtra.Pipeline

  defstruct []

  pipe(:noop)

  def init(_opts), do: :nope
  def noop(struct, _opts), do: struct
end

defmodule BadPipelineInitStandaloneSync do
  use FlowExtra.Sync.Pipeline

  defstruct []

  pipe(:noop)

  def init(_opts), do: :nope
  def noop(struct, _opts), do: struct
end
