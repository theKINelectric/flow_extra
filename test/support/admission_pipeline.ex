defmodule CountCapPipeline do
  use FlowExtra.Pipeline

  defstruct [:number]

  pipe(:id, count: 101)

  def id(struct, _opts), do: struct
end

defmodule BadInitModule do
  defstruct [:number]

  def init(_opts), do: :garbage

  def call(struct, %{}), do: struct
end

defmodule BadModuleInitPipeline do
  use FlowExtra.Pipeline

  defstruct [:number]

  pipe(BadInitModule, count: 1)
end

defmodule BadModuleInitPipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct [:number]

  pipe(BadInitModule, count: 1)
end

defmodule BadPipelineInitPipeline do
  use FlowExtra.Pipeline

  defstruct [:number]

  pipe(:id, count: 1)

  def init(_opts), do: :garbage

  def id(struct, _opts), do: struct
end

defmodule BadPipelineInitPipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct [:number]

  pipe(:id, count: 1)

  def init(_opts), do: :garbage

  def id(struct, _opts), do: struct
end
