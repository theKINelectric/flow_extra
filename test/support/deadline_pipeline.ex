defmodule DeadlinePipeline do
  use FlowExtra.Pipeline

  defstruct [:number]

  pipe(:sleep_500, count: 2)

  def sleep_500(struct, _opts) do
    :timer.sleep(500)
    struct
  end
end

defmodule DeadlinePipelineSync do
  use FlowExtra.Sync.Pipeline

  defstruct [:number]

  pipe(:sleep_500, count: 2)

  def sleep_500(struct, _opts) do
    :timer.sleep(500)
    struct
  end
end
