defmodule Flowex.Client do
  @moduledoc "Absctraction to call the pipeline"

  use GenServer

  @spec start(Flowex.Pipeline.t(), GenServer.options()) :: GenServer.on_start()
  def start(pipeline, opts \\ []) do
    GenServer.start_link(__MODULE__, pipeline, opts)
  end

  @spec stop(GenServer.server()) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  @impl true
  def init(pipeline) do
    {:ok, pipeline}
  end

  @spec call(GenServer.server(), struct()) :: struct()
  def call(pid, struct) do
    GenServer.call(pid, {:call, struct}, :infinity)
  end

  @spec cast(GenServer.server(), struct()) :: :ok
  def cast(pid, struct) do
    GenServer.cast(pid, {:cast, struct})
  end

  @spec call!(Flowex.Pipeline.t(), struct()) :: struct()
  def call!(pipeline, struct) do
    {:ok, pid} = GenServer.start_link(__MODULE__, pipeline)
    result = GenServer.call(pid, {:call, struct}, :infinity)
    GenServer.stop(pid)
    result
  end

  @impl true
  def handle_call({:call, struct}, _pid, pipeline) do
    struct = pipeline.module.call(pipeline, struct)
    {:reply, struct, pipeline}
  end

  @impl true
  def handle_cast({:cast, struct}, pipeline) do
    pipeline.module.cast(pipeline, struct)
    {:noreply, pipeline}
  end
end
