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

  @doc """
  Calls the pipeline through the client GenServer.

  The default (6_000 ms) sits strictly above the pipeline engines' own
  5_000 ms deadline, so a slow pipeline surfaces as the engine's
  `Flowex.PipelineError` — which carries the pipeline context — rather than
  as this outer GenServer timeout. The outer deadline still bounds queueing
  behind a busy client.
  """
  @spec call(GenServer.server(), struct(), timeout()) :: struct()
  def call(pid, struct, timeout \\ 6_000) do
    GenServer.call(pid, {:call, struct}, timeout)
  end

  @spec cast(GenServer.server(), struct()) :: :ok
  def cast(pid, struct) do
    GenServer.cast(pid, {:cast, struct})
  end

  @spec call!(Flowex.Pipeline.t(), struct(), timeout()) :: struct()
  def call!(pipeline, struct, timeout \\ 6_000) do
    {:ok, pid} = GenServer.start_link(__MODULE__, pipeline)
    result = GenServer.call(pid, {:call, struct}, timeout)
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
