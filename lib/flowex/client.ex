defmodule Flowex.Client do
  @moduledoc """
  Abstraction to call the pipeline.

  One budget (FX-006): `call/3` and `call!/3` start the clock at the
  public entry — queueing behind a busy client and the pipeline's
  execution share one absolute deadline, and the remaining budget is
  handed to the engine rather than a fresh default. Expected request
  failures (a missed deadline, a dead pipeline) raise as
  `Flowex.PipelineError` at the caller boundary without killing the
  reusable client; unexpected implementation failures crash it normally.
  Admission refusals are expected request failures too (FX-001 closure):
  `call/3` raises `Flowex.AdmissionError` the same way, and `cast/2`
  reports the pipeline's refusal — `:ok` from the client means the
  pipeline accepted and accounted the work, exactly as `cast/2` on the
  pipeline itself.
  """

  use GenServer

  # The deadline governs queueing and execution; the outer wait carries a
  # small delivery grace so an in-budget refusal reply (a raise) is not
  # beaten by a raw exit by microseconds. A server that is busy or gone
  # past deadline + grace still ends the caller's wait the hard way.
  @delivery_grace 50

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
  Calls the pipeline through the client GenServer. Expected request
  failures raise at this boundary; the client survives them.
  """
  @spec call(GenServer.server(), struct(), timeout()) :: struct()
  def call(pid, struct, timeout \\ 6_000) do
    deadline = Flowex.Pipeline.deadline(timeout)

    case GenServer.call(pid, {:call, struct, deadline}, outer_timeout(deadline)) do
      {:ok, result} -> result
      {:error, %Flowex.PipelineError{} = error} -> raise error
      {:error, %Flowex.AdmissionError{} = error} -> raise error
    end
  end

  @doc """
  Casts through the client and returns the pipeline's own answer —
  `:ok` when the work was admitted and accounted, `{:error, reason}`
  when the pipeline refused it.
  """
  @spec cast(GenServer.server(), struct()) ::
          :ok | {:error, :overloaded | :unavailable | :noprocess}
  def cast(pid, struct) do
    GenServer.call(pid, {:cast, struct})
  end

  @doc """
  One-shot call without a helper process (FX-006): direct delegation to the
  pipeline's `call/3` with the caller's own remaining budget. Expected
  failures raise `Flowex.PipelineError` directly in the caller.
  """
  @spec call!(Flowex.Pipeline.t(), struct(), timeout()) :: struct()
  def call!(pipeline, struct, timeout \\ 6_000) do
    deadline = Flowex.Pipeline.deadline(timeout)
    pipeline.module.call(pipeline, struct, Flowex.Pipeline.remaining(deadline))
  end

  defp outer_timeout(nil), do: :infinity

  defp outer_timeout(deadline) when is_integer(deadline),
    do: Flowex.Pipeline.remaining(deadline) + @delivery_grace

  @impl true
  def handle_call({:call, struct, deadline}, _from, pipeline) do
    if Flowex.Pipeline.expired?(deadline) do
      # Refused at dequeue: the request expired while queued — no callback
      # begins. The reply usually finds the caller gone; it must still be
      # sent for the case where the clocks crossed inside the grace.
      {:reply, {:error, %Flowex.PipelineError{pipeline: pipeline, reason: :timeout}}, pipeline}
    else
      try do
        result = pipeline.module.call(pipeline, struct, Flowex.Pipeline.remaining(deadline))
        {:reply, {:ok, result}, pipeline}
      rescue
        # Expected request failure: refuse through the protocol and
        # re-raise at the caller boundary — the reusable client survives.
        error in Flowex.PipelineError -> {:reply, {:error, error}, pipeline}
        error in Flowex.AdmissionError -> {:reply, {:error, error}, pipeline}
      end
    end
  end

  @impl true
  def handle_call({:cast, struct}, _from, pipeline) do
    # The pipeline's own acknowledgment IS the reply: a refusal the
    # engine already observed is not swallowed into :ok (FX-001 closure).
    {:reply, pipeline.module.cast(pipeline, struct), pipeline}
  end
end
