defmodule FlowExtra.Client do
  @moduledoc """
  Abstraction to call the pipeline.

  One budget (FX-006): `call/3` and `call!/3` start the clock at the
  public entry — queueing behind a busy client and the pipeline's
  execution share one absolute deadline, and the remaining budget is
  handed to the engine rather than a fresh default. Expected request
  failures (a missed deadline, a dead pipeline) raise as
  `FlowExtra.PipelineError` at the caller boundary without killing the
  reusable client; unexpected implementation failures crash it normally.
  Admission answers are expected request failures too (FX-001 closure):
  `call/3` raises `FlowExtra.AdmissionError` the same way, and `cast/2`
  reports the pipeline's own answer — `:ok` from the client means the
  pipeline accepted and accounted the work, a definite refusal arrives
  as `{:error, :overloaded | :unavailable}`, a lost admission endpoint
  arrives as `{:error, :noprocess}` (communication with the admission
  owner failed; whether the submission executed or produced effects
  before that failure is not established), and an outcome learned only
  by its acknowledgment timing out arrives as
  `{:error, {:unacknowledged, ref}}` — uncertain, never spelled as a
  refusal. See `FlowExtra.Admission.submit/3` for what each shape lets the
  caller infer.
  """

  use GenServer

  # The deadline governs queueing and execution; the outer wait carries a
  # small delivery grace so an in-budget refusal reply (a raise) is not
  # beaten by a raw exit by microseconds. A server that is busy or gone
  # past deadline + grace still ends the caller's wait the hard way.
  @delivery_grace 50

  @spec start(FlowExtra.Pipeline.t(), GenServer.options()) :: GenServer.on_start()
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
    deadline = FlowExtra.Pipeline.deadline(timeout)

    case GenServer.call(pid, {:call, struct, deadline}, outer_timeout(deadline)) do
      {:ok, result} -> result
      {:error, %FlowExtra.PipelineError{} = error} -> raise error
      {:error, %FlowExtra.AdmissionError{} = error} -> raise error
    end
  end

  @doc """
  Casts through the client and returns the pipeline's own answer —
  `:ok` when the work was admitted and accounted; `{:error,
  :overloaded | :unavailable}` when the pipeline definitely refused it;
  `{:error, :noprocess}` when communication with the admission owner
  failed (the process was unavailable or terminated; whether the
  submission executed or produced effects before that failure is not
  established — not a definite refusal); and `{:error,
  {:unacknowledged, ref}}` when only the acknowledgment timed out
  (uncertain; the ref is the request identity).
  """
  @spec cast(GenServer.server(), struct()) ::
          :ok
          | {:error, :overloaded | :unavailable | :noprocess}
          | {:error, {:unacknowledged, reference()}}
  def cast(pid, struct) do
    GenServer.call(pid, {:cast, struct})
  end

  @doc """
  One-shot call without a helper process (FX-006): direct delegation to the
  pipeline's `call/3` with the caller's own remaining budget. Expected
  failures raise `FlowExtra.PipelineError` directly in the caller.
  """
  @spec call!(FlowExtra.Pipeline.t(), struct(), timeout()) :: struct()
  def call!(pipeline, struct, timeout \\ 6_000) do
    deadline = FlowExtra.Pipeline.deadline(timeout)
    pipeline.module.call(pipeline, struct, FlowExtra.Pipeline.remaining(deadline))
  end

  defp outer_timeout(nil), do: :infinity

  defp outer_timeout(deadline) when is_integer(deadline),
    do: FlowExtra.Pipeline.remaining(deadline) + @delivery_grace

  @impl true
  def handle_call({:call, struct, deadline}, _from, pipeline) do
    if FlowExtra.Pipeline.expired?(deadline) do
      # Refused at dequeue: the request expired while queued — no callback
      # begins. The reply usually finds the caller gone; it must still be
      # sent for the case where the clocks crossed inside the grace.
      error = %FlowExtra.PipelineError{pipeline: pipeline, reason: :timeout}
      {:reply, {:error, error}, pipeline}
    else
      try do
        result = pipeline.module.call(pipeline, struct, FlowExtra.Pipeline.remaining(deadline))
        {:reply, {:ok, result}, pipeline}
      rescue
        # Expected request failure: refuse through the protocol and
        # re-raise at the caller boundary — the reusable client survives.
        error in FlowExtra.PipelineError -> {:reply, {:error, error}, pipeline}
        error in FlowExtra.AdmissionError -> {:reply, {:error, error}, pipeline}
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
