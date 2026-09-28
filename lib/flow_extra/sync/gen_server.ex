defmodule FlowExtra.Sync.GenServer do
  @moduledoc """
  Sync pipeline runner: one process, the line walked in a call. The state is
  the prepared stage list (FX-007) — module `init/1` already ran in the
  starting caller, so the walker reuses prepared options and never
  initializes per request.
  """

  use GenServer

  def start_link(state, opts \\ []) do
    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(prepared_stages) do
    {:ok, prepared_stages}
  end

  @impl true
  def handle_call(ip, _from, prepared_stages) do
    result = do_call(ip, prepared_stages)
    {:reply, result, prepared_stages}
  end

  @impl true
  def handle_cast(ip, prepared_stages) do
    do_call(ip, prepared_stages)
    {:noreply, prepared_stages}
  end

  defp do_call(ip, prepared_stages) do
    Enum.reduce(prepared_stages, ip, fn stage, ip -> process(stage, ip) end)
  end

  defp try_apply(ip, {module, function, pipe_opts}) do
    # The cast law (engine parity with FlowExtra.Stage): every module sees its
    # own struct at the pipe boundary, never the accumulated raw map.
    struct = struct(module, ip.struct)
    result = apply(module, function, [struct, pipe_opts])
    %{ip | struct: Map.merge(ip.struct, Map.delete(result, :__struct__))}
  rescue
    error ->
      error_struct = %FlowExtra.PipeError{
        error: error,
        message: Exception.message(error),
        pipe: {module, function, pipe_opts},
        struct: ip.struct
      }

      %{ip | error: error_struct}
  end

  # Dispatch on the packet's deadline, stage type, AND error state —
  # mirroring FlowExtra.Stage: an expired packet skips every remaining stage
  # (no callback, no error handler) and goes home expired; a healthy packet
  # skips the error track entirely; a failed packet skips the remaining
  # normal pipes and reaches the error handler.
  defp process(stage, ip) do
    cond do
      FlowExtra.Pipeline.expired?(ip.deadline) ->
        %{ip | expired: true}

      ip.error ->
        do_process_error(ip, stage)

      stage.type == :error_pipe ->
        ip

      true ->
        do_process(ip, stage)
    end
  end

  defp do_process(ip, %FlowExtra.StageOpts{module: module, function: function, opts: opts}) do
    try_apply(ip, {module, function, opts})
  end

  # Function and module error handlers unify here: a prepared function stage
  # carries {pipeline_module, :handle_error}, a module stage {module, :call}
  # — both three-argument, both seeing their own module's struct.
  defp do_process_error(ip, stage = %FlowExtra.StageOpts{type: :error_pipe}) do
    %{module: module, function: function, opts: opts} = stage
    struct = struct(module, ip.struct)
    result = apply(module, function, [ip.error, struct, opts])
    %{ip | struct: Map.merge(ip.struct, Map.delete(result, :__struct__))}
  end

  defp do_process_error(ip, %FlowExtra.StageOpts{type: :pipe}), do: ip
end
