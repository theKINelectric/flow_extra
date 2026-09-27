defmodule Flowex.Stage do
  @moduledoc "Pipes function is called here"

  use GenStage

  def start_link(state, opts \\ []) do
    GenStage.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(opts) do
    subscribe_to_with_opts = Enum.map(opts.producer_names, &{&1, max_demand: 1})
    {:producer_consumer, opts, subscribe_to: subscribe_to_with_opts}
  end

  @impl true
  def handle_events([ip], _from, state = %Flowex.StageOpts{type: :pipe}) do
    if Flowex.Pipeline.expired?(ip.deadline) do
      # The budget died before this stage could begin: no callback runs and
      # no error handler runs — the packet goes home expired (FX-005).
      {:noreply, [%{ip | expired: true}], state}
    else
      if ip.error do
        {:noreply, [ip], state}
      else
        new_ip = try_apply(ip, {state.module, state.function, state.opts})
        {:noreply, [new_ip], state}
      end
    end
  end

  @impl true
  def handle_events([ip], _from, state = %Flowex.StageOpts{type: :error_pipe}) do
    if Flowex.Pipeline.expired?(ip.deadline) do
      {:noreply, [%{ip | expired: true}], state}
    else
      if ip.error do
        struct = struct(state.module, ip.struct)
        result = apply(state.module, state.function, [ip.error, struct, state.opts])
        ip_struct = Map.merge(ip.struct, Map.delete(result, :__struct__))
        {:noreply, [%{ip | struct: ip_struct}], state}
      else
        {:noreply, [ip], state}
      end
    end
  end

  defp try_apply(ip, {module, function, opts}) do
    struct = struct(module, ip.struct)
    result = apply(module, function, [struct, opts])
    ip_struct = Map.merge(ip.struct, Map.delete(result, :__struct__))
    %{ip | struct: ip_struct}
  rescue
    error ->
      %{
        ip
        | error: %Flowex.PipeError{
            error: error,
            message: Exception.message(error),
            pipe: {module, function, opts},
            struct: ip.struct
          }
      }
  end
end
