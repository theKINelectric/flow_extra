defmodule Flowex.Sync.GenServer do
  @moduledoc "Sync pipeline runner: one process, the line walked in a call."

  use GenServer

  def start_link(state, opts \\ []) do
    GenServer.start_link(__MODULE__, state, opts)
  end

  @impl true
  def init(opts) do
    {:ok, opts}
  end

  @impl true
  def handle_call(ip, _from, {pipeline_module, opts}) do
    result = do_call(ip, {pipeline_module, opts})
    {:reply, result, {pipeline_module, opts}}
  end

  @impl true
  def handle_cast(ip, {pipeline_module, opts}) do
    do_call(ip, {pipeline_module, opts})
    {:noreply, {pipeline_module, opts}}
  end

  defp do_call(ip, {pipeline_module, opts}) do
    (pipeline_module.pipes() ++ [pipeline_module.error_pipe()])
    |> Enum.reduce(ip, fn pipe, ip ->
      process(pipe, ip, pipeline_module, opts)
    end)
  end

  defp try_apply(ip, {module, function, pipe_opts}) do
    # The cast law (engine parity with Flowex.Stage): every module sees its
    # own struct at the pipe boundary, never the accumulated raw map.
    struct = struct(module, ip.struct)
    result = apply(module, function, [struct, pipe_opts])
    %{ip | struct: Map.merge(ip.struct, Map.delete(result, :__struct__))}
  rescue
    error ->
      error_struct = %Flowex.PipeError{
        error: error,
        message: Exception.message(error),
        pipe: {module, function, pipe_opts},
        struct: ip.struct
      }

      %{ip | error: error_struct}
  end

  defp process(pipe, ip, pipeline_module, opts) do
    {atom, _count, pipe_opts, type} = pipe

    if ip.error do
      do_process_error(ip, pipeline_module, atom, {opts, pipe_opts}, type)
    else
      do_process(ip, pipeline_module, atom, {opts, pipe_opts})
    end
  end

  defp do_process(ip, pipeline_module, atom, {opts, pipe_opts}) do
    pipe_opts = Map.merge(Enum.into(opts, %{}), Enum.into(pipe_opts, %{}))

    case Atom.to_charlist(atom) do
      ~c"Elixir." ++ _ ->
        pipe_opts = atom.init(pipe_opts)
        Flowex.Pipeline.validate_module_init!(atom, pipe_opts)
        try_apply(ip, {atom, :call, pipe_opts})

      _ ->
        try_apply(ip, {pipeline_module, atom, pipe_opts})
    end
  end

  defp do_process_error(ip, pipeline_module, atom, {opts, pipe_opts}, :error_pipe) do
    pipe_opts = Map.merge(Enum.into(opts, %{}), Enum.into(pipe_opts, %{}))

    result =
      case Atom.to_charlist(atom) do
        ~c"Elixir." ++ _ ->
          pipe_opts = atom.init(pipe_opts)
          Flowex.Pipeline.validate_module_init!(atom, pipe_opts)
          struct = struct(atom, ip.struct)
          atom.call(ip.error, struct, pipe_opts)

        _ ->
          struct = struct(pipeline_module, ip.struct)
          apply(pipeline_module, atom, [ip.error, struct, pipe_opts])
      end

    %{ip | struct: Map.merge(ip.struct, Map.delete(result, :__struct__))}
  end

  defp do_process_error(ip, _pipeline_module, _atom, _opts, :pipe), do: ip
end
