defmodule Flowex.PipelineBuilder do
  @moduledoc "Defines functions to start and to stop a pipeline"

  @spec start(module(), map()) :: Flowex.Pipeline.t()
  def start(pipeline_module, opts) do
    {producer_name, consumer_name, all_specs, ref} = build_children(pipeline_module, opts)

    sup_name = supervisor_name(pipeline_module, ref)
    {:ok, _sup_pid} = Flowex.Supervisor.start_link(all_specs, sup_name)

    pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name)
  end

  @spec supervised_start(module(), pid(), map()) :: Flowex.Pipeline.t()
  def supervised_start(pipeline_module, pid, opts) do
    {producer_name, consumer_name, all_specs, ref} = build_children(pipeline_module, opts)

    sup_name = supervisor_name(pipeline_module, ref)

    sup_spec = %{
      id: sup_name,
      start: {Flowex.Supervisor, :start_link, [all_specs, sup_name]},
      restart: :permanent,
      type: :supervisor
    }

    {:ok, _sup_pid} = Supervisor.start_child(pid, sup_spec)
    pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name)
  end

  defp build_children(pipeline_module, opts) do
    ref = make_ref()
    producer_name = producer_name(pipeline_module, ref)

    producer_spec = %{
      id: producer_name,
      start: {Flowex.Producer, :start_link, [nil, [name: producer_name]]}
    }

    {wss, last_names} =
      init_pipes({producer_spec, producer_name}, {pipeline_module, ref, opts})

    consumer_name = consumer_name(pipeline_module, ref)

    consumer_worker_spec = %{
      id: consumer_name,
      start: {Flowex.Consumer, :start_link, [last_names, [name: consumer_name]]}
    }

    {producer_name, consumer_name, wss ++ [consumer_worker_spec], ref}
  end

  defp supervisor_name(pipeline_module, ref),
    do: Flowex.Names.via(pipeline_module, ref, :supervisor)

  defp producer_name(pipeline_module, ref), do: Flowex.Names.via(pipeline_module, ref, :producer)

  defp consumer_name(pipeline_module, ref), do: Flowex.Names.via(pipeline_module, ref, :consumer)

  defp pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name) do
    %Flowex.Pipeline{
      module: pipeline_module,
      in_name: producer_name,
      out_name: consumer_name,
      sup_name: sup_name
    }
  end

  defp init_pipes({producer_spec, producer_name}, {pipeline_module, ref, opts}) do
    (pipeline_module.pipes() ++ [pipeline_module.error_pipe()])
    |> Enum.reduce({[producer_spec], [producer_name]}, fn {atom, count, pipe_opts, type},
                                                          {wss, prev_names} ->
      opts = Map.merge(Enum.into(opts, %{}), Enum.into(pipe_opts, %{}))

      validate_count!(pipeline_module, atom, count)

      list =
        Enum.map(1..count, fn _i ->
          init_pipe({pipeline_module, ref, opts}, {atom, type}, prev_names)
        end)

      {new_wss, names} = Enum.unzip(list)
      {wss ++ new_wss, names}
    end)
  end

  defp validate_count!(pipeline_module, atom, count) do
    if is_integer(count) and count >= 1 do
      count
    else
      raise ArgumentError,
            "pipe #{inspect(atom)} in pipeline #{inspect(pipeline_module)} " <>
              "declared with count #{inspect(count)} — count must be a positive integer"
    end
  end

  def init_pipe({pipeline_module, ref, opts}, {atom, type}, prev_names) do
    case Atom.to_charlist(atom) do
      ~c"Elixir." ++ _ -> init_module_pipe({type, pipeline_module, ref, atom, opts}, prev_names)
      _ -> init_function_pipe({type, pipeline_module, ref, atom, opts}, prev_names)
    end
  end

  defp init_function_pipe({type, pipeline_module, ref, function, opts}, prev_names) do
    name = Flowex.Names.via(pipeline_module, ref, {:function_stage, make_ref()})

    opts = %Flowex.StageOpts{
      type: type,
      module: pipeline_module,
      function: function,
      opts: opts,
      name: name,
      producer_names: prev_names
    }

    worker_spec = %{id: name, start: {Flowex.Stage, :start_link, [opts, [name: name]]}}
    {worker_spec, name}
  end

  defp init_module_pipe({type, pipeline_module, ref, module, opts}, prev_names) do
    opts = module.init(opts)
    name = Flowex.Names.via(pipeline_module, ref, {:module_stage, make_ref()})

    opts = %Flowex.StageOpts{
      type: type,
      module: module,
      function: :call,
      opts: opts,
      name: name,
      producer_names: prev_names
    }

    worker_spec = %{id: name, start: {Flowex.Stage, :start_link, [opts, [name: name]]}}
    {worker_spec, name}
  end
end
