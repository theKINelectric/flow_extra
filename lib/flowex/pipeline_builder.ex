defmodule Flowex.PipelineBuilder do
  @moduledoc "Defines functions to start and to stop a pipeline"

  # Admission ceiling: a pipe's `count` is per-pipeline-process replication —
  # a runaway count builds runaway topology before any data flows. 100 gives
  # the documented use (count: 10) two orders of headroom and still refuses
  # the typos (count: 1000) loudly.
  @max_count 100

  @doc """
  Prepares the declared stages (FX-007): validates counts and prepares each
  stage's options — a module stage's `init/1` runs here, in the starting
  caller, once per declared occurrence. The prepared list is what the sync
  engine executes; the async engine prepares per replica through the same
  helpers. Prepared options are reused for every request and for
  supervisor-driven restarts; initialization never runs per packet.
  """
  @spec prepare_stages(module(), map() | keyword()) :: [Flowex.StageOpts.t()]
  def prepare_stages(pipeline_module, opts) do
    Flowex.Pipeline.validate_opts!(pipeline_module, opts)

    (pipeline_module.pipes() ++ [pipeline_module.error_pipe()])
    |> Enum.map(fn {atom, count, pipe_opts, type} ->
      validate_count!(pipeline_module, atom, count)
      prepare_stage(pipeline_module, atom, pipe_opts, type, opts)
    end)
  end

  defp prepare_stage(pipeline_module, atom, pipe_opts, type, opts) do
    merged = merge_opts(opts, pipe_opts)

    case stage_kind(atom) do
      :module ->
        %Flowex.StageOpts{
          type: type,
          module: atom,
          function: :call,
          opts: prepare_module_opts(atom, merged)
        }

      :function ->
        %Flowex.StageOpts{type: type, module: pipeline_module, function: atom, opts: merged}
    end
  end

  defp stage_kind(atom) do
    case Atom.to_charlist(atom) do
      ~c"Elixir." ++ _ -> :module
      _ -> :function
    end
  end

  defp merge_opts(opts, pipe_opts), do: Map.merge(Enum.into(opts, %{}), Enum.into(pipe_opts, %{}))

  defp prepare_module_opts(module, opts) do
    opts = module.init(opts)
    Flowex.Pipeline.validate_module_init!(module, opts)
    opts
  end

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
    pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name, pid)
  end

  defp build_children(pipeline_module, opts) do
    Flowex.Pipeline.validate_opts!(pipeline_module, opts)

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

  defp pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name, parent \\ nil) do
    %Flowex.Pipeline{
      module: pipeline_module,
      in_name: producer_name,
      out_name: consumer_name,
      sup_name: sup_name,
      parent: parent
    }
  end

  defp init_pipes({producer_spec, producer_name}, {pipeline_module, ref, opts}) do
    (pipeline_module.pipes() ++ [pipeline_module.error_pipe()])
    |> Enum.reduce({[producer_spec], [producer_name]}, fn {atom, count, pipe_opts, type},
                                                          {wss, prev_names} ->
      validate_count!(pipeline_module, atom, count)
      merged = merge_opts(opts, pipe_opts)

      # The replica rule: each of a stage's count replicas runs module
      # init/1 for its own options, at build time, in the starting caller.
      list =
        Enum.map(1..count, fn _i ->
          init_pipe({pipeline_module, ref, merged}, {atom, type}, prev_names)
        end)

      {new_wss, names} = Enum.unzip(list)
      {wss ++ new_wss, names}
    end)
  end

  defp validate_count!(pipeline_module, atom, count) do
    if is_integer(count) and count >= 1 and count <= @max_count do
      count
    else
      raise ArgumentError,
            "pipe #{inspect(atom)} in pipeline #{inspect(pipeline_module)} " <>
              "declared with count #{inspect(count)} — count must be a positive integer " <>
              "no greater than #{@max_count}"
    end
  end

  def init_pipe({pipeline_module, ref, opts}, {atom, type}, prev_names) do
    case stage_kind(atom) do
      :module -> init_module_pipe({type, pipeline_module, ref, atom, opts}, prev_names)
      :function -> init_function_pipe({type, pipeline_module, ref, atom, opts}, prev_names)
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
    opts = prepare_module_opts(module, opts)

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
