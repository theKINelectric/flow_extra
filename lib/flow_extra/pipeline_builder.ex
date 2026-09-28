defmodule FlowExtra.PipelineBuilder do
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
  @spec prepare_stages(module(), map() | keyword()) :: [FlowExtra.StageOpts.t()]
  def prepare_stages(pipeline_module, opts) do
    FlowExtra.Pipeline.validate_opts!(pipeline_module, opts)

    declarations = pipeline_module.pipes() ++ [pipeline_module.error_pipe()]

    # Validate the whole declaration before any module initializer runs, so
    # a refused start leaves no half-prepared side effects behind.
    Enum.each(declarations, fn {atom, count, _pipe_opts, _type} ->
      validate_count!(pipeline_module, atom, count)
    end)

    Enum.map(declarations, fn {atom, _count, pipe_opts, type} ->
      prepare_stage(pipeline_module, atom, pipe_opts, type, opts)
    end)
  end

  defp prepare_stage(pipeline_module, atom, pipe_opts, type, opts) do
    merged = merge_opts(opts, pipe_opts)

    case stage_kind(atom) do
      :module ->
        %FlowExtra.StageOpts{
          type: type,
          module: atom,
          function: :call,
          opts: prepare_module_opts(atom, merged)
        }

      :function ->
        %FlowExtra.StageOpts{type: type, module: pipeline_module, function: atom, opts: merged}
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
    FlowExtra.Pipeline.validate_module_init!(module, opts)
    opts
  end

  # Default admitted-work capacity (FX-001): queued + executing packets
  # together, deliberately far below GenStage's 10_000-event buffer.
  @default_capacity 100

  @spec start(module(), map()) :: FlowExtra.Pipeline.t()
  def start(pipeline_module, opts) do
    {producer_name, consumer_name, wrapper_specs, ref, owner_name} =
      build_children(pipeline_module, opts)

    sup_name = supervisor_name(pipeline_module, ref)
    {:ok, _sup_pid} = FlowExtra.Supervisor.start_link(wrapper_specs, sup_name)

    pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name, nil, owner_name)
  end

  @spec supervised_start(module(), pid(), map()) :: FlowExtra.Pipeline.t()
  def supervised_start(pipeline_module, pid, opts) do
    {producer_name, consumer_name, wrapper_specs, ref, owner_name} =
      build_children(pipeline_module, opts)

    sup_name = supervisor_name(pipeline_module, ref)

    sup_spec = %{
      id: sup_name,
      start: {FlowExtra.Supervisor, :start_link, [wrapper_specs, sup_name]},
      restart: :permanent,
      type: :supervisor
    }

    {:ok, _sup_pid} = Supervisor.start_child(pid, sup_spec)
    pipeline_struct(pipeline_module, producer_name, consumer_name, sup_name, pid, owner_name)
  end

  defp build_children(pipeline_module, opts) do
    FlowExtra.Pipeline.validate_opts!(pipeline_module, opts)

    capacity = capacity!(pipeline_module, opts)
    ref = make_ref()
    producer_name = producer_name(pipeline_module, ref)
    owner_name = FlowExtra.Names.via(pipeline_module, ref, :admission_owner)
    line_name = FlowExtra.Names.via(pipeline_module, ref, :line)

    producer_spec = %{
      id: producer_name,
      start: {FlowExtra.Producer, :start_link, [nil, [name: producer_name]]}
    }

    {stage_specs, last_names} =
      init_pipes({producer_spec, producer_name}, {pipeline_module, ref, opts})

    consumer_name = consumer_name(pipeline_module, ref)

    consumer_worker_spec = %{
      id: consumer_name,
      start: {FlowExtra.Consumer, :start_link, [last_names, owner_name, [name: consumer_name]]}
    }

    # The line: producer, stages, consumer under the pipeline's own
    # rest_for_one supervisor, exactly as before (init_pipes' accumulator
    # already carries the producer spec at its head).
    line_specs = stage_specs ++ [consumer_worker_spec]
    worker_names = Enum.map(line_specs, & &1.id)

    # The wrapper (FX-001, C design record): the admission owner FIRST, the
    # line second, under rest_for_one — the owner's death tears the whole
    # line down before a fresh owner can reopen capacity, and any line
    # worker's death quiesces the owner's generation. The owner carries
    # the ingress pair so a submission's reservation and its forwarding
    # are one transaction inside it.
    owner_spec = %{
      id: owner_name,
      start:
        {FlowExtra.Admission, :start_link,
         [capacity, worker_names, {producer_name, consumer_name}, owner_name]}
    }

    line_sup_spec = %{
      id: line_name,
      start: {FlowExtra.Supervisor, :start_link, [line_specs, line_name]},
      type: :supervisor
    }

    {producer_name, consumer_name, [owner_spec, line_sup_spec], ref, owner_name}
  end

  defp capacity!(pipeline_module, opts) do
    opts = Enum.into(opts, %{})
    capacity = Map.get(opts, :admission_capacity, @default_capacity)

    if is_integer(capacity) and capacity >= 1 do
      capacity
    else
      raise ArgumentError,
            "pipeline #{inspect(pipeline_module)} declared admission_capacity " <>
              "#{inspect(capacity)} — capacity must be a positive integer"
    end
  end

  defp supervisor_name(pipeline_module, ref),
    do: FlowExtra.Names.via(pipeline_module, ref, :supervisor)

  defp producer_name(pipeline_module, ref),
    do: FlowExtra.Names.via(pipeline_module, ref, :producer)

  defp consumer_name(pipeline_module, ref),
    do: FlowExtra.Names.via(pipeline_module, ref, :consumer)

  defp pipeline_struct(
         pipeline_module,
         producer_name,
         consumer_name,
         sup_name,
         parent,
         owner_name
       ) do
    %FlowExtra.Pipeline{
      module: pipeline_module,
      in_name: producer_name,
      out_name: consumer_name,
      sup_name: sup_name,
      parent: parent,
      owner_name: owner_name
    }
  end

  defp init_pipes({producer_spec, producer_name}, {pipeline_module, ref, opts}) do
    declarations = pipeline_module.pipes() ++ [pipeline_module.error_pipe()]

    Enum.each(declarations, fn {atom, count, _pipe_opts, _type} ->
      validate_count!(pipeline_module, atom, count)
    end)

    Enum.reduce(declarations, {[producer_spec], [producer_name]}, fn {atom, count, pipe_opts,
                                                                      type},
                                                                     {wss, prev_names} ->
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
    name = FlowExtra.Names.via(pipeline_module, ref, {:function_stage, make_ref()})

    opts = %FlowExtra.StageOpts{
      type: type,
      module: pipeline_module,
      function: function,
      opts: opts,
      name: name,
      producer_names: prev_names
    }

    worker_spec = %{id: name, start: {FlowExtra.Stage, :start_link, [opts, [name: name]]}}
    {worker_spec, name}
  end

  defp init_module_pipe({type, pipeline_module, ref, module, opts}, prev_names) do
    opts = prepare_module_opts(module, opts)

    name = FlowExtra.Names.via(pipeline_module, ref, {:module_stage, make_ref()})

    opts = %FlowExtra.StageOpts{
      type: type,
      module: module,
      function: :call,
      opts: opts,
      name: name,
      producer_names: prev_names
    }

    worker_spec = %{id: name, start: {FlowExtra.Stage, :start_link, [opts, [name: name]]}}
    {worker_spec, name}
  end
end
