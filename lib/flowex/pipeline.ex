defmodule Flowex.Pipeline do
  @moduledoc "Defines macros for pipeline creating"

  @type t :: %__MODULE__{
          module: module(),
          in_name: term(),
          out_name: term(),
          sup_name: term()
        }

  defstruct module: nil, in_name: nil, out_name: nil, sup_name: nil

  defmacro pipe(atom, options \\ [opts: [], count: 1]) do
    count = options[:count] || 1
    opts = options[:opts] || []

    quote do
      @pipes {unquote(atom), unquote(count), unquote(opts), :pipe}
    end
  end

  defmacro error_pipe(atom, options \\ [opts: [], count: 1]) do
    count = options[:count] || 1
    opts = options[:opts] || []

    quote do
      @error_pipe {unquote(atom), unquote(count), unquote(opts), :error_pipe}
    end
  end

  defmacro __using__(_args) do
    quote do
      import Flowex.Pipeline
      alias Flowex.PipelineBuilder

      Module.register_attribute(__MODULE__, :pipes, accumulate: true)
      Module.register_attribute(__MODULE__, :error_pipe, accumulate: false)
      @error_pipe {:handle_error, 1, [], :error_pipe}

      @before_compile Flowex.Pipeline

      def init(opts), do: opts

      defoverridable init: 1

      def start(opts \\ %{}) do
        opts = init(opts)
        PipelineBuilder.start(__MODULE__, opts)
      end

      # One admission law, both doors (FX-002): init/1 runs exactly once, in
      # the caller, whichever way the pipeline enters the world. The prepared
      # opts are baked into the child specs, so a supervisor-driven restart
      # reuses them — init does not run again on restart.
      def supervised_start(pid, opts \\ %{}) do
        opts = init(opts)
        PipelineBuilder.supervised_start(__MODULE__, pid, opts)
      end

      def stop(pipeline) do
        Flowex.Names.stop_pipeline(pipeline)
      end

      def handle_error(error, _struct, _opts) do
        raise error
      end

      defoverridable handle_error: 3
    end
  end

  @doc """
  Resolves the consumer through the registry and monitors it. Used by the
  generated `call/2` — kept here so the macro's generated code stays lean.
  """
  @spec monitor_consumer!(term(), Flowex.Pipeline.t()) :: reference()
  def monitor_consumer!(out_name, pipeline) do
    case GenServer.whereis(out_name) do
      nil -> raise Flowex.PipelineError, pipeline: pipeline, message: :noprocess
      pid -> Process.monitor(pid)
    end
  end

  @doc """
  Validates pipeline-level `init/1` output at admission: whatever init
  returns must be a map or keyword list — the forms the pipe walker can turn
  into pipe options. Shared by both engines so the refusal is loud, early,
  and names the culprit.
  """
  @spec validate_opts!(module(), term()) :: map() | keyword()
  def validate_opts!(pipeline_module, opts) do
    if is_map(opts) or Keyword.keyword?(opts) do
      opts
    else
      raise ArgumentError,
            "#{inspect(pipeline_module)}.init/1 must return a map or keyword list, " <>
              "got: #{inspect(opts)}"
    end
  end

  @doc """
  Validates module-pipe `init/1` output: a module pipe's options must be a
  map by the time they reach a stage or the sync walker.
  """
  @spec validate_module_init!(module(), term()) :: :ok
  def validate_module_init!(module, opts) do
    unless is_map(opts) do
      raise ArgumentError,
            "#{inspect(module)}.init/1 must return a map, got: #{inspect(opts)}"
    end

    :ok
  end

  defmacro __before_compile__(_env) do
    quote do
      def pipes, do: Enum.reverse(@pipes)
      def error_pipe, do: @error_pipe

      def pipe_info(name) do
        Enum.find_value(pipes(), fn {atom, count, opts, type} ->
          atom == name && %{name: atom, count: count, opts: opts, type: type}
        end)
      end

      def call(
            pipeline = %Flowex.Pipeline{in_name: in_name, out_name: out_name},
            struct = %__MODULE__{},
            timeout \\ 5_000
          ) do
        pid = self()
        # :erlang.monitor takes pids or local atoms only — no via — so the
        # consumer name is resolved through the registry first. A nil lookup
        # raises immediately, matching what a monitor on a dead name would do.
        monitor_ref = Flowex.Pipeline.monitor_consumer!(out_name, pipeline)

        ip_ref = make_ref()
        ip = %Flowex.IP{struct: Map.delete(struct, :__struct__), requester: pid, ref: ip_ref}

        GenServer.cast(out_name, {in_name, ip})
        wait_response(pid, monitor_ref, ip_ref, pipeline, timeout)
      end

      # The two ways a call ends, and both are the caller's liveness contract:
      # the pipeline answers, or the crash cascade does (a stage dies, the
      # rest_for_one supervisor tears the line down, the consumer's death
      # trips the monitor below). A slow-but-alive pipeline ends neither way —
      # the deadline is the third ending, so no caller waits forever.
      defp wait_response(pid, monitor_ref, ip_ref, pipeline, timeout) do
        receive do
          %Flowex.IP{requester: ^pid, ref: ^ip_ref} = ip ->
            Process.demonitor(monitor_ref, [:flush])
            struct(%__MODULE__{}, ip.struct)

          {:DOWN, ^monitor_ref, _, _, reason} ->
            raise Flowex.PipelineError, pipeline: pipeline, message: reason
        after
          timeout ->
            Process.demonitor(monitor_ref, [:flush])
            raise Flowex.PipelineError, pipeline: pipeline, message: :timeout
        end
      end

      def cast(
            pipeline = %Flowex.Pipeline{in_name: in_name, out_name: out_name},
            struct = %__MODULE__{}
          ) do
        ip = %Flowex.IP{struct: Map.delete(struct, :__struct__), requester: nil}
        GenServer.cast(out_name, {in_name, ip})
      end
    end
  end
end
