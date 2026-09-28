defmodule FlowExtra.Sync.Pipeline do
  @moduledoc """
  Sync pipeline behaviour — one GenServer walks the pipe line.

  Initialization follows the same contract as `FlowExtra.Pipeline` (see its
  moduledoc): pipeline and module `init/1` run in the starting caller at
  startup — once per declared occurrence here, once per replica on the
  asynchronous engine — and prepared options are reused for every request
  and for restarts.
  """

  defmacro __using__(_args) do
    quote do
      import FlowExtra.Pipeline
      alias FlowExtra.PipelineBuilder

      Module.register_attribute(__MODULE__, :pipes, accumulate: true)
      Module.register_attribute(__MODULE__, :error_pipe, accumulate: false)
      @error_pipe {:handle_error, 1, [], :error_pipe}

      @before_compile FlowExtra.Sync.Pipeline

      def init(opts), do: opts
      defoverridable init: 1

      def start(opts \\ %{}) do
        opts = init(opts)
        prepared = PipelineBuilder.prepare_stages(__MODULE__, opts)

        ref = make_ref()
        name = supervisor_name(__MODULE__, ref)
        {:ok, sup_pid} = FlowExtra.Sync.Supervisor.start_link(__MODULE__, ref, name, prepared)
        do_start(sup_pid, name)
      end

      def stop(pipeline) do
        FlowExtra.Names.stop_pipeline(pipeline)
      end

      # One admission law, both doors (FX-002): init/1 runs exactly once, in
      # the caller, and preparation validates its result there — a raise
      # inside a child's start_link under Supervisor.start_child would
      # surface as {:error, _}, not as the caller's ArgumentError. The
      # prepared stages are baked into the child spec (FX-007), so a restart
      # reuses them — module init/1 never runs per request.
      def supervised_start(pid, opts \\ %{}) do
        opts = init(opts)
        prepared = PipelineBuilder.prepare_stages(__MODULE__, opts)

        ref = make_ref()
        name = supervisor_name(__MODULE__, ref)

        sup_spec = %{
          id: name,
          start: {FlowExtra.Sync.Supervisor, :start_link, [__MODULE__, ref, name, prepared]},
          restart: :permanent,
          type: :supervisor
        }

        {:ok, sup_pid} = Supervisor.start_child(pid, sup_spec)
        do_start(sup_pid, name, pid)
      end

      defp do_start(sup_pid, name, parent \\ nil) do
        [{gen_server_name, _prod, :worker, [FlowExtra.Sync.GenServer]}] =
          Supervisor.which_children(sup_pid)

        %FlowExtra.Pipeline{
          in_name: gen_server_name,
          module: __MODULE__,
          out_name: gen_server_name,
          sup_name: name,
          parent: parent
        }
      end

      defp supervisor_name(pipeline_module, ref) do
        FlowExtra.Names.via(pipeline_module, ref, :sync_supervisor)
      end

      def handle_error(error, _struct, _opts) do
        raise error
      end
    end
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
            pipeline = %FlowExtra.Pipeline{in_name: in_name},
            struct = %__MODULE__{},
            timeout \\ 5_000
          ) do
        # One deadline (FX-005): the packet carries it, the wait derives from
        # it, and a packet that expired in the mailbox queue raises the
        # caller's own timeout without running a single callback.
        deadline = FlowExtra.Pipeline.deadline(timeout)

        ip = %FlowExtra.IP{struct: Map.delete(struct, :__struct__), deadline: deadline}
        ip = GenServer.call(in_name, ip, FlowExtra.Pipeline.remaining(deadline))
        FlowExtra.Pipeline.unwrap!(ip, pipeline)
        struct(%__MODULE__{}, ip.struct)
      end

      def cast(%FlowExtra.Pipeline{in_name: in_name}, struct = %__MODULE__{}) do
        # Fire-and-forget: no deadline.
        ip = %FlowExtra.IP{struct: Map.delete(struct, :__struct__)}
        GenServer.cast(in_name, ip)
      end
    end
  end
end
