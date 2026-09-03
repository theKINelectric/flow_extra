defmodule Flowex.Sync.Pipeline do
  @moduledoc "Sync pipeline behaviour — one GenServer walks the pipe line."

  defmacro __using__(_args) do
    quote do
      import Flowex.Pipeline

      Module.register_attribute(__MODULE__, :pipes, accumulate: true)
      Module.register_attribute(__MODULE__, :error_pipe, accumulate: false)
      @error_pipe {:handle_error, 1, [], :error_pipe}

      @before_compile Flowex.Sync.Pipeline

      def init(opts), do: opts
      defoverridable init: 1

      def start(opts \\ %{}) do
        opts = init(opts)
        ref = make_ref()
        name = supervisor_name(__MODULE__, ref)
        {:ok, sup_pid} = Flowex.Sync.Supervisor.start_link(__MODULE__, ref, name, opts)
        do_start(sup_pid, name)
      end

      def stop(pipeline) do
        Flowex.Names.stop_pipeline(pipeline)
      end

      def supervised_start(pid, opts \\ %{}) do
        ref = make_ref()
        name = supervisor_name(__MODULE__, ref)

        sup_spec = %{
          id: name,
          start: {Flowex.Sync.Supervisor, :start_link, [__MODULE__, ref, name, opts]},
          restart: :permanent,
          type: :supervisor
        }

        {:ok, sup_pid} = Supervisor.start_child(pid, sup_spec)
        do_start(sup_pid, name)
      end

      defp do_start(sup_pid, name) do
        [{gen_server_name, _prod, :worker, [Flowex.Sync.GenServer]}] =
          Supervisor.which_children(sup_pid)

        %Flowex.Pipeline{
          in_name: gen_server_name,
          module: __MODULE__,
          out_name: gen_server_name,
          sup_name: name
        }
      end

      defp supervisor_name(pipeline_module, ref) do
        Flowex.Names.via(pipeline_module, ref, :sync_supervisor)
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
            pipeline = %Flowex.Pipeline{in_name: in_name, out_name: out_name},
            struct = %__MODULE__{},
            timeout \\ 5_000
          ) do
        ip = %Flowex.IP{struct: Map.delete(struct, :__struct__)}
        ip = GenServer.call(in_name, ip, timeout)
        struct(%__MODULE__{}, ip.struct)
      end

      def cast(
            pipeline = %Flowex.Pipeline{in_name: in_name, out_name: out_name},
            struct = %__MODULE__{}
          ) do
        ip = %Flowex.IP{struct: Map.delete(struct, :__struct__)}
        GenServer.cast(in_name, ip)
      end
    end
  end
end
