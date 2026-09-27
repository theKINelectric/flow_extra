defmodule Flowex.Pipeline do
  @moduledoc """
  Defines macros for pipeline creating.

  ## Initialization and options

  Two `init/1` callbacks run when a pipeline starts, both **in the starting
  caller, before any worker process exists** — never per request:

  * the pipeline module's own `init/1` receives the options passed to
    `start/1`/`supervised_start/2` and returns the pipeline options;
  * each module pipe's `init/1` receives its options (pipeline options
    merged with the pipe's declared `opts:`) and returns the options that
    pipe's `call/2,3` sees. On the asynchronous engine it runs **once per
    declared replica** (`count: 3` prepares three times, one per stage
    process); the synchronous engine executes one representative per stage
    and prepares **once per declared occurrence**.

  `init/1` is configuration preparation: it must return a map (module
  pipes) or a map/keyword list (pipeline), validated before any topology
  starts, and it is **not** a worker-resource lifecycle callback — use a
  process-owned mechanism for resources that must be recreated with each
  worker. Supervisor-driven restarts reuse the prepared options; only a
  new explicit start runs `init/1` again.
  """

  @type t :: %__MODULE__{
          module: module(),
          in_name: term(),
          out_name: term(),
          sup_name: term(),
          parent: pid() | nil,
          owner_name: term() | nil
        }

  defstruct module: nil,
            in_name: nil,
            out_name: nil,
            sup_name: nil,
            parent: nil,
            owner_name: nil

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
  The call prelude, shared by every pipeline module: resolves the consumer
  once, arms the monitor and the revocable reply alias (FX-005), and takes
  the admission permit BEFORE any packet exists (FX-001) — an overloaded
  pipeline refuses here, observably, with no work begun.
  """
  @spec prepare_call(Flowex.Pipeline.t(), term(), term(), reference()) ::
          {pid(), reference(), reference()}
  def prepare_call(pipeline, out_name, owner_name, ip_ref) do
    consumer_pid = resolve_consumer!(out_name, pipeline)
    monitor_ref = Process.monitor(consumer_pid)
    reply_alias = Process.alias()

    case Flowex.Admission.admit(owner_name, ip_ref) do
      {:ok, _generation} ->
        {consumer_pid, monitor_ref, reply_alias}

      {:error, reason} ->
        Process.demonitor(monitor_ref, [:flush])
        Process.unalias(reply_alias)
        raise Flowex.AdmissionError, pipeline: pipeline, reason: reason
    end
  end

  @doc """
  Resolves the consumer through the registry — the ONE resolution a call
  makes (FX-005): the monitor and the submission both target this PID, so
  a consumer restart mid-call cannot pair one incarnation's monitor with
  another's work.

  A consumer name that is absent while the pipeline's own supervisor still
  exists means a restart is settling; the resolution waits it out briefly
  so an honest restart surfaces as service and not as :noprocess. A
  pipeline whose supervisor is gone refuses immediately.
  """
  @spec resolve_consumer!(term(), Flowex.Pipeline.t()) :: pid()
  def resolve_consumer!(out_name, pipeline = %Flowex.Pipeline{sup_name: sup_name}) do
    case GenServer.whereis(out_name) do
      nil ->
        if GenServer.whereis(sup_name) == nil do
          raise Flowex.PipelineError, pipeline: pipeline, reason: :noprocess
        else
          settle(out_name, sup_name, pipeline, System.monotonic_time(:millisecond) + 250)
        end

      pid ->
        pid
    end
  end

  defp settle(out_name, sup_name, pipeline, limit) do
    case GenServer.whereis(out_name) do
      nil ->
        if System.monotonic_time(:millisecond) > limit do
          raise Flowex.PipelineError, pipeline: pipeline, reason: :noprocess
        else
          Process.sleep(2)
          settle(out_name, sup_name, pipeline, limit)
        end

      pid ->
        pid
    end
  end

  @doc """
  The absolute local monotonic deadline (milliseconds) for a call timeout;
  `:infinity` maps to `nil` — no deadline. Local to one BEAM node.
  """
  @spec deadline(timeout()) :: integer() | nil
  def deadline(:infinity), do: nil

  def deadline(timeout) when is_integer(timeout),
    do: System.monotonic_time(:millisecond) + timeout

  @doc "Remaining budget for an absolute deadline — `:infinity` when there is none."
  @spec remaining(integer() | nil) :: timeout()
  def remaining(nil), do: :infinity

  def remaining(deadline),
    do: max(0, deadline - System.monotonic_time(:millisecond))

  @doc "Whether a packet's deadline has passed (`nil` never expires)."
  @spec expired?(integer() | nil) :: boolean()
  def expired?(nil), do: false

  def expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  @doc """
  Delivers a finished packet to the caller: a packet the pipeline itself
  expired raises the caller's own timeout — the caller cannot tell (and
  need not) whether its wait or the pipeline noticed the deadline first.
  """
  @spec unwrap!(Flowex.IP.t(), Flowex.Pipeline.t()) :: :ok
  def unwrap!(%Flowex.IP{expired: true}, pipeline) do
    raise Flowex.PipelineError, pipeline: pipeline, reason: :timeout
  end

  def unwrap!(_ip, _pipeline), do: :ok

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

      unquote(call_def())
      unquote(wait_response_def())
      unquote(finish_timeout_def())
      unquote(cast_def())
    end
  end

  defp call_def do
    quote do
      def call(
            pipeline = %Flowex.Pipeline{
              in_name: in_name,
              out_name: out_name,
              owner_name: owner_name
            },
            struct = %__MODULE__{},
            timeout \\ 5_000
          ) do
        pid = self()
        deadline = Flowex.Pipeline.deadline(timeout)
        ip_ref = make_ref()

        # One consumer incarnation for monitor and submission, a revocable
        # reply alias, and admission before forwarding — the permit is
        # reserved before the packet exists anywhere and held until the
        # consumer releases it, past this caller's own timeout.
        {consumer_pid, monitor_ref, reply_alias} =
          Flowex.Pipeline.prepare_call(pipeline, out_name, owner_name, ip_ref)

        ip = %Flowex.IP{
          struct: Map.delete(struct, :__struct__),
          requester: pid,
          ref: ip_ref,
          reply_to: reply_alias,
          deadline: deadline
        }

        GenServer.cast(consumer_pid, {in_name, ip})
        wait_response(pid, monitor_ref, ip_ref, reply_alias, pipeline, deadline)
      end
    end
  end

  defp wait_response_def do
    quote do
      # The three ways a call ends — the pipeline answers, the crash
      # cascade does (a stage dies, the rest_for_one supervisor tears the
      # line down, the consumer's death trips the monitor), or the budget
      # does. On every ending the alias is revoked and the monitor removed;
      # a reply that already beat the clock WINS over the deadline (an
      # arrived answer is an answer), and anything later is dropped by the
      # revoked alias.
      defp wait_response(pid, monitor_ref, ip_ref, reply_alias, pipeline, deadline) do
        receive do
          %Flowex.IP{requester: ^pid, ref: ^ip_ref} = ip ->
            Process.demonitor(monitor_ref, [:flush])
            Process.unalias(reply_alias)
            Flowex.Pipeline.unwrap!(ip, pipeline)
            struct(%__MODULE__{}, ip.struct)

          {:DOWN, ^monitor_ref, _, _, reason} ->
            Process.unalias(reply_alias)
            raise Flowex.PipelineError, pipeline: pipeline, reason: reason
        after
          Flowex.Pipeline.remaining(deadline) ->
            Process.demonitor(monitor_ref, [:flush])
            finish_timeout(pid, ip_ref, reply_alias, pipeline)
        end
      end
    end
  end

  defp finish_timeout_def do
    quote do
      defp finish_timeout(pid, ip_ref, reply_alias, pipeline) do
        # The reply may have beaten the clock to the mailbox: check for
        # exactly this request's answer without consuming anything else.
        receive do
          %Flowex.IP{requester: ^pid, ref: ^ip_ref} = ip ->
            Process.unalias(reply_alias)
            Flowex.Pipeline.unwrap!(ip, pipeline)
            struct(%__MODULE__{}, ip.struct)
        after
          0 ->
            Process.unalias(reply_alias)
            raise Flowex.PipelineError, pipeline: pipeline, reason: :timeout
        end
      end
    end
  end

  defp cast_def do
    quote do
      def cast(
            pipeline = %Flowex.Pipeline{
              in_name: in_name,
              out_name: out_name,
              owner_name: owner_name
            },
            struct = %__MODULE__{}
          ) do
        # Fire-and-forget: no reply destination, no deadline — but still
        # admitted (FX-001): :ok now MEANS accepted-and-accounted-for. An
        # overloaded pipeline answers {:error, :overloaded} instead of
        # silently discarding the work later.
        ip = %Flowex.IP{
          struct: Map.delete(struct, :__struct__),
          requester: nil,
          ref: make_ref()
        }

        case Flowex.Admission.admit(owner_name, ip.ref) do
          {:ok, _generation} ->
            GenServer.cast(out_name, {in_name, ip})
            :ok

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end
end
