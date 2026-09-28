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
  The call prelude, shared by every pipeline module: arms the revocable
  reply alias (FX-005), then hands the WHOLE packet to the admission
  owner (FX-001) — reservation and forwarding are one transaction inside
  the owner, so no caller death can strand a reserved permit without its
  packet, and the submission's settling wait is bounded by this call's
  own deadline (one budget, never reset, checked again at dequeue). The
  consumer incarnation the owner forwarded to is returned for the
  caller's monitor: one incarnation for monitor and submission. Every
  refusal path revokes the alias before it raises — cleanup is
  guaranteed on exceptional exits.
  """
  @spec prepare_call(
          Flowex.Pipeline.t(),
          term(),
          Flowex.IP.t(),
          integer() | nil,
          reference()
        ) :: {pid(), reference()}
  def prepare_call(pipeline, owner_name, ip, deadline, reply_alias) do
    case Flowex.Admission.submit(owner_name, ip, deadline) do
      {:ok, consumer_pid} ->
        {consumer_pid, Process.monitor(consumer_pid)}

      {:error, reason} ->
        Process.unalias(reply_alias)
        refuse!(pipeline, reason)
    end
  end

  # The unacknowledged outcome (see Flowex.Admission.submit/3) is its
  # own reason — uncertain, never spelled as a refusal — and carries the
  # request identity into the raise.
  defp refuse!(pipeline, {:unacknowledged, request_ref}) when is_reference(request_ref) do
    raise Flowex.AdmissionError,
      pipeline: pipeline,
      reason: :unacknowledged,
      request_ref: request_ref
  end

  defp refuse!(pipeline, :deadline),
    do: raise(Flowex.PipelineError, pipeline: pipeline, reason: :timeout)

  defp refuse!(pipeline, :noprocess),
    do: raise(Flowex.PipelineError, pipeline: pipeline, reason: :noprocess)

  defp refuse!(pipeline, reason),
    do: raise(Flowex.AdmissionError, pipeline: pipeline, reason: reason)

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
            pipeline = %Flowex.Pipeline{owner_name: owner_name},
            struct = %__MODULE__{},
            timeout \\ 5_000
          ) do
        pid = self()
        deadline = Flowex.Pipeline.deadline(timeout)
        ip_ref = make_ref()
        reply_alias = Process.alias()

        # The whole packet — alias destination and deadline included — is
        # submitted to the owner as one transaction; the permit is held
        # from reservation to the consumer's terminal release, past this
        # caller's own timeout.
        ip = %Flowex.IP{
          struct: Map.delete(struct, :__struct__),
          requester: pid,
          ref: ip_ref,
          reply_to: reply_alias,
          deadline: deadline
        }

        {consumer_pid, monitor_ref} =
          Flowex.Pipeline.prepare_call(pipeline, owner_name, ip, deadline, reply_alias)

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
            # Revoke FIRST (FX-005 closure): from this line on the alias
            # accepts no further delivery, closing the window in which a
            # reply could land after the check but before the revocation
            # and rot unread. A reply that already beat the clock is in
            # the mailbox; the selective check below finds it.
            Process.demonitor(monitor_ref, [:flush])
            Process.unalias(reply_alias)
            finish_timeout(pid, ip_ref, pipeline)
        end
      end
    end
  end

  defp finish_timeout_def do
    quote do
      defp finish_timeout(pid, ip_ref, pipeline) do
        # The alias is revoked, so nothing new can arrive: check for
        # exactly this request's already-delivered answer without
        # consuming anything else.
        receive do
          %Flowex.IP{requester: ^pid, ref: ^ip_ref} = ip ->
            Flowex.Pipeline.unwrap!(ip, pipeline)
            struct(%__MODULE__{}, ip.struct)
        after
          0 ->
            raise Flowex.PipelineError, pipeline: pipeline, reason: :timeout
        end
      end
    end
  end

  defp cast_def do
    quote do
      def cast(pipeline = %Flowex.Pipeline{owner_name: owner_name}, struct = %__MODULE__{}) do
        # Fire-and-forget: no reply destination, no deadline — but the
        # packet and its permit are submitted to the owner as one
        # transaction (FX-001): :ok MEANS accepted-and-accounted-for.
        # Refusals are the owner's own answers, and an outcome the caller
        # learned only by its acknowledgment timing out keeps its own
        # name and its request identity: {:error, {:unacknowledged, ref}}
        # — see Flowex.Admission.submit/3 for what each shape lets the
        # caller infer.
        ip = %Flowex.IP{
          struct: Map.delete(struct, :__struct__),
          requester: nil,
          ref: make_ref()
        }

        case Flowex.Admission.submit(owner_name, ip, nil) do
          {:ok, _consumer_pid} ->
            :ok

          {:error, reason} ->
            {:error, reason}
        end
      end
    end
  end
end
