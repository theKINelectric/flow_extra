defmodule Flowex.Admission do
  @moduledoc """
  The admission owner (FX-001): one process outside the replaceable stage
  subtree, holding its pipeline generation's admitted-work ledger.

  The contract, in brief — a submission is one transaction: the owner
  reserves the permit AND forwards the packet (`submit/3`), so no caller
  death can strand a reservation without its packet. A submission either
  acquires a permit (the work WILL be accounted to a terminal outcome) or
  is refused immediately and observably. Permits are held for the work's
  whole lifetime: a caller's timeout or death releases nothing, and a
  topology failure RETAINS its generation's permits — surviving work
  keeps executing and keeps its capacity until its own terminal release,
  while work destroyed with the topology stays admitted, outcome unknown.
  Nothing is invented into success or failure, and no slot is reused
  under surviving work. Every admitted job reaches exactly one terminal
  outcome. See `docs/research/flowex/C-admission-design.md`.

  The owner monitors every line worker. Any worker death quiesces the
  generation; because the wrapper supervisor is `:rest_for_one` with the
  owner FIRST, the owner's own death restarts the whole line too — a
  lost ledger can never overlap still-executing old work.
  """

  use GenServer

  # How long a submission keeps retrying while the topology settles after
  # a failure — ONE fixed budget per attempt, never reset by a retry, and
  # further bounded by the caller's own deadline when it carries one.
  # Capacity refusals are never retried.
  @settle_grace 250
  @attach_step 5

  @type outcome :: :succeeded | :recovered | :expired | :failed | :cancelled | :unknown

  @spec start_link(pos_integer(), [term()], {term(), term()} | nil, GenServer.name()) ::
          GenServer.on_start()
  def start_link(capacity, worker_names, ingress, name) do
    Flowex.Names.await_free!(name)
    GenServer.start_link(__MODULE__, {capacity, worker_names, ingress}, name: name)
  end

  @doc """
  Submits `ip` as one transaction: the permit is reserved and the packet
  forwarded to the pipeline's ingress inside the owner, so the reservation
  can never exist without its packet. Returns the consumer incarnation the
  packet was forwarded to — one incarnation for the caller's monitor and
  for the submission itself.

  While the topology settles, the attempt retries against ONE fixed
  budget: the caller's `deadline` when given (the call's clock is the
  admission's clock), capped by the settling grace. `{:error, :deadline}`
  means the caller's own budget ran out; `{:error, :unavailable}` means
  the grace did; `{:error, :overloaded}` is immediate — capacity, no
  retry; `{:error, :noprocess}` means the owner (and with it the line)
  is gone.
  """
  @spec submit(GenServer.name(), Flowex.IP.t(), integer() | nil) ::
          {:ok, pid()}
          | {:error, :overloaded | :unavailable | :deadline | :noprocess | :caller_dead}
  def submit(owner, ip, deadline \\ nil) do
    attempt(owner, {:submit, ip}, deadline)
  end

  @doc """
  Reserves a permit for `id` without a packet — a diagnostic for ledger
  and reconciliation probes; the engine paths use `submit/3`. Refusal
  semantics as `submit/3`, with the settling grace as the whole budget.
  """
  @spec admit(GenServer.name(), reference()) ::
          {:ok, integer()} | {:error, :overloaded | :unavailable | :noprocess}
  def admit(owner, id) do
    attempt(owner, {:admit, id}, nil)
  end

  @doc """
  Releases a permit with its terminal outcome — exactly once, by ref, in
  any generation: a job admitted before a topology failure still holds
  its permit while it survives, and this is how it frees it. A second
  notice, or one for an unknown ref, is refused as stale.
  """
  @spec release(GenServer.name(), reference(), outcome()) :: :ok | {:error, :stale}
  def release(owner, id, outcome) do
    GenServer.call(owner, {:release, id, outcome})
  end

  @doc "The generation's ledger, for diagnostics and reconciliation tests."
  @spec report(GenServer.name()) :: %{
          required(:generation) => integer(),
          required(:capacity) => pos_integer(),
          required(:active) => non_neg_integer(),
          required(:status) => :open | :settling,
          required(:counts) => %{optional(outcome()) => non_neg_integer()}
        }
  def report(owner) do
    GenServer.call(owner, :report)
  end

  defp attempt(owner, msg, deadline) do
    started = System.monotonic_time(:millisecond)
    grace_end = started + @settle_grace

    if deadline && deadline < grace_end do
      ask(owner, msg, deadline, :deadline)
    else
      ask(owner, msg, grace_end, :unavailable)
    end
  end

  defp ask(owner, msg, budget_end, bound) do
    remaining = budget_end - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error, bound}
    else
      try do
        case GenServer.call(owner, msg, remaining + 1) do
          {:error, :unavailable} ->
            Process.sleep(min(@attach_step, remaining))
            ask(owner, msg, budget_end, bound)

          reply ->
            reply
        end
      catch
        # GenServer.call exits with a wrapped reason on timeout — the
        # caller's budget is spent, not the owner gone.
        :exit, :timeout -> {:error, bound}
        :exit, {:timeout, {GenServer, :call, _}} -> {:error, bound}
        :exit, _gone -> {:error, :noprocess}
      end
    end
  end

  @impl true
  def init({capacity, worker_names}), do: init({capacity, worker_names, nil})

  def init({capacity, worker_names, ingress}) do
    send(self(), :attach)

    {:ok,
     %{
       capacity: capacity,
       ingress: ingress,
       active: %{},
       generation: 1,
       counts: %{
         succeeded: 0,
         recovered: 0,
         expired: 0,
         failed: 0,
         cancelled: 0,
         unknown: 0
       },
       worker_names: worker_names,
       monitors: %{},
       status: :settling
     }}
  end

  @impl true
  def handle_info(:attach, state) do
    pids = state.worker_names |> Enum.map(&GenServer.whereis/1) |> Enum.uniq()

    if pids != [] and Enum.all?(pids, &is_pid/1) do
      monitors = for pid <- pids, into: %{}, do: {Process.monitor(pid), pid}
      {:noreply, %{state | monitors: monitors, status: :open}}
    else
      Process.send_after(self(), :attach, @attach_step)
      {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    case Map.pop(state.monitors, ref) do
      {nil, _monitors} ->
        # A DOWN from a monitor of an already-quiesced generation.
        {:noreply, state}

      {_pid, monitors} ->
        if state.status != :open do
          {:noreply, %{state | monitors: monitors}}
        else
          # Topology failure: the generation quiesces, its permits are
          # RETAINED. Work that survives the failure keeps executing and
          # keeps its capacity until its own terminal release — the
          # reopened generation cannot double-book a slot under it. Work
          # destroyed with the topology stays admitted, outcome unknown:
          # never invented into success or failure, never quietly
          # balanced away.
          Process.send_after(self(), :attach, @attach_step)

          {:noreply,
           %{
             state
             | generation: state.generation + 1,
               monitors: monitors,
               status: :settling
           }}
        end
    end
  end

  @impl true
  def handle_call(:report, _from, state) do
    {:reply,
     %{
       generation: state.generation,
       capacity: state.capacity,
       active: map_size(state.active),
       status: state.status,
       counts: state.counts
     }, state}
  end

  def handle_call({:submit, ip}, {caller, _ref}, state) do
    cond do
      state.status != :open ->
        {:reply, {:error, :unavailable}, state}

      # The caller died before its submission was processed: no packet
      # ever exists on its behalf, so no permit is reserved and nothing
      # strands.
      not Process.alive?(caller) ->
        {:reply, {:error, :caller_dead}, state}

      state.ingress == nil ->
        {:reply, {:error, :unavailable}, state}

      map_size(state.active) >= state.capacity ->
        {:reply, {:error, :overloaded}, state}

      true ->
        forward(ip, state)
    end
  end

  def handle_call({:admit, _id}, _from, state) when state.status != :open do
    {:reply, {:error, :unavailable}, state}
  end

  def handle_call({:admit, id}, _from, state) do
    if map_size(state.active) >= state.capacity do
      {:reply, {:error, :overloaded}, state}
    else
      active = Map.put(state.active, id, state.generation)
      {:reply, {:ok, state.generation}, %{state | active: active}}
    end
  end

  def handle_call({:release, id, outcome}, _from, state) do
    case Map.pop(state.active, id) do
      {nil, _active} ->
        {:reply, {:error, :stale}, state}

      {_generation, active} ->
        counts = Map.update!(state.counts, outcome, &(&1 + 1))
        {:reply, :ok, %{state | active: active, counts: counts}}
    end
  end

  defp forward(ip, state) do
    {in_name, out_name} = state.ingress

    case GenServer.whereis(out_name) do
      nil ->
        {:reply, {:error, :unavailable}, state}

      consumer_pid ->
        GenServer.cast(out_name, {in_name, ip})
        active = Map.put(state.active, ip.ref, state.generation)
        {:reply, {:ok, consumer_pid}, %{state | active: active}}
    end
  end
end
