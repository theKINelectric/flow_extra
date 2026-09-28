defmodule Flowex.Admission do
  @moduledoc """
  The admission owner (FX-001): one process outside the replaceable stage
  subtree, holding its pipeline generation's admitted-work ledger.

  The contract, in brief — a submission is one transaction: the owner
  reserves the permit AND forwards the packet (`submit/3`), so no caller
  death can strand a reservation without its packet. A submission either
  acquires a permit (the work is accounted from there) or meets a
  definite refusal — every definite refusal is enforced again at
  dequeue. The one thing that can still execute after its caller heard
  otherwise is a submission admitted in the last instant before its
  acknowledgment was lost, and that outcome carries its own name —
  `{:unacknowledged, ref}` — never the name of a refusal. Permits are
  held for the work's whole lifetime: a caller's timeout or death
  releases nothing, and a topology failure RETAINS its generation's
  permits — surviving work keeps executing and keeps its capacity until
  its own terminal release, while work destroyed with the topology
  stays admitted, outcome unknown. Nothing is invented into success or
  failure, and no slot is reused under surviving work. Every admitted
  job is either released to exactly one terminal outcome or remains an
  unresolved reservation — reported `active`, outcome unknown — until
  the pipeline is restarted (retention with manual recovery: stop and
  start again, a confirmed termination of the whole execution
  generation). See `docs/research/flowex/C-admission-design.md`.

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
  # How long a caller whose budget has expired keeps waiting for the
  # acknowledgment: a reply already on its way converts would-be
  # uncertainty into a definite answer (and a late acceptance into the
  # ordinary timeout contract instead of a phantom refusal).
  @ack_slack 25

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
  admission's clock), capped by the settling grace. That budget travels
  IN the submission envelope and is checked at dequeue — a submission the
  caller has already been refused for (its budget expired waiting for the
  acknowledgment) is refused again by the owner, never forwarded, never
  executed after the fact.

  Refusal certainty — the outcome names what the caller actually knows:

  - `{:error, :overloaded}` — definite: at capacity, nothing reserved,
    nothing will execute.
  - `{:error, :unavailable}` — definite: the owner itself refused —
    settling, or the attempt's deadline expired at dequeue — nothing
    reserved, nothing will execute.
  - `{:error, {:unacknowledged, ref}}` — UNKNOWN: the acknowledgment
    timed out and the submission may have been admitted in the last
    instant before the reply was lost. Keep the `ref` and inspect
    `report/1`'s `:refs` — unresolved-reservation visibility, not
    admission history: a ref present means admitted-and-unresolved; a
    ref ABSENT means released or never admitted, and does not prove
    non-admission.
  - `{:error, :deadline}` — the caller's own budget ran out; the
    residual uncertainty is the ordinary timeout contract (execution
    may continue past a caller's deadline).
  - `{:error, :noprocess}` — the owner (and with it the line) is gone;
    anything accepted in its last instant died with the line.
  """
  @spec submit(GenServer.name(), Flowex.IP.t(), integer() | nil) ::
          {:ok, pid()}
          | {:error, :overloaded | :unavailable | :deadline | :noprocess}
          | {:error, {:unacknowledged, reference()}}
  def submit(owner, ip, deadline \\ nil) do
    started = System.monotonic_time(:millisecond)
    grace_end = started + @settle_grace

    if deadline && deadline < grace_end do
      ask(owner, {:submit, ip, deadline}, deadline, :deadline, ip.ref)
    else
      ask(owner, {:submit, ip, grace_end}, grace_end, :unavailable, ip.ref)
    end
  end

  @doc """
  Reserves a permit for `id` without a packet — a diagnostic for ledger
  and reconciliation probes; the engine paths use `submit/3`. Refusal
  semantics as `submit/3` with the settling grace as the whole budget;
  an unacknowledged diagnostic attempt reports `{:error,
  {:unacknowledged, nil}}`.
  """
  @spec admit(GenServer.name(), reference()) ::
          {:ok, integer()}
          | {:error, :overloaded | :unavailable | :noprocess}
          | {:error, {:unacknowledged, nil}}
  def admit(owner, id) do
    budget_end = System.monotonic_time(:millisecond) + @settle_grace
    ask(owner, {:admit, id}, budget_end, :unavailable, nil)
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

  @doc """
  The generation's ledger, for diagnostics and reconciliation tests.
  `active` counts unresolved reservations — queued and executing work,
  and work destroyed with the topology whose outcome is unknown until
  the pipeline is restarted; `refs` exposes those reservations'
  identities. `refs` is unresolved-reservation VISIBILITY, not admission
  history: a completed request leaves the list, so a ref's absence means
  released-or-never-admitted and does not prove non-admission. The
  ledger keeps no record of past admissions.
  """
  @spec report(GenServer.name()) :: %{
          required(:generation) => integer(),
          required(:capacity) => pos_integer(),
          required(:active) => non_neg_integer(),
          required(:refs) => [reference()],
          required(:status) => :open | :settling,
          required(:counts) => %{optional(outcome()) => non_neg_integer()}
        }
  def report(owner) do
    GenServer.call(owner, :report)
  end

  defp ask(owner, msg, budget_end, bound, identity) do
    remaining = budget_end - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      # The budget died without another attempt: nothing new was sent, so
      # the refusal is definite.
      {:error, definite(bound)}
    else
      try do
        GenServer.call(owner, msg, remaining + @ack_slack)
      catch
        # GenServer.call exits with a wrapped reason on timeout. The
        # acknowledgment never came: the submission may still be queued,
        # so the outcome is uncertain — the dequeue deadline check is
        # what keeps it from executing, and the identity is what lets
        # the caller reconcile the residual race.
        :exit, :timeout -> {:error, uncertain(bound, identity)}
        :exit, {:timeout, {GenServer, :call, _}} -> {:error, uncertain(bound, identity)}
        :exit, _gone -> {:error, :noprocess}
      else
        {:error, :unavailable} ->
          if System.monotonic_time(:millisecond) < budget_end do
            Process.sleep(min(@attach_step, budget_end - System.monotonic_time(:millisecond)))
            ask(owner, msg, budget_end, bound, identity)
          else
            # The owner itself refused, after the budget's end: definite.
            {:error, definite(bound)}
          end

        reply ->
          reply
      end
    end
  end

  defp definite(:unavailable), do: :unavailable
  defp definite(:deadline), do: :deadline

  defp uncertain(:unavailable, identity), do: {:unacknowledged, identity}

  # A deadline-bound wait that never got acknowledged has the ordinary
  # timeout contract as its residual: execution may continue past a
  # caller's deadline, so :timeout is already the honest report.
  defp uncertain(:deadline, _identity), do: :deadline

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
      {:noreply, %{state | monitors: reattach(state.monitors, pids), status: :open}}
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

  # Survivors keep their monitors; gone incarnations are demonitored and
  # flushed; only genuinely new incarnations get a fresh monitor — the
  # watched set never accumulates stale references across restarts.
  defp reattach(monitors, pids) do
    watched = MapSet.new(pids)

    {kept, stale} =
      Enum.split_with(monitors, fn {_ref, pid} -> MapSet.member?(watched, pid) end)

    for {ref, _pid} <- stale, do: Process.demonitor(ref, [:flush])

    monitored = MapSet.new(monitors, fn {_ref, pid} -> pid end)

    fresh =
      for pid <- pids, not MapSet.member?(monitored, pid), into: %{} do
        {Process.monitor(pid), pid}
      end

    Map.merge(Map.new(kept), fresh)
  end

  @impl true
  def handle_call(:report, _from, state) do
    {:reply,
     %{
       generation: state.generation,
       capacity: state.capacity,
       active: map_size(state.active),
       refs: Map.keys(state.active),
       status: state.status,
       counts: state.counts
     }, state}
  end

  def handle_call({:submit, ip, admission_deadline}, {caller, _ref}, state) do
    cond do
      # Refused at dequeue: this attempt's own admission deadline died
      # while the request waited — the caller has already been refused,
      # so no reservation, no forwarding, no execution behind the refusal.
      # (The packet's execution deadline is a separate, later boundary.)
      Flowex.Pipeline.expired?(admission_deadline) ->
        {:reply, {:error, :unavailable}, state}

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
        # The resolved incarnation is both the delivery target and the
        # identity handed back for the caller's monitor — they cannot
        # disagree, whatever a restart does between them.
        GenServer.cast(consumer_pid, {in_name, ip})
        active = Map.put(state.active, ip.ref, state.generation)
        {:reply, {:ok, consumer_pid}, %{state | active: active}}
    end
  end
end
