defmodule Flowex.Admission do
  @moduledoc """
  The admission owner (FX-001): one process outside the replaceable stage
  subtree, holding its pipeline generation's admitted-work ledger.

  The contract, in brief — a submission either acquires a permit (the work
  WILL be accounted to a terminal outcome) or is refused immediately and
  observably; permits are held for the work's whole lifetime (a caller's
  timeout or death releases nothing); every admitted job reaches exactly
  one terminal outcome; and any topology failure quiesces the generation —
  outstanding work is reported `unknown`, never invented into success or
  failure. See `docs/research/flowex/C-admission-design.md`.

  The owner monitors every line worker. Any worker death tears the
  generation: because the wrapper supervisor is `:rest_for_one` with the
  owner FIRST, the owner's own death restarts the whole line too — a lost
  ledger can never overlap still-executing old work.
  """

  use GenServer

  # How long a caller's admission attempt keeps retrying while the owner
  # settles after a topology failure — so an honest restart does not
  # surface to callers as a refusal. Capacity refusals are never retried.
  @settle_grace 250
  @attach_step 5

  @type outcome :: :succeeded | :recovered | :expired | :failed | :cancelled | :unknown

  @spec start_link(pos_integer(), [term()], GenServer.name()) :: GenServer.on_start()
  def start_link(capacity, worker_names, name) do
    Flowex.Names.await_free!(name)
    GenServer.start_link(__MODULE__, {capacity, worker_names}, name: name)
  end

  @doc """
  Reserves a permit for `id`, or refuses: `{:error, :overloaded}` at
  capacity (immediate — the first public contract), `{:error,
  :unavailable}` while the topology settles after a failure (retried for a
  short grace before being reported).
  """
  @spec admit(GenServer.name(), reference()) ::
          {:ok, integer()} | {:error, :overloaded | :unavailable}
  def admit(owner, id) do
    limit = System.monotonic_time(:millisecond) + @settle_grace

    case GenServer.call(owner, {:admit, id}) do
      {:error, :unavailable} = refusal ->
        if System.monotonic_time(:millisecond) < limit do
          Process.sleep(@attach_step)
          admit(owner, id)
        else
          refusal
        end

      reply ->
        reply
    end
  end

  @doc """
  Releases a permit with its terminal outcome — exactly once; a second
  notice, or one from a dead generation, is refused as stale.
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

  @impl true
  def init({capacity, worker_names}) do
    send(self(), :attach)

    {:ok,
     %{
       capacity: capacity,
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
          # Topology failure: quiesce the generation. Outstanding work is
          # unknown — in-flight packets died with the workers; nothing is
          # invented. Capacity reopens only after every name resolves to a
          # fresh incarnation, under a new generation that stale releases
          # from this one cannot touch.
          counts = Map.update!(state.counts, :unknown, &(&1 + map_size(state.active)))

          Process.send_after(self(), :attach, @attach_step)

          {:noreply,
           %{
             state
             | active: %{},
               generation: state.generation + 1,
               monitors: monitors,
               counts: counts,
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
end
