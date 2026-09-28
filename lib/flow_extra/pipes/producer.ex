defmodule FlowExtra.Producer do
  @moduledoc "Pushes data to pipeline"

  use GenStage

  def start_link(nil, opts \\ []) do
    name = Keyword.get(opts, :name)
    if name, do: FlowExtra.Names.await_free!(name)
    GenStage.start_link(__MODULE__, nil, opts)
  end

  @impl true
  def init(_) do
    # An explicitly bounded queue drained by accumulated demand (FX-001) —
    # the queue can never exceed the admission capacity, because admission
    # gates everything that reaches the producer. No reliance on GenStage's
    # keep-last event buffer, which silently discarded the audit's overload
    # probe: 10_020 casts in, 10_001 callbacks out.
    {:producer, %{queue: :queue.new(), demand: 0}}
  end

  @impl true
  def handle_demand(incoming, state = %{demand: demand}) do
    dispatch(%{state | demand: demand + incoming})
  end

  @impl true
  def handle_cast(ip = %FlowExtra.IP{}, state) do
    dispatch(%{state | queue: :queue.in(ip, state.queue)})
  end

  defp dispatch(state = %{demand: 0}), do: {:noreply, [], state}

  defp dispatch(state) do
    {events, state} = take_up_to(state, state.demand)
    {:noreply, events, %{state | demand: state.demand - length(events)}}
  end

  defp take_up_to(state, 0), do: {[], state}

  defp take_up_to(state, n) do
    case :queue.out(state.queue) do
      {:empty, _queue} ->
        {[], state}

      {{:value, ip}, queue} ->
        {events, state} = take_up_to(%{state | queue: queue}, n - 1)
        {[ip | events], state}
    end
  end
end
