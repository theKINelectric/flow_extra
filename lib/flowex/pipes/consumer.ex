defmodule Flowex.Consumer do
  @moduledoc "Consumes the pipeline data"

  use GenStage

  def start_link(subscribe_to, opts \\ []) do
    GenStage.start_link(__MODULE__, subscribe_to, opts)
  end

  @impl true
  def init(subscribe_to \\ []) do
    subscribe_to = Enum.map(subscribe_to, &{&1, max_demand: 1})
    {:consumer, nil, subscribe_to: subscribe_to}
  end

  @impl true
  def handle_events([ip], _from, nil) do
    # The reply destination is revocable (FX-005): calls carry a process
    # alias, casts carry nothing. `requester` remains on the packet for
    # observation.
    if ip.reply_to, do: send(ip.reply_to, ip)

    {:noreply, [], nil}
  end

  @impl true
  def handle_cast({in_name, ip}, nil) do
    GenStage.cast(in_name, ip)
    {:noreply, [], nil}
  end
end
