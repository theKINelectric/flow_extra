defmodule Flowex.Producer do
  @moduledoc "Pushes data to pipeline"

  use GenStage

  def start_link(nil, opts \\ []) do
    GenStage.start_link(__MODULE__, nil, opts)
  end

  @impl true
  def init(_), do: {:producer, []}

  @impl true
  def handle_demand(_demand, [ip | ips]) do
    {:noreply, [ip], ips}
  end

  @impl true
  def handle_demand(_demand, []) do
    {:noreply, [], []}
  end

  @impl true
  def handle_cast(ip = %Flowex.IP{}, ips) do
    {:noreply, [ip], ips}
  end
end
