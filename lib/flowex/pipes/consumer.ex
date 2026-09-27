defmodule Flowex.Consumer do
  @moduledoc "Consumes the pipeline data"

  use GenStage

  def start_link(subscribe_to, owner_name \\ nil, opts \\ []) do
    name = Keyword.get(opts, :name)
    if name, do: Flowex.Names.await_free!(name)
    GenStage.start_link(__MODULE__, {subscribe_to, owner_name}, opts)
  end

  @impl true
  def init({subscribe_to, owner_name}) do
    subscribe_to = Enum.map(subscribe_to, &{&1, max_demand: 1})
    {:consumer, owner_name, subscribe_to: subscribe_to}
  end

  @impl true
  def handle_events([ip], _from, owner_name) do
    # Accounting first (FX-001): the permit is released — exactly once, at
    # the terminal observation — before any reply, so a caller that learns
    # a result can immediately rely on the freed capacity.
    if owner_name,
      do: Flowex.Admission.release(owner_name, ip.ref, outcome(ip))

    # The reply destination is revocable (FX-005): calls carry a process
    # alias, casts carry nothing. `requester` remains for observation.
    if ip.reply_to, do: send(ip.reply_to, ip)

    {:noreply, [], owner_name}
  end

  @impl true
  def handle_cast({in_name, ip}, owner_name) do
    GenStage.cast(in_name, ip)
    {:noreply, [], owner_name}
  end

  # The terminal-outcome mapping (C design record): reaching the consumer
  # IS completion — recovered-by-error-pipe is a completed result, expired
  # means the reply is dropped and whether callbacks began is not recorded.
  defp outcome(%{expired: true}), do: :expired
  defp outcome(%{error: nil}), do: :succeeded
  defp outcome(%{error: _recovered}), do: :recovered
end
