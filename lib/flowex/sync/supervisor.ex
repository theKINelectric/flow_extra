defmodule Flowex.Sync.Supervisor do
  @moduledoc """
  Sync pipeline supervisor. Receives the prepared stage list (FX-007) —
  module `init/1` already ran in the starting caller — and bakes it into the
  child spec, so a restart reuses the prepared options.
  """

  use Supervisor

  def start_link(pipeline_module, ref, name, prepared_stages) do
    Supervisor.start_link(__MODULE__, [pipeline_module, ref, prepared_stages], name: name)
  end

  @impl true
  def init([pipeline_module, ref, prepared_stages]) do
    name = Flowex.Names.via(pipeline_module, ref, :sync_gen_server)

    children = [
      %{
        id: name,
        start: {Flowex.Sync.GenServer, :start_link, [prepared_stages, [name: name]]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
