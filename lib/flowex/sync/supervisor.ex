defmodule Flowex.Sync.Supervisor do
  @moduledoc "Sync pipeline supervisor"

  use Supervisor

  def start_link(pipeline_module, ref, name, opts) do
    # Admission before the supervisor spawns, so a bad init/1 result is
    # refused at start — not discovered on the first call, far from the cause.
    Flowex.Pipeline.validate_opts!(pipeline_module, opts)
    Supervisor.start_link(__MODULE__, [pipeline_module, ref, opts], name: name)
  end

  @impl true
  def init([pipeline_module, ref, opts]) do
    name = Flowex.Names.via(pipeline_module, ref, :sync_gen_server)

    children = [
      %{
        id: name,
        start: {Flowex.Sync.GenServer, :start_link, [{pipeline_module, opts}, [name: name]]}
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
