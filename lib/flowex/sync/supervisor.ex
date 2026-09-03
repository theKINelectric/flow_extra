defmodule Flowex.Sync.Supervisor do
  @moduledoc "Sync pipeline supevisor"

  use Supervisor

  def start_link(pipeline_module, ref, name, opts) do
    Supervisor.start_link(__MODULE__, [pipeline_module, ref, opts], name: name)
  end

  def init([pipeline_module, ref, opts]) do
    name = Flowex.Names.via(pipeline_module, ref, :sync_gen_server)

    children = [
      worker(Flowex.Sync.GenServer, [{pipeline_module, opts}, [name: name]], id: name)
    ]

    supervise(children, strategy: :one_for_one)
  end
end
