defmodule FlowExtra.Supervisor do
  @moduledoc """
  Pipeline supervisor — both the wrapper (admission owner + line) and the
  line itself, always `:rest_for_one`.
  """

  use Supervisor

  def start_link(children, name) do
    FlowExtra.Names.await_free!(name)
    Elixir.Supervisor.start_link(__MODULE__, children, name: name)
  end

  @impl true
  def init(children) do
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
