defmodule Flowex.Supervisor do
  @moduledoc "Pipeline supervisor"

  use Supervisor

  def start_link(children, name) do
    Elixir.Supervisor.start_link(__MODULE__, children, name: name)
  end

  @impl true
  def init(children) do
    Supervisor.init(children, strategy: :rest_for_one)
  end
end
