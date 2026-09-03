defmodule Flowex.Application do
  @moduledoc """
  The library's own Application (T5 ruling, PASSON_FLOWEX_T5_RULING.md).

  Owns exactly one permanent child: `Flowex.Registry`, the process-name
  registry. One atom, one process, app lifetime — the constant cost of
  atom-flat process naming, paid once for the fork's whole life.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Flowex.Registry}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__.Supervisor)
  end
end
