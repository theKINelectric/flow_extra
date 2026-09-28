defmodule AdmissionTrapPipeline do
  @moduledoc """
  FX-001 closure instrument (Astra's independent validation): the callback
  blocks until its own observer releases it, so a test can hold admitted
  work provably mid-execution across a topology failure — and prove the
  work SURVIVED that failure by finishing it afterwards.
  """

  use FlowExtra.Pipeline

  defstruct [:observer, :id]

  pipe(:work)

  def work(%{observer: observer, id: id}, _opts) do
    if observer, do: send(observer, {:entered, id, self()})

    receive do
      :release -> :ok
    after
      2_000 -> :ok
    end

    if observer, do: send(observer, {:finished, id})
    %{}
  end
end
