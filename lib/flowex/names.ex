defmodule Flowex.Names do
  @moduledoc """
  Pipeline process naming (T5 ruling).

  Every named process is `{:via, Registry, {Flowex.Registry, key}}` with key
  `{pipeline_module, ref, role}` — terms, not atoms: registered atoms are
  never garbage-collected, so per-instance atom names ratchet the atom table
  forever. The ref is minted once per pipeline build and baked into the child
  specs, so a restarted child re-registers its same key and consumers
  re-resolve through the registry — no hand-rolled re-subscribe, no cascade
  surgery.
  """

  @registry Flowex.Registry

  @spec registry :: atom()
  def registry, do: @registry

  @spec via(module(), reference(), term()) :: {:via, Registry, {atom(), term()}}
  def via(pipeline_module, ref, role) do
    {:via, Registry, {@registry, {pipeline_module, ref, role}}}
  end

  @doc """
  Stops a pipeline: workers first, then the supervisor. The one shared
  implementation for both tracks (the parallel builder and the sync pipeline
  carried duplicate copies of this logic).
  """
  def stop_pipeline(%Flowex.Pipeline{sup_name: sup_name}) do
    Enum.each(Supervisor.which_children(sup_name), fn {id, _pid, :worker, [_]} ->
      Supervisor.terminate_child(sup_name, id)
    end)

    Supervisor.stop(sup_name)
  end
end
