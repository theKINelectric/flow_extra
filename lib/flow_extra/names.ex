defmodule FlowExtra.Names do
  @moduledoc """
  Pipeline process naming (T5 ruling).

  Every named process is `{:via, Registry, {FlowExtra.Registry, key}}` with key
  `{pipeline_module, ref, role}` — terms, not atoms: registered atoms are
  never garbage-collected, so per-instance atom names ratchet the atom table
  forever. The ref is minted once per pipeline build and baked into the
  child specs, so a restarted child re-registers its same key and consumers
  re-resolve through the registry — no hand-rolled re-subscribe, no cascade
  surgery.

  Roles: `:supervisor` is the pipeline wrapper (admission owner first, line
  supervisor second, under `:rest_for_one` — see `FlowExtra.Admission`);
  `:admission_owner` is the admitted-work ledger; `:line` is the
  producer/stages/consumer supervisor; `:producer`, `:consumer`,
  `{:function_stage | :module_stage, ref}` are line workers;
  `:sync_supervisor` and `:sync_gen_server` are the sync engine's pair.
  """

  @registry FlowExtra.Registry

  @spec registry :: atom()
  def registry, do: @registry

  @spec via(module(), reference(), term()) :: {:via, Registry, {atom(), term()}}
  def via(pipeline_module, ref, role) do
    {:via, Registry, {@registry, {pipeline_module, ref, role}}}
  end

  @doc """
  Blocks until `name` is free (FX-001 restart determinism).

  A supervisor-driven restart races the old generation's asynchronous death
  cascade: a freshly started child that registers a still-held name of a
  dying process adopts that dead pid, and the cascade of adopt-and-DOWN
  burns restart intensity at every level — killing the owning parent (an
  OTP-only reproduction lives in the C design record). Waiting for the old
  name to clear — which the Registry does within moments of the death —
  makes every restart deterministic at effectively zero cost: no stale
  name, no waiting. Bounded, then the registration fails naturally.
  """
  @spec await_free!(term(), timeout()) :: :ok
  def await_free!(name, timeout \\ 5_000) do
    limit = System.monotonic_time(:millisecond) + timeout

    do_await_free!(name, limit)
  end

  defp do_await_free!(name, limit) do
    if GenServer.whereis(name) == nil or System.monotonic_time(:millisecond) > limit do
      :ok
    else
      Process.sleep(1)
      do_await_free!(name, limit)
    end
  end

  @doc """
  Stops a pipeline through its owner (FX-003).

  A supervised pipeline is a `restart: :permanent` child of the parent it
  was started under, and the permanent contract restarts a child even after
  normal termination — so `Supervisor.stop` on the pipeline supervisor just
  gets resurrected. Intentional removal therefore goes through the owning
  supervisor: terminate the child, then delete its spec. Abnormal exits are
  untouched: the restart policy still answers crashes.

  A standalone pipeline (no parent recorded) keeps the original shutdown:
  children first — the admission owner and the line supervisor alike, the
  wrapper's shape is not a caller's concern — then the supervisor itself.
  """
  def stop_pipeline(%FlowExtra.Pipeline{sup_name: sup_name, parent: nil}) do
    Enum.each(Supervisor.which_children(sup_name), fn {id, _pid, _type, _modules} ->
      Supervisor.terminate_child(sup_name, id)
    end)

    Supervisor.stop(sup_name)
  end

  def stop_pipeline(%FlowExtra.Pipeline{sup_name: sup_name, parent: parent})
      when is_pid(parent) do
    :ok = Supervisor.terminate_child(parent, sup_name)
    :ok = Supervisor.delete_child(parent, sup_name)
  end
end
